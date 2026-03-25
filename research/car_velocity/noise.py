import marimo

__generated_with = "0.20.4"
app = marimo.App(width="full")


@app.cell
def _():
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    return


@app.cell
def _():
    import marimo as mo
    import numpy as np
    import pandas as pd
    import requests
    import matplotlib.pyplot as plt
    from api.openf1 import get_sessions, get_drivers, get_car_data, get_laps

    return get_car_data, get_drivers, get_laps, get_sessions, mo, np, pd, plt


@app.cell
def _(mo):
    mo.md("""
    # Deriving Acceleration from F1 Telemetry

    The [OpenF1 API](https://openf1.org) provides car telemetry including speed, throttle, brake,
    DRS, and gear data sampled at approximately 3.7 Hz. Before we can compute meaningful
    acceleration or jerk signals, we need to understand the sampling characteristics of this data
    and make principled decisions about how to process it.

    This notebook walks through that process: from raw speed data to filtered acceleration and jerk,
    documenting each decision along the way.

    ---

    ## 1. Data Collection

    We'll work with Lewis Hamilton's car data from the 2024 Miami Grand Prix race (meeting 1280),
    focusing on laps 2–3 as a representative window of normal racing conditions.
    """)
    return


@app.cell
def _(get_sessions):
    MEETING_ID = 1280
    sessions = get_sessions(MEETING_ID)
    return (sessions,)


@app.cell
def _(sessions):
    for session in sessions:
        if session["session_type"] == "Race" and session["session_name"] == "Race":
            SESSION_KEY = session["session_key"]
    return (SESSION_KEY,)


@app.cell
def _(SESSION_KEY, get_drivers):
    drivers = get_drivers(SESSION_KEY)
    for driver in drivers:
        if driver["name_acronym"] == "HAM":
            DRIVER_NUMBER = driver["driver_number"]
    return DRIVER_NUMBER, drivers


@app.cell
def _(DRIVER_NUMBER, SESSION_KEY, get_laps):
    lap_info = get_laps(SESSION_KEY, DRIVER_NUMBER)
    START_TIME = lap_info[0]['date_start']
    END_TIME = lap_info[10]['date_start']
    return END_TIME, START_TIME


@app.cell
def _(DRIVER_NUMBER, END_TIME, SESSION_KEY, START_TIME, get_car_data):
    car_data = get_car_data(
        session_key=SESSION_KEY,
        driver_number=DRIVER_NUMBER,
        date_start=START_TIME,
        date_end=END_TIME
    )
    return (car_data,)


@app.cell
def _(car_data, pd):
    car_data_df = pd.DataFrame(car_data)
    car_data_df['date'] = pd.to_datetime(car_data_df['date'])
    return (car_data_df,)


@app.cell
def _(car_data_df, plt):
    plt.plot(car_data_df.date, car_data_df.speed)
    plt.xlabel('Time')
    plt.ylabel('Speed (kph)')
    plt.title('HAM raw speed (laps 1–10), 2026 China GP')
    return


@app.cell
def _(mo):
    mo.md("""
    ---

    ## 2. Assessing Sampling Consistency

    Before differentiating speed to get acceleration, we need to characterise the sampling rate.
    Numerical differentiation amplifies noise, and that effect is compounded when the time steps
    between samples are irregular — a small speed change divided by a very short time interval
    produces a spuriously large apparent acceleration.

    We compute the inter-sample time differences and summarise their distribution.
    """)
    return


@app.cell
def _(car_data_df):
    sampling_diffs = car_data_df.date.diff().dt.total_seconds()
    return (sampling_diffs,)


@app.cell
def _(plt, sampling_diffs):
    plt.hist(sampling_diffs.dropna())
    plt.xlabel('Inter-sample interval (s)')
    plt.ylabel('Count')
    plt.title('Distribution of sampling intervals — HAM laps 2–3')
    return


@app.cell
def _(sampling_diffs):
    sampling_diffs_mean = sampling_diffs.mean()
    sampling_diffs_std = sampling_diffs.std()
    sampling_diffs_cov = sampling_diffs_std / sampling_diffs_mean
    return sampling_diffs_cov, sampling_diffs_mean, sampling_diffs_std


@app.cell
def _(mo, sampling_diffs_cov, sampling_diffs_mean, sampling_diffs_std):
    mo.md(f"""
    | Statistic | Value |
    |---|---|
    | Mean interval | {sampling_diffs_mean:.4f} s (~{1/sampling_diffs_mean:.1f} Hz) |
    | Std deviation | {sampling_diffs_std:.4f} s |
    | Coefficient of variation | {sampling_diffs_cov:.3f} |

    A CV of **{sampling_diffs_cov:.2f}** indicates meaningful variability — roughly ±37% around the
    mean interval. This isn't noise in the physical signal; it's a property of how OpenF1
    publishes telemetry. The question is whether it's specific to Hamilton or consistent across
    the whole grid.
    """)
    return


@app.cell
def _(car_data_df):
    # Flag runs of >=8 consecutive identical speed values (~2s at 3.7Hz).
    # Shorter repeated values may be legitimate (e.g. brief coasting at constant speed);
    # long flat runs are almost certainly bfill/ffill artifacts from the API.
    _same = car_data_df['speed'].diff().eq(0)
    _group_id = (~_same).cumsum()
    _run_len = _group_id.map(_group_id.value_counts())
    dup_mask = _run_len >= 8
    return (dup_mask,)


@app.cell
def _(car_data_df, dup_mask, plt):
    plt.plot(car_data_df['date'], car_data_df['speed'], linewidth=0.8)
    plt.scatter(
        car_data_df.loc[dup_mask, 'date'],
        car_data_df.loc[dup_mask, 'speed'],
        color='red', s=20, zorder=5
    )
    plt.xlabel('Time')
    plt.ylabel('Speed (kph)')
    plt.title('Raw speed — duplicate-speed samples highlighted in red')
    return


@app.cell
def _(car_data_df, dup_mask, plt):
    v_clean = car_data_df['speed'].copy().astype(float)
    v_clean[dup_mask] = float('nan')
    plt.plot(car_data_df['date'], v_clean, linewidth=0.8)
    plt.xlabel('Time')
    plt.ylabel('Speed (kph)')
    plt.title('Raw speed — duplicate samples removed (PCHIP will interpolate over breaks)')
    return


@app.cell
def _(dup_mask, mo):
    mo.md(f"""
    **{dup_mask.sum()} duplicate sample(s)** identified — speed identical to the previous sample.
    These are dropped before PCHIP interpolation so the spline spans those points naturally,
    rather than being forced through a repeated value that isn't real new information.
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ---

    ## 3. Sampling Consistency Across All Drivers

    We repeat the same analysis for every driver in the race, using laps 3–7 as a
    consistent mid-race window (skipping the formation lap and first racing lap).
    Team colour is included so we can visually check whether any constructor shows
    systematically different behaviour.
    """)
    return


@app.cell
def _(SESSION_KEY, drivers, get_car_data, get_laps, pd):
    def _driver_sampling_stats(driver):
        try:
            dn = driver['driver_number']
            laps = get_laps(SESSION_KEY, dn)
            timed = [l for l in laps if not l.get('is_pit_out_lap') and l.get('date_start')]
            if len(timed) < 5:
                return None
            start = timed[2]['date_start']
            end = timed[6]['date_start'] if len(timed) > 6 else timed[-1]['date_start']
            data = get_car_data(SESSION_KEY, dn, start, end)
            if len(data) < 10:
                return None
            df = pd.DataFrame(data)
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').reset_index(drop=True)
            dt = df['date'].diff().dt.total_seconds().dropna()
            mean_dt = dt.mean()
            std_dt = dt.std()
            return {
                'driver': driver['name_acronym'],
                'team': driver['team_name'],
                'team_colour': '#' + (driver.get('team_colour') or 'aaaaaa'),
                'mean_dt': round(mean_dt, 4),
                'std_dt': round(std_dt, 4),
                'cv': round(std_dt / mean_dt, 4) if mean_dt > 0 else None,
                'n_samples': len(dt),
            }
        except Exception:
            return None

    rows = [r for d in drivers if (r := _driver_sampling_stats(d)) is not None]
    sampling_summary = pd.DataFrame(rows).sort_values('cv', ascending=False).reset_index(drop=True)
    return (sampling_summary,)


@app.cell
def _(sampling_summary):
    sampling_summary
    return


@app.cell
def _(sampling_summary):
    team_summary = (
        sampling_summary
        .groupby('team')[['mean_dt', 'std_dt', 'cv']]
        .mean()
        .round(4)
        .sort_values('cv', ascending=False)
    )
    team_summary
    return


@app.cell
def _(mo):
    mo.md("""
    Sampling variability is consistent across all drivers and constructors. This confirms
    it is a characteristic of the OpenF1 API, not anything specific to a car or team.
    Every downstream pipeline working with this data faces the same problem.

    ---

    ## 4. Why Direct Differentiation Fails

    To motivate the filtering pipeline, we first show what happens if we naively differentiate
    speed against the raw, irregularly-spaced timestamps. Acceleration is computed as
    Δspeed / Δtime and converted to g (1 g = 9.81 m/s²; 1 kph/s = 1000/3600 m/s²).
    """)
    return


@app.function
def get_accel_from_speed(timestamps, speeds):
    time_diffs = timestamps.diff().dt.total_seconds()
    speed_diffs = speeds.diff()
    return speed_diffs / time_diffs


@app.cell
def _(car_data_df):
    accels_raw = get_accel_from_speed(
        timestamps=car_data_df.date,
        speeds=car_data_df.speed
    )
    return (accels_raw,)


@app.cell
def _(accels_raw, car_data_df, plt):
    plt.plot(car_data_df['date'], accels_raw * (1000 / 3600) / 9.81)
    plt.axhline(0, color='white', linewidth=0.5, linestyle='--')
    plt.xlabel('Time')
    plt.ylabel('Acceleration (g)')
    plt.title('Longitudinal acceleration — raw differentiation (unfiltered)')
    return


@app.cell
def _(mo):
    mo.md("""
    The signal contains physically implausible spikes (±6-8g longitudinally). These are
    directly caused by the irregular sampling: when two consecutive samples happen to land
    unusually close together in time, even a modest speed change produces an artificially
    large apparent acceleration. This is not signal — it is a sampling artifact.

    F1 cars produce roughly 5–6g under heavy braking and 1.5–2g under acceleration. Any
    spike significantly outside that envelope is noise.

    ---

    ## 5. Signal Processing Pipeline

    We address the irregular sampling with a three-step pipeline:

    1. **Resample to a regular grid** using PCHIP interpolation
    2. **Apply a low-pass Butterworth filter** to attenuate sampling noise
    3. **Differentiate** on the clean, regular grid

    ### Step 1: Resampling with PCHIP

    PCHIP (Piecewise Cubic Hermite Interpolating Polynomial) was chosen over alternatives for
    two reasons:

    - **Monotonicity-preserving**: between any two samples it will not overshoot, so it cannot
      fabricate a braking event that isn't in the original data. A natural cubic spline can
      oscillate between unevenly-spaced knots, producing phantom accelerations before we have
      even differentiated.
    - **C1 continuous**: the first derivative is smooth, which is sufficient for our purposes
      since filtering handles the rest.

    We resample to **4 Hz** — close to the nominal source rate of ~3.7 Hz. Upsampling further
    (e.g. 10 Hz) does not add information; the signal's true bandwidth is capped at ~1.85 Hz
    (half the source rate), so additional points are pure interpolation with no new content.
    """)
    return


@app.cell
def _():
    from scipy.interpolate import PchipInterpolator

    return (PchipInterpolator,)


@app.cell
def _(PchipInterpolator, car_data_df, dup_mask, np):
    _clean = car_data_df[~dup_mask]
    _t = _clean['date'].astype(np.int64).to_numpy() / 1e9
    _v = _clean['speed'].to_numpy()

    _pchip = PchipInterpolator(_t, _v)

    FS = 4.0  # Hz — matches nominal source rate; upsampling further adds no information
    t_regular = np.arange(_t[0], _t[-1], 1 / FS)
    v_regular = _pchip(t_regular)
    return FS, t_regular, v_regular


@app.cell
def _(mo):
    mo.md("""
    ### Step 2: Butterworth Filter Cutoff Selection

    We used Winter's residual analysis to attempt to identify the cutoff frequency
    objectively. The method sweeps cutoff frequencies, computing the RMS difference
    between the original and filtered signal at each value. In biomechanical applications
    the resulting curve typically shows a steep drop (signal being removed) followed by a
    flat plateau (only noise remaining), with the optimal cutoff at the transition.

    For this data, no such plateau exists — the residual declines continuously across the
    entire testable range (0.1–1.85 Hz). This is expected: the irregular sampling contaminates
    all frequencies, not just the high end, so there is no clean noise floor to identify.
    Automated elbow-detection methods (kneedle, maximum curvature) both failed for this reason.

    The residual plot is shown below for transparency, but the cutoff was ultimately chosen
    on domain knowledge. F1 braking events typically last **2–4 seconds**, placing their fundamental energy at ~0.25–0.5 Hz — well
    below our source bandwidth ceiling of ~1.85 Hz. This means we have meaningful headroom
    above the signal of interest before we hit the noise floor, and a cutoff of **1.25 Hz**
    is justified: it preserves all braking and throttle dynamics while attenuating the
    bulk of the sampling irregularity noise. It also yielded gs similar to that reported elsewhere: https://f1chronicle.com/f1-g-force-how-many-gs-can-a-f1-car-pull/ & https://www.mercedesamgf1.com/news/g-force-and-formula-one-explained rather than overestimating the gs like with the raw data, so I think this is acceptable for our purposes.
    """)
    return


@app.cell
def _(plt, t_regular, v_regular):
    plt.plot(t_regular, v_regular)
    plt.xlabel('Time (s)')
    plt.ylabel('Speed (kph)')
    plt.title('Speed — PCHIP resampled (and gaps interpolated) to 4 Hz regular grid')
    return


@app.cell
def _(FS, np, plt, v_regular):
    from scipy.signal import butter, filtfilt
    CUTOFF_HZ = 1.25
    _cutoffs = np.linspace(0.1, 1.85, 50)
    _residuals = []
    for _fc in _cutoffs:
        _b, _a = butter(N=4, Wn=_fc / (FS / 2), btype='low')
        _v_filt = filtfilt(_b, _a, v_regular)
        _residuals.append(np.sqrt(np.mean((v_regular - _v_filt) ** 2)))

    plt.plot(_cutoffs, _residuals)
    plt.axvline(CUTOFF_HZ, color='orange', linestyle='--', label=f'Chosen cutoff: {CUTOFF_HZ} Hz')
    plt.xlabel('Cutoff frequency (Hz)')
    plt.ylabel('Residual (kph)')
    plt.title('Residual analysis — no clear noise floor, cutoff chosen by domain knowledge')
    plt.legend()
    return CUTOFF_HZ, butter, filtfilt


@app.cell
def _(CUTOFF_HZ, FS, butter, filtfilt, v_regular):
    _b, _a = butter(N=4, Wn=CUTOFF_HZ / (FS / 2), btype='low')
    v_filtered = filtfilt(_b, _a, v_regular)
    return (v_filtered,)


@app.cell
def _(mo):
    mo.md("""
    ---

    ## 6. Results

    ### Longitudinal Acceleration
    """)
    return


@app.cell
def _(FS, np, v_filtered):
    accel_g = np.diff(v_filtered) / (1 / FS) * (1000 / 3600) / 9.81
    jerk_gs = np.diff(accel_g) / (1 / FS)
    return accel_g, jerk_gs


@app.cell
def _(CUTOFF_HZ, accel_g, plt, t_regular):
    plt.plot(t_regular[1:], accel_g)
    plt.axhline(0, color='white', linewidth=0.5, linestyle='--')
    plt.xlabel('Time (s)')
    plt.ylabel('Acceleration (g)')
    plt.title(f'Longitudinal acceleration — PCHIP resampled, {CUTOFF_HZ} Hz Butterworth filtered')
    return


@app.cell
def _(mo):
    mo.md("""
    ### Jerk

    Jerk (rate of change of acceleration, g/s) is a candidate metric for driver smoothness —
    aggressive braking or throttle application produces high-magnitude jerk, while progressive
    inputs produce low jerk.

    **Caveat**: jerk is the second derivative of speed. Noise is amplified quadratically with
    frequency at each differentiation step. With a source bandwidth of only ~1.85 Hz and a
    1 Hz filter cutoff, the jerk signal should be treated as a directional indicator of
    smoothness rather than a precise physical measurement.
    """)
    return


@app.cell
def _(jerk_gs, plt, t_regular):
    plt.plot(t_regular[2:], jerk_gs)
    plt.axhline(0, color='white', linewidth=0.5, linestyle='--')
    plt.xlabel('Time (s)')
    plt.ylabel('Jerk (g/s)')
    plt.title('Jerk — directional smoothness indicator')
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
