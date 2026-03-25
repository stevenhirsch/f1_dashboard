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


@app.function
def compute_psd_stats(car_data_raw, fs=4.0, cutoff=1.25):
    """Dedup → PCHIP resample → Butterworth filter → Welch PSD.
    Returns (freqs, psd, frac_below_cutoff, peak_freq) or None if data insufficient."""
    import numpy as np, pandas as pd
    from scipy.interpolate import PchipInterpolator
    from scipy.signal import butter, filtfilt, welch

    if not car_data_raw or len(car_data_raw) < 20:
        return None
    df = pd.DataFrame(car_data_raw)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)

    same = df['speed'].diff().eq(0)
    group_id = (~same).cumsum()
    run_len = group_id.map(group_id.value_counts())
    clean = df[run_len < 8]
    if len(clean) < 10:
        return None

    t = clean['date'].astype(np.int64).to_numpy() / 1e9
    v = clean['speed'].to_numpy().astype(float)
    pchip = PchipInterpolator(t, v)
    t_reg = np.arange(t[0], t[-1], 1 / fs)
    if len(t_reg) < 32:
        return None
    v_reg = pchip(t_reg)

    b, a = butter(N=4, Wn=cutoff / (fs / 2), btype='low')
    v_filt = filtfilt(b, a, v_reg)

    freqs, psd = welch(v_filt, fs=fs, nperseg=min(256, len(v_filt) // 4))
    total = np.trapezoid(psd, freqs)
    below = np.trapezoid(psd[freqs <= cutoff], freqs[freqs <= cutoff])
    frac = below / total if total > 0 else 0.0
    return freqs, psd, frac, freqs[np.argmax(psd)]


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
def _(mo):
    mo.md("""
    ### Power Spectral Density

    The residual analysis tells us about *how much* signal energy we remove at each cutoff,
    but not *where* that energy sits in frequency. The PSD (estimated via Welch's method)
    lets us see the frequency distribution directly and check whether our chosen cutoff is
    consistent with the signal's natural spectral structure.

    Welch's method averages overlapping periodograms to reduce variance, giving a more
    reliable estimate than a raw FFT on a short, noisy segment.
    """)
    return


@app.cell
def _(CUTOFF_HZ, FS, plt, v_regular):
    from scipy.signal import welch

    _freqs, _psd = welch(v_regular, fs=FS, nperseg=min(256, len(v_regular) // 4))

    fig_psd, ax_psd = plt.subplots()
    ax_psd.semilogy(_freqs, _psd)
    ax_psd.axvline(CUTOFF_HZ, color='orange', linestyle='--', label=f'Chosen cutoff: {CUTOFF_HZ} Hz')
    ax_psd.axvline(FS / 2, color='grey', linestyle=':', label=f'Nyquist: {FS/2:.1f} Hz')
    ax_psd.set_xlabel('Frequency (Hz)')
    ax_psd.set_ylabel('PSD (kph² / Hz)')
    ax_psd.set_title('Power spectral density — PCHIP resampled speed (Welch estimate)')
    ax_psd.legend()
    fig_psd
    return (welch,)


@app.cell
def _(CUTOFF_HZ, FS, np, v_regular, welch):
    _freqs, _psd = welch(v_regular, fs=FS, nperseg=min(256, len(v_regular) // 4))
    _total_power = np.trapezoid(_psd, _freqs)
    _signal_power = np.trapezoid(_psd[_freqs <= CUTOFF_HZ], _freqs[_freqs <= CUTOFF_HZ])
    _frac = _signal_power / _total_power
    return


@app.cell
def _(CUTOFF_HZ, FS, mo, np, v_regular, welch):
    _freqs, _psd = welch(v_regular, fs=FS, nperseg=min(256, len(v_regular) // 4))
    _total_power = np.trapezoid(_psd, _freqs)
    _signal_power = np.trapezoid(_psd[_freqs <= CUTOFF_HZ], _freqs[_freqs <= CUTOFF_HZ])
    _frac = _signal_power / _total_power
    _peak_freq = _freqs[np.argmax(_psd)]
    mo.md(f"""
    | Metric | Value |
    |---|---|
    | Peak power frequency | {_peak_freq:.3f} Hz |
    | Power below {CUTOFF_HZ} Hz | {_signal_power:.3f} kph² / Hz ({_frac*100:.3f}% of total) |
    | Power above {CUTOFF_HZ} Hz | {(_total_power - _signal_power):.3f} kph² / Hz ({(1-_frac)*100:.3f}% of total) |

    The bulk of signal power sits at low frequencies, consistent with lap-scale speed variation
    (straights, braking zones, corners). The cutoff at **{CUTOFF_HZ} Hz** retains
    **{_frac*100:.3f}%** of total power while discarding the high-frequency tail that is
    dominated by sampling irregularity rather than physical dynamics.
    """)
    return


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
    from pipeline.api.openf1 import get_car_data as get_car_data_slow

    return (get_car_data_slow,)


@app.cell
def _():
    import pickle, pathlib

    try:
        CACHE_DIR = pathlib.Path(__file__).resolve().parent / 'cache'
    except NameError:
        CACHE_DIR = pathlib.Path('/home/steven/f1_dashboard/research/car_velocity/cache')
    CACHE_DIR.mkdir(exist_ok=True)
    # Set to True to load previously saved results instead of re-fetching from the API.
    USE_CACHE = True
    return CACHE_DIR, USE_CACHE, pickle


@app.cell
def _(mo):
    mo.md("""
    ---

    ## 7. Full-Race Spectral Analysis (HAM)

    The PSD analysis in Section 5 used only laps 1–10. To confirm the spectral structure and
    the 1.25 Hz cutoff are not artefacts of an early-race window, we repeat the per-lap PSD
    computation across **all race laps** for Hamilton. Each lap is treated independently: the
    PCHIP → Butterworth → Welch pipeline runs on data bounded by consecutive lap start times.

    If the cutoff is well-chosen, every lap should show the same low-frequency-dominated shape,
    and the fraction of power below 1.25 Hz should be stable (< 5 pp variation) across the race.
    """)
    return


@app.cell
def _(DRIVER_NUMBER, SESSION_KEY, get_laps):
    ham_laps_timed = [l for l in get_laps(SESSION_KEY, DRIVER_NUMBER) if l.get('date_start')]
    return (ham_laps_timed,)


@app.cell
def _(
    CACHE_DIR,
    DRIVER_NUMBER,
    SESSION_KEY,
    USE_CACHE,
    get_car_data_slow,
    ham_laps_timed,
    pickle,
):
    _cache_file = CACHE_DIR / 'ham_psd_rows.pkl'
    if USE_CACHE and _cache_file.exists():
        with open(_cache_file, 'rb') as _f:
            ham_psd_rows, ham_psd_failures = pickle.load(_f)
    else:
        ham_psd_rows = []
        ham_psd_failures = []
        for _i in range(len(ham_laps_timed) - 1):
            _lap_num = ham_laps_timed[_i].get('lap_number', _i + 1)
            try:
                _date_start = ham_laps_timed[_i]['date_start']
                _date_end = ham_laps_timed[_i + 1]['date_start']
                _raw = get_car_data_slow(
                    session_key=SESSION_KEY,
                    driver_number=DRIVER_NUMBER,
                    date_start=_date_start,
                    date_end=_date_end,
                )
                _result = compute_psd_stats(_raw)
                if _result is None:
                    ham_psd_failures.append(_lap_num)
                    continue
                _freqs, _psd, _frac, _peak = _result
                ham_psd_rows.append({
                    'lap': _lap_num,
                    'peak_freq_hz': round(_peak, 4),
                    'frac_below_cutoff': round(_frac, 4),
                    'pct_below_cutoff': round(_frac * 100, 2),
                    'freqs': _freqs,
                    'psd': _psd,
                })
            except Exception as _e:
                ham_psd_failures.append(_lap_num)
        with open(_cache_file, 'wb') as _f:
            pickle.dump((ham_psd_rows, ham_psd_failures), _f)
    return ham_psd_failures, ham_psd_rows


@app.cell
def _(ham_psd_failures, ham_psd_rows, mo):
    import pandas as _pd_s7
    _df = _pd_s7.DataFrame([{k: v for k, v in r.items() if k not in ('freqs', 'psd')} for r in ham_psd_rows])
    _mean_pct = _df['pct_below_cutoff'].mean()
    _min_pct = _df['pct_below_cutoff'].min()
    _max_pct = _df['pct_below_cutoff'].max()
    _range_pp = _max_pct - _min_pct
    mo.md(f"""
    **{len(ham_psd_rows)} laps** processed successfully; **{len(ham_psd_failures)} failed** (insufficient data): {ham_psd_failures}

    | Stat | Value |
    |---|---|
    | Mean % power below 1.25 Hz | {_mean_pct:.2f}% |
    | Min % power below 1.25 Hz | {_min_pct:.2f}% |
    | Max % power below 1.25 Hz | {_max_pct:.2f}% |
    | Range (pp) | {_range_pp:.2f} pp |

    A range of **{_range_pp:.2f} pp** {'is well within the 5 pp stability target ✓' if _range_pp < 5 else 'exceeds the 5 pp stability target — inspect outlier laps'}.
    """)
    return


@app.cell
def _(ham_psd_rows, mo):
    import pandas as _pd_s7b
    _rows = [{k: v for k, v in r.items() if k not in ('freqs', 'psd')} for r in ham_psd_rows]
    _df = _pd_s7b.DataFrame(_rows)
    mo.md(
        "| Lap | Peak freq (Hz) | % power below 1.25 Hz |\n|---|---|---|\n" +
        "\n".join(f"| {int(r['lap'])} | {r['peak_freq_hz']:.4f} | {r['pct_below_cutoff']:.2f}% |" for _, r in _df.iterrows())
    )
    return


@app.cell
def _(ham_psd_rows, plt):
    _cmap = plt.get_cmap('plasma')
    _n = len(ham_psd_rows)
    _fig_s7, _ax_s7 = plt.subplots()
    for _idx, _row in enumerate(ham_psd_rows):
        _ax_s7.semilogy(_row['freqs'], _row['psd'], color=_cmap(_idx / max(_n - 1, 1)), alpha=0.7, linewidth=0.8)
    _sm = plt.cm.ScalarMappable(cmap='plasma', norm=plt.Normalize(vmin=ham_psd_rows[0]['lap'], vmax=ham_psd_rows[-1]['lap']))
    _sm.set_array([])
    _fig_s7.colorbar(_sm, ax=_ax_s7, label='Lap number')
    _ax_s7.axvline(1.25, color='orange', linestyle='--', label='Cutoff: 1.25 Hz')
    _ax_s7.axvline(2.0, color='grey', linestyle=':', label='Nyquist: 2.0 Hz')
    _ax_s7.set_xlabel('Frequency (Hz)')
    _ax_s7.set_ylabel('PSD (kph² / Hz)')
    _ax_s7.set_title('PSD overlay — all HAM race laps (colour = lap number)')
    _ax_s7.legend(fontsize=8)
    _fig_s7
    return


@app.cell
def _(ham_psd_rows, np, plt):
    _laps = [r['lap'] for r in ham_psd_rows]
    _pcts = [r['pct_below_cutoff'] for r in ham_psd_rows]
    _mean_line = np.mean(_pcts)
    _fig_s7b, _ax_s7b = plt.subplots()
    _ax_s7b.plot(_laps, _pcts, marker='o', markersize=3, linewidth=1)
    _ax_s7b.axhline(_mean_line, color='orange', linestyle='--', label=f'Mean: {_mean_line:.1f}%')
    _ax_s7b.set_xlabel('Lap number')
    _ax_s7b.set_ylabel('% power below 1.25 Hz')
    _ax_s7b.set_title('Power retained below cutoff per lap — HAM full race')
    _ax_s7b.legend()
    _fig_s7b
    return


@app.cell
def _(mo):
    mo.md("""
    ---

    ## 8. Cross-Driver PSD Consistency

    Section 7 confirms spectral stability across the full race for Hamilton. Here we check whether
    the same pipeline parameters generalise to **all 20 drivers**. We reuse the laps 3–7 window
    from Section 3 (same `get_laps` calls, already cached). Car data is fetched via the rate-limited
    pipeline API. Each driver's data passes through the same `compute_psd_stats` pipeline.

    If the 1.25 Hz cutoff is universal, all drivers should show:
    - A low-frequency-dominated PSD shape (peak well below 1.25 Hz)
    - ≥ 90% of power retained below the cutoff
    - No driver significantly outside the cluster
    """)
    return


@app.cell
def _(
    CACHE_DIR,
    SESSION_KEY,
    USE_CACHE,
    drivers,
    get_car_data_slow,
    get_laps,
    pickle,
):
    _cache_file = CACHE_DIR / 'cross_psd_rows.pkl'
    if USE_CACHE and _cache_file.exists():
        with open(_cache_file, 'rb') as _f:
            cross_psd_rows, cross_psd_failures = pickle.load(_f)
    else:
        _cross_rows = []
        _cross_failures = []
        for _drv in drivers:
            _dn = _drv['driver_number']
            _acro = _drv['name_acronym']
            try:
                _laps = get_laps(SESSION_KEY, _dn)
                _timed = [l for l in _laps if not l.get('is_pit_out_lap') and l.get('date_start')]
                if len(_timed) < 5:
                    _cross_failures.append(_acro)
                    continue
                _start = _timed[2]['date_start']
                _end = _timed[6]['date_start'] if len(_timed) > 6 else _timed[-1]['date_start']
                _raw = get_car_data_slow(SESSION_KEY, _dn, _start, _end)
                _result = compute_psd_stats(_raw)
                if _result is None:
                    _cross_failures.append(_acro)
                    continue
                _freqs, _psd, _frac, _peak = _result
                _cross_rows.append({
                    'driver': _acro,
                    'team': _drv['team_name'],
                    'team_colour': '#' + (_drv.get('team_colour') or 'aaaaaa'),
                    'peak_freq_hz': round(_peak, 4),
                    'frac_below_cutoff': round(_frac, 4),
                    'pct_below_cutoff': round(_frac * 100, 2),
                    'freqs': _freqs,
                    'psd': _psd,
                })
            except Exception:
                _cross_failures.append(_acro)
        cross_psd_rows = _cross_rows
        cross_psd_failures = _cross_failures
        with open(_cache_file, 'wb') as _f:
            pickle.dump((cross_psd_rows, cross_psd_failures), _f)
    return cross_psd_failures, cross_psd_rows


@app.cell
def _(cross_psd_failures, cross_psd_rows, mo):
    import pandas as _pd_s8
    _df8 = _pd_s8.DataFrame([{k: v for k, v in r.items() if k not in ('freqs', 'psd', 'team_colour')} for r in cross_psd_rows])
    _mean8 = _df8['pct_below_cutoff'].mean()
    _min8 = _df8['pct_below_cutoff'].min()
    _max8 = _df8['pct_below_cutoff'].max()
    mo.md(f"""
    **{len(cross_psd_rows)} drivers** processed successfully; **{len(cross_psd_failures)} excluded** (insufficient data): {cross_psd_failures}

    | Stat | Value |
    |---|---|
    | Mean % power below 1.25 Hz | {_mean8:.2f}% |
    | Min % power below 1.25 Hz | {_min8:.2f}% |
    | Max % power below 1.25 Hz | {_max8:.2f}% |

    | Driver | Team | Peak freq (Hz) | % power below 1.25 Hz |
    |---|---|---|---|
    """ + "\n".join(
        f"| {r['driver']} | {r['team']} | {r['peak_freq_hz']:.4f} | {r['pct_below_cutoff']:.2f}% |"
        for r in sorted(cross_psd_rows, key=lambda x: x['pct_below_cutoff'], reverse=True)
    ))
    return


@app.cell
def _(cross_psd_rows, plt):
    _fig_s8, _ax_s8 = plt.subplots()
    for _row in cross_psd_rows:
        _ax_s8.semilogy(_row['freqs'], _row['psd'], color=_row['team_colour'], alpha=0.75, linewidth=0.9, label=_row['driver'])
    _ax_s8.axvline(1.25, color='orange', linestyle='--', label='Cutoff: 1.25 Hz', zorder=10)
    _ax_s8.axvline(2.0, color='grey', linestyle=':', label='Nyquist: 2.0 Hz', zorder=10)
    _ax_s8.set_xlabel('Frequency (Hz)')
    _ax_s8.set_ylabel('PSD (kph² / Hz)')
    _ax_s8.set_title('PSD overlay — all drivers, laps 3–7 (colour = team colour)')
    _ax_s8.legend(fontsize=6, ncol=2, loc='upper right', bbox_to_anchor=(1.18, 1))
    _fig_s8
    return


@app.cell
def _(cross_psd_rows, np, plt):
    _sorted = sorted(cross_psd_rows, key=lambda x: x['pct_below_cutoff'])
    _drivers_sorted = [r['driver'] for r in _sorted]
    _pcts_sorted = [r['pct_below_cutoff'] for r in _sorted]
    _colours_sorted = [r['team_colour'] for r in _sorted]
    _mean_all = np.mean(_pcts_sorted)
    _fig_s8b, _ax_s8b = plt.subplots(figsize=(12, 5))
    _ax_s8b.bar(_drivers_sorted, _pcts_sorted, color=_colours_sorted)
    _ax_s8b.axhline(_mean_all, color='orange', linestyle='--', label=f'Mean: {_mean_all:.1f}%')
    _ax_s8b.set_xlabel('Driver')
    _ax_s8b.set_ylabel('% power below 1.25 Hz')
    _ax_s8b.set_title('Power retained below cutoff — all drivers, laps 3–7')
    _ax_s8b.legend()
    _fig_s8b
    return


@app.cell
def _(mo):
    mo.md("""
    ---

    ## Conclusion: Pipeline Generalisation Evidence

    | Claim | Evidence |
    |---|---|
    | 4 Hz resample is appropriate | Source ~3.7 Hz irregular; 4 Hz is minimally super-Nyquist, no phantom information added |
    | 1.25 Hz cutoff retains signal | HAM laps 1–10: >95% power below cutoff (Section 5) |
    | Cutoff stable across full race | Section 7: per-lap % retained varies < 5 pp across all HAM race laps |
    | Cutoff applies to all drivers | Section 8: all ~20 drivers show same low-frequency-dominated structure; all within a narrow band |

    A single pipeline configuration — PCHIP resample to 4 Hz, 4th-order Butterworth at 1.25 Hz —
    can be applied universally to produce acceleration and jerk signals for all drivers across
    the full race.
    """)
    return


if __name__ == "__main__":
    app.run()
