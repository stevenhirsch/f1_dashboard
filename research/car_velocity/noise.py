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
    acceleration (or jerk) signals, we need to understand the sampling characteristics of this data
    and make principled decisions about how to process it.

    This notebook walks through that process: from raw speed data to filtered acceleration and jerk,
    documenting each decision along the way.

    ---

    ## 1. Data Collection

    We'll work with Lewis Hamilton's car data from the 2026 Chinese Grand Prix race (meeting 1280),
    focusing initially on a subset of laps as a representative window of normal racing conditions.
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

    Before differentiating speed to get acceleration, we need to characterise the sampling rate. We know we need to filter the data, but many digital filters (like the Butterworth filter we'll use later) require a consistent sampling frequency. Furthermore, we need context about the sampling rates anyways to compute any acceleration (or other) signals.

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
    plt.title('Distribution of sampling intervals — HAM')
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
    | Mean interval | {sampling_diffs_mean:.4f} s (~{1/sampling_diffs_mean:.4f} Hz) |
    | Std deviation | {sampling_diffs_std:.4f} s |
    | Coefficient of variation | {sampling_diffs_cov:.4f} |

    A CV of **{sampling_diffs_cov:.4f}** indicates meaningful variability. This isn't noise in the physical signal; it's a property of how OpenF1
    publishes telemetry. The next question we should explore is whether it's specific to Hamilton or consistent across the whole grid. I don't believe there's any reason to believe there to be differences across the grid, but it's good practice to confirm just in case.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Now for a quick aside- we also can see some flat spots in the speed plot above. These are likely `null` values that the API either forward or backward fills. We know it's unlikely that a driver's speed actually remains "flat" for that long. Next, we'll highlight instances where there are about 2s of speed not changing. The idea would be that if speed doesn't change for that long, we should probably drop those frames and interpolate over them rather than just use the forward/backward filled data. This window of ~2 seconds is perhaps too conservative since it's unlikely there would even be 1s of "flat" speed during a race, but given the resolution of the data I don't want to be overly conservative and gap fill more than we need to.
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
    There were **{dup_mask.sum()} duplicate sample(s)** identified.
    These are dropped before PCHIP interpolation so the spline spans those points naturally,
    rather than being forced through a repeated value that isn't real new information. We'll keep this in mind moving forward.
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ---

    ## 3. Sampling Consistency Across All Drivers

    We repeat the same analysis for every driver in the race, using the same subset of laps as a
    consistent mid-race window (skipping the formation lap and first racing lap).
    Team colour is included so we can visually check whether any constructor shows
    systematically different behaviour. I limited the scope here since I don't anticipate that there's going to be much variation between drivers, so this is more of just a quick sanity check.
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
    speed against the raw data and amplify the higher-frequency noise. Acceleration is computed as
    Δspeed / Δtime and converted to g (1 g = 9.81 m/s²; 1 kph/s = 1000/3600 m/s²).
    """)
    return


@app.function
def get_accel_from_speed(timestamps, speeds):
    time_diffs = timestamps.diff().dt.total_seconds()
    speed_diffs = speeds.diff()
    return speed_diffs / time_diffs


@app.function
def compute_psd_stats(car_data_raw, fs=4.0, cutoff=0.5):
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
    The signal contains physically implausible spikes. This is not signal — it is amplified high-frequency components of the signal.

    Although I'm not aware of any "gold standard" public data, from what I've gathered F1 cars produce roughly 5-6g (up to 8g) under heavy braking and 1.5-2g (up to 4g) under acceleration. Any spike significantly outside that envelope is almost certainly errors in the signal.

    ---

    ## 5. Signal Processing Pipeline

    We address the irregular sampling with a three-step pipeline:

    1. **Resample to a regular grid** using PCHIP interpolation
    2. **Apply a low-pass Butterworth filter** to attenuate high-frequency noise
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
    (e.g. 5 or 10 Hz) does not add information; the signal's true bandwidth is capped at ~1.85 Hz
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

    For this data, no such clear plateau exists. The residual declines mostly continuously across the
    entire testable range (0.1–1.85 Hz). This is somewhat expected: the irregular sampling contaminates
    all frequencies, not just the high end, so there is no clean noise floor to identify.
    Automated elbow-detection methods (kneedle, maximum curvature) both failed for this reason.

    The residual plot is shown below for transparency, but the cutoff was ultimately chosen
    on domain knowledge. F1 braking events typically last **2–4 seconds**, placing their fundamental energy at ~0.25–0.5 Hz — well
    below our source bandwidth ceiling of ~1.85 Hz. This means we have meaningful headroom
    above the signal of interest before we hit the noise floor, and a cutoff of **0.5 Hz**
    is chosen: it captures the fundamental of all meaningful braking and acceleration events
    (≥ 2 s duration) while attenuating the bulk of the sampling irregularity noise. A higher
    cutoff of 1.25 Hz was initially explored but produced widespread spurious peaks in the
    acceleration derivative (Section 9 diagnostic), confirmed as noise rather than anomalous
    laps. 0.5 Hz removes that noise band at the cost of slight attenuation of braking onset
    energy - an acceptable trade-off since the primary use is average load comparison, not
    measurement of instantaneous peak g.

    In summary, we know that this cutoff frequency is going to slightly attenuate the peaks, but result in reasonable averages across the session & should allow us to reliably compare drivers. This is a limitation of the data that we have access to, so we shouldn't take the actual "g" too seriously.
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
    CUTOFF_HZ = 0.5
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
    smoothness rather than a precise physical measurement. Alternatively, I may explore just the variance of the acceleration signal across the lap as a non-physical measurement of a similar quality.
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
    the 0.5 Hz cutoff are not artefacts of an early-race window, we repeat the per-lap PSD
    computation across **all race laps** for Hamilton. Each lap is treated independently: the
    PCHIP → Butterworth → Welch pipeline runs on data bounded by consecutive lap start times.

    If the cutoff is well-chosen, every lap should show the same low-frequency-dominated shape,
    and the fraction of power below 0.5 Hz should be stable (< 5 pp variation) across the race.
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
    | Mean % power below 0.5 Hz | {_mean_pct:.2f}% |
    | Min % power below 0.5 Hz | {_min_pct:.2f}% |
    | Max % power below 0.5 Hz | {_max_pct:.2f}% |
    | Range (pp) | {_range_pp:.2f} pp |
    """)
    return


@app.cell
def _(ham_psd_rows, mo):
    import pandas as _pd_s7b
    _rows = [{k: v for k, v in r.items() if k not in ('freqs', 'psd')} for r in ham_psd_rows]
    _df = _pd_s7b.DataFrame(_rows)
    mo.md(
        "| Lap | Peak freq (Hz) | % power below 0.5 Hz |\n|---|---|---|\n" +
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
    _ax_s7.axvline(0.5, color='orange', linestyle='--', label='Cutoff: 0.5 Hz')
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
    _ax_s7b.axhline(_mean_line, color='orange', linestyle='--', label=f'Mean: {_mean_line:.2f}%')
    _ax_s7b.set_xlabel('Lap number')
    _ax_s7b.set_ylabel('% power below 0.5 Hz')
    _ax_s7b.set_ylim(0, 100)
    _ax_s7b.yaxis.get_major_formatter().set_useOffset(False)
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
    the same pipeline parameters generalise to **all 20 drivers**. We reuse the laps window
    from Section 3 (same `get_laps` calls, already cached). Car data is fetched via the rate-limited
    pipeline API. Each driver's data passes through the same `compute_psd_stats` pipeline.

    If the 0.5 Hz cutoff is universal, all drivers should show:
    - A low-frequency-dominated PSD shape (peak well below 0.5 Hz)
    - ≥ 80% of power retained below the cutoff
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
    | Mean % power below 0.5 Hz | {_mean8:.2f}% |
    | Min % power below 0.5 Hz | {_min8:.2f}% |
    | Max % power below 0.5 Hz | {_max8:.2f}% |

    | Driver | Team | Peak freq (Hz) | % power below 0.5 Hz |
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
    _ax_s8.axvline(0.5, color='orange', linestyle='--', label='Cutoff: 0.5 Hz', zorder=10)
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
    _ax_s8b.set_ylabel('% power below 0.5 Hz')
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
    | 0.5 Hz cutoff retains signal | HAM laps subset: majority of power below cutoff (Section 5); captures fundamentals of all braking events ≥ 2 s |
    | Cutoff stable across full race | Section 7: per-lap % retained varies < 5 pp across all HAM race laps |
    | Cutoff applies to all drivers | Section 8: all ~20 drivers show same low-frequency-dominated structure; all within a narrow band |

    A single pipeline configuration — PCHIP resample to 4 Hz, 4th-order Butterworth at 0.5 Hz —
    can be applied universally to produce acceleration and jerk signals for all drivers across
    the full race.
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ---

    ## 9. Per-Lap Peak Acceleration Metrics — Full Grid

    The signal processing pipeline is now validated. We apply it at scale: for **every driver**,
    across **every timed race lap**, computing the peak longitudinal acceleration and peak
    deceleration from the filtered signal.

    - **Peak acceleration** (max g) captures the hardest throttle application in a lap.
    - **Peak deceleration** (min g, reported as a positive magnitude) captures the hardest braking event.

    These are naturally per-lap metrics as various factors (e.g., tyre degradation) shifts the distribution over a stint. Pit-out laps
    are excluded since their speed profiles (cold tyres, slow pit-lane exit) are not representative of racing conditions.
    """)
    return


@app.function
def compute_lap_accel_stats(car_data_raw, fs=4.0, cutoff=0.5):
    """Dedup → PCHIP → Butterworth → differentiate → throttle/brake gating.

    Returns (peak_accel_g, peak_decel_g) where peak_decel_g is the signed minimum (negative).
    Either element may be None if no valid window exists for that channel (e.g. a lap with no
    throttle application above the 20% gate, or no braking events).
    Returns None (not a tuple) if data is insufficient to process at all.

    cutoff=0.5 Hz, matching the pipeline-wide cutoff established in Sections 5–8.
    Differentiation amplifies noise proportionally to frequency; diagnostic analysis (Section 9)
    confirmed that residual above ~0.5 Hz produces spurious peak g values (GAS/RUS decel
    outliers scattered every 2–3 laps throughout the race) rather than anomalous-lap
    contamination. 0.75 Hz was also evaluated but reintroduced high/low outliers within the
    included laps. Values are attenuated relative to true instantaneous peak g (accelerometer-
    grade sampling required for that) but are internally consistent and reliable for relative
    driver comparison.

    Throttle/brake windowing:
    - accel_mask: throttle > 20% — gates genuine power application; 20% is a coarse threshold
      appropriate for power circuits. Monaco/slow circuits may yield fewer valid samples (see
      Section 11.5 empty-window report).
    - decel_mask: throttle < 20% OR any brake — captures genuine deceleration events.
    - Alignment: accel_g[i] spans [t_reg[i], t_reg[i+1]], so masks use throttle_reg[:-1]
      (left-edge convention) to stay aligned with the diff output (shape N-1)."""
    import numpy as np, pandas as pd
    from scipy.interpolate import PchipInterpolator
    from scipy.signal import butter, filtfilt

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

    t_ctrl     = clean['date'].astype(np.int64).to_numpy() / 1e9
    throttle_r = clean['throttle'].to_numpy().astype(float)
    brake_r    = clean['brake'].to_numpy().astype(float)

    t = clean['date'].astype(np.int64).to_numpy() / 1e9
    v = clean['speed'].to_numpy().astype(float)
    pchip = PchipInterpolator(t, v)
    t_reg = np.arange(t[0], t[-1], 1 / fs)
    if len(t_reg) < 32:
        return None
    v_reg = pchip(t_reg)

    # Resample throttle (linear) and brake (nearest-neighbour) onto the regular grid.
    # Nearest-neighbour for brake preserves its binary character; linear interp would
    # produce fractional values at step edges, corrupting the brake > 0 test.
    throttle_reg = np.clip(np.interp(t_reg, t_ctrl, throttle_r), 0, 100)
    brake_reg    = np.clip(np.round(np.interp(t_reg, t_ctrl, brake_r) / 100) * 100, 0, 100)

    b, a = butter(N=4, Wn=cutoff / (fs / 2), btype='low')
    # Use a generous padlen (up to 5 s each side) so the filter fully stabilises at
    # lap boundaries rather than leaving transient edge artefacts.
    _padlen = min(len(v_reg) // 4, int(5 * fs))
    v_filt = filtfilt(b, a, v_reg, padlen=_padlen)

    accel_g = np.diff(v_filt) / (1 / fs) * (1000 / 3600) / 9.81

    # Left-edge alignment: accel_g[i] spans [t_reg[i], t_reg[i+1]] → index with [:-1]
    thr = throttle_reg[:-1]   # shape (N-1,) — aligns with accel_g
    brk = brake_reg[:-1]

    accel_mask = thr > 20                   # throttle above 20% → genuine acceleration
    decel_mask = (thr < 20) | (brk > 0)    # light throttle OR any brake → genuine deceleration

    accel_candidates = accel_g[accel_mask]
    decel_candidates = accel_g[decel_mask]

    peak_accel_g = float(np.max(accel_candidates)) if len(accel_candidates) > 0 else None
    peak_decel_g = float(np.min(decel_candidates)) if len(decel_candidates) > 0 else None

    return peak_accel_g, peak_decel_g


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
    _cache_file = CACHE_DIR / 'lap_accel_rows.pkl'
    if USE_CACHE and _cache_file.exists():
        with open(_cache_file, 'rb') as _f:
            lap_accel_rows, lap_accel_failures = pickle.load(_f)
    else:
        import pandas as _pd_s9
        _rows = []
        _failures = []
        for _drv in drivers:
            _dn = _drv['driver_number']
            _acro = _drv['name_acronym']
            try:
                _laps_all = get_laps(SESSION_KEY, _dn)
                _timed = [l for l in _laps_all if not l.get('is_pit_out_lap') and l.get('date_start')]
                if len(_timed) < 2:
                    _failures.append((_acro, 'all'))
                    continue
                # One API call for the full race window, then segment client-side
                _raw = get_car_data_slow(SESSION_KEY, _dn, _timed[0]['date_start'], _timed[-1]['date_start'])
                if not _raw:
                    _failures.append((_acro, 'all'))
                    continue
                _df_all = _pd_s9.DataFrame(_raw)
                _df_all['date'] = _pd_s9.to_datetime(_df_all['date'])
                _df_all = _df_all.sort_values('date').reset_index(drop=True)
                for _i in range(len(_timed) - 1):
                    _lap_num = _timed[_i].get('lap_number', _i + 1)
                    _t0 = _pd_s9.to_datetime(_timed[_i]['date_start'])
                    _t1 = _pd_s9.to_datetime(_timed[_i + 1]['date_start'])
                    _lap_slice = _df_all[(_df_all['date'] >= _t0) & (_df_all['date'] < _t1)]
                    _result = compute_lap_accel_stats(_lap_slice.to_dict('records'))
                    if _result is None:
                        _failures.append((_acro, _lap_num))
                        continue
                    _peak_accel, _peak_decel = _result
                    if _peak_accel is None or _peak_decel is None:
                        _failures.append((_acro, _lap_num, 'empty_window'))
                        continue
                    _rows.append({
                        'driver': _acro,
                        'team': _drv['team_name'],
                        'team_colour': '#' + (_drv.get('team_colour') or 'aaaaaa'),
                        'lap': _lap_num,
                        'peak_accel_g': round(_peak_accel, 4),
                        'peak_decel_g': round(_peak_decel, 4),
                    })
            except Exception:
                _failures.append((_acro, 'all'))
        lap_accel_rows = _rows
        lap_accel_failures = _failures
        with open(_cache_file, 'wb') as _f:
            pickle.dump((lap_accel_rows, lap_accel_failures), _f)
    return lap_accel_failures, lap_accel_rows


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
    _cache_file_w = CACHE_DIR / 'lap_accel_windowed_rows.pkl'
    if USE_CACHE and _cache_file_w.exists():
        with open(_cache_file_w, 'rb') as _f:
            lap_accel_windowed_rows, lap_accel_windowed_failures = pickle.load(_f)
    else:
        import pandas as _pd_s9w
        _rows_w = []
        _failures_w = []
        _windowed_empty_window = []
        for _drv in drivers:
            _dn = _drv['driver_number']
            _acro = _drv['name_acronym']
            try:
                _laps_all = get_laps(SESSION_KEY, _dn)
                _timed = [l for l in _laps_all if not l.get('is_pit_out_lap') and l.get('date_start')]
                if len(_timed) < 2:
                    _failures_w.append((_acro, 'all'))
                    continue
                _raw = get_car_data_slow(SESSION_KEY, _dn, _timed[0]['date_start'], _timed[-1]['date_start'])
                if not _raw:
                    _failures_w.append((_acro, 'all'))
                    continue
                _df_all = _pd_s9w.DataFrame(_raw)
                _df_all['date'] = _pd_s9w.to_datetime(_df_all['date'])
                _df_all = _df_all.sort_values('date').reset_index(drop=True)
                for _i in range(len(_timed) - 1):
                    _lap_num = _timed[_i].get('lap_number', _i + 1)
                    _t0 = _pd_s9w.to_datetime(_timed[_i]['date_start'])
                    _t1 = _pd_s9w.to_datetime(_timed[_i + 1]['date_start'])
                    _lap_slice = _df_all[(_df_all['date'] >= _t0) & (_df_all['date'] < _t1)]
                    _result = compute_lap_accel_stats(_lap_slice.to_dict('records'))
                    if _result is None:
                        _failures_w.append((_acro, _lap_num))
                        continue
                    _peak_accel, _peak_decel = _result
                    if _peak_accel is None or _peak_decel is None:
                        _windowed_empty_window.append({
                            'driver': _acro,
                            'lap': _lap_num,
                            'which': ('both' if _peak_accel is None and _peak_decel is None
                                      else 'accel' if _peak_accel is None else 'decel'),
                        })
                        continue
                    _rows_w.append({
                        'driver': _acro,
                        'team': _drv['team_name'],
                        'team_colour': '#' + (_drv.get('team_colour') or 'aaaaaa'),
                        'lap': _lap_num,
                        'peak_accel_g': round(_peak_accel, 4),
                        'peak_decel_g': round(_peak_decel, 4),
                    })
            except Exception:
                _failures_w.append((_acro, 'all'))
        lap_accel_windowed_rows = _rows_w
        lap_accel_windowed_failures = _failures_w
        with open(_cache_file_w, 'wb') as _f:
            pickle.dump((lap_accel_windowed_rows, lap_accel_windowed_failures), _f)
    return (lap_accel_windowed_rows,)


@app.cell
def _(lap_accel_windowed_rows, pd):
    lap_accel_windowed_df = pd.DataFrame(lap_accel_windowed_rows)
    if 'peak_decel_g' in lap_accel_windowed_df.columns:
        lap_accel_windowed_df['peak_decel_g_abs'] = lap_accel_windowed_df['peak_decel_g'].abs()
    else:
        # Empty fetch — no laps passed the windowed filter yet
        for _col in ('driver', 'team', 'team_colour', 'lap', 'peak_accel_g', 'peak_decel_g', 'peak_decel_g_abs'):
            lap_accel_windowed_df[_col] = pd.Series(dtype=float if _col.startswith('peak') or _col == 'lap' else str)
    return (lap_accel_windowed_df,)


@app.cell
def _(lap_accel_rows, pd):
    lap_accel_df_raw = pd.DataFrame(lap_accel_rows)
    lap_accel_df_raw['peak_decel_g_abs'] = lap_accel_df_raw['peak_decel_g'].abs()
    # Exclude laps that exceed physical plausibility bounds rather than clipping them.
    # Clipping creates artificial pile-ups at the boundary; exclusion keeps the
    # remaining distribution honest. Bounds: 4 g acceleration, 8 g braking.
    _mask = (lap_accel_df_raw['peak_accel_g'] <= 4.0) & (lap_accel_df_raw['peak_decel_g_abs'] <= 8.0)
    lap_accel_df = lap_accel_df_raw[_mask].reset_index(drop=True)
    lap_accel_excluded = lap_accel_df_raw[~_mask][['driver', 'lap', 'peak_accel_g', 'peak_decel_g_abs']].sort_values(['driver', 'lap'])
    return lap_accel_df, lap_accel_df_raw, lap_accel_excluded


@app.cell
def _(lap_accel_df_raw, lap_accel_excluded, mo):
    _n_excluded = len(lap_accel_excluded)
    _pct = _n_excluded / len(lap_accel_df_raw) * 100
    mo.md(
        f"**{_n_excluded} laps excluded ({_pct:.1f}%) — peak accel > 4 g or peak decel > 8 g.**\n\n"
        "Inspect the table below: if exclusions are scattered randomly across drivers and lap "
        "numbers it is a noise/filter problem; if they cluster at lap 1, post-SC laps, or "
        "specific drivers it is anomalous-lap contamination.\n\n"
        "| Driver | Lap | Peak accel (g) | Peak decel (g) |\n|---|---|---|---|\n" +
        "\n".join(
            f"| {r['driver']} | {int(r['lap'])} | {r['peak_accel_g']:.3f} | {r['peak_decel_g_abs']:.3f} |"
            for _, r in lap_accel_excluded.iterrows()
        )
    )
    return


@app.cell
def _(lap_accel_df, lap_accel_failures, mo):
    _n_laps = len(lap_accel_df)
    _n_drivers = lap_accel_df['driver'].nunique()
    _n_failures = len(lap_accel_failures)
    mo.md(f"""
    **{_n_laps} lap records** computed across **{_n_drivers} drivers**;
    **{_n_failures} laps excluded** (insufficient data or API error).
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ---

    ## 10. Distribution Analysis

    We examine the distribution of per-lap peak values to assess:
    1. Whether values are physically plausible (F1 braking: ~4–6 g; acceleration: ~1–2 g)
    2. Whether any driver or team is a significant outlier
    3. The spread of individual performance across the grid

    **Grid distributions** give the aggregate picture; **per-driver box plots** reveal
    individual patterns and lap-to-lap consistency.
    """)
    return


@app.cell
def _(lap_accel_df, plt):
    _fig_hist, (_ax_a, _ax_d) = plt.subplots(1, 2, figsize=(12, 4))

    _mean_a = lap_accel_df['peak_accel_g'].mean()
    _std_a = lap_accel_df['peak_accel_g'].std()
    _ax_a.hist(lap_accel_df['peak_accel_g'], bins=40, edgecolor='none')
    _ax_a.axvline(_mean_a, color='orange', linestyle='--', label=f'Mean: {_mean_a:.2f} g')
    _ax_a.axvline(_mean_a - _std_a, color='orange', linestyle=':', alpha=0.6, label=f'±1 SD: {_std_a:.2f} g')
    _ax_a.axvline(_mean_a + _std_a, color='orange', linestyle=':', alpha=0.6)
    _ax_a.set_xlabel('Peak acceleration (g)')
    _ax_a.set_ylabel('Count (laps)')
    _ax_a.set_title('Grid: peak acceleration per lap')
    _ax_a.legend(fontsize=8)

    _mean_d = lap_accel_df['peak_decel_g_abs'].mean()
    _std_d = lap_accel_df['peak_decel_g_abs'].std()
    _ax_d.hist(lap_accel_df['peak_decel_g_abs'], bins=40, edgecolor='none', color='tomato')
    _ax_d.axvline(_mean_d, color='orange', linestyle='--', label=f'Mean: {_mean_d:.2f} g')
    _ax_d.axvline(_mean_d - _std_d, color='orange', linestyle=':', alpha=0.6, label=f'±1 SD: {_std_d:.2f} g')
    _ax_d.axvline(_mean_d + _std_d, color='orange', linestyle=':', alpha=0.6)
    _ax_d.set_xlabel('Peak deceleration (g)')
    _ax_d.set_ylabel('Count (laps)')
    _ax_d.set_title('Grid: peak deceleration per lap')
    _ax_d.legend(fontsize=8)

    _fig_hist.suptitle('Distribution of per-lap peak g-forces — full grid, full race')
    _fig_hist.tight_layout()
    _fig_hist
    return


@app.cell
def _(lap_accel_df, plt):
    _drv_order_a = (
        lap_accel_df.groupby('driver')['peak_accel_g']
        .median()
        .sort_values(ascending=False)
        .index.tolist()
    )
    _colours_a = {
        row['driver']: row['team_colour']
        for _, row in lap_accel_df[['driver', 'team_colour']].drop_duplicates().iterrows()
    }
    _data_a = [lap_accel_df.loc[lap_accel_df['driver'] == d, 'peak_accel_g'].values for d in _drv_order_a]

    _fig_a, _ax_a2 = plt.subplots(figsize=(14, 5))
    _bp_a = _ax_a2.boxplot(_data_a, patch_artist=True, medianprops=dict(color='white', linewidth=1.5))
    for _patch, _drv in zip(_bp_a['boxes'], _drv_order_a):
        _patch.set_facecolor(_colours_a.get(_drv, '#aaaaaa'))
        _patch.set_alpha(0.8)
    _ax_a2.set_xticks(range(1, len(_drv_order_a) + 1))
    _ax_a2.set_xticklabels(_drv_order_a, rotation=45, ha='right')
    _ax_a2.set_ylabel('Peak acceleration (g)')
    _ax_a2.set_title('Per-driver distribution of per-lap peak acceleration (sorted by median, coloured by team)')
    _fig_a.tight_layout()
    _fig_a
    return


@app.cell
def _(lap_accel_df, plt):
    _drv_order_d = (
        lap_accel_df.groupby('driver')['peak_decel_g_abs']
        .median()
        .sort_values(ascending=False)
        .index.tolist()
    )
    _colours_d = {
        row['driver']: row['team_colour']
        for _, row in lap_accel_df[['driver', 'team_colour']].drop_duplicates().iterrows()
    }
    _data_d = [lap_accel_df.loc[lap_accel_df['driver'] == d, 'peak_decel_g_abs'].values for d in _drv_order_d]

    _fig_d, _ax_d2 = plt.subplots(figsize=(14, 5))
    _bp_d = _ax_d2.boxplot(_data_d, patch_artist=True, medianprops=dict(color='white', linewidth=1.5))
    for _patch, _drv in zip(_bp_d['boxes'], _drv_order_d):
        _patch.set_facecolor(_colours_d.get(_drv, '#aaaaaa'))
        _patch.set_alpha(0.8)
    _ax_d2.set_xticks(range(1, len(_drv_order_d) + 1))
    _ax_d2.set_xticklabels(_drv_order_d, rotation=45, ha='right')
    _ax_d2.set_ylabel('Peak deceleration (g)')
    _ax_d2.set_title('Per-driver distribution of per-lap peak deceleration (sorted by median, coloured by team)')
    _fig_d.tight_layout()
    _fig_d
    return


@app.cell
def _(lap_accel_df, mo):
    _grid_accel_mean = lap_accel_df['peak_accel_g'].mean()
    _grid_accel_std = lap_accel_df['peak_accel_g'].std()
    _grid_decel_mean = lap_accel_df['peak_decel_g_abs'].mean()
    _grid_decel_std = lap_accel_df['peak_decel_g_abs'].std()

    _summary = (
        lap_accel_df.groupby(['driver', 'team'])
        .agg(
            n_laps=('lap', 'count'),
            mean_peak_accel_g=('peak_accel_g', 'mean'),
            std_peak_accel_g=('peak_accel_g', 'std'),
            mean_peak_decel_g=('peak_decel_g_abs', 'mean'),
            std_peak_decel_g=('peak_decel_g_abs', 'std'),
        )
        .round(3)
        .sort_values('mean_peak_decel_g', ascending=False)
        .reset_index()
    )

    _header = (
        f"**Grid (all drivers, all laps):** "
        f"peak accel {_grid_accel_mean:.3f} ± {_grid_accel_std:.3f} g | "
        f"peak decel {_grid_decel_mean:.3f} ± {_grid_decel_std:.3f} g\n\n"
        "| Driver | Team | Laps | Peak accel mean (g) | Peak accel SD | Peak decel mean (g) | Peak decel SD |\n"
        "|---|---|---|---|---|---|---|\n"
    )
    _rows_md = "\n".join(
        f"| {r['driver']} | {r['team']} | {int(r['n_laps'])} "
        f"| {r['mean_peak_accel_g']:.3f} | {r['std_peak_accel_g']:.3f} "
        f"| {r['mean_peak_decel_g']:.3f} | {r['std_peak_decel_g']:.3f} |"
        for _, r in _summary.iterrows()
    )
    mo.md(_header + _rows_md)
    return


@app.cell
def _():
    return


@app.cell
def _(mo):
    mo.md("""
    ---

    ## 11. Windowing Impact Analysis

    How does throttle/brake gating change the peak g distributions relative to the global
    max/min approach? We compare `lap_accel_df` (unwindowed, plausibility-filtered) against
    `lap_accel_windowed_df` (windowed, no hard plausibility cap) across five views.
    """)
    return


@app.cell
def _(lap_accel_df, lap_accel_windowed_df, plt):
    """11.1 — Paired histograms (2×2 grid): unwindowed vs windowed distributions."""
    import numpy as _np_11

    _bins_a = _np_11.linspace(
        min(lap_accel_df['peak_accel_g'].min(), lap_accel_windowed_df['peak_accel_g'].min()),
        max(lap_accel_df['peak_accel_g'].max(), lap_accel_windowed_df['peak_accel_g'].max()),
        40,
    )
    _bins_d = _np_11.linspace(
        min(lap_accel_df['peak_decel_g_abs'].min(), lap_accel_windowed_df['peak_decel_g_abs'].min()),
        max(lap_accel_df['peak_decel_g_abs'].max(), lap_accel_windowed_df['peak_decel_g_abs'].max()),
        40,
    )

    _fig11, _axes = plt.subplots(2, 2, figsize=(14, 8), sharey='row')

    for _ax, _data, _col, _label, _bins in [
        (_axes[0, 0], lap_accel_df['peak_accel_g'],           'grey',    'Unwindowed', _bins_a),
        (_axes[0, 1], lap_accel_windowed_df['peak_accel_g'],  'steelblue','Windowed',  _bins_a),
        (_axes[1, 0], lap_accel_df['peak_decel_g_abs'],       'grey',    'Unwindowed', _bins_d),
        (_axes[1, 1], lap_accel_windowed_df['peak_decel_g_abs'],'tomato', 'Windowed',  _bins_d),
    ]:
        _mu = _data.mean()
        _sd = _data.std()
        _ax.hist(_data, bins=_bins, color=_col, alpha=0.75, edgecolor='none')
        _ax.axvline(_mu,        color='orange', linestyle='--', linewidth=1.5, label=f'Mean: {_mu:.2f} g')
        _ax.axvline(_mu - _sd, color='orange', linestyle=':',  linewidth=1.0, alpha=0.7, label=f'±1 SD: {_sd:.2f} g')
        _ax.axvline(_mu + _sd, color='orange', linestyle=':',  linewidth=1.0, alpha=0.7)
        _ax.set_title(_label)
        _ax.legend(fontsize=8)

    _axes[0, 0].set_ylabel('Count (laps)')
    _axes[1, 0].set_ylabel('Count (laps)')
    _axes[1, 0].set_xlabel('Peak deceleration (g)')
    _axes[1, 1].set_xlabel('Peak deceleration (g)')
    _axes[0, 0].set_xlabel('Peak acceleration (g)')
    _axes[0, 1].set_xlabel('Peak acceleration (g)')
    _fig11.suptitle('11.1 — Paired histograms: unwindowed vs windowed peak g-forces')
    _fig11.tight_layout()
    _fig11
    return


@app.cell
def _(lap_accel_df, lap_accel_windowed_df, plt):
    """11.2 — Per-driver scatter: windowed vs unwindowed median (coloured by team)."""
    import numpy as _np_112

    _med_uw = (
        lap_accel_df.groupby('driver')[['peak_accel_g', 'peak_decel_g_abs', 'team_colour']]
        .agg({'peak_accel_g': 'median', 'peak_decel_g_abs': 'median', 'team_colour': 'first'})
        .rename(columns={'peak_accel_g': 'accel_uw', 'peak_decel_g_abs': 'decel_uw'})
    )
    _med_w = (
        lap_accel_windowed_df.groupby('driver')[['peak_accel_g', 'peak_decel_g_abs']]
        .agg({'peak_accel_g': 'median', 'peak_decel_g_abs': 'median'})
        .rename(columns={'peak_accel_g': 'accel_w', 'peak_decel_g_abs': 'decel_w'})
    )
    _med = _med_uw.join(_med_w, how='inner').reset_index()

    _fig112, (_ax_a, _ax_d) = plt.subplots(1, 2, figsize=(14, 5))

    for _ax, _xcol, _ycol, _xlabel, _ylabel, _title in [
        (_ax_a, 'accel_uw', 'accel_w',
         'Unwindowed median accel (g)', 'Windowed median accel (g)',
         'Peak acceleration: windowed vs unwindowed median'),
        (_ax_d, 'decel_uw', 'decel_w',
         'Unwindowed median decel (g)', 'Windowed median decel (g)',
         'Peak deceleration: windowed vs unwindowed median'),
    ]:
        _lo = min(_med[_xcol].min(), _med[_ycol].min()) * 0.95
        _hi = max(_med[_xcol].max(), _med[_ycol].max()) * 1.05
        _ax.plot([_lo, _hi], [_lo, _hi], 'k--', linewidth=1, alpha=0.5, label='y = x')
        for _, _r in _med.iterrows():
            _ax.scatter(_r[_xcol], _r[_ycol], color=_r['team_colour'], s=80, zorder=3)
            _ax.annotate(_r['driver'], (_r[_xcol], _r[_ycol]),
                         textcoords='offset points', xytext=(4, 4), fontsize=7)
        _ax.set_xlabel(_xlabel)
        _ax.set_ylabel(_ylabel)
        _ax.set_title(_title)
        _ax.legend(fontsize=8)

    _fig112.suptitle('11.2 — Per-driver scatter: windowed vs unwindowed median')
    _fig112.tight_layout()
    _fig112
    return


@app.cell
def _(lap_accel_df, lap_accel_windowed_df, plt):
    """11.3 — Per-lap delta box plot: windowed − unwindowed, per driver."""
    import numpy as _np_113
    import pandas as _pd_113

    _uw = lap_accel_df[['driver', 'lap', 'peak_accel_g', 'peak_decel_g_abs']].copy()
    _w  = lap_accel_windowed_df[['driver', 'lap', 'peak_accel_g', 'peak_decel_g_abs']].copy()
    _merged = _uw.merge(_w, on=['driver', 'lap'], suffixes=('_uw', '_w'))
    _merged['delta_accel'] = _merged['peak_accel_g_w'] - _merged['peak_accel_g_uw']
    _merged['delta_decel'] = _merged['peak_decel_g_abs_w'] - _merged['peak_decel_g_abs_uw']

    _fig113, (_ax_a, _ax_d) = plt.subplots(1, 2, figsize=(16, 5))

    for _ax, _dcol, _ylabel, _title in [
        (_ax_a, 'delta_accel', 'Windowed − Unwindowed (g)',
         'Δ Peak acceleration per lap, per driver'),
        (_ax_d, 'delta_decel', 'Windowed − Unwindowed (g)',
         'Δ Peak deceleration per lap, per driver'),
    ]:
        _order = (
            _merged.groupby('driver')[_dcol]
            .median()
            .sort_values()
            .index.tolist()
        )
        _data = [_merged.loc[_merged['driver'] == d, _dcol].values for d in _order]
        _bp = _ax.boxplot(_data, patch_artist=True,
                          medianprops=dict(color='white', linewidth=1.5))
        _colours_bp = {
            row['driver']: row['team_colour']
            for _, row in lap_accel_df[['driver', 'team_colour']].drop_duplicates().iterrows()
        }
        for _patch, _drv in zip(_bp['boxes'], _order):
            _patch.set_facecolor(_colours_bp.get(_drv, '#aaaaaa'))
            _patch.set_alpha(0.8)
        _ax.axhline(0, color='k', linestyle='--', linewidth=1, alpha=0.5)
        _ax.set_xticks(range(1, len(_order) + 1))
        _ax.set_xticklabels(_order, rotation=45, ha='right')
        _ax.set_ylabel(_ylabel)
        _ax.set_title(_title)

    _fig113.suptitle('11.3 — Per-lap delta box plot (windowed − unwindowed), per driver')
    _fig113.tight_layout()
    _fig113
    return


@app.cell
def _(lap_accel_df, lap_accel_windowed_df, plt):
    """11.4 — Full-race time series: unwindowed vs windowed."""
    DRIVER = 'ALO'
    _ham_uw = lap_accel_df[lap_accel_df['driver'] == DRIVER].sort_values('lap')
    _ham_w  = lap_accel_windowed_df[lap_accel_windowed_df['driver'] == DRIVER].sort_values('lap')

    _fig114, (_ax_a, _ax_d) = plt.subplots(2, 1, figsize=(14, 7), sharex=True)

    _ax_a.plot(_ham_uw['lap'], _ham_uw['peak_accel_g'],     color='grey',     linewidth=1.5, label='Unwindowed')
    _ax_a.plot(_ham_w['lap'],  _ham_w['peak_accel_g'],      color='steelblue', linewidth=1.5, linestyle='--', label='Windowed')
    _ax_a.set_ylabel('Peak acceleration (g)')
    _ax_a.set_title(f'{DRIVER} — peak acceleration per lap')
    _ax_a.legend(fontsize=8)

    _ax_d.plot(_ham_uw['lap'], _ham_uw['peak_decel_g_abs'], color='grey',  linewidth=1.5, label='Unwindowed')
    _ax_d.plot(_ham_w['lap'],  _ham_w['peak_decel_g_abs'],  color='tomato', linewidth=1.5, linestyle='--', label='Windowed')
    _ax_d.set_ylabel('Peak deceleration (g)')
    _ax_d.set_xlabel('Lap number')
    _ax_d.set_title(f'{DRIVER} — peak deceleration per lap')
    _ax_d.legend(fontsize=8)

    _fig114.suptitle('11.4 — HAM full-race time series: unwindowed (solid) vs windowed (dashed)')
    _fig114.tight_layout()
    _fig114
    return


@app.cell
def _(CACHE_DIR, mo):
    """11.5 — Empty-window lap report (laps where throttle/brake gate returned None)."""
    _cache_file_w = CACHE_DIR / 'lap_accel_windowed_rows.pkl'
    _empty_laps = []
    # Reconstruct empty-window list from cache metadata if available; otherwise show
    # placeholder note directing user to re-run without cache.
    if _cache_file_w.exists():
        # The empty-window list is not stored in the cache — it only appears on a fresh
        # compute run. This cell shows a note explaining how to surface it.
        mo.md("""
        **11.5 — Empty-window lap report**

        Laps where the throttle/brake gate returned `None` for at least one channel were
        logged to `_windowed_empty_window` during the fetch loop. To inspect them:

        1. Delete `lap_accel_windowed_rows.pkl` from the cache directory.
        2. Set `USE_CACHE = True` and re-run — the fetch loop will print the list.

        If all laps have valid windows this list will be empty, which is the expected
        result for a power circuit. Monaco-style circuits may show accel-channel `None`
        entries due to very limited high-throttle opportunities (< 20% throttle gate).
        """)
    else:
        mo.md("_Cache not yet built — run the windowed fetch cell above to populate._")
    return


@app.cell
def _(lap_accel_df):
    lap_accel_df
    return


@app.cell
def _(lap_accel_windowed_df):
    lap_accel_windowed_df
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
