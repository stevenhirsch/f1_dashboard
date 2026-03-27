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
    import matplotlib.pyplot as plt
    from scipy.interpolate import PchipInterpolator
    from scipy.signal import butter, filtfilt
    from api.openf1 import get_sessions, get_drivers, get_car_data, get_laps, get_location

    return (
        PchipInterpolator,
        butter,
        filtfilt,
        get_car_data,
        get_drivers,
        get_laps,
        get_location,
        get_sessions,
        mo,
        np,
        pd,
        plt,
    )


@app.cell
def _(mo):
    mo.md("""
    # Lateral G Estimation from OpenF1 Location Data

    The OpenF1 `/location` endpoint provides X, Y, Z coordinates at ~3.7 Hz for each driver.
    This notebook explores three independent methods for estimating lateral G-force per lap
    using those positions combined with car speed from `/car_data`.

    **Fundamental constraint:** at ~3.7 Hz, sample spacing is ~6 m at slow corners (good)
    and ~22 m at 300 km/h (marginal). All three methods are computing second-order spatial
    derivatives from noisy, low-rate data — they are mathematically equivalent but have
    different numerical conditioning. The comparison reveals where the signal is trustworthy
    vs where it is noise-dominated.

    **Methods compared:**
    1. **Circumradius (3-point):** R = (a·b·c) / (4·A), lat_g = v²_car / (R·g)
    2. **Yaw rate:** θ = atan2(dy/dt, dx/dt), lat_g = v_car · |dθ/dt| / g
    3. **Cross-track acceleration:** project (ax, ay) from position onto the track normal

    ---

    ## 1. Session and Driver Setup

    Using the 2026 Chinese GP Race (meeting 1280) and Hamilton.
    """)
    return


@app.cell
def _(get_sessions):
    MEETING_ID = 1280
    sessions = get_sessions(MEETING_ID)
    return (sessions,)


@app.cell
def _(sessions):
    SESSION_KEY = None
    for _s in sessions:
        if _s["session_type"] == "Race" and _s["session_name"] == "Race":
            SESSION_KEY = _s["session_key"]
    print(f"Race session key: {SESSION_KEY}")
    return (SESSION_KEY,)


@app.cell
def _(SESSION_KEY, get_drivers):
    drivers = get_drivers(SESSION_KEY)
    DRIVER_NUMBER = None
    DRIVER_NAME = 'LEC'
    for _d in drivers:
        if _d["name_acronym"] == DRIVER_NAME:
            DRIVER_NUMBER = _d["driver_number"]
    print(f" {DRIVER_NAME} driver number: {DRIVER_NUMBER}")
    return DRIVER_NAME, DRIVER_NUMBER


@app.cell
def _(DRIVER_NUMBER, SESSION_KEY, get_laps, pd):
    lap_info = get_laps(SESSION_KEY, DRIVER_NUMBER)
    lap_df = pd.DataFrame(lap_info)
    lap_df['date_start'] = pd.to_datetime(lap_df['date_start'], format='ISO8601')
    print(f"Total laps: {len(lap_df)}")
    lap_df[['lap_number', 'date_start', 'lap_duration',
            'duration_sector_1', 'duration_sector_2', 'duration_sector_3']].head(10)
    return (lap_df,)


@app.cell
def _(mo):
    mo.md("""
    ---

    ## 2. Fetch Location and Speed Data

    Fetch a wide window covering laps 3–end-of-race and slice per-lap client-side.
    Location and car_data are merged onto a common 4 Hz grid via PCHIP interpolation.
    """)
    return


@app.cell
def _(DRIVER_NUMBER, SESSION_KEY, get_car_data, get_location, lap_df):
    LAP_START_IDX = 2   # 0-indexed → lap 3
    LAP_END_IDX   = 55

    _t_start = lap_df.iloc[LAP_START_IDX]['date_start'].strftime('%Y-%m-%dT%H:%M:%S')
    _t_end   = lap_df.iloc[LAP_END_IDX]['date_start'].strftime('%Y-%m-%dT%H:%M:%S')

    loc_raw = get_location(SESSION_KEY, DRIVER_NUMBER, _t_start, _t_end)
    car_raw = get_car_data(SESSION_KEY, DRIVER_NUMBER, _t_start, _t_end)

    print(f"Location records: {len(loc_raw)}")
    print(f"Car data records: {len(car_raw)}")
    return LAP_END_IDX, LAP_START_IDX, car_raw, loc_raw


@app.cell
def _(car_raw, loc_raw, pd):
    loc_df = pd.DataFrame(loc_raw)
    loc_df['date'] = pd.to_datetime(loc_df['date'], format='ISO8601')
    loc_df = loc_df.sort_values('date').reset_index(drop=True)

    car_df = pd.DataFrame(car_raw)
    car_df['date'] = pd.to_datetime(car_df['date'], format='ISO8601')
    car_df = car_df.sort_values('date').reset_index(drop=True)

    print("Location columns:", loc_df.columns.tolist())
    print("Sample location row:")
    loc_df.head(3)
    return car_df, loc_df


@app.cell
def _(mo):
    mo.md("""
    ---

    ## 3. Coordinate Scale Validation

    Validate that OpenF1 XY is in **metres** by computing the raw path length of one lap
    and comparing to the Shanghai International Circuit (~5.451 km).
    """)
    return


@app.cell
def _(lap_df, loc_df, np):
    _lap_n = 3
    _t0 = lap_df.iloc[_lap_n]['date_start']
    _t1 = lap_df.iloc[_lap_n + 1]['date_start']

    loc_lap = loc_df[(loc_df['date'] >= _t0) & (loc_df['date'] < _t1)].copy()
    loc_lap = loc_lap.dropna(subset=['x', 'y']).reset_index(drop=True)

    _dx = np.diff(loc_lap['x'].to_numpy())
    _dy = np.diff(loc_lap['y'].to_numpy())
    raw_path_length_km = float(np.sum(np.sqrt(_dx**2 + _dy**2))) / 1000

    KNOWN_CIRCUIT_LENGTH_KM = 5.451
    scale_error_pct = 100 * (raw_path_length_km - KNOWN_CIRCUIT_LENGTH_KM) / KNOWN_CIRCUIT_LENGTH_KM

    print(f"Raw XY path length (one lap): {raw_path_length_km:.3f} km")
    print(f"Known circuit length:          {KNOWN_CIRCUIT_LENGTH_KM:.3f} km")
    print(f"Scale error:                   {scale_error_pct:+.1f}%")
    print()
    print("Close to 0% → XY is in metres. Large error → correction factor needed.")
    return (loc_lap,)


@app.cell
def _(loc_lap, plt):
    _fig, _ax = plt.subplots(figsize=(8, 6))
    _ax.plot(loc_lap['x'], loc_lap['y'], 'b-', linewidth=0.8, alpha=0.7)
    _ax.scatter(loc_lap['x'].iloc[0], loc_lap['y'].iloc[0], color='green', zorder=5, label='Lap start')
    _ax.set_aspect('equal')
    _ax.set_title('Raw XY track map — one lap (HAM, 2026 China Race)')
    _ax.set_xlabel('X (metres)')
    _ax.set_ylabel('Y (metres)')
    _ax.legend()
    plt.tight_layout()
    plt.gca()
    return


@app.cell
def _(mo):
    mo.md("""
    ---

    ## 4. Resampling Location and Speed to 4 Hz

    Both XY location and speed are resampled onto a common 4 Hz grid via PCHIP interpolation.
    X and Y are interpolated independently. The PCHIP objects are retained so that their
    exact analytical derivatives can be used downstream (Method 2 and 3).
    """)
    return


@app.cell
def _(PchipInterpolator, car_df, loc_df, np):
    FS = 4.0  # Hz — pipeline standard

    t_loc_epoch = loc_df['date'].astype('int64').to_numpy() / 1e9
    t_car_epoch = car_df['date'].astype('int64').to_numpy() / 1e9

    t_common_start = max(t_loc_epoch[0], t_car_epoch[0])
    t_common_end   = min(t_loc_epoch[-1], t_car_epoch[-1])

    t_reg = np.arange(t_common_start, t_common_end, 1 / FS)
    print(f"Common 4 Hz grid: {len(t_reg)} samples over {(t_reg[-1] - t_reg[0]):.1f}s")

    _x_raw = loc_df['x'].to_numpy().astype(float)
    _y_raw = loc_df['y'].to_numpy().astype(float)
    _valid  = ~(np.isnan(_x_raw) | np.isnan(_y_raw))

    # Retain PCHIP objects for analytical derivatives in methods 2 & 3
    pchip_x = PchipInterpolator(t_loc_epoch[_valid], _x_raw[_valid])
    pchip_y = PchipInterpolator(t_loc_epoch[_valid], _y_raw[_valid])

    x_reg = pchip_x(t_reg)
    y_reg = pchip_y(t_reg)

    speed_reg_kph = PchipInterpolator(t_car_epoch, car_df['speed'].to_numpy().astype(float))(t_reg)
    speed_reg_ms  = speed_reg_kph / 3.6

    print(f"Speed range: {speed_reg_ms.min():.1f} – {speed_reg_ms.max():.1f} m/s")
    return FS, pchip_x, pchip_y, speed_reg_ms, t_reg, x_reg, y_reg


@app.cell
def _(mo):
    mo.md("""
    ---

    ## 5. Filtering XY — Cutoff Options

    A Butterworth low-pass is swept across three cutoffs, plus raw (unfiltered) data.
    All four variants feed into all three estimation methods for comparison.

    - **raw**: no filtering — maximum curvature resolution, maximum noise
    - **0.3 Hz**: very smooth — corners heavily blurred at typical F1 lap times
    - **0.5 Hz**: matches speed-signal pipeline standard
    - **1.0 Hz**: lighter smoothing — preserves more corner geometry
    """)
    return


@app.cell
def _(FS, butter, filtfilt, x_reg, y_reg):
    def butterworth_xy(x, y, cutoff_hz, fs=FS):
        """4th-order Butterworth low-pass applied to X and Y independently."""
        b, a   = butter(N=4, Wn=cutoff_hz / (fs / 2), btype='low')
        padlen = min(len(x) // 4, int(5 * fs))
        return filtfilt(b, a, x, padlen=padlen), filtfilt(b, a, y, padlen=padlen)

    CUTOFFS = ["raw", 0.3, 0.5, 1.0]

    filtered = {"raw": (x_reg, y_reg)}
    for _cutoff in CUTOFFS[1:]:
        filtered[_cutoff] = butterworth_xy(x_reg, y_reg, _cutoff)

    print(f"Filtered XY computed at cutoffs: {CUTOFFS}")
    return CUTOFFS, filtered


@app.cell
def _(CUTOFFS, DRIVER_NAME, LAP_END_IDX, LAP_START_IDX, filtered, plt):
    _fig, _axes = plt.subplots(1, len(CUTOFFS), figsize=(18, 5))

    for _i, _c in enumerate(CUTOFFS):
        _xf, _yf = filtered[_c]
        _axes[_i].plot(_xf, _yf, '-', linewidth=0.8)
        _title = 'Unfiltered\n(raw resampled)' if _c == "raw" else f'Butterworth\n{_c} Hz cutoff'
        _axes[_i].set_title(_title)
        _axes[_i].set_aspect('equal')
        _axes[_i].set_xlabel('X')
    _axes[0].set_ylabel('Y')

    _fig.suptitle(f'Track map under different XY cutoffs ({DRIVER_NAME} laps {LAP_START_IDX}–{LAP_END_IDX})')
    plt.tight_layout()
    plt.gca()
    return


@app.cell
def _(mo):
    mo.md("""
    ---

    ## 6. Method 1 — Circumradius (3-Point)

    For each interior point i, fit the circumscribed circle to the triplet (p_{i−1}, p_i, p_{i+1}):

    > R = (a × b × c) / (4 × A)

    - a = |p_{i−1} → p_i|,  b = |p_i → p_{i+1}|,  c = |p_{i−1} → p_{i+1}|
    - A = ½ |cross(p_i − p_{i−1}, p_{i+1} − p_{i−1})| (2D triangle area)

    **Straight-line guard:** A < threshold → R = ∞ → lat_g = 0 (not R=0, which was a prior bug).

    **Lateral G ceiling:** 7 g — physical plausibility bound for F1.
    """)
    return


@app.cell
def _(np):
    def compute_circumradius(x_f, y_f, collinear_threshold=1e-3, r_max=2000.0):
        """Circumradius at each interior point via the 3-point method.

        Parameters
        ----------
        x_f, y_f            : array-like — filtered XY positions (metres), length N
        collinear_threshold : float — minimum triangle area (m²) below which
                              curvature is treated as zero (straight-line section)
        r_max               : float — radius upper clip (m)

        Returns
        -------
        R : np.ndarray, length N, with NaN at first and last indices
        """
        x, y = np.asarray(x_f, dtype=float), np.asarray(y_f, dtype=float)

        dx_a = x[1:-1] - x[:-2]   # p_i    − p_{i-1}
        dy_a = y[1:-1] - y[:-2]
        dx_c = x[2:]   - x[:-2]   # p_{i+1} − p_{i-1}
        dy_c = y[2:]   - y[:-2]

        a = np.sqrt(dx_a**2 + dy_a**2)
        b = np.sqrt((x[2:] - x[1:-1])**2 + (y[2:] - y[1:-1])**2)
        c = np.sqrt(dx_c**2 + dy_c**2)

        A = 0.5 * np.abs(dx_a * dy_c - dy_a * dx_c)

        # Straight sections (A < threshold): R = inf → lat_g = 0
        # Curved sections: circumradius, clipped to r_max
        with np.errstate(divide='ignore', invalid='ignore'):
            R_circ = (a * b * c) / (4 * A)
        R_int = np.where(
            A < collinear_threshold,
            np.inf,
            np.minimum(R_circ, r_max),
        )

        R = np.full(len(x), np.nan)
        R[1:-1] = R_int
        return R

    print("compute_circumradius defined")
    return (compute_circumradius,)


@app.cell
def _(CUTOFFS, compute_circumradius, filtered, np, pd, speed_reg_ms, t_reg):
    G         = 9.81
    LAT_G_MAX = 7.0

    results = {}
    for _c in CUTOFFS:
        _xf, _yf = filtered[_c]
        _R = compute_circumradius(_xf, _yf)
        # R=inf on straights → 0.0 g; R=NaN at endpoints → NaN
        with np.errstate(divide='ignore', invalid='ignore'):
            _lat_g = np.where(np.isinf(_R), 0.0, speed_reg_ms**2 / (_R * G))
        _lat_g = np.minimum(_lat_g, LAT_G_MAX)
        results[_c] = {'lat_g': _lat_g}

    t_dt = pd.to_datetime(t_reg, unit='s', utc=True)

    print("Peak lateral G by cutoff (Method 1 — circumradius):")
    for _c in CUTOFFS:
        _lg = results[_c]['lat_g']
        print(f"  {str(_c):>6}: peak={np.nanmax(_lg):.2f} g  |  mean={np.nanmean(_lg):.2f} g")
    return G, LAT_G_MAX, results, t_dt


@app.cell
def _(CUTOFFS, DRIVER_NAME, plt, results, t_dt):
    _fig, _ax = plt.subplots(figsize=(16, 4))
    for _c in CUTOFFS:
        _ax.plot(t_dt, results[_c]['lat_g'], linewidth=0.8, alpha=0.8, label=str(_c))
    _ax.set_xlabel('Time')
    _ax.set_ylabel('Lateral G (g)')
    _ax.set_title(f'Method 1: Circumradius — {DRIVER_NAME} 2026 China Race')
    _ax.legend(title='XY cutoff')
    _ax.set_ylim(0, 8)
    plt.tight_layout()
    plt.gca()
    return


@app.cell
def _(mo):
    mo.md("""
    ---

    ## 7. Method 2 — Yaw Rate (Heading Derivative)

    Compute heading θ from the PCHIP analytical derivative of position (one differentiation),
    unwrap across the ±π boundary, then differentiate heading to get yaw rate ω:

    > θ = atan2(ẏ, ẋ)  →  ω = dθ/dt  →  lat_g = v_car × |ω| / g

    Key differences from Method 1:
    - arctan2 output is bounded to [−π, π] — noise cannot produce R→0 singularities
    - Uses PCHIP analytical first derivative for ẋ, ẏ (smoother than finite differences)
    - v_car from car_data sensors, same as Method 1
    - Numerically equivalent to v²/R but the collinearity singularity is bypassed
    """)
    return


@app.cell
def _(
    CUTOFFS,
    FS,
    G,
    LAT_G_MAX,
    filtered,
    np,
    pchip_x,
    pchip_y,
    speed_reg_ms,
    t_reg,
):
    _dt = 1.0 / FS

    # PCHIP analytical derivatives at the resampled grid — smoother than np.gradient on raw XY
    _vx_pchip = pchip_x.derivative()(t_reg)
    _vy_pchip = pchip_y.derivative()(t_reg)

    results_yaw = {}
    for _c in CUTOFFS:
        if _c == "raw":
            # Use PCHIP derivative directly (unfiltered velocity)
            _vx, _vy = _vx_pchip, _vy_pchip
        else:
            # For filtered XY: finite differences on the filtered arrays
            _xf, _yf = filtered[_c]
            _vx = np.gradient(_xf, _dt)
            _vy = np.gradient(_yf, _dt)

        _heading_uw = np.unwrap(np.arctan2(_vy, _vx))
        _yaw_rate   = np.gradient(_heading_uw, _dt)  # rad/s
        _lat_g      = np.minimum(np.abs(speed_reg_ms * _yaw_rate) / G, LAT_G_MAX)
        results_yaw[_c] = {'lat_g': _lat_g, 'yaw_rate': _yaw_rate}

    print("Peak lateral G by cutoff (Method 2 — yaw rate):")
    for _c in CUTOFFS:
        _lg = results_yaw[_c]['lat_g']
        print(f"  {str(_c):>6}: peak={np.nanmax(_lg):.2f} g  |  mean={np.nanmean(_lg):.2f} g")
    return (results_yaw,)


@app.cell
def _(CUTOFFS, DRIVER_NAME, plt, results_yaw, t_dt):
    _fig, _ax = plt.subplots(figsize=(16, 4))
    for _c in CUTOFFS:
        _ax.plot(t_dt, results_yaw[_c]['lat_g'], linewidth=0.8, alpha=0.8, label=str(_c))
    _ax.set_xlabel('Time')
    _ax.set_ylabel('Lateral G (g)')
    _ax.set_title(f'Method 2: Yaw Rate — {DRIVER_NAME} 2026 China Race')
    _ax.legend(title='XY cutoff')
    _ax.set_ylim(0, 8)
    plt.tight_layout()
    plt.gca()
    return


@app.cell
def _(mo):
    mo.md("""
    ---

    ## 8. Method 3 — Cross-Track Acceleration

    Compute the full 2D acceleration vector (ax, ay) from double-differencing position,
    then project it onto the unit normal of the velocity vector:

    > n̂ = (−ẏ, ẋ) / |v|  →  a_lat = (ax, ay) · n̂  →  lat_g = |a_lat| / g

    Key differences:
    - Completely independent of car_data speed — derived purely from XY position
    - Double differentiation of position amplifies noise most aggressively of the three methods
    - Any bias or scale error in XY affects both numerator and denominator consistently
    - The sign of a_lat indicates left vs. right cornering (not used here, only magnitude)

    If this method gives dramatically different values from Methods 1 and 2, the speed data
    from car_data and the position-derived speed disagree — a useful diagnostic.
    """)
    return


@app.cell
def _(CUTOFFS, FS, G, LAT_G_MAX, filtered, np, pchip_x, pchip_y, t_reg):
    _dt = 1.0 / FS

    # PCHIP first and second analytical derivatives
    _vx_p = pchip_x.derivative()(t_reg)
    _vy_p = pchip_y.derivative()(t_reg)
    _ax_p = pchip_x.derivative(2)(t_reg)
    _ay_p = pchip_y.derivative(2)(t_reg)

    results_accel = {}
    for _c in CUTOFFS:
        if _c == "raw":
            _vx, _vy = _vx_p, _vy_p
            _ax_, _ay_ = _ax_p, _ay_p
        else:
            _xf, _yf = filtered[_c]
            _vx  = np.gradient(_xf, _dt)
            _vy  = np.gradient(_yf, _dt)
            _ax_ = np.gradient(_vx,  _dt)
            _ay_ = np.gradient(_vy,  _dt)

        _v_pos = np.sqrt(_vx**2 + _vy**2)
        # Mask near-zero speed (pit lane, standing start) to avoid div/0
        _v_safe = np.where(_v_pos < 1.0, np.nan, _v_pos)

        with np.errstate(invalid='ignore'):
            # Project (ax, ay) onto unit normal n̂ = (-vy, vx) / |v|
            _a_lat = (-_vy * _ax_ + _vx * _ay_) / _v_safe

        _lat_g = np.minimum(np.abs(_a_lat) / G, LAT_G_MAX)
        results_accel[_c] = {'lat_g': _lat_g}

    print("Peak lateral G by cutoff (Method 3 — cross-track acceleration):")
    for _c in CUTOFFS:
        _lg = results_accel[_c]['lat_g']
        print(f"  {str(_c):>6}: peak={np.nanmax(_lg):.2f} g  |  mean={np.nanmean(_lg):.2f} g")
    return (results_accel,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    _dt = 1.0 / FS

    # PCHIP first and second analytical derivatives
    _vx_p = pchip_x.derivative()(t_reg)
    _vy_p = pchip_y.derivative()(t_reg)
    _ax_p = pchip_x.derivative(2)(t_reg)
    _ay_p = pchip_y.derivative(2)(t_reg)

    results_accel = {}
    for _c in CUTOFFS:
        if _c == "raw":
            _vx, _vy = _vx_p, _vy_p
            _ax_, _ay_ = _ax_p, _ay_p
        else:
            _xf, _yf = filtered[_c]
            _vx  = np.gradient(_xf, _dt)
            _vy  = np.gradient(_yf, _dt)
            _ax_ = np.gradient(_vx,  _dt)
            _ay_ = np.gradient(_vy,  _dt)

        _v_pos = np.sqrt(_vx**2 + _vy**2)
        # Mask near-zero speed (pit lane, standing start) to avoid div/0
        _v_safe = np.where(_v_pos < 1.0, np.nan, _v_pos)

        with np.errstate(invalid='ignore'):
            # Project (ax, ay) onto unit normal n̂ = (-vy, vx) / |v|
            _a_lat = (-_vy * _ax_ + _vx * _ay_) / _v_safe

        _lat_g = np.minimum(np.abs(_a_lat) / G, LAT_G_MAX)
        results_accel[_c] = {'lat_g': _lat_g}

    print("Peak lateral G by cutoff (Method 3 — cross-track acceleration):")
    for _c in CUTOFFS:
        _lg = results_accel[_c]['lat_g']
        print(f"  {str(_c):>6}: peak={np.nanmax(_lg):.2f} g  |  mean={np.nanmean(_lg):.2f} g")
    """)
    return


@app.cell
def _(CUTOFFS, DRIVER_NAME, plt, results_accel, t_dt):
    _fig, _ax = plt.subplots(figsize=(16, 4))
    for _c in CUTOFFS:
        _ax.plot(t_dt, results_accel[_c]['lat_g'], linewidth=0.8, alpha=0.8, label=str(_c))
    _ax.set_xlabel('Time')
    _ax.set_ylabel('Lateral G (g)')
    _ax.set_title(f'Method 3: Cross-Track Acceleration — {DRIVER_NAME} 2026 China Race')
    _ax.legend(title='XY cutoff')
    _ax.set_ylim(0, 8)
    plt.tight_layout()
    plt.gca()
    return


@app.cell
def _(mo):
    mo.md("""
    ---

    ## 9. Single-Lap Method Comparison

    All three methods on the same lap with the same XY cutoff.
    Speed is shown for reference — G peaks should align with low-speed, high-curvature corners.

    If all three methods agree at a corner, that corner's G estimate is trustworthy.
    Where they diverge, the data is noise-dominated at that point.
    """)
    return


@app.cell
def _():
    filter_cutoff_options = ["raw", 0.3, 0.5, 1.0]
    return (filter_cutoff_options,)


@app.cell
def _(filter_cutoff_options, mo):
    cutoff_slider = mo.ui.slider(
        start=0, stop=len(filter_cutoff_options) - 1, step=1, value=0,
        label="Cutoff index  [0=raw  1=0.3 Hz  2=0.5 Hz  3=1.0 Hz]"
    )
    return (cutoff_slider,)


@app.cell
def _(cutoff_slider, filter_cutoff_options):
    CHOSEN_CUTOFF = filter_cutoff_options[cutoff_slider.value]
    print(f"Chosen cutoff: {CHOSEN_CUTOFF}")
    cutoff_slider
    return (CHOSEN_CUTOFF,)


@app.cell
def _(CHOSEN_CUTOFF, lap_df, np, results, t_reg):
    _lap_idx = 15
    _t0_epoch = lap_df.iloc[_lap_idx]['date_start'].timestamp()
    _t1_epoch = lap_df.iloc[_lap_idx + 1]['date_start'].timestamp()

    mask_single  = (t_reg >= _t0_epoch) & (t_reg < _t1_epoch)
    t_single     = t_reg[mask_single]
    lat_g_single = results[CHOSEN_CUTOFF]['lat_g'][mask_single]
    lap_idx_single = _lap_idx

    print(f"Lap {_lap_idx + 1}: {mask_single.sum()} samples = {mask_single.sum() / 4:.1f}s")
    print(f"Peak lateral G (circumradius): {np.nanmax(lat_g_single):.2f} g")
    return lap_idx_single, mask_single, t_single


@app.cell
def _(
    CHOSEN_CUTOFF,
    mask_single,
    plt,
    results,
    results_accel,
    results_yaw,
    speed_reg_ms,
    t_single,
):
    _t_s = t_single - t_single[0]
    _ref = dict(color='orange', linestyle='--', linewidth=0.8, alpha=0.7)

    _fig, _axes = plt.subplots(4, 1, figsize=(14, 13), sharex=True)

    _axes[0].plot(_t_s, speed_reg_ms[mask_single] * 3.6, 'b-', linewidth=1)
    _axes[0].set_ylabel('Speed (km/h)')
    _axes[0].set_title(f'HAM — single lap comparison (cutoff: {CHOSEN_CUTOFF})')

    _axes[1].plot(_t_s, results[CHOSEN_CUTOFF]['lat_g'][mask_single], 'r-', linewidth=1)
    _axes[1].axhline(3.0, label='3 g ref', **_ref)
    _axes[1].set_ylabel('Lat G (g)')
    _axes[1].set_title('Method 1: Circumradius')
    _axes[1].set_ylim(0, 8)
    _axes[1].legend(fontsize=8)

    _axes[2].plot(_t_s, results_yaw[CHOSEN_CUTOFF]['lat_g'][mask_single], 'g-', linewidth=1)
    _axes[2].axhline(3.0, label='3 g ref', **_ref)
    _axes[2].set_ylabel('Lat G (g)')
    _axes[2].set_title('Method 2: Yaw Rate')
    _axes[2].set_ylim(0, 8)
    _axes[2].legend(fontsize=8)

    _axes[3].plot(_t_s, results_accel[CHOSEN_CUTOFF]['lat_g'][mask_single], 'm-', linewidth=1)
    _axes[3].axhline(3.0, label='3 g ref', **_ref)
    _axes[3].set_ylabel('Lat G (g)')
    _axes[3].set_xlabel('Time from lap start (s)')
    _axes[3].set_title('Method 3: Cross-Track Acceleration')
    _axes[3].set_ylim(0, 8)
    _axes[3].legend(fontsize=8)

    plt.tight_layout()
    plt.gca()
    return


@app.cell
def _(mo):
    mo.md("""
    ---

    ## 10. Colour-Coded Track Map by Lateral G

    Track map for the selected lap and cutoff, coloured by the **yaw-rate method** lateral G.
    High-G corners should be visually obvious and match known circuit features.
    """)
    return


@app.cell
def _(CHOSEN_CUTOFF, filtered, mask_single, np, plt, results_yaw):
    _xf, _yf = filtered[CHOSEN_CUTOFF]
    _x_lap = _xf[mask_single]
    _y_lap = _yf[mask_single]
    _g_plot = np.nan_to_num(results_yaw[CHOSEN_CUTOFF]['lat_g'][mask_single], nan=0.0)

    _fig, _ax = plt.subplots(figsize=(9, 7))
    _sc = _ax.scatter(_x_lap, _y_lap, c=_g_plot, cmap='RdYlGn_r', vmin=0, vmax=5, s=4)
    plt.colorbar(_sc, ax=_ax, label='Lateral G (g)')
    _ax.set_aspect('equal')
    _ax.set_title(f'Track map coloured by lateral G — yaw rate (cutoff: {CHOSEN_CUTOFF})')
    _ax.set_xlabel('X'); _ax.set_ylabel('Y')
    plt.tight_layout()
    plt.gca()
    return


@app.cell
def _(mo):
    mo.md("""
    ---

    ## 11. Per-Sector Peak Lateral G — Yaw Rate

    Peak and mean lateral G per sector for the selected lap, using the yaw-rate method.
    This is the shape of the metric that will feed `lap_metrics` in the pipeline.
    """)
    return


@app.cell
def _(CHOSEN_CUTOFF, lap_df, lap_idx_single, np, results_yaw, t_reg):
    _row   = lap_df.iloc[lap_idx_single]
    _s1dur = _row.get('duration_sector_1')
    _s2dur = _row.get('duration_sector_2')
    _s3dur = _row.get('duration_sector_3')

    _t0 = _row['date_start'].timestamp()
    _s1_end  = (_t0 + float(_s1dur)) if _s1dur else None
    _s2_end  = (_s1_end + float(_s2dur)) if (_s1_end and _s2dur) else None
    _lap_end = (_s2_end + float(_s3dur)) if (_s2_end and _s3dur) else None

    _lat_g_all = results_yaw[CHOSEN_CUTOFF]['lat_g']

    def _stats(ta, tb):
        if ta is None or tb is None:
            return None, None
        _vals = _lat_g_all[(t_reg >= ta) & (t_reg < tb)]
        _vals = _vals[~np.isnan(_vals)]
        if len(_vals) == 0:
            return None, None
        return float(np.max(_vals)), float(np.mean(_vals))

    for _label, _ta, _tb in [
        ('S1',  _t0,    _s1_end),
        ('S2',  _s1_end, _s2_end),
        ('S3',  _s2_end, _lap_end),
        ('Lap', _t0,    _lap_end),
    ]:
        _pk, _mn = _stats(_ta, _tb)
        if _pk is not None:
            print(f"  {_label}: peak={_pk:.2f} g  mean={_mn:.2f} g")
        else:
            print(f"  {_label}: n/a")
    return


@app.cell
def _(mo):
    mo.md("""
    ---

    ## 12. Multi-Lap Consistency — Yaw Rate

    Peak and mean lateral G per sector across all laps (laps 3 to end of race).
    Consistent peaks lap-to-lap confirm the signal reflects real corner loading
    rather than GPS noise transients.
    """)
    return


@app.cell
def _():
    # _per_lap = []

    # for _i in range(2, len(lap_df) - 1):
    #     _t0_str = lap_df.iloc[_i]['date_start'].strftime('%Y-%m-%dT%H:%M:%S')
    #     _t1_str = lap_df.iloc[_i + 1]['date_start'].strftime('%Y-%m-%dT%H:%M:%S')
    #     _t0_ep  = lap_df.iloc[_i]['date_start'].timestamp()

    #     _loc_l = pd.DataFrame(get_location(SESSION_KEY, DRIVER_NUMBER, _t0_str, _t1_str))
    #     _car_l = pd.DataFrame(get_car_data(SESSION_KEY, DRIVER_NUMBER, _t0_str, _t1_str))
    #     if _loc_l.empty or _car_l.empty:
    #         continue

    #     _loc_l['date'] = pd.to_datetime(_loc_l['date'], format='ISO8601')
    #     _car_l['date'] = pd.to_datetime(_car_l['date'], format='ISO8601')
    #     _tl = _loc_l['date'].astype('int64').to_numpy() / 1e9
    #     _tc = _car_l['date'].astype('int64').to_numpy() / 1e9

    #     _treg = np.arange(max(_tl[0], _tc[0]), min(_tl[-1], _tc[-1]), 1 / FS)
    #     if len(_treg) < 32:
    #         continue

    #     _xl = _loc_l['x'].to_numpy().astype(float)
    #     _yl = _loc_l['y'].to_numpy().astype(float)
    #     _valid = ~(np.isnan(_xl) | np.isnan(_yl))
    #     if _valid.sum() < 10:
    #         continue

    #     _pchip_x = PchipInterpolator(_tl[_valid], _xl[_valid])
    #     _pchip_y = PchipInterpolator(_tl[_valid], _yl[_valid])
    #     _dt = 1.0 / FS

    #     if CHOSEN_CUTOFF == "raw":
    #         _vx = _pchip_x.derivative()(_treg)
    #         _vy = _pchip_y.derivative()(_treg)
    #     else:
    #         _xr = _pchip_x(_treg)
    #         _yr = _pchip_y(_treg)
    #         _xf, _yf = butterworth_xy(_xr, _yr, CHOSEN_CUTOFF, fs=FS)
    #         _vx = np.gradient(_xf, _dt)
    #         _vy = np.gradient(_yf, _dt)

    #     _spd_ms = PchipInterpolator(_tc, _car_l['speed'].to_numpy().astype(float))(_treg) / 3.6
    #     _heading_uw = np.unwrap(np.arctan2(_vy, _vx))
    #     _yaw_rate   = np.gradient(_heading_uw, _dt)
    #     _lg = np.minimum(np.abs(_spd_ms * _yaw_rate) / 9.81, LAT_G_MAX)

    #     _row   = lap_df.iloc[_i]
    #     _s1dur = _row.get('duration_sector_1')
    #     _s2dur = _row.get('duration_sector_2')
    #     _s3dur = _row.get('duration_sector_3')
    #     _s1end = (_t0_ep + float(_s1dur)) if _s1dur else None
    #     _s2end = (_s1end + float(_s2dur)) if (_s1end and _s2dur) else None
    #     _lend  = (_s2end + float(_s3dur)) if (_s2end and _s3dur) else None

    #     def _sector_stats(ta, tb, treg=_treg, lg=_lg):
    #         if ta is None or tb is None:
    #             return None, None
    #         _v = lg[(treg >= ta) & (treg < tb)]
    #         _v = _v[~np.isnan(_v)]
    #         if len(_v) == 0:
    #             return None, None
    #         return float(np.max(_v)), float(np.mean(_v))

    #     _pk_lap, _mn_lap = _sector_stats(_t0_ep, _lend)
    #     _pk_s1,  _mn_s1  = _sector_stats(_t0_ep, _s1end)
    #     _pk_s2,  _mn_s2  = _sector_stats(_s1end, _s2end)
    #     _pk_s3,  _mn_s3  = _sector_stats(_s2end, _lend)

    #     _per_lap.append({
    #         'lap':     _i + 1,
    #         'peak_lap': _pk_lap, 'mean_lap': _mn_lap,
    #         'peak_s1':  _pk_s1,  'mean_s1':  _mn_s1,
    #         'peak_s2':  _pk_s2,  'mean_s2':  _mn_s2,
    #         'peak_s3':  _pk_s3,  'mean_s3':  _mn_s3,
    #     })

    # peaks_df = pd.DataFrame(_per_lap)
    # print(f"Multi-lap lateral G — yaw rate, cutoff={CHOSEN_CUTOFF}:")
    # print(peaks_df.to_string(index=False, float_format=lambda x: f"{x:.2f}" if x is not None else "n/a"))
    return


@app.cell
def _(mo):
    mo.md("""
    ---

    ## 14. Summary — Pipeline Decisions

    ### Chosen method: Yaw Rate (Method 2)

    **Rationale:** Validated across multiple drivers at 2026 Chinese GP Race.
    Known high-G corners (Shanghai T1 hairpin, T14 hairpin, T3–T6 esses) are consistently
    lit up in the track map and align with speed minima. Absolute values are physically
    plausible. Circumradius (Method 1) produced anomalous spikes; cross-track acceleration
    (Method 3) is noisier due to double differentiation of position.

    ### Pipeline constants

    | Parameter | Value | Rationale |
    |---|---|---|
    | Method | Yaw rate: `lat_g = v_car × \|dθ/dt\| / 9.81` | Best corner discrimination, no collinearity singularity |
    | XY Butterworth cutoff | 0.5 Hz | Matches longitudinal G pipeline; preserves corner geometry |
    | Velocity from XY | PCHIP derivative (raw), `np.gradient` on filtered arrays | PCHIP derivative is smoothest for unfiltered case |
    | Speed source | `/car_data` sensor | Accurate; position-derived speed not used |
    | Lateral G ceiling | 7 g | Physical plausibility bound for F1 |
    | Metrics stored | peak + mean per sector (S1/S2/S3) and lap | Richer than peak-only; consistent with longitudinal approach |

    ### Known physical reference values (Shanghai)
    - Turn 1 hairpin: ~2–3 g (slow, tight)
    - Turns 3–6 esses: ~2–3 g (medium speed)
    - Turn 14 hairpin: ~2–3 g

    ### Data constraint
    At ~3.7 Hz, position sample spacing is ~6 m at slow corners (reliable) and
    ~22 m at 300 km/h (marginal). High-speed flowing corners will be underestimated.
    Flag lateral G metrics as lower-confidence than longitudinal G in the pipeline schema.

    ### Next step
    Implement `peak_lat_g_s1/s2/s3` and `mean_lat_g_s1/s2/s3` columns in
    `_compute_lap_metrics` in `pipeline/ingest.py` using the yaw-rate method at 0.5 Hz.
    """)
    return


if __name__ == "__main__":
    app.run()
