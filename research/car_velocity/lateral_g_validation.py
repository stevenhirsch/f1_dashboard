import marimo

__generated_with = "0.20.4"
app = marimo.App(width="full")


@app.cell
def _():
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    return (Path,)


@app.cell
def _(Path):
    import marimo as mo
    import numpy as np
    import pandas as pd
    import matplotlib.pyplot as plt
    import pickle
    from scipy.interpolate import PchipInterpolator
    from scipy.signal import butter, filtfilt, welch, detrend
    from api.openf1 import get_sessions, get_drivers, get_laps, get_location, get_car_data

    CACHE_DIR = Path(__file__).resolve().parent / ".cache"
    CACHE_DIR.mkdir(exist_ok=True)
    USE_CACHE = True
    return (
        CACHE_DIR,
        PchipInterpolator,
        USE_CACHE,
        butter,
        detrend,
        filtfilt,
        get_car_data,
        get_drivers,
        get_laps,
        get_location,
        get_sessions,
        mo,
        np,
        pd,
        pickle,
        plt,
        welch,
    )


@app.cell
def _(mo):
    mo.md("""
    # Lateral G Validation — Part A: Spectral Analysis

    **Goal:** Rigorously justify the Butterworth cutoff frequency for XY filtering in the
    lateral G pipeline. Two candidates emerged from `lateral_g.py`:

    - **0.3 Hz** — visually retains known corner geometry; current best candidate
    - **0.5 Hz** — pipeline standard for speed-derived metrics; appeared to over-smooth some corners

    **Approach:**

    1. **Winter (2009) residual analysis** on XY position — sweeps cutoffs and looks for
       the elbow between signal-removal and noise-removal regimes
    2. **Welch PSD of position-derived speed** — complements Winter by showing *where* in
       frequency the signal energy lives
    3. **Multi-lap spectral stability** — confirms the spectral structure is consistent
       across the full race (not a single-lap artefact)
    4. **Cross-driver spectral consistency** — confirms the cutoff generalises to all 20 drivers
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ---

    ## 1. Session and Driver Setup

    Using the 2026 Chinese GP Race (meeting 1280). LEC used for single-driver analysis
    in sections 2–3; all 20 drivers used in section 4.
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
    DRIVER_NAME = "LEC"
    DRIVER_NUMBER = None
    for _d in drivers:
        if _d["name_acronym"] == DRIVER_NAME:
            DRIVER_NUMBER = _d["driver_number"]
    print(f"{DRIVER_NAME} driver number: {DRIVER_NUMBER}")
    return DRIVER_NAME, DRIVER_NUMBER, drivers


@app.cell
def _(mo):
    mo.md("""
    ---

    ## 2. Fetch Wide Window (Laps 3 – End of Race)

    Single `get_location` and `get_car_data` call each, covering the full race window
    from lap 3 onward. Sliced per-lap client-side.
    """)
    return


@app.cell
def _(DRIVER_NUMBER, SESSION_KEY, get_laps, get_location, pd):
    lap_info = get_laps(SESSION_KEY, DRIVER_NUMBER)
    lap_df = pd.DataFrame(lap_info)
    lap_df["date_start"] = pd.to_datetime(lap_df["date_start"], format="ISO8601")
    lap_df = lap_df.sort_values("lap_number").reset_index(drop=True)

    LAP_START_IDX = 2  # 0-indexed → lap 3
    LAP_END_IDX = min(len(lap_df) - 2, 54)

    _t_start = lap_df.iloc[LAP_START_IDX]["date_start"].strftime("%Y-%m-%dT%H:%M:%S")
    _t_end = lap_df.iloc[LAP_END_IDX]["date_start"].strftime("%Y-%m-%dT%H:%M:%S")

    loc_raw = get_location(SESSION_KEY, DRIVER_NUMBER, _t_start, _t_end)

    print(f"Total laps: {len(lap_df)}")
    print(f"Location records: {len(loc_raw)}")
    return LAP_END_IDX, LAP_START_IDX, lap_df, loc_raw


@app.cell
def _(mo):
    mo.md("""
    ---

    ## 3. PCHIP Resample to 4 Hz

    XY resampled to a common 4 Hz grid via PCHIP interpolation.
    Duplicate timestamps deduped before interpolation.
    """)
    return


@app.cell
def _(PchipInterpolator, loc_raw, np, pd):
    FS = 4.0

    loc_df = pd.DataFrame(loc_raw)
    loc_df["date"] = pd.to_datetime(loc_df["date"], format="ISO8601")
    loc_df = loc_df.sort_values("date").drop_duplicates(subset="date").reset_index(drop=True)

    t_loc = loc_df["date"].astype("int64").to_numpy() / 1e9

    _x_raw = loc_df["x"].to_numpy().astype(float)
    _y_raw = loc_df["y"].to_numpy().astype(float)
    _valid = ~(np.isnan(_x_raw) | np.isnan(_y_raw))

    pchip_x = PchipInterpolator(t_loc[_valid], _x_raw[_valid])
    pchip_y = PchipInterpolator(t_loc[_valid], _y_raw[_valid])

    t_reg = np.arange(t_loc[_valid][0], t_loc[_valid][-1], 1 / FS)
    x_reg = pchip_x(t_reg)
    y_reg = pchip_y(t_reg)

    print(f"4 Hz grid: {len(t_reg)} samples over {(t_reg[-1] - t_reg[0]):.0f}s")
    print(f"X range: {x_reg.min():.0f} – {x_reg.max():.0f} m")
    print(f"Y range: {y_reg.min():.0f} – {y_reg.max():.0f} m")
    return FS, t_reg, x_reg, y_reg


@app.cell
def _(mo):
    mo.md("""
    ---

    ## Part A-1: Winter (2009) Residual Analysis

    The Winter method sweeps cutoff frequencies from 0.1 Hz to Nyquist, computing the
    RMS difference between the raw and filtered position signal at each cutoff:

    > RMSR(fc) = RMS(x_raw − x_filtered(fc))

    Applied here to X and Y position separately.

    **How to read the plots:**

    - **Linear scale:** The y-axis is the RMS residual in metres. As fc increases, less
      filtering is applied, so the residual shrinks (the filtered signal approaches the raw).
      Look for an "elbow" where the curve changes slope — that's the transition from
      removing signal (steep drop) to removing only noise (plateau). The optimal cutoff
      sits at the top of the plateau, before the steep drop begins.

    - **Log scale:** The same curve on a log y-axis. This is better for spotting a plateau
      when the total RMSR spans several orders of magnitude. On a log scale a true plateau
      appears as a horizontal line, and the elbow becomes a sharp corner. If the curve is
      a straight line on the log scale (continuously declining with no flat region), the
      noise is broadband — there is no clean signal/noise separation, and the optimal
      cutoff cannot be determined objectively from the data alone.

    In `noise.py`, the speed signal showed no clear plateau (broadband sampling noise).
    The question here is whether XY position shows the same behaviour or a cleaner separation.
    """)
    return


@app.cell
def _(FS, butter, filtfilt, np, plt, x_reg, y_reg):
    _N_SWEEP = 60
    _cutoffs_sweep = np.linspace(0.1, FS / 2 - 0.05, _N_SWEEP)

    _rmsr_x, _rmsr_y = [], []
    for _fc in _cutoffs_sweep:
        _b, _a = butter(N=4, Wn=_fc / (FS / 2), btype="low")
        _padlen = min(len(x_reg) // 4, int(5 * FS))
        _xf = filtfilt(_b, _a, x_reg, padlen=_padlen)
        _yf = filtfilt(_b, _a, y_reg, padlen=_padlen)
        _rmsr_x.append(float(np.sqrt(np.mean((x_reg - _xf) ** 2))))
        _rmsr_y.append(float(np.sqrt(np.mean((y_reg - _yf) ** 2))))

    _fig, _axes = plt.subplots(1, 2, figsize=(13, 4))
    for _ax, _rmsr, _lbl in [
        (_axes[0], _rmsr_x, "X position"),
        (_axes[1], _rmsr_y, "Y position"),
    ]:
        _ax.plot(_cutoffs_sweep, _rmsr, linewidth=1.2)
        _ax.axvline(0.15, color="mediumseagreen", linestyle="--", linewidth=1.2, label="0.15 Hz")
        _ax.axvline(0.3, color="dodgerblue", linestyle="--", linewidth=1.2, label="0.3 Hz")
        _ax.axvline(0.5, color="orange", linestyle="--", linewidth=1.2, label="0.5 Hz")
        _ax.set_xlabel("Cutoff frequency (Hz)")
        _ax.set_ylabel("RMS residual (m)")
        _ax.set_title(f"Winter residual — {_lbl}")
        _ax.legend(fontsize=9)

    plt.suptitle("Winter (2009) residual analysis — XY position (linear scale)", y=1.02)
    plt.tight_layout()
    plt.gca()
    return


@app.cell
def _(FS, butter, filtfilt, np, plt, x_reg, y_reg):
    """Log-scale Winter curves — easier to spot the elbow/plateau."""
    _N_SWEEP = 60
    _cutoffs_sweep = np.linspace(0.1, FS / 2 - 0.05, _N_SWEEP)

    _rmsr_x, _rmsr_y = [], []
    for _fc in _cutoffs_sweep:
        _b, _a = butter(N=4, Wn=_fc / (FS / 2), btype="low")
        _padlen = min(len(x_reg) // 4, int(5 * FS))
        _xf = filtfilt(_b, _a, x_reg, padlen=_padlen)
        _yf = filtfilt(_b, _a, y_reg, padlen=_padlen)
        _rmsr_x.append(float(np.sqrt(np.mean((x_reg - _xf) ** 2))))
        _rmsr_y.append(float(np.sqrt(np.mean((y_reg - _yf) ** 2))))

    # Print values at candidate cutoffs
    _idx_015 = int(np.argmin(np.abs(_cutoffs_sweep - 0.15)))
    _idx_03 = int(np.argmin(np.abs(_cutoffs_sweep - 0.3)))
    _idx_05 = int(np.argmin(np.abs(_cutoffs_sweep - 0.5)))
    print("Winter RMSR at candidate cutoffs:")
    print(f"  {'Signal':>12}  {'0.15 Hz':>10}  {'0.3 Hz':>10}  {'0.5 Hz':>10}  {'ratio 0.5/0.15':>16}")
    for _lbl, _rmsr in [("X pos (m)", _rmsr_x), ("Y pos (m)", _rmsr_y)]:
        _r015, _r03, _r05 = _rmsr[_idx_015], _rmsr[_idx_03], _rmsr[_idx_05]
        print(f"  {_lbl:>12}  {_r015:>10.4f}  {_r03:>10.4f}  {_r05:>10.4f}  {_r05 / _r015:>16.3f}")

    _fig, _axes = plt.subplots(1, 2, figsize=(13, 4))
    for _ax, _rmsr, _lbl in [
        (_axes[0], _rmsr_x, "X position"),
        (_axes[1], _rmsr_y, "Y position"),
    ]:
        _ax.semilogy(_cutoffs_sweep, _rmsr, linewidth=1.2)
        _ax.axvline(0.15, color="mediumseagreen", linestyle="--", linewidth=1.2, label="0.15 Hz")
        _ax.axvline(0.3, color="dodgerblue", linestyle="--", linewidth=1.2, label="0.3 Hz")
        _ax.axvline(0.5, color="orange", linestyle="--", linewidth=1.2, label="0.5 Hz")
        _ax.set_xlabel("Cutoff frequency (Hz)")
        _ax.set_ylabel("RMS residual (m), log scale")
        _ax.set_title(f"Winter residual (log) — {_lbl}")
        _ax.legend(fontsize=9)

    plt.suptitle("Winter residual analysis — log scale", y=1.02)
    plt.tight_layout()
    plt.gca()
    return


@app.cell
def _(mo):
    mo.md("""
    ---

    ## Part A-2: Welch PSD — Single-Lap Detrended XY Position

    **Why detrend, and why a single lap?**

    Over a full race, the absolute X and Y coordinates repeat the circuit 56 times.
    The PSD is completely dominated by the lap-period component (~0.011 Hz) and its
    harmonics — everything above ~0.05 Hz is buried. Within a single lap, the
    coordinates still drift from start to finish (a roughly linear trend). `scipy.signal.detrend`
    removes this linear component, leaving the non-linear position deviations — exactly
    the corner geometry that our Butterworth filter acts on.

    The resulting PSD shows the spectral content of within-lap position curvature:
    corner traversals appear as energy at frequencies corresponding to their duration.

    **How to read this plot:**
    - A steep rolloff → signal energy is concentrated below the rolloff point; a cutoff
      there retains most of the geometric signal
    - A flat region (noise floor) → energy there is GPS noise, not corner geometry
    - The frequency where the PSD transitions from rolloff to flat is the natural cutoff

    **Frequency reference for a ~90s F1 lap at Shanghai:**
    | Feature | Approx. frequency |
    |---|---|
    | Slow corner traversal — hairpins (T1, T14), 3–5 s duration | ~0.2–0.33 Hz |
    | Medium corners — esses (T3–T6), 1.5–3 s duration | ~0.33–0.67 Hz |
    | Nyquist at 4 Hz sampling | 2.0 Hz |
    """)
    return


@app.cell
def _(DRIVER_NAME, FS, lap_df, plt, t_reg, welch, x_reg, y_reg):
    # Use lap 16 (index 15) as the representative lap
    _lap_idx = 15
    _t0 = lap_df.iloc[_lap_idx]["date_start"].timestamp()
    _t1 = lap_df.iloc[_lap_idx + 1]["date_start"].timestamp()
    _mask = (t_reg >= _t0) & (t_reg < _t1)

    _x_lap = x_reg[_mask]
    _y_lap = y_reg[_mask]

    _nperseg = min(128, len(_x_lap) // 2)
    _freqs_x, _psd_x = welch(_x_lap, fs=FS, nperseg=_nperseg)
    _freqs_y, _psd_y = welch(_y_lap, fs=FS, nperseg=_nperseg)

    # Skip the DC bin (freq=0) for log-log — it's infinite on log scale
    _fig, _axes = plt.subplots(1, 2, figsize=(14, 5))
    for _ax, _freqs, _psd, _lbl in [
        (_axes[0], _freqs_x[1:], _psd_x[1:], "X position"),
        (_axes[1], _freqs_y[1:], _psd_y[1:], "Y position"),
    ]:
        _ax.loglog(_freqs, _psd, linewidth=1.2)
        _ax.axvline(0.15, color="mediumseagreen", linestyle="--", linewidth=1.2, label="0.15 Hz")
        _ax.axvline(0.3, color="dodgerblue", linestyle="--", linewidth=1.2, label="0.3 Hz")
        _ax.axvline(0.5, color="orange", linestyle="--", linewidth=1.2, label="0.5 Hz")
        _ax.axvline(FS / 2, color="grey", linestyle=":", linewidth=1.0, label=f"Nyquist {FS/2:.0f} Hz")
        _ax.set_xlabel("Frequency (Hz)")
        _ax.set_ylabel("PSD (m² / Hz)")
        _ax.set_title(f"Welch PSD — {_lbl}, {DRIVER_NAME} lap {_lap_idx + 1}")
        _ax.legend(fontsize=9)

    plt.suptitle("Single-lap XY position PSD (log-log) — rolloff shape identifies signal vs. noise band", y=1.02)
    plt.tight_layout()
    plt.gca()
    return


@app.cell
def _(FS, detrend, lap_df, np, t_reg, welch, x_reg, y_reg):
    """Power fraction below each candidate cutoff for X and Y."""
    _lap_idx = 15
    _t0 = lap_df.iloc[_lap_idx]["date_start"].timestamp()
    _t1 = lap_df.iloc[_lap_idx + 1]["date_start"].timestamp()
    _mask = (t_reg >= _t0) & (t_reg < _t1)
    _x_lap = detrend(x_reg[_mask])
    _y_lap = detrend(y_reg[_mask])

    print("Welch PSD power fractions — single-lap detrended position:")
    print(f"  {'Band':>12}  {'X fraction':>12}  {'Y fraction':>12}")
    _nperseg = min(128, len(_x_lap) // 2)
    _fx, _px = welch(_x_lap, fs=FS, nperseg=_nperseg)
    _fy, _py = welch(_y_lap, fs=FS, nperseg=_nperseg)
    for _label, _lo, _hi in [
        ("< 0.15 Hz",  0.0, 0.15),
        ("0.15–0.3 Hz",0.15, 0.3),
        ("0.3–0.5 Hz", 0.3, 0.5),
        ("0.5–2 Hz",   0.5, 2.0),
    ]:
        _mx = (_fx >= _lo) & (_fx < _hi)
        _my = (_fy >= _lo) & (_fy < _hi)
        _tx = np.trapezoid(_px, _fx)
        _ty = np.trapezoid(_py, _fy)
        _xfrac = np.trapezoid(_px[_mx], _fx[_mx]) / _tx if _mx.sum() > 1 else 0.0
        _yfrac = np.trapezoid(_py[_my], _fy[_my]) / _ty if _my.sum() > 1 else 0.0
        print(f"  {_label:>12}  {_xfrac:>12.1%}  {_yfrac:>12.1%}")
    return


@app.cell
def _(mo):
    mo.md("""
    ---

    ## Part A-3: Multi-Lap Spectral Stability

    Welch PSD of detrended single-lap X position computed independently for each race lap.
    Tracks how much power sits below 0.3 Hz and 0.5 Hz across the full race.

    If the spectral structure is stable (< 5 pp variation in power fraction), the chosen
    cutoff generalises across different fuel loads, tyre states, and track evolution.
    If it varies widely, neither cutoff is universally optimal for individual laps.
    """)
    return


@app.cell
def _(
    FS,
    LAP_END_IDX,
    LAP_START_IDX,
    detrend,
    lap_df,
    np,
    t_reg,
    welch,
    x_reg,
    y_reg,
):
    psd_rows = []

    for _i in range(LAP_START_IDX, min(LAP_END_IDX, len(lap_df) - 1)):
        _t0 = lap_df.iloc[_i]["date_start"].timestamp()
        _t1 = lap_df.iloc[_i + 1]["date_start"].timestamp()
        _mask = (t_reg >= _t0) & (t_reg < _t1)
        _sx = detrend(x_reg[_mask])
        _sy = detrend(y_reg[_mask])
        if len(_sx) < 32:
            continue
        _nperseg = min(128, len(_sx) // 2)
        _frx, _prx = welch(_sx, fs=FS, nperseg=_nperseg)
        _fry, _pry = welch(_sy, fs=FS, nperseg=_nperseg)
        _tx = np.trapezoid(_prx, _frx)
        _ty = np.trapezoid(_pry, _fry)
        if _tx == 0 or _ty == 0:
            continue
        psd_rows.append({
            "lap": _i + 1,
            "x_frac_below_0.15_hz": round(float(np.trapezoid(_prx[_frx <= 0.15], _frx[_frx <= 0.15])) / _tx, 4),
            "x_frac_below_0.3_hz": round(float(np.trapezoid(_prx[_frx <= 0.3], _frx[_frx <= 0.3])) / _tx, 4),
            "x_frac_below_0.5_hz": round(float(np.trapezoid(_prx[_frx <= 0.5], _frx[_frx <= 0.5])) / _tx, 4),
            "y_frac_below_0.15_hz": round(float(np.trapezoid(_pry[_fry <= 0.15], _fry[_fry <= 0.15])) / _ty, 4),
            "y_frac_below_0.3_hz": round(float(np.trapezoid(_pry[_fry <= 0.3], _fry[_fry <= 0.3])) / _ty, 4),
            "y_frac_below_0.5_hz": round(float(np.trapezoid(_pry[_fry <= 0.5], _fry[_fry <= 0.5])) / _ty, 4),
            "freqs": _frx,
            "psd_x": _prx,
            "psd_y": _pry,
        })

    import pandas as _pd_s3
    _df = _pd_s3.DataFrame([{k: v for k, v in r.items() if k not in ("freqs", "psd_x", "psd_y")} for r in psd_rows])
    for _sig, _col015, _col03, _col05 in [
        ("X position", "x_frac_below_0.15_hz", "x_frac_below_0.3_hz", "x_frac_below_0.5_hz"),
        ("Y position", "y_frac_below_0.15_hz", "y_frac_below_0.3_hz", "y_frac_below_0.5_hz"),
    ]:
        print(f"Multi-lap spectral stability — detrended {_sig} ({len(psd_rows)} laps):")
        print(f"  % power < 0.15 Hz: mean={_df[_col015].mean()*100:.1f}%  range={(_df[_col015].max()-_df[_col015].min())*100:.1f} pp")
        print(f"  % power < 0.3 Hz:  mean={_df[_col03].mean()*100:.1f}%  range={(_df[_col03].max()-_df[_col03].min())*100:.1f} pp")
        print(f"  % power < 0.5 Hz:  mean={_df[_col05].mean()*100:.1f}%  range={(_df[_col05].max()-_df[_col05].min())*100:.1f} pp")
    _df
    return (psd_rows,)


@app.cell
def _(plt, psd_rows):
    _laps = [r["lap"] for r in psd_rows]
    _fig, _axes = plt.subplots(1, 2, figsize=(14, 4), sharey=True)
    for _ax, _col015, _col03, _col05, _lbl in [
        (_axes[0], "x_frac_below_0.15_hz", "x_frac_below_0.3_hz", "x_frac_below_0.5_hz", "X position"),
        (_axes[1], "y_frac_below_0.15_hz", "y_frac_below_0.3_hz", "y_frac_below_0.5_hz", "Y position"),
    ]:
        _ax.plot(_laps, [r[_col015] * 100 for r in psd_rows], "^-", markersize=3, linewidth=1, color="mediumseagreen", label="< 0.15 Hz")
        _ax.plot(_laps, [r[_col03] * 100 for r in psd_rows], "o-", markersize=3, linewidth=1, color="dodgerblue", label="< 0.3 Hz")
        _ax.plot(_laps, [r[_col05] * 100 for r in psd_rows], "s-", markersize=3, linewidth=1, color="orange", label="< 0.5 Hz")
        _ax.set_xlabel("Lap number")
        _ax.set_ylabel("% of detrended position power")
        _ax.set_title(f"Power below cutoff per lap — {_lbl}")
        _ax.legend()
        _ax.set_ylim(0, 110)
    plt.suptitle("Multi-lap spectral stability — detrended XY position", y=1.02)
    plt.tight_layout()
    plt.gca()
    return


@app.cell
def _(FS, plt, psd_rows):
    """PSD overlay for all race laps — X and Y position side by side (colour = lap number)."""
    _cmap = plt.get_cmap("plasma")
    _n = len(psd_rows)
    _fig, _axes = plt.subplots(1, 2, figsize=(14, 5))
    for _ax, _psd_key, _lbl in [
        (_axes[0], "psd_x", "X position"),
        (_axes[1], "psd_y", "Y position"),
    ]:
        for _idx, _row in enumerate(psd_rows):
            _ax.semilogy(_row["freqs"], _row[_psd_key], color=_cmap(_idx / max(_n - 1, 1)), alpha=0.6, linewidth=0.8)
        _ax.axvline(0.15, color="mediumseagreen", linestyle="--", linewidth=1.2, label="0.15 Hz")
        _ax.axvline(0.3, color="dodgerblue", linestyle="--", linewidth=1.2, label="0.3 Hz")
        _ax.axvline(0.5, color="orange", linestyle="--", linewidth=1.2, label="0.5 Hz")
        _ax.axvline(FS / 2, color="grey", linestyle=":", linewidth=0.8, label=f"Nyquist {FS/2:.0f} Hz")
        _ax.set_xlabel("Frequency (Hz)")
        _ax.set_ylabel("PSD (m² / Hz)")
        _ax.set_title(f"Detrended {_lbl} PSD — all race laps")
        _ax.legend(fontsize=9)

    _sm = plt.cm.ScalarMappable(cmap="plasma", norm=plt.Normalize(vmin=psd_rows[0]["lap"], vmax=psd_rows[-1]["lap"]))
    _sm.set_array([])
    _fig.colorbar(_sm, ax=_axes.tolist(), label="Lap number")
    plt.suptitle("Multi-lap PSD overlay — detrended XY position (colour = lap number)", y=1.02)
    plt.tight_layout()
    plt.gca()
    return


@app.cell
def _(mo):
    mo.md("""
    ---

    ## Part A-4: Cross-Driver Spectral Consistency

    All 20 drivers, laps 3–7 window. Welch PSD of detrended X and Y position computed
    per driver from their own PCHIP interpolation of location data. Results cached.

    If the spectral structure is consistent across teams (different cars, setups, driving
    styles), a single cutoff frequency generalises across the full grid.
    """)
    return


@app.cell
def _(
    CACHE_DIR,
    FS,
    PchipInterpolator,
    SESSION_KEY,
    USE_CACHE,
    detrend,
    drivers,
    get_laps,
    get_location,
    np,
    pd,
    pickle,
    welch,
):
    _cache_file = CACHE_DIR / "cross_driver_xy_psd_rows.pkl"

    if USE_CACHE and _cache_file.exists():
        with open(_cache_file, "rb") as _f:
            cross_driver_rows, cross_driver_failures = pickle.load(_f)
    else:
        _rows = []
        _failures = []

        for _drv in drivers:
            _dn = _drv["driver_number"]
            _acro = _drv["name_acronym"]
            _colour = (_drv.get("team_colour") or "888888").lstrip("#")
            try:
                _laps_raw = get_laps(SESSION_KEY, _dn)
                _laps_timed = [l for l in _laps_raw if l.get("date_start")]
                if len(_laps_timed) < 6:
                    _failures.append(_acro)
                    continue

                _t_start = _laps_timed[2]["date_start"]
                _t_end = _laps_timed[6]["date_start"] if len(_laps_timed) > 6 else _laps_timed[-1]["date_start"]

                _loc_raw = get_location(SESSION_KEY, _dn, _t_start, _t_end)
                if len(_loc_raw) < 40:
                    _failures.append(_acro)
                    continue

                _loc_df = pd.DataFrame(_loc_raw)
                _loc_df["date"] = pd.to_datetime(_loc_df["date"], format="ISO8601")
                _loc_df = _loc_df.sort_values("date").drop_duplicates(subset="date").reset_index(drop=True)

                _tl = _loc_df["date"].astype("int64").to_numpy() / 1e9
                _xl = _loc_df["x"].to_numpy().astype(float)
                _yl = _loc_df["y"].to_numpy().astype(float)
                _valid = ~(np.isnan(_xl) | np.isnan(_yl))
                if _valid.sum() < 20:
                    _failures.append(_acro)
                    continue

                _t_reg_d = np.arange(_tl[_valid][0], _tl[_valid][-1], 1 / FS)
                if len(_t_reg_d) < 64:
                    _failures.append(_acro)
                    continue

                _pchip_x = PchipInterpolator(_tl[_valid], _xl[_valid])
                _pchip_y = PchipInterpolator(_tl[_valid], _yl[_valid])
                _sx = detrend(_pchip_x(_t_reg_d))
                _sy = detrend(_pchip_y(_t_reg_d))

                _nperseg = min(256, len(_sx) // 4)
                _frx, _prx = welch(_sx, fs=FS, nperseg=_nperseg)
                _fry, _pry = welch(_sy, fs=FS, nperseg=_nperseg)
                _tx = np.trapezoid(_prx, _frx)
                _ty = np.trapezoid(_pry, _fry)
                if _tx == 0 or _ty == 0:
                    _failures.append(_acro)
                    continue

                _rows.append({
                    "driver": _acro,
                    "team": _drv.get("team_name", ""),
                    "team_colour": "#" + _colour,
                    "x_frac_below_0.15_hz": round(float(np.trapezoid(_prx[_frx <= 0.15], _frx[_frx <= 0.15])) / _tx, 4),
                    "x_frac_below_0.3_hz": round(float(np.trapezoid(_prx[_frx <= 0.3], _frx[_frx <= 0.3])) / _tx, 4),
                    "x_frac_below_0.5_hz": round(float(np.trapezoid(_prx[_frx <= 0.5], _frx[_frx <= 0.5])) / _tx, 4),
                    "y_frac_below_0.15_hz": round(float(np.trapezoid(_pry[_fry <= 0.15], _fry[_fry <= 0.15])) / _ty, 4),
                    "y_frac_below_0.3_hz": round(float(np.trapezoid(_pry[_fry <= 0.3], _fry[_fry <= 0.3])) / _ty, 4),
                    "y_frac_below_0.5_hz": round(float(np.trapezoid(_pry[_fry <= 0.5], _fry[_fry <= 0.5])) / _ty, 4),
                    "freqs": _frx,
                    "psd_x": _prx,
                    "psd_y": _pry,
                })
            except Exception as _e:
                _failures.append(_acro)
                print(f"  {_acro}: error — {_e}")

        cross_driver_rows = _rows
        cross_driver_failures = _failures
        with open(_cache_file, "wb") as _f:
            pickle.dump((cross_driver_rows, cross_driver_failures), _f)

    print(f"Cross-driver: {len(cross_driver_rows)} drivers processed, {len(cross_driver_failures)} failed: {cross_driver_failures}")
    return cross_driver_failures, cross_driver_rows


@app.cell
def _(cross_driver_failures, cross_driver_rows, mo):
    import pandas as _pd_s4

    _df4 = _pd_s4.DataFrame([
        {k: v for k, v in r.items() if k not in ("freqs", "psd_x", "psd_y", "team_colour")}
        for r in cross_driver_rows
    ])

    mo.md(
        f"""
    **{len(cross_driver_rows)} drivers** processed; **{len(cross_driver_failures)} excluded**: {cross_driver_failures}

    | Metric | 0.15 Hz cutoff | 0.3 Hz cutoff | 0.5 Hz cutoff |
    |---|---|---|---|
    | Mean % X power below cutoff | {_df4["x_frac_below_0.15_hz"].mean()*100:.1f}% | {_df4["x_frac_below_0.3_hz"].mean()*100:.1f}% | {_df4["x_frac_below_0.5_hz"].mean()*100:.1f}% |
    | Mean % Y power below cutoff | {_df4["y_frac_below_0.15_hz"].mean()*100:.1f}% | {_df4["y_frac_below_0.3_hz"].mean()*100:.1f}% | {_df4["y_frac_below_0.5_hz"].mean()*100:.1f}% |
    | X range across drivers (pp) | {(_df4["x_frac_below_0.15_hz"].max()-_df4["x_frac_below_0.15_hz"].min())*100:.1f} pp | {(_df4["x_frac_below_0.3_hz"].max()-_df4["x_frac_below_0.3_hz"].min())*100:.1f} pp | {(_df4["x_frac_below_0.5_hz"].max()-_df4["x_frac_below_0.5_hz"].min())*100:.1f} pp |
    | Y range across drivers (pp) | {(_df4["y_frac_below_0.15_hz"].max()-_df4["y_frac_below_0.15_hz"].min())*100:.1f} pp | {(_df4["y_frac_below_0.3_hz"].max()-_df4["y_frac_below_0.3_hz"].min())*100:.1f} pp | {(_df4["y_frac_below_0.5_hz"].max()-_df4["y_frac_below_0.5_hz"].min())*100:.1f} pp |

    | Driver | Team | X % < 0.15 Hz | X % < 0.3 Hz | X % < 0.5 Hz | Y % < 0.15 Hz | Y % < 0.3 Hz | Y % < 0.5 Hz |
    |---|---|---|---|---|---|---|---|
    """
        + "\n".join(
            f"| {r['driver']} | {r['team']} | {r['x_frac_below_0.15_hz']*100:.1f}% | {r['x_frac_below_0.3_hz']*100:.1f}% | {r['x_frac_below_0.5_hz']*100:.1f}% | {r['y_frac_below_0.15_hz']*100:.1f}% | {r['y_frac_below_0.3_hz']*100:.1f}% | {r['y_frac_below_0.5_hz']*100:.1f}% |"
            for r in sorted(cross_driver_rows, key=lambda x: x["x_frac_below_0.3_hz"])
        )
    )
    return


@app.cell
def _(FS, cross_driver_rows, plt):
    _fig, _axes = plt.subplots(1, 2, figsize=(14, 5))
    for _ax, _psd_key, _lbl in [
        (_axes[0], "psd_x", "X position"),
        (_axes[1], "psd_y", "Y position"),
    ]:
        for _row in cross_driver_rows:
            _ax.semilogy(
                _row["freqs"], _row[_psd_key],
                color=_row["team_colour"], linewidth=0.9, alpha=0.75, label=_row["driver"]
            )
        _ax.axvline(0.15, color="mediumseagreen", linestyle="--", linewidth=1.2, label="0.15 Hz")
        _ax.axvline(0.3, color="dodgerblue", linestyle="--", linewidth=1.2, label="0.3 Hz")
        _ax.axvline(0.5, color="orange", linestyle="--", linewidth=1.2, label="0.5 Hz")
        _ax.axvline(FS / 2, color="grey", linestyle=":", linewidth=0.8, label=f"Nyquist {FS/2:.0f} Hz")
        _ax.set_xlabel("Frequency (Hz)")
        _ax.set_ylabel("PSD (m² / Hz)")
        _ax.set_title(f"Detrended {_lbl} PSD — all drivers, laps 3–7")
        _ax.legend(fontsize=6, ncol=4, loc="upper right")
    plt.suptitle("Cross-driver PSD overlay — detrended XY position (colour = team colour)", y=1.02)
    plt.tight_layout()
    plt.gca()
    return


@app.cell
def _(cross_driver_rows, plt):
    """Sorted bar chart of % power below each candidate cutoff, per driver — X and Y side by side."""
    _sorted_x = sorted(cross_driver_rows, key=lambda r: r["x_frac_below_0.3_hz"])
    _drivers = [r["driver"] for r in _sorted_x]
    _x = range(len(_drivers))

    _fig, _axes = plt.subplots(1, 2, figsize=(14, 5), sharey=True)
    for _ax, _col015, _col03, _col05, _lbl in [
        (_axes[0], "x_frac_below_0.15_hz", "x_frac_below_0.3_hz", "x_frac_below_0.5_hz", "X position"),
        (_axes[1], "y_frac_below_0.15_hz", "y_frac_below_0.3_hz", "y_frac_below_0.5_hz", "Y position"),
    ]:
        _ax.bar([i - 0.27 for i in _x], [r[_col015] * 100 for r in _sorted_x], width=0.25, color="mediumseagreen", alpha=0.8, label="< 0.15 Hz")
        _ax.bar([i        for i in _x], [r[_col03]  * 100 for r in _sorted_x], width=0.25, color="dodgerblue",     alpha=0.8, label="< 0.3 Hz")
        _ax.bar([i + 0.27 for i in _x], [r[_col05]  * 100 for r in _sorted_x], width=0.25, color="orange",         alpha=0.8, label="< 0.5 Hz")
        _ax.set_xticks(list(_x))
        _ax.set_xticklabels(_drivers, rotation=45, ha="right")
        _ax.set_ylabel("% of detrended position power")
        _ax.set_title(f"Power below cutoff — {_lbl} (sorted by 0.3 Hz fraction)")
        _ax.legend()
    plt.suptitle("Cross-driver spectral consistency — detrended XY position", y=1.02)
    plt.tight_layout()
    plt.gca()
    return


@app.cell
def _(mo):
    mo.md("""
    ---

    ## Part B: Tyre Wear vs. Lateral G

    Build a per-lap dataframe covering all drivers and all race laps:

    - **driver**, **team**, **compound**, **lap_number**, **tyre_age_laps** — identity and tyre state
    - **peak_lat_g_s1/s2/s3/lap** — peak lateral G per sector and full lap, computed via the
      yaw-rate method with a 0.3 Hz Butterworth filter (validated in Part A)

    Tyre age is derived from the laps endpoint: laps elapsed since the last `is_pit_out_lap`.
    Compound comes from the `compound` field in the laps endpoint.
    Sector boundaries come from `duration_sector_1/2/3` in the laps endpoint.
    """)
    return


@app.cell
def _(
    CACHE_DIR,
    FS,
    PchipInterpolator,
    SESSION_KEY,
    USE_CACHE,
    butter,
    drivers,
    filtfilt,
    get_car_data,
    get_laps,
    get_location,
    np,
    pd,
    pickle,
):
    _cache_file = CACHE_DIR / "tyre_wear_lat_g_df.pkl"

    if USE_CACHE and _cache_file.exists():
        with open(_cache_file, "rb") as _f:
            tyre_lat_g_df = pickle.load(_f)
    else:
        _G = 9.81
        _LAT_G_MAX = 7.0
        _CUTOFF_HZ = 0.15
        _b, _a = butter(N=4, Wn=_CUTOFF_HZ / (FS / 2), btype="low")

        _rows = []

        for _drv in drivers:
            _dn = _drv["driver_number"]
            _acro = _drv["name_acronym"]
            _team = _drv.get("team_name", "")

            try:
                _laps_raw = get_laps(SESSION_KEY, _dn)
                _lap_df = pd.DataFrame(_laps_raw)
                _lap_df["date_start"] = pd.to_datetime(_lap_df["date_start"], format="ISO8601")
                _lap_df = _lap_df.sort_values("lap_number").dropna(subset=["date_start"]).reset_index(drop=True)
                if len(_lap_df) < 3:
                    continue

                # Compute tyre age from laps: count laps since last pit-out lap
                _tyre_age_col = []
                _age = 0
                for _, _lr in _lap_df.iterrows():
                    if _lr.get("is_pit_out_lap"):
                        _age = 0
                    _tyre_age_col.append(_age)
                    _age += 1
                _lap_df["tyre_age_laps"] = _tyre_age_col

                _t_start = _lap_df.iloc[0]["date_start"].strftime("%Y-%m-%dT%H:%M:%S")
                _t_end   = _lap_df.iloc[-1]["date_start"].strftime("%Y-%m-%dT%H:%M:%S")

                _loc_raw = get_location(SESSION_KEY, _dn, _t_start, _t_end)
                _car_raw = get_car_data(SESSION_KEY, _dn, _t_start, _t_end)
                if len(_loc_raw) < 40 or len(_car_raw) < 40:
                    continue

                # PCHIP resample XY + speed to 4 Hz grid
                _loc_df = pd.DataFrame(_loc_raw)
                _loc_df["date"] = pd.to_datetime(_loc_df["date"], format="ISO8601")
                _loc_df = _loc_df.sort_values("date").drop_duplicates(subset="date").reset_index(drop=True)
                _tl = _loc_df["date"].astype("int64").to_numpy() / 1e9
                _xl = _loc_df["x"].to_numpy().astype(float)
                _yl = _loc_df["y"].to_numpy().astype(float)
                _valid_loc = ~(np.isnan(_xl) | np.isnan(_yl))
                if _valid_loc.sum() < 20:
                    continue

                _car_df = pd.DataFrame(_car_raw)
                _car_df["date"] = pd.to_datetime(_car_df["date"], format="ISO8601")
                _car_df = _car_df.sort_values("date").drop_duplicates(subset="date").reset_index(drop=True)
                _tc = _car_df["date"].astype("int64").to_numpy() / 1e9
                _spd = _car_df["speed"].to_numpy().astype(float)
                _valid_spd = ~np.isnan(_spd)
                if _valid_spd.sum() < 20:
                    continue

                _t_reg = np.arange(_tl[_valid_loc][0], _tl[_valid_loc][-1], 1 / FS)
                _x_reg = PchipInterpolator(_tl[_valid_loc], _xl[_valid_loc])(_t_reg)
                _y_reg = PchipInterpolator(_tl[_valid_loc], _yl[_valid_loc])(_t_reg)

                # Clip to car_data time range to avoid extrapolation
                _tc_min, _tc_max = _tc[_valid_spd][0], _tc[_valid_spd][-1]
                _spd_reg = PchipInterpolator(_tc[_valid_spd], _spd[_valid_spd])(
                    np.clip(_t_reg, _tc_min, _tc_max)
                ) / 3.6  # kph → m/s

                # Butterworth 0.3 Hz filter on XY
                _padlen = min(len(_x_reg) // 4, int(5 * FS))
                _xf = filtfilt(_b, _a, _x_reg, padlen=_padlen)
                _yf = filtfilt(_b, _a, _y_reg, padlen=_padlen)

                # Yaw rate → lateral G
                _dt = 1 / FS
                _vx = np.gradient(_xf, _dt)
                _vy = np.gradient(_yf, _dt)
                _heading = np.unwrap(np.arctan2(_vy, _vx))
                _yaw_rate = np.gradient(_heading, _dt)
                _lat_g = np.minimum(np.abs(_spd_reg * _yaw_rate) / _G, _LAT_G_MAX)

                for _i in range(len(_lap_df) - 1):
                    _row = _lap_df.iloc[_i]
                    _lap_num = int(_row["lap_number"])
                    _s1dur = _row.get("duration_sector_1")
                    _s2dur = _row.get("duration_sector_2")
                    _s3dur = _row.get("duration_sector_3")
                    if any(pd.isna(v) or v is None for v in [_s1dur, _s2dur, _s3dur]):
                        continue

                    _t0        = _row["date_start"].timestamp()
                    _t_s1_end  = _t0 + float(_s1dur)
                    _t_s2_end  = _t_s1_end + float(_s2dur)
                    _t_lap_end = _t_s2_end + float(_s3dur)

                    _m_lap = (_t_reg >= _t0)       & (_t_reg < _t_lap_end)
                    _m_s1  = (_t_reg >= _t0)       & (_t_reg < _t_s1_end)
                    _m_s2  = (_t_reg >= _t_s1_end) & (_t_reg < _t_s2_end)
                    _m_s3  = (_t_reg >= _t_s2_end) & (_t_reg < _t_lap_end)
                    if _m_lap.sum() < 4:
                        continue

                    _rows.append({
                        "driver":         _acro,
                        "team":           _team,
                        "team_colour":    "#" + (_drv.get("team_colour") or "888888").lstrip("#"),
                        "compound":       _row.get("compound"),
                        "lap_number":     _lap_num,
                        "tyre_age_laps":  int(_row["tyre_age_laps"]),
                        "peak_lat_g_s1":  float(np.nanmax(_lat_g[_m_s1]))   if _m_s1.sum()  > 0 else None,
                        "peak_lat_g_s2":  float(np.nanmax(_lat_g[_m_s2]))   if _m_s2.sum()  > 0 else None,
                        "peak_lat_g_s3":  float(np.nanmax(_lat_g[_m_s3]))   if _m_s3.sum()  > 0 else None,
                        "peak_lat_g_lap": float(np.nanmax(_lat_g[_m_lap]))  if _m_lap.sum() > 0 else None,
                    })

            except Exception as _e:
                print(f"  {_acro}: error — {_e}")

        tyre_lat_g_df = pd.DataFrame(_rows)
        with open(_cache_file, "wb") as _f:
            pickle.dump(tyre_lat_g_df, _f)

    print(f"Rows: {len(tyre_lat_g_df)}  |  Drivers: {tyre_lat_g_df['driver'].nunique()}  |  Compounds: {sorted(tyre_lat_g_df['compound'].dropna().unique())}")
    tyre_lat_g_df
    return (tyre_lat_g_df,)


@app.cell
def _(mo):
    mo.md("""
    ---

    ## Part C-1: Peak Lateral G Distribution — Grid Overview

    Overall histogram of peak lateral G per lap (full lap) for all drivers, plus
    per-driver box plots sorted by median — mirroring the acceleration/deceleration
    justification plots in `noise.py`.
    """)
    return


@app.cell
def _(plt, tyre_lat_g_df):
    """Overall histogram of peak_lat_g_lap — grid-wide distribution."""
    _df_hist = tyre_lat_g_df.dropna(subset=["peak_lat_g_lap"])
    _mean = _df_hist["peak_lat_g_lap"].mean()
    _std  = _df_hist["peak_lat_g_lap"].std()

    _fig_h, _ax_h = plt.subplots(figsize=(10, 4))
    _ax_h.hist(_df_hist["peak_lat_g_lap"], bins=40, edgecolor="none", color="steelblue", alpha=0.8)
    _ax_h.axvline(_mean,        color="orange", linestyle="--", linewidth=1.5, label=f"Mean: {_mean:.2f} g")
    _ax_h.axvline(_mean - _std, color="orange", linestyle=":",  linewidth=1.2, alpha=0.7, label=f"±1 SD: {_std:.2f} g")
    _ax_h.axvline(_mean + _std, color="orange", linestyle=":",  linewidth=1.2, alpha=0.7)
    _ax_h.set_xlabel("Peak lateral G (full lap)")
    _ax_h.set_ylabel("Count (laps)")
    _ax_h.set_title("Grid: peak lateral G per lap — all drivers, 2026 Chinese GP Race (0.15 Hz filter)")
    _ax_h.legend(fontsize=9)
    plt.tight_layout()
    plt.gca()
    return


@app.cell
def _(plt, tyre_lat_g_df):
    """Per-driver box plots of peak_lat_g_lap, sorted by median, coloured by team."""
    _df_box = tyre_lat_g_df.dropna(subset=["peak_lat_g_lap"])
    _drv_order = (
        _df_box.groupby("driver")["peak_lat_g_lap"]
        .median()
        .sort_values(ascending=False)
        .index.tolist()
    )
    _colours = {
        row["driver"]: row["team_colour"]
        for _, row in _df_box[["driver", "team_colour"]].drop_duplicates().iterrows()
    }
    _data = [_df_box.loc[_df_box["driver"] == d, "peak_lat_g_lap"].values for d in _drv_order]

    _fig_bp, _ax_bp = plt.subplots(figsize=(14, 5))
    _bp = _ax_bp.boxplot(_data, patch_artist=True, medianprops=dict(color="white", linewidth=1.5))
    for _patch, _drv in zip(_bp["boxes"], _drv_order):
        _patch.set_facecolor(_colours.get(_drv, "#888888"))
        _patch.set_alpha(0.85)
    _ax_bp.set_xticks(range(1, len(_drv_order) + 1))
    _ax_bp.set_xticklabels(_drv_order, rotation=45, ha="right")
    _ax_bp.set_ylabel("Peak lateral G (full lap)")
    _ax_bp.set_title("Per-driver peak lateral G distribution — sorted by median (0.15 Hz filter)")
    plt.tight_layout()
    plt.gca()
    return


@app.cell
def _(plt, tyre_lat_g_df):
    """Tyre age vs. peak lateral G — one subplot per sector + full lap, coloured by compound."""
    _compound_colours = {
        "SOFT": "tomato", "MEDIUM": "gold", "HARD": "lightgrey",
        "INTERMEDIATE": "limegreen", "WET": "dodgerblue",
    }
    _df = tyre_lat_g_df.dropna(subset=["tyre_age_laps", "compound"])

    _fig, _axes = plt.subplots(2, 2, figsize=(14, 10), sharex=True)
    for _ax, _col, _title in [
        (_axes[0, 0], "peak_lat_g_s1",  "Sector 1"),
        (_axes[0, 1], "peak_lat_g_s2",  "Sector 2"),
        (_axes[1, 0], "peak_lat_g_s3",  "Sector 3"),
        (_axes[1, 1], "peak_lat_g_lap", "Full Lap"),
    ]:
        for _cpd, _grp in _df.dropna(subset=[_col]).groupby("compound"):
            _ax.scatter(
                _grp["tyre_age_laps"], _grp[_col],
                color=_compound_colours.get(_cpd, "purple"),
                alpha=0.4, s=12, label=_cpd,
            )
        _ax.set_ylabel("Peak lateral G")
        _ax.set_title(_title)
        _ax.set_ylim(0, 7)

    for _ax in _axes[1]:
        _ax.set_xlabel("Tyre age (laps)")

    _handles = [
        plt.Line2D([0], [0], marker="o", color="w",
                   markerfacecolor=_compound_colours.get(_c, "purple"),
                   markersize=8, label=_c)
        for _c in sorted(_df["compound"].dropna().unique())
    ]
    _fig.legend(handles=_handles, loc="upper right", title="Compound", fontsize=9)
    plt.suptitle("Peak lateral G vs. tyre age — all drivers, 2026 Chinese GP Race", y=1.01)
    plt.tight_layout()
    plt.gca()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
