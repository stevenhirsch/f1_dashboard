import marimo

__generated_with = "0.20.4"
app = marimo.App(width="full", app_title="F1 Strategy Dashboard")


@app.cell
def _imports():
    import marimo as mo
    import sys
    import os
    sys.path.insert(0, os.path.dirname(__file__))

    from api import openf1
    from plots import tyre_strats, lap_map, lap_times
    import matplotlib
    matplotlib.use("Agg")
    return mo, openf1, tyre_strats, lap_map, lap_times


# ---------------------------------------------------------------------------
# Section 1 — Session selection
# ---------------------------------------------------------------------------

@app.cell
def _year_selector(mo):
    year_ui = mo.ui.dropdown(
        options=[str(y) for y in range(2026, 2022, -1)],
        value="2026",
        label="Year",
    )
    year_ui
    return (year_ui,)


@app.cell
def _fetch_meetings(mo, openf1, year_ui):
    _year = int(year_ui.value)
    _meetings_raw = openf1.get_meetings(_year)
    meeting_options = {
        m["meeting_name"]: m["meeting_key"]
        for m in _meetings_raw
        if m.get("meeting_name") and "pre-season" not in m["meeting_name"].lower()
        and "testing" not in m["meeting_name"].lower()
    }
    meeting_ui = mo.ui.dropdown(
        options=list(meeting_options.keys()),
        value=list(meeting_options.keys())[0] if meeting_options else None,
        label="Grand Prix",
    )
    meeting_ui
    return meeting_ui, meeting_options


@app.cell
def _fetch_sessions(mo, openf1, meeting_ui, meeting_options):
    _meeting_key = meeting_options.get(meeting_ui.value)
    _sessions_raw = openf1.get_sessions(_meeting_key) if _meeting_key else []
    _ALLOWED = {"Race", "Qualifying", "Sprint", "Sprint Qualifying", "Sprint Shootout"}
    session_options = {
        s["session_name"]: s["session_key"]
        for s in _sessions_raw
        if s.get("session_name") and s["session_name"] in _ALLOWED
    }
    _default_session = "Race" if "Race" in session_options else (list(session_options.keys())[0] if session_options else None)
    session_ui = mo.ui.dropdown(
        options=list(session_options.keys()),
        value=_default_session,
        label="Session",
    )
    session_ui
    return session_ui, session_options


@app.cell
def _load_session_data(openf1, session_ui, session_options, mo):
    _session_key = session_options.get(session_ui.value)
    if _session_key:
        with mo.status.spinner("Loading session data…"):
            stints = openf1.get_stints(_session_key)
            laps = openf1.get_laps(_session_key)
            drivers = openf1.get_drivers(_session_key)
            pit_stops = openf1.get_pit(_session_key)
            race_control = openf1.get_race_control(_session_key)
            weather = openf1.get_weather(_session_key)
            all_positions = openf1.get_all_positions(_session_key)
            session_result = openf1.get_session_result(_session_key)
            session_key = _session_key
    else:
        stints = laps = drivers = pit_stops = race_control = weather = all_positions = session_result = []
        session_key = None

    return session_key, stints, laps, drivers, pit_stops, race_control, weather, all_positions, session_result


# ---------------------------------------------------------------------------
# Section 2 — Race Summary
# ---------------------------------------------------------------------------

@app.cell
def _gp_history_table(mo, openf1, meeting_ui, tyre_strats):
    import pandas as _pd_hist
    import statistics as _stats_hist
    import time as _time_hist

    _gp_name = meeting_ui.value
    mo.stop(not _gp_name, mo.md("*Select a Grand Prix.*"))

    _available_years = list(range(2023, 2027))
    _hist_rows = []

    for _yr in _available_years:
        # Find this GP in the given year
        _yr_meetings = openf1.get_meetings(_yr)
        _match = next(
            (m for m in _yr_meetings if m.get("meeting_name") == _gp_name), None
        )
        if _match is None:
            continue

        # Get race session
        _yr_sessions = openf1.get_sessions(_match["meeting_key"])
        _race_sess = next(
            (s for s in _yr_sessions if s.get("session_name") == "Race"), None
        )
        if _race_sess is None:
            continue

        _sk = _race_sess["session_key"]
        _yr_laps = openf1.get_laps(_sk)
        _yr_rc = openf1.get_race_control(_sk)
        _yr_weather = openf1.get_weather(_sk)

        if not _yr_laps:
            continue

        # Filter to clean laps only (not under SC/VSC/Red Flag)
        _neutral = tyre_strats.neutralized_lap_numbers(_yr_rc, _yr_laps)
        _clean = [
            float(_l["lap_duration"])
            for _l in _yr_laps
            if _l.get("lap_number") not in _neutral
            and _l.get("lap_duration")
            and float(_l.get("lap_duration") or 0) > 0
        ]
        if not _clean:
            continue

        # Weather stats
        _track_temps = [
            float(w["track_temperature"])
            for w in _yr_weather
            if w.get("track_temperature") is not None
        ]
        _avg_track_temp = (
            f"{_stats_hist.mean(_track_temps):.1f}" if _track_temps else "—"
        )
        _had_rain = any((w.get("rainfall") or 0) > 0 for w in _yr_weather)

        _hist_rows.append({
            "Year": _yr,
            "Avg Lap (s)": f"{_stats_hist.mean(_clean):.3f}",
            "Fastest Lap (s)": f"{min(_clean):.3f}",
            "Avg Track Temp (°C)": _avg_track_temp,
            "Conditions": "Wet" if _had_rain else "Dry",
        })
        _time_hist.sleep(0.3)  # avoid rate-limiting between year fetches

    mo.stop(not _hist_rows, mo.md(f"*No historical data found for {_gp_name}.*"))
    _hist_df = _pd_hist.DataFrame(_hist_rows)
    mo.vstack([
        mo.md(f"## {_gp_name} — Year-on-Year Comparison"),
        mo.ui.table(_hist_df, selection=None),
    ])


@app.cell
def _race_results_table(mo, laps, drivers, pit_stops, race_control, tyre_strats, session_result):
    import pandas as _pd_results

    mo.stop(not laps or not drivers, mo.md("*No data loaded.*"))

    _drv_lookup = {d["driver_number"]: d for d in drivers}

    # Lap numbers under SC/VSC/Red Flag — excluded from best lap calculation
    _neutral = tyre_strats.neutralized_lap_numbers(race_control, laps)

    # Last lap per driver + clean lap durations per driver
    _by_driver: dict = {}
    _clean_laps: dict = {}
    for _l in laps:
        _dn = _l.get("driver_number")
        if _dn is None:
            continue
        if _dn not in _by_driver or (_l.get("lap_number") or 0) > (_by_driver[_dn].get("lap_number") or 0):
            _by_driver[_dn] = _l
        _lap_num = _l.get("lap_number")
        _dur = _l.get("lap_duration")
        if _lap_num and _lap_num not in _neutral and _dur:
            try:
                _f = float(_dur)
                if _f > 0:
                    _clean_laps.setdefault(_dn, []).append(_f)
            except (TypeError, ValueError):
                pass

    # Pit count per driver
    _pit_count: dict = {}
    for _p in pit_stops:
        _dn2 = _p.get("driver_number")
        if _dn2:
            _pit_count[_dn2] = _pit_count.get(_dn2, 0) + 1

    # Official results from /session_result
    # Fields: position, dnf (bool), dns (bool), dsq (bool),
    #         gap_to_leader (float seconds | "+N LAP" string | 0 for winner),
    #         duration (total race seconds for winner, null otherwise)
    _result_by_driver: dict = {r["driver_number"]: r for r in session_result if r.get("driver_number")}

    def _fmt_duration(total_s: float) -> str:
        h = int(total_s // 3600)
        m = int((total_s % 3600) // 60)
        s = total_s % 60
        if h > 0:
            return f"{h}:{m:02d}:{s:06.3f}"
        return f"{m}:{s:06.3f}"

    def _fmt_gap(gap) -> str:
        if gap == 0:
            return ""  # winner — use duration instead
        if isinstance(gap, str):
            # e.g. "+1 LAP", "+2 LAPS" — normalise capitalisation
            return gap.title()
        try:
            return f"+{float(gap):.3f}s"
        except (TypeError, ValueError):
            return str(gap)

    _rows = []
    # Union of drivers seen in session_result and drivers list
    _all_driver_numbers = (
        {r["driver_number"] for r in session_result if r.get("driver_number")}
        | {d["driver_number"] for d in drivers if d.get("driver_number")}
    )

    for _dn in _all_driver_numbers:
        _drv = _drv_lookup.get(_dn, {})
        _full_name = _drv.get("full_name") or _drv.get("name_acronym") or str(_dn)
        _result = _result_by_driver.get(_dn, {})
        _last_lap = _by_driver.get(_dn)

        _pos_raw = _result.get("position")
        _is_dnf = bool(_result.get("dnf"))
        _is_dns = bool(_result.get("dns"))
        _is_dsq = bool(_result.get("dsq"))
        _gap = _result.get("gap_to_leader")
        _duration = _result.get("duration")

        _drv_clean = _clean_laps.get(_dn, [])
        _best = f"{min(_drv_clean):.3f}" if _drv_clean else "—"
        _driver_laps = (_last_lap.get("lap_number") or 0) if _last_lap else 0

        if _is_dns:
            _pos_display, _time_display, _sort_key = "DNS", "DNS", 9999
        elif _is_dsq:
            _pos_display, _time_display, _sort_key = "DSQ", "DSQ", 9950
        elif _is_dnf:
            _pos_display, _time_display, _sort_key = "DNF", "DNF", 9900
        elif _pos_raw is not None:
            _pos_display = int(_pos_raw)
            _sort_key = int(_pos_raw)
            if _gap == 0 and _duration:
                _time_display = _fmt_duration(float(_duration))
            else:
                _time_display = _fmt_gap(_gap) if _gap is not None else "—"
        else:
            _pos_display, _time_display, _sort_key = "—", "—", 998

        _rows.append({
            "Pos": _pos_display,
            "Driver": _full_name,
            "Team": _drv.get("team_name", "—"),
            "Laps": _driver_laps if _driver_laps else "—",
            "Time": _time_display,
            "Pits": _pit_count.get(_dn, 0),
            "Best Lap (s)": _best,
            "_sort": _sort_key,
        })

    _rows.sort(key=lambda r: r["_sort"])
    for _r in _rows:
        del _r["_sort"]
    _df = _pd_results.DataFrame(_rows)
    mo.vstack([
        mo.md("## Race Results"),
        mo.ui.table(_df, selection=None),
    ])


@app.cell
def _tyre_strategy_section(
    mo, tyre_strats, stints, laps, race_control, drivers, session_ui, all_positions
):
    mo.stop(not stints or not drivers, mo.md("*Select a session to view tyre strategy.*"))

    # Build finish position from /position endpoint: last recorded position per driver
    _strat_pos: dict[int, int] = {}
    for _pentry in all_positions:
        _dn4 = _pentry.get("driver_number")
        _pval = _pentry.get("position")
        if _dn4 is not None and _pval is not None:
            if _dn4 not in _strat_pos:
                _strat_pos[_dn4] = (_pentry.get("date", ""), _pval)
            elif _pentry.get("date", "") > _strat_pos[_dn4][0]:
                _strat_pos[_dn4] = (_pentry.get("date", ""), _pval)
    _strat_final: dict[int, int] = {dn: v[1] for dn, v in _strat_pos.items()}

    _drivers_with_stints = {s["driver_number"] for s in stints if s.get("driver_number")}
    _ordered = sorted(
        [d for d in drivers if d["driver_number"] in _drivers_with_stints],
        key=lambda d: (_strat_final.get(d["driver_number"]) or 99),
    )

    _fig = tyre_strats.plot_stints(
        stints=stints,
        laps=laps,
        race_control=race_control,
        drivers_ordered=_ordered,
        title=f"{session_ui.value} — Tyre Strategy",
    )

    mo.vstack([
        mo.md("## Tyre Strategy"),
        mo.as_html(_fig),
    ])


# ---------------------------------------------------------------------------
# Section 3 — Driver Deep Dive
# ---------------------------------------------------------------------------

@app.cell
def _driver_selector(mo, drivers):
    if not drivers:
        driver_ui = mo.ui.dropdown(options=[], label="Driver")
    else:
        _opts = {d["name_acronym"]: d["driver_number"] for d in drivers if d.get("name_acronym")}
        driver_ui = mo.ui.dropdown(
            options=list(_opts.keys()),
            value=list(_opts.keys())[0] if _opts else None,
            label="Driver",
        )
    driver_ui
    return (driver_ui,)


@app.cell
def _lap_selector(mo, laps, driver_ui, drivers):
    _drv_map = {d["name_acronym"]: d["driver_number"] for d in drivers if d.get("name_acronym")}
    driver_number = _drv_map.get(driver_ui.value) if driver_ui.value else None

    _driver_laps = sorted(
        [l for l in laps if l.get("driver_number") == driver_number and l.get("lap_number")],
        key=lambda l: l["lap_number"],
    )
    _lap_nums = [l["lap_number"] for l in _driver_laps]

    if _lap_nums:
        lap_slider = mo.ui.slider(
            start=min(_lap_nums),
            stop=max(_lap_nums),
            step=1,
            value=min(_lap_nums),
            label="Lap",
        )
    else:
        lap_slider = mo.ui.slider(start=1, stop=1, value=1, label="Lap")

    lap_slider
    return lap_slider, driver_number


@app.cell
def _lap_info_panel(
    mo, laps, stints, pit_stops, race_control, weather,
    driver_ui, lap_slider, driver_number,
):
    from datetime import datetime

    def _parse_dt(s: str):
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return None

    _selected_lap = lap_slider.value

    _stint = next(
        (s for s in stints
         if s.get("driver_number") == driver_number
         and (s.get("lap_start") or 0) <= _selected_lap <= (s.get("lap_end") or 999)),
        None,
    )
    if _stint:
        _compound = _stint.get("compound", "?")
        _laps_on_tyre = _selected_lap - (_stint.get("lap_start") or 1) + 1
        _tyre_info = f"{_compound} — {_laps_on_tyre} laps on tyre"
    else:
        _tyre_info = "—"

    _lap_data = next(
        (l for l in laps
         if l.get("driver_number") == driver_number and l.get("lap_number") == _selected_lap),
        None,
    )
    _position = str(_lap_data.get("position", "—")) if _lap_data else "—"

    _pit_laps = sorted(
        [p.get("lap_number") for p in pit_stops
         if p.get("driver_number") == driver_number and p.get("lap_number")],
    )
    _is_pitout = (_selected_lap - 1) in _pit_laps if _pit_laps else False
    _prev_pits = [pl for pl in _pit_laps if pl < _selected_lap]
    _laps_since_pit = (_selected_lap - max(_prev_pits)) if _prev_pits else _selected_lap
    _pit_info = ("Pit-out | " if _is_pitout else "") + f"{_laps_since_pit} laps since pit"

    _lap_start_dt = _parse_dt(_lap_data["date_start"]) if _lap_data and _lap_data.get("date_start") else None
    _active_flags = []
    if _lap_start_dt:
        for _ev in race_control:
            _ev_dt = _parse_dt(_ev.get("date") or "")
            if _ev_dt and _ev_dt <= _lap_start_dt:
                _cat = _ev.get("category", "")
                _msg = (_ev.get("message") or "").upper()
                if _cat == "Flag" and ("YELLOW" in _msg or "RED" in _msg):
                    _active_flags.append(_ev.get("message", ""))
                elif _cat in ("SafetyCar", "VirtualSafetyCar") and "DEPLOYED" in _msg:
                    _active_flags.append(_ev.get("message", ""))
    _flag_info = _active_flags[-1] if _active_flags else "None"

    _w_times = [(_parse_dt(w.get("date") or ""), w) for w in weather if w.get("date")]
    _w_times = [(t, w) for t, w in _w_times if t is not None]
    _weather_str = "—"
    if _w_times and _lap_start_dt:
        _cw = min(_w_times, key=lambda tw: abs((tw[0] - _lap_start_dt).total_seconds()))
        _wd = _cw[1]
        _weather_str = (
            f"Track {_wd.get('track_temperature', '?')}°C "
            f"Air {_wd.get('air_temperature', '?')}°C "
            f"Rain {_wd.get('rainfall', 0)}mm"
        )

    _dur = _lap_data.get("lap_duration") if _lap_data else None
    _dur_str = f"{float(_dur):.3f}s" if _dur else "—"

    mo.vstack([
        mo.md(f"## {driver_ui.value} — Lap {_selected_lap}"),
        mo.hstack([
            mo.stat(label="Lap Time", value=_dur_str),
            mo.stat(label="Tyre", value=_tyre_info),
            mo.stat(label="Pit Status", value=_pit_info),
            mo.stat(label="Weather", value=_weather_str),
        ]),
    ])


@app.cell
def _lap_map_section(
    mo, openf1, session_key, driver_number, lap_slider, driver_ui, lap_map, laps
):
    import datetime as _dt_mod

    _selected_lap2 = lap_slider.value
    mo.stop(not session_key or not driver_number, mo.md("*Select a driver.*"))

    # Find date_start for the selected lap and the next lap (to bound the query)
    _driver_laps_sorted = sorted(
        [l for l in laps if l.get("driver_number") == driver_number and l.get("date_start")],
        key=lambda l: l.get("lap_number", 0),
    )
    _sel_lap_data = next((l for l in _driver_laps_sorted if l.get("lap_number") == _selected_lap2), None)
    _next_lap_data = next((l for l in _driver_laps_sorted if l.get("lap_number") == _selected_lap2 + 1), None)

    mo.stop(_sel_lap_data is None, mo.md("*No date info for this lap.*"))

    _date_start = _sel_lap_data["date_start"]
    if _next_lap_data:
        _date_end = _next_lap_data["date_start"]
    else:
        # Last lap: use date_start + lap duration as a bound
        _dur = _sel_lap_data.get("lap_duration") or 120
        _dt = _dt_mod.datetime.fromisoformat(_date_start.replace("Z", "+00:00"))
        _date_end = (_dt + _dt_mod.timedelta(seconds=float(_dur) + 5)).isoformat()

    with mo.status.spinner(f"Loading telemetry for lap {_selected_lap2}…"):
        _car_data = openf1.get_car_data(session_key, driver_number, _date_start, _date_end)
        _location = openf1.get_location(session_key, driver_number, _date_start, _date_end)

    _fig2 = lap_map.plot_lap_map(
        _car_data,
        _location,
        title=f"{driver_ui.value} — Lap {_selected_lap2} Track Map",
    )

    mo.vstack([
        mo.md("## Track Map"),
        mo.as_html(_fig2),
    ])


@app.cell
def _lap_times_section(
    mo, lap_times, laps, driver_number, lap_slider, driver_ui
):
    _selected_lap3 = lap_slider.value
    mo.stop(not laps or not driver_number, mo.md("*No lap data.*"))

    _fig3 = lap_times.plot_lap_times(
        laps=laps,
        driver_number=driver_number,
        selected_lap=_selected_lap3,
        title=f"{driver_ui.value} — Lap Times",
    )

    mo.vstack([
        mo.md("## Lap Times"),
        mo.as_html(_fig3),
    ])


if __name__ == "__main__":
    app.run()
