#!/usr/bin/env python3
"""
F1 Dashboard — Ingestion Pipeline

Fetches data from OpenF1 and upserts into Supabase.

Usage examples:
  # Ingest a specific session
  python ingest.py --session 9158

  # Ingest all sessions for a meeting
  python ingest.py --meeting 1217

  # Ingest all race weekends for a year
  python ingest.py --year 2024

  # Regenerate derived metrics only (raw data untouched)
  python ingest.py --session 9158 --recompute

Environment variables (required):
  SUPABASE_URL              — your project URL
  SUPABASE_SERVICE_ROLE_KEY — service role key (never the anon key)
"""

import argparse
import os
import sys
import time
from typing import Any

import httpx

from supabase import create_client, Client

sys.path.insert(0, os.path.dirname(__file__))
from api import openf1

# ---------------------------------------------------------------------------
# Supabase helpers
# ---------------------------------------------------------------------------

_CHUNK_SIZE = 500


def _upsert(client: Client, table: str, rows: list[dict]) -> None:
    """Upsert rows into a Supabase table in chunks, with retries on transient errors."""
    if not rows:
        return
    for i in range(0, len(rows), _CHUNK_SIZE):
        chunk = rows[i : i + _CHUNK_SIZE]
        for attempt in range(4):
            try:
                client.table(table).upsert(chunk).execute()
                break
            except (httpx.ReadError, httpx.ConnectError) as e:
                if attempt == 3:
                    raise
                wait = 2 ** attempt
                print(f"  [network error] {e.__class__.__name__} on {table}, retrying in {wait}s")
                time.sleep(wait)
    print(f"  upserted {len(rows)} rows → {table}")


def _pick(d: dict, *keys: str) -> dict:
    """Return a new dict with only the specified keys that exist in d."""
    return {k: d[k] for k in keys if k in d}


# ---------------------------------------------------------------------------
# Per-table ingest functions
# ---------------------------------------------------------------------------

def ingest_meeting(client: Client, meeting_key: int) -> dict | None:
    """Upsert meeting metadata into `races`. Returns the meeting dict."""
    meetings = openf1.get_meeting(meeting_key)
    if not meetings:
        print(f"  [warn] no meeting found for meeting_key={meeting_key}")
        return None
    m = meetings[0]
    row = {
        "meeting_key":        m.get("meeting_key"),
        "meeting_name":       m.get("meeting_name"),
        "meeting_official_name": m.get("meeting_official_name"),
        "circuit_short_name": m.get("circuit_short_name"),
        "circuit_key":        m.get("circuit_key"),
        "country_name":       m.get("country_name"),
        "country_code":       m.get("country_code"),
        "location":           m.get("location"),
        "year":               m.get("year"),
        "date_start":         m.get("date_start"),
        "circuit_type":       m.get("circuit_type"),
        "gmt_offset":         m.get("gmt_offset"),
    }
    _upsert(client, "races", [row])
    return m


def ingest_session(client: Client, session_key: int) -> dict | None:
    """Upsert session metadata into `sessions`. Returns the session dict."""
    sessions = openf1.get_session(session_key)
    if not sessions:
        print(f"  [warn] no session found for session_key={session_key}")
        return None
    s = sessions[0]
    row = {
        "session_key":  s.get("session_key"),
        "meeting_key":  s.get("meeting_key"),
        "session_name": s.get("session_name"),
        "session_type": s.get("session_type"),
        "date_start":   s.get("date_start"),
        "date_end":     s.get("date_end"),
        "year":         s.get("year"),
        "circuit_key":  s.get("circuit_key"),
        "circuit_short_name": s.get("circuit_short_name"),
        "country_name": s.get("country_name"),
        "location":     s.get("location"),
    }
    _upsert(client, "sessions", [row])
    return s


def ingest_drivers(client: Client, session_key: int) -> list[dict]:
    """Upsert drivers for a session into `drivers`."""
    drivers = openf1.get_drivers(session_key)
    rows = [
        {
            "session_key":    d.get("session_key"),
            "driver_number":  d.get("driver_number"),
            "name_acronym":   d.get("name_acronym"),
            "full_name":      d.get("full_name"),
            "broadcast_name": d.get("broadcast_name"),
            "team_name":      d.get("team_name"),
            "team_colour":    d.get("team_colour"),
            "country_code":   d.get("country_code"),
            "headshot_url":   d.get("headshot_url"),
        }
        for d in drivers
        if d.get("session_key") and d.get("driver_number") is not None
    ]
    _upsert(client, "drivers", rows)
    return drivers


def ingest_laps(client: Client, session_key: int) -> list[dict]:
    """Upsert lap data into `laps`."""
    laps = openf1.get_laps(session_key)
    rows = [
        {
            "session_key":       l.get("session_key"),
            "driver_number":     l.get("driver_number"),
            "lap_number":        l.get("lap_number"),
            "lap_duration":      l.get("lap_duration"),
            "duration_sector_1": l.get("duration_sector_1"),
            "duration_sector_2": l.get("duration_sector_2"),
            "duration_sector_3": l.get("duration_sector_3"),
            "i1_speed":          l.get("i1_speed"),
            "i2_speed":          l.get("i2_speed"),
            "st_speed":          l.get("st_speed"),
            "is_pit_out_lap":    l.get("is_pit_out_lap"),
            "date_start":        l.get("date_start"),
        }
        for l in laps
        if l.get("session_key") and l.get("driver_number") is not None and l.get("lap_number") is not None
    ]
    _upsert(client, "laps", rows)
    return laps


def ingest_stints(client: Client, session_key: int) -> list[dict]:
    """Upsert stint data into `stints`."""
    stints = openf1.get_stints(session_key)
    rows = [
        {
            "session_key":        s.get("session_key"),
            "driver_number":      s.get("driver_number"),
            "stint_number":       s.get("stint_number"),
            "lap_start":          s.get("lap_start"),
            "lap_end":            s.get("lap_end"),
            "compound":           s.get("compound"),
            "tyre_age_at_start":  s.get("tyre_age_at_start"),
        }
        for s in stints
        if s.get("session_key") and s.get("driver_number") is not None
    ]
    _upsert(client, "stints", rows)
    return stints


def ingest_pit_stops(client: Client, session_key: int) -> list[dict]:
    """Upsert pit stop data into `pit_stops`."""
    pit = openf1.get_pit(session_key)
    rows = [
        {
            "session_key":    p.get("session_key"),
            "driver_number":  p.get("driver_number"),
            "lap_number":     p.get("lap_number"),
            "pit_duration":   p.get("pit_duration"),
            "lane_duration":  p.get("lane_duration"),
            "stop_duration":  p.get("stop_duration"),  # available from 2024 US GP onwards
            "date":           p.get("date"),
        }
        for p in pit
        if p.get("session_key") and p.get("driver_number") is not None
    ]
    _upsert(client, "pit_stops", rows)
    return pit


def ingest_weather(client: Client, session_key: int) -> None:
    """Upsert weather data into `weather`."""
    weather = openf1.get_weather(session_key)
    rows = [
        {
            "session_key":       w.get("session_key"),
            "date":              w.get("date"),
            "track_temperature": w.get("track_temperature"),
            "air_temperature":   w.get("air_temperature"),
            "humidity":          w.get("humidity"),
            "pressure":          w.get("pressure"),
            "rainfall":          w.get("rainfall"),
            "wind_direction":    w.get("wind_direction"),
            "wind_speed":        w.get("wind_speed"),
        }
        for w in weather
        if w.get("session_key") and w.get("date")
    ]
    _upsert(client, "weather", rows)


def ingest_race_control(client: Client, session_key: int) -> list[dict]:
    """Upsert race control messages into `race_control`."""
    rc = openf1.get_race_control(session_key)
    rows = [
        {
            "session_key":   r.get("session_key"),
            "date":          r.get("date"),
            "lap_number":    r.get("lap_number"),
            "category":      r.get("category"),
            "flag":          r.get("flag"),
            "message":       r.get("message"),
            "driver_number": r.get("driver_number"),
            "scope":             r.get("scope"),
            "sector":            r.get("sector"),
            "qualifying_phase":  r.get("qualifying_phase"),
        }
        for r in rc
        if r.get("session_key") and r.get("date")
    ]
    # Deduplicate within the batch — OpenF1 can emit multiple messages at
    # the same timestamp, which would cause Postgres to reject the upsert.
    # Filter to rows where category and message are non-null — required by the PK.
    rows = [r for r in rows if r.get("category") and r.get("message")]
    _upsert(client, "race_control", rows)
    return rc


def ingest_race_results(client: Client, session_key: int, pit_stops: list[dict]) -> None:
    """Upsert final race classification into `race_results`."""
    results = openf1.get_session_result(session_key)
    pit_count: dict[int, int] = {}
    for p in pit_stops:
        dn = p.get("driver_number")
        if dn is not None:
            pit_count[dn] = pit_count.get(dn, 0) + 1

    rows = [
        {
            "session_key":    r.get("session_key"),
            "driver_number":  r.get("driver_number"),
            "position":       r.get("position"),
            "points":         r.get("points"),
            "gap_to_leader":  str(r.get("gap_to_leader")) if r.get("gap_to_leader") is not None else None,
            "duration":       r.get("duration"),
            "number_of_laps": r.get("number_of_laps"),
            "dnf":            bool(r.get("dnf", False)),
            "dns":            bool(r.get("dns", False)),
            "dsq":            bool(r.get("dsq", False)),
            "pit_count":      pit_count.get(r.get("driver_number"), 0),
            "status_detail":  None,  # populated in Phase 1 from race_control messages
        }
        for r in results
        if r.get("session_key") and r.get("driver_number") is not None
    ]
    _upsert(client, "race_results", rows)


_PHASE_LABELS: dict[str, str] = {
    "1": "Q1", "2": "Q2", "3": "Q3",
    "Q1": "Q1", "Q2": "Q2", "Q3": "Q3",
}


def _normalize_phase(v) -> str | None:
    """
    Normalize a qualifying_phase value from OpenF1 to a canonical string.

    The OpenF1 API returns qualifying_phase as an integer (1, 2, 3) in some
    sessions and as a string ("Q1", "Q2", "Q3") in others.  This function
    maps both forms to the canonical "Q1"/"Q2"/"Q3" representation.
    """
    if v is None:
        return None
    return _PHASE_LABELS.get(str(v).strip().upper())


def _assign_qualifying_phases(laps: list[dict], race_control: list[dict]) -> list[dict]:
    """
    Inject a ``_phase`` key into each lap based on qualifying_phase events from race_control.

    Each lap's ``_phase`` is set to the most recent qualifying_phase event whose
    ``date`` is <= the lap's ``date_start``.  Returns None for laps with no
    ``date_start`` or laps that precede all phase events.

    Handles both integer (1/2/3) and string ("Q1"/"Q2"/"Q3") qualifying_phase
    values returned by the OpenF1 API.
    """
    phase_events = sorted(
        [r for r in race_control if _normalize_phase(r.get("qualifying_phase")) and r.get("date")],
        key=lambda r: r["date"],
    )
    for lap in laps:
        date_start = lap.get("date_start")
        if date_start is None:
            lap["_phase"] = None
            continue
        current_phase = None
        for event in phase_events:
            if event["date"] <= date_start:
                current_phase = _normalize_phase(event["qualifying_phase"])
            else:
                break
        lap["_phase"] = current_phase
    return laps


def _get_compound_for_lap(driver_number: int, lap_number: int, stints: list[dict]) -> str | None:
    """
    Return the compound used on the lap identified by *driver_number* and *lap_number*.

    Scans *stints* for a matching stint (lap_start <= lap_number <= lap_end; an
    open-ended stint where lap_end is None matches any lap >= lap_start).

    Returns the compound uppercased, ``"UNKNOWN"`` if the compound field is None,
    or ``None`` if no matching stint is found.
    """
    for stint in stints:
        if stint.get("driver_number") != driver_number:
            continue
        lap_start = stint.get("lap_start")
        lap_end = stint.get("lap_end")
        if lap_start is None:
            continue
        if lap_number >= lap_start and (lap_end is None or lap_number <= lap_end):
            compound = stint.get("compound")
            if compound is None:
                return "UNKNOWN"
            return compound.upper()
    return None


def ingest_qualifying_results(
    client: Client,
    session_key: int,
    laps: list[dict],
    race_control: list[dict] | None = None,
    stints: list[dict] | None = None,
) -> None:
    """
    Upsert qualifying best times per phase into ``qualifying_results``.

    Assigns each lap to a qualifying phase (Q1/Q2/Q3) using race_control
    events, then computes per-phase best times, compounds, and lap counts.
    """
    if race_control is None:
        race_control = []
    if stints is None:
        stints = []

    laps = _assign_qualifying_phases(laps, race_control)

    # best_per_phase[dn][phase] = (time, lap_number)
    best_per_phase: dict[int, dict[str, tuple[float, int]]] = {}
    # laps_per_phase[dn][phase] = count (all laps in phase, not just timed ones)
    laps_per_phase: dict[int, dict[str, int]] = {}

    for lap in laps:
        dn = lap.get("driver_number")
        if dn is None:
            continue
        phase = lap.get("_phase")
        if phase is None:
            continue

        # Count every lap that has a phase assignment
        if dn not in laps_per_phase:
            laps_per_phase[dn] = {}
        laps_per_phase[dn][phase] = laps_per_phase[dn].get(phase, 0) + 1

        # Track best timed lap per (driver, phase)
        dur = lap.get("lap_duration")
        if not dur:
            continue
        try:
            t = float(dur)
        except (TypeError, ValueError):
            continue
        if t <= 0:
            continue
        if dn not in best_per_phase:
            best_per_phase[dn] = {}
        lap_num = lap.get("lap_number")
        if phase not in best_per_phase[dn] or t < best_per_phase[dn][phase][0]:
            best_per_phase[dn][phase] = (t, lap_num)

    all_drivers = set(best_per_phase.keys()) | set(laps_per_phase.keys())
    rows = []
    for dn in all_drivers:
        phases = best_per_phase.get(dn, {})
        lap_counts = laps_per_phase.get(dn, {})

        q1 = phases.get("Q1")
        q2 = phases.get("Q2")
        q3 = phases.get("Q3")

        q1_time   = q1[0] if q1 else None
        q2_time   = q2[0] if q2 else None
        q3_time   = q3[0] if q3 else None
        q1_lap_no = q1[1] if q1 else None
        q2_lap_no = q2[1] if q2 else None
        q3_lap_no = q3[1] if q3 else None

        # Overall best = minimum across all phased laps
        all_phase_times = list(phases.values())
        if all_phase_times:
            best_time, best_lap_num = min(all_phase_times, key=lambda x: x[0])
        else:
            best_time, best_lap_num = None, None

        rows.append({
            "session_key":     session_key,
            "driver_number":   dn,
            "best_lap_time":   best_time,
            "best_lap_number": best_lap_num,
            "q1_time":         q1_time,
            "q2_time":         q2_time,
            "q3_time":         q3_time,
            "q1_compound":     _get_compound_for_lap(dn, q1_lap_no, stints) if q1_lap_no is not None else None,
            "q2_compound":     _get_compound_for_lap(dn, q2_lap_no, stints) if q2_lap_no is not None else None,
            "q3_compound":     _get_compound_for_lap(dn, q3_lap_no, stints) if q3_lap_no is not None else None,
            "q1_laps":         lap_counts.get("Q1"),
            "q2_laps":         lap_counts.get("Q2"),
            "q3_laps":         lap_counts.get("Q3"),
        })

    _upsert(client, "qualifying_results", rows)


def ingest_overtakes(client: Client, session_key: int) -> None:
    """Upsert overtake events into `overtakes`."""
    overtakes = openf1.get_overtakes(session_key)
    rows = [
        {
            "session_key":              o.get("session_key"),
            "driver_number_overtaking": o.get("overtaking_driver_number"),
            "driver_number_overtaken":  o.get("overtaken_driver_number"),
            "position":                 o.get("position"),
            "date":                     o.get("date"),
        }
        for o in overtakes
        if o.get("session_key")
        and o.get("date")
        and o.get("overtaking_driver_number") is not None
        and o.get("overtaken_driver_number") is not None
    ]
    _upsert(client, "overtakes", rows)


def _parse_gap(value) -> tuple[float | None, int | None]:
    """
    Parse an OpenF1 gap value into (numeric_gap, laps_down).

    Returns (float, None) for normal gaps like 2.345.
    Returns (None, N) for lapped drivers like "+1 LAP" or "+2 LAPS".
    Returns (None, None) for null/missing values (leader or no data).
    """
    if value is None:
        return None, None
    try:
        return float(value), None
    except (TypeError, ValueError):
        s = str(value).strip()
        if "LAP" in s.upper():
            try:
                return None, int(s.split()[0].lstrip("+"))
            except (ValueError, IndexError):
                pass
        return None, None


def ingest_intervals(client: Client, session_key: int) -> None:
    """Upsert gap-to-leader and interval data into `intervals` (race/sprint only)."""
    intervals = openf1.get_intervals(session_key)
    rows = []
    for i in intervals:
        if not (i.get("session_key") and i.get("driver_number") is not None and i.get("date")):
            continue
        gap, laps_down = _parse_gap(i.get("gap_to_leader"))
        interval, _ = _parse_gap(i.get("interval"))
        rows.append({
            "session_key":   i.get("session_key"),
            "driver_number": i.get("driver_number"),
            "date":          i.get("date"),
            "gap_to_leader": gap,
            "interval":      interval,
            "laps_down":     laps_down,
        })
    _upsert(client, "intervals", rows)


def ingest_starting_grid(client: Client, session_key: int) -> None:
    """Upsert starting grid positions into `starting_grid` (race/sprint only)."""
    grid = openf1.get_starting_grid(session_key)
    rows = [
        {
            "session_key":   g.get("session_key"),
            "driver_number": g.get("driver_number"),
            "position":      g.get("position"),
            "lap_duration":  g.get("lap_duration"),
        }
        for g in grid
        if g.get("session_key") and g.get("driver_number") is not None
    ]
    _upsert(client, "starting_grid", rows)


def ingest_championship_drivers(client: Client, session_key: int) -> None:
    """Upsert driver championship standings into `championship_drivers` (race/sprint only)."""
    standings = openf1.get_championship_drivers(session_key)
    rows = [
        {
            "session_key":      s.get("session_key"),
            "driver_number":    s.get("driver_number"),
            "points_start":     s.get("points_start"),
            "points_current":   s.get("points_current"),
            "position_start":   s.get("position_start"),
            "position_current": s.get("position_current"),
        }
        for s in standings
        if s.get("session_key") and s.get("driver_number") is not None
    ]
    _upsert(client, "championship_drivers", rows)


def ingest_position(client: Client, session_key: int) -> None:
    """Upsert race position over time into `position` (race/sprint only)."""
    positions = openf1.get_all_positions(session_key)
    rows = [
        {
            "session_key":   p.get("session_key"),
            "driver_number": p.get("driver_number"),
            "date":          p.get("date"),
            "position":      p.get("position"),
        }
        for p in positions
        if p.get("session_key") and p.get("driver_number") is not None and p.get("date")
    ]
    _upsert(client, "position", rows)


def ingest_team_radio(client: Client, session_key: int) -> None:
    """Upsert team radio recording metadata into `team_radio`."""
    radio = openf1.get_team_radio(session_key)
    rows = [
        {
            "session_key":   r.get("session_key"),
            "driver_number": r.get("driver_number"),
            "date":          r.get("date"),
            "recording_url": r.get("recording_url"),
        }
        for r in radio
        if r.get("session_key") and r.get("driver_number") is not None and r.get("date")
    ]
    _upsert(client, "team_radio", rows)


def ingest_championship_teams(client: Client, session_key: int) -> None:
    """Upsert constructor championship standings into `championship_teams` (race/sprint only)."""
    standings = openf1.get_championship_teams(session_key)
    rows = [
        {
            "session_key":      s.get("session_key"),
            "team_name":        s.get("team_name"),
            "points_start":     s.get("points_start"),
            "points_current":   s.get("points_current"),
            "position_start":   s.get("position_start"),
            "position_current": s.get("position_current"),
        }
        for s in standings
        if s.get("session_key") and s.get("team_name")
    ]
    _upsert(client, "championship_teams", rows)


# ---------------------------------------------------------------------------
# Derived metrics (Phase 5)
# ---------------------------------------------------------------------------

def _compute_peak_g(
    car_data_records: list[dict],
    fs: float = 4.0,
    cutoff: float = 0.5,
) -> tuple[float | None, float | None] | None:
    """Windowed peak longitudinal acceleration and deceleration for one lap.

    Pipeline-side port of the validated research computation (noise.py Section 9).
    Returns (peak_accel_g, peak_decel_g_abs) — both positive floats — or None for
    either channel if the throttle/brake gate produces no valid samples, or None
    (not a tuple) if the raw data is insufficient to process.

    Signal processing:
      dedup → PCHIP resample to 4 Hz grid → 4th-order Butterworth low-pass at
      0.5 Hz → differentiate → convert to g.

    Throttle/brake windowing (left-edge convention, shape N-1 to match diff output):
      accel_mask  = throttle_reg[:-1] > 20          (>20 % throttle → genuine power)
      decel_mask  = throttle_reg[:-1] < 20 | brake_reg[:-1] > 0  (lifting/braking)

    Plausibility bounds are NOT applied here; the caller (ingest_race_peak_g_summary)
    applies them when computing the 'clean' race averages.
    """
    import numpy as np
    import pandas as pd
    from scipy.interpolate import PchipInterpolator
    from scipy.signal import butter, filtfilt

    if not car_data_records or len(car_data_records) < 20:
        return None

    df = pd.DataFrame(car_data_records)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)

    same     = df['speed'].diff().eq(0)
    group_id = (~same).cumsum()
    run_len  = group_id.map(group_id.value_counts())
    clean    = df[run_len < 8]
    if len(clean) < 10:
        return None

    t_ctrl     = clean['date'].astype(np.int64).to_numpy() / 1e9
    throttle_r = clean['throttle'].to_numpy().astype(float)
    brake_r    = clean['brake'].to_numpy().astype(float)

    t     = t_ctrl.copy()
    v     = clean['speed'].to_numpy().astype(float)
    pchip = PchipInterpolator(t, v)
    t_reg = np.arange(t[0], t[-1], 1 / fs)
    if len(t_reg) < 32:
        return None
    v_reg = pchip(t_reg)

    # Throttle: linear interp (smooth, 20 % gate is coarse enough).
    # Brake: nearest-neighbour (preserves binary 0/100 character; linear would
    # produce fractional values at step edges, corrupting the brake > 0 test).
    throttle_reg = np.clip(np.interp(t_reg, t_ctrl, throttle_r), 0, 100)
    brake_reg    = np.clip(np.round(np.interp(t_reg, t_ctrl, brake_r) / 100) * 100, 0, 100)

    b, a    = butter(N=4, Wn=cutoff / (fs / 2), btype='low')
    padlen  = min(len(v_reg) // 4, int(5 * fs))
    v_filt  = filtfilt(b, a, v_reg, padlen=padlen)

    accel_g = np.diff(v_filt) / (1 / fs) * (1000 / 3600) / 9.81

    thr = throttle_reg[:-1]
    brk = brake_reg[:-1]
    accel_mask = thr > 20
    decel_mask = (thr < 20) | (brk > 0)

    accel_cand = accel_g[accel_mask]
    decel_cand = accel_g[decel_mask]

    peak_accel_g     = float(np.max(accel_cand))          if len(accel_cand) > 0 else None
    peak_decel_g_abs = float(abs(np.min(decel_cand)))     if len(decel_cand) > 0 else None

    return peak_accel_g, peak_decel_g_abs


def ingest_lap_metrics(
    client: Client,
    session_key: int,
    laps: list[dict],
) -> dict[int, list[tuple[float, float]]]:
    """Compute windowed peak g per lap and upsert into `lap_metrics`.

    Uses a single get_car_data call per driver (full race window) and slices
    client-side, matching the batch-fetch pattern used throughout the pipeline.

    Returns a dict mapping driver_number → list of (peak_accel_g, peak_decel_g_abs)
    for use by ingest_race_peak_g_summary.  Laps where _compute_peak_g returns None
    or either channel is None are silently excluded from the dict (they are not
    upserted to lap_metrics either).
    """
    import pandas as pd

    # Group non-pit-out laps by driver, sorted by date_start.
    from collections import defaultdict
    laps_by_driver: dict[int, list[dict]] = defaultdict(list)
    for lap in laps:
        dn = lap.get('driver_number')
        if dn is not None and not lap.get('is_pit_out_lap') and lap.get('date_start'):
            laps_by_driver[dn].append(lap)

    rows: list[dict] = []
    driver_stats: dict[int, list[tuple[float, float]]] = {}

    for dn, driver_laps in laps_by_driver.items():
        driver_laps = sorted(driver_laps, key=lambda x: x['date_start'])
        if len(driver_laps) < 2:
            continue

        raw = openf1.get_car_data(
            session_key, dn,
            driver_laps[0]['date_start'],
            driver_laps[-1]['date_start'],
        )
        if not raw:
            continue

        df_all = pd.DataFrame(raw)
        df_all['date'] = pd.to_datetime(df_all['date'])
        df_all = df_all.sort_values('date').reset_index(drop=True)

        lap_stats: list[tuple[float, float]] = []
        for i in range(len(driver_laps) - 1):
            lap      = driver_laps[i]
            lap_num  = lap.get('lap_number', i + 1)
            t0       = pd.to_datetime(lap['date_start'])
            t1       = pd.to_datetime(driver_laps[i + 1]['date_start'])
            lap_data = df_all[(df_all['date'] >= t0) & (df_all['date'] < t1)]

            result = _compute_peak_g(lap_data.to_dict('records'))
            if result is None:
                continue
            peak_accel, peak_decel_abs = result
            if peak_accel is None or peak_decel_abs is None:
                continue

            rows.append({
                'session_key':    session_key,
                'driver_number':  dn,
                'lap_number':     lap_num,
                'peak_accel_g':   round(peak_accel,     4),
                'peak_decel_g_abs': round(peak_decel_abs, 4),
            })
            lap_stats.append((peak_accel, peak_decel_abs))

        if lap_stats:
            driver_stats[dn] = lap_stats

    _upsert(client, 'lap_metrics', rows)
    return driver_stats


def ingest_race_peak_g_summary(
    client: Client,
    session_key: int,
    driver_stats: dict[int, list[tuple[float, float]]],
) -> None:
    """Aggregate per-lap peak g into per-driver race means and upsert to `race_results`.

    Two averages per channel:
      mean_peak_*          — all laps (raw windowed values)
      mean_peak_*_clean    — laps within plausibility bounds (accel ≤ 4 g, decel ≤ 8 g)

    Bounds are not applied at the lap_metrics level so outlier laps remain
    inspectable in that table.
    """
    _ACCEL_BOUND = 4.0   # g — above this a lap is considered a noise artefact
    _DECEL_BOUND = 8.0   # g — above this a lap is considered a noise artefact

    rows: list[dict] = []
    for dn, stats in driver_stats.items():
        accels = [s[0] for s in stats]
        decels = [s[1] for s in stats]
        accels_clean = [a for a in accels if a <= _ACCEL_BOUND]
        decels_clean = [d for d in decels if d <= _DECEL_BOUND]

        rows.append({
            'session_key':              session_key,
            'driver_number':            dn,
            'mean_peak_accel_g':        round(sum(accels) / len(accels), 4),
            'mean_peak_accel_g_clean':  round(sum(accels_clean) / len(accels_clean), 4) if accels_clean else None,
            'mean_peak_decel_g_abs':    round(sum(decels) / len(decels), 4),
            'mean_peak_decel_g_abs_clean': round(sum(decels_clean) / len(decels_clean), 4) if decels_clean else None,
        })

    _upsert(client, 'race_results', rows)


def recompute_lap_metrics(client: Client, session_key: int, laps: list[dict], session_type: str) -> None:
    """Regenerate derived metrics for a session.

    Currently computes windowed peak longitudinal g per lap (lap_metrics table)
    and per-driver race averages (race_results table).  Only runs for race/sprint
    sessions — qualifying has no meaningful deceleration/acceleration comparison.
    """
    if session_type not in ('race', 'sprint'):
        print(f"  [skip] lap_metrics: session type '{session_type}' — race/sprint only")
        return
    print("  computing windowed peak g per lap …")
    driver_stats = ingest_lap_metrics(client, session_key, laps)
    ingest_race_peak_g_summary(client, session_key, driver_stats)
    print(f"  lap_metrics: {sum(len(v) for v in driver_stats.values())} laps across {len(driver_stats)} drivers")


# ---------------------------------------------------------------------------
# Top-level orchestration
# ---------------------------------------------------------------------------

def process_session(client: Client, session_key: int, recompute: bool = False) -> None:
    """Ingest all data for a single session."""
    openf1.reset_stats()
    t0 = time.time()

    # Fetch session metadata first to get meeting_key, but don't upsert yet —
    # sessions has a FK to races(meeting_key), so the meeting must exist first.
    sessions_raw = openf1.get_session(session_key)
    if not sessions_raw:
        print(f"  [warn] no session found for session_key={session_key}")
        return
    s0 = sessions_raw[0]
    session_name = s0.get("session_name") or "Unknown"
    session_type_raw = s0.get("session_type") or ""
    date_str = (s0.get("date_start") or "")[:10]  # YYYY-MM-DD
    print(f"\n  → Session {session_key} — {session_name} [{date_str}]")

    meeting_key = s0.get("meeting_key")
    if meeting_key:
        ingest_meeting(client, meeting_key)

    # Now safe to upsert session (meeting row exists)
    session = ingest_session(client, session_key)
    if session is None:
        return

    session_type = session_type_raw.lower()

    ingest_drivers(client, session_key)
    laps = ingest_laps(client, session_key)
    stints_rows = ingest_stints(client, session_key)
    pit = ingest_pit_stops(client, session_key)
    ingest_weather(client, session_key)
    rc_rows = ingest_race_control(client, session_key)

    if session_type in ("race", "sprint"):
        ingest_race_results(client, session_key, pit)
        ingest_overtakes(client, session_key)
        ingest_intervals(client, session_key)
        ingest_position(client, session_key)
        ingest_team_radio(client, session_key)
        ingest_championship_drivers(client, session_key)
        ingest_championship_teams(client, session_key)
    elif session_type in ("qualifying", "sprint qualifying", "sprint shootout"):
        ingest_qualifying_results(client, session_key, laps, rc_rows, stints_rows)
        ingest_starting_grid(client, session_key)

    if recompute:
        recompute_lap_metrics(client, session_key, laps, session_type)

    elapsed = time.time() - t0
    stats = openf1.get_stats()
    rate = openf1.current_rate()
    rate_str = f" @ {rate:.1f} req/s" if rate > 0 else ""
    rl_str = f" | ⚠ {stats['rate_limit_waits']} rate-limit waits" if stats["rate_limit_waits"] else ""
    print(f"  ✓ done in {elapsed:.1f}s | "
          f"{stats['real_calls']} API calls, {stats['cache_hits']} cached{rate_str}{rl_str}")


def process_meeting(client: Client, meeting_key: int, recompute: bool = False) -> None:
    """Ingest all sessions for a meeting."""
    meeting_info = openf1.get_meeting(meeting_key)
    if meeting_info:
        m = meeting_info[0]
        name = m.get("meeting_name") or "Unknown"
        location = m.get("location") or m.get("circuit_short_name") or ""
        year = m.get("year") or ""
        print(f"\n→ Meeting {meeting_key} — {name} · {location} · {year}")
    else:
        print(f"\n→ Meeting {meeting_key}")

    sessions = openf1.get_sessions(meeting_key)
    allowed = {"Race", "Qualifying", "Sprint", "Sprint Qualifying", "Sprint Shootout"}
    eligible = [s for s in sessions if s.get("session_name") in allowed]
    print(f"  {len(eligible)} session(s) to ingest: "
          f"{', '.join(s.get('session_name', '?') for s in eligible)}")

    for s in eligible:
        process_session(client, s["session_key"], recompute=recompute)

    print(f"\n✓ Meeting {meeting_key} complete")


def process_year(client: Client, year: int, recompute: bool = False) -> None:
    """Ingest all race weekends for a year."""
    meetings = openf1.get_meetings(year)
    eligible = [m for m in meetings
                if "pre-season" not in (m.get("meeting_name") or "").lower()
                and "testing" not in (m.get("meeting_name") or "").lower()]
    print(f"\n→ Year {year} — {len(eligible)} meeting(s) to ingest")
    t0 = time.time()
    for m in eligible:
        process_meeting(client, m["meeting_key"], recompute=recompute)
        time.sleep(1)  # brief pause between meetings
    elapsed = time.time() - t0
    print(f"\n✓ Year {year} complete — {len(eligible)} meetings in {elapsed/60:.1f}min")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="F1 Dashboard ingestion pipeline")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--session",  type=int, metavar="SESSION_KEY",  help="Ingest a single session")
    group.add_argument("--meeting",  type=int, metavar="MEETING_KEY",  help="Ingest all sessions in a meeting")
    group.add_argument("--year",     type=int, metavar="YEAR",         help="Ingest all meetings for a year")
    parser.add_argument("--recompute", action="store_true",
                        help="Regenerate derived metrics without re-fetching raw data")
    args = parser.parse_args()

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        print("Error: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set.")
        sys.exit(1)

    client = create_client(url, key)

    if args.session:
        if args.recompute:
            recompute_lap_metrics(client, args.session)
        else:
            process_session(client, args.session, recompute=False)
    elif args.meeting:
        process_meeting(client, args.meeting, recompute=args.recompute)
    elif args.year:
        process_year(client, args.year, recompute=args.recompute)


if __name__ == "__main__":
    main()
