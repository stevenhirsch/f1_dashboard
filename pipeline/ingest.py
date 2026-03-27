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


def _compute_qualifying_best_per_phase(
    laps: list[dict],
) -> dict[int, dict[str, tuple[float, int]]]:
    """Return the best timed lap per qualifying phase per driver.

    Expects laps to have ``_phase`` keys injected by ``_assign_qualifying_phases``.
    Returns ``{driver_number: {phase_str: (lap_time_s, lap_number)}}``.
    """
    best: dict[int, dict[str, tuple[float, int]]] = {}
    for lap in laps:
        dn    = lap.get("driver_number")
        phase = lap.get("_phase")
        if dn is None or phase is None:
            continue
        dur = lap.get("lap_duration")
        if not dur:
            continue
        try:
            t = float(dur)
        except (TypeError, ValueError):
            continue
        if t <= 0:
            continue
        lap_num = lap.get("lap_number")
        if dn not in best:
            best[dn] = {}
        if phase not in best[dn] or t < best[dn][phase][0]:
            best[dn][phase] = (t, lap_num)
    return best


def ingest_qualifying_results(
    client: Client,
    session_key: int,
    laps: list[dict],
    race_control: list[dict] | None = None,
    stints: list[dict] | None = None,
) -> dict[int, dict[str, tuple[float, int]]]:
    """
    Upsert qualifying best times per phase into ``qualifying_results``.

    Assigns each lap to a qualifying phase (Q1/Q2/Q3) using race_control
    events, then computes per-phase best times, compounds, and lap counts.

    Returns ``best_per_phase`` (driver → phase → (time, lap_number)) so that
    downstream callers (e.g. ``recompute_lap_metrics``) can look up which lap
    number produced the best time in each phase without re-computing.
    """
    if race_control is None:
        race_control = []
    if stints is None:
        stints = []

    laps = _assign_qualifying_phases(laps, race_control)

    # laps_per_phase[dn][phase] = count (all laps in phase, not just timed ones)
    laps_per_phase: dict[int, dict[str, int]] = {}
    for lap in laps:
        dn    = lap.get("driver_number")
        phase = lap.get("_phase")
        if dn is None or phase is None:
            continue
        if dn not in laps_per_phase:
            laps_per_phase[dn] = {}
        laps_per_phase[dn][phase] = laps_per_phase[dn].get(phase, 0) + 1

    best_per_phase = _compute_qualifying_best_per_phase(laps)

    all_drivers = set(best_per_phase.keys()) | set(laps_per_phase.keys())
    rows = []
    for dn in all_drivers:
        phases    = best_per_phase.get(dn, {})
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
    return best_per_phase


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

# Signal processing constants (pipeline-standard, validated in research/car_velocity/noise.py)
_BRAKE_G_THRESHOLD  = 0.5   # g  — minimum deceleration to count as a braking event
_DRS_OPEN_THRESHOLD = 10    # DRS value >= this → car is in a DRS zone or has DRS open


def _windowed_peak_g(accel_g, thr, brk):
    """Windowed peak accel and decel for a segment of the accel_g signal.

    All three arrays must have the same length (left-edge convention: index i
    covers the interval [t_reg[i], t_reg[i+1])).

    Returns (peak_accel_g, peak_decel_g_abs) — either can be None when the
    corresponding gate produces no valid samples.
    """
    import numpy as np
    accel_mask = thr > 20
    decel_mask = (thr < 20) | (brk > 0)
    accel_cand = accel_g[accel_mask]
    decel_cand = accel_g[decel_mask]
    peak_accel     = float(np.max(accel_cand))      if len(accel_cand) > 0 else None
    peak_decel_abs = float(abs(np.min(decel_cand))) if len(decel_cand) > 0 else None
    return peak_accel, peak_decel_abs


def _find_brake_zones(accel_g, v_filt):
    """Find contiguous braking events where deceleration exceeds _BRAKE_G_THRESHOLD.

    ``accel_g`` and ``v_filt`` must satisfy ``len(v_filt) >= len(accel_g)``;
    ``v_filt[i]`` is the speed (km/h) at the start of the i-th interval.

    Returns a list of (peak_decel_g_abs, speed_at_start_kph), one per event.
    """
    import numpy as np
    in_brake = accel_g < -_BRAKE_G_THRESHOLD
    zones, i, n = [], 0, len(in_brake)
    while i < n:
        if in_brake[i]:
            start = i
            while i < n and in_brake[i]:
                i += 1
            peak_decel = float(abs(np.min(accel_g[start:i])))
            speed_kph  = float(v_filt[start])
            zones.append((peak_decel, speed_kph))
        else:
            i += 1
    return zones


def _brake_zone_stats(zones):
    """Aggregate a list of brake zone tuples into summary statistics.

    Returns (count, mean_peak_decel_g, speed_at_primary_brake_start_kph).
    ``speed_at_primary`` is the entry speed of the hardest braking zone.
    Returns (0, None, None) when zones is empty.
    """
    if not zones:
        return 0, None, None
    count     = len(zones)
    mean_peak = float(sum(z[0] for z in zones) / count)
    primary   = max(zones, key=lambda z: z[0])
    return count, mean_peak, primary[1]


def _compute_lap_metrics(
    car_data_records: list[dict],
    s1_end_t: float | None = None,
    s2_end_t: float | None = None,
    fs: float = 4.0,
    cutoff: float = 0.5,
) -> dict | None:
    """Compute all car-data derived metrics for one lap.

    Parameters
    ----------
    car_data_records : list[dict]
        Raw OpenF1 car_data records for the lap window.
    s1_end_t, s2_end_t : float or None
        Absolute epoch timestamps (seconds) for the S1/S2 sector boundaries.
        When either is None, all ``_s1``/``_s2``/``_s3`` keys are returned as None.
    fs : float
        Resampling frequency (Hz).  Pipeline standard: 4 Hz.
    cutoff : float
        Butterworth low-pass cutoff (Hz).  Pipeline standard: 0.5 Hz.

    Returns
    -------
    dict or None
        All metrics as a flat dict, or None when data is insufficient.
        Per-sector keys (``_s1``/``_s2``/``_s3``) are None when sector times
        are unavailable or the sector slice is too short to compute reliably.

    Notes
    -----
    Signal processing pipeline (validated in research/car_velocity/noise.py):
      dedup → PCHIP resample to 4 Hz → 4th-order Butterworth low-pass at 0.5 Hz
      → diff → throttle/brake gating.
    Brake is boolean (0/100) in OpenF1 — nearest-neighbour resampling preserves
    that character.  Throttle uses linear interpolation.
    Plausibility bounds (accel ≤ 4 g, decel ≤ 8 g) are NOT applied here; the
    caller applies them when computing clean race-level averages.
    Schema note: lap_metrics requires new columns before these rows can be written.
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

    # Dedup: remove runs of 8+ identical speed samples (stationary / pit-lane sections)
    same     = df['speed'].diff().eq(0)
    group_id = (~same).cumsum()
    run_len  = group_id.map(group_id.value_counts())
    clean    = df[run_len < 8]
    if len(clean) < 10:
        return None

    t_ctrl     = clean['date'].astype(np.int64).to_numpy() / 1e9   # epoch seconds
    v          = clean['speed'].to_numpy().astype(float)            # km/h
    throttle_r = clean['throttle'].to_numpy().astype(float)
    brake_r    = clean['brake'].to_numpy().astype(float)

    has_drs = 'drs' in clean.columns and not clean['drs'].isna().all()
    drs_r   = clean['drs'].fillna(0).to_numpy().astype(float) if has_drs else None

    # PCHIP resample speed to regular grid; interp throttle/brake/drs
    pchip = PchipInterpolator(t_ctrl, v)
    t_reg = np.arange(t_ctrl[0], t_ctrl[-1], 1 / fs)
    N = len(t_reg)
    if N < 32:
        return None
    v_reg = pchip(t_reg)

    # Throttle: linear (coarse 20% gate tolerates small interpolation error)
    # Brake:    nearest-neighbour (preserves binary 0/100 character)
    # DRS:      nearest-neighbour (integer states)
    throttle_reg = np.clip(np.interp(t_reg, t_ctrl, throttle_r), 0, 100)
    brake_reg    = np.clip(np.round(np.interp(t_reg, t_ctrl, brake_r) / 100) * 100, 0, 100)
    drs_reg      = np.round(np.interp(t_reg, t_ctrl, drs_r)) if drs_r is not None else None

    # Butterworth low-pass on speed → accel in g (length N-1, left-edge convention)
    b, a    = butter(N=4, Wn=cutoff / (fs / 2), btype='low')
    padlen  = min(N // 4, int(5 * fs))
    v_filt  = filtfilt(b, a, v_reg, padlen=padlen)
    accel_g = np.diff(v_filt) / (1 / fs) * (1000 / 3600) / 9.81

    # Left-edge throttle/brake slices aligned with accel_g (length N-1)
    thr = throttle_reg[:-1]
    brk = brake_reg[:-1]

    # Sector boundary indices into t_reg
    _MIN = 4   # minimum samples for a reliable sector metric
    has_sectors = s1_end_t is not None and s2_end_t is not None
    if has_sectors:
        s1_idx = int(np.clip(np.searchsorted(t_reg, s1_end_t), 1, N))
        s2_idx = int(np.clip(np.searchsorted(t_reg, s2_end_t), s1_idx, N))
    else:
        s1_idx = s2_idx = None

    # Distance per sample (metres); v_reg in km/h, dt = 1/fs seconds
    dt_s = 1 / fs
    dist = v_reg * dt_s * (1000 / 3600)

    # -------------------------------------------------------------------
    # Sector-split helper closures
    # -------------------------------------------------------------------
    def _ratio_splits(mask):
        """Proportion of True samples for lap and each sector."""
        lap = float(np.mean(mask))
        if not has_sectors:
            return lap, None, None, None
        s1 = float(np.mean(mask[:s1_idx]))        if s1_idx            >= _MIN else None
        s2 = float(np.mean(mask[s1_idx:s2_idx])) if (s2_idx - s1_idx) >= _MIN else None
        s3 = float(np.mean(mask[s2_idx:]))        if (N - s2_idx)      >= _MIN else None
        return lap, s1, s2, s3

    def _dist_splits(mask):
        """Sum of dist[mask] for lap and each sector."""
        lap = float(np.sum(dist[mask]))
        if not has_sectors:
            return lap, None, None, None
        m1, m2, m3 = mask[:s1_idx], mask[s1_idx:s2_idx], mask[s2_idx:]
        s1 = float(np.sum(dist[:s1_idx][m1]))        if s1_idx            >= _MIN else None
        s2 = float(np.sum(dist[s1_idx:s2_idx][m2])) if (s2_idx - s1_idx) >= _MIN else None
        s3 = float(np.sum(dist[s2_idx:][m3]))        if (N - s2_idx)      >= _MIN else None
        return lap, s1, s2, s3

    def _thr_var_splits():
        """var(diff(throttle)) for lap and each sector."""
        def _var(arr):
            d = np.diff(arr)
            return float(np.var(d)) if len(d) > 0 else None
        lap = _var(throttle_reg)
        if not has_sectors:
            return lap, None, None, None
        s1 = _var(throttle_reg[:s1_idx])        if s1_idx            >= 3 else None
        s2 = _var(throttle_reg[s1_idx:s2_idx]) if (s2_idx - s1_idx) >= 3 else None
        s3 = _var(throttle_reg[s2_idx:])        if (N - s2_idx)      >= 3 else None
        return lap, s1, s2, s3

    def _maxspd_splits():
        """Max speed (km/h) for lap and each sector."""
        lap = float(np.max(v_reg))
        if not has_sectors:
            return lap, None, None, None
        s1 = float(np.max(v_reg[:s1_idx]))        if s1_idx            >= 1 else None
        s2 = float(np.max(v_reg[s1_idx:s2_idx])) if (s2_idx - s1_idx) >= 1 else None
        s3 = float(np.max(v_reg[s2_idx:]))        if (N - s2_idx)      >= 1 else None
        return lap, s1, s2, s3

    def _peak_g_splits():
        """Windowed peak G for lap and each sector (accel_g length N-1)."""
        lap_g = _windowed_peak_g(accel_g, thr, brk)
        if not has_sectors:
            return lap_g, (None, None), (None, None), (None, None)
        def _sg(sl):
            ag_sl = accel_g[sl]
            return _windowed_peak_g(ag_sl, thr[sl], brk[sl]) if len(ag_sl) >= _MIN else (None, None)
        return lap_g, _sg(slice(None, s1_idx)), _sg(slice(s1_idx, s2_idx)), _sg(slice(s2_idx, None))

    def _bz_splits():
        """Brake zone stats for lap and each sector."""
        bz_lap = _brake_zone_stats(_find_brake_zones(accel_g, v_filt))
        if not has_sectors:
            return bz_lap, (None, None, None), (None, None, None), (None, None, None)
        def _bz(ag_sl, vf_sl):
            if len(ag_sl) < _MIN:
                return None, None, None
            return _brake_zone_stats(_find_brake_zones(ag_sl, vf_sl))
        return (
            bz_lap,
            _bz(accel_g[:s1_idx],       v_filt[:s1_idx]),
            _bz(accel_g[s1_idx:s2_idx], v_filt[s1_idx:s2_idx]),
            _bz(accel_g[s2_idx:],       v_filt[s2_idx:]),
        )

    # -------------------------------------------------------------------
    # Compute all metrics
    # -------------------------------------------------------------------
    pg_lap, pg_s1, pg_s2, pg_s3 = _peak_g_splits()
    ms_lap, ms_s1, ms_s2, ms_s3 = _maxspd_splits()

    coast_mask = (throttle_reg < 1) & (brake_reg == 0)
    cr_lap, cr_s1, cr_s2, cr_s3 = _ratio_splits(coast_mask)
    cd_lap, cd_s1, cd_s2, cd_s3 = _dist_splits(coast_mask)

    ft_mask = throttle_reg >= 99
    ft_lap, ft_s1, ft_s2, ft_s3 = _ratio_splits(ft_mask)

    tb_mask = (brake_reg > 0) & (throttle_reg >= 10)
    tb_lap, tb_s1, tb_s2, tb_s3 = _ratio_splits(tb_mask)

    tv_lap, tv_s1, tv_s2, tv_s3 = _thr_var_splits()

    if drs_reg is not None:
        drs_open      = drs_reg >= _DRS_OPEN_THRESHOLD
        drs_act_count = int(np.sum(np.diff(drs_open.astype(int)) > 0))
        drs_dist_m    = float(np.sum(dist[drs_open]))
    else:
        drs_act_count = None
        drs_dist_m    = None

    bz_lap, bz_s1, bz_s2, bz_s3 = _bz_splits()

    # -------------------------------------------------------------------
    # Round helpers
    # -------------------------------------------------------------------
    def _r4(v_): return round(float(v_), 4) if v_ is not None else None
    def _r1(v_): return round(float(v_), 1) if v_ is not None else None
    def _r6(v_): return round(float(v_), 6) if v_ is not None else None

    return {
        # Longitudinal G — existing metric names preserved
        'peak_accel_g':     _r4(pg_lap[0]),
        'peak_decel_g_abs': _r4(pg_lap[1]),

        # Per-sector peak G (lap = same as peak_accel/decel_g above)
        'max_linear_acceleration_g_lap': _r4(pg_lap[0]),
        'max_linear_acceleration_g_s1':  _r4(pg_s1[0]),
        'max_linear_acceleration_g_s2':  _r4(pg_s2[0]),
        'max_linear_acceleration_g_s3':  _r4(pg_s3[0]),
        'max_linear_deceleration_g_lap': _r4(pg_lap[1]),
        'max_linear_deceleration_g_s1':  _r4(pg_s1[1]),
        'max_linear_deceleration_g_s2':  _r4(pg_s2[1]),
        'max_linear_deceleration_g_s3':  _r4(pg_s3[1]),

        # Max speed (km/h) from resampled signal (not filtered)
        'max_speed_kph_lap': _r1(ms_lap),
        'max_speed_kph_s1':  _r1(ms_s1),
        'max_speed_kph_s2':  _r1(ms_s2),
        'max_speed_kph_s3':  _r1(ms_s3),

        # Coasting: throttle < 1% AND brake == 0
        'coasting_ratio_lap':      _r4(cr_lap),
        'coasting_ratio_s1':       _r4(cr_s1),
        'coasting_ratio_s2':       _r4(cr_s2),
        'coasting_ratio_s3':       _r4(cr_s3),
        'coasting_distance_m_lap': _r1(cd_lap),
        'coasting_distance_m_s1':  _r1(cd_s1),
        'coasting_distance_m_s2':  _r1(cd_s2),
        'coasting_distance_m_s3':  _r1(cd_s3),

        # Full throttle: throttle >= 99%
        'full_throttle_pct_lap': _r4(ft_lap),
        'full_throttle_pct_s1':  _r4(ft_s1),
        'full_throttle_pct_s2':  _r4(ft_s2),
        'full_throttle_pct_s3':  _r4(ft_s3),

        # Throttle-brake overlap: brake > 0 AND throttle >= 10% (trail braking proxy)
        'throttle_brake_overlap_ratio_lap': _r4(tb_lap),
        'throttle_brake_overlap_ratio_s1':  _r4(tb_s1),
        'throttle_brake_overlap_ratio_s2':  _r4(tb_s2),
        'throttle_brake_overlap_ratio_s3':  _r4(tb_s3),

        # Throttle input variance: var(diff(throttle)) — higher = rougher inputs
        'throttle_input_variance_lap': _r6(tv_lap),
        'throttle_input_variance_s1':  _r6(tv_s1),
        'throttle_input_variance_s2':  _r6(tv_s2),
        'throttle_input_variance_s3':  _r6(tv_s3),

        # DRS (lap-level only — no meaningful per-sector breakdown)
        'drs_activation_count': drs_act_count,
        'drs_distance_m':       _r1(drs_dist_m),

        # Brake zones — contiguous windows where decel > _BRAKE_G_THRESHOLD
        'brake_zone_count_lap':         bz_lap[0],
        'brake_zone_count_s1':          bz_s1[0] if has_sectors else None,
        'brake_zone_count_s2':          bz_s2[0] if has_sectors else None,
        'brake_zone_count_s3':          bz_s3[0] if has_sectors else None,
        'mean_peak_decel_g_lap':        _r4(bz_lap[1]),
        'mean_peak_decel_g_s1':         _r4(bz_s1[1]) if has_sectors else None,
        'mean_peak_decel_g_s2':         _r4(bz_s2[1]) if has_sectors else None,
        'mean_peak_decel_g_s3':         _r4(bz_s3[1]) if has_sectors else None,
        'speed_at_brake_start_kph_lap': _r1(bz_lap[2]),
        'speed_at_brake_start_kph_s1':  _r1(bz_s1[2]) if has_sectors else None,
        'speed_at_brake_start_kph_s2':  _r1(bz_s2[2]) if has_sectors else None,
        'speed_at_brake_start_kph_s3':  _r1(bz_s3[2]) if has_sectors else None,
    }


def ingest_lap_metrics(
    client: Client,
    session_key: int,
    laps: list[dict],
) -> dict[int, dict[int, dict]]:
    """Compute all car-data derived metrics per lap and upsert into ``lap_metrics``.

    Fetches car_data once per driver (full session window) and slices
    client-side — never loops per-lap over the API.

    Returns ``{driver_number: {lap_number: metrics_dict}}`` so callers can
    look up any per-lap metric for a specific lap number without re-querying.
    """
    import pandas as pd
    from collections import defaultdict

    laps_by_driver: dict[int, list[dict]] = defaultdict(list)
    for lap in laps:
        dn = lap.get('driver_number')
        if dn is not None and not lap.get('is_pit_out_lap') and lap.get('date_start'):
            laps_by_driver[dn].append(lap)

    rows: list[dict] = []
    driver_lap_metrics: dict[int, dict[int, dict]] = {}

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

        for i in range(len(driver_laps) - 1):
            lap     = driver_laps[i]
            lap_num = lap.get('lap_number', i + 1)
            t0      = pd.to_datetime(lap['date_start'])
            t1      = pd.to_datetime(driver_laps[i + 1]['date_start'])
            lap_data = df_all[(df_all['date'] >= t0) & (df_all['date'] < t1)]

            # Sector boundary timestamps (absolute epoch seconds)
            t0_epoch = t0.timestamp()
            s1_dur   = lap.get('duration_sector_1')
            s2_dur   = lap.get('duration_sector_2')
            s1_end_t = (t0_epoch + float(s1_dur)) if s1_dur else None
            s2_end_t = (t0_epoch + float(s1_dur) + float(s2_dur)) if (s1_dur and s2_dur) else None

            result = _compute_lap_metrics(
                lap_data.to_dict('records'),
                s1_end_t=s1_end_t,
                s2_end_t=s2_end_t,
            )
            if result is None:
                continue

            rows.append({
                'session_key':   session_key,
                'driver_number': dn,
                'lap_number':    lap_num,
                **result,
            })
            if dn not in driver_lap_metrics:
                driver_lap_metrics[dn] = {}
            driver_lap_metrics[dn][lap_num] = result

    _upsert(client, 'lap_metrics', rows)
    return driver_lap_metrics


def ingest_race_peak_g_summary(
    client: Client,
    session_key: int,
    driver_lap_metrics: dict[int, dict[int, dict]],
) -> None:
    """Aggregate per-lap peak g into per-driver race means and upsert to ``race_results``.

    Two averages per channel:
      mean_peak_*        — all laps (raw windowed values, includes outliers)
      mean_peak_*_clean  — laps within plausibility bounds (accel ≤ 4 g, decel ≤ 8 g)

    Outlier laps remain visible in ``lap_metrics``; the clean averages give a
    reliable summary for the race results table.
    """
    _ACCEL_BOUND = 4.0
    _DECEL_BOUND = 8.0

    rows: list[dict] = []
    for dn, lap_metrics_by_num in driver_lap_metrics.items():
        accels = [m['peak_accel_g']     for m in lap_metrics_by_num.values() if m.get('peak_accel_g')     is not None]
        decels = [m['peak_decel_g_abs'] for m in lap_metrics_by_num.values() if m.get('peak_decel_g_abs') is not None]
        if not accels or not decels:
            continue
        accels_clean = [a for a in accels if a <= _ACCEL_BOUND]
        decels_clean = [d for d in decels if d <= _DECEL_BOUND]

        rows.append({
            'session_key':                 session_key,
            'driver_number':               dn,
            'mean_peak_accel_g':           round(sum(accels) / len(accels), 4),
            'mean_peak_accel_g_clean':     round(sum(accels_clean) / len(accels_clean), 4) if accels_clean else None,
            'mean_peak_decel_g_abs':       round(sum(decels) / len(decels), 4),
            'mean_peak_decel_g_abs_clean': round(sum(decels_clean) / len(decels_clean), 4) if decels_clean else None,
        })

    _upsert(client, 'race_results', rows)


def ingest_qualifying_peak_g_summary(
    client: Client,
    session_key: int,
    driver_lap_metrics: dict[int, dict[int, dict]],
    best_per_phase: dict[int, dict[str, tuple[float, int]]],
) -> None:
    """Upsert peak G for each driver's best qualifying lap per phase to ``qualifying_results``.

    Uses the lap number that produced the best time in each phase (Q1/Q2/Q3) to
    look up the pre-computed peak G from ``driver_lap_metrics``.  This links the
    G-force metric to the driver's representative lap in each phase rather than
    independently optimising for the highest-G lap.
    """
    rows: list[dict] = []
    for dn, phase_best in best_per_phase.items():
        lap_metrics = driver_lap_metrics.get(dn, {})

        def _phase_g(phase):
            pb = phase_best.get(phase)
            if pb is None:
                return None, None
            _, lap_num = pb
            if lap_num is None:
                return None, None
            m = lap_metrics.get(lap_num)
            if m is None:
                return None, None
            return m.get('peak_accel_g'), m.get('peak_decel_g_abs')

        q1_a, q1_d = _phase_g('Q1')
        q2_a, q2_d = _phase_g('Q2')
        q3_a, q3_d = _phase_g('Q3')

        if all(v is None for v in (q1_a, q1_d, q2_a, q2_d, q3_a, q3_d)):
            continue

        rows.append({
            'session_key':         session_key,
            'driver_number':       dn,
            'q1_peak_accel_g':     q1_a,
            'q1_peak_decel_g_abs': q1_d,
            'q2_peak_accel_g':     q2_a,
            'q2_peak_decel_g_abs': q2_d,
            'q3_peak_accel_g':     q3_a,
            'q3_peak_decel_g_abs': q3_d,
        })

    _upsert(client, 'qualifying_results', rows)


def recompute_lap_metrics(
    client: Client,
    session_key: int,
    laps: list[dict],
    session_type: str,
    rc_rows: list[dict] | None = None,
    stints_rows: list[dict] | None = None,
) -> None:
    """Regenerate derived metrics for a session without re-fetching raw data.

    For race/sprint sessions: populates ``lap_metrics`` with all car-data metrics
    and aggregates peak G means into ``race_results``.

    For qualifying sessions: populates ``lap_metrics`` per lap and upserts peak G
    for each driver's best lap per phase into ``qualifying_results``.

    ``rc_rows`` is required for qualifying sessions to assign phase labels to laps.
    """
    if session_type in ('race', 'sprint'):
        print("  computing lap metrics (race/sprint) …")
        driver_lap_metrics = ingest_lap_metrics(client, session_key, laps)
        ingest_race_peak_g_summary(client, session_key, driver_lap_metrics)
        total = sum(len(v) for v in driver_lap_metrics.values())
        print(f"  lap_metrics: {total} laps across {len(driver_lap_metrics)} drivers")
    elif session_type in ('qualifying', 'sprint qualifying', 'sprint shootout'):
        print("  computing lap metrics (qualifying) …")
        laps_with_phases = _assign_qualifying_phases(laps, rc_rows or [])
        best_per_phase   = _compute_qualifying_best_per_phase(laps_with_phases)
        driver_lap_metrics = ingest_lap_metrics(client, session_key, laps_with_phases)
        ingest_qualifying_peak_g_summary(client, session_key, driver_lap_metrics, best_per_phase)
        total = sum(len(v) for v in driver_lap_metrics.values())
        print(f"  lap_metrics: {total} laps across {len(driver_lap_metrics)} drivers")
    else:
        print(f"  [skip] lap_metrics: session type '{session_type}' not supported for recompute")


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
        recompute_lap_metrics(client, session_key, laps, session_type,
                              rc_rows=rc_rows, stints_rows=stints_rows)

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
