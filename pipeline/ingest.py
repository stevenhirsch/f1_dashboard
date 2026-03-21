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

def recompute_lap_metrics(client: Client, session_key: int) -> None:
    """
    Regenerate derived metrics in `lap_metrics` for a session.

    Not yet implemented — see Phase 5 of the roadmap.
    Run with `--recompute` to invoke this after Phase 5 is built.
    """
    print("  [skip] lap_metrics computation not yet implemented (Phase 5)")
    print("         Run `ingest.py --session <key> --recompute` again after Phase 5.")


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
        recompute_lap_metrics(client, session_key)

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
