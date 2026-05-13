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
import bisect
import math
import os
import statistics
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
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


def _query_in(client: Client, table: str, select: str, column: str, values: list) -> list[dict]:
    """Safe .select().in_() wrapper — returns [] when values is empty."""
    if not values:
        return []
    return client.table(table).select(select).in_(column, values).execute().data or []


def _query_in_all(client: Client, table: str, select: str, column: str, values: list) -> list[dict]:
    """Like _query_in but paginates to retrieve all rows, bypassing Supabase's default row limit.

    Use this for tables that can exceed ~1000 rows across multiple sessions (e.g. laps, position).
    """
    if not values:
        return []
    PAGE = 1000
    all_rows: list[dict] = []
    start = 0
    while True:
        chunk = (
            client.table(table)
            .select(select)
            .in_(column, values)
            .range(start, start + PAGE - 1)
            .execute()
            .data or []
        )
        all_rows.extend(chunk)
        if len(chunk) < PAGE:
            break
        start += PAGE
    return all_rows


# ---------------------------------------------------------------------------
# Static circuit length lookup (OpenF1 API does not expose this field).
# Keyed by circuit_short_name as returned by the OpenF1 meetings endpoint.
# ---------------------------------------------------------------------------
CIRCUIT_LENGTHS_KM: dict[str, float] = {
    'Sakhir':            5.412,  # Bahrain
    'Jeddah':            6.174,  # Saudi Arabia
    'Melbourne':         5.303,  # Australia
    'Suzuka':            5.807,  # Japan
    'Shanghai':          5.451,  # China
    'Miami':             5.410,  # Miami
    'Imola':             4.909,  # Emilia-Romagna
    'Monte Carlo':       3.337,  # Monaco
    'Montreal':          4.361,  # Canada
    'Catalunya':         4.675,  # Spain (Barcelona)
    'Spielberg':         4.318,  # Austria
    'Silverstone':       5.891,  # Great Britain
    'Hungaroring':       4.381,  # Hungary
    'Spa-Francorchamps': 7.004,  # Belgium
    'Zandvoort':         4.259,  # Netherlands
    'Monza':             5.793,  # Italy
    'Baku':              6.003,  # Azerbaijan
    'Singapore':         4.940,  # Singapore
    'Austin':            5.513,  # USA (COTA)
    'Mexico City':       4.304,  # Mexico
    'Interlagos':        4.309,  # Brazil
    'Las Vegas':         6.201,  # Las Vegas
    'Lusail':            5.419,  # Qatar
    'Yas Marina':        5.281,  # Abu Dhabi
}

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
        "circuit_length_km":  CIRCUIT_LENGTHS_KM.get(m.get("circuit_short_name") or ''),
    }
    _upsert(client, "races", [row])
    return m


def backfill_circuit_lengths(client: Client) -> None:
    """Update circuit_length_km for all meetings already in the races table
    using the static CIRCUIT_LENGTHS_KM lookup (OpenF1 does not expose this field)."""
    races_resp = client.table('races').select('meeting_key, circuit_short_name').execute()
    meetings = races_resp.data or []
    print(f"Backfilling circuit_length_km for {len(meetings)} meetings...")
    updated = skipped = 0
    for m in meetings:
        mk = m['meeting_key']
        short_name = m.get('circuit_short_name') or ''
        length = CIRCUIT_LENGTHS_KM.get(short_name)
        if length is not None:
            client.table('races').update({'circuit_length_km': length}).eq('meeting_key', mk).execute()
            print(f"  meeting {mk} ({short_name}): {length} km")
            updated += 1
        else:
            print(f"  meeting {mk} ({short_name!r}): not in lookup table — add to CIRCUIT_LENGTHS_KM")
            skipped += 1
    print(f"Done: {updated} updated, {skipped} skipped")


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
    # Deduplicate by (session_key, driver_number) — OpenF1 occasionally returns
    # duplicate entries for the same driver (e.g. team changes, data artifacts).
    # ON CONFLICT DO UPDATE cannot affect the same row twice in one batch.
    seen: dict[tuple, dict] = {}
    for d in drivers:
        if not (d.get("session_key") and d.get("driver_number") is not None):
            continue
        key = (d["session_key"], d["driver_number"])
        seen[key] = {
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
    rows = list(seen.values())
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
    ``date`` is <= the lap's ``date_start``.  Laps that precede all phase events
    are assigned to the first phase (Q1) rather than None — this handles drivers
    whose flying lap starts fractionally before the race_control Q1 marker fires.

    Handles both integer (1/2/3) and string ("Q1"/"Q2"/"Q3") qualifying_phase
    values returned by the OpenF1 API.
    """
    phase_events = sorted(
        [r for r in race_control if _normalize_phase(r.get("qualifying_phase")) and r.get("date")],
        key=lambda r: r["date"],
    )
    # Laps before the first phase event get None — we don't infer a phase the
    # API didn't emit (e.g. OpenF1 sometimes omits the Q1 marker entirely).
    first_phase = None
    for lap in laps:
        date_start = lap.get("date_start")
        if date_start is None:
            lap["_phase"] = None
            continue
        current_phase = first_phase
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

    # Include every driver who appeared in any lap, so drivers eliminated in Q1
    # with no phase-assigned laps still show up (with null times) rather than
    # being omitted from qualifying_results entirely.
    all_drivers = (
        set(best_per_phase.keys())
        | set(laps_per_phase.keys())
        | {lap["driver_number"] for lap in laps if lap.get("driver_number") is not None}
    )
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


def ingest_fastest_lap_flag(client: Client, session_key: int, laps: list[dict]) -> None:
    """Set fastest_lap_flag on race_results for the driver with the shortest valid lap.

    Only classified finishers (not DNF/DNS/DSQ) are considered.  The results
    endpoint is already cached from ingest_race_results, so this costs no
    additional API calls.
    """
    results = openf1.get_session_result(session_key)
    classified = {
        r["driver_number"]
        for r in results
        if r.get("driver_number") is not None
        and not r.get("dnf") and not r.get("dns") and not r.get("dsq")
    }
    if not classified:
        return

    best_time: float | None = None
    best_driver: int | None = None
    for lap in laps:
        dn = lap.get("driver_number")
        if dn not in classified:
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
        if best_time is None or t < best_time:
            best_time = t
            best_driver = dn

    if best_driver is None:
        return

    rows = [
        {
            "session_key":      session_key,
            "driver_number":    dn,
            "fastest_lap_flag": dn == best_driver,
        }
        for dn in classified
    ]
    _upsert(client, "race_results", rows)


def ingest_overtakes(client: Client, session_key: int) -> list[dict]:
    """Upsert overtake events into `overtakes`. Returns raw API rows."""
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
    return overtakes


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


def ingest_intervals(client: Client, session_key: int) -> list[dict]:
    """Upsert gap-to-leader and interval data into `intervals` (race/sprint only).
    Returns raw API rows."""
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
    return intervals


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
    all_pts = sorted(
        [s["points_current"] for s in standings if s.get("points_current") is not None],
        reverse=True,
    )
    leader_pts = all_pts[0] if all_pts else None
    p2_pts = all_pts[1] if len(all_pts) > 1 else None
    rows = []
    for s in standings:
        if not s.get("session_key") or s.get("driver_number") is None:
            continue
        pc = s.get("points_current")
        rows.append({
            "session_key":          s["session_key"],
            "driver_number":        s["driver_number"],
            "points_start":         s.get("points_start"),
            "points_current":       pc,
            "position_start":       s.get("position_start"),
            "position_current":     s.get("position_current"),
            "points_gap_to_leader": (leader_pts - pc) if (leader_pts is not None and pc is not None) else None,
            "points_gap_to_p2":     (p2_pts - pc)     if (p2_pts     is not None and pc is not None) else None,
        })
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
    all_pts = sorted(
        [s["points_current"] for s in standings if s.get("points_current") is not None],
        reverse=True,
    )
    leader_pts = all_pts[0] if all_pts else None
    p2_pts = all_pts[1] if len(all_pts) > 1 else None
    rows = []
    for s in standings:
        if not s.get("session_key") or not s.get("team_name"):
            continue
        pc = s.get("points_current")
        rows.append({
            "session_key":          s["session_key"],
            "team_name":            s["team_name"],
            "points_start":         s.get("points_start"),
            "points_current":       pc,
            "position_start":       s.get("position_start"),
            "position_current":     s.get("position_current"),
            "points_gap_to_leader": (leader_pts - pc) if (leader_pts is not None and pc is not None) else None,
            "points_gap_to_p2":     (p2_pts - pc)     if (p2_pts     is not None and pc is not None) else None,
        })
    _upsert(client, "championship_teams", rows)


def _compute_laps_led_by_sk(
    laps_raw: list[dict],
    race_results: list[dict],
    position_rows: list[dict],
) -> dict[int, dict[int, int]]:
    """Return {session_key: {driver_number: laps_led_count}}.

    Primary method: for each (session, lap_number) find the driver whose
    date_start is earliest — they crossed the line first and were leading.
    Fallback: for laps where every driver has a null date_start, estimate the
    lap's start time by linear interpolation from neighbouring known laps and
    look up who held P1 in the position table at that moment.
    """
    # --- Primary: date_start-based attribution ---
    _lap_starts: dict[tuple[int, int], list[tuple]] = defaultdict(list)
    for lap in laps_raw:
        ds = lap.get('date_start')
        if ds is not None:
            _lap_starts[(lap['session_key'], lap['lap_number'])].append(
                (ds, lap['driver_number'])
            )

    laps_led: dict[int, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    for (sk, _ln), entries in _lap_starts.items():
        if entries:
            entries.sort()
            laps_led[sk][entries[0][1]] += 1

    # --- Find which laps have no date_start at all ---
    total_laps_by_sk: dict[int, int] = defaultdict(int)
    for rr in race_results:
        sk = rr['session_key']
        n = rr.get('number_of_laps') or 0
        total_laps_by_sk[sk] = max(total_laps_by_sk[sk], n)

    missing: list[tuple[int, int]] = []
    for sk, total in total_laps_by_sk.items():
        for ln in range(1, total + 1):
            if (sk, ln) not in _lap_starts:
                missing.append((sk, ln))

    if not missing or not position_rows:
        return laps_led

    # --- Build P1 event timeline per session (sorted by date) ---
    p1_events_by_sk: dict[int, list[tuple[str, int]]] = defaultdict(list)
    for p in position_rows:
        if p.get('position') == 1 and p.get('date') and p.get('driver_number') is not None:
            p1_events_by_sk[p['session_key']].append((p['date'], p['driver_number']))
    for sk in p1_events_by_sk:
        p1_events_by_sk[sk].sort()

    # --- Build per-session sorted list of known lap start times ---
    known_start_by_skln: dict[tuple[int, int], str] = {}
    for lap in laps_raw:
        ds = lap.get('date_start')
        if ds is not None:
            key = (lap['session_key'], lap['lap_number'])
            if key not in known_start_by_skln or ds < known_start_by_skln[key]:
                known_start_by_skln[key] = ds

    sk_sorted_laps: dict[int, list[tuple[int, str]]] = defaultdict(list)
    for (sk, ln), t in known_start_by_skln.items():
        sk_sorted_laps[sk].append((ln, t))
    for sk in sk_sorted_laps:
        sk_sorted_laps[sk].sort()

    def _leader_at(sk: int, t_str: str) -> int | None:
        events = p1_events_by_sk.get(sk, [])
        if not events:
            return None
        dates = [e[0] for e in events]
        idx = bisect.bisect_right(dates, t_str) - 1
        return events[idx][1] if idx >= 0 else events[0][1]

    def _estimate_start(sk: int, ln: int) -> str | None:
        known = sk_sorted_laps.get(sk, [])
        if not known:
            return None
        lap_nums = [x[0] for x in known]
        idx = bisect.bisect_right(lap_nums, ln)  # first index where lap_num > ln
        if idx == 0:
            ln1, t1 = known[0]
            ln2, t2 = known[1] if len(known) > 1 else known[0]
        elif idx >= len(known):
            ln2, t2 = known[-1]
            ln1, t1 = known[-2] if len(known) > 1 else known[-1]
        else:
            ln1, t1 = known[idx - 1]
            ln2, t2 = known[idx]
        if ln1 == ln2:
            return t1
        try:
            dt1 = datetime.fromisoformat(t1.replace('Z', '+00:00'))
            dt2 = datetime.fromisoformat(t2.replace('Z', '+00:00'))
            frac = (ln - ln1) / (ln2 - ln1)
            est = dt1 + timedelta(seconds=(dt2 - dt1).total_seconds() * frac)
            return est.isoformat()
        except Exception:
            return None

    for sk, ln in missing:
        t_est = _estimate_start(sk, ln)
        if t_est is None:
            continue
        leader = _leader_at(sk, t_est)
        if leader is not None:
            laps_led[sk][leader] += 1

    return laps_led


def ingest_season_driver_stats(client: Client, year: int) -> None:
    """Compute and upsert cumulative per-driver season stats, one row per (year, round, driver)."""
    # 1. Round order is derived from ingested meetings only (sorted by date).
    #    Using the full F1 calendar would assign calendar-position round numbers,
    #    causing gaps (e.g. R6 for Miami when R4/R5 aren't ingested yet).
    races_resp = client.table('races').select('meeting_key,date_start,circuit_length_km').eq('year', year).execute()
    meetings = sorted(races_resp.data or [], key=lambda m: m.get('date_start') or '')
    if not meetings:
        return
    meeting_keys = [m['meeting_key'] for m in meetings]
    meeting_order = {m['meeting_key']: i + 1 for i, m in enumerate(meetings)}
    circuit_len_by_mk: dict[int, float | None] = {
        m['meeting_key']: m.get('circuit_length_km') for m in meetings
    }

    # Purge existing rows for this year so stale round numbers from prior runs
    # (when using full-calendar ordering) don't linger in the DB.
    client.table('season_driver_stats').delete().eq('year', year).execute()

    # 2. Sessions for these meetings.
    sessions_resp = (
        client.table('sessions')
        .select('session_key,meeting_key,session_type,session_name')
        .in_('meeting_key', meeting_keys)
        .execute()
    )
    all_sessions = sessions_resp.data or []
    all_sk  = [s['session_key'] for s in all_sessions]
    # Use session_type for race/qual split (OpenF1 returns "Race"/"Qualifying"),
    # and session_name to distinguish sprint from race within each type.
    race_sk = [s['session_key'] for s in all_sessions
               if (s.get('session_type') or '').lower() == 'race']
    qual_sk = [s['session_key'] for s in all_sessions
               if (s.get('session_type') or '').lower() == 'qualifying']

    if not race_sk:
        return

    # Sprint classification uses session_name (session_type is "Race" for both).
    sk_is_sprint: dict[int, bool] = {
        s['session_key']: (s.get('session_name') or '').lower() == 'sprint'
        for s in all_sessions
    }
    sk_is_sprint_qual: dict[int, bool] = {
        s['session_key']: (s.get('session_name') or '').lower() in ('sprint qualifying', 'sprint shootout')
        for s in all_sessions
    }

    # Group session keys by meeting.
    race_sk_by_meeting: dict[int, list[int]] = defaultdict(list)
    qual_sk_by_meeting: dict[int, list[int]] = defaultdict(list)
    for s in all_sessions:
        mk = s['meeting_key']
        st = (s.get('session_type') or '').lower()
        if st == 'race':
            race_sk_by_meeting[mk].append(s['session_key'])
        elif st == 'qualifying':
            qual_sk_by_meeting[mk].append(s['session_key'])

    # 3. Batch-read all needed tables.
    race_results = _query_in(client, 'race_results',
        'session_key,driver_number,position,number_of_laps,dnf,dns,dsq,fastest_lap_flag,points',
        'session_key', race_sk)
    laps_raw = _query_in_all(client, 'laps',
        'session_key,driver_number,lap_number,date_start',
        'session_key', race_sk)
    champ_drv = _query_in(client, 'championship_drivers',
        'session_key,driver_number,points_current',
        'session_key', race_sk)
    overtakes = _query_in(client, 'overtakes',
        'session_key,driver_number_overtaking,driver_number_overtaken',
        'session_key', race_sk)
    pit_stops = _query_in(client, 'pit_stops',
        'session_key,driver_number',
        'session_key', race_sk)
    starting_grid = _query_in(client, 'starting_grid',
        'session_key,driver_number,position',
        'session_key', qual_sk)
    qual_results = _query_in(client, 'qualifying_results',
        'session_key,driver_number,best_lap_time',
        'session_key', qual_sk)
    drivers_raw = _query_in(client, 'drivers',
        'session_key,driver_number,team_name',
        'session_key', all_sk)
    champ_teams = _query_in(client, 'championship_teams',
        'session_key,team_name,points_current',
        'session_key', race_sk)
    position_rows = _query_in_all(client, 'position',
        'session_key,driver_number,date,position',
        'session_key', race_sk)

    # 4. Build lookup indexes.
    rr_by_sk: dict[int, dict[int, dict]] = defaultdict(dict)
    for rr in race_results:
        rr_by_sk[rr['session_key']][rr['driver_number']] = rr

    champ_pts_by_sk: dict[int, dict[int, float | None]] = defaultdict(dict)
    for cd in champ_drv:
        champ_pts_by_sk[cd['session_key']][cd['driver_number']] = cd.get('points_current')

    champ_team_pts_by_sk: dict[int, dict[str, float | None]] = defaultdict(dict)
    for ct in champ_teams:
        champ_team_pts_by_sk[ct['session_key']][ct['team_name']] = ct.get('points_current')

    ov_made_by_sk: dict[int, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    ov_suffered_by_sk: dict[int, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    for ov in overtakes:
        ov_made_by_sk[ov['session_key']][ov['driver_number_overtaking']] += 1
        ov_suffered_by_sk[ov['session_key']][ov['driver_number_overtaken']] += 1

    ps_count_by_sk: dict[int, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    for ps in pit_stops:
        ps_count_by_sk[ps['session_key']][ps['driver_number']] += 1

    # Laps led: primary method uses date_start; position table fills in laps
    # where every driver's date_start is null so sum(laps_led) == total race laps.
    laps_led_by_sk = _compute_laps_led_by_sk(laps_raw, race_results, position_rows)

    # Pole per qualifying session: session_key -> driver_number
    pole_by_qual_sk: dict[int, int] = {}
    for sg in starting_grid:
        if sg.get('position') == 1 and sg.get('driver_number') is not None:
            pole_by_qual_sk[sg['session_key']] = sg['driver_number']

    # Qualifying best time per session per driver.
    qt_by_sk: dict[int, dict[int, float | None]] = defaultdict(dict)
    for qr in qual_results:
        qt_by_sk[qr['session_key']][qr['driver_number']] = qr.get('best_lap_time')

    # Team per (session_key, driver_number).
    team_by_sk_dn: dict[tuple[int, int], str | None] = {}
    for d in drivers_raw:
        team_by_sk_dn[(d['session_key'], d['driver_number'])] = d.get('team_name')

    # Teammates per session: session_key -> driver_number -> [teammate_numbers]
    team_members_by_sk: dict[int, dict[str, list[int]]] = defaultdict(lambda: defaultdict(list))
    for d in drivers_raw:
        if d.get('team_name'):
            team_members_by_sk[d['session_key']][d['team_name']].append(d['driver_number'])
    teammates_by_sk: dict[int, dict[int, list[int]]] = defaultdict(dict)
    for sk, team_map in team_members_by_sk.items():
        for members in team_map.values():
            for dn in members:
                teammates_by_sk[sk][dn] = [m for m in members if m != dn]

    # 5. Discover all driver numbers across race sessions.
    all_dns: set[int] = {rr['driver_number'] for rr in race_results}

    # 6. Accumulate stats per driver across meetings.
    def _new_state() -> dict:
        return {
            # Combined totals
            'races_entered': 0, 'races_classified': 0,
            'dnf_count': 0, 'dns_count': 0, 'dsq_count': 0,
            'laps_completed': 0, 'laps_led': 0, 'distance_km': 0.0,
            'wins': 0, 'podiums': 0, 'poles': 0,
            'fastest_laps': 0, 'wins_over_teammate': 0,
            'total_overtakes_made': 0, 'total_overtakes_suffered': 0,
            'total_pit_stops': 0, 'qual_gaps': [],
            # Race-specific
            'race_entries': 0, 'race_wins': 0, 'race_podiums': 0, 'race_poles': 0,
            'race_dnf_count': 0, 'race_dns_count': 0, 'race_dsq_count': 0,
            'race_points': 0.0,
            # Sprint-specific
            'sprint_entries': 0, 'sprint_wins': 0, 'sprint_podiums': 0, 'sprint_poles': 0,
            'sprint_dnf_count': 0, 'sprint_dns_count': 0, 'sprint_dsq_count': 0,
            'sprint_points': 0.0,
        }

    state: dict[int, dict] = {dn: _new_state() for dn in all_dns}
    running_pts: dict[int, float | None] = {}  # last known championship points per driver

    rows: list[dict] = []

    for meeting in meetings:
        mk = meeting['meeting_key']
        round_num = meeting_order[mk]

        # --- race/sprint sessions ---
        for sk in race_sk_by_meeting.get(mk, []):
            is_sprint = sk_is_sprint.get(sk, False)
            for dn, rr in rr_by_sk.get(sk, {}).items():
                if dn not in state:
                    state[dn] = _new_state()
                c = state[dn]
                c['races_entered'] += 1
                pts_earned = rr.get('points') or 0.0
                if is_sprint:
                    c['sprint_entries'] += 1
                    c['sprint_points'] += pts_earned
                else:
                    c['race_entries'] += 1
                    c['race_points'] += pts_earned
                if rr.get('dns'):
                    c['dns_count'] += 1
                    c['sprint_dns_count' if is_sprint else 'race_dns_count'] += 1
                elif rr.get('dsq'):
                    c['dsq_count'] += 1
                    c['sprint_dsq_count' if is_sprint else 'race_dsq_count'] += 1
                elif rr.get('dnf'):
                    c['dnf_count'] += 1
                    c['sprint_dnf_count' if is_sprint else 'race_dnf_count'] += 1
                else:
                    c['races_classified'] += 1
                laps_in_session = rr.get('number_of_laps') or 0
                c['laps_completed'] += laps_in_session
                c['laps_led']       += laps_led_by_sk[sk].get(dn, 0)
                ckm = circuit_len_by_mk.get(mk)
                if ckm:
                    c['distance_km'] += laps_in_session * ckm
                pos = rr.get('position')
                if pos == 1:
                    c['wins'] += 1
                    c['sprint_wins' if is_sprint else 'race_wins'] += 1
                if pos is not None and pos <= 3:
                    c['podiums'] += 1
                    c['sprint_podiums' if is_sprint else 'race_podiums'] += 1
                if rr.get('fastest_lap_flag'):
                    c['fastest_laps'] += 1
                c['total_overtakes_made']     += ov_made_by_sk[sk].get(dn, 0)
                c['total_overtakes_suffered'] += ov_suffered_by_sk[sk].get(dn, 0)
                c['total_pit_stops']          += ps_count_by_sk[sk].get(dn, 0)
                # wins_over_teammate (count once per session even if multiple teammates)
                if pos is not None and not rr.get('dns') and not rr.get('dsq'):
                    for tm in teammates_by_sk[sk].get(dn, []):
                        tm_pos = rr_by_sk[sk].get(tm, {}).get('position')
                        if tm_pos is not None and pos < tm_pos:
                            c['wins_over_teammate'] += 1
                            break
            # Update running championship points.
            for dn, pts in champ_pts_by_sk.get(sk, {}).items():
                running_pts[dn] = pts

        # --- qualifying sessions (poles + qual gaps) ---
        for sk in qual_sk_by_meeting.get(mk, []):
            is_sprint_qual = sk_is_sprint_qual.get(sk, False)
            pole_dn = pole_by_qual_sk.get(sk)
            if pole_dn is not None:
                if pole_dn not in state:
                    state[pole_dn] = _new_state()
                state[pole_dn]['poles'] += 1
                state[pole_dn]['sprint_poles' if is_sprint_qual else 'race_poles'] += 1

            qt = qt_by_sk.get(sk, {})
            for dn, my_time in qt.items():
                if my_time is None:
                    continue
                for tm in teammates_by_sk[sk].get(dn, []):
                    tm_time = qt.get(tm)
                    if tm_time is not None:
                        if dn not in state:
                            state[dn] = _new_state()
                        state[dn]['qual_gaps'].append(my_time - tm_time)
                        break

        # --- get team points for this round (latest per team across race sessions) ---
        team_pts_at_round: dict[str, float] = {}
        for sk in race_sk_by_meeting.get(mk, []):
            for team, pts in champ_team_pts_by_sk.get(sk, {}).items():
                if pts is not None:
                    team_pts_at_round[team] = pts

        # --- emit one row per driver seen so far ---
        for dn, c in state.items():
            if c['races_entered'] == 0 and c['poles'] == 0:
                continue  # driver hasn't appeared yet
            pts = running_pts.get(dn)

            # Team membership for this driver at this meeting (use first race session found).
            team: str | None = None
            for sk in race_sk_by_meeting.get(mk, []):
                team = team_by_sk_dn.get((sk, dn))
                if team:
                    break
            if team is None:  # try qualifying sessions
                for sk in qual_sk_by_meeting.get(mk, []):
                    team = team_by_sk_dn.get((sk, dn))
                    if team:
                        break

            pct_team: float | None = None
            if team and pts is not None:
                team_total = team_pts_at_round.get(team)
                if team_total and team_total > 0:
                    pct_team = round((pts / team_total) * 100.0, 4)

            gaps = c['qual_gaps']
            qual_gap_mean = sum(gaps) / len(gaps) if gaps else None
            rows.append({
                'year':                       year,
                'round_number':               round_num,
                'driver_number':              dn,
                'races_entered':              c['races_entered'],
                'race_entries':               c['race_entries'],
                'sprint_entries':             c['sprint_entries'],
                'races_classified':           c['races_classified'],
                'dnf_count':                  c['dnf_count'],
                'dns_count':                  c['dns_count'],
                'dsq_count':                  c['dsq_count'],
                'race_dnf_count':             c['race_dnf_count'],
                'race_dns_count':             c['race_dns_count'],
                'race_dsq_count':             c['race_dsq_count'],
                'sprint_dnf_count':           c['sprint_dnf_count'],
                'sprint_dns_count':           c['sprint_dns_count'],
                'sprint_dsq_count':           c['sprint_dsq_count'],
                'laps_completed':             c['laps_completed'],
                'laps_led':                   c['laps_led'],
                'distance_km':                round(c['distance_km'], 3) if c['distance_km'] else None,
                'wins':                       c['wins'],
                'race_wins':                  c['race_wins'],
                'sprint_wins':                c['sprint_wins'],
                'podiums':                    c['podiums'],
                'race_podiums':               c['race_podiums'],
                'sprint_podiums':             c['sprint_podiums'],
                'poles':                      c['poles'],
                'race_poles':                 c['race_poles'],
                'sprint_poles':               c['sprint_poles'],
                'fastest_laps':               c['fastest_laps'],
                'points_scored':              pts,
                'race_points':                c['race_points'],
                'sprint_points':              c['sprint_points'],
                'wins_over_teammate':         c['wins_over_teammate'],
                'qualifying_supertime_gap_s': qual_gap_mean,
                'percent_of_team_points':     pct_team,
                'total_overtakes_made':       c['total_overtakes_made'],
                'total_overtakes_suffered':   c['total_overtakes_suffered'],
                'total_pit_stops':            c['total_pit_stops'],
            })

    _upsert(client, 'season_driver_stats', rows)


def ingest_season_constructor_stats(client: Client, year: int) -> None:
    """Compute and upsert cumulative per-constructor season stats, one row per (year, round, team)."""
    # 1. Round order is derived from ingested meetings only (sorted by date).
    #    Using the full F1 calendar would assign calendar-position round numbers,
    #    causing gaps (e.g. R6 for Miami when R4/R5 aren't ingested yet).
    races_resp = client.table('races').select('meeting_key,date_start,circuit_length_km').eq('year', year).execute()
    meetings = sorted(races_resp.data or [], key=lambda m: m.get('date_start') or '')
    if not meetings:
        return
    meeting_keys = [m['meeting_key'] for m in meetings]
    meeting_order = {m['meeting_key']: i + 1 for i, m in enumerate(meetings)}
    circuit_len_by_mk: dict[int, float | None] = {
        m['meeting_key']: m.get('circuit_length_km') for m in meetings
    }

    # Purge existing rows for this year so stale round numbers from prior runs
    # (when using full-calendar ordering) don't linger in the DB.
    client.table('season_constructor_stats').delete().eq('year', year).execute()

    # 2. Sessions for these meetings.
    sessions_resp = (
        client.table('sessions')
        .select('session_key,meeting_key,session_type,session_name')
        .in_('meeting_key', meeting_keys)
        .execute()
    )
    all_sessions = sessions_resp.data or []
    all_sk  = [s['session_key'] for s in all_sessions]
    race_sk = [s['session_key'] for s in all_sessions
                if (s.get('session_type') or '').lower() == 'race']
    qual_sk = [s['session_key'] for s in all_sessions
                if (s.get('session_type') or '').lower() == 'qualifying']

    if not race_sk:
        return

    sk_is_sprint: dict[int, bool] = {
        s['session_key']: (s.get('session_name') or '').lower() == 'sprint'
        for s in all_sessions
    }
    sk_is_sprint_qual: dict[int, bool] = {
        s['session_key']: (s.get('session_name') or '').lower() in ('sprint qualifying', 'sprint shootout')
        for s in all_sessions
    }

    race_sk_by_meeting: dict[int, list[int]] = defaultdict(list)
    qual_sk_by_meeting: dict[int, list[int]] = defaultdict(list)
    for s in all_sessions:
        mk = s['meeting_key']
        st = (s.get('session_type') or '').lower()
        if st == 'race':
            race_sk_by_meeting[mk].append(s['session_key'])
        elif st == 'qualifying':
            qual_sk_by_meeting[mk].append(s['session_key'])

    # 3. Batch-read tables.
    race_results = _query_in(client, 'race_results',
        'session_key,driver_number,position,number_of_laps,dnf,dns,dsq,fastest_lap_flag,points',
        'session_key', race_sk)
    laps_raw = _query_in_all(client, 'laps',
        'session_key,driver_number,lap_number,date_start',
        'session_key', race_sk)
    champ_teams = _query_in(client, 'championship_teams',
        'session_key,team_name,points_current',
        'session_key', race_sk)
    pit_stops = _query_in(client, 'pit_stops',
        'session_key,driver_number',
        'session_key', race_sk)
    starting_grid = _query_in(client, 'starting_grid',
        'session_key,driver_number,position',
        'session_key', qual_sk)
    drivers_raw = _query_in(client, 'drivers',
        'session_key,driver_number,team_name',
        'session_key', all_sk)
    position_rows = _query_in_all(client, 'position',
        'session_key,driver_number,date,position',
        'session_key', race_sk)

    # 4. Build indexes.
    rr_by_sk: dict[int, dict[int, dict]] = defaultdict(dict)
    for rr in race_results:
        rr_by_sk[rr['session_key']][rr['driver_number']] = rr

    ct_pts_by_sk: dict[int, dict[str, float | None]] = defaultdict(dict)
    for ct in champ_teams:
        ct_pts_by_sk[ct['session_key']][ct['team_name']] = ct.get('points_current')

    ps_count_by_sk: dict[int, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    for ps in pit_stops:
        ps_count_by_sk[ps['session_key']][ps['driver_number']] += 1

    pole_by_qual_sk: dict[int, tuple[int, str | None]] = {}  # sk -> (driver_number, team)
    # team will be filled after building team_by_sk_dn
    pole_dn_by_qual_sk: dict[int, int] = {}
    for sg in starting_grid:
        if sg.get('position') == 1 and sg.get('driver_number') is not None:
            pole_dn_by_qual_sk[sg['session_key']] = sg['driver_number']

    team_by_sk_dn: dict[tuple[int, int], str | None] = {}
    for d in drivers_raw:
        team_by_sk_dn[(d['session_key'], d['driver_number'])] = d.get('team_name')

    # Teams per race session.
    teams_by_race_sk: dict[int, set[str]] = defaultdict(set)
    for d in drivers_raw:
        sk = d['session_key']
        if sk in race_sk and d.get('team_name'):
            teams_by_race_sk[sk].add(d['team_name'])

    # Drivers per team per race session.
    dns_by_sk_team: dict[tuple[int, str], list[int]] = defaultdict(list)
    for d in drivers_raw:
        if d.get('team_name') and d['session_key'] in set(race_sk):
            dns_by_sk_team[(d['session_key'], d['team_name'])].append(d['driver_number'])

    # All team names.
    all_teams: set[str] = {d['team_name'] for d in drivers_raw if d.get('team_name')}

    # Laps led per driver per session (with position-table fallback for null date_start laps).
    driver_laps_led_by_sk = _compute_laps_led_by_sk(laps_raw, race_results, position_rows)

    # Map driver-level laps_led to team level.
    constructor_laps_led_by_sk: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for sk, driver_counts in driver_laps_led_by_sk.items():
        for dn, cnt in driver_counts.items():
            team = team_by_sk_dn.get((sk, dn))
            if team:
                constructor_laps_led_by_sk[sk][team] += cnt

    # 5. Accumulate.
    def _new_team_state() -> dict:
        return {
            # Combined
            'wins': 0, 'podiums': 0, 'poles': 0, 'fastest_laps': 0,
            'races_entered': 0, 'dnf_count': 0, 'dns_count': 0, 'dsq_count': 0,
            'laps_completed': 0, 'laps_led': 0, 'distance_km': 0.0,
            'total_pit_stops': 0,
            # Race-specific
            'race_entries': 0, 'race_wins': 0, 'race_podiums': 0, 'race_poles': 0,
            'race_dnf_count': 0, 'race_dns_count': 0, 'race_dsq_count': 0, 'race_points': 0.0,
            # Sprint-specific
            'sprint_entries': 0, 'sprint_wins': 0, 'sprint_podiums': 0, 'sprint_poles': 0,
            'sprint_dnf_count': 0, 'sprint_dns_count': 0, 'sprint_dsq_count': 0, 'sprint_points': 0.0,
        }

    state: dict[str, dict] = {t: _new_team_state() for t in all_teams}
    running_pts: dict[str, float | None] = {}

    rows: list[dict] = []

    for meeting in meetings:
        mk = meeting['meeting_key']
        round_num = meeting_order[mk]

        # --- race/sprint sessions ---
        for sk in race_sk_by_meeting.get(mk, []):
            is_sprint = sk_is_sprint.get(sk, False)
            for dn, rr in rr_by_sk.get(sk, {}).items():
                team = team_by_sk_dn.get((sk, dn))
                if not team:
                    continue
                if team not in state:
                    state[team] = _new_team_state()
                c = state[team]
                pts_earned = rr.get('points') or 0.0
                c['races_entered'] += 1
                if is_sprint:
                    c['sprint_entries'] += 1
                    c['sprint_points'] += pts_earned
                else:
                    c['race_entries'] += 1
                    c['race_points'] += pts_earned
                if rr.get('dns'):
                    c['dns_count'] += 1
                    c['sprint_dns_count' if is_sprint else 'race_dns_count'] += 1
                elif rr.get('dsq'):
                    c['dsq_count'] += 1
                    c['sprint_dsq_count' if is_sprint else 'race_dsq_count'] += 1
                elif rr.get('dnf'):
                    c['dnf_count'] += 1
                    c['sprint_dnf_count' if is_sprint else 'race_dnf_count'] += 1
                laps_in_session = rr.get('number_of_laps') or 0
                c['laps_completed'] += laps_in_session
                ckm = circuit_len_by_mk.get(mk)
                if ckm:
                    c['distance_km'] += laps_in_session * ckm
                pos = rr.get('position')
                if pos == 1:
                    c['wins'] += 1
                    c['sprint_wins' if is_sprint else 'race_wins'] += 1
                if pos is not None and pos <= 3:
                    c['podiums'] += 1
                    c['sprint_podiums' if is_sprint else 'race_podiums'] += 1
                if rr.get('fastest_lap_flag'):
                    c['fastest_laps'] += 1
                c['total_pit_stops'] += ps_count_by_sk[sk].get(dn, 0)

            # Laps led credit per team for this session.
            for team_name, led_count in constructor_laps_led_by_sk.get(sk, {}).items():
                if team_name in state:
                    state[team_name]['laps_led'] += led_count

            for team, pts in ct_pts_by_sk.get(sk, {}).items():
                if pts is not None:
                    running_pts[team] = pts

        # --- qualifying sessions (poles) ---
        for sk in qual_sk_by_meeting.get(mk, []):
            is_sprint_qual = sk_is_sprint_qual.get(sk, False)
            pole_dn = pole_dn_by_qual_sk.get(sk)
            if pole_dn is not None:
                pole_team = team_by_sk_dn.get((sk, pole_dn))
                if not pole_team:
                    for rsk in race_sk_by_meeting.get(mk, []):
                        pole_team = team_by_sk_dn.get((rsk, pole_dn))
                        if pole_team:
                            break
                if pole_team:
                    if pole_team not in state:
                        state[pole_team] = _new_team_state()
                    state[pole_team]['poles'] += 1
                    state[pole_team]['sprint_poles' if is_sprint_qual else 'race_poles'] += 1

        # --- emit one row per team seen so far ---
        for team, c in state.items():
            if c['races_entered'] == 0 and c['poles'] == 0:
                continue
            pts = running_pts.get(team)
            rows.append({
                'year':              year,
                'round_number':      round_num,
                'team_name':         team,
                'points_scored':     pts,
                'race_points':       c['race_points'],
                'sprint_points':     c['sprint_points'],
                'wins':              c['wins'],
                'race_wins':         c['race_wins'],
                'sprint_wins':       c['sprint_wins'],
                'podiums':           c['podiums'],
                'race_podiums':      c['race_podiums'],
                'sprint_podiums':    c['sprint_podiums'],
                'poles':             c['poles'],
                'race_poles':        c['race_poles'],
                'sprint_poles':      c['sprint_poles'],
                'fastest_laps':      c['fastest_laps'],
                'races_entered':     c['races_entered'],
                'race_entries':      c['race_entries'],
                'sprint_entries':    c['sprint_entries'],
                'dnf_count':         c['dnf_count'],
                'race_dnf_count':    c['race_dnf_count'],
                'sprint_dnf_count':  c['sprint_dnf_count'],
                'dns_count':         c['dns_count'],
                'race_dns_count':    c['race_dns_count'],
                'sprint_dns_count':  c['sprint_dns_count'],
                'dsq_count':         c['dsq_count'],
                'race_dsq_count':    c['race_dsq_count'],
                'sprint_dsq_count':  c['sprint_dsq_count'],
                'laps_completed':    c['laps_completed'],
                'laps_led':          c['laps_led'],
                'distance_km':       round(c['distance_km'], 3) if c['distance_km'] else None,
                'total_pit_stops':   c['total_pit_stops'],
            })

    _upsert(client, 'season_constructor_stats', rows)


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

    # Superclipping: throttle >= 10% AND brake == 0 AND car decelerating
    # accel_g is length N-1 (left-edge); pad to N with False for the last sample
    decel_padded = np.append(accel_g < 0, False)
    superclip_mask = (throttle_reg >= 10) & (brake_reg == 0) & decel_padded
    scd_lap, scd_s1, scd_s2, scd_s3 = _dist_splits(superclip_mask)

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

        # Estimated superclipping: throttle >= 10% AND brake == 0 AND decelerating
        # In 2026+ regulations this is a battery-harvest proxy (car brakes via MGU-K under throttle)
        'estimated_superclipping_distance_m_lap': _r1(scd_lap),
        'estimated_superclipping_distance_m_s1':  _r1(scd_s1),
        'estimated_superclipping_distance_m_s2':  _r1(scd_s2),
        'estimated_superclipping_distance_m_s3':  _r1(scd_s3),

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
        df_all['date'] = pd.to_datetime(df_all['date'], format='ISO8601')
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


def _build_neutralized_periods(
    rc_rows: list[dict],
    pd_module,
) -> list[tuple[float, float]]:
    """Parse race control messages into (start_epoch, end_epoch) neutralized intervals.

    Three event types are handled:

    - Safety Car: ``SAFETY CAR DEPLOYED`` → ``SAFETY CAR IN THIS LAP``
    - Virtual Safety Car: ``VIRTUAL SAFETY CAR DEPLOYED`` → ``VIRTUAL SAFETY CAR ENDING``
    - Red flag: ``flag='RED'`` represented as a zero-width interval ``(ts, ts)``

    Lap overlap check: ``t_start <= period_end AND t_end >= period_start``
    (inclusive on both ends so a lap containing a boundary timestamp is still
    counted as neutralized).

    SC/VSC pairing scans forward to find the matching end message.  If no end
    message is found (e.g., truncated data) the period is silently skipped.
    """
    events: list[tuple[float, dict]] = []
    for r in rc_rows:
        d = r.get('date')
        if not d:
            continue
        try:
            events.append((pd_module.to_datetime(d).timestamp(), r))
        except Exception:
            pass
    events.sort(key=lambda x: x[0])

    periods: list[tuple[float, float]] = []
    i = 0
    while i < len(events):
        ts, r = events[i]
        cat = r.get('category')
        msg = (r.get('message') or '').upper()
        flag = r.get('flag')

        if cat == 'SafetyCar' and 'VIRTUAL SAFETY CAR DEPLOYED' in msg:
            # VSC: scan forward for VIRTUAL SAFETY CAR ENDING.
            for j in range(i + 1, len(events)):
                ts2, r2 = events[j]
                if r2.get('category') == 'SafetyCar' and \
                        'VIRTUAL SAFETY CAR ENDING' in (r2.get('message') or '').upper():
                    periods.append((ts, ts2))
                    break

        elif cat == 'SafetyCar' and 'DEPLOYED' in msg:
            # SC (checked after VSC so "VIRTUAL … DEPLOYED" is not matched here).
            # Scan forward for SAFETY CAR IN THIS LAP.
            for j in range(i + 1, len(events)):
                ts2, r2 = events[j]
                if r2.get('category') == 'SafetyCar' and \
                        'IN THIS LAP' in (r2.get('message') or '').upper():
                    periods.append((ts, ts2))
                    break

        elif flag == 'RED':
            # Red flag: zero-width point interval — overlap check degenerates to
            # the original point-in-time containment check for this case.
            periods.append((ts, ts))

        i += 1

    return periods


def ingest_lap_flags(
    client: Client,
    session_key: int,
    laps: list[dict],
    stints_rows: list[dict],
    rc_rows: list[dict],
) -> None:
    """Populate ``is_neutralized`` and ``tyre_age_at_lap`` in ``lap_metrics``.

    ``is_neutralized``: True when the lap's UTC window overlaps any neutralized
    period built from race control messages.  Periods are:

    - Safety Car: ``SAFETY CAR DEPLOYED`` → ``SAFETY CAR IN THIS LAP``
    - Virtual Safety Car: ``VIRTUAL SAFETY CAR DEPLOYED`` → ``VIRTUAL SAFETY CAR ENDING``
    - Red flag: the broadcast timestamp treated as a zero-width point interval

    This period-based approach correctly neutralizes laps that fall entirely
    within a SC/VSC deployment even when no new RC message is broadcast during
    that specific lap (e.g. SC laps after a pit stop during a sprint SC period).

    None when the lap's start time or duration is missing.

    ``tyre_age_at_lap``: ``tyre_age_at_start + (lap_number − stint.lap_start)``
    for the matching stint.  None when no matching stint is found (e.g. OpenF1
    data gaps on sprint stints).
    """
    import pandas as pd

    neutralized_periods = _build_neutralized_periods(rc_rows, pd)

    # Stint index: {driver_number: [stints sorted ascending by lap_start]}.
    stints_by_driver: dict[int, list[dict]] = {}
    for s in stints_rows:
        dn = s.get('driver_number')
        if dn is None:
            continue
        stints_by_driver.setdefault(dn, []).append(s)
    for dn in stints_by_driver:
        stints_by_driver[dn].sort(key=lambda s: s.get('lap_start') or 0)

    rows: list[dict] = []
    for lap in laps:
        dn = lap.get('driver_number')
        lap_num = lap.get('lap_number')
        if dn is None or lap_num is None:
            continue

        # is_neutralized -------------------------------------------------------
        is_neutralized: bool | None = None
        date_start = lap.get('date_start')
        lap_dur = lap.get('lap_duration')
        if date_start and lap_dur:
            try:
                t_start = pd.to_datetime(date_start).timestamp()
                t_end = t_start + float(lap_dur)
                is_neutralized = any(
                    t_start <= p_end and t_end >= p_start
                    for p_start, p_end in neutralized_periods
                )
            except Exception:
                pass

        # tyre_age_at_lap ------------------------------------------------------
        tyre_age: int | None = None
        for s in stints_by_driver.get(dn, []):
            lap_start = s.get('lap_start')
            lap_end = s.get('lap_end')
            if lap_start is None:
                continue
            if lap_num < lap_start:
                continue
            if lap_end is not None and lap_num > lap_end:
                continue
            age_at_start = s.get('tyre_age_at_start')
            if age_at_start is not None:
                tyre_age = age_at_start + (lap_num - lap_start)
            break

        rows.append({
            'session_key':     session_key,
            'driver_number':   dn,
            'lap_number':      lap_num,
            'is_neutralized':  is_neutralized,
            'tyre_age_at_lap': tyre_age,
        })

    _upsert(client, 'lap_metrics', rows)


def ingest_session_sector_bests(
    client: Client,
    session_key: int,
    laps: list[dict],
) -> None:
    """Compute session-best sector times and per-lap deltas.

    Upserts one row to ``session_sector_bests`` containing the fastest S1/S2/S3
    time set by any driver, the driver who set each, and the theoretical best
    lap (sum of the three sector bests).

    Also upserts ``delta_to_session_best_s1/s2/s3`` back to ``lap_metrics``
    for every lap where the sector time is available.  Zero-or-negative sector
    times are excluded from best computation (incomplete/aborted laps).
    """
    _SECTORS = (
        ('s1', 'duration_sector_1'),
        ('s2', 'duration_sector_2'),
        ('s3', 'duration_sector_3'),
    )

    # best[sector] = (time_float, driver_number) or None
    best: dict[str, tuple[float, int] | None] = {s: None for s, _ in _SECTORS}

    for lap in laps:
        dn = lap.get('driver_number')
        if dn is None:
            continue
        for sector, key in _SECTORS:
            t = lap.get(key)
            if t is None:
                continue
            try:
                t = float(t)
            except (TypeError, ValueError):
                continue
            if t <= 0:
                continue
            if best[sector] is None or t < best[sector][0]:
                best[sector] = (t, dn)

    best_s1 = best['s1'][0] if best['s1'] else None
    best_s2 = best['s2'][0] if best['s2'] else None
    best_s3 = best['s3'][0] if best['s3'] else None
    theoretical = (
        round(best_s1 + best_s2 + best_s3, 3)
        if best_s1 is not None and best_s2 is not None and best_s3 is not None
        else None
    )

    _upsert(client, 'session_sector_bests', [{
        'session_key':          session_key,
        'best_s1':              best_s1,
        'best_s1_driver':       best['s1'][1] if best['s1'] else None,
        'best_s2':              best_s2,
        'best_s2_driver':       best['s2'][1] if best['s2'] else None,
        'best_s3':              best_s3,
        'best_s3_driver':       best['s3'][1] if best['s3'] else None,
        'theoretical_best_lap': theoretical,
    }])

    # Per-lap deltas back to lap_metrics
    def _delta(sector_val, session_best: float | None) -> float | None:
        if sector_val is None or session_best is None:
            return None
        try:
            return round(float(sector_val) - session_best, 3)
        except (TypeError, ValueError):
            return None

    rows: list[dict] = []
    for lap in laps:
        dn = lap.get('driver_number')
        lap_num = lap.get('lap_number')
        if dn is None or lap_num is None:
            continue
        rows.append({
            'session_key':              session_key,
            'driver_number':            dn,
            'lap_number':               lap_num,
            'delta_to_session_best_s1': _delta(lap.get('duration_sector_1'), best_s1),
            'delta_to_session_best_s2': _delta(lap.get('duration_sector_2'), best_s2),
            'delta_to_session_best_s3': _delta(lap.get('duration_sector_3'), best_s3),
        })

    if rows:
        _upsert(client, 'lap_metrics', rows)


def ingest_brake_entry_speed_ranks(
    client: Client,
    session_key: int,
    driver_lap_metrics: dict[int, dict[int, dict]],
) -> None:
    """Compute cross-driver brake entry speed percentile ranks for the session.

    Pools all driver-lap ``speed_at_brake_start_kph_{sector}`` values per sector,
    then upserts ``brake_entry_speed_pct_rank_{sector}``,
    ``brake_entry_speed_z_score_{sector}``, and
    ``brake_entry_speed_category_{sector}`` back to ``lap_metrics``.

    Category thresholds (percentile rank):
      'early'   rank < 33.3  — driver braked before the typical session point
      'average' rank < 66.7  — mid-pack braking point
      'late'    rank >= 66.7 — driver braked later than typical
    """
    import numpy as np

    sectors = ('lap', 's1', 's2', 's3')

    entries: list[tuple[int, int, dict]] = []
    for dn, laps_by_num in driver_lap_metrics.items():
        for lap_num, metrics in laps_by_num.items():
            vals = {s: metrics.get(f'speed_at_brake_start_kph_{s}') for s in sectors}
            entries.append((dn, lap_num, vals))

    if not entries:
        return

    sector_stats: dict[str, tuple | None] = {}
    for sector in sectors:
        raw = [e[2][sector] for e in entries if e[2][sector] is not None]
        if len(raw) < 2:
            sector_stats[sector] = None
            continue
        arr = np.array(raw, dtype=float)
        mean = float(np.mean(arr))
        std = float(np.std(arr, ddof=0))
        sector_stats[sector] = (mean, std, sorted(raw))

    rows: list[dict] = []
    for dn, lap_num, vals in entries:
        row: dict = {'session_key': session_key, 'driver_number': dn, 'lap_number': lap_num}
        for sector in sectors:
            v = vals[sector]
            stats = sector_stats.get(sector)
            if v is None or stats is None:
                pct_rank = z_score = category = None
            else:
                mean, std, sorted_vals = stats
                n = len(sorted_vals)
                n_below = sum(1 for x in sorted_vals if x < v)
                n_equal = sum(1 for x in sorted_vals if x == v)
                pct_rank = round((n_below + 0.5 * n_equal) / n * 100, 1)
                z_score = round((v - mean) / std, 4) if std > 0 else 0.0
                if pct_rank < 100 / 3:
                    category = 'early'
                elif pct_rank < 200 / 3:
                    category = 'average'
                else:
                    category = 'late'
            row[f'brake_entry_speed_pct_rank_{sector}'] = pct_rank
            row[f'brake_entry_speed_z_score_{sector}'] = z_score
            row[f'brake_entry_speed_category_{sector}'] = category
        rows.append(row)

    _upsert(client, 'lap_metrics', rows)


def _parse_intervals_index(
    intervals_rows: list[dict],
) -> dict[int, list[tuple[float, float | None, float | None, int | None]]]:
    """Build a per-driver sorted index of interval records.

    Returns {driver_number: [(timestamp_epoch, interval, gap_to_leader, laps_down), ...]}.
    Each entry is sorted ascending by timestamp for binary-search lookups.
    ``interval`` and ``gap_to_leader`` are parsed through ``_parse_gap``.
    """
    import pandas as pd

    index: dict[int, list] = {}
    for row in intervals_rows:
        dn = row.get('driver_number')
        date = row.get('date')
        if dn is None or date is None:
            continue
        try:
            t = pd.to_datetime(date).timestamp()
        except Exception:
            continue
        interval, _ = _parse_gap(row.get('interval'))
        gap, laps_down = _parse_gap(row.get('gap_to_leader'))
        index.setdefault(dn, []).append((t, interval, gap, laps_down))
    for dn in index:
        index[dn].sort(key=lambda x: x[0])
    return index


def _nearest_interval_entry(
    index: dict[int, list],
    dn: int,
    t: float,
) -> tuple[float | None, float | None, int | None]:
    """Return (interval, gap_to_leader, laps_down) for driver ``dn`` at time ``t``.

    Uses bisect to find the nearest timestamp. Returns (None, None, None) when no
    records exist for this driver.
    """
    entries = index.get(dn)
    if not entries:
        return None, None, None
    timestamps = [e[0] for e in entries]
    idx = bisect.bisect_left(timestamps, t)
    if idx == 0:
        e = entries[0]
    elif idx == len(entries):
        e = entries[-1]
    else:
        before = entries[idx - 1]
        after = entries[idx]
        e = after if abs(after[0] - t) <= abs(before[0] - t) else before
    return e[1], e[2], e[3]


def _position_snapshot(
    index: dict[int, list],
    t: float,
) -> list[tuple[int, float | None, float | None]]:
    """Return a position-ordered list of (driver_number, interval, gap_to_leader) at time ``t``.

    Ordering: lead-lap drivers sorted by gap_to_leader ascending (leader first, None = 0.0),
    then lapped drivers sorted by laps_down ascending.
    """
    lead_lap: list[tuple[float, int, float | None, float | None]] = []
    lapped: list[tuple[int, int, float | None, float | None]] = []

    for dn in index:
        interval, gap, laps_down = _nearest_interval_entry(index, dn, t)
        if laps_down is not None and laps_down > 0:
            lapped.append((laps_down, dn, interval, gap))
        else:
            gtl_sort = gap if gap is not None else 0.0
            lead_lap.append((gtl_sort, dn, interval, gap))

    lead_lap.sort(key=lambda x: x[0])
    lapped.sort(key=lambda x: x[0])

    result = [(dn, interval, gap) for _, dn, interval, gap in lead_lap]
    for _, dn, interval, gap in lapped:
        result.append((dn, interval, gap))
    return result


def ingest_battle_states(
    client: Client,
    session_key: int,
    laps: list[dict],
    intervals_rows: list[dict],
    overtakes_rows: list[dict],
) -> None:
    """Populate gap, battle, overtake, and lap-context fields in ``lap_metrics``.

    Gap and battle fields (gap_ahead_s1/s2/s3, gap_behind_s1/s2/s3,
    battle_ahead/behind_s1/s2/s3_driver, is_estimated_clean_air) are derived from
    the intervals table by computing a position snapshot at each sector end timestamp.

    Overtake fields (overtakes_s1/s2/s3, overtaken_s1/s2/s3, lap_overtakes,
    lap_overtaken) count OpenF1 overtake events within each sector's UTC window.

    i1_speed, i2_speed, and sector_context_s1/s2/s3 are copied directly from
    the laps table (available for all session types).

    All fields are None when source data is missing or sector timestamps are unavailable.
    """
    import pandas as pd

    if not laps:
        return

    has_intervals = bool(intervals_rows)
    index = _parse_intervals_index(intervals_rows) if has_intervals else {}

    # Build overtake timestamp indexes.
    overtakes_made: dict[int, list[float]] = {}
    overtakes_suffered: dict[int, list[float]] = {}
    for row in overtakes_rows:
        date = row.get('date')
        if date is None:
            continue
        try:
            t = pd.to_datetime(date).timestamp()
        except Exception:
            continue
        dn_over = row.get('driver_number_overtaking') or row.get('overtaking_driver_number')
        dn_under = row.get('driver_number_overtaken') or row.get('overtaken_driver_number')
        if dn_over is not None:
            overtakes_made.setdefault(dn_over, []).append(t)
        if dn_under is not None:
            overtakes_suffered.setdefault(dn_under, []).append(t)
    for lst in overtakes_made.values():
        lst.sort()
    for lst in overtakes_suffered.values():
        lst.sort()

    def _count_in_window(ts_list: list[float], t_start: float, t_end: float) -> int:
        lo = bisect.bisect_left(ts_list, t_start)
        hi = bisect.bisect_right(ts_list, t_end)
        return hi - lo

    rows: list[dict] = []
    for lap in laps:
        dn = lap.get('driver_number')
        lap_num = lap.get('lap_number')
        if dn is None or lap_num is None:
            continue

        # Sector end timestamps.
        t0: float | None = None
        date_start = lap.get('date_start')
        if date_start:
            try:
                t0 = pd.to_datetime(date_start).timestamp()
            except Exception:
                pass

        s1_dur = lap.get('duration_sector_1')
        s2_dur = lap.get('duration_sector_2')
        lap_dur = lap.get('lap_duration')

        s1_end: float | None = (t0 + float(s1_dur)) if (t0 and s1_dur) else None
        s2_end: float | None = (t0 + float(s1_dur) + float(s2_dur)) if (t0 and s1_dur and s2_dur) else None
        s3_end: float | None = (t0 + float(lap_dur)) if (t0 and lap_dur) else None

        sector_ends = {'s1': s1_end, 's2': s2_end, 's3': s3_end}

        # Gap and battle state per sector.
        gap_ahead: dict[str, float | None] = {}
        gap_behind: dict[str, float | None] = {}
        battle_ahead_drv: dict[str, int | None] = {}
        battle_behind_drv: dict[str, int | None] = {}

        # Track clean-air status: True only if confirmed clean in all sectors.
        # None if no intervals data or any sector time is missing.
        clean_sectors: list[bool | None] = []

        for sector, t_sec in sector_ends.items():
            if t_sec is None or not has_intervals:
                gap_ahead[sector] = None
                gap_behind[sector] = None
                battle_ahead_drv[sector] = None
                battle_behind_drv[sector] = None
                clean_sectors.append(None)
                continue

            snapshot = _position_snapshot(index, t_sec)
            pos_map = {entry[0]: i for i, entry in enumerate(snapshot)}
            p = pos_map.get(dn)

            if p is None:
                # Driver not found in intervals for this session.
                gap_ahead[sector] = None
                gap_behind[sector] = None
                battle_ahead_drv[sector] = None
                battle_behind_drv[sector] = None
                clean_sectors.append(None)
                continue

            this_interval = snapshot[p][1]
            this_gap = snapshot[p][2]

            # OpenF1 artifact: the race leader sometimes returns interval=0.0 and
            # gap_to_leader=0.0 instead of null/null. Treat as leader (no car ahead).
            # Safe to condition on gap_to_leader=0.0 because a non-leader driver who is
            # genuinely 0.0s alongside the car ahead still has a positive gap_to_leader.
            is_leader = (this_interval is None and this_gap is None) or \
                        (this_interval == 0.0 and (this_gap is None or this_gap == 0.0))

            gap_ahead[sector] = None if is_leader else this_interval
            if is_leader:
                clean_sectors.append(True)
            elif this_interval is not None and this_interval > 2.0:
                clean_sectors.append(True)
            else:
                clean_sectors.append(False)

            # Battle ahead.
            if this_interval is not None and this_interval < 1.0 and p > 0:
                battle_ahead_drv[sector] = snapshot[p - 1][0]
            else:
                battle_ahead_drv[sector] = None

            # Gap behind: the driver at p+1's interval = their gap to driver at p (this driver).
            if p + 1 < len(snapshot):
                behind_entry = snapshot[p + 1]
                gap_behind[sector] = behind_entry[1]
                if behind_entry[1] is not None and behind_entry[1] < 1.0:
                    battle_behind_drv[sector] = behind_entry[0]
                else:
                    battle_behind_drv[sector] = None
            else:
                gap_behind[sector] = None
                battle_behind_drv[sector] = None

        # is_estimated_clean_air.
        if not has_intervals or None in clean_sectors:
            is_clean_air: bool | None = None
        else:
            is_clean_air = all(clean_sectors)

        # Overtakes per sector.
        made_list = overtakes_made.get(dn, [])
        suffered_list = overtakes_suffered.get(dn, [])

        if t0 is not None and s1_end is not None:
            ov_s1: int | None = _count_in_window(made_list, t0, s1_end)
            odn_s1: int | None = _count_in_window(suffered_list, t0, s1_end)
        else:
            ov_s1 = odn_s1 = None

        if s1_end is not None and s2_end is not None:
            ov_s2: int | None = _count_in_window(made_list, s1_end, s2_end)
            odn_s2: int | None = _count_in_window(suffered_list, s1_end, s2_end)
        else:
            ov_s2 = odn_s2 = None

        if s2_end is not None and s3_end is not None:
            ov_s3: int | None = _count_in_window(made_list, s2_end, s3_end)
            odn_s3: int | None = _count_in_window(suffered_list, s2_end, s3_end)
        else:
            ov_s3 = odn_s3 = None

        if ov_s1 is not None or ov_s2 is not None or ov_s3 is not None:
            lap_ov: int | None = (ov_s1 or 0) + (ov_s2 or 0) + (ov_s3 or 0)
            lap_odn: int | None = (odn_s1 or 0) + (odn_s2 or 0) + (odn_s3 or 0)
        else:
            lap_ov = lap_odn = None

        rows.append({
            'session_key':              session_key,
            'driver_number':            dn,
            'lap_number':               lap_num,
            'gap_ahead_s1':             gap_ahead['s1'],
            'gap_ahead_s2':             gap_ahead['s2'],
            'gap_ahead_s3':             gap_ahead['s3'],
            'gap_behind_s1':            gap_behind['s1'],
            'gap_behind_s2':            gap_behind['s2'],
            'gap_behind_s3':            gap_behind['s3'],
            'battle_ahead_s1_driver':   battle_ahead_drv['s1'],
            'battle_ahead_s2_driver':   battle_ahead_drv['s2'],
            'battle_ahead_s3_driver':   battle_ahead_drv['s3'],
            'battle_behind_s1_driver':  battle_behind_drv['s1'],
            'battle_behind_s2_driver':  battle_behind_drv['s2'],
            'battle_behind_s3_driver':  battle_behind_drv['s3'],
            'is_estimated_clean_air':   is_clean_air,
            'overtakes_s1':             ov_s1,
            'overtakes_s2':             ov_s2,
            'overtakes_s3':             ov_s3,
            'overtaken_s1':             odn_s1,
            'overtaken_s2':             odn_s2,
            'overtaken_s3':             odn_s3,
            'lap_overtakes':            lap_ov,
            'lap_overtaken':            lap_odn,
            'i1_speed':                 lap.get('i1_speed'),
            'i2_speed':                 lap.get('i2_speed'),
            'sector_context_s1':        lap.get('segments_sector_1'),
            'sector_context_s2':        lap.get('segments_sector_2'),
            'sector_context_s3':        lap.get('segments_sector_3'),
        })

    _upsert(client, 'lap_metrics', rows)


def ingest_stint_metrics(
    client: Client,
    session_key: int,
    laps: list[dict],
    stints_rows: list[dict],
) -> None:
    """Aggregate per-stint pace metrics into ``stint_metrics``.

    Reads ``is_estimated_clean_air`` and ``is_neutralized`` from ``lap_metrics``
    (written by earlier pipeline steps) and ``lap_duration`` / pit flags from the
    in-memory ``laps`` list.  Only race/sprint sessions should be passed; qualifying
    is skipped at the call site.

    Racing laps are laps that are not neutralized, not pit-in, not pit-out, and
    have a valid lap_duration.  ``is_neutralized=None`` is treated as not neutralized
    (include the lap unless we have positive evidence of a yellow/red/SC period).
    """
    if not laps or not stints_rows:
        return

    # --- lookup structures from laps -----------------------------------------
    lap_dur: dict[tuple[int, int], float] = {}
    pit_in: set[tuple[int, int]] = set()
    pit_out: set[tuple[int, int]] = set()

    for lap in laps:
        dn = lap.get('driver_number')
        ln = lap.get('lap_number')
        if dn is None or ln is None:
            continue
        dur = lap.get('lap_duration')
        if dur is not None:
            lap_dur[(dn, ln)] = float(dur)
        if lap.get('pit_in_time') is not None:
            pit_in.add((dn, ln))
        if lap.get('is_pit_out_lap'):
            pit_out.add((dn, ln))

    # --- stint assignment per driver -----------------------------------------
    driver_stints: dict[int, list[dict]] = defaultdict(list)
    for s in stints_rows:
        dn = s.get('driver_number')
        if dn is not None and s.get('stint_number') is not None and s.get('lap_start') is not None:
            driver_stints[dn].append(s)

    def _stint_for(dn: int, ln: int) -> int | None:
        for s in driver_stints.get(dn, []):
            start = s['lap_start']
            end = s.get('lap_end')
            if ln >= start and (end is None or ln <= end):
                return s['stint_number']
        return None

    # --- read clean_air / neutralized flags from Supabase --------------------
    resp = (
        client.table('lap_metrics')
        .select('driver_number,lap_number,is_estimated_clean_air,is_neutralized')
        .eq('session_key', session_key)
        .execute()
    )
    lm_data = resp.data or []

    # --- group into (driver, stint) buckets ----------------------------------
    stint_laps: dict[tuple[int, int], list[dict]] = defaultdict(list)
    for r in lm_data:
        dn = r.get('driver_number')
        ln = r.get('lap_number')
        if dn is None or ln is None:
            continue
        sn = _stint_for(dn, ln)
        if sn is None:
            continue
        stint_laps[(dn, sn)].append({
            'lap_number':             ln,
            'lap_duration':           lap_dur.get((dn, ln)),
            'is_estimated_clean_air': r.get('is_estimated_clean_air'),
            'is_neutralized':         r.get('is_neutralized'),
            'is_pit_in':              (dn, ln) in pit_in,
            'is_pit_out':             (dn, ln) in pit_out,
        })

    # --- compute metrics per stint -------------------------------------------
    def _mean(vals: list[float]) -> float | None:
        return sum(vals) / len(vals) if vals else None

    def _median(vals: list[float]) -> float | None:
        return statistics.median(vals) if vals else None

    rows: list[dict] = []
    for (dn, sn), entries in stint_laps.items():
        racing = sorted(
            [e for e in entries
             if e['lap_duration'] is not None
             and e['is_neutralized'] is not True
             and not e['is_pit_in']
             and not e['is_pit_out']],
            key=lambda e: e['lap_number'],
        )
        n = len(racing)
        durations = [e['lap_duration'] for e in racing]
        split = math.ceil(n / 2)

        rows.append({
            'session_key':           session_key,
            'driver_number':         dn,
            'stint_number':          sn,
            'representative_pace_s': _median(durations),
            'clean_air_pace_s':      _mean([e['lap_duration'] for e in racing
                                            if e['is_estimated_clean_air'] is True]),
            'dirty_air_pace_s':      _mean([e['lap_duration'] for e in racing
                                            if e['is_estimated_clean_air'] is False]),
            'first_half_pace_s':     _mean(durations[:split]),
            'second_half_pace_s':    _mean(durations[split:]),
            'racing_lap_count':      n,
        })

    _upsert(client, 'stint_metrics', rows)


def recompute_lap_metrics(
    client: Client,
    session_key: int,
    laps: list[dict],
    session_type: str,
    rc_rows: list[dict] | None = None,
    stints_rows: list[dict] | None = None,
    intervals_rows: list[dict] | None = None,
    overtakes_rows: list[dict] | None = None,
) -> None:
    """Regenerate derived metrics for a session without re-fetching raw data.

    For race/sprint sessions: populates ``lap_metrics`` with all car-data metrics
    and aggregates peak G means into ``race_results``.

    For qualifying sessions: populates ``lap_metrics`` per lap and upserts peak G
    for each driver's best lap per phase into ``qualifying_results``.

    ``rc_rows`` is required for qualifying sessions to assign phase labels to laps.
    ``intervals_rows`` and ``overtakes_rows`` are used for battle state metrics
    (race/sprint only; qualifying will still populate i1/i2/sector_context from laps).
    """
    if session_type in ('race', 'sprint'):
        print("  computing lap metrics (race/sprint) …")
        ingest_lap_flags(client, session_key, laps, stints_rows or [], rc_rows or [])
        ingest_session_sector_bests(client, session_key, laps)
        driver_lap_metrics = ingest_lap_metrics(client, session_key, laps)
        ingest_race_peak_g_summary(client, session_key, driver_lap_metrics)
        ingest_brake_entry_speed_ranks(client, session_key, driver_lap_metrics)
        ingest_battle_states(client, session_key, laps,
                             intervals_rows or [], overtakes_rows or [])
        ingest_stint_metrics(client, session_key, laps, stints_rows or [])
        total = sum(len(v) for v in driver_lap_metrics.values())
        print(f"  lap_metrics: {total} laps across {len(driver_lap_metrics)} drivers")
    elif session_type in ('qualifying', 'sprint qualifying', 'sprint shootout'):
        print("  computing lap metrics (qualifying) …")
        ingest_lap_flags(client, session_key, laps, stints_rows or [], rc_rows or [])
        ingest_session_sector_bests(client, session_key, laps)
        laps_with_phases = _assign_qualifying_phases(laps, rc_rows or [])
        best_per_phase   = _compute_qualifying_best_per_phase(laps_with_phases)
        driver_lap_metrics = ingest_lap_metrics(client, session_key, laps_with_phases)
        ingest_qualifying_peak_g_summary(client, session_key, driver_lap_metrics, best_per_phase)
        ingest_brake_entry_speed_ranks(client, session_key, driver_lap_metrics)
        ingest_battle_states(client, session_key, laps_with_phases,
                             intervals_rows or [], overtakes_rows or [])
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

    intervals_rows: list[dict] = []
    overtakes_rows: list[dict] = []

    if session_type in ("race", "sprint"):
        ingest_race_results(client, session_key, pit)
        ingest_fastest_lap_flag(client, session_key, laps)
        overtakes_rows = ingest_overtakes(client, session_key)
        intervals_rows = ingest_intervals(client, session_key)
        ingest_position(client, session_key)
        ingest_team_radio(client, session_key)
        ingest_championship_drivers(client, session_key)
        ingest_championship_teams(client, session_key)
    elif session_type in ("qualifying", "sprint qualifying", "sprint shootout"):
        ingest_qualifying_results(client, session_key, laps, rc_rows, stints_rows)
        ingest_starting_grid(client, session_key)

    if recompute:
        recompute_lap_metrics(client, session_key, laps, session_type,
                              rc_rows=rc_rows, stints_rows=stints_rows,
                              intervals_rows=intervals_rows, overtakes_rows=overtakes_rows)

    elapsed = time.time() - t0
    stats = openf1.get_stats()
    rate = openf1.current_rate()
    rate_str = f" @ {rate:.1f} req/s" if rate > 0 else ""
    rl_str = f" | ⚠ {stats['rate_limit_waits']} rate-limit waits" if stats["rate_limit_waits"] else ""
    print(f"  ✓ done in {elapsed:.1f}s | "
          f"{stats['real_calls']} API calls, {stats['cache_hits']} cached{rate_str}{rl_str}")


def process_meeting(client: Client, meeting_key: int, recompute: bool = False) -> None:
    """Ingest all sessions for a meeting, then refresh cumulative season stats."""
    meeting_info = openf1.get_meeting(meeting_key)
    year: int | None = None
    if meeting_info:
        m = meeting_info[0]
        name = m.get("meeting_name") or "Unknown"
        location = m.get("location") or m.get("circuit_short_name") or ""
        year = m.get("year") or None
        print(f"\n→ Meeting {meeting_key} — {name} · {location} · {year or ''}")
    else:
        print(f"\n→ Meeting {meeting_key}")

    sessions = openf1.get_sessions(meeting_key)
    allowed = {"Race", "Qualifying", "Sprint", "Sprint Qualifying", "Sprint Shootout"}
    eligible = [s for s in sessions if s.get("session_name") in allowed]
    if year is None and eligible:
        year = eligible[0].get("year")
    print(f"  {len(eligible)} session(s) to ingest: "
          f"{', '.join(s.get('session_name', '?') for s in eligible)}")

    for s in eligible:
        process_session(client, s["session_key"], recompute=recompute)

    if year:
        print(f"  computing season stats for {year} …")
        ingest_season_driver_stats(client, year)
        ingest_season_constructor_stats(client, year)

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
    group.add_argument("--season-stats", type=int, metavar="YEAR",
                        help="Recompute season driver/constructor stats for a year (no re-ingestion)")
    group.add_argument("--backfill-circuit-lengths", action="store_true",
                        help="Backfill circuit_length_km for all meetings already in the races table")
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
        process_session(client, args.session, recompute=args.recompute)
    elif args.meeting:
        process_meeting(client, args.meeting, recompute=args.recompute)
    elif args.year:
        process_year(client, args.year, recompute=args.recompute)
    elif args.season_stats:
        year = args.season_stats
        print(f"Recomputing season stats for {year}...")
        ingest_season_driver_stats(client, year)
        ingest_season_constructor_stats(client, year)
        print(f"✓ Season stats for {year} complete")
    elif args.backfill_circuit_lengths:
        backfill_circuit_lengths(client)


if __name__ == "__main__":
    main()
