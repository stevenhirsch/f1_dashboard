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

from supabase import create_client, Client

sys.path.insert(0, os.path.dirname(__file__))
from api import openf1

# ---------------------------------------------------------------------------
# Supabase helpers
# ---------------------------------------------------------------------------

_CHUNK_SIZE = 500


def _upsert(client: Client, table: str, rows: list[dict]) -> None:
    """Upsert rows into a Supabase table in chunks."""
    if not rows:
        return
    for i in range(0, len(rows), _CHUNK_SIZE):
        chunk = rows[i : i + _CHUNK_SIZE]
        client.table(table).upsert(chunk).execute()
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
            "scope":         r.get("scope"),
            "sector":        r.get("sector"),
        }
        for r in rc
        if r.get("session_key") and r.get("date")
    ]
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


def ingest_qualifying_results(client: Client, session_key: int, laps: list[dict]) -> None:
    """
    Upsert qualifying best times into `qualifying_results`.

    For Phase 0 this stores the overall best lap per driver from the session.
    Q1/Q2/Q3 splits are computed in Phase 2 once sub-session detection is added.
    """
    best: dict[int, float] = {}
    best_lap: dict[int, dict] = {}
    for l in laps:
        dn = l.get("driver_number")
        dur = l.get("lap_duration")
        if dn is None or not dur:
            continue
        try:
            t = float(dur)
        except (TypeError, ValueError):
            continue
        if t <= 0:
            continue
        if dn not in best or t < best[dn]:
            best[dn] = t
            best_lap[dn] = l

    rows = [
        {
            "session_key":  session_key,
            "driver_number": dn,
            "best_lap_time": best[dn],
            "best_lap_number": best_lap[dn].get("lap_number"),
            # Q1/Q2/Q3 splits — Phase 2
            "q1_time": None,
            "q2_time": None,
            "q3_time": None,
        }
        for dn in best
    ]
    _upsert(client, "qualifying_results", rows)


def ingest_overtakes(client: Client, session_key: int) -> None:
    """Upsert overtake events into `overtakes`."""
    overtakes = openf1.get_overtakes(session_key)
    rows = [
        {
            "session_key":              o.get("session_key"),
            "lap_number":               o.get("lap_number"),
            "driver_number_overtaking": o.get("driver_number_overtaking"),
            "driver_number_overtaken":  o.get("driver_number_overtaken"),
            "position":                 o.get("position"),
        }
        for o in overtakes
        if o.get("session_key")
    ]
    _upsert(client, "overtakes", rows)


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
    print(f"\n→ Processing session {session_key}")

    # Fetch session metadata first to get meeting_key, but don't upsert yet —
    # sessions has a FK to races(meeting_key), so the meeting must exist first.
    sessions_raw = openf1.get_session(session_key)
    if not sessions_raw:
        print(f"  [warn] no session found for session_key={session_key}")
        return
    meeting_key = sessions_raw[0].get("meeting_key")
    if meeting_key:
        ingest_meeting(client, meeting_key)

    # Now safe to upsert session (meeting row exists)
    session = ingest_session(client, session_key)
    if session is None:
        return

    session_type = (session.get("session_type") or "").lower()

    ingest_drivers(client, session_key)
    laps = ingest_laps(client, session_key)
    ingest_stints(client, session_key)
    pit = ingest_pit_stops(client, session_key)
    ingest_weather(client, session_key)
    ingest_race_control(client, session_key)

    if session_type == "race":
        ingest_race_results(client, session_key, pit)
    elif session_type in ("qualifying", "sprint qualifying", "sprint shootout"):
        ingest_qualifying_results(client, session_key, laps)

    ingest_overtakes(client, session_key)

    if recompute:
        recompute_lap_metrics(client, session_key)

    print(f"✓ Session {session_key} done")


def process_meeting(client: Client, meeting_key: int, recompute: bool = False) -> None:
    """Ingest all sessions for a meeting."""
    print(f"\n→ Processing meeting {meeting_key}")
    sessions = openf1.get_sessions(meeting_key)
    allowed = {"Race", "Qualifying", "Sprint", "Sprint Qualifying", "Sprint Shootout"}
    for s in sessions:
        if s.get("session_name") in allowed:
            process_session(client, s["session_key"], recompute=recompute)


def process_year(client: Client, year: int, recompute: bool = False) -> None:
    """Ingest all race weekends for a year."""
    print(f"\n→ Processing year {year}")
    meetings = openf1.get_meetings(year)
    for m in meetings:
        name = (m.get("meeting_name") or "").lower()
        if "pre-season" in name or "testing" in name:
            continue
        process_meeting(client, m["meeting_key"], recompute=recompute)
        time.sleep(1)  # brief pause between meetings


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
