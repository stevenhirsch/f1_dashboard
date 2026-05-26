#!/usr/bin/env python3
"""
List meetings for a season, highlighting the most recently completed race.

Usage:
  python latest_race.py            # current year, marks latest completed race
  python latest_race.py --year 2025
"""

import argparse
import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))
from api import openf1

SKIP = {"pre-season", "testing"}


def main() -> None:
    parser = argparse.ArgumentParser(description="List F1 meeting keys for a season")
    parser.add_argument("--year", type=int, default=datetime.now().year)
    args = parser.parse_args()

    meetings = openf1.get_meetings(args.year)
    eligible = [
        m for m in meetings
        if not any(kw in (m.get("meeting_name") or "").lower() for kw in SKIP)
    ]

    now = datetime.now(timezone.utc)
    latest_key = None
    for m in eligible:
        date_str = m.get("date_start") or ""
        if date_str and date_str <= now.isoformat():
            latest_key = m.get("meeting_key")

    print(f"\n  {'KEY':<8} {'ROUND':<6} {'NAME':<40} {'LOCATION'}")
    print(f"  {'-'*8} {'-'*6} {'-'*40} {'-'*20}")
    for i, m in enumerate(eligible, 1):
        key = m.get("meeting_key")
        name = m.get("meeting_name", "")
        loc = m.get("location", "")
        marker = " ◀ latest" if key == latest_key else ""
        print(f"  {key:<8} {i:<6} {name:<40} {loc}{marker}")

    if latest_key:
        print(f"\nTo ingest the latest race:")
        print(f"  pixi run -e pipeline python pipeline/ingest.py --meeting {latest_key}\n")


if __name__ == "__main__":
    main()
