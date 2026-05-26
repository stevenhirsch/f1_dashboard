# F1 Dashboard — Ingestion Pipeline

Fetches data from the [OpenF1 API](https://openf1.org) and upserts it into Supabase.

## Prerequisites

Environment variables must be set (`.env` or shell):

```
SUPABASE_URL=<your project URL>
SUPABASE_SERVICE_ROLE_KEY=<service role key — never the anon key>
```

All commands below are run from the repo root via `pixi run`.

---

## Common commands

### Ingest the most recent race
Use the helper script to look up the meeting key, then ingest:

```bash
# Print meeting key + name for every round this season
pixi run python pipeline/latest_race.py

# Ingest a specific meeting (all sessions: FP1/2/3, Quali, Race)
pixi run python pipeline/ingest.py --meeting <MEETING_KEY>
```

### Ingest a full season
```bash
pixi run python pipeline/ingest.py --year 2026
```

### Ingest a single session
```bash
pixi run python pipeline/ingest.py --session <SESSION_KEY>
```

### Recompute derived metrics (without re-fetching raw data)
Useful after a pipeline logic change:
```bash
pixi run python pipeline/ingest.py --session <SESSION_KEY> --recompute
```

### Recompute season standings only
```bash
pixi run python pipeline/ingest.py --season-stats 2026
```

### Backfill circuit lengths
```bash
pixi run python pipeline/ingest.py --backfill-circuit-lengths
```

---

## Finding IDs

### List all meetings for a year
```bash
pixi run python pipeline/latest_race.py --year 2025
```

### List all sessions in a meeting
```bash
pixi run python -c "
from pipeline.api import openf1
for s in openf1.get_sessions(<MEETING_KEY>):
    print(s['session_key'], s['session_type'], s['session_name'])
"
```

---

## Notes

- Upserts are **idempotent** — re-running the same command is safe and will pick up any data OpenF1 has added since the last run.
- OpenF1 sometimes takes 24–48 hours to finalize race data. If a session looks incomplete, just re-run it the next day.
- Season driver/constructor stats are recomputed automatically at the end of each `--meeting` or `--year` run.
