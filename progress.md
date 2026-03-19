# F1 Dashboard — Progress Log

## Current Status
**Phase 1 data ingestion complete. Next: build the React Race Tab frontend.**

---

## What's Been Built

### Infrastructure (Phase 0) — Complete
- `pipeline/api/openf1.py` — OpenF1 API client with `get_intervals`, `get_overtakes`, `get_team_radio`, `get_starting_grid`
- `pipeline/ingest.py` — ingestion pipeline with `--session`, `--meeting`, `--year`, `--recompute` flags
- `supabase/schema.sql` — 13 tables with PKs and RLS policies
- `.github/workflows/ingest.yml` — manual workflow dispatch for post-race ingestion
- `.github/workflows/deploy.yml` — auto-deploy to GitHub Pages on push to main
- `dashboard/` — React + Vite scaffold with Supabase JS client and session selector hook
- `pixi.toml` — pipeline environment with `supabase` package (`websockets <16` pin required to resolve conda conflict)

### Phase 1 Data Ingestion — Complete
All tables added and populating correctly for meeting 1280 (2026 Chinese GP):

**New tables added this phase:**
- `intervals` — gap_to_leader + interval per driver per timestamp, with `laps_down` integer for lapped drivers (non-numeric strings like "+1 LAP" parsed out)
- `starting_grid` — grid positions per driver, keyed to the qualifying session (not race session)

**New columns added to existing tables:**
- `pit_stops.lane_duration`
- `race_results.number_of_laps`
- `race_control.qualifying_phase`
- `overtakes.date`

**Pipeline improvements:**
- 404 responses now return empty list instead of raising — handles endpoints with no data for a session type (e.g. overtakes/starting_grid for non-race sessions)
- Network retry logic in `_upsert` — catches `httpx.ReadError` / `httpx.ConnectError`, retries up to 3 times with exponential backoff
- Sprint races correctly handled — `session_type == "sprint"` added alongside `"race"` for results, overtakes, and intervals
- Starting grid ingested during qualifying sessions (not race sessions — that's where OpenF1 keys the data)
- Overtakes field names corrected: API returns `overtaking_driver_number` / `overtaken_driver_number`, not the reversed form
- Overtakes PK changed to `(session_key, date, driver_number_overtaking, driver_number_overtaken)` — `lap_number` is not returned by the API

### Data in Supabase
- 2026 Chinese GP full weekend ingested (meeting 1280):
  - Session 11236 — Qualifying
  - Session 11240 — Sprint Race
  - Session 11241 — Sprint Qualifying
  - Session 11245 — Race

---

## Known Issues / Decisions Made
- `race_control` PK is `(session_key, date, category, message)` — OpenF1 can emit multiple messages at the same timestamp
- `qualifying_results` Q1/Q2/Q3 splits are not yet computed — deferred to Phase 2
- `lap_metrics` table exists in schema but is empty — populated in Phase 5
- `--recompute` flag is wired up but no-ops until Phase 5
- `race_results.status_detail` is always null — DNF/DSQ reasons need to be parsed from `race_control` messages; deferred to Phase 1 frontend work
- `race_results.fastest_lap_flag` is always null — not yet sourced from the API
- `overtakes` table is empty for session 11245 (Race) — OpenF1 may not have this data populated yet for the 2026 Chinese GP race session; sprint overtakes (11240) are present

---

## Where to Pick Up Next

### Phase 1 — Race Tab frontend
Create `dashboard/src/pages/RacePage.jsx` and wire into `App.jsx`. All data is in Supabase and ready.

**Build order:**
1. **Race Results Table** — query `race_results` joined with `drivers`, display position, driver, team, laps, time/gap, DNF/DSQ, pit count. Hook pattern: `useRaceResults(sessionKey)`.
2. **Tyre Strategy Plot** — query `stints`, render per-driver stint bars ordered by finish position with SC/VSC overlay from `race_control`.
3. **Gap Evolution Chart** — query `intervals`, plot gap to leader over time with SC/VSC overlay. Note: `gap_to_leader` is numeric seconds; `laps_down` column flags lapped drivers.
4. **Weather Strip** — query `weather`, show track/air temp and rainfall as a shared timeline.

**Also needed before Phase 1 is fully done:**
- Parse `status_detail` (DNF/DSQ reason) from `race_control` messages into `race_results` at ingest time
- Source `fastest_lap_flag` from the API

**Starting point:** `dashboard/src/pages/RacePage.jsx` (file does not exist yet — create it), wire it into `App.jsx` replacing the placeholder. The `useSessionSelector` hook is already in place — `selectedSessionKey` is available to pass to data-fetching hooks.

---

## Useful Reference
- OpenF1 API: `https://api.openf1.org/v1`
- Supabase dashboard: https://supabase.com/dashboard
- Live site: https://stevenhirsch.github.io/f1_dashboard/
- GitHub repo: https://github.com/stevenhirsch/f1_dashboard
