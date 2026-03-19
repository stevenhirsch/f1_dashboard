# F1 Dashboard — Progress Log

## Current Status
**Phase 1 data ingestion and pipeline testing complete. Next: build the React Race Tab frontend (Phase 1 frontend) or proceed to Phase 2.**

---

## What's Been Built

### Infrastructure (Phase 0) — Complete
- `pipeline/api/openf1.py` — OpenF1 API client with wrappers for all 18 endpoints including `get_championship_drivers`, `get_championship_teams`, `get_intervals`, `get_overtakes`, `get_team_radio`, `get_starting_grid`, `get_car_data`, `get_location`
- `pipeline/ingest.py` — ingestion pipeline with `--session`, `--meeting`, `--year`, `--recompute` flags
- `supabase/schema.sql` — 15 tables with PKs and RLS policies
- `.github/workflows/ingest.yml` — manual workflow dispatch for post-race ingestion
- `.github/workflows/deploy.yml` — auto-deploy to GitHub Pages on push to main
- `dashboard/` — React + Vite scaffold with Supabase JS client and session selector hook
- `pixi.toml` — pipeline environment with `supabase` package (`websockets <16` pin required to resolve conda conflict); `test` task runs pytest

### Phase 1 Data Ingestion — Complete
All tables populated correctly for meeting 1280 (2026 Chinese GP).

**New tables added:**
- `intervals` — gap_to_leader + interval per driver per timestamp, with `laps_down` integer for lapped drivers (non-numeric strings like "+1 LAP" parsed out)
- `starting_grid` — grid positions + qualifying `lap_duration` per driver, keyed to the qualifying session (not race session)
- `championship_drivers` — driver championship points/position before and after each race (Beta endpoint, race/sprint sessions only)
- `championship_teams` — constructor championship points/position before and after each race (Beta endpoint, race/sprint sessions only)

**New columns added to existing tables:**
- `pit_stops.lane_duration`
- `race_results.number_of_laps`
- `race_control.qualifying_phase` — PK corrected from `(session_key, date)` to `(session_key, date, category, message)` to handle multiple messages at the same timestamp
- `overtakes.date` — PK updated to `(session_key, date, driver_number_overtaking, driver_number_overtaken)` since `lap_number` is not in the API response
- `races.circuit_type` — "Permanent", "Temporary - Street", or "Temporary - Road"
- `starting_grid.lap_duration` — qualifying lap time that earned the grid position

**Pipeline improvements:**
- 404 responses return empty list instead of raising — handles endpoints with no data for a given session type
- Network retry logic in `_upsert` — catches `httpx.ReadError` / `httpx.ConnectError`, retries up to 3 times with exponential backoff
- Sprint races correctly handled — `session_type == "sprint"` added alongside `"race"` for results, overtakes, intervals, and championship standings
- Starting grid ingested during qualifying sessions (not race sessions — that's where OpenF1 keys the data)
- Overtakes field names corrected: API returns `overtaking_driver_number` / `overtaken_driver_number`
- Enriched logging: meeting name/city/year header, per-session name and date, upfront list of sessions to ingest, end-of-session summary with elapsed time, API call count, cache hit count, rolling req/s rate, and rate-limit wait count

### Pipeline Testing — Complete (2026-03-19)
119 tests across two files. Run with `pixi run -e pipeline test`. All use `unittest.mock` — no live API or database calls.

**`pipeline/tests/test_openf1.py`** — 54 tests:
- `_get` cache lifecycle, 404 handling, 500/503 raising immediately, 429 exponential backoff, retry exhaustion
- Dict and list-of-tuple param formats
- Full stats tracking: `real_calls`, `cache_hits`, `rate_limit_waits`, `call_times`, `current_rate` rolling window, `get_stats` independence
- `clear_cache` behaviour
- API wrapper spot-checks: correct endpoint and params for 8 wrappers including range-operator endpoints (`get_car_data`, `get_location`)

**`pipeline/tests/test_ingest.py`** — 65 tests:
- `_parse_gap`: 13 cases covering all gap value formats including lapped driver strings
- `_pick`: key selection edge cases
- `_upsert`: chunking at 500 rows, `ReadError`/`ConnectError` retry with exponential backoff, failure after 4 attempts
- All ingest functions: field mapping, filter conditions, empty API response behaviour
- `ingest_race_control`: same-timestamp test verifies PK fix — two different messages survive
- `ingest_race_results`: pit count aggregation, gap stringification, dnf/dns/dsq defaulting
- `ingest_qualifying_results`: fastest lap selection, invalid duration exclusion
- `ingest_intervals`: lapped driver handling, leader null gap
- `process_session`: all 5 session types route correctly; early return on missing session
- `process_meeting`: session filtering and practice session exclusion

**CI:** `.github/workflows/test.yml` runs the suite on every push and pull request. Verified locally with `act push --job test`.

### Data in Supabase
- 2026 Chinese GP full weekend ingested (meeting 1280):
  - Session 11236 — Sprint Qualifying [2026-03-13]
  - Session 11240 — Sprint [2026-03-14]
  - Session 11241 — Qualifying [2026-03-14]
  - Session 11245 — Race [2026-03-15]

---

## Known Issues / Decisions Made
- `race_results.status_detail` is always null — DNF/DSQ reasons need to be parsed from `race_control` messages; deferred to Phase 1 frontend work
- `race_results.fastest_lap_flag` is always null — not yet sourced from the API
- `qualifying_results` Q1/Q2/Q3 splits are not yet computed — deferred to Phase 2
- `lap_metrics` table exists in schema but is empty — populated in Phase 5
- `--recompute` flag is wired up but no-ops until Phase 5
- `overtakes` table is empty for session 11245 (Race) — OpenF1 may not have this data populated yet for the 2026 Chinese GP race session; sprint overtakes (11240) are present

---

## Where to Pick Up Next

### Option A — Phase 1 React Race Tab (frontend)
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

### Option B — Phase 2 Qualifying Tab
Data ingestion work required first: compute Q1/Q2/Q3 splits in `qualifying_results` using `qualifying_phase` from `race_control`, then add compound per qualifying phase from `stints`.

---

## Useful Reference
- OpenF1 API: `https://api.openf1.org/v1`
- Supabase dashboard: https://supabase.com/dashboard
- Live site: https://stevenhirsch.github.io/f1_dashboard/
- GitHub repo: https://github.com/stevenhirsch/f1_dashboard
