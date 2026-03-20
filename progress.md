# F1 Dashboard — Progress Log

## Current Status
**Phase 1 complete including UI/UX polish pass. Next: Phase 2 Qualifying Tab (requires Q1/Q2/Q3 split computation in pipeline first).**

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

### Phase 1 Frontend — Complete (2026-03-19)
### UI/UX Polish — Complete (2026-03-20)

**UI/UX polish changes (2026-03-20):**
- Navigation restructured: top-level Dashboard / Chat mode switcher (Chat is a future page, placeholder in place); session selector replaced with Year + Weekend + Event dropdowns (Event only shown on sprint weekends, defaults to Grand Prix); tabs are now Race / Qualifying / Driver scoped to the selected event — Race and Driver tabs share `raceSessionKey`, Qualifying tab uses `qualifyingSessionKey`
- `useSessionSelector` refactored: no longer exposes raw `selectedSessionKey`; derives `raceSessionKey` and `qualifyingSessionKey` from `selectedEvent`; `isSprintWeekend` and `events` array exposed for conditional Event dropdown rendering; URL params updated from `session` to `weekend` + `event`
- Weather tile: summary stats row added above chart — Avg Air Temp (blue), Avg Track Temp (red), Total Rainfall
- Weather tile: timestamps converted from UTC to local race timezone using `races.gmt_offset`; x-axis label shows e.g. `Time (UTC+08:00)`; `gmt_offset text` column added to `races` table and populated by `ingest_meeting()`; regex handles OpenF1's `"HH:MM:SS"` format (no leading `+`)
- Tyre strategy plot: plot title removed (section heading is sufficient); legend moved from right sidebar to top (`orientation: 'h'`, `y: 1.08`); right margin reduced from 160 → 20; x-axis label formalised as `"Lap Number"`
- F1 logo moved to top-left of header; app title ("Strategy Dashboard") removed — logo stands alone as brand mark
- Footer added: GitHub icon + repo link (`stevenhirsch/f1_dashboard`), dynamic copyright year (`new Date().getFullYear()`), OpenF1 API attribution with link to `openf1.org`; legal disclaimer kept in README only
- `dashboard/public/github.svg` added — GitHub mark SVG, rendered with `filter: invert(0.6)` to match muted theme color

**New files created:**
- `dashboard/src/hooks/useRaceResults.js` — fetches `race_results` + `drivers` separately and merges (no Supabase FK join — `drivers` has composite PK `(session_key, driver_number)` with no FK from `race_results`)
- `dashboard/src/hooks/useStints.js` — fetches stints + race_results + drivers; returns stints indexed by driver_number and a driverOrder array sorted by finish position
- `dashboard/src/hooks/useIntervals.js` — fetches intervals + race_results + drivers; enriches each row with name_acronym, team_colour, position
- `dashboard/src/hooks/useRaceControl.js` — fetches race_control; parses SC/VSC periods using `category == "SafetyCar"` (not the `flag` field) and retirement messages
- `dashboard/src/hooks/useWeather.js` — fetches weather ordered by date
- `dashboard/src/plots/TyreStrategyPlot.jsx` — horizontal Plotly bar chart; one trace per (compound, freshness) pair; fresh/used determined by `tyre_age_at_start`; SC/VSC shading + lap annotations; handles missing `lap_end` via next-stint inference and session-max fallback; gray placeholder bars for stints with no known first stint
- `dashboard/src/plots/WeatherStrip.jsx` — dual-axis Plotly chart; track/air temp (left) + rainfall bars (right); both axes start at 0
- `dashboard/src/pages/RacePage.jsx` — Race Results table + Tyre Strategy + Weather sections; dark theme
- `dashboard/src/index.css` — global CSS reset (margin/padding 0, dark background on html/body/#root)
- `dashboard/public/F1.png` — F1 logo for header

**Key design decisions:**
- Supabase `select('*, drivers(...)')` join syntax does NOT work for `drivers` because it has a composite PK `(session_key, driver_number)` with no matching FK in `race_results`. All hooks fetch both tables separately and merge in JS.
- SC/VSC detection uses `row.category === 'SafetyCar'` (OpenF1 field), not `row.flag`
- Gap Evolution chart removed — interval data has no reliable lap-number mapping and timestamp-based x-axis was uninformative; may revisit in Phase 3
- `tyre_age_at_start` solely determines fresh vs. used (dropped compound-first-seen heuristic which caused contradictions)

**Dark theme (applied globally):**
- Background: `#09090b`; surface cards: `#18181b`; borders: `rgba(255,255,255,0.08)`
- Text: `#fafafa`; muted labels: `#a1a1aa`; accent: `#e10600` (F1 red)
- All Plotly charts use matching dark `paper_bgcolor`/`plot_bgcolor`

### Data in Supabase
- 2026 Chinese GP full weekend ingested (meeting 1280):
  - Session 11236 — Sprint Qualifying [2026-03-13]
  - Session 11240 — Sprint [2026-03-14]
  - Session 11241 — Qualifying [2026-03-14]
  - Session 11245 — Race [2026-03-15]

---

## Known Issues / Decisions Made
- `race_results.status_detail` is always null — DNF/DSQ reasons need to be parsed from `race_control` messages; deferred
- `race_results.fastest_lap_flag` is always null — not yet sourced from the API
- `qualifying_results` Q1/Q2/Q3 splits are not yet computed — deferred to Phase 2
- `lap_metrics` table exists in schema but is empty — populated in Phase 5
- `--recompute` flag is wired up but no-ops until Phase 5
- `overtakes` table is empty for session 11245 (Race) — OpenF1 may not have this data populated yet for the 2026 Chinese GP race session; sprint overtakes (11240) are present
- **OpenF1 sprint stints data gap (session 11240):** The OpenF1 `/stints` endpoint only returns `stint_number=2` for drivers who pitted during the sprint (all pitted around lap 13 under SC). Their first stints (laps 1–13) are completely absent from the OpenF1 API — confirmed by direct API query. The dashboard renders a gray "Unknown compound" placeholder bar for the missing laps. This may be a permanent OpenF1 data gap or a delayed population issue. Candidate for a GitHub issue against OpenF1. Affected drivers: 1, 3, 6, 11, 12, 16, 18, 23, 43, 44, 63, 81 (session 11240).

---

## Where to Pick Up Next

### Phase 2 — Qualifying Tab

**Step 1 — Pipeline work first:**
Compute Q1/Q2/Q3 splits in `qualifying_results`. The `qualifying_phase` column in `race_control` identifies session boundaries (e.g. "Q1", "Q2", "Q3"). Cross-reference lap timestamps from `laps.date_start` against `race_control` session-start/end events to assign each lap to a qualifying phase, then pick the best lap per driver per phase.

**Step 2 — Frontend:**
- Qualifying Results Table — Pos, Driver, Team, Q1/Q2/Q3 best times, delta to pole, compound per phase
- Sector delta heatmap — all drivers × S1/S2/S3, coloured by delta to pole
- Track evolution chart — lap time vs. lap number scatter per Q phase

### Phase 3 — Driver Tab

Add `position` and `team_radio` tables to schema and pipeline, then build the Driver tab UI.

### Option — Backfill Data (any time)

Run `python pipeline/ingest.py --year 2025` (and 2023, 2024) to populate historical race weekends. Required before the year-on-year comparison table in the Race tab can be built.
