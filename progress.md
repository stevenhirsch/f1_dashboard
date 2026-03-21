# F1 Dashboard — Progress Log

## Current Status
**Phase 3 Driver Tab complete. Next: Phase 4 polish (mobile layout, loading/empty states, backfill ingestion).**

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

### Phase 2 — Qualifying Tab — Complete (2026-03-21)

**Pipeline changes:**
- Added `_normalize_phase(v)` — maps OpenF1 integer qualifying_phase values (1/2/3) and strings ("Q1"/"Q2"/"Q3") to canonical strings. Critical fix: OpenF1 returns integers, not strings, so old ingestion left q1/q2/q3 times all null.
- Added `_assign_qualifying_phases(laps, race_control)` — injects `_phase` field into each lap using sorted race_control events with lexicographic ISO 8601 date comparison.
- Added `_get_compound_for_lap(driver_number, lap_number, stints)` — returns compound uppercased, "UNKNOWN" for null compound, None if no matching stint.
- Rewrote `ingest_qualifying_results` — computes per-phase best times, compounds, and lap counts; upserts 6 new columns (`q1_compound`, `q2_compound`, `q3_compound`, `q1_laps`, `q2_laps`, `q3_laps`).
- `process_session` now captures `stints_rows` and `rc_rows` return values and passes them to `ingest_qualifying_results`.
- Schema: added 6 columns to `qualifying_results` with `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`.

**Pipeline testing additions (total now 144 tests):**
- `TestAssignQualifyingPhases` — 10 tests including integer normalization, boundary conditions, unsorted RC rows, null qualifying_phase ignored
- `TestGetCompoundForLap` — 6 tests including open-ended stints, uppercasing, null compound → "UNKNOWN"
- 8 new tests in `TestIngestQualifyingResults` — per-phase times, compounds, lap counts, unphased lap exclusion, empty stints fallback
- `TestProcessSessionQualifying` — verifies `ingest_qualifying_results` receives `rc_rows` and `stints_rows` as args 4 and 5

**Frontend — new files:**
- `dashboard/src/utils/compounds.js` — extracted `COMPOUND_COLOURS`, `COMPOUND_ORDER`, `compoundColour()` from `TyreStrategyPlot.jsx` for sharing
- `dashboard/src/utils/qualifying.js` — `normalizePhase`, `assignPhases`, `formatQualTime`, `formatDelta`, `computeSectorDeltas`, `computePhaseStints`
- `dashboard/src/hooks/useQualifyingData.js` — parallel fetch of `qualifying_results`, `laps`, `race_control`, `drivers`, `stints`, `weather`; results ordered by `starting_grid.position`
- `dashboard/src/pages/QualifyingPage.jsx` — Qualifying Results table + Phase Analysis tabs
- `dashboard/src/plots/SectorDeltaHeatmap.jsx` — Plotly heatmap, rows = drivers, columns = S1/S2/S3, colour scale green→red

**Qualifying Results table:**
- Ordered by `starting_grid.position` (official qualifying classification) — avoids raw `best_lap_time` sort which incorrectly placed Q1 times above Q2/Q3 drivers
- Columns: Pos, Driver (team-coloured), Team, Q1, Q2, Q3 (times only), Laps
- Dynamic elimination separators ("eliminated after Q1" / "eliminated after Q2") using `resolvePhase()` which prefers DB q-times, falls back to client-side `assignPhases` from laps

**Phase Analysis tabs (Q1 / Q2 / Q3):**
- Tabs default to Q1 (most inclusive), progressing right to most selective
- Per-driver stint table: Pos, Driver, Team, Tyres (all compound badges + F/U freshness per run), Best, Gap
- Elimination separators: drivers who advanced to the next phase shown first, then eliminated drivers, with "eliminated after Q1/Q2" divider
- Out/in-lap filter: laps > 180s excluded from best-time computation (note: may need revisiting for Monaco)
- `best_time` tracked independently of stint matching — if OpenF1 stint data is missing for a phase, drivers still show a time
- Phase-filtered weather strip: weather rows filtered to the active phase's time window (start of phase → start of next); falls back to full session weather if filter yields nothing
- Phase-filtered sector delta heatmap: each tab shows sector deltas within that phase only

**Race tab enhancement:**
- `useRaceResults` now accepts `qualifyingSessionKey`, fetches `starting_grid` in parallel, merges `grid_position` onto each result row
- `PositionChange` component: ▲N green (positions gained), ▼N red (positions lost), — (same), blank for DNF/DNS/DSQ
- Indicator rendered inline in the Pos cell for compact layout

### Pipeline Testing — Complete (2026-03-19)
144 tests across two files. Run with `pixi run -e pipeline test`. All use `unittest.mock` — no live API or database calls.

**`pipeline/tests/test_openf1.py`** — 54 tests:
- `_get` cache lifecycle, 404 handling, 500/503 raising immediately, 429 exponential backoff, retry exhaustion
- Dict and list-of-tuple param formats
- Full stats tracking: `real_calls`, `cache_hits`, `rate_limit_waits`, `call_times`, `current_rate` rolling window, `get_stats` independence
- `clear_cache` behaviour
- API wrapper spot-checks: correct endpoint and params for 8 wrappers including range-operator endpoints (`get_car_data`, `get_location`)

**`pipeline/tests/test_ingest.py`** — originally 65 tests, now 119 after Phase 2 additions:
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
- `lap_metrics` table exists in schema but is empty — populated in Phase 5
- `--recompute` flag is wired up but no-ops until Phase 5
- `overtakes` table is empty for session 11245 (Race) — OpenF1 may not have this data populated yet for the 2026 Chinese GP race session; sprint overtakes (11240) are present
- **OpenF1 sprint stints data gap (session 11240):** The OpenF1 `/stints` endpoint only returns `stint_number=2` for drivers who pitted during the sprint (all pitted around lap 13 under SC). Their first stints (laps 1–13) are completely absent from the OpenF1 API — confirmed by direct API query. The dashboard renders a gray "Unknown compound" placeholder bar for the missing laps. This may be a permanent OpenF1 data gap or a delayed population issue. Candidate for a GitHub issue against OpenF1. Affected drivers: 1, 3, 6, 11, 12, 16, 18, 23, 43, 44, 63, 81 (session 11240).
- **OpenF1 qualifying stint data incomplete:** Many drivers show no stint match for Q1/Q2 laps even though they participated. `computePhaseStints` falls back to `overallBest` (best lap duration regardless of stint match) so times always display; compound badges just show empty for unmatched phases.
- **Out-lap filter for qualifying (Monaco caveat):** Laps > 180s are excluded as out/in-laps in `computePhaseStints` and `computeSectorDeltas`. This threshold works for all current circuits but may incorrectly exclude valid flying laps at Monaco or other slow street circuits. Revisit when Monaco data is added.
- **OpenF1 `qualifying_phase` integers:** OpenF1 returns `qualifying_phase` as integers (1/2/3), not strings. Both pipeline (`_normalize_phase`) and frontend (`normalizePhase` in `qualifying.js`) handle this. Re-ingestion required after this fix was applied to populate q1/q2/q3 times.

---

### Phase 3 — Driver Tab — Complete (2026-03-21)

**New files:**
- `dashboard/src/api/openf1.js` — direct browser fetch of OpenF1 `/car_data` and `/location` endpoints (no URL-encoding of `>=`/`<=` operators)
- `dashboard/src/utils/telemetry.js` — `computeTelemetry()` (speed integration → cumulative distance, nearest-timestamp location merge), `coastingIntervals()` (throttle < 1% AND brake = 0, min 20m filter)
- `dashboard/src/hooks/useLapTelemetry.js` — `useRef`-based cache keyed by `${sessionKey}-${driverNumber}-${lapNum}`; fetches only uncached laps; cleans up deselected laps from state
- `dashboard/src/plots/TrackMapPlot.jsx` — 2D track map; single lap = speed-gradient markers (red→yellow→green); multi-lap = one coloured line per lap; `scaleanchor` preserves aspect ratio
- `dashboard/src/plots/TelemetryChart.jsx` — 8-panel subplot chart sharing a single distance x-axis: Speed, Power (%), Brake, Gear, Lift & Coast, Thr/Brk overlap, RPM, DRS; binary panels use filled step traces
- `dashboard/src/plots/CoastingChart.jsx` — Gantt-style coasting interval chart (created, superseded by TelemetryChart panel)
- `dashboard/src/components/InfoTooltip.jsx` — hoverable/tappable `?` circle; `placement` prop (`'top'`/`'bottom'`), `width` prop; closes on outside click

**Driver tab features:**
- Race sub-tab: finish position, best lap, pit summary, overtakes made/suffered, FL Speed (st_speed on fastest lap), Top Speed (session max incl. tow); lap-by-lap table with compound badges, sector splits, trap speed; position chart; gap-to-leader chart; lap times bar chart (S1/S2/S3 stacked)
- Qualifying sub-tab: Q1/Q2/Q3 times, FL Speed, Top Speed; same stacked bar lap times chart with Q1/Q2/Q3 boundary lines; lap-by-lap table with phase, delta-to-best, compound
- Lap detail panel: click any row to load telemetry; multi-select overlays laps in colour; track map (single lap only, hidden for multi-lap); 8-panel TelemetryChart at 740px (820px multi-lap)
- InfoTooltip wired into Lap Detail panel (explains Lift & Coast and trail-braking) and Tyre Strategy heading (explains solid vs. scrubbed tyres)
- `key={selectedDriverNumber}` on sub-tabs resets lap selection on driver change

**Race/Qualifying tab enhancements:**
- Top speed columns added to Race Results table and Qualifying phase analysis and overall qualifying table; gold highlight for session maximum
- Overtakes columns added to Race Results table
- Qualifying lap times chart replaced with same stacked sector bar chart as race; Q1/Q2/Q3 boundaries shown as neutral grey dotted lines
- Unknown compound bars: `?` badge instead of `U` in tables; hatched grey placeholder bars in compound strip for lap ranges with no stint data (fixes sprint race and qualifying Q1 gaps)
- Tyre strategy InfoTooltip condensed to two sentences

**Key decisions:**
- 3D track map removed — no good angle to show elevation without confusion
- Coasting integrated as TelemetryChart panel 7, not a separate chart — keeps x-axis alignment
- Throttle/brake split into separate panels (throttle = continuous %, brake = binary filled step)
- `useRef` cache in `useLapTelemetry` ensures re-selecting a lap is instant (no re-fetch)
- Sprint race missing stint data → hatched `?` placeholder bars via gap-fill logic after stint loop

---

## Where to Pick Up Next

### Phase 4 — Polish and Public Release

- Mobile responsive layout pass
- Loading states and empty state handling throughout
- Performance audit (lazy loading, query optimisation)
- Backfill ingestion: `python pipeline/ingest.py --year 2025` (and 2023, 2024) — required for year-on-year comparison table
