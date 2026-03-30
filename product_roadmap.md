# F1 Strategy Dashboard — Product Roadmap & Technical Reference

---

## Project Overview

A post-race Formula 1 analytics platform built as a static React web app. The Dashboard mode displays race results, qualifying analysis, and deep driver telemetry metrics. A future Chat mode will allow natural language queries across any race, season, or driver. Data is ingested from the OpenF1 API after each race weekend and stored in Supabase. The Marimo notebook is a parallel experimentation environment only and is not part of the production path.

---

## Technology Stack

| Layer | Tool | Notes |
|---|---|---|
| Experimentation | Marimo + Python | Local only, never deployed |
| Data ingestion | Python scripts | Run via GitHub Actions post-race |
| Database | Supabase (Postgres) | Free tier, anon key read-only via RLS |
| Frontend | React + Vite | Queries Supabase JS client directly |
| Charts | Plotly.js | Interactive, browser-native |
| Hosting | GitHub Pages | Static build deployed via GitHub Actions |
| CI/CD | GitHub Actions | Two workflows: ingest and deploy |
| Environment Management | Pixi | Conda & PyPI packages |

---

## Repository Structure

```
/
├── pipeline/
│   ├── api/               # OpenF1 API wrappers (ported from Marimo project)
│   ├── compute/           # Derived metric calculations
│   ├── tests/             # pytest unit and integration tests for the pipeline
│   └── ingest.py          # Main entry point, supports --recompute flag
├── dashboard/
│   ├── src/
│   │   ├── components/    # Reusable UI components
│   │   ├── pages/         # Race, Qualifying, Driver tabs
│   │   ├── hooks/         # Supabase data fetching hooks
│   │   └── plots/         # Plotly chart components
│   └── vite.config.js
└── .github/
    └── workflows/
        ├── ingest.yml     # Post-race pipeline, manual trigger
        ├── deploy.yml     # Builds React app, publishes to GitHub Pages
        └── test.yml       # Runs pipeline test suite on every push
```

---

## GitHub Actions Workflows

**`ingest.yml`** — triggered manually after each race weekend. Supabase service role key stored as a GitHub secret, never exposed to the browser. Fetches from OpenF1, computes metrics, writes to Supabase via upsert.

**`deploy.yml`** — triggered on push to main. Builds the React app with Vite, pushes `/dist` to the `gh-pages` branch.

---

## Data Architecture

### Supabase Schema

```
races                — meeting metadata, circuit, year, circuit_type, circuit_length_km
sessions             — session key, type (Race/Qualifying/Sprint), date
drivers              — driver number, name, acronym, team, per session
laps                 — core OpenF1 lap data (times, sector splits, compounds, pit flags)
lap_metrics          — derived values keyed on (session_key, driver_number, lap_number)
                       includes computed_at timestamp; see Phase 5 for full column spec
stints               — compound, lap start/end, tyre age per driver
pit_stops            — lane duration, stop duration (2024 US GP+), lap number
weather              — track temp, air temp, humidity, rainfall, wind_speed, wind_direction,
                       pressure per session
race_results         — position, gap, DNF/DSQ with reason, pit count, fastest lap flag
                       (fastest_lap_flag derived from minimum lap_duration among classified finishers)
                       + mean_peak_accel_g, mean_peak_accel_g_clean, mean_peak_decel_g_abs,
                         mean_peak_decel_g_abs_clean — per-driver race averages from lap_metrics
                         (_clean variants exclude laps exceeding plausibility bounds: accel ≤ 4 g, decel ≤ 8 g)
qualifying_results   — Q1/Q2/Q3 best times, compounds, lap counts, delta to pole
overtakes            — overtaking/overtaken driver, position, lap number, date

-- Added in Phase 1 --
intervals            — gap_to_leader and interval per driver per timestamp (race/sprint only)
starting_grid        — grid position + qualifying lap_duration per driver per session
championship_drivers — driver championship points/position before and after each race (Beta)
                       + points_gap_to_leader, points_gap_to_p2
championship_teams   — constructor championship points/position before and after each race (Beta)
                       + points_gap_to_leader, points_gap_to_p2

-- Added in Phase 3 --
position             — driver position over time throughout session
team_radio           — radio recording URLs per driver per timestamp

-- Added in Phase 5 --
session_sector_bests — best S1/S2/S3 time + driver per session; theoretical best lap
                       one row per session; enables fast chatbot queries without full lap scan
stint_metrics        — per (session_key, driver_number, stint_number)
                       clean_air_pace, dirty_air_pace, first/second half pace,
                       representative_pace, racing_lap_count
season_driver_stats  — cumulative season totals per (year, driver_number, meeting_key)
                       one row per round so full-season progression is plottable
                       includes: laps_completed, distance_km, overtakes, pit_stops,
                       podiums, poles, fastest_laps, points_scored, dnf/dns counts
season_constructor_stats — same structure at constructor level
                           keyed on (year, team_name, meeting_key)
circuits             — static reference table: circuit_key, name, location, country,
                       length_km, num_corners, drs_zones, circuit_type, lap_record,
                       lap_record_driver, lap_record_year, first_gp_year
                       populated by a one-off Python script (pipeline/seed_circuits.py),
                       not part of the automated ingest workflow
```

### Ingestion Principles

- All writes use **upsert** keyed on natural identifiers — e.g. `(session_key, driver_number, lap_number)` — running the pipeline twice produces no duplicates
- All OpenF1 fields treated as nullable — pipeline is defensive by default
- `ingest.py --recompute --session <session_key>` regenerates derived metrics without re-fetching raw data
- `computed_at` timestamp on `lap_metrics` tracks formula version implicitly

### Security

- Supabase **service role key** in GitHub secrets only — used by the pipeline, never the browser
- Supabase **anon key** embedded in the React bundle — intentionally public, read-only via RLS
- **RLS enabled on all tables** from day one, anon role is read-only across the entire schema

---

## Build Phases

---

### Phase 0 — Infrastructure Setup
- Status: **Complete**
*Prerequisite for everything.*

- ✅ Supabase project + full schema, RLS enabled from day one
- ✅ Port `api/openf1.py` from Marimo project into `pipeline/api/`
- ✅ Write `ingest.py` with upsert logic and `--recompute` flag
- ✅ `ingest.yml` GitHub Actions workflow with secrets
- ✅ Scaffold React + Vite in `dashboard/`, Supabase JS client configured
- ✅ `deploy.yml` workflow, GitHub Pages confirmed working end to end

**Exit criteria:** manually triggering the workflow populates Supabase for a full race weekend, and a barebones React page on GitHub Pages reads that data.

**Completed 2026-03-18.** First successful ingest was the 2026 Chinese GP Sprint Race (session 11240, meeting 1280). Dashboard live at https://stevenhirsch.github.io/f1_dashboard/.

---

### Pipeline Testing — Complete (updated through Phase 2)
*Covers all ingestion logic including qualifying phase assignment.*

144 tests across two files, run via `pixi run -e pipeline test`. All tests are pure unit/integration tests using `unittest.mock` — no live API or database calls.

**`pipeline/tests/test_openf1.py`** — 54 tests covering the OpenF1 API client:
- `_get` cache lifecycle: hit returns cached result, miss calls API and caches, 404 cached as empty list
- HTTP error handling: 500/503 raise immediately, 429 triggers exponential backoff and retry, exhausting retries raises
- Param formats: dict params and list-of-tuple range operators both work correctly
- Stats tracking: `real_calls`, `cache_hits`, `rate_limit_waits`, `call_times` all increment correctly; `get_stats` returns an independent copy; `current_rate` rolling window logic
- `clear_cache` forces a fresh API call on next request
- API wrapper spot-checks: correct endpoint and params for `get_meetings`, `get_laps` (with/without driver filter), `get_car_data` and `get_location` (range operator tuples), `get_championship_drivers`, and others

**`pipeline/tests/test_ingest.py`** — 65 tests covering all ingest functions:
- `_parse_gap`: 13 cases — None, numeric float/string, "+N LAP/LAPS", missing plus sign, arbitrary non-numeric, zero, negative
- `_pick`: key selection, missing keys silently excluded, empty result
- `_upsert`: empty rows no-op, 1001-row batch produces 3 chunks of 500/500/1, `ReadError`/`ConnectError` retry with exponential backoff, raises after 4 failed attempts
- Per-table ingest functions: field mapping, all filter conditions (missing session_key/driver_number/lap_number), empty API response behaviour
- `ingest_race_control`: the same-timestamp test explicitly verifies the PK fix — two different messages at identical timestamps both survive
- `ingest_race_results`: pit count aggregation, gap_to_leader stringification, dnf/dns/dsq defaulting
- `ingest_qualifying_results`: fastest lap selection, zero/None/non-numeric/negative duration all excluded
- `ingest_intervals`: `_parse_gap` integration, lapped driver handling, leader null gap
- `process_session` routing: all 5 session types (Race, Qualifying, Sprint, Sprint Qualifying, Sprint Shootout) route to the correct ingest functions; early return on missing session
- `process_meeting`: allowed session filtering, practice sessions skipped

**CI:** `test.yml` runs the suite on every push and pull request via `prefix-dev/setup-pixi`. Verified locally with `act push --job test` using the medium Docker image.

---

### Phase 1 — Race Tab
- Status: **Complete (2026-03-19)**

**Data ingestion additions — Complete (2026-03-18)**
- ✅ `intervals` table — `gap_to_leader` (numeric seconds) + `laps_down` integer for lapped drivers; "+1 LAP" strings parsed out
- ✅ `starting_grid` table — keyed to qualifying session (not race session — that's how OpenF1 structures it)
- ✅ `pit_stops.lane_duration`
- ✅ `race_results.number_of_laps`
- ✅ `overtakes.date` — PK updated to `(session_key, date, driver_number_overtaking, driver_number_overtaken)` since `lap_number` is not in the API response
- ✅ `starting_grid.lap_duration` — qualifying lap time that set the grid position
- ✅ `races.circuit_type` — "Permanent", "Temporary - Street", or "Temporary - Road"
- ✅ `championship_drivers` table — driver points/position before and after each race (Beta endpoint, race/sprint only)
- ✅ `championship_teams` table — constructor points/position before and after each race (Beta endpoint, race/sprint only)
- ✅ `race_control.qualifying_phase` — PK updated to `(session_key, date, category, message)` to correctly handle multiple messages at the same timestamp

**Race Results Table — Complete**
- ✅ Position, Driver (coloured by team), Team, Laps, Time/Gap, Pit count
- ✅ DNF/DNS/DSQ shown in Pos column in red
- Deferred: status_detail reason (needs race_control parsing), fastest_lap_flag (no API source yet), max speed, G-load metrics, clean air pace (Phase 5)

**Race Visualizations — Complete**
- ✅ Tyre strategy plot — per driver ordered by finish position, SC/VSC overlay, fresh/used hatching, gray placeholder for missing stints
- ✅ Weather strip — dual-axis: track/air temp (left) + rainfall (right)
- Gap evolution chart — removed (interval timestamps have no reliable lap-number mapping; may revisit in Phase 3)

**Known issues:**
- OpenF1 sprint stints data gap (session 11240): `/stints` only returns `stint_number=2` for pitters. First stints (laps 1–13) completely absent from the API. Dashboard renders gray placeholder bars. Candidate for OpenF1 GitHub issue.

**Year-on-Year Comparison Table — Deferred to Phase 4**
- Requires multi-season backfill (`python pipeline/ingest.py --year 2025` etc.)

**Frontend implementation details:**
- All data hooks fetch multiple tables via `Promise.all` and merge in JS — Supabase `select('*, drivers(...)')` join syntax does not work for `drivers` (composite PK, no FK defined in referencing tables)
- SC/VSC detection uses `row.category === 'SafetyCar'` (OpenF1 field), not `row.flag`
- Fresh/used tyre determination: solely `tyre_age_at_start === 0`
- Plotly pattern: when `pattern.shape` is set, `marker.color` = pattern line color; `marker.pattern.bgcolor` = bar fill

---

### Phase 2 — Qualifying Tab
- Status: **Complete (2026-03-21)**

**Data ingestion additions — Complete**
- ✅ `_normalize_phase(v)` — handles OpenF1 integer qualifying_phase (1/2/3) and string ("Q1"/"Q2"/"Q3") forms
- ✅ `_assign_qualifying_phases(laps, race_control)` — injects `_phase` into each lap via sorted race_control events
- ✅ `_get_compound_for_lap(driver_number, lap_number, stints)` — compound lookup for a driver's specific lap
- ✅ `qualifying_results` — 6 new columns: `q1_compound`, `q2_compound`, `q3_compound`, `q1_laps`, `q2_laps`, `q3_laps`; per-phase best times computed correctly

**Qualifying Results Table — Complete**
- ✅ Position ordered by `starting_grid.position` (official classification, avoids raw `best_lap_time` sort misranking Q1-only drivers above Q2/Q3 drivers)
- ✅ Driver (team-coloured), Team, Q1 / Q2 / Q3 best times, Laps
- ✅ Dynamic elimination separators ("eliminated after Q1" / "eliminated after Q2") using client-side phase detection as fallback when DB times are null

**Phase Analysis tabs (Q1 / Q2 / Q3) — Complete**
- ✅ Per-driver stint table: compound badges + fresh/used indicator, best phase time, gap to leader
- ✅ Elimination separators within each phase tab — advanced drivers shown first, eliminated drivers below divider
- ✅ Phase-filtered weather strip — shows only weather during the active phase's time window
- ✅ Phase-filtered sector delta heatmap — S1/S2/S3 deltas computed within the active phase only

**Qualifying Visualizations — Complete**
- ✅ Sector delta heatmap (`SectorDeltaHeatmap.jsx`) — Plotly heatmap, green = matched fastest, red = slower; per-phase when viewed in tabs
- Track evolution chart — removed (per-driver lap numbers restart from 1, making cross-driver comparison on a shared x-axis misleading)
- Tyre strategy per session — removed (same lap number issue; tyre data partially missing from OpenF1 for qualifying)

**Race tab enhancement — Complete**
- ✅ Grid position change indicator (▲N / ▼N / —) inline in Pos cell, sourced from `starting_grid` keyed to qualifying session

---

### Phase 3 — Driver Tab, Part A
- Status: **Complete (2026-03-21)**
*Direct API data only — telemetry fetched live from OpenF1 browser-side.*

**Delivered:**
- ✅ Driver selector with headshot, team colour, name
- ✅ Race sub-tab: finish position, best lap, pit summary, overtakes made/suffered, FL Speed (st_speed on fastest lap), Top Speed (session max incl. tow); position chart; gap-to-leader chart; stacked sector bar lap times chart; lap-by-lap table with compound badges, sector splits, trap speed
- ✅ Qualifying sub-tab: Q1/Q2/Q3 summary card, FL Speed, Top Speed; stacked sector bar lap times chart with Q1/Q2/Q3 phase boundary lines; lap-by-lap table with phase, delta-to-best, compound
- ✅ Lap telemetry: click table row to load OpenF1 car_data + location; multi-select overlays; `useRef`-based cache
- ✅ 2D track map (single lap: speed-gradient; multi-lap: hidden — XY overlay adds no value)
- ✅ 8-panel TelemetryChart: Speed, Power (%), Brake, Gear, Lift & Coast, Thr/Brk overlap, RPM, DRS — all sharing a single distance x-axis
- ✅ InfoTooltip on driving style panels (Lift & Coast, Thr/Brk) and Tyre Strategy heading

**Deferred to Phase 6 (requires Phase 5 derived metrics):**
- P90 accel / decel G
- DRS activation percentage
- Stint pace degradation chart
- Clean air pace
- Driving style fingerprint radar chart

---

### Phase 4 — Polish and Public Release
- Status: **Complete (2026-03-22)**

*No new data features. Full UX pass before derived metrics.*

- ✅ Custom F1-inspired theme — dark background, team colour accents, clean typography
- ✅ Session / GP / year selector refinement — restructured to Year + Weekend + Event; Event dropdown only shown on sprint weekends; tabs (Race / Qualifying / Driver) are now the view selector, not session selector
- ✅ Shareable URLs with session state encoded in query params (`year`, `weekend`, `event`)
- ✅ Footer — GitHub repo link, dynamic copyright year, OpenF1 attribution
- ✅ App branding — title removed, F1 logo stands alone top-left; Dashboard / Chat top-level nav top-right
- ✅ `races.gmt_offset` — weather chart x-axis now shows local race timezone, not UTC
- ✅ Mobile touch fix — `fixedrange: true` + `scrollZoom: false` on all 10 Plotly charts; page scroll no longer triggers accidental zoom
- ✅ Mobile text size fix — `text-size-adjust: 100%` on body prevents iOS Safari portrait/landscape font inflation
- ✅ Table horizontal scroll — `overflowX: auto` wrappers on all tables across Race, Qualifying, and Driver pages
- ✅ Lazy loading — `TyreStrategyPlot`, `WeatherStrip`, and Qualifying Session Analysis section defer Supabase queries until scrolled into view; Position and Gap charts in Driver tab defer rendering
- Loading states — deemed sufficient as-is (plain text loading messages, dropdowns only surface ingested sessions)
- Performance / query optimisation — intentionally deferred pending Phase 5 architecture decisions
- Backfill ingestion (2023–2025) — in progress, done slowly alongside other work

**Exit criteria:** a clean shareable link working on desktop and mobile, covering all races from 2023 onwards across all three tabs.

---

### Phase 5 — Derived Metrics Pipeline
- Status: **Partial — car-data metrics + lap flags + sector bests implemented; battle states + stint_metrics remaining**
*All computed at ingestion time. Populates `lap_metrics`, `stint_metrics`, `session_sector_bests`, `season_driver_stats`, `season_constructor_stats`. Unlocks Phase 6 and the chatbot data model.*

**Signal notes**
- OpenF1 `brake` is boolean (0/100) — no derivative is meaningful for brake; deceleration of the car is computed instead (output of braking strategy, not the inputs)
- Coasting uses `throttle < 1%` threshold, not strict zero — captures light lift-and-glide; annotate this definition in UI labels and chatbot descriptions for consistency
- Car data is fetched at ingestion per driver per session (3.7 Hz), metrics computed, raw data discarded — not stored
- **Peak G signal processing (validated in `research/car_velocity/noise.py`):** dedup → PCHIP resample to 4 Hz → 4th-order Butterworth low-pass at 0.5 Hz → diff → throttle/brake gating. Cutoff of 0.5 Hz is pipeline-wide standard. Windowed approach (throttle > 20% for accel, throttle < 20% OR brake > 0 for decel) is strictly better than global max/min. Brake resampled nearest-neighbour; throttle linearly interpolated.

**`lap_metrics` — car-data derived (per lap per driver)**
*All metrics computed per sector where possible. Variable names follow `_s1/_s2/_s3/_lap` suffix convention.*

**Longitudinal G — Implemented (race/sprint) ✅**
- `peak_accel_g` — windowed peak acceleration (throttle > 20% gate); 4th-order 0.5 Hz Butterworth on speed before diff; positive float or null
- `peak_decel_g_abs` — windowed peak deceleration magnitude (throttle < 20% OR brake > 0 gate); positive float or null
- Both stored at lap level; plausibility bounds (accel ≤ 4 g, decel ≤ 8 g) applied only at the race_results summary level — raw per-lap values remain inspectable

**Longitudinal G — Qualifying extension ✅ Implemented (2026-03-29)**
- `recompute_lap_metrics` runs for qualifying sessions; `ingest_qualifying_peak_g_summary` upserts best-lap peak G per Q1/Q2/Q3 phase to `qualifying_results`
- Schema: add 6 nullable float columns to `qualifying_results` (migration pending)

**Lateral G — Abandoned (2026-03-29)**
- **Status:** Not feasible. OpenF1 XY coordinates represent progression along a fixed reference path (track centre line or similar), not the car's actual 2D position on circuit. All three methods researched (circumradius, yaw rate, cross-track acceleration) derive heading or curvature from these coordinates and therefore recover track geometry rather than driver-specific lateral load. The apparent 2–3 g signal observed in the track map colour plots was track curvature, not per-driver G. No workaround exists without a true driving-line data source. All lateral G columns permanently removed from scope.

**Car-data metrics — ✅ All implemented (2026-03-29)**
- `coasting_ratio_lap/s1/s2/s3` ✅ — proportion where throttle < 1% AND brake == 0
- `coasting_distance_m_lap/s1/s2/s3` ✅ — metres under coasting condition; proxy for energy regeneration in 2026+
- `estimated_superclipping_distance_m_lap/s1/s2/s3` ✅ — metres where throttle ≥ 10% AND brake == 0 AND decelerating (2026+ battery-harvest proxy)
- `throttle_brake_overlap_ratio_lap/s1/s2/s3` ✅ — proportion where brake > 0 AND throttle ≥ 10% (trail braking proxy)
- `full_throttle_pct_lap/s1/s2/s3` ✅ — proportion where throttle ≥ 99%
- `throttle_input_variance_lap/s1/s2/s3` ✅ — var(diff(throttle)); higher = rougher inputs
- `drs_activation_count`, `drs_distance_m` ✅ — DRS open transitions and distance; None when no DRS column
- `max_speed_kph_lap/s1/s2/s3` ✅ — max speed per sector from resampled signal
- `max_linear_acceleration_g_lap/s1/s2/s3` ✅ — windowed peak accel per sector
- `max_linear_deceleration_g_lap/s1/s2/s3` ✅ — windowed peak decel per sector
- `brake_zone_count_lap/s1/s2/s3` ✅ — distinct braking events (contiguous decel > 0.5g) per sector
- `mean_peak_decel_g_lap/s1/s2/s3` ✅ — mean of peak decel across all braking events per sector
- `speed_at_brake_start_kph_lap/s1/s2/s3` ✅ — entry speed of hardest braking zone per sector
- `brake_entry_speed_pct_rank_lap/s1/s2/s3` ✅ — midpoint percentile rank (0–100) within session; higher = later braking
- `brake_entry_speed_z_score_lap/s1/s2/s3` ✅ — z-score vs session mean/SD (population std, ddof=0)
- `brake_entry_speed_category_lap/s1/s2/s3` ✅ — 'early' (rank < 33.3), 'average' (< 66.7), 'late' (≥ 66.7)
- ~~`accumulated_linear_acceleration_g_*`~~ — removed: no clear physical interpretation beyond peak G
- ~~`accumulated_linear_deceleration_g_*`~~ — removed: same reason
- ~~`peak/mean/accumulated_lat_g_*`~~ — removed: OpenF1 XY is reference-path, not driving-line position

**`lap_metrics` — battle and proximity states (per sector per lap) ✅ Implemented (2026-03-30)**
Derived from intervals timestamps cross-referenced with sector boundary times from `laps`.
- `gap_ahead_s1/s2/s3` ✅ — gap to car ahead at end of each sector (seconds); None for race leader
- `gap_behind_s1/s2/s3` ✅ — gap to car behind at end of each sector
- `battle_ahead_s1/s2/s3_driver` ✅ — driver number of car < 1s ahead at sector end (nullable)
- `battle_behind_s1/s2/s3_driver` ✅ — driver number of car < 1s behind (nullable)
- `is_estimated_clean_air` ✅ — gap_ahead > 2s across all sectors; None when no interval data; leader always True
- `overtakes_s1/s2/s3` ✅ — times driver overtook another driver in each sector window
- `overtaken_s1/s2/s3` ✅ — times driver was overtaken in each sector window
- `lap_overtakes` ✅ — sum of s1+s2+s3 overtakes
- `lap_overtaken` ✅ — sum of s1+s2+s3 overtaken
- `i1_speed` ✅ — speed at first intermediate point (from OpenF1 laps)
- `i2_speed` ✅ — speed at second intermediate point (from OpenF1 laps)
- `sector_context_s1/s2/s3` ✅ — mini-sector segment lists (`segments_sector_1/2/3` from OpenF1 laps)

**OpenF1 data artifact (documented):** Race leader returns `interval=0.0`/`gap_to_leader=0.0` instead of null/null. Pipeline treats `interval==0.0 AND gap_to_leader==0.0` as the leader sentinel; `gap_ahead` set to None, `is_estimated_clean_air=True`. Validated against 2026 Chinese GP: driver 12 shows gap_ahead=null from lap 3 onward (took lead after lap 2).

**Position snapshot algorithm:** At each sector end time `t`, build position order by nearest-neighbour lookup (bisect) in the intervals index per driver, then sort lead-lap drivers by `gap_to_leader`, lapped drivers by `laps_down`. `gap_ahead` = driver's own `interval`; `gap_behind` = following driver's `interval`.

- `sector_context` - uses the `segments_sector_1/2/3` key to return a list of mini sectors. Mini sectors are defined as:
|value | colour |
| ---- | ------ |
| 0    | not available |
| 2048 | yellow sector |
| 2049 | green sector |
| 2050 | ? (null) |
| 2051 | purple sector |
| 2052 | ? (null) |
| 2064 | pit lane |
| 2068 | ? (null) |
  - Purple (Fastest of All): Indicates the absolute fastest time recorded in that specific sector by any driver during the entire session.
  - Green (Personal Best): Indicates the driver has set their own best time for that sector, but it is not faster than the purple time.
  - Yellow (Slower Sector): Indicates the driver did not improve on their personal best time for that sector. This often suggests a mistake, heavy fuel, tire degradation, or traffic.
  - Sector Timing Usage: These colors are primarily used in qualifying and practice sessions to help teams and viewers analyze performance in the three designated sectors (S1, S2, S3) of the track. 
  - We can ingest this for qualifying data as additional context

**`lap_metrics` — lap-level flags and deltas ✅ Implemented (2026-03-29)**
- `is_neutralized` ✅ — True when any SC/VSC (`category='SafetyCar'`) or red flag (`flag='RED'`) RC event falls within `[date_start, date_start + lap_duration]`; None when start time or duration missing
- `tyre_age_at_lap` ✅ — `tyre_age_at_start + (lap_number − stint.lap_start)` via stint lookup; None when no matching stint (e.g. OpenF1 sprint data gap)
- `delta_to_session_best_s1/s2/s3` ✅ — sector time minus session best; computed as part of `ingest_session_sector_bests`; None when either value missing

**`session_sector_bests` ✅ Implemented (2026-03-29)** — one row per session
- `best_s1/s2/s3` ✅ — minimum valid (> 0, non-null) sector time across all laps
- `best_s1/s2/s3_driver` ✅ — driver who set each best
- `theoretical_best_lap` ✅ — sum of best_s1 + best_s2 + best_s3; None if any sector missing

**`stint_metrics`** — per (session_key, driver_number, stint_number)
- `clean_air_pace_s` — mean lap time on laps where is_estimated_clean_air == true
- `dirty_air_pace_s` — mean on non-clean laps
- `first_half_pace_s` / `second_half_pace_s` — split on stint midpoint, excluding neutralized laps
- `representative_pace_s` — median of non-neutralized, non-pit-in/out laps
- `racing_lap_count` — laps excluding neutralized and pit-in/out

**`race_results.fastest_lap_flag`** ✅ Implemented (2026-03-29)
Derived from minimum `lap_duration` among classified finishers (not DNF/DNS/DSQ). `ingest_fastest_lap_flag(client, session_key, laps)` — reuses cached `get_session_result` response, no extra API call. Called for race and sprint sessions in `process_session`.

**`weather` additions ✅ Pipeline-complete (2026-03-29)**
- `wind_speed` (m/s), `wind_direction` (degrees 0–359), `pressure` (mbar) — already fetched and mapped in `ingest_weather`; schema migration (3 columns) pending

**`championship_drivers` / `championship_teams` additions**
- `points_gap_to_leader`, `points_gap_to_p2` — derived from same API response at ingestion

**`season_driver_stats`** — cumulative per (year, driver_number, meeting_key)
One row per driver per round; full table gives season progression plottable over time.
- `round_number` — integer round within the year (for ordering)
- `races_entered`, `races_classified`, `dnf_count`, `dns_count`, `dsq_count`, `penalty_points`
- `laps_completed`, `distance_km` (laps × `races.circuit_length_km`)
- `total_overtakes_made`, `total_overtakes_suffered`, `total_pit_stops`
- `podiums`, `poles`, `fastest_laps`, `points_scored`, `points_per_race`, `percent_of_driver_points_relative_to_maximum` (i.e., winning every race), `percent_of_team_points_by_driver`, `wins_over_teammate`
- `qualifying_supertimes` - average gap between teammate from the best qualifying lap of each race weekend; assesses pure pace between drivers 
- Sprints count toward all totals (laps, distance, overtakes, etc.)

**`season_constructor_stats`** — same structure at constructor level, keyed on (year, team_name, meeting_key)

**`circuits`** — static reference table, populated by `pipeline/seed_circuits.py` (not automated ingest)
- `circuit_key`, `circuit_name`, `location`, `country`, `length_km`
- `num_corners`, `drs_zones`, `circuit_type`
- `lap_record_time_s`, `lap_record_driver`, `lap_record_year`, `first_gp_year`
- Note that openf1 only has data since 2023

**Ingestion order (as implemented in `recompute_lap_metrics`)**
```
✅ 1. Raw ingest — laps, stints, race_control, intervals, weather (+wind/pressure), overtakes
✅ 2. ingest_lap_flags — is_neutralized, tyre_age_at_lap
✅ 3. ingest_session_sector_bests — session_sector_bests + delta_to_session_best_s1/s2/s3
✅ 4. ingest_lap_metrics — car_data fetch per driver → all car-data metrics → store → discard
✅ 5. ingest_race/qualifying_peak_g_summary — G summary to race_results / qualifying_results
✅ 6. ingest_brake_entry_speed_ranks — session-level pct_rank/z_score/category
✅ 7. race_results.fastest_lap_flag — ingest_fastest_lap_flag (called in process_session, not recompute)
✅ 8. ingest_battle_states — gap/battle states per sector (intervals × sector timestamps)
   9. ingest_stint_metrics — requires is_estimated_clean_air + is_neutralized
  10. season_driver_stats + season_constructor_stats (cumulative row; reads prior round, adds delta)
```

When a formula changes, run `ingest.py --recompute --session <session_key>` to regenerate from step 2 onwards without re-fetching raw data. The `computed_at` timestamp on `lap_metrics` identifies rows predating a formula change.

---

### Phase 6 — Driver Tab, Part B
- Status: Incomplete
*Surfaces derived metrics from Phase 5.*

**Driver Summary additions**
- Coasting ratio, throttle-brake overlap, input smoothness summary stats
- Phase consistency score — sector-level SD across all laps
- Driving style fingerprint — radar chart across style dimensions (coasting, smoothness, braking aggressiveness, throttle-on-apex, DRS usage)

**Lap-by-Lap Table additions**
- Coasting ratio per lap
- High lateral load index per lap
- Longitudinal load index per lap
- Stint phase pace split per lap
- Braking consistency flag for outlier laps

**New visualizations**
- Phase consistency chart — per-sector SD across all laps, showing where a driver is most and least consistent
- Clean air pace vs. wheel-to-wheel pace comparison per stint

---

### Phase 7 — Composite and Latent Metrics *(v2, experimental)*
- Status: Incomplete
*Depends on Phase 5 being well validated.*

**Team-level analysis**
- Strategy-corrected team pace gap — normalised average race pace difference between constructors, corrected for tyre age and stint phase, comparable across races
- Cross-season pace dominance index — average pace gap between the leading team and each constructor by year, contextualisable against regulation eras (2023 Red Bull, 2025 McLaren, 2026 Mercedes)
- Super-clipping proxy — top speed at straight exit vs. entry speed into braking zone as a ratio, approximating power unit deployment advantage on straights

**Driver-level composites**
- Race Load model — latent composite of G-load indices, overtake count, temperature, stint length
- Cross-driver style clustering — group drivers by Phase 5 feature similarity across a season

---

### 2026+ Season Notes — Battery Management Metrics
*Pending OpenF1 exposing battery state data for the new power unit.*

**Existing metrics recontextualised for 2026**
- Coasting ratio now carries direct strategic meaning — coasting is the primary battery regeneration mechanism, not just a driving style indicator. Should be annotated accordingly in the UI from 2026 onwards
- Overtake count is not comparable across the 2025/2026 boundary — 120 overtakes in the 2026 Australian GP vs. 45 in 2025. Year-on-year overtake comparisons must flag this regulatory discontinuity

**New 2026-specific metrics**
- Battery deployment events — moments of offensive energy deployment, distinct from legacy DRS activations. Detectable if OpenF1 exposes the relevant car data channel for the new power unit
- Proximity recharge triggers — laps where a driver was within 1 second of the car ahead at the detection point, earning bonus recharge power for the following lap. Derivable from interval data crossed with lap boundaries
- Race start battery reserve proxy — driver's relative speed delta in S1 of lap 1 vs. their qualifying S1 time, as a crude indicator of charge held in reserve for the start
- Defensive vs. offensive deployment ratio — proportion of deployment events that defend a position vs. gain one, inferred from overtake records and position changes

*Monitor the OpenF1 changelog as the 2026 season progresses — the car data endpoint may be extended with new fields for the new power unit architecture.*

---

## Technical Reference

### OpenF1 API

**Base URL:** `https://api.openf1.org/v1`

**Rate limits:** 3 req/s and 30 req/min on the free tier. The ingestion pipeline should respect these with a small delay between bulk requests.

**Key endpoints and their role in this project:**

| Endpoint | Key fields | Used for |
|---|---|---|
| `/meetings` | `meeting_key`, `meeting_name`, `year` | Race/GP selector |
| `/sessions` | `session_key`, `session_name`, `session_type` | Session selector |
| `/drivers` | `driver_number`, `name_acronym`, `team_name` | Driver lookup |
| `/laps` | `lap_duration`, `lap_number`, `sector_*`, `date_start` | Lap times, sector splits |
| `/stints` | `compound`, `lap_start`, `lap_end`, `tyre_age_at_start` | Tyre strategy |
| `/pit` | `lap_number`, `pit_duration`, `stop_duration` | Pit analysis (stop_duration from 2024 US GP+) |
| `/car_data` | `throttle`, `brake`, `speed`, `rpm`, `gear`, `drs` | Telemetry — 3.7 Hz |
| `/location` | `x`, `y`, `z` | GPS position — 3.7 Hz |
| `/position` | `position`, `date` | Race position over time |
| `/intervals` | `gap_to_leader`, `interval` | Gap evolution |
| `/race_control` | `category`, `message`, `date` | SC/VSC flags, DNF reasons |
| `/weather` | `track_temperature`, `air_temperature`, `rainfall` | Weather context |
| `/overtakes` | `overtaking_driver_number`, `overtaken_driver_number`, `position` | Overtake events |
| `/session_result` | `position`, `gap_to_leader`, `dnf`, `dsq`, `duration` | Final classification |
| `/team_radio` | `recording_url`, `date` | Driver comms |

**Telemetry data note:** `/car_data` and `/location` operate at 3.7 Hz (~one sample every 270ms). At 200 km/h that's roughly 15m per sample. This is sufficient for lap-level aggregates but limits braking point localisation precision. Always query these with `date_start` and `date_end` bounds derived from `/laps` to avoid fetching an entire session.

**Data availability:** 2023 season onwards.

**Neutralised lap detection:** SC/VSC/red flag laps should be excluded from any pace or degradation calculations. Detect by scanning `/race_control` for `category` in `("SafetyCar", "VirtualSafetyCar")` with `"DEPLOYED"` in the message, and `category == "Flag"` with `"RED"` in the message, then cross-referencing lap numbers via `date_start`.

---

### Derived Metric Computation Notes

**G-force proxies from speed data**

OpenF1 does not expose G-force directly. All G metrics are approximated:

- **Longitudinal G (acceleration):** `Δspeed / Δtime` between consecutive car data samples, divided by 9.81. Positive = acceleration, negative = deceleration.
- **Lateral G:** not derivable from OpenF1 data. The `/location` XY coordinates track progression along a fixed reference path, not the car's actual 2D position. All curvature and yaw-rate methods therefore recover track geometry rather than driver-specific lateral load. Lateral G columns are permanently out of scope.
- **P90 aggregation:** compute the 90th percentile of absolute values per lap per driver, stored in `lap_metrics`. Use P90 rather than max to exclude kerb strikes and sensor noise.

**Coasting ratio**

```python
# Per lap, from car_data samples
# Threshold: throttle < 1% (not strict zero — captures light lift-and-glide)
# brake is boolean in OpenF1 (0 or 1)
coast_samples = sum(1 for s in samples if s['throttle'] < 1 and s['brake'] == 0)
coasting_ratio = coast_samples / total_samples
coasting_distance_m = sum(
    (samples[i+1]['distance'] - samples[i]['distance'])
    for i in range(len(samples)-1)
    if samples[i]['throttle'] < 1 and samples[i]['brake'] == 0
)
```

Use the `throttle < 1%` definition consistently across pipeline code, dashboard labels, and chatbot descriptions.

In 2026 this metric doubles as a battery regeneration proxy — annotate accordingly in the UI.

**Throttle-brake overlap (trail braking)**

```python
overlap_samples = sum(1 for s in samples if s['throttle'] > 0 and s['brake'] > 0)
overlap_ratio = overlap_samples / total_samples
```

**Throttle smoothness index**

```python
import numpy as np
# brake is boolean in OpenF1 — no derivative meaningful
# smoothness index applies to throttle only
throttle_vals = [s['throttle'] for s in samples]
throttle_smoothness_index = np.var(np.diff(throttle_vals))
# Lower = smoother throttle application
```

**Clean air pace**

Filter to laps where `interval_ahead > 2.0` and `interval_behind > 2.0` (from `/intervals`), then average lap time per stint. This is your best proxy for true car pace independent of traffic.

**Braking event detection**

```python
# Detect braking events from car_data
events = []
in_brake = False
for i, s in enumerate(samples):
    if s['brake'] > 10 and not in_brake:
        in_brake = True
        event_start = i
    elif s['brake'] < 5 and in_brake:
        in_brake = False
        event = samples[event_start:i]
        events.append({
            'speed_at_onset': event[0]['speed'],
            'min_speed': min(e['speed'] for e in event),
            'duration_samples': len(event),
            'decel_rate': (event[0]['speed'] - min(e['speed'] for e in event)) / len(event)
        })
```

**Braking consistency index**

Group braking events by approximate track position (GPS cluster or mini-sector anchor). For each corner cluster, compute standard deviation of speed at brake onset across all laps:

```python
import numpy as np
consistency_index = np.std([e['speed_at_onset'] for e in corner_events])
# Lower = more consistent
```

**Stint phase split**

```python
midpoint = len(stint_laps) // 2
early_pace = np.mean([l['lap_duration'] for l in stint_laps[:midpoint]])
late_pace = np.mean([l['lap_duration'] for l in stint_laps[midpoint:]])
phase_delta = late_pace - early_pace  # positive = degrading
```

**Sector delta to pole (qualifying)**

```python
pole_s1 = min(q3_laps, key=lambda l: l['duration_sector_1'])['duration_sector_1']
pole_s2 = min(q3_laps, key=lambda l: l['duration_sector_2'])['duration_sector_2']
pole_s3 = min(q3_laps, key=lambda l: l['duration_sector_3'])['duration_sector_3']

for driver_lap in q3_laps:
    delta_s1 = driver_lap['duration_sector_1'] - pole_s1
    delta_s2 = driver_lap['duration_sector_2'] - pole_s2
    delta_s3 = driver_lap['duration_sector_3'] - pole_s3
```

Note: pole in each sector is independently the fastest time set by any driver in that sector, not necessarily the pole lap. This matches how teams analyse qualifying.

---

### Frontend Architecture Notes

**Supabase JS client setup**

```javascript
import { createClient } from '@supabase/supabase-js'

const supabase = createClient(
  import.meta.env.VITE_SUPABASE_URL,
  import.meta.env.VITE_SUPABASE_ANON_KEY
)
```

Store keys in `.env.local` for development. For GitHub Pages, set as repository variables (not secrets — they're public by design) and reference in `deploy.yml`.

**Recommended query pattern**

Use custom React hooks per data domain to keep components clean:

```javascript
// hooks/useRaceResults.js
export function useRaceResults(sessionKey) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!sessionKey) return
    supabase
      .from('race_results')
      .select('*, drivers(name_acronym, team_name)')
      .eq('session_key', sessionKey)
      .order('position')
      .then(({ data }) => { setData(data); setLoading(false) })
  }, [sessionKey])

  return { data, loading }
}
```

**Plotly performance guidelines**

- Load charts on demand (tab visibility or user interaction), not all on page load
- Aggregate telemetry to lap-level in Supabase — never fetch raw 3.7 Hz samples to the browser
- For the gap evolution chart and tyre strategy plot, limit traces to drivers in the points or selected drivers to avoid rendering 20 simultaneous traces
- Paginate the lap-by-lap table — 60 rows × many columns in a single DOM render is slow on mobile

**Shareable URL pattern**

Encode session state in query params using `URLSearchParams`:

```javascript
// On selector change
const params = new URLSearchParams({ year, gp, session, driver })
window.history.replaceState({}, '', `?${params}`)

// On page load
const params = new URLSearchParams(window.location.search)
const year = params.get('year') ?? '2026'
```

---

### Year-on-Year Regulatory Context

When displaying cross-year comparisons, annotate with the regulation era. Lap times, overtake counts, and team dominance metrics are not directly comparable across these boundaries:

| Era | Years | Key characteristic |
|---|---|---|
| Pre-ground effect | up to 2021 | DRS-dependent overtaking |
| Ground effect regs | 2022–2025 | DRS retained, closer racing |
| Hybrid 50/50 regs | 2026+ | Battery management replaces DRS, ~3× overtake increase |

The pace dominance index should always be displayed with era context. Red Bull's 0.57s average advantage in 2023, McLaren's 0.31s in 2025, and Mercedes' early 2026 advantage are all from different regulatory environments and should not be presented as a simple linear trend.

---

### Marimo Notebook Role

The Marimo project continues as a local experimentation environment for:
- Prototyping new metric computations before adding them to the pipeline
- Validating ingestion logic and schema against real data
- Exploratory analysis that may or may not become dashboard features

It is never deployed and shares no runtime dependency with the production stack. The `pipeline/api/` module is ported from the existing Marimo `api/openf1.py` — keep them in sync manually or extract into a shared internal package if divergence becomes a maintenance burden.
