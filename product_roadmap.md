# F1 Strategy Dashboard — Product Roadmap & Technical Reference

---

## Project Overview

A post-race Formula 1 analytics dashboard built as a static React web app, displaying race results, qualifying analysis, and deep driver telemetry metrics. Data is ingested from the OpenF1 API after each race weekend and stored in Supabase. The Marimo notebook is a parallel experimentation environment only and is not part of the production path.

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
        └── deploy.yml     # Builds React app, publishes to GitHub Pages
```

---

## GitHub Actions Workflows

**`ingest.yml`** — triggered manually after each race weekend. Supabase service role key stored as a GitHub secret, never exposed to the browser. Fetches from OpenF1, computes metrics, writes to Supabase via upsert.

**`deploy.yml`** — triggered on push to main. Builds the React app with Vite, pushes `/dist` to the `gh-pages` branch.

---

## Data Architecture

### Supabase Schema

```
races                — meeting metadata, circuit, year
sessions             — session key, type (Race/Qualifying/Sprint), date
drivers              — driver number, name, acronym, team, per session
laps                 — core OpenF1 lap data (times, sector splits, compounds, pit flags)
lap_metrics          — derived values keyed on (session_key, driver_number, lap_number)
                       includes computed_at timestamp
stints               — compound, lap start/end, tyre age per driver
pit_stops            — lane duration, stop duration (2024 US GP+), lap number
weather              — track temp, air temp, humidity, rainfall per session
race_results         — position, gap, DNF/DSQ with reason, pit count, fastest lap flag
qualifying_results   — Q1/Q2/Q3 best times, compounds, lap counts, delta to pole
overtakes            — overtaking/overtaken driver, position, lap number
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
- Status: Incomplete
*Prerequisite for everything.*

- Supabase project + full schema, RLS enabled from day one
- Port `api/openf1.py` from Marimo project into `pipeline/api/`
- Write `ingest.py` with upsert logic and `--recompute` flag
- `ingest.yml` GitHub Actions workflow with secrets
- Scaffold React + Vite in `dashboard/`, Supabase JS client configured
- `deploy.yml` workflow, GitHub Pages confirmed working end to end

**Exit criteria:** manually triggering the workflow populates Supabase for a full race weekend, and a barebones React page on GitHub Pages reads that data.

---

### Phase 1 — Race Tab
- Status: Incomplete

**Race Results Table**
- Position, Driver, Team, Laps
- Final time / gap to winner
- DNF / DSQ status with reason (parsed from race control messages)
- Pit count
- Best lap time + lap number, fastest lap designation
- Max speed
- P90 acceleration G, P90 deceleration G, P90 lateral G
- Strategy-corrected race pace per driver (clean air laps only)

**Race Visualizations**
- Gap / interval evolution chart — driver gaps to leader over lap number, SC/VSC overlay
- Tyre strategy plot — per driver ordered by finish position, SC/VSC overlay
- Weather strip — track temp, air temp, rainfall as shared timeline context

**Year-on-Year Comparison Table**
- Average clean lap time and fastest lap per year for the same circuit
- Annotated with regulation era, average track temp, wet/dry condition
- SC/VSC laps excluded from averages

---

### Phase 2 — Qualifying Tab
- Status: Incomplete

**Qualifying Results Table**
- Position, Driver, Team
- Q1 / Q2 / Q3 best times
- Tyre compound used for each session best lap
- Delta to pole per session
- Q1 / Q2 elimination cutoff markers
- Laps completed per session per driver

**Qualifying Visualizations**
- **Sector delta heatmap** — all drivers × S1/S2/S3, coloured by delta to pole per sector. Immediately reveals where each team gains and loses time relative to the benchmark lap
- Track evolution chart — lap number vs. lap time scatter per session, all drivers
- Tyre strategy per session — compound and stint structure per driver

---

### Phase 3 — Driver Tab, Part A
- Status: Incomplete
*Direct API data only.*

**Driver Summary Card**
- Best lap time + lap number
- Max speed
- P90 accel / decel / lateral G
- Total overtakes (overtaking and overtaken counts, via OpenF1 overtakes endpoint)
- DRS activation percentage
- Pit summary — count, lap numbers, lane duration, stop duration (2024 US GP+)

**Lap-by-Lap Table**
- Lap time, S1 / S2 / S3 sector times
- Tyre compound, laps on current tyre
- Pit status flag
- Max speed per lap
- Overtake events per lap (overtaking / overtaken, position affected)
- Radio / comms flag linked to timestamp
- P90 accel / decel / lateral G per lap

**Driver Visualizations**
- Lap times chart — lap time over lap number, coloured by compound, pit lap markers
- Stint pace degradation chart — lap time vs. laps on tyre per stint, with degradation curve fit
- Gap evolution chart — this driver's gap to leader over race distance

*Implementation note: charts load on demand, lap table is paginated, telemetry aggregated to lap-level before reaching the browser.*

---

### Phase 4 — Polish and Public Release
- Status: Incomplete
*No new data features. Full UX pass before derived metrics.*

- Custom F1-inspired theme — dark background, team colour accents, clean typography
- Responsive layout for mobile and tablet
- Session / GP / year selector refinement
- Shareable URLs with session state encoded in query params
- Loading states and empty state handling throughout
- Performance audit — lazy loading, query optimisation
- Backfill ingestion for all available race weekends from 2023 onwards

**Exit criteria:** a clean shareable link working on desktop and mobile, covering all races from 2023 onwards across all three tabs.

---

### Phase 5 — Derived Metrics Pipeline
- Status: Incomplete
*All computed at ingestion time, stored in `lap_metrics`. Unlocks Phase 6.*

**Input characterisation**
- Coasting ratio — proportion of time with throttle = 0 and brake = 0
- Throttle-brake overlap — simultaneous inputs as proportion of lap (trail braking proxy)
- Input smoothness index — variance of first derivative of throttle and brake signals
- Full-throttle percentage

**Braking profile**
- Braking event detection — speed at brake onset, deceleration rate, event duration
- Braking consistency index — lap-over-lap variance at the same circuit corners

**Load indices**
- High lateral load index — speed × curvature integral over the lap
- Longitudinal load index — sum of absolute acceleration and deceleration events above threshold

**Pace analysis**
- Pace degradation coefficient — expressed as seconds-per-lap lost per lap on tyre, comparable across drivers and teams
- Clean air pace — average lap time when gap ahead > 2s and gap behind > 2s, stored per stint
- Stint phase split — average pace in first half vs. second half of each stint

When a formula changes, run `ingest.py --recompute --session <session_key>` to regenerate. The `computed_at` timestamp identifies rows predating a formula change.

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
- **Lateral G:** requires combining speed with curvature derived from GPS location data. Curvature `κ = |v × a| / |v|³` where v and a are velocity and acceleration vectors from consecutive location samples. Lateral G ≈ `speed² × κ / 9.81`.
- **P90 aggregation:** compute the 90th percentile of absolute values per lap per driver, stored in `lap_metrics`. Use P90 rather than max to exclude kerb strikes and sensor noise.

**Coasting ratio**

```python
# Per lap, from car_data samples
coast_samples = sum(1 for s in samples if s['throttle'] == 0 and s['brake'] == 0)
coasting_ratio = coast_samples / total_samples
```

In 2026 this metric doubles as a battery regeneration proxy — annotate accordingly in the UI.

**Throttle-brake overlap (trail braking)**

```python
overlap_samples = sum(1 for s in samples if s['throttle'] > 0 and s['brake'] > 0)
overlap_ratio = overlap_samples / total_samples
```

**Input smoothness index**

```python
import numpy as np
throttle_vals = [s['throttle'] for s in samples]
brake_vals = [s['brake'] for s in samples]
throttle_smoothness = np.var(np.diff(throttle_vals))
brake_smoothness = np.var(np.diff(brake_vals))
smoothness_index = (throttle_smoothness + brake_smoothness) / 2
# Lower = smoother
```

**Pace degradation coefficient**

For each stint, fit a linear regression of lap time vs. laps on tyre:

```python
from scipy.stats import linregress
slope, intercept, r, p, se = linregress(laps_on_tyre, lap_times)
# slope = seconds per lap degradation rate
# Store slope as degradation_rate in lap_metrics or a stints_metrics table
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
