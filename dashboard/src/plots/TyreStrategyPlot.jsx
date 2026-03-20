import Plot from 'react-plotly.js'
import { useStints } from '../hooks/useStints'
import { useRaceControl } from '../hooks/useRaceControl'

const COMPOUND_COLOURS = {
  SOFT: '#e8002d',
  MEDIUM: '#ffd800',
  HARD: '#afafaf',
  INTERMEDIATE: '#39b54a',
  WET: '#0067ff',
  UNKNOWN: '#888888',
}
const COMPOUND_ORDER = ['SOFT', 'MEDIUM', 'HARD', 'INTERMEDIATE', 'WET', 'UNKNOWN']

function compoundColour(compound) {
  return COMPOUND_COLOURS[(compound ?? '').toUpperCase()] ?? COMPOUND_COLOURS.UNKNOWN
}

function capitalize(s) {
  return s ? s[0] + s.slice(1).toLowerCase() : s
}

// Plotly pattern behaviour:
//   - pattern.shape is empty → marker.color fills the bar (fresh)
//   - pattern.shape is set   → marker.color = pattern LINE color;
//                               pattern.bgcolor = bar background color (used)
function makeMarker(colour, isFresh) {
  if (isFresh) {
    return {
      color: colour,
      line: { color: 'rgba(0,0,0,0.15)', width: 0.5 },
    }
  }
  return {
    color: 'rgba(255,255,255,0.65)', // pattern line color (white diagonals)
    pattern: {
      shape: '/',
      bgcolor: colour,               // compound colour as bar background
      size: 8,
    },
    line: { color: 'rgba(0,0,0,0.15)', width: 0.5 },
  }
}

export default function TyreStrategyPlot({ sessionKey }) {
  const { data: stintsByDriver, driverOrder, loading: stintsLoading } = useStints(sessionKey)
  const { safetyCarPeriods, loading: rcLoading } = useRaceControl(sessionKey)

  if (stintsLoading || rcLoading) return <p>Loading tyre strategy…</p>
  if (!stintsByDriver || driverOrder.length === 0) return <p>No stint data.</p>

  // P1 at top: sort ascending by position; DNF/null positions go to the bottom
  const driversSorted = [...driverOrder].sort((a, b) => (a.position ?? 999) - (b.position ?? 999))
  const yLabels = driversSorted.map(d => d.name_acronym)

  // Max known lap_end across all stints — used as fallback for last stints with null lap_end
  const maxKnownLap = Math.max(
    0,
    ...Object.values(stintsByDriver).flatMap(ss => ss.map(s => s.lap_end ?? 0))
  )

  // One trace per (compound, freshness) combination
  const traceMap = {}

  for (const driver of driversSorted) {
    const stints = (stintsByDriver[driver.driver_number] ?? [])
      .slice()
      .sort((a, b) => (a.stint_number ?? 0) - (b.stint_number ?? 0))

    // If first known stint doesn't start at lap 1, add a gray placeholder for the gap
    const firstLapStart = stints[0]?.lap_start ?? 1
    if (firstLapStart > 1) {
      const gapKey = 'UNKNOWN_fresh'
      if (!traceMap[gapKey]) {
        traceMap[gapKey] = {
          type: 'bar', orientation: 'h',
          name: 'Unknown',
          legendgroup: gapKey,
          marker: { color: '#444444', pattern: { shape: '/', fgcolor: 'rgba(255,255,255,0.3)', size: 8 } },
          x: [], y: [], base: [], customdata: [],
          hovertemplate: '<b>%{y}</b><br>Unknown compound<br>Laps %{customdata[0]}–%{customdata[1]}<extra></extra>',
        }
      }
      traceMap[gapKey].y.push(driver.name_acronym)
      traceMap[gapKey].x.push(firstLapStart - 1)
      traceMap[gapKey].base.push(0)
      traceMap[gapKey].customdata.push([1, firstLapStart - 1])
    }

    for (let i = 0; i < stints.length; i++) {
      const stint = stints[i]
      const compound = (stint.compound ?? 'UNKNOWN').toUpperCase()
      // Fresh = brand new set (tyre_age_at_start = 0); Used = pre-used (> 0 laps already on it)
      const isFresh = (stint.tyre_age_at_start ?? 0) === 0

      const lapStart = stint.lap_start ?? 1
      // Infer lap_end: use next stint's lap_start, then session max, then skip
      const lapEnd = stint.lap_end
        ?? (stints[i + 1]?.lap_start != null ? stints[i + 1].lap_start - 1 : null)
        ?? (maxKnownLap > 0 ? maxKnownLap : null)
      if (lapEnd == null || lapEnd < lapStart) continue

      const key = `${compound}_${isFresh ? 'fresh' : 'used'}`
      if (!traceMap[key]) {
        const colour = compoundColour(compound)
        traceMap[key] = {
          type: 'bar',
          orientation: 'h',
          name: `${capitalize(compound)} (${isFresh ? 'Fresh' : 'Used'})`,
          legendgroup: key,
          marker: makeMarker(colour, isFresh),
          x: [],
          y: [],
          base: [],
          customdata: [],
          hovertemplate:
            '<b>%{y}</b><br>%{customdata[0]}<br>' +
            'Age at start: %{customdata[1]} laps<br>' +
            'Laps %{customdata[2]}–%{customdata[3]}<extra></extra>',
        }
      }
      traceMap[key].y.push(driver.name_acronym)
      traceMap[key].x.push(lapEnd - lapStart + 1)
      traceMap[key].base.push(lapStart - 1)
      traceMap[key].customdata.push([compound, stint.tyre_age_at_start ?? 0, lapStart, lapEnd])
    }
  }

  // Sort legend: by compound order, fresh before used
  const traces = Object.entries(traceMap)
    .sort(([a], [b]) => {
      const [aC, aF] = a.split('_')
      const [bC, bF] = b.split('_')
      const ci = COMPOUND_ORDER.indexOf(aC) - COMPOUND_ORDER.indexOf(bC)
      return ci !== 0 ? ci : (aF === 'fresh' ? -1 : 1)
    })
    .map(([, t]) => t)

  // SC/VSC shaded regions + annotations
  const shapes = []
  const annotations = []

  for (const p of safetyCarPeriods) {
    if (p.lapStart == null || p.lapEnd == null) continue
    const isSC = p.type === 'SC'
    shapes.push({
      type: 'rect',
      xref: 'x',
      yref: 'paper',
      x0: p.lapStart - 1,
      x1: p.lapEnd,
      y0: 0,
      y1: 1,
      fillcolor: isSC ? 'rgba(255,200,0,0.30)' : 'rgba(180,180,180,0.30)',
      line: { color: isSC ? 'rgba(200,150,0,0.6)' : 'rgba(120,120,120,0.6)', width: 1 },
    })
    annotations.push({
      x: (p.lapStart + p.lapEnd) / 2 - 1,
      y: 1.01,
      xref: 'x',
      yref: 'paper',
      text: p.type,
      showarrow: false,
      font: { size: 9, color: isSC ? '#997700' : '#777' },
      xanchor: 'center',
      yanchor: 'bottom',
    })
  }

  const chartHeight = Math.max(420, driversSorted.length * 26 + 100)

  return (
    <Plot
      data={traces}
      layout={{
        barmode: 'overlay',
        xaxis: { title: { text: 'Lap Number', font: { color: '#a1a1aa' } }, color: '#a1a1aa', gridcolor: 'rgba(255,255,255,0.07)', tickfont: { color: '#a1a1aa' } },
        yaxis: {
          categoryorder: 'array',
          categoryarray: [...yLabels].reverse(),
          fixedrange: true,
          automargin: true,
          color: '#a1a1aa',
          tickfont: { color: '#fafafa' },
          gridcolor: 'rgba(255,255,255,0.05)',
        },
        shapes,
        annotations,
        legend: { orientation: 'h', x: 0, y: 1.08, xanchor: 'left', yanchor: 'bottom', font: { size: 11, color: '#fafafa' }, bgcolor: 'rgba(0,0,0,0)' },
        margin: { l: 55, r: 20, t: 60, b: 50 },
        height: chartHeight,
        paper_bgcolor: '#18181b',
        plot_bgcolor: '#18181b',
        font: { color: '#fafafa' },
      }}
      config={{ responsive: true, displayModeBar: false }}
      style={{ width: '100%' }}
    />
  )
}
