import Plot from 'react-plotly.js'
import { compoundColour } from '../utils/compounds'
import { assignPhases } from '../utils/qualifying'

// Sector colours — distinct from compound colours
const SECTOR_COLOURS = {
  S1: '#3b82f6',  // blue
  S2: '#f59e0b',  // amber
  S3: '#8b5cf6',  // purple
}

const PHASE_COLOURS = { Q1: '#a1a1aa', Q2: '#facc15', Q3: '#e10600' }

function hexToRgba(hex, alpha) {
  const h = (hex ?? '#888888').replace('#', '')
  const r = parseInt(h.slice(0, 2), 16)
  const g = parseInt(h.slice(2, 4), 16)
  const b = parseInt(h.slice(4, 6), 16)
  return `rgba(${r},${g},${b},${alpha})`
}

// ---------------------------------------------------------------------------
// Stacked sector bar chart (race mode)
// ---------------------------------------------------------------------------

// Subplot domains: main chart occupies the top portion, compound strip sits
// in a thin band at the bottom.  The gap between them (STRIP_DOMAIN[1] →
// MAIN_DOMAIN[0]) is where Plotly renders the x-axis ticks and title.
const MAIN_DOMAIN  = [0.22, 1]
const STRIP_DOMAIN = [0,    0.11]

function capitalize(s) {
  return s ? s[0] + s.slice(1).toLowerCase() : s
}

function makeStripMarker(colour, isFresh) {
  if (isFresh) {
    return { color: colour, line: { color: 'rgba(0,0,0,0.15)', width: 0.5 } }
  }
  return {
    color: 'rgba(255,255,255,0.65)',
    pattern: { shape: '/', bgcolor: colour, size: 8 },
    line: { color: 'rgba(0,0,0,0.15)', width: 0.5 },
  }
}

function SectorBarChart({ laps, stints, pitStops, safetyCarPeriods, phaseEvents, height }) {
  if (!laps || laps.length === 0) return <p style={{ color: '#a1a1aa' }}>No lap data.</p>

  const validLaps = laps.filter(l => l.lap_duration && l.lap_duration > 0)
  if (validLaps.length === 0) return <p style={{ color: '#a1a1aa' }}>No valid lap times.</p>

  // Split laps into those with all three sectors vs those without
  const sectorLaps = validLaps.filter(
    l => l.duration_sector_1 != null && l.duration_sector_2 != null && l.duration_sector_3 != null
  )
  const fallbackLaps = validLaps.filter(
    l => !(l.duration_sector_1 != null && l.duration_sector_2 != null && l.duration_sector_3 != null)
  )

  // Stacked sector traces — base is set manually so we don't rely on barmode stack
  const s1Trace = {
    type: 'bar',
    name: 'S1',
    x: sectorLaps.map(l => l.lap_number),
    y: sectorLaps.map(l => l.duration_sector_1),
    base: sectorLaps.map(() => 0),
    marker: { color: SECTOR_COLOURS.S1 },
    hovertemplate: 'Lap %{x}<br>S1: %{y:.3f}s<extra></extra>',
    legendgroup: 'sectors',
    xaxis: 'x', yaxis: 'y',
  }

  const s2Trace = {
    type: 'bar',
    name: 'S2',
    x: sectorLaps.map(l => l.lap_number),
    y: sectorLaps.map(l => l.duration_sector_2),
    base: sectorLaps.map(l => l.duration_sector_1),
    marker: { color: SECTOR_COLOURS.S2 },
    hovertemplate: 'Lap %{x}<br>S2: %{y:.3f}s<extra></extra>',
    legendgroup: 'sectors',
    xaxis: 'x', yaxis: 'y',
  }

  const s3Trace = {
    type: 'bar',
    name: 'S3',
    x: sectorLaps.map(l => l.lap_number),
    y: sectorLaps.map(l => l.duration_sector_3),
    base: sectorLaps.map(l => l.duration_sector_1 + l.duration_sector_2),
    marker: { color: SECTOR_COLOURS.S3 },
    hovertemplate: 'Lap %{x}<br>S3: %{y:.3f}s<extra></extra>',
    legendgroup: 'sectors',
    xaxis: 'x', yaxis: 'y',
  }

  // Fallback trace for laps without sector splits
  const fallbackTrace = {
    type: 'bar',
    name: 'Lap Time',
    showlegend: fallbackLaps.length > 0,
    x: fallbackLaps.map(l => l.lap_number),
    y: fallbackLaps.map(l => l.lap_duration),
    base: fallbackLaps.map(() => 0),
    marker: { color: 'rgba(161,161,170,0.4)' },
    hovertemplate: 'Lap %{x}<br>%{y:.3f}s<extra></extra>',
    xaxis: 'x', yaxis: 'y',
  }

  const maxLap = Math.max(...validLaps.map(l => l.lap_number))
  const maxTime = Math.max(...validLaps.map(l => l.lap_duration))
  const yMax = maxTime * 1.05

  const shapes = []
  const annotations = []

  // ── Compound strip bar traces (subplot on y2/x2) ──────────────────────────
  // Each stint is a single wide bar with makeStripMarker() — gives proper
  // hatching for used tyres, matching TyreStrategyPlot exactly.
  const stripTraces = []
  const seenCompoundKeys = new Set()
  const coveredRanges = []   // track which lap ranges have stint data

  if (stints) {
    for (let i = 0; i < stints.length; i++) {
      const stint = stints[i]
      const compound = (stint.compound ?? 'UNKNOWN').toUpperCase()
      const isFresh = (stint.tyre_age_at_start ?? 0) === 0
      const colour = compoundColour(compound)
      const lapStart = stint.lap_start ?? 1
      const lapEnd = stint.lap_end
        ?? (stints[i + 1]?.lap_start != null ? stints[i + 1].lap_start - 1 : maxLap)
      if (lapEnd == null || lapEnd < lapStart) continue

      coveredRanges.push({ lapStart, lapEnd })

      const key = compound + (isFresh ? '_fresh' : '_used')
      const isFirst = !seenCompoundKeys.has(key)
      if (isFirst) seenCompoundKeys.add(key)

      const midLap = (lapStart + lapEnd) / 2
      const width = lapEnd - lapStart + 1

      stripTraces.push({
        type: 'bar',
        name: (compound === 'UNKNOWN' ? '?' : capitalize(compound)) + (isFresh ? '' : ' (used)'),
        x: [midLap],
        y: [1],
        width: [width],
        base: [0],
        marker: makeStripMarker(colour, isFresh),
        xaxis: 'x2',
        yaxis: 'y2',
        legendgroup: key,
        showlegend: isFirst,
        hovertemplate: `${compound === 'UNKNOWN' ? '?' : capitalize(compound)}${isFresh ? '' : ' (used)'}<br>Laps ${lapStart}–${lapEnd}<extra></extra>`,
      })
    }
  }

  // ── Fill uncovered lap ranges with a grey '?' placeholder ────────────────
  // Handles sessions (e.g. qualifying Q1, sprint) where stint data is absent.
  {
    const minLap = Math.min(...validLaps.map(l => l.lap_number))
    const sorted = [...coveredRanges].sort((a, b) => a.lapStart - b.lapStart)
    const gaps = []
    let cursor = minLap
    for (const { lapStart, lapEnd } of sorted) {
      if (lapStart > cursor) gaps.push({ lapStart: cursor, lapEnd: lapStart - 1 })
      cursor = Math.max(cursor, lapEnd + 1)
    }
    if (cursor <= maxLap) gaps.push({ lapStart: cursor, lapEnd: maxLap })

    for (const { lapStart, lapEnd } of gaps) {
      stripTraces.push({
        type: 'bar',
        name: '?',
        x: [(lapStart + lapEnd) / 2],
        y: [1],
        width: [lapEnd - lapStart + 1],
        base: [0],
        marker: {
          color: 'rgba(255,255,255,0.65)',
          pattern: { shape: '/', bgcolor: '#444444', size: 8 },
          line: { color: 'rgba(0,0,0,0.15)', width: 0.5 },
        },
        xaxis: 'x2',
        yaxis: 'y2',
        legendgroup: 'UNKNOWN_gap',
        showlegend: !seenCompoundKeys.has('UNKNOWN_gap') && (() => { seenCompoundKeys.add('UNKNOWN_gap'); return true })(),
        hovertemplate: `?<br>Laps ${lapStart}–${lapEnd}<extra></extra>`,
      })
    }
  }

  // ── SC/VSC shaded bands (main chart area only) ────────────────────────────
  if (safetyCarPeriods) {
    for (const p of safetyCarPeriods) {
      if (p.lapStart == null || p.lapEnd == null) continue
      const isSC = p.type === 'SC'
      shapes.push({
        type: 'rect',
        xref: 'x', yref: 'paper',
        x0: p.lapStart - 0.5, x1: p.lapEnd + 0.5,
        y0: MAIN_DOMAIN[0], y1: MAIN_DOMAIN[1],
        fillcolor: isSC ? 'rgba(255,200,0,0.22)' : 'rgba(180,180,180,0.22)',
        line: { color: isSC ? 'rgba(200,150,0,0.5)' : 'rgba(120,120,120,0.5)', width: 1 },
      })
      annotations.push({
        x: (p.lapStart + p.lapEnd) / 2,
        y: MAIN_DOMAIN[1] + 0.01,
        xref: 'x', yref: 'paper',
        text: p.type,
        showarrow: false,
        font: { size: 9, color: isSC ? '#997700' : '#777' },
        xanchor: 'center', yanchor: 'bottom',
      })
    }
  }

  // ── Q1/Q2/Q3 phase boundary lines and labels ─────────────────────────────
  if (phaseEvents && phaseEvents.length > 0) {
    const phasedLaps = assignPhases(laps, phaseEvents)
    const phaseStarts = {}
    const phaseEnd = {}
    for (const lap of phasedLaps) {
      if (!lap._phase) continue
      if (!(lap._phase in phaseStarts) || lap.lap_number < phaseStarts[lap._phase]) {
        phaseStarts[lap._phase] = lap.lap_number
      }
      if (!(lap._phase in phaseEnd) || lap.lap_number > phaseEnd[lap._phase]) {
        phaseEnd[lap._phase] = lap.lap_number
      }
    }
    for (const [phase, lapNum] of Object.entries(phaseStarts)) {
      if (lapNum <= Math.min(...Object.values(phaseStarts))) continue
      shapes.push({
        type: 'line',
        xref: 'x', yref: 'paper',
        x0: lapNum - 0.5, x1: lapNum - 0.5,
        y0: MAIN_DOMAIN[0], y1: MAIN_DOMAIN[1],
        line: { color: '#71717a', width: 1.5, dash: 'dot' },
      })
    }
    for (const phase of ['Q1', 'Q2', 'Q3']) {
      if (!(phase in phaseStarts)) continue
      const midLap = (phaseStarts[phase] + phaseEnd[phase]) / 2
      annotations.push({
        x: midLap,
        y: MAIN_DOMAIN[1] + 0.01,
        xref: 'x', yref: 'paper',
        text: phase,
        showarrow: false,
        font: { size: 10, color: '#71717a' },
        xanchor: 'center', yanchor: 'bottom',
      })
    }
  }

  // ── Pit stop vertical lines (main chart area only, at end of pit lap) ────
  if (pitStops) {
    for (const pit of pitStops) {
      const x = pit.lap_number + 0.5
      shapes.push({
        type: 'line',
        xref: 'x', yref: 'paper',
        x0: x, x1: x,
        y0: MAIN_DOMAIN[0], y1: MAIN_DOMAIN[1],
        line: { color: 'rgba(255,255,255,0.45)', width: 1.5, dash: 'dot' },
      })
      annotations.push({
        x,
        y: MAIN_DOMAIN[1] + 0.01,
        xref: 'x', yref: 'paper',
        text: 'PIT',
        showarrow: false,
        font: { size: 8, color: '#a1a1aa' },
        xanchor: 'center', yanchor: 'bottom',
      })
    }
  }

  return (
    <Plot
      data={[s1Trace, s2Trace, s3Trace, fallbackTrace, ...stripTraces]}
      layout={{
        barmode: 'overlay',
        xaxis: {
          domain: [0, 1],
          anchor: 'y',
          color: '#a1a1aa',
          gridcolor: 'rgba(255,255,255,0.06)',
          tickfont: { size: 10, color: '#a1a1aa' },
          dtick: 5,
        },
        yaxis: {
          title: { text: 'Lap Time (s)', font: { size: 11, color: '#a1a1aa' } },
          domain: MAIN_DOMAIN,
          range: [0, yMax],
          tickmode: 'linear',
          tick0: 0,
          dtick: 20,
          color: '#a1a1aa',
          gridcolor: 'rgba(255,255,255,0.06)',
          tickfont: { size: 10, color: '#a1a1aa' },
          tickformat: '.0f',
        },
        // Secondary axes for the compound strip subplot
        xaxis2: {
          domain: [0, 1],
          anchor: 'y2',
          matches: 'x',
          visible: false,
        },
        yaxis2: {
          domain: STRIP_DOMAIN,
          range: [0, 1],
          visible: false,
          fixedrange: true,
        },
        legend: {
          orientation: 'h',
          y: -0.08, x: 0.5,
          xanchor: 'center',
          yanchor: 'top',
          font: { size: 10, color: '#a1a1aa' },
          bgcolor: 'transparent',
          entrywidth: 90,
        },
        shapes,
        annotations,
        margin: { l: 55, r: 20, t: 40, b: 50 },
        height,
        paper_bgcolor: '#18181b',
        plot_bgcolor: '#18181b',
        font: { color: '#fafafa', family: 'monospace' },
      }}
      config={{ responsive: true, displayModeBar: false }}
      style={{ width: '100%' }}
    />
  )
}

// ---------------------------------------------------------------------------
// Scatter chart (qualifying mode)
// ---------------------------------------------------------------------------

function ScatterChart({ laps, stints, phaseEvents, height }) {
  if (!laps || laps.length === 0) return <p style={{ color: '#a1a1aa' }}>No lap data.</p>

  function getCompound(lapNumber) {
    const stint = stints?.find(s => {
      const start = s.lap_start ?? 1
      const end = s.lap_end ?? Infinity
      return lapNumber >= start && lapNumber <= end
    })
    return (stint?.compound ?? 'UNKNOWN').toUpperCase()
  }

  const byCompound = {}
  for (const lap of laps) {
    if (!lap.lap_duration || lap.lap_duration <= 0) continue
    const compound = getCompound(lap.lap_number)
    if (!byCompound[compound]) byCompound[compound] = { x: [], y: [] }
    byCompound[compound].x.push(lap.lap_number)
    byCompound[compound].y.push(lap.lap_duration)
  }

  const traces = Object.entries(byCompound).map(([compound, d]) => ({
    type: 'scatter',
    mode: 'markers',
    name: compound.slice(0, 1) + compound.slice(1).toLowerCase(),
    x: d.x,
    y: d.y,
    hovertemplate: 'Lap %{x}<br>%{y:.3f}s<extra></extra>',
    marker: { color: compoundColour(compound), size: 7 },
  }))

  // Phase boundary lines and labels
  const shapes = []
  const annotations = []
  if (phaseEvents && phaseEvents.length > 0) {
    const phasedLaps = assignPhases(laps, phaseEvents)
    const phaseStarts = {}
    for (const lap of phasedLaps) {
      if (lap._phase && !(lap._phase in phaseStarts)) phaseStarts[lap._phase] = lap.lap_number
    }
    for (const [phase, lapNum] of Object.entries(phaseStarts)) {
      if (lapNum <= 1) continue
      shapes.push({
        type: 'line',
        x0: lapNum - 0.5, x1: lapNum - 0.5,
        y0: 0, y1: 1, yref: 'paper',
        line: { color: PHASE_COLOURS[phase] ?? '#888', width: 1, dash: 'dot' },
      })
    }
    for (const phase of ['Q1', 'Q2', 'Q3']) {
      const phaseLaps = phasedLaps.filter(l => l._phase === phase)
      if (!phaseLaps.length) continue
      const midLap = phaseLaps[Math.floor(phaseLaps.length / 2)].lap_number
      annotations.push({
        x: midLap, y: 1.04, xref: 'x', yref: 'paper',
        text: phase, showarrow: false,
        font: { size: 10, color: PHASE_COLOURS[phase] ?? '#888' },
      })
    }
  }

  return (
    <Plot
      data={traces}
      layout={{
        xaxis: {
          title: { text: 'Lap', font: { size: 11, color: '#a1a1aa' } },
          color: '#a1a1aa',
          gridcolor: 'rgba(255,255,255,0.06)',
          tickfont: { size: 10, color: '#a1a1aa' },
          dtick: 5,
        },
        yaxis: {
          title: { text: 'Lap Time (s)', font: { size: 11, color: '#a1a1aa' } },
          color: '#a1a1aa',
          gridcolor: 'rgba(255,255,255,0.06)',
          tickfont: { size: 10, color: '#a1a1aa' },
          tickformat: '.3f',
        },
        legend: {
          orientation: 'h',
          y: 1.08, x: 0,
          font: { size: 10, color: '#a1a1aa' },
          bgcolor: 'transparent',
        },
        shapes,
        annotations,
        margin: { l: 55, r: 20, t: 30, b: 45 },
        height,
        paper_bgcolor: '#18181b',
        plot_bgcolor: '#18181b',
        font: { color: '#fafafa', family: 'monospace' },
      }}
      config={{ responsive: true, displayModeBar: false }}
      style={{ width: '100%' }}
    />
  )
}

// ---------------------------------------------------------------------------
// Public component
// ---------------------------------------------------------------------------

/**
 * Lap times chart — two variants:
 *
 * mode='bars' (default, race):
 *   Stacked bar chart of S1/S2/S3 sector times per lap.
 *   Accepts stints (compound background shading), pitStops (vertical lines),
 *   and safetyCarPeriods (SC/VSC shaded bands).
 *
 * mode='scatter' (qualifying):
 *   Scatter plot coloured by compound.
 *   Accepts phaseEvents for Q1/Q2/Q3 boundary lines.
 */
export default function LapTimesChart({
  laps,
  stints,
  pitStops,
  safetyCarPeriods,
  phaseEvents,
  mode = 'bars',
  height = 340,
}) {
  if (mode === 'scatter') {
    return <ScatterChart laps={laps} stints={stints} phaseEvents={phaseEvents} height={height} />
  }
  return (
    <SectorBarChart
      laps={laps}
      stints={stints}
      pitStops={pitStops}
      safetyCarPeriods={safetyCarPeriods}
      phaseEvents={phaseEvents}
      height={height}
    />
  )
}
