import Plot from 'react-plotly.js'
import { useRaceLapDistribution } from '../hooks/useRaceLapDistribution'
import { COMPOUND_COLOURS, COMPOUND_ORDER } from '../utils/compounds'

function formatLapTime(seconds) {
  if (seconds == null || !isFinite(seconds)) return '—'
  const m = Math.floor(seconds / 60)
  const s = (seconds % 60).toFixed(3)
  return `${m}:${String(s).padStart(6, '0')}`
}

function getCompound(driverNumber, lapNumber, stintsByDriver) {
  const stints = stintsByDriver[driverNumber] ?? []
  const stint = stints.find(
    s => lapNumber >= (s.lap_start ?? 1) && lapNumber <= (s.lap_end ?? Infinity)
  )
  return (stint?.compound ?? 'UNKNOWN').toUpperCase()
}

// Deterministic jitter: stable across renders, unique per driver+lap combination
function jitter(driverNumber, lapNumber, spread = 0.3) {
  const hash = ((driverNumber * 2654435761) ^ (lapNumber * 2246822519)) >>> 0
  return (hash % 1000) / 1000 * spread - spread / 2
}

export default function LapDistributionPlot({ sessionKey }) {
  const { data, loading } = useRaceLapDistribution(sessionKey)

  if (loading) return <p style={{ color: '#a1a1aa' }}>Loading lap distribution…</p>
  if (!data) return null

  const { laps, stints, results, drivers, lapMetrics } = data

  // Build lookup indexes
  const stintsByDriver = {}
  for (const s of stints) {
    if (!stintsByDriver[s.driver_number]) stintsByDriver[s.driver_number] = []
    stintsByDriver[s.driver_number].push(s)
  }

  const driverMap = Object.fromEntries(drivers.map(d => [d.driver_number, d]))

  // is_neutralized keyed by "driver_number-lap_number"
  const neutralizedSet = new Set()
  for (const m of lapMetrics) {
    if (m.is_neutralized) neutralizedSet.add(`${m.driver_number}-${m.lap_number}`)
  }

  // Order drivers by finish position; exclude DNS (no racing laps)
  const orderedDrivers = results
    .filter(r => !r.dns)
    .sort((a, b) => (a.position ?? 999) - (b.position ?? 999))

  // Compute session fastest clean lap for outlier cutoff
  const allCleanLaps = laps.filter(l =>
    l.lap_duration > 0 &&
    !l.is_pit_out_lap &&
    !neutralizedSet.has(`${l.driver_number}-${l.lap_number}`)
  )
  if (allCleanLaps.length === 0) return <p style={{ color: '#a1a1aa' }}>No lap data.</p>
  const sessionFastest = Math.min(...allCleanLaps.map(l => l.lap_duration))
  // 1.15× catches pit-in laps while keeping all racing laps
  const lapCutoff = sessionFastest * 1.15

  // Group filtered laps by driver, tagging each with its driver index for x placement
  const lapsByDriver = {}
  for (const lap of laps) {
    if (!lap.lap_duration || lap.lap_duration <= 0) continue
    if (lap.is_pit_out_lap) continue
    if (neutralizedSet.has(`${lap.driver_number}-${lap.lap_number}`)) continue
    if (lap.lap_duration > lapCutoff) continue
    if (!lapsByDriver[lap.driver_number]) lapsByDriver[lap.driver_number] = []
    lapsByDriver[lap.driver_number].push(lap)
  }

  // Assign each driver a numeric x position (0, 1, 2, …)
  const driverIndex = Object.fromEntries(orderedDrivers.map((r, i) => [r.driver_number, i]))

  // Violin traces — one per driver at its numeric x0, team-coloured, no points
  const violinTraces = orderedDrivers.map(r => {
    const driver = driverMap[r.driver_number] ?? {}
    const rawColour = driver.team_colour ?? '888888'
    const colour = `#${rawColour.replace('#', '')}`
    const driverLaps = lapsByDriver[r.driver_number] ?? []
    const xi = driverIndex[r.driver_number]

    return {
      type: 'violin',
      x0: xi,
      y: driverLaps.map(l => l.lap_duration),
      width: 0.75,
      fillcolor: colour + 'aa', // ~67% opacity
      line: { color: colour, width: 1.5 },
      points: false,
      spanmode: 'soft',
      showlegend: false,
      hoverinfo: 'skip',
      box: { visible: false },
      meanline: { visible: false },
    }
  })

  // Scatter traces — one per compound, x positions jittered around each driver's index
  const compoundData = {}
  for (const r of orderedDrivers) {
    const driver = driverMap[r.driver_number] ?? {}
    const acronym = driver.name_acronym ?? String(r.driver_number)
    const xi = driverIndex[r.driver_number]
    const driverLaps = lapsByDriver[r.driver_number] ?? []

    for (const lap of driverLaps) {
      const compound = getCompound(r.driver_number, lap.lap_number, stintsByDriver)
      if (!compoundData[compound]) compoundData[compound] = { x: [], y: [], text: [] }
      compoundData[compound].x.push(xi + jitter(r.driver_number, lap.lap_number))
      compoundData[compound].y.push(lap.lap_duration)
      compoundData[compound].text.push(
        `<b>${acronym}</b> · Lap ${lap.lap_number}<br>${formatLapTime(lap.lap_duration)}`
      )
    }
  }

  const scatterTraces = COMPOUND_ORDER
    .filter(c => compoundData[c]?.x.length > 0)
    .map(compound => ({
      type: 'scatter',
      mode: 'markers',
      name: compound[0] + compound.slice(1).toLowerCase(),
      x: compoundData[compound].x,
      y: compoundData[compound].y,
      text: compoundData[compound].text,
      hovertemplate: '%{text}<extra></extra>',
      marker: {
        color: COMPOUND_COLOURS[compound] ?? '#888888',
        size: 6,
        opacity: 0.9,
        line: { color: 'rgba(0,0,0,0.35)', width: 0.5 },
      },
    }))

  // X-axis: numeric tickvals at each driver index, custom labels with best lap + acronym
  const n = orderedDrivers.length
  const tickvals = orderedDrivers.map((_, i) => i)
  const ticktext = orderedDrivers.map(r => {
    const driver = driverMap[r.driver_number] ?? {}
    const acronym = driver.name_acronym ?? String(r.driver_number)
    const driverLaps = lapsByDriver[r.driver_number] ?? []
    const best = driverLaps.length > 0 ? Math.min(...driverLaps.map(l => l.lap_duration)) : null
    return `${formatLapTime(best)}<br><b>${acronym}</b>`
  })

  // Y-axis range and ticks every 5s formatted as M:SS.000
  const allY = Object.values(lapsByDriver).flatMap(ls => ls.map(l => l.lap_duration))
  const yMin = Math.min(...allY)
  const yMax = Math.max(...allY)
  const yPad = (yMax - yMin) * 0.05
  const yRange = [yMin - yPad, yMax + yPad]

  const tickInterval = 5
  const tickStart = Math.ceil((yMin - yPad) / tickInterval) * tickInterval
  const tickEnd = Math.floor((yMax + yPad) / tickInterval) * tickInterval
  const yTickVals = []
  for (let t = tickStart; t <= tickEnd; t += tickInterval) yTickVals.push(t)
  const yTickText = yTickVals.map(t => formatLapTime(t))

  return (
    <Plot
      data={[...violinTraces, ...scatterTraces]}
      layout={{
        xaxis: {
          title: { text: 'Driver', font: { size: 11, color: '#a1a1aa' } },
          range: [-0.6, n - 0.4],
          tickvals,
          ticktext,
          tickfont: { size: 10, color: '#a1a1aa', family: 'monospace' },
          color: '#a1a1aa',
          gridcolor: 'rgba(0,0,0,0)',
          fixedrange: true,
        },
        yaxis: {
          title: { text: 'Lap Time', font: { size: 11, color: '#a1a1aa' } },
          range: yRange,
          tickvals: yTickVals,
          ticktext: yTickText,
          color: '#a1a1aa',
          gridcolor: 'rgba(255,255,255,0.06)',
          tickfont: { size: 10, color: '#a1a1aa', family: 'monospace' },
          fixedrange: true,
        },
        legend: {
          orientation: 'h',
          y: 1.06,
          x: 0,
          font: { size: 11, color: '#a1a1aa' },
          bgcolor: 'transparent',
          title: { text: 'Tyre Compound  ', font: { size: 10, color: '#71717a' } },
        },
        margin: { l: 90, r: 20, t: 30, b: 80 },
        height: 480,
        paper_bgcolor: '#18181b',
        plot_bgcolor: '#18181b',
        font: { color: '#fafafa', family: 'monospace' },
      }}
      config={{ responsive: true, displayModeBar: false, scrollZoom: false }}
      style={{ width: '100%' }}
    />
  )
}
