import Plot from '../components/ResponsivePlot'
import { useRaceLapDistribution } from '../hooks/useRaceLapDistribution'
import { COMPOUND_COLOURS, COMPOUND_ORDER } from '../utils/compounds'

function formatLapTime(seconds) {
  if (seconds == null || !isFinite(seconds)) return '—'
  const m = Math.floor(seconds / 60)
  const s = (seconds % 60).toFixed(3)
  return `${m}:${String(s).padStart(6, '0')}`
}

// Short format for mobile x-axis ticks — drops milliseconds to save space
function formatLapTimeShort(seconds) {
  if (seconds == null || !isFinite(seconds)) return '—'
  const m = Math.floor(seconds / 60)
  const s = String(Math.floor(seconds % 60)).padStart(2, '0')
  return `${m}:${s}`
}

function getCompound(driverNumber, lapNumber, stintsByDriver) {
  const stints = stintsByDriver[driverNumber] ?? []
  const stint = stints.find(
    s => lapNumber >= (s.lap_start ?? 1) && lapNumber <= (s.lap_end ?? Infinity)
  )
  return (stint?.compound ?? 'UNKNOWN').toUpperCase()
}

// Deterministic jitter: stable across renders, unique per driver+lap combination
function jitter(driverNumber, lapNumber, spread = 0.25) {
  const hash = ((driverNumber * 2654435761) ^ (lapNumber * 2246822519)) >>> 0
  return (hash % 1000) / 1000 * spread - spread / 2
}

export default function LapDistributionPlot({ sessionKey, isMobile }) {
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
  const lapCutoff = sessionFastest * 1.15

  const lapsByDriver = {}
  for (const lap of laps) {
    if (!lap.lap_duration || lap.lap_duration <= 0) continue
    if (lap.is_pit_out_lap) continue
    if (neutralizedSet.has(`${lap.driver_number}-${lap.lap_number}`)) continue
    if (lap.lap_duration > lapCutoff) continue
    if (!lapsByDriver[lap.driver_number]) lapsByDriver[lap.driver_number] = []
    lapsByDriver[lap.driver_number].push(lap)
  }

  // Assign each driver a numeric y position (0 = P1, rendered at top via reversed y range)
  const n = orderedDrivers.length
  const driverIndex = Object.fromEntries(orderedDrivers.map((r, i) => [r.driver_number, i]))

  // Horizontal violin traces — team-coloured, one per driver
  const violinTraces = orderedDrivers.map(r => {
    const driver = driverMap[r.driver_number] ?? {}
    const rawColour = driver.team_colour ?? '888888'
    const colour = `#${rawColour.replace('#', '')}`
    const driverLaps = lapsByDriver[r.driver_number] ?? []
    const yi = driverIndex[r.driver_number]

    return {
      type: 'violin',
      orientation: 'h',
      y0: yi,
      x: driverLaps.map(l => l.lap_duration),
      width: isMobile ? 0.72 : 0.78,
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

  // Scatter traces — compound-coloured dots, jittered vertically within each driver row
  const jitterSpread = isMobile ? 0.2 : 0.25
  const compoundData = {}
  for (const r of orderedDrivers) {
    const driver = driverMap[r.driver_number] ?? {}
    const acronym = driver.name_acronym ?? String(r.driver_number)
    const yi = driverIndex[r.driver_number]
    const driverLaps = lapsByDriver[r.driver_number] ?? []

    for (const lap of driverLaps) {
      const compound = getCompound(r.driver_number, lap.lap_number, stintsByDriver)
      if (!compoundData[compound]) compoundData[compound] = { x: [], y: [], text: [] }
      compoundData[compound].x.push(lap.lap_duration)
      compoundData[compound].y.push(yi + jitter(r.driver_number, lap.lap_number, jitterSpread))
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
        size: isMobile ? 4 : 5,
        opacity: 0.9,
        line: { color: 'rgba(0,0,0,0.35)', width: 0.5 },
      },
    }))

  // X-axis: lap time range and ticks every 5s
  const allX = Object.values(lapsByDriver).flatMap(ls => ls.map(l => l.lap_duration))
  const xMin = Math.min(...allX)
  const xMax = Math.max(...allX)
  const xPad = (xMax - xMin) * 0.05
  const xRange = [xMin - xPad, xMax + xPad]

  const tickInterval = 5
  const tickStart = Math.ceil((xMin - xPad) / tickInterval) * tickInterval
  const tickEnd = Math.floor((xMax + xPad) / tickInterval) * tickInterval
  const xTickVals = []
  for (let t = tickStart; t <= tickEnd; t += tickInterval) xTickVals.push(t)
  const xTickText = xTickVals.map(t => isMobile ? formatLapTimeShort(t) : formatLapTime(t))

  // Y-axis: driver acronyms, P1 at top
  const yTickVals = orderedDrivers.map((_, i) => i)
  const yTickText = orderedDrivers.map(r => {
    const driver = driverMap[r.driver_number] ?? {}
    return driver.name_acronym ?? String(r.driver_number)
  })

  const chartHeight = isMobile
    ? Math.max(420, n * 23 + 70)
    : Math.max(500, n * 27 + 80)

  return (
    <Plot
      data={[...violinTraces, ...scatterTraces]}
      layout={{
        xaxis: {
          title: { text: 'Lap Time', font: { size: isMobile ? 10 : 11, color: '#a1a1aa' } },
          range: xRange,
          tickvals: xTickVals,
          ticktext: xTickText,
          tickfont: { size: isMobile ? 9 : 10, color: '#a1a1aa', family: 'monospace' },
          color: '#a1a1aa',
          gridcolor: 'rgba(255,255,255,0.06)',
          fixedrange: true,
        },
        yaxis: {
          tickvals: yTickVals,
          ticktext: yTickText,
          range: [n - 0.4, -0.6],  // reversed so P1 (index 0) sits at top
          tickfont: { size: isMobile ? 9 : 10, color: '#fafafa', family: 'monospace' },
          color: '#a1a1aa',
          gridcolor: 'rgba(255,255,255,0.05)',
          fixedrange: true,
        },
        legend: {
          orientation: 'h',
          x: 0,
          y: 1.04,
          font: { size: isMobile ? 9 : 11, color: '#a1a1aa' },
          bgcolor: 'transparent',
          title: { text: 'Tyre Compound  ', font: { size: isMobile ? 9 : 10, color: '#71717a' } },
        },
        margin: isMobile
          ? { l: 40, r: 10, t: 42, b: 50 }
          : { l: 45, r: 20, t: 48, b: 60 },
        height: chartHeight,
        paper_bgcolor: '#18181b',
        plot_bgcolor: '#18181b',
        font: { color: '#fafafa', family: 'monospace' },
      }}
      config={{ responsive: true, displayModeBar: false, scrollZoom: false }}
      style={{ width: '100%' }}
    />
  )
}
