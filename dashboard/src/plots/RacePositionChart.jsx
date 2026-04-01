import Plot from '../components/ResponsivePlot'
import { useRacePositionHistory } from '../hooks/useRacePositionHistory'

// Binary search: largest index i where arr[i] <= target. Returns -1 if none.
function bisectRight(arr, target) {
  let lo = 0, hi = arr.length - 1
  if (arr.length === 0 || arr[0] > target) return -1
  while (lo < hi) {
    const mid = Math.floor((lo + hi + 1) / 2)
    if (arr[mid] <= target) lo = mid
    else hi = mid - 1
  }
  return lo
}

export default function RacePositionChart({ sessionKey, qualifyingSessionKey, isMobile }) {
  const { data, loading } = useRacePositionHistory(sessionKey, qualifyingSessionKey)

  if (loading) return <p style={{ color: '#a1a1aa' }}>Loading position history…</p>
  if (!data) return null

  const { positionRows, laps, results, drivers, raceControl, startingGrid } = data
  if (positionRows.length === 0) return <p style={{ color: '#a1a1aa' }}>No position data.</p>

  // ── Index building ─────────────────────────────────────────────────────────

  const driverMap = Object.fromEntries(drivers.map(d => [d.driver_number, d]))
  const resultMap = Object.fromEntries(results.map(r => [r.driver_number, r]))
  const gridMap   = Object.fromEntries(startingGrid.map(g => [g.driver_number, g.position]))

  // Per-driver sorted lap start timestamps for timestamp→lap mapping
  const lapStartsByDriver = {}  // driverNumber → [date_ms, ...] (sorted, parallel to lapNumsByDriver)
  const lapNumsByDriver   = {}  // driverNumber → [lap_number, ...]
  for (const lap of laps) {
    if (!lap.date_start) continue
    const ms = new Date(lap.date_start).getTime()
    if (!lapStartsByDriver[lap.driver_number]) {
      lapStartsByDriver[lap.driver_number] = []
      lapNumsByDriver[lap.driver_number]   = []
    }
    lapStartsByDriver[lap.driver_number].push(ms)
    lapNumsByDriver[lap.driver_number].push(lap.lap_number)
  }

  // Reference lap timeline (race leader / driver with most laps) for SC mapping
  const refDriverNum = [...results]
    .sort((a, b) => (b.number_of_laps ?? 0) - (a.number_of_laps ?? 0))[0]?.driver_number
  const refStarts = lapStartsByDriver[refDriverNum] ?? []
  const refNums   = lapNumsByDriver[refDriverNum]   ?? []

  function globalTimestampToLap(ms) {
    const i = bisectRight(refStarts, ms)
    if (i < 0) return 0
    return refNums[i] ?? refNums[refNums.length - 1] ?? 0
  }

  // ── Per-driver lap-indexed position series ──────────────────────────────────

  // Group position rows by driver; for each lap keep the last reading
  const lastPosByDriverLap = {}  // driverNumber → {lapNumber → position}
  for (const row of positionRows) {
    const dn = row.driver_number
    const starts = lapStartsByDriver[dn]
    const nums   = lapNumsByDriver[dn]
    if (!starts || starts.length === 0) continue

    const ms = new Date(row.date).getTime()
    const i  = bisectRight(starts, ms)
    if (i < 0) continue
    const lapNum = nums[i]

    if (!lastPosByDriverLap[dn]) lastPosByDriverLap[dn] = {}
    // Overwrite — later readings in the same lap win, giving end-of-lap position
    lastPosByDriverLap[dn][lapNum] = row.position
  }

  const maxLap = Math.max(...results.map(r => r.number_of_laps ?? 0), ...refNums)

  // Build x/y series per driver (include grid position at lap 0, forward-fill)
  const seriesByDriver = {}
  for (const r of results) {
    if (r.dns) continue
    const dn = r.driver_number
    const lapMax = r.number_of_laps ?? maxLap
    const posMap = lastPosByDriverLap[dn] ?? {}

    const xs = [], ys = []
    let lastPos = gridMap[dn] ?? null

    if (lastPos != null) { xs.push(0); ys.push(lastPos) }

    for (let lap = 1; lap <= lapMax; lap++) {
      const pos = posMap[lap]
      if (pos != null) {
        lastPos = pos
        xs.push(lap); ys.push(pos)
      } else if (lastPos != null) {
        // Forward-fill only up to the driver's last completed lap
        xs.push(lap); ys.push(lastPos)
      }
    }
    if (xs.length > 0) seriesByDriver[dn] = { xs, ys }
  }

  // ── Team assignment: solid = first driver by number, dashed = second ───────

  const teamDrivers = {}  // teamName → [driver_numbers sorted]
  for (const d of drivers) {
    if (!teamDrivers[d.team_name]) teamDrivers[d.team_name] = []
    teamDrivers[d.team_name].push(d.driver_number)
  }
  for (const arr of Object.values(teamDrivers)) arr.sort((a, b) => a - b)

  function isDashed(driverNumber) {
    const team = driverMap[driverNumber]?.team_name
    const arr  = teamDrivers[team] ?? []
    return arr.indexOf(driverNumber) === 1
  }

  // ── SC / VSC periods → lap numbers ─────────────────────────────────────────

  const scPeriods  = []
  const vscPeriods = []

  const sortedRC = [...raceControl].sort((a, b) => new Date(a.date) - new Date(b.date))

  let scStart = null, vscStart = null
  for (const row of sortedRC) {
    if (row.category !== 'SafetyCar') continue
    const msg = row.message ?? ''
    const ms  = new Date(row.date).getTime()
    const isVirtual = msg.includes('VIRTUAL')

    if (!isVirtual) {
      if (msg.includes('DEPLOYED') && scStart == null) {
        scStart = ms
      } else if (msg.includes('IN THIS LAP') && scStart != null) {
        scPeriods.push({
          lapStart: globalTimestampToLap(scStart),
          lapEnd:   globalTimestampToLap(ms),
        })
        scStart = null
      }
    } else {
      if (msg.includes('DEPLOYED') && vscStart == null) {
        vscStart = ms
      } else if (msg.includes('ENDING') && vscStart != null) {
        vscPeriods.push({
          lapStart: globalTimestampToLap(vscStart),
          lapEnd:   globalTimestampToLap(ms),
        })
        vscStart = null
      }
    }
  }

  // ── Plotly traces ───────────────────────────────────────────────────────────

  // Order drivers by finishing position for trace ordering
  const orderedDrivers = [...results]
    .filter(r => !r.dns && seriesByDriver[r.driver_number])
    .sort((a, b) => (a.position ?? 999) - (b.position ?? 999))

  const numDrivers = orderedDrivers.length

  const driverTraces = orderedDrivers.map(r => {
    const dn     = r.driver_number
    const driver = driverMap[dn] ?? {}
    const colour = `#${(driver.team_colour ?? '888888').replace('#', '')}`
    const { xs, ys } = seriesByDriver[dn]

    return {
      type: 'scatter',
      mode: 'lines',
      name: driver.name_acronym ?? String(dn),
      x: xs,
      y: ys,
      line: {
        color: colour,
        width: 1.8,
        dash: isDashed(dn) ? 'dash' : 'solid',
      },
      showlegend: false,
      hovertemplate: `<b>${driver.name_acronym ?? dn}</b><br>Lap %{x}<br>P%{y}<extra></extra>`,
    }
  })

  // Dummy traces for SC / VSC legend entries
  const legendTraces = []
  if (scPeriods.length > 0) {
    legendTraces.push({
      type: 'scatter', x: [null], y: [null], mode: 'markers',
      marker: { symbol: 'square', size: 14, color: 'rgba(218,165,32,0.75)' },
      name: 'SC', showlegend: true,
    })
  }
  if (vscPeriods.length > 0) {
    legendTraces.push({
      type: 'scatter', x: [null], y: [null], mode: 'markers',
      marker: { symbol: 'square', size: 14, color: 'rgba(180,180,100,0.55)' },
      name: 'VSC', showlegend: true,
    })
  }

  // ── Shapes (SC/VSC bands) ──────────────────────────────────────────────────

  const shapes = [
    ...scPeriods.map(p => ({
      type: 'rect', xref: 'x', yref: 'paper',
      x0: p.lapStart - 0.5, x1: p.lapEnd + 0.5,
      y0: 0, y1: 1,
      fillcolor: 'rgba(218,165,32,0.35)',
      line: { width: 0 },
      layer: 'below',
    })),
    ...vscPeriods.map(p => ({
      type: 'rect', xref: 'x', yref: 'paper',
      x0: p.lapStart - 0.5, x1: p.lapEnd + 0.5,
      y0: 0, y1: 1,
      fillcolor: 'rgba(180,180,100,0.25)',
      line: { width: 0 },
      layer: 'below',
    })),
  ]

  // ── Annotations: driver labels on the right ─────────────────────────────────

  const annotations = []
  for (const r of orderedDrivers) {
    const dn     = r.driver_number
    const driver = driverMap[dn] ?? {}
    const colour = `#${(driver.team_colour ?? '888888').replace('#', '')}`
    const { xs, ys } = seriesByDriver[dn]
    const isDNF  = r.dnf || r.dsq

    const labelX = isDNF ? xs[xs.length - 1] : maxLap
    const labelY = ys[ys.length - 1]

    annotations.push({
      x: labelX,
      y: labelY,
      xref: 'x', yref: 'y',
      text: driver.name_acronym ?? String(dn),
      showarrow: false,
      font: { color: colour, size: isMobile ? 8 : 10, family: 'monospace' },
      xanchor: isDNF ? 'center' : 'left',
      yanchor: 'middle',
      xshift: isDNF ? 0 : 5,
      // DNF labels sit inline with a slight vertical offset to reduce overlap
      yshift: isDNF ? -10 : 0,
    })
  }

  // ── Layout ─────────────────────────────────────────────────────────────────

  return (
    <Plot
      data={[...driverTraces, ...legendTraces]}
      layout={{
        xaxis: {
          title: { text: 'Lap', font: { size: isMobile ? 10 : 11, color: '#a1a1aa' } },
          range: [-0.5, maxLap + (isMobile ? 3 : 5)],  // extra room for right-side labels
          color: '#a1a1aa',
          gridcolor: 'rgba(255,255,255,0.06)',
          tickfont: { size: 9, color: '#a1a1aa' },
          dtick: 5,
          tick0: 0,
          fixedrange: true,
        },
        yaxis: {
          title: { text: 'Pos', font: { size: isMobile ? 10 : 11, color: '#a1a1aa' } },
          range: [numDrivers + 0.5, 0.5],  // P1 at top
          tickmode: 'linear',
          tick0: 1,
          dtick: 2,
          color: '#a1a1aa',
          gridcolor: 'rgba(255,255,255,0.06)',
          tickfont: { size: 9, color: '#a1a1aa' },
          fixedrange: true,
        },
        legend: {
          x: 0.99, y: 0.01,
          xanchor: 'right', yanchor: 'bottom',
          font: { size: 10, color: '#a1a1aa' },
          bgcolor: 'rgba(24,24,27,0.85)',
          bordercolor: 'rgba(255,255,255,0.1)',
          borderwidth: 1,
        },
        shapes,
        annotations,
        margin: isMobile
          ? { l: 35, r: 45, t: 15, b: 35 }
          : { l: 50, r: 65, t: 20, b: 50 },
        height: isMobile ? 320 : 560,
        paper_bgcolor: '#18181b',
        plot_bgcolor: '#18181b',
        font: { color: '#fafafa', family: 'monospace' },
      }}
      config={{ responsive: true, displayModeBar: false, scrollZoom: false }}
      style={{ width: '100%' }}
    />
  )
}
