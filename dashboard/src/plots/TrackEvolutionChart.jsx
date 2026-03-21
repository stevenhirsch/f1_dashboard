import Plot from 'react-plotly.js'
import { assignPhases } from '../utils/qualifying'

export default function TrackEvolutionChart({ laps, phaseEvents, driverMap }) {
  if (!laps || laps.length === 0) return <p style={{ color: '#a1a1aa' }}>No lap data.</p>

  const phasedLaps = assignPhases(laps, phaseEvents)
  const validLaps = phasedLaps.filter(l => l.lap_duration && l.lap_duration > 0)

  // Group laps by driver
  const byDriver = {}
  for (const lap of validLaps) {
    const dn = lap.driver_number
    if (!byDriver[dn]) byDriver[dn] = []
    byDriver[dn].push(lap)
  }

  // Build one scatter trace per driver
  const traces = Object.entries(byDriver).map(([dn, driverLaps]) => {
    const driver = driverMap[Number(dn)] ?? {}
    const colour = `#${(driver.team_colour ?? '888888').replace('#', '')}`
    return {
      type: 'scatter',
      mode: 'lines+markers',
      name: driver.name_acronym ?? dn,
      x: driverLaps.map(l => l.lap_number),
      y: driverLaps.map(l => l.lap_duration),
      line: { color: colour, width: 1.5 },
      marker: { color: colour, size: 4, opacity: 0.9 },
      hovertemplate:
        `<b>${driver.name_acronym ?? dn}</b><br>` +
        'Lap %{x} — %{y:.3f}s<extra></extra>',
    }
  })

  // Vertical lines at phase boundaries (first lap with that phase)
  const shapes = []
  const annotations = []
  for (const phase of ['Q2', 'Q3']) {
    const firstLap = validLaps
      .filter(l => l._phase === phase)
      .reduce((min, l) => (min == null || l.lap_number < min ? l.lap_number : min), null)
    if (firstLap != null) {
      shapes.push({
        type: 'line',
        xref: 'x',
        yref: 'paper',
        x0: firstLap - 0.5,
        x1: firstLap - 0.5,
        y0: 0,
        y1: 1,
        line: { color: 'rgba(161,161,170,0.4)', width: 1, dash: 'dash' },
      })
      annotations.push({
        x: firstLap - 0.5,
        y: 1.01,
        xref: 'x',
        yref: 'paper',
        text: phase,
        showarrow: false,
        font: { size: 9, color: '#a1a1aa' },
        xanchor: 'left',
        yanchor: 'bottom',
      })
    }
  }

  return (
    <Plot
      data={traces}
      layout={{
        xaxis: {
          title: { text: 'Lap Number', font: { color: '#a1a1aa' } },
          color: '#a1a1aa',
          gridcolor: 'rgba(255,255,255,0.07)',
          tickfont: { color: '#a1a1aa' },
        },
        yaxis: {
          title: { text: 'Lap Time (s)', font: { color: '#a1a1aa' } },
          color: '#a1a1aa',
          gridcolor: 'rgba(255,255,255,0.07)',
          tickfont: { color: '#a1a1aa' },
          autorange: true,
        },
        shapes,
        annotations,
        showlegend: false,
        margin: { l: 60, r: 20, t: 30, b: 50 },
        height: 380,
        paper_bgcolor: '#18181b',
        plot_bgcolor: '#18181b',
        font: { color: '#fafafa', family: 'monospace' },
      }}
      config={{ responsive: true, displayModeBar: false }}
      style={{ width: '100%' }}
    />
  )
}
