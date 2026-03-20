import Plot from 'react-plotly.js'
import { useIntervals } from '../hooks/useIntervals'

export default function GapEvolutionChart({ sessionKey }) {
  const { data: intervals, loading } = useIntervals(sessionKey)

  if (loading) return <p>Loading gap evolution…</p>
  if (!intervals || intervals.length === 0) return <p>No interval data.</p>

  // Group by driver, sorted by finish position
  const byDriver = {}
  for (const row of intervals) {
    if (!byDriver[row.driver_number]) {
      byDriver[row.driver_number] = {
        name_acronym: row.name_acronym,
        team_colour: row.team_colour,
        position: row.position,
        dates: [],
        gaps: [],
      }
    }
    // Leader has null gap_to_leader → plot as 0
    const gap = row.gap_to_leader == null ? 0 : parseFloat(row.gap_to_leader)
    if (!isNaN(gap) && row.laps_down == null) {
      // Only plot on-lead-lap drivers; lapped drivers are excluded
      byDriver[row.driver_number].dates.push(row.date)
      byDriver[row.driver_number].gaps.push(gap)
    }
  }

  const drivers = Object.values(byDriver).sort((a, b) => a.position - b.position)

  const traces = drivers.map(driver => {
    const colour = driver.team_colour
      ? `#${driver.team_colour.replace('#', '')}`
      : '#888888'
    return {
      type: 'scatter',
      mode: 'lines',
      name: driver.name_acronym,
      x: driver.dates,
      y: driver.gaps,
      line: { color: colour, width: 1.5 },
      hovertemplate: `<b>${driver.name_acronym}</b><br>%{x|%H:%M:%S}<br>Gap: %{y:.3f}s<extra></extra>`,
    }
  })

  return (
    <Plot
      data={traces}
      layout={{
        title: { text: 'Gap to Leader', font: { size: 14 } },
        xaxis: { title: 'Time', type: 'date' },
        yaxis: { title: 'Gap (s)', autorange: 'reversed' },
        legend: { orientation: 'h', y: -0.2, font: { size: 10 } },
        margin: { l: 60, r: 20, t: 40, b: 80 },
        height: 500,
        paper_bgcolor: 'white',
        plot_bgcolor: '#f8f8f8',
      }}
      config={{ responsive: true, displayModeBar: false }}
      style={{ width: '100%' }}
    />
  )
}
