import Plot from '../components/ResponsivePlot'
import { useIntervals } from '../hooks/useIntervals'
import { useMobile } from '../hooks/useMobile'

export default function GapEvolutionChart({ sessionKey }) {
  const isMobile = useMobile()
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
        title: { text: 'Gap to Leader', font: { size: isMobile ? 12 : 14, color: '#fafafa' } },
        xaxis: { title: 'Time', type: 'date', color: '#a1a1aa', tickfont: { color: '#a1a1aa' } },
        yaxis: { title: 'Gap (s)', autorange: 'reversed', color: '#a1a1aa', tickfont: { color: '#a1a1aa' } },
        legend: { orientation: 'h', y: -0.2, font: { size: isMobile ? 9 : 10, color: '#fafafa' } },
        margin: isMobile
          ? { l: 45, r: 10, t: 35, b: 65 }
          : { l: 60, r: 20, t: 40, b: 80 },
        height: isMobile ? 300 : 500,
        paper_bgcolor: '#18181b',
        plot_bgcolor: '#18181b',
        font: { color: '#fafafa', family: 'monospace' },
      }}
      config={{ responsive: true, displayModeBar: false }}
      style={{ width: '100%' }}
    />
  )
}
