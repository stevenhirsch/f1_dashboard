import Plot from '../components/ResponsivePlot'

const THEME = {
  bg: '#09090b',
  surface: '#18181b',
  border: 'rgba(255,255,255,0.08)',
  text: '#fafafa',
  muted: '#a1a1aa',
  gridColor: 'rgba(255,255,255,0.06)',
}

export default function DriverPointsTrendChart({ driverSeries, roundLabels, isMobile }) {
  if (!driverSeries?.length) return null

  // Sort series by final points descending so legend order matches standings
  const sorted = [...driverSeries].sort(
    (a, b) => (b.points[b.points.length - 1] ?? 0) - (a.points[a.points.length - 1] ?? 0)
  )

  const maxRound = Math.max(...sorted.flatMap(s => s.rounds), 0)
  // Tick labels only on actual race rounds (1..maxRound), not round 0
  const tickVals = Array.from({ length: maxRound }, (_, i) => i + 1)
  const tickText = tickVals.map(r => roundLabels[r] ?? `R${r}`)

  const seenColours = new Set()
  const traces = sorted.map(s => {
    const dashed = seenColours.has(s.team_colour)
    seenColours.add(s.team_colour)
    return {
      type: 'scatter',
      mode: 'lines+markers',
      name: s.name_acronym || s.full_name,
      x: [0, ...s.rounds],
      y: [0, ...s.points],
      line: { color: s.team_colour, width: 2, dash: dashed ? 'dash' : 'solid' },
      marker: { color: s.team_colour, size: isMobile ? 4 : 5 },
      hovertemplate: `<b>%{fullData.name}</b><br>Round %{x}: %{y} pts<extra></extra>`,
    }
  })

  const layout = {
    paper_bgcolor: THEME.bg,
    plot_bgcolor: THEME.bg,
    font: { family: 'monospace', color: THEME.text, size: isMobile ? 10 : 11 },
    margin: { t: 10, r: 20, b: isMobile ? 80 : 60, l: 50 },
    xaxis: {
      title: { text: 'Round', font: { size: isMobile ? 10 : 11 } },
      tickmode: 'array',
      tickvals: tickVals,
      ticktext: tickText,
      tickangle: isMobile ? -45 : -30,
      tickfont: { size: isMobile ? 8 : 10 },
      gridcolor: THEME.gridColor,
      zerolinecolor: THEME.border,
      range: [-0.2, maxRound + 0.3],
    },
    yaxis: {
      title: { text: 'Points', font: { size: isMobile ? 10 : 11 } },
      gridcolor: THEME.gridColor,
      zerolinecolor: THEME.border,
      rangemode: 'tozero',
    },
    legend: {
      orientation: 'h',
      x: 0,
      y: isMobile ? -0.35 : -0.25,
      font: { size: isMobile ? 9 : 10 },
      bgcolor: 'transparent',
      traceorder: 'normal',
    },
    hovermode: 'closest',
    showlegend: true,
  }

  return (
    <Plot
      data={traces}
      layout={layout}
      config={{ responsive: true, displayModeBar: false }}
      style={{ width: '100%', height: isMobile ? 340 : 420 }}
    />
  )
}
