import Plot from 'react-plotly.js'

// Red (slow) → yellow → green (fast)
const SPEED_COLORSCALE = [
  [0,   '#dc2626'],
  [0.5, '#facc15'],
  [1,   '#22c55e'],
]

const AXIS_BASE = {
  color: '#a1a1aa',
  gridcolor: 'rgba(255,255,255,0.06)',
  zeroline: false,
  showticklabels: false,
}

function makeTraces(data, lapColours) {
  const lapNums = Object.keys(data)
  const singleLap = lapNums.length === 1

  return lapNums.flatMap(lapNum => {
    const pts = data[lapNum].filter(p => p.x != null && p.y != null)
    if (pts.length === 0) return []
    const colour = lapColours[lapNum]

    if (singleLap) {
      // Speed-gradient: markers coloured by speed
      return [{
        type: 'scatter',
        mode: 'markers',
        name: `Lap ${lapNum}`,
        x: pts.map(p => p.x),
        y: pts.map(p => p.y),
        marker: {
          color: pts.map(p => p.speed ?? 0),
          colorscale: SPEED_COLORSCALE,
          size: 4,
          cmin: 80,
          cmax: 340,
          colorbar: {
            title: { text: 'km/h', side: 'right' },
            thickness: 12,
            len: 0.7,
            tickfont: { size: 9, color: '#a1a1aa' },
            titlefont: { size: 9, color: '#a1a1aa' },
          },
        },
        hovertemplate: 'Speed: %{marker.color} km/h<extra></extra>',
        showlegend: false,
      }]
    }

    // Multi-lap: one coloured line per lap
    return [{
      type: 'scatter',
      mode: 'lines',
      name: `Lap ${lapNum}`,
      x: pts.map(p => p.x),
      y: pts.map(p => p.y),
      line: { color: colour, width: 2 },
      hovertemplate: `Lap ${lapNum}<extra></extra>`,
    }]
  })
}

export default function TrackMapPlot({ data, lapColours, height = 380 }) {
  const hasData = data && Object.values(data).some(pts => pts.length > 0)
  if (!hasData) return null

  const traces = makeTraces(data, lapColours)

  return (
    <Plot
      data={traces}
      layout={{
        xaxis: { ...AXIS_BASE, scaleanchor: 'y', scaleratio: 1 },
        yaxis: { ...AXIS_BASE },
        legend: { font: { size: 10, color: '#a1a1aa' }, bgcolor: 'transparent' },
        margin: { l: 10, r: 10, t: 10, b: 10 },
        height,
        paper_bgcolor: '#18181b',
        plot_bgcolor:  '#18181b',
        font: { color: '#fafafa', family: 'monospace' },
      }}
      config={{ responsive: true, displayModeBar: false }}
      style={{ width: '100%' }}
    />
  )
}
