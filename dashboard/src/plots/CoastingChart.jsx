import Plot from '../components/ResponsivePlot'
import { coastingIntervals } from '../utils/telemetry'
import { useMobile } from '../hooks/useMobile'

/**
 * Horizontal Gantt-style chart showing coasting zones (throttle < 1 % AND
 * brake = 0) per lap plotted against distance.  One row per lap.
 *
 * Each lap's full distance is shown as a dim background bar; coasting
 * intervals are overlaid as solid coloured blocks.  A summary stat
 * (total coasting metres and % of lap) is appended to each row label.
 */
export default function CoastingChart({ data, lapColours, height = 160 }) {
  const isMobile = useMobile()
  if (!data || Object.keys(data).length === 0) return null

  const lapNums = Object.keys(data)
  const traces  = []
  const shapes  = []
  const tickVals = []
  const tickText = []

  lapNums.forEach((lapNum, i) => {
    const pts    = data[lapNum]
    if (!pts || pts.length === 0) return
    const colour   = lapColours[lapNum] ?? '#a1a1aa'
    const maxDist  = pts[pts.length - 1]?.distance ?? 0
    const intervals = coastingIntervals(pts)
    const totalCoast = intervals.reduce((sum, { x0, x1 }) => sum + (x1 - x0), 0)
    const pct = maxDist > 0 ? ((totalCoast / maxDist) * 100).toFixed(1) : '—'

    tickVals.push(i)
    // Shorter label on mobile to fit smaller left margin
    tickText.push(isMobile
      ? `L${lapNum} (${pct}%)`
      : `Lap ${lapNum}  ${totalCoast}m (${pct}%)`
    )

    // Full-lap background bar
    traces.push({
      type: 'scatter', mode: 'lines',
      x: [0, maxDist],
      y: [i, i],
      line: { color: 'rgba(255,255,255,0.07)', width: 14 },
      showlegend: false,
      hoverinfo: 'skip',
    })

    // Coasting interval blocks
    for (const { x0, x1 } of intervals) {
      shapes.push({
        type: 'rect',
        xref: 'x', yref: 'y',
        x0, x1,
        y0: i - 0.38, y1: i + 0.38,
        fillcolor: colour,
        line: { width: 0 },
        opacity: 0.85,
        layer: 'above',
      })
    }
  })

  if (traces.length === 0) return null

  const dynamicHeight = Math.max(height, 60 + lapNums.length * 48)

  return (
    <Plot
      data={traces}
      layout={{
        xaxis: {
          title: { text: 'Distance (m)', font: { size: 10, color: '#a1a1aa' } },
          color: '#a1a1aa',
          gridcolor: 'rgba(255,255,255,0.06)',
          tickfont: { size: 9, color: '#a1a1aa' },
          zeroline: false,
          fixedrange: true,
        },
        yaxis: {
          tickvals: tickVals,
          ticktext: tickText,
          tickfont: { size: isMobile ? 9 : 10, color: '#a1a1aa' },
          color: '#a1a1aa',
          gridcolor: 'rgba(255,255,255,0.06)',
          zeroline: false,
          range: [-0.6, lapNums.length - 0.4],
          fixedrange: true,
        },
        shapes,
        margin: isMobile
          ? { l: 90, r: 10, t: 10, b: 35 }
          : { l: 170, r: 20, t: 10, b: 40 },
        height: dynamicHeight,
        paper_bgcolor: '#18181b',
        plot_bgcolor:  '#18181b',
        font: { color: '#fafafa', family: 'monospace' },
        showlegend: false,
      }}
      config={{ responsive: true, displayModeBar: false, scrollZoom: false }}
      style={{ width: '100%' }}
    />
  )
}
