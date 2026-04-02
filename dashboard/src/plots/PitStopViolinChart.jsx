import Plot from '../components/ResponsivePlot'

const THEME = {
  bg: '#09090b',
  border: 'rgba(255,255,255,0.08)',
  text: '#fafafa',
  muted: '#a1a1aa',
  gridColor: 'rgba(255,255,255,0.06)',
  gold: '#f59e0b',
}

// Palette for colouring points by round (up to 24 rounds)
const ROUND_COLORS = [
  '#60a5fa', '#f97316', '#a3e635', '#e879f9',
  '#34d399', '#fb923c', '#818cf8', '#f43f5e',
  '#22d3ee', '#facc15', '#c084fc', '#4ade80',
]

function quantile(sorted, p) {
  const idx = p * (sorted.length - 1)
  const lo = Math.floor(idx)
  const hi = Math.ceil(idx)
  if (lo === hi) return sorted[lo]
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo)
}

export function computePitStopStats(stops) {
  // stops: [{ duration, round }]
  if (!stops || stops.length < 3) return null
  const sorted = stops.map(s => s.duration).sort((a, b) => a - b)
  const n = sorted.length
  const mean = sorted.reduce((s, v) => s + v, 0) / n
  const variance = sorted.reduce((s, v) => s + (v - mean) ** 2, 0) / n
  const std = Math.sqrt(variance)
  const median = quantile(sorted, 0.5)
  const q1 = quantile(sorted, 0.25)
  const q3 = quantile(sorted, 0.75)
  const iqr = q3 - q1
  const fastest = sorted[0]
  return { fastest, mean, std, median, iqr, n }
}

export function usePitStopStats(pitStopsByTeam) {
  const entries = Object.entries(pitStopsByTeam)
    .map(([team, stops]) => ({ team, stats: computePitStopStats(stops) }))
    .filter(e => e.stats !== null)

  entries.sort((a, b) => a.stats.fastest - b.stats.fastest)

  const globalFastest = entries.length > 0 ? entries[0].stats.fastest : null
  const globalFastestMean = entries.length > 0
    ? Math.min(...entries.map(e => e.stats.mean))
    : null

  return { entries, globalFastest, globalFastestMean }
}

export default function PitStopViolinChart({ pitStopsByTeam, constructorColour, roundLabels, isMobile }) {
  if (!pitStopsByTeam || !Object.keys(pitStopsByTeam).length) return null

  const { entries } = usePitStopStats(pitStopsByTeam)
  if (!entries.length) return null

  // Sort: fastest constructor at top → reverse for Plotly (draws bottom-to-top)
  const ordered = [...entries].reverse()

  // Collect all distinct rounds across all stops
  const allRounds = [...new Set(
    Object.values(pitStopsByTeam).flat().map(s => s.round).filter(r => r != null)
  )].sort((a, b) => a - b)
  const roundColorMap = Object.fromEntries(allRounds.map((r, i) => [r, ROUND_COLORS[i % ROUND_COLORS.length]]))

  const traces = ordered.map(({ team }) => {
    const stops = pitStopsByTeam[team]
    const colour = constructorColour?.[team] ?? '#888888'
    const markerColors = stops.map(s => s.round != null ? roundColorMap[s.round] : colour)
    return {
      type: 'violin',
      orientation: 'h',
      x: stops.map(s => s.duration),
      y: Array(stops.length).fill(team),
      name: team,
      line: { color: colour, width: 1.5 },
      fillcolor: colour + '22',
      points: 'all',
      jitter: 0.4,
      pointpos: 0,
      marker: {
        color: markerColors,
        size: isMobile ? 5 : 6,
        opacity: 0.85,
        line: { color: 'rgba(0,0,0,0.3)', width: 0.5 },
      },
      box: { visible: false },
      meanline: { visible: false },
      spanmode: 'hard',
      showlegend: false,
      hovertemplate: `<b>${team}</b><br>%{x:.3f}s<extra></extra>`,
    }
  })

  // Legend traces for rounds (invisible scatter just to get the legend entries)
  const legendTraces = allRounds.map(r => ({
    type: 'scatter',
    mode: 'markers',
    x: [null],
    y: [null],
    name: roundLabels?.[r] ?? `R${r}`,
    marker: { color: roundColorMap[r], size: 8, symbol: 'circle' },
    showlegend: true,
  }))

  const chartHeight = Math.max(220, entries.length * (isMobile ? 44 : 54))

  const layout = {
    paper_bgcolor: THEME.bg,
    plot_bgcolor: THEME.bg,
    font: { family: 'monospace', color: THEME.text, size: isMobile ? 10 : 11 },
    margin: { t: 10, r: 20, b: 50, l: isMobile ? 100 : 130 },
    xaxis: {
      title: { text: 'Stop duration (s)', font: { size: isMobile ? 10 : 11 } },
      gridcolor: THEME.gridColor,
      zerolinecolor: THEME.border,
    },
    yaxis: {
      tickfont: { size: isMobile ? 9 : 10 },
      gridcolor: THEME.gridColor,
    },
    legend: {
      orientation: 'h',
      x: 0,
      y: -0.18,
      font: { size: isMobile ? 9 : 10 },
      bgcolor: 'transparent',
      title: { text: 'Race  ', font: { size: isMobile ? 9 : 10, color: THEME.muted } },
    },
    hovermode: 'closest',
    violingap: 0.15,
    violingroupgap: 0.1,
  }

  return (
    <Plot
      data={[...traces, ...legendTraces]}
      layout={layout}
      config={{ responsive: true, displayModeBar: false }}
      style={{ width: '100%', height: chartHeight + (isMobile ? 40 : 50) }}
    />
  )
}
