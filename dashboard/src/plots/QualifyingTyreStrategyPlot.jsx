import Plot from 'react-plotly.js'
import { COMPOUND_COLOURS, COMPOUND_ORDER, compoundColour } from '../utils/compounds'

function capitalize(s) {
  return s ? s[0] + s.slice(1).toLowerCase() : s
}

function makeMarker(colour, isFresh) {
  if (isFresh) {
    return { color: colour, line: { color: 'rgba(0,0,0,0.15)', width: 0.5 } }
  }
  return {
    color: 'rgba(255,255,255,0.65)',
    pattern: { shape: '/', bgcolor: colour, size: 8 },
    line: { color: 'rgba(0,0,0,0.15)', width: 0.5 },
  }
}

export default function QualifyingTyreStrategyPlot({ results, stintsByDriver }) {
  if (!results || results.length === 0) return <p style={{ color: '#a1a1aa' }}>No qualifying data.</p>
  if (!stintsByDriver || Object.keys(stintsByDriver).length === 0) return <p style={{ color: '#a1a1aa' }}>No stint data.</p>

  // Drivers sorted by qualifying position (results already ordered by best_lap_time asc)
  const driversSorted = results.filter(r => r.driver)
  const yLabels = driversSorted.map(r => r.driver?.name_acronym ?? String(r.driver_number))

  // Max known lap_end — fallback for last stint with null lap_end
  const maxKnownLap = Math.max(
    0,
    ...Object.values(stintsByDriver).flatMap(ss => ss.map(s => s.lap_end ?? 0))
  )

  const traceMap = {}

  for (const result of driversSorted) {
    const dn = result.driver_number
    const acronym = result.driver?.name_acronym ?? String(dn)
    const stints = (stintsByDriver[dn] ?? [])
      .slice()
      .sort((a, b) => (a.stint_number ?? 0) - (b.stint_number ?? 0))

    for (let i = 0; i < stints.length; i++) {
      const stint = stints[i]
      const compound = (stint.compound ?? 'UNKNOWN').toUpperCase()
      const isFresh = (stint.tyre_age_at_start ?? 0) === 0
      const lapStart = stint.lap_start ?? 1
      const lapEnd = stint.lap_end
        ?? (stints[i + 1]?.lap_start != null ? stints[i + 1].lap_start - 1 : null)
        ?? (maxKnownLap > 0 ? maxKnownLap : null)
      if (lapEnd == null || lapEnd < lapStart) continue

      const key = `${compound}_${isFresh ? 'fresh' : 'used'}`
      if (!traceMap[key]) {
        const colour = compoundColour(compound)
        traceMap[key] = {
          type: 'bar',
          orientation: 'h',
          name: `${compound === 'UNKNOWN' ? '?' : capitalize(compound)} (${isFresh ? 'Fresh' : 'Used'})`,
          legendgroup: key,
          showlegend: true,
          marker: makeMarker(colour, isFresh),
          x: [], y: [], base: [], customdata: [],
          hovertemplate:
            '<b>%{y}</b><br>%{customdata[0]} (%{customdata[4]})<br>' +
            'Age at start: %{customdata[1]} laps<br>' +
            'Laps %{customdata[2]}–%{customdata[3]}<extra></extra>',
        }
      }
      traceMap[key].y.push(acronym)
      traceMap[key].x.push(lapEnd - lapStart + 1)
      traceMap[key].base.push(lapStart - 1)
      traceMap[key].customdata.push([compound, stint.tyre_age_at_start ?? 0, lapStart, lapEnd, isFresh ? 'Fresh' : 'Used'])
    }
  }

  // Sort legend by compound order, fresh before used
  const traces = Object.entries(traceMap)
    .sort(([a], [b]) => {
      const [aC, aF] = a.split('_')
      const [bC, bF] = b.split('_')
      const ci = COMPOUND_ORDER.indexOf(aC) - COMPOUND_ORDER.indexOf(bC)
      return ci !== 0 ? ci : (aF === 'fresh' ? -1 : 1)
    })
    .map(([, t]) => t)

  const chartHeight = Math.max(380, driversSorted.length * 22 + 120)

  return (
    <Plot
      data={traces}
      layout={{
        barmode: 'overlay',
        xaxis: {
          title: { text: 'Lap Number (per driver)', font: { color: '#a1a1aa' } },
          color: '#a1a1aa',
          gridcolor: 'rgba(255,255,255,0.07)',
          tickfont: { color: '#a1a1aa' },
        },
        yaxis: {
          categoryorder: 'array',
          categoryarray: [...yLabels].reverse(),
          fixedrange: true,
          automargin: true,
          color: '#a1a1aa',
          tickfont: { color: '#fafafa' },
          gridcolor: 'rgba(255,255,255,0.05)',
        },
        legend: {
          orientation: 'h', x: 0, y: 1.05, xanchor: 'left', yanchor: 'bottom',
          font: { size: 11, color: '#fafafa' }, bgcolor: 'rgba(0,0,0,0)',
          traceorder: 'normal',
        },
        margin: { l: 55, r: 20, t: 70, b: 50 },
        height: chartHeight,
        paper_bgcolor: '#18181b',
        plot_bgcolor: '#18181b',
        font: { color: '#fafafa', family: 'monospace' },
      }}
      config={{ responsive: true, displayModeBar: false }}
      style={{ width: '100%' }}
    />
  )
}
