import Plot from '../components/ResponsivePlot'

export default function SectorDeltaHeatmap({ deltas, isMobile }) {
  if (!deltas || deltas.length === 0) return <p style={{ color: '#a1a1aa' }}>No sector data.</p>
  // Delta = driver's best sector time minus the theoretical minimum across all drivers.
  // Green (0) = matched the fastest sector; red = time lost vs. theoretical pole pace.

  const drivers = deltas.map(d => d.driver)
  const zValues = deltas.map(d => [d.delta_s1 ?? null, d.delta_s2 ?? null, d.delta_s3 ?? null])
  const textValues = deltas.map(d => [
    d.s1 != null ? d.s1.toFixed(3) : '',
    d.s2 != null ? d.s2.toFixed(3) : '',
    d.s3 != null ? d.s3.toFixed(3) : '',
  ])

  const maxDelta = Math.max(
    ...deltas.flatMap(d => [d.delta_s1, d.delta_s2, d.delta_s3]).filter(v => v != null),
    0.001,
  )

  return (
    <Plot
      data={[{
        type: 'heatmap',
        x: ['S1', 'S2', 'S3'],
        y: drivers,
        z: zValues,
        text: textValues,
        texttemplate: '%{text}',
        colorscale: [
          [0, '#22c55e'],
          [1, '#ef4444'],
        ],
        zmin: 0,
        zmax: maxDelta,
        showscale: true,
        colorbar: {
          title: { text: isMobile ? '' : 'Delta (s)', font: { color: '#a1a1aa', size: 10 } },
          tickfont: { color: '#a1a1aa', size: isMobile ? 8 : 10 },
          thickness: isMobile ? 8 : 12,
          len: isMobile ? 0.8 : 1,
        },
        hovertemplate:
          '<b>%{y}</b> — %{x}<br>Sector time: %{text}s<br>Delta: +%{z:.3f}s<extra></extra>',
      }]}
      layout={{
        yaxis: {
          autorange: 'reversed',
          categoryorder: 'array',
          categoryarray: drivers,
          fixedrange: true,
          color: '#a1a1aa',
          tickfont: { color: '#fafafa', size: isMobile ? 9 : 11 },
          gridcolor: 'rgba(255,255,255,0.05)',
        },
        xaxis: {
          fixedrange: true,
          color: '#a1a1aa',
          tickfont: { color: '#a1a1aa', size: isMobile ? 10 : 12 },
          side: 'top',
        },
        margin: isMobile
          ? { l: 45, r: 50, t: 25, b: 10 }
          : { l: 55, r: 80, t: 30, b: 20 },
        height: isMobile
          ? Math.min(340, Math.max(200, deltas.length * 18 + 50))
          : Math.max(300, deltas.length * 24 + 60),
        paper_bgcolor: '#18181b',
        plot_bgcolor: '#18181b',
        font: { color: '#fafafa', family: 'monospace' },
      }}
      config={{ responsive: true, displayModeBar: false, scrollZoom: false }}
      style={{ width: '100%' }}
    />
  )
}
