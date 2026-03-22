import Plot from 'react-plotly.js'

// 8 panels top → bottom, ~12% each with small gaps
// Order: Speed, Power, Brake, Gear, Lift & Coast, Thr/Brk, RPM, DRS
const DOMAINS = {
  speed:    [0.88, 1.00],
  throttle: [0.75, 0.87],
  brake:    [0.62, 0.74],
  gear:     [0.49, 0.61],
  coast:    [0.36, 0.48],
  tboverlap:[0.23, 0.35],
  rpm:      [0.11, 0.23],
  drs:      [0.00, 0.10],
}

const AXIS_BASE = {
  color: '#a1a1aa',
  gridcolor: 'rgba(255,255,255,0.06)',
  tickfont: { size: 9, color: '#a1a1aa' },
  zeroline: false,
  fixedrange: true,
}

function hex2rgba(hex, alpha) {
  const h = (hex ?? '#888888').replace('#', '')
  const r = parseInt(h.slice(0, 2), 16)
  const g = parseInt(h.slice(2, 4), 16)
  const b = parseInt(h.slice(4, 6), 16)
  return `rgba(${r},${g},${b},${alpha})`
}

const binaryAxis = (text) => ({
  title: { text, font: { size: 9, color: '#a1a1aa' } },
  range: [0, 1],
  tickvals: [0, 1], ticktext: ['Off', 'On'],
  ...AXIS_BASE,
})

export default function TelemetryChart({ data, lapColours, height = 740 }) {
  if (!data || Object.keys(data).length === 0) return null

  const lapNums = Object.keys(data)
  const traces  = []

  for (const lapNum of lapNums) {
    const pts = data[lapNum]
    if (!pts || pts.length === 0) continue
    const colour = lapColours[lapNum] ?? '#a1a1aa'
    const group  = `lap${lapNum}`
    const label  = `Lap ${lapNum}`
    const x      = pts.map(p => p.distance)

    // ── Speed ─────────────────────────────────────────────────────────────
    traces.push({
      type: 'scatter', mode: 'lines',
      name: label, legendgroup: group, showlegend: true,
      x, y: pts.map(p => p.speed ?? null),
      yaxis: 'y',
      line: { color: colour, width: 1.5 },
      hovertemplate: '%{y} km/h<extra>' + label + '</extra>',
    })

    // ── Engine Power (throttle %) ──────────────────────────────────────────
    traces.push({
      type: 'scatter', mode: 'lines',
      name: label, legendgroup: group, showlegend: false,
      x, y: pts.map(p => p.throttle ?? null),
      yaxis: 'y2',
      line: { color: colour, width: 1.5 },
      hovertemplate: '%{y}%<extra>' + label + '</extra>',
    })

    // ── Brake (binary 0/1 filled step blocks) ────────────────────────────
    traces.push({
      type: 'scatter', mode: 'lines',
      name: label, legendgroup: group, showlegend: false,
      x, y: pts.map(p => (p.brake > 0 ? 1 : 0)),
      customdata: pts.map(p => (p.brake > 0 ? 'Braking' : 'Off')),
      yaxis: 'y3',
      fill: 'tozeroy',
      fillcolor: hex2rgba(colour, 0.45),
      line: { color: colour, width: 1, shape: 'hv' },
      hovertemplate: '%{customdata}<extra>' + label + '</extra>',
    })

    // ── Gear (step chart) ─────────────────────────────────────────────────
    traces.push({
      type: 'scatter', mode: 'lines',
      name: label, legendgroup: group, showlegend: false,
      x, y: pts.map(p => p.n_gear ?? null),
      yaxis: 'y4',
      line: { color: colour, width: 1.5, shape: 'hv' },
      hovertemplate: 'Gear %{y}<extra>' + label + '</extra>',
    })

    // ── Lift & Coast (throttle < 1 % AND brake = 0) ────────────────────────
    traces.push({
      type: 'scatter', mode: 'lines',
      name: label, legendgroup: group, showlegend: false,
      x, y: pts.map(p => ((p.throttle ?? 0) < 1 && (p.brake ?? 0) === 0 ? 1 : 0)),
      yaxis: 'y5',
      fill: 'tozeroy',
      fillcolor: hex2rgba(colour, 0.55),
      line: { color: colour, width: 1, shape: 'hv' },
      hoverinfo: 'skip',
    })

    // ── Throttle-Brake Overlap (brake > 0 AND throttle >= 10 %) ──────────
    // Indicates trail-braking or simultaneous inputs — a distinct driving style marker.
    traces.push({
      type: 'scatter', mode: 'lines',
      name: label, legendgroup: group, showlegend: false,
      x, y: pts.map(p => ((p.brake ?? 0) > 0 && (p.throttle ?? 0) >= 10 ? 1 : 0)),
      yaxis: 'y6',
      fill: 'tozeroy',
      fillcolor: hex2rgba(colour, 0.55),
      line: { color: colour, width: 1, shape: 'hv' },
      hoverinfo: 'skip',
    })

    // ── RPM ───────────────────────────────────────────────────────────────
    traces.push({
      type: 'scatter', mode: 'lines',
      name: label, legendgroup: group, showlegend: false,
      x, y: pts.map(p => p.rpm ?? null),
      yaxis: 'y7',
      line: { color: colour, width: 1.5 },
      hovertemplate: '%{y} rpm<extra>' + label + '</extra>',
    })

    // ── DRS (filled band when open: drs >= 10) ────────────────────────────
    traces.push({
      type: 'scatter', mode: 'lines',
      name: label, legendgroup: group, showlegend: false,
      x, y: pts.map(p => (p.drs >= 10 ? 1 : 0)),
      yaxis: 'y8',
      fill: 'tozeroy',
      fillcolor: hex2rgba(colour, 0.55),
      line: { color: colour, width: 1, shape: 'hv' },
      hoverinfo: 'skip',
    })
  }

  return (
    <Plot
      data={traces}
      layout={{
        xaxis: {
          title: { text: 'Distance (m)', font: { size: 9, color: '#a1a1aa' } },
          anchor: 'y8',
          ...AXIS_BASE,
        },
        yaxis:  { title: { text: 'Speed (km/h)', font: { size: 9, color: '#a1a1aa' } }, domain: DOMAINS.speed,    ...AXIS_BASE },
        yaxis2: { title: { text: 'Power (%)',    font: { size: 9, color: '#a1a1aa' } }, domain: DOMAINS.throttle, range: [0, 105], ...AXIS_BASE },
        yaxis3: { ...binaryAxis('Brake'),        domain: DOMAINS.brake,     range: [0, 1.5] },
        yaxis4: { title: { text: 'Gear', font: { size: 9, color: '#a1a1aa' } }, domain: DOMAINS.gear, range: [0, 9], dtick: 1, ...AXIS_BASE },
        yaxis5: { ...binaryAxis('Lift & Coast'), domain: DOMAINS.coast },
        yaxis6: { ...binaryAxis('Thr/Brk'),      domain: DOMAINS.tboverlap },
        yaxis7: { title: { text: 'RPM',  font: { size: 9, color: '#a1a1aa' } }, domain: DOMAINS.rpm,  ...AXIS_BASE },
        yaxis8: { ...binaryAxis('DRS'),           domain: DOMAINS.drs },
        legend: {
          orientation: 'h',
          x: 0, y: 1.03,
          font: { size: 10, color: '#a1a1aa' },
          bgcolor: 'transparent',
        },
        margin: { l: 75, r: 20, t: 30, b: 40 },
        height,
        paper_bgcolor: '#18181b',
        plot_bgcolor:  '#18181b',
        font: { color: '#fafafa', family: 'monospace' },
        hovermode: 'x unified',
      }}
      config={{ responsive: true, displayModeBar: false, scrollZoom: false }}
      style={{ width: '100%' }}
    />
  )
}
