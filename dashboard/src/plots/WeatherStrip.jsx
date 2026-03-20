import Plot from 'react-plotly.js'
import { useWeather } from '../hooks/useWeather'

function shiftToLocal(isoString, gmtOffset) {
  if (!gmtOffset) return isoString
  // OpenF1 returns gmt_offset as "08:00:00" or "-05:00:00" (sign optional, seconds present)
  const match = gmtOffset.match(/^([+-]?)(\d{2}):(\d{2})/)
  if (!match) return isoString
  const sign = match[1] === '-' ? -1 : 1
  const offsetMs = sign * (parseInt(match[2]) * 60 + parseInt(match[3])) * 60000
  const shifted = new Date(new Date(isoString).getTime() + offsetMs)
  // Strip the 'Z' so Plotly treats it as a naive local timestamp, not UTC
  return shifted.toISOString().replace('Z', '')
}

function formatOffsetLabel(gmtOffset) {
  if (!gmtOffset) return 'UTC'
  const match = gmtOffset.match(/^([+-]?)(\d{2}):(\d{2})/)
  if (!match) return `UTC (${gmtOffset})`
  const sign = match[1] === '-' ? '-' : '+'
  return `UTC${sign}${match[2]}:${match[3]}`
}

export default function WeatherStrip({ sessionKey, gmtOffset }) {
  const { data, loading } = useWeather(sessionKey)

  if (loading) return <p style={{ color: '#a1a1aa' }}>Loading weather…</p>
  if (!data || data.length === 0) return <p style={{ color: '#a1a1aa' }}>No weather data.</p>

  const timestamps = data.map(r => shiftToLocal(r.date, gmtOffset))
  const rainfall = data.map(r => r.rainfall ?? 0)
  const maxRain = Math.max(...rainfall, 1)

  const avgAir = (data.reduce((s, r) => s + (r.air_temperature ?? 0), 0) / data.length).toFixed(1)
  const avgTrack = (data.reduce((s, r) => s + (r.track_temperature ?? 0), 0) / data.length).toFixed(1)
  const totalRain = rainfall.reduce((s, v) => s + v, 0).toFixed(1)

  const traces = [
    {
      type: 'scatter',
      mode: 'lines+markers',
      name: 'Track temp (°C)',
      x: timestamps,
      y: data.map(r => r.track_temperature),
      line: { color: '#e10600', width: 2 },
      marker: { size: 4 },
      yaxis: 'y',
      hovertemplate: 'Track: %{y:.1f}°C<extra></extra>',
    },
    {
      type: 'scatter',
      mode: 'lines+markers',
      name: 'Air temp (°C)',
      x: timestamps,
      y: data.map(r => r.air_temperature),
      line: { color: '#60a5fa', width: 2 },
      marker: { size: 4 },
      yaxis: 'y',
      hovertemplate: 'Air: %{y:.1f}°C<extra></extra>',
    },
    {
      type: 'bar',
      name: 'Rainfall (mm)',
      x: timestamps,
      y: rainfall,
      marker: { color: 'rgba(100,180,255,0.4)' },
      yaxis: 'y2',
      hovertemplate: 'Rain: %{y:.1f}mm<extra></extra>',
    },
  ]

  const axisBase = {
    gridcolor: 'rgba(255,255,255,0.07)',
    tickfont: { color: '#a1a1aa' },
    linecolor: 'rgba(255,255,255,0.1)',
    zerolinecolor: 'rgba(255,255,255,0.1)',
  }

  const statStyle = {
    display: 'flex',
    flexDirection: 'column',
    gap: '0.15rem',
  }
  const statLabel = { fontSize: '0.7rem', color: '#a1a1aa', textTransform: 'uppercase', letterSpacing: '0.05em' }
  const statValue = { fontSize: '1rem', fontWeight: 'bold', color: '#fafafa' }

  return (
    <div>
      <div style={{ display: 'flex', gap: '2rem', padding: '0.75rem 1rem 0.5rem' }}>
        <div style={statStyle}>
          <span style={statLabel}>Avg Air Temp</span>
          <span style={{ ...statValue, color: '#60a5fa' }}>{avgAir}°C</span>
        </div>
        <div style={statStyle}>
          <span style={statLabel}>Avg Track Temp</span>
          <span style={{ ...statValue, color: '#e10600' }}>{avgTrack}°C</span>
        </div>
        <div style={statStyle}>
          <span style={statLabel}>Total Rainfall</span>
          <span style={statValue}>{totalRain} mm</span>
        </div>
      </div>
    <Plot
      data={traces}
      layout={{
        xaxis: { title: { text: `Time (${formatOffsetLabel(gmtOffset)})`, font: { color: '#a1a1aa' } }, type: 'date', ...axisBase },
        yaxis: {
          title: { text: 'Temperature (°C)', font: { color: '#e10600' } },
          side: 'left',
          rangemode: 'tozero',
          tickfont: { color: '#a1a1aa' },
          ...axisBase,
        },
        yaxis2: {
          title: { text: 'Rainfall (mm)', font: { color: '#60a5fa' } },
          side: 'right',
          overlaying: 'y',
          showgrid: false,
          range: [0, maxRain * 1.3],
          rangemode: 'tozero',
          tickfont: { color: '#60a5fa' },
          linecolor: 'rgba(255,255,255,0.1)',
        },
        legend: { orientation: 'h', y: -0.25, font: { color: '#fafafa' }, bgcolor: 'rgba(0,0,0,0)' },
        margin: { l: 60, r: 70, t: 20, b: 80 },
        height: 300,
        paper_bgcolor: '#18181b',
        plot_bgcolor: '#18181b',
        font: { color: '#fafafa' },
      }}
      config={{ responsive: true, displayModeBar: false }}
      style={{ width: '100%' }}
    />
    </div>
  )
}
