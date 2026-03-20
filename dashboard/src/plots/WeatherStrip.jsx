import Plot from 'react-plotly.js'
import { useWeather } from '../hooks/useWeather'

export default function WeatherStrip({ sessionKey }) {
  const { data, loading } = useWeather(sessionKey)

  if (loading) return <p style={{ color: '#a1a1aa' }}>Loading weather…</p>
  if (!data || data.length === 0) return <p style={{ color: '#a1a1aa' }}>No weather data.</p>

  const timestamps = data.map(r => r.date)
  const rainfall = data.map(r => r.rainfall ?? 0)
  const maxRain = Math.max(...rainfall, 1)

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

  return (
    <Plot
      data={traces}
      layout={{
        xaxis: { title: { text: 'Time', font: { color: '#a1a1aa' } }, type: 'date', ...axisBase },
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
  )
}
