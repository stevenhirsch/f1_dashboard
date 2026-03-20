import { useRaceResults } from '../hooks/useRaceResults'
import TyreStrategyPlot from '../plots/TyreStrategyPlot'
import WeatherStrip from '../plots/WeatherStrip'

const THEME = {
  bg: '#09090b',
  surface: '#18181b',
  border: 'rgba(255,255,255,0.08)',
  text: '#fafafa',
  muted: '#a1a1aa',
  red: '#e10600',
  tableAlt: '#1c1c1f',
}

function formatDuration(seconds) {
  if (seconds == null) return '—'
  const h = Math.floor(seconds / 3600)
  const m = Math.floor((seconds % 3600) / 60)
  const s = (seconds % 60).toFixed(3)
  if (h > 0) return `${h}:${String(m).padStart(2, '0')}:${String(s).padStart(6, '0')}`
  return `${m}:${String(s).padStart(6, '0')}`
}

function posLabel(row) {
  if (row.dsq) return 'DSQ'
  if (row.dns) return 'DNS'
  if (row.dnf) return 'DNF'
  return row.position ?? '—'
}

function ResultsTable({ sessionKey }) {
  const { data, loading } = useRaceResults(sessionKey)

  if (loading) return <p style={{ color: '#a1a1aa' }}>Loading results…</p>
  if (!data || data.length === 0) return <p style={{ color: '#a1a1aa' }}>No results data.</p>

  return (
    <table style={{
      borderCollapse: 'collapse',
      width: '100%',
      fontSize: '0.85rem',
      color: THEME.text,
    }}>
      <thead>
        <tr style={{ borderBottom: `1px solid ${THEME.border}`, textAlign: 'left' }}>
          {['Pos', 'Driver', 'Team', 'Laps', 'Time / Gap', 'Pits'].map(h => (
            <th key={h} style={{
              padding: '0.4rem 0.75rem',
              whiteSpace: 'nowrap',
              color: THEME.muted,
              fontWeight: 'normal',
              fontSize: '0.75rem',
              textTransform: 'uppercase',
              letterSpacing: '0.05em',
            }}>{h}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {data.map((row, i) => {
          const driver = row.drivers ?? {}
          const rawColour = driver.team_colour ?? '888888'
          const colour = `#${rawColour.replace('#', '')}`
          const pos = posLabel(row)
          const isDNF = row.dnf || row.dns || row.dsq
          const timeGap = row.position === 1
            ? formatDuration(row.duration)
            : (row.gap_to_leader ?? '—')
          return (
            <tr
              key={row.driver_number}
              style={{
                borderBottom: `1px solid ${THEME.border}`,
                background: i % 2 === 0 ? 'transparent' : THEME.tableAlt,
              }}
            >
              <td style={{ padding: '0.45rem 0.75rem', fontWeight: 'bold', color: isDNF ? '#ef4444' : THEME.muted }}>
                {pos}
              </td>
              <td style={{ padding: '0.45rem 0.75rem', color: colour, fontWeight: 'bold' }}>
                {driver.name_acronym ?? row.driver_number}
              </td>
              <td style={{ padding: '0.45rem 0.75rem', color: THEME.muted }}>{driver.team_name ?? '—'}</td>
              <td style={{ padding: '0.45rem 0.75rem' }}>{row.number_of_laps ?? '—'}</td>
              <td style={{ padding: '0.45rem 0.75rem' }}>{timeGap}</td>
              <td style={{ padding: '0.45rem 0.75rem', color: THEME.muted }}>{row.pit_count ?? '—'}</td>
            </tr>
          )
        })}
      </tbody>
    </table>
  )
}

export default function RacePage({ sessionKey, gmtOffset }) {
  if (!sessionKey) return <p style={{ color: '#a1a1aa' }}>Select a session above.</p>

  const sectionStyle = { marginBottom: '2rem' }
  const headingStyle = {
    fontSize: '0.75rem',
    fontWeight: 'bold',
    marginBottom: '0.75rem',
    color: '#a1a1aa',
    textTransform: 'uppercase',
    letterSpacing: '0.1em',
    borderBottom: `1px solid rgba(255,255,255,0.08)`,
    paddingBottom: '0.4rem',
  }

  return (
    <div>
      <div style={sectionStyle}>
        <h2 style={headingStyle}>Race Results</h2>
        <div style={{ background: THEME.surface, borderRadius: '8px', border: `1px solid ${THEME.border}`, overflow: 'hidden' }}>
          <ResultsTable sessionKey={sessionKey} />
        </div>
      </div>

      <div style={sectionStyle}>
        <h2 style={headingStyle}>Tyre Strategy</h2>
        <div style={{ background: THEME.surface, borderRadius: '8px', border: `1px solid ${THEME.border}`, padding: '0.5rem' }}>
          <TyreStrategyPlot sessionKey={sessionKey} />
        </div>
      </div>

      <div style={sectionStyle}>
        <h2 style={headingStyle}>Weather</h2>
        <div style={{ background: THEME.surface, borderRadius: '8px', border: `1px solid ${THEME.border}`, padding: '0.5rem' }}>
          <WeatherStrip sessionKey={sessionKey} gmtOffset={gmtOffset} />
        </div>
      </div>
    </div>
  )
}
