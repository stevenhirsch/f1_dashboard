import { useRaceResults } from '../hooks/useRaceResults'
import TyreStrategyPlot from '../plots/TyreStrategyPlot'
import WeatherStrip from '../plots/WeatherStrip'
import InfoTooltip from '../components/InfoTooltip'

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

function PositionChange({ racePos, gridPos, isDNF }) {
  if (isDNF || racePos == null || gridPos == null) return <span />
  const change = gridPos - racePos
  if (change > 0) return <span style={{ color: '#22c55e', fontWeight: 'bold' }}>▲{change}</span>
  if (change < 0) return <span style={{ color: '#e10600', fontWeight: 'bold' }}>▼{Math.abs(change)}</span>
  return <span style={{ color: '#a1a1aa' }}>—</span>
}

function ResultsTable({ sessionKey, qualifyingSessionKey }) {
  const { data, loading } = useRaceResults(sessionKey, qualifyingSessionKey)

  if (loading) return <p style={{ color: '#a1a1aa' }}>Loading results…</p>
  if (!data || data.length === 0) return <p style={{ color: '#a1a1aa' }}>No results data.</p>

  const bestFlSpeed = Math.max(...data.map(r => r.fl_speed ?? 0))
  const bestMaxSpeed = Math.max(...data.map(r => r.max_speed ?? 0))

  return (
    <table style={{
      borderCollapse: 'collapse',
      width: '100%',
      fontSize: '0.85rem',
      color: THEME.text,
    }}>
      <thead>
        <tr style={{ borderBottom: `1px solid ${THEME.border}`, textAlign: 'left' }}>
          {['Pos', 'Driver', 'Team', 'Laps', 'Time / Gap', 'Pits', 'FL Speed', 'Top Speed'].map(h => (
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
          const isTopFl = row.fl_speed != null && row.fl_speed === bestFlSpeed
          const isTopMax = row.max_speed != null && row.max_speed === bestMaxSpeed
          return (
            <tr
              key={row.driver_number}
              style={{
                borderBottom: `1px solid ${THEME.border}`,
                background: i % 2 === 0 ? 'transparent' : THEME.tableAlt,
              }}
            >
              <td style={{ padding: '0.45rem 0.75rem', fontWeight: 'bold', color: isDNF ? '#ef4444' : THEME.muted, whiteSpace: 'nowrap' }}>
                {pos}
                <span style={{ marginLeft: '0.35rem', fontSize: '0.75rem' }}>
                  <PositionChange racePos={row.position} gridPos={row.grid_position} isDNF={isDNF} />
                </span>
              </td>
              <td style={{ padding: '0.45rem 0.75rem', color: colour, fontWeight: 'bold' }}>
                {driver.name_acronym ?? row.driver_number}
              </td>
              <td style={{ padding: '0.45rem 0.75rem', color: THEME.muted }}>{driver.team_name ?? '—'}</td>
              <td style={{ padding: '0.45rem 0.75rem' }}>{row.number_of_laps ?? '—'}</td>
              <td style={{ padding: '0.45rem 0.75rem' }}>{timeGap}</td>
              <td style={{ padding: '0.45rem 0.75rem', color: THEME.muted }}>{row.pit_count ?? '—'}</td>
              <td style={{ padding: '0.45rem 0.75rem', fontWeight: isTopFl ? 'bold' : 'normal', color: isTopFl ? '#facc15' : THEME.text }}>
                {row.fl_speed != null ? `${row.fl_speed}` : '—'}
              </td>
              <td style={{ padding: '0.45rem 0.75rem', fontWeight: isTopMax ? 'bold' : 'normal', color: isTopMax ? '#facc15' : THEME.muted }}>
                {row.max_speed != null ? `${row.max_speed}` : '—'}
              </td>
            </tr>
          )
        })}
      </tbody>
    </table>
  )
}

export default function RacePage({ sessionKey, qualifyingSessionKey, gmtOffset }) {
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
          <ResultsTable sessionKey={sessionKey} qualifyingSessionKey={qualifyingSessionKey} />
        </div>
      </div>

      <div style={sectionStyle}>
        <h2 style={{ ...headingStyle, display: 'flex', alignItems: 'center' }}>
          Tyre Strategy
          <InfoTooltip placement="top" width={260} content={
            <div>
              <strong>Solid</strong> = new set · <strong>Hatched</strong> = scrubbed (pre-used). Used tyres may prevent graining and produce more consistent lap times since they have gone through heat cycles. Teams only get allocated a limited number of tyres per weekend, and will try to save their new tyres for specific times.
            </div>
          } />
        </h2>
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
