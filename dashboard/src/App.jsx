import { useState } from 'react'
import { useSessionSelector } from './hooks/useSessionSelector'
import RacePage from './pages/RacePage'

const TABS = ['Race', 'Qualifying', 'Driver']

const THEME = {
  bg: '#09090b',
  surface: '#18181b',
  border: 'rgba(255,255,255,0.08)',
  text: '#fafafa',
  muted: '#a1a1aa',
  red: '#e10600',
  inputBg: '#27272a',
}

const selectStyle = {
  background: THEME.inputBg,
  color: THEME.text,
  border: `1px solid ${THEME.border}`,
  borderRadius: '6px',
  padding: '0.3rem 0.5rem',
  fontSize: '0.875rem',
  cursor: 'pointer',
  outline: 'none',
}

const labelStyle = {
  color: THEME.muted,
  fontSize: '0.75rem',
  letterSpacing: '0.05em',
  textTransform: 'uppercase',
  display: 'flex',
  flexDirection: 'column',
  gap: '0.25rem',
}

export default function App() {
  const [activeTab, setActiveTab] = useState('Race')
  const selector = useSessionSelector()

  return (
    <div style={{ fontFamily: 'monospace', background: THEME.bg, minHeight: '100vh', color: THEME.text }}>

      {/* Header */}
      <div style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        padding: '1rem 1.5rem 0.75rem',
        borderBottom: `1px solid ${THEME.border}`,
      }}>
        <h1 style={{ margin: 0, fontSize: '1.25rem', fontWeight: 'bold', letterSpacing: '0.02em' }}>
          F1 Strategy Dashboard
        </h1>
        <img
          src="F1.png"
          alt="F1"
          style={{ height: '36px', objectFit: 'contain' }}
        />
      </div>

      {/* Session selector */}
      <div style={{
        display: 'flex',
        gap: '1.5rem',
        padding: '0.75rem 1.5rem',
        borderBottom: `1px solid ${THEME.border}`,
        flexWrap: 'wrap',
        background: THEME.surface,
      }}>
        <label style={labelStyle}>
          <span>Year</span>
          <select
            style={selectStyle}
            value={selector.selectedYear ?? ''}
            onChange={e => selector.setSelectedYear(Number(e.target.value))}
          >
            {selector.years.map(y => (
              <option key={y} value={y}>{y}</option>
            ))}
          </select>
        </label>

        <label style={labelStyle}>
          <span>Grand Prix</span>
          <select
            style={selectStyle}
            value={selector.selectedMeetingKey ?? ''}
            onChange={e => selector.setSelectedMeetingKey(Number(e.target.value))}
          >
            {selector.meetings.map(m => (
              <option key={m.meeting_key} value={m.meeting_key}>{m.meeting_name}</option>
            ))}
          </select>
        </label>

        <label style={labelStyle}>
          <span>Session</span>
          <select
            style={selectStyle}
            value={selector.selectedSessionKey ?? ''}
            onChange={e => selector.setSelectedSessionKey(Number(e.target.value))}
          >
            {selector.sessions.map(s => (
              <option key={s.session_key} value={s.session_key}>{s.session_name}</option>
            ))}
          </select>
        </label>
      </div>

      {/* Tab bar */}
      <div style={{
        display: 'flex',
        gap: '0',
        padding: '0 1.5rem',
        borderBottom: `1px solid ${THEME.border}`,
        background: THEME.surface,
      }}>
        {TABS.map(tab => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
            style={{
              padding: '0.6rem 1.25rem',
              cursor: 'pointer',
              background: 'none',
              border: 'none',
              borderBottom: activeTab === tab ? `2px solid ${THEME.red}` : '2px solid transparent',
              color: activeTab === tab ? THEME.text : THEME.muted,
              fontWeight: activeTab === tab ? 'bold' : 'normal',
              fontFamily: 'monospace',
              fontSize: '0.875rem',
              letterSpacing: '0.03em',
            }}
          >
            {tab}
          </button>
        ))}
      </div>

      {/* Tab content */}
      <div style={{ padding: '1.25rem 1.5rem' }}>
        {activeTab === 'Race' && (
          <RacePage sessionKey={selector.selectedSessionKey} />
        )}
        {activeTab === 'Qualifying' && (
          <p style={{ color: THEME.muted }}>Qualifying tab — Phase 2</p>
        )}
        {activeTab === 'Driver' && (
          <p style={{ color: THEME.muted }}>Driver tab — Phase 3</p>
        )}
      </div>
    </div>
  )
}
