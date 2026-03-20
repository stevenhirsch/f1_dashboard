import { useState } from 'react'
import { useSessionSelector } from './hooks/useSessionSelector'
import RacePage from './pages/RacePage'

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

function TopNavButton({ label, active, onClick }) {
  return (
    <button
      onClick={onClick}
      style={{
        background: 'none',
        border: 'none',
        borderBottom: active ? `2px solid ${THEME.red}` : '2px solid transparent',
        color: active ? THEME.text : THEME.muted,
        fontWeight: active ? 'bold' : 'normal',
        fontFamily: 'monospace',
        fontSize: '0.875rem',
        letterSpacing: '0.05em',
        padding: '0.25rem 0.75rem',
        cursor: 'pointer',
      }}
    >
      {label}
    </button>
  )
}

function TabButton({ label, active, onClick }) {
  return (
    <button
      onClick={onClick}
      style={{
        padding: '0.6rem 1.25rem',
        cursor: 'pointer',
        background: 'none',
        border: 'none',
        borderBottom: active ? `2px solid ${THEME.red}` : '2px solid transparent',
        color: active ? THEME.text : THEME.muted,
        fontWeight: active ? 'bold' : 'normal',
        fontFamily: 'monospace',
        fontSize: '0.875rem',
        letterSpacing: '0.03em',
      }}
    >
      {label}
    </button>
  )
}

const CURRENT_YEAR = new Date().getFullYear()

function Footer() {
  return (
    <footer style={{
      marginTop: '3rem',
      borderTop: `1px solid ${THEME.border}`,
      padding: '1.25rem 1.5rem',
      display: 'flex',
      flexDirection: 'column',
      gap: '0.5rem',
      alignItems: 'center',
      textAlign: 'center',
      color: THEME.muted,
      fontSize: '0.72rem',
      WebkitTextSizeAdjust: '100%',
      textSizeAdjust: '100%',
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '1.25rem' }}>
        <a
          href="https://github.com/stevenhirsch/f1_dashboard"
          target="_blank"
          rel="noopener noreferrer"
          style={{ color: THEME.muted, display: 'flex', alignItems: 'center', gap: '0.4rem', textDecoration: 'none' }}
        >
          <img src="github.svg" alt="GitHub" style={{ height: '16px', width: '16px', filter: 'invert(0.6)' }} />
          stevenhirsch/f1_dashboard
        </a>
        <span>© {CURRENT_YEAR} Steven Hirsch</span>
      </div>
      <p style={{ margin: 0 }}>
        Data provided by the{' '}
        <a
          href="https://openf1.org"
          target="_blank"
          rel="noopener noreferrer"
          style={{ color: THEME.muted, textDecoration: 'underline' }}
        >
          OpenF1 API
        </a>
        , an open-source project unaffiliated with Formula 1.
      </p>
    </footer>
  )
}

export default function App() {
  const [activeMode, setActiveMode] = useState('Dashboard') // 'Dashboard' | 'Chat'
  const [activeTab, setActiveTab] = useState('Race')
  const selector = useSessionSelector()

  return (
    <div style={{ fontFamily: 'monospace', background: THEME.bg, minHeight: '100vh', color: THEME.text }}>

      {/* Top nav: logo + title + Dashboard/Chat mode switcher */}
      <div style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        padding: '0.75rem 1.5rem',
        borderBottom: `1px solid ${THEME.border}`,
      }}>
        <img src="F1.png" alt="F1" style={{ height: '36px', objectFit: 'contain' }} />
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.25rem' }}>
          <TopNavButton label="Dashboard" active={activeMode === 'Dashboard'} onClick={() => setActiveMode('Dashboard')} />
          <TopNavButton label="Chat" active={activeMode === 'Chat'} onClick={() => setActiveMode('Chat')} />
        </div>
      </div>

      {activeMode === 'Dashboard' && (
        <>
          {/* Weekend selector: Year, Weekend, Event (Event only on sprint weekends) */}
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
              <span>Weekend</span>
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

            {selector.isSprintWeekend && (
              <label style={labelStyle}>
                <span>Event</span>
                <select
                  style={selectStyle}
                  value={selector.selectedEvent}
                  onChange={e => selector.setSelectedEvent(e.target.value)}
                >
                  {selector.events.map(ev => (
                    <option key={ev} value={ev}>{ev}</option>
                  ))}
                </select>
              </label>
            )}
          </div>

          {/* Tab bar */}
          <div style={{
            display: 'flex',
            padding: '0 1.5rem',
            borderBottom: `1px solid ${THEME.border}`,
            background: THEME.surface,
          }}>
            {['Race', 'Qualifying', 'Driver'].map(tab => (
              <TabButton
                key={tab}
                label={tab}
                active={activeTab === tab}
                onClick={() => setActiveTab(tab)}
              />
            ))}
          </div>

          {/* Tab content */}
          <div style={{ padding: '1.25rem 1.5rem' }}>
            {activeTab === 'Race' && (
              <RacePage sessionKey={selector.raceSessionKey} gmtOffset={selector.gmtOffset} />
            )}
            {activeTab === 'Qualifying' && (
              <p style={{ color: THEME.muted }}>Qualifying tab — Phase 2</p>
            )}
            {activeTab === 'Driver' && (
              <p style={{ color: THEME.muted }}>Driver tab — Phase 3</p>
            )}
          </div>
        </>
      )}

      {activeMode === 'Chat' && (
        <div style={{ padding: '3rem 1.5rem', textAlign: 'center', color: THEME.muted }}>
          <p style={{ fontSize: '1rem' }}>Chat — coming soon</p>
          <p style={{ fontSize: '0.85rem', marginTop: '0.5rem' }}>
            Ask questions about F1 data across any race, season, or driver.
          </p>
        </div>
      )}

      <Footer />
    </div>
  )
}
