import { useState } from 'react'
import { useSessionSelector } from './hooks/useSessionSelector'

const TABS = ['Race', 'Qualifying', 'Driver']

export default function App() {
  const [activeTab, setActiveTab] = useState('Race')
  const selector = useSessionSelector()

  return (
    <div style={{ fontFamily: 'monospace', padding: '1rem' }}>
      <h1 style={{ marginBottom: '1rem' }}>F1 Strategy Dashboard</h1>

      {/* Session selector */}
      <div style={{ display: 'flex', gap: '1rem', marginBottom: '1rem', flexWrap: 'wrap' }}>
        <label>
          Year{' '}
          <select
            value={selector.selectedYear ?? ''}
            onChange={e => selector.setSelectedYear(Number(e.target.value))}
          >
            {selector.years.map(y => (
              <option key={y} value={y}>{y}</option>
            ))}
          </select>
        </label>

        <label>
          Grand Prix{' '}
          <select
            value={selector.selectedMeetingKey ?? ''}
            onChange={e => selector.setSelectedMeetingKey(Number(e.target.value))}
          >
            {selector.meetings.map(m => (
              <option key={m.meeting_key} value={m.meeting_key}>{m.meeting_name}</option>
            ))}
          </select>
        </label>

        <label>
          Session{' '}
          <select
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
      <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '1rem', borderBottom: '1px solid #ccc' }}>
        {TABS.map(tab => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
            style={{
              padding: '0.4rem 1rem',
              cursor: 'pointer',
              background: 'none',
              border: 'none',
              borderBottom: activeTab === tab ? '2px solid #e10600' : '2px solid transparent',
              fontWeight: activeTab === tab ? 'bold' : 'normal',
            }}
          >
            {tab}
          </button>
        ))}
      </div>

      {/* Tab content — pages built in Phase 1-3 */}
      <div>
        {!selector.selectedSessionKey ? (
          <p>Select a session above to load data.</p>
        ) : (
          <p style={{ color: '#666' }}>
            [{activeTab} tab] session_key={selector.selectedSessionKey} — content coming in Phase {activeTab === 'Race' ? 1 : activeTab === 'Qualifying' ? 2 : 3}
          </p>
        )}
      </div>
    </div>
  )
}
