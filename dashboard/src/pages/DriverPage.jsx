import { useState, useEffect } from 'react'
import Plot from 'react-plotly.js'
import { supabase } from '../supabaseClient'
import { useDriverRaceData } from '../hooks/useDriverRaceData'
import { useDriverQualifyingData } from '../hooks/useDriverQualifyingData'
import { compoundColour, COMPOUND_COLOURS } from '../utils/compounds'
import { formatQualTime, assignPhases, normalizePhase } from '../utils/qualifying'
import LapTimesChart from '../plots/LapTimesChart'
import { useRaceControl } from '../hooks/useRaceControl'

const THEME = {
  bg: '#09090b',
  surface: '#18181b',
  border: 'rgba(255,255,255,0.08)',
  text: '#fafafa',
  muted: '#a1a1aa',
  red: '#e10600',
  tableAlt: '#1c1c1f',
  inputBg: '#27272a',
  green: '#22c55e',
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/**
 * Convert OpenF1 full_name format ("Lando NORRIS") to title case ("Lando Norris").
 */
function formatDriverName(fullName) {
  if (!fullName) return null
  return fullName
    .split(' ')
    .map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
    .join(' ')
}

function getStintForLap(lapNumber, stints) {
  return stints?.find(s => {
    const start = s.lap_start ?? 1
    const end = s.lap_end ?? Infinity
    return lapNumber >= start && lapNumber <= end
  }) ?? null
}

function tyreAgeAtLap(lapNumber, stint) {
  if (!stint) return null
  return (stint.tyre_age_at_start ?? 0) + (lapNumber - (stint.lap_start ?? 1))
}

// ---------------------------------------------------------------------------
// Shared sub-components
// ---------------------------------------------------------------------------

function TabButton({ label, active, onClick }) {
  return (
    <button
      onClick={onClick}
      style={{
        padding: '0.45rem 1rem',
        cursor: 'pointer',
        background: 'none',
        border: 'none',
        borderBottom: active ? `2px solid ${THEME.red}` : '2px solid transparent',
        color: active ? THEME.text : THEME.muted,
        fontWeight: active ? 'bold' : 'normal',
        fontFamily: 'monospace',
        fontSize: '0.82rem',
        letterSpacing: '0.03em',
      }}
    >
      {label}
    </button>
  )
}

function CompoundBadge({ compound, fresh }) {
  if (!compound) return null
  const bg = COMPOUND_COLOURS[(compound ?? '').toUpperCase()] ?? COMPOUND_COLOURS.UNKNOWN
  return (
    <span style={{
      display: 'inline-flex',
      alignItems: 'center',
      gap: '3px',
    }}>
      <span style={{
        display: 'inline-block',
        background: bg,
        color: '#000',
        fontSize: '0.65rem',
        fontWeight: 'bold',
        borderRadius: '3px',
        padding: '0 4px',
        lineHeight: '1.5',
      }}>
        {(compound ?? '?').slice(0, 1)}
      </span>
      {fresh != null && (
        <span style={{ fontSize: '0.65rem', color: THEME.muted }}>
          {fresh ? 'N' : 'U'}
        </span>
      )}
    </span>
  )
}

function StatCard({ label, value, sub, colour }) {
  return (
    <div style={{
      background: THEME.surface,
      border: `1px solid ${THEME.border}`,
      borderRadius: '8px',
      padding: '0.75rem 1rem',
      minWidth: '110px',
    }}>
      <div style={{ fontSize: '0.65rem', color: THEME.muted, textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: '0.3rem' }}>
        {label}
      </div>
      <div style={{ fontSize: '1rem', fontWeight: 'bold', color: colour ?? THEME.text, lineHeight: 1.2 }}>
        {value ?? '—'}
      </div>
      {sub && (
        <div style={{ fontSize: '0.7rem', color: THEME.muted, marginTop: '0.2rem' }}>{sub}</div>
      )}
    </div>
  )
}

const sectionHeading = {
  fontSize: '0.72rem',
  fontWeight: 'bold',
  color: THEME.muted,
  textTransform: 'uppercase',
  letterSpacing: '0.1em',
  borderBottom: `1px solid ${THEME.border}`,
  paddingBottom: '0.4rem',
  marginBottom: '0.75rem',
}

const thStyle = {
  padding: '0.35rem 0.6rem',
  whiteSpace: 'nowrap',
  color: THEME.muted,
  fontWeight: 'normal',
  fontSize: '0.7rem',
  textTransform: 'uppercase',
  letterSpacing: '0.05em',
  textAlign: 'left',
  borderBottom: `1px solid ${THEME.border}`,
}

const tdStyle = {
  padding: '0.35rem 0.6rem',
  whiteSpace: 'nowrap',
  fontSize: '0.82rem',
}

// ---------------------------------------------------------------------------
// Race sub-tab
// ---------------------------------------------------------------------------

function RaceSummaryCard({ result, laps, pitStops, driver }) {
  if (!result && laps.length === 0) return null

  const validLaps = laps.filter(l => l.lap_duration && l.lap_duration > 0 && !l.is_pit_out_lap)
  const bestLap = validLaps.reduce((best, l) => (!best || l.lap_duration < best.lap_duration) ? l : best, null)

  const pos = result?.dsq ? 'DSQ' : result?.dns ? 'DNS' : result?.dnf ? 'DNF' : result?.position != null ? `P${result.position}` : '—'
  const posColour = (result?.dnf || result?.dns || result?.dsq) ? '#ef4444' : THEME.text

  const pitLaps = pitStops.map(p => `L${p.lap_number}`).join(', ')

  const overtakesMade = 0   // overtakes data available via useDriverRaceData but not passed here — shown in table
  const teamColour = driver?.team_colour ? `#${driver.team_colour.replace('#', '')}` : THEME.muted

  return (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.75rem', marginBottom: '1.5rem' }}>
      <StatCard label="Finish" value={pos} colour={posColour} />
      <StatCard
        label="Best Lap"
        value={bestLap ? formatQualTime(bestLap.lap_duration) : '—'}
        sub={bestLap ? `Lap ${bestLap.lap_number}` : null}
      />
      <StatCard
        label="Pits"
        value={result?.pit_count ?? pitStops.length}
        sub={pitLaps || null}
      />
      <StatCard
        label="Gap"
        value={result?.position === 1 ? 'Winner' : (result?.gap_to_leader ?? '—')}
        colour={result?.position === 1 ? THEME.green : THEME.text}
      />
      {result?.points != null && (
        <StatCard label="Points" value={result.points} />
      )}
    </div>
  )
}

function RaceLapTable({ laps, stints, pitStops }) {
  if (!laps || laps.length === 0) return <p style={{ color: THEME.muted }}>No lap data.</p>
  const pitLapSet = new Set(pitStops.map(p => p.lap_number))

  return (
    <div style={{ overflowX: 'auto' }}>
      <table style={{ borderCollapse: 'collapse', width: '100%', fontSize: '0.82rem', color: THEME.text }}>
        <thead>
          <tr>
            {['Lap', 'Time', 'S1', 'S2', 'S3', 'Tyre', 'Pit', 'Trap'].map(h => (
              <th key={h} style={thStyle}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {laps.map((lap, i) => {
            const stint = getStintForLap(lap.lap_number, stints)
            const compound = (stint?.compound ?? 'UNKNOWN').toUpperCase()
            const age = tyreAgeAtLap(lap.lap_number, stint)
            const fresh = stint?.tyre_age_at_start === 0
            const isPit = pitLapSet.has(lap.lap_number)
            const isPitOut = lap.is_pit_out_lap
            const noTime = !lap.lap_duration || lap.lap_duration <= 0
            const dimStyle = isPitOut || noTime ? { opacity: 0.45 } : {}

            return (
              <tr
                key={lap.lap_number}
                style={{
                  borderBottom: `1px solid ${THEME.border}`,
                  background: i % 2 === 0 ? 'transparent' : THEME.tableAlt,
                  ...dimStyle,
                }}
              >
                <td style={{ ...tdStyle, color: THEME.muted }}>{lap.lap_number}</td>
                <td style={{ ...tdStyle, fontVariantNumeric: 'tabular-nums' }}>
                  {noTime ? '—' : formatQualTime(lap.lap_duration)}
                </td>
                <td style={{ ...tdStyle, color: THEME.muted, fontVariantNumeric: 'tabular-nums' }}>
                  {lap.duration_sector_1 != null ? lap.duration_sector_1.toFixed(3) : '—'}
                </td>
                <td style={{ ...tdStyle, color: THEME.muted, fontVariantNumeric: 'tabular-nums' }}>
                  {lap.duration_sector_2 != null ? lap.duration_sector_2.toFixed(3) : '—'}
                </td>
                <td style={{ ...tdStyle, color: THEME.muted, fontVariantNumeric: 'tabular-nums' }}>
                  {lap.duration_sector_3 != null ? lap.duration_sector_3.toFixed(3) : '—'}
                </td>
                <td style={tdStyle}>
                  {stint ? (
                    <span style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                      <CompoundBadge compound={compound} fresh={fresh} />
                      <span style={{ color: THEME.muted, fontSize: '0.75rem' }}>
                        {age != null ? `+${age}` : ''}
                      </span>
                    </span>
                  ) : '—'}
                </td>
                <td style={{ ...tdStyle, color: isPit ? THEME.red : THEME.muted }}>
                  {isPit ? 'PIT' : isPitOut ? 'OUT' : ''}
                </td>
                <td style={{ ...tdStyle, color: THEME.muted }}>
                  {lap.st_speed != null ? lap.st_speed : '—'}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function GapChart({ intervals, teamColour }) {
  if (!intervals || intervals.length === 0) return null

  // Exclude lapped cars and points where gap is 0 (driver is leading — those
  // drops to zero are real but create confusing visual artefacts mid-race)
  const plotData = intervals.filter(i => i.laps_down == null && i.gap_to_leader > 0)

  if (plotData.length === 0) return null

  const colour = teamColour ?? '#e10600'

  return (
    <Plot
      data={[{
        type: 'scatter',
        mode: 'lines',
        x: plotData.map(d => d.date),
        y: plotData.map(d => d.gap_to_leader),
        line: { color: colour, width: 2 },
        connectgaps: false,
        hovertemplate: 'Gap: +%{y:.3f}s<extra></extra>',
      }]}
      layout={{
        xaxis: {
          title: { text: 'Time', font: { size: 11 } },
          type: 'date',
          color: '#a1a1aa',
          gridcolor: 'rgba(255,255,255,0.06)',
          tickfont: { size: 10, color: '#a1a1aa' },
        },
        yaxis: {
          title: { text: 'Gap to Leader (s)', font: { size: 11 } },
          color: '#a1a1aa',
          gridcolor: 'rgba(255,255,255,0.06)',
          tickfont: { size: 10, color: '#a1a1aa' },
          rangemode: 'tozero',
        },
        margin: { l: 60, r: 20, t: 20, b: 45 },
        height: 280,
        paper_bgcolor: '#18181b',
        plot_bgcolor: '#18181b',
        font: { color: '#fafafa', family: 'monospace' },
      }}
      config={{ responsive: true, displayModeBar: false }}
      style={{ width: '100%' }}
    />
  )
}

function PositionChart({ positions, teamColour }) {
  if (!positions || positions.length === 0) return null

  const colour = teamColour ?? '#e10600'

  return (
    <Plot
      data={[{
        type: 'scatter',
        mode: 'lines',
        x: positions.map(p => p.date),
        y: positions.map(p => p.position),
        line: { color: colour, width: 2 },
        hovertemplate: 'P%{y}<extra></extra>',
      }]}
      layout={{
        xaxis: {
          title: { text: 'Time', font: { size: 11 } },
          type: 'date',
          color: '#a1a1aa',
          gridcolor: 'rgba(255,255,255,0.06)',
          tickfont: { size: 10, color: '#a1a1aa' },
        },
        yaxis: {
          title: { text: 'Position', font: { size: 11 } },
          range: [22, 1],
          color: '#a1a1aa',
          gridcolor: 'rgba(255,255,255,0.06)',
          tickfont: { size: 10, color: '#a1a1aa' },
          dtick: 2,
        },
        margin: { l: 50, r: 20, t: 20, b: 45 },
        height: 280,
        paper_bgcolor: '#18181b',
        plot_bgcolor: '#18181b',
        font: { color: '#fafafa', family: 'monospace' },
      }}
      config={{ responsive: true, displayModeBar: false }}
      style={{ width: '100%' }}
    />
  )
}

function RaceSubTab({ raceSessionKey, driverNumber }) {
  const { data, loading } = useDriverRaceData(raceSessionKey, driverNumber)
  const { safetyCarPeriods } = useRaceControl(raceSessionKey)

  if (loading) return <p style={{ color: THEME.muted }}>Loading race data…</p>
  if (!data) return <p style={{ color: THEME.muted }}>No data.</p>

  const { laps, stints, pitStops, result, intervals, positions, driver } = data
  const teamColour = driver?.team_colour ? `#${driver.team_colour.replace('#', '')}` : null

  return (
    <div>
      <RaceSummaryCard result={result} laps={laps} pitStops={pitStops} driver={driver} />

      <div style={{ marginBottom: '2rem' }}>
        <h3 style={sectionHeading}>Lap Times</h3>
        <div style={{ background: THEME.surface, borderRadius: '8px', border: `1px solid ${THEME.border}`, padding: '0.5rem' }}>
          <LapTimesChart
            laps={laps}
            stints={stints}
            pitStops={pitStops}
            safetyCarPeriods={safetyCarPeriods}
            mode="bars"
          />
        </div>
      </div>

      {positions.length > 0 && (
        <div style={{ marginBottom: '2rem' }}>
          <h3 style={sectionHeading}>Race Position</h3>
          <div style={{ background: THEME.surface, borderRadius: '8px', border: `1px solid ${THEME.border}`, padding: '0.5rem' }}>
            <PositionChart positions={positions} teamColour={teamColour} />
          </div>
        </div>
      )}

      {intervals.length > 0 && result?.position !== 1 && (
        <div style={{ marginBottom: '2rem' }}>
          <h3 style={sectionHeading}>Gap to Leader</h3>
          <div style={{ background: THEME.surface, borderRadius: '8px', border: `1px solid ${THEME.border}`, padding: '0.5rem' }}>
            <GapChart intervals={intervals} teamColour={teamColour} />
          </div>
        </div>
      )}

      <div style={{ marginBottom: '2rem' }}>
        <h3 style={sectionHeading}>Lap by Lap</h3>
        <div style={{ background: THEME.surface, borderRadius: '8px', border: `1px solid ${THEME.border}`, overflow: 'hidden' }}>
          <RaceLapTable laps={laps} stints={stints} pitStops={pitStops} />
        </div>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Qualifying sub-tab
// ---------------------------------------------------------------------------

function QualSummaryCard({ qualResult, gridPosition }) {
  if (!qualResult) return <p style={{ color: THEME.muted }}>No qualifying result.</p>

  return (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.75rem', marginBottom: '1.5rem' }}>
      <StatCard label="Grid" value={gridPosition != null ? `P${gridPosition}` : '—'} />
      {qualResult.q1_time != null && (
        <StatCard
          label="Q1"
          value={formatQualTime(qualResult.q1_time)}
          sub={qualResult.q1_compound ?? null}
        />
      )}
      {qualResult.q2_time != null && (
        <StatCard
          label="Q2"
          value={formatQualTime(qualResult.q2_time)}
          sub={qualResult.q2_compound ?? null}
        />
      )}
      {qualResult.q3_time != null && (
        <StatCard
          label="Q3"
          value={formatQualTime(qualResult.q3_time)}
          sub={qualResult.q3_compound ?? null}
        />
      )}
    </div>
  )
}

function QualLapTable({ laps, stints, phaseEvents }) {
  if (!laps || laps.length === 0) return <p style={{ color: THEME.muted }}>No lap data.</p>

  const phasedLaps = assignPhases(laps, phaseEvents)

  // Compute personal best per phase
  const phaseBest = {}
  for (const lap of phasedLaps) {
    if (!lap._phase || !lap.lap_duration || lap.lap_duration <= 0 || lap.lap_duration > 180) continue
    if (!phaseBest[lap._phase] || lap.lap_duration < phaseBest[lap._phase]) {
      phaseBest[lap._phase] = lap.lap_duration
    }
  }

  return (
    <div style={{ overflowX: 'auto' }}>
      <table style={{ borderCollapse: 'collapse', width: '100%', fontSize: '0.82rem', color: THEME.text }}>
        <thead>
          <tr>
            {['Lap', 'Phase', 'Time', 'Delta', 'S1', 'S2', 'S3', 'Tyre'].map(h => (
              <th key={h} style={thStyle}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {phasedLaps.map((lap, i) => {
            const stint = getStintForLap(lap.lap_number, stints)
            const compound = (stint?.compound ?? 'UNKNOWN').toUpperCase()
            const fresh = stint?.tyre_age_at_start === 0
            const noTime = !lap.lap_duration || lap.lap_duration <= 0
            const isOutIn = lap.lap_duration > 180
            const best = lap._phase ? phaseBest[lap._phase] : null
            const delta = best && lap.lap_duration && !isOutIn
              ? lap.lap_duration - best
              : null
            const isBest = delta === 0

            const dimStyle = isOutIn || noTime ? { opacity: 0.4 } : {}

            return (
              <tr
                key={`${lap.lap_number}-${i}`}
                style={{
                  borderBottom: `1px solid ${THEME.border}`,
                  background: i % 2 === 0 ? 'transparent' : THEME.tableAlt,
                  ...dimStyle,
                }}
              >
                <td style={{ ...tdStyle, color: THEME.muted }}>{lap.lap_number}</td>
                <td style={{ ...tdStyle, color: THEME.muted, fontSize: '0.72rem' }}>
                  {lap._phase ?? '—'}
                </td>
                <td style={{ ...tdStyle, fontVariantNumeric: 'tabular-nums', fontWeight: isBest ? 'bold' : 'normal', color: isBest ? THEME.green : THEME.text }}>
                  {noTime ? '—' : formatQualTime(lap.lap_duration)}
                </td>
                <td style={{ ...tdStyle, fontVariantNumeric: 'tabular-nums', color: isBest ? THEME.green : THEME.muted }}>
                  {isBest ? 'BEST' : delta != null ? `+${delta.toFixed(3)}` : '—'}
                </td>
                <td style={{ ...tdStyle, color: THEME.muted, fontVariantNumeric: 'tabular-nums' }}>
                  {lap.duration_sector_1 != null ? lap.duration_sector_1.toFixed(3) : '—'}
                </td>
                <td style={{ ...tdStyle, color: THEME.muted, fontVariantNumeric: 'tabular-nums' }}>
                  {lap.duration_sector_2 != null ? lap.duration_sector_2.toFixed(3) : '—'}
                </td>
                <td style={{ ...tdStyle, color: THEME.muted, fontVariantNumeric: 'tabular-nums' }}>
                  {lap.duration_sector_3 != null ? lap.duration_sector_3.toFixed(3) : '—'}
                </td>
                <td style={tdStyle}>
                  {stint ? <CompoundBadge compound={compound} fresh={fresh} /> : '—'}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function QualSubTab({ qualifyingSessionKey, driverNumber }) {
  const { data, loading } = useDriverQualifyingData(qualifyingSessionKey, driverNumber)

  if (loading) return <p style={{ color: THEME.muted }}>Loading qualifying data…</p>
  if (!data) return <p style={{ color: THEME.muted }}>No data.</p>

  const { laps, stints, qualResult, phaseEvents, gridPosition } = data

  return (
    <div>
      <QualSummaryCard qualResult={qualResult} gridPosition={gridPosition} />

      <div style={{ marginBottom: '2rem' }}>
        <h3 style={sectionHeading}>Lap Times</h3>
        <div style={{ background: THEME.surface, borderRadius: '8px', border: `1px solid ${THEME.border}`, padding: '0.5rem' }}>
          <LapTimesChart laps={laps.filter(l => l.lap_duration && l.lap_duration <= 180)} stints={stints} mode="scatter" phaseEvents={phaseEvents} />
        </div>
      </div>

      <div style={{ marginBottom: '2rem' }}>
        <h3 style={sectionHeading}>Lap by Lap</h3>
        <div style={{ background: THEME.surface, borderRadius: '8px', border: `1px solid ${THEME.border}`, overflow: 'hidden' }}>
          <QualLapTable laps={laps} stints={stints} phaseEvents={phaseEvents} />
        </div>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main DriverPage component
// ---------------------------------------------------------------------------

export default function DriverPage({ raceSessionKey, qualifyingSessionKey, gmtOffset }) {
  const [drivers, setDrivers] = useState([])
  const [selectedDriverNumber, setSelectedDriverNumber] = useState(null)
  const [activeSubTab, setActiveSubTab] = useState('Race')

  // Load driver list from race session (or qualifying if no race)
  const sessionForDrivers = raceSessionKey ?? qualifyingSessionKey
  useEffect(() => {
    if (!sessionForDrivers) return
    supabase
      .from('drivers')
      .select('driver_number, name_acronym, full_name, team_name, team_colour, headshot_url')
      .eq('session_key', sessionForDrivers)
      .order('driver_number')
      .then(({ data }) => {
        const list = data ?? []
        setDrivers(list)

        // Initialise from URL or default to first driver
        const params = new URLSearchParams(window.location.search)
        const driverParam = params.get('driver')
        const fromUrl = driverParam ? list.find(d => d.driver_number === parseInt(driverParam)) : null
        setSelectedDriverNumber(fromUrl?.driver_number ?? list[0]?.driver_number ?? null)
      })
  }, [sessionForDrivers])

  // Sync driver selection to URL
  useEffect(() => {
    if (selectedDriverNumber == null) return
    const params = new URLSearchParams(window.location.search)
    params.set('driver', selectedDriverNumber)
    window.history.replaceState({}, '', `?${params}`)
  }, [selectedDriverNumber])

  if (!raceSessionKey && !qualifyingSessionKey) {
    return <p style={{ color: THEME.muted }}>Select a weekend above.</p>
  }

  const selectedDriver = drivers.find(d => d.driver_number === selectedDriverNumber)
  const teamColour = selectedDriver?.team_colour
    ? `#${selectedDriver.team_colour.replace('#', '')}`
    : THEME.muted

  const selectStyle = {
    background: THEME.inputBg,
    color: THEME.text,
    border: `1px solid ${THEME.border}`,
    borderRadius: '6px',
    padding: '0.3rem 0.5rem',
    fontSize: '0.875rem',
    cursor: 'pointer',
    outline: 'none',
    fontFamily: 'monospace',
  }

  return (
    <div>
      {/* Driver selector + headshot */}
      <div style={{ display: 'flex', alignItems: 'center', gap: '1.25rem', marginBottom: '1.25rem' }}>
        {selectedDriver?.headshot_url && (
          <img
            src={selectedDriver.headshot_url}
            alt={formatDriverName(selectedDriver.full_name) ?? selectedDriver.name_acronym}
            style={{
              height: '72px',
              width: 'auto',
              objectFit: 'contain',
              borderRadius: '6px',
              background: THEME.surface,
              border: `1px solid ${THEME.border}`,
            }}
          />
        )}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '0.3rem' }}>
          {selectedDriver && (
            <div style={{ fontSize: '1.1rem', fontWeight: 'bold', color: teamColour }}>
              {formatDriverName(selectedDriver.full_name) ?? selectedDriver.name_acronym}
            </div>
          )}
          {selectedDriver && (
            <div style={{ fontSize: '0.78rem', color: THEME.muted }}>{selectedDriver.team_name}</div>
          )}
          <select
            style={selectStyle}
            value={selectedDriverNumber ?? ''}
            onChange={e => setSelectedDriverNumber(Number(e.target.value))}
          >
            {drivers.map(d => (
              <option key={d.driver_number} value={d.driver_number}>
                {formatDriverName(d.full_name) ?? d.name_acronym} — {d.team_name}
              </option>
            ))}
          </select>
        </div>
      </div>

      {/* Sub-tab bar */}
      <div style={{
        display: 'flex',
        borderBottom: `1px solid ${THEME.border}`,
        marginBottom: '1.25rem',
      }}>
        {['Race', 'Qualifying'].map(tab => (
          <TabButton key={tab} label={tab} active={activeSubTab === tab} onClick={() => setActiveSubTab(tab)} />
        ))}
      </div>

      {/* Sub-tab content */}
      {selectedDriverNumber != null && (
        <>
          {activeSubTab === 'Race' && raceSessionKey && (
            <RaceSubTab raceSessionKey={raceSessionKey} driverNumber={selectedDriverNumber} />
          )}
          {activeSubTab === 'Race' && !raceSessionKey && (
            <p style={{ color: THEME.muted }}>No race session for this weekend.</p>
          )}
          {activeSubTab === 'Qualifying' && qualifyingSessionKey && (
            <QualSubTab qualifyingSessionKey={qualifyingSessionKey} driverNumber={selectedDriverNumber} />
          )}
          {activeSubTab === 'Qualifying' && !qualifyingSessionKey && (
            <p style={{ color: THEME.muted }}>No qualifying session for this weekend.</p>
          )}
        </>
      )}
    </div>
  )
}
