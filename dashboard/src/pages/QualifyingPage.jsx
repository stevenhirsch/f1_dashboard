import { useState } from 'react'
import { useQualifyingData } from '../hooks/useQualifyingData'
import { formatQualTime, formatDelta, computeSectorDeltas, computePhaseStints, assignPhases, normalizePhase } from '../utils/qualifying'
import { COMPOUND_COLOURS } from '../utils/compounds'
import SectorDeltaHeatmap from '../plots/SectorDeltaHeatmap'
import WeatherStrip from '../plots/WeatherStrip'

const THEME = {
  bg: '#09090b',
  surface: '#18181b',
  border: 'rgba(255,255,255,0.08)',
  text: '#fafafa',
  muted: '#a1a1aa',
  red: '#e10600',
  tableAlt: '#1c1c1f',
  separator: 'rgba(161,161,170,0.25)',
}

function CompoundBadge({ compound }) {
  if (!compound) return null
  const bg = COMPOUND_COLOURS[(compound ?? '').toUpperCase()] ?? COMPOUND_COLOURS.UNKNOWN
  return (
    <span style={{
      display: 'inline-block',
      background: bg,
      color: '#000',
      fontSize: '0.65rem',
      fontWeight: 'bold',
      borderRadius: '3px',
      padding: '0 4px',
      marginLeft: '4px',
      verticalAlign: 'middle',
      lineHeight: '1.5',
    }}>
      {compound.toUpperCase() === 'UNKNOWN' ? '?' : compound.slice(0, 1)}
    </span>
  )
}

function QualifyingResultsTable({ results, driverPhaseMap, topSpeedByDriver }) {
  if (!results || results.length === 0) return <p style={{ color: THEME.muted }}>No qualifying results.</p>

  const totalLaps = results.reduce((sum, r) => {
    return sum + (r.q1_laps ?? 0) + (r.q2_laps ?? 0) + (r.q3_laps ?? 0)
  }, 0)
  const showLaps = totalLaps > 0

  const bestTopSpeed = Math.max(...results.map(r => topSpeedByDriver?.[r.driver_number] ?? 0))

  const headers = ['Pos', 'Driver', 'Team', 'Q1', 'Q2', 'Q3', 'Top Speed', ...(showLaps ? ['Laps'] : [])]

  const thStyle = {
    padding: '0.4rem 0.6rem',
    whiteSpace: 'nowrap',
    color: THEME.muted,
    fontWeight: 'normal',
    fontSize: '0.72rem',
    textTransform: 'uppercase',
    letterSpacing: '0.05em',
    textAlign: 'left',
  }
  const tdStyle = (extra = {}) => ({
    padding: '0.4rem 0.6rem',
    whiteSpace: 'nowrap',
    ...extra,
  })

  const rows = []
  results.forEach((r, i) => {
    const pos = i + 1
    const driver = r.driver ?? {}
    const rawColour = driver.team_colour ?? '888888'
    const colour = `#${rawColour.replace('#', '')}`

    const totalDriverLaps = (r.q1_laps ?? 0) + (r.q2_laps ?? 0) + (r.q3_laps ?? 0)

    // Dynamic separator: show when this driver reached a lower phase than the previous one.
    // Prefer DB q_times; fall back to client-side phase detection from laps.
    const phaseRank = { Q3: 3, Q2: 2, Q1: 1 }
    const resolvePhase = (row) => {
      if (row.q3_time != null) return 'Q3'
      if (row.q2_time != null) return 'Q2'
      return driverPhaseMap[row.driver_number] ?? 'Q1'
    }
    if (i > 0) {
      const prevPhase = resolvePhase(results[i - 1])
      const curPhase = resolvePhase(r)
      if (prevPhase !== curPhase) {
        const label = prevPhase === 'Q3' ? 'eliminated after Q2' : 'eliminated after Q1'
        rows.push(
          <tr key={`sep-${i}`} style={{ borderTop: `1px solid ${THEME.separator}` }}>
            <td
              colSpan={headers.length}
              style={{
                padding: '0.2rem 0.6rem',
                fontSize: '0.65rem',
                color: THEME.separator,
                textTransform: 'uppercase',
                letterSpacing: '0.08em',
              }}
            >
              {label}
            </td>
          </tr>
        )
      }
    }

    rows.push(
      <tr
        key={r.driver_number}
        style={{
          borderBottom: `1px solid ${THEME.border}`,
          background: i % 2 === 0 ? 'transparent' : THEME.tableAlt,
        }}
      >
        <td style={tdStyle({ color: THEME.muted, fontWeight: 'bold' })}>{pos}</td>
        <td style={tdStyle({ color: colour, fontWeight: 'bold' })}>
          {driver.name_acronym ?? r.driver_number}
        </td>
        <td style={tdStyle({ color: THEME.muted })}>{driver.team_name ?? '—'}</td>

        <td style={tdStyle()}>{formatQualTime(r.q1_time)}</td>
        <td style={tdStyle()}>{formatQualTime(r.q2_time)}</td>
        <td style={tdStyle()}>{formatQualTime(r.q3_time)}</td>

        {(() => {
          const spd = topSpeedByDriver?.[r.driver_number] ?? null
          const isTop = spd != null && spd === bestTopSpeed
          return (
            <td style={tdStyle({ fontWeight: isTop ? 'bold' : 'normal', color: isTop ? '#facc15' : THEME.text })}>
              {spd != null ? spd : '—'}
            </td>
          )
        })()}

        {showLaps && (
          <td style={tdStyle({ color: THEME.muted })}>{totalDriverLaps || '—'}</td>
        )}
      </tr>
    )
  })

  return (
    <table style={{ borderCollapse: 'collapse', width: '100%', fontSize: '0.85rem', color: THEME.text }}>
      <thead>
        <tr style={{ borderBottom: `1px solid ${THEME.border}` }}>
          {headers.map(h => <th key={h} style={thStyle}>{h}</th>)}
        </tr>
      </thead>
      <tbody>{rows}</tbody>
    </table>
  )
}

function PhaseStintTable({ driverRows, driverMap, driverPhaseMap, activePhase, topSpeedByDriver }) {
  if (!driverRows || driverRows.length === 0) return null

  const phaseRankMap = { Q1: 1, Q2: 2, Q3: 3 }
  const activeRank = phaseRankMap[activePhase] ?? 1

  const sortByBest = (a, b) => (a.best_time ?? 999999) - (b.best_time ?? 999999)
  const advanced = driverRows
    .filter(r => phaseRankMap[driverPhaseMap?.[r.driver_number] ?? 'Q1'] > activeRank)
    .sort(sortByBest)
  const eliminated = driverRows
    .filter(r => phaseRankMap[driverPhaseMap?.[r.driver_number] ?? 'Q1'] <= activeRank)
    .sort(sortByBest)

  const showSeparator = activePhase !== 'Q3' && advanced.length > 0 && eliminated.length > 0
  const sepLabel = activePhase === 'Q1' ? 'eliminated after Q1' : 'eliminated after Q2'
  const orderedRows = [...advanced, ...eliminated]

  const leaderTime = orderedRows[0]?.best_time

  const thStyle = {
    padding: '0.4rem 0.6rem',
    whiteSpace: 'nowrap',
    color: THEME.muted,
    fontWeight: 'normal',
    fontSize: '0.72rem',
    textTransform: 'uppercase',
    letterSpacing: '0.05em',
    textAlign: 'left',
  }
  const tdStyle = (extra = {}) => ({
    padding: '0.4rem 0.6rem',
    whiteSpace: 'nowrap',
    ...extra,
  })

  const bestTopSpeed = Math.max(...orderedRows.map(r => topSpeedByDriver?.[r.driver_number] ?? 0))

  return (
    <table style={{ borderCollapse: 'collapse', width: '100%', fontSize: '0.85rem', color: THEME.text }}>
      <thead>
        <tr style={{ borderBottom: `1px solid ${THEME.border}` }}>
          <th style={thStyle}>Pos</th>
          <th style={thStyle}>Driver</th>
          <th style={thStyle}>Team</th>
          <th style={thStyle}>Tyres</th>
          <th style={thStyle}>Best</th>
          <th style={thStyle}>Gap</th>
          <th style={thStyle}>Top Speed</th>
        </tr>
      </thead>
      <tbody>
        {orderedRows.map((row, i) => {
          const colour = `#${(row.team_colour ?? '888888').replace('#', '')}`
          const gap = row.best_time != null && leaderTime != null ? row.best_time - leaderTime : null
          const teamName = driverMap?.[row.driver_number]?.team_name ?? '—'
          const isFirstEliminated = showSeparator && i === advanced.length
          const topSpeed = topSpeedByDriver?.[row.driver_number] ?? null
          const isTopSpeed = topSpeed != null && topSpeed === bestTopSpeed
          return (
            <>
              {isFirstEliminated && (
                <tr key={`sep-${i}`} style={{ borderTop: `1px solid ${THEME.separator}` }}>
                  <td
                    colSpan={7}
                    style={{
                      padding: '0.2rem 0.6rem',
                      fontSize: '0.65rem',
                      color: THEME.separator,
                      textTransform: 'uppercase',
                      letterSpacing: '0.08em',
                    }}
                  >
                    {sepLabel}
                  </td>
                </tr>
              )}
              <tr
                key={row.driver_number}
                style={{
                  borderBottom: `1px solid ${THEME.border}`,
                  background: i % 2 === 0 ? 'transparent' : THEME.tableAlt,
                }}
              >
                <td style={tdStyle({ color: THEME.muted, fontWeight: 'bold' })}>{i + 1}</td>
                <td style={tdStyle({ color: colour, fontWeight: 'bold' })}>{row.driver}</td>
                <td style={tdStyle({ color: THEME.muted })}>{teamName}</td>
                <td style={tdStyle()}>
                  {row.stints.map((stint, j) => {
                    const isFresh = (stint.tyre_age_at_start ?? 0) === 0
                    return (
                      <span key={j} style={{ marginRight: '4px' }}>
                        <CompoundBadge compound={stint.compound} />
                        <span style={{ color: THEME.muted, fontSize: '0.68rem', marginLeft: '1px' }}>
                          {isFresh ? 'F' : 'U'}
                        </span>
                      </span>
                    )
                  })}
                </td>
                <td style={tdStyle({ fontWeight: 'bold' })}>{formatQualTime(row.best_time)}</td>
                <td style={tdStyle({ color: THEME.muted })}>
                  {gap === 0 ? 'LEADER' : formatDelta(gap)}
                </td>
                <td style={tdStyle({ fontWeight: isTopSpeed ? 'bold' : 'normal', color: isTopSpeed ? '#facc15' : THEME.text })}>
                  {topSpeed != null ? `${topSpeed}` : '—'}
                </td>
              </tr>
            </>
          )
        })}
      </tbody>
    </table>
  )
}

function PhaseTabView({ laps, phaseEvents, stintsByDriver, driverMap, weatherData, gmtOffset, driverPhaseMap }) {
  const phasedLaps = assignPhases(laps ?? [], phaseEvents ?? [])
  const phaseSet = new Set(phasedLaps.map(l => l._phase).filter(Boolean))
  const availablePhases = ['Q1', 'Q2', 'Q3'].filter(p => phaseSet.has(p))

  const [activePhase, setActivePhase] = useState(availablePhases[0] ?? 'Q1')

  if (availablePhases.length === 0) {
    return <p style={{ color: THEME.muted }}>No phase data available.</p>
  }

  const surfaceStyle = {
    background: THEME.surface,
    borderRadius: '8px',
    border: `1px solid ${THEME.border}`,
  }

  // Compute phase time windows from sorted phase events
  const sortedEvents = [...(phaseEvents ?? [])]
    .map(e => ({ ...e, _phase: normalizePhase(e.qualifying_phase) }))
    .filter(e => e._phase && e.date)
    .sort((a, b) => (a.date < b.date ? -1 : 1))
  const phaseOrder = ['Q1', 'Q2', 'Q3']
  const phaseStart = {}
  for (const e of sortedEvents) {
    if (!phaseStart[e._phase]) phaseStart[e._phase] = e.date
  }
  const activeStart = phaseStart[activePhase] ?? null
  const nextPhase = phaseOrder[phaseOrder.indexOf(activePhase) + 1] ?? null
  const activeEnd = nextPhase ? (phaseStart[nextPhase] ?? null) : null

  const allWeather = weatherData ?? []
  const phaseWeatherFiltered = allWeather.filter(w => {
    if (activeStart && w.date < activeStart) return false
    if (activeEnd && w.date >= activeEnd) return false
    return true
  })
  // Fall back to full session weather if phase filtering yields nothing
  const phaseWeather = phaseWeatherFiltered.length > 0 ? phaseWeatherFiltered : allWeather

  const phaseRows = computePhaseStints(laps ?? [], phaseEvents ?? [], stintsByDriver ?? {}, driverMap ?? {}, activePhase)
  const phaseDeltas = computeSectorDeltas(laps ?? [], phaseEvents ?? [], driverMap ?? {}, activePhase)

  // Top speed per driver for the active phase (max st_speed across phase laps)
  const phasedLapsAll = assignPhases(laps ?? [], phaseEvents ?? [])
  const topSpeedByDriver = {}
  for (const lap of phasedLapsAll) {
    if (lap._phase !== activePhase || lap.st_speed == null) continue
    const dn = lap.driver_number
    if (topSpeedByDriver[dn] == null || lap.st_speed > topSpeedByDriver[dn]) {
      topSpeedByDriver[dn] = lap.st_speed
    }
  }

  return (
    <div>
      <div style={{ display: 'flex', gap: '0.4rem', marginBottom: '1rem' }}>
        {availablePhases.map(p => (
          <button
            key={p}
            onClick={() => setActivePhase(p)}
            style={{
              padding: '0.3rem 0.9rem',
              fontSize: '0.75rem',
              fontWeight: 'bold',
              border: `1px solid ${activePhase === p ? THEME.text : THEME.border}`,
              borderRadius: '4px',
              background: activePhase === p ? THEME.text : 'transparent',
              color: activePhase === p ? THEME.bg : THEME.muted,
              cursor: 'pointer',
            }}
          >
            {p}
          </button>
        ))}
      </div>

      <div style={{ ...surfaceStyle, overflow: 'hidden', marginBottom: '1.5rem' }}>
        <PhaseStintTable driverRows={phaseRows} driverMap={driverMap} driverPhaseMap={driverPhaseMap} activePhase={activePhase} topSpeedByDriver={topSpeedByDriver} />
      </div>

      <div style={{ ...surfaceStyle, overflow: 'hidden', marginBottom: '1.5rem' }}>
        <WeatherStrip gmtOffset={gmtOffset} weatherData={phaseWeather} />
      </div>

      <p style={{ color: THEME.muted, fontSize: '0.78rem', marginBottom: '0.5rem', marginTop: 0 }}>
        Best sector time per driver in {activePhase}. Color = time lost vs. fastest sector (green = matched fastest, red = slower).
      </p>
      <div style={{ ...surfaceStyle, padding: '0.5rem' }}>
        <SectorDeltaHeatmap deltas={phaseDeltas} />
      </div>
    </div>
  )
}

export default function QualifyingPage({ sessionKey, gmtOffset }) {
  const { data, loading } = useQualifyingData(sessionKey)

  if (!sessionKey) return <p style={{ color: THEME.muted }}>Select a session above.</p>

  const sectionStyle = { marginBottom: '2rem' }
  const headingStyle = {
    fontSize: '0.75rem',
    fontWeight: 'bold',
    marginBottom: '0.75rem',
    color: THEME.muted,
    textTransform: 'uppercase',
    letterSpacing: '0.1em',
    borderBottom: `1px solid ${THEME.border}`,
    paddingBottom: '0.4rem',
  }
  const surfaceStyle = {
    background: THEME.surface,
    borderRadius: '8px',
    border: `1px solid ${THEME.border}`,
  }

  if (loading) {
    return <p style={{ color: THEME.muted }}>Loading qualifying data…</p>
  }

  const { results, laps, phaseEvents, driverMap, stintsByDriver, weatherData } = data ?? {}

  // Derive highest phase reached per driver from laps (works even before re-ingestion)
  const phasedLaps = (laps && phaseEvents) ? assignPhases(laps, phaseEvents) : []
  const phaseRankMap = { Q1: 1, Q2: 2, Q3: 3 }
  const driverPhaseMap = {}
  for (const lap of phasedLaps) {
    const dn = lap.driver_number
    const phase = lap._phase
    if (!phase) continue
    if (!driverPhaseMap[dn] || phaseRankMap[phase] > phaseRankMap[driverPhaseMap[dn]]) {
      driverPhaseMap[dn] = phase
    }
  }

  // Overall top speed per driver across all qualifying laps
  const overallTopSpeedByDriver = {}
  for (const lap of laps ?? []) {
    if (lap.st_speed == null) continue
    const dn = lap.driver_number
    if (overallTopSpeedByDriver[dn] == null || lap.st_speed > overallTopSpeedByDriver[dn]) {
      overallTopSpeedByDriver[dn] = lap.st_speed
    }
  }

  return (
    <div>
      <div style={sectionStyle}>
        <h2 style={headingStyle}>Qualifying Results</h2>
        <div style={{ ...surfaceStyle, overflow: 'hidden' }}>
          <QualifyingResultsTable results={results} driverPhaseMap={driverPhaseMap} topSpeedByDriver={overallTopSpeedByDriver} />
        </div>
      </div>

      <div style={sectionStyle}>
        <h2 style={headingStyle}>Phase Analysis</h2>
        <PhaseTabView
          laps={laps}
          phaseEvents={phaseEvents}
          stintsByDriver={stintsByDriver}
          driverMap={driverMap}
          weatherData={weatherData}
          gmtOffset={gmtOffset}
          driverPhaseMap={driverPhaseMap}
        />
      </div>

    </div>
  )
}
