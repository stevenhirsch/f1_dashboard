import { useState } from 'react'
import InfoTooltip from '../components/InfoTooltip'
import { useSeasonSummary } from '../hooks/useSeasonSummary'
import DriverPointsTrendChart from '../plots/DriverPointsTrendChart'
import ConstructorPointsTrendChart from '../plots/ConstructorPointsTrendChart'
import PitStopViolinChart, { usePitStopStats } from '../plots/PitStopViolinChart'

const fmtWPct = (wins, entries) => {
  if (wins == null || !entries) return '—'
  return `${((wins / entries) * 100).toFixed(0)}%`
}

const THEME = {
  bg: '#09090b',
  surface: '#18181b',
  border: 'rgba(255,255,255,0.08)',
  text: '#fafafa',
  muted: '#a1a1aa',
  red: '#e10600',
  tableAlt: '#1c1c1f',
  gold: '#f59e0b',
}

const fmt1 = v => (v == null ? '—' : Number(v).toFixed(1))
const fmt2 = v => (v == null ? '—' : Number(v).toFixed(2))
const fmtInt = v => (v == null ? '—' : Math.round(v).toLocaleString())
const fmtDist = v => (v == null ? '—' : `${Math.round(v).toLocaleString()} km`)
const fmtPct = v => (v == null ? '—' : `${Number(v).toFixed(1)}%`)

function SectionHeader({ children }) {
  return (
    <div style={{
      fontSize: '0.7rem',
      fontWeight: 'normal',
      color: THEME.muted,
      textTransform: 'uppercase',
      letterSpacing: '0.08em',
      marginBottom: '0.6rem',
      marginTop: '1.5rem',
    }}>
      {children}
    </div>
  )
}

function SubTabBar({ tabs, active, onChange, isMobile }) {
  return (
    <div style={{
      display: 'inline-flex',
      gap: '0.25rem',
      background: THEME.surface,
      border: `1px solid ${THEME.border}`,
      borderRadius: '8px',
      padding: '3px',
      marginBottom: '1.25rem',
    }}>
      {tabs.map(tab => (
        <button
          key={tab}
          onClick={() => onChange(tab)}
          style={{
            padding: isMobile ? '0.35rem 0.75rem' : '0.35rem 1.1rem',
            border: 'none',
            borderRadius: '6px',
            background: active === tab ? THEME.red : 'transparent',
            color: active === tab ? '#fff' : THEME.muted,
            fontFamily: 'monospace',
            fontSize: isMobile ? '0.78rem' : '0.82rem',
            fontWeight: active === tab ? 'bold' : 'normal',
            cursor: 'pointer',
            letterSpacing: '0.03em',
            transition: 'background 0.15s',
          }}
        >
          {tab}
        </button>
      ))}
    </div>
  )
}

function TableWrapper({ children }) {
  return (
    <div style={{ overflowX: 'auto', WebkitOverflowScrolling: 'touch' }}>
      {children}
    </div>
  )
}

function Th({ children, isMobile, right }) {
  return (
    <th style={{
      padding: isMobile ? '0.3rem 0.4rem' : '0.4rem 0.65rem',
      whiteSpace: 'nowrap',
      color: THEME.muted,
      fontWeight: 'normal',
      fontSize: isMobile ? '0.65rem' : '0.72rem',
      textTransform: 'uppercase',
      letterSpacing: '0.05em',
      textAlign: right ? 'right' : 'left',
    }}>{children}</th>
  )
}

function Td({ children, isMobile, right, style }) {
  return (
    <td style={{
      padding: isMobile ? '0.3rem 0.4rem' : '0.4rem 0.65rem',
      whiteSpace: 'nowrap',
      fontSize: isMobile ? '0.78rem' : '0.84rem',
      textAlign: right ? 'right' : 'left',
      ...style,
    }}>{children}</td>
  )
}

function DriverStatsTable({ driverRows, isMobile }) {
  if (!driverRows?.length) return <p style={{ color: THEME.muted }}>No data.</p>

  const totalSeasonLaps = Math.max(...driverRows.map(r => r.laps_completed ?? 0))

  const teamH2HTotal = {}
  for (const row of driverRows) {
    const team = row.meta?.team_name
    if (team) teamH2HTotal[team] = (teamH2HTotal[team] ?? 0) + (row.wins_over_teammate ?? 0)
  }

  const headers = ['Pos', 'Driver', 'Team', 'Pts', 'Race Pts', 'Sprint Pts', '% of Team', 'Wins', 'Podiums', 'FL', 'DNF', 'DNS', 'DSQ', 'W vs TM', 'W% vs TM', 'Laps Led', 'Led %', 'Pos Gained', 'Pos Lost', 'Laps', 'Distance']

  return (
    <TableWrapper>
      <table style={{ borderCollapse: 'collapse', width: '100%', color: THEME.text }}>
        <thead>
          <tr style={{ borderBottom: `1px solid ${THEME.border}` }}>
            {headers.map((h, i) => <Th key={h} isMobile={isMobile} right={i >= 3}>{h}</Th>)}
          </tr>
        </thead>
        <tbody>
          {driverRows.map((row, i) => {
            const meta = row.meta ?? {}
            const colour = meta.team_colour ?? '#888888'
            return (
              <tr
                key={row.driver_number}
                style={{
                  borderBottom: `1px solid ${THEME.border}`,
                  background: i % 2 === 0 ? 'transparent' : THEME.tableAlt,
                }}
              >
                <Td isMobile={isMobile} style={{ color: THEME.muted, fontWeight: 'bold' }}>
                  {row.position}
                </Td>
                <Td isMobile={isMobile}>
                  <span style={{
                    display: 'inline-block',
                    width: 3,
                    height: '1em',
                    background: colour,
                    borderRadius: 2,
                    marginRight: 6,
                    verticalAlign: 'middle',
                  }} />
                  {meta.full_name ?? `#${row.driver_number}`}
                </Td>
                <Td isMobile={isMobile} style={{ color: THEME.muted }}>{meta.team_name ?? '—'}</Td>
                <Td isMobile={isMobile} right style={{ fontWeight: 'bold' }}>{fmtInt(row.points_scored)}</Td>
                <Td isMobile={isMobile} right>{fmtInt(row.race_points)}</Td>
                <Td isMobile={isMobile} right>{row.sprint_entries > 0 ? fmtInt(row.sprint_points) : '—'}</Td>
                <Td isMobile={isMobile} right>{fmtPct(row.percent_of_team_points)}</Td>
                <Td isMobile={isMobile} right>{fmtInt(row.wins)}</Td>
                <Td isMobile={isMobile} right>{fmtInt(row.podiums)}</Td>
                <Td isMobile={isMobile} right>{fmtInt(row.fastest_laps)}</Td>
                <Td isMobile={isMobile} right style={{ color: row.dnf_count > 0 ? '#ef4444' : THEME.text }}>{fmtInt(row.dnf_count)}</Td>
                <Td isMobile={isMobile} right style={{ color: row.dns_count > 0 ? '#ef4444' : THEME.text }}>{fmtInt(row.dns_count)}</Td>
                <Td isMobile={isMobile} right style={{ color: row.dsq_count > 0 ? '#ef4444' : THEME.text }}>{fmtInt(row.dsq_count)}</Td>
                <Td isMobile={isMobile} right>{fmtInt(row.wins_over_teammate)}</Td>
                <Td isMobile={isMobile} right>{fmtWPct(row.wins_over_teammate, teamH2HTotal[meta.team_name])}</Td>
                <Td isMobile={isMobile} right>{fmtInt(row.laps_led)}</Td>
                <Td isMobile={isMobile} right>{fmtWPct(row.laps_led, totalSeasonLaps)}</Td>
                <Td isMobile={isMobile} right>{fmtInt(row.total_overtakes_made)}</Td>
                <Td isMobile={isMobile} right>{fmtInt(row.total_overtakes_suffered)}</Td>
                <Td isMobile={isMobile} right>{fmtInt(row.laps_completed)}</Td>
                <Td isMobile={isMobile} right>{fmtDist(row.distance_km)}</Td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </TableWrapper>
  )
}

function ConstructorStatsTable({ constructorRows, constructorColour, totalSeasonLaps, isMobile }) {
  if (!constructorRows?.length) return <p style={{ color: THEME.muted }}>No data.</p>

  const headers = ['Pos', 'Constructor', 'Pts', 'Race Pts', 'Sprint Pts', 'Wins', 'Podiums', 'DNF', 'Laps Led', 'Led %', 'Total Laps', 'Distance']

  return (
    <TableWrapper>
      <table style={{ borderCollapse: 'collapse', width: '100%', color: THEME.text }}>
        <thead>
          <tr style={{ borderBottom: `1px solid ${THEME.border}` }}>
            {headers.map((h, i) => <Th key={h} isMobile={isMobile} right={i >= 2}>{h}</Th>)}
          </tr>
        </thead>
        <tbody>
          {constructorRows.map((row, i) => {
            const colour = constructorColour?.[row.team_name] ?? '#888888'
            return (
              <tr
                key={row.team_name}
                style={{
                  borderBottom: `1px solid ${THEME.border}`,
                  background: i % 2 === 0 ? 'transparent' : THEME.tableAlt,
                }}
              >
                <Td isMobile={isMobile} style={{ color: THEME.muted, fontWeight: 'bold' }}>
                  {row.position}
                </Td>
                <Td isMobile={isMobile}>
                  <span style={{
                    display: 'inline-block',
                    width: 3,
                    height: '1em',
                    background: colour,
                    borderRadius: 2,
                    marginRight: 6,
                    verticalAlign: 'middle',
                  }} />
                  {row.team_name}
                </Td>
                <Td isMobile={isMobile} right style={{ fontWeight: 'bold' }}>{fmtInt(row.points_scored)}</Td>
                <Td isMobile={isMobile} right>{fmtInt(row.race_points)}</Td>
                <Td isMobile={isMobile} right>{row.sprint_entries > 0 ? fmtInt(row.sprint_points) : '—'}</Td>
                <Td isMobile={isMobile} right>{fmtInt(row.wins)}</Td>
                <Td isMobile={isMobile} right>{fmtInt(row.podiums)}</Td>
                <Td isMobile={isMobile} right style={{ color: row.dnf_count > 0 ? '#ef4444' : THEME.text }}>{fmtInt(row.dnf_count)}</Td>
                <Td isMobile={isMobile} right>{fmtInt(row.laps_led)}</Td>
                <Td isMobile={isMobile} right>{fmtWPct(row.laps_led, totalSeasonLaps)}</Td>
                <Td isMobile={isMobile} right>{fmtInt(row.laps_completed)}</Td>
                <Td isMobile={isMobile} right>{fmtDist(row.distance_km)}</Td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </TableWrapper>
  )
}

function PitStopStatsTable({ pitStopsByTeam, constructorColour, isMobile }) {
  const { entries, globalFastest, globalFastestMean } = usePitStopStats(pitStopsByTeam)
  if (!entries.length) return null

  const headers = ['Constructor', 'Fastest', 'Mean', 'Std Dev', 'Median', 'IQR', 'Stops']

  return (
    <TableWrapper>
      <table style={{ borderCollapse: 'collapse', width: '100%', color: THEME.text }}>
        <thead>
          <tr style={{ borderBottom: `1px solid ${THEME.border}` }}>
            {headers.map((h, i) => <Th key={h} isMobile={isMobile} right={i >= 1}>{h}</Th>)}
          </tr>
        </thead>
        <tbody>
          {entries.map(({ team, stats }, i) => {
            const colour = constructorColour?.[team] ?? '#888888'
            const isFastest = stats.fastest === globalFastest
            const isFastestMean = Math.abs(stats.mean - globalFastestMean) < 0.0001
            return (
              <tr
                key={team}
                style={{
                  borderBottom: `1px solid ${THEME.border}`,
                  background: i % 2 === 0 ? 'transparent' : THEME.tableAlt,
                }}
              >
                <Td isMobile={isMobile}>
                  <span style={{
                    display: 'inline-block',
                    width: 3,
                    height: '1em',
                    background: colour,
                    borderRadius: 2,
                    marginRight: 6,
                    verticalAlign: 'middle',
                  }} />
                  {team}
                </Td>
                <Td isMobile={isMobile} right style={isFastest ? { color: THEME.gold, fontWeight: 'bold' } : {}}>
                  {fmt2(stats.fastest)}s
                </Td>
                <Td isMobile={isMobile} right style={isFastestMean ? { color: THEME.gold, fontWeight: 'bold' } : {}}>{fmt2(stats.mean)}s</Td>
                <Td isMobile={isMobile} right>{fmt2(stats.std)}s</Td>
                <Td isMobile={isMobile} right>{fmt2(stats.median)}s</Td>
                <Td isMobile={isMobile} right>{fmt2(stats.iqr)}s</Td>
                <Td isMobile={isMobile} right style={{ color: THEME.muted }}>{stats.n}</Td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </TableWrapper>
  )
}

export default function SeasonPage({ year, isMobile }) {
  const [activeSubTab, setActiveSubTab] = useState('Drivers')
  const { data, loading } = useSeasonSummary(year)

  if (loading) return <p style={{ color: THEME.muted, padding: '1rem 0' }}>Loading season data…</p>
  if (!data) return <p style={{ color: THEME.muted, padding: '1rem 0' }}>No season data available.</p>

  const {
    driverRows, driverSeries,
    constructorRows, constructorSeries,
    constructorColour, roundLabels,
    pitStopsByTeam,
  } = data

  const totalSeasonLaps = Math.max(...(driverRows ?? []).map(r => r.laps_completed ?? 0))

  return (
    <div>
      <SubTabBar
        tabs={['Drivers', 'Constructors']}
        active={activeSubTab}
        onChange={setActiveSubTab}
        isMobile={isMobile}
      />

      {activeSubTab === 'Drivers' && (
        <div>
          <SectionHeader>
            Driver Standings
            <InfoTooltip placement="bottom" width={280} content={
              <div>
                <div style={{ fontWeight: 'bold', marginBottom: 6 }}>Pos Gained / Pos Lost</div>
                <div style={{ marginBottom: 8 }}>Counts every position change per race, including those caused by pit stops. OpenF1 notes this data may be incomplete, so treat these as approximate figures.</div>
                <div style={{ fontWeight: 'bold', marginBottom: 6 }}>W% vs Teammate</div>
                Only includes races where both teammates finished and were classified. DNS, DSQ, and unclassified DNFs are excluded from both the numerator and denominator.
              </div>
            } />
          </SectionHeader>
          <DriverStatsTable driverRows={driverRows} isMobile={isMobile} />

          <SectionHeader>Points Progression</SectionHeader>
          <DriverPointsTrendChart
            driverSeries={driverSeries}
            roundLabels={roundLabels}
            isMobile={isMobile}
          />
        </div>
      )}

      {activeSubTab === 'Constructors' && (
        <div>
          <SectionHeader>Constructor Standings</SectionHeader>
          <ConstructorStatsTable
            constructorRows={constructorRows}
            constructorColour={constructorColour}
            totalSeasonLaps={totalSeasonLaps}
            isMobile={isMobile}
          />

          <SectionHeader>Points Progression</SectionHeader>
          <ConstructorPointsTrendChart
            constructorSeries={constructorSeries}
            roundLabels={roundLabels}
            isMobile={isMobile}
          />

          <SectionHeader>Pit Stop Times</SectionHeader>
          <PitStopViolinChart
            pitStopsByTeam={pitStopsByTeam}
            constructorColour={constructorColour}
            roundLabels={roundLabels}
            isMobile={isMobile}
          />

          <SectionHeader>Pit Stop Statistics</SectionHeader>
          <PitStopStatsTable
            pitStopsByTeam={pitStopsByTeam}
            constructorColour={constructorColour}
            isMobile={isMobile}
          />
        </div>
      )}
    </div>
  )
}
