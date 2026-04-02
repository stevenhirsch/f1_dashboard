import { useState, useEffect, useMemo } from 'react'
import Plot from 'react-plotly.js'
import { supabase } from '../supabaseClient'
import { useDriverRaceData } from '../hooks/useDriverRaceData'
import { useDriverQualifyingData } from '../hooks/useDriverQualifyingData'
import { useLapTelemetry } from '../hooks/useLapTelemetry'
import { compoundColour, COMPOUND_COLOURS } from '../utils/compounds'
import { formatQualTime, assignPhases, normalizePhase } from '../utils/qualifying'
import LapTimesChart from '../plots/LapTimesChart'
import TrackMapPlot from '../plots/TrackMapPlot'
import TelemetryChart from '../plots/TelemetryChart'
import CoastingChart from '../plots/CoastingChart'
import { useRaceControl } from '../hooks/useRaceControl'
import InfoTooltip from '../components/InfoTooltip'
import LazySection from '../components/LazySection'

const LAP_COLOURS = [
  '#e10600', '#3b82f6', '#22c55e', '#f59e0b',
  '#8b5cf6', '#06b6d4', '#f97316', '#ec4899',
]

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
        {(compound ?? 'UNKNOWN').toUpperCase() === 'UNKNOWN' ? '?' : compound.slice(0, 1)}
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

function RaceSummaryCard({ result, laps, pitStops, driver, overtakes, driverNumber }) {
  if (!result && laps.length === 0) return null

  const validLaps = laps.filter(l => l.lap_duration && l.lap_duration > 0 && !l.is_pit_out_lap)
  const bestLap = validLaps.reduce((best, l) => (!best || l.lap_duration < best.lap_duration) ? l : best, null)

  const pos = result?.dsq ? 'DSQ' : result?.dns ? 'DNS' : result?.dnf ? 'DNF' : result?.position != null ? `P${result.position}` : '—'
  const posColour = (result?.dnf || result?.dns || result?.dsq) ? '#ef4444' : THEME.text

  const pitLaps = pitStops.map(p => `L${p.lap_number}`).join(', ')

  const overtakesMade = overtakes.filter(o => o.driver_number_overtaking === driverNumber).length
  const overtakesSuffered = overtakes.filter(o => o.driver_number_overtaken === driverNumber).length

  const maxSpeed = laps.reduce((max, l) => (l.st_speed != null && l.st_speed > max ? l.st_speed : max), 0) || null
  const fastestLapSpeed = bestLap?.st_speed ?? null

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
      <StatCard
        label={
          <span style={{ display: 'inline-flex', alignItems: 'center' }}>
            Pos Gained
            <InfoTooltip placement="bottom" width={260} content={
              <div>
                <div style={{ fontWeight: 'bold', marginBottom: 6 }}>Pos Gained / Pos Lost</div>
                Counts every position change per race, including those caused by pit stops.
                OpenF1 notes this data may be incomplete, so treat these as approximate figures.
              </div>
            } />
          </span>
        }
        value={overtakesMade}
        sub={overtakesSuffered > 0 ? `Lost ${overtakesSuffered}` : null}
      />
      {fastestLapSpeed != null && (
        <StatCard label="FL Speed" value={`${fastestLapSpeed} km/h`} sub="fastest lap" />
      )}
      {maxSpeed != null && (
        <StatCard label="Top Speed" value={`${maxSpeed} km/h`} sub="incl. tow" />
      )}
    </div>
  )
}

function RaceLapTable({ laps, stints, pitStops, selectedLapNums, onLapToggle, lapColourMap }) {
  if (!laps || laps.length === 0) return <p style={{ color: THEME.muted }}>No lap data.</p>
  const pitLapSet = new Set(pitStops.map(p => p.lap_number))

  return (
    <div style={{ overflowX: 'auto' }}>
      <p style={{ fontSize: '0.7rem', color: THEME.muted, margin: '0 0 0.5rem 0' }}>
        Click a row to load lap telemetry. Select multiple laps to overlay.
      </p>
      <table style={{ borderCollapse: 'collapse', width: '100%', fontSize: '0.82rem', color: THEME.text }}>
        <thead>
          <tr>
            {['', 'Lap', 'Time', 'S1', 'S2', 'S3', 'Tyre', 'Pit', 'Trap'].map(h => (
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
            const isSelected = selectedLapNums.includes(lap.lap_number)
            const lapColour = lapColourMap[lap.lap_number]
            const canSelect = !!lap.date_start && !noTime
            const dimStyle = (isPitOut || noTime) && !isSelected ? { opacity: 0.45 } : {}

            return (
              <tr
                key={lap.lap_number}
                onClick={() => canSelect && onLapToggle(lap.lap_number)}
                style={{
                  borderBottom: `1px solid ${THEME.border}`,
                  background: isSelected
                    ? `${lapColour}18`
                    : i % 2 === 0 ? 'transparent' : THEME.tableAlt,
                  cursor: canSelect ? 'pointer' : 'default',
                  ...dimStyle,
                }}
              >
                <td style={{ ...tdStyle, width: '18px', padding: '0.35rem 0.3rem' }}>
                  <span style={{
                    display: 'inline-block',
                    width: '10px', height: '10px',
                    borderRadius: '50%',
                    background: lapColour ?? 'transparent',
                    border: `1px solid ${lapColour ?? THEME.border}`,
                    opacity: isSelected ? 1 : 0.25,
                  }} />
                </td>
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

function GapChart({ intervals, teamColour, isMobile }) {
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
        margin: isMobile ? { l: 45, r: 10, t: 15, b: 35 } : { l: 60, r: 20, t: 20, b: 45 },
        height: isMobile ? 220 : 280,
        paper_bgcolor: '#18181b',
        plot_bgcolor: '#18181b',
        font: { color: '#fafafa', family: 'monospace' },
      }}
      config={{ responsive: true, displayModeBar: false }}
      style={{ width: '100%' }}
    />
  )
}

function PositionChart({ positions, teamColour, isMobile }) {
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
        margin: isMobile ? { l: 35, r: 10, t: 15, b: 35 } : { l: 50, r: 20, t: 20, b: 45 },
        height: isMobile ? 220 : 280,
        paper_bgcolor: '#18181b',
        plot_bgcolor: '#18181b',
        font: { color: '#fafafa', family: 'monospace' },
      }}
      config={{ responsive: true, displayModeBar: false }}
      style={{ width: '100%' }}
    />
  )
}

function LapDetailPanel({ sessionKey, driverNumber, selectedLapNums, laps, lapColourMap, onClear, isMobile }) {
  const selectedLaps = useMemo(
    () => selectedLapNums
      .map(n => laps.find(l => l.lap_number === n))
      .filter(Boolean)
      .map(l => ({ lap_number: l.lap_number, date_start: l.date_start, lap_duration: l.lap_duration })),
    [selectedLapNums, laps],
  )

  const { data: telemetryData, loading } = useLapTelemetry(sessionKey, driverNumber, selectedLaps)

  // lapColours keyed by lap_number as string (Plotly uses string keys from Object.keys)
  const lapColours = {}
  for (const n of selectedLapNums) lapColours[String(n)] = lapColourMap[n]

  return (
    <div style={{ marginBottom: '2rem' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '0.75rem' }}>
        <h3 style={{ ...sectionHeading, margin: 0, borderBottom: 'none', display: 'flex', alignItems: 'center' }}>
          Lap Detail — {selectedLapNums.map(n => `Lap ${n}`).join(', ')}
          <InfoTooltip content={
            <div>
              <div style={{ fontWeight: 'bold', marginBottom: 6 }}>Driving style panels</div>
              <div style={{ marginBottom: 6 }}>
                <span style={{ color: '#22c55e', fontWeight: 'bold' }}>Lift &amp; Coast</span> — both throttle &lt;1% <em>and</em> brake = 0.
                The driver is neither accelerating nor braking, allowing the car to coast.
                In modern F1 (post-2022), this zone is critical for regenerating the battery (MGU-K harvesting).
                More coasting generally means more ERS deployment available later in the lap.
              </div>
              <div>
                <span style={{ color: '#f59e0b', fontWeight: 'bold' }}>Thr/Brk overlap</span> — brake applied while throttle ≥10%.
                Known as <em>trail-braking</em>, this rotates the car into a corner by keeping some rear grip
                while braking. It is a hallmark of aggressive, high-confidence driving styles.
              </div>
            </div>
          } />
        </h3>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
          {loading && <span style={{ fontSize: '0.72rem', color: THEME.muted }}>Loading telemetry…</span>}
          <button
            onClick={onClear}
            style={{
              background: 'none', border: `1px solid ${THEME.border}`, color: THEME.muted,
              borderRadius: '4px', padding: '0.2rem 0.6rem', fontSize: '0.72rem',
              fontFamily: 'monospace', cursor: 'pointer',
            }}
          >
            Clear selection
          </button>
        </div>
      </div>

      <div style={{ background: THEME.surface, borderRadius: '8px', border: `1px solid ${THEME.border}`, padding: '0.75rem' }}>
        {/* Track map only shown for a single selected lap — multi-lap XY overlay adds no value */}
        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap' }}>
          {selectedLapNums.length === 1 && (
            <div style={{ flex: isMobile ? '1 1 100%' : '0 0 360px', minWidth: isMobile ? '0' : '280px' }}>
              <TrackMapPlot data={telemetryData} lapColours={lapColours} height={isMobile ? 240 : 360} />
            </div>
          )}
          <div style={{ flex: '1 1 320px', minWidth: '280px' }}>
            <TelemetryChart data={telemetryData} lapColours={lapColours} height={isMobile ? 500 : (selectedLapNums.length === 1 ? 740 : 820)} />
          </div>
        </div>
      </div>
    </div>
  )
}

function RaceSubTab({ raceSessionKey, driverNumber, isMobile }) {
  const { data, loading } = useDriverRaceData(raceSessionKey, driverNumber)
  const { safetyCarPeriods } = useRaceControl(raceSessionKey)
  const [selectedLapNums, setSelectedLapNums] = useState([])

  const toggleLap = lapNum => setSelectedLapNums(prev =>
    prev.includes(lapNum) ? prev.filter(n => n !== lapNum) : [...prev, lapNum]
  )

  const lapColourMap = useMemo(() => {
    const map = {}
    selectedLapNums.forEach((n, i) => { map[n] = LAP_COLOURS[i % LAP_COLOURS.length] })
    // Pre-assign colours to all laps for the swatch display
    return map
  }, [selectedLapNums])

  // Pre-assign a stable colour for each lap number (for dot preview before selection)
  const allLapColourMap = useMemo(() => {
    if (!data?.laps) return {}
    const map = {}
    data.laps.forEach((l, i) => { map[l.lap_number] = LAP_COLOURS[i % LAP_COLOURS.length] })
    return map
  }, [data?.laps])

  if (loading) return <p style={{ color: THEME.muted }}>Loading race data…</p>
  if (!data) return <p style={{ color: THEME.muted }}>No data.</p>

  const { laps, stints, pitStops, result, intervals, overtakes, positions, driver } = data
  const teamColour = driver?.team_colour ? `#${driver.team_colour.replace('#', '')}` : null

  const colourMap = selectedLapNums.length > 0 ? lapColourMap : allLapColourMap

  return (
    <div>
      <RaceSummaryCard result={result} laps={laps} pitStops={pitStops} driver={driver} overtakes={overtakes} driverNumber={driverNumber} />

      <div style={{ marginBottom: '2rem' }}>
        <h3 style={sectionHeading}>Lap Times</h3>
        <div style={{ background: THEME.surface, borderRadius: '8px', border: `1px solid ${THEME.border}`, padding: '0.5rem' }}>
          <LapTimesChart
            laps={laps}
            stints={stints}
            pitStops={pitStops}
            safetyCarPeriods={safetyCarPeriods}
            mode="bars"
            isMobile={isMobile}
          />
        </div>
      </div>

      {positions.length > 0 && (
        <LazySection minHeight={360}>
          <div style={{ marginBottom: '2rem' }}>
            <h3 style={sectionHeading}>Race Position</h3>
            <div style={{ background: THEME.surface, borderRadius: '8px', border: `1px solid ${THEME.border}`, padding: '0.5rem' }}>
              <PositionChart positions={positions} teamColour={teamColour} isMobile={isMobile} />
            </div>
          </div>
        </LazySection>
      )}

      {intervals.length > 0 && result?.position !== 1 && (
        <LazySection minHeight={360}>
          <div style={{ marginBottom: '2rem' }}>
            <h3 style={sectionHeading}>Gap to Leader</h3>
            <div style={{ background: THEME.surface, borderRadius: '8px', border: `1px solid ${THEME.border}`, padding: '0.5rem' }}>
              <GapChart intervals={intervals} teamColour={teamColour} isMobile={isMobile} />
            </div>
          </div>
        </LazySection>
      )}

      <div style={{ marginBottom: selectedLapNums.length > 0 ? '1rem' : '2rem' }}>
        <h3 style={sectionHeading}>Lap by Lap</h3>
        <div style={{ background: THEME.surface, borderRadius: '8px', border: `1px solid ${THEME.border}`, overflow: 'hidden' }}>
          <RaceLapTable
            laps={laps}
            stints={stints}
            pitStops={pitStops}
            selectedLapNums={selectedLapNums}
            onLapToggle={toggleLap}
            lapColourMap={colourMap}
          />
        </div>
      </div>

      {selectedLapNums.length > 0 && (
        <LapDetailPanel
          sessionKey={raceSessionKey}
          driverNumber={driverNumber}
          selectedLapNums={selectedLapNums}
          laps={laps}
          lapColourMap={lapColourMap}
          onClear={() => setSelectedLapNums([])}
          isMobile={isMobile}
        />
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Qualifying sub-tab
// ---------------------------------------------------------------------------

function QualSummaryCard({ qualResult, gridPosition, laps }) {
  if (!qualResult) return <p style={{ color: THEME.muted }}>No qualifying result.</p>

  const timedLaps = (laps ?? [])
    .filter(l => l.lap_duration && l.lap_duration > 0 && l.lap_duration <= 180)
    .sort((a, b) => a.lap_duration - b.lap_duration)
  const fastestQualLap = timedLaps[0] ?? null
  const fastestLapSpeed = fastestQualLap?.st_speed ?? null
  const maxSpeed = (laps ?? []).reduce((max, l) => (l.st_speed != null && l.st_speed > max ? l.st_speed : max), 0) || null

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
      {fastestLapSpeed != null && (
        <StatCard label="FL Speed" value={`${fastestLapSpeed} km/h`} sub="fastest lap" />
      )}
      {maxSpeed != null && (
        <StatCard label="Top Speed" value={`${maxSpeed} km/h`} sub="session max" />
      )}
    </div>
  )
}

function QualLapTable({ laps, stints, phaseEvents, selectedLapNums, onLapToggle, lapColourMap }) {
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
      <p style={{ fontSize: '0.7rem', color: THEME.muted, margin: '0 0 0.5rem 0' }}>
        Click a timed lap row to load telemetry. Select multiple laps to overlay.
      </p>
      <table style={{ borderCollapse: 'collapse', width: '100%', fontSize: '0.82rem', color: THEME.text }}>
        <thead>
          <tr>
            {['', 'Lap', 'Phase', 'Time', 'Delta', 'S1', 'S2', 'S3', 'Tyre'].map(h => (
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
            const isTimed = !noTime && !isOutIn
            const best = lap._phase ? phaseBest[lap._phase] : null
            const delta = best && lap.lap_duration && !isOutIn
              ? lap.lap_duration - best
              : null
            const isBest = delta === 0
            const isSelected = selectedLapNums.includes(lap.lap_number)
            const lapColour = lapColourMap[lap.lap_number]
            const canSelect = isTimed && !!lap.date_start

            const dimStyle = (isOutIn || noTime) && !isSelected ? { opacity: 0.4 } : {}

            return (
              <tr
                key={`${lap.lap_number}-${i}`}
                onClick={() => canSelect && onLapToggle(lap.lap_number)}
                style={{
                  borderBottom: `1px solid ${THEME.border}`,
                  background: isSelected
                    ? `${lapColour}18`
                    : i % 2 === 0 ? 'transparent' : THEME.tableAlt,
                  cursor: canSelect ? 'pointer' : 'default',
                  ...dimStyle,
                }}
              >
                <td style={{ ...tdStyle, width: '18px', padding: '0.35rem 0.3rem' }}>
                  <span style={{
                    display: 'inline-block',
                    width: '10px', height: '10px',
                    borderRadius: '50%',
                    background: lapColour ?? 'transparent',
                    border: `1px solid ${lapColour ?? THEME.border}`,
                    opacity: isSelected ? 1 : canSelect ? 0.25 : 0,
                  }} />
                </td>
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

function QualSubTab({ qualifyingSessionKey, driverNumber, isMobile }) {
  const { data, loading } = useDriverQualifyingData(qualifyingSessionKey, driverNumber)
  const [selectedLapNums, setSelectedLapNums] = useState([])

  const toggleLap = lapNum => setSelectedLapNums(prev =>
    prev.includes(lapNum) ? prev.filter(n => n !== lapNum) : [...prev, lapNum]
  )

  const timedLaps = useMemo(
    () => (data?.laps ?? []).filter(l => l.lap_duration && l.lap_duration <= 180),
    [data?.laps],
  )

  const lapColourMap = useMemo(() => {
    const map = {}
    selectedLapNums.forEach((n, i) => { map[n] = LAP_COLOURS[i % LAP_COLOURS.length] })
    return map
  }, [selectedLapNums])

  const allLapColourMap = useMemo(() => {
    const map = {}
    timedLaps.forEach((l, i) => { map[l.lap_number] = LAP_COLOURS[i % LAP_COLOURS.length] })
    return map
  }, [timedLaps])

  if (loading) return <p style={{ color: THEME.muted }}>Loading qualifying data…</p>
  if (!data) return <p style={{ color: THEME.muted }}>No data.</p>

  const { laps, stints, qualResult, phaseEvents, gridPosition } = data
  const colourMap = selectedLapNums.length > 0 ? lapColourMap : allLapColourMap

  return (
    <div>
      <QualSummaryCard qualResult={qualResult} gridPosition={gridPosition} laps={laps} />

      <div style={{ marginBottom: '2rem' }}>
        <h3 style={sectionHeading}>Lap Times</h3>
        <div style={{ background: THEME.surface, borderRadius: '8px', border: `1px solid ${THEME.border}`, padding: '0.5rem' }}>
          <LapTimesChart laps={timedLaps} stints={stints} mode="bars" phaseEvents={phaseEvents} isMobile={isMobile} />
        </div>
      </div>

      <div style={{ marginBottom: selectedLapNums.length > 0 ? '1rem' : '2rem' }}>
        <h3 style={sectionHeading}>Lap by Lap</h3>
        <div style={{ background: THEME.surface, borderRadius: '8px', border: `1px solid ${THEME.border}`, overflow: 'hidden' }}>
          <QualLapTable
            laps={laps}
            stints={stints}
            phaseEvents={phaseEvents}
            selectedLapNums={selectedLapNums}
            onLapToggle={toggleLap}
            lapColourMap={colourMap}
          />
        </div>
      </div>

      {selectedLapNums.length > 0 && (
        <LapDetailPanel
          sessionKey={qualifyingSessionKey}
          driverNumber={driverNumber}
          selectedLapNums={selectedLapNums}
          laps={laps}
          lapColourMap={lapColourMap}
          onClear={() => setSelectedLapNums([])}
          isMobile={isMobile}
        />
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main DriverPage component
// ---------------------------------------------------------------------------

export default function DriverPage({ raceSessionKey, qualifyingSessionKey, gmtOffset, isMobile }) {
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
    fontSize: isMobile ? '16px' : '0.875rem',
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
            <RaceSubTab key={selectedDriverNumber} raceSessionKey={raceSessionKey} driverNumber={selectedDriverNumber} isMobile={isMobile} />
          )}
          {activeSubTab === 'Race' && !raceSessionKey && (
            <p style={{ color: THEME.muted }}>No race session for this weekend.</p>
          )}
          {activeSubTab === 'Qualifying' && qualifyingSessionKey && (
            <QualSubTab key={selectedDriverNumber} qualifyingSessionKey={qualifyingSessionKey} driverNumber={selectedDriverNumber} isMobile={isMobile} />
          )}
          {activeSubTab === 'Qualifying' && !qualifyingSessionKey && (
            <p style={{ color: THEME.muted }}>No qualifying session for this weekend.</p>
          )}
        </>
      )}
    </div>
  )
}
