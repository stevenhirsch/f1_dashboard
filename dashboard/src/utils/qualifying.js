/**
 * Normalize a qualifying_phase value to "Q1" | "Q2" | "Q3" | null.
 * OpenF1 returns integers (1, 2, 3) in some sessions and strings ("Q1"/"Q2"/"Q3") in others.
 */
export function normalizePhase(v) {
  if (v == null) return null
  const s = String(v).trim().toUpperCase()
  if (s === '1' || s === 'Q1') return 'Q1'
  if (s === '2' || s === 'Q2') return 'Q2'
  if (s === '3' || s === 'Q3') return 'Q3'
  return null
}

/**
 * Assign a qualifying phase (_phase: "Q1" | "Q2" | "Q3" | null) to each lap.
 * Uses ISO 8601 lexicographic comparison, which works correctly for UTC timestamps.
 * Handles both integer (1/2/3) and string ("Q1"/"Q2"/"Q3") qualifying_phase values.
 */
export function assignPhases(laps, phaseEvents) {
  const events = [...phaseEvents]
    .filter(e => normalizePhase(e.qualifying_phase) && e.date)
    .sort((a, b) => (a.date < b.date ? -1 : a.date > b.date ? 1 : 0))

  return laps.map(lap => {
    if (!lap.date_start) return { ...lap, _phase: null }
    let phase = null
    for (const event of events) {
      if (event.date <= lap.date_start) {
        phase = normalizePhase(event.qualifying_phase)
      } else {
        break
      }
    }
    return { ...lap, _phase: phase }
  })
}

/**
 * Format a lap time in seconds as m:ss.mmm.
 * Returns '—' for null/undefined.
 */
export function formatQualTime(seconds) {
  if (seconds == null) return '—'
  const m = Math.floor(seconds / 60)
  const s = (seconds % 60).toFixed(3).padStart(6, '0')
  return `${m}:${s}`
}

/**
 * Format a time delta.
 * Returns 'POLE' for 0, '+N.NNN' for positive values, '—' for null.
 */
export function formatDelta(delta) {
  if (delta == null) return '—'
  if (delta === 0) return 'POLE'
  return `+${delta.toFixed(3)}`
}

/**
 * For a given qualifying phase, compute per-driver per-stint fastest lap data.
 * Returns drivers sorted by best phase time, each with their stint breakdown:
 * { driver_number, driver, team_colour, stints: [{stint_number, compound, tyre_age_at_start, best_time}], best_time }
 */
export function computePhaseStints(laps, phaseEvents, stintsByDriver, driverMap, phase) {
  const phasedLaps = assignPhases(laps, phaseEvents).filter(l => l._phase === phase)

  const lapsByDriver = {}
  for (const lap of phasedLaps) {
    if (!lapsByDriver[lap.driver_number]) lapsByDriver[lap.driver_number] = []
    lapsByDriver[lap.driver_number].push(lap)
  }

  const driverRows = []
  for (const [dnStr, driverLaps] of Object.entries(lapsByDriver)) {
    const dn = Number(dnStr)
    const driver = driverMap[dn] ?? {}
    const stints = (stintsByDriver[dn] ?? []).slice().sort((a, b) => (a.stint_number ?? 0) - (b.stint_number ?? 0))

    const stintBestMap = {}
    let overallBest = null
    for (const lap of driverLaps) {
      if (!lap.lap_duration || lap.lap_duration <= 0) continue
      // Ignore obvious out/in-laps: more than 150% of a typical Q lap (>3 min is not a flying lap)
      if (lap.lap_duration > 180) continue

      if (overallBest === null || lap.lap_duration < overallBest) overallBest = lap.lap_duration

      const stint = stints.find(s => {
        const start = s.lap_start ?? 1
        const end = s.lap_end ?? Infinity
        return lap.lap_number >= start && lap.lap_number <= end
      })
      if (!stint) continue
      const sn = stint.stint_number ?? 0
      if (!stintBestMap[sn] || lap.lap_duration < stintBestMap[sn].best_time) {
        stintBestMap[sn] = {
          stint_number: sn,
          compound: (stint.compound ?? 'UNKNOWN').toUpperCase(),
          tyre_age_at_start: stint.tyre_age_at_start ?? 0,
          best_time: lap.lap_duration,
        }
      }
    }

    const stintRows = Object.values(stintBestMap).sort((a, b) => a.stint_number - b.stint_number)
    // Use overall best time regardless of stint match — stints may be incomplete in qualifying
    const best_time = overallBest

    driverRows.push({
      driver_number: dn,
      driver: driver.name_acronym ?? String(dn),
      team_colour: driver.team_colour ?? '888888',
      stints: stintRows,
      best_time,
    })
  }

  return driverRows.sort((a, b) => (a.best_time ?? 999999) - (b.best_time ?? 999999))
}

/**
 * For each driver, find their best lap in the given qualifying phase (or across all phases if null),
 * compute per-sector deltas versus theoretical pole (independent min per sector),
 * and return a sorted array of driver sector delta objects.
 */
export function computeSectorDeltas(laps, phaseEvents, driverMap, phase = null) {
  const allPhasedLaps = assignPhases(laps, phaseEvents)
  const phasedLaps = phase ? allPhasedLaps.filter(l => l._phase === phase) : allPhasedLaps

  // For each driver, find the single best lap (lowest duration) across filtered laps
  const driverBest = {}
  for (const lap of phasedLaps) {
    const { driver_number, _phase, lap_duration } = lap
    if (!_phase || !lap_duration || lap_duration <= 0 || lap_duration > 180) continue
    if (!driverBest[driver_number] || lap_duration < driverBest[driver_number].lap_duration) {
      driverBest[driver_number] = lap
    }
  }

  const bestLaps = Object.values(driverBest)
  if (bestLaps.length === 0) return []

  // Theoretical pole: independent minimum per sector
  const s1vals = bestLaps.map(l => l.duration_sector_1).filter(s => s != null && s > 0)
  const s2vals = bestLaps.map(l => l.duration_sector_2).filter(s => s != null && s > 0)
  const s3vals = bestLaps.map(l => l.duration_sector_3).filter(s => s != null && s > 0)
  const poleS1 = s1vals.length ? Math.min(...s1vals) : null
  const poleS2 = s2vals.length ? Math.min(...s2vals) : null
  const poleS3 = s3vals.length ? Math.min(...s3vals) : null

  return bestLaps
    .map(lap => {
      const driver = driverMap[lap.driver_number] ?? {}
      const s1 = lap.duration_sector_1
      const s2 = lap.duration_sector_2
      const s3 = lap.duration_sector_3
      return {
        driver_number: lap.driver_number,
        driver: driver.name_acronym ?? String(lap.driver_number),
        team_colour: driver.team_colour ?? '888888',
        phase: lap._phase,
        lap_duration: lap.lap_duration,
        s1,
        s2,
        s3,
        delta_s1: s1 != null && poleS1 != null ? s1 - poleS1 : null,
        delta_s2: s2 != null && poleS2 != null ? s2 - poleS2 : null,
        delta_s3: s3 != null && poleS3 != null ? s3 - poleS3 : null,
      }
    })
    .sort((a, b) => (a.lap_duration ?? 999) - (b.lap_duration ?? 999))
}
