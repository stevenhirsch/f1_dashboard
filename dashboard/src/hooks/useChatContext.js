import { useState, useEffect } from 'react'
import { supabase } from '../supabaseClient'

function fmtTime(s) {
  if (s == null) return '—'
  const n = Number(s)
  if (isNaN(n)) return '—'
  const mins = Math.floor(n / 60)
  const secs = (n % 60).toFixed(3).padStart(6, '0')
  return mins > 0 ? `${mins}:${secs}` : `${n.toFixed(3)}`
}

function fmt(n, d = 2) {
  if (n == null || isNaN(Number(n))) return '—'
  return Number(n).toFixed(d)
}

export function useChatContext(year) {
  const [context, setContext] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  useEffect(() => {
    if (!year) return
    let cancelled = false
    setLoading(true)
    setContext(null)
    setError(null)

    buildContext(year)
      .then(ctx => { if (!cancelled) { setContext(ctx); setLoading(false) } })
      .catch(err => { if (!cancelled) { setError(err.message); setLoading(false) } })

    return () => { cancelled = true }
  }, [year])

  return { context, loading, error }
}

async function buildContext(year) {
  // 1. Races
  const { data: racesData, error: racesErr } = await supabase
    .from('races')
    .select('meeting_key, meeting_name, circuit_short_name, location, date_start')
    .eq('year', year)
    .order('date_start')
  if (racesErr) throw new Error(racesErr.message)

  const races = racesData ?? []
  const meetingKeys = races.map(r => r.meeting_key)
  const meetingByKey = {}
  races.forEach((r, i) => { meetingByKey[r.meeting_key] = { ...r, round: i + 1 } })

  if (!meetingKeys.length) return buildPrompt(year, {})

  // 2. Sessions
  const { data: sessionsData } = await supabase
    .from('sessions')
    .select('session_key, meeting_key, session_name, session_type')
    .in('meeting_key', meetingKeys)
  const sessions = sessionsData ?? []
  const sessionByKey = {}
  sessions.forEach(s => { sessionByKey[s.session_key] = s })
  const allSk = sessions.map(s => s.session_key)
  const raceSk = sessions
    .filter(s => ['race', 'sprint'].includes(s.session_type?.toLowerCase()))
    .map(s => s.session_key)
  const qualSk = sessions
    .filter(s => s.session_type?.toLowerCase().includes('qualifying') || s.session_type?.toLowerCase().includes('shootout'))
    .map(s => s.session_key)

  if (!allSk.length) return buildPrompt(year, { races, meetingByKey })

  // 3. Parallel fetch
  const [
    { data: driversRaw },
    { data: driverStats },
    { data: constructorStats },
    { data: raceResults },
    { data: qualResults },
    { data: sectorBests },
  ] = await Promise.all([
    supabase
      .from('drivers')
      .select('session_key, driver_number, full_name, name_acronym, team_name')
      .in('session_key', allSk),
    supabase.from('season_driver_stats').select('*').eq('year', year),
    supabase.from('season_constructor_stats').select('*').eq('year', year),
    raceSk.length
      ? supabase
          .from('race_results')
          .select('session_key, driver_number, position, points, gap_to_leader, dnf, dns, dsq, fastest_lap_flag, mean_peak_decel_g_abs_clean, mean_peak_accel_g_clean, number_of_laps')
          .in('session_key', raceSk)
          .order('position')
      : Promise.resolve({ data: [] }),
    qualSk.length
      ? supabase
          .from('qualifying_results')
          .select('session_key, driver_number, position, best_lap_time, q1_time, q2_time, q3_time')
          .in('session_key', qualSk)
          .order('position')
      : Promise.resolve({ data: [] }),
    supabase.from('session_sector_bests').select('*').in('session_key', allSk),
  ])

  // Driver meta: latest session entry per driver_number
  const driverMeta = {}
  for (const d of (driversRaw ?? [])) {
    if (!driverMeta[d.driver_number] || d.session_key > driverMeta[d.driver_number]._sk) {
      driverMeta[d.driver_number] = {
        acronym: d.name_acronym,
        full_name: d.full_name,
        team: d.team_name,
        _sk: d.session_key,
      }
    }
  }

  return buildPrompt(year, {
    races, meetingByKey, sessionByKey, driverMeta,
    driverStats:      driverStats      ?? [],
    constructorStats: constructorStats ?? [],
    raceResults:      raceResults      ?? [],
    qualResults:      qualResults      ?? [],
    sectorBests:      sectorBests      ?? [],
  })
}

function buildPrompt(year, d) {
  const { races, meetingByKey, sessionByKey, driverMeta,
          driverStats, constructorStats, raceResults, qualResults, sectorBests } = d
  const L = []

  L.push(`You are an F1 analytics assistant with access to data from the ${year} Formula 1 season. Answer questions accurately and concisely using only the data provided below. If a question cannot be answered from this data, say so clearly — do not invent or guess values.`)
  L.push('')
  L.push('## Metric Definitions')
  L.push('- mean_peak_decel_g: average peak braking G-force per lap (filtered car data, windowed to braking zones; higher = harder braking)')
  L.push('- mean_peak_accel_g: average peak acceleration G-force per lap (windowed to throttle application zones)')
  L.push('- laps_led: race laps spent in P1')
  L.push('- qualifying_supertimes: average qualifying gap to teammate across all rounds (seconds; negative = faster than teammate)')
  L.push('- coasting_ratio: proportion of lap time coasting (throttle < 1%, brake = 0); in 2026 also a battery regeneration proxy')
  L.push('')

  // Calendar
  if (races?.length) {
    L.push(`## ${year} F1 Calendar`)
    for (const r of races) {
      const m = meetingByKey[r.meeting_key]
      L.push(`R${m.round}: ${r.meeting_name} (${r.circuit_short_name || r.location})`)
    }
    L.push('')
  }

  // Driver standings
  if (driverStats?.length) {
    const latest = {}
    for (const r of driverStats) {
      if (!latest[r.driver_number] || r.round_number > latest[r.driver_number].round_number)
        latest[r.driver_number] = r
    }
    const rows = Object.values(latest).sort((a, b) => (b.points_scored ?? 0) - (a.points_scored ?? 0))
    const maxRound = Math.max(...rows.map(r => r.round_number))
    L.push(`## Driver Standings (after R${maxRound})`)
    rows.forEach((r, i) => {
      const m = driverMeta?.[r.driver_number] ?? {}
      const acr = m.acronym ?? r.driver_number
      const qs = r.qualifying_supertimes != null ? `${fmt(r.qualifying_supertimes, 3)}s` : '—'
      L.push(
        `${i + 1}. ${acr} (${m.full_name ?? acr}, ${m.team ?? '?'}) — ` +
        `${r.points_scored ?? 0}pts | Race: ${r.race_points ?? 0} | Sprint: ${r.sprint_points ?? 0} | ` +
        `Wins: ${r.wins ?? 0} | Podiums: ${r.podiums ?? 0} | Poles: ${r.poles ?? 0} | ` +
        `DNF: ${r.dnf_count ?? 0} | DNS: ${r.dns_count ?? 0} | ` +
        `Laps Led: ${r.laps_led ?? 0} | Dist: ${r.distance_km ? Math.round(r.distance_km) + 'km' : '—'} | ` +
        `Qual gap vs TM: ${qs}`
      )
    })
    L.push('')
  }

  // Constructor standings
  if (constructorStats?.length) {
    const latest = {}
    for (const r of constructorStats) {
      if (!latest[r.team_name] || r.round_number > latest[r.team_name].round_number)
        latest[r.team_name] = r
    }
    const rows = Object.values(latest).sort((a, b) => (b.points_scored ?? 0) - (a.points_scored ?? 0))
    const maxRound = Math.max(...rows.map(r => r.round_number))
    L.push(`## Constructor Standings (after R${maxRound})`)
    rows.forEach((r, i) => {
      L.push(
        `${i + 1}. ${r.team_name} — ${r.points_scored ?? 0}pts | ` +
        `Race: ${r.race_points ?? 0} | Sprint: ${r.sprint_points ?? 0} | ` +
        `Wins: ${r.wins ?? 0} | Podiums: ${r.podiums ?? 0} | ` +
        `DNF: ${r.dnf_count ?? 0} | DNS: ${r.dns_count ?? 0} | DSQ: ${r.dsq_count ?? 0} | ` +
        `Laps Led: ${r.laps_led ?? 0} | Total Laps: ${r.laps_completed ?? 0}`
      )
    })
    L.push('')
  }

  // Race & sprint results
  if (raceResults?.length && sessionByKey && meetingByKey) {
    L.push('## Race & Sprint Results')
    const bySession = {}
    for (const rr of raceResults) {
      if (!bySession[rr.session_key]) bySession[rr.session_key] = []
      bySession[rr.session_key].push(rr)
    }
    const skSorted = Object.keys(bySession).sort((a, b) => {
      const rA = meetingByKey[sessionByKey[a]?.meeting_key]?.round ?? 0
      const rB = meetingByKey[sessionByKey[b]?.meeting_key]?.round ?? 0
      return rA !== rB ? rA - rB : (sessionByKey[a]?.session_name < sessionByKey[b]?.session_name ? -1 : 1)
    })
    for (const sk of skSorted) {
      const sess = sessionByKey[sk]
      const mtg  = meetingByKey[sess?.meeting_key]
      if (!sess || !mtg) continue
      L.push(`### R${mtg.round} — ${mtg.meeting_name} ${sess.session_name}`)
      for (const rr of bySession[sk]) {
        const m   = driverMeta?.[rr.driver_number] ?? {}
        const acr = m.acronym ?? rr.driver_number
        if (rr.dsq) {
          L.push(`  DSQ: ${acr} (${m.team ?? '?'})`)
        } else if (rr.dns) {
          L.push(`  DNS: ${acr} (${m.team ?? '?'})`)
        } else {
          const gap    = rr.position === 1 ? 'WINNER' : (rr.gap_to_leader ? `+${rr.gap_to_leader}` : '—')
          const fl     = rr.fastest_lap_flag ? ' [FL]' : ''
          const dnfTag = rr.dnf ? ` [DNF @ lap ${rr.number_of_laps ?? '?'}]` : ''
          const decel  = rr.mean_peak_decel_g_abs_clean != null ? ` | Decel: ${fmt(rr.mean_peak_decel_g_abs_clean)}g` : ''
          const accel  = rr.mean_peak_accel_g_clean != null ? ` | Accel: ${fmt(rr.mean_peak_accel_g_clean)}g` : ''
          L.push(`  ${rr.position ?? '—'}. ${acr} (${m.team ?? '?'}) — ${rr.points ?? 0}pts | ${gap}${fl}${dnfTag}${decel}${accel}`)
        }
      }
      L.push('')
    }
  }

  // Qualifying results
  if (qualResults?.length && sessionByKey && meetingByKey) {
    L.push('## Qualifying Results')
    const bySession = {}
    for (const qr of qualResults) {
      if (!bySession[qr.session_key]) bySession[qr.session_key] = []
      bySession[qr.session_key].push(qr)
    }
    const skSorted = Object.keys(bySession).sort((a, b) => {
      const rA = meetingByKey[sessionByKey[a]?.meeting_key]?.round ?? 0
      const rB = meetingByKey[sessionByKey[b]?.meeting_key]?.round ?? 0
      return rA - rB
    })
    for (const sk of skSorted) {
      const sess = sessionByKey[sk]
      const mtg  = meetingByKey[sess?.meeting_key]
      if (!sess || !mtg) continue
      L.push(`### R${mtg.round} — ${mtg.meeting_name} ${sess.session_name}`)
      for (const qr of bySession[sk]) {
        const m   = driverMeta?.[qr.driver_number] ?? {}
        const acr = m.acronym ?? qr.driver_number
        const q1  = qr.q1_time ? fmtTime(qr.q1_time) : '—'
        const q2  = qr.q2_time ? fmtTime(qr.q2_time) : '—'
        const q3  = qr.q3_time ? fmtTime(qr.q3_time) : '—'
        L.push(`  ${qr.position}. ${acr} (${m.team ?? '?'}) — Q1: ${q1} | Q2: ${q2} | Q3: ${q3}`)
      }
      L.push('')
    }
  }

  // Sector bests
  if (sectorBests?.length && sessionByKey && meetingByKey) {
    L.push('## Session Sector Bests & Theoretical Best Laps')
    const sorted = [...sectorBests].sort((a, b) => {
      const rA = meetingByKey[sessionByKey[a.session_key]?.meeting_key]?.round ?? 0
      const rB = meetingByKey[sessionByKey[b.session_key]?.meeting_key]?.round ?? 0
      return rA - rB
    })
    for (const sb of sorted) {
      const sess = sessionByKey[sb.session_key]
      const mtg  = meetingByKey[sess?.meeting_key]
      if (!sess || !mtg) continue
      const s1d = driverMeta?.[sb.best_s1_driver]?.acronym ?? sb.best_s1_driver ?? '?'
      const s2d = driverMeta?.[sb.best_s2_driver]?.acronym ?? sb.best_s2_driver ?? '?'
      const s3d = driverMeta?.[sb.best_s3_driver]?.acronym ?? sb.best_s3_driver ?? '?'
      L.push(
        `R${mtg.round} — ${mtg.meeting_name} ${sess.session_name}: ` +
        `S1 ${fmtTime(sb.best_s1)} (${s1d}) | S2 ${fmtTime(sb.best_s2)} (${s2d}) | S3 ${fmtTime(sb.best_s3)} (${s3d}) | ` +
        `Theoretical best: ${fmtTime(sb.theoretical_best_lap)}`
      )
    }
    L.push('')
  }

  return L.join('\n')
}
