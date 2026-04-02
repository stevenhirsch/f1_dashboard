import { useState, useEffect } from 'react'
import { supabase } from '../supabaseClient'

export function useSeasonSummary(year) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!year) { setLoading(false); return }
    setLoading(true)
    setData(null)

    // Stage 1: get meetings → session keys
    supabase
      .from('races')
      .select('meeting_key, meeting_name, circuit_short_name, date_start')
      .eq('year', year)
      .order('date_start')
      .then(({ data: racesData }) => {
        const races = racesData ?? []
        const meetingKeys = races.map(r => r.meeting_key)

        // round_number in season stats = 1-indexed chronological order of meetings
        const roundLabels = {}
        races.forEach((r, i) => {
          roundLabels[i + 1] = r.circuit_short_name || r.meeting_name
        })

        if (!meetingKeys.length) {
          setData(null)
          setLoading(false)
          return
        }

        supabase
          .from('sessions')
          .select('session_key, meeting_key, session_type')
          .in('meeting_key', meetingKeys)
          .then(({ data: sessionsData }) => {
            const sessions = sessionsData ?? []
            const raceSessionKeys = sessions
              .filter(s => s.session_type?.toLowerCase() === 'race')
              .map(s => s.session_key)
            const allSessionKeys = sessions.map(s => s.session_key)

            if (!raceSessionKeys.length) {
              setData(null)
              setLoading(false)
              return
            }

            // Stage 2: parallel fetch all season data
            Promise.all([
              supabase.from('season_driver_stats').select('*').eq('year', year),
              supabase.from('season_constructor_stats').select('*').eq('year', year),
              supabase
                .from('drivers')
                .select('session_key, driver_number, full_name, name_acronym, team_name, team_colour')
                .in('session_key', allSessionKeys),
              supabase
                .from('pit_stops')
                .select('session_key, driver_number, stop_duration, lane_duration')
                .in('session_key', raceSessionKeys),
            ]).then(([driverStatsRes, constructorStatsRes, driversRes, pitStopsRes]) => {
              const driverStats     = driverStatsRes.data ?? []
              const constructorStats = constructorStatsRes.data ?? []
              const driversRaw      = driversRes.data ?? []
              const pitStops        = pitStopsRes.data ?? []

              // Driver metadata: use last-seen session entry per driver_number
              const driverMeta = {}
              for (const d of driversRaw) {
                driverMeta[d.driver_number] = {
                  full_name:    d.full_name,
                  name_acronym: d.name_acronym,
                  team_name:    d.team_name,
                  team_colour:  d.team_colour ? `#${d.team_colour}` : '#888888',
                }
              }

              // Constructor colour: take first driver found for each team
              const constructorColour = {}
              for (const d of driversRaw) {
                if (d.team_name && !constructorColour[d.team_name] && d.team_colour) {
                  constructorColour[d.team_name] = `#${d.team_colour}`
                }
              }

              // Driver rows: latest round per driver, sorted by points_scored desc
              const latestDriverStats = {}
              for (const row of driverStats) {
                const prev = latestDriverStats[row.driver_number]
                if (!prev || row.round_number > prev.round_number) {
                  latestDriverStats[row.driver_number] = row
                }
              }
              const driverRows = Object.values(latestDriverStats)
                .sort((a, b) => (b.points_scored ?? 0) - (a.points_scored ?? 0))
                .map((row, i) => ({
                  ...row,
                  position: i + 1,
                  meta: driverMeta[row.driver_number] ?? null,
                }))

              // Driver series for trend chart
              const driverSeriesMap = {}
              for (const row of [...driverStats].sort((a, b) => a.round_number - b.round_number)) {
                const dn = row.driver_number
                if (!driverSeriesMap[dn]) {
                  const meta = driverMeta[dn]
                  driverSeriesMap[dn] = {
                    driver_number: dn,
                    full_name:     meta?.full_name ?? `#${dn}`,
                    name_acronym:  meta?.name_acronym ?? `${dn}`,
                    team_colour:   meta?.team_colour ?? '#888888',
                    rounds: [],
                    points: [],
                  }
                }
                driverSeriesMap[dn].rounds.push(row.round_number)
                driverSeriesMap[dn].points.push(row.points_scored ?? 0)
              }
              const driverSeries = Object.values(driverSeriesMap)

              // Constructor rows: latest round per team, sorted by points_scored desc
              const latestConstructorStats = {}
              for (const row of constructorStats) {
                const prev = latestConstructorStats[row.team_name]
                if (!prev || row.round_number > prev.round_number) {
                  latestConstructorStats[row.team_name] = row
                }
              }
              const constructorRows = Object.values(latestConstructorStats)
                .sort((a, b) => (b.points_scored ?? 0) - (a.points_scored ?? 0))
                .map((row, i) => ({ ...row, position: i + 1 }))

              // Constructor series for trend chart
              const constructorSeriesMap = {}
              for (const row of [...constructorStats].sort((a, b) => a.round_number - b.round_number)) {
                const team = row.team_name
                if (!constructorSeriesMap[team]) {
                  constructorSeriesMap[team] = {
                    team_name:   team,
                    team_colour: constructorColour[team] ?? '#888888',
                    rounds: [],
                    points: [],
                  }
                }
                constructorSeriesMap[team].rounds.push(row.round_number)
                constructorSeriesMap[team].points.push(row.points_scored ?? 0)
              }
              const constructorSeries = Object.values(constructorSeriesMap)

              // session_key → round_number (for pit stop colouring by race)
              const skToMk = {}
              for (const s of sessions) skToMk[s.session_key] = s.meeting_key
              const mkToRound = {}
              races.forEach((r, i) => { mkToRound[r.meeting_key] = i + 1 })

              // Pit stop stationary times grouped by team.
              // Only use stop_duration (no fallback) — lane_duration mixes in transit time.
              const teamBySkDn = {}
              for (const d of driversRaw) {
                teamBySkDn[`${d.session_key}_${d.driver_number}`] = d.team_name
              }
              const pitStopsByTeam = {}
              for (const ps of pitStops) {
                if (ps.stop_duration == null || ps.stop_duration <= 0 || ps.stop_duration > 10) continue
                const team = teamBySkDn[`${ps.session_key}_${ps.driver_number}`]
                if (!team) continue
                const round = mkToRound[skToMk[ps.session_key]] ?? null
                if (!pitStopsByTeam[team]) pitStopsByTeam[team] = []
                pitStopsByTeam[team].push({ duration: ps.stop_duration, round })
              }

              setData({
                driverRows,
                driverSeries,
                constructorRows,
                constructorSeries,
                constructorColour,
                roundLabels,
                pitStopsByTeam,
              })
              setLoading(false)
            })
          })
      })
  }, [year])

  return { data, loading }
}
