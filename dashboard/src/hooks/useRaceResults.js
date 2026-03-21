import { useState, useEffect } from 'react'
import { supabase } from '../supabaseClient'

export function useRaceResults(sessionKey, qualifyingSessionKey) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!sessionKey) { setLoading(false); return }
    setLoading(true)
    Promise.all([
      supabase
        .from('race_results')
        .select('*')
        .eq('session_key', sessionKey)
        .order('position'),
      supabase
        .from('drivers')
        .select('driver_number, name_acronym, team_name, team_colour')
        .eq('session_key', sessionKey),
      qualifyingSessionKey
        ? supabase
            .from('starting_grid')
            .select('driver_number, position')
            .eq('session_key', qualifyingSessionKey)
        : Promise.resolve({ data: [] }),
      supabase
        .from('laps')
        .select('driver_number, lap_duration, st_speed, is_pit_out_lap')
        .eq('session_key', sessionKey),
    ]).then(([resultsRes, driversRes, gridRes, lapsRes]) => {
      const results = resultsRes.data ?? []
      const driverMap = Object.fromEntries(
        (driversRes.data ?? []).map(d => [d.driver_number, d])
      )
      const gridMap = Object.fromEntries(
        (gridRes.data ?? []).map(g => [g.driver_number, g.position])
      )

      // Compute per-driver speed stats from laps
      const lapsByDriver = {}
      for (const l of lapsRes.data ?? []) {
        if (!lapsByDriver[l.driver_number]) lapsByDriver[l.driver_number] = []
        lapsByDriver[l.driver_number].push(l)
      }
      const speedStats = {}
      for (const [dn, dLaps] of Object.entries(lapsByDriver)) {
        const validLaps = dLaps.filter(l => l.lap_duration && l.lap_duration > 0 && !l.is_pit_out_lap)
        const fastestLap = validLaps.sort((a, b) => a.lap_duration - b.lap_duration)[0]
        const maxSpeed = dLaps.reduce((m, l) => (l.st_speed != null && l.st_speed > m ? l.st_speed : m), 0) || null
        speedStats[dn] = {
          fl_speed: fastestLap?.st_speed ?? null,
          max_speed: maxSpeed,
        }
      }

      const merged = results.map(r => ({
        ...r,
        drivers: driverMap[r.driver_number] ?? null,
        grid_position: gridMap[r.driver_number] ?? null,
        fl_speed: speedStats[r.driver_number]?.fl_speed ?? null,
        max_speed: speedStats[r.driver_number]?.max_speed ?? null,
      }))
      setData(merged)
      setLoading(false)
    })
  }, [sessionKey, qualifyingSessionKey])

  return { data, loading }
}
