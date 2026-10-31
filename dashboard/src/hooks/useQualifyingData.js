import { useState, useEffect } from 'react'
import { supabase } from '../supabaseClient'
import { sortQualifyingResults } from '../utils/qualifying'

export function useQualifyingData(sessionKey) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!sessionKey) {
      setData(null)
      setLoading(false)
      return
    }
    setLoading(true)
    Promise.all([
      supabase
        .from('qualifying_results')
        .select('*')
        .eq('session_key', sessionKey)
        .order('best_lap_time', { ascending: true, nullsFirst: false }),
      supabase
        .from('laps')
        .select('driver_number,lap_number,lap_duration,duration_sector_1,duration_sector_2,duration_sector_3,date_start,st_speed')
        .eq('session_key', sessionKey),
      supabase
        .from('race_control')
        .select('date,qualifying_phase')
        .eq('session_key', sessionKey)
        .not('qualifying_phase', 'is', null),
      supabase
        .from('drivers')
        .select('*')
        .eq('session_key', sessionKey),
      supabase
        .from('stints')
        .select('driver_number,stint_number,lap_start,lap_end,compound,tyre_age_at_start')
        .eq('session_key', sessionKey),
      supabase
        .from('weather')
        .select('*')
        .eq('session_key', sessionKey)
        .order('date'),
      supabase
        .from('starting_grid')
        .select('driver_number,position')
        .eq('session_key', sessionKey),
    ]).then(([resultsRes, lapsRes, rcRes, driversRes, stintsRes, weatherRes, gridRes]) => {
      const drivers = driversRes.data ?? []
      const driverMap = Object.fromEntries(drivers.map(d => [d.driver_number, d]))

      // Race grid slot, used only to flag grid penalties — not for ordering.
      const gridMap = Object.fromEntries(
        (gridRes.data ?? []).map(g => [g.driver_number, g.position])
      )

      const results = (resultsRes.data ?? []).map(r => ({
        ...r,
        driver: driverMap[r.driver_number] ?? null,
        grid_position: gridMap[r.driver_number] ?? null,
      }))

      const stintsByDriver = {}
      for (const s of stintsRes.data ?? []) {
        if (!stintsByDriver[s.driver_number]) stintsByDriver[s.driver_number] = []
        stintsByDriver[s.driver_number].push(s)
      }

      // Order by the official qualifying classification. Never use starting_grid:
      // it includes grid penalties, so it is not the qualifying order.
      const sorted = sortQualifyingResults(results)

      setData({
        results: sorted,
        laps: lapsRes.data ?? [],
        phaseEvents: rcRes.data ?? [],
        driverMap,
        stintsByDriver,
        weatherData: weatherRes.data ?? [],
      })
      setLoading(false)
    })
  }, [sessionKey])

  return { data, loading }
}
