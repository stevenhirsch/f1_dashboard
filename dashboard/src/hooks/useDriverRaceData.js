import { useState, useEffect } from 'react'
import { supabase } from '../supabaseClient'

/**
 * Fetches all race data for a single driver in a session.
 * Returns laps, stints, pit stops, race result, intervals, overtakes,
 * position timeline, and driver metadata.
 */
export function useDriverRaceData(sessionKey, driverNumber) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!sessionKey || driverNumber == null) {
      setData(null)
      setLoading(false)
      return
    }
    setLoading(true)
    Promise.all([
      supabase
        .from('laps')
        .select('*')
        .eq('session_key', sessionKey)
        .eq('driver_number', driverNumber)
        .order('lap_number'),
      supabase
        .from('stints')
        .select('*')
        .eq('session_key', sessionKey)
        .eq('driver_number', driverNumber)
        .order('stint_number'),
      supabase
        .from('pit_stops')
        .select('*')
        .eq('session_key', sessionKey)
        .eq('driver_number', driverNumber)
        .order('lap_number'),
      supabase
        .from('race_results')
        .select('*')
        .eq('session_key', sessionKey)
        .eq('driver_number', driverNumber),
      supabase
        .from('intervals')
        .select('date,gap_to_leader,interval,laps_down')
        .eq('session_key', sessionKey)
        .eq('driver_number', driverNumber)
        .order('date'),
      supabase
        .from('overtakes')
        .select('*')
        .eq('session_key', sessionKey)
        .or(`driver_number_overtaking.eq.${driverNumber},driver_number_overtaken.eq.${driverNumber}`)
        .order('date'),
      supabase
        .from('position')
        .select('date,position')
        .eq('session_key', sessionKey)
        .eq('driver_number', driverNumber)
        .order('date'),
      supabase
        .from('drivers')
        .select('*')
        .eq('session_key', sessionKey)
        .eq('driver_number', driverNumber),
    ]).then(([lapsRes, stintsRes, pitsRes, resultRes, intervalsRes, overtakesRes, positionRes, driverRes]) => {
      setData({
        laps: lapsRes.data ?? [],
        stints: stintsRes.data ?? [],
        pitStops: pitsRes.data ?? [],
        result: resultRes.data?.[0] ?? null,
        intervals: intervalsRes.data ?? [],
        overtakes: overtakesRes.data ?? [],
        positions: positionRes.data ?? [],
        driver: driverRes.data?.[0] ?? null,
      })
      setLoading(false)
    })
  }, [sessionKey, driverNumber])

  return { data, loading }
}
