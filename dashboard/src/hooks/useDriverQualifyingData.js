import { useState, useEffect } from 'react'
import { supabase } from '../supabaseClient'

/**
 * Fetches all qualifying data for a single driver in a session.
 * Returns laps, stints, qualifying result, phase events, driver metadata,
 * and grid position.
 */
export function useDriverQualifyingData(sessionKey, driverNumber) {
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
        .from('qualifying_results')
        .select('*')
        .eq('session_key', sessionKey)
        .eq('driver_number', driverNumber),
      supabase
        .from('race_control')
        .select('date,qualifying_phase')
        .eq('session_key', sessionKey)
        .not('qualifying_phase', 'is', null),
      supabase
        .from('drivers')
        .select('*')
        .eq('session_key', sessionKey)
        .eq('driver_number', driverNumber),
      supabase
        .from('starting_grid')
        .select('driver_number,position')
        .eq('session_key', sessionKey)
        .eq('driver_number', driverNumber),
    ]).then(([lapsRes, stintsRes, qualRes, rcRes, driverRes, gridRes]) => {
      setData({
        laps: lapsRes.data ?? [],
        stints: stintsRes.data ?? [],
        qualResult: qualRes.data?.[0] ?? null,
        phaseEvents: rcRes.data ?? [],
        driver: driverRes.data?.[0] ?? null,
        gridPosition: gridRes.data?.[0]?.position ?? null,
      })
      setLoading(false)
    })
  }, [sessionKey, driverNumber])

  return { data, loading }
}
