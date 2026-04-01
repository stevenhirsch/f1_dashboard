import { useState, useEffect } from 'react'
import { supabase } from '../supabaseClient'

export function useRacePositionHistory(sessionKey, qualifyingSessionKey) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!sessionKey) { setLoading(false); return }
    setLoading(true)

    Promise.all([
      // High limit — position table has many rows for a full race
      supabase
        .from('position')
        .select('driver_number,date,position')
        .eq('session_key', sessionKey)
        .order('date')
        .limit(200000),
      supabase
        .from('laps')
        .select('driver_number,lap_number,date_start')
        .eq('session_key', sessionKey)
        .order('lap_number'),
      supabase
        .from('race_results')
        .select('driver_number,position,dnf,dns,dsq,number_of_laps')
        .eq('session_key', sessionKey),
      supabase
        .from('drivers')
        .select('driver_number,name_acronym,team_name,team_colour')
        .eq('session_key', sessionKey),
      supabase
        .from('race_control')
        .select('date,category,message')
        .eq('session_key', sessionKey)
        .order('date'),
      qualifyingSessionKey
        ? supabase
            .from('starting_grid')
            .select('driver_number,position')
            .eq('session_key', qualifyingSessionKey)
        : Promise.resolve({ data: [] }),
    ]).then(([posRes, lapsRes, resultsRes, driversRes, rcRes, gridRes]) => {
      setData({
        positionRows: posRes.data ?? [],
        laps: lapsRes.data ?? [],
        results: resultsRes.data ?? [],
        drivers: driversRes.data ?? [],
        raceControl: rcRes.data ?? [],
        startingGrid: gridRes.data ?? [],
      })
      setLoading(false)
    })
  }, [sessionKey, qualifyingSessionKey])

  return { data, loading }
}
