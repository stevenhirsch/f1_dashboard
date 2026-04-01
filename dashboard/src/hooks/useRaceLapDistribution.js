import { useState, useEffect } from 'react'
import { supabase } from '../supabaseClient'

export function useRaceLapDistribution(sessionKey) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!sessionKey) { setLoading(false); return }
    setLoading(true)

    Promise.all([
      supabase
        .from('laps')
        .select('driver_number,lap_number,lap_duration,is_pit_out_lap')
        .eq('session_key', sessionKey)
        .order('lap_number'),
      supabase
        .from('stints')
        .select('driver_number,stint_number,lap_start,lap_end,compound')
        .eq('session_key', sessionKey)
        .order('stint_number'),
      supabase
        .from('race_results')
        .select('driver_number,position,dns')
        .eq('session_key', sessionKey)
        .order('position'),
      supabase
        .from('drivers')
        .select('driver_number,name_acronym,team_colour')
        .eq('session_key', sessionKey),
      supabase
        .from('lap_metrics')
        .select('driver_number,lap_number,is_neutralized')
        .eq('session_key', sessionKey),
    ]).then(([lapsRes, stintsRes, resultsRes, driversRes, metricsRes]) => {
      setData({
        laps: lapsRes.data ?? [],
        stints: stintsRes.data ?? [],
        results: resultsRes.data ?? [],
        drivers: driversRes.data ?? [],
        lapMetrics: metricsRes.data ?? [],
      })
      setLoading(false)
    })
  }, [sessionKey])

  return { data, loading }
}
