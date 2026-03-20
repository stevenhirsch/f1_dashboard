import { useState, useEffect } from 'react'
import { supabase } from '../supabaseClient'

export function useRaceResults(sessionKey) {
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
    ]).then(([resultsRes, driversRes]) => {
      const results = resultsRes.data ?? []
      const driverMap = Object.fromEntries(
        (driversRes.data ?? []).map(d => [d.driver_number, d])
      )
      const merged = results.map(r => ({
        ...r,
        drivers: driverMap[r.driver_number] ?? null,
      }))
      setData(merged)
      setLoading(false)
    })
  }, [sessionKey])

  return { data, loading }
}
