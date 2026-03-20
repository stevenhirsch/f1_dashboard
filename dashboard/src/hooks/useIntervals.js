import { useState, useEffect } from 'react'
import { supabase } from '../supabaseClient'

export function useIntervals(sessionKey) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!sessionKey) { setLoading(false); return }
    setLoading(true)

    Promise.all([
      supabase
        .from('intervals')
        .select('driver_number, date, gap_to_leader, laps_down')
        .eq('session_key', sessionKey)
        .order('date'),
      supabase
        .from('race_results')
        .select('driver_number, position')
        .eq('session_key', sessionKey)
        .order('position'),
      supabase
        .from('drivers')
        .select('driver_number, name_acronym, team_colour')
        .eq('session_key', sessionKey),
    ]).then(([intervalsRes, resultsRes, driversRes]) => {
      const intervals = intervalsRes.data ?? []
      const driverMap = Object.fromEntries(
        (driversRes.data ?? []).map(d => [d.driver_number, d])
      )
      const positionMap = Object.fromEntries(
        (resultsRes.data ?? []).map(r => [r.driver_number, r.position])
      )

      const enriched = intervals.map(row => ({
        ...row,
        name_acronym: driverMap[row.driver_number]?.name_acronym ?? String(row.driver_number),
        team_colour: driverMap[row.driver_number]?.team_colour ?? '888888',
        position: positionMap[row.driver_number] ?? 99,
      }))

      setData(enriched)
      setLoading(false)
    })
  }, [sessionKey])

  return { data, loading }
}
