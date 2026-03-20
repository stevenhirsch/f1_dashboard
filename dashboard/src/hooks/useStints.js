import { useState, useEffect } from 'react'
import { supabase } from '../supabaseClient'

export function useStints(sessionKey) {
  const [data, setData] = useState(null)
  const [driverOrder, setDriverOrder] = useState([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!sessionKey) { setLoading(false); return }
    setLoading(true)

    Promise.all([
      supabase
        .from('stints')
        .select('*')
        .eq('session_key', sessionKey)
        .order('driver_number')
        .order('stint_number'),
      supabase
        .from('race_results')
        .select('driver_number, position')
        .eq('session_key', sessionKey)
        .order('position'),
      supabase
        .from('drivers')
        .select('driver_number, name_acronym')
        .eq('session_key', sessionKey),
    ]).then(([stintsRes, resultsRes, driversRes]) => {
      const stints = stintsRes.data ?? []
      const results = resultsRes.data ?? []
      const driverMap = Object.fromEntries(
        (driversRes.data ?? []).map(d => [d.driver_number, d])
      )

      const order = results.map(r => ({
        driver_number: r.driver_number,
        name_acronym: driverMap[r.driver_number]?.name_acronym ?? String(r.driver_number),
        position: r.position ?? 999,
      }))
      setDriverOrder(order)

      const indexed = {}
      for (const stint of stints) {
        if (!indexed[stint.driver_number]) indexed[stint.driver_number] = []
        indexed[stint.driver_number].push(stint)
      }
      setData(indexed)
      setLoading(false)
    })
  }, [sessionKey])

  return { data, driverOrder, loading }
}
