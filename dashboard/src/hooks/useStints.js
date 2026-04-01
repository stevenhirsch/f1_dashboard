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
        .select('driver_number, position, dnf, dns, dsq, number_of_laps')
        .eq('session_key', sessionKey),
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

      function sortKey(r) {
        if (r.dsq) return 4
        if (r.dns) return 3
        if (r.dnf) return 2
        if (r.position == null) return 1  // classified but position missing (many laps down)
        return 0
      }
      const order = [...results]
        .sort((a, b) => {
          const ka = sortKey(a), kb = sortKey(b)
          if (ka !== kb) return ka - kb
          if (ka === 0) return a.position - b.position
          return (b.number_of_laps ?? 0) - (a.number_of_laps ?? 0)
        })
        .map(r => ({
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
