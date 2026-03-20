import { useState, useEffect } from 'react'
import { supabase } from '../supabaseClient'

export function useWeather(sessionKey) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!sessionKey) { setLoading(false); return }
    setLoading(true)
    supabase
      .from('weather')
      .select('*')
      .eq('session_key', sessionKey)
      .order('date')
      .then(({ data }) => { setData(data ?? []); setLoading(false) })
  }, [sessionKey])

  return { data, loading }
}
