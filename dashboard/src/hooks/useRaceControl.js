import { useState, useEffect } from 'react'
import { supabase } from '../supabaseClient'

export function useRaceControl(sessionKey) {
  const [safetyCarPeriods, setSafetyCarPeriods] = useState([])
  const [retirements, setRetirements] = useState({})
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!sessionKey) { setLoading(false); return }
    setLoading(true)
    supabase
      .from('race_control')
      .select('*')
      .eq('session_key', sessionKey)
      .order('date')
      .then(({ data }) => {
        if (!data) { setLoading(false); return }

        // Parse SC/VSC periods using category field (matches OpenF1 API structure)
        const scPeriods = []
        const vscPeriods = []
        const retirementMap = {}

        for (const row of data) {
          const cat = row.category ?? ''
          const msg = (row.message ?? '').toUpperCase()

          if (cat === 'SafetyCar') {
            const isVSC = msg.includes('VIRTUAL') || msg.includes('VSC')
            const list = isVSC ? vscPeriods : scPeriods
            if (msg.includes('DEPLOYED')) {
              list.push({ lapStart: row.lap_number, lapEnd: null, type: isVSC ? 'VSC' : 'SC', dateStart: row.date })
            } else if (msg.includes('IN THIS LAP') || msg.includes('ENDING')) {
              const last = list[list.length - 1]
              if (last && last.lapEnd === null) {
                last.lapEnd = row.lap_number
                last.dateEnd = row.date
              }
            }
          }

          // Retirement messages
          if (row.driver_number && msg.includes('RETIR')) {
            retirementMap[row.driver_number] = row.message
          }
        }

        setSafetyCarPeriods([...scPeriods, ...vscPeriods])
        setRetirements(retirementMap)
        setLoading(false)
      })
  }, [sessionKey])

  return { safetyCarPeriods, retirements, loading }
}
