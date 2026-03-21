import { useState, useEffect, useRef } from 'react'
import { fetchCarData, fetchLocation } from '../api/openf1'
import { computeTelemetry } from '../utils/telemetry'

/**
 * Fetches and merges car_data + location for a set of selected laps,
 * querying OpenF1 directly (not Supabase — data volume is too large to store).
 *
 * selectedLaps: array of { lap_number, date_start, lap_duration }
 * Returns: { data: { [lap_number]: mergedPoint[] }, loading: boolean }
 *
 * Already-fetched laps are cached in a ref so re-selecting a lap is instant.
 */
export function useLapTelemetry(sessionKey, driverNumber, selectedLaps) {
  const cacheRef = useRef({})
  const [data, setData] = useState({})
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (!selectedLaps || selectedLaps.length === 0) {
      setData({})
      return
    }

    const selectedNums = new Set(selectedLaps.map(l => l.lap_number))

    const toFetch = selectedLaps.filter(lap => {
      const key = `${sessionKey}-${driverNumber}-${lap.lap_number}`
      return !cacheRef.current[key]
    })

    const buildData = () => {
      const result = {}
      for (const lap of selectedLaps) {
        const key = `${sessionKey}-${driverNumber}-${lap.lap_number}`
        if (cacheRef.current[key]) result[lap.lap_number] = cacheRef.current[key]
      }
      setData(result)
    }

    if (toFetch.length === 0) {
      buildData()
      return
    }

    setLoading(true)
    Promise.all(
      toFetch.map(async lap => {
        if (!lap.date_start || !lap.lap_duration) return null
        const dateStart = lap.date_start
        const dateEnd = new Date(
          new Date(lap.date_start).getTime() + lap.lap_duration * 1000,
        ).toISOString()

        const [carData, locData] = await Promise.all([
          fetchCarData(sessionKey, driverNumber, dateStart, dateEnd),
          fetchLocation(sessionKey, driverNumber, dateStart, dateEnd),
        ])
        return { lap_number: lap.lap_number, points: computeTelemetry(carData, locData) }
      }),
    )
      .then(results => {
        for (const r of results) {
          if (!r) continue
          const key = `${sessionKey}-${driverNumber}-${r.lap_number}`
          cacheRef.current[key] = r.points
        }
        buildData()
        setLoading(false)
      })
      .catch(err => {
        console.error('Telemetry fetch error:', err)
        setLoading(false)
      })

    // Remove data for deselected laps
    setData(prev => {
      const next = {}
      for (const [k, v] of Object.entries(prev)) {
        if (selectedNums.has(Number(k))) next[k] = v
      }
      return next
    })
  }, [sessionKey, driverNumber, selectedLaps])

  return { data, loading }
}
