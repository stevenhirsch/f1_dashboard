/**
 * Returns contiguous distance intervals [x0, x1] where throttle < 1 % AND
 * brake = 0.  Minimum width of 20 m filters out single noisy samples.
 */
export function coastingIntervals(pts) {
  if (!pts || pts.length === 0) return []
  const intervals = []
  let start = null
  for (const pt of pts) {
    const coasting = (pt.throttle ?? 0) < 1 && (pt.brake ?? 0) === 0
    if (coasting && start === null) {
      start = pt.distance
    } else if (!coasting && start !== null) {
      if (pt.distance - start >= 20) intervals.push({ x0: start, x1: pt.distance })
      start = null
    }
  }
  if (start !== null) {
    const last = pts[pts.length - 1].distance
    if (last - start >= 20) intervals.push({ x0: start, x1: last })
  }
  return intervals
}

/**
 * Merges car_data and location arrays for a single lap, adding cumulative
 * distance (metres from lap start) computed by integrating speed × Δt.
 *
 * Both arrays are sampled at ~3.7 Hz and share a UTC date field.
 * Location points are matched to the nearest car_data timestamp.
 */
export function computeTelemetry(carData, locationData) {
  if (!carData || carData.length === 0) return []

  const sortedCar = [...carData].sort((a, b) => (a.date < b.date ? -1 : 1))
  const sortedLoc = locationData
    ? [...locationData].sort((a, b) => (a.date < b.date ? -1 : 1))
    : []

  // Cumulative distance via speed integration
  let dist = 0
  const carWithDist = sortedCar.map((pt, i) => {
    if (i > 0) {
      const dt = (new Date(pt.date) - new Date(sortedCar[i - 1].date)) / 1000
      dist += ((pt.speed ?? 0) / 3.6) * dt
    }
    return { ...pt, distance: Math.round(dist) }
  })

  // Merge nearest location point into each car data point
  let locIdx = 0
  return carWithDist.map(pt => {
    const ptMs = new Date(pt.date).getTime()
    while (locIdx < sortedLoc.length - 1) {
      const cur  = Math.abs(new Date(sortedLoc[locIdx    ].date).getTime() - ptMs)
      const next = Math.abs(new Date(sortedLoc[locIdx + 1].date).getTime() - ptMs)
      if (next < cur) locIdx++
      else break
    }
    const loc = sortedLoc[locIdx]
    return {
      ...pt,
      x: loc?.x ?? null,
      y: loc?.y ?? null,
      z: loc?.z ?? null,
    }
  })
}
