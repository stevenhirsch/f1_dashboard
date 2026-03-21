const BASE = 'https://api.openf1.org/v1'

async function get(endpoint, queryString) {
  const res = await fetch(`${BASE}/${endpoint}?${queryString}`)
  if (!res.ok) throw new Error(`OpenF1 ${endpoint} failed: ${res.status}`)
  return res.json()
}

export function fetchCarData(sessionKey, driverNumber, dateStart, dateEnd) {
  return get(
    'car_data',
    `session_key=${sessionKey}&driver_number=${driverNumber}&date>=${dateStart}&date<=${dateEnd}`,
  )
}

export function fetchLocation(sessionKey, driverNumber, dateStart, dateEnd) {
  return get(
    'location',
    `session_key=${sessionKey}&driver_number=${driverNumber}&date>=${dateStart}&date<=${dateEnd}`,
  )
}
