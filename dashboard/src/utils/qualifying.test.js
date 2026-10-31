import { describe, it, expect } from 'vitest'
import { sortQualifyingResults } from './qualifying'

const order = rows => sortQualifyingResults(rows).map(r => r.driver_number)

describe('sortQualifyingResults', () => {
  it('uses official position, ignoring grid penalties', () => {
    // Baku 2026: SAI (55) qualified P9, COL (43) P10, BEA (87) P11
    const rows = [
      { driver_number: 87, position: 11, q1_time: 105.2, q2_time: 104.7 },
      { driver_number: 55, position: 9, q1_time: 105.1, q2_time: 104.6, q3_time: 104.566 },
      { driver_number: 43, position: 10, q1_time: 105.1, q2_time: 104.6, q3_time: 104.963 },
    ]
    expect(order(rows)).toEqual([55, 43, 87])
  })

  it('falls back to knockout order when position is missing', () => {
    const rows = [
      { driver_number: 1, q1_time: 100 },                              // out in Q1, fastest Q1 time
      { driver_number: 2, q1_time: 102, q2_time: 101 },                // out in Q2
      { driver_number: 3, q1_time: 103, q2_time: 102, q3_time: 104.9 },
      { driver_number: 4, q1_time: 103, q2_time: 102, q3_time: 104.5 },
    ]
    expect(order(rows)).toEqual([4, 3, 2, 1])
  })

  it('puts disqualified drivers last', () => {
    const rows = [
      { driver_number: 6, position: 22, dsq: true, q3_time: 90 },
      { driver_number: 1, position: 1, q3_time: 91 },
      { driver_number: 2, position: 2, q3_time: 92 },
    ]
    expect(order(rows)).toEqual([1, 2, 6])
  })
})
