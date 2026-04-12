import { supabase } from '../supabaseClient'
import { METRIC_META } from './definitions'

// Entry point — routes tool calls to the right handler.
// Returns a result string the model can read.
export async function executeToolCall(name, args, year) {
  try {
    if (name === 'aggregate_metric') return await aggregateMetric(args, year)
    return `Unknown tool: ${name}`
  } catch (err) {
    return `Tool error: ${err.message}`
  }
}

async function aggregateMetric(
  { metric, aggregation = 'avg', group_by, driver, race },
  year
) {
  const meta = METRIC_META[metric]
  if (!meta) {
    return `Unknown metric "${metric}". Available: ${Object.keys(METRIC_META).join(', ')}`
  }

  // 1. Races for this year
  const { data: racesData } = await supabase
    .from('races')
    .select('meeting_key, meeting_name, circuit_short_name, location')
    .eq('year', year)
  const races = racesData ?? []
  if (!races.length) return `No races found for ${year}.`

  const raceLabel = r => r.circuit_short_name || r.location || r.meeting_name
  const raceNameMap = Object.fromEntries(races.map(r => [r.meeting_key, raceLabel(r)]))
  let meetingKeys = races.map(r => r.meeting_key)

  // 2. Optionally filter to one race
  if (race) {
    const q = race.toLowerCase()
    const match = races.find(r =>
      r.circuit_short_name?.toLowerCase().includes(q) ||
      r.location?.toLowerCase().includes(q) ||
      r.meeting_name?.toLowerCase().includes(q)
    )
    if (!match) return `Race "${race}" not found. Available: ${races.map(raceLabel).join(', ')}`
    meetingKeys = [match.meeting_key]
  }

  // 3. Race/sprint session keys — filter client-side (mirrors useChatContext) to avoid case-sensitivity issues
  const { data: sessionData } = await supabase
    .from('sessions')
    .select('session_key, meeting_key, session_type')
    .in('meeting_key', meetingKeys)
  const sessions = (sessionData ?? [])
    .filter(s => ['race', 'sprint'].includes(s.session_type?.toLowerCase()))
  const sessionKeys = sessions.map(s => s.session_key)
  if (!sessionKeys.length) return 'No race sessions found.'

  const skToMeeting = Object.fromEntries(sessions.map(s => [s.session_key, s.meeting_key]))

  // 4. Resolve driver acronym → number
  let driverNumber = null
  let resolvedAcr  = driver?.toUpperCase()
  if (driver) {
    const { data: dRows } = await supabase
      .from('drivers')
      .select('driver_number, name_acronym')
      .in('session_key', sessionKeys)
      .ilike('name_acronym', driver)
      .limit(1)
    if (!dRows?.length) return `Driver "${driver}" not found in ${year} race sessions.`
    driverNumber = dRows[0].driver_number
    resolvedAcr  = dRows[0].name_acronym
  }

  // 5. Driver acronym lookup for labelling
  const { data: allDrivers } = await supabase
    .from('drivers')
    .select('driver_number, name_acronym')
    .in('session_key', sessionKeys)
  const acrByNumber = {}
  for (const d of (allDrivers ?? [])) {
    if (!acrByNumber[d.driver_number]) acrByNumber[d.driver_number] = d.name_acronym
  }

  // 6. Query the metric table
  let q = supabase
    .from(meta.table)
    .select(`session_key, driver_number, ${metric}`)
    .in('session_key', sessionKeys)
    .not(metric, 'is', null)
  if (driverNumber) q = q.eq('driver_number', driverNumber)

  const { data: rows, error } = await q
  if (error) return `Query error: ${error.message}`
  if (!rows?.length) {
    return `No ${metric} data found${resolvedAcr ? ` for ${resolvedAcr}` : ''}${race ? ` at ${race}` : ''}.`
  }

  // 7. Aggregate
  function agg(vals) {
    if (!vals.length) return null
    switch (aggregation) {
      case 'avg':    return vals.reduce((a, b) => a + b, 0) / vals.length
      case 'max':    return Math.max(...vals)
      case 'min':    return Math.min(...vals)
      case 'sum':    return vals.reduce((a, b) => a + b, 0)
      case 'median': {
        const s = [...vals].sort((a, b) => a - b)
        const m = Math.floor(s.length / 2)
        return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2
      }
      default: return null
    }
  }

  const fmt = v => v == null ? '—' : (Math.abs(v) >= 10 ? v.toFixed(2) : v.toFixed(4))

  // When a driver is specified, always return a single scalar
  const effectiveGroup = driverNumber ? 'none' : (group_by ?? 'driver')

  if (effectiveGroup === 'none') {
    const vals   = rows.map(r => Number(r[metric])).filter(n => !isNaN(n))
    const result = agg(vals)
    const label  = `${aggregation}(${metric})${resolvedAcr ? ` [${resolvedAcr}]` : ''}${race ? ` at ${race}` : ''} — ${year} season`
    return `${label} = ${fmt(result)} (n=${vals.length} laps)`
  }

  if (effectiveGroup === 'driver') {
    const byDriver = {}
    for (const row of rows) {
      const v = Number(row[metric])
      if (!isNaN(v)) {
        if (!byDriver[row.driver_number]) byDriver[row.driver_number] = []
        byDriver[row.driver_number].push(v)
      }
    }
    const results = Object.entries(byDriver)
      .map(([dn, vals]) => ({ acr: acrByNumber[dn] ?? dn, value: agg(vals), n: vals.length }))
      .filter(r => r.value != null)
      .sort((a, b) => b.value - a.value)

    const label = `${aggregation}(${metric}) per driver${race ? ` at ${race}` : ''} — ${year} season`
    return `${label}:\n${results.map((r, i) => `  ${i + 1}. ${r.acr}: ${fmt(r.value)} (n=${r.n})`).join('\n')}`
  }

  if (effectiveGroup === 'race') {
    const byMeeting = {}
    for (const row of rows) {
      const mk = skToMeeting[row.session_key]
      const v  = Number(row[metric])
      if (!isNaN(v)) {
        if (!byMeeting[mk]) byMeeting[mk] = []
        byMeeting[mk].push(v)
      }
    }
    const results = Object.entries(byMeeting)
      .map(([mk, vals]) => ({ race: raceNameMap[mk] ?? mk, value: agg(vals), n: vals.length }))
      .filter(r => r.value != null)
      .sort((a, b) => b.value - a.value)

    const label = `${aggregation}(${metric}) per race — ${year} season`
    return `${label}:\n${results.map((r, i) => `  ${i + 1}. ${r.race}: ${fmt(r.value)} (n=${r.n})`).join('\n')}`
  }

  return 'Invalid group_by value.'
}
