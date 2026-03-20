import { useState, useEffect } from 'react'
import { supabase } from '../supabaseClient'

const SPRINT_QUALIFYING_NAMES = ['Sprint Qualifying', 'Sprint Shootout']

/**
 * Loads years, race weekends, and sessions from Supabase.
 * Derives raceSessionKey and qualifyingSessionKey from the selected event
 * so that consumers never need to think about raw session keys.
 */
export function useSessionSelector() {
  const [years, setYears] = useState([])
  const [meetings, setMeetings] = useState([])
  const [sessions, setSessions] = useState([])

  const [selectedYear, setSelectedYear] = useState(null)
  const [selectedMeetingKey, setSelectedMeetingKey] = useState(null)
  const [selectedEvent, setSelectedEvent] = useState('Grand Prix') // 'Grand Prix' | 'Sprint'
  const [gmtOffset, setGmtOffset] = useState(null)

  // --- Derived values ---
  const isSprintWeekend = sessions.some(s => s.session_name === 'Sprint')
  const events = isSprintWeekend ? ['Grand Prix', 'Sprint'] : ['Grand Prix']

  const raceSessionKey = sessions.find(s =>
    selectedEvent === 'Sprint' ? s.session_name === 'Sprint' : s.session_name === 'Race'
  )?.session_key ?? null

  const qualifyingSessionKey = sessions.find(s =>
    selectedEvent === 'Sprint'
      ? SPRINT_QUALIFYING_NAMES.includes(s.session_name)
      : s.session_name === 'Qualifying'
  )?.session_key ?? null

  // --- Load years (on mount) ---
  useEffect(() => {
    supabase
      .from('races')
      .select('year')
      .order('year', { ascending: false })
      .then(({ data }) => {
        if (!data) return
        const uniqueYears = [...new Set(data.map(r => r.year))].sort((a, b) => b - a)
        setYears(uniqueYears)

        const params = new URLSearchParams(window.location.search)
        const yearParam = params.get('year')
        setSelectedYear(yearParam ? parseInt(yearParam) : uniqueYears[0] ?? null)
      })
  }, [])

  // --- Load meetings when year changes ---
  useEffect(() => {
    if (!selectedYear) return
    supabase
      .from('races')
      .select('meeting_key, meeting_name, date_start, gmt_offset')
      .eq('year', selectedYear)
      .order('date_start', { ascending: false })
      .then(({ data }) => {
        setMeetings(data ?? [])
        const first = data?.[0] ?? null
        setSelectedMeetingKey(first?.meeting_key ?? null)
        setGmtOffset(first?.gmt_offset ?? null)
      })
  }, [selectedYear])

  // --- Load sessions when meeting changes ---
  useEffect(() => {
    if (!selectedMeetingKey) return
    const relevant = ['Race', 'Qualifying', 'Sprint', ...SPRINT_QUALIFYING_NAMES]
    supabase
      .from('sessions')
      .select('session_key, session_name, session_type, date_start')
      .eq('meeting_key', selectedMeetingKey)
      .in('session_name', relevant)
      .order('date_start')
      .then(({ data }) => {
        setSessions(data ?? [])
        setSelectedEvent('Grand Prix') // reset to default when weekend changes
      })
  }, [selectedMeetingKey])

  // --- Sync selection to URL ---
  useEffect(() => {
    const params = new URLSearchParams(window.location.search)
    if (selectedYear) params.set('year', selectedYear)
    if (selectedMeetingKey) params.set('weekend', selectedMeetingKey)
    if (selectedEvent) params.set('event', selectedEvent === 'Sprint' ? 'sprint' : 'gp')
    window.history.replaceState({}, '', `?${params}`)
  }, [selectedYear, selectedMeetingKey, selectedEvent])

  // --- Setters that keep derived state in sync ---
  function setMeetingKey(key) {
    setSelectedMeetingKey(key)
    const meeting = meetings.find(m => m.meeting_key === key)
    setGmtOffset(meeting?.gmt_offset ?? null)
  }

  return {
    years,
    meetings,
    selectedYear, setSelectedYear,
    selectedMeetingKey, setSelectedMeetingKey: setMeetingKey,
    selectedEvent, setSelectedEvent,
    isSprintWeekend,
    events,
    raceSessionKey,
    qualifyingSessionKey,
    gmtOffset,
  }
}
