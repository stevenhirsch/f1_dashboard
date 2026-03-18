import { useState, useEffect } from 'react'
import { supabase } from '../supabaseClient'

/**
 * Loads available years, GPs, and sessions from Supabase.
 * Returns the current selection and setter functions.
 */
export function useSessionSelector() {
  const [years, setYears] = useState([])
  const [meetings, setMeetings] = useState([])
  const [sessions, setSessions] = useState([])

  const [selectedYear, setSelectedYear] = useState(null)
  const [selectedMeetingKey, setSelectedMeetingKey] = useState(null)
  const [selectedSessionKey, setSelectedSessionKey] = useState(null)

  // Load years from URL params on mount, default to latest
  useEffect(() => {
    const params = new URLSearchParams(window.location.search)
    const yearParam = params.get('year')

    supabase
      .from('races')
      .select('year')
      .order('year', { ascending: false })
      .then(({ data }) => {
        if (!data) return
        const uniqueYears = [...new Set(data.map(r => r.year))].sort((a, b) => b - a)
        setYears(uniqueYears)
        setSelectedYear(yearParam ? parseInt(yearParam) : uniqueYears[0] ?? null)
      })
  }, [])

  // Load meetings when year changes
  useEffect(() => {
    if (!selectedYear) return
    supabase
      .from('races')
      .select('meeting_key, meeting_name, date_start')
      .eq('year', selectedYear)
      .order('date_start', { ascending: false })
      .then(({ data }) => {
        setMeetings(data ?? [])
        setSelectedMeetingKey(data?.[0]?.meeting_key ?? null)
      })
  }, [selectedYear])

  // Load sessions when meeting changes
  useEffect(() => {
    if (!selectedMeetingKey) return
    const allowed = ['Race', 'Qualifying', 'Sprint', 'Sprint Qualifying', 'Sprint Shootout']
    supabase
      .from('sessions')
      .select('session_key, session_name, session_type, date_start')
      .eq('meeting_key', selectedMeetingKey)
      .in('session_name', allowed)
      .order('date_start')
      .then(({ data }) => {
        setSessions(data ?? [])
        const race = data?.find(s => s.session_name === 'Race')
        setSelectedSessionKey(race?.session_key ?? data?.[0]?.session_key ?? null)
      })
  }, [selectedMeetingKey])

  // Sync selection to URL
  useEffect(() => {
    const params = new URLSearchParams(window.location.search)
    if (selectedYear) params.set('year', selectedYear)
    if (selectedMeetingKey) params.set('meeting', selectedMeetingKey)
    if (selectedSessionKey) params.set('session', selectedSessionKey)
    window.history.replaceState({}, '', `?${params}`)
  }, [selectedYear, selectedMeetingKey, selectedSessionKey])

  return {
    years, meetings, sessions,
    selectedYear, setSelectedYear,
    selectedMeetingKey, setSelectedMeetingKey,
    selectedSessionKey, setSelectedSessionKey,
  }
}
