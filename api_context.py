API_CONTEXT = """
You have access to the OpenF1 REST API at https://api.openf1.org/v1.

FILTER SYNTAX:
- Exact match: ?param=value
- Greater than or equal: ?param>=value
- Less than or equal: ?param<=value
- Greater than: ?param>value
- Less than: ?param<value
- Special keyword: session_key=latest (most recent session), meeting_key=latest

ENDPOINTS:

/v1/sessions
  Params: session_key, session_name, session_type (Practice, Qualifying, Sprint, Race), year, circuit_key, circuit_short_name, country_name, location, date_start, date_end
  - country_name: the COUNTRY (e.g. "China", "Italy", "Singapore") — use this to filter by GP country
  - location: the CIRCUIT CITY (e.g. "Shanghai", "Monza", "Silverstone") — different from country_name
  Example: /v1/sessions?session_type=Race&year=2023&country_name=Singapore

/v1/meetings
  Params: meeting_key, meeting_name, meeting_official_name, year, country_key, country_name, country_code, location, circuit_key, circuit_short_name, date_start
  Example: /v1/meetings?year=2024&country_name=Monaco

/v1/drivers
  Params: session_key, driver_number, broadcast_name, full_name, name_acronym, team_name, team_colour, country_code
  Example: /v1/drivers?session_key=latest&name_acronym=VER

/v1/laps
  Params: session_key, driver_number, lap_number, lap_duration, duration_sector_1, duration_sector_2, duration_sector_3, i1_speed, i2_speed, st_speed, is_pit_out_lap, date_start
  Example: /v1/laps?session_key=latest&driver_number=1

/v1/car_data
  Params: session_key, driver_number, speed, rpm, gear, throttle, brake, drs, date
  Example: /v1/car_data?session_key=latest&driver_number=44&speed>=300

/v1/position
  Params: session_key, driver_number, position, date
  Example: /v1/position?session_key=latest&driver_number=1

/v1/intervals
  Params: session_key, driver_number, interval, gap_to_leader, date
  Example: /v1/intervals?session_key=latest&driver_number=1

/v1/pit
  Params: session_key, driver_number, pit_duration, lap_number, date
  Example: /v1/pit?session_key=latest

/v1/stints
  Params: session_key, driver_number, stint_number, lap_start, lap_end, compound, tyre_age_at_start
  Example: /v1/stints?session_key=latest&driver_number=44

/v1/weather
  Params: session_key, air_temperature, humidity, pressure, rainfall, track_temperature, wind_direction, wind_speed, date
  Example: /v1/weather?session_key=latest

/v1/race_control
  Params: session_key, driver_number, lap_number, category (Flag, SafetyCar, Drs, Other), flag, message, date
  Example: /v1/race_control?session_key=latest&category=Flag

/v1/session_result (alias: /v1/results)
  Params: session_key, driver_number, position, points, time, status
  Example: /v1/session_result?session_key=latest

/v1/starting_grid
  Params: session_key, driver_number, grid_position
  Example: /v1/starting_grid?session_key=latest

/v1/overtakes
  Params: session_key, driver_number_overtaking, driver_number_overtaken, lap_number
  Example: /v1/overtakes?session_key=latest

/v1/championship_driver
  Params: year, driver_number, position, points
  Example: /v1/championship_driver?year=2023

/v1/championship_team
  Params: year, team_name, position, points
  Example: /v1/championship_team?year=2023

NOTES:
- Use session_key=latest ONLY when the user is asking about the current/most recent live session
- For any historical query (specific year or GP name), ALWAYS fetch /v1/sessions first
  with year + country_name + session_type to get the correct session_key
  e.g. /v1/sessions?year=2025&country_name=China&session_type=Race
- When fetching /v1/sessions for a race result, include session_type=Race to get only the Race session
- ALWAYS include /v1/drivers in your call list when fetching session_result, laps, or position data
  so that driver numbers can be resolved to names
- For race winners, use /v1/session_result and look for position=1
- driver_number is the racing number (e.g. 1=Verstappen, 44=Hamilton, 63=Russell, 81=Piastri)
"""
