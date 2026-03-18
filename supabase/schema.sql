-- F1 Dashboard — Supabase Schema
-- Run this in the Supabase SQL editor to set up all tables and RLS.
-- All upserts are keyed on natural identifiers so running the pipeline twice is safe.

-- ---------------------------------------------------------------------------
-- races  (meeting metadata)
-- ---------------------------------------------------------------------------
create table if not exists races (
    meeting_key             integer primary key,
    meeting_name            text,
    meeting_official_name   text,
    circuit_short_name      text,
    circuit_key             integer,
    country_name            text,
    country_code            text,
    location                text,
    year                    integer,
    date_start              timestamptz
);

alter table races enable row level security;
create policy "anon read" on races for select to anon using (true);


-- ---------------------------------------------------------------------------
-- sessions
-- ---------------------------------------------------------------------------
create table if not exists sessions (
    session_key         integer primary key,
    meeting_key         integer references races(meeting_key),
    session_name        text,
    session_type        text,
    date_start          timestamptz,
    date_end            timestamptz,
    year                integer,
    circuit_key         integer,
    circuit_short_name  text,
    country_name        text,
    location            text
);

alter table sessions enable row level security;
create policy "anon read" on sessions for select to anon using (true);


-- ---------------------------------------------------------------------------
-- drivers  (per session — a driver's team/number can change between seasons)
-- ---------------------------------------------------------------------------
create table if not exists drivers (
    session_key     integer references sessions(session_key),
    driver_number   integer,
    name_acronym    text,
    full_name       text,
    broadcast_name  text,
    team_name       text,
    team_colour     text,
    country_code    text,
    headshot_url    text,
    primary key (session_key, driver_number)
);

alter table drivers enable row level security;
create policy "anon read" on drivers for select to anon using (true);


-- ---------------------------------------------------------------------------
-- laps
-- ---------------------------------------------------------------------------
create table if not exists laps (
    session_key         integer references sessions(session_key),
    driver_number       integer,
    lap_number          integer,
    lap_duration        numeric,
    duration_sector_1   numeric,
    duration_sector_2   numeric,
    duration_sector_3   numeric,
    i1_speed            integer,
    i2_speed            integer,
    st_speed            integer,
    is_pit_out_lap      boolean,
    date_start          timestamptz,
    primary key (session_key, driver_number, lap_number)
);

alter table laps enable row level security;
create policy "anon read" on laps for select to anon using (true);


-- ---------------------------------------------------------------------------
-- lap_metrics  (derived values — populated by Phase 5)
-- ---------------------------------------------------------------------------
create table if not exists lap_metrics (
    session_key                 integer references sessions(session_key),
    driver_number               integer,
    lap_number                  integer,
    -- G-force proxies
    p90_accel_g                 numeric,
    p90_decel_g                 numeric,
    p90_lateral_g               numeric,
    -- Driving inputs
    coasting_ratio              numeric,
    throttle_brake_overlap      numeric,
    smoothness_index            numeric,
    full_throttle_pct           numeric,
    -- Pace
    pace_degradation_rate       numeric,
    clean_air_pace              numeric,
    stint_phase_early_pace      numeric,
    stint_phase_late_pace       numeric,
    -- Braking
    braking_consistency_index   numeric,
    -- Load indices
    high_lateral_load_index     numeric,
    longitudinal_load_index     numeric,
    -- Metadata
    computed_at                 timestamptz default now(),
    primary key (session_key, driver_number, lap_number)
);

alter table lap_metrics enable row level security;
create policy "anon read" on lap_metrics for select to anon using (true);


-- ---------------------------------------------------------------------------
-- stints
-- ---------------------------------------------------------------------------
create table if not exists stints (
    session_key         integer references sessions(session_key),
    driver_number       integer,
    stint_number        integer,
    lap_start           integer,
    lap_end             integer,
    compound            text,
    tyre_age_at_start   integer,
    primary key (session_key, driver_number, stint_number)
);

alter table stints enable row level security;
create policy "anon read" on stints for select to anon using (true);


-- ---------------------------------------------------------------------------
-- pit_stops
-- ---------------------------------------------------------------------------
create table if not exists pit_stops (
    session_key     integer references sessions(session_key),
    driver_number   integer,
    lap_number      integer,
    pit_duration    numeric,
    stop_duration   numeric,    -- available from 2024 US GP onwards
    date            timestamptz,
    primary key (session_key, driver_number, lap_number)
);

alter table pit_stops enable row level security;
create policy "anon read" on pit_stops for select to anon using (true);


-- ---------------------------------------------------------------------------
-- weather
-- ---------------------------------------------------------------------------
create table if not exists weather (
    session_key         integer references sessions(session_key),
    date                timestamptz,
    track_temperature   numeric,
    air_temperature     numeric,
    humidity            numeric,
    pressure            numeric,
    rainfall            numeric,
    wind_direction      integer,
    wind_speed          numeric,
    primary key (session_key, date)
);

alter table weather enable row level security;
create policy "anon read" on weather for select to anon using (true);


-- ---------------------------------------------------------------------------
-- race_control
-- ---------------------------------------------------------------------------
create table if not exists race_control (
    session_key     integer references sessions(session_key),
    date            timestamptz,
    lap_number      integer,
    category        text,
    flag            text,
    message         text,
    driver_number   integer,
    scope           text,
    sector          integer,
    primary key (session_key, date)
);

alter table race_control enable row level security;
create policy "anon read" on race_control for select to anon using (true);


-- ---------------------------------------------------------------------------
-- race_results
-- ---------------------------------------------------------------------------
create table if not exists race_results (
    session_key     integer references sessions(session_key),
    driver_number   integer,
    position        integer,
    points          numeric,
    gap_to_leader   text,       -- can be "+N LAP" strings
    duration        numeric,    -- total race seconds for race winner
    dnf             boolean default false,
    dns             boolean default false,
    dsq             boolean default false,
    pit_count       integer,
    fastest_lap_flag boolean,
    status_detail   text,       -- DNF/DSQ reason — populated in Phase 1
    primary key (session_key, driver_number)
);

alter table race_results enable row level security;
create policy "anon read" on race_results for select to anon using (true);


-- ---------------------------------------------------------------------------
-- qualifying_results
-- ---------------------------------------------------------------------------
create table if not exists qualifying_results (
    session_key     integer references sessions(session_key),
    driver_number   integer,
    best_lap_time   numeric,
    best_lap_number integer,
    -- Q1/Q2/Q3 splits — populated in Phase 2
    q1_time         numeric,
    q2_time         numeric,
    q3_time         numeric,
    primary key (session_key, driver_number)
);

alter table qualifying_results enable row level security;
create policy "anon read" on qualifying_results for select to anon using (true);


-- ---------------------------------------------------------------------------
-- overtakes
-- ---------------------------------------------------------------------------
create table if not exists overtakes (
    session_key                 integer references sessions(session_key),
    lap_number                  integer,
    driver_number_overtaking    integer,
    driver_number_overtaken     integer,
    position                    integer,
    primary key (session_key, lap_number, driver_number_overtaking, driver_number_overtaken)
);

alter table overtakes enable row level security;
create policy "anon read" on overtakes for select to anon using (true);
