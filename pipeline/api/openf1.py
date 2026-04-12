"""OpenF1 REST API client with rate-limit-aware retries and in-memory caching."""

import time
import requests

BASE_URL = "https://api.openf1.org/v1"
_cache: dict = {}

# Minimum delay between requests to stay within free tier limits (3 req/s, 30 req/min).
_REQUEST_DELAY = 0.35

# ---------------------------------------------------------------------------
# Request stats — reset per session, read at session end for logging
# ---------------------------------------------------------------------------

_stats: dict = {
    "real_calls": 0,        # actual HTTP requests sent (excluding cache hits, including retries)
    "cache_hits": 0,        # requests served from in-memory cache
    "rate_limit_waits": 0,  # number of 429 responses received
    "call_times": [],       # wall-clock time of each real call (for rate calculation)
}


def reset_stats() -> None:
    _stats["real_calls"] = 0
    _stats["cache_hits"] = 0
    _stats["rate_limit_waits"] = 0
    _stats["call_times"] = []


def get_stats() -> dict:
    """Return a copy of current request stats."""
    return dict(_stats)


def current_rate() -> float:
    """Requests per second over the last 30 real calls (rolling window)."""
    times = _stats["call_times"]
    if len(times) < 2:
        return 0.0
    window = times[-1] - times[max(0, len(times) - 30)]
    count = min(len(times), 30) - 1
    return count / window if window > 0 else 0.0


def _get(endpoint: str, params) -> list[dict]:
    """
    Make a GET request to the OpenF1 API with caching and 429/422 backoff.

    params can be a dict or a list of (key, value) tuples.
    List-of-tuples allows OpenF1 range operators like ('date>', ...).
    Returns an empty list on any HTTP error.
    """
    if isinstance(params, dict):
        params_list = sorted(params.items())
    else:
        params_list = list(params)

    key = endpoint + "?" + "&".join(f"{k}={v}" for k, v in params_list)
    if key in _cache:
        _stats["cache_hits"] += 1
        return _cache[key]

    time.sleep(_REQUEST_DELAY)
    _stats["real_calls"] += 1
    _stats["call_times"].append(time.time())

    for attempt in range(6):
        resp = requests.get(f"{BASE_URL}/{endpoint}", params=params_list, timeout=30)
        if resp.status_code == 404:
            _cache[key] = []
            return []
        if resp.status_code not in (429, 422):
            resp.raise_for_status()
            break
        _stats["rate_limit_waits"] += 1
        wait = 2 ** attempt
        print(f"  [transient {resp.status_code}] on /{endpoint}, waiting {wait}s "
              f"(attempt {attempt + 1}/6)")
        time.sleep(wait)
    else:
        resp.raise_for_status()

    data = resp.json()
    _cache[key] = data
    return data


def clear_cache() -> None:
    _cache.clear()


# --- Session selection ---

def get_meetings(year: int) -> list[dict]:
    return _get("meetings", {"year": year})


def get_meeting(meeting_key: int) -> list[dict]:
    return _get("meetings", {"meeting_key": meeting_key})


def get_sessions(meeting_key: int) -> list[dict]:
    return _get("sessions", {"meeting_key": meeting_key})


def get_session(session_key: int) -> list[dict]:
    return _get("sessions", {"session_key": session_key})


# --- Session data ---

def get_drivers(session_key: int) -> list[dict]:
    return _get("drivers", {"session_key": session_key})


def get_stints(session_key: int) -> list[dict]:
    return _get("stints", {"session_key": session_key})


def get_laps(session_key: int, driver_number: int | None = None) -> list[dict]:
    params: dict = {"session_key": session_key}
    if driver_number is not None:
        params["driver_number"] = driver_number
    return _get("laps", params)


def get_pit(session_key: int) -> list[dict]:
    return _get("pit", {"session_key": session_key})


def get_race_control(session_key: int) -> list[dict]:
    return _get("race_control", {"session_key": session_key})


def get_weather(session_key: int) -> list[dict]:
    return _get("weather", {"session_key": session_key})


def get_position(session_key: int, driver_number: int) -> list[dict]:
    return _get("position", {"session_key": session_key, "driver_number": driver_number})


def get_all_positions(session_key: int) -> list[dict]:
    return _get("position", {"session_key": session_key})


def get_session_result(session_key: int) -> list[dict]:
    return _get("session_result", {"session_key": session_key})


def get_intervals(session_key: int) -> list[dict]:
    """Gap to leader and interval for all drivers throughout the session."""
    return _get("intervals", {"session_key": session_key})


def get_overtakes(session_key: int) -> list[dict]:
    """Overtake events for the session."""
    return _get("overtakes", {"session_key": session_key})


def get_starting_grid(session_key: int) -> list[dict]:
    """Starting grid positions for the session."""
    return _get("starting_grid", {"session_key": session_key})


def get_team_radio(session_key: int) -> list[dict]:
    """Team radio recording metadata for the session."""
    return _get("team_radio", {"session_key": session_key})


def get_championship_drivers(session_key: int) -> list[dict]:
    """Driver championship standings for a race session (Beta endpoint)."""
    return _get("championship_drivers", {"session_key": session_key})


def get_championship_teams(session_key: int) -> list[dict]:
    """Constructor championship standings for a race session (Beta endpoint)."""
    return _get("championship_teams", {"session_key": session_key})


# --- Telemetry — filtered by date range, not lap_number ---
# /car_data and /location do not support lap_number as a filter.
# Pass the lap's date_start and date_end (from /laps) as range bounds.

def get_car_data(session_key: int, driver_number: int,
                 date_start: str, date_end: str) -> list[dict]:
    return _get("car_data", [
        ("session_key", session_key),
        ("driver_number", driver_number),
        ("date>", date_start),
        ("date<", date_end),
    ])


def get_location(session_key: int, driver_number: int,
                 date_start: str, date_end: str) -> list[dict]:
    return _get("location", [
        ("session_key", session_key),
        ("driver_number", driver_number),
        ("date>", date_start),
        ("date<", date_end),
    ])
