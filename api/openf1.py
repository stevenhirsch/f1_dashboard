"""OpenF1 REST API client with simple in-memory caching."""

import time
import requests

BASE_URL = "https://api.openf1.org/v1"
_cache: dict = {}


def _get(endpoint: str, params) -> list[dict]:
    """
    Make a GET request to the OpenF1 API with caching.

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
        return _cache[key]

    for _attempt in range(6):
        resp = requests.get(f"{BASE_URL}/{endpoint}", params=params_list, timeout=30)
        if resp.status_code != 429:
            resp.raise_for_status()
            break
        if _attempt < 5:
            time.sleep(2 ** _attempt)  # 1, 2, 4, 8, 16s
    else:
        resp.raise_for_status()  # re-raise after exhausting retries

    data = resp.json()
    _cache[key] = data
    return data


def clear_cache() -> None:
    _cache.clear()


# --- Session selection ---

def get_meetings(year: int) -> list[dict]:
    return _get("meetings", {"year": year})


def get_sessions(meeting_key: int) -> list[dict]:
    return _get("sessions", {"meeting_key": meeting_key})


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
    """All position records for a session (all drivers)."""
    return _get("position", {"session_key": session_key})


def get_session_result(session_key: int) -> list[dict]:
    """Official session results: position, points, time/gap, and status per driver."""
    return _get("session_result", {"session_key": session_key})


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
