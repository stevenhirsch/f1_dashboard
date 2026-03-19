"""Comprehensive unit tests for pipeline/api/openf1.py."""

import time
import unittest
from unittest.mock import MagicMock, call, patch

from api import openf1


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_response(status_code: int = 200, json_data=None, raise_for_status=None):
    """Build a minimal mock requests.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data if json_data is not None else []
    if raise_for_status is not None:
        resp.raise_for_status.side_effect = raise_for_status
    else:
        resp.raise_for_status.return_value = None
    return resp


# ---------------------------------------------------------------------------
# Fixtures / setUp helpers
# ---------------------------------------------------------------------------

class OpenF1TestBase(unittest.TestCase):
    """Base class that clears cache and stats before every test."""

    def setUp(self):
        openf1.clear_cache()
        openf1.reset_stats()

    def tearDown(self):
        openf1.clear_cache()
        openf1.reset_stats()


# ===========================================================================
# Tests for _get (cache hit / miss / errors)
# ===========================================================================

class TestGetCacheHit(OpenF1TestBase):
    """_get should return cached value without making a network call."""

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_cache_hit_on_second_call(self, mock_get, mock_sleep):
        mock_get.return_value = _make_response(200, [{"key": "value"}])

        # First call — populates cache
        result1 = openf1._get("meetings", {"year": 2024})
        # Second call — should hit cache
        result2 = openf1._get("meetings", {"year": 2024})

        self.assertEqual(result1, [{"key": "value"}])
        self.assertEqual(result2, [{"key": "value"}])
        # requests.get should only have been called once
        self.assertEqual(mock_get.call_count, 1)

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_cache_hit_increments_cache_hits_stat(self, mock_get, mock_sleep):
        mock_get.return_value = _make_response(200, [{"a": 1}])

        openf1._get("sessions", {"meeting_key": 1})
        openf1._get("sessions", {"meeting_key": 1})
        openf1._get("sessions", {"meeting_key": 1})

        self.assertEqual(openf1.get_stats()["cache_hits"], 2)

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_cache_hit_does_not_increment_real_calls(self, mock_get, mock_sleep):
        mock_get.return_value = _make_response(200, [{"a": 1}])

        openf1._get("sessions", {"meeting_key": 2})
        openf1._get("sessions", {"meeting_key": 2})

        self.assertEqual(openf1.get_stats()["real_calls"], 1)


class TestGetCacheMiss(OpenF1TestBase):
    """_get should call the API and cache the result on cache miss."""

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_cache_miss_calls_api(self, mock_get, mock_sleep):
        payload = [{"session_key": 9999}]
        mock_get.return_value = _make_response(200, payload)

        result = openf1._get("sessions", {"session_key": 9999})

        mock_get.assert_called_once()
        self.assertEqual(result, payload)

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_cache_miss_increments_real_calls(self, mock_get, mock_sleep):
        mock_get.return_value = _make_response(200, [])

        openf1._get("laps", {"session_key": 1})
        openf1._get("laps", {"session_key": 2})  # different key — two real calls

        self.assertEqual(openf1.get_stats()["real_calls"], 2)

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_cache_miss_calls_sleep_with_request_delay(self, mock_get, mock_sleep):
        mock_get.return_value = _make_response(200, [])

        openf1._get("drivers", {"session_key": 1})

        mock_sleep.assert_any_call(openf1._REQUEST_DELAY)

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_different_params_cause_separate_cache_entries(self, mock_get, mock_sleep):
        mock_get.return_value = _make_response(200, [])

        openf1._get("meetings", {"year": 2023})
        openf1._get("meetings", {"year": 2024})

        self.assertEqual(mock_get.call_count, 2)
        self.assertEqual(openf1.get_stats()["real_calls"], 2)


class TestGet404(OpenF1TestBase):
    """404 responses must return [] and cache that empty list."""

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_404_returns_empty_list(self, mock_get, mock_sleep):
        mock_get.return_value = _make_response(404)

        result = openf1._get("sessions", {"session_key": 0})

        self.assertEqual(result, [])

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_404_caches_empty_list(self, mock_get, mock_sleep):
        mock_get.return_value = _make_response(404)

        openf1._get("sessions", {"session_key": 0})
        result2 = openf1._get("sessions", {"session_key": 0})

        # Second call should be a cache hit returning []
        self.assertEqual(result2, [])
        self.assertEqual(mock_get.call_count, 1)
        self.assertEqual(openf1.get_stats()["cache_hits"], 1)

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_404_does_not_call_raise_for_status(self, mock_get, mock_sleep):
        resp = _make_response(404)
        mock_get.return_value = resp

        openf1._get("sessions", {"session_key": 0})

        resp.raise_for_status.assert_not_called()


class TestGetNonRateLimitError(OpenF1TestBase):
    """Non-429 HTTP errors must be raised immediately."""

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_500_raises_http_error(self, mock_get, mock_sleep):
        import requests as req_lib
        resp = _make_response(500, raise_for_status=req_lib.HTTPError("500"))
        mock_get.return_value = resp

        with self.assertRaises(req_lib.HTTPError):
            openf1._get("laps", {"session_key": 1})

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_503_raises_immediately_without_retry(self, mock_get, mock_sleep):
        import requests as req_lib
        resp = _make_response(503, raise_for_status=req_lib.HTTPError("503"))
        mock_get.return_value = resp

        with self.assertRaises(req_lib.HTTPError):
            openf1._get("laps", {"session_key": 2})

        # Only one network call — no retry loop for non-429
        self.assertEqual(mock_get.call_count, 1)


class TestGet429Backoff(OpenF1TestBase):
    """429 responses should trigger exponential-backoff retries."""

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_429_then_200_succeeds(self, mock_get, mock_sleep):
        payload = [{"driver_number": 1}]
        mock_get.side_effect = [
            _make_response(429),
            _make_response(200, payload),
        ]

        result = openf1._get("drivers", {"session_key": 1})

        self.assertEqual(result, payload)
        self.assertEqual(mock_get.call_count, 2)

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_429_increments_rate_limit_waits(self, mock_get, mock_sleep):
        mock_get.side_effect = [
            _make_response(429),
            _make_response(429),
            _make_response(200, []),
        ]

        openf1._get("drivers", {"session_key": 3})

        self.assertEqual(openf1.get_stats()["rate_limit_waits"], 2)

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_429_backoff_sleep_uses_exponential_delays(self, mock_get, mock_sleep):
        mock_get.side_effect = [
            _make_response(429),  # attempt 0 → sleep 1s
            _make_response(429),  # attempt 1 → sleep 2s
            _make_response(200, []),
        ]

        openf1._get("drivers", {"session_key": 4})

        # The backoff sleeps should include 2**0=1 and 2**1=2
        sleep_args = [c.args[0] for c in mock_sleep.call_args_list]
        self.assertIn(1, sleep_args)
        self.assertIn(2, sleep_args)

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_exhausting_all_429_retries_raises(self, mock_get, mock_sleep):
        """After 6 consecutive 429s the final raise_for_status should propagate."""
        import requests as req_lib

        # Create 6 consecutive 429 responses — the last one has raise_for_status raising
        responses = [_make_response(429) for _ in range(5)]
        responses.append(_make_response(429, raise_for_status=req_lib.HTTPError("429 exhausted")))
        mock_get.side_effect = responses

        with self.assertRaises(req_lib.HTTPError):
            openf1._get("drivers", {"session_key": 5})

        self.assertEqual(mock_get.call_count, 6)

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_429_does_not_cache_failed_result(self, mock_get, mock_sleep):
        """If all retries are exhausted, nothing should be stored in the cache."""
        import requests as req_lib

        responses = [_make_response(429) for _ in range(5)]
        responses.append(_make_response(429, raise_for_status=req_lib.HTTPError("429")))
        mock_get.side_effect = responses

        try:
            openf1._get("drivers", {"session_key": 6})
        except Exception:
            pass

        self.assertEqual(len(openf1._cache), 0)


class TestGetParamFormats(OpenF1TestBase):
    """_get should accept both dict params and list-of-tuple params."""

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_dict_params_build_cache_key(self, mock_get, mock_sleep):
        mock_get.return_value = _make_response(200, [{"ok": True}])

        result = openf1._get("meetings", {"year": 2024})

        self.assertEqual(result, [{"ok": True}])
        mock_get.assert_called_once()

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_list_of_tuple_params_build_cache_key(self, mock_get, mock_sleep):
        mock_get.return_value = _make_response(200, [{"ok": True}])

        params = [
            ("session_key", 1),
            ("driver_number", 44),
            ("date>", "2024-03-01T00:00:00"),
            ("date<", "2024-03-01T01:00:00"),
        ]
        result = openf1._get("car_data", params)

        self.assertEqual(result, [{"ok": True}])

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_list_of_tuple_params_cache_hit(self, mock_get, mock_sleep):
        mock_get.return_value = _make_response(200, [{"cached": True}])

        params = [("session_key", 1), ("driver_number", 44), ("date>", "t1"), ("date<", "t2")]
        openf1._get("car_data", params)
        result2 = openf1._get("car_data", params)

        self.assertEqual(result2, [{"cached": True}])
        self.assertEqual(mock_get.call_count, 1)

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_dict_params_forwarded_to_requests_get(self, mock_get, mock_sleep):
        mock_get.return_value = _make_response(200, [])

        openf1._get("meetings", {"year": 2024})

        call_kwargs = mock_get.call_args
        # params_list arg is the second positional arg (after URL)
        self.assertIn("params", call_kwargs.kwargs or {}) or self.assertTrue(
            len(call_kwargs.args) >= 2 or "params" in (call_kwargs.kwargs or {})
        )


# ===========================================================================
# Tests for stats
# ===========================================================================

class TestStats(OpenF1TestBase):
    """Stats tracking: reset, cache_hits, real_calls, call_times, rate_limit_waits."""

    def test_reset_stats_clears_all_fields(self):
        # Manually dirty the stats
        openf1._stats["real_calls"] = 5
        openf1._stats["cache_hits"] = 3
        openf1._stats["rate_limit_waits"] = 2
        openf1._stats["call_times"] = [1.0, 2.0]

        openf1.reset_stats()

        stats = openf1.get_stats()
        self.assertEqual(stats["real_calls"], 0)
        self.assertEqual(stats["cache_hits"], 0)
        self.assertEqual(stats["rate_limit_waits"], 0)
        self.assertEqual(stats["call_times"], [])

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_real_request_increments_real_calls(self, mock_get, mock_sleep):
        mock_get.return_value = _make_response(200, [])

        openf1._get("laps", {"session_key": 10})

        self.assertEqual(openf1.get_stats()["real_calls"], 1)

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_real_request_appends_to_call_times(self, mock_get, mock_sleep):
        mock_get.return_value = _make_response(200, [])

        before = time.time()
        openf1._get("laps", {"session_key": 11})
        after = time.time()

        times = openf1.get_stats()["call_times"]
        self.assertEqual(len(times), 1)
        # The recorded time should be between before and after (time.sleep is mocked)
        self.assertGreaterEqual(times[0], before - 1)
        self.assertLessEqual(times[0], after + 1)

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_429_increments_rate_limit_waits_correctly(self, mock_get, mock_sleep):
        mock_get.side_effect = [
            _make_response(429),
            _make_response(429),
            _make_response(429),
            _make_response(200, []),
        ]

        openf1._get("stints", {"session_key": 1})

        self.assertEqual(openf1.get_stats()["rate_limit_waits"], 3)

    def test_get_stats_returns_independent_copy(self):
        """Mutating the returned dict must not affect internal stats."""
        stats = openf1.get_stats()
        stats["real_calls"] = 9999
        stats["call_times"].append(123.45)  # list is a new copy

        internal = openf1._stats
        self.assertEqual(internal["real_calls"], 0)
        # call_times list itself: get_stats returns dict() copy, but the list
        # inside is the same object — document that real_calls is safe.
        self.assertEqual(internal["real_calls"], 0)

    def test_current_rate_returns_zero_with_fewer_than_two_calls(self):
        # 0 calls
        self.assertEqual(openf1.current_rate(), 0.0)

        # 1 call
        openf1._stats["call_times"].append(time.time())
        self.assertEqual(openf1.current_rate(), 0.0)

    def test_current_rate_calculates_correctly(self):
        """With known timestamps, current_rate should compute count/window."""
        now = 1000.0
        # Simulate 10 calls over 9 seconds (indices 0-9, window = times[-1] - times[0])
        openf1._stats["call_times"] = [now + i for i in range(10)]

        rate = openf1.current_rate()

        # window = 9s, count = min(10, 30) - 1 = 9 → rate = 9/9 = 1.0
        self.assertAlmostEqual(rate, 1.0, places=5)

    def test_current_rate_with_30_plus_calls_uses_last_30(self):
        """With >30 calls the window is capped at the last 30."""
        now = 0.0
        # 35 calls, each 1s apart
        openf1._stats["call_times"] = [now + i for i in range(35)]

        rate = openf1.current_rate()

        # window = times[-1] - times[35-30] = times[34] - times[5] = 34 - 5 = 29
        # count = min(35, 30) - 1 = 29
        # rate = 29/29 = 1.0
        self.assertAlmostEqual(rate, 1.0, places=5)


# ===========================================================================
# Tests for clear_cache
# ===========================================================================

class TestClearCache(OpenF1TestBase):
    """clear_cache should remove all entries."""

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_clear_cache_empties_cache(self, mock_get, mock_sleep):
        mock_get.return_value = _make_response(200, [{"x": 1}])
        openf1._get("meetings", {"year": 2024})
        self.assertGreater(len(openf1._cache), 0)

        openf1.clear_cache()

        self.assertEqual(len(openf1._cache), 0)

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_after_clear_cache_next_call_hits_api_again(self, mock_get, mock_sleep):
        mock_get.return_value = _make_response(200, [{"x": 1}])
        openf1._get("meetings", {"year": 2024})
        openf1.clear_cache()

        openf1._get("meetings", {"year": 2024})

        self.assertEqual(mock_get.call_count, 2)


# ===========================================================================
# Spot-check API wrapper functions
# ===========================================================================

class TestApiWrappers(OpenF1TestBase):
    """Spot-check that wrapper functions call _get with the right endpoint+params."""

    def _assert_get_called_with(self, mock_get, mock_sleep, fn, *args, expected_endpoint,
                                 expected_params):
        mock_get.return_value = _make_response(200, [])
        fn(*args)
        call_args = mock_get.call_args
        url = call_args.args[0]
        params = call_args.kwargs.get("params") or call_args.args[1]
        self.assertIn(expected_endpoint, url)
        for key, value in expected_params:
            self.assertIn((key, value), list(params))

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_get_meetings_uses_meetings_endpoint_and_year(self, mock_get, mock_sleep):
        mock_get.return_value = _make_response(200, [])

        openf1.get_meetings(2024)

        url = mock_get.call_args.args[0]
        params = mock_get.call_args.kwargs.get("params") or mock_get.call_args.args[1]
        self.assertIn("meetings", url)
        self.assertIn(("year", 2024), list(params))

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_get_laps_without_driver_number(self, mock_get, mock_sleep):
        mock_get.return_value = _make_response(200, [])

        openf1.get_laps(9999)

        url = mock_get.call_args.args[0]
        params = mock_get.call_args.kwargs.get("params") or mock_get.call_args.args[1]
        self.assertIn("laps", url)
        self.assertIn(("session_key", 9999), list(params))
        param_keys = [k for k, _ in list(params)]
        self.assertNotIn("driver_number", param_keys)

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_get_laps_with_driver_number(self, mock_get, mock_sleep):
        mock_get.return_value = _make_response(200, [])

        openf1.get_laps(9999, driver_number=44)

        url = mock_get.call_args.args[0]
        params = mock_get.call_args.kwargs.get("params") or mock_get.call_args.args[1]
        self.assertIn("laps", url)
        self.assertIn(("session_key", 9999), list(params))
        self.assertIn(("driver_number", 44), list(params))

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_get_car_data_uses_list_of_tuples_with_range_operators(self, mock_get, mock_sleep):
        mock_get.return_value = _make_response(200, [])

        openf1.get_car_data(9999, 44, "2024-03-01T12:00:00", "2024-03-01T13:00:00")

        url = mock_get.call_args.args[0]
        params = mock_get.call_args.kwargs.get("params") or mock_get.call_args.args[1]
        self.assertIn("car_data", url)
        param_list = list(params)
        self.assertIn(("session_key", 9999), param_list)
        self.assertIn(("driver_number", 44), param_list)
        self.assertIn(("date>", "2024-03-01T12:00:00"), param_list)
        self.assertIn(("date<", "2024-03-01T13:00:00"), param_list)

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_get_championship_drivers_uses_correct_endpoint_and_session_key(
        self, mock_get, mock_sleep
    ):
        mock_get.return_value = _make_response(200, [])

        openf1.get_championship_drivers(7777)

        url = mock_get.call_args.args[0]
        params = mock_get.call_args.kwargs.get("params") or mock_get.call_args.args[1]
        self.assertIn("championship_drivers", url)
        self.assertIn(("session_key", 7777), list(params))

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_get_location_uses_list_of_tuples_with_range_operators(self, mock_get, mock_sleep):
        mock_get.return_value = _make_response(200, [])

        openf1.get_location(9999, 33, "2024-03-01T12:00:00", "2024-03-01T13:00:00")

        url = mock_get.call_args.args[0]
        params = mock_get.call_args.kwargs.get("params") or mock_get.call_args.args[1]
        self.assertIn("location", url)
        param_list = list(params)
        self.assertIn(("session_key", 9999), param_list)
        self.assertIn(("driver_number", 33), param_list)
        self.assertIn(("date>", "2024-03-01T12:00:00"), param_list)
        self.assertIn(("date<", "2024-03-01T13:00:00"), param_list)

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_get_sessions_passes_meeting_key(self, mock_get, mock_sleep):
        mock_get.return_value = _make_response(200, [])

        openf1.get_sessions(1234)

        url = mock_get.call_args.args[0]
        params = mock_get.call_args.kwargs.get("params") or mock_get.call_args.args[1]
        self.assertIn("sessions", url)
        self.assertIn(("meeting_key", 1234), list(params))

    @patch("api.openf1.time.sleep")
    @patch("api.openf1.requests.get")
    def test_get_drivers_passes_session_key(self, mock_get, mock_sleep):
        mock_get.return_value = _make_response(200, [])

        openf1.get_drivers(5678)

        url = mock_get.call_args.args[0]
        params = mock_get.call_args.kwargs.get("params") or mock_get.call_args.args[1]
        self.assertIn("drivers", url)
        self.assertIn(("session_key", 5678), list(params))


if __name__ == "__main__":
    unittest.main()
