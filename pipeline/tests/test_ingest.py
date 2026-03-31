"""Comprehensive unit and integration tests for pipeline/ingest.py."""

import unittest
from contextlib import ExitStack
from unittest.mock import MagicMock, call, patch

import httpx

import ingest
from ingest import (
    _assign_qualifying_phases,
    _brake_zone_stats,
    _build_neutralized_periods,
    _compute_lap_metrics,
    _compute_qualifying_best_per_phase,
    _find_brake_zones,
    _get_compound_for_lap,
    _normalize_phase,
    _parse_gap,
    _parse_intervals_index,
    _pick,
    _query_in,
    _upsert,
    ingest_battle_states,
    ingest_stint_metrics,
    ingest_championship_drivers,
    ingest_championship_teams,
    ingest_season_driver_stats,
    ingest_season_constructor_stats,
    ingest_drivers,
    ingest_brake_entry_speed_ranks,
    ingest_fastest_lap_flag,
    ingest_lap_flags,
    ingest_session_sector_bests,
    ingest_intervals,
    ingest_lap_metrics,
    ingest_laps,
    ingest_meeting,
    ingest_overtakes,
    ingest_pit_stops,
    ingest_qualifying_peak_g_summary,
    ingest_qualifying_results,
    ingest_race_control,
    ingest_race_results,
    ingest_session,
    ingest_starting_grid,
    ingest_stints,
    ingest_weather,
    process_meeting,
    process_session,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_client():
    """Return a mock Supabase client whose table().upsert().execute() chain succeeds."""
    client = MagicMock()
    client.table.return_value.upsert.return_value.execute.return_value = MagicMock()
    return client


# ===========================================================================
# _parse_gap
# ===========================================================================

class TestParseGap(unittest.TestCase):

    def test_none_returns_none_none(self):
        self.assertEqual(_parse_gap(None), (None, None))

    def test_numeric_float_returns_float_and_none(self):
        self.assertEqual(_parse_gap(1.234), (1.234, None))

    def test_numeric_string_returns_float_and_none(self):
        self.assertEqual(_parse_gap("5.678"), (5.678, None))

    def test_plus_one_lap_string_returns_none_and_one(self):
        gap, laps = _parse_gap("+1 LAP")
        self.assertIsNone(gap)
        self.assertEqual(laps, 1)

    def test_plus_two_laps_string_returns_none_and_two(self):
        gap, laps = _parse_gap("+2 LAPS")
        self.assertIsNone(gap)
        self.assertEqual(laps, 2)

    def test_lapped_string_without_plus_returns_none_none(self):
        # A string with LAP but without a leading integer is unparseable
        gap, laps = _parse_gap("LAP BEHIND")
        self.assertIsNone(gap)
        self.assertIsNone(laps)

    def test_arbitrary_non_numeric_string_returns_none_none(self):
        gap, laps = _parse_gap("DNF")
        self.assertIsNone(gap)
        self.assertIsNone(laps)

    def test_zero_value_returns_zero_float(self):
        gap, laps = _parse_gap(0)
        self.assertEqual(gap, 0.0)
        self.assertIsNone(laps)

    def test_zero_string_returns_zero_float(self):
        gap, laps = _parse_gap("0.0")
        self.assertEqual(gap, 0.0)
        self.assertIsNone(laps)

    def test_negative_float_returns_negative_float(self):
        gap, laps = _parse_gap(-3.14)
        self.assertAlmostEqual(gap, -3.14)
        self.assertIsNone(laps)

    def test_negative_string_returns_negative_float(self):
        gap, laps = _parse_gap("-1.5")
        self.assertAlmostEqual(gap, -1.5)
        self.assertIsNone(laps)

    def test_empty_string_returns_none_none(self):
        self.assertEqual(_parse_gap(""), (None, None))

    def test_integer_value_returns_float(self):
        gap, laps = _parse_gap(3)
        self.assertEqual(gap, 3.0)
        self.assertIsNone(laps)


# ===========================================================================
# _pick
# ===========================================================================

class TestPick(unittest.TestCase):

    def test_normal_pick(self):
        d = {"a": 1, "b": 2, "c": 3}
        self.assertEqual(_pick(d, "a", "c"), {"a": 1, "c": 3})

    def test_missing_keys_excluded(self):
        d = {"a": 1}
        result = _pick(d, "a", "b", "c")
        self.assertEqual(result, {"a": 1})

    def test_all_missing_returns_empty_dict(self):
        d = {"x": 99}
        self.assertEqual(_pick(d, "a", "b"), {})

    def test_empty_dict_returns_empty_dict(self):
        self.assertEqual(_pick({}, "a"), {})

    def test_no_keys_requested_returns_empty_dict(self):
        self.assertEqual(_pick({"a": 1}, ), {})

    def test_value_none_is_included(self):
        d = {"a": None}
        self.assertEqual(_pick(d, "a"), {"a": None})


# ===========================================================================
# _upsert
# ===========================================================================

class TestUpsert(unittest.TestCase):

    def test_empty_rows_does_not_call_table(self):
        client = _mock_client()
        _upsert(client, "laps", [])
        client.table.assert_not_called()

    @patch("ingest.time.sleep")
    def test_1001_rows_produces_three_chunks(self, mock_sleep):
        """1001 rows with _CHUNK_SIZE=500 → chunks of 500, 500, 1."""
        client = _mock_client()
        rows = [{"id": i} for i in range(1001)]

        _upsert(client, "laps", rows)

        execute_calls = client.table.return_value.upsert.return_value.execute.call_count
        self.assertEqual(execute_calls, 3)

        upsert_call_args = client.table.return_value.upsert.call_args_list
        self.assertEqual(len(upsert_call_args[0].args[0]), 500)
        self.assertEqual(len(upsert_call_args[1].args[0]), 500)
        self.assertEqual(len(upsert_call_args[2].args[0]), 1)

    @patch("ingest.time.sleep")
    def test_retries_on_read_error(self, mock_sleep):
        client = MagicMock()
        # Fail once with ReadError, then succeed
        client.table.return_value.upsert.return_value.execute.side_effect = [
            httpx.ReadError("connection reset"),
            MagicMock(),
        ]

        _upsert(client, "laps", [{"id": 1}])

        self.assertEqual(
            client.table.return_value.upsert.return_value.execute.call_count, 2
        )

    @patch("ingest.time.sleep")
    def test_retries_on_connect_error(self, mock_sleep):
        client = MagicMock()
        client.table.return_value.upsert.return_value.execute.side_effect = [
            httpx.ConnectError("refused"),
            MagicMock(),
        ]

        _upsert(client, "laps", [{"id": 1}])

        self.assertEqual(
            client.table.return_value.upsert.return_value.execute.call_count, 2
        )

    @patch("ingest.time.sleep")
    def test_raises_after_four_failed_attempts(self, mock_sleep):
        client = MagicMock()
        client.table.return_value.upsert.return_value.execute.side_effect = (
            httpx.ReadError("dead")
        )

        with self.assertRaises(httpx.ReadError):
            _upsert(client, "laps", [{"id": 1}])

        self.assertEqual(
            client.table.return_value.upsert.return_value.execute.call_count, 4
        )

    @patch("ingest.time.sleep")
    def test_succeeds_on_second_attempt_after_one_failure(self, mock_sleep):
        client = MagicMock()
        success_mock = MagicMock()
        client.table.return_value.upsert.return_value.execute.side_effect = [
            httpx.ReadError("transient"),
            success_mock,
        ]

        _upsert(client, "laps", [{"id": 1}])

        # After recovery, no further exceptions
        self.assertEqual(
            client.table.return_value.upsert.return_value.execute.call_count, 2
        )

    @patch("ingest.time.sleep")
    def test_retry_sleep_uses_exponential_backoff(self, mock_sleep):
        client = MagicMock()
        client.table.return_value.upsert.return_value.execute.side_effect = [
            httpx.ReadError("err"),
            httpx.ReadError("err"),
            MagicMock(),
        ]

        _upsert(client, "laps", [{"id": 1}])

        sleep_args = [c.args[0] for c in mock_sleep.call_args_list]
        # First retry → 2**0 = 1, second → 2**1 = 2
        self.assertIn(1, sleep_args)
        self.assertIn(2, sleep_args)


# ===========================================================================
# ingest_laps
# ===========================================================================

class TestIngestLaps(unittest.TestCase):

    def _api_lap(self, **overrides):
        base = {
            "session_key": 9000,
            "driver_number": 44,
            "lap_number": 1,
            "lap_duration": 90.123,
            "duration_sector_1": 30.0,
            "duration_sector_2": 30.0,
            "duration_sector_3": 30.123,
            "i1_speed": 200,
            "i2_speed": 250,
            "st_speed": 310,
            "is_pit_out_lap": False,
            "date_start": "2024-03-01T14:00:00",
        }
        base.update(overrides)
        return base

    @patch("ingest.openf1.get_laps")
    def test_normal_row_mapped_correctly_with_all_fields(self, mock_get_laps):
        client = _mock_client()
        lap = self._api_lap()
        mock_get_laps.return_value = [lap]

        ingest_laps(client, 9000)

        upserted = client.table.return_value.upsert.call_args.args[0]
        self.assertEqual(len(upserted), 1)
        row = upserted[0]
        self.assertEqual(row["session_key"], 9000)
        self.assertEqual(row["driver_number"], 44)
        self.assertEqual(row["lap_number"], 1)
        self.assertEqual(row["lap_duration"], 90.123)
        self.assertEqual(row["duration_sector_1"], 30.0)
        self.assertEqual(row["duration_sector_2"], 30.0)
        self.assertEqual(row["duration_sector_3"], 30.123)
        self.assertEqual(row["i1_speed"], 200)
        self.assertEqual(row["i2_speed"], 250)
        self.assertEqual(row["st_speed"], 310)
        self.assertEqual(row["is_pit_out_lap"], False)
        self.assertEqual(row["date_start"], "2024-03-01T14:00:00")

    @patch("ingest.openf1.get_laps")
    def test_missing_session_key_filtered(self, mock_get_laps):
        client = _mock_client()
        mock_get_laps.return_value = [self._api_lap(session_key=None)]

        ingest_laps(client, 9000)

        client.table.assert_not_called()

    @patch("ingest.openf1.get_laps")
    def test_missing_driver_number_filtered(self, mock_get_laps):
        client = _mock_client()
        mock_get_laps.return_value = [self._api_lap(driver_number=None)]

        ingest_laps(client, 9000)

        client.table.assert_not_called()

    @patch("ingest.openf1.get_laps")
    def test_missing_lap_number_filtered(self, mock_get_laps):
        client = _mock_client()
        mock_get_laps.return_value = [self._api_lap(lap_number=None)]

        ingest_laps(client, 9000)

        client.table.assert_not_called()

    @patch("ingest.openf1.get_laps")
    def test_empty_api_response_produces_no_upsert(self, mock_get_laps):
        client = _mock_client()
        mock_get_laps.return_value = []

        ingest_laps(client, 9000)

        client.table.assert_not_called()

    @patch("ingest.openf1.get_laps")
    def test_mixed_valid_and_invalid_rows_only_valid_upserted(self, mock_get_laps):
        client = _mock_client()
        mock_get_laps.return_value = [
            self._api_lap(lap_number=1),
            self._api_lap(lap_number=None),   # filtered
            self._api_lap(driver_number=None), # filtered
            self._api_lap(lap_number=2),
        ]

        ingest_laps(client, 9000)

        upserted = client.table.return_value.upsert.call_args.args[0]
        self.assertEqual(len(upserted), 2)


# ===========================================================================
# ingest_race_control
# ===========================================================================

class TestIngestRaceControl(unittest.TestCase):

    def _api_rc(self, **overrides):
        base = {
            "session_key": 9000,
            "date": "2024-03-01T14:05:00",
            "lap_number": 3,
            "category": "Flag",
            "flag": "YELLOW",
            "message": "YELLOW FLAG IN SECTOR 1",
            "driver_number": None,
            "scope": "Sector",
            "sector": 1,
            "qualifying_phase": None,
        }
        base.update(overrides)
        return base

    @patch("ingest.openf1.get_race_control")
    def test_normal_row_passes_through(self, mock_get_rc):
        client = _mock_client()
        mock_get_rc.return_value = [self._api_rc()]

        ingest_race_control(client, 9000)

        upserted = client.table.return_value.upsert.call_args.args[0]
        self.assertEqual(len(upserted), 1)
        self.assertEqual(upserted[0]["flag"], "YELLOW")

    @patch("ingest.openf1.get_race_control")
    def test_null_category_filtered(self, mock_get_rc):
        client = _mock_client()
        mock_get_rc.return_value = [self._api_rc(category=None)]

        ingest_race_control(client, 9000)

        client.table.assert_not_called()

    @patch("ingest.openf1.get_race_control")
    def test_null_message_filtered(self, mock_get_rc):
        client = _mock_client()
        mock_get_rc.return_value = [self._api_rc(message=None)]

        ingest_race_control(client, 9000)

        client.table.assert_not_called()

    @patch("ingest.openf1.get_race_control")
    def test_null_session_key_filtered_before_category_check(self, mock_get_rc):
        client = _mock_client()
        mock_get_rc.return_value = [self._api_rc(session_key=None)]

        ingest_race_control(client, 9000)

        client.table.assert_not_called()

    @patch("ingest.openf1.get_race_control")
    def test_two_different_messages_at_same_timestamp_both_survive(self, mock_get_rc):
        """
        Key correctness test: two race-control events with identical timestamps but
        different messages (e.g. SC deployed + YELLOW flag) must both be written.
        This guards against a PK design that would deduplicate on (session_key, date)
        alone and accidentally drop one.
        """
        client = _mock_client()
        mock_get_rc.return_value = [
            self._api_rc(message="SAFETY CAR DEPLOYED", category="SafetyCar"),
            self._api_rc(message="YELLOW FLAG IN SECTOR 1", category="Flag"),
        ]

        ingest_race_control(client, 9000)

        upserted = client.table.return_value.upsert.call_args.args[0]
        self.assertEqual(len(upserted), 2)
        messages = {r["message"] for r in upserted}
        self.assertIn("SAFETY CAR DEPLOYED", messages)
        self.assertIn("YELLOW FLAG IN SECTOR 1", messages)

    @patch("ingest.openf1.get_race_control")
    def test_missing_date_filtered(self, mock_get_rc):
        client = _mock_client()
        mock_get_rc.return_value = [self._api_rc(date=None)]

        ingest_race_control(client, 9000)

        client.table.assert_not_called()


# ===========================================================================
# ingest_race_results
# ===========================================================================

class TestIngestRaceResults(unittest.TestCase):

    def _api_result(self, **overrides):
        base = {
            "session_key": 9000,
            "driver_number": 44,
            "position": 1,
            "points": 25,
            "gap_to_leader": 0.0,
            "duration": 5400.0,
            "number_of_laps": 57,
            "dnf": False,
            "dns": False,
            "dsq": False,
        }
        base.update(overrides)
        return base

    def _pit(self, driver_number):
        return {"driver_number": driver_number, "lap_number": 10, "pit_duration": 22.5}

    @patch("ingest.openf1.get_session_result")
    def test_pit_count_correctly_summed_per_driver(self, mock_get_result):
        client = _mock_client()
        mock_get_result.return_value = [
            self._api_result(driver_number=44),
            self._api_result(driver_number=33),
        ]
        pit_stops = [
            self._pit(44),
            self._pit(44),
            self._pit(44),
            self._pit(33),
        ]

        ingest_race_results(client, 9000, pit_stops)

        upserted = client.table.return_value.upsert.call_args.args[0]
        by_driver = {r["driver_number"]: r for r in upserted}
        self.assertEqual(by_driver[44]["pit_count"], 3)
        self.assertEqual(by_driver[33]["pit_count"], 1)

    @patch("ingest.openf1.get_session_result")
    def test_driver_with_no_pits_gets_zero(self, mock_get_result):
        client = _mock_client()
        mock_get_result.return_value = [self._api_result(driver_number=16)]
        ingest_race_results(client, 9000, [])

        upserted = client.table.return_value.upsert.call_args.args[0]
        self.assertEqual(upserted[0]["pit_count"], 0)

    @patch("ingest.openf1.get_session_result")
    def test_gap_to_leader_stringified(self, mock_get_result):
        client = _mock_client()
        mock_get_result.return_value = [self._api_result(driver_number=44, gap_to_leader=3.456)]

        ingest_race_results(client, 9000, [])

        upserted = client.table.return_value.upsert.call_args.args[0]
        self.assertEqual(upserted[0]["gap_to_leader"], "3.456")

    @patch("ingest.openf1.get_session_result")
    def test_gap_to_leader_none_stays_none(self, mock_get_result):
        client = _mock_client()
        mock_get_result.return_value = [self._api_result(driver_number=44, gap_to_leader=None)]

        ingest_race_results(client, 9000, [])

        upserted = client.table.return_value.upsert.call_args.args[0]
        self.assertIsNone(upserted[0]["gap_to_leader"])

    @patch("ingest.openf1.get_session_result")
    def test_dnf_dns_dsq_default_to_false_when_absent(self, mock_get_result):
        client = _mock_client()
        # Provide a result dict with no dnf/dns/dsq keys
        result = {
            "session_key": 9000,
            "driver_number": 44,
            "position": 5,
            "points": 10,
            "gap_to_leader": None,
            "duration": None,
            "number_of_laps": 57,
        }
        mock_get_result.return_value = [result]

        ingest_race_results(client, 9000, [])

        upserted = client.table.return_value.upsert.call_args.args[0]
        row = upserted[0]
        self.assertFalse(row["dnf"])
        self.assertFalse(row["dns"])
        self.assertFalse(row["dsq"])

    @patch("ingest.openf1.get_session_result")
    def test_rows_missing_driver_number_filtered(self, mock_get_result):
        client = _mock_client()
        mock_get_result.return_value = [self._api_result(driver_number=None)]

        ingest_race_results(client, 9000, [])

        client.table.assert_not_called()

    @patch("ingest.openf1.get_session_result")
    def test_dnf_true_when_present(self, mock_get_result):
        client = _mock_client()
        mock_get_result.return_value = [self._api_result(driver_number=44, dnf=True)]

        ingest_race_results(client, 9000, [])

        upserted = client.table.return_value.upsert.call_args.args[0]
        self.assertTrue(upserted[0]["dnf"])


# ===========================================================================
# ingest_fastest_lap_flag
# ===========================================================================

class TestIngestFastestLapFlag(unittest.TestCase):

    def _result(self, dn, dnf=False, dns=False, dsq=False):
        return {"driver_number": dn, "dnf": dnf, "dns": dns, "dsq": dsq}

    def _lap(self, dn, duration):
        return {"driver_number": dn, "lap_duration": duration}

    @patch("ingest.openf1.get_session_result")
    def test_correct_driver_gets_true(self, mock_result):
        mock_result.return_value = [self._result(44), self._result(1)]
        laps = [self._lap(44, 90.0), self._lap(1, 89.5)]  # driver 1 is faster
        client = _mock_client()
        ingest_fastest_lap_flag(client, 9000, laps)
        upserted = client.table.return_value.upsert.call_args.args[0]
        by_driver = {r["driver_number"]: r["fastest_lap_flag"] for r in upserted}
        self.assertTrue(by_driver[1])
        self.assertFalse(by_driver[44])

    @patch("ingest.openf1.get_session_result")
    def test_dnf_driver_excluded_from_consideration(self, mock_result):
        mock_result.return_value = [self._result(44), self._result(1, dnf=True)]
        # driver 1 has the fastest lap time but DNF → should not win
        laps = [self._lap(44, 90.0), self._lap(1, 85.0)]
        client = _mock_client()
        ingest_fastest_lap_flag(client, 9000, laps)
        upserted = client.table.return_value.upsert.call_args.args[0]
        # only driver 44 is classified, so they get the flag
        self.assertEqual(len(upserted), 1)
        self.assertEqual(upserted[0]["driver_number"], 44)
        self.assertTrue(upserted[0]["fastest_lap_flag"])

    @patch("ingest.openf1.get_session_result")
    def test_dns_driver_excluded(self, mock_result):
        mock_result.return_value = [self._result(44), self._result(63, dns=True)]
        laps = [self._lap(44, 90.0), self._lap(63, 88.0)]
        client = _mock_client()
        ingest_fastest_lap_flag(client, 9000, laps)
        upserted = client.table.return_value.upsert.call_args.args[0]
        self.assertEqual(len(upserted), 1)
        self.assertTrue(upserted[0]["fastest_lap_flag"])

    @patch("ingest.openf1.get_session_result")
    def test_dsq_driver_excluded(self, mock_result):
        mock_result.return_value = [self._result(44), self._result(16, dsq=True)]
        laps = [self._lap(44, 90.0), self._lap(16, 87.0)]
        client = _mock_client()
        ingest_fastest_lap_flag(client, 9000, laps)
        upserted = client.table.return_value.upsert.call_args.args[0]
        self.assertEqual(len(upserted), 1)
        self.assertTrue(upserted[0]["fastest_lap_flag"])

    @patch("ingest.openf1.get_session_result")
    def test_invalid_lap_durations_excluded(self, mock_result):
        mock_result.return_value = [self._result(44), self._result(1)]
        laps = [
            self._lap(44, "INVALID"),
            self._lap(44, 0.0),
            self._lap(44, None),
            self._lap(44, 91.0),   # only valid lap for driver 44
            self._lap(1, "bad"),   # driver 1 has no valid laps
        ]
        client = _mock_client()
        ingest_fastest_lap_flag(client, 9000, laps)
        upserted = client.table.return_value.upsert.call_args.args[0]
        by_driver = {r["driver_number"]: r["fastest_lap_flag"] for r in upserted}
        self.assertTrue(by_driver[44])
        self.assertFalse(by_driver[1])

    @patch("ingest.openf1.get_session_result")
    def test_empty_results_produces_no_upsert(self, mock_result):
        mock_result.return_value = []
        client = _mock_client()
        ingest_fastest_lap_flag(client, 9000, [self._lap(44, 90.0)])
        client.table.assert_not_called()

    @patch("ingest.openf1.get_session_result")
    def test_all_classified_drivers_receive_a_flag_row(self, mock_result):
        mock_result.return_value = [self._result(44), self._result(1), self._result(63)]
        laps = [self._lap(44, 91.0), self._lap(1, 90.0), self._lap(63, 92.0)]
        client = _mock_client()
        ingest_fastest_lap_flag(client, 9000, laps)
        upserted = client.table.return_value.upsert.call_args.args[0]
        self.assertEqual(len(upserted), 3)
        flags = {r["driver_number"]: r["fastest_lap_flag"] for r in upserted}
        self.assertEqual(sum(flags.values()), 1)   # exactly one True
        self.assertTrue(flags[1])                  # driver 1 has shortest lap

    @patch("ingest.openf1.get_session_result")
    def test_no_valid_laps_produces_no_upsert(self, mock_result):
        mock_result.return_value = [self._result(44)]
        client = _mock_client()
        ingest_fastest_lap_flag(client, 9000, [])   # no laps at all
        client.table.assert_not_called()


# ===========================================================================
# _assign_qualifying_phases
# ===========================================================================

class TestAssignQualifyingPhases(unittest.TestCase):

    def _rc(self, qualifying_phase, date):
        return {"qualifying_phase": qualifying_phase, "date": date}

    def _lap(self, date_start):
        return {"date_start": date_start, "driver_number": 44, "lap_number": 1}

    def test_lap_after_q1_event_gets_q1(self):
        laps = [self._lap("2024-03-01T10:00:00")]
        rc = [self._rc("Q1", "2024-03-01T09:00:00")]
        result = _assign_qualifying_phases(laps, rc)
        self.assertEqual(result[0]["_phase"], "Q1")

    def test_lap_after_q2_event_gets_q2(self):
        laps = [self._lap("2024-03-01T12:00:00")]
        rc = [
            self._rc("Q1", "2024-03-01T09:00:00"),
            self._rc("Q2", "2024-03-01T11:00:00"),
        ]
        result = _assign_qualifying_phases(laps, rc)
        self.assertEqual(result[0]["_phase"], "Q2")

    def test_lap_before_any_event_gets_none(self):
        laps = [self._lap("2024-03-01T08:00:00")]
        rc = [self._rc("Q1", "2024-03-01T09:00:00")]
        result = _assign_qualifying_phases(laps, rc)
        self.assertIsNone(result[0]["_phase"])

    def test_lap_exactly_at_event_date_gets_that_phase(self):
        laps = [self._lap("2024-03-01T09:00:00")]
        rc = [self._rc("Q1", "2024-03-01T09:00:00")]
        result = _assign_qualifying_phases(laps, rc)
        self.assertEqual(result[0]["_phase"], "Q1")

    def test_empty_race_control_all_laps_get_none(self):
        laps = [self._lap("2024-03-01T10:00:00"), self._lap("2024-03-01T11:00:00")]
        result = _assign_qualifying_phases(laps, [])
        for lap in result:
            self.assertIsNone(lap["_phase"])

    def test_lap_with_no_date_start_gets_none(self):
        laps = [{"date_start": None, "driver_number": 44, "lap_number": 1}]
        rc = [self._rc("Q1", "2024-03-01T09:00:00")]
        result = _assign_qualifying_phases(laps, rc)
        self.assertIsNone(result[0]["_phase"])

    def test_full_q1_q2_q3_scenario(self):
        laps = [
            {"date_start": "2024-03-01T10:00:00", "driver_number": 44, "lap_number": 1},
            {"date_start": "2024-03-01T12:00:00", "driver_number": 44, "lap_number": 2},
            {"date_start": "2024-03-01T14:00:00", "driver_number": 44, "lap_number": 3},
        ]
        rc = [
            self._rc("Q1", "2024-03-01T09:00:00"),
            self._rc("Q2", "2024-03-01T11:00:00"),
            self._rc("Q3", "2024-03-01T13:00:00"),
        ]
        result = _assign_qualifying_phases(laps, rc)
        self.assertEqual(result[0]["_phase"], "Q1")
        self.assertEqual(result[1]["_phase"], "Q2")
        self.assertEqual(result[2]["_phase"], "Q3")

    def test_rc_rows_with_null_qualifying_phase_ignored(self):
        laps = [self._lap("2024-03-01T10:00:00")]
        rc = [
            {"qualifying_phase": None, "date": "2024-03-01T09:00:00"},
            self._rc("Q1", "2024-03-01T09:30:00"),
        ]
        result = _assign_qualifying_phases(laps, rc)
        self.assertEqual(result[0]["_phase"], "Q1")

    def test_unsorted_rc_rows_produce_correct_assignment(self):
        laps = [self._lap("2024-03-01T12:00:00")]
        rc = [
            self._rc("Q2", "2024-03-01T11:00:00"),  # out of order
            self._rc("Q1", "2024-03-01T09:00:00"),
        ]
        result = _assign_qualifying_phases(laps, rc)
        self.assertEqual(result[0]["_phase"], "Q2")

    def test_integer_qualifying_phase_normalized_to_q_string(self):
        """OpenF1 returns qualifying_phase as integer 1/2/3 in many sessions."""
        laps = [
            self._lap("2024-03-01T10:00:00"),
            self._lap("2024-03-01T12:00:00"),
            self._lap("2024-03-01T14:00:00"),
        ]
        rc = [
            {"qualifying_phase": 1, "date": "2024-03-01T09:00:00"},
            {"qualifying_phase": 2, "date": "2024-03-01T11:00:00"},
            {"qualifying_phase": 3, "date": "2024-03-01T13:00:00"},
        ]
        result = _assign_qualifying_phases(laps, rc)
        self.assertEqual(result[0]["_phase"], "Q1")
        self.assertEqual(result[1]["_phase"], "Q2")
        self.assertEqual(result[2]["_phase"], "Q3")


# ===========================================================================
# _get_compound_for_lap
# ===========================================================================

class TestGetCompoundForLap(unittest.TestCase):

    def _stint(self, driver_number, lap_start, lap_end, compound):
        return {
            "driver_number": driver_number,
            "lap_start": lap_start,
            "lap_end": lap_end,
            "compound": compound,
        }

    def test_lap_within_range_returns_compound(self):
        stints = [self._stint(44, 1, 5, "SOFT")]
        self.assertEqual(_get_compound_for_lap(44, 3, stints), "SOFT")

    def test_wrong_driver_returns_none(self):
        stints = [self._stint(44, 1, 5, "SOFT")]
        self.assertIsNone(_get_compound_for_lap(33, 3, stints))

    def test_lap_outside_all_ranges_returns_none(self):
        stints = [self._stint(44, 1, 3, "SOFT")]
        self.assertIsNone(_get_compound_for_lap(44, 5, stints))

    def test_open_ended_stint_matches_any_lap_gte_start(self):
        stints = [self._stint(44, 3, None, "MEDIUM")]
        self.assertEqual(_get_compound_for_lap(44, 10, stints), "MEDIUM")

    def test_compound_returned_uppercased(self):
        stints = [self._stint(44, 1, 5, "soft")]
        self.assertEqual(_get_compound_for_lap(44, 3, stints), "SOFT")

    def test_none_compound_returns_unknown(self):
        stints = [self._stint(44, 1, 5, None)]
        self.assertEqual(_get_compound_for_lap(44, 3, stints), "UNKNOWN")


# ===========================================================================
# ingest_qualifying_results
# ===========================================================================

class TestIngestQualifyingResults(unittest.TestCase):

    def _lap(self, driver_number, lap_number, lap_duration,
             date_start="2024-03-01T10:00:00"):
        return {
            "session_key": 9000,
            "driver_number": driver_number,
            "lap_number": lap_number,
            "lap_duration": lap_duration,
            "date_start": date_start,
        }

    def _rc(self, qualifying_phase, date):
        return {"qualifying_phase": qualifying_phase, "date": date, "session_key": 9000}

    def _stint(self, driver_number, lap_start, lap_end, compound):
        return {
            "driver_number": driver_number,
            "lap_start": lap_start,
            "lap_end": lap_end,
            "compound": compound,
        }

    def test_fastest_lap_selected_when_driver_has_multiple(self):
        client = _mock_client()
        laps = [
            self._lap(44, 1, 90.0),
            self._lap(44, 2, 88.5),  # fastest
            self._lap(44, 3, 89.0),
        ]
        rc = [self._rc("Q1", "2024-03-01T09:00:00")]

        ingest_qualifying_results(client, 9000, laps, rc, [])

        upserted = client.table.return_value.upsert.call_args.args[0]
        self.assertEqual(len(upserted), 1)
        self.assertAlmostEqual(upserted[0]["best_lap_time"], 88.5)
        self.assertEqual(upserted[0]["best_lap_number"], 2)

    def test_zero_duration_excluded(self):
        client = _mock_client()
        laps = [self._lap(44, 1, 0)]

        ingest_qualifying_results(client, 9000, laps)

        client.table.assert_not_called()

    def test_none_duration_excluded(self):
        client = _mock_client()
        laps = [self._lap(44, 1, None)]

        ingest_qualifying_results(client, 9000, laps)

        client.table.assert_not_called()

    def test_non_numeric_duration_excluded(self):
        client = _mock_client()
        laps = [self._lap(44, 1, "INVALID")]

        ingest_qualifying_results(client, 9000, laps)

        client.table.assert_not_called()

    def test_negative_duration_excluded(self):
        client = _mock_client()
        laps = [self._lap(44, 1, -1.0)]

        ingest_qualifying_results(client, 9000, laps)

        client.table.assert_not_called()

    def test_empty_laps_list_produces_no_rows(self):
        client = _mock_client()

        ingest_qualifying_results(client, 9000, [])

        client.table.assert_not_called()

    def test_multiple_drivers_each_get_own_best(self):
        client = _mock_client()
        laps = [
            self._lap(44, 1, 90.0),
            self._lap(44, 2, 88.5),
            self._lap(33, 1, 89.0),
            self._lap(33, 2, 91.0),
        ]
        rc = [self._rc("Q1", "2024-03-01T09:00:00")]

        ingest_qualifying_results(client, 9000, laps, rc, [])

        upserted = client.table.return_value.upsert.call_args.args[0]
        by_driver = {r["driver_number"]: r for r in upserted}
        self.assertAlmostEqual(by_driver[44]["best_lap_time"], 88.5)
        self.assertAlmostEqual(by_driver[33]["best_lap_time"], 89.0)

    def test_q1_q2_q3_times_computed_correctly(self):
        client = _mock_client()
        laps = [
            self._lap(44, 1, 90.0, "2024-03-01T10:00:00"),  # Q1
            self._lap(44, 2, 88.0, "2024-03-01T12:00:00"),  # Q2
            self._lap(44, 3, 86.0, "2024-03-01T14:00:00"),  # Q3
        ]
        rc = [
            self._rc("Q1", "2024-03-01T09:00:00"),
            self._rc("Q2", "2024-03-01T11:00:00"),
            self._rc("Q3", "2024-03-01T13:00:00"),
        ]

        ingest_qualifying_results(client, 9000, laps, rc, [])

        upserted = client.table.return_value.upsert.call_args.args[0]
        row = upserted[0]
        self.assertAlmostEqual(row["q1_time"], 90.0)
        self.assertAlmostEqual(row["q2_time"], 88.0)
        self.assertAlmostEqual(row["q3_time"], 86.0)

    def test_driver_eliminated_in_q1_has_null_q2_q3(self):
        client = _mock_client()
        laps = [self._lap(44, 1, 90.0, "2024-03-01T10:00:00")]
        rc = [
            self._rc("Q1", "2024-03-01T09:00:00"),
            self._rc("Q2", "2024-03-01T11:00:00"),
        ]

        ingest_qualifying_results(client, 9000, laps, rc, [])

        upserted = client.table.return_value.upsert.call_args.args[0]
        row = upserted[0]
        self.assertAlmostEqual(row["q1_time"], 90.0)
        self.assertIsNone(row["q2_time"])
        self.assertIsNone(row["q3_time"])

    def test_compound_attached_correctly_to_phase_best_lap(self):
        client = _mock_client()
        laps = [self._lap(44, 2, 90.0, "2024-03-01T10:00:00")]
        rc = [self._rc("Q1", "2024-03-01T09:00:00")]
        stints = [self._stint(44, 1, 5, "SOFT")]

        ingest_qualifying_results(client, 9000, laps, rc, stints)

        upserted = client.table.return_value.upsert.call_args.args[0]
        self.assertEqual(upserted[0]["q1_compound"], "SOFT")

    def test_lap_counts_per_phase_stored_correctly(self):
        client = _mock_client()
        laps = [
            self._lap(44, 1, 90.0, "2024-03-01T10:00:00"),  # Q1
            self._lap(44, 2, 89.0, "2024-03-01T10:30:00"),  # Q1
            self._lap(44, 3, 88.0, "2024-03-01T12:00:00"),  # Q2
        ]
        rc = [
            self._rc("Q1", "2024-03-01T09:00:00"),
            self._rc("Q2", "2024-03-01T11:00:00"),
        ]

        ingest_qualifying_results(client, 9000, laps, rc, [])

        upserted = client.table.return_value.upsert.call_args.args[0]
        row = upserted[0]
        self.assertEqual(row["q1_laps"], 2)
        self.assertEqual(row["q2_laps"], 1)

    def test_unphased_laps_excluded_from_phase_times(self):
        client = _mock_client()
        laps = [
            # Precedes all events — should be excluded
            self._lap(44, 1, 50.0, "2024-03-01T08:00:00"),
            self._lap(44, 2, 90.0, "2024-03-01T10:00:00"),  # Q1
        ]
        rc = [self._rc("Q1", "2024-03-01T09:00:00")]

        ingest_qualifying_results(client, 9000, laps, rc, [])

        upserted = client.table.return_value.upsert.call_args.args[0]
        row = upserted[0]
        self.assertAlmostEqual(row["q1_time"], 90.0)
        # The 50-second unphased lap must not influence the overall best
        self.assertAlmostEqual(row["best_lap_time"], 90.0)

    def test_overall_best_is_min_across_all_phases(self):
        client = _mock_client()
        laps = [
            self._lap(44, 1, 90.0, "2024-03-01T10:00:00"),  # Q1
            self._lap(44, 2, 88.0, "2024-03-01T12:00:00"),  # Q2
            self._lap(44, 3, 86.0, "2024-03-01T14:00:00"),  # Q3 — fastest
        ]
        rc = [
            self._rc("Q1", "2024-03-01T09:00:00"),
            self._rc("Q2", "2024-03-01T11:00:00"),
            self._rc("Q3", "2024-03-01T13:00:00"),
        ]

        ingest_qualifying_results(client, 9000, laps, rc, [])

        upserted = client.table.return_value.upsert.call_args.args[0]
        row = upserted[0]
        self.assertAlmostEqual(row["best_lap_time"], 86.0)
        self.assertEqual(row["best_lap_number"], 3)

    def test_empty_stints_compound_fields_all_none(self):
        client = _mock_client()
        laps = [self._lap(44, 1, 90.0, "2024-03-01T10:00:00")]
        rc = [self._rc("Q1", "2024-03-01T09:00:00")]

        ingest_qualifying_results(client, 9000, laps, rc, [])

        upserted = client.table.return_value.upsert.call_args.args[0]
        row = upserted[0]
        self.assertIsNone(row["q1_compound"])
        self.assertIsNone(row["q2_compound"])
        self.assertIsNone(row["q3_compound"])

    def test_driver_with_only_null_duration_laps_has_none_time_but_count(self):
        client = _mock_client()
        laps = [
            self._lap(44, 1, None, "2024-03-01T10:00:00"),
            self._lap(44, 2, None, "2024-03-01T10:30:00"),
        ]
        rc = [self._rc("Q1", "2024-03-01T09:00:00")]

        ingest_qualifying_results(client, 9000, laps, rc, [])

        upserted = client.table.return_value.upsert.call_args.args[0]
        row = upserted[0]
        self.assertIsNone(row["q1_time"])
        self.assertEqual(row["q1_laps"], 2)


# ===========================================================================
# ingest_overtakes
# ===========================================================================

class TestIngestOvertakes(unittest.TestCase):

    @patch("ingest.openf1.get_overtakes")
    def test_returns_raw_api_rows(self, mock_get_overtakes):
        """ingest_overtakes returns the raw API list for downstream use."""
        client = _mock_client()
        raw = [{"session_key": 9000, "date": "2024-03-01T14:00:00",
                "overtaking_driver_number": 44, "overtaken_driver_number": 1,
                "position": 3}]
        mock_get_overtakes.return_value = raw

        result = ingest_overtakes(client, 9000)

        self.assertIs(result, raw)

    @patch("ingest.openf1.get_overtakes")
    def test_empty_response_returns_empty_list(self, mock_get_overtakes):
        """Empty API response returns empty list."""
        client = _mock_client()
        mock_get_overtakes.return_value = []

        result = ingest_overtakes(client, 9000)

        self.assertEqual(result, [])


# ===========================================================================
# ingest_intervals
# ===========================================================================

class TestIngestIntervals(unittest.TestCase):

    def _interval(self, **overrides):
        base = {
            "session_key": 9000,
            "driver_number": 44,
            "date": "2024-03-01T14:30:00",
            "gap_to_leader": 0.0,
            "interval": 1.5,
        }
        base.update(overrides)
        return base

    @patch("ingest.openf1.get_intervals")
    def test_normal_numeric_gap_stored_correctly(self, mock_get_intervals):
        client = _mock_client()
        mock_get_intervals.return_value = [self._interval(gap_to_leader=5.678, interval=1.234)]

        ingest_intervals(client, 9000)

        upserted = client.table.return_value.upsert.call_args.args[0]
        self.assertEqual(len(upserted), 1)
        self.assertAlmostEqual(upserted[0]["gap_to_leader"], 5.678)
        self.assertAlmostEqual(upserted[0]["interval"], 1.234)
        self.assertIsNone(upserted[0]["laps_down"])

    @patch("ingest.openf1.get_intervals")
    def test_plus_one_lap_produces_laps_down_one_and_null_gap(self, mock_get_intervals):
        client = _mock_client()
        mock_get_intervals.return_value = [self._interval(gap_to_leader="+1 LAP")]

        ingest_intervals(client, 9000)

        upserted = client.table.return_value.upsert.call_args.args[0]
        self.assertIsNone(upserted[0]["gap_to_leader"])
        self.assertEqual(upserted[0]["laps_down"], 1)

    @patch("ingest.openf1.get_intervals")
    def test_leader_none_gap_produces_null_gap_and_null_laps_down(self, mock_get_intervals):
        client = _mock_client()
        mock_get_intervals.return_value = [self._interval(gap_to_leader=None)]

        ingest_intervals(client, 9000)

        upserted = client.table.return_value.upsert.call_args.args[0]
        self.assertIsNone(upserted[0]["gap_to_leader"])
        self.assertIsNone(upserted[0]["laps_down"])

    @patch("ingest.openf1.get_intervals")
    def test_missing_session_key_filtered(self, mock_get_intervals):
        client = _mock_client()
        mock_get_intervals.return_value = [self._interval(session_key=None)]

        ingest_intervals(client, 9000)

        client.table.assert_not_called()

    @patch("ingest.openf1.get_intervals")
    def test_missing_driver_number_filtered(self, mock_get_intervals):
        client = _mock_client()
        mock_get_intervals.return_value = [self._interval(driver_number=None)]

        ingest_intervals(client, 9000)

        client.table.assert_not_called()

    @patch("ingest.openf1.get_intervals")
    def test_missing_date_filtered(self, mock_get_intervals):
        client = _mock_client()
        mock_get_intervals.return_value = [self._interval(date=None)]

        ingest_intervals(client, 9000)

        client.table.assert_not_called()

    @patch("ingest.openf1.get_intervals")
    def test_interval_field_also_parsed_via_parse_gap(self, mock_get_intervals):
        """The interval column is also processed through _parse_gap."""
        client = _mock_client()
        mock_get_intervals.return_value = [self._interval(interval="+1 LAP")]

        ingest_intervals(client, 9000)

        upserted = client.table.return_value.upsert.call_args.args[0]
        # _parse_gap("+1 LAP") returns (None, 1); the interval field takes the float part
        self.assertIsNone(upserted[0]["interval"])

    @patch("ingest.openf1.get_intervals")
    def test_returns_raw_api_rows(self, mock_get_intervals):
        """ingest_intervals returns the raw API list for downstream use."""
        client = _mock_client()
        raw = [self._interval(gap_to_leader=1.0, interval=0.5)]
        mock_get_intervals.return_value = raw

        result = ingest_intervals(client, 9000)

        self.assertIs(result, raw)


# ===========================================================================
# ingest_starting_grid
# ===========================================================================

class TestIngestStartingGrid(unittest.TestCase):

    def _grid_entry(self, **overrides):
        base = {
            "session_key": 9000,
            "driver_number": 44,
            "position": 1,
            "lap_duration": 88.5,
        }
        base.update(overrides)
        return base

    @patch("ingest.openf1.get_starting_grid")
    def test_lap_duration_stored(self, mock_get_grid):
        client = _mock_client()
        mock_get_grid.return_value = [self._grid_entry(lap_duration=88.5)]

        ingest_starting_grid(client, 9000)

        upserted = client.table.return_value.upsert.call_args.args[0]
        self.assertEqual(upserted[0]["lap_duration"], 88.5)

    @patch("ingest.openf1.get_starting_grid")
    def test_none_lap_duration_stored_as_none(self, mock_get_grid):
        """None lap_duration should be stored, not filtered out."""
        client = _mock_client()
        mock_get_grid.return_value = [self._grid_entry(lap_duration=None)]

        ingest_starting_grid(client, 9000)

        upserted = client.table.return_value.upsert.call_args.args[0]
        self.assertEqual(len(upserted), 1)
        self.assertIsNone(upserted[0]["lap_duration"])

    @patch("ingest.openf1.get_starting_grid")
    def test_missing_driver_number_filtered(self, mock_get_grid):
        client = _mock_client()
        mock_get_grid.return_value = [self._grid_entry(driver_number=None)]

        ingest_starting_grid(client, 9000)

        client.table.assert_not_called()

    @patch("ingest.openf1.get_starting_grid")
    def test_missing_session_key_filtered(self, mock_get_grid):
        client = _mock_client()
        mock_get_grid.return_value = [self._grid_entry(session_key=None)]

        ingest_starting_grid(client, 9000)

        client.table.assert_not_called()

    @patch("ingest.openf1.get_starting_grid")
    def test_position_stored(self, mock_get_grid):
        client = _mock_client()
        mock_get_grid.return_value = [self._grid_entry(position=3)]

        ingest_starting_grid(client, 9000)

        upserted = client.table.return_value.upsert.call_args.args[0]
        self.assertEqual(upserted[0]["position"], 3)


# ===========================================================================
# ingest_weather
# ===========================================================================

class TestIngestWeather(unittest.TestCase):

    def _row(self, **overrides):
        base = {
            "session_key":       9000,
            "date":              "2024-03-02T13:00:00Z",
            "track_temperature": 45.2,
            "air_temperature":   28.1,
            "humidity":          42.0,
            "pressure":          1012.3,
            "rainfall":          False,
            "wind_direction":    180,
            "wind_speed":        3.5,
        }
        base.update(overrides)
        return base

    @patch("ingest.openf1.get_weather")
    def test_all_fields_stored(self, mock_get_weather):
        mock_get_weather.return_value = [self._row()]
        client = _mock_client()
        ingest_weather(client, 9000)
        upserted = client.table.return_value.upsert.call_args.args[0]
        row = upserted[0]
        self.assertEqual(row["wind_speed"], 3.5)
        self.assertEqual(row["wind_direction"], 180)
        self.assertAlmostEqual(row["pressure"], 1012.3)
        self.assertAlmostEqual(row["track_temperature"], 45.2)
        self.assertAlmostEqual(row["air_temperature"], 28.1)
        self.assertEqual(row["rainfall"], False)

    @patch("ingest.openf1.get_weather")
    def test_missing_wind_stored_as_none(self, mock_get_weather):
        """wind_speed/wind_direction may be absent from some OpenF1 responses."""
        mock_get_weather.return_value = [
            self._row(wind_speed=None, wind_direction=None, pressure=None)
        ]
        client = _mock_client()
        ingest_weather(client, 9000)
        upserted = client.table.return_value.upsert.call_args.args[0]
        self.assertIsNone(upserted[0]["wind_speed"])
        self.assertIsNone(upserted[0]["wind_direction"])
        self.assertIsNone(upserted[0]["pressure"])

    @patch("ingest.openf1.get_weather")
    def test_missing_session_key_filtered(self, mock_get_weather):
        mock_get_weather.return_value = [self._row(session_key=None)]
        client = _mock_client()
        ingest_weather(client, 9000)
        client.table.assert_not_called()

    @patch("ingest.openf1.get_weather")
    def test_missing_date_filtered(self, mock_get_weather):
        mock_get_weather.return_value = [self._row(date=None)]
        client = _mock_client()
        ingest_weather(client, 9000)
        client.table.assert_not_called()

    @patch("ingest.openf1.get_weather")
    def test_empty_response_no_upsert(self, mock_get_weather):
        mock_get_weather.return_value = []
        client = _mock_client()
        ingest_weather(client, 9000)
        client.table.assert_not_called()

    @patch("ingest.openf1.get_weather")
    def test_upserts_to_weather_table(self, mock_get_weather):
        mock_get_weather.return_value = [self._row()]
        client = _mock_client()
        ingest_weather(client, 9000)
        client.table.assert_called_with("weather")


# ===========================================================================
# process_session — integration tests
# ===========================================================================

RACE_SESSION = {
    "session_key": 9000,
    "meeting_key": 1234,
    "session_name": "Race",
    "session_type": "Race",
    "date_start": "2024-03-01T14:00:00",
    "date_end": "2024-03-01T16:00:00",
    "year": 2024,
}

QUALIFYING_SESSION = {
    "session_key": 9001,
    "meeting_key": 1234,
    "session_name": "Qualifying",
    "session_type": "Qualifying",
    "date_start": "2024-03-01T12:00:00",
    "date_end": "2024-03-01T13:00:00",
    "year": 2024,
}

SPRINT_SESSION = {
    "session_key": 9002,
    "meeting_key": 1234,
    "session_name": "Sprint",
    "session_type": "Sprint",
    "date_start": "2024-03-02T14:00:00",
    "date_end": "2024-03-02T15:00:00",
    "year": 2024,
}

SPRINT_QUALIFYING_SESSION = {
    "session_key": 9003,
    "meeting_key": 1234,
    "session_name": "Sprint Qualifying",
    "session_type": "Sprint Qualifying",
    "date_start": "2024-03-02T12:00:00",
    "date_end": "2024-03-02T13:00:00",
    "year": 2024,
}

SPRINT_SHOOTOUT_SESSION = {
    "session_key": 9004,
    "meeting_key": 1234,
    "session_name": "Sprint Shootout",
    "session_type": "Sprint Shootout",
    "date_start": "2024-03-02T10:00:00",
    "date_end": "2024-03-02T11:00:00",
    "year": 2024,
}


def _patch_all_ingest_functions(test_fn):
    """Decorator that patches every ingest_* function and openf1 helpers for process_session."""
    patches = [
        patch("ingest.openf1.get_session"),
        patch("ingest.openf1.reset_stats"),
        patch("ingest.ingest_meeting"),
        patch("ingest.ingest_session"),
        patch("ingest.ingest_drivers"),
        patch("ingest.ingest_laps"),
        patch("ingest.ingest_stints"),
        patch("ingest.ingest_pit_stops"),
        patch("ingest.ingest_weather"),
        patch("ingest.ingest_race_control"),
        patch("ingest.ingest_race_results"),
        patch("ingest.ingest_overtakes"),
        patch("ingest.ingest_intervals"),
        patch("ingest.ingest_championship_drivers"),
        patch("ingest.ingest_championship_teams"),
        patch("ingest.ingest_qualifying_results"),
        patch("ingest.ingest_starting_grid"),
    ]

    def wrapper(self):
        mocks = {}
        active = []
        for p in patches:
            m = p.start()
            active.append(p)
            # Extract the last part of the target name for a human-friendly key
            name = p.attribute
            mocks[name] = m
        try:
            test_fn(self, mocks)
        finally:
            for p in active:
                p.stop()

    wrapper.__name__ = test_fn.__name__
    return wrapper


class TestProcessSessionRace(unittest.TestCase):
    """process_session with a Race session type."""

    @patch("ingest.openf1.reset_stats")
    @patch("ingest.openf1.get_session", return_value=[RACE_SESSION])
    @patch("ingest.ingest_meeting")
    @patch("ingest.ingest_session", return_value=RACE_SESSION)
    @patch("ingest.ingest_drivers")
    @patch("ingest.ingest_laps", return_value=[])
    @patch("ingest.ingest_stints")
    @patch("ingest.ingest_pit_stops", return_value=[])
    @patch("ingest.ingest_weather")
    @patch("ingest.ingest_race_control")
    @patch("ingest.ingest_race_results")
    @patch("ingest.ingest_overtakes")
    @patch("ingest.ingest_intervals")
    @patch("ingest.ingest_championship_drivers")
    @patch("ingest.ingest_championship_teams")
    @patch("ingest.ingest_qualifying_results")
    @patch("ingest.ingest_starting_grid")
    @patch("ingest.ingest_fastest_lap_flag")
    def test_race_calls_race_specific_functions(
        self, mock_fastest_lap_flag, mock_starting_grid, mock_qual_results,
        mock_champ_teams, mock_champ_drivers, mock_intervals, mock_overtakes,
        mock_race_results, mock_race_control, mock_weather, mock_pit_stops,
        mock_stints, mock_laps, mock_drivers, mock_session, mock_meeting,
        mock_get_session, mock_reset_stats,
    ):
        client = _mock_client()

        process_session(client, 9000)

        mock_race_results.assert_called_once()
        mock_fastest_lap_flag.assert_called_once()
        mock_overtakes.assert_called_once()
        mock_intervals.assert_called_once()
        mock_champ_drivers.assert_called_once()
        mock_champ_teams.assert_called_once()

    @patch("ingest.openf1.reset_stats")
    @patch("ingest.openf1.get_session", return_value=[RACE_SESSION])
    @patch("ingest.ingest_meeting")
    @patch("ingest.ingest_session", return_value=RACE_SESSION)
    @patch("ingest.ingest_drivers")
    @patch("ingest.ingest_laps", return_value=[])
    @patch("ingest.ingest_stints")
    @patch("ingest.ingest_pit_stops", return_value=[])
    @patch("ingest.ingest_weather")
    @patch("ingest.ingest_race_control")
    @patch("ingest.ingest_race_results")
    @patch("ingest.ingest_overtakes")
    @patch("ingest.ingest_intervals")
    @patch("ingest.ingest_championship_drivers")
    @patch("ingest.ingest_championship_teams")
    @patch("ingest.ingest_qualifying_results")
    @patch("ingest.ingest_starting_grid")
    @patch("ingest.ingest_fastest_lap_flag")
    def test_race_does_not_call_qualifying_functions(
        self, mock_fastest_lap_flag, mock_starting_grid, mock_qual_results,
        mock_champ_teams, mock_champ_drivers, mock_intervals, mock_overtakes,
        mock_race_results, mock_race_control, mock_weather, mock_pit_stops,
        mock_stints, mock_laps, mock_drivers, mock_session, mock_meeting,
        mock_get_session, mock_reset_stats,
    ):
        client = _mock_client()

        process_session(client, 9000)

        mock_qual_results.assert_not_called()
        mock_starting_grid.assert_not_called()

    @patch("ingest.openf1.reset_stats")
    @patch("ingest.openf1.get_session", return_value=[RACE_SESSION])
    @patch("ingest.ingest_meeting")
    @patch("ingest.ingest_session", return_value=RACE_SESSION)
    @patch("ingest.ingest_drivers")
    @patch("ingest.ingest_laps", return_value=[])
    @patch("ingest.ingest_stints")
    @patch("ingest.ingest_pit_stops", return_value=[])
    @patch("ingest.ingest_weather")
    @patch("ingest.ingest_race_control")
    @patch("ingest.ingest_race_results")
    @patch("ingest.ingest_overtakes")
    @patch("ingest.ingest_intervals")
    @patch("ingest.ingest_championship_drivers")
    @patch("ingest.ingest_championship_teams")
    @patch("ingest.ingest_qualifying_results")
    @patch("ingest.ingest_starting_grid")
    @patch("ingest.ingest_fastest_lap_flag")
    def test_race_calls_common_functions(
        self, mock_fastest_lap_flag, mock_starting_grid, mock_qual_results,
        mock_champ_teams, mock_champ_drivers, mock_intervals, mock_overtakes,
        mock_race_results, mock_race_control, mock_weather, mock_pit_stops,
        mock_stints, mock_laps, mock_drivers, mock_session, mock_meeting,
        mock_get_session, mock_reset_stats,
    ):
        client = _mock_client()

        process_session(client, 9000)

        mock_drivers.assert_called_once_with(client, 9000)
        mock_laps.assert_called_once_with(client, 9000)
        mock_stints.assert_called_once_with(client, 9000)
        mock_pit_stops.assert_called_once_with(client, 9000)
        mock_weather.assert_called_once_with(client, 9000)
        mock_race_control.assert_called_once_with(client, 9000)


class TestProcessSessionQualifying(unittest.TestCase):
    """process_session with a Qualifying session type."""

    @patch("ingest.openf1.reset_stats")
    @patch("ingest.openf1.get_session", return_value=[QUALIFYING_SESSION])
    @patch("ingest.ingest_meeting")
    @patch("ingest.ingest_session", return_value=QUALIFYING_SESSION)
    @patch("ingest.ingest_drivers")
    @patch("ingest.ingest_laps", return_value=[])
    @patch("ingest.ingest_stints")
    @patch("ingest.ingest_pit_stops", return_value=[])
    @patch("ingest.ingest_weather")
    @patch("ingest.ingest_race_control")
    @patch("ingest.ingest_race_results")
    @patch("ingest.ingest_overtakes")
    @patch("ingest.ingest_intervals")
    @patch("ingest.ingest_championship_drivers")
    @patch("ingest.ingest_championship_teams")
    @patch("ingest.ingest_qualifying_results")
    @patch("ingest.ingest_starting_grid")
    def test_qualifying_calls_qualifying_specific_functions(
        self, mock_starting_grid, mock_qual_results, mock_champ_teams,
        mock_champ_drivers, mock_intervals, mock_overtakes, mock_race_results,
        mock_race_control, mock_weather, mock_pit_stops, mock_stints,
        mock_laps, mock_drivers, mock_session, mock_meeting, mock_get_session,
        mock_reset_stats,
    ):
        client = _mock_client()

        process_session(client, 9001)

        mock_qual_results.assert_called_once()
        mock_starting_grid.assert_called_once()

    @patch("ingest.openf1.reset_stats")
    @patch("ingest.openf1.get_session", return_value=[QUALIFYING_SESSION])
    @patch("ingest.ingest_meeting")
    @patch("ingest.ingest_session", return_value=QUALIFYING_SESSION)
    @patch("ingest.ingest_drivers")
    @patch("ingest.ingest_laps", return_value=[])
    @patch("ingest.ingest_stints")
    @patch("ingest.ingest_pit_stops", return_value=[])
    @patch("ingest.ingest_weather")
    @patch("ingest.ingest_race_control")
    @patch("ingest.ingest_race_results")
    @patch("ingest.ingest_overtakes")
    @patch("ingest.ingest_intervals")
    @patch("ingest.ingest_championship_drivers")
    @patch("ingest.ingest_championship_teams")
    @patch("ingest.ingest_qualifying_results")
    @patch("ingest.ingest_starting_grid")
    @patch("ingest.ingest_fastest_lap_flag")
    def test_qualifying_does_not_call_race_functions(
        self, mock_fastest_lap_flag, mock_starting_grid, mock_qual_results,
        mock_champ_teams, mock_champ_drivers, mock_intervals, mock_overtakes,
        mock_race_results, mock_race_control, mock_weather, mock_pit_stops,
        mock_stints, mock_laps, mock_drivers, mock_session, mock_meeting,
        mock_get_session, mock_reset_stats,
    ):
        client = _mock_client()

        process_session(client, 9001)

        mock_race_results.assert_not_called()
        mock_fastest_lap_flag.assert_not_called()
        mock_overtakes.assert_not_called()
        mock_intervals.assert_not_called()
        mock_champ_drivers.assert_not_called()
        mock_champ_teams.assert_not_called()

    @patch("ingest.openf1.reset_stats")
    @patch("ingest.openf1.get_session", return_value=[QUALIFYING_SESSION])
    @patch("ingest.ingest_meeting")
    @patch("ingest.ingest_session", return_value=QUALIFYING_SESSION)
    @patch("ingest.ingest_drivers")
    @patch("ingest.ingest_laps", return_value=[])
    @patch("ingest.ingest_stints", return_value=[{"compound": "SOFT"}])
    @patch("ingest.ingest_pit_stops", return_value=[])
    @patch("ingest.ingest_weather")
    @patch("ingest.ingest_race_control", return_value=[{"qualifying_phase": "Q1"}])
    @patch("ingest.ingest_race_results")
    @patch("ingest.ingest_overtakes")
    @patch("ingest.ingest_intervals")
    @patch("ingest.ingest_championship_drivers")
    @patch("ingest.ingest_championship_teams")
    @patch("ingest.ingest_qualifying_results")
    @patch("ingest.ingest_starting_grid")
    def test_qualifying_passes_rc_and_stints_to_qualifying_results(
        self, mock_starting_grid, mock_qual_results, mock_champ_teams,
        mock_champ_drivers, mock_intervals, mock_overtakes, mock_race_results,
        mock_race_control, mock_weather, mock_pit_stops, mock_stints,
        mock_laps, mock_drivers, mock_session, mock_meeting, mock_get_session,
        mock_reset_stats,
    ):
        client = _mock_client()

        process_session(client, 9001)

        call_args = mock_qual_results.call_args
        # args[3] = rc_rows (return value of ingest_race_control)
        self.assertEqual(call_args.args[3], [{"qualifying_phase": "Q1"}])
        # args[4] = stints_rows (return value of ingest_stints)
        self.assertEqual(call_args.args[4], [{"compound": "SOFT"}])


class TestProcessSessionSprint(unittest.TestCase):
    """process_session with a Sprint session type — should route like Race."""

    @patch("ingest.openf1.reset_stats")
    @patch("ingest.openf1.get_session", return_value=[SPRINT_SESSION])
    @patch("ingest.ingest_meeting")
    @patch("ingest.ingest_session", return_value=SPRINT_SESSION)
    @patch("ingest.ingest_drivers")
    @patch("ingest.ingest_laps", return_value=[])
    @patch("ingest.ingest_stints")
    @patch("ingest.ingest_pit_stops", return_value=[])
    @patch("ingest.ingest_weather")
    @patch("ingest.ingest_race_control")
    @patch("ingest.ingest_race_results")
    @patch("ingest.ingest_overtakes")
    @patch("ingest.ingest_intervals")
    @patch("ingest.ingest_championship_drivers")
    @patch("ingest.ingest_championship_teams")
    @patch("ingest.ingest_qualifying_results")
    @patch("ingest.ingest_starting_grid")
    @patch("ingest.ingest_fastest_lap_flag")
    def test_sprint_calls_race_specific_functions(
        self, mock_fastest_lap_flag, mock_starting_grid, mock_qual_results,
        mock_champ_teams, mock_champ_drivers, mock_intervals, mock_overtakes,
        mock_race_results, mock_race_control, mock_weather, mock_pit_stops,
        mock_stints, mock_laps, mock_drivers, mock_session, mock_meeting,
        mock_get_session, mock_reset_stats,
    ):
        client = _mock_client()

        process_session(client, 9002)

        mock_race_results.assert_called_once()
        mock_fastest_lap_flag.assert_called_once()
        mock_overtakes.assert_called_once()
        mock_intervals.assert_called_once()
        mock_champ_drivers.assert_called_once()
        mock_champ_teams.assert_called_once()
        mock_qual_results.assert_not_called()
        mock_starting_grid.assert_not_called()


class TestProcessSessionSprintQualifying(unittest.TestCase):
    """process_session with Sprint Qualifying — should route like Qualifying."""

    @patch("ingest.openf1.reset_stats")
    @patch("ingest.openf1.get_session", return_value=[SPRINT_QUALIFYING_SESSION])
    @patch("ingest.ingest_meeting")
    @patch("ingest.ingest_session", return_value=SPRINT_QUALIFYING_SESSION)
    @patch("ingest.ingest_drivers")
    @patch("ingest.ingest_laps", return_value=[])
    @patch("ingest.ingest_stints")
    @patch("ingest.ingest_pit_stops", return_value=[])
    @patch("ingest.ingest_weather")
    @patch("ingest.ingest_race_control")
    @patch("ingest.ingest_race_results")
    @patch("ingest.ingest_overtakes")
    @patch("ingest.ingest_intervals")
    @patch("ingest.ingest_championship_drivers")
    @patch("ingest.ingest_championship_teams")
    @patch("ingest.ingest_qualifying_results")
    @patch("ingest.ingest_starting_grid")
    def test_sprint_qualifying_calls_qualifying_functions(
        self, mock_starting_grid, mock_qual_results, mock_champ_teams,
        mock_champ_drivers, mock_intervals, mock_overtakes, mock_race_results,
        mock_race_control, mock_weather, mock_pit_stops, mock_stints,
        mock_laps, mock_drivers, mock_session, mock_meeting, mock_get_session,
        mock_reset_stats,
    ):
        client = _mock_client()

        process_session(client, 9003)

        mock_qual_results.assert_called_once()
        mock_starting_grid.assert_called_once()
        mock_race_results.assert_not_called()
        mock_overtakes.assert_not_called()


class TestProcessSessionSprintShootout(unittest.TestCase):
    """process_session with Sprint Shootout — should route like Qualifying."""

    @patch("ingest.openf1.reset_stats")
    @patch("ingest.openf1.get_session", return_value=[SPRINT_SHOOTOUT_SESSION])
    @patch("ingest.ingest_meeting")
    @patch("ingest.ingest_session", return_value=SPRINT_SHOOTOUT_SESSION)
    @patch("ingest.ingest_drivers")
    @patch("ingest.ingest_laps", return_value=[])
    @patch("ingest.ingest_stints")
    @patch("ingest.ingest_pit_stops", return_value=[])
    @patch("ingest.ingest_weather")
    @patch("ingest.ingest_race_control")
    @patch("ingest.ingest_race_results")
    @patch("ingest.ingest_overtakes")
    @patch("ingest.ingest_intervals")
    @patch("ingest.ingest_championship_drivers")
    @patch("ingest.ingest_championship_teams")
    @patch("ingest.ingest_qualifying_results")
    @patch("ingest.ingest_starting_grid")
    def test_sprint_shootout_calls_qualifying_functions(
        self, mock_starting_grid, mock_qual_results, mock_champ_teams,
        mock_champ_drivers, mock_intervals, mock_overtakes, mock_race_results,
        mock_race_control, mock_weather, mock_pit_stops, mock_stints,
        mock_laps, mock_drivers, mock_session, mock_meeting, mock_get_session,
        mock_reset_stats,
    ):
        client = _mock_client()

        process_session(client, 9004)

        mock_qual_results.assert_called_once()
        mock_starting_grid.assert_called_once()
        mock_race_results.assert_not_called()
        mock_overtakes.assert_not_called()


class TestProcessSessionEarlyReturn(unittest.TestCase):
    """process_session should return early without error for unknown session_key."""

    @patch("ingest.openf1.reset_stats")
    @patch("ingest.openf1.get_session", return_value=[])
    @patch("ingest.ingest_meeting")
    @patch("ingest.ingest_session")
    @patch("ingest.ingest_drivers")
    @patch("ingest.ingest_laps")
    @patch("ingest.ingest_stints")
    @patch("ingest.ingest_pit_stops")
    @patch("ingest.ingest_weather")
    @patch("ingest.ingest_race_control")
    def test_unknown_session_key_returns_early_without_error(
        self, mock_race_control, mock_weather, mock_pit_stops, mock_stints,
        mock_laps, mock_drivers, mock_session, mock_meeting, mock_get_session,
        mock_reset_stats,
    ):
        client = _mock_client()

        # Should not raise
        result = process_session(client, 99999)

        self.assertIsNone(result)
        mock_meeting.assert_not_called()
        mock_session.assert_not_called()
        mock_drivers.assert_not_called()

    @patch("ingest.openf1.reset_stats")
    @patch("ingest.openf1.get_session", return_value=[RACE_SESSION])
    @patch("ingest.ingest_meeting")
    @patch("ingest.ingest_session", return_value=None)  # session ingest fails
    @patch("ingest.ingest_drivers")
    @patch("ingest.ingest_laps")
    @patch("ingest.ingest_stints")
    @patch("ingest.ingest_pit_stops")
    @patch("ingest.ingest_weather")
    @patch("ingest.ingest_race_control")
    def test_none_from_ingest_session_returns_early(
        self, mock_race_control, mock_weather, mock_pit_stops, mock_stints,
        mock_laps, mock_drivers, mock_session, mock_meeting, mock_get_session,
        mock_reset_stats,
    ):
        client = _mock_client()

        result = process_session(client, 9000)

        self.assertIsNone(result)
        mock_drivers.assert_not_called()


# ===========================================================================
# process_meeting — integration tests
# ===========================================================================

_MEETING_INFO = [
    {
        "meeting_key": 1234,
        "meeting_name": "Bahrain Grand Prix",
        "location": "Bahrain",
        "year": 2024,
    }
]

_ALL_SESSIONS = [
    {"session_key": 9000, "session_name": "Race"},
    {"session_key": 9001, "session_name": "Qualifying"},
    {"session_key": 9002, "session_name": "Sprint"},
    {"session_key": 9003, "session_name": "Sprint Qualifying"},
    {"session_key": 9004, "session_name": "Sprint Shootout"},
    {"session_key": 9010, "session_name": "Practice 1"},
    {"session_key": 9011, "session_name": "Practice 2"},
    {"session_key": 9012, "session_name": "Practice 3"},
]


class TestProcessMeeting(unittest.TestCase):

    @patch("ingest.process_session")
    @patch("ingest.openf1.get_sessions", return_value=_ALL_SESSIONS)
    @patch("ingest.openf1.get_meeting", return_value=_MEETING_INFO)
    def test_only_allowed_session_names_are_processed(
        self, mock_get_meeting, mock_get_sessions, mock_process_session
    ):
        client = _mock_client()

        process_meeting(client, 1234)

        processed_keys = {c.args[1] for c in mock_process_session.call_args_list}
        # Allowed: Race, Qualifying, Sprint, Sprint Qualifying, Sprint Shootout
        self.assertIn(9000, processed_keys)
        self.assertIn(9001, processed_keys)
        self.assertIn(9002, processed_keys)
        self.assertIn(9003, processed_keys)
        self.assertIn(9004, processed_keys)

    @patch("ingest.process_session")
    @patch("ingest.openf1.get_sessions", return_value=_ALL_SESSIONS)
    @patch("ingest.openf1.get_meeting", return_value=_MEETING_INFO)
    def test_practice_sessions_skipped(
        self, mock_get_meeting, mock_get_sessions, mock_process_session
    ):
        client = _mock_client()

        process_meeting(client, 1234)

        processed_keys = {c.args[1] for c in mock_process_session.call_args_list}
        self.assertNotIn(9010, processed_keys)
        self.assertNotIn(9011, processed_keys)
        self.assertNotIn(9012, processed_keys)

    @patch("ingest.process_session")
    @patch("ingest.openf1.get_sessions", return_value=_ALL_SESSIONS)
    @patch("ingest.openf1.get_meeting", return_value=_MEETING_INFO)
    def test_process_session_called_for_each_eligible_session(
        self, mock_get_meeting, mock_get_sessions, mock_process_session
    ):
        client = _mock_client()

        process_meeting(client, 1234)

        # 5 allowed sessions
        self.assertEqual(mock_process_session.call_count, 5)

    @patch("ingest.process_session")
    @patch("ingest.openf1.get_sessions", return_value=[])
    @patch("ingest.openf1.get_meeting", return_value=_MEETING_INFO)
    def test_no_sessions_means_process_session_never_called(
        self, mock_get_meeting, mock_get_sessions, mock_process_session
    ):
        client = _mock_client()

        process_meeting(client, 1234)

        mock_process_session.assert_not_called()

    @patch("ingest.process_session")
    @patch("ingest.openf1.get_sessions", return_value=_ALL_SESSIONS)
    @patch("ingest.openf1.get_meeting", return_value=[])
    def test_empty_meeting_info_still_processes_sessions(
        self, mock_get_meeting, mock_get_sessions, mock_process_session
    ):
        """If get_meeting returns [] process_meeting should still process sessions."""
        client = _mock_client()

        process_meeting(client, 1234)

        # 5 eligible sessions in _ALL_SESSIONS
        self.assertEqual(mock_process_session.call_count, 5)

    @patch("ingest.process_session")
    @patch("ingest.openf1.get_sessions", return_value=[
        {"session_key": 9000, "session_name": "Race"},
        {"session_key": 9010, "session_name": "Practice 1"},
    ])
    @patch("ingest.openf1.get_meeting", return_value=_MEETING_INFO)
    def test_mixed_sessions_only_race_processed(
        self, mock_get_meeting, mock_get_sessions, mock_process_session
    ):
        client = _mock_client()

        process_meeting(client, 1234)

        self.assertEqual(mock_process_session.call_count, 1)
        self.assertEqual(mock_process_session.call_args.args[1], 9000)


# ===========================================================================
# _find_brake_zones / _brake_zone_stats
# ===========================================================================

class TestFindBrakeZones(unittest.TestCase):

    def test_no_zones_when_no_decel(self):
        import numpy as np
        accel_g = np.array([0.1, 0.2, 0.1, 0.0, 0.15])   # all positive
        v_filt  = np.array([200.0, 201.0, 202.0, 201.0, 200.0, 199.0])
        zones = _find_brake_zones(accel_g, v_filt)
        self.assertEqual(zones, [])

    def test_single_zone(self):
        import numpy as np
        # Two samples below threshold → one zone
        accel_g = np.array([0.1, -1.0, -1.5, 0.1])
        v_filt  = np.array([250.0, 240.0, 220.0, 200.0, 190.0])
        zones = _find_brake_zones(accel_g, v_filt)
        self.assertEqual(len(zones), 1)
        peak_decel, speed = zones[0]
        self.assertAlmostEqual(peak_decel, 1.5)
        self.assertAlmostEqual(speed, 240.0)   # v_filt[1], where zone starts

    def test_two_separate_zones(self):
        import numpy as np
        accel_g = np.array([-1.0, 0.1, 0.2, -0.8, 0.0])
        v_filt  = np.array([200.0, 190.0, 195.0, 200.0, 185.0, 190.0])
        zones = _find_brake_zones(accel_g, v_filt)
        self.assertEqual(len(zones), 2)


class TestBrakeZoneStats(unittest.TestCase):

    def test_empty_zones(self):
        count, mean_peak, speed = _brake_zone_stats([])
        self.assertEqual(count, 0)
        self.assertIsNone(mean_peak)
        self.assertIsNone(speed)

    def test_single_zone(self):
        count, mean_peak, speed = _brake_zone_stats([(2.5, 230.0)])
        self.assertEqual(count, 1)
        self.assertAlmostEqual(mean_peak, 2.5)
        self.assertAlmostEqual(speed, 230.0)

    def test_primary_is_hardest_zone(self):
        # Primary zone (returned speed) should be the one with the highest peak decel
        zones = [(1.0, 200.0), (3.5, 250.0), (2.0, 220.0)]
        count, mean_peak, speed = _brake_zone_stats(zones)
        self.assertEqual(count, 3)
        self.assertAlmostEqual(mean_peak, (1.0 + 3.5 + 2.0) / 3)
        self.assertAlmostEqual(speed, 250.0)   # speed from the 3.5 g zone


# ===========================================================================
# _compute_lap_metrics
# ===========================================================================

def _make_car_data(n=80, throttle=100, brake=0, speed_slope=0.5, drs=0):
    """Generate synthetic car_data records at 1 Hz with controllable inputs.

    Uses whole-second timestamps to avoid pandas ISO8601 microsecond ambiguity.
    Records at 1 Hz are fine: _compute_lap_metrics resamples to 4 Hz via PCHIP.
    """
    import datetime
    base = datetime.datetime(2024, 3, 2, 13, 0, 0)
    records = []
    for i in range(n):
        records.append({
            'date':     (base + datetime.timedelta(seconds=i)).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'speed':    200.0 + speed_slope * i,
            'throttle': throttle,
            'brake':    brake,
            'drs':      drs,
        })
    return records


class TestComputeLapMetrics(unittest.TestCase):

    def test_returns_none_for_empty(self):
        self.assertIsNone(_compute_lap_metrics([]))

    def test_returns_none_for_too_few_records(self):
        self.assertIsNone(_compute_lap_metrics(_make_car_data(n=10)))

    def test_returns_dict_for_valid_data(self):
        result = _compute_lap_metrics(_make_car_data(n=80, throttle=100, brake=0))
        self.assertIsInstance(result, dict)
        self.assertIn('peak_accel_g', result)
        self.assertIn('coasting_ratio_lap', result)
        self.assertIn('drs_activation_count', result)
        self.assertIn('brake_zone_count_lap', result)

    def test_peak_accel_none_when_throttle_always_low(self):
        result = _compute_lap_metrics(_make_car_data(n=80, throttle=0, brake=0))
        self.assertIsNotNone(result)
        self.assertIsNone(result['peak_accel_g'])

    def test_decel_captured_when_braking(self):
        result = _compute_lap_metrics(_make_car_data(n=80, throttle=0, brake=100, speed_slope=-0.5))
        self.assertIsNotNone(result)
        self.assertIsNotNone(result['peak_decel_g_abs'])
        self.assertGreaterEqual(result['peak_decel_g_abs'], 0.0)

    def test_coasting_ratio_zero_when_always_on_throttle(self):
        result = _compute_lap_metrics(_make_car_data(n=80, throttle=100, brake=0))
        self.assertIsNotNone(result)
        self.assertEqual(result['coasting_ratio_lap'], 0.0)

    def test_full_throttle_pct_one_when_always_full(self):
        result = _compute_lap_metrics(_make_car_data(n=80, throttle=100, brake=0))
        self.assertIsNotNone(result)
        self.assertAlmostEqual(result['full_throttle_pct_lap'], 1.0, places=4)

    def test_sector_metrics_none_without_sector_times(self):
        result = _compute_lap_metrics(_make_car_data(n=80))
        self.assertIsNotNone(result)
        self.assertIsNone(result['coasting_ratio_s1'])
        self.assertIsNone(result['max_speed_kph_s1'])
        self.assertIsNone(result['brake_zone_count_s1'])

    def test_returns_none_for_insufficient_after_dedup(self):
        import datetime
        base = datetime.datetime(2024, 3, 2, 13, 0, 0)
        records = [
            {
                'date':     (base + datetime.timedelta(seconds=i)).strftime('%Y-%m-%dT%H:%M:%SZ'),
                'speed':    200.0,
                'throttle': 50,
                'brake':    0,
                'drs':      0,
            }
            for i in range(80)
        ]
        self.assertIsNone(_compute_lap_metrics(records))

    def test_coasting_ratio_nonzero_when_coasting(self):
        # throttle=0 (<1%), brake=0 → every sample is a coasting sample
        result = _compute_lap_metrics(_make_car_data(n=80, throttle=0, brake=0))
        self.assertIsNotNone(result)
        self.assertGreater(result['coasting_ratio_lap'], 0.0)

    def test_coasting_distance_nonzero_when_coasting(self):
        result = _compute_lap_metrics(_make_car_data(n=80, throttle=0, brake=0))
        self.assertIsNotNone(result)
        self.assertGreater(result['coasting_distance_m_lap'], 0.0)

    def test_coasting_ratio_zero_when_braking(self):
        # brake=100 disqualifies from coasting even if throttle < 1%
        result = _compute_lap_metrics(_make_car_data(n=80, throttle=0, brake=100))
        self.assertIsNotNone(result)
        self.assertEqual(result['coasting_ratio_lap'], 0.0)

    def test_drs_activation_count_counts_open_transitions(self):
        import datetime
        base = datetime.datetime(2024, 3, 2, 13, 0, 0)
        records = []
        for i in range(80):
            # DRS opens at i=20 and i=60, closes at i=40 → 2 open transitions
            drs = 14 if (20 <= i < 40 or i >= 60) else 0
            records.append({
                'date':     (base + datetime.timedelta(seconds=i)).strftime('%Y-%m-%dT%H:%M:%SZ'),
                'speed':    200.0 + 0.5 * i,
                'throttle': 100,
                'brake':    0,
                'drs':      drs,
            })
        result = _compute_lap_metrics(records)
        self.assertIsNotNone(result)
        self.assertEqual(result['drs_activation_count'], 2)
        self.assertGreater(result['drs_distance_m'], 0.0)

    def test_drs_fields_none_when_no_drs_column(self):
        import datetime
        base = datetime.datetime(2024, 3, 2, 13, 0, 0)
        records = [
            {
                'date':     (base + datetime.timedelta(seconds=i)).strftime('%Y-%m-%dT%H:%M:%SZ'),
                'speed':    200.0 + 0.5 * i,
                'throttle': 100,
                'brake':    0,
                # no 'drs' key
            }
            for i in range(80)
        ]
        result = _compute_lap_metrics(records)
        self.assertIsNotNone(result)
        self.assertIsNone(result['drs_activation_count'])
        self.assertIsNone(result['drs_distance_m'])

    def test_throttle_brake_overlap_ratio_nonzero(self):
        # throttle=50 (>= 10%), brake=100 (> 0) → every sample is overlap
        result = _compute_lap_metrics(_make_car_data(n=80, throttle=50, brake=100, speed_slope=-0.5))
        self.assertIsNotNone(result)
        self.assertGreater(result['throttle_brake_overlap_ratio_lap'], 0.0)

    def test_throttle_brake_overlap_ratio_zero_when_no_brake(self):
        result = _compute_lap_metrics(_make_car_data(n=80, throttle=100, brake=0))
        self.assertIsNotNone(result)
        self.assertEqual(result['throttle_brake_overlap_ratio_lap'], 0.0)

    def test_max_speed_kph_captures_peak(self):
        # speed ramps from 200 to ~279 with slope=1.0 and 80 records
        result = _compute_lap_metrics(_make_car_data(n=80, throttle=100, brake=0, speed_slope=1.0))
        self.assertIsNotNone(result)
        self.assertGreater(result['max_speed_kph_lap'], 250.0)

    def test_sector_splits_populated_with_sector_times(self):
        import datetime
        import pandas as pd
        records = _make_car_data(n=80, throttle=0, brake=0)
        # Parse the first record's date the same way _compute_lap_metrics does so
        # the epoch is in UTC regardless of the local timezone on the test machine.
        t0_epoch = pd.to_datetime(records[0]['date']).timestamp()
        result = _compute_lap_metrics(
            records,
            s1_end_t=t0_epoch + 25,
            s2_end_t=t0_epoch + 50,
        )
        self.assertIsNotNone(result)
        self.assertIsNotNone(result['coasting_ratio_s1'])
        self.assertIsNotNone(result['coasting_ratio_s2'])
        self.assertIsNotNone(result['coasting_ratio_s3'])
        self.assertIsNotNone(result['max_speed_kph_s1'])
        self.assertIsNotNone(result['max_speed_kph_s2'])
        self.assertIsNotNone(result['max_speed_kph_s3'])

    def test_brake_zone_count_nonzero_when_hard_braking(self):
        # Build records with a steep speed drop in the second half that produces
        # deceleration well above the 0.5g brake-zone threshold.
        # 50 records gently accelerating (avoids dedup), then 30 records braking
        # from ~625 km/h at 25 km/h per step (~0.71g > 0.5g threshold).
        # All speeds are distinct so dedup never removes them.
        import datetime
        base = datetime.datetime(2024, 3, 2, 13, 0, 0)
        records = []
        start_speed = 600.0
        for i in range(50):
            records.append({
                'date':     (base + datetime.timedelta(seconds=i)).strftime('%Y-%m-%dT%H:%M:%SZ'),
                'speed':    start_speed + 0.5 * i,
                'throttle': 100,
                'brake':    0,
                'drs':      0,
            })
        phase2_start = start_speed + 0.5 * 49  # 624.5 km/h
        for j in range(30):
            records.append({
                'date':     (base + datetime.timedelta(seconds=50 + j)).strftime('%Y-%m-%dT%H:%M:%SZ'),
                'speed':    max(phase2_start - 25.0 * j, 1.0),
                'throttle': 0,
                'brake':    100,
                'drs':      0,
            })
        result = _compute_lap_metrics(records)
        self.assertIsNotNone(result)
        self.assertIsNotNone(result['brake_zone_count_lap'])
        self.assertGreater(result['brake_zone_count_lap'], 0)
        self.assertIsNotNone(result['mean_peak_decel_g_lap'])

    def test_estimated_superclipping_distance_zero_when_accelerating(self):
        # throttle >= 10, brake == 0, but car accelerates → no superclipping
        result = _compute_lap_metrics(_make_car_data(n=80, throttle=100, brake=0, speed_slope=0.5))
        self.assertIsNotNone(result)
        self.assertEqual(result['estimated_superclipping_distance_m_lap'], 0.0)

    def test_estimated_superclipping_distance_nonzero_when_decelerating_under_throttle(self):
        # throttle=50 (>= 10%), brake=0, speed decreasing → superclipping condition met
        result = _compute_lap_metrics(_make_car_data(n=80, throttle=50, brake=0, speed_slope=-0.5))
        self.assertIsNotNone(result)
        self.assertGreater(result['estimated_superclipping_distance_m_lap'], 0.0)

    def test_estimated_superclipping_zero_when_braking_with_throttle(self):
        # brake=100 disqualifies from superclipping even if throttle >= 10%
        result = _compute_lap_metrics(_make_car_data(n=80, throttle=50, brake=100, speed_slope=-0.5))
        self.assertIsNotNone(result)
        self.assertEqual(result['estimated_superclipping_distance_m_lap'], 0.0)

    def test_all_expected_keys_present_in_output(self):
        result = _compute_lap_metrics(_make_car_data(n=80, throttle=100, brake=0))
        self.assertIsNotNone(result)
        for key in _MOCK_METRICS:
            self.assertIn(key, result, f"Missing key: {key}")


# ===========================================================================
# process_session — recompute flag
# ===========================================================================

class TestProcessSessionRecompute(unittest.TestCase):
    """process_session recompute=True triggers recompute_lap_metrics."""

    _COMMON_PATCHES = [
        "ingest.openf1.reset_stats",
        "ingest.ingest_meeting",
        "ingest.ingest_drivers",
        "ingest.ingest_stints",
        "ingest.ingest_pit_stops",
        "ingest.ingest_weather",
        "ingest.ingest_race_results",
        "ingest.ingest_fastest_lap_flag",
        "ingest.ingest_overtakes",
        "ingest.ingest_intervals",
        "ingest.ingest_championship_drivers",
        "ingest.ingest_championship_teams",
        "ingest.ingest_starting_grid",
        "ingest.ingest_position",
        "ingest.ingest_team_radio",
    ]

    def _run_with_patches(self, session_fixture, recompute):
        """Run process_session with all side-effect functions patched out.

        Returns the mock for recompute_lap_metrics so callers can inspect calls.
        """
        with ExitStack() as stack:
            for target in self._COMMON_PATCHES:
                stack.enter_context(patch(target))
            stack.enter_context(patch("ingest.openf1.get_session", return_value=[session_fixture]))
            stack.enter_context(patch("ingest.ingest_session", return_value=session_fixture))
            stack.enter_context(patch("ingest.ingest_laps", return_value=[]))
            stack.enter_context(patch("ingest.ingest_race_control", return_value=[]))
            stack.enter_context(patch("ingest.ingest_qualifying_results", return_value={}))
            mock_recompute = stack.enter_context(patch("ingest.recompute_lap_metrics"))
            process_session(_mock_client(), session_fixture["session_key"], recompute=recompute)
        return mock_recompute

    def test_race_with_recompute_true_calls_recompute_lap_metrics(self):
        mock_recompute = self._run_with_patches(RACE_SESSION, recompute=True)
        mock_recompute.assert_called_once()
        self.assertEqual(mock_recompute.call_args.args[3], 'race')

    def test_qualifying_with_recompute_true_calls_recompute_lap_metrics(self):
        mock_recompute = self._run_with_patches(QUALIFYING_SESSION, recompute=True)
        mock_recompute.assert_called_once()
        self.assertEqual(mock_recompute.call_args.args[3], 'qualifying')

    def test_recompute_false_does_not_call_recompute_lap_metrics(self):
        mock_recompute = self._run_with_patches(RACE_SESSION, recompute=False)
        mock_recompute.assert_not_called()


# ===========================================================================
# ingest_lap_metrics
# ===========================================================================

def _make_laps(n_laps=5, driver_number=44):
    """Minimal lap dicts for ingest_lap_metrics."""
    import datetime
    base = datetime.datetime(2024, 3, 2, 13, 0, 0)
    laps = []
    for i in range(n_laps):
        laps.append({
            'session_key': 9158,
            'driver_number': driver_number,
            'lap_number': i + 1,
            'is_pit_out_lap': False,
            'date_start': (base + datetime.timedelta(minutes=i * 2)).strftime('%Y-%m-%dT%H:%M:%SZ'),
        })
    return laps


_MOCK_METRICS = {
    'peak_accel_g': 1.5, 'peak_decel_g_abs': 3.2,
    'max_linear_acceleration_g_lap': 1.5, 'max_linear_acceleration_g_s1': None,
    'max_linear_acceleration_g_s2': None, 'max_linear_acceleration_g_s3': None,
    'max_linear_deceleration_g_lap': 3.2, 'max_linear_deceleration_g_s1': None,
    'max_linear_deceleration_g_s2': None, 'max_linear_deceleration_g_s3': None,
    'max_speed_kph_lap': 310.0, 'max_speed_kph_s1': None,
    'max_speed_kph_s2': None,   'max_speed_kph_s3': None,
    'coasting_ratio_lap': 0.1,  'coasting_ratio_s1': None,
    'coasting_ratio_s2': None,  'coasting_ratio_s3': None,
    'coasting_distance_m_lap': 50.0, 'coasting_distance_m_s1': None,
    'coasting_distance_m_s2': None,  'coasting_distance_m_s3': None,
    'estimated_superclipping_distance_m_lap': 0.0, 'estimated_superclipping_distance_m_s1': None,
    'estimated_superclipping_distance_m_s2': None, 'estimated_superclipping_distance_m_s3': None,
    'full_throttle_pct_lap': 0.6, 'full_throttle_pct_s1': None,
    'full_throttle_pct_s2': None, 'full_throttle_pct_s3': None,
    'throttle_brake_overlap_ratio_lap': 0.02, 'throttle_brake_overlap_ratio_s1': None,
    'throttle_brake_overlap_ratio_s2': None,  'throttle_brake_overlap_ratio_s3': None,
    'throttle_input_variance_lap': 0.001, 'throttle_input_variance_s1': None,
    'throttle_input_variance_s2': None,   'throttle_input_variance_s3': None,
    'drs_activation_count': 2, 'drs_distance_m': 300.0,
    'brake_zone_count_lap': 5, 'brake_zone_count_s1': None,
    'brake_zone_count_s2': None, 'brake_zone_count_s3': None,
    'mean_peak_decel_g_lap': 2.8, 'mean_peak_decel_g_s1': None,
    'mean_peak_decel_g_s2': None, 'mean_peak_decel_g_s3': None,
    'speed_at_brake_start_kph_lap': 290.0, 'speed_at_brake_start_kph_s1': None,
    'speed_at_brake_start_kph_s2': None,   'speed_at_brake_start_kph_s3': None,
}


class TestIngestLapMetrics(unittest.TestCase):

    @patch("ingest.openf1.get_car_data")
    def test_empty_car_data_produces_no_rows(self, mock_get_car_data):
        mock_get_car_data.return_value = []
        client = _mock_client()
        result = ingest_lap_metrics(client, 9158, _make_laps(5))
        client.table.assert_not_called()
        self.assertEqual(result, {})

    @patch("ingest.openf1.get_car_data")
    def test_pit_out_laps_excluded(self, mock_get_car_data):
        mock_get_car_data.return_value = []
        client = _mock_client()
        laps = _make_laps(3)
        laps[1]['is_pit_out_lap'] = True
        ingest_lap_metrics(client, 9158, laps)
        # Still ≥ 2 non-pit-out laps → get_car_data is called
        mock_get_car_data.assert_called_once()

    @patch("ingest.openf1.get_car_data")
    def test_single_non_pit_lap_skips_driver(self, mock_get_car_data):
        mock_get_car_data.return_value = []
        client = _mock_client()
        laps = _make_laps(2)
        laps[1]['is_pit_out_lap'] = True
        result = ingest_lap_metrics(client, 9158, laps)
        mock_get_car_data.assert_not_called()
        self.assertEqual(result, {})

    @patch("ingest._compute_lap_metrics")
    @patch("ingest.openf1.get_car_data")
    def test_valid_laps_upserted_and_returned(self, mock_get_car_data, mock_clm):
        mock_get_car_data.return_value = _make_car_data(n=80)
        mock_clm.return_value = _MOCK_METRICS.copy()
        client = _mock_client()

        result = ingest_lap_metrics(client, 9158, _make_laps(3, driver_number=44))

        # 3 laps → 2 intervals → 2 _compute_lap_metrics calls → 2 rows upserted
        self.assertEqual(mock_clm.call_count, 2)
        self.assertIn(44, result)
        # Return type is {driver: {lap_number: metrics_dict}}
        self.assertEqual(len(result[44]), 2)
        self.assertEqual(result[44][1]['peak_accel_g'], 1.5)   # lap_number 1

    @patch("ingest._compute_lap_metrics")
    @patch("ingest.openf1.get_car_data")
    def test_none_result_laps_excluded_from_return(self, mock_get_car_data, mock_clm):
        mock_get_car_data.return_value = _make_car_data(n=80)
        # First call returns None (insufficient data); second returns valid metrics
        mock_clm.side_effect = [None, _MOCK_METRICS.copy()]
        client = _mock_client()

        result = ingest_lap_metrics(client, 9158, _make_laps(3, driver_number=44))

        self.assertIn(44, result)
        self.assertEqual(len(result[44]), 1)   # only the second lap

    @patch("ingest._compute_lap_metrics")
    @patch("ingest.openf1.get_car_data")
    def test_mixed_timestamp_formats_do_not_raise(self, mock_get_car_data, mock_clm):
        """Car data with mixed timestamp formats (some with .microseconds, some without,
        some with +00:00 instead of Z) must not raise ValueError.

        Regression: pd.to_datetime() without format='ISO8601' inferred the format from
        the first record and failed when a subsequent record omitted fractional seconds
        (e.g. '2026-03-13T07:46:38+00:00' after records with '.123456+00:00').
        """
        import datetime
        base = datetime.datetime(2024, 3, 2, 13, 0, 0)
        records = []
        for i in range(80):
            # Alternate between timestamps with microseconds and without,
            # and between Z and +00:00 timezone notation.
            if i % 3 == 0:
                ts = (base + datetime.timedelta(seconds=i)).strftime('%Y-%m-%dT%H:%M:%S.%f+00:00')
            elif i % 3 == 1:
                ts = (base + datetime.timedelta(seconds=i)).strftime('%Y-%m-%dT%H:%M:%S+00:00')
            else:
                ts = (base + datetime.timedelta(seconds=i)).strftime('%Y-%m-%dT%H:%M:%SZ')
            records.append({'date': ts, 'speed': 200.0, 'throttle': 100, 'brake': 0, 'drs': 0})

        mock_get_car_data.return_value = records
        mock_clm.return_value = _MOCK_METRICS.copy()
        client = _mock_client()

        # Should not raise ValueError
        ingest_lap_metrics(client, 9158, _make_laps(3, driver_number=44))


# ===========================================================================
# ingest_race_peak_g_summary
# ===========================================================================

class TestIngestRacePeakGSummary(unittest.TestCase):

    def _make_driver_lap_metrics(self, values):
        """Build driver_lap_metrics dict from list of (accel, decel) tuples."""
        from ingest import ingest_race_peak_g_summary  # noqa: local import
        laps = {}
        for i, (a, d) in enumerate(values, start=1):
            laps[i] = {'peak_accel_g': a, 'peak_decel_g_abs': d}
        return {44: laps}

    def test_mean_upserted_to_race_results(self):
        client = _mock_client()
        driver_lap_metrics = self._make_driver_lap_metrics([(1.0, 2.0), (2.0, 4.0)])
        from ingest import ingest_race_peak_g_summary
        ingest_race_peak_g_summary(client, 9158, driver_lap_metrics)
        upserted = client.table.return_value.upsert.call_args.args[0]
        self.assertEqual(len(upserted), 1)
        row = upserted[0]
        self.assertEqual(row['session_key'], 9158)
        self.assertAlmostEqual(row['mean_peak_accel_g'], 1.5)
        self.assertAlmostEqual(row['mean_peak_decel_g_abs'], 3.0)

    def test_clean_mean_excludes_outlier_laps(self):
        client = _mock_client()
        # Lap 3 exceeds accel bound (4 g) and decel bound (8 g) → excluded from clean
        driver_lap_metrics = {44: {
            1: {'peak_accel_g': 1.0, 'peak_decel_g_abs': 3.0},
            2: {'peak_accel_g': 1.5, 'peak_decel_g_abs': 4.0},
            3: {'peak_accel_g': 5.0, 'peak_decel_g_abs': 9.0},
        }}
        from ingest import ingest_race_peak_g_summary
        ingest_race_peak_g_summary(client, 9158, driver_lap_metrics)
        upserted = client.table.return_value.upsert.call_args.args[0]
        row = upserted[0]
        self.assertAlmostEqual(row['mean_peak_accel_g_clean'], (1.0 + 1.5) / 2)
        self.assertAlmostEqual(row['mean_peak_decel_g_abs_clean'], (3.0 + 4.0) / 2)

    def test_driver_with_no_peak_g_skipped(self):
        client = _mock_client()
        driver_lap_metrics = {44: {1: {'peak_accel_g': None, 'peak_decel_g_abs': None}}}
        from ingest import ingest_race_peak_g_summary
        ingest_race_peak_g_summary(client, 9158, driver_lap_metrics)
        # No valid G values → no rows upserted
        client.table.assert_not_called()


# ===========================================================================
# _compute_qualifying_best_per_phase
# ===========================================================================

class TestComputeQualifyingBestPerPhase(unittest.TestCase):

    def _lap(self, dn, lap_number, lap_duration, phase):
        return {
            'driver_number': dn,
            'lap_number':    lap_number,
            'lap_duration':  lap_duration,
            '_phase':        phase,
        }

    def test_best_lap_selected_per_phase(self):
        laps = [
            self._lap(44, 1, 90.0, 'Q1'),
            self._lap(44, 2, 89.5, 'Q1'),   # faster — should win
            self._lap(44, 3, 88.0, 'Q2'),
        ]
        result = _compute_qualifying_best_per_phase(laps)
        self.assertEqual(result[44]['Q1'], (89.5, 2))
        self.assertEqual(result[44]['Q2'], (88.0, 3))

    def test_no_phase_laps_ignored(self):
        laps = [
            {'driver_number': 44, 'lap_number': 1, 'lap_duration': 90.0, '_phase': None},
            {'driver_number': 44, 'lap_number': 2, 'lap_duration': 89.0},   # no _phase key
        ]
        result = _compute_qualifying_best_per_phase(laps)
        self.assertEqual(result, {})

    def test_invalid_duration_excluded(self):
        laps = [
            self._lap(44, 1, 'INVALID', 'Q1'),
            self._lap(44, 2, 0.0, 'Q1'),     # zero duration
            self._lap(44, 3, 89.0, 'Q1'),    # only valid one
        ]
        result = _compute_qualifying_best_per_phase(laps)
        self.assertEqual(result[44]['Q1'], (89.0, 3))

    def test_multiple_drivers(self):
        laps = [
            self._lap(44, 1, 88.0, 'Q1'),
            self._lap(1,  1, 87.5, 'Q1'),
        ]
        result = _compute_qualifying_best_per_phase(laps)
        self.assertIn(44, result)
        self.assertIn(1, result)
        self.assertEqual(result[1]['Q1'][0], 87.5)


# ===========================================================================
# ingest_qualifying_peak_g_summary
# ===========================================================================

class TestIngestQualifyingPeakGSummary(unittest.TestCase):

    def test_upserts_phase_peak_g_to_qualifying_results(self):
        client = _mock_client()
        driver_lap_metrics = {
            44: {
                2: {'peak_accel_g': 1.5, 'peak_decel_g_abs': 3.2},   # Q1 best lap
                5: {'peak_accel_g': 1.8, 'peak_decel_g_abs': 3.5},   # Q2 best lap
            }
        }
        best_per_phase = {
            44: {'Q1': (89.5, 2), 'Q2': (88.0, 5)}
        }
        ingest_qualifying_peak_g_summary(client, 9000, driver_lap_metrics, best_per_phase)
        upserted = client.table.return_value.upsert.call_args.args[0]
        self.assertEqual(len(upserted), 1)
        row = upserted[0]
        self.assertEqual(row['session_key'], 9000)
        self.assertAlmostEqual(row['q1_peak_accel_g'], 1.5)
        self.assertAlmostEqual(row['q2_peak_decel_g_abs'], 3.5)
        self.assertIsNone(row['q3_peak_accel_g'])

    def test_driver_with_no_lap_metrics_produces_no_row(self):
        client = _mock_client()
        # driver_lap_metrics has no data for driver 44
        ingest_qualifying_peak_g_summary(client, 9000, {}, {44: {'Q1': (89.5, 2)}})
        client.table.assert_not_called()

    def test_phase_with_missing_lap_number_skipped_gracefully(self):
        client = _mock_client()
        # best lap number is None (lap_duration was missing)
        best_per_phase = {44: {'Q1': (89.5, None)}}
        driver_lap_metrics = {44: {2: {'peak_accel_g': 1.5, 'peak_decel_g_abs': 3.2}}}
        ingest_qualifying_peak_g_summary(client, 9000, driver_lap_metrics, best_per_phase)
        # All phase G values are None → no row upserted
        client.table.assert_not_called()


# ===========================================================================
# recompute_lap_metrics — qualifying path
# ===========================================================================

class TestRecomputeLapMetricsQualifyingPath(unittest.TestCase):

    @patch("ingest.ingest_brake_entry_speed_ranks")
    @patch("ingest.ingest_qualifying_peak_g_summary")
    @patch("ingest.ingest_lap_metrics")
    @patch("ingest._compute_qualifying_best_per_phase")
    @patch("ingest._assign_qualifying_phases")
    @patch("ingest.ingest_session_sector_bests")
    @patch("ingest.ingest_lap_flags")
    def test_qualifying_calls_correct_functions(
        self, mock_ilf, mock_issb, mock_aqp, mock_cqbp, mock_ilm, mock_iqpg, mock_besr
    ):
        from ingest import recompute_lap_metrics
        mock_aqp.return_value = []
        mock_cqbp.return_value = {}
        mock_ilm.return_value = {}
        client = _mock_client()
        laps = []
        rc_rows = [{'qualifying_phase': 1, 'date': '2024-03-01T09:00:00'}]

        recompute_lap_metrics(client, 9000, laps, 'qualifying', rc_rows=rc_rows)

        mock_ilf.assert_called_once()
        mock_issb.assert_called_once()
        mock_aqp.assert_called_once_with(laps, rc_rows)
        mock_cqbp.assert_called_once()
        mock_ilm.assert_called_once()
        mock_iqpg.assert_called_once()
        mock_besr.assert_called_once()

    @patch("ingest.ingest_brake_entry_speed_ranks")
    @patch("ingest.ingest_race_peak_g_summary")
    @patch("ingest.ingest_lap_metrics")
    @patch("ingest.ingest_session_sector_bests")
    @patch("ingest.ingest_lap_flags")
    def test_race_does_not_call_qualifying_functions(
        self, mock_ilf, mock_issb, mock_ilm, mock_rpgs, mock_besr
    ):
        from ingest import recompute_lap_metrics
        mock_ilm.return_value = {}
        client = _mock_client()

        with patch("ingest.ingest_qualifying_peak_g_summary") as mock_iqpg:
            recompute_lap_metrics(client, 9000, [], 'race')
            mock_ilf.assert_called_once()
            mock_issb.assert_called_once()
            mock_iqpg.assert_not_called()
            mock_rpgs.assert_called_once()
            mock_besr.assert_called_once()

    @patch("ingest.ingest_lap_metrics")
    def test_practice_session_skipped(self, mock_ilm):
        from ingest import recompute_lap_metrics
        client = _mock_client()
        recompute_lap_metrics(client, 9000, [], 'practice')
        mock_ilm.assert_not_called()


# ===========================================================================
# ingest_session_sector_bests
# ===========================================================================

class TestIngestSessionSectorBests(unittest.TestCase):

    def _lap(self, driver_number=44, lap_number=1,
             s1=30.0, s2=40.0, s3=25.0):
        return {
            'driver_number':    driver_number,
            'lap_number':       lap_number,
            'duration_sector_1': s1,
            'duration_sector_2': s2,
            'duration_sector_3': s3,
        }

    def _bests_row(self, client):
        """Return the row upserted to session_sector_bests."""
        calls = client.table.return_value.upsert.call_args_list
        # session_sector_bests is upserted first (single-row list)
        for c in calls:
            rows = c.args[0]
            if len(rows) == 1 and 'best_s1' in rows[0]:
                return rows[0]
        return None

    def _delta_rows(self, client):
        """Return the rows upserted to lap_metrics for deltas."""
        calls = client.table.return_value.upsert.call_args_list
        for c in calls:
            rows = c.args[0]
            if rows and 'delta_to_session_best_s1' in rows[0]:
                return rows
        return []

    def test_best_times_are_minimums_across_laps(self):
        laps = [
            self._lap(driver_number=44, s1=30.0, s2=40.0, s3=25.0),
            self._lap(driver_number=63, s1=29.5, s2=41.0, s3=24.8),
        ]
        client = _mock_client()
        ingest_session_sector_bests(client, 9000, laps)
        row = self._bests_row(client)
        self.assertAlmostEqual(row['best_s1'], 29.5)
        self.assertAlmostEqual(row['best_s2'], 40.0)
        self.assertAlmostEqual(row['best_s3'], 24.8)

    def test_best_driver_correctly_identified(self):
        laps = [
            self._lap(driver_number=44, s1=30.0, s2=40.0, s3=25.0),
            self._lap(driver_number=63, s1=29.5, s2=41.0, s3=24.8),
        ]
        client = _mock_client()
        ingest_session_sector_bests(client, 9000, laps)
        row = self._bests_row(client)
        self.assertEqual(row['best_s1_driver'], 63)
        self.assertEqual(row['best_s2_driver'], 44)
        self.assertEqual(row['best_s3_driver'], 63)

    def test_theoretical_best_is_sum_of_sector_bests(self):
        laps = [
            self._lap(driver_number=44, s1=30.0, s2=40.0, s3=25.0),
            self._lap(driver_number=63, s1=29.5, s2=39.5, s3=24.8),
        ]
        client = _mock_client()
        ingest_session_sector_bests(client, 9000, laps)
        row = self._bests_row(client)
        # theoretical = 29.5 + 39.5 + 24.8 = 93.8
        self.assertAlmostEqual(row['theoretical_best_lap'], 93.8, places=3)

    def test_theoretical_best_none_when_any_sector_missing(self):
        laps = [self._lap(s1=30.0, s2=None, s3=25.0)]
        client = _mock_client()
        ingest_session_sector_bests(client, 9000, laps)
        row = self._bests_row(client)
        self.assertIsNone(row['theoretical_best_lap'])

    def test_zero_sector_time_excluded_from_best(self):
        """Zero and negative times are invalid (incomplete lap) — skip."""
        laps = [
            self._lap(driver_number=44, s1=0.0, s2=40.0, s3=25.0),
            self._lap(driver_number=63, s1=30.0, s2=39.0, s3=24.0),
        ]
        client = _mock_client()
        ingest_session_sector_bests(client, 9000, laps)
        row = self._bests_row(client)
        # zero s1 excluded → only driver 63's s1 counts
        self.assertAlmostEqual(row['best_s1'], 30.0)
        self.assertEqual(row['best_s1_driver'], 63)

    def test_none_sector_time_excluded_from_best(self):
        laps = [
            self._lap(driver_number=44, s1=None, s2=40.0, s3=25.0),
            self._lap(driver_number=63, s1=30.0, s2=39.0, s3=24.0),
        ]
        client = _mock_client()
        ingest_session_sector_bests(client, 9000, laps)
        row = self._bests_row(client)
        self.assertAlmostEqual(row['best_s1'], 30.0)

    def test_no_valid_laps_all_bests_none(self):
        """All sector times missing → row still upserted with nulls."""
        client = _mock_client()
        ingest_session_sector_bests(client, 9000, [self._lap(s1=None, s2=None, s3=None)])
        row = self._bests_row(client)
        self.assertIsNone(row['best_s1'])
        self.assertIsNone(row['best_s2'])
        self.assertIsNone(row['best_s3'])
        self.assertIsNone(row['theoretical_best_lap'])

    def test_empty_laps_still_upserts_null_row(self):
        client = _mock_client()
        ingest_session_sector_bests(client, 9000, [])
        row = self._bests_row(client)
        self.assertIsNotNone(row)
        self.assertEqual(row['session_key'], 9000)

    def test_missing_driver_number_excluded_from_bests(self):
        laps = [
            self._lap(driver_number=None, s1=20.0, s2=30.0, s3=15.0),
            self._lap(driver_number=44,   s1=30.0, s2=40.0, s3=25.0),
        ]
        client = _mock_client()
        ingest_session_sector_bests(client, 9000, laps)
        row = self._bests_row(client)
        # the None-driver lap's suspiciously fast times must not win
        self.assertAlmostEqual(row['best_s1'], 30.0)

    def test_delta_zero_for_session_best_holder(self):
        """Driver who set the session best has delta = 0."""
        laps = [
            self._lap(driver_number=44, lap_number=1, s1=29.5, s2=39.5, s3=24.8),
            self._lap(driver_number=63, lap_number=1, s1=30.0, s2=40.0, s3=25.0),
        ]
        client = _mock_client()
        ingest_session_sector_bests(client, 9000, laps)
        deltas = self._delta_rows(client)
        by_driver = {r['driver_number']: r for r in deltas}
        self.assertAlmostEqual(by_driver[44]['delta_to_session_best_s1'], 0.0)
        self.assertAlmostEqual(by_driver[44]['delta_to_session_best_s2'], 0.0)
        self.assertAlmostEqual(by_driver[44]['delta_to_session_best_s3'], 0.0)

    def test_delta_positive_for_slower_lap(self):
        laps = [
            self._lap(driver_number=44, lap_number=1, s1=29.5, s2=39.5, s3=24.8),
            self._lap(driver_number=63, lap_number=1, s1=30.0, s2=40.0, s3=25.0),
        ]
        client = _mock_client()
        ingest_session_sector_bests(client, 9000, laps)
        deltas = self._delta_rows(client)
        by_driver = {r['driver_number']: r for r in deltas}
        self.assertAlmostEqual(by_driver[63]['delta_to_session_best_s1'], 0.5, places=3)
        self.assertAlmostEqual(by_driver[63]['delta_to_session_best_s2'], 0.5, places=3)
        self.assertAlmostEqual(by_driver[63]['delta_to_session_best_s3'], 0.2, places=3)

    def test_delta_none_when_lap_sector_time_missing(self):
        laps = [
            self._lap(driver_number=44, lap_number=1, s1=None, s2=40.0, s3=25.0),
            self._lap(driver_number=63, lap_number=1, s1=30.0, s2=39.0, s3=24.0),
        ]
        client = _mock_client()
        ingest_session_sector_bests(client, 9000, laps)
        deltas = self._delta_rows(client)
        by_driver = {r['driver_number']: r for r in deltas}
        self.assertIsNone(by_driver[44]['delta_to_session_best_s1'])
        # s2 is present — delta should be non-None
        self.assertIsNotNone(by_driver[44]['delta_to_session_best_s2'])

    def test_upserts_to_correct_tables(self):
        client = _mock_client()
        laps = [self._lap(driver_number=44, lap_number=1)]
        ingest_session_sector_bests(client, 9000, laps)
        called_tables = [c.args[0] for c in client.table.call_args_list]
        self.assertIn('session_sector_bests', called_tables)
        self.assertIn('lap_metrics', called_tables)

    def test_delta_row_count_matches_valid_laps(self):
        laps = [
            self._lap(driver_number=44, lap_number=1),
            self._lap(driver_number=44, lap_number=2),
            self._lap(driver_number=63, lap_number=1),
        ]
        client = _mock_client()
        ingest_session_sector_bests(client, 9000, laps)
        self.assertEqual(len(self._delta_rows(client)), 3)

    def test_laps_missing_driver_excluded_from_delta_rows(self):
        laps = [
            self._lap(driver_number=None, lap_number=1),
            self._lap(driver_number=44,   lap_number=2),
        ]
        client = _mock_client()
        ingest_session_sector_bests(client, 9000, laps)
        self.assertEqual(len(self._delta_rows(client)), 1)


# ===========================================================================
# ingest_lap_flags
# ===========================================================================

class TestIngestLapFlags(unittest.TestCase):
    """Tests for is_neutralized and tyre_age_at_lap population."""

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _lap(self, driver_number=44, lap_number=5,
             date_start='2024-03-02T13:00:00Z', lap_duration=90.0):
        return {
            'driver_number': driver_number,
            'lap_number':    lap_number,
            'date_start':    date_start,
            'lap_duration':  lap_duration,
        }

    def _rc(self, date, category='SafetyCar', flag=None, message=None):
        return {'date': date, 'category': category, 'flag': flag, 'message': message}

    def _stint(self, driver_number=44, lap_start=1, lap_end=20,
               tyre_age_at_start=0):
        return {
            'driver_number':    driver_number,
            'lap_start':        lap_start,
            'lap_end':          lap_end,
            'tyre_age_at_start': tyre_age_at_start,
        }

    def _upserted(self, client):
        return client.table.return_value.upsert.call_args.args[0]

    # ------------------------------------------------------------------
    # is_neutralized
    # ------------------------------------------------------------------

    def test_sc_period_overlapping_lap_is_neutralized_true(self):
        """SC DEPLOYED within lap, IN THIS LAP after lap end → lap is neutralized."""
        client = _mock_client()
        # lap: 13:00:00 → 13:01:30; SC deployed at 13:00:30, ends 13:02:00
        ingest_lap_flags(
            client, 9000,
            laps=[self._lap()],
            stints_rows=[],
            rc_rows=[
                self._rc('2024-03-02T13:00:30Z', category='SafetyCar',
                         message='SAFETY CAR DEPLOYED'),
                self._rc('2024-03-02T13:02:00Z', category='SafetyCar',
                         message='SAFETY CAR IN THIS LAP'),
            ],
        )
        self.assertTrue(self._upserted(client)[0]['is_neutralized'])

    def test_vsc_period_overlapping_lap_is_neutralized_true(self):
        """VSC DEPLOYED within lap, ENDING after lap end → lap is neutralized."""
        client = _mock_client()
        ingest_lap_flags(
            client, 9000,
            laps=[self._lap()],
            stints_rows=[],
            rc_rows=[
                self._rc('2024-03-02T13:00:45Z', category='SafetyCar',
                         message='VIRTUAL SAFETY CAR DEPLOYED'),
                self._rc('2024-03-02T13:02:00Z', category='SafetyCar',
                         message='VIRTUAL SAFETY CAR ENDING'),
            ],
        )
        self.assertTrue(self._upserted(client)[0]['is_neutralized'])

    def test_red_flag_mid_lap_is_neutralized_true(self):
        """flag='RED' triggers neutralized regardless of category."""
        client = _mock_client()
        ingest_lap_flags(
            client, 9000,
            laps=[self._lap()],
            stints_rows=[],
            rc_rows=[self._rc('2024-03-02T13:00:10Z', category='Flag', flag='RED')],
        )
        self.assertTrue(self._upserted(client)[0]['is_neutralized'])

    def test_sc_period_ends_before_lap_not_neutralized(self):
        """SC period entirely before lap (IN THIS LAP at 12:59:59) → not neutralized."""
        client = _mock_client()
        # lap: 13:00:00 → 13:01:30; SC period: 12:58:00 → 12:59:59
        ingest_lap_flags(
            client, 9000,
            laps=[self._lap()],
            stints_rows=[],
            rc_rows=[
                self._rc('2024-03-02T12:58:00Z', category='SafetyCar',
                         message='SAFETY CAR DEPLOYED'),
                self._rc('2024-03-02T12:59:59Z', category='SafetyCar',
                         message='SAFETY CAR IN THIS LAP'),
            ],
        )
        self.assertFalse(self._upserted(client)[0]['is_neutralized'])

    def test_sc_period_starts_after_lap_not_neutralized(self):
        """SC period entirely after lap (DEPLOYED at 13:01:31) → not neutralized."""
        client = _mock_client()
        # lap ends at 13:01:30Z; SC starts 13:01:31Z
        ingest_lap_flags(
            client, 9000,
            laps=[self._lap()],
            stints_rows=[],
            rc_rows=[
                self._rc('2024-03-02T13:01:31Z', category='SafetyCar',
                         message='SAFETY CAR DEPLOYED'),
                self._rc('2024-03-02T13:03:00Z', category='SafetyCar',
                         message='SAFETY CAR IN THIS LAP'),
            ],
        )
        self.assertFalse(self._upserted(client)[0]['is_neutralized'])

    def test_no_neutralising_events_is_neutralized_false(self):
        client = _mock_client()
        ingest_lap_flags(
            client, 9000,
            laps=[self._lap()],
            stints_rows=[],
            rc_rows=[],
        )
        self.assertFalse(self._upserted(client)[0]['is_neutralized'])

    def test_non_neutralising_category_ignored(self):
        """DRS-enable or Flag/GREEN events must not trigger neutralized."""
        client = _mock_client()
        ingest_lap_flags(
            client, 9000,
            laps=[self._lap()],
            stints_rows=[],
            rc_rows=[
                self._rc('2024-03-02T13:00:30Z', category='Drs', flag='ENABLED'),
                self._rc('2024-03-02T13:00:40Z', category='Flag', flag='GREEN'),
            ],
        )
        self.assertFalse(self._upserted(client)[0]['is_neutralized'])

    def test_missing_lap_duration_is_neutralized_none(self):
        """Cannot compute window without lap_duration → None."""
        client = _mock_client()
        ingest_lap_flags(
            client, 9000,
            laps=[self._lap(lap_duration=None)],
            stints_rows=[],
            rc_rows=[self._rc('2024-03-02T13:00:30Z')],
        )
        self.assertIsNone(self._upserted(client)[0]['is_neutralized'])

    def test_missing_date_start_is_neutralized_none(self):
        client = _mock_client()
        ingest_lap_flags(
            client, 9000,
            laps=[self._lap(date_start=None)],
            stints_rows=[],
            rc_rows=[self._rc('2024-03-02T13:00:30Z')],
        )
        self.assertIsNone(self._upserted(client)[0]['is_neutralized'])

    def test_sc_period_starting_at_lap_start_is_neutralized_true(self):
        """Boundary: SC DEPLOYED exactly at lap start timestamp → neutralized."""
        client = _mock_client()
        # lap: 13:00:00 → 13:01:30; SC period starts at lap start
        ingest_lap_flags(
            client, 9000,
            laps=[self._lap()],
            stints_rows=[],
            rc_rows=[
                self._rc('2024-03-02T13:00:00Z', category='SafetyCar',
                         message='SAFETY CAR DEPLOYED'),
                self._rc('2024-03-02T13:02:00Z', category='SafetyCar',
                         message='SAFETY CAR IN THIS LAP'),
            ],
        )
        self.assertTrue(self._upserted(client)[0]['is_neutralized'])

    def test_sc_period_ending_at_lap_end_is_neutralized_true(self):
        """Boundary: SC IN THIS LAP exactly at lap end timestamp → neutralized."""
        client = _mock_client()
        # lap ends at 13:01:30Z; SC period ends at that exact moment
        ingest_lap_flags(
            client, 9000,
            laps=[self._lap()],
            stints_rows=[],
            rc_rows=[
                self._rc('2024-03-02T12:59:00Z', category='SafetyCar',
                         message='SAFETY CAR DEPLOYED'),
                self._rc('2024-03-02T13:01:30Z', category='SafetyCar',
                         message='SAFETY CAR IN THIS LAP'),
            ],
        )
        self.assertTrue(self._upserted(client)[0]['is_neutralized'])

    # ------------------------------------------------------------------
    # tyre_age_at_lap
    # ------------------------------------------------------------------

    def test_tyre_age_mid_stint(self):
        """lap_num=8, lap_start=3, tyre_age_at_start=5 → age=10."""
        client = _mock_client()
        ingest_lap_flags(
            client, 9000,
            laps=[self._lap(lap_number=8, date_start=None, lap_duration=None)],
            stints_rows=[self._stint(lap_start=3, lap_end=20, tyre_age_at_start=5)],
            rc_rows=[],
        )
        self.assertEqual(self._upserted(client)[0]['tyre_age_at_lap'], 10)

    def test_tyre_age_first_lap_of_stint(self):
        """Lap exactly at stint start → tyre_age = tyre_age_at_start."""
        client = _mock_client()
        ingest_lap_flags(
            client, 9000,
            laps=[self._lap(lap_number=5, date_start=None, lap_duration=None)],
            stints_rows=[self._stint(lap_start=5, lap_end=25, tyre_age_at_start=3)],
            rc_rows=[],
        )
        self.assertEqual(self._upserted(client)[0]['tyre_age_at_lap'], 3)

    def test_tyre_age_open_ended_stint(self):
        """lap_end=None (last stint) — lap is still matched."""
        client = _mock_client()
        ingest_lap_flags(
            client, 9000,
            laps=[self._lap(lap_number=50, date_start=None, lap_duration=None)],
            stints_rows=[self._stint(lap_start=30, lap_end=None, tyre_age_at_start=0)],
            rc_rows=[],
        )
        self.assertEqual(self._upserted(client)[0]['tyre_age_at_lap'], 20)

    def test_tyre_age_no_matching_stint_returns_none(self):
        """Lap before any stint → None (e.g. OpenF1 sprint data gap)."""
        client = _mock_client()
        ingest_lap_flags(
            client, 9000,
            laps=[self._lap(lap_number=2, date_start=None, lap_duration=None)],
            stints_rows=[self._stint(lap_start=5, lap_end=20, tyre_age_at_start=0)],
            rc_rows=[],
        )
        self.assertIsNone(self._upserted(client)[0]['tyre_age_at_lap'])

    def test_tyre_age_correct_stint_chosen_when_multiple(self):
        """Driver has two stints — correct one is selected by lap_number."""
        client = _mock_client()
        stints = [
            self._stint(lap_start=1,  lap_end=20, tyre_age_at_start=0),
            self._stint(lap_start=21, lap_end=55, tyre_age_at_start=0),
        ]
        ingest_lap_flags(
            client, 9000,
            laps=[self._lap(lap_number=25, date_start=None, lap_duration=None)],
            stints_rows=stints,
            rc_rows=[],
        )
        # age = 0 + (25 - 21) = 4
        self.assertEqual(self._upserted(client)[0]['tyre_age_at_lap'], 4)

    def test_tyre_age_none_when_stint_missing_age_at_start(self):
        """Matching stint exists but tyre_age_at_start is None → None."""
        client = _mock_client()
        ingest_lap_flags(
            client, 9000,
            laps=[self._lap(lap_number=5, date_start=None, lap_duration=None)],
            stints_rows=[self._stint(lap_start=1, lap_end=20, tyre_age_at_start=None)],
            rc_rows=[],
        )
        self.assertIsNone(self._upserted(client)[0]['tyre_age_at_lap'])

    # ------------------------------------------------------------------
    # row structure and filtering
    # ------------------------------------------------------------------

    def test_upserts_to_lap_metrics_table(self):
        client = _mock_client()
        ingest_lap_flags(client, 9000, laps=[self._lap()],
                         stints_rows=[], rc_rows=[])
        client.table.assert_called_with('lap_metrics')

    def test_row_contains_required_keys(self):
        client = _mock_client()
        ingest_lap_flags(client, 9000, laps=[self._lap()],
                         stints_rows=[], rc_rows=[])
        row = self._upserted(client)[0]
        for key in ('session_key', 'driver_number', 'lap_number',
                    'is_neutralized', 'tyre_age_at_lap'):
            self.assertIn(key, row)

    def test_missing_driver_number_filtered(self):
        client = _mock_client()
        ingest_lap_flags(client, 9000,
                         laps=[self._lap(driver_number=None)],
                         stints_rows=[], rc_rows=[])
        client.table.assert_not_called()

    def test_missing_lap_number_filtered(self):
        client = _mock_client()
        ingest_lap_flags(client, 9000,
                         laps=[self._lap(lap_number=None)],
                         stints_rows=[], rc_rows=[])
        client.table.assert_not_called()

    def test_empty_laps_no_upsert(self):
        client = _mock_client()
        ingest_lap_flags(client, 9000, laps=[], stints_rows=[], rc_rows=[])
        client.table.assert_not_called()

    def test_row_count_matches_valid_laps(self):
        client = _mock_client()
        laps = [
            self._lap(driver_number=44, lap_number=1),
            self._lap(driver_number=44, lap_number=2),
            self._lap(driver_number=63, lap_number=1),
        ]
        ingest_lap_flags(client, 9000, laps=laps, stints_rows=[], rc_rows=[])
        self.assertEqual(len(self._upserted(client)), 3)

    # ------------------------------------------------------------------
    # period-based neutralization (sprint SC fix)
    # ------------------------------------------------------------------

    def test_lap_entirely_within_sc_period_is_neutralized(self):
        """Core fix: lap entirely within SC period (no event inside the lap) → True.

        Reproduces the sprint scenario where SC is deployed before a pit-stop
        and laps run under the SC have no new RC event within their own window.
        """
        client = _mock_client()
        # SC deployed at 12:58:00, IN THIS LAP at 13:02:00.
        # Lap runs 13:00:00 → 13:01:30 — entirely within the SC period.
        ingest_lap_flags(
            client, 9000,
            laps=[self._lap()],
            stints_rows=[],
            rc_rows=[
                self._rc('2024-03-02T12:58:00Z', category='SafetyCar',
                         message='SAFETY CAR DEPLOYED'),
                self._rc('2024-03-02T13:02:00Z', category='SafetyCar',
                         message='SAFETY CAR IN THIS LAP'),
            ],
        )
        self.assertTrue(self._upserted(client)[0]['is_neutralized'])

    def test_restart_lap_after_sc_in_this_lap_not_neutralized(self):
        """Lap starting after SC IN THIS LAP timestamp → not neutralized (racing lap)."""
        client = _mock_client()
        # SC period: 12:58:00 → 12:59:30.  Lap starts at 13:00:00 (after end).
        ingest_lap_flags(
            client, 9000,
            laps=[self._lap()],
            stints_rows=[],
            rc_rows=[
                self._rc('2024-03-02T12:58:00Z', category='SafetyCar',
                         message='SAFETY CAR DEPLOYED'),
                self._rc('2024-03-02T12:59:30Z', category='SafetyCar',
                         message='SAFETY CAR IN THIS LAP'),
            ],
        )
        self.assertFalse(self._upserted(client)[0]['is_neutralized'])

    def test_multiple_sc_periods_any_overlap_neutralizes(self):
        """Two SC periods — lap overlapping the second is neutralized."""
        client = _mock_client()
        # First SC: 12:50 → 12:55 (before lap).  Second SC: 13:00:30 → 13:02:00 (overlaps lap).
        ingest_lap_flags(
            client, 9000,
            laps=[self._lap()],
            stints_rows=[],
            rc_rows=[
                self._rc('2024-03-02T12:50:00Z', category='SafetyCar',
                         message='SAFETY CAR DEPLOYED'),
                self._rc('2024-03-02T12:55:00Z', category='SafetyCar',
                         message='SAFETY CAR IN THIS LAP'),
                self._rc('2024-03-02T13:00:30Z', category='SafetyCar',
                         message='SAFETY CAR DEPLOYED'),
                self._rc('2024-03-02T13:02:00Z', category='SafetyCar',
                         message='SAFETY CAR IN THIS LAP'),
            ],
        )
        self.assertTrue(self._upserted(client)[0]['is_neutralized'])

    def test_vsc_lap_entirely_within_period_is_neutralized(self):
        """Lap entirely within a VSC period → neutralized."""
        client = _mock_client()
        ingest_lap_flags(
            client, 9000,
            laps=[self._lap()],
            stints_rows=[],
            rc_rows=[
                self._rc('2024-03-02T12:58:00Z', category='SafetyCar',
                         message='VIRTUAL SAFETY CAR DEPLOYED'),
                self._rc('2024-03-02T13:02:00Z', category='SafetyCar',
                         message='VIRTUAL SAFETY CAR ENDING'),
            ],
        )
        self.assertTrue(self._upserted(client)[0]['is_neutralized'])

    def test_sc_without_end_message_not_neutralized(self):
        """SC DEPLOYED with no matching IN THIS LAP → no period formed → not neutralized."""
        client = _mock_client()
        ingest_lap_flags(
            client, 9000,
            laps=[self._lap()],
            stints_rows=[],
            rc_rows=[
                self._rc('2024-03-02T12:58:00Z', category='SafetyCar',
                         message='SAFETY CAR DEPLOYED'),
            ],
        )
        self.assertFalse(self._upserted(client)[0]['is_neutralized'])


# ===========================================================================
# _build_neutralized_periods
# ===========================================================================

class TestBuildNeutralizedPeriods(unittest.TestCase):
    """Tests for the SC/VSC period parsing helper."""

    def _rc(self, date, category='SafetyCar', flag=None, message=None):
        return {'date': date, 'category': category, 'flag': flag, 'message': message}

    def _parse(self, rc_rows):
        import pandas as pd
        return _build_neutralized_periods(rc_rows, pd)

    def test_sc_period_parsed(self):
        """DEPLOYED + IN THIS LAP → one (start, end) tuple."""
        periods = self._parse([
            self._rc('2024-03-02T13:00:00Z', message='SAFETY CAR DEPLOYED'),
            self._rc('2024-03-02T13:05:00Z', message='SAFETY CAR IN THIS LAP'),
        ])
        self.assertEqual(len(periods), 1)
        self.assertLess(periods[0][0], periods[0][1])

    def test_vsc_period_parsed(self):
        """VSC DEPLOYED + ENDING → one (start, end) tuple."""
        periods = self._parse([
            self._rc('2024-03-02T13:00:00Z', message='VIRTUAL SAFETY CAR DEPLOYED'),
            self._rc('2024-03-02T13:03:00Z', message='VIRTUAL SAFETY CAR ENDING'),
        ])
        self.assertEqual(len(periods), 1)
        self.assertLess(periods[0][0], periods[0][1])

    def test_red_flag_as_zero_width_period(self):
        """flag='RED' → (ts, ts) zero-width interval."""
        periods = self._parse([
            self._rc('2024-03-02T13:00:00Z', category='Flag', flag='RED'),
        ])
        self.assertEqual(len(periods), 1)
        self.assertEqual(periods[0][0], periods[0][1])

    def test_sc_without_end_not_included(self):
        """DEPLOYED with no matching IN THIS LAP → period silently skipped."""
        periods = self._parse([
            self._rc('2024-03-02T13:00:00Z', message='SAFETY CAR DEPLOYED'),
        ])
        self.assertEqual(periods, [])

    def test_vsc_without_end_not_included(self):
        """VSC DEPLOYED with no matching ENDING → silently skipped."""
        periods = self._parse([
            self._rc('2024-03-02T13:00:00Z', message='VIRTUAL SAFETY CAR DEPLOYED'),
        ])
        self.assertEqual(periods, [])

    def test_multiple_sc_periods(self):
        """Two SC deployments produce two separate periods."""
        periods = self._parse([
            self._rc('2024-03-02T12:50:00Z', message='SAFETY CAR DEPLOYED'),
            self._rc('2024-03-02T12:55:00Z', message='SAFETY CAR IN THIS LAP'),
            self._rc('2024-03-02T13:10:00Z', message='SAFETY CAR DEPLOYED'),
            self._rc('2024-03-02T13:15:00Z', message='SAFETY CAR IN THIS LAP'),
        ])
        self.assertEqual(len(periods), 2)

    def test_vsc_not_matched_as_sc(self):
        """VIRTUAL SAFETY CAR DEPLOYED does not match the SC DEPLOYED branch."""
        periods = self._parse([
            self._rc('2024-03-02T13:00:00Z', message='VIRTUAL SAFETY CAR DEPLOYED'),
            self._rc('2024-03-02T13:03:00Z', message='SAFETY CAR IN THIS LAP'),
        ])
        # 'IN THIS LAP' should not pair with a VSC DEPLOYED
        self.assertEqual(periods, [])

    def test_unsorted_rows_sorted_by_timestamp(self):
        """Events out of order are sorted before pairing."""
        periods = self._parse([
            self._rc('2024-03-02T13:05:00Z', message='SAFETY CAR IN THIS LAP'),
            self._rc('2024-03-02T13:00:00Z', message='SAFETY CAR DEPLOYED'),
        ])
        self.assertEqual(len(periods), 1)
        self.assertLess(periods[0][0], periods[0][1])

    def test_empty_rows_returns_empty(self):
        self.assertEqual(self._parse([]), [])

    def test_non_neutralising_events_ignored(self):
        """DRS, green flag, yellow flag events produce no periods."""
        periods = self._parse([
            self._rc('2024-03-02T13:00:00Z', category='Drs', flag='ENABLED'),
            self._rc('2024-03-02T13:00:10Z', category='Flag', flag='GREEN'),
            self._rc('2024-03-02T13:00:20Z', category='Flag', flag='YELLOW'),
        ])
        self.assertEqual(periods, [])


# ===========================================================================
# ingest_brake_entry_speed_ranks
# ===========================================================================

class TestIngestBrakeEntrySpeedRanks(unittest.TestCase):

    def _make_dlm(self, driver_speeds):
        """Build a driver_lap_metrics dict from {dn: [(lap_num, lap, s1, s2, s3), ...]}.

        Pass None for any sector value to simulate missing data.
        """
        dlm = {}
        for dn, laps in driver_speeds.items():
            dlm[dn] = {}
            for lap_num, lap, s1, s2, s3 in laps:
                dlm[dn][lap_num] = {
                    'speed_at_brake_start_kph_lap': lap,
                    'speed_at_brake_start_kph_s1':  s1,
                    'speed_at_brake_start_kph_s2':  s2,
                    'speed_at_brake_start_kph_s3':  s3,
                }
        return dlm

    def test_empty_dict_no_upsert(self):
        client = _mock_client()
        ingest_brake_entry_speed_ranks(client, 9000, {})
        client.table.assert_not_called()

    def test_single_entry_insufficient_stats_all_none(self):
        """Only one value per sector → std undefined → all rank/z/category are None."""
        dlm = self._make_dlm({44: [(1, 250.0, 180.0, 200.0, 220.0)]})
        client = _mock_client()
        ingest_brake_entry_speed_ranks(client, 9000, dlm)
        upserted = client.table.return_value.upsert.call_args.args[0]
        self.assertEqual(len(upserted), 1)
        row = upserted[0]
        self.assertIsNone(row['brake_entry_speed_pct_rank_lap'])
        self.assertIsNone(row['brake_entry_speed_z_score_lap'])
        self.assertIsNone(row['brake_entry_speed_category_lap'])

    def test_row_contains_session_and_driver_keys(self):
        dlm = self._make_dlm({44: [(1, 250.0, None, None, None)]})
        client = _mock_client()
        ingest_brake_entry_speed_ranks(client, 9999, dlm)
        upserted = client.table.return_value.upsert.call_args.args[0]
        row = upserted[0]
        self.assertEqual(row['session_key'], 9999)
        self.assertEqual(row['driver_number'], 44)
        self.assertEqual(row['lap_number'], 1)

    def test_three_drivers_correct_categories(self):
        """Low speed → 'early', mid → 'average', high → 'late'."""
        dlm = self._make_dlm({
            1:  [(1, 100.0, None, None, None)],
            44: [(1, 200.0, None, None, None)],
            63: [(1, 300.0, None, None, None)],
        })
        client = _mock_client()
        ingest_brake_entry_speed_ranks(client, 9000, dlm)
        upserted = client.table.return_value.upsert.call_args.args[0]
        by_driver = {r['driver_number']: r for r in upserted}
        self.assertEqual(by_driver[1]['brake_entry_speed_category_lap'], 'early')
        self.assertEqual(by_driver[44]['brake_entry_speed_category_lap'], 'average')
        self.assertEqual(by_driver[63]['brake_entry_speed_category_lap'], 'late')

    def test_three_drivers_ranks_ordered(self):
        dlm = self._make_dlm({
            1:  [(1, 100.0, None, None, None)],
            44: [(1, 200.0, None, None, None)],
            63: [(1, 300.0, None, None, None)],
        })
        client = _mock_client()
        ingest_brake_entry_speed_ranks(client, 9000, dlm)
        upserted = client.table.return_value.upsert.call_args.args[0]
        by_driver = {r['driver_number']: r for r in upserted}
        r1  = by_driver[1]['brake_entry_speed_pct_rank_lap']
        r44 = by_driver[44]['brake_entry_speed_pct_rank_lap']
        r63 = by_driver[63]['brake_entry_speed_pct_rank_lap']
        self.assertLess(r1, r44)
        self.assertLess(r44, r63)

    def test_z_score_sign_correct(self):
        """Driver with speed below mean has negative z-score; above mean positive."""
        dlm = self._make_dlm({
            1:  [(1, 100.0, None, None, None)],
            44: [(1, 300.0, None, None, None)],
        })
        client = _mock_client()
        ingest_brake_entry_speed_ranks(client, 9000, dlm)
        upserted = client.table.return_value.upsert.call_args.args[0]
        by_driver = {r['driver_number']: r for r in upserted}
        self.assertLess(by_driver[1]['brake_entry_speed_z_score_lap'], 0)
        self.assertGreater(by_driver[44]['brake_entry_speed_z_score_lap'], 0)

    def test_z_score_symmetric_two_drivers(self):
        """Two drivers equidistant from mean → z-scores should be equal in magnitude."""
        dlm = self._make_dlm({
            1:  [(1, 100.0, None, None, None)],
            44: [(1, 300.0, None, None, None)],
        })
        client = _mock_client()
        ingest_brake_entry_speed_ranks(client, 9000, dlm)
        upserted = client.table.return_value.upsert.call_args.args[0]
        by_driver = {r['driver_number']: r for r in upserted}
        z1  = by_driver[1]['brake_entry_speed_z_score_lap']
        z44 = by_driver[44]['brake_entry_speed_z_score_lap']
        self.assertAlmostEqual(abs(z1), abs(z44), places=4)

    def test_tied_values_same_rank(self):
        """Tied brake entry speeds should produce the same percentile rank."""
        dlm = self._make_dlm({
            1:  [(1, 200.0, None, None, None)],
            44: [(1, 200.0, None, None, None)],
            63: [(1, 300.0, None, None, None)],
        })
        client = _mock_client()
        ingest_brake_entry_speed_ranks(client, 9000, dlm)
        upserted = client.table.return_value.upsert.call_args.args[0]
        by_driver = {r['driver_number']: r for r in upserted}
        self.assertEqual(
            by_driver[1]['brake_entry_speed_pct_rank_lap'],
            by_driver[44]['brake_entry_speed_pct_rank_lap'],
        )

    def test_none_sector_value_produces_none_outputs(self):
        """If a driver's sector value is None, all three outputs are None for that sector."""
        dlm = self._make_dlm({
            1:  [(1, 200.0, None, None, None)],
            44: [(1, 300.0, None, None, None)],
        })
        client = _mock_client()
        ingest_brake_entry_speed_ranks(client, 9000, dlm)
        upserted = client.table.return_value.upsert.call_args.args[0]
        by_driver = {r['driver_number']: r for r in upserted}
        for sector in ('s1', 's2', 's3'):
            self.assertIsNone(by_driver[1][f'brake_entry_speed_pct_rank_{sector}'])
            self.assertIsNone(by_driver[1][f'brake_entry_speed_z_score_{sector}'])
            self.assertIsNone(by_driver[1][f'brake_entry_speed_category_{sector}'])

    def test_all_sectors_computed_when_present(self):
        """All four sector columns populated when valid data exists for two+ drivers."""
        dlm = self._make_dlm({
            1:  [(1, 200.0, 120.0, 150.0, 180.0)],
            44: [(1, 300.0, 140.0, 170.0, 200.0)],
        })
        client = _mock_client()
        ingest_brake_entry_speed_ranks(client, 9000, dlm)
        upserted = client.table.return_value.upsert.call_args.args[0]
        by_driver = {r['driver_number']: r for r in upserted}
        for sector in ('lap', 's1', 's2', 's3'):
            self.assertIsNotNone(by_driver[1][f'brake_entry_speed_pct_rank_{sector}'])
            self.assertIsNotNone(by_driver[1][f'brake_entry_speed_z_score_{sector}'])
            self.assertIsNotNone(by_driver[1][f'brake_entry_speed_category_{sector}'])

    def test_upserts_to_lap_metrics_table(self):
        dlm = self._make_dlm({
            1:  [(1, 200.0, None, None, None)],
            44: [(1, 300.0, None, None, None)],
        })
        client = _mock_client()
        ingest_brake_entry_speed_ranks(client, 9000, dlm)
        client.table.assert_called_with('lap_metrics')

    def test_row_count_matches_total_laps(self):
        """One row per driver-lap pair."""
        dlm = self._make_dlm({
            1:  [(1, 200.0, None, None, None), (2, 210.0, None, None, None)],
            44: [(1, 300.0, None, None, None), (2, 310.0, None, None, None)],
        })
        client = _mock_client()
        ingest_brake_entry_speed_ranks(client, 9000, dlm)
        upserted = client.table.return_value.upsert.call_args.args[0]
        self.assertEqual(len(upserted), 4)


# ===========================================================================
# ingest_battle_states
# ===========================================================================

class TestIngestBattleStates(unittest.TestCase):
    """Tests for gap, battle, overtake, and lap-context fields in lap_metrics."""

    # ----- fixture builders -------------------------------------------------

    def _lap(self, driver_number=44, lap_number=5,
             date_start='2024-03-02T13:00:00Z', lap_duration=90.0,
             duration_sector_1=30.0, duration_sector_2=30.0,
             i1_speed=210, i2_speed=280,
             segments_sector_1=None, segments_sector_2=None, segments_sector_3=None):
        return {
            'driver_number':      driver_number,
            'lap_number':         lap_number,
            'date_start':         date_start,
            'lap_duration':       lap_duration,
            'duration_sector_1':  duration_sector_1,
            'duration_sector_2':  duration_sector_2,
            'i1_speed':           i1_speed,
            'i2_speed':           i2_speed,
            'segments_sector_1':  segments_sector_1,
            'segments_sector_2':  segments_sector_2,
            'segments_sector_3':  segments_sector_3,
        }

    def _interval_row(self, driver_number=44, date='2024-03-02T13:00:45Z',
                      interval=3.5, gap_to_leader=5.0, laps_down=None):
        # Raw API row format (gap_to_leader and interval are raw values, may be strings).
        gtl = str(laps_down) + ' LAP' if laps_down else (str(gap_to_leader) if gap_to_leader is not None else None)
        return {
            'driver_number': driver_number,
            'date':          date,
            'interval':      interval,
            'gap_to_leader': gap_to_leader,
        }

    def _raw_interval(self, driver_number=44, date='2024-03-02T13:00:45Z',
                      interval=3.5, gap_to_leader=5.0):
        """Raw API interval row with numeric fields already (no string parsing needed)."""
        return {
            'driver_number': driver_number,
            'date':          date,
            'interval':      interval,
            'gap_to_leader': gap_to_leader,
        }

    def _overtake_row(self, overtaking=44, overtaken=1, date='2024-03-02T13:00:10Z'):
        return {
            'driver_number_overtaking': overtaking,
            'driver_number_overtaken':  overtaken,
            'date':                     date,
        }

    def _upserted(self, client):
        return client.table.return_value.upsert.call_args.args[0]

    def _row_for(self, rows, driver_number):
        return next(r for r in rows if r['driver_number'] == driver_number)

    # ----- gap_ahead ---------------------------------------------------------

    def test_gap_ahead_from_interval_value(self):
        """gap_ahead_s{X} is the driver's interval value at the sector end time."""
        client = _mock_client()
        lap = self._lap()
        # Interval reading at ~t0+45s (midpoint) — nearest to s2_end (t0+60s) and s3_end (t0+90s)
        intervals = [self._raw_interval(driver_number=44, date='2024-03-02T13:00:45Z',
                                        interval=3.5, gap_to_leader=5.0)]
        ingest_battle_states(client, 9000, [lap], intervals, [])
        row = self._upserted(client)[0]
        # All three sector ends are within nearest-neighbour range of this single reading
        self.assertAlmostEqual(row['gap_ahead_s1'], 3.5)
        self.assertAlmostEqual(row['gap_ahead_s2'], 3.5)
        self.assertAlmostEqual(row['gap_ahead_s3'], 3.5)

    def test_gap_ahead_none_for_leader(self):
        """Leader has interval=None → gap_ahead=None."""
        client = _mock_client()
        lap = self._lap(driver_number=1)
        intervals = [self._raw_interval(driver_number=1, date='2024-03-02T13:00:45Z',
                                        interval=None, gap_to_leader=None)]
        ingest_battle_states(client, 9000, [lap], intervals, [])
        row = self._upserted(client)[0]
        self.assertIsNone(row['gap_ahead_s1'])
        self.assertIsNone(row['gap_ahead_s2'])
        self.assertIsNone(row['gap_ahead_s3'])

    def test_gap_ahead_none_when_no_intervals(self):
        """Empty intervals_rows → all gap_ahead fields None."""
        client = _mock_client()
        lap = self._lap()
        ingest_battle_states(client, 9000, [lap], [], [])
        row = self._upserted(client)[0]
        self.assertIsNone(row['gap_ahead_s1'])
        self.assertIsNone(row['gap_ahead_s2'])
        self.assertIsNone(row['gap_ahead_s3'])

    def test_gap_fields_none_when_sector_time_missing(self):
        """Missing duration_sector_1 → all s1 gap/battle fields None."""
        client = _mock_client()
        lap = self._lap(duration_sector_1=None)
        intervals = [self._raw_interval(driver_number=44, date='2024-03-02T13:00:45Z',
                                        interval=3.5, gap_to_leader=5.0)]
        ingest_battle_states(client, 9000, [lap], intervals, [])
        row = self._upserted(client)[0]
        self.assertIsNone(row['gap_ahead_s1'])
        self.assertIsNone(row['gap_behind_s1'])
        self.assertIsNone(row['battle_ahead_s1_driver'])
        self.assertIsNone(row['battle_behind_s1_driver'])

    # ----- gap_behind --------------------------------------------------------

    def test_gap_behind_from_car_behind_interval(self):
        """gap_behind_sX = interval of the driver ranked directly behind."""
        client = _mock_client()
        # Driver 44 is P2, driver 33 is P3 with interval=0.8 to driver 44.
        lap_44 = self._lap(driver_number=44)
        # Lap for driver 33 — not strictly needed since we only check driver 44's row
        lap_33 = self._lap(driver_number=33, lap_number=5,
                           date_start='2024-03-02T13:00:00Z', lap_duration=90.0,
                           duration_sector_1=30.0, duration_sector_2=30.0)
        # P1 leader
        int_1  = self._raw_interval(driver_number=1,  date='2024-03-02T13:00:45Z',
                                    interval=None, gap_to_leader=None)
        # P2 driver 44
        int_44 = self._raw_interval(driver_number=44, date='2024-03-02T13:00:45Z',
                                    interval=2.0, gap_to_leader=2.0)
        # P3 driver 33, 0.8s behind driver 44
        int_33 = self._raw_interval(driver_number=33, date='2024-03-02T13:00:45Z',
                                    interval=0.8, gap_to_leader=2.8)
        ingest_battle_states(client, 9000, [lap_44, lap_33],
                             [int_1, int_44, int_33], [])
        rows = self._upserted(client)
        row_44 = self._row_for(rows, 44)
        self.assertAlmostEqual(row_44['gap_behind_s1'], 0.8)

    def test_last_place_has_no_gap_behind(self):
        """Driver at last position → gap_behind = None."""
        client = _mock_client()
        lap = self._lap(driver_number=99)
        intervals = [self._raw_interval(driver_number=99, date='2024-03-02T13:00:45Z',
                                        interval=5.0, gap_to_leader=10.0)]
        ingest_battle_states(client, 9000, [lap], intervals, [])
        row = self._upserted(client)[0]
        self.assertIsNone(row['gap_behind_s1'])
        self.assertIsNone(row['gap_behind_s2'])
        self.assertIsNone(row['gap_behind_s3'])

    # ----- battle drivers ----------------------------------------------------

    def test_battle_ahead_driver_set_when_gap_under_1s(self):
        """battle_ahead_sX_driver = dn of car ahead when gap < 1.0."""
        client = _mock_client()
        lap_44 = self._lap(driver_number=44)
        lap_1  = self._lap(driver_number=1, lap_number=5,
                           date_start='2024-03-02T13:00:00Z', lap_duration=90.0,
                           duration_sector_1=30.0, duration_sector_2=30.0)
        int_1  = self._raw_interval(driver_number=1,  date='2024-03-02T13:00:45Z',
                                    interval=None, gap_to_leader=None)
        int_44 = self._raw_interval(driver_number=44, date='2024-03-02T13:00:45Z',
                                    interval=0.6, gap_to_leader=0.6)
        ingest_battle_states(client, 9000, [lap_44, lap_1], [int_1, int_44], [])
        rows = self._upserted(client)
        row_44 = self._row_for(rows, 44)
        self.assertEqual(row_44['battle_ahead_s1_driver'], 1)
        self.assertEqual(row_44['battle_ahead_s2_driver'], 1)
        self.assertEqual(row_44['battle_ahead_s3_driver'], 1)

    def test_battle_ahead_driver_none_when_gap_over_1s(self):
        """battle_ahead_sX_driver = None when gap ≥ 1.0."""
        client = _mock_client()
        lap_44 = self._lap(driver_number=44)
        lap_1  = self._lap(driver_number=1, lap_number=5,
                           date_start='2024-03-02T13:00:00Z', lap_duration=90.0,
                           duration_sector_1=30.0, duration_sector_2=30.0)
        int_1  = self._raw_interval(driver_number=1,  date='2024-03-02T13:00:45Z',
                                    interval=None, gap_to_leader=None)
        int_44 = self._raw_interval(driver_number=44, date='2024-03-02T13:00:45Z',
                                    interval=1.5, gap_to_leader=1.5)
        ingest_battle_states(client, 9000, [lap_44, lap_1], [int_1, int_44], [])
        rows = self._upserted(client)
        row_44 = self._row_for(rows, 44)
        self.assertIsNone(row_44['battle_ahead_s1_driver'])

    def test_battle_behind_driver_identified_correctly(self):
        """battle_behind_sX_driver set when car behind is within 1s."""
        client = _mock_client()
        lap_44 = self._lap(driver_number=44)
        lap_33 = self._lap(driver_number=33, lap_number=5,
                           date_start='2024-03-02T13:00:00Z', lap_duration=90.0,
                           duration_sector_1=30.0, duration_sector_2=30.0)
        int_1  = self._raw_interval(driver_number=1,  date='2024-03-02T13:00:45Z',
                                    interval=None, gap_to_leader=None)
        int_44 = self._raw_interval(driver_number=44, date='2024-03-02T13:00:45Z',
                                    interval=2.0, gap_to_leader=2.0)
        # Driver 33 is 0.5s behind driver 44
        int_33 = self._raw_interval(driver_number=33, date='2024-03-02T13:00:45Z',
                                    interval=0.5, gap_to_leader=2.5)
        ingest_battle_states(client, 9000, [lap_44, lap_33],
                             [int_1, int_44, int_33], [])
        rows = self._upserted(client)
        row_44 = self._row_for(rows, 44)
        self.assertEqual(row_44['battle_behind_s1_driver'], 33)

    def test_leader_has_no_battle_ahead(self):
        """Race leader → battle_ahead_sX_driver = None."""
        client = _mock_client()
        lap = self._lap(driver_number=1)
        intervals = [self._raw_interval(driver_number=1, date='2024-03-02T13:00:45Z',
                                        interval=None, gap_to_leader=None)]
        ingest_battle_states(client, 9000, [lap], intervals, [])
        row = self._upserted(client)[0]
        self.assertIsNone(row['battle_ahead_s1_driver'])
        self.assertIsNone(row['battle_ahead_s2_driver'])
        self.assertIsNone(row['battle_ahead_s3_driver'])

    # ----- is_estimated_clean_air --------------------------------------------

    def test_is_clean_air_true_when_all_gaps_over_2s(self):
        """is_estimated_clean_air = True when all sector gap_aheads > 2.0."""
        client = _mock_client()
        lap = self._lap(driver_number=44)
        intervals = [self._raw_interval(driver_number=44, date='2024-03-02T13:00:45Z',
                                        interval=3.5, gap_to_leader=5.0)]
        ingest_battle_states(client, 9000, [lap], intervals, [])
        row = self._upserted(client)[0]
        self.assertTrue(row['is_estimated_clean_air'])

    def test_is_clean_air_false_when_any_gap_under_2s(self):
        """is_estimated_clean_air = False when any sector gap ≤ 2.0."""
        client = _mock_client()
        lap = self._lap(driver_number=44)
        # interval of 1.5 → gap_ahead = 1.5 ≤ 2.0
        intervals = [self._raw_interval(driver_number=44, date='2024-03-02T13:00:45Z',
                                        interval=1.5, gap_to_leader=3.0)]
        ingest_battle_states(client, 9000, [lap], intervals, [])
        row = self._upserted(client)[0]
        self.assertFalse(row['is_estimated_clean_air'])

    def test_is_clean_air_none_when_no_intervals(self):
        """is_estimated_clean_air = None when intervals_rows is empty."""
        client = _mock_client()
        lap = self._lap()
        ingest_battle_states(client, 9000, [lap], [], [])
        row = self._upserted(client)[0]
        self.assertIsNone(row['is_estimated_clean_air'])

    def test_leader_is_estimated_clean_air_true(self):
        """Race leader (interval=None, gap_to_leader=None) → is_estimated_clean_air=True."""
        client = _mock_client()
        lap = self._lap(driver_number=1)
        intervals = [self._raw_interval(driver_number=1, date='2024-03-02T13:00:45Z',
                                        interval=None, gap_to_leader=None)]
        ingest_battle_states(client, 9000, [lap], intervals, [])
        row = self._upserted(client)[0]
        self.assertTrue(row['is_estimated_clean_air'])

    def test_leader_zero_interval_openf1_artifact(self):
        """OpenF1 sometimes returns interval=0.0 and gap_to_leader=0.0 for the race leader
        instead of null/null. These should be treated as the leader: gap_ahead=None,
        is_estimated_clean_air=True, not gap_ahead=0.0 and is_clean_air=False."""
        client = _mock_client()
        lap = self._lap(driver_number=12)
        intervals = [self._raw_interval(driver_number=12, date='2024-03-02T13:00:45Z',
                                        interval=0.0, gap_to_leader=0.0)]
        ingest_battle_states(client, 9000, [lap], intervals, [])
        row = self._upserted(client)[0]
        self.assertIsNone(row['gap_ahead_s1'])
        self.assertIsNone(row['gap_ahead_s2'])
        self.assertIsNone(row['gap_ahead_s3'])
        self.assertTrue(row['is_estimated_clean_air'])
        self.assertIsNone(row['battle_ahead_s1_driver'])

    def test_is_clean_air_none_when_sector_time_missing(self):
        """Missing sector duration → is_estimated_clean_air = None."""
        client = _mock_client()
        lap = self._lap(duration_sector_1=None)
        intervals = [self._raw_interval(driver_number=44, date='2024-03-02T13:00:45Z',
                                        interval=3.5, gap_to_leader=5.0)]
        ingest_battle_states(client, 9000, [lap], intervals, [])
        row = self._upserted(client)[0]
        self.assertIsNone(row['is_estimated_clean_air'])

    # ----- overtakes ---------------------------------------------------------

    def test_overtake_event_in_s1_window_counted(self):
        """Overtake at t0+10s falls in s1 window [t0, t0+30] → overtakes_s1=1."""
        client = _mock_client()
        lap = self._lap()
        overtakes = [self._overtake_row(overtaking=44, overtaken=1,
                                        date='2024-03-02T13:00:10Z')]
        ingest_battle_states(client, 9000, [lap], [], overtakes)
        row = self._upserted(client)[0]
        self.assertEqual(row['overtakes_s1'], 1)
        self.assertEqual(row['overtakes_s2'], 0)
        self.assertEqual(row['overtakes_s3'], 0)

    def test_overtake_event_in_s2_window_not_in_s1(self):
        """Overtake at t0+40s falls in s2 window [t0+30, t0+60] → overtakes_s2=1."""
        client = _mock_client()
        lap = self._lap()
        overtakes = [self._overtake_row(overtaking=44, overtaken=1,
                                        date='2024-03-02T13:00:40Z')]
        ingest_battle_states(client, 9000, [lap], [], overtakes)
        row = self._upserted(client)[0]
        self.assertEqual(row['overtakes_s1'], 0)
        self.assertEqual(row['overtakes_s2'], 1)
        self.assertEqual(row['overtakes_s3'], 0)

    def test_overtake_before_lap_start_not_counted(self):
        """Overtake 5s before lap start → not counted in any sector."""
        client = _mock_client()
        lap = self._lap()
        overtakes = [self._overtake_row(overtaking=44, overtaken=1,
                                        date='2024-03-02T12:59:55Z')]
        ingest_battle_states(client, 9000, [lap], [], overtakes)
        row = self._upserted(client)[0]
        self.assertEqual(row['overtakes_s1'], 0)
        self.assertEqual(row['overtakes_s2'], 0)
        self.assertEqual(row['overtakes_s3'], 0)

    def test_lap_overtakes_is_sum_of_sectors(self):
        """lap_overtakes = sum of overtakes_s1 + overtakes_s2 + overtakes_s3."""
        client = _mock_client()
        lap = self._lap()
        overtakes = [
            self._overtake_row(overtaking=44, overtaken=1,  date='2024-03-02T13:00:10Z'),
            self._overtake_row(overtaking=44, overtaken=33, date='2024-03-02T13:00:40Z'),
        ]
        ingest_battle_states(client, 9000, [lap], [], overtakes)
        row = self._upserted(client)[0]
        self.assertEqual(row['lap_overtakes'], 2)
        self.assertEqual(row['overtakes_s1'], 1)
        self.assertEqual(row['overtakes_s2'], 1)

    def test_overtaken_counted_separately_from_overtakes(self):
        """driver_number_overtaken events populate overtaken_sX independently."""
        client = _mock_client()
        lap = self._lap(driver_number=44)
        overtakes = [self._overtake_row(overtaking=1, overtaken=44,
                                        date='2024-03-02T13:00:20Z')]
        ingest_battle_states(client, 9000, [lap], [], overtakes)
        row = self._upserted(client)[0]
        self.assertEqual(row['overtaken_s1'], 1)
        self.assertEqual(row['overtakes_s1'], 0)

    def test_lap_overtaken_is_sum_of_sectors(self):
        """lap_overtaken = sum of overtaken_s1/s2/s3."""
        client = _mock_client()
        lap = self._lap(driver_number=44)
        overtakes = [
            self._overtake_row(overtaking=1,  overtaken=44, date='2024-03-02T13:00:10Z'),
            self._overtake_row(overtaking=33, overtaken=44, date='2024-03-02T13:01:10Z'),
        ]
        ingest_battle_states(client, 9000, [lap], [], overtakes)
        row = self._upserted(client)[0]
        self.assertEqual(row['lap_overtaken'], 2)

    def test_overtakes_none_when_lap_times_missing(self):
        """Missing lap_duration → all overtake counts None."""
        client = _mock_client()
        lap = self._lap(lap_duration=None, duration_sector_1=None)
        overtakes = [self._overtake_row(overtaking=44, overtaken=1,
                                        date='2024-03-02T13:00:10Z')]
        ingest_battle_states(client, 9000, [lap], [], overtakes)
        row = self._upserted(client)[0]
        self.assertIsNone(row['overtakes_s1'])
        self.assertIsNone(row['lap_overtakes'])
        self.assertIsNone(row['lap_overtaken'])

    # ----- i1_speed / i2_speed / sector_context ------------------------------

    def test_i1_i2_speed_copied_from_laps(self):
        """i1_speed and i2_speed pass through from the laps dict."""
        client = _mock_client()
        lap = self._lap(i1_speed=215, i2_speed=295)
        ingest_battle_states(client, 9000, [lap], [], [])
        row = self._upserted(client)[0]
        self.assertEqual(row['i1_speed'], 215)
        self.assertEqual(row['i2_speed'], 295)

    def test_i1_i2_none_when_missing_in_lap(self):
        """None i1/i2 values pass through as None."""
        client = _mock_client()
        lap = self._lap(i1_speed=None, i2_speed=None)
        ingest_battle_states(client, 9000, [lap], [], [])
        row = self._upserted(client)[0]
        self.assertIsNone(row['i1_speed'])
        self.assertIsNone(row['i2_speed'])

    def test_sector_context_copied_from_laps(self):
        """segments_sector_1/2/3 pass through as sector_context_s1/s2/s3."""
        client = _mock_client()
        lap = self._lap(segments_sector_1=[2048, 2049, 2051],
                        segments_sector_2=[2049, 2049],
                        segments_sector_3=[2051, 2051, 2048])
        ingest_battle_states(client, 9000, [lap], [], [])
        row = self._upserted(client)[0]
        self.assertEqual(row['sector_context_s1'], [2048, 2049, 2051])
        self.assertEqual(row['sector_context_s2'], [2049, 2049])
        self.assertEqual(row['sector_context_s3'], [2051, 2051, 2048])

    # ----- row structure / filtering -----------------------------------------

    def test_row_contains_required_keys(self):
        """Every upserted row has session_key, driver_number, lap_number."""
        client = _mock_client()
        lap = self._lap()
        ingest_battle_states(client, 9000, [lap], [], [])
        row = self._upserted(client)[0]
        for key in ('session_key', 'driver_number', 'lap_number'):
            self.assertIn(key, row)
        self.assertEqual(row['session_key'], 9000)
        self.assertEqual(row['driver_number'], 44)
        self.assertEqual(row['lap_number'], 5)

    def test_missing_driver_number_filtered(self):
        """Lap with driver_number=None produces no upsert row."""
        client = _mock_client()
        lap = self._lap(driver_number=None)
        ingest_battle_states(client, 9000, [lap], [], [])
        client.table.assert_not_called()

    def test_empty_laps_no_upsert(self):
        """Empty laps list → no upsert called."""
        client = _mock_client()
        ingest_battle_states(client, 9000, [], [], [])
        client.table.assert_not_called()

    def test_one_row_per_lap(self):
        """Two laps for the same driver produce two rows."""
        client = _mock_client()
        laps = [
            self._lap(driver_number=44, lap_number=1),
            self._lap(driver_number=44, lap_number=2),
        ]
        ingest_battle_states(client, 9000, laps, [], [])
        rows = self._upserted(client)
        self.assertEqual(len(rows), 2)

    # ----- _parse_intervals_index helper -------------------------------------

    def test_parse_intervals_index_sorts_by_timestamp(self):
        """Index entries are sorted by timestamp ascending."""
        rows = [
            {'driver_number': 44, 'date': '2024-03-02T13:00:05Z', 'interval': 2.0, 'gap_to_leader': 5.0},
            {'driver_number': 44, 'date': '2024-03-02T13:00:01Z', 'interval': 2.1, 'gap_to_leader': 5.1},
        ]
        idx = _parse_intervals_index(rows)
        timestamps = [e[0] for e in idx[44]]
        self.assertEqual(timestamps, sorted(timestamps))

    def test_parse_intervals_index_skips_missing_driver(self):
        """Rows without driver_number are skipped."""
        rows = [{'driver_number': None, 'date': '2024-03-02T13:00:01Z',
                 'interval': 1.0, 'gap_to_leader': 2.0}]
        idx = _parse_intervals_index(rows)
        self.assertEqual(idx, {})

    def test_parse_intervals_index_skips_missing_date(self):
        """Rows without date are skipped."""
        rows = [{'driver_number': 44, 'date': None, 'interval': 1.0, 'gap_to_leader': 2.0}]
        idx = _parse_intervals_index(rows)
        self.assertEqual(idx, {})


class TestIngestStintMetrics(unittest.TestCase):
    """Tests for ingest_stint_metrics — per-stint pace aggregation."""

    # ----- fixture builders --------------------------------------------------

    def _lap(self, driver_number=44, lap_number=1, lap_duration=90.0,
             is_pit_out_lap=False, pit_in_time=None):
        return {
            'driver_number':  driver_number,
            'lap_number':     lap_number,
            'lap_duration':   lap_duration,
            'is_pit_out_lap': is_pit_out_lap,
            'pit_in_time':    pit_in_time,
        }

    def _stint(self, driver_number=44, stint_number=1, lap_start=1, lap_end=10):
        return {
            'driver_number': driver_number,
            'stint_number':  stint_number,
            'lap_start':     lap_start,
            'lap_end':       lap_end,
        }

    def _lm(self, driver_number=44, lap_number=1,
            is_estimated_clean_air=True, is_neutralized=False):
        return {
            'driver_number':          driver_number,
            'lap_number':             lap_number,
            'is_estimated_clean_air': is_estimated_clean_air,
            'is_neutralized':         is_neutralized,
        }

    def _make_client(self, lap_metrics_data=None):
        """Return (client, stint_metrics_table_mock).

        client.table('lap_metrics') → read mock with configured .data
        client.table('stint_metrics') (or any other name) → write mock
        """
        read_mock = MagicMock()
        read_mock.select.return_value.eq.return_value.execute.return_value.data = \
            lap_metrics_data or []
        write_mock = MagicMock()
        client = MagicMock()
        client.table.side_effect = lambda name: read_mock if name == 'lap_metrics' else write_mock
        return client, write_mock

    def _upserted(self, write_mock):
        return write_mock.upsert.call_args.args[0]

    # ----- core pace computations --------------------------------------------

    def test_representative_pace_is_median(self):
        """representative_pace_s is the median of racing lap durations."""
        laps = [self._lap(lap_number=n, lap_duration=float(90 + n)) for n in range(1, 4)]
        stints = [self._stint(lap_start=1, lap_end=3)]
        lm = [self._lm(lap_number=n, is_estimated_clean_air=True) for n in range(1, 4)]
        client, write_mock = self._make_client(lm)
        ingest_stint_metrics(client, 1, laps, stints)
        row = self._upserted(write_mock)[0]
        # durations: 91, 92, 93 → median = 92.0
        self.assertAlmostEqual(row['representative_pace_s'], 92.0)

    def test_clean_air_pace_mean_of_clean_laps(self):
        """clean_air_pace_s averages only laps with is_estimated_clean_air=True."""
        laps = [self._lap(lap_number=1, lap_duration=90.0),
                self._lap(lap_number=2, lap_duration=92.0),
                self._lap(lap_number=3, lap_duration=94.0)]
        stints = [self._stint(lap_start=1, lap_end=3)]
        lm = [
            self._lm(lap_number=1, is_estimated_clean_air=True),
            self._lm(lap_number=2, is_estimated_clean_air=False),
            self._lm(lap_number=3, is_estimated_clean_air=True),
        ]
        client, write_mock = self._make_client(lm)
        ingest_stint_metrics(client, 1, laps, stints)
        row = self._upserted(write_mock)[0]
        self.assertAlmostEqual(row['clean_air_pace_s'], 92.0)   # (90+94)/2
        self.assertAlmostEqual(row['dirty_air_pace_s'], 92.0)   # just lap 2

    def test_clean_air_pace_none_when_no_clean_laps(self):
        """clean_air_pace_s is None when no racing lap has is_estimated_clean_air=True."""
        laps = [self._lap(lap_number=1, lap_duration=90.0)]
        stints = [self._stint(lap_start=1, lap_end=1)]
        lm = [self._lm(lap_number=1, is_estimated_clean_air=False)]
        client, write_mock = self._make_client(lm)
        ingest_stint_metrics(client, 1, laps, stints)
        row = self._upserted(write_mock)[0]
        self.assertIsNone(row['clean_air_pace_s'])

    def test_dirty_air_pace_none_when_no_dirty_laps(self):
        """dirty_air_pace_s is None when no racing lap has is_estimated_clean_air=False."""
        laps = [self._lap(lap_number=1, lap_duration=90.0)]
        stints = [self._stint(lap_start=1, lap_end=1)]
        lm = [self._lm(lap_number=1, is_estimated_clean_air=True)]
        client, write_mock = self._make_client(lm)
        ingest_stint_metrics(client, 1, laps, stints)
        row = self._upserted(write_mock)[0]
        self.assertIsNone(row['dirty_air_pace_s'])

    def test_clean_air_none_counts_toward_racing_laps(self):
        """A lap with is_estimated_clean_air=None still counts toward representative_pace."""
        laps = [self._lap(lap_number=1, lap_duration=91.0),
                self._lap(lap_number=2, lap_duration=92.0)]
        stints = [self._stint(lap_start=1, lap_end=2)]
        lm = [
            self._lm(lap_number=1, is_estimated_clean_air=None),
            self._lm(lap_number=2, is_estimated_clean_air=None),
        ]
        client, write_mock = self._make_client(lm)
        ingest_stint_metrics(client, 1, laps, stints)
        row = self._upserted(write_mock)[0]
        self.assertEqual(row['racing_lap_count'], 2)
        self.assertIsNotNone(row['representative_pace_s'])
        self.assertIsNone(row['clean_air_pace_s'])
        self.assertIsNone(row['dirty_air_pace_s'])

    # ----- racing lap exclusions ---------------------------------------------

    def test_neutralized_lap_excluded(self):
        """Laps with is_neutralized=True are not racing laps."""
        laps = [self._lap(lap_number=1, lap_duration=90.0),
                self._lap(lap_number=2, lap_duration=120.0)]  # SC lap
        stints = [self._stint(lap_start=1, lap_end=2)]
        lm = [
            self._lm(lap_number=1, is_neutralized=False),
            self._lm(lap_number=2, is_neutralized=True),
        ]
        client, write_mock = self._make_client(lm)
        ingest_stint_metrics(client, 1, laps, stints)
        row = self._upserted(write_mock)[0]
        self.assertEqual(row['racing_lap_count'], 1)
        self.assertAlmostEqual(row['representative_pace_s'], 90.0)

    def test_neutralized_none_treated_as_not_neutralized(self):
        """is_neutralized=None is treated as not neutralized — lap is included."""
        laps = [self._lap(lap_number=1, lap_duration=90.0)]
        stints = [self._stint(lap_start=1, lap_end=1)]
        lm = [self._lm(lap_number=1, is_neutralized=None)]
        client, write_mock = self._make_client(lm)
        ingest_stint_metrics(client, 1, laps, stints)
        row = self._upserted(write_mock)[0]
        self.assertEqual(row['racing_lap_count'], 1)

    def test_pit_in_lap_excluded(self):
        """Laps with pit_in_time set are excluded from racing laps."""
        laps = [self._lap(lap_number=1, lap_duration=90.0),
                self._lap(lap_number=2, lap_duration=110.0, pit_in_time=25.3)]
        stints = [self._stint(lap_start=1, lap_end=2)]
        lm = [self._lm(lap_number=n) for n in range(1, 3)]
        client, write_mock = self._make_client(lm)
        ingest_stint_metrics(client, 1, laps, stints)
        row = self._upserted(write_mock)[0]
        self.assertEqual(row['racing_lap_count'], 1)

    def test_pit_out_lap_excluded(self):
        """Laps with is_pit_out_lap=True are excluded from racing laps."""
        laps = [self._lap(lap_number=1, lap_duration=105.0, is_pit_out_lap=True),
                self._lap(lap_number=2, lap_duration=90.0)]
        stints = [self._stint(lap_start=1, lap_end=2)]
        lm = [self._lm(lap_number=n) for n in range(1, 3)]
        client, write_mock = self._make_client(lm)
        ingest_stint_metrics(client, 1, laps, stints)
        row = self._upserted(write_mock)[0]
        self.assertEqual(row['racing_lap_count'], 1)
        self.assertAlmostEqual(row['representative_pace_s'], 90.0)

    def test_missing_lap_duration_excluded(self):
        """A lap with no lap_duration in laps list is excluded from racing laps."""
        laps = [self._lap(lap_number=1, lap_duration=None),
                self._lap(lap_number=2, lap_duration=90.0)]
        stints = [self._stint(lap_start=1, lap_end=2)]
        lm = [self._lm(lap_number=n) for n in range(1, 3)]
        client, write_mock = self._make_client(lm)
        ingest_stint_metrics(client, 1, laps, stints)
        row = self._upserted(write_mock)[0]
        self.assertEqual(row['racing_lap_count'], 1)

    # ----- first/second half split -------------------------------------------

    def test_first_second_half_ceiling_split_odd(self):
        """For 3 racing laps, ceil(3/2)=2: first=[lap1,lap2], second=[lap3]."""
        laps = [self._lap(lap_number=n, lap_duration=float(90 + n)) for n in range(1, 4)]
        stints = [self._stint(lap_start=1, lap_end=3)]
        lm = [self._lm(lap_number=n) for n in range(1, 4)]
        client, write_mock = self._make_client(lm)
        ingest_stint_metrics(client, 1, laps, stints)
        row = self._upserted(write_mock)[0]
        # durations in lap order: 91, 92, 93
        # first half (laps 1,2): mean = 91.5; second half (lap 3): mean = 93.0
        self.assertAlmostEqual(row['first_half_pace_s'], 91.5)
        self.assertAlmostEqual(row['second_half_pace_s'], 93.0)

    def test_first_second_half_even_split(self):
        """For 4 racing laps, split=2: first=[1,2], second=[3,4]."""
        laps = [self._lap(lap_number=n, lap_duration=float(90 + n)) for n in range(1, 5)]
        stints = [self._stint(lap_start=1, lap_end=4)]
        lm = [self._lm(lap_number=n) for n in range(1, 5)]
        client, write_mock = self._make_client(lm)
        ingest_stint_metrics(client, 1, laps, stints)
        row = self._upserted(write_mock)[0]
        self.assertAlmostEqual(row['first_half_pace_s'], 91.5)   # (91+92)/2
        self.assertAlmostEqual(row['second_half_pace_s'], 93.5)  # (93+94)/2

    def test_first_half_none_for_single_racing_lap(self):
        """For 1 racing lap: ceil(1/2)=1, so first_half gets the lap, second_half=None."""
        laps = [self._lap(lap_number=1, lap_duration=90.0)]
        stints = [self._stint(lap_start=1, lap_end=1)]
        lm = [self._lm(lap_number=1)]
        client, write_mock = self._make_client(lm)
        ingest_stint_metrics(client, 1, laps, stints)
        row = self._upserted(write_mock)[0]
        self.assertAlmostEqual(row['first_half_pace_s'], 90.0)
        self.assertIsNone(row['second_half_pace_s'])

    def test_all_pace_none_for_zero_racing_laps(self):
        """When all laps are pit/neutralized, all pace fields are None."""
        laps = [self._lap(lap_number=1, lap_duration=90.0, pit_in_time=25.0)]
        stints = [self._stint(lap_start=1, lap_end=1)]
        lm = [self._lm(lap_number=1)]
        client, write_mock = self._make_client(lm)
        ingest_stint_metrics(client, 1, laps, stints)
        row = self._upserted(write_mock)[0]
        self.assertEqual(row['racing_lap_count'], 0)
        self.assertIsNone(row['representative_pace_s'])
        self.assertIsNone(row['first_half_pace_s'])
        self.assertIsNone(row['second_half_pace_s'])

    # ----- stint grouping and structure --------------------------------------

    def test_multi_stint_produces_separate_rows(self):
        """Two stints produce two separate rows in stint_metrics."""
        laps = [self._lap(lap_number=n, lap_duration=90.0) for n in range(1, 6)]
        # pit-in on lap 3, pit-out on lap 4
        laps[2] = self._lap(lap_number=3, lap_duration=110.0, pit_in_time=20.0)
        laps[3] = self._lap(lap_number=4, lap_duration=105.0, is_pit_out_lap=True)
        stints = [
            self._stint(stint_number=1, lap_start=1, lap_end=3),
            self._stint(stint_number=2, lap_start=4, lap_end=5),
        ]
        lm = [self._lm(lap_number=n) for n in range(1, 6)]
        client, write_mock = self._make_client(lm)
        ingest_stint_metrics(client, 1, laps, stints)
        rows = self._upserted(write_mock)
        stint_numbers = {r['stint_number'] for r in rows}
        self.assertEqual(stint_numbers, {1, 2})

    def test_final_stint_lap_end_none_covers_remaining_laps(self):
        """A stint with lap_end=None covers all laps from lap_start onward."""
        laps = [self._lap(lap_number=n, lap_duration=90.0) for n in range(5, 9)]
        stints = [self._stint(stint_number=2, lap_start=5, lap_end=None)]
        lm = [self._lm(lap_number=n) for n in range(5, 9)]
        client, write_mock = self._make_client(lm)
        ingest_stint_metrics(client, 1, laps, stints)
        row = self._upserted(write_mock)[0]
        self.assertEqual(row['racing_lap_count'], 4)
        self.assertEqual(row['stint_number'], 2)

    def test_lap_outside_stint_range_excluded(self):
        """Laps not covered by any stint range are not grouped and not upserted."""
        laps = [self._lap(lap_number=1, lap_duration=90.0),
                self._lap(lap_number=5, lap_duration=90.0)]  # lap 5 not in stint
        stints = [self._stint(lap_start=1, lap_end=1)]
        lm = [self._lm(lap_number=1), self._lm(lap_number=5)]
        client, write_mock = self._make_client(lm)
        ingest_stint_metrics(client, 1, laps, stints)
        rows = self._upserted(write_mock)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['racing_lap_count'], 1)

    # ----- row structure and guard clauses -----------------------------------

    def test_row_has_required_keys(self):
        """Each upserted row contains session_key, driver_number, stint_number."""
        laps = [self._lap(lap_number=1, lap_duration=90.0)]
        stints = [self._stint(lap_start=1, lap_end=1)]
        lm = [self._lm(lap_number=1)]
        client, write_mock = self._make_client(lm)
        ingest_stint_metrics(client, 1, laps, stints)
        row = self._upserted(write_mock)[0]
        self.assertEqual(row['session_key'], 1)
        self.assertEqual(row['driver_number'], 44)
        self.assertEqual(row['stint_number'], 1)

    def test_empty_stints_no_upsert(self):
        """Empty stints_rows triggers early return — no upsert called."""
        laps = [self._lap(lap_number=1, lap_duration=90.0)]
        client, write_mock = self._make_client()
        ingest_stint_metrics(client, 1, laps, [])
        write_mock.upsert.assert_not_called()

    def test_empty_laps_no_upsert(self):
        """Empty laps triggers early return — no upsert called."""
        stints = [self._stint(lap_start=1, lap_end=5)]
        client, write_mock = self._make_client()
        ingest_stint_metrics(client, 1, [], stints)
        write_mock.upsert.assert_not_called()

    def test_multi_driver_produces_separate_rows(self):
        """Drivers with laps in the same stint range each get their own row."""
        laps = [
            self._lap(driver_number=44, lap_number=1, lap_duration=90.0),
            self._lap(driver_number=1,  lap_number=1, lap_duration=91.0),
        ]
        stints = [
            self._stint(driver_number=44, lap_start=1, lap_end=1),
            self._stint(driver_number=1,  lap_start=1, lap_end=1),
        ]
        lm = [
            self._lm(driver_number=44, lap_number=1),
            self._lm(driver_number=1,  lap_number=1),
        ]
        client, write_mock = self._make_client(lm)
        ingest_stint_metrics(client, 1, laps, stints)
        rows = self._upserted(write_mock)
        driver_numbers = {r['driver_number'] for r in rows}
        self.assertEqual(driver_numbers, {44, 1})


# ===========================================================================
# _query_in
# ===========================================================================

class TestQueryIn(unittest.TestCase):

    def test_empty_values_returns_empty_without_calling_client(self):
        client = MagicMock()
        result = _query_in(client, 'some_table', 'col', 'id', [])
        self.assertEqual(result, [])
        client.table.assert_not_called()

    def test_nonempty_values_calls_client_and_returns_data(self):
        client = MagicMock()
        client.table.return_value.select.return_value.in_.return_value.execute.return_value.data = [
            {'id': 1}
        ]
        result = _query_in(client, 'sessions', 'session_key', 'meeting_key', [101])
        self.assertEqual(result, [{'id': 1}])
        client.table.assert_called_once_with('sessions')

    def test_none_data_returns_empty_list(self):
        client = MagicMock()
        client.table.return_value.select.return_value.in_.return_value.execute.return_value.data = None
        result = _query_in(client, 'sessions', 'session_key', 'meeting_key', [1])
        self.assertEqual(result, [])


# ===========================================================================
# ingest_championship_drivers — gap fields
# ===========================================================================

class TestIngestChampionshipDriversGaps(unittest.TestCase):

    def _make_standing(self, session_key=1, driver_number=44, points_start=0,
                       points_current=25, position_start=1, position_current=1):
        return {
            'session_key': session_key, 'driver_number': driver_number,
            'points_start': points_start, 'points_current': points_current,
            'position_start': position_start, 'position_current': position_current,
        }

    @patch('ingest.openf1.get_championship_drivers')
    def test_leader_gap_to_leader_is_zero(self, mock_get):
        """Championship leader should have points_gap_to_leader = 0."""
        mock_get.return_value = [
            self._make_standing(driver_number=1,  points_current=50),
            self._make_standing(driver_number=44, points_current=30),
        ]
        client = _mock_client()
        ingest_championship_drivers(client, 1)
        rows = client.table.return_value.upsert.call_args.args[0]
        leader = next(r for r in rows if r['driver_number'] == 1)
        self.assertEqual(leader['points_gap_to_leader'], 0)

    @patch('ingest.openf1.get_championship_drivers')
    def test_non_leader_gap_to_leader(self, mock_get):
        """Non-leader gap = leader_pts - driver_pts."""
        mock_get.return_value = [
            self._make_standing(driver_number=1,  points_current=50),
            self._make_standing(driver_number=44, points_current=30),
        ]
        client = _mock_client()
        ingest_championship_drivers(client, 1)
        rows = client.table.return_value.upsert.call_args.args[0]
        p2 = next(r for r in rows if r['driver_number'] == 44)
        self.assertEqual(p2['points_gap_to_leader'], 20)

    @patch('ingest.openf1.get_championship_drivers')
    def test_p2_gap_to_p2_is_zero(self, mock_get):
        """P2 driver has points_gap_to_p2 = 0."""
        mock_get.return_value = [
            self._make_standing(driver_number=1,  points_current=50),
            self._make_standing(driver_number=44, points_current=30),
        ]
        client = _mock_client()
        ingest_championship_drivers(client, 1)
        rows = client.table.return_value.upsert.call_args.args[0]
        p2 = next(r for r in rows if r['driver_number'] == 44)
        self.assertEqual(p2['points_gap_to_p2'], 0)

    @patch('ingest.openf1.get_championship_drivers')
    def test_leader_gap_to_p2_is_negative(self, mock_get):
        """Leader's points_gap_to_p2 = p2_pts - leader_pts (negative = ahead)."""
        mock_get.return_value = [
            self._make_standing(driver_number=1,  points_current=50),
            self._make_standing(driver_number=44, points_current=30),
        ]
        client = _mock_client()
        ingest_championship_drivers(client, 1)
        rows = client.table.return_value.upsert.call_args.args[0]
        leader = next(r for r in rows if r['driver_number'] == 1)
        self.assertEqual(leader['points_gap_to_p2'], -20)

    @patch('ingest.openf1.get_championship_drivers')
    def test_gap_none_when_points_current_none(self, mock_get):
        """Gaps are None when points_current is None."""
        mock_get.return_value = [
            self._make_standing(driver_number=1, points_current=None),
        ]
        client = _mock_client()
        ingest_championship_drivers(client, 1)
        rows = client.table.return_value.upsert.call_args.args[0]
        self.assertIsNone(rows[0]['points_gap_to_leader'])
        self.assertIsNone(rows[0]['points_gap_to_p2'])


# ===========================================================================
# ingest_championship_teams — gap fields
# ===========================================================================

class TestIngestChampionshipTeamsGaps(unittest.TestCase):

    def _make_standing(self, session_key=1, team_name='Red Bull Racing',
                       points_start=0, points_current=50):
        return {
            'session_key': session_key, 'team_name': team_name,
            'points_start': points_start, 'points_current': points_current,
            'position_start': 1, 'position_current': 1,
        }

    @patch('ingest.openf1.get_championship_teams')
    def test_leader_gap_zero(self, mock_get):
        mock_get.return_value = [
            self._make_standing(team_name='Red Bull Racing', points_current=80),
            self._make_standing(team_name='Ferrari',         points_current=60),
        ]
        client = _mock_client()
        ingest_championship_teams(client, 1)
        rows = client.table.return_value.upsert.call_args.args[0]
        leader = next(r for r in rows if r['team_name'] == 'Red Bull Racing')
        self.assertEqual(leader['points_gap_to_leader'], 0)

    @patch('ingest.openf1.get_championship_teams')
    def test_non_leader_gap(self, mock_get):
        mock_get.return_value = [
            self._make_standing(team_name='Red Bull Racing', points_current=80),
            self._make_standing(team_name='Ferrari',         points_current=60),
        ]
        client = _mock_client()
        ingest_championship_teams(client, 1)
        rows = client.table.return_value.upsert.call_args.args[0]
        ferrari = next(r for r in rows if r['team_name'] == 'Ferrari')
        self.assertEqual(ferrari['points_gap_to_leader'], 20)


# ===========================================================================
# ingest_season_driver_stats
# ===========================================================================

class TestIngestSeasonDriverStats(unittest.TestCase):
    """Unit tests for ingest_season_driver_stats using a multi-table mock."""

    # ----- fixture builders --------------------------------------------------

    def _meeting(self, key=101, date='2026-03-15'):
        return {'meeting_key': key, 'date_start': date}

    def _session(self, sk=1001, meeting_key=101, session_type='Race'):
        return {'session_key': sk, 'meeting_key': meeting_key, 'session_type': session_type}

    def _rr(self, sk=1001, dn=44, position=1, laps=57,
            dnf=False, dns=False, dsq=False, fastest_lap=False):
        return {
            'session_key': sk, 'driver_number': dn, 'position': position,
            'number_of_laps': laps, 'dnf': dnf, 'dns': dns, 'dsq': dsq,
            'fastest_lap_flag': fastest_lap,
        }

    def _cd(self, sk=1001, dn=44, pts=25.0):
        return {'session_key': sk, 'driver_number': dn, 'points_current': pts}

    def _ct(self, sk=1001, team='Red Bull Racing', pts=43.0):
        return {'session_key': sk, 'team_name': team, 'points_current': pts}

    def _driver(self, sk=1001, dn=44, team='Red Bull Racing'):
        return {'session_key': sk, 'driver_number': dn, 'team_name': team}

    def _sg(self, sk=2001, dn=44, pos=1):
        return {'session_key': sk, 'driver_number': dn, 'position': pos}

    def _qr(self, sk=2001, dn=44, best_time=90.5):
        return {'session_key': sk, 'driver_number': dn, 'best_lap_time': best_time}

    def _ov(self, sk=1001, overtaking=44, overtaken=1):
        return {'session_key': sk, 'driver_number_overtaking': overtaking,
                'driver_number_overtaken': overtaken}

    def _ps(self, sk=1001, dn=44):
        return {'session_key': sk, 'driver_number': dn}

    def _make_client(self, *, meetings, sessions, race_results,
                     champ_drv=None, champ_teams=None, overtakes=None,
                     pit_stops=None, starting_grid=None, qual_results=None,
                     drivers=None):
        """Build a mock client whose table responses match the supplied data."""
        table_data = {
            'races':                meetings,
            'sessions':             sessions,
            'race_results':         race_results,
            'championship_drivers': champ_drv or [],
            'championship_teams':   champ_teams or [],
            'overtakes':            overtakes or [],
            'pit_stops':            pit_stops or [],
            'starting_grid':        starting_grid or [],
            'qualifying_results':   qual_results or [],
            'drivers':              drivers or [],
        }
        client = MagicMock()

        def table_side(name):
            data = table_data.get(name, [])
            m = MagicMock()
            # .select().eq()... or .select().in_()... both lead to .execute().data
            m.select.return_value.eq.return_value.execute.return_value.data = data
            m.select.return_value.in_.return_value.execute.return_value.data = data
            m.upsert.return_value.execute.return_value = MagicMock()
            return m

        client.table.side_effect = table_side
        return client

    def _upserted(self, client):
        """Return the rows passed to the season_driver_stats upsert call."""
        for call in client.table.call_args_list:
            if call.args[0] == 'season_driver_stats':
                tbl = client.table('season_driver_stats')
                return tbl.upsert.call_args.args[0]
        return []

    # ----- tests -------------------------------------------------------------

    @patch('ingest.openf1.get_meetings', return_value=[])
    def test_no_meetings_no_upsert(self, _):
        client = self._make_client(meetings=[], sessions=[], race_results=[])
        ingest_season_driver_stats(client, 2026)
        # No upsert to season_driver_stats should occur
        for c in client.table.call_args_list:
            if c.args[0] == 'season_driver_stats':
                tbl = client.table('season_driver_stats')
                tbl.upsert.assert_not_called()

    @patch('ingest.openf1.get_meetings')
    def test_no_race_sessions_no_upsert(self, mock_get_meetings):
        """Only qualifying sessions → no race data → early return."""
        meetings = [self._meeting()]
        mock_get_meetings.return_value = meetings
        client = self._make_client(
            meetings=meetings,
            sessions=[self._session(sk=2001, session_type='Qualifying')],
            race_results=[],
        )
        ingest_season_driver_stats(client, 2026)
        for c in client.table.call_args_list:
            if c.args[0] == 'season_driver_stats':
                tbl = client.table('season_driver_stats')
                tbl.upsert.assert_not_called()

    def test_win_counted(self):
        """A P1 finish should increment wins to 1."""
        rows = self._run_and_capture(
            meetings=[self._meeting()],
            sessions=[self._session()],
            race_results=[self._rr(dn=44, position=1)],
            champ_drv=[self._cd(dn=44, pts=25.0)],
            champ_teams=[self._ct()],
            drivers=[self._driver(dn=44)],
        )
        self.assertEqual(rows[0]['wins'], 1)

    def _run_and_capture(self, *, meetings, sessions, race_results,
                         champ_drv=None, champ_teams=None, overtakes=None,
                         pit_stops=None, starting_grid=None,
                         qual_results=None, drivers=None):
        """Run ingest_season_driver_stats and return the upserted rows."""
        upserted: list[dict] = []

        table_data = {
            'races':                meetings,
            'sessions':             sessions,
            'race_results':         race_results,
            'championship_drivers': champ_drv or [],
            'championship_teams':   champ_teams or [],
            'overtakes':            overtakes or [],
            'pit_stops':            pit_stops or [],
            'starting_grid':        starting_grid or [],
            'qualifying_results':   qual_results or [],
            'drivers':              drivers or [],
            'season_driver_stats':  [],
        }

        def table_side(name):
            data = table_data.get(name, [])
            m = MagicMock()
            m.select.return_value.eq.return_value.execute.return_value.data = data
            m.select.return_value.in_.return_value.execute.return_value.data = data

            def capture_upsert(rows, **kwargs):
                if name == 'season_driver_stats':
                    upserted.extend(rows)
                return MagicMock()

            m.upsert.side_effect = capture_upsert
            m.upsert.return_value.execute.return_value = MagicMock()
            return m

        client = MagicMock()
        client.table.side_effect = table_side
        with patch('ingest.openf1.get_meetings', return_value=meetings):
            ingest_season_driver_stats(client, 2026)
        return upserted

    def test_podium_counted(self):
        rows = self._run_and_capture(
            meetings=[self._meeting()],
            sessions=[self._session()],
            race_results=[self._rr(dn=44, position=3)],
            champ_drv=[self._cd(dn=44, pts=15.0)],
            drivers=[self._driver(dn=44)],
        )
        self.assertEqual(rows[0]['podiums'], 1)

    def test_no_podium_for_p4(self):
        rows = self._run_and_capture(
            meetings=[self._meeting()],
            sessions=[self._session()],
            race_results=[self._rr(dn=44, position=4)],
            champ_drv=[self._cd(dn=44, pts=12.0)],
            drivers=[self._driver(dn=44)],
        )
        self.assertEqual(rows[0]['podiums'], 0)

    def test_dnf_counted(self):
        rows = self._run_and_capture(
            meetings=[self._meeting()],
            sessions=[self._session()],
            race_results=[self._rr(dn=44, position=None, dnf=True)],
            drivers=[self._driver(dn=44)],
        )
        self.assertEqual(rows[0]['dnf_count'], 1)
        self.assertEqual(rows[0]['races_classified'], 0)

    def test_dns_counted(self):
        rows = self._run_and_capture(
            meetings=[self._meeting()],
            sessions=[self._session()],
            race_results=[self._rr(dn=44, position=None, dns=True)],
            drivers=[self._driver(dn=44)],
        )
        self.assertEqual(rows[0]['dns_count'], 1)

    def test_fastest_lap_flag_counted(self):
        rows = self._run_and_capture(
            meetings=[self._meeting()],
            sessions=[self._session()],
            race_results=[self._rr(dn=44, position=2, fastest_lap=True)],
            champ_drv=[self._cd(dn=44, pts=19.0)],
            drivers=[self._driver(dn=44)],
        )
        self.assertEqual(rows[0]['fastest_laps'], 1)

    def test_points_scored_from_championship_drivers(self):
        rows = self._run_and_capture(
            meetings=[self._meeting()],
            sessions=[self._session()],
            race_results=[self._rr(dn=44, position=1)],
            champ_drv=[self._cd(dn=44, pts=75.0)],
            drivers=[self._driver(dn=44)],
        )
        self.assertEqual(rows[0]['points_scored'], 75.0)

    def test_pole_from_starting_grid(self):
        rows = self._run_and_capture(
            meetings=[self._meeting()],
            sessions=[
                self._session(sk=1001, session_type='Race'),
                self._session(sk=2001, session_type='Qualifying'),
            ],
            race_results=[self._rr(sk=1001, dn=44, position=1)],
            champ_drv=[self._cd(sk=1001, dn=44, pts=25.0)],
            starting_grid=[self._sg(sk=2001, dn=44, pos=1)],
            drivers=[
                self._driver(sk=1001, dn=44),
                self._driver(sk=2001, dn=44),
            ],
        )
        self.assertEqual(rows[0]['poles'], 1)

    def test_wins_over_teammate(self):
        """Driver 44 (P1) beats teammate 1 (P2) → wins_over_teammate = 1."""
        rows = self._run_and_capture(
            meetings=[self._meeting()],
            sessions=[self._session()],
            race_results=[
                self._rr(dn=44, position=1),
                self._rr(dn=1,  position=2),
            ],
            champ_drv=[
                self._cd(dn=44, pts=25.0),
                self._cd(dn=1,  pts=18.0),
            ],
            drivers=[
                self._driver(dn=44, team='Red Bull Racing'),
                self._driver(dn=1,  team='Red Bull Racing'),
            ],
        )
        d44 = next(r for r in rows if r['driver_number'] == 44)
        self.assertEqual(d44['wins_over_teammate'], 1)

    def test_no_wins_over_teammate_when_lost(self):
        """Driver 44 (P2) does not get wins_over_teammate when teammate is P1."""
        rows = self._run_and_capture(
            meetings=[self._meeting()],
            sessions=[self._session()],
            race_results=[
                self._rr(dn=44, position=2),
                self._rr(dn=1,  position=1),
            ],
            champ_drv=[
                self._cd(dn=44, pts=18.0),
                self._cd(dn=1,  pts=25.0),
            ],
            drivers=[
                self._driver(dn=44, team='Red Bull Racing'),
                self._driver(dn=1,  team='Red Bull Racing'),
            ],
        )
        d44 = next(r for r in rows if r['driver_number'] == 44)
        self.assertEqual(d44['wins_over_teammate'], 0)

    def test_overtakes_counted(self):
        rows = self._run_and_capture(
            meetings=[self._meeting()],
            sessions=[self._session()],
            race_results=[self._rr(dn=44, position=1)],
            overtakes=[self._ov(overtaking=44, overtaken=1)],
            drivers=[self._driver(dn=44)],
        )
        self.assertEqual(rows[0]['total_overtakes_made'], 1)
        self.assertEqual(rows[0]['total_overtakes_suffered'], 0)

    def test_pit_stops_counted(self):
        rows = self._run_and_capture(
            meetings=[self._meeting()],
            sessions=[self._session()],
            race_results=[self._rr(dn=44, position=1)],
            pit_stops=[self._ps(dn=44), self._ps(dn=44)],
            drivers=[self._driver(dn=44)],
        )
        self.assertEqual(rows[0]['total_pit_stops'], 2)

    def test_cumulative_over_multiple_rounds(self):
        """Stats accumulate correctly across two rounds."""
        rows = self._run_and_capture(
            meetings=[
                self._meeting(key=101, date='2026-03-01'),
                self._meeting(key=102, date='2026-03-15'),
            ],
            sessions=[
                self._session(sk=1001, meeting_key=101, session_type='Race'),
                self._session(sk=1002, meeting_key=102, session_type='Race'),
            ],
            race_results=[
                self._rr(sk=1001, dn=44, position=1),
                self._rr(sk=1002, dn=44, position=2),
            ],
            champ_drv=[
                self._cd(sk=1001, dn=44, pts=25.0),
                self._cd(sk=1002, dn=44, pts=43.0),
            ],
            drivers=[
                self._driver(sk=1001, dn=44),
                self._driver(sk=1002, dn=44),
            ],
        )
        # Round 1: wins=1, races_entered=1
        r1 = next(r for r in rows if r['round_number'] == 1)
        r2 = next(r for r in rows if r['round_number'] == 2)
        self.assertEqual(r1['wins'], 1)
        self.assertEqual(r1['races_entered'], 1)
        self.assertEqual(r2['wins'], 1)          # still 1 (no win in round 2)
        self.assertEqual(r2['races_entered'], 2)  # cumulative
        self.assertEqual(r2['podiums'], 2)        # P1+P2

    def test_qualifying_supertime_gap_mean(self):
        """qual gap = driver - teammate; mean computed over multiple rounds."""
        rows = self._run_and_capture(
            meetings=[
                self._meeting(key=101, date='2026-03-01'),
                self._meeting(key=102, date='2026-03-15'),
            ],
            sessions=[
                self._session(sk=1001, meeting_key=101, session_type='Race'),
                self._session(sk=2001, meeting_key=101, session_type='Qualifying'),
                self._session(sk=1002, meeting_key=102, session_type='Race'),
                self._session(sk=2002, meeting_key=102, session_type='Qualifying'),
            ],
            race_results=[
                self._rr(sk=1001, dn=44, position=1),
                self._rr(sk=1002, dn=44, position=1),
            ],
            champ_drv=[
                self._cd(sk=1001, dn=44, pts=25.0),
                self._cd(sk=1002, dn=44, pts=50.0),
            ],
            qual_results=[
                self._qr(sk=2001, dn=44, best_time=90.0),
                self._qr(sk=2001, dn=1,  best_time=91.0),   # gap = -1.0
                self._qr(sk=2002, dn=44, best_time=90.5),
                self._qr(sk=2002, dn=1,  best_time=90.0),   # gap = +0.5
            ],
            drivers=[
                self._driver(sk=1001, dn=44, team='Red Bull Racing'),
                self._driver(sk=1001, dn=1,  team='Red Bull Racing'),
                self._driver(sk=1002, dn=44, team='Red Bull Racing'),
                self._driver(sk=1002, dn=1,  team='Red Bull Racing'),
                self._driver(sk=2001, dn=44, team='Red Bull Racing'),
                self._driver(sk=2001, dn=1,  team='Red Bull Racing'),
                self._driver(sk=2002, dn=44, team='Red Bull Racing'),
                self._driver(sk=2002, dn=1,  team='Red Bull Racing'),
            ],
        )
        r1 = next(r for r in rows if r['round_number'] == 1 and r['driver_number'] == 44)
        r2 = next(r for r in rows if r['round_number'] == 2 and r['driver_number'] == 44)
        self.assertAlmostEqual(r1['qualifying_supertime_gap_s'], -1.0)
        self.assertAlmostEqual(r2['qualifying_supertime_gap_s'], (-1.0 + 0.5) / 2)

    def test_row_has_required_keys(self):
        rows = self._run_and_capture(
            meetings=[self._meeting()],
            sessions=[self._session()],
            race_results=[self._rr(dn=44, position=1)],
            drivers=[self._driver(dn=44)],
        )
        self.assertIn('year', rows[0])
        self.assertIn('round_number', rows[0])
        self.assertIn('driver_number', rows[0])
        self.assertEqual(rows[0]['year'], 2026)
        self.assertEqual(rows[0]['round_number'], 1)


# ===========================================================================
# ingest_season_constructor_stats
# ===========================================================================

class TestIngestSeasonConstructorStats(unittest.TestCase):

    def _meeting(self, key=101, date='2026-03-15'):
        return {'meeting_key': key, 'date_start': date}

    def _session(self, sk=1001, meeting_key=101, session_type='Race'):
        return {'session_key': sk, 'meeting_key': meeting_key, 'session_type': session_type}

    def _rr(self, sk=1001, dn=44, position=1, laps=57, dnf=False, dns=False,
            dsq=False, fastest_lap=False):
        return {
            'session_key': sk, 'driver_number': dn, 'position': position,
            'number_of_laps': laps, 'dnf': dnf, 'dns': dns, 'dsq': dsq,
            'fastest_lap_flag': fastest_lap,
        }

    def _ct(self, sk=1001, team='Red Bull Racing', pts=43.0):
        return {'session_key': sk, 'team_name': team, 'points_current': pts}

    def _driver(self, sk=1001, dn=44, team='Red Bull Racing'):
        return {'session_key': sk, 'driver_number': dn, 'team_name': team}

    def _sg(self, sk=2001, dn=44, pos=1):
        return {'session_key': sk, 'driver_number': dn, 'position': pos}

    def _ps(self, sk=1001, dn=44):
        return {'session_key': sk, 'driver_number': dn}

    def _run_and_capture(self, *, meetings, sessions, race_results,
                         champ_teams=None, pit_stops=None, starting_grid=None,
                         drivers=None):
        upserted: list[dict] = []
        table_data = {
            'races':                    meetings,
            'sessions':                 sessions,
            'race_results':             race_results,
            'championship_teams':       champ_teams or [],
            'pit_stops':                pit_stops or [],
            'starting_grid':            starting_grid or [],
            'drivers':                  drivers or [],
            'season_constructor_stats': [],
        }

        def table_side(name):
            data = table_data.get(name, [])
            m = MagicMock()
            m.select.return_value.eq.return_value.execute.return_value.data = data
            m.select.return_value.in_.return_value.execute.return_value.data = data

            def capture(rows, **kwargs):
                if name == 'season_constructor_stats':
                    upserted.extend(rows)
                return MagicMock()

            m.upsert.side_effect = capture
            m.upsert.return_value.execute.return_value = MagicMock()
            return m

        client = MagicMock()
        client.table.side_effect = table_side
        with patch('ingest.openf1.get_meetings', return_value=meetings):
            ingest_season_constructor_stats(client, 2026)
        return upserted

    def test_team_win_counted(self):
        rows = self._run_and_capture(
            meetings=[self._meeting()],
            sessions=[self._session()],
            race_results=[self._rr(dn=44, position=1)],
            champ_teams=[self._ct()],
            drivers=[self._driver(dn=44, team='Red Bull Racing')],
        )
        rbr = next(r for r in rows if r['team_name'] == 'Red Bull Racing')
        self.assertEqual(rbr['wins'], 1)

    def test_team_podium_counted(self):
        rows = self._run_and_capture(
            meetings=[self._meeting()],
            sessions=[self._session()],
            race_results=[
                self._rr(dn=44, position=1),
                self._rr(dn=11, position=3),
            ],
            drivers=[
                self._driver(dn=44, team='Red Bull Racing'),
                self._driver(dn=11, team='Red Bull Racing'),
            ],
        )
        rbr = next(r for r in rows if r['team_name'] == 'Red Bull Racing')
        self.assertEqual(rbr['podiums'], 2)

    def test_team_dnf_counted(self):
        rows = self._run_and_capture(
            meetings=[self._meeting()],
            sessions=[self._session()],
            race_results=[self._rr(dn=44, position=None, dnf=True)],
            drivers=[self._driver(dn=44, team='Red Bull Racing')],
        )
        rbr = next(r for r in rows if r['team_name'] == 'Red Bull Racing')
        self.assertEqual(rbr['dnf_count'], 1)

    def test_team_points_from_championship_teams(self):
        rows = self._run_and_capture(
            meetings=[self._meeting()],
            sessions=[self._session()],
            race_results=[self._rr(dn=44, position=1)],
            champ_teams=[self._ct(team='Red Bull Racing', pts=43.0)],
            drivers=[self._driver(dn=44, team='Red Bull Racing')],
        )
        rbr = next(r for r in rows if r['team_name'] == 'Red Bull Racing')
        self.assertEqual(rbr['points_scored'], 43.0)

    def test_team_pole_from_starting_grid(self):
        rows = self._run_and_capture(
            meetings=[self._meeting()],
            sessions=[
                self._session(sk=1001, session_type='Race'),
                self._session(sk=2001, session_type='Qualifying'),
            ],
            race_results=[self._rr(sk=1001, dn=44, position=2)],
            starting_grid=[self._sg(sk=2001, dn=44, pos=1)],
            drivers=[
                self._driver(sk=1001, dn=44, team='Red Bull Racing'),
                self._driver(sk=2001, dn=44, team='Red Bull Racing'),
            ],
        )
        rbr = next(r for r in rows if r['team_name'] == 'Red Bull Racing')
        self.assertEqual(rbr['poles'], 1)

    def test_pit_stops_counted(self):
        rows = self._run_and_capture(
            meetings=[self._meeting()],
            sessions=[self._session()],
            race_results=[self._rr(dn=44, position=1)],
            pit_stops=[self._ps(dn=44), self._ps(dn=44)],
            drivers=[self._driver(dn=44, team='Red Bull Racing')],
        )
        rbr = next(r for r in rows if r['team_name'] == 'Red Bull Racing')
        self.assertEqual(rbr['total_pit_stops'], 2)

    def test_multi_round_accumulation(self):
        rows = self._run_and_capture(
            meetings=[
                self._meeting(key=101, date='2026-03-01'),
                self._meeting(key=102, date='2026-03-15'),
            ],
            sessions=[
                self._session(sk=1001, meeting_key=101, session_type='Race'),
                self._session(sk=1002, meeting_key=102, session_type='Race'),
            ],
            race_results=[
                self._rr(sk=1001, dn=44, position=1),
                self._rr(sk=1002, dn=44, position=2),
            ],
            champ_teams=[
                self._ct(sk=1001, pts=25.0),
                self._ct(sk=1002, pts=43.0),
            ],
            drivers=[
                self._driver(sk=1001, dn=44, team='Red Bull Racing'),
                self._driver(sk=1002, dn=44, team='Red Bull Racing'),
            ],
        )
        r1 = next(r for r in rows if r['round_number'] == 1)
        r2 = next(r for r in rows if r['round_number'] == 2)
        self.assertEqual(r1['wins'], 1)
        self.assertEqual(r2['wins'], 1)         # P2 not a win
        self.assertEqual(r2['podiums'], 2)       # cumulative
        self.assertEqual(r2['points_scored'], 43.0)

    def test_row_has_required_keys(self):
        rows = self._run_and_capture(
            meetings=[self._meeting()],
            sessions=[self._session()],
            race_results=[self._rr(dn=44, position=1)],
            drivers=[self._driver(dn=44)],
        )
        self.assertIn('year', rows[0])
        self.assertIn('round_number', rows[0])
        self.assertIn('team_name', rows[0])
        self.assertEqual(rows[0]['year'], 2026)


if __name__ == "__main__":
    unittest.main()
