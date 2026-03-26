"""Comprehensive unit and integration tests for pipeline/ingest.py."""

import unittest
from unittest.mock import MagicMock, call, patch

import httpx

import ingest
from ingest import (
    _assign_qualifying_phases,
    _compute_peak_g,
    _get_compound_for_lap,
    _normalize_phase,
    _parse_gap,
    _pick,
    _upsert,
    ingest_championship_drivers,
    ingest_championship_teams,
    ingest_drivers,
    ingest_intervals,
    ingest_lap_metrics,
    ingest_laps,
    ingest_meeting,
    ingest_overtakes,
    ingest_pit_stops,
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
    def test_race_calls_race_specific_functions(
        self, mock_starting_grid, mock_qual_results, mock_champ_teams,
        mock_champ_drivers, mock_intervals, mock_overtakes, mock_race_results,
        mock_race_control, mock_weather, mock_pit_stops, mock_stints,
        mock_laps, mock_drivers, mock_session, mock_meeting, mock_get_session,
        mock_reset_stats,
    ):
        client = _mock_client()

        process_session(client, 9000)

        mock_race_results.assert_called_once()
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
    def test_race_does_not_call_qualifying_functions(
        self, mock_starting_grid, mock_qual_results, mock_champ_teams,
        mock_champ_drivers, mock_intervals, mock_overtakes, mock_race_results,
        mock_race_control, mock_weather, mock_pit_stops, mock_stints,
        mock_laps, mock_drivers, mock_session, mock_meeting, mock_get_session,
        mock_reset_stats,
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
    def test_race_calls_common_functions(
        self, mock_starting_grid, mock_qual_results, mock_champ_teams,
        mock_champ_drivers, mock_intervals, mock_overtakes, mock_race_results,
        mock_race_control, mock_weather, mock_pit_stops, mock_stints,
        mock_laps, mock_drivers, mock_session, mock_meeting, mock_get_session,
        mock_reset_stats,
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
    def test_qualifying_does_not_call_race_functions(
        self, mock_starting_grid, mock_qual_results, mock_champ_teams,
        mock_champ_drivers, mock_intervals, mock_overtakes, mock_race_results,
        mock_race_control, mock_weather, mock_pit_stops, mock_stints,
        mock_laps, mock_drivers, mock_session, mock_meeting, mock_get_session,
        mock_reset_stats,
    ):
        client = _mock_client()

        process_session(client, 9001)

        mock_race_results.assert_not_called()
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
    def test_sprint_calls_race_specific_functions(
        self, mock_starting_grid, mock_qual_results, mock_champ_teams,
        mock_champ_drivers, mock_intervals, mock_overtakes, mock_race_results,
        mock_race_control, mock_weather, mock_pit_stops, mock_stints,
        mock_laps, mock_drivers, mock_session, mock_meeting, mock_get_session,
        mock_reset_stats,
    ):
        client = _mock_client()

        process_session(client, 9002)

        mock_race_results.assert_called_once()
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
# _compute_peak_g
# ===========================================================================

def _make_car_data(n=80, throttle=100, brake=0, speed_slope=0.5):
    """Generate synthetic car_data records at 1 Hz with controllable throttle/brake.

    Uses whole-second timestamps to avoid pandas ISO8601 microsecond ambiguity.
    Records at 1 Hz are fine: _compute_peak_g resamples to 4 Hz via PCHIP.
    """
    import datetime
    base = datetime.datetime(2024, 3, 2, 13, 0, 0)
    records = []
    for i in range(n):
        records.append({
            'date': (base + datetime.timedelta(seconds=i)).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'speed': 200.0 + speed_slope * i,   # gently accelerating
            'throttle': throttle,
            'brake': brake,
        })
    return records


class TestComputePeakG(unittest.TestCase):

    def test_returns_none_for_empty(self):
        self.assertIsNone(_compute_peak_g([]))

    def test_returns_none_for_too_few_records(self):
        self.assertIsNone(_compute_peak_g(_make_car_data(n=10)))

    def test_returns_tuple_for_valid_data(self):
        result = _compute_peak_g(_make_car_data(n=80, throttle=100, brake=0))
        self.assertIsNotNone(result)
        peak_accel, peak_decel_abs = result
        # With throttle=100 there should be an accel window
        self.assertIsNotNone(peak_accel)

    def test_accel_none_when_throttle_always_low(self):
        # throttle=0 → accel_mask always False → peak_accel_g should be None
        result = _compute_peak_g(_make_car_data(n=80, throttle=0, brake=0))
        self.assertIsNotNone(result)
        peak_accel, _ = result
        self.assertIsNone(peak_accel)

    def test_decel_captured_when_braking(self):
        # brake=100 → decel_mask always True → peak_decel_g_abs should be non-None
        result = _compute_peak_g(_make_car_data(n=80, throttle=0, brake=100, speed_slope=-0.5))
        self.assertIsNotNone(result)
        _, peak_decel_abs = result
        self.assertIsNotNone(peak_decel_abs)
        self.assertGreaterEqual(peak_decel_abs, 0.0)

    def test_peak_accel_g_is_positive(self):
        result = _compute_peak_g(_make_car_data(n=80, throttle=100, brake=0, speed_slope=1.0))
        self.assertIsNotNone(result)
        peak_accel, _ = result
        if peak_accel is not None:
            self.assertGreater(peak_accel, 0.0)

    def test_returns_none_for_insufficient_after_dedup(self):
        # All records have identical speed → dedup removes them all → < 10 clean rows
        import datetime
        base = datetime.datetime(2024, 3, 2, 13, 0, 0)
        records = [
            {
                'date': (base + datetime.timedelta(seconds=i)).strftime('%Y-%m-%dT%H:%M:%SZ'),
                'speed': 200.0,  # constant speed → long run → deduped out
                'throttle': 50,
                'brake': 0,
            }
            for i in range(80)
        ]
        result = _compute_peak_g(records)
        self.assertIsNone(result)


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


class TestIngestLapMetrics(unittest.TestCase):

    @patch("ingest.openf1.get_car_data")
    def test_empty_car_data_produces_no_rows(self, mock_get_car_data):
        mock_get_car_data.return_value = []
        client = _mock_client()
        stats = ingest_lap_metrics(client, 9158, _make_laps(5))
        # No car data → _upsert called with empty rows (exits early), driver_stats empty
        client.table.assert_not_called()
        self.assertEqual(stats, {})

    @patch("ingest.openf1.get_car_data")
    def test_pit_out_laps_excluded(self, mock_get_car_data):
        mock_get_car_data.return_value = []
        client = _mock_client()
        laps = _make_laps(3)
        laps[1]['is_pit_out_lap'] = True
        ingest_lap_metrics(client, 9158, laps)
        # Pit-out lap excluded from driver laps, but the driver still has 2 laps
        # and we still call get_car_data if ≥ 2 non-pit-out laps remain
        mock_get_car_data.assert_called_once()

    @patch("ingest.openf1.get_car_data")
    def test_single_non_pit_lap_skips_driver(self, mock_get_car_data):
        mock_get_car_data.return_value = []
        client = _mock_client()
        # Only 1 non-pit-out lap → can't slice between consecutive laps → skip driver
        laps = _make_laps(2)
        laps[1]['is_pit_out_lap'] = True
        stats = ingest_lap_metrics(client, 9158, laps)
        mock_get_car_data.assert_not_called()
        self.assertEqual(stats, {})

    @patch("ingest._compute_peak_g")
    @patch("ingest.openf1.get_car_data")
    def test_valid_laps_upserted_and_returned(self, mock_get_car_data, mock_cpg):
        mock_get_car_data.return_value = _make_car_data(n=80)
        mock_cpg.return_value = (1.5, 3.2)
        client = _mock_client()

        stats = ingest_lap_metrics(client, 9158, _make_laps(3, driver_number=44))

        # 3 laps → 2 intervals → 2 peak_g calls → 2 rows upserted
        self.assertEqual(mock_cpg.call_count, 2)
        self.assertIn(44, stats)
        self.assertEqual(len(stats[44]), 2)
        self.assertEqual(stats[44][0], (1.5, 3.2))

    @patch("ingest._compute_peak_g")
    @patch("ingest.openf1.get_car_data")
    def test_none_peak_g_laps_excluded_from_stats(self, mock_get_car_data, mock_cpg):
        mock_get_car_data.return_value = _make_car_data(n=80)
        # First lap: valid; second: accel window empty
        mock_cpg.side_effect = [(1.5, 3.2), (None, 2.1)]
        client = _mock_client()

        stats = ingest_lap_metrics(client, 9158, _make_laps(3, driver_number=44))

        self.assertIn(44, stats)
        self.assertEqual(len(stats[44]), 1)   # only the valid lap


if __name__ == "__main__":
    unittest.main()
