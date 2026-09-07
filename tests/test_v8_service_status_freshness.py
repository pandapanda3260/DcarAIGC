"""Service freshness reads real anchor timestamps without inventing capture success."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from v8 import api, runtime_receipts
from v8.storage import connect, initialize_database


REFERENCE = datetime(2026, 9, 7, 8, tzinfo=timezone.utc)
STARTED_AT = "2026-09-07T00:00:00Z"
COMPLETED_AT = "2026-09-07T00:07:23Z"


class ServiceStatusFreshnessTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(prefix="dcar-freshness-test-")
        self.addCleanup(temporary.cleanup)
        self.db = Path(temporary.name) / "fixture.sqlite3"
        self.assertNotEqual(self.db.resolve(), api.DEFAULT_DB.resolve())
        with connect(self.db) as connection:
            initialize_database(connection)

    def _anchor(self, status: str, completed_at: str | None = COMPLETED_AT) -> int:
        with connect(self.db) as connection:
            cursor = connection.execute(
                "INSERT INTO scheduler_runs("
                "job_id,scheduled_for,status,started_at,completed_at,details_json"
                ") VALUES ('tikhub_reconcile',?,?,?,?,?)",
                (
                    STARTED_AT,
                    status,
                    STARTED_AT,
                    completed_at,
                    json.dumps({"identity": {"scheduled_at": STARTED_AT}}),
                ),
            )
            connection.commit()
            return int(cursor.lastrowid)

    def _freshness(self, *, run_id: int | None, complete: bool, status: str):
        coverage = {"round_run_id": run_id, "complete": complete, "status": status}
        with (
            connect(self.db) as connection,
            patch.object(runtime_receipts, "latest_runtime_coverage", return_value=coverage) as read,
        ):
            result = api._data_freshness(connection, current_at=REFERENCE)
            read.assert_called_once_with(connection, at="2026-09-07T08:00:00Z")
        self.assertEqual(result["basis"], "profile-day-coverage-receipt-v2")
        self.assertEqual(result["discovery_coverage"], coverage)
        return result

    def test_complete_partial_anchor_keeps_its_actual_completion_time(self) -> None:
        result = self._freshness(run_id=self._anchor("partial"), complete=True, status="complete")
        self.assertEqual(result["status"], "current")
        self.assertEqual(result["last_successful_capture_at"], COMPLETED_AT)
        self.assertEqual(result["latest_capture_run"]["status"], "partial")

    def test_complete_successful_anchor_keeps_its_actual_completion_time(self) -> None:
        result = self._freshness(run_id=self._anchor("succeeded"), complete=True, status="complete")
        self.assertEqual(result["status"], "current")
        self.assertEqual(result["last_successful_capture_at"], COMPLETED_AT)
        self.assertEqual(result["latest_capture_run"]["scheduled_for"], STARTED_AT)

    def test_incomplete_partial_anchor_does_not_claim_capture_success(self) -> None:
        result = self._freshness(run_id=self._anchor("partial"), complete=False, status="partial_publishable")
        self.assertEqual(result["status"], "stale")
        self.assertIsNone(result["last_successful_capture_at"])
        self.assertEqual(result["latest_capture_run"]["completed_at"], COMPLETED_AT)

    def test_failed_anchor_does_not_claim_success_even_if_coverage_says_complete(self) -> None:
        result = self._freshness(run_id=self._anchor("failed"), complete=True, status="complete")
        self.assertIsNone(result["last_successful_capture_at"])
        self.assertEqual(result["latest_capture_run"]["status"], "failed")

    def test_null_completion_time_never_falls_back_to_current_time(self) -> None:
        result = self._freshness(run_id=self._anchor("partial", None), complete=True, status="complete")
        self.assertIsNone(result["last_successful_capture_at"])
        self.assertIsNone(result["latest_capture_run"]["completed_at"])

    def test_empty_completion_time_never_falls_back_to_current_time(self) -> None:
        result = self._freshness(run_id=self._anchor("partial", ""), complete=True, status="complete")
        # Legacy empty values are not a successful timestamp; the UI omits either empty form.
        self.assertFalse(result["last_successful_capture_at"])
        self.assertEqual(result["latest_capture_run"]["completed_at"], "")

    def test_absent_round_does_not_fall_back_to_an_unrelated_success(self) -> None:
        self._anchor("succeeded")
        result = self._freshness(run_id=None, complete=False, status="unknown")
        self.assertEqual(result["status"], "unknown")
        self.assertIsNone(result["last_successful_capture_at"])
        self.assertIsNone(result["latest_capture_run"])
        self.assertTrue(all(value is None for value in result["stage_data"].values()))

    def test_invalid_receipt_is_not_downgraded_to_unknown_or_current(self) -> None:
        with (
            connect(self.db) as connection,
            patch.object(
                runtime_receipts,
                "latest_runtime_coverage",
                side_effect=runtime_receipts.RuntimeReceiptError("invalid receipt fixture"),
            ),
            self.assertRaisesRegex(runtime_receipts.RuntimeReceiptError, "invalid receipt fixture"),
        ):
            api._data_freshness(connection, current_at=REFERENCE)


if __name__ == "__main__":
    unittest.main()
