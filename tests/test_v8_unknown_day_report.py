"""Unknown native day receipts remain traceable, but cannot imply coverage."""
from __future__ import annotations

import copy
import sqlite3
import unittest
from unittest.mock import patch

from tests import test_v8_report_inputs as fixtures
from v8 import contracts, pipeline_cutover, report_inputs, runtime_receipts


T0, T1 = fixtures.T0, fixtures.T1


class UnknownDayReportTest(unittest.TestCase):
    def period(self):
        day = copy.deepcopy(fixtures.FrozenReportIntegrationTest._profile_period_coverage()["days"][1])
        day.update(known=False, complete=False, partial_publishable=False,
                   reason="catalog_plan_invalid")
        for key in ("eligible_identity_ids", "succeeded_identity_ids", "blocked_identity_ids",
                    "not_applicable_identity_ids", "accounted_identity_ids", "required_identity_ids"):
            day[key] = []
        receipt = {"run_id": 32, "attempt_id": 42, "self_sha256": "d" * 64,
                   "summary": {"sequence": 1, "coverage": {"days": [day], "scan_errors": {}}}}
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        with patch.object(runtime_receipts, "read_profile_day_coverage_receipt", return_value=receipt):
            return runtime_receipts.period_coverage_from_receipts(connection,
                period_start="2026-07-01", period_end="2026-07-01", cutoff_at=T0)

    def compact(self, period):
        return report_inputs.compact_profile_day_scan_inputs(period,
            period_start="2026-07-01", period_end="2026-07-01")

    def test_unknown_native_receipt_is_not_complete_or_zero_publication(self):
        period = self.period()
        self.assertFalse(period["complete"])
        self.assertFalse(period["partial_publishable"])
        self.assertFalse(period["roster_evidence_valid"])
        self.assertFalse(period["scan_traceable"])
        self.assertEqual(period["discovery_coverage"]["status"], "unknown")
        self.assertIsNone(period["discovery_coverage"]["percentage"])
        self.assertIsNone(period["discovery_coverage"]["accounted_percentage"])
        self.assertEqual(period["pipeline_observation"]["legacy_unobserved_dates"], ["2026-07-01"])
        self.assertEqual(period["pipeline_observation"]["zero_content_dates"], [])
        compact = self.compact(period)
        self.assertEqual(compact["receipt_references"][0]["run_id"], 32)
        self.assertEqual(compact["period_coverage_sha256"], report_inputs.digest(period))

    def test_unknown_receipt_report_is_partial_and_frozen_across_retry(self):
        fixture = fixtures.FrozenReportIntegrationTest()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        task = fixture.task()
        period = self.period()
        with patch.object(runtime_receipts, "period_coverage_from_receipts", return_value=period):
            report = fixture.run_report(task)
        contracts.validate_report(report)
        self.assertEqual(report["task"]["task_status"], "partial")
        self.assertIsNone(report["data_quality"]["discovery_coverage"])
        self.assertFalse(report["data_quality"]["scan_traceable"])
        self.assertEqual(report["data_quality_details"]["discovery_coverage"]["status"], "unknown")
        references = report["input_references"]["scans"]
        self.assertEqual(references["receipt_references"][0]["self_sha256"], "d" * 64)
        with patch.object(runtime_receipts, "period_coverage_from_receipts",
                          side_effect=AssertionError("retry must keep the frozen receipt")):
            retried = fixture.run_report(task, at=T1)
        self.assertEqual(retried["input_references"]["scans"], references)
        self.assertEqual(retried["frozen_inputs"], report["frozen_inputs"])
        self.assertEqual(retried["task"]["task_status"], "partial")

    def test_unknown_reference_cannot_claim_complete_or_change_identity(self):
        for scope, key in (("day", "complete"), ("day", "partial_publishable"),
                           ("period", "complete"), ("period", "partial_publishable")):
            with self.subTest(scope=scope, key=key):
                period = self.period()
                (period["days"][0] if scope == "day" else period)[key] = True
                with self.assertRaises(report_inputs.FrozenInputError):
                    self.compact(period)

        for key, value in (("run_id", 0), ("attempt_id", True), ("self_sha256", "invalid"),
                           ("business_day", "2026-07-02")):
            with self.subTest(key=key):
                period = self.period()
                period["scan_references"][0][key] = value
                with self.assertRaises(report_inputs.FrozenInputError):
                    self.compact(period)

    def test_publisher_keeps_and_validates_unknown_receipt_reference(self):
        period = self.period()
        compact = self.compact(period)
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        with patch.object(runtime_receipts, "period_coverage_from_receipts", return_value=period):
            pipeline_cutover.verify_frozen_scans(connection, compact,
                period_start="2026-07-01", period_end="2026-07-01", cutoff_at=T0)
            for mutation in ("drop", "identity", "period"):
                changed = copy.deepcopy(compact)
                if mutation == "drop":
                    changed["receipt_references"] = []
                elif mutation == "identity":
                    changed["receipt_references"][0]["activation_id"] += 1
                else:
                    changed["period_coverage_sha256"] = "a" * 64
                changed["receipt_references_sha256"] = report_inputs.digest(changed["receipt_references"])
                changed["self_sha256"] = report_inputs.digest({k: v for k, v in changed.items() if k != "self_sha256"})
                with self.subTest(mutation=mutation), self.assertRaises(pipeline_cutover.PublicationEvidenceError):
                    pipeline_cutover.verify_frozen_scans(connection, changed,
                        period_start="2026-07-01", period_end="2026-07-01", cutoff_at=T0)

    def test_mixed_known_unknown_and_missing_days_do_not_hide_the_gap(self):
        known, unknown = copy.deepcopy(fixtures.FrozenReportIntegrationTest._profile_period_coverage()["days"])
        unknown.update(known=False, complete=False, partial_publishable=False, reason="catalog_plan_invalid")
        def read(_connection, *, business_day, at):
            if business_day == "2026-07-02":
                return None
            day = known if business_day == known["date"] else unknown
            return {"run_id": 31, "attempt_id": 41, "self_sha256": "c" * 64,
                    "summary": {"sequence": 1, "coverage": {"days": [day]}}}
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        with patch.object(runtime_receipts, "read_profile_day_coverage_receipt", side_effect=read):
            period = runtime_receipts.period_coverage_from_receipts(connection,
                period_start="2026-06-30", period_end="2026-07-02", cutoff_at=T0)
        self.assertFalse(period["complete"])
        self.assertEqual(period["discovery_coverage"]["observed_occurrence_count"], 1)
        self.assertIsNone(period["discovery_coverage"]["percentage"])
        self.assertEqual(period["pipeline_observation"]["legacy_unobserved_dates"], ["2026-07-01", "2026-07-02"])
        compact = report_inputs.compact_profile_day_scan_inputs(period,
            period_start="2026-06-30", period_end="2026-07-02")
        self.assertEqual([item["business_day"] for item in compact["receipt_references"]], ["2026-06-30", "2026-07-01"])


if __name__ == "__main__":
    unittest.main()
