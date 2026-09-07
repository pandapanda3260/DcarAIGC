"""Current v8.9 freeze/lineage gates; v8.7 numeric tests remain version-pinned."""
from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import unittest
from unittest.mock import patch

from tests import test_v8_reports as fixtures
from tests import test_v8_media_completion as media_fixtures
from tests import v9_report_fixture
from tests.test_v8_contract import valid_report as legacy_report
from v8 import (
    contracts,
    evaluation,
    media_state,
    metric_observations,
    report_inputs,
    reports,
    scheduler,
)
from v8.storage import connect, transaction

T0 = "2026-09-01T00:00:00Z"
T1 = "2026-09-02T00:00:00Z"
FIXTURE_AT = "2026-08-31T23:00:00Z"


def current_report():
    report = legacy_report()
    report["report_version"] = contracts.CURRENT_REPORT_VERSION
    report["data_quality"].update(roster_evidence_valid=True, scan_traceable=True, scope_reconstructable=True)
    report["data_quality"]["discovery_coverage"] = 100.0
    report["data_quality_details"]["discovery_coverage"].update(
        covered_identity_occurrence_count=10,
        succeeded_identity_occurrence_count=10,
        blocked_identity_occurrence_count=0,
        not_applicable_identity_occurrence_count=0,
        accounted_identity_occurrence_count=10,
        required_identity_occurrence_count=10,
        accounted_percentage=100.0,
        percentage=100.0,
        eligible_basis="frozen_profile_roster_identity_occurrences",
        complete=True,
        partial_publishable=False,
        success_rule="profile-day-coverage-v1",
    )
    report["input_references"] = {"content_ids": list(range(1, 11)), "scans": {"complete": True}}
    return seal(report)


def v88_report():
    report = current_report()
    report["report_version"] = "dcar-content-operations-report-v8.8"
    discovery = report["data_quality_details"]["discovery_coverage"]
    for field in (
        "succeeded_identity_occurrence_count",
        "blocked_identity_occurrence_count",
        "not_applicable_identity_occurrence_count",
        "accounted_identity_occurrence_count",
        "required_identity_occurrence_count",
        "accounted_percentage",
        "complete",
        "partial_publishable",
        "success_rule",
    ):
        discovery.pop(field)
    discovery["eligible_basis"] = "frozen_matrix_roster_identity_occurrences"
    return seal(report)


def seal(report):
    body = copy.deepcopy(report)
    body.pop("frozen_inputs", None)
    body.pop("files", None)
    body["metadata"].pop("revision", None)
    body["metadata"].pop("generated_at", None)
    report["frozen_inputs"] = {"contract_version": "report-inputs-v1", "event_id": 1, "sha256": report_inputs.digest(body)}
    return report


class CurrentReportContractTest(unittest.TestCase):
    def test_current_fixture_and_old_contract_both_validate(self):
        contracts.validate_report(current_report())
        contracts.validate_report(v88_report())
        contracts.validate_report(legacy_report())
        self.assertEqual(contracts.CURRENT_REPORT_VERSION, "dcar-content-operations-report-v8.9")
        self.assertEqual(
            contracts.load_contract(
                report_version="dcar-content-operations-report-v8.8"
            )["report_version"],
            "dcar-content-operations-report-v8.8",
        )
        self.assertEqual(contracts.load_contract(report_version="dcar-content-operations-report-v8.7")["report_version"], "dcar-content-operations-report-v8.7")

    def test_freeze_hash_detects_fact_change_but_allows_new_revision_paths(self):
        report = current_report()
        report["metadata"].update(revision=2, generated_at="2026-08-03T00:00:00Z")
        report["files"] = [{"file_kind": "report-json", "path": "report.json", "status": "available"}]
        contracts.validate_report(report)
        report["summary_metrics"]["view_count"]["value"] += 1
        with self.assertRaisesRegex(contracts.V8ContractViolation, "frozen_inputs.sha256"):
            contracts.validate_report(report)

    def test_all_three_hard_gates_apply_even_to_zero_publication_or_skipped_boolean_mode(self):
        for field in ("roster_evidence_valid", "scan_traceable", "scope_reconstructable"):
            with self.subTest(field=field):
                report = current_report()
                report["data_quality"][field] = False
                self.assertEqual(contracts.expected_terminal_task_status(report["data_quality"],
                    data_quality_details=report["data_quality_details"], enforce_boolean_quality_gates=False), "partial")
                report["task"]["task_status"] = "partial"
                contracts.validate_report(seal(report))
                del report["data_quality"][field]
                with self.assertRaisesRegex(contracts.V8ContractViolation, field):
                    contracts.validate_report(seal(report))

    def test_existing_ninety_percent_boundaries_remain_exact_in_current_contract(self):
        for field in contracts.load_contract()["required_coverage_thresholds"]:
            for value, expected in ((90.0, "succeeded"), (89.99, "partial")):
                with self.subTest(field=field, value=value):
                    report = current_report()
                    report["data_quality"][field] = value
                    self.assertEqual(contracts.expected_terminal_task_status(report["data_quality"],
                        data_quality_details=report["data_quality_details"]), expected)

    def test_unknown_roster_is_not_a_zero_or_empty_success(self):
        report = current_report()
        report["data_quality"].update(roster_evidence_valid=False, scan_traceable=False, discovery_coverage=None)
        report["data_quality_details"]["discovery_coverage"].update(
            status="unknown",
            covered_identity_occurrence_count=0,
            succeeded_identity_occurrence_count=0,
            blocked_identity_occurrence_count=0,
            accounted_identity_occurrence_count=0,
            observed_occurrence_count=0,
            percentage=None,
            accounted_percentage=None,
            complete=False,
            partial_publishable=False,
            reason="missing frozen roster",
        )
        report["task"]["task_status"] = "partial"
        contracts.validate_report(seal(report))
        report["task"]["task_status"] = "succeeded"
        with self.assertRaisesRegex(contracts.V8ContractViolation, "must be partial"):
            contracts.validate_report(seal(report))

    def test_numeric_id_mapping_hash_survives_json_round_trip(self):
        value = {"metrics": {10: {"raw_id": 100}, 2: {"raw_id": 2}}}
        self.assertEqual(report_inputs.digest(value), report_inputs.digest(json.loads(json.dumps(value))))

    def test_profile_day_discovery_basis_requires_complete_terminal_contract(self):
        report = current_report()
        report["data_quality"]["discovery_coverage"] = 90.0
        discovery = report["data_quality_details"]["discovery_coverage"]
        discovery.update(
            covered_identity_occurrence_count=9,
            succeeded_identity_occurrence_count=9,
            blocked_identity_occurrence_count=1,
            not_applicable_identity_occurrence_count=0,
            accounted_identity_occurrence_count=10,
            required_identity_occurrence_count=10,
            accounted_percentage=100.0,
            percentage=90.0,
            eligible_basis="frozen_profile_roster_identity_occurrences",
            complete=False,
            partial_publishable=True,
            success_rule="profile-day-coverage-v1",
            reason="全部义务已终态且满足部分发布门槛",
        )
        contracts.validate_report(seal(report))

        for marker in (None, "profile-day-coverage-v2"):
            with self.subTest(marker=marker):
                invalid = copy.deepcopy(report)
                if marker is None:
                    invalid["data_quality_details"]["discovery_coverage"].pop(
                        "success_rule"
                    )
                else:
                    invalid["data_quality_details"]["discovery_coverage"][
                        "success_rule"
                    ] = marker
                with self.assertRaisesRegex(
                    contracts.V8ContractViolation,
                    "success_rule must equal profile-day-coverage-v1",
                ):
                    contracts.validate_report(seal(invalid))

        incomplete = copy.deepcopy(report)
        incomplete["data_quality_details"]["discovery_coverage"].pop(
            "blocked_identity_occurrence_count"
        )
        with self.assertRaisesRegex(
            contracts.V8ContractViolation,
            "missing .*blocked_identity_occurrence_count",
        ):
            contracts.validate_report(seal(incomplete))

        wrong_basis = copy.deepcopy(report)
        wrong_basis["data_quality_details"]["discovery_coverage"][
            "eligible_basis"
        ] = "frozen_matrix_roster_identity_occurrences"
        with self.assertRaisesRegex(
            contracts.V8ContractViolation,
            "eligible_basis must equal frozen_profile_roster_identity_occurrences",
        ):
            contracts.validate_report(seal(wrong_basis))

        historical = copy.deepcopy(report)
        historical["report_version"] = "dcar-content-operations-report-v8.8"
        with self.assertRaisesRegex(
            contracts.V8ContractViolation,
            "eligible_basis must equal frozen_matrix_roster_identity_occurrences",
        ):
            contracts.validate_report(seal(historical))


class ManagedMediaCutoffTest(unittest.TestCase):
    def test_actual_registered_source_closes_only_at_or_after_its_evidence_cutoff(self):
        fixture = media_fixtures.MediaCompletionTest()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.fixture._patch(evaluation, "now_utc", side_effect=lambda: fixture.fixture.now)
        bundle = fixture.ready()
        self.assertIn("artifact_id", bundle["manifest"]["source"])
        with connect(fixture.db) as connection:
            release_id = connection.execute("SELECT id FROM evaluation_releases WHERE status='active'").fetchone()[0]
            current = media_state.media_terminal_state_details(
                connection, release_id, [1], cutoff_at=fixture.fixture.now,
            )[1]
            previous = media_state.media_terminal_state_details(
                connection, release_id, [1], cutoff_at="2000-01-01T00:00:00Z",
            )[1]
        self.assertEqual(current.state, "complete", current)
        self.assertEqual(previous.state, "pending", previous)
        self.assertEqual(previous.reason, "managed_source_pending")


class FrozenReportIntegrationTest(unittest.TestCase):
    def setUp(self):
        for module in (fixtures, v9_report_fixture, evaluation, metric_observations):
            patcher = patch.object(module, "now_utc", return_value=FIXTURE_AT)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.fixture = fixtures.V8ReportTaskTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.db = self.fixture.db
        self.root = self.fixture.reports_root

    def task(self, *, period="2026-07-01", at=T0, automatic=False):
        with patch.object(reports, "now_utc", return_value=at):
            return reports.create_task(task_type="daily" if automatic else "custom", period_start=period,
                period_end=period, creation_source="automatic" if automatic else "manual", db_path=self.db)

    def run_report(self, task, *, at=T0):
        with patch.object(reports, "now_utc", return_value=at):
            return reports.run_task(task["id"], db_path=self.db, reports_root=self.root)

    @staticmethod
    def _profile_period_coverage():
        days = [
            {
                "date": "2026-06-30",
                "known": True,
                "complete": True,
                "partial_publishable": False,
                "activation_id": 11,
                "profile_id": "matrix_hybrid_v1",
                "activation_sha256": "e" * 64,
                "source_family": "matrix",
                "roster_snapshot_id": 21,
                "roster_snapshot_hash": "a" * 64,
                "eligible_identity_ids": [101],
                "succeeded_identity_ids": [101],
                "blocked_identity_ids": [],
                "not_applicable_identity_ids": [],
                "accounted_identity_ids": [101],
                "required_identity_ids": [101],
            },
            {
                "date": "2026-07-01",
                "known": True,
                "complete": True,
                "partial_publishable": False,
                "activation_id": 12,
                "profile_id": "tikhub_managed_v1",
                "activation_sha256": "f" * 64,
                "source_family": "system",
                "roster_snapshot_id": 22,
                "roster_snapshot_hash": "b" * 64,
                "eligible_identity_ids": [202],
                "succeeded_identity_ids": [202],
                "blocked_identity_ids": [],
                "not_applicable_identity_ids": [],
                "accounted_identity_ids": [202],
                "required_identity_ids": [202],
            },
        ]
        discovery = {
            "status": "available",
            "covered_identity_occurrence_count": 2,
            "eligible_identity_occurrence_count": 2,
            "succeeded_identity_occurrence_count": 2,
            "blocked_identity_occurrence_count": 0,
            "not_applicable_identity_occurrence_count": 0,
            "accounted_identity_occurrence_count": 2,
            "required_identity_occurrence_count": 2,
            "accounted_percentage": 100.0,
            "observed_occurrence_count": 2,
            "expected_occurrence_count": 2,
            "percentage": 100.0,
            "eligible_basis": "frozen_profile_roster_identity_occurrences",
            "complete": True,
            "partial_publishable": False,
            "missing_occurrence_dates": [],
            "roster_validation_failures": 0,
            "success_rule": "profile-day-coverage-v1",
            "reason": "",
        }
        return {
            "contract_version": "profile-day-coverage-period-v1",
            "cutoff_at": T0,
            "days": days,
            "discovery_coverage": discovery,
            "pipeline_observation": {
                "status": "complete",
                "capture_observation_start_date": None,
                "expected_dates": ["2026-06-30", "2026-07-01"],
                "legacy_unobserved_dates": [],
                "pipeline_gap_dates": [],
                "zero_content_dates": [],
            },
            "complete": True,
            "partial_publishable": False,
            "roster_evidence_valid": True,
            "scan_traceable": True,
            "scan_errors": {},
            "scan_references": [
                {
                    "business_day": "2026-06-30",
                    "run_id": 31,
                    "attempt_id": 41,
                    "sequence": 1,
                    "self_sha256": "c" * 64,
                },
                {
                    "business_day": "2026-07-01",
                    "run_id": 32,
                    "attempt_id": 42,
                    "sequence": 1,
                    "self_sha256": "d" * 64,
                },
            ],
        }

    def test_schema19_report_uses_compact_receipts_across_profile_days(self):
        with patch.object(reports, "now_utc", return_value=T0):
            task = reports.create_task(
                task_type="custom",
                period_start="2026-06-30",
                period_end="2026-07-01",
                creation_source="manual",
                db_path=self.db,
            )
        coverage = self._profile_period_coverage()
        with patch.object(
            reports.runtime_receipts,
            "period_coverage_from_receipts",
            return_value=coverage,
        ) as receipt_reader, patch.object(
            reports,
            "scan_coverage",
            side_effect=AssertionError("schema19 report must not verify raw scans"),
        ):
            report = self.run_report(task)
        receipt_reader.assert_called_once_with(
            unittest.mock.ANY,
            period_start="2026-06-30",
            period_end="2026-07-01",
            cutoff_at=T0,
        )
        self.assertEqual(report["data_quality"]["discovery_coverage"], 100.0)
        scans = report["input_references"]["scans"]
        self.assertEqual(scans["contract_version"], "report-profile-day-scan-inputs-v1")
        self.assertNotIn("days", scans)
        self.assertNotIn("discovery_coverage", scans)
        self.assertEqual(
            [
                (item["business_day"], item["activation_id"], item["profile_id"])
                for item in scans["receipt_references"]
            ],
            [
                ("2026-06-30", 11, "matrix_hybrid_v1"),
                ("2026-07-01", 12, "tikhub_managed_v1"),
            ],
        )
        self.assertEqual(
            [item["activation_sha256"] for item in scans["receipt_references"]],
            ["e" * 64, "f" * 64],
        )
        self.assertEqual(
            scans["receipt_references_sha256"],
            report_inputs.digest(scans["receipt_references"]),
        )
        sealed = dict(scans)
        self_sha256 = sealed.pop("self_sha256")
        self.assertEqual(self_sha256, report_inputs.digest(sealed))

        missing_reference = copy.deepcopy(coverage)
        missing_reference["scan_references"].pop()
        with self.assertRaisesRegex(
            reports.ReportTaskError,
            "do not cover every known day",
        ):
            reports._compact_profile_day_scan_inputs(
                missing_reference,
                period_start="2026-06-30",
                period_end="2026-07-01",
            )

    def test_schema19_missing_receipt_fails_closed_without_raw_verification(self):
        task = self.task()
        with patch.object(
            reports,
            "scan_coverage",
            side_effect=AssertionError("missing receipts must not fall back to raw scans"),
        ), patch(
            "v8.scan_receipts.verify_scan",
            side_effect=AssertionError("hot report path must not verify raw manifests"),
        ):
            report = self.run_report(task)
        self.assertEqual(report["task"]["task_status"], "partial")
        self.assertIsNone(report["data_quality"]["discovery_coverage"])
        self.assertFalse(report["data_quality"]["roster_evidence_valid"])
        self.assertFalse(report["data_quality"]["scan_traceable"])
        self.assertEqual(
            report["data_quality_details"]["discovery_coverage"]["status"],
            "unknown",
        )
        self.assertEqual(
            report["input_references"]["scans"]["receipt_references"], []
        )

    def test_schema18_report_keeps_legacy_scan_coverage_reader(self):
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.execute("PRAGMA user_version=18")
        legacy = {"contract_version": "matrix-first-coverage-v2", "complete": True}
        with patch.object(
            reports, "scan_coverage", return_value=legacy
        ) as legacy_reader, patch.object(
            reports.runtime_receipts,
            "period_coverage_from_receipts",
            side_effect=AssertionError("schema18 must retain its historical reader"),
        ):
            coverage, frozen = reports._report_scan_coverage(
                connection,
                period_start="2026-07-01",
                period_end="2026-07-01",
                cutoff_at=T0,
            )
        self.assertIs(coverage, legacy)
        self.assertIs(frozen, legacy)
        legacy_reader.assert_called_once_with(
            connection,
            period_start="2026-07-01",
            period_end="2026-07-01",
            cutoff_at=T0,
        )

    def test_scope_metrics_and_dimensions_are_frozen_across_retry(self):
        task = self.task()
        first = self.run_report(task)
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE content_items SET title='later title',manual_content_direction='new_car',updated_at=? WHERE id=1", (T1,))
            connection.execute("INSERT INTO content_items(link_id,platform,platform_content_id,canonical_url,published_at,content_type,imported_at,created_at,updated_at) "
                               "VALUES ('LATE01','douyin','late','https://www.douyin.com/video/late','2026-07-01T12:00:00Z','video',?,?,?)", (T1, T1, T1))
        reports.retry_task(task["id"], db_path=self.db)
        second = self.run_report(task, at=T1)
        self.assertEqual(first["frozen_inputs"], second["frozen_inputs"])
        for key in ("input_references", "summary_metrics", "content_details", "data_quality", "account_type_dimensions", "content_direction_dimensions"):
            self.assertEqual(first[key], second[key], key)
        self.assertEqual(second["metadata"]["revision"], 2)
        self.assertEqual(second["metadata"]["collection_cutoff_at"], T0)

    def test_automatic_empty_scope_is_frozen_and_does_not_expand(self):
        task = self.task(automatic=True)
        first = self.run_report(task)
        self.assertEqual(first["summary_metrics"]["publication_count"]["value"], 0)
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE content_items SET imported_at='2026-07-01T06:00:00Z',created_at='2026-07-01T06:00:00Z',updated_at='2026-07-01T06:00:00Z' WHERE id=1")
        reports.retry_task(task["id"], db_path=self.db)
        second = self.run_report(task, at=T1)
        self.assertEqual(second["summary_metrics"]["publication_count"]["value"], 0)
        self.assertEqual(second["metadata"]["collection_cutoff_at"], "2026-07-02T00:00:00Z")
        with connect(self.db) as connection:
            events = connection.execute("SELECT COUNT(*) FROM task_events WHERE task_id=? AND event_type='report_scope_v1'", (task["id"],)).fetchone()[0]
        self.assertEqual(events, 1)

    def test_delayed_first_report_does_not_backfill_current_account_dimensions(self):
        with connect(self.db) as connection, transaction(connection):
            connection.execute("INSERT INTO accounts(id,phone,account_type,content_direction,created_at,updated_at) VALUES (10,'','boutique_ip','new_car',?,?)", (T0, T1))
            connection.execute("UPDATE content_items SET account_id=10 WHERE id=1")
        report = self.run_report(self.task())
        self.assertFalse(report["data_quality"]["scope_reconstructable"])
        self.assertEqual(report["account_type_dimensions"][0]["key"], "unknown")
        self.assertIn("account_dimension_unreconstructable", report["data_quality_details"]["unknown_dimensions"]["1"])

    def test_late_evaluation_is_excluded_then_new_cutoff_correction_includes_it(self):
        with connect(self.db) as connection, transaction(connection):
            cid = self.fixture._insert_report_content(connection, suffix="late-eval")
            self.fixture._insert_report_evaluation(connection, content_id=cid, evidence_level="V3", included=1,
                direction="new_car", code="X2", evaluated_at=T1)
        task = self.task(period="2026-07-03")
        first = self.run_report(task)
        self.assertEqual(first["data_quality"]["evaluation_coverage"], 0)
        with patch.object(reports, "now_utc", return_value=T1):
            corrected = reports.create_correction_task(task["id"], reason="late evaluation", db_path=self.db)
        second = self.run_report(corrected, at=T1)
        self.assertNotEqual(task["id"], corrected["id"])
        self.assertEqual(second["data_quality"]["evaluation_coverage"], 100)
        self.assertEqual(second["metadata"]["collection_cutoff_at"], T1)
        with connect(self.db) as connection:
            event = json.loads(connection.execute("SELECT payload_json FROM task_events WHERE task_id=? AND event_type='corrects_report'", (corrected["id"],)).fetchone()[0])
        self.assertEqual(event["original_task_id"], task["id"])

    def test_tampered_task_event_is_rejected_without_reselection(self):
        task = self.task()
        self.run_report(task)
        with connect(self.db) as connection, transaction(connection):
            row = connection.execute("SELECT id,payload_json FROM task_events WHERE task_id=? AND event_type='report_inputs_v1'", (task["id"],)).fetchone()
            value = json.loads(row["payload_json"])
            value["payload"]["summary_metrics"]["publication_count"]["value"] = 999
            connection.execute("UPDATE task_events SET payload_json=? WHERE id=?", (json.dumps(value), row["id"]))
        reports.retry_task(task["id"], db_path=self.db)
        with self.assertRaisesRegex(report_inputs.FrozenInputError, "digest mismatch"):
            self.run_report(task, at=T1)

    def test_published_v87_is_read_only_and_requires_separate_correction(self):
        task = self.task()
        report = legacy_report()
        report["metadata"]["task_id"] = task["id"]
        path = self.fixture.root / "frozen-v87.json"
        path.write_text(json.dumps(report), encoding="utf-8")
        sha = hashlib.sha256(path.read_bytes()).hexdigest()
        with connect(self.db) as connection, transaction(connection):
            connection.execute("INSERT INTO report_revisions(task_id,revision,release_id,contract_version,rule_version,taxonomy_version,report_json_path,report_sha256,created_at) "
                "VALUES (?,1,?,'dcar-content-operations-report-v8.7','evaluation-v9','selling-points-v5.2',?,?,?)",
                (task["id"], self.fixture.release_id, str(path), sha, T0))
        with connect(self.db) as connection:
            before = [tuple(row) for row in connection.execute("SELECT * FROM task_events ORDER BY id")]
        self.assertEqual(self.run_report(task, at=T1), report)
        with self.assertRaisesRegex(reports.ReportTaskError, "read-only"):
            reports.retry_task(task["id"], db_path=self.db)
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), sha)
        with connect(self.db) as connection:
            self.assertEqual([tuple(row) for row in connection.execute("SELECT * FROM task_events ORDER BY id")], before)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM report_revisions WHERE task_id=?", (task["id"],)).fetchone()[0], 1)

    def test_snapshot_contains_exact_evidence_ids_and_cutoff_field_sources(self):
        task = self.task()
        report = self.run_report(task)
        refs = report["input_references"]
        self.assertEqual(refs["content_ids"], [1])
        self.assertEqual(report_inputs.digest(refs["source_policy"]), refs["source_policy_sha256"])
        self.assertTrue(refs["evidence_rows"]["envelopes"])
        for field in ("view_count", "like_count", "comment_count", "share_count", "collect_count"):
            self.assertIn(field, report["content_details"][0])
            self.assertIn(field, report["content_details"][0]["metric_sources"])
        contracts.validate_report(report)

    def test_scheduler_does_not_retry_frozen_task_on_later_fingerprint_watermark(self):
        task = self.task(automatic=True)
        self.run_report(task)
        occurrence = reports.datetime(2026, 7, 2, 8, 0, tzinfo=reports.SHANGHAI)
        self.assertIsNone(scheduler._report_duplicate_input_retry_before("daily_report", occurrence, db_path=self.db))


if __name__ == "__main__":
    unittest.main()
