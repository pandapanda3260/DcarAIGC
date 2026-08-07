from __future__ import annotations

import json
import tomllib
import unittest
from pathlib import Path

from v8.contracts import (
    CONTRACT_PATH,
    CURRENT_REPORT_EVIDENCE_VERSION,
    CURRENT_REPORT_RULE_VERSION,
    CURRENT_REPORT_VERSION,
    LEGACY_CONTRACT_PATHS,
    REPORT_RULE_VERSIONS,
    V8ContractViolation,
    expected_terminal_task_status,
    quality_gate_failures,
    quantity_metric,
    ratio_metric,
    score_metric,
    validate_report,
)


def _audience_quality() -> dict:
    return {
        "captured_comment_count": 120,
        "declared_comment_count": 130,
        "comment_collection_coverage_percentage": 92.31,
        "identity_coverage_percentage": 97.5,
        "candidate_user_count": 39,
        "classified_user_count": 39,
        "classification_coverage_percentage": 100.0,
        "capped_content_count": 0,
        "audience_definition_version": "audience-definition-v1",
        "classifier_version": "audience-classifier-v2",
        "user_key_version": "platform-user-hmac-v2",
        "evidence_window_start": "2026-05-04T16:00:00Z",
        "evidence_window_end": "2026-08-02T16:00:00Z",
        "report_cutoff_at": "2026-08-02T12:00:00Z",
        "warm_up": True,
    }


def _conclusion_group(label: str) -> dict:
    return {
        "label": label,
        "publication_count": 5,
        "audience_quality": _audience_quality(),
        "metrics": {
            "selling_point_count_share": ratio_metric(
                2, 5, status="available", eligible_count=5
            ),
            "core_selling_point_count_share": ratio_metric(
                1, 5, status="available", eligible_count=5
            ),
            "selling_point_exposure_share": ratio_metric(
                400, 1000, status="available", eligible_count=5
            ),
            "core_selling_point_exposure_share": ratio_metric(
                150, 1000, status="available", eligible_count=5
            ),
            "content_verticality": score_metric(
                72, status="available", scorable_items=5, total_items=5
            ),
            "automotive_user_rate": ratio_metric(
                None,
                12,
                status="below_threshold",
                eligible_count=12,
                coverage_percentage=92.31,
                reason="去重有效用户 12 人，低于 30 人门槛",
            ),
            "acquisition_potential": score_metric(
                55, status="available", scorable_items=5, total_items=5
            ),
        },
    }


def _channel_conclusions() -> dict:
    return {
        platform: {
            "platform": platform,
            "label": label,
            "publication_count": 5,
            "summary": _conclusion_group("汇总"),
            "scenes": {
                "used_car": _conclusion_group("二手车"),
                "new_car": _conclusion_group("新车"),
                "media": _conclusion_group("媒体-AI小懂"),
            },
        }
        for platform, label in (("douyin", "抖音"), ("xiaohongshu", "小红书"))
    }


def valid_report() -> dict:
    publications = 10
    return {
        "report_version": CURRENT_REPORT_VERSION,
        "rule_version": CURRENT_REPORT_RULE_VERSION,
        "taxonomy_version": "selling-points-v5.1",
        "evidence_version": CURRENT_REPORT_EVIDENCE_VERSION,
        "metadata": {
            "task_id": "task-test",
            "revision": 1,
            "generated_at": "2026-08-02T12:00:00Z",
        },
        "scope": {
            "period_start": "2026-08-01T00:00:00+08:00",
            "period_end": "2026-08-02T00:00:00+08:00",
            "timezone": "Asia/Shanghai",
        },
        "task": {"task_status": "succeeded"},
        "data_quality": {
            "discovery_coverage": 100.0,
            "detail_coverage": 100.0,
            "metrics_freshness": 90.0,
            "evaluation_coverage": 95.0,
            "core_artifact_coverage": 100.0,
            "media_terminal_coverage": 95.0,
            "duplicate_fingerprint_coverage": 95.0,
            "duplicate_calibration_ready": True,
            "weekly_comment_coverage": 90.0,
        },
        "summary_metrics": {
            "publication_count": quantity_metric(
                10, unit="content", status="available"
            ),
            "active_account_count": quantity_metric(
                3, unit="account", status="available"
            ),
            "view_count": quantity_metric(
                1000, unit="view", status="available", coverage_percentage=90
            ),
            "comment_count": quantity_metric(
                20, unit="comment", status="sample_only", coverage_percentage=50
            ),
            "verticality_rate": ratio_metric(
                7,
                publications,
                status="available",
                eligible_count=8,
                coverage_percentage=80,
            ),
            "selling_point_coverage_rate": ratio_metric(
                4,
                publications,
                status="available",
                eligible_count=8,
                coverage_percentage=80,
            ),
            "duplicate_rate": ratio_metric(
                2,
                publications,
                status="available",
                eligible_count=10,
                coverage_percentage=100,
            ),
            "estimated_new_users": quantity_metric(
                None, unit="person", status="not_calculable", reason="model unavailable"
            ),
            "estimated_reactivated_users": quantity_metric(
                None, unit="person", status="not_calculable", reason="model unavailable"
            ),
            "estimated_leads": quantity_metric(
                None, unit="lead", status="not_calculable", reason="model unavailable"
            ),
        },
        "channels": _channel_conclusions(),
        "platform_dimensions": [],
        "account_type_dimensions": [],
        "content_direction_dimensions": [],
        "selling_point_dimensions": [],
        "duplicates": [],
        "review_summary": [],
        "capture_summary": [],
        "provider_costs": [],
        "content_details": [],
        "files": [],
    }


class V8ContractTest(unittest.TestCase):
    def test_project_metadata_matches_runtime_report_version(self) -> None:
        project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
        self.assertEqual(
            project["tool"]["dcar"]["report-version"], CURRENT_REPORT_VERSION
        )
        live_contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        self.assertEqual(live_contract["report_version"], CURRENT_REPORT_VERSION)
        self.assertEqual(live_contract["rule_version"], CURRENT_REPORT_RULE_VERSION)
        self.assertEqual(
            live_contract["evidence_version"], CURRENT_REPORT_EVIDENCE_VERSION
        )

    def test_valid_operational_report_passes(self) -> None:
        validate_report(valid_report())

    def test_frozen_v8_2_report_remains_valid_but_versions_cannot_cross(self) -> None:
        historical = valid_report()
        historical["report_version"] = "dcar-content-operations-report-v8.2"
        historical["rule_version"] = "evaluation-v6"
        historical["evidence_version"] = "evidence-v1"
        validate_report(historical)

        historical["rule_version"] = "evaluation-v7"
        with self.assertRaisesRegex(V8ContractViolation, "rule_version"):
            validate_report(historical)

        for legacy_rule in ("evaluation-v6", "evaluation-v7"):
            current = valid_report()
            current["rule_version"] = legacy_rule
            with self.assertRaisesRegex(V8ContractViolation, "rule_version"):
                validate_report(current)

    def test_frozen_v8_3_report_stays_on_evaluation_v7(self) -> None:
        historical = valid_report()
        historical["report_version"] = "dcar-content-operations-report-v8.3"
        historical["rule_version"] = "evaluation-v7"
        historical["evidence_version"] = "evidence-v1"
        validate_report(historical)

        historical["rule_version"] = "evaluation-v8"
        with self.assertRaisesRegex(V8ContractViolation, "rule_version"):
            validate_report(historical)

    def test_frozen_v8_4_report_stays_on_evaluation_v8(self) -> None:
        version = "dcar-content-operations-report-v8.4"
        historical = valid_report()
        historical["report_version"] = version
        validate_report(historical)

        self.assertEqual(
            LEGACY_CONTRACT_PATHS[version].name, "report_contract_v8_4.json"
        )
        self.assertEqual(REPORT_RULE_VERSIONS[version], "evaluation-v8")
        historical["rule_version"] = "evaluation-v7"
        with self.assertRaisesRegex(V8ContractViolation, "rule_version"):
            validate_report(historical)

    def test_v7_report_is_not_accepted_as_v8(self) -> None:
        report = valid_report()
        report["report_version"] = "dcar-dual-channel-evaluation-v7.0"
        with self.assertRaisesRegex(V8ContractViolation, "report_version"):
            validate_report(report)

    def test_report_freezes_the_published_taxonomy_revision_not_only_v5_0(self) -> None:
        report = valid_report()
        report["taxonomy_version"] = "selling-points-v5.1"
        validate_report(report)
        report["taxonomy_version"] = "draft"
        with self.assertRaisesRegex(V8ContractViolation, "taxonomy_version"):
            validate_report(report)

    def test_metric_partial_is_forbidden(self) -> None:
        report = valid_report()
        report["summary_metrics"]["verticality_rate"]["status"] = "partial"
        with self.assertRaisesRegex(V8ContractViolation, "partial"):
            validate_report(report)

    def test_below_threshold_is_the_metric_coverage_status(self) -> None:
        report = valid_report()
        report["summary_metrics"]["verticality_rate"]["status"] = "below_threshold"
        report["summary_metrics"]["verticality_rate"]["coverage_percentage"] = 70
        validate_report(report)

    def test_ratio_percentage_is_only_published_for_publishing_statuses(self) -> None:
        blocked = (
            "below_threshold",
            "missing",
            "not_applicable",
            "not_calculable",
            "stale",
        )
        for status in blocked:
            with self.subTest(status=status):
                metric = ratio_metric(8, 10, status=status, eligible_count=8)
                self.assertEqual(metric["numerator"], 8)
                self.assertIsNone(metric["percentage"])
        self.assertEqual(
            ratio_metric(8, 10, status="available")["percentage"], 80.0
        )
        self.assertEqual(
            ratio_metric(8, 10, status="sample_only")["percentage"], 80.0
        )

    def test_required_coverage_controls_terminal_task_status(self) -> None:
        report = valid_report()
        report["data_quality"]["evaluation_coverage"] = 94.99
        self.assertEqual(
            expected_terminal_task_status(report["data_quality"]), "partial"
        )
        with self.assertRaisesRegex(V8ContractViolation, "task_status must be partial"):
            validate_report(report)
        report["task"]["task_status"] = "partial"
        validate_report(report)
        self.assertEqual(
            quality_gate_failures(report["data_quality"]),
            [
                {
                    "key": "evaluation_coverage",
                    "kind": "coverage",
                    "actual": 94.99,
                    "required": 95.0,
                }
            ],
        )

    def test_duplicate_calibration_is_separate_from_fingerprint_coverage(self) -> None:
        report = valid_report()
        report["data_quality"]["duplicate_fingerprint_coverage"] = 100.0
        report["data_quality"]["duplicate_calibration_ready"] = False
        self.assertEqual(
            expected_terminal_task_status(report["data_quality"]), "partial"
        )
        with self.assertRaisesRegex(V8ContractViolation, "task_status must be partial"):
            validate_report(report)
        report["task"]["task_status"] = "partial"
        validate_report(report)

        report["data_quality"]["duplicate_calibration_ready"] = "false"
        with self.assertRaisesRegex(V8ContractViolation, "must be boolean"):
            validate_report(report)

        report = valid_report()
        del report["data_quality"]["duplicate_calibration_ready"]
        with self.assertRaisesRegex(V8ContractViolation, "is required"):
            validate_report(report)

    def test_ratio_denominator_is_all_publications(self) -> None:
        report = valid_report()
        report["summary_metrics"]["selling_point_coverage_rate"]["denominator"] = 8
        with self.assertRaisesRegex(V8ContractViolation, "publication_count"):
            validate_report(report)

    def test_forecast_metrics_cannot_publish_a_value_as_not_calculable(self) -> None:
        report = valid_report()
        report["summary_metrics"]["estimated_leads"]["value"] = 30
        with self.assertRaisesRegex(V8ContractViolation, "value must be null"):
            validate_report(report)

    def test_audience_classification_coverage_is_required_and_bounded(self) -> None:
        report = valid_report()
        quality = report["channels"]["douyin"]["summary"]["audience_quality"]
        del quality["classification_coverage_percentage"]
        with self.assertRaisesRegex(
            V8ContractViolation, "classification_coverage_percentage"
        ):
            validate_report(report)

        report = valid_report()
        quality = report["channels"]["douyin"]["summary"]["audience_quality"]
        quality["classification_coverage_percentage"] = 100.01
        with self.assertRaisesRegex(V8ContractViolation, "between 0 and 100"):
            validate_report(report)

    def test_view_quantity_uses_view_not_people(self) -> None:
        report = valid_report()
        report["summary_metrics"]["view_count"]["unit"] = "person"
        with self.assertRaisesRegex(V8ContractViolation, "unit must be view"):
            validate_report(report)


if __name__ == "__main__":
    unittest.main()
