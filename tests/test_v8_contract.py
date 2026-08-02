from __future__ import annotations

import tomllib
import unittest
from pathlib import Path

from v8.contracts import (
    CURRENT_REPORT_VERSION,
    V8ContractViolation,
    expected_terminal_task_status,
    quantity_metric,
    ratio_metric,
    validate_report,
)


def valid_report() -> dict:
    publications = 10
    return {
        "report_version": CURRENT_REPORT_VERSION,
        "rule_version": "evaluation-v6",
        "taxonomy_version": "selling-points-v5.0",
        "evidence_version": "evidence-v1",
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
            "weekly_comment_coverage": 90.0,
        },
        "summary_metrics": {
            "publication_count": quantity_metric(10, unit="content", status="available"),
            "active_account_count": quantity_metric(3, unit="account", status="available"),
            "view_count": quantity_metric(1000, unit="view", status="available", coverage_percentage=90),
            "comment_count": quantity_metric(20, unit="comment", status="sample_only", coverage_percentage=50),
            "verticality_rate": ratio_metric(7, publications, status="available", eligible_count=8, coverage_percentage=80),
            "selling_point_coverage_rate": ratio_metric(4, publications, status="available", eligible_count=8, coverage_percentage=80),
            "estimated_new_users": quantity_metric(None, unit="person", status="not_calculable", reason="model unavailable"),
            "estimated_reactivated_users": quantity_metric(None, unit="person", status="not_calculable", reason="model unavailable"),
            "estimated_leads": quantity_metric(None, unit="lead", status="not_calculable", reason="model unavailable"),
        },
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
        self.assertEqual(project["tool"]["dcar"]["report-version"], CURRENT_REPORT_VERSION)

    def test_valid_operational_report_passes(self) -> None:
        validate_report(valid_report())

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

    def test_required_coverage_controls_terminal_task_status(self) -> None:
        report = valid_report()
        report["data_quality"]["evaluation_coverage"] = 94.99
        self.assertEqual(expected_terminal_task_status(report["data_quality"]), "partial")
        with self.assertRaisesRegex(V8ContractViolation, "task_status must be partial"):
            validate_report(report)
        report["task"]["task_status"] = "partial"
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

    def test_view_quantity_uses_view_not_people(self) -> None:
        report = valid_report()
        report["summary_metrics"]["view_count"]["unit"] = "person"
        with self.assertRaisesRegex(V8ContractViolation, "unit must be view"):
            validate_report(report)


if __name__ == "__main__":
    unittest.main()
