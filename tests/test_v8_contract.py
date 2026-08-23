from __future__ import annotations

import copy
import hashlib
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
    load_contract,
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
            "collection_cutoff_at": "2026-08-02T00:00:00Z",
        },
        "scope": {
            "period_start": "2026-08-01T00:00:00+08:00",
            "period_end": "2026-08-02T00:00:00+08:00",
            "timezone": "Asia/Shanghai",
        },
        "task": {
            "task_type": "custom",
            "creation_source": "manual",
            "task_status": "succeeded",
            "name": "合同测试报告",
        },
        "data_quality": {
            "discovery_coverage": 90.0,
            "detail_coverage": 100.0,
            "metrics_freshness": 90.0,
            "evaluation_coverage": 100.0,
            "core_artifact_coverage": 100.0,
            "media_terminal_coverage": 95.0,
            "duplicate_fingerprint_coverage": 100.0,
            "duplicate_calibration_ready": True,
            "weekly_comment_coverage": 90.0,
        },
        "data_quality_details": {
            "discovery_coverage": {
                "status": "available",
                "covered_identity_occurrence_count": 9,
                "eligible_identity_occurrence_count": 10,
                "observed_occurrence_count": 1,
                "expected_occurrence_count": 1,
                "percentage": 90.0,
                "eligible_basis": "scheduled_daily_capture_identity_occurrences",
                "reason": "",
            },
            "metrics_freshness": {
                "status": "available",
                "fresh_count": 9,
                "as_of_snapshot_count": 10,
                "eligible_count": 10,
                "percentage": 90.0,
                "window_hours": 36,
                "eligible_basis": "all_window_contents",
                "reason": "",
            }
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
                20, unit="comment", status="available", coverage_percentage=90
            ),
            "verticality_rate": ratio_metric(
                7,
                publications,
                status="available",
                eligible_count=10,
                coverage_percentage=100,
            ),
            "selling_point_coverage_rate": ratio_metric(
                4,
                publications,
                status="available",
                eligible_count=10,
                coverage_percentage=100,
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
        "capture_summary": [],
        "provider_costs": [],
        "content_details": [],
        "files": [],
    }


class V8ContractTest(unittest.TestCase):
    def test_project_metadata_matches_the_current_contract(self) -> None:
        project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
        deployed_version = project["tool"]["dcar"]["report-version"]
        self.assertEqual(deployed_version, CURRENT_REPORT_VERSION)
        self.assertEqual(CONTRACT_PATH.name, "report_contract_v8_7.json")
        live_contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        self.assertEqual(live_contract["report_version"], CURRENT_REPORT_VERSION)
        self.assertEqual(live_contract["rule_version"], CURRENT_REPORT_RULE_VERSION)
        self.assertEqual(
            live_contract["evidence_version"], CURRENT_REPORT_EVIDENCE_VERSION
        )
        self.assertEqual(
            hashlib.sha256(
                LEGACY_CONTRACT_PATHS[
                    "dcar-content-operations-report-v8.5"
                ].read_bytes()
            ).hexdigest(),
            "56c50464a8ed75ba0ebef2dbf13df3fa8c17baec5bc7bdc1cde2e5348673839b",
        )

    def test_contract_registry_is_exact_and_unknown_versions_fail_closed(self) -> None:
        self.assertEqual(
            set(REPORT_RULE_VERSIONS),
            {CURRENT_REPORT_VERSION, *LEGACY_CONTRACT_PATHS},
        )
        current = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        self.assertEqual(load_contract(), current)
        self.assertEqual(load_contract(report_version=""), current)
        self.assertEqual(load_contract(report_version=CURRENT_REPORT_VERSION), current)

        frozen_v85 = json.loads(
            LEGACY_CONTRACT_PATHS[
                "dcar-content-operations-report-v8.5"
            ].read_text(encoding="utf-8")
        )
        self.assertEqual(
            load_contract(report_version="dcar-content-operations-report-v8.5"),
            frozen_v85,
        )

        for version in (
            "dcar-content-operations-report-v8.0",
            "dcar-content-operations-report-v8.1",
            "dcar-content-operations-report-v99",
        ):
            with self.subTest(version=version):
                with self.assertRaisesRegex(
                    V8ContractViolation, "has no registered contract"
                ):
                    load_contract(report_version=version)

        self.assertEqual(
            load_contract(
                LEGACY_CONTRACT_PATHS["dcar-content-operations-report-v8.5"],
                report_version="dcar-content-operations-report-v8.1",
            ),
            frozen_v85,
        )

    def test_unregistered_v8_1_report_cannot_fall_back_to_current_contract(self) -> None:
        report = valid_report()
        report["report_version"] = "dcar-content-operations-report-v8.1"
        with self.assertRaisesRegex(
            V8ContractViolation, "report_version.*has no registered contract"
        ):
            validate_report(report)

    def test_valid_operational_report_passes(self) -> None:
        validate_report(valid_report())

    @staticmethod
    def _historical_report() -> dict:
        """<=v8.6 契约仍把 review_summary 列为必备键（v8.7 起才移除）。"""

        report = valid_report()
        report["review_summary"] = []
        return report

    def test_frozen_v8_2_report_remains_valid_but_versions_cannot_cross(self) -> None:
        historical = self._historical_report()
        historical["report_version"] = "dcar-content-operations-report-v8.2"
        historical["rule_version"] = "evaluation-v6"
        historical["evidence_version"] = "evidence-v1"
        historical["data_quality"]["discovery_coverage"] = 100.0
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
        historical = self._historical_report()
        historical["report_version"] = "dcar-content-operations-report-v8.3"
        historical["rule_version"] = "evaluation-v7"
        historical["evidence_version"] = "evidence-v1"
        historical["data_quality"]["discovery_coverage"] = 100.0
        validate_report(historical)

        historical["rule_version"] = "evaluation-v8"
        with self.assertRaisesRegex(V8ContractViolation, "rule_version"):
            validate_report(historical)

    def test_frozen_v8_4_report_stays_on_evaluation_v8(self) -> None:
        version = "dcar-content-operations-report-v8.4"
        historical = self._historical_report()
        historical["report_version"] = version
        historical["rule_version"] = "evaluation-v8"
        historical["data_quality"]["discovery_coverage"] = 100.0
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
        report["summary_metrics"]["view_count"]["status"] = "below_threshold"
        report["summary_metrics"]["view_count"]["coverage_percentage"] = 70
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

    def test_required_coverages_use_the_ninety_percent_boundary(self) -> None:
        required_coverages = (
            "detail_coverage",
            "evaluation_coverage",
            "media_terminal_coverage",
            "duplicate_fingerprint_coverage",
            "weekly_comment_coverage",
        )

        def align_summary(report: dict, key: str, coverage: float) -> None:
            status = "available" if coverage >= 90 else "below_threshold"
            if key == "evaluation_coverage":
                for metric_name in (
                    "verticality_rate",
                    "selling_point_coverage_rate",
                ):
                    metric = report["summary_metrics"][metric_name]
                    metric["coverage_percentage"] = coverage
                    metric["status"] = status
                    if status == "below_threshold":
                        metric["percentage"] = None
            elif key == "duplicate_fingerprint_coverage":
                metric = report["summary_metrics"]["duplicate_rate"]
                metric["coverage_percentage"] = coverage
                metric["status"] = status
                if status == "below_threshold":
                    metric["percentage"] = None

        for key in required_coverages:
            with self.subTest(key=key, coverage=90.0):
                report = valid_report()
                report["data_quality"][key] = 90.0
                align_summary(report, key, 90.0)
                self.assertEqual(
                    expected_terminal_task_status(
                        report["data_quality"],
                        data_quality_details=report["data_quality_details"],
                    ),
                    "succeeded",
                )
                validate_report(report)
                self.assertEqual(
                    quality_gate_failures(
                        report["data_quality"],
                        data_quality_details=report["data_quality_details"],
                    ),
                    [],
                )

            with self.subTest(key=key, coverage=89.99):
                report = valid_report()
                report["data_quality"][key] = 89.99
                align_summary(report, key, 89.99)
                self.assertEqual(
                    expected_terminal_task_status(
                        report["data_quality"],
                        data_quality_details=report["data_quality_details"],
                    ),
                    "partial",
                )
                with self.assertRaisesRegex(
                    V8ContractViolation, "task_status must be partial"
                ):
                    validate_report(report)
                report["task"]["task_status"] = "partial"
                validate_report(report)
                self.assertEqual(
                    quality_gate_failures(
                        report["data_quality"],
                        data_quality_details=report["data_quality_details"],
                    ),
                    [
                        {
                            "key": key,
                            "kind": "coverage",
                            "actual": 89.99,
                            "required": 90.0,
                        }
                    ],
                )

    def test_optional_pipeline_observation_gaps_force_partial(self) -> None:
        report = valid_report()
        report["data_quality_details"]["pipeline_observation"] = {
            "status": "incomplete",
            "capture_observation_start_date": "2026-08-21",
            "expected_dates": ["2026-08-20", "2026-08-21"],
            "legacy_unobserved_dates": ["2026-08-20"],
            "pipeline_gap_dates": [],
            "zero_content_dates": ["2026-08-21"],
        }
        report["task"]["task_status"] = "partial"
        validate_report(report)
        self.assertEqual(
            expected_terminal_task_status(
                report["data_quality"],
                data_quality_details=report["data_quality_details"],
            ),
            "partial",
        )
        self.assertIn(
            "pipeline_observation",
            {
                failure["key"]
                for failure in quality_gate_failures(
                    report["data_quality"],
                    data_quality_details=report["data_quality_details"],
                )
            },
        )
        validate_report(valid_report())

    def test_structured_discovery_controls_task_status_at_ninety_percent(self) -> None:
        report = valid_report()
        report["data_quality"]["discovery_coverage"] = 89.0
        report["data_quality_details"]["discovery_coverage"] = {
            "status": "below_threshold",
            "covered_identity_occurrence_count": 89,
            "eligible_identity_occurrence_count": 100,
            "observed_occurrence_count": 1,
            "expected_occurrence_count": 1,
            "percentage": 89.0,
            "eligible_basis": "scheduled_daily_capture_identity_occurrences",
            "reason": "身份采集覆盖低于 90%",
        }
        report["task"]["task_status"] = "partial"

        validate_report(report)
        self.assertEqual(
            expected_terminal_task_status(
                report["data_quality"],
                data_quality_details=report["data_quality_details"],
            ),
            "partial",
        )
        self.assertEqual(
            quality_gate_failures(
                report["data_quality"],
                data_quality_details=report["data_quality_details"],
            ),
            [
                {
                    "key": "discovery_coverage",
                    "kind": "coverage",
                    "actual": 89.0,
                    "required": 90.0,
                }
            ],
        )

    def test_empty_discovery_account_set_is_not_applicable_not_one_hundred(
        self,
    ) -> None:
        report = valid_report()
        report["data_quality"]["discovery_coverage"] = None
        report["data_quality_details"]["discovery_coverage"] = {
            "status": "not_applicable",
            "covered_identity_occurrence_count": 0,
            "eligible_identity_occurrence_count": 0,
            "observed_occurrence_count": 0,
            "expected_occurrence_count": 1,
            "percentage": None,
            "eligible_basis": "scheduled_daily_capture_identity_occurrences",
            "reason": "报告窗口没有适用的平台账号",
        }

        validate_report(report)
        self.assertEqual(
            expected_terminal_task_status(
                report["data_quality"],
                data_quality_details=report["data_quality_details"],
            ),
            "succeeded",
        )
        self.assertNotIn(
            "discovery_coverage",
            {
                failure["key"]
                for failure in quality_gate_failures(
                    report["data_quality"],
                    data_quality_details=report["data_quality_details"],
                )
            },
        )

        forged = valid_report()
        forged["data_quality"]["discovery_coverage"] = None
        forged["data_quality_details"]["discovery_coverage"].update(
            status="not_applicable",
            covered_identity_occurrence_count=0,
            eligible_identity_occurrence_count=10,
            percentage=None,
            reason="伪造空账号集合",
        )
        self.assertIn(
            "discovery_coverage",
            {
                failure["key"]
                for failure in quality_gate_failures(
                    forged["data_quality"],
                    data_quality_details=forged["data_quality_details"],
                )
            },
        )

    def test_structured_discovery_is_fail_closed(self) -> None:
        mutations = (
            (
                "missing detail",
                lambda report: report["data_quality_details"].pop(
                    "discovery_coverage"
                ),
                "discovery_coverage must be an object",
            ),
            (
                "boolean count",
                lambda report: report["data_quality_details"][
                    "discovery_coverage"
                ].update(covered_identity_occurrence_count=True),
                "covered_identity_occurrence_count must be a non-negative integer",
            ),
            (
                "covered exceeds eligible",
                lambda report: report["data_quality_details"][
                    "discovery_coverage"
                ].update(covered_identity_occurrence_count=11),
                "covered_identity_occurrence_count cannot exceed",
            ),
            (
                "observed exceeds expected",
                lambda report: report["data_quality_details"][
                    "discovery_coverage"
                ].update(observed_occurrence_count=2),
                "observed_occurrence_count cannot exceed",
            ),
            (
                "expected occurrence is empty",
                lambda report: report["data_quality_details"][
                    "discovery_coverage"
                ].update(expected_occurrence_count=0),
                "expected_occurrence_count must be positive",
            ),
            (
                "covered without an observed occurrence",
                lambda report: report["data_quality_details"][
                    "discovery_coverage"
                ].update(observed_occurrence_count=0),
                "covered_identity_occurrence_count must be zero",
            ),
            (
                "percentage drifts",
                lambda report: report["data_quality_details"][
                    "discovery_coverage"
                ].update(percentage=90.01),
                "percentage must equal covered_identity_occurrence_count",
            ),
            (
                "scalar drifts",
                lambda report: report["data_quality"].update(
                    discovery_coverage=90.01
                ),
                "discovery_coverage must equal the structured percentage",
            ),
            (
                "status contradicts threshold",
                lambda report: report["data_quality_details"][
                    "discovery_coverage"
                ].update(status="below_threshold", reason="低于门槛"),
                "status must be available",
            ),
            (
                "eligibility basis drifts",
                lambda report: report["data_quality_details"][
                    "discovery_coverage"
                ].update(eligible_basis="all_enabled_identities"),
                "eligible_basis must equal scheduled_daily_capture",
            ),
            (
                "applicable account denominator is empty",
                lambda report: (
                    report["data_quality"].update(discovery_coverage=0.0),
                    report["data_quality_details"]["discovery_coverage"].update(
                        status="below_threshold",
                        covered_identity_occurrence_count=0,
                        eligible_identity_occurrence_count=0,
                        percentage=0.0,
                        reason="分母异常",
                    ),
                ),
                "eligible_identity_occurrence_count must be positive",
            ),
            (
                "non-empty account set claims not applicable",
                lambda report: (
                    report["data_quality"].update(discovery_coverage=None),
                    report["data_quality_details"]["discovery_coverage"].update(
                        status="not_applicable",
                        covered_identity_occurrence_count=0,
                        eligible_identity_occurrence_count=10,
                        percentage=None,
                        reason="无适用账号",
                    ),
                ),
                "requires an empty eligible account set",
            ),
        )
        for name, mutate, expected in mutations:
            with self.subTest(name=name):
                report = copy.deepcopy(valid_report())
                mutate(report)
                with self.assertRaisesRegex(V8ContractViolation, expected):
                    validate_report(report)

    def test_structured_freshness_controls_task_status_with_exact_counts(self) -> None:
        report = valid_report()
        report["data_quality"]["metrics_freshness"] = 57.18
        report["data_quality_details"]["metrics_freshness"] = {
            "status": "below_threshold",
            "fresh_count": 219,
            "as_of_snapshot_count": 383,
            "eligible_count": 383,
            "percentage": 57.18,
            "window_hours": 36,
            "eligible_basis": "all_window_contents",
            "reason": "固定截止点前新鲜指标不足 90%",
        }
        report["summary_metrics"]["publication_count"]["value"] = 383
        for name in (
            "verticality_rate",
            "selling_point_coverage_rate",
            "duplicate_rate",
        ):
            report["summary_metrics"][name]["denominator"] = 383
        report["task"]["task_status"] = "partial"

        validate_report(report)
        self.assertEqual(
            expected_terminal_task_status(
                report["data_quality"],
                data_quality_details=report["data_quality_details"],
            ),
            "partial",
        )
        self.assertEqual(
            quality_gate_failures(
                report["data_quality"],
                data_quality_details=report["data_quality_details"],
            ),
            [
                {
                    "key": "metrics_freshness",
                    "kind": "coverage",
                    "actual": 57.18,
                    "required": 90.0,
                }
            ],
        )

    def test_task_type_and_creation_source_combinations_are_fail_closed(self) -> None:
        for task_type, creation_source, message in (
            ("custom", "automatic", "automatic creation_source requires"),
            ("daily", "manual", "manual creation_source requires"),
            ("custom", "unknown", "creation_source is invalid"),
        ):
            with self.subTest(task_type=task_type, creation_source=creation_source):
                report = valid_report()
                report["task"].update(
                    task_type=task_type, creation_source=creation_source
                )
                with self.assertRaisesRegex(V8ContractViolation, message):
                    validate_report(report)

    def test_empty_window_freshness_is_not_applicable_not_one_hundred(self) -> None:
        report = valid_report()
        report["summary_metrics"]["publication_count"]["value"] = 0
        for name in (
            "verticality_rate",
            "selling_point_coverage_rate",
            "duplicate_rate",
        ):
            report["summary_metrics"][name] = ratio_metric(
                None, 0, status="not_applicable", eligible_count=0
            )
        report["data_quality"]["metrics_freshness"] = None
        report["data_quality_details"]["metrics_freshness"] = {
            "status": "not_applicable",
            "fresh_count": 0,
            "as_of_snapshot_count": 0,
            "eligible_count": 0,
            "percentage": None,
            "window_hours": 36,
            "eligible_basis": "all_window_contents",
            "reason": "报告窗口没有发布内容",
        }

        validate_report(report)
        self.assertEqual(
            expected_terminal_task_status(
                report["data_quality"],
                data_quality_details=report["data_quality_details"],
                enforce_boolean_quality_gates=False,
            ),
            "succeeded",
        )
        self.assertNotIn(
            "metrics_freshness",
            {
                failure["key"]
                for failure in quality_gate_failures(
                    report["data_quality"],
                    data_quality_details=report["data_quality_details"],
                    enforce_boolean_quality_gates=False,
                )
            },
        )

    def test_structured_freshness_is_fail_closed(self) -> None:
        mutations = (
            (
                "missing detail",
                lambda report: report.pop("data_quality_details"),
                "data_quality_details",
            ),
            (
                "fresh exceeds snapshots",
                lambda report: report["data_quality_details"][
                    "metrics_freshness"
                ].update(fresh_count=11),
                "fresh_count cannot exceed",
            ),
            (
                "snapshots exceed eligible",
                lambda report: report["data_quality_details"][
                    "metrics_freshness"
                ].update(as_of_snapshot_count=11),
                "as_of_snapshot_count cannot exceed",
            ),
            (
                "eligible exceeds publications",
                lambda report: report["data_quality_details"][
                    "metrics_freshness"
                ].update(eligible_count=11),
                "eligible_count cannot exceed publication_count",
            ),
            (
                "eligibility excludes window content",
                lambda report: (
                    report["data_quality"].update(metrics_freshness=100.0),
                    report["data_quality_details"]["metrics_freshness"].update(
                        fresh_count=9,
                        as_of_snapshot_count=9,
                        eligible_count=9,
                        percentage=100.0,
                    ),
                ),
                "eligible_count must equal publication_count",
            ),
            (
                "percentage drifts",
                lambda report: report["data_quality_details"][
                    "metrics_freshness"
                ].update(percentage=90.01),
                "percentage must equal",
            ),
            (
                "scalar drifts",
                lambda report: report["data_quality"].update(
                    metrics_freshness=90.01
                ),
                "must equal the structured percentage",
            ),
            (
                "status contradicts threshold",
                lambda report: report["data_quality_details"][
                    "metrics_freshness"
                ].update(status="below_threshold", reason="低于门槛"),
                "status must be available",
            ),
            (
                "freshness window drifts",
                lambda report: report["data_quality_details"][
                    "metrics_freshness"
                ].update(window_hours=24),
                "window_hours must equal 36",
            ),
            (
                "eligibility basis drifts",
                lambda report: report["data_quality_details"][
                    "metrics_freshness"
                ].update(eligible_basis="recent_contents"),
                "eligible_basis must equal all_window_contents",
            ),
            (
                "applicable denominator is zero",
                lambda report: report["data_quality_details"][
                    "metrics_freshness"
                ].update(
                    fresh_count=0,
                    as_of_snapshot_count=0,
                    eligible_count=0,
                    percentage=0.0,
                    status="below_threshold",
                    reason="分母异常",
                ),
                "eligible_count must be positive",
            ),
            (
                "non-empty window claims not applicable",
                lambda report: (
                    report["data_quality"].update(metrics_freshness=None),
                    report["data_quality_details"]["metrics_freshness"].update(
                        status="not_applicable",
                        fresh_count=0,
                        as_of_snapshot_count=0,
                        eligible_count=0,
                        percentage=None,
                        reason="无适用内容",
                    ),
                ),
                "requires an empty publication window",
            ),
        )
        for name, mutate, expected in mutations:
            with self.subTest(name=name):
                report = copy.deepcopy(valid_report())
                mutate(report)
                with self.assertRaisesRegex(V8ContractViolation, expected):
                    validate_report(report)

    def test_collection_cutoff_is_required_and_must_be_utc(self) -> None:
        report = valid_report()
        del report["metadata"]["collection_cutoff_at"]
        with self.assertRaisesRegex(V8ContractViolation, "collection_cutoff_at"):
            validate_report(report)

        report = valid_report()
        report["metadata"]["collection_cutoff_at"] = "2026-08-02T08:00:00+08:00"
        with self.assertRaisesRegex(V8ContractViolation, "RFC3339 UTC"):
            validate_report(report)

        report = valid_report()
        report["metadata"]["generated_at"] = "2026-08-02T20:00:00+08:00"
        with self.assertRaisesRegex(V8ContractViolation, "RFC3339 UTC"):
            validate_report(report)

    def test_collection_cutoff_order_and_automatic_schedule_are_frozen(self) -> None:
        report = valid_report()
        report["metadata"]["generated_at"] = "2026-08-01T23:59:59Z"
        with self.assertRaisesRegex(V8ContractViolation, "generated_at must be"):
            validate_report(report)

        report = valid_report()
        report["metadata"]["collection_cutoff_at"] = "2026-08-01T15:59:59Z"
        with self.assertRaisesRegex(V8ContractViolation, "scope.period_end"):
            validate_report(report)

        daily = valid_report()
        daily["task"].update(task_type="daily", creation_source="automatic")
        validate_report(daily)
        daily["metadata"]["collection_cutoff_at"] = "2026-08-02T00:00:01Z"
        with self.assertRaisesRegex(V8ContractViolation, "daily collection cutoff"):
            validate_report(daily)

        weekly = valid_report()
        weekly["task"].update(task_type="weekly", creation_source="automatic")
        weekly["metadata"]["collection_cutoff_at"] = "2026-08-02T00:30:00Z"
        validate_report(weekly)
        weekly["metadata"]["collection_cutoff_at"] = "2026-08-02T00:00:00Z"
        with self.assertRaisesRegex(V8ContractViolation, "weekly collection cutoff"):
            validate_report(weekly)

    def test_missing_structured_freshness_fails_the_quality_gate(self) -> None:
        report = valid_report()
        self.assertEqual(
            quality_gate_failures(
                report["data_quality"], data_quality_details=None
            )[-1],
            {
                "key": "metrics_freshness",
                "kind": "coverage",
                "actual": None,
                "required": 90.0,
            },
        )

    def test_duplicate_calibration_is_separate_from_fingerprint_coverage(self) -> None:
        report = valid_report()
        report["data_quality"]["duplicate_fingerprint_coverage"] = 90.0
        report["data_quality"]["duplicate_calibration_ready"] = False
        report["summary_metrics"]["duplicate_rate"] = ratio_metric(
            2,
            10,
            status="not_calculable",
            eligible_count=9,
            coverage_percentage=90.0,
            reason="重复内容感知指纹尚未完成定标，重复率暂不可计算",
        )
        self.assertEqual(
            expected_terminal_task_status(
                report["data_quality"],
                data_quality_details=report["data_quality_details"],
            ),
            "succeeded",
        )
        validate_report(report)
        self.assertEqual(
            quality_gate_failures(
                report["data_quality"],
                data_quality_details=report["data_quality_details"],
            ),
            [],
        )

        forged = valid_report()
        forged["data_quality"]["duplicate_calibration_ready"] = False
        with self.assertRaisesRegex(
            V8ContractViolation, "status must be not_calculable"
        ):
            validate_report(forged)

        report["data_quality"]["duplicate_calibration_ready"] = "false"
        with self.assertRaisesRegex(V8ContractViolation, "must be boolean"):
            validate_report(report)

        report = valid_report()
        del report["data_quality"]["duplicate_calibration_ready"]
        with self.assertRaisesRegex(V8ContractViolation, "is required"):
            validate_report(report)

    def test_report_artifact_rejects_non_terminal_task_statuses(self) -> None:
        for status in (
            "queued",
            "running",
            "failed",
            "cancel_requested",
            "cancelled",
            "interrupted",
        ):
            with self.subTest(status=status):
                report = valid_report()
                report["task"]["task_status"] = status
                with self.assertRaisesRegex(
                    V8ContractViolation, "task_status is invalid"
                ):
                    validate_report(report)

    def test_summary_badges_must_match_declared_quality_coverage(self) -> None:
        report = valid_report()
        report["summary_metrics"]["view_count"]["coverage_percentage"] = 89.99
        with self.assertRaisesRegex(V8ContractViolation, "view_count.status"):
            validate_report(report)

        report = valid_report()
        report["summary_metrics"]["comment_count"]["status"] = "sample_only"
        with self.assertRaisesRegex(V8ContractViolation, "cannot bypass"):
            validate_report(report)

        report = valid_report()
        report["summary_metrics"]["verticality_rate"][
            "coverage_percentage"
        ] = 90.0
        with self.assertRaisesRegex(V8ContractViolation, "evaluation_coverage"):
            validate_report(report)

        report = valid_report()
        report["summary_metrics"]["duplicate_rate"][
            "coverage_percentage"
        ] = 90.0
        with self.assertRaisesRegex(V8ContractViolation, "fingerprint_coverage"):
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
