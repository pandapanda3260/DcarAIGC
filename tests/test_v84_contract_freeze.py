from __future__ import annotations

import copy
import json
import unittest

from v8.contracts import (
    V8ContractViolation,
    quantity_metric,
    ratio_metric,
    score_metric,
    validate_report,
)
from v8.storage import PROJECT_ROOT


V8_3_CONTRACT_PATH = PROJECT_ROOT / "config" / "report_contract_v8_3.json"
V8_4_CONTRACT_PATH = PROJECT_ROOT / "config" / "report_contract_v8_4.json"


def _audience_quality() -> dict:
    return {
        "captured_comment_count": 320,
        "declared_comment_count": 348,
        "comment_collection_coverage_percentage": 91.95,
        "identity_coverage_percentage": 97.19,
        "capped_content_count": 0,
        "audience_definition_version": "audience-definition-v1",
        "classifier_version": "audience-classifier-v1",
        "user_key_version": "platform-user-hmac-v2",
        "evidence_window_start": "2026-05-05T00:00:00+08:00",
        "evidence_window_end": "2026-08-03T00:00:00+08:00",
        "report_cutoff_at": "2026-08-03T02:00:00+08:00",
        "warm_up": True,
    }


def _group(label: str) -> dict:
    return {
        "label": label,
        "publication_count": 10,
        "audience_quality": _audience_quality(),
        "metrics": {
            "selling_point_count_share": ratio_metric(
                4, 10, status="available", eligible_count=9
            ),
            "core_selling_point_count_share": ratio_metric(
                2, 10, status="available", eligible_count=9
            ),
            "selling_point_exposure_share": ratio_metric(
                4000, 10000, status="available", eligible_count=9
            ),
            "core_selling_point_exposure_share": ratio_metric(
                1500, 10000, status="available", eligible_count=9
            ),
            "content_verticality": score_metric(
                72, status="available", scorable_items=10, total_items=10
            ),
            "automotive_user_rate": ratio_metric(
                120,
                300,
                status="available",
                eligible_count=300,
                coverage_percentage=91.95,
            ),
            "acquisition_potential": score_metric(
                55, status="available", scorable_items=10, total_items=10
            ),
        },
    }


def _channels() -> dict:
    return {
        platform: {
            "platform": platform,
            "label": label,
            "publication_count": 10,
            "summary": _group("汇总"),
            "scenes": {
                "used_car": _group("二手车"),
                "new_car": _group("新车"),
                "media": _group("媒体-AI小懂"),
            },
        }
        for platform, label in (("douyin", "抖音"), ("xiaohongshu", "小红书"))
    }


def _valid_v8_4_report() -> dict:
    return {
        "report_version": "dcar-content-operations-report-v8.4",
        "rule_version": "evaluation-v8",
        "taxonomy_version": "selling-points-v5.0",
        "evidence_version": "evidence-v2",
        "metadata": {
            "task_id": "D8-D-20260810-20260810",
            "revision": 1,
            "generated_at": "2026-08-11T02:00:00+08:00",
        },
        "scope": {
            "period_start": "2026-08-10T00:00:00+08:00",
            "period_end": "2026-08-11T00:00:00+08:00",
            "timezone": "Asia/Shanghai",
        },
        "task": {
            "task_type": "daily",
            "creation_source": "automatic",
            "task_status": "succeeded",
            "name": "2026-08-10 日报",
        },
        "data_quality": {
            "discovery_coverage": 100.0,
            "detail_coverage": 100.0,
            "metrics_freshness": 100.0,
            "evaluation_coverage": 100.0,
            "core_artifact_coverage": 100.0,
            "media_terminal_coverage": 100.0,
            "duplicate_fingerprint_coverage": 100.0,
            "weekly_comment_coverage": 100.0,
        },
        "summary_metrics": {
            "publication_count": quantity_metric(
                20, unit="content", status="available"
            ),
            "active_account_count": quantity_metric(
                5, unit="account", status="available"
            ),
            "view_count": quantity_metric(
                120000, unit="view", status="available", coverage_percentage=100.0
            ),
            "comment_count": quantity_metric(
                640, unit="comment", status="available", coverage_percentage=100.0
            ),
            "verticality_rate": ratio_metric(
                12, 20, status="available", eligible_count=19,
                coverage_percentage=100.0,
            ),
            "selling_point_coverage_rate": ratio_metric(
                8, 20, status="available", eligible_count=19,
                coverage_percentage=100.0,
            ),
            "duplicate_rate": ratio_metric(
                1, 20, status="available", eligible_count=20,
                coverage_percentage=100.0,
            ),
            "estimated_new_users": quantity_metric(
                None, unit="person", status="not_calculable",
                reason="v8 首版没有经过验证的业务预估模型",
            ),
            "estimated_reactivated_users": quantity_metric(
                None, unit="person", status="not_calculable",
                reason="v8 首版没有经过验证的业务预估模型",
            ),
            "estimated_leads": quantity_metric(
                None, unit="lead", status="not_calculable",
                reason="v8 首版没有经过验证的业务预估模型",
            ),
        },
        "channels": _channels(),
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


class V84ContractFreezeTest(unittest.TestCase):
    def test_frozen_fixture_versions_are_pinned(self) -> None:
        v83 = json.loads(V8_3_CONTRACT_PATH.read_text(encoding="utf-8"))
        self.assertEqual(
            v83["report_version"], "dcar-content-operations-report-v8.3"
        )
        self.assertEqual(v83["rule_version"], "evaluation-v7")
        self.assertEqual(v83["evidence_version"], "evidence-v1")
        self.assertNotIn("channel_conclusion_metrics", v83)

        v84 = json.loads(V8_4_CONTRACT_PATH.read_text(encoding="utf-8"))
        self.assertEqual(
            v84["report_version"], "dcar-content-operations-report-v8.4"
        )
        self.assertEqual(v84["rule_version"], "evaluation-v8")
        self.assertEqual(v84["evidence_version"], "evidence-v2")
        self.assertIn("channels", v84["required_top_level_keys"])
        self.assertEqual(
            list(v84["channel_conclusion_metrics"].keys()),
            [
                "selling_point_count_share",
                "core_selling_point_count_share",
                "selling_point_exposure_share",
                "core_selling_point_exposure_share",
                "content_verticality",
                "automotive_user_rate",
                "acquisition_potential",
            ],
        )
        self.assertNotIn("audience_verticality", v84["channel_conclusion_metrics"])

    def test_v8_4_accepts_a_complete_report(self) -> None:
        validate_report(_valid_v8_4_report(), contract_path=V8_4_CONTRACT_PATH)

    def _assert_rejected(self, report: dict, fragment: str) -> None:
        with self.assertRaises(V8ContractViolation) as context:
            validate_report(report, contract_path=V8_4_CONTRACT_PATH)
        self.assertIn(fragment, str(context.exception))

    def test_v8_4_rejects_broken_automotive_user_rate(self) -> None:
        base = _valid_v8_4_report()

        report = copy.deepcopy(base)
        metric = report["channels"]["douyin"]["summary"]["metrics"][
            "automotive_user_rate"
        ]
        metric["eligible_count"] = None
        self._assert_rejected(report, "eligible_count must equal denominator")

        report = copy.deepcopy(base)
        metric = report["channels"]["douyin"]["summary"]["metrics"][
            "automotive_user_rate"
        ]
        metric["eligible_count"] = 299
        self._assert_rejected(report, "eligible_count must equal denominator")

        report = copy.deepcopy(base)
        metric = report["channels"]["douyin"]["summary"]["metrics"][
            "automotive_user_rate"
        ]
        metric["coverage_percentage"] = 101
        self._assert_rejected(
            report, "coverage_percentage must be null or between 0 and 100"
        )

        report = copy.deepcopy(base)
        metric = report["channels"]["xiaohongshu"]["scenes"]["media"]["metrics"][
            "automotive_user_rate"
        ]
        metric["status"] = "below_threshold"
        self._assert_rejected(
            report, "percentage must be null for status below_threshold"
        )

        report = copy.deepcopy(base)
        report["channels"]["douyin"]["summary"]["metrics"][
            "automotive_user_rate"
        ] = ratio_metric(None, 300, status="available", eligible_count=300)
        self._assert_rejected(
            report, "must publish numerator and percentage for status available"
        )

    def test_v8_4_rejects_legacy_audience_verticality_metric(self) -> None:
        report = _valid_v8_4_report()
        metrics = report["channels"]["douyin"]["summary"]["metrics"]
        metrics["audience_verticality"] = metrics.pop("automotive_user_rate")
        self._assert_rejected(
            report, "must contain exactly the channel conclusion metrics"
        )

    def test_v8_4_rejects_broken_audience_quality(self) -> None:
        base = _valid_v8_4_report()

        report = copy.deepcopy(base)
        del report["channels"]["douyin"]["summary"]["audience_quality"]["warm_up"]
        self._assert_rejected(report, "audience_quality missing")

        report = copy.deepcopy(base)
        report["channels"]["douyin"]["summary"]["audience_quality"][
            "identity_coverage_percentage"
        ] = 120
        self._assert_rejected(
            report,
            "identity_coverage_percentage must be null or between 0 and 100",
        )

        report = copy.deepcopy(base)
        report["channels"]["douyin"]["summary"]["audience_quality"]["warm_up"] = "yes"
        self._assert_rejected(report, "warm_up must be a boolean")

    def test_v8_4_rejects_missing_or_incomplete_channels(self) -> None:
        report = _valid_v8_4_report()
        del report["channels"]
        self._assert_rejected(report, "$.channels must be an object")

        report = _valid_v8_4_report()
        del report["channels"]["xiaohongshu"]
        self._assert_rejected(report, "must contain exactly the platforms")

        report = _valid_v8_4_report()
        del report["channels"]["douyin"]["scenes"]["media"]
        self._assert_rejected(report, "must contain exactly the scenes")

    def test_released_v8_3_revisions_validate_against_frozen_contract(self) -> None:
        runs = PROJECT_ROOT / "reports" / "runs" / "v8"
        if not runs.is_dir():
            self.skipTest("released report files are not present in this checkout")
        released = []
        for path in sorted(runs.glob("*/revision_*/report.json")):
            report = json.loads(path.read_text(encoding="utf-8"))
            if report.get("report_version") == "dcar-content-operations-report-v8.3":
                released.append((path, report))
        if not released:
            self.skipTest("no released v8.3 revisions in this checkout")
        for path, report in released:
            with self.subTest(path=str(path)):
                validate_report(report, contract_path=V8_3_CONTRACT_PATH)


if __name__ == "__main__":
    unittest.main()
