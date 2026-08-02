from __future__ import annotations

import copy
import json
import unittest
from pathlib import Path

from workflow.contracts import (
    CONTRACT_PATH,
    ContractViolation,
    ratio_metric,
    score_metric,
    validate_report,
)


ROOT = Path(__file__).resolve().parents[1]


def ratio(value: int, total: int) -> dict:
    return ratio_metric(
        value,
        total,
        status="available",
        qualitative="测试",
        scope="测试范围",
    )


def score(value: int | None, scorable: int, total: int) -> dict:
    status = "unavailable" if scorable == 0 else "available" if scorable == total else "sample_only"
    return score_metric(
        value,
        scorable,
        total,
        status=status,
        qualitative="测试",
        scope="测试范围",
    )


def group(total: int) -> dict:
    return {
        "identifiable": ratio(total, total),
        "selling_point_covered": ratio(6, total),
        "core_selling_point": ratio(4, total),
        "other_selling_point": ratio(2, total),
    }


def verticality_coverage(total: int) -> dict:
    return {
        "content_automotive_scorable_items": total,
        "audience_automotive_scorable_items": total,
        "acquisition_potential_scorable_items": total,
        "total_items": total,
        "audience_gate": 20,
    }


def channel(total: int) -> dict:
    count = group(total)
    count["diagnostics"] = {
        "no_selling_point_count": total - 6,
        "unidentifiable_count": 0,
        "v2_v3_count": total,
        "v0_v1_count": 0,
        "evidence_coverage_percentage": 100.0,
        "failed_count": 0,
        "pending_review_count": 0,
    }
    exposure = group(total)
    exposure["coverage"] = {
        "total_valid_exposure": total,
        "valid_exposure_items": total,
        "missing_exposure_items": 0,
        "zero_or_invalid_exposure_items": 0,
        "label_exposure_cross_covered_items": total,
        "cross_coverage_percentage": 100.0,
        "required_percentage": 90,
        "calculable": True,
        "unavailable_reason": "",
    }
    scenes = {}
    for name in ("二手车", "新车", "媒体-AI小懂"):
        scenes[name] = {
            "publication_n": total,
            "count_distribution": copy.deepcopy(count),
            "exposure_distribution": copy.deepcopy(exposure),
            "verticality": {
                "content_automotive": score(80, total, total),
                "audience_automotive": score(40, total, total),
                "acquisition_potential": score(35, total, total),
                "coverage": verticality_coverage(total),
            },
            "scene_internal": {
                "core_share_within_scene_publications": ratio(4, total),
                "selling_point_coverage_within_scene": ratio(6, total),
            },
        }
    return {
        "scope": "测试渠道",
        "denominator": total,
        "count_distribution": count,
        "exposure_distribution": exposure,
        "verticality": {
            "content_automotive": score(80, total, total),
            "audience_automotive": score(40, total, total),
            "acquisition_potential": score(35, total, total),
            "coverage": verticality_coverage(total),
        },
        "channel_targets": {
            "core_selling_point_publication_share": {
                "minimum_percentage": 60,
                "maximum_percentage": 70,
            }
        },
        "scenes": scenes,
        "content_details": [],
    }


def report() -> dict:
    return {
        "report_version": "channel-structured-conclusions-v7.0",
        "rule_version": "dcar-evaluation-v5.0",
        "metadata": {},
        "run_summary": {},
        "channels": {"douyin": channel(10), "xiaohongshu": channel(10)},
        "conclusion_summary": [],
        "assets": [],
    }


class V7ContractTest(unittest.TestCase):
    def test_all_five_historical_v7_revisions_remain_valid(self):
        paths = sorted((ROOT / "reports" / "runs").glob("*/revision_*/report.json"))
        self.assertEqual(len(paths), 5)
        for path in paths:
            with self.subTest(path=path):
                validate_report(json.loads(path.read_text(encoding="utf-8")))

    def test_machine_contract_has_fixed_four_plus_four_plus_three_metrics(self):
        contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        self.assertEqual(len(contract["count_metrics"]), 4)
        self.assertEqual(len(contract["exposure_metrics"]), 4)
        self.assertEqual(len(contract["verticality_metrics"]), 3)
        self.assertEqual(contract["audience_gate"]["minimum_valid_unique_commenters"], 20)
        self.assertFalse(contract["acquisition_potential"]["reweight_missing_components"])

    def test_valid_minimal_report_passes(self):
        validate_report(report())

    def test_core_plus_other_must_equal_selling_point_coverage(self):
        value = report()
        value["channels"]["douyin"]["count_distribution"]["other_selling_point"]["numerator"] = 1
        with self.assertRaisesRegex(ContractViolation, "core \\+ other"):
            validate_report(value)

    def test_channel_target_is_forbidden_inside_scene(self):
        value = report()
        value["channels"]["douyin"]["scenes"]["二手车"]["channel_targets"] = {}
        with self.assertRaisesRegex(ContractViolation, "must not contain"):
            validate_report(value)

    def test_scene_internal_core_share_uses_scene_publications(self):
        value = report()
        value["channels"]["douyin"]["scenes"]["二手车"]["scene_internal"]["core_share_within_scene_publications"]["denominator"] = 438
        with self.assertRaisesRegex(ContractViolation, "scene publications"):
            validate_report(value)

    def test_scene_must_repeat_all_eleven_metrics(self):
        value = report()
        del value["channels"]["douyin"]["scenes"]["二手车"]["verticality"]["audience_automotive"]
        with self.assertRaisesRegex(ContractViolation, "audience_automotive is missing"):
            validate_report(value)

    def test_scene_verticality_coverage_uses_scene_publications(self):
        value = report()
        value["channels"]["douyin"]["scenes"]["二手车"]["verticality"]["coverage"]["total_items"] = 438
        with self.assertRaisesRegex(ContractViolation, "scope denominator"):
            validate_report(value)

    def test_actual_acquisition_fields_are_forbidden(self):
        value = report()
        value["channels"]["douyin"]["actual_acquisition_status"] = "not_tested"
        with self.assertRaisesRegex(ContractViolation, "forbidden actual-acquisition"):
            validate_report(value)

    def test_missing_audience_forces_missing_acquisition_potential(self):
        value = report()
        value["channels"]["douyin"]["content_details"] = [
            {
                "audience_automotive": {"score": None},
                "acquisition_potential": {"score": 50},
            }
        ]
        with self.assertRaisesRegex(ContractViolation, "acquisition must be null"):
            validate_report(value)

    def test_v5_rules_explicitly_remove_actual_acquisition_output(self):
        rules = (ROOT / "config" / "懂车帝内容评估判断标准与流程_v5_终版.md").read_text(encoding="utf-8")
        self.assertIn("废除实际拉新字段", rules)
        self.assertIn("不重加权", rules)
        self.assertIn("HMAC-SHA256", rules)


if __name__ == "__main__":
    unittest.main()
