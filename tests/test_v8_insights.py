from __future__ import annotations

import unittest

from v8.insights import build_channel_conclusions


class V8InsightsTest(unittest.TestCase):
    def test_comment_scores_use_valid_subset_without_blocking_channel_or_scene(self) -> None:
        rows = [
            {
                "platform": "douyin", "content_direction": "new_car",
                "evidence_level": "V3", "selling_point_included": True,
                "primary_tier": "core", "content_automotive_score": 90,
                "audience_automotive_score": 60,
                "acquisition_potential_score": 70, "view_count": 100,
            },
            {
                "platform": "douyin", "content_direction": "new_car",
                "evidence_level": "V3", "selling_point_included": False,
                "primary_tier": None, "content_automotive_score": 80,
                "audience_automotive_score": None,
                "acquisition_potential_score": None, "view_count": 200,
            },
            {
                "platform": "douyin", "content_direction": "used_car",
                "evidence_level": "V3", "selling_point_included": False,
                "primary_tier": None, "content_automotive_score": 70,
                "audience_automotive_score": None,
                "acquisition_potential_score": None, "view_count": 300,
            },
        ]

        channel = build_channel_conclusions(rows)["douyin"]
        summary = channel["summary"]["metrics"]
        new_car = channel["scenes"]["new_car"]["metrics"]
        for metrics in (summary, new_car):
            # audience_verticality is replaced by automotive_user_rate (ratio);
            # acquisition_potential keeps its score-averaging behavior.
            self.assertEqual(metrics["acquisition_potential"]["value"], 70)
            self.assertEqual(metrics["acquisition_potential"]["status"], "sample_only")
            self.assertEqual(metrics["automotive_user_rate"]["kind"], "ratio")
            self.assertNotIn("audience_verticality", metrics)
        # Without a wired user aggregation the rate never fabricates a value.
        self.assertEqual(
            summary["automotive_user_rate"]["status"], "not_calculable"
        )
        self.assertIsNone(summary["automotive_user_rate"]["percentage"])
        self.assertIn(
            "用户聚合", summary["automotive_user_rate"]["reason"]
        )

    def test_supplied_audience_rate_replaces_the_placeholder(self) -> None:
        rows = [
            {
                "platform": "douyin", "content_direction": "new_car",
                "content_id": 11,
                "evidence_level": "V3", "selling_point_included": True,
                "primary_tier": "core", "content_automotive_score": 90,
                "audience_automotive_score": 60,
                "acquisition_potential_score": 70, "view_count": 100,
            },
        ]
        audience_rates = {
            "douyin": {
                "summary": {
                    "metric": {
                        "kind": "ratio", "numerator": 40, "denominator": 100,
                        "percentage": 40.0, "unit": "percent", "status": "available",
                        "eligible_count": 100, "coverage_percentage": 92.0,
                        "reason": "",
                    },
                    "audience_quality": {"identity_coverage_percentage": 97.0},
                },
                "scenes": {},
            }
        }
        channel = build_channel_conclusions(rows, audience_rates=audience_rates)[
            "douyin"
        ]
        rate = channel["summary"]["metrics"]["automotive_user_rate"]
        self.assertEqual(rate["percentage"], 40.0)
        self.assertEqual(rate["status"], "available")
        self.assertEqual(
            channel["summary"]["audience_quality"]["identity_coverage_percentage"],
            97.0,
        )

    def test_exposure_shares_stay_unpublished_below_cross_coverage_gate(self) -> None:
        rows = [
            {
                "platform": "xiaohongshu",
                "content_direction": "media",
                "evidence_level": "V3",
                "selling_point_included": True,
                "primary_tier": "core",
                "content_automotive_score": 80,
                "audience_automotive_score": None,
                "acquisition_potential_score": None,
                "view_count": 100,
            },
            {
                "platform": "xiaohongshu",
                "content_direction": "other",
                "evidence_level": None,
                "selling_point_included": False,
                "primary_tier": None,
                "content_automotive_score": None,
                "audience_automotive_score": None,
                "acquisition_potential_score": None,
                "view_count": 100,
            },
        ]

        channel = build_channel_conclusions(rows)["xiaohongshu"]
        summary = channel["summary"]["metrics"]
        media = channel["scenes"]["media"]["metrics"]

        self.assertEqual(channel["exposure_coverage_percentage"], 50.0)
        self.assertEqual(summary["selling_point_count_share"]["percentage"], 50.0)
        for key in (
            "selling_point_exposure_share",
            "core_selling_point_exposure_share",
        ):
            self.assertEqual(summary[key]["status"], "below_threshold")
            self.assertIsNone(summary[key]["numerator"])
            self.assertIsNone(summary[key]["percentage"])
            self.assertEqual(media[key]["status"], "below_threshold")
            self.assertEqual(media[key]["denominator"], 200)
            self.assertEqual(media[key]["eligible_count"], 1)

    def test_exposure_share_uses_only_positive_exposure_as_its_data_range(self) -> None:
        rows = [
            {
                "platform": "douyin", "content_direction": "new_car",
                "evidence_level": "V3", "selling_point_included": True,
                "primary_tier": "core", "content_automotive_score": 90,
                "audience_automotive_score": None,
                "acquisition_potential_score": None, "view_count": 100,
            },
            {
                "platform": "douyin", "content_direction": "new_car",
                "evidence_level": "V3", "selling_point_included": False,
                "primary_tier": None, "content_automotive_score": 80,
                "audience_automotive_score": None,
                "acquisition_potential_score": None, "view_count": 300,
            },
            {
                "platform": "douyin", "content_direction": "other",
                "evidence_level": None, "selling_point_included": False,
                "primary_tier": None, "content_automotive_score": None,
                "audience_automotive_score": None,
                "acquisition_potential_score": None, "view_count": None,
            },
            {
                "platform": "douyin", "content_direction": "other",
                "evidence_level": None, "selling_point_included": False,
                "primary_tier": None, "content_automotive_score": None,
                "audience_automotive_score": None,
                "acquisition_potential_score": None, "view_count": 0,
            },
        ]

        channel = build_channel_conclusions(rows)["douyin"]
        selling = channel["summary"]["metrics"]["selling_point_exposure_share"]
        core = channel["summary"]["metrics"]["core_selling_point_exposure_share"]

        self.assertEqual(channel["publication_count"], 4)
        self.assertEqual(channel["valid_exposure_items"], 2)
        self.assertEqual(channel["exposure_coverage_percentage"], 100.0)
        self.assertEqual(selling["status"], "available")
        self.assertEqual(selling["numerator"], 100)
        self.assertEqual(selling["denominator"], 400)
        self.assertEqual(selling["percentage"], 25.0)
        self.assertEqual(selling["eligible_count"], 2)
        self.assertEqual(core, selling)

    def test_unclassified_high_exposure_keeps_share_below_threshold(self) -> None:
        rows = [
            {
                "platform": "douyin", "content_direction": "new_car",
                "evidence_level": "V3", "selling_point_included": True,
                "primary_tier": "core", "content_automotive_score": 90,
                "audience_automotive_score": None,
                "acquisition_potential_score": None, "view_count": 10,
            },
            {
                "platform": "douyin", "content_direction": "other",
                "evidence_level": None, "selling_point_included": False,
                "primary_tier": None, "content_automotive_score": None,
                "audience_automotive_score": None,
                "acquisition_potential_score": None, "view_count": 990,
            },
        ]

        channel = build_channel_conclusions(rows)["douyin"]
        metric = channel["summary"]["metrics"]["selling_point_exposure_share"]
        self.assertEqual(channel["exposure_coverage_percentage"], 1.0)
        self.assertEqual(metric["status"], "below_threshold")
        self.assertIsNone(metric["numerator"])
        self.assertIsNone(metric["percentage"])
        self.assertIn("10/1000", metric["reason"])

    def test_no_positive_exposure_is_missing_instead_of_zero_or_coverage(self) -> None:
        # douyin exposes view counts: zero positive exposure stays an honest
        # data gap (missing), never a fabricated 0% or coverage number.
        rows = [
            {
                "platform": "douyin", "content_direction": "media",
                "evidence_level": "V3", "selling_point_included": True,
                "primary_tier": "core", "content_automotive_score": 80,
                "audience_automotive_score": None,
                "acquisition_potential_score": None, "view_count": 0,
            },
        ]

        channel = build_channel_conclusions(rows)["douyin"]
        metric = channel["summary"]["metrics"]["selling_point_exposure_share"]
        self.assertEqual(channel["valid_exposure_items"], 0)
        self.assertIsNone(channel["exposure_coverage_percentage"])
        self.assertEqual(metric["status"], "missing")
        self.assertEqual(metric["denominator"], 0)
        self.assertIsNone(metric["percentage"])

    def test_platform_without_view_source_is_not_applicable(self) -> None:
        # 2026-08-07: TikHub's xiaohongshu statistics endpoint returns no read
        # counts (view_count always 0, pre_view_count=-1). The exposure pair
        # reports a structural platform gap, not a data defect.
        rows = [
            {
                "platform": "xiaohongshu", "content_direction": "media",
                "evidence_level": "V3", "selling_point_included": True,
                "primary_tier": "core", "content_automotive_score": 80,
                "audience_automotive_score": None,
                "acquisition_potential_score": None, "view_count": 0,
            },
        ]

        channel = build_channel_conclusions(rows)["xiaohongshu"]
        for key in ("selling_point_exposure_share", "core_selling_point_exposure_share"):
            metric = channel["summary"]["metrics"][key]
            self.assertEqual(metric["status"], "not_applicable")
            self.assertIn("未提供阅读数", metric["reason"])
            self.assertIsNone(metric["percentage"])
        scene = channel["scenes"]["media"]["metrics"]["selling_point_exposure_share"]
        self.assertEqual(scene["status"], "not_applicable")
        self.assertIn("未提供阅读数", scene["reason"])


if __name__ == "__main__":
    unittest.main()
