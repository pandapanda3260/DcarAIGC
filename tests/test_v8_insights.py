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
        used_car = channel["scenes"]["used_car"]["metrics"]
        for metrics in (summary, new_car):
            self.assertEqual(metrics["audience_verticality"]["value"], 60)
            self.assertEqual(metrics["audience_verticality"]["status"], "sample_only")
            self.assertEqual(metrics["acquisition_potential"]["value"], 70)
            self.assertEqual(metrics["acquisition_potential"]["status"], "sample_only")
        self.assertEqual(summary["audience_verticality"]["scorable_items"], 1)
        self.assertEqual(summary["audience_verticality"]["total_items"], 3)
        self.assertEqual(used_car["audience_verticality"]["status"], "missing")
        self.assertIsNone(used_car["audience_verticality"]["value"])

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


if __name__ == "__main__":
    unittest.main()
