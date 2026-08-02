from __future__ import annotations

import unittest

from v8.insights import build_channel_conclusions


class V8InsightsTest(unittest.TestCase):
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
