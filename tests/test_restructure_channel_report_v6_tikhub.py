#!/usr/bin/env python3

import unittest

import restructure_channel_report_v6_tikhub as v6


class StructuredChannelReportV6TikHubTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.data = v6.build()

    def test_user_approved_structure_and_metric_order(self) -> None:
        self.assertEqual(
            self.data["structure"],
            [
                "douyin_summary_and_scenes",
                "xiaohongshu_summary_and_scenes",
                "conclusion_summary",
                "douyin_content_details",
                "xiaohongshu_content_details",
                "supporting_files",
            ],
        )
        expected = set(self.data["metric_order"])
        for channel in self.data["channels"].values():
            self.assertEqual(set(channel["summary"]), expected)
            self.assertEqual(tuple(channel["scenes"]), v6.v4.SCENES)

    def test_douyin_tikhub_metrics(self) -> None:
        d = self.data["channels"]["douyin"]
        self.assertEqual(d["denominator"], 438)
        self.assertEqual(d["summary"]["selling_point_exposure_share"]["value"], 39.39)
        self.assertEqual(d["summary"]["core_selling_point_exposure_share"]["value"], 5.2)
        self.assertEqual(d["summary"]["audience_verticality"]["value"], 40)
        self.assertEqual(d["summary"]["acquisition_effect_estimate"]["value"], 34)
        self.assertEqual(len(d["content_details"]), 438)

    def test_scene_sample_boundaries(self) -> None:
        d = self.data["channels"]["douyin"]["scenes"]
        self.assertIsNone(d["二手车"]["audience_verticality"]["value"])
        self.assertEqual(d["新车"]["audience_verticality"]["value"], 52)
        self.assertEqual(d["新车"]["acquisition_effect_estimate"]["value"], 67)
        self.assertEqual(d["媒体-AI小懂"]["audience_verticality"]["value"], 50)
        self.assertEqual(d["媒体-AI小懂"]["acquisition_effect_estimate"]["value"], 63)

    def test_full_content_details_for_both_channels(self) -> None:
        self.assertEqual(len(self.data["channels"]["douyin"]["content_details"]), 438)
        self.assertEqual(len(self.data["channels"]["xiaohongshu"]["content_details"]), 338)
        for channel in self.data["channels"].values():
            for item in channel["summary"].values():
                self.assertIn("%", item["display"])

    def test_report_display_order(self) -> None:
        report = v6.build_report(self.data)
        headings = [
            "## 【抖音渠道】",
            "## 【小红书渠道】",
            "## 结论摘要",
            "## 抖音内容明细",
            "## 小红书内容明细",
            "## 配套文件",
        ]
        positions = [report.index(heading) for heading in headings]
        self.assertEqual(positions, sorted(positions))
        self.assertNotIn("### 3、内容明细", report)


if __name__ == "__main__":
    unittest.main()
