#!/usr/bin/env python3

import unittest

import rebuild_channel_evaluation_v4 as v4
import restructure_channel_report_v5 as v5


class StructuredChannelReportV5Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        taxonomy = v4.build_taxonomy()
        label_map = {item["id"]: item for item in taxonomy["labels"]}
        cls.data = {
            "douyin": v5.douyin_section(v4.rebuild_douyin(label_map)),
            "xiaohongshu": v5.xhs_section(*v4.xhs_sample_rows(label_map)),
        }

    def test_fixed_structure_and_metric_order(self) -> None:
        expected = {
            "selling_point_count_share",
            "core_selling_point_count_share",
            "selling_point_exposure_share",
            "core_selling_point_exposure_share",
            "content_verticality",
            "audience_verticality",
            "acquisition_effect_estimate",
        }
        for channel in self.data.values():
            self.assertEqual(set(channel["summary"]), expected)
            self.assertEqual(tuple(channel["scenes"]), v4.SCENES)
            for scene in v4.SCENES:
                self.assertTrue(expected.issubset(channel["scenes"][scene]))

    def test_douyin_summary_and_scene_count_denominator(self) -> None:
        d = self.data["douyin"]
        self.assertEqual(d["denominator"], 438)
        self.assertEqual(d["summary"]["selling_point_count_share"]["value"], 73.52)
        self.assertEqual(d["summary"]["core_selling_point_count_share"]["value"], 42.24)
        self.assertEqual(d["scenes"]["二手车"]["publication_n"], 38)
        self.assertEqual(d["scenes"]["新车"]["publication_n"], 194)
        self.assertEqual(d["scenes"]["媒体-AI小懂"]["publication_n"], 90)
        self.assertEqual(d["scenes"]["新车"]["core_selling_point_count_share"]["value"], 42.01)

    def test_douyin_unavailable_metrics_stay_unavailable(self) -> None:
        d = self.data["douyin"]
        for key in ("selling_point_exposure_share", "core_selling_point_exposure_share", "audience_verticality", "acquisition_effect_estimate"):
            self.assertEqual(d["summary"][key]["status"], "not_computable")

    def test_xhs_sample_does_not_become_channel_value(self) -> None:
        x = self.data["xiaohongshu"]
        self.assertEqual(x["denominator"], 338)
        self.assertIsNone(x["summary"]["selling_point_count_share"]["value"])
        self.assertEqual(x["summary"]["selling_point_count_share"]["status"], "sample_only")
        self.assertEqual(x["summary"]["acquisition_effect_estimate"]["value"], 27)

    def test_xhs_media_scene_sample_metrics(self) -> None:
        media = self.data["xiaohongshu"]["scenes"]["媒体-AI小懂"]
        self.assertEqual(media["sample_n"], 3)
        self.assertEqual(media["content_verticality"]["value"], 93)
        self.assertEqual(media["audience_verticality"]["value"], 50)
        self.assertEqual(media["acquisition_effect_estimate"]["value"], 64)
        self.assertEqual(self.data["xiaohongshu"]["scenes"]["二手车"]["sample_n"], 0)

    def test_all_summary_and_scene_displays_include_percentages(self) -> None:
        for channel in self.data.values():
            for item in channel["summary"].values():
                self.assertIn("%", item["display"])
            for scene in v4.SCENES:
                for key, item in channel["scenes"][scene].items():
                    if isinstance(item, dict) and "display" in item:
                        self.assertIn("%", item["display"])

    def test_content_detail_population_counts(self) -> None:
        taxonomy = v4.build_taxonomy()
        label_map = {item["id"]: item for item in taxonomy["labels"]}
        douyin_rows = v4.rebuild_douyin(label_map)
        xhs_rows, _ = v4.xhs_sample_rows(label_map)
        details = v5.douyin_content_details(douyin_rows) + v5.xhs_content_details(xhs_rows)
        self.assertEqual(len(v5.douyin_content_details(douyin_rows)), 438)
        self.assertEqual(len(v5.xhs_content_details(xhs_rows)), 338)
        for row in details:
            for key in (
                "selling_point",
                "core_selling_point",
                "exposure",
                "content_verticality",
                "audience_verticality",
                "acquisition_effect_estimate",
            ):
                self.assertIn("%", row[key])


if __name__ == "__main__":
    unittest.main()
