#!/usr/bin/env python3

import unittest

import rebuild_channel_evaluation_v4 as v4


class ChannelEvaluationV4Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        taxonomy = v4.build_taxonomy()
        cls.label_map = {item["id"]: item for item in taxonomy["labels"]}
        cls.rows = v4.rebuild_douyin(cls.label_map)

    def test_taxonomy_has_only_three_scenes(self) -> None:
        taxonomy = v4.build_taxonomy()
        self.assertEqual(taxonomy["business_scenes"], list(v4.SCENES))
        self.assertNotIn("媒体内容与社区", taxonomy["business_scenes"])
        self.assertTrue(all("business_line" not in item for item in taxonomy["labels"]))
        for item in taxonomy["labels"]:
            assigned = item.get("business_scene_options", [item.get("business_scene")])
            self.assertTrue(assigned)
            self.assertTrue(all(scene in v4.SCENES for scene in assigned))

    def test_all_publication_denominator_is_438(self) -> None:
        metrics = v4.count_metrics(self.rows)
        self.assertEqual(metrics["total_publications"], 438)
        self.assertEqual(metrics["core"] + metrics["other"], metrics["selling_point_covered"])
        self.assertEqual(metrics["selling_point_covered"] + metrics["uncovered_or_unidentifiable"], 438)

    def test_mid_and_end_roll_placements_are_suppressed(self) -> None:
        indexed = {row["aweme_id"]: row for row in self.rows}
        for aweme_id in v4.SUPPRESSED_V3_IDS:
            row = indexed[aweme_id]
            self.assertFalse(row["included"])
            self.assertEqual(row["no_match_id"], "NO_MATCH_PLACEMENT_ONLY")
            self.assertLess(row["content_auto_score"], 40)

    def test_c_labels_are_assigned_to_a_scene(self) -> None:
        for row in self.rows:
            if row["included"] and row["primary_id"].startswith("C"):
                self.assertIn(row["business_scene"], v4.SCENES)

    def test_incomplete_media_does_not_get_content_score(self) -> None:
        incomplete = [row for row in self.rows if row["evidence_level"] in {"V0", "V1"}]
        self.assertEqual(len(incomplete), 1)
        self.assertIsNone(incomplete[0]["content_auto_score"])

    def test_full_video_car_knowledge_is_automotive_even_without_selling_point(self) -> None:
        indexed = {row["aweme_id"]: row for row in self.rows}
        self.assertGreaterEqual(indexed["7664609585642229032"]["content_auto_score"], 70)

    def test_actual_acquisition_is_never_inferred(self) -> None:
        self.assertTrue(all(row["actual_acquisition_score"] is None for row in self.rows))

    def test_xhs_all_publication_selling_point_result_stays_unavailable(self) -> None:
        _, diagnostics = v4.xhs_sample_rows(self.label_map)
        self.assertEqual(diagnostics["total_unique_publication_links"], 338)
        self.assertEqual(diagnostics["full_evidence_selling_point_labelled"], 10)
        self.assertEqual(diagnostics["all_publication_selling_point_metrics"]["status"], "not_computable")
        self.assertEqual(diagnostics["actual_acquisition_effect"]["status"], "not_tested")


if __name__ == "__main__":
    unittest.main()
