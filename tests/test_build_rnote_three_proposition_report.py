import json
import unittest

from build_rnote_three_proposition_report import (
    base_content_rows,
    build_final_results,
    build_summary,
    read_jsonl,
    render_report,
)
from generate_three_proposition_visual import render_visual_svg
from project_paths import RNOTE_CACHE_DIR, XHS_PROCESSED_DIR


class RnoteFinalReportTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.attempts = read_jsonl(RNOTE_CACHE_DIR / "pilot_collection_attempts.jsonl")
        cls.scores = read_jsonl(XHS_PROCESSED_DIR / "rnote_final_scores_v1.jsonl")
        cls.results = build_final_results(cls.attempts, cls.scores)
        prior = read_jsonl(XHS_PROCESSED_DIR / "pilot_content_scores_v0.3.jsonl")
        cls.base = base_content_rows(cls.attempts, prior, cls.results)
        collection = json.loads(
            (RNOTE_CACHE_DIR / "pilot_collection_summary.json").read_text(
                encoding="utf-8"
            )
        )
        cls.summary = build_summary(
            cls.attempts, collection, cls.results, cls.base, None
        )

    def test_final_sample_is_balanced_and_complete(self):
        self.assertEqual(len(self.results), 10)
        self.assertEqual(
            {stratum: sum(row["source_stratum"] == stratum for row in self.results)
             for stratum in ("auto", "non_auto")},
            {"auto": 5, "non_auto": 5},
        )
        self.assertEqual(self.summary["report_state"], "complete")

    def test_every_bucket_exactly_matches_valid_users(self):
        for row in self.results:
            valid = row["valid_unique_commenters"]
            self.assertGreaterEqual(valid, 20)
            self.assertEqual(sum(row["audience_score_counts"].values()), valid)
            self.assertEqual(sum(row["action_score_counts"].values()), valid)
            self.assertTrue(0 <= row["content_auto_score"] <= 100)
            self.assertTrue(0 <= row["audience_auto_score"] <= 100)
            self.assertTrue(0 <= row["dcd_acquisition_score"] <= 100)

    def test_derived_business_cohort_is_not_diluted_by_controls(self):
        cohort = self.summary["derived_content_cohorts"]["automotive_content_70_plus"]
        self.assertEqual(cohort["sample_ids"], ["RA046", "RA077", "RA089"])
        self.assertEqual(cohort["sample_size"], 3)
        self.assertEqual(cohort["audience_auto_score"], 50)
        self.assertEqual(cohort["dcd_acquisition_score"], 64)

    def test_missing_actual_effect_remains_null(self):
        for row in self.results:
            self.assertEqual(row["actual_status"], "not_tested")
            self.assertIsNone(row["actual_clicks"])
            self.assertIsNone(row["actual_installs"])
            self.assertIsNone(row["actual_confirmed_new_users"])

    def test_result_table_shows_score_and_qualitative_conclusion(self):
        report = render_report(self.summary, self.results, self.attempts)
        self.assertIn(
            "| 样本 | 来源组 | 有效评论用户 | 命题1：是否为汽车内容 "
            "| 命题2：互动用户是否偏汽车 | 命题3：是否具备懂车帝拉新潜力 |",
            report,
        )
        self.assertNotIn(
            "| 样本 | 来源组 | 有效用户 | 命题1 内容 | 命题2 用户 | 命题3 拉新潜力 |",
            report,
        )
        row = self.results[0]
        result_line = next(
            line
            for line in report.splitlines()
            if f"[{row['sample_attempt_id']}]({row['url']})" in line
        )
        self.assertIn(
            f"**{row['content_auto_score']}/100**<br>{row['content_auto_conclusion']}",
            result_line,
        )
        self.assertIn(
            f"**{row['audience_auto_score']}/100**<br>{row['audience_auto_conclusion']}",
            result_line,
        )
        self.assertIn(
            f"**{row['dcd_acquisition_score']}/100**<br>{row['dcd_acquisition_conclusion']}",
            result_line,
        )

    def test_report_embeds_visual_and_visual_contains_all_scores(self):
        report = render_report(self.summary, self.results, self.attempts)
        self.assertIn(
            "![最终10篇笔记三命题评分对比]"
            "(rnote_cache/three_proposition_visual_summary.png)",
            report,
        )
        svg = render_visual_svg(self.results)
        for row in self.results:
            self.assertIn(row["sample_attempt_id"], svg)
            for key in (
                "content_auto_score",
                "audience_auto_score",
                "dcd_acquisition_score",
            ):
                self.assertIn(f">{row[key]}</text>", svg)


if __name__ == "__main__":
    unittest.main()
