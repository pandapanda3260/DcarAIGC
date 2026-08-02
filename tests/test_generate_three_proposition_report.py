#!/usr/bin/env python3

import unittest

from generate_three_proposition_report import (
    render_markdown,
    summarize_attempts,
    validate_attempt,
)


def attempt_row(**changes):
    row = {
        "sample_attempt_id": "T001",
        "sample_role": "base_random_sample",
        "final_sample_eligible": False,
        "replacement_reason": "comment_not_retrieved",
        "comment_fetch_status": "not_retrieved",
        "raw_comment_count": 0,
        "valid_unique_commenters": None,
        "comment_sample_status": "technical_missing",
        "comment_pages_fetched": 0,
        "comment_pagination_complete": None,
        "source_stratum": "auto",
        "content_auto_score": 90,
        "content_auto_conclusion": "明确属于汽车内容",
        "content_auto_evidence": ["标题和画面持续讨论车型"],
        "audience_score_counts": None,
        "audience_auto_score": None,
        "audience_auto_conclusion": None,
        "audience_auto_evidence": None,
        "dcd_fit_score": None,
        "action_intent_score": None,
        "dcd_acquisition_score": None,
        "dcd_acquisition_conclusion": None,
        "dcd_acquisition_evidence": None,
        "prediction_version": "three-proposition-v1.0",
        "actual_status": "not_tested",
        "actual_clicks": None,
        "actual_installs": None,
        "actual_confirmed_new_users": None,
    }
    row.update(changes)
    return row


def scorable_row(**changes):
    row = attempt_row(
        final_sample_eligible=True,
        replacement_reason=None,
        comment_fetch_status="complete",
        raw_comment_count=24,
        valid_unique_commenters=20,
        comment_sample_status="scorable",
        comment_pages_fetched=3,
        comment_pagination_complete=True,
        audience_score_counts={100: 10, 70: 5, 30: 3, 0: 2},
        audience_auto_score=72,
        audience_auto_conclusion="多数互动用户具有汽车兴趣或需求",
        audience_auto_evidence=["20人中10人表达明确汽车需求"],
        dcd_fit_score=75,
        action_intent_score=60,
        dcd_acquisition_score=74,
        dcd_acquisition_conclusion="存在清晰承接需求，值得进入拉新实验",
        dcd_acquisition_evidence=["价格和车型对比需求可由懂车帝承接"],
    )
    row.update(changes)
    return row


class GenerateThreePropositionReportTest(unittest.TestCase):
    def test_missing_comments_stay_null_and_render_as_data_status(self):
        rows = [attempt_row()]
        summary = summarize_attempts(rows)

        self.assertIsNone(summary["propositions"]["audience_automotive"]["score"])
        self.assertIsNone(
            summary["propositions"]["dcd_acquisition_potential"]["score"]
        )
        report = render_markdown(
            rows,
            summary,
            title="测试报告",
            report_date="2026-07-19",
            input_label="test.jsonl",
        )
        self.assertIn("—（评论正文技术缺失）", report)
        self.assertIn("不代表0分，也没有用50分代填", report)
        self.assertNotIn("命题2：`0/100`", report)
        self.assertNotIn("命题2：`50/100`", report)
        self.assertNotIn("命题3：`0/100`", report)
        self.assertNotIn("命题3：`50/100`", report)

    def test_scorable_row_outputs_all_three_scores_and_conclusions(self):
        rows = [scorable_row()]
        summary = summarize_attempts(rows)
        self.assertEqual(summary["propositions"]["content_automotive"]["score"], 90)
        self.assertEqual(summary["propositions"]["audience_automotive"]["score"], 72)
        self.assertEqual(
            summary["propositions"]["dcd_acquisition_potential"]["score"], 74
        )

        report = render_markdown(
            rows,
            summary,
            title="测试报告",
            report_date="2026-07-19",
            input_label="test.jsonl",
        )
        self.assertIn("命题1：`90/100`", report)
        self.assertIn("命题2：`72/100`", report)
        self.assertIn("命题3：`74/100`（预测分）", report)
        self.assertIn("存在清晰承接需求", report)

    def test_non_scorable_row_cannot_contain_fake_numeric_result(self):
        row = attempt_row(audience_auto_score=0)
        with self.assertRaisesRegex(ValueError, "non-scorable comments require null"):
            validate_attempt(row)

    def test_scorable_row_must_match_formula_and_comment_counts(self):
        with self.assertRaisesRegex(ValueError, "audience_auto_score does not match"):
            validate_attempt(scorable_row(audience_auto_score=50))
        with self.assertRaisesRegex(ValueError, "at least 20 valid users"):
            validate_attempt(scorable_row(valid_unique_commenters=19))

    def test_scorable_row_requires_successful_fetch_and_real_comment_page(self):
        with self.assertRaisesRegex(ValueError, "complete or partial"):
            validate_attempt(scorable_row(comment_fetch_status="failed"))
        with self.assertRaisesRegex(ValueError, "raw_comment_count > 0"):
            validate_attempt(scorable_row(raw_comment_count=0))
        with self.assertRaisesRegex(ValueError, "comment_pages_fetched > 0"):
            validate_attempt(scorable_row(comment_pages_fetched=0))
        with self.assertRaisesRegex(ValueError, "cannot be smaller"):
            validate_attempt(scorable_row(raw_comment_count=19))
        with self.assertRaisesRegex(ValueError, "pagination_complete=true"):
            validate_attempt(scorable_row(comment_pagination_complete=False))
        validate_attempt(
            scorable_row(
                comment_fetch_status="partial",
                comment_pagination_complete=False,
            )
        )
        with self.assertRaisesRegex(ValueError, "pagination_complete=false"):
            validate_attempt(
                scorable_row(
                    comment_fetch_status="partial",
                    comment_pagination_complete=True,
                )
            )

    def test_confirmed_zero_is_distinct_from_technical_missing(self):
        row = attempt_row(
            comment_fetch_status="confirmed_empty",
            valid_unique_commenters=0,
            comment_sample_status="confirmed_zero",
            comment_pages_fetched=1,
            comment_pagination_complete=True,
            replacement_reason="confirmed_zero",
        )
        validate_attempt(row)
        with self.assertRaisesRegex(ValueError, "requires comment_fetch_status=confirmed_empty"):
            validate_attempt({**row, "comment_fetch_status": "not_retrieved"})

    def test_unmeasured_actual_effect_cannot_be_written_as_zero(self):
        with self.assertRaisesRegex(ValueError, "requires null actual-effect fields"):
            validate_attempt(attempt_row(actual_confirmed_new_users=0))

    def test_balanced_target_requires_both_source_strata(self):
        all_auto = []
        for index in range(10):
            row = scorable_row(
                sample_attempt_id=f"A{index:03d}", source_stratum="auto"
            )
            all_auto.append(row)
        summary = summarize_attempts(all_auto, target_final_samples=10)
        self.assertEqual(summary["report_state"], "partial")
        self.assertEqual(summary["counts"]["final_sample_eligible"], 10)
        self.assertEqual(summary["counts"]["final_sample_slots_filled"], 5)
        self.assertEqual(
            summary["counts"]["final_sample_eligible_by_stratum"],
            {"auto": 10, "non_auto": 0},
        )
        self.assertEqual(
            summary["counts"]["final_sample_remaining_by_stratum"],
            {"auto": 0, "non_auto": 5},
        )
        report = render_markdown(
            all_auto,
            summary,
            title="测试报告",
            report_date="2026-07-19",
            input_label="test.jsonl",
        )
        self.assertIn("auto 10/5，non_auto 0/5", report)

        balanced = []
        for stratum in ("auto", "non_auto"):
            for index in range(5):
                balanced.append(
                    scorable_row(
                        sample_attempt_id=f"{stratum}-{index}",
                        source_stratum=stratum,
                    )
                )
        complete = summarize_attempts(balanced, target_final_samples=10)
        self.assertEqual(complete["report_state"], "complete")
        self.assertEqual(complete["counts"]["final_sample_slots_filled"], 10)

    def test_final_sample_requires_known_source_stratum(self):
        with self.assertRaisesRegex(ValueError, "source_stratum=auto or non_auto"):
            validate_attempt(scorable_row(source_stratum=None))


if __name__ == "__main__":
    unittest.main()
