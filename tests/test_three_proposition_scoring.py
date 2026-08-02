#!/usr/bin/env python3

import unittest

from three_proposition_scoring import (
    acquisition_conclusion,
    audience_auto_score,
    audience_conclusion,
    content_auto_score,
    content_conclusion,
    dcd_acquisition_score,
)


class ThreePropositionScoringTest(unittest.TestCase):
    def test_video_dominates_ambiguous_teaser_text(self):
        score, adjustment = content_auto_score(
            text_score=40,
            text_reliability=0.25,
            media_score=96,
            media_reliability=1,
        )
        self.assertEqual((score, adjustment), (83, 0))

    def test_missing_text_is_not_zero_signal(self):
        score, adjustment = content_auto_score(
            text_score=None,
            text_reliability=0,
            media_score=96,
            media_reliability=1,
        )
        self.assertEqual((score, adjustment), (96, 0))

    def test_comments_adjust_content_by_at_most_five(self):
        score, adjustment = content_auto_score(
            text_score=90,
            text_reliability=1,
            media_score=90,
            media_reliability=1,
            comment_topic_score=0,
            valid_unique_commenters=20,
        )
        self.assertEqual((score, adjustment), (85, -5))

    def test_audience_gate_and_score(self):
        self.assertIsNone(
            audience_auto_score(
                {100: 3, 70: 4, 30: 2, 0: 10},
                valid_unique_commenters=19,
                comment_sample_status="below_minimum",
            )
        )
        self.assertEqual(
            audience_auto_score(
                {100: 12, 70: 10, 30: 5, 0: 5},
                valid_unique_commenters=32,
                comment_sample_status="scorable",
            ),
            64,
        )

    def test_audience_requires_scorable_status_and_matching_count(self):
        self.assertIsNone(
            audience_auto_score(
                {100: 12, 70: 10, 30: 5, 0: 5},
                valid_unique_commenters=None,
                comment_sample_status="technical_missing",
            )
        )
        with self.assertRaises(ValueError):
            audience_auto_score(
                {100: 12, 70: 10, 30: 5, 0: 5},
                valid_unique_commenters=31,
                comment_sample_status="scorable",
            )

    def test_content_rounds_once_after_continuous_adjustment(self):
        score, adjustment = content_auto_score(
            text_score=84.6,
            text_reliability=1,
            media_score=84.6,
            media_reliability=1,
            comment_topic_score=90,
            valid_unique_commenters=20,
        )
        self.assertAlmostEqual(adjustment, 0.54)
        self.assertEqual(score, 85)

    def test_acquisition_formula_and_missing_comments(self):
        self.assertEqual(
            dcd_acquisition_score(
                content_score=91,
                audience_score=61,
                dcd_fit_score=95,
                action_intent_score=31,
            ),
            73,
        )
        self.assertIsNone(
            dcd_acquisition_score(
                content_score=91,
                audience_score=None,
                dcd_fit_score=95,
                action_intent_score=None,
            )
        )

    def test_conclusion_boundaries(self):
        self.assertIn("明确", content_conclusion(85))
        self.assertIn("多数", audience_conclusion(60))
        self.assertIn("优先", acquisition_conclusion(80))


if __name__ == "__main__":
    unittest.main()
