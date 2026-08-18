"""Full-history local-analysis batch runner is retired (schema v16).

The 2026-08 full-history local-analysis campaign is closed and its frozen
audit chain (`review_pending` item state, `review_queue` closure checks and
`pending_review` evaluation projections) validates against the manual review
domain that schema v16 deleted.  The runner therefore fails closed at
``main()``; its 107 behavioural tests covered exactly that chain and are
replaced by a boundary test pinning the retirement, so the guard cannot
silently disappear.

The module body is intentionally left intact for audit archaeology: the
frozen receipts under ``runtime/`` still reference its policies by name.
"""

from __future__ import annotations

import unittest

import scripts.run_full_local_analysis_batches as batches


class FullLocalAnalysisBatchesRetirementTest(unittest.TestCase):
    def test_main_fails_closed_with_an_explicit_retirement_message(self) -> None:
        with self.assertRaises(SystemExit) as raised:
            batches.main([])
        self.assertEqual(
            str(raised.exception), batches.FULL_LOCAL_ANALYSIS_RETIRED_MESSAGE
        )
        self.assertIn("retired", batches.FULL_LOCAL_ANALYSIS_RETIRED_MESSAGE)

    def test_frozen_audit_policies_are_still_readable_for_receipts(self) -> None:
        # 冻结的历史回执按名字引用这些策略常量，模块体保留以便审计追溯
        self.assertEqual(
            batches.REVIEW_PENDING_POLICY, "sha256_previous_plus_batch_delta_v1"
        )
        self.assertIn("review_pending", batches.ITEM_TERMINAL_STATUSES)


if __name__ == "__main__":
    unittest.main()
