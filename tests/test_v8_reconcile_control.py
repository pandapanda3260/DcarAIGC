from __future__ import annotations

import unittest
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context

from v8.reconcile_control import (
    MAX_RECONCILE_ITEMS,
    MAX_RECONCILE_SECONDS,
    current_reconcile_budget,
    reconcile_budget_scope,
)


class ReconcileControlTest(unittest.TestCase):
    def test_default_item_limit_is_exact_and_does_not_leak_context(self) -> None:
        self.assertIsNone(current_reconcile_budget())
        with reconcile_budget_scope() as budget:
            self.assertIs(current_reconcile_budget(), budget)
            self.assertEqual(budget.max_items, MAX_RECONCILE_ITEMS)
            self.assertEqual(budget.max_seconds, MAX_RECONCILE_SECONDS)
            self.assertTrue(budget.take(MAX_RECONCILE_ITEMS))
            self.assertEqual(budget.remaining, 0)
            self.assertEqual(budget.used, MAX_RECONCILE_ITEMS)
            self.assertFalse(budget.take())
        self.assertIsNone(current_reconcile_budget())

    def test_deadline_stops_new_claims_without_rewriting_consumed_count(self) -> None:
        now = [100.0]
        with reconcile_budget_scope(
            max_items=3,
            max_seconds=2.0,
            monotonic_fn=lambda: now[0],
        ) as budget:
            self.assertEqual(budget.deadline, 102.0)
            self.assertTrue(budget.take())
            self.assertEqual(budget.remaining, 2)
            now[0] = budget.deadline
            self.assertTrue(budget.expired)
            self.assertEqual(budget.remaining, 0)
            self.assertEqual(budget.used, 1)
            self.assertFalse(budget.take())

    def test_take_validates_count_and_is_all_or_nothing(self) -> None:
        with reconcile_budget_scope(max_items=3) as budget:
            for invalid in (True, 1.0):
                with self.subTest(invalid=invalid), self.assertRaises(TypeError):
                    budget.take(invalid)  # type: ignore[arg-type]
            for invalid in (0, -1):
                with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                    budget.take(invalid)
            self.assertFalse(budget.take(4))
            self.assertEqual(budget.remaining, 3)
            self.assertTrue(budget.take(3))

    def test_completed_work_is_accounted_after_deadline_without_new_authority(self) -> None:
        now = [100.0]
        with reconcile_budget_scope(
            max_items=3,
            max_seconds=2.0,
            monotonic_fn=lambda: now[0],
        ) as budget:
            materializer_limit = budget.remaining
            self.assertEqual(materializer_limit, 3)
            now[0] = budget.deadline
            self.assertFalse(budget.take())
            budget.account_completed(2)
            self.assertEqual(budget.used, 2)
            self.assertEqual(budget.remaining, 0)
            self.assertFalse(budget.take())

            for invalid in (True, 1.0):
                with self.subTest(invalid=invalid), self.assertRaises(TypeError):
                    budget.account_completed(invalid)  # type: ignore[arg-type]
            for invalid in (0, -1):
                with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                    budget.account_completed(invalid)
            with self.assertRaisesRegex(ValueError, "exceeds remaining"):
                budget.account_completed(2)
            self.assertEqual(budget.used, 2)

    def test_limits_reject_bool_float_and_out_of_contract_values(self) -> None:
        for item_limit in (True, 1.0):
            with self.subTest(max_items=item_limit), self.assertRaises(TypeError):
                with reconcile_budget_scope(max_items=item_limit):  # type: ignore[arg-type]
                    self.fail("invalid item limit entered the context")
        for item_limit in (0, MAX_RECONCILE_ITEMS + 1):
            with self.subTest(max_items=item_limit), self.assertRaises(ValueError):
                with reconcile_budget_scope(max_items=item_limit):
                    self.fail("out-of-range item limit entered the context")
        for seconds_limit in (True, "1"):
            with self.subTest(max_seconds=seconds_limit), self.assertRaises(TypeError):
                with reconcile_budget_scope(max_seconds=seconds_limit):  # type: ignore[arg-type]
                    self.fail("invalid duration entered the context")
        for seconds_limit in (
            0,
            float("nan"),
            float("inf"),
            MAX_RECONCILE_SECONDS + 0.1,
        ):
            with self.subTest(max_seconds=seconds_limit), self.assertRaises(ValueError):
                with reconcile_budget_scope(max_seconds=seconds_limit):
                    self.fail("out-of-range duration entered the context")

        with reconcile_budget_scope(max_items=1, max_seconds=0.25) as budget:
            self.assertEqual((budget.max_items, budget.max_seconds), (1, 0.25))

    def test_nested_context_restores_outer_budget_even_after_error(self) -> None:
        with reconcile_budget_scope(max_items=4) as outer:
            self.assertTrue(outer.take())
            with self.assertRaisesRegex(RuntimeError, "fixture"):
                with reconcile_budget_scope(max_items=2) as inner:
                    self.assertIs(current_reconcile_budget(), inner)
                    self.assertTrue(inner.take(2))
                    raise RuntimeError("fixture")
            self.assertIs(current_reconcile_budget(), outer)
            self.assertEqual(outer.remaining, 3)
        self.assertIsNone(current_reconcile_budget())

    def test_copied_context_threads_share_one_atomic_item_budget(self) -> None:
        with reconcile_budget_scope(max_items=MAX_RECONCILE_ITEMS) as budget:
            contexts = [copy_context() for _ in range(MAX_RECONCILE_ITEMS * 2)]

            def claim() -> tuple[bool, bool]:
                return current_reconcile_budget() is budget, budget.take()

            with ThreadPoolExecutor(max_workers=16) as executor:
                results = list(
                    executor.map(
                        lambda context: context.run(claim),
                        contexts,
                    )
                )

            self.assertTrue(all(shared for shared, _claimed in results))
            self.assertEqual(sum(claimed for _shared, claimed in results), 50)
            self.assertEqual(budget.remaining, 0)
            self.assertEqual(budget.used, 50)


if __name__ == "__main__":
    unittest.main()
