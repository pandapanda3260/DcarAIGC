"""Legacy illegal point/scene repair is retired (schema v16 removed its queue).

The 2026-08 repair already ran in production and its append-only result is
frozen in the database.  Schema v16 deleted the manual review domain the
repair seeded (``review_queue`` / ``evaluation_reviews`` /
``review_reopen_events`` / ``manual_evidence``), so the frozen plan, receipt
and verify chain can never execute again.  The previous 21 behavioural tests
covered exactly that chain; they are replaced by a boundary test that pins the
retirement guard, because a guard that is not tested is a guard that quietly
comes back.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import v8.legacy_scene_repair as repair_module
from v8.legacy_scene_repair import (
    LEGACY_SCENE_REPAIR_RETIRED_MESSAGE,
    LegacySceneRepairError,
    repair_legacy_illegal_scene_chains,
)
from v8.storage import connect, initialize_database


class LegacySceneRepairRetirementTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.addCleanup(self.temporary.cleanup)
        self.db = self.root / "repair.sqlite3"
        with connect(self.db) as connection:
            initialize_database(connection)

    def test_dry_run_and_apply_both_fail_closed_before_touching_anything(self) -> None:
        manifest = self.root / "manifest.json"
        receipt = self.root / "receipt.json"
        for apply, acknowledge in ((False, False), (True, True)):
            with self.assertRaises(LegacySceneRepairError) as raised:
                repair_legacy_illegal_scene_chains(
                    db_path=self.db,
                    manifest_path=manifest,
                    receipt_path=receipt,
                    operator_reason="尝试重跑已退役的历史场景修复",
                    apply=apply,
                    expected_plan_sha256="a" * 64 if apply else None,
                    acknowledge_rollback_window_close=acknowledge,
                )
            self.assertEqual(
                str(raised.exception), LEGACY_SCENE_REPAIR_RETIRED_MESSAGE
            )
        # 守卫在任何文件读取之前触发：缺失的 manifest/receipt 从未被打开
        self.assertFalse(manifest.exists())
        self.assertFalse(receipt.exists())

    def test_the_manual_review_domain_it_seeded_no_longer_exists(self) -> None:
        with connect(self.db) as connection:
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
        self.assertEqual(
            tables
            & {
                "review_queue",
                "evaluation_reviews",
                "review_reopen_events",
                "manual_evidence",
            },
            set(),
        )
        self.assertEqual(
            repair_module.QUEUE_REASON_CODE,
            "legacy_nonautomatic_point_scene_conflict_v1",
        )


if __name__ == "__main__":
    unittest.main()
