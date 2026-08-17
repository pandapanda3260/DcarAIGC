from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from v8.migration import _encode_link_material, migrate
from v8.storage import connect, initialize_database

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BASELINE = json.loads(
    (PROJECT_ROOT / "config" / "v8_migration_baseline.json").read_text(encoding="utf-8")
)


class V8MigrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temporary = tempfile.TemporaryDirectory()
        cls.database = Path(cls.temporary.name) / "migrated.sqlite3"
        migrate(target_db=cls.database)
        cls.connection = connect(cls.database)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.connection.close()
        cls.temporary.cleanup()

    def scalar(self, sql: str, parameters: tuple[object, ...] = ()) -> int:
        row = self.connection.execute(sql, parameters).fetchone()
        self.assertIsNotNone(row)
        return int(row[0])

    def test_link_ids_are_unique_mixed_and_unambiguous(self) -> None:
        ids = [
            str(row[0])
            for row in self.connection.execute("SELECT link_id FROM content_items")
        ]
        self.assertEqual(len(ids), BASELINE["content"]["total"])
        self.assertEqual(len(ids), len(set(ids)))
        for value in ids:
            self.assertRegex(value, r"^[23456789ABCDEFGHJKLMNPQRSTUVWXYZ]{6}$")
            self.assertRegex(value, r"[23456789]")
            self.assertRegex(value, r"[ABCDEFGHJKLMNPQRSTUVWXYZ]")
        self.assertEqual(
            _encode_link_material("douyin:123"), _encode_link_material("douyin:123")
        )

    def test_content_dates_and_direction_mapping_are_normalized(self) -> None:
        expected = BASELINE["content"]
        self.assertEqual(
            self.scalar("SELECT COUNT(*) FROM content_items"), expected["total"]
        )
        self.assertEqual(
            self.scalar(
                "SELECT COUNT(*) FROM content_items WHERE published_at IS NULL"
            ),
            expected["published_at"]["missing"],
        )
        self.assertEqual(
            self.scalar(
                "SELECT COUNT(*) FROM content_items WHERE published_at LIKE '%Z'"
            ),
            expected["total"] - expected["published_at"]["missing"],
        )
        direction_counts = {
            str(row["evaluation_content_direction"]): int(row["n"])
            for row in self.connection.execute(
                "SELECT evaluation_content_direction, COUNT(*) n FROM content_items GROUP BY 1"
            )
        }
        self.assertEqual(
            direction_counts,
            {
                "new_car": 195,
                "used_car": 44,
                "media": 159,
                "unknown": 378,
            },
        )

    def test_fetch_slot_initialization_matches_baseline(self) -> None:
        expected = BASELINE["fetch_slot_initialization"]
        checks = {
            ("detail", "succeeded"): expected["detail_succeeded"],
            ("detail", "terminal_failed"): expected["detail_terminal_failed"],
            ("metrics", "succeeded"): expected["metrics_succeeded"],
            ("metrics", "terminal_failed"): expected["metrics_terminal_failed"],
            ("comments", "succeeded"): expected["comments_succeeded"],
        }
        for (stage, status), count in checks.items():
            self.assertEqual(
                self.scalar(
                    "SELECT COUNT(*) FROM fetch_slots WHERE stage=? AND status=?",
                    (stage, status),
                ),
                count,
            )
        weeks = {
            str(row["iso_week"]): int(row["n"])
            for row in self.connection.execute(
                "SELECT iso_week, COUNT(*) n FROM comment_evidence_versions GROUP BY iso_week"
            )
        }
        self.assertEqual(weeks, {"2026-W29": 56, "2026-W31": 720})

    def test_evaluations_reviews_and_artifacts_are_complete(self) -> None:
        total = BASELINE["content"]["total"]
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM evidence_envelopes"), total)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM evaluation_versions"), total)
        self.assertEqual(
            self.scalar(
                "SELECT COUNT(*) FROM evaluation_versions WHERE evaluation_source='migrated_from_v5'"
            ),
            total,
        )
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM review_queue"), 186)
        routes = {
            str(row["reason_code"]): int(row["n"])
            for row in self.connection.execute(
                "SELECT reason_code, COUNT(*) n FROM review_queue GROUP BY reason_code"
            )
        }
        self.assertEqual(
            routes,
            {
                "legacy_content_unavailable": 12,
                "media_evidence_missing": 164,
                "stale_local_evidence": 1,
                "evaluation_gray_zone": 9,
            },
        )
        self.assertEqual(
            self.scalar(
                "SELECT COUNT(*) FROM evidence_artifacts WHERE status='available' AND sha256 IS NULL"
            ),
            0,
        )

    def test_budget_is_seeded_without_provider_calls(self) -> None:
        row = self.connection.execute(
            "SELECT * FROM provider_budget_batches WHERE purpose='legacy_media_backfill'"
        ).fetchone()
        self.assertIsNotNone(row)
        expected = BASELINE["legacy_media_backfill"]
        self.assertEqual(
            row["max_billable_requests"], expected["paid_refresh_candidates"]
        )
        self.assertAlmostEqual(row["verified_unit_price"], expected["unit_price_usd"])
        self.assertAlmostEqual(row["max_amount"], expected["hard_budget_usd"])
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM fetch_attempts"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM provider_usage"), 0)

    def test_pending_identities_are_materialized_by_the_migration_transaction(
        self,
    ) -> None:
        expected_groups = self.scalar(
            """
            SELECT COUNT(*) FROM (
                SELECT platform,raw_account_uid
                FROM content_items
                WHERE account_id IS NULL AND COALESCE(raw_account_uid,'')<>''
                GROUP BY platform,raw_account_uid
            )
            """
        )
        expected_contents = self.scalar(
            """
            SELECT COUNT(*) FROM content_items
            WHERE account_id IS NULL AND COALESCE(raw_account_uid,'')<>''
            """
        )
        self.assertEqual(
            self.scalar("SELECT COUNT(*) FROM pending_platform_identities"),
            expected_groups,
        )
        self.assertEqual(
            self.scalar(
                "SELECT COALESCE(SUM(content_count),0) FROM pending_platform_identities"
            ),
            expected_contents,
        )
        changes_before = self.connection.total_changes
        initialize_database(self.connection)
        self.assertEqual(self.connection.total_changes, changes_before)

    def test_migration_is_idempotent_and_foreign_keys_are_valid(self) -> None:
        before = {
            table: self.scalar(f"SELECT COUNT(*) FROM {table}")
            for table in (
                "content_items",
                "content_metric_snapshots",
                "content_metric_observations",
                "fetch_slots",
                "evaluation_versions",
                "review_queue",
            )
        }
        summary = migrate(target_db=self.database)
        after = {
            table: self.scalar(f"SELECT COUNT(*) FROM {table}") for table in before
        }
        self.assertEqual(before, after)
        self.assertEqual(summary["content_items"], BASELINE["content"]["total"])
        self.assertEqual(
            self.scalar("SELECT COUNT(*) FROM content_metric_observations"),
            self.scalar("SELECT COUNT(*) FROM content_metric_snapshots"),
        )
        self.assertEqual(
            self.scalar(
                """
                SELECT COUNT(*)
                FROM content_metric_snapshots ms
                LEFT JOIN content_metric_observations mo
                  ON mo.legacy_snapshot_id=ms.id
                 AND mo.observation_origin='legacy_snapshot_baseline'
                WHERE mo.id IS NULL
                """
            ),
            0,
        )
        self.assertEqual(
            self.connection.execute("PRAGMA foreign_key_check").fetchall(), []
        )

    def test_taxonomy_preserves_multi_scene_c_labels(self) -> None:
        rows = self.connection.execute(
            """
            SELECT sp.code, GROUP_CONCAT(sps.scene, ',') scenes
            FROM selling_points sp
            JOIN selling_point_scenes sps ON sps.selling_point_id=sp.id
            WHERE sp.code IN ('C1','C2','C3','C4')
            GROUP BY sp.code ORDER BY sp.code
            """
        ).fetchall()
        actual = {str(row["code"]): set(str(row["scenes"]).split(",")) for row in rows}
        self.assertEqual(
            actual,
            {
                "C1": {"used_car", "media"},
                "C2": {"used_car", "new_car"},
                "C3": {"media"},
                "C4": {"used_car", "new_car", "media"},
            },
        )


if __name__ == "__main__":
    unittest.main()
