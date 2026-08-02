from __future__ import annotations

import json
import sqlite3
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BASELINE_PATH = PROJECT_ROOT / "config" / "v8_migration_baseline.json"
LEGACY_DB = PROJECT_ROOT / "app" / "data" / "web_mvp.sqlite3"


class V8MigrationBaselineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.baseline = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
        if not LEGACY_DB.exists():
            raise AssertionError(f"required legacy database is missing: {LEGACY_DB}")
        cls.connection = sqlite3.connect(LEGACY_DB)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.connection.close()

    def scalar(self, sql: str, parameters: tuple[object, ...] = ()) -> int:
        row = self.connection.execute(sql, parameters).fetchone()
        self.assertIsNotNone(row)
        return int(row[0])

    def test_content_and_platform_counts_are_frozen(self) -> None:
        expected = self.baseline["content"]
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM content_items"), expected["total"])
        for platform, count in expected["by_platform"].items():
            self.assertEqual(
                self.scalar("SELECT COUNT(*) FROM content_items WHERE platform=?", (platform,)),
                count,
            )

    def test_published_at_and_exposure_counts_are_frozen(self) -> None:
        expected = self.baseline["content"]
        self.assertEqual(
            self.scalar("SELECT COUNT(*) FROM content_items WHERE published_at LIKE '%T%'") ,
            expected["published_at"]["iso_8601"],
        )
        self.assertEqual(
            self.scalar(
                "SELECT COUNT(*) FROM content_items "
                "WHERE length(published_at)=10 AND published_at NOT GLOB '*[^0-9]*'"
            ),
            expected["published_at"]["unix_seconds"],
        )
        self.assertEqual(
            self.scalar("SELECT COUNT(*) FROM content_items WHERE published_at=''"),
            expected["published_at"]["missing"],
        )
        self.assertEqual(
            self.scalar("SELECT COUNT(*) FROM content_items WHERE exposure_value IS NOT NULL"),
            expected["exposure"]["present"],
        )
        self.assertEqual(
            self.scalar("SELECT COUNT(*) FROM content_items WHERE exposure_value IS NULL"),
            expected["exposure"]["missing"],
        )

    def test_account_quality_and_business_scene_counts_are_frozen(self) -> None:
        for value, count in self.baseline["account_quality"].items():
            database_value = "" if value == "unknown" else value
            self.assertEqual(
                self.scalar("SELECT COUNT(*) FROM content_items WHERE account_quality=?", (database_value,)),
                count,
            )
        for value, count in self.baseline["business_scene"].items():
            database_value = "" if value == "unknown" else value
            self.assertEqual(
                self.scalar("SELECT COUNT(*) FROM evaluations WHERE business_scene=?", (database_value,)),
                count,
            )

    def test_evaluation_and_selling_point_counts_are_frozen(self) -> None:
        expected = self.baseline["evaluation"]
        combinations = {
            "V0_pending": ("V0", 1),
            "V1_closed": ("V1", 0),
            "V1_pending": ("V1", 1),
            "V2_closed": ("V2", 0),
            "V2_pending": ("V2", 1),
            "V3_closed": ("V3", 0),
        }
        for key, values in combinations.items():
            self.assertEqual(
                self.scalar(
                    "SELECT COUNT(*) FROM evaluations WHERE evidence_level=? AND pending_review=?",
                    values,
                ),
                expected[key],
            )
        for family, count in self.baseline["selling_point_primary_family"].items():
            self.assertEqual(
                self.scalar(
                    "SELECT COUNT(*) FROM evaluations WHERE primary_selling_point_id LIKE ?",
                    (f"{family}%",),
                ),
                count,
            )
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM manual_reviews"), expected["manual_review_history"])

    def test_comment_scores_and_v7_history_are_frozen(self) -> None:
        comments = self.baseline["comment_user_scores"]
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM comment_user_scores"), comments["rows"])
        self.assertEqual(
            self.scalar("SELECT COUNT(DISTINCT content_item_id) FROM comment_user_scores"),
            comments["content_items"],
        )
        history = self.baseline["v7_history"]
        self.assertEqual(
            self.scalar("SELECT COUNT(DISTINCT run_id) FROM report_revisions"),
            history["report_runs"],
        )
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM report_revisions"), history["revisions"])

    def test_legacy_media_backfill_split_is_frozen(self) -> None:
        rows = self.connection.execute(
            """
            SELECT c.platform_content_id
            FROM evaluations e
            JOIN content_items c ON c.id=e.content_item_id
            WHERE e.evidence_level='V1' AND e.pending_review=1
            ORDER BY c.platform_content_id
            """
        ).fetchall()
        present = 0
        missing = 0
        for (content_id,) in rows:
            video = PROJECT_ROOT / "data" / "cache" / "rnote" / "media" / content_id / "video.mp4"
            if video.exists() and video.stat().st_size > 1024:
                present += 1
            else:
                missing += 1
        expected = self.baseline["legacy_media_backfill"]
        self.assertEqual(len(rows), expected["queue_total"])
        self.assertEqual(present, expected["local_evidence_recompute"])
        self.assertEqual(missing, expected["paid_refresh_candidates"])


if __name__ == "__main__":
    unittest.main()
