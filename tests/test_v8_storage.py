from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from v8.storage import SCHEMA_VERSION, connect, initialize_database


class V8StorageTest(unittest.TestCase):
    def test_initial_schema_is_complete_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "v8.sqlite3"
            connection = connect(database)
            try:
                initialize_database(connection)
                initialize_database(connection)
                tables = {
                    str(row[0])
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                required = {
                    "accounts", "account_platform_identities", "content_items",
                    "content_identities", "content_aliases", "import_batches", "import_rows",
                    "fetch_slots", "fetch_attempts", "provider_raw_responses",
                    "provider_usage", "provider_budget_batches", "content_metric_snapshots",
                    "comment_evidence_versions", "comments", "comment_user_scores",
                    "evidence_artifacts", "evidence_envelopes", "media_processing_slots",
                    "taxonomy_versions", "selling_points", "selling_point_scenes",
                    "evaluation_versions", "evaluation_matches", "review_queue",
                    "evaluation_reviews", "manual_evidence", "duplicate_relations",
                    "report_tasks", "task_events", "task_contents", "report_revisions",
                    "report_files", "scheduler_runs", "migration_audit", "migration_row_audit",
                }
                self.assertEqual(required - tables, set())
                self.assertEqual(
                    connection.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0],
                    SCHEMA_VERSION,
                )
                violations = connection.execute("PRAGMA foreign_key_check").fetchall()
                self.assertEqual(violations, [])
            finally:
                connection.close()

    def test_content_platform_check_accepts_all_managed_platforms(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            connection = connect(Path(temporary) / "v8.sqlite3")
            try:
                initialize_database(connection)
                for index, platform in enumerate(
                    ("douyin", "xiaohongshu", "wechat_channels", "kuaishou"), start=1
                ):
                    connection.execute(
                        """
                        INSERT INTO content_items(
                            link_id, platform, platform_content_id, canonical_url,
                            imported_at, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (f"A{index}B2C3", platform, str(index), f"https://example.com/{index}",
                         "2026-08-02T00:00:00Z", "2026-08-02T00:00:00Z", "2026-08-02T00:00:00Z"),
                    )
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM content_items").fetchone()[0], 4)
            finally:
                connection.close()


if __name__ == "__main__":
    unittest.main()
