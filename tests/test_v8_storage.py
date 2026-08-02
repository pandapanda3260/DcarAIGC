from __future__ import annotations

import sqlite3
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
                    "accounts", "account_platform_identities", "pending_platform_identities",
                    "content_items",
                    "content_identities", "content_aliases", "import_batches", "import_rows",
                    "fetch_slots", "fetch_attempts", "provider_raw_responses",
                    "provider_usage", "provider_budget_batches", "content_metric_snapshots",
                    "comment_evidence_versions", "comments", "comment_user_scores",
                    "evidence_artifacts", "evidence_envelopes", "media_processing_slots",
                    "taxonomy_versions", "selling_points", "selling_point_scenes",
                    "evaluation_versions", "evaluation_matches", "review_queue",
                    "evaluation_reviews", "manual_evidence", "duplicate_relations",
                    "duplicate_fingerprints", "duplicate_calibration_runs",
                    "report_tasks", "task_events", "task_contents", "report_revisions",
                    "report_files", "scheduler_runs", "migration_audit", "migration_row_audit",
                }
                self.assertEqual(required - tables, set())
                self.assertEqual(
                    connection.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0],
                    SCHEMA_VERSION,
                )
                content_slot_columns = [
                    row[2]
                    for row in connection.execute("PRAGMA index_info(uq_fetch_content_slot)")
                ]
                account_slot_columns = [
                    row[2]
                    for row in connection.execute("PRAGMA index_info(uq_fetch_account_slot)")
                ]
                evaluation_columns = [
                    row[2]
                    for row in connection.execute("PRAGMA index_info(uq_evaluation_idempotency)")
                ]
                self.assertEqual(content_slot_columns, ["content_id", "stage", "window_key"])
                self.assertEqual(account_slot_columns, ["account_id", "stage", "window_key"])
                self.assertEqual(
                    evaluation_columns,
                    ["content_id", "rule_version", "taxonomy_version", "evidence_sha256"],
                )
                violations = connection.execute("PRAGMA foreign_key_check").fetchall()
                self.assertEqual(violations, [])
            finally:
                connection.close()

    def test_fetch_business_slot_is_unique_across_provider_and_adapter(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            connection = connect(Path(temporary) / "v8.sqlite3")
            try:
                initialize_database(connection)
                captured_at = "2026-08-02T00:00:00Z"
                connection.execute(
                    """
                    INSERT INTO content_items(
                        link_id, platform, platform_content_id, canonical_url,
                        imported_at, created_at, updated_at
                    ) VALUES ('A2BC3D', 'douyin', '1', 'https://example.com/1', ?, ?, ?)
                    """,
                    (captured_at, captured_at, captured_at),
                )
                connection.execute(
                    """
                    INSERT INTO fetch_slots(
                        content_id, stage, window_key, provider, adapter_version,
                        status, created_at, updated_at
                    ) VALUES (1, 'metrics', '2026-08-02', 'TikHub', 'v1',
                              'succeeded', ?, ?)
                    """,
                    (captured_at, captured_at),
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        """
                        INSERT INTO fetch_slots(
                            content_id, stage, window_key, provider, adapter_version,
                            status, created_at, updated_at
                        ) VALUES (1, 'metrics', '2026-08-02', 'Other', 'v2',
                                  'pending', ?, ?)
                        """,
                        (captured_at, captured_at),
                    )
            finally:
                connection.close()

    def test_unassigned_platform_identity_is_materialized_without_fake_phone(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            connection = connect(Path(temporary) / "v8.sqlite3")
            try:
                initialize_database(connection)
                captured_at = "2026-08-02T00:00:00Z"
                connection.execute(
                    """
                    INSERT INTO content_items(
                        link_id,platform,platform_content_id,canonical_url,
                        raw_account_uid,raw_account_name,imported_at,created_at,updated_at
                    ) VALUES ('A2BC3D','douyin','1','https://example.com/1',
                              '99887766','待归属车号',?,?,?)
                    """,
                    (captured_at, captured_at, captured_at),
                )
                connection.commit()
                initialize_database(connection)
                pending = connection.execute(
                    "SELECT * FROM pending_platform_identities"
                ).fetchone()
                self.assertEqual(pending["platform"], "douyin")
                self.assertEqual(pending["uid"], "99887766")
                self.assertEqual(pending["nickname"], "待归属车号")
                self.assertEqual(pending["content_count"], 1)
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM accounts").fetchone()[0], 0
                )
            finally:
                connection.close()

    def test_schema_upgrade_invalidates_statistics_polluted_evaluation_without_deleting_it(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            connection = connect(Path(temporary) / "v8.sqlite3")
            try:
                initialize_database(connection)
                captured_at = "2026-08-02T00:00:00Z"
                connection.execute(
                    """
                    INSERT INTO content_items(
                        link_id, platform, platform_content_id, canonical_url,
                        imported_at, created_at, updated_at
                    ) VALUES ('A2BC3D', 'douyin', '1', 'https://example.com/1', ?, ?, ?)
                    """,
                    (captured_at, captured_at, captured_at),
                )
                connection.execute(
                    """
                    INSERT INTO provider_raw_responses(
                        content_id, provider, operation, local_path, sha256, byte_size,
                        http_status, captured_at
                    ) VALUES (1, 'TikHub', 'douyin_video_statistics', 'statistics.json',
                              ?, 10, 200, ?)
                    """,
                    ("9" * 64, captured_at),
                )
                envelope = connection.execute(
                    """
                    INSERT INTO evidence_envelopes(
                        content_id, schema_version, detail_raw_sha256, text_sha256,
                        evidence_sha256, components_json, created_at
                    ) VALUES (1, 'evidence-v1', ?, ?, ?, '{}', ?)
                    """,
                    ("9" * 64, "8" * 64, "7" * 64, captured_at),
                )
                connection.execute(
                    """
                    INSERT INTO evaluation_versions(
                        content_id, evidence_envelope_id, rule_version, taxonomy_version,
                        evidence_sha256, evaluation_source, evaluation_status, evidence_level,
                        payload_json, evaluated_at
                    ) VALUES (1, ?, 'evaluation-v6', 'selling-points-v5.0', ?,
                              'automatic', 'evaluated', 'V1', '{}', ?)
                    """,
                    (envelope.lastrowid, "7" * 64, captured_at),
                )
                connection.commit()
                initialize_database(connection)
                row = connection.execute("SELECT * FROM evaluation_versions").fetchone()
                self.assertIsNotNone(row["invalidated_at"])
                self.assertEqual(
                    row["invalidation_reason"],
                    "non_evidence_provider_response_in_envelope",
                )
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM evaluation_versions").fetchone()[0],
                    1,
                )
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
