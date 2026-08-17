from __future__ import annotations

import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

from v8.storage import (
    DEFAULT_DB,
    LEGACY_MATCHER_RULE_SHA256,
    SCHEMA_VERSION,
    SchemaMigrationError,
    configure_connection_safety,
    connect,
    ensure_legacy_evaluation_release,
    initialize_database,
    require_schema_compatibility,
    schema_compatibility_state,
)


class V8StorageTest(unittest.TestCase):
    def test_test_guard_rejects_formal_database(self) -> None:
        with patch.dict("os.environ", {"DCAR_TEST_DENY_FORMAL_DB": "1"}):
            with self.assertRaisesRegex(RuntimeError, "formal DCar database"):
                connect(DEFAULT_DB)

    def test_connect_enables_recursive_triggers_for_writable_and_read_only(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "v8.sqlite3"
            with connect(database) as connection:
                self.assertEqual(
                    int(connection.execute("PRAGMA foreign_keys").fetchone()[0]), 1
                )
                self.assertEqual(
                    int(
                        connection.execute("PRAGMA recursive_triggers").fetchone()[0]
                    ),
                    1,
                )
                initialize_database(connection)
                connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            with connect(database, read_only=True) as connection:
                self.assertEqual(
                    int(connection.execute("PRAGMA foreign_keys").fetchone()[0]), 1
                )
                self.assertEqual(
                    int(
                        connection.execute("PRAGMA recursive_triggers").fetchone()[0]
                    ),
                    1,
                )
                self.assertTrue(schema_compatibility_state(connection)["compatible"])

    def test_connection_safety_fails_closed_when_foreign_keys_cannot_be_enabled(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "v8.sqlite3"
            raw = sqlite3.connect(database)
            try:
                raw.execute("BEGIN")
                with self.assertRaisesRegex(RuntimeError, "foreign keys"):
                    configure_connection_safety(raw)
                self.assertEqual(
                    int(raw.execute("PRAGMA foreign_keys").fetchone()[0]), 0
                )
            finally:
                raw.rollback()
                raw.close()

    def test_initialize_and_compatibility_reject_recursive_triggers_off(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "v8.sqlite3"
            raw = sqlite3.connect(database)
            raw.row_factory = sqlite3.Row
            try:
                self.assertEqual(
                    int(raw.execute("PRAGMA recursive_triggers").fetchone()[0]), 0
                )
                with self.assertRaisesRegex(
                    SchemaMigrationError, "recursive_triggers=ON"
                ):
                    initialize_database(raw)
                self.assertEqual(
                    raw.execute(
                        """
                        SELECT COUNT(*) FROM sqlite_master
                        WHERE type='table' AND name NOT LIKE 'sqlite_%'
                        """
                    ).fetchone()[0],
                    0,
                )
                raw.execute("PRAGMA recursive_triggers=ON")
                initialize_database(raw)
                raw.execute("PRAGMA recursive_triggers=OFF")
                state = schema_compatibility_state(raw)
                self.assertFalse(state["compatible"])
                self.assertFalse(state["recursive_triggers_enabled"])
                with self.assertRaisesRegex(
                    SchemaMigrationError, "recursive_triggers=False"
                ):
                    require_schema_compatibility(raw)
            finally:
                raw.close()

    def test_initial_schema_is_complete_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "v8.sqlite3"
            connection = connect(database)
            try:
                initialize_database(connection)
                changes_before = connection.total_changes
                initialize_database(connection)
                self.assertEqual(connection.total_changes, changes_before)
                tables = {
                    str(row[0])
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                required = {
                    "accounts",
                    "account_platform_identities",
                    "pending_platform_identities",
                    "content_items",
                    "content_identities",
                    "content_aliases",
                    "import_batches",
                    "import_rows",
                    "fetch_slots",
                    "fetch_attempts",
                    "provider_raw_responses",
                    "provider_usage",
                    "provider_budget_batches",
                    "content_metric_snapshots",
                    "content_metric_observations",
                    "comment_evidence_versions",
                    "comments",
                    "comment_user_scores",
                    "evidence_artifacts",
                    "evidence_envelopes",
                    "media_processing_slots",
                    "taxonomy_versions",
                    "selling_points",
                    "selling_point_scenes",
                    "evaluation_releases",
                    "evaluation_versions",
                    "evaluation_matches",
                    "review_queue",
                    "evaluation_reviews",
                    "review_reopen_events",
                    "manual_evidence",
                    "duplicate_relations",
                    "duplicate_fingerprints",
                    "duplicate_calibration_runs",
                    "report_tasks",
                    "task_events",
                    "task_contents",
                    "report_revisions",
                    "report_files",
                    "scheduler_runs",
                    "scheduler_run_attempts",
                    "migration_audit",
                    "migration_row_audit",
                }
                self.assertEqual(required - tables, set())
                self.assertEqual(
                    connection.execute(
                        "SELECT MAX(version) FROM schema_migrations"
                    ).fetchone()[0],
                    SCHEMA_VERSION,
                )
                content_slot_columns = [
                    row[2]
                    for row in connection.execute(
                        "PRAGMA index_info(uq_fetch_content_slot)"
                    )
                ]
                account_slot_columns = [
                    row[2]
                    for row in connection.execute(
                        "PRAGMA index_info(uq_fetch_account_slot)"
                    )
                ]
                self.assertEqual(
                    content_slot_columns, ["content_id", "stage", "window_key"]
                )
                self.assertEqual(
                    account_slot_columns, ["account_id", "stage", "window_key"]
                )
                evaluation_indexes = {
                    str(row["name"]): [
                        str(column["name"])
                        for column in connection.execute(
                            f"PRAGMA index_info('{row['name']}')"
                        )
                    ]
                    for row in connection.execute(
                        "PRAGMA index_list(evaluation_versions)"
                    )
                }
                self.assertEqual(
                    evaluation_indexes["uq_evaluation_automatic_idempotency"],
                    ["content_id", "release_id", "evidence_sha256"],
                )
                self.assertEqual(
                    evaluation_indexes["uq_evaluation_manual_idempotency"],
                    ["release_id", "review_id"],
                )
                self.assertEqual(
                    evaluation_indexes["uq_evaluation_migrated_parent_idempotency"],
                    ["release_id", "parent_evaluation_id"],
                )
                self.assertNotIn("uq_evaluation_idempotency", evaluation_indexes)
                self.assertNotIn(
                    [
                        "content_id",
                        "rule_version",
                        "taxonomy_version",
                        "evidence_sha256",
                    ],
                    evaluation_indexes.values(),
                )
                metric_indexes = {
                    str(row["name"])
                    for row in connection.execute(
                        "PRAGMA index_list(content_metric_observations)"
                    )
                }
                self.assertIn(
                    "idx_metric_observations_content_capture", metric_indexes
                )
                self.assertIsNotNone(
                    connection.execute(
                        """
                        SELECT 1 FROM sqlite_master
                        WHERE type='trigger'
                          AND name='trg_metric_observations_immutable_payload'
                        """
                    ).fetchone()
                )
                self.assertIsNotNone(
                    connection.execute(
                        """
                        SELECT 1 FROM sqlite_master
                        WHERE type='trigger'
                          AND name='trg_metric_observations_no_delete'
                        """
                    ).fetchone()
                )
                violations = connection.execute("PRAGMA foreign_key_check").fetchall()
                self.assertEqual(violations, [])
            finally:
                connection.close()

    def test_runtime_schema_compatibility_accepts_complete_v11_v12_and_v13(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            connection = connect(Path(temporary) / "v8.sqlite3")
            try:
                initialize_database(connection)
                self.assertEqual(require_schema_compatibility(connection), 13)
                self.assertTrue(schema_compatibility_state(connection)["compatible"])

                connection.execute("DROP TABLE scheduler_run_attempts")
                connection.execute("DELETE FROM schema_migrations WHERE version=13")
                connection.execute("PRAGMA user_version=12")
                connection.commit()
                self.assertEqual(require_schema_compatibility(connection), 12)
                self.assertTrue(schema_compatibility_state(connection)["compatible"])

                connection.execute("DROP TABLE content_metric_observations")
                connection.execute("DROP INDEX idx_content_identities_content_primary")
                connection.execute("DELETE FROM schema_migrations WHERE version=12")
                connection.execute("PRAGMA user_version=11")
                connection.commit()
                self.assertEqual(require_schema_compatibility(connection), 11)
            finally:
                connection.close()

    def test_runtime_schema_compatibility_rejects_incomplete_manifests(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            connection = connect(Path(temporary) / "v8.sqlite3")
            try:
                initialize_database(connection)
                connection.execute("DROP TABLE scheduler_run_attempts")
                with self.assertRaisesRegex(
                    SchemaMigrationError, "incompatible or incomplete schema"
                ):
                    require_schema_compatibility(connection)

                connection.execute(
                    """
                    CREATE TABLE scheduler_run_attempts(id INTEGER PRIMARY KEY)
                    """,
                )
                with self.assertRaisesRegex(
                    SchemaMigrationError, "incompatible or incomplete schema"
                ):
                    require_schema_compatibility(connection)
            finally:
                connection.close()

    def test_runtime_schema_compatibility_rejects_counterfeit_v11(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            connection = connect(Path(temporary) / "v8.sqlite3")
            try:
                connection.executescript(
                    """
                    CREATE TABLE schema_migrations(
                        version INTEGER PRIMARY KEY,
                        name TEXT NOT NULL UNIQUE,
                        applied_at TEXT NOT NULL
                    );
                    INSERT INTO schema_migrations(version,name,applied_at)
                    VALUES (11,'interaction-user-v1-fallback-keys',
                            '2026-08-15T00:00:00Z');
                    CREATE TABLE content_items(id INTEGER PRIMARY KEY);
                    PRAGMA user_version=11;
                    """
                )
                self.assertFalse(
                    schema_compatibility_state(connection)["compatible"]
                )
                with self.assertRaisesRegex(
                    SchemaMigrationError, "incompatible or incomplete schema"
                ):
                    require_schema_compatibility(connection)
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

    def test_current_schema_initialization_does_not_materialize_pending_identity(
        self,
    ) -> None:
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
                changes_before = connection.total_changes
                initialize_database(connection)
                self.assertEqual(connection.total_changes, changes_before)
                self.assertIsNone(
                    connection.execute(
                        "SELECT * FROM pending_platform_identities"
                    ).fetchone()
                )
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM accounts").fetchone()[0], 0
                )
            finally:
                connection.close()

    def test_current_schema_initialization_preserves_statistics_polluted_evaluation(
        self,
    ) -> None:
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
                    INSERT INTO taxonomy_versions(
                        id,version,status,definition,created_at,published_at
                    ) VALUES ('taxonomy','selling-points-v5.0','published','test',?,?)
                    """,
                    (captured_at, captured_at),
                )
                release = ensure_legacy_evaluation_release(
                    connection,
                    rule_version="evaluation-v6",
                    taxonomy_version="selling-points-v5.0",
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
                        content_id, evidence_envelope_id, release_id,
                        rule_version, taxonomy_version, matcher_rule_sha256,
                        evidence_sha256, evaluation_source, evaluation_status, evidence_level,
                        payload_json, evaluated_at
                    ) VALUES (1, ?, ?, 'evaluation-v6', 'selling-points-v5.0', ?, ?,
                              'automatic', 'evaluated', 'V1', '{}', ?)
                    """,
                    (
                        envelope.lastrowid,
                        release["id"],
                        LEGACY_MATCHER_RULE_SHA256,
                        "7" * 64,
                        captured_at,
                    ),
                )
                connection.commit()
                before = dict(
                    connection.execute("SELECT * FROM evaluation_versions").fetchone()
                )
                changes_before = connection.total_changes
                initialize_database(connection)
                row = dict(
                    connection.execute("SELECT * FROM evaluation_versions").fetchone()
                )
                self.assertEqual(connection.total_changes, changes_before)
                self.assertEqual(row, before)
                self.assertIsNone(row["invalidated_at"])
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM evaluation_versions"
                    ).fetchone()[0],
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
                        (
                            f"A{index}B2C3",
                            platform,
                            str(index),
                            f"https://example.com/{index}",
                            "2026-08-02T00:00:00Z",
                            "2026-08-02T00:00:00Z",
                            "2026-08-02T00:00:00Z",
                        ),
                    )
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM content_items").fetchone()[
                        0
                    ],
                    4,
                )
            finally:
                connection.close()


if __name__ == "__main__":
    unittest.main()
