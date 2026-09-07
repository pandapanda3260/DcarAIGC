from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import os
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from pathlib import Path

import v8.storage as storage_module
from tests.schema_fixture import initialize_historical_schema
from v8.storage import (
    DEFAULT_DB,
    LEGACY_MATCHER_RULE_SHA256,
    SCHEMA_VERSION,
    SchemaMigrationError,
    configure_connection_safety,
    connect,
    ensure_legacy_evaluation_release,
    initialize_database,
    live_wal_read_only_connections,
    require_schema_compatibility,
    same_database_path,
    schema_compatibility_state,
    transaction,
    transaction_metrics_context,
)


class V8StorageTest(unittest.TestCase):
    def test_write_transaction_metrics_are_external_private_and_context_bound(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            metrics_root = root / "metrics"
            metrics_root.mkdir(mode=0o700)
            metrics = metrics_root / "sqlite.jsonl"
            database = root / "instrumented.sqlite3"
            with patch.dict(os.environ, {"DCAR_SQLITE_METRICS_FILE": str(metrics)}):
                with connect(database) as connection:
                    connection.execute("CREATE TABLE writes(value TEXT NOT NULL)")
                    connection.commit()
                    with transaction_metrics_context(
                        job_id="fixture", scheduler_run_id=12, attempt_id=34
                    ), transaction(connection):
                        connection.execute("INSERT INTO writes VALUES ('ok')")

            records = [json.loads(line) for line in metrics.read_text().splitlines()]
            self.assertEqual(len(records), 2)
            self.assertEqual([row["phase"] for row in records], ["begin", "finish"])
            self.assertEqual(records[0]["transaction_id"], records[1]["transaction_id"])
            self.assertEqual(
                {
                    key: records[1][key]
                    for key in ("schema", "outcome", "job_id", "scheduler_run_id", "attempt_id")
                },
                {
                    "schema": "sqlite-write-transaction-v1",
                    "outcome": "committed",
                    "job_id": "fixture",
                    "scheduler_run_id": 12,
                    "attempt_id": 34,
                },
            )
            self.assertTrue(records[1]["wal"])
            self.assertGreaterEqual(records[0]["queue_wait_ms"], 0)
            self.assertGreaterEqual(records[1]["hold_ms"], 0)
            self.assertEqual(metrics.stat().st_mode & 0o777, 0o600)

    def test_write_transaction_metrics_classify_locked_rollback(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            metrics_root = root / "metrics"
            metrics_root.mkdir(mode=0o700)
            metrics = metrics_root / "sqlite.jsonl"
            database = root / "instrumented.sqlite3"
            with patch.dict(os.environ, {"DCAR_SQLITE_METRICS_FILE": str(metrics)}):
                with connect(database) as connection:
                    connection.execute("CREATE TABLE writes(value TEXT NOT NULL)")
                    connection.commit()
                    with self.assertRaisesRegex(sqlite3.OperationalError, "locked"):
                        with transaction(connection):
                            connection.execute("INSERT INTO writes VALUES ('rollback')")
                            raise sqlite3.OperationalError("database is locked")
                    self.assertEqual(
                        connection.execute("SELECT COUNT(*) FROM writes").fetchone()[0], 0
                    )
            records = [json.loads(line) for line in metrics.read_text().splitlines()]
            self.assertEqual([row["phase"] for row in records], ["begin", "finish"])
            record = records[-1]
            self.assertEqual(record["outcome"], "locked")
            self.assertEqual(record["error_type"], "OperationalError")

    def test_configured_metrics_path_fails_closed_before_begin(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "instrumented.sqlite3"
            with connect(database) as connection:
                connection.execute("CREATE TABLE writes(value TEXT NOT NULL)")
                connection.commit()
                with patch.dict(
                    os.environ, {"DCAR_SQLITE_METRICS_FILE": "relative.jsonl"}
                ), self.assertRaisesRegex(
                    storage_module.TransactionMetricsError, "must be absolute"
                ):
                    with transaction(connection):
                        connection.execute("INSERT INTO writes VALUES ('forbidden')")
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM writes").fetchone()[0], 0
                )

    def test_finish_metric_failure_never_makes_committed_write_look_retryable(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            metrics_root = root / "metrics"
            metrics_root.mkdir(mode=0o700)
            database = root / "instrumented.sqlite3"
            with connect(database) as connection:
                connection.execute("CREATE TABLE writes(value TEXT NOT NULL)")
                connection.commit()
                calls = 0

                def emit(_payload):
                    nonlocal calls
                    calls += 1
                    if calls == 2:
                        raise storage_module.TransactionMetricsError(
                            "fixture finish metric failure"
                        )

                with patch.dict(
                    os.environ,
                    {"DCAR_SQLITE_METRICS_FILE": str(metrics_root / "sqlite.jsonl")},
                ), patch.object(storage_module, "_emit_transaction_metric", side_effect=emit):
                    with transaction(connection):
                        connection.execute("INSERT INTO writes VALUES ('committed')")
                self.assertEqual(calls, 2)
                self.assertEqual(
                    connection.execute("SELECT value FROM writes").fetchone()[0],
                    "committed",
                )

    def test_read_only_connection_sees_uncheckpointed_wal(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "wal-visible.sqlite3"
            writer = sqlite3.connect(database)
            try:
                self.assertEqual(
                    writer.execute("PRAGMA journal_mode=WAL").fetchone()[0],
                    "wal",
                )
                writer.execute("CREATE TABLE visible(value TEXT NOT NULL)")
                writer.commit()
                writer.execute("INSERT INTO visible VALUES ('from-wal')")
                writer.commit()
                self.assertTrue(Path(f"{database}-wal").is_file())

                with live_wal_read_only_connections():
                    with connect(database, read_only=True) as reader:
                        self.assertEqual(reader.execute("PRAGMA query_only").fetchone()[0], 1)
                        self.assertEqual(
                            reader.execute("SELECT value FROM visible").fetchone()[0],
                            "from-wal",
                        )
                        with self.assertRaises(sqlite3.OperationalError):
                            reader.execute("INSERT INTO visible VALUES ('forbidden')")
            finally:
                writer.close()

    def test_configured_default_database_prefers_explicit_runtime_path(self) -> None:
        external = Path(
            "/Users/mark/Library/Application Support/"
            "DcarAIGC/data/dcar_insight.sqlite3"
        )
        with patch.dict(os.environ, {"DCAR_V8_DB": str(external)}):
            self.assertEqual(storage_module.configured_default_database(), external)
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(
                storage_module.configured_default_database(),
                storage_module.INSTALLED_DEFAULT_DB,
            )

    def test_installed_data_root_does_not_trust_home_environment(self) -> None:
        with patch.dict(os.environ, {"HOME": "/tmp/untrusted-dcar-home"}):
            self.assertEqual(
                storage_module.installed_data_root(),
                Path(storage_module.pwd.getpwuid(os.getuid()).pw_dir)
                / "Library"
                / "Application Support"
                / "DcarAIGC"
                / "data",
            )

    def test_runtime_legacy_defaults_use_installed_data_root(self) -> None:
        from v8 import api as api_module
        from v8 import migration as migration_module

        self.assertEqual(
            storage_module.INSTALLED_LEGACY_DB,
            storage_module.INSTALLED_DATA_ROOT / "web_mvp.sqlite3",
        )
        self.assertEqual(api_module.DEFAULT_LEGACY_DB, storage_module.INSTALLED_LEGACY_DB)
        self.assertEqual(migration_module.LEGACY_DB, storage_module.INSTALLED_LEGACY_DB)

    def test_write_transactions_wait_in_process_instead_of_raising_busy(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "serialized.sqlite3"
            with connect(database) as connection:
                connection.execute("CREATE TABLE writes(value TEXT NOT NULL)")
                connection.commit()

            first_started = threading.Event()
            second_attempted = threading.Event()
            release_first = threading.Event()

            def first_write() -> None:
                with connect(database) as connection, transaction(connection):
                    connection.execute("INSERT INTO writes VALUES ('first')")
                    first_started.set()
                    if not release_first.wait(5):
                        raise AssertionError("first transaction was not released")

            def second_write() -> None:
                if not first_started.wait(5):
                    raise AssertionError("first transaction did not start")
                with connect(database) as connection:
                    connection.execute("PRAGMA busy_timeout = 1")
                    second_attempted.set()
                    with transaction(connection):
                        connection.execute("INSERT INTO writes VALUES ('second')")

            with ThreadPoolExecutor(max_workers=2) as pool:
                first = pool.submit(first_write)
                second = pool.submit(second_write)
                self.assertTrue(second_attempted.wait(5))
                time.sleep(0.05)
                release_first.set()
                first.result(timeout=5)
                second.result(timeout=5)

            with connect(database) as connection:
                self.assertEqual(
                    [row["value"] for row in connection.execute(
                        "SELECT value FROM writes ORDER BY rowid"
                    )],
                    ["first", "second"],
                )

    def _make_v16_account_fixture(self, database: Path) -> None:
        from v8.operations import upsert_account, upsert_content

        with connect(database) as connection:
            initialize_historical_schema(connection, target_version=16)
        account = upsert_account(
            {
                "phone": "13800138000",
                "operator_name": "原运营人员",
                "platforms": [{"platform": "douyin", "uid": "v16-account"}],
            },
            db_path=database,
        )
        upsert_content(
            {
                "platform": "douyin",
                "canonical_url": "https://www.douyin.com/video/778899",
                "account_uid": "v16-account",
            },
            db_path=database,
        )
        with connect(database) as connection:
            connection.execute(
                "UPDATE sqlite_sequence SET seq=1000 WHERE name='accounts'"
            )
            identity = connection.execute(
                "SELECT id FROM account_platform_identities WHERE account_id=?",
                (account["id"],),
            ).fetchone()
            connection.execute(
                """INSERT INTO account_provider_references(
                    account_identity_id,provider,reference_kind,reference_value,created_at,updated_at
                ) VALUES (?,'TikHub','sec_user_id','MS4w.cached','2026-08-28','2026-08-28')""",
                (identity["id"],),
            )
            connection.commit()

    @patch.object(storage_module, "SCHEMA_VERSION", 17)
    def test_optional_phone_migration_preserves_accounts_children_and_sequence(self) -> None:
        # Seal the historical v17 uniqueness contract; v18's intentional
        # repeated-phone behavior has its own migration/constraint test.
        from v8.operations import upsert_account

        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "v16.sqlite3"
            self._make_v16_account_fixture(database)
            with connect(database) as connection:
                before = {
                    table: [tuple(row) for row in connection.execute("SELECT * FROM " + table)]
                    for table in ("accounts", "account_platform_identities", "account_provider_references", "content_items")
                }
                initialize_database(connection)
                for table, rows in before.items():
                    self.assertEqual(
                        [tuple(row) for row in connection.execute("SELECT * FROM " + table)], rows
                    )
                self.assertFalse(connection.execute("PRAGMA foreign_key_check").fetchall())
                self.assertTrue(schema_compatibility_state(connection)["compatible"])
                self.assertEqual(
                    connection.execute("PRAGMA foreign_key_list(account_platform_identities)").fetchone()["table"],
                    "accounts",
                )
                changes = connection.total_changes
                initialize_database(connection)
                self.assertEqual(connection.total_changes, changes)
            for uid in ("blank-phone-one", "blank-phone-two"):
                added = upsert_account(
                    {"platforms": [{"platform": "douyin", "uid": uid}]}, db_path=database
                )
                self.assertGreater(added["id"], 1000)
            with connect(database) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM accounts WHERE phone='' AND phone_normalized IS NULL"
                    ).fetchone()[0], 2
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "UPDATE accounts SET phone_normalized='13800138000' WHERE id=?",
                        (added["id"],),
                    )

    def test_optional_phone_migration_rolls_back_and_restores_foreign_keys(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "v16.sqlite3"
            self._make_v16_account_fixture(database)
            with connect(database) as connection:
                original_ddl = connection.execute(
                    "SELECT sql FROM sqlite_master WHERE name='accounts'"
                ).fetchone()[0]
                with patch.object(
                    storage_module, "_migration_checkpoint", side_effect=RuntimeError("injected")
                ):
                    with self.assertRaisesRegex(RuntimeError, "injected"):
                        initialize_database(connection)
                self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 16)
                self.assertEqual(connection.execute("PRAGMA foreign_keys").fetchone()[0], 1)
                self.assertEqual(connection.execute("PRAGMA legacy_alter_table").fetchone()[0], 0)
                self.assertEqual(
                    connection.execute("SELECT sql FROM sqlite_master WHERE name='accounts'").fetchone()[0],
                    original_ddl,
                )
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM content_items").fetchone()[0], 1)
                self.assertFalse(connection.execute("PRAGMA foreign_key_check").fetchall())
                self.assertNotIn("accounts_v16_old", storage_module._table_names(connection))
                initialize_database(connection)
                self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], SCHEMA_VERSION)

    def test_test_guard_rejects_formal_database(self) -> None:
        with patch.dict("os.environ", {"DCAR_TEST_DENY_FORMAL_DB": "1"}):
            with self.assertRaisesRegex(RuntimeError, "formal DCar database"):
                connect(DEFAULT_DB)

    def test_test_guard_rejects_existing_formal_alias_by_file_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            formal = root / "formal.sqlite3"
            alias = root / "apfs-firmlink-spelling.sqlite3"
            formal.write_bytes(b"formal-sentinel")
            alias.hardlink_to(formal)
            self.assertNotEqual(alias.resolve(), formal.resolve())
            self.assertTrue(same_database_path(alias, formal))
            with (
                patch.object(storage_module, "DEFAULT_DB", formal),
                patch.dict("os.environ", {"DCAR_TEST_DENY_FORMAL_DB": "1"}),
                self.assertRaisesRegex(RuntimeError, "formal DCar database"),
            ):
                connect(alias)

    def test_test_guard_recognizes_installed_formal_path_without_env(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            installed = root / "Application Support" / "DcarAIGC" / "data" / "dcar_insight.sqlite3"
            installed.parent.mkdir(parents=True)
            installed.write_bytes(b"formal-sentinel")
            retired_checkout = root / "checkout" / "app" / "data" / "dcar_insight.sqlite3"
            with (
                patch.object(storage_module, "DEFAULT_DB", retired_checkout),
                patch.object(storage_module, "INSTALLED_DEFAULT_DB", installed),
                patch.object(storage_module, "CHECKOUT_DEFAULT_DB", retired_checkout),
                patch.dict("os.environ", {"DCAR_TEST_DENY_FORMAL_DB": "1"}),
                self.assertRaisesRegex(RuntimeError, "formal DCar database"),
            ):
                connect(installed)

    def test_database_path_comparison_canonicalizes_missing_targets(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            formal = root / "formal.sqlite3"
            equivalent = root / "missing-parent" / ".." / formal.name
            self.assertFalse(formal.exists())
            self.assertFalse(equivalent.exists())
            self.assertTrue(same_database_path(equivalent, formal))

    def test_database_path_comparison_fails_closed_on_identity_probe_error(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            formal = root / "formal.sqlite3"
            alias = root / "alias.sqlite3"
            formal.write_bytes(b"formal-sentinel")
            alias.hardlink_to(formal)
            with patch.object(Path, "stat", side_effect=PermissionError("denied")):
                with self.assertRaisesRegex(PermissionError, "denied"):
                    same_database_path(alias, formal)

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

    def test_connect_context_closes_owned_connection(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "v8.sqlite3"
            with connect(database) as connection:
                connection.execute("CREATE TABLE close_probe(id INTEGER PRIMARY KEY)")

            with self.assertRaisesRegex(sqlite3.ProgrammingError, "closed"):
                connection.execute("SELECT 1")

            read_only = connect(database, read_only=True)
            with read_only:
                count = read_only.execute(
                    "SELECT COUNT(*) FROM close_probe"
                ).fetchone()[0]
                self.assertEqual(count, 0)
            with self.assertRaisesRegex(sqlite3.ProgrammingError, "closed"):
                read_only.execute("SELECT 1")

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
                # v16 起人工复核域已删除：四张表不得再出现
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
                    "account_roster_snapshots",
                    "account_roster_members",
                    "account_metric_observations",
                    "acquisition_profile_activations",
                    "activation_cancellations",
                    "account_state_events",
                    "scan_verification_receipts",
                    "profile_day_coverage_receipts",
                    "runtime_receipt_revocations",
                    "pipeline_paid_drain_events",
                    "paid_provider_dispatch_events",
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

    def test_runtime_schema_compatibility_accepts_complete_walk_down(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            connection = connect(Path(temporary) / "v8.sqlite3")
            try:
                initialize_database(connection)
                self.assertEqual(require_schema_compatibility(connection), SCHEMA_VERSION)
                self.assertTrue(schema_compatibility_state(connection)["compatible"])

                connection.execute("DROP TABLE scheduler_run_attempts")
                # 走回 v12 需要同时清掉 v13 之后的迁移清单行，否则
                # max_migration_version 与 user_version 不一致（fail-closed）。
                connection.execute("DELETE FROM schema_migrations WHERE version>=13")
                connection.execute("PRAGMA user_version=12")
                connection.commit()
                self.assertEqual(require_schema_compatibility(connection), 12)
                self.assertTrue(schema_compatibility_state(connection)["compatible"])

                connection.execute("DROP TABLE content_metric_observations")
                connection.execute("DROP INDEX idx_content_identities_content_primary")
                connection.execute("DELETE FROM schema_migrations WHERE version>=12")
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
