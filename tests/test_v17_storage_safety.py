from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import v8.storage as storage
from tests.schema_fixture import initialize_historical_schema


class V17StorageSafetyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def fixture(self, path: Path, version: int) -> None:
        with sqlite3.connect(path) as connection:
            connection.row_factory = sqlite3.Row
            storage.configure_connection_safety(connection)
            if version == storage.SCHEMA_VERSION:
                storage.initialize_database(connection)
            else:
                initialize_historical_schema(connection, target_version=version)

    def test_missing_formal_database_does_not_create_parent_or_file(self) -> None:
        formal = self.root / "missing" / "formal.sqlite3"
        with patch.object(storage, "DEFAULT_DB", formal), patch.dict(
            "os.environ", {"DCAR_TEST_DENY_FORMAL_DB": "0"}
        ):
            with self.assertRaisesRegex(storage.SchemaMigrationError, "missing"):
                storage.connect(formal, read_only=False)
        self.assertFalse(formal.parent.exists())

    def test_wrong_version_formal_aliases_fail_before_wal(self) -> None:
        formal = self.root / "formal.sqlite3"
        self.fixture(formal, 16)
        symlink = self.root / "symbolic.sqlite3"
        symlink.symlink_to(formal)
        hardlink = self.root / "hard.sqlite3"
        hardlink.hardlink_to(formal)
        before = formal.read_bytes()
        with patch.object(storage, "DEFAULT_DB", formal), patch.dict(
            "os.environ", {"DCAR_TEST_DENY_FORMAL_DB": "0"}
        ):
            for path in (formal, symlink, hardlink):
                with self.subTest(path=path.name), self.assertRaises(
                    storage.SchemaMigrationError
                ):
                    storage.connect(path, read_only=False)
        self.assertEqual(formal.read_bytes(), before)
        self.assertFalse(list(self.root.glob("*-wal")))
        self.assertFalse(list(self.root.glob("*-shm")))

    def test_formal_current_connection_uses_existing_only_uri(self) -> None:
        formal = self.root / "formal.sqlite3"
        self.fixture(formal, storage.SCHEMA_VERSION)
        real_connect = sqlite3.connect
        with patch.object(storage, "DEFAULT_DB", formal), patch.dict(
            "os.environ", {"DCAR_TEST_DENY_FORMAL_DB": "0"}
        ), patch.object(storage.sqlite3, "connect", wraps=real_connect) as opened:
            with storage.connect(formal, read_only=False) as connection:
                self.assertEqual(storage.require_schema_compatibility(connection), storage.SCHEMA_VERSION)
            self.assertTrue(opened.call_args.kwargs["uri"])
            self.assertTrue(opened.call_args.args[0].endswith("?mode=rw"))

    def test_direct_connection_and_hardlink_cannot_initialize_formal(self) -> None:
        formal = self.root / "formal.sqlite3"
        with sqlite3.connect(formal) as connection:
            connection.row_factory = sqlite3.Row
            storage.configure_connection_safety(connection)
        alias = self.root / "alias.sqlite3"
        alias.hardlink_to(formal)
        for path in (formal, alias):
            with sqlite3.connect(path) as connection:
                connection.row_factory = sqlite3.Row
                storage.configure_connection_safety(connection)
                with patch.object(storage, "DEFAULT_DB", formal):
                    with self.assertRaisesRegex(storage.SchemaMigrationError, "forbidden"):
                        storage.initialize_database(connection)
                    with self.assertRaisesRegex(storage.SchemaMigrationError, "forbidden"):
                        storage.migrate_database(connection, from_version=16, to_version=17)
                self.assertFalse(storage._table_names(connection))

    def test_runtime_history_is_read_only_and_explicit_contract_is_required(self) -> None:
        database = self.root / "historical.sqlite3"
        self.fixture(database, 16)
        with sqlite3.connect(database) as connection:
            connection.row_factory = sqlite3.Row
            storage.configure_connection_safety(connection)
            before = connection.total_changes
            with self.assertRaisesRegex(storage.SchemaMigrationError, "explicit offline"):
                storage.initialize_database(connection, allow_migrations=False)
            with self.assertRaisesRegex(storage.SchemaMigrationError, "source version mismatch"):
                storage.migrate_database(connection, from_version=15, to_version=16)
            self.assertEqual(connection.total_changes, before)
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 16)
            storage.migrate_database(connection, from_version=16, to_version=17)
            self.assertEqual(storage.require_schema_compatibility(connection), 17)

    def test_frozen_16_to_17_contract_does_not_follow_future_current_schema(self) -> None:
        database = self.root / "frozen.sqlite3"
        with storage.connect(database) as connection:
            with patch.object(storage, "SCHEMA_SQL", "invalid future DDL"), patch.object(
                storage, "SCHEMA_VERSION", 18
            ), patch.object(storage, "CURRENT_SCHEMA_MIGRATION_NAME", "future-release"):
                initialize_historical_schema(connection, target_version=16)
                storage.migrate_database(connection, from_version=16, to_version=17)
                self.assertEqual(
                    storage.require_schema_compatibility(
                        connection, supported_versions=frozenset({17})
                    ), 17
                )
                self.assertEqual(
                    connection.execute("SELECT name FROM schema_migrations WHERE version=17")
                    .fetchone()[0], "optional-account-phone"
                )
                self.assertEqual(
                    next(row for row in connection.execute("PRAGMA table_info(accounts)")
                         if row["name"] == "phone_normalized")["notnull"], 0
                )

    def test_historical_alter_add_variants_migrate_and_constraints_remain_exact(self) -> None:
        database = self.root / "altered.sqlite3"
        with storage.connect(database) as connection:
            statements = storage._schema_statements(schema_version=16)
            for statement in statements:
                if statement.startswith("CREATE TABLE IF NOT EXISTS provider_raw_responses "):
                    statement = statement.replace(
                        "    account_id INTEGER REFERENCES accounts(id) ON DELETE CASCADE,\n", "", 1
                    )
                elif statement.startswith("CREATE TABLE IF NOT EXISTS selling_points "):
                    statement = statement.replace(
                        "    matcher_rule_json TEXT NOT NULL DEFAULT '{}',\n", "", 1
                    )
                connection.execute(statement)
            connection.execute(
                "ALTER TABLE provider_raw_responses ADD COLUMN "
                "account_id INTEGER REFERENCES accounts(id) ON DELETE CASCADE"
            )
            connection.execute(
                "ALTER TABLE selling_points ADD COLUMN matcher_rule_json TEXT NOT NULL DEFAULT '{}'"
            )
            connection.execute(
                "INSERT INTO schema_migrations VALUES (16,'remove-manual-review','2026-08-28')"
            )
            connection.execute("PRAGMA user_version=16")
            connection.commit()
            self.assertEqual(storage.require_schema_compatibility(connection), 16)
            storage.migrate_database(connection, from_version=16, to_version=17)
            self.assertEqual(storage.require_schema_compatibility(connection), 17)
            connection.execute("ALTER TABLE accounts ADD COLUMN unexpected TEXT")
            self.assertFalse(storage.schema_compatibility_state(connection)["compatible"])


if __name__ == "__main__":
    unittest.main()
