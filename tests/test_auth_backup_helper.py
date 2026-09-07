from __future__ import annotations

import importlib.util
import json
import os
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
HELPER_PATH = REPOSITORY_ROOT / "deploy" / "server" / "libexec" / "dcar-auth-backup.py"
sys.path.insert(0, str(REPOSITORY_ROOT / "src" / "dcar_eval"))

from dcar_auth import store as auth_store  # noqa: E402


def _load_helper():
    specification = importlib.util.spec_from_file_location(
        "dcar_auth_backup", HELPER_PATH
    )
    module = importlib.util.module_from_spec(specification)
    assert specification.loader is not None
    specification.loader.exec_module(module)
    return module


class AuthBackupHelperTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.helper = _load_helper()
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "sessions.sqlite3"
        self.backup_dir = self.root / "backups"
        self.backup_dir.mkdir()
        store = auth_store.AuthStore(self.source, pepper=b"p" * 32)
        store.initialize()
        store.allow_phone("13800138000", "fixture")
        connection = sqlite3.connect(self.source)
        with connection:
            connection.execute(
                "INSERT INTO auth_users(username, phone, password_hash, status, created_at, "
                "password_updated_at) VALUES('operator', '13800138000', '$6$x$y', 'active', 1, 1)"
            )
        connection.close()

    def test_backup_validates_dedupes_and_verifies(self) -> None:
        first = self.helper.create_backup(self.source, self.backup_dir, 3)
        self.assertEqual(first["status"], "created")
        self.assertEqual(
            first["counts"],
            {"user_version": 3, "users": 1, "sessions": 0, "allowed_phones": 1},
        )
        manifest = json.loads(Path(first["manifest"]).read_text(encoding="utf-8"))
        self.assertEqual(manifest["sha256"], first["sha256"])
        self.assertEqual(manifest["counts"]["users"], 1)
        second = self.helper.create_backup(self.source, self.backup_dir, 3)
        self.assertEqual(second["status"], "unchanged")
        self.assertEqual(len(list(self.backup_dir.glob("dcar-auth-*.sqlite3"))), 1)
        counts = self.helper.validate_database(Path(first["path"]), 3)
        self.assertEqual(counts["users"], 1)
        with self.assertRaisesRegex(RuntimeError, "schema version"):
            self.helper.validate_database(Path(first["path"]), 0)
        self.assertEqual(oct(Path(first["path"]).stat().st_mode & 0o777), oct(0o600))

    def test_orphan_manifest_cannot_shadow_a_new_backup(self) -> None:
        first = self.helper.create_backup(self.source, self.backup_dir, 3)
        Path(first["path"]).unlink()
        second = self.helper.create_backup(self.source, self.backup_dir, 3)
        self.assertEqual(second["status"], "created")
        self.assertTrue(Path(second["path"]).is_file())
        self.helper.verify_backup_pair(Path(second["path"]), 3)

    def test_pair_verification_rejects_database_manifest_and_count_tampering(
        self,
    ) -> None:
        created = self.helper.create_backup(self.source, self.backup_dir, 3)
        database = Path(created["path"])
        manifest = Path(created["manifest"])
        original_database = database.read_bytes()
        original_manifest = manifest.read_text(encoding="utf-8")

        database.write_bytes(original_database + b"x")
        with self.assertRaisesRegex(RuntimeError, "size|sha256"):
            self.helper.verify_backup_pair(database, 3)
        database.write_bytes(original_database)

        payload = json.loads(original_manifest)
        payload["sha256"] = "0" * 64
        manifest.write_text(json.dumps(payload), encoding="utf-8")
        with self.assertRaisesRegex(RuntimeError, "sha256"):
            self.helper.verify_backup_pair(database, 3)

        payload = json.loads(original_manifest)
        payload["counts"]["users"] += 1
        manifest.write_text(json.dumps(payload), encoding="utf-8")
        with self.assertRaisesRegex(RuntimeError, "counts"):
            self.helper.verify_backup_pair(database, 3)

        manifest.write_text(original_manifest, encoding="utf-8")
        self.helper.verify_backup_pair(database, 3)

    def test_legacy_session_only_database_needs_version_zero(self) -> None:
        legacy = self.root / "legacy.sqlite3"
        connection = sqlite3.connect(legacy)
        with connection:
            connection.execute(
                "CREATE TABLE auth_sessions(token_sha256 TEXT PRIMARY KEY, username TEXT NOT NULL, "
                "credential_fingerprint TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL, "
                "expires_at INTEGER NOT NULL)"
            )
        connection.close()
        with self.assertRaisesRegex(RuntimeError, "schema version"):
            self.helper.create_backup(legacy, self.backup_dir, 1)
        result = self.helper.create_backup(legacy, self.backup_dir, 0)
        self.assertEqual(result["status"], "created")
        self.assertEqual(result["counts"]["users"], 0)

    def test_wal_source_and_corrupt_backup_are_rejected(self) -> None:
        connection = sqlite3.connect(self.source)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("CREATE TABLE scratch(x)")
        connection.commit()
        with self.assertRaisesRegex(RuntimeError, "WAL|DELETE"):
            self.helper.create_backup(self.source, self.backup_dir, 3)
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.close()
        corrupt = self.root / "corrupt.sqlite3"
        corrupt.write_bytes(b"not a database")
        with self.assertRaises((RuntimeError, sqlite3.DatabaseError)):
            self.helper.validate_database(corrupt, 1)

    def test_prune_keeps_newest_pair(self) -> None:
        old_stamp = time.strftime(
            "%Y%m%dT%H%M%SZ", time.gmtime(time.time() - 40 * 86400)
        )
        with patch.object(self.helper.time, "strftime", return_value=old_stamp):
            stale_result = self.helper.create_backup(self.source, self.backup_dir, 3)
        stale = Path(stale_result["path"])
        orphan = self.backup_dir / "dcar-auth-20200101T000000Z-000000000000.sqlite3"
        orphan.write_bytes(b"orphan without manifest")
        removed = self.helper.prune_backups(self.backup_dir, 30)
        self.assertEqual(removed, [])
        self.assertTrue(stale.exists())
        auth_store.AuthStore(self.source, pepper=b"p" * 32).allow_phone(
            "13900139000", "new"
        )
        fresh = self.helper.create_backup(self.source, self.backup_dir, 3)
        removed = self.helper.prune_backups(self.backup_dir, 30)
        self.assertEqual(removed, [stale.name])
        self.assertFalse(stale.exists())
        self.assertTrue(Path(fresh["path"]).exists())
        self.assertTrue(orphan.exists())
        self.assertEqual(self.helper.prune_backups(self.backup_dir, 0), [])

    def test_prune_ignores_newer_corrupt_pair_and_keeps_latest_valid_pair(
        self,
    ) -> None:
        old_stamp = time.strftime(
            "%Y%m%dT%H%M%SZ", time.gmtime(time.time() - 40 * 86400)
        )
        with patch.object(self.helper.time, "strftime", return_value=old_stamp):
            valid = self.helper.create_backup(self.source, self.backup_dir, 3)
        corrupt = self.backup_dir / "dcar-auth-29990101T000000Z-000000000000.sqlite3"
        corrupt.write_bytes(b"not sqlite")
        corrupt.with_suffix(".manifest.json").write_text("{}\n", encoding="utf-8")

        self.assertEqual(self.helper.prune_backups(self.backup_dir, 30), [])
        self.assertTrue(Path(valid["path"]).exists())
        self.assertTrue(corrupt.exists())

    def test_cli_verify_mode(self) -> None:
        import subprocess

        created = self.helper.create_backup(self.source, self.backup_dir, 3)
        completed = subprocess.run(
            [
                sys.executable,
                str(HELPER_PATH),
                "--verify",
                created["path"],
                "--expect-user-version",
                "3",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(json.loads(completed.stdout)["status"], "valid")
        failed = subprocess.run(
            [
                sys.executable,
                str(HELPER_PATH),
                "--verify",
                created["path"],
                "--expect-user-version",
                "1",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(failed.returncode, 1)
        self.assertIn("schema version", failed.stderr)
        ran = subprocess.run(
            [
                sys.executable,
                str(HELPER_PATH),
                "--source",
                str(self.source),
                "--backup-dir",
                str(self.backup_dir),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(ran.returncode, 0, ran.stderr)
        self.assertEqual(json.loads(ran.stdout)["status"], "unchanged")
        self.assertTrue(os.access(HELPER_PATH, os.X_OK))

    def test_restore_preserves_corrupt_current_before_atomic_replacement(self) -> None:
        created = self.helper.create_backup(self.source, self.backup_dir, 3)
        self.source.write_bytes(b"corrupt current database to retain")
        result = self.helper.restore_backup_pair(
            Path(created["path"]), self.source, self.backup_dir, 3
        )
        preserved = Path(result["safety_directory"]) / self.source.name
        self.assertEqual(preserved.read_bytes(), b"corrupt current database to retain")
        self.helper.validate_database(self.source, 3)
        self.assertEqual(self.source.stat().st_mode & 0o777, 0o600)

    def test_corrupt_restore_source_never_changes_current_database(self) -> None:
        created = self.helper.create_backup(self.source, self.backup_dir, 3)
        before = self.source.read_bytes()
        database = Path(created["path"])
        with sqlite3.connect(database) as connection:
            connection.execute("UPDATE auth_users SET username='changed'")
        with self.assertRaisesRegex(RuntimeError, "sha256"):
            self.helper.restore_backup_pair(database, self.source, self.backup_dir, 3)
        self.assertEqual(self.source.read_bytes(), before)

    def test_return_to_old_contents_creates_retention_safe_new_backup(self) -> None:
        old_stamp = time.strftime(
            "%Y%m%dT%H%M%SZ", time.gmtime(time.time() - 40 * 86400)
        )
        with patch.object(self.helper.time, "strftime", return_value=old_stamp):
            old = self.helper.create_backup(self.source, self.backup_dir, 3)
        original = Path(old["path"]).read_bytes()
        auth_store.AuthStore(self.source, pepper=b"p" * 32).allow_phone(
            "13900139000", "new"
        )
        self.helper.create_backup(self.source, self.backup_dir, 3)
        self.source.write_bytes(original)
        fresh = self.helper.create_backup(self.source, self.backup_dir, 3)
        self.assertEqual(fresh["status"], "created")
        self.helper.prune_backups(self.backup_dir, 30)
        self.assertTrue(Path(fresh["path"]).is_file())
        self.helper.verify_backup_pair(Path(fresh["path"]), 3)


if __name__ == "__main__":
    unittest.main()
