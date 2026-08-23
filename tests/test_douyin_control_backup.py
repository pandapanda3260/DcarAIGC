from __future__ import annotations

import hashlib
import importlib.util
import json
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT / "src" / "dcar_eval"))
HELPER_PATH = (
    REPOSITORY_ROOT
    / "deploy"
    / "server"
    / "libexec"
    / "dcar-douyin-vault-backup.py"
)

from dcar_douyin_control.store import VaultStore  # noqa: E402


def _load_helper():
    spec = importlib.util.spec_from_file_location("douyin_vault_backup", HELPER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load backup helper")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


backup_helper = _load_helper()


class DouyinControlBackupTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "vault.sqlite3"
        self.backup_dir = self.root / "backups"
        self.backup_dir.mkdir(mode=0o700)
        self.store = VaultStore(self.source)
        self.store.initialize()
        now = int(time.time())
        self.store.create_state(
            state_digest="d" * 64,
            bound_username="operator",
            session_binding="e" * 64,
            account_id=7,
            platform_uid="123456789",
            scopes=["user_info"],
            expires_at=now + 600,
            request_id="backup-canary",
            now=now,
        )
        self.store.begin_exchange(
            "d" * 64,
            "operator",
            "e" * 64,
            request_id="backup-canary",
            now=now + 1,
        )
        self.store.store_candidate(
            state_digest="d" * 64,
            ciphertext=b"encrypted-candidate-canary",
            open_id_fingerprint="f" * 64,
            confirmation_expires_at=now + 900,
            request_id="backup-canary",
            now=now + 2,
        )

    @staticmethod
    def _sha256(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def test_offline_backup_is_valid_atomic_and_source_is_unchanged(self) -> None:
        source_sha = self._sha256(self.source)
        source_mtime = self.source.stat().st_mtime_ns
        result = backup_helper.create_backup(self.source, self.backup_dir)
        self.assertEqual(result["status"], "created")
        target = Path(result["path"])
        manifest = Path(result["manifest"])
        self.assertTrue(target.is_file())
        self.assertTrue(manifest.is_file())
        self.assertEqual(target.stat().st_mode & 0o777, 0o600)
        self.assertEqual(manifest.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self._sha256(target), result["sha256"])
        self.assertEqual(self._sha256(self.source), source_sha)
        self.assertEqual(self.source.stat().st_mtime_ns, source_mtime)
        with sqlite3.connect(target) as connection:
            self.assertEqual(connection.execute("PRAGMA quick_check").fetchone()[0], "ok")
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 2)
            ciphertext = connection.execute(
                "SELECT candidate_ciphertext FROM oauth_states WHERE state_digest=?",
                ("d" * 64,),
            ).fetchone()[0]
        self.assertEqual(ciphertext, b"encrypted-candidate-canary")
        manifest_payload = json.loads(manifest.read_text(encoding="utf-8"))
        self.assertEqual(manifest_payload["schema"], "dcar-douyin-vault-backup-v2")
        self.assertEqual(manifest_payload["sha256"], result["sha256"])
        self.assertEqual(manifest_payload["counts"]["ciphertext_records"], 1)
        self.assertEqual(list(self.backup_dir.glob("*.partial")), [])
        self.assertFalse(Path(f"{target}-wal").exists())
        self.assertFalse(Path(f"{target}-shm").exists())

        unchanged = backup_helper.create_backup(self.source, self.backup_dir)
        self.assertEqual(unchanged["status"], "unchanged")
        self.assertEqual(len(list(self.backup_dir.glob("*.sqlite3"))), 1)

    def test_short_exclusive_lock_uses_sqlite_builtin_waiting(self) -> None:
        holder = sqlite3.connect(self.source, isolation_level=None)
        self.addCleanup(holder.close)
        holder.execute("BEGIN EXCLUSIVE")
        holder.execute(
            "UPDATE audit_events SET reason_code=reason_code WHERE id=-1"
        )
        outcome: list[object] = []

        def backup() -> None:
            try:
                outcome.append(
                    backup_helper.create_backup(self.source, self.backup_dir)
                )
            except BaseException as exc:  # pragma: no cover - asserted below
                outcome.append(exc)

        worker = threading.Thread(target=backup)
        worker.start()
        time.sleep(0.2)
        holder.rollback()
        worker.join(timeout=5)
        self.assertFalse(worker.is_alive())
        self.assertEqual(len(outcome), 1)
        self.assertIsInstance(outcome[0], dict)
        self.assertEqual(outcome[0]["status"], "created")

    def test_persistent_lock_can_be_killed_without_publishing_final(self) -> None:
        holder = sqlite3.connect(self.source, isolation_level=None)
        self.addCleanup(holder.close)
        holder.execute("BEGIN EXCLUSIVE")
        holder.execute(
            "UPDATE audit_events SET reason_code=reason_code WHERE id=-1"
        )
        process = subprocess.Popen(
            [
                sys.executable,
                str(HELPER_PATH),
                "--source",
                str(self.source),
                "--backup-dir",
                str(self.backup_dir),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            process.communicate(timeout=0.5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate(timeout=5)
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(list(self.backup_dir.glob("douyin-vault-*.sqlite3")), [])
        self.assertGreaterEqual(len(list(self.backup_dir.glob("*.partial"))), 1)

    def test_wal_and_hot_journal_sources_fail_closed(self) -> None:
        wal_source = self.root / "wal.sqlite3"
        connection = sqlite3.connect(wal_source)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("CREATE TABLE value(item TEXT)")
        connection.commit()
        connection.close()
        with self.assertRaisesRegex(RuntimeError, "journal mode"):
            backup_helper.create_backup(wal_source, self.backup_dir)

        hot_source = self.root / "hot.sqlite3"
        with sqlite3.connect(hot_source) as connection:
            connection.execute("PRAGMA journal_mode=DELETE")
            connection.execute("CREATE TABLE value(item TEXT)")
            connection.execute("INSERT INTO value VALUES('stable')")
            connection.commit()
        crash_script = (
            "import os,sqlite3,sys;"
            "c=sqlite3.connect(sys.argv[1],isolation_level=None);"
            "c.execute('BEGIN EXCLUSIVE');"
            "c.execute(\"UPDATE value SET item='dirty'\");"
            "os._exit(0)"
        )
        subprocess.run(
            [sys.executable, "-c", crash_script, str(hot_source)],
            check=True,
        )
        self.assertTrue(Path(f"{hot_source}-journal").exists())
        with self.assertRaises((RuntimeError, sqlite3.Error)):
            backup_helper.create_backup(hot_source, self.backup_dir)
        self.assertEqual(list(self.backup_dir.glob("douyin-vault-*.sqlite3")), [])

    def test_helper_is_stdlib_only_and_has_one_backup_call(self) -> None:
        source = HELPER_PATH.read_text(encoding="utf-8")
        self.assertNotIn("dcar_", "\n".join(
            line for line in source.splitlines() if line.lstrip().startswith(("import ", "from "))
        ))
        self.assertNotIn("cryptography", source)
        self.assertNotIn("credential", source.lower())
        self.assertEqual(source.count("source.backup("), 1)


if __name__ == "__main__":
    unittest.main()
