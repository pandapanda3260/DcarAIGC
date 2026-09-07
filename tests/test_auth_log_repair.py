from __future__ import annotations

import fcntl
import importlib.util
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src/dcar_eval"))
from dcar_auth.store import AuthStore, ChangeLogError  # noqa: E402


class AuthLogRepairTest(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location(
            "auth_log_repair", ROOT / "deploy/server/libexec/dcar-auth-log-repair.py"
        )
        self.helper = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.helper)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / "sessions.sqlite3"
        self.log = self.root / "auth-changes.log"
        self.backups = self.root / "backups"
        self.backups.mkdir()
        self.store = AuthStore(self.db, change_log_path=self.log)
        self.store.initialize()
        self.store.allow_phone("13800138000", actor="test")
        self.valid = self.log.read_bytes()
        self.db_bytes = self.db.read_bytes()

    def repair(self, **kwargs):
        return self.helper.repair_tail(
            self.db, self.log, self.backups, stopped_check=lambda: None, **kwargs
        )

    def test_quarantines_only_partial_tail_and_preserves_complete_evidence(self):
        damaged = self.valid + b'{"event":"intent","change_id":"'
        self.log.write_bytes(damaged)
        inode = self.log.stat().st_ino
        result = self.repair()
        self.assertEqual(result["status"], "repaired_reconciliation_required")
        self.assertEqual(self.log.read_bytes(), self.valid)
        self.assertEqual(self.log.stat().st_ino, inode)
        self.assertEqual(self.log.stat().st_mode & 0o777, 0o600)
        evidence = Path(result["evidence"])
        self.assertEqual((evidence / "original.log").read_bytes(), damaged)
        self.assertEqual((evidence / "original.log").stat().st_mode & 0o777, 0o600)
        self.assertTrue(json.loads((evidence / "prepared.json").read_text())["reconciliation_required"])
        self.assertEqual(self.db.read_bytes(), self.db_bytes)
        self.store.allow_phone("13800138001", actor="after-repair")

    def test_retains_complete_final_event_when_only_newline_is_missing(self):
        self.log.write_bytes(self.valid[:-1])
        result = self.repair()
        self.assertEqual(result["action"], "append_missing_newline")
        self.assertEqual(self.log.read_bytes(), self.valid)
        self.assertEqual(self.store.read_changes()[0]["state"], "committed")

    def test_unconfirmed_intent_survives_torn_commit_and_is_not_certified(self):
        intent = self.valid.splitlines(keepends=True)[0]
        self.log.write_bytes(intent + b'{"event":"commit"')
        self.repair()
        entries = self.store.read_changes()
        self.assertEqual(entries[0]["state"], "conservative")
        self.assertEqual(self.helper.main(["--db", str(self.db), "--change-log", str(self.log)]), 3)

    def test_refuses_complete_or_middle_corruption_without_changing_any_bytes(self):
        for damaged in (self.valid + b"broken\n", b"broken\n" + self.valid + b"partial", self.valid + b"{}"):
            with self.subTest(damaged=damaged[-12:]):
                self.log.write_bytes(damaged)
                with self.assertRaises(ChangeLogError):
                    self.repair()
                self.assertEqual(self.log.read_bytes(), damaged)
        self.assertEqual(list(self.backups.iterdir()), [])

    def test_inspection_never_modifies_files_and_valid_repair_is_noop(self):
        before = (self.log.read_bytes(), self.log.stat().st_mtime_ns)
        self.assertEqual(self.helper.inspect_log(self.db, self.log)["status"], "valid")
        self.assertFalse(self.repair()["changed"])
        self.assertEqual((self.log.read_bytes(), self.log.stat().st_mtime_ns), before)
        self.assertEqual(list(self.backups.iterdir()), [])

    def test_service_gate_and_recheck_prevent_mutation(self):
        damaged = self.valid + b"partial"
        self.log.write_bytes(damaged)
        for calls in ([self.helper.RepairError("active")], [None, self.helper.RepairError("active")]):
            with self.subTest(calls=len(calls)), patch.object(self.helper, "require_gateway_stopped", side_effect=calls) as check:
                with self.assertRaises(self.helper.RepairError):
                    self.helper.repair_tail(self.db, self.log, self.backups, stopped_check=check)
                self.assertEqual(self.log.read_bytes(), damaged)

    def test_database_and_log_locks_reject_concurrent_writers(self):
        self.log.write_bytes(self.valid + b"partial")
        with sqlite3.connect(self.db) as connection:
            connection.execute("BEGIN IMMEDIATE")
            with self.assertRaises(sqlite3.OperationalError):
                self.repair()
            connection.rollback()
        with self.log.open("rb") as locked:
            fcntl.flock(locked.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError):
                self.repair()
        self.assertEqual(list(self.backups.iterdir()), [])

    def test_evidence_write_or_disk_failure_leaves_original_untouched(self):
        damaged = self.valid + b"partial"
        self.log.write_bytes(damaged)
        with patch.object(self.helper, "write_new", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                self.repair()
        self.assertEqual(self.log.read_bytes(), damaged)
        usage = shutil.disk_usage(self.root)._replace(free=0)
        with patch.object(self.helper.shutil, "disk_usage", return_value=usage):
            with self.assertRaises(self.helper.RepairError):
                self.repair()
        self.assertEqual(self.log.read_bytes(), damaged)

    def test_rejects_symlink_hardlink_and_permissive_log(self):
        link = self.root / "linked.log"
        link.symlink_to(self.log)
        with self.assertRaises(self.helper.RepairError):
            self.helper.inspect_log(self.db, link)
        link.unlink()
        os.link(self.log, link)
        with self.assertRaises(self.helper.RepairError):
            self.repair()
        link.unlink()
        self.log.chmod(0o644)
        with self.assertRaises(self.helper.RepairError):
            self.repair()

    def test_cli_repair_always_requires_reconciliation_after_mutation(self):
        self.log.write_bytes(self.valid + b"partial")
        with patch.object(self.helper.subprocess, "run") as command:
            command.return_value.returncode = 0
            command.return_value.stdout = "inactive\n"
            result = self.helper.main([
                "--db", str(self.db), "--change-log", str(self.log),
                "--repair-tail", "--backup-dir", str(self.backups),
            ])
        self.assertEqual(result, 3)

    def test_noop_repair_does_not_certify_a_valid_but_unconfirmed_intent(self):
        self.log.write_bytes(self.valid.splitlines(keepends=True)[0])
        result = self.repair()
        self.assertFalse(result["changed"])
        self.assertEqual(result["unconfirmed"], 1)
        with patch.object(self.helper.subprocess, "run") as command:
            command.return_value.returncode = 0
            command.return_value.stdout = "inactive\n"
            self.assertEqual(self.helper.main([
                "--db", str(self.db), "--change-log", str(self.log),
                "--repair-tail", "--backup-dir", str(self.backups),
            ]), 3)


if __name__ == "__main__":
    unittest.main()
