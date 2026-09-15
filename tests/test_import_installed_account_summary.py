"""Formal import boundary on disposable installed schema22 Writer fixtures.

The actual inheritance verifier, exact source manifests and actual file locks
run here. Fixture receipts describe local tests, never production authority.
"""
from __future__ import annotations

import base64
from dataclasses import replace
import importlib.util
import json
import os
from pathlib import Path
import plistlib
import sqlite3
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests import test_account_intake_release as release_fixtures
from tests.test_account_summary_import_cli import FIXTURE_XLSX
from v8 import account_intake_release as release, runtime_database

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("installed_account_summary_cli", ROOT / "scripts/import_installed_account_summary.py")
cli = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(cli)


class InstalledAccountSummaryImportTest(unittest.TestCase):
    def setUp(self):
        case = release_fixtures.AccountIntakeReleaseTest("test_original_receipt_provenance_and_no_write_inheritance")
        acquired = []
        acquire = runtime_database.acquire_writer_lock
        def remember(access):
            context = acquire(access); acquired.append(context); return context
        extra = {"scripts/import_installed_account_summary.py", "scripts/import_account_summary.py",
                 "src/dcar_eval/v8/account_directory_reconciliation.py"}
        with patch.object(runtime_database, "acquire_writer_lock", side_effect=remember), \
             patch.object(release, "REQUIRED_SOURCE", release.REQUIRED_SOURCE | extra):
            case.setUp()
        self.addCleanup(case.doCleanups)
        for context in reversed(acquired):
            context.__exit__(None, None, None)
        self.case = case; self.fixture = f = case.fixture
        self.root = f.root
        f.connection.commit()
        old = runtime_database.load_installed_writer_contract(required=True)
        environment = {**old.payload["EnvironmentVariables"], "DCAR_WRITER_SOURCE_ROOT": str(case.source),
            "DCAR_V8_DB": str(f.db), "DCAR_PROJECT_ROOT": str(f.project), "DCAR_WRITER_LOCK": str(old.writer_lock),
            "DCAR_LOADED_BUILD_RECEIPT": case.build_ref["path"], "DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT": str(f.install_path)}
        payload = {"Label": runtime_database.WRITER_LABEL, "WorkingDirectory": str(f.project),
            "ProgramArguments": [str(case.source / "deploy/macos/run_writer_worker.sh")], "EnvironmentVariables": environment}
        plist = f.home / "Library/LaunchAgents" / runtime_database.WRITER_PLIST_NAME
        plist.write_bytes(plistlib.dumps(payload)); plist.chmod(0o600)
        self.installed = replace(old, home=f.home, plist_path=plist,
            program=case.source / "deploy/macos/run_writer_worker.sh", payload=payload)
        self.enterContext(patch.object(runtime_database, "load_installed_writer_contract", return_value=self.installed))
        self.enterContext(patch.object(cli, "ROOT", case.source))
        self.enterContext(patch.dict(os.environ, environment))
        self.enterContext(patch("socket.socket.connect", side_effect=AssertionError("Network forbidden")))
        self.xlsx = self.root / "summary.xlsx"; self.xlsx.write_bytes(base64.b64decode(FIXTURE_XLSX))
        self.sequence = 0

    def args(self, *, apply=True):
        self.sequence += 1
        return SimpleNamespace(database=self.fixture.db, project_root=self.fixture.project, xlsx=self.xlsx, metadata=None,
            backup=self.root / f"import-before-{self.sequence}.sqlite3", report=self.root / f"import-report-{self.sequence}.json", apply=apply)

    def state(self):
        return cli._protected(self.fixture.connection)

    def test_real_schema22_import_preserves_inode_history_and_zero_write_replay(self):
        before = self.state(); identity = self.fixture.db.stat()
        args = self.args(); result = cli.run_import(args)
        self.assertEqual(result["status"], "applied")
        self.assertTrue(result["transaction_committed"])
        self.assertGreater(result["database_writes"], 0)
        self.assertEqual(result["total"], 2)
        self.assertEqual(result["authority"]["loaded_build"], release.reference(Path(self.case.build_ref["path"])))
        self.assertEqual(self.fixture.db.stat().st_ino, identity.st_ino)
        with sqlite3.connect(args.backup) as backup:
            backup.row_factory = sqlite3.Row
            self.assertEqual(cli._protected(backup), before)
        after = self.state()
        self.assertEqual(len(after["tables"]["accounts"]), len(before["tables"]["accounts"]))
        self.assertEqual(len(after["tables"]["account_intake_requests"]) - len(before["tables"]["account_intake_requests"]), 2)
        again = cli.run_import(self.args())
        self.assertEqual(again["database_writes"], 0)
        self.assertEqual(self.state(), after)
        self.assertEqual(args.report.stat().st_mode & 0o777, 0o600)
        self.assertEqual(args.backup.stat().st_mode & 0o777, 0o600)

    def test_dry_run_rolls_back_owned_rows_and_sequences_with_backup(self):
        before = self.state(); args = self.args(apply=False)
        result = cli.run_import(args)
        self.assertEqual(result["status"], "rolled_back")
        self.assertTrue(result["rollback_verified"])
        self.assertFalse(result["transaction_committed"])
        self.assertEqual(result["database_writes"], 0)
        self.assertGreater(result["writes"], 0)
        self.assertEqual(self.state(), before)
        self.assertTrue(args.backup.exists())

    def test_actual_held_writer_lock_prevents_backup_and_any_import(self):
        before = self.state(); args = self.args()
        with runtime_database.hold_formal_mutation(self.fixture.db, project_root=self.fixture.project):
            with self.assertRaisesRegex(runtime_database.RuntimeDatabaseError, "already held"):
                cli.run_import(args)
        self.assertEqual(self.state(), before)
        self.assertFalse(args.backup.exists()); self.assertFalse(args.report.exists())

    def test_wrong_source_and_edited_frozen_source_fail_before_backup(self):
        args = self.args()
        with patch.object(cli, "ROOT", ROOT), self.assertRaisesRegex(ValueError, "source"):
            cli.run_import(args)
        self.assertFalse(args.backup.exists())
        target = self.case.source / "scripts/import_account_summary.py"
        target.write_text(target.read_text() + "\n# unreviewed edit\n")
        with self.assertRaisesRegex(ValueError, "inventory"):
            cli.run_import(args)
        self.assertFalse(args.backup.exists())

    def test_same_path_existing_report_and_wrong_schema_are_rejected(self):
        args = self.args(); args.report = args.database
        with self.assertRaisesRegex(ValueError, "distinct"):
            cli.run_import(args)
        args = self.args(); args.report.write_text("keep existing")
        with self.assertRaisesRegex(ValueError, "refusing overwrite"):
            cli.run_import(args)
        self.assertEqual(args.report.read_text(), "keep existing")
        self.fixture.connection.execute("PRAGMA user_version=21"); self.fixture.connection.commit()
        with self.assertRaisesRegex(ValueError, "schema22"):
            cli.run_import(self.args())

    def test_failure_after_engine_writes_rolls_back_and_preserves_backup(self):
        before = self.state(); args = self.args(); engine = cli.import_account_summary
        def fail(connection, payload, *, imported_at):
            engine(connection, payload, imported_at=imported_at)
            raise RuntimeError("injected after engine writes")
        with patch.object(cli, "import_account_summary", side_effect=fail), self.assertRaisesRegex(RuntimeError, "injected"):
            cli.run_import(args)
        self.assertEqual(self.state(), before)
        self.assertTrue(args.backup.exists())

    def test_report_failure_before_commit_rolls_back_import(self):
        before = self.state(); args = self.args()
        with patch.object(cli, "_write_report", side_effect=OSError("injected report failure")), self.assertRaisesRegex(OSError, "report failure"):
            cli.run_import(args)
        self.assertEqual(self.state(), before)
        self.assertEqual(args.report.stat().st_mode & 0o777, 0o600)

    def test_postcommit_report_failure_is_distinguished_from_database_rollback(self):
        args = self.args(); writer = cli._write_report; calls = []
        def fail_final(descriptor, report):
            calls.append(report["status"])
            if len(calls) == 2:
                raise OSError("injected final report failure")
            writer(descriptor, report)
        with patch.object(cli, "_write_report", side_effect=fail_final), self.assertRaisesRegex(RuntimeError, "import committed"):
            cli.run_import(args)
        self.assertEqual(self.fixture.connection.execute("SELECT count(*) FROM account_intake_requests").fetchone()[0], 2)
        self.assertEqual(json.loads(args.report.read_text())["status"], "validated")
        self.assertEqual(cli.run_import(self.args())["database_writes"], 0)

    def test_unrelated_row_update_is_denied_even_when_counts_do_not_change(self):
        before = self.state(); args = self.args(); engine = cli.import_account_summary
        def tamper(connection, payload, *, imported_at):
            result = engine(connection, payload, imported_at=imported_at)
            connection.execute("UPDATE schema_migrations SET name='tampered' WHERE version=19")
            return result
        with patch.object(cli, "import_account_summary", side_effect=tamper), self.assertRaises(sqlite3.DatabaseError):
            cli.run_import(args)
        self.assertEqual(self.state(), before)

    def test_retargeting_existing_identity_is_rolled_back(self):
        before = self.state(); args = self.args(); engine = cli.import_account_summary
        def tamper(connection, payload, *, imported_at):
            result = engine(connection, payload, imported_at=imported_at)
            connection.execute("UPDATE account_platform_identities SET uid='87654321' WHERE id=?", (self.fixture.member["account_identity_id"],))
            return result
        with patch.object(cli, "import_account_summary", side_effect=tamper), self.assertRaisesRegex(ValueError, "identity"):
            cli.run_import(args)
        self.assertEqual(self.state(), before)


if __name__ == "__main__":
    unittest.main()
