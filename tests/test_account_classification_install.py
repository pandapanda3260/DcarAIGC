"""Offline packaging/install recovery tests using temporary installed fixtures."""
from __future__ import annotations

import copy
from dataclasses import replace
import hashlib
import importlib.util
import json
from pathlib import Path
import plistlib
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests import test_account_classification_release as fixtures
from v8 import account_classification_release as release, runtime_database, schema_v21
from v8.account_directory import ensure_account_directory_schema

ROOT = Path(__file__).resolve().parents[1]


def script(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
    result = importlib.util.module_from_spec(spec)
    sys.modules[name] = result
    spec.loader.exec_module(result)
    return result


class AccountClassificationInstallTest(fixtures.AccountClassificationReleaseTest):
    def git_record(self, root):
        return {"mode": "working-tree-source-v1", **super().git_record(root)}

    def write(self, name, value):
        if name == "tree.json":
            ensure_account_directory_schema(self.connection)
            value = copy.deepcopy(value)
            relative = "src/dcar_eval/v8/runtime_paths.py"
            target = self.source / relative
            target.write_bytes((ROOT / relative).read_bytes())
            next(row for row in value["files"] if row["path"] == relative)["sha256"] = hashlib.sha256(target.read_bytes()).hexdigest()
        return super().write(name, value)

    def envelope(self, name, contract, payload):
        if name == "build.json":
            tree = json.loads((self.root / "tree.json").read_text())
            payload = {**payload, "critical_files": {row["path"]: row["sha256"] for row in tree["files"]}}
        return super().envelope(name, contract, payload)

    def setUp(self):
        # Release the real temporary Writer lease to represent a stopped
        # Writer, leaving installation to acquire its own real maintenance lease.
        acquire = runtime_database.acquire_writer_lock
        def remember(access):
            self.writer_context = acquire(access)
            return self.writer_context
        with patch.object(runtime_database, "acquire_writer_lock", side_effect=remember):
            super().setUp()
        self.writer_context.__exit__(None, None, None)
        self.connection.commit()
        self.make_child(migrate=False)
        self.connection.commit()
        self.enterContext(patch.object(release, "APPROVED_PREDECESSOR_BUILD_SHA256", frozenset({self.parent_ref["sha256"]})))
        self.package = script("prepare_account_classification_release")
        self.installer = script("install_account_classification")
        self.enterContext(patch.object(self.installer, "ROOT", self.child_source))
        self.installed_plist = self.home / "Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
        installed = runtime_database.load_installed_writer_contract(required=True)
        parent_env = {**self.child_env, "DCAR_LOADED_BUILD_RECEIPT": self.parent_ref["path"],
                      "DCAR_WRITER_SOURCE_ROOT": str(self.source), "DCAR_WRITER_LOCK": str(installed.writer_lock)}
        self.parent_plist = {"Label": "cn.tj.dcar.writer-worker", "WorkingDirectory": str(self.project),
            "ProgramArguments": [str(self.source / "deploy/macos/run_writer_worker.sh")], "EnvironmentVariables": parent_env}
        self.installed_plist.write_bytes(plistlib.dumps(self.parent_plist))
        self.installed_plist.chmod(0o600)
        contract = replace(installed, payload=self.parent_plist)
        self.enterContext(patch.object(runtime_database, "load_installed_writer_contract", return_value=contract))
        self.enterContext(patch.dict("os.environ", {**parent_env, "DCAR_LOADED_BUILD_ID": "sha256:" + self.parent_ref["sha256"]}))
        self.args = SimpleNamespace(database=self.db, project_root=self.project,
            installed_plist=self.installed_plist, parent_build=self.parent_path, parent_install=self.install_path,
            output_dir=self.root / "installation", check_report=[name + "=" + ref["path"] for name, ref in self.checks.items()])

    def do_install(self):
        return self.installer.install(self.args)

    def recovery_args(self):
        return SimpleNamespace(**vars(self.args), preflight=self.args.output_dir / "installation-preflight.json",
                               actor="offline fixture", reason="recover sealed classification migration")

    def test_install_preserves_inode_and_binds_external_receipt_to_database(self):
        inode = self.db.stat().st_ino
        before = self.installer.preserved_state(self.connection)
        result = self.do_install()
        receipt = release.object_at(result)
        self.assertEqual(self.db.stat().st_ino, inode)
        self.assertEqual(receipt["migration_proof"], schema_v21.migration_proof(self.connection))
        self.assertEqual(self.installer.preserved_state(self.connection), before)
        self.assertEqual(receipt["paid_gates_issued"], 0)
        self.assertTrue((self.args.output_dir / "before.sqlite3").is_file())

    def test_external_receipt_failure_recovers_committed_migration(self):
        original_write = self.installer.write
        def fail_receipt(path, value):
            if path.name == "migration-install.json":
                raise OSError("simulated receipt disk failure after commit")
            return original_write(path, value)
        with patch.object(self.installer, "write", side_effect=fail_receipt):
            with self.assertRaisesRegex(OSError, "after commit"):
                self.do_install()
        self.assertEqual(self.connection.execute("PRAGMA user_version").fetchone()[0], 21)
        proof = schema_v21.migration_proof(self.connection)
        result = self.installer.recover(self.recovery_args())
        self.assertEqual(release.object_at(result)["migration_proof"], proof)
        self.assertEqual(schema_v21.migration_proof(self.connection), proof)

    def test_truncated_external_receipt_recovers_with_original_bytes_retained(self):
        original_write = self.installer.write
        partial = b'{"contract":'
        def fail_receipt(path, value):
            if path.name == "migration-install.json":
                path.write_bytes(partial)
                path.chmod(0o600)
                raise OSError("simulated partial receipt write")
            return original_write(path, value)
        with patch.object(self.installer, "write", side_effect=fail_receipt):
            with self.assertRaisesRegex(OSError, "partial receipt"):
                self.do_install()
        result = self.installer.recover(self.recovery_args())
        self.assertEqual(release.object_at(result)["migration_proof"], schema_v21.migration_proof(self.connection))
        self.assertTrue(any(path.is_file() and path.read_bytes() == partial for path in self.args.output_dir.iterdir()))

    def test_rollback_receipt_failure_is_recoverable_after_database_restoration(self):
        self.do_install()
        original_write = self.installer.write
        def fail_receipt(path, value):
            if path.name == "rollback.json":
                raise OSError("simulated rollback receipt failure")
            return original_write(path, value)
        with patch.object(self.installer, "write", side_effect=fail_receipt):
            with self.assertRaisesRegex(OSError, "rollback receipt"):
                self.installer.recover(self.recovery_args(), rollback=True)
        self.assertEqual(self.connection.execute("PRAGMA user_version").fetchone()[0], 20)
        result = self.installer.recover(self.recovery_args(), rollback=True)
        self.assertEqual(release.object_at(result)["status"], "restored")

    def test_edit_since_source_review_is_rejected_before_database_migration(self):
        (self.child_source / "src/dcar_eval/v8/api.py").write_text("# unreviewed later edit\n")
        with self.assertRaises(ValueError):
            self.do_install()
        self.assertEqual(self.connection.execute("PRAGMA user_version").fetchone()[0], 20)

    def test_recovery_never_overwrites_a_valid_but_conflicting_receipt(self):
        result = self.do_install()
        target = Path(result["path"])
        value = release.object_at(result)
        value["paid_gates_issued"] = 1
        changed = json.dumps(value, sort_keys=True).encode()
        target.write_bytes(changed)
        with self.assertRaises(ValueError):
            self.installer.recover(self.recovery_args())
        self.assertEqual(target.read_bytes(), changed)
        self.assertEqual(self.connection.execute("PRAGMA user_version").fetchone()[0], 21)

    def test_recovery_rejects_modified_backup_or_installed_plist(self):
        self.do_install()
        (self.args.output_dir / "migration-install.json").unlink()
        self.installed_plist.write_bytes(self.installed_plist.read_bytes() + b"\n")
        with self.assertRaises(ValueError):
            self.installer.recover(self.recovery_args())
        self.installed_plist.write_bytes(plistlib.dumps(self.parent_plist))
        backup = self.args.output_dir / "before.sqlite3"
        with backup.open("ab") as stream:
            stream.write(b"tamper")
        with self.assertRaises(ValueError):
            self.installer.recover(self.recovery_args())

    def test_rollback_preserves_inode_and_refuses_to_overwrite_later_business_changes(self):
        self.do_install()
        inode = self.db.stat().st_ino
        account = self.connection.execute("SELECT id FROM accounts LIMIT 1").fetchone()[0]
        self.connection.execute("UPDATE accounts SET operator_name='later edit' WHERE id=?", (account,))
        self.connection.commit()
        with self.assertRaises(ValueError):
            self.installer.recover(self.recovery_args(), rollback=True)
        self.assertEqual(self.connection.execute("PRAGMA user_version").fetchone()[0], 21)
        self.assertEqual(self.db.stat().st_ino, inode)
        self.assertEqual(self.connection.execute("SELECT operator_name FROM accounts WHERE id=?", (account,)).fetchone()[0], "later edit")

    def test_unpinned_sibling_bootstrap_is_rejected_before_execution(self):
        malicious = self.root / "unreviewed-sibling"
        path = malicious / "src/dcar_eval/v8/runtime_paths.py"
        path.parent.mkdir(parents=True)
        marker = self.root / "untrusted-bootstrap-executed"
        body = "from pathlib import Path\ndef verify_source_before_import(**kwargs):\n    Path(" + repr(str(marker)) + ").write_text('executed')\n    return {}\n"
        path.write_text(body)
        loaded = {**self.parent, "source_root": str(malicious), "critical_files": {
            "src/dcar_eval/v8/runtime_paths.py": hashlib.sha256(path.read_bytes()).hexdigest()}}
        reference = self.envelope("unreviewed-sibling-build.json", "sealed-build-receipt-v1", loaded)
        value = copy.deepcopy(self.parent_plist)
        value["EnvironmentVariables"].update(DCAR_WRITER_SOURCE_ROOT=str(malicious), DCAR_LOADED_BUILD_RECEIPT=reference["path"])
        value["ProgramArguments"] = [str(malicious / "deploy/macos/run_writer_worker.sh")]
        unreviewed_plist = self.root / "unreviewed.plist"
        unreviewed_plist.write_bytes(plistlib.dumps(value))
        with self.assertRaises(ValueError):
            self.package.validate_installed_writer(release, unreviewed_plist, self.parent, self.install_path)
        self.assertFalse(marker.exists())

    def test_rollback_restores_exact_schema20_without_replacing_database_inode(self):
        before = self.installer.preserved_state(self.connection)
        self.do_install()
        inode = self.db.stat().st_ino
        result = self.installer.recover(self.recovery_args(), rollback=True)
        self.assertEqual(release.object_at(result)["status"], "restored")
        self.assertEqual(self.db.stat().st_ino, inode)
        self.assertEqual(self.connection.execute("PRAGMA user_version").fetchone()[0], 20)
        self.assertEqual(self.installer.preserved_state(self.connection), before)
        columns = {row[1] for row in self.connection.execute("PRAGMA table_info(accounts)")}
        self.assertTrue({"account_type", "content_direction"} <= columns)

    def test_publisher_proposal_uses_the_verified_sibling_source_as_its_previous_state(self):
        migrated = self.do_install()
        sibling_source = self.root / "quick-status-sibling"
        sibling_source.mkdir()
        sibling_build = self.envelope("quick-status-build.json", "sealed-build-receipt-v1",
            {**self.parent, "source_root": str(sibling_source)})
        sibling_plist = copy.deepcopy(self.parent_plist)
        sibling_plist["EnvironmentVariables"].update(DCAR_WRITER_SOURCE_ROOT=str(sibling_source),
            DCAR_LOADED_BUILD_RECEIPT=sibling_build["path"])
        sibling_plist["ProgramArguments"] = [str(sibling_source / "deploy/macos/run_writer_worker.sh")]
        sibling_bytes = plistlib.dumps(sibling_plist)
        self.installed_plist.write_bytes(sibling_bytes)
        receipt = release.object_at(migrated)
        receipt["previous_writer_plist_sha256"] = hashlib.sha256(sibling_bytes).hexdigest()
        receipt["previous_loaded_build"] = release.reference(Path(sibling_build["path"]))
        receipt["receipt_sha256"] = release.digest({key: value for key, value in receipt.items() if key != "receipt_sha256"})
        migration_ref = self.write("post-sibling-migration.json", receipt)
        publisher = {"Label": "cn.tj.dcar.snapshot-publisher", "EnvironmentVariables": {
            "DCAR_WRITER_SOURCE_ROOT": str(sibling_source), "DCAR_PROJECT_ROOT": str(self.project), "DCAR_V8_DB": str(self.db)},
            "ProgramArguments": [str(sibling_source / "deploy/macos/run_snapshot_publisher.sh")]}
        publisher_path = self.root / "publisher.plist"
        publisher_path.write_bytes(plistlib.dumps(publisher))
        args = SimpleNamespace(checkout=self.child_source, parent_build=self.parent_path, parent_install=self.install_path,
            source_root=self.root / "packaged-source", evidence_root=self.root / "packaged-evidence",
            installed_plist=self.installed_plist, migration=Path(migration_ref["path"]), publisher_plist=publisher_path,
            check_report=self.args.check_report, actor="offline fixture", reason="verified sibling packaging")
        # Predecessor trust is covered separately. This boundary fixture exercises
        # packaging after that verifier has accepted an exact quick-status pin.
        with patch.object(self.package, "validate_installed_writer", return_value=(sibling_bytes, sibling_plist)):
            result = self.package.prepare(args)
        proposed = plistlib.loads(Path(result["publisher"]["next_plist"]["path"]).read_bytes())
        self.assertEqual(proposed["EnvironmentVariables"]["DCAR_WRITER_SOURCE_ROOT"], str(args.source_root))
        self.assertEqual(Path(result["publisher"]["before_plist"]["path"]).read_bytes(), publisher_path.read_bytes())
        self.assertEqual(result["status"], "prepared")

    def test_prepare_exact_source_keeps_original_files_and_produces_bootstrapped_proposal(self):
        migrated = self.do_install()
        writer_before = self.installed_plist.read_bytes()
        args = SimpleNamespace(checkout=self.child_source, parent_build=self.parent_path, parent_install=self.install_path,
            source_root=self.root / "packaged-source", evidence_root=self.root / "packaged-evidence",
            installed_plist=self.installed_plist, migration=Path(migrated["path"]), publisher_plist=None,
            check_report=self.args.check_report, actor="offline fixture", reason="verified temporary packaging")
        result = self.package.prepare(args)
        self.assertEqual(result["status"], "prepared")
        self.assertFalse(result["services_changed"])
        self.assertEqual(self.installed_plist.read_bytes(), writer_before)
        self.assertEqual(result["bootstrap_verification"]["files"], len(self.tree["files"]))
        self.assertEqual(release.object_at(result["child_build"])["payload"]["schema_contract"], {"code_schema": 21, "formal_schema": 21})


def load_tests(loader, tests, pattern):
    # Reuse only fixture helpers; the base's release tests have their own run.
    return unittest.TestSuite(AccountClassificationInstallTest(name)
        for name in AccountClassificationInstallTest.__dict__ if name.startswith("test_"))
