"""In-place schema22 installation on disposable Writer fixtures only.

The fixtures own real temporary Git trees, SQLite files and process leases.
Their check logs and authority receipts explicitly describe test fixtures;
production authorization and provider calls are never manufactured here.
"""
from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
import plistlib
import shutil
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests import test_account_classification_install as fixtures
from v8 import account_intake_release as release, runtime_database, schema_v21, schema_v22

ROOT = Path(__file__).resolve().parents[1]


class AccountIntakeInstallTest(unittest.TestCase):
    def setUp(self):
        self.fixture = f = fixtures.AccountClassificationInstallTest(
            "test_install_preserves_inode_and_binds_external_receipt_to_database")
        f.setUp()
        self.addCleanup(f.doCleanups)
        installed21 = f.do_install()
        self.installer = fixtures.script("install_account_intake")
        # Finish installing the disposable schema21 predecessor before testing
        # the schema22 installer; the original schema21 verifier remains real.
        f.child["account_classification_successor"]["migration"] = installed21
        f.child["account_classification_successor"]["issued_at"] = self.installer._now()
        f.seal_child()
        environment = {**f.child_env, "DCAR_LOADED_BUILD_RECEIPT": f.child_ref["path"]}
        plist = {**f.parent_plist, "ProgramArguments": [str(f.child_source / "deploy/macos/run_writer_worker.sh")],
                 "EnvironmentVariables": environment}
        f.installed_plist.write_bytes(plistlib.dumps(plist))
        contract = replace(runtime_database.load_installed_writer_contract(required=True), payload=plist)
        self.enterContext(patch.object(runtime_database, "load_installed_writer_contract", return_value=contract))
        self.enterContext(patch.dict("os.environ", {**environment, "DCAR_LOADED_BUILD_ID": "sha256:"+f.child_ref["sha256"]}))
        self.source = f.root / "intake-source"
        shutil.copytree(f.child_source, self.source)
        for name in release.REQUIRED_SOURCE:
            (self.source / name).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, self.source / name)
        self.enterContext(patch.object(self.installer, "ROOT", self.source))
        tree = release.inventory(self.source)
        self.tree_ref = self.write("intake-tree.json", tree)
        changes = release.source_changes(release.object_at(f.tree_ref), tree)
        checks = {}
        for name in ("intake_schema", "intake_execution"):
            log = f.root / (name+".fixture.log")
            log.write_text("Disposable installation fixture, not a production test receipt.\n")
            log.chmod(0o600)
            checks[name] = self.write(name+".fixture.json", {"contract": release.CHECK_CONTRACT, "name": name,
                "status": "passed", "exit_code": 0, "source_tree": self.tree_ref, "changes": changes,
                "command": ["fixture-only"], "output": release.reference(log)})
        self.args = SimpleNamespace(database=f.db, project_root=f.project, installed_plist=f.installed_plist,
            parent_build=Path(f.child_ref["path"]), parent_install=f.install_path,
            source_tree=Path(self.tree_ref["path"]), output_dir=f.root / "intake-installation",
            check_report=[name+"="+ref["path"] for name,ref in checks.items()])
        self.before_schema = schema_v22._objects(f.connection)
        self.before_rows = schema_v22._table_digests(f.connection)

    def write(self, name, value):
        return self.installer.write(self.fixture.root / name, value)

    def test_install_preserves_inode_history_and_original_proof_without_paid_authority(self):
        f = self.fixture
        original = f.db.stat()
        old_proof = schema_v21.migration_proof(f.connection)
        result = self.installer.install(self.args)
        receipt = release.object_at(result)
        self.assertEqual(receipt["contract"], release.INSTALL_CONTRACT)
        self.assertEqual(receipt["migration_proof"], schema_v22.migration_proof(f.connection))
        self.assertEqual(receipt["inherited_classification_proof"], old_proof)
        self.assertEqual(receipt["authority_build"], f.child_ref)
        self.assertEqual(receipt["paid_gates_issued"], 0)
        self.assertEqual(receipt["receipt_sha256"], release.digest({k:v for k,v in receipt.items() if k!="receipt_sha256"}))
        self.assertEqual((f.db.stat().st_dev,f.db.stat().st_ino,f.db.stat().st_mode),
                         (original.st_dev,original.st_ino,original.st_mode))
        with self.installer.connect(self.args.output_dir / "before.sqlite3", read_only=True) as backup:
            self.assertTrue(schema_v22.validate_lineage(backup,f.connection)["retained_tables_verified"])
        writes = f.connection.total_changes
        self.assertEqual(self.installer.recover(self.args),result)
        self.assertEqual(f.connection.total_changes,writes)

    def test_committed_migration_recovers_missing_receipt_and_rejects_conflicting_receipt(self):
        original = self.installer.write
        def fail(path, value):
            if path.name == "migration-install.json":
                raise OSError("fixture receipt disk failure")
            return original(path,value)
        with patch.object(self.installer,"write",side_effect=fail), self.assertRaisesRegex(OSError,"disk failure"):
            self.installer.install(self.args)
        self.assertEqual(self.fixture.connection.execute("PRAGMA user_version").fetchone()[0],22)
        result = self.installer.recover(self.args)
        receipt = release.object_at(result)
        self.assertEqual(receipt["migration_proof"],schema_v22.migration_proof(self.fixture.connection))
        path = Path(result["path"])
        path.write_text(json.dumps({**receipt,"paid_gates_issued":1}))
        with self.assertRaisesRegex(ValueError,"Existing recovery receipt differs"):
            self.installer.recover(self.args)

    def test_rollback_preserves_inode_and_all_original_rows_and_is_repeatable(self):
        before = self.fixture.db.stat()
        self.installer.install(self.args)
        result = self.installer.recover(self.args,rollback=True)
        self.assertEqual(release.object_at(result)["schema_version"],21)
        self.assertEqual(schema_v22._objects(self.fixture.connection),self.before_schema)
        self.assertEqual(schema_v22._table_digests(self.fixture.connection),self.before_rows)
        self.assertEqual((self.fixture.db.stat().st_ino,self.fixture.db.stat().st_mode),(before.st_ino,before.st_mode))
        self.assertEqual(self.installer.recover(self.args,rollback=True),result)

    def test_rollback_never_discards_business_writes_after_migration(self):
        self.installer.install(self.args)
        changed = self.fixture.connection.execute("UPDATE accounts SET operator_name='fixture later business write' WHERE id=(SELECT min(id) FROM accounts)")
        self.assertEqual(changed.rowcount,1)
        self.fixture.connection.commit()
        after = schema_v22._table_digests(self.fixture.connection)
        with self.assertRaises(ValueError):
            self.installer.recover(self.args,rollback=True)
        self.assertEqual(schema_v22._table_digests(self.fixture.connection),after)
        self.assertEqual(self.fixture.connection.execute("PRAGMA user_version").fetchone()[0],22)

    def test_source_or_check_changes_are_rejected_before_any_database_write(self):
        path = self.source / "src/dcar_eval/v8/account_preparation.py"
        path.write_bytes(path.read_bytes()+b"\n# unreviewed fixture change\n")
        with self.assertRaises(ValueError):
            self.installer.install(self.args)
        self.assertFalse(self.args.output_dir.exists())
        self.assertEqual(schema_v22._objects(self.fixture.connection),self.before_schema)
        self.assertEqual(schema_v22._table_digests(self.fixture.connection),self.before_rows)

    def test_rollback_receipt_failure_recovers_after_restore(self):
        self.installer.install(self.args)
        original = self.installer.write_recovered
        def fail(path,value,**kwargs):
            if path.name == "rollback.json":
                raise OSError("fixture rollback receipt disk failure")
            return original(path,value,**kwargs)
        with patch.object(self.installer,"write_recovered",side_effect=fail), self.assertRaisesRegex(OSError,"receipt disk failure"):
            self.installer.recover(self.args,rollback=True)
        self.assertEqual(self.fixture.connection.execute("PRAGMA user_version").fetchone()[0],21)
        result = self.installer.recover(self.args,rollback=True)
        self.assertEqual(release.object_at(result)["status"],"restored")
        self.assertEqual(schema_v22._table_digests(self.fixture.connection),self.before_rows)

    def test_modified_backup_blocks_recovery_before_restore(self):
        self.installer.install(self.args)
        before = schema_v22._table_digests(self.fixture.connection)
        with (self.args.output_dir / "before.sqlite3").open("ab") as stream:
            stream.write(b"fixture backup mutation")
        with self.assertRaisesRegex(ValueError,"Recovery source, backup"):
            self.installer.recover(self.args,rollback=True)
        self.assertEqual(schema_v22._table_digests(self.fixture.connection),before)


if __name__ == "__main__":
    unittest.main()
