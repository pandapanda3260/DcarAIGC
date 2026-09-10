from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from v8 import account_cleanup_snapshot as snapshot
from v8.capture_authorizations import digest


class CleanupSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.db = sqlite3.connect(":memory:")
        self.addCleanup(self.db.close)
        self.db.executescript("""
          PRAGMA user_version=20;
          CREATE TABLE accounts(id INTEGER PRIMARY KEY);
          CREATE TABLE account_directory_rows(source_sha256,source_row,account_id,platform,uid,identity_status);
          CREATE TABLE content_items(id INTEGER PRIMARY KEY,account_id,platform,platform_content_id);
          CREATE TABLE account_platform_identities(id,account_id,platform,uid);
          CREATE TABLE account_roster_members(snapshot_id,account_identity_id);
          CREATE TABLE deployment_readiness_receipts(id INTEGER PRIMARY KEY,deployment_id,status,payload_json,recorded_at,receipt_sha256);
        """)
        self.attachment = "a" * 64
        self.db.executemany("INSERT INTO accounts VALUES (?)", [(i,) for i in range(1, 267)])
        self.db.executemany("INSERT INTO account_directory_rows VALUES (?,?,?,?,?,?)", [
            (self.attachment, i, i if i <= 266 else None, "douyin", str(i) if i <= 266 else "",
             "existing_verified" if i <= 168 else "uid_unverified" if i <= 266 else "identity_missing") for i in range(1,293)])
        self.db.executemany("INSERT INTO content_items VALUES (?,?,?,?)", [
            (i, 1 + (i % 168) if i <= 63403 else None, "douyin", str(i)) for i in range(1,63497)])
        members = [{"account_identity_id": i, "account_id": i, "platform": "douyin", "uid": str(i)} for i in range(1,105)]
        self.db.executemany("INSERT INTO account_platform_identities VALUES (?,?,?,?)", [tuple(row.values()) for row in members])
        self.db.executemany("INSERT INTO account_roster_members VALUES (1,?)", [(i,) for i in range(1,105)])
        self.db.commit()
        self.selection = digest(members)
        self.active = {"activation_id": 1, "profile_id": "integrated_route_v1", "activation_sha256": "b"*64,
                       "roster_snapshot_id": 1, "roster_members_sha256": "c"*64, "cancellation": None,
                       "metadata": {"account_cleanup": {"contract": "account-cleanup-generation-v1", "generation_id": "test-cleanup", "selection_sha256": self.selection}}}
        migration = {"contract": "account-cleanup-projection-v1", "status": "candidate_verified", "source_backup": {"sha256": "d"*64},
                     "source_attachment_sha256": self.attachment, "directory": {"matched_count":168,"created_count":98,"unresolved_count":26},
                     "selection": {"removed_account_ids": list(range(1000,1175)), "removed_content_ids": list(range(63500,81514))}}
        migration["receipt_sha256"] = digest(migration)
        source = {"contract":"account-cleanup-source-authority-v1", "source_database_sha256":"d"*64,"selection_sha256":self.selection}
        source["snapshot_sha256"] = digest(source)
        refs = {"cleanup_migration": self.write("migration", migration), "source_authority": self.write("source", source)}
        runtime = {"status":"succeeded","binding_stage":"prepared_installation"}
        refs["cleanup_runtime"] = self.write("runtime", self.envelope("runtime-root-binding-v1", runtime))
        build = {"status":"succeeded","runtime_root_receipt":refs["cleanup_runtime"], "account_cleanup_generation": {
            "generation_id":"test-cleanup","selection_sha256":self.selection,"source_database_sha256":"d"*64,
            "migration_receipt": refs["cleanup_migration"],"source_authority":refs["source_authority"],"config_sha256":"e"*64}}
        refs["cleanup_build"] = self.write("build", self.envelope("sealed-build-receipt-v1", build))
        self.active["build_receipt_sha256"] = refs["cleanup_build"]["sha256"]
        prepared = {"contract":"account-cleanup-runtime-preparation-v1","generation_id":"test-cleanup","source_sha256":"d"*64,
            "source_authority_sha256":refs["source_authority"]["sha256"],"build_receipt_sha256":refs["cleanup_build"]["sha256"],
            "runtime_root_receipt_sha256":refs["cleanup_runtime"]["sha256"],"active":{k:self.active[k] for k in snapshot.ACTIVE_KEYS},
            "selection_sha256":self.selection,"paid_gates_issued":False,"member_count":104,"release_control":{"config_sha256":"e"*64}}
        prepared["receipt_sha256"] = digest(prepared)
        refs["cleanup_prepared"] = self.write("prepared", prepared)
        self.refs = refs
        self.patcher = patch.object(snapshot, "activation_by_id", return_value=self.active)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)

    def write(self, name, value):
        path = self.root / (name + ".json")
        path.write_text(json.dumps(value))
        return snapshot.reference(path)

    @staticmethod
    def envelope(contract, payload):
        return {"contract_version": contract,"payload":payload,"payload_sha256":digest(payload)}

    def record(self):
        self.db.execute("BEGIN")
        proof = snapshot.record_candidate(self.db, evidence=self.refs, deployment_id="cleanup",recorded_at="2026-09-07T08:00:00Z")
        self.db.commit()
        return proof

    def test_readonly_portable_and_future_content(self):
        proof = self.record()
        self.assertTrue(proof["readonly_publish_eligible"])
        self.assertFalse(proof["deployment_eligible"])
        self.assertFalse(proof["coverage_complete"])
        self.assertEqual("candidate", proof["status"])
        self.db.execute("INSERT INTO content_items VALUES (90000,1,'douyin','new')")
        self.assertEqual(proof, snapshot.validate(self.db, verify_files=False))
        with self.assertRaisesRegex(ValueError,"acceptance"):
            snapshot.validate(self.db, require_accepted=True)
        self.db.execute("UPDATE content_items SET account_id=3 WHERE id=1")
        with self.assertRaisesRegex(ValueError,"ownership"):
            snapshot.validate(self.db, verify_files=False)

    def test_private_file_tamper_and_removed_subject(self):
        self.record()
        Path(self.refs["cleanup_prepared"]["path"]).write_text("{}")
        with self.assertRaisesRegex(ValueError,"changed"):
            snapshot.validate(self.db)
        self.db.execute("INSERT INTO accounts VALUES (1000)")
        with self.assertRaisesRegex(ValueError,"subject returned"):
            snapshot.validate(self.db, verify_files=False)

    def test_private_manifest_roles(self):
        proof = self.record()
        root = Path(__file__).resolve().parents[1]
        import sys
        sys.path.insert(0, str(root / "scripts"))
        self.addCleanup(lambda: sys.path.remove(str(root / "scripts")))
        import build_server_snapshot
        directory = build_server_snapshot._private_deployment_directory(proof, project_root=root)
        self.assertEqual(5, len(directory["references"]))
        spec = importlib.util.spec_from_file_location("cleanup_install_snapshot", root / "deploy/server/install_snapshot.py")
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        self.addCleanup(lambda: sys.modules.pop(spec.name, None))
        spec.loader.exec_module(module)
        manifest = {"deployment_readiness":proof,"private_deployment_references":directory,"writer_project_root":str(root)}
        # Use the genuine private-reference verifier with a bounded Writer root.
        with patch.object(module, "_writer_root", return_value=root):
            self.assertEqual(5, len(module._private_deployment_reference_index(manifest)))
        import v20_release_contract
        with patch("v8.capture_code_successor.deployment_context", side_effect=AssertionError("old chain must not run")):
            self.assertEqual(proof, v20_release_contract.validate_deployment_receipt(self.db,project_root=root))


if __name__ == "__main__":
    unittest.main()
