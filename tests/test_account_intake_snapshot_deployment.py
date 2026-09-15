"""Schema22 receiver evidence and paired recovery on disposable files only."""
from __future__ import annotations

import copy
import json
import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests import test_account_classification_snapshot_deployment as classification
from tests import test_server_schema_upgrade as upgrade
from v8 import account_cleanup_snapshot, account_intake_release, schema_v20, schema_v21, schema_v22, storage
from v8.capture_authorizations import digest

ROOT = Path(__file__).resolve().parents[1]
installer = classification.installer
publisher = classification.publisher


class IntakeSnapshotEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.fixture = f = classification.ClassificationSnapshotDeploymentTest()
        f.setUp(); self.addCleanup(f.doCleanups)
        self.db = storage.connect(f.database); self.addCleanup(self.db.close)
        schema_v22.migrate(self.db)
        self.proof = {"contract_version":account_cleanup_snapshot.CONTRACT,"deployment_id":"fixture", "schema_version":22,
            "schema_migration":"unified-account-intake-v1", "account_intake_migration":schema_v22.migration_proof(self.db),
            "account_classification_migration":account_intake_release.inherited_classification_proof(self.db),
            "bindings":{key:"fixture" for key in ("activation_id","profile_id","roster_snapshot_id","roster_members_sha256","activation_sha256")}}
        self.publication = {"account_classification":publisher._account_classification_publication_evidence(self.db),
            "account_intake":publisher._account_intake_publication_evidence(self.db), "verified_at":"2026-09-12T06:00:00Z"}
        self.source = {"contract_version":"snapshot-source-receipt-v1","snapshot_id":"20260912T060000Z-"+"a"*12,
            "publication_evidence":self.publication,"publication_evidence_sha256":digest(self.publication),
            "formal_database":{"access_mode":"formal_read","sha256":"d"*64},"snapshot_contract":{"fixture":True},
            "runtime_identity":installer._database_runtime_identity(f.database,expected_schema=22)}
        self.manifest = {"deployment_readiness":self.proof,"snapshot_id":self.source["snapshot_id"],
            "runtime_identity":self.source["runtime_identity"],"created_at":self.publication["verified_at"],
            "snapshot_contract":self.source["snapshot_contract"]}

    def write_source(self):
        path = self.fixture.root / "manifest.json"
        path.write_text(json.dumps(self.manifest))
        self.source["manifest_sha256"] = installer._sha256(path)
        self.source["publication_evidence_sha256"] = digest(self.source["publication_evidence"])
        self.source["payload_sha256"] = digest({k:v for k,v in self.source.items() if k!="payload_sha256"})
        (self.fixture.root / "snapshot-source-receipt.json").write_text(json.dumps(self.source))

    def verify_portable(self):
        # The unrelated 2026 cleanup roster is an explicit unit boundary.
        # Real schema22/21 proofs, source seals and activation seals are checked.
        with patch.object(account_cleanup_snapshot,"validate",return_value=self.proof), \
             patch("v8.profile_activations.activation_at",return_value=self.proof["bindings"]):
            installer._verify_schema20_deployment(self.fixture.database,self.manifest,bundle=self.fixture.root)

    def test_real22_schema_and_portable_publication_round_trip(self):
        installer._strict_schema(self.fixture.database,22)
        self.assertEqual(installer._transition_contract(21,22),
            ("dcar-schema21-to22-server-transition-v1","dcar-schema21-to22-release-seal-v1"))
        self.write_source(); self.verify_portable()
        self.assertTrue({(17,18),(18,19),(19,20),(20,21)} <= installer.SUPPORTED_SCHEMA_TRANSITIONS)
        with self.assertRaises(installer.SnapshotInstallError): installer._transition_contract(20,22)
        with self.assertRaises(installer.SnapshotInstallError): installer._strict_schema(self.fixture.database,21)

    def test_relabelled21_or_mutated_migration_is_rejected(self):
        with sqlite3.connect(self.fixture.parent) as connection:
            connection.execute("PRAGMA user_version=22")
            connection.execute("INSERT INTO schema_migrations VALUES (22,'unified-account-intake-v1','fixture')")
        with self.assertRaisesRegex(installer.SnapshotInstallError,"exact sealed schema"):
            installer._strict_schema(self.fixture.parent,22)
        self.db.execute("DROP TRIGGER trg_account_intake_migrations_update")
        with self.assertRaisesRegex(installer.SnapshotInstallError,"exact sealed schema"):
            installer._strict_schema(self.fixture.database,22)

    def test_all_intake_and_classification_fields_are_verified_from_snapshot(self):
        for key in self.publication["account_intake"]:
            source = copy.deepcopy(self.source)
            value = source["publication_evidence"]["account_intake"][key]
            source["publication_evidence"]["account_intake"][key] = value+1 if type(value) is int else "changed"
            with self.subTest(key=key), self.assertRaisesRegex(ValueError,"frozen snapshot rows"):
                installer._verify_schema22_publication(self.db,self.proof,source)
        source = copy.deepcopy(self.source)
        source["publication_evidence"]["account_classification"]["schema_version"] = 22
        with self.assertRaisesRegex(ValueError,"frozen snapshot rows"):
            installer._verify_schema22_publication(self.db,self.proof,source)
        source = copy.deepcopy(self.source)
        source["publication_evidence"]["account_intake"]["request_count"] = False
        with self.assertRaisesRegex(ValueError,"frozen snapshot rows"):
            installer._verify_schema22_publication(self.db,self.proof,source)

    def test_missing_source_inheritance_or_changed_requests_cannot_reach_install(self):
        with self.assertRaisesRegex(ValueError,"sealed snapshot source"):
            installer._verify_schema22_publication(self.db,self.proof,None)
        proof = {**self.proof,"account_classification_migration":{}}
        with self.assertRaisesRegex(ValueError,"inheritance proof"):
            installer._verify_schema22_publication(self.db,proof,self.source)
        self.db.execute("INSERT INTO account_intake_requests(request_key,input_sha256,preparation_key,platform,input_json,source_json,created_at,updated_at) VALUES ('fixture',?,'fixture','douyin','{}','{}','fixture','fixture')",("a"*64,))
        with self.assertRaisesRegex(ValueError,"frozen snapshot rows"):
            installer._verify_schema22_publication(self.db,self.proof,self.source)

    def test_original_source_and_snapshot_hashes_remain_required(self):
        self.write_source()
        self.source["formal_database"]["sha256"] = "changed"
        self.write_source()
        with self.assertRaisesRegex(installer.SnapshotInstallError,"portable deployment proof"):
            self.verify_portable()
        self.source["formal_database"]["sha256"] = "d"*64
        self.write_source()
        path = self.fixture.root / "snapshot-source-receipt.json"
        source = json.loads(path.read_text()); source["formal_database"]["sha256"] = "e"*64
        path.write_text(json.dumps(source))
        with self.assertRaisesRegex(installer.SnapshotInstallError,"activation proof"):
            self.verify_portable()
        self.write_source()
        (self.fixture.root / "manifest.json").write_text("{}")
        with self.assertRaisesRegex(installer.SnapshotInstallError,"portable deployment proof"):
            self.verify_portable()

    def test_release_must_contain_explicit22_contract_and_inheritance(self):
        installer._verify_release_contract(ROOT,22)
        installer._verify_release_contract(ROOT,21)
        with patch.object(installer,"_literal",side_effect=lambda path,name: 21 if name=="LATEST_SCHEMA_VERSION" else original(path,name)):
            with self.assertRaises(installer.SnapshotInstallError): installer._verify_release_contract(ROOT,22)


class IntakeSnapshotPairTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory()
        cls.template = Path(cls.temporary.name).resolve() / "source"
        upgrade._source_template(cls.template)

    @classmethod
    def tearDownClass(cls): cls.temporary.cleanup()

    def setUp(self):
        self.fixture = f = upgrade.ServerSchemaUpgradeTest()
        f.template = self.template
        f.setUp(); self.addCleanup(f.doCleanups)
        # Reuse only disposable server directories; real old/new DBs are
        # migrated to their declared versions before sealing the paired change.
        for name in ("src/dcar_eval/v8/storage.py","src/dcar_eval/v8/contracts.py","deploy/server/install_snapshot.py",
                     "deploy/macos/publish_snapshot.py","scripts/build_server_snapshot.py"):
            shutil.copy2(self.template / name,f.old / name)
        with storage.connect(f.active_database) as connection:
            storage.migrate_database(connection,from_version=18,to_version=19)
            schema_v20.migrate(connection,legacy_project_root=f.writer)
            schema_v21.migrate(connection)
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            connection.execute("PRAGMA journal_mode=DELETE")
        row = next(row for row in f.manifest["databases"] if row["name"]=="dcar_insight.sqlite3")
        database = f.bundle / row["bundle_path"]
        with storage.connect(database) as connection:
            schema_v20.migrate(connection,legacy_project_root=f.writer)
            schema_v21.migrate(connection)
            schema_v22.migrate(connection)
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            connection.execute("PRAGMA journal_mode=DELETE")
        row.update(byte_size=database.stat().st_size,sha256=installer._sha256(database),
                   **installer._validate_sqlite(database,expected_user_version=22))
        f.manifest["runtime_identity"] = installer._database_runtime_identity(database,expected_schema=22)
        (f.bundle / "manifest.json").write_text(json.dumps(f.manifest))
        (f.bundle / "manifest.sha256").write_text(installer._sha256(f.bundle / "manifest.json")+"  manifest.json\n")
        # Evidence acceptance is tested independently above. This fixture
        # exercises real atomic files/config/code pairing and power-loss paths.
        self.enterContext(patch.object(installer,"_verify_schema20_deployment",return_value=None))
        self.baseline = f._protected_state()

    def seal(self):
        f = self.fixture
        return installer.seal_schema_upgrade(f.bundle,f.config,release_dir=f.new,expected_current_release=f.old,
                                             from_schema=21,to_schema=22)

    def execute(self, **kwargs):
        f = self.fixture
        def old():
            self.assertEqual(f._protected_state(),self.baseline)
            installer._strict_schema(f.active_database,21)
        def new():
            self.assertEqual(f.config.current_release.resolve(),f.new)
            installer._strict_schema(f.active_database,22)
            self.assertEqual(installer._sha256(f.active_database),next(row["sha256"] for row in f.manifest["databases"] if row["name"]=="dcar_insight.sqlite3"))
            return {"fixture_only":True,"database_user_version":22}
        return installer.schema_upgrade(f.bundle,f.config,release_dir=f.new,expected_current_release=f.old,
            from_schema=21,to_schema=22,service_action=f._service_action,reload_configuration=f._reload_configuration,
            smoke_check=new,rollback_smoke_check=old,**kwargs)

    def test_real_pair_upgrade_replay_and_ordinary_rollback_fence(self):
        prior = {"schema":installer.CLASSIFICATION_SCHEMA_TRANSITION_CONTRACT,"from_schema":20,"to_schema":21,
                 "status":"succeeded","completed_at":"2026-09-12T01:00:00Z"}
        installer._write_json_atomic(self.fixture.config.transition_path,prior)
        seal = self.seal()
        self.assertEqual(seal["transition_schema"],installer.INTAKE_SCHEMA_TRANSITION_CONTRACT)
        self.assertEqual(self.fixture._protected_state(),self.baseline)
        result = self.execute()
        self.assertEqual(result["status"],"succeeded")
        self.assertEqual(result["predecessor_transition"],prior)
        self.assertEqual(self.execute(),result)
        with self.assertRaises(installer.SnapshotInstallError):
            installer.rollback_snapshot(self.fixture.config,expected_schema=21,service_action=lambda _:None,smoke_check=lambda:None)

    def test_failed22_install_restores_the_exact21_code_database_config_pair(self):
        self.seal()
        def fail(checkpoint):
            if checkpoint=="smoke_verified": raise RuntimeError("fixture new runtime failed")
        with self.assertRaisesRegex(installer.SnapshotInstallError,"sealed old.*restored"):
            self.execute(checkpoint_hook=fail)
        state = json.loads(self.fixture.config.transition_path.read_text())
        self.assertEqual((state["from_schema"],state["to_schema"],state["status"]),(21,22,"rolled_back"))
        self.assertEqual(self.fixture._protected_state(),self.baseline)


original = installer._literal
if __name__ == "__main__": unittest.main()
