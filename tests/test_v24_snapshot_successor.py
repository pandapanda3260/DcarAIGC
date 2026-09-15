"""Real migrated snapshots; receiver/publisher boundaries remain offline."""
from __future__ import annotations

import copy
import json
import shutil
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace

from tests import test_account_classification_publication as publication_fixture
from tests import test_account_classification_snapshot_deployment as deployment_fixture
from tests.test_macos_snapshot_publisher import FakeRunner
from tests.test_v24_duplicate_index import add_fingerprint
from tests import test_server_schema_upgrade as upgrade_fixture
from v8 import duplicate_index, schema_v22, schema_v23, schema_v24, snapshot_schema_successor as successor, storage

ROOT = Path(__file__).resolve().parents[1]
publisher = publication_fixture.publisher
installer, builder = deployment_fixture.installer, deployment_fixture.builder


def source_capsule(proof, chain):
    """Portable consumer fixture only; no claim of local installation verification."""
    version = 24 if "duplicate_index_migration" in chain else 23
    value = {"contract_version": successor.SOURCE_CONTRACT, "schema_version": version,
        "migration_receipt_sha256": chain["duplicate_index_migration" if version == 24 else "four_platform_flow_migration"]["receipt_sha256"],
        "deployment_receipt_sha256": proof["receipt_sha256"], "original_cleanup_build_sha256": proof["bindings"]["build_sha256"],
        **{name: {"path": "/offline-consumer-fixture/" + name, "sha256": "a" * 64, "byte_size": 100}
            for name in ("loaded_build", "original_install", "parent_build", "successor_install", "source_tree")},
        "inheritance_sha256": "b" * 64, "verification_scope": "frozen_original_parent_and_current_snapshot",
        "remote_installation": "not_verified", "paid_authority": False}
    return {**value, "proof_sha256": successor.digest(value)}


class SnapshotSuccessorPublicationTest(unittest.TestCase):
    def setUp(self):
        self.f = publication_fixture.AccountClassificationPublicationTest()
        self.f.setUp(); self.addCleanup(self.f.doCleanups)
        self.c = self.f.connection
        schema_v22.migrate(self.c)
        self.intake = publisher._account_intake_publication_evidence(self.c)
        self.classification = publisher._account_classification_publication_evidence(self.c)
        schema_v23.migrate(self.c)

    def to24(self, *, ready=True):
        schema_v24.migrate(self.c)
        generation = duplicate_index.create_generation(self.c, generation_id="snapshot-fixture")
        if ready:
            self.c.execute("UPDATE duplicate_index_generations SET state='ready'")
        self.c.commit()
        return generation["generation_id"]

    def test_real23_24_preserve_exact_ancestor_proofs_without_version_projection(self):
        legacy = successor.migration_chain(self.c)
        before = self.f.observe()
        publisher._validate_publication_evidence(before, expected_schema=23)
        self.assertEqual(before["account_intake"], self.intake)
        self.to24()
        with patch.object(schema_v23, "migration_proof", side_effect=AssertionError("never run schema23 verifier on schema24")):
            chain = successor.migration_chain(self.c)
            current = self.f.observe()
        self.assertEqual(chain["four_platform_flow_migration"], legacy["four_platform_flow_migration"])
        self.assertEqual(current["account_classification"], self.classification)
        self.assertEqual(current["account_intake"], self.intake)
        publisher._validate_publication_evidence(current, expected_schema=24)
        self.assertEqual(current["duplicate_index"]["posting_check"]["current_fingerprints"], 0)
        self.assertNotEqual(publisher._publication_fingerprint(before), publisher._publication_fingerprint(current))

    def test_code_successor_capsule_uses_current_source_but_original_migration(self):
        # Isolate portable capsule selection; the release suite separately checks
        # the complete authority and source verifier called by this boundary.
        self.to24()
        from v8 import duplicate_index_release, runtime_database, account_classification_release
        ref = lambda name: {"path": "/capsule-fixture/" + name, "sha256": "a" * 64, "byte_size": 100}
        parent_plan = {"parent_install": ref("install"), "parent_build": ref("parent"),
                       "migration": ref("migration"), "source_tree": ref("old-tree")}
        build = {"schema_contract": {"code_schema": 24, "formal_schema": 24},
                 "source_root": "/capsule-fixture/new-source", duplicate_index_release.FIELD: parent_plan,
                 duplicate_index_release.CODE_FIELD: {"source_tree": ref("new-tree")},
                 "account_cleanup_generation": {"source_tree": ref("new-tree")}}
        installed = SimpleNamespace(project_root=self.f.root, database=Path(self.c.execute("PRAGMA database_list").fetchone()[2]),
            payload={"EnvironmentVariables": {"DCAR_LOADED_BUILD_RECEIPT": ref("build")["path"],
                "DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT": ref("install")["path"]}})
        inherited = {"parent_build_ref": {"sha256": "b" * 64}}
        deployment = {"receipt_sha256": "c" * 64, "bindings": {"build_sha256": "b" * 64}}
        chain = {"duplicate_index_migration": {"receipt_sha256": "d" * 64}}
        with patch.object(runtime_database, "load_installed_writer_contract", return_value=installed), \
                patch.object(account_classification_release, "reference", side_effect=lambda path: ref(path.name)), \
                patch.object(account_classification_release, "payload_at", return_value=build), \
                patch.object(duplicate_index_release, "verify_inheritance", return_value=inherited) as verify:
            capsule = successor.installed_source_release(self.c, deployment, chain, project_root=self.f.root)
        self.assertEqual(capsule["source_tree"], ref("new-tree"))
        self.assertEqual(capsule["successor_install"], ref("migration"))
        self.assertEqual(capsule["parent_build"], ref("parent"))
        self.assertEqual(capsule["loaded_build"], ref("build"))
        verify.assert_called_once()
        successor.validate_source_release(capsule, deployment, chain)

    def test_unready24_and_corrupt_postings_block_publication(self):
        gid = self.to24(ready=False)
        with self.assertRaisesRegex(ValueError, "ready duplicate generation"):
            self.f.observe()
        self.c.execute("UPDATE duplicate_index_generations SET state='ready'")
        _, fid = add_fingerprint(self.c, frames=["f" * 16], generation_id=gid)
        self.c.commit()
        before = self.f.observe()
        self.c.execute("UPDATE duplicate_fingerprint_frames SET band2=0 WHERE fingerprint_id=?", (fid,))
        self.c.commit()
        with self.assertRaisesRegex(ValueError, "posting mismatch"):
            self.f.observe()
        missing = copy.deepcopy(before); missing.pop("duplicate_index")
        with self.assertRaises(publisher.SnapshotPublishError):
            publisher._validate_publication_evidence(missing, expected_schema=24)

    def test_receiver_recomputes_migration_and_every_snapshot_table_digest(self):
        self.to24()
        add_fingerprint(self.c, frames=["0" * 16, "f" * 16])
        self.c.commit()
        chain = successor.migration_chain(self.c)
        proof = {**chain, "schema_version": 24, "schema_migration": successor.DUPLICATE_SCHEMA_MIGRATION,
            "receipt_sha256": "c" * 64, "bindings": {"build_sha256": "d" * 64}}
        proof["snapshot_source_release"] = source_capsule(proof, chain)
        source = {"publication_evidence": self.f.observe(), "formal_database": {"access_mode": "formal_read", "sha256": "e" * 64}}
        successor.verify_publication(self.c, proof, source)
        installer._verify_schema22_publication(self.c, proof, source)
        for section in ("four_platform_flow", "duplicate_index"):
            for table in source["publication_evidence"][section]["tables"]:
                mutated = copy.deepcopy(source)
                mutated["publication_evidence"][section]["tables"][table]["sha256"] = "f" * 64
                with self.subTest(table=table), self.assertRaisesRegex(ValueError, "snapshot rows differ"):
                    successor.verify_publication(self.c, proof, mutated)
        self.c.execute("UPDATE content_link_intakes SET reason=reason")
        self.c.execute("UPDATE duplicate_dirty_work SET attempt_count=attempt_count+1")
        with self.assertRaisesRegex(ValueError, "snapshot rows differ"):
            successor.verify_publication(self.c, proof, source)

    def test_source_capsule_cannot_claim_remote_install_or_change_origin(self):
        chain = successor.migration_chain(self.c)
        proof = {"receipt_sha256": "c" * 64, "bindings": {"build_sha256": "d" * 64}}
        capsule = source_capsule(proof, chain)
        successor.validate_source_release(capsule, proof, chain)
        for key, value in (("remote_installation", "installed"), ("original_cleanup_build_sha256", "e" * 64), ("paid_authority", True)):
            changed = {**capsule, key: value}
            changed["proof_sha256"] = successor.digest({k: v for k, v in changed.items() if k != "proof_sha256"})
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "binding differs"):
                successor.validate_source_release(changed, proof, chain)


class SnapshotSuccessorBoundaryTest(unittest.TestCase):
    def setUp(self):
        self.f = deployment_fixture.ClassificationSnapshotDeploymentTest()
        self.f.setUp(); self.addCleanup(self.f.doCleanups)
        self.c = storage.connect(self.f.database); self.addCleanup(self.c.close)
        schema_v22.migrate(self.c); schema_v23.migrate(self.c)

    def test_explicit_builder_and_receiver_23_24_contracts_and_unproved_block(self):
        for version in (23, 24):
            if version == 24:
                schema_v24.migrate(self.c)
            self.assertEqual(builder._runtime_identity(self.f.database, expected_user_version=version)["database_schema_version"], version)
            installer._strict_schema(self.f.database, version)
            installer._verify_release_contract(ROOT, version)
            self.assertEqual(installer._transition_contract(version - 1, version)[0], f"dcar-schema{version-1}-to{version}-server-transition-v1")
            with self.assertRaisesRegex(builder.SnapshotBuildError, "inherited cleanup"):
                builder.build_snapshot(project_root=self.f.root, database=self.f.database, legacy_database=None,
                    output=self.f.root / ("bundle" + str(version)), expected_user_version=version)
            with self.assertRaisesRegex(publisher.SnapshotPublishError, "inherited cleanup"):
                publisher._schema20_deployment(self.c, project_root=self.f.root)
        with self.assertRaises(installer.SnapshotInstallError):
            installer._transition_contract(22, 24)

    def test_remote_receiver_must_have_completed_exact_pair_and_actual_support(self):
        for version in (23, 24):
            if version == 24:
                schema_v24.migrate(self.c)
            identity = builder._runtime_identity(self.f.database, expected_user_version=version)
            config = replace(self.f.config, expected_user_version=version)
            value = FakeRunner(identity).probe()
            value["health"]["database_state"]["user_version"] = version
            value["installer_schema_support"] = {"versions": list(installer.SUPPORTED_SCHEMA_VERSIONS),
                "transitions": [list(pair) for pair in installer.SUPPORTED_SCHEMA_TRANSITIONS]}
            value["schema_transition"] = {"schema": installer._transition_contract(version - 1, version)[0],
                "from_schema": version - 1, "to_schema": version, "status": "succeeded", "completed_at": "2026-09-15T00:00:00Z"}
            publisher._validate_remote_probe(value, config=config)
            for field in ("support", "transition", "health"):
                bad = copy.deepcopy(value)
                if field == "support": bad["installer_schema_support"]["versions"].remove(version)
                elif field == "transition": bad["schema_transition"]["status"] = "rolled_back"
                else: bad["health"]["database_state"]["user_version"] = version - 1
                with self.subTest(version=version, field=field), self.assertRaises(publisher.SnapshotPublishError):
                    publisher._validate_remote_probe(bad, config=config)

    def test_sealed_source_missing_portable_verifier_is_rejected(self):
        release = self.f.root / "release"
        for directory in ("src", "deploy", "scripts"):
            shutil.copytree(ROOT / directory, release / directory, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        target = release / "src/dcar_eval/v8/snapshot_schema_successor.py"
        target.write_text(target.read_text().replace("def verify_publication(", "def missing_verification("))
        with self.assertRaisesRegex(installer.SnapshotInstallError, "complete portable"):
            installer._verify_release_contract(release, 24)


class SnapshotSuccessorPairTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        cls.template = Path(cls.directory.name) / "source"
        upgrade_fixture._source_template(cls.template)

    @classmethod
    def tearDownClass(cls):
        cls.directory.cleanup()

    def prepare_pair(self, before):
        f = upgrade_fixture.ServerSchemaUpgradeTest()
        f.template = self.template
        f.setUp(); self.addCleanup(f.doCleanups)
        for name in ("src/dcar_eval/v8/storage.py", "src/dcar_eval/v8/contracts.py", "deploy/server/install_snapshot.py",
            "deploy/macos/publish_snapshot.py", "scripts/build_server_snapshot.py"):
            shutil.copy2(self.template / name, f.old / name)
        item = next(row for row in f.manifest["databases"] if row["name"] == "dcar_insight.sqlite3")
        incoming = f.bundle / item["bundle_path"]
        from v8 import schema_v20, schema_v21
        for database, initial, target in ((f.active_database, 18, before), (incoming, 19, before + 1)):
            with storage.connect(database) as c:
                if initial == 18:
                    storage.migrate_database(c, from_version=18, to_version=19)
                schema_v20.migrate(c, legacy_project_root=f.writer)
                schema_v21.migrate(c); schema_v22.migrate(c)
                if target >= 23: schema_v23.migrate(c)
                if target == 24: schema_v24.migrate(c)
                c.execute("PRAGMA wal_checkpoint(TRUNCATE)"); c.execute("PRAGMA journal_mode=DELETE")
        item.update(byte_size=incoming.stat().st_size, sha256=installer._sha256(incoming),
            **installer._validate_sqlite(incoming, expected_user_version=before + 1))
        f.manifest["runtime_identity"] = installer._database_runtime_identity(incoming, expected_schema=before + 1)
        (f.bundle / "manifest.json").write_text(json.dumps(f.manifest))
        (f.bundle / "manifest.sha256").write_text(installer._sha256(f.bundle / "manifest.json") + "  manifest.json\n")
        return f

    def test_explicit_successor_pairs_replay_only_the_exact_completed_transition(self):
        for before in (22, 23):
            with self.subTest(before=before):
                f = self.prepare_pair(before)
                prior = {"schema": installer._transition_contract(before - 1, before)[0], "from_schema": before - 1,
                    "to_schema": before, "status": "succeeded", "completed_at": "2026-09-15T00:00:00Z"}
                installer._write_json_atomic(f.config.transition_path, prior)
                # This engine fixture isolates deployment evidence; real source,
                # migration and DB-row proofs are independently tested above.
                with patch.object(installer, "_verify_schema20_deployment", return_value=None):
                    seal = installer.seal_schema_upgrade(f.bundle, f.config, release_dir=f.new,
                        expected_current_release=f.old, from_schema=before, to_schema=before + 1)
                    self.assertEqual(seal["transition_schema"], installer._transition_contract(before, before + 1)[0])
                    def smoke():
                        installer._strict_schema(f.active_database, before + 1)
                        return {"fixture_only": True, "database_user_version": before + 1}
                    kwargs = dict(release_dir=f.new, expected_current_release=f.old, from_schema=before, to_schema=before + 1,
                        service_action=f._service_action, reload_configuration=f._reload_configuration, smoke_check=smoke,
                        rollback_smoke_check=lambda: installer._strict_schema(f.active_database, before))
                    result = installer.schema_upgrade(f.bundle, f.config, **kwargs)
                    self.assertEqual(result["status"], "succeeded")
                    self.assertEqual(result["predecessor_transition"], prior)
                    self.assertEqual(installer.schema_upgrade(f.bundle, f.config, **kwargs), result)

    def test_failed_successor_pairs_restore_original_code_database_and_configuration(self):
        for before in (22, 23):
            with self.subTest(before=before):
                f = self.prepare_pair(before)
                baseline = f._protected_state()
                with patch.object(installer, "_verify_schema20_deployment", return_value=None):
                    installer.seal_schema_upgrade(f.bundle, f.config, release_dir=f.new,
                        expected_current_release=f.old, from_schema=before, to_schema=before + 1)
                    def failure(point):
                        if point == "smoke_verified": raise RuntimeError("injected smoke failure")
                    with self.assertRaisesRegex(installer.SnapshotInstallError, "sealed old.*restored"):
                        installer.schema_upgrade(f.bundle, f.config, release_dir=f.new, expected_current_release=f.old,
                            from_schema=before, to_schema=before + 1, service_action=f._service_action,
                            reload_configuration=f._reload_configuration, smoke_check=lambda: {"fixture_only": True},
                            rollback_smoke_check=lambda: installer._strict_schema(f.active_database, before), checkpoint_hook=failure)
                self.assertEqual(f._protected_state(), baseline)
                state = json.loads(f.config.transition_path.read_text())
                self.assertEqual((state["from_schema"], state["to_schema"], state["status"]), (before, before + 1, "rolled_back"))
