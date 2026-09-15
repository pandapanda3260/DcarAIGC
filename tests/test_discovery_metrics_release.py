"""Temporary release fixtures; no installed service or database is modified."""
from __future__ import annotations

from copy import deepcopy
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "discovery_metrics_release_under_test", ROOT / "src/dcar_eval/v8/discovery_metrics_release.py"
)
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)
AT = "2026-09-12T03:00:00+00:00"


class DiscoveryMetricsReleaseTest(unittest.TestCase):
    def write(self, name, value):
        target = self.root / name
        target.write_text(json.dumps(value, sort_keys=True))
        target.chmod(0o600)
        return release.reference(target)

    def record(self, name, body):
        return {"path": name, "sha256": hashlib.sha256(body).hexdigest(),
                "byte_size": len(body), "mode": 0o644}

    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        self.source = self.root / "child"
        self.source.mkdir()
        parent_source = self.root / "parent"
        parent_source.mkdir()
        parent_tree = {"contract": "writer-source-tree-v1", "source_root": str(parent_source),
                       "git": {"head": "parent"},
                       "files": [self.record("src/dcar_eval/v8/api.py", b"parent") ]}
        child_tree = {"contract": "writer-source-tree-v1", "source_root": str(self.source),
                      "git": {"head": "child"},
                      "files": [self.record("src/dcar_eval/v8/api.py", b"child"),
                                self.record(release.MODULE, release._LOADED_SOURCE)]}
        self.enterContext(patch.object(release, "REVIEWED_CHANGES", {
            "src/dcar_eval/v8/api.py": {"before_sha256": hashlib.sha256(b"parent").hexdigest(),
                                       "after_sha256": hashlib.sha256(b"child").hexdigest()}}))
        self.original_tree = parent_tree
        self.child_tree = child_tree
        changes = release.source_changes(parent_tree, child_tree)
        self.parent = {"status": "succeeded", "created_at": "2026-09-11T00:00:00+00:00",
            "source_root": str(parent_source), "project_root": str(self.root), "git": parent_tree["git"],
            "schema_contract": {"code_schema": 21, "formal_schema": 21},
            "account_cleanup_generation": {"source_tree": self.write("parent-tree.json", parent_tree),
                                           "operator_authority": {"original": True}},
            "daily_pipeline_successor": {"contract": "daily-pipeline-code-successor-v1",
                                         "transition": "daily-metric-validity-20260911-v1"},
            "overview_successor": {"contract": "overview-code-successor-v1", "transition": release.PARENT_TRANSITION},
            "account_profile_successor": {"authorization": {"issued_at": "2020-01-01", "max_requests": 111}},
            "capture_controls": {"paid_dispatch_state": "original"}}
        self.parent_ref = self.write("parent-build.json", {"payload": self.parent})
        self.inherited = {}
        for name in release.INHERITED_PROOFS.values():
            proof = {"contract": "original-proof", "loaded_build": {"sha256": name},
                     "authorization": {"expires_at": "2000-01-01", "max_requests": 1}}
            proof["proof_sha256"] = release.digest(proof)
            self.inherited[name] = proof
        self.inherited["catalog_capture_policy"] = {"identity": "existing_verified"}
        self.inherited["catalog_capture_policy_sha256"] = release.digest(self.inherited["catalog_capture_policy"])
        self.original_parent_context = release.parent_context
        self.enterContext(patch.object(release, "parent_context", return_value=(self.parent, self.inherited)))
        tree_ref = self.write("child-tree.json", child_tree)
        checks = {}
        for name in release.REQUIRED_CHECKS:
            output = self.root / (name + ".log")
            output.write_bytes(b"fixture passed\n")
            output.chmod(0o600)
            checks[name] = self.write(name + ".json", {"contract": release.CHECK_CONTRACT,
                "name": name, "status": "passed", "exit_code": 0,
                "command": ["python", "-B", "-m", "unittest"], "changes": changes,
                "output": release.reference(output)})
        plan = {"contract": release.CONTRACT, "transition": release.TRANSITION,
            "parent_build": self.parent_ref, "source_tree": tree_ref, "changes": changes, "checks": checks,
            "actor": "test", "reason": "discovery_metrics", "issued_at": AT,
            "production_rollout": "approved_by_user", "transport_qualification": "not_verified",
            "schema_migration_repeated": False, "provider_qualification_repeated": False,
            "database_writes": 0, "paid_gates_reopened": False, "business_scope_change": "none",
            **{field: self.inherited[name]["proof_sha256"] for field, name in release.INHERITED_PROOFS.items()}}
        self.build = {**deepcopy(self.parent), "source_root": str(self.source), "git": child_tree["git"],
            "created_at": AT, "critical_files": {row["path"]: row["sha256"] for row in child_tree["files"]},
            "account_cleanup_generation": {**self.parent["account_cleanup_generation"], "source_tree": tree_ref},
            "discovery_metrics_successor": plan,
            "code_successor_plan": self.write("source-plan.json", {
                "contract": "account-cleanup-source-plan-v1", "transition": "account-cleanup-0907-v1",
                "project_root": str(self.root), "source_root": str(self.source),
                "git": child_tree["git"], "source_tree": tree_ref})}

    def verify(self, build=None):
        build = build or self.build
        ref = self.write("child-build.json", {"contract_version": "sealed-build-receipt-v1",
                                             "payload": build, "payload_sha256": release.digest(build)})
        return release.verify_inheritance(build=build, build_ref=ref,
            install_path=self.root / "install.json", database=self.root / "unopened.sqlite3",
            source=self.source, at=AT)

    def test_keeps_every_historical_proof_and_authorization_unchanged(self):
        before = deepcopy(self.inherited)
        value = self.verify()
        for key, proof in before.items():
            self.assertEqual(value[key], proof)
        self.assertEqual(self.inherited, before)
        self.assertEqual(value["discovery_metrics_proof"]["transition"], release.TRANSITION)
        self.assertFalse((self.root / "unopened.sqlite3").exists())

    def test_rejects_each_historical_proof_hash_change(self):
        for field in release.INHERITED_PROOFS:
            with self.subTest(field=field):
                build = deepcopy(self.build)
                build["discovery_metrics_successor"][field] = "f" * 64
                with self.assertRaises(ValueError):
                    self.verify(build)

    def test_rejects_changed_daily_parent_schema_controls_or_authority(self):
        cases = [
            ("daily_pipeline_successor", {"changed": True}),
            ("overview_successor", {"changed": True}),
            ("schema_contract", {"code_schema": 22, "formal_schema": 22}),
            ("capture_controls", {"paid_dispatch_state": "reopened"}),
            ("account_profile_successor", {"authorization": {"renewed": True}}),
        ]
        for field, value in cases:
            with self.subTest(field=field):
                build = deepcopy(self.build)
                build[field] = value
                with self.assertRaises(ValueError):
                    self.verify(build)
        self.build["account_cleanup_generation"]["operator_authority"] = {"renewed": True}
        with self.assertRaises(ValueError):
            self.verify()

    def test_rejects_database_migration_paid_or_new_authority_scope(self):
        for field, value in (("database_writes", 1), ("schema_migration_repeated", True),
                             ("provider_qualification_repeated", True), ("paid_gates_reopened", True),
                             ("authorization", {}), ("business_scope_change", "new")):
            with self.subTest(field=field):
                build = deepcopy(self.build)
                build["discovery_metrics_successor"][field] = value
                with self.assertRaises(ValueError):
                    self.verify(build)

    def test_rejects_missing_check_and_changed_output(self):
        build = deepcopy(self.build)
        build["discovery_metrics_successor"]["checks"].pop("discovery_metrics_behavior")
        with self.assertRaises(ValueError):
            self.verify(build)
        (self.root / "discovery_metrics_behavior.log").write_text("tampered output")
        with self.assertRaises(ValueError):
            self.verify()

    def test_rejects_manifest_delta_and_critical_inventory_drift(self):
        self.build["critical_files"]["src/dcar_eval/v8/api.py"] = "0" * 64
        with self.assertRaises(ValueError):
            self.verify()
        tree = deepcopy(self.child_tree)
        tree["files"].append(self.record("config/unreviewed.json", b"unreviewed"))
        with self.assertRaises(ValueError):
            release.source_changes(self.original_tree, tree)
        tree = deepcopy(self.child_tree)
        tree["files"][0]["mode"] = 0o600
        with self.assertRaises(ValueError):
            release.source_changes(self.original_tree, tree)

    def test_rejects_future_issuance_and_changed_transition(self):
        build = deepcopy(self.build)
        build["discovery_metrics_successor"]["transition"] = release.PARENT_TRANSITION
        with self.assertRaises(ValueError):
            self.verify(build)
        self.build["created_at"] = self.build["discovery_metrics_successor"]["issued_at"] = "2100-01-01T00:00:00Z"
        with self.assertRaises(ValueError):
            self.verify()

    def test_parent_pin_fails_before_reading_any_receipt(self):
        with patch.object(release, "payload_at", side_effect=AssertionError("must not read")):
            with self.assertRaises(ValueError):
                self.original_parent_context({"sha256": "0" * 64},
                    install_path=self.root / "none", database=self.root / "none")


if __name__ == "__main__":
    unittest.main()
