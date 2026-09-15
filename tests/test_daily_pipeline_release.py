"""Offline daily successor tests with no production lineage dispatch.

Only the unchanged catalog's file/hash primitives are copied from the published
source. Every parent verifier is a temporary sentinel, and its dispatcher is
mocked before verification. The SQLite fixture, receipts and plist proposals
all live below TemporaryDirectory. Run with Python -B.
"""
from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import plistlib
import shutil
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
PUBLISHED = Path("/Users/mark/Library/Application Support/DcarAIGC/writer-sources/20260910-account-profile-recovery-v1")
MODULE = "src/dcar_eval/v8/daily_pipeline_release.py"
CLASSIFICATION = "src/dcar_eval/v8/account_classification_release.py"
CATALOG = "src/dcar_eval/v8/account_catalog_capture_release.py"
AUTHORITY = "src/dcar_eval/v8/account_profile_authority.py"
PARENT_MODULES = frozenset("src/dcar_eval/v8/" + name + ".py" for name in (
    "account_profile_recovery_release", "account_profile_release", "publisher_capacity_release",
    "account_classification_release", "publisher_snapshot_release", "control_simplification_release",
    "metric_gap_release", "account_catalog_capture_release", "manual_content_scope_release"))
AT = "2100-01-01T00:00:00+00:00"
PARENT_AT = "2000-01-01T00:00:00+00:00"


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class DailyPipelineReleaseTest(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        self.project = self.root / "data"
        self.project.mkdir()
        self.database = self.project / "fixture.sqlite3"
        with sqlite3.connect(self.database) as connection:
            connection.execute("CREATE TABLE fixture_guard (value TEXT NOT NULL)")
            connection.execute("INSERT INTO fixture_guard VALUES ('must remain unchanged')")
        self.install_path = self.write("historical-install.json", {"fixture_only": True})
        self.install_path = Path(self.install_path["path"])
        self.parent_source = self.root / "parent-source"
        for name in sorted(PARENT_MODULES | {MODULE, AUTHORITY, "src/dcar_eval/v8/media_worker.py"}):
            target = self.parent_source / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shared_path = ROOT / CATALOG if (ROOT / CATALOG).is_file() else PUBLISHED / CATALOG
            target.write_bytes(shared_path.read_bytes() if name == CATALOG else
                b"raise AssertionError('temporary parent verifier must never execute')\n")
            target.chmod(0o644)
        self.parent_tree = self.inventory(self.parent_source)
        parent_tree_ref = self.write("parent-tree.json", self.parent_tree)
        self.ancestor_refs = {name: self.write(name + "-ancestor.json", {"fixture_generation": name})
            for name in ("cleanup", "classification", "manual", "catalog", "metric", "controls",
                         "snapshot", "capacity", "profile", "recovery", "daily_v2")}
        self.profile_auth = self.write("original-profile-authorization.json", {
            "contract": "fixture-original-profile-authorization", "issued_at": PARENT_AT,
            "expires_at": "2000-01-02T00:00:00+00:00", "max_requests": 111})
        self.compensation_auth = self.write("original-compensation-authorization.json", {
            "contract": "fixture-original-compensation-authorization", "issued_at": PARENT_AT,
            "expires_at": "2000-01-02T00:00:00+00:00", "max_requests": 4})
        self.older_daily_proof = {"contract": "daily-pipeline-code-successor-v1",
            "transition": "daily-pipeline-20260911-v1", "loaded_build": self.ancestor_refs["daily_v2"],
            "parent_build": self.ancestor_refs["recovery"]}
        self.older_daily_proof["proof_sha256"] = digest(self.older_daily_proof)
        self.parent = {"status": "succeeded", "schema_contract": {"code_schema": 21, "formal_schema": 21},
            "created_at": PARENT_AT, "source_root": str(self.parent_source), "project_root": str(self.project),
            "git": self.parent_tree["git"], "critical_files": self.critical(self.parent_tree),
            "account_cleanup_generation": {"source_tree": parent_tree_ref, "selection_sha256": "1" * 64,
                "operator_authority": {"original": True, "paid_gate": "closed"}},
            "account_classification_successor": {"parent_build": self.ancestor_refs["cleanup"],
                "migration": {"sha256": "2" * 64}},
            "manual_content_scope_successor": {"parent_build": self.ancestor_refs["classification"]},
            "account_catalog_capture_successor": {"parent_build": self.ancestor_refs["manual"],
                "account_catalog_policy": {"fixture_unchanged": True}},
            "metric_gap_successor": {"parent_build": self.ancestor_refs["catalog"]},
            "control_simplification_successor": {"parent_build": self.ancestor_refs["metric"]},
            "publisher_snapshot_successor": {"parent_build": self.ancestor_refs["controls"]},
            "publisher_capacity_successor": {"parent_build": self.ancestor_refs["snapshot"]},
            "account_profile_successor": {"parent_build": self.ancestor_refs["capacity"],
                "authorization": self.profile_auth},
            "account_profile_recovery_successor": {"parent_build": self.ancestor_refs["profile"],
                "authorization": self.compensation_auth},
            "daily_pipeline_successor": {"contract": "daily-pipeline-code-successor-v1",
                "transition": "daily-pipeline-20260911-v1", "parent_build": self.ancestor_refs["daily_v2"],
                "inherited_previous_daily_pipeline_proof_sha256": self.older_daily_proof["proof_sha256"]}}
        self.parent_ref = self.write("parent-build.json", {"contract_version": "sealed-build-receipt-v1",
            "payload": self.parent, "payload_sha256": digest(self.parent)})
        self.source = self.root / "candidate-source"
        shutil.copytree(self.parent_source, self.source)
        reviewed = {}
        for name in ("src/dcar_eval/v8/media_worker.py",):
            target = self.source / name
            old = target.read_bytes()
            target.write_bytes(old + b"\n# synthetic reviewed daily pipeline change\n")
            reviewed[name] = {"before_sha256": hashlib.sha256(old).hexdigest(),
                "after_sha256": hashlib.sha256(target.read_bytes()).hexdigest()}
        module_path = self.source / MODULE
        module_path.write_bytes((ROOT / MODULE).read_bytes())
        module_path.chmod(0o644)
        self.module = load(module_path, "fixture_daily_pipeline_release")
        self.enterContext(patch.object(self.module, "PARENT_BUILD_SHA256", self.parent_ref["sha256"]))
        self.enterContext(patch.object(self.module, "PARENT_DAILY_MODULE_SHA256",
            hashlib.sha256((self.parent_source / MODULE).read_bytes()).hexdigest()))
        self.enterContext(patch.object(self.module, "RECOVERY_BUILD_SHA256", self.ancestor_refs["recovery"]["sha256"]))
        self.enterContext(patch.object(self.module, "PREVIOUS_BUILD_SHA256", self.ancestor_refs["daily_v2"]["sha256"]))
        self.enterContext(patch.object(self.module, "REVIEWED_CHANGES", reviewed))
        self.tree = self.inventory(self.source)
        self.changes = self.module.source_changes(self.parent_tree, self.tree)
        self.expected_loaded = {
            "proof": self.ancestor_refs["classification"],
            "manual_content_scope_proof": self.ancestor_refs["manual"],
            "catalog_capture_proof": self.ancestor_refs["catalog"],
            "metric_gap_proof": self.ancestor_refs["metric"],
            "control_simplification_proof": self.ancestor_refs["controls"],
            "publisher_snapshot_proof": self.ancestor_refs["snapshot"],
            "publisher_capacity_proof": self.ancestor_refs["capacity"],
            "account_profile_proof": self.ancestor_refs["profile"],
            "profile_operation_authority": self.ancestor_refs["profile"],
            "account_profile_recovery_proof": self.ancestor_refs["recovery"],
            "profile_compensation_authority": self.ancestor_refs["recovery"],
            "previous_daily_pipeline_proof": self.ancestor_refs["daily_v2"],
            "published_daily_pipeline_proof": self.parent_ref}
        predecessor = {"proof": "cleanup", "manual_content_scope_proof": "classification",
            "catalog_capture_proof": "manual", "metric_gap_proof": "catalog",
            "control_simplification_proof": "metric", "publisher_snapshot_proof": "controls",
            "publisher_capacity_proof": "snapshot", "account_profile_proof": "capacity",
            "profile_operation_authority": "capacity", "account_profile_recovery_proof": "profile",
            "profile_compensation_authority": "profile", "previous_daily_pipeline_proof": "recovery",
            "published_daily_pipeline_proof": "daily_v2"}
        self.inherited = {key: {"loaded_build": copy.deepcopy(ref),
            "parent_build": copy.deepcopy(self.ancestor_refs[predecessor[key]])}
            for key, ref in self.expected_loaded.items()}
        for name in ("previous_daily_pipeline_proof", "published_daily_pipeline_proof"):
            self.inherited[name].update(contract=self.module.CONTRACT, transition=self.module.PARENT_TRANSITION)
        self.inherited["profile_operation_authority"]["authorization"] = self.profile_auth
        self.inherited["account_profile_proof"]["authorization"] = self.profile_auth
        self.inherited["account_profile_recovery_proof"]["authorization"] = self.compensation_auth
        self.inherited["profile_compensation_authority"]["authorization"] = self.compensation_auth
        for key in self.expected_loaded:
            self.inherited[key]["proof_sha256"] = digest(self.inherited[key])
        compensation = self.inherited["profile_compensation_authority"]
        compensation["profile_authority_proof_sha256"] = self.inherited["profile_operation_authority"]["proof_sha256"]
        self.rehash(compensation)
        self.inherited.update({"catalog_capture_policy": copy.deepcopy(self.module._shared.ACCOUNT_CATALOG_POLICY),
            "catalog_capture_policy_sha256": digest(self.module._shared.ACCOUNT_CATALOG_POLICY),
            "parent_build": {"original_cleanup_payload": True},
            "parent_build_ref": self.ancestor_refs["cleanup"],
            "unrecognized_historical_field": {"preserve": [1, 2, 3]}})
        self.dispatch = Mock(side_effect=self.dispatch_parent)
        self.parent_loader = self.enterContext(patch.object(self.module, "_load", side_effect=self.parent_only))
        self.tree_ref = self.write("child-tree.json", self.tree)
        self.checks = {}
        for name in sorted(self.module.REQUIRED_CHECKS):
            log = self.root / (name + ".log")
            log.write_text("synthetic fixture only; not a production behavior result\n")
            log.chmod(0o600)
            self.checks[name] = self.write(name + ".json", {"contract": self.module.CHECK_CONTRACT,
                "name": name, "status": "passed", "exit_code": 0, "changes": self.changes,
                "command": ["synthetic-fixture-only"], "output": self.module.reference(log)})
        source_plan = self.write("source-plan.json", {"contract": "account-cleanup-source-plan-v1",
            "transition": "account-cleanup-0907-v1", "project_root": str(self.project),
            "source_root": str(self.source), "git": self.tree["git"], "source_tree": self.tree_ref})
        successor = {"contract": self.module.CONTRACT, "transition": self.module.TRANSITION,
            "parent_build": self.parent_ref, "source_tree": self.tree_ref, "changes": self.changes,
            "checks": self.checks, "actor": "offline fixture", "reason": "synthetic daily code-only review",
            "issued_at": AT, "production_rollout": "approved_by_user", "transport_qualification": "not_verified",
            "schema_migration_repeated": False, "provider_qualification_repeated": False,
            "database_writes": 0, "paid_gates_reopened": False, "business_scope_change": "none",
            **{field: self.inherited[key]["proof_sha256"] for field, key in self.module.INHERITED_PROOFS.items()}}
        self.build = {**copy.deepcopy(self.parent), "source_root": str(self.source), "git": self.tree["git"],
            "critical_files": self.critical(self.tree), "code_successor_plan": source_plan,
            "account_cleanup_generation": {**copy.deepcopy(self.parent["account_cleanup_generation"]),
                "source_tree": self.tree_ref}, "daily_pipeline_successor": successor,
            "created_at": AT, "validation_scope": "temporary daily pipeline fixture"}
        self.seal()
        self.writer_plist = self.root / "writer.plist"
        self.publisher_plist = self.root / "publisher.plist"
        common_env = {"DCAR_WRITER_SOURCE_ROOT": str(self.parent_source),
            "DCAR_PROJECT_ROOT": str(self.project), "DCAR_V8_DB": str(self.database)}
        self.writer = {"Label": "cn.tj.dcar.writer-worker", "WorkingDirectory": str(self.project),
            "ProgramArguments": [str(self.parent_source / "deploy/macos/run_writer_worker.sh")],
            "EnvironmentVariables": {**common_env, "DCAR_LOADED_BUILD_RECEIPT": self.parent_ref["path"]}}
        self.publisher = {"Label": "cn.tj.dcar.snapshot-publisher",
            "ProgramArguments": [str(self.parent_source / "deploy/macos/run_snapshot_publisher.sh")],
            "EnvironmentVariables": common_env}
        for path, value in ((self.writer_plist, self.writer), (self.publisher_plist, self.publisher)):
            path.write_bytes(plistlib.dumps(value))
            path.chmod(0o600)
        self.before = self.protected_snapshot()
        # A future accidental database access is a test failure, even for this
        # temporary DB. No production path exists in any verifier input.
        self.enterContext(patch.object(sqlite3, "connect", side_effect=AssertionError("verification must not open SQLite")))

    def dispatch_parent(self, **kwargs):
        result = copy.deepcopy(self.inherited)
        if "published_daily_pipeline_proof" in result:
            result["daily_pipeline_proof"] = result.pop("published_daily_pipeline_proof")
        return result

    def parent_only(self, path, name, body=None):
        self.assertEqual(path, self.parent_source / CLASSIFICATION)
        self.assertEqual(body, path.read_bytes())
        return SimpleNamespace(verify_inheritance=self.dispatch)

    def inventory(self, source):
        return {"contract": "writer-source-tree-v1", "source_root": str(source),
            "git": {"mode": "working-tree-source-v1", "head": "a" * 40, "tree": "b" * 40,
                "branch": "fixture-only", "status_porcelain_sha256": "c" * 64},
            "files": [{"path": path.relative_to(source).as_posix(),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "byte_size": path.stat().st_size, "mode": 0o644}
                for path in sorted(source.rglob("*.py"))]}

    def critical(self, tree):
        return {row["path"]: row["sha256"] for row in tree["files"]
            if row["path"].startswith(("src/", "config/")) and row["path"].endswith((".py", ".json"))}

    def write(self, name, value):
        path = self.root / name
        path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        path.chmod(0o600)
        return {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "byte_size": path.stat().st_size}

    def seal(self):
        self.ref = self.write("child-build.json", {"contract_version": "sealed-build-receipt-v1",
            "payload": self.build, "payload_sha256": digest(self.build)})

    def rehash(self, proof):
        proof["proof_sha256"] = digest({key: value for key, value in proof.items() if key != "proof_sha256"})

    def verify(self):
        return self.module.verify_inheritance(build=self.build, build_ref=self.ref,
            install_path=self.install_path, database=self.database, source=self.source, at=AT)

    def verify_parent(self):
        return self.module.parent_context(self.parent_ref, install_path=self.install_path,
            database=self.database, at=AT)

    def protected_snapshot(self):
        return {str(path): (path.read_bytes(), path.stat().st_ino) for path in (
            self.database, self.install_path, self.writer_plist, self.publisher_plist,
            Path(self.profile_auth["path"]), Path(self.compensation_auth["path"]))}

    def tearDown(self):
        if hasattr(self, "before"):
            self.assertEqual(self.protected_snapshot(), self.before)

    def test_retains_every_inherited_field_and_binds_the_new_proof(self):
        self.assertEqual(set(self.module.INHERITED_PROOFS.values()), set(self.expected_loaded))
        self.assertEqual(len(self.module.INHERITED_PROOFS), 13)
        result = self.verify()
        self.assertEqual({k: v for k, v in result.items() if k != "daily_pipeline_proof"}, self.inherited)
        proof = result["daily_pipeline_proof"]
        self.assertEqual(proof["loaded_build"], self.ref)
        self.assertEqual(proof["parent_build"], self.parent_ref)
        self.assertEqual(proof["source_tree"], self.tree_ref)
        self.assertEqual(proof["changes"], self.changes)
        self.assertEqual(proof["checks"], self.checks)
        self.assertEqual(proof["proof_sha256"], digest({k: v for k, v in proof.items() if k != "proof_sha256"}))
        for field, key in self.module.INHERITED_PROOFS.items():
            self.assertEqual(proof[field], self.inherited[key]["proof_sha256"])
        self.assertEqual(result["parent_build_ref"], self.ancestor_refs["cleanup"])
        self.build["validation_scope"] = "another valid temporary build envelope"
        self.seal()
        changed = self.verify()["daily_pipeline_proof"]
        self.assertNotEqual(changed["loaded_build"], proof["loaded_build"])
        self.assertNotEqual(changed["proof_sha256"], proof["proof_sha256"])
        self.assertEqual(changed["loaded_build"], self.ref)

    def test_every_parent_hash_is_checked_before_any_dispatch(self):
        self.assertEqual(set(self.module.PARENT_VERIFIER_MODULES), PARENT_MODULES)
        self.assertEqual(len(self.module.PARENT_VERIFIER_MODULES), 9)
        for name in sorted(PARENT_MODULES | {MODULE}):
            with self.subTest(module=name):
                path = self.parent_source / name
                original = path.read_bytes()
                path.write_bytes(original + b"\n# tampered parent\n")
                self.parent_loader.reset_mock()
                self.dispatch.reset_mock()
                try:
                    with self.assertRaisesRegex(ValueError, "parent verifier changed"):
                        self.verify()
                    self.parent_loader.assert_not_called()
                    self.dispatch.assert_not_called()
                finally:
                    path.write_bytes(original)

    def test_missing_or_rebound_ancestor_proof_is_rejected_at_every_generation(self):
        original = copy.deepcopy(self.inherited)
        for key in self.expected_loaded:
            for change in ("missing", "loaded_build", "proof_sha256"):
                with self.subTest(proof=key, change=change):
                    self.inherited = copy.deepcopy(original)
                    if change == "missing":
                        self.inherited.pop(key)
                    elif change == "loaded_build":
                        self.inherited[key][change] = self.ancestor_refs["cleanup"]
                        self.rehash(self.inherited[key])
                    else:
                        self.inherited[key][change] = "0" * 64
                    with self.assertRaises(ValueError):
                        self.verify_parent()
        self.inherited = original

    def test_original_authorization_bindings_cannot_be_replaced(self):
        original = copy.deepcopy(self.inherited)
        for key in ("profile_operation_authority", "profile_compensation_authority", "account_profile_recovery_proof"):
            with self.subTest(authority=key):
                self.inherited = copy.deepcopy(original)
                self.inherited[key]["authorization"] = self.ancestor_refs["cleanup"]
                self.rehash(self.inherited[key])
                with self.assertRaises(ValueError):
                    self.verify_parent()
        self.inherited = copy.deepcopy(original)
        self.inherited["profile_compensation_authority"]["profile_authority_proof_sha256"] = "0" * 64
        self.rehash(self.inherited["profile_compensation_authority"])
        with self.assertRaisesRegex(ValueError, "authorizations changed"):
            self.verify_parent()
        self.inherited = copy.deepcopy(original)
        self.inherited["parent_build_ref"] = self.ancestor_refs["profile"]
        with self.assertRaisesRegex(ValueError, "cleanup parent binding"):
            self.verify_parent()
        self.inherited = original

    def test_parent_pin_and_empty_freeze_fail_closed(self):
        for value in ("PENDING", "0" * 64):
            with self.subTest(parent=value), patch.object(self.module, "PARENT_BUILD_SHA256", value):
                with self.assertRaises(ValueError):
                    self.verify()
        with patch.object(self.module, "REVIEWED_CHANGES", {}):
            with self.assertRaisesRegex(ValueError, "PENDING|not frozen"):
                self.verify()

    def test_previous_daily_proof_retains_its_original_parent_and_contract(self):
        original = copy.deepcopy(self.inherited)
        for field, value in (("parent_build", self.ancestor_refs["profile"]),
                             ("contract", "unreviewed-contract"), ("transition", "unreviewed-transition")):
            with self.subTest(field=field):
                self.inherited = copy.deepcopy(original)
                self.inherited["previous_daily_pipeline_proof"][field] = value
                self.rehash(self.inherited["previous_daily_pipeline_proof"])
                with self.assertRaisesRegex(ValueError, "older daily pipeline proof"):
                    self.verify_parent()
        self.inherited = original

    def test_published_daily_proof_preserves_f585_and_its_7f83_parent(self):
        original = copy.deepcopy(self.inherited)
        for field, value in (("parent_build", self.ancestor_refs["recovery"]),
                             ("contract", "unreviewed-contract"), ("transition", self.module.TRANSITION)):
            with self.subTest(field=field):
                self.inherited = copy.deepcopy(original)
                self.inherited["published_daily_pipeline_proof"][field] = value
                self.rehash(self.inherited["published_daily_pipeline_proof"])
                with self.assertRaisesRegex(ValueError, "previous daily pipeline proof"):
                    self.verify_parent()
        self.inherited = original

    def test_changed_parent_daily_module_is_rejected_even_in_a_reviewed_delta(self):
        tree = copy.deepcopy(self.parent_tree)
        next(row for row in tree["files"] if row["path"] == MODULE)["sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "daily pipeline module differs"):
            self.module.source_changes(tree, self.tree)

    def test_child_cannot_change_any_historical_plan_or_operator_authority(self):
        original = copy.deepcopy(self.build)
        keys = list(self.module.PARENT_SUCCESSORS) + ["schema_contract", "account_cleanup_generation"]
        for key in keys:
            with self.subTest(field=key):
                self.build = copy.deepcopy(original)
                self.build[key]["unreviewed_authority"] = True
                self.seal()
                with self.assertRaises(ValueError):
                    self.verify()
        self.build = original

    def test_child_cannot_replace_ancestral_verifiers_or_original_authority_code(self):
        for name in sorted(PARENT_MODULES | {AUTHORITY}):
            with self.subTest(module=name):
                tree = copy.deepcopy(self.tree)
                next(row for row in tree["files"] if row["path"] == name)["sha256"] = "0" * 64
                before = next(row for row in self.parent_tree["files"] if row["path"] == name)["sha256"]
                reviewed = {**self.module.REVIEWED_CHANGES,
                    name: {"before_sha256": before, "after_sha256": "0" * 64}}
                with patch.object(self.module, "REVIEWED_CHANGES", reviewed):
                    message = "published profile authority" if name == AUTHORITY else "published inheritance verifiers"
                    with self.assertRaisesRegex(ValueError, message):
                        self.module.source_changes(self.parent_tree, tree)

    def test_unreviewed_delta_module_bytes_and_metadata_changes_are_rejected(self):
        for name, key, value in ((MODULE, "sha256", "0" * 64),
                ("src/dcar_eval/v8/media_worker.py", "sha256", "1" * 64),
                (CATALOG, "byte_size", 1), (CLASSIFICATION, "mode", 0o600)):
            with self.subTest(path=name, field=key):
                tree = copy.deepcopy(self.tree)
                next(row for row in tree["files"] if row["path"] == name)[key] = value
                with self.assertRaises(ValueError):
                    self.module.source_changes(self.parent_tree, tree)

    def test_every_inherited_proof_hash_must_match_the_verified_parent(self):
        original = copy.deepcopy(self.build)
        for field in self.module.INHERITED_PROOFS:
            with self.subTest(field=field):
                self.build = copy.deepcopy(original)
                self.build["daily_pipeline_successor"][field] = "0" * 64
                self.seal()
                with self.assertRaises(ValueError):
                    self.verify()
        self.build = original

    def test_code_only_release_cannot_reopen_gates_or_change_business_scope(self):
        original = copy.deepcopy(self.build)
        for key, value in (("paid_gates_reopened", True), ("database_writes", 1),
                ("schema_migration_repeated", True), ("provider_qualification_repeated", True),
                ("business_scope_change", "expanded"), ("transport_qualification", "qualified"),
                ("production_rollout", "unapproved"), ("authorization", self.compensation_auth)):
            with self.subTest(field=key):
                self.build = copy.deepcopy(original)
                self.build["daily_pipeline_successor"][key] = value
                self.seal()
                with self.assertRaises(ValueError):
                    self.verify()
        self.build = original

    def test_exact_check_set_and_bound_report_contents_are_required(self):
        original = copy.deepcopy(self.build)
        name = sorted(self.checks)[0]
        report = self.module.object_at(self.checks[name])
        for field, value in (("contract", "unrelated"), ("name", "unrelated"), ("status", "failed"),
                ("exit_code", 1), ("changes", {}), ("command", [])):
            with self.subTest(report_field=field):
                self.build = copy.deepcopy(original)
                self.build["daily_pipeline_successor"]["checks"][name] = self.write("changed-check.json", {**report, field: value})
                self.seal()
                with self.assertRaises(ValueError):
                    self.verify()
        for checks in ({}, {**self.checks, "unexpected": self.checks[name]}):
            self.build = copy.deepcopy(original)
            self.build["daily_pipeline_successor"]["checks"] = checks
            self.seal()
            with self.assertRaises(ValueError):
                self.verify()
        self.build = original
        self.seal()
        report = self.module.object_at(next(iter(self.checks.values())))
        Path(report["output"]["path"]).write_text("tampered fixture output\n")
        with self.assertRaisesRegex(ValueError, "check output changed"):
            self.verify()

    def test_plan_change_list_manifest_and_critical_inventory_are_bound(self):
        original = copy.deepcopy(self.build)
        for field in ("changes", "source_root", "critical_files", "code_successor_plan"):
            with self.subTest(field=field):
                self.build = copy.deepcopy(original)
                if field == "changes":
                    self.build["daily_pipeline_successor"]["changes"] = {}
                elif field == "source_root":
                    self.build[field] = str(self.parent_source)
                elif field == "critical_files":
                    self.build[field] = {}
                else:
                    self.build[field] = self.write("bad-source-plan.json", {"unrelated": True})
                self.seal()
                with self.assertRaises(ValueError):
                    self.verify()
        self.build = original

    def packaging_fixture(self):
        package = load(ROOT / "scripts/prepare_daily_pipeline_release.py", "fixture_daily_packaging")
        def write_path(path, value):
            return self.write(path.relative_to(self.root), value)
        def write_bytes(path, body):
            path.write_bytes(body)
            path.chmod(0o600)
            return self.module.reference(path)
        def bootstrap(**kwargs):
            # This packaging test mocks only the already-covered generic
            # bootstrap boundary; it still hashes the actual copied files.
            child = self.module.payload_at(self.module.reference(kwargs["build_receipt"]))
            tree = self.module.object_at(child["account_cleanup_generation"]["source_tree"])
            self.assertEqual(tree, self.inventory(kwargs["source"]))
            return {"files": len(tree["files"]), "fixture_only": True}
        base = SimpleNamespace(verified_inputs=lambda args: (self.module, self.parent_ref,
            copy.deepcopy(self.parent), self.writer_plist.read_bytes(), copy.deepcopy(self.writer),
            self.database, self.install_path, self.tree, self.changes), inventory=self.inventory,
            write=write_path, write_bytes=write_bytes,
            load=lambda *args: SimpleNamespace(verify_source_before_import=bootstrap))
        self.enterContext(patch.object(package, "packaging", return_value=base))
        args = SimpleNamespace(checkout=self.source, parent_build=Path(self.parent_ref["path"]),
            installed_plist=self.writer_plist, publisher_plist=self.publisher_plist,
            source_root=self.root / "sealed-source", evidence_root=self.root / "new-evidence",
            check_report=[name + "=" + ref["path"] for name, ref in self.checks.items()],
            actor="offline fixture", reason="synthetic code-only packaging")
        return package, args

    def test_prepare_preserves_all_old_plans_and_emits_only_paired_proposals(self):
        package, args = self.packaging_fixture()
        result = package.prepare(args)
        self.assertEqual(result["status"], "prepared")
        for key, value in (("database_writes", 0), ("provider_calls", 0), ("services_changed", False),
                           ("paid_gates_reopened", False), ("schema_migration_repeated", False)):
            self.assertEqual(result[key], value)
        child = self.module.payload_at(result["child_build"])
        for key in self.parent:
            if key in self.module.PARENT_SUCCESSORS:
                self.assertEqual(child[key], self.parent[key])
        self.assertEqual(child["daily_pipeline_successor"]["parent_build"], self.parent_ref)
        self.assertEqual(child["daily_pipeline_successor"]["inherited_previous_daily_pipeline_proof_sha256"],
            self.inherited["previous_daily_pipeline_proof"]["proof_sha256"])
        self.assertEqual(child["account_profile_successor"]["authorization"], self.profile_auth)
        self.assertEqual(child["account_profile_recovery_successor"]["authorization"], self.compensation_auth)
        for field, key in self.module.INHERITED_PROOFS.items():
            self.assertEqual(result[field], self.inherited[key]["proof_sha256"])
        evidence_names = {path.name for path in args.evidence_root.iterdir()}
        self.assertFalse(any("authorization" in name or "cohort" in name for name in evidence_names))
        next_writer = plistlib.loads(Path(result["next_plist"]["path"]).read_bytes())
        next_publisher = plistlib.loads(Path(result["publisher"]["next_plist"]["path"]).read_bytes())
        self.assertEqual(next_writer["EnvironmentVariables"]["DCAR_LOADED_BUILD_RECEIPT"], result["child_build"]["path"])
        for plist in (next_writer, next_publisher):
            self.assertEqual(plist["EnvironmentVariables"]["DCAR_WRITER_SOURCE_ROOT"], str(args.source_root))
            self.assertEqual(plist["EnvironmentVariables"]["DCAR_V8_DB"], str(self.database))

    def test_prepare_rejects_missing_or_duplicate_checks_before_creating_artifacts(self):
        package, args = self.packaging_fixture()
        for reports in ([], [args.check_report[0], args.check_report[0]]):
            with self.subTest(reports=reports):
                args.check_report = reports
                with self.assertRaises(ValueError):
                    package.prepare(args)
                self.assertFalse(args.source_root.exists())
                self.assertFalse(args.evidence_root.exists())


if __name__ == "__main__":
    unittest.main()
