"""Offline profile authorization lineage and bootstrap tests; no live writes."""
from __future__ import annotations
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import plistlib
import re
import shutil
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from tests import test_publisher_capacity_release as capacity_fixtures
from v8 import account_classification_release as classification, runtime_paths

ROOT = Path(__file__).resolve().parents[1]
MODULE = "src/dcar_eval/v8/account_profile_release.py"
AT = capacity_fixtures.AT
DISPATCH = '''    if build.get("account_profile_successor") is not None:
        import importlib.util
        spec = importlib.util.spec_from_file_location("verified_account_profile_release",
            source / "src/dcar_eval/v8/account_profile_release.py")
        require(spec is not None and spec.loader is not None, "account profile successor verifier is missing")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.verify_inheritance(build=build, build_ref=build_ref,
            install_path=install_path, database=database, source=source, at=at)
'''

class AccountProfileReleaseTest(unittest.TestCase):
    def setUp(self):
        body = classification._LOADED_SOURCE
        if DISPATCH.encode() in body:
            self.enterContext(patch.object(classification, "_LOADED_SOURCE", body.replace(DISPATCH.encode(), b"", 1)))
        self.parent_fixture = c = capacity_fixtures.PublisherCapacityReleaseTest()
        self.addCleanup(c.doCleanups)
        c.setUp()
        self.fixture = f = c.fixture
        self.parent, self.parent_ref = copy.deepcopy(c.build), dict(c.ref)
        self.baseline = c.verify()
        self.parent_plist = c.home / "Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
        self.before = f.snapshots()
        self.source = f.root / "profile-source"
        shutil.copytree(c.source, self.source)
        changes = {}
        name = "src/dcar_eval/v8/account_classification_release.py"
        path = self.source / name
        old = path.read_bytes()
        marker = '    if build.get("publisher_capacity_successor") is not None:\n'
        self.assertEqual(old.decode().count(marker), 1)
        new = old.decode().replace(marker, DISPATCH + marker, 1).encode()
        path.write_bytes(new)
        changes[name] = {"before_sha256": hashlib.sha256(old).hexdigest(), "after_sha256": hashlib.sha256(new).hexdigest()}
        body = (ROOT / MODULE).read_text()
        body = re.sub(r"^PARENT_BUILD_SHA256 = .*$", "PARENT_BUILD_SHA256 = " + repr(self.parent_ref["sha256"]), body, flags=re.M)
        body = re.sub(r"^REVIEWED_CHANGES:.*$", "REVIEWED_CHANGES: dict[str, dict[str, str | None]] = " + repr(changes), body, flags=re.M)
        path = self.source / MODULE
        path.write_text(body)
        spec = importlib.util.spec_from_file_location("fixture_profile_release", path)
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        self.tree = {"contract": "writer-source-tree-v1", "source_root": str(self.source), "git": f.git_record(self.source), "files": []}
        for file in sorted(self.source.rglob("*.py")):
            self.tree["files"].append({"path": file.relative_to(self.source).as_posix(), "sha256": hashlib.sha256(file.read_bytes()).hexdigest(), "byte_size": file.stat().st_size, "mode": 0o644})
        tree_ref = self.write("profile-tree.json", self.tree)
        changes = self.module.source_changes(self.module.object_at(self.parent["account_cleanup_generation"]["source_tree"]), self.tree)
        checks = {}
        for name in self.module.REQUIRED_CHECKS:
            log = f.root / (name + ".fixture.log")
            log.write_text("synthetic fixture only, not a production test result\n")
            log.chmod(0o600)
            checks[name] = self.write(name + ".fixture.json", {"contract": self.module.CHECK_CONTRACT, "name": name, "status": "passed", "exit_code": 0, "changes": changes, "command": ["synthetic-fixture-only"], "output": self.module.reference(log)})
        plan = self.write("profile-source-plan.json", {"contract": "account-cleanup-source-plan-v1", "transition": "account-cleanup-0907-v1", "project_root": str(f.project), "source_root": str(self.source), "git": self.tree["git"], "source_tree": tree_ref})
        self.authorization = {"contract": self.module.AUTHORIZATION_CONTRACT, "operations": ["douyin_uid_profile"], "production_rollout": "approved_by_user", "business_e2e": "required", "transport_qualification": "not_verified", "scope": "account_catalog_eligible", "parent_build": self.parent_ref, "source_tree": tree_ref, "catalog_policy_sha256": self.baseline["catalog_capture_policy_sha256"], "formal_database": {"path": str(f.db), "device": f.db.stat().st_dev, "inode": f.db.stat().st_ino}, "actor": "offline fixture", "reason": "explicit profile repair test", "issued_at": AT, "user_instruction": "fixture explicit profile repair", "source_thread_id": "fixture-only"}
        auth_ref = self.write("profile-authorization.json", self.authorization)
        self.build = {**self.parent, "source_root": str(self.source), "git": self.tree["git"], "critical_files": {r["path"]: r["sha256"] for r in self.tree["files"] if r["path"].startswith(("src/", "config/")) and r["path"].endswith((".py", ".json"))}, "code_successor_plan": plan, "account_cleanup_generation": {**self.parent["account_cleanup_generation"], "source_tree": tree_ref}, "account_profile_successor": {"contract": self.module.CONTRACT, "transition": self.module.TRANSITION, "parent_build": self.parent_ref, "source_tree": tree_ref, "changes": changes, "checks": checks, "actor": self.authorization["actor"], "reason": self.authorization["reason"], "issued_at": AT, "production_rollout": "approved_by_user", "business_e2e": "required", "authorization": auth_ref, "transport_qualification": "not_verified", "schema_migration_repeated": False, "provider_qualification_repeated": False, "database_writes": 0, "paid_gates_reopened": False, "business_scope_change": "explicit_profile_operation", "inherited_catalog_proof_sha256": self.baseline["catalog_capture_proof"]["proof_sha256"], "inherited_control_simplification_proof_sha256": self.baseline["control_simplification_proof"]["proof_sha256"], "inherited_publisher_snapshot_proof_sha256": self.baseline["publisher_snapshot_proof"]["proof_sha256"], "inherited_publisher_capacity_proof_sha256": self.baseline["publisher_capacity_proof"]["proof_sha256"]}, "created_at": AT, "validation_scope": "temporary profile fixture"}
        self.seal()
        env = plistlib.loads(self.parent_plist.read_bytes())["EnvironmentVariables"]
        env = {**env, "DCAR_LOADED_BUILD_RECEIPT": self.ref["path"], "DCAR_WRITER_SOURCE_ROOT": str(self.source)}
        self.home = f.root / "profile-home"
        path = self.home / "Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
        path.parent.mkdir(parents=True)
        path.write_bytes(plistlib.dumps({"Label": "cn.tj.dcar.writer-worker", "WorkingDirectory": str(f.project), "ProgramArguments": [str(self.source / "deploy/macos/run_writer_worker.sh")], "EnvironmentVariables": env}))
        path.chmod(0o600)

    def write(self, name, value):
        path = self.fixture.root / name
        path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        path.chmod(0o600)
        return self.module.reference(path)

    def seal(self):
        self.ref = self.write("profile-build.json", {"contract_version": "sealed-build-receipt-v1", "payload": self.build, "payload_sha256": self.module.digest(self.build)})

    def verify(self):
        return self.module.verify_inheritance(build=self.build, build_ref=self.ref, install_path=self.fixture.install_path, database=self.fixture.db, source=self.source, at=AT)

    def test_bootstrap_adds_independent_profile_authority_retaining_all_prior_proofs_without_writes(self):
        result = self.verify()
        self.assertEqual({k: v for k, v in result.items() if k not in {"account_profile_proof", "profile_operation_authority"}}, self.baseline)
        authority = result["profile_operation_authority"]
        self.assertEqual(authority["loaded_build"], self.ref)
        self.assertEqual(authority["operations"], ["douyin_uid_profile"])
        self.assertEqual(authority["authorization_payload"]["business_e2e"], "required")
        self.assertEqual(authority["proof_sha256"], self.module.digest({k: v for k, v in authority.items() if k != "proof_sha256"}))
        self.assertEqual(runtime_paths.verify_source_before_import(data=self.fixture.project, source=self.source, build_receipt=Path(self.ref["path"]), home=self.home)["files"], len(self.tree["files"]))
        self.assertEqual(self.fixture.snapshots(), self.before)

    def test_independent_approval_cannot_expand_operations_or_claim_e2e_or_qualification(self):
        for key, value in (("operations", ["douyin_user_posts", "douyin_uid_profile"]), ("business_e2e", "deferred_by_user"), ("transport_qualification", "qualified"), ("scope", "all_accounts"), ("user_instruction", "")):
            with self.subTest(key=key):
                auth = {**self.authorization, key: value}
                self.build["account_profile_successor"]["authorization"] = self.write("profile-authorization.json", auth)
                self.seal()
                with self.assertRaisesRegex(ValueError, "explicit profile authorization"):
                    self.verify()
        self.assertEqual(self.fixture.snapshots(), self.before)

    def test_approval_bound_to_actual_parent_source_and_database(self):
        for key, value in (("parent_build", {}), ("source_tree", {}), ("formal_database", {}), ("catalog_policy_sha256", "0" * 64)):
            with self.subTest(key=key):
                self.build["account_profile_successor"]["authorization"] = self.write("profile-authorization.json", {**self.authorization, key: value})
                self.seal()
                with self.assertRaisesRegex(ValueError, "explicit profile authorization"):
                    self.verify()

    def test_historical_authority_and_latest_capacity_proof_cannot_change(self):
        original = copy.deepcopy(self.build)
        for target in ("account_cleanup_generation", "publisher_capacity_successor", "publisher_snapshot_successor", "control_simplification_successor", "account_catalog_capture_successor"):
            self.build = copy.deepcopy(original)
            self.build[target]["unreviewed"] = True
            self.seal()
            with self.assertRaises(ValueError):
                self.verify()
        self.build = copy.deepcopy(original)
        self.build["account_profile_successor"]["inherited_publisher_capacity_proof_sha256"] = "0" * 64
        self.seal()
        with self.assertRaisesRegex(ValueError, "capacity proof differs"):
            self.verify()

    def test_missing_checks_and_modified_approval_are_rejected(self):
        original = copy.deepcopy(self.build)
        self.build["account_profile_successor"]["checks"] = {}
        self.seal()
        with self.assertRaisesRegex(ValueError, "focused checks"):
            self.verify()
        self.build = original
        self.seal()
        Path(self.build["account_profile_successor"]["authorization"]["path"]).write_text("{}")
        with self.assertRaises(ValueError):
            self.verify()

    def test_parent_and_delta_must_be_frozen(self):
        with patch.object(self.module, "PARENT_BUILD_SHA256", "PENDING"):
            with self.assertRaisesRegex(ValueError, "PENDING"):
                self.verify()
        with patch.object(self.module, "REVIEWED_CHANGES", {}):
            with self.assertRaisesRegex(ValueError, "PENDING"):
                self.verify()

    def _packaging_fixture(self):
        def load(path, name):
            spec = importlib.util.spec_from_file_location(name, path)
            result = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(result)
            return result
        package = load(ROOT / "scripts/prepare_account_profile_release.py", "fixture_controls_packaging")
        owner = Path(capacity_fixtures.__file__).resolve().parents[1]
        base = load(owner / "scripts/prepare_account_catalog_capture_release.py", "fixture_inherited_packaging")
        base.MODULE = MODULE
        # Historical fixture sources have a runtime_paths stub. Use the actual
        # read-only inventory/bootstrap implementation, as the parent tests do.
        original_load = base.load
        def loaded(path, name):
            return runtime_paths if path.name == "runtime_paths.py" else original_load(path, name)
        self.enterContext(patch.object(package, "packaging", return_value=base))
        self.enterContext(patch.object(base, "load", side_effect=loaded))
        self.enterContext(patch.object(base.base, "load", return_value=runtime_paths))
        return package

    def test_packaging_failed_check_records_failure_without_preparing_release(self):
        package = self._packaging_fixture()
        output = self.fixture.root / "profile-failed-check.json"
        args = SimpleNamespace(checkout=self.source, parent_build=Path(self.parent_ref["path"]),
            installed_plist=self.parent_plist, name="account_profile_release", output=output,
            command=[sys.executable, "-B", "-c", "print('synthetic failure'); raise SystemExit(3)"])
        with self.assertRaisesRegex(ValueError, "Focused check failed"):
            package.check(args)
        self.assertEqual(json.loads(output.read_text())["exit_code"], 3)
        self.assertIn("synthetic failure", output.with_suffix(".log").read_text())
        self.assertEqual(self.fixture.snapshots(), self.before)

    def test_packaging_emits_only_source_receipts_and_paired_plist_proposals(self):
        package = self._packaging_fixture()
        check_reports = []
        for name in sorted(self.module.REQUIRED_CHECKS):
            output = self.fixture.root / (name + ".actual-check.json")
            args = SimpleNamespace(checkout=self.source, parent_build=Path(self.parent_ref["path"]),
                installed_plist=self.parent_plist, name=name, output=output,
                command=[sys.executable, "-B", "-c", "print('synthetic packaging fixture only')"])
            package.check(args)
            check_reports.append(name + "=" + str(output))
        publisher = self.fixture.root / "fixture-publisher.plist"
        publisher.write_bytes(plistlib.dumps({"Label": "cn.tj.dcar.snapshot-publisher",
            "ProgramArguments": [str(Path(self.parent["source_root"]) / "deploy/macos/run_snapshot_publisher.sh")],
            "EnvironmentVariables": {"DCAR_WRITER_SOURCE_ROOT": self.parent["source_root"],
                "DCAR_PROJECT_ROOT": self.parent["project_root"], "DCAR_V8_DB": str(self.fixture.db)}}))
        publisher.chmod(0o600)
        writer_before, publisher_before = self.parent_plist.read_bytes(), publisher.read_bytes()
        args = SimpleNamespace(checkout=self.source, parent_build=Path(self.parent_ref["path"]),
            installed_plist=self.parent_plist, publisher_plist=publisher,
            source_root=self.fixture.root / "packaged-profile-source", evidence_root=self.fixture.root / "profile-evidence",
            check_report=check_reports, user_instruction="fixture explicit profile repair", source_thread_id="fixture-only", actor="offline fixture", reason="synthetic packaging fixture")
        result = package.prepare(args)
        self.assertEqual(result["status"], "prepared")
        authority = self.module.object_at(result["authorization"])
        self.assertEqual(authority["operations"], ["douyin_uid_profile"])
        self.assertEqual(authority["business_e2e"], "required")
        self.assertEqual(result["database_writes"], 0)
        self.assertFalse(result["services_changed"])
        self.assertFalse(result["paid_gates_reopened"])
        child = self.module.payload_at(result["child_build"])
        self.assertEqual(child["account_catalog_capture_successor"], self.parent["account_catalog_capture_successor"])
        self.assertEqual(child["metric_gap_successor"], self.parent["metric_gap_successor"])
        self.assertEqual(child["control_simplification_successor"], self.parent["control_simplification_successor"])
        self.assertEqual(child["publisher_snapshot_successor"], self.parent["publisher_snapshot_successor"])
        self.assertEqual(result["inherited_publisher_snapshot_proof_sha256"],
                         self.baseline["publisher_snapshot_proof"]["proof_sha256"])
        self.assertEqual(result["inherited_control_simplification_proof_sha256"],
                         self.baseline["control_simplification_proof"]["proof_sha256"])
        self.assertEqual(self.parent_plist.read_bytes(), writer_before)
        self.assertEqual(publisher.read_bytes(), publisher_before)
        self.assertEqual(self.fixture.snapshots(), self.before)


if __name__ == "__main__":
    unittest.main()
