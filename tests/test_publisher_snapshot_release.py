"""Offline successor fixtures; no service, production DB or provider calls.

Run with the candidate on PYTHONPATH. The real controls-v3 successor fixture creates
temporary DBs/receipts, including the catalog and historical schema lineage.
The Publisher verification under test performs no migration or database writes.
"""
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

from tests import test_control_simplification_release as controls_fixtures
from v8 import account_classification_release as classification, runtime_paths

ROOT = Path(__file__).resolve().parents[1]
MODULE = "src/dcar_eval/v8/publisher_snapshot_release.py"
AT = controls_fixtures.AT
DISPATCH = '''    if build.get("publisher_snapshot_successor") is not None:
        import importlib.util
        spec = importlib.util.spec_from_file_location("verified_publisher_snapshot_release",
            source / "src/dcar_eval/v8/publisher_snapshot_release.py")
        require(spec is not None and spec.loader is not None, "publisher snapshot successor verifier is missing")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.verify_inheritance(build=build, build_ref=build_ref,
            install_path=install_path, database=database, source=source, at=at)
'''


class PublisherSnapshotReleaseTest(unittest.TestCase):
    def setUp(self):
        # Model the actual published parent before controls dispatch existed.
        body = classification._LOADED_SOURCE
        self.assertEqual(body.decode().count(DISPATCH), 1)
        self.enterContext(patch.object(classification, "_LOADED_SOURCE", body.replace(DISPATCH.encode(), b"", 1)))
        self.controls_fixture = c = controls_fixtures.ControlSimplificationReleaseTest()
        self.addCleanup(c.doCleanups)
        c.setUp()
        self.fixture = f = c.fixture
        self.parent, self.parent_ref = copy.deepcopy(c.build), dict(c.ref)
        self.baseline = c.verify()
        self.parent_plist = c.home / "Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
        self.before = f.snapshots()
        self.source = f.root / "publisher-source"
        shutil.copytree(c.source, self.source)
        changes = {}
        for name in ("scripts/build_server_snapshot.py", "src/dcar_eval/v8/account_classification_release.py"):
            path = self.source / name
            old = path.read_bytes() if path.exists() else None
            path.parent.mkdir(parents=True, exist_ok=True)
            if name.endswith("build_server_snapshot.py"):
                new = (old or b"") + b"\n# synthetic reviewed Publisher snapshot fixture\n"
            else:
                marker = '    if build.get("control_simplification_successor") is not None:\n'
                self.assertEqual(old.decode().count(marker), 1)
                new = old.decode().replace(marker, DISPATCH + marker, 1).encode()
            path.write_bytes(new)
            changes[name] = {"before_sha256": hashlib.sha256(old).hexdigest() if old is not None else None, "after_sha256": hashlib.sha256(new).hexdigest()}
        body = (ROOT / MODULE).read_text()
        body = re.sub(r'^PARENT_BUILD_SHA256 = .*$', 'PARENT_BUILD_SHA256 = ' + repr(self.parent_ref['sha256']), body, flags=re.M)
        body = re.sub(r'^REVIEWED_CHANGES:.*$', 'REVIEWED_CHANGES: dict[str, dict[str, str | None]] = ' + repr(changes), body, flags=re.M)
        path = self.source / MODULE
        path.write_text(body)
        spec = importlib.util.spec_from_file_location("fixture_control_release", path)
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        self.tree = {"contract": "writer-source-tree-v1", "source_root": str(self.source),
                     "git": f.git_record(self.source), "files": []}
        for file in sorted(self.source.rglob("*.py")):
            self.tree["files"].append({"path": file.relative_to(self.source).as_posix(),
                "sha256": hashlib.sha256(file.read_bytes()).hexdigest(), "byte_size": file.stat().st_size, "mode": 0o644})
        tree_ref = self.write("publisher-tree.json", self.tree)
        changes = self.module.source_changes(self.module.object_at(self.parent["account_cleanup_generation"]["source_tree"]), self.tree)
        checks = {}
        for name in self.module.REQUIRED_CHECKS:
            log = f.root / (name + ".fixture.log")
            log.write_text("synthetic fixture only, not a production test result\n")
            log.chmod(0o600)
            checks[name] = self.write(name + ".fixture.json", {"contract": self.module.CHECK_CONTRACT,
                "name": name, "status": "passed", "exit_code": 0, "changes": changes,
                "command": ["synthetic-fixture-only"], "output": self.module.reference(log)})
        plan = self.write("publisher-source-plan.json", {"contract": "account-cleanup-source-plan-v1",
            "transition": "account-cleanup-0907-v1", "project_root": str(f.project),
            "source_root": str(self.source), "git": self.tree["git"], "source_tree": tree_ref})
        self.build = {**self.parent, "source_root": str(self.source), "git": self.tree["git"],
            "critical_files": {r["path"]: r["sha256"] for r in self.tree["files"]
                if r["path"].startswith(("src/", "config/")) and r["path"].endswith((".py", ".json"))}, "code_successor_plan": plan,
            "account_cleanup_generation": {**self.parent["account_cleanup_generation"], "source_tree": tree_ref},
            "publisher_snapshot_successor": {"contract": self.module.CONTRACT, "transition": self.module.TRANSITION,
                "parent_build": self.parent_ref, "source_tree": tree_ref, "changes": changes, "checks": checks,
                "actor": "offline fixture", "reason": "synthetic controls code-only review", "issued_at": AT,
                "production_rollout": "approved_by_user", "transport_qualification": "not_verified",
                "schema_migration_repeated": False, "provider_qualification_repeated": False,
                "database_writes": 0, "paid_gates_reopened": False, "business_scope_change": "none",
                "inherited_catalog_proof_sha256": self.baseline["catalog_capture_proof"]["proof_sha256"],
                "inherited_control_simplification_proof_sha256": self.baseline["control_simplification_proof"]["proof_sha256"]},
            "created_at": AT, "validation_scope": "temporary controls fixture"}
        self.seal()
        env = plistlib.loads(self.parent_plist.read_bytes())["EnvironmentVariables"]
        env = {**env, "DCAR_LOADED_BUILD_RECEIPT": self.ref["path"], "DCAR_WRITER_SOURCE_ROOT": str(self.source)}
        self.home = f.root / "publisher-home"
        path = self.home / "Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
        path.parent.mkdir(parents=True)
        path.write_bytes(plistlib.dumps({"Label": "cn.tj.dcar.writer-worker", "WorkingDirectory": str(f.project),
            "ProgramArguments": [str(self.source / "deploy/macos/run_writer_worker.sh")], "EnvironmentVariables": env}))
        path.chmod(0o600)

    def write(self, name, value):
        path = self.fixture.root / name
        path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        path.chmod(0o600)
        return self.module.reference(path)

    def seal(self):
        self.ref = self.write("publisher-build.json", {"contract_version": "sealed-build-receipt-v1",
            "payload": self.build, "payload_sha256": self.module.digest(self.build)})

    def verify(self):
        return self.module.verify_inheritance(build=self.build, build_ref=self.ref,
            install_path=self.fixture.install_path, database=self.fixture.db, source=self.source, at=AT)

    def bootstrap(self):
        return runtime_paths.verify_source_before_import(data=self.fixture.project, source=self.source,
            build_receipt=Path(self.ref["path"]), home=self.home)

    def test_bootstrap_retains_complete_controls_metric_catalog_and_legacy_proof_without_writes(self):
        self.assertEqual(self.bootstrap()["files"], len(self.tree["files"]))
        result = self.verify()
        self.assertEqual({k: v for k, v in result.items() if k != "publisher_snapshot_proof"}, self.baseline)
        self.assertEqual(self.build["account_catalog_capture_successor"], self.parent["account_catalog_capture_successor"])
        self.assertEqual(self.build["metric_gap_successor"], self.parent["metric_gap_successor"])
        self.assertEqual(result["metric_gap_proof"], self.baseline["metric_gap_proof"])
        self.assertEqual(result["control_simplification_proof"], self.baseline["control_simplification_proof"])
        self.assertEqual(result["control_simplification_proof"]["loaded_build"], self.parent_ref)
        self.assertEqual(result["publisher_snapshot_proof"]["loaded_build"], self.ref)
        self.assertEqual(self.fixture.snapshots(), self.before)

    def test_pending_parent_or_unfrozen_changes_cannot_pass(self):
        with patch.object(self.module, "PARENT_BUILD_SHA256", "PENDING"):
            with self.assertRaisesRegex(ValueError, "PENDING"):
                self.verify()
        with patch.object(self.module, "REVIEWED_CHANGES", {}):
            with self.assertRaisesRegex(ValueError, "PENDING"):
                self.verify()

    def test_catalog_schema_operator_and_database_authority_cannot_change(self):
        original = copy.deepcopy(self.build)
        for key in ("catalog", "controls", "metric", "schema", "operator", "migration"):
            self.build = copy.deepcopy(original)
            if key == "catalog":
                self.build["account_catalog_capture_successor"]["account_catalog_policy"]["statuses"].append("paused")
            elif key == "controls":
                self.build["control_simplification_successor"]["parent_build"]["sha256"] = "0" * 64
            elif key == "metric":
                self.build["metric_gap_successor"]["parent_build"]["sha256"] = "0" * 64
            elif key == "schema":
                self.build["schema_contract"]["formal_schema"] = 22
            elif key == "operator":
                self.build["account_cleanup_generation"]["selection_sha256"] = "0" * 64
            else:
                self.build["account_classification_successor"]["migration"]["sha256"] = "0" * 64
            self.seal()
            with self.assertRaises(ValueError):
                self.verify()
        self.assertEqual(self.fixture.snapshots(), self.before)

    def test_controls_proof_reference_must_match_complete_inheritance(self):
        self.build["publisher_snapshot_successor"]["inherited_control_simplification_proof_sha256"] = "0" * 64
        self.seal()
        with self.assertRaisesRegex(ValueError, "inherited controls proof differs"):
            self.verify()

    def test_release_cannot_reopen_gates_migrate_or_claim_transport_qualification(self):
        original = copy.deepcopy(self.build)
        for key, value in (("paid_gates_reopened", True), ("schema_migration_repeated", True),
                           ("provider_qualification_repeated", True), ("database_writes", 1),
                           ("business_scope_change", "approved_by_user"), ("transport_qualification", "qualified")):
            self.build = copy.deepcopy(original)
            self.build["publisher_snapshot_successor"][key] = value
            self.seal()
            with self.assertRaisesRegex(ValueError, "release scope"):
                self.verify()

    def test_missing_checks_changed_log_or_catalog_proof_are_rejected(self):
        original = copy.deepcopy(self.build)
        self.build["publisher_snapshot_successor"]["checks"] = {}
        self.seal()
        with self.assertRaisesRegex(ValueError, "checks are incomplete"):
            self.verify()
        self.build = copy.deepcopy(original)
        self.build["publisher_snapshot_successor"]["inherited_catalog_proof_sha256"] = "0" * 64
        self.seal()
        with self.assertRaisesRegex(ValueError, "catalog proof differs"):
            self.verify()
        self.build = original
        self.seal()
        report = self.module.object_at(next(iter(self.build["publisher_snapshot_successor"]["checks"].values())))
        Path(report["output"]["path"]).write_text("changed log\n")
        with self.assertRaisesRegex(ValueError, "check output changed"):
            self.verify()

    def test_parent_verifier_is_pinned_and_unreviewed_child_bytes_fail_bootstrap(self):
        for name in self.module.PARENT_VERIFIER_MODULES:
            path = Path(self.parent["source_root"]) / name
            old = path.read_bytes()
            path.write_bytes(old + b"\n# altered parent verifier\n")
            with self.assertRaisesRegex(ValueError, "parent verifier changed"):
                self.verify()
            path.write_bytes(old)
        self.controls_fixture.bootstrap()
        self.bootstrap()
        path = self.source / "src/dcar_eval/v8/api.py"
        path.write_bytes(path.read_bytes() + b"\n# unreviewed child\n")
        with self.assertRaisesRegex(ValueError, "source content"):
            self.bootstrap()

    def test_controls_inheritance_cannot_be_omitted_or_point_to_another_build(self):
        for key in ("missing", "wrong_build"):
            inherited = copy.deepcopy(self.baseline)
            if key == "missing":
                inherited.pop("control_simplification_proof")
            else:
                inherited["control_simplification_proof"]["loaded_build"]["sha256"] = "0" * 64
            verifier = SimpleNamespace(verify_inheritance=lambda **kwargs: inherited)
            with patch.object(self.module, "_load", return_value=verifier):
                with self.assertRaisesRegex(ValueError, "complete verified controls inheritance"):
                    self.verify()

    def test_latest_parent_controls_check_tamper_cannot_be_skipped(self):
        report = self.module.object_at(next(iter(self.parent["control_simplification_successor"]["checks"].values())))
        Path(report["output"]["path"]).write_text("changed latest parent metric log\n")
        with self.assertRaisesRegex(ValueError, "check output changed"):
            self.verify()

    def test_child_cannot_replace_historical_verifiers(self):
        for name in set(self.module.PARENT_VERIFIER_MODULES) - {self.module.CLASSIFICATION_MODULE}:
            tree = copy.deepcopy(self.tree)
            row = next(row for row in tree["files"] if row["path"] == name)
            row["sha256"] = "0" * 64
            original = self.module.object_at(self.parent["account_cleanup_generation"]["source_tree"])
            with self.assertRaisesRegex(ValueError, "published inheritance verifiers"):
                self.module.source_changes(original, tree)

    def _packaging_fixture(self):
        def load(path, name):
            spec = importlib.util.spec_from_file_location(name, path)
            result = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(result)
            return result
        package = load(ROOT / "scripts/prepare_publisher_snapshot_release.py", "fixture_controls_packaging")
        owner = Path(controls_fixtures.__file__).resolve().parents[1]
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
        output = self.fixture.root / "publisher-failed-check.json"
        args = SimpleNamespace(checkout=self.source, parent_build=Path(self.parent_ref["path"]),
            installed_plist=self.parent_plist, name="publisher_snapshot_release", output=output,
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
            source_root=self.fixture.root / "packaged-publisher-source", evidence_root=self.fixture.root / "publisher-evidence",
            check_report=check_reports, actor="offline fixture", reason="synthetic packaging fixture")
        result = package.prepare(args)
        self.assertEqual(result["status"], "prepared")
        self.assertEqual(result["database_writes"], 0)
        self.assertFalse(result["services_changed"])
        self.assertFalse(result["paid_gates_reopened"])
        child = self.module.payload_at(result["child_build"])
        self.assertEqual(child["account_catalog_capture_successor"], self.parent["account_catalog_capture_successor"])
        self.assertEqual(child["metric_gap_successor"], self.parent["metric_gap_successor"])
        self.assertEqual(child["control_simplification_successor"], self.parent["control_simplification_successor"])
        self.assertEqual(result["inherited_control_simplification_proof_sha256"],
                         self.baseline["control_simplification_proof"]["proof_sha256"])
        self.assertEqual(self.parent_plist.read_bytes(), writer_before)
        self.assertEqual(publisher.read_bytes(), publisher_before)
        self.assertEqual(self.fixture.snapshots(), self.before)


if __name__ == "__main__":
    unittest.main()
