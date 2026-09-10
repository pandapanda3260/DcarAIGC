"""Offline schema21 successor: real temporary receipts, DB, Git and capture gates.

Fixture pins replace only the unavailable production source review and receipt
identities. No formal database, provider call, or service is used.
"""
from __future__ import annotations

import copy
from dataclasses import replace
import hashlib
import os
from pathlib import Path
import plistlib
import re
import shutil
import subprocess
from unittest.mock import patch

from tests import test_v8_account_cleanup_runtime as fixtures
from v8 import account_classification_release as successor
from v8 import capture_authorizations as auth, capture_operator_release as operator
from v8 import capture_release as release, paid_drain, provider_budget, runtime_database, runtime_paths, schema_v21

AT = fixtures.AT


class AccountClassificationReleaseTest(fixtures.CleanupRuntimeTest):
    def git(self, root, *arguments):
        return subprocess.run(["git", "-c", "core.fsmonitor=false", "-C", str(root), *arguments],
            check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout

    def git_record(self, root):
        return {"head": self.git(root, "rev-parse", "HEAD").decode().strip(),
                "tree": self.git(root, "rev-parse", "HEAD^{tree}").decode().strip(),
                "branch": self.git(root, "symbolic-ref", "--quiet", "--short", "HEAD").decode().strip(),
                "status_porcelain_sha256": hashlib.sha256(self.git(root, "status", "--porcelain=v1", "--untracked-files=all")).hexdigest()}

    def write(self, name, value):
        if name == "tree.json":
            self.git(self.source, "init", "-q", "-b", "fixture")
            self.git(self.source, "add", "-A")
            self.git(self.source, "-c", "user.name=Offline Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "fixture")
            value = copy.deepcopy(value)
            value["git"] = self.git_record(self.source)
            for record in value["files"]:
                path = self.source / record["path"]
                record.update(byte_size=path.stat().st_size, mode=0o644)
        return super().write(name, value)

    def envelope(self, name, contract, payload):
        if name == "build.json":
            git = self.git_record(self.source)
            plan = self.write("source-plan.json", {"contract": "account-cleanup-source-plan-v1",
                "transition": "account-cleanup-0907-v1", "project_root": str(self.project),
                "source_root": str(self.source), "git": git, "source_tree": self.generation["source_tree"]})
            payload = {**payload, "project_root": str(self.project), "source_root": str(self.source),
                       "git": git, "code_successor_plan": plan}
        return super().envelope(name, contract, payload)

    def make_child(self, *, migrate=True):
        self.parent_path = Path(os.environ["DCAR_LOADED_BUILD_RECEIPT"])
        self.parent_ref = successor.reference(self.parent_path)
        self.install_path = Path(os.environ["DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT"])
        self.install_ref = successor.reference(self.install_path)
        self.parent = successor.payload_at(self.parent_ref, "sealed-build-receipt-v1")
        self.connection.commit()
        if migrate:
            # This installed-contract patch denotes only this temporary fixture
            # as offline during the real schema20 -> 21 migration.
            with patch.object(runtime_database, "load_installed_writer_contract", return_value=None):
                self.migration_result = schema_v21.migrate(self.connection)
        self.connection.execute("BEGIN IMMEDIATE")
        self.child_source = self.root / "child-source"
        shutil.copytree(self.source, self.child_source)
        api = self.child_source / "src/dcar_eval/v8/api.py"
        api.write_text("# reviewed classification fixture\n")
        reviewed = {"src/dcar_eval/v8/api.py": {"before_sha256": None,
                    "after_sha256": hashlib.sha256(api.read_bytes()).hexdigest()}}
        module_body = successor._LOADED_SOURCE.decode()
        for name, value in (("PARENT_BUILD_SHA256", self.parent_ref["sha256"]),
                            ("PARENT_INSTALL_SHA256", self.install_ref["sha256"])):
            module_body = re.sub(rf'^{name} = .*$', f'{name} = {value!r}', module_body, flags=re.M)
            self.enterContext(patch.object(successor, name, value))
        module_body = re.sub(r'^REVIEWED_CHANGES:.*$', f'REVIEWED_CHANGES: dict[str, dict[str, str | None]] = {reviewed!r}', module_body, flags=re.M)
        module_bytes = module_body.encode()
        (self.child_source / successor.MODULE).write_bytes(module_bytes)
        self.enterContext(patch.object(successor, "REVIEWED_CHANGES", reviewed))
        self.enterContext(patch.object(successor, "_LOADED_SOURCE", module_bytes))
        self.tree = {"contract": "writer-source-tree-v1", "source_root": str(self.child_source),
                     "git": self.git_record(self.child_source), "files": []}
        for path in sorted(self.child_source.rglob("*.py")):
            self.tree["files"].append({"path": path.relative_to(self.child_source).as_posix(),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "byte_size": path.stat().st_size, "mode": 0o644})
        self.tree_ref = self.write("child-tree.json", self.tree)
        changes = successor.source_changes(successor.object_at(self.generation["source_tree"]), self.tree)
        self.checks = {}
        for name in successor.REQUIRED_CHECKS:
            log = self.root / (name + ".log")
            log.write_text("offline fixture: passed\n")
            log.chmod(0o600)
            self.checks[name] = self.write(name + ".json", {"contract": "account-classification-check-v1",
                "name": name, "status": "passed", "exit_code": 0, "changes": changes,
                "output": successor.reference(log)})
        source_plan = self.write("child-source-plan.json", {"contract": "account-cleanup-source-plan-v1",
            "transition": "account-cleanup-0907-v1", "project_root": str(self.project),
            "source_root": str(self.child_source), "git": self.tree["git"], "source_tree": self.tree_ref})
        identity = self.db.stat()
        self.migration = {"contract": "account-classification-install-v1", "status": "migrated",
            "from_schema": 20, "to_schema": 21, "formal_database": str(self.db),
            "database_identity": {"device": identity.st_dev, "inode": identity.st_ino},
            "authority_build": self.parent_ref, "authority_install": self.install_ref,
            "preserved_tables_verified": True, "paid_gates_issued": 0, "migrated_at": AT,
            "migration_proof": schema_v21.migration_proof(self.connection) if migrate else {}}
        self.migration["receipt_sha256"] = successor.digest(self.migration)
        migration_ref = self.write("classification-migration.json", self.migration)
        self.child = {**self.parent, "source_root": str(self.child_source), "git": self.tree["git"],
            "schema_contract": {"code_schema": 21, "formal_schema": 21}, "code_successor_plan": source_plan,
            "critical_files": {r["path"]: r["sha256"] for r in self.tree["files"]},
            "account_cleanup_generation": {**self.generation, "source_tree": self.tree_ref},
            "account_classification_successor": {"contract": successor.CONTRACT, "transition": successor.TRANSITION,
                "parent_build": self.parent_ref, "parent_install": self.install_ref, "source_tree": self.tree_ref,
                "changes": changes, "checks": self.checks, "migration": migration_ref,
                "actor": "offline fixture", "reason": "temporary schema21 classification successor", "issued_at": AT,
                "business_e2e": "deferred_by_user", "transport_qualification": "not_verified", "production_rollout": "approved_by_user"}}
        self.seal_child()
        installed = runtime_database.load_installed_writer_contract(required=True)
        self.child_env = {**installed.payload["EnvironmentVariables"], "DCAR_LOADED_BUILD_RECEIPT": self.child_ref["path"],
                          "DCAR_WRITER_SOURCE_ROOT": str(self.child_source), "DCAR_V8_DB": str(self.db)}
        current = replace(installed, payload={**installed.payload, "EnvironmentVariables": self.child_env})
        self.enterContext(patch.object(runtime_database, "load_installed_writer_contract", return_value=current))
        self.enterContext(patch.dict(os.environ, {**self.child_env, "DCAR_LOADED_BUILD_ID": "sha256:" + self.child_ref["sha256"]}))
        self.home = self.root / "fixture-home"
        plist = self.home / "Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
        plist.parent.mkdir(parents=True)
        plist.write_bytes(plistlib.dumps({"Label": "cn.tj.dcar.writer-worker", "WorkingDirectory": str(self.project),
            "ProgramArguments": [str(self.child_source / "deploy/macos/run_writer_worker.sh")], "EnvironmentVariables": self.child_env}))
        plist.chmod(0o600)

    def seal_child(self):
        ref = super().envelope("child-build.json", "sealed-build-receipt-v1", self.child)
        self.child_ref = successor.reference(Path(ref["path"]))
        if hasattr(self, "child_env"):
            os.environ["DCAR_LOADED_BUILD_ID"] = "sha256:" + self.child_ref["sha256"]

    def verify(self):
        return successor.verify_inheritance(build=self.child, build_ref=self.child_ref, install_path=self.install_path,
            database=self.db, source=self.child_source, at=AT)

    def bootstrap(self):
        return runtime_paths.verify_source_before_import(data=self.project, source=self.child_source,
            build_receipt=Path(self.child_ref["path"]), home=self.home)

    def snapshots(self):
        names = ("acquisition_profile_activations", "pipeline_paid_drain_events", "capture_paid_send_gate_events",
                 "provider_readiness_receipts", "provider_usage", "provider_request_start_events", "capture_work_items")
        return {name: [tuple(row) for row in self.connection.execute("SELECT * FROM " + name)] for name in names}

    def test_schema21_retains_capture_authority_gates_and_budget_without_writes(self):
        original = release._installed_evidence(self.connection, at=AT)
        self.maintenance()
        before = operator.authority(self.connection, evidence=original, operation="douyin_user_posts", at=AT)
        snapshots = self.snapshots()
        budget = provider_budget.budget_summary(self.connection, at=AT)
        self.make_child()
        self.assertEqual(self.bootstrap()["files"], len(self.tree["files"]))
        child = release._installed_evidence(self.connection, at=AT)
        self.assertEqual({k: v for k, v in child.items() if k != "account_classification_successor"}, original)
        self.assertEqual(operator.authority(self.connection, evidence=child, operation="douyin_user_posts", at=AT), before)
        self.assertEqual(provider_budget.budget_summary(self.connection, at=AT), budget)
        self.assertEqual(self.maintenance()["operations"]["douyin_user_posts"]["status"], "fresh")
        self.assertEqual(self.snapshots(), snapshots)
        self.assertEqual(child["account_classification_successor"]["authority_build"], self.parent_ref)
        self.assertNotEqual(child["build_sha256"], self.child_ref["sha256"])

    def test_schema21_without_successor_or_migration_proof_cannot_open_gates(self):
        self.make_child()
        self.child.pop("account_classification_successor")
        self.seal_child()
        with self.assertRaisesRegex(ValueError, "successor contract"):
            release._installed_evidence(self.connection, at=AT)
        self.assertEqual(self.snapshots()["capture_paid_send_gate_events"], [])

    def test_missing_and_invalid_migration_checks_remain_closed(self):
        self.make_child()
        plan = self.child["account_classification_successor"]
        original = copy.deepcopy(plan)
        for field, value, message in (("checks", {}, "checks are incomplete"), ("parent_build", {**self.parent_ref, "sha256": "a" * 64}, "reviewed pins")):
            self.child["account_classification_successor"] = {**original, field: value}
            self.seal_child()
            with self.assertRaisesRegex(ValueError, message):
                self.verify()
        self.child["account_classification_successor"] = original
        self.migration["paid_gates_issued"] = 1
        self.migration["receipt_sha256"] = successor.digest({k: v for k, v in self.migration.items() if k != "receipt_sha256"})
        self.child["account_classification_successor"]["migration"] = self.write("classification-migration.json", self.migration)
        self.seal_child()
        with self.assertRaisesRegex(ValueError, "migration proof"):
            self.verify()

    def test_external_migration_proof_must_match_live_database_receipt(self):
        self.make_child()
        self.migration["migration_proof"]["receipt_sha256"] = "e" * 64
        self.migration["receipt_sha256"] = successor.digest({k: v for k, v in self.migration.items() if k != "receipt_sha256"})
        self.child["account_classification_successor"]["migration"] = self.write("classification-migration.json", self.migration)
        self.seal_child()
        with self.assertRaisesRegex(auth.AuthorizationError, "database migration differs"):
            release._installed_evidence(self.connection, at=AT)
        self.assertEqual(self.snapshots()["capture_paid_send_gate_events"], [])

    def test_unreviewed_source_and_mutated_check_logs_are_rejected(self):
        self.make_child()
        with patch.object(successor, "REVIEWED_CHANGES", {}):
            with self.assertRaisesRegex(ValueError, "not frozen"):
                self.verify()
        (self.root / "classification_reports.log").write_text("changed\n")
        with self.assertRaisesRegex(ValueError, "check log"):
            self.verify()

    def test_scope_runtime_and_false_qualification_changes_cannot_inherit(self):
        self.make_child()
        original = copy.deepcopy(self.child)
        self.child["account_cleanup_generation"]["selection_sha256"] = "e" * 64
        self.seal_child()
        with self.assertRaisesRegex(ValueError, "capture authority"):
            self.verify()
        self.child = copy.deepcopy(original)
        self.child["runtime_root_receipt"]["sha256"] = "a" * 64
        self.seal_child()
        with self.assertRaisesRegex(ValueError, "runtime binding"):
            self.verify()
        self.child = copy.deepcopy(original)
        self.child["account_classification_successor"]["transport_qualification"] = "verified"
        self.seal_child()
        with self.assertRaisesRegex(ValueError, "review scope"):
            self.verify()

    def test_unmigrated_database_is_blocked(self):
        self.make_child(migrate=False)
        with self.assertRaisesRegex(auth.AuthorizationError, "schema differs"):
            release._installed_evidence(self.connection, at=AT)

    def test_schema21_still_honors_operator_hold_drain_and_provider_circuit(self):
        self.make_child()
        gate = {"provider": "tikhub", "operation": "douyin_user_posts", "state": "closed", "reason": "explicit hold",
                "evidence_json": "{}", "recorded_at": AT}
        self.connection.execute("INSERT INTO capture_paid_send_gate_events(provider,operation,state,reason,evidence_json,recorded_at,event_sha256) VALUES(?,?,?,?,?,?,?)", (*gate.values(), auth.digest(gate)))
        self.assertEqual(self.maintenance()["operations"]["douyin_user_posts"]["status"], "not_enabled")
        with patch.object(paid_drain, "dispatch_state", return_value=paid_drain.DrainState("closed")):
            self.assertEqual(self.maintenance()["status"], "blocked")
        with patch.object(provider_budget, "circuit_state", return_value={"open": True}):
            self.assertEqual(self.maintenance()["status"], "blocked")

    def test_schema21_paid_send_retains_owner_and_route_fences(self):
        from v8 import capture_planning, durable_runs
        self.make_child()
        scope = provider_budget.PaidScope(scheduler_run_id=999, scheduler_attempt_id=999,
                                         scheduler_owner_token="expired", scheduler_scan_id="missing")
        with self.assertRaises(provider_budget.PaidScopeBlocked) as error:
            provider_budget.renew_paid_owner_lease(self.connection, scope, at=AT)
        self.assertEqual(error.exception.error_code, "attempt_owner_lost")
        self.assertIn("root_run_id IS NULL", durable_runs.root_run_predicate(self.connection, "r"))
        # A native send cannot fall through the pre-schema20 no-route branch.
        with self.assertRaises(provider_budget.PaidScopeBlocked):
            capture_planning.require_send_route(self.connection, scope=scope,
                                                operation="douyin_user_posts", at=AT)
        self.assertEqual(self.snapshots()["provider_usage"], [])

    def test_replaced_database_future_dated_build_and_changed_parent_are_rejected(self):
        self.make_child()
        other = self.root / "replacement.sqlite3"
        other.write_bytes(self.db.read_bytes())
        with self.assertRaisesRegex(ValueError, "inode"):
            successor.verify_inheritance(build=self.child, build_ref=self.child_ref, install_path=self.install_path,
                database=other, source=self.child_source, at=AT)
        self.child["account_classification_successor"]["issued_at"] = "2099-01-01T00:00:00Z"
        self.seal_child()
        with self.assertRaisesRegex(ValueError, "future dated"):
            self.verify()
        self.child["account_classification_successor"]["issued_at"] = AT
        self.seal_child()
        self.parent_path.write_text(self.parent_path.read_text() + "\n")
        with self.assertRaisesRegex(ValueError, "reference hash"):
            self.verify()

    def test_live_source_tamper_and_unlisted_ignored_executable_are_rejected(self):
        self.make_child()
        api = self.child_source / "src/dcar_eval/v8/api.py"
        before = api.read_bytes()
        api.write_text("# tampered\n")
        with self.assertRaisesRegex(ValueError, "source content|Git state"):
            self.bootstrap()
        api.write_bytes(before)
        (self.child_source / ".git/info/exclude").write_text("ignored.py\n")
        (self.child_source / "src/dcar_eval/v8/ignored.py").write_text("raise RuntimeError('must never execute')\n")
        with self.assertRaisesRegex(ValueError, "unlisted bootstrap executable"):
            self.bootstrap()

    def test_duplicate_inventory_mode_changes_and_private_receipt_rejected(self):
        self.make_child()
        broken = copy.deepcopy(self.tree)
        broken["files"].append(copy.deepcopy(broken["files"][0]))
        with self.assertRaisesRegex(ValueError, "manifest path"):
            successor.source_changes(successor.object_at(self.generation["source_tree"]), broken)
        broken = copy.deepcopy(self.tree)
        next(row for row in broken["files"] if row["path"].endswith("/paid_dispatch.py"))["mode"] = 0o755
        with self.assertRaisesRegex(ValueError, "source modes"):
            successor.source_changes(successor.object_at(self.generation["source_tree"]), broken)
        self.install_path.chmod(0o644)
        with self.assertRaisesRegex(ValueError, "unsafe"):
            self.verify()
