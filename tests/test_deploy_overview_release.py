"""Mock service/HTTP boundaries and temporary artifacts only; never deploy."""
import copy
from contextlib import nullcontext
import hashlib
import importlib.util
import json
from pathlib import Path
import plistlib
import tempfile
import socket
import sqlite3
import subprocess
import urllib.request
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

PATH = Path(__file__).resolve().parents[1] / "scripts/deploy_overview_release.py"
SPEC = importlib.util.spec_from_file_location("overview_deploy_under_test", PATH)
deploy = importlib.util.module_from_spec(SPEC)
with patch.object(sqlite3, "connect", side_effect=AssertionError("Database access forbidden during adapter import")), \
        patch.object(subprocess, "Popen", side_effect=AssertionError("Subprocess forbidden during adapter import")), \
        patch.object(socket, "socket", side_effect=AssertionError("Network forbidden during adapter import")), \
        patch.object(urllib.request, "urlopen", side_effect=AssertionError("HTTP forbidden during adapter import")):
    SPEC.loader.exec_module(deploy)
READ_ACTIVITY = deploy.base.read_activity
REAL_PARENT_SHA256 = deploy.EXPECTED_PARENT_BUILD_SHA256
REAL_PUBLISHED_DAILY_SHA256 = deploy.EXPECTED_PUBLISHED_DAILY_BUILD_SHA256
REAL_PREVIOUS_PARENT_SHA256 = deploy.EXPECTED_PREVIOUS_PARENT_BUILD_SHA256


class OverviewDeployAdapterTest(unittest.TestCase):
    def setUp(self):
        # Fail closed for every unmocked DB, provider/network or process boundary.
        for module, name in ((sqlite3, "connect"), (socket, "socket"), (socket, "create_connection"),
                             (urllib.request, "urlopen"), (subprocess, "Popen"),
                             (subprocess, "run"), (subprocess, "check_output")):
            self.enterContext(patch.object(module, name, side_effect=AssertionError("Forbidden external boundary: " + name)))
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve()
        self.database = self.root / "fixture.sqlite3"
        self.database.write_bytes(b"temporary identity fixture; no SQL executed")
        self.database.chmod(0o600)
        self.cleanup_ref = self.build("cleanup-build.json", {"generation": "cleanup"})
        self.classification_ref = self.build("classification-build.json", {"generation": "classification"})
        self.manual_ref = self.build("manual-build.json", {"generation": "manual"})
        self.catalog_ref = self.build("catalog-build.json", {"generation": "catalog"})
        self.metric_ref = self.build("metric-build.json", {"generation": "metric"})
        self.controls_ref = self.build("controls-build.json", {"generation": "controls"})
        self.publisher_ref = self.build("publisher-build.json", {"generation": "publisher"})
        self.capacity_ref = self.build("capacity-build.json", {"generation": "capacity"})
        self.profile_ref = self.build("profile-build.json", {"generation": "profile"})
        self.profile_authorization = {"path": str(self.root / "profile-approval.json"), "sha256": "9" * 64}
        self.compensation_authorization = {"path": str(self.root / "consumed-compensation-approval.json"),
                                           "sha256": "8" * 64, "issued_at": "2026-09-10T00:00:00Z", "expires_at": "2026-09-10T01:00:00Z"}
        self.parent = {"source_root": str(self.root / "parent-source"), "project_root": str(self.root / "data"),
            "schema_contract": {"code_schema": 21, "formal_schema": 21},
            "account_cleanup_generation": {"operator_approval": "unchanged", "source_tree": self.cleanup_ref}}
        for name, ancestor in (("account_classification_successor", self.cleanup_ref),
                ("manual_content_scope_successor", self.classification_ref),
                ("account_catalog_capture_successor", self.manual_ref), ("metric_gap_successor", self.catalog_ref),
                ("control_simplification_successor", self.metric_ref), ("publisher_snapshot_successor", self.controls_ref),
                ("publisher_capacity_successor", self.publisher_ref), ("account_profile_successor", self.capacity_ref),
                ("account_profile_recovery_successor", self.profile_ref)):
            self.parent[name] = {"contract": name.replace("_successor", "").replace("_", "-") + "-code-successor-v1",
                                 "parent_build": ancestor, "retained": name}
        self.parent["account_profile_successor"]["authorization"] = self.profile_authorization
        self.parent["account_profile_recovery_successor"]["authorization"] = self.compensation_authorization
        self.recovery_ref = self.build("recovery-build.json", self.parent)
        self.enterContext(patch.object(deploy, "EXPECTED_RECOVERY_BUILD_SHA256", self.recovery_ref["sha256"]))
        self.daily_v2_ref = self.build("daily-v2-build.json", {**self.parent, "generation": "daily-v2"})
        self.enterContext(patch.object(deploy, "EXPECTED_PREVIOUS_PARENT_BUILD_SHA256", self.daily_v2_ref["sha256"]))
        self.parent["daily_pipeline_successor"] = {"contract": "daily-pipeline-code-successor-v1",
            "parent_build": self.daily_v2_ref, "transition": "daily-pipeline-20260911-v1"}
        self.published_ref = self.build("published-daily-build.json", self.parent)
        self.enterContext(patch.object(deploy, "EXPECTED_PUBLISHED_DAILY_BUILD_SHA256", self.published_ref["sha256"]))
        self.parent["daily_pipeline_successor"] = {"contract": "daily-pipeline-code-successor-v1",
            "parent_build": self.published_ref, "transition": "daily-metric-validity-20260911-v1"}
        self.parent_ref = self.build("parent-build.json", self.parent)
        self.enterContext(patch.object(deploy, "EXPECTED_PARENT_BUILD_SHA256", self.parent_ref["sha256"]))
        self.proof_fields = {
            "inherited_catalog_proof_sha256": "a" * 64, "inherited_control_simplification_proof_sha256": "b" * 64,
            "inherited_publisher_snapshot_proof_sha256": "c" * 64, "inherited_publisher_capacity_proof_sha256": "d" * 64,
            "inherited_account_profile_proof_sha256": "e" * 64, "inherited_profile_operation_authority_sha256": "f" * 64,
            "account_profile_recovery_proof_sha256": "1" * 64, "profile_compensation_authority_sha256": "2" * 64,
            "inherited_metric_gap_proof_sha256": "3" * 64, "inherited_manual_content_scope_proof_sha256": "4" * 64,
            "inherited_account_classification_proof_sha256": "5" * 64,
            "inherited_previous_daily_pipeline_proof_sha256": "7" * 64,
            "inherited_published_daily_pipeline_proof_sha256": "8" * 64,
            "inherited_daily_pipeline_proof_sha256": "6" * 64}
        self.child = {**copy.deepcopy(self.parent), "source_root": str(self.root / "child-source"),
            "overview_successor": {"contract": "overview-code-successor-v1", "parent_build": self.parent_ref,
                "transition": "overview-four-platforms-20260912-v1",
                **self.proof_fields}}
        self.child_ref = self.build("child-build.json", self.child)
        env = {"DCAR_V8_DB": str(self.database), "DCAR_WRITER_SOURCE_ROOT": self.parent["source_root"],
            "DCAR_LOADED_BUILD_RECEIPT": self.parent_ref["path"], "DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT": str(self.root / "install.json"),
            "DCAR_KEEP_EXISTING_CAP": "50", "DCAR_SCHEDULER_START_PAUSED": "0"}
        self.writer_before = {"Label": deploy.base.WRITER, "EnvironmentVariables": env,
            "ProgramArguments": [self.parent["source_root"] + "/deploy/macos/run_writer_worker.sh"]}
        source = self.child["source_root"]
        self.writer_next = {**self.writer_before, "EnvironmentVariables": {**env,
            "DCAR_LOADED_BUILD_RECEIPT": self.child_ref["path"], "DCAR_WRITER_SOURCE_ROOT": source,
            "PYTHONPATH": source + "/src/dcar_eval:" + source + "/scripts"},
            "ProgramArguments": [source + "/deploy/macos/run_writer_worker.sh"]}
        self.publisher_before = {"Label": deploy.base.PUBLISHER,
            "EnvironmentVariables": {"DCAR_V8_DB": str(self.database), "DCAR_WRITER_SOURCE_ROOT": self.parent["source_root"]},
            "ProgramArguments": [self.parent["source_root"] + "/deploy/macos/run_snapshot_publisher.sh"]}
        self.publisher_next = {**self.publisher_before,
            "EnvironmentVariables": {**self.publisher_before["EnvironmentVariables"], "DCAR_WRITER_SOURCE_ROOT": source},
            "ProgramArguments": [source + "/deploy/macos/run_snapshot_publisher.sh"]}
        self.writer = self.root / "installed-writer.plist"
        self.publisher = self.root / "installed-publisher.plist"
        self.writer.write_bytes(plistlib.dumps(self.writer_before))
        self.writer.chmod(0o600)
        self.publisher.write_bytes(plistlib.dumps(self.publisher_before))
        self.publisher.chmod(0o600)
        self.proposal = {"contract": "overview-install-proposal-v1", "status": "prepared",
            "parent_build": self.parent_ref, "child_build": self.child_ref,
            "formal_database": str(self.database), "database_identity": deploy.base.identity(self.database),
            "installed_plist": str(self.writer), "before_plist": self.bytes("writer.before.plist", self.writer.read_bytes()),
            "next_plist": self.bytes("writer.next.plist", plistlib.dumps(self.writer_next)),
            "publisher": {"installed_plist": str(self.publisher),
                "before_plist": self.bytes("publisher.before.plist", self.publisher.read_bytes()),
                "next_plist": self.bytes("publisher.next.plist", plistlib.dumps(self.publisher_next))},
            "inherited_catalog_proof_sha256": "a" * 64, "inherited_control_simplification_proof_sha256": "b" * 64,
            "inherited_publisher_snapshot_proof_sha256": "c" * 64, "inherited_publisher_capacity_proof_sha256": "d" * 64,
            "inherited_account_profile_proof_sha256": "e" * 64, "inherited_profile_operation_authority_sha256": "f" * 64,
            "account_profile_recovery_proof_sha256": "1" * 64, "profile_compensation_authority_sha256": "2" * 64,
            "inherited_metric_gap_proof_sha256": "3" * 64, "inherited_manual_content_scope_proof_sha256": "4" * 64,
            "inherited_account_classification_proof_sha256": "5" * 64, "inherited_daily_pipeline_proof_sha256": "6" * 64,
            "overview_proof_sha256": "9" * 64,
            "inherited_previous_daily_pipeline_proof_sha256": "7" * 64,
            "inherited_published_daily_pipeline_proof_sha256": "8" * 64,
            "database_writes": 0, "provider_calls": 0, "services_changed": False,
            "paid_gates_reopened": False, "schema_migration_repeated": False, "business_scope_change": "none"}
        self.proposal_path = self.root / "proposal.json"
        self.save_proposal()
        self.instance = deploy.Deployer(SimpleNamespace(proposal=self.proposal_path,
            output_root=self.root / "output", expected_child_build=self.child_ref["sha256"]))
        self.instance.event = Mock()
        self.instance.verify_bootstrap = Mock(return_value={"files": 1})
        self.instance.job_state = Mock(return_value={"registered": True, "pid": None})
        self.instance.stop = Mock(side_effect=AssertionError("no real or mocked service stop allowed"))
        policy = {"contract": "synthetic-policy"}
        self.inherited = {"catalog_capture_policy": policy, "catalog_capture_policy_sha256": deploy.base.digest(policy)}
        for name, digit, ancestor in (("catalog_capture_proof", "a", self.catalog_ref),
                ("control_simplification_proof", "b", self.controls_ref), ("publisher_snapshot_proof", "c", self.publisher_ref),
                ("publisher_capacity_proof", "d", self.capacity_ref), ("account_profile_proof", "e", self.profile_ref),
                ("profile_operation_authority", "f", self.profile_ref), ("account_profile_recovery_proof", "1", self.recovery_ref),
                ("profile_compensation_authority", "2", self.recovery_ref), ("metric_gap_proof", "3", self.metric_ref),
                ("manual_content_scope_proof", "4", self.manual_ref), ("proof", "5", self.classification_ref),
                ("previous_daily_pipeline_proof", "7", self.daily_v2_ref),
                ("published_daily_pipeline_proof", "8", self.published_ref), ("daily_pipeline_proof", "6", self.parent_ref),
                ("overview_proof", "9", self.child_ref)):
            self.inherited[name] = {"proof_sha256": digit * 64, "loaded_build": ancestor}
        self.inherited["previous_daily_pipeline_proof"]["parent_build"] = self.recovery_ref
        self.inherited["published_daily_pipeline_proof"]["parent_build"] = self.daily_v2_ref
        self.inherited["daily_pipeline_proof"]["parent_build"] = self.published_ref
        self.inherited["daily_pipeline_proof"]["transition"] = "daily-metric-validity-20260911-v1"
        self.inherited["overview_proof"].update(parent_build=self.parent_ref,
            contract="overview-code-successor-v1", transition="overview-four-platforms-20260912-v1")
        self.proof_names = tuple(k for k, v in self.inherited.items() if isinstance(v, dict) and "proof_sha256" in v)
        self.inherited["profile_operation_authority"]["authorization"] = copy.deepcopy(self.profile_authorization)
        for name in ("account_profile_recovery_proof", "profile_compensation_authority"):
            self.inherited[name]["authorization"] = copy.deepcopy(self.compensation_authorization)
        self.inherited["profile_compensation_authority"]["profile_authority_proof_sha256"] = "f" * 64
        self.verifier = SimpleNamespace(verify_inheritance=Mock(return_value=self.inherited))
        self.module_loader = self.enterContext(patch.object(deploy.base, "load_verified_module", return_value=self.verifier))
        self.health = {"status": "ok", "read_only": False, "database_path": str(self.database),
            "database_state": {"user_version": 21}, "runtime_database_identity": deploy.base.identity(self.database),
            "writer_lock": {"held": True}}
        self.ready = {"status": "ready", "reason": None, "control_readiness": True, "data_readiness": True,
            "paid_dispatch_state": "open", "loaded_build_id": "sha256:" + self.parent_ref["sha256"],
            "conditions": {key: True for key in ("database", "writer_lock", "writer_heartbeat", "activation", "control_readiness", "profile_day_receipt")}}
        self.enterContext(patch.object(deploy.base, "read_json_endpoint", side_effect=lambda path:
            (200, self.health) if path.endswith("health") else (200 if self.ready["status"] == "ready" else 503, self.ready)))
        self.active = {"blocking_active": {"scheduler_attempts": 0, "provider_sent": 0}, "retained_uncertainty": {"billing_unknown": 102}}
        self.enterContext(patch.object(deploy.base, "read_activity", side_effect=lambda database: self.active))

    def bytes(self, name, body):
        path = self.root / name
        path.write_bytes(body)
        path.chmod(0o600)
        return {"path": str(path), "sha256": hashlib.sha256(body).hexdigest(), "byte_size": len(body)}

    def build(self, name, payload):
        return self.bytes(name, json.dumps({"contract_version": "sealed-build-receipt-v1", "payload": payload,
            "payload_sha256": deploy.base.digest(payload)}, sort_keys=True).encode())

    def save_proposal(self):
        self.proposal_path.write_text(json.dumps(self.proposal))
        self.proposal_path.chmod(0o600)

    def test_guarded_operational_methods_are_inherited_without_replacement(self):
        for name in ("run", "stop", "replace", "backup", "rollback", "require_health", "wait_health", "verify_bootstrap", "wait_pid_exit", "launch"):
            self.assertIs(getattr(deploy.Deployer, name), getattr(deploy.base.Deployer, name))

    def test_unsupported_python_fails_before_artifact_reads_or_service_actions(self):
        before = self.database.read_bytes(), self.writer.read_bytes(), self.publisher.read_bytes()
        with patch.object(deploy.sys, "version_info", (3, 9, 6)), \
                patch.object(deploy.base, "raw") as read_artifact, \
                patch.object(self.instance, "launch") as launch, \
                patch.object(self.instance, "replace") as replace, \
                patch.object(self.instance, "backup") as backup:
            with self.assertRaisesRegex(ValueError, "requires Python 3.12"):
                self.instance.run()
            read_artifact.assert_not_called()
            launch.assert_not_called()
            replace.assert_not_called()
            backup.assert_not_called()
        self.instance.stop.assert_not_called()
        self.module_loader.assert_not_called()
        self.instance.verify_bootstrap.assert_not_called()
        self.assertEqual((self.database.read_bytes(), self.writer.read_bytes(), self.publisher.read_bytes()), before)

    def test_overview_preflight_verifies_full_proof_chain_and_preserves_configuration(self):
        before = self.database.read_bytes(), self.writer.read_bytes(), self.publisher.read_bytes()
        self.instance.preflight()
        self.assertEqual(self.instance.policy_proof()["controls_proof_sha256"], "b" * 64)
        self.assertEqual(self.instance.policy_proof()["publisher_snapshot_proof_sha256"], "c" * 64)
        self.assertEqual(self.instance.policy_proof()["publisher_capacity_proof_sha256"], "d" * 64)
        self.assertEqual(self.module_loader.call_args.args[1], "src/dcar_eval/v8/overview_release.py")
        self.assertEqual(self.instance.policy_proof()["proof_sha256"], "a" * 64)
        self.assertEqual(self.instance.policy_proof()["daily_pipeline_proof_sha256"], "6" * 64)
        self.assertEqual(self.instance.policy_proof()["previous_daily_pipeline_proof_sha256"], "7" * 64)
        self.assertNotIn("authorization", self.proposal)
        self.assertEqual(self.instance.verify_bootstrap.call_count, 2)
        self.assertEqual(self.writer_next["EnvironmentVariables"]["DCAR_KEEP_EXISTING_CAP"], "50")
        self.assertEqual((self.database.read_bytes(), self.writer.read_bytes(), self.publisher.read_bytes()), before)
        self.instance.stop.assert_not_called()

    def test_proposal_gate_change_or_old_contract_fails_before_stop(self):
        for key, value in (("contract", "account-catalog-capture-install-proposal-v1"),
                           ("contract", "daily-pipeline-install-proposal-v1"), ("paid_gates_reopened", True)):
            with self.subTest(key=key):
                original = copy.deepcopy(self.proposal)
                self.proposal[key] = value
                self.save_proposal()
                with self.assertRaisesRegex(ValueError, "Unexpected overview proposal"):
                    self.instance.run()
                self.proposal = original
        self.instance.stop.assert_not_called()

    def test_unrelated_cap_change_is_rejected(self):
        self.writer_next["EnvironmentVariables"]["DCAR_KEEP_EXISTING_CAP"] = "500"
        self.proposal["next_plist"] = self.bytes("writer.next.plist", plistlib.dumps(self.writer_next))
        self.save_proposal()
        with self.assertRaisesRegex(ValueError, "unrelated configuration"):
            self.instance.preflight()
        self.instance.stop.assert_not_called()

    def test_child_parent_and_all_ten_historical_successors_are_preserved(self):
        names = [key for key in self.parent if key.endswith("_successor")]
        self.assertEqual(len(names), 10)
        for key in ("parent", *names):
            with self.subTest(key=key):
                child = copy.deepcopy(self.child)
                if key == "parent":
                    child["overview_successor"]["parent_build"]["sha256"] = "0" * 64
                else:
                    child[key]["retained"] = "changed"
                self.proposal["child_build"] = self.build("child-build.json", child)
                self.instance.args.expected_child_build = self.proposal["child_build"]["sha256"]
                self.save_proposal()
                with self.assertRaisesRegex(ValueError, "inherited successor/schema lineage"):
                    self.instance.run()
        self.instance.stop.assert_not_called()

    def test_failed_control_readiness_or_active_work_blocks_before_stop(self):
        self.ready.update(status="not_ready", reason="current_activation_permit_missing", control_readiness=False, data_readiness=False)
        self.ready["conditions"].update(control_readiness=False, profile_day_receipt=False)
        with self.assertRaisesRegex(ValueError, "health or installed runtime"):
            self.instance.run()
        self.instance.stop.assert_not_called()
        self.ready.update(status="ready", reason=None, control_readiness=True, data_readiness=True)
        self.ready["conditions"].update(control_readiness=True, profile_day_receipt=True)
        self.active["blocking_active"]["provider_sent"] = 1
        with self.assertRaisesRegex(ValueError, "Active paid work"):
            self.instance.run()
        self.instance.stop.assert_not_called()

    def test_active_publisher_blocks_before_any_service_stop(self):
        self.instance.job_state.side_effect = lambda label: {
            "registered": True, "pid": 123 if label == deploy.base.PUBLISHER else 456}
        with self.assertRaisesRegex(ValueError, "Active Publisher must finish"):
            self.instance.run()
        self.instance.stop.assert_not_called()

    def test_old_profile_and_consumed_compensation_proofs_stay_separate(self):
        self.instance.preflight()
        self.assertEqual(self.instance.policy_proof()["profile_operation_authority_sha256"], "f" * 64)
        self.assertEqual(self.instance.policy_proof()["profile_compensation_authority_sha256"], "2" * 64)
        self.inherited["profile_operation_authority"]["loaded_build"] = self.child_ref
        with self.assertRaisesRegex(ValueError, "proof differs"):
            self.instance.preflight()
        self.instance.stop.assert_not_called()

    def test_proof_mismatch_is_rejected(self):
        self.inherited["control_simplification_proof"]["proof_sha256"] = "c" * 64
        with self.assertRaisesRegex(ValueError, "proof differs"):
            self.instance.preflight()
        self.instance.stop.assert_not_called()

    def test_inherited_failure_rollback_restores_only_paired_plists(self):
        self.instance.preflight()
        self.instance.preflight = Mock()
        self.instance.stop = Mock()
        self.instance.replace, self.instance.launch, self.instance.backup = Mock(), Mock(), Mock()
        self.instance.wait_health = Mock(side_effect=[RuntimeError("synthetic child startup failure"), {"status": "restored"}])
        runtime = SimpleNamespace(hold_formal_mutation=lambda database, **kwargs: nullcontext(SimpleNamespace(database=database)))
        before = self.database.read_bytes()
        with patch.object(deploy.base, "load_verified_module", return_value=runtime):
            with self.assertRaisesRegex(RuntimeError, "synthetic child startup failure"):
                self.instance.run()
        self.assertEqual(self.instance.backup.call_count, 1)
        self.assertEqual(self.instance.replace.call_count, 4)
        self.assertEqual([call.args[1] for call in self.instance.replace.call_args_list[-2:]], [pair[1] for pair in self.instance.pairs])
        self.assertEqual(self.database.read_bytes(), before)
        self.assertFalse(json.loads((self.instance.output / "rollback-result.json").read_text())["database_restored"])

    def test_tampered_base_script_cannot_execute(self):
        marker = self.root / "must-not-exist"
        path = self.root / "tampered.py"
        path.write_text("from pathlib import Path\nPath(" + repr(str(marker)) + ").write_text('executed')\n")
        path.chmod(0o600)
        with self.assertRaisesRegex(ValueError, "SHA256"):
            deploy._load_pinned(path, deploy.DEPLOYER_SOURCE_SHA256)
        self.assertFalse(marker.exists())


    def test_exact_installed_parent_hash_is_mandatory(self):
        with patch.object(deploy, "EXPECTED_PARENT_BUILD_SHA256", "f" * 64):
            with self.assertRaisesRegex(ValueError, "parent/child build"):
                self.instance.run()
        self.instance.stop.assert_not_called()

    def test_proof_loaded_build_bindings_cannot_be_substituted(self):
        for key in self.proof_names:
            with self.subTest(key=key):
                original = self.inherited[key]["loaded_build"]
                self.inherited[key]["loaded_build"] = {**original, "sha256": "e" * 64}
                with self.assertRaisesRegex(ValueError, "proof differs"):
                    self.instance.preflight()
                self.inherited[key]["loaded_build"] = original
        self.instance.stop.assert_not_called()

    def test_all_fifteen_proof_hashes_are_independently_bound(self):
        self.assertEqual(len(self.proof_names), 15)
        for key in self.proof_names:
            with self.subTest(key=key):
                original = self.inherited[key]["proof_sha256"]
                self.inherited[key]["proof_sha256"] = "0" * 64
                with self.assertRaisesRegex(ValueError, "proof differs"):
                    self.instance.preflight()
                self.inherited[key]["proof_sha256"] = original
        self.instance.stop.assert_not_called()

    def test_old_proofs_cannot_be_rebound_to_capacity_or_publisher_parent(self):
        cases = (("control_simplification_proof", self.parent_ref),
                 ("control_simplification_proof", self.child_ref),
                 ("publisher_snapshot_proof", self.child_ref),
                 ("publisher_snapshot_proof", self.controls_ref),
                 ("publisher_capacity_proof", self.child_ref))
        for key, wrong_ref in cases:
            with self.subTest(proof=key, build=wrong_ref["path"]):
                original = self.inherited[key]["loaded_build"]
                self.inherited[key]["loaded_build"] = wrong_ref
                with self.assertRaisesRegex(ValueError, "proof differs"):
                    self.instance.preflight()
                self.inherited[key]["loaded_build"] = original
        self.instance.stop.assert_not_called()

    def test_old_publisher_proposal_is_not_a_daily_pipeline_proposal(self):
        self.proposal["contract"] = "publisher-snapshot-install-proposal-v1"
        self.save_proposal()
        with self.assertRaisesRegex(ValueError, "Unexpected overview proposal"):
            self.instance.run()
        self.instance.stop.assert_not_called()

    def test_daily_verifier_failure_blocks_before_operational_actions(self):
        self.verifier.verify_inheritance.side_effect = ValueError("synthetic capacity proof failure")
        before = self.database.read_bytes(), self.writer.read_bytes(), self.publisher.read_bytes()
        with self.assertRaisesRegex(ValueError, "capacity proof failure"):
            self.instance.run()
        self.assertEqual((self.database.read_bytes(), self.writer.read_bytes(), self.publisher.read_bytes()), before)
        self.instance.stop.assert_not_called()

    def test_coverage_gap_is_accepted_without_claiming_data_restoration(self):
        self.ready.update(status="not_ready", reason="current_activation_coverage_incomplete", data_readiness=False)
        self.ready["conditions"]["profile_day_receipt"] = False
        self.instance.preflight()
        receipt = json.loads((self.instance.output / "preflight.json").read_text())
        self.assertEqual(receipt["readiness_acceptance"]["classification"], "accepted_data_gap")
        self.assertFalse(receipt["readiness_acceptance"]["actual_data_restoration_claimed"])
        self.instance.stop.assert_not_called()

    def _stopped_run(self):
        self.instance.preflight()
        self.instance.preflight = Mock()
        self.instance.stop = Mock()
        self.instance.replace, self.instance.backup, self.instance.rollback = Mock(), Mock(), Mock()
        runtime = SimpleNamespace(hold_formal_mutation=lambda database, **kwargs:
            nullcontext(SimpleNamespace(database=database)))
        return patch.object(deploy.base, "load_verified_module", return_value=runtime)

    def test_activity_that_appears_during_stop_prevents_backup_and_switch(self):
        context = self._stopped_run()
        self.active["blocking_active"]["provider_sent"] = 1
        with context, self.assertRaisesRegex(ValueError, "Work became active during stop"):
            self.instance.run()
        self.assertEqual([call.args[0] for call in self.instance.stop.call_args_list], [deploy.base.PUBLISHER, deploy.base.WRITER])
        self.instance.backup.assert_not_called()
        self.instance.replace.assert_not_called()
        self.instance.rollback.assert_called_once()

    def test_plist_change_during_stop_prevents_backup_and_switch(self):
        context = self._stopped_run()
        self.writer.write_bytes(plistlib.dumps({**self.writer_before, "Changed": True}))
        with context, self.assertRaisesRegex(ValueError, "plist changed while stopping"):
            self.instance.run()
        self.instance.backup.assert_not_called()
        self.instance.replace.assert_not_called()
        self.instance.rollback.assert_called_once()

    def test_inherited_read_activity_is_read_only_and_covers_live_owner_boundaries(self):
        query = Mock()
        query.fetchone.return_value = (0,)
        connection = Mock()
        connection.execute.return_value = query
        manager = Mock()
        manager.__enter__ = Mock(return_value=connection)
        manager.__exit__ = Mock(return_value=None)
        with patch.object(deploy.base.sqlite3, "connect", return_value=manager) as connect:
            report = READ_ACTIVITY(self.database)
        self.assertIn("?mode=ro", connect.call_args.args[0])
        sql = [call.args[0] for call in connection.execute.call_args_list]
        self.assertEqual(sql[0], "PRAGMA query_only=ON")
        self.assertIn("scheduler_attempts", report["blocking_active"])
        self.assertIn("uncertain_with_live_owner", report["blocking_active"])
        self.assertIn("billing_unknown", report["retained_uncertainty"])
        self.assertFalse(any(text.startswith(("UPDATE", "INSERT", "DELETE")) for text in sql))

    def test_consumed_authorizations_cannot_be_changed_or_renewed(self):
        for key in ("profile_operation_authority", "profile_compensation_authority", "account_profile_recovery_proof"):
            for field, value in (("sha256", "0" * 64), ("expires_at", "2099-01-01T00:00:00Z")):
                with self.subTest(key=key, field=field):
                    original = copy.deepcopy(self.inherited[key]["authorization"])
                    self.inherited[key]["authorization"][field] = value
                    with self.assertRaisesRegex(ValueError, "authorization changed"):
                        self.instance.run()
                    self.inherited[key]["authorization"] = original
        self.instance.stop.assert_not_called()

    def test_new_proposal_or_successor_authorization_is_rejected(self):
        self.proposal["authorization"] = self.compensation_authorization
        self.save_proposal()
        with self.assertRaisesRegex(ValueError, "Unexpected overview proposal"):
            self.instance.run()
        del self.proposal["authorization"]
        child = copy.deepcopy(self.child)
        child["overview_successor"]["authorization"] = self.compensation_authorization
        self.proposal["child_build"] = self.build("child-build.json", child)
        self.instance.args.expected_child_build = self.proposal["child_build"]["sha256"]
        self.save_proposal()
        with self.assertRaisesRegex(ValueError, "inherited successor/schema lineage"):
            self.instance.run()
        self.instance.stop.assert_not_called()

    def test_compensation_remains_bound_to_original_profile_authority(self):
        self.inherited["profile_compensation_authority"]["profile_authority_proof_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "authorization changed"):
            self.instance.run()
        self.instance.stop.assert_not_called()

    def test_all_proofs_reject_every_other_generation(self):
        ancestors = (self.classification_ref, self.manual_ref, self.catalog_ref, self.metric_ref, self.controls_ref,
                     self.publisher_ref, self.capacity_ref, self.profile_ref, self.recovery_ref, self.daily_v2_ref, self.published_ref, self.parent_ref, self.child_ref)
        for key in self.proof_names:
            original = self.inherited[key]["loaded_build"]
            for other in ancestors:
                if other == original:
                    continue
                with self.subTest(proof=key, wrong_generation=other["path"]):
                    self.inherited[key]["loaded_build"] = other
                    with self.assertRaisesRegex(ValueError, "proof differs"):
                        self.instance.preflight()
            self.inherited[key]["loaded_build"] = original
        self.instance.stop.assert_not_called()

    def test_changed_publisher_configuration_is_rejected(self):
        changed = copy.deepcopy(self.publisher_next)
        changed["EnvironmentVariables"]["UNRELATED_NEW_AUTHORITY"] = "1"
        self.proposal["publisher"]["next_plist"] = self.bytes("publisher.next.plist", plistlib.dumps(changed))
        self.save_proposal()
        with self.assertRaisesRegex(ValueError, "unrelated configuration"):
            self.instance.run()
        self.instance.stop.assert_not_called()

    def test_missing_historical_successor_blocks_before_stop(self):
        for key in [name for name in self.parent if name.endswith("_successor")]:
            child = copy.deepcopy(self.child)
            del child[key]
            self.proposal["child_build"] = self.build("child-build.json", child)
            self.instance.args.expected_child_build = self.proposal["child_build"]["sha256"]
            self.save_proposal()
            with self.subTest(successor=key), self.assertRaisesRegex(ValueError, "inherited successor/schema lineage"):
                self.instance.run()
        self.instance.stop.assert_not_called()

    def test_missing_proof_blocks_before_stop(self):
        for name in self.proof_names:
            proof = self.inherited.pop(name)
            with self.subTest(proof=name), self.assertRaises(KeyError):
                self.instance.run()
            self.inherited[name] = proof
        self.instance.stop.assert_not_called()

    def test_reviewed_real_parent_and_its_previous_parent_are_pinned(self):
        self.assertEqual(REAL_PARENT_SHA256, "e5d53ba78459301648fbe080aa26beff810b135edd2f91021ea9bbc1f51931e8")
        self.assertEqual(REAL_PUBLISHED_DAILY_SHA256, "f5851e8c94031f7d5bd56784840b45fb57eb83df344a1203c5cee9b3eeb92005")
        self.assertEqual(REAL_PREVIOUS_PARENT_SHA256, "7f83f8b695f6f7f4e7eb474ec04c4667718c0f3e5c46f9364459292e2484b9cb")

    def test_previous_daily_parent_cannot_skip_the_recovery_generation(self):
        with patch.object(deploy, "EXPECTED_PREVIOUS_PARENT_BUILD_SHA256", self.parent_ref["sha256"]):
            with self.assertRaisesRegex(ValueError, "proof parent/transition differs"):
                self.instance.run()
        self.instance.stop.assert_not_called()

    def test_previous_and_current_daily_proofs_preserve_distinct_parents(self):
        self.instance.preflight()
        for name, original, wrong in (
                ("previous_daily_pipeline_proof", self.recovery_ref, self.parent_ref),
                ("previous_daily_pipeline_proof", self.recovery_ref, self.child_ref),
                ("published_daily_pipeline_proof", self.daily_v2_ref, self.recovery_ref),
                ("published_daily_pipeline_proof", self.daily_v2_ref, self.child_ref),
                ("daily_pipeline_proof", self.published_ref, self.recovery_ref),
                ("daily_pipeline_proof", self.published_ref, self.child_ref)):
            with self.subTest(proof=name, wrong=wrong["path"]):
                self.inherited[name]["parent_build"] = wrong
                with self.assertRaisesRegex(ValueError, "proof parent/transition differs|proof differs"):
                    self.instance.preflight()
                self.inherited[name]["parent_build"] = original
        self.instance.stop.assert_not_called()

    def test_proposal_cannot_overwrite_any_inherited_source_plan_proof(self):
        for field in self.proof_fields:
            with self.subTest(field=field):
                changed = copy.deepcopy(self.child)
                changed["overview_successor"][field] = "0" * 64
                child_ref = self.build("child-build.json", changed)
                self.proposal["child_build"] = child_ref
                self.instance.args.expected_child_build = child_ref["sha256"]
                self.inherited["overview_proof"]["loaded_build"] = child_ref
                self.save_proposal()
                with self.assertRaisesRegex(ValueError, "proposal and source plan proof differ"):
                    self.instance.run()
        self.instance.stop.assert_not_called()

    def test_profile_recovery_and_compensation_remain_on_e994_generation(self):
        self.instance.preflight()
        for name in ("account_profile_recovery_proof", "profile_compensation_authority"):
            self.assertEqual(self.inherited[name]["loaded_build"], self.recovery_ref)
            for wrong in (self.parent_ref, self.child_ref):
                with self.subTest(proof=name, wrong=wrong["path"]):
                    self.inherited[name]["loaded_build"] = wrong
                    with self.assertRaisesRegex(ValueError, "proof differs"):
                        self.instance.run()
            self.inherited[name]["loaded_build"] = self.recovery_ref
        self.instance.stop.assert_not_called()

    def test_code_only_build_rejects_unrelated_root_and_operator_field_changes(self):
        for scope in ("root", "generation"):
            changed = copy.deepcopy(self.child)
            if scope == "root":
                changed["unrelated_authority"] = {"enabled": True}
            else:
                changed["account_cleanup_generation"]["operator_approval"] = "changed"
            self.proposal["child_build"] = self.build("child-build.json", changed)
            self.instance.args.expected_child_build = self.proposal["child_build"]["sha256"]
            self.save_proposal()
            with self.subTest(scope=scope), self.assertRaisesRegex(ValueError, "non-code build fields"):
                self.instance.run()
        self.instance.stop.assert_not_called()

    def test_scheduler_and_catchup_flags_cannot_change(self):
        for key in ("DCAR_SCHEDULER_START_PAUSED", "DCAR_STARTUP_CATCHUP_ENABLED", "DCAR_SCHEDULER_ENABLED"):
            changed = copy.deepcopy(self.writer_next)
            changed["EnvironmentVariables"][key] = "1"
            self.proposal["next_plist"] = self.bytes("writer.next.plist", plistlib.dumps(changed))
            self.save_proposal()
            with self.subTest(flag=key), self.assertRaisesRegex(ValueError, "unrelated configuration"):
                self.instance.run()
        self.instance.stop.assert_not_called()

    def test_running_capture_and_scheduler_owners_block_even_without_provider_requests(self):
        for key in ("scheduler_runs", "scheduler_attempts", "capture_work_items", "uncertain_with_live_owner"):
            self.active["blocking_active"] = {"provider_reserved": 0, "provider_sent": 0, key: 1}
            with self.subTest(owner=key), self.assertRaisesRegex(ValueError, "Active paid work"):
                self.instance.run()
            (self.instance.output / "preflight.json").unlink()
        self.instance.stop.assert_not_called()

    def test_external_boundaries_are_explicitly_denied(self):
        for operation in (lambda: sqlite3.connect("/formal-db-must-not-open.sqlite3"),
                          lambda: urllib.request.urlopen("https://provider.invalid/paid"),
                          lambda: subprocess.run(["must-not-run"]), lambda: socket.create_connection(("provider.invalid", 443))):
            with self.assertRaisesRegex(AssertionError, "Forbidden external boundary"):
                operation()


if __name__ == "__main__":
    unittest.main()
