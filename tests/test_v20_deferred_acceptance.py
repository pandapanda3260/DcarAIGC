"""User-deferred E2E is explicit; real offline technical evidence stays required."""
from __future__ import annotations

import copy
import hashlib
import json
import os
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from tests import test_issue_v20_deployment_receipt as fixture
from v8 import capture_release
from v8.profile_activations import activation_at
from v8.runtime_database import RuntimeDatabaseError

AT = fixture.AT
issuer = fixture.issuer


class DeferredAcceptanceTest(unittest.TestCase):
    @contextmanager
    def ready(self):
        paired = fixture.CandidateIssuerTest(methodName="runTest")
        with paired.ready_pair() as (connection, candidate_args, layout, logs):
            candidate = issuer.issue_candidate(connection, **candidate_args)
            connection.commit()
            connection.execute("BEGIN IMMEDIATE")
            project = layout["project"]
            root = layout["migration_receipt"].parent
            build = issuer.sealer._read_receipt(candidate_args["preinstall_build_receipt"],
                                               contract_version=issuer.sealer.SEALED_BUILD_CONTRACT)
            post_runtime = issuer.sealer._read_receipt(layout["runtime20_receipt"],
                contract_version=issuer.sealer.RUNTIME_ROOT_CONTRACT)
            build.update(installed_runtime=post_runtime["installed_runtime"], runtime_root_receipt={
                "path": str(layout["runtime20_receipt"]), "sha256": issuer.sealer._sha256_file(layout["runtime20_receipt"])})
            build.update(schema_contract=issuer.sealer._schema_contract(20, 20), deployment_readiness=candidate,
                postmigration_lineage={"migration_receipt": candidate["evidence"]["migration"],
                    "install_receipt": candidate["evidence"]["install"],
                    "previous_build_receipt": {"path": str(candidate_args["preinstall_build_receipt"]),
                        "sha256": candidate["bindings"]["build_sha256"]}})
            loaded = root / "loaded-postinstall.json"
            loaded.write_text(json.dumps(issuer.sealer._envelope(issuer.sealer.SEALED_BUILD_CONTRACT, build)))
            loaded.chmod(0o600)
            evidence = {"deployment": candidate, "active": activation_at(connection, AT),
                "build_sha256": issuer.sealer._sha256_file(loaded),
                "runtime_sha256": build["runtime_root_receipt"]["sha256"],
                "config_sha256": candidate["bindings"]["config_sha256"],
                "manifest": issuer.forward_recovery._route()}
            args = {"deployment_id": "deferred-test", "candidate_id": candidate["deployment_id"],
                "operations": ["xiaohongshu_user_posts", "douyin_user_posts"],
                "actor": "explicit-test-user", "reason": "Defer business E2E and approve production rollout",
                "decision_receipt_path": root / "user-release-decision.json", "at": AT}
            # Only the OS-installed process boundary is isolated. The real
            # Writer lease, migrated DB, files, pair and validator are exercised.
            with patch.object(capture_release, "PROJECT_ROOT", project), patch.object(
                capture_release, "_installed_evidence", return_value=evidence,
            ), patch.dict(os.environ, {"DCAR_LOADED_BUILD_RECEIPT": str(loaded)}):
                yield connection, args, evidence, project, logs

    def test_explicit_decision_accepts_without_e2e_qualification_or_paid_gate(self):
        with self.ready() as (connection, args, _, project, _), patch.object(
            issuer, "_actual_e2e", side_effect=AssertionError("deferred acceptance must not purchase/prove E2E"),
        ):
            result = issuer.issue_deferred_acceptance(connection, **args)
            self.assertEqual(result["status"], "accepted")
            self.assertEqual(result["e2e_status"], "deferred")
            self.assertTrue(result["deployment_eligible"])
            self.assertFalse(result["coverage_complete"])
            self.assertFalse(result["ordinary_paid_authorized"])
            decision = result["release_decision"]
            self.assertEqual(decision["business_e2e"], "deferred_by_user")
            self.assertEqual(decision["transport_qualification"], "not_verified")
            self.assertEqual(decision["approved_target_profile"], "integrated_route_v1")
            self.assertEqual(decision["operations"], sorted(args["operations"]))
            path = args["decision_receipt_path"]
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(decision["decision_sha256"], hashlib.sha256(path.read_bytes()).hexdigest())
            self.assertNotIn("bounded_e2e", result["evidence"])
            for table in ("provider_usage", "fetch_attempts", "capture_paid_send_gate_events", "transport_continuity_permits"):
                self.assertEqual(connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0], 0)
            self.assertEqual(issuer.contract.validate_deployment_receipt(connection,
                require_accepted=True, project_root=project)["release_decision"], decision)

    def _reseal_tests(self, evidence, *, mutate=None):
        """Use real independent envelopes and hashes, not a mocked validator."""
        loaded = Path(os.environ["DCAR_LOADED_BUILD_RECEIPT"])
        build = issuer.sealer._read_receipt(loaded, contract_version=issuer.sealer.SEALED_BUILD_CONTRACT)
        original_ref = build["test_results_receipt"]
        tests = issuer.sealer._read_receipt(Path(original_ref["path"]), contract_version=issuer.sealer.TEST_RESULTS_CONTRACT)
        if mutate is not None:
            mutate(tests)
        post_tests = loaded.parent / "postseal-independent-tests.json"
        with patch.object(issuer.sealer, "_utc_now", return_value="2026-09-06T00:00:02Z"):
            envelope = issuer.sealer._envelope(issuer.sealer.TEST_RESULTS_CONTRACT, tests)
        post_tests.write_text(json.dumps(envelope))
        post_tests.chmod(0o600)
        post_ref = {"path": str(post_tests), "sha256": issuer.sealer._sha256_file(post_tests)}
        self.assertNotEqual(post_ref["sha256"], original_ref["sha256"])
        build["test_results_receipt"] = post_ref
        loaded.write_text(json.dumps(issuer.sealer._envelope(issuer.sealer.SEALED_BUILD_CONTRACT, build)))
        evidence["build_sha256"] = issuer.sealer._sha256_file(loaded)
        return post_tests

    def test_distinct_valid_test_envelopes_for_same_real_logs_accept(self):
        with self.ready() as (connection, args, evidence, _, _):
            self._reseal_tests(evidence)
            result = issuer.issue_deferred_acceptance(connection, **args)
            self.assertEqual(result["e2e_status"], "deferred")
            self.assertFalse(result["ordinary_paid_authorized"])
            self.assertEqual(connection.execute("SELECT count(*) FROM provider_usage").fetchone()[0], 0)

    def test_runtime_producer_contract_has_no_status_and_rejects_real_invariant_changes(self):
        for change in (None, "old_fake_status", "mutation", "schema", "integrity", "root", "inode"):
            with self.subTest(change=change), self.ready() as (connection, args, evidence, _, _):
                loaded = Path(os.environ["DCAR_LOADED_BUILD_RECEIPT"])
                build = issuer.sealer._read_receipt(loaded, contract_version=issuer.sealer.SEALED_BUILD_CONTRACT)
                runtime_path = Path(build["runtime_root_receipt"]["path"])
                body = issuer.sealer._read_receipt(runtime_path, contract_version=issuer.sealer.RUNTIME_ROOT_CONTRACT)
                self.assertNotIn("status", body)
                self.assertEqual(body["action"], "retain")
                self.assertEqual(body["formal_database"]["user_version"], 20)
                self.assertEqual(body["formal_database"]["quick_check"], ["ok"])
                if change == "old_fake_status":
                    body = {"status": "succeeded"}
                elif change == "mutation":
                    body["mutation_counts"]["deleted_files"] = 1
                elif change == "schema":
                    body["formal_database"]["user_version"] = 19
                elif change == "integrity":
                    body["formal_database"]["integrity_check"] = ["corrupt"]
                elif change == "root":
                    body["roots"]["raw"]["identity"]["path"] = "/wrong-root"
                elif change == "inode":
                    body["formal_database"]["inode"] += 1
                if change is not None:
                    runtime_path.write_text(json.dumps(issuer.sealer._envelope(issuer.sealer.RUNTIME_ROOT_CONTRACT, body)))
                    checksum = issuer.sealer._sha256_file(runtime_path)
                    build["runtime_root_receipt"]["sha256"] = checksum
                    loaded.write_text(json.dumps(issuer.sealer._envelope(issuer.sealer.SEALED_BUILD_CONTRACT, build)))
                    evidence.update(runtime_sha256=checksum, build_sha256=issuer.sealer._sha256_file(loaded))
                    with self.assertRaises(issuer.contract.ReleaseContractError):
                        issuer.issue_deferred_acceptance(connection, **args)
                else:
                    result = issuer.issue_deferred_acceptance(connection, **args)
                    self.assertEqual(result["status"], "accepted")
                    self.assertEqual(result["e2e_status"], "deferred")

    def test_resealed_test_content_log_hash_or_actual_log_drift_reject(self):
        for change in ("source", "log_hash", "actual_log", "different_valid_log", "receipt_hash"):
            with self.subTest(change=change), self.ready() as (connection, args, evidence, _, logs):
                def mutate(tests):
                    if change == "source":
                        tests["git"]["head"] = "f" * 40
                    if change == "log_hash":
                        tests["results"]["backend"]["sha256"] = "f" * 64
                    if change == "different_valid_log":
                        other = logs["backend"].parent / "other-backend.log"
                        other.write_text("different isolated parser evidence\nDCAR_TEST_RESULT name=backend exit=0\n")
                        other.chmod(0o600)
                        tests["results"]["backend"] = issuer.sealer._test_log_record("backend", other)
                post_tests = self._reseal_tests(evidence, mutate=mutate)
                if change == "actual_log":
                    logs["backend"].write_text("changed isolated parser evidence\nDCAR_TEST_RESULT name=backend exit=0\n")
                if change == "receipt_hash":
                    post_tests.write_bytes(post_tests.read_bytes() + b"\n")
                with self.assertRaises(issuer.contract.ReleaseContractError):
                    issuer.issue_deferred_acceptance(connection, **args)
                self.assertEqual(connection.execute("SELECT count(*) FROM deployment_readiness_receipts WHERE status='accepted'").fetchone()[0], 0)

    def test_immutable_decision_recovers_outer_rollback_and_exact_retry_only(self):
        with self.ready() as (connection, args, evidence, _, _):
            result = issuer.issue_deferred_acceptance(connection, **args)
            original = args["decision_receipt_path"].read_bytes()
            connection.rollback()
            connection.execute("BEGIN IMMEDIATE")
            retry = {**args, "at": "2026-09-06T00:00:01Z"}
            recovered = issuer.issue_deferred_acceptance(connection, **retry)
            self.assertEqual(recovered["release_decision"], result["release_decision"])
            self.assertEqual(args["decision_receipt_path"].read_bytes(), original)
            evidence["deployment"] = recovered
            self.assertTrue(issuer.issue_deferred_acceptance(connection, **retry)["idempotent"])
            for change in ({"actor": "another-user"}, {"reason": "another-decision"},
                           {"operations": ["douyin_user_posts"]}, {"candidate_id": "another-candidate"}):
                with self.subTest(change=change), self.assertRaises(issuer.contract.ReleaseContractError):
                    issuer.issue_deferred_acceptance(connection, **{**retry, **change})
            evidence["build_sha256"] = "f" * 64
            with self.assertRaisesRegex(issuer.contract.ReleaseContractError, "another user decision or runtime"):
                issuer.issue_deferred_acceptance(connection, **retry)
            self.assertEqual(connection.execute("SELECT count(*) FROM deployment_readiness_receipts WHERE status='accepted'").fetchone()[0], 1)

    def test_default_path_missing_decision_and_mixed_evidence_still_reject(self):
        with self.ready() as (connection, args, _, project, _):
            issuer.issue_deferred_acceptance(connection, **args)
            payload = json.loads(connection.execute("SELECT payload_json FROM deployment_readiness_receipts WHERE status='accepted'").fetchone()[0])
            connection.rollback()
            connection.execute("BEGIN IMMEDIATE")
            for mutate in (
                lambda p: p.pop("acceptance_mode"),
                lambda p: p["evidence"].pop("release_decision"),
                lambda p: p["evidence"].update(bounded_e2e=p["evidence"]["release_decision"]),
                lambda p: p.update(native_cohort_id=200),
                lambda p: p["evidence"]["release_decision"].update(result="passed"),
            ):
                changed = copy.deepcopy(payload)
                mutate(changed)
                with self.assertRaises(issuer.contract.ReleaseContractError):
                    issuer._append(connection, deployment_id="invalid", status="accepted", payload=changed, at=AT, project_root=project)
            default = copy.deepcopy(payload)
            default.pop("acceptance_mode")
            default["evidence"].pop("release_decision")
            with self.assertRaises(issuer.contract.ReleaseContractError):
                issuer._append(connection, deployment_id="default", status="accepted", payload=default, at=AT, project_root=project)
            self.assertEqual(connection.execute("SELECT count(*) FROM deployment_readiness_receipts WHERE status='accepted'").fetchone()[0], 0)

    def test_decision_tamper_wrong_candidate_runtime_and_future_reject(self):
        with self.ready() as (connection, args, _, project, _):
            issuer.issue_deferred_acceptance(connection, **args)
            payload = json.loads(connection.execute("SELECT payload_json FROM deployment_readiness_receipts WHERE status='accepted'").fetchone()[0])
            path = args["decision_receipt_path"]
            original = json.loads(path.read_text())
            connection.rollback()
            connection.execute("BEGIN IMMEDIATE")
            for mutate in (
                lambda d: d.update(candidate_id="other"),
                lambda d: d.update(candidate_receipt_sha256="f" * 64),
                lambda d: d["bindings"].update(activation_id=999),
                lambda d: d["runtime_bindings"].update(build_sha256="f" * 64),
                lambda d: d["runtime_bindings"].update(config_sha256="f" * 64),
                lambda d: d.update(issued_at="2026-09-07T00:00:00Z"),
                lambda d: d.update(transport_qualification="passed"),
                lambda d: d.update(operations=[]),
                lambda d: d["transport_manifest"].update(route_generation=999),
            ):
                changed = copy.deepcopy(original)
                mutate(changed)
                path.write_text(json.dumps(changed))
                copied = copy.deepcopy(payload)
                # Recompute the file ref: semantic evidence binding, not only
                # mismatched bytes, must still reject the forged body.
                copied["evidence"]["release_decision"]["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
                with self.assertRaises(issuer.contract.ReleaseContractError):
                    issuer._append(connection, deployment_id="invalid", status="accepted", payload=copied, at=AT, project_root=project)
            with self.assertRaisesRegex(issuer.contract.ReleaseContractError, "SHA-256 differs"):
                issuer._append(connection, deployment_id="tampered", status="accepted", payload=payload, at=AT, project_root=project)

    def test_technical_evidence_and_writer_guard_are_not_deferred(self):
        with self.ready() as (connection, args, _, _, logs):
            with patch.object(issuer, "require_current_process_writer_lock", side_effect=RuntimeDatabaseError("lease lost")):
                with self.assertRaisesRegex(RuntimeDatabaseError, "lease lost"):
                    issuer.issue_deferred_acceptance(connection, **args)
            self.assertFalse(args["decision_receipt_path"].exists())
            logs["backend"].write_text("real negative parser fixture\nDCAR_TEST_RESULT name=backend exit=1\n")
            with self.assertRaises(issuer.contract.ReleaseContractError):
                issuer.issue_deferred_acceptance(connection, **args)
            self.assertEqual(connection.execute("SELECT count(*) FROM deployment_readiness_receipts WHERE status='accepted'").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
