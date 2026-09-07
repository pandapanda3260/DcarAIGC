"""Code-only successor chains keep the original release authority immutable."""
from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src/dcar_eval"))
from v8 import capture_code_successor as successor
from v8.transport_receipts import _digest as transport_digest


def digest(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def reference(label: str) -> dict:
    return {"path": "/private/evidence/" + label, "sha256": digest(label), "byte_size": 100}


class SuccessorBoundaryTests(unittest.TestCase):
    def delta(self, before: dict, after: dict, **kwargs):
        old = {"git": {"head": "h"}, "source_archive": {"sha256": "old"}}
        new = {"git": {"head": "h"}, "source_archive": {"sha256": "new"}}
        with patch.object(successor.forward_recovery, "_successor_archive", side_effect=[before, after]), patch.object(
            successor.subprocess, "run"
        ) as show:
            show.return_value.stdout = b""
            return successor._source_changes(Path("/tmp"), old, new, **kwargs)

    def test_v2_runtime_and_reviewed_tests_are_bounded_to_exact_hashes(self):
        before = {successor.BUSINESS: b"before", successor.PIPELINE: b"same"}
        after = {**before, successor.BUSINESS: b"heartbeat", successor.BATCHES: b"shared-heartbeat",
                 next(iter(successor.V2_TESTS)): b"test"}
        delta = self.delta(before, after, version=2, parent_delta=True)
        self.assertEqual(delta[successor.BUSINESS], {"before_sha256": digest("before"), "after_sha256": digest("heartbeat")})
        self.assertNotIn(successor.PIPELINE, delta)
        self.assertEqual(delta[successor.BATCHES]["after_sha256"], digest("shared-heartbeat"))

    def test_v2_rejects_transport_budget_auth_or_arbitrary_test_changes(self):
        before = {successor.BUSINESS: b"before"}
        for name in ("src/dcar_eval/v8/provider_transport.py", "src/dcar_eval/v8/provider_budget.py",
                     "src/dcar_eval/v8/capture_authorizations.py", "tests/unreviewed.py"):
            with self.subTest(name=name), self.assertRaises(successor.auth.AuthorizationError):
                self.delta(before, {successor.BUSINESS: b"after", name: b"changed"}, version=2, parent_delta=True)

    def test_v2_requires_runtime_change_and_forbids_deletion(self):
        with self.assertRaises(successor.auth.AuthorizationError):
            self.delta({successor.BUSINESS: b"same"}, {successor.BUSINESS: b"same"}, version=2, parent_delta=True)
        with self.assertRaises(successor.auth.AuthorizationError):
            self.delta({successor.BUSINESS: b"before"}, {}, version=2, parent_delta=True)

    def test_legacy_parent_still_rejects_runtime_or_new_tests(self):
        for after in ({successor.BUSINESS: b"after"},
                      {successor.BUSINESS: b"after", next(iter(successor.V2_TESTS)): b"test"}):
            with self.assertRaises(successor.auth.AuthorizationError):
                self.delta({successor.BUSINESS: b"before"}, after, parent_delta=True)

    def test_prepare_requires_the_actual_installed_parent_before_creating_evidence(self):
        for installed in (None, Path("/private/evidence/other-build")):
            with self.subTest(installed=installed), patch(
                "v8.runtime_database.require_current_process_writer_lock"
            ), patch.object(successor, "_tools", return_value=(object(), object())), patch.object(
                successor, "_installed_build_path", return_value=installed
            ), patch.object(successor, "_ref", side_effect=lambda path: reference(path.name)), patch.object(
                successor, "_accepted"
            ) as accepted, self.assertRaises(successor.auth.AuthorizationError):
                successor.prepare_plan(None, project_root=Path("/tmp"), previous_build=Path("/private/evidence/selected-build"),
                    evidence_dir=Path("/private/evidence/uncreated"), tests={}, actor="operator", reason="reviewed", at="2026-09-07T00:00:00Z")
            accepted.assert_not_called()

    def test_cycle_and_depth_are_rejected_and_context_is_reset(self):
        with successor._chain_link("one"):
            with self.assertRaises(successor.auth.AuthorizationError):
                with successor._chain_link("one"):
                    pass
        self.assertEqual(successor._CHAIN.get(), ())
        token = successor._CHAIN.set(tuple(str(i) for i in range(successor.MAX_CHAIN_DEPTH)))
        try:
            with self.assertRaises(successor.auth.AuthorizationError):
                with successor._chain_link("too-deep"):
                    pass
        finally:
            successor._CHAIN.reset(token)


class PortableSuccessorTests(unittest.TestCase):
    """Use a real receipt ledger; stub only unrelated activation/permit checks."""
    def setUp(self):
        self.connection = sqlite3.connect(":memory:")
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("CREATE TABLE scheduler_runs(id INTEGER PRIMARY KEY, job_id TEXT, status TEXT, details_json TEXT)")
        self.addCleanup(self.connection.close)
        self.active = {"activation_id": 3, "profile_id": "integrated_route_v1", "roster_snapshot_id": 2,
                       "roster_members_sha256": digest("roster"), "activation_sha256": digest("active")}
        self.control = {"id": 4, "event_hash": digest("release")}
        self.origin = {"build_sha256": digest("origin"), "runtime_sha256": digest("origin-runtime"), "config_sha256": digest("config")}
        self.manifest = {"manifest_sha256": digest("route")}
        self.deployment = {"status": "accepted", "deployment_id": "original", "receipt_sha256": digest("accepted"),
            "release_decision": {"operations": ["douyin_user_posts"], "decision_sha256": digest("original-decision"),
                "runtime_bindings": self.origin, "transport_manifest": self.manifest}}
        self.addCleanup(patch.stopall)
        patch.object(successor, "_current_control", return_value=(self.active, self.control)).start()
        patch.object(successor, "activation_at", return_value=self.active).start()
        patch("v8.capture_activation_release.validate_installed_activation_successor").start()
        self.sequence = 0
        self.first = self.make("first")
        self.second = self.make("pipeline", self.first, pipeline=True)
        self.third = self.make("lease", self.second)

    def seal(self, proof):
        plan = proof["plan_payload"]
        plan_body = (json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()
        proof["plan_reference"].update(sha256=hashlib.sha256(plan_body).hexdigest(), byte_size=len(plan_body))
        refs = {"plan": proof["plan_reference"], "previous_build": plan.get("installed_parent", plan["previous_build"]),
                "source_archive": plan["source_archive"], "full_checks": plan["full_checks"],
                "build": proof["build_reference"], "runtime": proof["runtime_reference"]}
        proof["private_references"] = [{"role": role, **value} for role, value in refs.items()]
        self.sequence += 1
        at = "2026-09-07T02:00:00Z"
        payload = successor._decision_payload(plan, plan_sha=proof["plan_reference"]["sha256"],
            build_sha=proof["build_reference"]["sha256"], runtime_sha=proof["runtime_reference"]["sha256"], at=at)
        receipt = {"receipt_id": self.sequence, "recorded_at": at, "payload": payload, "payload_sha256": transport_digest(payload)}
        receipt["self_sha256"] = transport_digest(receipt)
        proof["decision_receipt"] = receipt
        self.connection.execute("DELETE FROM scheduler_runs WHERE json_extract(details_json,'$.payload.build_sha256')=?", (proof["build_reference"]["sha256"],))
        self.connection.execute("INSERT INTO scheduler_runs VALUES(?,?,?,?)", (self.sequence, "transport_receipt:campaign_terminal", "succeeded", json.dumps(receipt)))
        proof["proof_sha256"] = successor.auth.digest({k: v for k, v in proof.items() if k != "proof_sha256"})
        return proof

    def make(self, label, parent=None, pipeline=False):
        version = 2 if parent is not None and not pipeline else 1
        plan = {"contract": successor.PLAN_V2 if version == 2 else successor.PLAN,
            "business_module": successor.BUSINESS, "plumbing_allowlist": sorted(successor.PLUMBING),
            "source_deployment": {"deployment_id": "original", "receipt_sha256": self.deployment["receipt_sha256"]},
            "source_decision_sha256": self.deployment["release_decision"]["decision_sha256"],
            "previous_build": reference("original-build"), "source_archive": reference(label + "-source"),
            "full_checks": reference(label + "-tests"), "operations": ["douyin_user_posts"], "manifest": self.manifest,
            "active": self.active, "release": self.control, "actor": "operator", "reason": "reviewed bounded fix",
            "issued_at": "2026-09-07T01:00:00Z", "changes": {successor.BUSINESS: {"before_sha256": digest("original-source"), "after_sha256": digest("first-source")}}}
        if parent is not None:
            plan["changes"] = copy.deepcopy(parent["plan_payload"]["changes"])
            plan["installed_parent"] = copy.deepcopy(parent["build_reference"])
            if pipeline:
                change = {"before_sha256": digest("pipeline-before"), "after_sha256": successor.PIPELINE_SHA256}
                plan["changes"][successor.PIPELINE] = change
                plan["installed_parent_changes"] = {successor.PIPELINE: copy.deepcopy(change)}
            else:
                before = plan["changes"][successor.BUSINESS]["after_sha256"]
                plan["changes"][successor.BUSINESS]["after_sha256"] = digest(label + "-source")
                plan["installed_parent_changes"] = {successor.BUSINESS: {"before_sha256": before, "after_sha256": digest(label + "-source")}}
                plan.update(test_allowlist=sorted(successor.V2_TESTS), additional_source_allowlist=sorted(successor.V2_ADDITIONAL),
                            required_checks=sorted(successor.V2_CHECKS), change_scope="capture_runtime_lease_v1")
        proof = {"contract": successor.PROOF_V2 if version == 2 else successor.PROOF,
            "source_deployment_sha256": self.deployment["receipt_sha256"], "origin_runtime_bindings": self.origin,
            "runtime_bindings": {"build_sha256": digest(label + "-build"), "runtime_sha256": digest(label + "-runtime"), "config_sha256": self.origin["config_sha256"]},
            "active": self.active, "release_event_id": self.control["id"], "release_event_hash": self.control["event_hash"],
            "manifest": self.manifest, "plan_payload": plan, "plan_reference": reference(label + "-plan"),
            "build_reference": reference(label + "-build"), "runtime_reference": reference(label + "-runtime")}
        if parent is not None:
            proof["installed_parent_proof"] = copy.deepcopy(parent)
        return self.seal(proof)

    def validate(self, proof):
        return successor.validate_portable(self.connection, proof, deployment=self.deployment, at="2026-09-07T03:00:00Z")

    def test_legacy_two_generations_and_new_third_generation_validate(self):
        for proof in (self.first, self.second, self.third):
            self.assertEqual(self.validate(proof), proof)
        self.assertEqual(self.third["decision_receipt"]["payload"]["authorization"], "inherit_existing_user_release_only")
        self.assertFalse(self.third["decision_receipt"]["payload"]["schema_migration_repeated"])

    def test_fourth_generation_retains_the_whole_released_parent_chain(self):
        fourth = self.make("lease-next", self.third)
        self.assertEqual(self.validate(fourth), fourth)

    def test_v1_cannot_claim_an_extra_generation(self):
        forged = copy.deepcopy(self.third)
        forged["contract"] = successor.PROOF
        forged["plan_payload"]["contract"] = successor.PLAN
        with self.assertRaises(successor.auth.AuthorizationError):
            self.validate(self.seal(forged))

    def test_changed_operations_original_acceptance_or_config_are_rejected_even_when_rehashed(self):
        for field in ("operations", "source_decision_sha256", "config"):
            with self.subTest(field=field):
                forged = copy.deepcopy(self.third)
                if field == "config":
                    forged["runtime_bindings"]["config_sha256"] = digest("changed")
                else:
                    forged["plan_payload"][field] = ["extra-paid-operation"] if field == "operations" else digest("changed")
                with self.assertRaises(successor.auth.AuthorizationError):
                    self.validate(self.seal(forged))

    def test_parent_substitution_pipeline_rollback_and_changed_before_hash_are_rejected(self):
        for field in ("parent", "pipeline", "before", "unreviewed"):
            with self.subTest(field=field):
                forged = copy.deepcopy(self.third)
                plan = forged["plan_payload"]
                if field == "parent":
                    plan["installed_parent"] = copy.deepcopy(self.first["build_reference"])
                elif field == "pipeline":
                    plan["changes"][successor.PIPELINE]["after_sha256"] = digest("rollback")
                elif field == "before":
                    plan["installed_parent_changes"][successor.BUSINESS]["before_sha256"] = digest("another-parent")
                else:
                    plan["changes"]["src/dcar_eval/v8/provider_budget.py"] = {"before_sha256": None, "after_sha256": digest("larger-budget")}
                with self.assertRaises(successor.auth.AuthorizationError):
                    self.validate(self.seal(forged))

    def test_missing_postseal_decision_is_rejected(self):
        self.connection.execute("DELETE FROM scheduler_runs WHERE id=?", (self.third["decision_receipt"]["receipt_id"],))
        with self.assertRaises(successor.auth.AuthorizationError):
            self.validate(self.third)

    def test_duplicate_postseal_decisions_across_versions_are_rejected(self):
        receipt = copy.deepcopy(self.third["decision_receipt"])
        receipt["payload"]["contract"] = successor.DECISION
        self.connection.execute("INSERT INTO scheduler_runs VALUES(?,?,?,?)", (1000, "transport_receipt:campaign_terminal", "succeeded", json.dumps(receipt)))
        with self.assertRaises(successor.auth.AuthorizationError):
            self.validate(self.third)


if __name__ == "__main__":
    unittest.main()
