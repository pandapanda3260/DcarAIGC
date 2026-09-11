"""Independent profile approval must not borrow historical operation authority."""
from __future__ import annotations

import copy
import json
import unittest
from unittest.mock import patch

from v8 import account_profile_authority as profile, capture_authorizations as auth


AT = "2026-09-10T15:00:00Z"


def reference(name, digit):
    return {"path": "/private/profile-fixture/" + name, "sha256": digit * 64}


def fixture_evidence():
    policy = {"authority": "account_directory", "statuses": ["daily", "weekly"]}
    catalog = {"contract": "catalog-fixture", "catalog_policy_sha256": auth.digest(policy)}
    catalog["proof_sha256"] = auth.digest(catalog)
    approval = {"contract": profile.AUTHORIZATION_CONTRACT, "operations": ["douyin_uid_profile"],
                "production_rollout": "approved_by_user", "business_e2e": "required",
                "transport_qualification": "not_verified", "actor": "user",
                "reason": "Repair missing profile operation authority",
                "user_instruction": "Investigate and fix blocked account profile refresh",
                "source_thread_id": "profile-repair-fixture", "issued_at": "2026-09-10T14:00:00Z",
                "parent_build": reference("parent.json", "a"), "source_tree": reference("source-tree.json", "b"),
                "catalog_policy_sha256": auth.digest(policy),
                "formal_database": {"path": "/private/profile-fixture/formal.sqlite3", "device": 1, "inode": 2}}
    proof = {"contract": profile.PROOF_CONTRACT, "operations": ["douyin_uid_profile"],
             "authorization": reference("approval.json", "c"), "authorization_payload": approval,
             "loaded_build": reference("new-build.json", "d"), "parent_build": approval["parent_build"],
             "source_tree": approval["source_tree"], "runtime_root_receipt": reference("runtime.json", "e"),
             "config_sha256": "f" * 64, "transport_manifest": {"api_base": "https://api.tikhub.io"},
             "catalog_policy_sha256": auth.digest(policy),
             "inherited_authority_proof_sha256": catalog["proof_sha256"],
             "issued_at": "2026-09-10T14:01:00Z"}
    proof["proof_sha256"] = auth.digest(proof)
    return {"active": {"activation_id": 1, "profile_id": "integrated_route_v1", "roster_snapshot_id": 1,
                       "roster_members_sha256": "1" * 64, "activation_sha256": "2" * 64},
            "deployment": {"status": "accepted", "release_decision": {"operations": ["douyin_user_posts"]}},
            "build_sha256": "3" * 64, "runtime_sha256": "e" * 64, "config_sha256": "f" * 64,
            "manifest": proof["transport_manifest"], "catalog_capture_policy": policy,
            "catalog_capture_policy_sha256": auth.digest(policy), "catalog_capture_proof": catalog,
            "install": {"formal_database": approval["formal_database"]["path"], "installed": {"device": 1, "inode": 2}},
            profile.EVIDENCE_KEY: proof}


def reseal(evidence):
    proof = evidence[profile.EVIDENCE_KEY]
    proof["proof_sha256"] = auth.digest({key: value for key, value in proof.items() if key != "proof_sha256"})


class AccountProfileAuthorityTest(unittest.TestCase):
    def test_new_profile_decision_preserves_old_runtime_and_decision(self):
        evidence = fixture_evidence()
        before = copy.deepcopy(evidence)
        decision = profile.decision(evidence, "douyin_uid_profile", AT)
        self.assertEqual(evidence, before)
        self.assertEqual(decision["qualification"], "operator_authorized")
        self.assertEqual(decision["business_e2e"], "required")
        self.assertEqual(decision["transport_qualification"], "not_verified")
        self.assertEqual(decision["runtime_bindings"]["build_sha256"], "3" * 64)
        self.assertEqual(decision["loaded_build"]["sha256"], "d" * 64)
        self.assertNotEqual(decision["runtime_bindings"]["build_sha256"], decision["loaded_build"]["sha256"])
        self.assertEqual(decision["decision_sha256"], auth.digest({key: value for key, value in decision.items()
                                                                if key != "decision_sha256"}))

    def test_original_operations_and_missing_new_approval_never_gain_profile_authority(self):
        evidence = fixture_evidence()
        for operation in ("douyin_user_posts", "douyin_video_detail", "douyin_video_comments",
                          "douyin_video_statistics", "xiaohongshu_user_posts"):
            self.assertIsNone(profile.decision(evidence, operation, AT))
        del evidence[profile.EVIDENCE_KEY]
        self.assertIsNone(profile.decision(evidence, "douyin_uid_profile", AT))

    def test_proof_tampering_or_expanded_scope_is_rejected(self):
        for reseal_proof in (False, True):
            evidence = fixture_evidence()
            evidence[profile.EVIDENCE_KEY]["operations"].append("douyin_video_detail")
            if reseal_proof:
                reseal(evidence)
            with self.assertRaises(auth.AuthorizationError):
                profile.decision(evidence, "douyin_uid_profile", AT)

    def test_declared_approval_cannot_overclaim_or_omit_provenance(self):
        for key, value in (("business_e2e", "deferred_by_user"), ("business_e2e", "passed"),
                           ("transport_qualification", "qualified"), ("operations", ["douyin_user_posts"]),
                           ("actor", ""), ("source_thread_id", ""), ("user_instruction", "")):
            with self.subTest(key=key, value=value):
                evidence = fixture_evidence()
                evidence[profile.EVIDENCE_KEY]["authorization_payload"][key] = value
                reseal(evidence)
                with self.assertRaises(auth.AuthorizationError):
                    profile.decision(evidence, "douyin_uid_profile", AT)

    def test_parent_source_or_installed_database_mismatch_is_rejected(self):
        for key, value in (("parent_build", reference("other-parent.json", "6")),
                           ("source_tree", reference("other-tree.json", "7")),
                           ("formal_database", {"path": "/private/other.sqlite3", "device": 1, "inode": 2}),
                           ("formal_database", {"path": "/private/profile-fixture/formal.sqlite3", "device": 1, "inode": 99})):
            with self.subTest(key=key, value=value):
                evidence = fixture_evidence()
                evidence[profile.EVIDENCE_KEY]["authorization_payload"][key] = value
                reseal(evidence)
                with self.assertRaises(auth.AuthorizationError):
                    profile.decision(evidence, "douyin_uid_profile", AT)

    def test_unrelated_runtime_or_catalog_policy_is_rejected(self):
        for key, value in (("runtime_sha256", "7" * 64), ("config_sha256", "8" * 64),
                           ("manifest", {"api_base": "https://other.invalid"}),
                           ("catalog_capture_policy_sha256", "9" * 64)):
            with self.subTest(key=key):
                evidence = fixture_evidence()
                evidence[key] = value
                with self.assertRaises(auth.AuthorizationError):
                    profile.decision(evidence, "douyin_uid_profile", AT)
        evidence = fixture_evidence()
        evidence["catalog_capture_policy"]["statuses"].append("paused")
        with self.assertRaises(auth.AuthorizationError):
            profile.decision(evidence, "douyin_uid_profile", AT)

    def test_future_approval_and_unaccepted_installation_are_rejected(self):
        for mutate in (lambda e: e[profile.EVIDENCE_KEY]["authorization_payload"].update(issued_at="2026-09-11T00:00:00Z"),
                       lambda e: e[profile.EVIDENCE_KEY].update(issued_at="2026-09-11T00:00:00Z"),
                       lambda e: e["deployment"].update(status="candidate"),
                       lambda e: e["active"].update(profile_id="tikhub_managed_v1")):
            evidence = fixture_evidence()
            mutate(evidence)
            reseal(evidence)
            with self.assertRaises(auth.AuthorizationError):
                profile.decision(evidence, "douyin_uid_profile", AT)

    def test_new_approval_or_build_changes_decision_binding(self):
        evidence = fixture_evidence()
        original = profile.decision(evidence, "douyin_uid_profile", AT)["decision_sha256"]
        for key, value in (("authorization", reference("next-approval.json", "5")),
                           ("loaded_build", reference("next-build.json", "6"))):
            changed = copy.deepcopy(evidence)
            changed[profile.EVIDENCE_KEY][key] = value
            reseal(changed)
            self.assertNotEqual(profile.decision(changed, "douyin_uid_profile", AT)["decision_sha256"], original)

    def test_sealed_references_accept_verified_byte_size_but_reject_invalid_sizes(self):
        evidence = fixture_evidence()
        proof = evidence[profile.EVIDENCE_KEY]
        for key in ("authorization", "loaded_build", "parent_build", "source_tree", "runtime_root_receipt"):
            proof[key]["byte_size"] = 1024
        reseal(evidence)
        self.assertIsNotNone(profile.decision(evidence, "douyin_uid_profile", AT))
        for size in (0, -1, True, "1024"):
            proof["authorization"]["byte_size"] = size
            reseal(evidence)
            with self.assertRaises(auth.AuthorizationError):
                profile.decision(evidence, "douyin_uid_profile", AT)


class AccountProfileOperatorIntegrationTest(unittest.TestCase):
    """Real isolated cleanup installation, Writer lease, drain and gate tables.

Only the new verifier's already-verified profile/catalog proof is supplied by
the fixture. Its sealed-source validation is covered by release-module tests.
Every operation issuer, renewal and A/B authorization check runs unchanged.
"""

    def setUp(self):
        from tests.test_v8_account_cleanup_runtime import CleanupRuntimeTest, AT as cleanup_at
        from v8 import capture_release as release

        self.fixture = CleanupRuntimeTest("runTest")
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()
        self.connection = self.fixture.connection
        self.at = cleanup_at
        self.profile_enabled = False
        original = release._installed_evidence
        base = original(self.connection, at=self.at)
        sample = fixture_evidence()
        self.profile_evidence = {key: sample[key] for key in ("catalog_capture_policy",
            "catalog_capture_policy_sha256", "catalog_capture_proof", profile.EVIDENCE_KEY)}
        proof = self.profile_evidence[profile.EVIDENCE_KEY]
        proof.update(runtime_root_receipt=reference("actual-runtime.json", "0"),
                     config_sha256=base["config_sha256"], transport_manifest=base["manifest"],
                     issued_at="2026-09-07T11:59:00Z")
        proof["runtime_root_receipt"]["sha256"] = base["runtime_sha256"]
        proof["authorization_payload"].update(issued_at="2026-09-07T11:58:00Z",
            formal_database={"path": base["install"]["formal_database"],
                             "device": base["install"]["installed"]["device"],
                             "inode": base["install"]["installed"]["inode"]})
        reseal(self.profile_evidence)

        def installed(connection, *, at, **kwargs):
            evidence = original(connection, at=at, **kwargs)
            if self.profile_enabled:
                evidence.update(copy.deepcopy(self.profile_evidence))
            return evidence

        self.enterContext(patch.object(release, "_installed_evidence", side_effect=installed))

    def maintenance(self, at=None):
        return self.fixture.maintenance(at or self.at)

    def profile_rows(self):
        return self.connection.execute("SELECT * FROM capture_paid_send_gate_events "
            "WHERE operation='douyin_uid_profile' ORDER BY id").fetchall()

    def assert_no_provider_work(self):
        for table in ("provider_usage", "provider_request_start_events", "paid_provider_dispatch_events",
                      "fetch_attempts", "capture_work_items"):
            self.assertEqual(self.connection.execute("SELECT count(*) FROM " + table).fetchone()[0], 0, table)
        self.assertEqual(self.fixture.network.call_count, 0)

    def test_missing_profile_approval_does_not_bootstrap_or_use_native(self):
        from v8 import capture_release as release

        with patch.object(release, "freeze_operation_cohort", side_effect=AssertionError("native forbidden")):
            result = self.maintenance()
        self.assertEqual(result["operations"]["douyin_uid_profile"]["status"], "not_enabled")
        self.assertEqual(len(self.profile_rows()), 0)
        self.assertNotIn("douyin_uid_profile", release.CONTINUITY_OPERATIONS)
        self.assert_no_provider_work()

    def test_first_profile_gate_uses_original_budget_and_validates_A_B(self):
        from v8 import account_cleanup_runtime as cleanup, capture_operator_release as operator
        from v8 import capture_release as release, provider_budget

        original = release._installed_evidence(self.connection, at=self.at)
        decisions = {operation: operator._decision(original, operation, self.at)
                     for operation in cleanup.OPERATIONS}
        self.profile_enabled = True
        current = release._installed_evidence(self.connection, at=self.at)
        self.assertEqual(decisions, {operation: operator._decision(current, operation, self.at)
                                    for operation in cleanup.OPERATIONS})
        result = self.maintenance()["operations"]["douyin_uid_profile"]
        self.assertEqual(result["status"], "initialized")
        self.assertEqual(result["qualification"], "operator_authorized")
        self.assertEqual(result["business_e2e"], "required")
        self.assertFalse(result["coverage_complete"])
        self.assertEqual(result["provider_calls"], 0)
        gate = self.profile_rows()[-1]
        payload = json.loads(gate["evidence_json"])
        ready = self.connection.execute("SELECT * FROM provider_readiness_receipts WHERE id=?",
                                        (payload["readiness_receipt_id"],)).fetchone()
        evidence = json.loads(ready["evidence_json"])
        self.assertEqual(payload["budget"], {"total_microusd": provider_budget.AUTOMATIC_MICROUSD,
            "bucket": "discovery", "bucket_microusd": provider_budget.BUDGET_BUCKET_MICROUSD["discovery"]})
        bindings = release.current_runtime_bindings(self.connection, "douyin_uid_profile", self.at)
        operator.validate_request(self.connection, runtime_bindings=bindings, operation="douyin_uid_profile",
            at=self.at, readiness_evidence=evidence, gate_payload=payload)
        arguments = dict(runtime_bindings=bindings, operation="douyin_uid_profile",
            request_identity=auth.digest({"fixture": "one-unpurchased-profile"}), at=self.at,
            amount_microusd=provider_budget.PRICES_MICROUSD["douyin_uid_profile"])
        phase_a = auth.validate_authorization(self.connection, **arguments)
        phase_b = auth.validate_authorization(self.connection,
            expected_authority_sha256=phase_a["authority_sha256"], **arguments)
        self.assertEqual(phase_a["authority_sha256"], phase_b["authority_sha256"])
        self.assert_no_provider_work()

    def test_fresh_gate_is_unchanged_and_renews_at_six_hour_boundary(self):
        self.profile_enabled = True
        first = self.maintenance()["operations"]["douyin_uid_profile"]
        original = dict(self.profile_rows()[-1])
        fresh = self.maintenance("2026-09-07T13:00:00Z")["operations"]["douyin_uid_profile"]
        self.assertEqual(fresh["status"], "fresh")
        self.assertEqual(fresh["expires_at"], first["expires_at"])
        self.assertEqual([dict(row) for row in self.profile_rows()], [original])
        renewed = self.maintenance("2026-09-08T06:00:00Z")["operations"]["douyin_uid_profile"]
        self.assertEqual(renewed["status"], "renewed")
        self.assertEqual(renewed["expires_at"], "2026-09-09T06:00:00Z")
        self.assertEqual(renewed["business_e2e"], "required")
        self.assertEqual(len(self.profile_rows()), 2)
        self.assertEqual(dict(self.profile_rows()[0]), original)
        self.assert_no_provider_work()

    def test_explicit_profile_closure_is_never_reopened(self):
        self.profile_enabled = True
        self.maintenance()
        gate = {"provider": "tikhub", "operation": "douyin_uid_profile", "state": "closed",
                "reason": "explicit closure", "evidence_json": "{}", "recorded_at": self.at}
        self.connection.execute("INSERT INTO capture_paid_send_gate_events "
            "(provider,operation,state,reason,evidence_json,recorded_at,event_sha256) VALUES (?,?,?,?,?,?,?)",
            (*gate.values(), auth.digest(gate)))
        result = self.maintenance("2026-09-08T06:00:00Z")["operations"]["douyin_uid_profile"]
        self.assertEqual(result["status"], "not_enabled")
        self.assertEqual(len(self.profile_rows()), 2)
        self.assertEqual(self.profile_rows()[-1]["state"], "closed")
        self.assert_no_provider_work()

    def test_tampered_profile_proof_blocks_only_profile_and_changes_A_B_authority(self):
        from v8 import account_cleanup_runtime as cleanup, capture_operator_release as operator
        from v8 import capture_release as release

        self.profile_enabled = True
        self.profile_evidence[profile.EVIDENCE_KEY]["authorization_payload"]["operations"].append("douyin_video_detail")
        reseal(self.profile_evidence)
        result = self.maintenance()
        self.assertEqual(result["operations"]["douyin_uid_profile"]["status"], "blocked")
        self.assertEqual({operation for operation, row in result["operations"].items()
                          if row["status"] == "initialized"}, cleanup.OPERATIONS)
        self.assertEqual(len(self.profile_rows()), 0)
        self.profile_evidence[profile.EVIDENCE_KEY]["authorization_payload"]["operations"] = ["douyin_uid_profile"]
        reseal(self.profile_evidence)
        self.maintenance()
        payload = json.loads(self.profile_rows()[-1]["evidence_json"])
        ready = self.connection.execute("SELECT evidence_json FROM provider_readiness_receipts WHERE id=?",
                                        (payload["readiness_receipt_id"],)).fetchone()
        bindings = release.current_runtime_bindings(self.connection, "douyin_uid_profile", self.at)
        self.profile_evidence[profile.EVIDENCE_KEY]["loaded_build"] = reference("later-build.json", "9")
        reseal(self.profile_evidence)
        with self.assertRaises(auth.AuthorizationError):
            operator.validate_request(self.connection, runtime_bindings=bindings, operation="douyin_uid_profile",
                at=self.at, readiness_evidence=json.loads(ready[0]), gate_payload=payload)
        self.assert_no_provider_work()


if __name__ == "__main__":
    unittest.main()
