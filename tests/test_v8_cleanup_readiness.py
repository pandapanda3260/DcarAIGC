"""Readiness verifies real cleanup receipts without authorizing provider work."""
from __future__ import annotations

import copy
import os
from pathlib import Path
import unittest

from tests import test_v8_account_cleanup_runtime as fixture
from v8 import account_cleanup_runtime as cleanup, capture_release, paid_drain, runtime_receipts


class CleanupReadinessTest(unittest.TestCase):
    # Reuse the installed filesystem/SQLite fixture, not mocked validators.
    setUp = fixture.CleanupRuntimeTest.setUp
    write = fixture.CleanupRuntimeTest.write
    envelope = fixture.CleanupRuntimeTest.envelope
    capsule = fixture.CleanupRuntimeTest.capsule

    def readiness(self, at=fixture.AT):
        before = self.connection.total_changes
        result = runtime_receipts.current_activation_readiness(self.connection, at=at)
        self.assertEqual(self.connection.total_changes, before)
        return result

    def test_verified_cleanup_control_does_not_claim_data_completion(self):
        result = self.readiness()
        self.assertTrue(result["control_readiness"], result)
        self.assertFalse(result["data_readiness"])
        self.assertEqual(result["reason"], "current_activation_coverage_incomplete")
        self.assertIsNone(result["receipt"])
        for table in ("capture_paid_send_gate_events", "provider_usage", "capture_work_items"):
            self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM " + table).fetchone()[0], 0)

    def test_missing_install_receipt_fails_closed(self):
        Path(os.environ["DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT"]).unlink()
        result = self.readiness()
        self.assertFalse(result["control_readiness"])
        self.assertEqual(result["reason"], "current_activation_permit_missing")

    def test_original_operator_approval_bytes_must_match(self):
        path = Path(self.authority["decision_receipt"]["path"])
        path.write_bytes(path.read_bytes() + b" ")
        self.assertFalse(self.readiness()["control_readiness"])

    def test_source_authority_digest_must_match(self):
        path = Path(self.generation["source_authority"]["path"])
        path.write_bytes(path.read_bytes() + b" ")
        self.assertFalse(self.readiness()["control_readiness"])

    def test_changed_database_identity_fails_closed(self):
        self.install["installed"]["inode"] += 1
        self.write("install.json", self.install)
        self.assertFalse(self.readiness()["control_readiness"])

    def test_modified_critical_source_fails_closed(self):
        (self.source / "src/dcar_eval/v8/paid_dispatch.py").write_text("# changed\n")
        self.assertFalse(self.readiness()["control_readiness"])

    def test_nested_activation_cannot_be_substituted(self):
        active = {**self.active, "activation_sha256": "0" * 64}
        self.assertFalse(runtime_receipts._current_hold_control_valid(
            self.connection, active=active,
            drain_state=paid_drain.dispatch_state(self.connection, at=fixture.AT), at=fixture.AT))

    def test_valid_drain_chain_with_different_cleanup_control_is_rejected(self):
        evidence = capture_release._installed_evidence(self.connection, at=fixture.AT)
        control = copy.deepcopy(cleanup.release_control(evidence))
        control["selection_sha256"] = "0" * 64
        binding = {"source_activation_id": self.active["activation_id"],
                   "target_activation_id": self.active["activation_id"],
                   "business_day": "2026-09-08", "planned_effective_at": fixture.LATER,
                   "build_receipt_sha256": evidence["build_sha256"],
                   "runtime_root_receipt_sha256": evidence["runtime_sha256"]}
        paid_drain.start_profile_drain_in_transaction(self.connection, "altered-control",
            binding=binding, switch_kind="same_profile", now=fixture.LATER, control=control)
        paid_drain.seal_profile_drain_in_transaction(self.connection, "altered-control",
            now=fixture.LATER, control=control)
        self.assertFalse(self.readiness(fixture.LATER)["control_readiness"])
        paid_drain.release_profile_drain_in_transaction(self.connection, "altered-control",
            now=fixture.LATER, control=control)
        self.assertTrue(paid_drain.dispatch_state(self.connection, at=fixture.LATER).paid_dispatch_open)
        self.assertFalse(self.readiness(fixture.LATER)["control_readiness"])


if __name__ == "__main__":
    unittest.main()
