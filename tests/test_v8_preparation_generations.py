"""Intake revisions cannot bypass work deduplication or old billing holds."""
from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from tests import test_v8_account_preparation as fixtures
from tests import test_v8_preparation_recovery as recovery
from tests import test_v8_paid_dispatch as dispatch_fixture
from v8 import account_intake, account_preparation as prep, paid_dispatch, usage_settlements
from v8.storage import connect, initialize_database, now_utc

AT, ACTIVE = fixtures.AT, fixtures.ACTIVE
LATER = recovery.LATER


class PreparationGenerationsTest(unittest.TestCase):
    setUp = fixtures.AccountPreparationTest.setUp
    submit = fixtures.AccountPreparationTest.submit
    envelope = fixtures.AccountPreparationTest.envelope
    scope = fixtures.AccountPreparationTest.scope
    response = recovery.PreparationRecoveryTest.response
    raw_work = recovery.PreparationRecoveryTest.raw_work

    def replace_request(self, status="superseded", **extra):
        old = dict(self.db.execute("SELECT * FROM account_intake_requests ORDER BY id DESC LIMIT 1").fetchone())
        result = json.loads(old["result_json"])
        result["status"] = status
        self.db.execute("UPDATE account_intake_requests SET completed_at=?,result_json=? WHERE id=?", (LATER, json.dumps(result), old["id"]))
        return self.submit("revision-" + str(old["id"]), **extra)

    def test_same_locator_different_entry_keeps_one_existing_owner_and_zero_write_ticks(self):
        first = self.submit()
        self.assertEqual(prep.enqueue_pending(self.db, active=ACTIVE, at=AT)["created"], 1)
        original = self.envelope()
        self.submit("another-entry", phone="00123456789")
        before = self.db.total_changes
        self.assertEqual(prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)["created"], 0)
        self.assertEqual(self.db.total_changes, before)
        self.assertEqual(self.envelope(), original)
        self.assertEqual(original["preparation_revision"], first["intake_id"])

    def test_superseded_or_ready_new_same_locator_can_queue_once_when_never_sent(self):
        self.submit()
        prep.enqueue_pending(self.db, active=ACTIVE, at=AT)
        prior_identity = self.db.execute("SELECT work_identity FROM capture_work_items").fetchone()[0]
        prior_due = self.envelope()["logical_due"]
        for status in ("superseded", "ready"):
            with self.subTest(status=status):
                current = self.replace_request(status=status)
                result = prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)
                self.assertEqual(result["created"], 1)
                work = self.db.execute("SELECT * FROM capture_work_items ORDER BY id DESC LIMIT 1").fetchone()
                self.assertEqual(work["intake_request_id"], current["intake_id"])
                self.assertNotEqual(work["work_identity"], prior_identity)
                self.assertNotEqual(self.envelope()["logical_due"], prior_due)
                prior_identity = work["work_identity"]
                prior_due = self.envelope()["logical_due"]
                before = self.db.total_changes
                self.assertEqual(prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)["created"], 0)
                self.assertEqual(self.db.total_changes, before)

    def test_legacy_work_identity_is_adopted_without_duplicate(self):
        self.submit()
        prep.enqueue_pending(self.db, active=ACTIVE, at=AT)
        envelope = self.envelope()
        legacy_identity = prep.planning.digest({"contract": prep.CONTRACT, "preparation_key": envelope["preparation_key"],
            "target": envelope["request"], "shadow": False})
        self.db.execute("UPDATE capture_work_items SET work_identity=?", (legacy_identity,))
        before = self.db.total_changes
        self.assertEqual(prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)["created"], 0)
        self.assertEqual(self.db.total_changes, before)

    def test_directory_a_to_b_to_a_queues_one_task_for_each_real_revision(self):
        from v8.account_directory_reconciliation import reconcile_directory
        self.submit()
        reconcile_directory(self.db, at=AT)
        prep.enqueue_pending(self.db, active=ACTIVE, at=AT)
        for uid in ("987654321", "123456789"):
            self.db.execute("UPDATE account_directory_rows SET uid=?", (uid,))
            reconcile_directory(self.db, at=LATER)
            self.assertEqual(prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)["created"], 1)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM capture_work_items").fetchone()[0], 3)
        before = self.db.total_changes
        reconcile_directory(self.db, at=LATER)
        self.assertEqual(prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)["created"], 0)
        self.assertEqual(self.db.total_changes, before)

    def test_live_old_worker_blocks_replacement_even_without_send_yet(self):
        self.submit()
        prep.enqueue_pending(self.db, active=ACTIVE, at=AT)
        self.replace_request()
        self.db.execute("UPDATE capture_work_items SET state='running',owner_token='old-worker'")
        result = prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)
        self.assertEqual(result["created"], 0)
        self.assertEqual(result["blocked"][0]["reason"], "preparation_previous_revision_running")
        before = self.db.total_changes
        prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)
        self.assertEqual(self.db.total_changes, before)

    def test_unbound_paid_attempt_blocks_new_same_locator_request(self):
        self.raw_work()
        self.replace_request()
        result = prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)
        self.assertEqual(result["created"], 0)
        self.assertEqual(result["blocked"][0]["reason"], "preparation_previous_revision_billing_unverified")
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM capture_work_items").fetchone()[0], 1)

    def test_new_locator_same_directory_cannot_evade_old_billing(self):
        _, _, old, _, _ = self.raw_work()
        self.db.execute("UPDATE account_directory_rows SET uid='987654321' WHERE id=?", (old["directory_row_id"],))
        self.replace_request(uid="987654321")
        newest = self.db.execute("SELECT id FROM account_intake_requests ORDER BY id DESC LIMIT 1").fetchone()[0]
        self.db.execute("UPDATE account_intake_requests SET directory_row_id=? WHERE id=?", (old["directory_row_id"], newest))
        result = prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)
        self.assertEqual(result["created"], 0)
        self.assertEqual(result["blocked"][0]["reason"], "preparation_previous_revision_billing_unverified")


class PreparationGenerationLedgerTest(unittest.TestCase):
    def setUp(self):
        self.base = dispatch_fixture.PaidDispatchTest()
        self.base.setUp()
        self.addCleanup(self.base.doCleanups)
        self.db = connect(self.base.db)
        self.addCleanup(self.db.close)
        initialize_database(self.db, target_version=22)
        self.db.execute("BEGIN")
        self.at = now_utc()
        self.enterContext(patch("socket.socket.connect", side_effect=AssertionError("network forbidden")))
        self.enterContext(patch.object(prep, "_policy", return_value=fixtures.POLICY))
        self.enterContext(patch("v8.profile_activations.activation_at", return_value=ACTIVE))

    def request(self, key):
        return account_intake.submit_account_intake(self.db, request_key=key,
            value={"platform":"douyin", "uid":"123456789"}, source={"kind":"fixture"}, at=self.at)

    def reserve_old(self):
        old = self.request("old")
        prep.enqueue_pending(self.db, active=ACTIVE, at=self.at)
        self.db.execute("UPDATE account_intake_requests SET completed_at=?,result_json=json_set(result_json,'$.status','superseded') WHERE id=?", (self.at, old["intake_id"]))
        usage = self.db.execute("INSERT INTO provider_usage(provider,operation,request_attempts,amount,currency,recorded_at,details_json) VALUES ('TikHub','douyin_user_profile',1,0.001,'USD',?,'{}')", (self.at,)).lastrowid
        reserved = paid_dispatch.reserve_dispatch_in_transaction(self.db, provider="TikHub", operation="douyin_user_profile",
            activation_id=self.base.activation_id, business_day=self.at[:10], scheduler_run_id=self.base.run_id,
            scheduler_attempt_id=self.base.attempt_id, scope={"purpose":"reconcile", "intake_request_id":old["intake_id"]},
            provider_usage_id=usage, created_at=self.at)
        self.assertIsNotNone(reserved)
        self.request("new")
        return reserved, usage

    def test_reserved_old_dispatch_waits_until_real_not_sent_close(self):
        reserved, _ = self.reserve_old()
        result = prep.enqueue_pending(self.db, active=ACTIVE, at=self.at)
        self.assertEqual(result["created"], 0)
        self.assertEqual(result["blocked"][0]["reason"], "preparation_previous_revision_billing_unverified")
        paid_dispatch.close_dispatch_not_sent_in_transaction(self.db, reserved.dispatch_id, reason="fixture never sent", created_at=self.at)
        self.assertEqual(prep.enqueue_pending(self.db, active=ACTIVE, at=self.at)["created"], 1)

    def test_real_sent_closed_dispatch_requires_verified_settlement_before_new_work(self):
        reserved, usage = self.reserve_old()
        marker = paid_dispatch.mark_dispatch_sent_in_transaction(self.db, reserved.dispatch_id, fetch_attempt_id=None, created_at=self.at)
        claim = usage_settlements.claim_paid_scope(self.db, identity="f"*64, marker_id=marker.event_id, at=self.at)
        usage_settlements.record_request_start(self.db, marker_id=marker.event_id, request_claim_id=claim, at=self.at)
        paid_dispatch.finish_dispatch_in_transaction(self.db, reserved.dispatch_id, outcome="succeeded", created_at=self.at)
        settlement = usage_settlements.record_settlement(self.db, usage_id=usage, at=self.at)
        result = prep.enqueue_pending(self.db, active=ACTIVE, at=self.at)
        self.assertEqual(result["created"], 0)
        self.assertEqual(result["blocked"][0]["reason"], "preparation_previous_revision_billing_unverified")
        usage_settlements.verify_settlement(self.db, settlement_id=settlement["id"],
            evidence_kind="supplier_record", evidence_ref="isolated test fixture", evidence_sha256="e"*64,
            outcome="charged_verified", amount_microunits=1000, at=self.at)
        self.assertEqual(prep.enqueue_pending(self.db, active=ACTIVE, at=self.at)["created"], 1)
        before = self.db.total_changes
        self.assertEqual(prep.enqueue_pending(self.db, active=ACTIVE, at=self.at)["created"], 0)
        self.assertEqual(self.db.total_changes, before)


if __name__ == "__main__":
    unittest.main()
