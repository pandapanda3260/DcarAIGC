"""Restore only a released expired singleton reservation that never sent."""
from __future__ import annotations

import json
import socket
import unittest
from unittest.mock import patch

from tests import test_v8_paid_dispatch as dispatch_fixture
from tests import test_v8_account_preparation as preparation_fixture
from v8 import account_intake, account_preparation as prep, capture, paid_dispatch, providers, usage_settlements
from v8.provider_budget import budget_day, PRICES_MICROUSD, task_budget_id
from v8.storage import connect, initialize_database, now_utc


class NeverSentPreparationRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.base = dispatch_fixture.PaidDispatchTest()
        self.base.setUp()
        self.addCleanup(self.base.doCleanups)
        self.db = connect(self.base.db)
        self.addCleanup(self.db.close)
        initialize_database(self.db, target_version=22)
        self.at = now_utc()
        self.active = {**preparation_fixture.ACTIVE, "activation_id": self.base.activation_id}
        self.enterContext(patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden")))
        self.enterContext(patch.object(prep, "_policy", return_value=preparation_fixture.POLICY))
        self.enterContext(patch("v8.profile_activations.activation_at", return_value=self.active))
        self.db.execute("BEGIN")
        result = account_intake.submit_account_intake(self.db, request_key="never-sent-fixture",
            value={"platform": "douyin", "uid": "123456789"}, source={"kind": "fixture"}, at=self.at)
        self.intake_id = result["intake_id"]
        prep.enqueue_pending(self.db, active=self.active, at=self.at)
        self.work = dict(self.db.execute("SELECT * FROM capture_work_items WHERE intake_request_id=?", (self.intake_id,)).fetchone())
        self.envelope = json.loads(self.work["envelope_json"])
        self.target = self.envelope["request"]
        self.price_micro = PRICES_MICROUSD[self.target["operation"]]
        self.budget_id = task_budget_id("never-sent-fixture", "TikHub", self.target["operation"])
        self.db.execute("""INSERT INTO provider_budget_batches(id,purpose,provider,operation,currency,
            verified_unit_price,max_billable_requests,max_amount,pilot_size,daily_quota,price_verified_at,
            status,created_at,updated_at) VALUES(?,?,'TikHub',?,'USD',?,1000,10,0,1000,?,'approved',?,?)""",
            (self.budget_id, self.budget_id, self.target["operation"], self.price_micro / 1000000, self.at, self.at, self.at))
        self.identity = providers._paid_request_identity(operation=self.target["operation"], platform=self.target["platform"],
            subject=self.target["subject"], params=self.target["params"], cursor=None, due_bucket=self.envelope["logical_due"])
        self.slot = capture.ensure_intake_slot(self.db, intake_request_id=self.intake_id, stage="profile_prepare",
            window_key=self.envelope["logical_due"], provider="TikHub", adapter_version="fixture")
        self.batch = self.db.execute("""INSERT INTO fetch_request_batches(request_scope_identity,sequence,provider,
            operation,parameters_json,created_at) VALUES(?,0,'tikhub',?,?,?)""", (self.identity.scope_identity,
            self.target["operation"], prep.planning.canonical(self.target["params"]), self.at)).lastrowid
        self.member_scope = usage_settlements.member_identity(self.identity.document)
        self.member = self.db.execute("""INSERT INTO fetch_request_batch_members(batch_id,member_scope_identity,sequence,
            content_id,account_id,intake_request_id) VALUES(?,?,0,NULL,NULL,?)""", (self.batch, self.member_scope, self.intake_id)).lastrowid
        self.db.execute("""INSERT INTO admission_reservations(batch_id,state,amount_microusd,charge_business_day,
            created_at,expires_at,updated_at) VALUES(?,'released_unsent',?,?,?,?,?)""", (self.batch, self.price_micro, budget_day(self.at), self.at, self.at, self.at))
        self.dispatches = [self.add_dispatch(), self.add_dispatch()]
        self.db.execute("UPDATE scheduler_run_attempts SET status='partial',completed_at=? WHERE id=?", (self.at, self.base.attempt_id))
        self.db.execute("UPDATE scheduler_runs SET status='partial' WHERE id=?", (self.base.run_id,))
        self.db.execute("""UPDATE fetch_slots SET status='terminal_failed',last_error_code='paid_identity_hold',
            last_error_message='batch reservation expired or changed',attempt_count=0 WHERE id=?""", (self.slot,))
        self.db.execute("UPDATE capture_work_items SET state='paid_identity_hold',reason='paid_identity_hold',attempt_count=3 WHERE id=?", (self.work["id"],))

    def add_dispatch(self, *, closed=True, slot=None, state="not_sent", amount=0, attempts=0):
        slot_id = self.slot if slot is None else slot
        detail = {"state": state, "sent_at": None, "budget_day": budget_day(self.at),
                  "paid_sequence": 0, "paid_identity": self.identity.document,
                  "paid_execution_identity": self.identity.execution_identity,
                  "paid_scope_identity": self.identity.scope_identity,
                  "slot_id": slot_id, "request_batch_id": self.batch, "scope": {key: self.envelope[key] for key in prep.SCOPE_FIELDS}}
        usage = self.db.execute("""INSERT INTO provider_usage(provider,operation,request_attempts,billed_requests,
            amount,currency,recorded_at,details_json,budget_batch_id) VALUES('TikHub',?,?,0,?,'USD',?,?,?)""",
            (self.target["operation"], attempts, amount, self.at, prep.planning.canonical(detail), self.budget_id)).lastrowid
        reserved = paid_dispatch.reserve_dispatch_in_transaction(self.db, provider="TikHub", operation=self.target["operation"],
            activation_id=self.base.activation_id, business_day=budget_day(self.at), scheduler_run_id=self.base.run_id,
            scheduler_attempt_id=self.base.attempt_id, scope={"purpose": "reconcile", **{key: self.envelope[key] for key in prep.SCOPE_FIELDS}},
            provider_usage_id=usage, fetch_slot_id=slot_id, cursor_identity={"paid_scope_identity": self.identity.scope_identity,
                "paid_execution_identity": self.identity.execution_identity, "sequence": 0, "request": self.identity.document}, created_at=self.at)
        if closed:
            paid_dispatch.close_dispatch_not_sent_in_transaction(self.db, reserved.dispatch_id, reason="fixture not sent", created_at=self.at)
        return reserved

    def request(self):
        return prep._current_request(self.db, self.intake_id)

    def proof(self):
        return prep._never_sent_reservation_evidence(self.db, request=self.request(), target=self.target)

    def test_original_work_and_slot_recover_without_new_scope_generation_or_ledger_changes(self):
        proof = self.proof()
        self.assertIsNotNone(proof)
        ledger = {table: [tuple(r) for r in self.db.execute("SELECT * FROM " + table)] for table in
            ("provider_usage", "paid_provider_dispatch_events", "fetch_request_batches", "fetch_request_batch_members", "admission_reservations")}
        with patch.object(prep, "readiness", return_value=("runnable", "")):
            result = prep.enqueue_pending(self.db, active=self.active, at=self.at)
        self.assertEqual(result["created"], 0)
        self.assertEqual(result["recovered_unsent"], 1)
        work = self.db.execute("SELECT * FROM capture_work_items WHERE id=?", (self.work["id"],)).fetchone()
        self.assertEqual(work["state"], "runnable")
        self.assertEqual(work["envelope_json"], self.work["envelope_json"])
        self.assertEqual(work["work_identity"], self.work["work_identity"])
        self.assertEqual(work["attempt_count"], 3)
        self.assertEqual(self.db.execute("SELECT status FROM fetch_slots WHERE id=?", (self.slot,)).fetchone()[0], "retryable_failed")
        saved = json.loads(self.request()["result_json"])
        self.assertEqual(saved["unsent_reservation_recoveries"][0]["evidence"], proof)
        for table, rows in ledger.items():
            self.assertEqual([tuple(r) for r in self.db.execute("SELECT * FROM " + table)], rows)
        writes = self.db.total_changes
        prep.enqueue_pending(self.db, active=self.active, at=self.at)
        self.assertEqual(self.db.total_changes, writes)

    def test_missing_or_reserved_admission_does_not_recover(self):
        self.db.execute("UPDATE admission_reservations SET state='reserved_unsent'")
        self.assertIsNone(self.proof())

    def test_open_dispatch_and_any_send_are_not_never_sent(self):
        reserved = self.add_dispatch(closed=False)
        self.assertIsNone(self.proof())
        paid_dispatch.mark_dispatch_sent_in_transaction(self.db, reserved.dispatch_id, fetch_attempt_id=None, created_at=self.at)
        self.assertIsNone(self.proof())

    def test_different_slot_dispatch_cannot_hide_behind_same_scope(self):
        other_slot = capture.ensure_intake_slot(self.db, intake_request_id=self.intake_id, stage="profile_prepare",
            window_key="another-window", provider="TikHub", adapter_version="fixture")
        self.add_dispatch(slot=other_slot)
        self.assertIsNone(self.proof())

    def test_any_existing_attempt_or_raw_blocks_recovery(self):
        self.db.execute("INSERT INTO fetch_attempts(slot_id,attempt_number,request_started_at) VALUES(?,1,?)", (self.slot, self.at))
        self.assertIsNone(self.proof())

    def test_any_scope_claim_or_legacy_exclusion_blocks_recovery(self):
        self.db.execute("""INSERT INTO legacy_paid_scope_exclusions(scope_identity,evidence_sha256,evidence_ref,reason,created_at)
            VALUES(?,?,'fixture','fixture',?)""", (self.member_scope, "e" * 64, self.at))
        self.assertIsNone(self.proof())

    def test_nonzero_usage_or_extra_unlinked_usage_blocks_recovery(self):
        self.add_dispatch(amount=0.001)
        self.assertIsNone(self.proof())

    def test_live_scheduler_owner_and_shadow_mode_do_not_recover(self):
        self.base.attempt_id = self.db.execute("""INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,
            invocation_source,status,started_at,details_json) VALUES(?,2,'scheduled','running',?,'{}')""",
            (self.base.run_id, self.at)).lastrowid
        self.add_dispatch()
        self.assertIsNone(self.proof())
        self.db.execute("UPDATE scheduler_run_attempts SET status='partial',completed_at=? WHERE id=?", (self.at, self.base.attempt_id))
        result = prep.enqueue_pending(self.db, active=self.active, at=self.at, shadow=True)
        self.assertNotIn("recovered_unsent", result)
        self.assertEqual(self.db.execute("SELECT status FROM fetch_slots WHERE id=?", (self.slot,)).fetchone()[0], "terminal_failed")

    def test_unrelated_failure_message_does_not_recover(self):
        self.db.execute("UPDATE fetch_slots SET last_error_message='unknown result'")
        self.assertIsNone(self.proof())


if __name__ == "__main__":
    unittest.main()
