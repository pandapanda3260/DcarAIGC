"""Preparation recovery contracts on temporary databases and local raw fixtures."""
from __future__ import annotations

import json
from pathlib import Path
import unittest
from unittest.mock import patch

from tests import test_v8_account_preparation as fixtures
AT, ACTIVE = fixtures.AT, fixtures.ACTIVE
from tests import test_v8_intake_capture as raw_fixtures
from v8 import account_preparation as prep, capture, platform_adapters
from v8.paid_identity import build_paid_request_identity
from v8.provider_budget import PaidScopeBlocked

LATER = "2026-09-12T03:06:00Z"


class PreparationRecoveryTest(unittest.TestCase):
    setUp = fixtures.AccountPreparationTest.setUp
    submit = fixtures.AccountPreparationTest.submit
    envelope = fixtures.AccountPreparationTest.envelope
    scope = fixtures.AccountPreparationTest.scope
    response = raw_fixtures.IntakeCaptureTest.response

    def raw_work(self, *, successful=False):
        self.submit()
        prep.enqueue_pending(self.db, active=ACTIVE, at=AT)
        envelope = self.envelope()
        work = dict(self.db.execute("SELECT * FROM capture_work_items").fetchone())
        payload = {"code": 200, "router": envelope["request"]["path"], "params": envelope["request"]["params"],
            "data": {"status_code": 0, "data": {"id_str": "123456789", "sec_uid": "MS4wLjAB" + "A"*64, "nickname": "fixture"}}} if successful else {"code": 500, "data": {"message": "upstream temporary error"}}
        response = self.response(payload)
        slot = capture.ensure_intake_slot(self.db, intake_request_id=work["intake_request_id"], stage="profile_prepare",
            window_key=envelope["logical_due"], provider="TikHub", adapter_version="fixture")
        attempt = self.db.execute("INSERT INTO fetch_attempts(slot_id,attempt_number,request_started_at,http_status,billed,error_code) VALUES (?,1,?,200,1,?)", (slot, AT, None if successful else "provider_business_failure")).lastrowid
        claim = capture.SlotClaim(slot_id=slot, attempt_id=attempt, attempt_number=1, content_id=None,
            intake_request_id=work["intake_request_id"], stage="profile_prepare", window_key=envelope["logical_due"], provider="TikHub", adapter_version="fixture", paid_scope_identity="f"*64)
        raw_id = capture._store_raw_response(self.db, claim=claim, operation=envelope["operation"], value=payload,
            http_status=200, raw_root=Path(self.temp.name).resolve()/"raw", entity_bytes=response.entity_body, transport_receipt=response.receipt)
        self.db.execute("UPDATE fetch_slots SET status=?,attempt_count=1 WHERE id=?", ("succeeded" if successful else "retryable_failed", slot))
        self.db.execute("UPDATE capture_work_items SET state='paid_identity_hold',reason=?,updated_at=? WHERE id=?", ("local_apply_failed" if successful else "paid_identity_hold:provider_business_failure", AT, work["id"]))
        request = dict(self.db.execute("SELECT * FROM account_intake_requests").fetchone())
        return work, envelope, request, raw_id, response

    def test_complete_business_failure_with_unknown_billing_never_creates_retry(self):
        work, envelope, request, _, _ = self.raw_work()
        with self.assertRaisesRegex(PaidScopeBlocked, "preparation_billing_unverified"):
            prep._retry_evidence(self.db, previous_work_id=work["id"], request=request, target=envelope["request"])
        result = prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)
        self.assertEqual(result["created"], 0)
        self.assertEqual(result["blocked"][0]["reason"], "preparation_billing_unverified")
        self.assertEqual(self.db.execute("SELECT count(*) FROM capture_work_items").fetchone()[0], 1)

    def test_verified_failure_queues_one_bounded_delayed_generation_and_preserves_history(self):
        work, envelope, _, raw_id, _ = self.raw_work()
        evidence = {"settlement_id": 5, "settlement_state": "charged_verified", "settlement_event_sequence": 2,
                    "settlement_source_sha256": "a"*64, "send_marker_id": 7}
        # Billing verifier is an explicit unit boundary. Production requires its
        # actual immutable send/start/settlement join; missing proof is tested above.
        with patch.object(prep, "_verified_retry_settlement", return_value=evidence):
            result = prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)
            self.assertEqual(result["created"], 1)
            current = self.envelope()
            self.assertEqual(current["preparation_attempt_generation"], 1)
            self.assertTrue(current["logical_due"].endswith(":attempt:1"))
            self.assertNotEqual(current["logical_due"], envelope["logical_due"])
            self.assertEqual(current["preparation_retry_proof"]["raw_response_id"], raw_id)
            due = self.db.execute("SELECT due_at FROM capture_work_items ORDER BY id DESC").fetchone()[0]
            self.assertEqual(due, "2026-09-12T03:05:00.000000Z")
            changes = self.db.total_changes
            self.assertEqual(prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)["created"], 0)
            self.assertEqual(changes, self.db.total_changes)
        self.assertEqual(self.db.execute("SELECT state FROM capture_work_items WHERE id=?", (work["id"],)).fetchone()[0], "paid_identity_hold")
        self.assertEqual(self.db.execute("SELECT paid_scope_identity FROM provider_raw_responses WHERE id=?", (raw_id,)).fetchone()[0], "f"*64)

    def test_successful_raw_recovers_by_replay_and_cannot_create_a_new_paid_request(self):
        work, envelope, request, raw_id, _ = self.raw_work(successful=True)
        proof = prep._retry_evidence(self.db, previous_work_id=work["id"], request=request, target=envelope["request"])
        self.assertEqual(proof["kind"], "replay")
        result = prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)
        self.assertEqual(result["created"], 1)
        current = self.envelope()
        self.assertEqual(current["logical_due"], envelope["logical_due"])
        self.assertEqual(current["replay_raw_response_id"], raw_id)
        with self.assertRaisesRegex(PaidScopeBlocked, "preparation_replay_network_forbidden"):
            prep.validate_paid_target(self.db, self.scope(current), at=LATER, for_payment=True)
        self.db.commit()
        with patch.object(capture, "load_succeeded_raw_response", side_effect=capture.SlotUnavailable("gone")), \
             patch.object(capture, "execute_intake_fetch", side_effect=AssertionError("must never buy")), \
             self.assertRaisesRegex(PaidScopeBlocked, "preparation_replay_raw_missing"):
            prep.execute_step(current, db_path=Path(self.temp.name)/"test.sqlite3", at=LATER)

    def test_prevalidation_accepts_success_before_raw_id_but_final_parse_requires_real_ids(self):
        _, envelope, request, _, response = self.raw_work(successful=True)
        result = prep._validated_profile_result(response, value=json.loads(request["input_json"]), responses=[], target=envelope["request"])
        self.assertEqual(result.entity_bytes, response.entity_body)
        with self.assertRaisesRegex(platform_adapters.PlatformAdapterError, "raw_response_required"):
            platform_adapters.next_profile_request(json.loads(request["input_json"]), responses=[{"operation": envelope["operation"], "payload": response.payload}])

    def test_target_parameter_drift_is_rejected_even_when_subject_is_unchanged(self):
        self.submit(); prep.enqueue_pending(self.db, active=ACTIVE, at=AT)
        envelope = self.envelope()
        changed = {**envelope["request"], "params": {"uid": "987654321"}}
        with patch.object(platform_adapters, "next_profile_request", return_value=changed), self.assertRaisesRegex(PaidScopeBlocked, "preparation_step_changed"):
            prep.validate_paid_target(self.db, self.scope(envelope), at=AT)
        request = build_paid_request_identity(provider="TikHub", operation=envelope["operation"], platform="douyin", subject="123456789", request_parameters={"uid": "987654321"}, cursor=None, due_bucket=envelope["logical_due"])
        with self.assertRaisesRegex(PaidScopeBlocked, "parameters differ"):
            capture._intake_request_subject(self.db, scope=self.scope(envelope), request_identity=request, operation=envelope["operation"], at=AT)

    def test_exhausted_generation_is_not_reissued_and_stale_error_clears_once(self):
        work, envelope, _, _, _ = self.raw_work()
        envelope["preparation_attempt_generation"] = prep.MAX_AUTOMATIC_PREPARATION_RETRIES
        self.db.execute("UPDATE capture_work_items SET envelope_json=? WHERE id=?", (json.dumps(envelope), work["id"]))
        result = prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)
        self.assertEqual(result["blocked"][0]["reason"], "preparation_retry_exhausted")
        self.db.execute("UPDATE capture_work_items SET state='provider_blocked' WHERE id=?", (work["id"],))
        prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)
        self.assertNotIn("preparation_error", json.loads(self.db.execute("SELECT result_json FROM account_intake_requests").fetchone()[0]))
        changes = self.db.total_changes
        prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)
        self.assertEqual(changes, self.db.total_changes)


if __name__ == "__main__":
    unittest.main()
