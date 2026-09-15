"""A repaired resolver parser replays complete bytes without a second send."""
from __future__ import annotations

import copy
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from tests import test_v8_account_preparation as fixtures
from tests import test_v8_intake_capture as raw_fixtures
from tests import test_v8_preparation_recovery as recovery_fixtures
from v8 import account_intake, account_preparation as prep, capture
from v8.provider_budget import PaidScopeBlocked

AT, ACTIVE, LATER = fixtures.AT, fixtures.ACTIVE, recovery_fixtures.LATER


class ResolverParserReplayTest(unittest.TestCase):
    setUp = fixtures.AccountPreparationTest.setUp
    envelope = fixtures.AccountPreparationTest.envelope
    scope = fixtures.AccountPreparationTest.scope
    response = raw_fixtures.IntakeCaptureTest.response

    def raw_work(self, *, old_error="invalid_finder_candidate", status="terminal_failed", invalid_candidate=False):
        self.intake = account_intake.submit_account_intake(self.db, request_key="resolver-fixture",
            value={"platform": "wechat_channels", "display_account_id": "sphfixture"}, source={"kind": "fixture"}, at=AT)
        prep.enqueue_pending(self.db, active=ACTIVE, at=AT)
        env = self.envelope()
        self.work = dict(self.db.execute("SELECT * FROM capture_work_items").fetchone())
        self.payload = {"code": 200, "router": env["request"]["path"], "params": env["request"]["params"],
            "data": {"ret": 0, "data": [{"items": [
                {"accTypeName": "公众号", "jumpInfo": {"userName": "gh_012345abcdef"}},
                {"accTypeName": "视频号", "jumpInfo": {"userName": "bad-finder" if invalid_candidate else "v2_012345abcdef@finder"}}
            ]}]}}
        response = self.response(self.payload)
        self.slot = capture.ensure_intake_slot(self.db, intake_request_id=self.intake["intake_id"], stage="profile_prepare",
            window_key=env["logical_due"], provider="TikHub", adapter_version="fixture")
        self.attempt = self.db.execute("""INSERT INTO fetch_attempts(slot_id,attempt_number,request_started_at,
            http_status,billed,error_code) VALUES (?,1,?,200,1,?)""", (self.slot, AT, old_error)).lastrowid
        claim = capture.SlotClaim(slot_id=self.slot, attempt_id=self.attempt, attempt_number=1, content_id=None,
            intake_request_id=self.intake["intake_id"], stage="profile_prepare", window_key=env["logical_due"],
            provider="TikHub", adapter_version="fixture", paid_scope_identity="f" * 64)
        self.raw_id = capture._store_raw_response(self.db, claim=claim, operation=env["operation"], value=self.payload,
            http_status=200, raw_root=Path(self.temp.name).resolve()/"raw", entity_bytes=response.entity_body, transport_receipt=response.receipt)
        self.db.execute("UPDATE fetch_slots SET status=?,last_error_code=?,attempt_count=1 WHERE id=?", (status, old_error, self.slot))
        self.db.execute("UPDATE capture_work_items SET state='paid_identity_hold',reason=?,updated_at=? WHERE id=?",
            ("paid_identity_hold:" + old_error, AT, self.work["id"]))
        return env

    def request(self):
        return dict(self.db.execute("SELECT * FROM account_intake_requests WHERE id=?", (self.intake["intake_id"],)).fetchone())

    def snapshots(self):
        return {table: [tuple(row) for row in self.db.execute("SELECT * FROM " + table)] for table in
            ("fetch_slots", "fetch_attempts", "provider_raw_responses", "paid_provider_dispatch_events",
             "provider_usage", "provider_usage_settlements", "provider_paid_scope_claims")}

    def test_fixed_parser_queues_one_no_network_replay_and_preserves_failed_slot_and_billing(self):
        original = self.raw_work()
        before = self.snapshots()
        self.assertEqual(prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)["created"], 1)
        env = self.envelope()
        self.assertEqual(env["preparation_retry_proof"]["kind"], "replay")
        self.assertEqual(env["preparation_retry_proof"]["replay_reason"], "wechat_resolver_parser_repair")
        self.assertEqual(env["logical_due"], original["logical_due"])
        with self.assertRaisesRegex(PaidScopeBlocked, "preparation_replay_network_forbidden"):
            prep.validate_paid_target(self.db, self.scope(env), at=LATER, for_payment=True)
        writes = self.db.total_changes
        self.assertEqual(prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)["created"], 0)
        self.assertEqual(self.db.total_changes, writes)
        self.db.commit()
        with patch.object(capture, "load_succeeded_raw_response", side_effect=AssertionError("must not pretend slot succeeded")), \
             patch.object(capture, "execute_intake_fetch", side_effect=AssertionError("must not send")), \
             patch("v8.providers._budget_for_call", side_effect=AssertionError("must not buy")), \
             patch("v8.provider_budget.assert_paid_scope_owner"):
            result = prep.execute_step(env, db_path=Path(self.temp.name)/"test.sqlite3", at=LATER)
        self.assertEqual(result["provider_cost"], 0)
        self.assertEqual(result["evidence"]["next_operation"], "wechat_channels_channel_info")
        self.assertEqual(self.snapshots(), before)
        saved = json.loads(self.request()["result_json"])
        self.assertEqual(saved["preparation_responses"], [{"operation": "wechat_channels_resolve", "raw_response_id": self.raw_id}])
        self.db.execute("BEGIN")
        self.assertEqual(prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)["created"], 1)
        self.assertEqual(self.envelope()["operation"], "wechat_channels_channel_info")

    def test_still_invalid_candidate_and_unrelated_failure_are_not_replayed(self):
        self.raw_work(invalid_candidate=True)
        result = prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)
        self.assertEqual(result["created"], 0)
        self.assertEqual(result["blocked"][0]["reason"], "preparation_retry_not_transient")

    def test_other_terminal_error_does_not_borrow_resolver_repair(self):
        self.raw_work(old_error="request_identity_mismatch")
        result = prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)
        self.assertEqual(result["created"], 0)
        self.assertEqual(result["blocked"][0]["reason"], "preparation_retry_not_transient")

    def test_other_platform_cannot_borrow_resolver_repair(self):
        base = recovery_fixtures.PreparationRecoveryTest()
        base.setUp()
        self.addCleanup(base.doCleanups)
        work, env, request, _, _ = base.raw_work(successful=True)
        base.db.execute("UPDATE fetch_slots SET status='terminal_failed'")
        base.db.execute("UPDATE fetch_attempts SET error_code='invalid_finder_candidate'")
        with self.assertRaisesRegex(PaidScopeBlocked, "preparation_retry_not_transient"):
            prep._retry_evidence(base.db, previous_work_id=work["id"], request=request, target=env["request"])

    def test_incomplete_transport_cannot_be_replayed(self):
        self.raw_work()
        # Receipts are append-only. Exercise their validator boundary without
        # mutating committed transport evidence to manufacture a bad receipt.
        with patch.object(capture, "_validate_complete_transport_receipt", side_effect=capture.RawEvidenceError("incomplete")):
            result = prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)
        self.assertEqual(result["created"], 0)
        self.assertEqual(result["blocked"][0]["reason"], "preparation_retry_transport_incomplete")

    def test_proof_raw_or_logical_due_change_cannot_fall_back_to_network(self):
        self.raw_work()
        prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)
        original = self.envelope()
        self.db.commit()
        for change in ("raw", "window", "proof"):
            env = copy.deepcopy(original)
            if change == "raw":
                env["replay_raw_response_id"] += 1
            elif change == "window":
                env["logical_due"] += ":different"
            else:
                env["preparation_retry_proof"]["raw_sha256"] = "0" * 64
            with self.subTest(change=change), \
                 patch.object(capture, "execute_intake_fetch", side_effect=AssertionError("must not send")), \
                 self.assertRaisesRegex(PaidScopeBlocked, "preparation_replay_evidence_changed"):
                prep.execute_step(env, db_path=Path(self.temp.name)/"test.sqlite3", at=LATER)


if __name__ == "__main__":
    unittest.main()
