"""Reuse recovered profile bytes for unsent intake work, in isolated databases."""
from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from tests import test_v8_intake_reference_repair as repair_fixture
from tests.test_v8_account_preparation import ACTIVE, POLICY
from v8 import account_preparation as prep, capture
from v8.account_capture_eligibility import identity_capture_evidence
from v8.account_directory_reconciliation import reconcile_directory
from v8.account_intake import submit_account_intake
from v8.account_reference_storage import store_reference
from v8.provider_budget import PaidScope, PaidScopeBlocked
from v8.storage import transaction

AT, LATER, UID, SEC = repair_fixture.AT, repair_fixture.LATER, repair_fixture.UID, repair_fixture.SEC


class PreparationProfileReuseTest(unittest.TestCase):
    def setUp(self):
        self.base = repair_fixture.IntakeReferenceRepairTest()
        self.base.setUp()
        self.addCleanup(self.base.doCleanups)
        self.db = self.base.connection
        self.enterContext(patch.object(prep, "_policy", return_value=POLICY))
        self.enterContext(patch("v8.profile_activations.activation_at", return_value=ACTIVE))
        self.enterContext(patch.object(capture, "execute_intake_fetch", side_effect=AssertionError("network forbidden")))

    def pending(self, *, queued=True, **extra):
        with transaction(self.db):
            self.raw_id, self.raw_path = self.base.raw("existing", account_id=self.base.aid)
            store_reference(self.db, account_identity_id=self.base.iid, platform="douyin", provider="TikHub",
                reference_kind="sec_user_id", reference_value=SEC, source_raw_response_id=self.raw_id,
                created_at=AT, updated_at=AT)
        self.original_bytes = self.raw_path.read_bytes()
        self.raw_path.write_text("{}")
        with transaction(self.db):
            self.request = submit_account_intake(self.db, request_key="pending-profile-recovery",
                value={"platform": "douyin", "uid": UID, **extra}, source={"kind": "fixture"}, at=LATER)
            self.assertEqual(self.request["status"], "accepted")
            self.assertFalse(identity_capture_evidence(self.db, self.base.iid)["eligible"])
            if queued:
                self.assertEqual(prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)["created"], 1)
                self.envelope = json.loads(self.db.execute("SELECT envelope_json FROM capture_work_items").fetchone()[0])

    def restore(self):
        self.raw_path.write_bytes(self.original_bytes)
        self.assertTrue(identity_capture_evidence(self.db, self.base.iid)["eligible"])

    def result(self):
        return json.loads(self.db.execute("SELECT result_json FROM account_intake_requests WHERE id=?",
            (self.request["intake_id"],)).fetchone()[0])

    def test_recovered_profile_finishes_unsent_queue_without_fetch_and_repeats_without_writes(self):
        self.pending()
        self.restore()
        columns = "input_json,source_json,input_sha256,preparation_key,directory_row_id,account_id,account_identity_id"
        before = tuple(self.db.execute("SELECT " + columns + " FROM account_intake_requests").fetchone())
        raw_before = tuple(self.db.execute("SELECT * FROM provider_raw_responses").fetchone())
        with transaction(self.db):
            self.assertEqual(reconcile_directory(self.db, at=LATER)["counts"], {"eligible": 1})
            result = prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)
            self.assertEqual((result["created"], result["pending"], result["reused"]), (0, 0, 1))
            self.assertEqual(self.result()["status"], "ready")
            self.assertEqual(self.result()["existing_profile_evidence"]["raw_response_id"], self.raw_id)
            self.assertEqual(tuple(self.db.execute("SELECT " + columns + " FROM account_intake_requests").fetchone()), before)
            self.assertEqual(tuple(self.db.execute("SELECT * FROM provider_raw_responses").fetchone()), raw_before)
            work = self.db.execute("SELECT state,reason,attempt_count FROM capture_work_items").fetchone()
            self.assertEqual(tuple(work), ("terminal", "existing_profile_evidence_reused", 0))
            scope = PaidScope(purpose="reconcile", **{key: self.envelope[key] for key in prep.SCOPE_FIELDS})
            with self.assertRaisesRegex(PaidScopeBlocked, "preparation_scope_changed"):
                prep.validate_paid_target(self.db, scope, at=LATER, for_payment=True)
            changes = self.db.total_changes
            self.assertEqual(prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)["created"], 0)
            self.assertEqual(self.db.total_changes, changes)
        self.assertEqual(self.raw_path.read_bytes(), self.original_bytes)

    def test_recovered_profile_before_first_plan_creates_no_work(self):
        self.pending(queued=False)
        self.restore()
        with transaction(self.db):
            self.assertEqual(prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)["reused"], 1)
            self.assertEqual(self.db.execute("SELECT COUNT(*) FROM capture_work_items").fetchone()[0], 0)

    def test_corrupt_raw_and_shadow_mode_do_not_finish_pending_requests(self):
        self.pending()
        with transaction(self.db):
            self.assertNotIn("reused", prep.enqueue_pending(self.db, active=ACTIVE, at=LATER))
            self.assertEqual(self.result()["status"], "accepted")
        self.restore()
        with transaction(self.db):
            self.assertNotIn("reused", prep.enqueue_pending(self.db, active=ACTIVE, at=LATER, shadow=True))
            self.assertEqual(self.result()["status"], "accepted")

    def test_live_started_and_paid_hold_work_stay_in_their_original_recovery_path(self):
        self.pending()
        self.restore()
        for state, owner, attempts in (("running", "owner", 1), ("runnable", None, 1), ("paid_identity_hold", None, 0)):
            with self.subTest(state=state), transaction(self.db):
                self.db.execute("UPDATE capture_work_items SET state=?,owner_token=?,attempt_count=?", (state, owner, attempts))
                self.assertNotIn("reused", prep.enqueue_pending(self.db, active=ACTIVE, at=LATER))
                self.assertEqual(self.result()["status"], "accepted")
                self.assertEqual(tuple(self.db.execute("SELECT state,owner_token,attempt_count FROM capture_work_items").fetchone()),
                    (state, owner, attempts))

    def test_existing_fetch_slot_is_not_treated_as_an_unsent_queue_item(self):
        self.pending()
        self.restore()
        with transaction(self.db):
            capture.ensure_intake_slot(self.db, intake_request_id=self.request["intake_id"], stage="profile_prepare",
                window_key=self.envelope["logical_due"], provider="TikHub", adapter_version="fixture")
            self.assertNotIn("reused", prep.enqueue_pending(self.db, active=ACTIVE, at=LATER))
            self.assertEqual(self.result()["status"], "accepted")

    def test_changed_directory_uid_does_not_reuse_old_account_evidence(self):
        self.pending()
        self.restore()
        with transaction(self.db):
            self.db.execute("UPDATE account_directory_rows SET uid='88776655'")
            self.assertNotIn("reused", prep.enqueue_pending(self.db, active=ACTIVE, at=LATER))
            self.assertEqual(self.result()["status"], "accepted")
            self.assertEqual(self.result()["preparation_error"], "preparation_input_changed")

    def test_additional_display_locator_still_requires_its_normal_preparation(self):
        self.pending(display_account_id="unconfirmed_handle")
        self.restore()
        with transaction(self.db):
            self.assertNotIn("reused", prep.enqueue_pending(self.db, active=ACTIVE, at=LATER))
            self.assertEqual(self.result()["status"], "accepted")

    def test_exact_proven_sec_locator_can_reuse(self):
        self.pending(references={"sec_user_id": SEC})
        self.restore()
        with transaction(self.db):
            self.assertEqual(prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)["reused"], 1)

    def test_later_evidence_for_a_different_sec_locator_does_not_confirm_submitted_reference(self):
        with transaction(self.db):
            self.request = submit_account_intake(self.db, request_key="different-sec-input",
                value={"platform": "douyin", "uid": UID, "references": {"sec_user_id": "MS4wLjAB" + "B" * 68}},
                source={"kind": "fixture"}, at=LATER)
            raw_id, _ = self.base.raw("correct-profile", account_id=self.base.aid)
            store_reference(self.db, account_identity_id=self.base.iid, platform="douyin", provider="TikHub",
                reference_kind="sec_user_id", reference_value=SEC, source_raw_response_id=raw_id,
                created_at=AT, updated_at=AT)
            self.assertTrue(identity_capture_evidence(self.db, self.base.iid)["eligible"])
            self.assertNotIn("reused", prep.enqueue_pending(self.db, active=ACTIVE, at=LATER))
            self.assertEqual(self.result()["status"], "accepted")

    def test_valid_raw_for_a_different_uid_cannot_be_reused(self):
        self.base.payload["data"]["data"]["id_str"] = "88776655"
        self.pending()
        self.raw_path.write_bytes(self.original_bytes)
        with transaction(self.db):
            self.assertFalse(identity_capture_evidence(self.db, self.base.iid)["eligible"])
            self.assertNotIn("reused", prep.enqueue_pending(self.db, active=ACTIVE, at=LATER))
            self.assertEqual(self.result()["status"], "accepted")


if __name__ == "__main__":
    unittest.main()
