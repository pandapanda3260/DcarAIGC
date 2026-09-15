"""Four-platform intake regressions on the actual schema23 migration."""
from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tests import test_v8_account_directory_reconciliation as directory_fixtures
from tests import test_v8_preparation_recovery as recovery_fixtures
from tests.test_v8_account_preparation import AT, ACTIVE, POLICY
from v8 import account_intake as intake, account_preparation as prep, capture
from v8.account_directory import import_account_directory, directory_account_items
from v8.account_directory_reconciliation import directory_locator_snapshot, directory_value, validate_request_directory
from v8.storage import connect, initialize_database
from v8.provider_budget import PaidScopeBlocked

LATER = "2026-09-12T03:10:00Z"


class AccountIdentityRepairV23Test(unittest.TestCase):
    directory = directory_fixtures.DirectoryReconciliationTest.directory
    response = recovery_fixtures.PreparationRecoveryTest.response
    scope = recovery_fixtures.PreparationRecoveryTest.scope

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "test.sqlite3"
        self.db = connect(self.path); self.addCleanup(self.db.close)
        initialize_database(self.db, target_version=23)
        self.enterContext(patch("socket.socket.connect", side_effect=AssertionError("network forbidden")))
        self.enterContext(patch.object(prep, "_policy", return_value=POLICY))
        self.activation = self.enterContext(patch("v8.profile_activations.activation_at", return_value=ACTIVE))
        self.db.execute("BEGIN")

    def row(self, did):
        return dict(self.db.execute("SELECT * FROM account_directory_rows WHERE id=?", (did,)).fetchone())

    def repair(self, did, key="repair", **value):
        return intake.update_directory_identity(self.db, directory_row_id=did, request_key=key,
            expected_locator_sha256=intake._fingerprint(directory_locator_snapshot(self.row(did))),
            value={"platform": "douyin", "uid": "123456789", **value}, source={"actor": "fixture"}, at=AT)

    def submit(self, key, **value):
        return intake.submit_account_intake(self.db, request_key=key,
            value={"platform": "douyin", "uid": "123456789", **value}, source={"kind": "fixture"}, at=AT)

    def queue(self, active=ACTIVE):
        self.activation.return_value = active
        return prep.enqueue_pending(self.db, active=active, at=LATER)

    def envelope(self):
        return json.loads(self.db.execute("SELECT envelope_json FROM capture_work_items ORDER BY id DESC LIMIT 1").fetchone()[0])

    def raw(self):
        env = self.envelope()
        value = {"code": 200, "router": env["request"]["path"], "params": env["request"]["params"],
                 "data": {"status_code": 0, "data": {"id_str": env["preparation_subject"],
                          "sec_uid": "MS4wLjAB" + "A" * 64, "nickname": "fixture"}}}
        response = self.response(value)
        slot = capture.ensure_intake_slot(self.db, intake_request_id=env["intake_request_id"], stage="profile_prepare",
                                         window_key=env["logical_due"], provider="TikHub", adapter_version="fixture")
        attempt = self.db.execute("INSERT INTO fetch_attempts(slot_id,attempt_number,request_started_at,http_status,billed) VALUES (?,1,?,200,1)", (slot, AT)).lastrowid
        claim = capture.SlotClaim(slot_id=slot, attempt_id=attempt, attempt_number=1, content_id=None,
            intake_request_id=env["intake_request_id"], stage="profile_prepare", window_key=env["logical_due"],
            provider="TikHub", adapter_version="fixture", paid_scope_identity="f" * 64)
        raw_id = capture._store_raw_response(self.db, claim=claim, operation=env["operation"], value=value, http_status=200,
            raw_root=Path(self.temp.name).resolve() / "raw", entity_bytes=response.entity_body, transport_receipt=response.receipt)
        self.db.execute("UPDATE fetch_slots SET status='succeeded',attempt_count=1 WHERE id=?", (slot,))
        self.db.execute("UPDATE capture_work_items SET state='paid_identity_hold',reason='local_apply_failed' WHERE intake_request_id=?", (env["intake_request_id"],))
        return raw_id

    def test_legacy_four_platform_import_keeps_uid_and_uses_common_preparation(self):
        records = [{"sourceRow": i + 2, "raw": {"平台": platform, "UID": uid, "更新状态": "日更"}}
                   for i, (platform, uid) in enumerate((("抖音", "123456789"), ("小红书", "a" * 24),
                        ("快手", "987654321"), ("视频号", "v2_abcdef@finder")))]
        payload = {"sha256": "c" * 64, "source": "fixture.xlsx", "sheet": "accounts", "records": records}
        result = import_account_directory(self.db, payload, imported_at=AT)
        self.assertEqual(result["row_count"], 4)
        self.assertEqual([row[0] for row in self.db.execute("SELECT uid FROM account_directory_rows ORDER BY id")], [r["raw"]["UID"] for r in records])
        self.assertEqual(len(intake.preparation_inputs(self.db)), 4)
        self.assertEqual(self.db.execute("SELECT count(*) FROM accounts").fetchone()[0], 0)
        before = self.db.total_changes
        self.assertEqual(import_account_directory(self.db, payload, imported_at=LATER)["status"], "unchanged")
        self.assertEqual(before, self.db.total_changes)

    def test_invalid_source_uid_is_retained_with_blocked_reason(self):
        raw = {"平台": "快手", "UID": 1.25e8, "更新状态": "周更", "原始备注": "keep"}
        result = import_account_directory(self.db, {"sha256": "a" * 64, "source": "fixture.xlsx", "sheet": "accounts",
            "records": [{"sourceRow": 2, "raw": raw}]}, imported_at=AT)
        row = self.row(result["rows"][0]["directory_row_id"])
        self.assertEqual(json.loads(row["raw_json"]), raw)
        request = dict(self.db.execute("SELECT * FROM account_intake_requests").fetchone())
        self.assertEqual(json.loads(request["result_json"])["status"], "blocked")
        self.assertEqual(len(intake.preparation_inputs(self.db)), 0)

    def test_locator_cas_replay_preserves_source_and_readback(self):
        did = self.directory()
        old = self.row(did); expected = intake._fingerprint(directory_locator_snapshot(old))
        result = self.repair(did, profile_url="https://www.douyin.com/user/123456789")
        row = self.row(did)
        self.assertEqual((row["id"], row["locator_revision"], row["uid"]), (did, 1, "123456789"))
        for field in ("raw_json", "source_sha256", "source_name", "source_sheet", "source_row", "imported_at", "account_status", "phone", "operator_name"):
            self.assertEqual(row[field], old[field])
        self.assertEqual(directory_value(row)["profile_url"], "https://www.douyin.com/user/123456789")
        before = self.db.total_changes
        replay = intake.update_directory_identity(self.db, directory_row_id=did, request_key="repair", expected_locator_sha256=expected,
            value={"platform": "douyin", "uid": "123456789", "profile_url": "https://www.douyin.com/user/123456789"}, source={"actor": "fixture"}, at=LATER)
        self.assertTrue(replay["replayed"]); self.assertEqual(before, self.db.total_changes)
        with self.assertRaisesRegex(intake.DirectoryIdentityConflict, "已被修改"):
            intake.update_directory_identity(self.db, directory_row_id=did, request_key="stale", expected_locator_sha256=expected,
                value={"platform": "douyin", "uid": "987654321"}, source={}, at=LATER)
        items = directory_account_items(self.db, roster={}, update_frequencies={}, admission_members={})
        self.assertEqual(items[0]["locator_sha256"], result["locator_sha256"])
        self.assertEqual(items[0]["directory_locator"]["uid"], "123456789")

    def test_prepared_profile_binds_original_missing_row_without_touching_raw(self):
        did = self.directory()
        old_raw = self.row(did)["raw_json"]
        request = self.repair(did)
        self.assertEqual(self.queue()["created"], 1)
        raw_id = self.raw()
        profile = {"platform": "douyin", "uid": "123456789", "nickname": "fixture",
                   "references": {"sec_user_id": "MS4wLjAB" + "A" * 64}}
        result = intake.apply_prepared_profile(self.db, request["intake_id"], profile, raw_id, LATER)
        self.assertEqual(result["directory_row_id"], did)
        self.assertEqual(self.db.execute("SELECT count(*) FROM account_directory_rows").fetchone()[0], 1)
        self.assertEqual(self.row(did)["raw_json"], old_raw)
        self.assertIsNotNone(self.row(did)["account_id"])
        self.assertEqual(self.row(did)["identity_status"], "existing_verified")

    def test_locator_revision_fences_old_inflight_request(self):
        did = self.directory()
        one = self.repair(did)
        old = dict(self.db.execute("SELECT * FROM account_intake_requests WHERE id=?", (one["intake_id"],)).fetchone())
        self.repair(did, key="second", uid="987654321")
        with self.assertRaisesRegex(ValueError, "preparation_input_changed"):
            validate_request_directory(self.db, old)
        self.assertEqual(len(intake.preparation_inputs(self.db)), 1)

    def test_same_uid_redundant_input_has_one_owner_and_zero_write_replan(self):
        first = self.submit("excel")
        self.submit("web", profile_url="https://www.douyin.com/user/123456789")
        self.submit("extra", display_account_id="known_handle")
        self.assertEqual(self.queue()["created"], 1)
        self.assertEqual(self.db.execute("SELECT intake_request_id FROM account_preparation_owners").fetchone()[0], first["intake_id"])
        before = self.db.total_changes
        self.assertEqual(self.queue()["created"], 0)
        self.assertEqual(before, self.db.total_changes)
        self.assertEqual(self.db.execute("SELECT count(*) FROM capture_work_items").fetchone()[0], 1)

    def test_equivalent_owner_does_not_hide_a_conflicting_submitted_reference(self):
        first = self.submit("owner")
        second = self.submit("different-ref", references={"sec_user_id": "MS4wLjAB" + "B" * 64})
        self.assertEqual(self.queue()["created"], 1)
        raw_id = self.raw()
        result = intake.apply_prepared_profile(self.db, first["intake_id"], {"platform": "douyin", "uid": "123456789",
            "nickname": "fixture", "references": {"sec_user_id": "MS4wLjAB" + "A" * 64}}, raw_id, LATER)
        self.assertEqual(result["status"], "ready")
        rejected = self.db.execute("SELECT result_json FROM account_intake_requests WHERE id=?", (second["intake_id"],)).fetchone()[0]
        self.assertEqual(json.loads(rejected)["preparation_error"], "identity_conflict")
        self.assertEqual(self.db.execute("SELECT count(*) FROM accounts").fetchone()[0], 1)

    def test_original_row_cannot_claim_a_uid_already_owned_by_another_directory(self):
        did = self.directory()
        other = self.submit("other")
        before = self.db.total_changes
        with self.assertRaisesRegex(intake.DirectoryIdentityConflict, "另一目录"):
            self.repair(did)
        self.assertEqual(self.db.total_changes, before)
        self.assertNotEqual(did, other["directory_row_id"])

    def test_activation_with_unknown_attempt_cannot_authorize_a_repurchase(self):
        self.submit("fixture"); self.queue()
        env = self.envelope()
        slot = capture.ensure_intake_slot(self.db, intake_request_id=env["intake_request_id"], stage="profile_prepare",
                                         window_key=env["logical_due"], provider="TikHub", adapter_version="fixture")
        self.db.execute("INSERT INTO fetch_attempts(slot_id,attempt_number,request_started_at,http_status,billed) VALUES (?,1,?,NULL,0)", (slot, AT))
        result = self.queue({**ACTIVE, "activation_id": 2})
        self.assertEqual(result["created"], 0)
        self.assertEqual(result["blocked"][0]["reason"], "preparation_retry_raw_missing")
        self.assertEqual(self.db.execute("SELECT count(*) FROM capture_work_items").fetchone()[0], 1)

    def test_activation_switch_resumes_never_sent_without_new_paid_due(self):
        self.submit("fixture"); self.queue()
        old = self.envelope()
        active = {**ACTIVE, "activation_id": 2}
        self.assertEqual(self.queue(active)["created"], 1)
        current = self.envelope()
        self.assertEqual(current["logical_due"], old["logical_due"])
        self.assertEqual(current["preparation_attempt_generation"], old["preparation_attempt_generation"])
        self.assertEqual(self.db.execute("SELECT reason FROM capture_work_items ORDER BY id LIMIT 1").fetchone()[0], "profile_superseded")
        prep.validate_paid_target(self.db, self.scope(current), at=LATER)
        with self.assertRaisesRegex(PaidScopeBlocked, "profile_superseded"):
            prep.validate_paid_target(self.db, self.scope(old), at=LATER)
        before = self.db.total_changes
        self.assertEqual(self.queue(active)["created"], 0); self.assertEqual(before, self.db.total_changes)

    def test_activation_switch_live_worker_holds_and_success_raw_replays(self):
        self.submit("fixture"); self.queue()
        self.db.execute("UPDATE capture_work_items SET state='running',owner_token='worker'")
        active = {**ACTIVE, "activation_id": 2}
        self.assertEqual(self.queue(active)["blocked"][0]["reason"], "preparation_previous_revision_running")
        self.db.execute("UPDATE capture_work_items SET state='paid_identity_hold',owner_token=NULL")
        raw_id = self.raw()
        self.assertEqual(self.queue(active)["created"], 1)
        current = self.envelope()
        self.assertEqual(current["replay_raw_response_id"], raw_id)
        self.assertEqual(current["preparation_attempt_generation"], 0)
        with self.assertRaisesRegex(PaidScopeBlocked, "preparation_replay_network_forbidden"):
            prep.validate_paid_target(self.db, self.scope(current), at=LATER, for_payment=True)


if __name__ == "__main__":
    unittest.main()
