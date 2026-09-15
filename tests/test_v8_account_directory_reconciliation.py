"""Persistent directory intake, with real schema22 and no network/provider calls."""
from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from v8 import account_intake, account_preparation, capture_runtime
from v8.account_directory_reconciliation import reconcile_directory, validate_request_directory
from v8.provider_budget import PaidScopeBlocked
from v8.storage import connect, initialize_database

AT = "2026-09-12T03:00:00Z"
LATER = "2026-09-12T04:00:00Z"
ACTIVE = {"activation_id": 1, "profile_id": "integrated_route_v1", "roster_snapshot_id": None, "roster_members_sha256": "a" * 64}


class DirectoryReconciliationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "test.sqlite3"
        self.db = connect(self.path); self.addCleanup(self.db.close)
        initialize_database(self.db, target_version=22)
        self.enterContext(patch("socket.socket.connect", side_effect=AssertionError("Network forbidden")))
        self.eligibility = self.enterContext(patch("v8.account_capture_eligibility.derive_capture_eligibility", return_value={"eligible_members": []}))
        self.db.execute("BEGIN")

    def directory(self, *, platform="douyin", uid="", display="", raw=None):
        cursor = self.db.execute("""INSERT INTO account_directory_rows
            (source_sha256,source_name,source_sheet,source_row,platform,uid,nickname,display_account_id,
             phone,operator_name,account_status,identity_status,raw_json,imported_at,updated_at)
            VALUES(?,?,?,(SELECT count(*)+1 FROM account_directory_rows),?,?,?,?,?,?,'paused','identity_missing',?,?,?)""",
            ("a" * 64, "summary.xlsx", "汇总", platform, uid, "同名账号", display, "00123456789", "同名运营",
             json.dumps(raw or {"原始备注": "只保留"}, ensure_ascii=False), AT, AT))
        return cursor.lastrowid

    def request(self, intake_id):
        return dict(self.db.execute("SELECT * FROM account_intake_requests WHERE id=?", (intake_id,)).fetchone())

    def reconcile(self):
        return reconcile_directory(self.db, at=LATER)

    def test_missing_25_have_durable_reason_and_unchanged_repeat_has_zero_writes(self):
        for _ in range(25):
            self.directory()
        before = [tuple(row) for row in self.db.execute("SELECT * FROM account_directory_rows ORDER BY id")]
        result = self.reconcile()
        self.assertEqual(result["counts"], {"no_locator": 25})
        self.assertEqual(result["sql_writes"], 25)
        self.assertEqual({row["reason"] for row in result["rows"]}, {"identity_missing"})
        self.assertEqual(len(account_intake.preparation_inputs(self.db)), 0)
        self.assertEqual(self.db.execute("SELECT count(*) FROM capture_work_items").fetchone()[0], 0)
        self.assertEqual(self.db.execute("SELECT count(*) FROM accounts").fetchone()[0], 0)
        self.assertEqual([tuple(row) for row in self.db.execute("SELECT * FROM account_directory_rows ORDER BY id")], before)
        self.assertEqual(self.reconcile()["sql_writes"], 0)

    def test_added_locator_automatically_reenters_existing_queue_without_manual_reimport(self):
        did = self.directory()
        blocked = self.reconcile()["rows"][0]
        self.db.execute("UPDATE account_directory_rows SET uid='123456789' WHERE id=?", (did,))
        result = self.reconcile()
        accepted = result["rows"][0]
        self.assertNotEqual(accepted["intake_id"], blocked["intake_id"])
        self.assertEqual(accepted["status"], "accepted")
        self.assertEqual(json.loads(self.request(blocked["intake_id"])["result_json"])["status"], "blocked")
        with patch.object(account_preparation, "_policy", return_value=None):
            queued = account_preparation.enqueue_pending(self.db, active=ACTIVE, at=LATER)
        self.assertEqual(queued["created"], 1)
        self.assertEqual(self.db.execute("SELECT reason FROM capture_work_items").fetchone()[0], "preparation_policy_unavailable")
        self.assertEqual(self.reconcile()["sql_writes"], 0)

    def test_existing_entry_is_reused_without_directory_or_request_writes(self):
        result = account_intake.submit_account_intake(self.db, request_key="web", value={"platform": "douyin", "uid": "123456789"}, source={"kind": "web"}, at=AT)
        check = self.reconcile()
        self.assertEqual(check["sql_writes"], 0)
        self.assertEqual(check["rows"][0]["intake_id"], result["intake_id"])

    def test_legacy_request_is_adopted_once_without_repurchase_or_retimestamping(self):
        first = account_intake.submit_account_intake(self.db, request_key="legacy", value={"platform": "douyin", "uid": "123456789"}, source={"kind": "web"}, at=AT)
        request = self.request(first["intake_id"])
        result = json.loads(request["result_json"])
        result.pop("directory_locator_sha256"); result.pop("directory_locator_snapshot")
        self.db.execute("UPDATE account_intake_requests SET result_json=? WHERE id=?", (json.dumps(result), first["intake_id"]))
        self.assertEqual(self.reconcile()["sql_writes"], 1)
        self.assertEqual(self.request(first["intake_id"])["updated_at"], AT)
        self.assertEqual(self.reconcile()["sql_writes"], 0)
        self.assertEqual(self.db.execute("SELECT count(*) FROM account_intake_requests").fetchone()[0], 1)

    def test_actual_valid_evidence_skips_repreparation(self):
        did = self.directory(uid="123456789")
        self.eligibility.return_value = {"eligible_members": [{"directory_row_id": did}]}
        result = self.reconcile()
        self.assertEqual(result["counts"], {"eligible": 1})
        self.assertEqual(result["sql_writes"], 0)

    def test_completed_ready_journal_does_not_hide_now_invalid_profile_evidence(self):
        self.directory(uid="123456789")
        original = self.reconcile()["rows"][0]
        self.db.execute("UPDATE account_intake_requests SET result_json=?,completed_at=? WHERE id=?",
            (json.dumps({"status": "ready"}), AT, original["intake_id"]))
        result = self.reconcile()
        self.assertEqual(result["rows"][0]["status"], "accepted")
        self.assertNotEqual(result["rows"][0]["intake_id"], original["intake_id"])
        self.assertEqual(self.reconcile()["sql_writes"], 0)

    def test_uid_transition_supersedes_pending_request_and_back_transition_gets_new_revision(self):
        did = self.directory(uid="123456789")
        first = self.reconcile()["rows"][0]
        self.db.execute("UPDATE account_directory_rows SET uid='987654321' WHERE id=?", (did,))
        second = self.reconcile()["rows"][0]
        old = self.request(first["intake_id"])
        self.assertEqual(json.loads(old["result_json"])["status"], "superseded")
        self.assertIsNotNone(old["completed_at"])
        self.db.execute("UPDATE account_directory_rows SET uid='123456789' WHERE id=?", (did,))
        third = self.reconcile()["rows"][0]
        self.assertEqual(len({first["intake_id"], second["intake_id"], third["intake_id"]}), 3)
        self.assertEqual(len(account_intake.preparation_inputs(self.db)), 1)
        self.assertEqual(self.reconcile()["sql_writes"], 0)

    def test_typed_url_edit_blocks_old_send_and_old_apply_before_next_tick(self):
        did = self.directory(uid="123456789", raw={"profile_url": "https://www.douyin.com/user/123456789"})
        first = self.reconcile()["rows"][0]
        self.db.execute("UPDATE account_directory_rows SET raw_json=? WHERE id=?", (json.dumps({"profile_url": "https://www.douyin.com/user/987654321"}), did))
        with self.assertRaisesRegex(PaidScopeBlocked, "preparation_input_changed"):
            account_preparation._current_request(self.db, first["intake_id"])
        with self.assertRaisesRegex(ValueError, "preparation_input_changed"):
            account_intake.apply_prepared_profile(self.db, first["intake_id"], {"platform": "douyin", "uid": "123456789"}, 1, LATER)

    def test_display_edit_blocks_old_result_even_with_same_canonical_uid(self):
        did = self.directory(uid="123456789", display="old_handle")
        first = self.reconcile()["rows"][0]
        self.db.execute("UPDATE account_directory_rows SET display_account_id='new_handle' WHERE id=?", (did,))
        with self.assertRaisesRegex(ValueError, "preparation_input_changed"):
            validate_request_directory(self.db, self.request(first["intake_id"]))
        self.assertEqual(self.reconcile()["counts"], {"accepted": 1})

    def test_manual_metadata_and_source_dates_do_not_trigger_new_preparation(self):
        did = self.directory(uid="123456789")
        first = self.reconcile()["rows"][0]
        self.db.execute("UPDATE account_directory_rows SET phone='00987654321',operator_name='新运营',account_status='daily',raw_json=?,updated_at=?,source_sha256=? WHERE id=?",
            (json.dumps({"原始备注": "用户编辑了普通备注"}), LATER, "b" * 64, did))
        check = self.reconcile()
        self.assertEqual(check["sql_writes"], 0)
        self.assertEqual(check["rows"][0]["intake_id"], first["intake_id"])
        validate_request_directory(self.db, self.request(first["intake_id"]))

    def test_conflicting_uid_rows_are_logged_once_and_never_merged(self):
        self.directory(uid="123456789"); self.directory(uid="123456789")
        check = self.reconcile()
        self.assertEqual(check["counts"], {"conflict": 2})
        self.assertEqual(account_intake.preparation_inputs(self.db), [])
        self.assertEqual(self.reconcile()["sql_writes"], 0)
        self.assertEqual(self.db.execute("SELECT count(*) FROM account_directory_rows").fetchone()[0], 2)

    def test_corrected_conflicting_peer_reopens_unchanged_local_locator(self):
        one = self.directory(uid="123456789"); two = self.directory(uid="123456789")
        original = self.reconcile()
        self.assertEqual(original["counts"], {"conflict": 2})
        self.db.execute("UPDATE account_directory_rows SET uid='987654321' WHERE id=?", (two,))
        check = self.reconcile()
        self.assertEqual(check["counts"], {"accepted": 2})
        self.assertNotEqual(check["rows"][0]["intake_id"], original["rows"][0]["intake_id"])
        self.assertEqual(self.db.execute("SELECT uid FROM account_directory_rows WHERE id=?", (one,)).fetchone()[0], "123456789")
        self.assertEqual(self.reconcile()["sql_writes"], 0)

    def test_unsupported_locator_is_durable_without_queue_and_reopens_on_adapter_support(self):
        self.directory(platform="wechat_channels", display="wxid_unsupported")
        check = self.reconcile()
        self.assertEqual(check["counts"], {"no_locator": 1})
        self.assertEqual(check["rows"][0]["reason"], "locator_resolution_required")
        self.assertEqual(account_intake.preparation_inputs(self.db), [])
        self.assertEqual(self.reconcile()["sql_writes"], 0)
        # A capability upgrade is checked from current code, rather than a
        # permanent status that would require a manual reset or a timer.
        with patch("v8.platform_adapters.next_profile_request", return_value={"operation": "future_supported_resolver"}):
            self.assertEqual(self.reconcile()["counts"], {"accepted": 1})
            self.assertEqual(self.reconcile()["sql_writes"], 0)

    def test_four_platforms_and_paused_accounts_share_one_flow_without_name_phone_merge(self):
        values = [("douyin", "123456789"), ("xiaohongshu", "a" * 24),
                  ("kuaishou", "123456789"), ("wechat_channels", "v2_123abc@finder")]
        for platform, uid in values:
            self.directory(platform=platform, uid=uid)
        check = self.reconcile()
        self.assertEqual(check["counts"], {"accepted": 4})
        self.assertEqual({row["platform"] for row in account_intake.preparation_inputs(self.db)}, {platform for platform, _ in values})
        self.assertEqual(self.db.execute("SELECT count(*) FROM accounts").fetchone()[0], 0)
        self.assertEqual({row[0] for row in self.db.execute("SELECT account_status FROM account_directory_rows")}, {"paused"})

    def test_freeform_notes_do_not_become_profile_identifiers(self):
        self.directory(raw={"原始备注": "同事主页 https://www.douyin.com/user/123456789"})
        self.assertEqual(self.reconcile()["counts"], {"no_locator": 1})

    def test_unactivated_regular_tick_still_covers_missing_rows_without_paid_work(self):
        self.directory(); self.db.commit()
        with patch.object(capture_runtime, "activation_at", return_value=None):
            result = capture_runtime.plan_tick(self.path, at=LATER)
            second = capture_runtime.plan_tick(self.path, at=LATER)
        self.assertEqual(result["status"], "no_activation")
        self.assertEqual(result["directory_reconciliation"]["counts"], {"no_locator": 1})
        self.assertEqual(second["directory_reconciliation"]["sql_writes"], 0)
        self.assertEqual(self.db.execute("SELECT count(*) FROM capture_work_items").fetchone()[0], 0)

    def test_active_regular_tick_runs_reconciliation_before_existing_preparation_planner(self):
        self.directory(uid="123456789"); self.db.commit()
        observed = []
        def enqueue(connection, **_kwargs):
            observed.extend(account_intake.preparation_inputs(connection))
            return {"created": 0}
        with patch.object(capture_runtime, "activation_at", return_value={**ACTIVE, "profile_id": "shadow"}), \
             patch.object(account_preparation, "enqueue_pending", side_effect=enqueue), \
             patch.object(capture_runtime, "_cohort_plan", return_value={"id": 1, "cohort": []}):
            result = capture_runtime.plan_tick(self.path, at=LATER)
        self.assertEqual(len(observed), 1)
        self.assertEqual(result["directory_reconciliation"]["counts"], {"accepted": 1})


if __name__ == "__main__":
    unittest.main()
