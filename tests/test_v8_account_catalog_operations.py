from __future__ import annotations

import socket
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from uuid import uuid4

from v8.account_capture_eligibility import derive_capture_eligibility
from v8.account_creation import create_managed_account_in_transaction
from v8.account_directory import import_account_directory
from v8.account_directory_status import update_directory_only_status_in_transaction
from v8.account_operating_status import update_account_operating_status_in_transaction
from v8.operations import upsert_account
from v8.storage import connect, initialize_database, transaction


SEC = "MS4wLjAB" + "B" * 64


class AccountCatalogOperationsTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="catalog-operations-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.db = self.root / "candidate.sqlite3"
        self.connection = connect(self.db)
        self.addCleanup(self.connection.close)
        initialize_database(self.connection, target_version=20)
        self.account_id = upsert_account({"platforms": [{"platform": "douyin", "uid": "123456789"}]}, db_path=self.db)["id"]
        with transaction(self.connection):
            imported = import_account_directory(self.connection, {
                "sha256": "a" * 64, "source": "reviewed.xlsx", "sheet": "accounts", "records": [
                    {"sourceRow": 2, "raw": {"平台": "抖音", "UID": "123456789", "昵称": "现有账号", "更新状态": "暂停"}},
                    {"sourceRow": 3, "raw": {"平台": "抖音", "UID": "987654321", "更新状态": "周更"}},
                ],
            }, imported_at="2026-09-08T00:00:00Z")
        self.unverified_id = imported["rows"][1]["account_id"]
        self.directory_id = imported["rows"][0]["directory_row_id"]
        self.identity_id = self.connection.execute("SELECT id FROM account_platform_identities WHERE account_id=?", (self.account_id,)).fetchone()[0]
        self.policy_patch = patch("v8.account_catalog_capture.installed_policy", return_value={"catalog": "verified-test-policy"})
        self.policy = self.policy_patch.start()
        self.addCleanup(self.policy_patch.stop)
        denied = patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden"))
        denied.start()
        self.addCleanup(denied.stop)
        self.schedule = Mock(side_effect=AssertionError("catalog must not schedule a roster"))

    def count(self, table):
        return self.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def status(self, status):
        with transaction(self.connection):
            return update_account_operating_status_in_transaction(self.connection, self.account_id,
                {"account_status": status, "status_request_id": str(uuid4())},
                raw_root=self.root / "raw", actor="test", reason="directory status",
                schedule_activation=self.schedule)

    def create(self, *, status="daily", uid="223456789"):
        with transaction(self.connection):
            return create_managed_account_in_transaction(self.connection, {
                "platform": "douyin", "uid": uid, "nickname": "新增账号",
                "profile_ref": "https://www.douyin.com/user/" + SEC,
                "metadata": {"sec_user_id": SEC},
            }, account_status=status, phone="", operator_name="运营人", request_id=str(uuid4()),
                raw_root=self.root / "raw", actor="test", reason="verified homepage",
                schedule_activation=self.schedule)

    def test_verified_old_nonmember_saves_without_locator_or_old_roster(self):
        counts = [self.count(table) for table in ("account_roster_snapshots", "account_roster_members", "acquisition_profile_activations")]
        result = self.status("daily")
        self.assertEqual(result["account_status"], "daily")
        self.assertEqual(result["activation_status"], "pending_verification")
        self.assertNotIn("roster_change", result)
        self.assertIn("无需另行加入名单", result["message"])
        self.assertEqual(self.connection.execute("SELECT account_status FROM account_directory_rows WHERE account_id=?", (self.account_id,)).fetchone()[0], "daily")
        derived = derive_capture_eligibility(self.connection)
        row = next(row for row in derived["excluded_members"] if row["account_id"] == self.account_id)
        self.assertEqual(row["reason_code"], "reference_missing")
        self.assertEqual([self.count(table) for table in ("account_roster_snapshots", "account_roster_members", "acquisition_profile_activations")], counts)
        self.schedule.assert_not_called()

    def test_pause_and_resume_update_directory_atomically(self):
        self.status("daily")
        result = self.status("paused")
        self.assertEqual(result["automatic_capture"]["reason_code"], "account_paused")
        self.assertIn("历史内容和数据保留", result["message"])
        self.assertFalse(self.connection.execute("SELECT enabled FROM accounts WHERE id=?", (self.account_id,)).fetchone()[0])
        self.assertEqual(self.status("weekly")["account_status"], "weekly")

    def test_unverified_identity_remains_label_only(self):
        with transaction(self.connection):
            result = update_directory_only_status_in_transaction(self.connection, self.unverified_id,
                {"account_status": "daily", "status_request_id": str(uuid4())}, actor="test", reason="label")
        self.assertFalse(result["enabled"])
        self.assertEqual(self.connection.execute("SELECT identity_status FROM account_directory_rows WHERE account_id=?", (self.unverified_id,)).fetchone()[0], "uid_unverified")
        with transaction(self.connection):
            self.assertIsNone(update_directory_only_status_in_transaction(self.connection, self.account_id,
                {"account_status": "daily", "status_request_id": str(uuid4())}, actor="test", reason="normal status"))

    def test_new_verified_account_is_immediately_in_directory_and_next_plan(self):
        result = self.create()
        self.assertEqual(result["creation_action"], "created")
        self.assertNotIn("roster_change", result)
        row = self.connection.execute("SELECT * FROM account_directory_rows WHERE account_id=?", (result["account_id"],)).fetchone()
        self.assertEqual(row["identity_status"], "existing_verified")
        self.assertEqual(row["account_status"], "daily")
        self.assertEqual(row["operator_name"], "运营人")
        member = next(row for row in derive_capture_eligibility(self.connection)["eligible_members"] if row["account_id"] == result["account_id"])
        self.assertEqual(member["locator_evidence"]["kind"], "verified_account_admission")
        reference = self.connection.execute("SELECT reference_value,source_raw_response_id FROM account_provider_references WHERE account_identity_id=?", (member["identity_id"],)).fetchone()
        self.assertEqual(tuple(reference), (SEC, None))
        self.assertEqual(self.count("account_roster_snapshots"), 0)
        self.schedule.assert_not_called()

    def test_new_account_readiness_has_locator_but_still_obeys_provider_gate(self):
        from v8.capture_runtime import _readiness
        result = self.create()
        identity_id = self.connection.execute("SELECT id FROM account_platform_identities WHERE account_id=?", (result["account_id"],)).fetchone()[0]
        envelope = {"catalog_plan_id": 1, "identity_id": identity_id, "assignment_id": 1,
                    "stage": "discovery", "operation": "douyin_user_posts"}
        with patch("v8.account_catalog_capture.assignment_for_plan", return_value={"id": 1, "mode": "active", "route": "integrated"}):
            self.assertEqual(_readiness(self.connection, envelope, at="2026-09-10T00:00:00Z"),
                             ("provider_blocked", "provider_transport_blocked"))
        self.assertEqual(self.count("provider_raw_responses"), 0)
        self.assertEqual(self.count("provider_usage"), 0)

    def test_planner_materializes_historical_admission_without_scope_drift(self):
        from v8.account_catalog_capture import materialize_proven_locators
        self.create()
        self.connection.execute("DELETE FROM account_provider_references")
        self.connection.commit()
        before = derive_capture_eligibility(self.connection)
        with transaction(self.connection):
            self.assertEqual(materialize_proven_locators(self.connection, before["eligible_members"],
                                                        at="2026-09-10T00:00:00Z"), 1)
            self.assertEqual(materialize_proven_locators(self.connection, before["eligible_members"],
                                                        at="2026-09-10T00:00:00Z"), 0)
        after = derive_capture_eligibility(self.connection)
        self.assertEqual(before, after)
        self.assertEqual(self.count("provider_raw_responses"), 0)

    def test_locator_materialization_never_replaces_conflicting_reference(self):
        from v8.account_catalog_capture import materialize_proven_locators
        self.create()
        before = derive_capture_eligibility(self.connection)
        self.connection.execute("UPDATE account_provider_references SET reference_value=?", ("MS4wLjAB" + "C" * 64,))
        self.connection.commit()
        with self.assertRaises(ValueError):
            with transaction(self.connection):
                materialize_proven_locators(self.connection, before["eligible_members"], at="2026-09-10T00:00:00Z")
        self.assertEqual(self.connection.execute("SELECT reference_value FROM account_provider_references").fetchone()[0], "MS4wLjAB" + "C" * 64)

    def test_create_replay_reads_operating_state_from_directory_not_capture_projection(self):
        from v8.account_creation import replay_account_creation
        from v8.account_operating_receipts import find_status_request
        result = self.create()
        request_id = result["request_id"]
        context = find_status_request(self.connection, request_id=request_id)["payload"]["request"]["admission"]["input"]
        self.connection.execute("UPDATE accounts SET enabled=0 WHERE id=?", (result["account_id"],))
        self.connection.commit()
        replay = replay_account_creation(self.connection, request_id=request_id, request_context=context)
        self.assertEqual(replay["account_status"], "daily")
        self.assertEqual(replay["current_account_status"], "daily")
        self.assertFalse(replay["current_enabled"])
        self.assertEqual(replay["activation_status"], "pending_verification")

    def test_new_paused_account_keeps_verified_admission_but_is_not_selected(self):
        result = self.create(status="paused")
        member = next(row for row in derive_capture_eligibility(self.connection)["excluded_members"] if row["account_id"] == result["account_id"])
        self.assertEqual(member["reason_code"], "account_paused")
        self.assertEqual(self.count("account_roster_snapshots"), 0)

    def test_receipt_failure_rolls_back_directory_and_new_identity(self):
        tables = ("accounts", "account_platform_identities", "account_directory_rows", "scheduler_runs")
        before = [self.count(table) for table in tables]
        with patch("v8.account_operating_status.record_status_receipt", side_effect=ValueError("receipt failed")):
            with self.assertRaisesRegex(ValueError, "receipt failed"):
                self.create()
        self.assertEqual([self.count(table) for table in tables], before)
        with patch("v8.account_operating_status.record_status_receipt", side_effect=ValueError("receipt failed")):
            with self.assertRaisesRegex(ValueError, "receipt failed"):
                self.status("daily")
        self.assertEqual(self.connection.execute("SELECT account_status FROM account_directory_rows WHERE account_id=?", (self.account_id,)).fetchone()[0], "paused")

    def test_current_state_validation_skips_legacy_route_only_with_policy(self):
        from v8.api import _validate_active_account_capture
        with patch("v8.account_roster_capture.validate_current_account_capture") as legacy:
            _validate_active_account_capture(self.connection, self.account_id)
            legacy.assert_not_called()
            self.policy.return_value = None
            _validate_active_account_capture(self.connection, self.account_id)
            legacy.assert_called_once_with(self.connection, account_id=self.account_id)

    def test_directory_search_reports_published_capture_reason(self):
        from v8.api import AccountSearchRequest, _account_search
        with patch("v8.api.active_release", return_value={}), patch("v8.account_catalog_capture.public_statuses", return_value={
            self.directory_id: {"eligible": False, "reason_code": "reference_missing", "reason_label": "缺少可用的账号定位信息", "account_status": "daily",
                "account_id": self.account_id, "identity_id": self.identity_id, "platform": "douyin", "uid": "123456789", "identity_status": "existing_verified"},
        }):
            self.status("daily")
            result = _account_search(AccountSearchRequest(query="现有账号"), db_path=self.db)
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["items"][0]["automatic_capture"]["reason_code"], "reference_missing")

    def test_public_capture_status_does_not_survive_identity_changes(self):
        from v8.account_catalog_capture import annotate_accounts
        def account():
            return {"id": self.account_id, "directory_row_id": self.directory_id, "account_status": "daily",
                    "directory_identity_status": "existing_verified", "directory_platform": "douyin", "directory_uid": "123456789",
                    "platforms": [{"id": self.identity_id, "platform": "douyin", "uid": "123456789"}]}
        published = {self.directory_id: {"account_id": self.account_id, "identity_id": self.identity_id,
            "account_status": "daily", "identity_status": "existing_verified", "platform": "douyin", "uid": "123456789",
            "eligible": True, "reason_code": "eligible", "reason_label": "可自动采集"}}
        with patch("v8.account_catalog_capture.public_statuses", return_value=published):
            current = account()
            annotate_accounts(self.connection, [current])
            self.assertTrue(current["automatic_capture"]["eligible"])
            for field, replacement, reason in (
                ("directory_identity_status", "uid_unverified", "identity_unverified"),
                ("directory_uid", "777777777", "identity_conflict"),
                ("id", 999, "pending_verification"),
                ("account_status", "paused", "account_paused"),
            ):
                with self.subTest(field=field):
                    current = account()
                    current[field] = replacement
                    annotate_accounts(self.connection, [current])
                    self.assertFalse(current["automatic_capture"]["eligible"])
                    self.assertEqual(current["automatic_capture"]["reason_code"], reason)
            current = account()
            current["directory_uid"] = current["platforms"][0]["uid"] = "777777777"
            annotate_accounts(self.connection, [current])
            self.assertEqual(current["automatic_capture"]["reason_code"], "pending_verification")

    def test_legacy_search_without_published_catalog_keeps_existing_model(self):
        from v8.account_catalog_capture import annotate_accounts
        current = {"directory_row_id": self.directory_id, "account_status": "daily"}
        annotate_accounts(self.connection, [current])
        self.assertNotIn("automatic_capture", current)


if __name__ == "__main__":
    unittest.main()
