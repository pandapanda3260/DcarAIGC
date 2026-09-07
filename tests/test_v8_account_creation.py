from __future__ import annotations

import socket
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch
from uuid import uuid4

from v8.account_creation import create_managed_account_in_transaction
from v8.account_operating_receipts import ACCOUNT_STATUS_JOB, load_update_frequencies
from v8.account_operating_status import AccountOperatingStatusError, update_account_operating_status_in_transaction
from v8.account_roster import RosterError
from v8.account_states import set_account_enabled_in_transaction
from v8.operations import upsert_account
from v8.storage import connect, initialize_database, transaction
from v8.system_roster import current_system_members, remove_system_member, seal_system_members


class FixedBusinessClock(datetime):
    calls = 0

    @classmethod
    def now(cls, tz=None):
        # Keep a fixed business day while preserving append-only event order.
        cls.calls += 1
        instant = datetime(2026, 9, 6, 12, tzinfo=timezone.utc) + timedelta(microseconds=cls.calls)
        return instant.astimezone(tz) if tz is not None else instant.replace(tzinfo=None)


class AccountCreationTest(unittest.TestCase):
    def setUp(self) -> None:
        FixedBusinessClock.calls = 0
        clock_patch = patch("v8.account_operating_status.datetime", FixedBusinessClock)
        clock_patch.start()
        self.addCleanup(clock_patch.stop)
        temporary = tempfile.TemporaryDirectory(prefix="dcar-account-create-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.db = self.root / "fixture.sqlite3"
        self.connection = connect(self.db)
        self.addCleanup(self.connection.close)
        initialize_database(self.connection)
        self.member = {"platform": "douyin", "uid": "123456789", "nickname": "historical"}
        self.old_id = upsert_account({"platforms": [self.member], "phone": "13212343053",
                                      "operator_name": "original operator"}, db_path=self.db)["id"]
        self.old_identity = self.connection.execute("SELECT id FROM account_platform_identities").fetchone()[0]
        self.snapshot = seal_system_members(self.connection, [self.member], raw_root=self.root / "raw",
                                           actor="test", reason="initial fixture")["snapshot_id"]
        self.schedule = Mock(side_effect=lambda conn, result: {**result, "activation_status": "scheduled",
                                                              "scheduled_effective_at": "2026-09-07T16:00:00Z"})
        denied_network = patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden"))
        denied_network.start()
        self.addCleanup(denied_network.stop)

    def create(self, uid="987654321", status="daily", **kwargs):
        member = kwargs.pop("member", {"platform": "douyin", "uid": uid})
        with transaction(self.connection):
            return create_managed_account_in_transaction(
                self.connection, member, account_status=status,
                raw_root=self.root / "raw", actor="test", reason="explicit account creation",
                schedule_activation=self.schedule, **kwargs,
            )

    def row(self, account_id):
        return self.connection.execute("SELECT * FROM accounts WHERE id=?", (account_id,)).fetchone()

    def count(self, table):
        return self.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def disable_old(self):
        with transaction(self.connection):
            set_account_enabled_in_transaction(self.connection, self.old_identity, enabled=False,
                effective_at="2026-09-06T00:00:00Z", created_at="2026-09-06T00:00:00Z",
                actor="test", reason="historical disabled fixture")

    def test_new_daily_and_weekly_have_real_labels_events_and_one_final_roster(self):
        for uid, status in (("987654321", "daily"), ("987654322", "weekly")):
            before = self.count("account_roster_snapshots")
            result = self.create(uid, status, phone="", operator_name="")
            self.assertEqual(result["account_status"], status)
            self.assertTrue(result["enabled"])
            self.assertEqual(result["activation_status"], "scheduled")
            self.assertEqual(result["scheduled_effective_at"], "2026-09-07T16:00:00Z")
            self.assertIn("北京时间 2026-09-08 00:00", result["message"])
            self.assertEqual(load_update_frequencies(self.connection, [result["account_id"]])[result["account_id"]], status)
            self.assertEqual(self.count("account_roster_snapshots"), before + 1)
        self.assertEqual({row["uid"] for row in current_system_members(self.connection)},
                         {"123456789", "987654321", "987654322"})
        self.assertEqual(self.count("account_state_events"), 2)
        self.assertEqual(self.schedule.call_count, 2)
        self.assertEqual(self.connection.execute("PRAGMA user_version").fetchone()[0], 19)

    def test_new_paused_never_enters_roster_or_schedules_activation(self):
        before = self.count("account_roster_snapshots")
        result = self.create(status="paused", member={"platform": "douyin", "uid": "987654321",
            "profile_ref": "https://www.douyin.com/user/MS4w.paused", "sec_user_id": "MS4w.paused"})
        self.assertFalse(result["enabled"])
        self.assertEqual(result["activation_status"], "disabled")
        self.assertEqual(self.row(result["account_id"])["enabled"], 0)
        self.assertEqual(self.count("account_roster_snapshots"), before)
        self.schedule.assert_not_called()
        with transaction(self.connection):
            resumed = update_account_operating_status_in_transaction(self.connection, result["account_id"],
                {"account_status": "weekly"}, raw_root=self.root / "raw", actor="test", reason="later resume",
                schedule_activation=self.schedule)
        self.assertTrue(resumed["enabled"])
        member = next(row for row in current_system_members(self.connection) if row["uid"] == "987654321")
        self.assertEqual(member["sec_user_id"], "MS4w.paused")
        self.assertEqual(member["profile_ref"], "https://www.douyin.com/user/MS4w.paused")

    def test_disabled_member_still_in_latest_recovers_same_identity_and_blanks_preserve_fields(self):
        self.disable_old()
        before = self.count("account_roster_snapshots")
        result = self.create("123456789", "weekly", phone="  ", operator_name="", active_snapshot_id=self.snapshot)
        self.assertEqual(result["account_id"], self.old_id)
        self.assertTrue(result["enabled"])
        self.assertEqual(result["activation_status"], "active")
        self.assertEqual(self.count("account_roster_snapshots"), before)
        self.assertEqual(self.count("account_platform_identities"), 1)
        self.assertEqual(self.row(self.old_id)["phone"], "13212343053")
        self.assertEqual(self.row(self.old_id)["operator_name"], "original operator")
        self.assertEqual(self.count("account_state_events"), 2)
        self.schedule.assert_not_called()

    def test_disabled_historical_rejoin_preserves_history_and_writes_nonempty_fields(self):
        self.disable_old()
        remove_system_member(self.connection, self.old_id, raw_root=self.root / "raw", actor="test", reason="archive")
        self.connection.execute("""INSERT INTO content_items(link_id,platform,canonical_url,account_id,title,
            imported_at,created_at,updated_at) VALUES ('ABC123','douyin','https://example.test/123',?,'history',
            '2026-09-01T00:00:00Z','2026-09-01T00:00:00Z','2026-09-01T00:00:00Z')""", (self.old_id,))
        self.connection.commit()
        facts = [tuple(row) for row in self.connection.execute("SELECT * FROM content_items")]
        result = self.create("123456789", "daily", phone="13212343054", operator_name="replacement")
        self.assertEqual(result["account_id"], self.old_id)
        self.assertEqual(result["creation_action"], "restored")
        self.assertEqual(self.row(self.old_id)["phone"], "13212343054")
        self.assertEqual(self.row(self.old_id)["operator_name"], "replacement")
        self.assertEqual([tuple(row) for row in self.connection.execute("SELECT * FROM content_items")], facts)
        self.assertEqual(self.count("account_platform_identities"), 1)
        self.schedule.assert_called_once()

    def test_disabled_latest_member_without_active_membership_must_get_real_schedule(self):
        self.disable_old()
        snapshots = self.count("account_roster_snapshots")
        result = self.create("123456789", "weekly")
        self.assertEqual(result["activation_status"], "scheduled")
        self.assertEqual(self.count("account_roster_snapshots"), snapshots)
        self.assertTrue(result["roster_change"]["reused_snapshot"])
        self.schedule.assert_called_once()

    def test_unscheduled_acceptance_rolls_back_instead_of_returning_pending_success(self):
        self.disable_old()
        self.schedule.side_effect = lambda conn, result: result
        for uid in ("123456789", "987654321"):
            with self.assertRaises(AccountOperatingStatusError) as error:
                self.create(uid)
            self.assertEqual(error.exception.code, "account_activation_not_scheduled")
            self.assertEqual(self.count("accounts"), 1)
            self.assertFalse(self.row(self.old_id)["enabled"])

    def test_active_and_pending_duplicates_never_overwrite(self):
        for status in ("daily", "weekly", "paused"):
            with self.assertRaises(RosterError) as error:
                self.create("123456789", status, phone="13212343054", operator_name="replacement")
            self.assertEqual(error.exception.code, "system_member_exists")
        self.assertEqual(self.row(self.old_id)["phone"], "13212343053")
        created = self.create()
        with self.assertRaises(RosterError):
            self.create()
        self.assertTrue(self.row(created["account_id"])["enabled"])

    def test_member_removed_from_latest_but_still_active_cannot_be_added_as_duplicate(self):
        remove_system_member(self.connection, self.old_id, raw_root=self.root / "raw", actor="test", reason="archive")
        with self.assertRaises(RosterError):
            self.create("123456789", active_snapshot_id=self.snapshot)

    def test_explicit_add_enabled_labeled_history_rejoins_without_changing_patch_label_semantics(self):
        with transaction(self.connection):
            update_account_operating_status_in_transaction(self.connection, self.old_id,
                {"account_status": "weekly"}, raw_root=self.root / "raw", actor="test", reason="label")
        remove_system_member(self.connection, self.old_id, raw_root=self.root / "raw", actor="test", reason="archive")
        result = self.create("123456789", "daily")
        self.assertTrue(result["enabled"])
        self.assertIn("123456789", {row["uid"] for row in current_system_members(self.connection)})
        self.schedule.assert_called_once()

    def test_retry_is_idempotent_conflicting_input_rejected_and_later_pause_is_preserved(self):
        request_id = str(uuid4())
        original = self.create(request_id=request_id)
        before = self.count("account_roster_snapshots")
        replay = self.create(request_id=request_id)
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["account_id"], original["account_id"])
        self.assertEqual(self.count("account_roster_snapshots"), before)
        self.schedule.assert_called_once()
        with self.assertRaises(AccountOperatingStatusError):
            self.create(request_id=request_id, operator_name="different input")
        with transaction(self.connection):
            update_account_operating_status_in_transaction(self.connection, original["account_id"],
                {"account_status": "paused"}, raw_root=self.root / "raw", actor="test", reason="later pause")
        replay = self.create(request_id=request_id)
        self.assertFalse(replay["current_enabled"])
        self.assertEqual(replay["current_account_status"], "paused")
        self.assertFalse(replay["enabled"])
        self.assertEqual(replay["account_status"], "paused")
        self.assertEqual(replay["original_account_status"], "daily")
        self.assertTrue(replay["original_enabled"])
        self.assertEqual(replay["activation_status"], "disabled")
        self.assertNotIn("scheduled_effective_at", replay)
        self.assertFalse(self.row(original["account_id"])["enabled"])

    def test_activation_failure_rolls_back_new_and_restored_even_if_outer_catches(self):
        self.disable_old()
        remove_system_member(self.connection, self.old_id, raw_root=self.root / "raw", actor="test", reason="archive")
        counts = {table: self.count(table) for table in ("accounts", "account_platform_identities",
            "account_roster_snapshots", "account_state_events", "scheduler_runs")}
        self.schedule.side_effect = RuntimeError("activation refused")
        for uid in ("987654321", "123456789"):
            with transaction(self.connection):
                try:
                    create_managed_account_in_transaction(self.connection, {"platform": "douyin", "uid": uid},
                        account_status="daily", phone="13212343054", raw_root=self.root / "raw",
                        actor="test", reason="failed fixture", schedule_activation=self.schedule)
                except RuntimeError:
                    pass
            self.assertEqual({table: self.count(table) for table in counts}, counts)
            self.assertFalse(self.row(self.old_id)["enabled"])
            self.assertEqual(self.row(self.old_id)["phone"], "13212343053")

    def test_receipt_failure_rolls_back_after_schedule_callback(self):
        counts = {table: self.count(table) for table in ("accounts", "account_roster_snapshots", "account_state_events")}
        with patch("v8.account_operating_status.record_status_receipt", side_effect=RuntimeError("receipt failed")):
            with self.assertRaises(RuntimeError):
                self.create()
        self.schedule.assert_called_once()
        self.assertEqual({table: self.count(table) for table in counts}, counts)

    def test_bad_uid_or_missing_status_never_creates_account_or_roster(self):
        for uid, status in (("not-a-uid", "daily"), ("987654321", "unmarked")):
            with self.assertRaises((RosterError, AccountOperatingStatusError)):
                self.create(uid, status)
        self.assertEqual(self.count("accounts"), 1)
        self.assertEqual(self.count("account_roster_snapshots"), 1)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM scheduler_runs WHERE job_id=?",
                                                (ACCOUNT_STATUS_JOB,)).fetchone()[0], 0)

    def test_new_paused_profile_cannot_claim_another_existing_identity(self):
        self.connection.execute("""INSERT INTO account_provider_references(account_identity_id,provider,
            reference_kind,reference_value,created_at,updated_at) VALUES (?,'tikhub','sec_user_id','MS4w.taken',
            '2026-09-06T00:00:00Z','2026-09-06T00:00:00Z')""", (self.old_identity,))
        self.connection.commit()
        with self.assertRaises(RosterError):
            self.create(status="paused", member={"platform": "douyin", "uid": "987654321", "sec_user_id": "MS4w.taken"})
        self.assertEqual(self.count("accounts"), 1)

    def test_uppercase_provider_reference_conflict_cannot_be_overwritten(self):
        self.disable_old()
        self.connection.execute("""INSERT INTO account_provider_references(account_identity_id,provider,
            reference_kind,reference_value,created_at,updated_at) VALUES (?,'TikHub','sec_user_id','MS4w.old',
            '2026-09-06T00:00:00Z','2026-09-06T00:00:00Z')""", (self.old_identity,))
        self.connection.commit()
        with self.assertRaises(RosterError) as error:
            self.create(status="paused", member={**self.member, "sec_user_id": "MS4w.changed"})
        self.assertEqual(error.exception.code, "identity_conflict")
        with self.assertRaises(RosterError):
            seal_system_members(self.connection, [{**self.member, "sec_user_id": "MS4w.changed"}],
                                raw_root=self.root / "raw", actor="test", reason="conflicting fixture")
        self.assertEqual(self.connection.execute("SELECT reference_value FROM account_provider_references").fetchone()[0], "MS4w.old")

    def test_paused_receipt_profile_cannot_be_reclaimed_and_blank_readmission_keeps_reference(self):
        member = {"platform": "douyin", "uid": "987654321", "sec_user_id": "MS4w.paused",
                  "profile_ref": "https://www.douyin.com/user/MS4w.paused"}
        paused = self.create(status="paused", member=member)
        with self.assertRaises(RosterError):
            self.create(status="paused", member={**member, "uid": "987654322"})
        self.create("987654321", "paused")
        with transaction(self.connection):
            update_account_operating_status_in_transaction(self.connection, paused["account_id"],
                {"account_status": "weekly"}, raw_root=self.root / "raw", actor="test", reason="resume saved profile",
                schedule_activation=self.schedule)
        saved = next(row for row in current_system_members(self.connection) if row["uid"] == "987654321")
        self.assertEqual(saved["sec_user_id"], "MS4w.paused")

    def test_xhs_case_duplicates_are_rejected_and_historical_spelling_is_preserved(self):
        uid = "abcdef0123456789abcdef01"
        created = self.create(member={"platform": "xiaohongshu", "uid": uid.upper()})
        self.assertEqual(created["uid"], uid)
        with self.assertRaises(RosterError):
            self.create(member={"platform": "xiaohongshu", "uid": uid.upper()})
        old_uid = "BBCDEF0123456789ABCDEF01"
        old = upsert_account({"platforms": [{"platform": "xiaohongshu", "uid": old_uid}], "enabled": False}, db_path=self.db)
        restored = self.create(member={"platform": "xiaohongshu", "uid": old_uid.lower()})
        self.assertEqual(restored["account_id"], old["id"])
        self.assertEqual(restored["uid"], old_uid)


if __name__ == "__main__":
    unittest.main()
