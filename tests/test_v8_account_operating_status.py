from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from v8.account_operating_receipts import ACCOUNT_STATUS_JOB, find_status_request, load_update_frequencies, record_status_receipt
from v8.account_operating_status import (
    AccountOperatingStatusError,
    account_operating_status,
    update_account_operating_status_in_transaction,
)
from v8.account_states import state_events
from v8.account_roster import RosterError, SYSTEM_SOURCE_FAMILY, get_current_members, require_active_member
from v8.operations import account_read_model, upsert_account
from v8.statistics_scope import content_statistics_scope_sql
from v8.storage import connect, initialize_database, transaction
from v8.system_roster import current_system_members, seal_system_members


class AccountOperatingStatusTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.db = self.root / "accounts.sqlite3"
        self.connection = connect(self.db)
        initialize_database(self.connection, target_version=19)
        self.account_id = upsert_account(
            {"platforms": [{"platform": "douyin", "uid": "123456789", "nickname": "测试账号"}]},
            db_path=self.db,
        )["id"]
        self.identity_id = self.connection.execute(
            "SELECT id FROM account_platform_identities WHERE account_id=?", (self.account_id,)
        ).fetchone()[0]
        result = seal_system_members(
            self.connection,
            [{"platform": "douyin", "uid": "123456789", "nickname": "测试账号",
              "metadata": {"sec_user_id": "MS4w.retained", "display_account_id": "manual-id"}}],
            raw_root=self.root / "raw", actor="test", reason="fixture",
        )
        self.snapshot_id = result["snapshot_id"]
        self.connection.execute(
            """INSERT INTO content_items(link_id,platform,canonical_url,account_id,
                   title,imported_at,created_at,updated_at)
               VALUES ('ABC123','douyin','https://example.test/video',?,'历史内容',
                       '2026-09-01T00:00:00Z','2026-09-01T00:00:00Z','2026-09-01T00:00:00Z')""",
            (self.account_id,),
        )
        self.connection.commit()

    def tearDown(self) -> None:
        self.connection.close()
        self.temporary.cleanup()

    def frequency(self):
        return load_update_frequencies(self.connection, [self.account_id])[self.account_id]

    def account(self):
        return self.connection.execute("SELECT * FROM accounts WHERE id=?", (self.account_id,)).fetchone()

    def change(self, status: str, **extra):
        callback = extra.pop("callback", None)
        with transaction(self.connection):
            return update_account_operating_status_in_transaction(
                self.connection, self.account_id, {"account_status": status, **extra},
                raw_root=self.root / "raw", actor="tester", reason="manual account status",
                schedule_activation=callback,
            )

    def scheduled(self, connection, result):
        self.assertIs(connection, self.connection)
        self.assertTrue(connection.in_transaction)
        return {**result, "activation_status": "scheduled",
                "scheduled_effective_at": (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()}

    def business_rows(self):
        return {table: [tuple(row) for row in self.connection.execute(f"SELECT * FROM {table} ORDER BY rowid")]
                for table in ("accounts", "account_platform_identities", "account_roster_snapshots",
                              "account_roster_members", "content_items", "account_state_events",
                              "scheduler_runs", "scheduler_run_attempts",
                              "acquisition_profile_activations", "pipeline_paid_drain_events")}

    def catalog_directory(self, *, proven=False, identity_status="uid_unverified"):
        from v8.account_directory import import_account_directory
        with transaction(self.connection):
            import_account_directory(self.connection, {
                "sha256": "c" * 64, "source": "fixture.xlsx", "sheet": "accounts", "records": [
                    {"sourceRow": 2, "raw": {"平台": "抖音", "UID": "123456789", "更新状态": "日更"}},
                ],
            }, imported_at="2026-09-08T00:00:00Z")
            self.connection.execute("UPDATE account_directory_rows SET identity_status=?", (identity_status,))
            self.connection.execute("DELETE FROM account_provider_references WHERE account_identity_id=?", (self.identity_id,))
            if proven:
                record_status_receipt(self.connection, request_id="proven-profile", account_id=self.account_id,
                    account_identity_id=self.identity_id, requested_status="daily", update_frequency="daily",
                    request={"account_status": "daily", "fields": {}, "admission": {"member": {
                        "platform": "douyin", "uid": "123456789", "metadata": {"sec_user_id": "MS4wLjAB" + "D" * 64}}}},
                    actor="test", reason="verified fixture profile", before={"enabled": True, "update_frequency": None},
                    after={"enabled": True, "update_frequency": "daily"},
                    result={"id": self.account_id, "status_request_id": "proven-profile", "account_status": "daily",
                            "enabled": True, "update_frequency": "daily"}, timestamp="2026-09-08T01:00:00Z")

    def test_proven_import_identity_can_pause_and_resume_without_changing_old_flag(self):
        from v8.account_directory_status import update_directory_only_status_in_transaction
        self.catalog_directory(proven=True)
        original_history = self.business_rows()
        with patch("v8.account_catalog_capture.installed_policy", return_value={"catalog": "fixture"}):
            for status in ("weekly", "paused", "daily"):
                with self.subTest(status=status), transaction(self.connection):
                    values = {"account_status": status, "status_request_id": "proven-" + status}
                    self.assertIsNone(update_directory_only_status_in_transaction(self.connection, self.account_id,
                        values, actor="tester", reason="status"))
                    result = update_account_operating_status_in_transaction(self.connection, self.account_id, values,
                        raw_root=self.root / "raw", actor="tester", reason="status",
                        schedule_activation=lambda *_: self.fail("catalog must not rebuild roster"))
                    self.assertEqual(result["account_status"], status)
                    self.assertTrue(result["enabled"])
                    self.assertEqual(result["automatic_capture"]["reason_code"], "eligible")
                    self.assertEqual(find_status_request(self.connection, request_id=values["status_request_id"])["payload"]["result"], result)
        self.assertEqual(self.connection.execute("SELECT identity_status FROM account_directory_rows").fetchone()[0], "uid_unverified")
        after = self.business_rows()
        for table in ("account_platform_identities", "account_roster_snapshots", "account_roster_members", "content_items", "account_state_events", "acquisition_profile_activations"):
            self.assertEqual(original_history[table], after[table], table)

    def test_unproven_bound_identity_can_change_labels_without_changing_capture_projection(self):
        from v8.account_directory_status import update_directory_only_status_in_transaction
        self.catalog_directory()
        with patch("v8.account_catalog_capture.installed_policy", return_value={"catalog": "fixture"}):
            for enabled in (0, 1):
                self.connection.execute("UPDATE accounts SET enabled=?", (enabled,))
                self.connection.commit()
                for status in ("paused", "daily", "weekly"):
                    with self.subTest(enabled=enabled, status=status), transaction(self.connection):
                        values = {"account_status": status, "status_request_id": f"no-proof-{enabled}-{status}"}
                        self.assertIsNone(update_directory_only_status_in_transaction(self.connection, self.account_id,
                            values, actor="tester", reason="status"))
                        result = update_account_operating_status_in_transaction(self.connection, self.account_id, values,
                            raw_root=self.root / "raw", actor="tester", reason="status")
                        self.assertEqual(result["account_status"], status)
                        self.assertEqual(result["enabled"], bool(enabled))
                        self.assertFalse(result["automatic_capture"]["eligible"])
                        self.assertEqual(result["automatic_capture"]["reason_code"], "reference_missing")
                        self.assertEqual(self.account()["enabled"], enabled)

    def test_catalog_status_receipt_reports_concrete_missing_evidence(self):
        self.catalog_directory(identity_status="existing_verified")
        with patch("v8.account_catalog_capture.installed_policy", return_value={"catalog": "fixture"}):
            result = self.change("daily", status_request_id="missing-reference")
        self.assertEqual(result["activation_status"], "catalog_managed")
        self.assertEqual(result["automatic_capture"]["reason_code"], "reference_missing")
        self.assertNotIn("等待", result["message"])
        self.assertEqual(find_status_request(self.connection, request_id="missing-reference")["payload"]["result"], result)

    def test_old_directory_label_replay_survives_new_catalog_routing(self):
        from v8.account_directory_status import update_directory_only_status_in_transaction, find_directory_status_request
        self.catalog_directory()
        values = {"account_status": "paused", "status_request_id": "old-directory-label"}
        with transaction(self.connection):
            self.connection.execute("UPDATE accounts SET enabled=0")
            original = update_directory_only_status_in_transaction(self.connection, self.account_id, values,
                actor="tester", reason="status")
        frozen = find_directory_status_request(self.connection, values["status_request_id"])
        # Capture projection may have changed independently since this command.
        self.connection.execute("UPDATE accounts SET enabled=1")
        self.connection.commit()
        before = self.business_rows()
        with patch("v8.account_catalog_capture.installed_policy", return_value={"catalog": "fixture"}), transaction(self.connection):
            replay = update_directory_only_status_in_transaction(self.connection, self.account_id, values,
                actor="tester", reason="status")
        self.assertEqual(replay, {**original, "status_replayed": True})
        self.assertEqual(find_directory_status_request(self.connection, values["status_request_id"]), frozen)
        self.assertEqual(self.business_rows(), before)

    def test_legacy_pending_receipt_replay_projects_reason_without_rewriting_receipt(self):
        self.catalog_directory(identity_status="existing_verified")
        with transaction(self.connection):
            record_status_receipt(self.connection, request_id="legacy-pending", account_id=self.account_id,
                account_identity_id=self.identity_id, requested_status="daily", update_frequency="daily",
                request={"account_status": "daily", "fields": {}}, actor="tester", reason="manual account status",
                before={"enabled": True, "update_frequency": None}, after={"enabled": True, "update_frequency": "daily"},
                result={"id": self.account_id, "status_request_id": "legacy-pending", "account_status": "daily",
                    "enabled": True, "update_frequency": "daily", "activation_status": "pending_verification",
                    "automatic_capture": {"eligible": False, "reason_code": "pending_verification", "reason_label": "等待系统核验"}},
                timestamp="2026-09-08T01:00:00Z")
        frozen = find_status_request(self.connection, request_id="legacy-pending")
        before = self.business_rows()
        with patch("v8.account_catalog_capture.installed_policy", return_value={"catalog": "fixture"}):
            result = self.change("daily", status_request_id="legacy-pending")
        self.assertTrue(result["status_replayed"])
        self.assertEqual(result["activation_status"], "catalog_managed")
        self.assertEqual(result["automatic_capture"]["reason_code"], "reference_missing")
        self.assertEqual(find_status_request(self.connection, request_id="legacy-pending"), frozen)
        self.assertEqual(self.business_rows(), before)

    def test_old_schema_reads_unmarked_and_paused_without_inventing_frequency(self) -> None:
        self.assertEqual(account_operating_status(self.account()), "unmarked")
        model = account_read_model(self.connection, self.account())
        self.assertEqual(model["account_status"], "unmarked")
        self.assertIsNone(model["update_frequency"])
        self.assertNotIn("roster_state", model)
        result = self.change("paused")
        self.assertEqual(result["account_status"], "paused")
        self.assertNotIn("update_frequency", dict(self.account()))

    def test_frequency_write_uses_schema19_receipts_without_schema_changes(self) -> None:
        schema = self.connection.execute("SELECT name,sql FROM sqlite_master ORDER BY name").fetchall()
        self.assertEqual(self.connection.execute("PRAGMA user_version").fetchone()[0], 19)
        result = self.change("daily", operator_name="operator")
        self.assertEqual(result["account_status"], "daily")
        self.assertEqual(self.frequency(), "daily")
        self.assertEqual(self.account()["operator_name"], "operator")
        self.assertTrue(self.account()["enabled"])
        self.assertNotIn("update_frequency", dict(self.account()))
        self.assertEqual(self.connection.execute("SELECT name,sql FROM sqlite_master ORDER BY name").fetchall(), schema)
        self.assertEqual(self.connection.execute("PRAGMA user_version").fetchone()[0], 19)

    def test_daily_weekly_only_change_operator_label(self) -> None:
        before_snapshots = self.connection.execute("SELECT COUNT(*) FROM account_roster_snapshots").fetchone()[0]
        self.change("daily", operator_name="operator")
        result = self.change("weekly", callback=lambda *_: self.fail("Label must not activate roster"))
        self.assertEqual(result["account_status"], "weekly")
        self.assertEqual(self.account()["operator_name"], "operator")
        self.assertTrue(self.account()["enabled"])
        self.assertEqual(state_events(self.connection, self.identity_id), [])
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM account_roster_snapshots").fetchone()[0], before_snapshots)

    def test_marking_enabled_archive_rejoins_without_creating_an_identity(self) -> None:
        seal_system_members(self.connection, [], raw_root=self.root / "raw",
                            actor="test", reason="archive fixture")
        self.assertTrue(self.account()["enabled"])
        result = self.change("weekly", callback=self.scheduled)
        self.assertIn("roster_change", result)
        self.assertEqual(len(current_system_members(self.connection)), 1)
        self.assertEqual(self.connection.execute(
            "SELECT COUNT(*) FROM account_platform_identities WHERE account_id=?", (self.account_id,)
        ).fetchone()[0], 1)

    def test_pause_preserves_frequency_history_and_is_idempotent(self) -> None:
        self.change("weekly")
        callbacks = []
        def schedule(connection, result):
            self.assertIs(connection, self.connection)
            self.assertTrue(connection.in_transaction)
            callbacks.append(result)
            return {**result, "activation_status": "scheduled"}
        result = self.change("paused", callback=schedule)
        self.assertEqual(result["roster_change"]["activation_status"], "not_scheduled")
        self.assertEqual(result["activation_status"], "disabled")
        self.assertNotIn("scheduled_effective_at", result)
        self.assertIn("未安排新的名单激活", result["message"])
        self.assertEqual(self.frequency(), "weekly")
        self.assertEqual(current_system_members(self.connection), [])
        events = state_events(self.connection, self.identity_id)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["actor"], "tester")
        self.assertEqual(events[0]["metadata"]["account_status"], "paused")
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM content_items WHERE account_id=?", (self.account_id,)).fetchone()[0], 1)
        self.change("paused", callback=schedule)
        self.assertEqual(len(state_events(self.connection, self.identity_id)), 1)
        self.assertEqual(len(callbacks), 0)

    def test_resume_reuses_identity_and_historical_member_metadata(self) -> None:
        self.change("paused")
        result = self.change("daily", callback=self.scheduled)
        self.assertEqual(result["account_status"], "daily")
        self.assertEqual(result["activation_status"], "scheduled")
        self.assertIn("已安排于北京时间", result["message"])
        restored = current_system_members(self.connection)
        self.assertEqual(len(restored), 1)
        self.assertEqual(restored[0]["metadata"]["display_account_id"], "manual-id")
        self.assertEqual(restored[0]["sec_user_id"], "MS4w.retained")
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM account_platform_identities").fetchone()[0], 1)
        self.assertEqual([event["new_enabled"] for event in state_events(self.connection, self.identity_id)], [False, True])
        self.change("paused")
        self.assertEqual(current_system_members(self.connection), [])
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM account_roster_snapshots").fetchone()[0], 4)
        self.change("paused")
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM account_roster_snapshots").fetchone()[0], 4)
        self.assertEqual([event["new_enabled"] for event in state_events(self.connection, self.identity_id)], [False, True, False])

    def test_resume_callback_failure_rolls_back_operating_fields_events_and_roster(self) -> None:
        self.change("weekly")
        self.change("paused")
        before = self.business_rows()
        def fail(*_):
            raise RuntimeError("activation unavailable")
        with transaction(self.connection):
            with self.assertRaisesRegex(RuntimeError, "activation unavailable"):
                update_account_operating_status_in_transaction(
                    self.connection, self.account_id,
                    {"account_status": "daily", "operator_name": "must roll back"},
                    raw_root=self.root / "raw", actor="tester", reason="test rollback",
                    schedule_activation=fail,
                )
            self.assertTrue(self.connection.in_transaction)
            self.assertEqual(self.business_rows(), before)
        self.assertEqual(self.business_rows(), before)
        self.assertEqual(self.frequency(), "weekly")

    def _assert_pause_ignores_unavailable_activation(self):
        self.change("weekly")
        historical = [tuple(row) for row in self.connection.execute("SELECT * FROM content_items")]
        old_members = [tuple(row) for row in self.connection.execute(
            "SELECT * FROM account_roster_members WHERE snapshot_id=?", (self.snapshot_id,))]
        unavailable = Mock(side_effect=RuntimeError("qualification expired / writer unavailable / Mode B cannot schedule"))
        result = self.change("paused", callback=unavailable, operator_name="pause saved")
        unavailable.assert_not_called()
        self.assertFalse(self.account()["enabled"])
        self.assertEqual(self.account()["operator_name"], "pause saved")
        self.assertEqual(self.frequency(), "weekly")
        self.assertEqual(result["activation_status"], "disabled")
        self.assertEqual(result["roster_change"]["activation_status"], "not_scheduled")
        self.assertNotIn("scheduled_effective_at", result)
        self.assertNotIn("零点", result["message"])
        self.assertEqual(len(state_events(self.connection, self.identity_id)), 1)
        self.assertEqual(current_system_members(self.connection), [])
        self.assertEqual(get_current_members(self.connection, self.snapshot_id, enabled_only=True), [])
        self.assertEqual(self.connection.execute(
            f"SELECT COUNT(*) FROM content_items c WHERE {content_statistics_scope_sql()}"
        ).fetchone()[0], 0)
        self.assertEqual([tuple(row) for row in self.connection.execute("SELECT * FROM content_items")], historical)
        self.assertEqual([tuple(row) for row in self.connection.execute(
            "SELECT * FROM account_roster_members WHERE snapshot_id=?", (self.snapshot_id,))], old_members)

    def test_schema19_pause_saves_without_calling_unavailable_activation(self) -> None:
        self._assert_pause_ignores_unavailable_activation()

    def test_schema20_pause_saves_without_calling_unavailable_activation(self) -> None:
        initialize_database(self.connection, target_version=20)
        self._assert_pause_ignores_unavailable_activation()

    def test_resume_without_confirmed_future_schedule_rolls_back_every_business_row(self) -> None:
        self.change("weekly")
        self.change("paused")
        before = self.business_rows()
        bad_results = (
            (None, "account_activation_not_scheduled"),
            (lambda conn, result: result, "account_activation_not_scheduled"),
            (lambda *_: None, "account_activation_not_scheduled"),
            (lambda conn, result: {**result, "activation_status": "scheduled"}, "account_activation_time_invalid"),
            (lambda conn, result: {**result, "activation_status": "scheduled", "scheduled_effective_at": "2099-01-01T00:00:00"}, "account_activation_time_invalid"),
            (lambda conn, result: {**result, "activation_status": "scheduled", "scheduled_effective_at": "invalid"}, "account_activation_time_invalid"),
            (lambda conn, result: {**result, "activation_status": "scheduled", "scheduled_effective_at": "2000-01-01T00:00:00Z"}, "account_activation_time_invalid"),
        )
        for callback, code in bad_results:
            with self.subTest(code=code, callback=callback):
                with transaction(self.connection):
                    with self.assertRaises(AccountOperatingStatusError) as caught:
                        update_account_operating_status_in_transaction(
                            self.connection, self.account_id,
                            {"account_status": "daily", "operator_name": "must roll back"},
                            raw_root=self.root / "raw", actor="tester", reason="bad schedule",
                            schedule_activation=callback,
                        )
                    self.assertEqual(caught.exception.code, code)
                    self.assertTrue(self.connection.in_transaction)
                    self.assertEqual(self.business_rows(), before)
                self.assertEqual(self.business_rows(), before)

    def test_disabled_member_in_unactivated_latest_snapshot_also_requires_schedule(self) -> None:
        self.change("paused")
        seal_system_members(self.connection, [{"platform": "douyin", "uid": "123456789"}],
                            raw_root=self.root / "raw", actor="test", reason="accepted but not activated")
        before = self.business_rows()
        with self.assertRaises(AccountOperatingStatusError) as caught:
            self.change("weekly")
        self.assertEqual(caught.exception.code, "account_activation_not_scheduled")
        self.assertEqual(self.business_rows(), before)
        result = self.change("weekly", callback=self.scheduled)
        self.assertTrue(result["roster_change"]["reused_snapshot"])
        self.assertEqual(result["activation_status"], "scheduled")
        self.assertEqual(len(self.business_rows()["account_roster_snapshots"]), len(before["account_roster_snapshots"]))
        self.assertTrue(self.account()["enabled"])

    def test_invalid_status_and_conflicting_enabled_fail_atomically(self) -> None:
        for value in ("unknown", "unmarked", "", None, ["daily"]):
            with self.subTest(value=value), self.assertRaises(AccountOperatingStatusError):
                self.change(value)
        with self.assertRaises(AccountOperatingStatusError) as caught:
            self.change("paused", enabled=True)
        self.assertEqual(caught.exception.code, "account_status_conflict")
        self.assertTrue(self.account()["enabled"])

    def test_daily_weekly_does_not_rebuild_an_archived_roster(self) -> None:
        self.change("daily")
        seal_system_members(self.connection, [], raw_root=self.root / "raw",
                            actor="test", reason="archive after manual label")
        with patch("v8.account_operating_status.current_system_members", side_effect=AssertionError("label cannot inspect/rebuild roster")):
            self.change("weekly")
        self.assertEqual(current_system_members(self.connection), [])
        self.assertEqual(state_events(self.connection, self.identity_id), [])

    def test_replaying_old_request_cannot_overwrite_newer_status_or_fields(self) -> None:
        original = self.change("daily", operator_name="first", status_request_id="request-1")
        self.change("weekly", operator_name="second", status_request_id="request-2")
        replay = self.change("daily", operator_name="first", status_request_id="request-1")
        self.assertEqual(replay, {**original, "status_replayed": True})
        self.assertEqual(self.frequency(), "weekly")
        self.assertEqual(self.account()["operator_name"], "second")
        self.assertEqual(self.connection.execute(
            "SELECT COUNT(*) FROM scheduler_runs WHERE job_id=?", (ACCOUNT_STATUS_JOB,)
        ).fetchone()[0], 2)

    def test_reused_request_with_changed_fields_or_status_is_rejected(self) -> None:
        self.change("daily", operator_name="first", status_request_id="request-1")
        for status, fields in (("daily", {"operator_name": "different"}), ("weekly", {"operator_name": "first"})):
            with self.subTest(status=status), self.assertRaises(AccountOperatingStatusError) as caught:
                self.change(status, **fields, status_request_id="request-1")
            self.assertEqual(caught.exception.code, "account_status_request_conflict")
        self.assertEqual(self.frequency(), "daily")
        self.assertEqual(self.account()["operator_name"], "first")

    def test_receipt_failure_rolls_back_pause_fields_roster_and_event(self) -> None:
        self.change("weekly")
        def seal_then_fail(*args, **kwargs):
            record_status_receipt(*args, **kwargs)
            raise RuntimeError("receipt unavailable")
        with transaction(self.connection), patch(
            "v8.account_operating_status.record_status_receipt", side_effect=seal_then_fail
        ):
            with self.assertRaisesRegex(RuntimeError, "receipt unavailable"):
                update_account_operating_status_in_transaction(
                    self.connection, self.account_id, {"account_status": "paused", "operator_name": "rollback"},
                    raw_root=self.root / "raw", actor="tester", reason="test rollback",
                )
            self.assertTrue(self.connection.in_transaction)
            self.assertTrue(self.account()["enabled"])
            self.assertEqual(self.account()["operator_name"], "")
            self.assertEqual(self.frequency(), "weekly")
        self.assertEqual(state_events(self.connection, self.identity_id), [])
        self.assertEqual(len(current_system_members(self.connection)), 1)
        self.assertEqual(self.connection.execute(
            "SELECT COUNT(*) FROM scheduler_run_attempts a JOIN scheduler_runs r ON r.id=a.scheduler_run_id "
            "WHERE r.job_id=?", (ACCOUNT_STATUS_JOB,)
        ).fetchone()[0], 1)

    def test_request_id_validation_happens_before_edits(self) -> None:
        for value in (None, "", " ", " padded ", 1, "x" * 129):
            with self.subTest(value=value), self.assertRaises(AccountOperatingStatusError) as caught:
                self.change("daily", operator_name="rollback", status_request_id=value)
            self.assertEqual(caught.exception.code, "account_status_request_invalid")
        self.assertEqual(self.account()["operator_name"], "")
        self.assertIsNone(self.frequency())

    def test_pause_remains_in_directory_but_exits_capture_and_statistics(self) -> None:
        roster = {"ready": True, "snapshot_id": self.snapshot_id,
                  "active_profile_id": "tikhub_managed_v1", "source_family": SYSTEM_SOURCE_FAMILY}
        snapshot_hash = self.connection.execute(
            "SELECT members_sha256 FROM account_roster_snapshots WHERE id=?",
            (self.snapshot_id,),
        ).fetchone()[0]
        self.assertEqual(
            require_active_member(self.connection, self.identity_id,
                                  snapshot_id=self.snapshot_id, snapshot_hash=snapshot_hash)["account_id"],
            self.account_id,
        )
        statistics_sql = f"SELECT COUNT(*) FROM content_items c WHERE {content_statistics_scope_sql()}"
        self.assertEqual(self.connection.execute(statistics_sql).fetchone()[0], 1)
        self.change("paused")
        model = account_read_model(self.connection, self.account(), roster=roster)
        self.assertEqual(model["account_status"], "paused")
        self.assertNotIn("roster_state", model)
        self.assertEqual(get_current_members(self.connection, self.snapshot_id, enabled_only=True), [])
        with self.assertRaises(RosterError) as caught:
            require_active_member(self.connection, self.identity_id,
                                  snapshot_id=self.snapshot_id, snapshot_hash=snapshot_hash)
        self.assertEqual(caught.exception.code, "member_scope_changed")
        self.assertEqual(self.connection.execute(statistics_sql).fetchone()[0], 0)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM content_items").fetchone()[0], 1)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM account_roster_members WHERE snapshot_id=?", (self.snapshot_id,)).fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
