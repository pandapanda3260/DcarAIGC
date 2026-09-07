from __future__ import annotations

import socket
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from fastapi import HTTPException
from fastapi.testclient import TestClient

from tests.test_v8_api import _seed_read_model_database, _test_config
from v8 import api
from v8.account_roster import RosterError
from v8.account_states import set_account_enabled_in_transaction
from v8.storage import connect, transaction
from v8.system_roster import current_system_members, seal_system_members


class FixedBusinessClock(datetime):
    calls = 0

    @classmethod
    def now(cls, tz=None):
        # Keep a fixed business day while preserving append-only event order.
        cls.calls += 1
        instant = datetime(2026, 9, 6, 12, tzinfo=timezone.utc) + timedelta(microseconds=cls.calls)
        return instant.astimezone(tz) if tz is not None else instant.replace(tzinfo=None)


class AccountCreationApiTest(unittest.TestCase):
    def setUp(self) -> None:
        FixedBusinessClock.calls = 0
        clock_patch = patch("v8.account_operating_status.datetime", FixedBusinessClock)
        clock_patch.start()
        self.addCleanup(clock_patch.stop)
        temporary = tempfile.TemporaryDirectory(prefix="dcar-account-create-api-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.config = _test_config(self.root)
        _seed_read_model_database(self.config.db_path)
        with connect(self.config.db_path) as connection:
            self.account_id = connection.execute("SELECT id FROM accounts").fetchone()[0]
            self.identity_id = connection.execute("SELECT id FROM account_platform_identities").fetchone()[0]
            connection.execute("UPDATE account_platform_identities SET uid='123456789'")
            connection.commit()
            roster = seal_system_members(connection, [{"platform": "douyin", "uid": "123456789"}],
                                         raw_root=self.root / "raw", actor="test", reason="fixture")
        self.runtime = {"active_profile_id": "tikhub_managed_v1", "ready": True,
                        "source_family": "system", "snapshot_id": roster["snapshot_id"]}
        self.start_patch(patch.object(api, "runtime_account_summary", return_value=self.runtime))
        self.schedule = self.start_patch(patch.object(api, "_schedule_writer_roster_activation",
            side_effect=lambda request, conn, result, **kwargs: {**result, "activation_status": "scheduled",
                                                                 "scheduled_effective_at": "2026-09-07T16:00:00Z"}))
        self.actual_resolver = api._resolve_account_creation_profile
        self.lookup = self.start_patch(patch.object(api, "_resolve_account_creation_profile", side_effect=self.resolve))
        self.start_patch(patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden")))
        self.client = TestClient(api.create_app(self.config))
        self.addCleanup(self.client.close)

    def start_patch(self, patcher):
        mock = patcher.start()
        self.addCleanup(patcher.stop)
        return mock

    def resolve(self, request, connection, profile_url):
        self.assertFalse(connection.in_transaction, "profile lookup must not hold the write transaction")
        return {"platform": "douyin", "uid": "987654321", "nickname": "resolved profile",
                "profile_ref": "https://www.douyin.com/user/MS4w.resolved", "sec_user_id": "MS4w.resolved"}

    def payload(self, **values):
        return {"profile_url": "https://www.douyin.com/user/MS4w.resolved", "phone": "",
                "operator_name": "", "account_status": "daily", "request_id": str(uuid4()), **values}

    def count(self, table):
        with connect(self.config.db_path) as connection:
            return connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def disable(self):
        with connect(self.config.db_path) as connection, transaction(connection):
            set_account_enabled_in_transaction(connection, self.identity_id, enabled=False,
                effective_at="2026-09-06T00:00:00Z", created_at="2026-09-06T00:00:00Z",
                actor="test", reason="historical paused fixture")

    def test_profile_mode_requires_explicit_status_and_uuid_before_lookup(self):
        valid = self.payload()
        for body in ({key: value for key, value in valid.items() if key != "account_status"},
                     {key: value for key, value in valid.items() if key != "request_id"},
                     {**valid, "account_status": None}, {**valid, "account_status": "unmarked"},
                     {**valid, "request_id": "wrong"}, {**valid, "uid": "987654321"}):
            with self.subTest(body=body):
                self.assertEqual(self.client.post("/api/v8/accounts", json=body).status_code, 422)
        self.lookup.assert_not_called()
        self.assertEqual(self.count("accounts"), 1)

    def test_create_returns_real_saved_state_and_scheduled_activation(self):
        result = self.client.post("/api/v8/accounts", json=self.payload(account_status="weekly", phone="13212343053",
                                                                       operator_name="operator"))
        self.assertEqual(result.status_code, 200, result.text)
        saved = result.json()
        self.assertEqual(saved["account_status"], "weekly")
        self.assertTrue(saved["enabled"])
        self.assertEqual(saved["activation_status"], "scheduled")
        with connect(self.config.db_path) as connection:
            account = connection.execute("SELECT * FROM accounts WHERE id=?", (saved["account_id"],)).fetchone()
            self.assertEqual(account["phone"], "13212343053")
            self.assertEqual(account["operator_name"], "operator")
        self.schedule.assert_called_once()

    def test_paused_creation_has_no_roster_or_activation_and_is_not_current(self):
        before = self.count("account_roster_snapshots")
        result = self.client.post("/api/v8/accounts", json=self.payload(account_status="paused"))
        self.assertEqual(result.status_code, 200, result.text)
        self.assertFalse(result.json()["enabled"])
        self.assertEqual(result.json()["activation_status"], "disabled")
        self.assertIn("不会采集", result.json()["message"])
        self.assertEqual(self.count("account_roster_snapshots"), before)
        self.schedule.assert_not_called()
        with connect(self.config.db_path) as connection:
            self.assertNotIn("987654321", {row["uid"] for row in current_system_members(connection)})

    def test_historical_disabled_same_uid_resumes_but_enabled_duplicate_cannot_overwrite(self):
        self.disable()
        self.lookup.side_effect = lambda *args: {"platform": "douyin", "uid": "123456789", "nickname": "saved identity"}
        body = self.payload()
        result = self.client.post("/api/v8/accounts", json=body)
        self.assertEqual(result.status_code, 200, result.text)
        self.assertEqual(result.json()["account_id"], self.account_id)
        self.assertTrue(result.json()["enabled"])
        duplicate = self.client.post("/api/v8/accounts", json={**body, "request_id": str(uuid4()), "phone": "13212343053"})
        self.assertEqual(duplicate.status_code, 409)
        self.assertEqual(duplicate.headers["X-DCAR-Roster-Error"], "system_member_exists")
        self.assertEqual(self.count("accounts"), 1)

    def test_same_input_retry_reuses_receipt_before_lookup_and_conflict_does_not_write(self):
        body = self.payload()
        first = self.client.post("/api/v8/accounts", json=body)
        second = self.client.post("/api/v8/accounts", json=body)
        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual(second.status_code, 200, second.text)
        self.assertEqual(first.json()["account_id"], second.json()["account_id"])
        self.assertTrue(second.json()["replayed"])
        self.lookup.assert_called_once()
        conflict = self.client.post("/api/v8/accounts", json={**body, "account_status": "paused"})
        self.assertEqual(conflict.status_code, 409)
        self.lookup.assert_called_once()
        self.schedule.assert_called_once()

    def test_resolver_or_activation_failure_cannot_report_success_or_leave_partial_rows(self):
        before = {table: self.count(table) for table in ("accounts", "account_roster_snapshots", "account_state_events")}
        self.lookup.side_effect = HTTPException(status_code=503, detail="profile unavailable")
        self.assertEqual(self.client.post("/api/v8/accounts", json=self.payload()).status_code, 503)
        self.lookup.side_effect = self.resolve
        self.schedule.side_effect = RosterError("activation_failed", "activation refused")
        self.assertEqual(self.client.post("/api/v8/accounts", json=self.payload()).status_code, 409)
        self.assertEqual({table: self.count(table) for table in before}, before)

    def test_invalid_resolved_uid_is_rejected_without_account_write(self):
        self.lookup.side_effect = lambda *args: {"platform": "douyin", "uid": "bad"}
        result = self.client.post("/api/v8/accounts", json=self.payload())
        self.assertEqual(result.status_code, 422)
        self.assertEqual(self.count("accounts"), 1)

    def test_old_uid_creation_payloads_even_with_status_are_rejected_without_lookup_or_writes(self):
        self.disable()
        tables = ("accounts", "account_platform_identities", "account_roster_snapshots",
                  "account_state_events", "scheduler_runs", "scheduler_run_attempts")
        with connect(self.config.db_path) as connection:
            before = {table: [tuple(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY id")]
                      for table in tables}
        old_forms = (
            {"platform": "douyin", "uid": "987654321"},
            {"platform": "douyin", "uid": "987654321", "account_status": "daily", "request_id": str(uuid4())},
            {"platform": "douyin", "uid": "123456789", "nickname": "overwrite", "sec_user_id": "MS4w.old",
             "account_status": "weekly", "phone": "13212343053", "operator_name": "overwrite", "request_id": str(uuid4())},
            {**self.payload(), "platform": "douyin", "uid": "987654321"},
        )
        for body in old_forms:
            with self.subTest(body=body):
                response = self.client.post("/api/v8/accounts", json=body)
                self.assertEqual(response.status_code, 422, response.text)
                self.assertIn("刷新账号页后使用主页链接", response.json()["detail"])
        self.lookup.assert_not_called()
        self.schedule.assert_not_called()
        with connect(self.config.db_path) as connection:
            after = {table: [tuple(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY id")]
                     for table in tables}
        self.assertEqual(after, before)
        self.assertFalse(hasattr(api, "SystemAccountMemberRequest"))

    def test_real_resolver_boundary_requires_verified_public_result_and_reuses_paused_proof(self):
        from v8 import account_profile_public

        self.lookup.side_effect = self.actual_resolver
        sec = "MS4wLjAB" + "a" * 40
        url = "https://www.douyin.com/user/" + sec
        with patch.object(account_profile_public, "public_profile_lookup", return_value={
            "platform": "douyin", "uid": "987654321", "sec_user_id": sec,
            "nickname": "verified public profile", "display_account_id": "display123",
        }) as public_lookup:
            paused = self.client.post("/api/v8/accounts", json=self.payload(profile_url=url, account_status="paused"))
            self.assertEqual(paused.status_code, 200, paused.text)
            resumed = self.client.post("/api/v8/accounts", json=self.payload(profile_url=url, account_status="daily"))
            self.assertEqual(resumed.status_code, 200, resumed.text)
            self.assertEqual(paused.json()["account_id"], resumed.json()["account_id"])
            self.assertTrue(resumed.json()["enabled"])
            public_lookup.assert_called_once()

    def test_real_resolver_rejects_invalid_host_and_public_failure_without_any_account_write(self):
        from v8 import account_profile_public
        from v8.account_profile_input import ProfileInputError

        self.lookup.side_effect = self.actual_resolver
        with patch.object(account_profile_public, "public_profile_lookup", side_effect=ProfileInputError(
            "public_profile_unavailable", "公开资料暂不可用",
        )) as public_lookup:
            invalid = self.client.post("/api/v8/accounts", json=self.payload(profile_url="https://127.0.0.1/user/123456789"))
            self.assertEqual(invalid.status_code, 422)
            public_lookup.assert_not_called()
            unavailable = self.client.post("/api/v8/accounts", json=self.payload(profile_url="https://www.douyin.com/user/987654321"))
            self.assertEqual(unavailable.status_code, 503)
        self.assertEqual(self.count("accounts"), 1)


class Schema20AccountCreationApiTest(unittest.TestCase):
    def _payload(self, fixture, *, new=False):
        uid = "987654321" if new else fixture.uid
        sec = "MS4wLjAB" + "n" * 40 if new else fixture.sec
        member = {"platform": "douyin", "uid": uid, "nickname": "resolved fixture profile",
                  "profile_ref": "https://www.douyin.com/user/" + sec, "sec_user_id": sec}
        payload = {"profile_url": member["profile_ref"], "phone": "13212343053",
                   "operator_name": "must roll back", "account_status": "weekly", "request_id": str(uuid4())}
        return payload, member

    def _assert_active_restore_rejected(self, *, missing_routes):
        from tests import test_v8_account_status_api as fixtures
        from v8 import account_roster_capture

        fixture = fixtures._setup_schema20_capture_api(self, missing_routes=missing_routes)
        if not missing_routes:
            fixtures._close_schema20_gate(fixture)
        before = fixtures._account_database_rows(fixture.db)
        payload, member = self._payload(fixture)
        with (
            patch.object(api, "_resolve_account_creation_profile", return_value=member),
            patch.object(account_roster_capture, "validate_current_account_capture",
                         wraps=account_roster_capture.validate_current_account_capture) as validate,
            patch.object(api, "_schedule_writer_roster_activation",
                         wraps=api._schedule_writer_roster_activation) as schedule,
        ):
            response = fixture.client.post("/api/v8/accounts", json=payload)
        self.assertEqual(response.status_code, 409, response.text)
        self.assertEqual(response.headers["X-DCAR-Account-Error"], "account_roster_capture_unavailable")
        if missing_routes:
            self.assertIn("尚未具备完整的采集设置", response.json()["detail"])
        validate.assert_called_once()
        self.assertEqual(validate.call_args.kwargs["account_id"], fixture.account_id)
        schedule.assert_not_called()
        self.assertEqual(fixtures._account_database_rows(fixture.db), before)

    def test_active_disabled_create_restore_without_routes_rolls_back_every_api_write(self):
        self._assert_active_restore_rejected(missing_routes=True)

    def test_active_disabled_create_restore_with_closed_gate_rolls_back_every_api_write(self):
        self._assert_active_restore_rejected(missing_routes=False)

    def test_real_schema20_scheduler_rejects_unqualified_new_account_with_exact_id_and_no_writes(self):
        from tests import test_v8_account_status_api as fixtures
        from v8 import account_roster_capture

        fixture = fixtures._setup_schema20_capture_api(self)
        fixtures._close_schema20_gate(fixture)
        before = fixtures._account_database_rows(fixture.db)
        payload, member = self._payload(fixture, new=True)
        with (
            patch.object(api, "_resolve_account_creation_profile", return_value=member),
            patch.object(api, "_schedule_writer_roster_activation",
                         wraps=api._schedule_writer_roster_activation) as api_schedule,
            patch.object(account_roster_capture, "schedule_account_roster_capture_in_transaction",
                         wraps=account_roster_capture.schedule_account_roster_capture_in_transaction) as capture_schedule,
        ):
            response = fixture.client.post("/api/v8/accounts", json=payload)
        self.assertEqual(response.status_code, 409, response.text)
        self.assertEqual(response.headers["X-DCAR-Account-Error"], "account_roster_capture_unavailable")
        api_schedule.assert_called_once()
        capture_schedule.assert_called_once()
        admitted = api_schedule.call_args.args[2]["account_id"]
        self.assertIsInstance(admitted, int)
        self.assertNotEqual(admitted, fixture.account_id)
        self.assertEqual(capture_schedule.call_args.kwargs["account_id"], admitted)
        self.assertEqual(fixtures._account_database_rows(fixture.db), before)

    def test_real_scheduler_without_writer_admission_cannot_save_a_new_account(self):
        from tests import test_v8_account_status_api as fixtures
        from v8 import account_roster_capture

        fixture = fixtures._setup_schema20_capture_api(self)
        fixture.app.state.writer_lock_held = False
        before = fixtures._account_database_rows(fixture.db)
        payload, member = self._payload(fixture, new=True)
        with (
            patch.object(api, "_resolve_account_creation_profile", return_value=member),
            patch.object(api, "_schedule_writer_roster_activation",
                         wraps=api._schedule_writer_roster_activation) as api_schedule,
            patch.object(account_roster_capture, "schedule_account_roster_capture_in_transaction",
                         wraps=account_roster_capture.schedule_account_roster_capture_in_transaction) as capture_schedule,
        ):
            response = fixture.client.post("/api/v8/accounts", json=payload)
        self.assertEqual(response.status_code, 409, response.text)
        self.assertIn("账号本次没有保存", response.json()["detail"])
        api_schedule.assert_called_once()
        capture_schedule.assert_not_called()
        self.assertEqual(fixtures._account_database_rows(fixture.db), before)


if __name__ == "__main__":
    unittest.main()
