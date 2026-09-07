from __future__ import annotations

import tempfile
import unittest
import socket
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient

from tests.test_v8_api import _seed_read_model_database, _test_config
from v8 import api
from v8.account_states import set_account_enabled_in_transaction
from v8.account_operating_receipts import ACCOUNT_STATUS_JOB
from v8.storage import connect, transaction, initialize_database
from v8.system_roster import seal_system_members


CAPTURE_API_TIME = "2026-09-02T16:05:00Z"


def _account_database_rows(db_path):
    """Compare every persisted fixture row, including business and audit facts."""
    with connect(db_path) as connection:
        return {
            row[0]: sorted(
                (tuple(value) for value in connection.execute(
                    'SELECT * FROM "' + row[0].replace('"', '""') + '"'
                )), key=repr,
            )
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
        }


def _setup_schema20_capture_api(test, *, missing_routes=False):
    """Real activation, Writer lease, gates and validator; isolated install proof.

    The shared capture fixture substitutes installation/sample files only. API,
    state services, schedule preparation and route/gate validation stay real.
    """
    from tests import test_v8_account_roster_capture as capture_fixture
    from tests import test_v8_profile_control as profile_fixture
    from v8 import account_roster, storage

    capture = capture_fixture.AccountRosterCaptureTest()
    test.addCleanup(capture.doCleanups)
    original_snapshot = profile_fixture.ProfileControlTest._snapshot

    def sealed_snapshot(owner, connection, family, key):
        if family != "system":
            return original_snapshot(owner, connection, family, key)
        result = seal_system_members(
            connection, [{"platform": "douyin", "uid": "100001", "nickname": "source member",
                          "sec_user_id": "MS4wLjAB" + "s" * 40}],
            raw_root=owner.root / "rosters", actor="fixture", reason=key,
            sealed_at="2026-09-01T00:00:00Z",
        )
        return account_roster.snapshot_by_id(connection, result["snapshot_id"])

    # API admission seals complete rosters and must inherit a real matching
    # system scope, rather than the capture fixture's minimal scope placeholder.
    with patch.object(profile_fixture.ProfileControlTest, "_snapshot", sealed_snapshot):
        capture.setUp()
    sec = "MS4wLjAB" + "x" * 32 + "123456"
    with connect(capture.db) as connection:
        sealed = seal_system_members(connection, [{
            "platform": "douyin", "uid": "123456", "nickname": "original profile",
            "profile_ref": "https://www.douyin.com/user/" + sec, "sec_user_id": sec,
        }], raw_root=capture.root / "rosters", actor="fixture", reason="active API member",
            sealed_at=capture_fixture.AFTER)
        snapshot = account_roster.snapshot_by_id(connection, sealed["snapshot_id"])
        account_id = connection.execute(
            "SELECT account_id FROM account_platform_identities WHERE uid='123456'"
        ).fetchone()[0]
    capture.schedule(snapshot, account_id=account_id)

    def disable(at):
        with connect(capture.db) as connection, transaction(connection):
            identity_id = connection.execute(
                "SELECT id FROM account_platform_identities WHERE account_id=?", (account_id,)
            ).fetchone()[0]
            connection.execute(
                "UPDATE accounts SET phone='13200001111',operator_name='original operator' WHERE id=?",
                (account_id,),
            )
            set_account_enabled_in_transaction(
                connection, identity_id, enabled=False, effective_at=at, created_at=at,
                actor="fixture", reason="historical disabled member remains in active roster",
            )

    if missing_routes:
        disable("2026-09-02T15:59:00Z")
    capture.publish()
    if not missing_routes:
        disable("2026-09-02T16:04:00Z")

    class CaptureClock(datetime):
        @classmethod
        def now(cls, tz=None):
            instant = datetime.fromisoformat(CAPTURE_API_TIME.replace("Z", "+00:00"))
            return instant.astimezone(tz) if tz is not None else instant.replace(tzinfo=None)

    test.enterContext(patch("v8.account_operating_status.datetime", CaptureClock))
    test.enterContext(patch.object(storage, "now_utc", return_value=CAPTURE_API_TIME))
    test.enterContext(patch.object(api, "runtime_account_summary", side_effect=lambda connection:
        account_roster.runtime_account_summary(connection, at=CAPTURE_API_TIME)))
    test.enterContext(patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden")))
    config = _test_config(capture.root, db_name=capture.db.name)
    app = api.create_app(config)
    # The fixture already owns the real process/inode-bound Writer lease.
    app.state.writer_lock_held = True
    client = TestClient(app)
    test.addCleanup(client.close)
    with connect(capture.db) as connection:
        active = account_roster.runtime_account_summary(connection, at=CAPTURE_API_TIME)
        test.assertEqual(active["active_profile_id"], "integrated_route_v1")
        test.assertEqual(active["snapshot_id"], snapshot["id"])
        test.assertIsNotNone(connection.execute(
            "SELECT 1 FROM account_roster_members m JOIN account_platform_identities i "
            "ON i.id=m.account_identity_id WHERE m.snapshot_id=? AND i.account_id=?",
            (active["snapshot_id"], account_id),
        ).fetchone())
    return SimpleNamespace(capture=capture, client=client, app=app, account_id=account_id,
                           db=capture.db, uid="123456", sec=sec)


def _close_schema20_gate(fixture):
    from v8 import capture_authorizations as auth

    fields = {"provider": "tikhub", "operation": "douyin_user_posts", "state": "closed",
              "reason": "operator-hold", "evidence_json": "{}", "recorded_at": "2026-09-02T16:04:30Z"}
    with connect(fixture.db) as connection, transaction(connection):
        connection.execute(
            f"INSERT INTO capture_paid_send_gate_events({','.join(fields)},event_sha256) "
            f"VALUES ({','.join('?' for _ in range(len(fields) + 1))})",
            (*fields.values(), auth.digest(fields)),
        )


class Schema20AccountStatusApiTest(unittest.TestCase):
    def _assert_restore_rejected_without_writes(self, *, missing_routes):
        from v8 import account_roster_capture

        fixture = _setup_schema20_capture_api(self, missing_routes=missing_routes)
        if not missing_routes:
            _close_schema20_gate(fixture)
        before = _account_database_rows(fixture.db)
        with (
            patch.object(account_roster_capture, "validate_current_account_capture",
                         wraps=account_roster_capture.validate_current_account_capture) as validate,
            patch.object(api, "_schedule_writer_roster_activation",
                         wraps=api._schedule_writer_roster_activation) as schedule,
        ):
            response = fixture.client.patch(f"/api/v8/accounts/{fixture.account_id}", json={
                "account_status": "weekly", "operator_name": "must roll back", "phone": "13212343053",
                "status_request_id": "restore-active-without-capture",
            })
        self.assertEqual(response.status_code, 409, response.text)
        self.assertEqual(response.headers["X-DCAR-Account-Error"], "account_roster_capture_unavailable")
        if missing_routes:
            self.assertIn("尚未具备完整的采集设置", response.json()["detail"])
        validate.assert_called_once()
        self.assertEqual(validate.call_args.kwargs["account_id"], fixture.account_id)
        schedule.assert_not_called()  # Already in the real active roster.
        self.assertEqual(_account_database_rows(fixture.db), before)

    def test_active_disabled_restore_without_routes_rolls_back_every_api_write(self):
        self._assert_restore_rejected_without_writes(missing_routes=True)

    def test_active_disabled_restore_with_closed_gate_rolls_back_every_api_write(self):
        self._assert_restore_rejected_without_writes(missing_routes=False)


class AccountStatusApiTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
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
                                         raw_root=self.root / "rosters", actor="test", reason="fixture")
        self.runtime = {"active_profile_id": "tikhub_managed_v1", "ready": True,
                        "source_family": "system", "snapshot_id": roster["snapshot_id"]}
        self.addCleanup(patch.stopall)
        patch.object(api, "runtime_account_summary", return_value=self.runtime).start()
        self.schedule = patch.object(api, "_schedule_writer_roster_activation",
                                     side_effect=lambda request, conn, result, **kwargs: {
                                         **result, "activation_status": "scheduled",
                                         "scheduled_effective_at": (datetime.now(timezone.utc) + timedelta(days=1)).isoformat(),
                                     }).start()
        self.client = TestClient(api.create_app(self.config))
        self.addCleanup(self.client.close)

    def search(self, **values):
        response = self.client.post("/api/v8/accounts/search", json={"scope": "all", **values})
        self.assertEqual(response.status_code, 200)
        return response.json()

    def test_schema19_frequency_persists_without_migration(self) -> None:
        self.assertEqual(self.search()["items"][0]["account_status"], "unmarked")
        self.assertEqual(self.search(account_status="daily")["total"], 0)
        response = self.client.patch(f"/api/v8/accounts/{self.account_id}",
                                     json={"account_status": "daily", "operator_name": "updated"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.search()["items"][0]["operator_name"], "updated")
        self.assertEqual(self.search()["items"][0]["account_status"], "daily")
        with connect(self.config.db_path) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 19)
            self.assertNotIn("update_frequency", {row[1] for row in connection.execute("PRAGMA table_info(accounts)")})
        self.schedule.assert_not_called()

    def test_manual_frequency_pause_resume_and_status_filter(self) -> None:
        url = f"/api/v8/accounts/{self.account_id}"
        for status in ("daily", "weekly"):
            self.assertEqual(self.client.patch(url, json={"account_status": status}).status_code, 200)
            self.assertEqual(self.search(account_status=status)["total"], 1)
        self.schedule.assert_not_called()
        with connect(self.config.db_path) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM account_state_events").fetchone()[0], 0)
        self.assertEqual(self.client.patch(url, json={"account_status": "paused"}).status_code, 200)
        self.assertEqual(self.search()["total"], 1)
        paused = self.search(account_status="paused")
        self.assertEqual(paused["total"], 1)
        self.assertEqual(paused["items"][0]["update_frequency"], "weekly")
        self.assertEqual(self.client.patch(url, json={"account_status": "daily"}).status_code, 200)
        self.assertEqual(self.search(account_status="daily")["total"], 1)
        self.assertEqual(self.schedule.call_count, 1)
        with connect(self.config.db_path) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM content_items").fetchone()[0], 1)

    def test_pause_works_for_unmarked_account(self) -> None:
        response = self.client.patch(f"/api/v8/accounts/{self.account_id}", json={"account_status": "paused"})
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()["enabled"])
        self.assertEqual(self.search()["total"], 1)
        self.assertEqual(self.search(account_status="paused")["total"], 1)

    def test_pause_does_not_depend_on_collection_but_resume_requires_scheduling(self) -> None:
        url = f"/api/v8/accounts/{self.account_id}"
        self.schedule.side_effect = RuntimeError("collection unavailable")
        paused = self.client.patch(url, json={"account_status": "paused"})
        self.assertEqual(paused.status_code, 200, paused.text)
        self.assertFalse(paused.json()["enabled"])
        self.assertNotIn("下一激活边界生效", paused.json()["message"])
        self.schedule.assert_not_called()
        with connect(self.config.db_path) as connection:
            before = {table: [tuple(row) for row in connection.execute(f"SELECT * FROM {table}")]
                      for table in ("accounts", "account_state_events", "account_roster_snapshots", "scheduler_runs")}
        self.schedule.side_effect = lambda request, conn, result, **kwargs: result
        resumed = self.client.patch(url, json={"account_status": "daily"})
        self.assertEqual(resumed.status_code, 409, resumed.text)
        with connect(self.config.db_path) as connection:
            for table, rows in before.items():
                self.assertEqual([tuple(row) for row in connection.execute(f"SELECT * FROM {table}")], rows, table)
        self.assertEqual(self.search(account_status="paused")["total"], 1)

    def test_obsolete_enabled_write_cannot_bypass_account_status_flow(self) -> None:
        with connect(self.config.db_path) as connection:
            before = {table: [tuple(row) for row in connection.execute(f"SELECT * FROM {table}")]
                      for table in ("accounts", "account_state_events", "account_roster_snapshots", "scheduler_runs")}
        for value in (True, False):
            response = self.client.patch(f"/api/v8/accounts/{self.account_id}", json={"enabled": value})
            self.assertEqual(response.status_code, 422, response.text)
        with connect(self.config.db_path) as connection:
            for table, rows in before.items():
                self.assertEqual([tuple(row) for row in connection.execute(f"SELECT * FROM {table}")], rows, table)
        self.schedule.assert_not_called()

    def test_schema20_integrated_keeps_manual_status_and_pause_controls(self) -> None:
        with connect(self.config.db_path) as connection:
            initialize_database(connection, target_version=20)
        self.runtime["active_profile_id"] = "integrated_route_v1"
        url = f"/api/v8/accounts/{self.account_id}"
        for status in ("daily", "paused", "weekly"):
            result = self.client.patch(url, json={"account_status": status})
            self.assertEqual(result.status_code, 200, result.text)
            self.assertEqual(result.json()["account_status"], status)
        self.assertEqual(self.search(account_status="weekly")["total"], 1)
        with connect(self.config.db_path) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM content_items").fetchone()[0], 1)
        self.runtime["active_profile_id"] = "matrix_hybrid_v1"
        self.assertEqual(self.client.patch(url, json={"account_status": "paused"}).status_code, 409)

    def test_invalid_status_or_competing_enabled_is_rejected(self) -> None:
        for data in ({"account_status": "monthly"}, {"account_status": None},
                     {"account_status": "paused", "enabled": True}):
            self.assertEqual(self.client.patch(f"/api/v8/accounts/{self.account_id}", json=data).status_code, 422)
        self.assertTrue(self.search()["items"][0]["enabled"])

    def test_duplicate_status_request_does_not_override_later_edit(self) -> None:
        url = f"/api/v8/accounts/{self.account_id}"
        original = {"account_status": "daily", "status_request_id": "account-edit-a"}
        self.assertEqual(self.client.patch(url, json=original).status_code, 200)
        self.assertEqual(self.client.patch(url, json={"account_status": "weekly", "status_request_id": "account-edit-b"}).status_code, 200)
        self.assertEqual(self.client.patch(url, json=original).status_code, 200)
        self.assertEqual(self.search()["items"][0]["account_status"], "weekly")
        conflict = self.client.patch(url, json={**original, "operator_name": "different intent"})
        self.assertEqual(conflict.status_code, 409)
        with connect(self.config.db_path) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM scheduler_runs WHERE job_id=?", (ACCOUNT_STATUS_JOB,)).fetchone()[0], 2)

    def test_account_controls_do_not_appear_as_collection_jobs(self) -> None:
        self.assertEqual(self.client.patch(f"/api/v8/accounts/{self.account_id}", json={"account_status": "daily"}).status_code, 200)
        response = self.client.get("/api/v8/scheduler")
        self.assertEqual(response.status_code, 200)
        self.assertNotIn(ACCOUNT_STATUS_JOB, response.text)

    def test_pause_filters_content_count_window_and_quality_then_restores(self) -> None:
        start = datetime(2026, 8, 3, tzinfo=timezone.utc)
        for enabled, expected in ((True, 1), (False, 0), (True, 1)):
            with connect(self.config.db_path) as connection, transaction(connection):
                timestamp = datetime.now(timezone.utc).isoformat(timespec="microseconds")
                set_account_enabled_in_transaction(connection, self.identity_id, enabled=enabled,
                    effective_at=timestamp, created_at=timestamp, actor="test", reason="statistics fixture")
                rows = api._window_content_rows(connection, start, start + timedelta(days=1))
                self.assertEqual(len(rows), expected)
            for body in ({}, {"content_direction": "new_car"}):
                result = self.client.post("/api/v8/contents/search", json=body)
                self.assertEqual(result.status_code, 200)
                self.assertEqual(result.json()["total"], expected)
            overview = self.client.get("/api/v8/overview")
            self.assertEqual(overview.status_code, 200)
            points = self.client.get("/api/v8/selling-points")
            self.assertEqual(points.status_code, 200)
