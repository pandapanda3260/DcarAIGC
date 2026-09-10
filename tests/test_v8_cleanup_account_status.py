"""Real temporary cleanup authority and API pause/resume within its fixed scope."""
from __future__ import annotations

from datetime import datetime, timedelta
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from tests import test_v8_account_cleanup_runtime as cleanup_fixture
from tests.test_v8_api import _test_config
from v8 import api, account_cleanup_runtime as cleanup, account_operating_status as status_service, account_directory_status
from v8 import account_roster, account_roster_capture, capture_authorizations as auth, capture_planning, storage
from v8.account_directory import import_account_directory
from v8.account_states import state_events


NOW = "2026-09-07T12:01:00Z"
AT = cleanup_fixture.AT


class CleanupAccountStatusTest(unittest.TestCase):
    target_schema = 20

    def setUp(self):
        if self.target_schema == 21:
            from tests.test_account_classification_release import AccountClassificationReleaseTest
            self.fixture = AccountClassificationReleaseTest()
        else:
            self.fixture = cleanup_fixture.CleanupRuntimeTest()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()
        fixture = self.fixture
        self.account_id = fixture.member["account_id"]
        connection = fixture.connection
        import_account_directory(connection, {"sha256": "a" * 64, "source": "fixture.xlsx", "sheet": "accounts", "records": [
            {"sourceRow": 2, "raw": {"平台": "抖音", "UID": "123456", "更新状态": "日更"}},
            {"sourceRow": 3, "raw": {"平台": "抖音", "UID": "999999", "更新状态": "暂停"}},
        ]}, imported_at=AT)
        self.outside_id = connection.execute("SELECT account_id FROM account_directory_rows WHERE uid='999999'").fetchone()[0]
        # The second identity is verified but outside the frozen eligible set.
        connection.execute("UPDATE account_directory_rows SET identity_status='existing_verified' WHERE account_id=?", (self.outside_id,))
        for operation in cleanup.OPERATIONS:
            capture_planning.assign_route(connection, scope_type="account", scope_key=str(self.account_id),
                account_id=self.account_id, provider="tikhub", operation=operation, expected_generation=0,
                route="integrated", mode="active", effective_at=AT, recorded_at=AT)
        self.assertNotEqual(fixture.maintenance()["status"], "blocked")
        connection.commit()
        if self.target_schema == 21:
            # Exercise the real migration and inherited capture authority, using
            # only the release suite's independent temporary receipt fixture.
            fixture.make_child()
            connection.commit()
        self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], self.target_schema)
        self.now = NOW
        owner = self
        class Clock(datetime):
            @classmethod
            def now(cls, tz=None):
                instant = datetime.fromisoformat(owner.now.replace("Z", "+00:00"))
                return instant.astimezone(tz) if tz else instant.replace(tzinfo=None)
        self.enterContext(patch.object(status_service, "datetime", Clock))
        self.enterContext(patch.object(storage, "now_utc", side_effect=lambda: self.now))
        self.enterContext(patch.object(api, "now_utc", side_effect=lambda: self.now))
        self.enterContext(patch.object(account_directory_status, "now_utc", side_effect=lambda: self.now))
        self.enterContext(patch.object(api, "runtime_account_summary", side_effect=lambda conn:
            account_roster.runtime_account_summary(conn, at=self.now)))
        self.original_roster_changes = [self.enterContext(patch.object(status_service, name,
            side_effect=AssertionError("fixed cleanup cannot rebuild roster")))
            for name in ("remove_system_member", "upsert_system_members")]
        self.enterContext(patch.object(api, "_schedule_writer_roster_activation",
            side_effect=AssertionError("fixed cleanup cannot schedule activation")))
        self.capture_validation = self.enterContext(patch.object(account_roster_capture, "validate_current_account_capture",
            wraps=account_roster_capture.validate_current_account_capture))
        self.client = TestClient(api.create_app(_test_config(fixture.root, db_name=fixture.db.name)))
        self.addCleanup(self.client.close)

    def request(self, status, request_id, account_id=None):
        self.now = (datetime.fromisoformat(self.now.replace("Z", "+00:00")) + timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
        return self.client.patch(f"/api/v8/accounts/{self.account_id if account_id is None else account_id}",
            json={"account_status": status, "status_request_id": request_id})

    def frozen_scope(self):
        connection = self.fixture.connection
        return {table: [tuple(row) for row in connection.execute(f'SELECT * FROM "{table}" ORDER BY rowid')]
            for table in ("account_roster_snapshots", "account_roster_members", "acquisition_profile_activations",
                          "capture_route_assignments", "pipeline_paid_drain_events", "capture_paid_send_gate_events",
                          "provider_readiness_receipts", "provider_usage", "provider_request_start_events", "capture_work_items")}

    def all_rows(self):
        connection = self.fixture.connection
        return {row[0]: [tuple(item) for item in connection.execute('SELECT * FROM "' + row[0] + '" ORDER BY rowid')]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}

    def test_pause_resume_frequency_and_pause_keep_authorized_scope_and_receipts(self):
        frozen = self.frozen_scope()
        for index, status in enumerate(("paused", "daily", "weekly", "paused")):
            response = self.request(status, f"status-{index}")
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["account_status"], status)
            self.assertEqual(response.json()["enabled"], status != "paused")
            self.assertNotIn("roster_change", response.json())
            self.assertEqual(self.frozen_scope(), frozen)
            evidence = cleanup.installed_evidence(self.fixture.connection, at=self.now, maintenance_only=True)
            self.assertEqual(evidence["active"]["activation_id"], self.fixture.active["activation_id"])
            events = state_events(self.fixture.connection, self.fixture.member["account_identity_id"])
            self.assertEqual(events[-1]["new_enabled"], status != "paused")
            self.assertEqual(events[-1]["activation_id"], self.fixture.active["activation_id"])
        self.assertEqual(self.capture_validation.call_count, 1)
        before = self.all_rows()
        replayed = self.request("daily", "status-1")
        self.assertEqual(replayed.status_code, 200, replayed.text)
        self.assertTrue(replayed.json()["status_replayed"])
        self.assertEqual(self.all_rows(), before)

    def test_outside_authorized_set_changes_labels_without_resuming_or_expanding_scope(self):
        frozen = self.frozen_scope()
        connection = self.fixture.connection
        original_accounts = [tuple(row) for row in connection.execute("SELECT * FROM accounts ORDER BY id")]
        for index, status in enumerate(("daily", "weekly", "paused")):
            response = self.request(status, f"outside-{index}", self.outside_id)
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["account_status"], status)
            self.assertIs(response.json()["enabled"], False)
            self.assertEqual(connection.execute("SELECT account_status FROM account_directory_rows WHERE account_id=?",
                (self.outside_id,)).fetchone()[0], status)
            receipt = account_directory_status.find_directory_status_request(connection, f"outside-{index}")
            self.assertEqual(receipt["payload"]["target"]["capture_selection_sha256"], self.fixture.generation["selection_sha256"])
            self.assertEqual(self.frozen_scope(), frozen)
            self.assertEqual([tuple(row) for row in connection.execute("SELECT * FROM accounts ORDER BY id")], original_accounts)
        before = self.all_rows()
        replayed = self.request("daily", "outside-0", self.outside_id)
        self.assertEqual(replayed.status_code, 200, replayed.text)
        self.assertTrue(replayed.json()["status_replayed"])
        self.assertEqual(self.all_rows(), before)
        self.assertEqual(self.capture_validation.call_count, 0)
        with storage.transaction(connection):
            with self.assertRaisesRegex(status_service.AccountOperatingStatusError, "尚未加入当前采集名单"):
                status_service.update_account_operating_status_in_transaction(connection, self.outside_id,
                    {"account_status": "daily", "status_request_id": "direct-admission-rejected"}, raw_root=self.fixture.root / "raw",
                    actor="tester", reason="test outside fixed paid scope", activation_id=self.fixture.active["activation_id"])
        self.assertEqual(self.all_rows(), before)

    def test_closed_gate_blocks_resume_and_rolls_back_every_write(self):
        self.assertEqual(self.request("paused", "first-pause").status_code, 200)
        gate = {"provider": "tikhub", "operation": "douyin_user_posts", "state": "closed", "reason": "operator hold",
                "evidence_json": "{}", "recorded_at": self.now}
        self.fixture.connection.execute("INSERT INTO capture_paid_send_gate_events(provider,operation,state,reason,evidence_json,recorded_at,event_sha256) VALUES(?,?,?,?,?,?,?)",
            (*gate.values(), auth.digest(gate)))
        self.fixture.connection.commit()
        before = self.all_rows()
        response = self.request("daily", "closed-gate-resume")
        self.assertEqual(response.status_code, 409, response.text)
        self.assertEqual(self.all_rows(), before)


class CleanupAccountStatusSchema21Test(CleanupAccountStatusTest):
    target_schema = 21


if __name__ == "__main__":
    unittest.main()
