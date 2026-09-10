from __future__ import annotations

import json
import socket
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from tests.test_v8_api import _seed_read_model_database, _test_config
from v8 import api
from v8.account_directory import import_account_directory
from v8.account_directory_status import (
    DIRECTORY_STATUS_JOB, find_directory_status_request,
    update_directory_only_status_in_transaction,
)
from v8.account_operating_receipts import AccountOperatingStatusError, load_update_frequencies
from v8.storage import connect, transaction
from v8.system_roster import seal_system_members


class AccountDirectoryStatusTest(unittest.TestCase):
    target_schema = 19

    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.config = _test_config(self.root)
        _seed_read_model_database(self.config.db_path)
        if self.target_schema == 21:
            from v8.schema_v20 import migrate
            with connect(self.config.db_path) as connection:
                migrate(connection)
        with connect(self.config.db_path) as connection, transaction(connection):
            self.verified_id = connection.execute("SELECT id FROM accounts").fetchone()[0]
            connection.execute("UPDATE account_platform_identities SET uid='123456789'")
            imported = import_account_directory(connection, {
                "sha256": "a" * 64, "source": "fixture.xlsx", "sheet": "accounts", "records": [
                    {"sourceRow": 2, "raw": {"平台": "抖音", "UID": "123456789", "更新状态": "日更"}},
                    {"sourceRow": 3, "raw": {"平台": "抖音", "UID": "987654321", "更新状态": "日更"}},
                    {"sourceRow": 4, "raw": {"平台": "视频号", "UID": "无", "更新状态": "暂停"}},
                    {"sourceRow": 5, "raw": {"平台": "抖音", "UID": "无", "更新状态": "周更"}},
                ],
            }, imported_at="2026-09-08T00:00:00Z")
            self.unverified_id = imported["rows"][1]["account_id"]
            self.missing_id = -imported["rows"][2]["directory_row_id"]
            self.other_missing_id = -imported["rows"][3]["directory_row_id"]
        if self.target_schema == 21:
            from v8.schema_v21 import migrate
            with connect(self.config.db_path) as connection:
                migrate(connection)
        self.client = TestClient(api.create_app(self.config))
        self.addCleanup(self.client.close)
        self.enterContext(patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden")))

    def request(self, target, status, request_id, **fields):
        return self.client.patch(f"/api/v8/accounts/{target}", json={
            "account_status": status, "status_request_id": request_id, **fields,
        })

    def directory_status(self, target):
        with connect(self.config.db_path) as connection:
            return connection.execute("SELECT account_status FROM account_directory_rows WHERE " +
                                      ("id=?" if target < 0 else "account_id=?"),
                                      (-target if target < 0 else target,)).fetchone()[0]

    def state(self):
        with connect(self.config.db_path) as connection:
            return {table: [tuple(row) for row in connection.execute(f'SELECT * FROM "{table}" ORDER BY rowid')]
                    for table in ("accounts", "account_platform_identities", "content_items", "account_roster_members",
                                  "acquisition_profile_activations", "account_directory_rows", "scheduler_runs", "scheduler_run_attempts")}

    def test_all_directory_status_transitions_do_not_admit_enable_or_capture(self):
        before = self.state()
        for target in (self.missing_id, self.unverified_id):
            for status in ("daily", "weekly", "paused"):
                with self.subTest(target=target, status=status):
                    response = self.request(target, status, f"{target}:{status}")
                    self.assertEqual(response.status_code, 200, response.text)
                    self.assertEqual(response.json()["account_status"], status)
                    self.assertIs(response.json()["enabled"], False)
                    self.assertEqual(self.directory_status(target), status)
        after = self.state()
        for table in ("accounts", "account_platform_identities", "content_items", "account_roster_members", "acquisition_profile_activations"):
            self.assertEqual(after[table], before[table], table)
        with connect(self.config.db_path) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], self.target_schema)
            self.assertEqual(len(load_update_frequencies(connection)), 0)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM scheduler_runs WHERE job_id=?", (DIRECTORY_STATUS_JOB,)).fetchone()[0], 6)
            self.assertIsNone(connection.execute("SELECT account_id FROM account_directory_rows WHERE id=?", (-self.missing_id,)).fetchone()[0])
            self.assertEqual(json.loads(connection.execute("SELECT raw_json FROM account_directory_rows WHERE id=?", (-self.missing_id,)).fetchone()[0])["更新状态"], "暂停")

    def test_replay_does_not_overwrite_later_status_or_add_receipt(self):
        self.assertEqual(self.request(self.missing_id, "daily", "first").status_code, 200)
        self.assertEqual(self.request(self.missing_id, "weekly", "second").status_code, 200)
        before = self.state()
        response = self.request(self.missing_id, "daily", "first")
        self.assertEqual(response.status_code, 200, response.text)
        self.assertTrue(response.json()["status_replayed"])
        self.assertEqual(self.directory_status(self.missing_id), "weekly")
        self.assertEqual(self.state(), before)

    def test_request_id_cannot_change_status_target_or_enter_verified_flow(self):
        self.assertEqual(self.request(self.missing_id, "daily", "same").status_code, 200)
        before = self.state()
        for target, status in ((self.missing_id, "weekly"), (self.other_missing_id, "daily"), (self.verified_id, "daily")):
            response = self.request(target, status, "same")
            self.assertEqual(response.status_code, 409, response.text)
            self.assertEqual(self.state(), before)

    def test_missing_changed_or_unexpectedly_enabled_identity_is_rejected(self):
        self.assertEqual(self.request(-99999, "daily", "missing").status_code, 409)
        with connect(self.config.db_path) as connection, transaction(connection):
            connection.execute("DELETE FROM account_directory_rows WHERE account_id=?", (self.unverified_id,))
            connection.execute("UPDATE account_directory_rows SET account_id=?,identity_status='uid_unverified' WHERE id=?",
                               (self.unverified_id, -self.missing_id))
        before = self.state()
        self.assertEqual(self.request(self.missing_id, "daily", "stale").status_code, 409)
        self.assertEqual(self.state(), before)
        with connect(self.config.db_path) as connection, transaction(connection):
            connection.execute("UPDATE accounts SET enabled=1 WHERE id=?", (self.unverified_id,))
        before = self.state()
        self.assertEqual(self.request(self.unverified_id, "daily", "invalid-identity").status_code, 409)
        self.assertEqual(self.state(), before)

    def test_status_only_validation_and_read_only_guard(self):
        before = self.state()
        for fields in ({"enabled": True}, {"operator_name": "must not change"}, {"phone": "13312345678"}):
            response = self.request(self.missing_id, "daily", "invalid-fields", **fields)
            self.assertEqual(response.status_code, 422 if "enabled" in fields else 409, response.text)
        self.assertEqual(self.request(self.missing_id, "monthly", "invalid-status").status_code, 422)
        self.assertEqual(self.request(self.missing_id, "daily", " spaces ").status_code, 409)
        readonly = TestClient(api.create_app(replace(self.config, read_only=True)))
        self.addCleanup(readonly.close)
        self.assertEqual(readonly.patch(f"/api/v8/accounts/{self.missing_id}", json={
            "account_status": "daily", "status_request_id": "readonly",
        }).status_code, 403)
        self.assertEqual(self.state(), before)

    def test_receipt_failure_rolls_back_even_when_caller_catches_inside_transaction(self):
        before = self.state()
        with connect(self.config.db_path) as connection, transaction(connection):
            from v8 import account_directory_status as service
            original = service._record_receipt
            def fail_after_seal(*args, **kwargs):
                original(*args, **kwargs)
                raise RuntimeError("after seal")
            with patch.object(service, "_record_receipt", side_effect=fail_after_seal):
                with self.assertRaisesRegex(RuntimeError, "after seal"):
                    update_directory_only_status_in_transaction(connection, self.missing_id,
                        {"account_status": "daily", "status_request_id": "rollback"}, actor="tester", reason="test")
        self.assertEqual(self.state(), before)

    def test_mutable_parent_tampering_cannot_be_replayed(self):
        self.assertEqual(self.request(self.missing_id, "daily", "tampered").status_code, 200)
        with connect(self.config.db_path) as connection, transaction(connection):
            connection.execute("UPDATE scheduler_runs SET details_json='{}' WHERE job_id=?", (DIRECTORY_STATUS_JOB,))
        before = self.state()
        self.assertEqual(self.request(self.missing_id, "daily", "tampered").status_code, 409)
        self.assertEqual(self.state(), before)

    def test_verified_account_keeps_existing_status_service(self):
        with patch.object(api, "runtime_account_summary", return_value={"active_profile_id": "tikhub_managed_v1"}), \
                patch.object(api, "update_account_operating_status_in_transaction", return_value={
                    "id": self.verified_id, "account_status": "paused", "enabled": False,
                }) as original:
            response = self.request(self.verified_id, "paused", "verified")
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(original.call_args.args[1], self.verified_id)
        with connect(self.config.db_path) as connection:
            self.assertIsNone(find_directory_status_request(connection, "verified"))

    def test_service_requires_transaction_and_rejects_original_status_receipt_namespace(self):
        with connect(self.config.db_path) as connection:
            with self.assertRaises(AccountOperatingStatusError) as caught:
                update_directory_only_status_in_transaction(connection, self.missing_id,
                    {"account_status": "daily", "status_request_id": "no-transaction"}, actor="tester", reason="test")
            self.assertEqual(caught.exception.code, "account_status_transaction_required")
        with patch.object(api, "runtime_account_summary", return_value={"active_profile_id": "tikhub_managed_v1"}):
            response = self.request(self.verified_id, "paused", "normal-receipt")
        self.assertEqual(response.status_code, 200, response.text)
        before = self.state()
        self.assertEqual(self.request(self.missing_id, "paused", "normal-receipt").status_code, 409)
        self.assertEqual(self.state(), before)

    def test_verified_receipt_replay_does_not_rewind_directory_status(self):
        with connect(self.config.db_path) as connection:
            roster = seal_system_members(connection, [{"platform": "douyin", "uid": "123456789"}],
                raw_root=self.root / "rosters", actor="tester", reason="fixture")
        runtime = {"active_profile_id": "tikhub_managed_v1", "ready": True,
                   "source_family": "system", "snapshot_id": roster["snapshot_id"]}
        with patch.object(api, "runtime_account_summary", return_value=runtime):
            first = self.request(self.verified_id, "daily", "verified-first")
            self.assertEqual(first.status_code, 200, first.text)
            second = self.request(self.verified_id, "weekly", "verified-second")
            self.assertEqual(second.status_code, 200, second.text)
            before = self.state()
            replayed = self.request(self.verified_id, "daily", "verified-first")
        self.assertEqual(replayed.status_code, 200, replayed.text)
        self.assertTrue(replayed.json()["status_replayed"])
        self.assertEqual(self.directory_status(self.verified_id), "weekly")
        self.assertEqual(self.state(), before)


class AccountDirectoryStatusSchema21Test(AccountDirectoryStatusTest):
    target_schema = 21


if __name__ == "__main__":
    unittest.main()
