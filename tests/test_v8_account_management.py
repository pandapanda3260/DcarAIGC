from __future__ import annotations

import io
import socket
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from tests.test_v8_api import _seed_read_model_database, _test_config, _xlsx_sheet_values
from v8 import api, operations
from v8.account_creation import create_managed_account_in_transaction
from v8.account_operating_status import update_account_operating_status_in_transaction
from v8.account_roster import RosterError, require_active_member
from v8.operations import upsert_account
from v8.storage import connect, transaction
from v8.system_roster import seal_system_members


class AccountManagementTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(prefix="dcar-management-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.config = _test_config(self.root)
        _seed_read_model_database(self.config.db_path)
        with connect(self.config.db_path) as connection:
            self.active_id = connection.execute("SELECT id FROM accounts").fetchone()[0]
            self.active_identity = connection.execute("SELECT id FROM account_platform_identities").fetchone()[0]
            connection.execute("UPDATE account_platform_identities SET uid='123456789'")
            connection.commit()
            accepted = seal_system_members(connection, [{"platform": "douyin", "uid": "123456789"}],
                                           raw_root=self.root / "raw", actor="test", reason="initial active roster")
            self.snapshot = accepted["snapshot_id"]
            self.snapshot_hash = connection.execute("SELECT members_sha256 FROM account_roster_snapshots WHERE id=?",
                                                    (self.snapshot,)).fetchone()[0]
            with transaction(connection):
                update_account_operating_status_in_transaction(connection, self.active_id, {"account_status": "daily"},
                    raw_root=self.root / "raw", actor="test", reason="manual label")
                self.weekly = create_managed_account_in_transaction(connection,
                    {"platform": "douyin", "uid": "987654321", "nickname": "待激活账号",
                     "profile_ref": "https://www.douyin.com/user/MS4w.weekly",
                     "metadata": {"sec_user_id": "MS4w.weekly", "display_account_id": "weekly-short"}},
                    account_status="weekly", raw_root=self.root / "raw", actor="test", reason="pending fixture",
                    active_snapshot_id=self.snapshot, schedule_activation=self.schedule)
                self.paused = create_managed_account_in_transaction(connection,
                    {"platform": "douyin", "uid": "987654322", "nickname": "暂停新账号",
                     "profile_ref": "https://www.douyin.com/user/MS4w.paused",
                     "metadata": {"sec_user_id": "MS4w.paused", "display_account_id": "paused-short"}},
                    account_status="paused", phone="13212343053", raw_root=self.root / "raw", actor="test",
                    reason="paused fixture", active_snapshot_id=self.snapshot, schedule_activation=self.schedule)
        self.unmarked = upsert_account({"platforms": [{"platform": "douyin", "uid": "987654323", "nickname": "历史未标记"}]},
                                       db_path=self.config.db_path)["id"]
        self.runtime = {"active_profile_id": "tikhub_managed_v1", "ready": True,
                        "source_family": "system", "snapshot_id": self.snapshot}
        runtime_patch = patch.object(api, "runtime_account_summary", return_value=self.runtime)
        runtime_patch.start()
        self.addCleanup(runtime_patch.stop)
        network_patch = patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden"))
        network_patch.start()
        self.addCleanup(network_patch.stop)
        self.client = TestClient(api.create_app(self.config))
        self.addCleanup(self.client.close)

    @staticmethod
    def schedule(connection, result):
        return {**result, "activation_status": "scheduled", "scheduled_effective_at": "2026-09-07T16:00:00Z"}

    def search(self, **values):
        response = self.client.post("/api/v8/accounts/search", json=values)
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def export(self, **values):
        response = self.client.post("/api/v8/accounts/export", json=values)
        self.assertEqual(response.status_code, 200, response.text)
        with zipfile.ZipFile(io.BytesIO(response.content)) as workbook:
            return _xlsx_sheet_values(workbook.read("xl/worksheets/sheet1.xml"))

    def test_default_management_lists_all_saved_accounts_without_membership_projection(self):
        result = self.search()
        self.assertEqual(result["total"], 4)
        self.assertEqual(result["account_management_version"], 2)
        self.assertNotIn("archive_total", result)
        self.assertEqual(result["roster"]["active_profile_id"], "tikhub_managed_v1")
        self.assertEqual({item["account_status"] for item in result["items"]}, {"daily", "weekly", "paused", "unmarked"})
        self.assertTrue(all("roster_state" not in item for item in result["items"]))
        for obsolete_scope in ("current", "history", "unresolved", "all", "unknown"):
            self.assertEqual(self.search(scope=obsolete_scope)["total"], 4)
        self.assertNotIn("scope", api.AccountSearchRequest.model_json_schema()["properties"])
        self.assertNotIn("scope", api.AccountExportRequest.model_json_schema()["properties"])
        self.assertFalse(hasattr(operations, "account_scope_sql"))

    def test_status_filters_match_list_and_export_and_do_not_hide_paused_accounts(self):
        labels = {"daily": "日更", "weekly": "周更", "paused": "暂停", "unmarked": "待标记"}
        for status, label in labels.items():
            with self.subTest(status=status):
                result = self.search(account_status=status)
                self.assertEqual(result["total"], 1)
                self.assertEqual(result["items"][0]["account_status"], status)
                exported = self.export(account_status=status)
                self.assertEqual(len(exported), 2)
                header, row = exported
                self.assertEqual(row[header.index("账号状态")], label)
                self.assertEqual(row[header.index("平台 UID")], result["items"][0]["platforms"][0]["uid"])
        self.assertEqual(len(self.export()), 5)
        invalid = self.client.post("/api/v8/accounts/export", json={"account_status": "current"})
        self.assertEqual(invalid.status_code, 422)

    def test_paused_and_pending_admission_metadata_is_visible_with_one_batch_receipt_read(self):
        with patch.object(api, "load_admission_members", wraps=api.load_admission_members) as batch, \
                patch.object(operations, "load_admission_members", side_effect=AssertionError("per-row receipt read")):
            items = {item["id"]: item for item in self.search()["items"]}
        batch.assert_called_once()
        for result, nickname, short_id in ((self.weekly, "待激活账号", "weekly-short"), (self.paused, "暂停新账号", "paused-short")):
            identity = items[result["account_id"]]["platforms"][0]
            self.assertEqual(identity["nickname"], nickname)
            self.assertEqual(identity["unique_id"], short_id)
            self.assertTrue(identity["profile_ref"].startswith("https://www.douyin.com/user/"))
            found = self.search(query=short_id)
            self.assertEqual(found["total"], 1)
            self.assertEqual(found["items"][0]["id"], result["account_id"])
        exported = self.export(account_status="paused")
        header, row = exported
        self.assertEqual(row[header.index("短号")], "paused-short")
        self.assertEqual(row[header.index("手机号")], "13212343053")

    def test_export_keeps_exact_authorization_targets_and_rejects_old_scope_with_refresh_message(self):
        rows = self.export(account_status="paused", douyin_authorization_targets=[{
            "account_id": self.paused["account_id"], "platform_uid": "987654322", "state": "authorized",
        }])
        header, row = rows
        self.assertEqual(row[header.index("账号状态")], "暂停")
        self.assertEqual(row[header.index("抖音开平授权")], "已授权")
        self.assertNotIn("名单状态", header)
        self.assertNotIn("当前成员", str(rows))
        for scope in ("current", "all", "history"):
            response = self.client.post("/api/v8/accounts/export", json={"scope": scope})
            self.assertEqual(response.status_code, 422)
            self.assertIn("刷新账号页", response.text)

    def test_management_visibility_does_not_grant_collection_membership_or_restore_paused_statistics(self):
        with connect(self.config.db_path) as connection:
            paused_identity = connection.execute("SELECT id FROM account_platform_identities WHERE account_id=?",
                                                 (self.paused["account_id"],)).fetchone()[0]
            allowed = require_active_member(connection, self.active_identity, snapshot_id=self.snapshot,
                                            snapshot_hash=self.snapshot_hash)
            self.assertEqual(allowed["account_id"], self.active_id)
            with self.assertRaises(RosterError) as denied:
                require_active_member(connection, paused_identity, snapshot_id=self.snapshot,
                                      snapshot_hash=self.snapshot_hash)
            self.assertEqual(denied.exception.code, "member_scope_changed")
            snapshots = [tuple(row) for row in connection.execute("SELECT * FROM account_roster_snapshots")]
        self.assertEqual(self.search()["total"], 4)
        self.export()
        with connect(self.config.db_path) as connection:
            self.assertEqual([tuple(row) for row in connection.execute("SELECT * FROM account_roster_snapshots")], snapshots)
        paused = self.client.patch(f"/api/v8/accounts/{self.active_id}", json={"account_status": "paused"})
        self.assertEqual(paused.status_code, 200, paused.text)
        self.assertEqual(self.search()["total"], 4)
        with connect(self.config.db_path) as connection:
            with self.assertRaises(RosterError) as denied:
                require_active_member(connection, self.active_identity, snapshot_id=self.snapshot,
                                      snapshot_hash=self.snapshot_hash)
            self.assertEqual(denied.exception.code, "member_scope_changed")
        contents = self.client.post("/api/v8/contents/search", json={})
        self.assertEqual(contents.status_code, 200)
        self.assertEqual(contents.json()["total"], 0)


if __name__ == "__main__":
    unittest.main()
