from __future__ import annotations

import json
import sqlite3
import unittest
from unittest.mock import patch

from v8.account_directory import (
    directory_account_items, ensure_account_directory_schema, import_account_directory,
    update_directory_operating_fields, admit_directory_account,
)
from v8.schema_v18 import ACCOUNT_SQL, IDENTITY_SQL
from v8.statistics_scope import content_statistics_scope_sql


STAMP = "2026-09-07T16:00:00Z"


def payload():
    return {"sha256": "a" * 64, "source": "reviewed.xlsx", "sheet": "accounts", "records": [
        {"sourceRow": 2, "raw": {"平台": "抖音", "UID": "123456789", "手机号": "13300000001", "姓名": "甲",
                                 "昵称": "更新后的名字", "更新状态": "暂停", "质量标签": "创新号"}},
        {"sourceRow": 3, "raw": {"平台": "抖音", "UID": "987654321", "手机号": "13300000001", "姓名": "乙",
                                 "昵称": "新账号", "更新状态": "日更"}},
        {"sourceRow": 4, "raw": {"平台": "视频号", "UID": "无", "手机号": "13300000001", "姓名": "丙",
                                 "昵称": "待完善", "更新状态": ""}},
    ]}


class AccountDirectoryTest(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.addCleanup(self.db.close)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute(ACCOUNT_SQL)
        self.db.execute(IDENTITY_SQL)
        self.db.execute("CREATE TABLE content_items(id INTEGER PRIMARY KEY,account_id INTEGER,platform TEXT)")
        self.db.execute("INSERT INTO accounts(id,phone,enabled,created_at,updated_at) VALUES(7,'13300000001',1,?,?)", (STAMP, STAMP))
        self.db.execute("INSERT INTO accounts(id,phone,enabled,created_at,updated_at) VALUES(8,'13300000001',1,?,?)", (STAMP, STAMP))
        self.db.execute("INSERT INTO account_platform_identities(id,account_id,platform,uid,nickname,created_at,updated_at) VALUES(17,7,'douyin','123456789','旧名',?,?)", (STAMP, STAMP))
        self.db.execute("INSERT INTO account_platform_identities(id,account_id,platform,uid,nickname,created_at,updated_at) VALUES(18,8,'douyin','111111111','旧名单',?,?)", (STAMP, STAMP))
        self.db.executemany("INSERT INTO content_items VALUES(?,?,?)", [(1, 7, "douyin"), (2, 8, "douyin"), (3, None, "douyin")])
        self.db.commit()

    def install(self):
        self.db.execute("BEGIN")
        return import_account_directory(self.db, payload(), imported_at=STAMP)

    def test_exact_uid_reuses_identity_and_phone_never_merges_new_or_missing_uid(self):
        result = self.install()
        self.assertEqual((result["row_count"], result["matched_count"], result["created_count"], result["unresolved_count"]), (3, 1, 1, 1))
        self.assertEqual((result["rows"][0]["account_id"], result["rows"][0]["identity_id"]), (7, 17))
        self.assertNotIn(result["rows"][1]["account_id"], (7, 8))
        self.assertIsNone(result["rows"][2]["account_id"])
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM account_platform_identities").fetchone()[0], 3)
        self.assertEqual(self.db.execute("SELECT enabled FROM accounts WHERE id=7").fetchone()[0], 0)
        self.assertEqual(self.db.execute("SELECT enabled FROM accounts WHERE id=?", (result["rows"][1]["account_id"],)).fetchone()[0], 0)
        self.assertTrue(self.db.in_transaction)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM content_items").fetchone()[0], 3)
        self.assertEqual(import_account_directory(self.db, payload(), imported_at=STAMP)["status"], "unchanged")

    def test_directory_statistics_include_paused_history_but_not_old_or_unassociated(self):
        self.install()
        predicate = content_statistics_scope_sql(connection=self.db)
        self.assertEqual([row[0] for row in self.db.execute(f"SELECT c.id FROM content_items c WHERE {predicate}")], [1])

    def test_empty_directory_preserves_preexisting_scope_until_import(self):
        ensure_account_directory_schema(self.db)
        self.db.execute("UPDATE accounts SET enabled=0 WHERE id=7")
        predicate = content_statistics_scope_sql(connection=self.db)
        self.assertEqual([row[0] for row in self.db.execute(f"SELECT c.id FROM content_items c WHERE {predicate}")], [2, 3])

    def test_directory_list_contains_pending_rows_and_filters_source_status(self):
        self.install()
        def read_model(connection, account, **kwargs):
            identity = dict(connection.execute("SELECT * FROM account_platform_identities WHERE account_id=?", (account["id"],)).fetchone())
            return {**dict(account), "platforms": [identity]}
        with patch("v8.operations.account_read_model", side_effect=read_model):
            values = directory_account_items(self.db, roster={}, update_frequencies={}, admission_members={})
            self.assertEqual(len(values), 3)
            self.assertEqual([row["account_status"] for row in values], ["paused", "daily", "unmarked"])
            self.assertEqual(values[0]["platforms"][0]["nickname"], "更新后的名字")
            self.assertLess(values[2]["id"], 0)
            self.assertIsNone(values[2]["platforms"][0]["uid"])
            self.assertEqual(values[2]["directory_identity_status"], "identity_missing")
            self.assertFalse(values[1]["enabled"])
            filtered = directory_account_items(self.db, roster={}, update_frequencies={}, admission_members={}, account_status="daily")
            self.assertEqual(len(filtered), 1)
            self.assertEqual(filtered[0]["platforms"][0]["uid"], "987654321")
        update_directory_operating_fields(self.db, 7, {"account_status": "weekly", "operator_name": "修改"})
        updated = self.db.execute("SELECT account_status,operator_name,raw_json FROM account_directory_rows WHERE account_id=7").fetchone()
        self.assertEqual(tuple(updated[:2]), ("weekly", "修改"))
        self.assertEqual(json.loads(updated[2])["更新状态"], "暂停")

    def test_duplicate_uid_import_rolls_back_under_caller_transaction(self):
        duplicate = payload()
        duplicate["records"][1]["raw"]["UID"] = "123456789"
        self.db.execute("BEGIN")
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            import_account_directory(self.db, duplicate, imported_at=STAMP)
        self.db.rollback()
        self.assertEqual(self.db.execute("SELECT enabled FROM accounts WHERE id=7").fetchone()[0], 1)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM account_platform_identities").fetchone()[0], 2)

    def test_explicit_profile_admission_upgrades_known_row_or_appends_exact_identity(self):
        result = self.install()
        account_id = result["rows"][1]["account_id"]
        admit_directory_account(self.db, account_id=account_id,
            member={"platform": "douyin", "uid": "987654321", "nickname": "已核验"},
            account_status="daily", request_id="request-1", at=STAMP)
        self.assertEqual(self.db.execute("SELECT identity_status FROM account_directory_rows WHERE account_id=?", (account_id,)).fetchone()[0], "existing_verified")
        admit_directory_account(self.db, account_id=8,
            member={"platform": "douyin", "uid": "111111111", "nickname": "明确新增"},
            account_status="paused", request_id="request-2", at=STAMP)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM account_directory_rows").fetchone()[0], 4)
        admit_directory_account(self.db, account_id=8,
            member={"platform": "douyin", "uid": "111111111", "nickname": "明确新增"},
            account_status="paused", request_id="request-2", at=STAMP)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM account_directory_rows").fetchone()[0], 4)
        with self.assertRaisesRegex(ValueError, "identity"):
            admit_directory_account(self.db, account_id=8, member={"platform": "douyin", "uid": "999999999"},
                                    account_status="daily", request_id="invalid", at=STAMP)


if __name__ == "__main__":
    unittest.main()
