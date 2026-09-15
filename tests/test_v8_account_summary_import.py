"""Offline account-import contracts: identity, provenance and capture isolation."""
from __future__ import annotations

import copy
import json
import sqlite3
import unittest
from unittest.mock import patch

from v8.account_directory import DIRECTORY_SCHEMA
from v8.account_summary_import import DISPLAY_ID, HEADERS, import_account_summary

STAMP = "2026-09-11T00:00:00Z"
LATER = "2026-09-11T01:00:00Z"
UID = "123456789"
XHS = "1234567890abcdef12345678"


def record(row=2, platform="抖音", uid=UID, display="car_test", *, verified=True, **fields):
    raw = dict.fromkeys(HEADERS)
    raw.update({"平台": platform, "uid": uid, DISPLAY_ID: display, "账号名称": "测试账号", **fields})
    metadata = {"account_record_count": 1, "phone_record_count": 0,
                "enrichment_status": "verified" if verified else "unverified",
                "verified_uid": uid if verified else None, "conflict_fields": [],
                "enrichment_profile": {"uid": uid, "ID": display} if verified else {"uid": "rejected", "name": "被拒的候选"}}
    return {"sourceRow": row, "raw": raw, "comment": "原始批注，保留来源", "metadata": metadata}


def payload(*records, sha="a" * 64):
    return {"sha256": sha, "source": "summary.xlsx", "sheet": "汇总", "records": list(records)}


class AccountSummaryImportTest(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.addCleanup(self.db.close)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript("""
            CREATE TABLE accounts(id INTEGER PRIMARY KEY AUTOINCREMENT,phone TEXT NOT NULL,
              phone_normalized TEXT,operator_name TEXT NOT NULL DEFAULT '',enabled INTEGER NOT NULL DEFAULT 1,
              created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
            CREATE TABLE account_platform_identities(id INTEGER PRIMARY KEY AUTOINCREMENT,
              account_id INTEGER NOT NULL REFERENCES accounts(id),platform TEXT NOT NULL,uid TEXT,
              nickname TEXT NOT NULL DEFAULT '',real_name_status TEXT NOT NULL DEFAULT 'unknown',
              source TEXT NOT NULL DEFAULT 'manual',created_at TEXT NOT NULL,updated_at TEXT NOT NULL,
              UNIQUE(platform,uid),UNIQUE(account_id));
            CREATE TABLE content_items(id INTEGER PRIMARY KEY,account_id INTEGER REFERENCES accounts(id),
              platform TEXT,raw_account_uid TEXT);
            INSERT INTO content_items VALUES(1,NULL,'douyin','123456789');
            CREATE TABLE preserved_history(id INTEGER PRIMARY KEY,immutable_value TEXT);
            INSERT INTO preserved_history VALUES(1,'keep');
        """)
        self.db.execute(DIRECTORY_SCHEMA)
        self.db.commit()
        self.enterContext(patch("socket.socket.connect", side_effect=AssertionError("Network forbidden")))

    def existing(self, uid=UID, display="old_car", *, enabled=1, directory=True, platform="douyin", status="daily"):
        cursor = self.db.execute("INSERT INTO accounts(phone,phone_normalized,operator_name,enabled,created_at,updated_at) VALUES('00123456789','00123456789','旧运营',?,?,?)", (enabled, STAMP, STAMP))
        aid = cursor.lastrowid
        cursor = self.db.execute("INSERT INTO account_platform_identities(account_id,platform,uid,nickname,real_name_status,created_at,updated_at) VALUES(?,?,?,'旧昵称','yes',?,?)", (aid, platform, uid, STAMP, STAMP))
        iid = cursor.lastrowid
        if directory:
            self.db.execute("INSERT INTO account_directory_rows(source_sha256,source_name,source_sheet,source_row,account_id,platform,uid,nickname,display_account_id,phone,operator_name,account_status,identity_status,raw_json,imported_at,updated_at) VALUES(?,?,?,?,?,?,?,'旧昵称',?,'00123456789','旧运营',?,'existing_verified',?,?,?)", ("b" * 64, "old.xlsx", "旧目录", aid, aid, platform, uid, display, status, json.dumps({"旧备注": "keep", "持卡人": "旧持卡人"}, ensure_ascii=False), STAMP, STAMP))
        self.db.commit()
        return aid, iid

    def run_import(self, data, at=STAMP):
        if not self.db.in_transaction:
            self.db.execute("BEGIN")
        return import_account_summary(self.db, data, imported_at=at)

    def directory(self, row_id=None):
        row = self.db.execute("SELECT * FROM account_directory_rows" + (" WHERE id=?" if row_id else " ORDER BY id DESC LIMIT 1"), (row_id,) if row_id else ()).fetchone()
        return dict(row)

    def test_existing_exact_uid_updates_explicit_fields_and_retains_history_links_and_enabled(self):
        aid, iid = self.existing(enabled=0)
        self.db.execute("INSERT INTO content_items VALUES(2,?,'douyin',?)", (aid, UID)); self.db.commit()
        r = record(运营人员="新运营", 手机号="00987654321", 更新状态="周更", 粉丝=0, 是否实名="否", 持卡人="不同持卡人")
        result = self.run_import(payload(r))
        self.assertEqual(result["counts"]["updated"], 1)
        row = result["rows"][0]
        self.assertEqual((row["account_id"], row["identity_id"]), (aid, iid))
        account = self.db.execute("SELECT * FROM accounts WHERE id=?", (aid,)).fetchone()
        self.assertEqual((account["enabled"], account["phone"], account["operator_name"]), (0, "00987654321", "新运营"))
        d = self.directory()
        self.assertEqual((d["account_status"], d["identity_status"]), ("weekly", "existing_verified"))
        data = json.loads(d["raw_json"])
        self.assertEqual(data["旧备注"], "keep")
        self.assertEqual(data["account_summary"]["fields"]["粉丝"], 0)
        self.assertEqual(data["account_summary"]["fields"]["持卡人"], "不同持卡人")
        self.assertEqual(self.db.execute("SELECT account_id FROM content_items WHERE id=1").fetchone()[0], None)
        self.assertEqual(self.db.execute("SELECT account_id FROM content_items WHERE id=2").fetchone()[0], aid)
        self.assertEqual(self.db.execute("SELECT immutable_value FROM preserved_history").fetchone()[0], "keep")
        self.assertTrue(self.db.in_transaction)

    def test_new_verified_accounts_are_disabled_separate_by_platform_not_phone(self):
        result = self.run_import(payload(record(手机号="00123456789", 更新状态="日更"), record(3, "小红书", XHS, "same_display", 手机号="00123456789", 更新状态="日更")))
        self.assertEqual(result["counts"]["added"], 2)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM accounts WHERE enabled=0").fetchone()[0], 2)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM account_directory_rows WHERE identity_status='uid_unverified'").fetchone()[0], 2)
        self.assertNotEqual(result["rows"][0]["account_id"], result["rows"][1]["account_id"])

    def test_unverified_source_uid_can_match_existing_but_cannot_create_subject(self):
        aid, _ = self.existing()
        result = self.run_import(payload(record(verified=False), record(3, uid="987654321", display="other_id", verified=False)))
        self.assertEqual(result["rows"][0]["account_id"], aid)
        self.assertIsNone(result["rows"][1]["account_id"])
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM accounts").fetchone()[0], 1)
        self.assertEqual(self.directory()["identity_status"], "identity_missing")
        self.assertEqual(self.directory()["uid"], "987654321")

    def test_shared_display_id_does_not_bridge_distinct_uids(self):
        first = record(display="shared")
        second = record(3, uid="987654321", display="shared")
        third = record(4, uid="111111111", display="shared", verified=False)
        result = self.run_import(payload(first, second, third))
        self.assertEqual(result["counts"], {"added": 2, "updated": 0, "unchanged": 0, "review": 1, "asset": 0})
        self.assertEqual([r[0] for r in self.db.execute("SELECT display_account_id FROM account_directory_rows")], ["", ""])
        self.assertIn(DISPLAY_ID, result["rows"][0]["pending_fields"])

    def test_repeated_uid_entire_group_requires_review(self):
        result = self.run_import(payload(record(), record(3, display="other")))
        self.assertEqual(result["counts"]["review"], 2)
        self.assertEqual(result["writes"], 0)

    def test_uid_and_display_id_pointing_to_different_subjects_never_merge(self):
        aid, _ = self.existing(display="first")
        bid, _ = self.existing("987654321", "second")
        result = self.run_import(payload(record(display="second")))
        self.assertEqual(result["counts"]["review"], 1)
        self.assertEqual(result["writes"], 0)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM accounts").fetchone()[0], 2)
        self.assertNotEqual(aid, bid)

    def test_unknown_uid_with_existing_display_id_conflict_is_review(self):
        self.existing(display="shared")
        result = self.run_import(payload(record(uid="987654321", display="shared", verified=False)))
        self.assertEqual(result["counts"]["review"], 1)
        self.assertIn("冲突", result["rows"][0]["reason"])

    def test_missing_and_pending_values_do_not_clear_good_fields_or_rebind_roles(self):
        self.existing()
        r = record(display=None, 账号名称="待核实：旧名；抓取新名", 运营人员="新人员", 手机号="00765432100", 更新状态="待核实", 是否实名="待核实")
        r["metadata"]["conflict_fields"] = ["账号名称"]
        result = self.run_import(payload(r))
        d = self.directory()
        self.assertEqual((d["nickname"], d["operator_name"], d["phone"], d["account_status"], d["display_account_id"]), ("旧昵称", "旧运营", "00123456789", "daily", "old_car"))
        summary = json.loads(d["raw_json"])["account_summary"]
        self.assertEqual(summary["fields"]["持卡人"], "旧持卡人")
        self.assertEqual(summary["raw"]["运营人员"], "新人员")
        self.assertIn("手机号", result["rows"][0]["pending_fields"])

    def test_unrelated_comment_issue_does_not_block_valid_account_or_operator(self):
        self.existing()
        r = record(运营人员="明确运营", 质量标签="创新号")
        r["metadata"]["issues"] = ["持卡人待核实"]
        self.run_import(payload(r))
        self.assertEqual(self.directory()["operator_name"], "明确运营")
        self.assertEqual(self.directory()["account_group"], "innovation")

    def test_asset_and_unlocatable_record_keep_complete_material_without_fake_subject(self):
        asset = record(platform=None, uid=None, display=None, verified=False, 手机号="00123456789", 使用人证件号码="001234567890123456")
        asset["metadata"].update(account_record_count=0, phone_record_count=1)
        result = self.run_import(payload(asset, record(3, uid=None, display=None, verified=False)))
        self.assertEqual((result["counts"]["asset"], result["counts"]["review"], result["writes"]), (1, 1, 0))
        self.assertEqual(result["rows"][0]["source_record"]["raw"]["使用人证件号码"], "001234567890123456")
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM accounts").fetchone()[0], 0)

    def test_kuaishou_internal_uid_is_not_promoted_to_display_id(self):
        verified = record(platform="快手", uid="123456789", display="123456789")
        verified["metadata"]["enrichment_profile"]["ID"] = None
        unverified = record(3, "快手", None, "987654321", verified=False)
        result = self.run_import(payload(verified, unverified))
        self.assertEqual((result["counts"]["added"], result["counts"]["review"]), (1, 1))
        self.assertEqual(self.directory()["display_account_id"], "")
        self.assertEqual(json.loads(self.directory()["raw_json"])["account_summary"]["raw"][DISPLAY_ID], "123456789")

    def test_rejected_enrichment_profile_is_not_a_source_of_uid_name_or_fans(self):
        r = record(platform="小红书", uid=None, display="source_display", verified=False, 账号名称="原始名称")
        r["metadata"]["enrichment_profile"] = {"uid": XHS, "name": "被拒候选", "ID": "different", "fans": 999}
        result = self.run_import(payload(r))
        self.assertIsNone(result["rows"][0]["account_id"])
        self.assertIsNone(self.directory()["uid"])
        summary = json.loads(self.directory()["raw_json"])["account_summary"]
        self.assertEqual(summary["fields"]["账号名称"], "原始名称")
        self.assertIsNone(summary["fields"]["粉丝"])

    def test_replay_is_zero_writes_including_history_and_timestamp(self):
        data = payload(record())
        first = self.run_import(data)
        stored = self.directory()
        changes = self.db.total_changes
        result = self.run_import(data, LATER)
        self.assertEqual(result["counts"]["unchanged"], 1)
        self.assertEqual(result["writes"], 0)
        self.assertEqual(self.db.total_changes, changes)
        self.assertEqual(self.directory(), stored)
        self.assertEqual(result["rows"][0]["account_id"], first["rows"][0]["account_id"])

    def test_new_source_keeps_prior_values_comments_and_older_replay_does_not_revert(self):
        old = payload(record(粉丝="约38000", 手机号="00123456789"))
        self.run_import(old)
        new = payload(record(粉丝=39000, 手机号="00987654321"), sha="c" * 64)
        self.run_import(new, LATER)
        d = self.directory()
        summary = json.loads(d["raw_json"])["account_summary"]
        self.assertEqual(summary["fields"]["粉丝"], 39000)
        self.assertEqual(summary["history"][0]["previous_raw"]["fields"]["粉丝"], "约38000")
        self.assertEqual(len(summary["imports"]), 2)
        result = self.run_import(old, "2026-09-12T00:00:00Z")
        self.assertEqual(result["writes"], 0)
        self.assertEqual(self.directory(), d)

    def test_same_sha_and_row_with_different_payload_fails_before_mutation(self):
        self.run_import(payload(record()))
        before = self.db.total_changes
        with self.assertRaisesRegex(ValueError, "different input"):
            self.run_import(payload(record(粉丝=9)))
        self.assertEqual(before, self.db.total_changes)

    def test_directory_only_can_gain_verified_identity_without_rekeying_history(self):
        result = self.run_import(payload(record(uid=None, display="display_only", verified=False)))
        did = result["rows"][0]["directory_row_id"]
        result = self.run_import(payload(record(display="display_only"), sha="d" * 64), LATER)
        self.assertEqual(result["counts"]["updated"], 1)
        self.assertEqual(result["rows"][0]["directory_row_id"], did)
        self.assertEqual(self.directory()["identity_status"], "uid_unverified")
        self.assertEqual(self.db.execute("SELECT enabled FROM accounts").fetchone()[0], 0)
        self.assertIsNone(self.db.execute("SELECT account_id FROM content_items WHERE id=1").fetchone()[0])

    def test_unverified_uid_preserves_matching_key_across_new_source_and_changed_display(self):
        result = self.run_import(payload(record(verified=False, display="display_only")))
        did = result["rows"][0]["directory_row_id"]
        self.assertEqual(self.directory()["uid"], UID)
        result = self.run_import(payload(record(display=None), sha="e" * 64), LATER)
        self.assertEqual(result["counts"]["updated"], 1)
        self.assertEqual(result["rows"][0]["directory_row_id"], did)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM account_directory_rows").fetchone()[0], 1)

    def test_existing_subject_without_directory_is_retained_and_gets_one_directory(self):
        aid, iid = self.existing(directory=False)
        result = self.run_import(payload(record()))
        self.assertEqual((result["rows"][0]["account_id"], result["rows"][0]["identity_id"]), (aid, iid))
        self.assertEqual(self.db.execute("SELECT enabled FROM accounts WHERE id=?", (aid,)).fetchone()[0], 1)
        self.assertEqual(self.directory()["identity_status"], "uid_unverified")

    def test_two_different_source_locators_targeting_same_subject_are_reviewed_together(self):
        self.existing(display="existing_display")
        result = self.run_import(payload(record(display=None), record(3, uid=None, display="existing_display", verified=False)))
        self.assertEqual(result["counts"]["review"], 2)
        self.assertEqual(result["writes"], 0)

    def test_savepoint_rolls_back_entire_import_if_one_write_fails(self):
        self.db.execute("CREATE TRIGGER reject_second BEFORE INSERT ON account_directory_rows WHEN NEW.source_row=3 BEGIN SELECT RAISE(ABORT,'fixture rejection'); END")
        self.db.commit(); self.db.execute("BEGIN")
        with self.assertRaisesRegex(sqlite3.IntegrityError, "fixture rejection"):
            import_account_summary(self.db, payload(record(), record(3, uid="987654321", display="other")), imported_at=STAMP)
        self.assertTrue(self.db.in_transaction)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM accounts").fetchone()[0], 0)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM account_directory_rows").fetchone()[0], 0)

    def test_blank_source_does_not_replace_subject_values_from_stale_directory(self):
        aid, iid = self.existing()
        self.db.execute("UPDATE account_directory_rows SET phone='00999999999',operator_name='旧目录运营',nickname='旧目录名' WHERE account_id=?", (aid,))
        self.db.commit()
        self.run_import(payload(record(账号名称=None, 运营人员=None, 手机号=None, 是否实名=None)))
        self.assertEqual(tuple(self.db.execute("SELECT phone,operator_name FROM accounts WHERE id=?", (aid,)).fetchone()), ("00123456789", "旧运营"))
        self.assertEqual(tuple(self.db.execute("SELECT nickname,real_name_status FROM account_platform_identities WHERE id=?", (iid,)).fetchone()), ("旧昵称", "yes"))

    def test_empty_directory_falls_back_to_existing_subject_for_visible_fields(self):
        aid, _ = self.existing()
        self.db.execute("UPDATE account_directory_rows SET phone='',operator_name='',nickname='' WHERE account_id=?", (aid,))
        self.db.commit()
        self.run_import(payload(record(账号名称=None, 运营人员=None, 手机号=None)))
        summary = json.loads(self.directory()["raw_json"])["account_summary"]
        self.assertEqual(summary["fields"]["手机号"], "00123456789")
        self.assertEqual(summary["fields"]["运营人员"], "旧运营")
        self.assertEqual(summary["fields"]["账号名称"], "旧昵称")

    def test_malformed_existing_uid_is_not_treated_as_unbound(self):
        self.existing(uid="malformed_old_uid", display="shared")
        result = self.run_import(payload(record(uid="987654321", display="shared")))
        self.assertEqual(result["counts"]["review"], 1)
        self.assertEqual(result["writes"], 0)
        self.assertIn("现存UID格式异常", result["rows"][0]["reason"])

    def test_invalid_input_and_missing_transaction_fail_without_schema_mutation(self):
        schema = list(self.db.execute("SELECT sql FROM sqlite_master ORDER BY name"))
        with self.assertRaisesRegex(ValueError, "caller transaction"):
            import_account_summary(self.db, payload(record()), imported_at=STAMP)
        wrong = record(); wrong["raw"].pop("uid")
        with self.assertRaisesRegex(ValueError, "sixteen"):
            self.run_import(payload(wrong))
        self.assertEqual(list(self.db.execute("SELECT sql FROM sqlite_master ORDER BY name")), schema)
        wrong = record(); wrong["raw"]["uid"] = 1.234e15
        with self.assertRaisesRegex(ValueError, "exact text"):
            self.run_import(payload(wrong))


if __name__ == "__main__":
    unittest.main()
