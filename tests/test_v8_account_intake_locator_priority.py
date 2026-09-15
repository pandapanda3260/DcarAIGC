"""Real schema22 intake: UID precedence and unsupported-ID retention."""
from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tests.test_v8_account_summary_import import record, payload, STAMP, LATER, DISPLAY_ID
from v8.account_intake import import_account_summary, submit_account_intake, preparation_inputs
from v8.storage import connect, initialize_database


class AccountIntakeLocatorPriorityTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.db = connect(Path(self.temp.name) / "test.sqlite3"); self.addCleanup(self.db.close)
        initialize_database(self.db, target_version=22)
        self.enterContext(patch("socket.socket.connect", side_effect=AssertionError("Network forbidden")))
        self.db.execute("BEGIN")

    def existing(self, *, uid="123456789", platform="douyin", display="shared_handle"):
        aid = self.db.execute("INSERT INTO accounts(phone,operator_name,enabled,created_at,updated_at) VALUES('00123456789','old operator',0,?,?)", (STAMP, STAMP)).lastrowid
        iid = self.db.execute("INSERT INTO account_platform_identities(account_id,platform,uid,nickname,created_at,updated_at) VALUES(?,?,?,'old nickname',?,?)", (aid, platform, uid, STAMP, STAMP)).lastrowid
        did = self.db.execute("""INSERT INTO account_directory_rows
            (source_sha256,source_name,source_sheet,source_row,account_id,platform,uid,nickname,display_account_id,
             phone,operator_name,account_status,identity_status,raw_json,imported_at,updated_at)
            VALUES(?,?,?,1,?,?,?,'old nickname',?,'00123456789','old operator','paused','existing_verified',?,?,?)""",
            ("b" * 64, "old.xlsx", "old", aid, platform, uid, display, json.dumps({"original": "keep"}), STAMP, STAMP)).lastrowid
        return aid, iid, did

    def request(self, key):
        return dict(self.db.execute("SELECT * FROM account_intake_requests WHERE request_key=?", (key,)).fetchone())

    def test_existing_unique_uid_is_not_poisoned_by_another_uid_with_same_display(self):
        aid, iid, did = self.existing()
        result = import_account_summary(self.db, payload(record(87, uid="123456789", display="shared_handle", verified=True, 运营人员="new operator"),
            record(88, uid="987654321", display="shared_handle", verified=False)), imported_at=LATER)
        first, second = result["rows"]
        self.assertEqual((first["status"], first["account_id"], first["identity_id"], first["directory_row_id"]), ("updated", aid, iid, did))
        self.assertEqual(second["status"], "added"); self.assertIsNone(second["account_id"])
        self.assertNotEqual(second["directory_row_id"], did)
        self.assertEqual(self.db.execute("SELECT display_account_id FROM account_directory_rows WHERE id=?", (did,)).fetchone()[0], "shared_handle")
        new = self.db.execute("SELECT * FROM account_directory_rows WHERE id=?", (second["directory_row_id"],)).fetchone()
        self.assertEqual((new["uid"], new["display_account_id"]), ("987654321", ""))
        for row in result["rows"]:
            self.assertIn(DISPLAY_ID, row["pending_fields"])
        requests = [dict(row) for row in self.db.execute("SELECT * FROM account_intake_requests ORDER BY id")]
        self.assertEqual({json.loads(row["input_json"])["display_account_id"] for row in requests}, {""})
        self.assertEqual({json.loads(row["source_json"])["record"]["raw"][DISPLAY_ID] for row in requests}, {"shared_handle"})
        before = self.db.total_changes
        import_account_summary(self.db, payload(record(87, uid="123456789", display="shared_handle", verified=True, 运营人员="new operator"),
            record(88, uid="987654321", display="shared_handle", verified=False)), imported_at=LATER)
        self.assertEqual(self.db.total_changes, before)

    def test_distinct_xhs_uids_survive_duplicate_display_and_uidless_row_stays_unbound(self):
        result = import_account_summary(self.db, payload(
            record(570, platform="小红书", uid="a" * 24, display="same_one", verified=True),
            record(571, platform="小红书", uid="", display="same_one", verified=False),
            record(616, platform="小红书", uid="b" * 24, display="same_two", verified=True),
            record(617, platform="小红书", uid="c" * 24, display="same_two", verified=False)), imported_at=LATER)
        self.assertEqual([row["status"] for row in result["rows"]], ["added", "review", "added", "added"])
        self.assertEqual(self.db.execute("SELECT count(*) FROM accounts").fetchone()[0], 0)
        self.assertEqual(self.db.execute("SELECT count(*) FROM account_directory_rows").fetchone()[0], 3)
        self.assertEqual({row[0] for row in self.db.execute("SELECT display_account_id FROM account_directory_rows")}, {""})
        self.assertEqual(len(preparation_inputs(self.db)), 3)

    def test_same_uid_and_genuinely_same_existing_target_are_still_batch_conflicts(self):
        self.existing()
        one = import_account_summary(self.db, payload(record(2, uid="123456789", display="one"),
            record(3, uid="123456789", display="two")), imported_at=LATER)
        self.assertEqual(one["counts"]["review"], 2)
        two = import_account_summary(self.db, payload(record(4, uid="123456789", display="new_handle"),
            record(5, uid="", display="shared_handle", verified=False), sha="c" * 64), imported_at=LATER)
        self.assertEqual(two["counts"]["review"], 2)
        self.assertEqual(self.db.execute("SELECT operator_name FROM accounts").fetchone()[0], "old operator")

    def test_cross_platform_same_identifier_does_not_poison_batch_targets(self):
        self.existing(uid="123456789", platform="douyin")
        result = import_account_summary(self.db, payload(record(2, uid="123456789", display="dy_handle"),
            record(3, platform="快手", uid="123456789", display="", verified=False)), imported_at=LATER)
        self.assertEqual([row["status"] for row in result["rows"]], ["updated", "added"])

    def test_unsupported_ks_source_ids_keep_original_records_without_directory_or_false_binding(self):
        self.existing(uid="123456789", platform="kuaishou", display="99887766")
        before = tuple(self.db.execute("SELECT * FROM account_directory_rows").fetchone())
        for index, kind in enumerate(("source_numeric_ks_id", "source_short_link_eid_conflict"), 2):
            r = record(index, platform="快手", uid="", display="99887766", verified=False, 运营人员="must not overwrite")
            r["metadata"]["enrichment_identity_kind"] = kind
            r["metadata"]["enrichment_profile"] = {"uid": "55555555", "eid": "3xotherperson"}
            result = import_account_summary(self.db, payload(r, sha=str(index) * 64), imported_at=LATER)
            self.assertEqual(result["rows"][0]["status"], "blocked")
            self.assertEqual(result["rows"][0]["reason"], "locator_resolution_required")
            self.assertIsNone(result["rows"][0]["directory_row_id"])
            request = dict(self.db.execute("SELECT * FROM account_intake_requests ORDER BY id DESC LIMIT 1").fetchone())
            self.assertEqual(json.loads(request["source_json"])["record"]["metadata"], r["metadata"])
            self.assertEqual(json.loads(request["source_json"])["record"]["comment"], r["comment"])
            self.assertEqual(json.loads(request["input_json"])["uid"], "")
        self.assertEqual(tuple(self.db.execute("SELECT * FROM account_directory_rows").fetchone()), before)
        self.assertEqual(preparation_inputs(self.db), [])

    def test_web_unsupported_locator_is_blocked_then_new_uid_request_enters_preparation(self):
        value = {"platform": "kuaishou", "display_account_id": "99887766", "nickname": "source name"}
        blocked = submit_account_intake(self.db, request_key="web-unknown", value=value, source={"kind": "web"}, at=STAMP)
        self.assertEqual(blocked["status"], "blocked")
        self.assertIsNone(blocked["directory_row_id"])
        before = self.db.total_changes
        self.assertTrue(submit_account_intake(self.db, request_key="web-unknown", value=value, source={"kind": "web"}, at=STAMP)["replayed"])
        self.assertEqual(self.db.total_changes, before)
        accepted = submit_account_intake(self.db, request_key="web-new-uid", value={**value, "uid": "123456789"}, source={"kind": "web"}, at=LATER)
        self.assertEqual(accepted["status"], "accepted")
        self.assertIsNotNone(accepted["directory_row_id"])

    def test_six_syntactically_valid_unverified_douyin_uids_are_legitimate_pending_inputs(self):
        rows = [record(index + 2, uid=str(1234567890000000 + index), display="", verified=False) for index in range(6)]
        result = import_account_summary(self.db, payload(*rows), imported_at=LATER)
        self.assertEqual(result["counts"]["added"], 6)
        self.assertEqual(len(preparation_inputs(self.db)), 6)
        self.assertEqual(self.db.execute("SELECT count(*) FROM accounts").fetchone()[0], 0)

    def test_unresolved_account_name_never_rebinds_people_phone_or_documents_during_prepare(self):
        aid, _, did = self.existing()
        incoming = record(40, uid="123456789", display="shared_handle", verified=True,
            账号名称="待核实", 运营人员="different operator", 手机号="00999999999",
            手机号开卡人姓名="different opener", 使用人证件号码="001234567890123456", 持卡人="different holder")
        result = import_account_summary(self.db, payload(incoming), imported_at=LATER)
        self.assertEqual(tuple(self.db.execute("SELECT phone,operator_name FROM accounts WHERE id=?", (aid,)).fetchone()), ("00123456789", "old operator"))
        directory = dict(self.db.execute("SELECT * FROM account_directory_rows WHERE id=?", (did,)).fetchone())
        self.assertEqual((directory["phone"], directory["operator_name"]), ("00123456789", "old operator"))
        for field in ("运营人员", "手机号", "手机号开卡人姓名", "使用人证件号码", "持卡人"):
            self.assertEqual(result["rows"][0]["pending_fields"][field], "账号名称归属待核实，人员及手机号关系暂不重绑定")
        request = dict(self.db.execute("SELECT * FROM account_intake_requests").fetchone())
        self.assertEqual(json.loads(request["source_json"])["record"]["raw"]["使用人证件号码"], "001234567890123456")

    def test_ks_uid_prepares_without_promoting_unverified_internal_author_id_to_display(self):
        _, _, existing = self.existing(uid="123456789", platform="kuaishou", display="known_public_handle")
        rows = [record(2, platform="快手", uid="123456789", display="99887766", verified=True),
                record(3, platform="快手", uid="987654321", display="88776655", verified=True)]
        for row in rows:
            row["metadata"]["enrichment_profile"]["ID"] = "independently_known_other_handle"
        result = import_account_summary(self.db, payload(*rows), imported_at=LATER)
        self.assertEqual([row["status"] for row in result["rows"]], ["updated", "added"])
        self.assertEqual(self.db.execute("SELECT display_account_id FROM account_directory_rows WHERE id=?", (existing,)).fetchone()[0], "known_public_handle")
        self.assertEqual(self.db.execute("SELECT display_account_id FROM account_directory_rows WHERE uid='987654321'").fetchone()[0], "")
        for row in result["rows"]:
            self.assertIn(DISPLAY_ID, row["pending_fields"])
        requests = [dict(row) for row in self.db.execute("SELECT * FROM account_intake_requests ORDER BY id")]
        self.assertEqual(len(preparation_inputs(self.db)), 2)
        for saved, source in zip(requests, rows):
            self.assertEqual(json.loads(saved["input_json"])["display_account_id"], "")
            self.assertEqual(json.loads(saved["source_json"])["record"]["raw"][DISPLAY_ID], source["raw"][DISPLAY_ID])


if __name__ == "__main__":
    unittest.main()
