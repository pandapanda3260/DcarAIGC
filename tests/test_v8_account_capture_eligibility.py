from __future__ import annotations

import hashlib
import json
import socket
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from v8.account_capture_eligibility import (
    DirectoryCaptureEligibilityError, derive_capture_eligibility,
    require_directory_capture_member,
)


AT = "2026-09-09T00:00:00Z"
SEC = "MS4wLjAB" + "A" * 64


class AccountCaptureEligibilityTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="capture-eligibility-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.connection = sqlite3.connect(":memory:")
        self.connection.row_factory = sqlite3.Row
        self.addCleanup(self.connection.close)
        self.connection.executescript("""
            CREATE TABLE accounts(id INTEGER PRIMARY KEY,enabled INTEGER,created_at TEXT);
            CREATE TABLE account_platform_identities(id INTEGER PRIMARY KEY,account_id INTEGER,platform TEXT,uid TEXT);
            CREATE TABLE account_directory_rows(id INTEGER PRIMARY KEY,account_id INTEGER,platform TEXT,uid TEXT,
                account_status TEXT,identity_status TEXT,imported_at TEXT,raw_json TEXT DEFAULT '{}');
            CREATE TABLE account_provider_references(account_identity_id INTEGER,provider TEXT,reference_kind TEXT,
                reference_value TEXT,source_raw_response_id INTEGER);
            CREATE TABLE provider_raw_responses(id INTEGER PRIMARY KEY,account_id INTEGER,provider TEXT,
                operation TEXT,local_path TEXT,sha256 TEXT,byte_size INTEGER,http_status INTEGER);
            CREATE TABLE account_roster_members(snapshot_id INTEGER,account_identity_id INTEGER);
            CREATE TABLE scheduler_runs(id INTEGER PRIMARY KEY,job_id TEXT,scheduled_for TEXT,status TEXT,
                started_at TEXT,completed_at TEXT,details_json TEXT);
            CREATE TABLE scheduler_run_attempts(id INTEGER PRIMARY KEY,scheduler_run_id INTEGER,attempt_number INTEGER,
                invocation_source TEXT,status TEXT,started_at TEXT,completed_at TEXT,details_json TEXT);
        """)
        denied = patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden"))
        denied.start()
        self.addCleanup(denied.stop)

    def add(self, account_id=1, *, status="daily", identity_status="existing_verified",
            platform="douyin", uid=None, enabled=0, sec=SEC, raw=True):
        uid = uid or (str(12345670 + account_id) if platform == "douyin" else f"{account_id:024x}")
        self.connection.execute("INSERT INTO accounts VALUES(?,?,?)", (account_id, enabled, AT))
        self.connection.execute("INSERT INTO account_platform_identities VALUES(?,?,?,?)", (account_id, account_id, platform, uid))
        self.connection.execute("INSERT INTO account_directory_rows(id,account_id,platform,uid,account_status,identity_status,imported_at) VALUES(?,?,?,?,?,?,?)",
                                (account_id, account_id, platform, uid, status, identity_status, AT))
        if platform == "douyin" and sec is not None:
            self.connection.execute("INSERT INTO account_provider_references VALUES(?,'TikHub','sec_user_id',?,?)",
                                    (account_id, sec, account_id if raw else None))
            if raw:
                self.raw(account_id, {"code": 200, "data": {"status_code": 0,
                         "data": {"id_str": uid, "sec_user_id": sec}}})
        return uid

    def raw(self, account_id, value):
        entity = json.dumps(value).encode()
        path = self.root / f"{account_id}.json"
        path.write_bytes(entity)
        self.connection.execute("INSERT OR REPLACE INTO provider_raw_responses VALUES(?,?,'TikHub','douyin_uid_profile',?,?,?,200)",
                                (account_id, account_id, str(path), hashlib.sha256(entity).hexdigest(), len(entity)))

    def read(self):
        self.connection.commit()
        self.connection.execute("PRAGMA query_only=ON")
        before = self.connection.total_changes
        result = derive_capture_eligibility(self.connection)
        self.assertEqual(before, self.connection.total_changes)
        self.connection.execute("PRAGMA query_only=OFF")
        return result

    def reason(self):
        result = self.read()
        self.assertEqual(result["eligible_members"], [])
        self.assertEqual(len(result["excluded_members"]), 1)
        value = result["excluded_members"][0]
        self.assertTrue(value["reason_label"])
        self.assertFalse(value["enabled"])
        return value["reason_code"]

    def admission(self, account_id=1, *, sec=SEC):
        from v8.account_operating_receipts import record_status_receipt
        identity = dict(self.connection.execute("SELECT * FROM account_platform_identities WHERE id=?", (account_id,)).fetchone())
        if not self.connection.in_transaction:
            self.connection.execute("BEGIN")
        return record_status_receipt(self.connection, request_id=f"admission-{account_id}",
            account_id=account_id, account_identity_id=account_id, requested_status="daily", update_frequency="daily",
            request={"account_status": "daily", "fields": {}, "admission": {"member": {
                "platform": identity["platform"], "uid": identity["uid"], "metadata": {"sec_user_id": sec}}}},
            actor="test", reason="verified profile admission", before={"enabled": False, "update_frequency": None},
            after={"enabled": True, "update_frequency": "daily"},
            result={"id": account_id, "status_request_id": f"admission-{account_id}",
                    "account_status": "daily", "enabled": True, "update_frequency": "daily"}, timestamp=AT)

    def test_directory_authority_ignores_old_disabled_flag_and_absent_roster(self):
        self.add(enabled=0)
        result = self.read()
        self.assertEqual(result["excluded_members"], [])
        member = result["eligible_members"][0]
        self.assertEqual(member["identity_id"], member["account_identity_id"])
        self.assertEqual(member["locator_sha256"], hashlib.sha256(SEC.encode()).hexdigest())
        self.assertTrue(member["enabled"])
        self.assertEqual(member["created_at"], AT)
        self.assertEqual(member["accepted_at"], AT)
        self.assertNotIn(SEC, json.dumps(result))
        self.assertEqual(require_directory_capture_member(self.connection, 1), member)
        self.assertEqual(self.connection.execute("SELECT enabled FROM accounts").fetchone()[0], 0)

    def test_missing_migrated_blob_uses_only_hash_verified_original_response(self):
        from v8 import raw_archive
        self.add()
        self.connection.execute("ALTER TABLE provider_raw_responses ADD COLUMN raw_blob_id INTEGER")
        self.connection.execute("UPDATE provider_raw_responses SET raw_blob_id=9")
        self.connection.execute("CREATE TABLE provider_raw_blobs(id INTEGER,hot_state TEXT)")
        self.connection.execute("INSERT INTO provider_raw_blobs VALUES(9,'present')")
        with patch.object(raw_archive, "read_response_entity", side_effect=raw_archive.RawArchiveError("raw path is missing")):
            self.assertEqual(len(self.read()["eligible_members"]), 1)
            (self.root / "1.json").write_text('{"different":true}')
            self.assertEqual(self.reason(), "reference_evidence_unavailable")

    def test_retired_or_corrupt_blob_does_not_use_original_fallback(self):
        from v8 import raw_archive
        self.add()
        self.connection.execute("ALTER TABLE provider_raw_responses ADD COLUMN raw_blob_id INTEGER")
        self.connection.execute("UPDATE provider_raw_responses SET raw_blob_id=9")
        self.connection.execute("CREATE TABLE provider_raw_blobs(id INTEGER,hot_state TEXT)")
        self.connection.execute("INSERT INTO provider_raw_blobs VALUES(9,'deleted')")
        for reason in ("raw path is missing", "raw blob stored checksum/size mismatch"):
            with patch.object(raw_archive, "read_response_entity", side_effect=raw_archive.RawArchiveError(reason)):
                self.assertEqual(self.reason(), "reference_evidence_unavailable")

    def test_pause_rejects_already_planned_member_and_does_not_touch_history(self):
        self.add(enabled=1)
        self.connection.execute("INSERT INTO account_roster_members VALUES(3,1)")
        self.assertTrue(require_directory_capture_member(self.connection, 1)["eligible"])
        self.connection.execute("UPDATE account_directory_rows SET account_status='paused'")
        with self.assertRaises(DirectoryCaptureEligibilityError) as error:
            require_directory_capture_member(self.connection, 1)
        self.assertEqual(error.exception.code, "account_paused")
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM account_roster_members").fetchone()[0], 1)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM provider_raw_responses").fetchone()[0], 1)

    def test_unmarked_and_unverified_have_explicit_reasons(self):
        self.add(status="unmarked")
        self.assertEqual(self.reason(), "account_status_unmarked")
        self.connection.execute("UPDATE account_directory_rows SET account_status='weekly',identity_status='uid_unverified'")
        self.assertEqual(self.reason(), "identity_unverified")

    def test_missing_directory_identity_is_accounted_for(self):
        self.connection.execute("INSERT INTO account_directory_rows(id,platform,account_status,identity_status,imported_at) VALUES(1,'douyin','daily','identity_missing',?)", (AT,))
        self.assertEqual(self.reason(), "identity_missing")

    def test_uid_mismatch_and_multiple_account_identities_are_rejected(self):
        self.add()
        self.connection.execute("UPDATE account_directory_rows SET uid='99999999'")
        self.assertEqual(self.reason(), "identity_conflict")
        self.connection.execute("UPDATE account_directory_rows SET uid='12345671'")
        self.connection.execute("INSERT INTO account_platform_identities VALUES(2,1,'douyin','99999999')")
        self.assertEqual(self.reason(), "identity_conflict")

    def test_duplicate_directory_rows_are_never_chosen_arbitrarily(self):
        self.add()
        self.connection.execute("INSERT INTO account_directory_rows SELECT 2,account_id,platform,uid,account_status,identity_status,imported_at,raw_json FROM account_directory_rows")
        result = self.read()
        self.assertEqual(result["eligible_members"], [])
        self.assertEqual([row["reason_code"] for row in result["excluded_members"]], ["directory_conflict"] * 2)

    def test_missing_reference_and_source_evidence_are_distinct(self):
        self.add(sec=None)
        self.assertEqual(self.reason(), "reference_missing")
        self.connection.execute("INSERT INTO account_provider_references VALUES(1,'TikHub','sec_user_id',?,NULL)", (SEC,))
        self.assertEqual(self.reason(), "reference_evidence_missing")

    def test_raw_hash_and_availability_are_verified(self):
        self.add()
        (self.root / "1.json").write_text("tampered")
        self.assertEqual(self.reason(), "reference_evidence_unavailable")
        (self.root / "1.json").unlink()
        self.assertEqual(self.reason(), "reference_evidence_unavailable")

    def test_same_response_but_different_objects_do_not_prove_identity(self):
        uid = self.add()
        self.raw(1, {"code": 200, "data": {"user": {"uid": uid}, "other": {"sec_user_id": SEC}}})
        self.assertEqual(self.reason(), "reference_identity_mismatch")

    def test_wrong_uid_provider_account_or_operation_are_rejected(self):
        self.add()
        self.raw(1, {"code": 200, "data": {"uid": "99999999", "sec_user_id": SEC}})
        self.assertEqual(self.reason(), "reference_identity_mismatch")
        self.raw(1, {"code": 200, "data": {"uid": "12345671", "sec_user_id": SEC}})
        for column, value in (("account_id", 2), ("provider", "other"), ("operation", "douyin_video_detail"), ("http_status", 403)):
            with self.subTest(column=column):
                self.connection.execute(f"UPDATE provider_raw_responses SET {column}=?", (value,))
                self.assertEqual(self.reason(), "reference_identity_mismatch")
                self.raw(1, {"code": 200, "data": {"uid": "12345671", "sec_user_id": SEC}})

    def test_cross_identity_reference_conflict_includes_non_directory_identity(self):
        self.add()
        self.connection.execute("INSERT INTO account_provider_references VALUES(99,'tikhub','sec_user_id',?,NULL)", (SEC,))
        self.assertEqual(self.reason(), "reference_conflict")

    def test_case_duplicate_same_locator_is_safe_but_conflicting_locator_is_not(self):
        self.add()
        self.connection.execute("INSERT INTO account_provider_references VALUES(1,'tikhub','sec_user_id',?,NULL)", (SEC,))
        self.assertEqual(len(self.read()["eligible_members"]), 1)
        self.connection.execute("UPDATE account_provider_references SET reference_value=? WHERE provider='tikhub'", ("MS4wLjAB" + "B" * 64,))
        self.assertEqual(self.reason(), "reference_conflict")

    def test_weekly_label_does_not_change_snapshot_identity_but_locator_does(self):
        self.add()
        original = self.read()["selection_sha256"]
        self.connection.execute("UPDATE account_directory_rows SET account_status='weekly'")
        self.assertEqual(self.read()["selection_sha256"], original)
        self.connection.execute("UPDATE account_directory_rows SET account_status='paused'")
        self.assertNotEqual(self.read()["selection_sha256"], original)

    def test_xhs_uses_verified_uid_without_opening_provider_gate(self):
        self.add(platform="xiaohongshu")
        member = self.read()["eligible_members"][0]
        self.assertEqual(member["locator_kind"], "uid")
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM provider_raw_responses").fetchone()[0], 0)
        self.connection.execute("UPDATE account_directory_rows SET platform='kuaishou'")
        self.assertEqual(self.reason(), "platform_unsupported")

    def test_verified_admission_is_sufficient_without_a_paid_raw_lookup(self):
        self.add(sec=None)
        self.admission()
        member = self.read()["eligible_members"][0]
        self.assertEqual(member["locator_evidence"]["kind"], "verified_account_admission")
        self.assertEqual(member["locator_sha256"], hashlib.sha256(SEC.encode()).hexdigest())
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM account_provider_references").fetchone()[0], 0)

    def test_directory_raw_json_cannot_replace_immutable_admission(self):
        self.add(sec=None)
        self.connection.execute("UPDATE account_directory_rows SET raw_json=?", (json.dumps({"sec_user_id": SEC}),))
        self.assertEqual(self.reason(), "reference_missing")

    def test_tampered_admission_is_rejected(self):
        self.add(sec=None)
        self.admission()
        self.connection.execute("UPDATE scheduler_runs SET details_json='{}'")
        with self.assertRaises(DirectoryCaptureEligibilityError) as error:
            self.read()
        self.assertEqual(error.exception.code, "admission_evidence_invalid")

    def test_admission_and_stored_reference_must_agree(self):
        self.add(raw=False)
        self.admission(sec="MS4wLjAB" + "B" * 64)
        self.assertEqual(self.reason(), "reference_conflict")


if __name__ == "__main__":
    unittest.main()
