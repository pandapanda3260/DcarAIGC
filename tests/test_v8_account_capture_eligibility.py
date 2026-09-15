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
    identity_capture_evidence, require_directory_capture_member,
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

    def raw(self, account_id, value, *, operation="douyin_uid_profile"):
        entity = json.dumps(value).encode()
        path = self.root / f"{account_id}.json"
        path.write_bytes(entity)
        self.connection.execute("INSERT OR REPLACE INTO provider_raw_responses VALUES(?,?,'TikHub',?,?,?,?,200)",
                                (account_id, account_id, operation, str(path), hashlib.sha256(entity).hexdigest(), len(entity)))

    def xhs_profile(self, account_id=1):
        uid = self.connection.execute("SELECT uid FROM account_platform_identities WHERE id=?", (account_id,)).fetchone()[0]
        return {"code": 200, "router": "/api/v1/xiaohongshu/app_v2/get_user_info",
                "params": {"user_id": uid}, "data": {"code": 0, "success": True,
                "data": {"userid": uid, "nickname": "Test profile",
                         "result": {"success": True, "code": 0}}}}

    def xhs_raw(self, account_id=1, *, value=None):
        self.raw(account_id, value if value is not None else self.xhs_profile(account_id),
                 operation="xiaohongshu_user_profile")

    def xhs_reference(self, account_id=1):
        uid = self.connection.execute("SELECT uid FROM account_platform_identities WHERE id=?", (account_id,)).fetchone()[0]
        self.connection.execute("INSERT INTO account_provider_references VALUES(?,'TikHub','user_id',?,?)",
                                (account_id, uid, account_id))
        self.xhs_raw(account_id)

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

    def test_operating_labels_do_not_reject_member_or_change_history(self):
        self.add(enabled=1)
        self.connection.execute("INSERT INTO account_roster_members VALUES(3,1)")
        for status in ("daily", "weekly", "paused", "unmarked", "legacy-label"):
            with self.subTest(status=status):
                self.connection.execute("UPDATE account_directory_rows SET account_status=?", (status,))
                member = require_directory_capture_member(self.connection, 1)
                self.assertTrue(member["eligible"])
                self.assertEqual(member["account_status"], status)
                self.assertEqual(self.connection.execute("SELECT account_status FROM account_directory_rows").fetchone()[0], status)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM account_roster_members").fetchone()[0], 1)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM provider_raw_responses").fetchone()[0], 1)

    def test_unmarked_and_import_label_do_not_veto_proven_identity(self):
        self.add(status="unmarked")
        self.assertEqual(len(self.read()["eligible_members"]), 1)
        self.connection.execute("UPDATE account_directory_rows SET account_status='weekly',identity_status='uid_unverified'")
        self.assertEqual(len(self.read()["eligible_members"]), 1)
        self.assertEqual(self.connection.execute("SELECT identity_status FROM account_directory_rows").fetchone()[0], "uid_unverified")

    def test_live_account_annotation_matches_full_qualification_for_every_status(self):
        from v8.account_catalog_capture import annotate_accounts
        self.add(identity_status="uid_unverified")
        for status in ("daily", "weekly", "paused", "unmarked"):
            with self.subTest(status=status):
                self.connection.execute("UPDATE account_directory_rows SET account_status=?", (status,))
                account = {"id": 1, "directory_row_id": 1, "account_status": status,
                    "directory_platform": "douyin", "directory_uid": "12345671",
                    "platforms": [{"id": 1, "platform": "douyin", "uid": "12345671"}]}
                member = require_directory_capture_member(self.connection, 1)
                annotate_accounts(self.connection, [account], live=True)
                self.assertEqual(account["automatic_capture"],
                    {key: member[key] for key in ("eligible", "reason_code", "reason_label")})
        self.connection.execute("DELETE FROM account_provider_references")
        annotate_accounts(self.connection, [account], live=True)
        self.assertEqual(account["automatic_capture"]["reason_code"], "reference_missing")
        self.assertFalse(account["automatic_capture"]["eligible"])

    def test_imported_identity_needs_real_locator_evidence(self):
        self.add(identity_status="uid_unverified", sec=None)
        self.connection.execute("UPDATE account_directory_rows SET raw_json=?", (
            json.dumps({"enrichment_status": "verified", "sec_user_id": SEC}),))
        self.assertEqual(self.reason(), "reference_missing")
        self.connection.execute("INSERT INTO account_provider_references VALUES(1,'TikHub','sec_user_id',?,NULL)", (SEC,))
        self.assertEqual(self.reason(), "reference_evidence_missing")

    def test_imported_identity_still_rejects_bad_hash_and_wrong_uid(self):
        self.add(identity_status="uid_unverified")
        for status in ("daily", "weekly", "paused", "unmarked"):
            with self.subTest(status=status):
                self.connection.execute("UPDATE account_directory_rows SET account_status=?", (status,))
                (self.root / "1.json").write_text("tampered")
                self.assertEqual(self.reason(), "reference_evidence_unavailable")
                self.raw(1, {"code": 200, "data": {"uid": "99999999", "sec_user_id": SEC}})
                self.assertEqual(self.reason(), "reference_identity_mismatch")

    def test_paused_imported_identity_still_rejects_duplicate_identity(self):
        self.add(identity_status="uid_unverified", status="paused")
        self.connection.execute("INSERT INTO account_platform_identities VALUES(2,2,'douyin','12345671')")
        self.assertEqual(self.reason(), "identity_conflict")
        self.connection.execute("DELETE FROM account_platform_identities WHERE id=2")
        self.assertTrue(require_directory_capture_member(self.connection, 1)["eligible"])

    def test_identity_helper_and_capture_share_same_read_only_result(self):
        self.add(identity_status="uid_unverified", status="paused")
        self.connection.commit()
        self.connection.execute("PRAGMA query_only=ON")
        before = self.connection.total_changes
        member = require_directory_capture_member(self.connection, 1)
        self.assertTrue(member["eligible"])
        self.assertEqual(identity_capture_evidence(self.connection, 1), member)
        self.assertEqual(self.connection.total_changes, before)
        self.connection.execute("PRAGMA query_only=OFF")
        self.connection.execute("UPDATE account_directory_rows SET account_status='unmarked'")
        member = require_directory_capture_member(self.connection, 1)
        self.assertTrue(member["eligible"])
        self.assertEqual(identity_capture_evidence(self.connection, 1), member)

    def test_status_edit_identity_proof_still_requires_unique_matching_evidence(self):
        self.add(identity_status="uid_unverified", status="paused")
        (self.root / "1.json").write_text("tampered")
        self.assertEqual(identity_capture_evidence(self.connection, 1)["reason_code"], "reference_evidence_unavailable")
        self.assertEqual(identity_capture_evidence(self.connection, 999)["reason_code"], "identity_missing")
        self.assertEqual(identity_capture_evidence(self.connection, None)["reason_code"], "identity_missing")
        self.connection.execute("INSERT INTO account_directory_rows SELECT 2,account_id,platform,uid,account_status,identity_status,imported_at,raw_json FROM account_directory_rows")
        self.assertEqual(identity_capture_evidence(self.connection, 1)["reason_code"], "directory_conflict")

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

    def test_operating_labels_do_not_change_selection_but_locator_does(self):
        self.add()
        original = self.read()["selection_sha256"]
        for status in ("daily", "weekly", "paused", "unmarked"):
            self.connection.execute("UPDATE account_directory_rows SET account_status=?", (status,))
            self.assertEqual(self.read()["selection_sha256"], original)
        new_sec = "MS4wLjAB" + "B" * 64
        self.connection.execute("UPDATE account_provider_references SET reference_value=?", (new_sec,))
        self.raw(1, {"code": 200, "data": {"uid": "12345671", "sec_user_id": new_sec}})
        self.assertEqual(len(self.read()["eligible_members"]), 1)
        self.assertNotEqual(self.read()["selection_sha256"], original)

    def test_xhs_uses_verified_uid_without_opening_provider_gate(self):
        self.add(platform="xiaohongshu")
        member = self.read()["eligible_members"][0]
        self.assertEqual(member["locator_kind"], "uid")
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM provider_raw_responses").fetchone()[0], 0)
        self.connection.execute("UPDATE account_directory_rows SET platform='kuaishou'")
        self.assertEqual(self.reason(), "identity_conflict")

    def test_kuaishou_requires_original_profile_for_exact_numeric_uid(self):
        uid = self.add(platform="kuaishou", uid="000123456")
        self.assertEqual(self.reason(), "identity_evidence_missing")
        payload = {"code": 200, "router": "/api/v1/kuaishou/app/fetch_one_user_v2", "params": {"user_id": uid},
            "data": {"result": 1, "userProfile": {"profile": {"user_id": uid, "user_name": "Test"}, "ownerCount": {"fan": 12}}}}
        self.raw(1, payload, operation="kuaishou_user_profile")
        self.connection.execute("INSERT INTO account_provider_references VALUES(1,'TikHub','user_id',?,1)", (uid,))
        self.assertEqual(self.read()["eligible_members"][0]["uid"], uid)
        payload["data"]["userProfile"]["profile"]["user_id"] = "123456"
        self.raw(1, payload, operation="kuaishou_user_profile")
        self.assertEqual(self.reason(), "reference_identity_mismatch")

    def test_preparation_raw_is_bound_through_intake_without_rewriting_raw(self):
        uid = self.add(raw=False)
        from v8.platform_adapters import normalize_profile
        value = {"platform": "douyin", "uid": uid}
        payload = {"code": 200, "router": "/api/v1/douyin/web/fetch_user_profile_by_uid", "params": {"uid": uid},
                   "data": {"status_code": 0, "data": {"id_str": uid, "sec_user_id": SEC, "nickname": "Test"}}}
        self.raw(1, payload)
        profile = normalize_profile("douyin", value, payload, prior_responses=[{"operation": "douyin_uid_profile", "payload": payload, "raw_response_id": 1}])
        self.connection.execute("ALTER TABLE provider_raw_responses ADD COLUMN intake_request_id INTEGER")
        self.connection.execute("CREATE TABLE account_intake_requests(id INTEGER PRIMARY KEY,account_id INTEGER,account_identity_id INTEGER,platform TEXT,input_json TEXT,input_sha256 TEXT,result_json TEXT,completed_at TEXT)")
        encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        result = {"status": "ready", "uid": uid, "source_raw_response_ids": [1], "profile": profile}
        self.connection.execute("INSERT INTO account_intake_requests VALUES(10,1,1,'douyin',?,?,?,?)", (encoded, hashlib.sha256(encoded.encode()).hexdigest(), json.dumps(result), AT))
        self.connection.execute("UPDATE provider_raw_responses SET account_id=NULL,intake_request_id=10")
        self.connection.execute("UPDATE account_provider_references SET source_raw_response_id=1")
        self.assertEqual(len(self.read()["eligible_members"]), 1)
        self.connection.execute("UPDATE account_intake_requests SET account_identity_id=2")
        self.assertEqual(self.reason(), "reference_identity_mismatch")

    def test_wechat_intake_replays_exact_full_chain_and_rejects_swapped_input(self):
        from v8.platform_adapters import normalize_profile, ROUTES
        uid = self.add(platform="wechat_channels", uid="v2_0123456789abcdef@finder")
        value = {"platform": "wechat_channels", "display_account_id": "sphwanted"}
        operations = ["wechat_channels_resolve", "wechat_channels_channel_info", "wechat_channels_user_profile"]
        params = [{"channel_id": "sphwanted", "raw": True}, {"username": uid, "raw": True}, {"username": uid, "raw": True}]
        datas = [{"ret": 0, "data": [{"items": [{"jumpInfo": {"userName": uid}}]}]},
                 {"baseResponse": {"ret": 0}, "sections": [{"items": [{"title": "Channels ID", "content": "sphwanted"}]}]},
                 {"baseResponse": {"ret": 0}, "contact": {"username": uid, "nickname": "Test"}}]
        responses = []
        for response_id, (op, parameter, data) in enumerate(zip(operations, params, datas), 1):
            payload = {"code": 200, "router": ROUTES[op][1], "params": parameter, "data": data}
            self.raw(response_id, payload, operation=op)
            responses.append({"operation": op, "raw_response_id": response_id, "payload": payload})
        profile = normalize_profile("wechat_channels", value, responses[-1]["payload"], prior_responses=responses)
        self.connection.execute("ALTER TABLE provider_raw_responses ADD COLUMN intake_request_id INTEGER")
        self.connection.execute("CREATE TABLE account_intake_requests(id INTEGER PRIMARY KEY,account_id INTEGER,account_identity_id INTEGER,platform TEXT,input_json TEXT,input_sha256 TEXT,result_json TEXT,completed_at TEXT)")
        encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        result = {"status": "ready", "uid": uid, "source_raw_response_ids": [1, 2, 3], "profile": profile}
        self.connection.execute("INSERT INTO account_intake_requests VALUES(10,1,1,'wechat_channels',?,?,?,?)", (encoded, hashlib.sha256(encoded.encode()).hexdigest(), json.dumps(result), AT))
        self.connection.execute("UPDATE provider_raw_responses SET account_id=NULL,intake_request_id=10")
        self.connection.execute("INSERT INTO account_provider_references VALUES(1,'TikHub','username',?,3)", (uid,))
        self.assertEqual(len(self.read()["eligible_members"]), 1)
        # Even if someone rewrites the intake JSON and its mutable hash, the
        # immutable response params and real returned sph remain authoritative.
        value["display_account_id"] = "sphother"
        encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        self.connection.execute("UPDATE account_intake_requests SET input_json=?,input_sha256=?", (encoded, hashlib.sha256(encoded.encode()).hexdigest()))
        self.assertEqual(self.reason(), "reference_identity_mismatch")

    def test_imported_xhs_uid_format_or_mutable_metadata_is_not_profile_evidence(self):
        self.add(platform="xiaohongshu", identity_status="uid_unverified")
        self.connection.execute("UPDATE account_directory_rows SET raw_json=?", (
            json.dumps({"enrichment_status": "verified", "verified_uid": "000000000000000000000001"}),))
        self.assertEqual(self.reason(), "identity_evidence_missing")

    def test_imported_xhs_full_profile_proves_exact_uid_for_every_manual_status(self):
        self.add(platform="xiaohongshu", identity_status="uid_unverified")
        self.xhs_reference()
        for status in ("daily", "weekly", "paused", "unmarked"):
            with self.subTest(status=status):
                self.connection.execute("UPDATE account_directory_rows SET account_status=?", (status,))
                member = self.read()["eligible_members"][0]
                self.assertEqual(member["locator_kind"], "uid")
                self.assertEqual(member["locator_evidence"]["kind"], "provider_profile_raw")
                self.assertEqual(member["locator_evidence"]["raw_response_id"], 1)
                self.assertEqual(member["locator_evidence"]["raw_sha256"],
                                 member["locator_evidence"]["entity_sha256"])
                self.assertEqual(member["account_status"], status)
                self.assertEqual(member["identity_status"], "uid_unverified")

    def test_imported_xhs_profile_entity_hash_size_and_availability_are_required(self):
        self.add(platform="xiaohongshu", identity_status="uid_unverified")
        self.xhs_reference()
        (self.root / "1.json").write_text("tampered")
        self.assertEqual(self.reason(), "reference_evidence_unavailable")
        self.xhs_raw()
        self.connection.execute("UPDATE provider_raw_responses SET byte_size=byte_size+1")
        self.assertEqual(self.reason(), "reference_evidence_unavailable")
        (self.root / "1.json").unlink()
        self.assertEqual(self.reason(), "reference_evidence_unavailable")

    def test_imported_xhs_profile_requires_all_success_codes_and_exact_request_identity(self):
        self.add(platform="xiaohongshu", identity_status="uid_unverified")
        self.xhs_reference()
        failures = [
            (("code",), 403), (("code",), "200"),
            (("router",), "/api/v1/xiaohongshu/app_v2/search_users"),
            (("params", "user_id"), "000000000000000000000002"),
            (("params",), {"share_text": "https://example.invalid"}),
            (("data", "code"), 1), (("data", "code"), False),
            (("data", "success"), False), (("data", "success"), 1),
            (("data", "data", "userid"), "000000000000000000000002"),
            (("data", "data", "uid"), "000000000000000000000002"),
            (("data", "data", "nickname"), ""),
            (("data", "data", "result", "success"), False),
            (("data", "data", "result", "code"), 1),
            (("data", "data", "result", "code"), False),
            (("data", "data", "result"), None),
        ]
        for path, value in failures:
            with self.subTest(path=path, value=value):
                payload = self.xhs_profile()
                parent = payload
                for key in path[:-1]:
                    parent = parent[key]
                parent[path[-1]] = value
                self.xhs_raw(value=payload)
                self.assertEqual(self.reason(), "reference_identity_mismatch")
        for value in ({"usable": True, "profile": {"uid": "000000000000000000000001"}},
                      {"code": 200, "data": {"userid": "000000000000000000000001"}}, []):
            self.xhs_raw(value=value)
            self.assertEqual(self.reason(), "reference_identity_mismatch")

    def test_imported_xhs_raw_account_provider_operation_and_http_must_match(self):
        self.add(platform="xiaohongshu", identity_status="uid_unverified")
        self.xhs_reference()
        for column, value in (("account_id", 2), ("provider", "other"),
                              ("operation", "xiaohongshu_discovery"), ("http_status", 403)):
            with self.subTest(column=column):
                self.connection.execute(f"UPDATE provider_raw_responses SET {column}=?", (value,))
                self.assertEqual(self.reason(), "reference_identity_mismatch")
                self.xhs_raw()
        self.connection.execute("UPDATE account_provider_references SET source_raw_response_id=NULL")
        self.assertEqual(self.reason(), "reference_evidence_missing")
        self.connection.execute("UPDATE account_provider_references SET source_raw_response_id=999")
        self.assertEqual(self.reason(), "reference_evidence_missing")

    def test_imported_xhs_reference_conflicts_cannot_be_hidden_by_a_valid_entity(self):
        uid = self.add(platform="xiaohongshu", identity_status="uid_unverified")
        self.xhs_reference()
        self.connection.execute("INSERT INTO account_provider_references VALUES(1,'tikhub','user_id',?,NULL)",
                                ("000000000000000000000002",))
        self.assertEqual(self.reason(), "reference_conflict")
        self.connection.execute("DELETE FROM account_provider_references WHERE source_raw_response_id IS NULL")
        self.connection.execute("INSERT INTO account_provider_references VALUES(99,'tikhub','user_id',?,NULL)", (uid,))
        self.assertEqual(self.reason(), "reference_conflict")
        self.connection.execute("DELETE FROM account_provider_references WHERE account_identity_id=99")
        self.raw(2, {"unrelated": True}, operation="xiaohongshu_user_profile")
        self.connection.execute("INSERT INTO account_provider_references VALUES(1,'tikhub','user_id',?,2)", (uid,))
        self.assertEqual(self.reason(), "reference_identity_mismatch")

    def test_imported_xhs_duplicate_sources_do_not_hide_a_corrupted_old_entity(self):
        uid = self.add(platform="xiaohongshu", identity_status="uid_unverified")
        self.xhs_reference()
        self.raw(2, self.xhs_profile(), operation="xiaohongshu_user_profile")
        self.connection.execute("UPDATE provider_raw_responses SET account_id=1 WHERE id=2")
        self.connection.execute("INSERT INTO account_provider_references VALUES(1,'tikhub','user_id',?,2)", (uid,))
        self.assertEqual(self.read()["eligible_members"][0]["locator_evidence"]["raw_response_id"], 2)
        (self.root / "1.json").write_text("tampered")
        self.assertEqual(self.reason(), "reference_evidence_unavailable")

    def test_xhs_existing_directory_and_admission_evidence_remain_authoritative(self):
        self.add(platform="xiaohongshu", identity_status="existing_verified")
        self.xhs_reference()
        (self.root / "1.json").write_text("tampered")
        self.assertEqual(self.read()["eligible_members"][0]["locator_evidence"]["kind"], "verified_directory_identity")
        self.connection.execute("UPDATE account_directory_rows SET identity_status='uid_unverified'")
        self.admission(sec=None)
        self.assertEqual(self.read()["eligible_members"][0]["locator_evidence"]["kind"], "verified_account_identity_admission")

    def test_imported_xhs_reuses_identity_bound_admission_without_writes(self):
        self.add(platform="xiaohongshu", identity_status="uid_unverified")
        self.admission(sec=None)
        member = self.read()["eligible_members"][0]
        self.assertEqual(member["locator_kind"], "uid")
        self.assertEqual(member["locator_evidence"]["kind"], "verified_account_identity_admission")
        self.assertEqual(self.connection.execute("SELECT identity_status FROM account_directory_rows").fetchone()[0], "uid_unverified")

    def test_imported_xhs_admission_cannot_be_reused_after_uid_changes(self):
        self.add(platform="xiaohongshu", identity_status="uid_unverified")
        self.admission(sec=None)
        self.connection.execute("UPDATE account_platform_identities SET uid='000000000000000000000002'")
        with self.assertRaises(DirectoryCaptureEligibilityError) as error:
            self.read()
        self.assertEqual(error.exception.code, "admission_evidence_invalid")

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
