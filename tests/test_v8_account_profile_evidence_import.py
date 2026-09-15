from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import patch

from v8.account_capture_eligibility import derive_capture_eligibility
from v8.account_directory import import_account_directory
from v8.account_profile_evidence_import import apply_import, plan_import, validate_envelope
from v8.operations import upsert_account
from v8.storage import connect, initialize_database, transaction

AT = "2026-09-10T13:33:10Z"
SEC = "MS4wLjAB" + "A" * 64


def envelope(platform="douyin", uid="123456789"):
    route = "/api/v1/douyin/web/fetch_user_profile_by_uid" if platform == "douyin" else "/api/v1/xiaohongshu/app_v2/get_user_info"
    params = {"uid" if platform == "douyin" else "user_id": uid}
    user = {"id_str": uid, "sec_uid": SEC, "nickname": "fixture"} if platform == "douyin" else {
        "userid": uid, "nickname": "fixture", "result": {"success": True, "code": 0}}
    payload = {"code": 200, "router": route, "params": params,
               "data": {"status_code": 0, "data": user} if platform == "douyin" else {"success": True, "code": 0, "data": user}}
    entity = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
    return {"route": route, "params": params, "http_status": 200, "captured_at": AT, "payload": payload,
            "receipt": {"contract_version": "provider-json-transport-v1", "status": "succeeded", "http_status": 200,
                        "clean_eof": True, "json_parse_ok": True, "error_code": None, "zero_body": False,
                        "partial_bytes": 0, "request_host": "api.tikhub.io", "request_started_at": AT,
                        "response_finished_at": AT, "entity_bytes": len(entity), "entity_sha256": hashlib.sha256(entity).hexdigest()}}


class ProfileEvidenceImportTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.db = self.root / "isolated.sqlite3"
        self.connection = connect(self.db)
        self.addCleanup(self.connection.close)
        initialize_database(self.connection, target_version=21)
        self.enterContext(patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden")))
        self.account = upsert_account({"platforms": [{"platform": "douyin", "uid": "123456789"}]}, db_path=self.db)["id"]
        with transaction(self.connection):
            import_account_directory(self.connection, {"sha256": "a" * 64, "source": "fixture", "sheet": "accounts",
                "records": [{"sourceRow": 2, "raw": {"平台": "抖音", "UID": "123456789", "更新状态": "暂停"}}]}, imported_at=AT)
            self.connection.execute("UPDATE account_directory_rows SET identity_status='uid_unverified'")
        self.path = self.root / "response.json"
        self.path.write_text(json.dumps([envelope()]))
        self.evidence = self.root / "evidence"

    def test_complete_response_import_preserves_accounts_and_replays_without_writes(self):
        before = {table: [tuple(row) for row in self.connection.execute("SELECT * FROM " + table)] for table in (
            "accounts", "account_platform_identities", "account_directory_rows", "scheduler_runs", "capture_work_items", "provider_usage")}
        with transaction(self.connection):
            plans, rows = plan_import(self.connection, [self.path])
            self.assertEqual(len(plans), 1)
            apply_import(self.connection, plans, evidence_dir=self.evidence)
        self.assertEqual(rows[0]["status"], "imported")
        derived = derive_capture_eligibility(self.connection)
        self.assertEqual(len(derived["eligible_members"]), 1)
        self.assertEqual(derived["eligible_members"][0]["account_status"], "paused")
        for table, original in before.items():
            self.assertEqual([tuple(row) for row in self.connection.execute("SELECT * FROM " + table)], original, table)
        writes = self.connection.total_changes
        plans, rows = plan_import(self.connection, [self.path])
        self.assertEqual(plans, [])
        self.assertEqual(rows[0]["status"], "unchanged")
        self.assertEqual(writes, self.connection.total_changes)
        self.assertEqual(self.connection.execute("SELECT source FROM provider_raw_responses").fetchone()[0], "local_profile_evidence_import")

    def test_xhs_full_response_passes_shared_qualification_after_import(self):
        uid = "a" * 24
        with transaction(self.connection):
            self.connection.execute("UPDATE account_platform_identities SET platform='xiaohongshu',uid=?", (uid,))
            self.connection.execute("UPDATE account_directory_rows SET platform='xiaohongshu',uid=?", (uid,))
        self.path.write_text(json.dumps([envelope("xiaohongshu", uid)]))
        with transaction(self.connection):
            plans, _ = plan_import(self.connection, [self.path])
            apply_import(self.connection, plans, evidence_dir=self.evidence)
        member = derive_capture_eligibility(self.connection)["eligible_members"][0]
        self.assertEqual(member["platform"], "xiaohongshu")
        self.assertEqual(member["locator_evidence"]["kind"], "provider_profile_raw")

    def test_summary_altered_payload_failed_transport_and_request_mismatch_rejected(self):
        original = envelope()
        variations = []
        slim = copy.deepcopy(original); slim.pop("payload"); variations.append(slim)
        changed = copy.deepcopy(original); changed["payload"]["data"]["data"]["id_str"] = "987654321"; variations.append(changed)
        failed = copy.deepcopy(original); failed["receipt"]["clean_eof"] = False; variations.append(failed)
        mismatch = copy.deepcopy(original); mismatch["params"] = {"uid": "987654321"}; variations.append(mismatch)
        for value in variations:
            with self.subTest(value=variations.index(value)), self.assertRaises(ValueError):
                validate_envelope(value)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM provider_raw_responses").fetchone()[0], 0)

    def test_locator_conflict_and_changed_existing_evidence_are_never_overwritten(self):
        identity = self.connection.execute("SELECT id FROM account_platform_identities").fetchone()[0]
        with transaction(self.connection):
            self.connection.execute("INSERT INTO account_provider_references VALUES(?,'TikHub','sec_user_id',?,NULL,?,?)",
                                    (identity, "MS4wLjAB" + "B" * 64, AT, AT))
        plans, rows = plan_import(self.connection, [self.path])
        self.assertEqual(plans, [])
        self.assertEqual(rows[0]["reason"], "existing_reference_conflict")
        with transaction(self.connection):
            self.connection.execute("DELETE FROM account_provider_references")
            plans, _ = plan_import(self.connection, [self.path])
            apply_import(self.connection, plans, evidence_dir=self.evidence)
        Path(self.connection.execute("SELECT local_path FROM provider_raw_responses").fetchone()[0]).write_text('{}')
        plans, rows = plan_import(self.connection, [self.path])
        self.assertEqual(plans, [])
        self.assertEqual(rows[0]["reason"], "existing_evidence_requires_repair")

    def test_duplicate_source_is_not_duplicate_account_or_raw_evidence(self):
        with transaction(self.connection):
            plans, rows = plan_import(self.connection, [self.path, self.path])
            self.assertEqual(len(plans), 1)
            apply_import(self.connection, plans, evidence_dir=self.evidence)
        self.assertEqual([row["status"] for row in rows], ["imported", "unchanged"])
        self.assertEqual(self.connection.execute("SELECT count(*) FROM provider_raw_responses").fetchone()[0], 1)

    def test_existing_locator_only_gains_its_missing_source(self):
        identity = self.connection.execute("SELECT id FROM account_platform_identities").fetchone()[0]
        with transaction(self.connection):
            self.connection.execute("INSERT INTO account_provider_references VALUES(?,'tikhub','sec_user_id',?,NULL,?,?)",
                                    (identity, SEC, AT, AT))
        old = tuple(self.connection.execute("SELECT * FROM account_provider_references").fetchone())
        with transaction(self.connection):
            plans, _ = plan_import(self.connection, [self.path])
            apply_import(self.connection, plans, evidence_dir=self.evidence)
        current = tuple(self.connection.execute("SELECT * FROM account_provider_references").fetchone())
        self.assertEqual(old[:4] + old[5:], current[:4] + current[5:])
        self.assertIsNone(old[4])
        self.assertIsInstance(current[4], int)
        self.assertEqual(len(derive_capture_eligibility(self.connection)["eligible_members"]), 1)

    def test_orphan_evidence_after_transaction_rollback_is_reusable(self):
        with self.assertRaisesRegex(ValueError, "rollback"), transaction(self.connection):
            plans, _ = plan_import(self.connection, [self.path])
            apply_import(self.connection, plans, evidence_dir=self.evidence)
            raise ValueError("rollback")
        self.assertEqual(self.connection.execute("SELECT count(*) FROM provider_raw_responses").fetchone()[0], 0)
        with transaction(self.connection):
            plans, _ = plan_import(self.connection, [self.path])
            apply_import(self.connection, plans, evidence_dir=self.evidence)
        self.assertEqual(len(derive_capture_eligibility(self.connection)["eligible_members"]), 1)


if __name__ == "__main__":
    unittest.main()
