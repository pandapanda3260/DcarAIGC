from __future__ import annotations

from datetime import date
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests.test_v8_providers import upsert_account
from v8 import account_metrics, capture, capture_runtime, providers
from v8.account_reference_storage import store_reference
from v8.storage import connect, initialize_database

UID = "99887766"
SEC = "MS4wLjAB" + "A" * 68
AT = "2026-09-12T02:30:00Z"


class ReferenceStorageContractTest(unittest.TestCase):
    def test_schema21_and22_keep_upsert_history_and_reject_wrong_platform(self):
        for version in (21, 22):
            with self.subTest(version=version), sqlite3.connect(":memory:") as connection:
                connection.execute("CREATE TABLE account_platform_identities(id INTEGER PRIMARY KEY,platform TEXT)")
                connection.execute("INSERT INTO account_platform_identities VALUES(1,'douyin')")
                connection.execute("CREATE TABLE account_provider_references(account_identity_id INTEGER,provider TEXT,"
                    "reference_kind TEXT,reference_value TEXT,source_raw_response_id INTEGER,created_at TEXT,updated_at TEXT,"
                    + ("platform TEXT NOT NULL," if version == 22 else "")
                    + "PRIMARY KEY(account_identity_id,provider,reference_kind))")
                fields = dict(account_identity_id=1,platform="douyin",provider="TikHub",reference_kind="sec_user_id",
                    reference_value=SEC,source_raw_response_id=1,created_at="original",updated_at="original")
                store_reference(connection, **fields)
                store_reference(connection, **{**fields,"source_raw_response_id":2,"created_at":"later","updated_at":"later"}, update_existing=True)
                self.assertEqual(connection.execute("SELECT count(*),source_raw_response_id,created_at,updated_at FROM account_provider_references").fetchone(),(1,2,"original","later"))
                if version == 22:
                    self.assertEqual(connection.execute("SELECT platform FROM account_provider_references").fetchone()[0],"douyin")
                with self.assertRaisesRegex(ValueError,"bound identity"):
                    store_reference(connection, **{**fields,"platform":"xiaohongshu"}, update_existing=True)


class Schema22ReferenceWriterIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / "references.sqlite3"
        self.enterContext(patch.object(capture,"RAW_ROOT",self.root / "raw"))
        with connect(self.db) as connection:
            initialize_database(connection,target_version=22)
        self.account = upsert_account({"phone":"","platforms":[{"platform":"douyin","uid":UID}]},db_path=self.db)
        with connect(self.db) as connection:
            self.iid = connection.execute("SELECT id FROM account_platform_identities WHERE account_id=?",(self.account["id"],)).fetchone()[0]

    def profile(self):
        return {"code":200,"data":{"status_code":0,"data":{"uid":UID,"id_str":UID,"sec_user_id":SEC,
            "nickname":"fixture","follow_info":{"follower_count":123}}}}

    def saved_raw(self, operation, payload):
        body = json.dumps(payload).encode()
        path = self.root / (operation + ".json"); path.write_bytes(body)
        with connect(self.db) as connection:
            slot_id = connection.execute("INSERT INTO fetch_slots(account_id,stage,window_key,provider,adapter_version,status,created_at,updated_at) VALUES(?,'discovery',?,'TikHub','fixture','succeeded',?,?)",
                (self.account["id"],operation,AT,AT)).lastrowid
            attempt_id = connection.execute("INSERT INTO fetch_attempts(slot_id,attempt_number,request_started_at,http_status) VALUES(?,1,?,200)",(slot_id,AT)).lastrowid
            raw_id = connection.execute("INSERT INTO provider_raw_responses(fetch_attempt_id,account_id,provider,operation,local_path,sha256,byte_size,http_status,captured_at) VALUES(?,?,'TikHub',?,?,?,?,200,?)",
                (attempt_id,self.account["id"],operation,str(path),hashlib.sha256(body).hexdigest(),len(body),AT)).lastrowid
            connection.commit()
        return SimpleNamespace(raw_response_id=raw_id,value=payload,http_status=200,captured_at=AT,slot_id=slot_id)

    def test_recurring_profile_writes_schema22_reference_and_replays_without_call(self):
        envelope = {"platform":"douyin","uid":UID,"account_id":self.account["id"],"identity_id":self.iid,
                    "logical_due":"account-metrics:2026-09-12T00:00:00Z"}
        raw = self.saved_raw("douyin_uid_profile",self.profile())
        with patch.object(capture,"load_succeeded_raw_response",return_value=raw), patch.object(capture,"execute_account_fetch",side_effect=AssertionError("saved profile must replay")) as caller:
            first = capture_runtime._account_request(envelope,db_path=self.db,at=AT)
            replay = capture_runtime._account_request(envelope,db_path=self.db,at=AT)
        self.assertTrue(first["complete"])
        self.assertEqual(caller.call_count,0)
        self.assertEqual(replay["provider_cost"],0)
        with connect(self.db) as connection:
            ref = connection.execute("SELECT platform,reference_value,source_raw_response_id FROM account_provider_references WHERE account_identity_id=? AND reference_kind='sec_user_id'",(self.iid,)).fetchone()
            self.assertEqual(tuple(ref[:2]),("douyin",SEC))
            self.assertEqual(connection.execute("SELECT count(*) FROM account_metric_observations").fetchone()[0],1)
            self.assertEqual(account_metrics.select_account_metrics(connection,[self.iid])[self.iid]["follower_count"],123)

    def test_new_lowercase_intake_reference_survives_legacy_mixed_case_refresh(self):
        with connect(self.db) as connection:
            store_reference(connection,account_identity_id=self.iid,platform="douyin",provider="tikhub",
                reference_kind="sec_user_id",reference_value=SEC,source_raw_response_id=10,
                created_at="original",updated_at="original")
            store_reference(connection,account_identity_id=self.iid,platform="douyin",provider="TikHub",
                reference_kind="sec_user_id",reference_value=SEC,source_raw_response_id=11,
                created_at=AT,updated_at=AT,update_existing=True)
            rows = connection.execute("SELECT provider,source_raw_response_id,created_at FROM account_provider_references WHERE account_identity_id=? AND reference_kind='sec_user_id'",(self.iid,)).fetchall()
            self.assertEqual([tuple(row) for row in rows],[("tikhub",11,"original")])

    def test_legacy_discovery_restores_schema22_reference_from_saved_raw(self):
        raws = {"douyin_uid_profile": self.saved_raw("douyin_uid_profile",self.profile()),
                "douyin_user_posts": self.saved_raw("douyin_user_posts",{"data":{"aweme_list":[],"has_more":0}})}
        def saved(**kwargs):
            return raws[kwargs["operation"]]
        with patch.object(providers,"execute_account_fetch",side_effect=capture.SlotUnavailable("saved response")), patch.object(providers,"load_succeeded_raw_response",side_effect=saved):
            first = providers.discover_account_content(self.account["id"],"douyin",UID,
                as_of=date(2026,9,12),db_path=self.db,call_override=lambda *_:self.fail("saved raw must replay"))
            second = providers.discover_account_content(self.account["id"],"douyin",UID,
                as_of=date(2026,9,12),db_path=self.db,call_override=lambda *_:self.fail("saved raw must replay"))
        self.assertEqual(first["reference_status"],"replayed")
        self.assertEqual(second["reference_status"],"cached")
        with connect(self.db) as connection:
            rows = connection.execute("SELECT platform,reference_value FROM account_provider_references WHERE account_identity_id=? AND reference_kind='sec_user_id'",(self.iid,)).fetchall()
            self.assertEqual([tuple(row) for row in rows],[("douyin",SEC)])


if __name__ == "__main__":
    unittest.main()
