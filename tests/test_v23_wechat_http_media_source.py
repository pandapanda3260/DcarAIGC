"""Verified Channels HTTP source recovery; all writes stay in schema23 fixtures."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tests.roster_fixture import accept_roster
from tests.test_v8_wechat_channels_adapter import UID, OBJECT, video
from v8 import capture, media, paid_dispatch, pipeline, providers, storage
from v8.paid_drain import dispatch_state
from v8.runtime_database import (
    DatabaseAccessMode, FileIdentity, InstalledWriterContract,
    ResolvedDatabaseAccess, acquire_writer_lock,
)
from v8.wechat_video_crypto import WeChatDecryptionError, media_material

AT = "2026-09-13T04:47:51Z"
BASE = "http://wxapp.tc.qq.com/251/20302/stodownload?encfilekey=fixture%2B%2F%3D"
TOKEN = "&token=abc+//==&part=1&part=2"


class WeChatHttpMediaSourceV23Test(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.db = self.root / "fixture.db"
        self.enterContext(patch("socket.socket.connect", side_effect=AssertionError("network forbidden")))
        self.enterContext(patch.object(media.urllib.request, "urlopen", side_effect=AssertionError("download forbidden")))
        self.enterContext(patch.object(providers, "_budget_for_call", side_effect=AssertionError("purchase forbidden")))
        self.enterContext(patch.object(capture, "execute_content_fetch", side_effect=AssertionError("provider forbidden")))
        self.enterContext(patch.object(media, "MEDIA_ROOT", self.root / "media"))
        with storage.connect(self.db) as connection:
            storage.initialize_database(connection, target_version=23)
            connection.execute("INSERT INTO accounts(id,phone,enabled,created_at,updated_at) VALUES(1,'',1,?,?)", (AT, AT))
            connection.execute("INSERT INTO account_platform_identities(account_id,platform,uid,nickname,source,created_at,updated_at) VALUES(1,'wechat_channels',?,'fixture','manual',?,?)", (UID, AT, AT))
            connection.execute("INSERT INTO content_items(id,account_id,link_id,platform,platform_content_id,canonical_url,title,content_type,raw_account_uid,published_at,created_at,updated_at,imported_at) VALUES(1,1,'A2BC3D','wechat_channels',?,'','fixture','video',?,?,?,?,?)", (OBJECT, UID, AT, AT, AT, AT))
        lock = self.root / "writer.lock"
        lock.touch(mode=0o600)
        installed = InstalledWriterContract(self.root, self.root / "fixture.plist", self.root,
            self.root / "fixture.py", self.db, lock, {})
        self.enterContext(acquire_writer_lock(ResolvedDatabaseAccess(DatabaseAccessMode.WRITER, self.db,
            FileIdentity.from_stat(self.db.stat()), self.root, lock, installed)))

    def store_raw(self, payload=None, *, entity_bytes=None, object_id=OBJECT, uid=UID):
        """Create the real singleton ownership shape, without sending anything.

        An external, read-only audit can supply previously verified entity bytes;
        the committed tests use synthetic identifiers and no private raw fixture.
        """
        if payload is None:
            item = video()
            item["objectDesc"]["media"][0].update(url=BASE, urlToken=TOKEN)
            payload = {"code": 200, "data": {"objects": [item]}}
        body = entity_bytes if entity_bytes is not None else json.dumps(payload).encode()
        self.raw_path = self.root / "detail.json"
        self.raw_path.write_bytes(body)
        self.raw_path.chmod(0o600)
        with storage.connect(self.db) as connection:
            connection.execute("UPDATE content_items SET platform_content_id=?,raw_account_uid=? WHERE id=1", (object_id, uid))
            connection.execute("UPDATE account_platform_identities SET uid=? WHERE account_id=1", (uid,))
            accept_roster(connection, accepted_at=AT)
            slot = connection.execute("INSERT INTO fetch_slots(content_id,stage,window_key,provider,adapter_version,status,attempt_count,created_at,updated_at) VALUES(1,'detail','lifetime','TikHub','fixture','succeeded',1,?,?)", (AT, AT)).lastrowid
            scope = hashlib.sha256(body).hexdigest()
            batch = connection.execute("INSERT INTO fetch_request_batches(request_scope_identity,sequence,provider,operation,parameters_json,created_at) VALUES(?,0,'tikhub','wechat_channels_video_detail','{}',?)", (scope, AT)).lastrowid
            connection.execute("INSERT INTO fetch_request_batch_members(batch_id,member_scope_identity,sequence,content_id,account_id) VALUES(?,?,0,1,1)", (batch, scope))
            attempt = connection.execute("INSERT INTO fetch_attempts(slot_id,request_batch_id,attempt_number,request_started_at,response_finished_at,http_status,billed,amount,currency) VALUES(NULL,?,1,?,?,200,1,.01,'USD')", (batch, AT, AT)).lastrowid
            usage = connection.execute("INSERT INTO provider_usage(task_id,provider,operation,request_attempts,billed_requests,amount,currency,recorded_at) VALUES('fixture','TikHub','wechat_channels_video_detail',1,1,.01,'USD',?)", (AT,)).lastrowid
            raw_id = connection.execute("INSERT INTO provider_raw_responses(fetch_attempt_id,content_id,provider,operation,local_path,sha256,byte_size,http_status,captured_at,paid_scope_identity,sequence) VALUES(?,1,'TikHub','wechat_channels_video_detail',?,?,?,200,?,?,0)", (attempt, str(self.raw_path), hashlib.sha256(body).hexdigest(), len(body), AT, scope)).lastrowid
            run = connection.execute("INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,details_json) VALUES('capture_integrated_work',?,'running',?,'{}')", (AT, AT)).lastrowid
            run_attempt = connection.execute("INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,invocation_source,status,started_at,details_json) VALUES(?,1,'scheduled','running',?,'{}')", (run, AT)).lastrowid
            dispatch = paid_dispatch.reserve_dispatch_in_transaction(connection, provider="TikHub",
                operation="wechat_channels_video_detail", activation_id=dispatch_state(connection, at=AT).activation_id,
                business_day="2026-09-13", scheduler_run_id=run, scheduler_attempt_id=run_attempt,
                scope={"content_id": 1}, provider_usage_id=usage, fetch_slot_id=slot, created_at=AT)
            paid_dispatch.mark_dispatch_sent_in_transaction(connection, dispatch.dispatch_id,
                fetch_attempt_id=attempt, created_at=AT)
            paid_dispatch.finish_dispatch_in_transaction(connection, dispatch.dispatch_id,
                outcome="succeeded", raw_response_id=raw_id, created_at=AT)
            self.assertIsNone(connection.execute("SELECT slot_id FROM fetch_attempts WHERE id=?", (attempt,)).fetchone()[0])
            self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])
        return payload, raw_id

    def content(self):
        with storage.connect(self.db) as connection:
            return dict(connection.execute("SELECT * FROM content_items WHERE id=1").fetchone())

    def paid_counts(self):
        with storage.connect(self.db) as connection:
            return tuple(connection.execute(sql).fetchone()[0] for sql in (
                "SELECT count(*) FROM fetch_attempts", "SELECT count(*) FROM provider_raw_responses",
                "SELECT count(*) FROM paid_provider_dispatch_events WHERE event_type='send_marked'",
                "SELECT sum(request_attempts) FROM provider_usage", "SELECT sum(amount) FROM provider_usage"))

    def test_http_signed_query_survives_parser_registration_and_selection_exactly(self):
        payload, raw_id = self.store_raw()
        parsed = providers._parse_content_payload("wechat_channels", "detail", OBJECT, "video",
            payload, expected_uid=UID).data
        material = media_material(parsed)
        self.assertEqual(material["url"], BASE + TOKEN)
        self.assertEqual(media._normalize_media_url(material["url"]), material["url"])
        before = self.paid_counts()
        source = media.store_media_source_from_detail(1, parsed, raw_response_id=raw_id, db_path=self.db)
        self.assertIsNotNone(source)
        state = media.get_media_source_state(1, db_path=self.db)
        self.assertEqual(state["raw_response_id"], raw_id)
        with storage.connect(self.db) as connection:
            row = connection.execute("SELECT * FROM evidence_artifacts WHERE id=?", (source.id,)).fetchone()
            urls, _ = media._validated_video_media_source(row)
            self.assertEqual(urls, [BASE + TOKEN])
            self.assertEqual(json.loads(row["metadata_json"])["raw_response_id"], raw_id)
        again = media.store_media_source_from_detail(1, parsed, raw_response_id=raw_id, db_path=self.db)
        self.assertEqual(source.id, again.id)
        self.assertEqual(self.paid_counts(), before)

    def test_http_scope_rejects_other_hosts_ports_paths_and_url_rewriting(self):
        rejected = [
            "http://finder.video.qq.com/1/2/stodownload",
            "http://other.tc.qq.com/1/2/stodownload", "http://127.0.0.1/1/2/stodownload",
            "http://10.0.0.1/1/2/stodownload", "http://[::1]/1/2/stodownload",
            "http://wxapp.tc.qq.com.evil.test/1/2/stodownload",
            "http://evilwxapp.tc.qq.com/1/2/stodownload", "http://wxapp.tc.qq.com./1/2/stodownload",
            "http://u:p@wxapp.tc.qq.com/1/2/stodownload",
            "http://wxapp.tc.qq.com@127.0.0.1/1/2/stodownload",
            *[f"http://wxapp.tc.qq.com:{port}/1/2/stodownload" for port in (80, 443, 8080, "abc", 65536)],
            *["http://wxapp.tc.qq.com" + path for path in (
                "/1/2/other", "/1/2/stodownload/", "//1/2/stodownload", "/%31/2/stodownload",
                "/１/2/stodownload", "/1/2/../stodownload", "/1/2/stodownload;other")],
            BASE + "#fragment", BASE + "#", BASE + "\u00a0",
            "http://wxapp.tc.qq.com/1/2/stodownload?",
            " " + BASE, BASE + " ", BASE.replace("wxapp", "WXAPP"),
            BASE.replace("http:", "HTTP:"), BASE.replace("wxapp", "wx\tapp"),
            BASE + "\r", BASE + "\n", BASE + "\\", BASE + "\x00", BASE + "\x7f",
        ]
        for value in rejected:
            with self.subTest(value=value):
                self.assertFalse(media.is_supported_media_url(value))
                self.assertIsNone(media._normalize_media_url(value))
        for value in ("https://finder.video.qq.com/v", "http://v.kwaicdn.com/v",
                      "http://v.xhscdn.com/v", "http://v.rednotecdn.com/v"):
            self.assertTrue(media.is_supported_media_url(value))
            self.assertEqual(media._normalize_media_url(value), value)

    def test_source_missing_recovers_from_existing_singleton_raw_without_new_purchase(self):
        _, raw_id = self.store_raw()
        before = self.paid_counts()
        result = pipeline._reuse_captured_media_source(self.content(), db_path=self.db)
        self.assertEqual((result["status"], result["raw_response_id"], result["provider_calls"]),
            ("reused", raw_id, 0))
        again = pipeline._reuse_captured_media_source(self.content(), db_path=self.db)
        self.assertEqual((again["status"], again["provider_calls"]), ("unchanged", 0))
        self.assertEqual(self.paid_counts(), before)

    def test_untrusted_author_or_object_cannot_register_the_http_source(self):
        payload, raw_id = self.store_raw()
        parsed = providers._parse_content_payload("wechat_channels", "detail", OBJECT, "video",
            payload, expected_uid=UID).data
        for column, bad in (("raw_account_uid", "v2_0011aabc@finder"), ("platform_content_id", "123")):
            with self.subTest(column=column):
                old = self.content()[column]
                with storage.connect(self.db) as connection:
                    connection.execute(f"UPDATE content_items SET {column}=? WHERE id=1", (bad,))
                with self.assertRaises(capture.CaptureError):
                    media.store_media_source_from_detail(1, parsed, raw_response_id=raw_id, db_path=self.db)
                with storage.connect(self.db) as connection:
                    self.assertEqual(connection.execute("SELECT count(*) FROM evidence_artifacts").fetchone()[0], 0)
                    connection.execute(f"UPDATE content_items SET {column}=? WHERE id=1", (old,))

    def test_raw_corruption_and_cross_response_material_never_become_a_source(self):
        payload, raw_id = self.store_raw()
        parsed = providers._parse_content_payload("wechat_channels", "detail", OBJECT, "video",
            payload, expected_uid=UID).data
        for change in ({"full_url": BASE + "&other=1"}, {"decode_key": None},
                       {"decode_key": "-1"}, {"decode_key": "18446744073709551616"}):
            changed = deepcopy(parsed)
            changed["media_evidence"][0].update(change)
            with self.assertRaises(WeChatDecryptionError):
                media_material(changed)
        self.raw_path.write_bytes(b"{}")
        with self.assertRaises(capture.RawResponseIntegrityError):
            media.store_media_source_from_detail(1, parsed, raw_response_id=raw_id, db_path=self.db)
        with storage.connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM evidence_artifacts").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
