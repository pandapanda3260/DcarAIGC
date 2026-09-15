"""Direct metric request projection and real raw/media writes; transport is offline.

The paid dispatcher boundary is a fixture here. Its authorizations are covered
by manual-scope tests; parsers, persisted raw replay and media registration run.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from tests import test_v8_source_routing as fixture
from tests.test_v8_kuaishou_adapter import detail_payload
from tests.test_v8_wechat_channels_adapter import UID, OBJECT, response, video
from v8 import capture, media, providers, provider_updates as updates
from v8.storage import initialize_database, transaction


AT = fixture.NOW


class DirectMetricProjectionV23Test(unittest.TestCase):
    def setUp(self):
        self.fx = fixture.SourceRoutingTest()
        with patch.object(fixture, "initialize_database", side_effect=lambda db: initialize_database(db, target_version=23)):
            self.fx.setUp()
        self.addCleanup(self.fx.tearDown)
        self.db = self.fx.connection
        self.root = Path(self.fx.temp.name).resolve()
        self.enterContext(patch("socket.socket.connect", side_effect=AssertionError("network forbidden")))
        for module in ("providers", "provider_updates", "metric_observations", "media", "media_work_queue"):
            self.enterContext(patch(f"v8.{module}.now_utc", return_value=AT))
        self.enterContext(patch.object(media, "MEDIA_ROOT", self.root / "media"))
        self.enterContext(patch.object(providers, "_load_key", return_value="offline-fixture"))
        self.enterContext(patch.object(providers, "_freeze_tikhub_transport", return_value=None))
        self.enterContext(patch.object(providers, "_budget_for_call", return_value="offline-fixture"))
        self.executions = []
        self.fetch = self.enterContext(patch.object(updates, "execute_content_fetch", side_effect=self.execute))
        self.transport = self.enterContext(patch.object(providers, "_request_json", side_effect=self.request))

    def configure(self, platform):
        if platform == "kuaishou":
            self.payload = detail_payload()
            pid, uid = "5234567890123456789", "001234"
            url = "https://www.kuaishou.com/short-video/3xwork"
        else:
            self.payload = response(video(), object_id=OBJECT, object_nonce_id="12345")
            pid, uid = OBJECT, UID
            url = f"https://channels.weixin.qq.com/video/{OBJECT}?object_nonce_id=12345"
        self.platform, self.pid = platform, pid
        with transaction(self.db):
            self.db.execute("UPDATE content_items SET platform=?,platform_content_id=?,canonical_url=?,raw_account_uid=?,title='Keep title',body='Keep body',content_type='video' WHERE id=1", (platform, pid, url, uid))
        self.content_before = dict(self.db.execute("SELECT * FROM content_items WHERE id=1").fetchone())

    def request(self, url, **kwargs):
        if self.platform == "wechat_channels":
            self.assertEqual(kwargs["body"]["object_id"], OBJECT)
            self.assertEqual(kwargs["body"]["object_nonce_id"], "12345")
        return (200, self.payload)

    def seed_raw(self, *, operation, window, data):
        body = json.dumps(self.payload, ensure_ascii=False, separators=(",", ":")).encode()
        with transaction(self.db):
            raw_id = self.fx.raw("TikHub", AT, 1, operation, stage="metrics")
            row = self.db.execute("SELECT fa.id attempt_id,fs.id slot_id FROM provider_raw_responses pr JOIN fetch_attempts fa ON fa.id=pr.fetch_attempt_id JOIN fetch_slots fs ON fs.id=fa.slot_id WHERE pr.id=?", (raw_id,)).fetchone()
            path = self.root / f"raw-{raw_id}.json"
            path.write_bytes(body); path.chmod(0o600)
            self.db.execute("UPDATE fetch_slots SET window_key=? WHERE id=?", (window, row["slot_id"]))
            self.db.execute("UPDATE provider_raw_responses SET local_path=?,sha256=?,byte_size=?,http_status=200 WHERE id=?", (str(path), hashlib.sha256(body).hexdigest(), len(body), raw_id))
        return capture.CaptureOutcome(row["slot_id"], row["attempt_id"], raw_id, data, True, .006, "USD")

    def execute(self, **kwargs):
        self.executions.append(kwargs)
        identity = kwargs["paid_request_identity"].document
        self.assertEqual(identity["subject"], self.pid)
        if self.platform == "wechat_channels":
            self.assertEqual(identity["request_parameters"]["object_nonce_id"], "12345")
        result = kwargs["call"]()
        self.assertIn("_detail_projection", result.data)
        self.assertNotIn("_detail_projection", result.raw_response)
        return self.seed_raw(operation=kwargs["operation"], window=kwargs["window_key"], data=result.data)

    def refresh(self):
        return updates.refresh_content_metrics(1, db_path=self.fx.db, at=AT, cycle_key="offline-cycle")

    def assert_stored_projection(self, raw_id):
        self.assertEqual(dict(self.db.execute("SELECT * FROM content_items WHERE id=1").fetchone()), self.content_before)
        sources = self.db.execute("SELECT * FROM evidence_artifacts WHERE content_id=1 AND artifact_type='media_source'").fetchall()
        self.assertEqual(len(sources), 1)
        self.assertEqual(json.loads(sources[0]["metadata_json"])["raw_response_id"], raw_id)
        observation = self.db.execute("SELECT * FROM content_metric_observations WHERE content_id=1").fetchone()
        self.assertEqual(observation["raw_response_id"], raw_id)
        self.assertEqual(self.db.execute("SELECT count(*) FROM evaluation_versions").fetchone()[0], 0)
        raw = self.db.execute("SELECT * FROM provider_raw_responses WHERE id=?", (raw_id,)).fetchone()
        self.assertEqual(json.loads(Path(raw["local_path"]).read_bytes()), self.payload)

    def test_wechat_live_nonce_reaches_transport_and_paid_identity_and_shared_media(self):
        self.configure("wechat_channels")
        result = self.refresh()
        self.assertEqual(len(self.executions), 1)
        self.assertNotIn("view_count", result["missing_fields"])
        self.assert_stored_projection(result["requests"][0]["raw_response_id"])

    def test_kuaishou_live_metrics_register_media_with_same_raw_without_text_changes(self):
        self.configure("kuaishou")
        result = self.refresh()
        self.assertEqual(result["status"], "succeeded")
        self.assert_stored_projection(result["requests"][0]["raw_response_id"])

    def test_successful_wechat_raw_replay_restores_media_without_second_request(self):
        self.configure("wechat_channels")
        with patch.object(providers, "_store_stage_result", side_effect=RuntimeError("fixture business write failure")):
            with self.assertRaisesRegex(RuntimeError, "fixture business write failure"):
                self.refresh()
        result = self.refresh()
        self.assertEqual(result["provider_cost"], 0)
        self.assertEqual(result["requests"][0]["status"], "replayed")
        self.assertEqual(len(self.executions), 1)
        self.assert_stored_projection(result["requests"][0]["raw_response_id"])

    def test_replay_checks_saved_raw_integrity_before_free_projection(self):
        self.configure("kuaishou")
        with patch.object(providers, "_store_stage_result", side_effect=RuntimeError("fixture business write failure")):
            with self.assertRaises(RuntimeError):
                self.refresh()
        raw = self.db.execute("SELECT local_path FROM provider_raw_responses").fetchone()[0]
        Path(raw).write_bytes(b"changed")
        with self.assertRaises(capture.RawResponseIntegrityError):
            self.refresh()
        self.assertEqual(len(self.executions), 1)
        self.assertEqual(self.db.execute("SELECT count(*) FROM evidence_artifacts").fetchone()[0], 0)

    def test_live_and_replay_reject_wrong_author_with_original_raw(self):
        self.configure("wechat_channels")
        self.payload = response(video(username="v2_other@finder", contact={"username": "v2_other@finder"}))
        for replay in (False, True):
            with self.subTest(replay=replay):
                if replay:
                    self.seed_raw(operation="wechat_channels_video_statistics", window="offline-cycle:video_counts", data={})
                with self.assertRaises(capture.CaptureError) as raised:
                    self.refresh()
                self.assertEqual(raised.exception.error_code, "identity_conflict")
                self.assertEqual(raised.exception.raw_response, self.payload)
        self.assertEqual(len(self.executions), 1)
        self.assertEqual(self.db.execute("SELECT count(*) FROM content_metric_observations").fetchone()[0], 0)

    def test_detail_backed_metric_projection_keeps_transport_and_new_platform_media(self):
        for platform, pid, uid, payload in (("kuaishou", "5234567890123456789", "001234", detail_payload()),
                ("wechat_channels", OBJECT, UID, response(video()))):
            with self.subTest(platform=platform):
                parsed = providers._parse_content_payload(platform, "detail", pid, "video", payload, expected_uid=uid)
                entity, receipt = b"fixture entity bytes", {"fixture": "transport receipt"}
                parsed = capture.ProviderResult(parsed.data, parsed.raw_response, 200, True,
                    entity_bytes=entity, transport_receipt=receipt)
                result = updates._metric_only_result(parsed, platform=platform, source_stage="detail", expected_uid=uid)
                self.assertEqual(result.data["_detail_projection"], parsed.data)
                self.assertNotIn("title", result.data)
                self.assertNotIn("content_type", result.data)
                self.assertEqual((result.entity_bytes, result.transport_receipt), (entity, receipt))
                self.assertEqual(result.raw_response, payload)


if __name__ == "__main__":
    unittest.main()
