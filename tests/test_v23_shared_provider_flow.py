"""Shared raw consumers, strict authors and field-complete queued work."""
from __future__ import annotations
import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from tests import test_v8_source_routing as fixture
from tests.test_v8_kuaishou_adapter import detail_payload
from tests.test_v8_wechat_channels_adapter import UID, OBJECT, response, video
from v8 import capture, capture_planning as planning, providers, capture_runtime as runtime
from v8.storage import initialize_database, transaction


class SharedProviderFlowV23Test(unittest.TestCase):
    def setUp(self):
        self.fx = fixture.SourceRoutingTest()
        with patch.object(fixture, "initialize_database", side_effect=lambda c: initialize_database(c, target_version=23)):
            self.fx.setUp()
        self.addCleanup(self.fx.tearDown)
        self.c = self.fx.connection
        self.enterContext(patch("socket.socket.connect", side_effect=AssertionError("network forbidden")))

    def observe(self, platform, operation, values):
        with transaction(self.c):
            self.c.execute("UPDATE content_items SET platform=? WHERE id=1", (platform,))
        original = self.fx.raw
        with patch.object(self.fx, "raw", side_effect=lambda *args: original(*args, stage="metrics")):
            self.fx.observe("TikHub", operation=operation, values=values)
        with transaction(self.c):
            for raw in self.c.execute("SELECT id FROM provider_raw_responses").fetchall():
                body = json.dumps({"fixture": "verified captured counter bytes", "id": raw[0]}).encode()
                path = Path(self.fx.temp.name).resolve() / f"raw-{raw[0]}.json"
                path.write_bytes(body); path.chmod(0o600)
                self.c.execute("UPDATE provider_raw_responses SET local_path=?,sha256=?,byte_size=? WHERE id=?",
                    (str(path), hashlib.sha256(body).hexdigest(), len(body), raw[0]))

    def envelope(self, platform, group):
        return {"stage":"metrics", "platform":platform, "content_id":1,
                "source_stage":"metrics", "logical_due":"metrics:2026-08-29T00:00:00Z:"+group}

    def queued_detail_after_metrics(self, platform, *, created_at="2026-08-29T02:00:00Z", kind="content_detail"):
        """Persist one physical metrics response, then leave its detail consumer queued."""
        self.assertEqual(self.c.execute("PRAGMA user_version").fetchone()[0], 23)
        if platform == "kuaishou":
            content_id, identifier, uid = 1, "5234567890123456789", "001234"
            url, payload = "https://www.kuaishou.com/short-video/3xwork", detail_payload()
        else:
            content_id, identifier, uid = 2, OBJECT, UID
            url = f"https://channels.weixin.qq.com/video/{OBJECT}?object_nonce_id=12345"
            payload = response(video())
        operation = providers.STAGE_CONFIG[(platform, "detail")][2]
        metrics_operation = providers.STAGE_CONFIG[(platform, "metrics")][2]
        env = {"kind": kind, "stage": "detail", "source_stage": "detail", "platform": platform,
               "content_id": content_id, "operation": operation, "uid": uid,
               "logical_due": "detail:2026-08-29", "cursor": None, "seen_cursors": [],
               "page_count": 0, "raw_ids": []}
        identity = planning.digest({"provider": "tikhub", "operation": operation,
            "subject": f"content:{content_id}", "logical_due": env["logical_due"]})
        with transaction(self.c):
            self.c.execute("UPDATE content_items SET platform=?,platform_content_id=?,canonical_url=?,"
                "raw_account_uid=?,content_type='video',title='',body='' WHERE id=?",
                (platform, identifier, url, uid, content_id))
            assignment = self.c.execute("INSERT INTO capture_route_assignments("
                "scope_type,scope_key,provider,operation,content_id,generation,route,mode,"
                "effective_at,recorded_at,assignment_sha256) "
                "VALUES ('content',?,'tikhub',?,?,1,'integrated','active',?,?,?)",
                (str(content_id), operation, content_id, created_at, created_at, identity)).lastrowid
            self.c.execute("INSERT INTO capture_work_items(work_identity,assignment_id,content_id,"
                "provider,operation,due_at,data_business_day,state,envelope_json,created_at,updated_at) "
                "VALUES (?,?,?,'tikhub',?,?,'2026-08-29','runnable',?,?,?)",
                (identity, assignment, content_id, operation, created_at, json.dumps(env), created_at, created_at))
            raw_id = self.fx.raw("TikHub", fixture.CAPTURE, content_id, metrics_operation, stage="metrics")
            body = json.dumps(payload, ensure_ascii=False).encode()
            path = Path(self.fx.temp.name).resolve() / f"physical-metrics-{raw_id}.json"
            path.write_bytes(body)
            path.chmod(0o600)
            self.c.execute("UPDATE provider_raw_responses SET local_path=?,sha256=?,byte_size=?,http_status=200 WHERE id=?",
                (str(path), hashlib.sha256(body).hexdigest(), len(body), raw_id))
            self.c.execute("UPDATE fetch_attempts SET billed=1 WHERE id=("
                "SELECT fetch_attempt_id FROM provider_raw_responses WHERE id=?)", (raw_id,))
        content = dict(self.c.execute("SELECT * FROM content_items WHERE id=?", (content_id,)).fetchone())
        parsed = providers._parse_content_payload(platform, "metrics", identifier, "video", payload, expected_uid=uid)
        outcome = capture.CaptureOutcome(0, 0, raw_id, parsed.data, True, 0.01, "USD")
        providers._store_stage_result(content, "metrics", "metrics:2026-08-29", outcome,
            db_path=self.fx.db, media_root=Path(self.fx.temp.name) / "media")
        return env, content, raw_id

    def test_metrics_first_detail_reuses_one_physical_response_without_purchase(self):
        store_result = providers._store_stage_result
        def store_in_fixture(*args, **kwargs):
            return store_result(*args, **kwargs, media_root=Path(self.fx.temp.name) / "media")
        for platform in ("kuaishou", "wechat_channels"):
            with self.subTest(platform=platform):
                env, content, raw_id = self.queued_detail_after_metrics(platform)
                before = tuple(self.c.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]
                    for table in ("fetch_attempts", "provider_raw_responses"))
                self.assertEqual(self.c.execute("SELECT title FROM content_items WHERE id=?", (content["id"],)).fetchone()[0], "")
                with patch.object(providers, "_budget_for_call", side_effect=AssertionError("second purchase forbidden")), \
                     patch.object(capture, "execute_content_fetch", side_effect=AssertionError("second fetch forbidden")), \
                     patch.object(providers, "_store_stage_result", side_effect=store_in_fixture):
                    result = runtime._content_request(env, db_path=self.fx.db, at=fixture.NOW)
                self.assertTrue(result["complete"])
                self.assertEqual(result["provider_cost"], 0.0)
                self.assertEqual(result["evidence"], {"raw_response_ids": [raw_id], "all_raw_verified": True,
                    "completion_kind": "shared_detail_response"})
                self.assertEqual(tuple(self.c.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]
                    for table in ("fetch_attempts", "provider_raw_responses")), before)
                self.assertTrue(self.c.execute("SELECT title FROM content_items WHERE id=?", (content["id"],)).fetchone()[0])
                self.assertEqual(self.c.execute("SELECT source,captured_at FROM provider_raw_responses WHERE id=?",
                    (raw_id,)).fetchone()[:], ("live_applied", fixture.CAPTURE))

    def test_detail_does_not_reuse_raw_older_than_queued_work(self):
        for platform in ("kuaishou", "wechat_channels"):
            with self.subTest(platform=platform):
                env, content, _raw_id = self.queued_detail_after_metrics(platform, created_at="2026-08-29T03:00:01Z")
                with patch.object(capture, "_read_verified_raw_response", side_effect=AssertionError("old raw must be excluded")):
                    self.assertIsNone(runtime._shared_detail_outcome(self.c, env, content))
                with patch.object(providers, "_budget_for_call", side_effect=RuntimeError("normal paid path reached")), \
                     self.assertRaisesRegex(RuntimeError, "normal paid path reached"):
                    runtime._content_request(env, db_path=self.fx.db, at=fixture.NOW)

    def test_explicit_media_refresh_does_not_reuse_recent_metrics_response(self):
        for platform in ("kuaishou", "wechat_channels"):
            with self.subTest(platform=platform):
                env, content, _raw_id = self.queued_detail_after_metrics(platform, kind="media_source_refresh")
                with patch.object(capture, "_read_verified_raw_response", side_effect=AssertionError("refresh must bypass reuse")):
                    self.assertIsNone(runtime._shared_detail_outcome(self.c, env, content))
                with patch.object(providers, "_budget_for_call", side_effect=RuntimeError("normal paid path reached")), \
                     self.assertRaisesRegex(RuntimeError, "normal paid path reached"):
                    runtime._content_request(env, db_path=self.fx.db, at=fixture.NOW)

    def test_fresh_kuaishou_fields_finish_queued_work_without_second_call(self):
        self.observe("kuaishou", "kuaishou_video_statistics", dict(view_count=0,like_count=0,comment_count=2,share_count=3,collect_count=4))
        rules = runtime.load_policy()["metric_supplement_groups"]["kuaishou"]
        self.assertEqual(len(rules), 1)
        result = runtime._fresh_metric_work_result(self.c, self.envelope("kuaishou", rules[0]["name"]), at=fixture.NOW)
        self.assertTrue(result["complete"])
        self.assertEqual(result["provider_cost"], 0)
        self.assertTrue(result["evidence"]["all_raw_verified"])
        self.assertTrue(result["evidence"]["raw_response_ids"])

    def test_wechat_missing_vv_is_not_a_second_purchase_but_stale_fields_are(self):
        self.observe("wechat_channels", "wechat_channels_video_statistics", dict(view_count=None,like_count=0,comment_count=2,share_count=3,collect_count=4))
        rule = runtime.load_policy()["metric_supplement_groups"]["wechat_channels"][0]
        env = self.envelope("wechat_channels", rule["name"])
        self.assertTrue(runtime._fresh_metric_work_result(self.c, env, at=fixture.NOW)["complete"])
        self.assertIsNone(runtime._fresh_metric_work_result(self.c, env, at="2026-09-12T04:00:00Z"))

    def test_raw_tamper_blocks_free_completion(self):
        self.observe("kuaishou", "kuaishou_video_statistics", dict(view_count=0,like_count=0,comment_count=2,share_count=3,collect_count=4))
        path = self.c.execute("SELECT local_path FROM provider_raw_responses LIMIT 1").fetchone()[0]
        Path(path).write_text("changed")
        rule = runtime.load_policy()["metric_supplement_groups"]["kuaishou"][0]
        with self.assertRaises(Exception):
            runtime._fresh_metric_work_result(self.c, self.envelope("kuaishou", rule["name"]), at=fixture.NOW)

    def test_old_platform_detail_author_conflicts_are_rejected(self):
        dy = {"code":200, "data":{"aweme_detail":{"aweme_id":"123456789", "author":{"uid":"wrong"}}}}
        xhs = {"code":200,"data":{"data":{"note_list":[{"id":"a"*24,"user":{"user_id":"wrong"},"type":"normal"}]}}}
        for platform, identifier, payload in (("douyin","123456789",dy),("xiaohongshu","a"*24,xhs)):
            with self.subTest(platform=platform), self.assertRaises(providers.CaptureError) as caught:
                providers._parse_content_payload(platform,"detail",identifier,"video",payload,expected_uid="expected")
            self.assertEqual(caught.exception.error_code,"identity_conflict")

    def test_wechat_nonce_and_same_detail_response_survive_metric_projection(self):
        content = {"platform":"wechat_channels","platform_content_id":OBJECT,
                   "canonical_url":f"https://channels.weixin.qq.com/video/{OBJECT}?object_nonce_id=12345"}
        subject = providers._content_subject(content)
        self.assertEqual(providers._content_request_params("wechat_channels","detail",subject,"video")["object_nonce_id"],"12345")
        parsed = providers._parse_content_payload("wechat_channels","metrics",OBJECT,"video",response(video()),expected_uid=UID)
        self.assertEqual(parsed.data["like_count"],12)
        self.assertEqual(parsed.data["_detail_projection"]["platform_content_id"],OBJECT)
        self.assertNotIn("_detail_projection", parsed.raw_response)
