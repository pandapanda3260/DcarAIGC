"""Four-platform dispatch, persisted evidence, replay and pagination without network."""
from __future__ import annotations

import hashlib
import json
import socket
import unittest
from datetime import date
from unittest.mock import patch

from tests import test_v8_providers as base
from tests.test_v8_wechat_channels_adapter import UID, OBJECT, response, video
from tests.test_v8_kuaishou_adapter import profile_payload, discovery_payload, detail_payload
from v8 import providers, capture_runtime as runtime, tikhub_scan, account_metrics
from v8.capture import CaptureError
from v8.storage import connect


def page_payload(more=False, cursor=""):
    return response({"baseResponse": {"ret": 0}, "contact": {"username": UID},
                     "object": [video()], "continueFlag": int(more), "upContinueFlag": 0, "lastBuffer": cursor})


class NewPlatformDispatchTest(unittest.TestCase):
    def test_post_transport_preserves_json_and_original_entity_receipt(self):
        payload = page_payload()
        transport = base.transport_result(200, payload)
        with patch.object(providers, "request_json_transport", return_value=transport) as sender:
            result = providers._extra_call("wechat_channels", "discovery", UID, "fixture", cursor="ab+//==")
        request = sender.call_args.args[0]
        self.assertEqual(request.method, "POST")
        self.assertTrue(request.full_url.endswith("/api/v1/wechat_channels/v2/fetch_user_videos"))
        self.assertEqual(json.loads(request.data), {"raw": True, "username": UID, "last_buffer": "ab+//=="})
        self.assertEqual(result.entity_bytes, transport.entity_body)
        self.assertEqual(result.transport_receipt, transport.receipt)
        self.assertEqual(result.data["items"][0]["platform_content_id"], OBJECT)

    def test_wrong_author_response_carries_original_transport_evidence(self):
        payload = page_payload()
        payload["data"]["object"][0]["username"] = "v2_deadbeef@finder"
        transport = base.transport_result(200, payload)
        with patch.object(providers, "request_json_transport", return_value=transport):
            with self.assertRaises(CaptureError) as raised:
                providers._extra_call("wechat_channels", "discovery", UID, "fixture")
        self.assertEqual(raised.exception.error_code, "identity_conflict")
        self.assertEqual(raised.exception.entity_bytes, transport.entity_body)
        self.assertEqual(raised.exception.transport_receipt, transport.receipt)

    def test_scan_runtime_parses_bound_identity_and_opaque_cursor(self):
        payload = page_payload(True, "ab+//==")
        items, more, cursor, total = tikhub_scan._page_payload(payload, "wechat_channels", expected_uid=UID)
        self.assertTrue(more)
        self.assertEqual(cursor, "ab+//==")
        proof = tikhub_scan._item_evidence("wechat_channels", items[0])
        self.assertIsNotNone(proof["event_tuple"])
        self.assertEqual(proof["platform_content_id"], OBJECT)
        with self.assertRaises(tikhub_scan.TikHubScanError):
            tikhub_scan._page_payload(payload, "wechat_channels")

    def test_profiles_keep_explicit_zero_invalid_and_contract_boundaries(self):
        ks = account_metrics.parse_tikhub_profile(profile_payload(), platform="kuaishou", uid="001234")
        self.assertEqual(ks["metrics"]["follower_count"], 0)
        self.assertEqual(ks["metrics"]["platform_work_count"], 12)
        self.assertEqual(ks["metrics"]["total_likes"], 30)
        for raw, status in ((None, "missing"), (0, "invalid"), ("1.2万", "invalid"), (False, "invalid")):
            wx = account_metrics.parse_tikhub_profile(response({"contact": {"username": UID, "nickname": "测试"}, "fansCount": raw}), platform="wechat_channels", uid=UID)
            self.assertEqual(wx["field_status"]["follower_count"]["status"], status)
            self.assertIsNone(wx["metrics"]["platform_work_count"])
        self.assertNotIn(("kuaishou", "comments"), providers.STAGE_CONFIG)
        self.assertEqual(providers.STAGE_CONFIG[("wechat_channels", "comments")][2], "wechat_channels_video_comments")


class NewPlatformDatabaseTest(unittest.TestCase):
    def setUp(self):
        self.base = base.V8ProviderUpdateTest()
        self.base.setUp()
        self.addCleanup(self.base.doCleanups)
        self.addCleanup(self.base.tearDown)
        self.db = self.base.db
        self.enterContext(patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden")))
        self.account = base.upsert_account({"phone": "", "platforms": [{"platform": "wechat_channels", "uid": UID, "nickname": "测试"}]}, db_path=self.db)

    def test_discovery_detail_metrics_replay_share_exact_account_without_duplicate_calls(self):
        calls = []
        def page_call(stage, identity):
            calls.append((stage, identity["uid"]))
            return providers._extra_parse("wechat_channels", "discovery", UID, page_payload())
        first = providers.discover_account_content(self.account["id"], "wechat_channels", UID,
            as_of=date(2026, 9, 12), db_path=self.db, call_override=page_call,
            materialize_discovery_detail=False)
        replay = providers.discover_account_content(self.account["id"], "wechat_channels", UID,
            as_of=date(2026, 9, 12), db_path=self.db, call_override=page_call,
            materialize_discovery_detail=False)
        self.assertEqual(first["inserted"], 1)
        self.assertEqual(replay["inserted"], 0)
        self.assertEqual(calls, [("discover_content", UID)])
        with connect(self.db) as connection:
            row = connection.execute("SELECT * FROM content_items WHERE platform='wechat_channels'").fetchone()
            self.assertEqual(row["account_id"], self.account["id"])
            self.assertEqual(row["platform_content_id"], OBJECT)
            content_id = row["id"]
        detail_calls = []
        def content_call(stage, content):
            detail_calls.append(stage)
            return providers._extra_parse("wechat_channels", stage, OBJECT, response(video()), expected_uid=UID)
        result = providers.update_content_data(content_id, as_of=date(2026, 9, 12), db_path=self.db,
            call_override=content_call, stages=["detail", "metrics"], process_media=False)
        repeat = providers.update_content_data(content_id, as_of=date(2026, 9, 12), db_path=self.db,
            call_override=content_call, stages=["detail", "metrics"], process_media=False)
        self.assertEqual(detail_calls, ["detail"], result)
        self.assertEqual(result["provider_cost"], .01)
        self.assertTrue(all(row["status"] in {"already_succeeded", "replayed"} for row in repeat["stages"]), repeat)
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM content_items WHERE platform='wechat_channels'").fetchone()[0], 1)
            observations = connection.execute("SELECT * FROM content_metric_observations WHERE content_id=?", (content_id,)).fetchall()
            self.assertTrue(observations)
            self.assertTrue(any(row["like_count"] == 12 and row["comment_count"] == 0 for row in observations))
            self.assertTrue(all(row["view_count"] is None for row in observations))
            self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])
            for raw in connection.execute("SELECT local_path,sha256,byte_size FROM provider_raw_responses"):
                from pathlib import Path
                body = Path(raw["local_path"]).read_bytes()
                self.assertEqual(hashlib.sha256(body).hexdigest(), raw["sha256"])
                self.assertEqual(len(body), raw["byte_size"])

    def test_wechat_shared_comment_pager_round_trips_cursor_and_replays_without_request(self):
        discovery = page_payload()
        discovery["data"]["object"][0]["commentCount"] = 2
        providers.discover_account_content(self.account["id"], "wechat_channels", UID,
            as_of=date(2026, 9, 12), db_path=self.db,
            call_override=lambda *_: providers._extra_parse("wechat_channels", "discovery", UID, discovery),
            materialize_discovery_detail=False)
        with connect(self.db) as connection:
            content_id = connection.execute("SELECT id FROM content_items WHERE platform='wechat_channels'").fetchone()[0]
        cursors = []
        def comments(stage, content):
            self.assertEqual(stage, "comments")
            cursor = content.get("_comment_cursor"); cursors.append(cursor)
            first = cursor is None
            payload = response({"baseResponse": {"ret": 0}, "commentInfo": [{"commentId": "0001" if first else "0002",
                "username": "fixture-private-comment-user", "content": "汽车评论一" if first else "汽车评论二", "likeCount": 0}],
                "monotonicData": {"commentCount": 2}, "downContinueFlag": int(first),
                "lastBuffer": "YWJj+//==" if first else ""}, object_id=OBJECT)
            return providers._extra_parse("wechat_channels", "comments", OBJECT, payload)
        with patch.object(providers, "_xhs_call", side_effect=AssertionError("wrong platform fallback")):
            first = providers.capture_content_comments_live(content_id, db_path=self.db, as_of=date(2026, 9, 12), call_override=comments)
            repeat = providers.capture_content_comments_live(content_id, db_path=self.db, as_of=date(2026, 9, 12), call_override=comments)
        self.assertEqual(cursors, [None, {"last_buffer": "YWJj+//=="}])
        self.assertEqual(first["status"], "succeeded", first)
        self.assertEqual(repeat["provider_cost"], 0)
        with connect(self.db) as connection:
            rows = [dict(row) for row in connection.execute("SELECT c.* FROM comments c JOIN comment_evidence_versions e ON e.id=c.evidence_version_id WHERE e.content_id=?", (content_id,))]
            self.assertEqual(len(rows), 2)
            self.assertTrue(all(row["anonymous_user_key"] and row["interaction_user_id"] for row in rows))
            self.assertEqual(len({row["interaction_user_id"] for row in rows}), 1)
            self.assertNotIn("fixture-private-comment-user", json.dumps(rows))


    def test_kuaishou_bound_discovery_replay_detail_and_metrics_prices(self):
        uid, content_key = "001234", "5234567890123456789"
        account = base.upsert_account({"phone": "", "platforms": [{"platform": "kuaishou", "uid": uid}]}, db_path=self.db)
        page = discovery_payload()
        page["data"]["pcursor"] = "no_more"
        # Omit the discoverable view counter so its zero-cost observation does
        # not complete a later independent metric slot.
        del page["data"]["feeds"][0]["view_count"]
        calls = []
        def page_call(stage, identity):
            calls.append(stage)
            return providers._extra_parse("kuaishou", "discovery", uid, page)
        first = providers.discover_account_content(account["id"], "kuaishou", uid,
            as_of=date(2026, 9, 12), db_path=self.db, call_override=page_call, materialize_discovery_detail=False)
        self.assertEqual(first["provider_cost"], .01)
        self.assertEqual(first["inserted"], 1)
        providers.discover_account_content(account["id"], "kuaishou", uid,
            as_of=date(2026, 9, 12), db_path=self.db, call_override=page_call, materialize_discovery_detail=False)
        self.assertEqual(calls, ["discover_content"])
        with connect(self.db) as connection:
            row = connection.execute("SELECT * FROM content_items WHERE platform='kuaishou'").fetchone()
            self.assertEqual(row["account_id"], account["id"])
            self.assertEqual(row["platform_content_id"], content_key)
            content_id = row["id"]
        def content_call(stage, content):
            calls.append(stage)
            return providers._extra_parse("kuaishou", stage, content_key, detail_payload(), expected_uid=uid)
        result = providers.update_content_data(content_id, as_of=date(2026, 9, 12), db_path=self.db,
            call_override=content_call, stages=["detail", "metrics"], process_media=False)
        self.assertEqual(calls, ["discover_content", "detail"], result)
        self.assertEqual(result["provider_cost"], .001)
        replay = providers.update_content_data(content_id, as_of=date(2026, 9, 12), db_path=self.db,
            call_override=content_call, stages=["detail", "metrics"], process_media=False)
        self.assertTrue(all(row["status"] in {"already_succeeded", "replayed"} for row in replay["stages"]), replay)
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM content_items WHERE platform='kuaishou'").fetchone()[0], 1)
            selected = providers.select_content_metrics(connection, [content_id])[content_id]
            self.assertEqual(selected["like_count"], 0)
            self.assertEqual(selected["view_count"], 12)

    def test_missing_url_requires_explicit_internal_verified_identity_and_preserves_known_link(self):
        from v8.operations import upsert_content, OperationError
        item = {"platform": "wechat_channels", "platform_content_id": OBJECT, "account_uid": UID,
                "canonical_url": "", "content_type": "video"}
        with self.assertRaises(OperationError):
            upsert_content(item, db_path=self.db)
        with self.assertRaises(OperationError):
            upsert_content(item, db_path=self.db, verified_provider_identity=("wechat_channels", "wrong"))
        with self.assertRaises(OperationError):
            upsert_content({**item, "platform": "unknown"}, db_path=self.db, verified_provider_identity=("unknown", UID))
        first = upsert_content({**item, "canonical_url": f"https://channels.weixin.qq.com/video/{OBJECT}?object_nonce_id=fixture"}, db_path=self.db)
        replay = upsert_content(item, db_path=self.db, verified_provider_identity=("wechat_channels", UID))
        self.assertEqual(first["id"], replay["id"])
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT canonical_url FROM content_items WHERE id=?", (first["id"],)).fetchone()[0], f"https://channels.weixin.qq.com/video/{OBJECT}?object_nonce_id=fixture")


    def test_runtime_profile_refresh_persists_metrics_and_replays_without_repurchase(self):
        uid = "001234"
        account = base.upsert_account({"phone": "", "platforms": [{"platform": "kuaishou", "uid": uid}]}, db_path=self.db)
        with connect(self.db) as connection:
            iid = connection.execute("SELECT id FROM account_platform_identities WHERE account_id=?", (account["id"],)).fetchone()[0]
        envelope = {"platform": "kuaishou", "uid": uid, "account_id": account["id"], "identity_id": iid,
                    "logical_due": "account-metrics:2026-09-12T00:00:00Z"}
        with patch.object(providers, "_load_key", return_value="fixture"), patch.object(providers, "_freeze_tikhub_transport", return_value=None), patch.object(providers, "_extra_call", return_value=providers._extra_parse("kuaishou", "profile", uid, profile_payload())) as call:
            result = runtime._account_request(envelope, db_path=self.db, at="2026-09-12T02:30:00Z")
            repeat = runtime._account_request(envelope, db_path=self.db, at="2026-09-12T02:31:00Z")
        self.assertTrue(result["complete"])
        self.assertEqual(result["provider_cost"], .01)
        self.assertEqual(repeat["provider_cost"], 0)
        self.assertEqual(call.call_count, 1)
        with connect(self.db) as connection:
            rows = connection.execute("SELECT * FROM account_metric_observations WHERE account_identity_id=?", (iid,)).fetchall()
            self.assertEqual(len(rows), 1)
            selected = account_metrics.select_account_metrics(connection, [iid])[iid]
            self.assertEqual(selected["follower_count"], 0)
            self.assertEqual(selected["platform_work_count"], 12)
            self.assertEqual(selected["total_likes"], 30)

    def test_runtime_discovery_preserves_cursor_and_only_completes_terminal_page(self):
        first_page = page_payload(True, "ab+//==")
        second_page = response({"contact": {"username": UID}, "object": [], "continueFlag": 0, "upContinueFlag": 0, "lastBuffer": ""})
        envelope = {"platform": "wechat_channels", "uid": UID, "account_id": self.account["id"], "identity_id": 2,
            "operation": "wechat_channels_user_posts", "stage": "discovery", "logical_due": "discovery:2026-09-12T00:00:00Z",
            "window_start": "2023-11-01T00:00:00Z", "window_end": "2023-12-01T00:00:00Z", "cursor": "",
            "page_count": 0, "raw_ids": [], "seen_cursors": [], "counts": {"seen": 0, "valid": 0, "invalid": 0}}
        cursors = []
        def call(platform, stage, subject, key, *, cursor=None):
            cursors.append(cursor)
            return providers._extra_parse(platform, stage, subject, first_page if cursor == "" else second_page)
        with patch.object(providers, "_load_key", return_value="fixture"), patch.object(providers, "_freeze_tikhub_transport", return_value=None), patch.object(providers, "_extra_call", side_effect=call):
            first = runtime._discovery_page(envelope, db_path=self.db, at="2026-09-12T02:30:00Z")
            second = runtime._discovery_page(first["envelope"], db_path=self.db, at="2026-09-12T02:31:00Z")
        self.assertFalse(first["complete"])
        self.assertTrue(first["continuation"])
        self.assertTrue(second["complete"])
        self.assertFalse(second["continuation"])
        self.assertEqual(cursors, ["", "ab+//=="])
        self.assertEqual(len(second["evidence"]["raw_response_ids"]), 2)
        self.assertEqual(second["evidence"]["valid"], 1)

    def test_readiness_new_operations_retain_identity_and_budget_checks(self):
        from v8.work_readiness import WorkReadinessPass, _platform
        with connect(self.db) as connection:
            iid = connection.execute("SELECT id FROM account_platform_identities WHERE account_id=?", (self.account["id"],)).fetchone()[0]
            for suffix in ("user_posts", "user_profile", "video_detail", "video_statistics"):
                operation = "wechat_channels_" + suffix
                self.assertEqual(_platform(operation), "wechat_channels")
                result = WorkReadinessPass(connection, at="2026-09-12T02:30:00Z").assess(operation=operation,
                    category="reconcile" if suffix == "user_posts" else "metrics", account_id=self.account["id"], identity_id=iid)
                self.assertIsInstance(result["runnable"], bool)
                self.assertNotEqual(result["reason"], "identity_conflict")
            conflict = WorkReadinessPass(connection, at="2026-09-12T02:30:00Z").assess(operation="kuaishou_user_posts",
                category="reconcile", account_id=self.account["id"], identity_id=iid)
            self.assertFalse(conflict["runnable"])
            self.assertEqual(conflict["reason"], "identity_conflict")
        with self.assertRaises(ValueError):
            _platform("wechat_channels_unverified_operation")


class NewPlatformCatalogPlannerTest(unittest.TestCase):
    def test_profile_backed_paused_account_gets_all_content_work_but_requires_send_gate(self):
        from tests.test_v8_catalog_capture_planner import CatalogCapturePlannerTest, AT
        from v8 import account_directory
        from v8.operations import upsert_account, upsert_content
        from v8.storage import transaction
        fixture = CatalogCapturePlannerTest()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        db = fixture.db
        account = upsert_account({"phone": "", "platforms": [{"platform": "kuaishou", "uid": "001234"}]}, db_path=db)
        entity = json.dumps(profile_payload(), ensure_ascii=False).encode()
        path = fixture.base.root.resolve() / "kuaishou-profile.json"
        path.write_bytes(entity)
        path.chmod(0o600)
        with connect(db) as connection, transaction(connection):
            iid = connection.execute("SELECT id FROM account_platform_identities WHERE account_id=?", (account["id"],)).fetchone()[0]
            raw_id = connection.execute("INSERT INTO provider_raw_responses(account_id,provider,operation,local_path,sha256,byte_size,http_status,captured_at) VALUES(?,'TikHub','kuaishou_user_profile',?,?,?,200,?)",
                (account["id"], str(path), hashlib.sha256(entity).hexdigest(), len(entity), AT)).lastrowid
            connection.execute("INSERT INTO account_provider_references(account_identity_id,provider,reference_kind,reference_value,source_raw_response_id,created_at,updated_at) VALUES(?,'TikHub','kuaishou_user_id','001234',?,?,?)", (iid, raw_id, AT, AT))
            account_directory.ensure_account_directory_schema(connection)
            connection.execute("INSERT INTO account_directory_rows(source_sha256,source_name,source_sheet,source_row,account_id,platform,uid,account_status,identity_status,raw_json,imported_at,updated_at) VALUES(?,'fixture','fixture',3,?,'kuaishou','001234','paused','uid_unverified','{}',?,?)", ('b'*64, account["id"], AT, AT))
        content = upsert_content({"platform": "kuaishou", "platform_content_id": "5234567890123456789", "account_uid": "001234", "canonical_url": "https://www.kuaishou.com/short-video/3xwork", "published_at": "2026-09-01T15:00:00Z", "content_type": "video"}, db_path=db, verified_provider_identity=("kuaishou", "001234"))
        plan = fixture.plan()
        self.assertIn(iid, [member["identity_id"] for member in plan["cohort"]], plan.get("catalog_snapshot"))
        with connect(db) as connection, transaction(connection):
            runtime._plan_due(connection, plan, at=AT)
            rows = connection.execute("SELECT operation,state,reason FROM capture_work_items WHERE account_id=?", (account["id"],)).fetchall()
        self.assertEqual({row["operation"] for row in rows}, {"kuaishou_user_posts", "kuaishou_user_profile", "kuaishou_video_detail", "kuaishou_video_statistics"})
        self.assertTrue(all(row["state"] == "provider_blocked" and row["reason"] == "provider_transport_blocked" for row in rows))
