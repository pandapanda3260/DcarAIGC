from __future__ import annotations

import contextlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.metric_fixture import provider_metric
from tests.roster_fixture import accept_roster
from v8 import capture, providers
from v8.account_metrics import parse_tikhub_profile
from v8.capture import ProviderResult
from v8.operations import upsert_account, upsert_content
from v8.provider_updates import refresh_account_profile, refresh_content_metrics
from v8.provider_budget import _SCOPE, paid_scope
from v8.source_routing import select_content_metrics
from v8.storage import connect, initialize_database, transaction

AT = "2026-08-29T04:00:00Z"


class ProviderUpdatesTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.db = self.root / "updates.sqlite3"
        self.stack = contextlib.ExitStack()
        self.stack.enter_context(patch.object(capture, "RAW_ROOT", self.root / "raw"))
        for module in ("capture", "providers", "provider_updates", "provider_budget", "account_metrics", "metric_observations"):
            self.stack.enter_context(patch(f"v8.{module}.now_utc", return_value=AT))
        with connect(self.db) as connection:
            initialize_database(connection)
        self.account = upsert_account({"phone": "", "platforms": [
            {"platform": "douyin", "uid": "123456789", "nickname": "fixture"}
        ]}, db_path=self.db)
        self.content = upsert_content({
            "platform": "douyin", "platform_content_id": "7380000000000000001",
            "canonical_url": "https://www.douyin.com/video/7380000000000000001",
            "account_uid": "123456789", "title": "Original title", "body": "Original body",
            "content_type": "video", "published_at": "2026-08-27T00:00:00Z",
        }, db_path=self.db)
        with connect(self.db) as connection, transaction(connection):
            self.roster = accept_roster(connection, accepted_at="2026-08-01T00:00:00Z")
            self.identity_id = connection.execute("SELECT id FROM account_platform_identities").fetchone()[0]
        self.calls = []

    def tearDown(self):
        self.stack.close()
        self.temp.cleanup()

    def observe(self, **values):
        with connect(self.db) as connection, transaction(connection):
            return provider_metric(connection, content_id=self.content["id"], captured_at=AT,
                                   window_key="2026-08-29", view_count=values.get("view_count"),
                                   comment_count=values.get("comment_count"), like_count=values.get("like_count"),
                                   share_count=values.get("share_count"), collect_count=values.get("collect_count"),
                                   status="available", metadata_json="{}", provider="newrank_matrix")

    def fixture_call(self, group, content):
        self.calls.append(group)
        if group == "detail_counts":
            raw = {"code": 200, "data": {"status_code": 0, "aweme_detail": {
                "aweme_id": content["platform_content_id"], "author": {"uid": "123456789"},
                "desc": "Must not overwrite text", "statistics": {
                    "comment_count": 3, "collect_count": 2, "digg_count": 7, "share_count": 4,
                },
            }}}
            parsed = providers._parse_douyin_stage_payload("detail", content["platform_content_id"], raw)
            return ProviderResult(parsed.data["metrics"], raw, 200, True)
        raw = {"code": 200, "data": {"status_code": 0, "statistics_list": [{
            "aweme_id": content["platform_content_id"], "play_count": 88, "digg_count": 9, "share_count": 5,
        }]}}
        return providers._parse_douyin_stage_payload("metrics", content["platform_content_id"], raw)

    def run_metrics(self, **kwargs):
        return refresh_content_metrics(self.content["id"], db_path=self.db, at=AT,
                                       call_override=kwargs.pop("call_override", self.fixture_call), **kwargs)

    def test_fresh_matrix_fields_make_no_paid_request(self):
        self.observe(view_count=0, comment_count=1, like_count=2, share_count=3, collect_count=4)
        result = self.run_metrics()
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(self.calls, [])
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0], 0)

    def test_missing_play_only_uses_statistics_and_does_not_touch_media_text_or_analysis(self):
        self.observe(comment_count=1, like_count=2, share_count=3, collect_count=4)
        with connect(self.db) as connection:
            before = dict(connection.execute("SELECT * FROM content_items").fetchone())
        result = self.run_metrics()
        self.assertEqual(self.calls, ["statistics"])
        self.assertEqual(result["provider_cost"], .001)
        with connect(self.db) as connection:
            self.assertEqual(dict(connection.execute("SELECT * FROM content_items").fetchone()), before)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM evidence_artifacts").fetchone()[0], 0)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM evaluation_versions").fetchone()[0], 0)
            self.assertEqual(
                connection.execute(
                    "SELECT max_amount FROM provider_budget_batches"
                ).fetchone()[0],
                100,
            )
            row = select_content_metrics(connection, [self.content["id"]], cutoff_at=AT)[self.content["id"]]
            owner = connection.execute(
                "SELECT id,status FROM scheduler_runs "
                "WHERE job_id='paid_capture_direct'"
            ).fetchone()
            dispatch = connection.execute(
                "SELECT scheduler_run_id,event_type FROM paid_provider_dispatch_events "
                "ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
        self.assertEqual(row["view_count"], 88)
        self.assertEqual(row["like_count"], 2)  # Primary preference, not max/newest.
        self.assertEqual(row["fields"]["view_count"]["effective_provider"], "tikhub")
        self.assertEqual(owner["status"], "succeeded")
        self.assertEqual(dispatch["scheduler_run_id"], owner["id"])
        self.assertEqual(dispatch["event_type"], "succeeded")

    def test_two_fixed_routes_preserve_unrequested_fields_and_reentry_costs_zero(self):
        first = self.run_metrics()
        self.assertEqual(first["status"], "succeeded")
        self.assertEqual(self.calls, ["detail_counts", "statistics"])
        self.assertEqual(first["provider_cost"], .002)
        second = self.run_metrics()
        self.assertEqual(second["provider_cost"], 0)
        self.assertEqual(len(self.calls), 2)
        with connect(self.db) as connection:
            row = select_content_metrics(connection, [self.content["id"]], cutoff_at=AT)[self.content["id"]]
            usages = connection.execute("SELECT amount,details_json FROM provider_usage").fetchall()
        self.assertEqual((row["view_count"], row["comment_count"], row["collect_count"]), (88, 3, 2))
        self.assertTrue(all(json.loads(item["details_json"])["category"] == "metrics" for item in usages))

    def test_successful_raw_replays_free_after_business_write_failure(self):
        self.observe(comment_count=1, like_count=2, share_count=3, collect_count=4)
        with patch("v8.providers._store_stage_result", side_effect=RuntimeError("injected write failure")):
            with self.assertRaisesRegex(RuntimeError, "injected"):
                self.run_metrics()
        result = self.run_metrics()
        self.assertEqual(self.calls, ["statistics"])
        self.assertEqual(result["provider_cost"], 0)
        self.assertEqual(result["requests"][0]["status"], "replayed")
        with connect(self.db) as connection:
            fact = connection.execute("SELECT * FROM content_metric_observations WHERE source='tikhub'").fetchone()
        self.assertEqual(fact["captured_at"], AT)

    def test_success_with_missing_field_does_not_repurchase_same_cycle(self):
        self.observe(view_count=20, comment_count=1, like_count=2, share_count=3)

        def incomplete(group, content):
            parsed = self.fixture_call(group, content)
            raw = parsed.raw_response
            del raw["data"]["aweme_detail"]["statistics"]["collect_count"]
            detail = providers._parse_douyin_stage_payload("detail", content["platform_content_id"], raw)
            return ProviderResult(detail.data["metrics"], raw, 200, True)

        first = self.run_metrics(call_override=incomplete)
        second = self.run_metrics(call_override=incomplete)
        self.assertEqual(first["status"], "partial")
        self.assertEqual(first["missing_fields"], ["collect_count"])
        self.assertTrue(first["request_cycle_complete"])
        self.assertEqual(self.calls, ["detail_counts"])
        self.assertEqual(second["provider_cost"], 0)
        self.assertEqual(second["missing_fields"], ["collect_count"])

    def test_allowed_groups_skips_blocked_group_and_runs_other_ready_group(self):
        result = self.run_metrics(allowed_groups=["statistics"])

        self.assertEqual(self.calls, ["statistics"])
        self.assertEqual(result["provider_cost"], 0.001)
        self.assertEqual(
            [request["group"] for request in result["requests"]],
            ["statistics"],
        )
        self.assertEqual(result["deferred_groups"], ["detail_counts"])
        self.assertFalse(result["request_cycle_complete"])
        self.assertEqual(result["status"], "partial")
        self.assertEqual(
            set(result["missing_fields"]),
            {"comment_count", "share_count", "collect_count"},
        )

    def test_allowed_groups_all_deferred_makes_no_provider_request(self):
        result = self.run_metrics(allowed_groups=[])

        self.assertEqual(self.calls, [])
        self.assertEqual(result["provider_cost"], 0)
        self.assertEqual(result["requests"], [])
        self.assertEqual(
            result["deferred_groups"], ["detail_counts", "statistics"]
        )
        self.assertFalse(result["request_cycle_complete"])
        self.assertEqual(result["status"], "partial")
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0],
                0,
            )

    def test_allowed_groups_rejects_unknown_policy_group_before_provider_call(self):
        with self.assertRaisesRegex(
            providers.ProviderConfigurationError,
            "unknown metric supplement group: blocked_fixture",
        ):
            self.run_metrics(allowed_groups=["statistics", "blocked_fixture"])

        self.assertEqual(self.calls, [])
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0],
                0,
            )

    def test_xhs_counter_route_never_buys_exposure(self):
        account = upsert_account({"phone": "", "platforms": [
            {"platform": "xiaohongshu", "uid": "6" * 24}
        ]}, db_path=self.db)
        self.assertNotEqual(account["id"], self.account["id"])
        note = upsert_content({
            "platform": "xiaohongshu", "platform_content_id": "a" * 24,
            "canonical_url": "https://www.xiaohongshu.com/explore/" + "a" * 24,
            "account_uid": "6" * 24, "content_type": "image",
            "published_at": "2026-08-27T00:00:00Z",
        }, db_path=self.db)
        with connect(self.db) as connection, transaction(connection):
            accept_roster(connection, accepted_at=AT)

        def call(group, content):
            self.calls.append(group)
            raw = {"code": 200, "data": {"success": True, "code": 0, "data": {
                "id": content["platform_content_id"], "type": "normal",
                "liked_count": "3", "collected_count": "4", "comments_count": "5", "shared_count": "6",
            }}}
            return providers._parse_xhs_stage_payload("metrics", content["platform_content_id"], "image", raw)

        result = refresh_content_metrics(note["id"], db_path=self.db, at=AT, call_override=call)
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(self.calls, ["note_counts"])
        self.assertEqual(result["provider_cost"], .01)
        with connect(self.db) as connection:
            selected = select_content_metrics(connection, [note["id"]], cutoff_at=AT)[note["id"]]
        self.assertIsNone(selected["view_count"])
        self.assertEqual(selected["fields"]["view_count"]["status"], "not_applicable")

    def test_profile_fans_raw_replayed_once_without_local_account_rewrite(self):
        def call(group, member):
            self.calls.append(group)
            raw = {"code": 200, "data": {"status_code": 0, "data": {
                "id_str": member["uid"], "follow_info": {"follower_count": 0},
            }}}
            return ProviderResult(parse_tikhub_profile(raw, platform="douyin", uid=member["uid"]), raw, 200, True)

        first = refresh_account_profile(self.identity_id, db_path=self.db, at=AT, call_override=call)
        second = refresh_account_profile(self.identity_id, db_path=self.db, at=AT, call_override=call)
        self.assertEqual(self.calls, ["profile"])
        self.assertEqual((first["provider_cost"], second["provider_cost"]), (.001, 0))
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM account_metric_observations").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT nickname FROM account_platform_identities").fetchone()[0], "fixture")
            self.assertEqual(json.loads(connection.execute("SELECT details_json FROM provider_usage").fetchone()[0])["category"], "metrics")
            self.assertEqual(
                connection.execute(
                    "SELECT max_amount FROM provider_budget_batches"
                ).fetchone()[0],
                100,
            )

    def test_download_workers_keep_history_category_and_frozen_owner(self):
        from v8 import media
        scopes = []

        def download(cid, **kwargs):
            scopes.append(_SCOPE.get())
            return {"content_id": cid, "status": "downloaded"}

        with patch.object(media, "_queue_recovery_scope_content_ids", return_value=[1, 2]), \
             patch.object(media, "recover_stale_media_processing_slots", return_value={}), \
             patch.object(media, "_queue_content_ids", return_value=[1, 2]), \
             patch.object(media, "process_content_media", side_effect=download), \
             paid_scope("history", roster_snapshot_id=1, roster_snapshot_hash="a" * 64,
                        scheduler_run_id=11, scheduler_attempt_id=12):
            result = media.run_media_download_queue(db_path=self.db, limit=2, max_workers=2)
        self.assertEqual(result["downloaded"], 2)
        self.assertEqual(len(scopes), 2)
        self.assertTrue(all((scope.purpose, scope.scheduler_run_id, scope.scheduler_attempt_id) == ("history", 11, 12) for scope in scopes))


if __name__ == "__main__":
    unittest.main()
