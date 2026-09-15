"""Discovery persists current page metrics and queues only unmet metric groups.

The private schema21 database uses the real materializer, field selector, work
insertion and cycle deduplication. Only external activation/cohort/readiness
inputs are fixtures. No provider request or media/analysis execution is allowed.
"""
from __future__ import annotations

import hashlib
import json
import socket
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from v8 import account_roster, capture_runtime as runtime, metric_observations
from v8 import operations, profile_activations, providers, storage
from v8.source_routing import OPERATION_FIELD_POLICY_VERSION, select_content_metrics

AT = "2026-09-12T06:00:00Z"
CAPTURED = "2026-09-12T05:50:00Z"
UID = "99887766"
PID = "7500000000000000001"


class DiscoveryMetricsFlowTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.db = self.root / "discovery-metrics.sqlite3"
        self.clock = AT
        self.network = self.enterContext(patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden")))
        self.http = self.enterContext(patch.object(providers, "_request_json", side_effect=AssertionError("HTTP forbidden")))
        self.execute = self.enterContext(patch.object(runtime, "_execute_one", side_effect=AssertionError("provider execution forbidden")))
        for module in (runtime, providers, operations, metric_observations):
            self.enterContext(patch.object(module, "now_utc", lambda: self.clock))
        with storage.connect(self.db) as connection:
            storage.initialize_database(connection, target_version=21)
            connection.execute("""INSERT INTO accounts(id,phone,phone_normalized,
                operator_name,enabled,created_at,updated_at)
                VALUES(1,'',NULL,'fixture',1,?,?)""", (AT, AT))
            connection.execute("""INSERT INTO account_platform_identities(id,account_id,
                platform,uid,nickname,source,created_at,updated_at)
                VALUES(1,1,'douyin',?,'fixture','manual',?,?)""", (UID, AT, AT))
            connection.commit()
        self.active = {"activation_id": 3, "profile_id": "integrated_route_v1",
            "activation_sha256": "a" * 64, "roster_snapshot_id": 2,
            "roster_members_sha256": "b" * 64}
        self.member = runtime.planning.adaptive_cohorts([{
            "identity_id": 1, "account_id": 1, "platform": "douyin", "uid": UID,
            "enabled": 1, "monitoring_status": "monitored", "history_days": 7,
            "video_count": 1, "created_at": "2026-08-01T00:00:00Z",
            "accepted_at": "2026-08-01T00:00:00Z",
        }], business_day="2026-09-12")[0]
        self.activation = self.enterContext(patch.object(runtime, "activation_at", return_value=None))
        self.enterContext(patch.object(profile_activations, "activation_at", side_effect=lambda *_a, **_k: self.activation.return_value))
        self.enterContext(patch.object(runtime, "_cohort_plan", side_effect=self.cohort))
        self.enterContext(patch.object(account_roster, "require_active_member", return_value=self.member))
        self.enterContext(patch.object(runtime, "_readiness", return_value=("runnable", "")))
        with storage.connect(self.db) as connection, storage.transaction(connection):
            connection.execute("""INSERT INTO routing_input_changes(change_kind,payload_json,
                effective_at,recorded_at,change_sha256) VALUES('policy','{}',?,?,?)""",
                (AT, AT, "c" * 64))
            for operation in ("douyin_video_statistics", "douyin_video_detail"):
                runtime.planning.assign_route(connection, scope_type="account", scope_key="1",
                    account_id=1, provider="tikhub", operation=operation, expected_generation=0,
                    route="integrated", mode="active", effective_at=AT, recorded_at=AT)
        self.addCleanup(self.assert_no_execution)

    def cohort(self, connection, active, *, at, shadow):
        day = runtime._business_day(at)
        body = {**active, "business_day": day, "cohort": [self.member],
                "contract_version": runtime.CONTRACT, "shadow": shadow}
        connection.execute("""INSERT OR IGNORE INTO capture_source_plans(roster_change_id,
            business_day,generation,mode,payload_json,created_at,plan_sha256)
            VALUES(1,?,1,'active',?,?,?)""",
            (day, runtime.planning.canonical(body), at, runtime.planning.digest(body)))
        plan_id = connection.execute("SELECT id FROM capture_source_plans WHERE plan_sha256=?",
            (runtime.planning.digest(body),)).fetchone()[0]
        return {"id": plan_id, **body}

    def item(self, metrics=None, *, pid=PID):
        return {"platform": "douyin", "platform_content_id": pid,
            "canonical_url": f"https://www.douyin.com/video/{pid}",
            "account_uid": UID, "content_type": "video", "title": "discovered",
            "published_at": "2026-09-10T04:00:00Z", "metrics": metrics or {}}

    def raw(self, page, *, captured_at=CAPTURED, content_id=None, operation="douyin_user_posts"):
        with storage.connect(self.db) as connection, storage.transaction(connection):
            sequence = connection.execute("SELECT COUNT(*) FROM provider_raw_responses").fetchone()[0] + 1
            stage = "discovery" if operation == "douyin_user_posts" else "metrics"
            slot = connection.execute("""INSERT INTO fetch_slots(content_id,account_id,stage,
                window_key,provider,adapter_version,status,attempt_count,created_at,updated_at)
                VALUES(?,?,?,?, 'TikHub','fixture-v1','succeeded',1,?,?)""",
                (content_id, 1 if content_id is None else None, stage, f"page-{sequence}", captured_at, captured_at)).lastrowid
            attempt = connection.execute("""INSERT INTO fetch_attempts(slot_id,attempt_number,
                request_started_at,response_finished_at,http_status,billed)
                VALUES(?,1,?,?,200,0)""", (slot, captured_at, captured_at)).lastrowid
            payload = json.dumps(page, sort_keys=True).encode()
            path = self.root / f"raw-{sequence}.json"
            path.write_bytes(payload)
            return connection.execute("""INSERT INTO provider_raw_responses(fetch_attempt_id,
                content_id,account_id,provider,operation,local_path,sha256,byte_size,captured_at)
                VALUES(?,?,?,'TikHub',?,?,?,?,?)""", (attempt, content_id, 1 if content_id is None else None, operation,
                str(path), hashlib.sha256(payload).hexdigest(), len(payload), captured_at)).lastrowid

    def materialize(self, page, raw_id):
        return providers.materialize_account_discovery_page(account_id=1, platform="douyin",
            account_uid=UID, page=page, source_raw_response_id=raw_id,
            metrics_window_key="2026-09-12", discovery_operation="douyin_user_posts",
            provider="TikHub", derived_adapter_version="discovery-derived-v1",
            derived_operations={"detail": "douyin_discovery_detail", "metrics": "douyin_discovery_metrics"},
            zero_view_is_authoritative=False, materialize_detail=False,
            materialize_existing_stages=False, db_path=self.db,
            derived_raw_root=self.root / "derived", media_root=self.root / "media")

    def fields(self, content_id):
        with storage.connect(self.db) as connection:
            return select_content_metrics(connection, [content_id], cutoff_at=self.clock,
                policy_version=OPERATION_FIELD_POLICY_VERSION)[content_id]["fields"]

    def works(self):
        with storage.connect(self.db) as connection:
            return [dict(row) for row in connection.execute("SELECT * FROM capture_work_items ORDER BY id")]

    def observation_count(self):
        with storage.connect(self.db) as connection:
            return connection.execute("SELECT COUNT(*) FROM content_metric_observations").fetchone()[0]

    def assert_no_execution(self):
        self.network.assert_not_called()
        self.http.assert_not_called()
        self.execute.assert_not_called()
        with storage.connect(self.db) as connection:
            for table in ("provider_usage", "provider_request_start_events", "evaluation_versions"):
                self.assertEqual(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], 0)
            self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])

    def test_links_only_persists_list_counts_with_original_source_and_time(self):
        page = {"items": [self.item({"view_count": 0, "like_count": 9,
            "comment_count": 0, "share_count": 3, "collect_count": 4})]}
        raw_id = self.raw(page)
        result = self.materialize(page, raw_id)
        content_id = result["content_changes"][0]["content_id"]
        fields = self.fields(content_id)
        for name, value in (("comment_count", 0), ("share_count", 3), ("collect_count", 4)):
            with self.subTest(field=name):
                self.assertEqual(fields[name]["value"], value)
                self.assertEqual(fields[name]["captured_at"], CAPTURED)
                self.assertEqual(fields[name]["raw_response_id"], raw_id)
                self.assertEqual(fields[name]["effective_operation"], "douyin_user_posts")
                self.assertEqual(fields[name]["freshness"], "fresh")
        self.assertIsNone(fields["view_count"]["value"])
        self.assertFalse(fields["like_count"]["is_latest_valid"])
        self.assertEqual(self.works(), [])
        with storage.connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM fetch_slots WHERE content_id=? AND stage='detail'", (content_id,)).fetchone()[0], 0)

    def test_existing_content_takes_new_page_counts_and_same_raw_replay_is_idempotent(self):
        old_page = {"items": [self.item({"view_count": 0, "comment_count": 8})]}
        first = self.materialize(old_page, self.raw(old_page, captured_at="2026-09-12T05:00:00Z"))
        content_id = first["content_changes"][0]["content_id"]
        new_page = {"items": [self.item({"view_count": 0, "comment_count": 6})]}
        raw_id = self.raw(new_page)
        second = self.materialize(new_page, raw_id)
        self.assertEqual(second["content_changes"][0]["content_id"], content_id)
        self.assertEqual(self.fields(content_id)["comment_count"]["value"], 6)
        self.assertEqual(self.fields(content_id)["comment_count"]["captured_at"], CAPTURED)
        count = self.observation_count()
        self.materialize(new_page, raw_id)
        self.assertEqual(self.observation_count(), count)
        self.assertEqual(self.fields(content_id)["comment_count"]["raw_response_id"], raw_id)

    def test_discovery_immediately_queues_only_missing_statistics_without_media(self):
        self.activation.return_value = self.active
        page = {"items": [self.item({"view_count": 123, "like_count": 9,
            "comment_count": 1, "share_count": 2, "collect_count": 3})]}
        raw_id = self.raw(page)
        content_id = self.materialize(page, raw_id)["content_changes"][0]["content_id"]
        work = self.works()
        self.assertEqual(len(work), 1)
        self.assertEqual((work[0]["content_id"], work[0]["operation"]), (content_id, "douyin_video_statistics"))
        self.assertEqual(json.loads(work[0]["envelope_json"])["stage"], "metrics")
        self.assertEqual(self.observation_count(), 1)
        with storage.connect(self.db) as connection:
            # A positive list VV still is not a qualified statistics source;
            # neither a synthetic slot nor a duplicate fact may close its gap.
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM fetch_slots WHERE content_id=? AND stage='metrics'", (content_id,)).fetchone()[0], 0)
        self.materialize(page, raw_id)
        self.assertEqual(self.works(), work)
        self.assertEqual(self.observation_count(), 1)

    def test_complete_fresh_fields_need_no_work_and_expired_fields_queue_both_groups(self):
        page = {"items": [self.item({"view_count": 0, "comment_count": 1,
            "share_count": 2, "collect_count": 3})]}
        content_id = self.materialize(page, self.raw(page))["content_changes"][0]["content_id"]
        raw_id = self.raw({}, content_id=content_id, operation="douyin_video_statistics")
        with storage.connect(self.db) as connection, storage.transaction(connection):
            metric_observations.persist_metric_observation(connection, content_id=content_id,
                captured_at=CAPTURED, recorded_at=AT, window_key="2026-09-12",
                view_count=100, like_count=10, comment_count=None, share_count=None,
                collect_count=None, status="available", provider="TikHub", raw_response_id=raw_id,
                metadata_json='{"operation":"douyin_video_statistics"}')
        self.activation.return_value = self.active
        result = runtime.enqueue_discovered_metrics([content_id], db_path=self.db, at=AT)
        self.assertEqual(result["created"], 0)
        self.assertEqual(self.works(), [])
        self.clock = "2026-09-14T06:00:00Z"
        result = runtime.enqueue_discovered_metrics([content_id], db_path=self.db, at=self.clock)
        self.assertEqual(result["created"], 2)
        self.assertEqual({row["operation"] for row in self.works()}, {"douyin_video_statistics", "douyin_video_detail"})
        replay = runtime.enqueue_discovered_metrics([content_id], db_path=self.db, at=self.clock)
        self.assertEqual(replay["created"], 0)
        self.assertEqual(len(self.works()), 2)

    def test_no_activation_persists_links_but_does_not_invent_paid_work(self):
        page = {"items": [self.item()]}
        content_id = self.materialize(page, self.raw(page))["content_changes"][0]["content_id"]
        result = runtime.enqueue_discovered_metrics([content_id], db_path=self.db, at=AT)
        self.assertEqual(result["status"], "no_activation")
        self.assertEqual(result["created"], 0)
        self.assertEqual(self.works(), [])


if __name__ == "__main__":
    unittest.main()
