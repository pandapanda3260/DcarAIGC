from __future__ import annotations

import json
from copy import deepcopy
import sqlite3
import tempfile
import unittest
from pathlib import Path
from datetime import datetime
from unittest.mock import patch

from v8 import source_routing

from v8.metric_observations import (
    MetricObservationError, _insert_observation, persist_metric_observation,
    rebuild_metric_snapshots,
)
from v8.source_routing import (
    METRIC_FIELDS, load_policy, metric_cycle_key, metric_freshness_seconds,
    metric_refresh_due, select_content_metrics,
)
from v8.storage import connect, initialize_database, transaction


NOW = "2026-08-29T04:00:00Z"
CAPTURE = "2026-08-29T03:00:00Z"
VALUES = dict(zip(METRIC_FIELDS, (100, 10, 20, 2, 3)))


class SourceRoutingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "routing.sqlite3"
        self.connection = connect(self.db)
        initialize_database(self.connection)
        for content_id, platform in ((1, "douyin"), (2, "xiaohongshu"), (3, "douyin")):
            self.connection.execute(
                """
                INSERT INTO content_items(
                    id,link_id,platform,platform_content_id,canonical_url,published_at,
                    imported_at,created_at,updated_at
                ) VALUES (?,?,?,?,?,'2026-08-26T00:00:00Z',?,?,?)
                """,
                (content_id, f"R{content_id:05d}", platform, str(1000 + content_id),
                 f"https://example.com/{content_id}", CAPTURE, CAPTURE, CAPTURE),
            )
        self.connection.commit()

    def tearDown(self) -> None:
        self.connection.close()
        self.temp.cleanup()

    def raw(self, provider="newrank_matrix", captured_at=CAPTURE, content_id=1,
            operation=None, *, with_lineage=True, stage=None):
        normalized = str(provider or "").lower()
        operation = operation or (
            "matrix_works_list"
            if normalized == "newrank_matrix"
            else "douyin_video_statistics"
        )
        stages = {
            "aweme_list": "discovery",
            "matrix_works_list": "discovery",
            "matrix_account_list": "discovery",
            "douyin_user_posts": "discovery",
            "xiaohongshu_user_posts": "discovery",
            "douyin_discovery_metrics": "discovery",
            "xiaohongshu_discovery_metrics": "discovery",
            "douyin_video_detail": "detail",
            "xiaohongshu_note_detail": "detail",
            "xiaohongshu_video_detail": "detail",
            "douyin_video_statistics": "metrics",
            "xiaohongshu_note_statistics": "metrics",
            "douyin_openapi_video_list": "discovery",
        }
        attempt_id = None
        if normalized != "newrank_matrix" and with_lineage:
            stage = stage or stages[operation]
            slot = self.connection.execute(
                """
                INSERT INTO fetch_slots(
                    content_id,stage,window_key,provider,adapter_version,status,
                    attempt_count,created_at,updated_at
                ) VALUES (?,?,?,?,?,'succeeded',1,?,?)
                """,
                (
                    content_id,
                    stage,
                    f"fixture-{self.connection.total_changes}",
                    provider,
                    "fixture-v1",
                    captured_at,
                    captured_at,
                ),
            )
            attempt = self.connection.execute(
                """
                INSERT INTO fetch_attempts(
                    slot_id,attempt_number,request_started_at,response_finished_at,
                    http_status,billed
                ) VALUES (?,1,?,?,200,0)
                """,
                (int(slot.lastrowid), captured_at, captured_at),
            )
            attempt_id = int(attempt.lastrowid)
        cursor = self.connection.execute(
            """
            INSERT INTO provider_raw_responses(
                fetch_attempt_id,content_id,provider,operation,local_path,sha256,
                byte_size,captured_at
            ) VALUES (?,?,?,?,?,'fixture',1,?)
            """,
            (attempt_id, content_id, provider, operation,
             f"/fixture/{self.connection.total_changes}.json", captured_at),
        )
        return int(cursor.lastrowid)

    def observe(self, provider="newrank_matrix", *, captured_at=CAPTURE,
                recorded_at=NOW, content_id=1, values=None, metadata=None,
                raw_id=None, operation=None, origin="provider_capture",
                source=None, window="2026-08-29"):
        operation = operation or (
            "matrix_works_list"
            if str(provider or "").lower() == "newrank_matrix"
            else "douyin_video_statistics"
        )
        metadata = dict(metadata or {})
        metadata.setdefault("operation", operation)
        with transaction(self.connection):
            if raw_id is None and origin == "provider_capture":
                raw_id = self.raw(provider, captured_at, content_id, operation)
            return persist_metric_observation(
                self.connection, content_id=content_id, captured_at=captured_at,
                recorded_at=recorded_at, window_key=window,
                **(VALUES | (values or {})), status="available",
                provider=provider, source=source, raw_response_id=raw_id,
                metadata_json=json.dumps(metadata),
                observation_origin=origin,
            )

    def selected(self, content_id=1, cutoff=NOW):
        return select_content_metrics(self.connection, [content_id], cutoff_at=cutoff)[content_id]

    def historical(self, source="douyin", *, raw_id=None, content_id=1,
                   captured_at=CAPTURE, recorded_at=NOW):
        with transaction(self.connection):
            identity = f"link:R{content_id:05d}"
            return _insert_observation(
                self.connection, content_id=content_id, subject_key=identity,
                captured_at=captured_at, recorded_at=recorded_at,
                window_key="2026-08-29", **VALUES, status="available", source=source,
                raw_response_id=raw_id, metadata_json="{}", observation_origin="provider_capture",
                legacy_snapshot_id=None,
            )[0]

    def test_fixed_policy_and_mutation_safe_copy(self):
        policy = load_policy()
        self.assertEqual(policy["policy_version"], "source-routing-matrix-first-v2")
        self.assertEqual(policy["account_freshness_seconds"], 86400)
        self.assertEqual(policy["routes"]["douyin_metrics"]["batch_size"], 1)
        self.assertEqual(policy["routes"]["xiaohongshu_profile"]["enabled_fields"], [])
        policy["primary_provider"] = "changed"
        self.assertEqual(load_policy()["primary_provider"], "newrank_matrix")

    def test_fresh_matrix_wins_even_when_tikhub_is_newer_or_larger(self):
        self.observe(values={"view_count": 50})
        self.observe("TikHub", captured_at="2026-08-29T03:30:00Z",
                     values={"view_count": 900})
        result = self.selected()
        self.assertEqual(result["view_count"], 50)
        self.assertEqual(result["fields"]["view_count"]["effective_provider"], "newrank_matrix")
        self.assertEqual(result["source"], "douyin")
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM content_metric_snapshots").fetchone()[0], 1)

    def test_per_field_fallback_has_mixed_raw_and_original_times(self):
        first = self.observe("TikHub", captured_at="2026-08-29T02:00:00Z")
        second = self.observe(values={"view_count": None, "comment_count": 8})
        row = self.selected()
        self.assertEqual((row["view_count"], row["comment_count"]), (100, 8))
        self.assertIsNone(row["raw_response_id"])
        self.assertEqual(row["fields"]["view_count"]["observation_id"], first.observation_id)
        self.assertEqual(row["fields"]["comment_count"]["observation_id"], second.observation_id)
        self.assertEqual(row["fields"]["view_count"]["captured_at"], "2026-08-29T02:00:00Z")

    def test_missing_latest_primary_is_not_replaced_by_its_older_fresh_value(self):
        self.observe(captured_at="2026-08-29T01:00:00Z")
        self.observe(values={"view_count": None})
        row = self.selected()
        self.assertEqual(row["view_count"], 100)
        self.assertEqual(row["fields"]["view_count"]["freshness"], "stale")
        self.assertFalse(row["fields"]["view_count"]["is_latest_valid"])
        self.assertEqual(row["fields"]["view_count"]["latest_provider_status"], "missing")

    def test_not_requested_does_not_invalidate_prior_same_provider_value(self):
        first = self.observe(captured_at="2026-08-29T01:00:00Z")
        self.observe(values={"comment_count": None}, metadata={
            "fields": {"comment_count": {"status": "not_requested"}}
        })
        field = self.selected()["fields"]["comment_count"]
        self.assertEqual(field["value"], 10)
        self.assertEqual(field["observation_id"], first.observation_id)
        self.assertEqual(field["freshness"], "fresh")

    def test_missing_requested_fact_wins_over_newer_unrequested_fact(self):
        missing = self.observe(
            captured_at="2026-08-29T01:00:00Z", values={"comment_count": None}
        )
        self.observe(metadata={"requested_fields": ["view_count"]})
        field = self.selected()["fields"]["comment_count"]
        self.assertEqual(field["observation_id"], missing.observation_id)
        self.assertIsNone(field["value"])
        self.assertEqual(field["status"], "missing")
        self.assertEqual(field["latest_provider_status"], "missing")
        self.assertEqual(field["reason"], "provider_field_missing")

    def test_all_unrequested_history_preserves_newest_evidence(self):
        metadata = {"requested_fields": ["view_count"]}
        self.observe(captured_at="2026-08-29T01:00:00Z", metadata=metadata)
        latest = self.observe(metadata=metadata)
        field = self.selected()["fields"]["comment_count"]
        self.assertEqual(field["observation_id"], latest.observation_id)
        self.assertEqual(field["status"], "not_requested")
        self.assertIsNone(field["value"])
        self.assertIsNone(field["latest_provider_status"])
        self.assertFalse(field["is_latest_valid"])

    def test_zero_and_decreases_remain_valid(self):
        self.observe(captured_at="2026-08-29T01:00:00Z", values={"view_count": 1000})
        self.observe(values={"view_count": 0, "like_count": 0})
        row = self.selected()
        self.assertEqual((row["view_count"], row["like_count"]), (0, 0))
        self.assertEqual(row["fields"]["view_count"]["freshness"], "fresh")

    def test_placeholder_zero_is_operation_specific_not_all_tikhub_zero(self):
        self.observe("TikHub", operation="douyin_user_posts", values={"view_count": 0})
        field = self.selected()["fields"]["view_count"]
        self.assertIsNone(field["value"])
        self.assertEqual(field["status"], "invalid")
        self.observe("TikHub", operation="douyin_video_statistics",
                     captured_at="2026-08-29T03:30:00Z", values={"view_count": 0})
        self.assertEqual(self.selected()["view_count"], 0)
        self.assertEqual(self.selected()["fields"]["view_count"]["freshness"], "fresh")

    def test_detail_zero_is_invalid_but_statistics_zero_is_valid(self):
        self.observe(
            "TikHub", operation="douyin_video_detail", values={"view_count": 0}
        )
        detail = self.selected()["fields"]["view_count"]
        self.assertIsNone(detail["value"])
        self.assertEqual(detail["status"], "invalid")
        self.assertEqual(detail["effective_operation"], "douyin_video_detail")

        self.observe(
            "TikHub",
            operation="douyin_video_statistics",
            captured_at="2026-08-29T03:30:00Z",
            values={"view_count": 0},
        )
        statistics = self.selected()["fields"]["view_count"]
        self.assertEqual(statistics["value"], 0)
        self.assertEqual(statistics["status"], "provided")

    def test_statistics_share_is_audit_only_and_not_a_business_value(self):
        self.observe(
            "TikHub",
            operation="douyin_video_statistics",
            values={"share_count": 77},
        )
        row = self.selected()
        self.assertIsNone(row["share_count"])
        self.assertEqual(row["fields"]["share_count"]["status"], "audit_only")
        self.assertEqual(row["fields"]["share_count"]["observed_value"], 77)
        self.assertEqual(
            row["fields"]["share_count"]["reason"],
            "provider_operation_field_is_audit_only",
        )

    def test_statistics_audit_share_does_not_hide_detail_share(self):
        self.observe(
            "TikHub",
            operation="douyin_video_detail",
            captured_at="2026-08-29T02:30:00Z",
            values={"share_count": 12},
        )
        self.observe(
            "TikHub",
            operation="douyin_video_statistics",
            values={"share_count": 77},
        )
        field = self.selected()["fields"]["share_count"]
        self.assertEqual(field["value"], 12)
        self.assertEqual(field["status"], "provided")
        self.assertEqual(field["effective_operation"], "douyin_video_detail")
        self.assertEqual(field["freshness"], "fresh")

    def test_xhs_exposure_not_applicable_even_with_legacy_positive_value(self):
        self.observe(content_id=2, values={"view_count": 999})
        row = self.selected(2)
        self.assertIsNone(row["view_count"])
        self.assertEqual(row["fields"]["view_count"]["status"], "not_applicable")
        self.assertEqual(row["like_count"], 20)

    def test_late_recorded_fact_does_not_change_past_cutoff(self):
        first = self.observe(captured_at="2026-08-29T01:00:00Z",
                             recorded_at="2026-08-29T01:00:00Z")
        self.observe(captured_at="2026-08-29T02:00:00Z",
                     recorded_at="2026-08-29T05:00:00Z", values={"view_count": 222})
        self.assertEqual(self.selected()["fields"]["view_count"]["observation_id"], first.observation_id)
        self.assertEqual(self.selected(cutoff="2026-08-29T06:00:00Z")["view_count"], 222)

    def test_future_captured_fact_does_not_change_past_cutoff(self):
        self.observe()
        self.observe(captured_at="2026-08-29T05:00:00Z",
                     recorded_at="2026-08-29T06:00:00Z", values={"view_count": 222})
        self.assertEqual(self.selected()["view_count"], 100)

    def test_expired_current_valid_is_distinct_from_newer_missing(self):
        self.observe(captured_at="2026-08-28T02:00:00Z")
        field = self.selected()["fields"]["view_count"]
        self.assertEqual(field["freshness"], "stale")
        self.assertTrue(field["is_latest_valid"])
        self.assertEqual(field["freshness_reason"], "refresh_cycle_expired")

    def test_three_day_pool_and_daily_cycle_are_fixed(self):
        self.assertEqual(metric_freshness_seconds("2026-08-15T00:00:00Z", as_of=NOW), 259200)
        self.assertEqual(metric_freshness_seconds("2026-08-26T00:00:00Z", as_of=NOW), 86400)
        due = [metric_refresh_due(i, "2026-08-15T00:00:00Z", as_of=NOW) for i in (1, 2, 3)]
        self.assertEqual(sum(due), 1)
        self.assertTrue(metric_refresh_due(1, "2026-08-26T00:00:00Z", as_of=NOW))
        self.assertFalse(metric_refresh_due(1, "2026-06-01T00:00:00Z", as_of=NOW))
        self.assertEqual(metric_cycle_key(1, "2026-08-26T00:00:00Z", as_of=NOW),
                         metric_cycle_key(1, "2026-08-26T00:00:00Z", as_of="2026-08-29T10:00:00Z"))

    def test_unknown_raw_never_becomes_tikhub(self):
        self.historical()
        field = self.selected()["fields"]["view_count"]
        self.assertEqual(field["effective_provider"], "legacy_unknown")
        self.assertEqual(field["freshness"], "stale")
        self.assertFalse(field["is_latest_valid"])

    def test_wrong_raw_subject_is_unknown(self):
        with transaction(self.connection):
            raw = self.raw("TikHub", content_id=3)
        self.historical(raw_id=raw)
        self.assertEqual(self.selected()["fields"]["view_count"]["effective_provider"], "legacy_unknown")

    def test_tikhub_raw_without_attempt_slot_lineage_is_unknown(self):
        with transaction(self.connection):
            raw = self.raw("TikHub", with_lineage=False)
        self.historical(raw_id=raw)
        field = self.selected()["fields"]["view_count"]
        self.assertEqual(field["effective_provider"], "legacy_unknown")
        self.assertIsNone(field["effective_operation"])

    def test_new_tikhub_fact_without_attempt_slot_lineage_is_rejected(self):
        with transaction(self.connection):
            raw = self.raw("TikHub", with_lineage=False)
        with self.assertRaisesRegex(
            MetricObservationError, "requires verified Matrix or TikHub raw"
        ):
            self.observe("TikHub", raw_id=raw)

    def test_legacy_origin_with_proven_raw_keeps_provider_and_operation(self):
        with transaction(self.connection):
            raw = self.raw("TikHub", operation="douyin_video_detail")
        result = self.observe(
            provider=None,
            source="migrated_historical",
            origin="legacy_snapshot_baseline",
            raw_id=raw,
            operation="douyin_video_detail",
        )
        self.assertGreater(result.observation_id, 0)
        field = self.selected()["fields"]["view_count"]
        self.assertEqual(field["effective_provider"], "tikhub")
        self.assertEqual(field["effective_operation"], "douyin_video_detail")

    def test_operation_or_slot_conflict_is_legacy_unknown(self):
        with transaction(self.connection):
            raw = self.raw("TikHub", operation="douyin_video_detail")
            self.connection.execute(
                "UPDATE fetch_slots SET stage='comments' WHERE id=("
                "SELECT a.slot_id FROM provider_raw_responses r "
                "JOIN fetch_attempts a ON a.id=r.fetch_attempt_id WHERE r.id=?)",
                (raw,),
            )
        self.historical(raw_id=raw)
        field = self.selected()["fields"]["view_count"]
        self.assertEqual(field["effective_provider"], "legacy_unknown")
        self.assertIsNone(field["effective_operation"])

    def test_tikhub_detail_media_refresh_has_exact_raw_lineage(self):
        cases = (
            (1, "douyin_video_detail", "view_count"),
            (2, "xiaohongshu_note_detail", "comment_count"),
            (2, "xiaohongshu_video_detail", "comment_count"),
        )
        for content_id, operation, field_name in cases:
            with self.subTest(operation=operation), transaction(self.connection):
                raw = self.raw(
                    "TikHub",
                    content_id=content_id,
                    operation=operation,
                    stage="media_source_refresh",
                )
            self.observe(
                "TikHub",
                content_id=content_id,
                raw_id=raw,
                operation=operation,
            )
            field = self.selected(content_id)["fields"][field_name]
            self.assertEqual(field["effective_provider"], "tikhub")
            self.assertEqual(field["effective_operation"], operation)

    def test_tikhub_media_refresh_does_not_relax_operation_entity_or_stage(self):
        with transaction(self.connection):
            valid_raw = self.raw(
                "TikHub",
                content_id=2,
                operation="xiaohongshu_note_detail",
                stage="media_source_refresh",
            )
            unknown_operation_raw = self.raw(
                "TikHub",
                content_id=2,
                operation="unknown_detail_operation",
                stage="media_source_refresh",
            )
            wrong_stage_raw = self.raw(
                "TikHub",
                content_id=2,
                operation="xiaohongshu_note_detail",
                stage="comments",
            )
        rejected = (
            (valid_raw, 1, "xiaohongshu_note_detail"),
            (valid_raw, 2, "douyin_video_detail"),
            (unknown_operation_raw, 2, "unknown_detail_operation"),
            (wrong_stage_raw, 2, "xiaohongshu_note_detail"),
        )
        for raw_id, content_id, operation in rejected:
            with self.subTest(raw_id=raw_id, content_id=content_id, operation=operation):
                with self.assertRaisesRegex(
                    MetricObservationError,
                    "requires verified Matrix or TikHub raw",
                ):
                    self.observe(
                        "TikHub",
                        content_id=content_id,
                        raw_id=raw_id,
                        operation=operation,
                    )

    def test_legacy_platform_source_replay_keeps_hash_and_recording_time(self):
        with transaction(self.connection):
            raw = self.raw("TikHub")
        original = self.historical(raw_id=raw)
        before = dict(self.connection.execute("SELECT * FROM content_metric_observations").fetchone())
        replay = self.observe("TikHub", raw_id=raw, values={"view_count": 999},
                              recorded_at="2026-08-29T05:00:00Z")
        self.assertEqual(replay.observation_id, original)
        self.assertFalse(replay.observation_created)
        after = dict(self.connection.execute("SELECT * FROM content_metric_observations").fetchone())
        self.assertEqual(before, after)
        self.assertEqual(self.selected()["view_count"], 100)
        self.assertEqual(self.selected()["fields"]["view_count"]["effective_provider"], "tikhub")

    def test_unknown_and_new_openapi_raw_cannot_create_new_provider_fact(self):
        for provider in ("Rnote", "DouyinOpenAPI"):
            with self.subTest(provider=provider):
                with self.assertRaises(MetricObservationError):
                    self.observe(provider)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM content_metric_observations").fetchone()[0], 0)

    def test_existing_openapi_raw_can_replay_only_as_stale_history(self):
        with transaction(self.connection):
            raw = self.raw("DouyinOpenAPI", operation="aweme_list")
        first = self.historical(raw_id=raw)
        replay = self.observe("DouyinOpenAPI", raw_id=raw)
        self.assertEqual(replay.observation_id, first)
        field = self.selected()["fields"]["view_count"]
        self.assertEqual(field["effective_provider"], "douyin_openapi")
        self.assertEqual(field["freshness"], "stale")

    def test_correction_cutoff_replay_and_field_scope(self):
        first = self.observe("TikHub", values={"view_count": 0})
        original = self.connection.execute("SELECT * FROM content_metric_observations").fetchone()
        metadata = {"correction": {
            "target_observation_id": first.observation_id,
            "rule_id": "invalid-discovery-exposure-v1",
            "action": "invalidate", "fields": ["view_count"],
        }}
        correction = self.observe(
            "TikHub", raw_id=original["raw_response_id"], origin="system_correction",
            recorded_at="2026-08-29T05:00:00Z",
            values={"view_count": None, "comment_count": 999}, metadata=metadata,
        )
        self.assertEqual(self.selected()["view_count"], 0)
        after = self.selected(cutoff="2026-08-29T06:00:00Z")
        self.assertIsNone(after["view_count"])
        self.assertEqual(after["comment_count"], 10)
        self.assertEqual(after["fields"]["comment_count"]["observation_id"], first.observation_id)
        self.assertEqual(after["fields"]["view_count"]["observation_id"], correction.observation_id)
        replay = self.observe("TikHub", raw_id=original["raw_response_id"],
                              recorded_at="2026-08-29T06:00:00Z", values={"view_count": 0})
        self.assertFalse(replay.observation_created)
        self.assertIsNone(self.selected(cutoff="2026-08-29T06:00:00Z")["view_count"])
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM content_metric_observations").fetchone()[0], 2)

    def test_correction_requires_explicit_target_and_stable_rule(self):
        self.observe()
        with self.assertRaisesRegex(MetricObservationError, "requires target"):
            self.observe(origin="system_correction", metadata={"repair_reason": "old"})

    def test_invalidated_audit_only_fact_retains_observed_evidence(self):
        first = self.observe("TikHub", values={"share_count": 17})
        raw = self.connection.execute(
            "SELECT raw_response_id FROM content_metric_observations WHERE id=?",
            (first.observation_id,),
        ).fetchone()[0]
        correction = self.observe(
            "TikHub", raw_id=raw, origin="system_correction",
            values={"share_count": 17}, metadata={"correction": {
                "target_observation_id": first.observation_id,
                "rule_id": "audit-evidence-invalid-v1",
                "action": "invalidate", "fields": ["share_count"],
            }},
        )
        field = self.selected()["fields"]["share_count"]
        self.assertEqual(field["observation_id"], correction.observation_id)
        self.assertEqual(field["status"], "invalid")
        self.assertIsNone(field["value"])
        self.assertEqual(field["observed_value"], 17)
        self.assertEqual(field["latest_provider_status"], "invalid")
        self.assertEqual(field["reason"], "correction:audit-evidence-invalid-v1")

    def test_legacy_snapshot_rebuild_preserves_id_and_immutable_observation(self):
        result = self.observe(
            provider=None, source="migrated_historical",
            origin="legacy_snapshot_baseline", content_id=2,
        )
        before = dict(self.connection.execute("SELECT * FROM content_metric_observations").fetchone())
        with transaction(self.connection):
            rebuilt = rebuild_metric_snapshots(self.connection, [2], cutoff_at=NOW)
        after = dict(self.connection.execute("SELECT * FROM content_metric_observations").fetchone())
        snapshot = self.connection.execute("SELECT * FROM content_metric_snapshots").fetchone()
        self.assertEqual(before, after)
        self.assertEqual(snapshot["id"], result.snapshot_id)
        self.assertEqual(before["legacy_snapshot_id"], result.snapshot_id)
        self.assertEqual(rebuilt["snapshot_ids"], [result.snapshot_id])
        self.assertIsNone(snapshot["view_count"])
        self.assertEqual(snapshot["source"], "xiaohongshu")
        self.assertEqual(snapshot["status"], "stale")

    def test_snapshot_only_fallback_is_stale_and_never_used_for_cutoff(self):
        self.connection.execute(
            """
            INSERT INTO content_metric_snapshots(content_id,captured_at,window_key,view_count,status,source)
            VALUES (1,'2000-01-01T00:00:00Z','legacy',123,'available','douyin')
            """
        )
        self.connection.commit()
        self.assertEqual(select_content_metrics(self.connection, [1], cutoff_at=NOW), {})
        field = select_content_metrics(self.connection, [1])[1]["fields"]["view_count"]
        self.assertEqual(field["value"], 123)
        self.assertEqual(field["freshness"], "stale")
        self.assertEqual(field["effective_provider"], "legacy_unknown")

    def test_batch_selection_respects_runtime_sqlite_variable_limit(self):
        self.observe()
        self.observe(content_id=2)
        previous = self.connection.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 4)
        try:
            selected = select_content_metrics(self.connection, [1, 2, 3], cutoff_at=NOW)
            self.assertEqual(set(selected), {1, 2})
        finally:
            self.connection.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, previous)

    def test_streamed_current_metrics_preserve_history_and_connection_factory(self):
        older = self.observe(captured_at="2026-08-29T01:00:00Z")
        self.observe(values={"view_count": None, "comment_count": None})
        self.observe(content_id=2, values={"comment_count": 24})
        self.connection.execute(
            "INSERT INTO content_metric_snapshots("
            "content_id,captured_at,window_key,view_count,comment_count,status,source) "
            "VALUES (3,?,'legacy',123,7,'available','douyin')", (CAPTURE,)
        )
        self.connection.commit()
        original_factory = self.connection.row_factory
        previous_limit = self.connection.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 4)
        try:
            with patch.object(source_routing, "datetime") as clock:
                clock.now.return_value = datetime.fromisoformat(NOW.replace("Z", "+00:00"))
                clock.fromisoformat.side_effect = datetime.fromisoformat
                for fields in (("view_count", "comment_count"), METRIC_FIELDS):
                    with self.subTest(fields=fields):
                        selected = select_content_metrics(
                            self.connection, [1, 2, 3], metric_fields=fields
                        )
                        self.assertEqual(set(selected), {1, 2, 3})
                        self.assertEqual(selected[1]["view_count"], 100)
                        self.assertEqual(selected[1]["comment_count"], 10)
                        self.assertEqual(selected[1]["fields"]["view_count"]["observation_id"], older.observation_id)
                        self.assertEqual(selected[1]["fields"]["view_count"]["freshness"], "stale")
                        self.assertEqual(selected[1]["fields"]["view_count"]["latest_provider_status"], "missing")
                        self.assertIsNone(selected[2]["view_count"])
                        self.assertEqual(selected[2]["comment_count"], 24)
                        self.assertEqual(selected[3]["view_count"], 123)
                        self.assertEqual(selected[3]["comment_count"], 7)
                        self.assertEqual(selected[3]["fields"]["view_count"]["freshness"], "stale")
                        self.assertIs(self.connection.row_factory, original_factory)
                        self.assertIsInstance(self.connection.execute("SELECT id FROM content_items LIMIT 1").fetchone(), sqlite3.Row)
        finally:
            self.connection.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, previous_limit)

    def test_observation_join_column_names_are_unique_ignoring_case(self):
        self.observe()
        queries = []
        self.connection.set_trace_callback(queries.append)
        try:
            self.selected()
        finally:
            self.connection.set_trace_callback(None)
        observation_queries = [query for query in queries if "SELECT o.*,r.id raw_id" in query]
        self.assertEqual(len(observation_queries), 1)
        cursor = self.connection.execute(observation_queries[0])
        names = [column[0].casefold() for column in cursor.description]
        self.assertEqual(len(names), len(set(names)))
        self.assertIn("account_id", names)
        self.assertIn("content_id", names)
        self.assertIsInstance(cursor.fetchone(), sqlite3.Row)

    def test_metric_field_projection_matches_full_selection(self):
        self.observe(values={"comment_count": 8, "like_count": 21})
        full = self.selected()
        projected = select_content_metrics(
            self.connection,
            [1],
            cutoff_at=NOW,
            metric_fields=("comment_count", "view_count", "comment_count"),
        )[1]

        self.assertEqual(projected["view_count"], full["view_count"])
        self.assertEqual(projected["comment_count"], full["comment_count"])
        self.assertEqual(projected["fields"]["view_count"], full["fields"]["view_count"])
        self.assertEqual(projected["fields"]["comment_count"], full["fields"]["comment_count"])
        self.assertNotIn("like_count", projected)
        self.assertEqual(
            set(json.loads(projected["metadata_json"])["fields"]),
            {"view_count", "comment_count"},
        )

        with self.assertRaisesRegex(ValueError, "must include view_count"):
            select_content_metrics(self.connection, [1], metric_fields=("comment_count",))
        with self.assertRaisesRegex(ValueError, "unsupported metric fields"):
            select_content_metrics(self.connection, [1], metric_fields=("view_count", "bogus"))

    def test_current_read_never_leaks_future_recorded_fact_via_snapshot(self):
        self.observe(recorded_at="2099-01-01T00:00:00Z", values={"view_count": 999})
        self.assertEqual(select_content_metrics(self.connection, [1]), {})

    def test_legacy_baseline_replay_and_missing_projection_keep_original_id(self):
        first = self.observe(provider=None, origin="legacy_snapshot_baseline",
                             source="migrated_historical")
        original = dict(self.connection.execute("SELECT * FROM content_metric_observations").fetchone())
        replay = self.observe(provider=None, origin="legacy_snapshot_baseline",
                              source="migrated_historical", recorded_at="2026-08-29T05:00:00Z")
        self.assertFalse(replay.observation_created)
        self.assertEqual(replay.snapshot_id, first.snapshot_id)
        self.assertEqual(dict(self.connection.execute("SELECT * FROM content_metric_observations").fetchone()), original)
        self.connection.execute("DELETE FROM content_metric_snapshots")
        self.connection.commit()
        with transaction(self.connection):
            result = rebuild_metric_snapshots(self.connection, [1], cutoff_at=NOW)
        self.assertEqual(result["snapshot_ids"], [first.snapshot_id])
        self.assertEqual(dict(self.connection.execute("SELECT * FROM content_metric_observations").fetchone()), original)

    def test_invalid_numeric_values_do_not_become_zero_or_boolean_one(self):
        self.observe(values={"view_count": True, "like_count": -1, "share_count": 1.5})
        row = self.selected()
        for field in ("view_count", "like_count", "share_count"):
            self.assertIsNone(row[field])
            self.assertEqual(row["fields"][field]["status"], "invalid")
        self.assertEqual(row["comment_count"], 10)

    def test_stable_correction_replay_and_drift_rejection(self):
        first = self.observe()
        raw = self.connection.execute("SELECT raw_response_id FROM content_metric_observations").fetchone()[0]
        metadata = {"correction": {
            "target_observation_id": first.observation_id, "rule_id": "parser-repair-v1",
            "action": "replace", "fields": ["view_count"],
        }}
        corrected = self.observe(raw_id=raw, origin="system_correction", metadata=metadata,
                                 values={"view_count": 0})
        replay = self.observe(raw_id=raw, origin="system_correction", metadata=metadata,
                              values={"view_count": 0}, recorded_at="2026-08-29T05:00:00Z")
        self.assertEqual(corrected.observation_id, replay.observation_id)
        self.assertFalse(replay.observation_created)
        self.assertEqual(self.selected()["view_count"], 0)
        with self.assertRaisesRegex(MetricObservationError, "stable correction rule"):
            self.observe(raw_id=raw, origin="system_correction", metadata=metadata,
                         values={"view_count": 1})

    def test_correction_cannot_change_original_capture_time_or_wrong_window(self):
        first = self.observe()
        raw = self.connection.execute("SELECT raw_response_id FROM content_metric_observations").fetchone()[0]
        metadata = {"correction": {
            "target_observation_id": first.observation_id, "rule_id": "parser-repair-v1",
            "action": "invalidate", "fields": ["view_count"],
        }}
        with self.assertRaisesRegex(MetricObservationError, "original capture"):
            self.observe(raw_id=raw, origin="system_correction", metadata=metadata,
                         captured_at="2026-08-29T03:30:00Z", values={"view_count": None})
        with self.assertRaisesRegex(MetricObservationError, "match content and window"):
            self.observe(raw_id=raw, origin="system_correction", metadata=metadata,
                         window="2026-08-28", values={"view_count": None})


class SourceRoutingHistoryTest(unittest.TestCase):
    @staticmethod
    def observation(identifier, provider, *, captured_at=CAPTURE, values=None,
                    metadata=None, operation=None):
        operation = operation or {
            "newrank_matrix": "matrix_works_list",
            "tikhub": "douyin_video_statistics",
            "douyin_openapi": "aweme_list",
            "legacy_unknown": "unknown",
        }[provider]
        proven = provider != "legacy_unknown"
        return {
            "id": identifier, "content_id": 1, "account_id": None,
            "captured_at": captured_at, "recorded_at": NOW,
            "window_key": "history", "status": "available", "source": provider,
            "raw_response_id": identifier if proven else None,
            "raw_id": identifier if proven else None,
            "raw_provider": provider if proven else None,
            "raw_operation": operation, "raw_content_id": 1,
            "raw_account_id": None, "observation_origin": "provider_capture",
            "observation_sha256": str(identifier),
            "metadata_json": json.dumps({"operation": operation, **(metadata or {})}),
            **(VALUES | (values or {})),
        }

    def select(self, observations):
        return source_routing._select_row(
            {"id": 1, "platform": "douyin", "published_at": "2026-08-26T00:00:00Z"},
            deepcopy(observations), cutoff_at=NOW,
        )

    def test_long_history_matches_relevant_facts_for_every_provider(self):
        relevant = [
            self.observation(6004, "newrank_matrix", values={"comment_count": None}),
            self.observation(6003, "tikhub", values={"comment_count": None},
                             operation="douyin_video_detail"),
            self.observation(6002, "legacy_unknown", values={"comment_count": 73}),
            self.observation(6001, "douyin_openapi", values={"comment_count": 91}),
        ]
        providers = ("newrank_matrix", "tikhub", "legacy_unknown", "douyin_openapi")
        older = [
            self.observation(identifier, providers[identifier % len(providers)],
                             captured_at="2026-08-28T03:00:00Z",
                             values={field: 9999 for field in METRIC_FIELDS})
            for identifier in range(1, 5001)
        ]
        expected = self.select(relevant)
        actual = self.select(relevant + older)
        self.assertEqual(actual, expected)
        self.assertEqual(actual["fields"]["view_count"]["effective_provider"], "newrank_matrix")
        self.assertEqual(actual["comment_count"], 73)
        self.assertEqual(actual["fields"]["comment_count"]["effective_provider"], "legacy_unknown")
        self.assertEqual(actual["fields"]["comment_count"]["latest_provider_status"], "provided")
        self.assertEqual(actual["fields"]["comment_count"]["freshness"], "stale")

    def test_long_unrequested_history_does_not_hide_older_primary_fact(self):
        newest = [
            self.observation(6002, "tikhub", values={"comment_count": None}),
            self.observation(6001, "legacy_unknown", values={"comment_count": 73}),
        ]
        unrequested = [
            self.observation(identifier, "newrank_matrix",
                             metadata={"requested_fields": ["view_count"]})
            for identifier in range(2, 5002)
        ]
        primary = self.observation(1, "newrank_matrix", values={"comment_count": 37})
        expected = self.select(newest + [unrequested[-1], primary])
        actual = self.select(newest + unrequested + [primary])
        self.assertEqual(actual, expected)
        self.assertEqual(actual["comment_count"], 37)
        self.assertEqual(actual["fields"]["comment_count"]["effective_provider"], "newrank_matrix")
        self.assertEqual(actual["fields"]["comment_count"]["freshness"], "fresh")

    def test_long_audit_only_history_does_not_hide_older_detail_fact(self):
        primary = self.observation(6001, "newrank_matrix", values={"share_count": None})
        audit = [
            self.observation(identifier, "tikhub", values={"share_count": 9999})
            for identifier in range(2, 5002)
        ]
        detail = self.observation(1, "tikhub", operation="douyin_video_detail",
                                  values={"share_count": 19})
        expected = self.select([primary, audit[-1], detail])
        actual = self.select([primary, *audit, detail])
        self.assertEqual(actual, expected)
        self.assertEqual(actual["share_count"], 19)
        self.assertEqual(actual["fields"]["share_count"]["effective_operation"], "douyin_video_detail")
        self.assertEqual(actual["fields"]["share_count"]["freshness"], "fresh")


if __name__ == "__main__":
    unittest.main()
