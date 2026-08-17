from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

import v8.capture as capture_module
from v8.capture import ProviderResult
from v8.evaluation import evaluate_content, incremental_candidates
from v8.matcher_dsl import POINT_IDS, POINT_SCENES
from v8.media_state import MediaTerminalDetail
from v8.operations import upsert_account, upsert_content
from v8.range_backfill import (
    RangeBackfillError,
    _process_content_batch,
    pending_content_ids,
    run_content_backfill,
    run_discovery_backfill,
    run_local_evidence_backfill,
    summarize_range_status,
    tag_history_scopes,
)
from v8.scheduler import run_media_cutoff
from v8.storage import (
    HISTORY_ARCHIVE_SOURCE_GROUP,
    HISTORY_BACKFILL_SOURCE_GROUP,
    connect,
    initialize_database,
    now_utc,
)
from v8.taxonomy_rule_backfill import backfill_v5_1_matcher_rules


SHANGHAI = ZoneInfo("Asia/Shanghai")


class V8HistoryBackfillScopeTest(unittest.TestCase):
    """全量历史回溯的分组标记、防洪闸门与批处理语义。"""

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "history.sqlite3"
        self.state = self.root / "state"
        self.raw_root = self.root / "raw"
        raw_root_patch = patch.object(capture_module, "RAW_ROOT", self.raw_root)
        raw_root_patch.start()
        self.addCleanup(raw_root_patch.stop)
        with connect(self.db) as connection:
            initialize_database(connection)
            captured_at = now_utc()
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id, version, status, definition, created_at, published_at
                ) VALUES ('taxonomy', 'selling-points-v5.0', 'published', 'test', ?, ?)
                """,
                (captured_at, captured_at),
            )
            for code in sorted(POINT_IDS):
                point = connection.execute(
                    """
                    INSERT INTO selling_points(
                        taxonomy_id,code,tier,label,definition,matcher_rule_json
                    ) VALUES ('taxonomy',?,'other',?,?,'{}')
                    """,
                    (code, f"卖点 {code}", f"定义 {code}"),
                )
                for scene in sorted(POINT_SCENES[code]):
                    connection.execute(
                        """
                        INSERT INTO selling_point_scenes(selling_point_id,scene)
                        VALUES (?,?)
                        """,
                        (point.lastrowid, scene),
                    )
            connection.commit()
        matcher = backfill_v5_1_matcher_rules(db_path=self.db)
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE taxonomy_versions SET status='retired'
                WHERE version='selling-points-v5.0'
                """
            )
            connection.execute(
                """
                UPDATE taxonomy_versions SET status='published',published_at=?
                WHERE version='selling-points-v5.1'
                """,
                (now_utc(),),
            )
            connection.execute(
                """
                INSERT INTO evaluation_releases(
                    id,rule_version,taxonomy_version,matcher_rule_sha256,status,
                    created_at,updated_at,activated_at
                ) VALUES ('evaluation-v8__selling-points-v5.1','evaluation-v8',
                          'selling-points-v5.1',?,'active',?,?,?)
                """,
                (matcher["matcher_rule_sha256"], now_utc(), now_utc(), now_utc()),
            )
            connection.commit()
        upsert_account(
            {
                "phone": "13800138000",
                "platforms": [
                    {"platform": "douyin", "uid": "99887766", "nickname": "汽车号"},
                ],
            },
            db_path=self.db,
        )
        self.start = datetime(2010, 1, 1, 0, 0, tzinfo=SHANGHAI)
        self.end = datetime(2026, 8, 7, 0, 0, tzinfo=SHANGHAI)
        self.archive_before = datetime(2026, 2, 7, 0, 0, tzinfo=SHANGHAI)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _insert_content(self, content_id: str, published_at: str) -> int:
        result = upsert_content(
            {
                "platform": "douyin",
                "platform_content_id": content_id,
                "canonical_url": f"https://www.douyin.com/video/{content_id}",
                "title": "汽车保养知识",
                "body": "教你判断刹车故障",
                "published_at": published_at,
                "content_type": "video",
                "account_uid": "99887766",
                "account_name": "汽车号",
            },
            db_path=self.db,
        )
        return int(result["id"])

    def _source_group(self, content_id: int) -> str:
        with connect(self.db) as connection:
            row = connection.execute(
                "SELECT source_group FROM content_items WHERE id=?", (content_id,)
            ).fetchone()
        return str(row["source_group"])

    def test_tag_history_scopes_segments_and_protects_existing_rows(self) -> None:
        archived = self._insert_content("111111111", "2025-06-01T01:00:00Z")
        pending = self._insert_content("222222222", "2026-08-01T01:00:00Z")
        evaluated = self._insert_content("333333333", "2025-05-01T01:00:00Z")
        evaluate_content(evaluated, db_path=self.db)
        manual = self._insert_content("444444444", "2025-04-01T01:00:00Z")
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET source_group='manual-import' WHERE id=?",
                (manual,),
            )
            connection.commit()

        dry = tag_history_scopes(
            start=self.start, end=self.end, archive_before=self.archive_before,
            db_path=self.db,
        )
        self.assertEqual(dry["status"], "dry_run")
        self.assertEqual(
            dry["segments"][HISTORY_ARCHIVE_SOURCE_GROUP]["candidates"], 1
        )
        self.assertEqual(
            dry["segments"][HISTORY_BACKFILL_SOURCE_GROUP]["candidates"], 1
        )
        self.assertEqual(self._source_group(archived), "")

        applied = tag_history_scopes(
            start=self.start, end=self.end, archive_before=self.archive_before,
            db_path=self.db, apply_changes=True,
        )
        self.assertEqual(
            applied["segments"][HISTORY_ARCHIVE_SOURCE_GROUP]["applied"], 1
        )
        self.assertEqual(
            applied["segments"][HISTORY_BACKFILL_SOURCE_GROUP]["applied"], 1
        )
        self.assertEqual(self._source_group(archived), HISTORY_ARCHIVE_SOURCE_GROUP)
        self.assertEqual(self._source_group(pending), HISTORY_BACKFILL_SOURCE_GROUP)
        self.assertEqual(self._source_group(evaluated), "")
        self.assertEqual(self._source_group(manual), "manual-import")

        repeated = tag_history_scopes(
            start=self.start, end=self.end, archive_before=self.archive_before,
            db_path=self.db, apply_changes=True,
        )
        self.assertEqual(
            repeated["segments"][HISTORY_ARCHIVE_SOURCE_GROUP]["candidates"], 0
        )
        self.assertEqual(
            repeated["segments"][HISTORY_BACKFILL_SOURCE_GROUP]["candidates"], 0
        )

    def test_incremental_candidates_skip_tagged_until_cleared(self) -> None:
        tagged = self._insert_content("555555555", "2025-06-01T01:00:00Z")
        normal = self._insert_content("666666666", "2026-08-01T01:00:00Z")
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET source_group=? WHERE id=?",
                (HISTORY_ARCHIVE_SOURCE_GROUP, tagged),
            )
            connection.commit()
        self.assertEqual(incremental_candidates(db_path=self.db), [normal])
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET source_group='' WHERE id=?", (tagged,)
            )
            connection.commit()
        self.assertEqual(
            incremental_candidates(db_path=self.db), sorted([tagged, normal])
        )

    def test_media_cutoff_ignores_history_scoped_ingest(self) -> None:
        _fresh = self._insert_content("777777777", "2026-08-01T10:00:00Z")
        archived = self._insert_content("888888888", "2020-05-01T01:00:00Z")
        backfilled = self._insert_content("999999999", "2026-07-01T01:00:00Z")
        with connect(self.db) as connection:
            for content_id, group in (
                (archived, HISTORY_ARCHIVE_SOURCE_GROUP),
                (backfilled, HISTORY_BACKFILL_SOURCE_GROUP),
            ):
                connection.execute(
                    "UPDATE content_items SET source_group=? WHERE id=?",
                    (group, content_id),
                )
            connection.execute(
                "UPDATE content_items SET imported_at='2026-08-02T01:00:00Z'"
            )
            connection.commit()
        result = run_media_cutoff(
            datetime(2026, 8, 2, 7, 30, tzinfo=SHANGHAI), db_path=self.db
        )
        self.assertEqual(result["candidates"], 1)
        self.assertEqual(result["state_counts"]["pending"], 1)
        with connect(self.db) as connection:
            queue = connection.execute(
                """
                SELECT content_id FROM review_queue
                WHERE reason_code='media_processing_incomplete'
                """
            ).fetchall()
        self.assertEqual(queue, [])

    def test_discovery_backfill_tags_scopes_with_workers_and_compact(self) -> None:
        preexisting_unevaluated = self._insert_content(
            "454545454", "2025-03-01T01:00:00Z"
        )

        def discovery_call(operation, identity):
            if operation == "resolve_account":
                return ProviderResult(
                    {"reference": "MS4wLjAB" + "x" * 40},
                    {"reference": "profile"},
                    200,
                    True,
                )
            items = [
                {
                    "platform": "douyin",
                    "platform_content_id": "121212121",
                    "canonical_url": "https://www.douyin.com/video/121212121",
                    "title": "近月内容", "body": "汽车内容",
                    "published_at": "2026-08-01T01:00:00Z",
                    "content_type": "video",
                    "media_urls": ["https://media.example.com/video.mp4"],
                    "account_uid": "99887766", "account_name": "汽车号",
                },
                {
                    "platform": "douyin",
                    "platform_content_id": "343434343",
                    "canonical_url": "https://www.douyin.com/video/343434343",
                    "title": "远古内容", "body": "汽车内容",
                    "published_at": "2024-03-01T01:00:00Z",
                    "content_type": "video",
                    "account_uid": "99887766", "account_name": "汽车号",
                },
            ]
            return ProviderResult(
                {"items": items, "next_cursor": None, "has_more": False},
                {"items": items},
                200,
                True,
            )

        result = run_discovery_backfill(
            start=self.start, end=self.end,
            task_id="full-history-test", max_amount=1.0,
            db_path=self.db, platforms=["douyin"],
            call_override=discovery_call, state_root=self.state,
            archive_before=self.archive_before, workers=2, compact=True,
            require_live_detail=True,
        )
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["accounts_completed"], 1)
        self.assertEqual(result["inserted"], 2)
        self.assertIn("history_scopes", result)
        self.assertEqual(result["content_manifest"]["first_inserted"], 2)
        self.assertEqual(result["history_scopes"]["restricted_content_ids"], 2)
        self.assertEqual(
            result["history_scopes"]["segments"][HISTORY_ARCHIVE_SOURCE_GROUP][
                "applied"
            ],
            0,
        )
        account_summary = result["results"][0]
        self.assertNotIn("pages", account_summary)
        self.assertEqual(account_summary["pages_processed"], 1)
        self.assertTrue(account_summary["completed"])
        self.assertEqual(account_summary["completion_reason"], "provider_exhausted")
        with connect(self.db) as connection:
            groups = {
                str(row["platform_content_id"]): str(row["source_group"])
                for row in connection.execute(
                    "SELECT platform_content_id, source_group FROM content_items"
                )
            }
        self.assertEqual(groups["121212121"], HISTORY_BACKFILL_SOURCE_GROUP)
        self.assertEqual(groups["343434343"], HISTORY_ARCHIVE_SOURCE_GROUP)
        self.assertEqual(self._source_group(preexisting_unevaluated), "")
        with connect(self.db) as connection:
            derived_detail_slots = connection.execute(
                """
                SELECT COUNT(*) FROM fetch_slots
                WHERE content_id=(
                    SELECT id FROM content_items WHERE platform_content_id='121212121'
                ) AND stage='detail' AND window_key='lifetime'
                """
            ).fetchone()[0]
        self.assertEqual(int(derived_detail_slots), 0)
        manifest = json.loads(
            Path(result["content_manifest"]["path"]).read_text(encoding="utf-8")
        )
        self.assertEqual(len(manifest["contents"]), 2)
        self.assertEqual(
            {entry["first_action"] for entry in manifest["contents"]},
            {"inserted"},
        )

    def test_local_evidence_tagged_only_clears_tag_on_success(self) -> None:
        archived = self._insert_content("101010101", "2025-06-01T01:00:00Z")
        pending = self._insert_content("202020202", "2026-08-01T01:00:00Z")
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET source_group=? WHERE id=?",
                (HISTORY_ARCHIVE_SOURCE_GROUP, archived),
            )
            connection.execute(
                "UPDATE content_items SET source_group=? WHERE id=?",
                (HISTORY_BACKFILL_SOURCE_GROUP, pending),
            )
            connection.commit()
        with patch(
            "v8.range_backfill.process_content_media",
            return_value={"status": "evidence_ready"},
        ), patch(
            "v8.range_backfill.evaluate_content",
            return_value=SimpleNamespace(
                evaluation_id=101, created=True, evidence_level="V2"
            ),
        ) as evaluate, patch(
            "v8.range_backfill.fingerprint_content",
            return_value={"source_sha256": "fingerprint"},
        ) as fingerprint, patch(
            "v8.range_backfill.media_terminal_state_details",
            side_effect=[
                {pending: MediaTerminalDetail("pending", "evaluation_pending")},
                {pending: MediaTerminalDetail("complete", "complete")},
                {pending: MediaTerminalDetail("complete", "complete")},
            ],
        ) as terminal_states:
            result = run_local_evidence_backfill(
                start=self.start, end=self.end, task_id="local-evidence-test",
                max_amount=1.0, db_path=self.db, limit=10, state_root=self.state,
                tagged_only=True,
            )
        self.assertEqual(result["candidates"], 1)
        self.assertEqual(result["tags_cleared"], 1)
        self.assertEqual(self._source_group(pending), "")
        self.assertEqual(self._source_group(archived), HISTORY_ARCHIVE_SOURCE_GROUP)
        self.assertEqual(evaluate.call_count, 1)
        self.assertEqual(fingerprint.call_count, 1)
        self.assertEqual(terminal_states.call_count, 3)
        self.assertEqual(result["results"][0]["status"], "complete")
        with patch(
            "v8.range_backfill.process_content_media",
            return_value={"status": "evidence_ready"},
        ), patch(
            "v8.range_backfill.fingerprint_content",
            return_value={"source_sha256": "fingerprint"},
        ):
            again = run_local_evidence_backfill(
                start=self.start, end=self.end, task_id="local-evidence-test",
                max_amount=1.0, db_path=self.db, limit=10, state_root=self.state,
                tagged_only=True,
            )
        self.assertEqual(again["candidates"], 0)

    def test_local_evidence_terminal_insufficient_clears_tag_idempotently(self) -> None:
        pending = self._insert_content("212121213", "2026-08-01T01:00:00Z")
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET source_group=? WHERE id=?",
                (HISTORY_BACKFILL_SOURCE_GROUP, pending),
            )
            connection.commit()

        with patch(
            "v8.range_backfill.process_content_media",
            return_value={"status": "evidence_ready"},
        ), patch(
            "v8.range_backfill.evaluate_content",
            return_value=SimpleNamespace(
                evaluation_id=102, created=True, evidence_level="V1"
            ),
        ), patch(
            "v8.range_backfill.media_terminal_state_details",
            side_effect=[
                {pending: MediaTerminalDetail("pending", "evaluation_pending")},
                {
                    pending: MediaTerminalDetail(
                        "terminal_insufficient", "terminal_insufficient"
                    )
                },
                {
                    pending: MediaTerminalDetail(
                        "terminal_insufficient", "terminal_insufficient"
                    )
                },
            ],
        ) as terminal_states, patch(
            "v8.range_backfill.fingerprint_content"
        ) as fingerprint:
            result = run_local_evidence_backfill(
                start=self.start,
                end=self.end,
                task_id="local-evidence-v1",
                max_amount=1.0,
                db_path=self.db,
                limit=10,
                state_root=self.state,
                tagged_only=True,
            )

        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["failed"], 0)
        self.assertEqual(result["tags_cleared"], 1)
        self.assertEqual(self._source_group(pending), "")
        self.assertEqual(result["results"][0]["status"], "terminal_insufficient")
        self.assertEqual(result["results"][0]["evaluation_evidence_level"], "V1")
        self.assertEqual(terminal_states.call_count, 3)
        fingerprint.assert_not_called()
        again = run_local_evidence_backfill(
            start=self.start,
            end=self.end,
            task_id="local-evidence-v1",
            max_amount=1.0,
            db_path=self.db,
            limit=10,
            state_root=self.state,
            tagged_only=True,
        )
        self.assertEqual(again["candidates"], 0)
        self.assertEqual(again["tags_cleared"], 0)

    def test_stable_terminal_insufficient_resumes_without_reprocessing(self) -> None:
        pending = self._insert_content("212121215", "2026-08-01T01:00:00Z")
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET source_group=? WHERE id=?",
                (HISTORY_BACKFILL_SOURCE_GROUP, pending),
            )
            connection.commit()
        stable = {
            pending: MediaTerminalDetail(
                "terminal_insufficient", "terminal_insufficient"
            )
        }
        with patch(
            "v8.range_backfill.media_terminal_state_details",
            side_effect=[stable, stable],
        ) as terminal_states, patch(
            "v8.range_backfill.process_content_media"
        ) as process, patch(
            "v8.range_backfill.evaluate_content"
        ) as evaluate, patch(
            "v8.range_backfill.fingerprint_content"
        ) as fingerprint:
            result = run_local_evidence_backfill(
                start=self.start,
                end=self.end,
                task_id="local-evidence-resume-terminal-insufficient",
                max_amount=1.0,
                db_path=self.db,
                limit=10,
                state_root=self.state,
                tagged_only=True,
            )

        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["tags_cleared"], 1)
        self.assertEqual(self._source_group(pending), "")
        self.assertTrue(result["results"][0]["resumed_from_terminal_state"])
        self.assertEqual(terminal_states.call_count, 2)
        process.assert_not_called()
        evaluate.assert_not_called()
        fingerprint.assert_not_called()

    def test_terminal_state_drift_before_tag_release_fails_closed(self) -> None:
        pending = self._insert_content("212121216", "2026-08-01T01:00:00Z")
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET source_group=? WHERE id=?",
                (HISTORY_BACKFILL_SOURCE_GROUP, pending),
            )
            connection.commit()
        with patch(
            "v8.range_backfill.media_terminal_state_details",
            side_effect=[
                {
                    pending: MediaTerminalDetail(
                        "terminal_insufficient", "terminal_insufficient"
                    )
                },
                {pending: MediaTerminalDetail("pending", "source_missing")},
            ],
        ), patch("v8.range_backfill.process_content_media") as process, patch(
            "v8.range_backfill.evaluate_content"
        ) as evaluate, patch(
            "v8.range_backfill.fingerprint_content"
        ) as fingerprint:
            result = run_local_evidence_backfill(
                start=self.start,
                end=self.end,
                task_id="local-evidence-terminal-drift",
                max_amount=1.0,
                db_path=self.db,
                limit=10,
                state_root=self.state,
                tagged_only=True,
            )

        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["failed"], 1)
        self.assertEqual(result["tags_cleared"], 0)
        self.assertEqual(self._source_group(pending), HISTORY_BACKFILL_SOURCE_GROUP)
        self.assertIn("发生漂移", result["results"][0]["error"])
        process.assert_not_called()
        evaluate.assert_not_called()
        fingerprint.assert_not_called()

    def test_local_evidence_keeps_tag_when_media_source_is_missing(self) -> None:
        pending = self._insert_content("212121212", "2026-08-01T01:00:00Z")
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET source_group=? WHERE id=?",
                (HISTORY_BACKFILL_SOURCE_GROUP, pending),
            )
            connection.commit()

        result = run_local_evidence_backfill(
            start=self.start,
            end=self.end,
            task_id="local-evidence-no-source",
            max_amount=1.0,
            db_path=self.db,
            limit=10,
            state_root=self.state,
            tagged_only=True,
        )

        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["failed"], 1)
        self.assertEqual(result["tags_cleared"], 0)
        self.assertEqual(self._source_group(pending), HISTORY_BACKFILL_SOURCE_GROUP)
        with connect(self.db) as connection:
            evaluated = connection.execute(
                "SELECT COUNT(*) FROM evaluation_versions WHERE content_id=?",
                (pending,),
            ).fetchone()[0]
        self.assertEqual(int(evaluated), 0)

    def test_local_evidence_terminal_failed_keeps_tag_for_explicit_refresh(self) -> None:
        pending = self._insert_content("212121214", "2026-08-01T01:00:00Z")
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET source_group=? WHERE id=?",
                (HISTORY_BACKFILL_SOURCE_GROUP, pending),
            )
            connection.commit()

        with patch(
            "v8.range_backfill.process_content_media",
            return_value={"status": "evidence_ready"},
        ) as process, patch(
            "v8.range_backfill.evaluate_content",
            return_value=SimpleNamespace(
                evaluation_id=103, created=True, evidence_level="V1"
            ),
        ) as evaluate, patch(
            "v8.range_backfill.media_terminal_state_details",
            return_value={
                pending: MediaTerminalDetail(
                    "terminal_failed", "frames_terminal_failed"
                )
            },
        ) as terminal_states, patch(
            "v8.range_backfill.fingerprint_content"
        ) as fingerprint:
            result = run_local_evidence_backfill(
                start=self.start,
                end=self.end,
                task_id="local-evidence-terminal-failed",
                max_amount=1.0,
                db_path=self.db,
                limit=10,
                state_root=self.state,
                tagged_only=True,
            )

        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["failed"], 1)
        self.assertEqual(result["tags_cleared"], 0)
        self.assertEqual(self._source_group(pending), HISTORY_BACKFILL_SOURCE_GROUP)
        self.assertEqual(result["results"][0]["status"], "terminal_failed")
        self.assertEqual(
            result["results"][0]["media_terminal_reason"],
            "frames_terminal_failed",
        )
        self.assertEqual(terminal_states.call_count, 1)
        process.assert_not_called()
        evaluate.assert_not_called()
        fingerprint.assert_not_called()

    def test_local_evidence_release_switch_after_evaluation_keeps_tag(self) -> None:
        pending = self._insert_content("212121217", "2026-08-01T01:00:00Z")
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET source_group=? WHERE id=?",
                (HISTORY_BACKFILL_SOURCE_GROUP, pending),
            )
            connection.commit()

        def switch_release(*_args, **_kwargs):
            with connect(self.db) as connection:
                connection.execute(
                    """
                    UPDATE evaluation_releases
                    SET status='retired',retired_at=?,updated_at=?
                    WHERE status='active'
                    """,
                    (now_utc(), now_utc()),
                )
                connection.commit()
            return SimpleNamespace(
                evaluation_id=104,
                created=True,
                evidence_level="V2",
            )

        with patch(
            "v8.range_backfill.media_terminal_state_details",
            return_value={pending: MediaTerminalDetail("pending", "evaluation_pending")},
        ) as terminal_states, patch(
            "v8.range_backfill.process_content_media",
            return_value={"status": "evidence_ready"},
        ), patch(
            "v8.range_backfill.evaluate_content", side_effect=switch_release
        ), patch("v8.range_backfill.fingerprint_content") as fingerprint:
            result = run_local_evidence_backfill(
                start=self.start,
                end=self.end,
                task_id="local-evidence-release-switch",
                max_amount=1.0,
                db_path=self.db,
                limit=10,
                state_root=self.state,
                tagged_only=True,
            )

        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["failed"], 1)
        self.assertEqual(result["tags_cleared"], 0)
        self.assertEqual(self._source_group(pending), HISTORY_BACKFILL_SOURCE_GROUP)
        self.assertIn("active evaluation release", result["results"][0]["error"])
        self.assertEqual(terminal_states.call_count, 1)
        fingerprint.assert_not_called()

    def test_discovery_page_limit_and_missing_cursor_are_partial(self) -> None:
        def page_limit_call(operation, identity):
            if operation == "resolve_account":
                return ProviderResult(
                    {"reference": "MS4wLjAB" + "x" * 40},
                    {"reference": "profile"},
                    200,
                    True,
                )
            item = {
                "platform": "douyin",
                "platform_content_id": "232323232",
                "canonical_url": "https://www.douyin.com/video/232323232",
                "title": "分页内容",
                "body": "汽车内容",
                "published_at": "2026-08-01T01:00:00Z",
                "content_type": "video",
                "account_uid": "99887766",
                "account_name": "汽车号",
            }
            return ProviderResult(
                {"items": [item], "next_cursor": "next", "has_more": True},
                {"items": [item]},
                200,
                True,
            )

        limited = run_discovery_backfill(
            start=self.start,
            end=self.end,
            task_id="page-limit-test",
            max_amount=1.0,
            db_path=self.db,
            platforms=["douyin"],
            call_override=page_limit_call,
            state_root=self.state,
            max_pages_per_account=1,
        )
        self.assertEqual(limited["status"], "partial")
        self.assertEqual(limited["accounts_completed"], 0)
        self.assertEqual(limited["stopped_reason"], "page_limit_reached")

        def missing_cursor_call(operation, identity):
            result = page_limit_call(operation, identity)
            if operation == "discover_content":
                result.data["next_cursor"] = None
            return result

        missing_end = datetime(2026, 8, 6, 0, 0, tzinfo=SHANGHAI)
        missing_start = datetime(2010, 1, 2, 0, 0, tzinfo=SHANGHAI)
        missing = run_discovery_backfill(
            start=missing_start,
            end=missing_end,
            task_id="missing-cursor-test",
            max_amount=1.0,
            db_path=self.db,
            platforms=["douyin"],
            call_override=missing_cursor_call,
            state_root=self.state,
            max_pages_per_account=2,
        )
        self.assertEqual(missing["status"], "partial")
        self.assertEqual(missing["accounts_completed"], 0)
        self.assertEqual(missing["stopped_reason"], "missing_next_cursor")

        repeated_end = datetime(2026, 8, 5, 0, 0, tzinfo=SHANGHAI)

        def repeated_cursor_call(operation, identity):
            result = page_limit_call(operation, identity)
            if operation == "discover_content":
                result.data["next_cursor"] = "same-cursor"
            return result

        repeated = run_discovery_backfill(
            start=datetime(2010, 1, 3, 0, 0, tzinfo=SHANGHAI),
            end=repeated_end,
            task_id="repeated-cursor-test",
            max_amount=1.0,
            db_path=self.db,
            platforms=["douyin"],
            call_override=repeated_cursor_call,
            state_root=self.state,
            max_pages_per_account=4,
        )
        self.assertEqual(repeated["status"], "partial")
        self.assertEqual(repeated["accounts_completed"], 0)
        self.assertEqual(repeated["stopped_reason"], "cursor_repeated")

    def test_discovery_does_not_blank_existing_rich_content(self) -> None:
        existing = self._insert_content("242424242", "2026-08-01T01:00:00Z")

        def discovery_call(operation, identity):
            if operation == "resolve_account":
                return ProviderResult(
                    {"reference": "MS4wLjAB" + "x" * 40},
                    {"reference": "profile"},
                    200,
                    True,
                )
            item = {
                "platform": "douyin",
                "platform_content_id": "242424242",
                "canonical_url": "https://www.douyin.com/video/242424242",
                "title": "",
                "body": "",
                "published_at": "2026-08-01T01:00:00Z",
                "content_type": "video",
                "metrics": {"view_count": 0, "like_count": 10},
                "account_uid": str(identity["uid"]),
                "account_name": "汽车号",
            }
            return ProviderResult(
                {"items": [item], "next_cursor": None, "has_more": False},
                {"items": [item]},
                200,
                True,
            )

        result = run_discovery_backfill(
            start=self.start,
            end=self.end,
            task_id="preserve-rich-content-test",
            max_amount=1.0,
            db_path=self.db,
            platforms=["douyin"],
            call_override=discovery_call,
            state_root=self.state,
            skip_existing_derived_stages=True,
        )

        self.assertEqual(result["status"], "succeeded")
        with connect(self.db) as connection:
            row = connection.execute(
                "SELECT title,body FROM content_items WHERE id=?", (existing,)
            ).fetchone()
        self.assertEqual(row["title"], "汽车保养知识")
        self.assertEqual(row["body"], "教你判断刹车故障")
        with connect(self.db) as connection:
            snapshots = connection.execute(
                "SELECT COUNT(*) FROM content_metric_snapshots WHERE content_id=?",
                (existing,),
            ).fetchone()[0]
        self.assertEqual(int(snapshots), 0)

    def test_discovery_missing_published_at_is_partial_not_silent_drop(self) -> None:
        def discovery_call(operation, identity):
            if operation == "resolve_account":
                return ProviderResult(
                    {"reference": "MS4wLjAB" + "x" * 40},
                    {"reference": "profile"},
                    200,
                    True,
                )
            item = {
                "platform": "douyin",
                "platform_content_id": "252525252",
                "canonical_url": "https://www.douyin.com/video/252525252",
                "title": "缺时间内容",
                "body": "",
                "published_at": None,
                "content_type": "video",
                "account_uid": str(identity["uid"]),
                "account_name": "汽车号",
            }
            return ProviderResult(
                {"items": [item], "next_cursor": None, "has_more": False},
                {"items": [item]},
                200,
                True,
            )

        result = run_discovery_backfill(
            start=datetime(2010, 1, 4, 0, 0, tzinfo=SHANGHAI),
            end=datetime(2026, 8, 4, 0, 0, tzinfo=SHANGHAI),
            task_id="missing-published-test",
            max_amount=1.0,
            db_path=self.db,
            platforms=["douyin"],
            call_override=discovery_call,
            state_root=self.state,
        )

        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["failed_pages"], 1)
        self.assertEqual(result["inserted"], 0)
        self.assertEqual(
            result["results"][0]["pages"][0]["missing_published_at_count"],
            1,
        )

    def test_local_evidence_default_scope_excludes_archive(self) -> None:
        archived = self._insert_content("303030303", "2026-08-01T01:00:00Z")
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET source_group=? WHERE id=?",
                (HISTORY_ARCHIVE_SOURCE_GROUP, archived),
            )
            connection.commit()
        result = run_local_evidence_backfill(
            start=self.start, end=self.end, task_id="local-evidence-default",
            max_amount=1.0, db_path=self.db, limit=10, state_root=self.state,
        )
        self.assertEqual(result["candidates"], 0)

    def test_content_backfill_workers_compact_and_exception_isolation(self) -> None:
        for content_id, published in (
            ("515151515", "2026-08-01T01:00:00Z"),
            ("626262626", "2026-08-02T01:00:00Z"),
        ):
            self._insert_content(content_id, published)

        def content_call(stage, content):
            if stage == "detail":
                data = {
                    "title": "详情", "body": "汽车详情",
                    "published_at": content["published_at"],
                    "account_uid": "99887766", "account_name": "汽车号",
                    "content_type": "video", "media_urls": [],
                }
            elif stage == "metrics":
                data = {
                    "view_count": 100, "comment_count": 2, "like_count": 10,
                    "share_count": 1, "collect_count": None,
                }
            else:
                data = {"comment_count": 0, "comments": []}
            return ProviderResult(data, {"stage": stage, "data": data}, 200, True)

        result = run_content_backfill(
            start=self.start, end=self.end, task_id="content-workers-test",
            max_amount=1.0, db_path=self.db, call_override=content_call,
            state_root=self.state, workers=2, compact=True,
        )
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["processed"], 2)
        self.assertEqual(result["results"]["status_counts"], {"succeeded": 2})
        self.assertEqual(result["results"]["failed_contents"], 0)

        def broken(content_id: int) -> dict:
            if content_id == 1:
                raise RuntimeError("boom")
            return {"content_id": content_id, "status": "succeeded", "stages": []}

        results, stopped = _process_content_batch(
            [1, 2], processor=broken, workers=1
        )
        self.assertIsNone(stopped)
        self.assertEqual(results[0]["status"], "partial")
        self.assertEqual(results[0]["stages"][0]["error_code"], "RuntimeError")
        self.assertEqual(results[1]["status"], "succeeded")
        with self.assertRaises(RangeBackfillError):
            _process_content_batch([1], processor=broken, workers=9)

    def test_batch_stops_scheduling_after_blocking_code(self) -> None:
        seen: list[int] = []

        def blocked(content_id: int) -> dict:
            seen.append(content_id)
            return {
                "content_id": content_id,
                "status": "partial",
                "stages": [
                    {"stage": "metrics", "status": "failed",
                     "error_code": "budget_blocked", "message": "预算触顶"}
                ],
            }

        results, stopped = _process_content_batch(
            [1, 2, 3], processor=blocked, workers=1
        )
        self.assertEqual(stopped, "budget_blocked")
        self.assertEqual(seen, [1])
        self.assertEqual(len(results), 1)

    def test_history_only_scope_never_rebills_existing_corpus(self) -> None:
        tagged = self._insert_content("848484848", "2025-06-01T01:00:00Z")
        existing = self._insert_content("959595959", "2026-08-01T01:00:00Z")
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET source_group=? WHERE id=?",
                (HISTORY_ARCHIVE_SOURCE_GROUP, tagged),
            )
            connection.commit()
        scoped = pending_content_ids(
            start=self.start, end=self.end, as_of=self.end, db_path=self.db,
            stages=["metrics"], history_only=True,
        )
        unscoped = pending_content_ids(
            start=self.start, end=self.end, as_of=self.end, db_path=self.db,
            stages=["metrics"],
        )
        self.assertEqual(scoped, [tagged])
        self.assertEqual(sorted(unscoped), sorted([tagged, existing]))

        calls: list[int] = []

        def content_call(stage, content):
            calls.append(int(content["id"]))
            data = {
                "view_count": 100, "comment_count": 2, "like_count": 10,
                "share_count": 1, "collect_count": None,
            }
            return ProviderResult(data, {"stage": stage, "data": data}, 200, True)

        result = run_content_backfill(
            start=self.start, end=self.end, task_id="history-only-test",
            max_amount=1.0, db_path=self.db, call_override=content_call,
            state_root=self.state, stages=["metrics"], history_only=True,
        )
        self.assertEqual(result["candidates"], 1)
        self.assertTrue(result["history_only"])
        self.assertEqual(calls, [tagged])

        summary = summarize_range_status(
            start=self.start, end=self.end, db_path=self.db, history_only=True,
        )
        self.assertEqual(summary["pending"]["comments"]["douyin"], 1)
        self.assertEqual(summary["pending"]["metrics"]["douyin"], 0)

    def test_summarize_range_status_reports_pending_and_costs(self) -> None:
        content = self._insert_content("737373737", "2026-08-01T01:00:00Z")
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO content_metric_snapshots(
                    content_id,captured_at,window_key,view_count,comment_count,
                    like_count,status,source,metadata_json
                ) VALUES (?,?,'2026-08-06',100,45,10,'available','douyin','{}')
                """,
                (content, now_utc()),
            )
            connection.commit()
        summary = summarize_range_status(
            start=self.start, end=self.end, db_path=self.db,
            archive_before=self.archive_before,
        )
        self.assertEqual(summary["platforms"]["douyin"]["content_total"], 1)
        self.assertEqual(summary["pending"]["comments"]["douyin"], 1)
        # 45 条声明评论 ÷ 20 条/页 → 3 页 × $0.001
        self.assertEqual(
            summary["estimated_costs_usd"]["comment_pages"]["douyin"], 3
        )
        self.assertEqual(
            summary["estimated_costs_usd"]["douyin_comments"], 0.003
        )

    def test_unknown_comment_count_quotes_lower_and_safe_upper_bounds(self) -> None:
        self._insert_content("737373738", "2026-08-01T01:00:00Z")

        summary = summarize_range_status(
            start=self.start, end=self.end, db_path=self.db,
            archive_before=self.archive_before,
        )
        costs = summary["estimated_costs_usd"]

        self.assertEqual(costs["comment_declared_unknown"]["douyin"], 1)
        self.assertEqual(costs["comment_pages_lower"]["douyin"], 1)
        self.assertEqual(costs["comment_pages_upper"]["douyin"], 50)
        self.assertEqual(costs["douyin_comments_lower"], 0.001)
        self.assertEqual(costs["douyin_comments"], 0.05)


if __name__ == "__main__":
    unittest.main()
