from __future__ import annotations

import gc
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts import materialize_full_history_discovery_cache as replay
from v8 import media as media_module
from v8.capture import ProviderResult, execute_account_fetch
from v8.operations import upsert_account, upsert_content
from v8.storage import connect, initialize_database


class FullHistoryDiscoveryCacheMaterializationTest(unittest.TestCase):
    PLATFORM_CONTENT_ID = "998877665544332211"

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.db = self.root / "clone.sqlite3"
        self.raw_root = self.root / "raw"
        self.manifest = self.root / "contents.json"
        with closing(connect(self.db)) as connection:
            initialize_database(connection)
        account = upsert_account(
            {
                "phone": "13800138000",
                "platforms": [
                    {
                        "platform": "douyin",
                        "uid": "fixture-account",
                        "nickname": "缓存账号",
                    }
                ],
            },
            db_path=self.db,
        )
        self.account_id = int(account["id"])
        content = upsert_content(
            {
                "platform": "douyin",
                "platform_content_id": self.PLATFORM_CONTENT_ID,
                "canonical_url": (
                    f"https://www.douyin.com/video/{self.PLATFORM_CONTENT_ID}"
                ),
                "title": "缓存",
                "body": "缓存",
                "published_at": "2026-06-01T12:00:00Z",
                "content_type": "video",
                "account_uid": "fixture-account",
                "account_name": "缓存账号",
            },
            db_path=self.db,
            source_group_on_insert="history-backfill",
        )
        self.content_id = int(content["id"])
        outcome = execute_account_fetch(
            account_id=self.account_id,
            stage="discovery",
            window_key="range:test:page-001",
            provider="TikHub",
            adapter_version="fixture-discovery-v1",
            operation="douyin_user_posts",
            call=lambda: ProviderResult(
                data={},
                raw_response={"fixture": "cached-page"},
                http_status=200,
                billed=False,
            ),
            db_path=self.db,
            raw_root=self.raw_root,
        )
        with closing(connect(self.db)) as connection:
            connection.execute(
                "UPDATE provider_raw_responses SET source='live_applied' WHERE id=?",
                (outcome.raw_response_id,),
            )
            connection.commit()
        self.item = {
            "platform": "douyin",
            "platform_content_id": self.PLATFORM_CONTENT_ID,
            "canonical_url": (
                f"https://www.douyin.com/video/{self.PLATFORM_CONTENT_ID}"
            ),
            "title": "缓存完整标题",
            "body": "缓存完整正文",
            "published_at": 1780315200,
            "content_type": "video",
            "account_uid": "fixture-account",
            "account_name": "缓存账号",
            "media_urls": ["https://cdn.example.com/fixture-video.mp4"],
            "metrics": {
                "view_count": 0,
                "like_count": 7,
                "comment_count": 2,
                "share_count": 1,
                "collect_count": None,
            },
        }
        manifest_value = {
            "task_id": "fixture",
            "start": "2010-01-01T00:00:00+08:00",
            "end": "2026-08-07T23:00:00+08:00",
            "updated_at": "2026-08-08T00:00:00Z",
            "contents": [
                {
                    "content_id": self.content_id,
                    "platform": "douyin",
                    "source_group": "history-backfill",
                }
            ],
        }
        self.manifest.write_text(
            json.dumps(manifest_value, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        self.contract = replay.ReplayContract(
            manifest_path=self.manifest,
            raw_root=self.raw_root,
            window_key_like="range:test:%",
            metrics_window_key="2026-08-07",
            manifest_sha256=replay._file_sha256(self.manifest),
            expected_manifest_count=1,
            expected_raw_pages=1,
            expected_raw_items=1,
            expected_pair_sha256=replay._object_sha256(
                [["douyin", self.PLATFORM_CONTENT_ID]]
            ),
            expected_authoritative_view_items=0,
        )
        gc.collect()
        connection = sqlite3.connect(self.db)
        try:
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            connection.execute("PRAGMA journal_mode=DELETE")
        finally:
            connection.close()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _plan(self, item: dict | None = None) -> replay.ReplayPlan:
        page = {"items": [item or self.item], "has_more": False, "cursor": 0}
        with patch.object(
            replay.providers,
            "_parse_douyin_discovery_payload",
            return_value=SimpleNamespace(data=page),
        ):
            return replay.build_replay_plan(db_path=self.db, contract=self.contract)

    def test_clone_materialization_is_zero_cost_isolated_and_idempotent(self) -> None:
        plan = self._plan()
        self.assertEqual(plan.summary["history"]["ready"], 1)
        self.assertEqual(plan.summary["provider_calls_planned"], 0)
        self.assertEqual(
            plan.summary["content_text_expansion"]["candidate_rows_changed"], 1
        )
        self.assertEqual(
            plan.summary["content_text_expansion"]["title_fields_changed"], 1
        )
        self.assertEqual(
            plan.summary["content_text_expansion"]["body_fields_changed"], 1
        )
        derived_root = self.root / "derived-raw"
        media_root = self.root / "media"

        first = replay.apply_replay_plan(
            plan,
            db_path=self.db,
            derived_raw_root=derived_root,
            media_root=media_root,
        )
        self.assertEqual(first["status"], "succeeded")
        self.assertEqual(first["processed"], 1)
        self.assertEqual(first["provider_calls"], 0)
        self.assertEqual(
            first["provider_usage_before"], first["provider_usage_after"]
        )

        with closing(
            sqlite3.connect(
                f"file:{self.db.resolve()}?mode=ro&immutable=1", uri=True
            )
        ) as connection:
            connection.row_factory = sqlite3.Row
            detail = connection.execute(
                """
                SELECT fs.status,fa.billed,fa.amount,pr.source,pr.local_path
                FROM fetch_slots fs
                JOIN fetch_attempts fa ON fa.slot_id=fs.id
                JOIN provider_raw_responses pr ON pr.fetch_attempt_id=fa.id
                WHERE fs.content_id=? AND fs.stage='detail'
                """,
                (self.content_id,),
            ).fetchone()
            artifact = connection.execute(
                """
                SELECT local_path,status FROM evidence_artifacts
                WHERE content_id=? AND artifact_type='media_source'
                """,
                (self.content_id,),
            ).fetchone()
            content = connection.execute(
                "SELECT source_group,title,published_at FROM content_items WHERE id=?",
                (self.content_id,),
            ).fetchone()
            original_source = connection.execute(
                """
                SELECT source FROM provider_raw_responses
                WHERE account_id=? AND content_id IS NULL
                """,
                (self.account_id,),
            ).fetchone()[0]
            counts_before = tuple(
                connection.execute(
                    "SELECT COUNT(*) FROM fetch_slots UNION ALL SELECT COUNT(*) FROM fetch_attempts UNION ALL SELECT COUNT(*) FROM provider_raw_responses UNION ALL SELECT COUNT(*) FROM evidence_artifacts"
                ).fetchall()
            )
            metric_count = connection.execute(
                "SELECT COUNT(*) FROM content_metric_snapshots"
            ).fetchone()[0]
        self.assertEqual(tuple(detail)[:4], ("succeeded", 0, 0.0, "derived_applied"))
        self.assertTrue(
            Path(str(detail[4])).resolve().is_relative_to(derived_root.resolve())
        )
        self.assertEqual(artifact["status"], "available")
        self.assertTrue(
            Path(str(artifact["local_path"]))
            .resolve()
            .is_relative_to(media_root.resolve())
        )
        self.assertEqual(
            tuple(content),
            ("history-backfill", "缓存完整标题", "2026-06-01T12:00:00Z"),
        )
        self.assertEqual(original_source, "live_applied")
        self.assertEqual(metric_count, 0)

        second_plan = self._plan()
        self.assertEqual(second_plan.summary["history"]["ready"], 0)
        self.assertEqual(second_plan.summary["history"]["already_materialized"], 1)
        second = replay.apply_replay_plan(
            second_plan,
            db_path=self.db,
            derived_raw_root=derived_root,
            media_root=media_root,
            content_ids=[self.content_id],
        )
        self.assertEqual(second["processed"], 0)
        self.assertEqual(
            second["already_materialized_requested"], [self.content_id]
        )
        with closing(
            sqlite3.connect(
                f"file:{self.db.resolve()}?mode=ro&immutable=1", uri=True
            )
        ) as connection:
            connection.row_factory = sqlite3.Row
            counts_after = tuple(
                connection.execute(
                    "SELECT COUNT(*) FROM fetch_slots UNION ALL SELECT COUNT(*) FROM fetch_attempts UNION ALL SELECT COUNT(*) FROM provider_raw_responses UNION ALL SELECT COUNT(*) FROM evidence_artifacts"
                ).fetchall()
            )
        self.assertEqual(counts_after, counts_before)

    def test_missing_media_url_remains_explicitly_unprocessed(self) -> None:
        item = {**self.item, "media_urls": []}
        plan = self._plan(item)
        self.assertEqual(plan.summary["history"]["ready"], 0)
        self.assertEqual(plan.summary["history"]["missing_media_urls"], 1)
        self.assertEqual(plan.candidates, ())

    def test_plan_is_physically_read_only(self) -> None:
        before_sha = replay._file_sha256(self.db)
        before_mtime = self.db.stat().st_mtime_ns
        self._plan()
        self.assertEqual(replay._file_sha256(self.db), before_sha)
        self.assertEqual(self.db.stat().st_mtime_ns, before_mtime)
        self.assertFalse(Path(f"{self.db}-wal").exists())
        self.assertFalse(Path(f"{self.db}-shm").exists())

    def test_apply_refuses_formal_database_path(self) -> None:
        plan = self._plan()
        with patch.object(replay, "FORMAL_DB", self.db), self.assertRaisesRegex(
            replay.CacheReplayError, "正式数据库"
        ):
            replay.apply_replay_plan(
                plan,
                db_path=self.db,
                derived_raw_root=self.root / "derived",
                media_root=self.root / "media",
            )

    def test_apply_refuses_canonical_cache_roots(self) -> None:
        plan = self._plan()
        with self.assertRaisesRegex(replay.CacheReplayError, "正式缓存根"):
            replay.apply_replay_plan(
                plan,
                db_path=self.db,
                derived_raw_root=replay.DEFAULT_RAW_ROOT,
                media_root=self.root / "media",
            )

    def test_output_path_refuses_formal_database_in_clone_directory(self) -> None:
        formal = self.root / "formal.sqlite3"
        formal.write_bytes(b"formal-database-sentinel")
        before = formal.read_bytes()
        with patch.object(replay, "FORMAL_DB", formal), self.assertRaisesRegex(
            replay.CacheReplayError, "正式数据库"
        ):
            replay._validate_output_path(formal, db_path=self.db)
        self.assertEqual(formal.read_bytes(), before)

    def test_plan_blocks_conflicting_cached_text(self) -> None:
        conflicting = {
            **self.item,
            "title": "完全不同的标题",
            "body": "完全不同的正文",
        }
        plan = self._plan(conflicting)
        self.assertEqual(plan.summary["status"], "blocked")
        self.assertEqual(
            plan.summary["content_text_expansion"]["conflict_ids"],
            [self.content_id],
        )

    def test_keyboard_interrupt_still_finalizes_clone_sidecars(self) -> None:
        plan = self._plan()
        original = replay.providers._materialize_discovery_stages

        def interrupt_after_write(**kwargs: object) -> None:
            original(**kwargs)
            raise KeyboardInterrupt

        with patch.object(
            replay.providers,
            "_materialize_discovery_stages",
            side_effect=interrupt_after_write,
        ), self.assertRaises(KeyboardInterrupt):
            replay.apply_replay_plan(
                plan,
                db_path=self.db,
                derived_raw_root=self.root / "interrupt-derived",
                media_root=self.root / "interrupt-media",
            )
        self.assertFalse(Path(f"{self.db}-wal").exists())
        self.assertFalse(Path(f"{self.db}-shm").exists())
        self.assertFalse(Path(f"{self.db}-journal").exists())

    def test_succeeded_detail_without_artifact_repairs_media_only(self) -> None:
        plan = self._plan()
        usage_before = replay._usage_snapshot(self.db)

        def interrupt_business_storage(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("fixture business write interruption")

        with patch.object(
            replay.providers,
            "_store_stage_result",
            side_effect=interrupt_business_storage,
        ), self.assertRaisesRegex(replay.CacheReplayError, "物化失败"):
            replay.apply_replay_plan(
                plan,
                db_path=self.db,
                derived_raw_root=self.root / "replay-derived",
                media_root=self.root / "replay-media",
            )
        self.assertFalse(Path(f"{self.db}-wal").exists())
        self.assertFalse(Path(f"{self.db}-shm").exists())
        with closing(
            sqlite3.connect(
                f"file:{self.db.resolve()}?mode=ro&immutable=1", uri=True
            )
        ) as connection:
            failure_counts = tuple(
                int(row[0])
                for row in connection.execute(
                    "SELECT COUNT(*) FROM fetch_slots "
                    "UNION ALL SELECT COUNT(*) FROM fetch_attempts "
                    "UNION ALL SELECT COUNT(*) FROM provider_raw_responses"
                ).fetchall()
            )

        recovery_plan = self._plan()
        self.assertEqual(
            recovery_plan.summary["history"]["media_only_ready"], 1
        )
        self.assertEqual(recovery_plan.candidates[0].mode, "media_only")

        result = replay.apply_replay_plan(
            recovery_plan,
            db_path=self.db,
            derived_raw_root=self.root / "replay-derived",
            media_root=self.root / "replay-media",
        )
        self.assertEqual(result["processed"], 1)
        self.assertEqual(result["results"][0]["mode"], "media_only")
        self.assertEqual(result["results"][0]["replayed"], ["detail"])
        self.assertEqual(replay._usage_snapshot(self.db), usage_before)
        replay._finalize_disposable_database(self.db)
        with closing(
            sqlite3.connect(
                f"file:{self.db.resolve()}?mode=ro&immutable=1", uri=True
            )
        ) as connection:
            connection.row_factory = sqlite3.Row
            self.assertEqual(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM fetch_slots
                    WHERE content_id=? AND stage='detail' AND window_key='lifetime'
                    """,
                    (self.content_id,),
                ).fetchone()[0],
                1,
            )
            detail_raw = connection.execute(
                """
                SELECT pr.id,pr.source FROM fetch_slots fs
                JOIN fetch_attempts fa ON fa.slot_id=fs.id
                JOIN provider_raw_responses pr ON pr.fetch_attempt_id=fa.id
                WHERE fs.content_id=? AND fs.stage='detail'
                  AND fs.window_key='lifetime'
                """,
                (self.content_id,),
            ).fetchone()
            content = connection.execute(
                "SELECT title,body FROM content_items WHERE id=?",
                (self.content_id,),
            ).fetchone()
            artifact = connection.execute(
                """
                SELECT metadata_json FROM evidence_artifacts
                WHERE content_id=? AND artifact_type='media_source'
                """,
                (self.content_id,),
            ).fetchone()
            artifact_count = connection.execute(
                """
                SELECT COUNT(*) FROM evidence_artifacts
                WHERE content_id=? AND artifact_type='media_source'
                  AND status='available'
                """,
                (self.content_id,),
            ).fetchone()[0]
            recovery_counts = tuple(
                int(row[0])
                for row in connection.execute(
                    "SELECT COUNT(*) FROM fetch_slots "
                    "UNION ALL SELECT COUNT(*) FROM fetch_attempts "
                    "UNION ALL SELECT COUNT(*) FROM provider_raw_responses"
                ).fetchall()
            )
        self.assertEqual(tuple(content), ("缓存完整标题", "缓存完整正文"))
        self.assertEqual(detail_raw["source"], "derived_applied")
        self.assertEqual(
            json.loads(artifact["metadata_json"])["raw_response_id"],
            int(detail_raw["id"]),
        )
        self.assertEqual(artifact_count, 1)
        self.assertEqual(recovery_counts, failure_counts)

    def test_retryable_derived_detail_failure_can_resume_without_provider(self) -> None:
        plan = self._plan()
        usage_before = replay._usage_snapshot(self.db)

        def interrupt_derived_result(**_kwargs: object) -> None:
            raise RuntimeError("fixture derived raw interruption")

        with patch.object(
            replay.providers,
            "_derived_discovery_result",
            side_effect=interrupt_derived_result,
        ), self.assertRaisesRegex(replay.CacheReplayError, "物化失败"):
            replay.apply_replay_plan(
                plan,
                db_path=self.db,
                derived_raw_root=self.root / "retry-derived",
                media_root=self.root / "retry-media",
            )
        self.assertFalse(Path(f"{self.db}-wal").exists())
        self.assertFalse(Path(f"{self.db}-shm").exists())
        with closing(
            sqlite3.connect(
                f"file:{self.db.resolve()}?mode=ro&immutable=1", uri=True
            )
        ) as connection:
            failed = connection.execute(
                """
                SELECT fs.status,fa.billed,fa.amount
                FROM fetch_slots fs JOIN fetch_attempts fa ON fa.slot_id=fs.id
                WHERE fs.content_id=? AND fs.stage='detail'
                ORDER BY fa.attempt_number DESC LIMIT 1
                """,
                (self.content_id,),
            ).fetchone()
        self.assertEqual(tuple(failed), ("retryable_failed", 0, 0.0))

        retry_plan = self._plan()
        self.assertEqual(retry_plan.summary["history"]["ready"], 1)
        result = replay.apply_replay_plan(
            retry_plan,
            db_path=self.db,
            derived_raw_root=self.root / "retry-derived",
            media_root=self.root / "retry-media",
        )
        self.assertEqual(result["processed"], 1)
        self.assertEqual(result["provider_calls"], 0)
        self.assertEqual(replay._usage_snapshot(self.db), usage_before)
        with closing(
            sqlite3.connect(
                f"file:{self.db.resolve()}?mode=ro&immutable=1", uri=True
            )
        ) as connection:
            self.assertEqual(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM fetch_attempts fa
                    JOIN fetch_slots fs ON fs.id=fa.slot_id
                    WHERE fs.content_id=? AND fs.stage='detail'
                    """,
                    (self.content_id,),
                ).fetchone()[0],
                2,
            )

    def test_media_root_default_is_resolved_at_call_time(self) -> None:
        with closing(connect(self.db)) as connection:
            raw_response_id = int(
                connection.execute(
                    "SELECT id FROM provider_raw_responses ORDER BY id LIMIT 1"
                ).fetchone()[0]
            )
        patched_root = self.root / "patched-media"
        with patch.object(media_module, "MEDIA_ROOT", patched_root):
            artifact = media_module.store_media_source_manifest(
                self.content_id,
                media_kind="video",
                urls=["https://cdn.example.com/default-root.mp4"],
                raw_response_id=raw_response_id,
                db_path=self.db,
            )
        self.assertIsNotNone(artifact)
        self.assertTrue(
            Path(str(artifact.local_path))
            .resolve()
            .is_relative_to(patched_root.resolve())
        )


if __name__ == "__main__":
    unittest.main()
