from __future__ import annotations

import gc
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts import materialize_full_history_discovery_cache as replay
from scripts import run_full_history_cache_batches as batches
from v8.capture import ProviderResult, claim_content_slot, execute_account_fetch
from v8.operations import upsert_account, upsert_content
from v8.storage import connect, initialize_database


class FullHistoryCacheBatchControllerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.db = self.root / "clone.sqlite3"
        self.raw_root = self.root / "source-raw"
        self.derived_root = self.root / "derived"
        self.media_root = self.root / "media"
        self.run_root = self.root / "run"
        self.manifest = self.root / "contents.json"
        with closing(connect(self.db)) as connection:
            initialize_database(connection)
        account = upsert_account(
            {
                "phone": "13800138001",
                "platforms": [
                    {
                        "platform": "douyin",
                        "uid": "batch-fixture-account",
                        "nickname": "批处理账号",
                    }
                ],
            },
            db_path=self.db,
        )
        self.account_id = int(account["id"])
        self.items: list[dict[str, object]] = []
        self.content_ids: list[int] = []
        for index in range(5):
            platform_content_id = f"99887766554433{index:04d}"
            content = upsert_content(
                {
                    "platform": "douyin",
                    "platform_content_id": platform_content_id,
                    "canonical_url": f"https://www.douyin.com/video/{platform_content_id}",
                    "title": f"缓存{index}",
                    "body": f"正文{index}",
                    "published_at": "2026-06-01T12:00:00Z",
                    "content_type": "video",
                    "account_uid": "batch-fixture-account",
                    "account_name": "批处理账号",
                },
                db_path=self.db,
                source_group_on_insert="history-backfill",
            )
            self.content_ids.append(int(content["id"]))
            self.items.append(
                {
                    "platform": "douyin",
                    "platform_content_id": platform_content_id,
                    "canonical_url": f"https://www.douyin.com/video/{platform_content_id}",
                    "title": f"缓存{index}完整标题",
                    "body": f"正文{index}完整正文",
                    "published_at": 1780315200,
                    "content_type": "video",
                    "account_uid": "batch-fixture-account",
                    "account_name": "批处理账号",
                    "media_urls": [f"https://cdn.example.com/{index}.mp4"],
                    "metrics": {"view_count": 0, "like_count": index},
                }
            )
        outcome = execute_account_fetch(
            account_id=self.account_id,
            stage="discovery",
            window_key="range:batch:test:page-001",
            provider="TikHub",
            adapter_version="fixture-discovery-v1",
            operation="douyin_user_posts",
            call=lambda: ProviderResult(
                data={},
                raw_response={"fixture": "batch-cached-page"},
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
        manifest = {
            "task_id": "batch-fixture",
            "contents": [
                {
                    "content_id": content_id,
                    "platform": "douyin",
                    "source_group": "history-backfill",
                }
                for content_id in self.content_ids
            ],
        }
        self.manifest.write_text(
            json.dumps(manifest, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        self.contract = replay.ReplayContract(
            manifest_path=self.manifest,
            raw_root=self.raw_root,
            window_key_like="range:batch:test:%",
            metrics_window_key="2026-08-07",
            manifest_sha256=replay._file_sha256(self.manifest),
            expected_manifest_count=5,
            expected_raw_pages=1,
            expected_raw_items=5,
            expected_pair_sha256=replay._object_sha256(
                [
                    ["douyin", str(item["platform_content_id"])]
                    for item in self.items
                ]
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

    def _run(self, **overrides: object) -> dict[str, object]:
        values: dict[str, object] = {
            "db_path": self.db,
            "derived_raw_root": self.derived_root,
            "media_root": self.media_root,
            "run_root": self.run_root,
            "replay_contract": self.contract,
            "canary_size": 2,
            "batch_size": 2,
            "min_free_bytes": 1,
        }
        values.update(overrides)
        with patch.object(
            replay.providers,
            "_parse_douyin_discovery_payload",
            return_value=SimpleNamespace(
                data={"items": self.items, "has_more": False, "cursor": 0}
            ),
        ):
            return dict(batches.run_batches(**values))

    def _counts(self) -> tuple[int, int, int, int, int]:
        connection = sqlite3.connect(
            f"file:{self.db}?mode=ro&immutable=1", uri=True
        )
        try:
            return tuple(
                int(row[0])
                for row in connection.execute(
                    "SELECT COUNT(*) FROM fetch_slots "
                    "UNION ALL SELECT COUNT(*) FROM fetch_attempts "
                    "UNION ALL SELECT COUNT(*) FROM provider_raw_responses "
                    "UNION ALL SELECT COUNT(*) FROM evidence_artifacts "
                    "UNION ALL SELECT COUNT(*) FROM content_metric_snapshots"
                ).fetchall()
            )
        finally:
            connection.close()

    def test_direct_cli_help_resolves_repository_scripts_package(self) -> None:
        repository_root = Path(batches.__file__).resolve().parents[1]
        environment = dict(os.environ)
        environment.update(
            {
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYTHONPATH": str(repository_root / "src/dcar_eval"),
                "DCAR_TEST_DENY_FORMAL_DB": "1",
            }
        )
        completed = subprocess.run(
            [
                sys.executable,
                str(repository_root / "scripts/run_full_history_cache_batches.py"),
                "--help",
            ],
            cwd=self.root,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("--derived-raw-root", completed.stdout)
        self.assertIn("--max-batches", completed.stdout)

    def test_clean_wal_mode_clone_starts_without_baseline_sidecars(self) -> None:
        connection = sqlite3.connect(self.db)
        try:
            self.assertEqual(
                connection.execute("PRAGMA journal_mode=WAL").fetchone()[0],
                "wal",
            )
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            connection.close()
        gc.collect()
        self.assertFalse(Path(f"{self.db}-wal").exists())
        self.assertFalse(Path(f"{self.db}-shm").exists())

        result = self._run(max_batches=1)
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["completed"], 2)
        self.assertTrue(
            (self.run_root / "batches/batch-000001.receipt.json").is_file()
        )
        self.assertFalse(Path(f"{self.db}-wal").exists())
        self.assertFalse(Path(f"{self.db}-shm").exists())

    def test_canary_then_multi_batch_resume_and_idempotence(self) -> None:
        before_usage = replay._usage_snapshot(self.db)
        first = self._run(max_batches=1)
        self.assertEqual(first["status"], "partial")
        self.assertEqual(first["completed"], 2)
        self.assertEqual(first["remaining"], 3)
        self.assertEqual(first["receipts_created"], 1)

        second = self._run()
        self.assertEqual(second["status"], "succeeded")
        self.assertEqual(second["completed"], 5)
        self.assertEqual(second["remaining"], 0)
        self.assertEqual(second["receipts_total"], 3)
        self.assertEqual(second["completion"]["missing_media_urls"], 0)
        self.assertEqual(self._counts(), (6, 6, 6, 5, 0))
        self.assertEqual(replay._usage_snapshot(self.db), before_usage)
        self.assertFalse(Path(f"{self.db}-wal").exists())
        self.assertFalse(Path(f"{self.db}-shm").exists())
        self.assertTrue((self.run_root / "completion.json").is_file())
        first_receipt = json.loads(
            (self.run_root / "batches/batch-000001.receipt.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(
            set(first_receipt["disk"]),
            {"database", "derived_raw_root", "media_root", "run_root"},
        )

        before_sha = replay._file_sha256(self.db)
        third = self._run()
        self.assertEqual(third["status"], "succeeded")
        self.assertEqual(third["receipts_created"], 0)
        self.assertEqual(replay._file_sha256(self.db), before_sha)

    def test_crash_after_apply_before_receipt_recovers_without_duplicate_fetch(self) -> None:
        def crash(_index: int, _applied: object) -> None:
            raise RuntimeError("fixture hard stop after apply")

        with self.assertRaisesRegex(RuntimeError, "hard stop"):
            self._run(max_batches=1, after_batch_applied=crash)
        intent = self.run_root / "batches/batch-000001.intent.json"
        receipt = self.run_root / "batches/batch-000001.receipt.json"
        self.assertTrue(intent.is_file())
        self.assertFalse(receipt.exists())
        counts_after_crash = self._counts()

        resumed = self._run(max_batches=1)
        self.assertEqual(resumed["status"], "partial")
        self.assertEqual(resumed["receipts_created"], 1)
        recovered_receipt = json.loads(receipt.read_text(encoding="utf-8"))
        self.assertEqual(
            recovered_receipt["recovered_content_ids"], self.content_ids[:2]
        )
        self.assertEqual(recovered_receipt["processed_content_ids"], [])
        self.assertEqual(self._counts(), counts_after_crash)

    def test_orphan_output_cleanup_is_durable_across_second_crash(self) -> None:
        orphan: Path | None = None
        original_atomic_bytes = batches.capture_module._atomic_bytes

        def commit_raw_then_crash(path: Path, value: bytes) -> None:
            nonlocal orphan
            original_atomic_bytes(path, value)
            orphan = path
            raise RuntimeError("fixture orphan after file commit")

        with patch.object(
            batches.capture_module,
            "_atomic_bytes",
            side_effect=commit_raw_then_crash,
        ), self.assertRaisesRegex(replay.CacheReplayError, "缓存详情物化失败"):
            self._run(max_batches=1)
        self.assertIsNotNone(orphan)
        assert orphan is not None
        self.assertTrue(orphan.is_file())
        original_atomic_json = batches._atomic_json

        def crash_before_cleanup_receipt(path: Path, value: object) -> str:
            if path.name.endswith("output-cleanup-000001.receipt.json"):
                raise RuntimeError("fixture cleanup receipt crash")
            return original_atomic_json(path, value)

        with patch.object(
            batches, "_atomic_json", side_effect=crash_before_cleanup_receipt
        ), self.assertRaisesRegex(RuntimeError, "cleanup receipt crash"):
            self._run(max_batches=1)
        cleanup_intent = (
            self.run_root
            / "batches/batch-000001.output-cleanup-000001.intent.json"
        )
        cleanup_receipt = (
            self.run_root
            / "batches/batch-000001.output-cleanup-000001.receipt.json"
        )
        self.assertTrue(cleanup_intent.is_file())
        self.assertFalse(cleanup_receipt.exists())
        self.assertFalse(orphan.exists())

        resumed = self._run(max_batches=1)
        self.assertEqual(resumed["status"], "partial")
        self.assertTrue(cleanup_receipt.is_file())
        receipt = json.loads(
            (self.run_root / "batches/batch-000001.receipt.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(receipt["output_cleanup"]["files"], 1)

    def test_repeated_raw_orphans_append_cleanup_rounds(self) -> None:
        original_atomic_bytes = batches.capture_module._atomic_bytes

        def commit_raw_then_crash(path: Path, value: bytes) -> None:
            original_atomic_bytes(path, value)
            raise RuntimeError("fixture repeated raw orphan")

        for _attempt in range(2):
            with patch.object(
                batches.capture_module,
                "_atomic_bytes",
                side_effect=commit_raw_then_crash,
            ), self.assertRaisesRegex(
                replay.CacheReplayError, "缓存详情物化失败"
            ):
                self._run(max_batches=1)

        resumed = self._run(max_batches=1)
        self.assertEqual(resumed["status"], "partial")
        receipt = json.loads(
            (self.run_root / "batches/batch-000001.receipt.json").read_text(
                encoding="utf-8"
            )
        )
        cleanup = receipt["output_cleanup"]
        self.assertEqual(cleanup["rounds"], 2)
        self.assertEqual(cleanup["files"], 2)
        self.assertEqual([row[0] for row in cleanup["round_rows"]], [1, 2])
        first_receipt = (
            self.run_root
            / "batches/batch-000001.output-cleanup-000001.receipt.json"
        )
        second_intent = json.loads(
            (
                self.run_root
                / "batches/batch-000001.output-cleanup-000002.intent.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(
            second_intent["previous_cleanup_receipt_sha256"],
            replay._file_sha256(first_receipt),
        )
        completed = self._run()
        self.assertEqual(completed["status"], "succeeded")
        tampered = json.loads(first_receipt.read_text(encoding="utf-8"))
        tampered["status"] = "tampered"
        first_receipt.write_text(
            json.dumps(tampered, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        with self.assertRaises(batches.BatchReplayError):
            self._run()

    def test_repeated_media_temp_and_final_orphans_append_cleanup_rounds(
        self,
    ) -> None:
        def partial_atomic_media(path: Path, _byte_size: int) -> None:
            path.write_bytes(b"partial")
            raise RuntimeError("fixture media temp orphan")

        with patch.object(
            batches.media_module,
            "_after_private_staging_chunk",
            side_effect=partial_atomic_media,
        ), self.assertRaisesRegex(replay.CacheReplayError, "缓存详情物化失败"):
            self._run(max_batches=1)
        with patch.object(
            batches.media_module,
            "register_artifact",
            side_effect=RuntimeError("fixture media final orphan"),
        ), self.assertRaisesRegex(replay.CacheReplayError, "缓存详情物化失败"):
            self._run(max_batches=1)

        resumed = self._run(max_batches=1)
        self.assertEqual(resumed["status"], "partial")
        receipt = json.loads(
            (self.run_root / "batches/batch-000001.receipt.json").read_text(
                encoding="utf-8"
            )
        )
        cleanup = receipt["output_cleanup"]
        self.assertEqual(cleanup["rounds"], 2)
        self.assertEqual(cleanup["files"], 2)
        self.assertEqual([row[0] for row in cleanup["round_rows"]], [1, 2])

    def test_repeated_media_final_orphans_with_new_body_append_cleanup_rounds(
        self,
    ) -> None:
        final_path: Path | None = None
        first_sha: str | None = None
        for captured_at in (
            "2026-08-09T08:00:01+00:00",
            "2026-08-09T08:00:02+00:00",
        ):
            with patch.object(
                batches.media_module,
                "now_utc",
                return_value=captured_at,
            ), patch.object(
                batches.media_module,
                "register_artifact",
                side_effect=RuntimeError("fixture repeated media final orphan"),
            ), self.assertRaisesRegex(
                replay.CacheReplayError, "缓存详情物化失败"
            ):
                self._run(max_batches=1)
            manifests = list(self.media_root.rglob("source-*.json"))
            self.assertEqual(len(manifests), 1)
            final_path = manifests[0]
            current_sha = replay._file_sha256(final_path)
            if first_sha is None:
                first_sha = current_sha
            else:
                self.assertNotEqual(current_sha, first_sha)

        assert final_path is not None
        resumed = self._run(max_batches=1)
        self.assertEqual(resumed["status"], "partial")
        receipt = json.loads(
            (self.run_root / "batches/batch-000001.receipt.json").read_text(
                encoding="utf-8"
            )
        )
        cleanup = receipt["output_cleanup"]
        self.assertEqual(cleanup["rounds"], 2)
        self.assertEqual(cleanup["files"], 2)
        self.assertEqual([row[0] for row in cleanup["round_rows"]], [1, 2])
        self.assertTrue(final_path.is_file())

    def test_repeated_media_temps_with_new_bytes_append_cleanup_rounds(
        self,
    ) -> None:
        partial_bodies = (b"first", b"second-longer")
        for partial_body in partial_bodies:
            def partial_atomic_media(
                path: Path,
                _byte_size: int,
                *,
                body: bytes = partial_body,
            ) -> None:
                path.write_bytes(body)
                raise RuntimeError("fixture repeated media temp orphan")

            with patch.object(
                batches.media_module,
                "_after_private_staging_chunk",
                side_effect=partial_atomic_media,
            ), self.assertRaisesRegex(
                replay.CacheReplayError, "缓存详情物化失败"
            ):
                self._run(max_batches=1)
            temporary = list(self.media_root.rglob("*.tmp"))
            self.assertEqual(len(temporary), 1)
            self.assertEqual(temporary[0].read_bytes(), partial_body)

        resumed = self._run(max_batches=1)
        self.assertEqual(resumed["status"], "partial")
        receipt = json.loads(
            (self.run_root / "batches/batch-000001.receipt.json").read_text(
                encoding="utf-8"
            )
        )
        cleanup = receipt["output_cleanup"]
        self.assertEqual(cleanup["rounds"], 2)
        self.assertEqual(cleanup["files"], 2)
        self.assertEqual([row[0] for row in cleanup["round_rows"]], [1, 2])
        self.assertFalse(list(self.media_root.rglob("*.tmp")))

    def test_superseded_media_path_still_rejects_unowned_new_body(self) -> None:
        with patch.object(
            batches.media_module,
            "register_artifact",
            side_effect=RuntimeError("fixture first media final orphan"),
        ), self.assertRaisesRegex(replay.CacheReplayError, "缓存详情物化失败"):
            self._run(max_batches=1)
        manifests = list(self.media_root.rglob("source-*.json"))
        self.assertEqual(len(manifests), 1)
        manifest = manifests[0]

        def forge_same_path_after_cleanup(
            _index: int, _content_ids: object
        ) -> None:
            manifest.write_text('{"forged":true}\n', encoding="utf-8")
            raise RuntimeError("fixture forged superseding body")

        with self.assertRaisesRegex(RuntimeError, "forged superseding body"):
            self._run(
                max_batches=1,
                before_batch_apply=forge_same_path_after_cleanup,
            )
        self.assertTrue(
            (
                self.run_root
                / "batches/batch-000001.output-cleanup-000001.receipt.json"
            ).is_file()
        )
        with self.assertRaisesRegex(
            batches.BatchReplayError, "待清理媒体证据时间戳非法"
        ):
            self._run(max_batches=1)
        self.assertEqual(manifest.read_text(encoding="utf-8"), '{"forged":true}\n')
        self.assertFalse(
            (
                self.run_root
                / "batches/batch-000001.output-cleanup-000002.intent.json"
            ).exists()
        )

    def test_partial_atomic_raw_temp_is_cleaned_and_retried(self) -> None:
        before_usage = replay._usage_snapshot(self.db)

        def partial_atomic_raw(path: Path, value: bytes) -> None:
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_name(f".{path.name}.tmp")
            temporary.write_bytes(value[: max(1, len(value) // 2)])
            raise RuntimeError("fixture partial atomic raw")

        with patch.object(
            batches.capture_module,
            "_atomic_bytes",
            side_effect=partial_atomic_raw,
        ), self.assertRaisesRegex(replay.CacheReplayError, "缓存详情物化失败"):
            self._run(max_batches=1)
        leftovers = list(self.derived_root.rglob("*.tmp"))
        self.assertEqual(len(leftovers), 1)

        resumed = self._run(max_batches=1)
        self.assertEqual(resumed["status"], "partial")
        self.assertFalse(leftovers[0].exists())
        receipt = json.loads(
            (self.run_root / "batches/batch-000001.receipt.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(receipt["output_cleanup"]["files"], 1)
        self.assertEqual(replay._usage_snapshot(self.db), before_usage)

    def test_forged_raw_temp_digest_is_preserved_and_blocks_resume(self) -> None:
        fake: Path | None = None

        def claim_then_forge(_index: int, content_ids: object) -> None:
            nonlocal fake
            content_id = int(list(content_ids)[0])
            claim = claim_content_slot(
                db_path=self.db,
                content_id=content_id,
                stage="detail",
                window_key="lifetime",
                provider="TikHub",
                adapter_version="tikhub-discovery-derived-v8.1",
                allow_terminal_retry=True,
            )
            fake = (
                self.derived_root
                / "tikhub"
                / str(content_id)
                / "douyin_video_detail"
                / f".attempt-{claim.attempt_number:03d}-aaaaaaaaaaaa.json.tmp"
            )
            fake.parent.mkdir(parents=True, exist_ok=True)
            fake.write_bytes(b"not-owned")
            raise KeyboardInterrupt

        with self.assertRaises(KeyboardInterrupt):
            self._run(max_batches=1, before_batch_apply=claim_then_forge)
        assert fake is not None
        with self.assertRaisesRegex(
            batches.BatchReplayError, "不属于 pending 内容"
        ):
            self._run(max_batches=1)
        self.assertTrue(fake.is_file())
        self.assertFalse(
            (
                self.run_root
                / "batches/batch-000001.output-cleanup-000001.intent.json"
            ).exists()
        )

    def test_forged_final_raw_attempt_is_preserved_and_blocks_resume(self) -> None:
        forged: Path | None = None

        def forge_after_apply(_index: int, _applied: object) -> None:
            nonlocal forged
            with closing(
                sqlite3.connect(f"file:{self.db}?mode=ro&immutable=1", uri=True)
            ) as connection:
                stored_path, sha256 = connection.execute(
                    """
                    SELECT local_path,sha256 FROM provider_raw_responses
                    WHERE source='derived_applied' ORDER BY id LIMIT 1
                    """
                ).fetchone()
            source = batches._resolve_stored_path(str(stored_path))
            forged = source.with_name(f"attempt-999-{str(sha256)[:12]}.json")
            shutil.copy2(source, forged)
            raise RuntimeError("fixture forged final raw")

        with self.assertRaisesRegex(RuntimeError, "forged final raw"):
            self._run(max_batches=1, after_batch_applied=forge_after_apply)
        assert forged is not None
        with self.assertRaisesRegex(
            batches.BatchReplayError, "未绑定 pending attempt"
        ):
            self._run(max_batches=1)
        self.assertTrue(forged.is_file())
        self.assertFalse(
            (
                self.run_root
                / "batches/batch-000001.output-cleanup-000001.intent.json"
            ).exists()
        )

    def test_stale_attempt_raw_temp_is_preserved_and_blocks_resume(self) -> None:
        def claim_then_interrupt(_index: int, content_ids: object) -> None:
            claim_content_slot(
                db_path=self.db,
                content_id=int(list(content_ids)[0]),
                stage="detail",
                window_key="lifetime",
                provider="TikHub",
                adapter_version="tikhub-discovery-derived-v8.1",
                allow_terminal_retry=True,
            )
            raise KeyboardInterrupt

        with self.assertRaises(KeyboardInterrupt):
            self._run(max_batches=1, before_batch_apply=claim_then_interrupt)
        first = self.content_ids[0]
        batches._recover_interrupted_detail_slots(self.db, content_ids=[first])
        claim_content_slot(
            db_path=self.db,
            content_id=first,
            stage="detail",
            window_key="lifetime",
            provider="TikHub",
            adapter_version="tikhub-discovery-derived-v8.1",
            allow_terminal_retry=True,
        )
        contract = json.loads(
            (self.run_root / "run-contract.json").read_text(encoding="utf-8")
        )
        evidence = batches._target_contract_map(contract)[first]
        stale = (
            self.derived_root
            / "tikhub"
            / str(first)
            / "douyin_video_detail"
            / (
                ".attempt-001-"
                f"{str(evidence['expected_detail_raw_sha256'])[:12]}.json.tmp"
            )
        )
        stale.parent.mkdir(parents=True, exist_ok=True)
        stale.write_bytes(b"stale")

        with self.assertRaisesRegex(
            batches.BatchReplayError, "未绑定 pending attempt"
        ):
            self._run(max_batches=1)
        self.assertTrue(stale.is_file())

    def test_stale_attempt_final_raw_is_preserved_and_blocks_resume(self) -> None:
        with patch.object(
            replay.providers,
            "_parse_douyin_discovery_payload",
            return_value=SimpleNamespace(
                data={"items": self.items, "has_more": False, "cursor": 0}
            ),
        ):
            initial_plan = replay.build_replay_plan(
                db_path=self.db, contract=self.contract
            )
        candidate = initial_plan.candidates[0]

        def claim_then_interrupt(_index: int, content_ids: object) -> None:
            claim_content_slot(
                db_path=self.db,
                content_id=int(list(content_ids)[0]),
                stage="detail",
                window_key="lifetime",
                provider="TikHub",
                adapter_version="tikhub-discovery-derived-v8.1",
                allow_terminal_retry=True,
            )
            raise KeyboardInterrupt

        with self.assertRaises(KeyboardInterrupt):
            self._run(max_batches=1, before_batch_apply=claim_then_interrupt)
        first = self.content_ids[0]
        batches._recover_interrupted_detail_slots(self.db, content_ids=[first])
        claim_content_slot(
            db_path=self.db,
            content_id=first,
            stage="detail",
            window_key="lifetime",
            provider="TikHub",
            adapter_version="tikhub-discovery-derived-v8.1",
            allow_terminal_retry=True,
        )
        contract = json.loads(
            (self.run_root / "run-contract.json").read_text(encoding="utf-8")
        )
        evidence = batches._target_contract_map(contract)[first]
        body = batches.capture_module.canonical_json_bytes(
            batches._expected_detail_raw_body(
                candidate,
                source_sha256=str(evidence["source_discovery_sha256"]),
                source_captured_at=str(evidence["source_discovery_captured_at"]),
            )
        )
        stale = (
            self.derived_root
            / "tikhub"
            / str(first)
            / "douyin_video_detail"
            / (
                "attempt-001-"
                f"{str(evidence['expected_detail_raw_sha256'])[:12]}.json"
            )
        )
        stale.parent.mkdir(parents=True, exist_ok=True)
        stale.write_bytes(body)

        with self.assertRaisesRegex(
            batches.BatchReplayError, "未绑定 pending attempt"
        ):
            self._run(max_batches=1)
        self.assertTrue(stale.is_file())

    def test_partial_atomic_media_temp_is_cleaned_and_replayed(self) -> None:
        before_usage = replay._usage_snapshot(self.db)

        def partial_atomic_media(path: Path, _byte_size: int) -> None:
            body = path.read_bytes()
            path.write_bytes(body[: max(1, len(body) // 2)])
            raise RuntimeError("fixture partial atomic media")

        with patch.object(
            batches.media_module,
            "_after_private_staging_chunk",
            side_effect=partial_atomic_media,
        ), self.assertRaisesRegex(replay.CacheReplayError, "缓存详情物化失败"):
            self._run(max_batches=1)
        leftovers = list(self.media_root.rglob("*.tmp"))
        self.assertEqual(len(leftovers), 1)

        resumed = self._run(max_batches=1)
        self.assertEqual(resumed["status"], "partial")
        self.assertFalse(leftovers[0].exists())
        receipt = json.loads(
            (self.run_root / "batches/batch-000001.receipt.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(receipt["output_cleanup"]["files"], 1)
        self.assertEqual(replay._usage_snapshot(self.db), before_usage)

    def test_media_manifest_commit_before_artifact_row_is_cleaned(self) -> None:
        before_usage = replay._usage_snapshot(self.db)
        with patch.object(
            batches.media_module,
            "register_artifact",
            side_effect=RuntimeError("fixture artifact row failure"),
        ), self.assertRaisesRegex(replay.CacheReplayError, "缓存详情物化失败"):
            self._run(max_batches=1)
        manifests = list(self.media_root.rglob("source-*.json"))
        self.assertEqual(len(manifests), 1)
        with closing(
            sqlite3.connect(f"file:{self.db}?mode=ro&immutable=1", uri=True)
        ) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evidence_artifacts"
                ).fetchone()[0],
                0,
            )

        resumed = self._run(max_batches=1)
        self.assertEqual(resumed["status"], "partial")
        receipt = json.loads(
            (self.run_root / "batches/batch-000001.receipt.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(receipt["output_cleanup"]["files"], 1)
        cleanup = json.loads(
            (
                self.run_root
                / "batches/batch-000001.output-cleanup-000001.intent.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(cleanup["files"]["rows"][0][1], str(manifests[0]))
        with closing(
            sqlite3.connect(f"file:{self.db}?mode=ro&immutable=1", uri=True)
        ) as connection:
            local_path, sha256 = connection.execute(
                """
                SELECT local_path,sha256 FROM evidence_artifacts
                WHERE artifact_type='media_source' ORDER BY id LIMIT 1
                """
            ).fetchone()
        rebuilt = batches._resolve_stored_path(str(local_path))
        self.assertEqual(rebuilt, manifests[0])
        self.assertEqual(replay._file_sha256(rebuilt), sha256)
        self.assertEqual(replay._usage_snapshot(self.db), before_usage)

    def test_completed_cleanup_does_not_delete_recreated_media_on_resume(self) -> None:
        with patch.object(
            batches.media_module,
            "register_artifact",
            side_effect=RuntimeError("fixture artifact row failure"),
        ), self.assertRaisesRegex(replay.CacheReplayError, "缓存详情物化失败"):
            self._run(max_batches=1)

        def crash_after_recreated_media(_index: int, _applied: object) -> None:
            raise RuntimeError("fixture crash after recreated media")

        with self.assertRaisesRegex(RuntimeError, "after recreated media"):
            self._run(
                max_batches=1,
                after_batch_applied=crash_after_recreated_media,
            )
        cleanup_receipt = (
            self.run_root
            / "batches/batch-000001.output-cleanup-000001.receipt.json"
        )
        batch_receipt = self.run_root / "batches/batch-000001.receipt.json"
        self.assertTrue(cleanup_receipt.is_file())
        self.assertFalse(batch_receipt.exists())
        with closing(
            sqlite3.connect(f"file:{self.db}?mode=ro&immutable=1", uri=True)
        ) as connection:
            local_path = connection.execute(
                """
                SELECT local_path FROM evidence_artifacts
                WHERE artifact_type='media_source' ORDER BY id LIMIT 1
                """
            ).fetchone()[0]
        rebuilt = batches._resolve_stored_path(str(local_path))
        self.assertTrue(rebuilt.is_file())

        resumed = self._run(max_batches=1)
        self.assertEqual(resumed["status"], "partial")
        self.assertTrue(rebuilt.is_file())
        self.assertTrue(batch_receipt.is_file())

    def test_unowned_pending_orphan_is_preserved_and_blocks_resume(self) -> None:
        orphan = self.derived_root / "unowned.json"

        def crash_with_unowned_file(_index: int, _applied: object) -> None:
            orphan.write_text('{"unowned":true}\n', encoding="utf-8")
            raise RuntimeError("fixture unowned orphan")

        with self.assertRaisesRegex(RuntimeError, "unowned orphan"):
            self._run(max_batches=1, after_batch_applied=crash_with_unowned_file)
        with self.assertRaisesRegex(
            batches.BatchReplayError, "待清理派生 raw 路径形状非法"
        ):
            self._run(max_batches=1)
        self.assertTrue(orphan.is_file())
        self.assertFalse(
            (
                self.run_root
                / "batches/batch-000001.output-cleanup-000001.intent.json"
            ).exists()
        )

    def test_detail_commit_before_media_artifact_resumes_as_media_only(self) -> None:
        with patch.object(
            replay.providers,
            "store_media_source_manifest",
            side_effect=RuntimeError("fixture media artifact failure"),
        ), self.assertRaisesRegex(
            replay.CacheReplayError, "缓存详情物化失败"
        ):
            self._run(max_batches=1)
        self.assertTrue(
            (self.run_root / "batches/batch-000001.intent.json").is_file()
        )
        self.assertFalse(
            (self.run_root / "batches/batch-000001.receipt.json").exists()
        )

        resumed = self._run()
        self.assertEqual(resumed["status"], "succeeded")
        receipt = json.loads(
            (self.run_root / "batches/batch-000001.receipt.json").read_text(
                encoding="utf-8"
            )
        )
        modes = {
            int(row["content_id"]): str(row["mode"])
            for row in receipt["apply"]["results"]
        }
        self.assertEqual(modes[self.content_ids[0]], "media_only")
        self.assertEqual(replay._usage_snapshot(self.db), receipt["apply"]["provider_usage_before"])

    def test_artifact_commit_before_raw_mark_is_recovered_without_provider(self) -> None:
        with patch.object(
            replay.providers,
            "_mark_raw_response_applied",
            side_effect=RuntimeError("fixture raw mark failure"),
        ), self.assertRaisesRegex(
            replay.CacheReplayError, "缓存详情物化失败"
        ):
            self._run(max_batches=1)
        with closing(
            sqlite3.connect(f"file:{self.db}?mode=ro&immutable=1", uri=True)
        ) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM provider_raw_responses WHERE source='live'"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evidence_artifacts WHERE artifact_type='media_source'"
                ).fetchone()[0],
                1,
            )

        resumed = self._run()
        self.assertEqual(resumed["status"], "succeeded")
        receipt = json.loads(
            (self.run_root / "batches/batch-000001.receipt.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(
            receipt["raw_application_recovery"][0]["transition"],
            "live_to_derived_applied_after_artifact_commit",
        )
        self.assertIn(
            receipt["raw_application_recovery"][0]["content_id"],
            receipt["recovered_content_ids"],
        )

    def test_raw_mark_recovery_crash_is_idempotent_on_next_resume(self) -> None:
        before_usage = replay._usage_snapshot(self.db)
        with patch.object(
            replay.providers,
            "_mark_raw_response_applied",
            side_effect=RuntimeError("fixture raw mark failure"),
        ), self.assertRaisesRegex(replay.CacheReplayError, "缓存详情物化失败"):
            self._run(max_batches=1)

        original_recovery = batches._recover_pending_live_raw_with_artifact

        def recover_then_crash(*args: object, **kwargs: object) -> object:
            original_recovery(*args, **kwargs)
            raise RuntimeError("fixture post raw recovery crash")

        with patch.object(
            batches,
            "_recover_pending_live_raw_with_artifact",
            side_effect=recover_then_crash,
        ), self.assertRaisesRegex(RuntimeError, "post raw recovery crash"):
            self._run(max_batches=1)

        resumed = self._run(max_batches=1)
        self.assertEqual(resumed["status"], "partial")
        receipt = json.loads(
            (self.run_root / "batches/batch-000001.receipt.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(receipt["raw_application_recovery"], [])
        self.assertEqual(receipt["recovered_content_ids"], self.content_ids[:1])
        self.assertEqual(receipt["processed_content_ids"], self.content_ids[1:2])
        self.assertEqual(replay._usage_snapshot(self.db), before_usage)

    def test_final_batch_crash_must_commit_pending_receipt_before_completion(self) -> None:
        partial = self._run(max_batches=2)
        self.assertEqual(partial["completed"], 4)

        def crash(_index: int, _applied: object) -> None:
            raise RuntimeError("fixture final batch stop")

        with self.assertRaisesRegex(RuntimeError, "final batch stop"):
            self._run(after_batch_applied=crash)
        intent = self.run_root / "batches/batch-000003.intent.json"
        receipt = self.run_root / "batches/batch-000003.receipt.json"
        self.assertTrue(intent.is_file())
        self.assertFalse(receipt.exists())
        self.assertFalse((self.run_root / "completion.json").exists())

        resumed = self._run()
        self.assertEqual(resumed["status"], "succeeded")
        self.assertEqual(resumed["receipts_total"], 3)
        self.assertEqual(resumed["receipts_created"], 1)
        self.assertTrue(receipt.is_file())
        self.assertTrue((self.run_root / "completion.json").is_file())
        recovered = json.loads(receipt.read_text(encoding="utf-8"))
        self.assertEqual(recovered["recovered_content_ids"], self.content_ids[4:])
        self.assertEqual(recovered["processed_content_ids"], [])

    def test_sigkill_after_apply_recovers_from_durable_intent(self) -> None:
        child = os.fork()
        if child == 0:
            try:
                self._run(
                    max_batches=1,
                    after_batch_applied=lambda _index, _applied: os.kill(
                        os.getpid(), signal.SIGKILL
                    ),
                )
            finally:
                os._exit(97)
        _pid, status = os.waitpid(child, 0)
        self.assertTrue(os.WIFSIGNALED(status))
        self.assertEqual(os.WTERMSIG(status), signal.SIGKILL)
        intent = self.run_root / "batches/batch-000001.intent.json"
        receipt = self.run_root / "batches/batch-000001.receipt.json"
        self.assertTrue(intent.is_file())
        self.assertFalse(receipt.exists())
        counts_after_kill = self._counts()

        resumed = self._run(max_batches=1)
        self.assertEqual(resumed["status"], "partial")
        self.assertEqual(resumed["receipts_created"], 1)
        recovered = json.loads(receipt.read_text(encoding="utf-8"))
        self.assertEqual(recovered["recovered_content_ids"], self.content_ids[:2])
        self.assertEqual(recovered["processed_content_ids"], [])
        self.assertEqual(self._counts(), counts_after_kill)

    def test_running_detail_slot_is_recovered_before_plan_and_retried(self) -> None:
        def interrupt_after_claim(_index: int, content_ids: object) -> None:
            first = list(content_ids)[0]
            claim_content_slot(
                db_path=self.db,
                content_id=int(first),
                stage="detail",
                window_key="lifetime",
                provider="TikHub",
                adapter_version="tikhub-discovery-derived-v8.1",
                allow_terminal_retry=True,
            )
            raise KeyboardInterrupt

        with self.assertRaises(KeyboardInterrupt):
            self._run(max_batches=1, before_batch_apply=interrupt_after_claim)
        with closing(
            sqlite3.connect(f"file:{self.db}?mode=ro", uri=True)
        ) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM fetch_slots WHERE stage='detail' AND status='running'"
                ).fetchone()[0],
                1,
            )

        resumed = self._run(max_batches=1)
        self.assertEqual(resumed["status"], "partial")
        receipt = json.loads(
            (self.run_root / "batches/batch-000001.receipt.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(
            receipt["interrupted_slot_recovery"][0]["transition"],
            "running_to_retryable_failed",
        )
        self.assertEqual(self._counts(), (3, 4, 3, 2, 0))
        self.assertFalse(Path(f"{self.db}-wal").exists())
        self.assertFalse(Path(f"{self.db}-shm").exists())

    def test_concurrent_claim_and_disk_gate_fail_closed(self) -> None:
        paths = batches._paths(self.run_root)
        with batches._exclusive_claim(paths.claim), self.assertRaisesRegex(
            batches.BatchReplayError, "另一个批处理进程"
        ):
            self._run(max_batches=1)
        with batches._exclusive_claim(
            batches._database_claim_path(self.db)
        ), self.assertRaisesRegex(batches.BatchReplayError, "另一个批处理进程"):
            self._run(run_root=self.root / "second-run", max_batches=1)
        second_db = self.root / "second-clone.sqlite3"
        shutil.copy2(self.db, second_db)
        with batches._exclusive_claim(
            batches._output_claim_path(self.derived_root)
        ), self.assertRaisesRegex(batches.BatchReplayError, "另一个批处理进程"):
            self._run(
                db_path=second_db,
                run_root=self.root / "second-database-run",
                max_batches=1,
            )
        free = batches._disk_gate(self.run_root, min_free_bytes=1)["free"]
        with self.assertRaisesRegex(batches.BatchReplayError, "磁盘剩余空间不足"):
            self._run(max_batches=1, min_free_bytes=free + 1)
        external = self.root / "external-run"
        external.mkdir()
        alias = self.root / "run-alias"
        alias.symlink_to(external, target_is_directory=True)
        with self.assertRaisesRegex(batches.BatchReplayError, "不得是符号链接"):
            self._run(run_root=alias, max_batches=1)
        with self.assertRaisesRegex(batches.BatchReplayError, "相同或相互包含"):
            self._run(run_root=self.derived_root / "audit", max_batches=1)

    def test_code_snapshot_cannot_be_rebound_outside_run_root(self) -> None:
        self._run(max_batches=1)
        contract_path = self.run_root / "run-contract.json"
        contract = json.loads(contract_path.read_text(encoding="utf-8"))
        original = Path(contract["code_snapshots"]["storage"]["path"])
        external = self.root / "external-storage.py"
        external.write_bytes(original.read_bytes())
        external.chmod(0o444)
        contract["code_snapshots"]["storage"]["path"] = str(external)
        contract_path.chmod(0o600)
        contract_path.write_text(
            json.dumps(contract, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(batches.BatchReplayError, "代码快照路径越界"):
            self._run()

    def test_non_target_allowed_table_drift_is_blocked_in_same_batch(self) -> None:
        original = replay.apply_replay_plan

        def apply_then_tamper(*args: object, **kwargs: object) -> object:
            result = original(*args, **kwargs)
            with closing(connect(self.db)) as connection:
                connection.execute(
                    """
                    UPDATE provider_raw_responses SET source='derived_applied'
                    WHERE account_id=? AND content_id IS NULL
                    """,
                    (self.account_id,),
                )
                connection.commit()
            replay._finalize_disposable_database(self.db)
            return result

        with patch.object(
            replay, "apply_replay_plan", side_effect=apply_then_tamper
        ), self.assertRaisesRegex(
            batches.BatchReplayError, "关键禁止变更数据"
        ):
            self._run(max_batches=1)
        self.assertFalse(
            (self.run_root / "batches/batch-000001.receipt.json").exists()
        )

    def test_protected_table_drift_is_blocked_before_receipt(self) -> None:
        original = replay.apply_replay_plan

        def apply_then_tamper(*args: object, **kwargs: object) -> object:
            result = original(*args, **kwargs)
            with closing(connect(self.db)) as connection:
                connection.execute(
                    "UPDATE accounts SET operator_name='外部漂移' WHERE id=?",
                    (self.account_id,),
                )
                connection.commit()
            replay._finalize_disposable_database(self.db)
            return result

        with patch.object(
            replay, "apply_replay_plan", side_effect=apply_then_tamper
        ), self.assertRaisesRegex(
            batches.BatchReplayError, "关键禁止变更数据"
        ):
            self._run(max_batches=1)
        self.assertFalse(
            (self.run_root / "batches/batch-000001.receipt.json").exists()
        )

    def test_derived_detail_body_must_match_frozen_discovery_item(self) -> None:
        original = replay.apply_replay_plan

        def apply_then_tamper(*args: object, **kwargs: object) -> object:
            result = original(*args, **kwargs)
            with closing(
                sqlite3.connect(f"file:{self.db}?mode=ro&immutable=1", uri=True)
            ) as connection:
                row = connection.execute(
                    """
                    SELECT id,local_path FROM provider_raw_responses
                    WHERE source='derived_applied' ORDER BY id LIMIT 1
                    """
                ).fetchone()
            self.assertIsNotNone(row)
            raw_id, stored_path = int(row[0]), str(row[1])
            raw_path = batches._resolve_stored_path(stored_path)
            value = json.loads(raw_path.read_text(encoding="utf-8"))
            value["data"]["title"] = "被篡改但重新计算哈希的标题"
            body = (
                json.dumps(
                    value,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                )
                + "\n"
            ).encode("utf-8")
            digest = replay._sha256_bytes(body)
            target = raw_path.with_name(
                f"{raw_path.stem.rsplit('-', 1)[0]}-{digest[:12]}.json"
            )
            target.write_bytes(body)
            raw_path.unlink()
            with closing(connect(self.db)) as connection:
                connection.execute(
                    """
                    UPDATE provider_raw_responses
                    SET local_path=?,sha256=?,byte_size=? WHERE id=?
                    """,
                    (str(target), digest, len(body), raw_id),
                )
                connection.commit()
            replay._finalize_disposable_database(self.db)
            return result

        with patch.object(
            replay, "apply_replay_plan", side_effect=apply_then_tamper
        ), self.assertRaisesRegex(
            batches.BatchReplayError, "批次证据文件哈希或字节数漂移"
        ):
            self._run(max_batches=1)

    def test_media_metadata_must_match_manifest_urls(self) -> None:
        original = replay.apply_replay_plan

        def apply_then_tamper(*args: object, **kwargs: object) -> object:
            result = original(*args, **kwargs)
            with closing(connect(self.db)) as connection:
                row = connection.execute(
                    """
                    SELECT id,metadata_json FROM evidence_artifacts
                    WHERE artifact_type='media_source' ORDER BY id LIMIT 1
                    """
                ).fetchone()
                metadata = json.loads(str(row["metadata_json"]))
                metadata["source_count"] = 999
                connection.execute(
                    "UPDATE evidence_artifacts SET metadata_json=? WHERE id=?",
                    (
                        json.dumps(
                            metadata,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        int(row["id"]),
                    ),
                )
                connection.commit()
            replay._finalize_disposable_database(self.db)
            return result

        with patch.object(
            replay, "apply_replay_plan", side_effect=apply_then_tamper
        ), self.assertRaisesRegex(
            batches.BatchReplayError, "媒体证据未绑定详情 raw"
        ):
            self._run(max_batches=1)

    def test_receipt_chain_tamper_is_rejected(self) -> None:
        self._run(max_batches=1)
        receipt_path = self.run_root / "batches/batch-000001.receipt.json"
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        receipt["processed_content_ids"] = receipt["processed_content_ids"][:-1]
        receipt_path.write_text(
            json.dumps(receipt, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(batches.BatchReplayError, "receipt 链不合法"):
            self._run()

    def test_completion_binds_full_receipt_files_and_recomputed_evidence(self) -> None:
        completed = self._run()
        self.assertEqual(completed["status"], "succeeded")
        completion = json.loads(
            (self.run_root / "completion.json").read_text(encoding="utf-8")
        )
        self.assertEqual(len(completion["batch_chain"]["rows"]), 3)
        receipt_path = self.run_root / "batches/batch-000003.receipt.json"
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        receipt["artifacts"]["rows_sha256"] = "0" * 64
        receipt_path.write_text(
            json.dumps(receipt, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(batches.BatchReplayError, "制品证据漂移"):
            self._run()

    def test_completed_output_inventory_rejects_orphan_file(self) -> None:
        self.assertEqual(self._run()["status"], "succeeded")
        orphan = self.derived_root / "orphan.json"
        orphan.write_text("{}\n", encoding="utf-8")
        with self.assertRaisesRegex(
            batches.BatchReplayError, "文件系统清单与数据库不一致"
        ):
            self._run()

    def test_resume_revalidates_prior_receipt_artifacts_before_next_batch(self) -> None:
        first = self._run(max_batches=1)
        self.assertEqual(first["status"], "partial")
        with closing(
            sqlite3.connect(f"file:{self.db}?mode=ro&immutable=1", uri=True)
        ) as connection:
            stored_path = str(
                connection.execute(
                    """
                    SELECT local_path FROM evidence_artifacts
                    WHERE artifact_type='media_source' ORDER BY id LIMIT 1
                    """
                ).fetchone()[0]
            )
        batches._resolve_stored_path(stored_path).unlink()
        with self.assertRaisesRegex(
            batches.BatchReplayError, "媒体证据不存在"
        ):
            self._run(max_batches=1)

    def test_unknown_run_record_is_rejected_before_contract_creation(self) -> None:
        self.run_root.mkdir()
        (self.run_root / "unknown.txt").write_text("unknown\n", encoding="utf-8")
        with self.assertRaisesRegex(batches.BatchReplayError, "运行目录存在未知文件"):
            self._run(max_batches=1)

    def test_formal_database_rejection_has_no_controller_side_effects(self) -> None:
        before = sorted(path.name for path in self.root.iterdir())
        with patch.object(
            replay, "FORMAL_DB", self.db
        ), self.assertRaisesRegex(replay.CacheReplayError, "禁止直接写正式数据库"):
            self._run(max_batches=1)
        self.assertEqual(sorted(path.name for path in self.root.iterdir()), before)
        self.assertFalse(self.run_root.exists())

    def test_nested_new_output_parents_are_created_before_locking(self) -> None:
        derived = self.root / "new" / "deep" / "derived"
        media = self.root / "new" / "other" / "media"
        result = self._run(
            derived_raw_root=derived,
            media_root=media,
            max_batches=1,
        )
        self.assertEqual(result["status"], "partial")
        self.assertTrue(derived.is_dir())
        self.assertTrue(media.is_dir())

    def test_database_drift_after_receipt_is_rejected_before_next_batch(self) -> None:
        self._run(max_batches=1)
        with closing(connect(self.db)) as connection:
            connection.execute(
                "UPDATE content_items SET title='外部漂移' WHERE id=?",
                (self.content_ids[0],),
            )
            connection.commit()
        replay._finalize_disposable_database(self.db)
        with self.assertRaisesRegex(
            batches.BatchReplayError, "数据库与最新 receipt 不一致"
        ):
            self._run()


if __name__ == "__main__":
    unittest.main()
