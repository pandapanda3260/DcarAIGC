from __future__ import annotations

import gc
import hashlib
import io
import json
import os
import signal
import socket
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.parse
import urllib.request
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts import run_full_history_cache_batches as step3_controller
from scripts import run_local_analysis_canary as canary
from v8 import duplicates, evaluation, media, providers
from v8.storage import connect, initialize_database, now_utc


class _Response:
    def __init__(
        self,
        url: str,
        body: bytes,
        *,
        content_type: str = "video/mp4",
    ) -> None:
        self._url = url
        self._body = io.BytesIO(body)
        self.headers = {
            "Content-Length": str(len(body)),
            "Content-Type": content_type,
        }
        self.status = 200
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        return self._body.read(size)

    def geturl(self) -> str:
        return self._url

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *_args) -> None:
        self.close()


class LocalAnalysisCanaryControllerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.step3_root = self.root / "step3"
        self.analysis_root = self.root / "analysis"
        self.step3_root.mkdir()
        self.analysis_root.mkdir()
        self.global_claim_patch = patch.object(
            canary,
            "_global_claim_path",
            return_value=self.analysis_root / "global.claim",
        )
        self.global_claim_patch.start()
        self.addCleanup(self.global_claim_patch.stop)
        self.source_db = self.step3_root / "step3-final.sqlite3"
        self.step3_run = self.step3_root / "run"
        self.step3_batches = self.step3_run / "batches"
        self.step3_batches.mkdir(parents=True)
        self.source_completion = self.step3_run / "completion.json"
        self.db = self.analysis_root / "analysis-work.sqlite3"
        self.media_root = self.analysis_root / "isolated-media"
        self.run_root = self.analysis_root / "run"
        self.source_root = self.step3_root / "media"
        self.raw_root = self.step3_root / "derived"
        self.raw_root.mkdir()
        self.capture_raw_root_patch = patch.object(
            canary.capture_module, "RAW_ROOT", self.raw_root
        )
        self.capture_raw_root_patch.start()
        self.addCleanup(self.capture_raw_root_patch.stop)
        self.urls = ["https://v1.douyinvod.com/canary.mp4"]
        self.source_fixtures: dict[int, dict[str, object]] = {}
        captured_at = now_utc()
        with closing(connect(self.source_db)) as connection:
            initialize_database(connection)
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id,version,status,definition,created_at,published_at
                ) VALUES ('canary-taxonomy','canary-v1','published','{}',?,?)
                """,
                (captured_at, captured_at),
            )
            connection.execute(
                """
                INSERT INTO evaluation_releases(
                    id,rule_version,taxonomy_version,matcher_rule_sha256,status,
                    created_at,updated_at,activated_at
                ) VALUES ('canary-release','evaluation-v8','canary-v1',?,'active',?,?,?)
                """,
                ("a" * 64, captured_at, captured_at, captured_at),
            )
            connection.execute(
                """
                INSERT INTO accounts(
                    id,phone,phone_normalized,created_at,updated_at
                ) VALUES (39,'fixture-39','fixture-39',?,?)
                """,
                (captured_at, captured_at),
            )
            connection.execute(
                """
                INSERT INTO content_items(
                    id,account_id,link_id,platform,platform_content_id,canonical_url,title,body,
                    content_type,source_group,imported_at,created_at,updated_at
                ) VALUES (1,39,'C4N4RY','douyin','canary-1',
                          'https://www.douyin.com/video/canary-1','完整标题','完整正文',
                          'video','history-backfill',?,?,?)
                """,
                (captured_at, captured_at, captured_at),
            )
            raw_path = self.raw_root / "source.json"
            discovery_path = self.raw_root / "discovery.json"
            discovery_path.write_text('{"data":[]}\n', encoding="utf-8")
            discovery_sha = canary._sha256_file(discovery_path)
            raw_body = {
                "data": {
                    "_evidence_captured_at": captured_at,
                    "account_name": "fixture",
                    "account_uid": "fixture-uid",
                    "body": "完整正文",
                    "content_type": "video",
                    "media_urls": self.urls,
                    "published_at": captured_at,
                    "title": "完整标题",
                },
                "derived_from_operation": "douyin_user_posts",
                "source_captured_at": captured_at,
                "source_raw_response_id": 999,
                "source_sha256": discovery_sha,
                "stage": "detail",
            }
            raw_path.write_text(
                json.dumps(raw_body, ensure_ascii=False, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            self.source_fixtures[1] = {
                "link_id": "C4N4RY",
                "urls": list(self.urls),
                "raw_path": raw_path,
            }
            connection.execute(
                """
                INSERT INTO provider_raw_responses(
                    id,account_id,content_id,provider,operation,local_path,sha256,byte_size,
                    http_status,captured_at,source
                ) VALUES (999,39,NULL,'fixture','douyin_user_posts',?,?,?,200,?,'live_applied')
                """,
                (
                    str(discovery_path),
                    discovery_sha,
                    discovery_path.stat().st_size,
                    captured_at,
                ),
            )
            connection.execute(
                """
                INSERT INTO provider_raw_responses(
                    id,content_id,provider,operation,local_path,sha256,byte_size,
                    http_status,captured_at,source
                ) VALUES (1,1,'fixture','douyin_video_detail',?,?,?,200,?,'derived_applied')
                """,
                (
                    str(raw_path),
                    canary._sha256_file(raw_path),
                    raw_path.stat().st_size,
                    captured_at,
                ),
            )
            connection.execute(
                """
                INSERT INTO content_metric_snapshots(
                    content_id,captured_at,window_key,view_count,status,source
                ) VALUES (1,?,'fixture-window',123,'available','fixture')
                """,
                (captured_at,),
            )
            connection.execute(
                """
                INSERT INTO provider_usage(
                    provider,operation,request_attempts,billed_requests,amount,recorded_at
                ) VALUES ('fixture','existing',1,1,0.01,?)
                """,
                (captured_at,),
            )
            connection.execute(
                """
                INSERT INTO provider_budget_batches(
                    id,purpose,provider,operation,currency,verified_unit_price,
                    max_billable_requests,max_amount,pilot_size,daily_quota,
                    consumed_requests,consumed_amount,price_verified_at,status,
                    created_at,updated_at
                ) VALUES ('fixture-budget','fixture-purpose','fixture','existing','USD',
                          0.01,10,0.1,1,10,1,0.01,?,'approved',?,?)
                """,
                (captured_at, captured_at, captured_at),
            )
            connection.commit()
        media.store_media_source_manifest(
            1,
            media_kind="video",
            urls=self.urls,
            raw_response_id=1,
            db_path=self.source_db,
            media_root=self.source_root,
        )
        self.model = self.root / "whisper-model"
        self.model.mkdir()
        self.binary = self.root / "vision-ocr"
        self.binary.write_bytes(b"fixture-binary")
        self.binary.chmod(0o700)
        self.tools = {
            "ocr_binary": self._tool(self.binary),
            "ffmpeg": self._tool(self.binary),
            "ffprobe": self._tool(self.binary),
            "whisper": {
                "path": str(self.model),
                "model_id": "fixture-whisper",
                "revision": "fixture-revision",
            },
        }
        self._finalize(self.source_db)
        self._refresh_step3_proof()
        self.calls = {"media": 0, "evaluation": 0, "fingerprint": 0}

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def _tool(path: Path) -> dict[str, object]:
        return {
            "path": str(path),
            "sha256": canary._sha256_file(path),
            "byte_size": path.stat().st_size,
        }

    def _finalize(self, path: Path) -> None:
        gc.collect()
        connection = sqlite3.connect(path)
        try:
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            connection.execute("PRAGMA journal_mode=DELETE")
        finally:
            connection.close()

    def _source_identity(self) -> dict[str, object]:
        metadata = self.source_db.stat()
        return {
            "path": str(self.source_db),
            "sha256": canary._sha256_file(self.source_db),
            "bytes": metadata.st_size,
            "inode": metadata.st_ino,
            "nlink": metadata.st_nlink,
        }

    def _add_source_content(self, content_id: int) -> None:
        if content_id in self.source_fixtures:
            raise AssertionError(f"duplicate fixture content: {content_id}")
        captured_at = now_utc()
        link_id = f"C4N{content_id:03d}"
        urls = [f"https://v{content_id}.douyinvod.com/canary-{content_id}.mp4"]
        raw_path = self.raw_root / f"source-{content_id}.json"
        discovery_path = self.raw_root / f"discovery-{content_id}.json"
        discovery_path.write_text('{"data":[]}\n', encoding="utf-8")
        discovery_sha = canary._sha256_file(discovery_path)
        discovery_id = 1000 + content_id
        title = f"完整标题{content_id}"
        body = f"完整正文{content_id}"
        raw_body = {
            "data": {
                "_evidence_captured_at": captured_at,
                "account_name": "fixture",
                "account_uid": f"fixture-uid-{content_id}",
                "body": body,
                "content_type": "video",
                "media_urls": urls,
                "published_at": captured_at,
                "title": title,
            },
            "derived_from_operation": "douyin_user_posts",
            "source_captured_at": captured_at,
            "source_raw_response_id": discovery_id,
            "source_sha256": discovery_sha,
            "stage": "detail",
        }
        raw_path.write_text(
            json.dumps(raw_body, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        with closing(connect(self.source_db)) as connection:
            connection.execute(
                """
                INSERT INTO content_items(
                    id,account_id,link_id,platform,platform_content_id,canonical_url,title,body,
                    content_type,source_group,imported_at,created_at,updated_at
                ) VALUES (?,39,?,'douyin',?,?,?,?,
                          'video','history-backfill',?,?,?)
                """,
                (
                    content_id,
                    link_id,
                    f"canary-{content_id}",
                    f"https://www.douyin.com/video/canary-{content_id}",
                    title,
                    body,
                    captured_at,
                    captured_at,
                    captured_at,
                ),
            )
            connection.execute(
                """
                INSERT INTO provider_raw_responses(
                    id,account_id,content_id,provider,operation,local_path,sha256,byte_size,
                    http_status,captured_at,source
                ) VALUES (?,39,NULL,'fixture','douyin_user_posts',?,?,?,200,?,'live_applied')
                """,
                (
                    discovery_id,
                    str(discovery_path),
                    discovery_sha,
                    discovery_path.stat().st_size,
                    captured_at,
                ),
            )
            connection.execute(
                """
                INSERT INTO provider_raw_responses(
                    id,content_id,provider,operation,local_path,sha256,byte_size,
                    http_status,captured_at,source
                ) VALUES (?,?,'fixture','douyin_video_detail',?,?,?,200,?,'derived_applied')
                """,
                (
                    content_id,
                    content_id,
                    str(raw_path),
                    canary._sha256_file(raw_path),
                    raw_path.stat().st_size,
                    captured_at,
                ),
            )
            connection.commit()
        media.store_media_source_manifest(
            content_id,
            media_kind="video",
            urls=urls,
            raw_response_id=content_id,
            db_path=self.source_db,
            media_root=self.source_root,
        )
        self.source_fixtures[content_id] = {
            "link_id": link_id,
            "urls": urls,
            "raw_path": raw_path,
        }
        self._refresh_step3_proof()

    def _configure_douyin_image_source(
        self,
        candidate_groups: list[list[str]],
        *,
        media_urls: list[str] | None = None,
        aweme_items: list[dict[str, object]] | None = None,
    ) -> None:
        urls = list(media_urls) if media_urls is not None else [
            url for group in candidate_groups for url in group
        ]
        self.urls = urls
        raw_path = self.raw_root / "source.json"
        discovery_path = self.raw_root / "discovery.json"
        raw_body = json.loads(raw_path.read_text())
        raw_body["data"]["content_type"] = "image"
        raw_body["data"]["media_urls"] = urls
        images = [
            {
                "download_url_list": list(group[:2]),
                "url_list": list(group[2:]),
            }
            for group in candidate_groups
        ]
        discovery_body = {
            "data": {
                "aweme_list": (
                    aweme_items
                    if aweme_items is not None
                    else [{"aweme_id": "canary-1", "images": images}]
                )
            }
        }
        discovery_path.write_text(
            json.dumps(discovery_body, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        discovery_sha = canary._sha256_file(discovery_path)
        raw_body["source_sha256"] = discovery_sha
        raw_path.write_text(
            json.dumps(raw_body, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        with closing(connect(self.source_db)) as connection:
            connection.execute(
                "UPDATE content_items SET content_type='image' WHERE id=1"
            )
            connection.execute(
                "UPDATE provider_raw_responses SET sha256=?,byte_size=? WHERE id=999",
                (discovery_sha, discovery_path.stat().st_size),
            )
            connection.execute(
                "UPDATE provider_raw_responses SET sha256=?,byte_size=? WHERE id=1",
                (canary._sha256_file(raw_path), raw_path.stat().st_size),
            )
            connection.execute(
                "DELETE FROM evidence_artifacts WHERE artifact_type='media_source'"
            )
            connection.commit()
        for path in sorted(self.source_root.rglob("*"), reverse=True):
            if path.is_file():
                path.unlink()
        media.store_media_source_manifest(
            1,
            media_kind="image",
            urls=urls,
            raw_response_id=1,
            db_path=self.source_db,
            media_root=self.source_root,
        )
        self._refresh_step3_proof()

    def _step3_target_row(self, content_id: int = 1) -> list[object]:
        contract = json.loads((self.step3_run / "run-contract.json").read_text())
        return next(
            row
            for row in contract["target_contract"]["rows"]
            if int(row[0]) == content_id
        )

    def _refresh_step3_proof(self) -> None:
        self._finalize(self.source_db)
        target_ids = sorted(self.source_fixtures)
        target_ids_sha = hashlib.sha256(
            json.dumps(target_ids, separators=(",", ":"), sort_keys=True).encode()
        ).hexdigest()
        target_rows = []
        with closing(canary._immutable_connection(self.source_db)) as connection:
            for content_id in target_ids:
                raw_path = Path(
                    str(self.source_fixtures[content_id]["raw_path"])
                )
                raw_body = json.loads(raw_path.read_text())
                discovery = connection.execute(
                    "SELECT id,operation,sha256,captured_at "
                    "FROM provider_raw_responses WHERE id=?",
                    (int(raw_body["source_raw_response_id"]),),
                ).fetchone()
                target_rows.append(
                    [
                        content_id,
                        "douyin",
                        "douyin_video_detail",
                        int(discovery["id"]),
                        str(discovery["operation"]),
                        str(discovery["sha256"]),
                        str(discovery["captured_at"]),
                        canary._compact_json_sha256(raw_body["data"]),
                        canary._sha256_file(raw_path),
                        raw_path.stat().st_size,
                    ]
                )
        source_identity = self._source_identity()
        provider_usage = {
            "provider_usage": {
                "rows": 1,
                "max_id": 1,
                "billed": 1,
                "amount": 0.01,
            },
            "provider_budgets": {
                "rows": 1,
                "requests": 1,
                "amount": 0.01,
            },
        }
        contract = {
            "version": 1,
            "database": source_identity,
            "provider_usage": provider_usage,
            "min_free_bytes": 1,
            "target_count": len(target_ids),
            "target_ids": target_ids,
            "target_ids_sha256": target_ids_sha,
            "media_root": str(self.source_root.resolve()),
            "derived_raw_root": str(self.raw_root.resolve()),
            "target_contract": {
                "fields": list(canary.STEP3_TARGET_CONTRACT_FIELDS),
                "rows": target_rows,
                "rows_sha256": canary._compact_json_sha256(target_rows),
            },
        }
        self.step3_run.mkdir(exist_ok=True)
        self.step3_batches.mkdir(exist_ok=True)
        step3_contract = self.step3_run / "run-contract.json"
        step3_contract.write_text(
            json.dumps(contract, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        contract_sha = canary._sha256_file(step3_contract)
        intent_path = self.step3_batches / "batch-000001.intent.json"
        intent_body = {
            "version": 1,
            "batch_index": 1,
            "content_ids": target_ids,
            "content_ids_sha256": canary._compact_json_sha256(target_ids),
            "contract_sha256": contract_sha,
            "before_database": source_identity,
            "previous_receipt_sha256": None,
        }
        intent_path.write_text(
            json.dumps(intent_body, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        receipt = self.step3_batches / "batch-000001.receipt.json"
        artifacts = {
            "contents": len(target_ids),
            "row_fields": [
                "content_id",
                "detail_raw_response_id",
                "detail_raw_sha256",
                "media_artifact_id",
                "media_artifact_sha256",
            ],
            "rows_sha256": "a" * 64,
        }
        critical = {
            "protected": {"fixture": [len(target_ids), "b" * 64]},
            "allowed_prefix": {"fixture": [len(target_ids), "c" * 64]},
            "allowed_append_scope": {
                "fixture": {
                    "new_rows": len(target_ids),
                    "rows_sha256": "d" * 64,
                }
            },
        }
        output_inventory = {
            "derived_raw": {
                "count": len(target_ids),
                "rows_sha256": "e" * 64,
            },
            "media": {
                "count": len(target_ids),
                "rows_sha256": "f" * 64,
            },
        }
        receipt_body = {
            "version": 1,
            "batch_index": 1,
            "content_ids": target_ids,
            "content_ids_sha256": canary._compact_json_sha256(target_ids),
            "intent_sha256": canary._sha256_file(intent_path),
            "recovered_content_ids": [],
            "processed_content_ids": target_ids,
            "interrupted_slot_recovery": [],
            "raw_application_recovery": [],
            "output_cleanup": None,
            "apply": {
                "status": "succeeded",
                "processed": len(target_ids),
                "processed_ids_sha256": canary._compact_json_sha256(target_ids),
                "provider_calls": 0,
                "provider_usage_before": provider_usage,
                "provider_usage_after": provider_usage,
                "already_materialized_requested": [],
                "derived_raw_root": str(self.raw_root),
                "media_root": str(self.source_root),
                "storage": {
                    "journal_mode": "delete",
                    "checkpoint": [0, 0, 0],
                },
                "results": [
                    {
                        "content_id": content_id,
                        "mode": "detail_and_media",
                        "created": ["detail"],
                        "replayed": [],
                        "already_succeeded": [],
                        "failed": [],
                    }
                    for content_id in target_ids
                ],
            },
            "artifacts": artifacts,
            "critical_unchanged": critical,
            "output_inventory": output_inventory,
            "after_database": source_identity,
            "elapsed_seconds": 0.0,
            "disk": {
                label: {
                    "total": 100,
                    "used": 1,
                    "free": 99,
                    "device": self.root.stat().st_dev,
                    "anchor": str(self.root),
                }
                for label in (
                    "database",
                    "derived_raw_root",
                    "media_root",
                    "run_root",
                )
            },
        }
        receipt.write_text(
            json.dumps(receipt_body, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        batch_rows = [
            [
                1,
                canary._sha256_file(intent_path),
                canary._sha256_file(receipt),
                0,
                canary._compact_json_sha256([]),
            ]
        ]
        evidence_rows = [
            [
                1,
                canary._compact_json_sha256(artifacts),
                canary._compact_json_sha256(critical),
                canary._compact_json_sha256(output_inventory),
            ]
        ]
        completion = {
            "version": 1,
            "status": "succeeded",
            "run_root": str(self.step3_run),
            "contract_sha256": contract_sha,
            "target_count": len(target_ids),
            "receipts_total": 1,
            "database": source_identity,
            "completion": {
                "database": source_identity,
                "completed": len(target_ids),
                "ready": 0,
            },
            "batch_chain": {
                "fields": [
                    "batch_index",
                    "intent_file_sha256",
                    "receipt_file_sha256",
                    "cleanup_rounds",
                    "cleanup_file_chain_sha256",
                ],
                "rows": batch_rows,
                "rows_sha256": canary._compact_json_sha256(batch_rows),
            },
            "receipt_evidence": {
                "fields": [
                    "batch_index",
                    "artifacts_evidence_sha256",
                    "critical_evidence_sha256",
                    "output_inventory_sha256",
                ],
                "rows": evidence_rows,
                "rows_sha256": canary._compact_json_sha256(evidence_rows),
                "receipt_files_sha256": canary._compact_json_sha256(batch_rows),
            },
        }
        self.source_completion.write_text(
            json.dumps(completion, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        self.source_db_sha = canary._sha256_file(self.source_db)
        self.source_completion_sha = canary._sha256_file(self.source_completion)

    def _artifact(
        self,
        connection: sqlite3.Connection,
        *,
        content_id: int = 1,
        artifact_type: str,
        path: Path,
        processor_version: str,
    ) -> sqlite3.Row:
        body = path.read_bytes()
        captured_at = now_utc()
        connection.execute(
            """
            INSERT INTO evidence_artifacts(
                content_id,artifact_type,local_path,status,byte_size,sha256,
                captured_at,processor_version,metadata_json,created_at
            ) VALUES (?,?,?, 'available',?,?,?,?, '{}',?)
            ON CONFLICT(content_id,artifact_type,local_path) DO UPDATE SET
                byte_size=excluded.byte_size,sha256=excluded.sha256,
                status='available',processor_version=excluded.processor_version
            """,
            (
                content_id,
                artifact_type,
                str(path),
                len(body),
                hashlib.sha256(body).hexdigest(),
                captured_at,
                processor_version,
                captured_at,
            ),
        )
        return connection.execute(
            """
            SELECT * FROM evidence_artifacts
            WHERE content_id=? AND artifact_type=? AND local_path=?
            """,
            (content_id, artifact_type, str(path)),
        ).fetchone()

    def _slot(
        self,
        connection: sqlite3.Connection,
        *,
        content_id: int = 1,
        processor_type: str,
        processor_version: str,
        artifact: sqlite3.Row,
        source_sha: str,
    ) -> None:
        captured_at = now_utc()
        connection.execute(
            """
            INSERT INTO media_processing_slots(
                content_id,source_sha256,processor_type,processor_version,status,
                output_artifact_id,attempt_count,error_message,created_at,updated_at
            ) VALUES (?,?,?,?, 'succeeded',?,1,NULL,?,?)
            ON CONFLICT(content_id,source_sha256,processor_type,processor_version)
            DO UPDATE SET status='succeeded',output_artifact_id=excluded.output_artifact_id,
                          error_message=NULL,updated_at=excluded.updated_at
            """,
            (
                content_id,
                source_sha,
                processor_type,
                processor_version,
                int(artifact["id"]),
                captured_at,
                captured_at,
            ),
        )

    def _fake_media(self, content_id: int, **kwargs):
        self.calls["media"] += 1
        fixture = self.source_fixtures[content_id]
        urls = list(self.urls if content_id == 1 else fixture["urls"])
        link_id = str(fixture["link_id"])
        self.assertEqual(Path(kwargs["media_root"]), self.media_root)
        self.assertEqual(Path(kwargs["whisper_model_path"]), self.model)
        self.assertEqual(Path(kwargs["ocr_binary"]), self.binary)
        self.assertTrue(kwargs["require_exact_response_url"])
        self.assertEqual(kwargs["download_urls"], urls)
        self.assertFalse(kwargs["reuse_existing_downloads"])
        root = Path(kwargs["media_root"])
        content_root = root / link_id
        content_root.mkdir(parents=True, exist_ok=True)
        media_path = content_root / "source.mp4"
        frame_path = content_root / "frame-000.jpg"
        frames_path = content_root / "frames.json"
        asr_path = content_root / "asr.json"
        ocr_path = content_root / "ocr.json"
        with kwargs["urlopen_fn"](
            urllib.request.Request(urls[0]), timeout=90
        ) as response:
            media_path.write_bytes(response.read())
        frame_path.write_bytes(b"frame" * 1000)
        frames_path.write_text(
            json.dumps(
                {
                    "status": "success",
                    "duration_seconds": 1.0,
                    "frames": [
                        {
                            "path": str(frame_path),
                            "sha256": canary._sha256_file(frame_path),
                        }
                    ],
                    "contact_sheet": None,
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        asr_path.write_text(
            json.dumps(
                {
                    "status": "success",
                    "text": "这是完整的汽车视频语音证据内容可以支持分析判断",
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        ocr_path.write_text(
            json.dumps(
                {
                    "status": "success",
                    "combined_text": "这是完整的汽车视频画面文字证据可以支持分析判断",
                    "ocr_observation_count": 1,
                    "source_count": 1,
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        versions = media.processor_versions()
        with closing(connect(self.db)) as connection:
            source_artifact = connection.execute(
                """
                SELECT sha256 FROM evidence_artifacts
                WHERE content_id=? AND artifact_type='media_source'
                  AND status='available' AND processor_version=?
                ORDER BY id DESC LIMIT 1
                """,
                (content_id, media.MEDIA_SOURCE_VERSION),
            ).fetchone()
            self.assertIsNotNone(source_artifact)
            download_slot_id, cached = media._claim_processing_slot(
                connection,
                content_id=content_id,
                source_sha256=str(source_artifact["sha256"]),
                processor_type="download",
                processor_version=media.VIDEO_DOWNLOAD_VERSION,
            )
            self.assertIsNone(cached)
            media_row = self._artifact(
                connection,
                content_id=content_id,
                artifact_type="media",
                path=media_path,
                processor_version="provider-media-v8.0",
            )
            frames_row = self._artifact(
                connection,
                content_id=content_id,
                artifact_type="frames_manifest",
                path=frames_path,
                processor_version=versions["frames"],
            )
            asr_row = self._artifact(
                connection,
                content_id=content_id,
                artifact_type="asr",
                path=asr_path,
                processor_version=versions["asr"],
            )
            ocr_row = self._artifact(
                connection,
                content_id=content_id,
                artifact_type="ocr",
                path=ocr_path,
                processor_version=versions["ocr"],
            )
            connection.execute(
                """
                UPDATE media_processing_slots
                SET status='succeeded',output_artifact_id=?,error_message=NULL,
                    updated_at=? WHERE id=? AND status='running'
                """,
                (int(media_row["id"]), now_utc(), download_slot_id),
            )
            for processor_type, version, artifact, input_sha in (
                ("frames", versions["frames"], frames_row, media_row["sha256"]),
                ("asr", versions["asr"], asr_row, media_row["sha256"]),
                ("ocr", versions["ocr"], ocr_row, frames_row["sha256"]),
            ):
                self._slot(
                    connection,
                    content_id=content_id,
                    processor_type=processor_type,
                    processor_version=version,
                    artifact=artifact,
                    source_sha=str(input_sha),
                )
            connection.commit()
        return {
            "content_id": content_id,
            "status": "evidence_ready",
            "media_kind": "video",
            "artifacts": {
                "media": int(media_row["id"]),
                "frames": int(frames_row["id"]),
                "asr": int(asr_row["id"]),
                "ocr": int(ocr_row["id"]),
            },
        }

    def _fake_evaluation(
        self,
        content_id: int,
        *,
        db_path: Path,
    ):
        self.calls["evaluation"] += 1
        captured_at = now_utc()
        with closing(connect(db_path)) as connection:
            releases = connection.execute(
                "SELECT * FROM evaluation_releases WHERE status='active' ORDER BY id"
            ).fetchall()
            self.assertEqual(len(releases), 1)
            release = releases[0]
            artifacts, components, evidence_sha = evaluation._current_evidence_state(
                connection,
                content_id,
                rule_version=str(release["rule_version"]),
            )
            content = connection.execute(
                "SELECT * FROM content_items WHERE id=?", (content_id,)
            ).fetchone()
            asr = evaluation._read_json(artifacts["asr_path"])
            ocr = evaluation._read_json(artifacts["ocr_path"])
            body_text = "\n".join(
                value
                for value in (
                    str(content["title"] or ""),
                    str(content["body"] or ""),
                )
                if value
            )
            evidence_level, evidence_summary = evaluation._evidence_level(
                content_type=str(content["content_type"]),
                text=body_text,
                media_path=artifacts["media_path"],
                asr=asr,
                ocr=ocr,
            )
            payload = {
                "evaluation_status": "evaluated",
                "evidence_level": evidence_level,
                "evidence_summary": evidence_summary,
                "primary_selling_point_id": "",
                "selling_point_score": None,
                "selling_point_included": False,
                "content_direction": "media",
                "content_automotive_score": None,
                "audience_automotive_score": None,
                "action_intent_score": None,
                "valid_unique_commenters": 0,
                "acquisition_potential": None,
                "matches": [],
                "evaluation_source": "automatic",
                "release_id": str(release["id"]),
            }
            existing = connection.execute(
                """
                SELECT * FROM evaluation_versions
                WHERE content_id=? AND release_id=?
                  AND evidence_sha256=? AND evaluation_source='automatic'
                """,
                (content_id, release["id"], evidence_sha),
            ).fetchone()
            if existing is not None:
                evaluation_id = int(existing["id"])
                envelope_id = int(existing["evidence_envelope_id"])
                created = False
            else:
                cursor = connection.execute(
                    """
                    INSERT INTO evidence_envelopes(
                        content_id,schema_version,detail_raw_sha256,text_sha256,
                        media_sha256,asr_sha256,ocr_sha256,comments_version_sha256,
                        manual_evidence_sha256,evidence_sha256,components_json,created_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        content_id,
                        evaluation.EVIDENCE_VERSION,
                        components["detail_raw_sha256"],
                        components["text_sha256"],
                        components["media_sha256"],
                        components["asr_sha256"],
                        components["ocr_sha256"],
                        components["comments_version_sha256"],
                        components["manual_evidence_sha256"],
                        evidence_sha,
                        json.dumps(
                            components,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        captured_at,
                    ),
                )
                envelope_id = int(cursor.lastrowid)
                cursor = connection.execute(
                    """
                    INSERT INTO evaluation_versions(
                        content_id,evidence_envelope_id,release_id,rule_version,
                        taxonomy_version,matcher_rule_sha256,evidence_sha256,
                        evaluation_source,evaluation_status,evidence_level,
                        content_direction,selling_point_included,
                        payload_json,evaluated_at
                    ) VALUES (?,?,?,?,?,?,?,'automatic',
                              'evaluated',?,'media',0,?,?)
                    """,
                    (
                        content_id,
                        envelope_id,
                        release["id"],
                        release["rule_version"],
                        release["taxonomy_version"],
                        release["matcher_rule_sha256"],
                        evidence_sha,
                        evidence_level,
                        json.dumps(
                            payload,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        captured_at,
                    ),
                )
                evaluation_id = int(cursor.lastrowid)
                connection.execute(
                    "UPDATE content_items SET evaluation_content_direction='media' WHERE id=?",
                    (content_id,),
                )
                connection.commit()
                created = True
        return SimpleNamespace(
            evaluation_id=evaluation_id,
            evidence_envelope_id=envelope_id,
            content_id=content_id,
            evidence_sha256=evidence_sha,
            evidence_level=evidence_level,
            created=created,
        )

    def _fake_fingerprint(self, content_id: int, *, db_path: Path):
        self.calls["fingerprint"] += 1
        captured_at = now_utc()
        root = db_path.parent / "duplicate-fingerprints"
        root.mkdir(exist_ok=True)
        target = root / f"{self.source_fixtures[content_id]['link_id']}.json"
        with closing(connect(db_path)) as connection:
            _, source_sha = duplicates._current_source_state(connection, content_id)
        payload = {
            "schema_version": "duplicate-fingerprint-v1",
            "fingerprint_version": duplicates.FINGERPRINT_VERSION,
            "content_id": content_id,
            "source_sha256": source_sha,
            "text_sha256": None,
            "media_sha256": [],
            "frame_phashes": [],
            "text_simhash": None,
            "asr_simhash": None,
            "ocr_simhash": None,
            "text_char_count": 0,
            "asr_char_count": 0,
            "ocr_char_count": 0,
            "created_at": captured_at,
        }
        target.write_text(
            json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        with closing(connect(db_path)) as connection:
            artifact = self._artifact(
                connection,
                content_id=content_id,
                artifact_type="duplicate_fingerprint",
                path=target,
                processor_version=duplicates.FINGERPRINT_VERSION,
            )
            self._slot(
                connection,
                content_id=content_id,
                processor_type="duplicate_fingerprint",
                processor_version=duplicates.FINGERPRINT_VERSION,
                artifact=artifact,
                source_sha=source_sha,
            )
            connection.execute(
                """
                INSERT INTO duplicate_fingerprints(
                    content_id,fingerprint_version,source_sha256,
                    text_sha256,media_sha256_json,frame_phashes_json,
                    text_simhash,asr_simhash,ocr_simhash,text_char_count,
                    asr_char_count,ocr_char_count,artifact_id,payload_json,created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(content_id,fingerprint_version,source_sha256)
                DO UPDATE SET artifact_id=excluded.artifact_id,payload_json=excluded.payload_json
                """,
                (
                    content_id,
                    duplicates.FINGERPRINT_VERSION,
                    source_sha,
                    payload["text_sha256"],
                    "[]",
                    "[]",
                    payload["text_simhash"],
                    payload["asr_simhash"],
                    payload["ocr_simhash"],
                    payload["text_char_count"],
                    payload["asr_char_count"],
                    payload["ocr_char_count"],
                    int(artifact["id"]),
                    canary._canonical_bytes(payload).decode().removesuffix("\n"),
                    captured_at,
                ),
            )
            connection.commit()
        return payload

    def _arguments(self, **overrides) -> dict[str, object]:
        values: dict[str, object] = {
            "source_db_path": self.source_db,
            "source_completion_path": self.source_completion,
            "expected_source_db_sha256": self.source_db_sha,
            "expected_source_completion_sha256": self.source_completion_sha,
            "db_path": self.db,
            "media_root": self.media_root,
            "run_root": self.run_root,
            "content_ids": [1],
        }
        values.update(overrides)
        return values

    def _run(
        self,
        *,
        media_side_effect=None,
        evaluation_side_effect=None,
        fingerprint_side_effect=None,
        **overrides,
    ):
        with (
            patch.object(canary, "_local_tools", return_value=self.tools),
            patch.object(
                canary.urllib.request,
                "build_opener",
                return_value=SimpleNamespace(
                    open=lambda request, **_kwargs: _Response(
                        str(request.full_url), b"video" * 1000
                    )
                ),
            ),
            patch.object(
                media,
                "process_content_media",
                side_effect=media_side_effect or self._fake_media,
            ),
            patch.object(
                evaluation,
                "evaluate_content",
                side_effect=evaluation_side_effect or self._fake_evaluation,
            ),
            patch.object(
                duplicates,
                "fingerprint_content",
                side_effect=fingerprint_side_effect or self._fake_fingerprint,
            ),
        ):
            return canary.run_canary(**self._arguments(**overrides))

    def _plan(self, **overrides):
        with patch.object(canary, "_local_tools", return_value=self.tools):
            return canary.plan_canary(**self._arguments(**overrides))

    @staticmethod
    def _tree_state(root: Path) -> list[tuple[str, int, str]]:
        rows = []
        for path in sorted(root.rglob("*")):
            if path.is_file():
                rows.append(
                    (
                        str(path.relative_to(root)),
                        path.stat().st_size,
                        canary._sha256_file(path),
                    )
                )
        return rows

    @staticmethod
    def _protected_digests(path: Path) -> dict[str, object]:
        with closing(canary._immutable_connection(path)) as connection:
            return {
                table: canary._digest_query(
                    connection, f"SELECT * FROM {table} ORDER BY rowid"
                )
                for table in (
                    "provider_usage",
                    "provider_budget_batches",
                    "content_metric_snapshots",
                )
            }

    def test_default_plan_is_read_only_and_pins_step3_lineage(self) -> None:
        before = self._tree_state(self.root)

        result = self._plan()

        self.assertEqual(result["status"], "planned")
        self.assertFalse(result["apply"])
        self.assertEqual(result["source_database"]["sha256"], self.source_db_sha)
        self.assertEqual(result["source_summary"][0]["url_count"], 1)
        self.assertEqual(self._tree_state(self.root), before)
        self.assertFalse(self.db.exists())
        self.assertFalse(self.media_root.exists())
        self.assertFalse(self.run_root.exists())

    def test_active_v9_canary_uses_v9_evidence_without_manual_hash(self) -> None:
        with closing(connect(self.source_db)) as connection:
            connection.execute(
                "UPDATE evaluation_releases SET rule_version='evaluation-v9' "
                "WHERE id='canary-release'"
            )
            connection.commit()
        self._finalize(self.source_db)
        self._refresh_step3_proof()

        result = self._run()

        self.assertEqual(result["status"], "succeeded")
        with closing(canary._immutable_connection(self.db)) as connection:
            row = connection.execute(
                """
                SELECT ev.rule_version,ee.manual_evidence_sha256
                FROM evaluation_versions ev
                JOIN evidence_envelopes ee ON ee.id=ev.evidence_envelope_id
                WHERE ev.content_id=1 AND ev.invalidated_at IS NULL
                """
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["rule_version"], evaluation.V9_RULE_VERSION)
            self.assertIsNone(row["manual_evidence_sha256"])

    def test_step3_batch_receipt_requires_real_v1_semantic_chain(self) -> None:
        intent_path = self.step3_batches / "batch-000001.intent.json"
        receipt_path = self.step3_batches / "batch-000001.receipt.json"
        intent = json.loads(intent_path.read_text(encoding="utf-8"))
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        self.assertEqual(
            set(intent),
            {
                "version",
                "batch_index",
                "content_ids",
                "content_ids_sha256",
                "contract_sha256",
                "before_database",
                "previous_receipt_sha256",
            },
        )
        self.assertIn("intent_sha256", receipt)
        self.assertNotIn("contract_sha256", receipt)
        self.assertNotIn("batch_intent_sha256", receipt)
        step3_contract = json.loads(
            (self.step3_run / "run-contract.json").read_text(encoding="utf-8")
        )
        step3_controller._validate_receipt_apply(
            receipt, contract=step3_contract
        )
        step3_controller._validate_receipt_disk(
            receipt, contract=step3_contract
        )

        receipt.pop("intent_sha256")
        receipt["contract_sha256"] = intent["contract_sha256"]
        receipt["batch_intent_sha256"] = canary._sha256_file(intent_path)
        receipt_path.write_bytes(canary._canonical_bytes(receipt))
        completion = json.loads(
            self.source_completion.read_text(encoding="utf-8")
        )
        completion["batch_chain"]["rows"][0][2] = canary._sha256_file(
            receipt_path
        )
        completion["batch_chain"]["rows_sha256"] = canary._compact_json_sha256(
            completion["batch_chain"]["rows"]
        )
        completion["receipt_evidence"]["receipt_files_sha256"] = completion[
            "batch_chain"
        ]["rows_sha256"]
        self.source_completion.write_bytes(canary._canonical_bytes(completion))
        self.source_completion_sha = canary._sha256_file(self.source_completion)
        before = self._tree_state(self.root)

        with self.assertRaisesRegex(
            canary.LocalAnalysisCanaryError,
            "Step3 batch intent/receipt语义链漂移",
        ):
            self._plan()

        self.assertEqual(self._tree_state(self.root), before)
        self.assertFalse(self.db.exists())
        self.assertFalse(self.run_root.exists())
        self.assertFalse(self.media_root.exists())

    def test_first_apply_and_idempotent_rerun_are_exact(self) -> None:
        source_before = self._tree_state(self.root)
        protected = self._protected_digests(self.source_db)

        first = self._run()

        self.assertEqual(first["status"], "succeeded")
        self.assertFalse(first["idempotent"])
        self.assertEqual(canary._sha256_file(self.source_db), self.source_db_sha)
        self.assertEqual(self._protected_digests(self.db), protected)
        with closing(canary._immutable_connection(self.db)) as connection:
            row = connection.execute(
                "SELECT source_group,evaluation_content_direction FROM content_items WHERE id=1"
            ).fetchone()
        self.assertEqual(tuple(row), ("history-backfill", "media"))
        before_repeat = self._tree_state(self.root)
        calls = dict(self.calls)

        repeated = self._run()

        self.assertTrue(repeated["idempotent"])
        self.assertEqual(self.calls, calls)
        self.assertEqual(self._tree_state(self.root), before_repeat)
        self.assertEqual(canary._sha256_file(self.source_db), self.source_db_sha)
        self.assertNotEqual(source_before, before_repeat)

    def test_two_content_apply_succeeds_and_is_idempotent(self) -> None:
        self._add_source_content(2)
        protected = self._protected_digests(self.source_db)

        first = self._run(content_ids=[1, 2])

        self.assertEqual(first["status"], "succeeded")
        self.assertFalse(first["idempotent"])
        self.assertEqual(self.calls, {"media": 2, "evaluation": 2, "fingerprint": 2})
        self.assertEqual(self._protected_digests(self.db), protected)
        progress = json.loads((self.run_root / "progress.json").read_text())
        receipt = json.loads((self.run_root / "receipt.json").read_text())
        self.assertEqual(progress["completed_ids"], [1, 2])
        self.assertEqual(receipt["content_ids"], [1, 2])
        frozen_tree = self._tree_state(self.root)
        frozen_calls = dict(self.calls)

        repeated = self._run(content_ids=[1, 2])

        self.assertTrue(repeated["idempotent"])
        self.assertEqual(self.calls, frozen_calls)
        self.assertEqual(self._tree_state(self.root), frozen_tree)

    def test_programmatic_content_ids_require_exact_ints_and_preserve_order(
        self,
    ) -> None:
        before = self._tree_state(self.root)
        invalid_cases = {
            "bool": [True],
            "integral_float": [1.0],
            "fractional_float": [1.9],
            "string": ["2"],
        }
        for label, content_ids in invalid_cases.items():
            with self.subTest(label=label, entrypoint="plan"):
                with self.assertRaisesRegex(
                    canary.LocalAnalysisCanaryError,
                    "正整数 --content-id",
                ):
                    self._plan(content_ids=content_ids)
                self.assertEqual(self._tree_state(self.root), before)
                self.assertFalse(self.db.exists())
                self.assertFalse(self.media_root.exists())
                self.assertFalse(self.run_root.exists())
            with self.subTest(label=label, entrypoint="apply"):
                with self.assertRaisesRegex(
                    canary.LocalAnalysisCanaryError,
                    "正整数 --content-id",
                ):
                    self._run(content_ids=content_ids)
                self.assertEqual(self._tree_state(self.root), before)
                self.assertFalse(self.db.exists())
                self.assertFalse(self.media_root.exists())
                self.assertFalse(self.run_root.exists())
        with self.assertRaisesRegex(
            canary.LocalAnalysisCanaryError, "不得重复"
        ):
            self._plan(content_ids=[1, 1])
        with self.assertRaisesRegex(
            canary.LocalAnalysisCanaryError, "正整数 --content-id"
        ):
            self._run(content_ids=[])
        self.assertEqual(self._tree_state(self.root), before)

        self._add_source_content(2)
        before_ordered_plan = self._tree_state(self.root)
        planned = self._plan(content_ids=[2, 1])
        self.assertEqual(planned["content_ids"], [2, 1])
        self.assertEqual(
            [row["content_id"] for row in planned["source_summary"]], [2, 1]
        )
        self.assertEqual(self._tree_state(self.root), before_ordered_plan)
        self.assertFalse(self.db.exists())
        self.assertFalse(self.media_root.exists())
        self.assertFalse(self.run_root.exists())

    def test_three_content_real_sigkill_on_second_resumes_prefix_and_is_idempotent(
        self,
    ) -> None:
        self._add_source_content(2)
        self._add_source_content(3)
        child_calls = self.root / "child-media-calls.jsonl"
        child_error = self.root / "child-error.txt"

        def child_media(content_id: int, **kwargs):
            with child_calls.open("a", encoding="utf-8") as stream:
                stream.write(f"{content_id}\n")
                stream.flush()
                os.fsync(stream.fileno())
            if content_id == 2:
                while True:
                    signal.pause()
            return self._fake_media(content_id, **kwargs)

        pid = os.fork()
        if pid == 0:
            try:
                self._run(content_ids=[1, 2, 3], media_side_effect=child_media)
            except BaseException as exc:
                child_error.write_text(repr(exc), encoding="utf-8")
                os._exit(71)
            os._exit(72)

        reaped = False
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                calls = (
                    child_calls.read_text(encoding="utf-8").splitlines()
                    if child_calls.exists()
                    else []
                )
                if calls == ["1", "2"]:
                    break
                finished, status = os.waitpid(pid, os.WNOHANG)
                if finished:
                    reaped = True
                    self.fail(
                        "child exited before SIGKILL: "
                        f"status={status}, error="
                        f"{child_error.read_text() if child_error.exists() else ''}"
                    )
                time.sleep(0.02)
            else:
                self.fail("child did not reach second content before timeout")
            os.kill(pid, signal.SIGKILL)
            _, status = os.waitpid(pid, 0)
            reaped = True
            self.assertTrue(os.WIFSIGNALED(status))
            self.assertEqual(os.WTERMSIG(status), signal.SIGKILL)
        finally:
            if not reaped:
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                os.waitpid(pid, 0)

        progress_after_kill = json.loads(
            (self.run_root / "progress.json").read_text()
        )
        self.assertEqual(progress_after_kill["completed_ids"], [1])
        resumed_ids: list[int] = []

        def resumed_media(content_id: int, **kwargs):
            resumed_ids.append(content_id)
            return self._fake_media(content_id, **kwargs)

        recovered = self._run(
            content_ids=[1, 2, 3], media_side_effect=resumed_media
        )

        self.assertEqual(recovered["status"], "succeeded")
        self.assertFalse(recovered["idempotent"])
        self.assertEqual(resumed_ids, [2, 3])
        progress = json.loads((self.run_root / "progress.json").read_text())
        receipt_path = self.run_root / "receipt.json"
        receipt = json.loads(receipt_path.read_text())
        self.assertEqual(progress["completed_ids"], [1, 2, 3])
        self.assertEqual(receipt["content_ids"], [1, 2, 3])
        frozen_receipt = receipt_path.read_bytes()
        frozen_tree = self._tree_state(self.root)
        resumed_call_count = len(resumed_ids)

        repeated = self._run(
            content_ids=[1, 2, 3], media_side_effect=resumed_media
        )

        self.assertTrue(repeated["idempotent"])
        self.assertEqual(len(resumed_ids), resumed_call_count)
        self.assertEqual(receipt_path.read_bytes(), frozen_receipt)
        self.assertEqual(self._tree_state(self.root), frozen_tree)

    def test_step3_target_subset_projection_is_ordered_and_fail_closed(self) -> None:
        self._add_source_content(2)
        step3_contract = json.loads(
            (self.step3_run / "run-contract.json").read_text()
        )
        rows = step3_contract["target_contract"]["rows"]
        evidence = {
            "completion_kind": None,
            "contract": {
                "media_root": str(self.source_root),
                "derived_raw_root": str(self.raw_root),
                "explicit_target_rows": rows,
            },
        }
        with closing(canary._immutable_connection(self.source_db)) as connection:
            ordered = canary._completion_source_snapshots(
                connection,
                [2, 1],
                evidence,
                allow_step3_target_subset=True,
            )
            self.assertEqual(
                [int(source["content"]["id"]) for source in ordered], [2, 1]
            )
            with self.assertRaisesRegex(
                canary.LocalAnalysisCanaryError,
                "explicit target rows未覆盖",
            ):
                canary._completion_source_snapshots(connection, [1], evidence)
            missing = {
                **evidence,
                "contract": {
                    **evidence["contract"],
                    "explicit_target_rows": rows[:1],
                },
            }
            with self.assertRaisesRegex(
                canary.LocalAnalysisCanaryError,
                "explicit target rows未覆盖",
            ):
                canary._completion_source_snapshot(connection, 2, missing)
            duplicate = {
                **evidence,
                "contract": {
                    **evidence["contract"],
                    "explicit_target_rows": [rows[0], rows[0]],
                },
            }
            with self.assertRaisesRegex(
                canary.LocalAnalysisCanaryError,
                "重复content_id",
            ):
                canary._completion_source_snapshot(connection, 1, duplicate)

    def test_retryable_failure_and_copy_before_contract_crash_recover(self) -> None:
        original_build = canary._build_contract
        with (
            patch.object(canary, "_local_tools", return_value=self.tools),
            patch.object(canary, "_build_contract", side_effect=RuntimeError("copy kill")),
        ):
            with self.assertRaisesRegex(RuntimeError, "copy kill"):
                canary.run_canary(**self._arguments())
        self.assertTrue(self.db.exists())
        self.assertTrue((self.run_root / "copy-intent.json").exists())
        self.assertTrue((self.run_root / "copy-receipt.json").exists())
        self.assertFalse((self.run_root / "run-contract.json").exists())
        with patch.object(canary, "_build_contract", original_build):
            result = self._run()
        self.assertEqual(result["status"], "succeeded")

    def test_owned_partial_database_copy_is_rebuilt_but_unknown_partial_blocks(self) -> None:
        source_prefix = self.source_db.read_bytes()[:4096]

        def leave_owned_partial(paths, *, source_evidence):
            self.assertEqual(source_evidence["database"]["sha256"], self.source_db_sha)
            paths.copy_partial.write_bytes(source_prefix)
            raise RuntimeError("copy hard kill")

        with (
            patch.object(canary, "_local_tools", return_value=self.tools),
            patch.object(
                canary, "_copy_source_database", side_effect=leave_owned_partial
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "copy hard kill"):
                canary.run_canary(**self._arguments())
        copy_partial = self.db.with_name(f".{self.db.name}.copy-partial")
        self.assertFalse(self.db.exists())
        self.assertEqual(copy_partial.read_bytes(), source_prefix)
        self.assertTrue((self.run_root / "copy-intent.json").exists())
        self.assertFalse((self.run_root / "copy-receipt.json").exists())

        recovered = self._run()
        self.assertEqual(recovered["status"], "succeeded")
        self.assertFalse(copy_partial.exists())

    def test_unknown_partial_database_copy_is_preserved_and_blocks(self) -> None:
        unknown = b"not-a-step3-prefix"

        def leave_unknown_partial(paths, *, source_evidence):
            del source_evidence
            paths.copy_partial.write_bytes(unknown)
            raise RuntimeError("copy hard kill")

        with (
            patch.object(canary, "_local_tools", return_value=self.tools),
            patch.object(
                canary, "_copy_source_database", side_effect=leave_unknown_partial
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "copy hard kill"):
                canary.run_canary(**self._arguments())
        with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "精确前缀"):
            self._run()
        copy_partial = self.db.with_name(f".{self.db.name}.copy-partial")
        self.assertFalse(self.db.exists())
        self.assertEqual(copy_partial.read_bytes(), unknown)
        self.assertFalse((self.run_root / "run-contract.json").exists())

    def test_lexical_symlink_component_is_rejected_before_resolve(self) -> None:
        actual = self.root / "actual-analysis"
        actual.mkdir()
        alias = self.root / "analysis-alias"
        alias.symlink_to(actual, target_is_directory=True)
        before = self._tree_state(actual)
        with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "符号链接"):
            self._plan(
                db_path=alias / "work.sqlite3",
                media_root=alias / "media",
                run_root=alias / "run",
            )
        self.assertEqual(before, self._tree_state(actual))

        fifo = self.root / "analysis-fifo"
        os.mkfifo(fifo)
        with self.assertRaises(canary.LocalAnalysisCanaryError):
            self._plan(media_root=fifo)
        self.assertTrue(stat.S_ISFIFO(fifo.lstat().st_mode))

    def test_stage_failure_keeps_intent_and_same_contract_recovers(self) -> None:
        attempts = 0

        def fail_once(content_id: int, **kwargs):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("injected media failure")
            return self._fake_media(content_id, **kwargs)

        with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "同一contract恢复"):
            self._run(media_side_effect=fail_once)
        intent_sha = canary._sha256_file(self.run_root / "intent.json")

        result = self._run(media_side_effect=fail_once)

        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(canary._sha256_file(self.run_root / "intent.json"), intent_sha)
        receipt = json.loads((self.run_root / "receipt.json").read_text())
        self.assertEqual(receipt["startup_recovery"]["running_candidates"], 0)

    def test_atomic_record_windows_recover_and_receipt_heals_retryable_state(self) -> None:
        self._freeze_failed_contract()
        for name in ("copy-intent.json", "copy-receipt.json", "run-contract.json"):
            final = self.run_root / name
            final.with_name(f".{name}.tmp").write_bytes(final.read_bytes())
        intent = self.run_root / "intent.json"
        intent_temp = intent.with_name(f".{intent.name}.tmp")
        intent.replace(intent_temp)
        state = self.run_root / "state.json"
        predecessor_bytes = state.read_bytes()
        predecessor_sha = canary._sha256_bytes(predecessor_bytes)
        receipt = self.run_root / "receipt.json"
        receipt_temp = receipt.with_name(f".{receipt.name}.tmp")
        original_replace = canary.os.replace

        def crash_before_receipt_replace(source, target):
            if Path(source) == receipt_temp and Path(target) == receipt:
                raise RuntimeError("injected receipt rename crash")
            return original_replace(source, target)

        with (
            patch.object(canary.os, "replace", side_effect=crash_before_receipt_replace),
            self.assertRaisesRegex(RuntimeError, "receipt rename"),
        ):
            self._run()

        self.assertFalse(receipt.exists())
        self.assertTrue(receipt_temp.exists())
        self.assertEqual(state.read_bytes(), predecessor_bytes)
        self.assertFalse(intent_temp.exists())

        recovered_receipt = self._run()
        self.assertTrue(recovered_receipt["idempotent"])
        self.assertTrue(receipt.exists())
        self.assertFalse(receipt_temp.exists())
        healed_state = json.loads(state.read_text())
        self.assertEqual(healed_state["previous_state_sha256"], predecessor_sha)

        paths = canary._paths(
            source_db_path=self.source_db,
            source_completion_path=self.source_completion,
            db_path=self.db,
            media_root=self.media_root,
            run_root=self.run_root,
        )
        state_temp = state.with_name(f".{state.name}.tmp")
        state.write_bytes(predecessor_bytes)
        successor = canary._state_value(
            paths,
            status="succeeded",
            contract_sha256=canary._sha256_file(self.run_root / "run-contract.json"),
            intent_sha256=canary._sha256_file(intent),
        )
        self.assertEqual(successor["previous_state_sha256"], predecessor_sha)
        state_temp.write_bytes(canary._canonical_bytes(successor))
        recovered_state = self._run()
        self.assertTrue(recovered_state["idempotent"])
        self.assertFalse(state_temp.exists())

        state.write_bytes(predecessor_bytes)
        healed = self._run()
        self.assertTrue(healed["idempotent"])
        healed_state = json.loads(state.read_text())
        self.assertEqual(healed_state["status"], "succeeded")
        self.assertEqual(healed_state["previous_state_sha256"], predecessor_sha)
        self.assertEqual(
            healed_state["receipt_sha256"], canary._sha256_file(receipt)
        )

        partial = b'{"schema_version":'
        state_temp.write_bytes(partial)
        with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "合法JSON"):
            self._run()
        self.assertEqual(state_temp.read_bytes(), partial)

    def test_copy_intent_full_temp_is_promoted_after_rename_crash(self) -> None:
        final = self.run_root / "copy-intent.json"
        temporary = final.with_name(f".{final.name}.tmp")
        original_replace = canary.os.replace

        def crash(source, target):
            if Path(source) == temporary and Path(target) == final:
                raise RuntimeError("copy intent rename crash")
            return original_replace(source, target)

        with (
            patch.object(canary.os, "replace", side_effect=crash),
            self.assertRaisesRegex(RuntimeError, "copy intent rename"),
        ):
            self._run()
        self.assertTrue(temporary.exists())
        self.assertFalse(final.exists())
        self.assertFalse(self.db.exists())
        self.assertEqual(self._run()["status"], "succeeded")
        self.assertFalse(temporary.exists())

    def test_copy_receipt_prefix_temp_is_rebuilt_after_rename_crash(self) -> None:
        final = self.run_root / "copy-receipt.json"
        temporary = final.with_name(f".{final.name}.tmp")
        original_replace = canary.os.replace

        def crash(source, target):
            if Path(source) == temporary and Path(target) == final:
                raise RuntimeError("copy receipt rename crash")
            return original_replace(source, target)

        with (
            patch.object(canary.os, "replace", side_effect=crash),
            self.assertRaisesRegex(RuntimeError, "copy receipt rename"),
        ):
            self._run()
        full = temporary.read_bytes()
        temporary.write_bytes(full[: max(1, len(full) // 2)])
        self.assertTrue(self.db.exists())
        self.assertFalse(final.exists())
        self.assertEqual(self._run()["status"], "succeeded")
        self.assertFalse(temporary.exists())

    def test_contract_prefix_temp_is_rebuilt_after_rename_crash(self) -> None:
        final = self.run_root / "run-contract.json"
        temporary = final.with_name(f".{final.name}.tmp")
        original_replace = canary.os.replace

        def crash(source, target):
            if Path(source) == temporary and Path(target) == final:
                raise RuntimeError("contract rename crash")
            return original_replace(source, target)

        with (
            patch.object(canary.os, "replace", side_effect=crash),
            self.assertRaisesRegex(RuntimeError, "contract rename"),
        ):
            self._run()
        full = temporary.read_bytes()
        temporary.write_bytes(full[: max(1, len(full) // 2)])
        self.assertTrue(self.db.exists())
        self.assertFalse(final.exists())
        self.assertEqual(self._run()["status"], "succeeded")
        self.assertFalse(temporary.exists())

    def test_unknown_atomic_temps_are_preserved_and_block_before_processing(self) -> None:
        self._freeze_failed_contract()
        database_before = canary._sha256_file(self.db)
        calls_before = dict(self.calls)

        contract = self.run_root / "run-contract.json"
        contract_temp = contract.with_name(f".{contract.name}.tmp")
        contract_temp.write_text('{"unknown":true}\n')
        with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "既有终态漂移"):
            self._run()
        self.assertEqual(contract_temp.read_bytes(), b'{"unknown":true}\n')
        self.assertEqual(canary._sha256_file(self.db), database_before)
        self.assertEqual(self.calls, calls_before)
        contract_temp.unlink()

        intent = self.run_root / "intent.json"
        intent_temp = intent.with_name(f".{intent.name}.tmp")
        drifted_intent = json.loads(intent.read_text())
        drifted_intent["contract_sha256"] = "0" * 64
        intent_temp.write_bytes(canary._canonical_bytes(drifted_intent))
        with self.assertRaises(canary.LocalAnalysisCanaryError):
            self._run()
        self.assertEqual(
            intent_temp.read_bytes(), canary._canonical_bytes(drifted_intent)
        )
        self.assertEqual(canary._sha256_file(self.db), database_before)
        self.assertEqual(self.calls, calls_before)
        intent_temp.unlink()

        state = self.run_root / "state.json"
        state_temp = state.with_name(f".{state.name}.tmp")
        drifted_state = json.loads(state.read_text())
        drifted_state["previous_state_sha256"] = "0" * 64
        state_temp.write_bytes(canary._canonical_bytes(drifted_state))
        with self.assertRaises(canary.LocalAnalysisCanaryError):
            self._run()
        self.assertEqual(
            state_temp.read_bytes(), canary._canonical_bytes(drifted_state)
        )
        self.assertEqual(canary._sha256_file(self.db), database_before)
        self.assertEqual(self.calls, calls_before)
        state_temp.unlink()

        self.assertEqual(self._run()["status"], "succeeded")
        receipt = self.run_root / "receipt.json"
        receipt_temp = receipt.with_name(f".{receipt.name}.tmp")
        drifted_receipt = json.loads(receipt.read_text())
        drifted_receipt["pre_receipt_state_sha256"] = "0" * 64
        receipt_temp.write_bytes(canary._canonical_bytes(drifted_receipt))
        success_db = canary._sha256_file(self.db)
        success_calls = dict(self.calls)
        with self.assertRaises(canary.LocalAnalysisCanaryError):
            self._run()
        self.assertEqual(
            receipt_temp.read_bytes(), canary._canonical_bytes(drifted_receipt)
        )
        self.assertEqual(canary._sha256_file(self.db), success_db)
        self.assertEqual(self.calls, success_calls)

    def test_source_completion_raw_and_path_traversal_drift_block_without_work(self) -> None:
        with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "expected SHA256"):
            self._plan(expected_source_db_sha256="0" * 64)
        self.assertFalse(self.db.exists())
        with closing(connect(self.source_db)) as connection:
            connection.execute("UPDATE content_items SET link_id='../bad' WHERE id=1")
            connection.commit()
        self._refresh_step3_proof()
        with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "安全单段"):
            self._plan(
                expected_source_db_sha256=self.source_db_sha,
                expected_source_completion_sha256=self.source_completion_sha,
            )
        self.assertFalse(self.db.exists())
        self.assertFalse(self.run_root.exists())

    def test_douyin_discovery_images_freeze_groups_and_cache_shared_raw(self) -> None:
        groups = [
            [
                f"https://p3-sign.douyinpic.com/first-{index}"
                for index in range(4)
            ],
            [
                f"https://p3-sign.douyinpic.com/second-{index}"
                for index in range(5)
            ],
        ]
        self._configure_douyin_image_source(groups)
        cache = canary._DiscoveryRawCache()
        with closing(canary._immutable_connection(self.source_db)) as connection:
            first = canary._source_snapshot(
                connection,
                1,
                step3_media_root=self.source_root,
                step3_derived_raw_root=self.raw_root,
                target_contract_row=self._step3_target_row(),
                discovery_raw_cache=cache,
            )
            second = canary._source_snapshot(
                connection,
                1,
                step3_media_root=self.source_root,
                step3_derived_raw_root=self.raw_root,
                target_contract_row=self._step3_target_row(),
                discovery_raw_cache=cache,
            )

        expected = media.douyin_image_source_groups(
            [url for group in groups for url in group], groups
        )
        self.assertEqual(first["image_groups"], expected)
        self.assertEqual(second["image_groups"], expected)
        self.assertEqual(
            first["image_groups_sha256"], media.image_groups_sha256(expected)
        )
        self.assertEqual(cache.file_load_count, 1)
        self.assertEqual(cache.cache_hit_count, 1)

    def test_douyin_discovery_aweme_match_must_be_unique_and_direct(self) -> None:
        group = [
            f"https://p3-sign.douyinpic.com/unique-{index}"
            for index in range(4)
        ]
        image = {
            "download_url_list": group[:2],
            "url_list": group[2:],
        }
        cases = {
            "missing": [{"aweme_id": "other", "images": [image]}],
            "numeric": [{"aweme_id": 123, "images": [image]}],
            "duplicate": [
                {"aweme_id": "canary-1", "images": [image]},
                {"aweme_id": "canary-1", "images": [image]},
            ],
            "nested-only": [
                {
                    "aweme_id": "other",
                    "nested": {
                        "aweme_id": "canary-1",
                        "images": [image],
                    },
                }
            ],
        }
        for label, items in cases.items():
            with self.subTest(label=label):
                self._configure_douyin_image_source(
                    [group], aweme_items=items
                )
                with closing(connect(self.source_db)) as connection:
                    connection.execute(
                        "UPDATE content_items SET platform_content_id=? WHERE id=1",
                        ("123" if label == "numeric" else "canary-1",),
                    )
                    connection.commit()
                before = self._tree_state(self.root)
                with closing(
                    canary._immutable_connection(self.source_db)
                ) as connection:
                    with self.assertRaisesRegex(
                        canary.LocalAnalysisCanaryError,
                        "唯一aweme_id",
                    ):
                        canary._source_snapshot(
                            connection,
                            1,
                            step3_media_root=self.source_root,
                            step3_derived_raw_root=self.raw_root,
                            target_contract_row=self._step3_target_row(),
                        )
                self.assertEqual(self._tree_state(self.root), before)

    def test_douyin_discovery_groups_must_exactly_flatten_media_source(self) -> None:
        group = [
            f"https://p3-sign.douyinpic.com/flatten-{index}"
            for index in range(4)
        ]
        self._configure_douyin_image_source(
            [group], media_urls=[group[1], group[0], *group[2:]]
        )
        with closing(canary._immutable_connection(self.source_db)) as connection:
            with self.assertRaisesRegex(
                canary.LocalAnalysisCanaryError,
                "exactly flatten|精确扁平",
            ):
                canary._source_snapshot(
                    connection,
                    1,
                    step3_media_root=self.source_root,
                    step3_derived_raw_root=self.raw_root,
                    target_contract_row=self._step3_target_row(),
                )

    def test_douyin_discovery_raw_path_escape_and_symlink_fail_closed(self) -> None:
        group = [
            f"https://p3-sign.douyinpic.com/path-{index}"
            for index in range(4)
        ]
        self._configure_douyin_image_source([group])
        original = self.raw_root / "discovery.json"
        outside = self.step3_root / "outside-discovery.json"
        outside.write_bytes(original.read_bytes())
        link = self.raw_root / "discovery-link.json"
        link.symlink_to(original)
        hardlink = self.raw_root / "discovery-hardlink.json"
        os.link(original, hardlink)
        for label, path, message in (
            ("escape", outside, "RAW_ROOT"),
            ("symlink", link, "符号链接|私有单链接"),
            ("hardlink", hardlink, "私有单链接"),
        ):
            with self.subTest(label=label):
                with closing(connect(self.source_db)) as connection:
                    connection.execute(
                        "UPDATE provider_raw_responses SET local_path=? WHERE id=999",
                        (str(path),),
                    )
                    connection.commit()
                self._refresh_step3_proof()
                with closing(
                    canary._immutable_connection(self.source_db)
                ) as connection:
                    with self.assertRaisesRegex(
                        canary.LocalAnalysisCanaryError, message
                    ):
                        canary._source_snapshot(
                            connection,
                            1,
                            step3_media_root=self.source_root,
                            step3_derived_raw_root=self.raw_root,
                            target_contract_row=self._step3_target_row(),
                        )

    def test_douyin_discovery_database_evidence_drift_fails_closed(self) -> None:
        group = [
            f"https://p3-sign.douyinpic.com/db-evidence-{index}"
            for index in range(4)
        ]
        self._configure_douyin_image_source([group])
        target_row = self._step3_target_row()
        with closing(connect(self.source_db)) as connection:
            captured_at = now_utc()
            connection.execute(
                "INSERT INTO accounts(id,phone,phone_normalized,created_at,updated_at) "
                "VALUES (77,'fixture-77','fixture-77',?,?)",
                (captured_at, captured_at),
            )
            baseline = dict(
                connection.execute(
                    "SELECT * FROM provider_raw_responses WHERE id=999"
                ).fetchone()
            )
            connection.commit()
        mutations = {
            "sha256": "0" * 64,
            "byte_size": int(baseline["byte_size"]) + 1,
            "http_status": 201,
            "source": "live",
            "operation": "douyin_wrong_operation",
            "provider": "other-provider",
            "account_id": 77,
            "content_id": 1,
        }
        for column, value in mutations.items():
            with self.subTest(column=column):
                with closing(connect(self.source_db)) as connection:
                    connection.execute(
                        f"UPDATE provider_raw_responses SET {column}=? WHERE id=999",
                        (value,),
                    )
                    connection.commit()
                self._finalize(self.source_db)
                with closing(
                    canary._immutable_connection(self.source_db)
                ) as connection:
                    with self.assertRaisesRegex(
                        canary.LocalAnalysisCanaryError,
                        "数据库证据漂移|DB SHA/bytes",
                    ):
                        canary._source_snapshot(
                            connection,
                            1,
                            step3_media_root=self.source_root,
                            step3_derived_raw_root=self.raw_root,
                            target_contract_row=target_row,
                        )
                with closing(connect(self.source_db)) as connection:
                    connection.execute(
                        f"UPDATE provider_raw_responses SET {column}=? WHERE id=999",
                        (baseline[column],),
                    )
                    connection.commit()

    def test_douyin_discovery_cache_rejects_file_and_db_identity_drift(self) -> None:
        group = [
            f"https://p3-sign.douyinpic.com/cache-drift-{index}"
            for index in range(4)
        ]
        target_path = self.raw_root / "discovery.json"
        for mutation in ("size", "inode", "database"):
            with self.subTest(mutation=mutation):
                self._configure_douyin_image_source([group])
                target_row = self._step3_target_row()
                cache = canary._DiscoveryRawCache()
                with closing(
                    canary._immutable_connection(self.source_db)
                ) as connection:
                    canary._source_snapshot(
                        connection,
                        1,
                        step3_media_root=self.source_root,
                        step3_derived_raw_root=self.raw_root,
                        target_contract_row=target_row,
                        discovery_raw_cache=cache,
                    )
                original = target_path.read_bytes()
                if mutation == "size":
                    target_path.write_bytes(original + b" ")
                elif mutation == "inode":
                    replacement = target_path.with_name("replacement.json")
                    replacement.write_bytes(original)
                    os.replace(replacement, target_path)
                else:
                    with closing(connect(self.source_db)) as connection:
                        connection.execute(
                            "UPDATE provider_raw_responses SET byte_size=byte_size+1 "
                            "WHERE id=999"
                        )
                        connection.commit()
                    self._finalize(self.source_db)
                with closing(
                    canary._immutable_connection(self.source_db)
                ) as connection:
                    with self.assertRaisesRegex(
                        canary.LocalAnalysisCanaryError,
                        "cache命中时DB或文件身份漂移|DB SHA/bytes",
                    ):
                        canary._source_snapshot(
                            connection,
                            1,
                            step3_media_root=self.source_root,
                            step3_derived_raw_root=self.raw_root,
                            target_contract_row=target_row,
                            discovery_raw_cache=cache,
                        )
                self.assertEqual(cache.file_load_count, 1)
                self.assertEqual(cache.cache_hit_count, 0)

    def test_http_source_is_blocked_in_plan_without_side_effect(self) -> None:
        http_url = "http://v1.douyinvod.com/canary.mp4"
        raw_path = self.raw_root / "source.json"
        raw_body = json.loads(raw_path.read_text())
        raw_body["data"]["media_urls"] = [http_url]
        raw_path.write_text(json.dumps(raw_body, sort_keys=True) + "\n")
        with closing(connect(self.source_db)) as connection:
            connection.execute("DELETE FROM evidence_artifacts WHERE artifact_type='media_source'")
            connection.execute(
                "UPDATE provider_raw_responses SET sha256=?,byte_size=? WHERE id=1",
                (canary._sha256_file(raw_path), raw_path.stat().st_size),
            )
            connection.commit()
        for path in self.source_root.rglob("*"):
            if path.is_file():
                path.unlink()
        manifest = self.source_root / "C4N4RY" / "sources" / "http.json"
        manifest.parent.mkdir(parents=True, exist_ok=True)
        logical_sha = "e" * 64
        manifest.write_text(
            json.dumps(
                {
                    "schema_version": media.MEDIA_SOURCE_VERSION,
                    "media_kind": "video",
                    "urls": [http_url],
                    "source_sha256": logical_sha,
                    "raw_response_id": 1,
                    "captured_at": now_utc(),
                },
                sort_keys=True,
            )
            + "\n"
        )
        with closing(connect(self.source_db)) as connection:
            connection.execute(
                """
                INSERT INTO evidence_artifacts(
                    content_id,artifact_type,local_path,status,byte_size,sha256,
                    captured_at,processor_version,metadata_json,created_at
                ) VALUES (1,'media_source',?,'available',?,?,?,?,?,?)
                """,
                (
                    str(manifest),
                    manifest.stat().st_size,
                    canary._sha256_file(manifest),
                    now_utc(),
                    media.MEDIA_SOURCE_VERSION,
                    json.dumps(
                        {
                            "media_kind": "video",
                            "source_count": 1,
                            "source_sha256": logical_sha,
                            "raw_response_id": 1,
                        },
                        sort_keys=True,
                    ),
                    now_utc(),
                ),
            )
            connection.commit()
        self._refresh_step3_proof()
        before = self._tree_state(self.root)
        with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "HTTPS:443"):
            self._plan(
                expected_source_db_sha256=self.source_db_sha,
                expected_source_completion_sha256=self.source_completion_sha,
            )
        self.assertEqual(self._tree_state(self.root), before)

    def test_douyin_zjcdn_direct_video_gate_is_exact(self) -> None:
        object_path = (
            f"/{'a' * 32}/{'b' * 8}/video/tos/cn/tos-cn-ve-15/"
            f"{'C' * 38}/"
        )
        for host in sorted(canary.DOUYIN_DIRECT_VIDEO_CDN_HOSTS):
            with self.subTest(host=host):
                row = canary._safe_url(
                    f"https://{host}{object_path}?token=frozen",
                    media_kind="video",
                    platform="douyin",
                    provider="TikHub",
                    operation="douyin_video_detail",
                )
                self.assertTrue(row["network_allowed"])
                self.assertIsNone(row["deny_reason"])

        denied = (
            (
                f"https://evil.v5-dy-ov-experiment.zjcdn.com{object_path}"
                "?token=frozen",
                "video",
                "douyin",
                "TikHub",
                "douyin_video_detail",
            ),
            (
                "https://v5-dy-ov-experiment.zjcdn.com/wrong/path/"
                "?token=frozen",
                "video",
                "douyin",
                "TikHub",
                "douyin_video_detail",
            ),
            (
                f"https://v5-dy-ov-experiment.zjcdn.com{object_path}",
                "video",
                "douyin",
                "TikHub",
                "douyin_video_detail",
            ),
            (
                f"https://v5-dy-ov-experiment.zjcdn.com:443{object_path}"
                "?token=frozen",
                "video",
                "douyin",
                "TikHub",
                "douyin_video_detail",
            ),
            (
                "https://v5-dy-ov-experiment.zjcdn.com/"
                f"{'a' * 32}/{'b' * 8}/video/tos/cn/tos-cn-ve-15/"
                f"{'C' * 37}/?token=frozen",
                "video",
                "douyin",
                "TikHub",
                "douyin_video_detail",
            ),
            (
                "https://v5-dy-ov-experiment.zjcdn.com/"
                f"{'a' * 32}/{'b' * 8}/video/tos/cn/tos-cn-ve-15/"
                f"{'C' * 39}/?token=frozen",
                "video",
                "douyin",
                "TikHub",
                "douyin_video_detail",
            ),
            (
                "https://v5-dy-ov-experiment.zjcdn.com/"
                f"{'a' * 32}/{'b' * 8}/video/tos/cn/tos-cn-ve-15/"
                "CCCC%2F..%2FCCCC?token=frozen",
                "video",
                "douyin",
                "TikHub",
                "douyin_video_detail",
            ),
            (
                "https://api-play.amemv.com/aweme/v1/play/?video_id=frozen",
                "video",
                "douyin",
                "TikHub",
                "douyin_video_detail",
            ),
            (
                f"https://v5-dy-ov-experiment.zjcdn.com{object_path}"
                "?token=frozen#fragment",
                "video",
                "douyin",
                "TikHub",
                "douyin_video_detail",
            ),
            (
                f"https://v5-dy-ov-experiment.zjcdn.com{object_path}"
                "?token=frozen",
                "video",
                "xiaohongshu",
                "TikHub",
                "douyin_video_detail",
            ),
            (
                f"https://v5-dy-ov-experiment.zjcdn.com{object_path}"
                "?token=frozen",
                "image",
                "douyin",
                "TikHub",
                "douyin_video_detail",
            ),
            (
                f"https://v5-dy-ov-experiment.zjcdn.com{object_path}"
                "?token=frozen",
                "video",
                "douyin",
                "fixture",
                "douyin_video_detail",
            ),
            (
                f"https://v5-dy-ov-experiment.zjcdn.com{object_path}"
                "?token=frozen",
                "video",
                "douyin",
                "TikHub",
                "xiaohongshu_note_detail",
            ),
        )
        for url, media_kind, platform, provider, operation in denied:
            with self.subTest(
                url=url,
                media_kind=media_kind,
                platform=platform,
                provider=provider,
                operation=operation,
            ):
                row = canary._safe_url(
                    url,
                    media_kind=media_kind,
                    platform=platform,
                    provider=provider,
                    operation=operation,
                )
                self.assertFalse(row["network_allowed"])
                self.assertEqual(
                    row["deny_reason"], "host_not_in_media_cdn_allowlist"
                )

    def test_network_tripwire_blocks_dns_udp_subprocess_audio_and_total_overrun(self) -> None:
        urls = [
            "https://img1.xhscdn.com/one.jpg",
            "https://img1.xhscdn.com/two.jpg",
        ]
        guard = canary.ExactUrlNetworkGuard(
            urls, media_kind="image", maximum_bytes=5
        )
        responses = iter([_Response(urls[0], b"abc", content_type="image/jpeg"), _Response(urls[1], b"def", content_type="image/jpeg")])
        guard._opener = SimpleNamespace(open=lambda *_args, **_kwargs: next(responses))
        with guard.open(urls[0]) as first:
            self.assertEqual(first.read(), b"abc")
        with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "累计上限"):
            guard.open(urls[1])
        shared_budget = canary._DownloadBudget(5)
        first_guard = canary.ExactUrlNetworkGuard(
            [urls[0]],
            media_kind="image",
            maximum_bytes=5,
            budget=shared_budget,
        )
        first_guard._opener = SimpleNamespace(
            open=lambda *_args, **_kwargs: _Response(
                urls[0], b"abc", content_type="image/jpeg"
            )
        )
        with first_guard.open(urls[0]) as first:
            first.read()
        second_guard = canary.ExactUrlNetworkGuard(
            [urls[1]],
            media_kind="image",
            maximum_bytes=5,
            budget=shared_budget,
        )
        second_guard._opener = SimpleNamespace(
            open=lambda *_args, **_kwargs: _Response(
                urls[1], b"def", content_type="image/jpeg"
            )
        )
        with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "run剩余"):
            second_guard.open(urls[1])
        short_guard = canary.ExactUrlNetworkGuard(
            [urls[0]], media_kind="image", maximum_bytes=100
        )
        short_response = _Response(
            urls[0], b"short", content_type="image/jpeg"
        )
        short_response.headers["Content-Length"] = "10"
        short_guard._opener = SimpleNamespace(
            open=lambda *_args, **_kwargs: short_response
        )
        with self.assertRaisesRegex(
            canary.LocalAnalysisCanaryError, "Content-Length"
        ):
            with short_guard.open(urls[0]) as response:
                self.assertEqual(response.read(), b"short")
        for raw_length in ("0", "-1", "garbage"):
            strict_guard = canary.ExactUrlNetworkGuard(
                [urls[0]], media_kind="image", maximum_bytes=100
            )
            strict_response = _Response(
                urls[0], b"x", content_type="image/jpeg"
            )
            strict_response.headers["Content-Length"] = raw_length
            strict_guard._opener = SimpleNamespace(
                open=lambda *_args, response=strict_response, **_kwargs: response
            )
            with self.assertRaisesRegex(
                canary.LocalAnalysisCanaryError, "Content-Length"
            ):
                with strict_guard.open(urls[0]) as response:
                    response.read()
        video_guard = canary.ExactUrlNetworkGuard(
            [self.urls[0]], media_kind="video", maximum_bytes=100
        )
        video_guard._opener = SimpleNamespace(
            open=lambda *_args, **_kwargs: _Response(
                self.urls[0], b"audio", content_type="audio/mpeg"
            )
        )
        with self.assertRaisesRegex(media.MediaProcessingError, "audio MIME"):
            video_guard.open(self.urls[0])
        with canary._execution_guards(
            self.urls,
            media_kind="video",
            maximum_bytes=100,
            tools=self.tools,
        ):
            with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "远程调用"):
                providers._load_key(Path("missing"), "KEY")
            with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "DNS"):
                socket.getaddrinfo("unfrozen.example", 443)
            datagram = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "UDP"):
                    datagram.sendto(b"x", ("8.8.8.8", 53))
            finally:
                datagram.close()
            with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "子进程"):
                subprocess.Popen(["curl", "https://unfrozen.example"])

        playlist = self.root / "malicious-playlist.m3u8"
        playlist.write_text("https://unfrozen.example/segment.ts\n")
        ffprobe = self.root / "ffprobe-fixture"
        ffprobe.write_bytes(self.binary.read_bytes())
        ffprobe.chmod(0o700)
        protocol_tools = dict(self.tools)
        protocol_tools["ffprobe"] = self._tool(ffprobe)
        popen_calls: list[list[str]] = []

        def local_popen(command, *_args, **_kwargs):
            popen_calls.append(list(command))
            return object()

        with patch("subprocess.Popen", side_effect=local_popen):
            with canary._execution_guards(
                self.urls,
                media_kind="video",
                maximum_bytes=100,
                tools=protocol_tools,
            ):
                subprocess.Popen([str(ffprobe), str(playlist)])
        self.assertEqual(
            popen_calls[0][1:3], ["-protocol_whitelist", "file,pipe"]
        )
        self.assertEqual(popen_calls[0][3], str(playlist))

    def test_video_audio_mime_is_candidate_local_and_mirror_falls_back(self) -> None:
        urls = [
            "https://v1.douyinvod.com/audio-candidate.mp4",
            "https://v2.douyinvod.com/video-candidate.mp4",
        ]
        bodies = {
            urls[0]: (b"audio-body", "audio/mp4"),
            urls[1]: (b"selected-video-body", "video/mp4"),
        }
        sources = {
            1: {
                "urls": [
                    {
                        "url": url,
                        "host": urllib.parse.urlsplit(url).hostname,
                    }
                    for url in urls
                ],
                "download_urls": urls,
                "download_urls_sha256": canary._json_sha256(urls),
            }
        }
        ledger = canary._NetworkLedger(
            self.root / "audio-fallback-ledger.json",
            contract_sha256="c" * 64,
            intent_sha256="i" * 64,
            content_ids=[1],
            maximum_bytes=100,
            sources=sources,
        )
        guard = canary.ExactUrlNetworkGuard(
            urls,
            media_kind="video",
            maximum_bytes=100,
            ledger=ledger,
            content_id=1,
        )
        calls: list[str] = []

        def open_response(request, **_kwargs):
            url = request.full_url
            calls.append(url)
            body, content_type = bodies[url]
            return _Response(url, body, content_type=content_type)

        guard._opener = SimpleNamespace(open=open_response)
        target = self.root / "selected-video.mp4"

        def valid_selected_response(
            path: Path, *, inherited_descriptor=None, **_kwargs
        ) -> bool:
            if inherited_descriptor is not None:
                return (
                    os.pread(inherited_descriptor, 100, 0)
                    == bodies[urls[1]][0]
                )
            return (
                Path(path).is_file()
                and Path(path).read_bytes() == bodies[urls[1]][0]
            )

        with patch.object(
            media,
            "_valid_media",
            side_effect=valid_selected_response,
        ):
            result = media._download_video(
                urls,
                target,
                urlopen_fn=guard.open,
                maximum_bytes=100,
                require_exact_response_url=True,
            )
        self.assertEqual(result, target)
        self.assertEqual(target.read_bytes(), bodies[urls[1]][0])
        self.assertEqual(calls, urls)
        events = ledger.transcript(1)
        self.assertEqual(len(events), 2)
        self.assertEqual(
            (events[0]["status"], events[0]["mime"], events[0]["bytes"]),
            (200, "audio/mp4", 0),
        )
        self.assertEqual(events[0]["outcome"], "failed")
        self.assertIn("MediaProcessingError", str(events[0]["error"]))
        self.assertEqual(events[1]["outcome"], "succeeded")
        self.assertEqual(events[1]["mime"], "video/mp4")
        self.assertEqual(events[1]["bytes"], len(bodies[urls[1]][0]))
        self.assertEqual(
            events[1]["response_sha256"],
            hashlib.sha256(bodies[urls[1]][0]).hexdigest(),
        )
        self.assertEqual(ledger.total_bytes, len(bodies[urls[1]][0]))

        all_audio_ledger = canary._NetworkLedger(
            self.root / "all-audio-ledger.json",
            contract_sha256="c" * 64,
            intent_sha256="j" * 64,
            content_ids=[1],
            maximum_bytes=100,
            sources=sources,
        )
        all_audio_guard = canary.ExactUrlNetworkGuard(
            urls,
            media_kind="video",
            maximum_bytes=100,
            ledger=all_audio_ledger,
            content_id=1,
        )
        all_audio_calls: list[str] = []

        def open_audio(request, **_kwargs):
            url = request.full_url
            all_audio_calls.append(url)
            return _Response(url, b"audio", content_type="audio/mp4")

        all_audio_guard._opener = SimpleNamespace(open=open_audio)
        all_audio_target = self.root / "all-audio.mp4"
        with (
            patch.object(media, "_valid_media", return_value=False),
            self.assertRaisesRegex(media.MediaProcessingError, "download failed"),
        ):
            media._download_video(
                urls,
                all_audio_target,
                urlopen_fn=all_audio_guard.open,
                maximum_bytes=100,
                require_exact_response_url=True,
            )
        self.assertEqual(all_audio_calls, urls)
        self.assertFalse(all_audio_target.exists())
        all_audio_events = all_audio_ledger.transcript(1)
        self.assertEqual(len(all_audio_events), 2)
        self.assertTrue(
            all(
                event["outcome"] == "failed"
                and event["mime"] == "audio/mp4"
                and event["bytes"] == 0
                for event in all_audio_events
            )
        )

    def test_durable_network_budget_and_terminal_events_survive_resume(self) -> None:
        url = "https://img1.xhscdn.com/one.jpg"
        source = {
            "urls": [{"url": url, "host": "img1.xhscdn.com"}],
            "download_urls": [url],
            "download_urls_sha256": canary._json_sha256([url]),
        }
        ledger_path = self.root / "durable-network.json"
        ledger = canary._NetworkLedger(
            ledger_path,
            contract_sha256="a" * 64,
            intent_sha256="b" * 64,
            content_ids=[1],
            maximum_bytes=5,
            sources={1: source},
        )
        budget = canary._DownloadBudget(5, ledger=ledger)
        guard = canary.ExactUrlNetworkGuard(
            [url],
            media_kind="image",
            maximum_bytes=5,
            budget=budget,
            ledger=ledger,
            content_id=1,
        )
        guard._opener = SimpleNamespace(
            open=lambda *_args, **_kwargs: _Response(
                url, b"abc", content_type="image/jpeg"
            )
        )
        with guard.open(url) as response:
            self.assertEqual(response.read(), b"abc")
        reloaded = canary._NetworkLedger(
            ledger_path,
            contract_sha256="a" * 64,
            intent_sha256="b" * 64,
            content_ids=[1],
            maximum_bytes=5,
            sources={1: source},
        )
        self.assertEqual(reloaded.total_bytes, 3)
        self.assertEqual(reloaded.budget_consumed_bytes, 3)
        self.assertEqual(reloaded.value["events"][0]["response_sha256"], hashlib.sha256(b"abc").hexdigest())

        class _IgnoringResponse(_Response):
            def read(self, _size: int = -1) -> bytes:
                return self._body.read()

        overrun_path = self.root / "overrun-network.json"
        overrun = canary._NetworkLedger(
            overrun_path,
            contract_sha256="c" * 64,
            intent_sha256="d" * 64,
            content_ids=[1],
            maximum_bytes=2,
            sources={1: source},
        )
        overrun_guard = canary.ExactUrlNetworkGuard(
            [url],
            media_kind="image",
            maximum_bytes=2,
            budget=canary._DownloadBudget(2, ledger=overrun),
            ledger=overrun,
            content_id=1,
        )
        response = _IgnoringResponse(url, b"four", content_type="image/jpeg")
        response.headers.pop("Content-Length")
        overrun_guard._opener = SimpleNamespace(
            open=lambda *_args, **_kwargs: response
        )
        with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "预留预算"):
            with overrun_guard.open(url) as opened:
                opened.read()
        reloaded_overrun = canary._NetworkLedger(
            overrun_path,
            contract_sha256="c" * 64,
            intent_sha256="d" * 64,
            content_ids=[1],
            maximum_bytes=2,
            sources={1: source},
        )
        self.assertEqual(reloaded_overrun.total_bytes, 4)
        self.assertEqual(reloaded_overrun.budget_consumed_bytes, 4)
        self.assertTrue(reloaded_overrun.overrun)

        opening_path = self.root / "opening-network.json"
        opening = canary._NetworkLedger(
            opening_path,
            contract_sha256="e" * 64,
            intent_sha256="f" * 64,
            content_ids=[1],
            maximum_bytes=5,
            sources={1: source},
        )
        opening.begin(1, url)
        frozen = opening_path.read_bytes()
        with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "非终态"):
            canary._NetworkLedger(
                opening_path,
                contract_sha256="e" * 64,
                intent_sha256="f" * 64,
                content_ids=[1],
                maximum_bytes=5,
                sources={1: source},
                recover_incomplete=False,
            )
        self.assertEqual(opening_path.read_bytes(), frozen)

    def test_network_ledger_requires_exact_numeric_and_boolean_types(self) -> None:
        url = "https://img1.xhscdn.com/exact.jpg"
        source = {
            "urls": [{"url": url, "host": "img1.xhscdn.com"}],
            "download_urls": [url],
            "download_urls_sha256": canary._json_sha256([url]),
        }
        ledger = canary._NetworkLedger(
            self.root / "exact-types-network.json",
            contract_sha256="a" * 64,
            intent_sha256="b" * 64,
            content_ids=[1],
            maximum_bytes=5,
            sources={1: source},
        )
        event_index = ledger.begin(1, url)
        ledger.update(
            event_index,
            status=503,
            mime="image/jpeg",
            declared_bytes=0,
            outcome="failed",
            error="fixture failure",
        )
        mutations = {
            "content_ids": lambda value: value.__setitem__(
                "content_ids", [True]
            ),
            "maximum_bytes": lambda value: value.__setitem__(
                "maximum_bytes", "5"
            ),
            "total_bytes": lambda value: value.__setitem__(
                "total_bytes", False
            ),
            "budget_consumed_bytes": lambda value: value.__setitem__(
                "budget_consumed_bytes", "0"
            ),
            "overrun": lambda value: value.__setitem__("overrun", 0),
            "update_index": lambda value: value.__setitem__(
                "update_index", True
            ),
            "event_index": lambda value: value["events"][0].__setitem__(
                "event_index", True
            ),
            "event_content_id": lambda value: value["events"][0].__setitem__(
                "content_id", "1"
            ),
            "status": lambda value: value["events"][0].__setitem__(
                "status", True
            ),
            "declared_bytes": lambda value: value["events"][0].__setitem__(
                "declared_bytes", False
            ),
            "bytes": lambda value: value["events"][0].__setitem__(
                "bytes", False
            ),
            "charged_bytes": lambda value: value["events"][0].__setitem__(
                "charged_bytes", "0"
            ),
        }
        for label, mutate in mutations.items():
            with self.subTest(field=label), self.assertRaisesRegex(
                canary.LocalAnalysisCanaryError, "network ledger"
            ):
                value = json.loads(json.dumps(ledger.value))
                mutate(value)
                ledger._validate(value)

    def test_retryable_state_rejects_network_and_progress_rollback(self) -> None:
        processing_calls = 0

        def fail_after_network(_content_id: int, **kwargs):
            nonlocal processing_calls
            processing_calls += 1
            with kwargs["urlopen_fn"](
                urllib.request.Request(self.urls[0]), timeout=90
            ) as response:
                self.assertTrue(response.read())
            raise RuntimeError("fail after durable response")

        with self.assertRaisesRegex(
            canary.LocalAnalysisCanaryError, "fail after durable response"
        ):
            self._run(media_side_effect=fail_after_network)

        state_path = self.run_root / "state.json"
        ledger_path = self.run_root / "network-ledger.json"
        progress_path = self.run_root / "progress.json"
        state = json.loads(state_path.read_text())
        ledger = json.loads(ledger_path.read_text())
        progress = json.loads(progress_path.read_text())
        self.assertGreater(state["network_total_bytes"], 0)
        self.assertEqual(
            state["network_ledger_sha256"], canary._sha256_file(ledger_path)
        )
        self.assertEqual(state["progress_sha256"], canary._sha256_file(progress_path))
        self.assertEqual(
            progress["network_ledger_sha256"], state["network_ledger_sha256"]
        )

        reset_ledger = {
            **ledger,
            "events": [],
            "total_bytes": 0,
            "budget_consumed_bytes": 0,
            "overrun": False,
            "update_index": 0,
            "previous_ledger_sha256": None,
        }
        ledger_path.write_bytes(canary._canonical_bytes(reset_ledger))
        reset_progress = {
            **progress,
            "network_ledger_sha256": canary._sha256_file(ledger_path),
        }
        progress_path.write_bytes(canary._canonical_bytes(reset_progress))
        rolled_ledger = ledger_path.read_bytes()
        rolled_progress = progress_path.read_bytes()

        with self.assertRaisesRegex(
            canary.LocalAnalysisCanaryError, "analysis state合同"
        ):
            self._run(media_side_effect=fail_after_network)
        self.assertEqual(processing_calls, 1)
        self.assertEqual(ledger_path.read_bytes(), rolled_ledger)
        self.assertEqual(progress_path.read_bytes(), rolled_progress)

    def test_retryable_state_accepts_bound_network_prefix_after_hard_kill(self) -> None:
        def fail_after_network(_content_id: int, **kwargs):
            with kwargs["urlopen_fn"](
                urllib.request.Request(self.urls[0]), timeout=90
            ) as response:
                self.assertTrue(response.read())
            raise RuntimeError("first handled failure")

        with self.assertRaisesRegex(
            canary.LocalAnalysisCanaryError, "first handled failure"
        ):
            self._run(media_side_effect=fail_after_network)

        state_path = self.run_root / "state.json"
        ledger_path = self.run_root / "network-ledger.json"
        contract = json.loads((self.run_root / "run-contract.json").read_text())
        sources = {
            int(source["content"]["id"]): source for source in contract["sources"]
        }
        source_sha256 = canary._source_artifact_sha256(contract["sources"][0])
        stale = (
            datetime.now(timezone.utc)
            - timedelta(seconds=media.STALE_MEDIA_SLOT_SECONDS + 10)
        ).isoformat(timespec="seconds")
        with closing(connect(self.db)) as connection:
            connection.execute(
                """
                INSERT INTO media_processing_slots(
                    content_id,source_sha256,processor_type,processor_version,status,
                    attempt_count,error_message,created_at,updated_at
                ) VALUES (1,?,'download',?,'retryable_failed',1,'first failure',?,?)
                """,
                (
                    source_sha256,
                    media.VIDEO_DOWNLOAD_VERSION,
                    stale,
                    stale,
                ),
            )
            connection.commit()
        checkpoint_state = canary._state_value(
            canary._paths(
                source_db_path=self.source_db,
                source_completion_path=self.source_completion,
                db_path=self.db,
                media_root=self.media_root,
                run_root=self.run_root,
            ),
            status="retryable_failed",
            contract_sha256=canary._sha256_file(
                self.run_root / "run-contract.json"
            ),
            intent_sha256=canary._sha256_file(self.run_root / "intent.json"),
            error="RuntimeError: first handled failure",
        )
        canary._write_json(state_path, checkpoint_state, immutable=False)
        old_state_sha256 = canary._sha256_file(state_path)
        with closing(connect(self.db)) as connection:
            slot_id, cached = media._claim_processing_slot(
                connection,
                content_id=1,
                source_sha256=source_sha256,
                processor_type="download",
                processor_version=media.VIDEO_DOWNLOAD_VERSION,
            )
            self.assertIsNone(cached)
            connection.execute(
                "UPDATE media_processing_slots SET updated_at=? WHERE id=?",
                (stale, slot_id),
            )
            connection.commit()
        ledger = canary._NetworkLedger(
            ledger_path,
            contract_sha256=canary._sha256_file(self.run_root / "run-contract.json"),
            intent_sha256=canary._sha256_file(self.run_root / "intent.json"),
            content_ids=[1],
            maximum_bytes=int(contract["maximum_download_bytes"]),
            sources=sources,
        )
        ledger.begin(1, self.urls[0])
        self.assertEqual(ledger.value["events"][-1]["outcome"], "opening")

        processing_calls = 0

        def second_failure(_content_id: int, **_kwargs):
            nonlocal processing_calls
            processing_calls += 1
            raise RuntimeError("second handled failure")

        with (
            patch.object(canary, "_process_identity", return_value=None),
            self.assertRaisesRegex(
                canary.LocalAnalysisCanaryError, "second handled failure"
            ),
        ):
            self._run(media_side_effect=second_failure)

        self.assertEqual(processing_calls, 1)
        refreshed_state = json.loads(state_path.read_text())
        refreshed_ledger = json.loads(ledger_path.read_text())
        self.assertEqual(refreshed_state["previous_state_sha256"], old_state_sha256)
        self.assertEqual(
            refreshed_state["network_ledger_sha256"],
            canary._sha256_file(ledger_path),
        )
        self.assertEqual(
            refreshed_state["network_events_prefix_sha256"],
            canary._json_sha256(refreshed_ledger["events"]),
        )
        self.assertEqual(refreshed_ledger["events"][-1]["outcome"], "interrupted")
        with closing(connect(self.db)) as connection:
            recovered_slot = connection.execute(
                "SELECT status,attempt_count FROM media_processing_slots WHERE id=?",
                (slot_id,),
            ).fetchone()
        self.assertEqual(tuple(recovered_slot), ("retryable_failed", 2))

    def test_retryable_state_rejects_advanced_ledger_prefix_drift(self) -> None:
        processing_calls = 0

        def fail_after_network(_content_id: int, **kwargs):
            nonlocal processing_calls
            processing_calls += 1
            with kwargs["urlopen_fn"](
                urllib.request.Request(self.urls[0]), timeout=90
            ) as response:
                self.assertTrue(response.read())
            raise RuntimeError("freeze checkpoint")

        with self.assertRaises(canary.LocalAnalysisCanaryError):
            self._run(media_side_effect=fail_after_network)

        ledger_path = self.run_root / "network-ledger.json"
        contract = json.loads((self.run_root / "run-contract.json").read_text())
        sources = {
            int(source["content"]["id"]): source for source in contract["sources"]
        }
        ledger = canary._NetworkLedger(
            ledger_path,
            contract_sha256=canary._sha256_file(self.run_root / "run-contract.json"),
            intent_sha256=canary._sha256_file(self.run_root / "intent.json"),
            content_ids=[1],
            maximum_bytes=int(contract["maximum_download_bytes"]),
            sources=sources,
        )
        ledger.begin(1, self.urls[0])
        drifted = json.loads(ledger_path.read_text())
        drifted["events"][0]["error"] = "forged-prefix"
        ledger_path.write_bytes(canary._canonical_bytes(drifted))
        frozen = ledger_path.read_bytes()

        with self.assertRaisesRegex(
            canary.LocalAnalysisCanaryError, "analysis state合同"
        ):
            self._run(media_side_effect=fail_after_network)
        self.assertEqual(processing_calls, 1)
        self.assertEqual(ledger_path.read_bytes(), frozen)

    def test_retryable_state_accepts_completed_progress_before_receipt(self) -> None:
        self._freeze_failed_contract()
        state_path = self.run_root / "state.json"
        receipt_path = self.run_root / "receipt.json"
        progress_path = self.run_root / "progress.json"
        predecessor = state_path.read_bytes()
        processing_calls = 0

        def successful_media(content_id: int, **kwargs):
            nonlocal processing_calls
            processing_calls += 1
            return self._fake_media(content_id, **kwargs)

        disk_calls = 0
        original_disk_capacity = canary._disk_capacity

        def crash_after_progress(paths):
            nonlocal disk_calls
            disk_calls += 1
            if disk_calls == 2:
                raise RuntimeError("hard kill before receipt")
            return original_disk_capacity(paths)

        with (
            patch.object(canary, "_disk_capacity", side_effect=crash_after_progress),
            self.assertRaisesRegex(RuntimeError, "hard kill before receipt"),
        ):
            self._run(media_side_effect=successful_media)

        self.assertFalse(receipt_path.exists())
        self.assertEqual(state_path.read_bytes(), predecessor)
        progress_bytes = progress_path.read_bytes()
        progress = json.loads(progress_bytes)
        self.assertEqual(progress["completed_ids"], [1])
        recovered = self._run(media_side_effect=successful_media)
        self.assertEqual(recovered["status"], "succeeded")
        self.assertEqual(processing_calls, 1)
        self.assertTrue(receipt_path.exists())

    def test_completed_progress_database_drift_blocks_before_reprocessing(self) -> None:
        self._freeze_failed_contract()
        state_path = self.run_root / "state.json"
        progress_path = self.run_root / "progress.json"
        receipt_path = self.run_root / "receipt.json"
        processing_calls = 0

        def successful_media(content_id: int, **kwargs):
            nonlocal processing_calls
            processing_calls += 1
            return self._fake_media(content_id, **kwargs)

        disk_calls = 0
        original_disk_capacity = canary._disk_capacity

        def crash_after_progress(paths):
            nonlocal disk_calls
            disk_calls += 1
            if disk_calls == 2:
                raise RuntimeError("hard kill before receipt")
            return original_disk_capacity(paths)

        with (
            patch.object(canary, "_disk_capacity", side_effect=crash_after_progress),
            self.assertRaisesRegex(RuntimeError, "hard kill before receipt"),
        ):
            self._run(media_side_effect=successful_media)

        progress = json.loads(progress_path.read_text())
        progress["database"] = json.loads(state_path.read_text())["database"]
        progress_path.write_bytes(canary._canonical_bytes(progress))
        frozen_progress = progress_path.read_bytes()
        with self.assertRaisesRegex(
            canary.LocalAnalysisCanaryError,
            "成功progress未精确绑定最终数据库identity",
        ):
            self._run(media_side_effect=successful_media)
        self.assertEqual(processing_calls, 1)
        self.assertFalse(receipt_path.exists())
        self.assertEqual(progress_path.read_bytes(), frozen_progress)

    def _freeze_failed_contract(self) -> dict[str, object]:
        with self.assertRaises(canary.LocalAnalysisCanaryError):
            self._run(media_side_effect=RuntimeError("freeze intent"))
        return json.loads((self.run_root / "run-contract.json").read_text())

    def _insert_running_download(self, *, stale: bool, source_sha: str | None = None) -> None:
        (self.run_root / "state.json").unlink(missing_ok=True)
        updated_at = (
            datetime.now(timezone.utc)
            - timedelta(seconds=media.STALE_MEDIA_SLOT_SECONDS + 10)
            if stale
            else datetime.now(timezone.utc)
        ).isoformat(timespec="seconds")
        with closing(connect(self.db)) as connection:
            source_artifact = connection.execute(
                """
                SELECT sha256 FROM evidence_artifacts
                WHERE content_id=1 AND artifact_type='media_source'
                  AND status='available' AND processor_version=?
                ORDER BY id DESC LIMIT 1
                """,
                (media.MEDIA_SOURCE_VERSION,),
            ).fetchone()
            self.assertIsNotNone(source_artifact)
            expected_source_sha = str(source_artifact["sha256"])
            connection.execute(
                """
                INSERT INTO media_processing_slots(
                    content_id,source_sha256,processor_type,processor_version,status,
                    attempt_count,created_at,updated_at
                ) VALUES (1,?,'download',?,'running',1,?,?)
                """,
                (
                    source_sha or expected_source_sha,
                    media.VIDEO_DOWNLOAD_VERSION,
                    updated_at,
                    updated_at,
                ),
            )
            connection.commit()
        self._finalize(self.db)

    def _create_current_video_download(self) -> media.Artifact:
        def fake_download(_urls, target: Path, **_kwargs) -> Path:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b"current-video-download" * 100)
            return target

        with patch.object(media, "_download_video", side_effect=fake_download):
            artifact = media.download_video_sources(
                1,
                self.urls,
                db_path=self.db,
                media_root=self.media_root,
                reuse_existing=False,
            )
        self._finalize(self.db)
        return artifact

    def test_running_slot_recovery_requires_dead_owner_stale_and_exact_identity(self) -> None:
        self._freeze_failed_contract()
        self._insert_running_download(stale=True)
        with patch.object(canary, "_process_identity", return_value=None):
            result = self._run()
        self.assertEqual(result["status"], "succeeded")
        receipt = json.loads((self.run_root / "receipt.json").read_text())
        self.assertEqual(receipt["startup_recovery"]["recovered"], 1)
        self.assertEqual(
            receipt["startup_recovery"]["slot_attempt_expectations"],
            [
                {
                    "slot_id": 1,
                    "content_id": 1,
                    "source_sha256": canary._source_artifact_sha256(
                        json.loads((self.run_root / "run-contract.json").read_text())[
                            "sources"
                        ][0]
                    ),
                    "processor_type": "download",
                    "processor_version": media.VIDEO_DOWNLOAD_VERSION,
                    "from_attempt_count": 1,
                    "expected_attempt_count": 2,
                }
            ],
        )
        self.assertTrue((self.run_root / "running-recovery.json").exists())
        with closing(canary._immutable_connection(self.db)) as connection:
            slot = connection.execute(
                "SELECT status,attempt_count FROM media_processing_slots "
                "WHERE processor_type='download'"
            ).fetchone()
        self.assertEqual(tuple(slot), ("succeeded", 2))
        before = self._tree_state(self.root)
        repeated = self._run()
        self.assertTrue(repeated["idempotent"])
        self.assertEqual(before, self._tree_state(self.root))

    def test_running_recovery_sidecar_never_authorizes_live_identity_drift(self) -> None:
        contract = self._freeze_failed_contract()
        self._insert_running_download(stale=True)
        paths = canary._paths(
            source_db_path=self.source_db,
            source_completion_path=self.source_completion,
            db_path=self.db,
            media_root=self.media_root,
            run_root=self.run_root,
        )
        intent = json.loads(paths.intent.read_text())
        original_write = canary._write_json

        def crash_after_sidecar(path: Path, value, *args, **kwargs):
            original_write(path, value, *args, **kwargs)
            if path == paths.running_recovery:
                raise RuntimeError("crash after recovery sidecar")

        with (
            patch.object(canary, "_process_identity", return_value=None),
            patch.object(canary, "_write_json", side_effect=crash_after_sidecar),
            self.assertRaisesRegex(RuntimeError, "recovery sidecar"),
        ):
            canary._recover_owned_running_slots(
                paths,
                [1],
                intent_exists=True,
                intent=intent,
                contract=contract,
            )
        frozen_sidecar = paths.running_recovery.read_bytes()
        with closing(connect(self.db)) as connection:
            connection.execute(
                "UPDATE media_processing_slots SET source_sha256=?",
                ("0" * 64,),
            )
            connection.commit()
        self._finalize(self.db)
        with (
            patch.object(canary, "_process_identity", return_value=None),
            self.assertRaisesRegex(
                canary.LocalAnalysisCanaryError, "live slot身份漂移"
            ),
        ):
            canary._recover_owned_running_slots(
                paths,
                [1],
                intent_exists=True,
                intent=intent,
                contract=contract,
            )
        self.assertEqual(paths.running_recovery.read_bytes(), frozen_sidecar)
        with closing(canary._immutable_connection(self.db)) as connection:
            row = connection.execute(
                "SELECT source_sha256,status,attempt_count FROM media_processing_slots"
            ).fetchone()
        self.assertEqual(tuple(row), ("0" * 64, "running", 1))

    def test_running_recovery_succeeded_adjacent_requires_exact_output(self) -> None:
        contract = self._freeze_failed_contract()
        self._insert_running_download(stale=True)
        paths = canary._paths(
            source_db_path=self.source_db,
            source_completion_path=self.source_completion,
            db_path=self.db,
            media_root=self.media_root,
            run_root=self.run_root,
        )
        intent = json.loads(paths.intent.read_text())
        original_write = canary._write_json

        def crash_after_sidecar(path: Path, value, *args, **kwargs):
            original_write(path, value, *args, **kwargs)
            if path == paths.running_recovery:
                raise RuntimeError("crash after recovery sidecar")

        with (
            patch.object(canary, "_process_identity", return_value=None),
            patch.object(canary, "_write_json", side_effect=crash_after_sidecar),
            self.assertRaisesRegex(RuntimeError, "recovery sidecar"),
        ):
            canary._recover_owned_running_slots(
                paths,
                [1],
                intent_exists=True,
                intent=intent,
                contract=contract,
            )
        frozen_sidecar = paths.running_recovery.read_bytes()
        with closing(connect(self.db)) as connection:
            forged_output = connection.execute(
                "SELECT id FROM evidence_artifacts "
                "WHERE artifact_type='media_source'"
            ).fetchone()[0]
            connection.execute(
                """
                UPDATE media_processing_slots
                SET status='succeeded',attempt_count=2,output_artifact_id=?,
                    error_message=NULL
                """,
                (forged_output,),
            )
            connection.commit()
        self._finalize(self.db)
        with (
            patch.object(canary, "_process_identity", return_value=None),
            self.assertRaisesRegex(
                canary.LocalAnalysisCanaryError, "current download artifact"
            ),
        ):
            canary._recover_owned_running_slots(
                paths,
                [1],
                intent_exists=True,
                intent=intent,
                contract=contract,
            )
        self.assertEqual(paths.running_recovery.read_bytes(), frozen_sidecar)
        with closing(canary._immutable_connection(self.db)) as connection:
            row = connection.execute(
                "SELECT status,attempt_count,output_artifact_id "
                "FROM media_processing_slots"
            ).fetchone()
        self.assertEqual(tuple(row), ("succeeded", 2, forged_output))

    def test_running_asr_succeeded_adjacent_rejects_empty_json_output(self) -> None:
        contract = self._freeze_failed_contract()
        download = self._create_current_video_download()
        stale = (
            datetime.now(timezone.utc)
            - timedelta(seconds=media.STALE_MEDIA_SLOT_SECONDS + 10)
        ).isoformat(timespec="seconds")
        with closing(connect(self.db)) as connection:
            connection.execute(
                """
                INSERT INTO media_processing_slots(
                    content_id,source_sha256,processor_type,processor_version,status,
                    attempt_count,created_at,updated_at
                ) VALUES (1,?,'asr',?,'running',1,?,?)
                """,
                (download.sha256, media.processor_versions()["asr"], stale, stale),
            )
            connection.commit()
        self._finalize(self.db)
        paths = canary._paths(
            source_db_path=self.source_db,
            source_completion_path=self.source_completion,
            db_path=self.db,
            media_root=self.media_root,
            run_root=self.run_root,
        )
        intent = json.loads(paths.intent.read_text())
        original_write = canary._write_json

        def crash_after_sidecar(path: Path, value, *args, **kwargs):
            original_write(path, value, *args, **kwargs)
            if path == paths.running_recovery:
                raise RuntimeError("crash after ASR recovery sidecar")

        with (
            patch.object(canary, "_process_identity", return_value=None),
            patch.object(canary, "_write_json", side_effect=crash_after_sidecar),
            self.assertRaisesRegex(RuntimeError, "ASR recovery sidecar"),
        ):
            canary._recover_owned_running_slots(
                paths,
                [1],
                intent_exists=True,
                intent=intent,
                contract=contract,
            )
        frozen_sidecar = paths.running_recovery.read_bytes()
        asr_path = self.media_root / "C4N4RY" / "asr.json"
        asr_path.write_text("{}\n", encoding="utf-8")
        with closing(connect(self.db)) as connection:
            artifact = self._artifact(
                connection,
                artifact_type="asr",
                path=asr_path,
                processor_version=media.processor_versions()["asr"],
            )
            connection.execute(
                "UPDATE media_processing_slots SET status='succeeded',"
                "attempt_count=2,output_artifact_id=?,error_message=NULL "
                "WHERE processor_type='asr'",
                (artifact["id"],),
            )
            connection.commit()
        self._finalize(self.db)
        with (
            patch.object(canary, "_process_identity", return_value=None),
            self.assertRaisesRegex(
                canary.LocalAnalysisCanaryError, "ASR final orphan"
            ),
        ):
            canary._recover_owned_running_slots(
                paths,
                [1],
                intent_exists=True,
                intent=intent,
                contract=contract,
            )
        self.assertEqual(paths.running_recovery.read_bytes(), frozen_sidecar)
        with closing(canary._immutable_connection(self.db)) as connection:
            row = connection.execute(
                "SELECT status,attempt_count,output_artifact_id "
                "FROM media_processing_slots WHERE processor_type='asr'"
            ).fetchone()
        self.assertEqual(tuple(row), ("succeeded", 2, int(artifact["id"])))

    def test_running_ocr_succeeded_adjacent_rejects_empty_json_output(self) -> None:
        contract = self._freeze_failed_contract()
        download = self._create_current_video_download()
        frames_root = self.media_root / "C4N4RY" / "frames"
        frames_root.mkdir(parents=True)
        frame = frames_root / "frame-000.jpg"
        frame.write_bytes(b"\xff\xd8\xff" + b"F" * 700)
        frames_path = frames_root / "frames.json"
        frames_path.write_text(
            json.dumps(
                {
                    "status": "success",
                    "duration_seconds": 1.0,
                    "frames": [
                        {
                            "path": media._relative(frame),
                            "sha256": canary._sha256_file(frame),
                        }
                    ],
                    "contact_sheet": None,
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        stale = (
            datetime.now(timezone.utc)
            - timedelta(seconds=media.STALE_MEDIA_SLOT_SECONDS + 10)
        ).isoformat(timespec="seconds")
        with closing(connect(self.db)) as connection:
            frames_artifact = self._artifact(
                connection,
                artifact_type="frames_manifest",
                path=frames_path,
                processor_version=media.processor_versions()["frames"],
            )
            self._slot(
                connection,
                processor_type="frames",
                processor_version=media.processor_versions()["frames"],
                artifact=frames_artifact,
                source_sha=download.sha256,
            )
            connection.execute(
                """
                INSERT INTO media_processing_slots(
                    content_id,source_sha256,processor_type,processor_version,status,
                    attempt_count,created_at,updated_at
                ) VALUES (1,?,'ocr',?,'running',1,?,?)
                """,
                (
                    frames_artifact["sha256"],
                    media.processor_versions()["ocr"],
                    stale,
                    stale,
                ),
            )
            connection.commit()
        self._finalize(self.db)
        paths = canary._paths(
            source_db_path=self.source_db,
            source_completion_path=self.source_completion,
            db_path=self.db,
            media_root=self.media_root,
            run_root=self.run_root,
        )
        intent = json.loads(paths.intent.read_text())
        original_write = canary._write_json

        def crash_after_sidecar(path: Path, value, *args, **kwargs):
            original_write(path, value, *args, **kwargs)
            if path == paths.running_recovery:
                raise RuntimeError("crash after OCR recovery sidecar")

        with (
            patch.object(canary, "_process_identity", return_value=None),
            patch.object(canary, "_write_json", side_effect=crash_after_sidecar),
            self.assertRaisesRegex(RuntimeError, "OCR recovery sidecar"),
        ):
            canary._recover_owned_running_slots(
                paths,
                [1],
                intent_exists=True,
                intent=intent,
                contract=contract,
            )
        frozen_sidecar = paths.running_recovery.read_bytes()
        ocr_path = self.media_root / "C4N4RY" / "ocr.json"
        ocr_path.write_text("{}\n", encoding="utf-8")
        with closing(connect(self.db)) as connection:
            artifact = self._artifact(
                connection,
                artifact_type="ocr",
                path=ocr_path,
                processor_version=media.processor_versions()["ocr"],
            )
            connection.execute(
                "UPDATE media_processing_slots SET status='succeeded',"
                "attempt_count=2,output_artifact_id=?,error_message=NULL "
                "WHERE processor_type='ocr'",
                (artifact["id"],),
            )
            connection.commit()
        self._finalize(self.db)
        with (
            patch.object(canary, "_process_identity", return_value=None),
            self.assertRaisesRegex(
                canary.LocalAnalysisCanaryError, "OCR final orphan"
            ),
        ):
            canary._recover_owned_running_slots(
                paths,
                [1],
                intent_exists=True,
                intent=intent,
                contract=contract,
            )
        self.assertEqual(paths.running_recovery.read_bytes(), frozen_sidecar)
        with closing(canary._immutable_connection(self.db)) as connection:
            row = connection.execute(
                "SELECT status,attempt_count,output_artifact_id "
                "FROM media_processing_slots WHERE processor_type='ocr'"
            ).fetchone()
        self.assertEqual(tuple(row), ("succeeded", 2, int(artifact["id"])))

    def test_running_image_ocr_requires_current_composite_download_closure(self) -> None:
        groups_input = [
            ["https://p3-sign.douyinpic.com/recovery-0.jpeg"],
            ["https://p3-sign.douyinpic.com/recovery-1.jpeg"],
        ]
        self._configure_douyin_image_source(groups_input)
        contract = self._freeze_failed_contract()
        source = contract["sources"][0]
        binding = canary._source_image_download_binding(source)
        groups = canary._source_image_groups(source)
        manifest = (
            self.media_root
            / "C4N4RY"
            / "downloads"
            / binding
            / "images"
            / "manifest.json"
        )
        manifest.parent.mkdir(parents=True)
        manifest.write_text('{"arbitrary":true}\n', encoding="utf-8")
        metadata = {
            "source_count": len(groups),
            "source_url_count": len(self.urls),
            "source_sha256": source["artifact_body"]["source_sha256"],
            "image_groups_sha256": source["image_groups_sha256"],
            "download_binding_sha256": binding,
        }
        stale = (
            datetime.now(timezone.utc)
            - timedelta(seconds=media.STALE_MEDIA_SLOT_SECONDS + 10)
        ).isoformat(timespec="seconds")
        with closing(connect(self.db)) as connection:
            artifact = media.register_artifact(
                connection,
                content_id=1,
                artifact_type="media_manifest",
                path=manifest,
                processor_version=media.IMAGE_DOWNLOAD_VERSION,
                metadata=metadata,
            )
            connection.execute(
                """
                INSERT INTO media_processing_slots(
                    content_id,source_sha256,processor_type,processor_version,status,
                    attempt_count,created_at,updated_at
                ) VALUES (1,?,'ocr',?,'running',1,?,?)
                """,
                (artifact.sha256, media.processor_versions()["ocr"], stale, stale),
            )
            connection.commit()
        self._finalize(self.db)
        paths = canary._paths(
            source_db_path=self.source_db,
            source_completion_path=self.source_completion,
            db_path=self.db,
            media_root=self.media_root,
            run_root=self.run_root,
        )
        intent = json.loads(paths.intent.read_text())
        calls_before = dict(self.calls)
        with (
            patch.object(canary, "_process_identity", return_value=None),
            self.assertRaisesRegex(
                canary.LocalAnalysisCanaryError, "current download闭包"
            ),
        ):
            canary._recover_owned_running_slots(
                paths,
                [1],
                intent_exists=True,
                intent=intent,
                contract=contract,
            )
        self.assertFalse(paths.running_recovery.exists())
        self.assertEqual(self.calls, calls_before)
        with closing(canary._immutable_connection(self.db)) as connection:
            row = connection.execute(
                "SELECT status,attempt_count,output_artifact_id "
                "FROM media_processing_slots WHERE processor_type='ocr'"
            ).fetchone()
        self.assertEqual(tuple(row), ("running", 1, None))

    def test_real_retryable_slot_attempt_two_is_frozen_and_receipted(self) -> None:
        contract = self._freeze_failed_contract()
        (self.run_root / "state.json").unlink()
        source_sha = canary._source_artifact_sha256(contract["sources"][0])

        def fail_download() -> Path:
            raise RuntimeError("real first download attempt failed")

        with self.assertRaisesRegex(RuntimeError, "first download attempt"):
            media._run_processing_slot(
                db_path=self.db,
                content_id=1,
                source_sha256=source_sha,
                processor_type="download",
                processor_version=media.VIDEO_DOWNLOAD_VERSION,
                artifact_type="media",
                produce=fail_download,
            )
        self._finalize(self.db)
        with closing(canary._immutable_connection(self.db)) as connection:
            failed = connection.execute(
                "SELECT status,attempt_count FROM media_processing_slots"
            ).fetchone()
        self.assertEqual(tuple(failed), ("retryable_failed", 1))

        result = self._run()

        self.assertEqual(result["status"], "succeeded")
        receipt = json.loads((self.run_root / "receipt.json").read_text())
        self.assertEqual(receipt["startup_recovery"]["recovered"], 1)
        self.assertEqual(
            receipt["startup_recovery"]["slot_attempt_expectations"][0][
                "expected_attempt_count"
            ],
            2,
        )
        with closing(canary._immutable_connection(self.db)) as connection:
            succeeded = connection.execute(
                "SELECT status,attempt_count FROM media_processing_slots "
                "WHERE processor_type='download'"
            ).fetchone()
        self.assertEqual(tuple(succeeded), ("succeeded", 2))

    def test_owned_media_candidate_recovery_is_durable_and_idempotent(self) -> None:
        contract = self._freeze_failed_contract()
        self._insert_running_download(stale=True)
        source = contract["sources"][0]
        candidate = (
            self.media_root
            / "C4N4RY"
            / "downloads"
            / source["artifact_body"]["source_sha256"]
            / ".source.mp4.candidate-0"
        )
        candidate.parent.mkdir(parents=True)
        candidate.write_bytes(b"partial-video")
        quarantine = candidate.with_name(
            f".{candidate.name}.cleanup-"
            f"{canary._sha256_file(candidate)[:16]}.quarantine"
        )
        original_unlink = Path.unlink

        def crash_after_recovery_record(path: Path, *args, **kwargs):
            if path == quarantine:
                raise RuntimeError("output cleanup unlink crash")
            return original_unlink(path, *args, **kwargs)

        with (
            patch.object(canary, "_process_identity", return_value=None),
            patch.object(Path, "unlink", crash_after_recovery_record),
            self.assertRaisesRegex(RuntimeError, "output cleanup unlink"),
        ):
            self._run()

        recovery_path = self.run_root / "output-recovery.json"
        self.assertFalse(candidate.exists())
        self.assertTrue(quarantine.exists())
        self.assertTrue(recovery_path.exists())
        first_recovery = recovery_path.read_bytes()

        quarantine_bytes = quarantine.read_bytes()

        def drift_after_writer_lock(
            connection: sqlite3.Connection, _paths: canary.CanaryPaths
        ) -> None:
            connection.execute(
                "UPDATE media_processing_slots SET source_sha256=? "
                "WHERE processor_type='download'",
                ("0" * 64,),
            )

        with (
            patch.object(canary, "_process_identity", return_value=None),
            patch.object(
                canary,
                "_after_output_recovery_writer_lock",
                side_effect=drift_after_writer_lock,
            ),
            self.assertRaisesRegex(
                canary.LocalAnalysisCanaryError,
                "live slot身份漂移|必需slot集合不精确",
            ),
        ):
            self._run()
        self.assertFalse(candidate.exists())
        self.assertEqual(quarantine.read_bytes(), quarantine_bytes)
        self.assertEqual(recovery_path.read_bytes(), first_recovery)
        with closing(canary._immutable_connection(self.db)) as connection:
            row = connection.execute(
                "SELECT source_sha256 FROM media_processing_slots "
                "WHERE processor_type='download'"
            ).fetchone()
        self.assertEqual(
            row["source_sha256"],
            canary._source_artifact_sha256(contract["sources"][0]),
        )

        with patch.object(canary, "_process_identity", return_value=None):
            recovered = self._run()

        self.assertEqual(recovered["status"], "succeeded")
        self.assertFalse(candidate.exists())
        self.assertFalse(quarantine.exists())
        self.assertEqual(recovery_path.read_bytes(), first_recovery)
        receipt = json.loads((self.run_root / "receipt.json").read_text())
        self.assertEqual(receipt["startup_recovery"]["output_recovered"], 1)
        self.assertEqual(receipt["startup_recovery"]["output_recovery_rounds"], 1)
        tree = self._tree_state(self.root)
        calls = dict(self.calls)
        repeated = self._run()
        self.assertTrue(repeated["idempotent"])
        self.assertEqual(self._tree_state(self.root), tree)
        self.assertEqual(self.calls, calls)

    def test_unknown_or_unproven_final_output_is_preserved_and_blocks(self) -> None:
        contract = self._freeze_failed_contract()
        self._insert_running_download(stale=True)
        source = contract["sources"][0]
        download_root = (
            self.media_root
            / "C4N4RY"
            / "downloads"
            / source["artifact_body"]["source_sha256"]
        )
        download_root.mkdir(parents=True)
        unknown = download_root / "forged.bin"
        unknown.write_bytes(b"unknown")
        database_before = canary._sha256_file(self.db)
        calls_before = dict(self.calls)
        with (
            patch.object(canary, "_process_identity", return_value=None),
            self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "非owned文件"),
        ):
            self._run()
        self.assertEqual(unknown.read_bytes(), b"unknown")
        self.assertFalse((self.run_root / "receipt.json").exists())
        self.assertEqual(self.calls, calls_before)

        unknown.unlink()
        with closing(canary._immutable_connection(self.db)) as connection:
            slot_after_attempt_recovery = tuple(
                connection.execute(
                    "SELECT status,attempt_count,output_artifact_id,error_message "
                    "FROM media_processing_slots"
                ).fetchone()
            )
        final = download_root / "source.mp4"
        final.write_bytes(b"forged-final")
        with self.assertRaisesRegex(
            canary.LocalAnalysisCanaryError, "未绑定成功network响应"
        ):
            self._run()
        self.assertEqual(final.read_bytes(), b"forged-final")
        self.assertFalse((self.run_root / "receipt.json").exists())
        self.assertNotEqual(canary._sha256_file(self.db), database_before)
        with closing(canary._immutable_connection(self.db)) as connection:
            self.assertEqual(
                tuple(
                    connection.execute(
                        "SELECT status,attempt_count,output_artifact_id,error_message "
                        "FROM media_processing_slots"
                    ).fetchone()
                ),
                slot_after_attempt_recovery,
            )
        self.assertEqual(self.calls, calls_before)

    def test_owned_output_is_rehashed_immediately_before_cleanup(self) -> None:
        contract = self._freeze_failed_contract()
        self._insert_running_download(stale=True)
        source = contract["sources"][0]
        candidate = (
            self.media_root
            / "C4N4RY"
            / "downloads"
            / source["artifact_body"]["source_sha256"]
            / ".source.mp4.candidate-0"
        )
        candidate.parent.mkdir(parents=True)
        candidate.write_bytes(b"first-partial")
        original_sha256_descriptor = canary._sha256_descriptor
        original_saved = self.root / "original-candidate.bin"

        def swap_after_descriptor_hash(descriptor: int):
            result = original_sha256_descriptor(descriptor)
            candidate.replace(original_saved)
            candidate.write_bytes(b"changed-after-record")
            return result

        with (
            patch.object(canary, "_process_identity", return_value=None),
            patch.object(
                canary,
                "_sha256_descriptor",
                side_effect=swap_after_descriptor_hash,
            ),
            self.assertRaisesRegex(
                canary.LocalAnalysisCanaryError, "路径身份漂移"
            ),
        ):
            self._run()
        self.assertEqual(candidate.read_bytes(), b"changed-after-record")
        self.assertTrue((self.run_root / "output-recovery.json").exists())
        self.assertFalse((self.run_root / "receipt.json").exists())
        calls_before = dict(self.calls)
        with (
            patch.object(canary, "_process_identity", return_value=None),
            self.assertRaisesRegex(
                canary.LocalAnalysisCanaryError, "同attempt.*字节被替换"
            ),
        ):
            self._run()
        self.assertEqual(candidate.read_bytes(), b"changed-after-record")
        self.assertEqual(self.calls, calls_before)

    def test_forged_asr_final_on_exact_owned_path_is_preserved_and_blocks(self) -> None:
        self._freeze_failed_contract()
        (self.run_root / "state.json").unlink()
        download = self._create_current_video_download()
        content_root = self.media_root / "C4N4RY"
        stale = (
            datetime.now(timezone.utc)
            - timedelta(seconds=media.STALE_MEDIA_SLOT_SECONDS + 10)
        ).isoformat(timespec="seconds")
        with closing(connect(self.db)) as connection:
            connection.execute(
                "INSERT INTO media_processing_slots("
                "content_id,source_sha256,processor_type,processor_version,status,"
                "attempt_count,created_at,updated_at) VALUES (1,?,'asr',?,'running',1,?,?)",
                (download.sha256, media.processor_versions()["asr"], stale, stale),
            )
            connection.commit()
        self._finalize(self.db)
        forged = content_root / "asr.json"
        forged.write_text('{"status":"success","forged":true}\n')
        calls_before = dict(self.calls)

        with (
            patch.object(canary, "_process_identity", return_value=None),
            self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "ASR final orphan"),
        ):
            self._run()

        self.assertEqual(
            forged.read_bytes(), b'{"status":"success","forged":true}\n'
        )
        self.assertEqual(self.calls, calls_before)
        self.assertFalse((self.run_root / "receipt.json").exists())

    def test_unmanifested_frame_final_is_preserved_and_blocks(self) -> None:
        self._freeze_failed_contract()
        (self.run_root / "state.json").unlink()
        download = self._create_current_video_download()
        content_root = self.media_root / "C4N4RY"
        stale = (
            datetime.now(timezone.utc)
            - timedelta(seconds=media.STALE_MEDIA_SLOT_SECONDS + 10)
        ).isoformat(timespec="seconds")
        with closing(connect(self.db)) as connection:
            connection.execute(
                "INSERT INTO media_processing_slots("
                "content_id,source_sha256,processor_type,processor_version,status,"
                "attempt_count,created_at,updated_at) VALUES "
                "(1,?,'frames',?,'running',1,?,?)",
                (
                    download.sha256,
                    media.processor_versions()["frames"],
                    stale,
                    stale,
                ),
            )
            connection.commit()
        self._finalize(self.db)
        forged = content_root / "frames" / "frame-000.jpg"
        forged.parent.mkdir()
        forged.write_bytes(b"forged-frame")
        calls_before = dict(self.calls)

        with (
            patch.object(canary, "_process_identity", return_value=None),
            self.assertRaisesRegex(
                canary.LocalAnalysisCanaryError, "缺少完整manifest"
            ),
        ):
            self._run()

        self.assertEqual(forged.read_bytes(), b"forged-frame")
        self.assertEqual(self.calls, calls_before)
        self.assertFalse((self.run_root / "receipt.json").exists())

    def test_self_consistent_frame_manifest_cannot_own_non_image_final(self) -> None:
        self._freeze_failed_contract()
        (self.run_root / "state.json").unlink()
        download = self._create_current_video_download()
        content_root = self.media_root / "C4N4RY"
        stale = (
            datetime.now(timezone.utc)
            - timedelta(seconds=media.STALE_MEDIA_SLOT_SECONDS + 10)
        ).isoformat(timespec="seconds")
        with closing(connect(self.db)) as connection:
            connection.execute(
                "INSERT INTO media_processing_slots("
                "content_id,source_sha256,processor_type,processor_version,status,"
                "attempt_count,created_at,updated_at) VALUES "
                "(1,?,'frames',?,'running',1,?,?)",
                (
                    download.sha256,
                    media.processor_versions()["frames"],
                    stale,
                    stale,
                ),
            )
            connection.commit()
        self._finalize(self.db)
        frames_root = content_root / "frames"
        frames_root.mkdir()
        forged = frames_root / "frame-000.jpg"
        forged.write_bytes(b"not-a-jpeg")
        manifest = frames_root / "frames.json"
        manifest.write_text(
            json.dumps(
                {
                    "status": "success",
                    "duration_seconds": 1,
                    "frames": [
                        {
                            "path": str(forged),
                            "sha256": canary._sha256_file(forged),
                        }
                    ],
                    "contact_sheet": None,
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        manifest_before = manifest.read_bytes()
        calls_before = dict(self.calls)

        with (
            patch.object(canary, "_process_identity", return_value=None),
            self.assertRaisesRegex(
                canary.LocalAnalysisCanaryError, "frame不是有效图片"
            ),
        ):
            self._run()

        self.assertEqual(forged.read_bytes(), b"not-a-jpeg")
        self.assertEqual(manifest.read_bytes(), manifest_before)
        self.assertEqual(self.calls, calls_before)
        self.assertFalse((self.run_root / "receipt.json").exists())

    def test_final_download_orphan_requires_durable_response_provenance(self) -> None:
        contract = self._freeze_failed_contract()
        source = contract["sources"][0]
        sources = {1: source}
        ledger = canary._NetworkLedger(
            self.run_root / "network-ledger.json",
            contract_sha256=canary._sha256_file(self.run_root / "run-contract.json"),
            intent_sha256=canary._sha256_file(self.run_root / "intent.json"),
            content_ids=[1],
            maximum_bytes=int(contract["maximum_download_bytes"]),
            sources=sources,
        )
        body = b"validated-video-body"
        guard = canary.ExactUrlNetworkGuard(
            self.urls,
            media_kind="video",
            maximum_bytes=int(contract["maximum_download_bytes"]),
            budget=canary._DownloadBudget(
                int(contract["maximum_download_bytes"]), ledger=ledger
            ),
            ledger=ledger,
            content_id=1,
        )
        guard._opener = SimpleNamespace(
            open=lambda *_args, **_kwargs: _Response(self.urls[0], body)
        )
        with guard.open(self.urls[0]) as response:
            self.assertEqual(response.read(), body)
        self._insert_running_download(stale=True)
        final = (
            self.media_root
            / "C4N4RY"
            / "downloads"
            / source["artifact_body"]["source_sha256"]
            / "source.mp4"
        )
        final.parent.mkdir(parents=True)
        final.write_bytes(body)

        with (
            patch.object(canary, "_process_identity", return_value=None),
            patch.object(media, "_valid_media", return_value=True) as valid_media,
        ):
            result = self._run()

        self.assertEqual(result["status"], "succeeded")
        valid_media.assert_called_once_with(
            final,
            maximum_duration_seconds=float(
                contract["maximum_video_duration_seconds"]
            ),
        )
        receipt = json.loads((self.run_root / "receipt.json").read_text())
        self.assertEqual(receipt["startup_recovery"]["output_recovered"], 1)
        self.assertFalse(final.exists())
        successful = [
            event
            for event in receipt["processed"][0]["network_transcript"]
            if event["outcome"] == "succeeded"
        ]
        self.assertEqual(len(successful), 2)
        self.assertEqual(successful[0]["response_sha256"], hashlib.sha256(body).hexdigest())

    def test_image_output_recovery_resumes_after_partial_delete(self) -> None:
        image_urls = [
            "https://img1.xhscdn.com/one.jpg",
            "https://img1.xhscdn.com/two.jpg",
        ]
        self._configure_douyin_image_source([[image_urls[0]], [image_urls[1]]])
        contract = self._freeze_failed_contract()
        source = contract["sources"][0]
        slot_source_sha = canary._source_artifact_sha256(source)
        download_binding_sha = canary._source_image_download_binding(source)
        stale = (
            datetime.now(timezone.utc)
            - timedelta(seconds=media.STALE_MEDIA_SLOT_SECONDS + 10)
        ).isoformat(timespec="seconds")
        with closing(connect(self.db)) as connection:
            connection.execute(
                "INSERT INTO media_processing_slots("
                "content_id,source_sha256,processor_type,processor_version,status,"
                "attempt_count,created_at,updated_at) VALUES "
                "(1,?,'download',?,'running',1,?,?)",
                (slot_source_sha, media.IMAGE_DOWNLOAD_VERSION, stale, stale),
            )
            connection.commit()
        self._finalize(self.db)
        paths = canary._paths(
            source_db_path=self.source_db,
            source_completion_path=self.source_completion,
            db_path=self.db,
            media_root=self.media_root,
            run_root=self.run_root,
        )
        intent = json.loads(paths.intent.read_text())
        with patch.object(canary, "_process_identity", return_value=None):
            startup = canary._recover_owned_running_slots(
                paths,
                [1],
                intent_exists=True,
                intent=intent,
                contract=contract,
            )
        canary._finalize_database(self.db)
        ledger = canary._NetworkLedger(
            paths.network_ledger,
            contract_sha256=canary._sha256_file(paths.contract),
            intent_sha256=canary._sha256_file(paths.intent),
            content_ids=[1],
            maximum_bytes=int(contract["maximum_download_bytes"]),
            sources={1: source},
        )
        bodies = [b"\xff\xd8\xff" + b"a" * 600, b"\xff\xd8\xff" + b"b" * 600]
        for url, body in zip(image_urls, bodies, strict=True):
            guard = canary.ExactUrlNetworkGuard(
                [url],
                media_kind="image",
                maximum_bytes=int(contract["maximum_download_bytes"]),
                budget=canary._DownloadBudget(
                    int(contract["maximum_download_bytes"]),
                    consumed_bytes=ledger.budget_consumed_bytes,
                    ledger=ledger,
                ),
                ledger=ledger,
                content_id=1,
            )
            guard._opener = SimpleNamespace(
                open=lambda *_args, body=body, url=url, **_kwargs: _Response(
                    url, body, content_type="image/jpeg"
                )
            )
            with guard.open(url) as response:
                self.assertEqual(response.read(), body)
        images_root = (
            self.media_root
            / "C4N4RY"
            / "downloads"
            / download_binding_sha
            / "images"
        )
        images_root.mkdir(parents=True)
        image_paths = []
        for index, body in enumerate(bodies):
            image_path = images_root / f"image-{index:03d}.bin"
            image_path.write_bytes(body)
            image_paths.append(image_path)
        manifest = images_root / "manifest.json"
        groups = source["image_groups"]
        manifest.write_text(
            json.dumps(
                {
                    "schema_version": media.IMAGE_MANIFEST_VERSION,
                    "status": "complete",
                    "source_url_count": len(image_urls),
                    "source_count": len(groups),
                    "source_sha256": source["artifact_body"]["source_sha256"],
                    "image_groups_sha256": source["image_groups_sha256"],
                    "download_binding_sha256": download_binding_sha,
                    "image_paths": [media._relative(path) for path in image_paths],
                    "frames": [
                        {
                            "path": media._relative(path),
                            "sha256": canary._sha256_file(path),
                        }
                        for path in image_paths
                    ],
                    "groups": [
                        {
                            "group_index": index,
                            "identity": group["identity"],
                            "source_url_sha256s": [
                                candidate["url_sha256"]
                                for candidate in sorted(
                                    group["candidates"],
                                    key=lambda item: item["source_index"],
                                )
                            ],
                            "selected_url_sha256": group["candidates"][0][
                                "url_sha256"
                            ],
                            "selected_response_sha256": canary._sha256_file(
                                image_paths[index]
                            ),
                            "selected_byte_size": image_paths[index].stat().st_size,
                            "image_path": media._relative(image_paths[index]),
                            "attempts": [
                                {
                                    "attempt_index": 0,
                                    "source_index": group["candidates"][0][
                                        "source_index"
                                    ],
                                    "profile": group["candidates"][0]["profile"],
                                    "url_sha256": group["candidates"][0][
                                        "url_sha256"
                                    ],
                                    "outcome": "selected",
                                    "response_sha256": canary._sha256_file(
                                        image_paths[index]
                                    ),
                                    "byte_size": image_paths[index].stat().st_size,
                                    "error": None,
                                }
                            ],
                        }
                        for index, group in enumerate(groups)
                    ],
                },
                sort_keys=True,
            )
            + "\n"
        )
        original_unlink = Path.unlink
        second_quarantine = image_paths[1].with_name(
            f".{image_paths[1].name}.cleanup-"
            f"{canary._sha256_file(image_paths[1])[:16]}.quarantine"
        )

        def crash_after_first_delete(path: Path, *args, **kwargs):
            if path == second_quarantine:
                raise RuntimeError("second image unlink crash")
            return original_unlink(path, *args, **kwargs)

        with (
            patch.object(Path, "unlink", crash_after_first_delete),
            self.assertRaisesRegex(RuntimeError, "second image unlink"),
        ):
            canary._recover_owned_output_partials(
                paths,
                contract=contract,
                content_ids=[1],
                slot_attempt_expectations=startup["slot_attempt_expectations"],
                network_ledger=ledger,
            )
        self.assertFalse(image_paths[0].exists())
        self.assertFalse(image_paths[1].exists())
        self.assertTrue(second_quarantine.exists())
        self.assertTrue(manifest.exists())

        recovered = canary._recover_owned_output_partials(
            paths,
            contract=contract,
            content_ids=[1],
            slot_attempt_expectations=startup["slot_attempt_expectations"],
            network_ledger=ledger,
        )
        self.assertEqual(recovered["output_recovered"], 3)
        self.assertEqual(recovered["output_recovery_rounds"], 1)
        self.assertFalse(image_paths[1].exists())
        self.assertFalse(second_quarantine.exists())
        self.assertFalse(manifest.exists())

    def test_running_slot_live_or_nonowned_is_preserved_and_blocks(self) -> None:
        self._freeze_failed_contract()
        self._insert_running_download(stale=False)
        with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "仍存活"):
            self._run()
        with closing(canary._immutable_connection(self.db)) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM media_processing_slots"
                ).fetchone()[0],
                "running",
            )

    def test_same_contract_wal_is_finalized_but_unknown_sidecar_is_rejected(self) -> None:
        self._freeze_failed_contract()
        (self.run_root / "state.json").unlink()
        script = """
import os, sqlite3, sys
connection=sqlite3.connect(sys.argv[1])
connection.execute('PRAGMA journal_mode=WAL')
connection.execute('PRAGMA wal_autocheckpoint=0')
connection.execute("UPDATE content_items SET evaluation_content_direction=evaluation_content_direction WHERE id=1")
connection.commit()
os._exit(0)
"""
        subprocess.run([sys.executable, "-c", script, str(self.db)], check=True)
        self.assertTrue(Path(f"{self.db}-wal").exists())
        result = self._run()
        self.assertEqual(result["status"], "succeeded")
        self.assertFalse(Path(f"{self.db}-wal").exists())
        self.assertFalse(Path(f"{self.db}-shm").exists())

        other_root = self.root / "unknown"
        other_root.mkdir()
        unknown_db = other_root / "work.sqlite3"
        Path(f"{unknown_db}-wal").write_bytes(b"unknown")
        with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "未知sidecar"):
            self._run(
                db_path=unknown_db,
                media_root=other_root / "media",
                run_root=other_root / "run",
            )
        self.assertFalse((other_root / "run").exists())

    def test_output_orphan_cannot_be_signed(self) -> None:
        def orphan(content_id: int, **kwargs):
            result = self._fake_media(content_id, **kwargs)
            (self.media_root / "orphan.bin").write_bytes(b"orphan")
            return result

        with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "精确可达集合"):
            self._run(media_side_effect=orphan)
        self.assertFalse((self.run_root / "receipt.json").exists())

    def test_budget_row_mutation_cannot_be_signed(self) -> None:
        def budget(content_id: int, **kwargs):
            result = self._fake_media(content_id, **kwargs)
            with closing(connect(self.db)) as connection:
                connection.execute(
                    "UPDATE provider_budget_batches SET consumed_amount=9 WHERE id='fixture-budget'"
                )
                connection.commit()
            return result

        with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "保护表"):
            self._run(media_side_effect=budget)

    def test_evidence_level_and_summary_are_recomputed_from_current_artifacts(self) -> None:
        def forge_level(content_id: int, *, db_path: Path):
            result = self._fake_fingerprint(content_id, db_path=db_path)
            with closing(connect(db_path)) as connection:
                row = connection.execute(
                    "SELECT id,payload_json FROM evaluation_versions "
                    "WHERE content_id=? AND invalidated_at IS NULL",
                    (content_id,),
                ).fetchone()
                payload = json.loads(row["payload_json"])
                payload["evidence_level"] = "V2"
                payload["evidence_summary"] = "forged summary"
                connection.execute(
                    "UPDATE evaluation_versions SET evidence_level='V2',payload_json=? "
                    "WHERE id=?",
                    (
                        json.dumps(
                            payload,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        int(row["id"]),
                    ),
                )
                connection.commit()
            return result

        with self.assertRaisesRegex(
            canary.LocalAnalysisCanaryError, "current evidence精确状态"
        ):
            self._run(fingerprint_side_effect=forge_level)

    def test_extra_evaluation_match_cannot_be_signed(self) -> None:
        def add_extra_match(content_id: int, *, db_path: Path):
            result = self._fake_fingerprint(content_id, db_path=db_path)
            with closing(connect(db_path)) as connection:
                evaluation_id = int(
                    connection.execute(
                        "SELECT id FROM evaluation_versions "
                        "WHERE content_id=? AND invalidated_at IS NULL",
                        (content_id,),
                    ).fetchone()[0]
                )
                connection.execute(
                    "INSERT INTO evaluation_matches("
                    "evaluation_id,selling_point_code,scene,match_role,score,"
                    "evidence_json) VALUES (?,?,?,?,?,?)",
                    (evaluation_id, "forged", "media", "secondary", 1, "{}"),
                )
                connection.commit()
            return result

        with self.assertRaisesRegex(
            canary.LocalAnalysisCanaryError, "matches数量"
        ):
            self._run(fingerprint_side_effect=add_extra_match)

    def test_fingerprint_projection_column_mutation_cannot_be_signed(self) -> None:
        def mutate_fingerprint(content_id: int, *, db_path: Path):
            result = self._fake_fingerprint(content_id, db_path=db_path)
            with closing(connect(db_path)) as connection:
                connection.execute(
                    "UPDATE duplicate_fingerprints SET text_char_count=1 "
                    "WHERE content_id=?",
                    (content_id,),
                )
                connection.commit()
            return result

        with self.assertRaisesRegex(
            canary.LocalAnalysisCanaryError, "fingerprint正文/DB语义"
        ):
            self._run(fingerprint_side_effect=mutate_fingerprint)

    def test_progress_result_must_project_exact_fingerprint_source(self) -> None:
        def forged_result(content_id: int, *, db_path: Path):
            result = dict(self._fake_fingerprint(content_id, db_path=db_path))
            result["source_sha256"] = "0" * 64
            return result

        with self.assertRaisesRegex(
            canary.LocalAnalysisCanaryError, "progress fingerprint"
        ):
            self._run(fingerprint_side_effect=forged_result)

    def test_image_manifest_must_complete_every_frozen_logical_group(self) -> None:
        image_root = self.analysis_root / "image-fixture"
        image_root.mkdir()
        preview = (
            "https://sns-i11.rednotecdn.com/notes_pre_post/example?"
            "imageView2/2/w/576/format/webp/q/87%7CimageMogr2/strip&"
            "redImage/frame/0&ap=12&sc=USR_PRV&sign=abc&t=123&src=A&origin=0"
        )
        detail = (
            "https://sns-i11.rednotecdn.com/notes_pre_post/example?"
            "imageView2/2/w/1440/format/webp&ap=12&sc=USR_DTL&"
            "sign=abc&t=123&src=A&origin=0"
        )
        second = "https://sns-i11.rednotecdn.com/notes_pre_post/second.jpg"
        urls = [preview, detail, second]
        groups = media.image_source_groups(urls, platform="xiaohongshu")
        source_sha256 = media._media_source_identity("image", urls)[1]
        image_groups_sha256 = media.image_groups_sha256(groups)
        source = {
            "content": {"platform": "xiaohongshu"},
            "artifact_body": {
                "media_kind": "image",
                "source_sha256": source_sha256,
            },
            "download_urls": urls,
            "download_urls_sha256": canary._json_sha256(urls),
            "image_groups": groups,
            "image_groups_sha256": image_groups_sha256,
        }
        wrong_platform_source = json.loads(json.dumps(source))
        wrong_platform_source["content"]["platform"] = "douyin"
        with self.assertRaisesRegex(
            canary.LocalAnalysisCanaryError, "冻结逻辑图组"
        ):
            canary._source_image_groups(wrong_platform_source)
        image_paths = [image_root / f"image-{index:03d}.bin" for index in range(2)]
        for index, image_path in enumerate(image_paths):
            image_path.write_bytes(b"\xff\xd8\xff" + bytes([65 + index]) * 600)
        manifest = image_root / "manifest.json"
        selected_candidates = [group["candidates"][0] for group in groups]
        body = {
            "schema_version": media.IMAGE_MANIFEST_VERSION,
            "status": "complete",
            "source_url_count": len(urls),
            "source_count": len(groups),
            "source_sha256": source_sha256,
            "image_groups_sha256": image_groups_sha256,
            "download_binding_sha256": media.image_download_binding_sha256(
                source_sha256, image_groups_sha256
            ),
            "image_paths": [media._relative(path) for path in image_paths],
            "frames": [
                {
                    "path": media._relative(path),
                    "sha256": canary._sha256_file(path),
                }
                for path in image_paths
            ],
            "groups": [
                {
                    "group_index": index,
                    "identity": group["identity"],
                    "source_url_sha256s": [
                        candidate["url_sha256"]
                        for candidate in sorted(
                            group["candidates"],
                            key=lambda item: item["source_index"],
                        )
                    ],
                    "selected_url_sha256": selected["url_sha256"],
                    "selected_response_sha256": canary._sha256_file(
                        image_paths[index]
                    ),
                    "selected_byte_size": image_paths[index].stat().st_size,
                    "image_path": media._relative(image_paths[index]),
                    "attempts": [
                        {
                            "attempt_index": 0,
                            "source_index": selected["source_index"],
                            "profile": selected["profile"],
                            "url_sha256": selected["url_sha256"],
                            "outcome": "selected",
                            "response_sha256": canary._sha256_file(
                                image_paths[index]
                            ),
                            "byte_size": image_paths[index].stat().st_size,
                            "error": None,
                        }
                    ],
                }
                for index, (group, selected) in enumerate(
                    zip(groups, selected_candidates, strict=True)
                )
            ],
        }
        manifest.write_text(json.dumps(body) + "\n")
        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("CREATE TABLE artifact(local_path TEXT)")
            connection.execute("INSERT INTO artifact VALUES (?)", (str(manifest),))
            row = connection.execute("SELECT * FROM artifact").fetchone()
            partial = dict(body)
            partial["groups"] = body["groups"][:1]
            partial["frames"] = body["frames"][:1]
            partial["image_paths"] = body["image_paths"][:1]
            manifest.write_text(json.dumps(partial) + "\n")
            with self.assertRaisesRegex(
                canary.LocalAnalysisCanaryError, "形状或计数"
            ):
                canary._manifest_output_paths(
                    row,
                    media_kind="image",
                    media_root=image_root,
                    source=source,
                )
            manifest.write_text(json.dumps(body) + "\n")
            paths = canary._manifest_output_paths(
                row,
                media_kind="image",
                media_root=image_root,
                source=source,
            )
            self.assertEqual(paths, {manifest, *image_paths})

            fallback = json.loads(json.dumps(body))
            first_group = fallback["groups"][0]
            detail_candidate, preview_candidate = groups[0]["candidates"]
            partial_response = b"partial-response-before-read-error"
            partial_response_sha256 = hashlib.sha256(partial_response).hexdigest()
            first_group["attempts"] = [
                {
                    "attempt_index": 0,
                    "source_index": detail_candidate["source_index"],
                    "profile": detail_candidate["profile"],
                    "url_sha256": detail_candidate["url_sha256"],
                    "outcome": "request_failed",
                    "response_sha256": partial_response_sha256,
                    "byte_size": len(partial_response),
                    "error": "OSError",
                },
                {
                    "attempt_index": 1,
                    "source_index": preview_candidate["source_index"],
                    "profile": preview_candidate["profile"],
                    "url_sha256": preview_candidate["url_sha256"],
                    "outcome": "selected",
                    "response_sha256": canary._sha256_file(image_paths[0]),
                    "byte_size": image_paths[0].stat().st_size,
                    "error": None,
                },
            ]
            first_group["selected_url_sha256"] = preview_candidate["url_sha256"]
            manifest.write_text(json.dumps(fallback) + "\n")
            canary._manifest_output_paths(
                row,
                media_kind="image",
                media_root=image_root,
                source=source,
            )

            first_sha = canary._sha256_file(image_paths[0])
            second_sha = canary._sha256_file(image_paths[1])
            events = [
                {
                    "url_sha256": detail_candidate["url_sha256"],
                    "outcome": "failed",
                    "response_sha256": partial_response_sha256,
                    "bytes": len(partial_response),
                    "error": "OSError: failed after partial response read",
                },
                {
                    "url_sha256": preview_candidate["url_sha256"],
                    "outcome": "succeeded",
                    "response_sha256": first_sha,
                    "bytes": image_paths[0].stat().st_size,
                    "error": None,
                },
                {
                    "url_sha256": groups[1]["candidates"][0]["url_sha256"],
                    "outcome": "succeeded",
                    "response_sha256": second_sha,
                    "bytes": image_paths[1].stat().st_size,
                    "error": None,
                },
            ]
            ledger = SimpleNamespace(transcript=lambda _content_id: list(events))
            canary._validate_download_provenance(
                content_id=1,
                source=source,
                artifacts={"media_manifest": row},
                ledger=ledger,
            )
            extra_events = [dict(event) for event in events]
            extra_events.insert(2, dict(events[1]))
            with self.assertRaisesRegex(
                canary.LocalAnalysisCanaryError, "不是一一双射"
            ):
                canary._validate_download_provenance(
                    content_id=1,
                    source=source,
                    artifacts={"media_manifest": row},
                    ledger=SimpleNamespace(
                        transcript=lambda _content_id: list(extra_events)
                    ),
                )
            forged_events = [dict(event) for event in events]
            forged_events[0]["response_sha256"] = "0" * 64
            with self.assertRaisesRegex(
                canary.LocalAnalysisCanaryError, "顺序或响应投影"
            ):
                canary._validate_download_provenance(
                    content_id=1,
                    source=source,
                    artifacts={"media_manifest": row},
                    ledger=SimpleNamespace(
                        transcript=lambda _content_id: list(forged_events)
                    ),
                )
            forged_error_events = [dict(event) for event in events]
            forged_error_events[0]["error"] = (
                "TimeoutError: failed after partial response read"
            )
            with self.assertRaisesRegex(
                canary.LocalAnalysisCanaryError, "顺序或响应投影"
            ):
                canary._validate_download_provenance(
                    content_id=1,
                    source=source,
                    artifacts={"media_manifest": row},
                    ledger=SimpleNamespace(
                        transcript=lambda _content_id: list(
                            forged_error_events
                        )
                    ),
                )
        finally:
            connection.close()

    def test_step4_outputs_cannot_nest_under_step3_evidence_tree(self) -> None:
        forbidden_media = self.source_root / "step4-child"
        before = self._tree_state(self.step3_root)
        with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "Step3源证据树"):
            self._plan(media_root=forbidden_media)
        self.assertEqual(self._tree_state(self.step3_root), before)
        self.assertFalse(forbidden_media.exists())

    @unittest.skipUnless(hasattr(canary.fcntl, "F_GETPATH"), "Darwin path identity")
    def test_case_insensitive_path_aliases_cannot_bypass_roots_or_claims(self) -> None:
        canonical_child = Path(str(media.MEDIA_ROOT).swapcase()) / "step4-child"
        before = self._tree_state(self.root)
        with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "canonical"):
            self._plan(
                media_root=canonical_child,
                run_root=self.analysis_root / "case-canonical-run",
            )
        self.assertEqual(self._tree_state(self.root), before)

        step3_child = Path(str(self.source_root).swapcase()) / "step4-child"
        with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "Step3源证据树"):
            self._plan(
                media_root=step3_child,
                run_root=self.analysis_root / "case-step3-run",
            )
        self.assertEqual(self._tree_state(self.root), before)

        mixed_media = self.analysis_root / "FutureRoot"
        mixed_run = self.analysis_root / "futureroot" / "nested"
        with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "相同或相互包含"):
            self._plan(media_root=mixed_media, run_root=mixed_run)
        self.assertEqual(
            canary._claim_path(mixed_media, label="media"),
            canary._claim_path(
                self.analysis_root / "futureroot", label="media"
            ),
        )
        composed = self.analysis_root / "ÉRoot"
        decomposed = self.analysis_root / "E\u0301Root" / "nested"
        with self.assertRaisesRegex(
            canary.LocalAnalysisCanaryError, "相同或相互包含"
        ):
            self._plan(media_root=composed, run_root=decomposed)
        self.assertEqual(
            canary._claim_path(composed, label="media"),
            canary._claim_path(
                self.analysis_root / "E\u0301Root", label="media"
            ),
        )
        self.assertEqual(self._tree_state(self.root), before)

    def test_code_or_config_drift_blocks_resume(self) -> None:
        self._freeze_failed_contract()
        current = canary._code_snapshot()
        drifted = [*current, {"path": "drift", "sha256": "0" * 64, "byte_size": 1}]
        with patch.object(canary, "_code_snapshot", return_value=drifted):
            with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "代码SHA漂移"):
                self._run()

    def test_whisper_inventory_binds_symlink_target_and_blob_bytes(self) -> None:
        repo = self.root / "hf" / "models--fixture--whisper"
        blobs = repo / "blobs"
        snapshot = repo / "snapshots" / "revision"
        blobs.mkdir(parents=True)
        snapshot.mkdir(parents=True)
        blob = blobs / "abc123"
        blob.write_bytes(b"weights")
        link = snapshot / "weights.safetensors"
        link.symlink_to(Path("../../blobs/abc123"))

        first = canary._whisper_model_inventory(snapshot)

        self.assertEqual(first["files"], 1)
        self.assertEqual(first["rows"][0]["target"], "../../blobs/abc123")
        blob.write_bytes(b"drifted")
        second = canary._whisper_model_inventory(snapshot)
        self.assertNotEqual(first["rows_sha256"], second["rows_sha256"])

    def test_formal_canonical_and_claim_aliases_are_rejected_before_analysis(self) -> None:
        formal_run = self.root / "formal-run"
        with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "正式数据库"):
            canary.run_canary(
                **self._arguments(
                    db_path=canary.storage_module.DEFAULT_DB,
                    media_root=self.root / "formal-media",
                    run_root=formal_run,
                )
            )
        self.assertFalse(formal_run.exists())
        with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "canonical"):
            self._run(media_root=media.MEDIA_ROOT, run_root=self.root / "canonical-run")
        paths = canary._paths(
            source_db_path=self.source_db,
            source_completion_path=self.source_completion,
            db_path=self.db,
            media_root=self.media_root,
            run_root=self.run_root,
        )
        held = canary._claim_path(paths.media_root, label="media")
        with canary._exclusive_claim(held):
            with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "占用"):
                self._run()
        calls = dict(self.calls)
        with canary._exclusive_claim(canary._global_claim_path()):
            with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "占用"):
                self._run(
                    db_path=self.analysis_root / "other.sqlite3",
                    media_root=self.analysis_root / "other-media",
                    run_root=self.analysis_root / "other-run",
                )
        self.assertEqual(self.calls, calls)

    def test_existing_formal_database_alias_is_rejected_by_file_identity(self) -> None:
        formal = self.root / "formal.sqlite3"
        alias = self.root / "apfs-firmlink-spelling.sqlite3"
        formal.write_bytes(b"formal-sentinel")
        alias.hardlink_to(formal)
        paths = canary._paths(
            source_db_path=self.source_db,
            source_completion_path=self.source_completion,
            db_path=alias,
            media_root=self.media_root,
            run_root=self.run_root,
        )

        def allow_identity_alias(path: Path, *, label: str):
            del label
            return path.lstat()

        with (
            patch.object(canary.storage_module, "DEFAULT_DB", formal),
            patch.object(
                canary,
                "_private_file",
                side_effect=allow_identity_alias,
            ),
            self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "正式数据库"),
        ):
            canary._validate_paths(paths, work_database_must_exist=True)

    def test_cli_defaults_to_plan_and_requires_explicit_apply(self) -> None:
        help_text = canary._parser().format_help()
        for option in (
            "--source-db",
            "--source-completion",
            "--expected-source-db-sha256",
            "--expected-source-completion-sha256",
            "--content-id",
            "--apply",
        ):
            self.assertIn(option, help_text)
        arguments = [
            "--source-db",
            str(self.source_db),
            "--source-completion",
            str(self.source_completion),
            "--expected-source-db-sha256",
            self.source_db_sha,
            "--expected-source-completion-sha256",
            self.source_completion_sha,
            "--db",
            str(self.db),
            "--media-root",
            str(self.media_root),
            "--run-root",
            str(self.run_root),
            "--content-id",
            "1",
        ]
        with patch.object(canary, "plan_canary", return_value={"status": "planned"}) as plan:
            self.assertEqual(canary.main(arguments), 0)
            plan.assert_called_once()
        with (
            patch.object(canary, "run_canary", return_value={"status": "succeeded"}) as apply,
            patch.object(canary, "plan_canary") as plan,
        ):
            self.assertEqual(canary.main([*arguments, "--apply"]), 0)
            apply.assert_called_once()
            plan.assert_not_called()
        repository = Path(canary.__file__).resolve().parents[1]
        environment = dict(os.environ)
        environment.update(
            {
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYTHONPATH": str(repository / "src/dcar_eval"),
                "DCAR_TEST_DENY_FORMAL_DB": "1",
            }
        )
        completed = subprocess.run(
            [sys.executable, str(Path(canary.__file__).resolve()), "--help"],
            cwd=self.root,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("--apply", completed.stdout)


if __name__ == "__main__":
    unittest.main()
