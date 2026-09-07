from __future__ import annotations

import hashlib
import io
import json
import os
import subprocess
import threading
import unittest
from contextlib import contextmanager
from contextvars import Context
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from PIL import Image

from scripts import run_august_range_backfill as runner
from tests import test_run_august_range_backfill as fixture_module
from v8 import (
    capture,
    duplicates,
    evaluation,
    media,
    provider_budget,
    providers,
    range_backfill as rb,
    raw_evidence,
)
from v8.media_state import MediaTerminalDetail
from v8.operations import upsert_content
from v8.storage import connect, now_utc


OLD_IMAGE = "https://sns-i11.rednotecdn.com/campaign-old.jpg"
NEW_IMAGE = "https://sns-i11.rednotecdn.com/campaign-new.jpg"
OLD_VIDEO = "https://cdn.example/audio-only.mp4"
NEW_VIDEO = "https://cdn.example/web-video.mp4"
OCR_TEXT = "懂车帝查二手车价格并对比同款车型，了解透明车况和真实报价。"


class _Response(io.BytesIO):
    def __init__(self, body: bytes, url: str, content_type: str) -> None:
        super().__init__(body)
        self.url = url
        self.headers = {"Content-Length": str(len(body)), "Content-Type": content_type}

    def geturl(self) -> str:
        return self.url


class AugustRangeBackfillMediaTest(unittest.TestCase):
    """Offline campaign boundaries; no production DB, credentials or real HTTP."""

    def setUp(self) -> None:
        # Compose the shared fixture: inheriting would rerun its entire test suite.
        self.fixture = fixture_module.AugustRangeBackfillTest()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()
        self.db = self.fixture.db.resolve()
        self.root = self.fixture.root.resolve()
        self.fixture.patches.enter_context(
            patch.dict("os.environ", {"DCAR_TEST_DENY_FORMAL_DB": "1"})
        )
        for target in ("socket.getaddrinfo", "socket.create_connection", "socket.socket.connect", "socket.socket.connect_ex"):
            self.fixture.patches.enter_context(
                patch(target, side_effect=AssertionError("real network forbidden"))
            )

    def _detail(self, content, urls, *, image_groups=None):
        base = self.fixture.result("detail", content)
        data = {**base.data, "media_urls": list(urls)}
        raw = {"stage": "detail", "data": data}
        if image_groups is not None:
            raw["aweme_detail"] = {
                "aweme_id": str(content["platform_content_id"]),
                "desc": data["body"],
                "images": [{"url_list": list(group)} for group in image_groups],
            }
        return capture.ProviderResult(data, raw, 200, True)

    def _seed_detail(self, cid, urls, *, image_groups=None):
        content = self.fixture.row(cid)
        provider, adapter, operation, price = providers.STAGE_CONFIG[(content["platform"], "detail")]
        budget = providers.ensure_task_budget(
            provider=provider, operation=operation, price=price,
            task_id=runner.TASK_ID, task_max_amount=runner.MAX_AMOUNT, db_path=self.db,
        )
        # Seed ordinary detail evidence; the campaign under test may consume it
        # locally, but this fixture is not itself a historical repair purchase.
        with provider_budget.paid_scope("detail"):
            outcome = capture.execute_content_fetch(
                content_id=cid, stage="detail", window_key="lifetime",
                provider=provider, adapter_version=adapter, operation=operation,
                call=lambda: self._detail(content, urls, image_groups=image_groups),
                db_path=self.db, budget_id=budget, task_id=runner.TASK_ID,
                task_max_amount=runner.MAX_AMOUNT,
            )
        providers._store_stage_result(content, "detail", "lifetime", outcome, db_path=self.db)
        return outcome

    @contextmanager
    def _ordinary_refresh(self):
        """Run the real refresh capture as an ordinary fixture detail call.

        Campaign orchestration keeps its production ``history`` scope.  Media
        algorithm tests use a fresh Context only at the provider capture seam,
        so repair=0 is not treated as historical authorization while the real
        slot, ledger, retry and raw-evidence behavior remains under test.
        """

        original = runner.capture.execute_content_fetch
        with patch.object(
            runner.capture,
            "execute_content_fetch",
            side_effect=lambda *args, **kwargs: Context().run(
                original, *args, **kwargs
            ),
        ):
            yield

    def _seed_calibration(self) -> None:
        # This is a fixture for the already-approved calibration gate, not a
        # claim that this test recalibrates the detector or validates production.
        with connect(self.db) as connection:
            connection.execute(
                """INSERT INTO duplicate_calibration_runs(
                    id,calibration_version,fingerprint_version,dataset_sha256,
                    pair_count,positive_count,negative_count,predicted_positive_count,
                    true_positive_count,false_positive_count,precision,recall,
                    thresholds_json,status,created_at
                ) VALUES ('campaign-fixture','fixture-only',?,?,2,1,1,1,1,0,1,1,?,'passed',?)""",
                (duplicates.FINGERPRINT_VERSION, "f" * 64,
                 duplicates._canonical_json(duplicates.THRESHOLDS), now_utc()),
            )
            connection.commit()

    def _rows(self, sql, values=()):
        with connect(self.db) as connection:
            return [dict(row) for row in connection.execute(sql, values)]

    def _source(self, cid):
        source = media.get_media_source_state(cid, db_path=self.db)
        self.assertIsNotNone(source)
        return source

    def test_campaign_raw_reader_supports_legacy_and_compressed_evidence(self) -> None:
        campaign = self.fixture.campaign()
        content_id = self.fixture.content()
        outcome = self._seed_detail(content_id, [OLD_IMAGE])
        with connect(self.db) as connection:
            row = connection.execute(
                "SELECT local_path,sha256,byte_size "
                "FROM provider_raw_responses WHERE id=?",
                (outcome.raw_response_id,),
            ).fetchone()
        assert row is not None
        compressed_path = Path(str(row["local_path"]))
        self.assertTrue(compressed_path.name.endswith(".json.zst"))
        expected = campaign._raw(outcome.raw_response_id)
        loaded = raw_evidence.read_raw_evidence(
            compressed_path,
            expected_stored_sha256=str(row["sha256"]),
            expected_stored_size=int(row["byte_size"]),
        )

        legacy_path = self.root / "legacy-provider-response.json"
        legacy_path.write_bytes(loaded.entity_bytes)
        legacy_path.chmod(0o600)
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE provider_raw_responses "
                "SET local_path=?,sha256=?,byte_size=? WHERE id=?",
                (
                    str(legacy_path),
                    hashlib.sha256(loaded.entity_bytes).hexdigest(),
                    len(loaded.entity_bytes),
                    outcome.raw_response_id,
                ),
            )
            connection.commit()
        self.assertEqual(campaign._raw(outcome.raw_response_id), expected)

    @staticmethod
    def _fp_result(cid):
        return {
            "failed": 0, "has_more": False, "calibration_ready": True,
            "fingerprinted_content_ids": [cid], "relations": {},
        }

    def test_existing_video_rebinds_cached_artifact_without_download_or_paid_refresh(self) -> None:
        cid = self.fixture.content()
        url = "https://cdn.example/campaign-cached.mp4"
        media.store_media_source_manifest(
            cid, media_kind="video", urls=[url], raw_response_id=101, db_path=self.db,
        )
        video = self.root / "tiny-video.mp4"
        subprocess.run(
            ["ffmpeg", "-y", "-nostdin", "-loglevel", "error", "-f", "lavfi",
             "-i", "color=c=black:s=32x24:d=0.2", "-c:v", "libx264", str(video)],
            check=True, timeout=30,
        )
        video_bytes = video.read_bytes()
        cached = media.download_video_sources(
            cid, [url], db_path=self.db,
            urlopen_fn=lambda *_args, **_kwargs: _Response(video_bytes, url, "video/mp4"),
            _slot_source_sha256=media._media_source_identity("video", [url])[1],
        )
        self.assertIsNone(self._source(cid)["download_slot"])
        before_artifacts = self._rows("SELECT * FROM evidence_artifacts WHERE artifact_type='media'")
        paid = Mock(side_effect=AssertionError("existing video was repurchased"))
        supplied_http = Mock(side_effect=AssertionError("existing video was redownloaded"))
        campaign = self.fixture.campaign()
        with campaign.window(), patch.object(media, "_download_video") as download, patch.object(
            media, "process_content_media", wraps=media.process_content_media
        ) as process:
            first = campaign.download_one(cid, allow_refresh=True, call_override=paid, urlopen_fn=supplied_http)
            second = campaign.download_one(cid, allow_refresh=True, call_override=paid, urlopen_fn=supplied_http)
        self.assertEqual(first["artifact_id"], cached.id)
        self.assertEqual(second["artifact_id"], cached.id)
        self.assertTrue(all(call.kwargs["urlopen_fn"] is runner.deny_network for call in process.call_args_list))
        download.assert_not_called()
        paid.assert_not_called()
        supplied_http.assert_not_called()
        self.assertEqual(self.fixture.usage()[0], 0)
        self.assertEqual(self._rows("SELECT * FROM evidence_artifacts WHERE artifact_type='media'"), before_artifacts)
        slot = self._source(cid)["download_slot"]
        self.assertEqual((slot["status"], slot["attempt_count"], slot["output_artifact_id"]), ("succeeded", 1, cached.id))

    def test_existing_pending_video_missing_cache_redownloads_without_provider_refresh(
        self,
    ) -> None:
        cid = self.fixture.content()
        campaign = self.fixture.campaign()
        url = "https://cdn.example/existing-pending.mp4"
        video = self.root / "existing-pending.mp4"
        subprocess.run(
            [
                "ffmpeg", "-y", "-nostdin", "-loglevel", "error",
                "-f", "lavfi", "-i", "color=c=black:s=32x24:d=0.2",
                "-c:v", "libx264", str(video),
            ],
            check=True,
            timeout=30,
        )
        response = Mock(
            side_effect=lambda *_args, **_kwargs: _Response(
                video.read_bytes(), url, "video/mp4"
            )
        )
        paid = Mock(side_effect=AssertionError("existing video paid fallback"))
        with campaign.window():
            self._seed_detail(cid, [url])
            usage_before = self.fixture.usage()
            result = campaign.download_one(
                cid,
                allow_refresh=True,
                call_override=paid,
                urlopen_fn=response,
            )
        self.assertEqual(result["status"], "downloaded")
        response.assert_called()
        paid.assert_not_called()
        self.assertEqual(self.fixture.usage(), usage_before)

    def test_existing_history_video_can_download_real_media_without_cache(self) -> None:
        cid = self.fixture.content()
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET source_group='history-backfill' WHERE id=?",
                (cid,),
            )
            connection.commit()
        campaign = self.fixture.campaign()
        url = "https://cdn.example/existing-history.mp4"
        video = self.root / "existing-history.mp4"
        subprocess.run(
            [
                "ffmpeg", "-y", "-nostdin", "-loglevel", "error",
                "-f", "lavfi", "-i", "color=c=black:s=32x24:d=0.2",
                "-c:v", "libx264", str(video),
            ],
            check=True,
            timeout=30,
        )
        response = Mock(
            side_effect=lambda *_args, **_kwargs: _Response(
                video.read_bytes(), url, "video/mp4"
            )
        )
        with campaign.window():
            self.assertIn(cid, campaign.state["existing_pending_ids"])
            self._seed_detail(cid, [url])
            result = campaign.download_one(cid, urlopen_fn=response)
        self.assertEqual(result["status"], "downloaded")
        response.assert_called()
        slot = self._source(cid)["download_slot"]
        self.assertEqual(slot["status"], "succeeded")

    def test_terminal_download_repair_resets_only_two_exact_current_errors(self) -> None:
        campaign = self.fixture.campaign()
        cases = [
            (
                "douyin",
                "https://cdn.example/exact-video.mp4",
                media.VIDEO_DOWNLOAD_VERSION,
                "MediaProcessingError: media download failed: "
                "candidate 0 was not a playable video | "
                "candidate 1 was not a playable video",
                True,
            ),
            (
                "xiaohongshu",
                "https://sns-i11.rednotecdn.com/exact-image.jpg",
                media.IMAGE_DOWNLOAD_VERSION,
                "MediaProcessingError: image download incomplete: "
                "logical image group 0 exhausted",
                True,
            ),
            (
                "douyin",
                "https://cdn.example/generic-video.mp4",
                media.VIDEO_DOWNLOAD_VERSION,
                "MediaProcessingError: provider media URL exhausted",
                False,
            ),
            (
                "douyin",
                "https://cdn.example/private-source.mp4",
                media.VIDEO_DOWNLOAD_VERSION,
                "MediaProcessingError: video media source must be a private regular file",
                False,
            ),
        ]
        expected: list[int] = []
        rejected: list[int] = []
        with campaign.window():
            for platform, url, processor_version, error, allowed in cases:
                cid = self.fixture.content(platform, campaign=campaign)
                self._seed_detail(cid, [url])
                source = self._rows(
                    "SELECT sha256 FROM evidence_artifacts WHERE content_id=? "
                    "AND artifact_type='media_source' AND status='available' "
                    "ORDER BY id DESC LIMIT 1",
                    (cid,),
                )[0]
                with connect(self.db) as connection:
                    connection.execute(
                        """
                        INSERT INTO media_processing_slots(
                          content_id,source_sha256,processor_type,
                          processor_version,status,output_artifact_id,
                          attempt_count,error_message,created_at,updated_at
                        ) VALUES (?,?,'download',?,'terminal_failed',NULL,3,?,?,?)
                        """,
                        (
                            cid,
                            source["sha256"],
                            processor_version,
                            error,
                            now_utc(),
                            now_utc(),
                        ),
                    )
                    connection.commit()
                (expected if allowed else rejected).append(cid)
            result = campaign.repair_terminal_downloads()
            repeated = campaign.repair_terminal_downloads()

        self.assertEqual(result["checked"], 2)
        self.assertEqual(result["slots_reset"], 2)
        self.assertEqual(result["content_ids"], expected)
        self.assertEqual(repeated, result)
        for cid in expected:
            slot = self._rows(
                "SELECT status,attempt_count,error_message FROM "
                "media_processing_slots WHERE content_id=?",
                (cid,),
            )[0]
            self.assertEqual(
                (slot["status"], slot["attempt_count"], slot["error_message"]),
                (
                    "retryable_failed",
                    0,
                    runner.TERMINAL_DOWNLOAD_REPAIR_MESSAGE,
                ),
            )
            self.assertEqual(
                campaign.state["results"]["download"][str(cid)]["status"],
                "repair_ready",
            )
        for cid in rejected:
            slot = self._rows(
                "SELECT status,attempt_count,error_message FROM "
                "media_processing_slots WHERE content_id=?",
                (cid,),
            )[0]
            self.assertEqual((slot["status"], slot["attempt_count"]), ("terminal_failed", 3))

    def test_campaign_repairs_media_source_hardlink_and_refunds_terminal_slot(self) -> None:
        url = "https://cdn.example/campaign-repaired.mp4"
        video = self.root / "repair-video.mp4"
        subprocess.run(
            ["ffmpeg", "-y", "-nostdin", "-loglevel", "error", "-f", "lavfi",
             "-i", "color=c=black:s=32x24:d=0.2", "-c:v", "libx264", str(video)],
            check=True, timeout=30,
        )
        video_bytes = video.read_bytes()
        campaign = self.fixture.campaign()
        with campaign.window():
            unready = self.fixture.content(campaign=campaign)
            content_id = self.fixture.content(campaign=campaign)
            self._seed_detail(content_id, [url])
            source = self._rows(
                "SELECT * FROM evidence_artifacts WHERE content_id=? "
                "AND artifact_type='media_source' ORDER BY id DESC LIMIT 1",
                (content_id,),
            )[0]
            source_path = media._resolved(source["local_path"])
            original_inode = source_path.stat().st_ino
            rollback_copy = self.root / "rollback-media-source.json"
            os.link(source_path, rollback_copy)
            self.assertEqual(source_path.stat().st_nlink, 2)
            with connect(self.db) as connection:
                connection.execute(
                    """
                    INSERT INTO media_processing_slots(
                      content_id,source_sha256,processor_type,processor_version,
                      status,output_artifact_id,attempt_count,error_message,created_at,updated_at
                    ) VALUES (?,?,'download',?,'terminal_failed',NULL,3,?,?,?)
                    """,
                    (
                        content_id,
                        source["sha256"],
                        media.VIDEO_DOWNLOAD_VERSION,
                        "MediaProcessingError: video media source must be a private regular file",
                        now_utc(),
                        now_utc(),
                    ),
                )
                connection.commit()
            campaign.state["results"].setdefault("download", {})[str(content_id)] = {
                "content_id": content_id,
                "status": "failed",
                "error_code": "TerminalMediaSlotError",
            }
            campaign._save()
            usage_before = self.fixture.usage()
            repaired = campaign.repair_media_source_links()
            self.assertEqual(
                (repaired["checked"], repaired["dealiased"], repaired["slots_reset"]),
                (1, 1, 1),
            )
            self.assertNotEqual(source_path.stat().st_ino, original_inode)
            self.assertEqual(source_path.stat().st_nlink, 1)
            self.assertEqual(rollback_copy.stat().st_ino, original_inode)
            self.assertEqual(rollback_copy.stat().st_nlink, 1)
            reopened = self._rows(
                "SELECT status,attempt_count,error_message,output_artifact_id "
                "FROM media_processing_slots WHERE content_id=?",
                (content_id,),
            )[0]
            self.assertEqual(
                (reopened["status"], reopened["attempt_count"], reopened["output_artifact_id"]),
                ("retryable_failed", 0, None),
            )
            self.assertTrue(reopened["error_message"].startswith("CampaignRepair:"))
            campaign.state.pop("media_source_link_repair")
            campaign.state["results"]["download"][str(content_id)] = {
                "content_id": content_id,
                "status": "failed",
                "error_code": "TerminalMediaSlotError",
            }
            campaign._save()
            resumed = campaign.repair_media_source_links()
            self.assertEqual(
                (resumed["checked"], resumed["dealiased"], resumed["already_private"],
                 resumed["slots_reset"], resumed["already_reset"]),
                (1, 0, 1, 0, 1),
            )
            self.assertEqual(
                campaign.state["results"]["download"][str(content_id)]["status"],
                "repair_ready",
            )
            result = campaign.run_phase(
                "download",
                limit=1,
                urlopen_fn=lambda *_args, **_kwargs: _Response(
                    video_bytes, url, "video/mp4"
                ),
            )
            self.assertEqual(result["processed"], 1)
            self.assertEqual(
                campaign.state["results"]["download"][str(content_id)]["status"],
                "downloaded",
            )
            self.assertNotIn(str(unready), campaign.state["results"]["download"])
            self.assertEqual(self.fixture.usage(), usage_before)
        slot = self._rows(
            "SELECT status,attempt_count,error_message,output_artifact_id "
            "FROM media_processing_slots WHERE content_id=?",
            (content_id,),
        )[0]
        self.assertEqual((slot["status"], slot["attempt_count"]), ("succeeded", 1))
        self.assertIsNotNone(slot["output_artifact_id"])

    def test_media_source_link_repair_excludes_other_terminal_errors(self) -> None:
        campaign = self.fixture.campaign()
        with campaign.window():
            content_id = self.fixture.content(campaign=campaign)
            self._seed_detail(content_id, ["https://cdn.example/not-repairable.mp4"])
            source = self._rows(
                "SELECT * FROM evidence_artifacts WHERE content_id=? "
                "AND artifact_type='media_source' ORDER BY id DESC LIMIT 1",
                (content_id,),
            )[0]
            source_path = media._resolved(source["local_path"])
            rollback_copy = self.root / "other-terminal-source.json"
            os.link(source_path, rollback_copy)
            with connect(self.db) as connection:
                connection.execute(
                    """
                    INSERT INTO media_processing_slots(
                      content_id,source_sha256,processor_type,processor_version,
                      status,output_artifact_id,attempt_count,error_message,created_at,updated_at
                    ) VALUES (?,?,'download',?,'terminal_failed',NULL,3,?,?,?)
                    """,
                    (
                        content_id,
                        source["sha256"],
                        media.VIDEO_DOWNLOAD_VERSION,
                        "MediaProcessingError: provider media URL exhausted",
                        now_utc(),
                        now_utc(),
                    ),
                )
                connection.commit()
            result = campaign.repair_media_source_links()
        self.assertEqual(
            (result["checked"], result["dealiased"], result["slots_reset"]),
            (0, 0, 0),
        )
        self.assertEqual(source_path.stat().st_nlink, 2)
        slot = self._rows(
            "SELECT status,attempt_count,error_message FROM media_processing_slots "
            "WHERE content_id=?",
            (content_id,),
        )[0]
        self.assertEqual((slot["status"], slot["attempt_count"]), ("terminal_failed", 3))
        self.assertEqual(slot["error_message"], "MediaProcessingError: provider media URL exhausted")

    def test_media_source_link_repair_refunds_retryable_local_failure(self) -> None:
        campaign = self.fixture.campaign()
        with campaign.window():
            content_id = self.fixture.content(campaign=campaign)
            self._seed_detail(content_id, ["https://cdn.example/retryable-source.mp4"])
            source = self._rows(
                "SELECT * FROM evidence_artifacts WHERE content_id=? "
                "AND artifact_type='media_source' ORDER BY id DESC LIMIT 1",
                (content_id,),
            )[0]
            source_path = media._resolved(source["local_path"])
            os.link(source_path, self.root / "retryable-source-rollback.json")
            with connect(self.db) as connection:
                connection.execute(
                    """
                    INSERT INTO media_processing_slots(
                      content_id,source_sha256,processor_type,processor_version,
                      status,output_artifact_id,attempt_count,error_message,created_at,updated_at
                    ) VALUES (?,?,'download',?,'retryable_failed',NULL,1,?,?,?)
                    """,
                    (
                        content_id,
                        source["sha256"],
                        media.VIDEO_DOWNLOAD_VERSION,
                        "MediaProcessingError: video media source must be a private regular file",
                        now_utc(),
                        now_utc(),
                    ),
                )
                connection.commit()
            result = campaign.repair_media_source_links()
        self.assertEqual(
            (result["checked"], result["dealiased"], result["slots_reset"],
             result["attempts_refunded"]),
            (1, 1, 1, 1),
        )
        self.assertEqual(source_path.stat().st_nlink, 1)
        slot = self._rows(
            "SELECT status,attempt_count,error_message FROM media_processing_slots "
            "WHERE content_id=?",
            (content_id,),
        )[0]
        self.assertEqual((slot["status"], slot["attempt_count"]), ("retryable_failed", 0))
        self.assertTrue(slot["error_message"].startswith("CampaignRepair:"))

    def test_local_failure_repair_dealiases_frames_and_reopens_ocr(self) -> None:
        campaign = self.fixture.campaign()
        with campaign.window():
            content_id = self.fixture.content(campaign=campaign)
            link_id = self.fixture.row(content_id)["link_id"]
            frames_path = self.fixture.media_root / link_id / "frames" / "frames.json"
            frames_path.parent.mkdir(parents=True)
            frames_path.write_text('{"frames": []}\n')
            frame_path = frames_path.parent / "frame-000.jpg"
            frame_path.write_bytes(b"frame-evidence")
            asr_path = self.fixture.media_root / link_id / "asr.json"
            asr_path.write_text('{"text": "cached"}\n')
            versions = media.processor_versions()
            with connect(self.db) as connection:
                frames = media.register_artifact(
                    connection,
                    content_id=content_id,
                    artifact_type="frames_manifest",
                    path=frames_path,
                    processor_version=versions["frames"],
                )
                captured_at = now_utc()
                connection.execute(
                    """
                    INSERT INTO media_processing_slots(
                      content_id,source_sha256,processor_type,processor_version,
                      status,output_artifact_id,attempt_count,created_at,updated_at
                    ) VALUES (?,?,'frames',?,'succeeded',?,1,?,?)
                    """,
                    (
                        content_id,
                        "1" * 64,
                        versions["frames"],
                        frames.id,
                        captured_at,
                        captured_at,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO media_processing_slots(
                      content_id,source_sha256,processor_type,processor_version,
                      status,output_artifact_id,attempt_count,created_at,updated_at
                    ) VALUES (?,?,'ocr',?,'running',NULL,1,?,?)
                    """,
                    (
                        content_id,
                        frames.sha256,
                        versions["ocr"],
                        captured_at,
                        captured_at,
                    ),
                )
                connection.commit()
            rollback = self.root / "frames-rollback.json"
            frame_rollback = self.root / "frame-rollback.jpg"
            asr_rollback = self.root / "asr-rollback.json"
            original_inode = frames_path.stat().st_ino
            os.link(frames_path, rollback)
            os.link(frame_path, frame_rollback)
            os.link(asr_path, asr_rollback)
            campaign.state["results"].setdefault("local", {})[str(content_id)] = {
                "content_id": content_id,
                "status": "failed",
                "error_code": "MediaProcessingError",
                "error": "cached frames_manifest output must be a private regular file",
            }
            campaign._save()
            result = campaign.repair_local_failures()
        self.assertEqual(
            (result["derived_checked"], result["derived_dealiased"],
             result["ocr_slots_reset"]),
            (1, 3, 1),
        )
        self.assertNotEqual(frames_path.stat().st_ino, original_inode)
        self.assertEqual(frames_path.stat().st_nlink, 1)
        self.assertEqual(rollback.stat().st_ino, original_inode)
        self.assertEqual(frame_path.stat().st_nlink, 1)
        self.assertEqual(frame_rollback.stat().st_nlink, 1)
        self.assertEqual(asr_path.stat().st_nlink, 1)
        self.assertEqual(asr_rollback.stat().st_nlink, 1)
        slot = self._rows(
            "SELECT status,attempt_count,error_message FROM media_processing_slots "
            "WHERE content_id=? AND processor_type='ocr'",
            (content_id,),
        )[0]
        self.assertEqual((slot["status"], slot["attempt_count"]), ("retryable_failed", 0))
        self.assertEqual(slot["error_message"], runner.DERIVED_LINK_REPAIR_MESSAGE)
        self.assertEqual(
            campaign.state["results"]["local"][str(content_id)]["status"],
            "repair_ready",
        )

    def test_local_failure_repair_quarantines_undecodable_video(self) -> None:
        valid_video = self.root / "replacement-video.mp4"
        subprocess.run(
            [
                "ffmpeg", "-y", "-nostdin", "-loglevel", "error",
                "-f", "lavfi", "-i", "color=c=black:s=32x24:d=0.2",
                "-c:v", "libx264", str(valid_video),
            ],
            check=True,
            timeout=30,
        )
        valid_video_bytes = valid_video.read_bytes()
        campaign = self.fixture.campaign()
        with campaign.window():
            content_id = self.fixture.content(campaign=campaign)
            self._seed_detail(content_id, ["https://cdn.example/truncated.mp4"])
            source = self._rows(
                "SELECT * FROM evidence_artifacts WHERE content_id=? "
                "AND artifact_type='media_source' ORDER BY id DESC LIMIT 1",
                (content_id,),
            )[0]
            link_id = self.fixture.row(content_id)["link_id"]
            source_identity = media._media_source_identity(
                "video", ["https://cdn.example/truncated.mp4"]
            )[1]
            video_path = (
                self.fixture.media_root / link_id / "downloads" / source_identity / "source.mp4"
            )
            video_path.parent.mkdir(parents=True)
            video_path.write_bytes(b"truncated-video" * 512)
            versions = media.processor_versions()
            with connect(self.db) as connection:
                artifact = media.register_artifact(
                    connection,
                    content_id=content_id,
                    artifact_type="media",
                    path=video_path,
                    processor_version=media.VIDEO_DOWNLOAD_VERSION,
                    metadata={"source_count": 1, "source_sha256": "2" * 64},
                )
                captured_at = now_utc()
                connection.execute(
                    """
                    INSERT INTO media_processing_slots(
                      content_id,source_sha256,processor_type,processor_version,
                      status,output_artifact_id,attempt_count,created_at,updated_at
                    ) VALUES (?,?,'download',?,'succeeded',?,1,?,?)
                    """,
                    (
                        content_id,
                        source["sha256"],
                        media.VIDEO_DOWNLOAD_VERSION,
                        artifact.id,
                        captured_at,
                        captured_at,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO media_processing_slots(
                      content_id,source_sha256,processor_type,processor_version,
                      status,output_artifact_id,attempt_count,error_message,
                      created_at,updated_at
                    ) VALUES (?,?,'frames',?,'retryable_failed',NULL,1,?,?,?)
                    """,
                    (
                        content_id,
                        artifact.sha256,
                        versions["frames"],
                        "MediaProcessingError: no frames were extracted",
                        captured_at,
                        captured_at,
                    ),
                )
                connection.commit()
            campaign.state["results"].setdefault("download", {})[str(content_id)] = {
                "content_id": content_id,
                "status": "downloaded",
            }
            campaign.state["results"].setdefault("local", {})[str(content_id)] = {
                "content_id": content_id,
                "status": "failed",
                "error_code": "MediaProcessingError",
                "error": "no frames were extracted",
            }
            campaign._save()
            with patch.object(media, "_valid_media", return_value=False):
                result = campaign.repair_local_failures()
                repeated = campaign.repair_local_failures()
            repaired_artifact = self._rows(
                "SELECT status FROM evidence_artifacts WHERE id=?", (artifact.id,)
            )[0]
            self.assertEqual(repaired_artifact["status"], "failed")
            repaired_slot = self._rows(
                "SELECT status,attempt_count,output_artifact_id,error_message "
                "FROM media_processing_slots WHERE content_id=? "
                "AND processor_type='download'",
                (content_id,),
            )[0]
            self.assertEqual(
                (
                    repaired_slot["status"],
                    repaired_slot["attempt_count"],
                    repaired_slot["output_artifact_id"],
                ),
                ("retryable_failed", 0, None),
            )
            self.assertEqual(
                repaired_slot["error_message"], runner.INVALID_VIDEO_REPAIR_MESSAGE
            )
            downloaded = campaign.run_phase(
                "download",
                limit=1,
                urlopen_fn=lambda *_args, **_kwargs: _Response(
                    valid_video_bytes,
                    "https://cdn.example/truncated.mp4",
                    "video/mp4",
                ),
            )
        self.assertEqual(
            (result["invalid_video_checked"], result["videos_quarantined"],
             result["download_slots_reset"]),
            (1, 1, 1),
        )
        self.assertEqual(
            (repeated["invalid_video_checked"], repeated["videos_quarantined"],
             repeated["download_slots_reset"]),
            (1, 1, 1),
        )
        self.assertEqual(downloaded["processed"], 1)
        self.assertTrue(video_path.is_file())
        quarantine = Path(result["quarantine_paths"][0])
        self.assertTrue(quarantine.is_file())
        artifact_row = self._rows(
            "SELECT status FROM evidence_artifacts WHERE id=?", (artifact.id,)
        )[0]
        self.assertEqual(artifact_row["status"], "available")
        slot = self._rows(
            "SELECT status,attempt_count,output_artifact_id,error_message "
            "FROM media_processing_slots WHERE content_id=? AND processor_type='download'",
            (content_id,),
        )[0]
        self.assertEqual(
            (slot["status"], slot["attempt_count"], slot["output_artifact_id"]),
            ("succeeded", 1, artifact.id),
        )
        self.assertEqual(
            campaign.state["results"]["download"][str(content_id)]["status"],
            "downloaded",
        )

    def test_local_failure_repair_accepts_exact_invalid_media_preflight(self) -> None:
        campaign = self.fixture.campaign()
        with campaign.window():
            content_id = self.fixture.content(campaign=campaign)
            self._seed_detail(
                content_id, ["https://cdn.example/preflight-invalid.mp4"]
            )
            source = self._rows(
                "SELECT * FROM evidence_artifacts WHERE content_id=? "
                "AND artifact_type='media_source' ORDER BY id DESC LIMIT 1",
                (content_id,),
            )[0]
            link_id = self.fixture.row(content_id)["link_id"]
            source_identity = media._media_source_identity(
                "video", ["https://cdn.example/preflight-invalid.mp4"]
            )[1]
            video_path = (
                self.fixture.media_root
                / link_id
                / "downloads"
                / source_identity
                / "source.mp4"
            )
            video_path.parent.mkdir(parents=True)
            video_path.write_bytes(b"preflight-invalid-video" * 512)
            with connect(self.db) as connection:
                artifact = media.register_artifact(
                    connection,
                    content_id=content_id,
                    artifact_type="media",
                    path=video_path,
                    processor_version=media.VIDEO_DOWNLOAD_VERSION,
                    metadata={"source_count": 1, "source_sha256": "3" * 64},
                )
                captured_at = now_utc()
                connection.execute(
                    """
                    INSERT INTO media_processing_slots(
                      content_id,source_sha256,processor_type,processor_version,
                      status,output_artifact_id,attempt_count,created_at,updated_at
                    ) VALUES (?,?,'download',?,'succeeded',?,1,?,?)
                    """,
                    (
                        content_id,
                        source["sha256"],
                        media.VIDEO_DOWNLOAD_VERSION,
                        artifact.id,
                        captured_at,
                        captured_at,
                    ),
                )
                connection.commit()
            campaign.state["results"].setdefault("download", {})[
                str(content_id)
            ] = {"content_id": content_id, "status": "downloaded"}
            campaign.state["results"].setdefault("local", {})[
                str(content_id)
            ] = {
                "content_id": content_id,
                "status": "failed",
                "error_code": "MediaProcessingError",
                "error": f"invalid media: {video_path}",
            }
            campaign._save()
            with patch.object(media, "_valid_media", return_value=False):
                result = campaign.repair_local_failures()

        self.assertEqual(
            (
                result["invalid_video_checked"],
                result["videos_quarantined"],
                result["download_slots_reset"],
            ),
            (1, 1, 1),
        )
        self.assertFalse(video_path.exists())
        self.assertTrue(Path(result["quarantine_paths"][0]).is_file())
        artifact_row = self._rows(
            "SELECT status FROM evidence_artifacts WHERE id=?", (artifact.id,)
        )[0]
        self.assertEqual(artifact_row["status"], "failed")
        slot = self._rows(
            "SELECT status,attempt_count,output_artifact_id,error_message "
            "FROM media_processing_slots WHERE content_id=? "
            "AND processor_type='download'",
            (content_id,),
        )[0]
        self.assertEqual(
            (slot["status"], slot["attempt_count"], slot["output_artifact_id"]),
            ("retryable_failed", 0, None),
        )
        self.assertEqual(
            slot["error_message"], runner.INVALID_VIDEO_REPAIR_MESSAGE
        )

    def test_download_prioritizes_partial_content_with_available_source(self) -> None:
        campaign = self.fixture.campaign()
        with campaign.window():
            unready = self.fixture.content(campaign=campaign)
            ready = self.fixture.content(campaign=campaign)
            self._seed_detail(ready, ["https://cdn.example/source-ready.mp4"])
            campaign.state["results"].setdefault("content", {})[str(ready)] = {
                "content_id": ready,
                "status": "partial",
                "stages": [{
                    "stage": "metrics",
                    "status": "failed",
                    "error_code": "provider_retry_requested",
                }],
            }
            campaign._save()
            with patch.object(
                media,
                "process_content_media",
                return_value={"status": "downloaded", "artifact_id": 1},
            ) as process:
                result = campaign.run_phase("download", limit=1)
        self.assertEqual(result["processed"], 1)
        self.assertIn(str(ready), campaign.state["results"]["download"])
        self.assertNotIn(str(unready), campaign.state["results"]["download"])
        self.assertEqual(process.call_args.args[0], ready)

    def test_download_actionable_is_exactly_captured_source_or_failed(self) -> None:
        existing_pending = self.fixture.content()
        campaign = self.fixture.campaign()
        with campaign.window():
            uncaptured = self.fixture.content(campaign=campaign)
            ready = self.fixture.content(campaign=campaign)
            failed = self.fixture.content(campaign=campaign)
            self._seed_detail(
                existing_pending,
                ["https://cdn.example/existing-pending.mp4"],
            )
            self._seed_detail(ready, ["https://cdn.example/source-ready.mp4"])
            campaign.state["results"].setdefault("content", {}).update({
                str(ready): {"content_id": ready, "status": "partial"},
                str(failed): {"content_id": failed, "status": "succeeded"},
            })
            campaign.state["results"].setdefault("download", {})[
                str(failed)
            ] = {"content_id": failed, "status": "failed"}
            campaign._save()
            self.assertEqual(
                campaign._cycle_actionable("download"),
                [existing_pending, ready, failed],
            )
            with patch.object(
                media,
                "process_content_media",
                return_value={"status": "downloaded", "artifact_id": 1},
            ) as process:
                result = campaign.run_phase(
                    "download", limit=1, scope_content_ids=[ready]
                )
        self.assertEqual(result["processed_content_ids"], [ready])
        self.assertEqual(process.call_args.args[0], ready)
        self.assertNotIn(str(uncaptured), campaign.state["results"]["download"])
        self.assertEqual(
            campaign.state["results"]["download"][str(failed)]["status"],
            "failed",
        )

    def test_cycle_drains_backlog_then_closes_only_one_new_batch(self) -> None:
        campaign = self.fixture.campaign()
        with campaign.window():
            backlog_download = self.fixture.content(campaign=campaign)
            backlog_local = self.fixture.content(campaign=campaign)
            fresh = self.fixture.content(campaign=campaign)
            campaign.state["results"].setdefault("content", {}).update({
                str(backlog_download): {"status": "succeeded"},
                str(backlog_local): {"status": "succeeded"},
            })
            download_queue = [backlog_download]
            local_queue = [backlog_local]
            calls: list[tuple[str, tuple[int, ...], bool]] = []

            def run_phase(phase, **kwargs):
                scope = tuple(kwargs.get("scope_content_ids") or ())
                calls.append(
                    (phase, scope, kwargs.get("allow_download_refresh", True))
                )
                if phase == "content":
                    self.assertTrue(kwargs["pipeline_fresh_local"])
                    campaign.state["results"]["content"][str(fresh)] = {
                        "status": "succeeded"
                    }
                    campaign.state["results"].setdefault("download", {})[
                        str(fresh)
                    ] = {"status": "downloaded"}
                    campaign.state["results"].setdefault("local", {})[
                        str(fresh)
                    ] = {"status": "complete"}
                    return {
                        "status": "partial",
                        "processed_content_ids": [fresh],
                    }
                if phase == "download":
                    content_id = scope[0]
                    download_queue.remove(content_id)
                    local_queue.append(content_id)
                    campaign.state["results"].setdefault("download", {})[
                        str(content_id)
                    ] = {"status": "downloaded"}
                else:
                    for content_id in scope:
                        local_queue.remove(content_id)
                        campaign.state["results"].setdefault("local", {})[
                            str(content_id)
                        ] = {
                            "status": (
                                "terminal_insufficient"
                                if content_id == backlog_local
                                else "complete"
                            )
                        }
                return {"status": "succeeded", "processed_content_ids": list(scope)}

            disk = SimpleNamespace(free=20 * 1024**3)
            with (
                patch.object(runner.shutil, "disk_usage", return_value=disk),
                patch.object(
                    campaign,
                    "_cycle_actionable",
                    side_effect=lambda phase: (
                        list(download_queue)
                        if phase == "download"
                        else list(local_queue)
                    ),
                ),
                patch.object(campaign, "run_phase", side_effect=run_phase),
            ):
                result = campaign.run_cycle(limit=1, max_seconds=30)

        self.assertEqual(
            calls,
            [
                ("download", (backlog_download,), False),
                ("local", (backlog_local, backlog_download), False),
                ("content", (), True),
            ],
        )
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["attempted"], 1)
        self.assertEqual(result["captured_downloaded"], 1)
        self.assertEqual(result["analyzed"], 3)

    def test_cycle_accepts_200_and_rejects_201(self) -> None:
        campaign = self.fixture.campaign()
        disk = SimpleNamespace(free=20 * 1024**3)
        with campaign.window(), patch.object(
            runner.shutil, "disk_usage", return_value=disk
        ):
            campaign.state["discovery_complete"] = True
            campaign._save()

            accepted = campaign.run_cycle(limit=200, max_seconds=30)
            with self.assertRaisesRegex(runner.CampaignError, "1\\.\\.200"):
                campaign.run_cycle(limit=201, max_seconds=30)

        self.assertEqual(accepted["status"], "succeeded")

    def test_content_limit_200_keeps_100_content_checkpoints(self) -> None:
        campaign = self.fixture.campaign()
        checkpoint_sizes: list[int] = []
        with campaign.window():
            with patch.object(campaign, "_save"):
                content_ids = [
                    self.fixture.content(campaign=campaign) for _ in range(200)
                ]
            campaign._save()

            def capture_one(content_id, **_kwargs):
                return {"content_id": content_id, "status": "succeeded"}

            def pipeline(batch, _captured, _download):
                checkpoint_sizes.append(len(batch))
                return (
                    {
                        content_id: {
                            "content_id": content_id,
                            "status": "downloaded",
                        }
                        for content_id in batch
                    },
                    {
                        content_id: {
                            "content_id": content_id,
                            "status": "complete",
                        }
                        for content_id in batch
                    },
                )

            with patch.object(
                campaign, "capture_one", side_effect=capture_one
            ), patch.object(
                campaign, "_pipeline_fresh_content_batch", side_effect=pipeline
            ):
                result = campaign.run_phase(
                    "content", limit=200, pipeline_fresh_local=True
                )

        self.assertEqual(checkpoint_sizes, [100, 100])
        self.assertEqual(result["processed"], 200)
        self.assertEqual(result["processed_content_ids"], content_ids)
        self.assertEqual(result["status"], "succeeded")

    def test_cycle_retries_fresh_download_failure_with_refresh_then_finalizes(self) -> None:
        campaign = self.fixture.campaign()
        with campaign.window():
            fresh = self.fixture.content(campaign=campaign)
            download_queue: list[int] = []
            local_queue: list[int] = []
            calls: list[tuple[str, tuple[int, ...], bool]] = []

            def run_phase(phase, **kwargs):
                scope = tuple(kwargs.get("scope_content_ids") or ())
                calls.append(
                    (phase, scope, kwargs.get("allow_download_refresh", True))
                )
                if phase == "content":
                    self.assertTrue(kwargs["pipeline_fresh_local"])
                    campaign.state["results"].setdefault("content", {})[
                        str(fresh)
                    ] = {
                        "status": "succeeded"
                    }
                    campaign.state["results"].setdefault("download", {})[
                        str(fresh)
                    ] = {"status": "failed"}
                    download_queue.append(fresh)
                    return {
                        "status": "partial",
                        "processed_content_ids": [fresh],
                    }
                if phase == "download":
                    self.assertEqual(scope, (fresh,))
                    download_queue.remove(fresh)
                    local_queue.append(fresh)
                    campaign.state["results"]["download"][str(fresh)] = {
                        "status": "downloaded"
                    }
                else:
                    self.assertEqual(scope, (fresh,))
                    local_queue.remove(fresh)
                    campaign.state["results"].setdefault("local", {})[
                        str(fresh)
                    ] = {"status": "complete"}
                return {
                    "status": "succeeded",
                    "processed_content_ids": list(scope),
                }

            disk = SimpleNamespace(free=20 * 1024**3)
            with (
                patch.object(runner.shutil, "disk_usage", return_value=disk),
                patch.object(
                    campaign,
                    "_cycle_actionable",
                    side_effect=lambda phase: (
                        list(download_queue)
                        if phase == "download"
                        else list(local_queue)
                    ),
                ),
                patch.object(campaign, "run_phase", side_effect=run_phase),
            ):
                result = campaign.run_cycle(limit=1, max_seconds=30)

        self.assertEqual(
            calls,
            [
                ("content", (), True),
                ("download", (fresh,), True),
                ("local", (fresh,), False),
            ],
        )
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["captured_downloaded"], 1)
        self.assertEqual(result["analyzed"], 1)

    def test_cycle_blocks_before_purchase_when_backlog_makes_no_progress(self) -> None:
        campaign = self.fixture.campaign()
        with campaign.window():
            blocked = self.fixture.content(campaign=campaign)
            phases: list[str] = []
            disk = SimpleNamespace(free=20 * 1024**3)

            def unchanged_phase(phase, **_kwargs):
                phases.append(phase)
                return {"status": "partial", "processed_content_ids": [blocked]}

            with (
                patch.object(runner.shutil, "disk_usage", return_value=disk),
                patch.object(
                    campaign,
                    "_cycle_actionable",
                    side_effect=lambda phase: [blocked]
                    if phase == "download"
                    else [],
                ),
                patch.object(campaign, "run_phase", side_effect=unchanged_phase),
            ):
                result = campaign.run_cycle(limit=1, max_seconds=30)
        self.assertEqual(phases, ["download"])
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["blocked_reason"], "download_made_no_progress")
        self.assertEqual(result["attempted"], 0)

    def test_cycle_blocks_when_whole_cycle_makes_no_progress(self) -> None:
        campaign = self.fixture.campaign()
        with campaign.window():
            self.fixture.content(campaign=campaign)
            disk = SimpleNamespace(free=20 * 1024**3)
            with (
                patch.object(runner.shutil, "disk_usage", return_value=disk),
                patch.object(campaign, "_cycle_actionable", return_value=[]),
                patch.object(
                    campaign,
                    "run_phase",
                    return_value={
                        "status": "partial",
                        "processed_content_ids": [],
                    },
                ),
            ):
                result = campaign.run_cycle(limit=1, max_seconds=30)
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["blocked_reason"], "cycle_made_no_progress")
        self.assertEqual(result["attempted"], 0)
        self.assertEqual(result["analyzed"], 0)

    def test_cycle_capacity_gate_blocks_without_deleting_or_running_phase(self) -> None:
        campaign = self.fixture.campaign()
        with campaign.window(), patch.object(
            runner.shutil,
            "disk_usage",
            return_value=SimpleNamespace(free=runner.media_policy.MIN_FREE_BYTES - 1),
        ), patch.object(campaign, "run_phase") as run_phase:
            with self.assertRaisesRegex(runner.CampaignError, "below 10 GiB"):
                campaign.run_cycle(limit=1, max_seconds=30)
        run_phase.assert_not_called()

    def test_cycle_fresh_pipeline_prepares_fast_item_while_slow_download_runs(self) -> None:
        campaign = self.fixture.campaign()
        slow_started = threading.Event()
        fast_prepare_started = threading.Event()
        slow_done = threading.Event()
        prepared_done: set[int] = set()
        calls: dict[tuple[str, int], int] = {}
        lock = threading.Lock()
        main_thread = threading.get_ident()

        with campaign.window():
            slow = self.fixture.content(campaign=campaign)
            fast = self.fixture.content(campaign=campaign)

            def count(kind, content_id):
                with lock:
                    key = (kind, content_id)
                    calls[key] = calls.get(key, 0) + 1

            def capture_one(content_id, **_kwargs):
                count("capture", content_id)
                return {"content_id": content_id, "status": "succeeded"}

            def download_one(content_id, **kwargs):
                count("download", content_id)
                self.assertFalse(kwargs["allow_refresh"])
                if content_id == slow:
                    slow_started.set()
                    self.assertTrue(fast_prepare_started.wait(timeout=2))
                    slow_done.set()
                else:
                    self.assertTrue(slow_started.wait(timeout=2))
                return {"content_id": content_id, "status": "downloaded"}

            def prepare(content_id, **_kwargs):
                count("prepare", content_id)
                if content_id == fast:
                    self.assertFalse(slow_done.is_set())
                    fast_prepare_started.set()
                else:
                    self.assertTrue(slow_done.is_set())
                with lock:
                    prepared_done.add(content_id)
                return {"content_id": content_id, "status": "ready"}

            def finalize(group):
                self.assertEqual(threading.get_ident(), main_thread)
                self.assertTrue(slow_done.is_set())
                self.assertEqual(prepared_done, {slow, fast})
                return {
                    content_id: {"content_id": content_id, "status": "complete"}
                    for content_id in group
                }

            with patch.object(
                campaign, "capture_one", side_effect=capture_one
            ), patch.object(
                campaign, "download_one", side_effect=download_one
            ), patch.object(
                campaign, "_prepare_local_media_one", side_effect=prepare
            ), patch.object(
                campaign, "_finalize_local_group", side_effect=finalize
            ) as finalize_group, patch.object(
                media, "ocr_binary_path", return_value=self.root / "ocr"
            ), patch.object(
                media,
                "pinned_whisper_model_path",
                return_value=self.root / "whisper",
            ):
                result = campaign.run_phase(
                    "content", limit=2, pipeline_fresh_local=True
                )

        self.assertEqual(result["status"], "succeeded")
        finalize_group.assert_called_once()
        for content_id in (slow, fast):
            self.assertEqual(calls[("capture", content_id)], 1)
            self.assertEqual(calls[("download", content_id)], 1)
            self.assertEqual(calls[("prepare", content_id)], 1)
            self.assertEqual(
                campaign.state["results"]["local"][str(content_id)]["status"],
                "complete",
            )

    def test_cycle_fresh_pipeline_settles_and_preserves_blocked_failures(self) -> None:
        campaign = self.fixture.campaign()
        calls: dict[tuple[str, int], int] = {}
        downloaded: set[int] = set()
        prepared: set[int] = set()

        with campaign.window():
            blocked = self.fixture.content(campaign=campaign)
            prepare_failed = self.fixture.content(campaign=campaign)
            succeeded = self.fixture.content(campaign=campaign)
            content_ids = {blocked, prepare_failed, succeeded}

            def count(kind, content_id):
                key = (kind, content_id)
                calls[key] = calls.get(key, 0) + 1

            def capture_one(content_id, **_kwargs):
                count("capture", content_id)
                return {"content_id": content_id, "status": "succeeded"}

            def download_one(content_id, **_kwargs):
                count("download", content_id)
                downloaded.add(content_id)
                if content_id == blocked:
                    return {
                        "content_id": content_id,
                        "status": "failed",
                        "error_code": "provider_balance_blocked",
                    }
                return {"content_id": content_id, "status": "downloaded"}

            def prepare(content_id, **_kwargs):
                count("prepare", content_id)
                prepared.add(content_id)
                if content_id == prepare_failed:
                    raise OSError("fixture prepare failed")
                return {"content_id": content_id, "status": "ready"}

            def finalize(group):
                self.assertEqual(downloaded, content_ids)
                self.assertEqual(prepared, {prepare_failed, succeeded})
                self.assertEqual(set(group), {succeeded})
                return {
                    succeeded: {"content_id": succeeded, "status": "complete"}
                }

            with patch.object(
                campaign, "capture_one", side_effect=capture_one
            ), patch.object(
                campaign, "download_one", side_effect=download_one
            ), patch.object(
                campaign, "_prepare_local_media_one", side_effect=prepare
            ), patch.object(
                campaign, "_finalize_local_group", side_effect=finalize
            ), patch.object(
                media, "ocr_binary_path", return_value=self.root / "ocr"
            ), patch.object(
                media,
                "pinned_whisper_model_path",
                return_value=self.root / "whisper",
            ):
                result = campaign.run_phase(
                    "content", limit=3, pipeline_fresh_local=True
                )

        self.assertEqual(result["status"], "blocked")
        self.assertEqual(
            campaign.state["results"]["download"][str(blocked)]["status"],
            "failed",
        )
        self.assertEqual(
            campaign.state["results"]["local"][str(prepare_failed)]["status"],
            "failed",
        )
        self.assertEqual(
            campaign.state["results"]["local"][str(succeeded)]["status"],
            "complete",
        )
        for content_id in content_ids:
            self.assertEqual(calls[("capture", content_id)], 1)
            self.assertEqual(calls[("download", content_id)], 1)
        self.assertNotIn(("prepare", blocked), calls)
        self.assertEqual(calls[("prepare", prepare_failed)], 1)
        self.assertEqual(calls[("prepare", succeeded)], 1)

    def test_cycle_fresh_pipeline_finalizes_in_32_item_batches(self) -> None:
        campaign = self.fixture.campaign()
        group_sizes: list[int] = []

        with campaign.window():
            content_ids = [
                self.fixture.content(campaign=campaign)
                for _ in range(runner.LOCAL_FINALIZE_BATCH_SIZE + 1)
            ]
            captured = {
                content_id: {"content_id": content_id, "status": "succeeded"}
                for content_id in content_ids
            }

            def finalize(group):
                group_sizes.append(len(group))
                return {
                    content_id: {"content_id": content_id, "status": "complete"}
                    for content_id in group
                }

            with (
                patch.object(
                    campaign,
                    "_prepare_local_media_one",
                    side_effect=lambda content_id, **_kwargs: {
                        "content_id": content_id,
                        "status": "ready",
                    },
                ),
                patch.object(
                    campaign, "_finalize_local_group", side_effect=finalize
                ),
                patch.object(
                    media, "ocr_binary_path", return_value=self.root / "ocr"
                ),
                patch.object(
                    media,
                    "pinned_whisper_model_path",
                    return_value=self.root / "whisper",
                ),
            ):
                downloaded, local = campaign._pipeline_fresh_content_batch(
                    content_ids,
                    captured,
                    lambda content_id: {
                        "content_id": content_id,
                        "status": "downloaded",
                    },
                )

        self.assertEqual(group_sizes, [runner.LOCAL_FINALIZE_BATCH_SIZE, 1])
        self.assertEqual(set(downloaded), set(content_ids))
        self.assertEqual(set(local), set(content_ids))

    def test_douyin_image_groups_are_frozen_from_raw_not_flattened_into_images(self) -> None:
        groups = [
            ["https://p3-sign.douyinpic.com/first.jpg", "https://p9-sign.douyinpic.com/first.jpg"],
            ["https://p3-sign.douyinpic.com/second.jpg", "https://p9-sign.douyinpic.com/second.jpg"],
        ]
        urls = [url for group in groups for url in group]
        campaign = self.fixture.campaign()
        with campaign.window():
            cid = self.fixture.content(campaign=campaign)
            with connect(self.db) as connection:
                connection.execute("UPDATE content_items SET content_type='image' WHERE id=?", (cid,))
                connection.commit()
            self._seed_detail(cid, urls, image_groups=groups)
            expected = media.douyin_image_source_groups(urls, groups)
            with patch.object(media, "process_content_media", return_value={"status": "downloaded", "artifact_id": 1}) as process:
                campaign.download_one(cid)
            frozen = process.call_args.kwargs["frozen_image_groups"]
            self.assertEqual(frozen, expected)
            self.assertEqual(len(frozen), 2)
            self.assertEqual([len(group["candidates"]) for group in frozen], [2, 2])
            self.assertTrue(process.call_args.kwargs["download_only"])

    def test_source_refresh_is_one_purchase_with_same_task_50_budget_across_resume(self) -> None:
        campaign = self.fixture.campaign()
        paid = Mock(side_effect=lambda _stage, content: self._detail(content, [NEW_IMAGE]))
        expired = media.MediaProcessingError("HTTP Error 403: media URL expired")
        with self._ordinary_refresh(), campaign.window():
            cid = self.fixture.content("xiaohongshu", campaign=campaign)
            self._seed_detail(cid, [OLD_IMAGE])
            old_sha = self._source(cid)["source_sha256"]
            with patch.object(media, "process_content_media", side_effect=[expired, {"status": "downloaded"}]):
                campaign.download_one(cid, allow_refresh=True, call_override=paid)
            self.assertNotEqual(self._source(cid)["source_sha256"], old_sha)
        usage_after_first = self.fixture.usage()
        resumed = self.fixture.campaign(fixture_module.AS_OF + timedelta(days=1))
        with resumed.window(), patch.object(media, "process_content_media", side_effect=[expired, {"status": "downloaded"}]):
            resumed.download_one(cid, allow_refresh=True, call_override=paid)
        self.assertEqual(paid.call_count, 1)
        self.assertEqual(self.fixture.usage(), usage_after_first)
        self.assertEqual(usage_after_first[0], 2)  # original detail + exactly one refresh
        refresh_slots = self._rows("SELECT status,attempt_count FROM fetch_slots WHERE stage='media_source_refresh'")
        self.assertEqual(refresh_slots, [{"status": "succeeded", "attempt_count": 1}])
        budgets = self._rows("""SELECT DISTINCT u.task_id,b.max_amount
            FROM provider_usage u JOIN provider_budget_batches b ON b.id=u.budget_batch_id""")
        self.assertEqual(budgets, [{"task_id": runner.TASK_ID, "max_amount": 50.0}])

    def test_xhs_exhausted_image_group_refreshes_once(self) -> None:
        campaign = self.fixture.campaign()
        paid = Mock(
            side_effect=lambda _stage, content: self._detail(content, [NEW_IMAGE])
        )
        exhausted = media.MediaProcessingError(
            "image download incomplete: logical image group 0 exhausted"
        )
        with self._ordinary_refresh(), campaign.window():
            cid = self.fixture.content("xiaohongshu", campaign=campaign)
            self._seed_detail(cid, [OLD_IMAGE])
            old_sha = self._source(cid)["source_sha256"]
            with patch.object(
                media,
                "process_content_media",
                side_effect=[exhausted, {"status": "downloaded"}],
            ):
                result = campaign.download_one(
                    cid,
                    allow_refresh=True,
                    call_override=paid,
                )
        self.assertEqual(result["status"], "downloaded")
        paid.assert_called_once()
        self.assertNotEqual(self._source(cid)["source_sha256"], old_sha)
        self.assertEqual(
            self._rows(
                "SELECT status,attempt_count FROM fetch_slots "
                "WHERE stage='media_source_refresh'"
            ),
            [{"status": "succeeded", "attempt_count": 1}],
        )

    def test_existing_history_douyin_can_use_web_and_high_quality_refresh(self) -> None:
        cid = self.fixture.content()
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET source_group='history-backfill' WHERE id=?",
                (cid,),
            )
            connection.commit()
        campaign = self.fixture.campaign()
        content = self.fixture.row(cid)
        unplayable = media.MediaProcessingError(
            "media download failed: candidate 0 was not a playable video"
        )
        high_quality = capture.ProviderResult(
            {
                "title": "",
                "body": "",
                "published_at": None,
                "account_uid": "",
                "account_name": "",
                "content_type": "video",
                "media_urls": [NEW_VIDEO],
            },
            {
                "code": 200,
                "data": {
                    "video_id": str(content["platform_content_id"]),
                    "original_video_url": NEW_VIDEO,
                },
            },
            200,
            True,
        )
        with self._ordinary_refresh(), campaign.window():
            self.assertIn(cid, campaign.state["baseline_ids"])
            self.assertIn(cid, campaign.state["existing_pending_ids"])
            self._seed_detail(cid, [OLD_VIDEO])
            with (
                patch.object(providers, "_load_key", return_value="secret"),
                patch.object(
                    providers,
                    "_douyin_web_detail_call",
                    return_value=self._detail(content, [OLD_VIDEO]),
                ) as web_detail,
                patch.object(
                    providers,
                    "_douyin_high_quality_video_call",
                    return_value=high_quality,
                ) as high_quality_detail,
                patch.object(
                    media,
                    "process_content_media",
                    side_effect=[unplayable, {"status": "downloaded"}],
                ),
            ):
                result = campaign.download_one(cid, allow_refresh=True)

        self.assertEqual(result["status"], "downloaded")
        web_detail.assert_called_once_with(
            str(content["platform_content_id"]), "secret"
        )
        high_quality_detail.assert_called_once_with(
            str(content["platform_content_id"]), "secret"
        )
        self.assertEqual(self._source(cid)["urls"], [NEW_VIDEO])

    def test_ordinary_baseline_authorizes_only_frozen_media_debt(self) -> None:
        cid = self.fixture.content()
        campaign = self.fixture.campaign()
        with campaign.window():
            content = self.fixture.row(cid)
            outside_id = self.fixture.content()
            outside = self.fixture.row(outside_id)
            self.assertIn(cid, campaign.state["existing_pending_ids"])
            self.assertFalse(campaign._paid_content_authorized(content))
            self.assertTrue(campaign._paid_media_authorized(content))
            self.assertNotIn(outside_id, campaign.state["existing_pending_ids"])
            self.assertFalse(campaign._paid_content_authorized(outside))
            self.assertFalse(campaign._paid_media_authorized(outside))

    def test_new_douyin_unplayable_video_refreshes_once_through_web_detail(self) -> None:
        campaign = self.fixture.campaign()
        with self._ordinary_refresh(), campaign.window():
            cid = self.fixture.content(campaign=campaign)
            content = self.fixture.row(cid)
            self._seed_detail(cid, [OLD_VIDEO])
            unplayable = media.MediaProcessingError(
                "media download failed: candidate 0 was not a playable video"
            )
            with (
                patch.object(providers, "_load_key", return_value="secret"),
                patch.object(
                    providers,
                    "_douyin_web_detail_call",
                    return_value=self._detail(content, [NEW_VIDEO]),
                ) as web_detail,
                patch.object(
                    providers,
                    "_douyin_call",
                    side_effect=AssertionError("App V3 detail must not be repeated"),
                ) as app_detail,
                patch.object(
                    media,
                    "process_content_media",
                    side_effect=[unplayable, {"status": "downloaded"}],
                ),
            ):
                result = campaign.download_one(cid, allow_refresh=True)

        self.assertEqual(result["status"], "downloaded")
        web_detail.assert_called_once_with(
            str(content["platform_content_id"]), "secret"
        )
        app_detail.assert_not_called()
        self.assertEqual(self._source(cid)["urls"], [NEW_VIDEO])
        refresh = self._rows(
            "SELECT adapter_version,status,attempt_count FROM fetch_slots "
            "WHERE content_id=? AND stage='media_source_refresh'",
            (cid,),
        )
        self.assertEqual(
            refresh,
            [{
                "adapter_version": "tikhub-douyin-web-media-source-refresh-v8.1",
                "status": "succeeded",
                "attempt_count": 1,
            }],
        )
        self.assertEqual(self.fixture.usage()[0], 2)

    def test_douyin_web_audio_source_falls_back_to_high_quality_video(self) -> None:
        campaign = self.fixture.campaign()
        with self._ordinary_refresh(), campaign.window():
            cid = self.fixture.content(campaign=campaign)
            content = self.fixture.row(cid)
            self._seed_detail(cid, [OLD_VIDEO])
            unplayable = media.MediaProcessingError(
                "media download failed: candidate 0 was not a playable video"
            )
            high_quality = capture.ProviderResult(
                {
                    "title": "",
                    "body": "",
                    "published_at": None,
                    "account_uid": "",
                    "account_name": "",
                    "content_type": "video",
                    "media_urls": [NEW_VIDEO],
                },
                {
                    "code": 200,
                    "data": {
                        "video_id": str(content["platform_content_id"]),
                        "original_video_url": NEW_VIDEO,
                    },
                },
                200,
                True,
            )
            with (
                patch.object(providers, "_load_key", return_value="secret"),
                patch.object(
                    providers,
                    "_douyin_web_detail_call",
                    return_value=self._detail(content, [OLD_VIDEO]),
                ) as web_detail,
                patch.object(
                    providers,
                    "_douyin_high_quality_video_call",
                    return_value=high_quality,
                ) as high_quality_detail,
                patch.object(
                    media,
                    "process_content_media",
                    side_effect=[unplayable, {"status": "downloaded"}],
                ),
            ):
                result = campaign.download_one(cid, allow_refresh=True)

        self.assertEqual(result["status"], "downloaded")
        web_detail.assert_called_once_with(
            str(content["platform_content_id"]), "secret"
        )
        high_quality_detail.assert_called_once_with(
            str(content["platform_content_id"]), "secret"
        )
        self.assertEqual(self._source(cid)["urls"], [NEW_VIDEO])
        self.assertEqual(self.fixture.usage()[0], 3)

    def test_douyin_high_quality_retry_requires_compensation_authorization(self) -> None:
        campaign = self.fixture.campaign()
        retry = capture.CaptureError(
            "TikHub HTTP 400",
            retryable=True,
            error_code="provider_retry_requested",
            http_status=400,
            billed=False,
            raw_response={
                "detail": {
                    "message": "Please retry. You won't be charged."
                }
            },
        )
        with self._ordinary_refresh(), campaign.window():
            cid = self.fixture.content(campaign=campaign)
            content = self.fixture.row(cid)
            self._seed_detail(cid, [OLD_VIDEO])
            unplayable = media.MediaProcessingError(
                "media download failed: candidate 0 was not a playable video"
            )
            with (
                patch.object(providers, "_load_key", return_value="secret"),
                patch.object(
                    providers,
                    "_douyin_web_detail_call",
                    return_value=self._detail(content, [OLD_VIDEO]),
                ),
                patch.object(
                    providers,
                    "_douyin_high_quality_video_call",
                    side_effect=retry,
                ),
                patch.object(
                    media,
                    "process_content_media",
                    side_effect=unplayable,
                ),
            ):
                with self.assertRaises(capture.CaptureError):
                    campaign.download_one(cid, allow_refresh=True)

        high_quality = capture.ProviderResult(
            {
                "title": "",
                "body": "",
                "published_at": None,
                "account_uid": "",
                "account_name": "",
                "content_type": "video",
                "media_urls": [NEW_VIDEO],
            },
            {
                "code": 200,
                "data": {
                    "video_id": str(content["platform_content_id"]),
                    "original_video_url": NEW_VIDEO,
                },
            },
            200,
            True,
        )
        resumed = self.fixture.campaign()
        with (
            resumed.window(),
            patch.object(providers, "_load_key", return_value="secret"),
            patch.object(
                providers,
                "_douyin_web_detail_call",
                side_effect=AssertionError("Web detail must not repeat"),
            ) as web_detail,
            patch.object(
                providers,
                "_douyin_high_quality_video_call",
                return_value=high_quality,
            ) as high_quality_detail,
            patch.object(
                media,
                "process_content_media",
                return_value={"status": "downloaded"},
            ) as process,
        ):
            with self.assertRaisesRegex(
                runner.CampaignError,
                "held pending compensation authorization",
            ):
                resumed.download_one(cid, allow_refresh=True)

        web_detail.assert_not_called()
        high_quality_detail.assert_not_called()
        process.assert_not_called()
        slots = self._rows(
            "SELECT window_key,status,attempt_count FROM fetch_slots "
            "WHERE content_id=? AND stage='media_source_refresh' ORDER BY id",
            (cid,),
        )
        self.assertEqual(
            slots,
            [
                {"window_key": "lifetime", "status": "succeeded", "attempt_count": 1},
                {"window_key": "high-quality", "status": "retryable_failed", "attempt_count": 1},
            ],
        )

    def test_douyin_audio_post_synthesizes_cover_and_audio_after_unbilled_retries(
        self,
    ) -> None:
        audio_url = "https://cdn.example/audio-post.m4a"
        cover_url = "https://cdn.example/audio-post-cover.jpg"
        audio_path = self.root / "audio-post.m4a"
        subprocess.run(
            [
                "ffmpeg", "-y", "-nostdin", "-loglevel", "error",
                "-f", "lavfi", "-i", "sine=frequency=880:duration=1.2",
                "-c:a", "aac", str(audio_path),
            ],
            check=True,
            timeout=30,
        )
        cover = io.BytesIO()
        Image.new("RGB", (32, 24), "black").save(cover, format="JPEG")
        campaign = self.fixture.campaign()
        retry = capture.CaptureError(
            "TikHub HTTP 400",
            retryable=True,
            error_code="provider_retry_requested",
            http_status=400,
            billed=False,
            raw_response={
                "detail": {"message": "Please retry. You won't be charged."}
            },
        )
        unplayable = media.MediaProcessingError(
            "media download failed: candidate 0 was not a playable video"
        )
        with self._ordinary_refresh(), campaign.window():
            cid = self.fixture.content(campaign=campaign)
            content = self.fixture.row(cid)
            self._seed_detail(cid, [audio_url])
            data = {
                **self.fixture.result("detail", content).data,
                "content_type": "video",
                "media_urls": [audio_url],
            }
            audio_post = capture.ProviderResult(
                data,
                {
                    "aweme_detail": {
                        "aweme_id": str(content["platform_content_id"]),
                        "aweme_type": 163,
                        "media_type": 43,
                        "images": None,
                        "desc": data["body"],
                        "author": {
                            "uid": data.get("account_uid", ""),
                            "nickname": data.get("account_name", ""),
                        },
                        "video": {
                            "play_addr": {"url_list": [audio_url]},
                            "cover": {"url_list": [cover_url]},
                        },
                    }
                },
                200,
                True,
            )
            with (
                patch.object(providers, "_load_key", return_value="secret"),
                patch.object(
                    providers, "_douyin_web_detail_call", return_value=audio_post
                ),
                patch.object(
                    providers,
                    "_douyin_high_quality_video_call",
                    side_effect=retry,
                ),
                patch.object(
                    media, "process_content_media", side_effect=unplayable
                ),
            ):
                with self.assertRaises(capture.CaptureError):
                    campaign.download_one(cid, allow_refresh=True)

        evidence_raw_id = self._source(cid)["raw_response_id"]
        usage_before_synthesis = self.fixture.usage()
        provider_call = Mock(side_effect=AssertionError("provider must not repeat"))

        def open_frozen(request, **_kwargs):
            if request.full_url == audio_url:
                return _Response(audio_path.read_bytes(), audio_url, "audio/mp4")
            if request.full_url == cover_url:
                return _Response(cover.getvalue(), cover_url, "image/jpeg")
            raise AssertionError(f"unexpected URL: {request.full_url}")

        def open_without_cover(request, **_kwargs):
            if request.full_url == audio_url:
                return _Response(audio_path.read_bytes(), audio_url, "audio/mp4")
            raise OSError("cover temporarily unavailable")

        interrupted = self.fixture.campaign()
        with interrupted.window(), patch.object(
            providers,
            "_douyin_high_quality_video_call",
            side_effect=provider_call,
        ):
            with self.assertRaisesRegex(
                runner.CampaignError, "audio-post part download failed"
            ):
                interrupted.download_one(
                    cid,
                    allow_refresh=False,
                    urlopen_fn=open_without_cover,
                )
        derived_after_failure = self._rows(
            "SELECT id FROM provider_raw_responses WHERE content_id=? "
            "AND operation='douyin_audio_post_synthesis_derived'",
            (cid,),
        )
        self.assertEqual(len(derived_after_failure), 1)
        self.assertEqual(self.fixture.usage(), usage_before_synthesis)

        final = self.fixture.campaign()
        with final.window(), patch.object(
            providers,
            "_douyin_high_quality_video_call",
            side_effect=provider_call,
        ):
            result = final.download_one(
                cid,
                allow_refresh=False,
                urlopen_fn=open_frozen,
            )

        self.assertEqual(result["status"], "downloaded")
        provider_call.assert_not_called()
        self.assertEqual(self.fixture.usage(), usage_before_synthesis)
        source = self._source(cid)
        self.assertEqual(source["download_slot"]["status"], "succeeded")
        raw = self._rows(
            "SELECT operation FROM provider_raw_responses WHERE id=?",
            (source["raw_response_id"],),
        )
        self.assertEqual(
            raw,
            [{"operation": "douyin_audio_post_synthesis_derived"}],
        )
        self.assertEqual(
            final._raw(source["raw_response_id"])["source_raw_response_id"],
            evidence_raw_id,
        )
        artifact = self._rows(
            "SELECT local_path FROM evidence_artifacts WHERE id=?",
            (source["download_slot"]["output_artifact_id"],),
        )[0]
        artifact_path = media._resolved(artifact["local_path"])
        self.assertTrue(media._valid_media(artifact_path))
        self.assertTrue(final._has_decodable_audio(artifact_path))
        self.assertAlmostEqual(
            final._audio_duration(artifact_path),
            final._audio_duration(audio_path),
            delta=0.25,
        )
        replay = self.fixture.campaign()
        deny_http = Mock(side_effect=AssertionError("durable MP4 was redownloaded"))
        with replay.window():
            replayed = replay.download_one(
                cid,
                allow_refresh=False,
                urlopen_fn=deny_http,
            )
        self.assertIn(replayed["status"], {"downloaded", "evidence_ready"})
        deny_http.assert_not_called()
        self.assertEqual(self.fixture.usage(), usage_before_synthesis)
        self.assertEqual(
            self._rows(
                "SELECT id FROM provider_raw_responses WHERE content_id=? "
                "AND operation='douyin_audio_post_synthesis_derived'",
                (cid,),
            ),
            derived_after_failure,
        )

    def test_douyin_refresh_routing_keeps_http_expiry_and_rejects_other_errors(self) -> None:
        campaign = self.fixture.campaign()
        with self._ordinary_refresh(), campaign.window():
            expired_id = self.fixture.content(campaign=campaign)
            expired_content = self.fixture.row(expired_id)
            self._seed_detail(expired_id, [OLD_VIDEO])
            generic_id = self.fixture.content(campaign=campaign)
            self._seed_detail(generic_id, [OLD_VIDEO])
            expired = media.MediaProcessingError("HTTP Error 403: media URL expired")
            generic = media.MediaProcessingError(
                "media download failed: candidate 0: URLError"
            )
            with (
                patch.object(providers, "_load_key", return_value="secret"),
                patch.object(
                    providers,
                    "_douyin_call",
                    return_value=self._detail(expired_content, [NEW_VIDEO]),
                ) as app_detail,
                patch.object(
                    providers,
                    "_douyin_web_detail_call",
                    side_effect=AssertionError("Web detail used for the wrong error"),
                ) as web_detail,
                patch.object(
                    media,
                    "process_content_media",
                    side_effect=[expired, {"status": "downloaded"}, generic],
                ),
            ):
                campaign.download_one(expired_id, allow_refresh=True)
                with self.assertRaisesRegex(media.MediaProcessingError, "URLError"):
                    campaign.download_one(generic_id, allow_refresh=True)

        app_detail.assert_called_once_with(
            "detail", str(expired_content["platform_content_id"]), "secret"
        )
        web_detail.assert_not_called()
        self.assertNotIn(str(generic_id), campaign.state["refresh_intents"])

    def test_failed_refresh_keeps_intent_and_forbids_second_paid_attempt(self) -> None:
        campaign = self.fixture.campaign()
        failed = capture.CaptureError("temporary provider failure", retryable=True, error_code="provider_unavailable", billed=False)
        paid = Mock(side_effect=failed)
        with self._ordinary_refresh(), campaign.window():
            cid = self.fixture.content("xiaohongshu", campaign=campaign)
            self._seed_detail(cid, [OLD_IMAGE])
            with patch.object(media, "process_content_media", side_effect=media.MediaProcessingError("HTTP 410 gone")):
                with self.assertRaises(capture.CaptureError):
                    campaign.download_one(cid, allow_refresh=True, call_override=paid)
        resumed = self.fixture.campaign()
        with resumed.window(), patch.object(media, "process_content_media") as process:
            with self.assertRaisesRegex(runner.CampaignError, "second purchase forbidden"):
                resumed.download_one(cid, allow_refresh=True, call_override=paid)
            self.assertEqual(resumed.state["refresh_intents"][str(cid)]["status"], "intent")
        paid.assert_called_once()
        process.assert_not_called()

    def test_douyin_web_transport_failure_forbids_automatic_repurchase(self) -> None:
        campaign = self.fixture.campaign()
        transport = capture.CaptureError(
            "connection closed before the response completed",
            retryable=True,
            error_code="transport_error",
            billed=None,
        )
        with self._ordinary_refresh(), campaign.window():
            cid = self.fixture.content(campaign=campaign)
            content = self.fixture.row(cid)
            self._seed_detail(cid, [OLD_VIDEO])
            unplayable = media.MediaProcessingError(
                "media download failed: candidate 0 was not a playable video"
            )
            with (
                patch.object(providers, "_load_key", return_value="secret"),
                patch.object(
                    providers,
                    "_douyin_web_detail_call",
                    side_effect=transport,
                ) as first_web_detail,
                patch.object(
                    media,
                    "process_content_media",
                    side_effect=unplayable,
                ),
            ):
                with self.assertRaises(capture.CaptureError):
                    campaign.download_one(cid, allow_refresh=True)
            first_web_detail.assert_called_once_with(
                str(content["platform_content_id"]), "secret"
            )
        usage_before_resume = self.fixture.usage()
        unknown_before_resume = self._rows(
            "SELECT billed_requests,amount,details_json FROM provider_usage "
            "ORDER BY id DESC LIMIT 1"
        )
        attempts_before_resume = self._rows(
            "SELECT a.billed,a.amount FROM fetch_attempts AS a "
            "JOIN fetch_slots AS s ON s.id=a.slot_id "
            "WHERE s.content_id=? AND s.stage='media_source_refresh'",
            (cid,),
        )
        self.assertEqual(len(unknown_before_resume), 1)
        self.assertEqual(
            json.loads(unknown_before_resume[0]["details_json"])["state"],
            "billing_unknown",
        )
        self.assertEqual(
            (unknown_before_resume[0]["billed_requests"], unknown_before_resume[0]["amount"]),
            (1, 0.001),
        )
        self.assertEqual(attempts_before_resume, [{"billed": 0, "amount": None}])

        resumed = self.fixture.campaign()
        with (
            resumed.window(),
            patch.object(providers, "_load_key", return_value="secret"),
            patch.object(
                providers,
                "_douyin_web_detail_call",
                return_value=self._detail(content, [NEW_VIDEO]),
            ) as resumed_web_detail,
            patch.object(
                media,
                "process_content_media",
                return_value={"status": "downloaded"},
            ) as resumed_process,
        ):
            with self.assertRaisesRegex(
                runner.CampaignError,
                "automatic second purchase forbidden",
            ):
                resumed.download_one(cid, allow_refresh=True)

        resumed_web_detail.assert_not_called()
        resumed_process.assert_not_called()
        self.assertEqual(self._source(cid)["urls"], [OLD_VIDEO])
        refresh = self._rows(
            "SELECT status,attempt_count,last_error_code FROM fetch_slots "
            "WHERE content_id=? AND stage='media_source_refresh'",
            (cid,),
        )
        self.assertEqual(refresh, [{
            "status": "retryable_failed",
            "attempt_count": 1,
            "last_error_code": capture.BILLING_UNKNOWN_SLOT_ERROR,
        }])
        self.assertEqual(self.fixture.usage(), usage_before_resume)
        self.assertEqual(
            self._rows(
                "SELECT billed_requests,amount,details_json FROM provider_usage "
                "ORDER BY id DESC LIMIT 1"
            ),
            unknown_before_resume,
        )
        self.assertEqual(
            self._rows(
                "SELECT a.billed,a.amount FROM fetch_attempts AS a "
                "JOIN fetch_slots AS s ON s.id=a.slot_id "
                "WHERE s.content_id=? AND s.stage='media_source_refresh'",
                (cid,),
            ),
            attempts_before_resume,
        )

    def test_committed_refresh_raw_recovers_storage_failure_without_repurchase(self) -> None:
        campaign = self.fixture.campaign()
        paid = Mock(side_effect=lambda _stage, content: self._detail(content, [NEW_IMAGE]))
        with self._ordinary_refresh(), campaign.window():
            cid = self.fixture.content("xiaohongshu", campaign=campaign)
            original = self._seed_detail(cid, [OLD_IMAGE])
            with patch.object(media, "process_content_media", side_effect=media.MediaProcessingError("HTTP 404 expired")), patch.object(
                providers, "_store_stage_result", side_effect=OSError("fixture disk full after raw commit")
            ):
                with self.assertRaisesRegex(OSError, "disk full"):
                    campaign.download_one(cid, allow_refresh=True, call_override=paid)
            self.assertEqual(self._source(cid)["raw_response_id"], original.raw_response_id)
            self.assertEqual(campaign.state["refresh_intents"][str(cid)]["status"], "intent")
        usage_before_resume = self.fixture.usage()
        raw = capture.load_succeeded_raw_response(content_id=cid, stage="media_source_refresh", window_key="lifetime", db_path=self.db)
        forbidden = Mock(side_effect=AssertionError("committed raw was purchased again"))
        resumed = self.fixture.campaign()
        with resumed.window(), patch.object(media, "process_content_media", return_value={"status": "downloaded"}):
            resumed.download_one(cid, allow_refresh=False, call_override=forbidden)
            self.assertEqual(resumed.state["refresh_intents"][str(cid)]["status"], "succeeded")
        self.assertEqual(self._source(cid)["raw_response_id"], raw.raw_response_id)
        self.assertEqual(self._source(cid)["urls"], [NEW_IMAGE])
        self.assertEqual(self.fixture.usage(), usage_before_resume)
        paid.assert_called_once()
        forbidden.assert_not_called()

    def test_refresh_rejects_same_logical_source_even_with_new_raw_response(self) -> None:
        campaign = self.fixture.campaign()
        paid = Mock(side_effect=lambda _stage, content: self._detail(content, [OLD_IMAGE]))
        with self._ordinary_refresh(), campaign.window():
            cid = self.fixture.content("xiaohongshu", campaign=campaign)
            original = self._seed_detail(cid, [OLD_IMAGE])
            before = self._source(cid)
            with patch.object(media, "process_content_media", side_effect=media.MediaProcessingError("HTTP 403 expired")) as process:
                with self.assertRaisesRegex(runner.CampaignError, "different media source"):
                    campaign.download_one(cid, allow_refresh=True, call_override=paid)
            process.assert_called_once()
            after = self._source(cid)
            self.assertNotEqual(after["raw_response_id"], original.raw_response_id)
            self.assertEqual(after["source_sha256"], before["source_sha256"])
            raw_rows = self._rows(
                "SELECT local_path,sha256,byte_size FROM provider_raw_responses WHERE content_id=? ORDER BY id", (cid,)
            )
            self.assertEqual(len(raw_rows), 2)
            self.assertNotEqual(raw_rows[0]["local_path"], raw_rows[1]["local_path"])
            self.assertEqual(raw_rows[0]["sha256"], raw_rows[1]["sha256"])
            self.assertEqual(raw_rows[0]["byte_size"], raw_rows[1]["byte_size"])
            self.assertEqual(Path(raw_rows[0]["local_path"]).read_bytes(), Path(raw_rows[1]["local_path"]).read_bytes())
            with patch.object(media, "process_content_media") as retry:
                with self.assertRaisesRegex(runner.CampaignError, "different media source"):
                    campaign.download_one(cid, allow_refresh=True, call_override=paid)
            retry.assert_not_called()
        paid.assert_called_once()

    def test_disk_or_missing_frozen_groups_never_trigger_paid_refresh(self) -> None:
        campaign = self.fixture.campaign()
        paid = Mock(side_effect=AssertionError("non-expiry failure triggered paid refresh"))
        with campaign.window():
            xhs = self.fixture.content("xiaohongshu", campaign=campaign)
            self._seed_detail(xhs, [OLD_IMAGE])
            usage = self.fixture.usage()
            with patch.object(media, "process_content_media", side_effect=OSError("No space left on device")):
                with self.assertRaises(OSError):
                    campaign.download_one(xhs, allow_refresh=True, call_override=paid)
            self.assertEqual(self.fixture.usage(), usage)
            dy = self.fixture.content(campaign=campaign)
            with connect(self.db) as connection:
                connection.execute("UPDATE content_items SET content_type='image' WHERE id=?", (dy,))
                connection.commit()
            self._seed_detail(dy, ["https://p3-sign.douyinpic.com/no-groups.jpg"])
            usage = self.fixture.usage()
            with patch.object(media, "process_content_media") as process:
                with self.assertRaisesRegex(runner.CampaignError, "frozen raw images"):
                    campaign.download_one(dy, allow_refresh=True, call_override=paid)
            process.assert_not_called()
            self.assertEqual(self.fixture.usage(), usage)
            self.assertEqual(campaign.state["refresh_intents"], {})
        paid.assert_not_called()

    def test_evaluation_pending_skips_media_and_orders_scoped_fingerprint_before_clear(self) -> None:
        campaign = self.fixture.campaign()
        calls = []
        with campaign.window():
            cid = self.fixture.content(campaign=campaign)

            def evaluate(content_id, **kwargs):
                self.assertEqual(content_id, cid)
                self.assertEqual(kwargs, {"db_path": self.db, "expected_active_release_id": runner.RELEASE_ID})
                calls.append("evaluate")
                return SimpleNamespace(evaluation_id=71, created=True)

            def fingerprint(**kwargs):
                self.assertEqual(kwargs, {"scope_content_ids": [cid], "limit": 1, "db_path": self.db})
                calls.append("fingerprint")
                return self._fp_result(cid)

            def clear(**kwargs):
                self.assertEqual(kwargs, {"db_path": self.db, "release_id": runner.RELEASE_ID, "content_id": cid})
                calls.append("clear")
                return True

            with patch.object(rb, "_pinned_media_terminal_detail", side_effect=[
                MediaTerminalDetail("pending", "evaluation_pending"),
                MediaTerminalDetail("pending", "evaluation_pending"),
                MediaTerminalDetail("pending", "evaluation_pending"),
                MediaTerminalDetail("complete", "complete"),
            ]), patch.object(media, "process_content_media") as process, patch.object(
                evaluation, "evaluate_content", side_effect=evaluate
            ), patch.object(duplicates, "run_duplicate_fingerprint_queue", side_effect=fingerprint), patch.object(
                rb, "_release_history_backfill_tag", side_effect=clear
            ):
                result = campaign.local_one(cid)
            process.assert_not_called()
            self.assertEqual(result["status"], "complete")
            self.assertEqual(calls, ["evaluate", "fingerprint", "clear"])

    def test_provider_unavailable_uses_weak_evaluation_and_clears_without_fingerprint(
        self,
    ) -> None:
        campaign = self.fixture.campaign()
        calls = []
        with campaign.window():
            cid = self.fixture.content(campaign=campaign)

            def evaluate(content_id, **kwargs):
                self.assertEqual(content_id, cid)
                self.assertEqual(
                    kwargs,
                    {
                        "db_path": self.db,
                        "expected_active_release_id": runner.RELEASE_ID,
                    },
                )
                calls.append("evaluate")
                return SimpleNamespace(evaluation_id=81, created=True)

            def clear(**kwargs):
                self.assertEqual(
                    kwargs,
                    {
                        "db_path": self.db,
                        "release_id": runner.RELEASE_ID,
                        "content_id": cid,
                    },
                )
                calls.append("clear")
                return True

            with (
                patch.object(
                    rb,
                    "_pinned_media_terminal_detail",
                    side_effect=[
                        MediaTerminalDetail("pending", "source_missing"),
                        MediaTerminalDetail("pending", "source_missing"),
                        MediaTerminalDetail("pending", "source_missing"),
                        MediaTerminalDetail(
                            "terminal_insufficient", "terminal_insufficient"
                        ),
                    ],
                ),
                patch.object(
                    rb,
                    "provider_terminal_unavailable_content_ids",
                    return_value={cid},
                ),
                patch.object(media, "process_content_media") as process,
                patch.object(evaluation, "evaluate_content", side_effect=evaluate),
                patch.object(
                    duplicates, "run_duplicate_fingerprint_queue"
                ) as fingerprint,
                patch.object(
                    rb, "_release_history_backfill_tag", side_effect=clear
                ),
            ):
                result = campaign.local_one(cid)
            process.assert_not_called()
            fingerprint.assert_not_called()
            self.assertEqual(result["status"], "terminal_insufficient")
            self.assertEqual(calls, ["evaluate", "clear"])

    def test_incomplete_local_items_remain_tagged_and_do_not_block_next_item(self) -> None:
        campaign = self.fixture.campaign()
        with campaign.window():
            failed = self.fixture.content(campaign=campaign)
            waiting = self.fixture.content(campaign=campaign)
            states = {
                failed: MediaTerminalDetail("terminal_failed", "download_terminal_failed"),
                waiting: MediaTerminalDetail("pending", "download_pending"),
            }
            with patch.object(rb, "_pinned_media_terminal_detail", side_effect=lambda **kw: states[kw["content_id"]]), patch.object(
                rb, "_release_history_backfill_tag"
            ) as clear, patch.object(evaluation, "evaluate_content") as evaluate, patch.object(
                duplicates, "run_duplicate_fingerprint_queue"
            ) as fingerprint:
                result = campaign.run_phase("local", limit=2, max_seconds=30)
            self.assertEqual(result["processed"], 2)
            self.assertEqual(result["remaining"], 2)
            self.assertEqual(campaign.state["results"]["local"][str(failed)]["status"], "terminal_failed")
            self.assertEqual(campaign.state["results"]["local"][str(waiting)]["status"], "download_pending")
        clear.assert_not_called()
        evaluate.assert_not_called()
        fingerprint.assert_not_called()
        self.assertEqual([self.fixture.row(cid)["source_group"] for cid in (failed, waiting)], ["history-backfill"] * 2)

    def test_local_prioritizes_repair_ready_content(self) -> None:
        campaign = self.fixture.campaign()
        with campaign.window():
            unready = self.fixture.content(campaign=campaign)
            repaired = self.fixture.content(campaign=campaign)
            campaign.state["results"].setdefault("local", {})[str(repaired)] = {
                "content_id": repaired,
                "status": "repair_ready",
            }
            campaign._save()
            prepared = {"content_id": repaired, "status": "ready"}
            with patch.object(
                campaign,
                "_prepare_local_media_one",
                return_value=prepared,
            ) as prepare, patch.object(
                campaign,
                "_finalize_local_group",
                return_value={
                    repaired: {"content_id": repaired, "status": "complete"}
                },
            ) as finalize:
                result = campaign.run_phase("local", limit=1, max_seconds=30)
        self.assertEqual(result["processed"], 1)
        prepare.assert_called_once_with(
            repaired, whisper_model_path=None, ocr_binary=None
        )
        finalize.assert_called_once_with({repaired: prepared})
        self.assertEqual(
            campaign.state["results"]["local"][str(repaired)]["status"],
            "complete",
        )
        self.assertNotIn(str(unready), campaign.state["results"]["local"])

    def test_local_prioritizes_downloaded_work_before_uncaptured_content(self) -> None:
        campaign = self.fixture.campaign()
        with campaign.window():
            uncaptured = self.fixture.content(campaign=campaign)
            downloaded = self.fixture.content(campaign=campaign)
            terminal = {
                uncaptured: MediaTerminalDetail("pending", "source_missing"),
                downloaded: MediaTerminalDetail("pending", "frames_pending"),
            }
            prepared = {"content_id": downloaded, "status": "ready"}
            model_path = self.root / "whisper-model"
            ocr_path = self.root / "vision-ocr"
            with (
                patch.object(
                    rb,
                    "media_terminal_state_details",
                    return_value=terminal,
                ),
                patch.object(
                    campaign,
                    "_prepare_local_media_one",
                    return_value=prepared,
                ) as prepare,
                patch.object(
                    campaign,
                    "_finalize_local_group",
                    return_value={
                        downloaded: {
                            "content_id": downloaded,
                            "status": "complete",
                        }
                    },
                ) as finalize,
                patch.object(
                    media, "pinned_whisper_model_path", return_value=model_path
                ) as pinned_model,
                patch.object(media, "ocr_binary_path", return_value=ocr_path),
            ):
                result = campaign.run_phase("local", limit=1, max_seconds=30)
        self.assertEqual(result["processed"], 1)
        pinned_model.assert_called_once_with(local_files_only=True)
        prepare.assert_called_once_with(
            downloaded,
            whisper_model_path=model_path,
            ocr_binary=ocr_path,
        )
        finalize.assert_called_once_with({downloaded: prepared})
        self.assertEqual(
            campaign.state["results"]["local"][str(downloaded)]["status"],
            "complete",
        )
        self.assertNotIn(
            str(uncaptured), campaign.state["results"]["local"]
        )

    def test_local_prepares_groups_then_finalizes_serially_on_caller(self) -> None:
        campaign = self.fixture.campaign()
        main_thread = threading.get_ident()
        group_barrier = threading.Barrier(runner.LOCAL_MEDIA_WORKERS)
        guard = threading.Lock()
        active = 0
        peak = 0
        prepared_ids: list[int] = []
        finalized_ids: list[int] = []

        def prepare(
            content_id: int,
            *,
            whisper_model_path: Path | None,
            ocr_binary: Path | None,
        ) -> dict[str, object]:
            nonlocal active, peak
            self.assertIsNone(whisper_model_path)
            self.assertIsNone(ocr_binary)
            with guard:
                active += 1
                peak = max(peak, active)
                prepared_ids.append(content_id)
            try:
                group_barrier.wait(timeout=2)
                return {"content_id": content_id, "status": "ready"}
            finally:
                with guard:
                    active -= 1

        def finalize(
            prepared: dict[int, dict[str, object]],
        ) -> dict[int, dict[str, object]]:
            self.assertEqual(threading.get_ident(), main_thread)
            with guard:
                self.assertEqual(active, 0)
            finalized_ids.extend(sorted(prepared))
            return {
                content_id: {"content_id": content_id, "status": "complete"}
                for content_id in prepared
            }

        with campaign.window():
            content_count = runner.LOCAL_MEDIA_WORKERS * 2
            content_ids = [
                self.fixture.content(campaign=campaign)
                for _ in range(content_count)
            ]
            with patch.object(
                campaign, "_prepare_local_media_one", side_effect=prepare
            ), patch.object(
                campaign, "_finalize_local_group", side_effect=finalize
            ):
                result = campaign.run_phase(
                    "local", limit=content_count, max_seconds=30
                )

        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["processed"], content_count)
        self.assertEqual(result["processed_content_ids"], sorted(content_ids))
        self.assertEqual(set(prepared_ids), set(content_ids))
        self.assertEqual(finalized_ids, sorted(content_ids))
        self.assertEqual(peak, runner.LOCAL_MEDIA_WORKERS)

    def test_local_group_batches_fingerprints_and_keeps_failed_debt_and_tag(self) -> None:
        campaign = self.fixture.campaign()
        with campaign.window():
            succeeded = self.fixture.content(campaign=campaign)
            failed = self.fixture.content(campaign=campaign)
            terminal_calls = {succeeded: 0, failed: 0}

            def terminal(**kwargs):
                content_id = kwargs["content_id"]
                terminal_calls[content_id] += 1
                return (
                    MediaTerminalDetail("pending", "evaluation_pending")
                    if terminal_calls[content_id] == 1
                    else MediaTerminalDetail("complete", "complete")
                )

            def fingerprint(**kwargs):
                saved = json.loads(campaign.state_path.read_text())
                self.assertEqual(
                    saved["relation_pending_ids"], [succeeded, failed]
                )
                self.assertEqual(
                    kwargs,
                    {
                        "scope_content_ids": [succeeded, failed],
                        "limit": 2,
                        "db_path": self.db,
                    },
                )
                return {
                    "failed": 1,
                    "failures": [{"content_id": failed, "error": "fixture"}],
                    "has_more": True,
                    "calibration_ready": True,
                    "fingerprinted_content_ids": [succeeded],
                }

            with patch.object(
                rb, "_pinned_media_terminal_detail", side_effect=terminal
            ), patch.object(
                evaluation,
                "evaluate_content",
                side_effect=lambda cid, **_kwargs: SimpleNamespace(
                    evaluation_id=cid + 100, created=True
                ),
            ) as evaluate, patch.object(
                duplicates,
                "run_duplicate_fingerprint_queue",
                side_effect=fingerprint,
            ) as fingerprint_queue, patch.object(
                campaign,
                "repair_relations",
                return_value={"restored": 0, "pending": 1},
            ), patch.object(
                rb, "_release_history_backfill_tag", return_value=True
            ) as clear:
                results = campaign._finalize_local_group({
                    succeeded: {"content_id": succeeded, "status": "ready"},
                    failed: {"content_id": failed, "status": "ready"},
                })

        self.assertEqual(evaluate.call_count, 2)
        fingerprint_queue.assert_called_once()
        clear.assert_called_once_with(
            db_path=self.db,
            release_id=runner.RELEASE_ID,
            content_id=succeeded,
        )
        self.assertEqual(results[succeeded]["status"], "complete")
        self.assertEqual(results[failed]["status"], "failed")
        self.assertEqual(campaign.state["relation_pending_ids"], [failed])
        self.assertEqual(
            json.loads(campaign.state_path.read_text())["relation_pending_ids"],
            [failed],
        )

    def test_local_group_repairs_already_current_fingerprint_before_clear(self) -> None:
        campaign = self.fixture.campaign()
        with campaign.window():
            content_id = self.fixture.content(campaign=campaign)

            def repair():
                self.assertEqual(
                    campaign.state["relation_pending_ids"], [content_id]
                )
                campaign.state["relation_pending_ids"] = []
                campaign._save()
                return {"restored": 1, "pending": 0}

            with patch.object(
                rb,
                "_pinned_media_terminal_detail",
                return_value=MediaTerminalDetail("complete", "complete"),
            ), patch.object(
                duplicates,
                "run_duplicate_fingerprint_queue",
                return_value={
                    "failed": 0,
                    "failures": [],
                    "has_more": False,
                    "calibration_ready": True,
                    "fingerprinted_content_ids": [],
                },
            ) as fingerprint_queue, patch.object(
                campaign, "repair_relations", side_effect=repair
            ) as repair_relations, patch.object(
                rb, "_release_history_backfill_tag", return_value=True
            ) as clear:
                result = campaign._finalize_local_group({
                    content_id: {"content_id": content_id, "status": "ready"}
                })[content_id]

        fingerprint_queue.assert_called_once_with(
            scope_content_ids=[content_id], limit=1, db_path=self.db
        )
        repair_relations.assert_called_once_with()
        clear.assert_called_once()
        self.assertEqual(result["status"], "complete")
        self.assertEqual(campaign.state["relation_pending_ids"], [])

    def test_local_group_restores_durable_debt_when_relation_repair_fails(self) -> None:
        campaign = self.fixture.campaign()
        with campaign.window():
            content_id = self.fixture.content(campaign=campaign)
            with patch.object(
                rb,
                "_pinned_media_terminal_detail",
                return_value=MediaTerminalDetail("complete", "complete"),
            ), patch.object(
                duplicates,
                "run_duplicate_fingerprint_queue",
                return_value={
                    "failed": 0,
                    "failures": [],
                    "has_more": False,
                    "calibration_ready": True,
                    "fingerprinted_content_ids": [content_id],
                },
            ), patch.object(
                campaign,
                "repair_relations",
                side_effect=RuntimeError("fixture relation repair failed"),
            ), patch.object(
                rb, "_release_history_backfill_tag", return_value=True
            ) as clear:
                result = campaign._finalize_local_group({
                    content_id: {"content_id": content_id, "status": "ready"}
                })[content_id]

        clear.assert_not_called()
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["error_code"], "RuntimeError")
        self.assertIn("relation repair failed", result["error"])
        self.assertEqual(campaign.state["relation_pending_ids"], [content_id])
        self.assertEqual(
            json.loads(campaign.state_path.read_text())["relation_pending_ids"],
            [content_id],
        )

    def test_release_change_after_fingerprinting_cannot_clear_history_tag(self) -> None:
        campaign = self.fixture.campaign()
        with campaign.window():
            cid = self.fixture.content(campaign=campaign)

            def switch_release(**_kwargs):
                with connect(self.db) as connection:
                    connection.execute("UPDATE evaluation_releases SET status='retired' WHERE id=?", (runner.RELEASE_ID,))
                    connection.commit()
                return self._fp_result(cid)

            with patch.object(rb, "_pinned_media_terminal_detail", return_value=MediaTerminalDetail("complete", "complete")), patch.object(
                duplicates, "run_duplicate_fingerprint_queue", side_effect=switch_release
            ):
                with self.assertRaises(rb.RangeBackfillError):
                    campaign.local_one(cid)
            self.assertEqual(self.fixture.row(cid)["source_group"], "history-backfill")

    def test_visited_existing_relations_stay_idempotent_without_refingerprinting(self) -> None:
        left = self.fixture.content()
        right = self.fixture.content()
        for cid in (left, right):
            with connect(self.db) as connection:
                connection.execute("UPDATE content_items SET title=?,body=? WHERE id=?", (OCR_TEXT, OCR_TEXT, cid))
                connection.commit()
            evaluation.evaluate_content(cid, db_path=self.db, expected_active_release_id=runner.RELEASE_ID)
            duplicates.fingerprint_content(cid, db_path=self.db)
        self._seed_calibration()
        duplicates.update_duplicate_relations_incremental([left, right], db_path=self.db)
        before_fp = self._rows("SELECT * FROM duplicate_fingerprints ORDER BY id")
        before_evaluations = self._rows("SELECT * FROM evaluation_versions ORDER BY id")
        self.assertEqual(len(before_evaluations), 2)
        self.assertEqual(len(self._rows("SELECT * FROM duplicate_relations WHERE method='fingerprint_v1'")), 1)
        campaign = self.fixture.campaign()
        with campaign.window():
            existing = self.fixture.row(left)
            page = {"items": [existing], "has_more": False, "next_cursor": 0}
            operation = "douyin_user_posts"
            budget = providers.ensure_task_budget(
                provider="TikHub", operation=operation, price=providers.TIKHUB_PRICE,
                task_id=runner.TASK_ID, task_max_amount=runner.MAX_AMOUNT, db_path=self.db,
            )
            # Pre-existing discovery raw belongs to an ordinary reconcile
            # capture.  The subsequent history workflow must replay it locally.
            with provider_budget.paid_scope("reconcile"):
                with connect(self.db) as connection:
                    identity_id = int(
                        connection.execute(
                            "SELECT id FROM account_platform_identities WHERE account_id=?",
                            (existing["account_id"],),
                        ).fetchone()[0]
                    )
                    connection.execute(
                        """INSERT OR REPLACE INTO account_provider_references(
                               account_identity_id,provider,reference_kind,reference_value,
                               created_at,updated_at
                           ) VALUES (?,'TikHub','sec_user_id',?,?,?)""",
                        (
                            identity_id,
                            fixture_module.UIDS["douyin"],
                            now_utc(),
                            now_utc(),
                        ),
                    )
                    connection.commit()
                raw = capture.execute_account_fetch(
                    account_id=existing["account_id"], stage="discovery",
                    window_key="fixture:page:0", provider="TikHub",
                    adapter_version="tikhub-discovery-v8.0", operation=operation,
                    call=lambda: capture.ProviderResult(page, {"data": page}, 200, False),
                    db_path=self.db, raw_root=self.fixture.raw_root,
                    budget_id=budget, task_id=runner.TASK_ID, task_max_amount=runner.MAX_AMOUNT,
                    paid_request_identity=providers._paid_request_identity(
                        operation=operation,
                        platform="douyin",
                        subject=fixture_module.UIDS["douyin"],
                        params={
                            "sec_user_id": fixture_module.UIDS["douyin"],
                            "max_cursor": 0,
                            "count": 20,
                            "sort_type": 0,
                        },
                        cursor=None,
                        due_bucket="fixture:page:0",
                    ),
                )
            with patch.object(providers, "upsert_content", wraps=upsert_content) as upsert:
                materialized = providers.materialize_account_discovery_page(
                    account_id=existing["account_id"], platform="douyin",
                    account_uid=fixture_module.UIDS["douyin"], page=page,
                    source_raw_response_id=raw.raw_response_id,
                    metrics_window_key="2026-08-29", discovery_operation=operation,
                    provider="TikHub", derived_adapter_version="tikhub-discovery-derived-v8.1",
                    derived_operations={"detail": "fixture-detail-derived", "metrics": "fixture-metrics-derived"},
                    zero_view_is_authoritative=False, db_path=self.db,
                    materialize_existing_stages=False,
                )
            upsert.assert_called_once()
            self.assertEqual(materialized["content_changes"], [{"content_id": left, "action": "updated"}])
            self.assertEqual(materialized["derived_stages"]["created"], 0)
            self.assertEqual(materialized["derived_stages"]["skipped"], 2)
            self.assertEqual(
                len(self._rows("SELECT * FROM duplicate_relations WHERE method='fingerprint_v1'")),
                1,
            )
            with patch.object(duplicates, "fingerprint_content") as fingerprint:
                skipped = duplicates.run_duplicate_fingerprint_queue(scope_content_ids=[left], limit=1, db_path=self.db)
            fingerprint.assert_not_called()
            self.assertEqual(skipped["candidates"], 0)
            self.assertEqual(skipped["fingerprinted_content_ids"], [])
            self.assertIsNone(skipped["relations"])
            self.assertEqual(
                len(self._rows("SELECT * FROM duplicate_relations WHERE method='fingerprint_v1'")),
                1,
            )
            campaign.day_root.mkdir(parents=True, exist_ok=True)
            manifest = campaign.day_root / f"{runner.TASK_ID}.contents.json"
            manifest.write_text(json.dumps({
                "task_id": runner.TASK_ID, "start": rb._iso(runner.START), "end": rb._iso(runner.END),
                "contents": [{"content_id": left, "first_action": "updated"}],
            }))
            campaign._merge_manifests()
            self.assertEqual(campaign.state["visited_ids"], [left])
            self.assertEqual(campaign.state["relation_pending_ids"], [left])
            with patch.object(duplicates, "fingerprint_content") as fingerprint, patch.object(
                duplicates, "update_duplicate_relations_incremental", wraps=duplicates.update_duplicate_relations_incremental
            ) as incremental, patch.object(media, "process_content_media") as process, patch.object(
                evaluation, "evaluate_content"
            ) as evaluate:
                result = campaign.repair_relations()
            incremental.assert_called_once_with([left], db_path=self.db)
            fingerprint.assert_not_called()
            process.assert_not_called()
            evaluate.assert_not_called()
            self.assertEqual(result["restored"], 1)
            self.assertEqual(campaign.state["relation_pending_ids"], [])
            self.assertEqual(campaign.state["new_ids"], [])
        self.assertEqual(self._rows("SELECT * FROM duplicate_fingerprints ORDER BY id"), before_fp)
        self.assertEqual(self._rows("SELECT * FROM evaluation_versions ORDER BY id"), before_evaluations)
        self.assertEqual(len(self._rows("SELECT * FROM duplicate_relations WHERE method='fingerprint_v1'")), 1)
        self.assertEqual(self.fixture.usage()[1], 0.0)

    def test_offline_image_download_real_evaluation_fingerprint_and_clear_are_idempotent(self) -> None:
        self._seed_calibration()
        image_buffer = io.BytesIO()
        Image.new("RGB", (32, 24), "#327abc").save(image_buffer, format="JPEG")
        image_bytes = image_buffer.getvalue()
        http = Mock(side_effect=lambda *_args, **_kwargs: _Response(image_bytes, OLD_IMAGE, "image/jpeg"))
        paid = Mock(side_effect=lambda stage, content: self._detail(content, [OLD_IMAGE]))
        campaign = self.fixture.campaign()
        calls = []

        def ocr(_manifest: Path, target: Path, **kwargs):
            self.assertEqual(len(kwargs["validated_frame_paths"]), 1)
            media._atomic_json(target, {
                "status": "success", "processor_version": media.processor_versions()["ocr"],
                "source_count": 1, "ocr_observation_count": 1,
                "combined_text": OCR_TEXT, "observations": [{"text": OCR_TEXT}],
            })
            return target

        real_fp = duplicates.run_duplicate_fingerprint_queue
        real_clear = rb._release_history_backfill_tag

        def fingerprint(**kwargs):
            self.assertEqual(kwargs["scope_content_ids"], [cid])
            calls.append("fingerprint")
            return real_fp(**kwargs)

        def clear(**kwargs):
            calls.append("clear")
            return real_clear(**kwargs)

        with self.fixture.ordinary_processors(content=True), campaign.window(), patch.object(
            capture, "now_utc", return_value=fixture_module.RAW_CAPTURED_AT
        ), patch.object(
            providers, "now_utc", return_value=fixture_module.RAW_CAPTURED_AT
        ):
            cid = self.fixture.content("xiaohongshu", campaign=campaign)
            campaign.capture_one(cid, call_override=paid)
            downloaded = campaign.download_one(cid, urlopen_fn=http)
            self.assertEqual(downloaded["status"], "downloaded")
            usage = self.fixture.usage()
            with patch.object(media, "_run_ocr", side_effect=ocr) as run_ocr, patch.object(
                duplicates, "run_duplicate_fingerprint_queue", side_effect=fingerprint
            ), patch.object(rb, "_release_history_backfill_tag", side_effect=clear), patch.object(
                evaluation, "evaluate_content", wraps=evaluation.evaluate_content
            ) as evaluate:
                result = campaign.local_one(cid)
                self.assertEqual(result["status"], "complete")
                self.assertTrue(result["backfill_tag_cleared"])
                again = campaign.local_one(cid)
            run_ocr.assert_called_once()
            evaluate.assert_called_once_with(cid, db_path=self.db, expected_active_release_id=runner.RELEASE_ID)
            self.assertEqual(calls, ["fingerprint", "clear", "fingerprint", "clear"])
            self.assertEqual(again["status"], "complete")
            self.assertFalse(again["backfill_tag_cleared"])
            self.assertEqual(campaign.state["relation_pending_ids"], [])
            self.assertEqual(self.fixture.row(cid)["source_group"], "")
            self.assertEqual(self.fixture.usage(), usage)
        paid.assert_called_once()
        http.assert_called_once()
        self.assertEqual(len(self._rows("SELECT * FROM duplicate_fingerprints WHERE content_id=?", (cid,))), 1)
        self.assertEqual(len(self._rows("SELECT * FROM evaluation_versions WHERE content_id=? AND release_id=?", (cid, runner.RELEASE_ID))), 1)
        self.assertEqual(len(self.fixture.metrics(cid)), 1)
        self.fixture.assert_no_comments()
        for artifact in self._rows("SELECT local_path FROM evidence_artifacts WHERE content_id=?", (cid,)):
            self.assertTrue(Path(artifact["local_path"]).is_relative_to(self.root))


if __name__ == "__main__":
    unittest.main()
