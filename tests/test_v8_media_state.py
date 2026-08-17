from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from v8 import media
from v8.media_state import (
    MediaStateError,
    MediaTerminalDetail,
    media_terminal_state_details,
    media_terminal_states,
)
from v8.storage import connect, initialize_database


class V8MediaTerminalStateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "media-state.sqlite3"
        self.release_id = "evaluation-test__selling-points-test"
        with connect(self.db) as connection:
            initialize_database(connection)
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id,version,status,definition,created_at,published_at
                ) VALUES ('taxonomy-test','selling-points-test','published','{}',?,?)
                """,
                (self._timestamp(0), self._timestamp(0)),
            )
            connection.execute(
                """
                INSERT INTO evaluation_releases(
                    id,rule_version,taxonomy_version,matcher_rule_sha256,status,
                    created_at,updated_at,activated_at
                ) VALUES (?,'evaluation-test','selling-points-test',?,'active',?,?,?)
                """,
                (
                    self.release_id,
                    "a" * 64,
                    self._timestamp(0),
                    self._timestamp(0),
                    self._timestamp(0),
                ),
            )
            connection.commit()

    def tearDown(self) -> None:
        self.temp.cleanup()

    @staticmethod
    def _timestamp(index: int) -> str:
        return f"2026-08-15T00:{index:02d}:00Z"

    @staticmethod
    def _sha(label: str) -> str:
        return hashlib.sha256(label.encode("utf-8")).hexdigest()

    def _content(
        self,
        connection,
        *,
        content_type: str = "video",
        platform: str = "douyin",
    ) -> int:
        next_id = int(
            connection.execute("SELECT COUNT(*) + 1 FROM content_items").fetchone()[0]
        )
        cursor = connection.execute(
            """
            INSERT INTO content_items(
                link_id,platform,platform_content_id,canonical_url,content_type,
                imported_at,created_at,updated_at
            ) VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                f"C{next_id:05d}",
                platform,
                str(next_id),
                f"https://example.test/content/{next_id}",
                content_type,
                self._timestamp(0),
                self._timestamp(0),
                self._timestamp(0),
            ),
        )
        assert cursor.lastrowid is not None
        return int(cursor.lastrowid)

    def _source(
        self,
        connection,
        content_id: int,
        *,
        media_kind: str,
        suffix: str,
    ) -> tuple[str, list[str]]:
        urls = [f"https://media.example.test/{content_id}/{suffix}.{media_kind}"]
        normalized_urls, source_sha256 = media._media_source_identity(media_kind, urls)
        raw_response_id = content_id * 100 + len(suffix)
        payload = {
            "schema_version": media.MEDIA_SOURCE_VERSION,
            "media_kind": media_kind,
            "urls": normalized_urls,
            "source_sha256": source_sha256,
            "raw_response_id": raw_response_id,
            "captured_at": self._timestamp(1),
        }
        path = self.root / f"source-{content_id}-{suffix}.json"
        path.write_text(
            json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        metadata = {
            "media_kind": media_kind,
            "source_count": len(normalized_urls),
            "source_sha256": source_sha256,
            "raw_response_id": raw_response_id,
        }
        connection.execute(
            """
            INSERT INTO evidence_artifacts(
                content_id,artifact_type,local_path,status,byte_size,sha256,
                captured_at,processor_version,metadata_json,created_at
            ) VALUES (?,'media_source',?,'available',?,?,?,?,?,?)
            """,
            (
                content_id,
                str(path),
                path.stat().st_size,
                media.file_sha256(path),
                self._timestamp(1),
                media.MEDIA_SOURCE_VERSION,
                json.dumps(metadata, ensure_ascii=False, sort_keys=True),
                self._timestamp(1),
            ),
        )
        return source_sha256, normalized_urls

    def _artifact(
        self,
        connection,
        content_id: int,
        *,
        artifact_type: str,
        processor_version: str,
        label: str,
        metadata: dict[str, object] | None = None,
    ) -> tuple[int, str]:
        sha256 = self._sha(f"{content_id}:{label}")
        cursor = connection.execute(
            """
            INSERT INTO evidence_artifacts(
                content_id,artifact_type,local_path,status,byte_size,sha256,
                captured_at,processor_version,metadata_json,created_at
            ) VALUES (?,?,?,'available',128,?,?,?,?,?)
            """,
            (
                content_id,
                artifact_type,
                str(self.root / f"{content_id}-{label}.artifact"),
                sha256,
                self._timestamp(2),
                processor_version,
                json.dumps(metadata or {}, sort_keys=True),
                self._timestamp(2),
            ),
        )
        assert cursor.lastrowid is not None
        return int(cursor.lastrowid), sha256

    def _slot(
        self,
        connection,
        content_id: int,
        *,
        source_sha256: str,
        processor_type: str,
        processor_version: str,
        status: str,
        output_artifact_id: int | None = None,
        attempt_count: int = 1,
    ) -> None:
        connection.execute(
            """
            INSERT INTO media_processing_slots(
                content_id,source_sha256,processor_type,processor_version,status,
                output_artifact_id,attempt_count,error_message,created_at,updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                content_id,
                source_sha256,
                processor_type,
                processor_version,
                status,
                output_artifact_id,
                attempt_count,
                "fixture failure" if status.endswith("failed") else None,
                self._timestamp(2),
                self._timestamp(2),
            ),
        )

    def _video_dag(
        self,
        connection,
        content_id: int,
        *,
        source_sha256: str,
        download_version: str = media.VIDEO_DOWNLOAD_VERSION,
    ) -> tuple[str, str, str]:
        versions = media.processor_versions()
        media_id, media_sha256 = self._artifact(
            connection,
            content_id,
            artifact_type="media",
            processor_version=download_version,
            label="media",
        )
        self._slot(
            connection,
            content_id,
            source_sha256=source_sha256,
            processor_type="download",
            processor_version=download_version,
            status="succeeded",
            output_artifact_id=media_id,
        )
        frames_id, frames_sha256 = self._artifact(
            connection,
            content_id,
            artifact_type="frames_manifest",
            processor_version=versions["frames"],
            label="frames",
        )
        self._slot(
            connection,
            content_id,
            source_sha256=media_sha256,
            processor_type="frames",
            processor_version=versions["frames"],
            status="succeeded",
            output_artifact_id=frames_id,
        )
        asr_id, asr_sha256 = self._artifact(
            connection,
            content_id,
            artifact_type="asr",
            processor_version=versions["asr"],
            label="asr",
        )
        self._slot(
            connection,
            content_id,
            source_sha256=media_sha256,
            processor_type="asr",
            processor_version=versions["asr"],
            status="succeeded",
            output_artifact_id=asr_id,
        )
        ocr_id, ocr_sha256 = self._artifact(
            connection,
            content_id,
            artifact_type="ocr",
            processor_version=versions["ocr"],
            label="ocr",
        )
        self._slot(
            connection,
            content_id,
            source_sha256=frames_sha256,
            processor_type="ocr",
            processor_version=versions["ocr"],
            status="succeeded",
            output_artifact_id=ocr_id,
        )
        return media_sha256, asr_sha256, ocr_sha256

    def _image_dag(
        self,
        connection,
        content_id: int,
        *,
        source_sha256: str,
        urls: list[str],
    ) -> tuple[str, None, str]:
        versions = media.processor_versions()
        groups = media.image_source_groups(urls, platform="xiaohongshu")
        binding_sha256 = media.image_download_binding_sha256(
            source_sha256, media.image_groups_sha256(groups)
        )
        manifest_id, manifest_sha256 = self._artifact(
            connection,
            content_id,
            artifact_type="media_manifest",
            processor_version=media.IMAGE_DOWNLOAD_VERSION,
            label="media-manifest",
        )
        self._slot(
            connection,
            content_id,
            source_sha256=binding_sha256,
            processor_type="download",
            processor_version=media.IMAGE_DOWNLOAD_VERSION,
            status="succeeded",
            output_artifact_id=manifest_id,
        )
        ocr_id, ocr_sha256 = self._artifact(
            connection,
            content_id,
            artifact_type="ocr",
            processor_version=versions["ocr"],
            label="image-ocr",
        )
        self._slot(
            connection,
            content_id,
            source_sha256=manifest_sha256,
            processor_type="ocr",
            processor_version=versions["ocr"],
            status="succeeded",
            output_artifact_id=ocr_id,
        )
        return manifest_sha256, None, ocr_sha256

    def _evaluation(
        self,
        connection,
        content_id: int,
        *,
        media_sha256: str,
        asr_sha256: str | None,
        ocr_sha256: str,
        evaluation_status: str,
        evidence_level: str,
        suffix: str = "current",
        pending_review: int = 0,
        evaluation_source: str = "automatic",
    ) -> None:
        evidence_sha256 = self._sha(f"{content_id}:envelope:{suffix}")
        components = {
            "detail_raw_sha256": None,
            "text_sha256": self._sha(f"{content_id}:text:{suffix}"),
            "media_sha256": media_sha256,
            "asr_sha256": asr_sha256,
            "ocr_sha256": ocr_sha256,
            "comments_version_sha256": None,
            "manual_evidence_sha256": None,
        }
        envelope = connection.execute(
            """
            INSERT INTO evidence_envelopes(
                content_id,schema_version,text_sha256,media_sha256,asr_sha256,
                ocr_sha256,evidence_sha256,components_json,created_at
            ) VALUES (?,'evidence-test',?,?,?,?,?,?,?)
            """,
            (
                content_id,
                components["text_sha256"],
                media_sha256,
                asr_sha256,
                ocr_sha256,
                evidence_sha256,
                json.dumps(components, sort_keys=True, separators=(",", ":")),
                self._timestamp(3),
            ),
        )
        assert envelope.lastrowid is not None
        connection.execute(
            """
            INSERT INTO evaluation_versions(
                content_id,evidence_envelope_id,release_id,rule_version,
                taxonomy_version,matcher_rule_sha256,evidence_sha256,
                evaluation_source,evaluation_status,evidence_level,
                pending_review,payload_json,evaluated_at
            ) VALUES (?,?,?,'evaluation-test','selling-points-test',?,?,
                      ?,?,?,?,?,?)
            """,
            (
                content_id,
                int(envelope.lastrowid),
                self.release_id,
                "a" * 64,
                evidence_sha256,
                evaluation_source,
                evaluation_status,
                evidence_level,
                pending_review,
                "{}",
                self._timestamp(4),
            ),
        )

    def test_video_current_dag_and_v3_envelope_is_complete(self) -> None:
        with connect(self.db) as connection:
            content_id = self._content(connection)
            source_sha256, _urls = self._source(
                connection, content_id, media_kind="video", suffix="current"
            )
            media_sha256, asr_sha256, ocr_sha256 = self._video_dag(
                connection, content_id, source_sha256=source_sha256
            )
            self._evaluation(
                connection,
                content_id,
                media_sha256=media_sha256,
                asr_sha256=asr_sha256,
                ocr_sha256=ocr_sha256,
                evaluation_status="evaluated",
                evidence_level="V3",
                pending_review=1,
            )
            connection.commit()
            states = media_terminal_states(
                connection, self.release_id, [content_id, content_id]
            )
        self.assertEqual(states, {content_id: "complete"})

    def test_fully_succeeded_video_with_v1_is_terminal_insufficient(self) -> None:
        with connect(self.db) as connection:
            content_id = self._content(connection)
            source_sha256, _urls = self._source(
                connection, content_id, media_kind="video", suffix="current"
            )
            media_sha256, asr_sha256, ocr_sha256 = self._video_dag(
                connection, content_id, source_sha256=source_sha256
            )
            self._evaluation(
                connection,
                content_id,
                media_sha256=media_sha256,
                asr_sha256=asr_sha256,
                ocr_sha256=ocr_sha256,
                evaluation_status="insufficient_evidence",
                evidence_level="V1",
            )
            connection.commit()
            states = media_terminal_states(connection, self.release_id, [content_id])
        self.assertEqual(states[content_id], "terminal_insufficient")

    def test_required_current_terminal_slot_is_terminal_failed(self) -> None:
        with connect(self.db) as connection:
            content_id = self._content(connection)
            source_sha256, _urls = self._source(
                connection, content_id, media_kind="video", suffix="current"
            )
            self._slot(
                connection,
                content_id,
                source_sha256=source_sha256,
                processor_type="download",
                processor_version=media.VIDEO_DOWNLOAD_VERSION,
                status="terminal_failed",
            )
            connection.commit()
            states = media_terminal_states(connection, self.release_id, [content_id])
        self.assertEqual(states[content_id], "terminal_failed")

    def test_retryable_or_missing_current_slot_remains_pending(self) -> None:
        with connect(self.db) as connection:
            retryable_id = self._content(connection)
            retryable_source, _urls = self._source(
                connection, retryable_id, media_kind="video", suffix="retryable"
            )
            self._slot(
                connection,
                retryable_id,
                source_sha256=retryable_source,
                processor_type="download",
                processor_version=media.VIDEO_DOWNLOAD_VERSION,
                status="retryable_failed",
            )
            missing_id = self._content(connection)
            self._source(
                connection, missing_id, media_kind="video", suffix="missing"
            )
            connection.commit()
            states = media_terminal_states(
                connection, self.release_id, [retryable_id, missing_id]
            )
        self.assertEqual(
            states,
            {retryable_id: "pending", missing_id: "pending"},
        )

    def test_running_at_limit_is_pending_but_retryable_at_limit_is_terminal(self) -> None:
        with connect(self.db) as connection:
            running_id = self._content(connection)
            running_source, _urls = self._source(
                connection, running_id, media_kind="video", suffix="running"
            )
            self._slot(
                connection,
                running_id,
                source_sha256=running_source,
                processor_type="download",
                processor_version=media.VIDEO_DOWNLOAD_VERSION,
                status="running",
                attempt_count=media.MAX_MEDIA_DOWNLOAD_ATTEMPTS,
            )
            retryable_id = self._content(connection)
            retryable_source, _urls = self._source(
                connection, retryable_id, media_kind="video", suffix="retryable"
            )
            self._slot(
                connection,
                retryable_id,
                source_sha256=retryable_source,
                processor_type="download",
                processor_version=media.VIDEO_DOWNLOAD_VERSION,
                status="retryable_failed",
                attempt_count=media.MAX_MEDIA_DOWNLOAD_ATTEMPTS,
            )
            connection.commit()
            details = media_terminal_state_details(
                connection, self.release_id, [running_id, retryable_id]
            )
        self.assertEqual(
            details[running_id], MediaTerminalDetail("pending", "download_pending")
        )
        self.assertEqual(
            details[retryable_id],
            MediaTerminalDetail("terminal_failed", "download_terminal_failed"),
        )

    def test_old_source_is_ignored_but_same_source_legacy_terminal_blocks(self) -> None:
        with connect(self.db) as connection:
            source_changed_id = self._content(connection)
            old_source, _urls = self._source(
                connection, source_changed_id, media_kind="video", suffix="old"
            )
            self._slot(
                connection,
                source_changed_id,
                source_sha256=old_source,
                processor_type="download",
                processor_version=media.VIDEO_DOWNLOAD_VERSION,
                status="terminal_failed",
            )
            self._source(
                connection, source_changed_id, media_kind="video", suffix="new"
            )

            old_version_id = self._content(connection)
            _current_source, current_urls = self._source(
                connection, old_version_id, media_kind="video", suffix="current"
            )
            self._slot(
                connection,
                old_version_id,
                source_sha256=media._legacy_media_source_sha256(current_urls),
                processor_type="download",
                processor_version=media.LEGACY_VIDEO_DOWNLOAD_VERSION,
                status="terminal_failed",
            )
            connection.commit()
            states = media_terminal_states(
                connection, self.release_id, [source_changed_id, old_version_id]
            )
        self.assertEqual(
            states,
            {source_changed_id: "pending", old_version_id: "terminal_failed"},
        )

    def test_same_source_legacy_succeeded_download_reuses_current_dag(self) -> None:
        with connect(self.db) as connection:
            content_id = self._content(connection)
            _source_sha256, urls = self._source(
                connection, content_id, media_kind="video", suffix="current"
            )
            legacy_source_sha256 = media._legacy_media_source_sha256(urls)
            media_sha256, asr_sha256, ocr_sha256 = self._video_dag(
                connection,
                content_id,
                source_sha256=legacy_source_sha256,
                download_version=media.LEGACY_VIDEO_DOWNLOAD_VERSION,
            )
            self._evaluation(
                connection,
                content_id,
                media_sha256=media_sha256,
                asr_sha256=asr_sha256,
                ocr_sha256=ocr_sha256,
                evaluation_status="insufficient_evidence",
                evidence_level="V1",
            )
            connection.commit()
            details = media_terminal_state_details(
                connection, self.release_id, [content_id]
            )
        self.assertEqual(
            details[content_id],
            MediaTerminalDetail(
                "terminal_insufficient", "terminal_insufficient"
            ),
        )

    def test_v1_envelope_mismatched_to_current_dag_remains_pending(self) -> None:
        with connect(self.db) as connection:
            content_id = self._content(connection)
            source_sha256, _urls = self._source(
                connection, content_id, media_kind="video", suffix="current"
            )
            media_sha256, asr_sha256, ocr_sha256 = self._video_dag(
                connection, content_id, source_sha256=source_sha256
            )
            self._evaluation(
                connection,
                content_id,
                media_sha256=self._sha("stale-media"),
                asr_sha256=asr_sha256,
                ocr_sha256=ocr_sha256,
                evaluation_status="insufficient_evidence",
                evidence_level="V1",
            )
            connection.commit()
            states = media_terminal_states(connection, self.release_id, [content_id])
        self.assertNotEqual(media_sha256, self._sha("stale-media"))
        self.assertEqual(states[content_id], "pending")

    def test_current_image_dag_and_v1_envelope_is_terminal_insufficient(self) -> None:
        with connect(self.db) as connection:
            content_id = self._content(
                connection, content_type="image", platform="xiaohongshu"
            )
            source_sha256, urls = self._source(
                connection, content_id, media_kind="image", suffix="current"
            )
            media_sha256, asr_sha256, ocr_sha256 = self._image_dag(
                connection,
                content_id,
                source_sha256=source_sha256,
                urls=urls,
            )
            self._evaluation(
                connection,
                content_id,
                media_sha256=media_sha256,
                asr_sha256=asr_sha256,
                ocr_sha256=ocr_sha256,
                evaluation_status="insufficient_evidence",
                evidence_level="V1",
            )
            connection.commit()
            states = media_terminal_states(connection, self.release_id, [content_id])
        self.assertEqual(states[content_id], "terminal_insufficient")

    def test_valid_v3_envelope_is_complete_without_current_source_or_slots(self) -> None:
        with connect(self.db) as connection:
            content_id = self._content(connection)
            self._evaluation(
                connection,
                content_id,
                media_sha256=self._sha("sealed-media"),
                asr_sha256=self._sha("sealed-asr"),
                ocr_sha256=self._sha("sealed-ocr"),
                evaluation_status="evaluated",
                evidence_level="V3",
            )
            connection.commit()
            details = media_terminal_state_details(
                connection, self.release_id, [content_id]
            )
        self.assertEqual(
            details[content_id], MediaTerminalDetail("complete", "complete")
        )

    def test_image_v3_envelope_with_asr_hash_is_not_complete(self) -> None:
        with connect(self.db) as connection:
            content_id = self._content(
                connection,
                content_type="image",
                platform="xiaohongshu",
            )
            self._evaluation(
                connection,
                content_id,
                media_sha256=self._sha("sealed-image-media"),
                asr_sha256=self._sha("polluted-image-asr"),
                ocr_sha256=self._sha("sealed-image-ocr"),
                evaluation_status="evaluated",
                evidence_level="V3",
            )
            connection.commit()
            details = media_terminal_state_details(
                connection, self.release_id, [content_id]
            )
        self.assertEqual(
            details[content_id], MediaTerminalDetail("pending", "source_missing")
        )

    def test_detail_reasons_distinguish_each_pending_stage(self) -> None:
        with connect(self.db) as connection:
            source_missing_id = self._content(connection)

            download_pending_id = self._content(connection)
            self._source(
                connection,
                download_pending_id,
                media_kind="video",
                suffix="download-pending",
            )

            frames_pending_id = self._content(connection)
            frames_source, _urls = self._source(
                connection,
                frames_pending_id,
                media_kind="video",
                suffix="frames-pending",
            )
            self._video_dag(
                connection, frames_pending_id, source_sha256=frames_source
            )
            connection.execute(
                "DELETE FROM media_processing_slots WHERE content_id=? AND processor_type='frames'",
                (frames_pending_id,),
            )

            asr_pending_id = self._content(connection)
            asr_source, _urls = self._source(
                connection,
                asr_pending_id,
                media_kind="video",
                suffix="asr-pending",
            )
            self._video_dag(connection, asr_pending_id, source_sha256=asr_source)
            connection.execute(
                "DELETE FROM media_processing_slots WHERE content_id=? AND processor_type='asr'",
                (asr_pending_id,),
            )

            ocr_pending_id = self._content(connection)
            ocr_source, _urls = self._source(
                connection,
                ocr_pending_id,
                media_kind="video",
                suffix="ocr-pending",
            )
            self._video_dag(connection, ocr_pending_id, source_sha256=ocr_source)
            connection.execute(
                "DELETE FROM media_processing_slots WHERE content_id=? AND processor_type='ocr'",
                (ocr_pending_id,),
            )

            evaluation_pending_id = self._content(connection)
            evaluation_source, _urls = self._source(
                connection,
                evaluation_pending_id,
                media_kind="video",
                suffix="evaluation-pending",
            )
            self._video_dag(
                connection,
                evaluation_pending_id,
                source_sha256=evaluation_source,
            )
            connection.commit()
            details = media_terminal_state_details(
                connection,
                self.release_id,
                [
                    source_missing_id,
                    download_pending_id,
                    frames_pending_id,
                    asr_pending_id,
                    ocr_pending_id,
                    evaluation_pending_id,
                ],
            )
        self.assertEqual(
            details,
            {
                source_missing_id: MediaTerminalDetail("pending", "source_missing"),
                download_pending_id: MediaTerminalDetail(
                    "pending", "download_pending"
                ),
                frames_pending_id: MediaTerminalDetail("pending", "frames_pending"),
                asr_pending_id: MediaTerminalDetail("pending", "asr_pending"),
                ocr_pending_id: MediaTerminalDetail("pending", "ocr_pending"),
                evaluation_pending_id: MediaTerminalDetail(
                    "pending", "evaluation_pending"
                ),
            },
        )

    def test_exhausted_attempts_report_the_exact_terminal_stage(self) -> None:
        with connect(self.db) as connection:
            download_id = self._content(connection)
            download_source, _urls = self._source(
                connection,
                download_id,
                media_kind="video",
                suffix="download-terminal",
            )
            self._slot(
                connection,
                download_id,
                source_sha256=download_source,
                processor_type="download",
                processor_version=media.VIDEO_DOWNLOAD_VERSION,
                status="retryable_failed",
                attempt_count=media.MAX_MEDIA_DOWNLOAD_ATTEMPTS,
            )

            ids_by_stage: dict[str, int] = {}
            for stage in ("frames", "asr", "ocr"):
                content_id = self._content(connection)
                source_sha256, _urls = self._source(
                    connection,
                    content_id,
                    media_kind="video",
                    suffix=f"{stage}-terminal",
                )
                self._video_dag(
                    connection, content_id, source_sha256=source_sha256
                )
                connection.execute(
                    """
                    UPDATE media_processing_slots
                    SET status='retryable_failed',attempt_count=?
                    WHERE content_id=? AND processor_type=?
                    """,
                    (media.MAX_MEDIA_PROCESSING_ATTEMPTS, content_id, stage),
                )
                ids_by_stage[stage] = content_id
            connection.commit()
            details = media_terminal_state_details(
                connection,
                self.release_id,
                [download_id, *ids_by_stage.values()],
            )
        self.assertEqual(
            details[download_id],
            MediaTerminalDetail("terminal_failed", "download_terminal_failed"),
        )
        for stage, content_id in ids_by_stage.items():
            with self.subTest(stage=stage):
                self.assertEqual(
                    details[content_id],
                    MediaTerminalDetail(
                        "terminal_failed", f"{stage}_terminal_failed"
                    ),
                )

    def test_nonautomatic_evaluation_cannot_create_a_terminal_state(self) -> None:
        with connect(self.db) as connection:
            content_id = self._content(connection)
            source_sha256, _urls = self._source(
                connection, content_id, media_kind="video", suffix="current"
            )
            media_sha256, asr_sha256, ocr_sha256 = self._video_dag(
                connection, content_id, source_sha256=source_sha256
            )
            self._evaluation(
                connection,
                content_id,
                media_sha256=media_sha256,
                asr_sha256=asr_sha256,
                ocr_sha256=ocr_sha256,
                evaluation_status="evaluated",
                evidence_level="V3",
                evaluation_source="migrated_from_v5",
            )
            connection.commit()
            details = media_terminal_state_details(
                connection, self.release_id, [content_id]
            )
        self.assertEqual(
            details[content_id], MediaTerminalDetail("pending", "evaluation_pending")
        )

    def test_succeeded_output_artifact_closure_drift_is_pending(self) -> None:
        with connect(self.db) as connection:
            content_id = self._content(connection)
            source_sha256, _urls = self._source(
                connection, content_id, media_kind="video", suffix="current"
            )
            media_sha256, asr_sha256, ocr_sha256 = self._video_dag(
                connection, content_id, source_sha256=source_sha256
            )
            self._evaluation(
                connection,
                content_id,
                media_sha256=media_sha256,
                asr_sha256=asr_sha256,
                ocr_sha256=ocr_sha256,
                evaluation_status="insufficient_evidence",
                evidence_level="V1",
            )
            spare_content_id = self._content(connection)
            slot = connection.execute(
                """
                SELECT id,output_artifact_id FROM media_processing_slots
                WHERE content_id=? AND processor_type='download'
                """,
                (content_id,),
            ).fetchone()
            assert slot is not None and slot["output_artifact_id"] is not None
            artifact_id = int(slot["output_artifact_id"])
            artifact = connection.execute(
                "SELECT * FROM evidence_artifacts WHERE id=?", (artifact_id,)
            ).fetchone()
            assert artifact is not None
            mutations = [
                (
                    "media_processing_slots",
                    "output_artifact_id",
                    None,
                    int(slot["output_artifact_id"]),
                    int(slot["id"]),
                ),
                (
                    "evidence_artifacts",
                    "content_id",
                    spare_content_id,
                    int(artifact["content_id"]),
                    artifact_id,
                ),
                (
                    "evidence_artifacts",
                    "artifact_type",
                    "ocr",
                    str(artifact["artifact_type"]),
                    artifact_id,
                ),
                (
                    "evidence_artifacts",
                    "status",
                    "missing",
                    str(artifact["status"]),
                    artifact_id,
                ),
                (
                    "evidence_artifacts",
                    "processor_version",
                    "drifted-version",
                    str(artifact["processor_version"]),
                    artifact_id,
                ),
                (
                    "evidence_artifacts",
                    "sha256",
                    "0" * 64,
                    str(artifact["sha256"]),
                    artifact_id,
                ),
                (
                    "evidence_artifacts",
                    "byte_size",
                    0,
                    int(artifact["byte_size"]),
                    artifact_id,
                ),
                (
                    "evidence_artifacts",
                    "metadata_json",
                    "[]",
                    str(artifact["metadata_json"]),
                    artifact_id,
                ),
            ]
            for table, column, drifted, original, row_id in mutations:
                with self.subTest(column=column):
                    connection.execute(
                        f"UPDATE {table} SET {column}=? WHERE id=?",
                        (drifted, row_id),
                    )
                    self.assertEqual(
                        media_terminal_states(
                            connection, self.release_id, [content_id]
                        )[content_id],
                        "pending",
                    )
                    connection.execute(
                        f"UPDATE {table} SET {column}=? WHERE id=?",
                        (original, row_id),
                    )

    def test_large_batch_is_chunked_below_sqlite_variable_limit(self) -> None:
        with connect(self.db) as connection:
            content_ids = [self._content(connection) for _index in range(405)]
            connection.commit()
            previous_limit = connection.setlimit(
                sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER,
                401,
            )
            try:
                details = media_terminal_state_details(
                    connection,
                    self.release_id,
                    content_ids,
                )
            finally:
                connection.setlimit(
                    sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER,
                    previous_limit,
                )
        self.assertEqual(list(details), content_ids)
        self.assertEqual(
            set(details.values()),
            {MediaTerminalDetail("pending", "source_missing")},
        )

    def test_batch_isolated_and_invalid_inputs_fail_closed(self) -> None:
        with connect(self.db) as connection:
            complete_id = self._content(connection)
            source_sha256, _urls = self._source(
                connection, complete_id, media_kind="video", suffix="complete"
            )
            media_sha256, asr_sha256, ocr_sha256 = self._video_dag(
                connection, complete_id, source_sha256=source_sha256
            )
            self._evaluation(
                connection,
                complete_id,
                media_sha256=media_sha256,
                asr_sha256=asr_sha256,
                ocr_sha256=ocr_sha256,
                evaluation_status="evaluated",
                evidence_level="V2",
            )
            pending_id = self._content(connection)
            self._source(
                connection, pending_id, media_kind="video", suffix="pending"
            )
            connection.commit()
            states = media_terminal_states(
                connection, self.release_id, [pending_id, complete_id]
            )
            self.assertEqual(
                states,
                {pending_id: "pending", complete_id: "complete"},
            )
            self.assertEqual(media_terminal_states(connection, self.release_id, []), {})
            with self.assertRaisesRegex(MediaStateError, "positive integers"):
                media_terminal_states(connection, self.release_id, [True])
            with self.assertRaisesRegex(MediaStateError, "does not exist"):
                media_terminal_states(connection, "missing-release", [complete_id])
            with self.assertRaisesRegex(MediaStateError, "content items do not exist"):
                media_terminal_states(connection, self.release_id, [999_999])


if __name__ == "__main__":
    unittest.main()
