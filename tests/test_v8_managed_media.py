"""Managed-entry integration uses only temporary SQLite and generated media.

The archive completion stub below is deliberately fixture-only: completion's
full evidence gate is tested separately. Copy, decode, leases, immutable identity,
source selection, download slots and file hashes here are all real.
"""
from __future__ import annotations

import hashlib
import io
import json
import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from PIL import Image, ImageDraw

from v8 import duplicates, evaluation, media, media_lifecycle as lifecycle, media_retention as retention
from v8.media_state import media_terminal_state_details
from v8.storage import connect, initialize_database, now_utc, transaction


class _Response(io.BytesIO):
    def __init__(self, body: bytes, url: str) -> None:
        super().__init__(body)
        self.url = url
        self.headers = {"Content-Length": str(len(body))}

    def geturl(self) -> str:
        return self.url


class ManagedMediaTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.db = self.root / "managed.sqlite3"
        self.media_root = self.root / "media"
        self.archive = self.root / "archive"
        self.archive.mkdir(mode=0o700)
        self.calls: list[str] = []
        self.ocr_counts: list[int] = []
        self.now = now_utc()
        self.urls = [
            f"https://p3-sign.douyinpic.com/fixture-{index}.bin" for index in range(6)
        ]
        self.payloads: dict[str, bytes] = {}
        for index, url in enumerate(self.urls):
            buffer = io.BytesIO()
            image = Image.new("RGB", (96, 80), (index * 23, 66, 100))
            drawing = ImageDraw.Draw(image)
            drawing.rectangle((index + 3, 8, 55 + index, 43), fill=(200, index * 25, 10))
            drawing.line((0, index + 10, 94, 70), fill="white", width=4)
            image.save(buffer, "PNG")
            self.payloads[url] = buffer.getvalue()
        self._patch(media, "MEDIA_ROOT", self.media_root)
        self._patch(media.urllib.request, "urlopen", side_effect=self._open)
        self._patch(media, "snapshot_download", side_effect=AssertionError("model network forbidden"))
        self._patch(media, "compile_ocr_binary", return_value=self.root / "unused-ocr")
        self._patch(media, "_run_ocr", side_effect=self._ocr)
        self._patch(media, "_run_asr", side_effect=self._asr)
        for module in (media, retention, lifecycle):
            self._patch(module, "now_utc", side_effect=lambda: self.now)
        with connect(self.db) as connection:
            initialize_database(connection)
            connection.execute(
                "INSERT INTO accounts(id,phone,created_at,updated_at) VALUES (1,'',?,?)",
                (self.now, self.now),
            )
            connection.execute(
                """INSERT INTO account_platform_identities(account_id,platform,uid,created_at,updated_at)
                   VALUES (1,'douyin','100001',?,?)""", (self.now, self.now),
            )
            for content_id, kind in ((1, "image"), (2, "video")):
                connection.execute(
                    """INSERT INTO content_items(id,link_id,platform,platform_content_id,canonical_url,
                        account_id,raw_account_uid,title,body,content_type,published_at,imported_at,created_at,updated_at)
                        VALUES (?,?,'douyin',?, ?,1,'100001','汽车保养知识','判断刹车轮胎故障',?,?,?,?,?)""",
                    (content_id, f"M{content_id:05d}", str(9000000000000000000 + content_id),
                     f"https://www.douyin.com/video/{9000000000000000000 + content_id}",
                     kind, self.now, self.now, self.now, self.now),
                )
            connection.commit()

    def _patch(self, target: Any, name: str, *args: Any, **kwargs: Any) -> Any:
        patcher = patch.object(target, name, *args, **kwargs)
        value = patcher.start()
        self.addCleanup(patcher.stop)
        return value

    def _open(self, request: Any, **_kwargs: Any) -> _Response:
        url = request.full_url
        self.assertIn(url, self.payloads)
        self.calls.append(url)
        return _Response(self.payloads[url], url)

    def _ocr(self, manifest: Path, target: Path, **_kwargs: Any) -> Path:
        count = len(json.loads(manifest.read_bytes())["frames"])
        self.ocr_counts.append(count)
        text = "汽车刹车轮胎保养维修故障判断方法注意行车安全"
        media._atomic_json(target, {
            "status": "success", "processor_version": media.processor_versions()["ocr"],
            "source_count": count, "ocr_observation_count": count, "combined_text": text,
            "observations": [{"text": text} for _ in range(count)],
        })
        return target

    def _asr(self, _source: Path, target: Path, **_kwargs: Any) -> Path:
        config = media.load_media_config()["asr"]
        media._atomic_json(target, {
            "status": "success", "processor_version": media.processor_versions()["asr"],
            "model_id": config["model_id"], "model_revision": config["model_revision"],
            "language": config["language"], "text": "教你判断汽车刹车轮胎保养维修故障方法注意行车安全",
            "segments": [], "elapsed_seconds": 0.1,
        })
        return target

    def _activate(self) -> None:
        with connect(self.db) as connection, transaction(connection):
            lifecycle.activate(
                connection, mode="active", activation_id="managed-entry-fixture",
                release="fixture-release", rules_sha256="a" * 64, archive_root=self.archive,
                canary_content_ids=(1, 2),
                proofs={"contract_version": lifecycle.FIXTURE_PROOF_CONTRACT, "fixture_only": True,
                        "mac_consumers": True, "server_pairing": True, "canary_restore": True},
                now=self.now,
            )

    def _source(self, content_id: int = 1, *, suffix: str = "") -> media.Artifact:
        kind = "image" if content_id == 1 else "video"
        if suffix:
            urls = [url.replace(".bin", suffix + ".bin") for url in self.urls]
            for old, new in zip(self.urls, urls, strict=True):
                self.payloads[new] = self.payloads[old]
        else:
            urls = self.urls
        if kind == "video":
            video = self.root / "generated.mp4"
            if not video.exists():
                subprocess.run(
                    ["ffmpeg", "-nostdin", "-v", "error", "-y", "-f", "lavfi", "-i",
                     "testsrc2=size=96x64:rate=12", "-t", "1", "-pix_fmt", "yuv420p", str(video)],
                    check=True, capture_output=True, timeout=30,
                )
            urls = [f"https://v3.douyinvod.com/fixture{suffix}.mp4"]
            self.payloads[urls[0]] = video.read_bytes()
        aweme: dict[str, Any] = {
            "aweme_id": str(9000000000000000000 + content_id),
            "author": {"uid": "100001"}, "desc": "汽车保养知识",
        }
        if kind == "image":
            aweme["images"] = [
                {"download_url_list": [urls[index]], "url_list": [urls[index + 1]]}
                for index in range(0, len(urls), 2)
            ]
        else:
            aweme["video"] = {"play_addr": {"url_list": urls}}
        raw = {"code": 200, "data": {"status_code": 0, "aweme_detail": aweme}}
        encoded = json.dumps(raw, sort_keys=True).encode()
        with connect(self.db) as connection, transaction(connection):
            count = int(connection.execute("SELECT COUNT(*) FROM provider_raw_responses").fetchone()[0])
            path = self.root / f"raw-{count}.json"
            path.write_bytes(encoded)
            path.chmod(0o600)
            cursor = connection.execute(
                """INSERT INTO provider_raw_responses(account_id,content_id,provider,operation,local_path,
                    sha256,byte_size,http_status,captured_at,source)
                    VALUES (1,?,'TikHub','douyin_video_detail',?,?,?,200,?,'live_applied')""",
                (content_id, str(path), hashlib.sha256(encoded).hexdigest(), len(encoded), self.now),
            )
            raw_id = int(cursor.lastrowid or 0)
        source = media.store_media_source_manifest(
            content_id, media_kind=kind, urls=urls, raw_response_id=raw_id,
            db_path=self.db, media_root=self.media_root,
        )
        assert source is not None
        return source

    def _bundle(self, content_id: int = 1) -> dict[str, Any]:
        with connect(self.db) as connection:
            bundle = lifecycle.current_bundle(connection, content_id)
        self.assertIsNotNone(bundle)
        assert bundle is not None
        return bundle

    def _download(self, content_id: int = 1, *, process: bool = False) -> dict[str, Any]:
        result = media.process_content_media(
            content_id, download_only=not process, db_path=self.db,
            media_root=self.media_root, maximum_download_bytes=2 * 1024 * 1024,
        )
        self.assertEqual(result["status"], "evidence_ready" if process else "downloaded", result)
        return result

    def _release(self) -> str:
        from v8.matcher_dsl import POINT_IDS, POINT_SCENES
        from v8.taxonomy_rule_backfill import backfill_v5_1_matcher_rules

        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                """INSERT INTO taxonomy_versions(id,version,status,definition,created_at,published_at)
                   VALUES ('taxonomy','selling-points-v5.0','published','fixture',?,?)""",
                (self.now, self.now),
            )
            for code in sorted(POINT_IDS):
                cursor = connection.execute(
                    """INSERT INTO selling_points(taxonomy_id,code,tier,label,definition,matcher_rule_json)
                       VALUES ('taxonomy',?,'other',?,?,'{}')""", (code, code, code),
                )
                for scene in sorted(POINT_SCENES[code]):
                    connection.execute(
                        "INSERT INTO selling_point_scenes(selling_point_id,scene) VALUES (?,?)",
                        (cursor.lastrowid, scene),
                    )
        matcher = backfill_v5_1_matcher_rules(db_path=self.db)
        release_id = "evaluation-v8__selling-points-v5.1"
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE taxonomy_versions SET status='retired' WHERE version='selling-points-v5.0'")
            connection.execute("UPDATE taxonomy_versions SET status='published',published_at=? WHERE version='selling-points-v5.1'", (self.now,))
            connection.execute(
                """INSERT INTO evaluation_releases(id,rule_version,taxonomy_version,matcher_rule_sha256,
                   status,created_at,updated_at,activated_at)
                   VALUES (?,'evaluation-v8','selling-points-v5.1',?,'active',?,?,?)""",
                (release_id, matcher["matcher_rule_sha256"], self.now, self.now, self.now),
            )
        return release_id

    def _archive_fixture(self, content_id: int = 1) -> dict[str, Any]:
        bundle = self._bundle(content_id)
        path = Path(bundle["evidence_root"]) / "fixture-completion.json"
        path.write_text(json.dumps({"fixture_only": True, "bundle_id": bundle["bundle_id"]}))
        path.chmod(0o600)
        with connect(self.db) as connection, transaction(connection):
            artifact = media.register_artifact(
                connection, content_id=content_id, artifact_type="fixture_completion",
                path=path, processor_version="fixture-only",
            )
            reference = {"artifact_id": artifact.id, "path": str(path),
                         "sha256": artifact.sha256, "byte_size": path.stat().st_size}
            lifecycle.update_state(
                connection, bundle, {"completion_receipt": reference},
                expected_revision=bundle["state"]["revision"],
            )
        with patch.object(retention, "_completion", return_value={
            "ready": True, "blockers": [], "receipt": reference, "evidence_files": [reference],
        }):
            result = retention._execute(
                "archive", bundle["bundle_id"], db_path=self.db, at=self.now, release_hot=True
            )
        self.assertEqual(result["status"], "archived", result)
        return self._bundle(content_id)

    def test_direct_image_entry_enrolls_and_cache_keeps_identity(self) -> None:
        source = self._source()
        self._activate()
        groups = media.douyin_image_source_groups(self.urls, [self.urls[i:i + 2] for i in range(0, 6, 2)])
        first = media.download_image_sources(
            1, self.urls, db_path=self.db, frozen_image_groups=groups, maximum_bytes=2 * 1024 * 1024,
        )
        bundle = self._bundle()
        manifest = json.loads(media._resolved(first.local_path).read_bytes())
        self.assertEqual(Path(first.local_path), bundle["evidence_root"] / "download-manifest.json")
        self.assertEqual(len(manifest["image_paths"]), 3)
        self.assertEqual(len(bundle["manifest"]["members"]), 3)
        self.assertEqual(bundle["manifest"]["source"]["artifact_id"], source.id)
        self.assertTrue(all(Path(value).is_relative_to(bundle["originals_root"]) for value in manifest["image_paths"]))
        before = len(self.calls)
        second = media.download_image_sources(1, self.urls, db_path=self.db, frozen_image_groups=groups)
        self.assertEqual(first, second)
        self.assertEqual(len(self.calls), before)
        self.assertEqual(self._bundle()["manifest"], bundle["manifest"])

    def test_default_queue_processes_every_image_under_managed_paths(self) -> None:
        source = self._source()
        before = media._resolved(source.local_path).read_bytes()
        self._activate()
        download = media.run_media_download_queue(db_path=self.db, max_workers=1)
        self.assertEqual((download["candidates"], download["downloaded"]), (1, 1), download)
        result = media.run_media_processing_queue(db_path=self.db)
        self.assertEqual(result["evidence_ready"], 1, result)
        self.assertEqual(self.ocr_counts, [3])
        self.assertEqual(self.calls, [self.urls[0], self.urls[2], self.urls[4]])
        bundle = self._bundle()
        with connect(self.db) as connection:
            _, ocr = media.managed_bound_artifact(connection, 1, ("ocr",))
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0], 0)
        assert ocr is not None
        self.assertTrue(Path(ocr["local_path"]).is_relative_to(bundle["evidence_root"] / "ocr"))
        self.assertEqual(media._resolved(source.local_path).read_bytes(), before)
        self.assertEqual(media.run_media_processing_queue(db_path=self.db)["candidates"], 0)
        self.assertFalse((self.media_root / "M00001" / "ocr.json").exists())

    def test_video_stages_and_fingerprint_are_source_and_version_bound(self) -> None:
        self._source(2)
        self._activate()
        self._download(2, process=True)
        bundle = self._bundle(2)
        value = duplicates.fingerprint_content(2, db_path=self.db)
        self.assertTrue(value["media_sha256"])
        with connect(self.db) as connection:
            rows = connection.execute(
                "SELECT * FROM evidence_artifacts WHERE content_id=2 AND artifact_type IN ('asr','ocr','frames_manifest','duplicate_fingerprint')"
            ).fetchall()
        self.assertEqual(len(rows), 4)
        for row in rows:
            path = media._resolved(row["local_path"])
            self.assertTrue(path.is_relative_to(bundle["evidence_root"]))
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(json.loads(row["metadata_json"])["media_lifecycle"]["bundle_id"], bundle["bundle_id"])
        self.assertFalse((self.media_root / "M00002" / "asr.json").exists())
        self.assertFalse((self.db.parent / "duplicate-fingerprints" / "M00002.json").exists())

    def test_legacy_cache_is_not_enrolled_after_activation(self) -> None:
        self._source()
        result = self._download(process=True)
        self._activate()
        before = len(self.calls)
        again = self._download(process=True)
        self.assertEqual(result["artifacts"], again["artifacts"])
        self.assertEqual(len(self.calls), before)
        with connect(self.db) as connection:
            self.assertIsNone(lifecycle.current_bundle(connection, 1))
        self.assertTrue((self.media_root / "M00001" / "ocr.json").exists())
        self.assertFalse((self.media_root / "managed-v1").exists())

    def test_registration_rollback_reuses_published_files_without_redownload(self) -> None:
        self._source()
        self._activate()
        register = lifecycle.register_download
        def fail_after_register(connection: sqlite3.Connection, intent: dict[str, Any],
                                artifact_id: int, slot_id: int) -> dict[str, Any]:
            register(connection, intent, artifact_id, slot_id)
            raise RuntimeError("fixture after registration before commit")
        with patch.object(lifecycle, "register_download", side_effect=fail_after_register):
            with self.assertRaisesRegex(RuntimeError, "before commit"):
                media.process_content_media(1, download_only=True, db_path=self.db)
        calls = len(self.calls)
        with connect(self.db) as connection:
            self.assertIsNone(lifecycle.current_bundle(connection, 1))
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM evidence_artifacts WHERE artifact_type='media_manifest'").fetchone()[0], 0)
            intent = json.loads(connection.execute(
                "SELECT details_json FROM scheduler_runs WHERE job_id=?", (lifecycle.INTENT_JOB_ID,)
            ).fetchone()[0])
        self._download()
        self.assertEqual(len(self.calls), calls)
        self.assertEqual(self._bundle()["bundle_id"], intent["bundle_id"])

    def test_original_namespace_and_identity_cannot_be_rewritten(self) -> None:
        self._source()
        self._activate()
        self._download()
        bundle = self._bundle()
        with connect(self.db) as connection, transaction(connection):
            original = lifecycle.original_artifact(connection, bundle)
            metadata = json.loads(original["metadata_json"])
            preserved = {key: value for key, value in metadata.items() if key != "media_lifecycle"}
            same = media.register_artifact(
                connection, content_id=1, artifact_type="media_manifest",
                path=media._resolved(original["local_path"]), processor_version=original["processor_version"],
                metadata=preserved,
            )
            self.assertEqual(same.id, original["id"])
            self.assertEqual(dict(connection.execute("SELECT * FROM evidence_artifacts WHERE id=?", (same.id,)).fetchone()), original)
            with self.assertRaises(media.MediaProcessingError):
                media.register_artifact(
                    connection, content_id=1, artifact_type="media_manifest",
                    path=media._resolved(original["local_path"]), processor_version=original["processor_version"],
                    metadata={**preserved, "unexpected": True},
                )

    def test_new_processor_version_retains_previous_file(self) -> None:
        self._source()
        self._activate()
        first = self._download(process=True)
        with connect(self.db) as connection:
            original = dict(connection.execute("SELECT * FROM evidence_artifacts WHERE id=?", (first["artifacts"]["ocr"],)).fetchone())
        path = media._resolved(original["local_path"])
        content = path.read_bytes()
        versions = media.processor_versions()
        with patch.object(media, "processor_versions", return_value={**versions, "ocr": versions["ocr"] + "|fixture-next"}):
            second = self._download(process=True)
        self.assertNotEqual(first["artifacts"]["ocr"], second["artifacts"]["ocr"])
        self.assertEqual(path.read_bytes(), content)
        self.assertEqual(self.ocr_counts, [3, 3])

    def test_reset_success_slot_cannot_overwrite_registered_managed_stage(self) -> None:
        self._source()
        self._activate()
        result = self._download(process=True)
        with connect(self.db) as connection, transaction(connection):
            row = connection.execute("SELECT * FROM evidence_artifacts WHERE id=?", (result["artifacts"]["ocr"],)).fetchone()
            path = media._resolved(row["local_path"])
            before = path.read_bytes()
            connection.execute("UPDATE media_processing_slots SET status='retryable_failed',output_artifact_id=NULL WHERE processor_type='ocr' AND content_id=1")
        with self.assertRaisesRegex(media.MediaProcessingError, "cannot be overwritten"):
            self._download(process=True)
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(self.ocr_counts, [3])

    def test_cold_cached_fingerprint_and_evaluation_keep_hashes_and_report_complete(self) -> None:
        self._source(2)
        self._activate()
        self._download(2, process=True)
        release_id = self._release()
        evaluated = evaluation.evaluate_content(2, db_path=self.db)
        fingerprint = duplicates.fingerprint_content(2, db_path=self.db)
        bundle = self._archive_fixture(2)
        with connect(self.db) as connection:
            original = lifecycle.original_artifact(connection, bundle)
            self.assertEqual(original["status"], "missing")
            selected = duplicates._source_inputs(connection, 2)
            self.assertEqual(selected["source"]["media_artifact_sha256"], original["sha256"])
            detail = media_terminal_state_details(connection, release_id, [2])[2]
        self.assertEqual(detail.state, "complete")
        with patch.object(retention, "media_read_lease", side_effect=AssertionError("cached result must not read originals")):
            self.assertEqual(duplicates.fingerprint_content(2, db_path=self.db), fingerprint)
            again = evaluation.evaluate_content(2, db_path=self.db)
        self.assertFalse(again.created)
        self.assertEqual(again.evidence_sha256, evaluated.evidence_sha256)
        self.assertEqual(media.process_content_media(2, db_path=self.db)["status"], "restore_required")

    def test_cold_new_work_does_not_create_empty_fingerprint_or_weak_evaluation(self) -> None:
        self._source()
        self._activate()
        self._download(process=True)
        self._release()
        evaluation.evaluate_content(1, db_path=self.db)
        duplicates.fingerprint_content(1, db_path=self.db)
        self._archive_fixture()
        with connect(self.db) as connection, transaction(connection):
            before = connection.execute("SELECT COUNT(*) FROM evaluation_versions").fetchone()[0]
            connection.execute("UPDATE content_items SET title='变更后的汽车内容' WHERE id=1")
        with self.assertRaisesRegex(lifecycle.LifecycleError, "original_archived"):
            duplicates.fingerprint_content(1, db_path=self.db)
        with self.assertRaisesRegex(lifecycle.LifecycleError, "original_archived"):
            evaluation.evaluate_content(1, db_path=self.db)
        self.assertEqual(duplicates._pending_content_ids(
            limit=None, db_path=self.db, scope_content_ids=(1,)
        ), [])
        self.assertNotIn(1, evaluation.incremental_candidates(db_path=self.db))
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM duplicate_fingerprints").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM evaluation_versions").fetchone()[0], before)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM media_processing_slots WHERE processor_type='duplicate_fingerprint'").fetchone()[0], 1)

    def test_expiry_blocks_original_processing_without_touching_completion(self) -> None:
        self._source()
        self._activate()
        self._download(process=True)
        release_id = self._release()
        evaluated = evaluation.evaluate_content(1, db_path=self.db)
        bundle = self._archive_fixture()
        self.now = str(bundle["state"]["delete_due_at"])
        calls = len(self.calls)
        result = media.process_content_media(1, db_path=self.db)
        self.assertEqual(result["status"], "expired_non_replayable")
        self.assertEqual(len(self.calls), calls)
        self.assertEqual(media.run_media_processing_queue(db_path=self.db)["candidates"], 0)
        self.assertEqual(evaluation.evaluate_content(1, db_path=self.db).evaluation_id, evaluated.evaluation_id)
        with connect(self.db) as connection:
            self.assertEqual(media_terminal_state_details(connection, release_id, [1])[1].state, "complete")
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE content_items SET title='到期后的新标题' WHERE id=1")
            before = connection.execute("SELECT COUNT(*) FROM media_processing_slots").fetchone()[0]
        self.assertEqual(duplicates._pending_content_ids(limit=None, db_path=self.db, scope_content_ids=(1,)), [])
        self.assertNotIn(1, evaluation.incremental_candidates(db_path=self.db))
        with self.assertRaisesRegex(lifecycle.LifecycleError, "original_expiry_pending"):
            duplicates.fingerprint_content(1, db_path=self.db)
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM media_processing_slots").fetchone()[0], before)

    def test_new_source_does_not_fall_back_to_previous_bundle(self) -> None:
        self._source()
        self._activate()
        self._download(process=True)
        previous = self._bundle()
        self._source(suffix="-new")
        with connect(self.db) as connection:
            self.assertIsNone(lifecycle.current_bundle(connection, 1))
            with self.assertRaisesRegex(lifecycle.LifecycleError, "managed_source_pending"):
                duplicates._source_inputs(connection, 1)
            with self.assertRaisesRegex(lifecycle.LifecycleError, "managed_source_pending"):
                evaluation._artifact_components(connection, 1, rule_version="evaluation-v8")
        self._download(process=True)
        self.assertNotEqual(self._bundle()["bundle_id"], previous["bundle_id"])

    def test_active_direct_download_without_bound_source_is_refused(self) -> None:
        self._activate()
        with self.assertRaisesRegex(lifecycle.LifecycleError, "managed_source_required"):
            media.download_video_sources(2, ["https://v3.douyinvod.com/unbound.mp4"], db_path=self.db)
        self.assertEqual(self.calls, [])

    def test_reserved_namespace_does_not_allow_other_metadata_drift(self) -> None:
        self._source()
        self._activate()
        result = self._download(process=True)
        bundle = self._bundle()
        with connect(self.db) as connection, transaction(connection):
            original = lifecycle.original_artifact(connection, bundle)
            changed = json.loads(original["metadata_json"])
            changed["media_lifecycle"]["control_artifact_id"] += 100
            connection.execute("UPDATE evidence_artifacts SET metadata_json=? WHERE id=?",
                               (json.dumps(changed), original["id"]))
        groups = media.douyin_image_source_groups(self.urls, [self.urls[i:i + 2] for i in range(0, 6, 2)])
        with self.assertRaisesRegex(lifecycle.LifecycleError, "bundle_original_namespace_changed"):
            media.download_image_sources(1, self.urls, db_path=self.db, frozen_image_groups=groups)
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE evidence_artifacts SET metadata_json=? WHERE id=?",
                               (original["metadata_json"], original["id"]))
            ocr_id = result["artifacts"]["ocr"]
            raw = connection.execute("SELECT metadata_json FROM evidence_artifacts WHERE id=?", (ocr_id,)).fetchone()[0]
            metadata = json.loads(raw)
            metadata["unrelated"] = "must not be ignored"
            connection.execute("UPDATE evidence_artifacts SET metadata_json=? WHERE id=?", (json.dumps(metadata), ocr_id))
        with connect(self.db) as connection:
            with self.assertRaisesRegex(lifecycle.LifecycleError, "managed_evidence_binding_invalid"):
                evaluation._artifact_components(connection, 1, rule_version="evaluation-v8")


    def test_managed_image_recovery_uses_exact_manifest_and_slot_binding(self) -> None:
        self._source()
        self._activate()
        self._download()
        bundle = self._bundle()
        versions = media.processor_versions()
        slot_source = media.managed_slot_source(bundle, bundle["manifest"]["original_artifact"]["sha256"])
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                """INSERT INTO media_processing_slots(content_id,source_sha256,processor_type,
                   processor_version,status,attempt_count,created_at,updated_at)
                   VALUES (1,?,'ocr',?,'running',1,'2000-01-01T00:00:00Z','2000-01-01T00:00:00Z')""",
                (slot_source, versions["ocr"]),
            )
        recovered = media.recover_stale_media_processing_slots(
            db_path=self.db, processor_types=("ocr",), processor_version_by_type=versions,
            stale_after_seconds=1, content_ids=(1,),
        )
        self.assertEqual(recovered["recovered"], 1, recovered)
        self._download(process=True)
        self.assertEqual(self.ocr_counts, [3])

    def test_reassigned_content_cannot_consume_previous_owner_bundle(self) -> None:
        self._source()
        self._activate()
        self._download(process=True)
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE content_items SET raw_account_uid='100002' WHERE id=1")
        with connect(self.db) as connection:
            with self.assertRaisesRegex(lifecycle.LifecycleError, "managed_content_identity_changed"):
                duplicates._source_inputs(connection, 1)
            with self.assertRaisesRegex(lifecycle.LifecycleError, "managed_content_identity_changed"):
                evaluation._artifact_components(connection, 1, rule_version="evaluation-v8")
        before = len(self.calls)
        groups = media.douyin_image_source_groups(self.urls, [self.urls[i:i + 2] for i in range(0, 6, 2)])
        with self.assertRaisesRegex(lifecycle.LifecycleError, "managed_content_identity_changed"):
            media.download_image_sources(1, self.urls, db_path=self.db, frozen_image_groups=groups)
        self.assertEqual(len(self.calls), before)


    def test_full_completion_archive_and_explicit_retained_evaluation(self) -> None:
        from v8 import media_completion as completion

        self._source()
        self._activate()
        self._download(process=True)
        self._release()
        first = evaluation.evaluate_content(1, db_path=self.db)
        duplicates.fingerprint_content(1, db_path=self.db)
        bundle = self._bundle()
        proof = completion.seal_completion(bundle["bundle_id"], db_path=self.db, at=self.now)
        self.assertTrue(proof["ready"], proof)
        receipt = proof["receipt"]
        archived = retention.archive_bundle(bundle["bundle_id"], db_path=self.db, at=self.now)
        self.assertEqual(archived["status"], "archived", archived)
        verified = completion.verify_completion(self._bundle(), db_path=self.db)
        self.assertTrue(verified["ready"], verified)
        self.assertEqual(verified["receipt"], receipt)
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE content_items SET title='补充汽车保养新知识' WHERE id=1")
        with self.assertRaisesRegex(lifecycle.LifecycleError, "original_archived"):
            evaluation.evaluate_content(1, db_path=self.db)
        with patch.object(retention, "media_read_lease", wraps=retention.media_read_lease) as lease:
            retained = evaluation.evaluate_content(1, db_path=self.db, reuse_retained_evidence=True)
        self.assertTrue(retained.created)
        self.assertEqual(retained.evidence_level, "V2")
        self.assertNotEqual(retained.evidence_sha256, first.evidence_sha256)
        self.assertEqual(lease.call_args.kwargs["require_original"], False)
        self.assertTrue(completion.verify_completion(self._bundle(), db_path=self.db)["ready"])

    def test_retained_evaluation_rejects_new_unsealed_ocr(self) -> None:
        from v8 import media_completion as completion

        self._source()
        self._activate()
        self._download(process=True)
        self._release()
        evaluation.evaluate_content(1, db_path=self.db)
        duplicates.fingerprint_content(1, db_path=self.db)
        bundle = self._bundle()
        proof = completion.seal_completion(bundle["bundle_id"], db_path=self.db, at=self.now)
        self.assertTrue(proof["ready"], proof)
        versions = media.processor_versions()
        with patch.object(media, "processor_versions", return_value={**versions, "ocr": versions["ocr"] + "|unsealed"}):
            self._download(process=True)
        archived = retention.archive_bundle(bundle["bundle_id"], db_path=self.db, at=self.now)
        self.assertEqual(archived["status"], "archived", archived)
        with self.assertRaisesRegex(evaluation.EvaluationError, "not sealed"):
            evaluation.evaluate_content(1, db_path=self.db, reuse_retained_evidence=True)
