"""Real temporary media/SQLite closure; no real supplier, model or archive access."""
from __future__ import annotations

import hashlib
import io
import json
import os
import sqlite3
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

from PIL import Image

from tests import test_v8_managed_media as fixtures
from v8 import duplicates, evaluation, media, media_completion as completion, media_lifecycle as lifecycle, media_retention as retention
from v8.storage import connect


class MediaCompletionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = fixtures.ManagedMediaTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture._patch(completion, "now_utc", side_effect=lambda: self.fixture.now)
        self.db = self.fixture.db

    def ready(self, content_id: int = 1, *, fingerprint: bool = True, evaluate: bool = True,
              source_mode: str = "detail") -> dict[str, Any]:
        self.fixture._source(content_id)
        if source_mode != "detail":
            with connect(self.db) as connection:
                raw = connection.execute("SELECT * FROM provider_raw_responses ORDER BY id DESC LIMIT 1").fetchone()
                body = json.loads(media._resolved(raw["local_path"]).read_bytes())
                aweme = body["data"].pop("aweme_detail")
                body["data"]["aweme_list"] = [aweme]
                encoded = json.dumps(body, sort_keys=True).encode()
                media._resolved(raw["local_path"]).write_bytes(encoded)
                digest = hashlib.sha256(encoded).hexdigest()
                connection.execute("UPDATE provider_raw_responses SET content_id=NULL,operation='douyin_user_posts',sha256=?,byte_size=? WHERE id=?", (digest, len(encoded), raw["id"]))
                if source_mode == "derived":
                    payload = {"stage": "detail", "derived_from_operation": "douyin_user_posts", "source_raw_response_id": raw["id"],
                               "source_sha256": digest, "source_captured_at": raw["captured_at"],
                               "data": {"content_type": "image", "account_uid": "100001", "media_urls": self.fixture.urls}}
                    path = self.fixture.root / "derived-detail.json"
                    encoded = json.dumps(payload, sort_keys=True).encode()
                    path.write_bytes(encoded)
                    path.chmod(0o600)
                    cursor = connection.execute(
                        "INSERT INTO provider_raw_responses(content_id,account_id,provider,operation,local_path,sha256,byte_size,http_status,captured_at,source) "
                        "VALUES (?,1,'TikHub','douyin_video_detail',?,?,?,200,?,'derived')",
                        (content_id, str(path), hashlib.sha256(encoded).hexdigest(), len(encoded), self.fixture.now),
                    )
                    raw_id = int(cursor.lastrowid or 0)
            if source_mode == "derived":
                media.store_media_source_manifest(content_id, media_kind="image", urls=self.fixture.urls,
                    raw_response_id=raw_id, db_path=self.db, media_root=self.fixture.media_root)
        self.fixture._activate()
        self.fixture._download(content_id, process=True)
        self.fixture._release()
        if fingerprint:
            duplicates.fingerprint_content(content_id, db_path=self.db)
        if evaluate:
            evaluation.evaluate_content(content_id, db_path=self.db)
        return self.fixture._bundle(content_id)

    def seal(self, bundle: dict[str, Any]) -> dict[str, Any]:
        return completion.seal_completion(bundle["bundle_id"], db_path=self.db)

    def assert_blocked(self, result: dict[str, Any], reason: str) -> None:
        self.assertFalse(result["ready"], result)
        self.assertTrue(any(reason in value for value in result["blockers"]), result)
        self.assertIsNone(result["receipt"])
        self.assertEqual(result["evidence_files"], [])

    def test_unsealed_verification_is_read_only(self) -> None:
        bundle = self.ready()
        with connect(self.db) as connection:
            before = connection.execute("SELECT COUNT(*) FROM evidence_artifacts").fetchone()[0]
        self.assert_blocked(completion.verify_completion(bundle, db_path=self.db), "completion_not_sealed")
        with connect(self.db) as connection:
            self.assertEqual(before, connection.execute("SELECT COUNT(*) FROM evidence_artifacts").fetchone()[0])
        self.assertFalse((bundle["evidence_root"] / "completion").exists())

    def test_online_writer_wal_is_visible_to_seal_and_verify(self) -> None:
        writer = connect(self.db)
        self.addCleanup(writer.close)
        writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        writer.execute("PRAGMA wal_autocheckpoint=0")
        bundle = self.ready()
        self.assertTrue(Path(str(self.db) + "-wal").exists())
        with sqlite3.connect(f"file:{self.db}?mode=ro&immutable=1", uri=True) as stale:
            self.assertEqual(stale.execute("SELECT COUNT(*) FROM evidence_artifacts WHERE artifact_type='media_lifecycle_manifest'").fetchone()[0], 0)
        result = self.seal(bundle)
        self.assertTrue(result["ready"], result)
        verified = completion.verify_completion(bundle, db_path=self.db)
        self.assertTrue(verified["ready"], verified)
        self.assertEqual(verified["receipt"], result["receipt"])

    def test_read_only_replica_verification_does_not_write(self) -> None:
        bundle = self.ready()
        result = self.seal(bundle)
        self.assertTrue(result["ready"], result)
        before = self.db.read_bytes()
        files = {str(path): (path.stat().st_size, path.stat().st_mtime_ns)
                 for path in self.fixture.root.rglob("*") if path.is_file()}
        with patch.dict(os.environ, {"DCAR_READ_ONLY": "1"}):
            verified = completion.verify_completion(bundle, db_path=self.db)
        self.assertTrue(verified["ready"], verified)
        self.assertEqual(before, self.db.read_bytes())
        self.assertEqual(files, {str(path): (path.stat().st_size, path.stat().st_mtime_ns)
                                for path in self.fixture.root.rglob("*") if path.is_file()})

    def test_unsealed_read_only_replica_cannot_generate_previews_or_proofs(self) -> None:
        bundle = self.ready()
        before = self.db.read_bytes()
        paths = set(self.fixture.root.rglob("*"))
        with patch.dict(os.environ, {"DCAR_READ_ONLY": "1"}):
            self.assert_blocked(self.seal(bundle), "read_only_completion_not_sealed")
        self.assertEqual(before, self.db.read_bytes())
        self.assertEqual(paths, set(self.fixture.root.rglob("*")))

    def test_account_discovery_raw_is_bound_to_unique_work_and_author(self) -> None:
        bundle = self.ready(source_mode="discovery")
        result = self.seal(bundle)
        self.assertTrue(result["ready"], result)
        self.assertTrue(completion.verify_completion(bundle, db_path=self.db)["ready"])

    def test_derived_detail_seals_and_retains_original_discovery_raw(self) -> None:
        bundle = self.ready(source_mode="derived")
        result = self.seal(bundle)
        self.assertTrue(result["ready"], result)
        raw = next(row for row in result["evidence_files"] if row["role"] == "source_discovery_raw")
        self.assertTrue(media._resolved(raw["path"]).is_file())
        media._resolved(raw["path"]).write_bytes(b"tampered discovery fixture")
        self.assertFalse(completion.verify_completion(bundle, db_path=self.db)["ready"])

    def test_arbitrary_null_content_detail_raw_is_not_accepted(self) -> None:
        bundle = self.ready()
        with connect(self.db) as connection:
            connection.execute("UPDATE provider_raw_responses SET content_id=NULL")
        self.assert_blocked(self.seal(bundle), "source_raw_binding_changed")

    def test_images_seal_every_primary_preview_without_upscaling(self) -> None:
        bundle = self.ready()
        calls = len(self.fixture.calls)
        result = self.seal(bundle)
        self.assertTrue(result["ready"], result)
        self.assertEqual(calls, len(self.fixture.calls))
        previews = [row for row in result["evidence_files"] if row["role"] == "image_preview"]
        self.assertEqual(len(previews), 3)
        for reference in previews:
            path = media._resolved(reference["path"])
            with Image.open(path) as image:
                self.assertEqual(image.format, "JPEG")
                self.assertEqual(image.size, (96, 80))
            self.assertLessEqual(path.stat().st_size, 100 * 1024)
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), reference["sha256"])
        preview_ref = next(row for row in result["evidence_files"] if row["role"] == "preview_manifest")
        preview = json.loads(media._resolved(preview_ref["path"]).read_bytes())
        self.assertEqual([row["source_member_id"] for row in preview["members"]], ["m0000", "m0001", "m0002"])
        self.assertEqual(preview["jpeg_quality"], 70)
        self.assertTrue(completion.verify_completion(bundle, db_path=self.db)["ready"])

    def test_large_image_preview_is_256_pixels_and_bounded_only(self) -> None:
        buffer = io.BytesIO()
        Image.new("RGB", (1200, 600), "navy").save(buffer, "PNG")
        for url in self.fixture.urls:
            self.fixture.payloads[url] = buffer.getvalue()
        result = self.seal(self.ready())
        self.assertTrue(result["ready"], result)
        for reference in result["evidence_files"]:
            if reference["role"] == "image_preview":
                with Image.open(media._resolved(reference["path"])) as image:
                    self.assertEqual(image.size, (256, 128))
                self.assertLessEqual(reference["byte_size"], 100 * 1024)

    def test_video_seals_asr_ocr_frames_and_contact_sheet(self) -> None:
        result = self.seal(self.ready(2))
        self.assertTrue(result["ready"], result)
        roles = {row["role"] for row in result["evidence_files"]}
        self.assertTrue({"asr", "ocr", "frames_manifest", "video_frame", "duplicate_fingerprint"} <= roles)
        self.assertFalse(any(Path(row["path"]).suffix == ".mp4" for row in result["evidence_files"]))

    def test_existing_success_contract_allows_unavailable_audio(self) -> None:
        def no_audio(_source: Path, target: Path, **_kwargs: Any) -> Path:
            config = media.load_media_config()["asr"]
            media._atomic_json(target, {"status": "unavailable", "processor_version": media.processor_versions()["asr"],
                "model_id": config["model_id"], "model_revision": config["model_revision"],
                "language": config["language"], "text": "", "segments": [], "reason": "audio_decode_failed"})
            return target
        with patch.object(media, "_run_asr", side_effect=no_audio):
            bundle = self.ready(2)
        result = self.seal(bundle)
        self.assertTrue(result["ready"], result)
        receipt = json.loads(media._resolved(result["receipt"]["path"]).read_bytes())
        self.assertEqual(receipt["applicability"]["asr"], "unavailable")

    def test_missing_ocr_slot_does_not_release_originals(self) -> None:
        bundle = self.ready()
        with connect(self.db) as connection:
            connection.execute("UPDATE media_processing_slots SET status='retryable_failed' WHERE processor_type='ocr'")
        self.assert_blocked(self.seal(bundle), "ocr_slot_not_complete")
        self.assertTrue(all((bundle["originals_root"] / row["relative_path"]).is_file() for row in bundle["manifest"]["members"]))

    def test_missing_fingerprint_blocks_even_with_formal_evaluation(self) -> None:
        bundle = self.ready(fingerprint=False)
        self.assert_blocked(self.seal(bundle), "duplicate_fingerprint_artifact_missing")

    def test_hash_consistent_but_empty_fingerprint_is_rejected(self) -> None:
        bundle = self.ready()
        with connect(self.db) as connection:
            artifact = connection.execute("SELECT * FROM evidence_artifacts WHERE artifact_type='duplicate_fingerprint'").fetchone()
            path = media._resolved(artifact["local_path"])
            payload = json.loads(path.read_bytes())
            payload["media_sha256"] = []
            media._atomic_json(path, payload)
            body = path.read_bytes()
            connection.execute("UPDATE evidence_artifacts SET sha256=?,byte_size=? WHERE id=?", (hashlib.sha256(body).hexdigest(), len(body), artifact["id"]))
            connection.execute("UPDATE duplicate_fingerprints SET media_sha256_json='[]',payload_json=? WHERE artifact_id=?", (json.dumps(payload), artifact["id"]))
        self.assert_blocked(self.seal(bundle), "fingerprint_binding_or_members_invalid")

    def test_V1_and_no_evaluation_never_pass(self) -> None:
        bundle = self.ready(evaluate=False)
        self.assert_blocked(self.seal(bundle), "formal_V2_V3_evaluation_missing")
        evaluation.evaluate_content(1, db_path=self.db)
        with connect(self.db) as connection:
            connection.execute("UPDATE evaluation_versions SET evidence_level='V1',evaluation_status='insufficient_evidence'")
        self.assert_blocked(self.seal(bundle), "formal_V2_V3_evaluation_missing")

    def test_low_business_score_is_not_a_media_failure(self) -> None:
        bundle = self.ready()
        with connect(self.db) as connection:
            connection.execute("UPDATE evaluation_versions SET selling_point_score=0,selling_point_included=0")
        self.assertTrue(self.seal(bundle)["ready"])

    def test_envelope_bound_to_other_media_is_rejected(self) -> None:
        bundle = self.ready()
        with connect(self.db) as connection:
            connection.execute("UPDATE evidence_envelopes SET media_sha256=?", ("a" * 64,))
        self.assert_blocked(self.seal(bundle), "formal_V2_V3_evaluation_missing")

    def test_different_instance_processing_slot_cannot_be_reused(self) -> None:
        bundle = self.ready()
        with connect(self.db) as connection:
            connection.execute("UPDATE media_processing_slots SET source_sha256=? WHERE processor_type='ocr'", ("c" * 64,))
        self.assert_blocked(self.seal(bundle), "ocr_slot_not_complete")

    def test_inflight_processing_blocks_first_seal(self) -> None:
        bundle = self.ready()
        with connect(self.db) as connection:
            connection.execute("UPDATE media_processing_slots SET status='running' WHERE processor_type='ocr'")
        self.assert_blocked(self.seal(bundle), "media_processing_in_flight")

    def test_retained_evidence_damage_does_not_trigger_repair_or_resign(self) -> None:
        bundle = self.ready()
        result = self.seal(bundle)
        self.assertTrue(result["ready"], result)
        reference = next(row for row in result["evidence_files"] if row["role"] == "ocr")
        media._resolved(reference["path"]).write_bytes(b"broken fixture evidence")
        calls = len(self.fixture.calls)
        self.assertFalse(completion.verify_completion(bundle, db_path=self.db)["ready"])
        self.assertFalse(self.seal(bundle)["ready"])
        self.assertEqual(calls, len(self.fixture.calls))

    def test_text_metrics_and_time_changes_do_not_reseal_existing_proof(self) -> None:
        bundle = self.ready()
        first = self.seal(bundle)
        self.assertTrue(first["ready"], first)
        with connect(self.db) as connection:
            connection.execute("UPDATE content_items SET title='后续改标题不改封存媒体证据' WHERE id=1")
            before = connection.execute("SELECT COUNT(*) FROM evidence_artifacts").fetchone()[0]
        second = completion.seal_completion(bundle["bundle_id"], db_path=self.db, at="2030-01-01T00:00:00Z")
        self.assertTrue(second["ready"], second)
        self.assertEqual(first["receipt"], second["receipt"])
        with connect(self.db) as connection:
            self.assertEqual(before, connection.execute("SELECT COUNT(*) FROM evidence_artifacts").fetchone()[0])

    def test_new_source_does_not_replace_explicit_old_bundle_lease(self) -> None:
        bundle = self.ready()
        self.fixture._source(suffix="-new")
        with connect(self.db) as connection:
            self.assertIsNone(lifecycle.current_bundle(connection, 1))
        result = self.seal(bundle)
        self.assertTrue(result["ready"], result)
        self.assertTrue(completion.verify_completion(bundle, db_path=self.db)["ready"])

    def test_real_seal_archive_restore_expiry_preserves_evaluation_and_fingerprint(self) -> None:
        bundle = self.ready()
        with connect(self.db) as connection:
            original = dict(connection.execute("SELECT * FROM evidence_artifacts WHERE id=?", (bundle["manifest"]["original_artifact"]["artifact_id"],)).fetchone())
            evaluation_before = [dict(row) for row in connection.execute("SELECT * FROM evaluation_versions")]
            fingerprint_before = [dict(row) for row in connection.execute("SELECT * FROM duplicate_fingerprints")]
        calls = len(self.fixture.calls)
        archived = retention.archive_bundle(bundle["bundle_id"], db_path=self.db, at=self.fixture.now)
        self.assertEqual(archived["status"], "archived", archived)
        bundle = self.fixture._bundle()
        first_time = bundle["state"]["archive_verified_at"]
        deadline = bundle["state"]["delete_due_at"]
        self.assertFalse(any((bundle["originals_root"] / item["relative_path"]).exists() for item in bundle["manifest"]["members"]))
        self.assertTrue(completion.verify_completion(bundle, db_path=self.db)["ready"])
        restored = retention.restore_bundle(bundle["bundle_id"], db_path=self.db, at=self.fixture.now, request_id="completion-fixture-restore")
        self.assertEqual(restored["status"], "restored", restored)
        for member in bundle["manifest"]["members"]:
            self.assertEqual(hashlib.sha256((bundle["originals_root"] / member["relative_path"]).read_bytes()).hexdigest(), member["sha256"])
        self.fixture.now = (datetime.fromisoformat(deadline.replace("Z", "+00:00")) + timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
        expired = retention.purge_bundle(bundle["bundle_id"], db_path=self.db, at=self.fixture.now)
        self.assertEqual(expired["status"], "expired", expired)
        final = self.fixture._bundle()
        self.assertEqual((final["state"]["archive_verified_at"], final["state"]["delete_due_at"]), (first_time, deadline))
        self.assertTrue(completion.verify_completion(final, db_path=self.db)["ready"])
        evaluation.evaluate_content(1, db_path=self.db)
        duplicates.fingerprint_content(1, db_path=self.db)
        with connect(self.db) as connection:
            current = dict(connection.execute("SELECT * FROM evidence_artifacts WHERE id=?", (original["id"],)).fetchone())
            for key in ("id", "sha256", "created_at", "captured_at", "byte_size", "local_path"):
                self.assertEqual(original[key], current[key])
            self.assertEqual(evaluation_before, [dict(row) for row in connection.execute("SELECT * FROM evaluation_versions")])
            self.assertEqual(fingerprint_before, [dict(row) for row in connection.execute("SELECT * FROM duplicate_fingerprints")])
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0], 0)
        self.assertEqual(calls, len(self.fixture.calls))


if __name__ == "__main__":
    unittest.main()
