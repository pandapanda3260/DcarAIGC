"""Real SQLite/filesystem lifecycle tests, with an isolated completion-gate stub.

The completion gate has its own integration suite. These tests exercise actual
copy/decode/locks/intents/CAS/unlink, never the formal DB or a network provider.
Only temporary registered bundles are deletion targets; fixture clocks are
explicitly not evidence that production's natural 72 hours have elapsed.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from PIL import Image

from v8 import media, media_lifecycle as lifecycle, media_retention as retention
from v8 import pipeline
from v8.scheduler import PIPELINE_REPORT_EXECUTION_LOCK
from v8.storage import connect, initialize_database, transaction

T0 = "2026-08-29T02:00:00Z"


class SimulatedCrash(BaseException):
    pass


class V8MediaRetentionTest(unittest.TestCase):
    def test_pipeline_report_lock_is_independent_from_local_processor_lock(self):
        with PIPELINE_REPORT_EXECUTION_LOCK:
            with retention._local_processor_lock():
                pass

        self.assertTrue(pipeline.LOCAL_PROCESSING_LOCK.acquire(blocking=False))
        try:
            self.assertTrue(PIPELINE_REPORT_EXECUTION_LOCK.acquire(blocking=False))
            PIPELINE_REPORT_EXECUTION_LOCK.release()
        finally:
            pipeline.LOCAL_PROCESSING_LOCK.release()

    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.db = self.root / "fixture.sqlite3"
        self.archive = self.root / "separate-archive"
        self.archive.mkdir(mode=0o700)
        self.media_root = self.root / "media"
        self.now = T0
        for module in (media, lifecycle, retention):
            self._patch(module, "now_utc", side_effect=lambda: self.now)
        self._patch(retention, "_completion", side_effect=self._verified_completion_fixture)
        self._patch(media.urllib.request, "urlopen", side_effect=AssertionError("network forbidden"))
        with connect(self.db) as connection:
            initialize_database(connection)
            connection.execute("INSERT INTO accounts(id,phone,created_at,updated_at) VALUES (1,'',?,?)", (T0, T0))
            connection.commit()
        self.bundle = self._bundle("image")
        self.bundle_id = self.bundle["bundle_id"]

    def _patch(self, target: Any, name: str, **kwargs: Any) -> Any:
        patcher = patch.object(target, name, **kwargs)
        value = patcher.start()
        self.addCleanup(patcher.stop)
        return value

    def _json_file(self, path: Path, value: Any) -> None:
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")
        path.chmod(0o600)

    def _activate(self, *, mode: str = "active", proofs: bool = True) -> None:
        with connect(self.db) as connection, transaction(connection):
            lifecycle.activate(connection, mode=mode, activation_id="fixture-activation", release="fixture-release",
                rules_sha256="f" * 64, archive_root=self.archive, canary_content_ids=(1, 2, 3), now=T0,
                proofs={"contract_version": lifecycle.FIXTURE_PROOF_CONTRACT, "fixture_only": True,
                        "mac_consumers": True, "server_pairing": True, "canary_restore": True} if proofs else {})

    def _bundle(self, kind: str, content_id: int = 1) -> dict[str, Any]:
        link_id = f"T{content_id:05d}"
        source_path = self.root / f"source-{content_id}.json"
        self._json_file(source_path, {"media_kind": kind, "platform_content_id": str(content_id)})
        with connect(self.db) as connection, transaction(connection):
            connection.execute("""INSERT INTO content_items(id,link_id,platform,platform_content_id,canonical_url,
                account_id,raw_account_uid,title,content_type,published_at,imported_at,created_at,updated_at)
                VALUES (?,?,'douyin',?,'https://www.douyin.com/video/fixture',1,'fixture-uid','fixture',?,?,?,?,?)""",
                (content_id, link_id, str(content_id), kind, T0, T0, T0, T0))
            source = media.register_artifact(connection, content_id=content_id, artifact_type="media_source",
                path=source_path, processor_version="source-fixture-v1", captured_at=T0,
                metadata={"media_kind": kind, "source_sha256": "a" * 64})
        if content_id == 1:
            self._activate()
        intent = lifecycle.prepare_download(content_id, source.id, self.media_root, self.db)
        assert intent is not None
        original_root = intent["originals_root"]
        if kind == "image":
            paths = []
            frames = []
            groups = []
            for index, color in enumerate(((50, 80, 140), (180, 90, 70))):
                image_path = original_root / f"image-{index}.jpg"
                Image.new("RGB", (20 + index, 16), color).save(image_path, "JPEG")
                image_path.chmod(0o600)
                path = str(image_path)
                digest = hashlib.sha256(image_path.read_bytes()).hexdigest()
                paths.append(path)
                frames.append({"path": path, "sha256": digest})
                groups.append({"group_index": index, "image_path": path, "selected_response_sha256": digest,
                               "selected_byte_size": image_path.stat().st_size})
            original_path = intent["evidence_root"] / "download-manifest.json"
            self._json_file(original_path, {"status": "complete", "source_count": 2, "image_paths": paths,
                                            "frames": frames, "groups": groups})
            artifact_type = "media_manifest"
        else:
            original_path = original_root / "source.mp4"
            subprocess.run(["ffmpeg", "-nostdin", "-v", "error", "-f", "lavfi", "-i", "color=c=blue:s=32x32:d=0.1",
                            "-c:v", "libx264", "-pix_fmt", "yuv420p", str(original_path)], check=True, capture_output=True)
            original_path.chmod(0o600)
            artifact_type = "media"
        with connect(self.db) as connection, transaction(connection):
            artifact = media.register_artifact(connection, content_id=content_id, artifact_type=artifact_type,
                path=original_path, processor_version="download-fixture-v1", captured_at=self.now,
                metadata={"source_sha256": "a" * 64})
            slot = connection.execute("""INSERT INTO media_processing_slots(content_id,processor_type,source_sha256,
                 processor_version,status,attempt_count,output_artifact_id,created_at,updated_at)
                 VALUES (?,'download',?,'download-fixture-v1','succeeded',1,?,?,?)""",
                 (content_id, intent["download_source_sha256"], artifact.id, self.now, self.now))
            bundle = lifecycle.register_download(connection, intent, artifact.id, int(slot.lastrowid or 0))
        proof = bundle["evidence_root"] / "completion-fixture.json"
        self._json_file(proof, {"fixture_only": True, "bundle_id": bundle["bundle_id"]})
        with connect(self.db) as connection, transaction(connection):
            artifact = media.register_artifact(connection, content_id=content_id, artifact_type="fixture_completion",
                                               path=proof, processor_version="fixture-only-v1")
            reference = {"artifact_id": artifact.id, "path": str(proof), "sha256": artifact.sha256,
                         "byte_size": proof.stat().st_size}
            lifecycle.update_state(connection, bundle, {"completion_receipt": reference}, expected_revision=0)
        return retention._load(bundle["bundle_id"], self.db)

    def _verified_completion_fixture(self, bundle: dict[str, Any], _db: Path) -> dict[str, Any]:
        reference = bundle["state"].get("completion_receipt")
        if not reference or hashlib.sha256(Path(reference["path"]).read_bytes()).hexdigest() != reference["sha256"]:
            raise lifecycle.LifecycleError("fixture_completion_missing")
        return {"ready": True, "receipt": reference, "blockers": [], "evidence_files": [reference]}

    def _archive(self, *, release_hot: bool = True, bundle: dict[str, Any] | None = None) -> dict[str, Any]:
        value = bundle or self.bundle
        return retention._execute("archive", value["bundle_id"], db_path=self.db, at=self.now, release_hot=release_hot)

    def _load(self) -> dict[str, Any]:
        return retention._load(self.bundle_id, self.db)

    def _paths(self, *, archive: bool = False) -> list[Path]:
        root = self.archive / "objects" / self.bundle_id if archive else self.bundle["originals_root"]
        return [root / item["relative_path"] for item in self.bundle["manifest"]["members"]]

    def _availability(self) -> dict[str, Any]:
        with connect(self.db) as connection:
            return retention.original_availability(connection, 1, at=self.now)

    def test_archive_real_all_image_restore_and_frozen_72_hours(self) -> None:
        original = [path.read_bytes() for path in self._paths()]
        self.assertEqual(self._archive()["status"], "archived")
        self.assertTrue(all(not path.exists() for path in self._paths()))
        self.assertEqual([path.read_bytes() for path in self._paths(archive=True)], original)
        cold = self._load()
        self.assertEqual(cold["state"]["archive_verified_at"], T0)
        self.assertEqual(cold["state"]["delete_due_at"], "2026-09-01T02:00:00Z")
        self.assertTrue(self._availability()["can_restore"])
        self.now = "2026-08-30T02:00:00Z"
        self.assertEqual(retention.restore_bundle(self.bundle_id, db_path=self.db, at=self.now, request_id="restore-1")["status"], "restored")
        self.assertEqual([path.read_bytes() for path in self._paths()], original)
        restored = self._load()
        self.assertEqual(restored["state"]["delete_due_at"], cold["state"]["delete_due_at"])
        self.assertEqual(restored["manifest"], cold["manifest"])
        self.assertEqual(self._availability()["http_status"], 200)

    def test_at_72_hours_delete_all_originals_keep_manifest_and_evidence(self) -> None:
        self.assertEqual(self._archive()["status"], "archived")
        frozen_manifest = Path(self.bundle["manifest"]["original_artifact"]["local_path"]).read_bytes()
        self.now = "2026-09-01T01:59:59Z"
        self.assertEqual(retention.purge_bundle(self.bundle_id, db_path=self.db, at=self.now)["reason"], "retention_not_due")
        self.assertTrue(all(path.exists() for path in self._paths(archive=True)))
        # The prior bounded partial is not due until its scheduled resume.
        self.now = "2026-09-01T02:05:00Z"
        self.assertEqual(self._availability()["reason"], "original_expiry_pending")
        self.assertFalse(self._availability()["can_restore"])
        result = retention.purge_bundle(self.bundle_id, db_path=self.db, at=self.now)
        self.assertEqual(result["status"], "expired", result)
        self.assertTrue(all(not path.exists() for path in self._paths(archive=True)))
        bundle = self._load()
        self.assertEqual(self._availability()["http_status"], 410)
        self.assertEqual(Path(bundle["manifest"]["original_artifact"]["local_path"]).read_bytes(), frozen_manifest)
        self.assertTrue(Path(bundle["state"]["completion_receipt"]["path"]).exists())
        self.assertTrue(Path(bundle["state"]["deletion_receipt"]["path"]).exists())
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM content_items").fetchone()[0], 1)
            self.assertEqual(lifecycle.original_artifact(connection, bundle)["status"], "available")

    def test_real_video_decode_missing_available_preserves_identity(self) -> None:
        video = self._bundle("video", 2)
        with connect(self.db) as connection:
            initial = lifecycle.original_artifact(connection, video)
        result = self._archive(bundle=video)
        self.assertEqual(result["status"], "archived", result)
        with connect(self.db) as connection:
            cold = lifecycle.original_artifact(connection, video)
        self.assertEqual(cold["status"], "missing")
        self.now = "2026-08-29T03:00:00Z"
        result = retention.restore_bundle(video["bundle_id"], db_path=self.db, at=self.now, request_id="video-restore")
        self.assertEqual(result["status"], "restored", result)
        with connect(self.db) as connection:
            restored = lifecycle.original_artifact(connection, video)
        self.assertEqual(initial, restored)

    def test_enrollment_copy_does_not_release_without_real_gate(self) -> None:
        self._activate(mode="enrollment_only", proofs=False)
        result = self._archive(release_hot=False)
        self.assertEqual(result["status"], "verified_copy_only", result)
        self.assertTrue(all(path.exists() for path in self._paths()))
        self.assertTrue(all(path.exists() for path in self._paths(archive=True)))
        result = self._archive()
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["reason"], "lifecycle_activation_proof_required")
        self.assertTrue(all(path.exists() for path in self._paths()))

    def test_failed_full_decode_never_starts_deadline_or_deletes_original(self) -> None:
        self._patch(retention, "_decode_member", side_effect=lifecycle.LifecycleError("restore_image_decode_failed"))
        result = self._archive()
        self.assertEqual(result["status"], "partial")
        self.assertIsNone(self._load()["state"]["archive_verified_at"])
        self.assertTrue(all(path.exists() for path in self._paths()))

    def test_archive_receipt_corruption_blocks_purge(self) -> None:
        self.assertEqual(self._archive()["status"], "archived")
        path = Path(self._load()["state"]["archive_receipt"]["path"])
        path.write_bytes(path.read_bytes() + b" ")
        self.now = "2026-09-01T02:00:00Z"
        result = retention.purge_bundle(self.bundle_id, db_path=self.db, at=self.now)
        self.assertEqual(result["reason"], "archive_receipt_changed")
        self.assertNotEqual(self._load()["state"]["operation_state"], "purging")
        self.assertTrue(all(path.exists() for path in self._paths(archive=True)))

    def test_unexplained_missing_archive_member_cannot_be_signed_deleted(self) -> None:
        self._archive()
        self._paths(archive=True)[0].unlink()
        self.now = "2026-09-01T02:00:00Z"
        result = retention.purge_bundle(self.bundle_id, db_path=self.db, at=self.now)
        self.assertEqual(result["reason"], "original_missing_without_delete_intent")
        self.assertNotEqual(self._load()["state"]["operation_state"], "purging")
        self.assertTrue(self._paths(archive=True)[1].exists())

    def test_foreign_file_inside_archive_is_preserved_and_blocks_deletion(self) -> None:
        self.assertEqual(self._archive()["status"], "archived")
        foreign = self._paths(archive=True)[0].parent / "not-in-registered-manifest.bin"
        foreign.write_bytes(b"not ours to delete")
        foreign.chmod(0o600)
        self.now = "2026-09-01T02:00:00Z"
        result = retention.purge_bundle(self.bundle_id, db_path=self.db, at=self.now)
        self.assertEqual(result["reason"], "archive_pack_unowned_path")
        self.assertEqual(foreign.read_bytes(), b"not ours to delete")
        self.assertTrue(all(path.exists() for path in self._paths(archive=True)))
        self.assertNotEqual(self._load()["state"]["operation_state"], "purging")

    def test_crash_after_unlink_resumes_intent_without_recreating_missing_bytes(self) -> None:
        self._archive()
        self.now = "2026-09-01T02:00:00Z"
        with patch.object(retention, "_after_unlink", side_effect=SimulatedCrash()):
            with self.assertRaises(SimulatedCrash):
                retention.purge_bundle(self.bundle_id, db_path=self.db, at=self.now)
        self.assertEqual(self._load()["state"]["operation_state"], "purging")
        self.assertEqual(self._availability()["reason"], "original_purge_in_progress")
        with self.assertRaisesRegex(lifecycle.LifecycleError, "original_purge_in_progress"):
            retention.request_restore(1, self.bundle_id, "manual", db_path=self.db)
        result = retention.purge_bundle(self.bundle_id, db_path=self.db, at=self.now)
        self.assertEqual(result["status"], "expired", result)
        with connect(self.db) as connection:
            attempts = connection.execute("SELECT status FROM scheduler_run_attempts WHERE scheduler_run_id=? ORDER BY attempt_number",
                                          (result["run_id"],)).fetchall()
        self.assertEqual([row[0] for row in attempts], ["interrupted", "succeeded"])

    def test_restore_collision_never_overwrites_different_bytes(self) -> None:
        self._archive()
        path = self._paths()[0]
        path.write_bytes(b"different owned by nobody")
        path.chmod(0o600)
        self.now = "2026-08-29T03:00:00Z"
        result = retention.restore_bundle(self.bundle_id, db_path=self.db, at=self.now, request_id="conflict")
        self.assertEqual(result["reason"], "member_identity_changed")
        self.assertEqual(path.read_bytes(), b"different owned by nobody")

    def test_shared_artifact_reference_blocks_archive_before_copy(self) -> None:
        with connect(self.db) as connection, transaction(connection):
            media.register_artifact(connection, content_id=1, artifact_type="manual_original_reference",
                                     path=self._paths()[0], processor_version="fixture-ref")
        result = self._archive()
        self.assertEqual(result["reason"], "original_shared_by_other_artifact")
        self.assertTrue(all(path.exists() for path in self._paths()))

    def test_hardlink_and_symlink_are_rejected(self) -> None:
        path = self._paths()[0]
        hardlink = self.root / "unmanaged-hardlink.jpg"
        os.link(path, hardlink)
        result = self._archive()
        self.assertEqual(result["status"], "partial")
        self.assertTrue(path.exists())
        hardlink.unlink()
        moved = self.root / "outside.jpg"
        path.rename(moved)
        path.symlink_to(moved)
        self.now = "2026-08-29T02:06:00Z"
        result = self._archive()
        self.assertEqual(result["status"], "partial")
        self.assertTrue(moved.exists())

    def test_identity_replacement_after_intent_is_not_deleted(self) -> None:
        def replace(path: Path, _member: Any) -> None:
            if not path.is_relative_to(self.bundle["originals_root"]):
                return  # Target the real hot-release boundary, not test-copy cleanup.
            path.rename(self.root / "preserved-original.jpg")
            path.write_bytes(b"unowned replacement")
            path.chmod(0o600)
        with patch.object(retention, "_before_unlink", side_effect=replace):
            result = self._archive()
        self.assertEqual(result["status"], "partial")
        self.assertTrue((self.root / "preserved-original.jpg").exists())
        self.assertEqual(self._paths()[0].read_bytes(), b"unowned replacement")
        self.assertIsNotNone(self._load()["state"]["archive_verified_at"])
        self.assertIn("initial_release:hot:m0000", self._load()["state"]["delete_intents"])

    def test_read_lease_blocks_purge_and_nested_read_can_finish_after_deadline(self) -> None:
        self._archive()
        self.now = "2026-08-29T03:00:00Z"
        retention.restore_bundle(self.bundle_id, db_path=self.db, at=self.now, request_id="lease")
        with retention.media_read_lease(1, db_path=self.db, purpose="media_processing"):
            self.now = "2026-09-01T02:00:00Z"
            with retention.media_read_lease(1, db_path=self.db, purpose="duplicate_fingerprint"):
                self.assertTrue(all(path.exists() for path in self._paths()))
            result = retention.purge_bundle(self.bundle_id, db_path=self.db, at=self.now)
            self.assertEqual(result["reason"], "media_bundle_busy")
        with self.assertRaisesRegex(lifecycle.LifecycleError, "original_expiry_pending"):
            with retention.media_read_lease(1, db_path=self.db, purpose="file_response"):
                self.fail("new reader was granted after deadline")
        with retention.media_read_lease(1, db_path=self.db, purpose="retained_evidence_evaluation", require_original=False):
            pass
        self.assertEqual(retention.purge_bundle(self.bundle_id, db_path=self.db, at=self.now)["status"], "expired")

    def test_24_hour_hot_release_uses_last_reader_but_does_not_change_deadline(self) -> None:
        self._archive()
        self.now = "2026-08-29T03:00:00Z"
        retention.restore_bundle(self.bundle_id, db_path=self.db, at=self.now, request_id="hot")
        self.now = "2026-08-29T05:00:00Z"
        with retention.media_read_lease(1, db_path=self.db, purpose="file_response"):
            pass
        self.now = "2026-08-30T03:00:00Z"
        self.assertEqual(retention.release_restored_hot(self.bundle_id, db_path=self.db, at=self.now)["reason"], "restored_hot_not_due")
        self.now = "2026-08-30T05:01:00Z"
        result = retention.release_restored_hot(self.bundle_id, db_path=self.db, at=self.now)
        self.assertEqual(result["status"], "hot_released", result)
        self.assertTrue(all(not path.exists() for path in self._paths()))
        self.assertTrue(all(path.exists() for path in self._paths(archive=True)))
        self.assertEqual(self._load()["state"]["delete_due_at"], "2026-09-01T02:00:00Z")

    def test_aged_incomplete_bundle_is_visible_protected_idempotent_and_not_auto_cleared(self) -> None:
        bundle = self._load()
        with connect(self.db) as connection, transaction(connection):
            lifecycle.update_state(connection, bundle, {"completion_receipt": None}, expected_revision=bundle["state"]["revision"])
        self.now = "2026-09-12T01:59:59Z"
        self.assertEqual(retention.age_incomplete_bundles(db_path=self.db, at=self.now)["added"], [])
        self.now = "2026-09-12T02:00:00Z"
        self.assertEqual(retention.age_incomplete_bundles(db_path=self.db, at=self.now)["added"], [self.bundle_id])
        first = self._load()
        self.now = "2026-09-13T02:00:00Z"
        self.assertEqual(retention.age_incomplete_bundles(db_path=self.db, at=self.now)["added"], [])
        self.assertEqual(self._load()["state"]["completion_gate_aged"], first["state"]["completion_gate_aged"])
        with connect(self.db) as connection, transaction(connection):
            lifecycle.update_state(connection, self._load(), {"completion_receipt": self.bundle["state"]["completion_receipt"]},
                                   expected_revision=first["state"]["revision"])
        result = self._archive()
        self.assertEqual(result["reason"], "bundle_protected")
        with connect(self.db) as connection:
            todo = retention.lifecycle_summary(connection, at=self.now)["manual_todos"][0]
        self.assertTrue(todo["protected"])
        self.assertTrue(todo["evidence_ready"])
        self.assertTrue(all(path.exists() for path in self._paths()))

    def test_restore_queue_has_no_file_copy_and_expired_pending_request_stops(self) -> None:
        self._archive()
        self.now = "2026-08-29T03:00:00Z"
        with connect(self.db) as connection:
            connection.execute("UPDATE accounts SET enabled=0 WHERE id=1")
            connection.commit()
        request = retention.request_restore(1, self.bundle_id, "manual", db_path=self.db)
        self.assertEqual(request["http_status"], 202)
        self.assertTrue(all(not path.exists() for path in self._paths()))
        self.assertEqual(retention.request_restore(1, self.bundle_id, "manual", db_path=self.db)["run_id"], request["run_id"])
        self.now = "2026-09-01T02:00:00Z"
        result = retention.run_lifecycle_jobs(db_path=self.db, at=self.now, include_archive=False)
        self.assertEqual([entry["status"] for entry in result["results"]], ["failed", "expired"], result)
        with connect(self.db) as connection:
            run = connection.execute("SELECT status,details_json FROM scheduler_runs WHERE id=?", (request["run_id"],)).fetchone()
        self.assertEqual(run["status"], "failed")
        self.assertNotIn("next_resume_at", json.loads(run["details_json"]))
        self.assertEqual(self._load()["state"]["restore_request"]["reason"], "retention_due")

    def test_readonly_replica_never_opens_archive_or_creates_lock(self) -> None:
        self._archive()
        with connect(self.db) as connection:
            value = retention.original_availability(connection, 1, at=self.now, replica=True)
        self.assertEqual(value["reason"], "replica_original_omitted")
        self.assertFalse(value["can_restore"])
        with patch.dict(os.environ, {"DCAR_READ_ONLY": "1"}):
            with self.assertRaisesRegex(lifecycle.LifecycleError, "read_only_lifecycle_operation"):
                retention.request_restore(1, self.bundle_id, "manual", db_path=self.db)

    def test_capacity_reserves_all_inflight_bytes_by_actual_volume(self) -> None:
        fake = shutil._ntuple_diskusage(1000, 100, retention.MIN_FREE_BYTES + 30)
        with patch.object(retention.shutil, "disk_usage", return_value=fake):
            with retention.media_space_reservation(db_path=self.db, allocations=[(self.root, 20)], purpose="first"):
                with self.assertRaisesRegex(lifecycle.LifecycleError, "media_space_below_reserve"):
                    with retention.media_space_reservation(db_path=self.db, allocations=[(self.archive, 11)], purpose="second"):
                        self.fail("same-volume concurrent capacity was not reserved")
            with retention.media_space_reservation(db_path=self.db, allocations=[(self.archive, 30)], purpose="after-release"):
                pass

    def test_bundle_lock_rejects_concurrent_thread_and_has_no_wait(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        errors: list[BaseException] = []
        def reader() -> None:
            try:
                with retention.media_read_lease(1, db_path=self.db, purpose="file_response"):
                    entered.set()
                    if not release.wait(5):
                        raise AssertionError("test reader timeout")
            except BaseException as error:
                errors.append(error)
        thread = threading.Thread(target=reader)
        thread.start()
        try:
            self.assertTrue(entered.wait(5))
            self.assertEqual(self._archive()["reason"], "media_bundle_busy")
        finally:
            release.set()
            thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])

    def test_formal_clock_override_and_path_injection_rejected_before_side_effect(self) -> None:
        with patch.object(retention, "is_formal_database_path", return_value=True):
            with self.assertRaisesRegex(lifecycle.LifecycleError, "formal_clock_override_forbidden"):
                retention.purge_bundle(self.bundle_id, db_path=self.db, at=T0)
        with self.assertRaisesRegex(lifecycle.LifecycleError, "bundle_id_unsafe"):
            retention.request_restore(1, "../../outside", "manual", db_path=self.db)
        self.assertFalse((self.root / "outside.lock").exists())

    def test_copy_published_but_control_commit_crash_preserves_hot_and_starts_new_actual_T(self) -> None:
        actual = lifecycle.update_state
        def interrupted(connection: Any, bundle: Any, changes: Any, *args: Any, **kwargs: Any) -> Any:
            if changes.get("archive_verified_at"):
                raise SimulatedCrash()
            return actual(connection, bundle, changes, *args, **kwargs)
        with patch.object(lifecycle, "update_state", side_effect=interrupted):
            with self.assertRaises(SimulatedCrash):
                self._archive()
        self.assertIsNone(self._load()["state"]["archive_verified_at"])
        self.assertTrue(all(path.exists() for path in self._paths()))
        self.assertTrue(all(path.exists() for path in self._paths(archive=True)))
        self.now = "2026-08-29T02:10:00Z"
        result = self._archive()
        self.assertEqual(result["status"], "archived", result)
        self.assertEqual(result["archive_verified_at"], self.now)
        self.assertEqual(result["delete_due_at"], "2026-09-01T02:10:00Z")

    def test_queue_crash_between_request_and_run_is_repaired_without_recopies(self) -> None:
        self._archive()
        self.now = "2026-08-29T03:00:00Z"
        with patch.object(retention.durable_runs, "claim_run", side_effect=SimulatedCrash()):
            with self.assertRaises(SimulatedCrash):
                retention.request_restore(1, self.bundle_id, "manual", db_path=self.db)
        frozen_request = self._load()["state"]["restore_request"]["request_id"]
        request = retention.request_restore(1, self.bundle_id, "manual", db_path=self.db)
        self.assertEqual(request["request_id"], frozen_request)
        self.assertIsInstance(request["run_id"], int)
        self.assertTrue(all(not path.exists() for path in self._paths()))
        result = retention.run_lifecycle_jobs(db_path=self.db, at=self.now, include_archive=False)
        self.assertEqual(result["results"][0]["status"], "restored", result)
        self.assertEqual(self._load()["state"]["delete_due_at"], "2026-09-01T02:00:00Z")

    def test_registered_jobs_require_activation_and_use_fixed_hourly_cadence(self) -> None:
        from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore[import-untyped]

        scheduler = BackgroundScheduler()
        retention.install_lifecycle_jobs(scheduler, db_path=self.db)
        jobs = {job.id: job for job in scheduler.get_jobs()}
        self.assertEqual(set(jobs), {"media_archive", "media_retention", "media_restore"})
        self.assertIn("minute='40'", str(jobs["media_archive"].trigger))
        self.assertIn("minute='30'", str(jobs["media_retention"].trigger))
        self.assertIn("0:05:00", str(jobs["media_restore"].trigger))
        self.assertTrue(jobs["media_retention"].kwargs["include_purge"])
        self.assertFalse(jobs["media_restore"].kwargs["include_purge"])
        empty = self.root / "not-activated.sqlite3"
        with connect(empty) as connection:
            initialize_database(connection)
        unactivated = BackgroundScheduler()
        retention.install_lifecycle_jobs(unactivated, db_path=empty)
        self.assertEqual(unactivated.get_jobs(), [])
        with patch.dict(os.environ, {"DCAR_READ_ONLY": "1"}):
            readonly = BackgroundScheduler()
            retention.install_lifecycle_jobs(readonly, db_path=self.db)
        self.assertEqual(readonly.get_jobs(), [])
        self._activate(mode="enrollment_only")
        enrolling = BackgroundScheduler()
        retention.install_lifecycle_jobs(enrolling, db_path=self.db)
        self.assertEqual(enrolling.get_jobs(), [])
        self.assertEqual(retention.run_lifecycle_jobs(db_path=self.db, at=self.now)["reason"], "media_activation_required")

    def test_one_rule_file_prohibits_trash_or_another_retention_period(self) -> None:
        from v8.media_policy import POLICY, load_media_policy

        self.assertEqual(POLICY["archive_retention_hours"], 72)
        self.assertFalse(POLICY["trash_phase"])
        self.assertEqual(POLICY["completion_gate_aged_days"], 14)
        changed = {key: value for key, value in POLICY.items() if key != "sha256"}
        changed["archive_retention_hours"] = 30 * 24
        path = self.root / "changed-policy.json"
        self._json_file(path, changed)
        with self.assertRaisesRegex(ValueError, "versioned upgrade"):
            load_media_policy(path)

    def test_normal_new_file_does_not_authorize_other_consumer_to_inherit_lease(self) -> None:
        with retention.media_read_lease(1, db_path=self.db, purpose="media_processing"):
            with self.assertRaisesRegex(lifecycle.LifecycleError, "new_consumer_cannot_inherit_lease"):
                with retention.media_read_lease(1, db_path=self.db, purpose="file_response"):
                    self.fail("independent file response inherited a processing lease")

    def test_budgeted_pass_prioritizes_expiry_then_restore_then_archive(self) -> None:
        self._archive()
        self.now = "2026-08-29T03:00:00Z"
        second = self._bundle("image", 2)
        self.assertEqual(self._archive(bundle=second)["status"], "archived")
        self.now = "2026-09-01T01:00:00Z"
        request = retention.request_restore(2, second["bundle_id"], "manual", db_path=self.db)
        third = self._bundle("image", 3)
        self.now = "2026-09-01T02:00:00Z"
        # Unit boundary only: these bundles already have the independently
        # verified completion fixture. Exercise every physical operation here.
        def ready_archive(bundle_id: str, **kwargs: Any) -> dict[str, Any]:
            return retention._execute("archive", bundle_id, **kwargs)
        with patch.object(retention, "archive_bundle", side_effect=ready_archive):
            result = retention.run_lifecycle_jobs(db_path=self.db, at=self.now)
        self.assertEqual([item["status"] for item in result["results"]], ["expired", "restored", "archived"], result)
        self.assertEqual([item["bundle_id"] for item in result["results"]],
                         [self.bundle_id, second["bundle_id"], third["bundle_id"]])
        self.assertEqual(result["results"][1]["run_id"], request["run_id"])


if __name__ == "__main__":
    unittest.main()
