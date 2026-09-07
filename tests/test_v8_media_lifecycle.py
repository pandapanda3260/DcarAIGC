from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from v8 import durable_runs, media_lifecycle as lifecycle
from v8.storage import connect, initialize_database, now_utc, transaction


class MediaLifecycleFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.db = self.root / "fixture.sqlite3"
        self.archive = self.root / "archive"
        self.archive.mkdir(mode=0o700)
        self.media_root = self.root / "media"
        self.media_root.mkdir(mode=0o700)
        with connect(self.db) as connection:
            initialize_database(connection)
            at = now_utc()
            connection.execute(
                "INSERT INTO content_items(link_id,platform,platform_content_id,canonical_url,title,content_type,raw_account_uid,imported_at,created_at,updated_at) "
                "VALUES ('A2BC3D','douyin','123456789','https://www.douyin.com/video/123456789','fixture','video','99887766',?,?,?)",
                (at, at, at),
            )
        self.source_id = self.source("video")
        self.last_intent = None
        self.last_artifact_id = None
        self.last_slot_id = None

    def write(self, path: Path, body: bytes) -> Path:
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        path.write_bytes(body)
        path.chmod(0o600)
        return path

    def artifact(self, connection, kind, path, *, metadata=None, version="fixture-download-v1"):
        at = now_utc()
        body = path.read_bytes()
        cursor = connection.execute(
            "INSERT INTO evidence_artifacts(content_id,artifact_type,local_path,status,byte_size,sha256,captured_at,processor_version,metadata_json,created_at) "
            "VALUES (1,?,?,'available',?,?,?,?,?,?)",
            (kind, str(path), len(body), hashlib.sha256(body).hexdigest(), at, version,
             json.dumps(metadata or {}, sort_keys=True), at),
        )
        return int(cursor.lastrowid)

    def source(self, kind, *, suffix=""):
        path = self.root / f"source-{kind}{suffix}.json"
        body = json.dumps({"media_kind": kind, "urls": [f"https://example.invalid/{kind}{suffix}"]}).encode()
        self.write(path, body)
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE content_items SET content_type=? WHERE id=1", (kind,))
            return self.artifact(connection, "media_source", path, metadata={
                "media_kind": kind, "source_count": 1, "source_sha256": hashlib.sha256(body).hexdigest(),
                "raw_response_id": None,
            }, version="fixture-source-v1")

    def activate(self, *, mode="enrollment_only", canary=(1,), now=None, proofs=None):
        if proofs is None and mode == "active":
            proofs = {"contract_version": lifecycle.FIXTURE_PROOF_CONTRACT, "fixture_only": True,
                      "mac_consumers": True, "server_pairing": True, "canary_restore": True}
        with connect(self.db) as connection, transaction(connection):
            return lifecycle.activate(
                connection, mode=mode, activation_id="fixture-activation", release="fixture-release",
                rules_sha256="a" * 64, archive_root=self.archive, canary_content_ids=canary,
                proofs=proofs, now=now,
            )

    def slot(self, source_id=None, *, status="running", attempts=1, binding=None):
        with connect(self.db) as connection, transaction(connection):
            source = binding or connection.execute("SELECT sha256 FROM evidence_artifacts WHERE id=?", (source_id or self.source_id,)).fetchone()[0]
            at = now_utc()
            row = connection.execute(
                "INSERT INTO media_processing_slots(content_id,source_sha256,processor_type,processor_version,status,attempt_count,created_at,updated_at) "
                "VALUES (1,?,'download','fixture-download-v1',?,?,?,?)",
                (source, status, attempts, at, at),
            )
            return int(row.lastrowid)

    def prepare(self, slot_id=None):
        return lifecycle.prepare_download(1, self.source_id, self.media_root, self.db, slot_id)

    def register(self, intent, slot_id, *, extra_candidate=False, unlisted=False):
        kind = intent["media_kind"]
        metadata = {"source_sha256": intent["source"]["logical_sha256"], "keep_me": "unchanged"}
        if kind == "video":
            original = self.write(intent["originals_root"] / "video.mp4", b"fixture video bytes" * 100)
            artifact_kind = "media"
        else:
            paths = [self.write(intent["originals_root"] / f"image-{index:03d}.bin", bytes([index + 1]) * 1800)
                     for index in range(2)]
            groups = [{"group_index": index, "image_path": str(path),
                       "selected_response_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                       "selected_byte_size": path.stat().st_size} for index, path in enumerate(paths)]
            value = {"status": "complete", "source_count": 2,
                     "image_paths": [str(path) for path in paths],
                     "frames": [{"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()} for path in paths],
                     "groups": groups}
            original = self.write(intent["evidence_root"] / "download-manifest.json", json.dumps(value, sort_keys=True).encode())
            artifact_kind = "media_manifest"
            if extra_candidate:
                candidate = self.write(intent["originals_root"] / "alternative.bin", b"candidate" * 300)
                metadata["media_lifecycle_download_members"] = [{"path": str(candidate), "group_index": 0, "candidate_index": 1, "kind": "image_candidate"}]
        if unlisted:
            self.write(intent["originals_root"] / "unlisted.bin", b"not in manifest")
        with connect(self.db) as connection, transaction(connection):
            artifact_id = self.artifact(connection, artifact_kind, original, metadata=metadata)
            connection.execute("UPDATE media_processing_slots SET status='succeeded',output_artifact_id=?,updated_at=? WHERE id=?",
                               (artifact_id, now_utc(), slot_id))
            bundle = lifecycle.register_download(connection, intent, artifact_id, slot_id)
        self.last_intent, self.last_artifact_id, self.last_slot_id = intent, artifact_id, slot_id
        return bundle

    def registered(self, kind="video"):
        if kind != "video":
            self.source_id = self.source(kind)
        self.activate()
        slot_id = self.slot()
        intent = self.prepare(slot_id)
        self.assertIsNotNone(intent)
        return self.register(intent, slot_id)

    def archived(self, bundle):
        first = bundle["created_at"]
        due = (datetime.fromisoformat(first.replace("Z", "+00:00")) + timedelta(hours=72)).isoformat(timespec="seconds").replace("+00:00", "Z")
        with connect(self.db) as connection, transaction(connection):
            return lifecycle.update_state(connection, bundle, {
                "storage_state": "archived", "archive_verified_at": first, "delete_due_at": due,
                "archive_receipt": {"fixture_only": True},
            }, bundle["state"]["revision"])


class MediaLifecycleTest(MediaLifecycleFixture):
    def test_no_activation_has_no_intent_or_managed_directory(self):
        self.assertIsNone(self.prepare())
        self.assertFalse((self.media_root / "managed-v1").exists())
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM scheduler_runs").fetchone()[0], 0)

    def test_enrollment_only_excludes_non_canary(self):
        self.activate(canary=())
        self.assertIsNone(self.prepare())
        self.assertFalse((self.media_root / "managed-v1").exists())

    def test_active_requires_explicit_proofs_and_never_accepts_them_in_production(self):
        self.activate()
        with self.assertRaisesRegex(lifecycle.LifecycleError, "lifecycle_activation_proof_required"):
            self.activate(mode="active", proofs={})
        self.activate(mode="active")
        slot_id = self.slot()
        bundle = self.register(self.prepare(slot_id), slot_id)
        with connect(self.db) as connection, patch("v8.media_lifecycle.is_formal_database_path", return_value=True):
            with self.assertRaisesRegex(lifecycle.LifecycleError, "production_lifecycle_proofs_not_bound"):
                lifecycle.require_destructive_activation(connection, bundle)

    def test_activation_mode_change_preserves_cutover_and_exclusion_snapshot(self):
        first = self.activate(now="2026-08-29T01:02:03Z")
        second = self.activate(mode="active", now="2026-08-30T01:02:03Z")
        self.assertEqual(first["activated_at"], second["activated_at"])
        self.assertEqual(first["preexisting_started_slots"], second["preexisting_started_slots"])
        self.assertEqual(second["revision"], first["revision"] + 1)

    def test_pre_activation_started_slot_is_excluded_even_after_timestamp_update(self):
        slot_id = self.slot()
        self.activate()
        with connect(self.db) as connection:
            connection.execute("UPDATE media_processing_slots SET updated_at=? WHERE id=?", (now_utc(), slot_id))
        self.assertIsNone(self.prepare(slot_id))
        self.assertFalse((self.media_root / "managed-v1").exists())

    def test_preexisting_unstarted_slot_can_start_after_activation(self):
        slot_id = self.slot(status="pending", attempts=0)
        self.activate()
        with connect(self.db) as connection:
            connection.execute("UPDATE media_processing_slots SET status='running',attempt_count=1,updated_at=? WHERE id=?", (now_utc(), slot_id))
        self.assertIsNotNone(self.prepare(slot_id))

    def test_existing_artifact_is_not_adopted(self):
        path = self.write(self.root / "legacy.mp4", b"legacy video")
        with connect(self.db) as connection:
            metadata = json.loads(connection.execute("SELECT metadata_json FROM evidence_artifacts WHERE id=?", (self.source_id,)).fetchone()[0])
            self.artifact(connection, "media", path, metadata=metadata)
        self.activate()
        self.assertIsNone(self.prepare())
        self.assertEqual(path.read_bytes(), b"legacy video")

    def test_failed_intent_reuses_instance_and_directories(self):
        self.activate()
        slot_id = self.slot()
        first = self.prepare(slot_id)
        second = self.prepare(slot_id)
        self.assertEqual(first, second)
        self.assertIsInstance(first["originals_root"], Path)
        self.assertEqual(first["originals_root"].stat().st_mode & 0o777, 0o700)
        self.assertEqual(first["evidence_root"].stat().st_mode & 0o777, 0o700)

    def test_logical_download_source_is_not_confused_with_source_file_hash(self):
        logical_sha = "f" * 64
        with connect(self.db) as connection:
            row = connection.execute("SELECT sha256,metadata_json FROM evidence_artifacts WHERE id=?", (self.source_id,)).fetchone()
            metadata = json.loads(row["metadata_json"])
            metadata["source_sha256"] = logical_sha
            connection.execute("UPDATE evidence_artifacts SET metadata_json=? WHERE id=?", (json.dumps(metadata), self.source_id))
            file_sha = row["sha256"]
        self.activate()
        slot_id = self.slot(binding=logical_sha)
        bundle = self.register(self.prepare(slot_id), slot_id)
        self.assertEqual(bundle["manifest"]["source"]["sha256"], file_sha)
        self.assertEqual(bundle["manifest"]["download_slot"]["source_sha256"], logical_sha)
        self.assertNotEqual(file_sha, logical_sha)

    def test_image_download_binding_is_frozen_separately(self):
        self.source_id = self.source("image")
        self.activate()
        binding = "d" * 64
        slot_id = self.slot(binding=binding)
        intent = lifecycle.prepare_download(1, self.source_id, self.media_root, self.db,
                                            slot_id, download_source_sha256=binding)
        bundle = self.register(intent, slot_id)
        self.assertEqual(bundle["manifest"]["download_slot"]["source_sha256"], binding)
        self.assertEqual(bundle["state"]["protections"], {})

    def test_unbound_slot_hash_is_not_admitted(self):
        self.activate()
        slot_id = self.slot(binding="e" * 64)
        with self.assertRaisesRegex(lifecycle.LifecycleError, "download_slot_not_claimed"):
            self.prepare(slot_id)

    def test_directory_failure_keeps_original_durable_instance(self):
        self.activate()
        slot_id = self.slot()
        with patch("v8.media_lifecycle._prepare_directories", side_effect=OSError("fixture failure")):
            with self.assertRaises(OSError):
                self.prepare(slot_id)
        with connect(self.db) as connection:
            stored = json.loads(connection.execute("SELECT details_json FROM scheduler_runs WHERE job_id=?", (lifecycle.INTENT_JOB_ID,)).fetchone()[0])
        recovered = self.prepare(slot_id)
        self.assertEqual(recovered["bundle_id"], stored["bundle_id"])
        self.assertEqual(recovered["intent_run_id"], stored["intent_run_id"])

    def test_video_registration_binds_source_slot_and_original_without_changing_identity(self):
        bundle = self.registered()
        manifest = bundle["manifest"]
        self.assertEqual(manifest["source"]["artifact_id"], self.source_id)
        self.assertEqual(manifest["download_slot"]["id"], self.last_slot_id)
        self.assertEqual(manifest["archive_key"], "objects/" + bundle["bundle_id"])
        self.assertEqual([member["relative_path"] for member in manifest["members"]], ["video.mp4"])
        self.assertNotIn(str(self.archive), json.dumps(manifest))
        with connect(self.db) as connection:
            original = lifecycle.original_artifact(connection, bundle)
            self.assertEqual(json.loads(original["metadata_json"])["keep_me"], "unchanged")
            for key in ("sha256", "byte_size", "created_at", "captured_at", "processor_version"):
                self.assertEqual(original[key], manifest["original_artifact"][key])
            self.assertEqual(lifecycle.current_bundle(connection, 1)["bundle_id"], bundle["bundle_id"])
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0], 0)

    def test_complete_image_group_and_written_alternative_are_all_owned(self):
        self.source_id = self.source("image")
        self.activate()
        slot_id = self.slot()
        bundle = self.register(self.prepare(slot_id), slot_id, extra_candidate=True)
        members = bundle["manifest"]["members"]
        self.assertEqual([member["index"] for member in members], [0, 1, 2])
        self.assertEqual([member["kind"] for member in members], ["image", "image", "image_candidate"])
        self.assertEqual(len(members), 3)
        self.assertEqual(bundle["manifest"]["member_count"], 3)
        self.assertTrue((bundle["evidence_root"] / "download-manifest.json").is_file())
        self.assertNotIn("download-manifest.json", {member["relative_path"] for member in members})

    def test_unlisted_original_blocks_registration_and_slot_transaction_rolls_back(self):
        self.source_id = self.source("image")
        self.activate()
        slot_id = self.slot()
        intent = self.prepare(slot_id)
        with self.assertRaisesRegex(lifecycle.LifecycleError, "unregistered_original_members"):
            self.register(intent, slot_id, unlisted=True)
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT status FROM media_processing_slots WHERE id=?", (slot_id,)).fetchone()[0], "running")
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM evidence_artifacts WHERE artifact_type='media_lifecycle_manifest'").fetchone()[0], 0)

    def test_register_is_idempotent_with_original_intent(self):
        bundle = self.registered()
        with connect(self.db) as connection, transaction(connection):
            same = lifecycle.register_download(connection, self.last_intent, self.last_artifact_id, self.last_slot_id)
            self.assertEqual(same, bundle)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM evidence_artifacts WHERE artifact_type='media_lifecycle_manifest'").fetchone()[0], 1)

    def test_new_source_never_falls_back_to_old_available_bundle(self):
        self.registered()
        self.source("video", suffix="-new")
        with connect(self.db) as connection:
            self.assertIsNone(lifecycle.current_bundle(connection, 1))

    def test_hot_missing_original_preserves_identity_not_availability(self):
        bundle = self.registered()
        original_path = bundle["originals_root"] / "video.mp4"
        original_path.unlink()
        with connect(self.db) as connection:
            connection.execute("UPDATE evidence_artifacts SET status='missing' WHERE id=?", (self.last_artifact_id,))
        with connect(self.db) as connection:
            current = lifecycle.load_bundle(connection, bundle["bundle_id"])
            original = lifecycle.original_artifact(connection, current)
            self.assertEqual(original["status"], "missing")
            self.assertEqual(original["sha256"], bundle["manifest"]["original_artifact"]["sha256"])

    def test_cas_rejects_stale_revision_and_keeps_created_at(self):
        bundle = self.registered()
        with connect(self.db) as connection, transaction(connection):
            changed = lifecycle.update_state(connection, bundle, {"protections": [{"reason": "completion_gate_aged"}]}, 0)
        with connect(self.db) as connection, transaction(connection):
            with self.assertRaisesRegex(lifecycle.LifecycleError, "lifecycle_revision_conflict"):
                lifecycle.update_state(connection, bundle, {"operation_state": "blocked"}, 0)
        self.assertEqual(changed["created_at"], bundle["created_at"])
        self.assertEqual(changed["manifest_sha256"], bundle["manifest_sha256"])

    def test_first_archive_time_and_exact_72_hour_deadline_are_immutable(self):
        bundle = self.archived(self.registered())
        original = bundle["state"]
        first = datetime.fromisoformat(original["archive_verified_at"].replace("Z", "+00:00"))
        due = datetime.fromisoformat(original["delete_due_at"].replace("Z", "+00:00"))
        self.assertEqual(due - first, timedelta(hours=72))
        for key in ("archive_verified_at", "delete_due_at"):
            with self.subTest(key=key), connect(self.db) as connection, transaction(connection):
                with self.assertRaisesRegex(lifecycle.LifecycleError, "retention_time_immutable"):
                    lifecycle.update_state(connection, bundle, {key: "2099-01-01T00:00:00Z"}, original["revision"])
        with connect(self.db) as connection, transaction(connection):
            restored = lifecycle.update_state(connection, bundle, {"restore_receipts": [{"fixture": True}], "hot_members": ["m0000"]}, original["revision"])
        self.assertEqual(restored["state"]["delete_due_at"], original["delete_due_at"])

    def test_wrong_deadline_and_missing_archive_proof_are_rejected(self):
        bundle = self.registered()
        with connect(self.db) as connection, transaction(connection):
            with self.assertRaisesRegex(lifecycle.LifecycleError, "archive_receipt_required"):
                lifecycle.update_state(connection, bundle, {"storage_state": "archived", "archive_verified_at": bundle["created_at"]}, 0)
        with connect(self.db) as connection, transaction(connection):
            with self.assertRaisesRegex(lifecycle.LifecycleError, "retention_deadline_invalid"):
                lifecycle.update_state(connection, bundle, {"storage_state": "archived", "archive_verified_at": bundle["created_at"], "delete_due_at": bundle["created_at"], "archive_receipt": {"fixture": True}}, 0)

    def test_purging_does_not_reopen_and_expired_never_auto_reacquires(self):
        bundle = self.archived(self.registered())
        with connect(self.db) as connection, transaction(connection):
            bundle = lifecycle.update_state(connection, bundle, {"operation_state": "purging", "delete_intents": {"m0000": {"fixture": True}}}, 1)
        with connect(self.db) as connection, transaction(connection):
            with self.assertRaisesRegex(lifecycle.LifecycleError, "purging_cannot_reopen"):
                lifecycle.update_state(connection, bundle, {"operation_state": "idle"}, 2)
        with connect(self.db) as connection, transaction(connection):
            lifecycle.update_state(connection, bundle, {"storage_state": "expired", "operation_state": "idle", "deleted_at": bundle["state"]["delete_due_at"], "deleted_members": ["m0000"]}, 2)
        with self.assertRaisesRegex(lifecycle.LifecycleError, "expired_non_replayable"):
            self.prepare(self.last_slot_id)

    def test_manifest_tampering_is_not_trusted(self):
        bundle = self.registered()
        with connect(self.db) as connection:
            row = connection.execute("SELECT local_path FROM evidence_artifacts WHERE id=?", (bundle["control_artifact_id"],)).fetchone()
        self.write(Path(row[0]), b"{}")
        with connect(self.db) as connection:
            with self.assertRaisesRegex(lifecycle.LifecycleError, "lifecycle_artifact_bytes_changed"):
                lifecycle.load_bundle(connection, bundle["bundle_id"])

    def test_root_identity_change_blocks_archive_access(self):
        bundle = self.registered()
        old = self.root / "old-archive"
        self.archive.rename(old)
        self.archive.mkdir(mode=0o700)
        with connect(self.db) as connection:
            with self.assertRaisesRegex(lifecycle.LifecycleError, "archive_root_binding_changed"):
                lifecycle.archive_root_for_bundle(connection, bundle)

    def test_symlink_and_hardlink_originals_are_rejected(self):
        path = self.write(self.root / "private.bin", b"private")
        alias = self.root / "alias.bin"
        alias.symlink_to(path)
        with self.assertRaises(lifecycle.LifecycleError):
            lifecycle._file(alias)
        alias.unlink()
        os.link(path, alias)
        with self.assertRaisesRegex(lifecycle.LifecycleError, "lifecycle_file_not_private"):
            lifecycle._file(path)

    def test_attempt_fence_is_checked_and_bound_to_the_bundle(self):
        bundle = self.registered()
        wrong = durable_runs.claim_run("media_archive", {"bundle_id": "b" * 32}, db_path=self.db)
        with connect(self.db) as connection, transaction(connection):
            with self.assertRaisesRegex(lifecycle.LifecycleError, "lifecycle_owner_scope_mismatch"):
                lifecycle.update_state(connection, bundle, {"operation_state": "archiving"}, 0, wrong)
        owner = durable_runs.claim_run("media_archive", {"bundle_id": bundle["bundle_id"]}, db_path=self.db)
        with connect(self.db) as connection, transaction(connection):
            changed = lifecycle.update_state(connection, bundle, {"operation_state": "archiving"}, 0, owner)
        self.assertTrue(durable_runs.recover_run(
            owner.scheduler_run_id, expected_attempt_id=owner.attempt_id, db_path=self.db,
        ))
        durable_runs.claim_run("media_archive", {"bundle_id": bundle["bundle_id"]}, db_path=self.db)
        with connect(self.db) as connection, transaction(connection):
            with self.assertRaises(durable_runs.LostOwnership):
                lifecycle.update_state(connection, changed, {"operation_state": "blocked"}, 1, owner)


if __name__ == "__main__":
    unittest.main()
