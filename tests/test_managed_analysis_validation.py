"""New read-only acceptance never relaxes the legacy analysis contracts."""
from __future__ import annotations

import copy
import hashlib
import io
import json
import sqlite3
import unittest
from contextlib import closing, redirect_stdout
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

from scripts import managed_analysis_validation as managed
from scripts import run_full_local_analysis_batches as batches
from scripts import run_local_analysis_canary as canary
from tests import test_v8_media_completion as completion_fixtures
from v8 import media, media_completion as completion, media_lifecycle as lifecycle, media_retention as retention
from v8.storage import connect


class ManagedAnalysisValidationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.completion_fixture = completion_fixtures.MediaCompletionTest()
        self.completion_fixture.setUp()
        self.addCleanup(self.completion_fixture.doCleanups)
        self.fixture = self.completion_fixture.fixture
        self.db = self.fixture.db

    def prepare(self, kind: str = "image", stage: str = "sealed") -> dict[str, Any]:
        content_id = 1 if kind == "image" else 2
        if stage == "sealed":
            bundle = self.completion_fixture.ready(content_id)
            result = completion.seal_completion(bundle["bundle_id"], db_path=self.db)
            self.assertTrue(result["ready"], result)
        else:
            self.fixture._source(content_id)
            self.fixture._activate()
            self.fixture._download(content_id, process=stage == "processed")
        return self.fixture._bundle(content_id)

    def contract(self, bundle: dict[str, Any], stage: str = "sealed", originals: str = "online") -> dict[str, Any]:
        with connect(self.db) as connection:
            current = lifecycle.load_bundle(connection, bundle["bundle_id"])
            ids = [row[0] for row in connection.execute(
                "SELECT id FROM evidence_artifacts WHERE json_valid(metadata_json) AND json_extract(metadata_json,'$.media_lifecycle.bundle_id')=? ORDER BY id",
                (bundle["bundle_id"],),
            )]
        return {"schema_version": managed.MANAGED_CONTRACT_VERSION, "database": str(self.db), "media_root": str(self.fixture.media_root),
                "bundles": [{"content_id": current["manifest"]["content_id"], "bundle_id": current["bundle_id"],
                    "manifest_sha256": current["manifest_sha256"], "stage": stage, "storage_state": current["state"]["storage_state"],
                    "originals_state": originals, "artifact_ids": ids}]}

    def verify_both(self, contract: dict[str, Any]) -> dict[str, Any]:
        calls = len(self.fixture.calls)
        first = canary.validate_managed_v1(contract)
        second = batches.validate_managed_v1(contract)
        self.assertEqual(first, second)
        self.assertTrue(first["ok"])
        self.assertEqual((first["provider_calls"], first["mutations"]), (0, 0))
        self.assertEqual(calls, len(self.fixture.calls))
        return dict(first)

    def test_downloaded_image_has_exact_two_owned_artifact_types(self) -> None:
        bundle = self.prepare(stage="downloaded")
        result = self.verify_both(self.contract(bundle, "downloaded"))
        self.assertEqual(result["bundles"][0]["artifact_counts"], {"media_manifest": 1, "media_lifecycle_manifest": 1})
        self.assertEqual(result["bundles"][0]["original_member_count"], 3)

    def test_downloaded_video_does_not_require_unproduced_derivatives(self) -> None:
        bundle = self.prepare("video", "downloaded")
        result = self.verify_both(self.contract(bundle, "downloaded"))
        self.assertEqual(result["bundles"][0]["artifact_counts"], {"media": 1, "media_lifecycle_manifest": 1})

    def test_processed_image_requires_full_ocr_without_fabricating_seal(self) -> None:
        bundle = self.prepare(stage="processed")
        result = self.verify_both(self.contract(bundle, "processed"))
        self.assertEqual(result["bundles"][0]["artifact_counts"], {"media_manifest": 1, "media_lifecycle_manifest": 1, "ocr": 1})
        with connect(self.db) as connection:
            self.assertIsNone(lifecycle.load_bundle(connection, bundle["bundle_id"])["state"]["completion_receipt"])

    def test_processed_video_has_exact_asr_ocr_frames_prefix(self) -> None:
        bundle = self.prepare("video", "processed")
        result = self.verify_both(self.contract(bundle, "processed"))
        self.assertEqual(result["bundles"][0]["artifact_counts"], {"media": 1, "media_lifecycle_manifest": 1, "frames_manifest": 1, "asr": 1, "ocr": 1})

    def test_sealed_hot_image_has_preview_and_proof_in_exact_closure(self) -> None:
        bundle = self.prepare()
        contract = self.contract(bundle)
        before = self.db.read_bytes()
        files = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in self.fixture.root.rglob("*") if path.is_file()}
        result = self.verify_both(contract)
        self.assertEqual(result["bundles"][0]["artifact_counts"], {"media_manifest": 1, "media_lifecycle_manifest": 1, "ocr": 1,
            "duplicate_fingerprint": 1, "media_preview_manifest": 1, "media_completion_receipt": 1})
        self.assertEqual(before, self.db.read_bytes())
        after = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in self.fixture.root.rglob("*") if path.is_file()}
        sidecars = {f"{self.db}-wal", f"{self.db}-shm"}
        self.assertEqual({path: sha for path, sha in files.items() if path not in sidecars},
                         {path: sha for path, sha in after.items() if path not in sidecars})
        self.assertLessEqual(set(after) - set(files), sidecars)
        wal = f"{self.db}-wal"
        if wal in files:
            self.assertEqual(after.get(wal), files[wal])
        elif wal in after:
            self.assertEqual(after[wal], hashlib.sha256(b"").hexdigest())
        self.assertEqual(result["mutation_scope"], "database_rows_and_media_evidence")
        self.assertTrue(result["sqlite_reader_sidecars_possible"])

    def test_wal_reader_forbids_sql_writes(self) -> None:
        with closing(managed._reader(self.db)) as connection:
            self.assertEqual(connection.execute("PRAGMA query_only").fetchone()[0], 1)
            with self.assertRaises(sqlite3.OperationalError):
                connection.execute("UPDATE contents SET title='forbidden'")
            self.assertEqual(connection.total_changes, 0)

    def test_video_cold_restore_and_expired_keep_frozen_original_identity(self) -> None:
        bundle = self.prepare("video")
        original = dict(bundle["manifest"]["original_artifact"])
        archived = retention.archive_bundle(bundle["bundle_id"], db_path=self.db, at=self.fixture.now)
        self.assertEqual(archived["status"], "archived", archived)
        result = self.verify_both(self.contract(bundle, originals="archived"))
        self.assertEqual(result["bundles"][0]["original_artifact"], original)
        self.assertFalse(result["bundles"][0]["restore_availability_checked"])
        restored = retention.restore_bundle(bundle["bundle_id"], db_path=self.db, at=self.fixture.now, request_id="validation-fixture")
        self.assertEqual(restored["status"], "restored", restored)
        result = self.verify_both(self.contract(bundle, originals="online"))
        self.assertEqual(result["bundles"][0]["storage_state"], "archived")
        current = self.fixture._bundle(2)
        self.fixture.now = (datetime.fromisoformat(current["state"]["delete_due_at"].replace("Z", "+00:00")) + timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
        expired = retention.purge_bundle(bundle["bundle_id"], db_path=self.db, at=self.fixture.now)
        self.assertEqual(expired["status"], "expired", expired)
        result = self.verify_both(self.contract(bundle, originals="expired"))
        self.assertEqual(result["bundles"][0]["original_artifact"], original)
        self.assertEqual(result["bundles"][0]["storage_state"], "expired")

    def test_cold_image_manifest_available_does_not_mean_children_are_online(self) -> None:
        bundle = self.prepare()
        archived = retention.archive_bundle(bundle["bundle_id"], db_path=self.db, at=self.fixture.now)
        self.assertEqual(archived["status"], "archived", archived)
        self.verify_both(self.contract(bundle, originals="archived"))
        with self.assertRaisesRegex(managed.ManagedAnalysisValidationError, "not_online"):
            managed.validate_managed_contract(self.contract(bundle, originals="online"))

    def test_missing_unarchived_original_fails_not_a_planned_cold_state(self) -> None:
        bundle = self.prepare(stage="downloaded")
        member = bundle["manifest"]["members"][0]
        (bundle["originals_root"] / member["relative_path"]).unlink()
        with self.assertRaisesRegex(managed.ManagedAnalysisValidationError, "not_online"):
            managed.validate_managed_contract(self.contract(bundle, "downloaded"))

    def test_unknown_owned_artifact_still_fails_whitelist(self) -> None:
        bundle = self.prepare()
        path = bundle["evidence_root"] / "unknown.json"
        media._atomic_json(path, {"fixture": True})
        with connect(self.db) as connection:
            media.register_artifact(connection, content_id=1, artifact_type="not_a_media_type", path=path, processor_version="fixture-v1",
                metadata={"media_lifecycle": {"bundle_id": bundle["bundle_id"], "control_artifact_id": bundle["control_artifact_id"]}})
        with self.assertRaisesRegex(managed.ManagedAnalysisValidationError, "not_whitelisted"):
            managed.validate_managed_contract(self.contract(bundle))

    def test_duplicate_allowed_artifact_type_is_not_silently_overwritten(self) -> None:
        bundle = self.prepare()
        path = bundle["evidence_root"] / "second-ocr.json"
        media._atomic_json(path, {"fixture": True})
        with connect(self.db) as connection:
            media.register_artifact(connection, content_id=1, artifact_type="ocr", path=path, processor_version="fixture-v1",
                metadata={"media_lifecycle": {"bundle_id": bundle["bundle_id"], "control_artifact_id": bundle["control_artifact_id"]}})
        with self.assertRaisesRegex(managed.ManagedAnalysisValidationError, "count_invalid"):
            managed.validate_managed_contract(self.contract(bundle))

    def test_contract_must_freeze_every_owned_artifact_id(self) -> None:
        bundle = self.prepare()
        contract = self.contract(bundle)
        contract["bundles"][0]["artifact_ids"].remove(bundle["control_artifact_id"])
        with self.assertRaisesRegex(managed.ManagedAnalysisValidationError, "artifact_set_changed"):
            managed.validate_managed_contract(contract)

    def test_original_time_identity_cannot_be_rewritten_after_archive(self) -> None:
        bundle = self.prepare()
        with connect(self.db) as connection:
            connection.execute("UPDATE evidence_artifacts SET captured_at='2031-01-01T00:00:00Z' WHERE id=?", (bundle["manifest"]["original_artifact"]["artifact_id"],))
        with self.assertRaisesRegex(managed.ManagedAnalysisValidationError, "original_identity_changed"):
            managed.validate_managed_contract(self.contract(bundle))

    def test_preview_corruption_is_not_ignored_in_cold_state(self) -> None:
        bundle = self.prepare()
        archived = retention.archive_bundle(bundle["bundle_id"], db_path=self.db, at=self.fixture.now)
        self.assertEqual(archived["status"], "archived", archived)
        result = completion.verify_completion(bundle, db_path=self.db)
        preview = next(row for row in result["evidence_files"] if row["role"] == "image_preview")
        media._resolved(preview["path"]).write_bytes(b"corrupt preview")
        with self.assertRaises(managed.ManagedAnalysisValidationError):
            managed.validate_managed_contract(self.contract(bundle, originals="archived"))

    def test_unowned_extra_file_is_not_hidden_by_known_artifact_types(self) -> None:
        bundle = self.prepare()
        path = bundle["evidence_root"] / "orphan.json"
        media._atomic_json(path, {"unowned": True})
        with self.assertRaisesRegex(managed.ManagedAnalysisValidationError, "file_closure_changed"):
            managed.validate_managed_contract(self.contract(bundle))

    def test_new_contract_does_not_relax_legacy_whitelist_or_versions(self) -> None:
        bundle = self.prepare()
        self.assertEqual(canary.SCHEMA_VERSION, "local-analysis-canary-v1")
        self.assertEqual(batches.SCHEMA_VERSION, "full-local-analysis-batches-v3")
        self.assertEqual(canary.GENERATED_ARTIFACT_TYPES, {"media", "media_manifest", "frames_manifest", "asr", "ocr", "duplicate_fingerprint"})
        with connect(self.db) as connection:
            baseline = [row[0] for row in connection.execute("SELECT id FROM evidence_artifacts WHERE id<>?", (bundle["control_artifact_id"],))]
            paths = cast(canary.CanaryPaths, SimpleNamespace(media_root=self.fixture.media_root, fingerprint_root=self.fixture.root / "unused-fingerprint"))
            with self.assertRaisesRegex(canary.LocalAnalysisCanaryError, "非白名单"):
                canary._validate_generated_artifacts(connection, contract={"baseline_artifact_ids": baseline, "sources": []}, paths=paths, content_ids=[1])

    def test_stage_and_unknown_missing_override_fields_are_strict(self) -> None:
        bundle = self.prepare()
        wrong = self.contract(bundle, "downloaded")
        with self.assertRaisesRegex(managed.ManagedAnalysisValidationError, "artifact_types_invalid"):
            managed.validate_managed_contract(wrong)
        wrong = self.contract(bundle)
        wrong["allow_missing"] = True
        with self.assertRaisesRegex(managed.ManagedAnalysisValidationError, "fields_invalid"):
            managed.validate_managed_contract(wrong)

    def test_purging_is_not_signed_as_complete_expiry(self) -> None:
        bundle = self.prepare()
        with connect(self.db) as connection:
            metadata = json.loads(connection.execute("SELECT metadata_json FROM evidence_artifacts WHERE id=?", (bundle["control_artifact_id"],)).fetchone()[0])
            metadata["media_lifecycle"]["operation_state"] = "purging"
            connection.execute("UPDATE evidence_artifacts SET metadata_json=? WHERE id=?", (json.dumps(metadata), bundle["control_artifact_id"]))
        with self.assertRaisesRegex(managed.ManagedAnalysisValidationError, "operation_in_progress"):
            managed.validate_managed_contract(self.contract(bundle))

    def test_new_source_cannot_redirect_the_frozen_bundle(self) -> None:
        bundle = self.prepare()
        contract = self.contract(bundle)
        self.fixture._source(suffix="-later")
        self.verify_both(contract)
        contract["bundles"][0]["manifest_sha256"] = "f" * 64
        with self.assertRaisesRegex(managed.ManagedAnalysisValidationError, "bundle_identity_changed"):
            managed.validate_managed_contract(contract)

    def test_live_wal_is_visible_without_checkpointing_or_mutation(self) -> None:
        writer = connect(self.db)
        self.addCleanup(writer.close)
        writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        bundle = self.prepare()
        contract = self.contract(bundle)
        with sqlite3.connect(f"file:{self.db}?mode=ro&immutable=1", uri=True) as stale:
            self.assertEqual(stale.execute("SELECT COUNT(*) FROM evidence_artifacts WHERE artifact_type='media_lifecycle_manifest'").fetchone()[0], 0)
        before = self.db.read_bytes()
        self.verify_both(contract)
        self.assertEqual(before, self.db.read_bytes())

    def test_cli_exact_managed_opt_in_never_calls_legacy_execution(self) -> None:
        bundle = self.prepare()
        path = self.fixture.root / "managed-contract.json"
        body = managed._canonical(self.contract(bundle))
        path.write_bytes(body)
        path.chmod(0o600)
        argv = ["--verify-managed-contract", str(path), "--expected-contract-sha256", hashlib.sha256(body).hexdigest()]
        with patch.object(canary, "run_canary", side_effect=AssertionError("legacy writer forbidden")), \
             patch.object(batches, "run_batches", side_effect=AssertionError("legacy batch writer forbidden")), \
             redirect_stdout(io.StringIO()) as output:
            self.assertEqual(canary.main(argv), 0)
            self.assertEqual(batches.main(argv), 0)
        results = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(len(results), 2)
        self.assertTrue(all(row["status"] == "verified" for row in results))
        with redirect_stdout(io.StringIO()):
            self.assertEqual(canary.main([*argv[:-1], "a" * 64]), 2)
        with self.assertRaises(SystemExit):
            batches.main(["--apply"])

    def test_duplicate_bundle_scope_is_rejected(self) -> None:
        contract = self.contract(self.prepare())
        contract["bundles"].append(copy.deepcopy(contract["bundles"][0]))
        with self.assertRaisesRegex(managed.ManagedAnalysisValidationError, "scope_duplicated"):
            managed.validate_managed_contract(contract)


if __name__ == "__main__":
    unittest.main()
