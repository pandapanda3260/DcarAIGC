"""Actual protected snapshot receipts and SQLite, with no Writer/provider calls."""
from __future__ import annotations

import os
import shutil
import unittest
from dataclasses import replace
from unittest.mock import patch

from fastapi.testclient import TestClient

from tests import test_v8_artifact_paths as fixtures
from v8 import api, reader_readiness
from v8.runtime_database import DatabaseAccessMode


class ReaderReadinessTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.ArtifactRelocationTest()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()
        self.fixture.receipt.update(schema="dcar-read-replica-install-receipt-v1",
            activation_status="succeeded", installed_at="2026-08-29T04:01:00Z")
        self.fixture.write_receipt()
        self.config = api.ApiConfig(db_path=self.fixture.replica_db,
            reports_root=self.fixture.replica / "reports", read_only=True,
            scheduler_enabled=False, startup_catchup_enabled=False,
            legacy_db_path=self.fixture.root / "legacy.sqlite3",
            operator_freeze_lock=self.fixture.root / "freeze.lock")

    def client(self):
        return TestClient(api.create_app(self.config))

    def test_verified_reader_is_ready_without_writer_or_data_qualification(self):
        with self.client() as client:
            with patch("v8.runtime_receipts.current_activation_readiness", side_effect=AssertionError("Writer proof read")), \
                 patch("v8.paid_drain.dispatch_state", side_effect=AssertionError("Writer control read")), \
                 patch.object(api, "_file_sha256", side_effect=AssertionError("database rehashed")):
                for _ in range(2):
                    response = client.get("/api/v8/readyz")
                    self.assertEqual(response.status_code, 200, response.text)
                    value = response.json()
                    self.assertEqual(value["role"], "reader")
                    self.assertTrue(value["snapshot_readiness"]["ready"])
                    self.assertIsNone(value["control_readiness"])
                    self.assertIsNone(value["data_readiness"])
                    self.assertEqual(value["snapshot_readiness"]["installed_at"], "2026-08-29T04:01:00Z")
            # Stale data remains visible as delayed, independent of service readiness.
            health = client.get("/api/v8/health")
            self.assertEqual(health.status_code, 200, health.text)
            self.assertEqual(health.json()["snapshot_sync"]["status"], "delayed")

    def test_missing_receipt_is_not_ready_even_when_database_health_is_ok(self):
        with self.client() as client:
            self.fixture.receipt_path.unlink()
            response = client.get("/api/v8/readyz")
            self.assertEqual(response.status_code, 503, response.text)
            self.assertEqual(response.json()["reason"], "snapshot_receipt_missing")
            self.assertEqual(client.get("/api/v8/health").status_code, 200)

    def test_hash_identical_database_replacement_requires_restart_verification(self):
        with self.client() as client:
            replacement = self.fixture.root / "replacement.sqlite3"
            shutil.copyfile(self.fixture.replica_db, replacement)
            os.replace(replacement, self.fixture.replica_db)
            response = client.get("/api/v8/readyz")
            self.assertEqual(response.status_code, 503, response.text)
            self.assertEqual(response.json()["reason"], "snapshot_database_changed")
            self.assertEqual(client.get("/api/v8/health").status_code, 200)

    def test_manifest_tamper_and_unprotected_receipt_fail_closed(self):
        with self.client() as client:
            self.fixture.manifest_path.write_text("{}")
            response = client.get("/api/v8/readyz")
            self.assertEqual(response.status_code, 503, response.text)
            self.assertEqual(response.json()["reason"], "snapshot_receipt_invalid")
            self.fixture.write_receipt()
            self.fixture.receipt_path.chmod(0o666)
            self.assertEqual(client.get("/api/v8/readyz").status_code, 503)

    def test_pending_smoke_can_start_but_only_verified_install_is_ready(self):
        self.fixture.receipt["activation_status"] = "pending_smoke"
        self.fixture.write_receipt()
        with self.client() as client:
            response = client.get("/api/v8/readyz")
            self.assertEqual(response.status_code, 503, response.text)
            self.assertEqual(response.json()["reason"], "snapshot_install_not_verified")
            self.fixture.receipt["activation_status"] = "succeeded"
            self.fixture.write_receipt()
            self.assertEqual(client.get("/api/v8/readyz").status_code, 200)

    def test_reader_cannot_mask_writer_scheduler_or_incompatible_database(self):
        with self.client() as client:
            for values in ({"scheduler_enabled": True}, {"startup_catchup_enabled": True},
                           {"runtime_access_mode": DatabaseAccessMode.WRITER}):
                client.app.state.config = replace(self.config, **values)
                response = client.get("/api/v8/readyz")
                self.assertEqual(response.status_code, 503, response.text)
                self.assertEqual(response.json()["reason"], "snapshot_reader_role_conflict")
            client.app.state.config = self.config
            for key in ("writer_lock_held", "scheduler_requested", "scheduler_enabled"):
                setattr(client.app.state, key, True)
                self.assertEqual(client.get("/api/v8/readyz").json()["reason"], "snapshot_reader_role_conflict")
                setattr(client.app.state, key, False)
            with patch.object(api, "schema_compatibility_state", return_value={"compatible": False}):
                self.assertEqual(client.get("/api/v8/readyz").json()["reason"], "database_incompatible")

    def test_receipt_identity_cannot_be_swapped_under_running_database(self):
        with self.client() as client:
            identity = {**self.fixture.receipt["runtime_identity"], "active_release_id": "different"}
            self.fixture.receipt["runtime_identity"] = identity
            self.fixture.manifest["runtime_identity"] = identity
            self.fixture.write_receipt()
            response = client.get("/api/v8/readyz")
            self.assertEqual(response.status_code, 503, response.text)
            self.assertEqual(response.json()["reason"], "snapshot_database_changed")

    def test_nonreader_and_unconfigured_legacy_keep_existing_readiness_path(self):
        with patch.object(reader_readiness.artifact_paths, "installed_snapshot",
                          side_effect=AssertionError("Writer used snapshot permission")):
            self.assertIsNone(reader_readiness.snapshot_readiness(
                config=replace(self.config, read_only=False), state=None, compatibility={}))
        with patch.dict(os.environ, {"DCAR_READ_ONLY": "0", "DCAR_ACTIVE_SNAPSHOT": ""}), \
             patch.object(reader_readiness.artifact_paths, "installed_snapshot", return_value=None):
            self.assertIsNone(reader_readiness.snapshot_readiness(config=self.config, state=None, compatibility={}))


if __name__ == "__main__":
    unittest.main()
