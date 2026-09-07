"""Snapshot freshness uses verified installation time and the real publisher window."""
from __future__ import annotations

import unittest
from datetime import datetime
from unittest.mock import patch

from fastapi.testclient import TestClient

from tests import test_v8_artifact_paths as relocation
from v8 import api, artifact_paths, snapshot_sync


def at(value: str) -> datetime:
    return datetime.fromisoformat(value)


class SnapshotSyncTest(unittest.TestCase):
    def status(self, installed: str, now: str) -> dict:
        return snapshot_sync.snapshot_sync_from_receipt(
            {"activation_status": "succeeded", "installed_at": installed}, now=at(now)
        )

    def test_delay_requires_more_than_two_active_hours(self):
        installed = "2026-09-07T10:00:00+08:00"
        self.assertEqual(self.status(installed, "2026-09-07T12:00:00+08:00")["status"], "current")
        self.assertEqual(self.status(installed, "2026-09-07T12:00:01+08:00")["status"], "delayed")

    def test_night_pauses_age_but_never_erases_previous_delay(self):
        for now in ["2026-09-08T00:00:00+08:00", "2026-09-08T08:59:59+08:00"]:
            with self.subTest(now=now):
                fresh = self.status("2026-09-07T23:00:00+08:00", now)
                self.assertEqual(fresh["status"], "current")
                self.assertEqual(fresh["window_state"], "inactive")
                self.assertEqual(fresh["next_scheduled_at"], "2026-09-08T09:00:00+08:00")
                self.assertEqual(self.status("2026-09-07T21:00:00+08:00", now)["status"], "delayed")

    def test_twenty_to_midnight_is_a_publishing_window(self):
        state = self.status("2026-09-07T20:00:00+08:00", "2026-09-07T23:00:00+08:00")
        self.assertEqual(state["status"], "delayed")
        self.assertEqual(state["window_state"], "active")
        self.assertIsNone(state["next_scheduled_at"])

    def test_age_resumes_at_nine_and_spans_multiple_days(self):
        installed = "2026-09-07T23:00:00+08:00"
        self.assertEqual(self.status(installed, "2026-09-08T10:00:00+08:00")["status"], "current")
        self.assertEqual(self.status(installed, "2026-09-08T10:00:01+08:00")["status"], "delayed")
        self.assertEqual(snapshot_sync.active_publish_seconds(at(installed), at("2026-09-09T10:00:00+08:00")), 17 * 3600)
        self.assertEqual(self.status("2026-09-08T01:00:00+08:00", "2026-09-08T11:00:00+08:00")["status"], "current")

    def test_utc_input_is_displayed_as_a_real_install_instant(self):
        state = self.status("2026-09-07T11:15:00+08:00", "2026-09-07T04:00:00Z")
        self.assertEqual(state["last_verified_install_at"], "2026-09-07T03:15:00Z")
        self.assertEqual(state["status"], "current")
        self.assertEqual(state["window_state"], "active")

    def test_absent_failed_pending_invalid_and_future_receipts_are_unknown(self):
        now = at("2026-09-07T12:00:00+08:00")
        receipts = [None, {}, {"installed_at": "2026-09-07T10:00:00+08:00"}]
        receipts += [{"activation_status": status, "installed_at": "2026-09-07T10:00:00+08:00"}
                     for status in ["pending_smoke", "failed", "rolled_back"]]
        receipts += [{"activation_status": "succeeded", "installed_at": value}
                     for value in [None, "", 0, "2026-09-07", "2026-09-07T10:00:00", "2026-02-30T10:00:00Z",
                                   "invalid", "0001-01-01T00:00:00+14:00", "2026-09-07T10:00:00+08:90",
                                   "2026-09-07T12:00:01+08:00"]]
        for receipt in receipts:
            with self.subTest(receipt=receipt):
                state = snapshot_sync.snapshot_sync_from_receipt(receipt, now=now)
                self.assertEqual(state["status"], "unknown")
                self.assertIsNone(state["last_verified_install_at"])

    def test_writer_does_not_inspect_a_replica_receipt(self):
        with patch.object(artifact_paths, "installed_snapshot", side_effect=AssertionError("writer read a replica receipt")):
            self.assertIsNone(snapshot_sync.snapshot_sync_status(read_only=False))

    def test_only_verified_context_is_accepted_and_errors_do_not_claim_success(self):
        now = at("2026-09-07T12:00:00+08:00")
        receipt = {"activation_status": "succeeded", "installed_at": "2026-09-07T11:00:00+08:00"}
        with patch.object(artifact_paths, "installed_snapshot", return_value={"receipt": receipt}) as verified:
            self.assertEqual(snapshot_sync.snapshot_sync_status(read_only=True, now=now)["status"], "current")
            verified.assert_called_once_with()
        for failure in [artifact_paths.ArtifactPathError("manifest_changed"), OSError("receipt unreadable")]:
            with patch.object(artifact_paths, "installed_snapshot", side_effect=failure):
                self.assertEqual(snapshot_sync.snapshot_sync_status(read_only=True, now=now)["status"], "unknown")

    def test_clock_requires_a_timezone(self):
        with self.assertRaisesRegex(ValueError, "timezone"):
            snapshot_sync.snapshot_sync_from_receipt(None, now=at("2026-09-07T12:00:00"))


class SnapshotSyncHealthTest(unittest.TestCase):
    def test_health_uses_verified_receipt_and_reuses_manifest_cache_without_writes(self):
        fixture = relocation.ArtifactRelocationTest()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.receipt.update(activation_status="succeeded", installed_at="2026-09-07T03:15:00Z")
        fixture.write_receipt()
        config = api.ApiConfig(
            db_path=fixture.replica_db, reports_root=fixture.replica / "reports", read_only=True,
            scheduler_enabled=False, startup_catchup_enabled=False,
            legacy_db_path=fixture.root / "legacy.sqlite3", operator_freeze_lock=fixture.root / "freeze.lock",
        )
        before = fixture.replica_db.read_bytes()
        status = snapshot_sync.snapshot_sync_status
        def fixed_status(**kwargs):
            return status(**kwargs, now=at("2026-09-07T04:00:00Z"))
        with TestClient(api.create_app(config)) as client, patch.object(snapshot_sync, "snapshot_sync_status", side_effect=fixed_status):
            with patch.object(artifact_paths, "_object", wraps=artifact_paths._object) as reads:
                for _ in range(2):
                    response = client.get("/api/v8/health")
                    self.assertEqual(response.status_code, 200, response.text)
                    body = response.json()
                    self.assertTrue(body["read_only"])
                    self.assertEqual(body["automation"]["scheduler_state"], "read_only")
                    self.assertEqual(body["snapshot_sync"]["last_verified_install_at"], "2026-09-07T03:15:00Z")
                    self.assertIn(body["snapshot_sync"]["status"], {"current", "delayed"})
                self.assertTrue(reads.call_args_list, "expected identity-checked small receipt reads")
                self.assertFalse(any(call.args[0] == fixture.manifest_path for call in reads.call_args_list), "health reread the verified manifest")
        self.assertEqual(fixture.replica_db.read_bytes(), before)
        self.assertFalse(fixture.replica_db.with_name(fixture.replica_db.name + "-wal").exists())
