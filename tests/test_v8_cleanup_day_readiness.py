"""Real cleanup authority and offline scan materials through day receipt sealing.

SQLite rows, raw hashes, manifests, durable attempts, release controls and gates
are verified by production code. No readiness/coverage validator is mocked.
"""
from __future__ import annotations

import hashlib
import os
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import patch

from tests import test_account_classification_release as classification
from tests import test_v8_account_cleanup_runtime as fixtures
from v8 import capture_authorizations as auth, durable_runs
from v8 import paid_drain, provider_budget, runtime_receipts, scan_receipts, storage
from v8.tikhub_scan import _digest


class CleanupDayReadinessTest(unittest.TestCase):
    def setUp(self):
        self.use_fixture()

    def use_fixture(self, *, schema21=False):
        if hasattr(self, "fixture"):
            self.fixture.doCleanups()
        cls = classification.AccountClassificationReleaseTest if schema21 else fixtures.CleanupRuntimeTest
        self.fixture = cls(methodName="runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        if schema21:
            self.fixture.make_child()
        self.connection = self.fixture.connection
        self.active = self.fixture.active
        fixture_db = self.fixture.db.resolve(strict=True)
        fixture_stat = fixture_db.stat()
        fixture_identity = (fixture_stat.st_dev, fixture_stat.st_ino)

        def receipt_connect(path, *, read_only=None):
            # The installed-contract fixture calls only this temporary DB
            # formal. Keep the process-wide production DB guard unchanged.
            self.assertEqual(Path(path), fixture_db, "receipt connection left the fixture DB")
            self.assertFalse(fixture_db.is_symlink())
            current = fixture_db.stat()
            self.assertEqual((current.st_dev, current.st_ino), fixture_identity)
            if read_only is None:
                read_only = os.environ.get("DCAR_READ_ONLY", "0").strip() == "1"
            connection = sqlite3.connect(
                f"{fixture_db.as_uri()}?mode={'ro' if read_only else 'rw'}",
                uri=True, timeout=10, factory=storage._ClosingSQLiteConnection)
            connection.row_factory = sqlite3.Row
            try:
                storage.configure_connection_safety(connection)
                storage.require_schema_compatibility(connection, supported_versions=frozenset({20, 21}))
                connection.execute("PRAGMA query_only = ON" if read_only else "PRAGMA journal_mode = WAL")
                connection.execute("PRAGMA busy_timeout = 10000")
            except Exception:
                connection.close()
                raise
            return connection

        self.fixture.enterContext(patch.object(runtime_receipts, "connect", side_effect=receipt_connect))

    def readiness(self, at):
        before = self.connection.total_changes
        result = runtime_receipts.current_activation_readiness(self.connection, at=at)
        self.assertEqual(self.connection.total_changes, before)
        return result

    def gates(self, at):
        if not self.connection.in_transaction:
            self.connection.execute("BEGIN IMMEDIATE")
        result = self.fixture.maintenance(at)
        self.assertEqual(result["status"], "checked", result)
        self.connection.commit()

    def run_row(self, job, identity, checkpoint, at):
        details = {"contract_version": durable_runs.CONTRACT_VERSION,
                   "scan_id": durable_runs.scan_identity(job, identity),
                   "identity": identity, "checkpoint": checkpoint, "complete": True}
        encoded = auth.canonical(details)
        cursor = self.connection.execute(
            "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,completed_at,details_json) "
            "VALUES (?,?,'succeeded',?,?,?)",
            (job, "offline:" + details["scan_id"], at, at, encoded),
        )
        run_id = cursor.lastrowid
        self.connection.execute(
            "INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,invocation_source,"
            "status,started_at,completed_at,details_json) VALUES (?,1,'scheduled','succeeded',?,?,?)",
            (run_id, at, at, encoded),
        )
        return run_id

    def seal_day(self, day="2026-09-08", *, scanned=True):
        """One real frozen member and an exhausted empty supplier-page fixture."""
        local_end = datetime.combine(date.fromisoformat(day) + timedelta(days=1), time.min,
                                     runtime_receipts.BEIJING)
        def iso(value):
            return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        anchor_at = iso(local_end + timedelta(hours=3))
        cutoff = iso(local_end + timedelta(hours=4))
        end, start = iso(local_end), iso(local_end - timedelta(days=7))
        anchor = {"pipeline_version": "matrix-first-pipeline-v1", "beijing_day": local_end.date().isoformat(),
                  "round_id": "tikhub_reconcile:03:00", "registration_id": "tikhub_reconcile",
                  "job_id": "tikhub_reconcile", "scheduled_at": anchor_at,
                  "activation_id": self.active["activation_id"], "profile_id": self.active["profile_id"],
                  "activation_sha256": self.active["activation_sha256"],
                  "roster_snapshot_id": self.active["roster_snapshot_id"],
                  "roster_snapshot_hash": self.active["roster_members_sha256"],
                  "eligible_identity_ids": [self.fixture.member["account_identity_id"]]}
        self.run_row("pipeline_round:tikhub_reconcile", anchor, {"complete": True}, anchor_at)
        run_id = None
        if scanned:
            scope = {key: anchor[key] for key in ("activation_id", "profile_id", "activation_sha256",
                                                 "roster_snapshot_id", "roster_snapshot_hash")}
            scope.update(contract_version="tikhub-account-scan-v1", provider="TikHub", platform="douyin",
                         account_id=self.fixture.member["account_id"], identity_id=self.fixture.member["account_identity_id"],
                         window_start=start, window_end=end, purpose="reconcile")
            scan_id = durable_runs.scan_identity("tikhub_reconcile", scope)
            window = f"scan:{scan_id}:g0:p0:{_digest(0)}"
            slot_id = self.connection.execute(
                "INSERT INTO fetch_slots(account_id,stage,window_key,provider,adapter_version,status,"
                "attempt_count,created_at,updated_at) VALUES (?,'discovery',?,'TikHub',?,'succeeded',1,?,?)",
                (scope["account_id"], window, scope["contract_version"], anchor_at, anchor_at),
            ).lastrowid
            attempt_id = self.connection.execute(
                "INSERT INTO fetch_attempts(slot_id,attempt_number,request_started_at,response_finished_at,"
                "http_status,billed) VALUES (?,1,?,?,200,0)", (slot_id, anchor_at, anchor_at),
            ).lastrowid
            raw = self.fixture.root / f"raw-{day}.json"
            raw.write_text(auth.canonical({"code": 200, "data": {"aweme_list": [], "has_more": False}}))
            raw_sha = hashlib.sha256(raw.read_bytes()).hexdigest()
            raw_id = self.connection.execute(
                "INSERT INTO provider_raw_responses(fetch_attempt_id,account_id,provider,operation,"
                "local_path,sha256,byte_size,http_status,captured_at,source) "
                "VALUES (?,?,'TikHub','douyin_user_posts',?,?,?,200,?,'offline_fixture')",
                (attempt_id, scope["account_id"], str(raw), raw_sha, raw.stat().st_size, anchor_at),
            ).lastrowid
            counts = {key: 0 for key in ("existing", "inserted", "quarantined", "unparseable")}
            manifest = {"contract_version": scope["contract_version"], "scan_id": scan_id,
                        "scope": scope, "generation": 0, "page_number": 0, "request_cursor": 0,
                        "next_cursor": None, "previous": None, "items": [], "raw_items": 0,
                        "counts": counts, "raw": {"raw_response_id": raw_id, "sha256": raw_sha,
                                                   "slot_id": slot_id, "window_key": window}}
            ref = self.fixture.write(f"manifest-{day}.json", manifest)
            ref["byte_size"] = Path(ref["path"]).stat().st_size
            checkpoint = {"complete": True, "last_manifest": ref, "generation": 0,
                          "page_number": 1, "cursor": None, "raw_items": 0, "counts": counts}
            run_id = self.run_row("tikhub_reconcile", scope, checkpoint, anchor_at)
        self.connection.commit()
        evidence = self.fixture.root / "runtime-evidence"
        if run_id is not None:
            runtime_receipts.record_scan_verification_receipt(
                run_id, db_path=self.fixture.db, cutoff_at=cutoff, evidence_root=evidence)
        coverage = scan_receipts.runtime_coverage(self.connection, at=cutoff)
        self.assertEqual(coverage["complete"], scanned, coverage)
        receipt = runtime_receipts.record_profile_day_coverage_receipt(
            db_path=self.fixture.db, cutoff_at=cutoff, evidence_root=evidence)
        self.assertEqual(receipt["summary"]["complete"], scanned)
        return cutoff, receipt

    def test_real_complete_day_and_four_operator_gates_become_ready(self):
        self.gates("2026-09-08T18:00:00Z")
        at, receipt = self.seal_day()
        result = self.readiness(at)
        self.assertTrue(result["control_readiness"], result)
        self.assertTrue(result["data_readiness"], result)
        self.assertEqual(result["receipt"]["run_id"], receipt["run_id"])
        self.assertEqual(receipt["scope"]["control_contract_version"], "account-cleanup-release-control-v1")

    def test_incomplete_real_day_never_becomes_ready(self):
        self.gates("2026-09-08T18:00:00Z")
        at, _ = self.seal_day(scanned=False)
        result = self.readiness(at)
        self.assertTrue(result["control_readiness"], result)
        self.assertFalse(result["data_readiness"])
        self.assertEqual(result["reason"], "current_activation_coverage_incomplete")

    def test_complete_transition_day_is_not_a_full_activation_day(self):
        self.gates("2026-09-07T18:00:00Z")
        at, _ = self.seal_day("2026-09-07")
        result = self.readiness(at)
        self.assertFalse(result["data_readiness"])
        self.assertEqual(result["reason"], "activation_transition_day")

    def test_missing_and_expired_operation_gates_block_complete_day(self):
        at, _ = self.seal_day()
        self.assertEqual(self.readiness(at)["reason"], "required_operation_unqualified")
        self.gates("2026-09-08T18:00:00Z")
        self.assertTrue(self.readiness(at)["data_readiness"])
        result = self.readiness("2026-09-09T18:00:00Z")
        self.assertTrue(result["control_readiness"])
        self.assertFalse(result["data_readiness"])
        self.assertEqual(result["reason"], "required_operation_unqualified")

    def test_operation_fault_blocks_without_changing_complete_receipt(self):
        self.gates("2026-09-08T18:00:00Z")
        at, receipt = self.seal_day()
        self.connection.execute("BEGIN IMMEDIATE")
        provider_budget.record_fault_state(
            self.connection, scope_kind="operation", provider="TikHub", operation="douyin_video_comments",
            fault_class="provider_transient", reason="fixture operation fault", usage_id=None,
            at=at, state_evidence={"http_status": 503},
        )
        result = self.readiness(at)
        self.assertTrue(result["control_readiness"])
        self.assertFalse(result["data_readiness"])
        self.assertEqual(result["reason"], "required_operation_unqualified")
        self.assertEqual(result["receipt"]["self_sha256"], receipt["self_sha256"])

    def test_modified_private_operator_decision_invalidates_day_binding(self):
        self.gates("2026-09-08T18:00:00Z")
        at, _ = self.seal_day()
        path = Path(self.fixture.authority["decision_receipt"]["path"])
        path.write_bytes(path.read_bytes() + b" ")
        self.assertFalse(self.readiness(at)["data_readiness"])
        binding = runtime_receipts._day_release_binding(
            self.connection, activation_id=self.active["activation_id"], business_day="2026-09-08", at=at)
        self.assertIsNone(binding["release_event_id"])

    def test_later_schema21_successor_binds_historical_cleanup_release(self):
        self.use_fixture(schema21=True)
        # The installed source successor postdates the business day's end;
        # its validated inherited release still predates that day.
        self.fixture.child["account_classification_successor"]["issued_at"] = "2026-09-08T19:30:00Z"
        self.fixture.seal_child()
        self.gates("2026-09-08T19:30:00Z")
        at, receipt = self.seal_day()
        self.assertEqual(self.connection.execute("PRAGMA user_version").fetchone()[0], 21)
        self.assertTrue(self.readiness(at)["data_readiness"])
        state = paid_drain.dispatch_state(self.connection, at=at)
        self.assertEqual(receipt["scope"]["release_event_id"], state.permit_event_id)

    def test_release_after_requested_day_cannot_bind_that_day(self):
        binding = runtime_receipts._day_release_binding(
            self.connection, activation_id=self.active["activation_id"], business_day="2026-09-06",
            at="2026-09-09T00:00:00Z")
        self.assertIsNone(binding["release_event_id"])

    def test_explicitly_closed_gate_blocks_existing_complete_day(self):
        self.gates("2026-09-08T18:00:00Z")
        at, _ = self.seal_day()
        gate = {"provider": "tikhub", "operation": "douyin_video_detail", "state": "closed",
                "reason": "explicit fixture hold", "evidence_json": "{}", "recorded_at": at}
        self.connection.execute(
            "INSERT INTO capture_paid_send_gate_events(provider,operation,state,reason,evidence_json,recorded_at,event_sha256) "
            "VALUES(?,?,?,?,?,?,?)", (*gate.values(), auth.digest(gate)))
        result = self.readiness(at)
        self.assertFalse(result["data_readiness"])
        self.assertEqual(result["reason"], "required_operation_unqualified")

    def test_changed_day_bridge_summary_is_rejected_by_real_immutable_attempt(self):
        self.gates("2026-09-08T18:00:00Z")
        at, receipt = self.seal_day()
        self.connection.execute("UPDATE scheduler_runs SET details_json='{}' WHERE id=?", (receipt["run_id"],))
        result = self.readiness(at)
        self.assertFalse(result["data_readiness"])
        self.assertEqual(result["reason"], "current_activation_receipt_mismatch")


if __name__ == "__main__":
    unittest.main()
