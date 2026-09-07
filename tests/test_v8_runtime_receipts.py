from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.roster_fixture import accept_roster
from v8 import (
    account_states,
    durable_runs,
    runtime_receipts,
    scan_terminals,
    system_roster,
)
from v8.operations import upsert_account
from v8.profile_activations import TIKHUB_PROFILE, activation_at, append_activation
from v8.reconcile_control import reconcile_budget_scope
from v8.storage import connect, initialize_database, transaction


NOW = "2026-09-01T01:00:00Z"
EARLIER = "2026-09-01T00:30:00Z"
BUSINESS_DAY = "2026-08-31"


class RuntimeReceiptsTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(prefix="dcar-runtime-receipts-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.db = self.root / "runtime.sqlite3"
        self.evidence = self.root / "evidence"
        with connect(self.db) as connection:
            initialize_database(connection)
        upsert_account(
            {
                "phone": "",
                "platforms": [{"platform": "douyin", "uid": "123456789"}],
            },
            db_path=self.db,
        )
        with connect(self.db) as connection:
            self.snapshot = accept_roster(
                connection, accepted_at="2026-08-28T16:00:00Z"
            )
            self.identity_id = int(
                connection.execute(
                    "SELECT id FROM account_platform_identities"
                ).fetchone()[0]
            )
            active = activation_at(
                connection,
                runtime_receipts._coverage_anchor_at(BUSINESS_DAY),
            )
            assert active is not None
            self.active = active
        self.anchor_run_id = self._anchor()

    def _anchor(self) -> int:
        identity = {
            "pipeline_version": "matrix-first-pipeline-v1",
            "beijing_day": "2026-09-01",
            "round_id": "tikhub_reconcile:03:00",
            "registration_id": "tikhub_reconcile",
            "job_id": "tikhub_reconcile",
            "scheduled_at": runtime_receipts._coverage_anchor_at(BUSINESS_DAY),
            "roster_snapshot_id": self.snapshot["id"],
            "roster_snapshot_hash": self.snapshot["members_sha256"],
            "activation_id": self.active["activation_id"],
            "profile_id": self.active["profile_id"],
            "activation_sha256": self.active["activation_sha256"],
            "eligible_identity_ids": [self.identity_id],
        }
        details = {
            "contract_version": durable_runs.CONTRACT_VERSION,
            "scan_id": durable_runs.scan_identity(
                "pipeline_round:tikhub_reconcile", identity
            ),
            "identity": identity,
            "checkpoint": {"complete": True},
            "complete": True,
        }
        encoded = json.dumps(details, sort_keys=True, separators=(",", ":"))
        with connect(self.db) as connection, transaction(connection):
            cursor = connection.execute(
                "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,"
                "completed_at,details_json) VALUES "
                "('pipeline_round:tikhub_reconcile',?,'succeeded',?,?,?)",
                ("anchor", identity["scheduled_at"], EARLIER, encoded),
            )
            run_id = int(cursor.lastrowid or 0)
            connection.execute(
                "INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,"
                "invocation_source,status,started_at,completed_at,details_json) "
                "VALUES (?,1,'scheduled','succeeded',?,?,?)",
                (run_id, identity["scheduled_at"], EARLIER, encoded),
            )
        return run_id

    def _coverage(
        self,
        *,
        complete: bool = True,
        scan_run_ids: list[int] | None = None,
        roster_snapshot_id: int | None = None,
    ) -> dict[str, object]:
        selected = list(scan_run_ids or [])
        roster_id = roster_snapshot_id or int(self.snapshot["id"])
        roster_hash = (
            self.snapshot["members_sha256"]
            if roster_id == self.snapshot["id"]
            else "f" * 64
        )
        day = {
            "date": BUSINESS_DAY,
            "known": True,
            "complete": complete,
            "partial_publishable": False,
            "activation_id": self.active["activation_id"],
            "profile_id": self.active["profile_id"],
            "activation_sha256": self.active["activation_sha256"],
            "source_family": "matrix",
            "roster_snapshot_id": roster_id,
            "roster_snapshot_hash": roster_hash,
            "round_run_id": self.anchor_run_id,
            "anchor_scheduled_at": runtime_receipts._coverage_anchor_at(BUSINESS_DAY),
            "matrix_expected_windows": 60,
            "matrix_complete_windows": 60 if complete else 0,
            "matrix_forbidden_run_ids": [],
            "matrix_run_ids": [],
            "tikhub_run_ids": selected,
            "eligible_identity_ids": [self.identity_id],
            "covered_identity_ids": [self.identity_id] if complete else [],
            "succeeded_identity_ids": [self.identity_id] if complete else [],
            "blocked_identity_ids": [],
            "not_applicable_identity_ids": [],
            "accounted_identity_ids": [self.identity_id] if complete else [],
            "required_identity_ids": [self.identity_id],
            "reason": "" if complete else "missing",
        }
        return {
            "contract_version": runtime_receipts.PROFILE_DAY_CONTRACT,
            "business_day": BUSINESS_DAY,
            "activation_id": self.active["activation_id"],
            "profile_id": self.active["profile_id"],
            "activation_sha256": self.active["activation_sha256"],
            "source_family": "matrix",
            "matrix_expected_windows": 60,
            "matrix_complete_windows": 60 if complete else 0,
            "tikhub_expected_members": 1,
            "tikhub_complete_members": 1 if complete else 0,
            "status": "complete" if complete else "incomplete",
            "complete": complete,
            "partial_publishable": False,
            "reason": "" if complete else "missing",
            "roster_snapshot_id": roster_id,
            "roster_snapshot_hash": roster_hash,
            "round_run_id": self.anchor_run_id,
            "anchor_scheduled_at": runtime_receipts._coverage_anchor_at(BUSINESS_DAY),
            "matrix_run_ids": [],
            "tikhub_run_ids": selected,
            "required_scan_run_ids": selected,
            "days": [day],
            "scan_errors": {},
        }

    def _scan(
        self,
        *,
        completed_at: str = EARLIER,
        window_end: str = "2026-08-31T16:00:00Z",
    ) -> int:
        identity = {
            "provider": "TikHub",
            "contract_version": "tikhub-account-scan-v2",
            "identity_id": 1,
            "purpose": "reconcile",
            "window_end": window_end,
        }
        details = {
            "contract_version": durable_runs.CONTRACT_VERSION,
            "scan_id": durable_runs.scan_identity("tikhub_reconcile", identity),
            "identity": identity,
            "checkpoint": {"complete": True},
            "complete": True,
        }
        encoded = json.dumps(details, sort_keys=True, separators=(",", ":"))
        with connect(self.db) as connection, transaction(connection):
            cursor = connection.execute(
                "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,"
                "completed_at,details_json) VALUES "
                "('tikhub_reconcile',?,'succeeded',?,?,?)",
                (f"scan:{details['scan_id']}", EARLIER, completed_at, encoded),
            )
            run_id = int(cursor.lastrowid or 0)
            connection.execute(
                "INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,"
                "invocation_source,status,started_at,completed_at,details_json) "
                "VALUES (?,1,'scheduled','succeeded',?,?,?)",
                (run_id, EARLIER, completed_at, encoded),
            )
        return run_id

    def _failed_scan(self, *, completed_at: str = EARLIER) -> int:
        identity = {
            "provider": "TikHub",
            "contract_version": "tikhub-account-scan-v2",
            "identity_id": 1,
            "purpose": "reconcile",
            "window_end": "2026-08-31T16:00:00Z",
            "task_id": "terminal-receipt-fixture",
        }
        details = {
            "contract_version": durable_runs.CONTRACT_VERSION,
            "scan_id": durable_runs.scan_identity("tikhub_reconcile", identity),
            "identity": identity,
            "checkpoint": {"complete": False},
            "complete": False,
            "summary": scan_terminals.terminal_summary(
                reason="http_503", terminal_class="provider_transient"
            ),
        }
        encoded = json.dumps(details, sort_keys=True, separators=(",", ":"))
        with connect(self.db) as connection, transaction(connection):
            cursor = connection.execute(
                "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,"
                "completed_at,details_json) VALUES "
                "('tikhub_reconcile',?,'failed',?,?,?)",
                (f"scan:{details['scan_id']}", EARLIER, completed_at, encoded),
            )
            run_id = int(cursor.lastrowid or 0)
            connection.execute(
                "INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,"
                "invocation_source,status,started_at,completed_at,details_json) "
                "VALUES (?,1,'scheduled','failed',?,?,?)",
                (run_id, EARLIER, completed_at, encoded),
            )
        return run_id

    @staticmethod
    def _proof(run_id: int) -> dict[str, object]:
        return {
            "run_id": run_id,
            "scope": {
                "provider": "TikHub",
                "contract_version": "tikhub-account-scan-v2",
                "identity_id": 1,
            },
            "completed_at": EARLIER,
            "counts": {"known": 1},
            "references": [
                {
                    "manifest": {"path": "/fixture", "sha256": "b" * 64},
                    "raw_id": 3,
                    "raw_sha256": "c" * 64,
                    "raw_path": "/fixture/raw",
                    "raw_byte_size": 12,
                }
            ],
        }

    def test_scan_receipt_is_idempotent_and_attempt_is_authoritative(self) -> None:
        run_id = self._scan()
        with patch(
            "v8.scan_receipts.verify_scan", return_value=self._proof(run_id)
        ) as verifier:
            first = runtime_receipts.record_scan_verification_receipt(
                run_id,
                db_path=self.db,
                cutoff_at=NOW,
                evidence_root=self.evidence,
            )
            second = runtime_receipts.record_scan_verification_receipt(
                run_id,
                db_path=self.db,
                cutoff_at=NOW,
                evidence_root=self.evidence,
            )
        self.assertEqual(first, second)
        verifier.assert_called_once()
        self.assertEqual(len(list(self.evidence.iterdir())), 1)
        with connect(self.db) as connection:
            stored = runtime_receipts.read_scan_verification_receipt(
                connection, run_id
            )
            self.assertEqual(stored, first)
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM scheduler_runs WHERE job_id=?",
                    (runtime_receipts.SCAN_RECEIPT_JOB,),
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM scheduler_run_attempts "
                    "WHERE scheduler_run_id=?",
                    (first["run_id"],),
                ).fetchone()[0],
                1,
            )
            native = connection.execute(
                "SELECT * FROM scan_verification_receipts WHERE scan_run_id=?",
                (run_id,),
            ).fetchone()
            self.assertIsNotNone(native)
            assert native is not None
            self.assertEqual(native["source_bridge_run_id"], first["run_id"])
            self.assertEqual(native["source_bridge_attempt_id"], first["attempt_id"])
            self.assertEqual(native["receipt_sha256"], first["self_sha256"])

    def test_missing_evidence_file_does_not_open_the_db_authority(self) -> None:
        run_id = self._scan()
        with patch(
            "v8.scan_receipts.verify_scan", return_value=self._proof(run_id)
        ):
            receipt = runtime_receipts.record_scan_verification_receipt(
                run_id,
                db_path=self.db,
                cutoff_at=NOW,
                evidence_root=self.evidence,
            )
        Path(receipt["evidence"]["path"]).unlink()
        with connect(self.db) as connection:
            selected = runtime_receipts.read_scan_verification_receipt(
                connection, run_id
            )
        self.assertEqual(selected, receipt)

    def test_failed_accounted_terminal_gets_an_individual_scan_receipt(self) -> None:
        run_id = self._failed_scan()
        proof = {
            "run_id": run_id,
            "scope": {
                "provider": "TikHub",
                "contract_version": "tikhub-account-scan-v2",
                "identity_id": 1,
            },
            "completed_at": EARLIER,
            "terminal_class": "provider_transient",
            "accounted": True,
            "required": True,
            "publication_blocker": False,
            "reason": "http_503",
            "references": [],
        }
        with patch(
            "v8.scan_receipts.verify_terminal_scan", return_value=proof
        ) as verifier:
            receipt = runtime_receipts.record_scan_verification_receipt(
                run_id,
                db_path=self.db,
                cutoff_at=NOW,
                evidence_root=self.evidence,
            )
        verifier.assert_called_once()
        self.assertEqual(receipt["summary"]["scan_status"], "failed")
        self.assertEqual(
            receipt["summary"]["terminal_class"], "provider_transient"
        )
        self.assertTrue(receipt["summary"]["accounted"])
        with connect(self.db) as connection:
            self.assertEqual(
                runtime_receipts.read_scan_verification_receipt(connection, run_id),
                receipt,
            )

    def test_tampered_mutable_run_is_rejected_by_immutable_attempt(self) -> None:
        run_id = self._scan()
        with patch(
            "v8.scan_receipts.verify_scan", return_value=self._proof(run_id)
        ):
            receipt = runtime_receipts.record_scan_verification_receipt(
                run_id,
                db_path=self.db,
                cutoff_at=NOW,
                evidence_root=self.evidence,
            )
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE scheduler_runs SET details_json='{}' WHERE id=?",
                (receipt["run_id"],),
            )
            connection.commit()
            with self.assertRaisesRegex(
                runtime_receipts.RuntimeReceiptError,
                "run and immutable attempt disagree",
            ):
                runtime_receipts.read_scan_verification_receipt(connection, run_id)

    def test_day_receipt_is_selected_without_deep_reverification(self) -> None:
        coverage = self._coverage()
        with patch(
            "v8.scan_receipts.runtime_coverage", return_value=coverage
        ) as verifier:
            receipt = runtime_receipts.record_profile_day_coverage_receipt(
                db_path=self.db,
                cutoff_at=NOW,
                evidence_root=self.evidence,
            )
            self._scan(window_end="2026-08-30T16:00:00Z")
            repeated = runtime_receipts.record_profile_day_coverage_receipt(
                db_path=self.db,
                cutoff_at=NOW,
                evidence_root=self.evidence,
            )
        self.assertEqual(receipt, repeated)
        verifier.assert_called_once()
        with connect(self.db) as connection, patch(
            "v8.scan_receipts.runtime_coverage",
            side_effect=AssertionError("hot path must not verify raw"),
        ):
            current = runtime_receipts.latest_runtime_coverage(connection, at=NOW)
        self.assertTrue(current["complete"])
        self.assertEqual(current["receipt"]["run_id"], receipt["run_id"])
        self.assertEqual(current["receipt"]["sequence"], 1)
        with connect(self.db) as connection:
            native = connection.execute(
                "SELECT * FROM profile_day_coverage_receipts"
            ).fetchone()
            self.assertIsNotNone(native)
            assert native is not None
            self.assertEqual(native["activation_id"], self.active["activation_id"])
            self.assertEqual(native["business_day"], BUSINESS_DAY)
            self.assertEqual(native["sequence"], 1)
            self.assertEqual(native["receipt_sha256"], receipt["self_sha256"])

    def test_refresh_before_closeout_anchor_skips_without_deep_work(self) -> None:
        before_anchor = "2026-08-31T18:59:59Z"
        with patch.object(
            runtime_receipts,
            "refresh_current_scan_receipts",
            side_effect=AssertionError("scan receipts must wait for the anchor"),
        ) as scan_refresh, patch.object(
            runtime_receipts,
            "record_profile_day_coverage_receipt",
            side_effect=AssertionError("day receipt must wait for the anchor"),
        ) as day_refresh:
            result = runtime_receipts.refresh_runtime_receipts(
                db_path=self.db,
                cutoff_at=before_anchor,
                evidence_root=self.evidence,
            )

        scan_refresh.assert_not_called()
        day_refresh.assert_not_called()
        self.assertEqual(
            (result["status"], result["reason"]),
            ("skipped", "profile_day_anchor_not_due"),
        )
        self.assertEqual(result["business_day"], BUSINESS_DAY)
        self.assertEqual(
            result["anchor_at"],
            runtime_receipts._coverage_anchor_at(BUSINESS_DAY),
        )

    def test_revoked_scan_invalidates_itself_and_linked_day_receipt(self) -> None:
        run_id = self._scan()
        with patch(
            "v8.scan_receipts.verify_scan", return_value=self._proof(run_id)
        ):
            runtime_receipts.record_scan_verification_receipt(
                run_id,
                db_path=self.db,
                cutoff_at=NOW,
                evidence_root=self.evidence,
            )
        coverage = self._coverage(scan_run_ids=[run_id])
        with patch("v8.scan_receipts.runtime_coverage", return_value=coverage):
            runtime_receipts.record_profile_day_coverage_receipt(
                db_path=self.db,
                cutoff_at=NOW,
                evidence_root=self.evidence,
            )
        with connect(self.db) as connection, transaction(connection):
            native_id = int(
                connection.execute(
                    "SELECT id FROM scan_verification_receipts WHERE scan_run_id=?",
                    (run_id,),
                ).fetchone()[0]
            )
            connection.execute(
                """INSERT INTO runtime_receipt_revocations(
                       scan_receipt_id,revoked_at,actor,reason,contract_version,
                       revocation_sha256,metadata_json,created_at)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (
                    native_id,
                    NOW,
                    "test",
                    "fixture revocation",
                    "runtime-receipt-revocation-v1",
                    "d" * 64,
                    "{}",
                    NOW,
                ),
            )
        with connect(self.db) as connection:
            self.assertIsNone(
                runtime_receipts.read_scan_verification_receipt(connection, run_id)
            )
            self.assertIsNone(
                runtime_receipts.read_profile_day_coverage_receipt(
                    connection, at=NOW
                )
            )
        with self.assertRaisesRegex(
            runtime_receipts.RuntimeReceiptError, "missing or revoked"
        ):
            runtime_receipts.record_scan_verification_receipt(
                run_id,
                db_path=self.db,
                cutoff_at=NOW,
                evidence_root=self.evidence,
            )

    def test_day_receipt_uses_anchor_activation_not_a_later_activation(self) -> None:
        coverage = self._coverage()
        with patch("v8.scan_receipts.runtime_coverage", return_value=coverage):
            runtime_receipts.record_profile_day_coverage_receipt(
                db_path=self.db,
                cutoff_at=NOW,
                evidence_root=self.evidence,
            )
        with connect(self.db) as connection:
            accept_roster(
                connection, accepted_at="2026-09-01T00:45:00Z"
            )
        with connect(self.db) as connection:
            selected = runtime_receipts.latest_runtime_coverage(connection, at=NOW)
        self.assertTrue(selected["complete"])
        self.assertEqual(
            selected["activation_id"], self.active["activation_id"]
        )

    def test_day_sequence_is_scoped_to_activation_and_business_day(self) -> None:
        with patch(
            "v8.scan_receipts.runtime_coverage", return_value=self._coverage()
        ):
            first = runtime_receipts.record_profile_day_coverage_receipt(
                db_path=self.db,
                cutoff_at=NOW,
                evidence_root=self.evidence,
            )
        later = "2026-09-01T01:10:00Z"
        with connect(self.db) as connection:
            account_states.set_account_enabled(
                connection,
                self.identity_id,
                enabled=False,
                effective_at="2026-09-01T01:05:00Z",
                actor="test",
                reason="new profile-day revision",
                created_at="2026-09-01T01:05:00Z",
                activation_id=self.active["activation_id"],
            )
        with patch(
            "v8.scan_receipts.runtime_coverage",
            return_value=self._coverage(complete=False),
        ):
            second = runtime_receipts.record_profile_day_coverage_receipt(
                db_path=self.db,
                cutoff_at=later,
                evidence_root=self.evidence,
            )
        self.assertEqual(first["summary"]["sequence"], 1)
        self.assertEqual(second["summary"]["sequence"], 2)
        with connect(self.db) as connection:
            self.assertEqual(
                [
                    row["sequence"]
                    for row in connection.execute(
                        "SELECT sequence FROM profile_day_coverage_receipts "
                        "WHERE activation_id=? AND business_day=? ORDER BY sequence",
                        (self.active["activation_id"], BUSINESS_DAY),
                    )
                ],
                [1, 2],
            )

    def test_frozen_day_anchor_survives_a_later_retry_but_rejects_tamper(self) -> None:
        with patch("v8.scan_receipts.runtime_coverage", return_value=self._coverage()):
            receipt = runtime_receipts.record_profile_day_coverage_receipt(
                db_path=self.db, cutoff_at=NOW, evidence_root=self.evidence,
            )
        frozen = receipt["scope"]["source_binding"]["anchor"]
        with connect(self.db) as connection, transaction(connection):
            row = connection.execute("SELECT * FROM scheduler_runs WHERE id=?", (self.anchor_run_id,)).fetchone()
            details = json.loads(row["details_json"])
            details["complete"] = False
            encoded = json.dumps(details, sort_keys=True, separators=(",", ":"))
            later = "2026-09-01T10:00:00Z"
            connection.execute(
                "INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,invocation_source,"
                "status,started_at,completed_at,details_json) VALUES (?,2,'scheduled','partial',?,?,?)",
                (self.anchor_run_id, later, later, encoded),
            )
            connection.execute("UPDATE scheduler_runs SET status='partial',started_at=?,completed_at=?,details_json=? WHERE id=?",
                               (later, later, encoded, self.anchor_run_id))
        with connect(self.db) as connection:
            observed = runtime_receipts.read_profile_day_coverage_receipt(connection, at=NOW)
            self.assertEqual(observed["self_sha256"], receipt["self_sha256"])
            self.assertEqual(observed["scope"]["source_binding"]["anchor"], frozen)
            with self.assertRaisesRegex(Exception, "scheduler attempt permits one running-to-terminal update"):
                connection.execute("UPDATE scheduler_run_attempts SET details_json='{}' WHERE id=?", (frozen["attempt_id"],))
            with self.assertRaisesRegex(runtime_receipts.RuntimeReceiptError, "frozen anchor binding mismatch"):
                runtime_receipts._anchor_binding(
                    connection, day={"round_run_id": self.anchor_run_id},
                    active=receipt["scope"], business_day=BUSINESS_DAY,
                    frozen_anchor={**frozen, "details_sha256": "f" * 64}, cutoff_at=NOW,
                )

    def test_day_receipt_rechecks_source_revision_before_db_commit(self) -> None:
        coverage = self._coverage(complete=False)
        original_write = runtime_receipts._write_evidence

        def change_source(*args, **kwargs):
            evidence = original_write(*args, **kwargs)
            with connect(self.db) as connection:
                account_states.set_account_enabled(
                    connection,
                    self.identity_id,
                    enabled=False,
                    effective_at=NOW,
                    actor="test",
                    reason="source revision race",
                    created_at=NOW,
                    activation_id=self.active["activation_id"],
                )
            return evidence

        with patch(
            "v8.scan_receipts.runtime_coverage", return_value=coverage
        ), patch.object(
            runtime_receipts, "_write_evidence", side_effect=change_source
        ), self.assertRaisesRegex(
            runtime_receipts.RuntimeReceiptError,
            "coverage source changed before day receipt commit",
        ):
            runtime_receipts.record_profile_day_coverage_receipt(
                db_path=self.db,
                cutoff_at=NOW,
                evidence_root=self.evidence,
            )
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM scheduler_runs WHERE job_id=?",
                    (runtime_receipts.DAY_RECEIPT_JOB,),
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM scan_verification_receipts"
                ).fetchone()[0],
                0,
            )

    def test_complete_day_receipt_rejects_a_different_roster(self) -> None:
        coverage = self._coverage(roster_snapshot_id=999)
        with patch(
            "v8.scan_receipts.runtime_coverage", return_value=coverage
        ), self.assertRaisesRegex(
            runtime_receipts.RuntimeReceiptError,
            "coverage activation or roster does not match",
        ):
            runtime_receipts.record_profile_day_coverage_receipt(
                db_path=self.db,
                cutoff_at=NOW,
                evidence_root=self.evidence,
            )
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM scheduler_runs WHERE job_id=?",
                    (runtime_receipts.DAY_RECEIPT_JOB,),
                ).fetchone()[0],
                0,
            )

    def test_atomic_attempt_failure_rolls_back_receipt_run(self) -> None:
        run_id = self._scan()
        with connect(self.db) as connection:
            connection.execute(
                """
                CREATE TRIGGER fail_runtime_receipt_attempt
                BEFORE UPDATE ON scheduler_run_attempts
                WHEN (SELECT job_id FROM scheduler_runs WHERE id=NEW.scheduler_run_id)
                     ='scan_verification_receipt_v2'
                BEGIN
                    SELECT RAISE(ABORT,'fixture receipt finalization failure');
                END
                """
            )
            connection.commit()
        with patch(
            "v8.scan_receipts.verify_scan", return_value=self._proof(run_id)
        ), self.assertRaisesRegex(Exception, "fixture receipt finalization failure"):
            runtime_receipts.record_scan_verification_receipt(
                run_id,
                db_path=self.db,
                cutoff_at=NOW,
                evidence_root=self.evidence,
            )
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM scheduler_runs WHERE job_id=?",
                    (runtime_receipts.SCAN_RECEIPT_JOB,),
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM scan_verification_receipts"
                ).fetchone()[0],
                0,
            )

    def test_native_insert_failure_rolls_back_the_one_shot_bridge(self) -> None:
        run_id = self._scan()
        with connect(self.db) as connection:
            connection.execute(
                """CREATE TRIGGER fail_native_scan_receipt
                   BEFORE INSERT ON scan_verification_receipts
                   BEGIN
                       SELECT RAISE(ABORT,'fixture native receipt failure');
                   END"""
            )
            connection.commit()
        with patch(
            "v8.scan_receipts.verify_scan", return_value=self._proof(run_id)
        ), self.assertRaisesRegex(Exception, "fixture native receipt failure"):
            runtime_receipts.record_scan_verification_receipt(
                run_id,
                db_path=self.db,
                cutoff_at=NOW,
                evidence_root=self.evidence,
            )
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM scheduler_runs WHERE job_id=?",
                    (runtime_receipts.SCAN_RECEIPT_JOB,),
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM scan_verification_receipts"
                ).fetchone()[0],
                0,
            )

    def test_missing_day_receipt_is_fast_unknown(self) -> None:
        with connect(self.db) as connection, patch(
            "v8.scan_receipts.runtime_coverage",
            side_effect=AssertionError("missing receipt must remain cheap"),
        ):
            coverage = runtime_receipts.latest_runtime_coverage(connection, at=NOW)
        self.assertEqual(coverage["status"], "unknown")
        self.assertEqual(coverage["reason"], "profile_day_receipt_missing")
        self.assertEqual(coverage["profile_id"], self.active["profile_id"])
        self.assertEqual(coverage["activation_id"], self.active["activation_id"])
        self.assertEqual(coverage["matrix_expected_windows"], 60)

    def test_missing_mode_b_day_receipt_is_fast_unknown_with_zero_matrix(self) -> None:
        with connect(self.db) as connection:
            sealed = system_roster.seal_system_members(
                connection,
                [
                    {
                        "platform": "douyin",
                        "uid": "123456789",
                        "profile_ref": None,
                    }
                ],
                raw_root=self.root / "system-roster",
                actor="test",
                reason="mode B health fixture",
                sealed_at="2026-08-30T00:00:00Z",
            )
            snapshot = connection.execute(
                "SELECT * FROM account_roster_snapshots WHERE id=?",
                (sealed["snapshot_id"],),
            ).fetchone()
            assert snapshot is not None
            mode_b = append_activation(
                connection,
                profile_id=TIKHUB_PROFILE,
                roster_snapshot_id=int(snapshot["id"]),
                roster_members_sha256=str(snapshot["members_sha256"]),
                effective_at="2026-08-31T18:00:00Z",
                build_receipt_sha256=hashlib.sha256(b"mode-b-build").hexdigest(),
                actor="test",
                reason="mode B health fixture",
                created_at="2026-08-30T00:00:00Z",
            )
        with connect(self.db) as connection, patch(
            "v8.scan_receipts.runtime_coverage",
            side_effect=AssertionError("missing receipt must remain cheap"),
        ):
            coverage = runtime_receipts.latest_runtime_coverage(connection, at=NOW)
        self.assertEqual(coverage["status"], "unknown")
        self.assertEqual(coverage["profile_id"], TIKHUB_PROFILE)
        self.assertEqual(coverage["activation_id"], mode_b["activation_id"])
        self.assertEqual(coverage["roster_snapshot_id"], snapshot["id"])
        self.assertEqual(coverage["matrix_expected_windows"], 0)

    def test_period_coverage_uses_day_receipts_without_deep_verification(self) -> None:
        coverage = self._coverage()
        with patch("v8.scan_receipts.runtime_coverage", return_value=coverage):
            runtime_receipts.record_profile_day_coverage_receipt(
                db_path=self.db,
                cutoff_at=NOW,
                evidence_root=self.evidence,
            )
        with connect(self.db) as connection, patch(
            "v8.scan_receipts.runtime_coverage",
            side_effect=AssertionError("period hot path must not verify raw"),
        ):
            period = runtime_receipts.period_coverage_from_receipts(
                connection,
                period_start="2026-08-30",
                period_end=BUSINESS_DAY,
                cutoff_at=NOW,
            )
        self.assertFalse(period["complete"])
        self.assertFalse(period["partial_publishable"])
        self.assertEqual(period["discovery_coverage"]["status"], "unknown")
        self.assertIsNone(period["discovery_coverage"]["percentage"])
        self.assertIsNone(period["discovery_coverage"]["accounted_percentage"])
        self.assertEqual(
            period["pipeline_observation"]["legacy_unobserved_dates"],
            ["2026-08-30"],
        )
        self.assertTrue(period["days"][1]["complete"])

    def test_refresh_current_scan_receipts_targets_only_the_health_day(self) -> None:
        current = self._scan()
        self._scan(window_end="2026-08-30T16:00:00Z")
        with patch(
            "v8.scan_receipts.runtime_coverage",
            return_value=self._coverage(scan_run_ids=[current]),
        ), patch(
            "v8.scan_receipts.verify_scan", return_value=self._proof(current)
        ) as verifier:
            result = runtime_receipts.refresh_current_scan_receipts(
                db_path=self.db,
                cutoff_at=NOW,
                evidence_root=self.evidence,
            )
        self.assertEqual(result["candidate_count"], 1)
        self.assertEqual(result["recorded_count"], 1)
        self.assertEqual(result["receipts"][0]["scan_run_id"], current)
        verifier.assert_called_once()

    def test_scan_refresh_stops_at_budget_and_preserves_unprocessed_debt(self) -> None:
        first = self._scan()
        second = self._failed_scan()
        with patch(
            "v8.scan_receipts.runtime_coverage",
            return_value=self._coverage(scan_run_ids=[first, second]),
        ), patch(
            "v8.scan_receipts.verify_scan", return_value=self._proof(first)
        ) as success_verifier, patch(
            "v8.scan_receipts.verify_terminal_scan",
            side_effect=AssertionError("second receipt must remain as debt"),
        ) as terminal_verifier, reconcile_budget_scope(max_items=1) as budget:
            result = runtime_receipts.refresh_current_scan_receipts(
                db_path=self.db,
                cutoff_at=NOW,
                evidence_root=self.evidence,
            )

        self.assertEqual(result["candidate_count"], 2)
        self.assertEqual(result["recorded_count"], 1)
        self.assertTrue(result["budget_exhausted"])
        self.assertTrue(result["limit_reached"])
        self.assertEqual(budget.used, 1)
        success_verifier.assert_called_once()
        terminal_verifier.assert_not_called()
        with connect(self.db) as connection:
            self.assertIsNotNone(
                runtime_receipts.read_scan_verification_receipt(connection, first)
            )
            self.assertIsNone(
                runtime_receipts.read_scan_verification_receipt(connection, second)
            )

    def test_expired_budget_starts_no_scan_receipt(self) -> None:
        run_id = self._scan()
        now = [100.0]
        with patch(
            "v8.scan_receipts.runtime_coverage",
            return_value=self._coverage(scan_run_ids=[run_id]),
        ), patch.object(
            runtime_receipts,
            "record_scan_verification_receipt",
            side_effect=AssertionError("expired budget must start no receipt"),
        ) as recorder, reconcile_budget_scope(
            max_items=2,
            max_seconds=1.0,
            monotonic_fn=lambda: now[0],
        ) as budget:
            now[0] = budget.deadline
            result = runtime_receipts.refresh_current_scan_receipts(
                db_path=self.db,
                cutoff_at=NOW,
                evidence_root=self.evidence,
            )

        recorder.assert_not_called()
        self.assertEqual(result["recorded_count"], 0)
        self.assertTrue(result["budget_exhausted"])
        self.assertTrue(result["limit_reached"])
        self.assertEqual(budget.used, 0)

    def test_runtime_refresh_does_not_seal_day_after_budget_is_consumed(self) -> None:
        run_id = self._scan()
        with patch(
            "v8.scan_receipts.runtime_coverage",
            return_value=self._coverage(scan_run_ids=[run_id]),
        ), patch(
            "v8.scan_receipts.verify_scan", return_value=self._proof(run_id)
        ), patch.object(
            runtime_receipts,
            "record_profile_day_coverage_receipt",
            side_effect=AssertionError("day receipt must remain as debt"),
        ) as day_recorder, reconcile_budget_scope(max_items=1) as budget:
            result = runtime_receipts.refresh_runtime_receipts(
                db_path=self.db,
                cutoff_at=NOW,
                evidence_root=self.evidence,
            )

        day_recorder.assert_not_called()
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["reason"], "reconcile_budget_exhausted")
        self.assertEqual(result["scan_receipts"]["recorded_count"], 1)
        self.assertIsNone(result["day_receipt"])
        self.assertEqual(budget.used, 1)
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM profile_day_coverage_receipts"
                ).fetchone()[0],
                0,
            )


if __name__ == "__main__":
    unittest.main()
