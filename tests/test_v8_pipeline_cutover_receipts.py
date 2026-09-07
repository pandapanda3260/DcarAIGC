from __future__ import annotations

import copy
import json
import sqlite3
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

from tests.roster_fixture import accept_roster
from tests.v9_report_fixture import activate_v9_report_fixture
from v8 import (
    durable_runs,
    pipeline_cutover,
    reports,
    runtime_receipts,
    scan_receipts,
)
from v8.operations import upsert_account
from v8.profile_activations import activation_at
from v8.storage import connect, initialize_database, transaction


NOW = "2026-09-01T01:00:00Z"
EARLIER = "2026-09-01T00:30:00Z"
BUSINESS_DAY = "2026-08-31"


class PipelineCutoverNativeReceiptsTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(prefix="dcar-cutover-receipts-")
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
        with connect(self.db) as connection, transaction(connection):
            self.roster = accept_roster(
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
        self.scan_run_id = self._scan()
        with patch(
            "v8.scan_receipts.verify_scan",
            return_value=self._scan_proof(),
        ):
            runtime_receipts.record_scan_verification_receipt(
                self.scan_run_id,
                db_path=self.db,
                cutoff_at=NOW,
                evidence_root=self.evidence,
            )
        with patch(
            "v8.scan_receipts.runtime_coverage",
            return_value=self._coverage(),
        ):
            self.day_receipt = (
                runtime_receipts.record_profile_day_coverage_receipt(
                    db_path=self.db,
                    cutoff_at=NOW,
                    evidence_root=self.evidence,
                )
            )

    def _anchor(self) -> int:
        identity = {
            "registration_id": "tikhub_reconcile",
            "job_id": "tikhub_reconcile",
            "beijing_day": "2026-09-01",
            "scheduled_at": runtime_receipts._coverage_anchor_at(BUSINESS_DAY),
            "activation_id": self.active["activation_id"],
            "profile_id": self.active["profile_id"],
            "activation_sha256": self.active["activation_sha256"],
            "roster_snapshot_id": self.roster["id"],
            "roster_snapshot_hash": self.roster["members_sha256"],
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
                (identity["scheduled_at"], identity["scheduled_at"], EARLIER, encoded),
            )
            run_id = int(cursor.lastrowid or 0)
            connection.execute(
                "INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,"
                "invocation_source,status,started_at,completed_at,details_json) "
                "VALUES (?,1,'scheduled','succeeded',?,?,?)",
                (run_id, identity["scheduled_at"], EARLIER, encoded),
            )
        return run_id

    def _scan(self) -> int:
        identity = {
            "provider": "TikHub",
            "contract_version": "tikhub-account-scan-v2",
            "identity_id": self.identity_id,
            "purpose": "reconcile",
            "window_end": "2026-08-31T16:00:00Z",
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
                (f"scan:{details['scan_id']}", EARLIER, EARLIER, encoded),
            )
            run_id = int(cursor.lastrowid or 0)
            connection.execute(
                "INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,"
                "invocation_source,status,started_at,completed_at,details_json) "
                "VALUES (?,1,'scheduled','succeeded',?,?,?)",
                (run_id, EARLIER, EARLIER, encoded),
            )
        return run_id

    def _scan_proof(self) -> dict[str, object]:
        return {
            "run_id": self.scan_run_id,
            "scope": {
                "provider": "TikHub",
                "contract_version": "tikhub-account-scan-v2",
                "identity_id": self.identity_id,
            },
            "completed_at": EARLIER,
            "counts": {"known": 1},
            "references": [],
        }

    def _coverage(self) -> dict[str, object]:
        day = {
            "date": BUSINESS_DAY,
            "known": True,
            "complete": True,
            "partial_publishable": False,
            "activation_id": self.active["activation_id"],
            "profile_id": self.active["profile_id"],
            "activation_sha256": self.active["activation_sha256"],
            "source_family": "matrix",
            "roster_snapshot_id": self.roster["id"],
            "roster_snapshot_hash": self.roster["members_sha256"],
            "round_run_id": self.anchor_run_id,
            "anchor_scheduled_at": runtime_receipts._coverage_anchor_at(
                BUSINESS_DAY
            ),
            "matrix_run_ids": [],
            "tikhub_run_ids": [self.scan_run_id],
            "eligible_identity_ids": [self.identity_id],
            "covered_identity_ids": [self.identity_id],
            "succeeded_identity_ids": [self.identity_id],
            "blocked_identity_ids": [],
            "not_applicable_identity_ids": [],
            "accounted_identity_ids": [self.identity_id],
            "required_identity_ids": [self.identity_id],
            "reason": "",
        }
        return {
            "contract_version": runtime_receipts.PROFILE_DAY_CONTRACT,
            "business_day": BUSINESS_DAY,
            "activation_id": self.active["activation_id"],
            "profile_id": self.active["profile_id"],
            "activation_sha256": self.active["activation_sha256"],
            "source_family": "matrix",
            "matrix_expected_windows": 60,
            "matrix_complete_windows": 60,
            "tikhub_expected_members": 1,
            "tikhub_complete_members": 1,
            "status": "complete",
            "complete": True,
            "partial_publishable": False,
            "reason": "",
            "roster_snapshot_id": self.roster["id"],
            "roster_snapshot_hash": self.roster["members_sha256"],
            "round_run_id": self.anchor_run_id,
            "anchor_scheduled_at": runtime_receipts._coverage_anchor_at(
                BUSINESS_DAY
            ),
            "matrix_run_ids": [],
            "tikhub_run_ids": [self.scan_run_id],
            "required_scan_run_ids": [self.scan_run_id],
            "days": [day],
            "scan_errors": {},
        }

    def _period(self) -> dict[str, object]:
        with connect(self.db) as connection:
            return runtime_receipts.period_coverage_from_receipts(
                connection,
                period_start=BUSINESS_DAY,
                period_end=BUSINESS_DAY,
                cutoff_at=NOW,
            )

    def test_schema19_runtime_and_frozen_period_use_only_native_receipts(self) -> None:
        frozen = self._period()
        with connect(self.db) as connection, patch.object(
            scan_receipts,
            "runtime_coverage",
            side_effect=AssertionError("publisher must not rebuild raw coverage"),
        ), patch.object(
            scan_receipts,
            "verify_scan",
            side_effect=AssertionError("publisher must not re-read raw scan pages"),
        ):
            evidence = pipeline_cutover.runtime_evidence(connection, at=NOW)
            pipeline_cutover.verify_frozen_scans(
                connection,
                frozen,
                period_start=BUSINESS_DAY,
                period_end=BUSINESS_DAY,
                cutoff_at=NOW,
            )
        self.assertEqual(
            evidence["contract_version"],
            pipeline_cutover.PROFILE_DAY_DISCOVERY_CONTRACT,
        )
        self.assertEqual(
            evidence["coverage"]["receipt"]["self_sha256"],
            self.day_receipt["self_sha256"],
        )
        self.assertEqual(
            evidence["coverage"]["activation_id"], self.active["activation_id"]
        )

    def test_schema19_frozen_period_rejects_tamper_and_wrong_activation(self) -> None:
        frozen = self._period()
        tampered = copy.deepcopy(frozen)
        tampered["scan_references"][0]["self_sha256"] = "f" * 64
        wrong_activation = copy.deepcopy(frozen)
        wrong_activation["days"][0]["activation_id"] += 1
        with connect(self.db) as connection:
            for value in (tampered, wrong_activation):
                with self.subTest(value=value), self.assertRaisesRegex(
                    pipeline_cutover.PublicationEvidenceError,
                    "publication_frozen_receipt_reference_changed",
                ):
                    pipeline_cutover.verify_frozen_scans(
                        connection,
                        value,
                        period_start=BUSINESS_DAY,
                        period_end=BUSINESS_DAY,
                        cutoff_at=NOW,
                    )

    def test_schema19_report_compact_binding_is_rebuilt_from_native_receipts(
        self,
    ) -> None:
        period = self._period()
        compact = reports._compact_profile_day_scan_inputs(
            period,
            period_start=BUSINESS_DAY,
            period_end=BUSINESS_DAY,
        )
        with connect(self.db) as connection, patch.object(
            scan_receipts,
            "verify_scan",
            side_effect=AssertionError("compact publisher path must not read raw"),
        ):
            pipeline_cutover.verify_frozen_scans(
                connection,
                compact,
                period_start=BUSINESS_DAY,
                period_end=BUSINESS_DAY,
                cutoff_at=NOW,
            )
            for field, value in (
                ("self_sha256", "e" * 64),
                ("period_coverage_sha256", "f" * 64),
            ):
                changed = copy.deepcopy(compact)
                changed[field] = value
                with self.subTest(field=field), self.assertRaisesRegex(
                    pipeline_cutover.PublicationEvidenceError,
                    "publication_frozen_receipt_reference_changed",
                ):
                    pipeline_cutover.verify_frozen_scans(
                        connection,
                        changed,
                        period_start=BUSINESS_DAY,
                        period_end=BUSINESS_DAY,
                        cutoff_at=NOW,
                    )
            wrong_activation = copy.deepcopy(compact)
            wrong_activation["receipt_references"][0]["activation_id"] += 1
            with self.assertRaisesRegex(
                pipeline_cutover.PublicationEvidenceError,
                "publication_frozen_receipt_reference_changed",
            ):
                pipeline_cutover.verify_frozen_scans(
                    connection,
                    wrong_activation,
                    period_start=BUSINESS_DAY,
                    period_end=BUSINESS_DAY,
                    cutoff_at=NOW,
                )

    def test_schema19_report_dependency_verifies_compact_receipt_without_raw(self) -> None:
        activate_v9_report_fixture(self.db, [])
        with ExitStack() as stack:
            for target in (
                "v8.reports.now_utc",
                "v8.report_inputs.now_utc",
                "v8.operations.now_utc",
            ):
                stack.enter_context(patch(target, return_value=NOW))
            stack.enter_context(patch.object(reports, "PROJECT_ROOT", self.root))
            stack.enter_context(
                patch.object(reports, "render_summary_png", return_value=False)
            )
            task = reports.create_and_run_task(
                task_type="custom",
                period_start=BUSINESS_DAY,
                period_end=BUSINESS_DAY,
                creation_source="manual",
                db_path=self.db,
                reports_root=self.root / "reports",
            )
        self.assertIn(task["task_status"], {"succeeded", "partial"})
        with connect(self.db) as connection, patch.object(
            pipeline_cutover, "load_policy", return_value={"policy_version": "a-later-routing-policy"},
        ), patch.object(
            scan_receipts,
            "verify_scan",
            side_effect=AssertionError("report publication must not re-read raw"),
        ):
            dependency = pipeline_cutover.report_dependency(
                connection,
                task["id"],
                at=NOW,
                project_root=self.root,
            )
        self.assertEqual(dependency["task_id"], task["id"])
        self.assertEqual(dependency["status"], task["task_status"])
        self.assertEqual(dependency["raw_files"], [])

    def test_frozen_policy_requires_an_approved_version_and_exact_content(self) -> None:
        path = Path(__file__).resolve().parents[1] / "config/source_routing_matrix_first_v1.json"
        policy = json.loads(path.read_text())
        pipeline_cutover.verify_frozen_source_policy(policy, pipeline_cutover.digest(policy))
        for changed in ({**policy, "policy_version": "unapproved"}, {**policy, "primary_provider": "unapproved"}):
            with self.subTest(changed=changed["policy_version"]), self.assertRaisesRegex(
                pipeline_cutover.PublicationEvidenceError, "source_policy_invalid",
            ):
                pipeline_cutover.verify_frozen_source_policy(changed, pipeline_cutover.digest(changed))

    def test_schema19_revoked_scan_invalidates_runtime_and_frozen_period(self) -> None:
        frozen = self._period()
        with connect(self.db) as connection, transaction(connection):
            receipt_id = int(
                connection.execute(
                    "SELECT id FROM scan_verification_receipts WHERE scan_run_id=?",
                    (self.scan_run_id,),
                ).fetchone()[0]
            )
            connection.execute(
                "INSERT INTO runtime_receipt_revocations(scan_receipt_id,revoked_at,"
                "actor,reason,contract_version,revocation_sha256,metadata_json,"
                "created_at) VALUES (?,?,?,?,?,?,?,?)",
                (
                    receipt_id,
                    NOW,
                    "test",
                    "publisher rejection fixture",
                    "runtime-receipt-revocation-v1",
                    "d" * 64,
                    "{}",
                    NOW,
                ),
            )
        with connect(self.db) as connection:
            with self.assertRaisesRegex(
                pipeline_cutover.PublicationEvidenceError,
                "publication_current_profile_day_receipt_missing",
            ):
                pipeline_cutover.runtime_evidence(connection, at=NOW)
            with self.assertRaisesRegex(
                pipeline_cutover.PublicationEvidenceError,
                "publication_frozen_receipt_reference_changed",
            ):
                pipeline_cutover.verify_frozen_scans(
                    connection,
                    frozen,
                    period_start=BUSINESS_DAY,
                    period_end=BUSINESS_DAY,
                    cutoff_at=NOW,
                )

    def test_schema19_tampered_native_summary_is_rejected(self) -> None:
        with connect(self.db) as connection, transaction(connection):
            connection.execute("DROP TRIGGER trg_day_receipts_no_update")
            connection.execute(
                "UPDATE profile_day_coverage_receipts SET summary_json='{}'"
            )
        with connect(self.db) as connection, self.assertRaisesRegex(
            pipeline_cutover.PublicationEvidenceError,
            "publication_runtime_receipt_lineage_invalid",
        ):
            pipeline_cutover.runtime_evidence(connection, at=NOW)

    def test_schema18_frozen_contract_keeps_legacy_verifier(self) -> None:
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.execute("PRAGMA user_version=18")
        with patch.object(pipeline_cutover, "_verify_frozen_scans_v2") as legacy:
            pipeline_cutover.verify_frozen_scans(
                connection,
                {
                    "contract_version": scan_receipts.MATRIX_FIRST_CONTRACT_VERSION
                },
                period_start=BUSINESS_DAY,
                period_end=BUSINESS_DAY,
                cutoff_at=NOW,
            )
        legacy.assert_called_once()


if __name__ == "__main__":
    unittest.main()
