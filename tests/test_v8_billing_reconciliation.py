from __future__ import annotations

import json
import fcntl
import hashlib
import os
import plistlib
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.roster_fixture import accept_roster
from v8 import billing_reconciliation as billing_module
from v8 import capture
from v8.billing_reconciliation import (
    BillingReconciliationError,
    main,
    open_live_read_only,
    preview_unknown_billing,
    reconcile_unknown_billing,
    unknown_billing_inventory,
)
from v8.paid_dispatch import (
    dispatch_events,
    finish_dispatch_in_transaction,
    mark_dispatch_sent_in_transaction,
    reserve_dispatch_in_transaction,
)
from v8.paid_drain import dispatch_state
from v8.storage import connect, initialize_database, transaction


AT = "2026-09-02T01:00:00Z"


class BillingReconciliationTest(unittest.TestCase):
    def test_malformed_raw_database_receipt_is_classified_as_integrity_failure(
        self,
    ) -> None:
        raw = Path(self.temp.name) / "malformed.json"
        raw.write_bytes(b"{}")
        raw.chmod(0o600)

        with self.assertRaisesRegex(
            BillingReconciliationError, "failed immutable receipt readback"
        ):
            billing_module._verified_raw_receipt(
                {
                    "id": 999,
                    "local_path": str(raw),
                    "sha256": "not-a-sha256",
                    "byte_size": 2,
                    "http_status": 200,
                }
            )

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = Path(self.temp.name) / "billing.sqlite3"
        with connect(self.db) as connection:
            initialize_database(connection)
            connection.execute(
                """INSERT INTO accounts(
                       id,phone,phone_normalized,operator_name,account_type,
                       content_direction,enabled,created_at,updated_at)
                   VALUES (1,'',NULL,'','unknown','unknown',1,?,?)""",
                (AT, AT),
            )
            connection.execute(
                """INSERT INTO content_items(
                       id,link_id,platform,platform_content_id,canonical_url,
                       account_id,raw_account_uid,imported_at,created_at,updated_at)
                   VALUES (1,'C00001','douyin','1','https://example.com/1',
                           1,'10000001',?,?,?)""",
                (AT, AT, AT),
            )
            connection.execute(
                """INSERT INTO provider_budget_batches(
                       id,purpose,provider,operation,currency,verified_unit_price,
                       max_billable_requests,max_amount,pilot_size,daily_quota,
                       price_verified_at,status,consumed_requests,consumed_amount,
                       created_at,updated_at)
                   VALUES ('budget','budget','TikHub','douyin_video_detail','USD',.001,
                           100,1,0,100,?,'approved',1,.001,?,?)""",
                (AT, AT, AT),
            )
            slot = connection.execute(
                """INSERT INTO fetch_slots(
                       content_id,stage,window_key,provider,adapter_version,status,
                       attempt_count,last_error_code,last_error_message,
                       started_at,finished_at,created_at,updated_at)
                   VALUES (1,'detail','lifetime','TikHub','test-v1','retryable_failed',
                           1,?,'manual reconciliation required',?,?,?,?)""",
                (
                    capture.BILLING_UNKNOWN_SLOT_ERROR,
                    AT,
                    AT,
                    AT,
                    AT,
                ),
            )
            self.slot_id = int(slot.lastrowid or 0)
            attempt = connection.execute(
                """INSERT INTO fetch_attempts(
                       slot_id,attempt_number,request_started_at,response_finished_at,
                       billed,amount,currency,error_code,error_message)
                   VALUES (?,1,?,?,0,NULL,'USD','transport_error','timeout')""",
                (self.slot_id, AT, AT),
            )
            self.attempt_id = int(attempt.lastrowid or 0)
            usage = connection.execute(
                """INSERT INTO provider_usage(
                       task_id,budget_batch_id,provider,operation,request_attempts,
                       billed_requests,currency,amount,recorded_at,details_json)
                   VALUES ('task','budget','TikHub','douyin_video_detail',1,1,
                           'USD',.001,?,?)""",
                (
                    AT,
                    json.dumps(
                        {
                            "state": "billing_unknown",
                            "slot_id": self.slot_id,
                            "attempt_number": 1,
                            "category": "detail",
                            "budget_day": "2026-09-02",
                            "error_code": "transport_error",
                        },
                        sort_keys=True,
                    ),
                ),
            )
            self.usage_id = int(usage.lastrowid or 0)
            connection.commit()

    def preview(self, *, outcome: str = "billed") -> dict[str, object]:
        with open_live_read_only(self.db) as connection:
            return preview_unknown_billing(
                connection,
                usage_id=self.usage_id,
                expected_slot_id=self.slot_id,
                expected_attempt_number=1,
                outcome=outcome,  # type: ignore[arg-type]
                evidence_ref="tikhub-billing-export-row-1",
                operator_ref="operator-test",
            )

    def settle(self, *, outcome: str = "billed") -> dict[str, object]:
        preview = self.preview(outcome=outcome)
        with patch("v8.billing_reconciliation.now_utc", return_value=AT):
            return reconcile_unknown_billing(
                db_path=self.db,
                usage_id=self.usage_id,
                expected_slot_id=self.slot_id,
                expected_attempt_number=1,
                outcome=outcome,  # type: ignore[arg-type]
                evidence_ref="tikhub-billing-export-row-1",
                operator_ref="operator-test",
                expected_fingerprint=str(preview["fingerprint"]),
                isolated=True,
            )

    def test_inventory_is_wal_aware_read_only_and_audit_ready(self) -> None:
        with open_live_read_only(self.db) as connection:
            before = connection.total_changes
            result = unknown_billing_inventory(connection, limit=10)
            self.assertEqual(connection.execute("PRAGMA query_only").fetchone()[0], 1)
            self.assertTrue(connection.in_transaction)
            self.assertEqual(connection.total_changes, before)
        self.assertEqual(result["summary"]["unresolved_count"], 1)
        self.assertEqual(result["summary"]["unresolved_microusd"], 1000)
        self.assertEqual(result["summary"]["invalid_linkage_count"], 0)
        item = result["items"][0]
        self.assertTrue(item["valid"])
        self.assertEqual(item["usage_id"], self.usage_id)
        self.assertTrue(item["slot_guard_materialized"])
        self.assertTrue(item["retry_block_required"])

    def test_billed_settlement_keeps_cost_and_writes_immutable_receipt(self) -> None:
        result = self.settle(outcome="billed")
        self.assertTrue(result["slot_unfrozen"])
        with connect(self.db) as connection:
            usage = connection.execute(
                "SELECT * FROM provider_usage WHERE id=?", (self.usage_id,)
            ).fetchone()
            details = json.loads(usage["details_json"])
            self.assertEqual(details["state"], "failed")
            self.assertEqual(details["billing_reconciliation"]["outcome"], "billed")
            self.assertEqual(
                details["billing_reconciliation"]["operator_identity"],
                billing_module._os_operator_identity(),
            )
            self.assertEqual((usage["billed_requests"], usage["amount"]), (1, .001))
            attempt = connection.execute(
                "SELECT billed,amount FROM fetch_attempts WHERE id=?", (self.attempt_id,)
            ).fetchone()
            self.assertEqual((attempt["billed"], attempt["amount"]), (1, .001))
            batch = connection.execute(
                "SELECT consumed_requests,consumed_amount FROM provider_budget_batches"
            ).fetchone()
            self.assertEqual((batch[0], batch[1]), (1, .001))
            slot = connection.execute(
                "SELECT last_error_code FROM fetch_slots WHERE id=?", (self.slot_id,)
            ).fetchone()
            self.assertEqual(slot[0], "transport_error")
            receipt_id = int(result["receipt_attempt_id"])
            with self.assertRaisesRegex(sqlite3.IntegrityError, "one running-to-terminal"):
                connection.execute(
                    "UPDATE scheduler_run_attempts SET details_json='{}' WHERE id=?",
                    (receipt_id,),
                )

    def test_unbilled_settlement_refunds_exactly_once(self) -> None:
        preview = self.preview(outcome="unbilled")
        with patch("v8.billing_reconciliation.now_utc", return_value=AT):
            first = reconcile_unknown_billing(
                db_path=self.db,
                usage_id=self.usage_id,
                expected_slot_id=self.slot_id,
                expected_attempt_number=1,
                outcome="unbilled",
                evidence_ref="tikhub-billing-export-row-1",
                operator_ref="operator-test",
                expected_fingerprint=str(preview["fingerprint"]),
                isolated=True,
            )
            second = reconcile_unknown_billing(
                db_path=self.db,
                usage_id=self.usage_id,
                expected_slot_id=self.slot_id,
                expected_attempt_number=1,
                outcome="unbilled",
                evidence_ref="tikhub-billing-export-row-1",
                operator_ref="operator-test",
                expected_fingerprint=str(preview["fingerprint"]),
                isolated=True,
            )
        self.assertTrue(first["applied"])
        self.assertEqual(second["status"], "already_reconciled")
        with connect(self.db) as connection:
            usage = connection.execute(
                "SELECT billed_requests,amount FROM provider_usage"
            ).fetchone()
            batch = connection.execute(
                "SELECT consumed_requests,consumed_amount FROM provider_budget_batches"
            ).fetchone()
            attempt = connection.execute(
                "SELECT billed,amount FROM fetch_attempts"
            ).fetchone()
        self.assertEqual(tuple(usage), (0, 0.0))
        self.assertEqual(tuple(batch), (0, 0.0))
        self.assertEqual(tuple(attempt), (0, 0.0))

    def test_billing_evidence_cannot_be_reused_across_usage_rows(self) -> None:
        self.settle(outcome="billed")
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                "UPDATE provider_budget_batches SET consumed_requests=2,consumed_amount=.002"
            )
            connection.execute(
                """INSERT INTO fetch_attempts(
                       slot_id,attempt_number,request_started_at,response_finished_at,
                       billed,amount,currency,error_code,error_message)
                   VALUES (?,2,?,?,0,NULL,'USD','transport_error','timeout again')""",
                (self.slot_id, AT, AT),
            )
            second_usage = connection.execute(
                """INSERT INTO provider_usage(
                       task_id,budget_batch_id,provider,operation,request_attempts,
                       billed_requests,currency,amount,recorded_at,details_json)
                   VALUES ('task','budget','TikHub','douyin_video_detail',1,1,
                           'USD',.001,?,?)""",
                (
                    AT,
                    json.dumps(
                        {
                            "state": "billing_unknown",
                            "slot_id": self.slot_id,
                            "attempt_number": 2,
                            "category": "detail",
                            "budget_day": "2026-09-02",
                        },
                        sort_keys=True,
                    ),
                ),
            )
            connection.execute(
                "UPDATE fetch_slots SET attempt_count=2 WHERE id=?", (self.slot_id,)
            )
        with open_live_read_only(self.db) as connection:
            with self.assertRaisesRegex(
                BillingReconciliationError, "cannot settle more than one usage"
            ):
                preview_unknown_billing(
                    connection,
                    usage_id=int(second_usage.lastrowid or 0),
                    expected_slot_id=self.slot_id,
                    expected_attempt_number=2,
                    outcome="billed",
                    evidence_ref="tikhub-billing-export-row-1",
                    operator_ref="operator-test",
                )

    def test_one_of_two_unknowns_does_not_clear_the_slot_guard(self) -> None:
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                "UPDATE provider_budget_batches SET consumed_requests=2,consumed_amount=.002"
            )
            second_attempt = connection.execute(
                """INSERT INTO fetch_attempts(
                       slot_id,attempt_number,request_started_at,response_finished_at,
                       billed,amount,currency,error_code,error_message)
                   VALUES (?,2,?,?,0,NULL,'USD','transport_error','timeout again')""",
                (self.slot_id, AT, AT),
            )
            second_usage = connection.execute(
                """INSERT INTO provider_usage(
                       task_id,budget_batch_id,provider,operation,request_attempts,
                       billed_requests,currency,amount,recorded_at,details_json)
                   VALUES ('task','budget','TikHub','douyin_video_detail',1,1,
                           'USD',.001,?,?)""",
                (
                    AT,
                    json.dumps(
                        {
                            "state": "billing_unknown", "slot_id": self.slot_id,
                            "attempt_number": 2, "category": "detail",
                            "budget_day": "2026-09-02",
                        },
                        sort_keys=True,
                    ),
                ),
            )
            connection.execute(
                "UPDATE fetch_slots SET attempt_count=2 WHERE id=?", (self.slot_id,)
            )
        first = self.settle(outcome="billed")
        self.assertFalse(first["slot_unfrozen"])
        with open_live_read_only(self.db) as connection:
            second_preview = preview_unknown_billing(
                connection,
                usage_id=int(second_usage.lastrowid or 0),
                expected_slot_id=self.slot_id,
                expected_attempt_number=2,
                outcome="billed",
                evidence_ref="tikhub-billing-export-row-2",
                operator_ref="operator-test",
            )
        with patch("v8.billing_reconciliation.now_utc", return_value=AT):
            second = reconcile_unknown_billing(
                db_path=self.db,
                usage_id=int(second_usage.lastrowid or 0),
                expected_slot_id=self.slot_id,
                expected_attempt_number=2,
                outcome="billed",
                evidence_ref="tikhub-billing-export-row-2",
                operator_ref="operator-test",
                expected_fingerprint=str(second_preview["fingerprint"]),
                isolated=True,
            )
        self.assertTrue(second["slot_unfrozen"])
        self.assertEqual(second["remaining_unknown_for_slot"], 0)
        self.assertGreater(int(second_attempt.lastrowid or 0), 0)

    def test_settlement_clears_guard_after_free_derived_success(self) -> None:
        with patch("v8.capture.now_utc", return_value=AT):
            capture.execute_content_fetch(
                content_id=1,
                stage="detail",
                window_key="lifetime",
                provider="Matrix",
                adapter_version="fixture-v1",
                operation="matrix_detail",
                db_path=self.db,
                raw_root=Path(self.temp.name) / "raw",
                call=lambda: capture.ProviderResult({}, {"derived": True}, 200, False),
            )
        with connect(self.db) as connection:
            slot = connection.execute(
                "SELECT status,last_error_code FROM fetch_slots WHERE id=?",
                (self.slot_id,),
            ).fetchone()
            self.assertEqual(slot["status"], "succeeded")
            self.assertEqual(
                slot["last_error_code"], capture.BILLING_UNKNOWN_SLOT_ERROR
            )
        settled = self.settle(outcome="billed")
        self.assertTrue(settled["slot_unfrozen"])
        with connect(self.db) as connection:
            self.assertNotEqual(
                connection.execute(
                    "SELECT last_error_code FROM fetch_slots WHERE id=?",
                    (self.slot_id,),
                ).fetchone()[0],
                capture.BILLING_UNKNOWN_SLOT_ERROR,
            )

    def test_successful_raw_response_fails_closed(self) -> None:
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                """INSERT INTO provider_raw_responses(
                       fetch_attempt_id,content_id,provider,operation,local_path,
                       sha256,byte_size,http_status,captured_at)
                   VALUES (?,1,'TikHub','douyin_video_detail','raw.json',?,2,200,?)""",
                (self.attempt_id, "a" * 64, AT),
            )
        with open_live_read_only(self.db) as connection:
            with self.assertRaisesRegex(
                BillingReconciliationError, "materialize it before allowing a retry"
            ):
                preview_unknown_billing(
                    connection,
                    usage_id=self.usage_id,
                    expected_slot_id=self.slot_id,
                    expected_attempt_number=1,
                    outcome="billed",
                    evidence_ref="tikhub-billing-export-row-1",
                    operator_ref="operator-test",
                )

    def test_raw_response_without_http_status_fails_closed(self) -> None:
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                """INSERT INTO provider_raw_responses(
                       fetch_attempt_id,content_id,provider,operation,local_path,
                       sha256,byte_size,http_status,captured_at)
                   VALUES (?,1,'TikHub','douyin_video_detail','raw.json',?,2,NULL,?)""",
                (self.attempt_id, "a" * 64, AT),
            )
        with open_live_read_only(self.db) as connection:
            with self.assertRaisesRegex(
                BillingReconciliationError, "no HTTP status"
            ):
                preview_unknown_billing(
                    connection,
                    usage_id=self.usage_id,
                    expected_slot_id=self.slot_id,
                    expected_attempt_number=1,
                    outcome="billed",
                    evidence_ref="tikhub-billing-export-row-1",
                    operator_ref="operator-test",
                )

    def test_inventory_exposes_invalid_slot_attempt_linkage(self) -> None:
        with connect(self.db) as connection, transaction(connection):
            row = connection.execute(
                "SELECT details_json FROM provider_usage WHERE id=?", (self.usage_id,)
            ).fetchone()
            details = json.loads(row[0])
            details["attempt_number"] = 99
            connection.execute(
                "UPDATE provider_usage SET details_json=? WHERE id=?",
                (json.dumps(details, sort_keys=True), self.usage_id),
            )
        with open_live_read_only(self.db) as connection:
            result = unknown_billing_inventory(connection, limit=10)
        self.assertEqual(result["summary"]["unresolved_count"], 1)
        self.assertEqual(result["summary"]["invalid_linkage_count"], 1)
        self.assertFalse(result["items"][0]["valid"])
        self.assertEqual(
            result["items"][0]["invalid_reason"], "missing_fetch_attempt"
        )

    def test_inventory_counts_string_slot_attempt_as_invalid(self) -> None:
        with connect(self.db) as connection, transaction(connection):
            details = json.loads(
                connection.execute(
                    "SELECT details_json FROM provider_usage WHERE id=?", (self.usage_id,)
                ).fetchone()[0]
            )
            details.update(slot_id=str(self.slot_id), attempt_number="1")
            connection.execute(
                "UPDATE provider_usage SET details_json=? WHERE id=?",
                (json.dumps(details, sort_keys=True), self.usage_id),
            )
        with open_live_read_only(self.db) as connection:
            result = unknown_billing_inventory(connection, limit=10)
        self.assertEqual(result["summary"]["invalid_linkage_count"], 1)
        self.assertFalse(result["items"][0]["valid"])
        self.assertEqual(
            result["items"][0]["invalid_reason"], "invalid_slot_attempt_identity"
        )

    def test_schema19_scoped_unknown_requires_dispatch_chain(self) -> None:
        with connect(self.db) as connection, transaction(connection):
            details = json.loads(
                connection.execute(
                    "SELECT details_json FROM provider_usage WHERE id=?", (self.usage_id,)
                ).fetchone()[0]
            )
            details["scope"] = {
                "activation_id": 999,
                "scheduler_run_id": 998,
                "scheduler_attempt_id": 997,
            }
            connection.execute(
                "UPDATE provider_usage SET details_json=? WHERE id=?",
                (json.dumps(details, sort_keys=True), self.usage_id),
            )
        with open_live_read_only(self.db) as connection:
            inventory = unknown_billing_inventory(connection, limit=10)
            self.assertFalse(inventory["items"][0]["valid"])
            self.assertEqual(
                inventory["items"][0]["invalid_reason"],
                "missing_paid_dispatch_evidence",
            )
            with self.assertRaisesRegex(
                BillingReconciliationError, "no paid dispatch evidence"
            ):
                preview_unknown_billing(
                    connection,
                    usage_id=self.usage_id,
                    expected_slot_id=self.slot_id,
                    expected_attempt_number=1,
                    outcome="billed",
                    evidence_ref="tikhub-billing-export-row-1",
                    operator_ref="operator-test",
                )

    def test_schema19_dispatch_chain_is_bound_into_settlement_receipt(self) -> None:
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                """INSERT INTO account_platform_identities(
                       account_id,platform,uid,nickname,source,created_at,updated_at)
                   VALUES (1,'douyin','10000001','fixture','test',?,?)""",
                (AT, AT),
            )
            accept_roster(connection, accepted_at="2026-09-02T00:59:00Z")
            state = dispatch_state(connection, at=AT)
            self.assertTrue(state.paid_dispatch_open)
            assert state.activation_id is not None
            run = connection.execute(
                """INSERT INTO scheduler_runs(
                       job_id,scheduled_for,status,started_at,details_json)
                   VALUES ('billing-dispatch-test',?,'running',?,'{}')""",
                (AT, AT),
            )
            attempt = connection.execute(
                """INSERT INTO scheduler_run_attempts(
                       scheduler_run_id,attempt_number,invocation_source,status,
                       started_at,details_json)
                   VALUES (?,1,'scheduled','running',?,'{}')""",
                (run.lastrowid, AT),
            )
            details = json.loads(
                connection.execute(
                    "SELECT details_json FROM provider_usage WHERE id=?", (self.usage_id,)
                ).fetchone()[0]
            )
            details["scope"] = {
                "activation_id": int(state.activation_id),
                "scheduler_run_id": int(run.lastrowid or 0),
                "scheduler_attempt_id": int(attempt.lastrowid or 0),
            }
            connection.execute(
                "UPDATE provider_usage SET details_json=? WHERE id=?",
                (json.dumps(details, sort_keys=True), self.usage_id),
            )
            reserved = reserve_dispatch_in_transaction(
                connection,
                provider="TikHub",
                operation="douyin_video_detail",
                activation_id=int(state.activation_id),
                business_day="2026-09-02",
                scheduler_run_id=int(run.lastrowid or 0),
                scheduler_attempt_id=int(attempt.lastrowid or 0),
                scope={"purpose": "billing-test"},
                provider_usage_id=self.usage_id,
                fetch_slot_id=self.slot_id,
                created_at=AT,
            )
            assert reserved is not None
            mark_dispatch_sent_in_transaction(
                connection,
                reserved.dispatch_id,
                fetch_attempt_id=self.attempt_id,
                created_at=AT,
            )
            finish_dispatch_in_transaction(
                connection,
                reserved.dispatch_id,
                outcome="billing_unknown",
                created_at=AT,
            )
            dispatch_id = reserved.dispatch_id
        preview = self.preview(outcome="billed")
        evidence = preview["dispatch_evidence"]
        self.assertEqual(evidence["dispatch_id"], dispatch_id)
        before_hashes = list(evidence["event_hashes"])
        settled = self.settle(outcome="billed")
        self.assertEqual(
            settled["resolution"]["dispatch_evidence"]["event_hashes"], before_hashes
        )
        with connect(self.db) as connection:
            self.assertEqual(
                [event.event_hash for event in dispatch_events(connection, dispatch_id)],
                before_hashes,
            )

    def test_inventory_exposes_verified_external_request_identifiers(self) -> None:
        raw_path = Path(self.temp.name) / "provider-400.json"
        body = json.dumps(
            {"detail": {"request_id": "tikhub-request-fixture"}},
            sort_keys=True,
        ).encode("utf-8")
        raw_path.write_bytes(body)
        raw_path.chmod(0o600)
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                """INSERT INTO provider_raw_responses(
                       fetch_attempt_id,content_id,provider,operation,local_path,
                       sha256,byte_size,http_status,captured_at)
                   VALUES (?,1,'TikHub','douyin_video_detail',?,?,?,?,?)""",
                (
                    self.attempt_id,
                    str(raw_path),
                    hashlib.sha256(body).hexdigest(),
                    len(body),
                    400,
                    AT,
                ),
            )
        with open_live_read_only(self.db) as connection:
            result = unknown_billing_inventory(connection, limit=10)
            preview = preview_unknown_billing(
                connection,
                usage_id=self.usage_id,
                expected_slot_id=self.slot_id,
                expected_attempt_number=1,
                outcome="billed",
                evidence_ref="tikhub-billing-export-row-1",
                operator_ref="operator-test",
            )
        item = result["items"][0]
        self.assertTrue(item["valid"])
        self.assertEqual(item["content_platform"], "douyin")
        self.assertEqual(item["platform_content_id"], "1")
        self.assertEqual(
            item["raw_receipts"][0]["request_id"], "tikhub-request-fixture"
        )
        self.assertTrue(item["raw_receipts"][0]["evidence_valid"])
        self.assertEqual(
            preview["raw_receipts"][0]["local_path"], str(raw_path)
        )
        self.assertEqual(
            preview["subject_identity"]["content"]["platform_content_id"], "1"
        )

        raw_path.write_text("tampered", encoding="utf-8")
        with open_live_read_only(self.db) as connection:
            with self.assertRaisesRegex(
                BillingReconciliationError, "immutable receipt"
            ):
                preview_unknown_billing(
                    connection,
                    usage_id=self.usage_id,
                    expected_slot_id=self.slot_id,
                    expected_attempt_number=1,
                    outcome="billed",
                    evidence_ref="tikhub-billing-export-row-1",
                    operator_ref="operator-test",
                )

    def test_stale_fingerprint_and_missing_apply_confirmation_fail(self) -> None:
        with self.assertRaisesRegex(BillingReconciliationError, "changed after review"):
            reconcile_unknown_billing(
                db_path=self.db,
                usage_id=self.usage_id,
                expected_slot_id=self.slot_id,
                expected_attempt_number=1,
                outcome="billed",
                evidence_ref="tikhub-billing-export-row-1",
                operator_ref="operator-test",
                expected_fingerprint="0" * 64,
                isolated=True,
            )
        exit_code = main(
            [
                "--db", str(self.db), "settle", "--usage-id", str(self.usage_id),
                "--expected-slot-id", str(self.slot_id),
                "--expected-attempt-number", "1", "--outcome", "billed",
                "--evidence-ref", "evidence", "--operator-ref", "operator", "--apply",
            ]
        )
        self.assertEqual(exit_code, 2)

    def test_cli_requires_explicit_isolated_database_authorization(self) -> None:
        self.assertEqual(main(["--db", str(self.db), "list"]), 2)
        self.assertEqual(
            main(["--db", str(self.db), "--isolated-db", "list"]), 0
        )

    def test_fingerprint_binds_outcome_evidence_and_operator(self) -> None:
        billed = self.preview(outcome="billed")
        variants = (
            ("unbilled", "tikhub-billing-export-row-1", "operator-test"),
            ("billed", "different-evidence", "operator-test"),
            ("billed", "tikhub-billing-export-row-1", "different-operator"),
        )
        for outcome, evidence_ref, operator_ref in variants:
            with self.subTest(
                outcome=outcome, evidence_ref=evidence_ref, operator_ref=operator_ref
            ):
                with self.assertRaisesRegex(
                    BillingReconciliationError, "changed after review"
                ):
                    reconcile_unknown_billing(
                        db_path=self.db,
                        usage_id=self.usage_id,
                        expected_slot_id=self.slot_id,
                        expected_attempt_number=1,
                        outcome=outcome,  # type: ignore[arg-type]
                        evidence_ref=evidence_ref,
                        operator_ref=operator_ref,
                        expected_fingerprint=str(billed["fingerprint"]),
                        isolated=True,
                    )

    def test_reconciled_old_usage_cannot_own_a_later_claim(self) -> None:
        self.settle(outcome="billed")
        old_claim = capture.SlotClaim(
            slot_id=self.slot_id,
            attempt_id=self.attempt_id,
            attempt_number=1,
            content_id=1,
            stage="detail",
            window_key="lifetime",
            provider="TikHub",
            adapter_version="test-v1",
            reserved_usage_id=self.usage_id,
            reserved_unit_price=.001,
            reserved_currency="USD",
        )
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                """UPDATE fetch_slots SET status='running',attempt_count=1,
                       last_error_code=NULL,last_error_message=NULL WHERE id=?""",
                (self.slot_id,),
            )
            connection.execute(
                """INSERT INTO provider_usage(
                       task_id,budget_batch_id,provider,operation,request_attempts,
                       billed_requests,currency,amount,recorded_at,details_json)
                   VALUES ('task','budget','TikHub','douyin_video_detail',0,1,
                           'USD',.001,?,?)""",
                (
                    AT,
                    json.dumps(
                        {
                            "state": "reserved", "slot_id": self.slot_id,
                            "attempt_number": 2,
                        },
                        sort_keys=True,
                    ),
                ),
            )
            self.assertFalse(
                capture._paid_slot_owner(connection, old_claim, check_scheduler=False)
            )

    def test_formal_apply_requires_freeze_and_stopped_writer(self) -> None:
        canonical_root = Path(self.temp.name) / "canonical"
        canonical_root.mkdir()
        canonical_root = canonical_root.resolve(strict=True)
        (canonical_root / "runtime").mkdir()
        freeze = canonical_root / "runtime" / "operator-freeze.lock"
        installed_root = Path(self.temp.name) / "installed"
        (installed_root / "Library" / "LaunchAgents").mkdir(parents=True)
        installed_root = installed_root.resolve(strict=True)
        runtime = (
            installed_root
            / "Library"
            / "Application Support"
            / "DcarAIGC"
            / "runtime"
        )
        runtime.mkdir(parents=True)
        writer = runtime / "writer-worker.lock"
        writer.touch(mode=0o600)
        plist = (
            installed_root
            / "Library"
            / "LaunchAgents"
            / "cn.tj.dcar.writer-worker.plist"
        )
        plist.write_bytes(
            plistlib.dumps(
                {
                    "Label": "cn.tj.dcar.writer-worker",
                    "WorkingDirectory": str(canonical_root),
                    "ProgramArguments": [
                        str(canonical_root / "deploy/macos/run_writer_worker.sh")
                    ],
                    "EnvironmentVariables": {
                        "DCAR_PROJECT_ROOT": str(canonical_root),
                        "DCAR_OPERATOR_FREEZE_LOCK": str(freeze),
                        "DCAR_WRITER_LOCK": str(writer),
                        "DCAR_V8_DB": str(self.db),
                    },
                }
            )
        )
        plist.chmod(0o644)
        arguments = {
            "db_path": self.db,
            "usage_id": self.usage_id,
            "expected_slot_id": self.slot_id,
            "expected_attempt_number": 1,
            "outcome": "billed",
            "evidence_ref": "tikhub-billing-export-row-1",
            "operator_ref": "operator-test",
            "expected_fingerprint": str(self.preview()["fingerprint"]),
        }
        with (
            patch.object(billing_module, "PROJECT_ROOT", canonical_root),
            patch("v8.runtime_database._current_home", return_value=installed_root),
        ):
            with self.assertRaisesRegex(
                BillingReconciliationError, "requires.*operator-freeze"
            ):
                reconcile_unknown_billing(**arguments)  # type: ignore[arg-type]
            freeze.write_text("{}", encoding="utf-8")
            freeze.chmod(0o600)
            descriptor = os.open(writer, os.O_CREAT | os.O_RDWR, 0o600)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with self.assertRaisesRegex(
                    BillingReconciliationError, "writer lock is already held"
                ):
                    reconcile_unknown_billing(**arguments)  # type: ignore[arg-type]
            finally:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
                os.close(descriptor)
            installed = plistlib.loads(plist.read_bytes())
            installed["WorkingDirectory"] = str(Path(self.temp.name) / "other-checkout")
            plist.write_bytes(plistlib.dumps(installed))
            plist.chmod(0o644)
            with self.assertRaisesRegex(
                BillingReconciliationError, "project root"
            ):
                reconcile_unknown_billing(**arguments)  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
