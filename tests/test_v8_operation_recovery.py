from __future__ import annotations

import hashlib
import json
import multiprocessing
import sqlite3
import threading
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from tests import test_v8_provider_budget as fixture
from v8 import capture, operation_recovery as recovery, provider_budget as budget
from v8.storage import connect, transaction
from v8.work_readiness import assess_work_readiness

AT = fixture.AT
OP = "douyin_video_detail"
DUE = "2026-08-29T04:05:00Z"
GOOD_PAYLOAD = {"code": 200, "data": {"aweme_list": [{"aweme_id": "1", "desc": "one"}, {"aweme_id": "2", "desc": "two"}]}}


def sent_usage(connection, identity, at=DUE):
    return connection.execute("""INSERT INTO provider_usage(provider,operation,currency,amount,
        request_attempts,recorded_at,details_json) VALUES('TikHub',?,'USD',.001,1,?,?)""",
        (OP, at, json.dumps({"state": "sent", "paid_scope_identity": identity, "sent_at": at}))).lastrowid


def child_contender(path, at, queue):
    path = Path(path)
    try:
        with recovery.operation_probe_lock(db_path=path, operation=OP):
            with connect(path) as connection, transaction(connection):
                usage = sent_usage(connection, "child", at)
                recovery.claim_operation_probe(connection, operation=OP, usage_id=usage,
                    identity="child", sequence=0, at=at)
        queue.put("claimed")
    except budget.BudgetBlocked as error:
        queue.put(error.error_code)


class OperationRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.f = fixture.ProviderBudgetTest()
        self.f.setUp()
        self.f.roster()
        self.batch = self.f.budget()

    def tearDown(self):
        self.f.tearDown()

    def open(self, fault_class="transport", at=AT):
        with connect(self.f.db) as connection, transaction(connection):
            return budget.record_fault_state(connection, scope_kind="operation", operation=OP,
                fault_class=fault_class, reason=fault_class, usage_id=None, at=at)

    def state(self, fault_class="transport"):
        with connect(self.f.db) as connection:
            return budget.fault_state(connection, scope_kind="operation", operation=OP, fault_class=fault_class)

    def good(self, payload=None):
        self.f.calls += 1
        payload = GOOD_PAYLOAD if payload is None else payload
        entity = json.dumps(payload).encode()
        receipt = {"contract_version": capture.TRANSPORT_CONTRACT_VERSION, "status": "succeeded",
            "error_code": None, "json_parse_ok": True, "clean_eof": True, "http_status": 200,
            "entity_bytes": len(entity), "entity_sha256": hashlib.sha256(entity).hexdigest(),
            "content_encoding": "identity", "content_length": len(entity), "length_match": True,
            "gzip_crc_ok": None, "zero_body": False, "transport_route_id": "fixture",
            "route_generation": "fixture", "http_stack": "fixture", "request_host": "example.test"}
        return capture.ProviderResult({}, payload, 200, True, entity, receipt)

    def execute(self, at=DUE, content_id=1, call=None):
        with patch("v8.capture.now_utc", return_value=at):
            return self.f.execute(self.batch, content_id=content_id, call=call or self.good)

    def test_cooldown_five_minutes_and_offline_authority(self):
        self.open()
        with connect(self.f.db) as connection:
            self.assertTrue(recovery.operation_faults_allow_authority(connection, operation=OP))
            self.assertFalse(recovery.operation_recovery_due(connection, operation=OP, at="2026-08-29T04:04:59Z"))
            self.assertTrue(recovery.operation_recovery_due(connection, operation=OP, at=DUE))
            before = connection.total_changes
            early = assess_work_readiness(connection, operation=OP, category="detail", at=AT, content_id=1)
            due = assess_work_readiness(connection, operation=OP, category="detail", at=DUE, content_id=1)
            self.assertFalse(early["runnable"])
            self.assertTrue(due["runnable"])
            self.assertEqual(connection.total_changes, before)
        with self.assertRaises(budget.BudgetBlocked):
            self.execute(at="2026-08-29T04:04:59Z")
        self.assertEqual(self.f.calls, 0)

    def test_real_capture_closes_after_verified_raw_once(self):
        self.open()
        result = self.execute()
        state = self.state()
        self.assertFalse(state["open"])
        self.assertEqual(state["recovery_evidence_id"], result.raw_response_id)
        self.assertEqual(self.f.calls, 1)
        self.assertEqual(self.f.usage()[0]["amount"], .001)
        self.assertNotIn("half_open", state)

    def test_rate_fault_recovers_through_same_bounded_path(self):
        self.open("rate_limit")
        self.execute()
        self.assertFalse(self.state("rate_limit")["open"])

    def test_http_success_with_wrong_business_identity_keeps_fault_and_raw(self):
        self.open()
        outcome = self.execute(call=lambda: self.good({"code": 200, "data": {"aweme_id": "wrong"}}))
        self.assertTrue(self.state()["open"])
        self.assertTrue(self.state("field_contract")["open"])
        self.assertEqual(json.loads(self.f.usage()[0]["details_json"])["state"], "completed")
        self.assertIsNotNone(outcome.raw_response_id)
        with connect(self.f.db) as connection:
            self.assertFalse(recovery.operation_recovery_due(connection, operation=OP, at="2026-08-30T04:05:00Z"))

    def test_unsupported_operation_has_no_automatic_probe(self):
        operation = "douyin_video_comments"
        with connect(self.f.db) as connection, transaction(connection):
            budget.record_fault_state(connection, scope_kind="operation", operation=operation,
                fault_class="transport", reason="transport", usage_id=None, at=AT)
            self.assertFalse(recovery.operation_recovery_due(connection, operation=operation, at=DUE))

    def test_multiple_transient_faults_close_together(self):
        self.open()
        self.open("rate_limit")
        self.execute()
        self.assertFalse(self.state()["open"])
        self.assertFalse(self.state("rate_limit")["open"])

    def test_transport_failure_keeps_unknown_identity_and_doubles_cooldown(self):
        self.open()
        def broken():
            raise capture.CaptureError("broken", retryable=True, error_code="transport_error", billed=None)
        with self.assertRaises(capture.CaptureError):
            self.execute(call=broken)
        state = self.state()
        self.assertTrue(state["open"])
        self.assertEqual(state["cooldown"]["retry_after"], "2026-08-29T04:15:00Z")
        self.assertNotIn("half_open", state)
        self.assertEqual(json.loads(self.f.usage()[0]["details_json"])["state"], "billing_unknown")
        with self.assertRaises((budget.BudgetBlocked, capture.CaptureError, capture.SlotUnavailable)):
            self.execute(at="2026-08-29T04:15:00Z")
        self.assertEqual(self.f.calls, 0)
        self.execute(at="2026-08-29T04:15:00Z", content_id=2)
        self.assertEqual(self.f.calls, 1)
        self.assertFalse(self.state()["open"])
        self.assertEqual(json.loads(self.f.usage()[0]["details_json"])["state"], "billing_unknown")

    def test_backoff_caps_at_thirty_minutes(self):
        self.open()
        at = DUE
        for index, minutes in enumerate((10, 20, 30, 30)):
            with recovery.operation_probe_lock(db_path=self.f.db, operation=OP):
                with connect(self.f.db) as connection, transaction(connection):
                    usage = sent_usage(connection, str(index), at)
                    recovery.claim_operation_probe(connection, operation=OP, usage_id=usage,
                        identity=str(index), sequence=0, at=at)
                    recovery.finish_operation_probe(connection, operation=OP, usage_id=usage, at=at, succeeded=False)
            expected = (datetime.fromisoformat(at.replace("Z", "+00:00")) + timedelta(minutes=minutes)).isoformat().replace("+00:00", "Z")
            self.assertEqual(self.state()["cooldown"]["retry_after"], expected)
            at = expected

    def test_process_exit_releases_lock_but_restart_waits_for_lease(self):
        self.open()
        with recovery.operation_probe_lock(db_path=self.f.db, operation=OP):
            with connect(self.f.db) as connection, transaction(connection):
                usage = sent_usage(connection, "abandoned")
                recovery.claim_operation_probe(connection, operation=OP, usage_id=usage,
                    identity="abandoned", sequence=0, at=DUE)
        with connect(self.f.db) as connection:
            self.assertFalse(recovery.operation_recovery_due(connection, operation=OP, at="2026-08-29T04:06:59Z"))
            self.assertTrue(recovery.operation_recovery_due(connection, operation=OP, at="2026-08-29T04:07:00Z"))
        self.execute(at="2026-08-29T04:07:00Z")
        self.assertFalse(self.state()["open"])
        self.assertEqual(self.f.usage()[0]["request_attempts"], 1)

    def test_process_lock_prevents_overlap_even_after_lease_expiry(self):
        self.open()
        context = multiprocessing.get_context("spawn")
        queue = context.Queue()
        with recovery.operation_probe_lock(db_path=self.f.db, operation=OP):
            with connect(self.f.db) as connection, transaction(connection):
                usage = sent_usage(connection, "parent")
                recovery.claim_operation_probe(connection, operation=OP, usage_id=usage,
                    identity="parent", sequence=0, at=DUE)
            process = context.Process(target=child_contender, args=(str(self.f.db), "2026-08-29T04:08:00Z", queue))
            process.start()
            process.join(10)
            self.assertFalse(process.is_alive())
            self.assertEqual(process.exitcode, 0)
            self.assertEqual(queue.get(timeout=2), "operation_blocked")
        self.assertEqual(len(self.f.usage()), 1)

    def test_failed_contender_refunds_reservation(self):
        self.open()
        with recovery.operation_probe_lock(db_path=self.f.db, operation=OP):
            with self.assertRaises(budget.BudgetBlocked):
                self.execute()
        usage = self.f.usage()
        self.assertEqual(len(usage), 1)
        self.assertEqual(usage[0]["amount"], 0)
        self.assertEqual(usage[0]["request_attempts"], 0)
        self.assertEqual(json.loads(usage[0]["details_json"])["state"], "not_sent")
        self.assertEqual(self.f.calls, 0)
        self.assertNotIn("half_open", self.state())
        # The same unstarted identity must remain retryable after contention;
        # no immutable paid-send marker may have escaped the failed B transaction.
        self.execute()
        self.assertEqual(self.f.calls, 1)
        self.assertFalse(self.state()["open"])

    def test_budget_and_storage_hard_still_block_recovery(self):
        self.open()
        self.f.prefill(15, category="metrics", operation=OP)
        with self.assertRaises(budget.BudgetBlocked) as error:
            self.execute()
        self.assertEqual(error.exception.error_code, "metrics_budget_exhausted")
        with connect(self.f.db) as connection, transaction(connection):
            budget.record_fault_state(connection, scope_kind="storage_hard", provider="all",
                fault_class="local_evidence_store", reason="storage_hard", usage_id=None, at=AT)
        with self.assertRaises(budget.BudgetBlocked) as error:
            self.execute()
        self.assertEqual(error.exception.error_code, "storage_hard")
        self.assertEqual(self.f.calls, 0)

    def test_field_fault_never_gets_temporary_authority_or_auto_probe(self):
        self.open()
        self.open("field_contract")
        with connect(self.f.db) as connection:
            self.assertFalse(recovery.operation_faults_allow_authority(connection, operation=OP))
            self.assertFalse(recovery.operation_recovery_due(connection, operation=OP, at=DUE))
        with self.assertRaises(budget.BudgetBlocked):
            self.execute()
        self.assertEqual(self.f.calls, 0)
        self.assertTrue(self.state("field_contract")["open"])

    def test_missing_or_failed_raw_proof_does_not_close(self):
        self.open()
        self.execute(call=self.f.call)
        self.assertTrue(self.state()["open"])
        self.assertEqual(self.state()["cooldown"]["retry_after"], "2026-08-29T04:15:00Z")

    def test_failed_raw_readback_keeps_fault_and_opens_storage_hold(self):
        from v8.raw_evidence import RawEvidenceError
        self.open()
        with patch("v8.raw_evidence.read_raw_evidence", side_effect=RawEvidenceError("corrupt readback")):
            with self.assertRaises(capture.CaptureError) as error:
                self.execute()
        self.assertEqual(error.exception.error_code, "storage_hard")
        self.assertTrue(self.state()["open"])
        self.assertNotIn("half_open", self.state())
        with connect(self.f.db) as connection:
            self.assertTrue(budget.fault_state(connection, scope_kind="storage_hard", provider="all")["open"])

    def test_balance_and_credentials_remain_hard_blocks(self):
        self.open()
        with connect(self.f.db) as connection, transaction(connection):
            budget.record_circuit(connection, reason="provider_balance_blocked", usage_id=None, at=AT)
            budget.record_circuit(connection, reason="provider_auth_blocked", usage_id=None, at=AT)
        with self.assertRaises(budget.BudgetBlocked) as error:
            self.execute()
        self.assertEqual(error.exception.error_code, "provider_circuit_open")
        self.assertEqual(self.f.calls, 0)
        with connect(self.f.db) as connection:
            self.assertTrue(budget.fault_state(connection, scope_kind="provider_hard", fault_class="balance")["open"])
            self.assertTrue(budget.fault_state(connection, scope_kind="provider_hard", fault_class="application_auth")["open"])

    def test_failure_record_preserves_probe_owner_for_backoff(self):
        self.open("rate_limit")
        def limited():
            raise capture.CaptureError("rate", retryable=True, error_code="provider_rate_limited", http_status=429, billed=False)
        with self.assertRaises(capture.CaptureError):
            self.execute(call=limited)
        state = self.state("rate_limit")
        self.assertEqual(state["cooldown"]["failures"], 1)
        self.assertEqual(state["cooldown"]["retry_after"], "2026-08-29T04:15:00Z")
        self.assertNotIn("half_open", state)

    def test_success_does_not_close_a_fault_opened_after_probe_claim(self):
        self.open()
        def intervening_fault():
            self.open("rate_limit", at=DUE)
            return self.good()
        self.execute(call=intervening_fault)
        self.assertFalse(self.state()["open"])
        self.assertTrue(self.state("rate_limit")["open"])
        self.assertNotIn("half_open", self.state("rate_limit"))

    def test_same_class_failure_from_another_usage_supersedes_probe(self):
        initial = self.open()
        def intervening_failure():
            with connect(self.f.db) as connection, transaction(connection):
                budget.record_fault_state(connection, scope_kind="operation", operation=OP,
                    fault_class="transport", reason="transport", usage_id=999, at=DUE)
            return self.good()
        self.execute(call=intervening_failure)
        state = self.state()
        self.assertTrue(state["open"])
        self.assertNotEqual(initial["generation"], state["generation"])
        self.assertNotIn("half_open", state)

    def test_failed_probe_preserves_other_requests_rate_limit_generation(self):
        self.open()
        retained = {}
        def interleaved_failure():
            with connect(self.f.db) as connection, transaction(connection):
                usage = sent_usage(connection, "other-request")
                connection.execute("UPDATE provider_usage SET details_json=? WHERE id=?",
                    (json.dumps({"state": "completed", "retry_after_seconds": 3600}), usage))
                budget.record_fault_state(connection, scope_kind="operation", operation=OP,
                    fault_class="rate_limit", reason="provider_rate_limited", usage_id=usage, at=DUE)
                retained.update(budget.fault_state(connection, scope_kind="operation", operation=OP, fault_class="rate_limit"))
            raise capture.CaptureError("transport", retryable=True, error_code="transport_error", billed=None)
        with self.assertRaises(capture.CaptureError):
            self.execute(call=interleaved_failure)
        self.assertEqual(self.state("rate_limit"), retained)
        with connect(self.f.db) as connection:
            self.assertFalse(recovery.operation_recovery_due(connection, operation=OP, at="2026-08-29T04:50:00Z"))

    def test_schema20_archived_blob_is_read_back_before_success(self):
        from v8 import raw_archive
        from v8.storage import initialize_database
        path = self.f.db.parent / "schema20.sqlite3"
        entity = json.dumps(GOOD_PAYLOAD).encode()
        with connect(path) as connection:
            initialize_database(connection, target_version=20)
            with transaction(connection):
                blob_id = raw_archive.put_blob(connection, entity, live_root=(self.f.raw / "blobs").resolve(),
                                               raw_stored_at=DUE)
                blob = connection.execute("SELECT * FROM provider_raw_blobs WHERE id=?", (blob_id,)).fetchone()
                raw_id = connection.execute("""INSERT INTO provider_raw_responses(provider,operation,local_path,
                    sha256,byte_size,http_status,captured_at,raw_blob_id,raw_stored_at) VALUES('TikHub',?,?,?,?,200,?,?,?)""",
                    (OP, blob["hot_path"], blob["stored_sha256"], blob["stored_size"], DUE, blob_id, DUE)).lastrowid
                details = {"state": "completed", "raw_response_id": raw_id,
                    "paid_identity": {"provider": "tikhub", "operation": OP, "subject": "1"}, "transport": {
                    "clean_eof": True, "json_parse_ok": True, "http_status": 200,
                    "entity_sha256": hashlib.sha256(entity).hexdigest()}}
                usage_id = connection.execute("""INSERT INTO provider_usage(provider,operation,request_attempts,
                    recorded_at,details_json) VALUES('TikHub',?,1,?,?)""", (OP, DUE, json.dumps(details))).lastrowid
                self.assertTrue(recovery._verified_success(connection, usage_id, raw_id))

    def test_initial_rate_fault_honors_provider_retry_after_seconds(self):
        def limited():
            raise capture.CaptureError("rate", retryable=True, error_code="provider_rate_limited",
                                       http_status=429, billed=False, retry_after_seconds=1200)
        with self.assertRaises(capture.CaptureError):
            self.execute(at=AT, call=limited)
        with connect(self.f.db) as connection:
            self.assertFalse(recovery.operation_recovery_due(connection, operation=OP, at=DUE))
            self.assertFalse(recovery.operation_recovery_due(connection, operation=OP, at="2026-08-29T04:19:59Z"))
            self.assertTrue(recovery.operation_recovery_due(connection, operation=OP, at="2026-08-29T04:20:00Z"))

    def test_legacy_fault_with_new_truncated_usage_blocks_actual_reservation_until_cooldown(self):
        self.open()
        fault = self.state()
        with connect(self.f.db) as connection, transaction(connection):
            details = {'state': 'billing_unknown', 'error_code': 'transport_error', 'sent_at': DUE,
                       'transport': {'http_status': 200, 'clean_eof': False}}
            connection.execute("INSERT INTO provider_usage(provider,operation,request_attempts,recorded_at,details_json) "
                "VALUES('TikHub',?,1,?,?)", (OP, DUE, json.dumps(details)))
        with connect(self.f.db) as connection:
            with self.assertRaises(budget.PaidScopeBlocked) as caught:
                budget.check_reservation(connection, scope=budget.PaidScope(category='detail'), operation=OP,
                    unit_price=.001, currency='USD', at='2026-08-29T04:09:59Z')
            self.assertEqual(caught.exception.error_code, 'operation_blocked')
            self.assertTrue(recovery.operation_recovery_due(connection, operation=OP, at='2026-08-29T04:10:00Z'))
            self.assertEqual(budget.fault_state(connection, scope_kind='operation', operation=OP), fault)

    def test_probe_failure_honors_longer_provider_retry_after(self):
        self.open("rate_limit")
        def limited():
            raise capture.CaptureError("rate", retryable=True, error_code="provider_rate_limited",
                                       http_status=429, billed=False, retry_after_seconds=3600)
        with self.assertRaises(capture.CaptureError):
            self.execute(call=limited)
        self.assertEqual(self.state("rate_limit")["cooldown"]["retry_after"], "2026-08-29T05:05:00Z")

    def test_retry_after_http_date_numeric_header_and_invalid_value(self):
        for header, expected in (("Sat, 29 Aug 2026 05:00:00 GMT", "2026-08-29T05:00:00Z"),
                                 ("1200", "2026-08-29T04:20:00Z"),
                                 ("not-a-date", None), ("inf", None)):
            with self.subTest(header=header), connect(self.f.db) as connection, transaction(connection):
                usage_id = sent_usage(connection, header, AT)
                connection.execute("UPDATE provider_usage SET details_json=? WHERE id=?",
                    (json.dumps({"transport": {"retry_after": header}}), usage_id))
                deadline = recovery._provider_retry_after(connection, usage_id, AT)
                self.assertEqual(recovery._utc(deadline) if deadline is not None else None, expected)

    def test_late_429_extends_open_cooldown_and_never_shortens_prior_deadline(self):
        started, release, done = threading.Event(), threading.Event(), threading.Event()
        errors = []
        clock = [AT]
        def late_rate():
            started.set()
            if not release.wait(5):
                raise AssertionError("late response was not released")
            raise capture.CaptureError("late rate", retryable=True, error_code="provider_rate_limited",
                                       http_status=429, billed=False, retry_after_seconds=3600)
        def run_late():
            try:
                self.f.execute(self.batch, content_id=2, call=late_rate)
            except Exception as error:
                errors.append(error)
            finally:
                done.set()
        with patch("v8.capture.now_utc", side_effect=lambda: clock[0]):
            worker = threading.Thread(target=run_late)
            worker.start()
            self.assertTrue(started.wait(5))
            def first_rate():
                raise capture.CaptureError("first rate", retryable=True, error_code="provider_rate_limited",
                                           http_status=429, billed=False, retry_after_seconds=600)
            with self.assertRaises(capture.CaptureError):
                self.f.execute(self.batch, call=first_rate)
            initial = self.state("rate_limit")
            clock[0] = "2026-08-29T04:01:00Z"
            release.set()
            self.assertTrue(done.wait(5))
            worker.join(5)
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], capture.CaptureError)
        current = self.state("rate_limit")
        self.assertNotEqual(current["generation"], initial["generation"])
        self.assertEqual(current["cooldown"]["retry_after"], "2026-08-29T05:01:00Z")
        with connect(self.f.db) as connection, transaction(connection):
            usage_id = sent_usage(connection, "shorter", "2026-08-29T04:02:00Z")
            budget.record_fault_state(connection, scope_kind="operation", operation=OP,
                fault_class="rate_limit", reason="provider_rate_limited", usage_id=usage_id,
                at="2026-08-29T04:02:00Z")
        self.assertEqual(self.state("rate_limit")["cooldown"]["retry_after"], "2026-08-29T05:01:00Z")

    def test_real_late_transport_failure_cannot_be_cleared_by_older_probe_success(self):
        started, release, done = threading.Event(), threading.Event(), threading.Event()
        errors = []
        clock = [AT]
        def late_transport():
            started.set()
            if not release.wait(5):
                raise AssertionError("late response was not released")
            raise capture.CaptureError("late transport", retryable=True, error_code="transport_error", billed=None)
        def run_late():
            try:
                self.f.execute(self.batch, content_id=2, call=late_transport)
            except Exception as error:
                errors.append(error)
            finally:
                done.set()
        with patch("v8.capture.now_utc", side_effect=lambda: clock[0]):
            worker = threading.Thread(target=run_late)
            worker.start()
            self.assertTrue(started.wait(5))
            initial = self.open()
            clock[0] = DUE
            def successful_probe():
                release.set()
                if not done.wait(5):
                    raise AssertionError("late capture did not finish")
                return self.good()
            self.f.execute(self.batch, call=successful_probe)
            worker.join(5)
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], capture.CaptureError)
        state = self.state()
        self.assertTrue(state["open"])
        self.assertNotEqual(state["generation"], initial["generation"])
        self.assertNotIn("half_open", state)
        self.assertEqual(self.f.calls, 1)
        usage = {row["id"]: json.loads(row["details_json"]) for row in self.f.usage()}
        self.assertEqual(usage[state["usage_id"]]["state"], "billing_unknown")

    def test_old_window_does_not_reopen_just_recovered_transport(self):
        self.f._transport_samples(starts=20, uncertain=5, operation=OP)
        self.open()
        self.execute()
        with connect(self.f.db) as connection, transaction(connection):
            result = budget.evaluate_transport_operation_fault(connection, operation=OP,
                at="2026-08-29T04:05:01Z", planner_tick_id="new")
        self.assertIsNone(result["decision"])
        self.assertFalse(self.state()["open"])

    def test_repeated_planner_samples_do_not_reset_inflight_lease(self):
        self.f._transport_samples(starts=20, uncertain=5, operation=OP)
        self.open()
        with recovery.operation_probe_lock(db_path=self.f.db, operation=OP):
            with connect(self.f.db) as connection, transaction(connection):
                usage = sent_usage(connection, "probe")
                recovery.claim_operation_probe(connection, operation=OP, usage_id=usage,
                    identity="probe", sequence=0, at=DUE)
                budget.evaluate_transport_operation_fault(connection, operation=OP,
                    at="2026-08-29T04:05:01Z", planner_tick_id="again")
        self.assertEqual(self.state()["half_open"]["usage_id"], usage)

    def test_fixed_threshold_ignores_old_baseline_receipts(self):
        self.f._transport_samples(starts=50, uncertain=3, operation=OP)
        with connect(self.f.db) as connection, transaction(connection):
            connection.execute("""INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,details_json)
                VALUES('provider_transport_baseline:tikhub:douyin_video_detail',?,'succeeded',?,?)""",
                (AT, AT, json.dumps({"p95_transport_uncertain_rate": 0.5})))
            result = budget.evaluate_transport_operation_fault(connection, operation=OP, at=DUE, planner_tick_id="fixed")
        self.assertEqual(result["decision"], "candidate")


class LegacyUsageCooldownTest(unittest.TestCase):
    """An old Writer may settle new failures while retaining its old fault row."""
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        self.connection.row_factory = sqlite3.Row
        self.addCleanup(self.connection.close)
        self.connection.executescript('''
            CREATE TABLE provider_usage(id INTEGER PRIMARY KEY,provider TEXT,operation TEXT,
                request_attempts INTEGER,recorded_at TEXT,details_json TEXT);
            CREATE TABLE paid_provider_dispatch_events(provider_usage_id INTEGER,dispatch_id TEXT,
                sequence INTEGER,event_type TEXT,created_at TEXT,UNIQUE(dispatch_id,sequence));
            CREATE INDEX idx_settlement_send_usage ON paid_provider_dispatch_events(provider_usage_id)
                WHERE event_type='send_marked';
        ''')
        self.state = {'operation': OP, 'fault_class': 'transport',
            'last_failure_at': '2026-09-08T00:00:00Z', 'usage_id': None}

    def usage(self, *, sent='2026-09-10T01:32:27Z', finished=None,
              error='transport_error', transport=None, attempts=1, provider='TikHub', operation=OP):
        details = {'state': 'billing_unknown', 'error_code': error, 'sent_at': sent,
            'transport': transport if transport is not None else {'http_status': 200, 'clean_eof': False}}
        usage_id = self.connection.execute('INSERT INTO provider_usage(provider,operation,request_attempts,recorded_at,details_json) VALUES(?,?,?,?,?)',
            (provider, operation, attempts, sent, json.dumps(details))).lastrowid
        if finished:
            self.connection.executemany('INSERT INTO paid_provider_dispatch_events VALUES(?,?,?,?,?)', [
                (usage_id, str(usage_id), 2, 'send_marked', sent),
                (usage_id, str(usage_id), 3, 'billing_unknown', finished)])
        return usage_id

    def ready(self, at):
        return recovery._ready(self.connection, self.state, at)

    def test_old_fault_uses_real_http200_truncation_send_when_no_completion_exists(self):
        self.usage()
        before = self.connection.total_changes
        self.assertFalse(self.ready('2026-09-10T01:37:26Z'))
        self.assertTrue(self.ready('2026-09-10T01:37:27Z'))
        self.assertEqual(self.connection.total_changes, before)
        self.assertEqual(self.state['last_failure_at'], '2026-09-08T00:00:00Z')

    def test_slow_response_cools_from_terminal_completion_and_preserves_longer_backoff(self):
        self.usage(finished='2026-09-10T01:34:27Z')
        self.assertFalse(self.ready('2026-09-10T01:39:26Z'))
        self.assertTrue(self.ready('2026-09-10T01:39:27Z'))
        self.state['cooldown'] = {'failures': 3, 'retry_after': '2026-09-10T02:00:00Z'}
        self.assertFalse(self.ready('2026-09-10T01:59:59Z'))
        self.assertTrue(self.ready('2026-09-10T02:00:00Z'))

    def test_every_rate_failure_retry_after_is_retained_including_earlier_longer_header(self):
        self.state['fault_class'] = 'rate_limit'
        self.state['last_failure_at'] = '2026-09-10T01:40:00Z'
        self.usage(error='rate_limit_exceeded', finished='2026-09-10T01:34:27Z',
            transport={'http_status': 429, 'retry_after': '3600'})
        self.usage(sent='2026-09-10T01:40:00Z', error='provider_rate_limited',
            transport={'http_status': 429, 'retry_after': 'Thu, 10 Sep 2026 02:00:00 GMT'})
        self.assertFalse(self.ready('2026-09-10T02:34:26Z'))
        self.assertTrue(self.ready('2026-09-10T02:34:27Z'))

    def test_unsent_other_operation_provider_or_fault_class_cannot_extend_transport(self):
        self.usage(attempts=0)
        self.usage(provider='Other')
        self.usage(operation='douyin_user_posts')
        self.usage(error='field_contract_invalid')
        self.usage(error='provider_auth_blocked')
        self.usage(error='rate_limit_exceeded', transport={'http_status': 429, 'clean_eof': False})
        self.assertTrue(self.ready('2026-09-10T01:33:00Z'))
        self.connection.execute('DELETE FROM provider_usage')
        self.usage()
        self.state['fault_class'] = 'rate_limit'
        self.assertTrue(self.ready('2026-09-10T01:33:00Z'))

    def test_no_connection_cache_hides_new_failure_or_retains_rolled_back_failure(self):
        self.assertTrue(self.ready('2026-09-10T01:33:00Z'))
        self.usage()
        self.assertFalse(self.ready('2026-09-10T01:33:00Z'))
        self.connection.rollback()
        self.assertTrue(self.ready('2026-09-10T01:33:00Z'))

    def test_legacy_schema_and_malformed_transport_fall_back_to_valid_recorded_time(self):
        self.connection.execute('DROP TABLE paid_provider_dispatch_events')
        usage = self.usage(transport=['malformed'])
        self.connection.execute('UPDATE provider_usage SET details_json=? WHERE id=?',
            (json.dumps({'state': 'billing_unknown', 'error_code': 'transport_error',
                         'sent_at': 'bad-time', 'transport': ['malformed']}), usage))
        self.assertFalse(self.ready('2026-09-10T01:37:26Z'))
        self.assertTrue(self.ready('2026-09-10T01:37:27Z'))
