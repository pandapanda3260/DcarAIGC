from __future__ import annotations

import json
import unittest

from tests import test_v8_diagnostic_capture_boundary as capture_fixture
from v8 import operation_recovery
from v8.provider_budget import PaidScopeBlocked, check_reservation, fault_state, record_fault_state
from v8.storage import connect, transaction

AT = capture_fixture.AT


class DiagnosticBudgetBoundaryTest(unittest.TestCase):
    def setUp(self):
        self.fixture = capture_fixture.DiagnosticCaptureBoundaryTest(methodName="runTest")
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()
        self.db = self.fixture.db

    def _fault(self, scope_kind="operation", fault_class="transport", **kwargs):
        if scope_kind == "operation":
            kwargs["operation"] = "douyin_user_posts"
        with connect(self.db) as connection, transaction(connection):
            return record_fault_state(
                connection, scope_kind=scope_kind, fault_class=fault_class,
                reason="fixture", usage_id=None, at=AT, **kwargs,
            )

    def _assert_zero_reservations(self):
        self.assertEqual(self.fixture.calls, 0)
        with connect(self.db) as connection:
            for table in ("provider_usage", "paid_provider_dispatch_events", "fetch_attempts"):
                self.assertEqual(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], 0)

    def _legacy_transport(self, *, reason="transport_error", evidence_change=None):
        with connect(self.db) as connection, transaction(connection):
            slot = connection.execute(
                "INSERT INTO fetch_slots(account_id,stage,window_key,provider,adapter_version,status,"
                "attempt_count,created_at,updated_at) VALUES (?,'metrics','legacy-fixture',"
                "'TikHub','legacy','retryable_failed',1,?,?)",
                (self.fixture.candidates[0]["scope"].account_id, AT, AT),
            ).lastrowid
            connection.execute(
                "INSERT INTO fetch_attempts(slot_id,attempt_number,request_started_at,"
                "response_finished_at,error_code,error_message) VALUES (?,1,?,?,"
                "'transport_error','TikHub transport error: IncompleteRead')",
                (slot, AT, AT),
            )
            details = {"state": "billing_unknown", "slot_id": slot,
                       "attempt_number": 1, "error_code": "transport_error"}
            if evidence_change:
                details.update(evidence_change)
            usage_id = connection.execute(
                "INSERT INTO provider_usage(provider,operation,request_attempts,currency,amount,"
                "recorded_at,details_json) VALUES ('TikHub','douyin_uid_profile',1,'USD',.001,?,?)",
                (AT, json.dumps(details, sort_keys=True)),
            ).lastrowid
            connection.execute(
                "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,completed_at,details_json) "
                "VALUES ('provider_circuit:tikhub',?,'succeeded',?,?,?)",
                (AT, AT, AT, json.dumps({"contract_version": "provider-circuit-v1",
                                       "provider": "TikHub", "open": True,
                                       "reason": reason, "usage_id": usage_id})),
            )
        self.legacy_rows = self._legacy_rows()

    def _legacy_rows(self):
        with connect(self.db) as connection:
            return {
                table: tuple(connection.execute(f"SELECT * FROM {table} ORDER BY id LIMIT 1").fetchone())
                for table in ("provider_usage", "fetch_slots", "fetch_attempts")
            } | {"circuit": tuple(connection.execute(
                "SELECT * FROM scheduler_runs WHERE job_id='provider_circuit:tikhub'"
            ).fetchone())}

    def _assert_only_legacy_usage(self):
        self.assertEqual(self.fixture.calls, 0)
        self.assertEqual(self._legacy_rows(), self.legacy_rows)
        with connect(self.db) as connection:
            for table in ("provider_usage", "fetch_attempts"):
                self.assertEqual(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM paid_provider_dispatch_events").fetchone()[0], 0)

    def test_proven_legacy_transport_allows_diagnostic_only_and_preserves_history(self):
        self._legacy_transport()
        with self.fixture._context():
            self.fixture._fetch()
        self.assertEqual(self.fixture.calls, 1)
        self.assertEqual(self._legacy_rows(), self.legacy_rows)
        with connect(self.db) as connection, self.assertRaises(PaidScopeBlocked) as caught:
            check_reservation(
                connection, scope=self.fixture.candidates[1]["scope"],
                operation="douyin_user_posts", unit_price=.001, currency="USD", at=AT,
            )
        self.assertEqual(caught.exception.error_code, "provider_circuit_open")

    def test_legacy_other_reasons_cannot_use_diagnostic_exception(self):
        for reason in ("provider_balance_blocked", "provider_auth_blocked", "provider_outage",
                       "provider_global_quota", "unknown", "IncompleteRead"):
            with self.subTest(reason=reason):
                self._legacy_transport(reason=reason)
                with self.fixture._context(), self.assertRaises(PaidScopeBlocked) as caught:
                    self.fixture._fetch()
                self.assertEqual(caught.exception.error_code, "provider_circuit_open")
                self._assert_only_legacy_usage()
                # A fresh isolated fixture, never a production receipt rewrite.
                self.fixture.doCleanups()
                self.setUp()

    def test_legacy_incomplete_evidence_is_not_classified_as_transport(self):
        for change in ({"slot_id": 999999}, {"attempt_number": 2},
                       {"error_code": "unknown"}, {"state": "sent"}):
            with self.subTest(change=change):
                self._legacy_transport(evidence_change=change)
                with self.fixture._context(), self.assertRaises(PaidScopeBlocked) as caught:
                    self.fixture._fetch()
                self.assertEqual(caught.exception.error_code, "provider_circuit_open")
                self._assert_only_legacy_usage()
                self.fixture.doCleanups()
                self.setUp()

    def test_legacy_http_response_or_unfinished_attempt_stays_blocked(self):
        for assignment in ("http_status=402", "response_finished_at=NULL"):
            with self.subTest(assignment=assignment):
                self._legacy_transport()
                with connect(self.db) as connection, transaction(connection):
                    connection.execute(f"UPDATE fetch_attempts SET {assignment}")
                self.legacy_rows = self._legacy_rows()
                with self.fixture._context(), self.assertRaises(PaidScopeBlocked) as caught:
                    self.fixture._fetch()
                self.assertEqual(caught.exception.error_code, "provider_circuit_open")
                self._assert_only_legacy_usage()
                self.fixture.doCleanups()
                self.setUp()

    def test_older_provider_hard_fault_cannot_be_hidden_by_legacy_transport(self):
        self._fault(scope_kind="provider_hard", fault_class="balance")
        self._legacy_transport()
        with self.fixture._context(), self.assertRaises(PaidScopeBlocked) as caught:
            self.fixture._fetch()
        self.assertEqual(caught.exception.error_code, "provider_circuit_open")
        self._assert_only_legacy_usage()

    def test_legacy_transport_does_not_bypass_other_scopes(self):
        for scope, fault_class, error in (
            ("operation", "rate_limit", "operation_blocked"),
            ("operation", "field_contract", "operation_blocked"),
            ("authorization_hard", "authorization", "authorization_hard"),
            ("storage_hard", "storage", "storage_hard"),
        ):
            with self.subTest(scope=scope, fault_class=fault_class):
                self._legacy_transport()
                kwargs = ({"authorization_id": self.fixture.candidates[0]["scope"].identity_id}
                          if scope == "authorization_hard" else {})
                self._fault(scope_kind=scope, fault_class=fault_class, **kwargs)
                with self.fixture._context(), self.assertRaises(PaidScopeBlocked) as caught:
                    self.fixture._fetch()
                self.assertEqual(caught.exception.error_code, error)
                self._assert_only_legacy_usage()
                self.fixture.doCleanups()
                self.setUp()

    def test_new_hard_fault_at_send_boundary_releases_unsent_diagnostic(self):
        self._legacy_transport()
        with (
            self.fixture._context(),
            self.fixture._after_network_wait(lambda: self._fault(scope_kind="provider_hard", fault_class="balance")),
            self.assertRaises(PaidScopeBlocked) as caught,
        ):
            self.fixture._fetch()
        self.assertEqual(caught.exception.error_code, "provider_circuit_open")
        self.assertEqual(self.fixture.calls, 0)
        self.assertEqual(self._legacy_rows(), self.legacy_rows)
        with connect(self.db) as connection:
            usage = connection.execute("SELECT * FROM provider_usage ORDER BY id DESC LIMIT 1").fetchone()
            self.assertEqual(usage["request_attempts"], 0)
            self.assertEqual(usage["amount"], 0)
            self.assertEqual(json.loads(usage["details_json"])["state"], "not_sent")
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM fetch_attempts").fetchone()[0], 1)
            self.assertEqual(connection.execute(
                "SELECT event_type FROM paid_provider_dispatch_events ORDER BY id DESC LIMIT 1"
            ).fetchone()[0], "not_sent")

    def test_transport_fault_allows_one_member_without_clearing_fault(self):
        self._fault()
        with connect(self.db) as connection:
            opened = fault_state(connection, scope_kind="operation", operation="douyin_user_posts")
        with self.fixture._context():
            self.fixture._fetch()
        self.assertEqual(self.fixture.calls, 1)
        with connect(self.db) as connection:
            self.assertEqual(fault_state(connection, scope_kind="operation", operation="douyin_user_posts"), opened)
            # No request context means no exception, including direct budget calls.
            with self.assertRaises(PaidScopeBlocked) as caught:
                check_reservation(
                    connection, scope=self.fixture.candidates[1]["scope"],
                    operation="douyin_user_posts", unit_price=.001, currency="USD", at=AT,
                )
            self.assertEqual(caught.exception.error_code, "operation_blocked")

    def test_diagnostic_contender_refunds_and_same_unsent_member_can_retry(self):
        self._fault()
        with connect(self.db) as connection:
            opened = fault_state(connection, scope_kind="operation", operation="douyin_user_posts")
        with operation_recovery.operation_probe_lock(db_path=self.db, operation="douyin_user_posts"):
            with self.fixture._context(), self.assertRaises(PaidScopeBlocked) as caught:
                self.fixture._fetch()
        self.assertEqual(caught.exception.error_code, "operation_blocked")
        self.fixture._assert_not_sent()
        with connect(self.db) as connection:
            self.assertEqual(fault_state(connection, scope_kind="operation", operation="douyin_user_posts"), opened)
        # The diagnostic authorization remains valid before ordinary cooldown;
        # contention created no paid-send file or consumed member start.
        with self.fixture._context():
            self.fixture._fetch()
        self.assertEqual(self.fixture.calls, 1)
        with connect(self.db) as connection:
            self.assertEqual(fault_state(connection, scope_kind="operation", operation="douyin_user_posts"), opened)
            usage = connection.execute("SELECT request_attempts,amount FROM provider_usage ORDER BY id").fetchall()
            self.assertEqual([tuple(row) for row in usage], [(0, 0), (1, .001)])

    def test_diagnostic_success_keeps_operation_lock_through_response(self):
        self._fault()
        original = self.fixture._response
        observed = []
        def response():
            with connect(self.db) as connection:
                operation_recovery.require_operation_lock(connection, operation="douyin_user_posts")
            with operation_recovery.operation_probe_lock(db_path=self.db, operation="douyin_user_posts"):
                with connect(self.db) as connection, self.assertRaises(PaidScopeBlocked) as caught:
                    operation_recovery.require_operation_lock(connection, operation="douyin_user_posts")
                observed.append(caught.exception.error_code)
            return original()
        self.fixture._response = response
        with self.fixture._context():
            self.fixture._fetch()
        self.assertEqual(observed, ["operation_blocked"])
        self.assertEqual(self.fixture.calls, 1)

    def test_older_field_fault_cannot_be_hidden_by_newer_transport(self):
        self._fault(fault_class="field_contract")
        self._fault()
        with self.fixture._context(), self.assertRaises(PaidScopeBlocked) as caught:
            self.fixture._fetch()
        self.assertEqual(caught.exception.error_code, "operation_blocked")
        self._assert_zero_reservations()

    def test_rate_limit_is_not_a_transport_exception(self):
        self._fault(fault_class="rate_limit")
        with self.fixture._context(), self.assertRaises(PaidScopeBlocked) as caught:
            self.fixture._fetch()
        self.assertEqual(caught.exception.error_code, "operation_blocked")
        self._assert_zero_reservations()

    def test_provider_balance_remains_closed(self):
        self._fault(scope_kind="provider_hard", fault_class="balance")
        with self.fixture._context(), self.assertRaises(PaidScopeBlocked) as caught:
            self.fixture._fetch()
        self.assertEqual(caught.exception.error_code, "provider_circuit_open")
        self._assert_zero_reservations()

    def test_account_authorization_remains_closed(self):
        self._fault()
        self._fault(scope_kind="authorization_hard", fault_class="authorization",
                    authorization_id=self.fixture.candidates[0]["scope"].identity_id)
        with self.fixture._context(), self.assertRaises(PaidScopeBlocked) as caught:
            self.fixture._fetch()
        self.assertEqual(caught.exception.error_code, "authorization_hard")
        self._assert_zero_reservations()

    def test_storage_remains_closed(self):
        self._fault(scope_kind="storage_hard", fault_class="storage")
        with self.fixture._context(), self.assertRaises(PaidScopeBlocked) as caught:
            self.fixture._fetch()
        self.assertEqual(caught.exception.error_code, "storage_hard")
        self._assert_zero_reservations()

    def test_field_fault_while_waiting_releases_only_unsent_reservation(self):
        self._fault()
        with (
            self.fixture._context(),
            self.fixture._after_network_wait(lambda: self._fault(fault_class="field_contract")),
            self.assertRaises(PaidScopeBlocked) as caught,
        ):
            self.fixture._fetch()
        self.assertEqual(caught.exception.error_code, "operation_blocked")
        self.fixture._assert_not_sent()

    def test_transport_fault_while_waiting_still_requires_valid_member(self):
        with self.fixture._context(), self.fixture._after_network_wait(self._fault):
            self.fixture._fetch()
        self.assertEqual(self.fixture.calls, 1)
        with connect(self.db) as connection:
            self.assertTrue(fault_state(connection, scope_kind="operation", operation="douyin_user_posts")["open"])
