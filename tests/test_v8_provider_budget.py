from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import threading
import unittest
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from tests.roster_fixture import accept_roster
from tests.test_v8_fault_recovery import seed_campaign
from v8 import capture, durable_runs, providers
from v8.capture import BudgetBlocked, CaptureError, ProviderResult
from v8.paid_identity import build_paid_request_identity
from v8.paid_dispatch import dispatch_events
from v8.provider_budget import (
    AUTOMATIC_MICROUSD, BUDGET_BUCKET_MICROUSD, CATEGORY_MICROUSD,
    DEFAULT_TASK_MAX_AMOUNT_USD, GLOBAL_MICROUSD, PaidScope, _SCOPE,
    assert_paid_scope_owner,
    authorize_compensation, authorize_recovery_probe,
    budget_summary, check_reservation, consume_compensation_authorization,
    circuit_recovery_probe, circuit_state, paid_scope,
    evaluate_transport_operation_fault, fault_state, record_circuit,
    record_compensation_gap, record_fault_state,
    resolve_fault_state, resolve_operation_fault, resolve_storage_fault,
    task_budget_id,
    transport_circuit_decision,
)
from v8.storage import connect, initialize_database, transaction

AT = "2026-08-29T04:00:00Z"


class ProviderBudgetTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "budget.sqlite3"
        self.raw = Path(self.temp.name) / "raw"
        with connect(self.db) as connection:
            initialize_database(connection)
            connection.execute(
                """INSERT INTO accounts(id,phone,phone_normalized,operator_name,account_type,
                   content_direction,enabled,created_at,updated_at)
                   VALUES (1,'',NULL,'','unknown','unknown',1,?,?)""", (AT, AT),
            )
            connection.execute(
                """INSERT INTO account_platform_identities(
                   id,account_id,platform,uid,nickname,source,created_at,updated_at)
                   VALUES(1,1,'douyin','10000001','test','manual',?,?)""", (AT, AT),
            )
            for content_id in (1, 2):
                connection.execute(
                    """INSERT INTO content_items(id,link_id,platform,platform_content_id,
                       canonical_url,account_id,raw_account_uid,imported_at,created_at,updated_at)
                       VALUES(?,?,'douyin',?,'https://example.com',1,'10000001',?,?,?)""",
                    (content_id, f"C{content_id:05d}", str(content_id), AT, AT, AT),
                )
            connection.commit()
        self.calls = 0
        self.dispatch_owner = None

    def tearDown(self):
        self.temp.cleanup()

    def roster(self):
        with connect(self.db) as connection, transaction(connection):
            snapshot = accept_roster(
                connection, accepted_at="2026-08-01T00:00:00Z"
            )
        owner = durable_runs.claim_run(
            f"provider_budget_fixture:{snapshot['id']}",
            {
                "fixture": True,
                "roster_snapshot_id": snapshot["id"],
                "roster_snapshot_hash": snapshot["members_sha256"],
            },
            db_path=self.db,
            now=AT,
        )
        self.assertIsNotNone(owner)
        self.dispatch_owner = owner
        return snapshot

    @contextmanager
    def dispatch_scope(self, stage="detail"):
        current = _SCOPE.get()
        if current.scheduler_run_id is not None:
            yield
            return
        if self.dispatch_owner is None:
            yield
            return
        purpose = {
            "discovery": "reconcile",
            "detail": "detail",
            "media_source_refresh": "detail",
            "metrics": "metrics",
            "comments": "comments",
        }[stage]
        with paid_scope(
            purpose,
            scheduler_run_id=self.dispatch_owner.scheduler_run_id,
            scheduler_attempt_id=self.dispatch_owner.attempt_id,
        ):
            yield

    def budget(self, *, operation="douyin_video_detail", task="test", cap=20, price=.001):
        budget_id = task_budget_id(task, "TikHub", operation)
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                """INSERT INTO provider_budget_batches(
                   id,purpose,provider,operation,currency,verified_unit_price,
                   max_billable_requests,max_amount,pilot_size,daily_quota,price_verified_at,
                   status,created_at,updated_at)
                   VALUES(?,?,'TikHub',?,'USD',?,100000,?,0,100000,?,'approved',?,?)""",
                (budget_id, budget_id, operation, price, cap, AT, AT, AT),
            )
        return budget_id

    def call(self):
        self.calls += 1
        return ProviderResult({}, {"ok": True}, 200, True)

    def execute(self, budget_id, *, content_id=1, operation="douyin_video_detail",
                task="test", cap=20, stage="detail", call=None, window="lifetime"):
        with self.dispatch_scope(stage):
            return capture.execute_content_fetch(
                content_id=content_id, stage=stage, window_key=window, provider="TikHub",
                adapter_version="test-v1", operation=operation, budget_id=budget_id,
                task_id=task, task_max_amount=cap, db_path=self.db, raw_root=self.raw,
                call=call or self.call,
            )

    def prefill(self, amount, *, category=None, at=AT, operation="historical"):
        details = {} if category is None else {"category": category, "budget_day": "2026-08-29", "state": "completed"}
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                """INSERT INTO provider_usage(provider,operation,request_attempts,billed_requests,
                   currency,amount,recorded_at,details_json)
                   VALUES('TikHub',?,1,1,'USD',?,?,?)""",
                (operation, amount, at, json.dumps(details)),
            )

    def usage(self):
        with connect(self.db) as connection:
            return [dict(row) for row in connection.execute("SELECT * FROM provider_usage ORDER BY id")]

    def test_no_roster_means_no_network_no_usage_no_attempt(self):
        budget = self.budget()
        with self.assertRaises(BudgetBlocked) as caught:
            self.execute(budget)
        self.assertEqual(caught.exception.error_code, "roster_not_ready")
        self.assertEqual(self.calls, 0)
        with connect(self.db) as connection:
            for table in ("provider_usage", "fetch_attempts", "fetch_slots"):
                self.assertEqual(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], 0)

    def test_compensation_sequence_is_rejected_without_authorization(self):
        self.roster()
        budget = self.budget()
        identity = build_paid_request_identity(
            provider="TikHub",
            operation="douyin_video_detail",
            platform="douyin",
            subject="1",
            request_parameters={"aweme_id": "1"},
            cursor=None,
            due_bucket="lifetime",
            sequence=1,
        )
        with self.dispatch_scope("detail"), self.assertRaises(BudgetBlocked) as caught:
            capture.execute_content_fetch(
                content_id=1,
                stage="detail",
                window_key="lifetime",
                provider="TikHub",
                adapter_version="test-v1",
                operation="douyin_video_detail",
                budget_id=budget,
                task_id="test",
                task_max_amount=20,
                db_path=self.db,
                raw_root=self.raw,
                paid_request_identity=identity,
                call=self.call,
            )
        self.assertEqual(
            caught.exception.error_code,
            "compensation_authorization_required",
        )
        self.assertEqual(self.calls, 0)
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0],
                0,
            )

    def test_compensation_authorization_is_bound_and_consumed_once(self):
        self.roster()
        budget = self.budget()
        original_identity = build_paid_request_identity(
            provider="TikHub",
            operation="douyin_video_detail",
            platform="douyin",
            subject="1",
            request_parameters={"aweme_id": "1"},
            cursor=None,
            due_bucket="lifetime",
        )
        retry_identity = build_paid_request_identity(
            provider="TikHub",
            operation="douyin_video_detail",
            platform="douyin",
            subject="1",
            request_parameters={"aweme_id": "1"},
            cursor=None,
            due_bucket="lifetime",
            sequence=1,
        )
        with connect(self.db) as connection, transaction(connection):
            cursor = connection.execute(
                """INSERT INTO provider_usage(
                       provider,operation,request_attempts,billed_requests,currency,
                       amount,recorded_at,details_json)
                   VALUES ('TikHub','douyin_video_detail',1,1,'USD',.001,?,?)""",
                (
                    AT,
                    json.dumps(
                        {
                            "state": "charged_unverified",
                            "budget_day": "2026-08-29",
                            "category": "detail",
                            "paid_scope_identity": original_identity.scope_identity,
                            "paid_sequence": 0,
                        }
                    ),
                ),
            )
            original_usage_id = int(cursor.lastrowid)
            settlement = connection.execute(
                """INSERT INTO scheduler_runs(
                       job_id,scheduled_for,status,started_at,completed_at,details_json)
                   VALUES ('provider_settlement_terminal:tikhub',?,'succeeded',?,?,?)""",
                (
                    f"usage:{original_usage_id}",
                    AT,
                    AT,
                    json.dumps({"usage_id": original_usage_id}),
                ),
            )
            original_details = {
                "state": "charged_unverified",
                "budget_day": "2026-08-29",
                "category": "detail",
                "paid_scope_identity": original_identity.scope_identity,
                "paid_sequence": 0,
                "settlement_receipt_id": int(settlement.lastrowid),
            }
            connection.execute(
                "UPDATE provider_usage SET details_json=? WHERE id=?",
                (json.dumps(original_details), original_usage_id),
            )
            gap = record_compensation_gap(
                connection,
                original_usage_id=original_usage_id,
                paid_scope_identity=original_identity.scope_identity,
                operation="douyin_video_detail",
                local_replay_exhausted=True,
                raw_unrecoverable=True,
                business_gap_due=True,
                at=AT,
            )
            authorization = authorize_compensation(
                connection,
                original_usage_id=original_usage_id,
                paid_scope_identity=original_identity.scope_identity,
                operation="douyin_video_detail",
                reason="original raw is unrecoverable",
                owner="release-owner",
                gap_evidence_id=int(gap["id"]),
                expires_at="2026-08-29T05:00:00Z",
                at=AT,
            )
        with (
            paid_scope(
                "history",
                scheduler_run_id=self.dispatch_owner.scheduler_run_id,
                scheduler_attempt_id=self.dispatch_owner.attempt_id,
                compensation_authorization_id=authorization["id"],
            ),
            patch("v8.capture.now_utc", return_value=AT),
            self.assertRaises(BudgetBlocked) as sequence_zero,
        ):
            capture.execute_content_fetch(
                content_id=1,
                stage="detail",
                window_key="lifetime",
                provider="TikHub",
                adapter_version="test-v1",
                operation="douyin_video_detail",
                budget_id=budget,
                task_id="test",
                task_max_amount=20,
                db_path=self.db,
                raw_root=self.raw,
                paid_request_identity=original_identity,
                call=self.call,
            )
        self.assertEqual(
            sequence_zero.exception.error_code,
            "compensation_authorization_invalid",
        )
        self.assertEqual(self.calls, 0)
        with (
            paid_scope(
                "detail",
                scheduler_run_id=self.dispatch_owner.scheduler_run_id,
                scheduler_attempt_id=self.dispatch_owner.attempt_id,
                compensation_authorization_id=authorization["id"],
            ),
            patch("v8.capture.now_utc", return_value=AT),
        ):
            capture.execute_content_fetch(
                content_id=1,
                stage="detail",
                window_key="lifetime",
                provider="TikHub",
                adapter_version="test-v1",
                operation="douyin_video_detail",
                budget_id=budget,
                task_id="test",
                task_max_amount=20,
                db_path=self.db,
                raw_root=self.raw,
                paid_request_identity=retry_identity,
                call=self.call,
            )
        self.assertEqual(self.calls, 1)
        with connect(self.db) as connection, transaction(connection):
            with self.assertRaises(BudgetBlocked) as reused:
                consume_compensation_authorization(
                    connection,
                    authorization_id=authorization["id"],
                    paid_scope_identity=original_identity.scope_identity,
                    sequence=1,
                    operation="douyin_video_detail",
                    at=AT,
                )
        self.assertEqual(
            reused.exception.error_code, "compensation_authorization_consumed"
        )

    def test_budget_metadata_survives_settlement_and_carries_roster(self):
        snapshot = self.roster()
        budget = self.budget()
        with patch("v8.capture.now_utc", return_value=AT):
            self.execute(budget)
        row = self.usage()[0]
        details = json.loads(row["details_json"])
        self.assertEqual(row["amount"], .001)
        self.assertEqual(details["category"], "detail")
        self.assertEqual(details["budget_day"], "2026-08-29")
        self.assertEqual(details["scope"]["roster_snapshot_id"], snapshot["id"])
        self.assertEqual(details["state"], "completed")
        self.assertEqual(details["sent_at"], AT)
        with connect(self.db) as connection:
            dispatch_id = connection.execute(
                "SELECT dispatch_id FROM paid_provider_dispatch_events "
                "WHERE provider_usage_id=? LIMIT 1",
                (row["id"],),
            ).fetchone()[0]
            events = dispatch_events(connection, str(dispatch_id))
        self.assertEqual(
            [event.event_type for event in events],
            ["reserved", "send_marked", "succeeded"],
        )
        self.assertEqual(events[1].fetch_attempt_id, events[2].fetch_attempt_id)
        self.assertIsNotNone(events[2].raw_response_id)

    def test_schema19_direct_owner_freezes_business_day_and_activation(self):
        snapshot = self.roster()
        budget = self.budget()
        with (
            patch("v8.capture.now_utc", return_value=AT),
            patch("v8.provider_budget.now_utc", return_value=AT),
        ):
            capture.execute_content_fetch(
                content_id=1,
                stage="detail",
                window_key="direct-owner-success",
                provider="TikHub",
                adapter_version="test-v1",
                operation="douyin_video_detail",
                budget_id=budget,
                task_id="test",
                task_max_amount=20,
                db_path=self.db,
                raw_root=self.raw,
                call=self.call,
            )
        with connect(self.db) as connection:
            run = connection.execute(
                "SELECT id,status,details_json FROM scheduler_runs "
                "WHERE job_id='paid_capture_direct'"
            ).fetchone()
            dispatch = connection.execute(
                "SELECT activation_id,business_day,scheduler_run_id,event_type "
                "FROM paid_provider_dispatch_events ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            activation_id = connection.execute(
                "SELECT id FROM acquisition_profile_activations "
                "WHERE roster_snapshot_id=?",
                (snapshot["id"],),
            ).fetchone()[0]
        identity = json.loads(run["details_json"])["identity"]
        self.assertEqual(run["status"], "succeeded")
        self.assertEqual(identity["business_day"], "2026-08-29")
        self.assertEqual(identity["purpose"], "detail")
        self.assertEqual(identity["activation_id"], activation_id)
        self.assertEqual(identity["roster_snapshot_id"], snapshot["id"])
        self.assertEqual(dispatch["activation_id"], activation_id)
        self.assertEqual(dispatch["business_day"], "2026-08-29")
        self.assertEqual(dispatch["scheduler_run_id"], run["id"])
        self.assertEqual(dispatch["event_type"], "succeeded")

    def test_schema19_direct_owner_rejects_superseded_activation_before_network(self):
        self.roster()
        budget = self.budget()
        with (
            paid_scope("detail", activation_id=999_999),
            patch("v8.capture.now_utc", return_value=AT),
            self.assertRaises(BudgetBlocked) as caught,
        ):
            capture.execute_content_fetch(
                content_id=1,
                stage="detail",
                window_key="direct-owner-stale",
                provider="TikHub",
                adapter_version="test-v1",
                operation="douyin_video_detail",
                budget_id=budget,
                task_id="test",
                task_max_amount=20,
                db_path=self.db,
                raw_root=self.raw,
                call=self.call,
            )
        self.assertEqual(caught.exception.error_code, "profile_superseded")
        self.assertEqual(self.calls, 0)
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM scheduler_runs "
                    "WHERE job_id='paid_capture_direct'"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0],
                0,
            )

    def test_schema19_failed_direct_owner_never_remains_running(self):
        self.roster()
        budget = self.budget()

        def fail() -> ProviderResult:
            raise RuntimeError("injected direct provider failure")

        with (
            patch("v8.capture.now_utc", return_value=AT),
            patch("v8.provider_budget.now_utc", return_value=AT),
            self.assertRaises(CaptureError) as caught,
        ):
            capture.execute_content_fetch(
                content_id=1,
                stage="detail",
                window_key="direct-owner-failure",
                provider="TikHub",
                adapter_version="test-v1",
                operation="douyin_video_detail",
                budget_id=budget,
                task_id="test",
                task_max_amount=20,
                db_path=self.db,
                raw_root=self.raw,
                call=fail,
            )
        self.assertEqual(caught.exception.error_code, "unhandled_adapter_error")
        with connect(self.db) as connection:
            run = connection.execute(
                "SELECT status,details_json FROM scheduler_runs "
                "WHERE job_id='paid_capture_direct'"
            ).fetchone()
            attempt = connection.execute(
                "SELECT status FROM scheduler_run_attempts "
                "WHERE scheduler_run_id=(SELECT id FROM scheduler_runs "
                "WHERE job_id='paid_capture_direct')"
            ).fetchone()
        self.assertEqual(run["status"], "failed")
        self.assertFalse(json.loads(run["details_json"])["complete"])
        self.assertEqual(attempt["status"], "failed")

    def test_automatic_ceiling_spans_tasks_and_unknown_legacy_category(self):
        self.roster()
        first = self.budget(task="first")
        self.prefill(49.999, operation="legacy_unclassified")
        with patch("v8.capture.now_utc", return_value=AT):
            self.execute(first, task="first")
            with self.assertRaises(BudgetBlocked) as caught:
                self.execute(first, task="first", content_id=2)
        self.assertEqual(caught.exception.error_code, "automatic_budget_exhausted")
        self.assertEqual(self.calls, 1)
        with connect(self.db) as connection:
            self.assertEqual(budget_summary(connection, at=AT)["total_microusd"], 50_000_000)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM fetch_attempts").fetchone()[0], 1)

    def test_metrics_bucket_cannot_borrow_from_discovery(self):
        self.roster()
        budget = self.budget()
        self.prefill(15, category="reconcile", operation="douyin_video_detail")
        with patch("v8.capture.now_utc", return_value="2026-08-29T12:00:00Z"):
            with self.assertRaises(BudgetBlocked) as caught:
                self.execute(budget)
        self.assertEqual(caught.exception.error_code, "metrics_budget_exhausted")
        self.assertEqual(self.calls, 0)

    def test_history_context_cannot_spend_the_zero_dollar_repair_bucket(self):
        snapshot = self.roster()
        budget = self.budget()
        with paid_scope("history", roster_snapshot_id=snapshot["id"],
                        roster_snapshot_hash=snapshot["members_sha256"]):
            with paid_scope("metrics"), patch("v8.capture.now_utc", return_value=AT):
                with self.assertRaises(BudgetBlocked) as blocked:
                    self.execute(budget)
        self.assertEqual(blocked.exception.error_code, "repair_budget_exhausted")
        self.assertEqual(self.calls, 0)
        self.assertEqual(self.usage(), [])

    def test_fixed_bucket_automatic_and_absolute_ceilings(self):
        self.assertEqual(
            CATEGORY_MICROUSD,
            {
                "reconcile": 30_000_000,
                "detail": 15_000_000,
                "metrics": 15_000_000,
                "comments": 15_000_000,
                "history": 0,
            },
        )
        self.assertEqual(
            BUDGET_BUCKET_MICROUSD,
            {"discovery": 30_000_000, "metrics": 15_000_000, "repair": 0},
        )
        self.assertEqual(AUTOMATIC_MICROUSD, 50_000_000)
        self.assertEqual(GLOBAL_MICROUSD, 100_000_000)
        self.assertEqual(DEFAULT_TASK_MAX_AMOUNT_USD, 100.0)
        self.roster()
        budget = self.budget()
        self.prefill(100, category="history")
        with paid_scope("history"), patch("v8.capture.now_utc", return_value=AT):
            with self.assertRaises(BudgetBlocked) as caught:
                self.execute(budget)
        self.assertEqual(caught.exception.error_code, "global_budget_exhausted")
        self.assertEqual(self.calls, 0)

    def test_historical_incident_cannot_raise_current_budget_limits(self):
        proof = {
            "contract_version": "provider-budget-incident-v1", "approval_ref": "historical-incident",
            "owner": "budget-owner", "provider": "tikhub", "business_day": "2026-08-29",
            "bucket": "metrics", "approved_total_microusd": 60_000_000,
            "approved_bucket_microusd": 16_000_000, "authorized_at": AT,
            "expires_at": "2026-08-29T15:59:59Z",
        }
        with connect(self.db) as connection, transaction(connection):
            authorization_id = connection.execute(
                """INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,completed_at,details_json)
                   VALUES ('provider_budget_incident:tikhub',?,'succeeded',?,?,?)""",
                (AT, AT, AT, json.dumps(proof)),
            ).lastrowid
        # Old frozen proofs remain readable, but the ID is neither transmitted
        # by new scopes nor consulted by the current capacity decision.
        scope = PaidScope(category="metrics", incident_authorization_id=authorization_id)
        token = _SCOPE.set(scope)
        try:
            with paid_scope("metrics") as current:
                self.assertIsNone(current.incident_authorization_id)
        finally:
            _SCOPE.reset(token)
        with connect(self.db) as connection:
            approved = check_reservation(connection, scope=scope,
                operation="douyin_video_statistics", unit_price=.001, currency="USD", at=AT)
        self.assertIsNone(approved["scope"]["incident_authorization_id"])
        for total, category, operation, code in (
            (15, "metrics", "douyin_video_statistics", "metrics_budget_exhausted"),
            (50, "legacy", "legacy_unclassified", "automatic_budget_exhausted"),
            (100, "legacy", "legacy_unclassified", "global_budget_exhausted"),
        ):
            with self.subTest(code=code):
                with connect(self.db) as connection, transaction(connection):
                    connection.execute("DELETE FROM provider_usage")
                self.prefill(total, category=category, operation=operation)
                with connect(self.db) as connection, self.assertRaises(BudgetBlocked) as caught:
                    check_reservation(connection, scope=scope,
                        operation="douyin_video_statistics", unit_price=.001, currency="USD", at=AT)
                self.assertEqual(caught.exception.error_code, code)
        self.assertEqual(self.calls, 0)

    def test_discovery_and_metrics_aggregate_buckets_are_hard_gates(self):
        self.prefill(
            30,
            category="reconcile",
            operation="douyin_user_posts",
        )
        with connect(self.db) as connection:
            with self.assertRaises(BudgetBlocked) as discovery:
                check_reservation(
                    connection,
                    scope=PaidScope(category="reconcile"),
                    operation="douyin_user_posts",
                    unit_price=.001,
                    currency="USD",
                    at=AT,
                )
        self.assertEqual(discovery.exception.error_code, "discovery_budget_exhausted")

        with connect(self.db) as connection, transaction(connection):
            connection.execute("DELETE FROM provider_usage")
        self.prefill(
            15,
            category="comments",
            operation="xiaohongshu_note_comments",
        )
        with connect(self.db) as connection:
            with self.assertRaises(BudgetBlocked) as metrics:
                check_reservation(
                    connection,
                    scope=PaidScope(category="metrics"),
                    operation="douyin_video_statistics",
                    unit_price=.001,
                    currency="USD",
                    at=AT,
                )
        self.assertEqual(metrics.exception.error_code, "metrics_budget_exhausted")

    def test_legacy_history_and_compensation_charge_the_operation_bucket(self):
        self.prefill(
            1,
            category="history",
            operation="douyin_user_posts",
        )
        with connect(self.db) as connection, transaction(connection):
            details = json.loads(
                connection.execute(
                    "SELECT details_json FROM provider_usage ORDER BY id DESC LIMIT 1"
                ).fetchone()[0]
            )
            details["paid_sequence"] = 1
            connection.execute(
                "UPDATE provider_usage SET details_json=?",
                (json.dumps(details),),
            )
        with connect(self.db) as connection:
            summary = budget_summary(connection, at=AT)
        self.assertEqual(summary["buckets_microusd"]["discovery"], 1_000_000)
        self.assertEqual(summary["buckets_microusd"]["repair"], 0)

    def test_transport_threshold_requires_sufficient_sample(self):
        self.assertIsNone(transport_circuit_decision(starts=1, uncertain=1))
        self.assertIsNone(transport_circuit_decision(starts=19, uncertain=19))
        self.assertEqual(
            transport_circuit_decision(starts=20, uncertain=4), "immediate"
        )
        self.assertEqual(
            transport_circuit_decision(starts=50, uncertain=3), "candidate"
        )

    def _transport_samples(self, *, starts, uncertain, operation):
        with connect(self.db) as connection, transaction(connection):
            for index in range(starts):
                connection.execute(
                    """INSERT INTO provider_usage(
                           provider,operation,request_attempts,billed_requests,
                           currency,amount,recorded_at,details_json)
                       VALUES ('TikHub',?,1,1,'USD',.001,?,?)""",
                    (
                        operation,
                        AT,
                        json.dumps(
                            {
                                "state": (
                                    "billing_unknown"
                                    if index < uncertain
                                    else "completed"
                                ),
                                "sent_at": AT,
                                "error_code": (
                                    "transport_error" if index < uncertain else None
                                ),
                            }
                        ),
                    ),
                )

    def test_transport_ratio_opens_on_immediate_and_two_tick_production_paths(self):
        self._transport_samples(
            starts=20, uncertain=4, operation="douyin_video_detail"
        )
        with connect(self.db) as connection, transaction(connection):
            immediate = evaluate_transport_operation_fault(
                connection,
                operation="douyin_video_detail",
                at="2026-08-29T04:01:00Z",
            )
        self.assertEqual(immediate["decision"], "immediate")
        with connect(self.db) as connection:
            self.assertTrue(
                fault_state(
                    connection,
                    scope_kind="operation",
                    operation="douyin_video_detail",
                    fault_class="transport",
                )["open"]
            )

        self._transport_samples(
            starts=50, uncertain=3, operation="douyin_video_statistics"
        )
        with connect(self.db) as connection, transaction(connection):
            first = evaluate_transport_operation_fault(
                connection,
                operation="douyin_video_statistics",
                at="2026-08-29T04:01:00Z",
                planner_tick_id="tick-1",
            )
            repeated = evaluate_transport_operation_fault(
                connection,
                operation="douyin_video_statistics",
                at="2026-08-29T04:01:30Z",
                planner_tick_id="tick-1",
            )
            second = evaluate_transport_operation_fault(
                connection,
                operation="douyin_video_statistics",
                at="2026-08-29T04:02:00Z",
                planner_tick_id="tick-2",
            )
        self.assertFalse(first["consecutive_candidate"])
        self.assertFalse(repeated["consecutive_candidate"])
        self.assertTrue(second["consecutive_candidate"])
        self.assertIsNotNone(second["fault"])

    def test_provider_hard_fingerprint_is_idempotent_and_probe_is_domain_bound(self):
        with connect(self.db) as connection, transaction(connection):
            first_receipt = record_circuit(
                connection,
                reason="provider_balance_blocked",
                usage_id=1,
                at=AT,
                state_evidence={"balance_snapshot": "low-a"},
            )
            first = dict(fault_state(connection, scope_kind="provider_hard"))
            record_circuit(
                connection,
                reason="provider_balance_blocked",
                usage_id=2,
                at="2026-08-29T04:01:00Z",
                state_evidence={"balance_snapshot": "low-a"},
            )
            second = dict(fault_state(connection, scope_kind="provider_hard"))
            changed_receipt = record_circuit(
                connection,
                reason="provider_balance_blocked",
                usage_id=3,
                at="2026-08-29T04:02:00Z",
                state_evidence={"balance_snapshot": "funded-b"},
            )
            changed = dict(fault_state(connection, scope_kind="provider_hard"))
            with self.assertRaises(BudgetBlocked) as storage_probe:
                authorize_recovery_probe(
                    connection,
                    authorization_ref="operator",
                    operation="douyin_video_detail",
                    at=AT,
                    scope_kind="storage_hard",
                )
        self.assertEqual(first["generation"], second["generation"])
        self.assertEqual(first["state_fingerprint"], second["state_fingerprint"])
        self.assertEqual(first_receipt["id"], second["receipt_id"])
        self.assertNotEqual(first["generation"], changed["generation"])
        self.assertEqual(changed_receipt["state_evidence"]["balance_snapshot"], "funded-b")
        self.assertEqual(
            storage_probe.exception.error_code,
            "recovery_fault_domain_not_probeable",
        )

    def test_v2_fault_events_preserve_legacy_circuit_and_fault_classes(self):
        legacy = {
            "contract_version": "provider-circuit-v1",
            "provider": "TikHub",
            "open": False,
            "generation": "legacy-generation",
            "reason": "unhandled_adapter_error",
        }
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                """INSERT INTO scheduler_runs(
                       job_id,scheduled_for,status,started_at,completed_at,details_json)
                   VALUES ('provider_circuit:tikhub','1970-01-01T00:00:00Z',
                           'succeeded',?,?,?)""",
                (AT, AT, json.dumps(legacy, sort_keys=True)),
            )
            record_circuit(
                connection,
                reason="provider_balance_blocked",
                usage_id=1,
                at=AT,
                state_evidence={"balance_snapshot": "low"},
            )
            record_fault_state(
                connection,
                scope_kind="operation",
                operation="douyin_video_detail",
                fault_class="rate_limit",
                reason="http_429",
                usage_id=2,
                at=AT,
                state_evidence={"quota_window": "window-1"},
            )
            record_fault_state(
                connection,
                scope_kind="operation",
                operation="douyin_video_detail",
                fault_class="field_contract",
                reason="field_contract_invalid",
                usage_id=3,
                at=AT,
                state_evidence={"contract_hash": "contract-1"},
            )
            rate = fault_state(
                connection,
                scope_kind="operation",
                operation="douyin_video_detail",
                fault_class="rate_limit",
            )
            field = fault_state(
                connection,
                scope_kind="operation",
                operation="douyin_video_detail",
                fault_class="field_contract",
            )
            with self.assertRaisesRegex(ValueError, "typed recovery"):
                resolve_fault_state(
                    connection,
                    scope_kind="operation",
                    operation="douyin_video_detail",
                    fault_class="rate_limit",
                    expected_generation=rate["generation"],
                    expected_fingerprint=rate["state_fingerprint"],
                    evidence_id="unverified-HTTP-success",
                    at=AT,
                )
            self.raw.mkdir(mode=0o700, exist_ok=True)
            recovery_evidence = seed_campaign(connection, self.raw, rate, transport=False)
            recovered = resolve_operation_fault(
                connection,
                operation="douyin_video_detail",
                fault_class="rate_limit",
                recovery_evidence=recovery_evidence,
                at="2026-08-29T05:00:00Z",
            )
            legacy_after = json.loads(
                connection.execute(
                    """SELECT details_json FROM scheduler_runs
                       WHERE job_id='provider_circuit:tikhub'"""
                ).fetchone()[0]
            )
            field_after = fault_state(
                connection,
                scope_kind="operation",
                operation="douyin_video_detail",
                fault_class="field_contract",
            )
        self.assertEqual(legacy_after, legacy)
        self.assertTrue(recovered["fault_closed"])
        self.assertTrue(field["open"])
        self.assertTrue(field_after["open"])

    def test_legacy_open_circuit_is_not_masked_and_is_bound_before_probe(self):
        legacy = {
            "contract_version": "provider-circuit-v1",
            "provider": "TikHub",
            "open": True,
            "generation": "legacy-open-generation",
            "reason": "provider_balance_blocked",
            "opened_at": AT,
            "last_failure_at": AT,
        }
        with connect(self.db) as connection, transaction(connection):
            legacy_row = connection.execute(
                """INSERT INTO scheduler_runs(
                       job_id,scheduled_for,status,started_at,completed_at,details_json)
                   VALUES ('provider_circuit:tikhub','1970-01-01T00:00:00Z',
                           'partial',?,?,?)""",
                (AT, AT, json.dumps(legacy, sort_keys=True)),
            )
            application_fault = record_fault_state(
                connection,
                scope_kind="provider_hard",
                fault_class="application_auth",
                reason="provider_auth_blocked",
                usage_id=None,
                at="2026-08-29T04:01:00Z",
                state_evidence={"credential_fingerprint": "credential-a"},
            )
            app_row = connection.execute(
                "SELECT job_id,details_json FROM scheduler_runs WHERE id=?",
                (application_fault["id"],),
            ).fetchone()
            app_closed = json.loads(app_row["details_json"])
            app_closed.update(
                open=False,
                recovered_at="2026-08-29T04:02:00Z",
                recovery_evidence_id="fixture",
            )
            connection.execute(
                """INSERT INTO scheduler_runs(
                       job_id,scheduled_for,status,started_at,completed_at,details_json)
                   VALUES (?,'closed-app','succeeded',?,?,?)""",
                (
                    app_row["job_id"],
                    "2026-08-29T04:02:00Z",
                    "2026-08-29T04:02:00Z",
                    json.dumps(app_closed, sort_keys=True),
                ),
            )
            projected = circuit_state(connection)
            probe = authorize_recovery_probe(
                connection,
                authorization_ref="budget-owner:recharge-receipt",
                operation="douyin_video_statistics",
                at="2026-08-29T04:03:00Z",
            )
            bound = fault_state(
                connection,
                scope_kind="provider_hard",
                fault_class="balance",
            )
            stored_legacy = json.loads(
                connection.execute(
                    "SELECT details_json FROM scheduler_runs WHERE id=?",
                    (int(legacy_row.lastrowid),),
                ).fetchone()[0]
            )
        self.assertEqual(
            projected["contract_version"], "provider-fault-v2-legacy-pending"
        )
        self.assertEqual(probe["circuit_generation"], bound["generation"])
        self.assertEqual(
            bound["state_evidence"]["legacy_circuit_receipt_id"],
            int(legacy_row.lastrowid),
        )
        self.assertEqual(stored_legacy, legacy)

    def test_storage_operation_and_authorization_faults_gate_only_their_scope(self):
        with connect(self.db) as connection, transaction(connection):
            record_fault_state(
                connection,
                scope_kind="operation",
                operation="douyin_video_statistics",
                fault_class="rate_limit",
                reason="http_429",
                usage_id=None,
                at=AT,
            )
        with connect(self.db) as connection:
            with self.assertRaises(BudgetBlocked) as operation:
                check_reservation(
                    connection,
                    scope=PaidScope(category="metrics", identity_id=1),
                    operation="douyin_video_statistics",
                    unit_price=.001,
                    currency="USD",
                    at=AT,
                )
            unrelated = check_reservation(
                connection,
                scope=PaidScope(category="metrics", identity_id=1),
                operation="douyin_video_detail",
                unit_price=.001,
                currency="USD",
                at=AT,
            )
        self.assertEqual(operation.exception.error_code, "operation_blocked")
        self.assertEqual(unrelated["budget_bucket"], "metrics")

        with connect(self.db) as connection, transaction(connection):
            record_fault_state(
                connection,
                scope_kind="authorization_hard",
                authorization_id=1,
                fault_class="account_authorization",
                reason="authorization_token_invalid",
                usage_id=None,
                at=AT,
            )
        with connect(self.db) as connection:
            with self.assertRaises(BudgetBlocked) as authorization:
                check_reservation(
                    connection,
                    scope=PaidScope(category="metrics", identity_id=1),
                    operation="douyin_video_detail",
                    unit_price=.001,
                    currency="USD",
                    at=AT,
                )
        self.assertEqual(authorization.exception.error_code, "authorization_hard")

        with connect(self.db) as connection, transaction(connection):
            record_fault_state(
                connection,
                scope_kind="storage_hard",
                fault_class="local_evidence_store",
                reason="storage_hard",
                usage_id=None,
                at=AT,
            )
        with connect(self.db) as connection:
            with self.assertRaises(BudgetBlocked) as storage:
                check_reservation(
                    connection,
                    scope=PaidScope(category="metrics", identity_id=2),
                    operation="douyin_video_detail",
                    unit_price=.001,
                    currency="USD",
                    at=AT,
                )
        self.assertEqual(storage.exception.error_code, "storage_hard")
        with connect(self.db) as connection, transaction(connection):
            self.raw.mkdir(mode=0o700, exist_ok=True)
            current_storage = fault_state(connection, scope_kind="storage_hard", provider="all")
            capacity = connection.execute(
                """INSERT INTO scheduler_runs(
                       job_id,scheduled_for,status,started_at,completed_at,details_json)
                   VALUES ('storage_capacity','capacity-1','succeeded',?,?,?)""",
                (
                    AT,
                    AT,
                    json.dumps(
                        {
                            "contract_version": "storage_capacity_receipt_v1",
                            "admitted": True,
                            "raw_root": str(self.raw),
                            "issued_at": AT,
                            "expires_at": "2026-08-29T06:00:00Z",
                        }
                    ),
                ),
            )
            recovered = resolve_storage_fault(
                connection,
                recovery_evidence={
                    "contract_version": "storage-recovery-v1",
                    "idempotent_write_passed": True,
                    "file_fsync_passed": True,
                    "directory_fsync_passed": True,
                    "hash_readback_passed": True,
                    "capacity_receipt_id": int(capacity.lastrowid),
                    "fault_generation": current_storage["generation"],
                    "fault_fingerprint": current_storage["state_fingerprint"],
                },
                at="2026-08-29T04:05:00Z",
            )
        self.assertTrue(recovered["fault_closed"])
        with connect(self.db) as connection:
            self.assertFalse(
                fault_state(
                    connection,
                    scope_kind="storage_hard",
                    provider="all",
                    fault_class="local_evidence_store",
                )["open"]
            )

    def test_v2_task_budget_can_coexist_with_legacy_v1_purpose(self):
        task_id = "legacy-v1-budget"
        operation = "douyin_video_detail"
        digest = hashlib.sha256(task_id.encode()).hexdigest()[:16]
        legacy_id = f"task-{digest}-tikhub-{operation}-v1"
        legacy_purpose = f"task_{digest}_{operation}"
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                """
                INSERT INTO provider_budget_batches(
                  id,purpose,provider,operation,currency,verified_unit_price,
                  max_billable_requests,max_amount,pilot_size,daily_quota,
                  price_verified_at,status,created_at,updated_at
                ) VALUES(?,?, 'TikHub',?,'USD',0.001,30000,30,0,30000,
                  ?,'approved',?,?)
                """,
                (legacy_id, legacy_purpose, operation, AT, AT, AT),
            )

        current_id = providers.ensure_task_budget(
            provider="TikHub",
            operation=operation,
            price=0.001,
            task_id=task_id,
            task_max_amount=50.0,
            db_path=self.db,
        )
        with connect(self.db) as connection:
            rows = connection.execute(
                """
                SELECT id,purpose,max_amount FROM provider_budget_batches
                WHERE id IN (?,?) ORDER BY id
                """,
                (legacy_id, current_id),
            ).fetchall()
        self.assertEqual(current_id, f"task-{digest}-tikhub-{operation}-v2")
        self.assertEqual(len(rows), 2)
        self.assertEqual(
            {str(row["purpose"]) for row in rows},
            {legacy_purpose, f"{legacy_purpose}_v2"},
        )
        self.assertEqual(
            {float(row["max_amount"]) for row in rows},
            {30.0, 50.0},
        )

    def test_unknown_price_is_closed_before_attempt(self):
        self.roster()
        budget = self.budget(price=.008)
        with self.assertRaises(BudgetBlocked) as caught:
            self.execute(budget)
        self.assertEqual(caught.exception.error_code, "price_contract_unverified")
        self.assertEqual(self.calls, 0)
        self.assertEqual(self.usage(), [])

    def test_rate_code_without_http_429_uses_only_operation_rate_fault(self):
        self.roster()
        budget = self.budget()
        with patch("v8.capture.now_utc", return_value=AT):
            with self.assertRaises(CaptureError):
                self.execute(
                    budget,
                    call=lambda: (_ for _ in ()).throw(
                        CaptureError(
                            "limited", retryable=True,
                            error_code="provider_rate_limited", billed=False,
                            http_status=200, retry_after_seconds=120,
                        )
                    ),
                )
            with self.assertRaises(BudgetBlocked):
                self.execute(budget, content_id=2)
        self.assertEqual(self.calls, 0)
        self.assertEqual(len(self.usage()), 1)
        with connect(self.db) as connection:
            fault = fault_state(
                connection, scope_kind="operation", operation="douyin_video_detail"
            )
            self.assertEqual(fault["fault_class"], "rate_limit")
            self.assertTrue(fault["open"])
            self.assertIsNone(circuit_state(connection))

    def test_transport_unknown_keeps_money_and_retry_after(self):
        self.roster()
        budget = self.budget()
        with patch("v8.capture.now_utc", return_value=AT):
            with self.assertRaises(CaptureError):
                self.execute(budget, call=lambda: (_ for _ in ()).throw(
                    CaptureError(
                        "timeout", retryable=True, error_code="transport_error",
                        billed=None, retry_after_seconds=120,
                    )
                ))
        usage = self.usage()[0]
        self.assertEqual(usage["amount"], .001)
        details = json.loads(usage["details_json"])
        self.assertEqual(details["state"], "billing_unknown")
        self.assertEqual(details["retry_after_seconds"], 120)

    def test_single_transport_billing_unknown_does_not_open_provider_circuit(self):
        self.roster()
        budget = self.budget()
        with patch("v8.capture.now_utc", return_value=AT):
            with self.assertRaises(CaptureError):
                self.execute(
                    budget,
                    call=lambda: (_ for _ in ()).throw(
                        CaptureError(
                            "timeout",
                            retryable=True,
                            error_code="transport_error",
                            billed=None,
                        )
                    ),
                )
            self.execute(budget, content_id=2)
        self.assertEqual(self.calls, 1)
        self.assertEqual(len(self.usage()), 2)
        with connect(self.db) as connection:
            circuit = budget_summary(connection, at=AT)["circuit"]
            identity_hold = fault_state(
                connection,
                scope_kind="paid_identity_hold",
                paid_identity=json.loads(self.usage()[0]["details_json"])[
                    "paid_scope_identity"
                ],
            )
        self.assertIsNone(circuit)
        self.assertTrue(identity_hold["open"])

    def test_transport_billing_unknown_does_not_stop_other_reserved_slot(self):
        self.roster()
        budget = self.budget()
        with patch("v8.capture.now_utc", return_value=AT):
            first = self.claim_only(budget, content_id=1)
            second = self.claim_only(budget, content_id=2)
            with self.assertRaises(CaptureError):
                capture._execute_claimed_fetch(
                    claim=first,
                    operation="douyin_video_detail",
                    call=lambda: (_ for _ in ()).throw(
                        CaptureError(
                            "timeout",
                            retryable=True,
                            error_code="transport_error",
                            billed=None,
                        )
                    ),
                    db_path=self.db,
                    raw_root=self.raw,
                    budget_id=budget,
                    task_id="test",
                    task_max_amount=20,
                )
            self.send_claim(second, budget)
        self.assertEqual(self.calls, 1)
        usage = self.usage()
        self.assertEqual(
            [json.loads(row["details_json"])["state"] for row in usage],
            ["billing_unknown", "completed"],
        )
        self.assertEqual(
            [(row["request_attempts"], row["amount"]) for row in usage],
            [(1, 0.001), (1, 0.001)],
        )

    def test_transport_error_cannot_claim_unbilled_before_reconciliation(self):
        self.roster()
        budget = self.budget()
        with patch("v8.capture.now_utc", return_value=AT):
            with self.assertRaises(CaptureError):
                self.execute(
                    budget,
                    call=lambda: (_ for _ in ()).throw(
                        CaptureError(
                            "connection reset",
                            retryable=True,
                            error_code="transport_error",
                            billed=False,
                        )
                    ),
                )
        with connect(self.db) as connection:
            usage = connection.execute("SELECT * FROM provider_usage").fetchone()
            attempt = connection.execute(
                "SELECT billed,amount FROM fetch_attempts"
            ).fetchone()
            self.assertEqual(json.loads(usage["details_json"])["state"], "billing_unknown")
            self.assertEqual((usage["billed_requests"], usage["amount"]), (1, .001))
            self.assertEqual((attempt["billed"], attempt["amount"]), (0, None))

    def test_non_tikhub_failure_is_not_mislabeled_as_billing_unknown(self):
        with patch("v8.capture.now_utc", return_value=AT):
            with self.assertRaises(CaptureError):
                capture.execute_content_fetch(
                    content_id=1,
                    stage="detail",
                    window_key="lifetime",
                    provider="Matrix",
                    adapter_version="fixture-v1",
                    operation="matrix_detail",
                    db_path=self.db,
                    raw_root=self.raw,
                    call=lambda: (_ for _ in ()).throw(
                        CaptureError(
                            "derived failure",
                            retryable=True,
                            error_code="derived_failure",
                        )
                    ),
                )
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT last_error_code FROM fetch_slots"
                ).fetchone()[0],
                "derived_failure",
            )

    def test_billing_unknown_slot_cannot_reserve_or_send_again(self):
        self.roster()
        budget = self.budget()
        with patch("v8.capture.now_utc", return_value=AT):
            with self.assertRaises(CaptureError):
                self.execute(budget, call=lambda: (_ for _ in ()).throw(
                    CaptureError(
                        "connection closed",
                        retryable=True,
                        error_code="transport_error",
                        billed=None,
                    )
                ))
        before = self.usage()
        with patch("v8.capture.now_utc", return_value=AT):
            with self.assertRaises(BudgetBlocked) as caught:
                self.execute(
                    budget,
                    call=lambda: self.fail("billing-unknown retry reached the network"),
                )
        self.assertEqual(caught.exception.error_code, "billing_unknown_retry_blocked")
        self.assertEqual(self.usage(), before)
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM fetch_attempts").fetchone()[0],
                1,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT last_error_code FROM fetch_slots"
                ).fetchone()[0],
                capture.BILLING_UNKNOWN_SLOT_ERROR,
            )
            summary = budget_summary(connection, at=AT)
            self.assertEqual(
                summary["billing_unknown"],
                {
                    "unresolved_count": 1,
                    "unresolved_microusd": 1000,
                    "budget_day_count": 1,
                    "budget_day_microusd": 1000,
                },
            )

    def test_derived_failure_cannot_erase_existing_unknown_guard(self):
        self.roster()
        budget = self.budget()
        with patch("v8.capture.now_utc", return_value=AT):
            with self.assertRaises(CaptureError):
                self.execute(
                    budget,
                    call=lambda: (_ for _ in ()).throw(
                        CaptureError(
                            "connection closed",
                            retryable=True,
                            error_code="transport_error",
                            billed=None,
                        )
                    ),
                )
            with self.assertRaises(CaptureError):
                capture.execute_content_fetch(
                    content_id=1,
                    stage="detail",
                    window_key="lifetime",
                    provider="Matrix",
                    adapter_version="fixture-v1",
                    operation="matrix_detail",
                    db_path=self.db,
                    raw_root=self.raw,
                    call=lambda: (_ for _ in ()).throw(
                        CaptureError(
                            "derived failure",
                            retryable=True,
                            error_code="derived_failure",
                        )
                    ),
                )
            with self.assertRaises(BudgetBlocked) as caught:
                self.execute(
                    budget,
                    call=lambda: self.fail("derived path erased the billing guard"),
                )
        self.assertEqual(
            caught.exception.error_code, capture.BILLING_UNKNOWN_SLOT_ERROR
        )

    def test_any_unresolved_attempt_blocks_even_after_a_newer_legacy_usage(self):
        self.roster()
        budget = self.budget()
        with patch("v8.capture.now_utc", return_value=AT):
            with self.assertRaises(CaptureError):
                self.execute(
                    budget,
                    call=lambda: (_ for _ in ()).throw(
                        CaptureError(
                            "connection closed", retryable=True,
                            error_code="transport_error", billed=None,
                        )
                    ),
                )
        with connect(self.db) as connection, transaction(connection):
            slot_id = connection.execute("SELECT id FROM fetch_slots").fetchone()[0]
            connection.execute(
                """INSERT INTO fetch_attempts(
                       slot_id,attempt_number,request_started_at,response_finished_at,
                       http_status,billed,amount,currency)
                   VALUES (?,2,?,?,200,1,.001,'USD')""",
                (slot_id, AT, AT),
            )
            connection.execute(
                "UPDATE fetch_slots SET status='retryable_failed',attempt_count=2 WHERE id=?",
                (slot_id,),
            )
            connection.execute(
                """INSERT INTO provider_usage(
                       task_id,budget_batch_id,provider,operation,request_attempts,
                       billed_requests,currency,amount,recorded_at,details_json)
                   VALUES ('legacy',?,'TikHub','douyin_video_detail',1,1,'USD',.001,?,?)""",
                (
                    budget,
                    AT,
                    json.dumps(
                        {
                            "state": "completed", "slot_id": slot_id,
                            "attempt_number": 2, "budget_day": "2026-08-29",
                            "category": "detail",
                        }
                    ),
                ),
            )
        with patch("v8.capture.now_utc", return_value=AT):
            with self.assertRaises(BudgetBlocked) as caught:
                self.execute(
                    budget,
                    call=lambda: self.fail("unresolved legacy attempt reached the network"),
                )
        self.assertEqual(caught.exception.error_code, "billing_unknown_retry_blocked")

    def test_null_attempt_amount_without_unknown_usage_does_not_block(self):
        self.roster()
        budget = self.budget()
        with connect(self.db) as connection, transaction(connection):
            slot_id = capture.ensure_content_slot(
                connection,
                content_id=1,
                stage="detail",
                window_key="lifetime",
                provider="TikHub",
                adapter_version="test-v1",
            )
            connection.execute(
                """INSERT INTO fetch_attempts(
                       slot_id,attempt_number,request_started_at,response_finished_at,
                       billed,amount,error_code,error_message)
                   VALUES (?,1,?,?,0,NULL,'budget_blocked','blocked before provider send')""",
                (slot_id, AT, AT),
            )
            connection.execute(
                """UPDATE fetch_slots SET status='retryable_failed',attempt_count=1,
                       last_error_code='budget_blocked',
                       last_error_message='blocked before provider send' WHERE id=?""",
                (slot_id,),
            )
        with patch("v8.capture.now_utc", return_value=AT):
            self.execute(budget)
        self.assertEqual(self.calls, 1)

    def test_startup_materializes_existing_unknown_usage_guard(self):
        self.roster()
        budget = self.budget()
        with patch("v8.capture.now_utc", return_value=AT):
            with self.assertRaises(CaptureError):
                self.execute(
                    budget,
                    call=lambda: (_ for _ in ()).throw(
                        CaptureError(
                            "connection closed", retryable=True,
                            error_code="transport_error", billed=None,
                        )
                    ),
                )
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                """UPDATE fetch_slots SET last_error_code='transport_error',
                       last_error_message='legacy state without materialized guard'"""
            )
        capture.recover_stale_fetch_slots(
            db_path=self.db,
            current_time=datetime(2026, 8, 29, 4, 5, tzinfo=timezone.utc),
        )
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT last_error_code FROM fetch_slots"
                ).fetchone()[0],
                capture.BILLING_UNKNOWN_SLOT_ERROR,
            )
            receipt = connection.execute(
                """SELECT a.id FROM scheduler_run_attempts a
                   JOIN scheduler_runs r ON r.id=a.scheduler_run_id
                   WHERE r.job_id='billing_unknown_guard_sync:tikhub'"""
            ).fetchone()
            self.assertIsNotNone(receipt)
            with self.assertRaisesRegex(
                sqlite3.IntegrityError, "one running-to-terminal"
            ):
                connection.execute(
                    "UPDATE scheduler_run_attempts SET details_json='{}' WHERE id=?",
                    (receipt["id"],),
                )

    def test_startup_guards_succeeded_unknown_before_derived_reopen(self):
        self.roster()
        budget = self.budget()
        with patch("v8.capture.now_utc", return_value=AT):
            with self.assertRaises(CaptureError):
                self.execute(
                    budget,
                    call=lambda: (_ for _ in ()).throw(
                        CaptureError(
                            "connection closed",
                            retryable=True,
                            error_code="transport_error",
                            billed=None,
                        )
                    ),
                )
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                """UPDATE fetch_slots SET status='succeeded',last_error_code=NULL,
                       last_error_message=NULL WHERE content_id=1"""
            )
        capture.recover_stale_fetch_slots(
            db_path=self.db,
            current_time=datetime(2026, 8, 29, 4, 5, tzinfo=timezone.utc),
        )
        with connect(self.db) as connection:
            slot = connection.execute(
                "SELECT id,status,last_error_code FROM fetch_slots"
            ).fetchone()
            self.assertEqual(slot["status"], "succeeded")
            self.assertEqual(
                slot["last_error_code"], capture.BILLING_UNKNOWN_SLOT_ERROR
            )
            slot_id = int(slot["id"])
        capture.mark_succeeded_fetch_slot_retryable_failure(
            db_path=self.db,
            slot_id=slot_id,
            error_code="derived_materialization_failed",
            error_message="fixture",
        )
        with patch("v8.capture.now_utc", return_value=AT):
            with self.assertRaises(BudgetBlocked) as caught:
                self.execute(
                    budget,
                    call=lambda: self.fail("reopened succeeded slot bypassed guard"),
                )
        self.assertEqual(
            caught.exception.error_code, capture.BILLING_UNKNOWN_SLOT_ERROR
        )

    def test_confirmed_unbilled_releases_capacity(self):
        self.roster()
        budget = self.budget(cap=.001)
        with patch("v8.capture.now_utc", return_value=AT):
            first = self.execute(budget, cap=.001, call=lambda: ProviderResult({}, {}, 200, False))
            second = self.execute(budget, cap=.001, content_id=2)
        self.assertFalse(first.billed)
        self.assertTrue(second.billed)
        self.assertEqual([row["amount"] for row in self.usage()], [0, .001])

    def test_402_persists_circuit_across_new_calls(self):
        self.roster()
        budget = self.budget()
        with patch("v8.capture.now_utc", return_value=AT):
            with self.assertRaises(CaptureError):
                self.execute(budget, call=lambda: (_ for _ in ()).throw(
                    CaptureError("payment", retryable=True, error_code="provider_balance_blocked",
                                 billed=False, http_status=402)
                ))
            with self.assertRaises(BudgetBlocked) as caught:
                self.execute(budget, content_id=2)
        self.assertEqual(caught.exception.error_code, "provider_circuit_open")
        self.assertEqual(self.calls, 0)
        with connect(self.db) as connection:
            self.assertTrue(budget_summary(connection, at=AT)["circuit"]["open"])

    def claim_only(self, budget, *, content_id=1):
        with self.dispatch_scope("detail"):
            return capture._claim_paid_tikhub(
                content_id=content_id, account_id=None, stage="detail", window_key="lifetime",
                provider="TikHub", adapter_version="test-v1", operation="douyin_video_detail",
                db_path=self.db, budget_id=budget, task_id="test", task_max_amount=20,
                allow_terminal_retry=False,
            )

    def send_claim(self, claim, budget):
        return capture._execute_claimed_fetch(
            claim=claim, operation="douyin_video_detail", call=self.call, db_path=self.db,
            raw_root=self.raw, budget_id=budget, task_id="test", task_max_amount=20,
        )

    def test_midnight_moves_unsent_reservation_and_keeps_actual_send_day(self):
        self.roster()
        budget = self.budget()
        with patch("v8.capture.now_utc", return_value="2026-08-28T15:59:59Z"):
            claim = self.claim_only(budget)
        with patch("v8.capture.now_utc", return_value="2026-08-28T16:00:00Z"):
            self.send_claim(claim, budget)
        row = self.usage()[0]
        self.assertEqual(row["recorded_at"], "2026-08-28T16:00:00Z")
        self.assertEqual(json.loads(row["details_json"])["budget_day"], "2026-08-29")

    def test_midnight_new_day_exhausted_releases_unsent_without_attempt(self):
        self.roster()
        budget = self.budget()
        with patch("v8.capture.now_utc", return_value="2026-08-28T15:59:59Z"):
            claim = self.claim_only(budget)
        self.prefill(100, at="2026-08-28T16:00:00Z")
        with patch("v8.capture.now_utc", return_value="2026-08-28T16:00:00Z"):
            with self.assertRaises(BudgetBlocked):
                self.send_claim(claim, budget)
        self.assertEqual(self.calls, 0)
        self.assertEqual(self.usage()[0]["amount"], 0)
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM fetch_attempts").fetchone()[0], 0)
            self.assertEqual(tuple(connection.execute("SELECT status,attempt_count FROM fetch_slots").fetchone()), ("pending", 0))

    def test_member_paused_after_claim_never_dispatches(self):
        self.roster()
        budget = self.budget()
        claim = self.claim_only(budget)
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE accounts SET enabled=0 WHERE id=1")
        with self.assertRaises(BudgetBlocked) as caught:
            self.send_claim(claim, budget)
        self.assertEqual(caught.exception.error_code, "member_scope_changed")
        self.assertEqual(self.calls, 0)
        self.assertEqual(self.usage()[0]["request_attempts"], 0)
        with connect(self.db) as connection:
            dispatch_id = connection.execute(
                "SELECT dispatch_id FROM paid_provider_dispatch_events LIMIT 1"
            ).fetchone()[0]
            self.assertEqual(
                [
                    event.event_type
                    for event in dispatch_events(connection, str(dispatch_id))
                ],
                ["reserved", "not_sent"],
            )

    def test_concurrent_tasks_share_one_automatic_remaining_request(self):
        self.roster()
        first, second = self.budget(task="first"), self.budget(task="second")
        self.prefill(49.999)
        def work(args):
            budget, task, content_id = args
            try:
                self.execute(budget, task=task, content_id=content_id)
                return "success"
            except BudgetBlocked as exc:
                return exc.error_code
        with patch("v8.capture.now_utc", return_value=AT), ThreadPoolExecutor(2) as pool:
            outcomes = list(pool.map(work, ((first, "first", 1), (second, "second", 2))))
        self.assertCountEqual(outcomes, ["success", "automatic_budget_exhausted"])

    def test_all_live_tikhub_calls_share_four_network_permits(self):
        self.roster()
        budget = self.budget()
        lock = threading.Lock()
        release = threading.Event()
        four_entered = threading.Event()
        active = peak = 0

        def work(number):
            def call():
                nonlocal active, peak
                with lock:
                    active += 1
                    peak = max(peak, active)
                    if active == 4:
                        four_entered.set()
                try:
                    self.assertTrue(release.wait(5), "test network barrier was never released")
                    return ProviderResult({}, {"request": number}, 200, True)
                finally:
                    with lock:
                        active -= 1
            return self.execute(budget, window=f"parallel-{number}", call=call)

        with patch("v8.capture.now_utc", return_value=AT), ThreadPoolExecutor(6) as pool:
            futures = [pool.submit(work, number) for number in range(6)]
            try:
                self.assertTrue(four_entered.wait(5))
                with lock:
                    self.assertEqual(active, 4)
                    self.assertEqual(peak, 4)
            finally:
                release.set()
            self.assertEqual(len([future.result() for future in futures]), 6)
        self.assertEqual(peak, 4)
        self.assertEqual(sum(row["amount"] for row in self.usage()), .006)

    def test_paid_drain_blocks_preflight_and_claim_then_release_reopens(self):
        self.roster()
        budget = self.budget()
        binding = {
            "source_activation_id": 1,
            "target_activation_id": 2,
            "business_day": "2026-08-29",
            "planned_effective_at": "2026-08-29T16:00:00Z",
            "build_receipt_sha256": "a" * 64,
            "runtime_root_receipt_sha256": "b" * 64,
        }
        capture.paid_drain.start_paid_drain(
            "capture-lifecycle", binding=binding, db_path=self.db, now=AT
        )
        with self.assertRaises(BudgetBlocked) as preflight:
            capture.require_tikhub_paid_dispatch_open(
                operation="douyin_video_detail", db_path=self.db, at=AT
            )
        with self.assertRaises(BudgetBlocked) as claim:
            self.execute(budget)
        self.assertEqual(preflight.exception.error_code, "profile_switch_drain")
        self.assertEqual(claim.exception.error_code, "profile_switch_drain")
        self.assertEqual(self.calls, 0)
        with connect(self.db) as connection:
            for table in ("provider_usage", "fetch_attempts", "fetch_slots"):
                self.assertEqual(
                    connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0],
                    0,
                )

        capture.paid_drain.seal_paid_drain(
            "capture-lifecycle", db_path=self.db, now="2026-08-29T04:01:00Z"
        )
        capture.paid_drain.release_paid_drain(
            "capture-lifecycle", db_path=self.db, now="2026-08-29T04:02:00Z"
        )
        with patch("v8.capture.now_utc", return_value="2026-08-29T04:03:00Z"):
            outcome = self.execute(budget)
        self.assertTrue(outcome.billed)
        self.assertEqual(self.calls, 1)
        self.assertEqual(len(self.usage()), 1)
        self.assertEqual(json.loads(self.usage()[0]["details_json"])["state"], "completed")

    def test_drain_start_while_waiting_for_network_permit_never_sends(self):
        self.roster()
        budget = self.budget()
        test = self
        binding = {
            "source_activation_id": 1,
            "target_activation_id": 2,
            "business_day": "2026-08-29",
            "planned_effective_at": "2026-08-29T16:00:00Z",
            "build_receipt_sha256": "a" * 64,
            "runtime_root_receipt_sha256": "b" * 64,
        }

        class DrainStartsWhileWaiting:
            def __enter__(self):
                capture.paid_drain.start_paid_drain(
                    "capture-semaphore-race",
                    binding=binding,
                    db_path=test.db,
                    now="2026-08-29T04:01:00Z",
                )
                return self

            def __exit__(self, *_args):
                return False

        with patch("v8.capture.now_utc", return_value=AT), patch.object(
            capture, "TIKHUB_NETWORK_SLOTS", DrainStartsWhileWaiting()
        ):
            with self.assertRaises(BudgetBlocked) as caught:
                self.execute(budget)
        self.assertEqual(caught.exception.error_code, "profile_switch_drain")
        self.assertEqual(self.calls, 0)
        usage = self.usage()
        self.assertEqual(len(usage), 1)
        self.assertEqual(json.loads(usage[0]["details_json"])["state"], "reserved")
        self.assertEqual(usage[0]["request_attempts"], 0)
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM fetch_attempts").fetchone()[0],
                0,
            )
            self.assertEqual(
                tuple(
                    connection.execute(
                        "SELECT status,attempt_count FROM fetch_slots"
                    ).fetchone()
                ),
                ("running", 0),
            )
            state = capture.paid_drain.dispatch_state(connection)
        self.assertEqual(state.state, "draining")

    def test_waiting_for_network_permit_holds_no_transaction_and_rechecks_uid(self):
        self.roster()
        budget = self.budget()
        test = self

        class ChangedWhileWaiting:
            def __enter__(self):
                # BEGIN IMMEDIATE succeeds: the waiting claimant owns no DB lock.
                with connect(test.db) as connection, transaction(connection):
                    connection.execute("UPDATE account_platform_identities SET uid='changed' WHERE id=1")
                return self

            def __exit__(self, *_args):
                return False

        with patch.object(capture, "TIKHUB_NETWORK_SLOTS", ChangedWhileWaiting()):
            with self.assertRaises(BudgetBlocked) as caught:
                self.execute(budget)
        self.assertEqual(caught.exception.error_code, "identity_conflict")
        self.assertEqual(self.calls, 0)
        self.assertEqual(self.usage()[0]["amount"], 0)
        self.assertEqual(self.usage()[0]["request_attempts"], 0)

    def test_waiting_for_permit_rechecks_beijing_send_day(self):
        self.roster()
        budget = self.budget()
        self.prefill(100, at="2026-08-28T16:00:00Z")
        clock = {"value": "2026-08-28T15:59:59Z"}

        class MidnightWhileWaiting:
            def __enter__(self):
                clock["value"] = "2026-08-28T16:00:00Z"
                return self

            def __exit__(self, *_args):
                return False

        with patch("v8.capture.now_utc", side_effect=lambda: clock["value"]):
            with patch.object(capture, "TIKHUB_NETWORK_SLOTS", MidnightWhileWaiting()):
                with self.assertRaises(BudgetBlocked) as caught:
                    self.execute(budget)
        self.assertEqual(caught.exception.error_code, "global_budget_exhausted")
        self.assertEqual(self.calls, 0)
        self.assertEqual(self.usage()[-1]["amount"], 0)

    def test_paid_business_day_expiry_blocks_before_network_send(self):
        self.roster()
        budget = self.budget()
        next_day = "2026-08-29T16:00:00Z"
        with paid_scope("detail", business_day="2026-08-29"):
            with patch("v8.capture.now_utc", return_value=next_day):
                with self.assertRaises(BudgetBlocked) as caught:
                    self.execute(budget)
        self.assertEqual(caught.exception.error_code, "business_day_expired")
        self.assertEqual(self.calls, 0)
        self.assertEqual(len(self.usage()), 1)
        self.assertEqual(self.usage()[0]["request_attempts"], 0)
        self.assertEqual(self.usage()[0]["amount"], 0)
        details = json.loads(self.usage()[0]["details_json"])
        self.assertEqual(details["state"], "not_sent")
        self.assertEqual(details["scope"]["business_day"], "2026-08-29")

    def test_recovery_refunds_only_provably_unsent_reservations(self):
        self.roster()
        budget = self.budget()
        with patch("v8.capture.now_utc", return_value=AT):
            unsent = self.claim_only(budget)
            sent = self.claim_only(budget, content_id=2)
            capture._mark_paid_sent(sent, operation="douyin_video_detail", budget_id=budget, db_path=self.db)
        result = capture.recover_stale_fetch_slots(
            db_path=self.db, current_time=datetime(2026, 8, 29, 4, 20, tzinfo=timezone.utc),
        )
        self.assertEqual(result, {"stale_candidates": 2, "recovered": 2})
        self.assertEqual([row["amount"] for row in self.usage()], [0, .001])
        self.assertEqual([json.loads(row["details_json"])["state"] for row in self.usage()],
                         ["not_sent", "billing_unknown"])
        with connect(self.db) as connection:
            dispatch_ids = [
                str(row["dispatch_id"])
                for row in connection.execute(
                    "SELECT dispatch_id FROM paid_provider_dispatch_events "
                    "WHERE sequence=1 ORDER BY id"
                )
            ]
            sequences = [
                [event.event_type for event in dispatch_events(connection, dispatch_id)]
                for dispatch_id in dispatch_ids
            ]
        self.assertEqual(
            sequences,
            [
                ["reserved", "not_sent"],
                ["reserved", "send_marked", "billing_unknown"],
            ],
        )
        with connect(self.db) as connection:
            circuit = budget_summary(connection, at=AT)["circuit"]
            paid_identity = json.loads(self.usage()[1]["details_json"])[
                "paid_scope_identity"
            ]
            hold = fault_state(
                connection,
                scope_kind="paid_identity_hold",
                paid_identity=paid_identity,
            )
        self.assertIsNone(circuit)
        self.assertTrue(hold["open"])
        generation = hold["generation"]
        capture.recover_stale_fetch_slots(
            db_path=self.db,
            current_time=datetime(2026, 8, 29, 4, 40, tzinfo=timezone.utc),
        )
        with connect(self.db) as connection:
            self.assertEqual(
                fault_state(
                    connection,
                    scope_kind="paid_identity_hold",
                    paid_identity=paid_identity,
                )["generation"],
                generation,
            )
        with self.assertRaises(BudgetBlocked):
            self.send_claim(unsent, budget)
        with self.assertRaises(BudgetBlocked):
            self.send_claim(sent, budget)
        self.assertEqual(self.calls, 0)
        self.assertEqual([row["amount"] for row in self.usage()], [0, .001])

    def test_duplicate_send_does_not_refund_a_sent_claim(self):
        self.roster()
        budget = self.budget()
        claim = self.claim_only(budget)
        capture._mark_paid_sent(claim, operation="douyin_video_detail", budget_id=budget, db_path=self.db)
        with self.assertRaises(BudgetBlocked):
            self.send_claim(claim, budget)
        self.assertEqual(self.usage()[0]["amount"], .001)
        self.assertEqual(json.loads(self.usage()[0]["details_json"])["state"], "sent")
        self.assertEqual(self.calls, 0)

    def test_paid_slot_owner_does_not_scan_unrelated_usage_json(self):
        self.roster()
        budget = self.budget()
        claim = self.claim_only(budget)
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                """INSERT INTO provider_usage(
                       provider,operation,request_attempts,billed_requests,currency,
                       amount,recorded_at,details_json)
                   VALUES ('TikHub','unrelated',0,0,'USD',0,?,'not-json')""",
                (AT,),
            )
            self.assertTrue(
                capture._paid_slot_owner(connection, claim, check_scheduler=False)
            )

    def test_running_attempt_must_also_own_the_frozen_run_fence(self):
        snapshot = self.roster()
        budget = self.budget()
        run = durable_runs.claim_run(
            "content_pipeline",
            {"purpose": "detail", "uid": "10000001", "platform": "douyin",
             "identity_id": 1, "account_id": 1, "roster_snapshot_id": snapshot["id"],
             "roster_snapshot_hash": snapshot["members_sha256"]},
            db_path=self.db, now=AT,
        )
        self.assertIsNotNone(run)
        with paid_scope("detail", scheduler_run_id=run.scheduler_run_id,
                        scheduler_attempt_id=run.attempt_id):
            claim = self.claim_only(budget)
            with connect(self.db) as connection, transaction(connection):
                details = json.loads(connection.execute(
                    "SELECT details_json FROM scheduler_runs WHERE id=?",
                    (run.scheduler_run_id,),
                ).fetchone()[0])
                details["owner"]["attempt_id"] = 999999
                connection.execute(
                    "UPDATE scheduler_runs SET details_json=? WHERE id=?",
                    (json.dumps(details), run.scheduler_run_id),
                )
            with self.assertRaises(BudgetBlocked):
                self.send_claim(claim, budget)
            with connect(self.db) as connection, transaction(connection):
                with self.assertRaises(BudgetBlocked):
                    assert_paid_scope_owner(connection)
        self.assertEqual(self.calls, 0)
        self.assertEqual(self.usage()[0]["amount"], 0)



    def history_catalog_debt(self, *, at="2026-08-29T10:40:00Z"):
        claim = durable_runs.claim_run(
            "history_scan_catalog",
            {"kind": "initial", "fixture": "new-donor-debt"},
            db_path=self.db,
            now=at,
            initial_checkpoint={
                "items": [{
                    "provider": "tikhub",
                    "identity_id": 1,
                    "purpose": "history",
                }],
                "pending_indices": [0],
                "children": {},
                "complete": False,
            },
        )
        durable_runs.finish_run(
            claim,
            status="partial",
            db_path=self.db,
            now=at,
            next_resume_at=at,
        )

    def metric_borrower(self):
        self.roster()
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE content_items SET published_at='2026-08-28T04:00:00Z'")
        self.prefill(8, category="metrics")
        return self.budget(operation="douyin_video_statistics")

    def execute_metrics(self, budget, *, content_id=1):
        with paid_scope("metrics"):
            return self.execute(budget, content_id=content_id, operation="douyin_video_statistics",
                                stage="metrics", call=self.call)

    @patch.dict(CATEGORY_MICROUSD, {"history": 2_000_000})
    def test_historical_closeout_does_not_enable_cross_bucket_borrowing(self):
        budget = self.metric_borrower()
        self.prefill(6.999, category="metrics")
        at = "2026-08-29T10:35:00Z"
        fingerprint = hashlib.sha256(b"archived-work-state").hexdigest()
        with connect(self.db) as connection, transaction(connection):
            round_ids = []
            for registration in ("matrix_account_metrics", "matrix_works_refresh"):
                scheduled = "2026-08-29T10:00:00Z"
                details = {"contract_version": "durable-run-v1", "complete": True,
                           "identity": {"beijing_day": "2026-08-29", "scheduled_at": scheduled,
                                        "round_id": registration + ":18:00"},
                           "checkpoint": {"complete": True, "child_run_ids": []}}
                cursor = connection.execute(
                    """INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,completed_at,details_json)
                       VALUES (?,?,'succeeded',?,?,?)""",
                    ("pipeline_round:" + registration, scheduled, scheduled, scheduled, json.dumps(details)),
                )
                round_ids.append(cursor.lastrowid)
            inventory = {"contract_version": "budget-queue-inventory-v1", "category": "history",
                         "budget_day": "2026-08-29", "checked_at": at, "due_candidate_ids": [],
                         "due_count": 0, "candidate_sha256": hashlib.sha256(b"[]").hexdigest(),
                         "work_state_fingerprint": fingerprint}
            cursor = connection.execute(
                """INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,completed_at,details_json)
                   VALUES ('provider_queue_inventory:tikhub:history',?,'succeeded',?,?,?)""",
                (at, at, at, json.dumps(inventory)),
            )
            closeout = {"contract_version": "budget-closeout-v1", "category": "history",
                        "budget_day": "2026-08-29", "closed_at": at, "inventory_ids": [cursor.lastrowid],
                        "round_run_ids": round_ids, "work_state_fingerprint": fingerprint}
            connection.execute(
                """INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,completed_at,details_json)
                   VALUES ('provider_budget_closeout:tikhub:history',?,'succeeded',?,?,?)""",
                (at, at, at, json.dumps(closeout)),
            )
        with patch("v8.capture.now_utc", return_value=at):
            self.execute_metrics(budget)
            with self.assertRaises(BudgetBlocked) as caught:
                self.execute_metrics(budget, content_id=2)
        self.assertEqual(caught.exception.error_code, "metrics_budget_exhausted")
        self.assertEqual(self.calls, 1)
        charged = self.usage()[2:]
        self.assertEqual(len(charged), 1)
        details = json.loads(charged[0]["details_json"])
        self.assertEqual(details["borrowed_from"], {})
        self.assertEqual(details["borrowing_proofs"], {})
        with connect(self.db) as connection:
            self.assertEqual(budget_summary(connection, at=at)["total_microusd"], 15_000_000)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM fetch_attempts").fetchone()[0], 1)

    def test_donor_debt_does_not_change_independent_metrics_bucket(self):
        budget = self.metric_borrower()
        self.history_catalog_debt()
        with patch("v8.capture.now_utc", return_value="2026-08-29T10:40:00Z"):
            self.execute_metrics(budget)
        self.assertEqual(self.calls, 1)
        self.assertEqual(len(self.usage()), 2)

    def test_detail_reservation_does_not_require_lending_to_metrics(self):
        budget = self.metric_borrower()
        detail_budget = self.budget()
        with paid_scope("detail"), patch("v8.capture.now_utc", return_value="2026-08-29T10:36:00Z"):
            self.claim_only(detail_budget, content_id=2)
        with patch("v8.capture.now_utc", return_value="2026-08-29T10:37:00Z"):
            self.execute_metrics(budget)
        self.assertEqual(self.calls, 1)
        self.assertEqual(len(self.usage()), 3)

    def open_circuit(self, budget):
        with patch("v8.capture.now_utc", return_value=AT):
            with self.assertRaises(CaptureError):
                self.execute(budget, call=lambda: (_ for _ in ()).throw(
                    CaptureError("balance", retryable=True, error_code="provider_balance_blocked",
                                 billed=False, http_status=402)
                ))

    def authorize_probe(self):
        with connect(self.db) as connection, transaction(connection):
            return authorize_recovery_probe(
                connection, authorization_ref="isolated-operator-receipt",
                operation="douyin_video_detail", at=AT,
            )

    def test_explicit_recovery_probe_is_one_shot_and_never_resets_costs(self):
        self.roster()
        budget = self.budget()
        self.prefill(1, category="detail")
        self.open_circuit(budget)
        probe = self.authorize_probe()
        with circuit_recovery_probe(probe["id"]), patch("v8.capture.now_utc", return_value=AT):
            self.execute(budget, content_id=2)
            with self.assertRaises(BudgetBlocked):
                self.execute(budget)
        self.assertEqual(self.calls, 1)
        with connect(self.db) as connection:
            summary = budget_summary(connection, at=AT)
            self.assertFalse(summary["circuit"]["open"])
            self.assertEqual(summary["total_microusd"], 1_001_000)
            details = json.loads(connection.execute(
                "SELECT details_json FROM scheduler_runs WHERE id=?", (probe["id"],)
            ).fetchone()[0])
            self.assertEqual(details["state"], "succeeded")
        self.assertEqual(self.usage()[0]["amount"], 1)

    def test_probe_still_obeys_global_ceiling(self):
        self.roster()
        budget = self.budget()
        self.open_circuit(budget)
        probe = self.authorize_probe()
        self.prefill(100)
        with circuit_recovery_probe(probe["id"]), patch("v8.capture.now_utc", return_value=AT):
            with self.assertRaises(BudgetBlocked) as caught:
                self.execute(budget, content_id=2)
        self.assertEqual(caught.exception.error_code, "global_budget_exhausted")
        self.assertEqual(self.calls, 0)
        with connect(self.db) as connection:
            self.assertTrue(budget_summary(connection, at=AT)["circuit"]["open"])
            self.assertEqual(budget_summary(connection, at=AT)["total_microusd"], 100_000_000)

    def test_failed_probe_retains_unknown_money_and_does_not_auto_retry(self):
        self.roster()
        budget = self.budget()
        self.open_circuit(budget)
        probe = self.authorize_probe()
        with circuit_recovery_probe(probe["id"]), patch("v8.capture.now_utc", return_value=AT):
            with self.assertRaises(CaptureError):
                self.execute(budget, content_id=2, call=lambda: (_ for _ in ()).throw(
                    CaptureError("timeout", retryable=True, error_code="transport_error", billed=None)
                ))
        with patch("v8.capture.now_utc", return_value=AT):
            with self.assertRaises(BudgetBlocked) as caught:
                self.execute(budget)
        self.assertEqual(caught.exception.error_code, "provider_circuit_open")
        self.assertEqual(self.calls, 0)
        self.assertEqual(self.usage()[-1]["amount"], .001)
        self.assertEqual(json.loads(self.usage()[-1]["details_json"])["state"], "billing_unknown")



    def test_revoked_budget_while_waiting_never_sends(self):
        self.roster()
        budget = self.budget()
        test = self

        class RevokedWhileWaiting:
            def __enter__(self):
                with connect(test.db) as connection, transaction(connection):
                    connection.execute("UPDATE provider_budget_batches SET status='completed' WHERE id=?", (budget,))
                return self

            def __exit__(self, *_args):
                return False

        with patch.object(capture, "TIKHUB_NETWORK_SLOTS", RevokedWhileWaiting()):
            with self.assertRaises(BudgetBlocked) as caught:
                self.execute(budget)
        self.assertEqual(caught.exception.error_code, "budget_batch_changed")
        self.assertEqual(self.calls, 0)
        self.assertEqual(self.usage()[0]["amount"], 0)

    def test_late_success_keeps_provider_truth_without_stealing_recovered_slot(self):
        self.roster()
        budget = self.budget()

        def response_after_recovery():
            capture.recover_stale_fetch_slots(
                db_path=self.db, current_time=datetime(2026, 8, 29, 4, 20, tzinfo=timezone.utc),
            )
            return ProviderResult({}, {"real": "response"}, 200, True)

        with patch("v8.capture.now_utc", return_value=AT):
            with self.assertRaises(CaptureError) as caught:
                self.execute(budget, call=response_after_recovery)
        self.assertEqual(caught.exception.error_code, "attempt_owner_lost")
        self.assertEqual(self.usage()[0]["amount"], .001)
        self.assertEqual(json.loads(self.usage()[0]["details_json"])["state"], "completed")
        with connect(self.db) as connection:
            slot = connection.execute(
                "SELECT status,last_error_code FROM fetch_slots"
            ).fetchone()
            self.assertEqual(slot["status"], "retryable_failed")
            self.assertEqual(slot["last_error_code"], "late_provider_result_retryable")
            self.assertIsNone(connection.execute("SELECT error_code FROM fetch_attempts").fetchone()[0])
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_raw_responses").fetchone()[0], 1)

    def test_late_known_failure_clears_only_the_derived_billing_guard(self):
        self.roster()
        budget = self.budget()

        def failure_after_recovery():
            capture.recover_stale_fetch_slots(
                db_path=self.db,
                current_time=datetime(2026, 8, 29, 4, 20, tzinfo=timezone.utc),
            )
            raise CaptureError(
                "provider rejected request",
                retryable=True,
                error_code="provider_retry_requested",
                billed=True,
                http_status=400,
            )

        with patch("v8.capture.now_utc", return_value=AT):
            with self.assertRaises(CaptureError) as caught:
                self.execute(budget, call=failure_after_recovery)
        self.assertEqual(caught.exception.error_code, "provider_retry_requested")
        self.assertEqual(
            json.loads(self.usage()[0]["details_json"])["state"], "failed"
        )
        with connect(self.db) as connection:
            slot = connection.execute(
                "SELECT status,last_error_code FROM fetch_slots"
            ).fetchone()
            self.assertEqual(slot["status"], "retryable_failed")
            self.assertEqual(slot["last_error_code"], "provider_retry_requested")

    def test_response_after_midnight_stays_on_actual_send_day(self):
        self.roster()
        budget = self.budget()
        clock = {"value": "2026-08-28T15:59:59Z"}

        def response():
            clock["value"] = "2026-08-28T16:00:01Z"
            return ProviderResult({}, {}, 200, True)

        with patch("v8.capture.now_utc", side_effect=lambda: clock["value"]):
            self.execute(budget, call=response)
        row = self.usage()[0]
        self.assertEqual(row["recorded_at"], "2026-08-28T15:59:59Z")
        self.assertEqual(json.loads(row["details_json"])["budget_day"], "2026-08-28")

    def test_donor_debt_arriving_during_wait_does_not_affect_fixed_bucket(self):
        budget = self.metric_borrower()
        test = self

        class DonorChangedWhileWaiting:
            def __enter__(self):
                test.history_catalog_debt(at="2026-08-29T10:35:00Z")
                return self

            def __exit__(self, *_args):
                return False

        with patch("v8.capture.now_utc", return_value="2026-08-29T10:35:00Z"):
            with patch.object(capture, "TIKHUB_NETWORK_SLOTS", DonorChangedWhileWaiting()):
                self.execute_metrics(budget)
        self.assertEqual(self.calls, 1)
        self.assertEqual(self.usage()[-1]["amount"], .001)
        self.assertEqual(self.usage()[-1]["request_attempts"], 1)

    def test_expired_recovery_authorization_never_sends(self):
        self.roster()
        budget = self.budget()
        self.open_circuit(budget)
        probe = self.authorize_probe()
        with circuit_recovery_probe(probe["id"]):
            with patch("v8.capture.now_utc", return_value="2026-08-29T04:16:00Z"):
                with self.assertRaises(BudgetBlocked) as caught:
                    self.execute(budget, content_id=2)
        self.assertEqual(caught.exception.error_code, "recovery_not_authorized")
        self.assertEqual(self.calls, 0)
        self.assertEqual(len(self.usage()), 1)


if __name__ == "__main__":
    unittest.main()
