"""Bounded explicit retry keeps real paid dispatch, raw and operation holds."""
from __future__ import annotations

from datetime import timedelta
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from tests import test_v8_manual_content_scope as fixture
from tests import test_v8_account_operator_roster as operator_fixture
from v8 import api, capture, capture_commands, capture_manual, capture_runtime
from v8 import operation_recovery, provider_budget, provider_updates, providers, capture_operator_release as operator
from v8.operations import upsert_content
from v8.source_routing import parse_time
from v8.storage import connect, transaction
from v8.work_readiness import WorkReadinessPass

AT = fixture.AT
OP = "douyin_video_detail"
OLD = (parse_time(AT) - timedelta(minutes=10)).isoformat()


class ManualTransportRetryTest(unittest.TestCase):
    setUp = fixture.ManualContentScopeTest.setUp
    submit = fixture.ManualContentScopeTest.submit
    response = fixture.ManualContentScopeTest.response
    refresh = fixture.ManualContentScopeTest.refresh
    snapshot = fixture.ManualContentScopeTest.snapshot

    def fault(self, *, at=OLD, fault_class="transport", cooldown_until=None):
        with connect(self.db) as connection, transaction(connection):
            state = provider_budget.record_fault_state(connection, scope_kind="operation", operation=OP,
                fault_class=fault_class, reason="fixture_transport", usage_id=None, at=at)
            if cooldown_until is not None:
                # Retained state after earlier failed ordinary probes. Explicit
                # one-request commands keep their own five-minute contract.
                state["cooldown"] = {"failures": 2, "retry_after": cooldown_until}
                operation_recovery._append(connection, state, at)
            return state

    def retry(self, *, cid=None, **options):
        return capture_commands.submit_command(db_path=self.db, content_id=cid or self.cid,
            kind="metrics_update", at=options.pop("at", AT), allowed_groups=["detail_counts"],
            task_id="explicit-retry-fixture", task_max_amount=options.pop("task_max_amount", .1),
            cycle_key="explicit-fixture-cycle", retry_transport_fault=True, **options)

    def scope(self, command):
        with connect(self.db) as connection:
            spec = capture_manual.validate_command(connection, command["run_id"], content_id=self.cid)
        return provider_budget.PaidScope(category="metrics", content_id=self.cid,
            account_id=self.account["id"], identity_id=self.identity_id,
            manual_command_run_id=command["run_id"], paid_scope_identity=spec["transport_retry"]["paid_scope_identity"])

    def test_real_paid_retry_succeeds_replays_once_and_keeps_automatic_fault(self):
        fault = self.fault(cooldown_until=(parse_time(AT) + timedelta(minutes=10)).isoformat())
        with self.assertRaises(provider_budget.PaidScopeBlocked):
            self.refresh(allowed_groups=["detail_counts"])
        command = self.retry()
        before = self.snapshot()
        def response_without_recovery_owner(group, content):
            with connect(self.db) as connection:
                current = provider_budget.fault_state(connection, scope_kind="operation", operation=OP)
                self.assertNotIn("half_open", current)
                self.assertEqual(current["state_fingerprint"], fault["state_fingerprint"])
                self.assertTrue(current["open"])
            return self.response(group, content)
        result = self.refresh(manual_command_run_id=command["run_id"], allowed_groups=["detail_counts"],
            call_override=response_without_recovery_owner)
        self.assertEqual(result["requests"][0]["status"], "succeeded")
        self.assertEqual(self.refresh(manual_command_run_id=command["run_id"], allowed_groups=["detail_counts"])["provider_cost"], 0)
        self.assertEqual(self.calls, ["detail_counts"])
        self.assertEqual(self.snapshot(), before)
        with connect(self.db) as connection:
            current = provider_budget.fault_state(connection, scope_kind="operation", operation=OP)
            self.assertEqual((current["generation"], current["open"]), (fault["generation"], True))
            self.assertEqual(current["cooldown"], fault["cooldown"])
            self.assertNotIn("half_open", current)
            self.assertEqual(connection.execute("SELECT count(*) FROM provider_usage WHERE request_attempts=1").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT count(*) FROM provider_raw_responses WHERE fetch_attempt_id IS NOT NULL").fetchone()[0], 1)
            with self.assertRaises(provider_budget.PaidScopeBlocked):
                provider_budget.check_reservation(connection, scope=provider_budget.PaidScope(category="metrics"),
                    operation=OP, unit_price=.001, currency="USD", at=AT)

    def test_new_uncertain_response_stops_other_frozen_targets_and_advances_generation(self):
        fault = self.fault()
        first = self.retry()
        second_content = upsert_content({"platform": "douyin", "platform_content_id": "7380000000000000002",
            "canonical_url": "https://www.douyin.com/video/7380000000000000002", "account_uid": fixture.UID,
            "content_type": "video", "published_at": "2026-08-31T00:00:00Z"}, db_path=self.db)
        second = self.retry(cid=second_content["id"])
        def uncertain(group, content):
            self.calls.append(group)
            raise capture.CaptureError("fixture incomplete response", retryable=True,
                error_code="transport_error", billed=None)
        with self.assertRaises(capture.CaptureError):
            self.refresh(manual_command_run_id=first["run_id"], allowed_groups=["detail_counts"], call_override=uncertain)
        with self.assertRaises(provider_budget.PaidScopeBlocked) as caught:
            provider_updates.refresh_content_metrics(second_content["id"], db_path=self.db, at=AT,
                manual_command_run_id=second["run_id"], call_override=self.response)
        self.assertEqual(caught.exception.error_code, "manual_transport_retry_stale")
        self.assertEqual(self.calls, ["detail_counts"])
        with connect(self.db) as connection:
            current = provider_budget.fault_state(connection, scope_kind="operation", operation=OP)
            self.assertNotEqual(current["generation"], fault["generation"])
            self.assertTrue(current["open"])
            self.assertNotIn("half_open", current)

    def test_manual_retry_lock_contention_refunds_and_preserves_same_unsent_identity(self):
        fault = self.fault()
        command = self.retry()
        with operation_recovery.operation_probe_lock(db_path=self.db, operation=OP):
            with self.assertRaises(provider_budget.PaidScopeBlocked) as caught:
                self.refresh(manual_command_run_id=command["run_id"], allowed_groups=["detail_counts"])
        self.assertEqual(caught.exception.error_code, "operation_blocked")
        self.assertEqual(self.calls, [])
        with connect(self.db) as connection:
            usage = connection.execute("SELECT * FROM provider_usage").fetchone()
            details = json.loads(usage["details_json"])
            identity = details["paid_scope_identity"]
            self.assertEqual((usage["request_attempts"], usage["amount"], details["state"]), (0, 0, "not_sent"))
            batch = connection.execute("SELECT * FROM provider_budget_batches WHERE id=?", (usage["budget_batch_id"],)).fetchone()
            self.assertEqual((batch["consumed_requests"], batch["consumed_amount"]), (0, 0))
        self.assertEqual(list((self.db.parent / "paid_send_claims").rglob("*.json")), [])
        result = self.refresh(manual_command_run_id=command["run_id"], allowed_groups=["detail_counts"])
        self.assertEqual(result["requests"][0]["status"], "succeeded")
        self.assertEqual(self.calls, ["detail_counts"])
        with connect(self.db) as connection:
            usages = connection.execute("SELECT details_json FROM provider_usage ORDER BY id").fetchall()
            self.assertEqual([json.loads(row[0])["paid_scope_identity"] for row in usages], [identity, identity])
            current = provider_budget.fault_state(connection, scope_kind="operation", operation=OP)
            self.assertEqual((current["generation"], current["open"]), (fault["generation"], True))
            self.assertNotIn("half_open", current)

    def test_manual_retry_expiring_after_reservation_cannot_cross_send_boundary(self):
        self.fault(at=(parse_time(AT) - timedelta(minutes=100)).isoformat())
        command = self.retry(at=(parse_time(AT) - timedelta(minutes=90) + timedelta(seconds=1)).isoformat())
        mark_sent = capture._mark_paid_sent
        expired = (parse_time(AT) + timedelta(seconds=2)).isoformat()
        def expire_before_send(*args, **kwargs):
            with patch.object(capture, "now_utc", return_value=expired):
                return mark_sent(*args, **kwargs)
        with patch.object(capture, "_mark_paid_sent", side_effect=expire_before_send):
            with self.assertRaises(provider_budget.PaidScopeBlocked) as caught:
                self.refresh(manual_command_run_id=command["run_id"], allowed_groups=["detail_counts"])
        self.assertEqual(caught.exception.error_code, "manual_transport_retry_expired")
        self.assertEqual(self.calls, [])
        with connect(self.db) as connection:
            usage = connection.execute("SELECT * FROM provider_usage").fetchone()
            self.assertEqual((usage["request_attempts"], usage["amount"], json.loads(usage["details_json"])["state"]),
                (0, 0, "not_sent"))
            self.assertEqual(connection.execute("SELECT count(*) FROM fetch_attempts").fetchone()[0], 0)
        self.assertEqual(list((self.db.parent / "paid_send_claims").rglob("*.json")), [])

    def _ordinary_manual_detail(self, *, fault_open):
        if fault_open:
            self.fault()
        command = capture_commands.submit_command(db_path=self.db, content_id=self.cid,
            kind="media_retry", at=AT, task_id="ordinary-manual-detail", task_max_amount=.1)
        with connect(self.db) as connection, transaction(connection):
            assignment = capture_manual.assignment_for_command(connection, command["run_id"],
                content_id=self.cid, operation=OP, at=AT, create=True)
        budget_id = providers._budget_for_call(provider="TikHub", operation=OP, price=.001,
            task_id="ordinary-manual-detail", task_max_amount=.1, db_path=self.db)
        _, params = providers._douyin_request("detail", fixture.PID)
        request = providers._paid_request_identity(operation=OP, platform="douyin", subject=fixture.PID,
            params=params, cursor=None, due_bucket="lifetime")
        with capture_runtime.planning.execution_route_context(assignment["id"]), \
                provider_budget.paid_scope("detail", manual_command_run_id=command["run_id"]):
            result = capture.execute_content_fetch(content_id=self.cid, stage="detail", window_key="lifetime",
                provider="TikHub", adapter_version=providers.STAGE_CONFIG[("douyin", "detail")][1], operation=OP,
                db_path=self.db, budget_id=budget_id, task_id="ordinary-manual-detail", task_max_amount=.1,
                paid_request_identity=request,
                call=lambda: self.response("detail_counts", {"platform_content_id": fixture.PID}))
        self.assertEqual(result.amount, .001)
        self.assertEqual(self.calls, ["detail_counts"])
        with connect(self.db) as connection:
            usage = connection.execute("SELECT * FROM provider_usage").fetchone()
            self.assertEqual(usage["request_attempts"], 1)
            self.assertEqual(json.loads(usage["details_json"])["scope"]["category"], "detail")
            if fault_open:
                self.assertFalse(provider_budget.fault_state(connection, scope_kind="operation", operation=OP)["open"])

    def test_healthy_manual_media_detail_reaches_real_send_boundary(self):
        self._ordinary_manual_detail(fault_open=False)

    def test_manual_media_detail_uses_ordinary_recovery_after_cooldown(self):
        self._ordinary_manual_detail(fault_open=True)

    def test_unsent_retry_command_keeps_same_identity_after_ordinary_probe_recovers_operation(self):
        self.fault()
        command = self.retry()
        with connect(self.db) as connection:
            original = capture_manual.validate_command(connection, command["run_id"], content_id=self.cid)
        expected_identity = original["transport_retry"]["paid_scope_identity"]
        # A different, ordinary lifetime request really passes B and closes the
        # operation. The pending metrics command has not sent anything yet.
        self._ordinary_manual_detail(fault_open=False)
        with connect(self.db) as connection:
            self.assertFalse(provider_budget.fault_state(connection, scope_kind="operation", operation=OP)["open"])
            with self.assertRaises(provider_budget.PaidScopeBlocked):
                capture_manual.permits_transport_retry(connection, scope=self.scope(command), operation=OP, at=AT)
        result = self.refresh(manual_command_run_id=command["run_id"], allowed_groups=["detail_counts"])
        self.assertEqual(result["requests"][0]["status"], "succeeded")
        self.assertEqual(result["provider_cost"], .001)
        self.assertEqual(self.calls, ["detail_counts", "detail_counts"])
        with connect(self.db) as connection:
            self.assertEqual(capture_manual.validate_command(connection, command["run_id"], content_id=self.cid), original)
            usages = connection.execute("SELECT request_attempts,details_json FROM provider_usage "
                "WHERE json_extract(details_json,'$.paid_scope_identity')=?", (expected_identity,)).fetchall()
            self.assertEqual(len(usages), 1)
            self.assertEqual(usages[0]["request_attempts"], 1)
            self.assertEqual(json.loads(usages[0]["details_json"])["scope"]["manual_command_run_id"], command["run_id"])
        self.assertEqual(self.refresh(manual_command_run_id=command["run_id"], allowed_groups=["detail_counts"])["provider_cost"], 0)
        self.assertEqual(self.calls, ["detail_counts", "detail_counts"])

    def _close_fault(self):
        with connect(self.db) as connection, transaction(connection):
            state = provider_budget.fault_state(connection, scope_kind="operation", operation=OP)
            state.update(open=False, recovered_at=AT)
            operation_recovery._append(connection, state, AT)

    def test_reopened_operation_rejects_old_retry_proof_at_real_send_boundary(self):
        self.fault()
        command = self.retry()
        mark_sent = capture._mark_paid_sent
        def reopen_before_send(*args, **kwargs):
            self._close_fault()
            self.fault(at=AT)
            return mark_sent(*args, **kwargs)
        with patch.object(capture, "_mark_paid_sent", side_effect=reopen_before_send):
            with self.assertRaises(provider_budget.PaidScopeBlocked) as caught:
                self.refresh(manual_command_run_id=command["run_id"], allowed_groups=["detail_counts"])
        self.assertEqual(caught.exception.error_code, "manual_transport_retry_stale")
        self.assertEqual(self.calls, [])
        with connect(self.db) as connection:
            usage = connection.execute("SELECT * FROM provider_usage").fetchone()
            self.assertEqual((usage["request_attempts"], usage["amount"], json.loads(usage["details_json"])["state"]),
                (0, 0, "not_sent"))

    def test_closed_operation_does_not_repurchase_unknown_retry_identity(self):
        self.fault()
        command = self.retry()
        def uncertain(group, content):
            self.calls.append(group)
            raise capture.CaptureError("fixture incomplete response", retryable=True,
                error_code="transport_error", billed=None)
        with self.assertRaises(capture.CaptureError):
            self.refresh(manual_command_run_id=command["run_id"], allowed_groups=["detail_counts"], call_override=uncertain)
        self._close_fault()
        with self.assertRaises((provider_budget.PaidScopeBlocked, capture.CaptureError, capture.SlotUnavailable)):
            self.refresh(manual_command_run_id=command["run_id"], allowed_groups=["detail_counts"])
        self.assertEqual(self.calls, ["detail_counts"])
        with connect(self.db) as connection:
            usage = connection.execute("SELECT * FROM provider_usage").fetchone()
            self.assertEqual((usage["request_attempts"], json.loads(usage["details_json"])["state"]), (1, "billing_unknown"))

    def test_cooldown_expiry_scope_and_nontransport_fault_remain_blocked(self):
        self.fault(at=AT)
        with self.assertRaises(provider_budget.PaidScopeBlocked) as caught:
            self.retry()
        self.assertEqual(caught.exception.error_code, "manual_transport_retry_cooldown")
        later = (parse_time(AT) + timedelta(minutes=6)).isoformat()
        command = self.retry(at=later)
        scope = self.scope(command)
        with connect(self.db) as connection:
            for at in (AT, (parse_time(later) + timedelta(minutes=91)).isoformat()):
                with self.assertRaises(provider_budget.PaidScopeBlocked):
                    capture_manual.permits_transport_retry(connection, scope=scope, operation=OP, at=at)
        self.fault(fault_class="field_contract")
        with connect(self.db) as connection:
            with self.assertRaises(provider_budget.PaidScopeBlocked):
                provider_budget.check_reservation(connection, scope=scope, operation=OP,
                    unit_price=.001, currency="USD", at=later)
        self.assertEqual(self.calls, [])

    def test_readiness_is_command_specific_and_never_poisons_automatic_cache(self):
        self.fault(cooldown_until=(parse_time(AT) + timedelta(minutes=10)).isoformat())
        command = self.retry()
        with connect(self.db) as connection:
            assessment = WorkReadinessPass(connection, at=AT)
            args = dict(operation=OP, category="metrics", content_id=self.cid, account_id=self.account["id"],
                identity_id=self.identity_id, stage="metrics", window_key="explicit-fixture-cycle:detail_counts")
            self.assertEqual(assessment.assess(**args)["reason"], "operation_blocked")
            self.assertTrue(assessment.assess(**args, manual_command_run_id=command["run_id"])["runnable"])
            self.assertEqual(assessment.assess(**args)["reason"], "operation_blocked")
        self.assertEqual(self.calls, [])

    def test_exact_command_enqueue_and_runtime_executor_complete_real_dispatch(self, _outside=False):
        self.fault()
        command = self.retry()
        with connect(self.db) as connection, transaction(connection):
            spec = capture_manual.validate_command(connection, command["run_id"], content_id=self.cid)
            queued = capture_runtime.enqueue_manual_work(connection, specification=spec,
                command_run_id=command["run_id"], at=AT)
            self.assertEqual(len(queued["work_ids"]), 1)
            self.assertEqual(connection.execute("SELECT state FROM capture_work_items WHERE id=?",
                (queued["work_ids"][0],)).fetchone()[0], "runnable")
            outside_id, outside_before = None, None
            if _outside:
                outside = dict(connection.execute("SELECT * FROM capture_work_items WHERE id=?", (queued["work_ids"][0],)).fetchone())
                outside.pop("id")
                envelope = json.loads(outside["envelope_json"])
                envelope.pop("manual_command_run_id")
                envelope.pop("manual_command_run_ids")
                envelope["kind"] = "automatic"
                envelope["logical_due"] = "another-automatic-cycle:detail_counts"
                outside.update(work_identity=capture_runtime.planning.digest({"unrelated": True}),
                    due_at=capture_runtime.planning.timestamp(OLD), envelope_json=json.dumps(envelope))
                outside_id = connection.execute(f"INSERT INTO capture_work_items({','.join(outside)}) VALUES({','.join('?' for _ in outside)})", tuple(outside.values())).lastrowid
                outside_before = tuple(connection.execute("SELECT * FROM capture_work_items WHERE id=?", (outside_id,)).fetchone())
        def raw_call(stage, content_id, key, **kwargs):
            result = self.response("detail_counts", {"platform_content_id": content_id})
            return capture.ProviderResult({"metrics": result.data, "account_uid": fixture.UID},
                result.raw_response, result.http_status, result.billed,
                result.entity_bytes, result.transport_receipt)
        with patch.object(capture_runtime, "now_utc", return_value=AT), \
                patch.object(providers, "_load_key", return_value="fixture"), \
                patch.object(providers, "_freeze_tikhub_transport", return_value=None), \
                patch.object(providers, "_douyin_call", side_effect=raw_call):
            options = {"manual_work_id": queued["work_ids"][0]} if _outside else {}
            result = capture_runtime._run_single(self.db, AT, **options)
        self.assertEqual(result["status"], "terminal", result)
        self.assertEqual(result["provider_cost"], .001)
        self.assertEqual(self.calls, ["detail_counts"])
        with connect(self.db) as connection:
            self.assertTrue(provider_budget.fault_state(connection, scope_kind="operation", operation=OP)["open"])
            self.assertEqual(connection.execute("SELECT count(*) FROM provider_raw_responses WHERE fetch_attempt_id IS NOT NULL").fetchone()[0], 1)
            if outside_id is not None:
                self.assertEqual(tuple(connection.execute("SELECT * FROM capture_work_items WHERE id=?", (outside_id,)).fetchone()), outside_before)

    def test_bounded_manual_work_does_not_consume_or_recover_other_runnable_work(self):
        with patch.object(capture_runtime, "_recover_work", side_effect=AssertionError("bounded execution must not recover unrelated work")):
            self.test_exact_command_enqueue_and_runtime_executor_complete_real_dispatch(_outside=True)
        with self.assertRaises(ValueError):
            capture_runtime._run_single(self.db, AT, manual_work_id=1, compensation_work_id=1)

    def test_http_records_explicit_option_and_rejects_string_true_and_excess_budget(self):
        self.fault()
        app = FastAPI()
        app.include_router(api.router)
        app.state.writer_lock_held = True
        config = SimpleNamespace(db_path=self.db, read_only=False)
        with patch.object(api, "_request_config", return_value=config), \
                patch.object(api, "_connect_for_request", side_effect=lambda _: connect(self.db)), TestClient(app) as client:
            url = f"/api/v8/contents/{self.cid}/metrics/refresh"
            body = {"allowed_groups": ["detail_counts"], "retry_transport_fault": True,
                "task_id": "http-retry", "task_max_amount": .1, "cycle_key": "fixed-http-cycle"}
            response = client.post(url, json=body)
            self.assertEqual(response.status_code, 202, response.text)
            with connect(self.db) as connection:
                spec = capture_manual.validate_command(connection, response.json()["run_id"], content_id=self.cid)
                self.assertTrue(spec["retry_transport_fault"])
                self.assertEqual(spec["transport_retry"]["operation"], OP)
                self.assertEqual(spec["transport_retry"]["max_requests"], 1)
            self.assertEqual(client.post(url, json={**body, "retry_transport_fault": "true"}).status_code, 422)
            self.assertEqual(client.post(url, json={**body, "task_max_amount": 4}).status_code, 409)
        self.assertEqual(self.calls, [])


class RetainedOperatorDecisionTest(unittest.TestCase):
    def test_renewing_true_business_authority_does_not_close_operation_fault(self):
        base = operator_fixture.OperatorRosterCaptureTest()
        base.setUp()
        self.addCleanup(base.doCleanups)
        with connect(base.db) as connection, transaction(connection):
            fault = provider_budget.record_fault_state(connection, scope_kind="operation", operation=OP,
                fault_class="transport", reason="fixture_transport", usage_id=None, at=AT)
            evidence = operator_fixture.release._installed_evidence(connection, at=AT)
            issued = operator.publish(connection, evidence=evidence, operation=OP, at=AT)
            self.assertEqual(issued["qualification"], "operator_authorized")
            self.assertEqual(issued["transport_qualification"], "not_verified")
            current = provider_budget.fault_state(connection, scope_kind="operation", operation=OP)
            self.assertEqual((current["generation"], current["open"]), (fault["generation"], True))
            with self.assertRaises(provider_budget.PaidScopeBlocked):
                provider_budget.check_reservation(connection, scope=provider_budget.PaidScope(category="metrics"),
                    operation=OP, unit_price=.001, currency="USD", at=AT)


if __name__ == "__main__":
    unittest.main()
