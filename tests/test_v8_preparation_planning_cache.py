from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
import ast
import inspect
import json
import sqlite3
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from v8 import account_intake, account_preparation as prep, capture_runtime
from v8.provider_budget import PaidScope, PaidScopeBlocked
from v8.storage import connect, initialize_database

AT = "2026-09-12T03:00:00Z"
ACTIVE = {"activation_id": 1, "profile_id": "integrated_route_v1", "roster_snapshot_id": None, "roster_members_sha256": "a" * 64}
POLICY = {"account_preparation": prep.CONTRACT}


class PreparationPlanningCacheTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = connect(Path(self.temp.name) / "test.sqlite3")
        self.addCleanup(self.db.close)
        initialize_database(self.db, target_version=22)
        self.policy = self.enterContext(patch("v8.account_catalog_capture.installed_policy", return_value=POLICY))
        self.enterContext(patch("v8.profile_activations.activation_at", return_value=ACTIVE))
        self.db.execute("BEGIN")

    def submit(self, count=1):
        for index in range(count):
            account_intake.submit_account_intake(self.db, request_key="fixture-" + str(index),
                value={"platform": "douyin", "uid": str(123456789 + index)}, source={"kind": "fixture"}, at=AT)

    def queue(self, count=1):
        self.submit(count)
        return prep.enqueue_pending(self.db, active=ACTIVE, at=AT)

    def envelope(self):
        return json.loads(self.db.execute("SELECT envelope_json FROM capture_work_items ORDER BY id LIMIT 1").fetchone()[0])

    def scope(self, envelope):
        return PaidScope(purpose="reconcile", **{key: envelope[key] for key in prep.SCOPE_FIELDS})

    def validation(self, envelope):
        return prep._planning_validation(self.db, at=AT, plan_id=envelope["preparation_plan_id"], policy=POLICY)

    def test_n_members_verify_install_once_decode_plan_once_and_index_once(self):
        self.submit(8)
        original_loads, original_digest = json.loads, prep.planning.digest
        decoded_plans, hashed_plans = [], []

        def loads(value, *args, **kwargs):
            result = original_loads(value, *args, **kwargs)
            if isinstance(result, dict) and result.get("contract") == prep.CONTRACT and "members" in result:
                decoded_plans.append(result)
            return result

        def digest(value):
            if isinstance(value, dict) and value.get("contract") == prep.CONTRACT and "members" in value:
                hashed_plans.append(value)
            return original_digest(value)

        with patch.object(prep.json, "loads", side_effect=loads), patch.object(prep.planning, "digest", side_effect=digest):
            result = prep.enqueue_pending(self.db, active=ACTIVE, at=AT)
        self.assertEqual(result["created"], 8)
        self.assertEqual(self.policy.call_count, 1)
        self.policy.assert_called_with(self.db, at=AT, use_planning_cache=False)
        self.assertEqual(len(decoded_plans), 1)
        # One creation digest and one validation digest, independent of N.
        self.assertEqual(len(hashed_plans), 2)
        self.assertEqual(len(decoded_plans[0]["members"]), 8)
        self.assertIsNone(prep._PLANNING_VALIDATION.get())
        self.policy.reset_mock()
        before = self.db.total_changes
        self.assertEqual(prep.enqueue_pending(self.db, active=ACTIVE, at=AT)["created"], 0)
        self.assertEqual(self.db.total_changes, before)
        self.assertEqual(self.policy.call_count, 1)  # a new pass verifies again

    def test_existing_blocked_work_reconsideration_verifies_once_across_plans(self):
        self.submit(2)
        first = prep.enqueue_pending(self.db, active=ACTIVE, at=AT)
        self.submit(3)
        second = prep.enqueue_pending(self.db, active=ACTIVE, at=AT)
        self.assertNotEqual(first["plan_id"], second["plan_id"])
        self.assertEqual((first["created"], second["created"]), (2, 1))
        self.policy.reset_mock()
        original_loads, original_digest = json.loads, prep.planning.digest
        decoded_plans, hashed_plans = [], []

        def loads(value, *args, **kwargs):
            result = original_loads(value, *args, **kwargs)
            if isinstance(result, dict) and result.get("contract") == prep.CONTRACT and "members" in result:
                decoded_plans.append(result)
            return result

        def digest(value):
            if isinstance(value, dict) and value.get("contract") == prep.CONTRACT and "members" in value:
                hashed_plans.append(value)
            return original_digest(value)

        with patch.object(prep.json, "loads", side_effect=loads), patch.object(prep.planning, "digest", side_effect=digest):
            result = capture_runtime._plan_due(self.db, {"id": second["plan_id"], "cohort": []}, at=AT)
        self.assertEqual(result["created"], 0)
        self.assertEqual(result["reconsidered"], 3)
        self.assertEqual(self.policy.call_count, 1)
        self.assertEqual(len(decoded_plans), 2)
        self.assertEqual(len(hashed_plans), 2)
        self.assertIsNone(prep._PLANNING_VALIDATION.get())
        self.policy.reset_mock()
        idle = capture_runtime._plan_due(self.db, {"id": second["plan_id"], "cohort": []}, at=AT)
        self.assertEqual(idle["reconsidered"], 0)
        self.assertEqual(self.policy.call_count, 0)
        # The same blocked work on a later pass obtains a new policy proof.
        later = capture_runtime._plan_due(self.db, {"id": second["plan_id"], "cohort": []}, at="2026-09-12T03:06:00Z")
        self.assertEqual(later["reconsidered"], 3)
        self.assertEqual(self.policy.call_count, 1)

    def test_business_activity_matches_original_sql_for_states_dates_and_nulls(self):
        self.queue()
        assignment = self.db.execute("SELECT assignment_id FROM capture_work_items LIMIT 1").fetchone()[0]
        self.db.execute("UPDATE capture_work_items SET due_at='2099-01-01T00:00:00Z'")
        for account_id in (1, 2):
            self.db.execute("INSERT INTO accounts(id,phone,created_at,updated_at) VALUES(?,?,?,?)", (account_id, "", AT, AT))
        for cid in range(1, 18):
            self.db.execute("""INSERT INTO content_items(id,link_id,platform,canonical_url,account_id,
                content_type,published_at,imported_at,created_at,updated_at) VALUES(?,?,?,?,?,'video',?,?,?,?)""",
                (cid, f"f{cid:05d}", "kuaishou" if cid == 17 else "douyin", f"https://example.invalid/{cid}",
                    2 if cid == 16 else 1, AT, AT, AT, AT))
        tasks = [(2, "queued", None), (3, "running", None), (4, "cancel_requested", None),
            (5, "succeeded", "2026-09-11T16:00:00Z"), (6, "failed", "2026-09-11T15:59:59Z"),
            (7, "succeeded", None), (14, "cancelled", "2026-09-12T04:00:00Z")]
        for cid, state, completed in tasks:
            self.db.execute("""INSERT INTO report_tasks(id,task_type,name,period_start,period_end,creation_source,
                task_status,created_at,updated_at,completed_at) VALUES(?,'daily','fixture',?,?,'manual',?,?,?,?)""",
                (str(cid), AT, AT, state, AT, AT, completed))
            # The original rule does not filter inclusion_status.
            self.db.execute("INSERT INTO task_contents(task_id,content_id,inclusion_status) VALUES(?,?,'excluded_other')", (str(cid), cid))
        manual = [(8, "runnable", None, "manual_update"), (9, "terminal", "2026-09-11T16:00:00Z", "manual_update"),
            (10, "terminal", "2026-09-11T15:59:59Z", "manual_update"), (11, "runnable", None, "metrics_update"),
            (12, "runnable", None, None), (13, "terminal", "2026-09-12T00:00:00+08:00", "manual_update"),
            (15, "terminal", "2026-09-11T23:59:59+08:00", "manual_update"), (None, "runnable", None, "manual_update")]
        for index, (cid, state, completed, kind) in enumerate(manual):
            self.db.execute("""INSERT INTO capture_work_items(work_identity,assignment_id,content_id,provider,
                operation,due_at,data_business_day,state,envelope_json,created_at,updated_at,completed_at)
                VALUES(?,?,?,'tikhub','douyin_video_statistics','2099-01-01T00:00:00Z','2026-09-12',?,?,?,?,?)""",
                (prep.planning.digest({"fixture": index}), assignment, cid, state, json.dumps({"kind": kind}), AT, AT, completed))
        day_start = "2026-09-11T16:00:00Z"
        expected = {row["id"]: bool(row["business_active"]) for row in self.db.execute("""
            SELECT c.id,(EXISTS(SELECT 1 FROM task_contents tc JOIN report_tasks t ON t.id=tc.task_id
            WHERE tc.content_id=c.id AND (t.task_status IN ('queued','running','cancel_requested')
            OR julianday(t.completed_at)>=julianday(?))) OR EXISTS(SELECT 1 FROM capture_work_items w
            WHERE w.content_id=c.id AND json_extract(w.envelope_json,'$.kind')='manual_update'
            AND (w.state!='terminal' OR julianday(w.completed_at)>=julianday(?)))) business_active
            FROM content_items c WHERE c.account_id=1 AND c.platform='douyin'""", (day_start, day_start))}
        actual = {}
        def metric_groups(_connection, _plan, _member, content, **kwargs):
            actual[content["id"]] = kwargs["business_active"]
            return 0
        plan = {"id": 1, "cohort": [{"account_id": 1, "platform": "douyin", "interval_minutes": 60, "phase_seconds": 0}]}
        with patch.object(capture_runtime, "_discovery_pending", return_value=True), \
                patch.object(capture_runtime, "_enqueue", return_value=0), \
                patch.object(capture_runtime, "_enqueue_metric_groups", side_effect=metric_groups), \
                patch.object(capture_runtime, "within_automatic_scope", return_value=True), \
                patch.object(capture_runtime.planning, "refresh_interval", return_value=(3600, None)):
            capture_runtime._plan_due(self.db, plan, at=AT)
        self.assertEqual(actual, expected)
        self.assertEqual({cid for cid, active in actual.items() if active}, {2, 3, 4, 5, 8, 9, 13, 14})
        self.assertEqual(len(actual), 15)  # other account/platform remain outside the cohort

    def test_retry_attempt_query_preserves_direct_and_unique_fallback_semantics(self):
        syntax = ast.parse(inspect.getsource(prep._retry_evidence))
        assignment = next(node for node in syntax.body[0].body if isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "attempt" for target in node.targets))
        optimized = ast.literal_eval(assignment.value.func.value.args[0])
        original = """SELECT a.*,s.status slot_status,s.id fetch_slot_id
            FROM fetch_slots s JOIN fetch_attempts a ON COALESCE(a.slot_id,
            (SELECT CASE WHEN count(*)=1 THEN min(d.fetch_slot_id) END
             FROM paid_provider_dispatch_events d WHERE d.fetch_attempt_id=a.id AND d.event_type='send_marked'))=s.id
            WHERE s.intake_request_id=? AND s.stage='profile_prepare' AND s.window_key=?
            ORDER BY a.attempt_number DESC,a.id DESC LIMIT 1"""
        db = sqlite3.connect(":memory:")
        self.addCleanup(db.close)
        db.executescript("""
            CREATE TABLE fetch_slots(id INTEGER PRIMARY KEY,intake_request_id INTEGER,stage TEXT,window_key TEXT,status TEXT);
            CREATE TABLE fetch_attempts(id INTEGER PRIMARY KEY,slot_id INTEGER,attempt_number INTEGER);
            CREATE TABLE paid_provider_dispatch_events(id INTEGER PRIMARY KEY,fetch_attempt_id INTEGER,fetch_slot_id INTEGER,event_type TEXT);
            INSERT INTO fetch_slots VALUES(1,7,'profile_prepare','fixture','retryable_failed');
            INSERT INTO fetch_slots VALUES(2,8,'profile_prepare','fixture','succeeded');
            INSERT INTO fetch_slots VALUES(3,7,'profile_prepare','other','succeeded');
            INSERT INTO fetch_slots VALUES(4,7,'detail','fixture','succeeded');
        """)
        cases = [
            ("direct_wins_conflicting_dispatch", [(1,1,1)], [(1,1,2,"send_marked")], 1),
            ("nonnull_other_slot_cannot_fallback", [(1,2,1)], [(1,1,1,"send_marked")], None),
            ("nonnull_zero_cannot_fallback", [(1,0,1)], [(1,1,1,"send_marked")], None),
            ("unique_fallback", [(1,None,1)], [(1,1,1,"send_marked")], 1),
            ("two_same_slot_sends_rejected", [(1,None,1)], [(1,1,1,"send_marked"),(2,1,1,"send_marked")], None),
            ("two_different_slot_sends_rejected", [(1,None,1)], [(1,1,1,"send_marked"),(2,1,2,"send_marked")], None),
            ("nonnull_and_null_send_count_is_two", [(1,None,1)], [(1,1,1,"send_marked"),(2,1,None,"send_marked")], None),
            ("single_null_send_has_no_slot", [(1,None,1)], [(1,1,None,"send_marked")], None),
            ("other_event_does_not_count", [(1,None,1)], [(1,1,1,"send_marked"),(2,1,2,"closed")], 1),
            ("attempt_number_then_id_order", [(1,1,2),(2,None,2),(3,1,1),(4,1,None)], [(1,2,1,"send_marked")], 2),
            ("null_attempt_number_order", [(1,1,None),(2,None,None)], [(1,2,1,"send_marked")], 2),
            ("window_stage_and_intake_filter", [(1,2,9),(2,3,9),(3,4,9),(4,1,1)], [], 4),
            ("no_attempt", [], [], None),
        ]
        for label, attempts, dispatches, expected_id in cases:
            with self.subTest(label=label):
                db.execute("DELETE FROM fetch_attempts")
                db.execute("DELETE FROM paid_provider_dispatch_events")
                db.executemany("INSERT INTO fetch_attempts VALUES(?,?,?)", attempts)
                db.executemany("INSERT INTO paid_provider_dispatch_events VALUES(?,?,?,?)", dispatches)
                before = db.execute(original, (7,"fixture")).fetchone()
                after = db.execute(optimized, (7,"fixture")).fetchone()
                self.assertEqual(after, before)
                self.assertEqual(None if after is None else after[0], expected_id)

    def test_nonplanning_and_payment_validation_always_fresh(self):
        self.queue()
        envelope = self.envelope()
        scope = self.scope(envelope)
        self.policy.reset_mock()
        for for_payment in (False, False, True, True):
            self.assertEqual(prep.validate_paid_target(self.db, scope, at=AT, for_payment=for_payment), envelope["request"])
        self.assertEqual(self.policy.call_count, 4)
        with self.validation(envelope):
            self.assertEqual(prep.validate_paid_target(self.db, scope, at=AT), envelope["request"])
            for _ in range(2):
                self.assertEqual(prep.validate_paid_target(self.db, scope, at=AT, for_payment=True), envelope["request"])
            self.assertEqual(self.policy.call_count, 6)
            self.policy.return_value = None
            with self.assertRaisesRegex(PaidScopeBlocked, "preparation_policy_unavailable"):
                prep.validate_paid_target(self.db, scope, at=AT, for_payment=True)
        self.assertEqual(self.policy.call_count, 7)
        self.assertIsNone(prep._PLANNING_VALIDATION.get())

    def test_bound_connection_time_plan_thread_and_exception_cleanup(self):
        self.queue()
        envelope = self.envelope()
        plan_id = envelope["preparation_plan_id"]
        with self.assertRaisesRegex(RuntimeError, "fixture stop"):
            with self.validation(envelope):
                self.assertIsNotNone(prep._planning_cache(self.db, at=AT, plan_id=plan_id))
                self.assertIsNone(prep._planning_cache(object(), at=AT, plan_id=plan_id))
                self.assertIsNone(prep._planning_cache(self.db, at="2026-09-12T03:00:01Z", plan_id=plan_id))
                self.assertIsNone(prep._planning_cache(self.db, at=AT, plan_id=plan_id + 1))
                # Even an explicitly copied ContextVar cannot carry authority to another thread.
                with ThreadPoolExecutor(max_workers=1) as executor:
                    copied = copy_context()
                    self.assertIsNone(executor.submit(copied.run, prep._planning_cache, self.db, at=AT, plan_id=plan_id).result())
                raise RuntimeError("fixture stop")
        self.assertIsNone(prep._PLANNING_VALIDATION.get())
        self.policy.reset_mock()
        prep.validate_paid_target(self.db, self.scope(envelope), at=AT)
        self.assertEqual(self.policy.call_count, 1)

    def test_ended_transaction_disables_and_clears_cache(self):
        self.queue()
        envelope = self.envelope()
        with self.assertRaisesRegex(ValueError, "transaction ended"):
            with self.validation(envelope):
                self.db.rollback()
                self.assertIsNone(prep._planning_cache(self.db, at=AT, plan_id=envelope["preparation_plan_id"]))
        self.assertIsNone(prep._PLANNING_VALIDATION.get())

    def test_changed_request_is_checked_again_with_cached_plan(self):
        self.queue()
        envelope = self.envelope()
        scope = self.scope(envelope)
        with self.validation(envelope):
            prep.validate_paid_target(self.db, scope, at=AT)
            self.db.execute("UPDATE account_directory_rows SET uid='987654321'")
            with self.assertRaisesRegex(PaidScopeBlocked, "preparation_input_changed"):
                prep.validate_paid_target(self.db, scope, at=AT)

    def test_gate_and_budget_readiness_are_never_cached(self):
        self.queue()
        envelope = self.envelope()

        def gate(state):
            value = {"provider": "tikhub", "operation": envelope["operation"], "state": state,
                "reason": "fixture", "evidence_json": "{}", "recorded_at": AT}
            self.db.execute("INSERT INTO capture_paid_send_gate_events(provider,operation,state,reason,evidence_json,recorded_at,event_sha256) VALUES(?,?,?,?,?,?,?)",
                (*value.values(), prep.planning.digest(value)))

        gate("open")
        with self.validation(envelope), patch("v8.work_readiness.WorkReadinessPass._base_assessment",
                side_effect=[{"runnable": True, "reason": ""}, {"runnable": False, "reason": "task_budget_exceeded"}]) as budget:
            self.assertEqual(prep.readiness(self.db, envelope, at=AT), ("runnable", ""))
            self.assertEqual(prep.readiness(self.db, envelope, at=AT), ("provider_blocked", "task_budget_exceeded"))
            gate("closed")
            self.assertEqual(prep.readiness(self.db, envelope, at=AT), ("provider_blocked", "provider_transport_blocked"))
            self.assertEqual(budget.call_count, 2)


if __name__ == "__main__":
    unittest.main()
