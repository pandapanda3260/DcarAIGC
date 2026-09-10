"""Future discovery never redrives an old uncertain paid request.

The real schema20 planner, route/readiness/slot guards and preclaim transition
run against a private database. Only activation and frozen cohort inputs are
fixtures; provider execution and all network connections are forbidden.
"""
from __future__ import annotations

import json
import socket
from types import SimpleNamespace
import threading
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from v8 import capture, capture_runtime as runtime, provider_budget, schema_v20, storage
from v8.runtime_database import (
    DatabaseAccessMode, FileIdentity, InstalledWriterContract,
    ResolvedDatabaseAccess, acquire_writer_lock,
)

AT = "2026-09-07T03:00:00Z"
OPERATION = "xiaohongshu_user_posts"


def after(*, days=0, hours=0):
    return (datetime.fromisoformat(AT.replace("Z", "+00:00"))
            + timedelta(days=days, hours=hours)).isoformat().replace("+00:00", "Z")


class DiscoveryContinuityTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve()
        self.db = self.root / "discovery.sqlite3"
        self.enterContext(patch.dict("os.environ", {
            "DCAR_TEST_DENY_FORMAL_DB": "1", "DCAR_READ_ONLY": "0",
            "DCAR_DAILY_CAPTURE_RECONCILE_FROM": "2026-09-06",
        }))
        self.network = self.enterContext(patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden")))
        self.http = self.enterContext(patch.object(runtime.providers, "_request_json", side_effect=AssertionError("HTTP forbidden")))
        self.execute = self.enterContext(patch.object(runtime, "_execute_one", side_effect=AssertionError("paid execution forbidden")))
        self.clock = AT
        self.enterContext(patch.object(runtime, "now_utc", lambda: self.clock))
        with storage.connect(self.db) as connection:
            storage.initialize_database(connection)
            schema_v20.migrate(connection)
        lock = self.root / "writer.lock"
        lock.touch(mode=0o600)
        installed = InstalledWriterContract(self.root, self.root / "fixture.plist", self.root,
            self.root / "fixture.py", self.db, lock, {})
        access = ResolvedDatabaseAccess(DatabaseAccessMode.WRITER, self.db,
            FileIdentity.from_stat(self.db.stat()), self.root, lock, installed)
        self.enterContext(acquire_writer_lock(access))
        self.active = {"activation_id": 3, "profile_id": "integrated_route_v1",
            "activation_sha256": "a" * 64, "roster_snapshot_id": 2, "roster_members_sha256": "b" * 64}
        self.member = runtime.planning.adaptive_cohorts([{
            "identity_id": 1, "account_id": 1, "platform": "xiaohongshu",
            "uid": "0123456789abcdef01234567", "enabled": 1,
            "monitoring_status": "monitored", "history_days": 7, "video_count": 1,
            "created_at": "2026-08-01T00:00:00Z", "accepted_at": "2026-08-01T00:00:00Z",
        }], business_day="2026-09-07")[0]
        with storage.connect(self.db) as connection, storage.transaction(connection):
            connection.execute("""INSERT INTO accounts(id,phone,phone_normalized,operator_name,
                account_type,content_direction,enabled,created_at,updated_at)
                VALUES(1,'',NULL,'fixture','unknown','unknown',1,?,?)""", (AT, AT))
            connection.execute("""INSERT INTO account_platform_identities(id,account_id,platform,uid,
                nickname,source,created_at,updated_at)
                VALUES(1,1,'xiaohongshu',?,'fixture','manual',?,?)""", (self.member["uid"], AT, AT))
            runtime.planning.assign_route(connection, scope_type="account", scope_key="1",
                account_id=1, provider="tikhub", operation=OPERATION, expected_generation=0,
                route="integrated", mode="active", effective_at=AT, recorded_at=AT)
            connection.execute("""INSERT INTO routing_input_changes(change_kind,payload_json,
                effective_at,recorded_at,change_sha256) VALUES('policy','{}',?,?,?)""", (AT, AT, "c" * 64))
            connection.execute("""INSERT INTO capture_paid_send_gate_events(provider,operation,state,
                reason,evidence_json,recorded_at,event_sha256)
                VALUES('tikhub',?,'open','isolated planning marker only','{}',?,?)""", (OPERATION, AT, "d" * 64))
        self.enterContext(patch.object(runtime, "activation_at", return_value=self.active))
        self.enterContext(patch.object(runtime, "_cohort_plan", side_effect=self.cohort))
        self.assertEqual(runtime.plan_tick(self.db, AT)["created"], 1)
        self.old_id = self.works()[0]["id"]
        self.hold(self.old_id, AT)
        self.old = self.works()[0]
        self.original_ledgers = self.ledgers()
        self.addCleanup(self.assert_no_provider)

    def cohort(self, connection, _active, *, at, shadow):
        local = runtime._time(at).astimezone(runtime.BEIJING)
        day = (local.date() - timedelta(days=int((local.hour, local.minute) < (0, 10)))).isoformat()
        body = {**self.active, "business_day": day, "cohort": [self.member],
                "contract_version": runtime.CONTRACT, "shadow": False}
        connection.execute("""INSERT OR IGNORE INTO capture_source_plans(roster_change_id,
            business_day,generation,mode,payload_json,created_at,plan_sha256)
            VALUES(1,?,1,'active',?,?,?)""", (day, runtime.planning.canonical(body), at, runtime.planning.digest(body)))
        plan_id = connection.execute("SELECT id FROM capture_source_plans WHERE business_day=?", (day,)).fetchone()[0]
        return {"id": plan_id, **body}

    def works(self):
        with storage.connect(self.db) as connection:
            return [dict(row) for row in connection.execute("SELECT * FROM capture_work_items ORDER BY id")]

    def ledgers(self):
        with storage.connect(self.db) as connection:
            return {name: [dict(row) for row in connection.execute(f"SELECT * FROM {name} ORDER BY id")]
                    for name in ("fetch_slots", "provider_usage", "scheduler_runs", "capture_watermarks")}

    def hold(self, work_id, at):
        self.clock = at
        with storage.connect(self.db) as connection, storage.transaction(connection):
            work = dict(connection.execute("SELECT * FROM capture_work_items WHERE id=?", (work_id,)).fetchone())
            window = runtime._page_window(json.loads(work["envelope_json"]))
            slot = capture.ensure_account_slot(connection, account_id=1, stage="discovery",
                window_key=window, provider="TikHub", adapter_version="fixture")
            connection.execute("UPDATE fetch_slots SET last_error_code='billing_unknown_retry_blocked' WHERE id=?", (slot,))
            paid_identity = runtime.planning.digest({"fixture": work_id, "window": window})
            usage = connection.execute("""INSERT INTO provider_usage(provider,operation,request_attempts,
                billed_requests,currency,amount,recorded_at,details_json)
                VALUES('TikHub',?,1,1,'USD',.001,?,?)""", (OPERATION, at, json.dumps({
                    "state": "billing_unknown", "slot_id": slot, "category": "reconcile",
                    "budget_day": runtime._business_day(at), "paid_scope_identity": paid_identity}))).lastrowid
            provider_budget.record_fault_state(connection, scope_kind="paid_identity_hold",
                paid_identity=paid_identity, fault_class="billing_unknown", reason="fixture unknown",
                usage_id=usage, at=at)
        result = runtime._run_single(self.db, at)
        self.assertEqual((result["work_id"], result["status"], result["provider_calls"]), (work_id, "paid_identity_hold", 0))

    def tick(self, at):
        self.clock = at
        return runtime.plan_tick(self.db, at)

    def enqueue(self, at, *, due=None, stage="discovery", plan_day=None):
        with storage.connect(self.db) as connection, storage.transaction(connection):
            plan = self.cohort(connection, self.active, at=at, shadow=False)
            if plan_day is not None:
                plan["business_day"] = plan_day
            due = due or "discovery:" + runtime._bucket(at, self.member["interval_minutes"] * 60, self.member["phase_seconds"])
            return runtime._enqueue(connection, plan, self.member, stage=stage,
                operation=OPERATION, logical_due=due, at=at)

    def assert_old_unchanged(self):
        self.assertEqual(self.works()[0], self.old)
        self.assertEqual(self.ledgers(), self.original_ledgers)

    def assert_no_provider(self):
        self.network.assert_not_called()
        self.http.assert_not_called()
        self.execute.assert_not_called()
        with storage.connect(self.db) as connection:
            for table in ("provider_request_start_events", "fetch_attempts", "scheduler_run_attempts", "paid_provider_dispatch_events"):
                self.assertEqual(connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0], 0)
            self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])

    def test_next_day_creates_fresh_scan_and_keeps_original_slot_hold(self):
        self.assertEqual(self.tick(after(days=1))["created"], 1)
        fresh = self.works()[1]
        old_envelope = json.loads(self.old["envelope_json"])
        new_envelope = json.loads(fresh["envelope_json"])
        self.assertNotEqual(fresh["work_identity"], self.old["work_identity"])
        self.assertEqual((fresh["state"], new_envelope["cursor"], new_envelope["raw_ids"]), ("runnable", "", []))
        self.assertNotEqual(new_envelope["logical_due"], old_envelope["logical_due"])
        self.assertGreater(new_envelope["window_end"], old_envelope["window_end"])
        with storage.connect(self.db) as connection:
            self.assertEqual(runtime._readiness(connection, old_envelope, at=after(days=1)), ("paid_identity_hold", "billing_unknown_retry_blocked"))
            self.assertEqual(runtime._readiness(connection, new_envelope, at=after(days=1)), ("runnable", ""))
        self.assert_old_unchanged()

    def test_same_bucket_and_later_same_day_still_block(self):
        for at in (AT, after(hours=1), after(hours=10)):
            with self.subTest(at=at):
                self.assertEqual(self.tick(at)["created"], 0)
                self.assertFalse(self.enqueue(at))
        self.assert_old_unchanged()

    def test_midnight_requires_new_day_plan_and_natural_phased_bucket(self):
        self.assertEqual(self.tick("2026-09-07T16:01:00Z")["created"], 0)
        self.member["interval_minutes"], self.member["phase_seconds"] = 60, 3599
        self.assertEqual(self.tick("2026-09-07T16:10:00Z")["created"], 0)
        self.assertEqual(self.tick("2026-09-07T16:59:59Z")["created"], 1)
        self.assert_old_unchanged()

    def test_future_direct_enqueue_and_planner_are_idempotent(self):
        self.assertTrue(self.enqueue(after(days=1)))
        self.assertFalse(self.enqueue(after(days=1)))
        self.assertEqual(self.tick(after(days=1))["created"], 0)
        self.assertEqual(len(self.works()), 2)
        self.assert_old_unchanged()

    def test_every_other_unfinished_state_retains_account_mutex(self):
        for state in ("runnable", "provider_blocked", "budget_deferred", "leased", "running"):
            with self.subTest(state=state), storage.connect(self.db) as connection, storage.transaction(connection):
                owner = "fixture-owner" if state in {"leased", "running"} else None
                connection.execute("UPDATE capture_work_items SET state=?,owner_token=? WHERE id=?", (state, owner, self.old_id))
                plan = self.cohort(connection, self.active, at=after(days=1), shadow=False)
                due = "discovery:" + runtime._bucket(after(days=1), self.member["interval_minutes"] * 60, self.member["phase_seconds"])
                self.assertFalse(runtime._enqueue(connection, plan, self.member, stage="discovery", operation=OPERATION, logical_due=due, at=after(days=1)))

    def test_malformed_old_dates_identity_and_future_window_fail_closed(self):
        envelope = json.loads(self.old["envelope_json"])
        cases = [
            {**envelope, "logical_due": "missing-date"},
            {key: value for key, value in envelope.items() if key != "window_end"},
            {**envelope, "window_end": "2026-09-09T03:00:00Z"},
            {**envelope, "window_start": envelope["window_end"]},
            {**envelope, "uid": "another-account"},
            {**envelope, "source_stage": "manual_update"},
            {**envelope, "logical_due": envelope["logical_due"] + "junk"},
            [],
        ]
        for malformed in cases:
            with self.subTest(malformed=malformed):
                with storage.connect(self.db) as connection, storage.transaction(connection):
                    connection.execute("UPDATE capture_work_items SET envelope_json=? WHERE id=?", (json.dumps(malformed), self.old_id))
                self.assertEqual(self.tick(after(days=1))["created"], 0)

    def test_missing_or_future_business_day_blocks(self):
        for day in ("", "2026-9-7", "2026-09-08", "2030-01-01"):
            with self.subTest(day=day):
                with storage.connect(self.db) as connection, storage.transaction(connection):
                    connection.execute("UPDATE capture_work_items SET data_business_day=? WHERE id=?", (day, self.old_id))
                self.assertEqual(self.tick(after(days=1))["created"], 0)

    def test_multiple_old_holds_allow_only_one_current_day_scan(self):
        self.assertEqual(self.tick(after(days=1))["created"], 1)
        self.hold(self.works()[1]["id"], after(days=1))
        held = self.works()
        ledgers = self.ledgers()
        self.assertEqual(self.tick(after(days=2))["created"], 1)
        self.assertEqual(self.tick(after(days=2, hours=3))["created"], 0)
        self.assertEqual(self.works()[:2], held)
        self.assertEqual(self.ledgers(), ledgers)

    def test_delayed_old_scan_held_today_cannot_spawn_another_scan_today(self):
        with storage.connect(self.db) as connection, storage.transaction(connection):
            connection.execute("UPDATE capture_work_items SET updated_at=? WHERE id=?", (after(days=1), self.old_id))
        self.assertEqual(self.tick(after(days=1, hours=3))["created"], 0)
        self.assertEqual(self.tick(after(days=2))["created"], 1)

    def test_new_operation_fault_blocks_without_reopening_old_hold(self):
        with storage.connect(self.db) as connection, storage.transaction(connection):
            provider_budget.record_fault_state(connection, scope_kind="operation", operation=OPERATION,
                fault_class="rate_limit", reason="fixture operation closed", usage_id=None, at=after(days=1))
        self.assertEqual(self.tick(after(days=1))["created"], 1)
        fresh = self.works()[1]
        self.assertEqual((fresh["state"], fresh["reason"]), ("provider_blocked", "operation_blocked"))
        self.assertEqual(self.works()[0], self.old)

    def test_no_allowance_for_non_discovery_manual_or_compensation(self):
        self.assertFalse(self.enqueue(after(days=1), stage="account_metrics"))
        envelope = json.loads(self.old["envelope_json"])
        for extra in ({"kind": "manual_update"}, {"compensation": {}}, {"compensation": None}):
            with self.subTest(extra=extra):
                with storage.connect(self.db) as connection, storage.transaction(connection):
                    connection.execute("UPDATE capture_work_items SET envelope_json=? WHERE id=?", (json.dumps({**envelope, **extra}), self.old_id))
                self.assertEqual(self.tick(after(days=1))["created"], 0)

    def test_caller_cannot_invent_future_due_or_relabel_old_plan(self):
        for due in ("discovery:" + after(days=1, hours=1), "discovery:" + after(days=365), "made-up"):
            with self.subTest(due=due):
                self.assertFalse(self.enqueue(after(days=1), due=due))
        self.assertFalse(self.enqueue(after(days=1), plan_day="2026-09-07"))
        self.assert_old_unchanged()

    def test_year_later_is_new_work_not_a_retry_of_the_held_request(self):
        self.assertEqual(self.tick(after(days=365))["created"], 1)
        self.assert_old_unchanged()
        fresh = self.works()[1]
        self.assertEqual(fresh["attempt_count"], 0)
        self.assertEqual(fresh["data_business_day"], "2027-09-07")

    def test_new_planner_window_ignores_old_watermark_without_rewriting_held_scan(self):
        with storage.connect(self.db) as connection, storage.transaction(connection):
            connection.execute("""INSERT INTO capture_watermarks(provider,operation,scope_key,work_id,
                complete_through,cursor_json,evidence_json,recorded_at)
                VALUES('tikhub',?,?,?,'2026-08-01T03:00:00.000000Z','null','{}',?)""",
                (OPERATION, f"xiaohongshu:{self.member['uid']}", self.old_id, AT))
        before = self.ledgers()
        self.assertEqual(self.tick(after(days=1))["created"], 1)
        envelope = json.loads(self.works()[1]["envelope_json"])
        self.assertEqual(runtime._time(envelope["window_end"]) - runtime._time(envelope["window_start"]), timedelta(hours=72))
        self.assertEqual(self.ledgers(), before)
        self.assertEqual(self.works()[0], self.old)

    def test_competing_planners_create_only_one_new_account_scan(self):
        results, failures = [], []
        start = threading.Barrier(2)
        def enqueue():
            try:
                start.wait(timeout=3)
                results.append(self.enqueue(after(days=1)))
            except BaseException as error:
                failures.append(error)
        threads = [threading.Thread(target=enqueue) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
        self.assertEqual(failures, [])
        self.assertEqual(sorted(results), [False, True])
        self.assertEqual(len(self.works()), 2)
        self.assert_old_unchanged()

    def test_partial_cap_or_loop_blocks_same_day_only_and_keeps_evidence(self):
        for reason in ("page_cap_hit", "cursor_loop"):
            with self.subTest(reason=reason):
                with storage.connect(self.db) as connection, storage.transaction(connection):
                    connection.execute("UPDATE capture_work_items SET state='terminal',reason=?,completed_at=? WHERE id=?",
                        (reason, AT, self.old_id))
                self.assertEqual(self.tick(after(hours=3))["created"], 0)
                self.assertEqual(self.tick(after(days=1))["created"], int(reason == "page_cap_hit"))
                self.assertEqual(self.works()[0]["reason"], reason)
        self.assertEqual(self.ledgers(), self.original_ledgers)

    def test_comments_only_for_active_business_and_manual_targets_preserved(self):
        with storage.connect(self.db) as connection, storage.transaction(connection):
            for identifier in (1, 2):
                connection.execute("""INSERT INTO content_items(id,link_id,platform,platform_content_id,
                    canonical_url,account_id,content_type,published_at,imported_at,created_at,updated_at)
                    VALUES(?,?,'xiaohongshu',?,?,1,'video',?,?,?,?)""",
                    (identifier, f"test0{identifier}", str(identifier), f"https://example.invalid/{identifier}", AT, AT, AT, AT))
            connection.execute("""INSERT INTO report_tasks(id,task_type,name,period_start,period_end,
                creation_source,task_status,created_at,updated_at)
                VALUES('active','daily','fixture',?,?,'manual','running',?,?)""", (AT, AT, AT, AT))
            connection.execute("INSERT INTO task_contents(task_id,content_id,inclusion_status) VALUES('active',2,'included')")
        queued = []
        def enqueue(_connection, _plan, _member, **kwargs):
            queued.append((kwargs["stage"], kwargs.get("content")["id"] if kwargs.get("content") else None))
            return False
        with patch.object(runtime, "_enqueue", side_effect=enqueue), patch.object(runtime, "canonical_content_predicate", return_value="1"):
            self.tick(AT)
        self.assertEqual([identifier for stage, identifier in queued if stage == "comments"], [2])
        with storage.connect(self.db) as connection, patch.object(runtime, "canonical_content_predicate", return_value="1"):
            manual = runtime.manual_work_spec(connection, content_id=1, kind="manual_update", at=AT)
        self.assertIn("comments", [target["stage"] for target in manual["targets"]])


class ForwardWindowAndPageTest(unittest.TestCase):
    def test_forward_only_window_is_72h_while_legacy_behavior_is_unchanged(self):
        start, end = runtime.planning.discovery_window(at=AT, complete_through="2026-07-01T00:00:00Z", forward_only=True)
        self.assertEqual(runtime._time(end) - runtime._time(start), timedelta(hours=72))
        old_start, old_end = runtime.planning.discovery_window(at=AT, complete_through="2026-07-01T00:00:00Z")
        self.assertEqual(runtime._time(old_end) - runtime._time(old_start), timedelta(days=30))
        with self.assertRaises(ValueError):
            runtime.planning.discovery_window(at=AT, complete_through=after(days=1), forward_only=True)

    def envelope(self):
        start, end = runtime.planning.discovery_window(at=AT, complete_through=None, forward_only=True)
        return {"account_id": 1, "identity_id": 1, "platform": "douyin", "uid": "123",
            "operation": "douyin_user_posts", "stage": "discovery", "logical_due": "discovery:"+AT,
            "window_start": start, "window_end": end, "cursor": 0, "page_count": 0,
            "counts": {"seen": 0, "valid": 0, "missing": 0, "invalid": 0, "unavailable": 0},
            "raw_ids": [], "seen_cursors": []}

    def page(self, envelope, *, items=(), next_cursor=1):
        raw = SimpleNamespace(raw_response_id=envelope["page_count"]+1, captured_at=AT)
        with patch.object(runtime.providers, "discover_account_content", return_value={"status":"succeeded","provider_cost":0}), patch.object(
            runtime, "_raw_for_page", return_value=raw
        ), patch.object(runtime.tikhub_scan, "_page", return_value=(list(items), True, next_cursor, None)), patch.object(
            runtime, "_verify_raws"
        ):
            return runtime._discovery_page(envelope, db_path=Path("/unused-fixture-db"), at=AT)

    def test_old_pages_without_ordering_proof_do_not_early_stop(self):
        envelope = self.envelope()
        old = {"aweme_id": "1234567890", "create_time": int(runtime._time("2026-08-01T00:00:00Z").timestamp()),
               "video": {"play_addr": {"url_list": ["https://example.invalid/video"]}}}
        first = self.page(envelope, items=[old], next_cursor=1)
        second = self.page(first["envelope"], items=[old], next_cursor=2)
        self.assertEqual(second["envelope"]["counts"]["valid"], 2)
        self.assertTrue(first["continuation"])
        self.assertTrue(second["continuation"])
        self.assertFalse(second["complete"])
        self.assertEqual(second["envelope"]["video_inventory"], {})

    def test_received_cap_and_loop_pages_are_partial_not_complete(self):
        envelope = self.envelope()
        envelope.update(page_count=31, cursor=31, raw_ids=list(range(1, 32)),
                        seen_cursors=[runtime.planning.canonical(value) for value in range(31)])
        cap = self.page(envelope, next_cursor=32)
        loop = self.page(self.envelope(), next_cursor=0)
        for result, reason in ((cap, "page_cap_hit"), (loop, "cursor_loop")):
            self.assertEqual(result["reason"], reason)
            self.assertTrue(result["terminal_partial"])
            self.assertFalse(result["complete"])
            self.assertFalse(result["continuation"])
            self.assertFalse(result["evidence"]["complete"])
            self.assertTrue(result["evidence"]["all_raw_verified"])


class PartialWorkerTest(unittest.TestCase):
    def test_partial_terminal_never_advances_watermark_or_schedules_retry(self):
        from tests import test_v8_capture_worker_backoff as fixtures
        for reason in ("page_cap_hit", "cursor_loop"):
            with self.subTest(reason=reason):
                fixture = fixtures.CaptureWorkerBackoffTest(methodName="runTest")
                fixture.setUp()
                try:
                    envelope = {**fixture.envelope, "stage": "discovery", "platform": "douyin", "uid": "123"}
                    with storage.connect(fixture.db) as connection, storage.transaction(connection):
                        connection.execute("UPDATE capture_work_items SET envelope_json=? WHERE id=?",
                            (json.dumps(envelope), fixture.work_id))
                    result = {"complete": False, "continuation": False, "terminal_partial": True,
                        "envelope": envelope, "reason": reason, "provider_cost": 0,
                        "evidence": {"complete": False, "all_raw_verified": True, "terminal_cursor": False,
                            "cap_hit": reason == "page_cap_hit", "cursor_loop": reason == "cursor_loop",
                            "raw_response_ids": []}}
                    with patch.object(runtime, "_execute_one", return_value=result) as execute:
                        outcome = runtime._run_single(fixture.db, fixtures.AT)
                        self.assertEqual(runtime._run_single(fixture.db, fixtures.AT)["status"], "idle")
                    self.assertEqual(execute.call_count, 1)
                    self.assertEqual((outcome["status"], outcome["complete"], outcome["reason"]), ("terminal", False, reason))
                    self.assertIsNotNone(fixture.work()["completed_at"])
                    with storage.connect(fixture.db) as connection:
                        self.assertEqual(connection.execute("SELECT count(*) FROM capture_watermarks").fetchone()[0], 0)
                        row = connection.execute("SELECT status,details_json FROM scheduler_runs").fetchone()
                        self.assertEqual(row[0], "failed")
                        details = json.loads(row[1])
                        self.assertNotIn("next_resume_at", details)
                        self.assertEqual(details["summary"]["disposition"], "partial")
                        self.assertFalse(details["checkpoint"]["complete"])
                        self.assertFalse(json.loads(connection.execute("SELECT payload_json FROM data_quality_receipts").fetchone()[0])["complete"])
                    fixture.assert_no_provider()
                finally:
                    fixture.doCleanups()


if __name__ == "__main__":
    unittest.main()
