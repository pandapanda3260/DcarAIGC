"""Bounded discovery recovery uses real plans, state events and SQLite evidence.

Only provider calls/readiness are fixtures. No production database, paid send,
network request, media processing or analysis runs in these tests.
"""
from __future__ import annotations

import copy
import json
import socket
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from v8 import account_states, capture_discovery_recovery as recovery
from v8 import capture_runtime as runtime, storage

AT = "2026-09-14T03:00:00Z"
ADMITTED = "2026-08-01T03:00:00Z"
UID = "99887766"
OPERATION = "douyin_user_posts"


def stamp(value):
    return runtime.planning.timestamp(value)


def shifted(value=AT, *, days=0, hours=0):
    return stamp((runtime._time(value) + timedelta(days=days, hours=hours)).isoformat())


class DiscoveryRecoveryTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.db = self.root / "recovery.sqlite3"
        self.c = storage.connect(self.db)
        self.addCleanup(self.c.close)
        self.enterContext(patch.dict("os.environ", {
            "DCAR_TEST_DENY_FORMAL_DB": "1", "DCAR_READ_ONLY": "0",
            "DCAR_DAILY_CAPTURE_RECONCILE_FROM": "2026-07-01",
        }))
        self.network = self.enterContext(patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden")))
        self.http = self.enterContext(patch.object(runtime.providers, "_request_json", side_effect=AssertionError("HTTP forbidden")))
        storage.initialize_database(self.c, target_version=23)
        self.member = runtime.planning.adaptive_cohorts([{
            "identity_id": 1, "account_id": 1, "platform": "douyin", "uid": UID,
            "enabled": 1, "monitoring_status": "monitored", "history_days": 7,
            "video_count": 1, "created_at": "2026-07-01T00:00:00Z",
            "accepted_at": "2026-07-01T00:00:00Z",
        }], business_day="2026-09-14")[0]
        with storage.transaction(self.c):
            self.c.execute("""INSERT INTO accounts(id,phone,phone_normalized,
                operator_name,enabled,created_at,updated_at)
                VALUES(1,'',NULL,'fixture',1,?,?)""", (ADMITTED, ADMITTED))
            self.c.execute("""INSERT INTO account_platform_identities(id,account_id,
                platform,uid,nickname,source,created_at,updated_at)
                VALUES(1,1,'douyin',?,'fixture','manual',?,?)""", (UID, ADMITTED, ADMITTED))
            self.c.execute("""INSERT INTO routing_input_changes(change_kind,payload_json,
                effective_at,recorded_at,change_sha256) VALUES('policy','{}',?,?,?)""",
                (ADMITTED, ADMITTED, "c" * 64))
            assignment = runtime.planning.assign_route(self.c, scope_type="account", scope_key="1",
                account_id=1, provider="tikhub", operation=OPERATION, expected_generation=0,
                route="integrated", mode="active", effective_at=ADMITTED, recorded_at=ADMITTED)
            self.assignment_id = assignment
            self.old_plan = self.plan(ADMITTED)
            self.current_plan = self.plan(AT)
        self.addCleanup(self.assert_isolated)

    def assert_isolated(self):
        self.network.assert_not_called()
        self.http.assert_not_called()
        for table in ("provider_usage", "provider_request_start_events", "evaluation_versions"):
            self.assertEqual(self.c.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], 0)
        self.assertEqual(self.c.execute("PRAGMA foreign_key_check").fetchall(), [])

    def plan(self, at, *, member=None):
        day = runtime._business_day(at)
        body = {"contract_version": runtime.CONTRACT, "shadow": False,
            "business_day": day, "activation_id": 3, "profile_id": "integrated_route_v1",
            "activation_sha256": "a" * 64, "roster_snapshot_id": 2,
            "roster_members_sha256": "b" * 64, "cohort": [member or self.member]}
        generation = self.c.execute("SELECT COUNT(*)+1 FROM capture_source_plans").fetchone()[0]
        identifier = self.c.execute("""INSERT INTO capture_source_plans(roster_change_id,
            business_day,generation,mode,payload_json,created_at,plan_sha256)
            VALUES(1,?,?,'active',?,?,?)""", (day, generation,
                runtime.planning.canonical(body), stamp(at), runtime.planning.digest(body))).lastrowid
        return {"id": identifier, **body}

    def window(self, at=AT, *, plan=None, member=None):
        plan = plan or self.current_plan
        member = member or self.member
        scopes = recovery.prepare_scopes(self.c, plan, at=at)
        return recovery.freeze_window(self.c, member, at=at, scope=scopes[member["identity_id"]])

    def envelope(self, window, *, at=AT):
        return {**self.member, **window, "contract_version": runtime.CONTRACT,
            "stage": "discovery", "capture_stage": "discovery", "source_stage": "discovery",
            "category": "reconcile", "content_id": None, "operation": OPERATION,
            "logical_due": "discovery:" + stamp(at), "source_plan_id": self.current_plan["id"],
            "assignment_id": self.assignment_id, "cursor": 0, "page_count": 0,
            "raw_ids": [], "seen_cursors": [],
            "counts": {"seen": 0, "valid": 0, "missing": 0, "invalid": 0, "unavailable": 0}}

    def work(self, envelope, *, at=AT, state="terminal", reason=""):
        identity = runtime.planning.digest({"provider": "tikhub", "operation": OPERATION,
            "subject": "account:1", "logical_due": envelope["logical_due"]})
        identifier = self.c.execute("""INSERT INTO capture_work_items(work_identity,assignment_id,
            source_plan_id,account_id,provider,operation,due_at,data_business_day,state,
            reason,envelope_json,created_at,updated_at,completed_at)
            VALUES(?,?,?,1,'tikhub',?,?,?,?,?,?,?,?,?)""",
            (identity, self.assignment_id, self.current_plan["id"], OPERATION, stamp(at),
                runtime._business_day(at), state, reason, runtime.planning.canonical(envelope),
                stamp(at), stamp(at), stamp(at) if state == "terminal" else None)).lastrowid
        return identifier

    def complete(self, start, end, *, mode="incremental"):
        window = {"window_start": stamp(start), "window_end": stamp(end)}
        if mode is not None:
            window.update(recovery_contract="discovery-recovery-v1", discovery_mode=mode,
                published_intervals=[[stamp(start), stamp(end)]])
        env = self.envelope(window, at=end)
        work_id = self.work(env, at=end)
        evidence = {**env, **env["counts"], "complete": True, "terminal_cursor": True,
            "all_raw_verified": True, "cap_hit": False, "cursor_loop": False,
            "raw_response_ids": [1], "inventory_contract": "verified-provider-scan-inventory-v1"}
        runtime.planning.advance_watermark(self.c, work_id=work_id, scope_key="douyin:" + UID,
            complete_through=end, evidence=evidence, recorded_at=end)
        return work_id

    def mark_daily(self, *, at=AT, reason="page_cap_hit"):
        env = self.envelope(self.window(at), at=at)
        self.assertEqual(env["discovery_mode"], "daily_recheck")
        return self.work(env, at=at, reason=reason)

    def test_first_daily_rechecks_30_days_even_after_a_newer_legacy_watermark(self):
        with storage.transaction(self.c):
            self.complete(shifted(days=-4), shifted(days=-1), mode=None)
            window = self.window()
        self.assertEqual(window["discovery_mode"], "daily_recheck")
        self.assertEqual(window["window_start"], shifted(days=-30))
        self.assertEqual(window["window_end"], stamp(AT))
        self.assertEqual(window["published_intervals"], [[shifted(days=-30), stamp(AT)]])
        self.assertEqual(window["bounded_out_gaps"], [[stamp(ADMITTED), shifted(days=-30)]])

    def test_global_start_and_exact_account_admission_clip_daily_history(self):
        with patch.dict("os.environ", {"DCAR_DAILY_CAPTURE_RECONCILE_FROM": "2026-09-10"}):
            window = self.window()
        self.assertEqual(window["window_start"], stamp("2026-09-09T16:00:00Z"))
        self.assertEqual(window["bounded_out_gaps"], [])
        # A new platform identity must not inherit this account's previous UID history.
        new_member = {**self.member, "uid": "88776655"}
        with storage.transaction(self.c):
            new_plan = self.plan(shifted(hours=-1), member=new_member)
            self.c.execute("UPDATE account_platform_identities SET uid=? WHERE id=1", (new_member["uid"],))
            new = self.window(plan=new_plan, member=new_member)
        self.assertEqual(new["window_start"], shifted(hours=-1))
        self.assertEqual(new["bounded_out_gaps"], [])
        at_admission = self.window(shifted(hours=-1), plan=new_plan, member=new_member)
        self.assertEqual(at_admission["published_intervals"], [])

    def test_disabled_period_is_excluded_and_operating_paused_tag_is_not_a_switch(self):
        disabled, resumed = shifted(days=-10), shifted(days=-5)
        with storage.transaction(self.c):
            for enabled, at in ((False, disabled), (True, resumed)):
                account_states.set_account_enabled_in_transaction(self.c, 1,
                    enabled=enabled, effective_at=at, created_at=at,
                    actor="fixture", reason="explicit capture switch")
            tagged = {**self.member, "monitoring_status": "paused"}
            window = self.window(member=tagged)
        self.assertEqual(window["published_intervals"], [
            [shifted(days=-30), disabled], [resumed, stamp(AT)]])

    def test_same_day_uses_72_hours_after_recent_complete_scan(self):
        with storage.transaction(self.c):
            self.complete(shifted(days=-4), shifted(days=-1))
            self.mark_daily()
            window = self.window(shifted(hours=1))
        self.assertEqual(window["discovery_mode"], "incremental")
        self.assertEqual(window["window_start"], shifted(hours=1, days=-3))

    def test_same_day_recovers_from_success_watermark_with_72_hour_overlap(self):
        with storage.transaction(self.c):
            self.complete(shifted(days=-10), shifted(days=-7))
            self.mark_daily()
            window = self.window(shifted(hours=1))
        self.assertEqual(window["discovery_mode"], "restart_gap")
        self.assertEqual(window["window_start"], shifted(days=-10))

    def test_30_day_cap_retains_only_real_uncovered_debt(self):
        with storage.transaction(self.c):
            self.complete(ADMITTED, "2026-08-05T03:00:00Z")
            self.complete("2026-08-08T03:00:00Z", "2026-08-10T03:00:00Z")
            self.mark_daily()
            window = self.window(shifted(hours=1))
        self.assertEqual(window["discovery_mode"], "restart_gap")
        self.assertEqual(window["window_start"], shifted(days=-30, hours=1))
        self.assertEqual(window["bounded_out_gaps"], [
            [stamp("2026-08-05T03:00:00Z"), stamp("2026-08-08T03:00:00Z")],
            [stamp("2026-08-10T03:00:00Z"), shifted(days=-30, hours=1)]])

    def test_daily_marker_survives_partial_and_does_not_claim_complete_watermark(self):
        with storage.transaction(self.c):
            work_id = self.mark_daily()
            env = json.loads(self.c.execute("SELECT envelope_json FROM capture_work_items WHERE id=?", (work_id,)).fetchone()[0])
            with self.assertRaisesRegex(ValueError, "complete verified terminal"):
                runtime.planning.advance_watermark(self.c, work_id=work_id, scope_key="douyin:" + UID,
                    complete_through=AT, evidence={"complete": False, "cap_hit": True}, recorded_at=AT)
            later = self.window(shifted(hours=1))
            self.assertNotEqual(later["discovery_mode"], "daily_recheck")
            self.assertEqual(self.c.execute("SELECT COUNT(*) FROM capture_watermarks").fetchone()[0], 0)
            self.assertEqual(env, json.loads(self.c.execute("SELECT envelope_json FROM capture_work_items WHERE id=?", (work_id,)).fetchone()[0]))
            tomorrow = self.window(shifted(days=1))
        self.assertEqual(tomorrow["discovery_mode"], "daily_recheck")

    def test_runtime_enqueue_freezes_once_and_records_uncovered_scope_without_completion(self):
        due = "discovery:" + runtime._bucket(AT, self.member["interval_minutes"] * 60,
            self.member["phase_seconds"])
        with storage.transaction(self.c), patch.object(runtime, "_readiness", return_value=("runnable", "")):
            arguments = dict(stage="discovery", operation=OPERATION, logical_due=due, at=AT)
            self.assertTrue(runtime._enqueue(self.c, self.current_plan, self.member, **arguments))
            self.assertFalse(runtime._enqueue(self.c, self.current_plan, self.member, **arguments))
        rows = self.c.execute("SELECT * FROM capture_work_items").fetchall()
        self.assertEqual(len(rows), 1)
        env = json.loads(rows[0]["envelope_json"])
        self.assertEqual(env["discovery_mode"], "daily_recheck")
        self.assertEqual(env["window_start"], shifted(days=-30))
        self.assertEqual(rows[0]["attempt_count"], 0)
        receipt = self.c.execute("SELECT payload_json FROM data_quality_receipts WHERE scope_key=?",
            (f"capture-discovery-scope:{rows[0]['id']}",)).fetchone()
        evidence = json.loads(receipt[0])
        self.assertFalse(evidence["complete"])
        self.assertEqual(evidence["disposition"], "planned")
        self.assertEqual(evidence["bounded_out_gaps"], [[stamp(ADMITTED), shifted(days=-30)]])
        alerts = self.c.execute("SELECT scope_json FROM operational_alerts").fetchall()
        self.assertEqual(len(alerts), 1)
        self.assertEqual(json.loads(alerts[0][0])["window_start"], stamp(ADMITTED))
        self.assertEqual(self.c.execute("SELECT COUNT(*) FROM capture_watermarks").fetchone()[0], 0)
        # The next natural day gets new work; the existing open debt grows and
        # the first immutable planned receipt stays byte-for-byte unchanged.
        tomorrow = shifted(days=1)
        with storage.transaction(self.c), patch.object(runtime, "_readiness", return_value=("runnable", "")):
            self.c.execute("UPDATE capture_work_items SET state='terminal',reason='page_cap_hit',completed_at=? WHERE id=?",
                (stamp(AT), rows[0]["id"]))
            plan = self.plan(tomorrow)
            due = "discovery:" + runtime._bucket(tomorrow, self.member["interval_minutes"] * 60,
                self.member["phase_seconds"])
            self.assertTrue(runtime._enqueue(self.c, plan, self.member, stage="discovery",
                operation=OPERATION, logical_due=due, at=tomorrow))
        alerts = self.c.execute("SELECT scope_json,evidence_json FROM operational_alerts").fetchall()
        self.assertEqual(len(alerts), 1)
        self.assertEqual(json.loads(alerts[0]["scope_json"])["window_end"], shifted(days=-29))
        self.assertEqual(json.loads(alerts[0]["evidence_json"])["work_id"], 2)
        self.assertEqual(self.c.execute("SELECT payload_json FROM data_quality_receipts WHERE scope_key=?",
            (f"capture-discovery-scope:{rows[0]['id']}",)).fetchone()[0], receipt[0])
        self.assertEqual(self.c.execute("SELECT COUNT(*) FROM capture_watermarks").fetchone()[0], 0)

    def page(self, envelope, *, at=AT, more=True, next_cursor=1):
        raw = SimpleNamespace(raw_response_id=envelope["page_count"] + 1, captured_at=at)
        with patch.object(runtime.providers, "discover_account_content", return_value={"status": "succeeded", "provider_cost": 0}) as discover, \
                patch.object(runtime, "_raw_for_page", return_value=raw), \
                patch.object(runtime.tikhub_scan, "_page", return_value=([], more, next_cursor, None)), \
                patch.object(runtime, "_verify_raws"):
            result = runtime._discovery_page(envelope, db_path=self.db, at=at)
        return result, discover.call_args.kwargs

    def test_retry_and_continuation_keep_frozen_window_and_enabled_intervals(self):
        original = self.envelope(self.window())
        before = copy.deepcopy(original)
        first, args = self.page(original)
        with storage.transaction(self.c):
            account_states.set_account_enabled_in_transaction(self.c, 1, enabled=False,
                effective_at=shifted(hours=1), created_at=shifted(hours=1),
                actor="fixture", reason="switch after frozen request")
        later, retry_args = self.page(first["envelope"], at=shifted(days=2), more=False, next_cursor=None)
        self.assertTrue(later["complete"])
        for key in ("window_start", "window_end", "published_intervals", "scope_start", "bounded_out_gaps"):
            self.assertEqual(later["envelope"][key], before[key])
        self.assertEqual(original, before)
        for key in ("published_start", "published_end", "published_intervals"):
            self.assertEqual(args[key], retry_args[key])
        self.assertEqual(args["published_intervals"], [(runtime._time(start), runtime._time(end))
            for start, end in before["published_intervals"]])

    def test_32_pages_remains_partial_and_carries_the_frozen_recovery_scope(self):
        envelope = self.envelope(self.window())
        envelope.update(page_count=31, cursor=31, raw_ids=list(range(1, 32)),
            seen_cursors=[runtime.planning.canonical(value) for value in range(31)])
        result, _ = self.page(envelope, next_cursor=32)
        self.assertEqual(result["reason"], "page_cap_hit")
        self.assertTrue(result["terminal_partial"])
        self.assertFalse(result["complete"])
        self.assertFalse(result["continuation"])
        self.assertFalse(result["evidence"]["complete"])
        self.assertEqual(result["evidence"]["published_intervals"], envelope["published_intervals"])


class RecoveryMaterializationTest(unittest.TestCase):
    def setUp(self):
        from tests import test_discovery_metrics_flow as fixtures
        self.fx = fixtures.DiscoveryMetricsFlowTest(methodName="runTest")
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)
        self.intervals = [["2026-09-01T00:00:00.000000Z", "2026-09-05T00:00:00.000000Z"],
            ["2026-09-10T00:00:00.000000Z", "2026-09-12T06:00:00.000000Z"]]

    def materialize(self, page, raw_id):
        from v8 import providers
        return providers.materialize_account_discovery_page(account_id=1, platform="douyin",
            account_uid=UID, page=page, source_raw_response_id=raw_id,
            metrics_window_key="2026-09-12", discovery_operation=OPERATION,
            provider="TikHub", derived_adapter_version="discovery-derived-v1",
            derived_operations={"detail": "douyin_discovery_detail", "metrics": "douyin_discovery_metrics"},
            zero_view_is_authoritative=False, materialize_detail=False,
            materialize_existing_stages=False, db_path=self.fx.db,
            published_start=runtime._time(self.intervals[0][0]),
            published_end=runtime._time(self.intervals[-1][1]) - timedelta(microseconds=1),
            published_intervals=[(runtime._time(start), runtime._time(end)) for start, end in self.intervals],
            derived_raw_root=self.fx.root / "derived", media_root=self.fx.root / "media")

    def test_enabled_intervals_preserve_author_checks_dedup_and_latest_valid_metrics(self):
        accepted = {**self.fx.item({"comment_count": 8}), "published_at": "2026-09-10T00:00:00Z"}
        paused = {**accepted, "platform_content_id": "7500000000000000002", "published_at": "2026-09-05T00:00:00Z"}
        before = {**accepted, "platform_content_id": "7500000000000000003", "published_at": "2026-08-31T00:00:00Z"}
        wrong_author = {**accepted, "platform_content_id": "7500000000000000004", "account_uid": "another"}
        after = {**accepted, "platform_content_id": "7500000000000000005", "published_at": "2026-09-12T06:00:00Z"}
        page = {"items": [accepted, accepted, paused, before, wrong_author, after]}
        result = self.materialize(page, self.fx.raw(page, captured_at="2026-09-12T05:00:00Z"))
        with storage.connect(self.fx.db) as connection:
            rows = connection.execute("SELECT id,platform_content_id FROM content_items").fetchall()
        self.assertEqual(len(rows), 1)
        content_id = rows[0]["id"]
        self.assertEqual(rows[0]["platform_content_id"], accepted["platform_content_id"])
        self.assertIn("identity_conflict", {entry["error_code"] for entry in result["derived_stages"]["failures"]})
        lower = {"items": [self.fx.item({"comment_count": 6})]}
        raw_id = self.fx.raw(lower)
        self.materialize(lower, raw_id)
        self.assertEqual(self.fx.fields(content_id)["comment_count"]["value"], 6)
        self.assertEqual(self.fx.fields(content_id)["comment_count"]["raw_response_id"], raw_id)
        observations = self.fx.observation_count()
        self.materialize(lower, raw_id)
        self.assertEqual(self.fx.observation_count(), observations)
        missing = {"items": [self.fx.item()]}
        self.materialize(missing, self.fx.raw(missing, captured_at="2026-09-12T05:55:00Z"))
        fields = self.fx.fields(content_id)
        self.assertEqual(fields["comment_count"]["value"], 6)
        self.assertEqual(fields["comment_count"]["captured_at"], "2026-09-12T05:50:00Z")


class RecoveryDayCoverageTest(unittest.TestCase):
    def setUp(self):
        from tests import test_v8_capture_day_coverage as fixtures
        self.fixtures = fixtures
        self.fx = fixtures.CatalogDayCoverageTest(methodName="runTest")
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)

    def scope(self, intervals):
        value = {"recovery_contract": "discovery-recovery-v1", "discovery_mode": "daily_recheck",
            "published_intervals": intervals, "scope_start": self.fx.env["window_start"],
            "scope_evidence": {"admission": {"kind": "active_capture_source_plan", "id": 1}},
            "bounded_out_gaps": []}
        self.fx.env.update(value)
        self.fx.evidence.update(value)
        self.fx.c.execute("UPDATE capture_work_items SET envelope_json=? WHERE id=1", (json.dumps(self.fx.env),))
        self.fx.write_receipts()

    def test_pause_or_admission_inside_day_never_claims_complete_day_coverage(self):
        for intervals in (
            [[self.fixtures.LOWER, "2026-09-10T02:00:00Z"],
             ["2026-09-10T03:00:00Z", self.fixtures.UPPER]],
            [["2026-09-10T03:00:00Z", self.fixtures.UPPER]],
        ):
            with self.subTest(intervals=intervals):
                self.scope(intervals)
                result = self.fx.result()
                self.assertTrue(result["known"])
                self.assertFalse(result["complete"])
                self.assertEqual(result["covered_identity_ids"], [])
                self.assertIn("catalog_day_outside_admitted_intervals", result["scan_errors"].values())

    def test_full_enabled_day_is_provable_and_binding_detects_later_scope_change(self):
        from v8 import capture_day_coverage as coverage
        self.scope([[self.fixtures.LOWER, self.fixtures.UPPER]])
        result = self.fx.result()
        self.assertTrue(result["complete"], result)
        self.assertTrue(coverage.validate_source_binding(self.fx.c, result["source_binding"],
            self.fixtures.CUTOFF)["valid"])
        self.fx.env["published_intervals"] = [["2026-09-10T03:00:00Z", self.fixtures.UPPER]]
        self.fx.c.execute("UPDATE capture_work_items SET envelope_json=? WHERE id=1", (json.dumps(self.fx.env),))
        with self.assertRaisesRegex(ValueError, "catalog_bound_work_changed"):
            coverage.validate_source_binding(self.fx.c, result["source_binding"], self.fixtures.CUTOFF)


if __name__ == "__main__":
    unittest.main()
