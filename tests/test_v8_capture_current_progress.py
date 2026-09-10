"""Current-day progress reads a frozen denominator; never fetches or rewrites."""
from __future__ import annotations

import json
import socket
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from v8 import capture_quality as quality, capture_planning as planning, schema_v20, storage

AT = "2026-09-07T03:00:00Z"
CUTOFF = "2026-09-07T03:05:00Z"


class CurrentPlanProgressTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.db = Path(temporary.name) / "progress.sqlite3"
        self.network = self.enterContext(patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden")))
        self.addCleanup(self.network.assert_not_called)
        self.members = [{"account_id": identifier, "identity_id": identifier,
                         "platform": "douyin", "uid": str(identifier)}
                        for identifier in range(1, 7)]
        self.members.append({"account_id": 1, "identity_id": 7, "platform": "xiaohongshu", "uid": "xhs"})
        self.routes = {}
        with storage.connect(self.db) as connection:
            storage.initialize_database(connection)
            schema_v20.migrate(connection)
            with storage.transaction(connection):
                for identifier in range(1, 7):
                    connection.execute("""INSERT INTO accounts(id,phone,operator_name,account_type,
                        content_direction,created_at,updated_at) VALUES(?,'','','unknown','unknown',?,?)""",
                        (identifier, AT, AT))
                for member in self.members:
                    operation = member["platform"] + "_user_posts"
                    self.routes[member["identity_id"]] = planning.assign_route(connection, scope_type="account",
                        scope_key=str(member["account_id"]), account_id=member["account_id"],
                        provider="tikhub", operation=operation, expected_generation=0, route="integrated",
                        mode="active", effective_at=AT, recorded_at=AT)
                connection.execute("""INSERT INTO routing_input_changes(change_kind,payload_json,
                    effective_at,recorded_at,change_sha256) VALUES('policy','{}',?,?,?)""", (AT, AT, "a" * 64))
        self.plan_id = self.plan()

    def plan(self, *, day="2026-09-07", at=AT, mode="active", members=None):
        body = {"cohort": self.members if members is None else members}
        with storage.connect(self.db) as connection, storage.transaction(connection):
            generation = connection.execute("SELECT count(*)+1 FROM capture_source_plans").fetchone()[0]
            body["generation"] = generation
            cursor = connection.execute("""INSERT INTO capture_source_plans(roster_change_id,business_day,
                generation,mode,payload_json,created_at,plan_sha256) VALUES(1,?,?,?,?,?,?)""",
                (day, generation, mode, planning.canonical(body), at, planning.digest(body)))
            return int(cursor.lastrowid)

    def work(self, index, *, state="runnable", reason="", at=AT, plan_id=None, day="2026-09-07", watermark=False):
        member = self.members[index - 1]
        envelope = {**member, "stage": "discovery"}
        operation = member["platform"] + "_user_posts"
        with storage.connect(self.db) as connection, storage.transaction(connection):
            sequence = connection.execute("SELECT count(*)+1 FROM capture_work_items").fetchone()[0]
            cursor = connection.execute("""INSERT INTO capture_work_items(work_identity,assignment_id,
                source_plan_id,account_id,provider,operation,due_at,data_business_day,state,reason,
                envelope_json,owner_token,created_at,updated_at,completed_at)
                VALUES(?,?,?,?,'tikhub',?,?,?,?,?,?,?,?,?,?)""",
                (planning.digest({"fixture": sequence}), self.routes[member["identity_id"]],
                 plan_id or self.plan_id, member["account_id"], operation, at, day, state, reason,
                 json.dumps(envelope), "owner" if state in {"running", "leased"} else None,
                 at, at, at if state == "terminal" else None))
            work_id = int(cursor.lastrowid)
            if watermark:
                evidence = {"complete": True, "terminal_cursor": True, "all_raw_verified": True,
                            "cap_hit": False, "cursor_loop": False,
                            "seen": 0, "valid": 0, "invalid": 0, "missing": 0, "unavailable": 0}
                planning.advance_watermark(connection, work_id=work_id, scope_key=f"{member['platform']}:{member['uid']}",
                    complete_through=at, evidence=evidence, recorded_at=at)
            return work_id

    def measure(self, *, at=CUTOFF):
        with storage.connect(self.db) as connection:
            before = connection.total_changes
            connection.execute("PRAGMA query_only=ON")
            result = quality.measure(connection, at=at)
            self.assertEqual(connection.total_changes, before)
            self.assertIsNone(result["video_completeness"])
            self.assertIsNone(result["field_accuracy"])
            self.assertEqual(result["external_notifications"], 0)
            self.assertFalse(result["paid_gates_changed"])
            return result["current_plan_progress"]

    def test_expected_includes_members_without_work_and_distinct_platform_scopes(self):
        self.work(2)
        self.work(3, state="terminal", watermark=True)
        self.work(4, state="terminal", reason="page_cap_hit")
        self.work(5, state="paid_identity_hold", reason="billing_unknown")
        self.work(6, state="terminal")
        result = self.measure()
        self.assertEqual(result["expected"], 7)
        self.assertEqual({key: result[key] for key in ("queued_or_running", "succeeded", "partial", "blocked", "failed", "not_planned")},
            {"queued_or_running": 1, "succeeded": 1, "partial": 1, "blocked": 1, "failed": 1, "not_planned": 2})
        self.assertEqual(result["last_success_at"], planning.timestamp(AT))
        self.assertEqual(result["oldest_wait_seconds"], 300)
        self.assertEqual(result["denominator_kind"], "frozen_platform_account_scopes")

    def test_terminal_without_watermark_is_not_success_and_loop_is_partial(self):
        self.work(1, state="terminal")
        self.work(2, state="terminal", reason="cursor_loop")
        result = self.measure()
        self.assertEqual((result["succeeded"], result["failed"], result["partial"]), (0, 1, 1))
        self.assertIsNone(result["last_success_at"])

    def test_blocked_states_remain_in_denominator(self):
        for index, state in enumerate(("paid_identity_hold", "provider_blocked", "budget_deferred"), 1):
            self.work(index, state=state)
        result = self.measure()
        self.assertEqual((result["expected"], result["blocked"], result["not_planned"], result["succeeded"]), (7, 3, 4, 0))

    def test_latest_discovery_wins_but_prior_current_day_success_time_is_retained(self):
        self.work(1, state="terminal", watermark=True)
        self.work(1, state="running", at="2026-09-07T03:02:00Z")
        self.work(1, state="terminal", watermark=True, at="2026-09-07T04:00:00Z")
        result = self.measure()
        self.assertEqual((result["queued_or_running"], result["succeeded"], result["not_planned"]), (1, 0, 6))
        self.assertEqual(result["last_success_at"], planning.timestamp(AT))
        self.assertEqual(result["oldest_wait_seconds"], 180)

    def test_future_or_shadow_plan_does_not_supersede_current_active_plan(self):
        self.plan(at="2026-09-07T04:00:00Z", members=[])
        self.plan(at="2026-09-07T03:01:00Z", mode="shadow", members=[])
        result = self.measure()
        self.assertEqual((result["plan_id"], result["expected"]), (self.plan_id, 7))

    def test_latest_current_plan_has_its_own_independent_denominator(self):
        self.work(1, state="terminal", watermark=True)
        current = self.plan(at="2026-09-07T03:01:00Z", members=[self.members[0]])
        result = self.measure()
        self.assertEqual((result["plan_id"], result["expected"], result["not_planned"], result["succeeded"]), (current, 1, 1, 0))

    def test_new_business_day_does_not_reuse_yesterday_plan_or_success(self):
        self.work(1, state="terminal", watermark=True)
        result = self.measure(at="2026-09-08T03:00:00Z")
        self.assertEqual(result["status"], "unknown")
        self.assertIsNone(result["expected"])
        self.assertIsNone(result["succeeded"])
        self.assertIsNone(result["last_success_at"])
        new_plan = self.plan(day="2026-09-08", at="2026-09-08T00:10:00Z")
        result = self.measure(at="2026-09-08T03:00:00Z")
        self.assertEqual((result["plan_id"], result["not_planned"], result["succeeded"]), (new_plan, 7, 0))

    def test_invalid_frozen_cohort_is_unknown_not_false_zero_or_complete(self):
        self.plan(at="2026-09-07T03:01:00Z", members=[self.members[0], self.members[0]])
        result = self.measure()
        self.assertEqual(result["status"], "unknown")
        self.assertEqual(result["reason"], "current_plan_cohort_invalid")
        self.assertIsNone(result["expected"])


if __name__ == "__main__":
    unittest.main()
