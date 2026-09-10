"""Exercise real read-only cycle SQL; activation/catalog authorities are fixtures.

This module never opens a filesystem database or contacts a provider. Combined
release integration must additionally exercise the real catalog authority.
"""
import copy
import json
import socket
import sqlite3
import sys
import types
import unittest
from unittest.mock import patch

from v8 import capture_metric_cycles as cycles

AT = "2026-09-10T06:00:00Z"
OLD = "2026-09-10T00:00:00Z"
OP = "douyin_video_statistics"


class MetricCycleContinuityTest(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden")))
        self.c = sqlite3.connect(":memory:")
        self.c.row_factory = sqlite3.Row
        self.addCleanup(self.c.close)
        self.c.executescript("""
            PRAGMA user_version=21;
            CREATE TABLE accounts(id INTEGER PRIMARY KEY,enabled INTEGER);
            CREATE TABLE account_platform_identities(id INTEGER PRIMARY KEY,account_id INTEGER,uid TEXT);
            CREATE TABLE account_roster_members(snapshot_id INTEGER,account_identity_id INTEGER,platform TEXT);
            CREATE TABLE content_items(id INTEGER PRIMARY KEY,account_id INTEGER,platform TEXT,published_at TEXT);
            CREATE TABLE content_identity_merge_events(loser_content_id INTEGER);
            CREATE TABLE task_contents(content_id INTEGER,task_id TEXT);
            CREATE TABLE report_tasks(id TEXT,task_status TEXT,completed_at TEXT);
            CREATE TABLE capture_source_plans(id INTEGER PRIMARY KEY,mode TEXT,business_day TEXT,payload_json TEXT,plan_sha256 TEXT);
            CREATE TABLE capture_work_items(id INTEGER PRIMARY KEY,account_id INTEGER,content_id INTEGER,
                operation TEXT,state TEXT,owner_token TEXT,provider TEXT,assignment_id INTEGER,source_plan_id INTEGER,
                envelope_json TEXT,work_identity TEXT,created_at TEXT,updated_at TEXT,completed_at TEXT);
            INSERT INTO accounts VALUES(1,1);
            INSERT INTO account_platform_identities VALUES(1,1,'10000001');
            INSERT INTO account_roster_members VALUES(2,1,'douyin');
            INSERT INTO content_items VALUES(1,1,'douyin','2026-09-08T12:00:00Z');
        """)
        self.active = {"activation_id": 3, "profile_id": "integrated_route_v1", "activation_sha256": "a"*64,
                       "roster_snapshot_id": 2, "roster_members_sha256": "b"*64}
        self.member = {"identity_id": 1, "account_id": 1, "platform": "douyin", "uid": "10000001"}
        self.plan = {"id": 1, **self.active, "business_day": "2026-09-10", "contract_version": cycles.CONTRACT,
                     "shadow": False, "cohort": [self.member]}
        self.content = dict(self.c.execute("SELECT * FROM content_items").fetchone())
        self.save_plan()
        self.envelope = {key: value for key, value in self.active.items() if key != "activation_sha256"}
        self.envelope.update(**self.member, contract_version=cycles.CONTRACT, content_id=1, stage="metrics",
            capture_stage="metrics", category="metrics", source_stage="metrics", operation=OP,
            logical_due="metrics:"+OLD+":statistics", assignment_id=1, source_plan_id=1, request_batch_id=100)
        self.insert_work()
        self.enterContext(patch.object(cycles.profile_activations, "activation_at", return_value=self.active))
        self.enterContext(patch.object(cycles.account_roster, "_supports_source_families", return_value=True))
        self.enterContext(patch.object(cycles.account_roster, "snapshot_by_id",
            return_value={"id": 2, "members_sha256": "b"*64}))

    def save_plan(self):
        body = {key: value for key, value in self.plan.items() if key != "id"}
        self.c.execute("INSERT OR REPLACE INTO capture_source_plans VALUES(?,?,?,?,?)",
            (1, "active", body["business_day"], json.dumps(body), cycles.planning.digest(body)))

    def insert_work(self, *, state="paid_identity_hold", due=None, ident=1):
        envelope = copy.deepcopy(self.envelope)
        if due:
            envelope["logical_due"] = due
        identity = cycles.planning.digest({"provider": "tikhub", "operation": OP,
            "subject": "content:1", "logical_due": envelope["logical_due"]})
        self.c.execute("INSERT OR REPLACE INTO capture_work_items VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (ident,1,1,OP,state,None,"tikhub",1,1,json.dumps(envelope),identity,OLD,
             "2026-09-10T00:03:00Z",None))

    def check(self, *, due="metrics:"+AT+":statistics", at=AT, operation=OP, source_stage="metrics"):
        before = self.c.total_changes
        self.c.execute("PRAGMA query_only=ON")
        try:
            result = cycles.metric_cycle_pending(self.c,self.plan,self.member,self.content,
                operation,source_stage,due,at)
            self.assertEqual(self.c.total_changes,before)
            return result
        finally:
            self.c.execute("PRAGMA query_only=OFF")

    def test_next_same_day_cycle_is_allowed_and_old_evidence_unchanged(self):
        before = [tuple(row) for row in self.c.execute("SELECT * FROM capture_work_items")]
        self.assertFalse(self.check())
        self.assertFalse(self.check())
        self.assertEqual(before,[tuple(row) for row in self.c.execute("SELECT * FROM capture_work_items")])

    def test_same_cycle_and_arbitrary_future_cycle_stay_blocked(self):
        self.assertTrue(self.check(due="metrics:"+OLD+":statistics",at="2026-09-10T05:59:59Z"))
        self.assertTrue(self.check(due="metrics:2026-09-10T12:00:00Z:statistics"))
        self.assertTrue(self.check(due="metrics:"+AT+":detail_counts"))

    def test_duplicate_new_identity_blocks_every_status(self):
        for state in ("terminal","paid_identity_hold","running","runnable"):
            with self.subTest(state=state):
                self.insert_work(ident=2,state=state,due="metrics:"+AT+":statistics")
                self.assertTrue(self.check())

    def test_non_hold_states_retain_backpressure(self):
        for state in ("running","leased","runnable","provider_blocked","budget_deferred"):
            with self.subTest(state=state):
                self.c.execute("UPDATE capture_work_items SET state=?",(state,))
                self.assertTrue(self.check())

    def test_hold_updated_at_must_be_strictly_before_current_bucket(self):
        for updated in (AT,"2026-09-10T06:01:00Z","invalid"):
            self.c.execute("UPDATE capture_work_items SET updated_at=?",(updated,))
            self.assertTrue(self.check())

    def test_old_manual_compensation_and_other_stages_cannot_gain_exception(self):
        for changes in ({"kind":"metrics_update"},{"manual_command_run_id":9},
                {"manual_command_run_ids":[9]},{"compensation":None},{"stage":"detail"},
                {"source_stage":"detail"},{"content_id":2},{"source_plan_id":99}):
            with self.subTest(changes=changes):
                self.c.execute("UPDATE capture_work_items SET envelope_json=?",
                    (json.dumps({**self.envelope,**changes}),))
                self.assertTrue(self.check())

    def test_owner_hash_and_persisted_plan_tampering_fail_closed(self):
        self.c.execute("UPDATE capture_work_items SET owner_token='owner'")
        self.assertTrue(self.check())
        self.c.execute("UPDATE capture_work_items SET owner_token=NULL,work_identity='tampered'")
        self.assertTrue(self.check())
        self.insert_work()
        self.c.execute("UPDATE capture_source_plans SET plan_sha256='tampered'")
        self.assertTrue(self.check())

    def test_detail_counts_is_a_metric_cycle_and_lifetime_detail_is_not(self):
        operation = "douyin_video_detail"
        due = "metrics:"+OLD+":detail_counts"
        old = {**self.envelope,"operation":operation,"source_stage":"detail","logical_due":due}
        identity = cycles.planning.digest({"provider":"tikhub","operation":operation,
            "subject":"content:1","logical_due":due})
        self.c.execute("UPDATE capture_work_items SET operation=?,envelope_json=?,work_identity=?",
            (operation,json.dumps(old),identity))
        self.assertFalse(self.check(operation=operation,source_stage="detail",
            due="metrics:"+AT+":detail_counts"))
        old.update(stage="detail",capture_stage="detail",logical_due="lifetime")
        self.c.execute("UPDATE capture_work_items SET envelope_json=?",(json.dumps(old),))
        self.assertTrue(self.check(operation=operation,source_stage="detail",
            due="metrics:"+AT+":detail_counts"))

    def test_old_hold_is_not_isolated_while_other_work_owns_same_operation(self):
        self.insert_work(ident=2,state="provider_blocked",due="metrics:2026-09-10T02:00:00Z:statistics")
        self.assertTrue(self.check())
        self.c.execute("UPDATE capture_work_items SET state='terminal' WHERE id=2")
        self.assertFalse(self.check())

    def test_real_roster_query_rejects_paused_and_removed_member(self):
        self.c.execute("UPDATE accounts SET enabled=0")
        self.assertTrue(self.check())
        self.c.execute("UPDATE accounts SET enabled=1")
        self.c.execute("DELETE FROM account_roster_members")
        self.assertTrue(self.check())

    def test_catalog_uses_public_directory_authority_without_old_enabled(self):
        module = types.ModuleType("v8.account_catalog_capture")
        result = {**self.member,"account_identity_id":1}
        self.plan["catalog_snapshot"] = {"snapshot_sha256":"c"*64}
        self.save_plan()
        self.c.execute("UPDATE accounts SET enabled=0")
        self.c.execute("DELETE FROM account_roster_members")
        with patch.dict(sys.modules,{module.__name__:module}), patch.object(module,
                "validate_plan_member",create=True,return_value=result) as authority:
            self.assertFalse(self.check())
            authority.assert_called_once_with(self.c, 1, 1, at=AT)
            authority.side_effect=ValueError("account_paused")
            self.assertTrue(self.check())

    def test_midnight_plan_day_matches_planner_until_0010(self):
        at = "2026-09-10T16:09:00Z"
        due = "metrics:2026-09-10T12:00:00Z:statistics"
        self.assertFalse(self.check(at=at, due=due))
        self.assertTrue(self.check(at="2026-09-10T16:10:00Z", due=due))
        self.plan["business_day"] = "2026-09-11"
        self.save_plan()
        self.assertTrue(self.check(at=at, due=due))
        self.assertFalse(self.check(at="2026-09-10T16:10:00Z", due=due))

    def test_business_active_recomputed_from_database_not_caller(self):
        self.content["business_active"] = True
        self.assertTrue(self.check(due="metrics:2026-09-10T08:00:00Z:statistics",at="2026-09-10T08:00:00Z"))
        self.c.execute("INSERT INTO report_tasks VALUES('task','running',NULL)")
        self.c.execute("INSERT INTO task_contents VALUES(1,'task')")
        self.assertFalse(self.check(due="metrics:2026-09-10T08:00:00Z:statistics",at="2026-09-10T08:00:00Z"))

    def test_age_policy_72_hour_bucket_and_ineligible_content(self):
        self.c.execute("UPDATE content_items SET published_at='2026-08-25T12:00:00Z'")
        self.content["published_at"]="2026-08-25T12:00:00Z"
        self.assertTrue(self.check())  # 06:00 is not the real 72-hour bucket.
        self.c.execute("UPDATE content_items SET published_at='2026-05-01T12:00:00Z'")
        self.content["published_at"]="2026-05-01T12:00:00Z"
        self.assertTrue(self.check())

    def test_content_merge_and_activation_changes_fail_closed(self):
        self.c.execute("INSERT INTO content_identity_merge_events VALUES(1)")
        self.assertTrue(self.check())
        self.c.execute("DELETE FROM content_identity_merge_events")
        self.active["activation_id"]=4
        self.assertTrue(self.check())


if __name__ == "__main__":
    unittest.main()
