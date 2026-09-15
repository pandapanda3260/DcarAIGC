"""Real schema23 plan-cache boundaries and unchanged live metric-cycle fences."""
from __future__ import annotations

import copy
import json
import sqlite3
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tests import test_v8_metric_cycle_continuity as continuity
from v8 import account_catalog_capture as catalog, capture_metric_cycles as cycles
from v8 import capture_work_index as work_index, schema_v23
from v8.account_catalog_capture_release import ACCOUNT_CATALOG_POLICY_V3 as POLICY
from v8.storage import connect, initialize_database, transaction

AT = "2026-09-12T03:00:00Z"


class MetricCycleContextV23Test(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.c = connect(Path(self.tmp.name) / "fixture.sqlite3"); self.addCleanup(self.c.close)
        initialize_database(self.c, target_version=23)
        self.enterContext(patch("socket.socket.connect", side_effect=AssertionError("network forbidden")))
        self.body = {"contract_version": cycles.CONTRACT, "shadow": False, "business_day": "2026-09-12",
                     "activation_id": 1, "cohort": [{"identity_id": 1, "enabled": True}], "zero": 0.0}
        with transaction(self.c):
            change = self.c.execute("INSERT INTO routing_input_changes(change_kind,payload_json,effective_at,recorded_at,change_sha256) "
                "VALUES('policy','{}',?,?,?)", (AT, AT, "a" * 64)).lastrowid
            self.plan_id = self.c.execute("INSERT INTO capture_source_plans(roster_change_id,business_day,generation,mode,payload_json,created_at,plan_sha256) "
                "VALUES(?,'2026-09-12',1,'active',?,?,?)", (change, json.dumps(self.body), AT, cycles.planning.digest(self.body))).lastrowid

    def simulate_broken_schema_guard(self):
        # Only this test's temporary database: simulate an out-of-band corruptor
        # after proving schema23 normally rejects every plan UPDATE itself.
        for row in self.c.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='capture_source_plans'").fetchall():
            self.c.execute('DROP TRIGGER "' + row[0].replace('"', '""') + '"')

    def test_twenty_accesses_parse_and_hash_one_persisted_plan(self):
        with transaction(self.c), cycles.planning_validation(self.c), patch.object(cycles.planning, "digest", wraps=cycles.planning.digest) as digest:
            for _ in range(20):
                stored = cycles._stored_plan(self.c, self.plan_id)
                self.assertTrue(cycles._same_plan(self.c, stored, {"id": self.plan_id, **self.body}))
            self.assertEqual(digest.call_count, 1)
        self.assertIsNone(cycles._PLANNING_PLANS.get())

    def test_bound_plan_is_deeply_immutable_and_compared_only_at_page_entry(self):
        supplied = {"id": self.plan_id, **copy.deepcopy(self.body)}
        with transaction(self.c), patch.object(cycles.planning, "canonical", wraps=cycles.planning.canonical) as canonical:
            with cycles.planning_validation(self.c, plan=supplied) as bound:
                validated_calls = canonical.call_count
                for _ in range(20):
                    self.assertTrue(cycles._same_plan(self.c, cycles._stored_plan(self.c, self.plan_id), bound))
                self.assertEqual(canonical.call_count, validated_calls)
                with self.assertRaises(TypeError): bound["activation_id"] = 2
                with self.assertRaises(TypeError): bound["cohort"][0]["identity_id"] = 2
                with self.assertRaises(TypeError): bound["cohort"][0] = {}
                supplied["activation_id"] = 2
                self.assertEqual(bound["activation_id"], 1)
                self.assertFalse(cycles._same_plan(self.c, cycles._stored_plan(self.c, self.plan_id), supplied))
        self.assertIsNone(cycles._PLANNING_PLANS.get())

    def test_invalid_bound_input_and_reused_binding_outside_page_fail_closed(self):
        with transaction(self.c):
            with self.assertRaisesRegex(ValueError, "differs from persisted"):
                with cycles.planning_validation(self.c, plan={"id": self.plan_id, **self.body, "activation_id": True}):
                    self.fail("forged input entered planning")
            self.assertIsNone(cycles._PLANNING_PLANS.get())
            with cycles.planning_validation(self.c, plan={"id": self.plan_id, **self.body}) as bound:
                pass
            with self.assertRaises(TypeError):
                cycles._same_plan(self.c, cycles._stored_plan(self.c, self.plan_id), bound)

    def test_cache_does_not_survive_page_context_or_connection(self):
        with transaction(self.c), patch.object(cycles.planning, "digest", wraps=cycles.planning.digest) as digest:
            for _ in range(2):
                with cycles.planning_validation(self.c):
                    cycles._stored_plan(self.c, self.plan_id)
            self.assertEqual(digest.call_count, 2)
        with connect(Path(self.tmp.name) / "fixture.sqlite3") as other:
            with transaction(self.c), cycles.planning_validation(self.c), patch.object(cycles.planning, "digest", wraps=cycles.planning.digest) as digest:
                cycles._stored_plan(self.c, self.plan_id)
                cycles._stored_plan(other, self.plan_id)
                self.assertEqual(digest.call_count, 2)

    def test_mutated_persisted_payload_even_with_new_matching_hash_is_rejected(self):
        with transaction(self.c), cycles.planning_validation(self.c):
            cycles._stored_plan(self.c, self.plan_id)
            changed = {**self.body, "activation_id": 2}
            with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
                self.c.execute("UPDATE capture_source_plans SET payload_json=? WHERE id=?", (json.dumps(changed), self.plan_id))
            self.simulate_broken_schema_guard()
            self.c.execute("UPDATE capture_source_plans SET payload_json=?,plan_sha256=? WHERE id=?",
                (json.dumps(changed), cycles.planning.digest(changed), self.plan_id))
            with self.assertRaisesRegex(ValueError, "changed during planning"):
                cycles._stored_plan(self.c, self.plan_id)

    def test_corrupt_hash_mode_and_day_never_reuse_verified_payload(self):
        for column, value in (("plan_sha256", "f" * 64), ("mode", "shadow"), ("business_day", "2026-09-13")):
            with self.subTest(column=column):
                with self.assertRaises(ValueError), transaction(self.c), cycles.planning_validation(self.c):
                    cycles._stored_plan(self.c, self.plan_id)
                    self.simulate_broken_schema_guard()
                    self.c.execute(f"UPDATE capture_source_plans SET {column}=? WHERE id=?", (value, self.plan_id))
                    cycles._stored_plan(self.c, self.plan_id)

    def test_mutable_caller_input_and_numeric_type_changes_fail_strict_comparison(self):
        with transaction(self.c), cycles.planning_validation(self.c):
            stored = cycles._stored_plan(self.c, self.plan_id)
            supplied = copy.deepcopy(stored)
            self.assertTrue(cycles._same_plan(self.c, stored, supplied))
            for path, replacement in ((["activation_id"], True), (["activation_id"], 1.0),
                    (["cohort", 0, "enabled"], 1), (["cohort", 0, "identity_id"], 9), (["zero"], -0.0)):
                changed = copy.deepcopy(supplied); target = changed
                for key in path[:-1]: target = target[key]
                target[path[-1]] = replacement
                self.assertFalse(cycles._same_plan(self.c, stored, changed), path)

    def test_catalog_context_owns_nested_cache_and_exception_clears_it(self):
        with transaction(self.c):
            snapshot = catalog.freeze_snapshot(self.c, policy=POLICY)
            with self.assertRaisesRegex(RuntimeError, "fixture"):
                with catalog.planning_validation(self.c, POLICY, snapshot):
                    self.assertIs(cycles._PLANNING_PLANS.get()["connection"], self.c)
                    raise RuntimeError("fixture")
            self.assertIsNone(cycles._PLANNING_PLANS.get())
        self.assertEqual(self.c.execute("PRAGMA user_version").fetchone()[0], 23)

    def catalog_plan(self, snapshot):
        body = {**self.body, "cohort": [], "catalog_snapshot": snapshot, "catalog_mode": "active",
                "roster_snapshot_id": 1, "roster_members_sha256": "b" * 64}
        plan_id = self.c.execute("INSERT INTO capture_source_plans(roster_change_id,business_day,generation,mode,payload_json,created_at,plan_sha256) "
            "VALUES(1,'2026-09-12',2,'active',?,?,?)", (json.dumps(body), AT, cycles.planning.digest(body))).lastrowid
        return {"id": plan_id, **body}

    def test_catalog_shares_verified_plan_and_separate_snapshot_copy_stays_strict(self):
        with transaction(self.c):
            snapshot = catalog.freeze_snapshot(self.c, policy=POLICY)
            plan = self.catalog_plan(copy.deepcopy(snapshot))
            with catalog.planning_validation(self.c, POLICY, snapshot, plan=plan):
                cached = catalog._planning_cache(self.c)["plans"][plan["id"]]
                self.assertEqual(cached, {k: v for k, v in plan.items() if k != "id"})
                with self.assertRaisesRegex(RuntimeError, "not uniquely included"):
                    catalog.validate_plan_member(self.c, plan["id"], 1, at=AT)

    def test_forged_embedded_snapshot_with_same_claimed_digest_cannot_prime_catalog(self):
        with transaction(self.c):
            snapshot = catalog.freeze_snapshot(self.c, policy=POLICY)
            forged = copy.deepcopy(snapshot)
            forged["eligibility"]["excluded_members"].append({"identity_id": 9})
            plan = self.catalog_plan(forged)
            with self.assertRaisesRegex(ValueError, "differs from catalog snapshot"):
                with catalog.planning_validation(self.c, POLICY, snapshot, plan=plan):
                    self.fail("mismatched snapshot entered catalog cache")
            self.assertIsNone(cycles._PLANNING_PLANS.get())
            self.assertIsNone(catalog._planning_cache(self.c))


class MetricCycleLiveFenceCacheTest(unittest.TestCase):
    def setUp(self):
        self.fx = continuity.MetricCycleContinuityTest(); self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)

    def test_activation_and_new_work_ownership_still_change_each_decision(self):
        with cycles.planning_validation(self.fx.c, plan=self.fx.plan) as bound:
            self.fx.plan = bound
            self.assertFalse(self.fx.check())
            self.fx.active["activation_id"] = 4
            self.assertTrue(self.fx.check())
            self.fx.active["activation_id"] = 3
            self.fx.insert_work(ident=2, state="provider_blocked", due="metrics:2026-09-10T02:00:00Z:statistics")
            self.assertTrue(self.fx.check())

    def test_current_membership_and_content_merge_are_not_plan_cache_results(self):
        with cycles.planning_validation(self.fx.c, plan=self.fx.plan) as bound:
            self.fx.plan = bound
            self.assertFalse(self.fx.check())
            self.fx.c.execute("UPDATE accounts SET enabled=0")
            self.assertTrue(self.fx.check())
            self.fx.c.execute("UPDATE accounts SET enabled=1")
            self.fx.c.execute("INSERT INTO content_identity_merge_events VALUES(1)")
            self.assertTrue(self.fx.check())

    def test_member_with_preparation_chain_lists_keeps_original_json_identity(self):
        self.fx.member["locator_evidence"] = {"sources": [{"raw_response_id": 7, "raw_sha256": "c" * 64}]}
        self.fx.save_plan()
        with cycles.planning_validation(self.fx.c, plan=self.fx.plan) as bound:
            self.fx.plan = bound
            self.assertIsInstance(self.fx.member["locator_evidence"]["sources"], list)
            self.assertIsInstance(bound["cohort"][0]["locator_evidence"]["sources"], tuple)
            self.assertFalse(self.fx.check())

    def test_business_activity_requeries_after_savepoint_rollback_and_nested_context(self):
        c = self.fx.c
        with cycles.planning_validation(c, plan=self.fx.plan) as outer:
            self.assertFalse(cycles._business_active(c, 1, cycles._time(continuity.AT)))
            c.execute("SAVEPOINT report_activity")
            c.execute("INSERT INTO report_tasks VALUES('task','running',NULL)")
            c.execute("INSERT INTO task_contents VALUES(1,'task')")
            with cycles.planning_validation(c, plan=self.fx.plan) as inner:
                self.assertIsNot(inner, outer)
                self.assertTrue(cycles._business_active(c, 1, cycles._time(continuity.AT)))
            self.assertIs(cycles._PLANNING_PLANS.get()["bound_plan"], outer)
            c.execute("ROLLBACK TO report_activity")
            c.execute("RELEASE report_activity")
            self.assertFalse(cycles._business_active(c, 1, cycles._time(continuity.AT)))
            self.assertFalse(cycles._business_active(c, 1, cycles._time("2026-09-11T06:00:00Z")))
        self.assertIsNone(cycles._PLANNING_PLANS.get())


class MetricWorkIndexStructureV23Test(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.c = connect(Path(self.tmp.name) / "fixture.sqlite3"); self.addCleanup(self.c.close)
        initialize_database(self.c, target_version=23)

    def test_absent_or_exact_index_preserves_every_original_migration_proof_byte(self):
        before = tuple(self.c.execute("SELECT * FROM four_platform_flow_migrations").fetchone())
        old_objects = schema_v23.objects(self.c)
        schema_v23.validate_structure(self.c)
        with transaction(self.c): self.c.execute(work_index.INDEX_SQL)
        self.assertIn(work_index.INDEX_OBJECT, schema_v23.objects(self.c))
        self.assertEqual(work_index.normalize_objects(schema_v23.objects(self.c)), old_objects)
        schema_v23.validate_structure(self.c)
        self.assertEqual(tuple(self.c.execute("SELECT * FROM four_platform_flow_migrations").fetchone()), before)
        self.assertEqual(schema_v23.migration_proof(self.c)["target_schema_sha256"], schema_v23.digest(old_objects))

    def test_changed_index_name_or_sql_is_rejected(self):
        statements = [work_index.INDEX_SQL.replace(work_index.INDEX_NAME, "renamed_capture_index"),
                      work_index.INDEX_SQL.replace("content_id,operation,state,account_id", "content_id,account_id"),
                      work_index.INDEX_SQL.replace(" WHERE content_id IS NOT NULL", "")]
        for sql in statements:
            with self.subTest(sql=sql), self.assertRaises(ValueError), transaction(self.c):
                self.c.execute(sql)
                schema_v23.validate_structure(self.c)

    def test_exact_index_does_not_hide_other_ddl_changes(self):
        for sql in ("CREATE TABLE unexpected_table(id INTEGER)", "CREATE INDEX unexpected_index ON capture_work_items(operation)",
                    "CREATE VIEW unexpected_view AS SELECT id FROM capture_work_items"):
            with self.subTest(sql=sql), self.assertRaises(ValueError), transaction(self.c):
                self.c.execute(work_index.INDEX_SQL)
                self.c.execute(sql)
                schema_v23.validate_structure(self.c)

    def test_normalizer_rejects_duplicate_or_non_index_object_with_reserved_name(self):
        with self.assertRaises(ValueError):
            work_index.normalize_objects([work_index.INDEX_OBJECT, work_index.INDEX_OBJECT])
        with self.assertRaises(ValueError):
            work_index.normalize_objects([("table", work_index.INDEX_NAME, "capture_work_items", work_index.INDEX_SQL)])
