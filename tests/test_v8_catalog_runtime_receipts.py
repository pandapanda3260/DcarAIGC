"""Catalog pages -> native day receipts -> cleanup readiness on real schemas.

The installed generation, SQLite constraints, authority and operator gates use
the offline cleanup fixture. Catalog payloads use the same frozen one-member
shape as the coverage tests; exhausted provider JSON is a local fixture. No
coverage, activation, receipt or readiness validator is patched, and no network
or production installation is involved.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
import hashlib
import sqlite3
import unittest
from unittest.mock import patch

from tests import test_v8_capture_day_coverage as catalog_fixtures
from tests import test_v8_cleanup_day_readiness as cleanup_fixtures
from v8 import capture_day_coverage as catalog
from v8 import capture_authorizations as auth, durable_runs, raw_archive
from v8 import runtime_receipts, scan_receipts


class CatalogRuntimeReceiptsTest(unittest.TestCase):
    def setUp(self):
        # Composition avoids importing the fixture's own tests into this class.
        self.runtime = cleanup_fixtures.CleanupDayReadinessTest(methodName="runTest")
        self.runtime.setUp()
        self.addCleanup(self.runtime.doCleanups)
        self.bind_fixture()

    def bind_fixture(self):
        self.fixture = self.runtime.fixture
        self.connection = self.runtime.connection
        self.active = self.runtime.active

    def insert(self, table, values):
        return self.connection.execute(
            f"INSERT INTO {table}({','.join(values)}) VALUES ({','.join('?' for _ in values)})",
            tuple(values.values()),
        ).lastrowid

    def seed_catalog_day(self, *, day="2026-09-08", complete=True, transition=False):
        """Freeze actual FK-bound plans/work/attempt/quality/watermark/raw rows."""
        lower = datetime.combine(date.fromisoformat(day), time.min, catalog.BEIJING)
        upper = lower + timedelta(days=1)

        def iso(value):
            return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

        created, finished = iso(upper + timedelta(minutes=10)), iso(upper + timedelta(minutes=11))
        self.cutoff = iso(upper + timedelta(hours=4))
        self.day = day
        member = {**self.fixture.member, "identity_id": self.fixture.member["account_identity_id"],
                  "locator_sha256": "c" * 64, "eligible": True, "reason_code": "eligible"}
        snapshot = catalog_fixtures.CatalogDayCoverageTest.make_snapshot(self, [member])
        change = {"change_kind": "roster", "roster_snapshot_id": self.active["roster_snapshot_id"],
                  "payload_json": "{}", "effective_at": self.active["effective_at"],
                  "recorded_at": self.active["created_at"]}
        change_id = self.insert("routing_input_changes", {**change, "change_sha256": auth.digest(change)})

        def plan(plan_day, at):
            payload = {"contract_version": "capture-runtime-v1", "business_day": plan_day,
                       "shadow": False, "catalog_mode": "active",
                       **{key: self.active[key] for key in catalog.EPOCH_KEYS},
                       "catalog_snapshot": snapshot, "catalog_snapshot_sha256": snapshot["snapshot_sha256"],
                       "cohort": [{key: member[key] for key in catalog.MEMBER_KEYS}]}
            return self.insert("capture_source_plans", {
                "roster_change_id": change_id, "business_day": plan_day, "generation": 1,
                "mode": "active", "payload_json": auth.canonical(payload),
                "created_at": at, "plan_sha256": auth.digest(payload)})

        if not transition:
            plan((lower.date() - timedelta(days=1)).isoformat(), self.active["effective_at"])
        plan(day, iso(lower + timedelta(minutes=10)) if not transition else self.active["effective_at"])
        work_day = upper.date().isoformat()
        self.plan_id = plan(work_day, created)
        assignment = {"scope_type": "account", "scope_key": f"account:{member['identity_id']}",
                      "provider": "tikhub", "operation": "douyin_user_posts",
                      "account_id": member["account_id"], "generation": 1, "source_plan_id": self.plan_id,
                      "route": "integrated", "mode": "active", "effective_at": created, "recorded_at": created}
        assignment_id = self.insert("capture_route_assignments", {
            **assignment, "assignment_sha256": auth.digest(assignment)})
        env = {"contract_version": "capture-runtime-v1", **{key: member[key] for key in catalog.MEMBER_KEYS},
               "content_id": None, "stage": "discovery", "capture_stage": "discovery",
               "source_stage": "discovery", "category": "reconcile", "operation": "douyin_user_posts",
               "logical_due": "discovery:" + created, "assignment_id": assignment_id,
               "source_plan_id": self.plan_id, "catalog_plan_id": self.plan_id,
               "window_start": iso(lower - timedelta(days=1)), "window_end": created,
               **{key: self.active[key] for key in catalog.EPOCH_KEYS if key != "activation_sha256"}}
        work_identity = auth.digest({"provider": "tikhub", "operation": env["operation"],
                                     "subject": f"account:{member['identity_id']}", "logical_due": env["logical_due"]})
        self.work_id = self.insert("capture_work_items", {
            "work_identity": work_identity, "assignment_id": assignment_id, "source_plan_id": self.plan_id,
            "account_id": member["account_id"], "provider": "tikhub", "operation": env["operation"],
            "due_at": created, "data_business_day": work_day, "state": "terminal",
            "reason": "" if complete else "page_cap_hit", "envelope_json": auth.canonical(env),
            "created_at": created, "updated_at": finished, "completed_at": finished})
        slot_id = self.insert("fetch_slots", {
            "account_id": member["account_id"], "stage": "discovery",
            "window_key": env["logical_due"] + ":cursor:" + auth.digest(0)[:24],
            "provider": "TikHub", "adapter_version": "capture-runtime-v1", "status": "succeeded",
            "attempt_count": 1, "created_at": created, "updated_at": finished})
        fetch_id = self.insert("fetch_attempts", {
            "slot_id": slot_id, "attempt_number": 1, "request_started_at": created,
            "response_finished_at": finished, "http_status": 200, "billed": 0})
        self.raw_path = self.fixture.root / "catalog-offline-page.json"
        self.raw_path.write_text(auth.canonical({"data": {"aweme_list": [], "has_more": not complete,
                                                        "max_cursor": 0 if complete else 20}}))
        body = self.raw_path.read_bytes()
        self.raw_id = self.insert("provider_raw_responses", {
            "fetch_attempt_id": fetch_id, "account_id": member["account_id"], "provider": "TikHub",
            "operation": env["operation"], "local_path": str(self.raw_path),
            "sha256": hashlib.sha256(body).hexdigest(), "byte_size": len(body), "http_status": 200,
            "captured_at": finished, "source": "offline_fixture"})
        evidence = {"contract_version": "capture-runtime-v1", "work_id": self.work_id,
                    "complete": complete, "terminal_cursor": complete, "all_raw_verified": True,
                    "cap_hit": not complete, "cursor_loop": False,
                    "disposition": "complete" if complete else "partial",
                    "seen": 0, "valid": 0, "missing": 0, "invalid": 0, "unavailable": 0,
                    "raw_response_ids": [self.raw_id],
                    **{key: env[key] for key in ("identity_id", "account_id", "platform", "operation",
                                                "window_start", "window_end")}}
        identity = {"work_identity": work_identity, "business_day": work_day, "catalog_plan_id": self.plan_id}
        scan_id = durable_runs.scan_identity("capture_integrated_work", identity)
        details = {"contract_version": durable_runs.CONTRACT_VERSION, "identity": identity, "scan_id": scan_id,
                   "complete": complete, "checkpoint": {"complete": complete, "last_result": evidence}}
        terminal = {"status": "succeeded" if complete else "failed", "started_at": created,
                    "completed_at": finished, "details_json": auth.canonical(details)}
        self.run_id = self.insert("scheduler_runs", {
            "job_id": "capture_integrated_work", "scheduled_for": "scan:" + scan_id, **terminal})
        self.attempt_id = self.insert("scheduler_run_attempts", {
            "scheduler_run_id": self.run_id, "attempt_number": 1, "invocation_source": "scheduled", **terminal})
        self.quality_id = self.insert("data_quality_receipts", {
            "scope_key": f"capture-scan:{self.work_id}", "cutoff_at": finished,
            "payload_json": auth.canonical(evidence), "recorded_at": finished, "receipt_sha256": auth.digest(evidence)})
        if complete:
            watermark = {key: value for key, value in evidence.items() if key not in {"contract_version", "work_id"}}
            self.watermark_id = self.insert("capture_watermarks", {
                "work_id": self.work_id, "provider": "tikhub", "operation": env["operation"],
                "scope_key": "douyin:" + member["uid"], "complete_through": env["window_end"],
                "cursor_json": "null", "evidence_json": auth.canonical(watermark), "recorded_at": finished})
        self.connection.commit()
        self.runtime.gates(iso(upper + timedelta(hours=2)))
        self.assertEqual(self.connection.execute("PRAGMA foreign_key_check").fetchall(), [])

    def seal(self, *, complete):
        coverage = scan_receipts.runtime_coverage(self.connection, at=self.cutoff)
        self.assertEqual(coverage["complete"], complete, coverage)
        self.assertEqual(coverage["days"][0]["coverage_contract"], catalog.CONTRACT)
        receipt = runtime_receipts.record_profile_day_coverage_receipt(
            db_path=self.fixture.db, cutoff_at=self.cutoff, evidence_root=self.fixture.root / "day-evidence")
        self.assertEqual(receipt["summary"]["complete"], complete)
        self.assertEqual(receipt["scope"]["source_binding"]["catalog"]["contract"], catalog.SOURCE_CONTRACT)
        return receipt

    def read_without_raw(self):
        before = self.connection.total_changes
        with patch.object(raw_archive, "read_response_entity", side_effect=AssertionError("reader opened raw page")):
            receipt = runtime_receipts.read_profile_day_coverage_receipt(self.connection, at=self.cutoff)
            readiness = runtime_receipts.current_activation_readiness(self.connection, at=self.cutoff)
        self.assertEqual(self.connection.total_changes, before)
        return receipt, readiness

    def test_complete_catalog_can_seal_read_and_become_ready_on_schema20_and21(self):
        for schema in (20, 21):
            with self.subTest(schema=schema):
                if schema == 21:
                    self.runtime.use_fixture(schema21=True)
                    self.bind_fixture()
                self.seed_catalog_day()
                sealed = self.seal(complete=True)
                # Only sealing performs deep raw validation; the reader consumes
                # the frozen metadata/attempt/quality/watermark source binding.
                read, ready = self.read_without_raw()
                self.assertEqual(read["self_sha256"], sealed["self_sha256"])
                self.assertTrue(ready["control_readiness"], ready)
                self.assertTrue(ready["data_readiness"], ready)
                self.assertEqual(self.connection.execute("PRAGMA user_version").fetchone()[0], schema)

    def test_partial_and_catalog_switch_days_seal_without_becoming_ready(self):
        for transition in (False, True):
            with self.subTest(transition=transition):
                if transition:
                    self.runtime.use_fixture()
                    self.bind_fixture()
                self.seed_catalog_day(day="2026-09-07" if transition else "2026-09-08",
                                      complete=transition, transition=transition)
                sealed = self.seal(complete=False)
                read, ready = self.read_without_raw()
                self.assertEqual(read["self_sha256"], sealed["self_sha256"])
                self.assertTrue(ready["control_readiness"], ready)
                self.assertFalse(ready["data_readiness"], ready)
                day = read["summary"]["coverage"]["days"][0]
                self.assertEqual(day["known"], not transition)
                self.assertEqual(day["covered_identity_ids"], [])

    def test_frozen_plan_quality_watermark_and_attempt_tamper_are_rejected(self):
        self.seed_catalog_day()
        self.seal(complete=True)
        mutations = (
            ("capture_source_plans", "trg_v20_capture_source_plans_no_update", self.plan_id, "payload_json='{}'"),
            ("data_quality_receipts", "trg_v20_data_quality_receipts_no_update", self.quality_id, "payload_json='{}'"),
            ("capture_watermarks", "trg_v20_capture_watermarks_no_update", self.watermark_id, "evidence_json='{}'"),
            ("scheduler_run_attempts", "trg_scheduler_run_attempts_terminal_update", self.attempt_id, "details_json='{}'"),
        )
        for table, trigger, ident, changed in mutations:
            with self.subTest(table=table):
                self.connection.execute("SAVEPOINT corrupt_fixture")
                try:
                    with self.assertRaises(sqlite3.IntegrityError):
                        self.connection.execute(f"UPDATE {table} SET {changed} WHERE id=?", (ident,))
                    # Simulate offline corruption only inside this temporary DB;
                    # normal application SQL was proven unable to mutate it.
                    self.connection.execute(f'DROP TRIGGER "{trigger}"')
                    self.connection.execute(f"UPDATE {table} SET {changed} WHERE id=?", (ident,))
                    with self.assertRaises(runtime_receipts.RuntimeReceiptError):
                        runtime_receipts.read_profile_day_coverage_receipt(self.connection, at=self.cutoff)
                    result = self.runtime.readiness(self.cutoff)
                    self.assertFalse(result["data_readiness"], result)
                    self.assertEqual(result["reason"], "current_activation_receipt_mismatch")
                finally:
                    self.connection.execute("ROLLBACK TO corrupt_fixture")
                    self.connection.execute("RELEASE corrupt_fixture")
        self.assertTrue(self.read_without_raw()[1]["data_readiness"])


if __name__ == "__main__":
    unittest.main()
