"""Frozen catalog coverage with SQLite, real raw files and real page parsing."""

import copy
import hashlib
import json
from pathlib import Path
import socket
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from v8 import capture_day_coverage as coverage, profile_activations, scan_receipts

DAY = "2026-09-10"
LOWER = "2026-09-09T16:00:00Z"
UPPER = "2026-09-10T16:00:00Z"
CUTOFF = "2026-09-10T17:00:00Z"
CREATED = "2026-09-10T16:10:00Z"
FINISHED = "2026-09-10T16:11:00Z"


class CatalogDayCoverageTest(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            patch.object(
                socket.socket,
                "connect",
                side_effect=AssertionError("network forbidden"),
            )
        )
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.c = sqlite3.connect(":memory:")
        self.c.row_factory = sqlite3.Row
        self.addCleanup(self.c.close)
        self.c.executescript("""
            CREATE TABLE capture_source_plans(id INTEGER PRIMARY KEY,mode TEXT,created_at TEXT,business_day TEXT,plan_sha256 TEXT,payload_json TEXT);
            CREATE TABLE capture_work_items(id INTEGER PRIMARY KEY,work_identity TEXT,account_id INTEGER,content_id INTEGER,provider TEXT,operation TEXT,state TEXT,reason TEXT,completed_at TEXT,created_at TEXT,updated_at TEXT,assignment_id INTEGER,source_plan_id INTEGER,data_business_day TEXT,envelope_json TEXT);
            CREATE TABLE scheduler_runs(id INTEGER PRIMARY KEY,job_id TEXT,scheduled_for TEXT,status TEXT,details_json TEXT,started_at TEXT,completed_at TEXT,root_run_id INTEGER,continuation_sequence INTEGER,charge_business_day TEXT);
            CREATE TABLE scheduler_run_attempts(id INTEGER PRIMARY KEY,scheduler_run_id INTEGER,attempt_number INTEGER,status TEXT,started_at TEXT,completed_at TEXT,details_json TEXT);
            CREATE TABLE data_quality_receipts(id INTEGER PRIMARY KEY,scope_key TEXT,cutoff_at TEXT,payload_json TEXT,recorded_at TEXT,receipt_sha256 TEXT);
            CREATE TABLE capture_watermarks(id INTEGER PRIMARY KEY,work_id INTEGER,provider TEXT,operation TEXT,scope_key TEXT,complete_through TEXT,cursor_json TEXT,evidence_json TEXT,recorded_at TEXT);
            CREATE TABLE provider_raw_responses(id INTEGER PRIMARY KEY,fetch_attempt_id INTEGER,account_id INTEGER,content_id INTEGER,provider TEXT,operation TEXT,sha256 TEXT,byte_size INTEGER,captured_at TEXT,http_status INTEGER,transport_receipt_id INTEGER,local_path TEXT,raw_blob_id INTEGER);
            CREATE TABLE fetch_attempts(id INTEGER PRIMARY KEY,slot_id INTEGER);
            CREATE TABLE fetch_slots(id INTEGER PRIMARY KEY,account_id INTEGER,content_id INTEGER,stage TEXT,window_key TEXT);
        """)
        self.active = {
            "activation_id": 4,
            "activation_sha256": "a" * 64,
            "profile_id": "integrated_route_v1",
            "roster_snapshot_id": 3,
            "roster_members_sha256": "b" * 64,
        }
        self.enterContext(
            patch.object(profile_activations, "activation_at", return_value=self.active)
        )
        self.member = {
            "identity_id": 111,
            "account_identity_id": 111,
            "account_id": 222,
            "platform": "douyin",
            "uid": "10000001",
            "locator_sha256": "c" * 64,
            "eligible": True,
            "reason_code": "eligible",
            "account_status": "daily",
        }
        self.snapshot = self.make_snapshot([self.member])
        self.add_plan(1, "2026-09-09", "2026-09-08T16:10:00Z")
        self.add_plan(2, DAY, "2026-09-09T16:10:00Z")
        self.add_plan(3, "2026-09-11", "2026-09-10T16:10:00Z")
        self.env = {
            "contract_version": "capture-runtime-v1",
            "identity_id": 111,
            "account_id": 222,
            "platform": "douyin",
            "uid": "10000001",
            "content_id": None,
            "stage": "discovery",
            "capture_stage": "discovery",
            "source_stage": "discovery",
            "category": "reconcile",
            "operation": "douyin_user_posts",
            "logical_due": "discovery:" + CREATED,
            "assignment_id": 1,
            "source_plan_id": 3,
            "catalog_plan_id": 3,
            "window_start": "2026-09-08T16:00:00Z",
            "window_end": CREATED,
            **{k: v for k, v in self.active.items() if k != "activation_sha256"},
        }
        self.work_identity = coverage.planning.digest(
            {
                "provider": "tikhub",
                "operation": "douyin_user_posts",
                "subject": "account:111",
                "logical_due": self.env["logical_due"],
            }
        )
        self.c.execute(
            "INSERT INTO capture_work_items VALUES(1,?,222,NULL,'tikhub','douyin_user_posts','terminal','',?,?,?,1,3,'2026-09-11',?)",
            (self.work_identity, FINISHED, CREATED, FINISHED, json.dumps(self.env)),
        )
        self.raw_path = self.root / "raw.json"
        self.raw_path.write_text(
            json.dumps({"data": {"aweme_list": [], "has_more": False, "max_cursor": 0}})
        )
        body = self.raw_path.read_bytes()
        self.c.execute(
            "INSERT INTO provider_raw_responses VALUES(1,1,222,NULL,'TikHub','douyin_user_posts',?,?,?,200,NULL,?,NULL)",
            (hashlib.sha256(body).hexdigest(), len(body), FINISHED, str(self.raw_path)),
        )
        self.c.execute("INSERT INTO fetch_attempts VALUES(1,1)")
        self.c.execute(
            "INSERT INTO fetch_slots VALUES(1,222,NULL,'discovery',?)",
            (self.env["logical_due"] + ":cursor:" + coverage.planning.digest(0)[:24],),
        )
        self.evidence = {
            "contract_version": "capture-runtime-v1",
            "work_id": 1,
            "complete": True,
            "terminal_cursor": True,
            "all_raw_verified": True,
            "cap_hit": False,
            "cursor_loop": False,
            "disposition": "complete",
            "seen": 0,
            "valid": 0,
            "missing": 0,
            "invalid": 0,
            "unavailable": 0,
            "raw_response_ids": [1],
            **{
                k: self.env[k]
                for k in (
                    "identity_id",
                    "account_id",
                    "platform",
                    "operation",
                    "window_start",
                    "window_end",
                )
            },
        }
        self.write_receipts()

    def make_snapshot(self, members):
        selection = [
            {
                k: m[k]
                for k in (
                    "account_identity_id",
                    "account_id",
                    "platform",
                    "uid",
                    "locator_sha256",
                )
            }
            for m in members
        ]
        snap = {
            "contract": coverage.SNAPSHOT_CONTRACT,
            "policy_sha256": "d" * 64,
            "eligibility": {
                "eligible_members": members,
                "excluded_members": [],
                "selection_sha256": coverage.planning.digest(selection),
            },
        }
        return {**snap, "snapshot_sha256": coverage.planning.digest(snap)}

    def add_plan(self, ident, day, created, snapshot=None):
        snapshot = snapshot or self.snapshot
        payload = {
            "contract_version": "capture-runtime-v1",
            "business_day": day,
            "shadow": False,
            "catalog_mode": "active",
            **self.active,
            "catalog_snapshot": snapshot,
            "catalog_snapshot_sha256": snapshot["snapshot_sha256"],
            "cohort": [
                {k: m[k] for k in coverage.MEMBER_KEYS}
                for m in snapshot["eligibility"]["eligible_members"]
            ],
        }
        self.c.execute(
            "INSERT OR REPLACE INTO capture_source_plans VALUES(?,'active',?,?,?,?)",
            (
                ident,
                created,
                day,
                coverage.planning.digest(payload),
                json.dumps(payload),
            ),
        )

    def write_receipts(self, status="succeeded"):
        identity = {
            "work_identity": self.work_identity,
            "business_day": "2026-09-11",
            "catalog_plan_id": 3,
        }
        scan = coverage.durable_runs.scan_identity("capture_integrated_work", identity)
        details = {
            "contract_version": coverage.durable_runs.CONTRACT_VERSION,
            "identity": identity,
            "scan_id": scan,
            "complete": self.evidence["complete"],
            "checkpoint": {
                "complete": self.evidence["complete"],
                "last_result": self.evidence,
            },
        }
        self.c.execute(
            "INSERT OR REPLACE INTO scheduler_runs VALUES(1,'capture_integrated_work',?,?,?,?,?,NULL,NULL,NULL)",
            ("scan:" + scan, status, json.dumps(details), CREATED, FINISHED),
        )
        self.c.execute(
            "INSERT OR REPLACE INTO scheduler_run_attempts VALUES(1,1,1,?,?,?,?)",
            (status, CREATED, FINISHED, json.dumps(details)),
        )
        self.c.execute(
            "INSERT OR REPLACE INTO data_quality_receipts VALUES(1,'capture-scan:1',?,?,?,?)",
            (
                FINISHED,
                json.dumps(self.evidence),
                FINISHED,
                coverage.planning.digest(self.evidence),
            ),
        )
        wm = {
            k: v
            for k, v in self.evidence.items()
            if k not in {"contract_version", "work_id"}
        }
        self.c.execute(
            "INSERT OR REPLACE INTO capture_watermarks VALUES(1,1,'tikhub','douyin_user_posts','douyin:10000001',?,'null',?,?)",
            (self.env["window_end"], json.dumps(wm), FINISHED),
        )

    def result(self, cutoff=CUTOFF):
        before = self.c.total_changes
        self.c.execute("pragma query_only=on")
        try:
            result = coverage.catalog_day_coverage(self.c, day=DAY, cutoff_at=cutoff)
            self.assertEqual(before, self.c.total_changes)
            return result
        finally:
            self.c.execute("pragma query_only=off")

    def test_complete_empty_provider_page_covers_exact_catalog_member(self):
        result = self.result()
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["eligible_identity_ids"], [111])
        self.assertEqual(result["roster_snapshot_id"], 3)
        self.assertEqual(result["source_family"], "system")
        self.assertEqual(result["tikhub_run_ids"], [])
        self.assertEqual(result["integrated_run_ids"], [1])
        self.assertTrue(
            coverage.validate_source_binding(self.c, result["source_binding"], CUTOFF)[
                "valid"
            ]
        )

    def test_terminal_cap_or_loop_is_not_complete_even_with_terminal_work(self):
        for reason in ("page_cap_hit", "cursor_loop"):
            with self.subTest(reason=reason):
                self.c.execute("UPDATE capture_work_items SET reason=?", (reason,))
                self.evidence.update(
                    complete=False, disposition="partial", terminal_cursor=False
                )
                self.write_receipts("failed")
                self.c.execute("DELETE FROM capture_watermarks")
                result = self.result()
                self.assertFalse(result["complete"])
                self.assertEqual(result["covered_identity_ids"], [])

    def test_missing_quality_watermark_or_attempt_cannot_complete(self):
        for table in (
            "data_quality_receipts",
            "capture_watermarks",
            "scheduler_run_attempts",
        ):
            with self.subTest(table=table):
                self.c.execute(f"DELETE FROM {table}")
                self.assertFalse(self.result()["complete"])
                self.write_receipts()

    def test_cutoff_before_completion_does_not_consume_future_evidence(self):
        self.assertFalse(self.result(CREATED)["complete"])
        self.c.execute(
            "UPDATE data_quality_receipts SET recorded_at='2026-09-10T18:00:00Z'"
        )
        self.assertFalse(self.result()["complete"])

    def test_raw_tamper_and_open_cursor_fail_deep_validation(self):
        self.raw_path.write_text("{}")
        self.assertFalse(self.result()["complete"])
        body = json.dumps(
            {"data": {"aweme_list": [], "has_more": True, "max_cursor": 20}}
        ).encode()
        self.raw_path.write_bytes(body)
        self.c.execute(
            "UPDATE provider_raw_responses SET sha256=?,byte_size=?",
            (hashlib.sha256(body).hexdigest(), len(body)),
        )
        self.assertFalse(self.result()["complete"])

    def test_raw_identity_or_cursor_binding_mismatch_fails(self):
        self.c.execute("UPDATE provider_raw_responses SET account_id=999")
        self.assertFalse(self.result()["complete"])
        self.c.execute("UPDATE provider_raw_responses SET account_id=222")
        self.c.execute("UPDATE fetch_slots SET window_key='wrong'")
        self.assertFalse(self.result()["complete"])

    def test_two_page_chain_requires_real_continuation_and_matching_slot(self):
        body = json.dumps(
            {"data": {"aweme_list": [], "has_more": True, "max_cursor": 20}}
        ).encode()
        self.raw_path.write_bytes(body)
        self.c.execute(
            "UPDATE provider_raw_responses SET sha256=?,byte_size=?",
            (hashlib.sha256(body).hexdigest(), len(body)),
        )
        second = self.root / "second.json"
        second.write_text(
            json.dumps({"data": {"aweme_list": [], "has_more": False, "max_cursor": 0}})
        )
        body = second.read_bytes()
        self.c.execute(
            "INSERT INTO provider_raw_responses VALUES(2,2,222,NULL,'TikHub','douyin_user_posts',?,?,?,200,NULL,?,NULL)",
            (hashlib.sha256(body).hexdigest(), len(body), FINISHED, str(second)),
        )
        self.c.execute("INSERT INTO fetch_attempts VALUES(2,2)")
        self.c.execute(
            "INSERT INTO fetch_slots VALUES(2,222,NULL,'discovery',?)",
            (self.env["logical_due"] + ":cursor:" + coverage.planning.digest(20)[:24],),
        )
        self.evidence["raw_response_ids"] = [1, 2]
        self.write_receipts()
        self.assertTrue(self.result()["complete"])
        self.c.execute("UPDATE fetch_slots SET window_key='wrong' WHERE id=2")
        self.assertFalse(self.result()["complete"])

    def test_unknown_binding_cannot_forge_complete(self):
        self.c.execute("DELETE FROM capture_source_plans WHERE id=1")
        binding = self.result()["source_binding"]
        binding["complete"] = True
        binding["binding_sha256"] = coverage.planning.digest(
            {k: v for k, v in binding.items() if k != "binding_sha256"}
        )
        with self.assertRaises(ValueError):
            coverage.validate_source_binding(self.c, binding, CUTOFF)

    def test_window_must_cover_entire_report_day(self):
        self.env["window_start"] = "2026-09-10T02:00:00Z"
        self.c.execute(
            "UPDATE capture_work_items SET envelope_json=?", (json.dumps(self.env),)
        )
        self.assertFalse(self.result()["complete"])

    def test_scope_change_and_switch_day_are_unknown_but_keep_runtime_context(self):
        self.c.execute("DELETE FROM capture_source_plans WHERE id=1")
        result = self.result()
        self.assertFalse(result["known"])
        self.assertFalse(result["complete"])
        self.assertEqual(result["activation_id"], 4)
        self.assertTrue(
            coverage.validate_source_binding(self.c, result["source_binding"], CUTOFF)[
                "valid"
            ]
        )
        self.add_plan(1, "2026-09-09", "2026-09-08T16:10:00Z")
        other = {**self.member, "identity_id": 112, "account_identity_id": 112}
        self.add_plan(
            2, DAY, "2026-09-09T16:10:00Z", self.make_snapshot([self.member, other])
        )
        self.assertFalse(self.result()["known"])

    def test_missing_day_plan_and_empty_scope_do_not_complete(self):
        self.c.execute("DELETE FROM capture_source_plans WHERE id=2")
        self.assertFalse(self.result()["known"])
        self.add_plan(2, DAY, "2026-09-09T16:10:00Z", self.make_snapshot([]))
        self.assertFalse(self.result()["complete"])

    def test_missing_snapshot_marker_remains_unknown(self):
        row = self.c.execute("SELECT * FROM capture_source_plans WHERE id=2").fetchone()
        value = json.loads(row["payload_json"])
        value.pop("catalog_snapshot")
        self.c.execute(
            "UPDATE capture_source_plans SET payload_json=?,plan_sha256=? WHERE id=2",
            (json.dumps(value), coverage.planning.digest(value)),
        )
        self.assertFalse(self.result()["known"])

    def test_non_catalog_keeps_existing_path(self):
        self.c.execute("DELETE FROM capture_source_plans")
        self.assertIsNone(self.result())

    def test_frozen_successful_attempt_survives_later_run_retry(self):
        result = self.result()
        binding = result["source_binding"]
        self.c.execute(
            "UPDATE scheduler_runs SET status='running',details_json='{}',completed_at=NULL"
        )
        with patch.object(
            coverage.raw_archive,
            "read_response_entity",
            side_effect=AssertionError("hot path read raw"),
        ):
            self.assertTrue(
                coverage.validate_source_binding(self.c, binding, CUTOFF)["valid"]
            )
        self.c.execute("UPDATE scheduler_run_attempts SET details_json='{}'")
        with self.assertRaises(ValueError):
            coverage.validate_source_binding(self.c, binding, CUTOFF)

    def test_source_binding_detects_raw_metadata_and_scope_tamper(self):
        binding = self.result()["source_binding"]
        self.c.execute("UPDATE provider_raw_responses SET sha256='changed'")
        with self.assertRaises(ValueError):
            coverage.validate_source_binding(self.c, binding, CUTOFF)

    def test_two_members_do_not_complete_with_only_one_receipt(self):
        other = {
            **self.member,
            "identity_id": 112,
            "account_identity_id": 112,
            "account_id": 223,
            "uid": "10000002",
        }
        self.snapshot = self.make_snapshot([self.member, other])
        self.add_plan(1, "2026-09-09", "2026-09-08T16:10:00Z")
        self.add_plan(2, DAY, "2026-09-09T16:10:00Z")
        self.add_plan(3, "2026-09-11", CREATED)
        result = self.result()
        self.assertTrue(result["known"])
        self.assertFalse(result["complete"])
        self.assertEqual(result["blocked_identity_ids"], [112])
        self.assertEqual(result["success_percentage"], 50)

    def test_cross_day_child_uses_root_schedule_and_frozen_child_attempt(self):
        root = json.loads(
            self.c.execute(
                "SELECT details_json FROM scheduler_runs WHERE id=1"
            ).fetchone()[0]
        )
        details = copy.deepcopy(root)
        details["identity"].update(
            data_business_day="2026-09-11",
            business_day="2026-09-12",
            continuation={
                "root_run_id": 1,
                "sequence": 1,
                "charge_business_day": "2026-09-12",
            },
        )
        details["scan_id"] = coverage.durable_runs.scan_identity(
            "capture_integrated_work", details["identity"]
        )
        self.c.execute(
            "INSERT INTO scheduler_runs SELECT 2,job_id,scheduled_for,'succeeded',?,started_at,completed_at,1,1,'2026-09-12' FROM scheduler_runs WHERE id=1",
            (json.dumps(details),),
        )
        self.c.execute(
            "UPDATE scheduler_run_attempts SET scheduler_run_id=2,details_json=?",
            (json.dumps(details),),
        )
        self.c.execute(
            "UPDATE scheduler_runs SET status='partial',details_json='{}' WHERE id=1"
        )
        result = self.result()
        self.assertTrue(result["complete"], result)
        binding = result["source_binding"]
        self.assertEqual(result["integrated_run_ids"], [2])
        self.c.execute(
            "UPDATE scheduler_runs SET status='running',details_json='{}' WHERE id=2"
        )
        self.assertTrue(
            coverage.validate_source_binding(self.c, binding, CUTOFF)["valid"]
        )
        self.c.execute("UPDATE scheduler_runs SET root_run_id=999 WHERE id=2")
        with self.assertRaises(ValueError):
            coverage.validate_source_binding(self.c, binding, CUTOFF)

    def test_old_partial_remains_diagnostic_after_new_complete_and_aggregate_traceable(
        self,
    ):
        self.c.execute(
            "INSERT INTO capture_work_items SELECT 2,work_identity||':old',account_id,content_id,provider,operation,'terminal','page_cap_hit',completed_at,created_at,updated_at,assignment_id,source_plan_id,data_business_day,envelope_json FROM capture_work_items WHERE id=1"
        )
        result = scan_receipts.coverage(
            self.c, period_start=DAY, period_end=DAY, cutoff_at=CUTOFF
        )
        self.assertTrue(result["complete"], result)
        self.assertTrue(result["scan_traceable"], result)
        self.assertEqual(result["scan_errors"], {})
        self.assertIn("2", result["days"][0]["diagnostic_scan_errors"])
        self.assertEqual(
            self.c.execute(
                "SELECT reason FROM capture_work_items WHERE id=2"
            ).fetchone()[0],
            "page_cap_hit",
        )

    def test_locator_provenance_refresh_is_same_scope_but_cadence_or_locator_change_is_not(
        self,
    ):
        member = {**self.member, "locator_evidence": {"source_raw_response_id": 99}}
        self.add_plan(2, DAY, "2026-09-09T16:10:00Z", self.make_snapshot([member]))
        result = self.result()
        self.assertTrue(result["complete"], result)
        self.assertTrue(
            coverage.validate_source_binding(self.c, result["source_binding"], CUTOFF)[
                "valid"
            ]
        )
        for change in ({"account_status": "weekly"}, {"locator_sha256": "different"}):
            self.add_plan(
                2,
                DAY,
                "2026-09-09T16:10:00Z",
                self.make_snapshot([{**member, **change}]),
            )
            self.assertFalse(self.result()["known"])

    def test_unknown_reason_must_match_frozen_plan_inputs(self):
        self.c.execute("DELETE FROM capture_source_plans WHERE id=1")
        binding = self.result()["source_binding"]
        binding["reason"] = "fabricated"
        binding["binding_sha256"] = coverage.planning.digest(
            {k: v for k, v in binding.items() if k != "binding_sha256"}
        )
        with self.assertRaises(ValueError):
            coverage.validate_source_binding(self.c, binding, CUTOFF)


if __name__ == "__main__":
    unittest.main()
