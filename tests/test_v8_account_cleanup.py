from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from v8.account_cleanup import CleanupError, build_candidate
from v8.storage import initialize_database


class AccountCleanupTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source = self.root / "original.sqlite3"
        self.plan = {"subject_id_sets": {"exact_uid_confirmed": [1], "unresolved_phone_candidates": [3],
                                          "additional_new_uid_phone_candidates": [], "other_subjects": [2]},
                     "source_sha256": "f" * 64}
        memory = sqlite3.connect(":memory:")
        memory.row_factory = sqlite3.Row
        memory.execute("PRAGMA recursive_triggers=ON")
        memory.execute("PRAGMA foreign_keys=ON")
        initialize_database(memory, target_version=20)
        at = "2026-09-07T00:00:00Z"
        for account_id in (1, 2, 3):
            memory.execute("INSERT INTO accounts(id,phone,created_at,updated_at) VALUES (?,'',?,?)", (account_id, at, at))
        for content_id, account_id in ((1, 1), (2, 2), (3, None)):
            memory.execute("INSERT INTO content_items(id,link_id,platform,platform_content_id,canonical_url,account_id,imported_at,created_at,updated_at) "
                           "VALUES (?,?,'douyin',?,?,?, ?,?,?)", (content_id, f"a{content_id:05d}", str(content_id), f"https://example.test/{content_id}", account_id, at, at, at))
            memory.execute("INSERT INTO content_metric_snapshots(content_id,captured_at,window_key,status,source,view_count) VALUES (?,?,?,'available','fixture',?)", (content_id, at, "day", content_id * 10))
        memory.execute("INSERT INTO fetch_request_batches(id,request_scope_identity,sequence,provider,operation,parameters_json,created_at) VALUES(1,?,0,'TikHub','douyin_video_statistics','{}',?)", ("a" * 64, at))
        memory.execute("INSERT INTO fetch_slots(id,content_id,stage,window_key,provider,adapter_version,status,created_at,updated_at) VALUES(1,1,'metrics','day','TikHub','fixture','succeeded',?,?)", (at, at))
        memory.execute("INSERT INTO fetch_attempts(id,slot_id,attempt_number,request_started_at,request_batch_id) VALUES(1,NULL,1,?,1)", (at,))
        memory.execute("INSERT INTO provider_raw_responses(id,content_id,fetch_attempt_id,provider,operation,local_path,sha256,byte_size,captured_at) VALUES(1,1,1,'fixture','fixture','fixture','abc',1,?)", (at,))
        memory.execute("UPDATE content_metric_snapshots SET raw_response_id=1 WHERE content_id=1")
        memory.execute("INSERT INTO legacy_paid_scope_exclusions(scope_identity,evidence_sha256,evidence_ref,reason,created_at) VALUES(?,?, 'fixture','fixture',?)", ("b" * 64, "c" * 64, at))
        for task in ("mixed", "kept"):
            memory.execute("INSERT INTO report_tasks(id,task_type,name,period_start,period_end,creation_source,task_status,created_at,updated_at) VALUES(?,'custom',?,'2026-09-01','2026-09-02','manual','succeeded',?,?)", (task, task, at, at))
            memory.execute("INSERT INTO task_contents(task_id,content_id,inclusion_status) VALUES(?,1,'included')", (task,))
        memory.execute("INSERT INTO task_contents(task_id,content_id,inclusion_status) VALUES('mixed',2,'included')")
        memory.commit()
        with sqlite3.connect(self.source) as disk:
            memory.backup(disk)
        memory.close()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def build(self, **options):
        return build_candidate(source_database=self.source, output_directory=self.root / "output",
                               approved_plan=self.plan, expected_deleted_contents=1, **options)

    def test_physical_projection_retains_facts_and_blocks_old_paid_scope(self) -> None:
        original_hash = hashlib.sha256(self.source.read_bytes()).hexdigest()
        receipt = self.build()
        self.assertEqual(hashlib.sha256(self.source.read_bytes()).hexdigest(), original_hash)
        self.assertEqual(receipt["historical_raw_detachments"], [{"raw_response_id": 1, "archived_fetch_attempt_id": 1, "archived_transport_receipt_id": None,
                                                                 "archived_account_id": None, "archived_content_id": None}])
        with sqlite3.connect(receipt["candidate"]["path"]) as candidate:
            candidate.row_factory = sqlite3.Row
            self.assertEqual([row[0] for row in candidate.execute("SELECT id FROM accounts ORDER BY id")], [1, 3])
            self.assertEqual([tuple(row) for row in candidate.execute("SELECT content_id,view_count FROM content_metric_snapshots ORDER BY content_id")], [(1, 10), (3, 30)])
            self.assertEqual([row[0] for row in candidate.execute("SELECT id FROM report_tasks")], ["kept"])
            self.assertEqual(candidate.execute("SELECT COUNT(*) FROM fetch_attempts WHERE id=1").fetchone()[0], 0)
            self.assertIsNone(candidate.execute("SELECT fetch_attempt_id FROM provider_raw_responses WHERE id=1").fetchone()[0])
            self.assertEqual(candidate.execute("SELECT COUNT(*) FROM fetch_request_batches").fetchone()[0], 0)
            self.assertEqual(list(candidate.execute("PRAGMA foreign_key_check")), [])
            from v8.usage_settlements import SettlementError, require_scope_available
            with self.assertRaises(SettlementError):
                require_scope_available(candidate, identity="b" * 64)
            self.assertIsNotNone(candidate.execute("SELECT 1 FROM sqlite_master WHERE name='trg_metric_observations_no_delete'").fetchone())

    def test_shared_old_subject_raw_is_retained_without_losing_metric_or_fake_owner(self) -> None:
        with sqlite3.connect(self.source) as source:
            source.execute("INSERT INTO provider_raw_responses(id,content_id,provider,operation,local_path,sha256,byte_size,captured_at) VALUES(99,2,'fixture','fixture','old-fixture','abc',1,'2026-09-07T00:00:00Z')")
            source.execute("UPDATE content_metric_snapshots SET raw_response_id=99 WHERE content_id=1")
        receipt = self.build()
        with sqlite3.connect(receipt["candidate"]["path"]) as candidate:
            self.assertEqual(candidate.execute("SELECT raw_response_id,view_count FROM content_metric_snapshots WHERE content_id=1").fetchone(), (99, 10))
            self.assertEqual(candidate.execute("SELECT content_id,sha256 FROM provider_raw_responses WHERE id=99").fetchone(), (None, "abc"))
        self.assertTrue(any(row["archived_content_id"] == 2 for row in receipt["historical_raw_detachments"]))

    def test_budget_daily_and_unresolved_guards_survive_cleanup_and_midnight(self) -> None:
        from v8.account_cleanup import budget_carry
        from v8.capture import clear_billing_unknown_slot_guard_if_resolved
        from v8.provider_budget import budget_summary
        from v8.usage_settlements import SettlementError, require_scope_available
        from v8.work_readiness import WorkReadinessPass

        usages = [
            ("2026-09-06T15:59:59Z", 3, "douyin_video_detail", {"state": "billing_unknown", "category": "detail", "slot_id": 1}),
            ("2026-09-06T16:00:00Z", 4, "douyin_video_detail", {"state": "charged_unverified", "category": "detail", "slot_id": 1}),
            ("2026-09-07T01:00:00Z", 7, "douyin_video_detail", {"state": "billing_unknown", "category": "detail", "slot_id": 1, "paid_scope_identity": "d" * 64,
                "budget_day": "2026-09-07", "borrowed_from": {"metrics": 900_000}}),
            ("2026-09-07T01:00:00Z", 2, "douyin_user_posts", {"state": "reserved", "category": "reconcile"}),
            ("2026-09-07T01:00:00Z", 1, "legacy", {}),
        ]
        with sqlite3.connect(self.source) as source:
            source.row_factory = sqlite3.Row
            source.executemany("INSERT INTO provider_usage(provider,operation,currency,amount,recorded_at,details_json) VALUES('TikHub',?,'USD',?,?,?)",
                               [(operation, amount, at, json.dumps(details)) for at, amount, operation, details in usages])
            expected = {at: budget_summary(source, at=at) for at in ("2026-09-06T15:59:59Z", "2026-09-06T16:00:00Z", "2026-09-07T16:00:00Z")}
        receipt = self.build()
        with sqlite3.connect(receipt["candidate"]["path"]) as candidate:
            candidate.row_factory = sqlite3.Row
            for at, summary in expected.items():
                self.assertEqual(budget_summary(candidate, at=at), summary)
            self.assertEqual(candidate.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0], 0)
            self.assertEqual(candidate.execute("SELECT COUNT(*) FROM account_cleanup_budget_daily").fetchone()[0], 2)
            self.assertEqual(budget_carry(candidate, "2026-09-07")["lent"], {"metrics": 900_000})
            self.assertEqual(budget_carry(candidate, "2026-09-07")["received"], {"detail": 900_000})
            self.assertEqual(budget_carry(candidate, "2026-09-07")["pending_categories"],
                             {"detail": 1, "reconcile": 1})
            with self.assertRaises(SettlementError):
                require_scope_available(candidate, identity="d" * 64)
            # The original ledger is offline, so a missing current usage row
            # cannot be interpreted as a resolved bill or release its slot.
            self.assertEqual(clear_billing_unknown_slot_guard_if_resolved(candidate, slot_id=1, fallback_error_code="retry", fallback_error_message="retry"), (False, 2))
            gate = WorkReadinessPass(candidate, at="2026-09-07T01:00:00Z")._slot_gate(work_scope={"content_id": 1}, stage="metrics", window_key="day")
            self.assertFalse(gate["runnable"])
            current = candidate.execute("INSERT INTO provider_usage(provider,operation,currency,amount,recorded_at,details_json) VALUES('TikHub','douyin_video_detail','USD',1,'2026-09-07T01:00:00Z',?)", (json.dumps({"state": "completed", "category": "detail"}),))
            self.assertGreater(current.lastrowid, len(usages))
            self.assertEqual(budget_summary(candidate, at="2026-09-07T01:00:00Z")["total_microusd"], 15_000_000)
            self.assertEqual(budget_summary(candidate, at="2026-09-07T01:00:00Z", exclude_usage_id=current.lastrowid), expected["2026-09-06T16:00:00Z"])
            with self.assertRaisesRegex(sqlite3.IntegrityError, "immutable"):
                candidate.execute("DELETE FROM account_cleanup_budget_daily")

    def test_new_unclassified_table_is_a_blocker(self) -> None:
        with sqlite3.connect(self.source) as source:
            source.execute("CREATE TABLE future_state(id INTEGER PRIMARY KEY)")
        with self.assertRaisesRegex(CleanupError, "unclassified source tables"):
            self.build()

    def test_partition_change_and_existing_output_are_rejected(self) -> None:
        self.plan["subject_id_sets"]["unresolved_phone_candidates"] = []
        with self.assertRaisesRegex(CleanupError, "approved exact partition"):
            self.build()
        with self.assertRaisesRegex(CleanupError, "output directory must be new"):
            self.build()


if __name__ == "__main__":
    unittest.main()
