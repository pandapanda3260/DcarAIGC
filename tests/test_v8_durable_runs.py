from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from threading import Barrier

from v8.durable_runs import (
    DurableRunError, LostOwnership, assert_owner, checkpoint, claim_run,
    claim_run_in_transaction, finish_run, get_run, recover_run, scan_identity,
)
from v8.storage import connect, initialize_database, transaction

NOW = "2026-08-29T04:00:00Z"
DUE = "2026-08-29T04:01:00Z"


class DurableRunsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = Path(self.temp.name).resolve() / "durable.sqlite3"
        with connect(self.db) as connection:
            initialize_database(connection)
        self.identity = {
            "purpose": "daily", "platform": "douyin", "start_at": "2026-08-28T16:00:00Z",
            "end_at": "2026-08-29T16:00:00Z", "roster_snapshot_id": 1,
            "roster_snapshot_hash": "a" * 64,
        }

    def claim(self, **kwargs):
        return claim_run(
            "matrix_works_scan", self.identity, db_path=self.db,
            initial_checkpoint={"page_index": 0, "next_cursor": None},
            now=kwargs.pop("now", NOW), **kwargs,
        )

    def state(self, claim):
        return get_run(claim.scheduler_run_id, db_path=self.db)["details"]

    def save(self, claim, changes):
        with connect(self.db) as connection, transaction(connection):
            return checkpoint(connection, claim, changes, now=NOW)

    def test_identity_is_canonical_and_scope_is_frozen(self):
        claim = self.claim()
        self.assertEqual(claim.scan_id, scan_identity("matrix_works_scan", dict(reversed(list(self.identity.items())))))
        self.assertIsNone(self.claim())
        for key, value in (
            ("purpose", "historical"), ("platform", "xiaohongshu"),
            ("roster_snapshot_id", 2), ("roster_snapshot_hash", "b" * 64),
            ("end_at", "2026-08-30T16:00:00Z"),
        ):
            changed = claim_run("matrix_works_scan", {**self.identity, key: value}, db_path=self.db, now=NOW)
            self.assertNotEqual(changed.scheduler_run_id, claim.scheduler_run_id)
        self.assertEqual(self.state(claim)["identity"], self.identity)

    def test_checkpoint_changes_run_only_and_trigger_remains_strict(self):
        claim = self.claim()
        with connect(self.db) as connection:
            before = dict(connection.execute("SELECT * FROM scheduler_run_attempts WHERE id=?", (claim.attempt_id,)).fetchone())
        self.save(claim, {"page_index": 1, "next_cursor": [1, "7379190309625810185"]})
        with connect(self.db) as connection:
            after = dict(connection.execute("SELECT * FROM scheduler_run_attempts WHERE id=?", (claim.attempt_id,)).fetchone())
            self.assertEqual(before, after)
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("UPDATE scheduler_run_attempts SET details_json='{}' WHERE id=?", (claim.attempt_id,))
        self.assertEqual(self.state(claim)["checkpoint"]["page_index"], 1)

    def test_partial_resumes_same_run_with_checkpoint_and_new_fence(self):
        first = self.claim()
        self.save(first, {"page_index": 3, "next_cursor": [2, "1234567890123456789"]})
        finish_run(first, status="partial", db_path=self.db, next_resume_at=DUE, now=NOW, summary={"pages": 3})
        self.assertIsNone(self.claim())
        second = self.claim(now=DUE)
        self.assertEqual(second.scheduler_run_id, first.scheduler_run_id)
        self.assertEqual(second.attempt_number, 2)
        self.assertNotEqual(second.owner_token, first.owner_token)
        self.assertEqual(self.state(second)["checkpoint"]["page_index"], 3)
        self.assertEqual(self.state(second)["summary"], {"pages": 3})
        with self.assertRaises(LostOwnership):
            self.save(first, {"page_index": 999})
        with self.assertRaises(LostOwnership):
            finish_run(first, status="failed", db_path=self.db, now=DUE)
        self.assertEqual(self.state(second)["checkpoint"]["page_index"], 3)

    def test_completion_requires_checkpoint_and_finish_merges_latest(self):
        claim = self.claim()
        with self.assertRaises(DurableRunError):
            finish_run(claim, status="succeeded", db_path=self.db, now=NOW)
        self.save(claim, {"page_index": 2, "complete": True, "raw_id": 19})
        with self.assertRaises(DurableRunError):
            self.save(claim, {"page_index": 3})
        details = finish_run(claim, status="succeeded", db_path=self.db, now=NOW, summary={"reason": None})
        self.assertTrue(details["complete"])
        self.assertEqual(details["checkpoint"]["raw_id"], 19)
        self.assertEqual(details["checkpoint"]["page_index"], 2)
        self.assertIsNone(self.claim(now=DUE))
        with connect(self.db) as connection:
            attempt = connection.execute("SELECT * FROM scheduler_run_attempts WHERE id=?", (claim.attempt_id,)).fetchone()
            self.assertEqual(json.loads(attempt["details_json"]), details)
            self.assertEqual(attempt["status"], "succeeded")

    def test_recovery_uses_run_checkpoint_not_immutable_attempt_seed(self):
        first = self.claim()
        self.save(first, {"page_index": 7, "next_cursor": [123, "998877665544332211"]})
        self.assertFalse(recover_run(first.scheduler_run_id, expected_attempt_id=first.attempt_id + 1, db_path=self.db, now=DUE))
        self.assertTrue(recover_run(first.scheduler_run_id, expected_attempt_id=first.attempt_id, db_path=self.db, now=DUE))
        self.assertFalse(recover_run(first.scheduler_run_id, expected_attempt_id=first.attempt_id, db_path=self.db, now=DUE))
        with connect(self.db) as connection:
            attempt = connection.execute("SELECT * FROM scheduler_run_attempts WHERE id=?", (first.attempt_id,)).fetchone()
            self.assertEqual(attempt["status"], "interrupted")
            self.assertEqual(json.loads(attempt["details_json"])["checkpoint"]["page_index"], 7)
        second = self.claim(now=DUE)
        self.assertEqual(self.state(second)["checkpoint"]["next_cursor"], [123, "998877665544332211"])
        with self.assertRaises(LostOwnership):
            self.save(first, {"page_index": 8})

    def test_complete_checkpoint_survives_crash_before_finish(self):
        first = self.claim()
        self.save(first, {"complete": True, "page_index": 9})
        recover_run(first.scheduler_run_id, expected_attempt_id=first.attempt_id, db_path=self.db, now=DUE)
        second = self.claim(now=DUE)
        self.assertTrue(self.state(second)["checkpoint"]["complete"])
        finish_run(second, status="succeeded", db_path=self.db, now=DUE)
        self.assertEqual(get_run(second.scheduler_run_id, db_path=self.db)["status"], "succeeded")

    def test_caller_transaction_and_matching_owner_token_are_required(self):
        claim = self.claim()
        with connect(self.db) as connection:
            with self.assertRaises(DurableRunError):
                assert_owner(connection, claim)
            with transaction(connection), self.assertRaises(LostOwnership):
                assert_owner(connection, replace(claim, owner_token="stale"))
        self.assertEqual(self.state(claim)["checkpoint"]["page_index"], 0)

    def test_caller_owned_claim_requires_transaction_and_rolls_back_as_one_unit(self):
        with connect(self.db) as connection:
            with self.assertRaisesRegex(DurableRunError, "caller transaction"):
                claim_run_in_transaction(
                    connection,
                    "matrix_works_scan",
                    self.identity,
                    initial_checkpoint={"page_index": 0, "next_cursor": None},
                    now=NOW,
                )

        with self.assertRaisesRegex(RuntimeError, "compound fence rollback"):
            with connect(self.db) as connection, transaction(connection):
                claim = claim_run_in_transaction(
                    connection,
                    "matrix_works_scan",
                    self.identity,
                    initial_checkpoint={"page_index": 0, "next_cursor": None},
                    now=NOW,
                )
                self.assertIsNotNone(claim)
                raise RuntimeError("compound fence rollback")

        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM scheduler_runs").fetchone()[0], 0)
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM scheduler_run_attempts").fetchone()[0],
                0,
            )

        with connect(self.db) as connection, transaction(connection):
            claim = claim_run_in_transaction(
                connection,
                "matrix_works_scan",
                self.identity,
                initial_checkpoint={"page_index": 0, "next_cursor": None},
                now=NOW,
            )
        self.assertIsNotNone(claim)
        self.assertEqual(self.state(claim)["checkpoint"]["page_index"], 0)

    def test_checkpoint_and_business_write_roll_back_together(self):
        claim = self.claim()
        with self.assertRaises(RuntimeError), connect(self.db) as connection, transaction(connection):
            checkpoint(connection, claim, {"page_index": 4}, now=NOW)
            connection.execute("INSERT INTO accounts(phone,phone_normalized,operator_name,account_type,content_direction,enabled,created_at,updated_at) VALUES ('',NULL,'test','unknown','unknown',1,?,?)", (NOW, NOW))
            raise RuntimeError("crash")
        self.assertEqual(self.state(claim)["checkpoint"]["page_index"], 0)
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM accounts").fetchone()[0], 0)

    def test_invalid_partial_and_checkpoint_are_not_written(self):
        claim = self.claim()
        with self.assertRaises(DurableRunError):
            finish_run(claim, status="partial", db_path=self.db, now=NOW)
        with self.assertRaises(DurableRunError):
            finish_run(claim, status="partial", next_resume_at="2026-08-29T03:00:00Z", db_path=self.db, now=NOW)
        with self.assertRaises(DurableRunError):
            self.save(claim, {"complete": 1})
        self.assertIs(self.state(claim)["checkpoint"]["complete"], False)

    def test_terminal_failure_is_not_retried_as_new_work(self):
        claim = self.claim()
        finish_run(claim, status="failed", db_path=self.db, now=NOW, summary={"reason": "terminal"})
        self.assertIsNone(self.claim(now=DUE))
        self.assertEqual(get_run(claim.scheduler_run_id, db_path=self.db)["status"], "failed")


    def test_new_run_requires_real_false_initial_completion(self):
        for value in (True, 0, None, ""):
            with self.subTest(value=value), self.assertRaises(DurableRunError):
                claim_run(
                    "invalid_initial_state", self.identity, db_path=self.db,
                    initial_checkpoint={"complete": value}, now=NOW,
                )
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM scheduler_runs").fetchone()[0], 0)



    def test_concurrent_rosters_share_one_calendar_scope_and_recovery_keeps_first_identity(self):
        job = "pipeline_round:matrix_works_scan"
        scope_key = {"beijing_day": "2026-08-29", "round_id": "matrix_works_scan:02:10"}
        identities = [
            self.identity,
            {**self.identity, "roster_snapshot_id": 2, "roster_snapshot_hash": "b" * 64},
        ]
        ready = Barrier(2)

        def compete(identity):
            ready.wait(timeout=5)
            return identity, claim_run(
                job, identity, scope_key=scope_key, db_path=self.db, now=NOW,
                initial_checkpoint={"page_index": 0, "seed_roster_id": identity["roster_snapshot_id"]},
            )

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(compete, identity) for identity in identities]
            results = [future.result(timeout=10) for future in futures]
        winners = [(identity, claim) for identity, claim in results if claim is not None]
        self.assertEqual(len(winners), 1)
        frozen_identity, first = winners[0]
        rejected_identity = next(identity for identity, claim in results if claim is None)
        state = self.state(first)
        self.assertEqual(state["identity"], frozen_identity)
        self.assertEqual(state["scope_key"], scope_key)
        self.assertEqual(state["checkpoint"]["seed_roster_id"], frozen_identity["roster_snapshot_id"])
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM scheduler_runs").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM scheduler_run_attempts").fetchone()[0], 1)
        self.save(first, {"page_index": 7, "next_cursor": [19, "7379190309625810185"]})
        self.assertTrue(recover_run(
            first.scheduler_run_id, expected_attempt_id=first.attempt_id, db_path=self.db, now=DUE,
        ))
        resumed = claim_run(
            job, {**rejected_identity, "end_at": "2026-08-30T16:00:00Z"},
            scope_key=scope_key, db_path=self.db, now=DUE,
            initial_checkpoint={"page_index": 999, "seed_roster_id": rejected_identity["roster_snapshot_id"]},
        )
        self.assertIsNotNone(resumed)
        self.assertEqual(resumed.scheduler_run_id, first.scheduler_run_id)
        self.assertEqual(resumed.attempt_number, 2)
        self.assertEqual(resumed.scan_id, first.scan_id)
        self.assertEqual(self.state(resumed)["identity"], frozen_identity)
        self.assertEqual(self.state(resumed)["checkpoint"], {
            "complete": False, "page_index": 7,
            "next_cursor": [19, "7379190309625810185"],
            "seed_roster_id": frozen_identity["roster_snapshot_id"],
        })
        with self.assertRaises(LostOwnership):
            self.save(first, {"page_index": 999})
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM scheduler_runs").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM scheduler_run_attempts").fetchone()[0], 2)

if __name__ == "__main__":
    unittest.main()
