from __future__ import annotations

import sqlite3
import unittest
from unittest.mock import patch

from tests import test_v8_tikhub_scan as fixtures
from tests.roster_fixture import accept_roster
from v8 import pipeline, providers, tikhub_scan
from v8.durable_runs import get_run
from v8.reconcile_control import reconcile_budget_scope
from v8.storage import connect, transaction


class LocalMaterializationEpochTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.TikHubScanTest(methodName="runTest")
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()
        self.db = self.fixture.db
        with patch.object(providers, "materialize_account_discovery_page", side_effect=sqlite3.OperationalError("local write failure")):
            self.pending = self.fixture.scan(call_override=lambda *_: fixtures.result(
                fixtures.dy_page([fixtures.dy_item()], more=True, cursor=7),
            ))
        self.run_id = self.pending["scheduler_run_id"]
        self.assertEqual(self.pending["reason"], "materialization_pending")
        self.original_scope = get_run(self.run_id, db_path=self.db)["details"]["identity"]
        with connect(self.db) as connection, transaction(connection):
            accept_roster(connection, [1, 2], accepted_at=fixtures.later(fixtures.NOW, 1))
        self.ledger = self._ledger()

    def _ledger(self):
        with connect(self.db) as connection:
            return {table: [tuple(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY id")]
                    for table in ("provider_usage", "paid_provider_dispatch_events")}

    def _resume(self):
        with patch.object(tikhub_scan, "_monotonic_now", return_value=0.0):
            return tikhub_scan.resume_local_materialization(
                self.run_id, db_path=self.db, raw_root=self.fixture.raw_root,
                now=fixtures.later(), max_items=50, deadline=60.0,
            )

    def test_historical_pending_replays_without_reauthorizing_old_paid_scope(self):
        with patch.object(tikhub_scan, "_raw", side_effect=AssertionError("local replay must not fetch")):
            result = self._resume()
        self.assertTrue(result["materialization_finalized"])
        self.assertEqual(result["succeeded_items"], 1)
        self.assertIsNone(self.fixture.state(self.run_id)["pending_materialization"])
        self.assertEqual(get_run(self.run_id, db_path=self.db)["details"]["identity"], self.original_scope)
        self.assertEqual(self._ledger(), self.ledger)
        self.assertIsNone(tikhub_scan._LOCAL_REPLAY.get())
        refused = self.fixture.resume(self.run_id, now=fixtures.later(fixtures.NOW, 305),
                                      call_override=lambda *_: self.fail("old scope must not buy the next page"))
        self.assertEqual(refused["reason"], "profile_superseded")
        self.assertEqual(self._ledger(), self.ledger)

    def test_old_failed_epoch_attempt_is_preserved_and_replayed_by_new_local_attempt(self):
        failed = self.fixture.resume(self.run_id, call_override=lambda *_: self.fail("old scope must not send"))
        self.assertEqual((failed["status"], failed["reason"]), ("failed", "profile_superseded"))
        with connect(self.db) as connection:
            original = tuple(connection.execute("SELECT * FROM scheduler_run_attempts WHERE id=?", (failed["attempt_id"],)).fetchone())
        # Exercise the actual reconcile selector too: failed epoch debt must
        # remain visible instead of silently dropping out of the replay queue.
        with reconcile_budget_scope():
            result = pipeline._replay_materialization_debt(db_path=self.db, at=fixtures.later(fixtures.NOW, 306))
        self.assertEqual(len(result), 1)
        self.assertTrue(result[0]["materialization_finalized"])
        self.assertGreater(result[0]["attempt_id"], failed["attempt_id"])
        with connect(self.db) as connection:
            self.assertEqual(tuple(connection.execute("SELECT * FROM scheduler_run_attempts WHERE id=?", (failed["attempt_id"],)).fetchone()), original)
        self.assertEqual(self._ledger(), self.ledger)
        self.assertEqual(get_run(self.run_id, db_path=self.db)["details"]["identity"], self.original_scope)

    def test_corrupt_raw_does_not_reopen_failed_parent(self):
        failed = self.fixture.resume(self.run_id, call_override=lambda *_: self.fail("no send"))
        before = get_run(self.run_id, db_path=self.db)
        with patch.object(tikhub_scan, "_eligible_materialization_page", side_effect=tikhub_scan.TikHubScanError(
            "materialization_integrity_error", "damaged raw evidence",
        )), self.assertRaises(tikhub_scan.TikHubScanError):
            self._resume()
        self.assertEqual(get_run(self.run_id, db_path=self.db), before)
        self.assertEqual(failed["status"], "failed")
        self.assertEqual(self._ledger(), self.ledger)
        self.assertIsNone(tikhub_scan._LOCAL_REPLAY.get())

    def test_operator_disabled_account_stays_disabled(self):
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE accounts SET enabled=0 WHERE id=1")
        result = self._resume()
        self.assertEqual(result["reason"], "operator_paused")
        self.assertEqual(result["processed_items"], 0)
        self.assertEqual(self._ledger(), self.ledger)
        self.assertIsNone(tikhub_scan._LOCAL_REPLAY.get())
