"""``process_commands`` must never let one poison command stall the worker."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from v8 import capture_commands, durable_runs, schema_v20
from v8.storage import connect, initialize_database
from v8.runtime_database import (DatabaseAccessMode, FileIdentity, InstalledWriterContract,
    ResolvedDatabaseAccess, acquire_writer_lock)


class ProcessCommandsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name).resolve() / "v20.sqlite3"
        with connect(self.db) as connection:
            initialize_database(connection)
        with connect(self.db) as connection:
            schema_v20.migrate(connection)
        self.at = "2026-09-07T03:00:00Z"
        root = self.db.parent
        lock = root / "writer.lock"
        lock.touch(mode=0o600)
        installed = InstalledWriterContract(root, root / "fixture.plist", root,
            root / "fixture.py", self.db, lock, {})
        access = ResolvedDatabaseAccess(DatabaseAccessMode.WRITER, self.db,
            FileIdentity.from_stat(self.db.stat()), root, lock, installed)
        self.enterContext(acquire_writer_lock(access))

    def tearDown(self):
        self.temp.cleanup()

    def _submit(self, content_id: int) -> int:
        spec = {"content_id": content_id, "account_id": 1, "platform": "douyin", "kind": "manual_update", "targets": []}
        identity = {"contract_version": capture_commands.CONTRACT, "specification": spec}
        scan_id = durable_runs.scan_identity(capture_commands.JOB, identity)
        details = {"contract_version": durable_runs.CONTRACT_VERSION, "scan_id": scan_id,
                   "identity": identity, "checkpoint": {"complete": False}, "complete": False}
        with connect(self.db) as connection:
            cursor = connection.execute(
                "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,completed_at,details_json) "
                "VALUES(?,?,'interrupted',?,?,?)",
                (capture_commands.JOB, "scan:" + scan_id, self.at, self.at, json.dumps(details, sort_keys=True)))
            connection.commit()
            return int(cursor.lastrowid)

    def _status(self, run_id: int) -> str:
        with connect(self.db) as connection:
            return connection.execute("SELECT status FROM scheduler_runs WHERE id=?", (run_id,)).fetchone()[0]

    def test_poison_command_is_finished_failed_and_does_not_block_the_next_one(self):
        poison = self._submit(101)
        healthy = self._submit(102)
        calls: list[int] = []

        def enqueue(connection, *, specification, command_run_id, at):
            calls.append(specification["content_id"])
            if specification["content_id"] == 101:
                raise RuntimeError("route resolver exploded")
            return {"status": "blocked", "reason": "account_not_in_active_roster", "work_ids": [], "provider_calls": 0}

        with mock.patch.object(capture_commands.capture_runtime, "enqueue_manual_work", enqueue):
            first = capture_commands.process_commands(db_path=self.db, at=self.at)
            second = capture_commands.process_commands(db_path=self.db, at=self.at)

        self.assertEqual(first["failed_run_ids"], [poison])
        self.assertEqual(first["run_ids"], [healthy])
        self.assertEqual(second, {"count": 0, "run_ids": [], "failed_run_ids": [], "provider_calls": 0})
        # The poison command was attempted exactly once, then retired.
        self.assertEqual(calls, [101, 102])
        self.assertEqual(self._status(poison), "failed")
        self.assertEqual(self._status(healthy), "succeeded")
        with connect(self.db) as connection:
            command = capture_commands.read_command(connection, run_id=poison, content_id=101)
            self.assertEqual(command["status"], "failed")
            self.assertIn("route resolver exploded", command["reason"])
            alert = connection.execute(
                "SELECT severity,owner,status FROM operational_alerts WHERE dedupe_key=?",
                (f"capture-command-failed:{poison}",)).fetchone()
            self.assertEqual(tuple(alert), ("P2", "capture-commands", "open"))
            attempts = connection.execute(
                "SELECT status FROM scheduler_run_attempts WHERE scheduler_run_id=? ORDER BY id", (poison,)).fetchall()
            # The rolled-back attempt leaves no row; only the failed terminal attempt persists.
            self.assertEqual([row[0] for row in attempts], ["failed"])

    def test_unreadable_details_are_retired_instead_of_looping(self):
        run_id = self._submit(103)
        with connect(self.db) as connection:
            connection.execute("UPDATE scheduler_runs SET details_json='{not json' WHERE id=?", (run_id,))
            connection.commit()
        result = capture_commands.process_commands(db_path=self.db, at=self.at)
        self.assertEqual(result["failed_run_ids"], [run_id])
        self.assertEqual(self._status(run_id), "failed")
        self.assertEqual(capture_commands.process_commands(db_path=self.db, at=self.at)["count"], 0)

    def test_valid_json_corrupt_contracts_do_not_reclaim_or_block_next_command(self):
        for index, broken in enumerate(("scan", "identity", "checkpoint", "contract")):
            with self.subTest(broken=broken):
                poison, healthy = self._submit(200 + index), self._submit(300 + index)
                with connect(self.db) as connection:
                    row = connection.execute("SELECT details_json FROM scheduler_runs WHERE id=?", (poison,)).fetchone()
                    details = json.loads(row[0])
                    if broken == "scan":
                        details["scan_id"] = "0" * 64
                    elif broken == "identity":
                        # A different valid identity must not create/claim another row.
                        details["identity"]["specification"]["content_id"] = 9999
                    elif broken == "checkpoint":
                        details["checkpoint"] = []
                    else:
                        details["contract_version"] = "unknown"
                    corrupt = json.dumps(details, sort_keys=True)
                    connection.execute("UPDATE scheduler_runs SET details_json=? WHERE id=?", (corrupt, poison))
                    connection.commit()
                with mock.patch.object(capture_commands.capture_runtime, "enqueue_manual_work", return_value={
                        "status": "queued", "reason": "", "work_ids": [], "provider_calls": 0}) as enqueue:
                    result = capture_commands.process_commands(db_path=self.db, at=self.at)
                self.assertEqual(result["failed_run_ids"], [poison])
                self.assertEqual(result["run_ids"], [healthy])
                self.assertEqual(enqueue.call_count, 1)
                with connect(self.db) as connection:
                    self.assertEqual(connection.execute("SELECT details_json FROM scheduler_runs WHERE id=?", (poison,)).fetchone()[0], corrupt)
                    self.assertEqual(connection.execute("SELECT count(*) FROM scheduler_run_attempts WHERE scheduler_run_id=?", (poison,)).fetchone()[0], 0)
                    self.assertEqual(connection.execute("SELECT count(*) FROM provider_request_start_events").fetchone()[0], 0)
                    self.assertEqual(connection.execute("SELECT count(*) FROM provider_usage").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
