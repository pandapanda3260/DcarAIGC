from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from v8 import api, runtime_thread_diagnostics


class ThreadSnapshotTest(unittest.TestCase):
    def test_real_blocked_thread_has_locations_without_source_or_values(self):
        entered, release = threading.Event(), threading.Event()

        def blocked_worker():
            private_argument_value = "PRIVATE-LOCAL-VALUE-MUST-NOT-APPEAR"
            entered.set()
            release.wait(5)
            return private_argument_value

        worker = threading.Thread(target=blocked_worker, name="diagnostics-event-worker")
        worker.start()
        try:
            self.assertTrue(entered.wait(2))
            with patch("builtins.open", side_effect=AssertionError("source read")), \
                    patch("linecache.getlines", side_effect=AssertionError("source text")), \
                    patch("sys.settrace", side_effect=AssertionError("trace mutation")):
                value = runtime_thread_diagnostics.snapshot()
            row = next(row for row in value["threads"] if row["ident"] == worker.ident)
            self.assertEqual(row["name"], worker.name)
            self.assertEqual(row["native_id"], worker.native_id)
            self.assertTrue(any(frame["function"] == "blocked_worker" for frame in row["frames"]))
            self.assertFalse(row["truncated"])
            self.assertEqual(row["omitted_frame_count"], 0)
            for thread in value["threads"]:
                self.assertLessEqual(len(thread["frames"]), 20)
                for frame in thread["frames"]:
                    self.assertEqual(set(frame), {"module", "function", "line"})
            serialized = json.dumps(value)
            self.assertNotIn("PRIVATE-LOCAL-VALUE-MUST-NOT-APPEAR", serialized)
            self.assertNotIn("private_argument_value", serialized)
            self.assertNotIn(str(Path(__file__).resolve()), serialized)
        finally:
            release.set()
            worker.join(2)
        self.assertFalse(worker.is_alive())

    def test_deep_real_stack_keeps_inner_wait_and_outer_business_entry(self):
        entered, release = threading.Event(), threading.Event()

        def descend(depth):
            if depth:
                return descend(depth - 1)
            entered.set()
            release.wait(5)

        def named_worker_entry():
            descend(40)

        worker = threading.Thread(target=named_worker_entry)
        worker.start()
        try:
            self.assertTrue(entered.wait(2))
            row = next(row for row in runtime_thread_diagnostics.snapshot()["threads"]
                       if row["ident"] == worker.ident)
            self.assertEqual(len(row["frames"]), 20)
            self.assertTrue(row["truncated"])
            self.assertGreater(row["omitted_frame_count"], 20)
            inner, outer = row["frames"][:10], row["frames"][10:]
            self.assertTrue(any(frame["module"] == "threading" and frame["function"] == "wait"
                                for frame in inner))
            self.assertIn("named_worker_entry", [frame["function"] for frame in outer])
            self.assertEqual(outer[-1]["function"], "_bootstrap")
            self.assertTrue(any(frame["module"] == "threading" and frame["function"] == "run"
                                for frame in outer))
        finally:
            release.set()
            worker.join(2)
        self.assertFalse(worker.is_alive())


class SchedulerThreadDiagnosticsTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        self.db = root / "scheduler.sqlite3"
        with sqlite3.connect(self.db) as connection:
            connection.executescript("""
                CREATE TABLE scheduler_runs(id INTEGER PRIMARY KEY, job_id TEXT);
                CREATE TABLE scheduler_run_attempts(id INTEGER PRIMARY KEY,
                    scheduler_run_id INTEGER, attempt_number INTEGER, invocation_source TEXT);
            """)
        self.app = FastAPI()
        self.app.state.config = api.ApiConfig(db_path=self.db, reports_root=root / "reports",
            legacy_db_path=root / "legacy.sqlite3", operator_freeze_lock=root / "freeze",
            scheduler_enabled=False, startup_catchup_enabled=False, project_root=root)
        self.app.add_api_route("/api/v8/scheduler", api.get_v8_scheduler_status, methods=["GET"])

        @contextmanager
        def reader(path, **kwargs):
            self.assertEqual(Path(path), self.db)
            connection = sqlite3.connect(self.db.as_uri() + "?mode=ro", uri=True)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only=ON")
            try:
                yield connection
            finally:
                connection.close()

        self.addCleanup(patch.stopall)
        patch.object(api, "connect", reader).start()
        patch.object(api, "_request_data_freshness", return_value={"status": "fixture"}).start()

    def test_default_and_explicit_false_are_unchanged_and_never_capture(self):
        with TestClient(self.app, client=("127.0.0.1", 1234)) as client, \
                patch.object(runtime_thread_diagnostics, "snapshot", side_effect=AssertionError("default capture")):
            default = client.get("/api/v8/scheduler")
            explicit = client.get("/api/v8/scheduler?include_runtime_threads=false")
        self.assertEqual(default.status_code, 200)
        self.assertEqual(default.json(), explicit.json())
        self.assertNotIn("runtime_threads", default.json())

    def test_explicit_loopback_request_captures_real_blocked_worker(self):
        entered, release = threading.Event(), threading.Event()

        def scheduler_blocked_worker():
            entered.set()
            release.wait(5)

        worker = threading.Thread(target=scheduler_blocked_worker, name="scheduler-diagnostic-worker")
        worker.start()
        try:
            self.assertTrue(entered.wait(2))
            for host in ("127.0.0.1", "::1"):
                with self.subTest(host=host), TestClient(self.app, client=(host, 1234)) as client:
                    response = client.get("/api/v8/scheduler?include_runtime_threads=true")
                self.assertEqual(response.status_code, 200)
                rows = response.json()["runtime_threads"]["threads"]
                row = next(row for row in rows if row["ident"] == worker.ident)
                self.assertTrue(any(frame["function"] == "scheduler_blocked_worker" for frame in row["frames"]))
        finally:
            release.set()
            worker.join(2)
        self.assertFalse(worker.is_alive())

    def test_remote_clients_and_readonly_replica_cannot_capture(self):
        for host, readonly in (("203.0.113.5", False), ("testclient", False), ("127.0.0.1", True)):
            with self.subTest(host=host, readonly=readonly):
                self.app.state.config = replace(self.app.state.config, read_only=readonly)
                with TestClient(self.app, client=(host, 1234)) as client, \
                        patch.object(runtime_thread_diagnostics, "snapshot", side_effect=AssertionError("remote capture")):
                    response = client.get("/api/v8/scheduler?include_runtime_threads=true",
                        headers={"x-forwarded-for": "127.0.0.1", "forwarded": "for=127.0.0.1"})
                self.assertEqual(response.status_code, 200)
                self.assertNotIn("runtime_threads", response.json())


if __name__ == "__main__":
    unittest.main()
