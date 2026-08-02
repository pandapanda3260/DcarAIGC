from __future__ import annotations

import tempfile
import threading
import time
import unittest
from pathlib import Path

from workflow.storage import connect, migrate, now_iso
from workflow.tasks import (
    CURRENT_REPORT_VERSION,
    CURRENT_RULE_VERSION,
    ProviderHTTPError,
    RunContext,
    TaskManager,
    promote_formal_baseline,
    recover_interrupted_runs,
)


def add_run(db: Path, run_id: str, **changes) -> None:
    values = {
        "status": "queued",
        "run_kind": "temporary",
        "scope": "single_channel",
        "rule_version": CURRENT_RULE_VERSION,
        "report_version": CURRENT_REPORT_VERSION,
        "report_revision": 0,
        "report_stale": 0,
        "output_path": None,
    }
    values.update(changes)
    timestamp = now_iso()
    with connect(db) as connection:
        connection.execute(
            """
            INSERT INTO runs(
                id, created_at, updated_at, mode, channel, status, progress,
                input_count, message, run_kind, scope, rule_version, report_version,
                report_revision, report_stale, output_path
            ) VALUES (?, ?, ?, 'test', 'dual', ?, 0, 0, '', ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id, timestamp, timestamp, values["status"], values["run_kind"],
                values["scope"], values["rule_version"], values["report_version"],
                values["report_revision"], values["report_stale"], values["output_path"],
            ),
        )
        connection.commit()


class WorkflowTasksTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.db = Path(self.temporary.name) / "tasks.sqlite3"
        with connect(self.db) as connection:
            migrate(connection)

    def tearDown(self):
        self.temporary.cleanup()

    def row(self, run_id: str):
        with connect(self.db) as connection:
            return dict(connection.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone())

    def test_transient_provider_failure_is_attempted_exactly_twice(self):
        add_run(self.db, "retry")
        manager = TaskManager(self.db)
        attempts = []

        def task(context: RunContext):
            def operation():
                attempts.append(1)
                raise ProviderHTTPError(503)
            context.call_with_retry(operation, "test")

        manager.submit("retry", task).result(timeout=5)
        manager.shutdown()
        self.assertEqual(len(attempts), 2)
        self.assertEqual(self.row("retry")["status"], "failed")
        self.assertEqual(self.row("retry")["attempt_count"], 2)

    def test_non_retryable_failure_is_attempted_once(self):
        add_run(self.db, "no-retry")
        manager = TaskManager(self.db)
        attempts = []

        def task(context: RunContext):
            def operation():
                attempts.append(1)
                raise ProviderHTTPError(401)
            context.call_with_retry(operation, "test")

        manager.submit("no-retry", task).result(timeout=5)
        manager.shutdown()
        self.assertEqual(len(attempts), 1)
        self.assertEqual(self.row("no-retry")["status"], "failed")

    def test_running_task_cancels_at_checkpoint(self):
        add_run(self.db, "cancel")
        manager = TaskManager(self.db)
        entered = threading.Event()

        def task(context: RunContext):
            entered.set()
            while True:
                context.checkpoint()
                time.sleep(0.005)

        future = manager.submit("cancel", task)
        self.assertTrue(entered.wait(2))
        self.assertTrue(manager.cancel("cancel"))
        future.result(timeout=5)
        manager.shutdown()
        self.assertEqual(self.row("cancel")["status"], "cancelled")

    def test_executor_runs_only_one_task_at_a_time(self):
        add_run(self.db, "one")
        add_run(self.db, "two")
        manager = TaskManager(self.db)
        lock = threading.Lock()
        active = 0
        maximum = 0

        def task(context: RunContext):
            nonlocal active, maximum
            with lock:
                active += 1
                maximum = max(maximum, active)
            time.sleep(0.03)
            with lock:
                active -= 1

        first = manager.submit("one", task)
        second = manager.submit("two", task)
        first.result(timeout=5)
        second.result(timeout=5)
        manager.shutdown()
        self.assertEqual(maximum, 1)

    def test_restart_marks_active_runs_interrupted(self):
        add_run(self.db, "active", status="running")
        self.assertEqual(recover_interrupted_runs(self.db), 1)
        self.assertEqual(self.row("active")["status"], "interrupted")

    def test_only_complete_fresh_dual_full_run_can_be_formal_baseline(self):
        add_run(self.db, "temporary", status="completed", output_path="report.json", report_revision=1)
        with self.assertRaisesRegex(ValueError, "full_corpus"):
            promote_formal_baseline(self.db, "temporary")
        add_run(
            self.db,
            "formal",
            status="completed",
            run_kind="full_corpus",
            scope="dual_channel",
            output_path="reports/runs/formal/report.json",
            report_revision=1,
        )
        promote_formal_baseline(self.db, "formal")
        self.assertEqual(self.row("formal")["is_formal_baseline"], 1)


if __name__ == "__main__":
    unittest.main()
