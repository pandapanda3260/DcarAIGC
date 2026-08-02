"""Single-concurrency, cooperative, resumable local task execution."""

from __future__ import annotations

import json
import socket
import ssl
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, TypeVar

from .storage import connect, now_iso


T = TypeVar("T")
MAX_TOTAL_ATTEMPTS = 2
CURRENT_RULE_VERSION = "dcar-evaluation-v5.0"
CURRENT_REPORT_VERSION = "channel-structured-conclusions-v7.0"


class RunCancelled(RuntimeError):
    pass


class ProviderHTTPError(RuntimeError):
    def __init__(self, status_code: int, message: str = "provider HTTP error") -> None:
        super().__init__(message)
        self.status_code = status_code


class TruncatedJSONError(ValueError):
    pass


def retryable_error(exc: BaseException) -> bool:
    if isinstance(exc, ProviderHTTPError):
        return exc.status_code in {408, 429} or 500 <= exc.status_code <= 599
    return isinstance(
        exc,
        (
            TimeoutError,
            ConnectionError,
            socket.timeout,
            socket.gaierror,
            ssl.SSLError,
            TruncatedJSONError,
        ),
    )


def append_event(
    db_path: Path,
    run_id: str,
    event_type: str,
    message: str = "",
    payload: dict[str, Any] | None = None,
) -> None:
    with connect(db_path) as connection:
        connection.execute(
            "INSERT INTO run_events(run_id, created_at, event_type, message, payload_json) VALUES (?, ?, ?, ?, ?)",
            (run_id, now_iso(), event_type, message, json.dumps(payload or {}, ensure_ascii=False)),
        )
        connection.commit()


def update_run(db_path: Path, run_id: str, **changes: Any) -> None:
    if not changes:
        return
    changes["updated_at"] = now_iso()
    columns = ", ".join(f"{key} = ?" for key in changes)
    with connect(db_path) as connection:
        connection.execute(
            f"UPDATE runs SET {columns} WHERE id = ?",
            [*changes.values(), run_id],
        )
        connection.commit()


@dataclass
class RunContext:
    db_path: Path
    run_id: str
    cancel_event: threading.Event

    def checkpoint(self) -> None:
        if self.cancel_event.is_set():
            raise RunCancelled("task cancellation requested")
        with connect(self.db_path) as connection:
            row = connection.execute(
                "SELECT cancel_requested FROM runs WHERE id = ?", (self.run_id,)
            ).fetchone()
        if row and int(row["cancel_requested"]):
            self.cancel_event.set()
            raise RunCancelled("task cancellation requested")

    def progress(self, percentage: int, message: str) -> None:
        self.checkpoint()
        update_run(
            self.db_path,
            self.run_id,
            progress=max(0, min(100, int(percentage))),
            message=message[:500],
        )
        append_event(self.db_path, self.run_id, "progress", message, {"progress": percentage})

    def call_with_retry(self, operation: Callable[[], T], operation_name: str) -> T:
        for attempt in range(1, MAX_TOTAL_ATTEMPTS + 1):
            self.checkpoint()
            update_run(self.db_path, self.run_id, attempt_count=attempt)
            try:
                return operation()
            except Exception as exc:
                transient = retryable_error(exc)
                append_event(
                    self.db_path,
                    self.run_id,
                    "attempt_failed",
                    f"{operation_name}: {type(exc).__name__}",
                    {"attempt": attempt, "retryable": transient},
                )
                if not transient or attempt >= MAX_TOTAL_ATTEMPTS:
                    raise
        raise AssertionError("unreachable")


TaskFunction = Callable[[RunContext], None]


class TaskManager:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="dcar-workflow")
        self._lock = threading.Lock()
        self._futures: dict[str, Future[None]] = {}
        self._cancel_events: dict[str, threading.Event] = {}

    def submit(self, run_id: str, function: TaskFunction) -> Future[None]:
        with self._lock:
            existing = self._futures.get(run_id)
            if existing and not existing.done():
                raise RuntimeError("run is already active")
            event = threading.Event()
            self._cancel_events[run_id] = event
            future = self._executor.submit(self._execute, run_id, function, event)
            self._futures[run_id] = future
            return future

    def _execute(self, run_id: str, function: TaskFunction, event: threading.Event) -> None:
        update_run(
            self.db_path,
            run_id,
            status="running",
            cancel_requested=0,
            message="任务开始执行",
        )
        append_event(self.db_path, run_id, "started", "任务开始执行")
        context = RunContext(self.db_path, run_id, event)
        try:
            context.checkpoint()
            function(context)
            context.checkpoint()
            update_run(
                self.db_path,
                run_id,
                status="completed",
                progress=100,
                message="任务完成",
            )
            append_event(self.db_path, run_id, "completed", "任务完成")
        except RunCancelled as exc:
            update_run(
                self.db_path,
                run_id,
                status="cancelled",
                cancel_requested=1,
                message=str(exc),
            )
            append_event(self.db_path, run_id, "cancelled", str(exc))
        except Exception as exc:
            update_run(
                self.db_path,
                run_id,
                status="failed",
                message=f"{type(exc).__name__}: {exc}"[:500],
                last_error_code=type(exc).__name__,
            )
            append_event(self.db_path, run_id, "failed", f"{type(exc).__name__}: {exc}"[:500])

    def cancel(self, run_id: str) -> bool:
        with self._lock:
            event = self._cancel_events.get(run_id)
            future = self._futures.get(run_id)
            if event is None or future is None or future.done():
                return False
            event.set()
            was_queued = future.cancel()
        if was_queued:
            update_run(
                self.db_path,
                run_id,
                status="cancelled",
                cancel_requested=1,
                message="排队任务已取消",
            )
            append_event(self.db_path, run_id, "cancelled", "排队任务已取消")
        else:
            update_run(
                self.db_path,
                run_id,
                status="cancelling",
                cancel_requested=1,
                message="正在等待当前安全检查点取消",
            )
            append_event(self.db_path, run_id, "cancel_requested", "已请求取消")
        return True

    def shutdown(self, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait, cancel_futures=True)


def recover_interrupted_runs(db_path: Path) -> int:
    with connect(db_path) as connection:
        cursor = connection.execute(
            """
            UPDATE runs
            SET status='interrupted', message='进程重启导致中断，可从任务类型重新恢复', updated_at=?
            WHERE status IN ('queued', 'running', 'cancelling')
            """,
            (now_iso(),),
        )
        connection.commit()
        return int(cursor.rowcount)


def promote_formal_baseline(db_path: Path, run_id: str) -> None:
    with connect(db_path) as connection:
        row = connection.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        if row is None:
            raise ValueError("run not found")
        reasons: list[str] = []
        if row["status"] != "completed":
            reasons.append("run is not completed")
        if row["run_kind"] != "full_corpus":
            reasons.append("run kind is not full_corpus")
        if row["scope"] != "dual_channel":
            reasons.append("scope is not dual_channel")
        if row["rule_version"] != CURRENT_RULE_VERSION:
            reasons.append("rule version is not current")
        if row["report_version"] != CURRENT_REPORT_VERSION:
            reasons.append("report version is not current")
        if int(row["report_stale"]):
            reasons.append("report is stale")
        if int(row["report_revision"]) < 1 or not row["output_path"]:
            reasons.append("fresh report revision is missing")
        if reasons:
            raise ValueError("; ".join(reasons))
        selected_at = now_iso()
        connection.execute("UPDATE runs SET is_formal_baseline=0")
        connection.execute(
            "UPDATE runs SET is_formal_baseline=1, updated_at=? WHERE id=?",
            (selected_at, run_id),
        )
        connection.execute(
            "INSERT OR REPLACE INTO formal_baseline(singleton_id, run_id, selected_at) VALUES (1, ?, ?)",
            (run_id, selected_at),
        )
        connection.commit()

