"""Schema20 manual capture commands backed by existing durable scheduler rows.

POST commits the command only. The writer's bounded worker plans shared capture
work, and GET merely projects those work rows; none of these helpers calls HTTP.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from . import capture_runtime, durable_runs
from .runtime_database import require_current_process_writer_lock
from .storage import connect, now_utc, transaction

JOB = "capture_manual_command"
CONTRACT = "capture-manual-command-v1"


def read_command(connection: sqlite3.Connection, *, run_id: int, content_id: int) -> dict[str, Any]:
    """Pure SELECT; caller may and API does use a query_only connection."""
    row = connection.execute("SELECT * FROM scheduler_runs WHERE id=? AND job_id=? AND root_run_id IS NULL",
                             (run_id, JOB)).fetchone()
    if row is None:
        raise LookupError("capture_command_not_found")
    details = json.loads(row["details_json"])
    specification = details["identity"]["specification"]
    if specification["content_id"] != content_id:
        raise LookupError("capture_command_not_found")
    result = details.get("checkpoint", {}).get("result")
    work: list[dict[str, Any]] = []
    status = "failed" if row["status"] == "failed" else "pending"
    reason = "capture_command_failed" if status == "failed" else ""
    if result is not None:
        reason = result.get("reason", "")
        work = [dict(item) for item in connection.execute(
            "SELECT id,state,reason,completed_at FROM capture_work_items WHERE id IN (" +
            ",".join("?" for _ in result["work_ids"]) + ") ORDER BY id", result["work_ids"]).fetchall()] if result["work_ids"] else []
        if result["status"] == "blocked":
            status = "blocked"
        elif result["status"] == "failed":
            # The writer could not link any capture work for this command.
            status = "failed"
        elif len(work) != len(result["work_ids"]):
            status, reason = "failed", "linked_capture_work_missing"
        elif work and all(item["state"] == "terminal" for item in work):
            status = "partial" if reason else "succeeded"
        elif any(item["state"] == "paid_identity_hold" for item in work):
            status = "blocked"
        elif any(item["state"] in {"running", "leased"} for item in work):
            status = "running"
    return {"run_id": run_id, "content_id": content_id, "kind": specification["kind"],
        "status": status, "reason": reason, "work": work, "provider_calls": 0}


def submit_command(*, db_path: Path, content_id: int, kind: str = "manual_update",
                   at: str | None = None) -> dict[str, Any]:
    """Persist once by content, command kind and existing logical due buckets."""
    at = at or now_utc()
    with connect(db_path) as connection, transaction(connection):
        require_current_process_writer_lock(connection)
        spec = capture_runtime.manual_work_spec(connection, content_id=content_id, kind=kind, at=at)
        identity = {"contract_version": CONTRACT, "specification": spec}
        scan_id = durable_runs.scan_identity(JOB, identity)
        scheduled = "scan:" + scan_id
        existing = connection.execute("SELECT id FROM scheduler_runs WHERE job_id=? AND scheduled_for=? AND root_run_id IS NULL",
                                      (JOB, scheduled)).fetchone()
        if existing is None:
            details = {"contract_version": durable_runs.CONTRACT_VERSION, "scan_id": scan_id,
                       "identity": identity, "checkpoint": {"complete": False}, "complete": False}
            run_id = int(connection.execute("""INSERT INTO scheduler_runs(job_id,scheduled_for,status,
                started_at,completed_at,details_json) VALUES(?,?,'interrupted',?,?,?)""",
                (JOB, scheduled, at, at, json.dumps(details, sort_keys=True))).lastrowid or 0)
        else:
            run_id = int(existing[0])
        result = read_command(connection, run_id=run_id, content_id=content_id)
    # The return is deliberately outside the context: commit failures are 503,
    # never a successful-looking run_id that did not reach durable storage.
    return result


def process_commands(*, db_path: Path, at: str | None = None, limit: int = 10) -> dict[str, Any]:
    """10-second writer hook. One short transaction per command, no network.

    A command whose enqueue raises is finished as ``failed`` in its own
    transaction (the failed enqueue itself is rolled back) and surfaced through
    ``operational_alerts``.  Otherwise the same ``interrupted`` row would be
    selected again every tick, re-raise before ``run_ready`` and stall the
    whole worker batch behind one poison command.
    """
    if not 1 <= limit <= 10:
        raise ValueError("manual command limit must be 1..10")
    at = at or now_utc()
    processed: list[int] = []
    failed: list[int] = []
    for _ in range(limit):
        row_id: int | None = None
        identity: Any = None
        try:
            with connect(db_path) as connection, transaction(connection):
                require_current_process_writer_lock(connection)
                row = connection.execute("SELECT * FROM scheduler_runs WHERE job_id=? AND status='interrupted' AND root_run_id IS NULL ORDER BY id LIMIT 1", (JOB,)).fetchone()
                if row is None:
                    break
                row_id = int(row["id"])
                details = json.loads(row["details_json"])
                identity = details["identity"]
                _validate_command_identity(row, identity)
                claim = durable_runs.claim_run_in_transaction(connection, JOB, identity,
                    invocation_source="operator_retry", now=at)
                if claim is None:
                    break
                result = capture_runtime.enqueue_manual_work(connection,
                    specification=identity["specification"], command_run_id=claim.scheduler_run_id, at=at)
                durable_runs.checkpoint(connection, claim, {"complete": True, "result": result}, now=at)
                durable_runs.finish_run_in_transaction(connection, claim, status="succeeded", now=at,
                    summary={"provider_calls": 0, "enqueued_only": True})
                processed.append(claim.scheduler_run_id)
        except Exception as error:
            if row_id is None:
                raise
            _finish_failed_command(db_path=db_path, run_id=row_id, identity=identity, error=error, at=at)
            failed.append(row_id)
    return {"count": len(processed), "run_ids": processed, "failed_run_ids": failed, "provider_calls": 0}


def _finish_failed_command(*, db_path: Path, run_id: int, identity: Any, error: BaseException, at: str) -> None:
    """Move one poison command to a ``failed`` terminal and open an alert."""
    reason = f"{type(error).__name__}:{str(error)[:200]}"
    result = {"status": "failed", "reason": reason, "work_ids": [], "provider_calls": 0}
    with connect(db_path) as connection, transaction(connection):
        require_current_process_writer_lock(connection)
        row = connection.execute("SELECT * FROM scheduler_runs WHERE id=? AND job_id=? "
                                 "AND root_run_id IS NULL AND status='interrupted'", (run_id, JOB)).fetchone()
        if row is None:
            return
        # Validation errors are not retryable claims. In particular, never claim
        # an identity that points to a different command row while retiring this one.
        try:
            _validate_command_identity(row, identity)
        except (ValueError, TypeError, KeyError, durable_runs.DurableRunError):
            claim = None
        else:
            claim = durable_runs.claim_run_in_transaction(connection, JOB, identity,
                invocation_source="operator_retry", now=at)
        if claim is not None:
            durable_runs.checkpoint(connection, claim, {"complete": False, "result": result}, now=at)
            durable_runs.finish_run_in_transaction(connection, claim, status="failed", now=at,
                summary={"provider_calls": 0, "error": reason})
        else:
            # Preserve the corrupt evidence; retire only the selected interrupted row.
            connection.execute("UPDATE scheduler_runs SET status='failed',completed_at=? WHERE id=? AND status='interrupted'",
                               (at, run_id))
        connection.execute(
            "INSERT OR IGNORE INTO operational_alerts(dedupe_key,severity,scope_json,evidence_json,owner,status,opened_at) "
            "VALUES(?,'P2',?,?,'capture-commands','open',?)",
            (f"capture-command-failed:{run_id}", json.dumps({"run_id": run_id, "job_id": JOB}, sort_keys=True),
             json.dumps({"contract_version": CONTRACT, "reason": reason}, sort_keys=True), at))


def _validate_command_identity(row: sqlite3.Row, identity: Any) -> None:
    """Bind the stored command row before asking durable_runs to claim it."""
    details = json.loads(row["details_json"])
    if not isinstance(identity, dict) or not isinstance(details, dict):
        raise ValueError("invalid capture command identity")
    scan_id = durable_runs.scan_identity(JOB, identity)
    if (details.get("contract_version") != durable_runs.CONTRACT_VERSION
            or details.get("identity") != identity or details.get("scan_id") != scan_id
            or row["scheduled_for"] != "scan:" + scan_id
            or not isinstance(details.get("checkpoint"), dict)
            or details.get("complete") is not False
            or details["checkpoint"].get("complete") is not False
            or identity.get("contract_version") != CONTRACT
            or not isinstance(identity.get("specification"), dict)):
        raise ValueError("capture command frozen contract mismatch")
