"""Fenced durable jobs using the existing scheduler run/attempt history."""

from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

from .source_routing import parse_time
from .storage import (
    DEFAULT_DB,
    connect,
    now_utc,
    transaction,
    transaction_metrics_context,
)

CONTRACT_VERSION = "durable-run-v1"
INVOCATION_SOURCES = {"scheduled", "startup_report_catchup", "operator_retry"}
LEASE_SECONDS = 180
HEARTBEAT_SECONDS = 30


class DurableRunError(RuntimeError):
    pass


class LostOwnership(DurableRunError):
    pass


@dataclass(frozen=True)
class DurableClaim:
    scheduler_run_id: int
    attempt_id: int
    attempt_number: int
    owner_token: str
    scan_id: str


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def _time(value: str | None) -> str:
    return parse_time(value or now_utc()).isoformat(timespec="seconds").replace("+00:00", "Z")


def root_run_predicate(connection: sqlite3.Connection, alias: str = "") -> str:
    """Preserve legacy exact-slot readers when continuation children exist."""
    if alias and not alias.replace("_", "").isalnum():
        raise ValueError("invalid SQL alias")
    if connection.execute("PRAGMA user_version").fetchone()[0] == 20:
        return f" AND {alias + '.' if alias else ''}root_run_id IS NULL"
    return ""


def _lease_time(value: str) -> str:
    return parse_time(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def heartbeat(connection: sqlite3.Connection, claim: DurableClaim, *, now: str | None = None) -> None:
    if connection.execute("PRAGMA user_version").fetchone()[0] != 20:
        return
    timestamp = _lease_time(now or now_utc())
    expires = _lease_time((parse_time(timestamp) + timedelta(seconds=LEASE_SECONDS)).isoformat())
    updated = connection.execute(
        "UPDATE scheduler_run_attempts SET heartbeat_at=?,lease_expires_at=? WHERE id=? "
        "AND scheduler_run_id=? AND status='running' AND owner_token=? AND lease_expires_at>=?",
        (timestamp, expires, claim.attempt_id, claim.scheduler_run_id, claim.owner_token, timestamp),
    )
    if updated.rowcount != 1:
        raise LostOwnership("durable lease expired or changed owner")


def scan_identity(job_id: str, identity: Mapping[str, Any]) -> str:
    if not job_id or not isinstance(identity, Mapping) or not identity:
        raise DurableRunError("durable job and frozen identity are required")
    return hashlib.sha256(_json({"job_id": job_id, "identity": dict(identity)}).encode()).hexdigest()


def get_run(run_id: int, *, db_path: Path = DEFAULT_DB) -> dict[str, Any]:
    with connect(db_path) as connection:
        row = connection.execute("SELECT * FROM scheduler_runs WHERE id=?", (run_id,)).fetchone()
    if row is None:
        raise DurableRunError("durable run does not exist")
    result = dict(row)
    result["details"] = json.loads(result["details_json"])
    return result


def claim_run(
    job_id: str,
    identity: Mapping[str, Any],
    *,
    db_path: Path = DEFAULT_DB,
    initial_checkpoint: Mapping[str, Any] | None = None,
    invocation_source: str = "scheduled",
    now: str | None = None,
    scope_key: Mapping[str, Any] | None = None,
) -> DurableClaim | None:
    """New jobs and due incomplete partials resume without changing their scope."""
    with transaction_metrics_context(job_id=job_id), connect(
        db_path
    ) as connection, transaction(connection):
        return claim_run_in_transaction(
            connection,
            job_id,
            identity,
            initial_checkpoint=initial_checkpoint,
            invocation_source=invocation_source,
            now=now,
            scope_key=scope_key,
        )


def claim_run_in_transaction(
    connection: sqlite3.Connection,
    job_id: str,
    identity: Mapping[str, Any],
    *,
    initial_checkpoint: Mapping[str, Any] | None = None,
    invocation_source: str = "scheduled",
    now: str | None = None,
    scope_key: Mapping[str, Any] | None = None,
) -> DurableClaim | None:
    """Claim through a caller-owned transaction for compound dispatch fences."""

    if not connection.in_transaction:
        raise DurableRunError("durable claim requires a caller transaction")
    if invocation_source not in INVOCATION_SOURCES:
        raise DurableRunError("unsupported durable invocation source")
    frozen = json.loads(_json(dict(identity)))
    scan_id = scan_identity(job_id, frozen)
    stable_key = json.loads(_json(dict(scope_key))) if scope_key is not None else None
    scheduled_for = "scan:" + (scan_identity(job_id, stable_key) if stable_key is not None else scan_id)
    timestamp = _time(now)
    row = connection.execute(
        "SELECT * FROM scheduler_runs WHERE job_id=? AND scheduled_for=?" + root_run_predicate(connection),
        (job_id, scheduled_for),
    ).fetchone()
    if row is None:
        details = {
            "contract_version": CONTRACT_VERSION, "scan_id": scan_id,
            "identity": frozen,
            "checkpoint": {"complete": False, **dict(initial_checkpoint or {})},
            "complete": False,
        }
        if stable_key is not None:
            details["scope_key"] = stable_key
        if details["checkpoint"]["complete"] is not False:
            raise DurableRunError("new durable jobs cannot start completed")
        cursor = connection.execute(
            "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,details_json) "
            "VALUES (?,?,'running',?,?)",
            (job_id, scheduled_for, timestamp, _json(details)),
        )
        run_id = int(cursor.lastrowid or 0)
    else:
        run_id = int(row["id"])
        details = json.loads(row["details_json"])
        if stable_key is not None:
            if details.get("scope_key") != stable_key:
                raise DurableRunError("durable scope key mismatch")
            # Under the writer lock, the first claimant freezes mutable
            # inputs (notably the roster) for this calendar round.
            frozen = details.get("identity", {})
            scan_id = scan_identity(job_id, frozen)
        if (details.get("contract_version") != CONTRACT_VERSION
                or details.get("identity") != frozen or details.get("scan_id") != scan_id):
            raise DurableRunError("durable frozen contract mismatch")
        if row["status"] == "running":
            return None
        if row["status"] == "partial":
            due = details.get("next_resume_at")
            if details.get("complete") or not due or parse_time(timestamp) < parse_time(due):
                return None
        elif row["status"] != "interrupted":
            return None
    attempt_number = int(connection.execute(
        "SELECT COALESCE(MAX(attempt_number),0)+1 FROM scheduler_run_attempts WHERE scheduler_run_id=?",
        (run_id,),
    ).fetchone()[0])
    token = uuid.uuid4().hex
    details["owner"] = {"token": token, "attempt_number": attempt_number}
    details["claimed_at"] = timestamp
    if connection.execute("PRAGMA user_version").fetchone()[0] == 20:
        lease_start = _lease_time(timestamp)
        lease_end = _lease_time((parse_time(timestamp) + timedelta(seconds=LEASE_SECONDS)).isoformat())
        attempt = connection.execute(
            "INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,invocation_source,"
            "status,started_at,details_json,owner_token,heartbeat_at,lease_expires_at) "
            "VALUES (?,?,?,'running',?,?,?,?,?)",
            (run_id, attempt_number, invocation_source, timestamp, _json(details), token, lease_start, lease_end),
        )
    else:
        attempt = connection.execute(
            "INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,invocation_source,"
            "status,started_at,details_json) VALUES (?,?,?,'running',?,?)",
            (run_id, attempt_number, invocation_source, timestamp, _json(details)),
        )
    attempt_id = int(attempt.lastrowid or 0)
    details["owner"]["attempt_id"] = attempt_id
    connection.execute(
        "UPDATE scheduler_runs SET status='running',started_at=?,completed_at=NULL,details_json=? WHERE id=?",
        (timestamp, _json(details), run_id),
    )
    return DurableClaim(run_id, attempt_id, attempt_number, token, scan_id)


def assert_owner(connection: sqlite3.Connection, claim: DurableClaim) -> dict[str, Any]:
    if not connection.in_transaction:
        raise DurableRunError("ownership and checkpoint require a caller transaction")
    row = connection.execute(
        "SELECT r.status,r.details_json,a.id attempt_id,a.status attempt_status "
        "FROM scheduler_runs r LEFT JOIN scheduler_run_attempts a "
        "ON a.scheduler_run_id=r.id AND a.status='running' WHERE r.id=?",
        (claim.scheduler_run_id,),
    ).fetchone()
    if row is None or row["status"] != "running" or row["attempt_id"] != claim.attempt_id:
        raise LostOwnership("durable active attempt changed")
    details = json.loads(row["details_json"])
    owner = details.get("owner", {})
    if (details.get("contract_version") != CONTRACT_VERSION
            or details.get("scan_id") != claim.scan_id
            or owner.get("token") != claim.owner_token
            or owner.get("attempt_id") != claim.attempt_id):
        raise LostOwnership("durable owner fence changed")
    return details


def checkpoint(
    connection: sqlite3.Connection,
    claim: DurableClaim,
    changes: Mapping[str, Any],
    *,
    now: str | None = None,
) -> dict[str, Any]:
    """Update the checkpoint and renew only schema20's fenced lease fields."""
    details = assert_owner(connection, claim)
    heartbeat(connection, claim, now=now)
    state = details["checkpoint"]
    if state.get("complete") and changes:
        raise DurableRunError("completed checkpoint cannot change")
    state.update(copy.deepcopy(dict(changes)))
    if type(state.get("complete")) is not bool:
        raise DurableRunError("checkpoint complete must be boolean")
    details["complete"] = state["complete"]
    details["checkpoint_at"] = _time(now)
    connection.execute(
        "UPDATE scheduler_runs SET details_json=? WHERE id=?",
        (_json(details), claim.scheduler_run_id),
    )
    return details


def recover_expired_leases(connection: sqlite3.Connection, *, now: str | None = None,
                           owned_run_ids: Sequence[int] | None = None) -> int:
    """Reconcile fenced running attempts without reissuing any paid identity."""
    if not connection.in_transaction:
        raise DurableRunError("lease recovery requires a caller transaction")
    if connection.execute("PRAGMA user_version").fetchone()[0] != 20:
        return 0
    owned_clause = ""
    owned_ids: tuple[int, ...] = ()
    if owned_run_ids is not None:
        if any(type(value) is not int or value <= 0 for value in owned_run_ids):
            raise DurableRunError("lease recovery requires positive owned run ids")
        owned_ids = tuple(sorted(set(owned_run_ids)))
        if not owned_ids:
            return 0
        owned_clause = f" AND r.id IN ({','.join('?' for _ in owned_ids)})"
    stamp = _lease_time(now or now_utc())
    rows = connection.execute(
        "SELECT a.id,a.scheduler_run_id,r.details_json FROM scheduler_run_attempts a "
        "JOIN scheduler_runs r ON r.id=a.scheduler_run_id WHERE a.status='running' "
        "AND r.status='running' AND a.lease_expires_at IS NOT NULL AND a.lease_expires_at<?" + owned_clause,
        (stamp, *owned_ids),
    ).fetchall()
    for row in rows:
        details = json.loads(row["details_json"])
        details["recovery"] = {"reason": "lease_expired", "recovered_at": stamp}
        encoded = _json(details)
        connection.execute(
            "UPDATE scheduler_run_attempts SET status='interrupted',completed_at=?,details_json=? "
            "WHERE id=? AND status='running'", (stamp, encoded, row["id"]),
        )
        connection.execute(
            "UPDATE scheduler_runs SET status='interrupted',completed_at=?,details_json=? "
            "WHERE id=? AND status='running'", (stamp, encoded, row["scheduler_run_id"]),
        )
    return len(rows)


def claim_child(connection: sqlite3.Connection, *, root_run_id: int, continuation_sequence: int,
                charge_business_day: str, now: str | None = None) -> DurableClaim | None:
    """Claim a charge-day continuation. Frozen data scope and paid keys survive."""
    if not connection.in_transaction or connection.execute("PRAGMA user_version").fetchone()[0] != 20:
        raise DurableRunError("child claim requires schema20 caller transaction")
    from datetime import date

    if continuation_sequence < 1 or date.fromisoformat(charge_business_day).isoformat() != charge_business_day:
        raise DurableRunError("invalid continuation identity")
    row = connection.execute("SELECT * FROM scheduler_runs WHERE id=? AND root_run_id IS NULL", (root_run_id,)).fetchone()
    if row is None:
        raise DurableRunError("child must reference root")
    original = json.loads(row["details_json"])
    if original.get("contract_version") != CONTRACT_VERSION:
        raise DurableRunError("child requires durable root contract")
    if row["status"] == "running":
        raise DurableRunError("running root must yield before continuation")
    stamp = _time(now)
    identity = {**original["identity"], "continuation": {"root_run_id": root_run_id,
                "sequence": continuation_sequence, "charge_business_day": charge_business_day}}
    if "business_day" in identity:
        identity["data_business_day"] = identity.get("data_business_day", identity["business_day"])
        identity["business_day"] = charge_business_day
    scan_id = scan_identity(str(row["job_id"]), identity)
    child = connection.execute(
        "SELECT * FROM scheduler_runs WHERE root_run_id=? AND continuation_sequence=? AND charge_business_day=?",
        (root_run_id, continuation_sequence, charge_business_day),
    ).fetchone()
    if child is None:
        previous = connection.execute(
            "SELECT * FROM scheduler_runs WHERE root_run_id=? ORDER BY continuation_sequence DESC LIMIT 1",
            (root_run_id,),
        ).fetchone()
        expected = int(previous["continuation_sequence"]) + 1 if previous else 1
        if continuation_sequence != expected:
            raise DurableRunError("continuation sequence must be consecutive")
        if previous and (previous["status"] == "running" or previous["charge_business_day"] > charge_business_day):
            raise DurableRunError("previous continuation must yield without reversing charge day")
        checkpoint_source = json.loads(previous["details_json"]) if previous else original
        details = {"contract_version": CONTRACT_VERSION, "scan_id": scan_id, "identity": identity,
                   "checkpoint": copy.deepcopy(checkpoint_source["checkpoint"]),
                   "complete": checkpoint_source.get("complete", False)}
        if details["complete"]:
            return None
        inserted = connection.execute(
            "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,details_json,root_run_id,"
            "continuation_sequence,charge_business_day) VALUES (?,?,'running',?,?,?,?,?)",
            (row["job_id"], row["scheduled_for"], stamp, _json(details), root_run_id,
             continuation_sequence, charge_business_day),
        )
        run_id = int(inserted.lastrowid or 0)
    else:
        details = json.loads(child["details_json"])
        if details.get("identity") != identity or details.get("scan_id") != scan_id:
            raise DurableRunError("continuation frozen identity conflict")
        if child["status"] not in {"partial", "interrupted"} or details.get("complete"):
            return None
        if child["status"] == "partial" and parse_time(str(details.get("next_resume_at"))) > parse_time(stamp):
            return None
        run_id = int(child["id"])
    attempt_number = int(connection.execute(
        "SELECT COALESCE(MAX(attempt_number),0)+1 FROM scheduler_run_attempts WHERE scheduler_run_id=?", (run_id,),
    ).fetchone()[0])
    token = uuid.uuid4().hex
    details["owner"] = {"token": token, "attempt_number": attempt_number}
    details["claimed_at"] = stamp
    started = _lease_time(stamp)
    expires = _lease_time((parse_time(stamp) + timedelta(seconds=LEASE_SECONDS)).isoformat())
    attempt = connection.execute(
        "INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,invocation_source,status,"
        "started_at,details_json,owner_token,heartbeat_at,lease_expires_at) "
        "VALUES (?,?,'scheduled','running',?,?,?,?,?)",
        (run_id, attempt_number, stamp, _json(details), token, started, expires),
    )
    attempt_id = int(attempt.lastrowid or 0)
    details["owner"]["attempt_id"] = attempt_id
    connection.execute("UPDATE scheduler_runs SET status='running',started_at=?,completed_at=NULL,details_json=? WHERE id=?",
                       (stamp, _json(details), run_id))
    return DurableClaim(run_id, attempt_id, attempt_number, token, scan_id)


def finish_run(
    claim: DurableClaim,
    *,
    status: str,
    db_path: Path = DEFAULT_DB,
    summary: Mapping[str, Any] | None = None,
    next_resume_at: str | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    with transaction_metrics_context(
        job_id="durable_run_finish",
        scheduler_run_id=claim.scheduler_run_id,
        attempt_id=claim.attempt_id,
    ), connect(db_path) as connection, transaction(connection):
        return finish_run_in_transaction(
            connection,
            claim,
            status=status,
            summary=summary,
            next_resume_at=next_resume_at,
            now=now,
        )


def finish_run_in_transaction(
    connection: sqlite3.Connection,
    claim: DurableClaim,
    *,
    status: str,
    summary: Mapping[str, Any] | None = None,
    next_resume_at: str | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    """Finalize a durable run inside the caller's existing transaction."""

    if not connection.in_transaction:
        raise DurableRunError("durable finish requires a caller transaction")
    if status not in {"succeeded", "partial", "failed", "interrupted"}:
        raise DurableRunError("unsupported durable terminal status")
    timestamp = _time(now)
    details = assert_owner(connection, claim)
    complete = details["checkpoint"].get("complete") is True
    if (status == "succeeded") != complete:
        raise DurableRunError("durable completion and terminal status disagree")
    if status == "partial":
        if next_resume_at is None or parse_time(next_resume_at) < parse_time(timestamp):
            raise DurableRunError("partial durable runs require a future resume time")
        details["next_resume_at"] = _time(next_resume_at)
    else:
        details.pop("next_resume_at", None)
    details["summary"] = {**details.get("summary", {}), **dict(summary or {})}
    details["complete"] = complete
    details["completed_at"] = timestamp
    encoded = _json(details)
    attempt = connection.execute(
        "UPDATE scheduler_run_attempts SET status=?,completed_at=?,details_json=? "
        "WHERE id=? AND scheduler_run_id=? AND status='running'",
        (status, timestamp, encoded, claim.attempt_id, claim.scheduler_run_id),
    )
    run = connection.execute(
        "UPDATE scheduler_runs SET status=?,completed_at=?,details_json=? "
        "WHERE id=? AND status='running'",
        (status, timestamp, encoded, claim.scheduler_run_id),
    )
    if attempt.rowcount != 1 or run.rowcount != 1:
        raise LostOwnership("durable terminal owner changed")
    return details


def recover_run(
    run_id: int,
    *,
    expected_attempt_id: int,
    db_path: Path = DEFAULT_DB,
    reason: str = "writer_process_restarted",
    now: str | None = None,
) -> bool:
    """An explicit recovery fence copies the RUN's latest state, not its seed."""
    timestamp = _time(now)
    with transaction_metrics_context(
        job_id="durable_run_recovery",
        scheduler_run_id=run_id,
        attempt_id=expected_attempt_id,
    ), connect(db_path) as connection, transaction(connection):
        row = connection.execute(
            "SELECT r.status,r.details_json,a.id attempt_id FROM scheduler_runs r "
            "JOIN scheduler_run_attempts a ON a.scheduler_run_id=r.id AND a.status='running' "
            "WHERE r.id=?", (run_id,),
        ).fetchone()
        if row is None or row["status"] != "running" or row["attempt_id"] != expected_attempt_id:
            return False
        details = json.loads(row["details_json"])
        if details.get("contract_version") != CONTRACT_VERSION:
            raise DurableRunError("legacy runs must retain their own recovery semantics")
        details["recovery"] = {"reason": reason, "recovered_at": timestamp}
        encoded = _json(details)
        connection.execute(
            "UPDATE scheduler_run_attempts SET status='interrupted',completed_at=?,details_json=? "
            "WHERE id=? AND status='running'", (timestamp, encoded, expected_attempt_id),
        )
        connection.execute(
            "UPDATE scheduler_runs SET status='interrupted',completed_at=?,details_json=? WHERE id=?",
            (timestamp, encoded, run_id),
        )
        return True
