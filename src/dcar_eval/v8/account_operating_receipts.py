"""Immutable receipts for manual account labels in the existing database schema.

These are completed operator commands, not scheduled collection occurrences.
The job ID is deliberately not registered with the scheduler. The immutable
attempt is the durable copy; a mutable parent alone is never trusted.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Mapping, Sequence
from itertools import groupby
from typing import Any
from urllib.parse import urlsplit


ACCOUNT_STATUS_JOB = "operator_account_status"
ACCOUNT_STATUS_JOB_ID = ACCOUNT_STATUS_JOB
ACCOUNT_STATUS_RECEIPT_VERSION = "account-operating-status-v1"
_FREQUENCIES = {"daily", "weekly"}
_STATUSES = _FREQUENCIES | {"paused"}


class AccountOperatingStatusError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _canonical(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _invalid() -> AccountOperatingStatusError:
    return AccountOperatingStatusError(
        "account_status_receipt_invalid", "账号状态操作回执损坏，不能读取或覆盖人工标记"
    )


def validate_status_request_id(value: object) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip() or len(value) > 128:
        raise AccountOperatingStatusError(
            "account_status_request_invalid", "账号状态请求编号必须是非空且不超过 128 字符的字符串"
        )
    return value


_RECEIPT_SELECT = """
SELECT r.*, a.id receipt_attempt_id, a.attempt_number receipt_attempt_number,
       a.invocation_source receipt_invocation_source, a.status receipt_attempt_status,
       a.started_at receipt_attempt_started_at, a.completed_at receipt_attempt_completed_at,
       a.details_json receipt_attempt_details_json
FROM scheduler_runs r
LEFT JOIN scheduler_run_attempts a ON a.scheduler_run_id=r.id
WHERE r.job_id=?
"""


def _read_receipt(rows: Sequence[sqlite3.Row]) -> dict[str, Any]:
    if len(rows) != 1:
        raise _invalid()
    row = rows[0]
    try:
        details = json.loads(row["details_json"])
        if not isinstance(details, dict):
            raise _invalid()
        frozen = dict(details)
        receipt_hash = frozen.pop("self_sha256", None)
        payload = details.get("payload")
        if not isinstance(payload, dict):
            raise _invalid()
        before, after, result = payload.get("before"), payload.get("after"), payload.get("result")
        request = payload.get("request")
        if not all(isinstance(item, dict) for item in (before, after, result, request)):
            raise _invalid()
        # The container checks above deliberately precede all indexed accesses.
        assert isinstance(before, dict) and isinstance(after, dict)
        assert isinstance(result, dict) and isinstance(request, dict)
        frequency = payload.get("update_frequency")
        requested_status = payload.get("requested_status")
        if (
            row["job_id"] != ACCOUNT_STATUS_JOB_ID
            or row["status"] != "succeeded"
            or row["started_at"] != row["completed_at"]
            or row["receipt_attempt_id"] is None
            or row["receipt_attempt_number"] != 1
            or row["receipt_invocation_source"] != "operator_retry"
            or row["receipt_attempt_status"] != "succeeded"
            or row["receipt_attempt_started_at"] != row["started_at"]
            or row["receipt_attempt_completed_at"] != row["completed_at"]
            or row["receipt_attempt_details_json"] != row["details_json"]
            or _canonical(details) != row["details_json"]
            or details.get("contract_version") != ACCOUNT_STATUS_RECEIPT_VERSION
            or details.get("job_id") != ACCOUNT_STATUS_JOB_ID
            or type(details.get("run_id")) is not int
            or details["run_id"] != row["id"]
            or type(details.get("attempt_id")) is not int
            or details["attempt_id"] != row["receipt_attempt_id"]
            or details.get("request_id") != row["scheduled_for"]
            or details.get("recorded_at") != row["started_at"]
            or receipt_hash != _digest(frozen)
            or type(payload.get("account_id")) is not int
            or payload["account_id"] < 1
            or type(payload.get("account_identity_id")) is not int
            or payload["account_identity_id"] < 1
            or requested_status not in _STATUSES
            or frequency not in (*_FREQUENCIES, None)
            or (requested_status in _FREQUENCIES and frequency != requested_status)
            or type(before.get("enabled")) is not bool
            or before.get("update_frequency") not in (*_FREQUENCIES, None)
            or type(after.get("enabled")) is not bool
            or after["enabled"] != (requested_status != "paused")
            or after.get("update_frequency") != frequency
            or (requested_status == "paused" and before.get("update_frequency") != frequency)
            or request.get("account_status") != requested_status
            or not isinstance(request.get("fields"), dict)
            or result.get("id") != payload["account_id"]
            or result.get("account_status") != requested_status
            or result.get("update_frequency") != frequency
            or type(result.get("enabled")) is not bool
            or result["enabled"] != after["enabled"]
            or result.get("status_request_id") != details["request_id"]
            or not isinstance(payload.get("actor"), str)
            or not payload["actor"].strip()
            or not isinstance(payload.get("reason"), str)
            or not payload["reason"].strip()
        ):
            raise _invalid()
        validate_status_request_id(details["request_id"])
    except (KeyError, TypeError, ValueError, OverflowError) as error:
        raise _invalid() from error
    return details


def find_status_request(
    connection: sqlite3.Connection, *, request_id: str
) -> dict[str, Any] | None:
    """Return a verified prior command; callers must compare its original intent."""
    request_id = validate_status_request_id(request_id)
    rows = connection.execute(
        _RECEIPT_SELECT + " AND r.scheduled_for=? ORDER BY a.id",
        (ACCOUNT_STATUS_JOB_ID, request_id),
    ).fetchall()
    return _read_receipt(rows) if rows else None


def load_update_frequencies(
    connection: sqlite3.Connection, account_ids: Sequence[int] | None = None
) -> dict[int, str | None]:
    """Validate the manual ledger once and project its latest label per account.

    Filtering untrusted JSON in SQL could hide a corrupted latest receipt. One
    join over this dedicated operator job instead validates the immutable copies
    before selecting accounts, with no per-account queries or silent fallback.
    """
    requested = set(account_ids) if account_ids is not None else None
    if requested is not None and any(type(value) is not int or value < 1 for value in requested):
        raise AccountOperatingStatusError("account_status_account_invalid", "账号编号无效")
    if requested == set():
        return {}
    frequencies: dict[int, str | None] = {value: None for value in requested or ()}
    rows = connection.execute(
        _RECEIPT_SELECT + " ORDER BY r.id,a.id", (ACCOUNT_STATUS_JOB_ID,)
    ).fetchall()
    for _, grouped in groupby(rows, key=lambda row: row["id"]):
        payload = _read_receipt(list(grouped))["payload"]
        account_id = payload["account_id"]
        if requested is None or account_id in requested:
            frequencies[account_id] = payload["update_frequency"]
    return frequencies


def load_admission_members(connection: sqlite3.Connection) -> dict[int, dict[str, Any]]:
    """Read identity-bound, immutable create receipts without inventing provider refs.

    Newer nonempty fields take precedence, while later blank forms cannot erase
    a previously verified sec/profile needed to resume a paused account.
    """
    cursor = connection.cursor()
    cursor.row_factory = lambda selected_cursor, row: sqlite3.Row(selected_cursor, tuple(row))
    rows = cursor.execute(
        _RECEIPT_SELECT + " ORDER BY r.id DESC,a.id", (ACCOUNT_STATUS_JOB_ID,)
    ).fetchall()
    identities = {int(row["id"]): dict(row) for row in cursor.execute(
        "SELECT id,account_id,platform,uid FROM account_platform_identities"
    )}
    result: dict[int, dict[str, Any]] = {}
    for _, grouped in groupby(rows, key=lambda row: row["id"]):
        payload = _read_receipt(list(grouped))["payload"]
        admission = payload["request"].get("admission")
        if admission is None:
            continue
        if not isinstance(admission, dict) or not isinstance(admission.get("member"), dict):
            raise _invalid()
        member = admission["member"]
        identity_id = int(payload["account_identity_id"])
        identity = identities.get(identity_id)
        if (identity is None or identity["account_id"] != payload["account_id"]
                or identity["platform"] != member.get("platform") or identity["uid"] != member.get("uid")):
            raise _invalid()
        metadata = member.get("metadata") or {}
        if not isinstance(metadata, dict):
            raise _invalid()
        values = {key: value for key, value in member.items() if value not in (None, "") and key != "metadata"}
        values["metadata"] = {key: value for key, value in metadata.items() if value not in (None, "")}
        profile_token = urlsplit(str(values.get("profile_ref", ""))).path.rsplit("/", 1)[-1]
        if member["platform"] == "douyin" and profile_token.startswith("MS4w"):
            sec = values["metadata"].get("sec_user_id")
            if sec and sec != profile_token:
                raise _invalid()
            values["metadata"]["sec_user_id"] = profile_token
        if identity_id in result:
            newer = result[identity_id]["member"]
            old_sec = values["metadata"].get("sec_user_id")
            new_sec = newer["metadata"].get("sec_user_id")
            if old_sec and new_sec and old_sec != new_sec:
                raise _invalid()
            values = {**values, **newer, "metadata": {**values["metadata"], **newer["metadata"]}}
        result[identity_id] = {"account_id": payload["account_id"], "member": values}
    return result


def latest_admission_member(
    connection: sqlite3.Connection, account_id: int
) -> dict[str, Any] | None:
    """Recover resolved profile fields for a paused, never-admitted account."""
    for value in load_admission_members(connection).values():
        if value["account_id"] == account_id:
            return dict(value["member"])
    return None


def record_status_receipt(
    connection: sqlite3.Connection,
    *,
    request_id: str,
    account_id: int,
    account_identity_id: int,
    requested_status: str,
    update_frequency: str | None,
    request: Mapping[str, Any],
    actor: str,
    reason: str,
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    result: Mapping[str, Any],
    timestamp: str,
) -> dict[str, Any]:
    """Seal one completed manual command inside the business transaction."""
    if not connection.in_transaction:
        raise AccountOperatingStatusError(
            "account_status_transaction_required", "账号状态回执需要已有事务"
        )
    request_id = validate_status_request_id(request_id)
    if find_status_request(connection, request_id=request_id) is not None:
        raise AccountOperatingStatusError("account_status_request_conflict", "账号状态请求编号已使用")
    payload = {
        "account_id": account_id, "account_identity_id": account_identity_id,
        "requested_status": requested_status, "update_frequency": update_frequency,
        "request": dict(request), "actor": actor, "reason": reason,
        "before": dict(before), "after": dict(after), "result": dict(result),
    }
    cursor = connection.execute(
        "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,details_json) "
        "VALUES (?,?,'running',?,'{}')", (ACCOUNT_STATUS_JOB_ID, request_id, timestamp),
    )
    run_id = int(cursor.lastrowid or 0)
    attempt_cursor = connection.execute(
        "INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,invocation_source,"
        "status,started_at,details_json) VALUES (?,1,'operator_retry','running',?,'{}')",
        (run_id, timestamp),
    )
    attempt_id = int(attempt_cursor.lastrowid or 0)
    details = {
        "contract_version": ACCOUNT_STATUS_RECEIPT_VERSION,
        "job_id": ACCOUNT_STATUS_JOB_ID,
        "run_id": run_id, "attempt_id": attempt_id, "request_id": request_id,
        "recorded_at": timestamp, "payload": payload,
    }
    details["self_sha256"] = _digest(details)
    encoded = _canonical(details)
    attempt = connection.execute(
        "UPDATE scheduler_run_attempts SET status='succeeded',completed_at=?,details_json=? "
        "WHERE id=? AND status='running'", (timestamp, encoded, attempt_id),
    )
    run = connection.execute(
        "UPDATE scheduler_runs SET status='succeeded',completed_at=?,details_json=? "
        "WHERE id=? AND status='running'", (timestamp, encoded, run_id),
    )
    if attempt.rowcount != 1 or run.rowcount != 1:
        raise _invalid()
    verified = find_status_request(connection, request_id=request_id)
    assert verified is not None
    return verified
