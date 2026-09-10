"""Directory-only status edits without capture admission or paid requests.

Completed commands use a separate job namespace in the existing immutable
scheduler receipt tables. No scheduler job is registered for these receipts.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Mapping
from uuid import uuid4

from .account_directory import has_account_directory
from .account_operating_receipts import (
    AccountOperatingStatusError, _canonical, _digest, _RECEIPT_SELECT,
    find_status_request, validate_status_request_id,
)
from .storage import now_utc


DIRECTORY_STATUS_JOB = "operator_account_directory_status"
DIRECTORY_STATUS_CONTRACT = "account-directory-status-v1"
STATUSES = frozenset({"daily", "weekly", "paused"})


def _error(code: str, message: str) -> AccountOperatingStatusError:
    return AccountOperatingStatusError(code, message)


def find_directory_status_request(connection: sqlite3.Connection, request_id: str) -> dict[str, Any] | None:
    request_id = validate_status_request_id(request_id)
    rows = connection.execute(
        _RECEIPT_SELECT + " AND r.scheduled_for=? ORDER BY a.id",
        (DIRECTORY_STATUS_JOB, request_id),
    ).fetchall()
    if not rows:
        return None
    try:
        if len(rows) != 1:
            raise ValueError("duplicate receipt")
        row = rows[0]
        details = json.loads(row["details_json"])
        frozen = dict(details)
        checksum = frozen.pop("self_sha256")
        payload = details["payload"]
        target, result = payload["target"], payload["result"]
        requested_status = payload["account_status"]
        if (
            row["status"] != "succeeded"
            or row["started_at"] != row["completed_at"]
            or row["receipt_attempt_id"] is None
            or row["receipt_attempt_number"] != 1
            or row["receipt_invocation_source"] != "operator_retry"
            or row["receipt_attempt_status"] != "succeeded"
            or row["receipt_attempt_started_at"] != row["started_at"]
            or row["receipt_attempt_completed_at"] != row["completed_at"]
            or row["receipt_attempt_details_json"] != row["details_json"]
            or _canonical(details) != row["details_json"]
            or checksum != _digest(frozen)
            or details["contract_version"] != DIRECTORY_STATUS_CONTRACT
            or details["job_id"] != DIRECTORY_STATUS_JOB
            or details["run_id"] != row["id"]
            or details["attempt_id"] != row["receipt_attempt_id"]
            or details["request_id"] != request_id
            or details["recorded_at"] != row["started_at"]
            or type(target["directory_row_id"]) is not int or target["directory_row_id"] < 1
            or type(target["id"]) is not int or target["id"] == 0
            or target["identity_status"] not in {"identity_missing", "uid_unverified", "existing_verified"}
            or (target["id"] < 0 and (target["id"] != -target["directory_row_id"]
                or target["account_id"] is not None or target["identity_status"] != "identity_missing"))
            or (target["id"] > 0 and (target["account_id"] != target["id"]
                or target["identity_status"] not in {"uid_unverified", "existing_verified"}))
            or (target["identity_status"] == "existing_verified" and (
                not isinstance(target.get("capture_selection_sha256"), str)
                or len(target["capture_selection_sha256"]) != 64
                or any(char not in "0123456789abcdef" for char in target["capture_selection_sha256"])))
            or requested_status not in STATUSES
            or payload["previous_status"] not in STATUSES | {"unmarked"}
            or not isinstance(payload["actor"], str) or not payload["actor"].strip()
            or not isinstance(payload["reason"], str) or not payload["reason"].strip()
            or result["id"] != target["id"]
            or result["directory_row_id"] != target["directory_row_id"]
            or result["account_status"] != requested_status
            or result["update_frequency"] != (requested_status if requested_status in {"daily", "weekly"} else None)
            or result["enabled"] is not False
            or result["status_request_id"] != request_id
        ):
            raise ValueError("invalid receipt")
        return details
    except (KeyError, TypeError, ValueError, OverflowError) as error:
        raise _error("account_directory_status_receipt_invalid", "账号状态操作回执损坏，请联系管理员。") from error


def _record_receipt(connection: sqlite3.Connection, request_id: str, payload: Mapping[str, Any], timestamp: str) -> None:
    run = connection.execute(
        "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,details_json) VALUES (?,?,'running',?,'{}')",
        (DIRECTORY_STATUS_JOB, request_id, timestamp),
    )
    run_id = int(run.lastrowid)
    attempt = connection.execute(
        "INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,invocation_source,status,started_at,details_json) "
        "VALUES (?,1,'operator_retry','running',?,'{}')", (run_id, timestamp),
    )
    attempt_id = int(attempt.lastrowid)
    details = {"contract_version": DIRECTORY_STATUS_CONTRACT, "job_id": DIRECTORY_STATUS_JOB,
               "run_id": run_id, "attempt_id": attempt_id, "request_id": request_id,
               "recorded_at": timestamp, "payload": dict(payload)}
    details["self_sha256"] = _digest(details)
    encoded = _canonical(details)
    connection.execute(
        "UPDATE scheduler_run_attempts SET status='succeeded',completed_at=?,details_json=? WHERE id=? AND status='running'",
        (timestamp, encoded, attempt_id),
    )
    connection.execute(
        "UPDATE scheduler_runs SET status='succeeded',completed_at=?,details_json=? WHERE id=? AND status='running'",
        (timestamp, encoded, run_id),
    )
    if find_directory_status_request(connection, request_id) is None:
        raise _error("account_directory_status_receipt_invalid", "账号状态操作回执未保存。")


def _outside_cleanup_capture_scope(connection: sqlite3.Connection, row: sqlite3.Row | None) -> str | None:
    """Find a verified but disabled directory row outside the frozen paid scope."""
    if row is None or row["identity_status"] != "existing_verified" or row["account_id"] is None:
        return None
    from .account_catalog_capture import installed_policy
    if installed_policy(connection, at=now_utc()) is not None:
        # Verified directory accounts use the ordinary status transaction. The
        # historical cleanup selection is no longer a business admission list.
        return None
    account = connection.execute("SELECT enabled FROM accounts WHERE id=?", (row["account_id"],)).fetchone()
    if account is None or account["enabled"] or connection.execute("PRAGMA user_version").fetchone()[0] not in {20, 21}:
        return None
    from . import account_cleanup_runtime as cleanup
    from .capture_authorizations import AuthorizationError
    from .profile_activations import activation_at

    timestamp = now_utc()
    active = activation_at(connection, timestamp)
    if active is None or "account_cleanup" not in active.get("metadata", {}):
        return None
    try:
        evidence = cleanup.installed_evidence(connection, at=timestamp, maintenance_only=True)
        proof = evidence["account_cleanup_generation"]
        capsule = cleanup.private_object(proof["source_authority"])
        if any(member["account_id"] == row["account_id"] for member in capsule["eligible_members"]):
            return None
        return proof["selection_sha256"]
    except AuthorizationError as error:
        raise _error("account_cleanup_status_unavailable", "当前账号名单校验未通过，请稍后重试。") from error


def update_directory_only_status_in_transaction(
    connection: sqlite3.Connection, account_id: int, values: Mapping[str, Any], *, actor: str, reason: str,
) -> dict[str, Any] | None:
    """Handle directory labels, or return None for the capture-enabled flow.

    A negative display ID is valid only while its exact directory row still has
    no account identity. Unverified identities and verified identities outside
    the frozen capture scope must remain disabled; only their labels change.
    The caller owns the write transaction; the savepoint also protects callers
    that catch a receipt failure inside their transaction.
    """
    if not connection.in_transaction:
        raise _error("account_status_transaction_required", "账号状态更新需要已有事务")
    if not has_account_directory(connection):
        return None
    row = connection.execute(
        "SELECT * FROM account_directory_rows WHERE " + ("id=?" if account_id < 0 else "account_id=?"),
        (-account_id if account_id < 0 else account_id,),
    ).fetchone()
    supplied_id = values.get("status_request_id")
    existing = (find_directory_status_request(connection, supplied_id)
                if supplied_id is not None else None)
    selection_sha256 = (_outside_cleanup_capture_scope(connection, row)
                        if account_id > 0 and "account_status" in values else None)
    if account_id >= 0 and (row is None or (row["identity_status"] == "existing_verified" and selection_sha256 is None)):
        if existing is not None:
            raise _error("account_status_request_conflict", "账号身份已变更，请刷新后重试。")
        return None
    if row is None:
        raise _error("account_not_found", "账号不存在，请刷新后重试。")
    if account_id < 0 and (row["account_id"] is not None or row["identity_status"] != "identity_missing"):
        raise _error("account_directory_identity_changed", "账号身份已完善，请刷新后重试。")
    if account_id > 0:
        account = connection.execute("SELECT enabled FROM accounts WHERE id=?", (account_id,)).fetchone()
        identity = connection.execute(
            "SELECT 1 FROM account_platform_identities WHERE account_id=? AND platform=? AND uid=?",
            (account_id, row["platform"], row["uid"]),
        ).fetchone()
        if (row["identity_status"] != "uid_unverified" and selection_sha256 is None) or account is None or account["enabled"] or identity is None:
            raise _error("account_directory_identity_changed", "账号身份状态已变更，请刷新后重试。")
    if "account_status" not in values and account_id > 0:
        return None
    status = values.get("account_status")
    if not isinstance(status, str) or status not in STATUSES:
        raise _error("account_status_invalid", "账号状态必须是日更、周更或暂停")
    if set(values) - {"account_status", "status_request_id"}:
        raise _error("account_directory_status_fields_invalid", "请单独修改此账号的状态。")
    if not actor.strip() or not reason.strip():
        raise _error("account_status_reason_required", "账号状态更新需要操作人和原因")
    request_id = validate_status_request_id(supplied_id) if "status_request_id" in values else str(uuid4())
    if find_status_request(connection, request_id=request_id) is not None:
        raise _error("account_status_request_conflict", "该请求编号已用于不同的账号修改，请刷新后重试")
    target = {"id": account_id, "directory_row_id": row["id"], "account_id": row["account_id"],
              "identity_status": row["identity_status"], "platform": row["platform"], "uid": row["uid"],
              "source_sha256": row["source_sha256"], "source_row": row["source_row"]}
    if selection_sha256 is not None:
        target["capture_selection_sha256"] = selection_sha256
    intent = {"target": target, "account_status": status, "actor": actor, "reason": reason}
    if existing is not None:
        if any(existing["payload"].get(key) != value for key, value in intent.items()):
            raise _error("account_status_request_conflict", "该请求编号已用于不同的账号修改，请刷新后重试")
        return {**existing["payload"]["result"], "status_replayed": True}
    timestamp = now_utc()
    result = {"id": account_id, "directory_row_id": row["id"], "account_status": status,
              "update_frequency": status if status in {"daily", "weekly"} else None,
              "enabled": False, "status_request_id": request_id, "message": "账号状态已更新。"}
    connection.execute("SAVEPOINT directory_account_status")
    try:
        updated = connection.execute(
            "UPDATE account_directory_rows SET account_status=?,updated_at=? "
            "WHERE id=? AND account_id IS ? AND identity_status=?",
            (status, timestamp, row["id"], row["account_id"], row["identity_status"]),
        )
        if updated.rowcount != 1:
            raise _error("account_directory_identity_changed", "账号身份已变更，请刷新后重试。")
        _record_receipt(connection, request_id, {**intent, "previous_status": row["account_status"], "result": result}, timestamp)
    except BaseException:
        connection.execute("ROLLBACK TO directory_account_status")
        connection.execute("RELEASE directory_account_status")
        raise
    connection.execute("RELEASE directory_account_status")
    return result
