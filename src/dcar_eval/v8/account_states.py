"""Atomic enabled-state changes backed by append-only identity events."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager, nullcontext
from datetime import datetime, timezone
from typing import Any, Iterator, Mapping

from .storage import write_lock


CONTRACT_VERSION = "account-state-event-v1"


class AccountStateError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _canonical(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _timestamp(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as error:
        raise AccountStateError(
            "account_state_time_invalid", "Timezone-aware account-state time is required"
        ) from error
    if parsed.tzinfo is None:
        raise AccountStateError(
            "account_state_time_invalid", "Timezone-aware account-state time is required"
        )
    return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


@contextmanager
def _atomic(connection: sqlite3.Connection) -> Iterator[None]:
    nested = connection.in_transaction
    # A standalone BEGIN IMMEDIATE must hold the process write lock for its
    # whole span (see storage.write_lock); a nested SAVEPOINT already runs
    # inside the caller's locked transaction.
    with nullcontext() if nested else write_lock():
        connection.execute("SAVEPOINT account_state" if nested else "BEGIN IMMEDIATE")
        try:
            yield
        except Exception:
            if nested:
                connection.execute("ROLLBACK TO account_state")
                connection.execute("RELEASE account_state")
            else:
                connection.rollback()
            raise
        else:
            if nested:
                connection.execute("RELEASE account_state")
            else:
                connection.commit()


def event_digest(value: Mapping[str, Any]) -> str:
    keys = (
        "account_identity_id",
        "activation_id",
        "old_enabled",
        "new_enabled",
        "effective_at",
        "actor",
        "reason",
        "contract_version",
        "metadata",
        "created_at",
    )
    payload = {key: value.get(key) for key in keys}
    return hashlib.sha256(_canonical(payload).encode("utf-8")).hexdigest()


def _event(row: sqlite3.Row | Mapping[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result["event_id"] = int(result.pop("id"))
    result["old_enabled"] = bool(result["old_enabled"])
    result["new_enabled"] = bool(result["new_enabled"])
    result["metadata"] = json.loads(result.pop("metadata_json"))
    if result["event_sha256"] != event_digest(result):
        raise AccountStateError(
            "account_state_event_invalid", "Account-state event digest is invalid"
        )
    return result


def state_events(
    connection: sqlite3.Connection, account_identity_id: int
) -> list[dict[str, Any]]:
    values = [
        _event(row)
        for row in connection.execute(
            """SELECT * FROM account_state_events
               WHERE account_identity_id=? ORDER BY id""",
            (account_identity_id,),
        )
    ]
    for previous, current in zip(values, values[1:]):
        if previous["new_enabled"] != current["old_enabled"]:
            raise AccountStateError(
                "account_state_chain_invalid", "Account-state transition chain is invalid"
            )
        if previous["effective_at"] >= current["effective_at"]:
            raise AccountStateError(
                "account_state_effective_order_invalid",
                "Account-state effective times must be strictly increasing",
            )
    return values


def set_account_enabled_in_transaction(
    connection: sqlite3.Connection,
    account_identity_id: int,
    *,
    enabled: bool,
    effective_at: str,
    actor: str,
    reason: str,
    created_at: str,
    activation_id: int | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Apply one enabled-state transition inside the caller's transaction."""

    if not connection.in_transaction:
        raise AccountStateError(
            "account_state_transaction_required",
            "Account-state changes require an active transaction",
        )
    if type(enabled) is not bool:
        raise AccountStateError(
            "account_state_invalid", "Enabled state must be a boolean"
        )
    actor = str(actor).strip()
    reason = str(reason).strip()
    if not actor or not reason:
        raise AccountStateError(
            "account_state_reason_required", "Actor and reason are required"
        )
    effective = _timestamp(effective_at)
    created = _timestamp(created_at)
    if effective > created:
        raise AccountStateError(
            "account_state_future_invalid", "Enabled state cannot be applied before it is effective"
        )
    metadata_value = dict(metadata or {})
    _canonical(metadata_value)
    row = connection.execute(
        """SELECT i.id identity_id,i.account_id,a.enabled
           FROM account_platform_identities i
           JOIN accounts a ON a.id=i.account_id WHERE i.id=?""",
        (account_identity_id,),
    ).fetchone()
    if row is None:
        raise AccountStateError(
            "account_identity_not_found", "Account identity does not exist"
        )
    old_enabled = bool(row["enabled"])
    if old_enabled is enabled:
        return {
            "status": "unchanged",
            "account_identity_id": account_identity_id,
            "enabled": enabled,
        }
    previous = connection.execute(
        """SELECT MAX(effective_at) effective_at FROM account_state_events
           WHERE account_identity_id=?""",
        (account_identity_id,),
    ).fetchone()
    if previous is not None and previous["effective_at"] is not None and effective <= str(
        previous["effective_at"]
    ):
        raise AccountStateError(
            "account_state_effective_order_invalid",
            "Account-state effective times must be strictly increasing",
        )
    connection.execute(
        "UPDATE accounts SET enabled=?,updated_at=? WHERE id=?",
        (int(enabled), created, row["account_id"]),
    )
    value: dict[str, Any] = {
        "account_identity_id": int(account_identity_id),
        "activation_id": activation_id,
        "old_enabled": old_enabled,
        "new_enabled": enabled,
        "effective_at": effective,
        "actor": actor,
        "reason": reason,
        "contract_version": CONTRACT_VERSION,
        "metadata": metadata_value,
        "created_at": created,
    }
    value["event_sha256"] = event_digest(value)
    cursor = connection.execute(
        """INSERT INTO account_state_events(
               account_identity_id,activation_id,old_enabled,new_enabled,
               effective_at,actor,reason,contract_version,event_sha256,
               metadata_json,created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (
            value["account_identity_id"],
            value["activation_id"],
            int(value["old_enabled"]),
            int(value["new_enabled"]),
            value["effective_at"],
            value["actor"],
            value["reason"],
            value["contract_version"],
            value["event_sha256"],
            _canonical(value["metadata"]),
            value["created_at"],
        ),
    )
    inserted = connection.execute(
        "SELECT * FROM account_state_events WHERE id=?", (cursor.lastrowid,)
    ).fetchone()
    if inserted is None:
        raise AccountStateError(
            "account_state_event_invalid", "Account-state event was not retained"
        )
    return {"status": "changed", **_event(inserted)}


def set_account_enabled(
    connection: sqlite3.Connection,
    account_identity_id: int,
    *,
    enabled: bool,
    effective_at: str,
    actor: str,
    reason: str,
    created_at: str,
    activation_id: int | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Update the account and append its matching immutable event atomically."""

    with _atomic(connection):
        return set_account_enabled_in_transaction(
            connection,
            account_identity_id,
            enabled=enabled,
            effective_at=effective_at,
            actor=actor,
            reason=reason,
            created_at=created_at,
            activation_id=activation_id,
            metadata=metadata,
        )
