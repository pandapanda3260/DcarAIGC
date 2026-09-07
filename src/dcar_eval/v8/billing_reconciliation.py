"""Audited manual reconciliation for TikHub ``billing_unknown`` usage.

This command never contacts TikHub and never guesses a billing outcome.  An
operator must compare one usage row with TikHub's billing records, then submit
the exact usage/slot/attempt identity and an evidence reference.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pwd
import sqlite3
import stat
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Literal, Sequence

from . import raw_archive
from .capture import (
    BILLING_UNKNOWN_SLOT_ERROR,
    clear_billing_unknown_slot_guard_if_resolved,
)
from .paid_dispatch import (
    PaidDispatchError,
    dispatch_events,
    dispatch_id_for_usage,
    supports_dispatch_ledger,
)
from .provider_budget import finish_recovery_probe, micro_usd
from .raw_evidence import RawEvidenceError, read_raw_json
from .runtime_database import (
    DatabaseAccessMode,
    ResolvedDatabaseAccess,
    RuntimeDatabaseError,
    hold_formal_mutation,
    is_installed_formal_database,
    resolve_installed_database_access,
    resolve_isolated_candidate,
)
from .storage import (
    PROJECT_ROOT,
    SchemaMigrationError,
    connect,
    now_utc,
    require_schema_compatibility,
    transaction,
)


CONTRACT_VERSION = "tikhub-billing-reconciliation-v1"
OUTCOMES = ("billed", "unbilled")
Outcome = Literal["billed", "unbilled"]


class BillingReconciliationError(RuntimeError):
    """A reconciliation request failed a fail-closed identity or ledger gate."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.error_code = code


def _object(value: str | None) -> dict[str, Any]:
    try:
        result = json.loads(value or "{}")
    except ValueError as exc:
        raise BillingReconciliationError(
            "invalid_usage_metadata", "Provider usage metadata is not valid JSON"
        ) from exc
    if not isinstance(result, dict):
        raise BillingReconciliationError(
            "invalid_usage_metadata", "Provider usage metadata is not an object"
        )
    return result


def _reference(value: str, *, label: str) -> str:
    cleaned = value.strip()
    if (
        not cleaned
        or len(cleaned) > 300
        or any(ord(character) < 32 for character in cleaned)
    ):
        raise BillingReconciliationError(
            "invalid_reconciliation_reference", f"{label} must be 1-300 printable characters"
        )
    return cleaned


def _require_formal_freeze(access: ResolvedDatabaseAccess) -> None:
    if access.access_mode is DatabaseAccessMode.ISOLATED_CANDIDATE:
        return
    if access.project_root is None or access.installed is None:
        raise BillingReconciliationError(
            "formal_database_identity_unresolved",
            "Formal reconciliation requires the installed writer contract",
        )
    freeze_lock = access.project_root / "runtime" / "operator-freeze.lock"
    environment = access.installed.payload.get("EnvironmentVariables")
    if (
        not isinstance(environment, dict)
        or environment.get("DCAR_OPERATOR_FREEZE_LOCK") != str(freeze_lock)
    ):
        raise BillingReconciliationError(
            "formal_writer_contract_mismatch",
            "Installed writer operator-freeze path is not canonical",
        )
    try:
        identity = freeze_lock.lstat()
    except FileNotFoundError as exc:
        raise BillingReconciliationError(
            "formal_freeze_missing",
            f"Formal reconciliation requires {freeze_lock}",
        ) from exc
    if (
        freeze_lock.is_symlink()
        or not stat.S_ISREG(identity.st_mode)
        or identity.st_uid != os.geteuid()
        or identity.st_nlink != 1
        or stat.S_IMODE(identity.st_mode) != 0o600
    ):
        raise BillingReconciliationError(
            "formal_freeze_invalid",
            "Formal reconciliation requires the canonical owner-only 0600 freeze lock",
        )


@contextmanager
def _billing_mutation_access(
    db_path: Path, *, isolated: bool
) -> Iterator[ResolvedDatabaseAccess]:
    try:
        if isolated:
            yield resolve_isolated_candidate(db_path)
            return
        with hold_formal_mutation(db_path, project_root=PROJECT_ROOT) as access:
            _require_formal_freeze(access)
            yield access
    except RuntimeDatabaseError as exc:
        raise BillingReconciliationError(
            "formal_database_identity_unresolved", str(exc)
        ) from exc


@contextmanager
def open_live_read_only(path: Path) -> Iterator[sqlite3.Connection]:
    """Open a WAL-aware reader; deliberately do not use ``immutable=1``."""
    resolved = Path(path).expanduser()
    if (
        os.environ.get("DCAR_TEST_DENY_FORMAL_DB") == "1"
        and is_installed_formal_database(resolved, required=False)
    ):
        raise RuntimeError("test process attempted to open the formal DCar database")
    if not resolved.is_file():
        raise BillingReconciliationError(
            "database_missing", f"SQLite database is missing: {resolved}"
        )
    connection = sqlite3.connect(
        resolved.resolve().as_uri() + "?mode=ro", uri=True, timeout=10
    )
    try:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA busy_timeout=10000")
        connection.execute("PRAGMA recursive_triggers=ON")
        connection.execute("PRAGMA foreign_keys=ON")
        try:
            require_schema_compatibility(connection, supported_versions=frozenset({19, 20}))
        except SchemaMigrationError as exc:
            raise BillingReconciliationError(
                "schema_mismatch", "Billing reconciliation requires a complete schema 19 or 20"
            ) from exc
        connection.execute("BEGIN")
        yield connection
    finally:
        if connection.in_transaction:
            connection.rollback()
        connection.close()


def _verified_raw_receipt(
    row: sqlite3.Row, *, connection: sqlite3.Connection | None = None
) -> dict[str, Any]:
    """Verify frozen raw bytes and expose only reconciliation identifiers."""
    local_path = Path(str(row["local_path"]))
    resolved = local_path if local_path.is_absolute() else PROJECT_ROOT / local_path
    try:
        if "raw_blob_id" in row.keys() and row["raw_blob_id"] is not None:
            if connection is None:
                raise RawEvidenceError("managed raw receipt requires its database connection")
            payload = json.loads(raw_archive.read_response_entity(connection, int(row["id"])))
        else:
            payload = read_raw_json(
                resolved,
                expected_stored_sha256=str(row["sha256"]),
                expected_stored_size=int(row["byte_size"]),
            )
    except (OSError, RawEvidenceError, TypeError, ValueError) as exc:
        if isinstance(exc, raw_archive.RawArchiveError) and str(exc).startswith("raw_expired:"):
            raise BillingReconciliationError(
                "raw_expired", f"Raw response {row['id']} has expired; billing evidence cannot be verified"
            ) from exc
        raise BillingReconciliationError(
            "raw_response_integrity_failed",
            f"Raw response {row['id']} failed immutable receipt readback",
        ) from exc
    detail = payload.get("detail") if isinstance(payload, dict) else None
    request_id = detail.get("request_id") if isinstance(detail, dict) else None
    if not isinstance(request_id, str) or not request_id.strip() or len(request_id) > 200:
        request_id = None
    return {
        "raw_response_id": int(row["id"]),
        "http_status": row["http_status"],
        "sha256": str(row["sha256"]),
        "byte_size": int(row["byte_size"]),
        "local_path": str(row["local_path"]),
        "request_id": request_id,
        "evidence_valid": True,
    }


def unknown_billing_inventory(
    connection: sqlite3.Connection, *, after_id: int = 0, limit: int = 100
) -> dict[str, Any]:
    """Return a bounded, audit-ready page plus the complete unresolved total."""
    if after_id < 0 or not 1 <= limit <= 1000:
        raise BillingReconciliationError(
            "invalid_inventory_page", "after_id must be nonnegative and limit must be 1-1000"
        )
    total = connection.execute(
        """WITH unknown AS (
                 SELECT * FROM provider_usage
                 WHERE lower(provider)='tikhub' AND json_valid(details_json)
                   AND json_extract(details_json,'$.state')='billing_unknown'
             )
             SELECT COUNT(*) rows,
                    COUNT(DISTINCT CAST(json_extract(unknown.details_json,'$.slot_id') AS INTEGER)) slots,
                    COALESCE(SUM(unknown.amount),0) amount,
                    MIN(unknown.id) first_id,MAX(unknown.id) last_id,
                    MIN(unknown.recorded_at) first_recorded_at,
                    MAX(unknown.recorded_at) last_recorded_at,
                    SUM(CASE
                          WHEN json_type(unknown.details_json,'$.slot_id') IS NOT 'integer'
                            OR json_type(unknown.details_json,'$.attempt_number') IS NOT 'integer'
                            OR fs.id IS NULL OR fa.id IS NULL
                          THEN 1 ELSE 0
                        END) invalid_rows
             FROM unknown
             LEFT JOIN fetch_slots fs
               ON fs.id=CAST(json_extract(unknown.details_json,'$.slot_id') AS INTEGER)
             LEFT JOIN fetch_attempts fa
               ON fa.slot_id=fs.id
              AND fa.attempt_number=CAST(json_extract(unknown.details_json,'$.attempt_number') AS INTEGER)"""
    ).fetchone()
    rows = connection.execute(
        """SELECT pu.*,fs.content_id,fs.account_id,fs.stage,fs.window_key,
                  fs.id linked_slot_id,fs.status slot_status,
                  fs.last_error_code slot_error_code,
                  ci.platform content_platform,
                  ci.platform_content_id,
                  ci.account_id content_account_id,
                  fa.id fetch_attempt_id,fa.request_started_at,fa.response_finished_at,
                  fa.http_status,fa.error_code attempt_error_code
           FROM provider_usage pu
           LEFT JOIN fetch_slots fs
             ON fs.id=CAST(json_extract(pu.details_json,'$.slot_id') AS INTEGER)
           LEFT JOIN content_items ci ON ci.id=fs.content_id
           LEFT JOIN fetch_attempts fa
             ON fa.slot_id=fs.id
            AND fa.attempt_number=CAST(json_extract(pu.details_json,'$.attempt_number') AS INTEGER)
           WHERE pu.id>? AND lower(pu.provider)='tikhub' AND json_valid(pu.details_json)
             AND json_extract(pu.details_json,'$.state')='billing_unknown'
           ORDER BY pu.id LIMIT ?""",
        (after_id, limit),
    ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        details = _object(row["details_json"])
        slot_id = details.get("slot_id")
        attempt_number = details.get("attempt_number")
        invalid_reason = None
        if type(slot_id) is not int or type(attempt_number) is not int:
            invalid_reason = "invalid_slot_attempt_identity"
        elif row["linked_slot_id"] is None:
            invalid_reason = "missing_fetch_slot"
        elif row["fetch_attempt_id"] is None:
            invalid_reason = "missing_fetch_attempt"
        raw_receipts: list[dict[str, Any]] = []
        if invalid_reason is None:
            for raw in connection.execute(
                """SELECT *
                   FROM provider_raw_responses
                   WHERE fetch_attempt_id=? ORDER BY id""",
                (row["fetch_attempt_id"],),
            ):
                try:
                    raw_receipts.append(_verified_raw_receipt(raw, connection=connection))
                except BillingReconciliationError as exc:
                    raw_receipts.append(
                        {
                            "raw_response_id": int(raw["id"]),
                            "http_status": raw["http_status"],
                            "sha256": str(raw["sha256"]),
                            "byte_size": int(raw["byte_size"]),
                            "local_path": str(raw["local_path"]),
                            "request_id": None,
                            "evidence_valid": False,
                            "evidence_error": exc.error_code,
                        }
                    )
                    invalid_reason = exc.error_code
        latest_attempt_id = None
        if invalid_reason is None:
            latest = connection.execute(
                """SELECT id FROM fetch_attempts
                   WHERE slot_id=? ORDER BY attempt_number DESC LIMIT 1""",
                (slot_id,),
            ).fetchone()
            latest_attempt_id = int(latest[0]) if latest is not None else None
        dispatch_evidence = None
        if invalid_reason is None:
            assert type(slot_id) is int and row["fetch_attempt_id"] is not None
            try:
                dispatch_evidence = _verified_dispatch_evidence(
                    connection,
                    usage=row,
                    slot_id=int(slot_id),
                    fetch_attempt_id=int(row["fetch_attempt_id"]),
                    details=details,
                )
            except BillingReconciliationError as exc:
                invalid_reason = exc.error_code
        scope = details.get("scope")
        if not isinstance(scope, dict):
            scope = {}
        subject_account_id = row["account_id"] or row["content_account_id"]
        account_identities = []
        if subject_account_id is not None:
            account_identities = [
                {"platform": str(identity["platform"]), "uid": str(identity["uid"])}
                for identity in connection.execute(
                    """SELECT platform,uid FROM account_platform_identities
                       WHERE account_id=? ORDER BY platform,uid""",
                    (subject_account_id,),
                )
            ]
        items.append(
            {
                "valid": invalid_reason is None,
                "invalid_reason": invalid_reason,
                "usage_id": int(row["id"]),
                "slot_id": slot_id,
                "attempt_number": attempt_number,
                "fetch_attempt_id": (
                    int(row["fetch_attempt_id"])
                    if row["fetch_attempt_id"] is not None
                    else None
                ),
                "content_id": row["content_id"],
                "content_platform": row["content_platform"],
                "platform_content_id": row["platform_content_id"],
                "account_id": row["account_id"],
                "subject_account_id": subject_account_id,
                "account_identities": account_identities,
                "scope_platform": scope.get("platform"),
                "scope_uid": scope.get("uid"),
                "stage": row["stage"],
                "window_key": row["window_key"],
                "slot_status": row["slot_status"],
                "slot_error_code": row["slot_error_code"],
                "slot_guard_materialized": (
                    row["slot_error_code"] == BILLING_UNKNOWN_SLOT_ERROR
                ),
                "retry_block_required": row["slot_status"]
                in {"pending", "retryable_failed", "terminal_failed"},
                "shadowed_by_later_attempt": bool(
                    row["slot_status"] == "succeeded"
                    or (
                        latest_attempt_id is not None
                        and row["fetch_attempt_id"] != latest_attempt_id
                    )
                ),
                "operation": row["operation"],
                "category": details.get("category"),
                "budget_day": details.get("budget_day"),
                "amount": row["amount"],
                "currency": row["currency"],
                "recorded_at": row["recorded_at"],
                "request_started_at": row["request_started_at"],
                "response_finished_at": row["response_finished_at"],
                "http_status": row["http_status"],
                "error_code": details.get("error_code")
                or row["attempt_error_code"]
                or details.get("recovery_reason"),
                "raw_receipts": raw_receipts,
                "dispatch_evidence": dispatch_evidence,
            }
        )
    return {
        "contract_version": CONTRACT_VERSION,
        "summary": {
            "unresolved_count": int(total["rows"]),
            "unresolved_slots": int(total["slots"]),
            "unresolved_microusd": micro_usd(total["amount"]),
            "invalid_linkage_count": int(total["invalid_rows"] or 0),
            "first_usage_id": total["first_id"],
            "last_usage_id": total["last_id"],
            "first_recorded_at": total["first_recorded_at"],
            "last_recorded_at": total["last_recorded_at"],
        },
        "items": items,
        "next_after_id": items[-1]["usage_id"] if len(items) == limit else None,
    }


def _batch_totals(
    connection: sqlite3.Connection, budget_batch_id: str
) -> tuple[int, int]:
    row = connection.execute(
        """SELECT COALESCE(SUM(billed_requests),0) requests,
                  COALESCE(SUM(amount),0) amount
           FROM provider_usage WHERE budget_batch_id=?""",
        (budget_batch_id,),
    ).fetchone()
    return int(row["requests"]), micro_usd(row["amount"])


def _subject_identity(
    connection: sqlite3.Connection, slot: sqlite3.Row
) -> dict[str, Any]:
    content = None
    if slot["content_id"] is not None:
        row = connection.execute(
            """SELECT id,platform,platform_content_id,account_id
               FROM content_items WHERE id=?""",
            (slot["content_id"],),
        ).fetchone()
        if row is not None:
            content = {key: row[key] for key in row.keys()}
    account_id = slot["account_id"] or (content or {}).get("account_id")
    account_identities = []
    if account_id is not None:
        account_identities = [
            {"platform": str(row["platform"]), "uid": str(row["uid"])}
            for row in connection.execute(
                """SELECT platform,uid FROM account_platform_identities
                   WHERE account_id=? ORDER BY platform,uid""",
                (account_id,),
            )
        ]
    return {
        "content": content,
        "account_id": account_id,
        "account_identities": account_identities,
    }


def _verified_dispatch_evidence(
    connection: sqlite3.Connection,
    *,
    usage: sqlite3.Row,
    slot_id: int,
    fetch_attempt_id: int,
    details: dict[str, Any],
) -> dict[str, Any] | None:
    """Verify schema19 dispatch lineage without mutating its append-only chain."""
    scope = details.get("scope")
    activation_id = scope.get("activation_id") if isinstance(scope, dict) else None
    dispatch_id = dispatch_id_for_usage(connection, int(usage["id"]))
    if dispatch_id is None:
        if type(activation_id) is int:
            raise BillingReconciliationError(
                "missing_paid_dispatch_evidence",
                "Schema 19 billing unknown usage has no paid dispatch evidence",
            )
        return None
    if not supports_dispatch_ledger(connection):
        raise BillingReconciliationError(
            "invalid_paid_dispatch_evidence", "Paid dispatch evidence is unavailable"
        )
    try:
        events = dispatch_events(connection, dispatch_id)
    except PaidDispatchError as exc:
        raise BillingReconciliationError(
            "invalid_paid_dispatch_evidence", str(exc)
        ) from exc
    if [event.event_type for event in events] != [
        "reserved",
        "send_marked",
        "billing_unknown",
    ]:
        raise BillingReconciliationError(
            "invalid_paid_dispatch_evidence",
            "Billing unknown usage has no complete dispatch terminal chain",
        )
    terminal = events[-1]
    expected_business_day = details.get("budget_day")
    if (
        any(event.provider_usage_id != int(usage["id"]) for event in events)
        or any(event.fetch_slot_id != slot_id for event in events)
        or terminal.fetch_attempt_id != fetch_attempt_id
        or str(terminal.provider).lower() != "tikhub"
        or terminal.operation != usage["operation"]
        or type(activation_id) is not int
        or terminal.activation_id != activation_id
        or (
            isinstance(scope, dict)
            and scope.get("scheduler_run_id") is not None
            and terminal.scheduler_run_id != scope.get("scheduler_run_id")
        )
        or (
            isinstance(scope, dict)
            and scope.get("scheduler_attempt_id") is not None
            and terminal.scheduler_attempt_id != scope.get("scheduler_attempt_id")
        )
        or not isinstance(expected_business_day, str)
        or terminal.business_day != expected_business_day
    ):
        raise BillingReconciliationError(
            "invalid_paid_dispatch_evidence",
            "Paid dispatch evidence does not match the billing usage identity",
        )
    return {
        "contract_version": terminal.contract_version,
        "dispatch_id": dispatch_id,
        "activation_id": terminal.activation_id,
        "business_day": terminal.business_day,
        "scheduler_run_id": terminal.scheduler_run_id,
        "scheduler_attempt_id": terminal.scheduler_attempt_id,
        "event_ids": [event.event_id for event in events],
        "event_hashes": [event.event_hash for event in events],
    }


def _case_fingerprint(
    *,
    usage: sqlite3.Row,
    slot: sqlite3.Row,
    attempt: sqlite3.Row,
    batch: sqlite3.Row,
    raw_receipts: list[dict[str, Any]],
    outcome: Outcome,
    evidence_ref: str,
    operator_ref: str,
    operator_identity: dict[str, Any],
    subject_identity: dict[str, Any],
    dispatch_evidence: dict[str, Any] | None,
) -> str:
    payload = {
        "contract_version": CONTRACT_VERSION,
        "decision": {
            "outcome": outcome,
            "evidence_ref": evidence_ref,
            "operator_ref": operator_ref,
            "operator_identity": operator_identity,
        },
        "usage": {key: usage[key] for key in usage.keys()},
        "slot": {
            key: slot[key]
            for key in (
                "id", "status", "attempt_count", "last_error_code",
                "last_error_message", "updated_at",
            )
        },
        "attempt": {key: attempt[key] for key in attempt.keys()},
        "budget": {
            key: batch[key]
            for key in (
                "id", "provider", "operation", "currency", "verified_unit_price",
                "consumed_requests", "consumed_amount", "status", "updated_at",
            )
        },
        "raw_receipts": raw_receipts,
        "subject_identity": subject_identity,
        "dispatch_evidence": dispatch_evidence,
    }
    body = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


def _os_operator_identity() -> dict[str, Any]:
    uid = os.getuid()
    return {"uid": uid, "username": pwd.getpwuid(uid).pw_name}


def _load_case(
    connection: sqlite3.Connection,
    *,
    usage_id: int,
    expected_slot_id: int,
    expected_attempt_number: int,
    outcome: Outcome,
    evidence_ref: str,
    operator_ref: str,
    expected_fingerprint: str | None,
) -> dict[str, Any]:
    if usage_id <= 0 or expected_slot_id <= 0 or expected_attempt_number <= 0:
        raise BillingReconciliationError(
            "invalid_reconciliation_identity", "Usage, slot and attempt identities must be positive"
        )
    if outcome not in OUTCOMES:
        raise BillingReconciliationError(
            "invalid_reconciliation_outcome", "Outcome must be billed or unbilled"
        )
    evidence = _reference(evidence_ref, label="evidence_ref")
    operator = _reference(operator_ref, label="operator_ref")
    operator_identity = _os_operator_identity()
    usage = connection.execute(
        "SELECT * FROM provider_usage WHERE id=?", (usage_id,)
    ).fetchone()
    if usage is None or str(usage["provider"]).lower() != "tikhub":
        raise BillingReconciliationError(
            "usage_not_found", "TikHub provider usage does not exist"
        )
    details = _object(usage["details_json"])
    previous = details.get("billing_reconciliation")
    if isinstance(previous, dict) and previous.get("contract_version") == CONTRACT_VERSION:
        expected = {
            "contract_version": CONTRACT_VERSION,
            "outcome": outcome,
            "evidence_ref": evidence,
            "operator_ref": operator,
            "operator_identity": operator_identity,
            "usage_id": usage_id,
            "slot_id": expected_slot_id,
            "attempt_number": expected_attempt_number,
        }
        if all(previous.get(key) == value for key, value in expected.items()):
            return {
                "status": "already_reconciled",
                "usage": usage,
                "details": details,
                "resolution": previous,
            }
        raise BillingReconciliationError(
            "reconciliation_conflict", "Usage was already reconciled with different evidence or outcome"
        )
    if details.get("state") != "billing_unknown":
        raise BillingReconciliationError(
            "usage_not_billing_unknown", "Provider usage is not awaiting billing reconciliation"
        )
    reused_evidence = connection.execute(
        """SELECT id FROM provider_usage
           WHERE id<>? AND json_valid(details_json)
             AND json_extract(details_json,'$.billing_reconciliation.evidence_ref')=?
           LIMIT 1""",
        (usage_id, evidence),
    ).fetchone()
    if reused_evidence is not None:
        raise BillingReconciliationError(
            "reconciliation_evidence_reused",
            "The same billing evidence cannot settle more than one usage row",
        )
    slot_id = details.get("slot_id")
    attempt_number = details.get("attempt_number")
    if type(slot_id) is not int or type(attempt_number) is not int:
        raise BillingReconciliationError(
            "invalid_reconciliation_identity", "Usage has no exact slot/attempt identity"
        )
    if slot_id != expected_slot_id or attempt_number != expected_attempt_number:
        raise BillingReconciliationError(
            "reconciliation_identity_mismatch", "Expected slot/attempt does not match the usage row"
        )
    if (
        int(usage["request_attempts"]) != 1
        or int(usage["billed_requests"]) != 1
        or usage["currency"] != "USD"
        or not usage["budget_batch_id"]
        or micro_usd(usage["amount"]) <= 0
    ):
        raise BillingReconciliationError(
            "usage_ledger_mismatch", "Unknown usage no longer has its original billed reservation"
        )
    slot = connection.execute(
        "SELECT * FROM fetch_slots WHERE id=?", (slot_id,)
    ).fetchone()
    attempt = connection.execute(
        "SELECT * FROM fetch_attempts WHERE slot_id=? AND attempt_number=?",
        (slot_id, attempt_number),
    ).fetchone()
    if (
        slot is None
        or attempt is None
        or int(attempt["billed"]) != 0
        or attempt["amount"] is not None
        or attempt["currency"] != usage["currency"]
    ):
        raise BillingReconciliationError(
            "attempt_ledger_mismatch", "Unknown usage does not match one unresolved fetch attempt"
        )
    if slot["status"] == "running":
        raise BillingReconciliationError(
            "slot_running", "A running slot cannot be reconciled concurrently"
        )
    active = connection.execute(
        """SELECT id FROM provider_usage
           WHERE lower(provider)='tikhub' AND json_valid(details_json)
             AND CAST(json_extract(details_json,'$.slot_id') AS INTEGER)=?
             AND json_extract(details_json,'$.state') IN ('reserved','sent') LIMIT 1""",
        (slot_id,),
    ).fetchone()
    if active is not None:
        raise BillingReconciliationError(
            "slot_has_active_usage", "Slot has another active TikHub reservation or request"
        )
    batch = connection.execute(
        "SELECT * FROM provider_budget_batches WHERE id=?",
        (usage["budget_batch_id"],),
    ).fetchone()
    amount_microusd = micro_usd(usage["amount"])
    if (
        batch is None
        or str(batch["provider"]).lower() != "tikhub"
        or batch["operation"] != usage["operation"]
        or batch["currency"] != usage["currency"]
        or micro_usd(batch["verified_unit_price"]) != amount_microusd
    ):
        raise BillingReconciliationError(
            "budget_batch_mismatch", "Usage does not match its verified budget batch"
        )
    batch_totals = _batch_totals(connection, str(batch["id"]))
    stored_totals = (
        int(batch["consumed_requests"]), micro_usd(batch["consumed_amount"])
    )
    if batch_totals != stored_totals:
        raise BillingReconciliationError(
            "budget_batch_drift", "Budget batch differs from its provider usage ledger"
        )
    raw_rows = connection.execute(
        """SELECT *
           FROM provider_raw_responses
           WHERE fetch_attempt_id=? ORDER BY id""",
        (attempt["id"],),
    ).fetchall()
    if any(row["http_status"] is None for row in raw_rows):
        raise BillingReconciliationError(
            "raw_response_status_unknown",
            "A stored raw response has no HTTP status and requires manual investigation",
        )
    if any(200 <= int(row["http_status"]) < 300 for row in raw_rows):
        raise BillingReconciliationError(
            "successful_raw_response_requires_replay",
            "A successful raw response exists; materialize it before allowing a retry",
        )
    raw_receipts = [_verified_raw_receipt(row, connection=connection) for row in raw_rows]
    subject_identity = _subject_identity(connection, slot)
    dispatch_evidence = _verified_dispatch_evidence(
        connection,
        usage=usage,
        slot_id=slot_id,
        fetch_attempt_id=int(attempt["id"]),
        details=details,
    )
    fingerprint = _case_fingerprint(
        usage=usage,
        slot=slot,
        attempt=attempt,
        batch=batch,
        raw_receipts=raw_receipts,
        outcome=outcome,
        evidence_ref=evidence,
        operator_ref=operator,
        operator_identity=operator_identity,
        subject_identity=subject_identity,
        dispatch_evidence=dispatch_evidence,
    )
    if expected_fingerprint is not None and expected_fingerprint != fingerprint:
        raise BillingReconciliationError(
            "reconciliation_fingerprint_mismatch",
            "The billing case changed after review; export and review it again",
        )
    return {
        "status": "ready",
        "usage": usage,
        "details": details,
        "slot": slot,
        "attempt": attempt,
        "batch": batch,
        "amount_microusd": amount_microusd,
        "raw_receipts": raw_receipts,
        "fingerprint": fingerprint,
        "evidence_ref": evidence,
        "operator_ref": operator,
        "operator_identity": operator_identity,
        "subject_identity": subject_identity,
        "dispatch_evidence": dispatch_evidence,
    }


def preview_unknown_billing(
    connection: sqlite3.Connection,
    *,
    usage_id: int,
    expected_slot_id: int,
    expected_attempt_number: int,
    outcome: Outcome,
    evidence_ref: str,
    operator_ref: str,
    expected_fingerprint: str | None = None,
) -> dict[str, Any]:
    case = _load_case(
        connection,
        usage_id=usage_id,
        expected_slot_id=expected_slot_id,
        expected_attempt_number=expected_attempt_number,
        outcome=outcome,
        evidence_ref=evidence_ref,
        operator_ref=operator_ref,
        expected_fingerprint=expected_fingerprint,
    )
    if case["status"] == "already_reconciled":
        return {
            "contract_version": CONTRACT_VERSION,
            "status": "already_reconciled",
            "applied": False,
            "resolution": case["resolution"],
        }
    usage = case["usage"]
    return {
        "contract_version": CONTRACT_VERSION,
        "status": "ready",
        "applied": False,
        "usage_id": int(usage["id"]),
        "slot_id": expected_slot_id,
        "attempt_number": expected_attempt_number,
        "outcome": outcome,
        "amount_microusd": case["amount_microusd"],
        "raw_receipts": case["raw_receipts"],
        "subject_identity": case["subject_identity"],
        "dispatch_evidence": case["dispatch_evidence"],
        "fingerprint": case["fingerprint"],
    }


def reconcile_unknown_billing(
    *,
    db_path: Path,
    usage_id: int,
    expected_slot_id: int,
    expected_attempt_number: int,
    outcome: Outcome,
    evidence_ref: str,
    operator_ref: str,
    expected_fingerprint: str,
    isolated: bool = False,
) -> dict[str, Any]:
    """Settle exactly one reviewed usage and release only a fully clear slot."""
    with (
        _billing_mutation_access(db_path, isolated=isolated) as access,
        connect(access.database) as connection,
        transaction(connection),
    ):
        case = _load_case(
            connection,
            usage_id=usage_id,
            expected_slot_id=expected_slot_id,
            expected_attempt_number=expected_attempt_number,
            outcome=outcome,
            evidence_ref=evidence_ref,
            operator_ref=operator_ref,
            expected_fingerprint=expected_fingerprint,
        )
        if case["status"] == "already_reconciled":
            return {
                "contract_version": CONTRACT_VERSION,
                "status": "already_reconciled",
                "applied": False,
                "resolution": case["resolution"],
            }
        usage = case["usage"]
        attempt = case["attempt"]
        batch = case["batch"]
        reconciled_at = now_utc()
        parent_cursor = connection.execute(
            """INSERT INTO scheduler_runs(
                   job_id,scheduled_for,status,started_at,completed_at,details_json)
               VALUES (?,?,'succeeded',?,?,?)""",
            (
                "operator_billing_settlement:tikhub",
                f"usage:{usage_id}",
                reconciled_at,
                reconciled_at,
                "{}",
            ),
        )
        if parent_cursor.lastrowid is None:
            raise BillingReconciliationError(
                "receipt_insert_failed", "Billing settlement parent receipt was not created"
            )
        receipt_run_id = int(parent_cursor.lastrowid)
        resolution = {
            "contract_version": CONTRACT_VERSION,
            "outcome": outcome,
            "evidence_ref": case["evidence_ref"],
            "operator_ref": case["operator_ref"],
            "operator_identity": case["operator_identity"],
            "reconciled_at": reconciled_at,
            "usage_id": usage_id,
            "slot_id": expected_slot_id,
            "attempt_number": expected_attempt_number,
            "original_amount_microusd": case["amount_microusd"],
            "raw_receipts": case["raw_receipts"],
            "subject_identity": case["subject_identity"],
            "dispatch_evidence": case["dispatch_evidence"],
            "receipt_run_id": receipt_run_id,
        }
        details = dict(case["details"])
        details.update(
            state="failed",
            billing_reconciliation=resolution,
        )
        amount = float(usage["amount"])
        if outcome == "unbilled":
            batch_cursor = connection.execute(
                """UPDATE provider_budget_batches
                   SET consumed_requests=consumed_requests-1,
                       consumed_amount=ROUND(consumed_amount-?,6),updated_at=?
                   WHERE id=? AND consumed_requests>=1 AND consumed_amount>=?""",
                (amount, reconciled_at, batch["id"], amount),
            )
            if batch_cursor.rowcount != 1:
                raise BillingReconciliationError(
                    "budget_batch_cas_failed", "Budget batch changed during reconciliation"
                )
        usage_cursor = connection.execute(
            """UPDATE provider_usage SET billed_requests=?,amount=?,details_json=?
               WHERE id=? AND request_attempts=1 AND billed_requests=1
                 AND currency=? AND amount=? AND details_json=?""",
            (
                1 if outcome == "billed" else 0,
                amount if outcome == "billed" else 0.0,
                json.dumps(
                    details,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                usage_id,
                usage["currency"],
                amount,
                usage["details_json"],
            ),
        )
        attempt_cursor = connection.execute(
            """UPDATE fetch_attempts SET billed=?,amount=?,currency=?
               WHERE id=? AND billed=0 AND amount IS NULL AND currency=?""",
            (
                1 if outcome == "billed" else 0,
                amount if outcome == "billed" else 0.0,
                usage["currency"],
                attempt["id"],
                usage["currency"],
            ),
        )
        if usage_cursor.rowcount != 1 or attempt_cursor.rowcount != 1:
            raise BillingReconciliationError(
                "reconciliation_cas_failed", "Usage or attempt changed during reconciliation"
            )
        finish_recovery_probe(
            connection,
            usage_id=usage_id,
            succeeded=False,
            at=reconciled_at,
            reason=f"billing_reconciled_{outcome}",
        )
        slot_unfrozen, remaining = clear_billing_unknown_slot_guard_if_resolved(
            connection,
            slot_id=expected_slot_id,
            fallback_error_code="billing_reconciled_retryable",
            fallback_error_message=(
                "TikHub billing reconciled; the slot may follow its normal retry policy"
            ),
        )
        expected_batch = (
            int(batch["consumed_requests"]) - (1 if outcome == "unbilled" else 0),
            micro_usd(batch["consumed_amount"])
            - (case["amount_microusd"] if outcome == "unbilled" else 0),
        )
        refreshed_batch = connection.execute(
            "SELECT consumed_requests,consumed_amount FROM provider_budget_batches WHERE id=?",
            (batch["id"],),
        ).fetchone()
        stored_batch = (
            int(refreshed_batch["consumed_requests"]),
            micro_usd(refreshed_batch["consumed_amount"]),
        )
        recomputed_batch = _batch_totals(connection, str(batch["id"]))
        if stored_batch != expected_batch or recomputed_batch != expected_batch:
            raise BillingReconciliationError(
                "budget_batch_postcondition_failed", "Reconciled budget totals did not close exactly"
            )
        receipt = {
            "contract_version": CONTRACT_VERSION,
            "fingerprint": case["fingerprint"],
            "usage_id": usage_id,
            "slot_id": expected_slot_id,
            "attempt_number": expected_attempt_number,
            "fetch_attempt_id": int(attempt["id"]),
            "budget_batch_id": batch["id"],
            "outcome": outcome,
            "evidence_ref": case["evidence_ref"],
            "operator_ref": case["operator_ref"],
            "operator_identity": case["operator_identity"],
            "reconciled_at": reconciled_at,
            "raw_receipts": case["raw_receipts"],
            "subject_identity": case["subject_identity"],
            "dispatch_evidence": case["dispatch_evidence"],
            "before": {
                "usage_state": case["details"].get("state"),
                "usage_billed_requests": int(usage["billed_requests"]),
                "usage_amount_microusd": case["amount_microusd"],
                "attempt_billed": int(attempt["billed"]),
                "attempt_amount": attempt["amount"],
                "slot_status": case["slot"]["status"],
                "slot_error_code": case["slot"]["last_error_code"],
                "batch_requests": int(batch["consumed_requests"]),
                "batch_amount_microusd": micro_usd(batch["consumed_amount"]),
            },
            "after": {
                "usage_state": "failed",
                "usage_billed_requests": 1 if outcome == "billed" else 0,
                "usage_amount_microusd": (
                    case["amount_microusd"] if outcome == "billed" else 0
                ),
                "attempt_billed": 1 if outcome == "billed" else 0,
                "attempt_amount_microusd": (
                    case["amount_microusd"] if outcome == "billed" else 0
                ),
                "slot_unfrozen": slot_unfrozen,
                "remaining_unknown_for_slot": remaining,
                "batch_requests": stored_batch[0],
                "batch_amount_microusd": stored_batch[1],
            },
        }
        attempt_cursor = connection.execute(
            """INSERT INTO scheduler_run_attempts(
                   scheduler_run_id,attempt_number,invocation_source,status,
                   started_at,completed_at,details_json)
               VALUES (?,1,'operator_retry','succeeded',?,?,?)""",
            (
                receipt_run_id,
                reconciled_at,
                reconciled_at,
                json.dumps(
                    receipt,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            ),
        )
        if attempt_cursor.lastrowid is None:
            raise BillingReconciliationError(
                "receipt_insert_failed", "Immutable billing settlement receipt was not created"
            )
        connection.execute(
            "UPDATE scheduler_runs SET details_json=? WHERE id=?",
            (
                json.dumps(
                    {
                        "contract_version": CONTRACT_VERSION,
                        "usage_id": usage_id,
                        "receipt_attempt_id": int(attempt_cursor.lastrowid),
                    },
                    sort_keys=True,
                ),
                receipt_run_id,
            ),
        )
        return {
            "contract_version": CONTRACT_VERSION,
            "status": "reconciled",
            "applied": True,
            "usage_id": usage_id,
            "slot_id": expected_slot_id,
            "attempt_number": expected_attempt_number,
            "outcome": outcome,
            "slot_unfrozen": slot_unfrozen,
            "remaining_unknown_for_slot": remaining,
            "receipt_run_id": receipt_run_id,
            "receipt_attempt_id": int(attempt_cursor.lastrowid),
            "resolution": resolution,
        }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument(
        "--isolated-db",
        action="store_true",
        help="explicitly authorize a non-installed test/candidate database",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    inventory = commands.add_parser("list", help="read-only unresolved inventory")
    inventory.add_argument("--after-id", type=int, default=0)
    inventory.add_argument("--limit", type=int, default=100)
    settle = commands.add_parser("settle", help="preview or apply one reviewed outcome")
    settle.add_argument("--usage-id", type=int, required=True)
    settle.add_argument("--expected-slot-id", type=int, required=True)
    settle.add_argument("--expected-attempt-number", type=int, required=True)
    settle.add_argument("--outcome", choices=OUTCOMES, required=True)
    settle.add_argument("--evidence-ref", required=True)
    settle.add_argument("--operator-ref", required=True)
    settle.add_argument("--expected-fingerprint")
    settle.add_argument("--apply", action="store_true")
    settle.add_argument("--confirm-tikhub-reviewed", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "list":
            access = (
                resolve_isolated_candidate(args.db)
                if args.isolated_db
                else resolve_installed_database_access(
                    DatabaseAccessMode.FORMAL_READ,
                    database=args.db,
                    project_root=PROJECT_ROOT,
                )
            )
            with open_live_read_only(access.database) as connection:
                result = unknown_billing_inventory(
                    connection, after_id=args.after_id, limit=args.limit
                )
        elif args.apply:
            if not args.confirm_tikhub_reviewed:
                raise BillingReconciliationError(
                    "explicit_confirmation_required",
                    "--apply requires --confirm-tikhub-reviewed",
                )
            if not args.expected_fingerprint:
                raise BillingReconciliationError(
                    "expected_fingerprint_required",
                    "--apply requires the fingerprint returned by a fresh dry-run",
                )
            result = reconcile_unknown_billing(
                db_path=args.db,
                usage_id=args.usage_id,
                expected_slot_id=args.expected_slot_id,
                expected_attempt_number=args.expected_attempt_number,
                outcome=args.outcome,
                evidence_ref=args.evidence_ref,
                operator_ref=args.operator_ref,
                expected_fingerprint=args.expected_fingerprint,
                isolated=args.isolated_db,
            )
        else:
            access = (
                resolve_isolated_candidate(args.db)
                if args.isolated_db
                else resolve_installed_database_access(
                    DatabaseAccessMode.FORMAL_READ,
                    database=args.db,
                    project_root=PROJECT_ROOT,
                )
            )
            with open_live_read_only(access.database) as connection:
                result = preview_unknown_billing(
                    connection,
                    usage_id=args.usage_id,
                    expected_slot_id=args.expected_slot_id,
                    expected_attempt_number=args.expected_attempt_number,
                    outcome=args.outcome,
                    evidence_ref=args.evidence_ref,
                    operator_ref=args.operator_ref,
                    expected_fingerprint=args.expected_fingerprint,
                )
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0
    except (
        BillingReconciliationError,
        RuntimeDatabaseError,
        sqlite3.Error,
        RuntimeError,
    ) as exc:
        print(
            json.dumps(
                {
                    "status": "error",
                    "error_code": getattr(exc, "error_code", type(exc).__name__),
                    "message": str(exc),
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
