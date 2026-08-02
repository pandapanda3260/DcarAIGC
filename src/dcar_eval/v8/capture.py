"""Idempotent capture slots, provider evidence and fail-closed budgets."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from .storage import DEFAULT_DB, PROJECT_ROOT, connect, now_utc, transaction


RAW_ROOT = PROJECT_ROOT / "data" / "cache" / "v8" / "raw_responses"


class CaptureError(RuntimeError):
    """Provider or transport failure with explicit retry and billing semantics."""

    def __init__(
        self,
        message: str,
        *,
        retryable: bool,
        error_code: str,
        http_status: Optional[int] = None,
        billed: bool = False,
        raw_response: Any = None,
    ) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.error_code = error_code
        self.http_status = http_status
        self.billed = billed
        self.raw_response = raw_response


class SlotUnavailable(RuntimeError):
    """The requested idempotency slot is running, terminal or already successful."""


class BudgetBlocked(RuntimeError):
    """The budget is absent, inactive, stale or exhausted."""


@dataclass(frozen=True)
class ProviderResult:
    data: Any
    raw_response: Any
    http_status: int
    billed: bool


@dataclass(frozen=True)
class SlotClaim:
    slot_id: int
    attempt_id: int
    attempt_number: int
    content_id: Optional[int]
    stage: str
    window_key: str
    provider: str
    adapter_version: str
    account_id: Optional[int] = None


@dataclass(frozen=True)
class CaptureOutcome:
    slot_id: int
    attempt_id: int
    raw_response_id: int
    data: Any
    billed: bool
    amount: float
    currency: str


def canonical_json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _scrub_secrets(value: Any) -> Any:
    secret_names = {
        "authorization", "cookie", "set-cookie", "x-api-key", "api-key",
        "api_key", "access_token", "token",
    }
    if isinstance(value, dict):
        return {
            key: "[REDACTED]" if str(key).lower() in secret_names else _scrub_secrets(child)
            for key, child in value.items()
        }
    if isinstance(value, list):
        return [_scrub_secrets(child) for child in value]
    return value


def _atomic_bytes(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_bytes(value)
    os.chmod(temporary, 0o600)
    temporary.replace(path)


def ensure_content_slot(
    connection: sqlite3.Connection,
    *,
    content_id: int,
    stage: str,
    window_key: str,
    provider: str,
    adapter_version: str,
) -> int:
    row = connection.execute(
        """
        SELECT id FROM fetch_slots
        WHERE content_id=? AND stage=? AND window_key=?
        """,
        (content_id, stage, window_key),
    ).fetchone()
    if row is not None:
        return int(row["id"])
    captured_at = now_utc()
    cursor = connection.execute(
        """
        INSERT INTO fetch_slots(
            content_id, stage, window_key, provider, adapter_version,
            status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
        """,
        (content_id, stage, window_key, provider, adapter_version, captured_at, captured_at),
    )
    if cursor.lastrowid is None:
        raise RuntimeError("fetch slot insert returned no id")
    return int(cursor.lastrowid)


def ensure_account_slot(
    connection: sqlite3.Connection,
    *,
    account_id: int,
    stage: str,
    window_key: str,
    provider: str,
    adapter_version: str,
) -> int:
    row = connection.execute(
        """
        SELECT id FROM fetch_slots
        WHERE account_id=? AND stage=? AND window_key=?
        """,
        (account_id, stage, window_key),
    ).fetchone()
    if row is not None:
        return int(row["id"])
    captured_at = now_utc()
    cursor = connection.execute(
        """
        INSERT INTO fetch_slots(
            account_id, stage, window_key, provider, adapter_version,
            status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
        """,
        (account_id, stage, window_key, provider, adapter_version, captured_at, captured_at),
    )
    if cursor.lastrowid is None:
        raise RuntimeError("account fetch slot insert returned no id")
    return int(cursor.lastrowid)


def claim_content_slot(
    *,
    db_path: Path,
    content_id: int,
    stage: str,
    window_key: str,
    provider: str,
    adapter_version: str,
    allow_terminal_retry: bool = False,
) -> SlotClaim:
    with connect(db_path) as connection, transaction(connection):
        slot_id = ensure_content_slot(
            connection,
            content_id=content_id,
            stage=stage,
            window_key=window_key,
            provider=provider,
            adapter_version=adapter_version,
        )
        row = connection.execute("SELECT * FROM fetch_slots WHERE id=?", (slot_id,)).fetchone()
        if row is None:
            raise RuntimeError("fetch slot disappeared")
        allowed = {"pending", "retryable_failed"}
        if allow_terminal_retry:
            allowed.add("terminal_failed")
        if row["status"] not in allowed:
            raise SlotUnavailable(f"slot {slot_id} is {row['status']}")
        attempt_number = int(row["attempt_count"]) + 1
        started_at = now_utc()
        cursor = connection.execute(
            """
            INSERT INTO fetch_attempts(slot_id, attempt_number, request_started_at)
            VALUES (?, ?, ?)
            """,
            (slot_id, attempt_number, started_at),
        )
        if cursor.lastrowid is None:
            raise RuntimeError("fetch attempt insert returned no id")
        connection.execute(
            """
            UPDATE fetch_slots
            SET provider=?, adapter_version=?, status='running', attempt_count=?,
                started_at=?, finished_at=NULL,
                last_error_code=NULL, last_error_message=NULL, updated_at=?
            WHERE id=?
            """,
            (provider, adapter_version, attempt_number, started_at, started_at, slot_id),
        )
    return SlotClaim(
        slot_id=slot_id,
        attempt_id=int(cursor.lastrowid),
        attempt_number=attempt_number,
        content_id=content_id,
        stage=stage,
        window_key=window_key,
        provider=provider,
        adapter_version=adapter_version,
    )


def claim_account_slot(
    *,
    db_path: Path,
    account_id: int,
    stage: str,
    window_key: str,
    provider: str,
    adapter_version: str,
    allow_terminal_retry: bool = False,
) -> SlotClaim:
    with connect(db_path) as connection, transaction(connection):
        slot_id = ensure_account_slot(
            connection,
            account_id=account_id,
            stage=stage,
            window_key=window_key,
            provider=provider,
            adapter_version=adapter_version,
        )
        row = connection.execute("SELECT * FROM fetch_slots WHERE id=?", (slot_id,)).fetchone()
        if row is None:
            raise RuntimeError("account fetch slot disappeared")
        allowed = {"pending", "retryable_failed"}
        if allow_terminal_retry:
            allowed.add("terminal_failed")
        if row["status"] not in allowed:
            raise SlotUnavailable(f"slot {slot_id} is {row['status']}")
        attempt_number = int(row["attempt_count"]) + 1
        started_at = now_utc()
        cursor = connection.execute(
            """
            INSERT INTO fetch_attempts(slot_id, attempt_number, request_started_at)
            VALUES (?, ?, ?)
            """,
            (slot_id, attempt_number, started_at),
        )
        if cursor.lastrowid is None:
            raise RuntimeError("account fetch attempt insert returned no id")
        connection.execute(
            """
            UPDATE fetch_slots
            SET provider=?, adapter_version=?, status='running', attempt_count=?,
                started_at=?, finished_at=NULL,
                last_error_code=NULL, last_error_message=NULL, updated_at=?
            WHERE id=?
            """,
            (provider, adapter_version, attempt_number, started_at, started_at, slot_id),
        )
    return SlotClaim(
        slot_id=slot_id,
        attempt_id=int(cursor.lastrowid),
        attempt_number=attempt_number,
        content_id=None,
        stage=stage,
        window_key=window_key,
        provider=provider,
        adapter_version=adapter_version,
        account_id=account_id,
    )


def activate_pilot_budget(
    budget_id: str,
    *,
    expected_unit_price: float,
    db_path: Path = DEFAULT_DB,
) -> None:
    with connect(db_path) as connection, transaction(connection):
        row = connection.execute(
            "SELECT * FROM provider_budget_batches WHERE id=?", (budget_id,)
        ).fetchone()
        if row is None:
            raise BudgetBlocked(f"budget {budget_id} does not exist")
        if row["status"] != "draft":
            raise BudgetBlocked(f"budget {budget_id} is {row['status']}, expected draft")
        if abs(float(row["verified_unit_price"]) - expected_unit_price) > 1e-9:
            raise BudgetBlocked("verified provider price does not match the approved price")
        if not row["price_verified_at"]:
            raise BudgetBlocked("provider price has not been verified")
        connection.execute(
            "UPDATE provider_budget_batches SET status='pilot', updated_at=? WHERE id=?",
            (now_utc(), budget_id),
        )


def _reserve_budget(
    connection: sqlite3.Connection,
    *,
    budget_id: str,
    provider: str,
    operation: str,
) -> tuple[int, float, str]:
    row = connection.execute(
        "SELECT * FROM provider_budget_batches WHERE id=?", (budget_id,)
    ).fetchone()
    if row is None:
        raise BudgetBlocked(f"budget {budget_id} does not exist")
    if row["status"] not in {"pilot", "approved"}:
        raise BudgetBlocked(f"budget {budget_id} is {row['status']}")
    if row["provider"] != provider or row["operation"] != operation:
        raise BudgetBlocked("budget provider or operation mismatch")
    unit_price = float(row["verified_unit_price"])
    consumed_requests = int(row["consumed_requests"])
    consumed_amount = float(row["consumed_amount"])
    if consumed_requests >= int(row["max_billable_requests"]):
        raise BudgetBlocked("billable request ceiling reached")
    if consumed_amount + unit_price > float(row["max_amount"]) + 1e-9:
        raise BudgetBlocked("amount ceiling reached")
    today = now_utc()[:10]
    usage = connection.execute(
        """
        SELECT COALESCE(SUM(request_attempts), 0) attempts
        FROM provider_usage
        WHERE budget_batch_id=? AND substr(recorded_at, 1, 10)=?
        """,
        (budget_id, today),
    ).fetchone()
    daily_attempts = int(usage["attempts"])
    if daily_attempts >= int(row["daily_quota"]):
        raise BudgetBlocked("daily attempt quota reached")
    total_attempts = int(
        connection.execute(
            "SELECT COALESCE(SUM(request_attempts), 0) FROM provider_usage WHERE budget_batch_id=?",
            (budget_id,),
        ).fetchone()[0]
    )
    if row["status"] == "pilot" and total_attempts >= int(row["pilot_size"]):
        raise BudgetBlocked("pilot sample is complete and awaits quality gate")

    recorded_at = now_utc()
    cursor = connection.execute(
        """
        INSERT INTO provider_usage(
            budget_batch_id, provider, operation, request_attempts, billed_requests,
            currency, amount, recorded_at, details_json
        ) VALUES (?, ?, ?, 1, 1, ?, ?, ?, '{"state":"reserved"}')
        """,
        (budget_id, provider, operation, row["currency"], unit_price, recorded_at),
    )
    if cursor.lastrowid is None:
        raise RuntimeError("provider usage insert returned no id")
    new_attempts = total_attempts + 1
    next_status = "suspended" if row["status"] == "pilot" and new_attempts >= int(row["pilot_size"]) else row["status"]
    connection.execute(
        """
        UPDATE provider_budget_batches
        SET consumed_requests=consumed_requests+1,
            consumed_amount=ROUND(consumed_amount+?, 6), status=?, updated_at=?
        WHERE id=?
        """,
        (unit_price, next_status, recorded_at, budget_id),
    )
    return int(cursor.lastrowid), unit_price, str(row["currency"])


def _settle_budget(
    connection: sqlite3.Connection,
    *,
    usage_id: int,
    budget_id: str,
    unit_price: float,
    billed: bool,
    details: Dict[str, Any],
) -> None:
    details_json = json.dumps(details, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    if billed:
        connection.execute(
            "UPDATE provider_usage SET details_json=? WHERE id=?",
            (details_json, usage_id),
        )
        return
    connection.execute(
        """
        UPDATE provider_usage
        SET billed_requests=0, amount=0, details_json=? WHERE id=?
        """,
        (details_json, usage_id),
    )
    connection.execute(
        """
        UPDATE provider_budget_batches
        SET consumed_requests=MAX(0, consumed_requests-1),
            consumed_amount=MAX(0, ROUND(consumed_amount-?, 6)), updated_at=?
        WHERE id=?
        """,
        (unit_price, now_utc(), budget_id),
    )


def _store_raw_response(
    connection: sqlite3.Connection,
    *,
    claim: SlotClaim,
    operation: str,
    value: Any,
    http_status: Optional[int],
    raw_root: Path,
) -> int:
    captured_at = now_utc()
    body = canonical_json_bytes(_scrub_secrets(value))
    digest = hashlib.sha256(body).hexdigest()
    target = str(claim.content_id) if claim.content_id is not None else f"account-{claim.account_id}"
    path = (
        raw_root / claim.provider.lower() / target / operation
        / f"attempt-{claim.attempt_number:03d}-{digest[:12]}.json"
    )
    _atomic_bytes(path, body)
    try:
        local_path = str(path.relative_to(PROJECT_ROOT))
    except ValueError:
        local_path = str(path)
    cursor = connection.execute(
        """
        INSERT INTO provider_raw_responses(
            fetch_attempt_id, account_id, content_id, provider, operation, local_path,
            sha256, byte_size, http_status, captured_at, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'live')
        """,
        (
            claim.attempt_id, claim.account_id, claim.content_id, claim.provider, operation,
            local_path, digest, len(body), http_status, captured_at,
        ),
    )
    if cursor.lastrowid is None:
        raise RuntimeError("raw response insert returned no id")
    return int(cursor.lastrowid)


def _execute_claimed_fetch(
    *,
    claim: SlotClaim,
    operation: str,
    call: Callable[[], ProviderResult],
    db_path: Path = DEFAULT_DB,
    raw_root: Path = RAW_ROOT,
    budget_id: Optional[str] = None,
) -> CaptureOutcome:
    provider = claim.provider
    usage_id: Optional[int] = None
    unit_price = 0.0
    currency = ""
    if budget_id is not None:
        try:
            with connect(db_path) as connection, transaction(connection):
                usage_id, unit_price, currency = _reserve_budget(
                    connection,
                    budget_id=budget_id,
                    provider=provider,
                    operation=operation,
                )
        except Exception as exc:
            with connect(db_path) as connection, transaction(connection):
                finished_at = now_utc()
                connection.execute(
                    """
                    UPDATE fetch_attempts
                    SET response_finished_at=?, error_code='budget_blocked', error_message=?
                    WHERE id=?
                    """,
                    (finished_at, str(exc)[:500], claim.attempt_id),
                )
                connection.execute(
                    """
                    UPDATE fetch_slots
                    SET status='retryable_failed', last_error_code='budget_blocked',
                        last_error_message=?, finished_at=?, updated_at=? WHERE id=?
                    """,
                    (str(exc)[:500], finished_at, finished_at, claim.slot_id),
                )
            raise

    try:
        result = call()
        with connect(db_path) as connection, transaction(connection):
            raw_id = _store_raw_response(
                connection,
                claim=claim,
                operation=operation,
                value=result.raw_response,
                http_status=result.http_status,
                raw_root=raw_root,
            )
            if budget_id is not None and usage_id is not None:
                _settle_budget(
                    connection,
                    usage_id=usage_id,
                    budget_id=budget_id,
                    unit_price=unit_price,
                    billed=result.billed,
                    details={"state": "completed", "slot_id": claim.slot_id, "http_status": result.http_status},
                )
            finished_at = now_utc()
            connection.execute(
                """
                UPDATE fetch_attempts
                SET response_finished_at=?, http_status=?, billed=?, amount=?, currency=?
                WHERE id=?
                """,
                (
                    finished_at, result.http_status, int(result.billed),
                    unit_price if result.billed else 0.0, currency, claim.attempt_id,
                ),
            )
            connection.execute(
                """
                UPDATE fetch_slots
                SET status='succeeded', finished_at=?, updated_at=? WHERE id=?
                """,
                (finished_at, finished_at, claim.slot_id),
            )
        return CaptureOutcome(
            slot_id=claim.slot_id,
            attempt_id=claim.attempt_id,
            raw_response_id=raw_id,
            data=result.data,
            billed=result.billed,
            amount=unit_price if result.billed else 0.0,
            currency=currency,
        )
    except Exception as exc:
        failure = exc if isinstance(exc, CaptureError) else CaptureError(
            f"{type(exc).__name__}: {exc}",
            retryable=True,
            error_code="unhandled_adapter_error",
        )
        with connect(db_path) as connection, transaction(connection):
            if failure.raw_response is not None:
                _store_raw_response(
                    connection,
                    claim=claim,
                    operation=operation,
                    value=failure.raw_response,
                    http_status=failure.http_status,
                    raw_root=raw_root,
                )
            if budget_id is not None and usage_id is not None:
                _settle_budget(
                    connection,
                    usage_id=usage_id,
                    budget_id=budget_id,
                    unit_price=unit_price,
                    billed=failure.billed,
                    details={"state": "failed", "slot_id": claim.slot_id, "error_code": failure.error_code},
                )
            finished_at = now_utc()
            next_status = "retryable_failed" if failure.retryable else "terminal_failed"
            connection.execute(
                """
                UPDATE fetch_attempts
                SET response_finished_at=?, http_status=?, billed=?, amount=?, currency=?,
                    error_code=?, error_message=? WHERE id=?
                """,
                (
                    finished_at, failure.http_status, int(failure.billed),
                    unit_price if failure.billed else 0.0, currency,
                    failure.error_code, str(failure)[:500], claim.attempt_id,
                ),
            )
            connection.execute(
                """
                UPDATE fetch_slots
                SET status=?, last_error_code=?, last_error_message=?,
                    finished_at=?, updated_at=? WHERE id=?
                """,
                (
                    next_status, failure.error_code, str(failure)[:500],
                    finished_at, finished_at, claim.slot_id,
                ),
            )
        raise failure from exc


def execute_content_fetch(
    *,
    content_id: int,
    stage: str,
    window_key: str,
    provider: str,
    adapter_version: str,
    operation: str,
    call: Callable[[], ProviderResult],
    db_path: Path = DEFAULT_DB,
    raw_root: Path = RAW_ROOT,
    budget_id: Optional[str] = None,
    allow_terminal_retry: bool = False,
) -> CaptureOutcome:
    claim = claim_content_slot(
        db_path=db_path,
        content_id=content_id,
        stage=stage,
        window_key=window_key,
        provider=provider,
        adapter_version=adapter_version,
        allow_terminal_retry=allow_terminal_retry,
    )
    return _execute_claimed_fetch(
        claim=claim,
        operation=operation,
        call=call,
        db_path=db_path,
        raw_root=raw_root,
        budget_id=budget_id,
    )


def execute_account_fetch(
    *,
    account_id: int,
    stage: str,
    window_key: str,
    provider: str,
    adapter_version: str,
    operation: str,
    call: Callable[[], ProviderResult],
    db_path: Path = DEFAULT_DB,
    raw_root: Path = RAW_ROOT,
    budget_id: Optional[str] = None,
    allow_terminal_retry: bool = False,
) -> CaptureOutcome:
    claim = claim_account_slot(
        db_path=db_path,
        account_id=account_id,
        stage=stage,
        window_key=window_key,
        provider=provider,
        adapter_version=adapter_version,
        allow_terminal_retry=allow_terminal_retry,
    )
    return _execute_claimed_fetch(
        claim=claim,
        operation=operation,
        call=call,
        db_path=db_path,
        raw_root=raw_root,
        budget_id=budget_id,
    )


def evaluate_pilot_gate(
    budget_id: str,
    *,
    attempted: int,
    media_recovered: int,
    evidence_ready: int,
    db_path: Path = DEFAULT_DB,
) -> Dict[str, Any]:
    if attempted <= 0:
        raise ValueError("attempted must be positive")
    media_rate = media_recovered / attempted
    evidence_rate = evidence_ready / attempted
    approved = media_rate >= 0.70 and evidence_rate >= 0.60
    with connect(db_path) as connection, transaction(connection):
        row = connection.execute(
            "SELECT status FROM provider_budget_batches WHERE id=?", (budget_id,)
        ).fetchone()
        if row is None:
            raise BudgetBlocked(f"budget {budget_id} does not exist")
        if row["status"] != "suspended":
            raise BudgetBlocked("pilot budget must be suspended before evaluation")
        connection.execute(
            "UPDATE provider_budget_batches SET status=?, updated_at=? WHERE id=?",
            ("approved" if approved else "suspended", now_utc(), budget_id),
        )
    return {
        "attempted": attempted,
        "media_recovered": media_recovered,
        "evidence_ready": evidence_ready,
        "media_recovery_rate": round(media_rate, 4),
        "evidence_ready_rate": round(evidence_rate, 4),
        "approved": approved,
    }
