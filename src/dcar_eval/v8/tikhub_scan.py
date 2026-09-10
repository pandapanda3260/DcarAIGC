"""Frozen, fenced TikHub account scans; directory discovery is not detail capture.

The successful provider response is persisted by capture before page application.
Application, its four-way disposition receipt and the next cursor share one caller
transaction.  A crash between those steps replays the same raw response for free.
"""

from __future__ import annotations

from . import durable_runs

import copy
import hashlib
import json
import math
import os
import sqlite3
import time as monotonic_time
from contextvars import ContextVar
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo

from . import providers
from .account_roster import RosterError, require_active_member
from .capture import (
    RAW_ROOT,
    BudgetBlocked,
    CaptureError,
    ProviderResult,
    RawResponseIntegrityError,
    SlotUnavailable,
    StoredRawResponse,
    execute_account_fetch,
    load_succeeded_raw_response,
)
from .durable_runs import (
    DurableClaim,
    DurableRunError,
    LostOwnership,
    assert_owner,
    checkpoint,
    claim_run,
    claim_run_in_transaction,
    finish_run,
    get_run,
    recover_run,
    scan_identity,
)
from .metric_observations import persist_metric_observation
from .migration import normalize_timestamp
from .operations import OperationError, content_identity, upsert_content
from .provider_budget import DEFAULT_TASK_MAX_AMOUNT_USD, micro_usd, paid_scope
from .scan_terminals import (
    TRANSIENT_RETRY_LIMIT,
    classify_error,
    terminal_summary,
)
from .source_routing import METRIC_FIELDS, parse_time
from .storage import DEFAULT_DB, connect, now_utc, transaction

CONTRACT_VERSION = "tikhub-account-scan-v2"
LEGACY_CONTRACT_VERSION = "tikhub-account-scan-v1"
MATERIALIZATION_CONTRACT_VERSION = "tikhub-scan-materialization-v1"
MATERIALIZATION_PROGRESS_CONTRACT_VERSION = "tikhub-scan-materialization-progress-v1"
MATERIALIZATION_JOB = "tikhub_scan_materialize"
MATERIALIZATION_ITEM_LIMIT = 50
MATERIALIZATION_DEADLINE_SECONDS = 60.0
SUPPORTED_CONTRACT_VERSIONS = {LEGACY_CONTRACT_VERSION, CONTRACT_VERSION}
PAGE_LIMIT = 20
SHANGHAI = ZoneInfo("Asia/Shanghai")
DISPOSITIONS = ("existing", "inserted", "quarantined", "unparseable")
PageCall = Callable[[str, Mapping[str, Any]], ProviderResult]
_LOCAL_REPLAY: ContextVar[tuple[str, int, str] | None] = ContextVar("tikhub_local_replay", default=None)


class TikHubScanError(RuntimeError):
    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def _digest(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _timestamp(value: str) -> str:
    try:
        return parse_time(value).isoformat().replace("+00:00", "Z")
    except (TypeError, ValueError, AttributeError) as error:
        raise TikHubScanError("invalid_window", "Scan times must include a timezone") from error


def _job_id(purpose: str) -> str:
    if purpose not in {"reconcile", "history"}:
        raise TikHubScanError("invalid_purpose", "Account scans require reconcile or history purpose")
    return "tikhub_reconcile" if purpose == "reconcile" else "history_recovery"


def _cursor(platform: str, value: Any, *, initial: bool = False) -> int | str:
    if platform == "douyin":
        if type(value) is int and value >= 0:
            return value
        if isinstance(value, str) and value.isdecimal():
            return int(value)
    elif isinstance(value, str) and (value.strip() or initial):
        return value
    raise TikHubScanError("invalid_cursor", "A nonempty, typed provider cursor is required")


def _initial_cursor(platform: str) -> int | str:
    return 0 if platform == "douyin" else ""


def _freeze(
    identity_id: int, *, window_start: str, window_end: str, purpose: str,
    roster_snapshot_id: int, roster_snapshot_hash: str, db_path: Path,
    task_id: str | None, task_max_amount: float | None,
    activation_id: int | None = None, profile_id: str | None = None,
    activation_sha256: str | None = None,
    at: str | None = None,
) -> dict[str, Any]:
    _job_id(purpose)
    start, end = _timestamp(window_start), _timestamp(window_end)
    if parse_time(start) >= parse_time(end):
        raise TikHubScanError("invalid_window", "The frozen half-open window must be nonempty")
    if (type(identity_id) is not int or type(roster_snapshot_id) is not int
            or identity_id <= 0 or roster_snapshot_id <= 0):
        raise TikHubScanError("invalid_identity", "Identity and roster IDs must be positive")
    try:
        amount = micro_usd(
            DEFAULT_TASK_MAX_AMOUNT_USD
            if task_max_amount is None else task_max_amount
        )
    except BudgetBlocked as error:
        raise TikHubScanError("invalid_task_budget", str(error)) from error
    if amount <= 0 or (task_id is not None and (not isinstance(task_id, str) or not task_id.strip())):
        raise TikHubScanError("invalid_task_budget", "A positive budget and task ID are required")
    with connect(db_path) as connection:
        active = None
        if connection.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type='table' AND name='acquisition_profile_activations'"
        ).fetchone() is not None:
            from .profile_activations import activation_at, activation_by_id

            effective = activation_at(connection, at or now_utc())
            if effective is None:
                raise RosterError(
                    "roster_activation_required",
                    "TikHub account scans require an effective acquisition activation",
                )
            supplied_epoch = (
                activation_id is not None,
                profile_id is not None,
                activation_sha256 is not None,
            )
            if any(supplied_epoch) and not all(supplied_epoch):
                raise RosterError(
                    "roster_activation_required",
                    "TikHub account scans must bind the complete acquisition epoch",
                )
            active = (
                effective
                if activation_id is None
                else activation_by_id(connection, activation_id)
            )
            if (
                active.get("cancellation") is not None
                or int(active["activation_id"]) != int(effective["activation_id"])
            ):
                raise RosterError(
                    "profile_superseded", "TikHub account scan activation is no longer effective"
                )
            if profile_id is not None and profile_id != active["profile_id"]:
                raise RosterError(
                    "roster_evidence_mismatch",
                    "TikHub account scan profile differs from its activation",
                )
            if (
                activation_sha256 is not None
                and activation_sha256 != active["activation_sha256"]
            ):
                raise RosterError(
                    "roster_evidence_mismatch",
                    "TikHub account scan activation digest is invalid",
                )
            if (
                int(active["roster_snapshot_id"]) != roster_snapshot_id
                or str(active["roster_members_sha256"]) != roster_snapshot_hash
            ):
                raise RosterError(
                    "roster_evidence_mismatch",
                    "TikHub account scan roster differs from its activation",
                )
        try:
            member = require_active_member(
                connection,
                identity_id,
                roster_snapshot_id,
                roster_snapshot_hash,
                activation=active,
            )
        except RosterError as error:
            if error.code != "member_scope_changed":
                raise
            disabled = connection.execute(
                "SELECT m.*,i.uid,i.account_id,a.enabled "
                "FROM account_roster_members m "
                "JOIN account_platform_identities i ON i.id=m.account_identity_id "
                "JOIN accounts a ON a.id=i.account_id "
                "WHERE m.snapshot_id=? AND m.account_identity_id=?",
                (roster_snapshot_id, identity_id),
            ).fetchone()
            if disabled is None or bool(disabled["enabled"]):
                raise
            member = dict(disabled)
    if member["platform"] not in {"douyin", "xiaohongshu"}:
        raise TikHubScanError("unsupported_platform", "Only the two fixed TikHub list routes are enabled")
    result = {
        "contract_version": CONTRACT_VERSION, "provider": "TikHub", "purpose": purpose,
        "identity_id": identity_id, "account_id": int(member["account_id"]),
        "platform": str(member["platform"]), "uid": str(member["uid"]),
        "roster_snapshot_id": roster_snapshot_id, "roster_snapshot_hash": roster_snapshot_hash,
        "window_start": start, "window_end": end,
        "task_id": task_id, "task_max_microusd": amount,
    }
    if active is not None:
        result.update(
            activation_id=int(active["activation_id"]),
            profile_id=str(active["profile_id"]),
            activation_sha256=str(active["activation_sha256"]),
        )
    return result


def _owned_scope(
    connection: sqlite3.Connection, claim: DurableClaim, scope: Mapping[str, Any],
) -> dict[str, Any]:
    details = assert_owner(connection, claim)
    if details["identity"] != scope:
        raise LostOwnership("Frozen scan scope changed")
    database = next(row[2] for row in connection.execute("PRAGMA database_list") if row[1] == "main")
    local_replay = _LOCAL_REPLAY.get() == (
        str(Path(database).resolve()), claim.scheduler_run_id, _digest(scope),
    )
    if "source_family" in {
        column["name"]
        for column in connection.execute(
            "PRAGMA table_info(account_roster_snapshots)"
        )
    }:
        from .profile_activations import activation_at, activation_by_id

        active = activation_at(connection, now_utc())
        activation_id = scope.get("activation_id")
        profile_id = scope.get("profile_id")
        activation_sha256 = scope.get("activation_sha256")
        if local_replay:
            # Already captured raw is historical data, not permission to send.
            # Validate its frozen epoch if present, without requiring it to be
            # today's active epoch. Pre-schema19 pages have no epoch fields.
            fields = ("activation_id", "profile_id", "activation_sha256")
            if any(field in scope for field in fields):
                if type(activation_id) is not int or not isinstance(profile_id, str) or not isinstance(activation_sha256, str):
                    raise RosterError("profile_superseded", "Historical materialization epoch is malformed")
                historical = activation_by_id(connection, activation_id)
                if (
                    historical["profile_id"] != profile_id
                    or historical["activation_sha256"] != activation_sha256
                    or historical["roster_snapshot_id"] != scope["roster_snapshot_id"]
                    or historical["roster_members_sha256"] != scope["roster_snapshot_hash"]
                ):
                    raise RosterError("member_scope_changed", "Historical materialization epoch changed")
            frozen = None
        else:
            if (
                type(activation_id) is not int
                or not isinstance(profile_id, str)
                or not isinstance(activation_sha256, str)
            ):
                raise RosterError(
                    "profile_superseded", "Frozen TikHub account scan has no acquisition epoch"
                )
            frozen = activation_by_id(connection, activation_id)
            if (
                active is None
                or frozen.get("cancellation") is not None
                or int(active["activation_id"]) != activation_id
                or frozen["profile_id"] != profile_id
                or active["profile_id"] != profile_id
                or frozen["activation_sha256"] != activation_sha256
            ):
                raise RosterError(
                    "profile_superseded", "Frozen TikHub account scan activation was replaced"
                )
            if (
                active["roster_snapshot_id"] != int(scope["roster_snapshot_id"])
                or active["roster_members_sha256"] != str(scope["roster_snapshot_hash"])
            ):
                raise RosterError(
                    "member_scope_changed",
                    "Frozen scan roster is no longer the effective acquisition scope",
                )
    else:
        frozen = None
    disabled = connection.execute(
        "SELECT a.enabled FROM account_roster_members m "
        "JOIN account_platform_identities i ON i.id=m.account_identity_id "
        "JOIN accounts a ON a.id=i.account_id "
        "WHERE m.snapshot_id=? AND m.account_identity_id=?",
        (int(scope["roster_snapshot_id"]), int(scope["identity_id"])),
    ).fetchone()
    if disabled is not None and not bool(disabled["enabled"]):
        raise RosterError(
            "operator_paused", "The frozen account was disabled by an operator"
        )
    member = require_active_member(
        connection, int(scope["identity_id"]), int(scope["roster_snapshot_id"]),
        str(scope["roster_snapshot_hash"]), activation=frozen,
    )
    if any(str(member[key]) != str(scope[key]) for key in ("platform", "account_id", "uid")):
        raise TikHubScanError("identity_conflict", "The frozen platform identity changed")
    return copy.deepcopy(details["checkpoint"])


def _state(claim: DurableClaim, scope: Mapping[str, Any], db_path: Path) -> dict[str, Any]:
    with connect(db_path) as connection, transaction(connection):
        state = _owned_scope(connection, claim, scope)
    head = state.get("last_manifest")
    if head:
        try:
            path = Path(head["path"])
            if path.is_symlink():
                raise ValueError("Manifest is a symlink")
            body = path.read_bytes()
            if len(body) != head["byte_size"] or hashlib.sha256(body).hexdigest() != head["sha256"]:
                raise ValueError("Manifest bytes changed")
            value = json.loads(body)
            if (value["scan_id"] != claim.scan_id or value["scope"] != scope
                    or value["contract_version"] != scope["contract_version"]):
                raise ValueError("Manifest scope changed")
        except (OSError, ValueError, KeyError, TypeError) as error:
            raise TikHubScanError("manifest_integrity_error", "The committed page receipt is not intact") from error
    return state


def _page_key(claim: DurableClaim, state: Mapping[str, Any]) -> str:
    return f"scan:{claim.scan_id}:g{state['generation']}:p{state['page_number']}:{_digest(state['cursor'])}"


def _provider_call(operation: str, request: Mapping[str, Any], override: PageCall | None) -> ProviderResult:
    if override is not None:
        return override(operation, request)
    key = providers._load_key(providers.TIKHUB_KEY_FILE, "TIKHUB_API_KEY")
    try:
        if operation == "douyin_uid_profile":
            return providers._douyin_reference_call(str(request["uid"]), key)
        if operation == "douyin_user_posts":
            return providers._douyin_discovery_call(str(request["reference"]), key, request["cursor"])
        if operation == "xiaohongshu_user_posts":
            return providers._xhs_discovery_call(str(request["uid"]), key, request["cursor"])
        raise TikHubScanError("unsupported_operation", "No fixed list route for this operation")
    except CaptureError as error:
        # Invalid HTTP-success shapes remain raw replay candidates, not an
        # invitation to pay for the same malformed response on every resume.
        if (
            error.error_code == "invalid_response"
            and error.http_status == 200
            and error.raw_response is not None
        ):
            return ProviderResult(
                {},
                error.raw_response,
                200,
                error.billed is True,
                entity_bytes=error.entity_bytes,
                transport_receipt=error.transport_receipt,
            )
        raise


def _expired_cursor(error: CaptureError) -> bool:
    if error.error_code in {"cursor_expired", "invalid_cursor"}:
        return True
    if error.error_code not in {"http_400", "upstream_invalid_request", "upstream_error", "semantic_error"}:
        return False
    body = error.raw_response
    if not isinstance(body, dict):
        return False
    containers = [body, body.get("data"), body.get("error")]
    for item in containers:
        if not isinstance(item, dict):
            continue
        for name in ("message", "msg", "message_zh", "status_msg"):
            message = str(item.get(name) or "").lower()
            if ("cursor" in message or "游标" in message) and any(
                word in message for word in ("expired", "invalid", "过期", "失效", "无效")
            ):
                return True
    return False


def _raw(
    claim: DurableClaim, scope: Mapping[str, Any], *, window_key: str,
    operation: str, request: Mapping[str, Any], db_path: Path,
    raw_root: Path | None, call_override: PageCall | None,
) -> tuple[StoredRawResponse, bool]:
    if _LOCAL_REPLAY.get() is not None:
        raise TikHubScanError("local_materialization_network_forbidden", "Local raw replay cannot enter provider discovery")
    try:
        return load_succeeded_raw_response(
            stage="discovery", window_key=window_key, account_id=int(scope["account_id"]),
            operation=operation, db_path=db_path,
        ), True
    except SlotUnavailable:
        pass
    adapter = (
        "tikhub-frozen-reference-v1"
        if operation == "douyin_uid_profile"
        else str(scope["contract_version"])
    )
    price = providers.TIKHUB_PRICE if scope["platform"] == "douyin" else providers.TIKHUB_XHS_PRICE
    task_id = str(scope["task_id"] or f"tikhub-scan-{claim.scan_id}")
    task_max_amount = int(scope["task_max_microusd"]) / 1_000_000
    with connect(db_path) as connection:
        slot = connection.execute(
            "SELECT attempt_count,last_error_code FROM fetch_slots "
            "WHERE account_id=? AND stage='discovery' AND window_key=?",
            (scope["account_id"], window_key),
        ).fetchone()
    prior_attempts = int(slot["attempt_count"]) if slot is not None else 0
    # A paid scope may cross the network boundary only once at sequence 0.
    # Provider retry hints and explicit unbilled responses do not authorize a
    # second purchase; a later compensation sequence must be issued by the
    # settlement workflow instead of this scanner.
    allowed_attempts = min(1, max(0, TRANSIENT_RETRY_LIMIT - prior_attempts))
    if allowed_attempts == 0:
        exhausted = CaptureError(
            "TikHub cursor exhausted its provider retry allowance",
            retryable=False,
            error_code=str(slot["last_error_code"] or "provider_retry_requested"),
        )
        exhausted.scan_attempt_count = prior_attempts
        raise exhausted
    for attempt in range(allowed_attempts):
        with connect(db_path) as connection, transaction(connection):
            _owned_scope(connection, claim, scope)
        try:
            with paid_scope(
                str(scope["purpose"]), activation_id=scope.get("activation_id"),
                roster_snapshot_id=int(scope["roster_snapshot_id"]),
                roster_snapshot_hash=str(scope["roster_snapshot_hash"]),
                scheduler_run_id=claim.scheduler_run_id, scheduler_attempt_id=claim.attempt_id,
                business_day=parse_time(str(scope["window_end"])).astimezone(SHANGHAI).date().isoformat(),
            ):
                budget_id = providers._budget_for_call(
                    provider="TikHub", operation=operation, price=price, task_id=task_id,
                    task_max_amount=task_max_amount, db_path=db_path,
                )
                request_params: dict[str, Any]
                if operation == "douyin_uid_profile":
                    request_params = {"uid": str(request["uid"])}
                    subject = str(request["uid"])
                    request_cursor: Any = None
                elif operation == "douyin_user_posts":
                    request_params = {
                        "sec_user_id": str(request["reference"]),
                        "max_cursor": request["cursor"] or 0,
                        "count": 20,
                        "sort_type": 0,
                    }
                    subject = str(request["reference"])
                    request_cursor = request["cursor"]
                else:
                    request_params = {
                        "user_id": str(request["uid"]),
                        "cursor": request["cursor"] or "",
                    }
                    subject = str(request["uid"])
                    request_cursor = request["cursor"]
                execute_account_fetch(
                    request_transport=providers._freeze_tikhub_transport(call_override),
                    account_id=int(scope["account_id"]), stage="discovery", window_key=window_key,
                    provider="TikHub", adapter_version=adapter, operation=operation,
                    call=lambda: _provider_call(operation, request, call_override),
                    db_path=db_path,
                    raw_root=raw_root,
                    budget_id=budget_id,
                    task_id=task_id,
                    task_max_amount=task_max_amount,
                    paid_request_identity=providers._paid_request_identity(
                        operation=operation,
                        platform=str(scope["platform"]),
                        subject=subject,
                        params=request_params,
                        cursor=request_cursor,
                        due_bucket=window_key,
                        request_window={
                            "start": str(scope["window_start"]),
                            "end": str(scope["window_end"]),
                        },
                    ),
                )
            break
        except CaptureError as error:
            with connect(db_path) as connection:
                current = connection.execute(
                    "SELECT attempt_count FROM fetch_slots "
                    "WHERE account_id=? AND stage='discovery' AND window_key=?",
                    (scope["account_id"], window_key),
                ).fetchone()
            error.scan_attempt_count = int(current[0]) if current is not None else prior_attempts + attempt + 1
            if "cursor" in request and _expired_cursor(error):
                expired = CaptureError(
                    "TikHub cursor expired", retryable=False, error_code="cursor_expired",
                    http_status=error.http_status, billed=error.billed,
                    raw_response=error.raw_response,
                    entity_bytes=error.entity_bytes,
                    transport_receipt=error.transport_receipt,
                    transport_partial=error.transport_partial,
                )
                expired.scan_attempt_count = error.scan_attempt_count
                raise expired from error
            transient = error.error_code in {"transport_error", "provider_retry_requested"} or (
                error.http_status is not None and (error.http_status in {408, 429} or error.http_status >= 500)
            )
            if (
                error.billed is not False
                or not error.retryable
                or not transient
                or error.retry_after_seconds
                or attempt == allowed_attempts - 1
            ):
                raise
    return load_succeeded_raw_response(
        stage="discovery", window_key=window_key, account_id=int(scope["account_id"]),
        operation=operation, db_path=db_path,
    ), False


def _raw_receipt(raw: StoredRawResponse, *, window_key: str, kind: str) -> dict[str, Any]:
    return {
        "kind": kind, "window_key": window_key, "raw_response_id": raw.raw_response_id,
        "slot_id": raw.slot_id, "sha256": raw.sha256, "captured_at": raw.captured_at,
    }


def _record_received(
    claim: DurableClaim, scope: Mapping[str, Any], receipt: Mapping[str, Any],
    *, db_path: Path, now: str,
) -> None:
    with connect(db_path) as connection, transaction(connection):
        state = _owned_scope(connection, claim, scope)
        if state.get("pending_raw") not in (None, receipt):
            raise TikHubScanError("raw_identity_conflict", "Pending page receipt changed")
        checkpoint(connection, claim, {"pending_raw": dict(receipt)}, now=now)


def _reference(
    claim: DurableClaim, scope: Mapping[str, Any], *, db_path: Path,
    raw_root: Path | None, call_override: PageCall | None, now: str,
) -> str:
    if scope["platform"] != "douyin":
        return str(scope["uid"])
    state = _state(claim, scope, db_path)
    if state.get("reference"):
        return str(state["reference"])
    with connect(db_path) as connection, transaction(connection):
        _owned_scope(connection, claim, scope)
        row = connection.execute(
            "SELECT reference_value FROM account_provider_references WHERE account_identity_id=? "
            "AND provider='TikHub' COLLATE NOCASE AND reference_kind='sec_user_id'", (scope["identity_id"],),
        ).fetchone()
        if row is not None and providers._valid_douyin_sec_user_id(str(row["reference_value"])):
            value = str(row["reference_value"])
            checkpoint(connection, claim, {"reference": value}, now=now)
            return value
    key = f"reference:{scope['identity_id']}:{_digest(scope['uid'])}:v1"
    raw, _replayed = _raw(
        claim, scope, window_key=key, operation="douyin_uid_profile", request=scope,
        db_path=db_path, raw_root=raw_root, call_override=call_override,
    )
    parsed = providers._parse_douyin_reference_payload(raw.value, status=raw.http_status or 200)
    value = str(parsed.data["reference"])
    payload = raw.value
    data = (payload.get("data") if isinstance(payload, dict) and "code" not in payload
            else providers._tikhub_douyin_data(payload))
    containers = [data]
    if isinstance(data, dict):
        containers += [data.get("data"), data.get("user")]
    subjects = {str(item[key]) for item in containers if isinstance(item, dict)
                for key in ("uid", "id_str", "user_id") if item.get(key) not in (None, "")}
    if str(scope["uid"]) not in subjects or any(value != str(scope["uid"]) for value in subjects):
        raise TikHubScanError("identity_conflict", "UID profile did not prove the requested canonical UID")
    receipt = _raw_receipt(raw, window_key=key, kind="reference")
    _record_received(claim, scope, receipt, db_path=db_path, now=now)
    with connect(db_path) as connection, transaction(connection):
        _owned_scope(connection, claim, scope)
        connection.execute(
            "INSERT INTO account_provider_references(account_identity_id,provider,reference_kind,"
            "reference_value,source_raw_response_id,created_at,updated_at) "
            "VALUES (?,'TikHub','sec_user_id',?,?,?,?) ON CONFLICT(account_identity_id,provider,reference_kind) "
            "DO UPDATE SET reference_value=excluded.reference_value,source_raw_response_id=excluded.source_raw_response_id,"
            "updated_at=excluded.updated_at",
            (scope["identity_id"], value, raw.raw_response_id, now, now),
        )
        connection.execute("UPDATE provider_raw_responses SET source='derived_applied' WHERE id=?",
                           (raw.raw_response_id,))
        checkpoint(connection, claim, {"reference": value, "pending_raw": None}, now=now)
    return value


def _page(raw: StoredRawResponse, platform: str) -> tuple[list[Any], bool, Any, int | None]:
    return _page_payload(raw.value, platform)


def _page_payload(payload: Any, platform: str) -> tuple[list[Any], bool, Any, int | None]:
    if platform == "douyin":
        data = (payload.get("data") if isinstance(payload, dict) and "code" not in payload
                else providers._tikhub_douyin_data(payload))
        list_name = "aweme_list"
    else:
        data = (providers._tikhub_xhs_data(payload)
                if isinstance(payload, dict) and "code" in payload and "data" in payload else payload)
        list_name = "notes"
    page = providers._find_list_page(data, list_name)
    if page is None:
        raise TikHubScanError("invalid_response", "The fixed route omitted its raw list")
    items = page[list_name]
    more = providers._discovery_has_more(page, provider="TikHub", raw_response=payload)
    cursor = page.get("max_cursor") if platform == "douyin" else page.get("cursor")
    if cursor is None or cursor == "":
        cursor = page.get("cursor") or page.get("next_cursor")
    if platform == "xiaohongshu" and cursor in (None, "") and items:
        last = items[-1]
        if isinstance(last, dict):
            cursor = last.get("cursor") or last.get("next_cursor")
    total = next((page[key] for key in ("total_count", "total") if key in page), None)
    if total is not None and (type(total) is not int or total < 0):
        raise TikHubScanError("invalid_total", "Declared page total is not a nonnegative integer")
    return items, more, cursor, total


def _item(platform: str, value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("Item is not an object")
    if platform == "douyin":
        identifier = value.get("aweme_id")
        normalized = providers._parse_douyin_discovery_payload(
            {"data": {"aweme_list": [value], "has_more": False}},
        ).data["items"]
        content_type = "image" if value.get("images") else "video" if value.get("video") else "unknown"
    else:
        card = value.get("note_card") or value.get("note") or value
        if not isinstance(card, dict):
            raise ValueError("Note card is not an object")
        identifier = value.get("note_id") or card.get("note_id") or card.get("id")
        normalized = providers._parse_xhs_discovery_payload({"notes": [value], "has_more": False})["items"]
        kind = str(card.get("type") or value.get("type") or "").lower()
        content_type = "video" if kind == "video" else "image" if kind in {"normal", "image"} else "unknown"
    if type(identifier) not in (str, int) or not str(identifier).strip() or len(normalized) != 1:
        raise ValueError("Item has no unambiguous platform ID")
    item = dict(normalized[0])
    if item["platform_content_id"] != str(identifier).strip():
        raise ValueError("Normalized platform ID changed")
    item["content_type"] = content_type
    return item


def _pin_marker(value: Any) -> bool | None:
    """Normalize only explicit provider pin markers; ambiguity is unknown."""
    if type(value) is bool:
        return value
    if type(value) is int and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        marker = value.strip().lower()
        if marker in {"true", "1"}:
            return True
        if marker in {"false", "0"}:
            return False
    return None


def _item_evidence(platform: str, value: Any) -> dict[str, Any]:
    """Return verifier-recomputable ID/time/pin facts from one raw list item."""
    containers = [value]
    if platform == "xiaohongshu" and isinstance(value, dict):
        containers.extend((value.get("note_card"), value.get("note")))
    markers: list[bool] = []
    marker_unknown = False
    marker_seen = False
    for container in containers:
        if not isinstance(container, dict):
            continue
        for name in ("is_pinned", "is_top", "sticky", "is_sticky"):
            if name not in container:
                continue
            marker_seen = True
            marker = _pin_marker(container[name])
            if marker is None:
                marker_unknown = True
            else:
                markers.append(marker)
    is_pinned = (
        markers[0]
        if marker_seen and not marker_unknown and markers and len(set(markers)) == 1
        else None
    )
    evidence: dict[str, Any] = {
        "platform_content_id": None,
        "published_at": None,
        "is_pinned": is_pinned,
        "event_tuple": None,
    }
    try:
        item = _item(platform, value)
    except (ValueError, TypeError, KeyError, CaptureError, OperationError):
        return evidence
    identifier = str(item["platform_content_id"])
    evidence["platform_content_id"] = identifier
    try:
        published = normalize_timestamp(item.get("published_at"))
    except (ValueError, TypeError, OverflowError, OSError):
        published = None
    evidence["published_at"] = published
    if published is not None:
        evidence["event_tuple"] = [published, identifier]
    return evidence


def _range_start_page_proof(
    platform: str,
    items: list[Any],
    *,
    window_start: str,
    prior_qualifying_old_page_count: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Recompute one v2 boundary page and its same-generation streak."""
    if type(prior_qualifying_old_page_count) is not int or prior_qualifying_old_page_count < 0:
        raise TikHubScanError("checkpoint_integrity_error", "Old-page streak is not a nonnegative integer")
    evidence = [_item_evidence(platform, value) for value in items]
    non_pinned = [item for item in evidence if item["is_pinned"] is False]
    events = [item["event_tuple"] for item in non_pinned if item["event_tuple"] is not None]
    missing_event_count = sum(item["event_tuple"] is None for item in evidence)
    unknown_pinned_count = sum(item["is_pinned"] is None for item in evidence)
    all_before = bool(non_pinned) and len(events) == len(non_pinned) and all(
        parse_time(event[0]) < parse_time(window_start) for event in events
    )
    qualifies = (
        bool(non_pinned)
        and unknown_pinned_count == 0
        and missing_event_count == 0
        and all_before
    )
    streak = prior_qualifying_old_page_count + 1 if qualifies else 0
    event_tuples = [tuple(event) for event in events]
    proof = {
        "qualifies": qualifies,
        "pinned_count": sum(item["is_pinned"] is True for item in evidence),
        "unknown_pinned_count": unknown_pinned_count,
        "non_pinned_count": len(non_pinned),
        "missing_event_count": missing_event_count,
        "non_pinned_min_event": list(min(event_tuples)) if event_tuples else None,
        "non_pinned_max_event": list(max(event_tuples)) if event_tuples else None,
        "all_non_pinned_before_window_start": all_before,
        "qualifying_old_page_count": streak,
    }
    return evidence, proof


def _manifest(root: Path, claim: DurableClaim, value: Mapping[str, Any]) -> dict[str, Any]:
    body = (_json(value) + "\n").encode("utf-8")
    digest = hashlib.sha256(body).hexdigest()
    directory = root / "scan-manifests" / claim.scan_id
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    path = directory / f"{digest}.json"
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        if path.is_symlink() or path.read_bytes() != body:
            raise TikHubScanError("manifest_conflict", "An immutable page manifest changed")
    else:
        with os.fdopen(descriptor, "wb") as output:
            output.write(body)
            output.flush()
            os.fsync(output.fileno())
        descriptor = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return {"path": str(path), "sha256": digest, "byte_size": len(body)}


def _observe(
    connection: sqlite3.Connection, content_id: int, item: Mapping[str, Any],
    raw: StoredRawResponse, claim: DurableClaim,
) -> None:
    metrics = item.get("metrics")
    if not isinstance(metrics, dict):
        return
    values = {field: metrics.get(field) for field in METRIC_FIELDS}
    if not any(type(value) is int and value >= 0 for value in values.values()):
        return
    values = {field: value if type(value) is int and value >= 0 else None for field, value in values.items()}
    metadata = {"scan_id": claim.scan_id, "operation": raw.operation, "fields": {
        field: {"status": "not_requested"} for field, value in values.items() if value is None
    }}
    persist_metric_observation(
        connection, content_id=content_id, captured_at=raw.captured_at,
        recorded_at=now_utc(), window_key=parse_time(raw.captured_at).astimezone(SHANGHAI).date().isoformat(),
        view_count=values["view_count"], comment_count=values["comment_count"],
        like_count=values["like_count"], share_count=values["share_count"],
        collect_count=values["collect_count"], status="available", provider="TikHub",
        platform=str(item["platform"]), raw_response_id=raw.raw_response_id,
        metadata_json=_json(metadata),
    )


def _materialization_identity(
    claim: DurableClaim,
    scope: Mapping[str, Any],
    receipt: Mapping[str, Any],
    *,
    generation: int,
    page_number: int,
    eligible_indexes: list[int],
) -> dict[str, Any]:
    """Freeze the exact local derivation without copying provider data into JSON."""
    return {
        "contract_version": MATERIALIZATION_CONTRACT_VERSION,
        "parent_scan_id": claim.scan_id,
        "parent_scheduler_run_id": claim.scheduler_run_id,
        "generation": generation,
        "page_number": page_number,
        "window_key": str(receipt["window_key"]),
        "raw_response_id": int(receipt["raw_response_id"]),
        "raw_sha256": str(receipt["sha256"]),
        "eligible_indexes": eligible_indexes,
        "purpose": str(scope["purpose"]),
        "identity_id": int(scope["identity_id"]),
        "account_id": int(scope["account_id"]),
        "platform": str(scope["platform"]),
        "uid": str(scope["uid"]),
        "roster_snapshot_id": int(scope["roster_snapshot_id"]),
        "roster_snapshot_hash": str(scope["roster_snapshot_hash"]),
        **({
            "activation_id": int(scope["activation_id"]),
            "profile_id": str(scope["profile_id"]),
            "activation_sha256": str(scope["activation_sha256"]),
        } if all(key in scope for key in (
            "activation_id", "profile_id", "activation_sha256"
        )) else {}),
        "window_start": str(scope["window_start"]),
        "window_end": str(scope["window_end"]),
        "derived_adapter_version": "tikhub-discovery-derived-v8.1",
        "derived_operations": {
            "detail": providers.STAGE_CONFIG[(str(scope["platform"]), "detail")][2],
            "metrics": providers.STAGE_CONFIG[(str(scope["platform"]), "metrics")][2],
        },
        "zero_view_is_authoritative": False,
        "materialize_detail": True,
        "materialize_existing_stages": True,
        "preserve_existing_content_fields": True,
    }


def _materialization_run(
    identity: Mapping[str, Any], *, db_path: Path,
) -> dict[str, Any] | None:
    scheduled_for = "scan:" + scan_identity(MATERIALIZATION_JOB, identity)
    with connect(db_path) as connection:
        row = connection.execute(
            "SELECT id FROM scheduler_runs WHERE job_id=? AND scheduled_for=?" + durable_runs.root_run_predicate(connection),
            (MATERIALIZATION_JOB, scheduled_for),
        ).fetchone()
    return None if row is None else get_run(int(row["id"]), db_path=db_path)


def _monotonic_now() -> float:
    return monotonic_time.monotonic()


def _local_materialization_preflight(
    scheduler_run_id: int, *, db_path: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Validate a frozen local continuation before creating a new attempt."""
    run = get_run(scheduler_run_id, db_path=db_path)
    details = run.get("details")
    scope = details.get("identity") if isinstance(details, dict) else None
    purpose = scope.get("purpose") if isinstance(scope, dict) else None
    expected_job = (
        {
            "reconcile": "tikhub_reconcile",
            "history": "history_recovery",
        }.get(purpose)
        if isinstance(purpose, str)
        else None
    )
    try:
        expected_scan_id = (
            scan_identity(str(expected_job), scope)
            if expected_job is not None and isinstance(scope, dict)
            else None
        )
    except DurableRunError as error:
        raise TikHubScanError(
            "scan_contract_mismatch", "This is not a frozen TikHub v2 scan",
        ) from error
    if (
        not isinstance(details, dict)
        or not isinstance(scope, dict)
        or scope.get("contract_version") != CONTRACT_VERSION
        or run.get("job_id") != expected_job
        or details.get("scan_id") != expected_scan_id
        or run.get("scheduled_for") != f"scan:{expected_scan_id}"
    ):
        raise TikHubScanError(
            "scan_contract_mismatch", "This is not a frozen TikHub v2 scan",
        )
    state = details.get("checkpoint")
    if not isinstance(state, dict):
        raise TikHubScanError(
            "materialization_integrity_error", "Parent materialization checkpoint is invalid",
        )
    pending = state.get("pending_materialization")
    if pending is None:
        raise TikHubScanError(
            "materialization_not_pending", "The scan has no pending local materialization",
        )
    if not isinstance(pending, dict):
        raise TikHubScanError(
            "materialization_integrity_error", "Pending materialization is invalid",
        )
    identity = pending.get("identity")
    desired = pending.get("after_materialization")
    indexes = identity.get("eligible_indexes") if isinstance(identity, dict) else None
    if (
        not isinstance(identity, dict)
        or not isinstance(desired, dict)
        or identity.get("contract_version") != MATERIALIZATION_CONTRACT_VERSION
        or type(identity.get("parent_scheduler_run_id")) is not int
        or identity.get("parent_scheduler_run_id") != scheduler_run_id
        or identity.get("parent_scan_id") != expected_scan_id
        or type(identity.get("generation")) is not int
        or type(identity.get("page_number")) is not int
        or not isinstance(identity.get("window_key"), str)
        or type(identity.get("raw_response_id")) is not int
        or not isinstance(identity.get("raw_sha256"), str)
        or not isinstance(indexes, list)
        or any(type(index) is not int or index < 0 for index in indexes)
        or indexes != sorted(set(indexes))
        or type(desired.get("complete")) is not bool
        or state.get("complete") is not False
    ):
        raise TikHubScanError(
            "materialization_integrity_error", "Pending materialization contract is invalid",
        )
    mirrored_fields = (
        "purpose",
        "identity_id",
        "account_id",
        "platform",
        "uid",
        "roster_snapshot_id",
        "roster_snapshot_hash",
        "window_start",
        "window_end",
    )
    if any(identity.get(key) != scope.get(key) for key in mirrored_fields):
        raise TikHubScanError(
            "materialization_integrity_error", "Pending materialization scope changed",
        )
    activation_fields = ("activation_id", "profile_id", "activation_sha256")
    if any(identity.get(key) != scope.get(key) for key in activation_fields) or any(
        (key in identity) != (key in scope) for key in activation_fields
    ):
        raise TikHubScanError(
            "materialization_integrity_error", "Pending acquisition epoch changed",
        )
    expected_derived = {
        "detail": providers.STAGE_CONFIG[(str(scope["platform"]), "detail")][2],
        "metrics": providers.STAGE_CONFIG[(str(scope["platform"]), "metrics")][2],
    }
    if (
        identity.get("derived_adapter_version") != "tikhub-discovery-derived-v8.1"
        or identity.get("derived_operations") != expected_derived
        or identity.get("zero_view_is_authoritative") is not False
        or identity.get("materialize_detail") is not True
        or identity.get("materialize_existing_stages") is not True
        or identity.get("preserve_existing_content_fields") is not True
    ):
        raise TikHubScanError(
            "materialization_integrity_error", "Pending derivation policy changed",
        )

    head = state.get("last_manifest")
    try:
        if not isinstance(head, dict):
            raise ValueError("manifest receipt missing")
        path = Path(head["path"])
        if path.is_symlink():
            raise ValueError("manifest is a symlink")
        body = path.read_bytes()
        if (
            type(head.get("byte_size")) is not int
            or len(body) != head["byte_size"]
            or hashlib.sha256(body).hexdigest() != head.get("sha256")
        ):
            raise ValueError("manifest bytes changed")
        manifest = json.loads(body)
        raw = manifest["raw"]
        items = manifest["items"]
        if not isinstance(manifest, dict) or not isinstance(raw, dict) or not isinstance(items, list):
            raise ValueError("manifest shape changed")
        eligible = [
            item["index"]
            for item in items
            if isinstance(item, dict)
            and item.get("reason") == ""
            and type(item.get("content_id")) is int
        ]
        completion_reason = manifest.get("completion_reason")
        if (
            manifest.get("contract_version") != CONTRACT_VERSION
            or manifest.get("scan_id") != expected_scan_id
            or manifest.get("scope") != scope
            or manifest.get("generation") != identity["generation"]
            or manifest.get("page_number") != identity["page_number"]
            or state.get("generation") != identity["generation"]
            or state.get("page_number") != identity["page_number"] + 1
            or manifest.get("execution_next_cursor") != state.get("cursor")
            or manifest.get("provider_next_cursor") != state.get("provider_next_cursor")
            or raw.get("window_key") != identity["window_key"]
            or raw.get("raw_response_id") != identity["raw_response_id"]
            or raw.get("sha256") != identity["raw_sha256"]
            or eligible != indexes
            or desired.get("complete") != (completion_reason is not None)
            or desired.get("completion_reason") != completion_reason
        ):
            raise ValueError("manifest continuation changed")
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as error:
        raise TikHubScanError(
            "materialization_integrity_error",
            "Pending materialization is not bound to its committed page",
        ) from error
    child = _materialization_run(identity, db_path=db_path)
    if child is not None:
        child_details = child.get("details")
        child_state = (
            child_details.get("checkpoint") if isinstance(child_details, dict) else None
        )
        completed_indexes = (
            child_state.get("completed_indexes") if isinstance(child_state, dict) else None
        )
        next_offset = (
            child_state.get("next_item_offset") if isinstance(child_state, dict) else None
        )
        if (
            not isinstance(child_details, dict)
            or not isinstance(child_state, dict)
            or child_details.get("identity") != identity
            or child_details.get("scan_id") != scan_identity(MATERIALIZATION_JOB, identity)
            or child.get("scheduled_for")
            != "scan:" + scan_identity(MATERIALIZATION_JOB, identity)
            or (
                child_state.get("progress_contract_version") is not None
                and (
                    child_state.get("progress_contract_version")
                    != MATERIALIZATION_PROGRESS_CONTRACT_VERSION
                    or type(next_offset) is not int
                    or not isinstance(completed_indexes, list)
                    or not 0 <= next_offset <= len(indexes)
                    or completed_indexes != indexes[:next_offset]
                )
            )
            or (
                child.get("status") == "succeeded"
                and child_state.get("complete") is not True
            )
            or (
                child.get("status") in {"partial", "failed", "interrupted"}
                and child_state.get("complete") is True
            )
        ):
            raise TikHubScanError(
                "materialization_integrity_error", "Materialization child contract changed",
            )
    # Raw bytes, parsed items, account ownership and frozen eligible indexes are
    # all local reads.  Validate them before the parent attempt is claimed.
    _eligible_materialization_page(scope, pending, db_path=db_path)
    return copy.deepcopy(run), copy.deepcopy(scope), copy.deepcopy(pending)


def _eligible_materialization_page(
    scope: Mapping[str, Any], pending: Mapping[str, Any], *, db_path: Path,
) -> tuple[StoredRawResponse, dict[str, Any]]:
    identity = pending.get("identity")
    if not isinstance(identity, dict):
        raise TikHubScanError(
            "materialization_integrity_error", "Pending materialization identity is missing",
        )
    operation = (
        "douyin_user_posts"
        if scope["platform"] == "douyin"
        else "xiaohongshu_user_posts"
    )
    raw = load_succeeded_raw_response(
        stage="discovery",
        window_key=str(identity["window_key"]),
        account_id=int(scope["account_id"]),
        operation=operation,
        db_path=db_path,
    )
    if (
        raw.raw_response_id != identity.get("raw_response_id")
        or raw.sha256 != identity.get("raw_sha256")
    ):
        raise TikHubScanError(
            "materialization_integrity_error", "Pending materialization raw response changed",
        )
    values, _more, _cursor_value, _total = _page(raw, str(scope["platform"]))
    indexes = identity.get("eligible_indexes")
    if (
        not isinstance(indexes, list)
        or any(type(index) is not int or index < 0 for index in indexes)
        or indexes != sorted(set(indexes))
        or any(index >= len(values) for index in indexes)
    ):
        raise TikHubScanError(
            "materialization_integrity_error", "Pending materialization indexes are invalid",
        )
    normalized: list[dict[str, Any]] = []
    start = parse_time(str(scope["window_start"]))
    end = parse_time(str(scope["window_end"]))
    with connect(db_path) as connection:
        for index in indexes:
            try:
                item = _item(str(scope["platform"]), values[index])
                published = normalize_timestamp(item.get("published_at"))
            except (ValueError, TypeError, KeyError, CaptureError, OperationError) as error:
                raise TikHubScanError(
                    "materialization_integrity_error", "Eligible raw item no longer normalizes",
                ) from error
            if (
                str(item.get("account_uid") or "") != str(scope["uid"])
                or published is None
                or not (start <= parse_time(published) < end)
            ):
                raise TikHubScanError(
                    "materialization_integrity_error", "Eligible raw item left its frozen scope",
                )
            current = connection.execute(
                "SELECT account_id,raw_account_uid "
                "FROM content_items WHERE platform=? AND platform_content_id=?",
                (scope["platform"], item["platform_content_id"]),
            ).fetchone()
            if (
                current is None
                or current["account_id"] != scope["account_id"]
                or current["raw_account_uid"] not in (None, "", scope["uid"])
            ):
                raise TikHubScanError(
                    "materialization_integrity_error", "Eligible content identity changed",
                )
            normalized.append({**item, "published_at": published})
    return raw, {"items": normalized}


def _finish_materialization_parent(
    claim: DurableClaim,
    scope: Mapping[str, Any],
    pending: Mapping[str, Any],
    *,
    db_path: Path,
    now: str,
) -> bool:
    identity = pending.get("identity")
    child = _materialization_run(identity, db_path=db_path) if isinstance(identity, dict) else None
    if (
        child is None
        or child["status"] != "succeeded"
        or child["details"].get("identity") != identity
        or child["details"].get("checkpoint", {}).get("complete") is not True
    ):
        raise TikHubScanError(
            "materialization_pending", "Derived page materialization is not complete",
        )
    with connect(db_path) as connection, transaction(connection):
        state = _owned_scope(connection, claim, scope)
        if state.get("pending_materialization") != pending:
            raise TikHubScanError(
                "materialization_integrity_error", "Pending materialization changed",
            )
        desired = pending.get("after_materialization")
        if not isinstance(desired, dict) or type(desired.get("complete")) is not bool:
            raise TikHubScanError(
                "materialization_integrity_error", "Pending completion state is invalid",
            )
        checkpoint(
            connection,
            claim,
            {
                "pending_materialization": None,
                "complete": desired["complete"],
                "completion_reason": desired.get("completion_reason"),
            },
            now=now,
        )
    return bool(desired["complete"])


def _completed_child_claim(run: Mapping[str, Any]) -> DurableClaim:
    details = run.get("details")
    if not isinstance(details, dict):
        raise TikHubScanError(
            "materialization_integrity_error", "Materialization child details are invalid",
        )
    owner = details.get("owner")
    state = details.get("checkpoint")
    if (
        run.get("status") != "running"
        or not isinstance(owner, dict)
        or not isinstance(state, dict)
        or state.get("complete") is not True
        or type(owner.get("attempt_id")) is not int
        or type(owner.get("attempt_number")) is not int
        or not isinstance(owner.get("token"), str)
        or not owner["token"]
        or not isinstance(details.get("scan_id"), str)
        or not isinstance(state.get("result_sha256"), str)
        or type(state.get("raw_response_id")) is not int
    ):
        raise TikHubScanError(
            "materialization_integrity_error", "Completed materialization child is malformed",
        )
    return DurableClaim(
        int(run["id"]),
        owner["attempt_id"],
        owner["attempt_number"],
        owner["token"],
        details["scan_id"],
    )


def _finish_completed_materialization_child(
    child_claim: DurableClaim, *, db_path: Path, now: str,
) -> None:
    finish_run(
        child_claim,
        status="succeeded",
        db_path=db_path,
        now=now,
        summary={"reason": "local_materialization_complete", "blocker": None},
    )


def _materialize_page(
    scope: Mapping[str, Any],
    identity: Mapping[str, Any],
    raw: StoredRawResponse,
    page: Mapping[str, Any],
    child_claim: DurableClaim,
    *,
    db_path: Path,
    raw_root: Path | None,
) -> dict[str, Any]:
    with paid_scope(
        str(scope["purpose"]),
        activation_id=scope.get("activation_id"),
        roster_snapshot_id=int(scope["roster_snapshot_id"]),
        roster_snapshot_hash=str(scope["roster_snapshot_hash"]),
        scheduler_run_id=child_claim.scheduler_run_id,
        scheduler_attempt_id=child_claim.attempt_id,
        business_day=parse_time(str(scope["window_end"])).astimezone(SHANGHAI).date().isoformat(),
    ):
        return providers.materialize_account_discovery_page(
            account_id=int(scope["account_id"]),
            platform=str(scope["platform"]),
            account_uid=str(scope["uid"]),
            page=page,
            source_raw_response_id=raw.raw_response_id,
            metrics_window_key=parse_time(raw.captured_at).astimezone(SHANGHAI).date().isoformat(),
            discovery_operation=raw.operation,
            provider="TikHub",
            derived_adapter_version=str(identity["derived_adapter_version"]),
            derived_operations=dict(identity["derived_operations"]),
            zero_view_is_authoritative=bool(identity["zero_view_is_authoritative"]),
            db_path=db_path,
            materialize_detail=bool(identity["materialize_detail"]),
            materialize_existing_stages=bool(identity["materialize_existing_stages"]),
            preserve_existing_content_fields=bool(
                identity["preserve_existing_content_fields"]
            ),
            new_content_source_group=(
                (lambda _published: "history-backfill")
                if scope["purpose"] == "history"
                else None
            ),
            derived_raw_root=raw_root,
            media_root=(raw_root / "derived-media") if raw_root is not None else None,
        )


def _materialize_pending(
    claim: DurableClaim,
    scope: Mapping[str, Any],
    pending: Mapping[str, Any],
    *,
    db_path: Path,
    raw_root: Path | None,
    now: str,
    max_items: int | None = None,
    deadline: float | None = None,
    progress: dict[str, Any] | None = None,
) -> bool:
    """Run or replay one local-only child before the parent may advance."""
    identity = pending.get("identity")
    if not isinstance(identity, dict):
        raise TikHubScanError(
            "materialization_integrity_error", "Pending materialization identity is missing",
        )
    bounded = max_items is not None or deadline is not None or progress is not None
    if bounded and (
        type(max_items) is not int
        or not 1 <= max_items <= MATERIALIZATION_ITEM_LIMIT
        or not isinstance(deadline, (int, float))
        or isinstance(deadline, bool)
        or not math.isfinite(float(deadline))
        or progress is None
    ):
        raise TikHubScanError(
            "invalid_materialization_limit", "Bounded materialization controls are invalid",
        )
    eligible_indexes = identity.get("eligible_indexes")
    if not isinstance(eligible_indexes, list):
        raise TikHubScanError(
            "materialization_integrity_error", "Pending materialization indexes are missing",
        )

    def finalized_progress() -> None:
        if progress is None:
            return
        progress["processed_items"] = max(1, int(progress["processed_items"]))
        progress["remaining_items"] = 0
        progress["next_item_offset"] = len(eligible_indexes)
        progress["finalized"] = True

    child_claim = claim_run(
        MATERIALIZATION_JOB,
        identity,
        db_path=db_path,
        now=now,
        initial_checkpoint={"parent_attempt_id": claim.attempt_id},
    )
    if child_claim is not None:
        claimed = get_run(child_claim.scheduler_run_id, db_path=db_path)
        if claimed["details"].get("checkpoint", {}).get("complete") is True:
            _finish_completed_materialization_child(child_claim, db_path=db_path, now=now)
            finalized_progress()
            return _finish_materialization_parent(
                claim, scope, pending, db_path=db_path, now=now,
            )
    if child_claim is None:
        existing = _materialization_run(identity, db_path=db_path)
        if existing is None:
            raise TikHubScanError(
                "materialization_integrity_error", "Materialization child disappeared",
            )
        if existing["status"] == "succeeded":
            finalized_progress()
            return _finish_materialization_parent(
                claim, scope, pending, db_path=db_path, now=now,
            )
        if (
            existing["status"] == "running"
            and existing["details"].get("checkpoint", {}).get("complete") is True
        ):
            _finish_completed_materialization_child(
                _completed_child_claim(existing), db_path=db_path, now=now,
            )
            finalized_progress()
            return _finish_materialization_parent(
                claim, scope, pending, db_path=db_path, now=now,
            )
        owner_attempt = existing["details"].get("checkpoint", {}).get("parent_attempt_id")
        child_attempt_id = existing["details"].get("owner", {}).get("attempt_id")
        if (
            existing["status"] == "running"
            and owner_attempt != claim.attempt_id
            and type(child_attempt_id) is int
            and recover_run(
                int(existing["id"]),
                expected_attempt_id=child_attempt_id,
                db_path=db_path,
                reason="parent_scan_attempt_recovered",
                now=now,
            )
        ):
            child_claim = claim_run(
                MATERIALIZATION_JOB,
                identity,
                db_path=db_path,
                now=now,
                initial_checkpoint={"parent_attempt_id": claim.attempt_id},
            )
        if child_claim is None:
            raise TikHubScanError(
                "materialization_pending", "Derived page materialization is awaiting retry",
            )
    with connect(db_path) as connection, transaction(connection):
        checkpoint(
            connection,
            child_claim,
            {"parent_attempt_id": claim.attempt_id},
            now=now,
        )
    item_in_flight = False
    try:
        raw, page = _eligible_materialization_page(scope, pending, db_path=db_path)
        if not bounded:
            materialized = _materialize_page(
                scope,
                identity,
                raw,
                page,
                child_claim,
                db_path=db_path,
                raw_root=raw_root,
            )
            failures = materialized.get("derived_stages", {}).get("failures")
            if not isinstance(failures, list) or failures:
                raise TikHubScanError(
                    "materialization_failed", "One or more zero-cost derived stages failed",
                )
            result_sha256 = _digest(materialized)
        else:
            child = get_run(child_claim.scheduler_run_id, db_path=db_path)
            child_state = child["details"].get("checkpoint", {})
            completed_indexes = child_state.get("completed_indexes", [])
            item_result_sha256 = child_state.get("item_result_sha256", [])
            next_offset = child_state.get("next_item_offset", 0)
            page_items = page.get("items")
            if (
                not isinstance(child_state, dict)
                or child_state.get("progress_contract_version") not in (
                    None,
                    MATERIALIZATION_PROGRESS_CONTRACT_VERSION,
                )
                or not isinstance(completed_indexes, list)
                or not isinstance(item_result_sha256, list)
                or type(next_offset) is not int
                or not 0 <= next_offset <= len(eligible_indexes)
                or completed_indexes != eligible_indexes[:next_offset]
                or len(item_result_sha256) != next_offset
                or any(not isinstance(value, str) for value in item_result_sha256)
                or not isinstance(page_items, list)
                or len(page_items) != len(eligible_indexes)
                or child_state.get("raw_response_id") not in (None, raw.raw_response_id)
            ):
                raise TikHubScanError(
                    "materialization_integrity_error",
                    "Materialization child continuation is invalid",
                )
            if progress is None or max_items is None or deadline is None:
                raise TikHubScanError(
                    "invalid_materialization_limit", "Bounded materialization controls are missing",
                )
            progress["remaining_items"] = len(eligible_indexes) - next_offset
            progress["next_item_offset"] = next_offset
            with connect(db_path) as connection, transaction(connection):
                source = connection.execute(
                    "SELECT source FROM provider_raw_responses WHERE id=?",
                    (raw.raw_response_id,),
                ).fetchone()
                if source is None or source["source"] not in {
                    "live",
                    "materialization_pending",
                    "derived_applied",
                }:
                    raise TikHubScanError(
                        "materialization_integrity_error",
                        "Discovery raw materialization state is invalid",
                    )
                connection.execute(
                    "UPDATE provider_raw_responses SET source='materialization_pending' "
                    "WHERE id=? AND source='live'",
                    (raw.raw_response_id,),
                )
                checkpoint(
                    connection,
                    child_claim,
                    {
                        "progress_contract_version": MATERIALIZATION_PROGRESS_CONTRACT_VERSION,
                        "completed_indexes": completed_indexes,
                        "item_result_sha256": item_result_sha256,
                        "next_item_offset": next_offset,
                        "eligible_item_count": len(eligible_indexes),
                        "raw_response_id": raw.raw_response_id,
                    },
                    now=now,
                )
            while next_offset < len(eligible_indexes):
                if int(progress["processed_items"]) >= max_items:
                    break
                if _monotonic_now() >= float(deadline):
                    progress["deadline_reached"] = True
                    break
                item_in_flight = True
                progress["processed_items"] = int(progress["processed_items"]) + 1
                materialized = _materialize_page(
                    scope,
                    identity,
                    raw,
                    {"items": [page_items[next_offset]]},
                    child_claim,
                    db_path=db_path,
                    raw_root=raw_root,
                )
                failures = materialized.get("derived_stages", {}).get("failures")
                if not isinstance(failures, list) or failures:
                    raise TikHubScanError(
                        "materialization_failed", "One or more zero-cost derived stages failed",
                    )
                item_in_flight = False
                completed_indexes.append(eligible_indexes[next_offset])
                item_result_sha256.append(_digest(materialized))
                next_offset += 1
                progress["succeeded_items"] = int(progress["succeeded_items"]) + 1
                progress["remaining_items"] = len(eligible_indexes) - next_offset
                progress["next_item_offset"] = next_offset
                with connect(db_path) as connection, transaction(connection):
                    checkpoint(
                        connection,
                        child_claim,
                        {
                            "progress_contract_version": MATERIALIZATION_PROGRESS_CONTRACT_VERSION,
                            "completed_indexes": completed_indexes,
                            "item_result_sha256": item_result_sha256,
                            "next_item_offset": next_offset,
                            "eligible_item_count": len(eligible_indexes),
                            "raw_response_id": raw.raw_response_id,
                        },
                        now=now,
                    )
            if next_offset < len(eligible_indexes):
                retry_at = _timestamp((parse_time(now) + timedelta(seconds=1)).isoformat())
                finish_run(
                    child_claim,
                    status="partial",
                    db_path=db_path,
                    now=now,
                    next_resume_at=retry_at,
                    summary={
                        "reason": "local_materialization_yield",
                        "blocker": "deadline" if progress["deadline_reached"] else "max_items",
                    },
                )
                return False
            result_sha256 = _digest(
                {
                    "raw_response_id": raw.raw_response_id,
                    "eligible_indexes": completed_indexes,
                    "item_result_sha256": item_result_sha256,
                }
            )
        with connect(db_path) as connection, transaction(connection):
            connection.execute(
                "UPDATE provider_raw_responses SET source='derived_applied' "
                "WHERE id=? AND source IN ('live','materialization_pending')",
                (raw.raw_response_id,),
            )
            checkpoint(
                connection,
                child_claim,
                {
                    "complete": True,
                    "result_sha256": result_sha256,
                    "raw_response_id": raw.raw_response_id,
                },
                now=now,
            )
        _finish_completed_materialization_child(child_claim, db_path=db_path, now=now)
        finalized_progress()
    except Exception as error:
        if progress is not None:
            progress["processed_items"] = max(1, int(progress["processed_items"]))
            progress["failed_items"] = int(progress["failed_items"]) + 1
            if item_in_flight:
                progress["remaining_items"] = max(1, int(progress["remaining_items"]))
        retry_at = _timestamp((parse_time(now) + timedelta(seconds=1)).isoformat())
        try:
            finish_run(
                child_claim,
                status="partial",
                db_path=db_path,
                now=now,
                next_resume_at=retry_at,
                summary={
                    "reason": "materialization_pending",
                    "blocker": getattr(error, "reason", type(error).__name__),
                },
            )
        except (LostOwnership, DurableRunError, sqlite3.OperationalError):
            pass
        if isinstance(error, TikHubScanError):
            raise
        raise TikHubScanError(
            "materialization_pending", "Zero-cost derived page materialization failed",
        ) from error
    return _finish_materialization_parent(
        claim, scope, pending, db_path=db_path, now=now,
    )


def _apply(
    claim: DurableClaim, scope: Mapping[str, Any], raw: StoredRawResponse,
    *, window_key: str, db_path: Path, raw_root: Path | None, now: str,
) -> bool:
    receipt = _raw_receipt(raw, window_key=window_key, kind="page")
    _record_received(claim, scope, receipt, db_path=db_path, now=now)
    items, more, raw_next_cursor, total = _page(raw, str(scope["platform"]))
    provider_next_cursor = (
        _cursor(str(scope["platform"]), raw_next_cursor) if more else raw_next_cursor
    )
    with connect(db_path) as connection, transaction(connection):
        state = _owned_scope(connection, claim, scope)
        if state.get("pending_raw") != receipt or _page_key(claim, state) != window_key:
            raise TikHubScanError("raw_identity_conflict", "Raw response no longer matches the frozen page")
        if total is not None and state.get("declared_total") not in (None, total):
            raise TikHubScanError("total_drift", "Declared total changed within this frozen scan")
        if more:
            prior = connection.execute(
                "SELECT 1 FROM fetch_slots WHERE account_id=? AND stage='discovery' AND status='succeeded' "
                "AND window_key LIKE ? AND window_key LIKE ? LIMIT 1",
                (scope["account_id"], f"scan:{claim.scan_id}:g{state['generation']}:%",
                 f"%:{_digest(provider_next_cursor)}"),
            ).fetchone()
            if prior is not None:
                raise TikHubScanError("repeated_cursor", "Provider cursor repeated within this scan generation")
        v2 = scope["contract_version"] == CONTRACT_VERSION
        range_start_proof: dict[str, Any]
        if v2:
            item_evidence, range_start_proof = _range_start_page_proof(
                str(scope["platform"]), items, window_start=str(scope["window_start"]),
                prior_qualifying_old_page_count=state.get("qualifying_old_page_count", 0),
            )
            completion_reason = (
                "provider_exhausted"
                if not more
                else "range_start_reached"
                if range_start_proof["qualifying_old_page_count"] >= 2
                else None
            )
            complete = completion_reason is not None
        else:
            item_evidence = [{} for _item_value in items]
            range_start_proof = {}
            completion_reason = None
            complete = not more
        execution_next_cursor = None if complete else provider_next_cursor
        dispositions: list[dict[str, Any]] = []
        eligible_indexes: list[int] = []
        counts = dict.fromkeys(DISPOSITIONS, 0)
        seen: set[str] = set()
        for index, (value, evidence) in enumerate(zip(items, item_evidence, strict=True)):
            disposition: dict[str, Any] = {"index": index, **evidence}
            try:
                item = _item(str(scope["platform"]), value)
                identity = content_identity(str(item["platform"]), str(item["canonical_url"]), item["platform_content_id"])
            except (ValueError, TypeError, KeyError, CaptureError, OperationError):
                disposition.update(disposition="unparseable", reason="invalid_item")
                counts["unparseable"] += 1
                dispositions.append(disposition)
                continue
            identifier = str(item["platform_content_id"])
            disposition.update(platform_content_id=identifier, duplicate_in_page=identifier in seen)
            seen.add(identifier)
            matches = connection.execute(
                "SELECT id,account_id,raw_account_uid FROM content_items WHERE platform=? "
                "AND (platform_content_id=? OR normalized_url_hash=?) "
                "ORDER BY (platform_content_id=?) DESC,id",
                (scope["platform"], identifier, identity["normalized_url_hash"], identifier),
            ).fetchall()
            existing = matches[0] if matches else None
            try:
                published = normalize_timestamp(item.get("published_at"))
            except (ValueError, TypeError, OverflowError, OSError):
                published = None
            reason = ""
            if len(matches) > 1:
                reason = "existing_identity_conflict"
            elif str(item.get("account_uid") or "") != str(scope["uid"]):
                reason = "identity_unresolved" if not item.get("account_uid") else "identity_conflict"
            elif existing is not None and (
                existing["account_id"] not in (None, scope["account_id"])
                or existing["raw_account_uid"] not in (None, "", scope["uid"])
            ):
                reason = "existing_identity_conflict"
            elif published is None:
                reason = "publication_time_unresolved"
            elif not (parse_time(str(scope["window_start"])) <= parse_time(published)
                      < parse_time(str(scope["window_end"]))):
                reason = "outside_frozen_window"
            if reason:
                category = "existing" if existing is not None else "quarantined"
                disposition.update(disposition=category, reason=reason)
                if existing is not None:
                    disposition["content_id"] = int(existing["id"])
            else:
                connection.execute("SAVEPOINT scan_item")
                try:
                    content = upsert_content(
                        {**item, "published_at": published, "_preserve_existing_content_fields": True},
                        db_path=db_path, connection=connection,
                        source_group_on_insert="history-backfill" if scope["purpose"] == "history" else "",
                    )
                except (OperationError, ValueError):
                    connection.execute("ROLLBACK TO scan_item")
                    connection.execute("RELEASE scan_item")
                    category = "existing" if existing is not None else "unparseable"
                    disposition.update(disposition=category, reason="invalid_content_identity")
                    if existing is not None:
                        disposition["content_id"] = int(existing["id"])
                else:
                    connection.execute("RELEASE scan_item")
                    category = "inserted" if content["action"] == "inserted" else "existing"
                    content_id = int(content["id"])
                    disposition.update(disposition=category, content_id=content_id, reason="")
                    _observe(connection, content_id, item, raw, claim)
                    eligible_indexes.append(index)
            counts[category] += 1
            dispositions.append(disposition)
        if len(items) != sum(counts.values()) or len(items) != len(dispositions):
            raise TikHubScanError("disposition_mismatch", "Raw item conservation failed")
        manifest = _manifest(raw_root or RAW_ROOT, claim, {
            "contract_version": scope["contract_version"], "scan_id": claim.scan_id,
            "scope": dict(scope), "generation": state["generation"], "page_number": state["page_number"],
            "request_cursor": state["cursor"], "next_cursor": execution_next_cursor,
            "raw": receipt, "raw_items": len(items), "counts": counts, "items": dispositions,
            "previous": state.get("last_manifest"),
            **({
                "provider_has_more": more,
                "provider_next_cursor": provider_next_cursor,
                "execution_next_cursor": execution_next_cursor,
                "completion_reason": completion_reason,
                "range_start_proof": range_start_proof,
            } if v2 else {}),
        })
        totals = {key: int(state["counts"][key]) + counts[key] for key in DISPOSITIONS}
        if not v2:
            connection.execute("UPDATE provider_raw_responses SET source='derived_applied' WHERE id=?",
                               (raw.raw_response_id,))
        pending_materialization = None
        if v2:
            pending_materialization = {
                "identity": _materialization_identity(
                    claim,
                    scope,
                    receipt,
                    generation=int(state["generation"]),
                    page_number=int(state["page_number"]),
                    eligible_indexes=eligible_indexes,
                ),
                "after_materialization": {
                    "complete": complete,
                    "completion_reason": completion_reason,
                },
            }
        changes = {
            "complete": complete if not v2 else False,
            "page_number": int(state["page_number"]) + 1,
            "cursor": execution_next_cursor, "last_manifest": manifest, "pending_raw": None,
            "counts": totals, "raw_items": int(state["raw_items"]) + len(items),
            "declared_total": total if total is not None else state.get("declared_total"),
            "last_raw_response_id": raw.raw_response_id,
        }
        if v2:
            changes.update({
                "provider_next_cursor": provider_next_cursor,
                "completion_reason": None,
                "qualifying_old_page_count": range_start_proof["qualifying_old_page_count"],
                "pending_materialization": pending_materialization,
            })
        checkpoint(connection, claim, changes, now=now)
    return complete


def _result(run_id: int, *, db_path: Path, pages_this_run: int = 0) -> dict[str, Any]:
    run = get_run(run_id, db_path=db_path)
    details = run["details"]
    state = details["checkpoint"]
    return {
        "scheduler_run_id": run_id, "attempt_id": details.get("owner", {}).get("attempt_id"),
        "status": run["status"], "complete": bool(state["complete"]),
        "reason": details.get("summary", {}).get("reason", ""),
        "blocker": details.get("summary", {}).get("blocker"),
        "next_resume_at": details.get("next_resume_at"), "next_cursor": state["cursor"],
        "pages": state["page_number"], "pages_this_run": pages_this_run,
        "raw_items": state["raw_items"], "counts": state["counts"],
        "last_manifest": state.get("last_manifest"), "scan_id": details["scan_id"],
        "completion_reason": state.get("completion_reason"),
        "provider_next_cursor": state.get("provider_next_cursor"),
        "qualifying_old_page_count": state.get("qualifying_old_page_count", 0),
        "terminal_class": details.get("summary", {}).get("terminal_class"),
        "accounted": details.get("summary", {}).get("accounted", False),
        "required": details.get("summary", {}).get("required", True),
        "publication_blocker": details.get("summary", {}).get(
            "publication_blocker", False
        ),
    }


def _finish_success(
    claim: DurableClaim, reason: str, *, db_path: Path, now: str, pages: int,
) -> dict[str, Any]:
    finish_run(
        claim,
        status="succeeded",
        db_path=db_path,
        now=now,
        summary=terminal_summary(reason=reason, terminal_class="success"),
    )
    return _result(claim.scheduler_run_id, db_path=db_path, pages_this_run=pages)


def _finish_terminal(
    claim: DurableClaim,
    reason: str,
    terminal_class: str,
    *,
    db_path: Path,
    now: str,
    pages: int,
) -> dict[str, Any]:
    try:
        finish_run(
            claim,
            status="failed",
            db_path=db_path,
            now=now,
            summary=terminal_summary(reason=reason, terminal_class=terminal_class),
        )
    except LostOwnership:
        return {
            **_result(claim.scheduler_run_id, db_path=db_path, pages_this_run=pages),
            "reason": "attempt_owner_lost",
        }
    return _result(claim.scheduler_run_id, db_path=db_path, pages_this_run=pages)


def _transient_failure(
    claim: DurableClaim,
    scope: Mapping[str, Any],
    reason: str,
    *,
    db_path: Path,
    now: str,
    pages: int,
    delay: float,
    attempts_hint: int | None = None,
) -> dict[str, Any]:
    business_day = (
        parse_time(str(scope["window_end"])).astimezone(SHANGHAI).date().isoformat()
    )
    with connect(db_path) as connection, transaction(connection):
        state = _owned_scope(connection, claim, scope)
        key = _digest(
            {
                "business_day": business_day,
                "generation": state["generation"],
                "cursor": state["cursor"],
            }
        )
        prior = state.get("provider_transient")
        attempts = (
            int(prior["attempts"]) + 1
            if isinstance(prior, dict) and prior.get("key") == key
            else 1
        )
        if type(attempts_hint) is int:
            attempts = max(attempts, attempts_hint)
        checkpoint(
            connection,
            claim,
            {
                "provider_transient": {
                    "key": key,
                    "business_day": business_day,
                    "generation": state["generation"],
                    "cursor": state["cursor"],
                    "attempts": attempts,
                    "last_error_code": reason,
                }
            },
            now=now,
        )
    if attempts >= TRANSIENT_RETRY_LIMIT:
        return _finish_terminal(
            claim,
            reason,
            "provider_transient",
            db_path=db_path,
            now=now,
            pages=pages,
        )
    return _partial(
        claim,
        reason,
        db_path=db_path,
        now=now,
        pages=pages,
        delay=delay,
    )


def _classified_failure(
    claim: DurableClaim,
    scope: Mapping[str, Any],
    reason: str,
    *,
    db_path: Path,
    now: str,
    pages: int,
    delay: float = 300,
    http_status: int | None = None,
    has_raw: bool = False,
    attempts_hint: int | None = None,
) -> dict[str, Any]:
    terminal_class = classify_error(
        reason, http_status=http_status, has_raw=has_raw
    )
    if terminal_class == "provider_transient":
        return _transient_failure(
            claim,
            scope,
            reason,
            db_path=db_path,
            now=now,
            pages=pages,
            delay=delay,
            attempts_hint=attempts_hint,
        )
    if terminal_class is not None:
        return _finish_terminal(
            claim,
            reason,
            terminal_class,
            db_path=db_path,
            now=now,
            pages=pages,
        )
    return _partial(
        claim, reason, db_path=db_path, now=now, pages=pages, delay=delay
    )


def _partial(
    claim: DurableClaim, reason: str, *, db_path: Path, now: str,
    pages: int, delay: float = 300, budget: bool = False,
) -> dict[str, Any]:
    current = parse_time(now)
    if budget:
        local = current.astimezone(SHANGHAI)
        due = datetime.combine(local.date() + timedelta(days=1), time.min, SHANGHAI)
    else:
        due = current + timedelta(seconds=max(1, delay))
    try:
        finish_run(claim, status="partial", db_path=db_path, now=now,
                   summary={"reason": "budget_partial" if budget else reason, "blocker": reason if budget else None},
                   next_resume_at=_timestamp(due.isoformat()))
    except LostOwnership:
        return {**_result(claim.scheduler_run_id, db_path=db_path, pages_this_run=pages),
                "reason": "attempt_owner_lost"}
    return _result(claim.scheduler_run_id, db_path=db_path, pages_this_run=pages)


def _run(
    scope: Mapping[str, Any], *, db_path: Path, max_pages: int,
    raw_root: Path | None, now: str | None, call_override: PageCall | None,
) -> dict[str, Any]:
    if type(max_pages) is not int or not 1 <= max_pages <= PAGE_LIMIT:
        raise TikHubScanError("invalid_page_limit", "One scan invocation may process 1 through 20 pages")
    def clock() -> str:
        return _timestamp(now or now_utc())
    job = _job_id(str(scope["purpose"]))
    initial_checkpoint = {
        "cursor": _initial_cursor(str(scope["platform"])), "generation": 0,
        "page_number": 0, "counts": dict.fromkeys(DISPOSITIONS, 0), "raw_items": 0,
        "last_manifest": None, "pending_raw": None, "reference": None,
        "provider_transient": None,
    }
    if scope["contract_version"] == CONTRACT_VERSION:
        initial_checkpoint.update({
            "provider_next_cursor": None,
            "completion_reason": None,
            "qualifying_old_page_count": 0,
            "pending_materialization": None,
        })
    claim = claim_run(job, scope, db_path=db_path, now=clock(),
                      initial_checkpoint=initial_checkpoint)
    if claim is None:
        with connect(db_path) as connection:
            row = connection.execute("SELECT id FROM scheduler_runs WHERE job_id=? AND scheduled_for=?" + durable_runs.root_run_predicate(connection),
                                     (job, "scan:" + scan_identity(job, scope))).fetchone()
        if row is None:
            raise DurableRunError("Durable scan disappeared")
        return _result(int(row["id"]), db_path=db_path)
    pages = 0
    try:
        state = _state(claim, scope, db_path)
        pending = state.get("pending_materialization")
        if scope["contract_version"] == CONTRACT_VERSION and pending is not None:
            _materialize_pending(
                claim, scope, pending, db_path=db_path, raw_root=raw_root, now=clock(),
            )
            state = _state(claim, scope, db_path)
            if state["complete"]:
                return _finish_success(
                    claim,
                    str(state["completion_reason"]),
                    db_path=db_path,
                    now=clock(),
                    pages=pages,
                )
            # A recovery invocation is deliberately local-only. Even after the
            # child succeeds, yield before the next paid cursor so replay can
            # never hide an additional provider dispatch.
            return _partial(
                claim,
                "materialization_replay_yield",
                db_path=db_path,
                now=clock(),
                pages=pages,
                delay=1,
            )
        reference = _reference(claim, scope, db_path=db_path, raw_root=raw_root,
                               call_override=call_override, now=clock())
        for _ in range(max_pages):
            state = _state(claim, scope, db_path)
            if state["complete"]:
                break
            operation = "douyin_user_posts" if scope["platform"] == "douyin" else "xiaohongshu_user_posts"
            key = _page_key(claim, state)
            raw, _replayed = _raw(
                claim, scope, window_key=key, operation=operation,
                request={**scope, "reference": reference, "cursor": state["cursor"]},
                db_path=db_path, raw_root=raw_root, call_override=call_override,
            )
            complete = _apply(claim, scope, raw, window_key=key, db_path=db_path,
                              raw_root=raw_root, now=clock())
            pages += 1
            if scope["contract_version"] == CONTRACT_VERSION:
                pending = _state(claim, scope, db_path).get("pending_materialization")
                if pending is None:
                    raise TikHubScanError(
                        "materialization_integrity_error", "Applied v2 page has no local child",
                    )
                complete = _materialize_pending(
                    claim, scope, pending, db_path=db_path, raw_root=raw_root, now=clock(),
                )
            if complete:
                break
        if _state(claim, scope, db_path)["complete"]:
            state = _state(claim, scope, db_path)
            reason = (
                str(state["completion_reason"])
                if scope["contract_version"] == CONTRACT_VERSION
                else "source_exhausted"
            )
            return _finish_success(
                claim, reason, db_path=db_path, now=clock(), pages=pages
            )
        return _partial(claim, "page_limit_yield", db_path=db_path, now=clock(), pages=pages)
    except LostOwnership:
        result = _result(claim.scheduler_run_id, db_path=db_path, pages_this_run=pages)
        return {**result, "reason": "attempt_owner_lost"}
    except BudgetBlocked as error:
        reason = getattr(error, "error_code", "budget_blocked")
        return _classified_failure(
            claim, scope, reason, db_path=db_path, now=clock(), pages=pages
        )
    except RosterError as error:
        return _classified_failure(
            claim, scope, error.code, db_path=db_path, now=clock(), pages=pages
        )
    except TikHubScanError as error:
        with connect(db_path) as connection, transaction(connection):
            state = _owned_scope(connection, claim, scope)
            has_raw = state.get("pending_raw") is not None
        return _classified_failure(
            claim,
            scope,
            error.reason,
            db_path=db_path,
            now=clock(),
            pages=pages,
            has_raw=has_raw,
        )
    except RawResponseIntegrityError:
        return _finish_terminal(
            claim,
            "raw_integrity_error",
            "integrity",
            db_path=db_path,
            now=clock(),
            pages=pages,
        )
    except SlotUnavailable as error:
        return _classified_failure(
            claim, scope, error.error_code, db_path=db_path, now=clock(), pages=pages
        )
    except CaptureError as error:
        if error.error_code in {"cursor_expired", "invalid_cursor"}:
            # Restarting at cursor zero changes the slot generation but not the
            # paid requests already sent for this frozen scan.  A later retry
            # must consume a compensation authorization and sequence instead
            # of disguising the same pages as a fresh sequence-zero purchase.
            return _finish_terminal(
                claim,
                "cursor_expired",
                "provider_transient",
                db_path=db_path,
                now=clock(),
                pages=pages,
            )
        return _classified_failure(
            claim,
            scope,
            error.error_code,
            db_path=db_path,
            now=clock(),
            pages=pages,
            delay=error.retry_after_seconds or 300,
            http_status=error.http_status,
            has_raw=error.raw_response is not None,
            attempts_hint=getattr(error, "scan_attempt_count", None),
        )


def run_account_scan(
    identity_id: int, *, window_start: str, window_end: str, purpose: str,
    roster_snapshot_id: int, roster_snapshot_hash: str, db_path: Path = DEFAULT_DB,
    activation_id: int | None = None, profile_id: str | None = None,
    activation_sha256: str | None = None,
    task_id: str | None = None, task_max_amount: float | None = None,
    max_pages: int = PAGE_LIMIT, raw_root: Path | None = None,
    now: str | None = None, call_override: PageCall | None = None,
) -> dict[str, Any]:
    """Freeze an accepted member/window, then process at most twenty list pages.

    ``complete`` means provider exhaustion or the v2 two-page non-pinned range
    proof, not that quarantined records became valid content or recall is 100%.
    A supplied task_id shares its cumulative task ceiling across scan windows.
    """
    scope = _freeze(
        identity_id, window_start=window_start, window_end=window_end, purpose=purpose,
        roster_snapshot_id=roster_snapshot_id, roster_snapshot_hash=roster_snapshot_hash,
        db_path=db_path, task_id=task_id, task_max_amount=task_max_amount,
        activation_id=activation_id, profile_id=profile_id,
        activation_sha256=activation_sha256, at=now,
    )
    return _run(scope, db_path=db_path, max_pages=max_pages, raw_root=raw_root,
                now=now, call_override=call_override)


def _local_materialization_result(
    result: Mapping[str, Any], progress: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        **dict(result),
        "processed_items": int(progress["processed_items"]),
        "succeeded_items": int(progress["succeeded_items"]),
        "failed_items": int(progress["failed_items"]),
        "remaining_items": int(progress["remaining_items"]),
        "next_item_offset": int(progress["next_item_offset"]),
        "materialization_finalized": bool(progress["finalized"]),
        "deadline_reached": bool(progress["deadline_reached"]),
    }


def resume_local_materialization(
    scheduler_run_id: int,
    *,
    db_path: Path = DEFAULT_DB,
    raw_root: Path | None = None,
    now: str | None = None,
    max_items: int = MATERIALIZATION_ITEM_LIMIT,
    deadline: float,
) -> dict[str, Any]:
    """Resume only an existing v2 local child, never provider discovery.

    ``deadline`` is an absolute monotonic timestamp shared by the caller.  The
    entry point stops taking new content before it and never accepts a provider
    cursor, request callback, or replacement scope.
    """
    started = _monotonic_now()
    if type(scheduler_run_id) is not int or scheduler_run_id <= 0:
        raise TikHubScanError(
            "scan_contract_mismatch", "scheduler_run_id must identify one frozen scan",
        )
    if type(max_items) is not int or not 1 <= max_items <= MATERIALIZATION_ITEM_LIMIT:
        raise TikHubScanError(
            "invalid_materialization_limit", "max_items must be an integer from 1 through 50",
        )
    if (
        not isinstance(deadline, (int, float))
        or isinstance(deadline, bool)
        or not math.isfinite(float(deadline))
        or float(deadline) - started > MATERIALIZATION_DEADLINE_SECONDS
    ):
        raise TikHubScanError(
            "invalid_materialization_deadline",
            "The monotonic deadline must be finite and no more than 60 seconds away",
        )
    run, scope, pending = _local_materialization_preflight(
        scheduler_run_id, db_path=db_path,
    )
    token = _LOCAL_REPLAY.set((str(db_path.resolve()), scheduler_run_id, _digest(scope)))
    try:
        return _resume_validated_local_materialization(
            run, scope, pending, db_path=db_path, raw_root=raw_root,
            now=now, max_items=max_items, deadline=deadline,
        )
    finally:
        _LOCAL_REPLAY.reset(token)


def _claim_local_materialization_parent(
    run: Mapping[str, Any], scope: Mapping[str, Any], *, db_path: Path, now: str,
) -> DurableClaim | None:
    """Recover only a proven pending raw rejected by the old epoch gate.

    The old failed attempt stays immutable. Only the mutable parent cache is
    fenced and claimed into a new local-only attempt in one transaction.
    """
    with connect(db_path) as connection, transaction(connection):
        current = connection.execute("SELECT * FROM scheduler_runs WHERE id=?", (run["id"],)).fetchone()
        if current is None or json.loads(current["details_json"]) != run["details"]:
            raise LostOwnership("Local materialization parent changed before claim")
        if current["status"] == "failed":
            details = json.loads(current["details_json"])
            prior = connection.execute(
                "SELECT * FROM scheduler_run_attempts WHERE id=? AND scheduler_run_id=?",
                (details["owner"]["attempt_id"], run["id"]),
            ).fetchone()
            if (
                details.get("summary", {}).get("reason") != "profile_superseded"
                or details.get("complete") is not False
                or not isinstance(details["checkpoint"].get("pending_materialization"), dict)
                or prior is None or prior["status"] != "failed"
                or prior["details_json"] != current["details_json"]
            ):
                raise TikHubScanError("materialization_parent_not_retryable", "Failed parent has no recoverable local epoch debt")
            details["local_materialization_recovery"] = {
                "prior_attempt_id": prior["id"], "prior_details_sha256": _digest(run["details"]),
                "reason": "historical_raw_epoch_gate", "recovered_at": now,
            }
            connection.execute(
                "UPDATE scheduler_runs SET status='interrupted',details_json=? WHERE id=? AND status='failed'",
                (json.dumps(details, sort_keys=True, separators=(",", ":")), run["id"]),
            )
        return claim_run_in_transaction(
            connection, str(run["job_id"]), scope, invocation_source="operator_retry", now=now,
        )


def _resume_validated_local_materialization(
    run: dict[str, Any], scope: dict[str, Any], pending: dict[str, Any], *,
    db_path: Path, raw_root: Path | None, now: str | None, max_items: int, deadline: float,
) -> dict[str, Any]:
    scheduler_run_id = int(run["id"])
    indexes = pending["identity"]["eligible_indexes"]
    progress: dict[str, Any] = {
        "processed_items": 0,
        "succeeded_items": 0,
        "failed_items": 0,
        "remaining_items": len(indexes),
        "next_item_offset": 0,
        "finalized": False,
        "deadline_reached": False,
    }
    child = _materialization_run(pending["identity"], db_path=db_path)
    if child is not None:
        child_state = child["details"].get("checkpoint", {})
        offset = child_state.get("next_item_offset", 0)
        if type(offset) is int and 0 <= offset <= len(indexes):
            progress["next_item_offset"] = offset
            progress["remaining_items"] = len(indexes) - offset

    # Manifest/raw preflight is deliberately local but can still consume the
    # caller's shared deadline.  Recheck before creating any parent/child claim.
    if _monotonic_now() >= float(deadline):
        progress["deadline_reached"] = True
        return _local_materialization_result(
            {
                **_result(scheduler_run_id, db_path=db_path),
                "reason": "local_materialization_yield",
            },
            progress,
        )

    def clock() -> str:
        return _timestamp(now or now_utc())

    claim = _claim_local_materialization_parent(
        run, scope, db_path=db_path, now=clock(),
    )
    if claim is None:
        return _local_materialization_result(
            _result(scheduler_run_id, db_path=db_path), progress,
        )
    try:
        state = _state(claim, scope, db_path)
        if state.get("pending_materialization") != pending:
            raise TikHubScanError(
                "materialization_integrity_error", "Pending materialization changed after claim",
            )
        _materialize_pending(
            claim,
            scope,
            pending,
            db_path=db_path,
            raw_root=raw_root,
            now=clock(),
            max_items=max_items,
            deadline=float(deadline),
            progress=progress,
        )
        state = _state(claim, scope, db_path)
        if state.get("pending_materialization") is not None:
            result = _partial(
                claim,
                "local_materialization_yield",
                db_path=db_path,
                now=clock(),
                pages=0,
                delay=1,
            )
        elif state["complete"]:
            result = _finish_success(
                claim,
                str(state["completion_reason"]),
                db_path=db_path,
                now=clock(),
                pages=0,
            )
        else:
            result = _partial(
                claim,
                "materialization_replay_yield",
                db_path=db_path,
                now=clock(),
                pages=0,
                delay=1,
            )
    except LostOwnership:
        result = {
            **_result(scheduler_run_id, db_path=db_path),
            "reason": "attempt_owner_lost",
        }
    except BudgetBlocked as error:
        result = _classified_failure(
            claim,
            scope,
            getattr(error, "error_code", "budget_blocked"),
            db_path=db_path,
            now=clock(),
            pages=0,
        )
    except RosterError as error:
        result = _classified_failure(
            claim, scope, error.code, db_path=db_path, now=clock(), pages=0,
        )
    except TikHubScanError as error:
        if error.reason in {"materialization_failed", "materialization_pending"}:
            result = _partial(
                claim,
                error.reason,
                db_path=db_path,
                now=clock(),
                pages=0,
                delay=1,
            )
        else:
            result = _classified_failure(
                claim, scope, error.reason, db_path=db_path, now=clock(), pages=0,
            )
    except RawResponseIntegrityError:
        result = _finish_terminal(
            claim,
            "raw_integrity_error",
            "integrity",
            db_path=db_path,
            now=clock(),
            pages=0,
        )
    return _local_materialization_result(result, progress)


def resume_account_scan(
    scheduler_run_id: int, *, db_path: Path = DEFAULT_DB, max_pages: int = PAGE_LIMIT,
    raw_root: Path | None = None, now: str | None = None, call_override: PageCall | None = None,
) -> dict[str, Any]:
    """Resume the stored identity and budget; callers cannot replace their scope."""
    run = get_run(scheduler_run_id, db_path=db_path)
    scope = run["details"].get("identity", {})
    if (scope.get("contract_version") not in SUPPORTED_CONTRACT_VERSIONS
            or run["job_id"] != _job_id(str(scope.get("purpose")))):
        raise TikHubScanError("scan_contract_mismatch", "This is not a frozen TikHub scan")
    return _run(scope, db_path=db_path, max_pages=max_pages, raw_root=raw_root,
                now=now, call_override=call_override)
