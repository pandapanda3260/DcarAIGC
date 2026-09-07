"""Raw-first cross-account Matrix pages with durable fenced checkpoints."""

from __future__ import annotations

from . import durable_runs

import hashlib
import json
import os
import sqlite3
import stat
import threading
import time
import uuid
from collections import deque
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterator, Literal, Mapping

from .account_metrics import persist_account_metric_observation
from .account_roster import current_snapshot
from .capture import CaptureError, RAW_ROOT
from .durable_runs import (
    DurableClaim, LostOwnership, assert_owner, checkpoint,
    claim_run_in_transaction, finish_run, get_run, scan_identity,
)
from .metric_observations import persist_metric_observation
from .newrank_matrix import (
    ACCOUNTS_PATH, WORKS_PATH, MatrixConfigurationError, MatrixPage, MatrixRowError, NewrankMatrixClient,
    SHANGHAI, beijing_query_bounds, normalize_account, normalize_work,
)
from .operations import OperationError, content_identity, upsert_content
from .paid_dispatch import (
    close_dispatch_not_sent_in_transaction,
    finish_dispatch_in_transaction,
    mark_dispatch_sent_in_transaction,
    reserve_dispatch_in_transaction,
    supports_dispatch_ledger,
)
from .paid_drain import (
    PaidDrainBlocked,
    require_paid_dispatch_open,
)
from .source_routing import parse_time
from .storage import (
    DEFAULT_DB,
    connect,
    now_utc,
    transaction,
    transaction_metrics_context,
)

CONTRACT_VERSION = "matrix-scan-v1"
DEFAULT_RAW_ROOT = RAW_ROOT / "matrix_scans"
DISPOSITIONS = ("known", "new", "quarantined", "unparseable")
AUTOMATIC_PURPOSES = frozenset({
    "daily-works",
    "incremental-works",
    "daily-account-metrics",
})


class MatrixScanError(RuntimeError):
    pass


def _automatic_business_day(spec: Mapping[str, Any]) -> date | None:
    if spec.get("purpose") not in AUTOMATIC_PURPOSES:
        return None
    if spec.get("kind") == "works":
        return parse_time(str(spec["overall_end_at"])).astimezone(SHANGHAI).date()
    return date.fromisoformat(str(spec["rank_date"])) + timedelta(days=1)


def _request_business_day(spec: Mapping[str, Any], at: str) -> str:
    frozen = _automatic_business_day(spec)
    return (
        frozen
        if frozen is not None
        else parse_time(at).astimezone(SHANGHAI).date()
    ).isoformat()


def _assert_request_profile(
    connection: sqlite3.Connection,
    spec: Mapping[str, Any],
    *,
    at: str,
) -> None:
    if "activation_id" not in spec:
        return
    from .account_roster import runtime_snapshot
    from .profile_activations import MATRIX_PROFILE, activation_at

    active = activation_at(connection, at)
    if active is None:
        raise MatrixScanError("profile_superseded")
    if active["profile_id"] != MATRIX_PROFILE:
        raise MatrixScanError("profile_not_scheduled")
    if int(active["activation_id"]) != int(spec["activation_id"]):
        raise MatrixScanError("profile_superseded")
    roster = runtime_snapshot(connection, active)
    if (
        roster["id"] != spec["roster_snapshot_id"]
        or roster["members_sha256"] != spec["roster_snapshot_hash"]
    ):
        raise MatrixScanError("profile_superseded")


def _dispatch_scope(claim: DurableClaim, spec: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "contract_version": CONTRACT_VERSION,
        "scan_id": claim.scan_id,
        "scan": dict(spec),
    }


def _cursor_identity(claim: DurableClaim, state: Mapping[str, Any]) -> dict[str, Any]:
    cursor = state.get("next_cursor")
    return {
        "scan_id": claim.scan_id,
        "page_index": int(state["page_index"]),
        "request_number": int(state.get("network_requests", 0)) + 1,
        "request_cursor": cursor,
        "request_cursor_sha256": _cursor_hash(cursor),
    }


def _reserve_paid_request(
    connection: sqlite3.Connection,
    claim: DurableClaim,
    spec: Mapping[str, Any],
    state: Mapping[str, Any],
    *,
    operation: str,
    business_day: str,
    at: str,
) -> str | None:
    assert_owner(connection, claim)
    _assert_request_profile(connection, spec, at=at)
    require_paid_dispatch_open(
        connection,
        provider="newrank_matrix",
        operation=operation,
        at=at,
    )
    if not supports_dispatch_ledger(connection):
        return None
    if "activation_id" not in spec:
        raise MatrixScanError("dispatch_activation_required")
    reserved = reserve_dispatch_in_transaction(
        connection,
        provider="newrank_matrix",
        operation=operation,
        activation_id=int(spec["activation_id"]),
        business_day=business_day,
        scheduler_run_id=claim.scheduler_run_id,
        scheduler_attempt_id=claim.attempt_id,
        scope=_dispatch_scope(claim, spec),
        cursor_identity=_cursor_identity(claim, state),
        created_at=at,
    )
    if reserved is None:
        raise MatrixScanError("dispatch_reservation_missing")
    return reserved.dispatch_id


def _settle_aborted_dispatch(
    dispatch_id: str | None,
    *,
    sent: bool,
    error: Exception,
    db_path: Path,
    at: str | None = None,
) -> None:
    if dispatch_id is None:
        return
    if isinstance(error, LostOwnership):
        reason = "owner_lost"
    elif isinstance(error, PaidDrainBlocked):
        reason = error.error_code
    elif isinstance(error, MatrixConfigurationError):
        reason = "matrix_configuration_invalid"
    else:
        reason = str(error) or type(error).__name__
    settled_at = at or now_utc()
    with connect(db_path) as connection, transaction(connection):
        if sent:
            finish_dispatch_in_transaction(
                connection,
                dispatch_id,
                outcome=(
                    "failed"
                    if isinstance(error, MatrixConfigurationError)
                    else "billing_unknown"
                ),
                reason=reason,
                created_at=settled_at,
            )
        else:
            close_dispatch_not_sent_in_transaction(
                connection,
                dispatch_id,
                reason=reason,
                created_at=settled_at,
            )


class MatrixRateLimiter:
    """One writer process shares two slots and a two-starts/second limit."""

    def __init__(self, *, clock=time.monotonic, sleeper=time.sleep) -> None:
        self._clock = clock
        self._sleep = sleeper
        self._slots = threading.BoundedSemaphore(2)
        self._lock = threading.Lock()
        self._starts: deque[float] = deque()

    @contextmanager
    def slot(self) -> Iterator[None]:
        with self._slots:
            with self._lock:
                while True:
                    now = self._clock()
                    while self._starts and now - self._starts[0] >= 1:
                        self._starts.popleft()
                    if len(self._starts) < 2:
                        self._starts.append(now)
                        break
                    self._sleep(max(0.001, 1 - (now - self._starts[0])))
            yield


RATE_LIMITER = MatrixRateLimiter()


def _bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, ensure_ascii=False,
                       separators=(",", ":"), allow_nan=False) + "\n").encode()


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _cursor_hash(value: Any) -> str:
    return _sha(_bytes(value))


def _root(path: Path) -> Path:
    if ".." in path.parts:
        raise MatrixScanError("unsafe_artifact_root")
    path = path.absolute()
    for candidate in (path, *path.parents):
        if candidate.is_symlink():
            raise MatrixScanError("artifact_symlink")
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    if not path.is_dir():
        raise MatrixScanError("artifact_root_not_directory")
    return path


def _write_blob(root: Path, value: Mapping[str, Any], suffix: str) -> dict[str, Any]:
    root = _root(root)
    payload = _bytes(value)
    digest = _sha(payload)
    target = root / (digest + suffix)
    temporary = root / ("." + uuid.uuid4().hex + ".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, target)
        except FileExistsError:
            pass
    finally:
        temporary.unlink(missing_ok=True)
    directory = os.open(root, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    reference = {"path": str(target), "sha256": digest, "byte_size": len(payload)}
    _read_blob(reference)
    return reference


def _read_blob(reference: Mapping[str, Any]) -> dict[str, Any]:
    from .artifact_paths import resolve
    path = resolve(str(reference["path"]))
    for candidate in (path, *path.parents):
        if candidate.is_symlink():
            raise MatrixScanError("artifact_symlink")
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise MatrixScanError("artifact_not_regular")
            payload = stream.read()
        if len(payload) != reference["byte_size"] or _sha(payload) != reference["sha256"]:
            raise MatrixScanError("raw_hash_mismatch")
        value = json.loads(payload)
    except (OSError, ValueError, KeyError) as error:
        raise MatrixScanError("raw_unavailable") from error
    if not isinstance(value, dict):
        raise MatrixScanError("raw_invalid_envelope")
    return value


def scan_spec(
    kind: str,
    platform: str,
    *,
    purpose: str,
    db_path: Path = DEFAULT_DB,
    start_at: str | None = None,
    end_at: str | None = None,
    rank_date: str | None = None,
    roster_snapshot_id: int | None = None,
    roster_snapshot_hash: str | None = None,
    activation_id: int | None = None,
    profile_id: str | None = None,
    at: str | None = None,
    overall_start_at: str | None = None,
    overall_end_at: str | None = None,
) -> dict[str, Any]:
    if kind not in {"works", "accounts"} or platform not in {"douyin", "xiaohongshu"} or not purpose:
        raise MatrixScanError("invalid_scan_scope")
    with connect(db_path) as connection:
        has_profiles = connection.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type='table' AND name='acquisition_profile_activations'"
        ).fetchone() is not None
        active: dict[str, Any] | None = None
        roster: dict[str, Any] | None
        if has_profiles:
            from .account_roster import runtime_snapshot
            from .profile_activations import (
                MATRIX_PROFILE,
                activation_at,
                activation_by_id,
            )

            effective = activation_at(connection, at or now_utc())
            if effective is None:
                if connection.execute(
                    "SELECT 1 FROM account_roster_snapshots LIMIT 1"
                ).fetchone() is None:
                    raise MatrixScanError("roster_not_ready")
                raise MatrixScanError("roster_activation_required")
            active = (
                activation_by_id(connection, activation_id)
                if activation_id is not None
                else effective
            )
            if active.get("cancellation") is not None:
                raise MatrixScanError("profile_superseded")
            if int(active["activation_id"]) != int(effective["activation_id"]):
                raise MatrixScanError("profile_superseded")
            if active["profile_id"] != MATRIX_PROFILE:
                raise MatrixScanError("profile_not_scheduled")
            if profile_id is not None and profile_id != active["profile_id"]:
                raise MatrixScanError("profile_scope_mismatch")
            roster = runtime_snapshot(connection, active)
        else:
            roster = current_snapshot(connection)
        if roster is None:
            raise MatrixScanError("roster_not_ready")
        if roster_snapshot_id is not None and roster_snapshot_id != roster["id"]:
            if has_profiles:
                raise MatrixScanError("profile_superseded")
            row = connection.execute(
                "SELECT * FROM account_roster_snapshots WHERE id=?", (roster_snapshot_id,),
            ).fetchone()
            roster = dict(row) if row else None
        if roster is None:
            raise MatrixScanError("roster_not_ready")
        if roster_snapshot_hash is not None and roster_snapshot_hash != roster["members_sha256"]:
            raise MatrixScanError("roster_hash_mismatch")
    result: dict[str, Any] = {
        "contract_version": CONTRACT_VERSION, "provider": "newrank_matrix",
        "kind": kind, "purpose": purpose, "platform": platform,
        "roster_snapshot_id": int(roster["id"]), "roster_snapshot_hash": roster["members_sha256"],
        "roster_scope_key": roster["scope_key"],
    }
    if active is not None:
        result.update(
            activation_id=int(active["activation_id"]),
            profile_id=str(active["profile_id"]),
        )
    if kind == "works":
        if start_at is None or end_at is None:
            raise MatrixScanError("work_window_required")
        beijing_query_bounds(start_at, end_at)
        result.update(
            start_at=parse_time(start_at).isoformat(),
            end_at=parse_time(end_at).isoformat(),
            overall_start_at=parse_time(overall_start_at or start_at).isoformat(),
            overall_end_at=parse_time(overall_end_at or end_at).isoformat(),
        )
        if not (parse_time(result["overall_start_at"]) <= parse_time(start_at)
                < parse_time(end_at) <= parse_time(result["overall_end_at"])):
            raise MatrixScanError("window_outside_frozen_recovery_scope")
    else:
        if rank_date is None or date.fromisoformat(rank_date).isoformat() != rank_date:
            raise MatrixScanError("account_statistics_day_required")
        result["rank_date"] = rank_date
    return result


def _identity(connection: sqlite3.Connection, platform: str, uid: str | None) -> sqlite3.Row | None:
    if not uid:
        return None
    return connection.execute(
        "SELECT i.*,a.enabled FROM account_platform_identities i JOIN accounts a ON a.id=i.account_id "
        "WHERE i.platform=? AND i.uid=?", (platform, uid),
    ).fetchone()


def _managed_at(
    connection: sqlite3.Connection, identity_id: int, published_at: str | None, spec: Mapping[str, Any]
) -> bool:
    frozen_id = spec["roster_snapshot_id"]
    if connection.execute(
        "SELECT 1 FROM account_roster_members WHERE snapshot_id=? AND account_identity_id=?",
        (frozen_id, identity_id),
    ).fetchone():
        return True
    if not published_at:
        return False
    snapshots = connection.execute(
        "SELECT s.id,s.accepted_at,m.account_identity_id FROM account_roster_snapshots s "
        "LEFT JOIN account_roster_members m ON m.snapshot_id=s.id AND m.account_identity_id=? "
        "WHERE s.scope_key=? AND s.id<=? ORDER BY s.id",
        (identity_id, spec["roster_scope_key"], frozen_id),
    ).fetchall()
    managed = False
    interval_start = None
    published = parse_time(published_at)
    for index, snapshot in enumerate(snapshots):
        present = snapshot["account_identity_id"] is not None
        if present and not managed:
            interval_start = parse_time(spec["overall_start_at"]) if index == 0 else parse_time(snapshot["accepted_at"])
        if managed and not present and interval_start is not None:
            if interval_start <= published < parse_time(snapshot["accepted_at"]):
                return True
        managed = present
    return False


def _expected_query(spec: Mapping[str, Any], cursor: Any) -> dict[str, Any]:
    query: dict[str, Any] = {"pageSize": 100, "platType": 2 if spec["platform"] == "douyin" else 6}
    if cursor is not None:
        query["scrollId"] = cursor
    if spec["kind"] == "works":
        query["startDate"], query["endDate"] = beijing_query_bounds(spec["start_at"], spec["end_at"])
    else:
        query["rankData"] = spec["rank_date"]
    return query


def _register_response(
    claim: DurableClaim, spec: Mapping[str, Any], state: Mapping[str, Any],
    *, page: MatrixPage | None, error: CaptureError | None,
    dispatch_id: str | None,
    dispatch_outcome: Literal["succeeded", "failed", "billing_unknown"],
    db_path: Path, raw_root: Path,
) -> int:
    expected = _expected_query(spec, state["next_cursor"])
    path_name = WORKS_PATH if spec["kind"] == "works" else ACCOUNTS_PATH
    if page is not None and (page.query != expected or page.path_name != path_name):
        raise MatrixScanError("response_query_mismatch")
    envelope = {
        "contract_version": CONTRACT_VERSION, "scan_id": claim.scan_id,
        "page_index": state["page_index"], "request_cursor": state["next_cursor"],
        "request_number": state.get("network_requests", 0) + 1,
        "query": expected, "path_name": path_name,
        "captured_at": page.captured_at if page else now_utc(),
        "http_status": page.http_status if page else error.http_status if error else None,
        "response": page.raw_response if page else error.raw_response if error else None,
        "error_code": error.error_code if error else None,
    }
    reference = _write_blob(raw_root / claim.scan_id, envelope, ".response.json")
    registered_at = now_utc()
    with connect(db_path) as connection, transaction(connection):
        details = assert_owner(connection, claim)
        latest = details["checkpoint"]
        if (latest["page_index"] != state["page_index"]
                or latest["next_cursor"] != state["next_cursor"] or latest.get("pending_page")):
            raise LostOwnership("response no longer belongs to the active page")
        operation = "matrix_works_list" if spec["kind"] == "works" else "matrix_account_list"
        existing = connection.execute(
            "SELECT * FROM provider_raw_responses WHERE local_path=? AND sha256=? AND provider='newrank_matrix'",
            (reference["path"], reference["sha256"]),
        ).fetchone()
        if existing is None:
            raw = connection.execute(
                "INSERT INTO provider_raw_responses(provider,operation,local_path,sha256,byte_size,"
                "http_status,captured_at,source) VALUES ('newrank_matrix',?,?,?,?,?,?,?)",
                (operation, reference["path"], reference["sha256"], reference["byte_size"],
                 envelope["http_status"], envelope["captured_at"], "matrix_page_pending" if page else "matrix_scan_error"),
            )
            raw_id = int(raw.lastrowid or 0)
        else:
            raw_id = int(existing["id"])
        changes: dict[str, Any] = {"network_requests": latest.get("network_requests", 0) + 1}
        if page is not None:
            changes["pending_page"] = {"raw_id": raw_id, **reference}
        else:
            changes["last_error"] = {"raw_id": raw_id, "reason": envelope["error_code"], **reference}
        checkpoint(connection, claim, changes)
        if dispatch_id is not None:
            finish_dispatch_in_transaction(
                connection,
                dispatch_id,
                outcome=dispatch_outcome,
                raw_response_id=raw_id,
                reason=envelope["error_code"],
                created_at=registered_at,
            )
    return raw_id


def _manifest_chain(reference: Mapping[str, Any] | None) -> Iterator[dict[str, Any]]:
    visited: set[str] = set()
    while reference:
        digest = str(reference["sha256"])
        if digest in visited:
            raise MatrixScanError("manifest_cycle")
        visited.add(digest)
        manifest = _read_blob(reference)
        yield manifest
        reference = manifest.get("previous")


def read_manifests(run_id: int, *, db_path: Path = DEFAULT_DB) -> list[dict[str, Any]]:
    state = get_run(run_id, db_path=db_path)["details"]["checkpoint"]
    return list(reversed(list(_manifest_chain(state.get("last_manifest")))))


def _work_row(
    connection: sqlite3.Connection, item: Any, spec: Mapping[str, Any], raw: Mapping[str, Any],
    *, index: int, seen: set[str], db_path: Path,
) -> dict[str, Any]:
    try:
        normalized = normalize_work(item, platform=spec["platform"])
    except MatrixRowError as error:
        return {"index": index, "disposition": "unparseable", "reason": error.reason}
    work_id = normalized["platform_content_id"]
    result: dict[str, Any] = {"index": index, "platform_content_id": work_id}
    if work_id in seen:
        return {**result, "disposition": "known", "reason": "duplicate_in_page"}
    seen.add(work_id)
    existing = connection.execute(
        "SELECT * FROM content_items WHERE platform=? AND platform_content_id=?",
        (spec["platform"], work_id),
    ).fetchone()
    identity = _identity(connection, spec["platform"], normalized["account_uid"])
    if existing is not None:
        result.update(disposition="known", content_id=int(existing["id"]))
        if (identity is None or
                (existing["raw_account_uid"] and existing["raw_account_uid"] != normalized["account_uid"])
                or existing["account_id"] not in (None, identity["account_id"])):
            return {**result, "reason": "existing_identity_unverified"}
    else:
        if identity is None:
            return {**result, "disposition": "quarantined", "reason": "identity_unmatched"}
        published = normalized["published_at"]
        if published and not (
            parse_time(spec["overall_start_at"]) <= parse_time(published) < parse_time(spec["overall_end_at"])
        ):
            return {**result, "disposition": "quarantined", "reason": "outside_frozen_scope"}
        if not _managed_at(connection, int(identity["id"]), published, spec):
            return {**result, "disposition": "quarantined", "reason": "outside_management_window"}
        result["disposition"] = "new"
    try:
        content_identity(spec["platform"], normalized["canonical_url"], work_id)
    except OperationError:
        return {**result, "disposition": "known" if existing is not None else "unparseable",
                "reason": "invalid_content_identity"}
    content = upsert_content(
        {**normalized, "_preserve_existing_content_fields": True},
        connection=connection, db_path=db_path,
        source_group_on_insert="history-backfill" if str(spec["purpose"]).startswith("history") else "",
    )
    result["content_id"] = int(content["id"])
    result["reason"] = "registered" if normalized["published_at"] else "publication_time_pending"
    metric_window = parse_time(str(raw["captured_at"])).astimezone(SHANGHAI).date().isoformat()
    metrics = normalized["metrics"]
    outcome = persist_metric_observation(
        connection, content_id=int(content["id"]), captured_at=str(raw["captured_at"]),
        window_key=metric_window, provider="newrank_matrix", platform=spec["platform"],
        raw_response_id=int(raw["id"]),
        status="available" if any(value is not None for value in metrics.values()) else "missing",
        metadata_json=json.dumps({
            "fields": normalized["field_status"], "operation": raw["operation"],
            "matrix_scan_id": spec.get("scan_id"), "row_index": index,
            "original_share_url": normalized["original_share_url"],
        }, sort_keys=True, ensure_ascii=False),
        **metrics,
    )
    result["observation_id"] = outcome.observation_id
    return result


def _account_row(
    connection: sqlite3.Connection, item: Any, spec: Mapping[str, Any], raw: Mapping[str, Any],
    *, index: int, seen: set[str],
) -> dict[str, Any]:
    try:
        normalized = normalize_account(item, platform=spec["platform"], rank_date=spec["rank_date"])
    except MatrixRowError as error:
        return {"index": index, "disposition": "unparseable", "reason": error.reason}
    uid = normalized["uid"]
    result: dict[str, Any] = {"index": index, "uid": uid}
    if uid in seen:
        return {**result, "disposition": "known", "reason": "duplicate_in_page"}
    seen.add(uid)
    identity = _identity(connection, spec["platform"], uid)
    if identity is None:
        return {**result, "disposition": "quarantined", "reason": "identity_unmatched"}
    if normalized["field_status"]["statistics_date"]["status"] != "provided":
        return {**result, "disposition": "unparseable", "reason": "statistics_date_mismatch"}
    fact = persist_account_metric_observation(
        connection, account_identity_id=int(identity["id"]), provider="newrank_matrix",
        raw_response_id=int(raw["id"]), normalized=normalized, captured_at=str(raw["captured_at"]),
    )
    return {**result, "disposition": "known", "reason": "account_metrics_only",
            "identity_id": int(identity["id"]), "observation_id": fact["id"]}


def apply_pending_page(
    claim: DurableClaim, *, db_path: Path = DEFAULT_DB, raw_root: Path = DEFAULT_RAW_ROOT
) -> dict[str, Any]:
    """The owner check, all row writes, manifest and cursor commit together."""
    with connect(db_path) as connection, transaction(connection):
        details = assert_owner(connection, claim)
        spec, state = {**details["identity"], "scan_id": claim.scan_id}, details["checkpoint"]
        pending = state.get("pending_page")
        if pending is None:
            return state
        envelope = _read_blob(pending)
        if (envelope["scan_id"] != claim.scan_id or envelope["page_index"] != state["page_index"]
                or envelope["query"] != _expected_query(spec, state["next_cursor"]) or envelope["error_code"]):
            raise MatrixScanError("pending_page_scope_mismatch")
        raw_row = connection.execute(
            "SELECT * FROM provider_raw_responses WHERE id=?", (pending["raw_id"],),
        ).fetchone()
        if (raw_row is None or raw_row["sha256"] != pending["sha256"]
                or raw_row["local_path"] != pending["path"] or raw_row["provider"] != "newrank_matrix"
                or raw_row["account_id"] is not None or raw_row["content_id"] is not None):
            raise MatrixScanError("pending_raw_identity_mismatch")
        raw = dict(raw_row)
        payload = envelope["response"]
        rows = payload.get("data")
        rows = json.loads(rows) if isinstance(rows, str) else rows
        if not isinstance(rows, list):
            raise MatrixScanError("pending_rows_invalid")
        next_cursor = rows[-1].get("scrollId") if rows and isinstance(rows[-1], dict) else None
        reason = None
        if rows and (not isinstance(next_cursor, list) or not next_cursor):
            reason = "missing_cursor"
        if rows and reason is None:
            seen_cursors = {_cursor_hash(state["next_cursor"])}
            for previous in _manifest_chain(state.get("last_manifest")):
                seen_cursors.add(_cursor_hash(previous["request_cursor"]))
            if _cursor_hash(next_cursor) in seen_cursors:
                reason = "repeated_cursor"
        total = payload.get("totalNum")
        if isinstance(total, str) and total.isdecimal():
            total = int(total)
        if total is not None:
            if type(total) is not int or total < 0:
                reason = "invalid_declared_total"
            elif state.get("declared_total") is not None and total != state["declared_total"]:
                reason = "declared_total_drift"
        seen: set[str] = set()
        dispositions = [
            _work_row(connection, item, spec, raw, index=index, seen=seen, db_path=db_path)
            if spec["kind"] == "works"
            else _account_row(connection, item, spec, raw, index=index, seen=seen)
            for index, item in enumerate(rows)
        ]
        counts = {name: sum(item["disposition"] == name for item in dispositions) for name in DISPOSITIONS}
        if sum(counts.values()) != len(rows):
            raise MatrixScanError("disposition_conservation_failed")
        complete = not rows and reason is None
        manifest = {
            "contract_version": CONTRACT_VERSION, "scan_id": claim.scan_id,
            "page_index": state["page_index"], "raw_id": int(raw["id"]), "raw_sha256": raw["sha256"],
            "captured_at": raw["captured_at"], "request_cursor": state["next_cursor"],
            "next_cursor": next_cursor, "declared_total": total,
            "row_count": len(rows), "counts": counts, "rows": dispositions,
            "reason": reason, "complete": complete, "previous": state.get("last_manifest"),
        }
        reference = _write_blob(raw_root / claim.scan_id, manifest, ".manifest.json")
        checkpoint(connection, claim, {
            "pending_page": None, "last_manifest": reference,
            "page_index": state["page_index"] + 1,
            "next_cursor": next_cursor if reason is None else state["next_cursor"],
            "counts": {name: state["counts"].get(name, 0) + counts[name] for name in DISPOSITIONS},
            "raw_row_count": state["raw_row_count"] + len(rows),
            "declared_total": total if total is not None else state.get("declared_total"),
            "blocked_reason": reason, "complete": complete,
        })
        connection.execute(
            "UPDATE provider_raw_responses SET source='matrix_page_applied' WHERE id=?",
            (raw["id"],),
        )
        return assert_owner(connection, claim)["checkpoint"]


def run_matrix_scan(
    kind: str,
    platform: str,
    *,
    purpose: str,
    db_path: Path = DEFAULT_DB,
    start_at: str | None = None,
    end_at: str | None = None,
    rank_date: str | None = None,
    roster_snapshot_id: int | None = None,
    roster_snapshot_hash: str | None = None,
    activation_id: int | None = None,
    profile_id: str | None = None,
    overall_start_at: str | None = None,
    overall_end_at: str | None = None,
    client: NewrankMatrixClient | None = None,
    raw_root: Path = DEFAULT_RAW_ROOT,
    max_pages: int = 20,
    rate_limiter: MatrixRateLimiter = RATE_LIMITER,
    now: str | None = None,
    invocation_source: str = "scheduled",
) -> dict[str, Any]:
    if type(max_pages) is not int or not 1 <= max_pages <= 20:
        raise MatrixScanError("max_pages_must_be_1_to_20")
    spec = scan_spec(
        kind, platform, purpose=purpose, db_path=db_path,
        start_at=start_at, end_at=end_at, rank_date=rank_date,
        roster_snapshot_id=roster_snapshot_id, roster_snapshot_hash=roster_snapshot_hash,
        activation_id=activation_id, profile_id=profile_id, at=now,
        overall_start_at=overall_start_at, overall_end_at=overall_end_at,
    )
    job_id = "matrix_works_scan" if kind == "works" else "matrix_account_metrics"
    operation = "matrix_works_list" if kind == "works" else "matrix_account_list"
    # The paid gate and durable claim share one BEGIN IMMEDIATE.  START can
    # therefore win before both (no paid child), or follow both and freeze this
    # exact running attempt; it can never split the gate from run creation.
    with transaction_metrics_context(job_id=job_id), connect(
        db_path
    ) as connection, transaction(connection):
        require_paid_dispatch_open(
            connection,
            provider="newrank_matrix",
            operation=operation,
            at=now,
        )
        claim = claim_run_in_transaction(
            connection,
            job_id,
            spec,
            now=now,
            invocation_source=invocation_source,
            initial_checkpoint={
                "complete": False, "page_index": 0, "next_cursor": None,
                "pending_page": None, "last_manifest": None,
                "counts": {name: 0 for name in DISPOSITIONS},
                "raw_row_count": 0, "network_requests": 0,
                "declared_total": None, "blocked_reason": None,
            },
        )
    if claim is None:
        scan_id = scan_identity(job_id, spec)
        with connect(db_path) as connection:
            row = connection.execute(
                "SELECT * FROM scheduler_runs WHERE job_id=? AND scheduled_for=?" + durable_runs.root_run_predicate(connection),
                (job_id, "scan:" + scan_id),
            ).fetchone()
        if row is None:
            raise MatrixScanError("claimed_run_missing")
        details = json.loads(row["details_json"])
        state = details["checkpoint"]
        return {
            **details.get("summary", {}), "status": row["status"],
            "scheduler_run_id": int(row["id"]), "scan_id": scan_id,
            "complete": state["complete"], "counts": state["counts"],
            "raw_row_count": state["raw_row_count"],
            "network_requests": state["network_requests"],
            "reason": "already_complete" if state["complete"] else
                "already_running" if row["status"] == "running" else "not_due",
        }

    def stop(
        reason: str | None,
        *,
        complete: bool = False,
        stopped_at: str | None = None,
    ) -> dict[str, Any]:
        timestamp = stopped_at or now or now_utc()
        state = get_run(claim.scheduler_run_id, db_path=db_path)["details"]["checkpoint"]
        summary = {
            "reason": reason, "counts": state["counts"], "raw_row_count": state["raw_row_count"],
            "network_requests": state["network_requests"], "provider_cost": None,
            "provider_cost_status": "unknown",
        }
        details = finish_run(
            claim, status="succeeded" if complete else "partial",
            db_path=db_path, summary=summary, now=timestamp,
            next_resume_at=None if complete else (parse_time(timestamp) + timedelta(minutes=1 if reason == "page_yield" else 30)).isoformat(),
        )
        return {"status": "succeeded" if complete else "partial",
                "scheduler_run_id": claim.scheduler_run_id, "scan_id": claim.scan_id,
                "complete": details["complete"], **summary}

    try:
        for _ in range(max_pages):
            with connect(db_path) as connection, transaction(connection):
                state = assert_owner(connection, claim)["checkpoint"]
            if state["complete"]:
                return stop(None, complete=True)
            if state.get("blocked_reason"):
                return stop(str(state["blocked_reason"]))
            if state.get("pending_page") is None:
                _root(raw_root / claim.scan_id)
                client = client or NewrankMatrixClient()
                for attempt in range(2):
                    reservation_now = now_utc()
                    frozen_business_day = _automatic_business_day(spec)
                    if (
                        frozen_business_day is not None
                        and parse_time(reservation_now).astimezone(SHANGHAI).date()
                        != frozen_business_day
                    ):
                        return stop(
                            "business_day_expired", stopped_at=reservation_now
                        )
                    business_day = _request_business_day(spec, reservation_now)
                    with transaction_metrics_context(
                        job_id=job_id,
                        scheduler_run_id=claim.scheduler_run_id,
                        attempt_id=claim.attempt_id,
                        phase="matrix_dispatch_reserve",
                    ), connect(db_path) as connection, transaction(connection):
                        dispatch_id = _reserve_paid_request(
                            connection,
                            claim,
                            spec,
                            state,
                            operation=operation,
                            business_day=business_day,
                            at=reservation_now,
                        )
                    sent = False
                    try:
                        with rate_limiter.slot():
                            live_now = now_utc()
                            if (
                                frozen_business_day is not None
                                and parse_time(live_now).astimezone(SHANGHAI).date()
                                != frozen_business_day
                            ):
                                expired = MatrixScanError("business_day_expired")
                                _settle_aborted_dispatch(
                                    dispatch_id,
                                    sent=False,
                                    error=expired,
                                    db_path=db_path,
                                    at=live_now,
                                )
                                dispatch_id = None
                                return stop("business_day_expired", stopped_at=live_now)
                            # This is the final linearization point after the
                            # shared rate limiter and immediately before the
                            # provider call.  START and this gate both use
                            # BEGIN IMMEDIATE: START first means zero network;
                            # this gate first makes this one request part of
                            # START's frozen in-flight Matrix attempt.
                            with transaction_metrics_context(
                                job_id=job_id,
                                scheduler_run_id=claim.scheduler_run_id,
                                attempt_id=claim.attempt_id,
                                phase="matrix_final_paid_gate",
                            ), connect(db_path) as connection, transaction(connection):
                                assert_owner(connection, claim)
                                _assert_request_profile(connection, spec, at=live_now)
                                require_paid_dispatch_open(
                                    connection,
                                    provider="newrank_matrix",
                                    operation=operation,
                                    at=live_now,
                                )
                                if dispatch_id is not None:
                                    mark_dispatch_sent_in_transaction(
                                        connection,
                                        dispatch_id,
                                        fetch_attempt_id=None,
                                        created_at=live_now,
                                    )
                            sent = True
                            page = (
                                client.fetch_works_page(platform, spec["start_at"], spec["end_at"], scroll_id=state["next_cursor"])
                                if kind == "works" else
                                client.fetch_accounts_page(platform, spec["rank_date"], scroll_id=state["next_cursor"])
                            )
                    except CaptureError as error:
                        try:
                            _register_response(
                                claim,
                                spec,
                                state,
                                page=None,
                                error=error,
                                dispatch_id=dispatch_id,
                                dispatch_outcome=(
                                    "failed"
                                    if error.billed is not None
                                    else "billing_unknown"
                                ),
                                db_path=db_path,
                                raw_root=raw_root,
                            )
                        except Exception as registration_error:
                            _settle_aborted_dispatch(
                                dispatch_id,
                                sent=sent,
                                error=registration_error,
                                db_path=db_path,
                            )
                            raise
                        dispatch_id = None
                        if not error.retryable or attempt == 1:
                            return stop(error.error_code)
                        with connect(db_path) as connection, transaction(connection):
                            state = assert_owner(connection, claim)["checkpoint"]
                    except Exception as error:
                        _settle_aborted_dispatch(
                            dispatch_id,
                            sent=sent,
                            error=error,
                            db_path=db_path,
                        )
                        raise
                    else:
                        try:
                            _register_response(
                                claim,
                                spec,
                                state,
                                page=page,
                                error=None,
                                dispatch_id=dispatch_id,
                                dispatch_outcome="succeeded",
                                db_path=db_path,
                                raw_root=raw_root,
                            )
                        except Exception as registration_error:
                            _settle_aborted_dispatch(
                                dispatch_id,
                                sent=sent,
                                error=registration_error,
                                db_path=db_path,
                            )
                            raise
                        dispatch_id = None
                        break
            state = apply_pending_page(claim, db_path=db_path, raw_root=raw_root)
            if state["complete"]:
                return stop(None, complete=True)
            if state.get("blocked_reason"):
                return stop(str(state["blocked_reason"]))
        return stop("page_yield")
    except LostOwnership:
        return {"status": "interrupted", "reason": "owner_lost",
                "scheduler_run_id": claim.scheduler_run_id, "complete": False}
    except PaidDrainBlocked as error:
        return stop(error.error_code)
    except MatrixScanError as error:
        return stop(str(error))
    except MatrixConfigurationError:
        return stop("matrix_configuration_invalid")
