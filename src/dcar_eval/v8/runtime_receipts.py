"""Runtime receipts for verified scans and profile-day coverage.

Schema 18 stores an atomic scheduler-run bridge. Schema 19 writes that bridge
and its native receipt row in the same transaction. Full evidence remains in a
private append-only file; hot readers use only bound, self-hashed DB summaries.
"""

from __future__ import annotations

from . import durable_runs

import hashlib
import json
import os
import sqlite3
import stat
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo

from .reconcile_control import ReconcileBudget, current_reconcile_budget
from .source_routing import parse_time
from .storage import DEFAULT_DB, connect, is_formal_database_path, now_utc, transaction


BEIJING = ZoneInfo("Asia/Shanghai")
BRIDGE_CONTRACT = "schema18-runtime-receipt-bridge-v2"
SCAN_RECEIPT_JOB = "scan_verification_receipt_v2"
DAY_RECEIPT_JOB = "profile_day_coverage_receipt_v2"
PIPELINE_ACTIVATION_JOB = "matrix_pipeline_activation"
PIPELINE_CONTRACT = "matrix-first-pipeline-v1"
MODE_A_PROFILE = "mode_a_matrix_primary"
PROFILE_DAY_CONTRACT = "profile-day-coverage-v1"
PROFILE_DAY_SCOPE_CONTRACT = "profile-day-scope-v4"
SCAN_SCOPE_CONTRACT = "scan-verification-receipt-v2"
PERIOD_RECEIPT_CONTRACT = "profile-day-coverage-period-v1"
_CLEANUP_CONTROL_CONTRACT = "account-cleanup-release-control-v1"
_DAY_RELEASE_CONTRACTS = frozenset({
    "current_activation_hold_v1", "current_activation_forward_release_v1",
    _CLEANUP_CONTROL_CONTRACT,
})


class RuntimeReceiptError(RuntimeError):
    """A receipt is missing, inconsistent, or cannot be recorded safely."""


def _json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as error:
        raise RuntimeReceiptError("receipt payload is not canonical JSON") from error


def _sha(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _time(value: str | None) -> str:
    return (
        parse_time(value or now_utc())
        .astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _self_hashed(value: Mapping[str, Any]) -> dict[str, Any]:
    payload = json.loads(_json(dict(value)))
    payload["self_sha256"] = _sha(payload)
    return payload


def _verify_self_hash(value: Mapping[str, Any]) -> None:
    payload = dict(value)
    actual = payload.pop("self_sha256", None)
    if not isinstance(actual, str) or actual != _sha(payload):
        raise RuntimeReceiptError("receipt self hash mismatch")


def _evidence_root(db_path: Path, evidence_root: Path | None) -> Path:
    if evidence_root is not None:
        root = Path(evidence_root).expanduser()
    else:
        configured = os.environ.get("DCAR_EVIDENCE_ROOT", "").strip()
        if configured:
            root = Path(configured).expanduser()
        elif is_formal_database_path(Path(db_path)):
            root = (
                Path.home()
                / "Library"
                / "Application Support"
                / "DcarAIGC"
                / "evidence"
            )
        else:
            root = Path(db_path).expanduser().resolve(strict=False).parent / "evidence"
    if not root.is_absolute() or root.is_symlink():
        raise RuntimeReceiptError("evidence root must be an absolute non-symlink path")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    metadata = root.stat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise RuntimeReceiptError("evidence root must be private and user-owned")
    return root.resolve(strict=True)


def _write_evidence(
    db_path: Path,
    kind: str,
    payload: Mapping[str, Any],
    *,
    evidence_root: Path | None,
) -> dict[str, Any]:
    encoded = (_json(_self_hashed(payload)) + "\n").encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    root = _evidence_root(db_path, evidence_root)
    path = root / f"{kind}.{digest}.json"
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError:
        if path.is_symlink() or not path.is_file() or path.read_bytes() != encoded:
            raise RuntimeReceiptError("existing evidence file does not match receipt")
    else:
        try:
            offset = 0
            while offset < len(encoded):
                written = os.write(descriptor, encoded[offset:])
                if written <= 0:
                    raise RuntimeReceiptError("evidence file write was truncated")
                offset += written
            os.fsync(descriptor)
        except Exception:
            try:
                path.unlink()
            except OSError:
                pass
            raise
        finally:
            os.close(descriptor)
        directory = os.open(root, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    metadata = path.stat()
    if (
        metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
        or metadata.st_nlink != 1
    ):
        raise RuntimeReceiptError("evidence file is not private and single-link")
    return {
        "path": str(path),
        "sha256": digest,
        "byte_size": len(encoded),
        "audit_state": "present_unverified_by_consumer",
    }


def _read_one_shot(
    connection: sqlite3.Connection,
    row: sqlite3.Row | Mapping[str, Any],
    *,
    expected_job: str,
    expected_scope: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    run = dict(row)
    if run.get("job_id") != expected_job or run.get("status") != "succeeded":
        raise RuntimeReceiptError("receipt run is not terminal succeeded")
    if not run.get("completed_at") or run.get("started_at") != run.get("completed_at"):
        raise RuntimeReceiptError("receipt run is not atomic one-shot")
    attempts = connection.execute(
        "SELECT * FROM scheduler_run_attempts WHERE scheduler_run_id=? ORDER BY id",
        (run["id"],),
    ).fetchall()
    if len(attempts) != 1:
        raise RuntimeReceiptError("receipt must have exactly one immutable attempt")
    attempt = dict(attempts[0])
    if (
        attempt["attempt_number"] != 1
        or attempt["invocation_source"] != "scheduled"
        or attempt["status"] != "succeeded"
        or attempt["started_at"] != run["started_at"]
        or attempt["completed_at"] != run["completed_at"]
        or attempt["details_json"] != run["details_json"]
    ):
        raise RuntimeReceiptError("receipt run and immutable attempt disagree")
    try:
        details = json.loads(str(run["details_json"]))
    except (TypeError, ValueError) as error:
        raise RuntimeReceiptError("receipt details are invalid JSON") from error
    _verify_self_hash(details)
    if (
        details.get("contract_version") != BRIDGE_CONTRACT
        or details.get("receipt_kind") != expected_job
        or details.get("run_id") != run["id"]
        or details.get("attempt_id") != attempt["id"]
        or details.get("scheduled_for") != run["scheduled_for"]
        or details.get("recorded_at") != run["completed_at"]
    ):
        raise RuntimeReceiptError("receipt binding mismatch")
    if expected_scope is not None and details.get("scope") != dict(expected_scope):
        raise RuntimeReceiptError("receipt scope mismatch")
    return details


def _existing_one_shot(
    connection: sqlite3.Connection,
    *,
    job_id: str,
    scheduled_for: str,
    scope: Mapping[str, Any],
) -> dict[str, Any] | None:
    row = connection.execute(
        "SELECT * FROM scheduler_runs WHERE job_id=? AND scheduled_for=?" + durable_runs.root_run_predicate(connection),
        (job_id, scheduled_for),
    ).fetchone()
    if row is None:
        return None
    return _read_one_shot(
        connection, row, expected_job=job_id, expected_scope=scope
    )


def _record_one_shot(
    *,
    db_path: Path,
    job_id: str,
    scope: Mapping[str, Any],
    summary: Mapping[str, Any] | None,
    evidence: Mapping[str, Any],
    recorded_at: str,
    precondition: Callable[[sqlite3.Connection], None] | None = None,
    summary_factory: Callable[[sqlite3.Connection], Mapping[str, Any]] | None = None,
    native_sync: Callable[
        [sqlite3.Connection, Mapping[str, Any], bool], None
    ]
    | None = None,
) -> dict[str, Any]:
    if (summary is None) == (summary_factory is None):
        raise RuntimeReceiptError(
            "receipt requires exactly one summary or summary factory"
        )
    frozen_scope = json.loads(_json(dict(scope)))
    scheduled_for = "receipt:" + _sha({"job_id": job_id, "scope": frozen_scope})
    timestamp = _time(recorded_at)
    with connect(db_path) as connection, transaction(connection):
        existing = _existing_one_shot(
            connection,
            job_id=job_id,
            scheduled_for=scheduled_for,
            scope=frozen_scope,
        )
        if existing is not None:
            if (
                summary is not None
                and existing.get("summary") != dict(summary)
            ) or existing.get("evidence") != dict(evidence):
                raise RuntimeReceiptError("existing receipt payload changed")
            if native_sync is not None:
                native_sync(connection, existing, False)
            return existing
        if precondition is not None:
            precondition(connection)
        if summary is not None:
            resolved_summary = summary
        else:
            if summary_factory is None:  # guarded above; keeps type narrowing local
                raise RuntimeReceiptError("receipt summary factory is missing")
            resolved_summary = summary_factory(connection)
        frozen_summary = json.loads(
            _json(dict(resolved_summary))
        )
        draft = _json(
            {
                "contract_version": BRIDGE_CONTRACT,
                "receipt_kind": job_id,
                "scope": frozen_scope,
            }
        )
        cursor = connection.execute(
            "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,details_json) "
            "VALUES (?,?,'running',?,?)",
            (job_id, scheduled_for, timestamp, draft),
        )
        run_id = int(cursor.lastrowid or 0)
        cursor = connection.execute(
            "INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,"
            "invocation_source,status,started_at,details_json) "
            "VALUES (?,1,'scheduled','running',?,?)",
            (run_id, timestamp, draft),
        )
        attempt_id = int(cursor.lastrowid or 0)
        details = _self_hashed(
            {
                "contract_version": BRIDGE_CONTRACT,
                "receipt_kind": job_id,
                "scheduled_for": scheduled_for,
                "run_id": run_id,
                "attempt_id": attempt_id,
                "recorded_at": timestamp,
                "scope": frozen_scope,
                "summary": frozen_summary,
                "evidence": json.loads(_json(dict(evidence))),
            }
        )
        encoded = _json(details)
        updated = connection.execute(
            "UPDATE scheduler_run_attempts SET status='succeeded',completed_at=?,"
            "details_json=? WHERE id=? AND scheduler_run_id=? AND status='running'",
            (timestamp, encoded, attempt_id, run_id),
        )
        if updated.rowcount != 1:
            raise RuntimeReceiptError("receipt attempt could not finalize")
        updated = connection.execute(
            "UPDATE scheduler_runs SET status='succeeded',completed_at=?,details_json=? "
            "WHERE id=? AND status='running'",
            (timestamp, encoded, run_id),
        )
        if updated.rowcount != 1:
            raise RuntimeReceiptError("receipt run could not finalize")
        if native_sync is not None:
            native_sync(connection, details, True)
        return details


def _has_native_receipts(connection: sqlite3.Connection) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' "
        "AND name='profile_day_coverage_receipts'"
    ).fetchone() is not None


def _native_scan_row(
    connection: sqlite3.Connection,
    *,
    scan_run_id: int | None = None,
    bridge_run_id: int | None = None,
) -> sqlite3.Row | None:
    if not _has_native_receipts(connection):
        return None
    if (scan_run_id is None) == (bridge_run_id is None):
        raise RuntimeReceiptError("native scan receipt lookup is ambiguous")
    field, value = (
        ("r.scan_run_id", scan_run_id)
        if scan_run_id is not None
        else ("r.source_bridge_run_id", bridge_run_id)
    )
    row = connection.execute(
        f"SELECT r.* FROM scan_verification_receipts r WHERE {field}=? "
        "ORDER BY r.id DESC LIMIT 1",
        (value,),
    ).fetchone()
    if row is None:
        return None
    revoked = connection.execute(
        "SELECT 1 FROM runtime_receipt_revocations WHERE scan_receipt_id=?",
        (int(row["id"]),),
    ).fetchone()
    return None if revoked is not None else row


def _validate_native_scan_row(
    connection: sqlite3.Connection,
    row: sqlite3.Row | Mapping[str, Any],
) -> dict[str, Any]:
    native = dict(row)
    bridge_row = connection.execute(
        "SELECT * FROM scheduler_runs WHERE id=?",
        (native["source_bridge_run_id"],),
    ).fetchone()
    if bridge_row is None:
        raise RuntimeReceiptError("native scan receipt bridge is missing")
    details = _read_one_shot(
        connection, bridge_row, expected_job=SCAN_RECEIPT_JOB
    )
    scope = details.get("scope")
    summary = details.get("summary")
    evidence = details.get("evidence")
    if (
        not isinstance(scope, dict)
        or not isinstance(summary, dict)
        or not isinstance(evidence, dict)
    ):
        raise RuntimeReceiptError("native scan receipt payload is invalid")
    if (
        native["source_bridge_attempt_id"] != details["attempt_id"]
        or native["scan_run_id"] != scope.get("scan_run_id")
        or native["scan_attempt_id"] != scope.get("scan_attempt_id")
        or native["scan_status"] != scope.get("scan_status")
        or native["scope_json"] != _json(scope)
        or native["summary_json"] != _json(summary)
        or native["evidence_json"] != _json(evidence)
        or native["contract_version"] != details.get("contract_version")
        or native["receipt_sha256"] != details.get("self_sha256")
        or native["recorded_at"] != details.get("recorded_at")
    ):
        raise RuntimeReceiptError("native scan receipt binding mismatch")
    _run, _scan_details, binding = _terminal_scan_binding(
        connection, int(native["scan_run_id"])
    )
    expected_scope = {"contract_version": SCAN_SCOPE_CONTRACT, **binding}
    if scope != expected_scope:
        raise RuntimeReceiptError("native scan source binding mismatch")
    return details


def _sync_native_scan(
    connection: sqlite3.Connection,
    details: Mapping[str, Any],
    insert: bool,
) -> None:
    if not _has_native_receipts(connection):
        return
    scope = details["scope"]
    summary = details["summary"]
    evidence = details["evidence"]
    if insert:
        connection.execute(
            """INSERT INTO scan_verification_receipts(
                   source_bridge_run_id,source_bridge_attempt_id,scan_run_id,
                   scan_attempt_id,scan_status,scope_json,summary_json,evidence_json,
                   contract_version,receipt_sha256,recorded_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                details["run_id"],
                details["attempt_id"],
                scope["scan_run_id"],
                scope["scan_attempt_id"],
                scope["scan_status"],
                _json(scope),
                _json(summary),
                _json(evidence),
                details["contract_version"],
                details["self_sha256"],
                details["recorded_at"],
            ),
        )
    row = _native_scan_row(connection, bridge_run_id=int(details["run_id"]))
    if row is None:
        raise RuntimeReceiptError("native scan receipt is missing or revoked")
    current = _validate_native_scan_row(connection, row)
    if current.get("self_sha256") != details.get("self_sha256"):
        raise RuntimeReceiptError("native scan receipt hash mismatch")


def _terminal_scan_binding(
    connection: sqlite3.Connection, scan_run_id: int
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    row = connection.execute(
        "SELECT * FROM scheduler_runs WHERE id=?", (scan_run_id,)
    ).fetchone()
    if (
        row is None
        or row["status"] not in {"succeeded", "failed"}
        or not row["completed_at"]
    ):
        raise RuntimeReceiptError("scan source is not an accounted terminal")
    run = dict(row)
    attempts = connection.execute(
        "SELECT * FROM scheduler_run_attempts WHERE scheduler_run_id=? ORDER BY id",
        (scan_run_id,),
    ).fetchall()
    terminal = [dict(item) for item in attempts if item["status"] == row["status"]]
    if len(terminal) != 1 or terminal[0]["details_json"] != run["details_json"]:
        raise RuntimeReceiptError("scan source attempt binding is ambiguous")
    try:
        details = json.loads(str(run["details_json"]))
    except (TypeError, ValueError) as error:
        raise RuntimeReceiptError("scan source details are invalid") from error
    binding = {
        "scan_run_id": int(run["id"]),
        "scan_attempt_id": int(terminal[0]["id"]),
        "scan_job_id": str(run["job_id"]),
        "scan_status": str(run["status"]),
        "scan_id": details.get("scan_id"),
        "completed_at": str(run["completed_at"]),
        "details_sha256": hashlib.sha256(str(run["details_json"]).encode()).hexdigest(),
    }
    return run, details, binding


def record_scan_verification_receipt(
    scan_run_id: int,
    *,
    db_path: Path = DEFAULT_DB,
    cutoff_at: str | None = None,
    evidence_root: Path | None = None,
) -> dict[str, Any]:
    """Deep-verify one terminal scan, then seal its reusable DB summary."""

    timestamp = _time(cutoff_at)
    with connect(db_path) as connection:
        connection.execute("BEGIN")
        run, _details, binding = _terminal_scan_binding(connection, scan_run_id)
        if parse_time(binding["completed_at"]) > parse_time(timestamp):
            raise RuntimeReceiptError("scan completes after receipt cutoff")
        scope = {"contract_version": SCAN_SCOPE_CONTRACT, **binding}
        scheduled_for = "receipt:" + _sha(
            {"job_id": SCAN_RECEIPT_JOB, "scope": scope}
        )
        existing = _existing_one_shot(
            connection,
            job_id=SCAN_RECEIPT_JOB,
            scheduled_for=scheduled_for,
            scope=scope,
        )
        if existing is not None:
            if _has_native_receipts(connection):
                _sync_native_scan(connection, existing, False)
            return existing
        from .scan_receipts import verify_scan, verify_terminal_scan

        proof = (
            verify_scan(connection, run, cutoff_at=timestamp)
            if run["status"] == "succeeded"
            else verify_terminal_scan(connection, run, cutoff_at=timestamp)
        )
        connection.commit()
    full_evidence = {
        "contract_version": "scan-verification-evidence-v2",
        "verified_at": timestamp,
        "source": binding,
        "proof": proof,
    }
    evidence = _write_evidence(
        db_path,
        "scan-verification-v2",
        full_evidence,
        evidence_root=evidence_root,
    )
    summary = {
        "verified": True,
        "scan_run_id": scan_run_id,
        "scan_status": run["status"],
        "scope": proof["scope"],
        "completed_at": proof["completed_at"],
        "counts": proof.get("counts", {}),
        "terminal_class": proof.get("terminal_class", "success"),
        "accounted": proof.get("accounted", True),
        "required": proof.get("required", True),
        "publication_blocker": proof.get("publication_blocker", False),
        "reference_count": len(proof["references"]),
        "proof_sha256": _sha(proof),
    }

    def source_is_current(connection: sqlite3.Connection) -> None:
        _run, _details, current = _terminal_scan_binding(connection, scan_run_id)
        if current != binding:
            raise RuntimeReceiptError("scan source changed before receipt commit")

    return _record_one_shot(
        db_path=db_path,
        job_id=SCAN_RECEIPT_JOB,
        scope=scope,
        summary=summary,
        evidence=evidence,
        recorded_at=timestamp,
        precondition=source_is_current,
        native_sync=_sync_native_scan,
    )


def read_scan_verification_receipt(
    connection: sqlite3.Connection, scan_run_id: int
) -> dict[str, Any] | None:
    """Return a verified summary only while its immutable scan binding matches."""

    if _has_native_receipts(connection):
        row = _native_scan_row(connection, scan_run_id=scan_run_id)
        return _validate_native_scan_row(connection, row) if row is not None else None
    try:
        _run, _details, binding = _terminal_scan_binding(connection, scan_run_id)
    except RuntimeReceiptError:
        return None
    scope = {"contract_version": SCAN_SCOPE_CONTRACT, **binding}
    scheduled_for = "receipt:" + _sha({"job_id": SCAN_RECEIPT_JOB, "scope": scope})
    row = connection.execute(
        "SELECT * FROM scheduler_runs WHERE job_id=? AND scheduled_for=?" + durable_runs.root_run_predicate(connection),
        (SCAN_RECEIPT_JOB, scheduled_for),
    ).fetchone()
    if row is None:
        return None
    return _read_one_shot(
        connection, row, expected_job=SCAN_RECEIPT_JOB, expected_scope=scope
    )


def _legacy_coverage_source_revision(
    connection: sqlite3.Connection, *, cutoff_at: str
) -> str:
    rows = connection.execute(
        """
        SELECT r.id run_id,r.job_id,r.status run_status,r.completed_at run_completed,
               r.details_json run_details,a.id attempt_id,a.status attempt_status,
               a.completed_at attempt_completed,a.details_json attempt_details
        FROM scheduler_runs r
        LEFT JOIN scheduler_run_attempts a ON a.scheduler_run_id=r.id
        WHERE (r.job_id IN ('matrix_works_scan','tikhub_reconcile')
               OR r.job_id='pipeline_round:tikhub_reconcile')
          AND julianday(r.started_at)<=julianday(?)
        ORDER BY r.id,a.id
        """,
        (cutoff_at,),
    ).fetchall()
    projection = [
        {
            "run_id": row["run_id"],
            "job_id": row["job_id"],
            "run_status": row["run_status"],
            "run_completed": row["run_completed"],
            "run_details_sha256": hashlib.sha256(
                str(row["run_details"]).encode()
            ).hexdigest(),
            "attempt_id": row["attempt_id"],
            "attempt_status": row["attempt_status"],
            "attempt_completed": row["attempt_completed"],
            "attempt_details_sha256": (
                hashlib.sha256(str(row["attempt_details"]).encode()).hexdigest()
                if row["attempt_id"] is not None
                else None
            ),
        }
        for row in rows
    ]
    return _sha(projection)


def _coverage_anchor_at(business_day: str) -> str:
    following = date.fromisoformat(business_day) + timedelta(days=1)
    anchor = datetime.combine(following, time(3, 0), BEIJING)
    return anchor.astimezone(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _active_schema19_profile(
    connection: sqlite3.Connection, *, business_day: str
) -> dict[str, Any]:
    from .profile_activations import PROFILE_FAMILIES, activation_at

    active = activation_at(connection, _coverage_anchor_at(business_day))
    if active is None:
        raise RuntimeReceiptError("active acquisition profile is missing")
    roster = connection.execute(
        "SELECT id,members_sha256,source_family FROM account_roster_snapshots WHERE id=?",
        (active["roster_snapshot_id"],),
    ).fetchone()
    if (
        roster is None
        or roster["members_sha256"] != active["roster_members_sha256"]
        or active["profile_id"] not in PROFILE_FAMILIES
        or roster["source_family"] != PROFILE_FAMILIES[active["profile_id"]]
    ):
        raise RuntimeReceiptError("active acquisition profile roster is invalid")
    return {
        "activation_id": int(active["activation_id"]),
        "profile_id": str(active["profile_id"]),
        "activation_sha256": str(active["activation_sha256"]),
        "source_family": PROFILE_FAMILIES[str(active["profile_id"])],
        "roster_snapshot_id": int(active["roster_snapshot_id"]),
        "roster_snapshot_hash": str(active["roster_members_sha256"]),
    }


def _active_profile_for_day(
    connection: sqlite3.Connection, *, business_day: str, at: str
) -> dict[str, Any]:
    if _has_native_receipts(connection):
        return _active_schema19_profile(connection, business_day=business_day)
    return _active_schema18_profile(connection, at=at)


def _coverage_day(
    coverage: Mapping[str, Any], *, business_day: str
) -> dict[str, Any]:
    days = coverage.get("days")
    if isinstance(days, list) and len(days) == 1 and isinstance(days[0], dict):
        day = dict(days[0])
    elif isinstance(coverage.get("day"), dict):
        day = dict(coverage["day"])
    else:
        raise RuntimeReceiptError("profile-day coverage has no exact day scope")
    if day.get("date") != business_day:
        raise RuntimeReceiptError("profile-day coverage selected the wrong day")
    return day


def _selected_scan_run_ids(day: Mapping[str, Any]) -> list[int]:
    values: list[Any] = []
    for field in ("matrix_run_ids", "tikhub_run_ids"):
        selected = day.get(field, [])
        if not isinstance(selected, list):
            raise RuntimeReceiptError("profile-day required scan IDs are invalid")
        values.extend(selected)
    if any(type(value) is not int or value <= 0 for value in values):
        raise RuntimeReceiptError("profile-day required scan IDs are invalid")
    if len(values) != len(set(values)):
        raise RuntimeReceiptError("profile-day required scan IDs are duplicated")
    return sorted(values)


def _anchor_binding(
    connection: sqlite3.Connection,
    *,
    day: Mapping[str, Any],
    active: Mapping[str, Any],
    business_day: str,
    frozen_anchor: Mapping[str, Any] | None = None,
    cutoff_at: str | None = None,
) -> dict[str, Any]:
    run_id = day.get("round_run_id")
    if type(run_id) is not int or run_id <= 0:
        raise RuntimeReceiptError("profile-day closeout anchor is missing")
    row = connection.execute(
        "SELECT * FROM scheduler_runs WHERE id=?", (run_id,)
    ).fetchone()
    if row is None:
        raise RuntimeReceiptError("profile-day closeout anchor is not terminal")
    if frozen_anchor is not None:
        if (frozen_anchor.get("run_id") != run_id
                or type(frozen_anchor.get("attempt_id")) is not int):
            raise RuntimeReceiptError("profile-day frozen anchor identity is invalid")
        attempt = connection.execute(
            "SELECT * FROM scheduler_run_attempts WHERE id=? AND scheduler_run_id=?",
            (frozen_anchor["attempt_id"], run_id),
        ).fetchone()
        if attempt is None:
            raise RuntimeReceiptError("profile-day frozen anchor attempt is missing")
        row = {**dict(row), **{key: attempt[key] for key in (
            "status", "completed_at", "details_json"
        )}}
    if row["status"] not in {"succeeded", "partial"} or not row["completed_at"]:
        raise RuntimeReceiptError("profile-day closeout anchor is not terminal")
    if cutoff_at is not None and parse_time(row["completed_at"]) > parse_time(cutoff_at):
        raise RuntimeReceiptError("profile-day closeout anchor completes after cutoff")
    attempts = connection.execute(
        "SELECT * FROM scheduler_run_attempts WHERE scheduler_run_id=? ORDER BY id",
        (run_id,),
    ).fetchall()
    terminal = [
        item
        for item in attempts
        if item["status"] == row["status"]
        and item["details_json"] == row["details_json"]
        and item["completed_at"] == row["completed_at"]
    ]
    if len(terminal) != 1:
        raise RuntimeReceiptError("profile-day closeout anchor attempt is ambiguous")
    try:
        details = json.loads(str(row["details_json"]))
        identity = details["identity"]
    except (KeyError, TypeError, ValueError) as error:
        raise RuntimeReceiptError("profile-day closeout anchor is invalid") from error
    expected = {
        "registration_id": "tikhub_reconcile",
        "job_id": "tikhub_reconcile",
        "beijing_day": (date.fromisoformat(business_day) + timedelta(days=1)).isoformat(),
        "scheduled_at": _coverage_anchor_at(business_day),
        "activation_id": active["activation_id"],
        "profile_id": active["profile_id"],
        "activation_sha256": active["activation_sha256"],
        "roster_snapshot_id": active["roster_snapshot_id"],
        "roster_snapshot_hash": active["roster_snapshot_hash"],
    }
    if not isinstance(identity, dict) or any(
        identity.get(key) != value for key, value in expected.items()
    ):
        raise RuntimeReceiptError("profile-day closeout anchor binding mismatch")
    if any(day.get(key) != value for key, value in expected.items() if key in day):
        raise RuntimeReceiptError("profile-day coverage anchor binding mismatch")
    binding = {
        "run_id": int(row["id"]),
        "attempt_id": int(terminal[0]["id"]),
        "status": str(row["status"]),
        "completed_at": str(row["completed_at"]),
        "details_sha256": hashlib.sha256(
            str(row["details_json"]).encode("utf-8")
        ).hexdigest(),
    }
    if frozen_anchor is not None and binding != dict(frozen_anchor):
        raise RuntimeReceiptError("profile-day frozen anchor binding mismatch")
    return binding


def _compact_coverage_source_revision(
    connection: sqlite3.Connection,
    *,
    coverage: Mapping[str, Any],
    active: Mapping[str, Any],
    business_day: str,
    cutoff_at: str,
    frozen_binding: Mapping[str, Any] | None = None,
) -> tuple[str, dict[str, Any]]:
    day = _coverage_day(coverage, business_day=business_day)
    if day.get("coverage_contract") == "catalog-day-coverage-v1":
        from .capture_day_coverage import validate_source_binding

        try:
            binding = dict(day["source_binding"])
            verified = validate_source_binding(connection, binding, at=cutoff_at)
            if verified.get("valid") is not True:
                raise ValueError("catalog source validation did not succeed")
        except (KeyError, TypeError, ValueError, OSError, sqlite3.Error, RuntimeError) as error:
            raise RuntimeReceiptError("catalog profile-day source evidence is invalid") from error
        if (binding.get("date") != business_day
                or any(day.get(key) != value for key, value in active.items())
                or any(binding.get(key) != active[key] for key in
                       ("activation_id", "activation_sha256", "profile_id"))):
            raise RuntimeReceiptError("catalog profile-day activation binding mismatch")
        projection = {"activation": dict(active), "business_day": business_day, "catalog": binding}
        if frozen_binding is not None and projection != dict(frozen_binding):
            raise RuntimeReceiptError("catalog profile-day frozen source binding mismatch")
        return _sha(projection), projection
    anchor = _anchor_binding(
        connection, day=day, active=active, business_day=business_day,
        frozen_anchor=frozen_binding.get("anchor") if frozen_binding is not None else None,
        cutoff_at=cutoff_at,
    )
    scans = []
    for run_id in _selected_scan_run_ids(day):
        row = _native_scan_row(connection, scan_run_id=run_id)
        if row is None:
            raise RuntimeReceiptError(
                "required closeout scan receipt is missing or revoked"
            )
        details = _validate_native_scan_row(connection, row)
        if parse_time(details["scope"]["completed_at"]) > parse_time(cutoff_at):
            raise RuntimeReceiptError("required closeout scan completes after cutoff")
        scans.append(
            {
                "scan_run_id": run_id,
                "scan_attempt_id": int(row["scan_attempt_id"]),
                "receipt_id": int(row["id"]),
                "receipt_sha256": str(row["receipt_sha256"]),
            }
        )
    eligible = day.get("eligible_identity_ids", [])
    if not isinstance(eligible, list) or any(
        type(identity_id) is not int for identity_id in eligible
    ):
        raise RuntimeReceiptError("profile-day eligible identity scope is invalid")
    account_state_events: list[dict[str, Any]] = []
    if eligible:
        placeholders = ",".join("?" for _value in eligible)
        rows = connection.execute(
            "SELECT id,account_identity_id,activation_id,old_enabled,new_enabled,"
            "effective_at,contract_version,event_sha256 FROM account_state_events "
            f"WHERE account_identity_id IN ({placeholders}) "
            "AND julianday(effective_at)<=julianday(?) ORDER BY id",
            (*eligible, cutoff_at),
        ).fetchall()
        account_state_events = [dict(row) for row in rows]
    projection = {
        "activation": dict(active),
        "business_day": business_day,
        "anchor": anchor,
        "scans": scans,
        "account_state_events": account_state_events,
    }
    return _sha(projection), projection


def _active_schema18_profile(
    connection: sqlite3.Connection, *, at: str | None = None
) -> dict[str, Any]:
    query = (
        "SELECT * FROM scheduler_runs WHERE job_id=? AND status='succeeded' "
    )
    parameters: tuple[Any, ...] = (PIPELINE_ACTIVATION_JOB,)
    if at is not None:
        query += "AND julianday(completed_at)<=julianday(?) "
        parameters += (_time(at),)
    row = connection.execute(query + "ORDER BY id DESC LIMIT 1", parameters).fetchone()
    if row is None:
        raise RuntimeReceiptError("active pipeline receipt is missing")
    try:
        details = json.loads(str(row["details_json"]))
    except (TypeError, ValueError) as error:
        raise RuntimeReceiptError("active pipeline receipt is invalid") from error
    if details.get("contract_version") != PIPELINE_CONTRACT:
        raise RuntimeReceiptError("active pipeline contract is unsupported")
    return {
        "activation_id": int(row["id"]),
        "profile_id": str(details.get("profile_id") or MODE_A_PROFILE),
        "roster_snapshot_id": details.get("roster_snapshot_id"),
        "roster_snapshot_hash": details.get("roster_snapshot_hash"),
    }


def _coverage_business_day(cutoff_at: str) -> str:
    local_day = parse_time(cutoff_at).astimezone(BEIJING).date()
    return (local_day - timedelta(days=1)).isoformat()


def _native_day_row(
    connection: sqlite3.Connection,
    *,
    activation_id: int,
    business_day: str,
    at: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """SELECT r.* FROM profile_day_coverage_receipts r
           WHERE r.activation_id=? AND r.business_day=?
             AND julianday(r.sealed_at)<=julianday(?)
             AND julianday(r.recorded_at)<=julianday(?)
           ORDER BY r.sequence DESC,r.sealed_at DESC,r.id DESC LIMIT 1""",
        (activation_id, business_day, at, at),
    ).fetchone()


def _latest_complete_current_day_row(
    connection: sqlite3.Connection,
    *,
    activation_id: int,
    at: str,
) -> sqlite3.Row | None:
    """Keep current readiness on the latest release-bound activation day.

    Only the newest revision of each business day is authoritative.  An
    incomplete, revoked, or ordinary later day may be skipped in favour of an
    earlier completed release day, but an older revision of that same day can
    never be resurrected.
    """

    current_day = parse_time(at).astimezone(BEIJING).date().isoformat()
    rows = connection.execute(
        """SELECT r.* FROM profile_day_coverage_receipts r
           WHERE r.activation_id=? AND r.business_day<?
             AND julianday(r.sealed_at)<=julianday(?)
             AND julianday(r.recorded_at)<=julianday(?)
           ORDER BY r.business_day DESC,r.sequence DESC,r.sealed_at DESC,r.id DESC""",
        (activation_id, current_day, at, at),
    ).fetchall()
    seen_days: set[str] = set()
    for row in rows:
        business_day = str(row["business_day"])
        if business_day in seen_days:
            continue
        seen_days.add(business_day)
        if connection.execute(
            "SELECT 1 FROM runtime_receipt_revocations WHERE day_receipt_id=?",
            (int(row["id"]),),
        ).fetchone() is not None:
            continue
        if int(row["complete"]) != 1:
            continue
        try:
            scope = json.loads(str(row["scope_json"]))
        except (TypeError, ValueError):
            continue
        if (
            isinstance(scope, dict)
            and type(scope.get("release_event_id")) is int
            and isinstance(scope.get("release_event_hash"), str)
            and isinstance(scope.get("released_at"), str)
            and isinstance(scope.get("drain_id"), str)
            and scope.get("control_contract_version")
            in _DAY_RELEASE_CONTRACTS
        ):
            return row
    return None


def _day_release_binding(
    connection: sqlite3.Connection,
    *,
    activation_id: int,
    business_day: str,
    at: str | None = None,
) -> dict[str, Any]:
    """Bind a day to its actual release, without asserting completed coverage."""

    day_start = datetime.combine(
        date.fromisoformat(business_day), time.min, BEIJING
    ).astimezone(timezone.utc)
    day_end = day_start + timedelta(days=1)
    rows = connection.execute(
        """SELECT * FROM pipeline_paid_drain_events
           WHERE target_activation_id=? AND event_type='release'
             AND julianday(created_at)<julianday(?)
           ORDER BY id""",
        (
            activation_id,
            day_end.isoformat(timespec="seconds").replace("+00:00", "Z"),
        ),
    ).fetchall()
    matches: list[tuple[sqlite3.Row, dict[str, Any]]] = []
    for row in rows:
        try:
            payload = json.loads(str(row["payload_json"]))
        except (TypeError, ValueError):
            continue
        control = payload.get("control") if isinstance(payload, dict) else None
        if (
            isinstance(control, dict)
            and control.get("contract_version") == "current_activation_hold_v1"
            and control.get("control_purpose") == "full_day_release"
            and control.get("activation_id") == activation_id
            and control.get("release_business_day") == business_day
            and day_start <= parse_time(str(row["created_at"])) < day_start + timedelta(minutes=5)
        ):
            matches.append((row, control))
        elif isinstance(control, dict) and control.get("contract") == _CLEANUP_CONTROL_CONTRACT:
            # Verify the currently installed inheritance, not whether a later
            # source successor was already installed at the historical day start.
            from .capture_release import _native_control
            from .profile_activations import activation_by_id

            try:
                timestamp = _time(at)
                evidence = _cleanup_release_evidence(
                    connection, active=activation_by_id(connection, activation_id),
                    release_control=control, at=timestamp,
                )
                if (parse_time(str(row["created_at"])) <= parse_time(timestamp)
                        and _native_control(connection, evidence, at=timestamp) == int(row["id"])):
                    matches.append((row, control))
            except (KeyError, TypeError, ValueError, OSError, sqlite3.Error, RuntimeError):
                continue
        elif isinstance(control, dict):
            from .forward_recovery import forward_release_matches
            from .profile_activations import activation_by_id
            from .profile_control import _hold_event

            start = _hold_event(connection, drain_id=str(row["drain_id"]), event_type="start")
            if (start is not None and forward_release_matches(
                control, active=activation_by_id(connection, activation_id),
                start=start, released_at=str(row["created_at"]),
            ) and str(control["scope_start"]) <= business_day):
                matches.append((row, control))
    if matches and (
        matches[-1][1].get("contract_version") == "current_activation_forward_release_v1"
        or matches[-1][1].get("contract") == _CLEANUP_CONTROL_CONTRACT
    ):
        matches = matches[-1:]
    if len(matches) != 1:
        return {
            "drain_id": None,
            "release_event_id": None,
            "release_event_hash": None,
            "released_at": None,
            "control_contract_version": None,
        }
    row, control = matches[0]
    return {
        "drain_id": str(row["drain_id"]),
        "release_event_id": int(row["id"]),
        "release_event_hash": str(row["event_hash"]),
        "released_at": str(row["created_at"]),
        "control_contract_version": str(control.get("contract_version") or control["contract"]),
    }


def _validate_native_day_row(
    connection: sqlite3.Connection,
    row: sqlite3.Row | Mapping[str, Any],
) -> dict[str, Any]:
    native = dict(row)
    bridge_row = connection.execute(
        "SELECT * FROM scheduler_runs WHERE id=?",
        (native["source_bridge_run_id"],),
    ).fetchone()
    if bridge_row is None:
        raise RuntimeReceiptError("native profile-day receipt bridge is missing")
    details = _read_one_shot(
        connection, bridge_row, expected_job=DAY_RECEIPT_JOB
    )
    scope = details.get("scope")
    summary = details.get("summary")
    evidence = details.get("evidence")
    if (
        not isinstance(scope, dict)
        or not isinstance(summary, dict)
        or not isinstance(evidence, dict)
    ):
        raise RuntimeReceiptError("native profile-day receipt payload is invalid")
    coverage = summary.get("coverage")
    if not isinstance(coverage, dict):
        raise RuntimeReceiptError("native profile-day receipt payload is invalid")
    if (
        native["source_bridge_attempt_id"] != details["attempt_id"]
        or native["activation_id"] != scope.get("activation_id")
        or native["profile_id"] != scope.get("profile_id")
        or native["roster_snapshot_id"] != scope.get("roster_snapshot_id")
        or native["roster_members_sha256"] != scope.get("roster_snapshot_hash")
        or native["business_day"] != scope.get("business_day")
        or native["sequence"] != summary.get("sequence")
        or native["sealed_at"] != summary.get("sealed_at")
        or native["status"] != summary.get("status")
        or native["complete"] != int(bool(summary.get("complete")))
        or native["partial_publishable"]
        != int(bool(coverage.get("partial_publishable", False)))
        or native["scope_json"] != _json(scope)
        or native["summary_json"] != _json(summary)
        or native["evidence_json"] != _json(evidence)
        or native["contract_version"] != details.get("contract_version")
        or native["receipt_sha256"] != details.get("self_sha256")
        or native["recorded_at"] != details.get("recorded_at")
    ):
        raise RuntimeReceiptError("native profile-day receipt binding mismatch")
    from .profile_activations import PROFILE_FAMILIES, activation_by_id

    active = activation_by_id(connection, int(native["activation_id"]))
    if (
        active["profile_id"] != native["profile_id"]
        or active["roster_snapshot_id"] != native["roster_snapshot_id"]
        or active["roster_members_sha256"] != native["roster_members_sha256"]
    ):
        raise RuntimeReceiptError("native profile-day activation binding mismatch")
    active_scope = {
        "activation_id": int(active["activation_id"]),
        "profile_id": str(active["profile_id"]),
        "activation_sha256": str(active["activation_sha256"]),
        "source_family": PROFILE_FAMILIES[str(active["profile_id"])],
        "roster_snapshot_id": int(active["roster_snapshot_id"]),
        "roster_snapshot_hash": str(active["roster_members_sha256"]),
    }
    revision, source_binding = _compact_coverage_source_revision(
        connection,
        coverage=coverage,
        active=active_scope,
        business_day=str(native["business_day"]),
        cutoff_at=str(details["recorded_at"]),
        frozen_binding=scope.get("source_binding"),
    )
    if (
        scope.get("source_revision") != revision
        or scope.get("source_binding") != source_binding
        or scope.get("coverage_sha256") != _sha(coverage)
    ):
        raise RuntimeReceiptError("native profile-day source binding mismatch")
    return details


def _sync_native_day(
    connection: sqlite3.Connection,
    details: Mapping[str, Any],
    insert: bool,
) -> None:
    if not _has_native_receipts(connection):
        return
    scope = details["scope"]
    summary = details["summary"]
    coverage = summary["coverage"]
    evidence = details["evidence"]
    if insert:
        connection.execute(
            """INSERT INTO profile_day_coverage_receipts(
                   source_bridge_run_id,source_bridge_attempt_id,activation_id,
                   profile_id,roster_snapshot_id,roster_members_sha256,business_day,
                   sequence,sealed_at,status,complete,partial_publishable,scope_json,
                   summary_json,evidence_json,contract_version,receipt_sha256,recorded_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                details["run_id"],
                details["attempt_id"],
                scope["activation_id"],
                scope["profile_id"],
                scope["roster_snapshot_id"],
                scope["roster_snapshot_hash"],
                scope["business_day"],
                summary["sequence"],
                summary["sealed_at"],
                summary["status"],
                int(bool(summary["complete"])),
                int(bool(coverage.get("partial_publishable", False))),
                _json(scope),
                _json(summary),
                _json(evidence),
                details["contract_version"],
                details["self_sha256"],
                details["recorded_at"],
            ),
        )
    row = connection.execute(
        "SELECT * FROM profile_day_coverage_receipts "
        "WHERE source_bridge_run_id=? AND NOT EXISTS ("
        "SELECT 1 FROM runtime_receipt_revocations v "
        "WHERE v.day_receipt_id=profile_day_coverage_receipts.id)",
        (details["run_id"],),
    ).fetchone()
    if row is None:
        raise RuntimeReceiptError("native profile-day receipt is missing or revoked")
    current = _validate_native_day_row(connection, row)
    if current.get("self_sha256") != details.get("self_sha256"):
        raise RuntimeReceiptError("native profile-day receipt hash mismatch")


def record_profile_day_coverage_receipt(
    *,
    db_path: Path = DEFAULT_DB,
    cutoff_at: str | None = None,
    evidence_root: Path | None = None,
) -> dict[str, Any]:
    """Compute deep coverage off the hot path and seal its lightweight summary."""

    timestamp = _time(cutoff_at)
    business_day = _coverage_business_day(timestamp)
    with connect(db_path) as connection:
        connection.execute("BEGIN")
        native = _has_native_receipts(connection)
        active = _active_profile_for_day(
            connection, business_day=business_day, at=timestamp
        )
        if native:
            existing_row = _native_day_row(
                connection,
                activation_id=int(active["activation_id"]),
                business_day=business_day,
                at=timestamp,
            )
            if existing_row is not None and existing_row["sealed_at"] == timestamp:
                existing = _validate_native_day_row(connection, existing_row)
                existing_coverage = existing.get("summary", {}).get("coverage")
                existing_scope = existing.get("scope", {})
                if isinstance(existing_coverage, dict) and isinstance(
                    existing_scope, dict
                ):
                    current_revision, _binding = _compact_coverage_source_revision(
                        connection,
                        coverage=existing_coverage,
                        active=active,
                        business_day=business_day,
                        cutoff_at=timestamp,
                    )
                    if (
                        current_revision == existing_scope.get("source_revision")
                        and _sha(existing_coverage)
                        == existing_scope.get("coverage_sha256")
                    ):
                        return existing
        from .scan_receipts import runtime_coverage

        coverage = runtime_coverage(connection, at=timestamp)
        if native and coverage.get("contract_version") != PROFILE_DAY_CONTRACT:
            raise RuntimeReceiptError("profile-day coverage contract is unsupported")
        if native:
            day = _coverage_day(coverage, business_day=business_day)
            if any(
                day.get(key) != active[key]
                for key in (
                    "activation_id",
                    "profile_id",
                    "activation_sha256",
                    "source_family",
                    "roster_snapshot_id",
                    "roster_snapshot_hash",
                )
            ):
                raise RuntimeReceiptError("coverage activation or roster does not match")
        if not native and (
            coverage.get("roster_snapshot_id") is not None
            and coverage.get("roster_snapshot_id") != active["roster_snapshot_id"]
        ):
            raise RuntimeReceiptError(
                "coverage roster does not match the active schema18 profile"
            )
        if native:
            source_revision, source_binding = _compact_coverage_source_revision(
                connection,
                coverage=coverage,
                active=active,
                business_day=business_day,
                cutoff_at=timestamp,
            )
        else:
            source_revision = _legacy_coverage_source_revision(
                connection, cutoff_at=timestamp
            )
            source_binding = None
        scope = {
            "contract_version": (
                PROFILE_DAY_SCOPE_CONTRACT if native else "profile-day-scope-v2"
            ),
            **active,
            "business_day": business_day,
            "source_revision": source_revision,
            "coverage_sha256": _sha(coverage),
            **({"source_binding": source_binding} if source_binding else {}),
            **(
                _day_release_binding(
                    connection,
                    activation_id=int(active["activation_id"]),
                    business_day=business_day,
                    at=timestamp,
                )
                if native
                else {}
            ),
        }
        connection.commit()
    full_evidence = {
        "contract_version": "profile-day-coverage-evidence-v3",
        "sealed_at": timestamp,
        "scope": scope,
        "coverage": coverage,
    }
    evidence = _write_evidence(
        db_path,
        "profile-day-coverage-v3",
        full_evidence,
        evidence_root=evidence_root,
    )
    def summary_factory(connection: sqlite3.Connection) -> Mapping[str, Any]:
        if native:
            sequence = 1 + int(
                connection.execute(
                    "SELECT COALESCE(MAX(sequence),0) "
                    "FROM profile_day_coverage_receipts "
                    "WHERE activation_id=? AND business_day=?",
                    (active["activation_id"], business_day),
                ).fetchone()[0]
            )
        else:
            sequence = 1 + int(
                connection.execute(
                    "SELECT COUNT(*) FROM scheduler_runs WHERE job_id=? "
                    "AND json_extract(details_json,'$.scope.business_day')=? "
                    "AND json_extract(details_json,'$.scope.activation_id')=?",
                    (DAY_RECEIPT_JOB, business_day, active["activation_id"]),
                ).fetchone()[0]
            )
        return {
            "sequence": sequence,
            "sealed_at": timestamp,
            "status": coverage["status"],
            "complete": coverage["complete"],
            "coverage": coverage,
        }

    def source_is_current(connection: sqlite3.Connection) -> None:
        if _active_profile_for_day(
            connection, business_day=business_day, at=timestamp
        ) != active:
            raise RuntimeReceiptError("activation changed before day receipt commit")
        if native:
            current_revision, _binding = _compact_coverage_source_revision(
                connection,
                coverage=coverage,
                active=active,
                business_day=business_day,
                cutoff_at=timestamp,
            )
        else:
            current_revision = _legacy_coverage_source_revision(
                connection, cutoff_at=timestamp
            )
        if current_revision != source_revision:
            raise RuntimeReceiptError("coverage source changed before day receipt commit")

    return _record_one_shot(
        db_path=db_path,
        job_id=DAY_RECEIPT_JOB,
        scope=scope,
        summary=None,
        evidence=evidence,
        recorded_at=timestamp,
        precondition=source_is_current,
        summary_factory=summary_factory,
        native_sync=_sync_native_day if native else None,
    )


def refresh_current_scan_receipts(
    *,
    db_path: Path = DEFAULT_DB,
    cutoff_at: str | None = None,
    evidence_root: Path | None = None,
    limit: int = 1000,
    budget: ReconcileBudget | None = None,
) -> dict[str, Any]:
    """Seal missing scan receipts for the one day addressed by runtime health."""

    if type(limit) is not int or limit < 1:
        raise RuntimeReceiptError("scan receipt refresh limit must be positive")
    active_budget = budget if budget is not None else current_reconcile_budget()
    timestamp = _time(cutoff_at)
    expected_end = day_cutoff_for(timestamp)
    candidates: list[int] = []
    with connect(db_path) as connection:
        if _has_native_receipts(connection):
            from .scan_receipts import runtime_coverage

            coverage = runtime_coverage(connection, at=timestamp)
            if coverage.get("contract_version") != PROFILE_DAY_CONTRACT:
                raise RuntimeReceiptError(
                    "profile-day coverage contract is unsupported"
                )
            day = _coverage_day(
                coverage, business_day=_coverage_business_day(timestamp)
            )
            selected = _selected_scan_run_ids(day)
            if len(selected) > limit:
                selected = selected[:limit]
            candidates = [
                run_id
                for run_id in selected
                if read_scan_verification_receipt(connection, run_id) is None
            ]
        else:
            for row in connection.execute(
                "SELECT * FROM scheduler_runs WHERE job_id IN "
                "('matrix_works_scan','tikhub_reconcile') "
                "AND status IN ('succeeded','failed') "
                "AND julianday(completed_at)<=julianday(?) ORDER BY id",
                (timestamp,),
            ):
                try:
                    details = json.loads(str(row["details_json"]))
                    identity = details["identity"]
                except (KeyError, TypeError, ValueError):
                    continue
                if not isinstance(identity, dict):
                    continue
                if row["job_id"] == "matrix_works_scan":
                    belongs = identity.get("overall_end_at") == expected_end
                else:
                    belongs = (
                        identity.get("purpose") == "reconcile"
                        and identity.get("window_end") == expected_end
                    )
                if belongs and read_scan_verification_receipt(
                    connection, int(row["id"])
                ) is None:
                    candidates.append(int(row["id"]))
                if len(candidates) >= limit:
                    break
    receipts: list[dict[str, Any]] = []
    errors: dict[str, str] = {}
    budget_exhausted = False
    for run_id in candidates:
        if active_budget is not None and not active_budget.take():
            budget_exhausted = True
            break
        try:
            receipt = record_scan_verification_receipt(
                run_id,
                db_path=db_path,
                cutoff_at=timestamp,
                evidence_root=evidence_root,
            )
        except (OSError, RuntimeError, sqlite3.Error, ValueError) as error:
            errors[str(run_id)] = str(error)
        else:
            receipts.append(
                {
                    "scan_run_id": run_id,
                    "receipt_run_id": receipt["run_id"],
                    "attempt_id": receipt["attempt_id"],
                }
            )
    return {
        "contract_version": "scan-verification-refresh-v2",
        "cutoff_at": timestamp,
        "expected_window_end": expected_end,
        "candidate_count": len(candidates),
        "recorded_count": len(receipts),
        "receipts": receipts,
        "errors": errors,
        "budget_exhausted": budget_exhausted,
        "limit_reached": len(candidates) == limit or budget_exhausted,
    }


def refresh_runtime_receipts(
    *,
    db_path: Path = DEFAULT_DB,
    cutoff_at: str | None = None,
    evidence_root: Path | None = None,
    budget: ReconcileBudget | None = None,
) -> dict[str, Any]:
    """Writer-side background refresh; API consumers never call this path."""

    active_budget = budget if budget is not None else current_reconcile_budget()
    timestamp = _time(cutoff_at)
    business_day = _coverage_business_day(timestamp)
    anchor_at = _coverage_anchor_at(business_day)
    if parse_time(timestamp) < parse_time(anchor_at):
        return {
            "status": "skipped",
            "reason": "profile_day_anchor_not_due",
            "business_day": business_day,
            "anchor_at": anchor_at,
        }
    scans = refresh_current_scan_receipts(
        db_path=db_path,
        cutoff_at=timestamp,
        evidence_root=evidence_root,
        budget=active_budget,
    )
    if active_budget is not None and not active_budget.take():
        return {
            "status": "partial",
            "reason": "reconcile_budget_exhausted",
            "scan_receipts": scans,
            "day_receipt": None,
        }
    day = record_profile_day_coverage_receipt(
        db_path=db_path,
        cutoff_at=timestamp,
        evidence_root=evidence_root,
    )
    return {"status": "succeeded", "scan_receipts": scans, "day_receipt": day}


def read_profile_day_coverage_receipt(
    connection: sqlite3.Connection,
    *,
    at: str,
    business_day: str | None = None,
) -> dict[str, Any] | None:
    """Select the latest valid revision sealed no later than the consumer cutoff."""

    target_day = business_day or _coverage_business_day(at)
    if _has_native_receipts(connection):
        try:
            active = _active_schema19_profile(
                connection, business_day=target_day
            )
        except RuntimeReceiptError:
            return None
        row = _native_day_row(
            connection,
            activation_id=int(active["activation_id"]),
            business_day=target_day,
            at=_time(at),
        )
        if row is None:
            return None
        # Revision order is authoritative.  A revoked newest revision must
        # not resurrect an older receipt for the same activation/day.
        if connection.execute(
            "SELECT 1 FROM runtime_receipt_revocations WHERE day_receipt_id=?",
            (int(row["id"]),),
        ).fetchone() is not None:
            return None
        try:
            selected = _validate_native_day_row(connection, row)
        except RuntimeReceiptError as error:
            if str(error) == "required closeout scan receipt is missing or revoked":
                return None
            raise
        scope = selected["scope"]
        if any(
            scope.get(key) != active[key]
            for key in (
                "activation_id",
                "profile_id",
                "activation_sha256",
                "source_family",
                "roster_snapshot_id",
                "roster_snapshot_hash",
            )
        ) or scope.get("business_day") != target_day:
            raise RuntimeReceiptError("profile-day receipt selected the wrong scope")
        return selected
    try:
        active = _active_schema18_profile(connection, at=at)
    except RuntimeReceiptError:
        return None
    rows = connection.execute(
        "SELECT * FROM scheduler_runs WHERE job_id=? AND status='succeeded' "
        "AND json_extract(details_json,'$.scope.business_day')=? "
        "AND json_extract(details_json,'$.scope.activation_id')=? "
        "AND json_extract(details_json,'$.scope.profile_id')=? "
        "AND julianday(completed_at)<=julianday(?) ORDER BY id DESC",
        (
            DAY_RECEIPT_JOB,
            target_day,
            active["activation_id"],
            active["profile_id"],
            _time(at),
        ),
    ).fetchall()
    receipts = [
        _read_one_shot(connection, row, expected_job=DAY_RECEIPT_JOB) for row in rows
    ]
    if not receipts:
        return None
    receipts.sort(
        key=lambda value: (
            int(value["summary"]["sequence"]),
            parse_time(value["summary"]["sealed_at"]),
            int(value["run_id"]),
        ),
        reverse=True,
    )
    selected = receipts[0]
    scope = selected["scope"]
    if scope.get("business_day") != target_day:
        raise RuntimeReceiptError("profile-day receipt selected the wrong day")
    return selected


def unknown_runtime_coverage(
    *, at: str, reason: str, active: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Cheap fail-closed health shape when no DB day receipt is available."""

    profile_id = active.get("profile_id") if active else None
    from .profile_activations import SYSTEM_PROFILES

    matrix_expected = 0 if profile_id in SYSTEM_PROFILES else 60
    return {
        "contract_version": PROFILE_DAY_CONTRACT,
        # The missing-receipt shape still describes one frozen profile day.
        # Keep its cutoff stable across API calls made in the same Beijing day
        # instead of leaking the wall-clock second into otherwise identical
        # health/overview/scheduler payloads.
        "cutoff_at": day_cutoff_for(at),
        "business_day": _coverage_business_day(at),
        "activation_id": active.get("activation_id") if active else None,
        "profile_id": profile_id,
        "activation_sha256": active.get("activation_sha256") if active else None,
        "source_family": active.get("source_family") if active else None,
        "matrix_expected_windows": matrix_expected,
        "matrix_complete_windows": 0,
        "tikhub_expected_members": None,
        "tikhub_complete_members": 0,
        "status": "unknown",
        "complete": False,
        "reason": reason,
        "roster_snapshot_id": active.get("roster_snapshot_id") if active else None,
        "roster_snapshot_hash": active.get("roster_snapshot_hash") if active else None,
        "round_run_id": None,
        "scan_errors": {},
    }


def _current_hold_control_valid(
    connection: sqlite3.Connection,
    *,
    active: Mapping[str, Any],
    drain_state: Any,
    at: str,
) -> bool:
    if drain_state.state == "open":
        event_id = drain_state.permit_event_id
    elif drain_state.state in {"draining", "sealed"}:
        event_id = drain_state.last_event_id
    else:
        return False
    if type(event_id) is not int:
        return False
    row = connection.execute(
        "SELECT target_activation_id,payload_json,event_type,contract_version "
        "FROM pipeline_paid_drain_events WHERE id=?",
        (event_id,),
    ).fetchone()
    if row is None or int(row["target_activation_id"]) != int(active["activation_id"]):
        return False
    try:
        payload = json.loads(str(row["payload_json"]))
    except (TypeError, ValueError):
        return False
    control = payload.get("control") if isinstance(payload, dict) else None
    if (
        active.get("profile_id") == "integrated_route_v1"
        and isinstance(payload, dict)
        and "control" not in payload
    ):
        # dispatch_state has already validated the native chain and selected
        # this activation's RELEASE. This is control readiness only: it does
        # not qualify a provider, grant paid authority or assert data coverage.
        from .paid_drain import PROFILE_CONTRACT_VERSION

        return bool(
            drain_state.state == "open"
            and drain_state.activation_id == int(active["activation_id"])
            and row["event_type"] == "release"
            and row["contract_version"] == PROFILE_CONTRACT_VERSION
        )
    if isinstance(control, dict) and control.get("contract") == "account-cleanup-release-control-v1":
        # Cleanup releases inherit a sealed operator decision. Their control
        # has a nested activation binding, not the older flat hold contract.
        # Revalidate that inheritance; an open drain alone proves no authority.
        if not drain_state.paid_dispatch_open:
            return False
        try:
            from . import account_cleanup_runtime as cleanup
            from .capture_release import _installed_evidence

            evidence = _installed_evidence(connection, at=at, maintenance_only=True)
            if any(evidence["active"].get(key) != active.get(key) for key in cleanup.ACTIVE_KEYS):
                return False
            cleanup.validate_control(connection, evidence, control, at=at)
            decision = cleanup.validate_decision(evidence, "douyin_user_posts", at)
            return decision is not None
        except (KeyError, TypeError, ValueError, OSError, sqlite3.Error, RuntimeError):
            return False
    return bool(
        isinstance(control, dict)
        and control.get("contract_version") in {"current_activation_hold_v1", "current_activation_forward_release_v1"}
        and control.get("activation_id") == int(active["activation_id"])
        and control.get("roster_snapshot_id") == int(active["roster_snapshot_id"])
        and control.get("roster_snapshot_hash") == active["roster_members_sha256"]
        and control.get("control_purpose")
        in {"hold_begin", "hold_seal", "full_day_release", "forward_only_seal", "forward_only_release"}
    )


def _cleanup_release_evidence(
    connection: sqlite3.Connection, *, active: Mapping[str, Any],
    release_control: Mapping[str, Any], at: str,
) -> dict[str, Any]:
    from . import account_cleanup_runtime as cleanup
    from .capture_release import _installed_evidence

    evidence = _installed_evidence(connection, at=at, maintenance_only=True)
    cleanup.require(
        all(evidence["active"].get(key) == active.get(key) for key in cleanup.ACTIVE_KEYS),
        "Cleanup day release activation differs from the installed generation",
    )
    cleanup.validate_control(connection, evidence, release_control, at=at)
    cleanup.require(cleanup.validate_decision(evidence, "douyin_user_posts", at) is not None,
                    "Cleanup day release lacks its inherited operator decision")
    return evidence


def _required_operations_qualified(
    connection: sqlite3.Connection,
    *,
    active: Mapping[str, Any],
    release_control: Mapping[str, Any],
    at: str,
) -> tuple[bool, str | None]:
    """Validate the selected release contract without upgrading sample qualification."""

    try:
        from .profile_control import (
            ProfileControlError,
            validate_current_hold_release_prerequisites,
        )

        from .forward_recovery import is_forward_release, validate_forward_release

        if release_control.get("contract") == _CLEANUP_CONTROL_CONTRACT:
            from . import account_cleanup_runtime as cleanup, capture_operator_release as operator, provider_budget

            evidence = _cleanup_release_evidence(
                connection, active=active, release_control=release_control, at=at,
            )
            # A transient fault may allow renewal or a bounded half-open probe.
            # Neither makes the operation healthy for a complete-day claim.
            if any((provider_budget.fault_state(connection, scope_kind="operation", operation=operation)
                    or {}).get("open") is True for operation in cleanup.OPERATIONS):
                return False, "required_operation_unqualified"
            for operation in sorted(cleanup.OPERATIONS):
                value = operator.authority(connection, evidence=evidence, operation=operation, at=at)
                cleanup.require(value is not None, "Cleanup operation lacks current operator authority")
                gate = connection.execute(
                    "SELECT * FROM capture_paid_send_gate_events WHERE provider='tikhub' "
                    "AND operation=? AND julianday(recorded_at)<=julianday(?) ORDER BY id DESC LIMIT 1",
                    (operation, at),
                ).fetchone()
                cleanup.require(gate is not None, "Cleanup operation has no issued gate")
                proof = operator._gate_evidence(
                    connection, gate, value=value, evidence=evidence, operation=operation,
                )
                newest = connection.execute(
                    "SELECT id FROM provider_readiness_receipts WHERE provider='tikhub' "
                    "AND operation=? AND julianday(created_at)<=julianday(?) ORDER BY id DESC LIMIT 1",
                    (operation, at),
                ).fetchone()
                cleanup.require(
                    newest is not None and newest[0] == proof["readiness"]["id"]
                    and parse_time(gate["recorded_at"]) <= parse_time(at) < parse_time(proof["expires_at"]),
                    "Cleanup operation gate expired or readiness was superseded",
                )
        elif is_forward_release(release_control):
            validate_forward_release(connection, active=active, release_control=release_control, at=at, check_runtime=False)
        else:
            validate_current_hold_release_prerequisites(
                connection, active=active, release_control=release_control, at=at,
            )
    except ProfileControlError as exc:
        return False, (
            "provider_blocked"
            if exc.code == "provider_blocked"
            else "required_operation_unqualified"
        )
    except (KeyError, TypeError, ValueError, OSError, sqlite3.Error, RuntimeError):
        return False, "required_operation_unqualified"
    return True, None


def current_activation_readiness(
    connection: sqlite3.Connection,
    *,
    at: str,
) -> dict[str, Any]:
    """Evaluate current control/data readiness without re-reading raw evidence."""

    from .paid_drain import dispatch_state
    from .profile_activations import activation_at

    timestamp = _time(at)
    active = activation_at(connection, timestamp)
    if active is None:
        return {
            "control_readiness": False,
            "data_readiness": False,
            "reason": "current_activation_receipt_mismatch",
            "activation": None,
            "receipt": None,
        }
    drain = dispatch_state(connection, at=timestamp)
    control_ready = _current_hold_control_valid(
        connection, active=active, drain_state=drain, at=timestamp
    )
    base = {
        "control_readiness": control_ready,
        "data_readiness": False,
        "activation": {
            "activation_id": int(active["activation_id"]),
            "profile_id": str(active["profile_id"]),
            "roster_snapshot_id": int(active["roster_snapshot_id"]),
            "roster_snapshot_hash": str(active["roster_members_sha256"]),
            "effective_at": str(active["effective_at"]),
        },
        "paid_dispatch_state": drain.state,
        "drain_id": drain.drain_id,
        "receipt": None,
    }
    if drain.state == "closed" and drain.reason == "current_activation_hold_missing":
        return {**base, "reason": "current_activation_hold_missing"}
    if not control_ready or drain.state == "invalid":
        return {**base, "reason": "current_activation_permit_missing"}
    if drain.state in {"draining", "sealed", "closed"}:
        return {**base, "reason": "current_activation_coverage_incomplete"}

    row = _latest_complete_current_day_row(
        connection,
        activation_id=int(active["activation_id"]),
        at=timestamp,
    )
    if row is None:
        return {**base, "reason": "current_activation_coverage_incomplete"}
    business_day = str(row["business_day"])
    day_start = datetime.combine(
        date.fromisoformat(business_day), time.min, BEIJING
    ).astimezone(timezone.utc)
    if parse_time(str(active["effective_at"])) > day_start:
        return {**base, "reason": "activation_transition_day"}
    try:
        receipt = _validate_native_day_row(connection, row)
    except RuntimeReceiptError:
        return {**base, "reason": "current_activation_receipt_mismatch"}
    scope = receipt.get("scope")
    coverage = receipt.get("summary", {}).get("coverage")
    reference = {
        "run_id": receipt.get("run_id"),
        "attempt_id": receipt.get("attempt_id"),
        "sequence": receipt.get("summary", {}).get("sequence"),
        "sealed_at": receipt.get("summary", {}).get("sealed_at"),
        "scope": scope,
        "self_sha256": receipt.get("self_sha256"),
    }
    base["receipt"] = reference
    expected_scope = {
        "activation_id": int(active["activation_id"]),
        "profile_id": str(active["profile_id"]),
        "roster_snapshot_id": int(active["roster_snapshot_id"]),
        "roster_snapshot_hash": str(active["roster_members_sha256"]),
        "business_day": business_day,
    }
    if not isinstance(scope, dict) or any(
        scope.get(key) != value for key, value in expected_scope.items()
    ):
        return {**base, "reason": "current_activation_receipt_mismatch"}
    if (
        int(row["complete"]) != 1
        or not isinstance(coverage, dict)
        or coverage.get("complete") is not True
    ):
        return {**base, "reason": "current_activation_coverage_incomplete"}
    release_event_id = scope.get("release_event_id")
    release = connection.execute(
        "SELECT * FROM pipeline_paid_drain_events WHERE id=? AND event_type='release'",
        (release_event_id,),
    ).fetchone()
    if (
        release is None
        or scope.get("drain_id") != release["drain_id"]
        or scope.get("release_event_hash") != release["event_hash"]
        or scope.get("released_at") != release["created_at"]
        or scope.get("control_contract_version") not in _DAY_RELEASE_CONTRACTS
        or int(release["target_activation_id"]) != int(active["activation_id"])
        or drain.permit_event_id != int(release["id"])
    ):
        return {**base, "reason": "current_activation_release_scope_mismatch"}
    try:
        release_payload = json.loads(str(release["payload_json"]))
    except (TypeError, ValueError):
        return {**base, "reason": "current_activation_release_scope_mismatch"}
    release_control = (
        release_payload.get("control")
        if isinstance(release_payload, dict)
        else None
    )
    from .forward_recovery import is_forward_release

    if not isinstance(release_control, dict) or not (
        (release_control.get("control_purpose") == "full_day_release"
         and release_control.get("release_business_day") == business_day)
        or (is_forward_release(release_control) and str(release_control.get("scope_start", "9999")) <= business_day)
        or (release_control.get("contract") == _CLEANUP_CONTROL_CONTRACT
            and scope.get("control_contract_version") == _CLEANUP_CONTROL_CONTRACT
            and parse_time(str(release["created_at"])) < day_start + timedelta(days=1))
    ):
        return {**base, "reason": "current_activation_release_scope_mismatch"}
    release_ready, readiness_reason = _required_operations_qualified(
        connection,
        active=active,
        release_control=release_control,
        at=timestamp,
    )
    if not release_ready:
        return {
            **base,
            "reason": readiness_reason or "required_operation_unqualified",
        }
    return {
        **base,
        "control_readiness": True,
        "data_readiness": True,
        "reason": None,
    }


def latest_runtime_coverage(connection: sqlite3.Connection, *, at: str) -> dict[str, Any]:
    receipt = read_profile_day_coverage_receipt(connection, at=at)
    if receipt is None:
        active = None
        try:
            active = _active_profile_for_day(
                connection,
                business_day=_coverage_business_day(at),
                at=at,
            )
        except RuntimeReceiptError:
            pass
        return unknown_runtime_coverage(
            at=at, reason="profile_day_receipt_missing", active=active
        )
    coverage = receipt.get("summary", {}).get("coverage")
    if not isinstance(coverage, dict):
        raise RuntimeReceiptError("profile-day coverage summary is invalid")
    return {
        **coverage,
        "receipt": {
            "run_id": receipt["run_id"],
            "attempt_id": receipt["attempt_id"],
            "sequence": receipt["summary"]["sequence"],
            "sealed_at": receipt["summary"]["sealed_at"],
            "scope": receipt["scope"],
            "self_sha256": receipt["self_sha256"],
        },
    }


def period_coverage_from_receipts(
    connection: sqlite3.Connection,
    *,
    period_start: str,
    period_end: str,
    cutoff_at: str,
) -> dict[str, Any]:
    """Aggregate immutable per-day summaries without re-reading raw evidence."""

    start, end = date.fromisoformat(period_start), date.fromisoformat(period_end)
    if end < start:
        raise RuntimeReceiptError("invalid profile-day receipt period")
    days: list[dict[str, Any]] = []
    receipt_refs: list[dict[str, Any]] = []
    scan_errors: dict[str, str] = {}
    missing: list[str] = []
    current = start
    while current <= end:
        day_text = current.isoformat()
        receipt = read_profile_day_coverage_receipt(
            connection, at=cutoff_at, business_day=day_text
        )
        if receipt is None:
            missing.append(day_text)
            days.append(
                {
                    "date": day_text,
                    "known": False,
                    "complete": False,
                    "partial_publishable": False,
                    "reason": "profile_day_receipt_missing",
                    "eligible_identity_ids": [],
                    "succeeded_identity_ids": [],
                    "blocked_identity_ids": [],
                    "not_applicable_identity_ids": [],
                    "accounted_identity_ids": [],
                    "required_identity_ids": [],
                }
            )
            current += timedelta(days=1)
            continue
        coverage = receipt.get("summary", {}).get("coverage")
        if not isinstance(coverage, dict):
            raise RuntimeReceiptError("profile-day coverage summary is invalid")
        day = _coverage_day(coverage, business_day=day_text)
        days.append(day)
        errors = coverage.get("scan_errors", {})
        if isinstance(errors, dict):
            scan_errors.update({str(key): str(value) for key, value in errors.items()})
        receipt_refs.append(
            {
                "business_day": day_text,
                "run_id": receipt["run_id"],
                "attempt_id": receipt["attempt_id"],
                "sequence": receipt["summary"]["sequence"],
                "self_sha256": receipt["self_sha256"],
            }
        )
        current += timedelta(days=1)

    def total(field: str) -> int:
        return sum(len(day.get(field, [])) for day in days if day.get("known"))

    scope_total = total("eligible_identity_ids")
    succeeded = total("succeeded_identity_ids")
    blocked = total("blocked_identity_ids")
    not_applicable = total("not_applicable_identity_ids")
    accounted = total("accounted_identity_ids")
    required = total("required_identity_ids")
    gaps = [
        str(day["date"])
        for day in days
        if day.get("known") and not day.get("complete")
    ]
    complete = not missing and not gaps
    partial_publishable = not complete and not missing and all(
        day.get("complete") or day.get("partial_publishable") for day in days
    )
    percentage = (
        round(100 * succeeded / required, 2)
        if required and not missing
        else 100.0
        if scope_total and not missing
        else None
    )
    discovery = {
        "status": (
            "unknown"
            if missing
            else "not_applicable"
            if not scope_total
            else "available"
            if percentage is not None and percentage >= 90
            else "below_threshold"
        ),
        "covered_identity_occurrence_count": succeeded,
        "eligible_identity_occurrence_count": scope_total,
        "succeeded_identity_occurrence_count": succeeded,
        "blocked_identity_occurrence_count": blocked,
        "not_applicable_identity_occurrence_count": not_applicable,
        "accounted_identity_occurrence_count": accounted,
        "required_identity_occurrence_count": required,
        "accounted_percentage": (
            round(100 * accounted / scope_total, 2)
            if scope_total and not missing
            else 100.0
            if not missing
            else None
        ),
        "observed_occurrence_count": len(days) - len(missing),
        "expected_occurrence_count": len(days),
        "percentage": percentage,
        "eligible_basis": "frozen_profile_roster_identity_occurrences",
        "complete": complete,
        "partial_publishable": partial_publishable,
        "missing_occurrence_dates": missing + gaps,
        "roster_validation_failures": len(missing),
        "success_rule": PROFILE_DAY_CONTRACT,
        "reason": "" if complete else "profile-day receipt coverage is incomplete",
    }
    return {
        "contract_version": PERIOD_RECEIPT_CONTRACT,
        "cutoff_at": _time(cutoff_at),
        "days": days,
        "discovery_coverage": discovery,
        "pipeline_observation": {
            "status": (
                "complete"
                if complete
                else "partial_publishable"
                if partial_publishable
                else "incomplete"
            ),
            "capture_observation_start_date": None,
            "expected_dates": [day["date"] for day in days],
            "legacy_unobserved_dates": missing,
            "pipeline_gap_dates": gaps,
            "zero_content_dates": [],
        },
        "complete": complete,
        "partial_publishable": partial_publishable,
        "roster_evidence_valid": not missing,
        "scan_traceable": not missing,
        "scan_errors": scan_errors,
        "scan_references": receipt_refs,
    }


def day_cutoff_for(at: str) -> str:
    """Return the Beijing midnight ending the covered business day."""

    local = parse_time(at).astimezone(BEIJING)
    midnight = datetime.combine(local.date(), time.min, BEIJING)
    return midnight.astimezone(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )
