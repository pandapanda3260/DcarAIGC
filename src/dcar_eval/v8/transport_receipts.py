"""Schema-19 durable receipts for transport diagnostic control evidence.

This ledger only records immutable evidence.  A receipt is never a paid-send
authorization and is intentionally not exposed through the HTTP control API.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
from collections.abc import Mapping
from datetime import timezone
from pathlib import Path
from typing import Any

from . import artifact_paths
from .raw_evidence import (
    MAX_SIDECAR_BYTES,
    RawEvidenceError,
    _read_single_regular,
    canonical_json_bytes,
    write_immutable_json_receipt,
)
from .source_routing import parse_time

CONTRACT_VERSION = "transport-receipt-v1"
KINDS = frozenset(
    {"cohort", "campaign", "member_permit", "campaign_terminal", "qualification", "accounting_terminal", "route_verdict"}
)
_CORE_KEYS = frozenset(
    {
        "contract_version",
        "receipt_id",
        "run_id",
        "attempt_id",
        "kind",
        "identity_key",
        "job_id",
        "scheduled_for",
        "recorded_at",
        "payload",
        "payload_sha256",
        "self_sha256",
    }
)
_DETAIL_KEYS = _CORE_KEYS | {"mirror"}
_MIRROR_KEYS = frozenset({"path", "sha256", "byte_size"})


class TransportReceiptError(RuntimeError):
    """The transport receipt ledger or its immutable mirror is invalid."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _canonical(value: object) -> bytes:
    try:
        return canonical_json_bytes(value)
    except ValueError as exc:
        raise TransportReceiptError(
            "transport_receipt_payload_invalid",
            "Transport receipt value is not canonical JSON",
        ) from exc


def _canonical_text(value: object) -> str:
    return _canonical(value).decode("utf-8").removesuffix("\n")


def _frozen_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    try:
        frozen = json.loads(_canonical(value).decode("utf-8"))
    except (TypeError, ValueError) as exc:
        raise TransportReceiptError(
            "transport_receipt_payload_invalid",
            "Transport receipt payload must be a canonical JSON object",
        ) from exc
    if not isinstance(frozen, dict):
        raise TransportReceiptError(
            "transport_receipt_payload_invalid",
            "Transport receipt payload must be a canonical JSON object",
        )
    return frozen


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _timestamp(value: str) -> str:
    try:
        parsed = parse_time(value).astimezone(timezone.utc)
    except (TypeError, ValueError) as exc:
        raise TransportReceiptError(
            "transport_receipt_time_invalid",
            "Transport receipt time must be timezone-aware",
        ) from exc
    return parsed.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _kind(value: str) -> str:
    if not isinstance(value, str) or value not in KINDS:
        raise TransportReceiptError(
            "transport_receipt_kind_invalid", "Transport receipt kind is unsupported"
        )
    return value


def _identity(value: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or "\x00" in value
        or len(value) > 1024
    ):
        raise TransportReceiptError(
            "transport_receipt_identity_invalid",
            "Transport receipt identity key is invalid",
        )
    return value


def _mirror_path(*, root: Path, kind: str, identity_key: str) -> Path:
    resolved = Path(root).expanduser()
    if not resolved.is_absolute() or resolved.is_symlink():
        raise TransportReceiptError(
            "transport_receipt_mirror_invalid",
            "Transport receipt mirror root must be absolute and non-symlink",
        )
    identity_digest = hashlib.sha256(identity_key.encode("utf-8")).hexdigest()
    return resolved / f"transport-receipt.{kind}.{identity_digest}.json"


def _verify_private_parent(path: Path) -> None:
    try:
        parent = path.parent.lstat()
    except FileNotFoundError as exc:
        raise TransportReceiptError(
            "transport_receipt_mirror_missing",
            "Transport receipt mirror directory is missing",
        ) from exc
    if (
        stat.S_ISLNK(parent.st_mode)
        or not stat.S_ISDIR(parent.st_mode)
        or parent.st_uid != os.geteuid()
        or stat.S_IMODE(parent.st_mode) & 0o077
    ):
        raise TransportReceiptError(
            "transport_receipt_mirror_invalid",
            "Transport receipt mirror directory is not private",
        )


def _verify_mirror(
    core: Mapping[str, Any], mirror: Mapping[str, Any]
) -> dict[str, Any]:
    if frozenset(mirror) != _MIRROR_KEYS:
        raise TransportReceiptError(
            "transport_receipt_mirror_invalid",
            "Transport receipt mirror binding is malformed",
        )
    path_value = mirror.get("path")
    digest = mirror.get("sha256")
    byte_size = mirror.get("byte_size")
    if (
        not isinstance(path_value, str)
        or not Path(path_value).is_absolute()
        or not isinstance(digest, str)
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
        or type(byte_size) is not int
        or byte_size < 0
        or byte_size > MAX_SIDECAR_BYTES
    ):
        raise TransportReceiptError(
            "transport_receipt_mirror_invalid",
            "Transport receipt mirror binding is invalid",
        )
    path = Path(path_value)
    try:
        relocated = artifact_paths.resolve(path)
        replica = artifact_paths.replica_file(relocated)
        if relocated != path and replica is not None:
            if replica.get("sha256") != digest or replica.get("byte_size") != byte_size:
                raise TransportReceiptError("transport_receipt_mirror_invalid", "Replica mirror binding differs")
            body = _canonical(artifact_paths._object(
                relocated, maximum_bytes=MAX_SIDECAR_BYTES, expected_sha256=digest,
            ))
        else:
            _verify_private_parent(path)
            body = _read_single_regular(path, max_bytes=MAX_SIDECAR_BYTES)
    except (RawEvidenceError, artifact_paths.ArtifactPathError) as exc:
        code = (
            "transport_receipt_mirror_missing"
            if not path.exists() and not path.is_symlink()
            else "transport_receipt_mirror_invalid"
        )
        raise TransportReceiptError(
            code, "Transport receipt mirror is missing or unsafe"
        ) from exc
    expected = _canonical(dict(core))
    if (
        body != expected
        or len(body) != byte_size
        or hashlib.sha256(body).hexdigest() != digest
    ):
        raise TransportReceiptError(
            "transport_receipt_mirror_invalid",
            "Transport receipt mirror differs from its durable binding",
        )
    return {"path": str(path), "sha256": digest, "byte_size": byte_size}


def _decode_details(value: object) -> dict[str, Any]:
    if not isinstance(value, str):
        raise TransportReceiptError(
            "transport_receipt_db_invalid", "Transport receipt DB payload is invalid"
        )
    try:
        details = json.loads(value)
    except (TypeError, ValueError) as exc:
        raise TransportReceiptError(
            "transport_receipt_db_invalid", "Transport receipt DB payload is invalid"
        ) from exc
    if (
        not isinstance(details, dict)
        or frozenset(details) != _DETAIL_KEYS
        or _canonical_text(details) != value
    ):
        raise TransportReceiptError(
            "transport_receipt_db_invalid",
            "Transport receipt DB payload is not canonical or complete",
        )
    return details


def read_transport_receipt(
    connection: sqlite3.Connection, receipt_id: int
) -> dict[str, Any]:
    """Read and fully verify one durable transport receipt and its mirror."""

    if type(receipt_id) is not int or receipt_id < 1:
        raise TransportReceiptError(
            "transport_receipt_id_invalid", "Transport receipt ID is invalid"
        )
    row = connection.execute(
        "SELECT * FROM scheduler_runs WHERE id=?", (receipt_id,)
    ).fetchone()
    if row is None:
        raise TransportReceiptError(
            "transport_receipt_missing", "Transport receipt does not exist"
        )
    attempts = connection.execute(
        "SELECT * FROM scheduler_run_attempts WHERE scheduler_run_id=? ORDER BY id",
        (receipt_id,),
    ).fetchall()
    details = _decode_details(row["details_json"])
    core = {key: details[key] for key in _CORE_KEYS}
    kind = _kind(core["kind"])
    identity_key = _identity(core["identity_key"])
    job_id = f"transport_receipt:{kind}"
    recorded_at = _timestamp(core["recorded_at"])
    if (
        row["job_id"] != job_id
        or row["scheduled_for"] != identity_key
        or row["status"] != "succeeded"
        or row["started_at"] != recorded_at
        or row["completed_at"] != recorded_at
        or len(attempts) != 1
    ):
        raise TransportReceiptError(
            "transport_receipt_db_invalid",
            "Transport receipt scheduler run is not its immutable terminal form",
        )
    attempt = attempts[0]
    if (
        attempt["attempt_number"] != 1
        or attempt["invocation_source"] != "operator_retry"
        or attempt["status"] != "succeeded"
        or attempt["started_at"] != recorded_at
        or attempt["completed_at"] != recorded_at
        or attempt["details_json"] != row["details_json"]
        or int(attempt["scheduler_run_id"]) != receipt_id
        or details["contract_version"] != CONTRACT_VERSION
        or details["receipt_id"] != receipt_id
        or details["run_id"] != receipt_id
        or details["attempt_id"] != int(attempt["id"])
        or details["job_id"] != job_id
        or details["scheduled_for"] != identity_key
        or details["recorded_at"] != recorded_at
    ):
        raise TransportReceiptError(
            "transport_receipt_db_invalid",
            "Transport receipt attempt and run bindings differ",
        )
    payload = details["payload"]
    if not isinstance(payload, dict) or details["payload_sha256"] != _digest(payload):
        raise TransportReceiptError(
            "transport_receipt_payload_invalid",
            "Transport receipt payload hash is invalid",
        )
    self_hashed = dict(core)
    self_sha256 = self_hashed.pop("self_sha256", None)
    if self_sha256 != _digest(self_hashed):
        raise TransportReceiptError(
            "transport_receipt_db_invalid", "Transport receipt self hash is invalid"
        )
    mirror = details["mirror"]
    if not isinstance(mirror, dict):
        raise TransportReceiptError(
            "transport_receipt_mirror_invalid",
            "Transport receipt mirror binding is invalid",
        )
    verified_mirror = _verify_mirror(core, mirror)
    return _frozen_mapping({**core, "mirror": verified_mirror})


def append_transport_receipt(
    connection: sqlite3.Connection,
    *,
    kind: str,
    identity_key: str,
    payload: Mapping[str, Any],
    at: str,
    mirror_root: Path,
) -> dict[str, Any]:
    """Append one mirrored terminal receipt under the caller's transaction."""

    if not connection.in_transaction:
        raise TransportReceiptError(
            "transport_receipt_transaction_required",
            "Transport receipt append requires a caller transaction",
        )
    receipt_kind = _kind(kind)
    identity = _identity(identity_key)
    timestamp = _timestamp(at)
    frozen_payload = _frozen_mapping(payload)
    job_id = f"transport_receipt:{receipt_kind}"
    existing = connection.execute(
        "SELECT id FROM scheduler_runs WHERE job_id=? AND scheduled_for=?",
        (job_id, identity),
    ).fetchone()
    if existing is not None:
        receipt = read_transport_receipt(connection, int(existing["id"]))
        if receipt["payload"] != frozen_payload:
            raise TransportReceiptError(
                "transport_receipt_idempotency_conflict",
                "Transport receipt identity already has another payload",
            )
        return receipt

    savepoint = "transport_receipt_append"
    connection.execute(f"SAVEPOINT {savepoint}")
    try:
        run = connection.execute(
            "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,details_json) "
            "VALUES (?,?,'running',?,'{}')",
            (job_id, identity, timestamp),
        )
        run_id = int(run.lastrowid or 0)
        attempt = connection.execute(
            "INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,"
            "invocation_source,status,started_at,details_json) "
            "VALUES (?,1,'operator_retry','running',?,'{}')",
            (run_id, timestamp),
        )
        attempt_id = int(attempt.lastrowid or 0)
        core: dict[str, Any] = {
            "contract_version": CONTRACT_VERSION,
            "receipt_id": run_id,
            "run_id": run_id,
            "attempt_id": attempt_id,
            "kind": receipt_kind,
            "identity_key": identity,
            "job_id": job_id,
            "scheduled_for": identity,
            "recorded_at": timestamp,
            "payload": frozen_payload,
            "payload_sha256": _digest(frozen_payload),
        }
        core["self_sha256"] = _digest(core)
        path = _mirror_path(root=mirror_root, kind=receipt_kind, identity_key=identity)
        try:
            written = write_immutable_json_receipt(
                path, core, evidence_root=Path(mirror_root).expanduser()
            )
        except (OSError, RawEvidenceError, ValueError) as exc:
            raise TransportReceiptError(
                "transport_receipt_mirror_invalid",
                "Transport receipt mirror could not be published immutably",
            ) from exc
        mirror = {
            "path": str(written.path),
            "sha256": written.sha256,
            "byte_size": written.byte_size,
        }
        _verify_mirror(core, mirror)
        details = _frozen_mapping({**core, "mirror": mirror})
        encoded = _canonical_text(details)
        attempt_update = connection.execute(
            "UPDATE scheduler_run_attempts SET status='succeeded',completed_at=?,"
            "details_json=? WHERE id=? AND scheduler_run_id=? AND status='running'",
            (timestamp, encoded, attempt_id, run_id),
        )
        run_update = connection.execute(
            "UPDATE scheduler_runs SET status='succeeded',completed_at=?,details_json=? "
            "WHERE id=? AND status='running'",
            (timestamp, encoded, run_id),
        )
        if attempt_update.rowcount != 1 or run_update.rowcount != 1:
            raise TransportReceiptError(
                "transport_receipt_finalize_failed",
                "Transport receipt did not reach its terminal DB state",
            )
        verified = read_transport_receipt(connection, run_id)
    except BaseException:
        connection.execute(f"ROLLBACK TO {savepoint}")
        connection.execute(f"RELEASE {savepoint}")
        raise
    connection.execute(f"RELEASE {savepoint}")
    return verified
