"""Append-only acquisition-profile activation and cancellation events."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from contextlib import contextmanager, nullcontext
from datetime import datetime, timezone
from typing import Any, Iterator, Mapping

from .storage import write_lock


CONTRACT_VERSION = "acquisition-profile-activation-v1"
CANCELLATION_CONTRACT_VERSION = "acquisition-profile-cancellation-v1"
CANCELLATION_V2 = "acquisition-profile-cancellation-v2"
ELIGIBILITY_CONTRACT = "activation-eligibility-v1"
MATRIX_PROFILE = "matrix_hybrid_v1"
TIKHUB_PROFILE = "tikhub_managed_v1"
INTEGRATED_PROFILE = "integrated_route_v1"
PROFILE_FAMILIES = {MATRIX_PROFILE: "matrix", TIKHUB_PROFILE: "system", INTEGRATED_PROFILE: "system"}
SYSTEM_PROFILES = frozenset(profile for profile, family in PROFILE_FAMILIES.items() if family == "system")
_SHA256 = re.compile(r"[0-9a-f]{64}")


class ProfileActivationError(ValueError):
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


def _sha(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _timestamp(value: str) -> str:
    if not isinstance(value, str):
        raise ProfileActivationError(
            "activation_time_invalid", "Timezone-aware activation time is required"
        )
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ProfileActivationError(
            "activation_time_invalid", "Timezone-aware activation time is required"
        ) from error
    if parsed.tzinfo is None:
        raise ProfileActivationError(
            "activation_time_invalid", "Timezone-aware activation time is required"
        )
    return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(_timestamp(value).replace("Z", "+00:00"))


def _metadata(value: Mapping[str, Any] | None) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ProfileActivationError(
            "activation_metadata_invalid", "Activation metadata must be an object"
        )
    result = dict(value)
    _canonical(result)
    return result


@contextmanager
def _atomic(connection: sqlite3.Connection) -> Iterator[None]:
    nested = connection.in_transaction
    # A standalone BEGIN IMMEDIATE must hold the process write lock for its
    # whole span (see storage.write_lock); a nested SAVEPOINT already runs
    # inside the caller's locked transaction.
    with nullcontext() if nested else write_lock():
        connection.execute("SAVEPOINT profile_activation" if nested else "BEGIN IMMEDIATE")
        try:
            yield
        except Exception:
            if nested:
                connection.execute("ROLLBACK TO profile_activation")
                connection.execute("RELEASE profile_activation")
            else:
                connection.rollback()
            raise
        else:
            if nested:
                connection.execute("RELEASE profile_activation")
            else:
                connection.commit()


def activation_digest(value: Mapping[str, Any]) -> str:
    """Return the canonical digest shared by migration and runtime writers."""

    keys = (
        "profile_id",
        "roster_snapshot_id",
        "roster_members_sha256",
        "effective_at",
        "contract_version",
        "build_receipt_sha256",
        "previous_activation_id",
        "previous_activation_sha256",
        "actor",
        "reason",
        "metadata",
        "created_at",
    )
    return _sha({key: value.get(key) for key in keys})


def cancellation_digest(value: Mapping[str, Any]) -> str:
    keys = (
        "activation_id",
        "cancelled_at",
        "actor",
        "reason",
        "contract_version",
        "metadata",
        "created_at",
    )
    hashed = {key: value.get(key) for key in keys}
    if value.get("contract_version") == CANCELLATION_V2:
        hashed["cancellation_kind"] = value.get("cancellation_kind")
    return _sha(hashed)


def _activation_row(row: sqlite3.Row | Mapping[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result["activation_id"] = int(result.pop("id"))
    result["metadata"] = json.loads(result.pop("metadata_json"))
    return result


def _cancellation_row(row: sqlite3.Row | Mapping[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result["cancellation_id"] = int(result.pop("id"))
    result["metadata"] = json.loads(result.pop("metadata_json"))
    return result


def _validate_chain(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    schema_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    rows = [
        _activation_row(row)
        for row in connection.execute(
            "SELECT * FROM acquisition_profile_activations ORDER BY id"
        )
    ]
    previous: dict[str, Any] | None = None
    for value in rows:
        expected_id = previous["activation_id"] if previous else None
        expected_sha = previous["activation_sha256"] if previous else None
        if (
            value["previous_activation_id"] != expected_id
            or value["previous_activation_sha256"] != expected_sha
            or value["activation_sha256"] != activation_digest(value)
            or value["profile_id"] not in PROFILE_FAMILIES
            or (value["profile_id"] == INTEGRATED_PROFILE and schema_version != 20)
            or value["contract_version"] != CONTRACT_VERSION
        ):
            raise ProfileActivationError(
                "activation_chain_invalid", "Activation event chain is invalid"
            )
        previous = value
    for row in connection.execute("SELECT * FROM activation_cancellations ORDER BY id"):
        value = _cancellation_row(row)
        if (
            value["contract_version"] not in ({CANCELLATION_CONTRACT_VERSION, CANCELLATION_V2}
                                               if schema_version == 20 else {CANCELLATION_CONTRACT_VERSION})
            or value["cancellation_sha256"] != cancellation_digest(value)
        ):
            raise ProfileActivationError(
                "activation_cancellation_invalid", "Activation cancellation is invalid"
            )
    return rows


def activation_eligibility(connection: sqlite3.Connection, activation: Mapping[str, Any]) -> dict[str, Any]:
    """Future v20 cross-profile rows need a valid pre-effective native chain.

    Independent of activation_at/paid_drain readers to avoid recursive authority
    validation. Historical v19 rows retain their original activation semantics.
    """
    metadata = activation.get("metadata", {})
    from . import account_roster_capture
    if metadata.get("eligibility_contract") == account_roster_capture.ELIGIBILITY_CONTRACT:
        return account_roster_capture.activation_eligibility(connection, activation)
    required = (connection.execute("PRAGMA user_version").fetchone()[0] == 20
                and metadata.get("eligibility_contract") == ELIGIBILITY_CONTRACT)
    result: dict[str, Any] = {"contract_version": ELIGIBILITY_CONTRACT, "required": required,
                             "eligible": not required, "activation_id": activation["activation_id"]}
    if not required:
        return result
    rows = connection.execute("SELECT * FROM pipeline_paid_drain_events WHERE drain_id=? ORDER BY id",
                              (metadata.get("drain_id"),)).fetchall()
    if [row["event_type"] for row in rows[:3]] != ["start", "sealed", "release"]:
        return result
    start, sealed, release = rows[:3]
    for index, row in enumerate((start, sealed, release)):
        payload = json.loads(row["payload_json"])
        value = {key: row[key] for key in ("drain_id", "target_activation_id", "sequence", "event_type",
            "previous_event_id", "previous_event_hash", "contract_version", "created_at")}
        value["payload"] = payload
        if (row["contract_version"] != "pipeline-paid-drain-v2" or row["bridge_run_id"] is not None
                or row["target_activation_id"] != activation["activation_id"] or row["sequence"] != index + 1
                or row["event_hash"] != _sha(value)
                or _parse_time(row["created_at"]) >= _parse_time(activation["effective_at"])):
            return result
        if index and (row["previous_event_id"] != rows[index-1]["id"]
                      or row["previous_event_hash"] != rows[index-1]["event_hash"]):
            return result
    binding = json.loads(start["payload_json"]).get("binding", {})
    sealed_payload = json.loads(sealed["payload_json"])
    released_payload = json.loads(release["payload_json"])
    verification = sealed_payload.get("verification", {})
    if (binding.get("target_activation_id") != activation["activation_id"]
            or binding.get("planned_effective_at") != activation["effective_at"]
            or binding.get("build_receipt_sha256") != activation["build_receipt_sha256"]
            or verification.get("nonblocking") is True
            or sealed_payload.get("start_event_id") != start["id"]
            or sealed_payload.get("start_event_hash") != start["event_hash"]
            or released_payload.get("sealed_event_id") != sealed["id"]
            or released_payload.get("sealed_event_hash") != sealed["event_hash"]):
        return result
    return {**result, "eligible": True, "release_event_id": release["id"],
            "release_event_hash": release["event_hash"], "sealed_event_id": sealed["id"]}


def activation_by_id(
    connection: sqlite3.Connection, activation_id: int
) -> dict[str, Any]:
    rows = _validate_chain(connection)
    match = next(
        (value for value in rows if value["activation_id"] == activation_id), None
    )
    if match is None:
        raise ProfileActivationError(
            "activation_not_found", "Acquisition profile activation does not exist"
        )
    cancellation = connection.execute(
        "SELECT * FROM activation_cancellations WHERE activation_id=?",
        (activation_id,),
    ).fetchone()
    return {
        **match,
        "cancellation": _cancellation_row(cancellation) if cancellation else None,
    }


def activation_at(connection: sqlite3.Connection, at: str) -> dict[str, Any] | None:
    """Resolve the last uncancelled activation effective at the supplied instant."""

    timestamp = _timestamp(at)
    _validate_chain(connection)
    rows = connection.execute(
        """SELECT a.* FROM acquisition_profile_activations a
           WHERE a.effective_at<=?
             AND NOT EXISTS (
                 SELECT 1 FROM activation_cancellations c WHERE c.activation_id=a.id
             )
           ORDER BY a.effective_at DESC,a.id DESC""",
        (timestamp,),
    ).fetchall()
    for row in rows:
        candidate = activation_by_id(connection, int(row["id"]))
        if activation_eligibility(connection, candidate)["eligible"]:
            return candidate
    return None


def append_activation(
    connection: sqlite3.Connection,
    *,
    profile_id: str,
    roster_snapshot_id: int,
    roster_members_sha256: str,
    effective_at: str,
    build_receipt_sha256: str,
    actor: str,
    reason: str = "",
    metadata: Mapping[str, Any] | None = None,
    created_at: str,
) -> dict[str, Any]:
    """Append one activation; accepting a roster alone never calls this API."""

    if profile_id not in PROFILE_FAMILIES:
        raise ProfileActivationError(
            "activation_profile_invalid", "Unknown acquisition profile"
        )
    if profile_id == INTEGRATED_PROFILE and int(connection.execute("PRAGMA user_version").fetchone()[0]) != 20:
        raise ProfileActivationError("activation_schema_invalid", "Integrated acquisition requires schema 20")
    if not _SHA256.fullmatch(str(roster_members_sha256)):
        raise ProfileActivationError(
            "activation_roster_hash_invalid", "Roster hash must be SHA-256"
        )
    if not _SHA256.fullmatch(str(build_receipt_sha256)):
        raise ProfileActivationError(
            "activation_build_invalid", "Build receipt hash must be SHA-256"
        )
    actor = str(actor).strip()
    if not actor:
        raise ProfileActivationError(
            "activation_actor_invalid", "Activation actor is required"
        )
    effective = _timestamp(effective_at)
    created = _timestamp(created_at)
    if _parse_time(created) > _parse_time(effective):
        raise ProfileActivationError(
            "activation_schedule_invalid", "Activation must be scheduled before it is effective"
        )
    metadata_value = _metadata(metadata)
    with _atomic(connection):
        _validate_chain(connection)
        snapshot = connection.execute(
            "SELECT id,source_family,members_sha256 FROM account_roster_snapshots WHERE id=?",
            (roster_snapshot_id,),
        ).fetchone()
        if snapshot is None:
            raise ProfileActivationError(
                "activation_roster_missing", "Activation roster snapshot does not exist"
            )
        if (
            snapshot["source_family"] != PROFILE_FAMILIES[profile_id]
            or snapshot["members_sha256"] != roster_members_sha256
        ):
            raise ProfileActivationError(
                "activation_roster_mismatch",
                "Activation profile, roster family and roster hash must match",
            )
        if connection.execute(
            """SELECT 1 FROM acquisition_profile_activations a
               WHERE a.effective_at=? AND NOT EXISTS (
                   SELECT 1 FROM activation_cancellations c WHERE c.activation_id=a.id
               )""",
            (effective,),
        ).fetchone():
            raise ProfileActivationError(
                "activation_schedule_conflict",
                "An uncancelled activation already occupies this effective time",
            )
        previous_row = connection.execute(
            "SELECT * FROM acquisition_profile_activations ORDER BY id DESC LIMIT 1"
        ).fetchone()
        previous = _activation_row(previous_row) if previous_row else None
        value = {
            "profile_id": profile_id,
            "roster_snapshot_id": int(roster_snapshot_id),
            "roster_members_sha256": roster_members_sha256,
            "effective_at": effective,
            "contract_version": CONTRACT_VERSION,
            "build_receipt_sha256": build_receipt_sha256,
            "previous_activation_id": previous["activation_id"] if previous else None,
            "previous_activation_sha256": previous["activation_sha256"]
            if previous
            else None,
            "actor": actor,
            "reason": str(reason),
            "metadata": metadata_value,
            "created_at": created,
        }
        value["activation_sha256"] = activation_digest(value)
        cursor = connection.execute(
            """INSERT INTO acquisition_profile_activations(
                   profile_id,roster_snapshot_id,roster_members_sha256,effective_at,
                   contract_version,build_receipt_sha256,previous_activation_id,
                   previous_activation_sha256,activation_sha256,actor,reason,
                   metadata_json,created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                value["profile_id"],
                value["roster_snapshot_id"],
                value["roster_members_sha256"],
                value["effective_at"],
                value["contract_version"],
                value["build_receipt_sha256"],
                value["previous_activation_id"],
                value["previous_activation_sha256"],
                value["activation_sha256"],
                value["actor"],
                value["reason"],
                _canonical(value["metadata"]),
                value["created_at"],
            ),
        )
        return activation_by_id(connection, int(cursor.lastrowid or 0))


def cancel_activation(
    connection: sqlite3.Connection,
    activation_id: int,
    *,
    cancelled_at: str,
    actor: str,
    reason: str,
    metadata: Mapping[str, Any] | None = None,
    cancellation_kind: str = "pre_effective",
) -> dict[str, Any]:
    """Cancel a future activation with a separate immutable event."""

    actor = str(actor).strip()
    reason = str(reason).strip()
    if not actor or not reason:
        raise ProfileActivationError(
            "activation_cancellation_invalid", "Cancellation actor and reason are required"
        )
    timestamp = _timestamp(cancelled_at)
    metadata_value = _metadata(metadata)
    schema20 = connection.execute("PRAGMA user_version").fetchone()[0] == 20
    if cancellation_kind not in {"pre_effective", "never_eligible_cleanup"} or (not schema20 and cancellation_kind != "pre_effective"):
        raise ProfileActivationError("activation_cancellation_invalid", "Unsupported cancellation kind")
    with _atomic(connection):
        activation = activation_by_id(connection, activation_id)
        if activation["cancellation"] is not None:
            raise ProfileActivationError(
                "activation_already_cancelled", "Activation was already cancelled"
            )
        late = _parse_time(timestamp) >= _parse_time(activation["effective_at"])
        eligibility = activation_eligibility(connection, activation)
        if cancellation_kind == "never_eligible_cleanup":
            if (not late or not eligibility["required"] or eligibility["eligible"]
                    or connection.execute("SELECT 1 FROM paid_provider_dispatch_events WHERE activation_id=? AND event_type='send_marked' LIMIT 1", (activation_id,)).fetchone()):
                raise ProfileActivationError("activation_cleanup_ineligible", "Cleanup requires a never-eligible, never-sent target")
            metadata_value = {**metadata_value, "eligibility": eligibility}
        elif late:
            raise ProfileActivationError(
                "activation_already_effective", "An effective activation cannot be cancelled"
            )
        value = {
            "activation_id": activation_id,
            "cancelled_at": timestamp,
            "actor": actor,
            "reason": reason,
            "contract_version": CANCELLATION_V2 if schema20 else CANCELLATION_CONTRACT_VERSION,
            "metadata": metadata_value,
            "created_at": timestamp,
        }
        if schema20:
            value["cancellation_kind"] = cancellation_kind
        value["cancellation_sha256"] = cancellation_digest(value)
        columns = ",cancellation_kind" if schema20 else ""
        placeholder = ",?" if schema20 else ""
        cursor = connection.execute(
            """INSERT INTO activation_cancellations(
                   activation_id,cancelled_at,actor,reason,contract_version,
                   cancellation_sha256,metadata_json,created_at""" + columns + ") VALUES (?,?,?,?,?,?,?,?" + placeholder + ")",
            (
                value["activation_id"],
                value["cancelled_at"],
                value["actor"],
                value["reason"],
                value["contract_version"],
                value["cancellation_sha256"],
                _canonical(value["metadata"]),
                value["created_at"],
            ) + ((cancellation_kind,) if schema20 else ()),
        )
        row = connection.execute(
            "SELECT * FROM activation_cancellations WHERE id=?",
            (cursor.lastrowid,),
        ).fetchone()
        if row is None:
            raise ProfileActivationError(
                "activation_cancellation_invalid", "Cancellation insert was not retained"
            )
        return _cancellation_row(row)


def replace_scheduled_activation(
    connection: sqlite3.Connection,
    activation_id: int,
    *,
    profile_id: str,
    roster_snapshot_id: int,
    roster_members_sha256: str,
    effective_at: str,
    build_receipt_sha256: str,
    actor: str,
    reason: str,
    metadata: Mapping[str, Any] | None = None,
    created_at: str,
) -> dict[str, Any]:
    """Atomically append cancellation and replacement at one future slot."""

    with _atomic(connection):
        old = activation_by_id(connection, activation_id)
        if _timestamp(effective_at) != old["effective_at"]:
            raise ProfileActivationError(
                "activation_replacement_slot_mismatch",
                "Replacement must retain the cancelled effective time",
            )
        cancel_activation(
            connection,
            activation_id,
            cancelled_at=created_at,
            actor=actor,
            reason=reason,
            metadata={"replacement": True},
        )
        return append_activation(
            connection,
            profile_id=profile_id,
            roster_snapshot_id=roster_snapshot_id,
            roster_members_sha256=roster_members_sha256,
            effective_at=effective_at,
            build_receipt_sha256=build_receipt_sha256,
            actor=actor,
            reason=reason,
            metadata=metadata,
            created_at=created_at,
        )
