"""Independent integrated-operation issuance over one accepted schema20 install.

The migration acceptance is never copied or rewritten. A source qualification
snapshot is frozen into the new activation's hashed metadata before START.
The successor inherits only unchanged installation/transport evidence, never
the source's paid authorization or its continuity permit hash.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping, Sequence
from datetime import timedelta
from pathlib import Path
from typing import Any

from . import capture_authorizations as auth, capture_release as release
from . import paid_drain, provider_budget
from .metric_field_facts import utc
from .profile_activations import (
    ELIGIBILITY_CONTRACT,
    INTEGRATED_PROFILE,
    TIKHUB_PROFILE,
    activation_at,
    activation_by_id,
    activation_eligibility,
)
from .runtime_database import require_current_process_writer_lock
from .source_routing import parse_time
from .storage import connect

SOURCE_CONTRACT = "capture-activation-source-v1"
SUCCESSOR_CONTRACT = "capture-installed-activation-successor-v1"
ISSUANCE_CONTRACT = "capture-integrated-operation-issuance-v1"
METADATA_KEY = "capture_operation_source"
_ACTIVE_KEYS = (
    "activation_id",
    "profile_id",
    "roster_snapshot_id",
    "roster_members_sha256",
    "activation_sha256",
)
_RUNTIME_KEYS = ("build_sha256", "runtime_sha256", "config_sha256")


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise auth.AuthorizationError(message)


def _copy(value: Any) -> Any:
    return json.loads(auth.canonical(value))


def _active(value: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value[key] for key in _ACTIVE_KEYS}


def _runtime(evidence: Mapping[str, Any]) -> dict[str, Any]:
    return {key: evidence[key] for key in _RUNTIME_KEYS}


def _source_shape(
    snapshot: Mapping[str, Any], *, at: str, require_unexpired: bool = True
) -> dict[str, Any]:
    value = _copy(snapshot)
    _require(
        isinstance(value, dict) and value.get("contract") == SOURCE_CONTRACT,
        "Integrated activation has no frozen source qualification snapshot",
    )
    checksum = value.pop("snapshot_sha256", None)
    _require(
        checksum == auth.digest(value), "Source qualification snapshot SHA changed"
    )
    value["snapshot_sha256"] = checksum
    _require(
        value["source_active"]["profile_id"] == TIKHUB_PROFILE
        and parse_time(value["frozen_at"]) <= parse_time(at),
        "Source qualification is not a prior Mode B snapshot",
    )
    operations = value.get("operations")
    _require(
        isinstance(operations, dict)
        and bool(operations)
        and set(operations) <= release.CONTINUITY_OPERATIONS,
        "Source snapshot operation scope is invalid",
    )
    for operation, qualification in operations.items():
        _require(
            qualification.get("operation") == operation
            and (
                not require_unexpired
                or parse_time(at) < parse_time(qualification["expires_at"])
            ),
            "Source operation qualification expired or operation changed",
        )
        _require(
            qualification.get("runtime_bindings")
            == {"active": value["source_active"], **value["runtime_bindings"]},
            "Frozen operation runtime differs from source snapshot",
        )
        _require(
            qualification.get("manifest") == value["manifest"]
            and qualification.get("transport_code_sha256")
            == value["transport_code_sha256"],
            "Frozen source operation transport differs",
        )
    return value


def _verify_frozen(
    connection: sqlite3.Connection,
    snapshot: Mapping[str, Any],
    *,
    at: str,
    qualify_operations: bool = True,
    portable_deployment: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    _require(portable_deployment is None or not qualify_operations,
             "Portable installation proof cannot qualify paid operations")
    value = _source_shape(snapshot, at=at, require_unexpired=qualify_operations)
    source = activation_by_id(connection, value["source_active"]["activation_id"])
    _require(
        source.get("cancellation") is None
        and _active(source) == value["source_active"],
        "Frozen source activation was cancelled or changed",
    )
    _require(
        value["transport_code_sha256"] == release._transport_code(),
        "Source transport implementation changed",
    )
    if qualify_operations:
        for qualification in value["operations"].values():
            release.validate_frozen_operation_qualification(
                connection, qualification, at=at
            )
    else:
        # The deployment decision remains authoritative after the source gate
        # expires; still revalidate its private receipt and frozen evidence.
        from .capture_operator_release import validate_frozen
        for qualification in value["operations"].values():
            if qualification.get("qualification_kind") == "operator_authorized":
                validate_frozen(connection, qualification, at=at, require_unexpired=False,
                                portable_deployment=portable_deployment)
    row = connection.execute(
        "SELECT event_hash,target_activation_id FROM pipeline_paid_drain_events "
        "WHERE id=? AND event_type='release'",
        (value["source_release"]["event_id"],),
    ).fetchone()
    _require(
        row is not None
        and row["event_hash"] == value["source_release"]["event_hash"]
        and row["target_activation_id"] == source["activation_id"],
        "Frozen source RELEASE changed",
    )
    return value


def snapshot_source_operations(
    connection: sqlite3.Connection, *, operations: Sequence[str], at: str
) -> dict[str, Any]:
    """Read-only pre-BEGIN qualification; this snapshot grants no authority."""
    names = list(operations)
    _require(
        bool(names)
        and len(names) == len(set(names))
        and set(names) <= release.CONTINUITY_OPERATIONS,
        "Source operations must be a nonempty unique supported set",
    )
    evidence = release._installed_evidence(connection, at=at)
    _require(
        evidence["active"]["profile_id"] == TIKHUB_PROFILE
        and evidence["deployment"]["status"] == "accepted",
        "Integrated BEGIN requires an accepted Mode B source",
    )
    state = paid_drain.dispatch_state(connection, at=at)
    _require(
        state.paid_dispatch_open
        and state.activation_id == evidence["active"]["activation_id"]
        and state.permit_event_id is not None,
        "Source is held or has no current RELEASE",
    )
    row = connection.execute(
        "SELECT event_hash FROM pipeline_paid_drain_events WHERE id=? AND event_type='release'",
        (state.permit_event_id,),
    ).fetchone()
    _require(row is not None, "Source RELEASE is missing")
    value = {
        "contract": SOURCE_CONTRACT,
        "frozen_at": utc(at),
        "source_active": _active(evidence["active"]),
        "runtime_bindings": _runtime(evidence),
        "manifest": evidence["manifest"],
        "transport_code_sha256": release._transport_code(),
        "source_deployment": {
            key: evidence["deployment"][key]
            for key in ("deployment_id", "receipt_sha256", "bindings")
        },
        "source_release": {
            "event_id": state.permit_event_id,
            "event_hash": row["event_hash"],
        },
        "operations": {
            name: release.snapshot_operation_qualification(
                connection, operation=name, at=at
            )
            for name in sorted(names)
        },
    }
    value["snapshot_sha256"] = auth.digest(value)
    return _source_shape(value, at=at)


def validate_source_snapshot(
    connection: sqlite3.Connection, snapshot: Mapping[str, Any], *, at: str
) -> dict[str, Any]:
    """Recheck caller-provided snapshot immediately before writing the target."""
    value = _verify_frozen(connection, snapshot, at=at)
    evidence = release._installed_evidence(connection, at=at)
    _require(
        evidence["deployment"]["status"] == "accepted"
        and _active(evidence["active"]) == value["source_active"]
        and _runtime(evidence) == value["runtime_bindings"]
        and evidence["manifest"] == value["manifest"]
        and all(
            evidence["deployment"][key] == expected
            for key, expected in value["source_deployment"].items()
        ),
        "Source installation, profile or transport changed before BEGIN",
    )
    state = paid_drain.dispatch_state(connection, at=at)
    _require(
        state.paid_dispatch_open
        and state.permit_event_id == value["source_release"]["event_id"],
        "Source drain changed before BEGIN",
    )
    for operation, qualification in value["operations"].items():
        for table, column, field in (
            ("capture_paid_send_gate_events", "recorded_at", "gate_id"),
            ("provider_readiness_receipts", "created_at", "readiness_id"),
        ):
            row = connection.execute(
                f"SELECT id FROM {table} WHERE provider='tikhub' AND operation=? AND {column}<=? ORDER BY id DESC LIMIT 1",
                (operation, utc(at)),
            ).fetchone()
            _require(
                row is not None and row[0] == qualification[field],
                "Source operation qualification was superseded before BEGIN",
            )
    return value


def begin_integrated_switch(
    *,
    db_path: Path,
    drain_id: str,
    roster_snapshot_id: int,
    operations: Sequence[str],
    actor: str,
    reason: str,
    now: str,
) -> dict[str, Any]:
    """Freeze real current source qualifications, then use the existing BEGIN."""
    from . import profile_control

    with connect(db_path) as connection:
        existing = connection.execute(
            "SELECT target_activation_id FROM pipeline_paid_drain_events WHERE drain_id=? AND event_type='start'",
            (drain_id,),
        ).fetchone()
        if existing is None:
            snapshot = snapshot_source_operations(
                connection, operations=operations, at=now
            )
        else:
            target = activation_by_id(connection, existing["target_activation_id"])
            frozen = target.get("metadata", {}).get(METADATA_KEY)
            _require(
                isinstance(frozen, Mapping),
                "Existing BEGIN has no qualified source snapshot",
            )
            assert isinstance(frozen, Mapping)
            snapshot = _verify_frozen(connection, frozen, at=now)
            _require(
                target["roster_snapshot_id"] == roster_snapshot_id
                and sorted(operations) == sorted(snapshot["operations"]),
                "Existing integrated BEGIN operation or roster scope changed",
            )
    return profile_control.begin_cross_profile_switch(
        db_path=db_path,
        drain_id=drain_id,
        target_profile_id=INTEGRATED_PROFILE,
        roster_snapshot_id=roster_snapshot_id,
        build_receipt_sha256=snapshot["runtime_bindings"]["build_sha256"],
        runtime_root_receipt_sha256=snapshot["runtime_bindings"]["runtime_sha256"],
        actor=actor,
        reason=reason,
        now=now,
        capture_source_operations=snapshot,
    )


def validate_installed_activation_successor(
    connection: sqlite3.Connection,
    *,
    source_deployment: Mapping[str, Any],
    current_active: Mapping[str, Any],
    runtime_bindings: Mapping[str, Any],
    manifest: Mapping[str, Any],
    at: str,
    portable: bool = False,
) -> dict[str, Any]:
    """Narrow exception to activation equality, not to installation acceptance.

    Called only after the existing deployment validator has verified the real
    accepted receipt/files. The caller must still perform every existing live
    migration/install/source/archive/inode check after this returns.
    """
    _require(
        connection.execute("PRAGMA user_version").fetchone()[0] == 20,
        "Activation successor requires exact schema20",
    )
    active = activation_at(connection, at)
    _require(
        active is not None
        and _active(active) == _active(current_active)
        and active["profile_id"] == INTEGRATED_PROFILE
        and active.get("cancellation") is None,
        "Integrated target is not the current uncancelled activation",
    )
    assert active is not None
    metadata = active.get("metadata", {})
    from . import account_roster_capture
    if metadata.get("eligibility_contract") == account_roster_capture.ELIGIBILITY_CONTRACT:
        return account_roster_capture.validate_installed_successor(
            connection, source_deployment=source_deployment, current_active=active,
            runtime_bindings=runtime_bindings, manifest=manifest, at=at, portable=portable)
    _require(
        metadata.get("eligibility_contract") == ELIGIBILITY_CONTRACT
        and isinstance(metadata.get(METADATA_KEY), Mapping),
        "Integrated target has no qualified source snapshot",
    )
    # Installation succession is common to every operation. Its immutable
    # metadata remains valid after one operation expires; paid admission below
    # separately revalidates this request's exact qualification and TTL.
    snapshot = _verify_frozen(
        connection, metadata[METADATA_KEY], at=at, qualify_operations=False,
        portable_deployment=source_deployment if portable else None,
    )
    _require(
        source_deployment.get("status") == "accepted"
        and all(
            source_deployment.get(key) == expected
            for key, expected in snapshot["source_deployment"].items()
        ),
        "Successor does not reference its accepted source deployment",
    )
    _require(
        dict(runtime_bindings) == snapshot["runtime_bindings"]
        and dict(manifest) == snapshot["manifest"],
        "Successor build, runtime, configuration or transport changed",
    )
    for qualification in snapshot["operations"].values():
        if qualification.get("qualification_kind") == "operator_authorized":
            _require(qualification.get("release_decision") == source_deployment.get("release_decision")
                     and all(active[key] == snapshot["source_active"][key]
                             for key in ("roster_snapshot_id", "roster_members_sha256")),
                     "Operator successor decision or roster changed")
    _require(
        all(
            source_deployment["bindings"][key] == snapshot["source_active"][key]
            for key in _ACTIVE_KEYS
        ),
        "Accepted source deployment activation differs",
    )
    eligibility = activation_eligibility(connection, active)
    _require(
        eligibility.get("required") is True and eligibility.get("eligible") is True,
        "Target has no legal pre-effective START/SEALED/RELEASE",
    )
    state = paid_drain.dispatch_state(connection, at=at)
    _require(
        state.paid_dispatch_open
        and state.activation_id == active["activation_id"]
        and state.permit_event_id == eligibility["release_event_id"],
        "Target drain or RELEASE changed",
    )
    start = connection.execute(
        "SELECT * FROM pipeline_paid_drain_events WHERE drain_id=? AND event_type='start'",
        (metadata["drain_id"],),
    ).fetchone()
    _require(start is not None, "Target START is absent")
    assert start is not None
    binding = json.loads(start["payload_json"])["binding"]
    _require(
        binding["source_activation_id"] == snapshot["source_active"]["activation_id"]
        and binding["target_activation_id"] == active["activation_id"]
        and binding["build_receipt_sha256"]
        == snapshot["runtime_bindings"]["build_sha256"]
        and binding["runtime_root_receipt_sha256"]
        == snapshot["runtime_bindings"]["runtime_sha256"]
        and parse_time(snapshot["frozen_at"]) <= parse_time(start["created_at"])
        and parse_time(start["created_at"]) < parse_time(active["effective_at"]),
        "Target START is not bound to its pre-frozen qualified source",
    )
    proof = {
        "contract": SUCCESSOR_CONTRACT,
        "source_snapshot_sha256": snapshot["snapshot_sha256"],
        "source_deployment_sha256": source_deployment["receipt_sha256"],
        "target_active": _active(active),
        "runtime_bindings": dict(runtime_bindings),
        "manifest_sha256": auth.digest(manifest),
        "target_release_event_id": eligibility["release_event_id"],
        "target_release_event_hash": eligibility["release_event_hash"],
        "operations": {
            operation: {
                "snapshot_sha256": qualification["snapshot_sha256"],
                "expires_at": qualification["expires_at"],
            }
            for operation, qualification in snapshot["operations"].items()
        },
    }
    return {**proof, "successor_sha256": auth.digest(proof)}


def _target_authority(
    connection: sqlite3.Connection, *, operation: str, at: str
) -> tuple[dict[str, Any], dict[str, Any], str]:
    evidence = release._installed_evidence(connection, at=at)
    from .capture_operator_release import authority as operator_authority
    approved = operator_authority(connection, evidence=evidence, operation=operation, at=at)
    if approved is not None:
        return evidence, approved["bindings"], utc((parse_time(at) + timedelta(hours=24)).isoformat())
    successor = evidence.get("activation_successor")
    _require(
        isinstance(successor, dict) and successor.get("contract") == SUCCESSOR_CONTRACT,
        "Installed integrated activation lacks successor qualification",
    )
    assert isinstance(successor, dict)
    proof = {
        key: value for key, value in successor.items() if key != "successor_sha256"
    }
    _require(
        auth.digest(proof) == successor.get("successor_sha256")
        and evidence["active"]["profile_id"] == INTEGRATED_PROFILE
        and successor["target_active"] == _active(evidence["active"])
        and operation in successor["operations"],
        "Target operation has no frozen source qualification",
    )
    expires = utc(successor["operations"][operation]["expires_at"])
    _require(utc(at) < expires, "Target operation source qualification expired")
    from .account_roster_capture import frozen_operation as source_operation
    frozen_operation = source_operation(evidence["active"], operation)
    release.validate_frozen_operation_qualification(connection, frozen_operation, at=at)
    issuance = {
        "contract": ISSUANCE_CONTRACT,
        "operation": operation,
        "successor_sha256": successor["successor_sha256"],
        "source_operation_snapshot_sha256": successor["operations"][operation][
            "snapshot_sha256"
        ],
        "expires_at": expires,
    }
    # The existing field name is wire compatibility only: the value is a NEW
    # target issuance digest and is never a transport_continuity_permits hash.
    bindings = {
        **{
            key: evidence["active"][key]
            for key in _ACTIVE_KEYS
            if key != "activation_sha256"
        },
        "build_receipt_sha256": evidence["build_sha256"],
        "runtime_root_receipt_sha256": evidence["runtime_sha256"],
        "config_receipt_sha256": evidence["config_sha256"],
        "continuity_permit_sha256": auth.digest(issuance),
    }
    return evidence, bindings, expires


def current_runtime_bindings(
    connection: sqlite3.Connection, operation: str, at: str
) -> dict[str, Any]:
    """The ordinary A/B verifier; never cache this result across transactions."""
    try:
        return _target_authority(connection, operation=operation, at=at)[1]
    except auth.AuthorizationError:
        raise
    except (
        KeyError,
        TypeError,
        ValueError,
        OSError,
        RuntimeError,
        sqlite3.Error,
    ) as error:
        raise auth.AuthorizationError(
            f"Integrated activation evidence is invalid: {error}"
        ) from error


def publish_target_operation_gate(
    connection: sqlite3.Connection, *, operation: str, at: str
) -> dict[str, Any]:
    """Issue independent ordinary authority; does not capture or repeat samples."""
    _require(
        connection.in_transaction, "Target gate issuance requires a writer transaction"
    )
    require_current_process_writer_lock(connection)
    from . import capture_operator_release
    installed = release._installed_evidence(connection, at=at)
    if capture_operator_release.authority(connection, evidence=installed, operation=operation, at=at) is not None:
        _require(installed["active"]["profile_id"] == INTEGRATED_PROFILE, "Target publication requires the integrated profile")
        return capture_operator_release.publish(connection, evidence=installed, operation=operation, at=at)
    evidence, bindings, expires = _target_authority(
        connection, operation=operation, at=at
    )
    scope = auth.scope_hash(
        runtime_bindings=bindings, provider="tikhub", operation=operation
    )
    manifest_sha = auth.digest(evidence["manifest"])
    ready_evidence = {
        "contract": auth.READINESS_CONTRACT,
        "bindings": bindings,
        "scope_hash": scope,
        "qualification": "qualified",
        "continuity_permit_sha256": bindings["continuity_permit_sha256"],
        "transport_manifest_sha256": manifest_sha,
        "activation_successor": evidence["activation_successor"],
    }
    ready = {
        "provider": "tikhub",
        "operation": operation,
        "status": "ready",
        "reason": "integrated-activation-qualified",
        "evidence_json": auth.canonical(ready_evidence),
        "created_at": utc(at),
        "expires_at": expires,
    }
    checksum = auth.digest(ready)
    previous = connection.execute(
        "SELECT id FROM provider_readiness_receipts WHERE receipt_sha256=?", (checksum,)
    ).fetchone()
    if previous is None:
        identifier = connection.execute(
            f"INSERT INTO provider_readiness_receipts({','.join(ready)},receipt_sha256) VALUES ({','.join('?' for _ in range(len(ready) + 1))})",
            (*ready.values(), checksum),
        ).lastrowid
    else:
        identifier = previous[0]
    bucket = (
        "discovery" if operation in provider_budget.DISCOVERY_OPERATIONS else "metrics"
    )
    payload = {
        "contract": auth.CONTRACT,
        "bindings": bindings,
        "scope_hash": scope,
        "operation": operation,
        "issued_at": utc(at),
        "expires_at": expires,
        "readiness_receipt_id": identifier,
        "readiness_receipt_sha256": checksum,
        "transport_manifest_sha256": manifest_sha,
        "budget": {
            "total_microusd": provider_budget.AUTOMATIC_MICROUSD,
            "bucket": bucket,
            "bucket_microusd": provider_budget.BUDGET_BUCKET_MICROUSD[bucket],
        },
    }
    from .account_roster_capture import operation_budget
    inherited_budget = operation_budget(evidence["active"], operation)
    if inherited_budget is not None:
        payload["budget"] = inherited_budget
    gate = {
        "provider": "tikhub",
        "operation": operation,
        "state": "open",
        "reason": ready["reason"],
        "evidence_json": auth.canonical(payload),
        "recorded_at": utc(at),
    }
    gate_sha = auth.digest(gate)
    connection.execute(
        f"INSERT OR IGNORE INTO capture_paid_send_gate_events({','.join(gate)},event_sha256) VALUES ({','.join('?' for _ in range(len(gate) + 1))})",
        (*gate.values(), gate_sha),
    )
    return {
        "readiness_receipt_id": identifier,
        "gate_sha256": gate_sha,
        "qualification": "qualified",
        "ordinary_paid_authorized": True,
        "activation_id": bindings["activation_id"],
        "expires_at": expires,
        "coverage_complete": False,
        "provider_calls": 0,
    }
