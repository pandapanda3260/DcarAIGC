"""Read-only binding for transport diagnostics inside the current HOLD.

The binding is deliberately derived from the schema-19 drain authority and
durable prerequisite receipts.  It is not itself a permit: callers must compare
it again at the final send boundary in the same transaction that records the
network-start authority.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from typing import Any

from .paid_drain import CURRENT_ACTIVATION_HOLD_CONTRACT, dispatch_state
from .profile_control import (
    CURRENT_HOLD_CONTROL_JOB,
    CURRENT_HOLD_PREREQUISITE_CONTRACT,
    ProfileControlError,
    _canonical,
    _current_hold_prerequisites,
    _hold_generation,
    _read_hold_control_receipt,
    _require_current_hold_start,
    _require_matrix_fence,
    _require_schema19,
    _sha,
    _utc,
)

CONTRACT_VERSION = "current-hold-diagnostic-binding-v1"
_REQUIRED_PREREQUISITES = ("build", "runtime", "config", "price", "budget")


class TransportHoldBindingError(RuntimeError):
    """A current HOLD cannot authorize a transport diagnostic."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _frozen(value: Mapping[str, Any]) -> dict[str, Any]:
    try:
        loaded = json.loads(_canonical(dict(value)))
    except ProfileControlError as exc:
        raise TransportHoldBindingError(
            "diagnostic_hold_binding_invalid",
            "Diagnostic HOLD binding is not canonical JSON",
        ) from exc
    if not isinstance(loaded, dict):  # Defensive; ``dict(value)`` is an object.
        raise TransportHoldBindingError(
            "diagnostic_hold_binding_invalid",
            "Diagnostic HOLD binding must be an object",
        )
    return loaded


def _latest_prerequisite_payloads(
    connection: sqlite3.Connection,
    *,
    drain_id: str,
    generation: int,
) -> dict[str, dict[str, Any]]:
    """Read the same latest-per-kind receipts consumed by HOLD_SEAL."""

    latest: dict[str, dict[str, Any]] = {}
    rows = connection.execute(
        "SELECT * FROM scheduler_runs WHERE job_id=? AND status='succeeded' "
        "ORDER BY id DESC",
        (CURRENT_HOLD_CONTROL_JOB,),
    ).fetchall()
    for row in rows:
        try:
            receipt = _read_hold_control_receipt(connection, row)
        except ProfileControlError as exc:
            raise TransportHoldBindingError(
                "diagnostic_hold_prerequisite_invalid",
                "A current-HOLD control receipt is invalid",
            ) from exc
        payload = receipt.get("payload")
        if (
            not isinstance(payload, dict)
            or payload.get("action") != "hold_prerequisite"
            or payload.get("drain_id") != drain_id
            or payload.get("generation") != generation
        ):
            continue
        kind = payload.get("kind")
        if kind in _REQUIRED_PREREQUISITES and kind not in latest:
            latest[str(kind)] = {
                "receipt": receipt,
                "payload": payload,
            }
    missing = sorted(set(_REQUIRED_PREREQUISITES) - set(latest))
    if missing:
        raise TransportHoldBindingError(
            "diagnostic_hold_prerequisite_missing",
            "Diagnostic HOLD prerequisites are missing: " + ",".join(missing),
        )
    return latest


def read_current_diagnostic_hold(
    connection: sqlite3.Connection,
    *,
    drain_id: str,
    at: str,
    expected: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return the current canonical HOLD binding or fail closed.

    ``expected`` is an exact compare-and-swap value.  Permit issuance and the
    final network-send gate should call this function under their existing
    ``BEGIN IMMEDIATE`` transaction, passing the binding frozen by the campaign
    or permit on the latter call.
    """

    try:
        _require_schema19(connection)
        timestamp = _utc(at)
        start, start_control, active = _require_current_hold_start(
            connection, drain_id=drain_id, timestamp=timestamp
        )
    except ProfileControlError as exc:
        raise TransportHoldBindingError(
            "diagnostic_hold_invalid", "Current diagnostic HOLD is invalid"
        ) from exc

    state = dispatch_state(connection, at=timestamp)
    if (
        state.state != "draining"
        or state.drain_id != drain_id
        or state.last_event_id != int(start["event_id"])
        or state.last_event_hash != str(start["event_hash"])
        or state.activation_id != int(active["activation_id"])
    ):
        raise TransportHoldBindingError(
            "diagnostic_hold_not_current",
            "Diagnostic HOLD START is not the current unsealed drain authority",
        )

    # Schema 19 seals permit_event_id to a real RELEASE foreign key. Keep that
    # column as historical lineage, not present send authority: the diagnostic
    # member separately binds this START and is revalidated at both boundaries.
    # Only this START's immediate, same-activation predecessor is admissible.
    release = connection.execute(
        "SELECT id,event_hash,event_type,target_activation_id,created_at "
        "FROM pipeline_paid_drain_events WHERE id=?",
        (start["previous_event_id"],),
    ).fetchone()
    if (
        release is None or release["event_type"] != "release"
        or release["event_hash"] != start["previous_event_hash"]
        or release["target_activation_id"] != active["activation_id"]
        or _utc(release["created_at"]) > _utc(start["created_at"])
    ):
        raise TransportHoldBindingError(
            "diagnostic_release_lineage_invalid",
            "Current HOLD has no exact same-activation historical RELEASE anchor",
        )

    profile_id = str(active["profile_id"])
    owner = str(start_control.get("actor") or "").strip()
    if start_control.get("profile_id") != profile_id or not owner:
        raise TransportHoldBindingError(
            "diagnostic_hold_start_invalid",
            "Diagnostic HOLD START profile or owner is invalid",
        )

    frozen_matrix = start_control.get("matrix_high_watermarks")
    if not isinstance(frozen_matrix, Mapping):
        raise TransportHoldBindingError(
            "diagnostic_hold_matrix_invalid",
            "Diagnostic HOLD START has no Matrix high-watermark fence",
        )
    try:
        matrix_high_watermarks = _require_matrix_fence(connection, frozen_matrix)
    except ProfileControlError as exc:
        raise TransportHoldBindingError(
            "diagnostic_hold_matrix_delta",
            "Matrix evidence changed after the diagnostic HOLD START",
        ) from exc

    try:
        generation = _hold_generation(
            connection, drain_id=drain_id, start_control=start_control
        )
    except (KeyError, ProfileControlError) as exc:
        raise TransportHoldBindingError(
            "diagnostic_hold_generation_invalid",
            "Diagnostic HOLD build generation is invalid",
        ) from exc
    generation_number = generation.get("generation")
    if type(generation_number) is not int or generation_number < 1:
        raise TransportHoldBindingError(
            "diagnostic_hold_generation_invalid",
            "Diagnostic HOLD build generation is invalid",
        )

    latest = _latest_prerequisite_payloads(
        connection, drain_id=drain_id, generation=generation_number
    )
    config_artifact = generation.get("config_receipt_sha256")
    if config_artifact is None:
        # Generation 1 predates a config binding in HOLD_BEGIN.  Its current
        # config must therefore come from the latest durable same-generation
        # prerequisite, never from a caller-provided hash.
        config_artifact = latest["config"]["payload"].get("artifact_sha256")
    expected_artifacts = {
        "build": generation.get("build_receipt_sha256"),
        "runtime": generation.get("runtime_root_receipt_sha256"),
        "config": config_artifact,
        "price": latest["price"]["payload"].get("artifact_sha256"),
        "budget": latest["budget"]["payload"].get("artifact_sha256"),
    }
    try:
        artifacts = {
            kind: _sha(str(value or ""), label=f"diagnostic {kind} receipt")
            for kind, value in expected_artifacts.items()
        }
        prerequisites = _current_hold_prerequisites(
            connection,
            drain_id=drain_id,
            generation=generation_number,
            active=active,
            expected=artifacts,
            at=timestamp,
        )
    except ProfileControlError as exc:
        raise TransportHoldBindingError(
            "diagnostic_hold_prerequisite_invalid",
            "Diagnostic HOLD prerequisite is missing, stale, or invalid",
        ) from exc

    for kind in _REQUIRED_PREREQUISITES:
        payload = latest[kind]["payload"]
        receipt = latest[kind]["receipt"]
        if (
            payload.get("contract_version") != CURRENT_HOLD_PREREQUISITE_CONTRACT
            or payload.get("control_contract_version")
            != CURRENT_ACTIVATION_HOLD_CONTRACT
            or payload.get("start_event_id") != int(start["event_id"])
            or payload.get("start_event_hash") != str(start["event_hash"])
            or payload.get("activation_id") != int(active["activation_id"])
            or payload.get("roster_snapshot_id") != int(active["roster_snapshot_id"])
            or payload.get("roster_snapshot_hash")
            != str(active["roster_members_sha256"])
            or payload.get("artifact_sha256") != artifacts[kind]
            or prerequisites[kind]["receipt_sha256"]
            != str(receipt.get("self_sha256") or "")
        ):
            raise TransportHoldBindingError(
                "diagnostic_hold_prerequisite_drift",
                f"Diagnostic HOLD {kind} prerequisite changed its START binding",
            )

    binding = _frozen(
        {
            "contract_version": CONTRACT_VERSION,
            "control_contract_version": CURRENT_ACTIVATION_HOLD_CONTRACT,
            "drain_id": drain_id,
            "start_event_id": int(start["event_id"]),
            "start_event_hash": str(start["event_hash"]),
            "dispatch_legacy_release_anchor": {
                "event_id": int(release["id"]),
                "event_hash": str(release["event_hash"]),
                "role": "historical_lineage_only",
            },
            "activation_id": int(active["activation_id"]),
            "profile_id": profile_id,
            "roster_snapshot_id": int(active["roster_snapshot_id"]),
            "roster_snapshot_hash": str(active["roster_members_sha256"]),
            "generation": generation_number,
            "build_receipt_sha256": artifacts["build"],
            "runtime_root_receipt_sha256": artifacts["runtime"],
            "config_receipt_sha256": artifacts["config"],
            "price_receipt_sha256": artifacts["price"],
            "budget_receipt_sha256": artifacts["budget"],
            "matrix_high_watermarks": matrix_high_watermarks,
            "prerequisites": prerequisites,
            "actor": owner,
        }
    )
    if expected is not None and _frozen(expected) != binding:
        raise TransportHoldBindingError(
            "diagnostic_hold_binding_changed",
            "Diagnostic HOLD binding changed after it was frozen",
        )
    return binding


def diagnostic_dispatch_binding(member: Mapping[str, Any]) -> dict[str, Any]:
    """Explicitly distinguish current START authority from the legacy FK."""
    payload = member["payload"]
    hold = payload["hold_binding"]
    return {
        "receipt_id": member["receipt_id"], "receipt_sha256": member["self_sha256"],
        "campaign_receipt_id": payload["campaign_receipt_id"], "rank": payload["rank"],
        "arm": payload["arm"],
        "authorization_kind": "current_hold_diagnostic_permit_v1",
        "start_event_id": hold["start_event_id"],
        "start_event_hash": hold["start_event_hash"],
        "legacy_release_anchor": dict(hold["dispatch_legacy_release_anchor"]),
    }
