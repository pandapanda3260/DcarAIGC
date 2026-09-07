"""Bounded v25 release actions on the existing durable Writer command queue.

No new HTTP surface, scheduler, database writer, or automatic paid retry. A
member action handles exactly one previously frozen natural request. Paths,
clock, prices, transport and runtime proofs are never client parameters.
"""
from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Callable, Mapping, NoReturn

from . import capture, capture_release as release, transport_execution
from .runtime_database import require_current_process_writer_lock
from .storage import connect, now_utc, transaction

_FIELDS = {
    "continuity_freeze": {"operation", "qualification_receipt_id"},
    "continuity_publish": {"operation"},
    "continuity_member": {"permit_id", "rank"},
    "continuity_resume": {"permit_id", "rank"},
    "operation_publish": {"operation"},
    "native_freeze": {"operation"},
    "native_member": {"cohort_id", "rank"},
    "native_resume": {"cohort_id", "rank"},
    "native_qualify": {"cohort_receipt_id"},
    "operation_renew": {"operation"},
    "deployment_accept": {"deployment_id", "candidate_id", "native_cohort_id"},
    "deployment_accept_deferred": {"deployment_id", "candidate_id", "operations", "actor", "reason"},
    "integrated_begin": {"drain_id", "roster_snapshot_id", "operations", "actor", "reason"},
    "integrated_complete": {"drain_id"},
    "integrated_publish": {"operation"},
}


def validate_parameters(parameters: Mapping[str, Any]) -> dict[str, Any]:
    from .profile_control import ProfileControlError

    def invalid() -> NoReturn:
        raise ProfileControlError("capture_release_command_invalid", "Invalid bounded capture-release action")

    value = dict(parameters)
    action = value.get("action")
    if not isinstance(action, str) or action not in _FIELDS or set(value) != {"action", *_FIELDS[action]}:
        invalid()
    for key, item in value.items():
        if key.endswith("_id") and key not in {"drain_id", "deployment_id", "candidate_id"}:
            if type(item) is not int or item < 1:
                invalid()
    if "operation" in value and (not isinstance(value["operation"], str)
                                 or value["operation"] not in release.CONTINUITY_OPERATIONS):
        invalid()
    if "rank" in value:
        cap = 200 if action.startswith("native_") else 20
        if type(value["rank"]) is not int or not 1 <= value["rank"] <= cap:
            invalid()
    if "operations" in value:
        ops = value["operations"]
        if (not isinstance(ops, list) or not ops or any(not isinstance(op, str) for op in ops)
                or len(ops) != len(set(ops)) or not set(ops).issubset(release.CONTINUITY_OPERATIONS)):
            invalid()
        value["operations"] = sorted(ops)
    for key in ("drain_id", "deployment_id", "candidate_id", "actor", "reason"):
        if key in value and (not isinstance(value[key], str) or not value[key].strip()
                             or len(value[key]) > (2000 if key == "reason" else 128)):
            invalid()
    return value


def _deployment_issuer() -> Any:
    # Same sealed checkout as the validator; no caller-selected script path.
    release._release_tools()
    name = "_dcar_capture_deployment_issuer"
    if name not in sys.modules:
        path = release.source_root(release.PROJECT_ROOT) / "scripts/issue_v20_deployment_receipt.py"
        spec = importlib.util.spec_from_file_location(name, path)
        if spec is None or spec.loader is None:
            raise ValueError("Installed deployment issuer is unavailable")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        try:
            spec.loader.exec_module(module)
        except Exception:
            sys.modules.pop(name, None)
            raise
    return sys.modules[name]


def _current_command(connection: sqlite3.Connection, command_claim: Mapping[str, Any],
                     parameters: Mapping[str, Any]) -> None:
    """An arbitrary direct caller cannot borrow another queued command's claim."""
    from . import profile_control

    require_current_process_writer_lock(connection)
    if connection.execute("PRAGMA user_version").fetchone()[0] != 20:
        raise profile_control.ProfileControlError("capture_release_schema_required", "Capture release actions require schema20")
    row = connection.execute("SELECT * FROM scheduler_runs WHERE id=?", (command_claim["run_id"],)).fetchone()
    attempt = connection.execute("SELECT * FROM scheduler_run_attempts WHERE id=? AND scheduler_run_id=?",
        (command_claim["attempt_id"], command_claim["run_id"])).fetchone()
    if row is None or attempt is None:
        raise profile_control.ProfileControlError("capture_release_claim_invalid", "Writer command claim is missing")
    decoded = profile_control._decode_command_row(row)
    details = decoded["details"]
    if (row["status"] != "running" or attempt["status"] != "running"
            or decoded["binding"] != command_claim["binding"]
            or decoded["binding"]["command"] != "capture_release"
            or decoded["binding"]["parameters"] != dict(parameters)
            or json.loads(attempt["details_json"]) != details
            or details != command_claim["details"]):
        raise profile_control.ProfileControlError("capture_release_claim_invalid", "Writer command claim changed")


def run_command(*, db_path: Path, mirror_root: Path | None, command_claim: Mapping[str, Any],
                action: str, **arguments: Any) -> dict[str, Any]:
    parameters = validate_parameters({"action": action, **arguments})
    with connect(db_path) as connection:
        _current_command(connection, command_claim, parameters)
    # Only the server-configured private mirror and live clock reach issuers.
    root = mirror_root if mirror_root is not None else db_path.resolve().parent / "current-hold-control"
    at = now_utc()
    if action in {"continuity_member", "continuity_resume", "native_member", "native_resume"}:
        functions: dict[str, Callable[..., dict[str, Any]]] = {
            "continuity_member": transport_execution.execute_continuity_member_page,
            "continuity_resume": transport_execution.resume_continuity_member_local,
            "native_member": transport_execution.execute_native_member_page,
            "native_resume": transport_execution.resume_native_member_local,
        }
        identifier = arguments["cohort_id"] if action.startswith("native_") else arguments["permit_id"]
        return functions[action](identifier, arguments["rank"], db_path=db_path, raw_root=capture.RAW_ROOT, at=at)
    if action == "integrated_begin":
        from .capture_activation_release import begin_integrated_switch

        return begin_integrated_switch(db_path=db_path, now=at, **arguments)
    if action == "integrated_complete":
        from .profile_control import complete_cross_profile_switch

        return complete_cross_profile_switch(db_path=db_path, drain_id=arguments["drain_id"],
            now=at, mirror_root=root, command_claim=command_claim)
    with connect(db_path) as connection, transaction(connection):
        _current_command(connection, command_claim, parameters)
        if action == "continuity_freeze":
            return release.freeze_continuity_permit(connection, at=at, **arguments)
        if action == "continuity_publish":
            return release.publish_continuity_gate(connection, at=at, **arguments)
        if action == "operation_publish":
            return release.publish_operation_gate(connection, at=at, **arguments)
        if action == "native_freeze":
            return release.freeze_operation_cohort(connection, at=at, mirror_root=root, **arguments)
        if action == "native_qualify":
            return release.record_native_operation_qualification(connection, at=at, mirror_root=root, **arguments)
        if action == "operation_renew":
            return release.renew_operation_gate(connection, at=at, mirror_root=root, **arguments)
        if action == "deployment_accept":
            # A digest-derived filename prevents a deployment ID from choosing
            # another filesystem destination. The issuer proves live evidence.
            receipt_path = root / ("deployment-e2e-" + release.auth.digest({
                "deployment_id": arguments["deployment_id"]}) + ".json")
            return _deployment_issuer().issue_accepted(connection,
                e2e_receipt_path=receipt_path, at=at, **arguments)
        if action == "deployment_accept_deferred":
            receipt_path = root / ("deployment-release-decision-" + release.auth.digest({
                "deployment_id": arguments["deployment_id"]}) + ".json")
            return _deployment_issuer().issue_deferred_acceptance(connection,
                decision_receipt_path=receipt_path, at=at, **arguments)
        if action == "integrated_publish":
            from .capture_activation_release import publish_target_operation_gate

            return publish_target_operation_gate(connection, at=at, **arguments)
    raise ValueError("Unsupported capture release action")
