"""Writer-owned coordination of one fixed primary diagnostic batch.

The existing writer command queue supplies explicit invocation, never an
automatic schedule, qualification or HOLD release. All members are frozen
before the first provider request.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from dataclasses import asdict
from datetime import timedelta
from pathlib import Path
from typing import Any

from apscheduler.schedulers.base import STATE_PAUSED, BaseScheduler  # type: ignore[import-untyped]

from . import durable_runs
from .provider_budget import micro_usd
from .runtime_database import require_current_process_writer_lock
from .source_routing import parse_time
from .storage import connect, now_utc, transaction
from .transport_execution import execute_primary_member_page
from .transport_members import DiagnosticMemberError, _current_campaign, _operator_binding, issue_primary_member_batch
from .transport_receipts import append_transport_receipt, read_transport_receipt

CONTRACT_VERSION = "transport-primary-execution-v1"
CONTROL_ROUTE_SPECS = {
    "control_io": ("https://api.tikhub.io", "urllib-stream-v1"),
    "control_legacy": ("https://api.tikhub.dev", "urllib-legacy-v1"),
}


def _terminal_key(campaign: dict[str, Any]) -> str:
    arm = str(campaign["payload"].get("arm") or "primary")
    prefix = "primary" if arm == "primary" else arm
    return f"{prefix}-execution:{campaign['receipt_id']}"


def _campaign_terminal_receipt(
    connection: sqlite3.Connection,
    campaign: dict[str, Any],
) -> dict[str, Any] | None:
    row = connection.execute(
        "SELECT id FROM scheduler_runs WHERE job_id='transport_receipt:campaign_terminal' AND scheduled_for=?",
        (_terminal_key(campaign),),
    ).fetchone()
    return read_transport_receipt(connection, int(row["id"])) if row is not None else None


def _control_transport_binding(*, mirror_root: Path, arm: str) -> dict[str, Any]:
    from tikhub_config import resolve_tikhub_transport_manifest  # type: ignore[import-untyped]

    if arm not in CONTROL_ROUTE_SPECS:
        raise DiagnosticMemberError("diagnostic_control_arm_invalid", "Control arm is unsupported")
    api_base, http_stack = CONTROL_ROUTE_SPECS[arm]
    route_dir = mirror_root / "transport-control-routes"
    route_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    path = route_dir / f"{arm}.env"
    body = f"TIKHUB_API_BASE={api_base}\nTIKHUB_HTTP_STACK={http_stack}\n".encode("utf-8")
    if path.exists():
        if path.read_bytes() != body:
            raise DiagnosticMemberError("diagnostic_control_route_changed", "Control route config changed")
    else:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(body)
                handle.flush()
                os.fsync(handle.fileno())
        except BaseException:
            path.unlink(missing_ok=True)
            raise
    return {
        "manifest": resolve_tikhub_transport_manifest(path, honor_environment=False),
        "config_path": str(path), "honor_environment": False,
    }


def run_primary_command(
    *, command_claim: dict[str, Any], drain_id: str, scheduler: BaseScheduler | None,
    db_path: Path, mirror_root: Path | None,
) -> dict[str, Any]:
    """Adapt the existing writer queue to the original, durably saved operator.

    A new command cannot adopt an existing campaign owner. Recovered commands
    retain their original claim, and the runner's ownership gates decide whether
    execution is still safe. No route, sample, cost or raw-path override exists.
    """
    from .capture import RAW_ROOT
    from .providers import _freeze_tikhub_transport
    from .transport_accounting import settle_closed_hold_unknowns
    from .transport_campaign import freeze_primary_transport_campaign
    from .transport_cohort import freeze_large_page_cohort
    from .transport_members import OPERATOR_JOB, primary_operator_identity
    from .transport_verdict import record_primary_route_verdict

    if not isinstance(scheduler, BaseScheduler) or scheduler.state != STATE_PAUSED:
        raise DiagnosticMemberError("diagnostic_scheduler_active", "Explicit diagnostic command requires paused startup")
    if mirror_root is None:
        raise DiagnosticMemberError("diagnostic_mirror_required", "Diagnostic command requires the writer mirror root")
    with connect(db_path) as connection, transaction(connection):
        require_current_process_writer_lock(connection)
        row = connection.execute(
            "SELECT r.details_json FROM scheduler_runs r JOIN scheduler_run_attempts a "
            "ON a.scheduler_run_id=r.id WHERE r.id=? AND a.id=? "
            "AND r.job_id='current_activation_hold_command' AND r.status='running' AND a.status='running'",
            (command_claim["run_id"], command_claim["attempt_id"]),
        ).fetchone()
        if row is None or json.loads(row["details_json"]) != command_claim["details"]:
            raise DiagnosticMemberError("diagnostic_command_owner_changed", "Writer command lost its durable claim")
        details = command_claim["details"]
        saved = details.get("primary_operator")
        if saved is None:
            cohort = freeze_large_page_cohort(connection, drain_id=drain_id, at=now_utc(), mirror_root=mirror_root)
            transport = _freeze_tikhub_transport()
            assert transport is not None
            campaign = freeze_primary_transport_campaign(
                connection, drain_id=drain_id, cohort_receipt_id=cohort["receipt_id"],
                request_transport=transport, at=now_utc(), mirror_root=mirror_root,
            )
            identity = primary_operator_identity(campaign)
            key = "scan:" + durable_runs.scan_identity(OPERATOR_JOB, identity)
            if connection.execute("SELECT 1 FROM scheduler_runs WHERE job_id=? AND scheduled_for=?", (OPERATOR_JOB, key)).fetchone():
                raise DiagnosticMemberError("diagnostic_operator_exists", "Campaign already belongs to its original command")
            operator = durable_runs.claim_run_in_transaction(
                connection, OPERATOR_JOB, identity, invocation_source="operator_retry", now=now_utc(),
            )
            assert operator is not None
            saved = {"campaign_receipt_id": campaign["receipt_id"], "claim": asdict(operator)}
            details = {**details, "primary_operator": saved}
            connection.execute("UPDATE scheduler_runs SET details_json=? WHERE id=?", (
                json.dumps(details, sort_keys=True, separators=(",", ":")), command_claim["run_id"],
            ))
        else:
            campaign = read_transport_receipt(connection, saved["campaign_receipt_id"])
            if campaign["payload"]["hold_binding"]["drain_id"] != drain_id:
                raise DiagnosticMemberError("diagnostic_command_binding_changed", "Saved operator belongs to a different HOLD")
            operator = durable_runs.DurableClaim(**saved["claim"])
    # Keep the queue's final receipt bound to exactly the committed owner.
    command_claim["details"] = details
    result = run_primary_campaign(
        campaign["receipt_id"], operator_claim=operator, scheduler=scheduler,
        db_path=db_path, raw_root=RAW_ROOT, mirror_root=mirror_root,
    )
    with connect(db_path) as connection, transaction(connection):
        settle_closed_hold_unknowns(connection, drain_id=drain_id, scheduler=scheduler, at=now_utc(), mirror_root=mirror_root)
    # A later verdict failure must not roll back valid conservative accounting.
    with connect(db_path) as connection, transaction(connection):
        verdict = record_primary_route_verdict(connection, campaign["receipt_id"], at=now_utc(), mirror_root=mirror_root)
    return {"campaign_receipt_id": campaign["receipt_id"], "terminal_receipt_id": result["receipt"]["receipt_id"],
            "verdict_receipt_id": verdict["receipt_id"], **verdict["payload"]}


def run_control_command(
    *, command_claim: dict[str, Any], drain_id: str, source_verdict_receipt_id: int,
    arm: str, scheduler: BaseScheduler | None, db_path: Path, mirror_root: Path | None,
) -> dict[str, Any]:
    from .capture import RAW_ROOT
    from .transport_accounting import settle_closed_hold_unknowns
    from .transport_campaign import freeze_control_transport_campaign
    from .transport_members import OPERATOR_JOB, primary_operator_identity
    from .transport_verdict import record_primary_route_verdict

    if not isinstance(source_verdict_receipt_id, int) or source_verdict_receipt_id < 1:
        raise DiagnosticMemberError("diagnostic_source_verdict_invalid", "Control diagnostics require a source verdict")
    if not isinstance(scheduler, BaseScheduler) or scheduler.state != STATE_PAUSED:
        raise DiagnosticMemberError("diagnostic_scheduler_active", "Explicit diagnostic command requires paused startup")
    if mirror_root is None:
        raise DiagnosticMemberError("diagnostic_mirror_required", "Diagnostic command requires the writer mirror root")
    transport = _control_transport_binding(mirror_root=mirror_root, arm=arm)
    with connect(db_path) as connection, transaction(connection):
        require_current_process_writer_lock(connection)
        row = connection.execute(
            "SELECT r.details_json FROM scheduler_runs r JOIN scheduler_run_attempts a "
            "ON a.scheduler_run_id=r.id WHERE r.id=? AND a.id=? "
            "AND r.job_id='current_activation_hold_command' AND r.status='running' AND a.status='running'",
            (command_claim["run_id"], command_claim["attempt_id"]),
        ).fetchone()
        if row is None or json.loads(row["details_json"]) != command_claim["details"]:
            raise DiagnosticMemberError("diagnostic_command_owner_changed", "Writer command lost its durable claim")
        details = command_claim["details"]
        saved = details.get("control_operator")
        if saved is None:
            campaign = freeze_control_transport_campaign(
                connection, drain_id=drain_id, source_verdict_receipt_id=source_verdict_receipt_id,
                arm=arm, request_transport=transport, at=now_utc(), mirror_root=mirror_root,
            )
            identity = primary_operator_identity(campaign)
            key = "scan:" + durable_runs.scan_identity(OPERATOR_JOB, identity)
            if connection.execute("SELECT 1 FROM scheduler_runs WHERE job_id=? AND scheduled_for=?", (OPERATOR_JOB, key)).fetchone():
                terminal = _campaign_terminal_receipt(connection, campaign)
                if terminal is None:
                    raise DiagnosticMemberError("diagnostic_operator_exists", "Campaign already belongs to its original command")
                try:
                    operator = durable_runs.DurableClaim(**terminal["payload"]["operator_claim"])
                except (KeyError, TypeError) as error:
                    raise DiagnosticMemberError(
                        "diagnostic_operator_changed",
                        "Completed control campaign lost its original operator claim",
                    ) from error
            else:
                claimed = durable_runs.claim_run_in_transaction(
                    connection, OPERATOR_JOB, identity, invocation_source="operator_retry", now=now_utc(),
                )
                if claimed is None:
                    raise DiagnosticMemberError(
                        "diagnostic_operator_claim_failed",
                        "Control campaign operator claim was not created",
                    )
                operator = claimed
            saved = {
                "arm": arm,
                "campaign_receipt_id": campaign["receipt_id"],
                "claim": asdict(operator),
            }
            details = {**details, "control_operator": saved}
            connection.execute("UPDATE scheduler_runs SET details_json=? WHERE id=?", (
                json.dumps(details, sort_keys=True, separators=(",", ":")), command_claim["run_id"],
            ))
        else:
            if saved.get("arm") != arm:
                raise DiagnosticMemberError("diagnostic_control_arm_changed", "Saved control operator changed arms")
            campaign = read_transport_receipt(connection, saved["campaign_receipt_id"])
            if (
                campaign["payload"]["hold_binding"]["drain_id"] != drain_id
                or campaign["payload"].get("arm") != arm
            ):
                raise DiagnosticMemberError("diagnostic_command_binding_changed", "Saved operator belongs to a different HOLD")
            operator = durable_runs.DurableClaim(**saved["claim"])
    command_claim["details"] = details
    result = run_primary_campaign(
        campaign["receipt_id"], operator_claim=operator, scheduler=scheduler,
        db_path=db_path, raw_root=RAW_ROOT, mirror_root=mirror_root,
    )
    with connect(db_path) as connection, transaction(connection):
        settle_closed_hold_unknowns(connection, drain_id=drain_id, scheduler=scheduler, at=now_utc(), mirror_root=mirror_root)
    with connect(db_path) as connection, transaction(connection):
        verdict = record_primary_route_verdict(connection, campaign["receipt_id"], at=now_utc(), mirror_root=mirror_root)
    return {"campaign_receipt_id": campaign["receipt_id"], "terminal_receipt_id": result["receipt"]["receipt_id"],
            "verdict_receipt_id": verdict["receipt_id"], **verdict["payload"]}


def _compact_result(result: dict[str, Any]) -> dict[str, Any]:
    return {
        key: result[key] for key in (
            "member_receipt_id", "rank", "dispatch_id", "dispatch_terminal",
            "effective_starts", "raw_response_id", "materialized", "response_complete",
        )
    } | {
        "source_run_id": result["scan"]["scheduler_run_id"],
        "scan_complete": result["scan"]["complete"], "scan_reason": result["scan"].get("reason"),
        "result_sha256": hashlib.sha256(json.dumps(
            result, sort_keys=True, separators=(",", ":"), allow_nan=False,
        ).encode()).hexdigest(),
    }


def _close_inventory(connection: sqlite3.Connection, inventory: dict[str, Any], *, at: str) -> None:
    """Close only attempts owned by this preparation; never claim or replace."""
    for entry in [*inventory.get("child_claims", []), *inventory.get("parent_claims", [])]:
        claim = durable_runs.DurableClaim(**entry)
        row = connection.execute(
            "SELECT status FROM scheduler_run_attempts WHERE id=? AND scheduler_run_id=?",
            (claim.attempt_id, claim.scheduler_run_id),
        ).fetchone()
        if row is None:
            raise durable_runs.LostOwnership("Prepared attempt disappeared")
        if row["status"] != "running":
            continue  # Member execution has already finalized its own attempt.
        details = durable_runs.assert_owner(connection, claim)
        if details["checkpoint"].get("complete") is True:
            raise DiagnosticMemberError("diagnostic_unfinalized_child", "Completed prepared child was not finalized")
        durable_runs.finish_run_in_transaction(
            connection, claim, status="partial", now=at,
            next_resume_at=(parse_time(at) + timedelta(minutes=5)).isoformat(),
            summary={"reason": "diagnostic_inventory_yield", "diagnostic_only": True},
        )


def _failure_summary(error: Exception) -> dict[str, Any]:
    return {
        "type": type(error).__name__,
        "code": getattr(error, "code", None),
        "message": str(error),
    }


def run_primary_campaign(
    campaign_receipt_id: int, *, operator_claim: durable_runs.DurableClaim,
    scheduler: BaseScheduler, db_path: Path, raw_root: Path, mirror_root: Path,
) -> dict[str, Any]:
    from .transport_preparation import prepare_primary_due_inventory

    if not isinstance(scheduler, BaseScheduler) or scheduler.state != STATE_PAUSED:
        raise DiagnosticMemberError("diagnostic_scheduler_active", "Campaign requires the paused writer scheduler")
    with connect(db_path) as connection, transaction(connection):
        require_current_process_writer_lock(connection)
        campaign = _current_campaign(connection, campaign_receipt_id, now_utc())
        terminal_key = _terminal_key(campaign)
        prior = connection.execute(
            "SELECT id FROM scheduler_runs WHERE job_id='transport_receipt:campaign_terminal' AND scheduled_for=?",
            (terminal_key,),
        ).fetchone()
        if prior is not None:
            receipt = read_transport_receipt(connection, int(prior["id"]))
            if receipt["payload"].get("operator_claim") != asdict(operator_claim):
                raise DiagnosticMemberError("diagnostic_operator_changed", "Completed campaign belongs to its original operator")
            return {"already_completed": True, "receipt": receipt}
        _operator_binding(connection, operator_claim, campaign)

    members: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    failure: Exception | None = None
    try:
        # Preparation and the full batch are one transaction. Insufficient
        # eligible samples roll back newly created parents/children/permits.
        with connect(db_path) as connection, transaction(connection):
            prepare_primary_due_inventory(
                connection, campaign_receipt_id=campaign_receipt_id,
                operator_claim=operator_claim, scheduler=scheduler, at=now_utc(),
            )
            members = issue_primary_member_batch(
                connection, campaign_receipt_id=campaign_receipt_id,
                operator_claim=operator_claim, at=now_utc(), mirror_root=mirror_root,
            )
        for member in members:
            result = execute_primary_member_page(
                member["receipt_id"], operator_claim=operator_claim,
                scheduler=scheduler, db_path=db_path, raw_root=raw_root,
            )
            compact = _compact_result(result)
            results.append(compact)
            with connect(db_path) as connection, transaction(connection):
                durable_runs.checkpoint(connection, operator_claim, {"primary_results": results}, now=now_utc())
            if compact["effective_starts"] != 1 or compact["dispatch_terminal"] not in {"succeeded", "failed", "billing_unknown"}:
                break  # Hard readiness blocks keep all remaining ranks unused.
    except Exception as error:
        failure = error

    with connect(db_path) as connection, transaction(connection):
        require_current_process_writer_lock(connection)
        details = durable_runs.assert_owner(connection, operator_claim)
        # Read committed inventory, not Python state from a rolled-back prepare.
        committed_inventory = details["checkpoint"].get("primary_due_inventory")
        if committed_inventory is not None:
            _close_inventory(connection, committed_inventory, at=now_utc())
        if failure is not None:
            usage = connection.execute(
                "SELECT amount,currency,request_attempts,details_json FROM provider_usage "
                "WHERE json_extract(details_json,'$.diagnostic_member.campaign_receipt_id')=? ORDER BY id",
                (campaign_receipt_id,),
            ).fetchall()
            starts = sum(row["request_attempts"] for row in usage)
            terminal_receipt_id: int | None = None
            closed_at = now_utc()
            if (
                str(campaign["payload"].get("arm") or "primary") != "primary"
                and not members
                and not usage
                and starts == 0
            ):
                receipt = append_transport_receipt(
                    connection, kind="campaign_terminal", identity_key=_terminal_key(campaign),
                    payload={
                        "contract_version": CONTRACT_VERSION, "campaign_receipt_id": campaign_receipt_id,
                        "campaign_receipt_sha256": campaign["self_sha256"],
                        "hold_binding": campaign["payload"]["hold_binding"],
                        "operator_claim": asdict(operator_claim),
                        "members": [], "results": [], "sample_limit": 20,
                        "sample_complete": False, "effective_starts": 0,
                        "charged_microusd": 0, "unresolved_billing_count": 0,
                        "qualified": False, "failure": _failure_summary(failure),
                        "closed_at": closed_at,
                    }, at=closed_at, mirror_root=mirror_root,
                )
                terminal_receipt_id = int(receipt["receipt_id"])
                durable_runs.checkpoint(connection, operator_claim, {
                    "blocked_terminal_receipt_id": terminal_receipt_id,
                    "blocked_terminal_reason": "diagnostic_operator_blocked",
                }, now=closed_at)
            durable_runs.finish_run_in_transaction(
                connection, operator_claim, status="partial", now=closed_at,
                next_resume_at=(parse_time(closed_at) + timedelta(minutes=5)).isoformat(),
                summary={"reason": "diagnostic_operator_blocked", "error_type": type(failure).__name__,
                         "error_code": getattr(failure, "code", None), "sample_complete": False,
                         "terminal_receipt_id": terminal_receipt_id},
            )
        else:
            usage = connection.execute(
                "SELECT amount,currency,request_attempts,details_json FROM provider_usage "
                "WHERE json_extract(details_json,'$.diagnostic_member.campaign_receipt_id')=? ORDER BY id",
                (campaign_receipt_id,),
            ).fetchall()
            if any(row["currency"] != "USD" for row in usage):
                raise DiagnosticMemberError("diagnostic_currency_changed", "Campaign has a non-USD usage")
            total = sum(micro_usd(row["amount"]) for row in usage)
            starts = sum(row["request_attempts"] for row in usage)
            if (total > campaign["payload"]["max_cost_microusd"] or starts > 20
                    or len(usage) > 20 or starts != sum(row["effective_starts"] for row in results)):
                raise DiagnosticMemberError("diagnostic_cost_exceeded", "Campaign exceeded its immutable cap")
            complete = len(results) == 20 and all(row["effective_starts"] == 1 for row in results)
            receipt = append_transport_receipt(
                connection, kind="campaign_terminal", identity_key=terminal_key,
                payload={
                    "contract_version": CONTRACT_VERSION, "campaign_receipt_id": campaign_receipt_id,
                    "campaign_receipt_sha256": campaign["self_sha256"],
                    "hold_binding": campaign["payload"]["hold_binding"],
                    "operator_claim": asdict(operator_claim),
                    "members": [{"receipt_id": row["receipt_id"], "receipt_sha256": row["self_sha256"],
                                 "rank": row["payload"]["rank"]} for row in members],
                    "results": results, "sample_limit": 20, "sample_complete": complete,
                    "effective_starts": starts, "charged_microusd": total,
                    "unresolved_billing_count": sum(json.loads(row["details_json"]).get("state") == "billing_unknown" for row in usage),
                    "qualified": False, "closed_at": now_utc(),
                }, at=now_utc(), mirror_root=mirror_root,
            )
            durable_runs.checkpoint(connection, operator_claim, {
                "complete": True, "primary_terminal_receipt_id": receipt["receipt_id"],
            }, now=now_utc())
            durable_runs.finish_run_in_transaction(
                connection, operator_claim, status="succeeded", now=now_utc(),
                summary={"reason": "diagnostic_command_finished", "sample_complete": complete,
                         "qualified": False, "terminal_receipt_id": receipt["receipt_id"]},
            )
    if failure is not None:
        raise failure
    return {"already_completed": False, "receipt": receipt}
