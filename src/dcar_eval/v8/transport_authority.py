"""Request-local references to durable diagnostic authority, never authority alone."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import asdict
from typing import Any, Iterator, Mapping

from apscheduler.schedulers.base import STATE_PAUSED, BaseScheduler  # type: ignore[import-untyped]

from .durable_runs import DurableClaim
from .paid_identity import PaidRequestIdentity, build_paid_request_identity
from .provider_budget import PaidScope
from .transport_hold_binding import diagnostic_dispatch_binding as diagnostic_dispatch_binding
from .transport_members import DiagnosticMemberError, validate_primary_member_for_send
from .transport_receipts import read_transport_receipt

_REQUEST: ContextVar[tuple[dict[str, Any], BaseScheduler] | None] = ContextVar(
    "diagnostic_request_references", default=None,
)


@contextmanager
def diagnostic_request_context(
    member_receipt_id: int, operator_claim: DurableClaim, *, scheduler: BaseScheduler,
) -> Iterator[None]:
    """Scope one writer-runner request while its actual scheduler stays paused.

    The final DB gate independently reads the member, mirror, HOLD and owner.
    This context cannot confer authority by itself and cannot nest/replace a
    member mid-request. It is not installed by ordinary scheduler or API work.
    """
    if (
        type(member_receipt_id) is not int or member_receipt_id < 1
        or not isinstance(operator_claim, DurableClaim)
        or not isinstance(scheduler, BaseScheduler) or scheduler.state != STATE_PAUSED
        or _REQUEST.get() is not None
    ):
        raise DiagnosticMemberError("diagnostic_context_invalid", "Diagnostic request requires one member and a paused scheduler")
    binding = {
        "contract_version": "diagnostic-request-binding-v1",
        "member_receipt_id": member_receipt_id,
        "operator_claim": asdict(operator_claim),
    }
    token = _REQUEST.set((binding, scheduler))
    try:
        yield
    finally:
        _REQUEST.reset(token)


def current_diagnostic_request_binding() -> dict[str, Any] | None:
    request = _REQUEST.get()
    return None if request is None else json.loads(json.dumps(request[0], sort_keys=True))


def authorize_diagnostic_request(
    connection: sqlite3.Connection, *, binding: Mapping[str, Any], scope: PaidScope,
    request_identity: PaidRequestIdentity, request_transport: Mapping[str, Any] | None,
    stage: str, at: str,
) -> dict[str, Any]:
    """Recheck actual runtime and durable authority before reserve and send."""
    request = _REQUEST.get()
    if (
        request is None or request[0] != dict(binding) or request[1].state != STATE_PAUSED
        or stage != "discovery" or request_transport is None
        or not connection.in_transaction
    ):
        raise DiagnosticMemberError("diagnostic_context_changed", "Diagnostic context/scheduler/stage is no longer valid")
    try:
        operator = DurableClaim(**request[0]["operator_claim"])
        return validate_primary_member_for_send(
            connection, member_receipt_id=request[0]["member_receipt_id"],
            operator_claim=operator, scope=scope, request_identity=request_identity,
            request_transport=request_transport, at=at,
        )
    except DiagnosticMemberError:
        raise
    except (RuntimeError, ValueError, TypeError, KeyError, OSError) as exc:
        # The capture cleanup path needs one stable blocked classification;
        # retain the precise cause instead of silently treating it as success.
        raise DiagnosticMemberError(
            str(getattr(exc, "code", "diagnostic_authority_invalid")),
            f"Diagnostic authority rejected: {exc}",
        ) from exc


def authorize_transport_fault_diagnostic(
    connection: sqlite3.Connection, *, scope: PaidScope, operation: str, at: str,
) -> bool:
    """A live member can diagnose transport, never clear a fault or bypass caps.

    Budget callers do not supply a boolean exception. Reconstruct the frozen
    request from durable evidence, then run the complete send-authority gate.
    Ordinary budget/readiness callers have no request context and stay blocked.
    """
    binding = current_diagnostic_request_binding()
    if binding is None:
        return False
    member = read_transport_receipt(connection, binding["member_receipt_id"])
    payload = member["payload"]
    document = payload["natural_due"]["request_document"]
    if operation != payload["operation"] or document["operation"] != operation:
        raise DiagnosticMemberError("diagnostic_operation_changed", "Budget operation differs from the member")
    identity = build_paid_request_identity(
        provider=document["provider"], operation=document["operation"],
        platform=document["platform"], subject=document["subject"],
        request_parameters=document["request_parameters"], cursor=document["cursor"],
        due_bucket=document["due_bucket"], request_window=document["request_window"],
        sequence=payload["sequence"],
    )
    authorize_diagnostic_request(
        connection, binding=binding, scope=scope, request_identity=identity,
        request_transport=payload["request_transport"], stage="discovery", at=at,
    )
    return True
