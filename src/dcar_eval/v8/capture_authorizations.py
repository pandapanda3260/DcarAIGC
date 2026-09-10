"""Writer-owned schema20 A/B admission checks; this module never opens a gate.

The release/continuity controller supplies freshly verified runtime_bindings,
not a client payload or a state='open' assertion. It must verify the actual
installed build and schema20 continuity permit before invoking these helpers.
Both transactions re-read immutable DB authorization/readiness and current
activation; missing qualification or explicit deployment release is a refusal. Route, storage, provider
fault and transport-manifest checks at the existing paid boundary remain required.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Mapping, Callable, Iterator

from . import provider_budget, usage_settlements
from .profile_activations import activation_at
from .runtime_database import require_current_process_writer_lock

CONTRACT = "capture-paid-authorization-v1"
READINESS_CONTRACT = "capture-paid-readiness-v1"
BINDING_KEYS = frozenset({
    "activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256",
    "build_receipt_sha256", "runtime_root_receipt_sha256", "config_receipt_sha256",
    "continuity_permit_sha256",
})
_RUNTIME_VERIFIER: ContextVar[Callable[[sqlite3.Connection, str, str], Mapping[str, Any]] | None] = ContextVar(
    "capture_schema20_runtime_verifier", default=None)


@contextmanager
def runtime_authority(verifier: Callable[[sqlite3.Connection, str, str], Mapping[str, Any]]) -> Iterator[None]:
    """Internal writer context, never populated from HTTP request parameters.

    The installed release controller supplies a verifier, not cached claims. A
    and B call it independently so expiry, code/permit and lock drift are live.
    """
    token = _RUNTIME_VERIFIER.set(verifier)
    try:
        yield
    finally:
        _RUNTIME_VERIFIER.reset(token)


def current_runtime_bindings(connection: sqlite3.Connection, operation: str, at: str) -> Mapping[str, Any]:
    verifier = _RUNTIME_VERIFIER.get()
    if verifier is None:
        raise AuthorizationError("Installed schema20 runtime/continuity authority is absent")
    return verifier(connection, operation, at)


class AuthorizationError(provider_budget.PaidScopeBlocked):
    def __init__(self, message: str):
        super().__init__("capture_authorization_blocked", message)


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _require(value: bool, message: str) -> None:
    if not value:
        raise AuthorizationError(message)


def _payload(value: str) -> dict[str, Any]:
    try:
        result = json.loads(value)
    except (TypeError, ValueError) as error:
        raise AuthorizationError("Authorization evidence is invalid JSON") from error
    _require(isinstance(result, dict), "Authorization evidence must be an object")
    return dict(result)


def scope_hash(*, runtime_bindings: Mapping[str, Any], provider: str, operation: str) -> str:
    """Operation authorization scope; request/member billing identity is separate."""
    return digest({"bindings": dict(runtime_bindings), "provider": provider, "operation": operation})


def _require_binding(connection: sqlite3.Connection, bindings: Mapping[str, Any], *, at: str) -> None:
    _require(set(bindings) == BINDING_KEYS, "Runtime binding is incomplete or unrecognized")
    for key in BINDING_KEYS:
        if key.endswith("_sha256"):
            value = bindings[key]
            _require(isinstance(value, str) and len(value) == 64
                     and all(char in "0123456789abcdef" for char in value), "Runtime hash is invalid")
    active = activation_at(connection, at)
    _require(active is not None, "Current activation is missing")
    assert active is not None
    _require(bindings["profile_id"] in {"tikhub_managed_v1", "integrated_route_v1"},
             "This profile has no TikHub production authority")
    for key in ("activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256"):
        _require(bindings[key] == active[key], "Runtime activation/profile/roster binding changed")


def _validate_continuity_request(connection: sqlite3.Connection, *, bindings: Mapping[str, Any],
                                 operation: str, request_identity: str, at: str, sequence: int) -> None:
    from .capture_release import CONTINUITY_CONTRACT, _PERMIT_KEYS
    _require(bindings["profile_id"] == "tikhub_managed_v1",
             "Continuity authorizes Mode B legacy only; integrated capture requires independent qualification")
    row = connection.execute("SELECT * FROM transport_continuity_permits WHERE provider='tikhub' AND operation=? ORDER BY id DESC LIMIT 1",
                             (operation,)).fetchone()
    _require(row is not None, "Fixed continuity permit is absent")
    assert row is not None
    permit = dict(row)
    payload = _payload(permit["payload_json"])
    _require(sequence == 0 and permit["max_starts"] == 20
             and permit["permit_sha256"] == bindings["continuity_permit_sha256"]
             and permit["permit_sha256"] == digest({key: permit[key] for key in _PERMIT_KEYS})
             and payload.get("contract") == CONTINUITY_CONTRACT
             and usage_settlements._utc(permit["created_at"]) <= at < usage_settlements._utc(permit["expires_at"])
             and permit["build_sha256"] == bindings["build_receipt_sha256"]
             and permit["config_sha256"] == bindings["config_receipt_sha256"]
             and payload.get("runtime_sha256") == bindings["runtime_root_receipt_sha256"],
             "Fixed continuity permit is expired, changed or compensation was requested")
    members = connection.execute("SELECT rank,request_scope_identity FROM transport_continuity_permit_members WHERE permit_id=? ORDER BY rank",
                                 (permit["id"],)).fetchall()
    _require([item["rank"] for item in members] == list(range(1, 21))
             and digest([dict(item) for item in members]) == payload.get("members_sha256")
             and request_identity in {item["request_scope_identity"] for item in members},
             "Request is not one of the fixed twenty continuity members")
    from .capture_release import validate_continuity_natural_request
    validate_continuity_natural_request(connection, permit=permit, request_identity=request_identity, at=at)
    started = connection.execute("""SELECT count(*) FROM provider_request_start_events e
        JOIN paid_provider_dispatch_events d ON d.id=e.provider_send_marker_id
        JOIN provider_usage u ON u.id=d.provider_usage_id
        WHERE e.id>? AND lower(u.provider)='tikhub' AND u.operation=?""",
        (permit["start_high_watermark"], operation)).fetchone()[0]
    _require(started < 20, "Fixed continuity start cap is exhausted")


def validate_authorization(
    connection: sqlite3.Connection, *, runtime_bindings: Mapping[str, Any],
    operation: str, request_identity: str, at: str,
    amount_microusd: int, provider: str = "tikhub", sequence: int = 0,
    member_identities: tuple[str, ...] = (), issuance_ids: Mapping[str, int] | None = None,
    exclude_usage_id: int | None = None,
    expected_authority_sha256: str | None = None,
) -> dict[str, Any]:
    """Read-only A/B gate inside the current writer's short transaction.

    B may exclude only its current reserved/sent usage from the budget snapshot
    and must pass A's authority_sha256. Other reservations and all unknowns count.
    This is admission evidence, NOT permission to invoke HTTP before B commits.
    """
    _require(connection.in_transaction, "Authorization requires a writer transaction")
    require_current_process_writer_lock(connection)
    _require(connection.execute("PRAGMA user_version").fetchone()[0] in {20, 21}, "Authorization requires schema20")
    at = usage_settlements._utc(at)
    _require_binding(connection, runtime_bindings, at=at)
    _require(provider == "tikhub" and operation in provider_budget.PRICES_MICROUSD,
             "Provider operation has no verified price policy")
    _require(type(amount_microusd) is int and amount_microusd == provider_budget.PRICES_MICROUSD[operation],
             "Request amount differs from the verified operation price")
    gate = connection.execute("""SELECT * FROM capture_paid_send_gate_events
        WHERE provider=? AND operation=? AND recorded_at<=? ORDER BY id DESC LIMIT 1""",
        (provider, operation, at)).fetchone()
    _require(gate is not None and gate["state"] in {"open", "diagnostic_only"}, "Operation has no current authorization")
    assert gate is not None
    payload = _payload(gate["evidence_json"])
    _require(gate["event_sha256"] == digest({key: gate[key] for key in
        ("provider", "operation", "state", "reason", "evidence_json", "recorded_at")}),
        "Gate event hash does not match immutable evidence")
    expected_scope = scope_hash(runtime_bindings=runtime_bindings, provider=provider, operation=operation)
    _require(payload.get("contract") == CONTRACT and payload.get("bindings") == dict(runtime_bindings)
             and payload.get("scope_hash") == expected_scope and payload.get("operation") == operation,
             "Authorization scope/build binding is missing or changed")
    issued = usage_settlements._utc(str(payload.get("issued_at", "")))
    expires = usage_settlements._utc(str(payload.get("expires_at", "")))
    _require(issued == gate["recorded_at"] and issued <= at < expires, "Authorization expired or not yet issued")
    readiness = connection.execute("""SELECT * FROM provider_readiness_receipts
        WHERE provider=? AND operation=? AND created_at<=? ORDER BY id DESC LIMIT 1""",
        (provider, operation, at)).fetchone()
    _require(readiness is not None and readiness["id"] == payload.get("readiness_receipt_id")
             and readiness["receipt_sha256"] == payload.get("readiness_receipt_sha256"),
             "Readiness receipt is missing or superseded")
    assert readiness is not None
    _require(readiness["status"] in {"ready", "diagnostic_only"} and readiness["created_at"] <= at < readiness["expires_at"],
             "Provider readiness is blocked or expired")
    _require(readiness["receipt_sha256"] == digest({key: readiness[key] for key in
        ("provider", "operation", "status", "reason", "evidence_json", "created_at", "expires_at")}),
        "Readiness receipt hash is invalid")
    evidence = _payload(readiness["evidence_json"])
    _require(evidence.get("contract") == READINESS_CONTRACT
             and evidence.get("bindings") == dict(runtime_bindings)
             and evidence.get("scope_hash") == expected_scope
             and evidence.get("qualification") in {"qualified", "continuity_candidate", "native_candidate", "operator_authorized"},
             "No actual schema20-bound qualification receipt")
    if evidence["qualification"] == "continuity_candidate":
        _require(gate["state"] == "diagnostic_only" and readiness["status"] == "diagnostic_only",
                 "Continuity candidate cannot impersonate ordinary qualification")
        _validate_continuity_request(connection, bindings=runtime_bindings, operation=operation,
                                     request_identity=request_identity, at=at, sequence=sequence)
    elif evidence["qualification"] == "native_candidate":
        _require(gate["state"] == "diagnostic_only" and readiness["status"] == "diagnostic_only"
                 and runtime_bindings["profile_id"] in {"tikhub_managed_v1", "integrated_route_v1"},
                 "Native candidate cannot impersonate ordinary qualification")
        from .capture_release import validate_native_natural_request
        cohort_id = payload.get("native_cohort_id")
        _require(type(cohort_id) is int, "Native cohort ID is missing")
        assert isinstance(cohort_id, int)
        validate_native_natural_request(connection, operation=operation, cohort_id=cohort_id,
            request_identity=request_identity, at=at, sequence=sequence)
    elif evidence["qualification"] == "operator_authorized":
        _require(gate["state"] == "open" and readiness["status"] == "ready", "Operator production release is not open")
        from .capture_operator_release import validate_request
        validate_request(connection, runtime_bindings=runtime_bindings, operation=operation, at=at,
                         readiness_evidence=evidence, gate_payload=payload)
    else:
        _require(gate["state"] == "open" and readiness["status"] == "ready", "Ordinary qualification is not open")
    # These SHA fields bind the controller's verified permit and actual transport
    # manifest; old schema19 samples or a bare 'ready' row cannot authorize v20.
    _require(evidence.get("continuity_permit_sha256") == runtime_bindings["continuity_permit_sha256"]
             and evidence.get("transport_manifest_sha256") == payload.get("transport_manifest_sha256")
             and isinstance(evidence.get("transport_manifest_sha256"), str)
             and len(evidence["transport_manifest_sha256"]) == 64,
             "Readiness lacks the verified continuity/transport binding")
    authority = digest({"gate_id": gate["id"], "gate_sha256": gate["event_sha256"],
        "readiness_id": readiness["id"], "readiness_sha256": readiness["receipt_sha256"],
        "bindings": dict(runtime_bindings)})
    _require(expected_authority_sha256 is None or expected_authority_sha256 == authority,
             "Authorization changed between reservation and send")
    if exclude_usage_id is not None:
        usage = connection.execute("SELECT * FROM provider_usage WHERE id=?", (exclude_usage_id,)).fetchone()
        _require(usage is not None, "Reservation usage is missing")
        assert usage is not None
        details = _payload(usage["details_json"])
        _require(usage["provider"].lower() == provider and usage["operation"] == operation
                 and usage["currency"] == "USD" and provider_budget.micro_usd(usage["amount"]) == amount_microusd
                 and details.get("state") in {"reserved", "sent"}
                 and details.get("paid_scope_identity") == request_identity
                 and str(details.get("budget_day")) == provider_budget.budget_day(at),
                 "Budget exclusion does not identify this current reserved/sent request")
    summary = provider_budget.budget_summary(connection, at=at, exclude_usage_id=exclude_usage_id)
    budget = payload.get("budget", {})
    _require(isinstance(budget, dict), "Authorization budget is missing")
    bucket, budget_blocker = provider_budget.assess_budget_capacity(
        summary, operation=operation, amount_microusd=amount_microusd,
        authorization_budget=budget)
    if budget_blocker is not None:
        raise AuthorizationError(
            str(budget_blocker) if budget_blocker.error_code == "authorization_budget_invalid"
            else "Budget including reservations and unknown charges is exhausted") from budget_blocker
    identities = (request_identity, *member_identities)
    _require(len(set(identities)) == len(identities), "Duplicate request/member identity")
    issuances = dict(issuance_ids or {})
    _require((sequence == 0 and not issuances)
             or (1 <= sequence <= 4 and set(issuances) == set(identities) and len(member_identities) <= 1),
             "Compensation requires one fresh issuance per scope and singleton missing member")
    for index, identity in enumerate(identities):
        usage_settlements.require_scope_available(connection, identity=identity, sequence=sequence)
        if sequence:
            usage_settlements.validate_compensation_issuance(connection, issuance_id=issuances[identity],
                identity=identity, scope_kind="request" if index == 0 else "member", sequence=sequence,
                provider=provider, operation=operation, amount_microunits=amount_microusd, at=at)
    return {"authority_sha256": authority, "gate_event_id": gate["id"],
        "readiness_receipt_id": readiness["id"], "scope_hash": expected_scope,
        "charge_business_day": summary["budget_day"], "budget_bucket": bucket,
        "total_microusd_before": summary["total_microusd"], "provider_calls": 0}


def consume_authorized_start(connection: sqlite3.Connection, *, marker_id: int,
                             expected_authority_sha256: str, **admission: Any) -> dict[str, Any]:
    """B only: revalidate after send_marked, then claim/consume atomically once.

    Caller must roll back its entire B transaction (including marker/reservation)
    on failure and perform HTTP only after a successful commit. Never replay a
    returned result to send again; claim_network_start refuses repeat execution.
    """
    marker = connection.execute("""SELECT provider_usage_id FROM paid_provider_dispatch_events
        WHERE id=? AND event_type='send_marked'""", (marker_id,)).fetchone()
    _require(marker is not None, "B requires the real send marker")
    assert marker is not None
    proof = validate_authorization(connection, expected_authority_sha256=expected_authority_sha256,
                                    exclude_usage_id=marker[0], **admission)
    claimed = usage_settlements.claim_network_start(connection, marker_id=marker_id,
        request_identity=admission["request_identity"], at=admission["at"],
        sequence=admission.get("sequence", 0), member_identities=admission.get("member_identities", ()),
        issuance_ids=admission.get("issuance_ids"))
    return {**proof, **claimed}
