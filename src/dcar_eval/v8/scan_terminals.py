"""Fixed terminal classes for frozen account-discovery obligations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

CONTRACT_VERSION = "scan-terminal-v2"
LEGACY_CONTRACT_VERSION = "scan-terminal-v1"
TRANSIENT_RETRY_LIMIT = 3
TERMINAL_CLASSES = frozenset(
    {
        "success",
        "not_applicable",
        "provider_transient",
        "budget_deferred",
        "readiness_operator",
        "integrity",
        "deadline",
    }
)
PUBLICATION_BLOCKING_CLASSES = frozenset({"readiness_operator", "integrity"})
TERMINAL_BLOCKER_PRIORITY = {
    "deadline": 10,
    "budget_deferred": 20,
    "provider_transient": 30,
    "readiness_operator": 40,
    "integrity": 50,
}

NOT_APPLICABLE_CODES = frozenset({"operator_paused"})
DEADLINE_CODES = frozenset(
    {"business_day_expired", "profile_superseded", "missed_round"}
)
BUDGET_DEFERRED_CODES = frozenset(
    {
        "budget_blocked",
        "budget_daily_quota_exhausted",
        "category_budget_exhausted",
        "discovery_budget_exhausted",
        "metrics_budget_exhausted",
        "automatic_budget_exhausted",
        "repair_budget_exhausted",
        "global_budget_exhausted",
        "task_budget_exhausted",
        "incident_total_budget_exhausted",
        "incident_bucket_budget_exhausted",
        "paid_round_reconcile_cutoff",
        "profile_switch_drain",
    }
)
READINESS_OPERATOR_CODES = frozenset(
    {
        "identity_unresolved",
        "roster_not_ready",
        "provider_auth_blocked",
        "provider_balance_blocked",
        "price_contract_unverified",
        "required_capability_missing",
        "unsupported_operation",
        "unsupported_platform",
        "invalid_reference",
        "missing_reference",
        "paid_identity_hold",
        "billing_unknown_retry_blocked",
        "provider_circuit_open",
        "provider_blocked",
        "provider_transport_blocked",
        "operation_blocked",
        "storage_hard",
        "authorization_hard",
        "recovery_not_authorized",
        "recovery_probe_in_flight",
        "recovery_probe_consumed",
        "recovery_fault_domain_not_probeable",
        "compensation_authorization_required",
        "compensation_authorization_invalid",
        "compensation_authorization_consumed",
        "compensation_gap_invalid",
        "incident_authorization_invalid",
        "operation_recovery_invalid",
        "operation_recovery_stale",
        "storage_recovery_invalid",
        "storage_recovery_stale",
    }
)
PROVIDER_TRANSIENT_CODES = frozenset(
    {
        "transport_error",
        "provider_retry_requested",
        "upstream_error",
        "semantic_error",
    }
)
INTEGRITY_CODES = frozenset(
    {
        "identity_conflict",
        "member_scope_changed",
        "raw_identity_conflict",
        "repeated_cursor",
        "invalid_cursor",
        "cursor_reset_exhausted",
        "total_drift",
        "invalid_total",
        "scan_contract_mismatch",
        "roster_evidence_mismatch",
    }
)


@dataclass(frozen=True)
class TerminalDecision:
    terminal_class: str
    accounted: bool
    required: bool
    publication_blocker: bool


def classify_error(
    reason: str,
    *,
    http_status: int | None = None,
    has_raw: bool = False,
) -> str | None:
    """Return the fixed class for a terminal-capable error, or ``None``.

    Provider-transient classification is intentionally narrow.  A malformed
    HTTP-success response is retryable only when immutable raw evidence exists;
    otherwise it is an integrity failure rather than a reason to pay again.
    """

    if reason in NOT_APPLICABLE_CODES:
        return "not_applicable"
    if reason in DEADLINE_CODES:
        return "deadline"
    if reason in BUDGET_DEFERRED_CODES:
        return "budget_deferred"
    if reason in READINESS_OPERATOR_CODES:
        return "readiness_operator"
    if reason in INTEGRITY_CODES or reason.endswith("_integrity_error"):
        return "integrity"
    if reason.startswith(("manifest_", "checkpoint_", "raw_", "roster_")):
        return "integrity"
    if reason == "invalid_response":
        return "provider_transient" if has_raw else "integrity"
    if reason in PROVIDER_TRANSIENT_CODES:
        return "provider_transient"
    status = http_status
    if status is None and reason.startswith("http_"):
        try:
            status = int(reason.removeprefix("http_"))
        except ValueError:
            status = None
    if status in {408, 429} or (status is not None and status >= 500):
        return "provider_transient"
    if status is not None and 400 <= status < 500:
        return "readiness_operator"
    return None


def _legacy_classify_error(
    reason: str, *, http_status: int | None = None, has_raw: bool = False
) -> str | None:
    if reason in {
        "provider_circuit_open",
        "recovery_not_authorized",
        "recovery_probe_in_flight",
    }:
        return "budget_deferred"
    return classify_error(reason, http_status=http_status, has_raw=has_raw)


def decision(terminal_class: str) -> TerminalDecision:
    if terminal_class not in TERMINAL_CLASSES:
        raise ValueError("unknown scan terminal class")
    return TerminalDecision(
        terminal_class=terminal_class,
        accounted=True,
        required=terminal_class != "not_applicable",
        publication_blocker=terminal_class in PUBLICATION_BLOCKING_CLASSES,
    )


def blocker_priority(terminal_class: str) -> int:
    """Order coexisting blocked proofs without letting a softer retry mask a veto."""

    try:
        return TERMINAL_BLOCKER_PRIORITY[terminal_class]
    except KeyError as error:
        raise ValueError("scan terminal class is not a blocker") from error


def terminal_summary(*, reason: str, terminal_class: str) -> dict[str, Any]:
    value = decision(terminal_class)
    return {
        "terminal_contract_version": CONTRACT_VERSION,
        "terminal_class": value.terminal_class,
        "accounted": value.accounted,
        "required": value.required,
        "publication_blocker": value.publication_blocker,
        "reason": reason,
        "blocker": None if terminal_class in {"success", "not_applicable"} else reason,
    }


def _terminal_summary_for_contract(
    *, reason: str, terminal_class: str, contract_version: str
) -> dict[str, Any]:
    value = decision(terminal_class)
    return {
        "terminal_contract_version": contract_version,
        "terminal_class": value.terminal_class,
        "accounted": value.accounted,
        "required": value.required,
        "publication_blocker": value.publication_blocker,
        "reason": reason,
        "blocker": None if terminal_class in {"success", "not_applicable"} else reason,
    }


def validate_terminal_summary(summary: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(summary, Mapping):
        raise ValueError("scan_terminal_summary_invalid")
    reason = summary.get("reason")
    terminal_class = summary.get("terminal_class")
    if (
        summary.get("terminal_contract_version")
        not in {CONTRACT_VERSION, LEGACY_CONTRACT_VERSION}
        or not isinstance(reason, str)
        or not reason
        or not isinstance(terminal_class, str)
        or terminal_class not in TERMINAL_CLASSES
    ):
        raise ValueError("scan_terminal_summary_invalid")
    contract_version = str(summary["terminal_contract_version"])
    expected = _terminal_summary_for_contract(
        reason=reason,
        terminal_class=terminal_class,
        contract_version=contract_version,
    )
    if any(summary.get(key) != value for key, value in expected.items()):
        raise ValueError("scan_terminal_summary_invalid")
    if terminal_class not in {"success", "not_applicable"}:
        inferred = (
            _legacy_classify_error(reason, has_raw=reason == "invalid_response")
            if contract_version == LEGACY_CONTRACT_VERSION
            else classify_error(reason, has_raw=reason == "invalid_response")
        )
        if inferred != terminal_class:
            raise ValueError("scan_terminal_class_mismatch")
    elif terminal_class == "not_applicable" and reason not in NOT_APPLICABLE_CODES:
        raise ValueError("scan_terminal_class_mismatch")
    return expected


def coverage_decision(
    *,
    scope_total: int,
    succeeded: int,
    blocked: int,
    not_applicable: int,
    blocker_classes: frozenset[str] = frozenset(),
    prerequisite_complete: bool = True,
) -> dict[str, Any]:
    values = (scope_total, succeeded, blocked, not_applicable)
    if any(type(value) is not int or value < 0 for value in values):
        raise ValueError("scan coverage counts must be nonnegative integers")
    accounted = succeeded + blocked + not_applicable
    required = scope_total - not_applicable
    if required < 0 or accounted > scope_total or succeeded > required:
        raise ValueError("scan coverage counts are not conserved")
    if not blocker_classes <= TERMINAL_CLASSES:
        raise ValueError("scan coverage blocker class is unknown")
    success_percentage = round(100 * succeeded / required, 2) if required else 100.0
    complete = prerequisite_complete and succeeded == required
    partial_publishable = (
        prerequisite_complete
        and accounted == scope_total
        and success_percentage >= 99.0
        and not blocker_classes & PUBLICATION_BLOCKING_CLASSES
    )
    return {
        "scope_total": scope_total,
        "succeeded": succeeded,
        "blocked": blocked,
        "not_applicable": not_applicable,
        "accounted": accounted,
        "required": required,
        "accounted_percentage": (
            round(100 * accounted / scope_total, 2) if scope_total else 100.0
        ),
        "success_percentage": success_percentage,
        "complete": complete,
        "partial_publishable": partial_publishable,
    }
