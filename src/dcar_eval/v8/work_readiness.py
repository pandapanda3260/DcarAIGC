"""Read-only queue readiness decisions bound to one planner pass.

The final paid claim remains authoritative.  This module only separates work
that is runnable now from work already blocked by the same price, fault and
Beijing-day budget contracts, without creating a usage row or dispatch marker.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from decimal import Decimal
from typing import Any, Mapping

from .provider_budget import (
    CATEGORY_MICROUSD,
    DISCOVERY_OPERATIONS,
    POLICY_VERSION,
    PRICES_MICROUSD,
    BudgetBlocked,
    PaidScope,
    check_reservation,
    fault_state,
)


ASSESSMENT_CONTRACT = "work-readiness-v1"
BLOCKED_RECEIPT_CONTRACT = "work-readiness-blocked-v1"
BLOCKED_JOB_PREFIX = "work_readiness_blocked:"
_UNRESOLVED_USAGE_STATES = frozenset({"billing_unknown", "charged_unverified"})
_EARLY_BASE_BLOCKS = frozenset(
    {"storage_hard", "provider_circuit_open", "operation_blocked"}
)
_FAULT_SCOPE_BY_REASON = {
    "storage_hard": ("storage_hard", "all"),
    "provider_circuit_open": ("provider_hard", "tikhub"),
    "operation_blocked": ("operation", "tikhub"),
}


def _json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _sha(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _positive_optional(value: int | None, *, field: str) -> int | None:
    if value is None:
        return None
    if type(value) is not int:
        raise TypeError(f"{field} must be an integer")
    if value < 1:
        raise ValueError(f"{field} must be positive")
    return value


def _fault_marker(state: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if state is None:
        return None
    return {
        "open": state.get("open") is True,
        "generation": state.get("generation"),
        "state_fingerprint": state.get("state_fingerprint"),
        "fault_class": state.get("fault_class"),
        "reason": state.get("reason"),
        "cooldown": state.get("cooldown"),
        "half_open": state.get("half_open"),
    }


def _platform(operation: str) -> str:
    if operation.startswith("douyin_"):
        return "douyin"
    if operation.startswith("xiaohongshu_"):
        return "xiaohongshu"
    raise ValueError("operation has no supported platform")


def _target_scope(
    connection: sqlite3.Connection,
    *,
    operation: str,
    content_id: int | None,
    account_id: int | None,
    identity_id: int | None,
) -> tuple[dict[str, Any], str | None]:
    platform = _platform(operation)
    requested_content = _positive_optional(content_id, field="content_id")
    requested_account = _positive_optional(account_id, field="account_id")
    requested_identity = _positive_optional(identity_id, field="identity_id")
    row: sqlite3.Row | None = None
    if requested_identity is not None:
        row = connection.execute(
            "SELECT id identity_id,account_id,platform FROM "
            "account_platform_identities WHERE id=?",
            (requested_identity,),
        ).fetchone()
    elif requested_content is not None:
        row = connection.execute(
            """SELECT i.id identity_id,i.account_id,i.platform
               FROM content_items c
               JOIN account_platform_identities i
                 ON i.account_id=c.account_id AND i.platform=c.platform
               WHERE c.id=?""",
            (requested_content,),
        ).fetchone()
    elif requested_account is not None:
        row = connection.execute(
            "SELECT id identity_id,account_id,platform FROM "
            "account_platform_identities WHERE account_id=? AND platform=?",
            (requested_account, platform),
        ).fetchone()
    if row is None and any(
        value is not None
        for value in (requested_content, requested_account, requested_identity)
    ):
        return {
            "content_id": requested_content,
            "account_id": requested_account,
            "identity_id": requested_identity,
        }, "identity_unresolved"
    if row is None:
        return {
            "content_id": None,
            "account_id": None,
            "identity_id": None,
        }, None
    resolved = {
        "content_id": requested_content,
        "account_id": int(row["account_id"]),
        "identity_id": int(row["identity_id"]),
    }
    if (
        row["platform"] != platform
        or (
            requested_account is not None
            and requested_account != resolved["account_id"]
        )
        or (
            requested_identity is not None
            and requested_identity != resolved["identity_id"]
        )
    ):
        return resolved, "identity_conflict"
    if requested_content is not None:
        content = connection.execute(
            "SELECT account_id,platform FROM content_items WHERE id=?",
            (requested_content,),
        ).fetchone()
        if (
            content is None
            or content["account_id"] != resolved["account_id"]
            or content["platform"] != platform
        ):
            return resolved, "identity_conflict"
    return resolved, None


def _gate(
    *,
    reason: str,
    scope: Mapping[str, Any],
    evidence: Any,
    time_boundary: str | None = None,
) -> dict[str, Any]:
    marker = {
        "reason": reason,
        "scope": dict(scope),
        "evidence": evidence,
        **({"time_boundary": time_boundary} if time_boundary is not None else {}),
    }
    return {
        "reason": reason,
        "gate_scope": dict(scope),
        "state_fingerprint": _sha(marker),
        "time_boundary": time_boundary,
    }


class WorkReadinessPass:
    """Explicit cache scoped to one read-only planner pass and one timestamp."""

    def __init__(self, connection: sqlite3.Connection, *, at: str) -> None:
        self.connection = connection
        # Use the authoritative parser rather than maintaining another day rule.
        from .provider_budget import budget_day

        self.at = at
        self.budget_day = budget_day(at)
        self._base: dict[tuple[str, str], dict[str, Any]] = {}
        self._unresolved_by_slot: dict[int, list[dict[str, Any]]] | None = None

    def _faults(self, operation: str) -> dict[str, Any]:
        return {
            "storage": _fault_marker(
                fault_state(
                    self.connection, scope_kind="storage_hard", provider="all"
                )
            ),
            "provider": _fault_marker(
                fault_state(self.connection, scope_kind="provider_hard")
            ),
            "operation": _fault_marker(
                fault_state(
                    self.connection,
                    scope_kind="operation",
                    operation=operation,
                )
            ),
        }

    def _base_assessment(self, operation: str, category: str, *, manual_scope: PaidScope | None = None) -> dict[str, Any]:
        key = (operation, category)
        existing = self._base.get(key) if manual_scope is None else None
        if existing is not None:
            return existing
        unit_price = Decimal(PRICES_MICROUSD[operation]) / Decimal(1_000_000)
        faults = self._faults(operation)
        try:
            reservation = check_reservation(
                self.connection,
                scope=manual_scope if manual_scope is not None else PaidScope(category=category),
                operation=operation,
                unit_price=unit_price,
                currency="USD",
                at=self.at,
            )
        except BudgetBlocked as error:
            reason = str(getattr(error, "error_code", "budget_blocked"))
            fault_key = {
                "storage_hard": "storage",
                "provider_circuit_open": "provider",
                "operation_blocked": "operation",
            }.get(reason)
            evidence = faults.get(fault_key) if fault_key is not None else {
                "policy_version": POLICY_VERSION,
                "budget_bucket": (
                    "discovery"
                    if operation in DISCOVERY_OPERATIONS
                    else "metrics"
                ),
                "reserved_microusd": PRICES_MICROUSD[operation],
            }
            fault_scope = _FAULT_SCOPE_BY_REASON.get(reason)
            scope = (
                {
                    "scope_kind": fault_scope[0],
                    "provider": fault_scope[1],
                    **({"operation": operation} if fault_scope[0] == "operation" else {}),
                }
                if fault_scope is not None
                else {
                    "scope_kind": "budget",
                    "provider": "tikhub",
                    "category": category,
                    "operation": operation,
                }
            )
            result = {
                "runnable": False,
                **_gate(
                    reason=reason,
                    scope=scope,
                    evidence=evidence,
                    time_boundary=(
                        None if reason in _EARLY_BASE_BLOCKS else self.budget_day
                    ),
                ),
            }
        else:
            result = {
                "runnable": True,
                **_gate(
                    reason="ready",
                    scope={
                        "scope_kind": "ready",
                        "provider": "tikhub",
                        "category": category,
                        "operation": operation,
                    },
                    evidence={
                        "policy_version": reservation["policy_version"],
                        "budget_bucket": reservation["budget_bucket"],
                        "reserved_microusd": reservation["reserved_microusd"],
                        "relevant_faults": faults,
                    },
                    time_boundary=self.budget_day,
                ),
            }
        if manual_scope is None:
            self._base[key] = result
        return result

    def _unresolved_usage(self) -> dict[int, list[dict[str, Any]]]:
        if self._unresolved_by_slot is not None:
            return self._unresolved_by_slot
        values: dict[int, list[dict[str, Any]]] = {}
        rows = self.connection.execute(
            "SELECT id,details_json FROM provider_usage "
            "WHERE lower(provider)='tikhub' AND json_valid(details_json) "
            "AND json_extract(details_json,'$.state') "
            "IN ('billing_unknown','charged_unverified') ORDER BY id"
        )
        for row in rows:
            details = json.loads(str(row["details_json"]))
            slot_id = details.get("slot_id")
            if type(slot_id) is not int:
                continue
            values.setdefault(slot_id, []).append(
                {
                    "usage_id": int(row["id"]),
                    "state": details["state"],
                    "paid_scope_identity": details.get("paid_scope_identity"),
                }
            )
        from .account_cleanup import archived_slot_holds

        for slot_id, states in archived_slot_holds(self.connection).items():
            for state, count in states.items():
                values.setdefault(slot_id, []).append({
                    "state": state, "archived_usage_count": count,
                    "evidence_source": "account_cleanup_budget_daily",
                })
        self._unresolved_by_slot = values
        return values

    def _slot_gate(
        self,
        *,
        work_scope: Mapping[str, Any],
        stage: str | None,
        window_key: str | None,
    ) -> dict[str, Any] | None:
        if stage is None and window_key is None:
            return None
        if not stage or not window_key:
            raise ValueError("stage and window_key must be supplied together")
        content_id = work_scope.get("content_id")
        account_id = work_scope.get("account_id")
        if content_id is not None:
            predicate = "content_id=?"
            target_id = content_id
        elif account_id is not None:
            predicate = "account_id=?"
            target_id = account_id
        else:
            return None
        slot = self.connection.execute(
            f"SELECT id,last_error_code FROM fetch_slots WHERE {predicate} "
            "AND stage=? AND window_key=? AND lower(provider)='tikhub'",
            (target_id, stage, window_key),
        ).fetchone()
        if slot is None:
            return None
        unresolved = self._unresolved_usage().get(int(slot["id"]), [])
        identity_faults = []
        for paid_identity in sorted(
            {
                str(value["paid_scope_identity"])
                for value in unresolved
                if value.get("paid_scope_identity")
            }
        ):
            current = fault_state(
                self.connection,
                scope_kind="paid_identity_hold",
                paid_identity=paid_identity,
            )
            if current is not None:
                identity_faults.append(
                    {"paid_scope_identity": paid_identity, **(_fault_marker(current) or {})}
                )
        guarded = slot["last_error_code"] == "billing_unknown_retry_blocked"
        if not unresolved and not guarded and not any(
            value.get("open") is True for value in identity_faults
        ):
            return None
        return {
            "runnable": False,
            **_gate(
                reason="billing_unknown_retry_blocked",
                scope={
                    "scope_kind": "paid_identity_hold",
                    "provider": "tikhub",
                    "slot_id": int(slot["id"]),
                    "stage": stage,
                    "window_key": window_key,
                },
                evidence={
                    "guarded": guarded,
                    "unresolved": unresolved,
                    "identity_faults": identity_faults,
                },
            ),
        }

    def assess(
        self,
        *,
        operation: str,
        category: str,
        content_id: int | None = None,
        account_id: int | None = None,
        identity_id: int | None = None,
        stage: str | None = None,
        window_key: str | None = None,
        manual_command_run_id: int | None = None,
    ) -> dict[str, Any]:
        if operation not in PRICES_MICROUSD:
            raise ValueError("operation has no verified price")
        if category not in CATEGORY_MICROUSD:
            raise ValueError("category is not in the fixed paid policy")
        if (stage is None) != (window_key is None) or (
            stage is not None and (not stage or not window_key)
        ):
            raise ValueError("stage and window_key must be supplied together")
        resolved, target_error = _target_scope(
            self.connection,
            operation=operation,
            content_id=content_id,
            account_id=account_id,
            identity_id=identity_id,
        )
        work_scope = {
            "provider": "tikhub",
            "operation": operation,
            "category": category,
            **resolved,
            "stage": stage,
            "window_key": window_key,
        }
        work_scope_sha256 = _sha(work_scope)
        selected: dict[str, Any] | None
        if target_error is not None:
            selected = {
                "runnable": False,
                **_gate(
                    reason=target_error,
                    scope={"scope_kind": "target", **resolved},
                    evidence={"operation": operation},
                ),
            }
        else:
            selected = self._slot_gate(
                work_scope=work_scope,
                stage=stage,
                window_key=window_key,
            )
            if selected is None:
                manual_scope = None
                if manual_command_run_id is not None:
                    from .capture_manual import validate_command
                    specification = validate_command(self.connection, manual_command_run_id,
                        content_id=content_id, operation=operation, stage=stage)
                    proof = specification.get("transport_retry") or {}
                    manual_scope = PaidScope(category=category, content_id=content_id,
                        account_id=resolved["account_id"], identity_id=resolved["identity_id"],
                        manual_command_run_id=manual_command_run_id,
                        paid_scope_identity=proof.get("paid_scope_identity"))
                base = self._base_assessment(operation, category, manual_scope=manual_scope)
                if not base["runnable"] and base["reason"] in _EARLY_BASE_BLOCKS:
                    selected = base
                else:
                    authorization_id = resolved.get("identity_id") or resolved.get(
                        "account_id"
                    )
                    authorization = (
                        fault_state(
                            self.connection,
                            scope_kind="authorization_hard",
                            authorization_id=authorization_id,
                        )
                        if authorization_id is not None
                        else None
                    )
                    if authorization is not None and authorization.get("open") is True:
                        selected = {
                            "runnable": False,
                            **_gate(
                                reason="authorization_hard",
                                scope={
                                    "scope_kind": "authorization_hard",
                                    "provider": "tikhub",
                                    "authorization_id": str(authorization_id),
                                },
                                evidence=_fault_marker(authorization),
                            ),
                        }
                    elif not base["runnable"]:
                        selected = base
                    else:
                        selected = {
                            **base,
                            "state_fingerprint": _sha(
                                {
                                    "base": base["state_fingerprint"],
                                    "authorization": _fault_marker(authorization),
                                }
                            ),
                        }
        if selected is None:
            raise RuntimeError("readiness assessment produced no decision")
        readiness_generation = _sha(
            {
                "contract_version": "work-readiness-generation-v1",
                "work_scope_sha256": work_scope_sha256,
                "runnable": selected["runnable"],
                "reason": selected["reason"],
                "gate_scope": selected["gate_scope"],
                "state_fingerprint": selected["state_fingerprint"],
                "time_boundary": selected.get("time_boundary"),
            }
        )
        return {
            "contract_version": ASSESSMENT_CONTRACT,
            "runnable": bool(selected["runnable"]),
            "reason": str(selected["reason"]),
            "readiness_generation": readiness_generation,
            "work_scope": work_scope,
            "work_scope_sha256": work_scope_sha256,
            "budget_day": self.budget_day,
            "gate_scope": dict(selected["gate_scope"]),
            "state_fingerprint": str(selected["state_fingerprint"]),
        }


def assess_work_readiness(
    connection: sqlite3.Connection,
    *,
    operation: str,
    category: str,
    at: str,
    content_id: int | None = None,
    account_id: int | None = None,
    identity_id: int | None = None,
    stage: str | None = None,
    window_key: str | None = None,
) -> dict[str, Any]:
    """Assess one candidate without retaining cache beyond this call."""

    return WorkReadinessPass(connection, at=at).assess(
        operation=operation,
        category=category,
        content_id=content_id,
        account_id=account_id,
        identity_id=identity_id,
        stage=stage,
        window_key=window_key,
    )


def record_work_blocked(
    connection: sqlite3.Connection,
    *,
    assessment: Mapping[str, Any],
    at: str,
) -> dict[str, Any]:
    """Append the first audit receipt for a blocked scope/generation.

    This receipt is never an authority for current blocked statistics; callers
    must use a fresh assessment.  No scheduler attempt is created.
    """

    if not connection.in_transaction:
        raise RuntimeError("blocked receipt requires a caller transaction")
    if (
        assessment.get("contract_version") != ASSESSMENT_CONTRACT
        or assessment.get("runnable") is not False
        or not isinstance(assessment.get("work_scope"), Mapping)
        or not isinstance(assessment.get("reason"), str)
        or not isinstance(assessment.get("gate_scope"), Mapping)
    ):
        raise ValueError("only a valid blocked assessment may be recorded")
    scope_sha = assessment.get("work_scope_sha256")
    generation = assessment.get("readiness_generation")
    state_fingerprint = assessment.get("state_fingerprint")
    if any(
        not isinstance(value, str) or len(value) != 64
        for value in (scope_sha, generation, state_fingerprint)
    ) or scope_sha != _sha(dict(assessment["work_scope"])):
        raise ValueError("blocked assessment hashes are invalid")
    job_id = f"{BLOCKED_JOB_PREFIX}{scope_sha[:24]}"
    scheduled_for = f"generation:{generation}"
    existing = connection.execute(
        "SELECT id,details_json FROM scheduler_runs WHERE job_id=? AND scheduled_for=?",
        (job_id, scheduled_for),
    ).fetchone()
    if existing is not None:
        details = json.loads(str(existing["details_json"]))
        if (
            details.get("contract_version") != BLOCKED_RECEIPT_CONTRACT
            or details.get("work_scope_sha256") != scope_sha
            or details.get("readiness_generation") != generation
            or details.get("work_scope") != dict(assessment["work_scope"])
            or details.get("reason") != assessment["reason"]
            or details.get("gate_scope") != dict(assessment["gate_scope"])
            or details.get("state_fingerprint") != state_fingerprint
        ):
            raise RuntimeError("blocked receipt identity conflicts with stored data")
        return {"recorded": False, "run_id": int(existing["id"]), **details}
    details = {
        "contract_version": BLOCKED_RECEIPT_CONTRACT,
        "work_scope": dict(assessment["work_scope"]),
        "work_scope_sha256": scope_sha,
        "readiness_generation": generation,
        "reason": assessment["reason"],
        "gate_scope": dict(assessment["gate_scope"]),
        "state_fingerprint": state_fingerprint,
        "blocked_at": at,
    }
    cursor = connection.execute(
        """INSERT INTO scheduler_runs(
               job_id,scheduled_for,status,started_at,completed_at,details_json)
           VALUES (?,?,'succeeded',?,?,?)""",
        (job_id, scheduled_for, at, at, _json(details)),
    )
    if cursor.lastrowid is None:
        raise RuntimeError("blocked receipt insert returned no id")
    return {"recorded": True, "run_id": int(cursor.lastrowid), **details}
