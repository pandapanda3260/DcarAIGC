"""Catalog-derived automatic work, with immutable plans and live send fences.

The installed policy is additional, explicit business-scope authority. The
historical activation remains a binding for provider/runtime/budget controls;
it is not represented as containing the new catalog members.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
import json
import os
import sqlite3
from typing import Any, Mapping

from . import capture_planning as planning

CONTRACT = "account-catalog-capture-snapshot-v1"
IDENTITY_KEYS = ("identity_id", "account_id", "platform", "uid", "locator_sha256")
_PLANNING_VALIDATION = ContextVar("catalog_planning_validation", default=None)


def _planning_cache(connection):
    value = _PLANNING_VALIDATION.get()
    return value if value is not None and value["connection"] is connection and connection.in_transaction else None


@contextmanager
def planning_validation(connection: sqlite3.Connection, policy: Mapping[str, Any], snapshot: Mapping[str, Any], *,
                        plan: Mapping[str, Any] | None = None):
    """Reuse a verified snapshot only inside the caller's single planning transaction.

    The planner holds BEGIN IMMEDIATE for this whole block and does not mutate
    directory identities/status. Admission and send checks explicitly disable
    this cache and always resolve current member evidence again.
    """
    from .account_catalog_capture_release import ACCOUNT_CATALOG_POLICY_V2, ACCOUNT_CATALOG_POLICY_V3
    if (not connection.in_transaction or policy not in (ACCOUNT_CATALOG_POLICY_V2, ACCOUNT_CATALOG_POLICY_V3)
            or snapshot.get("contract") != CONTRACT
            or snapshot.get("policy_sha256") != planning.digest(policy)
            or snapshot.get("snapshot_sha256") != planning.digest({k: v for k, v in snapshot.items() if k != "snapshot_sha256"})):
        raise ValueError("planning validation requires one transaction and its verified catalog snapshot")
    members = snapshot["eligibility"]["eligible_members"]
    indexed = {member["identity_id"]: member for member in members}
    if len(indexed) != len(members):
        raise ValueError("catalog snapshot repeats an identity")
    if plan is not None and plan.get("catalog_snapshot", {}).get("snapshot_sha256") != snapshot["snapshot_sha256"]:
        raise ValueError("planning source plan differs from catalog snapshot")
    token = _PLANNING_VALIDATION.set({"connection": connection, "policy": dict(policy),
        "snapshot_sha256": snapshot["snapshot_sha256"], "members": indexed, "plans": {}})
    try:
        from .capture_metric_cycles import planning_validation as metric_cycle_validation, verified_planning_plan
        with metric_cycle_validation(connection, plan=plan) as bound_plan:
            if plan is not None:
                verified = verified_planning_plan(connection, plan["id"])
                # The ordinary call passes the exact snapshot just validated
                # above. The cycle binding already proved its whole plan equals
                # persisted JSON. Separate snapshot copies retain a strict check.
                if (plan.get("catalog_snapshot") is not snapshot and
                        planning.canonical(verified.get("catalog_snapshot")) != planning.canonical(snapshot)):
                    raise ValueError("planning source plan differs from catalog snapshot")
                if (verified.get("catalog_mode") == "active" and verified.get("shadow") is False
                        and type(verified.get("roster_snapshot_id")) is int
                        and verified.get("roster_members_sha256")):
                    _PLANNING_VALIDATION.get()["plans"][plan["id"]] = {k: v for k, v in verified.items() if k != "id"}
                # An empty plan can predate its roster binding. Keep the old
                # behavior: no member can use it until validate_plan_member
                # performs (and fails) the complete catalog proof below.
            yield bound_plan
        if not connection.in_transaction:
            raise ValueError("catalog planning transaction ended inside validation context")
    finally:
        _PLANNING_VALIDATION.reset(token)


def installed_policy(connection: sqlite3.Connection, *, at: str, use_planning_cache: bool = True) -> dict[str, Any] | None:
    cached = _planning_cache(connection) if use_planning_cache else None
    if cached is not None:
        return dict(cached["policy"])
    if not os.environ.get("DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT"):
        return None
    from .account_cleanup_runtime import installed_evidence
    evidence = installed_evidence(connection, at=at, maintenance_only=True)
    policy = evidence.get("catalog_capture_policy")
    proof = evidence.get("catalog_capture_proof")
    if policy is None and proof is None:
        return None
    from .account_catalog_capture_release import ACCOUNT_CATALOG_POLICY_V2, ACCOUNT_CATALOG_POLICY_V3
    if (policy not in (ACCOUNT_CATALOG_POLICY_V2, ACCOUNT_CATALOG_POLICY_V3) or not isinstance(proof, dict)
            or evidence.get("catalog_capture_policy_sha256") != planning.digest(policy)
            or proof.get("proof_sha256") != planning.digest({k: v for k, v in proof.items() if k != "proof_sha256"})):
        raise ValueError("catalog capture policy is not verified")
    return dict(policy)


def _blocked(code: str, message: str):
    from .provider_budget import PaidScopeBlocked
    return PaidScopeBlocked(code, message)


def freeze_snapshot(connection: sqlite3.Connection, *, policy: Mapping[str, Any]) -> dict[str, Any]:
    from .account_capture_eligibility import derive_capture_eligibility
    derived = derive_capture_eligibility(connection)
    value = {"contract": CONTRACT, "policy_sha256": planning.digest(policy),
             "eligibility": derived}
    return {**value, "snapshot_sha256": planning.digest(value)}


def validate_plan_member(connection: sqlite3.Connection, plan_id: int, identity_id: int, *,
                         at: str, policy: Mapping[str, Any] | None = None,
                         use_planning_cache: bool = True) -> dict[str, Any]:
    if type(plan_id) is not int or plan_id <= 0 or type(identity_id) is not int or identity_id <= 0:
        raise _blocked("catalog_plan_invalid", "Automatic work requires a persisted catalog plan and identity")
    cached = _planning_cache(connection) if use_planning_cache else None
    policy = policy if policy is not None else installed_policy(connection, at=at, use_planning_cache=use_planning_cache)
    if policy is None:
        raise _blocked("catalog_policy_unavailable", "Automatic catalog policy is not installed")
    plan = cached["plans"].get(plan_id) if cached is not None else None
    if plan is None:
        row = connection.execute("SELECT payload_json,plan_sha256,mode FROM capture_source_plans WHERE id=?", (plan_id,)).fetchone()
        if row is None:
            raise _blocked("catalog_plan_invalid", "Automatic catalog plan is missing")
        try:
            plan = json.loads(row["payload_json"])
            snap = plan["catalog_snapshot"]
            valid = (isinstance(plan, dict) and isinstance(snap, dict)
                and row["mode"] == "active" and plan.get("shadow") is False and plan.get("catalog_mode") == "active"
                and planning.digest(plan) == row["plan_sha256"] and snap.get("contract") == CONTRACT
                and snap.get("policy_sha256") == planning.digest(policy)
                and snap.get("snapshot_sha256") == planning.digest({k: v for k, v in snap.items() if k != "snapshot_sha256"})
                and isinstance(snap["eligibility"]["eligible_members"], list)
                and type(plan["roster_snapshot_id"]) is int and bool(plan["roster_members_sha256"]))
        except (KeyError, TypeError, ValueError):
            valid = False
        if not valid:
            raise _blocked("catalog_plan_changed", "Automatic catalog plan or policy changed")
        if cached is not None:
            cached["plans"][plan_id] = plan
    snap = plan["catalog_snapshot"]
    matches = [m for m in snap["eligibility"]["eligible_members"] if m.get("identity_id") == identity_id]
    if len(matches) != 1:
        raise _blocked("catalog_member_not_planned", "Account was not uniquely included in this task's catalog snapshot")
    frozen = matches[0]
    from .account_capture_eligibility import require_directory_capture_member
    try:
        if cached is not None:
            current = cached["members"].get(identity_id)
            if current is None:
                raise _blocked("catalog_member_ineligible", "Account is ineligible in the current planning snapshot")
        else:
            current = require_directory_capture_member(connection, identity_id)
    except ValueError as exc:
        raise _blocked(str(getattr(exc, "code", "catalog_member_ineligible")), str(exc)) from exc
    if any(current.get(k) != frozen.get(k) for k in IDENTITY_KEYS):
        raise _blocked("catalog_identity_changed", "Account identity or locator changed after planning")
    return {**current, **{key: plan[key] for key in ("activation_id", "activation_sha256", "profile_id")},
            "roster_snapshot_id": plan["roster_snapshot_id"], "roster_snapshot_hash": plan["roster_members_sha256"]}


def assignment_for_plan(connection: sqlite3.Connection, plan_id: int, *, identity_id: int,
                        operation: str, at: str, create: bool = False,
                        policy: Mapping[str, Any] | None = None,
                        use_planning_cache: bool = True) -> dict[str, Any]:
    member = validate_plan_member(connection, plan_id, identity_id, at=at, policy=policy,
        use_planning_cache=use_planning_cache)
    if not operation.startswith(member["platform"] + "_"):
        raise _blocked("identity_conflict", "Catalog operation differs from the account platform")
    key = "catalog-account:" + str(member["account_id"])
    assignment = planning.current_assignment(connection, "account", key, operation, at=at)
    if assignment is None and create:
        from .runtime_database import require_current_process_writer_lock
        require_current_process_writer_lock(connection)
        planning.assign_route(connection, scope_type="account", scope_key=key, provider="tikhub",
            operation=operation, expected_generation=0, route="integrated", mode="active",
            effective_at=at, recorded_at=at, account_id=member["account_id"])
        assignment = planning.current_assignment(connection, "account", key, operation, at=at)
    if (assignment is None or assignment["account_id"] != member["account_id"]
            or assignment["content_id"] is not None or assignment["source_plan_id"] is not None
            or assignment["route"] != "integrated" or assignment["mode"] != "active"
            or assignment["provider"] != "tikhub"):
        raise _blocked("route_not_active", "Account has no matching automatic catalog route")
    return assignment


def validate_paid_target(connection: sqlite3.Connection, scope: Any, *, identity_id: int,
                         content_id: int | None = None, at: str) -> dict[str, Any]:
    """Fence a single target or exact frozen-batch member under its real owner.

    This runs at A/B paid boundaries and never reuses planning-time policy or
    member caches, even when its caller accidentally enters a planning context.
    """
    if (getattr(scope, "manual_command_run_id", None) is not None
            or type(scope.catalog_plan_id) is not int or scope.catalog_plan_id <= 0
            or not scope.scheduler_owner_token):
        raise _blocked("catalog_work_invalid", "Catalog request lacks an exclusive plan and durable owner")
    row = connection.execute("SELECT details_json FROM scheduler_runs WHERE id=?", (scope.scheduler_run_id,)).fetchone()
    if row is None:
        raise _blocked("attempt_owner_lost", "Catalog request has no scheduler owner")
    try:
        details = json.loads(row[0])
        work_id = details["checkpoint"]["work_id"]
        frozen_identity = details["identity"]
        if (type(work_id) is not int or frozen_identity.get("catalog_plan_id") != scope.catalog_plan_id):
            raise ValueError("scheduler plan differs")
    except (KeyError, TypeError, ValueError) as exc:
        raise _blocked("catalog_work_invalid", "Catalog scheduler owner has no intact frozen work/plan") from exc
    owner = connection.execute("SELECT * FROM capture_work_items WHERE id=?", (work_id,)).fetchone()
    if owner is None or owner["work_identity"] != frozen_identity.get("work_identity"):
        raise _blocked("catalog_work_invalid", "Catalog request has no matching owner work")
    try:
        owner_envelope = json.loads(owner["envelope_json"])
        batch_id = owner_envelope.get("request_batch_id")
    except (TypeError, ValueError) as exc:
        raise _blocked("catalog_work_changed", "Catalog owner envelope is invalid") from exc
    work = owner
    if batch_id is not None and owner["operation"] == "douyin_video_statistics":
        batch = connection.execute("SELECT * FROM fetch_request_batches WHERE id=?", (batch_id,)).fetchone()
        if (content_id is None or batch is None or batch["work_id"] != owner["id"]
                or batch["provider"] != "tikhub" or batch["operation"] != owner["operation"] or batch["sequence"] != 0):
            raise _blocked("catalog_batch_invalid", "Catalog request has no matching frozen statistics batch")
        members = connection.execute("SELECT content_id,account_id FROM fetch_request_batch_members WHERE batch_id=? AND sequence=0", (batch_id,)).fetchall()
        targets = [m for m in members if m["content_id"] == content_id]
        if len(members) not in {1, 2} or len(targets) != 1:
            raise _blocked("catalog_batch_invalid", "Paid target is not one exact frozen batch member")
        candidates = connection.execute("SELECT * FROM capture_work_items WHERE content_id=? AND account_id=? AND operation=? "
            "AND source_plan_id=? AND json_extract(envelope_json,'$.request_batch_id')=?",
            (content_id, targets[0]["account_id"], owner["operation"], scope.catalog_plan_id, batch_id)).fetchall()
        if len(candidates) != 1:
            raise _blocked("catalog_batch_invalid", "Frozen batch member has no unique catalog work")
        work = candidates[0]
    try:
        envelope = json.loads(work["envelope_json"])
        for checked_work, checked_envelope in ((owner, owner_envelope), (work, envelope)):
            if (checked_envelope.get("catalog_plan_id") != scope.catalog_plan_id
                    or checked_work["source_plan_id"] != scope.catalog_plan_id
                    or checked_work["owner_token"] != scope.scheduler_owner_token
                    or checked_work["state"] != "running"
                    or checked_envelope.get("manual_command_run_id") is not None):
                raise ValueError("catalog owner or plan changed")
        if (envelope.get("identity_id") != identity_id or work["content_id"] != content_id
                or envelope.get("content_id") != content_id
                or envelope.get("account_id") != work["account_id"]
                or envelope.get("operation") != work["operation"]
                or any(envelope.get(key) != owner_envelope.get(key) for key in
                    ("catalog_plan_id", "logical_due", "activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256"))):
            raise ValueError("catalog target changed")
    except (KeyError, TypeError, ValueError) as exc:
        raise _blocked("catalog_work_changed", "Catalog work target, plan or owner changed") from exc
    member = validate_plan_member(connection, scope.catalog_plan_id, identity_id, at=at, use_planning_cache=False)
    if member["account_id"] != work["account_id"]:
        raise _blocked("catalog_identity_changed", "Catalog work account changed")
    bindings = {"activation_id": "activation_id", "profile_id": "profile_id",
        "roster_snapshot_id": "roster_snapshot_id", "roster_members_sha256": "roster_snapshot_hash"}
    if any(envelope.get(wire) != member[key] for wire, key in bindings.items()):
        raise _blocked("catalog_work_changed", "Catalog work no longer matches its frozen plan bindings")
    from .profile_activations import activation_at
    active = activation_at(connection, at)
    if (active is None or any(active.get(wire) != member[key] for wire, key in bindings.items())
            or active.get("activation_sha256") != member["activation_sha256"]
            or any(getattr(scope, key) != member[key] for key in
                ("activation_id", "roster_snapshot_id", "roster_snapshot_hash"))):
        raise _blocked("profile_superseded", "Catalog plan belongs to a different runtime activation")
    return member


def prepare_proven_locators(connection: sqlite3.Connection, members: list[dict[str, Any]]) -> dict[int, str]:
    """Validate admission/raw evidence before acquiring the SQLite write lock."""
    import hashlib
    from .account_capture_eligibility import require_directory_capture_member
    from .account_operating_receipts import load_admission_members
    candidates = [m for m in members if m.get("locator_evidence", {}).get("kind") == "verified_account_admission"]
    if not candidates:
        return {}
    admissions = load_admission_members(connection)
    result = {}
    for member in candidates:
        current = require_directory_capture_member(connection, member["identity_id"])
        admission = admissions.get(member["identity_id"])
        if (admission is None or any(current.get(k) != member.get(k) for k in IDENTITY_KEYS)
                or current.get("locator_evidence") != member.get("locator_evidence")
                or planning.digest(admission) != member["locator_evidence"].get("admission_sha256")
                or admission["account_id"] != member["account_id"]
                or admission["member"].get("platform") != "douyin"
                or admission["member"].get("uid") != member["uid"]):
            raise ValueError("Catalog locator admission no longer matches its frozen member")
        sec = admission["member"]["metadata"]["sec_user_id"]
        if hashlib.sha256(sec.encode()).hexdigest() != member["locator_sha256"]:
            raise ValueError("Catalog locator differs from its verified admission")
        result[member["identity_id"]] = sec
    return result


def materialize_proven_locators(connection: sqlite3.Connection, members: list[dict[str, Any]], *, at: str,
                               verified_locators: Mapping[int, str] | None = None) -> int:
    """Cache receipt-proven locators without purchasing or replacing a reference."""
    import hashlib
    from .account_capture_eligibility import require_directory_capture_member
    from .account_operating_receipts import load_admission_members

    if not connection.in_transaction:
        raise ValueError("Catalog locator projection requires a writer transaction")
    candidates = [member for member in members
                  if member.get("locator_evidence", {}).get("kind") == "verified_account_admission"]
    if not candidates:
        return 0
    if verified_locators is not None:
        created = 0
        for member in candidates:
            identity_id = member["identity_id"]
            sec = verified_locators.get(identity_id)
            if not isinstance(sec, str) or hashlib.sha256(sec.encode()).hexdigest() != member["locator_sha256"]:
                raise ValueError("Prepared catalog locator differs from snapshot")
            old = connection.execute("SELECT reference_value FROM account_provider_references "
                "WHERE account_identity_id=? AND lower(provider)='tikhub' AND reference_kind='sec_user_id'", (identity_id,)).fetchall()
            if any(row[0] != sec for row in old):
                raise ValueError("An existing account locator cannot be replaced")
            if not old:
                from .account_reference_storage import store_reference
                store_reference(connection, account_identity_id=identity_id, platform=member["platform"],
                    provider="tikhub", reference_kind="sec_user_id", reference_value=sec,
                    source_raw_response_id=None, created_at=at, updated_at=at)
                created += 1
        return created
    admissions = load_admission_members(connection)
    created = 0
    for member in candidates:
        identity_id = member["identity_id"]
        current = require_directory_capture_member(connection, identity_id)
        admission = admissions.get(identity_id)
        if (admission is None or any(current.get(key) != member.get(key) for key in IDENTITY_KEYS)
                or current.get("locator_evidence") != member.get("locator_evidence")
                or planning.digest(admission) != member["locator_evidence"].get("admission_sha256")
                or admission["account_id"] != member["account_id"]
                or admission["member"].get("platform") != "douyin"
                or admission["member"].get("uid") != member["uid"]):
            raise ValueError("Catalog locator admission no longer matches its frozen member")
        sec = admission["member"]["metadata"]["sec_user_id"]
        if hashlib.sha256(sec.encode()).hexdigest() != member["locator_sha256"]:
            raise ValueError("Catalog locator differs from its verified admission")
        old = connection.execute("SELECT reference_value FROM account_provider_references "
            "WHERE account_identity_id=? AND lower(provider)='tikhub' AND reference_kind='sec_user_id'", (identity_id,)).fetchall()
        if any(row[0] != sec for row in old):
            raise ValueError("An existing account locator cannot be replaced")
        if not old:
            from .account_reference_storage import store_reference
            store_reference(connection, account_identity_id=identity_id, platform=member["platform"],
                provider="tikhub", reference_kind="sec_user_id", reference_value=sec,
                source_raw_response_id=None, created_at=at, updated_at=at)
            created += 1
    return created


def synchronize_enabled(connection: sqlite3.Connection, snapshot: Mapping[str, Any], *,
                        activation_id: int, at: str, verified_locators: Mapping[int, str] | None = None) -> None:
    """Keep the legacy projection derived, never another input to eligibility."""
    from .account_states import set_account_enabled_in_transaction
    materialize_proven_locators(connection, snapshot["eligibility"]["eligible_members"], at=at,
                               verified_locators=verified_locators)
    eligible = {m["account_id"] for m in snapshot["eligibility"]["eligible_members"]}
    for row in connection.execute("SELECT DISTINCT i.id identity_id,a.id account_id,a.enabled "
            "FROM account_directory_rows d JOIN accounts a ON a.id=d.account_id "
            "JOIN account_platform_identities i ON i.account_id=a.id AND i.platform=d.platform").fetchall():
        enabled = row["account_id"] in eligible
        if bool(row["enabled"]) != enabled:
            set_account_enabled_in_transaction(connection, row["identity_id"], enabled=enabled,
                effective_at=at, created_at=at, actor="catalog-planner", reason="derive automatic eligibility from account directory",
                activation_id=activation_id, metadata={"contract": CONTRACT, "snapshot_sha256": snapshot["snapshot_sha256"]})


def public_statuses(connection: sqlite3.Connection) -> dict[int, dict[str, Any]]:
    """Read published planner results; replicas need no local provider raw files."""
    if not connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='capture_source_plans'").fetchone():
        return {}
    row = connection.execute("SELECT payload_json,created_at FROM capture_source_plans "
        "WHERE mode='active' AND json_extract(payload_json,'$.catalog_mode')='active' "
        "AND json_extract(payload_json,'$.shadow')=0 "
        "AND json_type(payload_json,'$.catalog_snapshot')='object' ORDER BY id DESC LIMIT 1").fetchone()
    if row is None:
        return {}
    derived = json.loads(row[0])["catalog_snapshot"]["eligibility"]
    output = {}
    for member in derived["eligible_members"] + derived["excluded_members"]:
        directory_id = member.get("directory_row_id")
        if directory_id is not None:
            output[int(directory_id)] = {"eligible": bool(member.get("eligible", member in derived["eligible_members"])),
                "reason_code": member.get("reason_code", "eligible"),
                "reason_label": member.get("reason_label", "自动更新"),
                "account_status": member.get("account_status"), "account_id": member.get("account_id"),
                "identity_id": member.get("identity_id"), "platform": member.get("platform"),
                "uid": member.get("uid"), "identity_status": member.get("identity_status"),
                "snapshot_created_at": row["created_at"]}
    return output


def _public_platform_blocks(connection: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    """Project installed transport state without reading local evidence files.

    Schema23 exposes the first missing stage contract even before the first
    catalog plan exists. This read model never grants send permission: complete
    runtime, budget and immutable scope checks still run at dispatch time.
    """
    from . import platform_adapters as adapters
    from .account_capture_eligibility import SUPPORTED_PLATFORMS
    output = {}
    tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    current = connection.execute("PRAGMA user_version").fetchone()[0] >= 23
    gates = {}
    if "capture_paid_send_gate_events" in tables:
        gates = {row["operation"]: row["state"] for row in connection.execute(
            "SELECT operation,state FROM capture_paid_send_gate_events WHERE provider='tikhub' "
            "AND julianday(recorded_at)<=julianday('now') ORDER BY id")}
    # The policy digest is part of every published plan. A replica can inspect
    # its platform scope without opening the local installation receipt/raw.
    policy_platforms = None
    if "capture_source_plans" in tables:
        plan = connection.execute("SELECT payload_json FROM capture_source_plans WHERE mode='active' "
            "AND json_extract(payload_json,'$.catalog_mode')='active' "
            "AND json_extract(payload_json,'$.shadow')=0 ORDER BY id DESC LIMIT 1").fetchone()
        if plan:
            from .account_catalog_capture_release import ACCOUNT_CATALOG_POLICY_V2, ACCOUNT_CATALOG_POLICY_V3
            policy_hash = json.loads(plan[0]).get("catalog_snapshot", {}).get("policy_sha256")
            for policy in (ACCOUNT_CATALOG_POLICY_V2, ACCOUNT_CATALOG_POLICY_V3):
                if planning.digest(policy) == policy_hash:
                    policy_platforms = policy.get("platforms", ["douyin", "xiaohongshu"])
                    break
    for platform in SUPPORTED_PLATFORMS:
        stages = (adapters.PROFILE_OPERATIONS.get(platform), adapters.POST_OPERATIONS.get(platform))
        if any(not operation or operation not in adapters.ROUTES for operation in stages):
            reason, label = "platform_unsupported", "当前运行版本未配置此平台采集合同"
        elif policy_platforms is not None and platform not in policy_platforms:
            reason, label = "platform_policy_unavailable", "当前采集运行合同尚未启用此平台"
        elif current and any(operation not in gates for operation in stages):
            reason, label = "platform_source_unconfigured", "平台资料或作品数据源尚未启用"
        elif any(operation in gates and gates[operation] not in {"open", "diagnostic_only"} for operation in stages):
            reason, label = "provider_transport_blocked", "平台数据源当前暂停采集"
        elif any(gates.get(operation) == "diagnostic_only" for operation in stages):
            reason, label = "provider_diagnostic_only", "平台数据源仅开放验证，尚未开放日常采集"
        else:
            continue
        output[platform] = {"eligible": False, "reason_code": reason, "reason_label": label}
    return output


def annotate_accounts(
    connection: sqlite3.Connection, accounts: list[dict[str, Any]], *, live: bool = False,
) -> None:
    """Show stage capability before identity and published evidence/plan state.

    Only an explicit local preview reads raw evidence. Replicas can show missing
    contracts and inputs before the first snapshot, but never infer admission.
    """
    from .account_capture_eligibility import SUPPORTED_PLATFORMS
    blocks = _public_platform_blocks(connection)
    if live:
        from .account_capture_eligibility import derive_capture_eligibility
        checked = derive_capture_eligibility(connection)
        values = {member["directory_row_id"]: member for member in
                  checked["eligible_members"] + checked["excluded_members"]}
    else:
        values = public_statuses(connection)
    for account in accounts:
        platform = account.get("directory_platform")
        if platform is None:
            # Non-directory/legacy callers have no platform authority to show.
            continue
        value = values.get(account.get("directory_row_id"))
        uid = account.get("directory_uid")
        current_identity = next((item for item in account.get("platforms", [])
                                 if item.get("platform") == platform and item.get("uid") == uid), None)
        if platform not in SUPPORTED_PLATFORMS:
            value = {"eligible": False, "reason_code": "platform_unsupported", "reason_label": "暂不支持此平台采集"}
        elif platform in blocks:
            value = blocks[platform]
        elif live and value is not None:
            pass
        elif not uid or account.get("id", 0) <= 0:
            value = {"eligible": False, "reason_code": "identity_missing", "reason_label": "缺少有效的平台 UID 或账号关联"}
        elif current_identity is None:
            value = {"eligible": False, "reason_code": "identity_conflict", "reason_label": "账号与平台身份不一致"}
        elif (value is None
                or value.get("account_id") != account.get("id")
                or value.get("identity_id") != current_identity.get("id")
                or value.get("platform") != platform or value.get("uid") != uid):
            value = {"eligible": False, "reason_code": "capture_plan_pending", "reason_label": "采集计划尚未更新"}
        elif value.get("reason_code") == "identity_unverified":
            value = {"eligible": False, "reason_code": "identity_evidence_missing", "reason_label": "缺少可验证的主页资料"}
        elif value.get("reason_code") == "pending_verification":
            value = {"eligible": False, "reason_code": "capture_plan_pending", "reason_label": "采集计划尚未更新"}
        elif value.get("reason_code") in {"account_paused", "account_status_unmarked", "account_status_invalid"}:
            value = {"eligible": False, "reason_code": "capture_plan_pending", "reason_label": "采集计划尚未按全量名单规则更新"}
        elif value.get("reason_code") == "platform_unsupported":
            value = {"eligible": False, "reason_code": "capture_plan_pending", "reason_label": "采集计划尚未接入此平台"}
        account["automatic_capture"] = {key: value[key] for key in ("eligible", "reason_code", "reason_label")}
