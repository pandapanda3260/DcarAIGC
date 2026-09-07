"""Fixed, field-aware TikHub supplementation after Matrix observations.

This module never probes alternative endpoints. A known detail response is used
for counters that the statistics endpoint does not expose, without rewriting
text/media or rerunning analysis. Each route has a separate cycle-scoped slot.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Collection, Mapping
from zoneinfo import ZoneInfo

from . import providers
from .account_metrics import parse_tikhub_profile, persist_account_metric_observation, select_account_metrics
from .account_roster import RosterError, require_active_member
from .capture import (
    CaptureError,
    ProviderResult,
    SlotUnavailable,
    execute_account_fetch,
    execute_content_fetch,
    load_succeeded_raw_response,
)
from .provider_budget import DEFAULT_TASK_MAX_AMOUNT_USD, paid_scope
from .source_routing import METRIC_FIELDS, load_policy, metric_cycle_key, parse_time, select_content_metrics
from .storage import DEFAULT_DB, connect, now_utc, transaction

BEIJING = ZoneInfo("Asia/Shanghai")


def missing_metric_fields(connection, content_id: int, *, at: str) -> list[str]:
    content = connection.execute("SELECT platform FROM content_items WHERE id=?", (content_id,)).fetchone()
    if content is None:
        raise providers.ProviderConfigurationError("content does not exist")
    selected = select_content_metrics(connection, [content_id], cutoff_at=at).get(content_id, {})
    fields = selected.get("fields", {})
    return [name for name in METRIC_FIELDS
            if not (name == "view_count" and content["platform"] == "xiaohongshu")
            and not (fields.get(name, {}).get("status") == "provided"
                     and fields.get(name, {}).get("freshness") == "fresh")]


def refresh_content_metrics(
    content_id: int, *, db_path: Path = DEFAULT_DB, at: str | None = None,
    cycle_key: str | None = None, task_id: str | None = None,
    task_max_amount: float | None = None,
    call_override: Callable[[str, Mapping[str, Any]], ProviderResult] | None = None,
    allowed_groups: Collection[str] | None = None,
) -> dict[str, Any]:
    """Reuse all fresh fields; buy at most one success per fixed route/cycle."""
    timestamp = at or now_utc()
    if task_id is None and task_max_amount is None:
        task_id = "matrix-metrics:" + parse_time(timestamp).astimezone(BEIJING).date().isoformat()
        task_max_amount = DEFAULT_TASK_MAX_AMOUNT_USD
    with connect(db_path) as connection:
        row = connection.execute("SELECT * FROM content_items WHERE id=?", (content_id,)).fetchone()
        if row is None:
            raise providers.ProviderConfigurationError("content does not exist")
        content = dict(row)
        missing = missing_metric_fields(connection, content_id, at=timestamp)
    platform = content["platform"]
    if platform not in {"douyin", "xiaohongshu"}:
        raise providers.ProviderConfigurationError("unsupported metrics platform")
    cycle = cycle_key or metric_cycle_key(content_id, content["published_at"], as_of=timestamp)
    groups = load_policy()["metric_supplement_groups"][platform]
    policy_groups = {str(rule["name"]) for rule in groups}
    if allowed_groups is not None:
        if isinstance(allowed_groups, (str, bytes)):
            raise providers.ProviderConfigurationError(
                "allowed metric groups must be a collection of policy group names"
            )
        invalid_groups = {
            group
            for group in allowed_groups
            if not isinstance(group, str) or group not in policy_groups
        }
        if invalid_groups:
            invalid = ",".join(sorted(str(group) for group in invalid_groups))
            raise providers.ProviderConfigurationError(
                f"unknown metric supplement group: {invalid}"
            )
        allowed = set(allowed_groups)
    else:
        allowed = None
    results: list[dict[str, Any]] = []
    deferred_groups: list[str] = []
    for rule in groups:
        if not missing:
            break
        if not set(missing) & set(rule["fields"]):
            continue
        group, source_stage = rule["name"], rule["stage"]
        if allowed is not None and group not in allowed:
            deferred_groups.append(str(group))
            continue
        provider, adapter, operation, price = providers.STAGE_CONFIG[(platform, source_stage)]
        window = f"{cycle}:{group}"

        def parse(raw: Any, http_status: int) -> ProviderResult:
            parsed = (providers._parse_douyin_stage_payload(source_stage, content["platform_content_id"], raw, status=http_status)
                      if platform == "douyin" else providers._parse_xhs_stage_payload("metrics", content["platform_content_id"], content["content_type"], raw, status=http_status))
            values = parsed.data.get("metrics") if source_stage == "detail" else parsed.data
            if not isinstance(values, Mapping):
                raise providers.ProviderConfigurationError("fixed counter route omitted metrics")
            return ProviderResult(dict(values), raw, http_status, parsed.billed)

        def call() -> ProviderResult:
            if call_override is not None:
                return call_override(group, content)
            key = providers._load_key(providers.TIKHUB_KEY_FILE, "TIKHUB_API_KEY")
            result = (providers._douyin_call(source_stage, content["platform_content_id"], key)
                      if platform == "douyin" else providers._xhs_call("metrics", content["platform_content_id"], key, content["content_type"]))
            if source_stage == "detail" and result.data.get("account_uid") and content.get("raw_account_uid"):
                if str(result.data["account_uid"]) != str(content["raw_account_uid"]):
                    raise CaptureError(
                        "counter detail author conflicts with stored identity",
                        retryable=False,
                        error_code="identity_conflict",
                        http_status=result.http_status,
                        billed=result.billed,
                        raw_response=result.raw_response,
                        entity_bytes=result.entity_bytes,
                        transport_receipt=result.transport_receipt,
                    )
            values = (
                result.data.get("metrics") if source_stage == "detail" else result.data
            )
            if not isinstance(values, Mapping):
                raise CaptureError(
                    "fixed counter route omitted metrics",
                    retryable=True,
                    error_code="invalid_response",
                    http_status=result.http_status,
                    billed=result.billed,
                    raw_response=result.raw_response,
                    entity_bytes=result.entity_bytes,
                    transport_receipt=result.transport_receipt,
                )
            return ProviderResult(
                dict(values),
                result.raw_response,
                result.http_status,
                result.billed,
                entity_bytes=result.entity_bytes,
                transport_receipt=result.transport_receipt,
            )

        replayed = False
        try:
            # A successful request is authoritative for idempotency even if
            # some fields stayed missing. No second purchase in the same cycle.
            stored = load_succeeded_raw_response(content_id=content_id, stage="metrics", window_key=window,
                                                operation=operation, db_path=db_path)
            parsed = parse(stored.value, stored.http_status or 200)
            outcome = providers.CaptureOutcome(stored.slot_id, 0, stored.raw_response_id, parsed.data, False, 0.0, "USD")
            replayed = True
        except SlotUnavailable:
            budget_id = providers._budget_for_call(provider=provider, operation=operation, price=price,
                                                  task_id=task_id, task_max_amount=task_max_amount, db_path=db_path)
            # A surrounding history scope is retained by paid_scope.
            with paid_scope("metrics"):
                platform_content_id = str(content["platform_content_id"])
                if platform == "douyin":
                    _, request_params = providers._douyin_request(
                        source_stage, platform_content_id
                    )
                else:
                    _, request_params = providers._xhs_request(
                        "metrics",
                        platform_content_id,
                        str(content["content_type"]),
                    )
                outcome = execute_content_fetch(
                    request_transport=providers._freeze_tikhub_transport(call_override),
                    content_id=content_id,
                    stage="metrics",
                    window_key=window,
                    provider=provider,
                    adapter_version=adapter + "+matrix-first-v1",
                    operation=operation,
                    call=call,
                    db_path=db_path,
                    budget_id=budget_id,
                    task_id=task_id,
                    task_max_amount=task_max_amount,
                    paid_request_identity=providers._paid_request_identity(
                        operation=operation,
                        platform=platform,
                        subject=platform_content_id,
                        params=request_params,
                        cursor=None,
                        due_bucket=window,
                    ),
                )
        providers._store_stage_result(
            content, "metrics", window, outcome, db_path=db_path
        )
        results.append(
            {
                "group": group,
                "status": "replayed" if replayed else "succeeded",
                "raw_response_id": outcome.raw_response_id,
                "amount": outcome.amount,
            }
        )
        with connect(db_path) as connection:
            # Read at actual application time: caller's frozen schedule is not
            # permission to backdate a newly returned observation.
            missing = missing_metric_fields(connection, content_id, at=now_utc())
    return {"content_id": content_id, "cycle_key": cycle,
            "status": "partial" if missing else "succeeded", "missing_fields": missing,
            "request_cycle_complete": not deferred_groups,
            "deferred_groups": deferred_groups,
            "requests": results, "provider_cost": round(sum(item["amount"] for item in results), 6)}


def refresh_account_profile(
    identity_id: int, *, db_path: Path = DEFAULT_DB, at: str | None = None,
    task_id: str | None = None, task_max_amount: float | None = None,
    call_override: Callable[[str, Mapping[str, Any]], ProviderResult] | None = None,
) -> dict[str, Any]:
    timestamp = at or now_utc()
    if task_id is None and task_max_amount is None:
        task_id = "matrix-profile:" + parse_time(timestamp).astimezone(BEIJING).date().isoformat()
        task_max_amount = DEFAULT_TASK_MAX_AMOUNT_USD
    with connect(db_path) as connection:
        activation = None
        if "source_family" in {
            column["name"]
            for column in connection.execute(
                "PRAGMA table_info(account_roster_snapshots)"
            )
        }:
            from .profile_activations import activation_at

            activation = activation_at(connection, timestamp)
            if activation is None:
                raise RosterError(
                    "roster_activation_required",
                    "Account profile refresh requires an effective activation",
                )
        member = require_active_member(
            connection, identity_id, activation=activation
        )
        current = select_account_metrics(connection, [identity_id], cutoff_at=timestamp)[identity_id]
    if member["platform"] != "douyin":
        return {"identity_id": identity_id, "status": "skipped", "reason": "profile_contract_not_enabled", "provider_cost": 0.0}
    if current["metric_fields"]["follower_count"].get("freshness") == "fresh":
        return {"identity_id": identity_id, "status": "succeeded", "reason": "fresh_primary_or_cached", "provider_cost": 0.0}
    operation = "douyin_uid_profile"
    window = "matrix-first:profile:" + parse_time(timestamp).astimezone(BEIJING).date().isoformat()

    def call() -> ProviderResult:
        if call_override is not None:
            return call_override("profile", member)
        response = providers._request_json(
            providers._tikhub_url("/api/v1/douyin/web/fetch_user_profile_by_uid"),
            headers={
                "Authorization": "Bearer "
                + providers._load_key(providers.TIKHUB_KEY_FILE, "TIKHUB_API_KEY")
            },
            params={"uid": member["uid"]},
            provider="TikHub",
        )
        status, raw = response
        try:
            normalized = parse_tikhub_profile(
                raw,
                platform="douyin",
                uid=member["uid"],
                http_status=status,
            )
        except Exception as error:
            raise CaptureError(
                f"TikHub profile response failed validation: {error}",
                retryable=False,
                error_code=str(getattr(error, "code", "invalid_response")),
                http_status=status,
                billed=True,
                raw_response=raw,
                entity_bytes=(
                    response.entity_body if hasattr(response, "entity_body") else None
                ),
                transport_receipt=(
                    response.receipt if hasattr(response, "receipt") else None
                ),
            ) from error
        return providers._with_transport(
            response,
            ProviderResult(
                normalized,
                raw,
                status,
                True,
            ),
        )

    try:
        stored = load_succeeded_raw_response(account_id=member["account_id"], stage="discovery", window_key=window,
                                            operation=operation, db_path=db_path)
        normalized = parse_tikhub_profile(stored.value, platform="douyin", uid=member["uid"], http_status=stored.http_status or 200)
        raw_id, captured, cost = stored.raw_response_id, stored.captured_at, 0.0
    except SlotUnavailable:
        budget_id = providers._budget_for_call(
            provider="TikHub",
            operation=operation,
            price=providers.TIKHUB_PRICE,
            task_id=task_id,
            task_max_amount=task_max_amount,
            db_path=db_path,
        )
        with paid_scope(
            "metrics",
            roster_snapshot_id=member["roster_snapshot_id"],
            roster_snapshot_hash=member["roster_snapshot_hash"],
        ):
            result = execute_account_fetch(
                request_transport=providers._freeze_tikhub_transport(call_override),
                account_id=member["account_id"],
                stage="discovery",
                window_key=window,
                provider="TikHub",
                adapter_version="tikhub-profile-matrix-first-v1",
                operation=operation,
                call=call,
                db_path=db_path,
                budget_id=budget_id,
                task_id=task_id,
                task_max_amount=task_max_amount,
                paid_request_identity=providers._paid_request_identity(
                    operation=operation,
                    platform="douyin",
                    subject=str(member["uid"]),
                    params={"uid": str(member["uid"])},
                    cursor=None,
                    due_bucket=window,
                ),
            )
        normalized, raw_id, cost = result.data, result.raw_response_id, result.amount
        captured = providers._raw_response_captured_at(raw_id, db_path=db_path)
    with connect(db_path) as connection, transaction(connection):
        from .provider_budget import assert_paid_scope_owner
        assert_paid_scope_owner(connection)
        fact = persist_account_metric_observation(connection, account_identity_id=identity_id, provider="tikhub",
                                                 raw_response_id=raw_id, normalized=normalized, captured_at=captured)
    available = normalized["field_status"]["follower_count"].get("status") == "provided"
    return {"identity_id": identity_id, "status": "succeeded" if available else "partial",
            "request_cycle_complete": True, "observation_id": fact["id"], "provider_cost": cost}
