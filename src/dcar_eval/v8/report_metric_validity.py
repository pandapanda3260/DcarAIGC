"""Daily-only, versioned metric quality; historical reports retain v2 rules."""
from __future__ import annotations

from datetime import timedelta
from typing import Any, Mapping, Sequence

from .source_routing import LEGACY_POLICY_VERSION, METRIC_FIELDS, parse_time
from .metric_source_policy import CURRENT_METRIC_POLICY, current_policy_binding, field_capability

CONTRACT_VERSION = "report-metric-validity-v1"
SOURCE_POLICY_VERSION = "source-routing-operation-field-v3"
CURRENT_CONTRACT_VERSION = "report-metric-validity-v2"
CONTRACT_VERSIONS = frozenset({CONTRACT_VERSION, CURRENT_CONTRACT_VERSION})
SOURCE_POLICY_VERSIONS = frozenset({SOURCE_POLICY_VERSION, CURRENT_METRIC_POLICY})
EFFECTIVE_AT = "2026-09-12T00:00:00Z"
FIELD_LABELS = {
    "view_count": "播放量", "like_count": "点赞数", "comment_count": "评论数",
    "share_count": "分享数", "collect_count": "收藏数",
}
STATUS_LABELS = {
    "fresh": "有效", "stale": "旧数据，见采集时间", "missing": "未取到",
    "invalid": "无效", "not_applicable": "不适用", "unavailable": "当前数据源无法提供",
}


def scope_binding(task: Mapping[str, Any], *, cutoff_at: str, schema_version: int) -> dict[str, Any] | None:
    if schema_version >= 23:
        binding = current_policy_binding()
        return {"contract_version": CURRENT_CONTRACT_VERSION,
                "source_policy_version": binding["policy_version"],
                "source_policy_sha256": binding["policy_sha256"],
                "field_capabilities": binding["field_capabilities"],
                "window_hours": 36, "require_unexpired_fields": True,
                "eligible_basis": "all_window_contents_supported_fields"}
    if (schema_version not in {21, 22} or task.get("task_type") != "daily"
            or parse_time(cutoff_at) < parse_time(EFFECTIVE_AT)):
        return None
    return {
        "contract_version": CONTRACT_VERSION,
        "source_policy_version": SOURCE_POLICY_VERSION,
        "effective_at": EFFECTIVE_AT,
        "window_hours": 36,
        "require_unexpired_fields": True,
        "eligible_basis": "all_window_contents",
    }


def policy_for_scope(payload: Mapping[str, Any]) -> str:
    binding = payload.get("metric_validity")
    if binding is None:
        return LEGACY_POLICY_VERSION
    if isinstance(binding, Mapping) and binding.get("contract_version") == CURRENT_CONTRACT_VERSION:
        if binding != scope_binding({}, cutoff_at=str(payload["cutoff_at"]), schema_version=23):
            raise ValueError("frozen current metric contract changed")
        return CURRENT_METRIC_POLICY
    expected = scope_binding({"task_type": "daily"}, cutoff_at=str(payload["cutoff_at"]), schema_version=21)
    if expected is None or binding != expected:
        raise ValueError("unsupported frozen report metric validity contract")
    return SOURCE_POLICY_VERSION


def field_status(field: Mapping[str, Any], *, cutoff_at: str,
                 policy_version: str = SOURCE_POLICY_VERSION) -> str:
    """Classify one applicable field without changing its value or timestamps."""
    status = field.get("status")
    value = field.get("value")
    if status != "provided":
        return "invalid" if status == "invalid" else "missing"
    if type(value) is not int or value < 0:
        return "invalid"
    try:
        cutoff = parse_time(cutoff_at)
        captured = parse_time(str(field["captured_at"]))
        recorded = parse_time(str(field["recorded_at"]))
        basis = parse_time(str(field.get("provider_data_at") or field["captured_at"]))
        expires = parse_time(str(field["expires_at"]))
    except (KeyError, TypeError, ValueError):
        return "stale"
    fresh = (
        field.get("policy_version") == policy_version
        and field.get("effective_provider") == "tikhub"
        and field.get("is_latest_valid") is True
        and field.get("freshness") == "fresh"
        and cutoff - timedelta(hours=36) <= captured <= cutoff
        and recorded <= cutoff and basis <= cutoff < expires
    )
    return "fresh" if fresh else "stale"


def quality_detail(ids: Sequence[int], snapshots: Mapping[int, Mapping[str, Any]], *,
                   platforms: Mapping[int, str], cutoff_at: str,
                   minimum_percentage: float, policy_version: str = SOURCE_POLICY_VERSION) -> dict[str, Any]:
    """Keep all frozen contents in the denominator, including missing rows."""
    ids = list(dict.fromkeys(ids))
    if set(ids) - set(platforms):
        raise ValueError("metric validity requires every frozen content platform")
    fields = {name: {
        "label": FIELD_LABELS[name], "eligible_count": 0, "fresh_count": 0,
        "stale_count": 0, "missing_count": 0, "invalid_count": 0,
        "not_applicable_count": 0, **({"unavailable_count": 0} if policy_version == CURRENT_METRIC_POLICY else {}),
    } for name in METRIC_FIELDS}
    fresh_count = partial_count = 0
    for content_id in ids:
        observation = snapshots.get(content_id, {})
        selected = observation.get("fields", {})
        statuses = []
        for name in METRIC_FIELDS:
            counts = fields[name]
            capability = field_capability(platforms[content_id], name, policy_version=policy_version)
            if capability["status"] != "supported":
                counts[capability["status"] + "_count"] += 1
                continue
            counts["eligible_count"] += 1
            state = field_status(selected.get(name, {}), cutoff_at=cutoff_at, policy_version=policy_version)
            counts[state + "_count"] += 1
            statuses.append(state)
        if statuses and all(state == "fresh" for state in statuses):
            fresh_count += 1
        elif any(state == "fresh" for state in statuses):
            partial_count += 1
    for counts in fields.values():
        count = counts["eligible_count"]
        counts["percentage"] = round(counts["fresh_count"] * 100 / count, 2) if count else None
    count = len(ids)
    percentage = round(fresh_count * 100 / count, 2) if count else None
    status = ("not_applicable" if not count else
              "available" if percentage >= minimum_percentage else "below_threshold")
    reason = ("所选时间内没有发布内容" if not count else "" if status == "available" else
              f"全部适用指标有效且未过期的内容占 {percentage:.2f}%，"
              f"低于至少 {minimum_percentage:g}% 的要求；各项数据情况见下方明细")
    return {
        "status": status, "fresh_count": fresh_count,
        "as_of_snapshot_count": sum(content_id in snapshots for content_id in ids),
        "eligible_count": count, "percentage": percentage, "window_hours": 36,
        "eligible_basis": "all_window_contents_supported_fields" if policy_version == CURRENT_METRIC_POLICY else "all_window_contents", "reason": reason,
        "contract_version": CURRENT_CONTRACT_VERSION if policy_version == CURRENT_METRIC_POLICY else CONTRACT_VERSION,
        "source_policy_version": policy_version,
        **({"business_availability": {name: {"supported_count": item["eligible_count"],
            "unavailable_count": item["unavailable_count"], "not_applicable_count": item["not_applicable_count"],
            "complete": item["unavailable_count"] == 0 and item["fresh_count"] == item["eligible_count"]}
            for name, item in fields.items()}} if policy_version == CURRENT_METRIC_POLICY else {}),
        "partial_content_count": partial_count, "fields": fields,
    }


def display_sources(snapshot: Mapping[str, Any] | None, *, platform: str,
                    cutoff_at: str, policy_version: str = SOURCE_POLICY_VERSION) -> dict[str, Any]:
    selected = snapshot.get("fields", {}) if snapshot else {}
    result = {}
    for name in METRIC_FIELDS:
        field = dict(selected.get(name, {}))
        capability = field_capability(platform, name, policy_version=policy_version)
        state = (capability["status"] if capability["status"] != "supported"
                 else field_status(field, cutoff_at=cutoff_at, policy_version=policy_version))
        field.update(report_status=state, report_status_label=STATUS_LABELS[state])
        result[name] = field
    return result


def mark_historical_exposure(channels: dict[str, Any], rows: Sequence[Mapping[str, Any]]) -> None:
    """Carry old exposure evidence into every ratio using that channel total."""
    for platform, channel in channels.items():
        old_count = sum(
            row.get("platform") == platform and row.get("view_count") is not None
            and row.get("metric_sources", {}).get("view_count", {}).get("report_status") == "stale"
            for row in rows
        )
        if not old_count:
            continue
        for group in [channel["summary"], *channel.get("scenes", {}).values()]:
            for name in ("selling_point_exposure_share", "core_selling_point_exposure_share"):
                metric = group["metrics"][name]
                if metric["status"] in {"available", "sample_only"}:
                    metric["status"] = "stale"
                    metric["percentage"] = None
                metric["reason"] = (
                    f"本平台曝光数据包含 {old_count} 条旧数据，曝光占比不能当作最新结果；"
                    + str(metric.get("reason") or "")
                )
