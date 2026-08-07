"""Channel and business-scene conclusions for v8 overview windows."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .contracts import ratio_metric, score_metric


CHANNELS = (("douyin", "抖音"), ("xiaohongshu", "小红书"))
SCENES = (("used_car", "二手车"), ("new_car", "新车"), ("media", "媒体-AI小懂"))
METRIC_ORDER = (
    "selling_point_count_share",
    "core_selling_point_count_share",
    "selling_point_exposure_share",
    "core_selling_point_exposure_share",
    "content_verticality",
    "automotive_user_rate",
    "acquisition_potential",
)
EXPOSURE_CLASSIFIABLE_COVERAGE_MIN = 90.0
#: 平台侧结构性缺口：TikHub 当前接口对这些平台不返回阅读/播放数
#: （2026-08-07 核查：xhs statistics 端点 view_count 恒为 0、pre_view_count=-1）。
#: 命中时曝光两指标按 not_applicable 处理并说明原因，而不是报数据缺陷。
EXPOSURE_SOURCE_GAPS = {
    "xiaohongshu": "小红书接口当前未提供阅读数，曝光指标不适用",
}


def _automotive_user_rate_placeholder(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Fallback rate when no user-level aggregation was supplied.

    Never fabricates a percentage: an unwired slice reports ``not_applicable``
    (empty) or ``not_calculable`` (has content but no computed user union).
    """

    total = len(rows)
    status = "not_applicable" if total == 0 else "not_calculable"
    reason = (
        "该切片在所选窗口没有发布内容"
        if total == 0
        else "用户级汽车兴趣占比尚未接入用户聚合，暂不发布"
    )
    return ratio_metric(
        None,
        0,
        status=status,
        eligible_count=0,
        coverage_percentage=None,
        reason=reason,
    )


def _score_metric(rows: List[Dict[str, Any]], field: str) -> Dict[str, Any]:
    total = len(rows)
    values = [int(row[field]) for row in rows if row.get(field) is not None]
    if total == 0:
        status, reason = "not_applicable", "该场景在所选窗口没有发布内容"
    elif not values:
        status, reason = "missing", "没有满足评分证据门槛的内容"
    elif len(values) == total:
        status, reason = "available", ""
    else:
        status, reason = "sample_only", f"仅 {len(values)}/{total} 条满足评分证据门槛"
    value = round(sum(values) / len(values)) if values else None
    return score_metric(
        value,
        status=status,
        scorable_items=len(values),
        total_items=total,
        reason=reason,
    )


def _metrics(
    rows: List[Dict[str, Any]],
    *,
    platform: str = "",
    channel_total: int,
    channel_label: str,
    total_exposure: int,
    valid_exposure_items: int,
    classifiable_exposure: int,
    classifiable_exposure_items: int,
    exposure_calculable: bool,
    exposure_coverage: Optional[float],
    automotive_user_rate: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    eligible = sum(row.get("evidence_level") in {"V2", "V3"} for row in rows)
    selling = [row for row in rows if row.get("selling_point_included")]
    core = [row for row in selling if row.get("primary_tier") == "core"]
    count_status = "available" if channel_total else "not_applicable"
    count_reason = (
        f"分母为{channel_label}渠道窗口内全部发布 {channel_total} 条"
        if channel_total else "该渠道在所选窗口没有发布内容"
    )

    def count_metric(selected: List[Dict[str, Any]]) -> Dict[str, Any]:
        return ratio_metric(
            len(selected) if channel_total else None,
            channel_total,
            status=count_status,
            eligible_count=eligible,
            coverage_percentage=(
                round(eligible * 100 / len(rows), 2) if rows else None
            ),
            reason=count_reason,
        )

    if not channel_total:
        exposure_status = "not_applicable"
        exposure_reason = "该渠道在所选窗口没有发布内容"
    elif total_exposure <= 0 and platform in EXPOSURE_SOURCE_GAPS:
        exposure_status = "not_applicable"
        exposure_reason = EXPOSURE_SOURCE_GAPS[platform]
    elif total_exposure <= 0:
        exposure_status = "missing"
        exposure_reason = "该渠道窗口没有 view_count > 0 的有效曝光数据"
    elif exposure_calculable:
        exposure_status = "available"
        exposure_reason = (
            f"分母为{channel_label}渠道 {valid_exposure_items} 条有效曝光内容的"
            f"总曝光 {total_exposure}；可归类曝光覆盖 {exposure_coverage}%"
        )
    else:
        exposure_status = "below_threshold"
        exposure_reason = (
            f"可归类有效曝光 {classifiable_exposure}/{total_exposure}，覆盖 "
            f"{exposure_coverage or 0}%：低于 "
            f"{EXPOSURE_CLASSIFIABLE_COVERAGE_MIN:g}% 发布门槛"
        )

    def exposure_metric(selected: List[Dict[str, Any]]) -> Dict[str, Any]:
        numerator = (
            sum(
                int(row["view_count"])
                for row in selected
                if int(row.get("view_count") or 0) > 0
            )
            if exposure_calculable else None
        )
        return ratio_metric(
            numerator,
            total_exposure,
            status=exposure_status,
            eligible_count=classifiable_exposure_items,
            coverage_percentage=exposure_coverage,
            reason=exposure_reason,
        )

    return {
        "selling_point_count_share": count_metric(selling),
        "core_selling_point_count_share": count_metric(core),
        "selling_point_exposure_share": exposure_metric(selling),
        "core_selling_point_exposure_share": exposure_metric(core),
        "content_verticality": _score_metric(rows, "content_automotive_score"),
        "automotive_user_rate": (
            automotive_user_rate
            if automotive_user_rate is not None
            else _automotive_user_rate_placeholder(rows)
        ),
        "acquisition_potential": _score_metric(rows, "acquisition_potential_score"),
    }


def _channel(
    rows: List[Dict[str, Any]],
    platform: str,
    label: str,
    *,
    audience: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    channel_rows = [row for row in rows if row["platform"] == platform]
    audience = audience or {}
    summary_audience = audience.get("summary") or {}
    scene_audience = audience.get("scenes") or {}
    total = len(channel_rows)
    identifiable = sum(
        row.get("evidence_level") in {"V2", "V3"} for row in channel_rows
    )
    valid_exposure = [
        row for row in channel_rows if int(row.get("view_count") or 0) > 0
    ]
    cross_covered = [
        row
        for row in valid_exposure
        if row.get("evidence_level") in {"V2", "V3"}
    ]
    total_exposure = sum(int(row["view_count"]) for row in valid_exposure)
    classifiable_exposure = sum(
        int(row["view_count"]) for row in cross_covered
    )
    exposure_coverage = (
        round(classifiable_exposure * 100 / total_exposure, 2)
        if total_exposure > 0
        else None
    )
    exposure_calculable = bool(
        total
        and total_exposure > 0
        and (exposure_coverage or 0) >= EXPOSURE_CLASSIFIABLE_COVERAGE_MIN
    )
    scenes: Dict[str, Any] = {}
    for scene, scene_label in SCENES:
        selected = [
            row for row in channel_rows if row["content_direction"] == scene
        ]
        scene_rate = scene_audience.get(scene) or {}
        scenes[scene] = {
            "label": scene_label,
            "publication_count": len(selected),
            "audience_quality": scene_rate.get("audience_quality"),
            "metrics": _metrics(
                selected,
                platform=platform,
                channel_total=total,
                channel_label=label,
                total_exposure=total_exposure,
                valid_exposure_items=len(valid_exposure),
                classifiable_exposure=classifiable_exposure,
                classifiable_exposure_items=len(cross_covered),
                exposure_calculable=exposure_calculable,
                exposure_coverage=exposure_coverage,
                automotive_user_rate=scene_rate.get("metric"),
            ),
        }
    return {
        "platform": platform,
        "label": label,
        "publication_count": total,
        "evidence_coverage_percentage": (
            round(identifiable * 100 / total, 2) if total else None
        ),
        "valid_exposure_items": len(valid_exposure),
        "exposure_coverage_percentage": exposure_coverage,
        "summary": {
            "label": "汇总",
            "publication_count": total,
            "audience_quality": summary_audience.get("audience_quality"),
            "metrics": _metrics(
                channel_rows,
                platform=platform,
                channel_total=total,
                channel_label=label,
                total_exposure=total_exposure,
                valid_exposure_items=len(valid_exposure),
                classifiable_exposure=classifiable_exposure,
                classifiable_exposure_items=len(cross_covered),
                exposure_calculable=exposure_calculable,
                exposure_coverage=exposure_coverage,
                automotive_user_rate=summary_audience.get("metric"),
            ),
        },
        "scenes": scenes,
    }


def build_channel_conclusions(
    rows: List[Dict[str, Any]],
    *,
    audience_rates: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Return the fixed channel -> summary/scenes -> seven-metric contract.

    ``audience_rates`` (from :mod:`audience_rate`) supplies the user-level
    ``automotive_user_rate`` and ``audience_quality`` per slice. When omitted,
    the rate degrades to a non-fabricating placeholder so callers that have not
    yet wired the user aggregation still emit a valid contract shape.
    """

    audience_rates = audience_rates or {}
    return {
        platform: _channel(rows, platform, label, audience=audience_rates.get(platform))
        for platform, label in CHANNELS
    }
