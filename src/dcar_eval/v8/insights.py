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
    "audience_verticality",
    "acquisition_potential",
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
    channel_total: int,
    channel_label: str,
    total_exposure: int,
    exposure_calculable: bool,
    exposure_coverage: Optional[float],
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
    elif exposure_calculable:
        exposure_status = "available"
        exposure_reason = f"分母为{channel_label}渠道有效总曝光 {total_exposure}"
    else:
        exposure_status = "below_threshold"
        exposure_reason = (
            f"标签×有效曝光交叉覆盖 {exposure_coverage or 0}%：低于 90% 发布门槛"
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
            eligible_count=sum(
                int(row.get("view_count") or 0) > 0 for row in selected
            ),
            coverage_percentage=exposure_coverage,
            reason=exposure_reason,
        )

    return {
        "selling_point_count_share": count_metric(selling),
        "core_selling_point_count_share": count_metric(core),
        "selling_point_exposure_share": exposure_metric(selling),
        "core_selling_point_exposure_share": exposure_metric(core),
        "content_verticality": _score_metric(rows, "content_automotive_score"),
        "audience_verticality": _score_metric(rows, "audience_automotive_score"),
        "acquisition_potential": _score_metric(rows, "acquisition_potential_score"),
    }


def _channel(rows: List[Dict[str, Any]], platform: str, label: str) -> Dict[str, Any]:
    channel_rows = [row for row in rows if row["platform"] == platform]
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
    exposure_coverage = (
        round(len(cross_covered) * 100 / total, 2) if total else None
    )
    total_exposure = sum(int(row["view_count"]) for row in valid_exposure)
    exposure_calculable = bool(
        total and total_exposure > 0 and (exposure_coverage or 0) >= 90
    )
    scenes: Dict[str, Any] = {}
    for scene, scene_label in SCENES:
        selected = [
            row for row in channel_rows if row["content_direction"] == scene
        ]
        scenes[scene] = {
            "label": scene_label,
            "publication_count": len(selected),
            "metrics": _metrics(
                selected,
                channel_total=total,
                channel_label=label,
                total_exposure=total_exposure,
                exposure_calculable=exposure_calculable,
                exposure_coverage=exposure_coverage,
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
            "metrics": _metrics(
                channel_rows,
                channel_total=total,
                channel_label=label,
                total_exposure=total_exposure,
                exposure_calculable=exposure_calculable,
                exposure_coverage=exposure_coverage,
            ),
        },
        "scenes": scenes,
    }


def build_channel_conclusions(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Return the fixed channel -> summary/scenes -> seven-metric contract."""

    return {
        platform: _channel(rows, platform, label)
        for platform, label in CHANNELS
    }
