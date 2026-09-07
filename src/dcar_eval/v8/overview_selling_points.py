"""Overview-only selling-point details, using its already selected formal facts.

No reads, writes, provider calls, or report-contract changes belong here. Counts
use the included primary point once per publication; exposure denominators are
the existing channel totals. Missing and historical metric fields stay explicit.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Mapping, Sequence

from .contracts import quantity_metric, ratio_metric
from .insights import EXPOSURE_SOURCE_GAPS


def _provided_view(row: Mapping[str, Any]) -> bool:
    value = row.get("view_count")
    return (
        row.get("view_count_status") == "provided"
        and isinstance(value, int)
        and not isinstance(value, bool)
        and value >= 0
    )


def build_overview_selling_points(
    rows: Sequence[Mapping[str, Any]],
    channels: Mapping[str, Mapping[str, Any]],
    *,
    minimum_view_coverage: float,
) -> dict[str, list[dict[str, Any]]]:
    """Appendable detail arrays; callers decide where to expose the new field."""
    result: dict[str, list[dict[str, Any]]] = {}
    for platform, channel in channels.items():
        channel_rows = [row for row in rows if row.get("platform") == platform]
        channel_total = int(channel["publication_count"])
        channel_exposure = channel["summary"]["metrics"]["selling_point_exposure_share"]
        denominator = int(channel_exposure["denominator"])
        stale_denominator = any(
            _provided_view(row)
            and row["view_count"] > 0
            and row.get("view_count_freshness") != "fresh"
            for row in channel_rows
        )
        actual_codes = {
            row["primary_selling_point_code"] for row in channel_rows
            if isinstance(row.get("primary_selling_point_code"), str)
            and row["primary_selling_point_code"].strip()
        }
        missing_codes = {}
        for tier in ("core", "other"):
            # Authorable codes are [A-Z][1-9][0-9]?; still avoid any malformed
            # historical code present in the input instead of merging it.
            bucket = f"__unclassified_{tier}__"
            while bucket in actual_codes:
                bucket += "_"
            missing_codes[tier] = bucket
        groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for row in channel_rows:
            if not row.get("selling_point_included"):
                continue
            code = row.get("primary_selling_point_code")
            if not isinstance(code, str) or not code.strip():
                tier = "core" if row.get("primary_tier") == "core" else "other"
                code = missing_codes[tier]
            groups[code].append(row)
        items: list[dict[str, Any]] = []
        for code in sorted(groups):
            selected = groups[code]
            total = len(selected)
            provided = [row for row in selected if _provided_view(row)]
            stale_count = sum(row.get("view_count_freshness") != "fresh" for row in provided)
            coverage: float | None = round(len(provided) * 100 / total, 2)
            observed_views = sum(int(row["view_count"]) for row in provided) if provided else None
            source_gap = EXPOSURE_SOURCE_GAPS.get(platform)
            if source_gap:
                view_status, view_reason = "not_applicable", source_gap
                observed_views, coverage = None, None
            elif not provided:
                view_status, view_reason = "missing", "该卖点内容暂未取得有效曝光数据。"
            elif len(provided) * 100 < minimum_view_coverage * total:
                view_status = "below_threshold"
                view_reason = (
                    f"该卖点有曝光数据 {len(provided)}/{total} 条，"
                    f"未达到至少 {minimum_view_coverage:g}% 的要求。"
                )
            elif stale_count:
                view_status = "stale"
                view_reason = f"该卖点有 {stale_count} 条曝光数据需要更新，累计值包含历史数据。"
            else:
                view_status = "available"
                view_reason = f"按该卖点 {len(provided)}/{total} 条有曝光数据的内容计算累计 VV。"

            # Inherit the channel's classification gate before considering row
            # completeness. A complete point cannot rescue an incomplete total.
            if source_gap:
                share_status, share_reason = "not_applicable", source_gap
            elif channel_exposure["status"] != "available":
                share_status = channel_exposure["status"]
                share_reason = channel_exposure["reason"]
            elif view_status != "available":
                share_status, share_reason = view_status, view_reason
            elif stale_denominator:
                share_status = "stale"
                share_reason = "渠道总曝光包含需要更新的历史数据，曝光占比暂不显示。"
            else:
                share_status = "available"
                share_reason = f"按渠道全部正值累计曝光 {denominator} 计算。"

            items.append({
                "code": code,
                **({"code_missing": True} if code in missing_codes.values() else {}),
                "label": "卖点编码缺失（待核对）" if code in missing_codes.values()
                else str(selected[0].get("primary_label") or code),
                "tier": "core" if selected[0].get("primary_tier") == "core" else "other",
                "publication_count": total,
                "count_share": ratio_metric(
                    total, channel_total, status="available",
                    reason=f"按该渠道在所选时间内发布的全部 {channel_total} 条内容计算。",
                ),
                "view_count": quantity_metric(
                    observed_views, unit="view", status=view_status,
                    coverage_percentage=coverage, reason=view_reason,
                ),
                "exposure_share": ratio_metric(
                    observed_views if channel_exposure["status"] == "available" and not source_gap else None,
                    denominator, status=share_status,
                    coverage_percentage=channel_exposure.get("coverage_percentage"),
                    reason=share_reason,
                ),
                "provided_view_items": len(provided) if not source_gap else 0,
                "missing_view_items": total - len(provided) if not source_gap else 0,
                "stale_view_items": stale_count if not source_gap else 0,
            })
        result[platform] = items
    return result
