#!/usr/bin/env python3
"""Build the channel -> summary -> three-scene conclusion structure.

This script only restructures conclusions that are supported by the existing
full-media evidence.  It does not turn sample diagnostics into channel-wide
results and does not infer audience or acquisition scores when comments are
missing.
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path
from statistics import mean
from typing import Any

import rebuild_channel_evaluation_v4 as v4
from three_proposition_scoring import acquisition_conclusion, audience_conclusion
from project_paths import ARCHIVE_REPORTS_DIR


RUN_DATE = "2026-08-02"
OUT_JSON = ARCHIVE_REPORTS_DIR / f"双渠道结构化结论_v5_{RUN_DATE}.json"
OUT_REPORT = ARCHIVE_REPORTS_DIR / f"双渠道结构化结论报告_v5_{RUN_DATE}.md"


def metric(
    *,
    value: float | int | None,
    display: str,
    status: str = "available",
    qualitative: str = "",
    scope: str = "",
    reason: str = "",
) -> dict[str, Any]:
    return {
        "value": value,
        "display": display,
        "status": status,
        "qualitative": qualitative,
        "scope": scope,
        "reason": reason,
    }


def unavailable(reason: str, scope: str = "") -> dict[str, Any]:
    return metric(
        value=None,
        display="—",
        status="not_computable",
        qualitative="暂不可计算",
        scope=scope,
        reason=reason,
    )


def pct(n: int | float, d: int | float) -> float:
    return round(float(n) * 100 / float(d), 2) if d else 0.0


def score_display(score: int | float, suffix: str = "/100") -> str:
    shown = round(float(score), 1)
    number = str(int(shown)) if shown.is_integer() else str(shown)
    return f"{number}{suffix}"


def douyin_section(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    included = [row for row in rows if row.get("included")]
    core = [row for row in included if row.get("primary_tier") == "core"]
    auto_scores = [row["content_auto_score"] for row in rows if row.get("content_auto_score") is not None]
    auto_score = round(mean(auto_scores))
    auto_pass = sum(score >= 70 for score in auto_scores)

    summary = {
        "selling_point_count_share": metric(
            value=pct(len(included), total),
            display=f"{len(included)}/{total} · {pct(len(included), total)}%",
            qualitative="多数发布表达正式卖点",
            scope="全部438条抖音发布",
        ),
        "core_selling_point_count_share": metric(
            value=pct(len(core), total),
            display=f"{len(core)}/{total} · {pct(len(core), total)}%",
            qualitative="低于60%-70%目标",
            scope="全部438条抖音发布",
        ),
        "selling_point_exposure_share": metric(
            value=None,
            display=f"0/{total}条有效播放 · 0.0%数据覆盖",
            status="not_computable",
            qualitative="暂不可计算",
            scope="全部438条抖音发布",
            reason="438条play_count均为占位值0，不能用点赞或评论替代播放量",
        ),
        "core_selling_point_exposure_share": metric(
            value=None,
            display=f"0/{len(core)}条有效播放 · 0.0%数据覆盖",
            status="not_computable",
            qualitative="暂不可计算",
            scope="全部438条抖音发布",
            reason="438条play_count均为占位值0，无法切分核心卖点曝光",
        ),
        "content_verticality": metric(
            value=auto_score,
            display=f"{score_display(auto_score)} · {auto_pass}/{total} · {pct(auto_pass, total)}%",
            qualitative=v4.auto_qualitative(auto_score),
            scope=f"{len(auto_scores)}/{total}条具备V2/V3完整媒体证据",
        ),
        "audience_verticality": metric(
            value=None,
            display=f"0/{total}条可评估 · 0.0%数据覆盖",
            status="not_computable",
            qualitative="暂不可计算",
            scope="抖音渠道",
            reason="未采集评论文本和有效独立评论用户",
        ),
        "acquisition_effect_estimate": metric(
            value=None,
            display=f"0/{total}条可计算 · 0.0%数据覆盖",
            status="not_computable",
            qualitative="暂不可计算",
            scope="抖音渠道",
            reason="统一拉新预估模型需要内容、受众、懂车帝承接和行动意图；当前缺少评论受众证据",
        ),
    }

    scenes: dict[str, Any] = {}
    for scene in v4.SCENES:
        scene_rows = [row for row in included if row.get("business_scene") == scene]
        scene_core = [row for row in scene_rows if row.get("primary_tier") == "core"]
        scores = [row["content_auto_score"] for row in scene_rows if row.get("content_auto_score") is not None]
        scene_auto = round(mean(scores)) if scores else None
        scene_auto_pass = sum(score >= 70 for score in scores)
        scenes[scene] = {
            "publication_n": len(scene_rows),
            "denominator": total,
            "selling_point_count_share": metric(
                value=pct(len(scene_rows), total),
                display=f"{len(scene_rows)}/{total} · {pct(len(scene_rows), total)}%",
                qualitative="占全部抖音发布",
                scope="该场景正式卖点命中条数 / 全部438条发布",
            ),
            "core_selling_point_count_share": metric(
                value=pct(len(scene_core), total),
                display=f"{len(scene_core)}/{total} · {pct(len(scene_core), total)}%",
                qualitative="占全部抖音发布",
                scope="该场景核心卖点条数 / 全部438条发布",
            ),
            "selling_point_exposure_share": metric(
                value=None,
                display=f"0/{len(scene_rows)}条有效曝光 · 0.0%数据覆盖",
                status="not_computable",
                qualitative="暂不可计算",
                scope=scene,
                reason="play_count均为占位值0",
            ),
            "core_selling_point_exposure_share": metric(
                value=None,
                display=f"0/{len(scene_core)}条有效曝光 · 0.0%数据覆盖",
                status="not_computable",
                qualitative="暂不可计算",
                scope=scene,
                reason="play_count均为占位值0",
            ),
            "content_verticality": metric(
                value=scene_auto,
                display=(
                    f"{score_display(scene_auto)} · {scene_auto_pass}/{len(scene_rows)} · {pct(scene_auto_pass, len(scene_rows))}%"
                    if scene_auto is not None else "0/0条 · 0.0%样本覆盖"
                ),
                qualitative=v4.auto_qualitative(scene_auto),
                scope=f"仅该场景{len(scene_rows)}条卖点命中内容，不代表所有未命中内容",
            ),
            "audience_verticality": metric(
                value=None,
                display=f"0/{len(scene_rows)}条可评估 · 0.0%数据覆盖",
                status="not_computable",
                qualitative="暂不可计算",
                scope=scene,
                reason="未采集评论文本和有效独立评论用户",
            ),
            "acquisition_effect_estimate": metric(
                value=None,
                display=f"0/{len(scene_rows)}条可计算 · 0.0%数据覆盖",
                status="not_computable",
                qualitative="暂不可计算",
                scope=scene,
                reason="缺少评论受众证据，不能按统一模型计算",
            ),
        }
    return {
        "scope": "仅抖音内容链接；30个随机账号样本中的438条发布",
        "denominator": total,
        "summary": summary,
        "scenes": scenes,
    }


def xhs_section(sample_rows: list[dict[str, Any]], diagnostics: dict[str, Any]) -> dict[str, Any]:
    total = diagnostics["total_unique_publication_links"]
    sample_n = len(sample_rows)
    covered = [row for row in sample_rows if row.get("selling_point_id")]
    core = [row for row in covered if row.get("tier") == "core"]
    known_view = [row for row in sample_rows if row.get("vv") is not None]
    total_view = sum(row.get("vv") or 0 for row in known_view)
    covered_view = sum(row.get("vv") or 0 for row in covered)
    core_view = sum(row.get("vv") or 0 for row in core)
    audience_scores = [row["audience_auto_score"] for row in sample_rows if row.get("audience_auto_score") is not None]
    potential_scores = [row["predicted_acquisition_potential_score"] for row in sample_rows if row.get("predicted_acquisition_potential_score") is not None]
    audience_score = round(mean(audience_scores)) if audience_scores else None
    potential_score = round(mean(potential_scores)) if potential_scores else None

    summary = {
        "selling_point_count_share": metric(
            value=None,
            display=f"渠道不可上卷；样本{len(covered)}/{sample_n} · {pct(len(covered), sample_n)}%",
            status="sample_only",
            qualitative="仅10条全媒体样本诊断",
            scope=f"完整卖点标注仅{sample_n}/{total}条",
            reason="覆盖率2.96%，不能代表338条全部发布",
        ),
        "core_selling_point_count_share": metric(
            value=None,
            display=f"渠道不可上卷；样本{len(core)}/{sample_n} · {pct(len(core), sample_n)}%",
            status="sample_only",
            qualitative="样本未命中核心卖点",
            scope=f"完整卖点标注仅{sample_n}/{total}条",
            reason="覆盖率2.96%，不能代表338条全部发布",
        ),
        "selling_point_exposure_share": metric(
            value=None,
            display=f"{covered_view}/{total_view}次 · {pct(covered_view, total_view)}%",
            status="sample_only",
            qualitative="渠道不可上卷，仅5条标签×浏览量交叉样本",
            scope=f"{covered_view}/{total_view}次样本浏览",
            reason="同时具备完整标签和浏览量的内容仅5/338条",
        ),
        "core_selling_point_exposure_share": metric(
            value=None,
            display=f"{core_view}/{total_view}次 · {pct(core_view, total_view)}%",
            status="sample_only",
            qualitative="渠道不可上卷，交叉样本核心曝光为0",
            scope=f"{core_view}/{total_view}次样本浏览",
            reason="同时具备完整标签和浏览量的内容仅5/338条",
        ),
        "content_verticality": metric(
            value=diagnostics["content_automotive"]["score"],
            display=(
                f"{score_display(diagnostics['content_automotive']['score'])} · "
                f"{sum(row['content_auto_score'] >= 70 for row in sample_rows)}/{sample_n} · "
                f"{pct(sum(row['content_auto_score'] >= 70 for row in sample_rows), sample_n)}%样本达标"
            ),
            status="directional_estimate",
            qualitative="方向性后分层估计",
            scope="基础随机样本按313:25来源占比后分层加权",
            reason="不是338条逐条全媒体结果",
        ),
        "audience_verticality": metric(
            value=audience_score,
            display=f"{score_display(audience_score)} · {audience_score}%",
            status="sample_only",
            qualitative=audience_conclusion(audience_score) or "",
            scope=f"评论达标的{sample_n}条5+5平衡样本",
            reason="存在评论量替补选择偏差，不能上卷为渠道结果",
        ),
        "acquisition_effect_estimate": metric(
            value=potential_score,
            display=f"{score_display(potential_score)} · {potential_score}%",
            status="sample_only",
            qualitative=acquisition_conclusion(potential_score) or "",
            scope=f"评论达标的{sample_n}条5+5平衡样本",
            reason="是拉新潜力预测，不是实际新增效果",
        ),
    }

    scenes: dict[str, Any] = {}
    for scene in v4.SCENES:
        scene_rows = [row for row in covered if row.get("business_scene") == scene]
        scene_core = [row for row in scene_rows if row.get("tier") == "core"]
        scene_known_view = [row for row in scene_rows if row.get("vv") is not None]
        scene_view = sum(row.get("vv") or 0 for row in scene_known_view)
        scene_core_view = sum(row.get("vv") or 0 for row in scene_core if row.get("vv") is not None)
        if not scene_rows:
            scenes[scene] = {
                "sample_n": 0,
                "selling_point_count_share": metric(value=None, display=f"0/{sample_n}条 · 0.0%样本覆盖", status="not_computable", qualitative="暂无该场景样本", scope=scene, reason="10条全媒体样本中没有该场景卖点内容"),
                "core_selling_point_count_share": metric(value=None, display=f"0/{sample_n}条 · 0.0%样本覆盖", status="not_computable", qualitative="暂无该场景样本", scope=scene, reason="10条全媒体样本中没有该场景卖点内容"),
                "selling_point_exposure_share": metric(value=None, display="0/0次 · 0.0%样本覆盖", status="not_computable", qualitative="暂无该场景样本", scope=scene, reason="没有该场景的标签×浏览量交叉样本"),
                "core_selling_point_exposure_share": metric(value=None, display="0/0次 · 0.0%样本覆盖", status="not_computable", qualitative="暂无该场景样本", scope=scene, reason="没有该场景的标签×浏览量交叉样本"),
                "content_verticality": metric(value=None, display="0/0条 · 0.0%样本覆盖", status="not_computable", qualitative="暂不可计算", scope=scene, reason="没有该场景的完整内容样本"),
                "audience_verticality": metric(value=None, display="0/0条 · 0.0%样本覆盖", status="not_computable", qualitative="暂不可计算", scope=scene, reason="没有该场景的评论达标样本"),
                "acquisition_effect_estimate": metric(value=None, display="0/0条 · 0.0%样本覆盖", status="not_computable", qualitative="暂不可计算", scope=scene, reason="没有该场景的评论达标样本"),
            }
            continue
        content_score = round(mean(row["content_auto_score"] for row in scene_rows))
        scene_audience = round(mean(row["audience_auto_score"] for row in scene_rows))
        scene_potential = round(mean(row["predicted_acquisition_potential_score"] for row in scene_rows))
        scenes[scene] = {
            "sample_n": len(scene_rows),
            "selling_point_count_share": metric(
                value=None,
                display=f"样本{len(scene_rows)}/{sample_n} · {pct(len(scene_rows), sample_n)}%",
                status="sample_only",
                qualitative="不可上卷为渠道",
                scope="10条全媒体诊断样本",
            ),
            "core_selling_point_count_share": metric(
                value=None,
                display=f"样本{len(scene_core)}/{sample_n} · {pct(len(scene_core), sample_n)}%",
                status="sample_only",
                qualitative="不可上卷为渠道",
                scope="10条全媒体诊断样本",
            ),
            "selling_point_exposure_share": metric(
                value=None,
                display=f"{scene_view}/{total_view}次 · {pct(scene_view, total_view)}%",
                status="sample_only",
                qualitative="不可上卷为渠道",
                scope=f"{scene_view}/{total_view}次样本浏览",
            ),
            "core_selling_point_exposure_share": metric(
                value=None,
                display=f"{scene_core_view}/{total_view}次 · {pct(scene_core_view, total_view)}%",
                status="sample_only",
                qualitative="不可上卷为渠道",
                scope=f"{scene_core_view}/{total_view}次样本浏览",
            ),
            "content_verticality": metric(
                value=content_score,
                display=f"{score_display(content_score)} · {sum(row['content_auto_score'] >= 70 for row in scene_rows)}/{len(scene_rows)} · {pct(sum(row['content_auto_score'] >= 70 for row in scene_rows), len(scene_rows))}%",
                status="sample_only",
                qualitative=v4.auto_qualitative(content_score),
                scope=f"该场景{len(scene_rows)}条全媒体样本",
            ),
            "audience_verticality": metric(
                value=scene_audience,
                display=f"{score_display(scene_audience)} · {scene_audience}%",
                status="sample_only",
                qualitative=audience_conclusion(scene_audience) or "",
                scope=f"该场景{len(scene_rows)}条评论达标样本",
            ),
            "acquisition_effect_estimate": metric(
                value=scene_potential,
                display=f"{score_display(scene_potential)} · {scene_potential}%",
                status="sample_only",
                qualitative=acquisition_conclusion(scene_potential) or "",
                scope=f"该场景{len(scene_rows)}条评论达标样本",
                reason="是拉新潜力预测，不是实际新增效果",
            ),
        }
    return {
        "scope": "仅小红书内容链接；338条唯一发布链接",
        "denominator": total,
        "summary": summary,
        "scenes": scenes,
    }


def compact(metric_obj: dict[str, Any]) -> str:
    text = metric_obj["display"]
    if metric_obj.get("qualitative"):
        text += f"；{metric_obj['qualitative']}"
    return text


def clean_title(value: str, limit: int = 58) -> str:
    cleaned = " ".join(str(value or "").replace("|", "｜").split())
    return (cleaned[:limit] + "…") if len(cleaned) > limit else cleaned


def douyin_content_details(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    for index, row in enumerate(rows, start=1):
        included = bool(row.get("included"))
        score = int(row.get("score") or 0)
        content_score = row.get("content_auto_score")
        is_core = included and row.get("primary_tier") == "core"
        point_text = (
            f"{score}/100 · {score}% · {row.get('qualitative') or '命中'}（{row.get('primary_id')}）"
            if included
            else "0/100 · 0.0% · 未命中正式卖点"
        )
        details.append(
            {
                "item_no": f"D{index:03d}",
                "content_id": str(row.get("aweme_id") or ""),
                "title": clean_title(row.get("desc") or "无标题"),
                "url": row.get("share_url") or "",
                "account": row.get("account_name") or "",
                "business_scene": row.get("business_scene") or "未归入业务场景",
                "selling_point": point_text,
                "core_selling_point": (
                    "1/1条 · 100.0% · 核心卖点"
                    if is_core else "0/1条 · 0.0% · 非核心或未命中"
                ),
                "exposure": "0/1条有效播放 · 0.0%数据覆盖 · play_count为占位值0",
                "content_verticality": (
                    f"{content_score}/100 · {content_score}% · {row.get('content_auto_qualitative') or v4.auto_qualitative(content_score)}"
                    if content_score is not None
                    else "0/1条完整证据 · 0.0%数据覆盖 · 暂不可计算"
                ),
                "audience_verticality": "0/20名有效评论用户 · 0.0%门槛覆盖 · 暂不可计算",
                "acquisition_effect_estimate": "0/1个统一预估分 · 0.0%可计算率 · 暂不可计算",
            }
        )
    return details


def xhs_content_details(sample_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    with v4.XHS_LINKS.open(encoding="utf-8-sig", newline="") as handle:
        links = list(csv.DictReader(handle))
    samples = {str(row["note_id"]): row for row in sample_rows}
    valid_views = [int(row["vv"]) for row in links if str(row.get("vv") or "").isdigit()]
    total_views = sum(valid_views)
    details: list[dict[str, Any]] = []
    for index, link in enumerate(links, start=1):
        note_id = str(link.get("note_id") or "")
        row = samples.get(note_id)
        vv = int(link["vv"]) if str(link.get("vv") or "").isdigit() else None
        if row:
            selling_score = int(row.get("selling_point_score") or 0)
            selling = (
                f"{selling_score}/100 · {selling_score}% · {row.get('selling_point_qualitative')}（{row.get('selling_point_id')}）"
                if row.get("selling_point_id")
                else "0/100 · 0.0% · 未命中正式卖点"
            )
            core = (
                "1/1条 · 100.0% · 核心卖点"
                if row.get("tier") == "core" else "0/1条 · 0.0% · 非核心或未命中"
            )
            content_score = int(row["content_auto_score"])
            audience_score = int(row["audience_auto_score"])
            potential_score = int(row["predicted_acquisition_potential_score"])
            content = f"{content_score}/100 · {content_score}% · {row.get('content_auto_conclusion')}"
            audience = f"{audience_score}/100 · {audience_score}% · {row.get('audience_auto_conclusion')}"
            potential = f"{potential_score}/100 · {potential_score}% · {acquisition_conclusion(potential_score)}"
            scene = row.get("business_scene") or "未归入业务场景"
        else:
            selling = "0/1条完整媒体证据 · 0.0%数据覆盖 · 待补采，不能视为未命中"
            core = "0/1条完整媒体证据 · 0.0%数据覆盖 · 待补采"
            content = "0/1条完整媒体证据 · 0.0%数据覆盖 · 暂不可计算"
            audience = "0/20名有效评论用户 · 0.0%门槛覆盖 · 暂不可计算"
            potential = "0/1个统一预估分 · 0.0%可计算率 · 暂不可计算"
            scene = "待完整媒体判定"
        exposure = (
            f"{vv}/{total_views}次 · {pct(vv, total_views)}%渠道已知浏览 · 有效浏览量"
            if vv is not None
            else "0/1条有效浏览 · 0.0%数据覆盖 · 浏览量缺失"
        )
        details.append(
            {
                "item_no": f"X{index:03d}",
                "content_id": note_id,
                "title": clean_title(link.get("title") or link.get("note_title") or "无标题"),
                "url": link.get("url") or "",
                "account": "",
                "business_scene": scene,
                "selling_point": selling,
                "core_selling_point": core,
                "exposure": exposure,
                "content_verticality": content,
                "audience_verticality": audience,
                "acquisition_effect_estimate": potential,
            }
        )
    return details


def detail_lines(details: list[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for row in details:
        label = f"{row['item_no']} · {row['content_id']}"
        if row.get("account"):
            label += f" · {row['account']}"
        lines.extend(
            [
                f"#### {label}",
                "",
                f"- 内容：[{row['title'] or '查看原内容'}]({row['url']})",
                f"- 业务场景：{row['business_scene']}",
                f"- 卖点：{row['selling_point']}",
                f"- 核心卖点：{row['core_selling_point']}",
                f"- 曝光：{row['exposure']}",
                f"- 内容垂直度：{row['content_verticality']}",
                f"- 互动用户垂直度：{row['audience_verticality']}",
                f"- 内容拉新效果预估：{row['acquisition_effect_estimate']}",
                "",
            ]
        )
    return lines


def scene_table(channel: dict[str, Any]) -> list[str]:
    lines = [
        "| 业务场景 | 卖点条数占比 | 核心卖点条数占比 | 卖点曝光占比 | 核心卖点曝光占比 | 内容垂直度 | 互动用户垂直度 | 内容拉新效果预估 |",
        "|---|---|---|---|---|---|---|---|",
    ]
    keys = (
        "selling_point_count_share",
        "core_selling_point_count_share",
        "selling_point_exposure_share",
        "core_selling_point_exposure_share",
        "content_verticality",
        "audience_verticality",
        "acquisition_effect_estimate",
    )
    for scene in v4.SCENES:
        row = channel["scenes"][scene]
        lines.append("| " + " | ".join([scene] + [compact(row[key]) for key in keys]) + " |")
    return lines


def summary_table(channel: dict[str, Any]) -> list[str]:
    labels = (
        ("selling_point_count_share", "卖点条数占比"),
        ("core_selling_point_count_share", "核心卖点条数占比"),
        ("selling_point_exposure_share", "卖点曝光占比"),
        ("core_selling_point_exposure_share", "核心卖点曝光占比"),
        ("content_verticality", "内容垂直度"),
        ("audience_verticality", "互动用户垂直度"),
        ("acquisition_effect_estimate", "内容拉新效果预估"),
    )
    lines = ["| 指标 | 结果 | 口径/边界 |", "|---|---|---|"]
    for key, label in labels:
        item = channel["summary"][key]
        boundary = item.get("scope") or item.get("reason") or "—"
        if item.get("reason") and item.get("scope"):
            boundary += f"；{item['reason']}"
        lines.append(f"| {label} | {compact(item)} | {boundary} |")
    return lines


def build_report(data: dict[str, Any]) -> str:
    d = data["channels"]["douyin"]
    x = data["channels"]["xiaohongshu"]
    lines = [
        "# 双渠道结构化结论报告 v5.1",
        "",
        f"生成日期：{RUN_DATE}  ",
        "固定结构：渠道 → 汇总 → 三个业务场景 → 内容明细。  ",
        "条数占比分母统一为该渠道全部发布；场景占比表示该场景对全渠道的贡献，不使用场景内自占比。  ",
        "所有指标均使用“数字 + 百分比 + 文字描述”；不可计算项中的0%只表示数据覆盖率，不表示业务结果为0。小红书样本结果不当作338条渠道结论。",
        "",
        "## 【抖音渠道】",
        "",
        f"范围：{d['scope']}。",
        "",
        "### 1、汇总",
        "",
        *summary_table(d),
        "",
        "### 2、三个业务场景",
        "",
        *scene_table(d),
        "",
        "> 场景内容垂直度只对已命中正式卖点、且已归入该场景的内容求均值；当前不能代表尚未命中卖点内容的场景分布。",
        "",
        "### 3、内容明细",
        "",
        f"共{len(d['content_details'])}条，逐条展示全部抖音发布。",
        "",
        *detail_lines(d["content_details"]),
        "",
        "## 【小红书渠道】",
        "",
        f"范围：{x['scope']}。",
        "",
        "### 1、汇总",
        "",
        *summary_table(x),
        "",
        "### 2、三个业务场景",
        "",
        *scene_table(x),
        "",
        "> 小红书只有10/338条完成全媒体卖点标注，三场景表是诊断样本结构；其中二手车和新车没有可用场景样本，不以0%冒充渠道结论。",
        "",
        "### 3、内容明细",
        "",
        f"共{len(x['content_details'])}条，逐条展示全部小红书链接；未完成全媒体采集的内容明确标记为待补采。",
        "",
        *detail_lines(x["content_details"]),
        "",
        "## 结论摘要",
        "",
        "1. 抖音卖点条数占比73.52%，核心卖点条数占比42.24%，核心生产占比仍低于60%-70%目标。",
        "2. 抖音核心内容几乎全部来自新车场景：新车核心184条，二手车核心1条，媒体-AI小懂核心0条。",
        "3. 抖音播放量、评论文本缺失，因此曝光、互动用户垂直度和统一拉新效果预估均暂不可计算。",
        "4. 小红书只有10条完整卖点样本，当前能输出样本诊断，但不能输出渠道级卖点条数/曝光分布。",
        "5. 小红书媒体-AI小懂的3条样本内容垂直度93/100、互动用户垂直度50/100、拉新效果预估64/100；仅代表这3条样本。",
        "",
        "## 配套文件",
        "",
        "- `抖音渠道结构化结论_v5_2026-08-02.png`",
        "- `小红书渠道结构化结论_v5_2026-08-02.png`",
        "- `双渠道结构化结论_v5_2026-08-02.json`",
        "- `懂车帝内容评估判断标准与流程_v4_终版.md`（已更新为v4.1终版结构）",
    ]
    return "\n".join(lines) + "\n"


def build() -> dict[str, Any]:
    taxonomy = v4.build_taxonomy()
    label_map = {item["id"]: item for item in taxonomy["labels"]}
    douyin_rows = v4.rebuild_douyin(label_map)
    xhs_rows, xhs_diagnostics = v4.xhs_sample_rows(label_map)
    douyin = douyin_section(douyin_rows)
    douyin["content_details"] = douyin_content_details(douyin_rows)
    xiaohongshu = xhs_section(xhs_rows, xhs_diagnostics)
    xiaohongshu["content_details"] = xhs_content_details(xhs_rows)
    data = {
        "report_version": "channel-structured-conclusions-v5.1",
        "generated_at": RUN_DATE,
        "structure": ["channel", "summary", "three_business_scenes", "content_details"],
        "metric_order": [
            "selling_point_count_share",
            "core_selling_point_count_share",
            "selling_point_exposure_share",
            "core_selling_point_exposure_share",
            "content_verticality",
            "audience_verticality",
            "acquisition_effect_estimate",
        ],
        "channels": {
            "douyin": douyin,
            "xiaohongshu": xiaohongshu,
        },
    }
    OUT_JSON.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    OUT_REPORT.write_text(build_report(data), encoding="utf-8")
    return data


if __name__ == "__main__":
    print(json.dumps(build(), ensure_ascii=False, indent=2))
