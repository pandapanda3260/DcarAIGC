"""Versioned v7 report generation and run-scoped manual review."""

from __future__ import annotations

import csv
import hashlib
import html
import json
import shutil
import subprocess
import sqlite3
import uuid
from pathlib import Path
from statistics import mean
from typing import Any, Iterable

from three_proposition_scoring import (
    acquisition_conclusion,
    audience_conclusion,
    content_conclusion,
    dcd_acquisition_score,
)

from .contracts import PROJECT_ROOT, load_contract, ratio_metric, score_metric, validate_report
from .storage import connect, migrate, now_iso, transaction
from .tasks import CURRENT_REPORT_VERSION, CURRENT_RULE_VERSION


DEFAULT_DB = PROJECT_ROOT / "app/data/web_mvp.sqlite3"
DEFAULT_REPORTS_ROOT = PROJECT_ROOT / "reports/runs"
SCORE_FIELDS = {
    "selling_point_score",
    "content_automotive_score",
    "audience_automotive_score",
    "dcar_task_fit_score",
    "action_intent_score",
}
REVIEWABLE_FIELDS = {
    "evaluation_status", "evidence_level", "evidence_summary",
    "primary_selling_point_id", "primary_selling_point_label", "primary_tier",
    "business_scene", "selling_point_score", "selling_point_qualitative",
    "selling_point_included", "pending_review", "secondary_selling_point_ids_json",
    "no_match_reason", "content_automotive_score", "valid_unique_commenters",
    "comment_sample_status", "audience_automotive_score", "dcar_task_fit_score",
    "action_intent_score",
}


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError:
        return str(path.resolve())


def create_report_run(
    db_path: Path = DEFAULT_DB,
    run_id: str | None = None,
    *,
    initial_status: str = "running",
) -> str:
    if initial_status not in {"queued", "running"}:
        raise ValueError("initial_status must be queued or running")
    run_id = run_id or f"run-{uuid.uuid4().hex[:12]}"
    timestamp = now_iso()
    with connect(db_path) as connection:
        migrate(connection)
        snapshot = connection.execute(
            "SELECT id FROM corpus_snapshots ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        evaluations = connection.execute(
            "SELECT * FROM evaluations ORDER BY content_item_id"
        ).fetchall()
        if not snapshot or not evaluations:
            raise RuntimeError("corpus snapshot and evaluations are required before report generation")
        with transaction(connection):
            connection.execute(
                """
                INSERT INTO runs(
                    id, created_at, updated_at, mode, channel, status, progress,
                    input_count, message, run_kind, scope, rule_version, report_version,
                    corpus_snapshot_id, report_revision, report_stale
                ) VALUES (?, ?, ?, 'report', 'dual', ?, 0, ?, ?,
                          'full_corpus', 'dual_channel', ?, ?, ?, 0, 0)
                """,
                (
                    run_id, timestamp, timestamp, initial_status, len(evaluations),
                    "任务已进入本地队列" if initial_status == "queued" else "正在生成动态报告",
                    CURRENT_RULE_VERSION, CURRENT_REPORT_VERSION, str(snapshot["id"]),
                ),
            )
            for evaluation in evaluations:
                value = dict(evaluation)
                connection.execute(
                    """
                    INSERT INTO run_evaluations(run_id, content_item_id, evaluation_json, updated_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (run_id, int(evaluation["content_item_id"]), _json(value), timestamp),
                )
    return run_id


def record_provider_usage(
    db_path: Path,
    run_id: str,
    entries: Iterable[dict[str, Any]],
) -> None:
    with connect(db_path) as connection, transaction(connection):
        for entry in entries:
            connection.execute(
                """
                INSERT INTO provider_usage(
                    run_id, provider, operation, request_attempts, billed_requests,
                    currency, amount, recorded_at, details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id, str(entry["provider"]), str(entry["operation"]),
                    int(entry.get("request_attempts") or 0),
                    int(entry.get("billed_requests") or 0),
                    str(entry.get("currency") or ""), entry.get("amount"), now_iso(),
                    _json(entry.get("details") or {}),
                ),
            )


def _load_rows(connection: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT c.*, r.evaluation_json
        FROM run_evaluations r
        JOIN content_items c ON c.id=r.content_item_id
        WHERE r.run_id=?
        ORDER BY c.platform, c.id
        """,
        (run_id,),
    ).fetchall()
    output = []
    for row in rows:
        item = dict(row)
        evaluation = json.loads(item.pop("evaluation_json"))
        item["evaluation"] = evaluation
        output.append(item)
    return output


def _ratio_qualitative(label: str, numerator: int, denominator: int) -> str:
    percentage = round(numerator * 100 / denominator, 2) if denominator else 0.0
    return f"{label}：{numerator}/{denominator}（{percentage}%）"


def _count_distribution(rows: list[dict[str, Any]], denominator: int, scope: str) -> dict[str, Any]:
    identifiable = [row for row in rows if row["evaluation"].get("evidence_level") in {"V2", "V3"}]
    selling = [row for row in rows if bool(row["evaluation"].get("selling_point_included"))]
    core = [row for row in selling if row["evaluation"].get("primary_tier") == "core"]
    other = [row for row in selling if row["evaluation"].get("primary_tier") != "core"]

    def metric(label: str, value: int) -> dict[str, Any]:
        return ratio_metric(
            value, denominator, status="available",
            qualitative=_ratio_qualitative(label, value, denominator), scope=scope,
        )

    return {
        "identifiable": metric("可识别内容", len(identifiable)),
        "selling_point_covered": metric("卖点覆盖", len(selling)),
        "core_selling_point": metric("核心卖点覆盖", len(core)),
        "other_selling_point": metric("其他卖点覆盖", len(other)),
        "diagnostics": {
            "no_selling_point_count": max(0, len(identifiable) - len(selling)),
            "unidentifiable_count": max(0, denominator - len(identifiable)) if len(rows) == denominator else max(0, len(rows) - len(identifiable)),
            "v2_v3_count": len(identifiable),
            "v0_v1_count": max(0, len(rows) - len(identifiable)),
            "evidence_coverage_percentage": round(len(identifiable) * 100 / denominator, 2) if denominator else None,
            "failed_count": sum(row["evaluation"].get("evaluation_status") == "failed" for row in rows),
            "pending_review_count": sum(bool(row["evaluation"].get("pending_review")) for row in rows),
        },
    }


def _exposure_distribution(
    rows: list[dict[str, Any]],
    *,
    all_channel_rows: list[dict[str, Any]],
    scope: str,
) -> dict[str, Any]:
    valid_all = [row for row in all_channel_rows if isinstance(row.get("exposure_value"), int) and row["exposure_value"] > 0]
    cross_all = [
        row for row in valid_all
        if row["evaluation"].get("evidence_level") in {"V2", "V3"}
    ]
    denominator_items = len(all_channel_rows)
    cross_percentage = round(len(cross_all) * 100 / denominator_items, 2) if denominator_items else None
    calculable = bool(cross_percentage is not None and cross_percentage >= 90)
    total_exposure = sum(row["exposure_value"] for row in valid_all)
    valid_scope = [row for row in rows if isinstance(row.get("exposure_value"), int) and row["exposure_value"] > 0]
    identifiable = [row for row in valid_scope if row["evaluation"].get("evidence_level") in {"V2", "V3"}]
    selling = [row for row in valid_scope if bool(row["evaluation"].get("selling_point_included"))]
    core = [row for row in selling if row["evaluation"].get("primary_tier") == "core"]
    other = [row for row in selling if row["evaluation"].get("primary_tier") != "core"]
    reason = "" if calculable else f"标签×有效曝光交叉覆盖仅{len(cross_all)}/{denominator_items}（{cross_percentage or 0}%），低于90%门槛"

    def metric(label: str, selected: list[dict[str, Any]]) -> dict[str, Any]:
        value = sum(row["exposure_value"] for row in selected) if calculable else None
        qualitative = (
            _ratio_qualitative(label, int(value or 0), total_exposure)
            if calculable
            else f"{label}暂不可计算：{reason}"
        )
        return ratio_metric(
            value, total_exposure, status="available" if calculable else "unavailable",
            qualitative=qualitative, scope=scope, reason=reason,
        )

    return {
        "identifiable": metric("可识别内容曝光", identifiable),
        "selling_point_covered": metric("卖点覆盖曝光", selling),
        "core_selling_point": metric("核心卖点曝光", core),
        "other_selling_point": metric("其他卖点曝光", other),
        "coverage": {
            "total_valid_exposure": total_exposure,
            "valid_exposure_items": len(valid_all),
            "missing_exposure_items": sum(row.get("exposure_value") is None for row in all_channel_rows),
            "zero_or_invalid_exposure_items": sum(row.get("exposure_value") is not None and int(row.get("exposure_value") or 0) <= 0 for row in all_channel_rows),
            "label_exposure_cross_covered_items": len(cross_all),
            "cross_coverage_percentage": cross_percentage,
            "required_percentage": 90,
            "calculable": calculable,
            "unavailable_reason": reason,
        },
    }


def _score_group(rows: list[dict[str, Any]], scope: str) -> dict[str, Any]:
    mapping = {
        "content_automotive": ("content_automotive_score", "内容汽车性"),
        "audience_automotive": ("audience_automotive_score", "互动受众汽车性"),
        "acquisition_potential": ("acquisition_potential", "懂车帝拉新潜力"),
    }
    total = len(rows)
    result: dict[str, Any] = {}
    coverage: dict[str, Any] = {"total_items": total, "audience_gate": 20}
    for key, (field, label) in mapping.items():
        values = [int(row["evaluation"][field]) for row in rows if row["evaluation"].get(field) is not None]
        score = round(mean(values)) if values else None
        status = "unavailable" if not values else "available" if len(values) == total else "sample_only"
        reason = "" if status == "available" else "无可评分证据" if not values else f"仅{len(values)}/{total}条满足评分证据门槛"
        if score is None:
            qualitative = f"{label}暂不可计算"
        else:
            conclusion = (
                content_conclusion(score)
                if key == "content_automotive"
                else audience_conclusion(score)
                if key == "audience_automotive"
                else acquisition_conclusion(score)
            )
            qualitative = f"{conclusion}；平均{score}/100，覆盖{len(values)}/{total}条"
        result[key] = score_metric(
            score, len(values), total, status=status, qualitative=qualitative,
            scope=scope, reason=reason,
        )
        coverage[f"{key}_scorable_items"] = len(values)
    result["coverage"] = coverage
    return result


def _detail(row: dict[str, Any]) -> dict[str, Any]:
    evaluation = row["evaluation"]

    def item_score(field: str, qualitative_field: str) -> dict[str, Any]:
        score = evaluation.get(field)
        return {
            "score": score,
            "scale": 100,
            "status": "available" if score is not None else "unavailable",
            "qualitative": evaluation.get(qualitative_field) or "暂不可计算",
        }

    return {
        "content_item_id": int(row["id"]),
        "platform_content_id": row["platform_content_id"],
        "canonical_url": row["canonical_url"],
        "account_name": row["account_name"],
        "account_quality": row["account_quality"],
        "caption": row["caption"],
        "content_type": row["content_type"],
        "exposure_value": row["exposure_value"],
        "exposure_status": row["exposure_status"],
        "evidence_level": evaluation.get("evidence_level"),
        "evidence_summary": evaluation.get("evidence_summary"),
        "selling_point": {
            "id": evaluation.get("primary_selling_point_id"),
            "label": evaluation.get("primary_selling_point_label"),
            "tier": evaluation.get("primary_tier"),
            "business_scene": evaluation.get("business_scene"),
            "score": evaluation.get("selling_point_score"),
            "qualitative": evaluation.get("selling_point_qualitative"),
            "included": bool(evaluation.get("selling_point_included")),
            "pending_review": bool(evaluation.get("pending_review")),
            "no_match_reason": evaluation.get("no_match_reason"),
        },
        "valid_unique_commenters": evaluation.get("valid_unique_commenters"),
        "comment_sample_status": evaluation.get("comment_sample_status"),
        "content_automotive": item_score("content_automotive_score", "content_automotive_qualitative"),
        "audience_automotive": item_score("audience_automotive_score", "audience_automotive_qualitative"),
        "acquisition_potential": item_score("acquisition_potential", "acquisition_potential_qualitative"),
        "dcar_task_fit_score": evaluation.get("dcar_task_fit_score"),
        "action_intent_score": evaluation.get("action_intent_score"),
    }


def _scene(rows: list[dict[str, Any]], all_rows: list[dict[str, Any]], name: str) -> dict[str, Any]:
    selected = [row for row in rows if row["evaluation"].get("business_scene") == name]
    core = sum(
        bool(row["evaluation"].get("selling_point_included"))
        and row["evaluation"].get("primary_tier") == "core"
        for row in selected
    )
    selling = sum(bool(row["evaluation"].get("selling_point_included")) for row in selected)
    publication_n = len(selected)
    return {
        "publication_n": publication_n,
        "count_distribution": _count_distribution(selected, len(all_rows), f"{name}场景/渠道全部发布"),
        "exposure_distribution": _exposure_distribution(
            selected, all_channel_rows=all_rows, scope=f"{name}场景曝光/渠道有效总曝光",
        ),
        "verticality": _score_group(selected, f"{name}场景发布"),
        "scene_internal": {
            "core_share_within_scene_publications": ratio_metric(
                core, publication_n, status="available" if publication_n else "unavailable",
                qualitative=_ratio_qualitative("场景内部核心卖点", core, publication_n),
                scope=f"{name}场景内部发布",
                reason="" if publication_n else "该场景没有发布内容",
            ),
            "selling_point_coverage_within_scene": ratio_metric(
                selling, publication_n, status="available" if publication_n else "unavailable",
                qualitative=_ratio_qualitative("场景内部卖点覆盖", selling, publication_n),
                scope=f"{name}场景内部发布",
                reason="" if publication_n else "该场景没有发布内容",
            ),
        },
    }


def _channel(rows: list[dict[str, Any]], platform: str) -> dict[str, Any]:
    selected = [row for row in rows if row["platform"] == platform]
    denominator = len(selected)
    count = _count_distribution(selected, denominator, f"{platform}渠道全部发布")
    core_percentage = count["core_selling_point"]["percentage"]
    target_status = "within_target" if core_percentage is not None and 60 <= core_percentage <= 70 else "below_target" if core_percentage is not None and core_percentage < 60 else "above_target"
    contract = load_contract()
    scenes = {
        name: _scene(selected, selected, name)
        for name in contract["business_scenes"]
    }
    return {
        "scope": f"{platform}渠道全部发布",
        "denominator": denominator,
        "count_distribution": count,
        "exposure_distribution": _exposure_distribution(
            selected, all_channel_rows=selected, scope=f"{platform}渠道有效总曝光",
        ),
        "verticality": _score_group(selected, f"{platform}渠道全部发布"),
        "channel_targets": {
            "core_selling_point_publication_share": {
                "actual_percentage": core_percentage,
                "minimum_percentage": 60,
                "maximum_percentage": 70,
                "status": target_status,
                "gap_to_minimum_percentage_points": round(60 - core_percentage, 2) if core_percentage is not None and core_percentage < 60 else 0,
                "denominator": denominator,
            }
        },
        "scenes": scenes,
        "content_details": [_detail(row) for row in selected],
    }


def _conclusions(channels: dict[str, Any]) -> list[dict[str, str]]:
    output = []
    for platform, title in (("douyin", "抖音"), ("xiaohongshu", "小红书")):
        channel = channels[platform]
        count = channel["count_distribution"]
        target = channel["channel_targets"]["core_selling_point_publication_share"]
        vertical = channel["verticality"]
        exposure = channel["exposure_distribution"]["coverage"]
        output.extend([
            {
                "title": f"{title}卖点与核心目标",
                "text": f"卖点覆盖{count['selling_point_covered']['percentage']}%，核心卖点{target['actual_percentage']}%，目标状态为{target['status']}。",
            },
            {
                "title": f"{title}证据与曝光边界",
                "text": f"可识别{count['identifiable']['numerator']}/{channel['denominator']}条；曝光交叉覆盖{exposure['cross_coverage_percentage']}%，曝光指标{'可计算' if exposure['calculable'] else '暂不可计算'}。",
            },
            {
                "title": f"{title}垂直度与拉新潜力",
                "text": f"内容汽车性{vertical['content_automotive']['score']}分，互动受众汽车性{vertical['audience_automotive']['score']}分，拉新潜力{vertical['acquisition_potential']['score']}分；后两项均按有效评论门槛展示样本覆盖。",
            },
        ])
    return output


def _markdown(report: dict[str, Any]) -> str:
    lines = [
        "# 双渠道结构化结论报告 v7",
        "",
        f"- 规则版本：{report['rule_version']}",
        f"- 报告运行：{report['metadata']['run_id']}",
        f"- 报告修订：{report['metadata']['revision']}",
        f"- 生成时间：{report['metadata']['generated_at']}",
        "",
    ]

    def metric_value(metric: dict[str, Any]) -> str:
        if metric.get("kind") == "score":
            return "暂不可计算" if metric.get("score") is None else f"{metric['score']}/100（覆盖{metric['scorable_items']}/{metric['total_items']}）"
        return "暂不可计算" if metric.get("percentage") is None else f"{metric['numerator']}/{metric['denominator']}（{metric['percentage']}%）"

    def add_channel(platform: str, title: str) -> None:
        channel = report["channels"][platform]
        lines.extend([f"## {title}渠道", "", "### 1、汇总", ""])
        lines.extend(["| 指标 | 条数维度 | 曝光维度 |", "|---|---:|---:|"])
        labels = {
            "identifiable": "可识别内容",
            "selling_point_covered": "卖点覆盖",
            "core_selling_point": "核心卖点覆盖",
            "other_selling_point": "其他卖点",
        }
        for key, label in labels.items():
            lines.append(f"| {label} | {metric_value(channel['count_distribution'][key])} | {metric_value(channel['exposure_distribution'][key])} |")
        lines.extend(["", "| 内容垂直度 | 分值与定性说明 |", "|---|---|"])
        for key, label in (("content_automotive", "内容汽车性"), ("audience_automotive", "互动受众汽车性"), ("acquisition_potential", "懂车帝拉新潜力")):
            metric = channel["verticality"][key]
            lines.append(f"| {label} | {metric_value(metric)}；{metric['qualitative']} |")
        lines.extend(["", "### 2、三个业务场景", ""])
        for scene_name, scene in channel["scenes"].items():
            lines.extend([
                f"#### {scene_name}", "",
                f"发布条数：{scene['publication_n']}。卖点覆盖：{metric_value(scene['count_distribution']['selling_point_covered'])}。场景内部核心卖点占比：{metric_value(scene['scene_internal']['core_share_within_scene_publications'])}。",
                "",
            ])

    add_channel("douyin", "抖音")
    add_channel("xiaohongshu", "小红书")
    lines.extend(["## 结论摘要", ""])
    for item in report["conclusion_summary"]:
        lines.append(f"- {item['title']}：{item['text']}")
    lines.append("")
    for platform, title in (("douyin", "抖音内容明细"), ("xiaohongshu", "小红书内容明细")):
        lines.extend([f"## {title}", "", "| 内容ID | 标题/正文 | 证据 | 卖点 | 内容汽车性 | 互动受众汽车性 | 拉新潜力 |", "|---|---|---|---|---:|---:|---:|"])
        for detail in report["channels"][platform]["content_details"]:
            caption = " ".join(str(detail["caption"] or "").split()).replace("|", "\\|")[:90]
            selling = detail["selling_point"]["label"] or detail["selling_point"]["no_match_reason"] or "未命中"
            values = [detail[name]["score"] for name in ("content_automotive", "audience_automotive", "acquisition_potential")]
            scores = ["暂不可计算" if value is None else f"{value}/100" for value in values]
            lines.append(f"| {detail['platform_content_id']} | {caption} | {detail['evidence_level']} | {selling} | {scores[0]} | {scores[1]} | {scores[2]} |")
        lines.append("")
    lines.extend(["## 配套文件", ""])
    for asset in report["assets"]:
        lines.append(f"- {asset['label']}：`{asset['path']}`")
    lines.append("")
    return "\n".join(lines)


def _summary_svg(report: dict[str, Any]) -> str:
    width, height = 1600, 1600
    blocks = []
    for index, (platform, title) in enumerate((("douyin", "抖音"), ("xiaohongshu", "小红书"))):
        channel = report["channels"][platform]
        count = channel["count_distribution"]
        vertical = channel["verticality"]
        blocks.append({
            "title": title,
            "identifiable": count["identifiable"]["percentage"] or 0,
            "selling": count["selling_point_covered"]["percentage"] or 0,
            "core": count["core_selling_point"]["percentage"] or 0,
            "content": vertical["content_automotive"]["score"],
            "audience": vertical["audience_automotive"]["score"],
            "acquisition": vertical["acquisition_potential"]["score"],
            "content_q": vertical["content_automotive"]["qualitative"].split("；", 1)[0],
            "audience_q": vertical["audience_automotive"]["qualitative"].split("；", 1)[0],
            "acquisition_q": vertical["acquisition_potential"]["qualitative"].split("；", 1)[0],
            "x": 70 + index * 765,
        })
    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="1600" height="1600" fill="#F5F7FA"/>',
        '<style>text{font-family:-apple-system,BlinkMacSystemFont,"PingFang SC","Microsoft YaHei",sans-serif;fill:#172033}.title{font-size:42px;font-weight:700}.sub{font-size:24px;fill:#64748b}.desc{font-size:18px;fill:#64748b}.h{font-size:34px;font-weight:700}.label{font-size:24px}.value{font-size:31px;font-weight:700}</style>',
        '<text x="70" y="75" class="title">双渠道内容与拉新潜力核心结论</text>',
        f'<text x="70" y="115" class="sub">规则 v5 · 报告 v7 · 修订 {report["metadata"]["revision"]}</text>',
    ]
    colors = ["#2563EB", "#14B8A6", "#F59E0B"]
    for block in blocks:
        x = block["x"]
        svg.extend([
            f'<rect x="{x}" y="155" width="700" height="1320" rx="28" fill="#FFFFFF"/>',
            f'<text x="{x+35}" y="215" class="h">{block["title"]}渠道</text>',
        ])
        for i, (label, key) in enumerate((("可识别内容", "identifiable"), ("卖点覆盖", "selling"), ("核心卖点覆盖", "core"))):
            y = 315 + i * 145
            value = float(block[key])
            svg.extend([
                f'<text x="{x+35}" y="{y}" class="label">{label}</text>',
                f'<rect x="{x+235}" y="{y-25}" width="360" height="28" rx="14" fill="#E8EDF5"/>',
                f'<rect x="{x+235}" y="{y-25}" width="{round(3.6*value,1)}" height="28" rx="14" fill="{colors[i]}"/>',
                f'<text x="{x+615}" y="{y}" class="value">{value:.1f}%</text>',
            ])
        svg.append(f'<text x="{x+35}" y="790" class="h">内容垂直度</text>')
        for i, (label, key) in enumerate((("内容汽车性", "content"), ("互动受众汽车性", "audience"), ("懂车帝拉新潜力", "acquisition"))):
            y = 900 + i * 160
            value = block[key]
            display = "暂不可计算" if value is None else f"{value}/100"
            svg.extend([
                f'<text x="{x+35}" y="{y}" class="label">{label}</text>',
                f'<text x="{x+430}" y="{y}" class="value">{html.escape(display)}</text>',
                f'<text x="{x+35}" y="{y+42}" class="desc">{html.escape(str(block[key+"_q"]))}</text>',
            ])
    svg.extend([
        '<text x="70" y="1545" class="sub">注：互动受众与拉新潜力仅汇总达到 20 个有效独立评论用户门槛的样本；证据不足不补 0、不重加权。</text>',
        '</svg>',
    ])
    return "\n".join(svg) + "\n"


def _render_share_png(svg_path: Path, png_path: Path) -> bool:
    renderer = shutil.which("qlmanage")
    if not renderer:
        return False
    result = subprocess.run(
        [renderer, "-t", "-s", "1600", "-o", str(svg_path.parent), str(svg_path)],
        capture_output=True,
        timeout=30,
    )
    generated = svg_path.with_name(svg_path.name + ".png")
    if result.returncode != 0 or not generated.exists() or generated.stat().st_size <= 1024:
        return False
    generated.replace(png_path)
    return True


def _write_details(path: Path, details: list[dict[str, Any]]) -> None:
    fields = [
        "content_item_id", "platform_content_id", "canonical_url", "account_name",
        "caption", "content_type", "exposure_value", "evidence_level",
        "selling_point_id", "selling_point_label", "selling_point_tier", "business_scene",
        "selling_point_score", "content_automotive_score", "valid_unique_commenters",
        "audience_automotive_score", "acquisition_potential", "comment_sample_status",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for detail in details:
            writer.writerow({
                "content_item_id": detail["content_item_id"],
                "platform_content_id": detail["platform_content_id"],
                "canonical_url": detail["canonical_url"],
                "account_name": detail["account_name"],
                "caption": detail["caption"],
                "content_type": detail["content_type"],
                "exposure_value": detail["exposure_value"],
                "evidence_level": detail["evidence_level"],
                "selling_point_id": detail["selling_point"]["id"],
                "selling_point_label": detail["selling_point"]["label"],
                "selling_point_tier": detail["selling_point"]["tier"],
                "business_scene": detail["selling_point"]["business_scene"],
                "selling_point_score": detail["selling_point"]["score"],
                "content_automotive_score": detail["content_automotive"]["score"],
                "valid_unique_commenters": detail["valid_unique_commenters"],
                "audience_automotive_score": detail["audience_automotive"]["score"],
                "acquisition_potential": detail["acquisition_potential"]["score"],
                "comment_sample_status": detail["comment_sample_status"],
            })


def build_report_revision(
    db_path: Path,
    run_id: str,
    reports_root: Path = DEFAULT_REPORTS_ROOT,
) -> dict[str, Any]:
    with connect(db_path) as connection:
        migrate(connection)
        run = connection.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
        if not run:
            raise ValueError("run not found")
        rows = _load_rows(connection, run_id)
        if not rows:
            raise ValueError("run has no evaluation snapshot")
        revision = int(connection.execute(
            "SELECT COALESCE(MAX(revision), 0) + 1 FROM report_revisions WHERE run_id=?",
            (run_id,),
        ).fetchone()[0])
        usage = [dict(row) for row in connection.execute(
            "SELECT provider, operation, request_attempts, billed_requests, currency, amount FROM provider_usage WHERE run_id=? ORDER BY id",
            (run_id,),
        )]
        review_count = int(connection.execute(
            "SELECT COUNT(*) FROM manual_reviews WHERE run_id=?", (run_id,)
        ).fetchone()[0])

    revision_dir = reports_root / run_id / f"revision_{revision:03d}"
    revision_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "json": revision_dir / "report.json",
        "markdown": revision_dir / "report.md",
        "svg": revision_dir / "core_summary.svg",
        "png": revision_dir / "core_summary.png",
        "douyin_csv": revision_dir / "douyin_content_details.csv",
        "xiaohongshu_csv": revision_dir / "xiaohongshu_content_details.csv",
    }
    channels = {
        "douyin": _channel(rows, "douyin"),
        "xiaohongshu": _channel(rows, "xiaohongshu"),
    }
    report = {
        "report_version": CURRENT_REPORT_VERSION,
        "rule_version": CURRENT_RULE_VERSION,
        "metadata": {
            "run_id": run_id,
            "revision": revision,
            "generated_at": now_iso(),
            "corpus_snapshot_id": run["corpus_snapshot_id"],
            "report_order": load_contract()["report_order"],
        },
        "run_summary": {
            "content_items": len(rows),
            "douyin_items": len(channels["douyin"]["content_details"]),
            "xiaohongshu_items": len(channels["xiaohongshu"]["content_details"]),
            "manual_review_count": review_count,
            "provider_usage": usage,
        },
        "channels": channels,
        "conclusion_summary": _conclusions(channels),
        "assets": [
            {"label": "v7 JSON", "path": _relative(paths["json"]), "type": "application/json"},
            {"label": "结构化文字报告", "path": _relative(paths["markdown"]), "type": "text/markdown"},
            {"label": "核心结论图片", "path": _relative(paths["svg"]), "type": "image/svg+xml"},
            {"label": "抖音内容明细", "path": _relative(paths["douyin_csv"]), "type": "text/csv"},
            {"label": "小红书内容明细", "path": _relative(paths["xiaohongshu_csv"]), "type": "text/csv"},
        ],
    }
    paths["svg"].write_text(_summary_svg(report), encoding="utf-8")
    if _render_share_png(paths["svg"], paths["png"]):
        report["assets"].append({
            "label": "核心结论分享图",
            "path": _relative(paths["png"]),
            "type": "image/png",
        })
    validate_report(report)
    paths["json"].write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    paths["markdown"].write_text(_markdown(report), encoding="utf-8")
    _write_details(paths["douyin_csv"], channels["douyin"]["content_details"])
    _write_details(paths["xiaohongshu_csv"], channels["xiaohongshu"]["content_details"])
    output_sha = hashlib.sha256(paths["json"].read_bytes()).hexdigest()
    source_sha = _sha([
        {"content_item_id": row["id"], "evaluation": row["evaluation"]}
        for row in rows
    ])
    with connect(db_path) as connection, transaction(connection):
        connection.execute("UPDATE report_revisions SET is_current=0 WHERE run_id=?", (run_id,))
        connection.execute(
            """
            INSERT INTO report_revisions(
                run_id, revision, created_at, report_json_path, report_markdown_path,
                summary_image_path, output_sha256, source_evaluation_sha256, is_current
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                run_id, revision, now_iso(), _relative(paths["json"]),
                _relative(paths["markdown"]), _relative(paths["svg"]), output_sha, source_sha,
            ),
        )
        connection.execute(
            """
            UPDATE runs SET status='completed', progress=100, message='动态报告已生成',
                output_path=?, output_sha256=?, report_revision=?, report_stale=0,
                updated_at=? WHERE id=?
            """,
            (_relative(paths["json"]), output_sha, revision, now_iso(), run_id),
        )
    return report


def _validate_review_patch(patch: dict[str, Any]) -> None:
    unknown = sorted(set(patch) - REVIEWABLE_FIELDS)
    if unknown:
        raise ValueError(f"unsupported review fields: {unknown}")
    for field in SCORE_FIELDS:
        value = patch.get(field)
        if field in patch and value is not None and (not isinstance(value, int) or not 0 <= value <= 100):
            raise ValueError(f"{field} must be null or an integer from 0 to 100")
    if "evidence_level" in patch and patch["evidence_level"] not in {"V0", "V1", "V2", "V3"}:
        raise ValueError("evidence_level must be V0, V1, V2, or V3")


def submit_manual_review(
    db_path: Path,
    run_id: str,
    content_item_id: int,
    patch: dict[str, Any],
    reason: str,
    *,
    reports_root: Path = DEFAULT_REPORTS_ROOT,
    reviewer: str = "local-user",
) -> dict[str, Any]:
    reason = " ".join(reason.split())
    if not reason:
        raise ValueError("manual review reason is required")
    _validate_review_patch(patch)
    with connect(db_path) as connection:
        row = connection.execute(
            "SELECT evaluation_json FROM run_evaluations WHERE run_id=? AND content_item_id=?",
            (run_id, content_item_id),
        ).fetchone()
        run = connection.execute("SELECT report_revision FROM runs WHERE id=?", (run_id,)).fetchone()
        if not row or not run:
            raise ValueError("run evaluation not found")
        previous = json.loads(row["evaluation_json"])
        updated = {**previous, **patch}
        valid_users = updated.get("valid_unique_commenters")
        if valid_users is None or int(valid_users) < 20 or updated.get("comment_sample_status") != "scorable":
            updated["audience_automotive_score"] = None
            updated["action_intent_score"] = None
        content_score = updated.get("content_automotive_score")
        audience_score = updated.get("audience_automotive_score")
        fit_score = updated.get("dcar_task_fit_score")
        action_score = updated.get("action_intent_score")
        potential = (
            dcd_acquisition_score(
                content_score=content_score,
                audience_score=audience_score,
                dcd_fit_score=fit_score,
                action_intent_score=action_score,
            )
            if content_score is not None
            else None
        )
        updated["content_automotive_qualitative"] = content_conclusion(content_score) if content_score is not None else "暂不可计算"
        updated["audience_automotive_qualitative"] = audience_conclusion(audience_score) or "暂不可计算"
        updated["acquisition_potential"] = potential
        updated["acquisition_potential_qualitative"] = acquisition_conclusion(potential) or "暂不可计算"
        updated["evaluated_at"] = now_iso()
        next_revision = int(run["report_revision"]) + 1
        with transaction(connection):
            connection.execute(
                "UPDATE run_evaluations SET evaluation_json=?, updated_at=? WHERE run_id=? AND content_item_id=?",
                (_json(updated), now_iso(), run_id, content_item_id),
            )
            connection.execute(
                """
                INSERT INTO manual_reviews(
                    run_id, content_item_id, previous_evaluation_json, patch_json,
                    reason, reviewer, applied_revision, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (run_id, content_item_id, _json(previous), _json(patch), reason, reviewer, next_revision, now_iso()),
            )
            connection.execute(
                "UPDATE runs SET report_stale=1, status='running', message='人工复核后正在重算报告', updated_at=? WHERE id=?",
                (now_iso(), run_id),
            )
    return build_report_revision(db_path, run_id, reports_root)
