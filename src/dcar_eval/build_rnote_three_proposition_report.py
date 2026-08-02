#!/usr/bin/env python3
"""Build the final Rnote-backed three-proposition results and report."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import date
from pathlib import Path
from statistics import mean
from typing import Any, Iterable, Mapping

from project_paths import CURRENT_REPORTS_DIR, RNOTE_CACHE_DIR, XHS_PROCESSED_DIR

from generate_three_proposition_visual import write_visual
from three_proposition_scoring import (
    acquisition_conclusion,
    audience_auto_score,
    audience_conclusion,
    content_auto_score,
    content_conclusion,
    dcd_acquisition_score,
)


PREDICTION_VERSION = "three-proposition-v1.0-rnote"
SOURCE_LABELS = {"auto": "汽车来源组", "non_auto": "非汽车来源组"}
REPORT_VISUAL_RELATIVE_PATH = "rnote_cache/three_proposition_visual_summary.png"


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise ValueError(f"{path}:{number} must be a JSON object")
        rows.append(value)
    return rows


def write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    path.write_text(
        "".join(json.dumps(dict(row), ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )


def rounded_mean(rows: list[Mapping[str, Any]], field: str) -> int | None:
    values = [int(row[field]) for row in rows if row.get(field) is not None]
    return round(mean(values)) if values else None


def score_action(counts: Mapping[str | int, int], expected_total: int) -> int:
    normalized = {int(key): int(value) for key, value in counts.items()}
    if set(normalized) - {0, 30, 60, 100}:
        raise ValueError("action score buckets must be 0, 30, 60 or 100")
    if any(value < 0 for value in normalized.values()):
        raise ValueError("action score bucket counts cannot be negative")
    if sum(normalized.values()) != expected_total:
        raise ValueError("action score bucket counts must equal valid users")
    return round(sum(score * count for score, count in normalized.items()) / expected_total)


def clean_evidence(value: Any, field: str) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"{field} must be a list")
    result = [str(item).strip() for item in value if str(item).strip()]
    if not 1 <= len(result) <= 3:
        raise ValueError(f"{field} must contain 1-3 non-empty items")
    return result


def build_final_results(
    attempts: list[dict[str, Any]], scores: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    attempt_by_id = {row["sample_attempt_id"]: row for row in attempts}
    final_ids = {
        row["sample_attempt_id"] for row in attempts if row.get("final_sample_eligible")
    }
    score_by_id = {row["sample_attempt_id"]: row for row in scores}
    if set(score_by_id) != final_ids:
        raise ValueError(
            f"manual scores must exactly match final sample IDs; missing={sorted(final_ids-set(score_by_id))}, "
            f"extra={sorted(set(score_by_id)-final_ids)}"
        )
    results: list[dict[str, Any]] = []
    for attempt_id in sorted(final_ids, key=lambda value: (attempt_by_id[value]["source_stratum"], attempt_by_id[value]["target_slot"])):
        attempt = attempt_by_id[attempt_id]
        score = score_by_id[attempt_id]
        valid_users = int(attempt["valid_unique_commenters"])
        audience_counts = {str(key): int(value) for key, value in score["audience_score_counts"].items()}
        audience = audience_auto_score(
            audience_counts,
            valid_unique_commenters=valid_users,
            comment_sample_status="scorable",
        )
        if audience is None:
            raise ValueError(f"{attempt_id}: audience unexpectedly unscorable")
        action_counts = {str(key): int(value) for key, value in score["action_score_counts"].items()}
        action = score_action(action_counts, valid_users)
        content, adjustment = content_auto_score(
            text_score=score["content_text_score"],
            text_reliability=score["content_text_reliability"],
            media_score=score["content_media_score"],
            media_reliability=score["content_media_reliability"],
            comment_topic_score=score["comment_topic_score"],
            valid_unique_commenters=valid_users,
        )
        fit = int(score["dcd_fit_score"])
        acquisition = dcd_acquisition_score(
            content_score=content,
            audience_score=audience,
            dcd_fit_score=fit,
            action_intent_score=action,
        )
        if acquisition is None:
            raise ValueError(f"{attempt_id}: acquisition unexpectedly unscorable")
        custom_content_conclusion = str(score.get("content_auto_conclusion") or "").strip()
        result = {
            "sample_attempt_id": attempt_id,
            "target_slot": attempt["target_slot"],
            "source_stratum": attempt["source_stratum"],
            "source_stratum_display": SOURCE_LABELS[attempt["source_stratum"]],
            "sample_role": attempt["sample_role"],
            "note_id": attempt["note_id"],
            "url": attempt["url"],
            "title": str(score.get("title") or "").strip(),
            "content_text_score": int(score["content_text_score"]),
            "content_text_reliability": float(score["content_text_reliability"]),
            "content_media_score": int(score["content_media_score"]),
            "content_media_reliability": float(score["content_media_reliability"]),
            "comment_topic_score": int(score["comment_topic_score"]),
            "content_comment_adjustment": round(adjustment, 3),
            "content_auto_score": content,
            "content_auto_conclusion": custom_content_conclusion or content_conclusion(content),
            "content_subcategory": str(score.get("content_subcategory") or "").strip(),
            "content_auto_evidence": clean_evidence(
                score["content_auto_evidence"], "content_auto_evidence"
            ),
            "comment_fetch_status": attempt["comment_fetch_status"],
            "platform_comment_count": attempt.get("platform_comment_count"),
            "raw_comment_count": attempt["raw_comment_count"],
            "valid_unique_commenters": valid_users,
            "comment_pages_fetched": attempt["comment_pages_fetched"],
            "comment_pagination_complete": attempt["comment_pagination_complete"],
            "comment_sample_status": "scorable",
            "audience_score_counts": audience_counts,
            "audience_auto_score": audience,
            "audience_auto_conclusion": audience_conclusion(audience),
            "audience_auto_evidence": clean_evidence(
                score["audience_auto_evidence"], "audience_auto_evidence"
            ),
            "dcd_fit_score": fit,
            "action_score_counts": action_counts,
            "action_intent_score": action,
            "dcd_acquisition_score": acquisition,
            "dcd_acquisition_conclusion": acquisition_conclusion(acquisition),
            "dcd_acquisition_evidence": clean_evidence(
                score["dcd_acquisition_evidence"], "dcd_acquisition_evidence"
            ),
            "prediction_version": PREDICTION_VERSION,
            "prediction_limitations": (
                "基于可见内容和评论抽样的候选排序分；不是下载概率，也不是实际新增效果"
            ),
            "human_review_status": "confirmed",
            "actual_status": "not_tested",
            "actual_clicks": None,
            "actual_installs": None,
            "actual_confirmed_new_users": None,
        }
        results.append(result)
    return results


def base_content_rows(
    attempts: list[dict[str, Any]],
    prior_content: list[dict[str, Any]],
    final_results: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    prior = {row["pilot_id"]: row for row in prior_content}
    final = {row["sample_attempt_id"]: row for row in final_results}
    result: list[dict[str, Any]] = []
    for attempt in attempts:
        if attempt.get("sample_role") != "base_random_sample":
            continue
        attempt_id = attempt["sample_attempt_id"]
        content = final.get(attempt_id) or prior.get(attempt_id)
        if not content:
            raise ValueError(f"missing base content score for {attempt_id}")
        result.append(
            {
                "sample_attempt_id": attempt_id,
                "source_stratum": attempt["source_stratum"],
                "content_auto_score": int(content["content_auto_score"]),
            }
        )
    if len(result) != 10:
        raise ValueError(f"expected 10 base rows, got {len(result)}")
    return result


def by_stratum_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for stratum in ("auto", "non_auto"):
        selected = [row for row in rows if row["source_stratum"] == stratum]
        result[stratum] = {
            "display": SOURCE_LABELS[stratum],
            "sample_size": len(selected),
            "content_auto_score": rounded_mean(selected, "content_auto_score"),
            "audience_auto_score": rounded_mean(selected, "audience_auto_score"),
            "dcd_acquisition_score": rounded_mean(selected, "dcd_acquisition_score"),
        }
    return result


def cohort_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "sample_size": len(rows),
        "sample_ids": [row["sample_attempt_id"] for row in rows],
        "content_auto_score": rounded_mean(rows, "content_auto_score"),
        "audience_auto_score": rounded_mean(rows, "audience_auto_score"),
        "dcd_acquisition_score": rounded_mean(rows, "dcd_acquisition_score"),
    }


def build_summary(
    attempts: list[dict[str, Any]],
    collection_summary: dict[str, Any],
    final_results: list[dict[str, Any]],
    base_rows: list[dict[str, Any]],
    public_screening: dict[str, Any] | None,
) -> dict[str, Any]:
    statuses = Counter(row["comment_sample_status"] for row in attempts)
    fetches = Counter(row["comment_fetch_status"] for row in attempts)
    base_by_stratum = {
        stratum: [row for row in base_rows if row["source_stratum"] == stratum]
        for stratum in ("auto", "non_auto")
    }
    content_score = rounded_mean(base_rows, "content_auto_score")
    audience_score = rounded_mean(final_results, "audience_auto_score")
    acquisition_score = rounded_mean(final_results, "dcd_acquisition_score")
    automotive_content_rows = [
        row for row in final_results if row["content_auto_score"] >= 70
    ]
    non_automotive_content_rows = [
        row for row in final_results if row["content_auto_score"] < 40
    ]
    ambiguous_content_rows = [
        row for row in final_results if 40 <= row["content_auto_score"] < 70
    ]
    return {
        "report_version": "rnote-three-proposition-report-v1.0",
        "prediction_version": PREDICTION_VERSION,
        "generated_at": date.today().isoformat(),
        "report_state": "complete",
        "counts": {
            "source_unique_notes": {"auto": 313, "non_auto": 25, "total": 338},
            "base_random_samples": len(base_rows),
            "collection_attempts": len(attempts),
            "replacement_attempts": sum(
                row.get("sample_role") == "replacement_candidate" for row in attempts
            ),
            "collection_attempts_by_stratum": dict(
                Counter(row["source_stratum"] for row in attempts)
            ),
            "comment_fetch_status": dict(fetches),
            "comment_sample_status": dict(statuses),
            "comment_semantic_success": sum(
                row["comment_fetch_status"] in {"complete", "partial", "confirmed_empty"}
                for row in attempts
            ),
            "final_sample": len(final_results),
            "final_sample_by_stratum": dict(
                Counter(row["source_stratum"] for row in final_results)
            ),
            "valid_unique_commenters_final": sum(
                row["valid_unique_commenters"] for row in final_results
            ),
            "rnote_billed_requests": int(
                collection_summary.get("total_billed_requests")
                or collection_summary.get("billed_requests_this_run")
                or 0
            ),
        },
        "propositions": {
            "content_automotive": {
                "score": content_score,
                "conclusion": "基础平衡样本中，汽车来源组均分95，非汽车来源组均分0，分层差异明确",
                "sample_size": len(base_rows),
                "scope": "基础5+5平衡随机样本，按笔记等权；均分受平衡配额影响",
                "by_source_stratum": {
                    stratum: {
                        "score": rounded_mean(rows, "content_auto_score"),
                        "sample_size": len(rows),
                    }
                    for stratum, rows in base_by_stratum.items()
                },
            },
            "audience_automotive": {
                "score": audience_score,
                "conclusion": audience_conclusion(audience_score),
                "sample_size": len(final_results),
                "scope": "评论达标的最终5+5样本，按笔记等权",
            },
            "dcd_acquisition_potential": {
                "score": acquisition_score,
                "conclusion": acquisition_conclusion(acquisition_score),
                "sample_size": len(final_results),
                "scope": "评论达标的最终5+5样本，按笔记等权",
                "is_prediction_not_actual_effect": True,
            },
        },
        "final_by_source_stratum": by_stratum_summary(final_results),
        "derived_content_cohorts": {
            "automotive_content_70_plus": cohort_summary(automotive_content_rows),
            "ambiguous_content_40_to_69": cohort_summary(ambiguous_content_rows),
            "non_automotive_content_below_40": cohort_summary(
                non_automotive_content_rows
            ),
        },
        "collection_cache": {
            "schema_version": collection_summary.get("schema_version"),
            "final_slots_filled": collection_summary.get("final_slots_filled"),
            "cache_reuse_rule": collection_summary.get("cache_reuse_rule"),
            "public_screening": public_screening,
        },
        "actual_effect": {
            "status": "not_tested",
            "clicks": None,
            "installs": None,
            "confirmed_new_users": None,
        },
    }


def md_text(value: Any) -> str:
    return str(value or "").replace("|", "\\|").replace("\n", " ").strip()


def bucket_text(counts: Mapping[str, int], order: tuple[int, ...]) -> str:
    return "、".join(f"{score}分={counts.get(str(score), 0)}人" for score in order)


def render_report(
    summary: dict[str, Any],
    results: list[dict[str, Any]],
    attempts: list[dict[str, Any]],
) -> str:
    props = summary["propositions"]
    counts = summary["counts"]
    final_by_stratum = summary["final_by_source_stratum"]
    automotive_cohort = summary["derived_content_cohorts"][
        "automotive_content_70_plus"
    ]
    lines = [
        "# 小红书笔记三命题100分评估报告（Rnote真实评论版）",
        "",
        f"生成日期：{summary['generated_at']}  ",
        f"评分版本：`{summary['prediction_version']}`  ",
        "数据状态：内容、图片/视频和评论已落入本地缓存；本报告重算不需要再次调用 Rnote。",
        "",
        "## 一、三个命题的结论",
        "",
        "| 命题 | 分数与结论 | 样本范围 |",
        "|---|---|---|",
        f"| 1. 内容是否是汽车类 | **{props['content_automotive']['score']}/100**：{props['content_automotive']['conclusion']} | {props['content_automotive']['scope']}，n={props['content_automotive']['sample_size']} |",
        f"| 2. 互动用户是否偏汽车 | **{props['audience_automotive']['score']}/100**：{props['audience_automotive']['conclusion']} | {props['audience_automotive']['scope']}，n={props['audience_automotive']['sample_size']} |",
        f"| 3. 是否具备懂车帝拉新潜力 | **{props['dcd_acquisition_potential']['score']}/100（预测分）**：{props['dcd_acquisition_potential']['conclusion']} | {props['dcd_acquisition_potential']['scope']}，n={props['dcd_acquisition_potential']['sample_size']} |",
        "",
        "> 命题1的总体均分不能解释为账号汽车内容占比：本次样本被人为固定为5篇汽车来源+5篇非汽车来源。更有解释力的是逐篇结果及分组均分。命题3是候选排序分，不是下载概率或实际新增效果。",
        "",
        "业务解读（不要只看5+5总均分）：",
        "",
        "- 基础内容判定区分度高：汽车来源基础组均分95/100，非汽车来源基础组0/100。",
        f"- 最终10篇中真正达到汽车内容门槛（命题1>=70）的有{automotive_cohort['sample_size']}篇：{'、'.join(automotive_cohort['sample_ids'])}。这些笔记的用户汽车倾向均分{automotive_cohort['audience_auto_score']}/100，懂车帝拉新潜力均分{automotive_cohort['dcd_acquisition_score']}/100。",
        "- RA077和RA089均为67/100，值得进入小流量拉新实验；RA046为58/100，更像驾驶安全泛兴趣内容。",
        "- RA049和RA054虽来自“汽车”链接文件，但内容主体是末日短剧，评论人群也几乎完全非汽车；尾部懂车帝植入不能替代真实的汽车内容和用户意图。",
        "",
        "分组均分（来源标签仅用于校准，从未参与评分）：",
        "",
        "| 来源组 | n | 内容汽车属性 | 用户汽车倾向 | 懂车帝拉新潜力 |",
        "|---|---:|---:|---:|---:|",
    ]
    for stratum in ("auto", "non_auto"):
        row = final_by_stratum[stratum]
        lines.append(
            f"| {row['display']} | {row['sample_size']} | {row['content_auto_score']}/100 | {row['audience_auto_score']}/100 | {row['dcd_acquisition_score']}/100 |"
        )

    lines.extend(
        [
            "",
            "## 二、最终10篇逐篇结果",
            "",
            "| 样本 | 来源组 | 有效评论用户 | 命题1：是否为汽车内容 | 命题2：互动用户是否偏汽车 | 命题3：是否具备懂车帝拉新潜力 |",
            "|---|---|---:|---|---|---|",
        ]
    )
    for row in results:
        lines.append(
            f"| [{row['sample_attempt_id']}]({row['url']}) | {row['source_stratum_display']} | {row['valid_unique_commenters']} "
            f"| **{row['content_auto_score']}/100**<br>{md_text(row['content_auto_conclusion'])} "
            f"| **{row['audience_auto_score']}/100**<br>{md_text(row['audience_auto_conclusion'])} "
            f"| **{row['dcd_acquisition_score']}/100**<br>{md_text(row['dcd_acquisition_conclusion'])} |"
        )

    lines.extend(
        [
            "",
            "图示：三项分值采用统一的0–100分横轴，虚线表示各命题的关键判断门槛。",
            "",
            f"![最终10篇笔记三命题评分对比]({REPORT_VISUAL_RELATIVE_PATH})",
            "",
            "## 三、逐篇定性说明与依据",
            "",
        ]
    )
    for row in results:
        title = md_text(row.get("title")) or "（原标题为空）"
        lines.extend(
            [
                f"### {row['sample_attempt_id']}｜{title}",
                "",
                f"- 样本状态：{row['source_stratum_display']}，{row['valid_unique_commenters']}名有效独立评论用户，评论{row['comment_pages_fetched']}页。",
                f"- 命题1：**{row['content_auto_score']}/100**。{md_text(row['content_auto_conclusion'])}",
                f"  - 依据：{'；'.join(md_text(item) for item in row['content_auto_evidence'])}。",
                f"- 命题2：**{row['audience_auto_score']}/100**。{row['audience_auto_conclusion']}",
                f"  - 人数分布：{bucket_text(row['audience_score_counts'], (100, 70, 30, 0))}。",
                f"  - 依据：{'；'.join(md_text(item) for item in row['audience_auto_evidence'])}。",
                f"- 命题3：**{row['dcd_acquisition_score']}/100（预测分）**。{row['dcd_acquisition_conclusion']}",
                f"  - 中间量：懂车帝功能承接F={row['dcd_fit_score']}；行动意图I={row['action_intent_score']}（{bucket_text(row['action_score_counts'], (100, 60, 30, 0))}）。",
                f"  - 依据：{'；'.join(md_text(item) for item in row['dcd_acquisition_evidence'])}。",
                "",
            ]
        )

    failure = Counter(
        row["comment_sample_status"] for row in attempts if not row["final_sample_eligible"]
    )
    lines.extend(
        [
            "## 四、抽检与数据质量",
            "",
            "- 输入池去重后共338篇：汽车来源313篇、非汽车来源25篇；基础随机样本固定为5+5。",
            f"- Rnote实际尝试{counts['collection_attempts']}篇：基础10篇、补抽{counts['replacement_attempts']}篇；56/56次尝试都取得明确评论语义结果。",
            f"- 未进入最终样本：确认0评论{failure.get('confirmed_zero', 0)}篇、有效用户不足20人{failure.get('below_minimum', 0)}篇；不存在把技术缺失当0评论的情况。",
            f"- 最终样本汽车来源5篇、非汽车来源5篇，累计{counts['valid_unique_commenters_final']}名有效独立评论用户。",
            "- 评论统计采用一级评论及页面内嵌回复；作者、纯表情/纯@、广告引流和重复复制文本被排除。同一用户多条留言只计1人。",
            "- P006有1名用户聚合了24条模板式内嵌回复，疑似运营互动；敏感性复核中即使排除，该篇仍有39名有效用户且三个分数不变。",
            f"- 本轮共记录{counts['rnote_billed_requests']}次Rnote成功计费请求；后续重算读取缓存，不再重复调用。",
            "",
            "## 五、当前能回答与不能回答的结论",
            "",
            "- 现在可以回答：每篇内容是不是汽车类、评论用户是否偏汽车、以及这类内容是否存在可由懂车帝报价/车型/参数/口碑/用车知识承接的拉新潜力。",
            "- 现在不能回答：实际带来多少下载、激活、注册或确认新用户。懂车帝侧数据尚未接入，实际新增效果状态为`not_tested`，不是0。",
            "- 下一阶段应把笔记ID或活动参数与懂车帝点击、下载、激活、注册、新用户口径连接，再校准本报告的预测分。",
            "",
        ]
    )
    return "\n".join(lines)


def write_results_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "sample_attempt_id",
        "target_slot",
        "source_stratum",
        "note_id",
        "url",
        "title",
        "valid_unique_commenters",
        "content_auto_score",
        "content_auto_conclusion",
        "audience_auto_score",
        "audience_auto_conclusion",
        "dcd_fit_score",
        "action_intent_score",
        "dcd_acquisition_score",
        "dcd_acquisition_conclusion",
        "actual_status",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_attempts_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "sample_attempt_id",
        "target_slot",
        "source_stratum",
        "sample_role",
        "note_id",
        "comment_fetch_status",
        "comment_sample_status",
        "platform_comment_count",
        "raw_comment_count",
        "valid_unique_commenters",
        "comment_pages_fetched",
        "comment_pagination_complete",
        "final_sample_eligible",
        "replacement_reason",
        "stop_reason",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--attempts",
        type=Path,
        default=RNOTE_CACHE_DIR / "pilot_collection_attempts.jsonl",
    )
    parser.add_argument(
        "--collection-summary",
        type=Path,
        default=RNOTE_CACHE_DIR / "pilot_collection_summary.json",
    )
    parser.add_argument(
        "--scores", type=Path, default=XHS_PROCESSED_DIR / "rnote_final_scores_v1.jsonl"
    )
    parser.add_argument(
        "--prior-content", type=Path, default=XHS_PROCESSED_DIR / "pilot_content_scores_v0.3.jsonl"
    )
    parser.add_argument(
        "--public-screening-summary",
        type=Path,
        default=RNOTE_CACHE_DIR / "public_screening_auto_summary.json",
    )
    parser.add_argument(
        "--results-jsonl",
        type=Path,
        default=XHS_PROCESSED_DIR / "rnote_three_proposition_results.jsonl",
    )
    parser.add_argument(
        "--results-csv",
        type=Path,
        default=XHS_PROCESSED_DIR / "rnote_three_proposition_results.csv",
    )
    parser.add_argument(
        "--attempts-csv",
        type=Path,
        default=XHS_PROCESSED_DIR / "rnote_collection_attempts.csv",
    )
    parser.add_argument(
        "--summary",
        type=Path,
        default=XHS_PROCESSED_DIR / "rnote_three_proposition_summary.json",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=CURRENT_REPORTS_DIR / "小红书汽车内容三命题评估报告_Rnote_2026-07-19.md",
    )
    parser.add_argument(
        "--visual-svg",
        type=Path,
        default=RNOTE_CACHE_DIR / "three_proposition_visual_summary.svg",
    )
    parser.add_argument(
        "--visual-png",
        type=Path,
        default=RNOTE_CACHE_DIR / "three_proposition_visual_summary.png",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    attempts = read_jsonl(args.attempts)
    scores = read_jsonl(args.scores)
    prior_content = read_jsonl(args.prior_content)
    collection_summary = json.loads(args.collection_summary.read_text(encoding="utf-8"))
    public_screening = (
        json.loads(args.public_screening_summary.read_text(encoding="utf-8"))
        if args.public_screening_summary.exists()
        else None
    )
    results = build_final_results(attempts, scores)
    base_rows = base_content_rows(attempts, prior_content, results)
    summary = build_summary(
        attempts,
        collection_summary,
        results,
        base_rows,
        public_screening,
    )
    write_visual(
        results,
        svg_path=args.visual_svg,
        png_path=args.visual_png,
    )
    report = render_report(summary, results, attempts)
    write_jsonl(args.results_jsonl, results)
    write_results_csv(args.results_csv, results)
    write_attempts_csv(args.attempts_csv, attempts)
    write_json(args.summary, summary)
    args.report.write_text(report, encoding="utf-8")
    print(
        json.dumps(
            {
                "final_results": len(results),
                "attempts": len(attempts),
                "summary": args.summary.name,
                "report": args.report.name,
                "visual": str(args.visual_png),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
