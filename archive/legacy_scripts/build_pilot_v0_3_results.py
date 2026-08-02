#!/usr/bin/env python3
"""Build the v0.3 three-proposition attempt ledger and human report."""

from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from pathlib import Path

from three_proposition_scoring import content_auto_score


ROOT = Path(__file__).resolve().parent


def read_jsonl(path: Path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def optional_int(value):
    if value in {None, ""}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def main() -> int:
    contents = {row["pilot_id"]: row for row in read_jsonl(ROOT / "pilot_public_content.jsonl")}
    scores = read_jsonl(ROOT / "pilot_content_scores_v0.3.jsonl")
    with (ROOT / "pilot_sample_10_labels.csv").open(encoding="utf-8-sig", newline="") as handle:
        labels = {row["pilot_id"]: row for row in csv.DictReader(handle)}
    metadata = json.loads((ROOT / "pilot_sample_10_metadata.json").read_text(encoding="utf-8"))

    stratum_positions = defaultdict(int)
    rows = []
    for score in scores:
        calculated_score, calculated_adjustment = content_auto_score(
            text_score=score["content_text_score"],
            text_reliability=score["content_text_reliability"],
            media_score=score["content_media_score"],
            media_reliability=score["content_media_reliability"],
        )
        if calculated_score != score["content_auto_score"]:
            raise ValueError(
                f"{score['pilot_id']} content score mismatch: "
                f"stored={score['content_auto_score']} calculated={calculated_score}"
            )
        if calculated_adjustment != score["content_comment_adjustment"]:
            raise ValueError(f"{score['pilot_id']} comment adjustment mismatch")
        pilot_id = score["pilot_id"]
        content = contents[pilot_id]
        stratum = labels[pilot_id]["source_label"]
        stratum_positions[stratum] += 1
        platform_count = optional_int(content.get("interactions", {}).get("commentCount"))
        rows.append(
            {
                "sample_attempt_id": pilot_id,
                "target_slot": f"{stratum}_{stratum_positions[stratum]:02d}",
                "sampling_seed": metadata["seed"],
                "source_stratum": stratum,
                "sample_role": "base_random_sample",
                "sample_attempt_status": "content_scored_awaiting_comments",
                "final_sample_eligible": False,
                "replacement_reason": "comment_not_retrieved",
                "replacement_sample_id": None,
                "note_id": content["note_id"],
                "url": content["url"],
                "comment_fetch_status": "not_retrieved",
                "comment_fetch_error_code": None,
                "platform_comment_count": platform_count,
                "raw_comment_count": 0,
                "valid_unique_commenters": None,
                "comment_sample_status": "technical_missing",
                "comment_pages_fetched": 0,
                "comment_pagination_complete": None,
                "content_text_score": score["content_text_score"],
                "content_text_reliability": score["content_text_reliability"],
                "content_media_score": score["content_media_score"],
                "content_media_reliability": score["content_media_reliability"],
                "content_comment_adjustment": score["content_comment_adjustment"],
                "content_auto_score": score["content_auto_score"],
                "content_auto_conclusion": score["content_auto_conclusion"],
                "content_auto_evidence": score["content_auto_evidence"],
                "content_subcategory": score["content_subcategory"],
                "audience_score_counts": None,
                "audience_auto_score": None,
                "audience_auto_conclusion": None,
                "audience_auto_evidence": None,
                "dcd_fit_score": None,
                "action_intent_score": None,
                "dcd_acquisition_score": None,
                "dcd_acquisition_conclusion": None,
                "dcd_acquisition_evidence": None,
                "prediction_version": score["scoring_version"],
                "prediction_limitations": (
                    "comment text was not retrieved; propositions 2 and 3 were not scored"
                ),
                "human_review_status": "pending",
                "human_content_score": None,
                "human_audience_score": None,
                "human_acquisition_score": None,
                "actual_status": "not_tested",
                "actual_clicks": None,
                "actual_installs": None,
                "actual_confirmed_new_users": None,
            }
        )

    json_path = ROOT / "pilot_three_proposition_attempts_v0.3.jsonl"
    json_path.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )

    csv_rows = []
    for row in rows:
        item = dict(row)
        for key in ("content_auto_evidence", "audience_score_counts", "audience_auto_evidence", "dcd_acquisition_evidence"):
            value = item[key]
            item[key] = "" if value is None else json.dumps(value, ensure_ascii=False)
        csv_rows.append(item)
    csv_path = ROOT / "pilot_three_proposition_attempts_v0.3.csv"
    with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(csv_rows[0]))
        writer.writeheader()
        writer.writerows(csv_rows)

    source_scores = defaultdict(list)
    for row in rows:
        source_scores[row["source_stratum"]].append(row["content_auto_score"])
    content_average = sum(row["content_auto_score"] for row in rows) / len(rows)
    platform_known = sum(row["platform_comment_count"] is not None for row in rows)
    platform_ge_20 = sum(
        row["platform_comment_count"] is not None and row["platform_comment_count"] >= 20
        for row in rows
    )
    report = [
        "# 10篇基础随机样本：三命题100分口径审计",
        "",
        "版本：`three-proposition-v1.0 / data-v0.3`  ",
        "日期：2026-07-19",
        "",
        "## 一、当前能得出的结果",
        "",
        "- 命题1“内容汽车属性”已完成10/10评分。",
        "- 命题2“互动用户汽车倾向”未评分；当前是评论采集通道未取得正文，不是真实0评论。",
        "- 命题3“懂车帝用户拉新潜力”未评分；正式公式需要命题2及评论中的行动意图。",
        "- 当前10篇全部保留为基础随机样本，不会被删除；评论通道恢复后，少于20名有效独立评论用户的笔记再启动同层补抽。",
        "",
        "不能把命题2、3填成0分或中性50分；那会把技术缺失伪装成业务结果。",
        "",
        "## 二、当前10篇逐篇状态",
        "",
        "| ID | 命题1：内容汽车属性 | 定性结论 | 页面评论计数 | 命题2 | 命题3 | 抽检处理 |",
        "|---|---:|---|---:|---:|---:|---|",
    ]
    for row in rows:
        platform = "未知" if row["platform_comment_count"] is None else str(row["platform_comment_count"])
        report.append(
            f"| {row['sample_attempt_id']} | {row['content_auto_score']}/100 | "
            f"{row['content_auto_conclusion']} | {platform} | — | — | "
            "保留基础记录；等待评论通道 |"
        )

    report.extend(
        [
            "",
            "## 三、汇总说明",
            "",
            f"- 10篇内容汽车属性等权平均：`{content_average:.1f}/100`。该样本刻意按5+5平衡，不能当作账号全部笔记的自然占比。",
            f"- 汽车来源预设组内容分均值：`{sum(source_scores['auto']) / len(source_scores['auto']):.1f}/100`。",
            f"- 非汽车来源预设组内容分均值：`{sum(source_scores['non_auto']) / len(source_scores['non_auto']):.1f}/100`。来源分组仍是预设标签，不是独立人工金标。",
            f"- 页面评论计数可读：`{platform_known}/10`；页面计数至少20条：`{platform_ge_20}/10`。页面计数只用于决定采集优先级，不能替代评论正文和有效独立用户数。",
            "",
            "## 四、评论通道恢复后的抽检动作",
            "",
            "1. 先测试1篇图文和1篇视频，确认评论正文、用户去重和分页字段。",
            "2. 每篇至少20名有效独立评论用户，优先取30名，最多纳入50名。",
            "3. 页面有评论但正文仍为0：重试2次并切换通道；仍失败则记技术缺失并同层补抽。",
            "4. 成功取完评论但有效独立用户少于20：保留原基础记录，并从预生成的同层随机队列补抽。",
            "5. 最终凑齐5+5篇可评分样本后，逐篇输出命题1、命题2、命题3三个整数分和定性说明。",
            "",
            "已经生成20篇汽车来源组、20篇非汽车来源组的隔离替补队列，但在评论采集通道修复前不消费该队列。",
            "",
        ]
    )
    report_path = ROOT / "pilot_three_proposition_report_v0.3.md"
    report_path.write_text("\n".join(report), encoding="utf-8")

    summary = {
        "attempted_base_notes": len(rows),
        "content_scored": len(rows),
        "comment_text_retrieved": 0,
        "final_three_proposition_eligible": 0,
        "content_score_average_balanced_sample": round(content_average, 1),
        "platform_comment_count_known": platform_known,
        "platform_comment_count_at_least_20": platform_ge_20,
        "source_counts": dict(Counter(row["source_stratum"] for row in rows)),
        "interpretation": "current sample is a content-scored attempt ledger, not the final three-proposition sample",
    }
    (ROOT / "pilot_three_proposition_summary_v0.3.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
