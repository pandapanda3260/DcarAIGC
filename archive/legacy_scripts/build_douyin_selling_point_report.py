#!/usr/bin/env python3
"""Validate Douyin selling-point labels and build reusable report artifacts."""

from __future__ import annotations

import csv
import datetime as dt
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
POSTS_PATH = ROOT / "douyin_cache" / "douyin_posts.jsonl"
LABELS_PATH = ROOT / "douyin_selling_point_labels_2026-08-01.jsonl"
TAXONOMY_PATH = ROOT / "business_selling_points_v1.json"
SUMMARY_PATH = ROOT / "douyin_selling_point_summary_2026-08-01.json"
CSV_PATH = ROOT / "douyin_selling_point_results_2026-08-01.csv"
REPORT_PATH = ROOT / "抖音最近作品业务卖点标注报告_2026-08-01.md"


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def pct(numerator: int, denominator: int) -> float | None:
    return round(numerator * 100 / denominator, 1) if denominator else None


def pct_text(value: float | None) -> str:
    return "不适用" if value is None else f"{value:.1f}%"


def score_qualitative(score: int) -> str:
    if score >= 90:
        return "强匹配"
    if score >= 75:
        return "明确匹配"
    if score >= 60:
        return "弱匹配待复核"
    return "无明确匹配"


def esc(value: Any) -> str:
    return str(value if value is not None else "").replace("|", "\\|").replace("\n", " ")


def main() -> None:
    posts = load_jsonl(POSTS_PATH)
    labels = load_jsonl(LABELS_PATH)
    taxonomy = json.loads(TAXONOMY_PATH.read_text(encoding="utf-8"))

    point_map: dict[str, dict[str, Any]] = {}
    for line in taxonomy["business_lines"]:
        for point in line["selling_points"]:
            point_map[point["id"]] = {
                **point,
                "business_line_id": line["id"],
                "business_line": line["name"],
            }
    point_map["NO_MATCH"] = {
        "id": "NO_MATCH",
        "label": "无明确业务卖点",
        "tier": "none",
        "status": "confirmed",
        "business_line_id": "NONE",
        "business_line": "无法判断",
    }

    post_map = {row["aweme_id"]: row for row in posts}
    label_map = {row["aweme_id"]: row for row in labels}
    if len(post_map) != len(posts):
        raise ValueError("作品数据存在重复 aweme_id")
    if len(label_map) != len(labels):
        raise ValueError("标签数据存在重复 aweme_id")
    missing = sorted(set(post_map) - set(label_map))
    extra = sorted(set(label_map) - set(post_map))
    if missing or extra:
        raise ValueError(f"标签与作品不一一对应：missing={missing}, extra={extra}")

    merged: list[dict[str, Any]] = []
    for post in posts:
        label = label_map[post["aweme_id"]]
        if label.get("uid") != post.get("uid"):
            raise ValueError(f"{post['aweme_id']} 的 UID 不一致")
        point_id = label.get("primary_selling_point")
        if point_id not in point_map:
            raise ValueError(f"{post['aweme_id']} 的卖点 ID 无效：{point_id}")
        score = label.get("match_score")
        if not isinstance(score, int) or not 0 <= score <= 100:
            raise ValueError(f"{post['aweme_id']} 的 match_score 无效")
        expected = score_qualitative(score)
        if label.get("qualitative") != expected:
            raise ValueError(
                f"{post['aweme_id']} 定性应为 {expected}，实际为 {label.get('qualitative')}"
            )
        if score < 60 and point_id != "NO_MATCH":
            raise ValueError(f"{post['aweme_id']} 低于60分时主标签必须为 NO_MATCH")
        if score >= 75 and point_id == "NO_MATCH":
            raise ValueError(f"{post['aweme_id']} 达到自动纳入阈值却为 NO_MATCH")
        included = point_id != "NO_MATCH" and (
            score >= 75 or label.get("review_status") == "manual_approved"
        )
        if 60 <= score < 75 and label.get("review_status") not in {
            "manual_pending", "manual_approved", "manual_rejected"
        }:
            raise ValueError(f"{post['aweme_id']} 弱匹配内容缺少人工复核状态")
        point = point_map[point_id]
        merged.append({
            **post,
            **label,
            "business_line": point["business_line"],
            "selling_point_name": point["label"],
            "selling_point_tier": point["tier"],
            "is_core": included and point["tier"] == "core",
            "included_in_metrics": included,
        })

    def metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
        total = len(rows)
        clear = sum(row["included_in_metrics"] for row in rows)
        core = sum(row["is_core"] for row in rows)
        pending = sum(row.get("review_status") == "manual_pending" for row in rows)
        return {
            "total_posts": total,
            "clear_selling_point_posts": clear,
            "core_posts": core,
            "manual_review_pending": pending,
            "selling_point_coverage_pct": pct(clear, total),
            "core_share_all_posts_pct": pct(core, total),
            "core_share_labeled_posts_pct": pct(core, clear),
        }

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in merged:
        grouped[row["uid"]].append(row)
    account_summaries: list[dict[str, Any]] = []
    for uid, rows in grouped.items():
        rows.sort(key=lambda item: item["create_time"], reverse=True)
        account_summaries.append({
            "uid": uid,
            "account_name": rows[0]["account_name"],
            "newest_post_time": rows[0]["create_time_cn"],
            "oldest_post_time": rows[-1]["create_time_cn"],
            **metrics(rows),
        })
    account_summaries.sort(key=lambda item: item["uid"])

    distribution = Counter(
        row["primary_selling_point"]
        for row in merged
        if row["included_in_metrics"]
    )
    overall = metrics(merged)
    overall["target_core_share_pct"] = [60, 70]
    overall["target_status"] = (
        "below_target"
        if (overall["core_share_all_posts_pct"] or 0) < 60
        else "above_target"
        if (overall["core_share_all_posts_pct"] or 0) > 70
        else "within_target"
    )
    summary = {
        "schema_version": "1.0",
        "taxonomy_version": taxonomy["taxonomy_version"],
        "generated_at": dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).isoformat(),
        "sample_scope": "四个UID各自最近一页公开作品；不是统一自然月",
        "overall": overall,
        "accounts": account_summaries,
        "selling_point_distribution": [
            {
                "selling_point_id": point_id,
                "selling_point_name": point_map[point_id]["label"],
                "tier": point_map[point_id]["tier"],
                "count": count,
                "share_all_posts_pct": pct(count, len(merged)),
            }
            for point_id, count in sorted(distribution.items())
        ],
        "method_notes": [
            "每条内容只按主卖点参与核心占比统计。",
            "卖点匹配度达到75分，或60-74分人工复核通过，才进入卖点统计。",
            "本轮按发布条数计算，尚未做跨账号创意去重。",
            "评论不参与卖点主标签判断。",
        ],
    }
    SUMMARY_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    fields = [
        "uid", "account_name", "aweme_id", "create_time_cn", "share_url", "desc",
        "primary_selling_point", "business_line", "selling_point_name",
        "selling_point_tier", "is_core", "match_score", "qualitative",
        "review_status", "evidence_source", "evidence", "included_in_metrics",
    ]
    with CSV_PATH.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(merged)

    md: list[str] = [
        "# 抖音最近作品业务卖点标注报告",
        "",
        "日期：2026-08-01  ",
        "标签体系：v1.0  ",
        "样本：4个抖音UID最近一页公开作品，共60条",
        "",
        "## 一、结论",
        "",
        f"本轮样本共 **{overall['total_posts']}** 条，其中明确对应业务卖点 **{overall['clear_selling_point_posts']}** 条，卖点覆盖率 **{pct_text(overall['selling_point_coverage_pct'])}**；核心卖点内容 **{overall['core_posts']}** 条，按全部发布内容计算的核心卖点占比为 **{pct_text(overall['core_share_all_posts_pct'])}**。",
        "",
        "该结果只代表四个账号的“最近一页作品样本”，不能直接外推几百个账号的完整生产占比。正式统计应统一为最近30个自然日，并完成翻页采集与跨账号创意去重。",
        "",
        "## 二、总体指标",
        "",
        "| 指标 | 结果 | 说明 |",
        "|---|---:|---|",
        f"| 样本发布内容 | {overall['total_posts']}条 | 每次发布计1条 |",
        f"| 明确卖点内容 | {overall['clear_selling_point_posts']}条 | 匹配度≥75，或人工复核通过 |",
        f"| 卖点覆盖率 | {pct_text(overall['selling_point_coverage_pct'])} | 明确卖点内容/全部内容 |",
        f"| 核心卖点内容 | {overall['core_posts']}条 | 只按主标签是否核心计算 |",
        f"| 核心卖点占比 | {pct_text(overall['core_share_all_posts_pct'])} | 核心内容/全部内容；目标60%–70% |",
        f"| 已覆盖内容中的核心占比 | {pct_text(overall['core_share_labeled_posts_pct'])} | 核心内容/明确卖点内容 |",
        f"| 待人工复核 | {overall['manual_review_pending']}条 | 60–74分，未计入当前比例 |",
        "",
        "## 三、分账号结果",
        "",
        "| 账号 | UID | 样本区间 | 条数 | 明确卖点 | 卖点覆盖率 | 核心条数 | 核心/全部 |",
        "|---|---|---|---:|---:|---:|---:|---:|",
    ]
    for account in account_summaries:
        period = f"{account['oldest_post_time'][:10]} 至 {account['newest_post_time'][:10]}"
        md.append(
            f"| {esc(account['account_name'])} | {account['uid']} | {period} | "
            f"{account['total_posts']} | {account['clear_selling_point_posts']} | "
            f"{pct_text(account['selling_point_coverage_pct'])} | {account['core_posts']} | "
            f"{pct_text(account['core_share_all_posts_pct'])} |"
        )

    md.extend([
        "",
        "## 四、卖点分布",
        "",
        "| 卖点 | 层级 | 条数 | 占全部内容 |",
        "|---|---|---:|---:|",
    ])
    for item in summary["selling_point_distribution"]:
        md.append(
            f"| {item['selling_point_id']} {esc(item['selling_point_name'])} | "
            f"{'核心' if item['tier'] == 'core' else '其他'} | {item['count']} | "
            f"{pct_text(item['share_all_posts_pct'])} |"
        )
    if not summary["selling_point_distribution"]:
        md.append("| 无 | - | 0 | 0.0% |")

    md.extend([
        "",
        "## 五、逐条标注",
        "",
        "| 账号 | 发布时间 | 作品 | 命题4 主卖点 | 层级 | 匹配度与定性 | 判断证据 |",
        "|---|---|---|---|---|---|---|",
    ])
    for row in sorted(merged, key=lambda item: (item["uid"], -item["create_time"])):
        link = f"[{row['aweme_id']}]({row['share_url']})"
        tier = "核心" if row["selling_point_tier"] == "core" else (
            "其他" if row["selling_point_tier"] == "other" else "无"
        )
        md.append(
            f"| {esc(row['account_name'])} | {row['create_time_cn'][:10]} | {link} | "
            f"{row['primary_selling_point']} {esc(row['selling_point_name'])} | {tier} | "
            f"{row['match_score']}/100，{row['qualitative']} | {esc(row['evidence'])} |"
        )

    md.extend([
        "",
        "## 六、口径与限制",
        "",
        "- 本报告判断的是内容设计对应的业务卖点，不是泛汽车题材分类；仅出现“汽车”“懂车帝”等词不会自动获得卖点标签。",
        "- 评论可用于后续验证用户理解和互动受众，但不决定创作者表达的主卖点。",
        "- 本轮为每个账号接口返回的最近一页，账号活跃度不同，所以时间跨度不一致。",
        "- 当前是发布条数口径；批量生产治理还应增加视频、音频、字幕相似度去重后的创意口径。",
        "- 抖音公开网页接口属于非正式、可能变更的技术路径，应保留缓存并准备付费服务商作为生产兜底。",
        "",
    ])
    REPORT_PATH.write_text("\n".join(md), encoding="utf-8")
    print(f"wrote {SUMMARY_PATH.name}")
    print(f"wrote {CSV_PATH.name}")
    print(f"wrote {REPORT_PATH.name}")


if __name__ == "__main__":
    main()
