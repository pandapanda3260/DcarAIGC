#!/usr/bin/env python3
"""Generate the final v3 report, caption-baseline comparison, and QA queue."""

from __future__ import annotations

import csv
import json
import random
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
LABELS = ROOT / "douyin_selling_point_labels_v3_video_2026-08-01.jsonl"
SUMMARY = ROOT / "douyin_selling_point_summary_v3_video_2026-08-01.json"
BASELINE = ROOT / "douyin_selling_point_labels_v3_caption_only_screening_2026-08-01.jsonl"
TAXONOMY = ROOT / "business_selling_points_v3_final.json"
REPORT = ROOT / "抖音30账号业务卖点终版报告_v3_视频证据_2026-08-01.md"
COMPARISON = ROOT / "douyin_caption_vs_video_comparison_v3_2026-08-01.json"
QA_CSV = ROOT / "抖音卖点终版质量复核清单_v3_2026-08-01.csv"


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def pct(n: int, d: int) -> float:
    return round(100 * n / d, 2) if d else 0.0


def short(text: str, n: int = 100) -> str:
    text = " ".join(text.split())
    return text if len(text) <= n else text[: n - 1] + "…"


def baseline_id(row: dict[str, Any]) -> str:
    if "screening_id" in row:
        return str(row.get("screening_id") or "")
    if row.get("official_included"):
        return str(row.get("official_primary_id") or "")
    if row.get("candidate_included"):
        return str(row.get("candidate_primary_id") or "")
    return ""


def compare(rows: list[dict[str, Any]], baseline_rows: list[dict[str, Any]]) -> dict[str, Any]:
    base = {str(row["aweme_id"]): row for row in baseline_rows}
    evaluable = [row for row in rows if row["evidence_level"] in {"V2", "V3"}]
    exact = binary = missed = overcalled = changed = 0
    examples: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in evaluable:
        old = baseline_id(base.get(row["aweme_id"], {}))
        new = row["primary_id"] if row["included"] else ""
        if old == new:
            exact += 1
        if bool(old) == bool(new):
            binary += 1
        if not old and new:
            missed += 1
            category = "caption_missed_video_found"
        elif old and not new:
            overcalled += 1
            category = "caption_overcalled_video_rejected"
        elif old and new and old != new:
            changed += 1
            category = "label_changed"
        else:
            continue
        examples[category].append(
            {
                "aweme_id": row["aweme_id"],
                "account_name": row["account_name"],
                "caption_label": old or "未命中",
                "video_label": new or "未命中",
                "score": row["score"],
                "caption": short(str(row.get("desc") or "")),
                "video_evidence": short(str(row.get("asr_text") or row.get("ocr_text") or row.get("visual_review_summary") or "")),
                "share_url": row["share_url"],
            }
        )
    for values in examples.values():
        values.sort(key=lambda item: item["score"], reverse=True)
    result = {
        "comparison_scope": len(evaluable),
        "note": "同一套v3终版标签下，仅标题/正文候选筛查与视频证据终判的方法一致性；不把标题筛查当作真值。",
        "exact_primary_label_agreement": exact,
        "exact_primary_label_agreement_pct": pct(exact, len(evaluable)),
        "binary_hit_agreement": binary,
        "binary_hit_agreement_pct": pct(binary, len(evaluable)),
        "caption_missed_video_found": missed,
        "caption_missed_video_found_pct": pct(missed, len(evaluable)),
        "caption_overcalled_video_rejected": overcalled,
        "caption_overcalled_video_rejected_pct": pct(overcalled, len(evaluable)),
        "both_hit_but_label_changed": changed,
        "both_hit_but_label_changed_pct": pct(changed, len(evaluable)),
        "examples": {key: values[:8] for key, values in examples.items()},
    }
    COMPARISON.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return result


def make_qa(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected: dict[str, tuple[dict[str, Any], set[str]]] = {}

    def add(row: dict[str, Any], reason: str) -> None:
        key = row["aweme_id"]
        if key not in selected:
            selected[key] = (row, set())
        selected[key][1].add(reason)

    for row in rows:
        if row["included"] and row["primary_tier"] == "core":
            add(row, "全部核心卖点命中")
        if row["pending"]:
            add(row, "全部60-74分待复核")
        if row["evidence_level"] in {"V0", "V1"}:
            add(row, "证据不完整/低文本画面复核")

    rng = random.Random(20260801)
    strata: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row["evidence_level"] not in {"V2", "V3"}:
            continue
        status = "included" if row["included"] else "pending" if row["pending"] else "no_match"
        strata[(row["quality_label"], status)].append(row)
    for (quality, status), values in strata.items():
        count = max(1, round(len(values) * 0.1))
        for row in rng.sample(values, min(count, len(values))):
            add(row, f"{quality}-{status}随机10%抽检")

    output = []
    for row, reasons in sorted(selected.values(), key=lambda pair: (pair[0]["account_sample_index"], pair[0]["aweme_id"])):
        output.append(
            {
                "账号序号": row["account_sample_index"],
                "账号类型": row["quality_label"],
                "账号": row["account_name"],
                "作品ID": row["aweme_id"],
                "作品链接": row["share_url"],
                "抽检原因": "；".join(sorted(reasons)),
                "证据完整度": row["evidence_level"],
                "主卖点ID": row["primary_id"],
                "主卖点": row["primary_label"],
                "分值": row["score"],
                "定性": row["qualitative"],
                "作品文案": row["desc"],
                "视频口播摘要": short(str(row.get("asr_text") or ""), 260),
                "视频OCR摘要": short(str(row.get("ocr_text") or ""), 180),
                "关键帧画面语义": row.get("visual_review_summary", ""),
                "平台承接证据": row.get("platform_linkage", ""),
                "模型判断说明": row["decision_reason"],
                "人工复核结论": "",
                "人工修正标签": "",
                "复核备注": "",
            }
        )
    with QA_CSV.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(output[0].keys()) if output else [])
        if output:
            writer.writeheader()
            writer.writerows(output)
    return output


def metric_line(metric: dict[str, Any]) -> str:
    return (
        f"{metric['included']}/{metric['final_evaluable']}（{metric['unified_coverage_pct']:.1f}%） | "
        f"{metric['core']}/{metric['total']}（{metric['core_share_all_publications_pct']:.1f}%） | "
        f"{metric['evidence_incomplete']}（{metric['evidence_incomplete_pct']:.1f}%）"
    )


def main() -> None:
    rows = read_jsonl(LABELS)
    summary = json.loads(SUMMARY.read_text(encoding="utf-8"))
    taxonomy = json.loads(TAXONOMY.read_text(encoding="utf-8"))
    comparison = compare(rows, read_jsonl(BASELINE))
    qa = make_qa(rows)
    pub = summary["publication_metrics"]
    dedup = summary["deduplicated_creative_metrics"]
    by_quality = summary["by_quality_label"]
    label_map = {item["id"]: item for item in taxonomy["labels"]}
    included = [row for row in rows if row["included"]]
    counts = Counter(row["primary_id"] for row in included)
    explicit = sum(row.get("platform_linkage", "").startswith("视频/正文明确") for row in included)
    contextual = len(included) - explicit

    lines = [
        "# 抖音30账号业务卖点终版报告 v3.0（视频证据版）",
        "",
        "日期：2026-08-01  ",
        "样本：30个账号、438条抖音作品  ",
        "证据：作品正文 + 414条视频ASR/关键帧OCR + 24条图文原图OCR + 低文本内容画面复核  ",
        "标签：二手车、新车、AI小懂、媒体内容与社区统一标签体系；C1–C4已转为正式其他卖点。",
        "",
        "## 一、核心结论",
        "",
        f"- 终版可评估作品 **{pub['final_evaluable']}/{pub['total']}（{pub['final_evaluable_pct']:.1f}%）**；证据不完整 {pub['evidence_incomplete']} 条，不用标题强行下结论。",
        f"- 统一卖点覆盖 **{pub['included']}/{pub['final_evaluable']}（{pub['unified_coverage_pct']:.1f}%）**。",
        f"- 主卖点为核心的内容 **{pub['core']}/{pub['total']}（{pub['core_share_all_publications_pct']:.1f}%）**，与计划目标60%–70%相差 **{max(0, 60-pub['core_share_all_publications_pct']):.1f}–{max(0, 70-pub['core_share_all_publications_pct']):.1f} 个百分点**。",
        f"- 去重创意口径核心占比 **{dedup['core']}/{dedup['total']}（{dedup['core_share_all_publications_pct']:.1f}%）**，用于识别混剪重复是否放大了某类卖点。",
        f"- 正式命中中，视频/正文明确提及懂车帝或AI小懂 {explicit} 条；仅由自有账号内容服务承接 {contextual} 条。后者不能解读为用户已感知平台能力。",
        "",
        "## 二、只看标题是否准确",
        "",
        "不够准确。标题/文案适合做候选初筛，但会漏掉口播中的具体价格、功能、比较维度和结论，也会把夸张标题、历史价格、概念娱乐误当成业务卖点。以下是在同一套v3终版标签下，仅标题/正文候选筛查与视频终判的一致性：",
        "",
        "| 对比项 | 条数 | 占终版可评估作品 |",
        "|---|---:|---:|",
        f"| 主标签完全一致 | {comparison['exact_primary_label_agreement']} | {comparison['exact_primary_label_agreement_pct']:.1f}% |",
        f"| 是否命中（二分类）一致 | {comparison['binary_hit_agreement']} | {comparison['binary_hit_agreement_pct']:.1f}% |",
        f"| 文案未命中、视频发现卖点 | {comparison['caption_missed_video_found']} | {comparison['caption_missed_video_found_pct']:.1f}% |",
        f"| 文案命中、视频终判不计入 | {comparison['caption_overcalled_video_rejected']} | {comparison['caption_overcalled_video_rejected_pct']:.1f}% |",
        f"| 两者都命中但主标签改变 | {comparison['both_hit_but_label_changed']} | {comparison['both_hit_but_label_changed_pct']:.1f}% |",
        "",
        "> 这里衡量的是两种证据范围的一致性，不把标题筛查当作真值。终版以视频实际内容证据为准。",
        "",
        "## 三、终版统一卖点分布",
        "",
        "| ID | 层级 | 业务线 | 卖点 | 条数 | 占全部发布 |",
        "|---|---|---|---|---:|---:|",
    ]
    for point_id, count in counts.most_common():
        meta = label_map[point_id]
        lines.append(f"| {point_id} | {'核心' if meta['tier']=='core' else '其他'} | {meta['business_line']} | {meta['label']} | {count} | {pct(count, len(rows)):.1f}% |")
    lines.extend(
        [
            "",
            "## 四、三类账号对比",
            "",
            "| 账号类型 | 作品 | 统一卖点覆盖 | 核心/全部发布 | 待复核 | 证据不完整 |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for quality in ("精品IP号", "原创号", "混剪号"):
        m = by_quality[quality]
        lines.append(
            f"| {quality} | {m['total']} | {m['included']}/{m['final_evaluable']}（{m['unified_coverage_pct']:.1f}%） | "
            f"{m['core']}/{m['total']}（{m['core_share_all_publications_pct']:.1f}%） | {m['pending']} | {m['evidence_incomplete']} |"
        )
    lines.extend(
        [
            "",
            "## 五、终版判断规则",
            "",
            "1. 先识别视频的主要用户任务，再匹配“用户通过懂车帝能完成什么”，不从关键词直接跳到卖点。",
            "2. 证据优先级：ASR口播 > OCR字幕/画面文字 > 关键帧画面语义 > 正文 > 封面 > 话题标签 > 账号上下文。",
            "3. 100分由标签语义30、平台承接25、视频证据20、用户收益15、主叙事10组成。",
            "4. 90–100强匹配；75–89明确匹配；60–74待复核；低于60未命中。V1文案证据最高74分，不计入终版。",
            "5. 每条只有一个主卖点参与核心占比；最多两个次卖点不抬高核心比例。",
            "6. C1–C4已经并入统一标签，但属于其他卖点；核心集合仍为E1、E2、X1、X2、X3、M1、M2、M3。",
            "",
            "完整规则见《业务卖点判定标准与流程_v3_终版.md》。",
            "",
            "## 六、质量控制与复核",
            "",
            f"已生成 **{len(qa)} 条**质量复核清单，覆盖：全部核心卖点命中、全部60–74分、全部证据不完整作品，以及精品IP/原创/混剪各状态随机10%抽检。",
            "",
            f"本轮已按去重代表素材复核全部 **{dedup['core']} 个核心卖点创意簇**，并以40个边界测试固定标题、价格、界面导航、剧情词、社区和交易类误判规则。",
            "",
            "重点检查四类风险：标题与视频不一致、ASR车型/数字错误、OCR误识别、相同视频不同文案导致的重复创意。复核修正后必须全量重算，不允许只改汇总数字。",
            "",
            "## 七、业务使用建议",
            "",
            "- 生产排期以“全部发布内容中主卖点为核心”的比例为唯一60%–70%目标口径，不能用“已命中内容内部核心占比”。",
            "- 每条脚本立项时先选唯一主卖点ID，再写用户任务、懂车帝承接方式、用户收益和可验证证据。",
            "- 对只有自有账号归属、视频未明确提及平台能力的内容，另看“用户能否感知懂车帝”的承接率，避免把内容价值误当作产品认知。",
            "- 后续新增样本直接复用v3规则、原始视频缓存和逐条证据字段；规则升级必须递增版本并全量重跑。",
            "",
            "## 八、输出文件",
            "",
            "- `业务卖点判定标准与流程_v3_终版.md`：终版标签与规则；",
            "- `抖音438条内容卖点逐条结果_v3_视频终版_2026-08-01.csv`：逐条结果；",
            "- `douyin_caption_vs_video_comparison_v3_2026-08-01.json`：标题/文案初筛与视频终判差异；",
            "- `douyin_selling_point_labels_v3_caption_only_screening_2026-08-01.jsonl`：同规则、仅标题/正文的候选筛查基线；",
            "- `抖音卖点终版质量复核清单_v3_2026-08-01.csv`：质量复核队列；",
            "- `douyin_video_analysis_v3/`：已缓存视频、ASR、OCR、关键帧与采集结果。",
        ]
    )
    REPORT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"report": str(REPORT), "comparison": str(COMPARISON), "qa": str(QA_CSV), "qa_rows": len(qa)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
