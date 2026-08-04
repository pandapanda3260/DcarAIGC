#!/usr/bin/env python3
"""Build the human-readable 30-account selling-point report from v2 outputs."""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SUMMARY = ROOT / "douyin_selling_point_summary_v2_2026-08-01.json"
TAXONOMY = ROOT / "business_selling_points_v2.json"
OUT = ROOT / "抖音30账号业务卖点重标报告_v2_2026-08-01.md"


def pct(n: int, d: int) -> str:
    return f"{n * 100 / d:.1f}%" if d else "0.0%"


def main() -> None:
    summary = json.loads(SUMMARY.read_text(encoding="utf-8"))
    taxonomy = json.loads(TAXONOMY.read_text(encoding="utf-8"))
    pub = summary["publication_metrics"]
    dedup = summary["deduplicated_creative_metrics"]
    official = {item["id"]: item for item in taxonomy["official_points"]}
    candidates = {item["id"]: item for item in taxonomy["candidate_points"]}
    fallbacks = {item["id"]: item for item in taxonomy["fallbacks"]}

    lines: list[str] = []
    lines += [
        "# 抖音30账号业务卖点重标报告 v2",
        "",
        "日期：2026-08-01  ",
        "账号样本：30个有效账号（精品IP号8、原创号11、混剪号11）  ",
        "内容样本：每账号从接口最近公开作品页固定种子随机抽10–20条，共438条  ",
        "标签口径：卖点必须回答“用户通过懂车帝能完成什么任务”",
        "",
        "## 一、结论",
        "",
        f"扩大到 **30个账号、438条作品** 后，未命中正式卖点的内容仍占绝大多数。严格口径下，正式卖点内容 **{pub['official_included']}条（{pub['official_coverage_pct']:.1f}%）**，其中核心卖点 **{pub['official_core']}条（占全部内容 {pub['official_core_share_all_pct']:.1f}%）**。这远低于未来内容生产要求的60%–70%。",
        "",
        f"从正式未命中内容中，新提炼的4类候选卖点可以解释 **{pub['candidate_only']}条（占全部内容 {pub['candidate_explainable_share_pct']:.1f}%；占正式未命中内容 {pub['candidate_rescue_rate_among_official_unmatched_pct']:.1f}%）**。加入候选后总可解释率也只有 **{pub['expanded_coverage_pct']:.1f}%**，仍有 **{pub['expanded_unmatched']}条（{pub['expanded_unmatched_share_pct']:.1f}%）** 无法合理解释为懂车帝卖点。",
        "",
        "因此，问题不只是标签库过窄：**确实可以从无标签内容中补出C1–C4四个候选方向，但主要矛盾仍是大量内容在创作时没有设计“懂车帝任务承接”。** 只有懂车帝话题、账号归属、泛汽车知识、车型展示或降价噱头，都不能自动算卖点。",
        "",
        "## 二、修正后的判定口径",
        "",
        "| 证据级别 | 解释 | 统计处理 |",
        "|---|---|---|",
        "| A 明确承接 | 出现懂车帝具体模块、工具、服务、数据、入口或行动引导 | 计入正式卖点 |",
        "| B 能力演示 | 内容完整演示平台可持续提供的比较、价格、车源等能力 | 计入正式卖点 |",
        "| C 仅品牌露出 | 只有 `#懂车帝`、Logo或账号归属 | 不计入 |",
        "| D 仅内容价值 | 泛知识、单车夸赞、视觉娱乐或情绪故事 | 不计入 |",
        "",
        "正式卖点75分及以上计入；60–74分进入待复核。候选卖点只解释未命中内容，不进入正式核心卖点60%–70%的分母或分子。",
        "",
        "## 三、总体指标",
        "",
        "| 指标 | 发布条数口径 | 去重创意口径 | 解释 |",
        "|---|---:|---:|---|",
        f"| 样本数 | {pub['total']} | {dedup['total']} | 去话题、链接、标点后标准化文案完全相同视为同一创意 |",
        f"| 正式卖点内容 | {pub['official_included']}（{pub['official_coverage_pct']:.1f}%） | {dedup['official_included']}（{dedup['official_coverage_pct']:.1f}%） | 只含A/B证据且≥75分 |",
        f"| 正式核心卖点 | {pub['official_core']}（{pub['official_core_share_all_pct']:.1f}%） | {dedup['official_core']}（{dedup['official_core_share_all_pct']:.1f}%） | 目标为全部生产内容的60%–70% |",
        f"| 已命中内容中的核心占比 | {pub['official_core_share_within_official_pct']:.1f}% | {dedup['official_core_share_within_official_pct']:.1f}% | 不能替代“占全部内容”指标 |",
        f"| 正式弱匹配待复核 | {pub['official_pending_review']} | {dedup['official_pending_review']} | 与候选/未命中类别有交叉，不另行相加 |",
        f"| 候选卖点可解释 | {pub['candidate_only']}（{pub['candidate_explainable_share_pct']:.1f}%） | {dedup['candidate_only']}（{dedup['candidate_explainable_share_pct']:.1f}%） | 不计入正式核心比例 |",
        f"| 扩展后仍未命中 | {pub['expanded_unmatched']}（{pub['expanded_unmatched_share_pct']:.1f}%） | {dedup['expanded_unmatched']}（{dedup['expanded_unmatched_share_pct']:.1f}%） | 无稳定平台任务 |",
        "",
        f"438次发布对应366个标准化文案创意，存在 **{summary['duplicate_publications']}次重复发布、{summary['duplicate_creative_groups']}组重复创意**。发布口径与去重口径的结论接近，说明“正式卖点低”不是单纯由重复混剪造成的。",
        "",
        "## 四、正式卖点分布",
        "",
        "| ID | 层级 | 修正后的懂车帝用户任务 | 条数 | 占全部内容 |",
        "|---|---|---|---:|---:|",
    ]
    for point_id, count in sorted(pub["official_counts"].items()):
        item = official[point_id]
        tier = "核心" if item["tier"] == "core" else "其他"
        lines.append(f"| {point_id} | {tier} | {item['label']} | {count} | {pct(count, pub['total'])} |")

    zero_core = [item["id"] for item in taxonomy["official_points"] if item["tier"] == "core" and item["id"] not in pub["official_counts"]]
    lines += [
        "",
        f"核心卖点中，未出现明确样本的有：**{'、'.join(zero_core)}**。尤其AI小懂 M1/M2/M3 为0，说明当前样本几乎没有把AI小懂能力写入内容主叙事。",
        "",
        "### 弱匹配待复核",
        "",
        f"共有 **{pub['official_pending_review']}条**：" + "、".join(f"{k} {v}条" for k, v in pub["official_pending_counts"].items()) + "。其中多数是“降价了”但文案没有具体价格、数据来源或懂车帝查询入口。它们可能在视频口播/字幕中提供了证据，但在未做ASR/OCR前不能计入正式卖点。",
        "",
        "## 五、从无标签内容中提炼出的候选卖点",
        "",
        "| ID | 候选卖点 | 条数 | 占全部内容 | 建议 |",
        "|---|---|---:|---:|---|",
    ]
    candidate_recommendations = {
        "C1": "数量最大；若产品可承接知识搜索/问答，可升级，否则只作为内容主题",
        "C2": "与车型库、实拍和车型页核对承接入口后再决定",
        "C3": "已有懂车帝玩车社区线索，产品承接最清楚，优先确认",
        "C4": "样本很少，先扩充事实型资讯样本并增加准确性门槛",
    }
    for point_id, count in sorted(pub["candidate_counts"].items()):
        item = candidates[point_id]
        lines.append(
            f"| {point_id} | {item['label']} | {count} | {pct(count, pub['total'])} | {candidate_recommendations[point_id]} |"
        )

    lines += [
        "",
        "候选标签证明“无标签”中确实有一部分可以重新解释，但不能为了提高覆盖率直接转正。转正前至少要确认：懂车帝里是否存在稳定入口、用户是否真能完成该任务、内容能否明确引导到该入口。",
        "",
        "## 六、仍未命中的原因",
        "",
        "| 原因 | 条数 | 占全部内容 |",
        "|---|---:|---:|",
    ]
    for fallback_id, count in sorted(pub["fallback_counts"].items(), key=lambda item: (-item[1], item[0])):
        lines.append(f"| {fallbacks[fallback_id]['label']} | {count} | {pct(count, pub['total'])} |")

    lines += [
        "",
        "其中重复低信息、品牌露出、娱乐/故事三类合计占比较高。它们不是标签体系漏识别，而是内容本身没有可落地的平台任务；不建议继续从这些内容里硬提炼新卖点。",
        "",
        "## 七、按账号类型统计",
        "",
        "| 类型 | 账号数 | 作品数 | 正式卖点 | 正式核心/全部 | 候选可解释 | 扩展后仍未命中 |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    account_counts = {"精品IP号": 8, "原创号": 11, "混剪号": 11}
    for quality in ("精品IP号", "原创号", "混剪号"):
        item = summary["by_quality_label"][quality]
        lines.append(
            f"| {quality} | {account_counts[quality]} | {item['total']} | {item['official_included']}（{item['official_coverage_pct']:.1f}%） | "
            f"{item['official_core']}（{item['official_core_share_all_pct']:.1f}%） | {item['candidate_only']}（{item['candidate_explainable_share_pct']:.1f}%） | "
            f"{item['expanded_unmatched']}（{item['expanded_unmatched_share_pct']:.1f}%） |"
        )

    lines += [
        "",
        "精品IP号的正式覆盖率相对最高，但大量情绪故事、概念视觉使总体仍低；原创号最容易沉淀C1知识型候选；混剪号的重复、品牌露出和单车型展示最多，正式卖点覆盖最低。",
        "",
        "## 八、30个账号逐账号结果",
        "",
        "| 序号 | 类型 | 账号 | 作品 | 正式卖点 | 核心/全部 | 候选 | 仍未命中 |",
        "|---:|---|---|---:|---:|---:|---:|---:|",
    ]
    for account in summary["by_account"]:
        lines.append(
            f"| {account['account_sample_index']} | {account['quality_label']} | {account['account_name']} | {account['total']} | "
            f"{account['official_included']}（{account['official_coverage_pct']:.1f}%） | {account['official_core']}（{account['official_core_share_all_pct']:.1f}%） | "
            f"{account['candidate_only']} | {account['expanded_unmatched']} |"
        )

    lines += [
        "",
        "## 九、抽样与证据限制",
        "",
        "- 用户清单中可用于抽样的数字UID共180个：原创号56、精品IP号12、混剪号112。固定种子为 `20260801`。",
        "- 原计划每类10个账号；精品IP号中4个账号公开作品不足10条，因此最终为精品IP号8、原创号11、混剪号11。失败账号与递补过程已保存在抽样元数据中。",
        "- 每个账号只从接口最近返回的一页作品中随机抽10–20条，不是统一自然月；样本发布时间范围为2025-11-11至2026-07-25。账号活跃度差异可能影响账号间比较。",
        f"- 本轮批量判断使用作品文案、话题、账号上下文及已缓存元数据，未对438条视频全部做语音转写和画面OCR。因此{pub['official_pending_review']}条弱匹配不能直接升级。",
        "- 所有原始接口响应、账号页和作品数据已本地缓存；复跑统计不需要再次调用采集接口。",
        "",
        "## 十、建议的下一步",
        "",
        "1. **先锁定正式标签体系 v2**：业务确认C1–C4哪些能对应真实产品入口；未确认前不进入正式核心比例。",
        f"2. **做一次定向视频复核**：优先处理{pub['official_pending_review']}条弱匹配，再从正式未命中内容中按类型各抽10%做ASR/OCR审计，测算文案判定的漏标率。",
        "3. **把卖点前置到内容设计**：每条脚本必须明确写出“用户通过懂车帝完成什么”“使用哪个能力/入口”“得到什么结果”，而不是发布后再猜标签。",
        "4. **分别治理三类账号**：精品IP减少纯情绪/概念故事；原创号把C1知识内容接到产品能力；混剪号减少重复铺量并补充具体价格、来源和行动入口。",
        "5. **上线持续看板时同时保留三项指标**：正式核心/全部发布、正式核心/去重创意、候选标签占比；不能只看已打标签内容内部的核心比例。",
        "",
        "## 十一、输出文件",
        "",
        "- `业务卖点标签体系_v2_懂车帝用户任务.md`：修订后的正式与候选标签定义",
        "- `douyin_30_account_final_sample_2026-08-01.csv`：30个账号样本",
        "- `douyin_30_account_content_sample_2026-08-01.jsonl`：438条原始内容样本",
        "- `抖音438条内容卖点逐条结果_v2_2026-08-01.csv`：中文列名的逐条标签、分数、定性与依据",
        "- `douyin_selling_point_results_v2_2026-08-01.csv`：机器处理版逐条结果",
        "- `douyin_selling_point_summary_v2_2026-08-01.json`：机器可读汇总",
        "- `douyin_cache/sample30_2026-08-01/`：接口原始缓存",
    ]

    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
