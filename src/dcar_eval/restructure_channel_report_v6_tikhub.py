#!/usr/bin/env python3
"""Rebuild TikHub-enriched conclusions in the user-approved v5 report structure."""

from __future__ import annotations

import json
from statistics import mean
from typing import Any

import analyze_douyin_tikhub_v6 as enrichment
import rebuild_channel_evaluation_v4 as v4
import restructure_channel_report_v5 as v5
from three_proposition_scoring import acquisition_conclusion, audience_conclusion
from project_paths import CURRENT_REPORTS_DIR


RUN_DATE = "2026-08-02"
OUT_JSON = CURRENT_REPORTS_DIR / f"双渠道结构化结论_v6.2_TikHub_{RUN_DATE}.json"
OUT_REPORT = CURRENT_REPORTS_DIR / f"双渠道结构化结论报告_v6.2_TikHub_{RUN_DATE}.md"


def metric(**kwargs: Any) -> dict[str, Any]:
    return v5.metric(**kwargs)


def pct(n: int | float, d: int | float) -> float:
    return v5.pct(n, d)


def valid_play_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in rows if row.get("play_count_valid")]


def play_sum(rows: list[dict[str, Any]]) -> int:
    return sum(int(row.get("play_count_tikhub") or 0) for row in rows if row.get("play_count_valid"))


def average(rows: list[dict[str, Any]], field: str) -> int | None:
    values = [int(row[field]) for row in rows if row.get(field) is not None]
    return round(mean(values)) if values else None


def douyin_section(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    included = [row for row in rows if row.get("included")]
    core = [row for row in included if row.get("primary_tier") == "core"]
    valid_play = valid_play_rows(rows)
    total_play = play_sum(valid_play)
    included_play = play_sum(included)
    core_play = play_sum(core)
    content_scores = [int(row["content_auto_score_v6"]) for row in rows if row.get("content_auto_score_v6") is not None]
    content_score = round(mean(content_scores))
    content_pass = sum(score >= 70 for score in content_scores)
    comment_rows = [row for row in rows if row.get("audience_auto_score") is not None]
    audience_score = average(comment_rows, "audience_auto_score")
    acquisition_score = average(comment_rows, "acquisition_effect_estimate")

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
            value=pct(included_play, total_play),
            display=f"{included_play:,}/{total_play:,}次 · {pct(included_play, total_play)}%",
            qualitative="卖点内容贡献不足四成有效曝光",
            scope=f"TikHub有效播放覆盖{len(valid_play)}/{total}条 · {pct(len(valid_play), total)}%",
            reason="12条play_count=0按终版规则排除",
        ),
        "core_selling_point_exposure_share": metric(
            value=pct(core_play, total_play),
            display=f"{core_play:,}/{total_play:,}次 · {pct(core_play, total_play)}%",
            qualitative="核心卖点曝光明显不足",
            scope=f"TikHub有效总播放{total_play:,}次",
        ),
        "content_verticality": metric(
            value=content_score,
            display=f"{content_score}/100 · {content_pass}/{total} · {pct(content_pass, total)}%",
            qualitative=v4.auto_qualitative(content_score),
            scope="437/438条完整媒体证据；评论达标内容按终版规则作最多±5分校准",
        ),
        "audience_verticality": metric(
            value=audience_score,
            display=f"{audience_score}/100 · {len(comment_rows)}/{total}条 · {pct(len(comment_rows), total)}%可评估",
            status="sample_only",
            qualitative=audience_conclusion(audience_score) or "",
            scope=f"{len(comment_rows)}条达到20名有效独立评论用户门槛，共1504名有效用户",
            reason="仅代表高评论内容，存在选择偏差，不上卷为438条全量受众",
        ),
        "acquisition_effect_estimate": metric(
            value=acquisition_score,
            display=f"{acquisition_score}/100 · {len(comment_rows)}/{total}条 · {pct(len(comment_rows), total)}%可计算",
            status="sample_only",
            qualitative=acquisition_conclusion(acquisition_score) or "",
            scope="与互动用户相同的67条评论达标内容",
            reason="是内容侧拉新效果预估，不是懂车帝实际新增；TikHub不提供跨App归因",
        ),
    }

    scenes: dict[str, Any] = {}
    for scene in v4.SCENES:
        scene_rows = [row for row in included if row.get("business_scene") == scene]
        scene_core = [row for row in scene_rows if row.get("primary_tier") == "core"]
        scene_play_rows = valid_play_rows(scene_rows)
        scene_play = play_sum(scene_rows)
        scene_core_play = play_sum(scene_core)
        scene_content_score = average(scene_rows, "content_auto_score_v6")
        scene_content_pass = sum(int(row.get("content_auto_score_v6") or 0) >= 70 for row in scene_rows)
        scene_comment_rows = [row for row in scene_rows if row.get("audience_auto_score") is not None]
        scene_audience = average(scene_comment_rows, "audience_auto_score")
        scene_acquisition = average(scene_comment_rows, "acquisition_effect_estimate")
        if scene_comment_rows:
            audience_metric = metric(
                value=scene_audience,
                display=f"{scene_audience}/100 · {len(scene_comment_rows)}/{len(scene_rows)}条 · {pct(len(scene_comment_rows), len(scene_rows))}%可评估",
                status="sample_only",
                qualitative=audience_conclusion(scene_audience) or "",
                scope=f"该场景{len(scene_comment_rows)}条评论达标内容",
                reason="只代表高评论内容",
            )
            acquisition_metric = metric(
                value=scene_acquisition,
                display=f"{scene_acquisition}/100 · {len(scene_comment_rows)}/{len(scene_rows)}条 · {pct(len(scene_comment_rows), len(scene_rows))}%可计算",
                status="sample_only",
                qualitative=acquisition_conclusion(scene_acquisition) or "",
                scope=f"该场景{len(scene_comment_rows)}条评论达标内容",
                reason="是预估，不是实际新增",
            )
        else:
            audience_metric = metric(
                value=None,
                display=f"0/{len(scene_rows)}条 · 0.0%数据覆盖",
                status="not_computable",
                qualitative="暂不可计算",
                scope=scene,
                reason="没有内容达到20名有效独立评论用户门槛；不能写成0分",
            )
            acquisition_metric = metric(
                value=None,
                display=f"0/{len(scene_rows)}条 · 0.0%数据覆盖",
                status="not_computable",
                qualitative="暂不可计算",
                scope=scene,
                reason="缺少评论达标内容；不能写成0分",
            )
        scenes[scene] = {
            "publication_n": len(scene_rows),
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
                value=pct(scene_play, total_play),
                display=f"{scene_play:,}/{total_play:,}次 · {pct(scene_play, total_play)}%",
                qualitative="占全部抖音有效曝光",
                scope=f"该场景{len(scene_play_rows)}/{len(scene_rows)}条具备有效播放",
            ),
            "core_selling_point_exposure_share": metric(
                value=pct(scene_core_play, total_play),
                display=f"{scene_core_play:,}/{total_play:,}次 · {pct(scene_core_play, total_play)}%",
                qualitative="占全部抖音有效曝光",
                scope="该场景核心卖点有效播放 / 全部有效播放",
            ),
            "content_verticality": metric(
                value=scene_content_score,
                display=f"{scene_content_score}/100 · {scene_content_pass}/{len(scene_rows)} · {pct(scene_content_pass, len(scene_rows))}%",
                qualitative=v4.auto_qualitative(scene_content_score),
                scope=f"仅该场景{len(scene_rows)}条正式卖点内容",
            ),
            "audience_verticality": audience_metric,
            "acquisition_effect_estimate": acquisition_metric,
        }
    return {
        "scope": "仅抖音内容链接；30个随机账号样本中的438条发布",
        "denominator": total,
        "summary": summary,
        "scenes": scenes,
    }


def douyin_content_details(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    total_play = play_sum(rows)
    details: list[dict[str, Any]] = []
    for index, row in enumerate(rows, 1):
        included = bool(row.get("included"))
        selling_score = int(row.get("score") or 0)
        is_core = included and row.get("primary_tier") == "core"
        play = int(row.get("play_count_tikhub") or 0)
        content_score = row.get("content_auto_score_v6")
        audience_score = row.get("audience_auto_score")
        acquisition_score = row.get("acquisition_effect_estimate")
        if play > 0:
            exposure = f"{play:,}/{total_play:,}次 · {pct(play, total_play)}%渠道有效播放 · 有效播放量"
        else:
            exposure = "0/1条有效播放 · 0.0%数据覆盖 · TikHub返回0，按终版规则排除"
        if audience_score is not None:
            audience = (
                f"{audience_score}/100 · {audience_score}% · {row.get('audience_auto_qualitative')}；"
                f"{row.get('valid_unique_commenters')}名有效独立评论用户"
            )
            acquisition = (
                f"{acquisition_score}/100 · {acquisition_score}% · "
                f"{row.get('acquisition_effect_qualitative')}"
            )
        else:
            valid_n = int(row.get("valid_unique_commenters") or 0)
            audience = f"{valid_n}/20名有效评论用户 · {pct(valid_n, 20)}%门槛覆盖 · 暂不可计算"
            acquisition = "0/1个统一预估分 · 0.0%可计算率 · 暂不可计算"
        details.append({
            "item_no": f"D{index:03d}",
            "content_id": str(row.get("aweme_id") or ""),
            "title": v5.clean_title(row.get("desc") or "无标题"),
            "url": row.get("share_url") or "",
            "account": row.get("account_name") or "",
            "business_scene": row.get("business_scene") or "未归入业务场景",
            "selling_point": (
                f"{selling_score}/100 · {selling_score}% · {row.get('qualitative') or '命中'}（{row.get('primary_id')}）"
                if included else "0/100 · 0.0% · 未命中正式卖点"
            ),
            "core_selling_point": "1/1条 · 100.0% · 核心卖点" if is_core else "0/1条 · 0.0% · 非核心或未命中",
            "exposure": exposure,
            "content_verticality": (
                f"{content_score}/100 · {content_score}% · {v4.auto_qualitative(content_score)}"
                if content_score is not None else "0/1条完整证据 · 0.0%数据覆盖 · 暂不可计算"
            ),
            "audience_verticality": audience,
            "acquisition_effect_estimate": acquisition,
        })
    return details


def build_report(data: dict[str, Any]) -> str:
    d = data["channels"]["douyin"]
    x = data["channels"]["xiaohongshu"]
    lines = [
        "# 双渠道结构化结论报告 v6.2（TikHub补充）",
        "",
        f"生成日期：{RUN_DATE}  ",
        "固定结构：抖音汇总与场景 → 小红书汇总与场景 → 结论摘要 → 抖音内容明细 → 小红书内容明细 → 配套文件。  ",
        "条数占比分母统一为该渠道全部发布；场景占比表示该场景对全渠道的贡献，不使用场景内自占比。  ",
        "所有指标均使用“数字 + 百分比 + 文字描述”；不可计算项中的0%只表示数据覆盖率，不表示业务结果为0。小红书样本结果不当作338条渠道结论。  ",
        "TikHub补充的是抖音公开播放量与评论；内容拉新效果为实验前预估，不是懂车帝实际新增。",
        "",
        "## 【抖音渠道】",
        "",
        f"范围：{d['scope']}。",
        "",
        "### 1、汇总",
        "",
        *v5.summary_table(d),
        "",
        "### 2、三个业务场景",
        "",
        *v5.scene_table(d),
        "",
        "> 场景内容垂直度只对已命中正式卖点、且已归入该场景的内容求均值；互动用户和拉新预估只代表评论达标内容。二手车没有评论达标内容，不能写成0分。",
        "",
        "## 【小红书渠道】",
        "",
        f"范围：{x['scope']}。",
        "",
        "### 1、汇总",
        "",
        *v5.summary_table(x),
        "",
        "### 2、三个业务场景",
        "",
        *v5.scene_table(x),
        "",
        "> 小红书只有10/338条完成全媒体卖点标注，三场景表是诊断样本结构；其中二手车和新车没有可用场景样本，不以0%冒充渠道结论。",
        "",
        "## 结论摘要",
        "",
        "1. 抖音卖点条数占比73.52%，核心卖点条数占比42.24%，核心生产占比仍低于60%-70%目标。",
        "2. 抖音卖点内容贡献39.39%有效曝光，核心卖点仅贡献5.20%；核心内容既生产不足，流量效率也明显偏低。",
        "3. 抖音67条评论达标内容的互动用户垂直度为40/100，内容拉新效果预估34/100；只代表高评论内容，不上卷为438条全量受众。",
        "4. TikHub不提供懂车帝侧点击、安装、登录与确认新增，实际拉新效果仍需懂车帝侧归因。",
        "5. 小红书仍只有10条完整卖点样本，当前能输出样本诊断，但不能输出渠道级卖点条数/曝光分布。",
        "",
        "## 抖音内容明细",
        "",
        f"共{len(d['content_details'])}条，逐条展示全部抖音发布。",
        "",
        *v5.detail_lines(d["content_details"]),
        "",
        "## 小红书内容明细",
        "",
        f"共{len(x['content_details'])}条，逐条展示全部小红书链接；未完成全媒体采集的内容明确标记为待补采。",
        "",
        *v5.detail_lines(x["content_details"]),
        "",
        "## 配套文件",
        "",
        "- `双渠道核心结论_v6_TikHub补充_2026-08-02.png`",
        f"- `{OUT_JSON.name}`",
        "- `抖音438条内容渠道评估_v6_TikHub补充_2026-08-02.csv`",
        "- `抖音评论匿名用户评分_v6_TikHub_2026-08-02.jsonl`",
        "- `懂车帝内容评估判断标准与流程_v4_终版.md`",
    ]
    return "\n".join(lines) + "\n"


def build() -> dict[str, Any]:
    douyin_rows, _ = enrichment.build_rows()
    taxonomy = v4.build_taxonomy()
    label_map = {item["id"]: item for item in taxonomy["labels"]}
    xhs_rows, xhs_diagnostics = v4.xhs_sample_rows(label_map)
    douyin = douyin_section(douyin_rows)
    douyin["content_details"] = douyin_content_details(douyin_rows)
    xhs = v5.xhs_section(xhs_rows, xhs_diagnostics)
    xhs["content_details"] = v5.xhs_content_details(xhs_rows)
    return {
        "report_version": "channel-structured-conclusions-v6.2-tikhub",
        "generated_at": RUN_DATE,
        "template_source": "双渠道结构化结论报告_v5_2026-08-02.md",
        "structure": [
            "douyin_summary_and_scenes",
            "xiaohongshu_summary_and_scenes",
            "conclusion_summary",
            "douyin_content_details",
            "xiaohongshu_content_details",
            "supporting_files",
        ],
        "metric_order": [
            "selling_point_count_share", "core_selling_point_count_share",
            "selling_point_exposure_share", "core_selling_point_exposure_share",
            "content_verticality", "audience_verticality", "acquisition_effect_estimate",
        ],
        "channels": {"douyin": douyin, "xiaohongshu": xhs},
    }


def main() -> int:
    data = build()
    OUT_JSON.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    OUT_REPORT.write_text(build_report(data), encoding="utf-8")
    print(OUT_REPORT)
    print(OUT_JSON)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
