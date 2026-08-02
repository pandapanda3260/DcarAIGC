#!/usr/bin/env python3
"""Rebuild the final v4 channel evaluation on an all-publication denominator.

The script intentionally keeps unavailable metrics unavailable:

* Douyin exposure is not calculated because the provider returned play_count=0
  for every sampled publication.
* Douyin audience automotiveity is not calculated without comment text.
* Actual DCar acquisition effect is never inferred from content or comments.
* Xiaohongshu selling-point distribution is reported as a sample diagnostic,
  not a channel-wide result, until full-media evidence covers the publication
  population.
"""

from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from copy import deepcopy
from datetime import date
from pathlib import Path
from statistics import mean
from typing import Any, Iterable

from project_paths import (
    CONFIG_DIR,
    CURRENT_REPORTS_DIR,
    DOUYIN_INPUT_DIR,
    DOUYIN_PROCESSED_DIR,
    XHS_INPUT_DIR,
    XHS_PROCESSED_DIR,
)


RUN_DATE = "2026-08-02"

V3_TAXONOMY = CONFIG_DIR / "business_selling_points_v3_final.json"
V3_DOUYIN = DOUYIN_PROCESSED_DIR / "douyin_selling_point_labels_v3_video_2026-08-01.jsonl"
DOUYIN_SOURCE = DOUYIN_INPUT_DIR / "douyin_30_account_content_sample_2026-08-01.jsonl"
XHS_LINKS = XHS_INPUT_DIR / "notes_unique.csv"
XHS_RESULTS = XHS_PROCESSED_DIR / "rnote_three_proposition_results.jsonl"
XHS_SUMMARY = XHS_PROCESSED_DIR / "rnote_three_proposition_summary.json"

OUT_TAXONOMY = CONFIG_DIR / "business_selling_points_v4_final.json"
OUT_DOUYIN_JSONL = DOUYIN_PROCESSED_DIR / f"douyin_selling_point_labels_v4_full_publication_{RUN_DATE}.jsonl"
OUT_DOUYIN_CSV = CURRENT_REPORTS_DIR / f"抖音438条内容渠道评估_v4_全发布口径_{RUN_DATE}.csv"
OUT_XHS_CSV = CURRENT_REPORTS_DIR / f"小红书渠道评估样本与数据缺口_v4_{RUN_DATE}.csv"
OUT_SUMMARY = DOUYIN_PROCESSED_DIR / f"channel_evaluation_summary_v4_{RUN_DATE}.json"
OUT_REPORT = CURRENT_REPORTS_DIR / f"双渠道内容与卖点评估报告_v4_{RUN_DATE}.md"

CORE_IDS = {"E1", "E2", "X1", "X2", "X3", "M1", "M2", "M3"}
SCENES = ("二手车", "新车", "媒体-AI小懂")

STORY_TERMS = (
    "人生副本", "情感", "治愈", "婚姻", "女性视角", "童年", "回忆",
    "妈妈", "爸爸", "父亲", "老公", "前女友", "家庭故事",
)
ENTERTAINMENT_TERMS = (
    "末日", "末世", "丧尸", "拒绝道德绑架", "ai短片", "ai短剧",
    "概念车", "机甲", "脑洞", "合体", "变形",
)
DCAR_VARIANTS = (
    "懂车帝", "懂车地", "懂车弟", "懂车递", "董车帝", "董车地", "董车弟",
    "总车帝", "总车地", "总车递", "dcar",
)
AUTO_TERMS = (
    "汽车", "车型", "车主", "买车", "卖车", "提车", "选车", "新车", "二手车",
    "老车", "车龄", "家用车", "代步车", "车企", "零件", "汽车零件",
    "车价", "落地价", "指导价", "优惠", "车况", "车源", "估价", "保值率",
    "试驾", "配置", "参数", "空间", "内饰", "外观", "座舱", "后排", "后备箱",
    "发动机", "变速箱", "底盘", "轮胎", "刹车", "机油", "保养", "维修", "故障",
    "手刹", "油门", "点火", "火花塞", "车钥匙", "无钥匙启动", "离合", "雨刮", "安全带",
    "油耗", "续航", "充电", "加油", "驾驶", "开车", "方向盘", "车门", "悬架",
    "车机", "电车", "燃油车", "suv", "mpv", "轿车", "跑车", "事故车", "泡水车",
    "火烧车", "调表车", "懂车帝", "ai小懂", "车友", "改装", "上市", "销量",
    "奔驰", "宝马", "奥迪", "大众", "丰田", "本田", "日产", "别克", "福特",
    "沃尔沃", "凯迪拉克", "特斯拉", "比亚迪", "吉利", "领克", "极氪", "长安",
    "奇瑞", "捷途", "哈弗", "坦克", "小鹏", "理想", "蔚来", "零跑", "问界",
    "小米", "五菱", "红旗", "腾势", "岚图", "阿维塔", "深蓝", "荣威", "马自达",
    "卡罗拉", "汉兰达", "凯美瑞", "雅阁", "迈腾", "桑塔纳", "轩逸", "帕萨特",
)
USED_SCENE_TERMS = (
    "二手车", "二手行情", "二手车源", "收车", "卖车", "车况报告", "检测报告",
    "事故车", "泡水车", "火烧车", "调表车", "过户", "估价", "保值率", "车商",
)

SUPPRESSED_V3_IDS = {
    # Main narrative is emotional or end-of-world entertainment.  The DCar task
    # appears only in a short mid-/end-roll segment and is not the main story.
    "7658265374835608882",
    "7657148699033832745",
    "7650455781225106715",
    "7664239453280493745",
    "7666030091839402726",
    "7660094838444305914",
    "7659724890028080238",
}

XHS_SELLING_POINT_SAMPLE = {
    "RA046": {"id": "C1", "score": 90, "scene": "媒体-AI小懂", "reason": "视频主体持续回答查酒驾与道路安全问题"},
    "RA077": {"id": "C1", "score": 96, "scene": "媒体-AI小懂", "reason": "视频主体讲解新手驾驶坏习惯和汽车零件保护"},
    "RA089": {"id": "C1", "score": 92, "scene": "媒体-AI小懂", "reason": "视频主体演绎等红灯起步、鸣笛与道路礼仪"},
}


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


def pct(numerator: int | float, denominator: int | float) -> float | None:
    if not denominator:
        return None
    return round(float(numerator) * 100 / float(denominator), 2)


def contains(text: str, terms: Iterable[str]) -> bool:
    low = text.lower()
    return any(term.lower() in low for term in terms)


def unique_term_count(text: str, terms: Iterable[str]) -> int:
    low = text.lower()
    return sum(term.lower() in low for term in terms)


def strip_hashtags(text: str) -> str:
    return re.sub(r"#[^#\s]+", " ", text.replace("＃", "#"))


def first_ratio(text: str, terms: Iterable[str]) -> float | None:
    low = text.lower()
    positions = [low.find(term.lower()) for term in terms if low.find(term.lower()) >= 0]
    return min(positions) / max(len(low), 1) if positions else None


def scene_for_label(row: dict[str, Any]) -> str:
    point_id = str(row.get("primary_id") or "")
    if point_id.startswith("E"):
        return "二手车"
    if point_id.startswith("X"):
        return "新车"
    if point_id.startswith("M"):
        return "媒体-AI小懂"
    text = "\n".join(
        str(row.get(key) or "")
        for key in ("desc", "asr_text", "ocr_text", "visual_review_summary")
    )
    if point_id == "C1":
        return "二手车" if contains(text, USED_SCENE_TERMS) else "媒体-AI小懂"
    if point_id == "C2":
        return "二手车" if contains(text, ("二手车", "二手车源", "车况", "表显里程")) else "新车"
    if point_id == "C3":
        return "媒体-AI小懂"
    if point_id == "C4":
        return "二手车" if contains(text, ("二手车市场", "二手行情", "二手车销量")) else "新车"
    return ""


def content_auto_score(row: dict[str, Any]) -> int | None:
    """Score automotive centrality from full publication evidence.

    This is independent from selling-point coverage: automotive entertainment,
    opinion, or stories may be automotive content without expressing a DCar
    business selling point.
    """

    if row.get("evidence_level") not in {"V2", "V3"}:
        return None
    desc = str(row.get("desc") or "")
    desc_plain = strip_hashtags(desc)
    asr = str(row.get("asr_text") or "")
    ocr = str(row.get("ocr_text") or "")
    visual = str(row.get("visual_review_summary") or "")
    all_text = "\n".join((desc, asr, ocr, visual))

    asr_terms = unique_term_count(asr, AUTO_TERMS)
    ocr_terms = unique_term_count(ocr, AUTO_TERMS)
    desc_terms = unique_term_count(desc_plain, AUTO_TERMS)
    visual_terms = unique_term_count(visual, AUTO_TERMS)
    score = 8 + min(50, asr_terms * 6) + min(18, ocr_terms * 3) + min(16, desc_terms * 4) + min(8, visual_terms * 4)

    if row.get("included"):
        score = max(score, 88)
    no_match_id = str(row.get("no_match_id") or "")
    if no_match_id == "NO_MATCH_BRAND_ONLY":
        score = max(score, 78)
    if no_match_id == "NO_MATCH_OTHER":
        score = max(score, 72)

    automotive_knowledge_post = contains(
        desc,
        ("#汽车知识", "#用车知识", "#汽车零件", "#驾驶知识", "#带你懂车", "#用车知识分享计划"),
    ) and bool(asr.strip() or ocr.strip() or visual.strip())
    automotive_concept_post = "#概念车" in desc and (
        desc_terms >= 1 or asr_terms >= 1 or ocr_terms >= 1 or visual_terms >= 1
    )
    if no_match_id == "NO_MATCH_ENTERTAINMENT" and automotive_knowledge_post:
        score = max(score, 82)

    story = contains(desc, STORY_TERMS)
    entertainment = contains(desc, ENTERTAINMENT_TERMS)
    dcar_position = first_ratio(asr, DCAR_VARIANTS)
    tail_placement = dcar_position is not None and dcar_position >= 0.45

    if str(row.get("aweme_id")) in SUPPRESSED_V3_IDS:
        score = min(score, 39)
    elif entertainment:
        # Automotive fantasy can still be car content; end-roll car placement
        # in an unrelated short drama cannot.
        if automotive_concept_post and not tail_placement:
            score = max(score, 70)
        elif desc_terms >= 1 and (asr_terms + ocr_terms + visual_terms) >= 2 and not tail_placement:
            score = max(score, 70)
        else:
            score = min(score, 39)
    elif story and tail_placement and asr_terms < 6:
        score = min(score, 39)
    elif story and asr_terms >= 5 and contains(desc, ("#汽车", "#车型", "#买车", "#二手车")):
        if "人生副本" in desc:
            score = min(max(score, 55), 69)
        else:
            score = max(score, 70)

    return max(0, min(100, int(round(score))))


def auto_qualitative(score: int | None) -> str:
    if score is None:
        return "证据不足"
    if score >= 85:
        return "明确属于汽车内容"
    if score >= 70:
        return "主体属于汽车内容"
    if score >= 40:
        return "与汽车有关但不是明确主体"
    return "不属于汽车内容"


def build_taxonomy() -> dict[str, Any]:
    taxonomy = json.loads(V3_TAXONOMY.read_text(encoding="utf-8"))
    taxonomy["taxonomy_version"] = "4.0-final"
    taxonomy["generated_at"] = RUN_DATE
    taxonomy["business_scenes"] = list(SCENES)
    taxonomy["primary_denominator"] = "全部唯一内容链接的发布条数；同链接去重，不做创意去重"
    taxonomy["deprecated_outputs"] = ["仅标题/正文口径", "去重创意主结论"]
    taxonomy["scene_assignment_rules"] = {
        "E1-E7": "二手车",
        "X1-X8": "新车",
        "M1-M6": "媒体-AI小懂",
        "C1": "二手车交易/鉴别/二手用车任务归二手车；其他实用汽车知识归媒体-AI小懂",
        "C2": "二手车源/车况实拍归二手车；新车或在售车型细节体验归新车",
        "C3": "媒体-AI小懂",
        "C4": "二手车市场动态归二手车；新车发布、销量及行业动态归新车；泛社区服务仅在不能归入前两者时归媒体-AI小懂",
    }
    taxonomy["narrative_dominance_rule"] = (
        "情感、剧情、末日或娱乐内容中，卖点只在中后段短暂植入且不构成主要用户任务时，"
        "不得命中正式卖点。"
    )
    for label in taxonomy["labels"]:
        point_id = label["id"]
        if point_id.startswith("E"):
            label["business_scene"] = "二手车"
        elif point_id.startswith("X"):
            label["business_scene"] = "新车"
        elif point_id.startswith("M"):
            label["business_scene"] = "媒体-AI小懂"
        else:
            # C类是跨场景能力标签，不把“动态归属”伪装成第四个业务场景。
            label.pop("business_scene", None)
            label["business_scene_options"] = {
                "C1": ["二手车", "媒体-AI小懂"],
                "C2": ["二手车", "新车"],
                "C3": ["媒体-AI小懂"],
                "C4": ["二手车", "新车", "媒体-AI小懂"],
            }[point_id]
        label.pop("business_line", None)
    return taxonomy


def rebuild_douyin(label_map: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows = read_jsonl(V3_DOUYIN)
    rebuilt: list[dict[str, Any]] = []
    for original in rows:
        row = deepcopy(original)
        aweme_id = str(row.get("aweme_id") or "")
        if aweme_id in SUPPRESSED_V3_IDS:
            previous_id = str(row.get("primary_id") or "")
            row["v3_suppressed_primary_id"] = previous_id
            row["v3_suppression_reason"] = "情感/末日主叙事占主导，懂车帝任务仅为中后段短暂植入"
            row.update(
                {
                    "primary_id": "",
                    "primary_label": "",
                    "primary_business_line": "",
                    "primary_tier": "",
                    "score": 0,
                    "qualitative": "未命中",
                    "included": False,
                    "pending": False,
                    "secondary_ids": [],
                    "secondary_labels": [],
                    "no_match_id": "NO_MATCH_PLACEMENT_ONLY",
                    "no_match_reason": "情感或娱乐主叙事中只有短暂懂车帝植入，未形成主要用户任务",
                    "decision_reason": "主叙事占比规则覆盖上一版的关键词命中",
                }
            )
        point_id = str(row.get("primary_id") or "")
        meta = label_map.get(point_id, {})
        scene = scene_for_label(row) if row.get("included") else ""
        row["business_scene"] = scene
        row["primary_tier"] = meta.get("tier", row.get("primary_tier", "")) if point_id else ""
        row["primary_label"] = meta.get("label", row.get("primary_label", "")) if point_id else ""
        row["publication_denominator"] = True
        row["content_auto_score"] = content_auto_score(row)
        row["content_auto_qualitative"] = auto_qualitative(row["content_auto_score"])
        row["audience_auto_score"] = None
        row["audience_auto_status"] = "缺少评论文本，不能由评论量推断"
        row["actual_acquisition_score"] = None
        row["actual_acquisition_status"] = "缺少懂车帝侧归因新增数据"
        rebuilt.append(row)
    return rebuilt


def count_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    identifiable = [row for row in rows if row.get("evidence_level") in {"V2", "V3"}]
    included = [row for row in rows if row.get("included")]
    core = [row for row in included if row.get("primary_tier") == "core"]
    other = [row for row in included if row.get("primary_tier") == "other"]
    uncovered = [row for row in rows if not row.get("included")]
    auto_scores = [int(row["content_auto_score"]) for row in rows if row.get("content_auto_score") is not None]
    auto_publications = [row for row in rows if row.get("content_auto_score") is not None and int(row["content_auto_score"]) >= 70]
    scene_counts = Counter(row.get("business_scene") for row in included)
    scene_core = Counter(row.get("business_scene") for row in core)
    scene_other = Counter(row.get("business_scene") for row in other)
    return {
        "total_publications": total,
        "identifiable": len(identifiable),
        "identifiable_pct_all": pct(len(identifiable), total),
        "selling_point_covered": len(included),
        "selling_point_covered_pct_all": pct(len(included), total),
        "core": len(core),
        "core_pct_all": pct(len(core), total),
        "other": len(other),
        "other_pct_all": pct(len(other), total),
        "uncovered_or_unidentifiable": len(uncovered),
        "uncovered_or_unidentifiable_pct_all": pct(len(uncovered), total),
        "primary_counts": dict(sorted(Counter(row.get("primary_id") for row in included).items())),
        "scene_counts": {scene: scene_counts.get(scene, 0) for scene in SCENES},
        "scene_core_counts": {scene: scene_core.get(scene, 0) for scene in SCENES},
        "scene_other_counts": {scene: scene_other.get(scene, 0) for scene in SCENES},
        "content_automotive": {
            "score": round(mean(auto_scores)) if auto_scores else None,
            "qualitative": auto_qualitative(round(mean(auto_scores))) if auto_scores else "证据不足",
            "evaluable": len(auto_scores),
            "automotive_publications": len(auto_publications),
            "automotive_publications_pct_all": pct(len(auto_publications), total),
        },
    }


def xhs_sample_rows(label_map: dict[str, dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    with XHS_LINKS.open(encoding="utf-8-sig", newline="") as handle:
        links = list(csv.DictReader(handle))
    link_by_id = {row["note_id"]: row for row in links}
    results = read_jsonl(XHS_RESULTS)
    output: list[dict[str, Any]] = []
    for row in results:
        sample_id = str(row["sample_attempt_id"])
        point = XHS_SELLING_POINT_SAMPLE.get(sample_id)
        meta = label_map.get(point["id"], {}) if point else {}
        link = link_by_id.get(str(row["note_id"]), {})
        output.append(
            {
                "sample_id": sample_id,
                "note_id": row["note_id"],
                "url": row["url"],
                "source_stratum": row["source_stratum"],
                "vv": int(link["vv"]) if str(link.get("vv") or "").isdigit() else None,
                "valid_unique_commenters": row["valid_unique_commenters"],
                "selling_point_id": point["id"] if point else "",
                "selling_point_label": meta.get("label", "") if point else "",
                "business_scene": point["scene"] if point else "",
                "tier": meta.get("tier", "") if point else "",
                "selling_point_score": point["score"] if point else 0,
                "selling_point_qualitative": "强匹配" if point and point["score"] >= 90 else "未命中",
                "selling_point_reason": point["reason"] if point else "未形成以懂车帝用户任务为主叙事的正式卖点",
                "content_auto_score": row["content_auto_score"],
                "content_auto_conclusion": row["content_auto_conclusion"],
                "audience_auto_score": row["audience_auto_score"],
                "audience_auto_conclusion": row["audience_auto_conclusion"],
                "predicted_acquisition_potential_score": row["dcd_acquisition_score"],
                "actual_acquisition_score": None,
                "actual_acquisition_status": "缺少懂车帝侧归因新增数据",
            }
        )

    total = len(links)
    vv_rows = [row for row in links if str(row.get("vv") or "").isdigit()]
    labelled_with_vv = [row for row in output if row["vv"] is not None]
    covered_sample = [row for row in output if row["selling_point_id"]]
    core_sample = [row for row in covered_sample if row["tier"] == "core"]
    other_sample = [row for row in covered_sample if row["tier"] == "other"]
    xhs_old_summary = json.loads(XHS_SUMMARY.read_text(encoding="utf-8"))
    auto_n = sum(row["gold_label"] == "auto" for row in links)
    non_auto_n = total - auto_n
    auto_stratum_score = xhs_old_summary["propositions"]["content_automotive"]["by_source_stratum"]["auto"]["score"]
    non_auto_stratum_score = xhs_old_summary["propositions"]["content_automotive"]["by_source_stratum"]["non_auto"]["score"]
    weighted_content_estimate = round((auto_n * auto_stratum_score + non_auto_n * non_auto_stratum_score) / total)
    diagnostics = {
        "total_unique_publication_links": total,
        "source_strata": {"auto": auto_n, "non_auto": non_auto_n},
        "full_evidence_selling_point_labelled": len(output),
        "full_evidence_selling_point_labelled_pct_all": pct(len(output), total),
        "view_count_available": len(vv_rows),
        "view_count_available_pct_all": pct(len(vv_rows), total),
        "full_evidence_and_view_count": len(labelled_with_vv),
        "full_evidence_and_view_count_pct_all": pct(len(labelled_with_vv), total),
        "sample_diagnostic": {
            "n": len(output),
            "selling_point_covered": len(covered_sample),
            "selling_point_covered_pct": pct(len(covered_sample), len(output)),
            "core": len(core_sample),
            "core_pct": pct(len(core_sample), len(output)),
            "other": len(other_sample),
            "other_pct": pct(len(other_sample), len(output)),
            "known_view_n": len(labelled_with_vv),
            "known_view_total": sum(row["vv"] or 0 for row in output),
            "known_view_selling_point_covered": sum(row["vv"] or 0 for row in covered_sample),
        },
        "all_publication_selling_point_metrics": {
            "status": "not_computable",
            "reason": "仅10/338条具备完整内容证据，不能上卷为全渠道卖点分布",
            "identifiable": None,
            "selling_point_covered": None,
            "core": None,
            "other": None,
        },
        "all_publication_exposure_metrics": {
            "status": "not_computable",
            "reason": "只有5/338条同时具备完整卖点标签和浏览量，交叉覆盖不足",
        },
        "content_automotive": {
            "status": "directional_stratified_estimate",
            "score": weighted_content_estimate,
            "qualitative": auto_qualitative(weighted_content_estimate),
            "basis": "按313条汽车来源、25条非汽车来源占比，对各层5条完整内容随机样本均分95/0做后分层加权",
            "limitation": "每层仅5条，不能替代338条逐条全媒体判定",
        },
        "audience_automotive": {
            "status": "sample_only_not_channel_result",
            "score": xhs_old_summary["propositions"]["audience_automotive"]["score"],
            "sample_n": xhs_old_summary["propositions"]["audience_automotive"]["sample_size"],
            "basis": "评论达标的5+5平衡样本",
            "limitation": "汽车来源笔记为满足20名有效评论用户而替补，存在高评论量选择偏差",
        },
        "actual_acquisition_effect": {
            "status": "not_tested",
            "score": None,
            "reason": "没有懂车帝侧点击、安装、登录及确认新增归因数据",
        },
    }
    return output, diagnostics


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def format_value(value: Any) -> str:
    return "—" if value is None else str(value)


def render_report(summary: dict[str, Any]) -> str:
    dy = summary["channels"]["douyin"]
    dm = dy["count_dimension"]
    xhs = summary["channels"]["xiaohongshu"]
    xd = xhs["data_diagnostics"]
    lines = [
        "# 懂车帝双渠道内容与卖点评估报告 v4.0",
        "",
        f"生成日期：{RUN_DATE}  ",
        "主口径：唯一内容链接的全部发布条数；同一链接去重，不做创意去重。  ",
        "证据口径：终版卖点只接受完整视频/图片主体证据，不接受仅标题、正文、封面或话题标签的强结论。",
        "",
        "## 一、核心结论",
        "",
        f"- 抖音438条发布中，{dm['identifiable']}条可识别（{dm['identifiable_pct_all']}%）；{dm['selling_point_covered']}条命中正式卖点（{dm['selling_point_covered_pct_all']}%）。其中核心卖点{dm['core']}条（{dm['core_pct_all']}%），其他卖点{dm['other']}条（{dm['other_pct_all']}%）。",
        f"- 抖音核心卖点占全部发布{dm['core_pct_all']}%，低于计划的60%-70%。即使只看卖点命中内容，核心占比也为{pct(dm['core'], dm['selling_point_covered'])}%。",
        f"- 抖音内容汽车性为{dy['content_verticality']['content_automotive']['score']}/100，{dy['content_verticality']['content_automotive']['qualitative']}；{dy['content_verticality']['content_automotive']['automotive_publications']}条达到汽车内容门槛，占全部发布{dy['content_verticality']['content_automotive']['automotive_publications_pct_all']}%。",
        "- 抖音曝光分布、互动受众汽车性和实际拉新效果当前均不可计算：播放量全部为平台占位值0、未采集评论文本、未接入懂车帝侧新增归因。",
        f"- 小红书有{xhs['total_publications']}条唯一链接，但只有{xd['full_evidence_selling_point_labelled']}条完成全媒体卖点标注；因此条数和曝光维度的渠道卖点分布均不能上卷。现有10条样本仅作诊断：卖点命中3条、核心0条。",
        f"- 小红书内容汽车性只有方向性后分层估计：{xd['content_automotive']['score']}/100；互动受众15/100仅代表评论达标的5+5样本；实际拉新效果未测试。",
        "",
        "## 二、三业务场景归属",
        "",
        "`媒体内容与社区`不再作为第四业务线。C1-C4保留为卖点标签，但每条内容必须归入以下三个业务场景之一：",
        "",
        "| 原标签 | 二手车场景 | 新车场景 | 媒体-AI小懂场景 |",
        "|---|---|---|---|",
        "| C1 实用汽车知识 | 二手车鉴别、二手交易和二手用车任务 | 若已形成具体新车决策任务，应优先改标X类 | 泛驾驶、安全、零件、保养、维修、应急知识 |",
        "| C2 车型细节与体验 | 二手车源、车况和实车体验 | 新车/在售车型外观、内饰、空间和场景体验 | — |",
        "| C3 车友社区 | — | — | 车友交流、改装、玩车和社区创作 |",
        "| C4 行业动态 | 二手车市场和交易行业动态 | 新车发布、上市、销量和汽车行业动态 | 泛社区议题仅在确属社区服务时归入 |",
        "",
        "抖音已命中卖点内容的场景分布：",
        "",
        "| 业务场景 | 卖点命中 | 核心 | 其他 | 占全部发布 |",
        "|---|---:|---:|---:|---:|",
    ]
    for scene in SCENES:
        n = dm["scene_counts"][scene]
        lines.append(
            f"| {scene} | {n} | {dm['scene_core_counts'][scene]} | {dm['scene_other_counts'][scene]} | {pct(n, dm['total_publications'])}% |"
        )
    lines.extend(
        [
            "",
            "## 三、抖音渠道（仅抖音内容链接）",
            "",
            "### 1. 卖点分布 - 条数维度",
            "",
            "| 指标 | 条数 | 占全部438条 | 解释 |",
            "|---|---:|---:|---|",
            f"| 可识别内容 | {dm['identifiable']} | {dm['identifiable_pct_all']}% | V2/V3，已读取实际视频或图文主体 |",
            f"| 卖点覆盖 | {dm['selling_point_covered']} | {dm['selling_point_covered_pct_all']}% | 主卖点>=75分且证据V2/V3 |",
            f"| 核心卖点覆盖 | {dm['core']} | {dm['core_pct_all']}% | E1/E2、X1/X2/X3、M1/M2/M3 |",
            f"| 其他卖点覆盖 | {dm['other']} | {dm['other_pct_all']}% | 正式其他卖点，包括按场景归属后的C1-C4 |",
            f"| 未覆盖/不可识别 | {dm['uncovered_or_unidentifiable']} | {dm['uncovered_or_unidentifiable_pct_all']}% | 未命中正式卖点或证据不完整 |",
            "",
            "> 可识别内容是证据完整度指标；卖点覆盖、核心和其他是业务表达指标。核心+其他=卖点覆盖，三者不是与可识别内容并列互斥的四个桶。",
            "",
            "### 2. 卖点分布 - 曝光维度",
            "",
            "暂不可计算。438条原始返回均有`play_count`字段，但值全部为0，属于不可用占位值。点赞、评论、收藏、分享不能替代播放量。",
            "",
            "### 3. 内容垂直度",
            "",
            "| 命题 | 结果 | 定性 | 数据边界 |",
            "|---|---:|---|---|",
            f"| 内容汽车性 | {dy['content_verticality']['content_automotive']['score']}/100 | {dy['content_verticality']['content_automotive']['qualitative']} | 437条具备完整媒体证据；按全部发布汇总 |",
            "| 互动受众汽车性 | — | 暂不可判断 | 只有评论数，没有评论文本和有效独立评论用户 |",
            "| 懂车帝拉新效果 | — | 尚未测试 | 必须接入懂车帝侧归因点击、安装、登录和确认新增 |",
            "",
            "## 四、小红书渠道（仅小红书内容链接）",
            "",
            "### 1. 卖点分布 - 条数维度",
            "",
            "渠道级暂不可计算。338条唯一链接中只有10条完成全媒体卖点标注（2.96%），达不到全发布口径。现有10条仅作为规则诊断：可识别10条、卖点覆盖3条、核心0条、其他3条。",
            "",
            "### 2. 卖点分布 - 曝光维度",
            "",
            f"渠道级暂不可计算。浏览量字段覆盖{xd['view_count_available']}/{xhs['total_publications']}条（{xd['view_count_available_pct_all']}%），但同时具备完整卖点标签和浏览量的只有{xd['full_evidence_and_view_count']}条（{xd['full_evidence_and_view_count_pct_all']}%）。",
            "",
            "### 3. 内容垂直度",
            "",
            "| 命题 | 结果 | 定性 | 数据边界 |",
            "|---|---:|---|---|",
            f"| 内容汽车性 | {xd['content_automotive']['score']}/100 | {xd['content_automotive']['qualitative']}（方向性估计） | 分层各5条完整内容样本，按313:25来源占比后分层加权 |",
            f"| 互动受众汽车性 | {xd['audience_automotive']['score']}/100 | 互动主要来自非汽车或泛娱乐人群（样本） | 评论达标的5+5样本，不能上卷为全渠道 |",
            "| 懂车帝拉新效果 | — | 尚未测试 | 现有67/58等为拉新潜力预测，不是实际新增效果 |",
            "",
            "## 五、终版规则变化与数据缺口",
            "",
            "1. 删除标题/正文单独口径；V1只能进入采集补全队列，不能进入终版覆盖率。",
            "2. 主结论只用全部发布口径；同一平台内容ID只计一次，不再用去重创意口径替代发布结果。",
            "3. 增加主叙事占比规则：情感/末日内容中的中后段短植入不能命中卖点。本次据此纠正7条抖音误标。",
            "4. 曝光维度只接受真实播放/浏览量；字段缺失或占位值为0时输出不可计算。",
            "5. 互动受众必须基于评论文本和有效独立评论用户；少于20人时跳过并按既定随机队列补样。",
            "6. `懂车帝拉新效果`只由懂车帝侧实验归因产生；内容与评论只能给拉新潜力预测，且不再放入“效果”主结论。",
            "",
            "## 六、当前应补的数据",
            "",
            "- 抖音：真实播放量、评论文本与评论用户去重信息、懂车帝侧归因新增数据。",
            "- 小红书：其余328条的正文+全部图片/视频主体证据；之后才能输出全发布卖点分布。曝光仍需补齐32条缺失浏览量。",
            "- 两渠道：为拉新实验统一内容ID、渠道、曝光、落地页点击、下载/激活、登录和确认新用户字段。",
            "",
            "## 七、产出文件",
            "",
            f"- 终版标签体系：`{OUT_TAXONOMY.name}`",
            f"- 抖音逐条结果：`{OUT_DOUYIN_CSV.name}`、`{OUT_DOUYIN_JSONL.name}`",
            f"- 小红书样本与缺口：`{OUT_XHS_CSV.name}`",
            f"- 双渠道结构化汇总：`{OUT_SUMMARY.name}`",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    taxonomy = build_taxonomy()
    OUT_TAXONOMY.write_text(json.dumps(taxonomy, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    label_map = {item["id"]: item for item in taxonomy["labels"]}

    douyin = rebuild_douyin(label_map)
    with OUT_DOUYIN_JSONL.open("w", encoding="utf-8") as handle:
        for row in douyin:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    dy_csv_fields = [
        "account_sample_index", "quality_label", "account_name", "uid", "aweme_id", "share_url",
        "create_time_cn", "desc", "evidence_level", "business_scene", "primary_id", "primary_label",
        "primary_tier", "score", "qualitative", "included", "decision_reason", "evidence_source",
        "evidence_snippet", "content_auto_score", "content_auto_qualitative", "audience_auto_score",
        "audience_auto_status", "actual_acquisition_score", "actual_acquisition_status", "no_match_id",
        "no_match_reason", "v3_suppressed_primary_id", "v3_suppression_reason",
    ]
    with OUT_DOUYIN_CSV.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=dy_csv_fields)
        writer.writeheader()
        for row in douyin:
            writer.writerow({key: row.get(key, "") for key in dy_csv_fields})

    xhs_rows, xhs_diagnostics = xhs_sample_rows(label_map)
    write_csv(OUT_XHS_CSV, xhs_rows)

    dy_metrics = count_metrics(douyin)
    summary = {
        "report_version": "channel-evaluation-v4.0-final",
        "generated_at": RUN_DATE,
        "primary_denominator": "全部唯一内容链接的发布条数；同链接去重，不做创意去重",
        "channels": {
            "douyin": {
                "scope": "仅抖音内容链接，30个账号随机样本中的438条发布",
                "total_publications": len(douyin),
                "count_dimension": dy_metrics,
                "exposure_dimension": {
                    "status": "not_computable",
                    "play_count_fields": 438,
                    "positive_play_counts": 0,
                    "reason": "438条play_count全部为0，属于不可用占位值",
                },
                "content_verticality": {
                    "content_automotive": dy_metrics["content_automotive"],
                    "audience_automotive": {
                        "status": "not_computable",
                        "score": None,
                        "reason": "未采集评论文本和有效独立评论用户",
                    },
                    "actual_acquisition_effect": {
                        "status": "not_tested",
                        "score": None,
                        "reason": "未接入懂车帝侧归因新增数据",
                    },
                },
                "quality_correction": {
                    "suppressed_mid_or_end_roll_placements": len(SUPPRESSED_V3_IDS),
                    "aweme_ids": sorted(SUPPRESSED_V3_IDS),
                },
            },
            "xiaohongshu": {
                "scope": "仅小红书内容链接，338条唯一链接",
                "total_publications": xhs_diagnostics["total_unique_publication_links"],
                "data_diagnostics": xhs_diagnostics,
            },
        },
    }
    OUT_SUMMARY.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    OUT_REPORT.write_text(render_report(summary), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
