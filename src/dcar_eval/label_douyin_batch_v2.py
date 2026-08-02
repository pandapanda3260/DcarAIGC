#!/usr/bin/env python3
"""Label the 30-account Douyin sample with the DCar user-task taxonomy v2.

The classifier is deliberately conservative: a DCar hashtag or account ownership
alone never establishes an official selling point.  Official labels require an
explicit platform task or a sufficiently complete demonstration of a repeatable
DCar capability. Candidate labels explain recurring media/community value but do
not enter the official core-selling-point ratio.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from project_paths import ARCHIVE_PROCESSED_DIR, CONFIG_ARCHIVE_DIR, DOUYIN_INPUT_DIR


SOURCE = DOUYIN_INPUT_DIR / "douyin_30_account_content_sample_2026-08-01.jsonl"
TAXONOMY = CONFIG_ARCHIVE_DIR / "business_selling_points_v2.json"
OUT_JSONL = ARCHIVE_PROCESSED_DIR / "douyin_selling_point_labels_v2_2026-08-01.jsonl"
OUT_CSV = ARCHIVE_PROCESSED_DIR / "douyin_selling_point_results_v2_2026-08-01.csv"
OUT_CSV_CN = ARCHIVE_PROCESSED_DIR / "抖音438条内容卖点逐条结果_v2_2026-08-01.csv"
OUT_SUMMARY = ARCHIVE_PROCESSED_DIR / "douyin_selling_point_summary_v2_2026-08-01.json"

AUTO_INCLUDE = 75

# Accounts whose sampled page consistently demonstrates one repeatable content
# capability. This can support evidence level B, but never level A by itself.
PRICE_CAPABILITY_UIDS = {
    "2770412918283052", "4276433016599520", "7572444388237493305",
    "294299856549959", "837201288241035", "7592913454420263985",
    "7626590900580533305", "7626588632656512057", "3242877433691552",
}
KNOWLEDGE_CAPABILITY_UIDS = {
    "2209356824214972", "1760758145233291", "7634495945283077157",
    "3757456406621992", "762375428905835", "3124132046189994",
}
STORY_UIDS = {"2719509278899035", "903120263585667"}
CONCEPT_UIDS = {"7634084008151188537", "4322889907309118"}


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8-sig") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def normalize_text(text: str) -> str:
    text = text.lower().replace("＃", "#")
    text = re.sub(r"#([^#\s]+)", " ", text)
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", text)
    return text


def strip_hashtags(text: str) -> str:
    """Remove hashtag tokens so a tag alone cannot become task evidence."""
    text = text.lower().replace("＃", "#")
    return re.sub(r"#([^#\s]+)", " ", text)


def contains_any(text: str, terms: tuple[str, ...] | list[str] | set[str]) -> bool:
    return any(term.lower() in text for term in terms)


def label_name_map(taxonomy: dict[str, Any]) -> dict[str, str]:
    items = taxonomy["official_points"] + taxonomy["candidate_points"] + taxonomy["fallbacks"]
    return {item["id"]: item["label"] for item in items}


def official_meta(taxonomy: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {item["id"]: item for item in taxonomy["official_points"]}


def evidence_level(text: str, has_dcar: bool, demonstrated: bool) -> tuple[str, int]:
    body = strip_hashtags(text)
    explicit_module = contains_any(
        body,
        (
            "懂车帝app", "懂车帝平台", "打开懂车帝", "上懂车帝", "在懂车帝",
            "懂车帝显示", "懂车帝查询", "懂车帝搜索", "懂车帝二手车",
            "懂车帝估价", "懂车帝车型库", "懂车帝实测", "懂车帝车友圈",
        ),
    )
    if explicit_module:
        return "A", 93
    if demonstrated:
        return "B", 82 if has_dcar else 78
    return ("C", 58) if has_dcar else ("D", 35)


def classify_official(row: dict[str, Any]) -> tuple[str | None, int, str, str]:
    text = str(row.get("desc") or "").lower()
    body = strip_hashtags(text)
    uid = str(row.get("uid") or "")
    has_dcar = contains_any(text, ("懂车帝", "dcar"))
    has_dcar_in_body = contains_any(body, ("懂车帝", "dcar"))

    # AI Xiaodong requires explicit product naming; an account nickname containing
    # “小懂” is not enough.
    has_ai_xiaodong = contains_any(body, ("ai小懂", "ai 小懂", "小懂ai助手", "小懂助手"))
    if has_ai_xiaodong:
        if contains_any(text, ("预约试驾", "试驾指南", "试驾清单")):
            return "M1", 94, "A", "明确表达通过AI小懂预约试驾或获取试驾指南"
        if contains_any(text, ("库存", "现车", "展车", "试驾车", "真实优惠")):
            return "M2", 94, "A", "明确表达通过AI小懂查询优惠或库存"
        if contains_any(text, ("洗车", "维修保养", "保养预约", "用车账本", "车档案")):
            return "M3", 94, "A", "明确表达通过AI小懂管理日常用车服务"
        if contains_any(text, ("选车", "推荐", "怎么选", "对比")):
            return "M4", 91, "A", "明确表达通过AI小懂问答选车"
        if contains_any(text, ("故障", "报警灯", "异响", "安全处置")):
            return "M5", 91, "A", "明确表达通过AI小懂解释故障"
        if contains_any(text, ("估价", "卖车")):
            return "M6", 91, "A", "明确表达通过AI小懂估价或准备卖车"

    is_used = "二手车" in text
    platform_task = contains_any(
        body,
        ("懂车帝app", "懂车帝平台", "打开懂车帝", "上懂车帝", "在懂车帝", "懂车帝二手车"),
    )

    if has_dcar and contains_any(text, ("卖车", "收车", "上门评估", "高价收车", "到账")):
        level, score = evidence_level(text, has_dcar, platform_task)
        return "E1", max(score, 80), level, "出现懂车帝卖车、收车、评估或到账任务"

    if is_used and has_dcar and contains_any(body, ("同一车源不同价格", "二手车数据", "成交行情", "市场行情")):
        level, score = evidence_level(text, has_dcar, True)
        return "E6", score, level, "展示懂车帝二手车价格、车源或市场行情数据"

    e2_feature = contains_any(body, ("透明车况", "平台保障", "验车一体", "认证车源", "个人车源", "商家认证车源"))
    e2_long_platform_review = contains_any(body, ("二手车app", "二手车软件", "二手车平台")) and len(normalize_text(body)) >= 45
    if is_used and has_dcar and (e2_feature or e2_long_platform_review):
        level, score = evidence_level(text, has_dcar, True)
        return "E2", score, level, "出现懂车帝二手车车源、透明车况或平台保障能力"

    if is_used and has_dcar and contains_any(body, ("检测报告", "车辆档案", "专业检测", "验车", "车况报告")):
        level, score = evidence_level(text, has_dcar, platform_task)
        return "E5", max(score, 78), level, "出现懂车帝检测报告、车辆档案或专业验车能力"

    if is_used and has_dcar and contains_any(body, ("估价", "保值率", "成交行情", "市场行情", "历史价格")):
        level, score = evidence_level(text, has_dcar, True)
        return "E6", score, level, "出现懂车帝二手车估价、行情或保值数据"

    if is_used and has_dcar and contains_any(body, ("过户", "交易合同", "交易流程", "权益保障")):
        level, score = evidence_level(text, has_dcar, platform_task)
        return "E7", max(score, 78), level, "出现懂车帝二手车合同、过户或交易保障能力"

    if is_used and has_dcar and contains_any(body, ("预算", "万以内", "性价比", "行情", "怎么选", "首选")):
        scene = contains_any(body, ("家庭", "家用", "新手", "女生", "年轻人", "通勤", "代步", "宝妈"))
        point = "E4" if scene else "E3"
        level, score = evidence_level(text, has_dcar, True)
        reason = "按人群或用车场景展示懂车帝二手车推荐能力" if scene else "按预算或行情展示懂车帝二手车筛选能力"
        return point, score, level, reason

    # A real comparison must contain a comparison decision, not merely two brand hashtags.
    comparison = contains_any(body, ("对比", "pk", " vs ", "二选一", "同级", "横向", "选哪个"))
    comparison_auto_context = contains_any(
        body,
        (
            "汽车", "车型", "买车", "同样钱", "奥迪", "奔驰", "宝马", "大众", "丰田",
            "本田", "特斯拉", "比亚迪", "红旗", "吉利", "长安", "小米", "model 3",
            "a4l", "glc", "suv", "轿车",
        ),
    )
    if comparison and not is_used and comparison_auto_context:
        level, score = evidence_level(text, has_dcar, True)
        comparison_dimensions = sum(
            1
            for term in ("空间", "动力", "配置", "油耗", "续航", "价格", "舒适", "操控", "安全", "成本", "智能")
            if term in body
        )
        if comparison_dimensions < 2 and not platform_task:
            score = min(score, 68)
        return "X1", score, level, "展示候选车型的横向比较或选择路径"

    third_party_test = contains_any(body, ("懂车帝实测", "懂车分", "车主口碑", "车主评价", "实测榜", "权威测评", "长期测评"))
    if third_party_test:
        level, score = evidence_level(text, has_dcar, True)
        return "X2", score, level, "展示懂车帝实测、榜单或车主口碑能力"

    cost_detail = contains_any(body, ("养车成本", "用车成本", "一年要花", "一年花", "月供", "贷款方案", "保险油费", "油费保险", "每公里成本", "保养成本"))
    if cost_detail:
        level, score = evidence_level(text, has_dcar, True)
        if not has_dcar_in_body and len(normalize_text(body)) < 45:
            score = min(score, 68)
        return "X6", score, level, "展示购车或长期用车成本计算能力"

    independent_check = contains_any(
        body,
        ("减配", "虚标", "营销话术", "智商税", "配置骗局", "参数骗局", "真实油耗实测", "真实续航实测", "车企不敢说", "打脸"),
    )
    if independent_check:
        level, score = evidence_level(text, has_dcar, True)
        if not has_dcar_in_body and not contains_any(body, ("实测", "数据", "拆解")):
            score = min(score, 68)
        return "X7", score, level, "以独立解析或实测验证参数、配置或营销话术"

    config_compare = contains_any(body, ("版本对比", "配置对比", "高配和低配", "高低配", "选哪个配置", "哪个版本", "选装"))
    if config_compare:
        level, score = evidence_level(text, has_dcar, True)
        if not has_dcar_in_body and len(normalize_text(body)) < 45:
            score = min(score, 68)
        return "X5", score, level, "展示车型版本、配置差异或配置价值"

    if has_dcar and contains_any(text, ("购车合同", "定金", "交付权益", "购车权益", "退订", "选配合同")):
        level, score = evidence_level(text, has_dcar, platform_task)
        return "X8", max(score, 78), level, "出现购车合同、定金、交付或权益任务"

    # Price content can be evidence B only when it includes a useful price/offer
    # fact, or when the sampled account consistently performs price broadcasting.
    price_topic = contains_any(body, ("价格", "降价", "直降", "优惠", "一口价", "落地价", "全款", "裸车价", "现车", "指导价", "售价", "只要"))
    price_number = bool(re.search(r"(?:\d+(?:\.\d+)?\s*万|\d{2,3},\d{3}|\d+\s*[wW])", body))
    chinese_exact_price = bool(
        re.search(r"[一二三四五六七八九十两]+万[一二三四五六七八九十百千]*", body)
        or re.search(r"[一二三四五六七八九十两]+千[一二三四五六七八九十百]*", body)
    )
    price_number = price_number or chinese_exact_price
    stable_price_feed = uid in PRICE_CAPABILITY_UIDS and price_topic
    if price_topic and (price_number or stable_price_feed):
        demonstrated = price_number or stable_price_feed
        level, score = evidence_level(text, has_dcar, demonstrated)
        if not price_number:
            score = min(score, 68)
        if is_used:
            return "E6", score, level, "提供二手车具体价格或市场行情"
        return "X3", score, level, "提供具体新车车价、优惠、现车或稳定价格播报"

    new_car_choice = contains_any(body, ("预算", "选车", "怎么选", "推荐", "首选", "适合", "买什么车", "购车指南"))
    choice_context = contains_any(body, ("新手", "女生", "家庭", "家用", "通勤", "代步", "年轻人", "第一辆车", "买车"))
    if new_car_choice and choice_context and not is_used and (has_dcar or len(normalize_text(body)) >= 30):
        level, score = evidence_level(text, has_dcar, True)
        return "X4", score, level, "按预算、人群或场景展示选车能力"

    return None, 0, "", ""


def classify_candidate_or_fallback(
    row: dict[str, Any], duplicate_count: int
) -> tuple[str, int, str]:
    text = str(row.get("desc") or "").lower()
    body = strip_hashtags(text)
    uid = str(row.get("uid") or "")
    has_dcar = contains_any(text, ("懂车帝", "dcar"))
    norm_len = len(normalize_text(text))

    emotional = contains_any(
        body,
        ("情感", "婚姻", "前女友", "爱情", "童年", "治愈", "女性视角", "人生", "故事", "家庭关系", "父亲", "母亲"),
    )
    if uid in STORY_UIDS or emotional:
        return "NO_MATCH_STORY", 0, "主要是情绪、生活或身份表达，没有懂车帝平台任务"

    entertainment = contains_any(
        body,
        ("概念车", "如果把", "假如", "ai创作", "ai汽车", "机甲", "末日", "世界末日", "脑洞", "未来车"),
    )
    if uid in CONCEPT_UIDS or entertainment:
        return "NO_MATCH_ENTERTAINMENT", 0, "主要是汽车视觉娱乐、幻想或无来源概念内容"

    if contains_any(text, ("懂车帝玩车", "车友圈", "车友社区", "玩车", "改装交流")):
        return "C3", 83 if has_dcar else 76, "展示车友交流、兴趣讨论或玩车参与场景"

    knowledge = contains_any(
        body,
        (
            "汽车知识", "汽车零件", "用车知识", "用车技巧", "开车技巧", "驾驶技巧",
            "安全知识", "保养", "维修", "故障", "发动机", "变速箱", "轮胎", "刹车",
            "车灯", "玻璃水", "机油", "冷却液", "防冻液", "积碳", "底盘", "避坑",
            "泡水车", "火烧车", "事故车", "调表车", "查验", "鉴别", "如何判断",
            "电池", "密封圈", "护板", "车门", "启动电机", "雨刮", "点火线圈",
            "油门", "空调", "滤芯", "手刹", "充气泵", "灭火器", "汽车落水",
            "马力", "门把手", "充电桩", "车机", "差速器", "火花塞", "油泵",
            "变速箱油", "刹车油", "换挡", "爆胎", "仪表盘", "故障灯", "二手车平台",
            "买二手车", "购车指南", "买车流程", "卖车流程",
        ),
    )
    if knowledge or (uid in KNOWLEDGE_CAPABILITY_UIDS and norm_len >= 8):
        score = 84 if knowledge and norm_len >= 24 else 77
        return "C1", score, "提供可复用的汽车知识、用车问题或风险识别解答"

    factual_news = contains_any(body, ("正式上市", "正式发布", "官图", "预售", "申报图", "新车发布", "销量榜", "销量排行", "新款发布"))
    speculative = contains_any(body, ("概念车", "如果把", "假如", "未来会", "ai设计", "脑洞", "想象"))
    if factual_news and not speculative and norm_len >= 18:
        return "C4", 81 if has_dcar else 76, "提供新车发布、上市、销量或行业动态"

    model_experience = contains_any(
        body,
        (
            "内饰", "外观", "空间", "座舱", "后排", "驾驶感受", "乘坐", "车型细节",
            "氛围灯", "音响", "尾门", "天窗", "实车", "试驾", "动力", "隔音",
            "座椅", "储物", "后备箱", "悬架", "舒适", "操控", "自动泊车", "车漆",
            "车身线条", "方向盘", "油耗", "续航",
        ),
    )
    if has_dcar and model_experience:
        return "C2", 80 if model_experience else 75, "展示单车细节、真实影像或场景化体验"

    if duplicate_count >= 3 or norm_len < 12:
        return "NO_MATCH_DUPLICATE_LOW_INFO", 0, "文案高度重复或信息量过低，无法识别平台任务"

    if has_dcar:
        return "NO_MATCH_BRAND_ONLY", 0, "仅有懂车帝标签或品牌露出，没有明确平台能力"

    return "NO_MATCH_OTHER", 0, "未表达可由懂车帝承接的具体用户任务"


def qualitative(score: int, official_id: str, is_official: bool, is_candidate: bool) -> str:
    if official_id:
        if score >= 90:
            return "正式卖点强匹配"
        if score >= AUTO_INCLUDE:
            return "正式卖点明确匹配"
        if score >= 60:
            return "正式卖点弱匹配待复核"
    if is_candidate:
        return "候选卖点可解释，待业务确认"
    return "未命中懂车帝平台卖点"


def pct(n: int, d: int) -> float:
    return round(n * 100 / d, 2) if d else 0.0


def metric_block(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    official = [r for r in rows if r["official_included"]]
    pending = [r for r in rows if r["official_pending"]]
    core = [r for r in official if r["official_tier"] == "core"]
    candidate = [r for r in rows if r["candidate_included"]]
    unmatched = [r for r in rows if not r["official_included"] and not r["candidate_included"]]
    official_counts = Counter(r["official_primary_id"] for r in official)
    candidate_counts = Counter(r["candidate_primary_id"] for r in candidate)
    fallback_counts = Counter(r["fallback_id"] for r in unmatched)
    return {
        "total": total,
        "official_included": len(official),
        "official_coverage_pct": pct(len(official), total),
        "official_core": len(core),
        "official_core_share_all_pct": pct(len(core), total),
        "official_core_share_within_official_pct": pct(len(core), len(official)),
        "official_pending_review": len(pending),
        "official_pending_counts": dict(sorted(Counter(r["official_primary_id"] for r in pending).items())),
        "candidate_only": len(candidate),
        "candidate_explainable_share_pct": pct(len(candidate), total),
        "candidate_rescue_rate_among_official_unmatched_pct": pct(len(candidate), total - len(official)),
        "expanded_covered": len(official) + len(candidate),
        "expanded_coverage_pct": pct(len(official) + len(candidate), total),
        "expanded_unmatched": len(unmatched),
        "expanded_unmatched_share_pct": pct(len(unmatched), total),
        "official_counts": dict(sorted(official_counts.items())),
        "candidate_counts": dict(sorted(candidate_counts.items())),
        "fallback_counts": dict(sorted(fallback_counts.items())),
    }


def main() -> None:
    taxonomy = json.loads(TAXONOMY.read_text(encoding="utf-8"))
    names = label_name_map(taxonomy)
    meta = official_meta(taxonomy)
    source_rows = read_jsonl(SOURCE)
    normalized_counts = Counter(normalize_text(str(r.get("desc") or "")) for r in source_rows)

    labeled: list[dict[str, Any]] = []
    for row in source_rows:
        desc = str(row.get("desc") or "")
        norm = normalize_text(desc)
        duplicate_count = normalized_counts[norm]
        official_id, official_score, evidence, reason = classify_official(row)
        official_included = bool(official_id and official_score >= AUTO_INCLUDE)
        official_pending = bool(official_id and 60 <= official_score < AUTO_INCLUDE)

        candidate_id = ""
        candidate_score = 0
        fallback_id = ""
        if not official_included:
            candidate_or_fallback, candidate_score, reason = classify_candidate_or_fallback(row, duplicate_count)
            if candidate_or_fallback.startswith("C"):
                candidate_id = candidate_or_fallback
            else:
                fallback_id = candidate_or_fallback

        candidate_included = bool(candidate_id and candidate_score >= AUTO_INCLUDE)
        official_tier = meta.get(official_id or "", {}).get("tier", "")
        official_label = names.get(official_id or "", "")
        candidate_label = names.get(candidate_id, "")
        fallback_label = names.get(fallback_id, "")
        result = {
            "account_sample_index": row.get("account_sample_index"),
            "quality_label": row.get("quality_label", ""),
            "account_name": row.get("account_name", ""),
            "uid": str(row.get("uid") or ""),
            "aweme_id": str(row.get("aweme_id") or ""),
            "share_url": row.get("share_url", ""),
            "create_time_cn": row.get("create_time_cn", ""),
            "desc": desc,
            "digg_count": row.get("digg_count", 0),
            "comment_count": row.get("comment_count", 0),
            "official_primary_id": official_id or "",
            "official_primary_label": official_label,
            "official_tier": official_tier,
            "official_score": official_score,
            "official_included": official_included,
            "official_pending": official_pending,
            "evidence_level": evidence,
            "candidate_primary_id": candidate_id,
            "candidate_primary_label": candidate_label,
            "candidate_score": candidate_score if candidate_id else 0,
            "candidate_included": candidate_included,
            "fallback_id": fallback_id,
            "fallback_label": fallback_label,
            "official_qualitative": qualitative(official_score, official_id or "", official_included, False) if official_id else "无正式卖点",
            "candidate_qualitative": "候选卖点可解释，待业务确认" if candidate_included else "无候选卖点",
            "qualitative": qualitative(official_score, official_id or "", official_included, candidate_included),
            "decision_reason": reason,
            "normalized_creative_hash": hashlib.sha1(norm.encode("utf-8")).hexdigest()[:12],
            "normalized_duplicate_count": duplicate_count,
        }
        labeled.append(result)

    # A unique-creative row is represented by the first publication for each
    # normalized caption. This is reported alongside publication counts.
    seen: set[str] = set()
    dedup_rows: list[dict[str, Any]] = []
    for row in labeled:
        key = row["normalized_creative_hash"]
        if key not in seen:
            seen.add(key)
            dedup_rows.append(row)

    by_quality: dict[str, Any] = {}
    for quality in ("精品IP号", "原创号", "混剪号"):
        by_quality[quality] = metric_block([r for r in labeled if r["quality_label"] == quality])

    by_account: list[dict[str, Any]] = []
    grouped: dict[tuple[int, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in labeled:
        key = (int(row["account_sample_index"]), row["quality_label"], row["account_name"], row["uid"])
        grouped[key].append(row)
    for (index, quality, account, uid), rows in sorted(grouped.items()):
        item = {"account_sample_index": index, "quality_label": quality, "account_name": account, "uid": uid}
        item.update(metric_block(rows))
        by_account.append(item)

    summary = {
        "generated_at": "2026-08-01",
        "taxonomy_version": taxonomy["taxonomy_version"],
        "method": {
            "official_threshold": AUTO_INCLUDE,
            "official_rule": "仅明确平台任务或完整能力演示计入；品牌标签或账号归属单独不计入",
            "candidate_rule": "仅解释正式未命中内容，不进入正式核心卖点占比",
            "content_evidence": "本轮批量判定使用作品文案、话题和账号页上下文；未做全量视频ASR/OCR",
            "dedup_rule": "删除话题、链接、空白和标点后，标准化文案完全相同视为同一创意",
        },
        "publication_metrics": metric_block(labeled),
        "deduplicated_creative_metrics": metric_block(dedup_rows),
        "by_quality_label": by_quality,
        "by_account": by_account,
        "duplicate_publications": len(labeled) - len(dedup_rows),
        "duplicate_creative_groups": sum(1 for count in normalized_counts.values() if count > 1),
    }

    with OUT_JSONL.open("w", encoding="utf-8") as handle:
        for row in labeled:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    fields = list(labeled[0].keys())
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(labeled)

    cn_fields = {
        "account_sample_index": "账号序号",
        "quality_label": "账号类型",
        "account_name": "账号",
        "uid": "UID",
        "aweme_id": "作品ID",
        "share_url": "作品链接",
        "create_time_cn": "发布时间",
        "desc": "作品文案",
        "digg_count": "点赞数",
        "comment_count": "评论数",
        "official_primary_id": "正式卖点ID",
        "official_primary_label": "正式卖点",
        "official_tier": "正式卖点层级",
        "official_score": "正式匹配分",
        "official_qualitative": "正式定性",
        "official_included": "是否计入正式卖点",
        "official_pending": "是否待视频复核",
        "evidence_level": "证据等级",
        "candidate_primary_id": "候选卖点ID",
        "candidate_primary_label": "候选卖点",
        "candidate_score": "候选匹配分",
        "candidate_qualitative": "候选定性",
        "fallback_label": "未命中原因",
        "decision_reason": "判断说明",
        "normalized_creative_hash": "标准化创意ID",
        "normalized_duplicate_count": "相同文案发布次数",
    }
    with OUT_CSV_CN.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(cn_fields.values()))
        writer.writeheader()
        for row in labeled:
            writer.writerow({cn_fields[key]: row.get(key, "") for key in cn_fields})

    OUT_SUMMARY.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary["publication_metrics"], ensure_ascii=False, indent=2))
    print(json.dumps(summary["deduplicated_creative_metrics"], ensure_ascii=False, indent=2))
    print(f"wrote {OUT_JSONL}")
    print(f"wrote {OUT_CSV}")
    print(f"wrote {OUT_CSV_CN}")
    print(f"wrote {OUT_SUMMARY}")


if __name__ == "__main__":
    main()
