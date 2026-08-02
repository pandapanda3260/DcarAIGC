#!/usr/bin/env python3
"""Apply the unified v3 selling-point taxonomy to ASR/OCR-enriched content."""

from __future__ import annotations

import csv
import difflib
import hashlib
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from project_paths import CONFIG_DIR, CURRENT_REPORTS_DIR, DOUYIN_INPUT_DIR, DOUYIN_MEDIA_CACHE_DIR, DOUYIN_PROCESSED_DIR


SOURCE = DOUYIN_INPUT_DIR / "douyin_30_account_content_sample_2026-08-01.jsonl"
TAXONOMY = CONFIG_DIR / "business_selling_points_v3_final.json"
ANALYSIS = DOUYIN_MEDIA_CACHE_DIR
TRANSCRIPT_DIR = ANALYSIS / "transcripts"
OCR_DIR = ANALYSIS / "ocr"
MEDIA_DIR = ANALYSIS / "media"
VISUAL_REVIEWS = ANALYSIS / "visual_reviews.jsonl"
OUT_JSONL = DOUYIN_PROCESSED_DIR / "douyin_selling_point_labels_v3_video_2026-08-01.jsonl"
OUT_CSV = CURRENT_REPORTS_DIR / "抖音438条内容卖点逐条结果_v3_视频终版_2026-08-01.csv"
OUT_SUMMARY = DOUYIN_PROCESSED_DIR / "douyin_selling_point_summary_v3_video_2026-08-01.json"

INCLUDE_MIN = 75
REVIEW_MIN = 60
CORE_IDS = {"E1", "E2", "X1", "X2", "X3", "M1", "M2", "M3"}
PRIORITY = {
    "M1": 1, "M2": 1, "M3": 1, "M4": 1, "M5": 1, "M6": 1,
    "E1": 2, "E2": 2, "E5": 2, "E7": 2, "E4": 2, "E3": 3, "E6": 4,
    "X4": 1, "X1": 2, "X2": 2, "X3": 2, "X5": 3, "X6": 3, "X7": 3, "X8": 3,
    "C3": 5, "C1": 6, "C2": 6, "C4": 6,
}


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def strip_hashtags(text: str) -> str:
    return re.sub(r"#([^#\s]+)", " ", text.replace("＃", "#"))


def canonical(text: str) -> str:
    substitutions = {
        "总车帝": "懂车帝", "董车帝": "懂车帝", "懂车弟": "懂车帝", "懂车地": "懂车帝",
        "总车递": "懂车帝", "总车第一": "懂车帝", "懂车第一": "懂车帝", "等车帝": "懂车帝",
        "懂事得": "懂车帝", "董车弟": "懂车帝", "懂车递": "懂车帝", "堵车币": "懂车帝",
        "小东": "小懂", "ai 小懂": "AI小懂", "ai小懂": "AI小懂",
    }
    output = text
    for old, new in substitutions.items():
        output = output.replace(old, new)
    return output


def normalized_creative(text: str) -> str:
    text = strip_hashtags(text.lower())
    text = re.sub(r"https?://\S+", " ", text)
    return re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", text)


def contains(text: str, terms: tuple[str, ...] | list[str]) -> bool:
    low = text.lower()
    return any(term.lower() in low for term in terms)


def term_count(text: str, terms: tuple[str, ...] | list[str]) -> int:
    low = text.lower()
    return sum(1 for term in terms if term.lower() in low)


def chinese_count(text: str) -> int:
    return len(re.findall(r"[\u4e00-\u9fff]", text))


def transcript_is_repetitive(transcript: dict[str, Any]) -> bool:
    segments = [normalized_creative(str(item.get("text") or "")) for item in (transcript.get("segments") or [])]
    segments = [item for item in segments if item]
    if len(segments) < 4:
        return False
    most_common = Counter(segments).most_common(1)[0][1]
    return most_common / len(segments) >= 0.6


def evidence_level(
    media_exists: bool,
    transcript: dict[str, Any],
    ocr: dict[str, Any],
    desc: str,
    visual: dict[str, Any] | None = None,
) -> tuple[str, str]:
    visual = visual or {}
    asr = canonical(str(transcript.get("text") or ""))
    ocr_text = canonical(str(ocr.get("combined_text") or ""))
    asr_chars = chinese_count(asr)
    avg_logprob = float(transcript.get("avg_logprob") or -9)
    asr_ok = (
        transcript.get("status") == "success"
        and asr_chars >= 15
        and (avg_logprob > -1.2 or asr_chars >= 80)
        and not transcript_is_repetitive(transcript)
    )
    ocr_ok = (
        ocr.get("status") == "success"
        and chinese_count(ocr_text) >= 15
        and int(ocr.get("ocr_observation_count") or 0) >= 2
    )
    visual_ok = visual.get("status") == "reviewed" and visual.get("confidence") in {"high", "medium"} and chinese_count(str(visual.get("summary") or "")) >= 8
    if media_exists and asr_ok and (ocr_ok or visual_ok):
        return "V3", "视频ASR与关键帧OCR/画面语义均可用"
    if media_exists and (asr_ok or ocr_ok or visual_ok):
        return "V2", "视频ASR、连续OCR或关键帧画面语义可覆盖主要内容"
    if desc.strip():
        return "V1", "仅正文/标题等文字可用，视频语义证据不足"
    return "V0", "视频与有效文字均不可用"


def exact_price(text: str) -> bool:
    patterns = (
        r"\d+(?:\.\d+)?\s*万(?!\s*(?:公里|千米|km))",
        r"\d{1,3}(?:[,，]\d{3})+",
        r"[一二三四五六七八九十两]+万[一二三四五六七八九十百千]*",
        r"\d+(?:\.\d+)?\s*[wW]",
        r"\d{4,7}\s*(?:元|块钱)",
    )
    return any(re.search(pattern, text, re.IGNORECASE) for pattern in patterns)


def current_price_mention(text: str) -> bool:
    """Require a concrete price and a nearby current-price cue.

    This avoids treating mileage ("6万公里"), story amounts, repair costs, or
    an old purchase price elsewhere in a long narration as a current car quote.
    """
    low = re.sub(r"\s+", "", text.lower())
    value_pattern = re.compile(
        r"\d+(?:\.\d+)?万(?!公里|千米|km)|\d{1,3}(?:[,，]\d{3})+|"
        r"[一二三四五六七八九十两]+万[一二三四五六七八九十百千]*|"
        r"\d+(?:\.\d+)?[wW]|\d{4,7}(?:元|块钱)|(?<!\d)\d{5,6}(?!\d|公里|千米|km)",
        re.IGNORECASE,
    )
    specific_cues = (
        "最新价", "一口价", "指导价", "售价", "裸车价", "落地价", "渠道价", "降到", "降至",
        "跌到", "杀到", "干到", "砸到", "低至", "优惠后", "优惠完", "起售价", "价格来到",
        "价格是", "价格为", "价格直接", "万起", "元起", "起步",
    )
    generic_cues = ("现在", "现价", "只要", "仅售")
    auto_markers = (
        "车型", "车价", "买车", "提车", "开走", "车主", "新车", "款", "版", "奔驰", "宝马", "奥迪",
        "大众", "丰田", "本田", "日产", "别克", "福特", "现代", "起亚", "沃尔沃", "凯迪拉克",
        "特斯拉", "比亚迪", "吉利", "领克", "极氪", "长安", "奇瑞", "捷途", "哈弗", "坦克",
        "小鹏", "理想", "蔚来", "零跑", "问界", "小米", "五菱", "红旗", "速腾", "桑塔纳",
        "轩逸", "凯美瑞", "雅阁", "迈腾", "高尔夫", "思域", "model",
    )
    bare_number_auto_markers = tuple(
        marker for marker in auto_markers if marker not in {"款", "版", "车主", "买车", "提车", "开走"}
    )
    non_vehicle_amount_markers = ("奖金", "借钱", "贷款", "维修费", "保养费", "停车费", "收入", "工资", "成本", "一套", "公里")
    historical_cues = ("当年", "以前", "曾经", "那年", "早年", "原来花", "之前花")
    for match in value_pattern.finditer(low):
        before = low[max(0, match.start() - 18) : match.start()]
        after = low[match.end() : min(len(low), match.end() + 12)]
        nearby = before + after
        wider = low[max(0, match.start() - 30) : min(len(low), match.end() + 30)]
        has_specific_cue = any(cue in nearby for cue in specific_cues)
        has_generic_auto_cue = any(cue in nearby for cue in generic_cues) and any(marker in wider for marker in auto_markers)
        bare_number = bool(re.fullmatch(r"\d{5,6}", match.group()))
        if bare_number and not (has_specific_cue and any(marker in wider for marker in bare_number_auto_markers)):
            continue
        if not (has_specific_cue or has_generic_auto_cue):
            continue
        if any(marker in nearby for marker in non_vehicle_amount_markers) and not has_specific_cue:
            continue
        if any(cue in before for cue in historical_cues) and not any(
            cue in before for cue in ("现在", "现价", "一口价", "指导价", "售价", "只要", "降到", "降至", "低至", "渠道价")
        ):
            continue
        return True
    return False


def snippet(text: str, terms: tuple[str, ...] | list[str], width: int = 150) -> str:
    low = text.lower()
    positions = [low.find(term.lower()) for term in terms if low.find(term.lower()) >= 0]
    start = max(0, (min(positions) if positions else 0) - 30)
    return re.sub(r"\s+", " ", text[start : start + width]).strip()


def terms_near(
    text: str,
    left_terms: tuple[str, ...],
    right_terms: tuple[str, ...],
    distance: int = 50,
) -> bool:
    low = text.lower()
    left_positions = [match.start() for term in left_terms for match in re.finditer(re.escape(term.lower()), low)]
    right_positions = [match.start() for term in right_terms for match in re.finditer(re.escape(term.lower()), low)]
    return any(abs(left - right) <= distance for left in left_positions for right in right_positions)


def score_match(
    point_id: str,
    semantic: int,
    benefit: int,
    evidence: str,
    explicit_dcar: bool,
    in_asr: bool,
    in_ocr: bool,
    in_desc: bool,
) -> tuple[int, dict[str, int]]:
    linkage = 25 if explicit_dcar or point_id.startswith("M") else 18
    label_has_video_evidence = in_asr or in_ocr
    video_score = {"V3": 20, "V2": 16, "V1": 6, "V0": 0}[evidence] if label_has_video_evidence else (6 if in_desc else 0)
    source_count = sum((in_asr, in_ocr, in_desc))
    prominence = 10 if source_count >= 2 and (in_asr or in_ocr) else 8 if in_asr or in_ocr else 4
    dimensions = {
        "semantic_fit": semantic,
        "dcar_linkage": linkage,
        "video_evidence": video_score,
        "user_benefit": benefit,
        "narrative_prominence": prominence,
    }
    score = sum(dimensions.values())
    cap = {"V3": 100, "V2": 90, "V1": 74, "V0": 0}[evidence]
    if not label_has_video_evidence:
        cap = min(cap, 74)
    return min(score, cap), dimensions


def match_points(
    row: dict[str, Any],
    transcript: dict[str, Any],
    ocr: dict[str, Any],
    evidence: str,
    visual: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    visual = visual or {}
    desc = canonical(strip_hashtags(str(row.get("desc") or "")))
    full_desc = canonical(str(row.get("desc") or ""))
    asr = canonical(str(transcript.get("text") or ""))
    ocr_text = canonical(str(ocr.get("combined_text") or ""))
    visual_text = canonical(str(visual.get("summary") or ""))
    video_text = f"{asr}\n{ocr_text}\n{visual_text}"
    combined = f"{video_text}\n{desc}"
    explicit_dcar = contains(f"{video_text}\n{desc}", ("懂车帝", "DCar", "AI小懂"))
    has_dcar_any = explicit_dcar or contains(full_desc, ("懂车帝", "DCar"))
    has_dcar_video = contains(video_text, ("懂车帝", "DCar", "AI小懂"))
    is_image_post = str(row.get("content_type") or "") == "image_text" or int(row.get("media_type") or 0) == 2
    used_narrative_terms = ("二手车", "二手行情", "二手平台", "二手车源", "买二手", "卖二手")
    used_support_terms = ("车况", "里程", "过户", "车源", "检测报告", "验车", "二手行情", "二手平台", "估价")
    strong_used_listing_terms = ("同一车源", "表显里程", "上牌时间", "免费估价", "车况报告", "二手行情")
    used_basis = combined if is_image_post else f"{asr}\n{visual_text}\n{desc}"
    used_narrative_count = sum(used_basis.count(term) for term in used_narrative_terms)
    new_car_count = sum(used_basis.count(term) for term in ("新车", "一手车", "一手新车", "全新车"))
    is_used = (
        used_narrative_count > 0 and used_narrative_count >= max(1, new_car_count)
    ) or (
        "二手车" in full_desc
        and contains(combined, used_support_terms)
        and (new_car_count == 0 or contains(combined, strong_used_listing_terms))
    )
    matches: list[dict[str, Any]] = []

    def add(
        point_id: str,
        terms: tuple[str, ...],
        reason: str,
        semantic: int = 30,
        benefit: int = 13,
        source_flags: tuple[bool, bool, bool, bool] | None = None,
    ) -> None:
        source_terms = tuple(term for term in terms if term)
        if source_flags is None:
            in_asr = contains(asr, source_terms)
            in_ocr = contains(ocr_text, source_terms)
            in_visual = contains(visual_text, source_terms)
            in_desc = contains(desc, source_terms)
        else:
            in_asr, in_ocr, in_visual, in_desc = source_flags
        score, dimensions = score_match(point_id, semantic, benefit, evidence, explicit_dcar, in_asr, in_ocr or in_visual, in_desc)
        matches.append(
            {
                "id": point_id,
                "score": score,
                "dimensions": dimensions,
                "reason": reason,
                "evidence_snippet": snippet(asr if in_asr else ocr_text if in_ocr else visual_text if in_visual else desc, source_terms),
                "source": "ASR" if in_asr else "OCR" if in_ocr else "关键帧画面语义" if in_visual else "正文",
            }
        )

    # AI Xiaodong labels require explicit product naming in actual content.
    ai_text = f"{video_text}\n{desc}"
    if contains(ai_text, ("AI小懂", "小懂助手")):
        if contains(ai_text, ("预约试驾", "试驾指南", "试驾清单")):
            add("M1", ("AI小懂", "预约试驾", "试驾指南"), "明确展示AI小懂预约试驾或试驾指南", benefit=15)
        if contains(ai_text, ("库存", "现车", "展车", "试驾车", "真实优惠")):
            add("M2", ("AI小懂", "库存", "现车", "真实优惠"), "明确展示AI小懂查询优惠或库存", benefit=15)
        if contains(ai_text, ("洗车", "维修保养", "保养预约", "车档案", "用车账本")):
            add("M3", ("AI小懂", "洗车", "维修保养", "车档案"), "明确展示AI小懂管理日常用车", benefit=15)
        if contains(ai_text, ("选车", "推荐", "怎么选", "对比")):
            add("M4", ("AI小懂", "选车", "推荐", "对比"), "明确展示AI小懂问答选车")
        if contains(ai_text, ("故障", "报警灯", "异响", "安全处置")):
            add("M5", ("AI小懂", "故障", "报警灯", "异响"), "明确展示AI小懂故障解释或处置建议", benefit=15)
        if contains(ai_text, ("估价", "卖车")):
            add("M6", ("AI小懂", "估价", "卖车"), "明确展示AI小懂估价或卖车准备")

    # Used-car platform tasks.
    if is_used and contains(combined, ("收车", "上门评估", "高价卖车", "卖车到账", "专业收车")) and has_dcar_any:
        add("E1", ("收车", "上门评估", "卖车到账", "专业收车"), "展示懂车帝收车、评估或到账服务", benefit=15)
    if is_used and contains(combined, ("认证车源", "个人车源", "商家车源", "透明车况", "平台保障", "选车估价验车")) and has_dcar_any:
        add("E2", ("认证车源", "个人车源", "透明车况", "平台保障", "选车估价验车"), "展示懂车帝车源、车况或平台保障", benefit=15)
    if is_used and contains(combined, ("检测报告", "车辆档案", "车况报告", "懂车帝检测", "专业验车")) and has_dcar_video:
        add("E5", ("检测报告", "车辆档案", "车况报告", "专业验车"), "展示懂车帝检测报告或车辆档案", benefit=15)
    if is_used and has_dcar_any and contains(combined, ("估价", "保值率", "成交行情", "市场行情", "历史价格", "同一车源不同价格")):
        add("E6", ("估价", "保值率", "成交行情", "市场行情", "历史价格", "同一车源不同价格"), "展示二手车估价、保值率或行情数据", benefit=15)
    if is_used and contains(combined, ("过户", "交易合同", "交易流程", "权益保障")) and has_dcar_any:
        add("E7", ("过户", "交易合同", "交易流程", "权益保障"), "展示二手车交易流程或权益保障", benefit=15)
    used_budget_amount = bool(
        re.search(r"预算.{0,10}(?:\d+(?:\.\d+)?万|[一二三四五六七八九十两]+万)", combined)
        or re.search(r"\d+(?:\.\d+)?万以内", combined)
        or re.search(r"\d+(?:\.\d+)?(?:[-—到至]\d+(?:\.\d+)?)?万.{0,8}预算", combined)
    )
    used_recommendation = contains(combined, ("车型推荐", "推荐车型", "值得买", "怎么选", "如何选", "首选", "性价比二手车"))
    used_budget = used_budget_amount or used_recommendation
    used_scene = contains(combined, ("新手", "家庭", "家用", "女生", "通勤", "代步", "宝妈"))
    if is_used and used_budget:
        add("E4" if used_scene else "E3", ("预算", "万以内", "怎么选", "推荐", "新手", "家用"), "展示按预算或场景筛选二手车")

    # New-car decision tasks.
    compare_terms = ("全面对比", "横向对比", "同级对比", "二选一", "选哪个", "vs", "PK")
    dimensions = ("空间", "动力", "配置", "油耗", "续航", "价格", "舒适", "操控", "安全", "成本", "智能")
    car_brands = (
        "奔驰", "宝马", "奥迪", "大众", "丰田", "本田", "日产", "别克", "福特", "现代", "起亚",
        "沃尔沃", "凯迪拉克", "雷克萨斯", "保时捷", "路虎", "捷豹", "特斯拉", "比亚迪", "吉利",
        "领克", "极氪", "长安", "奇瑞", "捷途", "哈弗", "坦克", "小鹏", "理想", "蔚来", "零跑",
        "鸿蒙智行", "问界", "智界", "享界", "尊界", "小米", "五菱", "红旗", "广汽传祺", "腾势",
        "岚图", "阿维塔", "深蓝", "荣威", "名爵", "马自达", "斯巴鲁", "雪佛兰", "标致", "雪铁龙",
    )
    explicit_two_candidates = contains(
        combined,
        ("两款车", "两台车", "这两款", "这两台", "同级车型", "同级竞品", "二选一", "选哪个", "对手"),
    ) or term_count(combined, car_brands) >= 2
    comparison_basis = combined if is_image_post else f"{asr}\n{visual_text}\n{desc}"
    if not is_used and contains(comparison_basis, compare_terms) and explicit_two_candidates and term_count(comparison_basis, dimensions) >= 2:
        add("X1", compare_terms + dimensions, "对至少两款候选车型进行多维度比较", benefit=15)
    x2_strong_terms = ("懂车帝实测", "实测榜", "权威测评", "车主口碑", "车主评价", "长期测评")
    x2_score_terms = ("懂车分",)
    sales_topic = contains(combined, ("销量", "销量榜", "销量排行", "卖不动", "月销"))
    x2_narrative_text = f"{asr}\n{visual_text}\n{desc}"
    x2_supported = contains(x2_narrative_text, x2_strong_terms) or (
        (contains(combined, x2_strong_terms) or contains(combined, x2_score_terms)) and not sales_topic
    )
    if x2_supported:
        add("X2", ("懂车帝实测", "实测榜", "权威测评", "车主口碑", "车主评价", "懂车分"), "展示第三方实测、榜单或车主口碑", benefit=15)
    price_terms = ("价格", "现价", "降价", "直降", "优惠", "一口价", "落地价", "裸车价", "现车", "售价", "只要", "免息")
    def price_match(text: str) -> bool:
        if not current_price_mention(text):
            return False
        speculative_price = contains(text, ("据说", "预计", "预测", "可能", "估计", "网传", "传闻", "即将", "有望", "或将", "听说", "概念车", "渲染图"))
        verified_price = contains(text, ("指导价", "渠道价", "一口价", "裸车价", "落地价", "优惠后", "优惠完", "懂车帝降价榜", "实时查看降价"))
        return not speculative_price or verified_price

    price_sources = (price_match(asr), price_match(ocr_text), price_match(visual_text), price_match(desc))
    quote_capability_sources = tuple(
        contains(text, ("查看当地经销商报价", "查询经销商报价", "查看最新报价", "查询最新报价", "对比价格", "查询底价", "查询真实成交价", "查真实成交价"))
        and contains(text, ("懂车帝", "DCar"))
        for text in (asr, ocr_text, visual_text, desc)
    )
    price_sources = tuple(a or b for a, b in zip(price_sources, quote_capability_sources))
    new_car_context = contains(combined, ("新车", "全新", "指导价", "经销商", "现车", "裸车价", "新车价格"))
    if (not is_used or new_car_context) and any(price_sources):
        add("X3", price_terms, "视频提供具体价格、优惠或现车信息", benefit=15, source_flags=price_sources)
    x4_scene_terms = ("新手", "女生", "家庭", "家用", "通勤", "代步", "年轻人", "第一辆车", "工资", "月薪", "收入")

    def x4_match(text: str) -> bool:
        explicit_selection = (
            contains(text, ("买什么车", "配什么车", "该配什么车", "车型推荐", "推荐车型", "怎么选车", "如何选车", "车怎么选", "车型怎么选", "买车怎么选"))
            or terms_near(text, ("第一辆车",), ("买", "选", "推荐"), distance=50)
        )
        budget_selection = terms_near(text, ("预算",), ("选车", "推荐", "车型", "买车"))
        scene_selection = terms_near(text, x4_scene_terms, ("适合", "推荐", "选车", "买什么车", "怎么选车", "如何选车"))
        return explicit_selection or budget_selection or scene_selection

    x4_sources = (x4_match(asr), x4_match(ocr_text), x4_match(visual_text), x4_match(desc))
    if not is_used and any(x4_sources):
        add(
            "X4",
            ("预算", "选车", "怎么选", "如何选", "车型推荐", "推荐车型", "适合", "新手", "家用", "买什么车", "配什么车", "工资", "月薪", "收入"),
            "按预算、人群或用车场景推荐车型",
            source_flags=x4_sources,
        )
    if contains(combined, ("版本对比", "配置对比", "高低配", "哪个版本", "选装包", "配置差异")):
        add("X5", ("版本对比", "配置对比", "高低配", "哪个版本", "选装包", "配置差异"), "比较版本、配置差异或配置价值")
    cost_terms = ("养车成本", "用车成本", "一年要花", "月供", "贷款方案", "保险油费", "每公里成本", "保养成本", "维修费用", "保险费用", "能耗成本")
    cost_categories = ("保养", "维修", "保险", "油费", "电费", "月供", "贷款", "折旧", "保值")
    cost_supported = contains(combined, cost_terms) and (
        contains(combined, ("一年要花", "每公里", "每月", "每年", "算下来", "合计", "总成本"))
        or term_count(combined, cost_categories) >= 2
    )
    if cost_supported:
        add("X6", cost_terms, "计算购车或长期用车成本", benefit=15)

    risk_terms = ("减配", "虚标", "营销话术", "智商税", "配置骗局", "参数骗局")
    validation_terms = ("拆解", "验证", "实测", "测试数据", "数据对比", "实验", "测出来")
    if contains(combined, risk_terms) and contains(combined, validation_terms):
        add("X7", risk_terms + validation_terms, "以数据、实测或拆解验证配置和营销话术", benefit=15)

    x8_terms = ("购车合同", "交定金", "退定金", "交付权益", "购车权益", "提车周期", "选配合同")
    x8_instruction = ("注意", "检查", "看清", "流程", "避坑", "怎么", "如何", "不要", "必须", "签订", "条款")
    if contains(combined, x8_terms) and (
        term_count(combined, x8_terms) >= 2
        or contains(combined, ("购车合同", "退定金", "交付权益", "购车权益", "选配合同")) and contains(combined, x8_instruction)
    ):
        add("X8", x8_terms + x8_instruction, "解释购车合同、定金、交付或权益", benefit=15)

    # Integrated content and community selling points.
    c3_actual = f"{asr}\n{visual_text}"
    c3_narrative = f"{full_desc}\n{c3_actual}"
    c3_campaign_signal = contains(
        c3_actual,
        ("玩车社区", "趣玩车社区", "主题改装大赛", "电车还能这么玩", "改装玩家", "上传你的改装作品"),
    )
    if (c3_campaign_signal and has_dcar_any) or contains(c3_actual, ("懂车帝玩车社区", "懂车帝趣玩车社区", "懂车帝车友圈")) or (
        contains(c3_actual, ("车友社区", "车友圈", "改装交流", "车友分享"))
        and contains(c3_actual, ("交流", "分享", "改装", "帖子", "车友", "社区"))
        and has_dcar_any
    ):
        add(
            "C3",
            ("懂车帝玩车社区", "懂车帝趣玩车社区", "懂车帝车友圈", "玩车社区", "主题改装大赛", "电车还能这么玩", "车友社区", "车友圈"),
            "展示懂车帝车友社区、玩车或交流场景",
        )

    speculative = contains(combined, ("概念车", "如果把", "假如", "AI设计", "脑洞", "想象", "机甲", "末日"))
    factual_news = contains(combined, ("正式上市", "正式发布", "官图", "预售", "申报图", "新车发布", "销量榜", "销量排行", "新款发布", "发布会"))
    if factual_news and not speculative:
        add("C4", ("正式上市", "正式发布", "官图", "预售", "申报图", "销量榜", "新款发布", "发布会"), "提供有事实内容的新车或行业动态")

    knowledge_terms = (
        "保养", "维修", "故障", "发动机", "变速箱", "轮胎", "刹车", "玻璃水", "机油",
        "冷却液", "防冻液", "底盘", "安全驾驶", "用车技巧", "驾驶技巧", "避坑", "事故车",
        "泡水车", "火烧车", "调表车", "如何判断", "电池", "油门", "空调", "滤芯", "手刹",
        "充气泵", "灭火器", "汽车落水", "门把手", "充电桩", "差速器", "火花塞", "油泵",
        "换挡", "爆胎", "仪表盘", "故障灯", "二手车平台", "买二手车", "买车流程",
        "汽油", "燃油标号", "加错油", "92号", "95号", "98号",
        "快充", "充电枪", "油表", "油箱", "燃油泵", "搭电线", "拖车绳", "安全锤",
        "隐藏把手", "机械把手", "电动把手", "车门限位器", "安全带导向环", "调表车",
        "雨刮", "雨刮片", "暴雨", "车机", "OTA", "起动机", "启动电机", "喷油嘴",
        "高压泵", "低压泵", "油路", "油温", "刹车灯", "手动挡", "离合", "车钥匙",
        "织物座椅", "真皮座椅", "硅胶管", "编织管", "机械锁扣", "智能感应", "电子手刹",
        "悬挂", "空气悬架", "螺旋弹簧", "车载大屏", "智能大屏", "手机支架",
    )
    instructional_terms = (
        "怎么", "如何", "为什么", "原理", "教你", "讲清楚", "区别", "注意", "记住", "判断",
        "检查", "作用", "千万别", "不要", "正确", "误区", "步骤", "方法", "先看", "再看", "一定要", "vs", "对比",
    )
    story_terms = (
        "人生副本", "小时候", "父亲", "爸爸", "妈妈", "老公", "婚姻", "前女友", "成长", "体面",
        "家里", "家庭", "那年", "回忆", "眼泪", "治愈", "情感", "情感故事", "女性视角", "童年", "剧情", "短片",
    )
    knowledge_signal = contains(combined, instructional_terms) or term_count(combined, knowledge_terms) >= 2
    explicit_tutorial = contains(
        video_text,
        ("记住", "注意", "教你", "步骤", "方法", "指南", "避坑", "正确操作", "千万别", "必须", "一定要", "如何判断"),
    )
    explicit_platform_task = has_dcar_video and contains(video_text, ("查询", "查过", "检测", "报告", "车况", "数据", "功能", "攻略", "技巧"))
    story_without_instruction = contains(full_desc, story_terms) and not explicit_tutorial and not explicit_platform_task
    if contains(combined, knowledge_terms) and knowledge_signal and chinese_count(video_text) >= 15 and not story_without_instruction:
        add("C1", knowledge_terms, "视频实质提供汽车知识、用车问题或风险识别解答")

    model_terms = (
        "内饰", "外观", "空间", "座舱", "后排", "驾驶感受", "乘坐体验", "氛围灯", "音响",
        "电动尾门", "天窗", "实车", "试驾感受", "动力表现", "隔音", "座椅", "储物", "后备箱",
        "悬架", "操控", "自动泊车", "车身线条",
    )
    emotional_only = contains(combined, ("精神内耗", "情绪", "人生", "成长", "治愈", "身份", "体面", "父亲", "爸爸", "妈妈", "婚姻", "回忆", "那年"))
    descriptive_terms = ("实拍", "体验", "展示", "来看", "带你看", "试驾", "测量", "打开", "乘坐", "坐进", "后排", "储物")
    model_signal = contains(combined, descriptive_terms) or term_count(combined, model_terms) >= 2
    story_model_task_text = combined if is_image_post else f"{asr}\n{visual_text}"
    story_model_platform_task = contains(story_model_task_text, ("懂车帝", "DCar")) and terms_near(
        story_model_task_text,
        ("懂车帝",),
        ("实拍", "图片", "空间", "后备箱", "参数", "内饰", "外观", "车主体验"),
        distance=120,
    )
    strong_story = contains(full_desc, story_terms)
    if (
        contains(combined, model_terms)
        and model_signal
        and chinese_count(video_text) >= 15
        and not (emotional_only and not contains(combined, descriptive_terms))
        and (not strong_story or story_model_platform_task)
    ):
        add("C2", model_terms, "视频实质展示车型细节、真实影像或使用体验")

    # Remove lower-priority content labels when the exact same evidence has a
    # clearer product/transaction interpretation.
    unique: dict[str, dict[str, Any]] = {}
    for item in matches:
        previous = unique.get(item["id"])
        if previous is None or item["score"] > previous["score"]:
            unique[item["id"]] = item
    return sorted(unique.values(), key=lambda item: (-item["score"], PRIORITY.get(item["id"], 99), item["id"]))


def no_match_reason(text: str) -> tuple[str, str]:
    if contains(text, ("情感", "婚姻", "爱情", "童年", "治愈", "女性视角", "人生故事")):
        return "NO_MATCH_STORY", "情绪或生活故事，没有懂车帝用户任务"
    if contains(text, ("概念车", "如果把", "假如", "AI创作", "机甲", "末日", "脑洞", "合体")):
        return "NO_MATCH_ENTERTAINMENT", "汽车娱乐、AI幻想或概念视觉，没有稳定平台任务"
    if "懂车帝" in text:
        return "NO_MATCH_BRAND_ONLY", "只有懂车帝品牌或标签，没有可识别平台任务"
    return "NO_MATCH_OTHER", "未识别出统一卖点标签所要求的用户任务"


def qualitative(score: int, included: bool, pending: bool) -> str:
    if included and score >= 90:
        return "强匹配"
    if included:
        return "明确匹配"
    if pending:
        return "弱匹配待复核"
    return "未命中"


def pct(n: int, d: int) -> float:
    return round(n * 100 / d, 2) if d else 0.0


def metric_block(rows: list[dict[str, Any]]) -> dict[str, Any]:
    # V1 is useful for a screening queue, but the final standard explicitly
    # forbids treating caption-only content as an evaluable final conclusion.
    final_evaluable = [row for row in rows if row["evidence_level"] in {"V2", "V3"}]
    incomplete = [row for row in rows if row["evidence_level"] in {"V0", "V1"}]
    included = [row for row in final_evaluable if row["included"]]
    core = [row for row in included if row["primary_tier"] == "core"]
    pending = [row for row in final_evaluable if row["pending"]]
    no_match = [row for row in final_evaluable if not row["included"] and not row["pending"]]
    return {
        "total": len(rows),
        "final_evaluable": len(final_evaluable),
        "final_evaluable_pct": pct(len(final_evaluable), len(rows)),
        "evidence_incomplete": len(incomplete),
        "evidence_incomplete_pct": pct(len(incomplete), len(rows)),
        "included": len(included),
        "unified_coverage_pct": pct(len(included), len(final_evaluable)),
        "core": len(core),
        "core_share_all_publications_pct": pct(len(core), len(rows)),
        "core_share_final_evaluable_pct": pct(len(core), len(final_evaluable)),
        "core_share_within_included_pct": pct(len(core), len(included)),
        "pending": len(pending),
        "pending_pct": pct(len(pending), len(final_evaluable)),
        "no_match": len(no_match),
        "no_match_pct": pct(len(no_match), len(final_evaluable)),
        "primary_counts": dict(sorted(Counter(row["primary_id"] for row in included).items())),
        "evidence_counts": dict(sorted(Counter(row["evidence_level"] for row in rows).items())),
        "no_match_counts": dict(sorted(Counter(row["no_match_id"] for row in no_match).items())),
    }


def main() -> None:
    source = read_jsonl(SOURCE)
    taxonomy = json.loads(TAXONOMY.read_text(encoding="utf-8"))
    label_map = {item["id"]: item for item in taxonomy["labels"]}
    visual_map = {str(item.get("aweme_id")): item for item in read_jsonl(VISUAL_REVIEWS)} if VISUAL_REVIEWS.exists() else {}
    results: list[dict[str, Any]] = []

    for row in source:
        aweme_id = str(row["aweme_id"])
        transcript = read_json(TRANSCRIPT_DIR / f"{aweme_id}.json")
        ocr = read_json(OCR_DIR / f"{aweme_id}.json")
        visual = visual_map.get(aweme_id, {})
        media_exists = (MEDIA_DIR / f"{aweme_id}.mp4").exists()
        evidence, evidence_note = evidence_level(media_exists, transcript, ocr, str(row.get("desc") or ""), visual)
        matches = match_points(row, transcript, ocr, evidence, visual)
        primary = matches[0] if matches else None
        score = int(primary["score"]) if primary else 0
        included = bool(primary and score >= INCLUDE_MIN and evidence in {"V2", "V3"})
        pending = bool(primary and REVIEW_MIN <= score < INCLUDE_MIN)
        secondary = [item for item in matches[1:] if item["score"] >= REVIEW_MIN][:2]
        combined_for_no_match = canonical(f"{transcript.get('text','')}\n{ocr.get('combined_text','')}\n{row.get('desc','')}")
        no_match_id, no_match_text = ("", "") if included or pending else no_match_reason(combined_for_no_match)
        primary_id = primary["id"] if primary else ""
        meta = label_map.get(primary_id, {})
        result = {
            "account_sample_index": row.get("account_sample_index"),
            "quality_label": row.get("quality_label", ""),
            "account_name": row.get("account_name", ""),
            "uid": str(row.get("uid") or ""),
            "aweme_id": aweme_id,
            "share_url": row.get("share_url", ""),
            "create_time_cn": row.get("create_time_cn", ""),
            "desc": row.get("desc", ""),
            "asr_text": transcript.get("text", ""),
            "ocr_text": ocr.get("combined_text", ""),
            "visual_review_summary": visual.get("summary", ""),
            "visual_review_confidence": visual.get("confidence", ""),
            "media_downloaded": media_exists,
            "asr_status": transcript.get("status", "missing"),
            "ocr_status": ocr.get("status", "missing"),
            "evidence_level": evidence,
            "evidence_note": evidence_note,
            "primary_id": primary_id,
            "primary_label": meta.get("label", ""),
            "primary_business_line": meta.get("business_line", ""),
            "primary_tier": meta.get("tier", ""),
            "score": score,
            "qualitative": qualitative(score, included, pending),
            "included": included,
            "pending": pending,
            "score_dimensions": primary.get("dimensions", {}) if primary else {},
            "evidence_source": primary.get("source", "") if primary else "",
            "evidence_snippet": primary.get("evidence_snippet", "") if primary else "",
            "decision_reason": primary.get("reason", "") if primary else no_match_text,
            "secondary_ids": [item["id"] for item in secondary],
            "secondary_labels": [label_map[item["id"]]["label"] for item in secondary],
            "no_match_id": no_match_id,
            "no_match_reason": no_match_text,
            "platform_linkage": "视频/正文明确提及懂车帝或AI小懂" if contains(canonical(f"{transcript.get('text','')}\n{ocr.get('combined_text','')}\n{strip_hashtags(str(row.get('desc') or ''))}"), ("懂车帝", "DCar", "AI小懂")) else "懂车帝自有账号内容服务承接（视频未明确口播平台名）",
        }
        results.append(result)

    # Deduplicate on actual creative evidence.  ASR is preferred because the
    # same video is often distributed with slightly different captions.
    creative_keys: list[str] = []
    for result in results:
        asr_key = normalized_creative(str(result.get("asr_text") or ""))
        ocr_key = normalized_creative(str(result.get("ocr_text") or ""))
        desc_key = normalized_creative(str(result.get("desc") or ""))
        if chinese_count(asr_key) >= 30:
            basis = f"asr:{asr_key}"
        elif chinese_count(ocr_key) >= 20:
            basis = f"ocr:{ocr_key}|desc:{desc_key}"
        else:
            basis = f"desc:{desc_key}|aweme:{result['aweme_id']}" if not desc_key else f"desc:{desc_key}"
        creative_keys.append(basis)
    # Conservative fuzzy clustering catches the same distributed video when
    # ASR differs by a few characters, while keeping model/price variants apart.
    representatives: list[str] = []
    assignments: list[int] = []
    for basis in creative_keys:
        assigned = -1
        for index, representative in enumerate(representatives):
            length_ratio = min(len(basis), len(representative)) / max(1, max(len(basis), len(representative)))
            if length_ratio < 0.82:
                continue
            matcher = difflib.SequenceMatcher(None, basis, representative)
            if matcher.quick_ratio() >= 0.93 and matcher.ratio() >= 0.93:
                assigned = index
                break
        if assigned < 0:
            assigned = len(representatives)
            representatives.append(basis)
        assignments.append(assigned)
    cluster_counts = Counter(assignments)
    for result, cluster_index in zip(results, assignments):
        representative = representatives[cluster_index]
        result["normalized_creative_hash"] = hashlib.sha1(representative.encode()).hexdigest()[:12]
        result["normalized_duplicate_count"] = cluster_counts[cluster_index]

    seen: set[str] = set()
    dedup = []
    for result in results:
        key = result["normalized_creative_hash"]
        if key not in seen:
            seen.add(key)
            dedup.append(result)

    by_quality = {quality: metric_block([row for row in results if row["quality_label"] == quality]) for quality in ("精品IP号", "原创号", "混剪号")}
    grouped: dict[tuple[int, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for result in results:
        key = (int(result["account_sample_index"]), result["quality_label"], result["account_name"], result["uid"])
        grouped[key].append(result)
    by_account = []
    for (index, quality, name, uid), rows in sorted(grouped.items()):
        item = {"account_sample_index": index, "quality_label": quality, "account_name": name, "uid": uid}
        item.update(metric_block(rows))
        by_account.append(item)

    summary = {
        "generated_at": "2026-08-01",
        "taxonomy_version": taxonomy["taxonomy_version"],
        "method": "完整视频ASR + 关键帧OCR + 正文；C1-C4已并入统一正式标签",
        "publication_metrics": metric_block(results),
        "deduplicated_creative_metrics": metric_block(dedup),
        "by_quality_label": by_quality,
        "by_account": by_account,
        "duplicate_publications": len(results) - len(dedup),
    }

    with OUT_JSONL.open("w", encoding="utf-8") as handle:
        for result in results:
            handle.write(json.dumps(result, ensure_ascii=False) + "\n")

    cn_fields = {
        "account_sample_index": "账号序号", "quality_label": "账号类型", "account_name": "账号", "uid": "UID",
        "aweme_id": "作品ID", "share_url": "作品链接", "create_time_cn": "发布时间", "desc": "作品文案",
        "asr_text": "视频口播转写", "ocr_text": "视频画面OCR", "visual_review_summary": "关键帧画面语义",
        "visual_review_confidence": "画面复核置信度", "evidence_level": "证据完整度",
        "evidence_note": "证据说明", "primary_id": "主卖点ID", "primary_label": "主卖点",
        "primary_business_line": "业务线", "primary_tier": "层级", "score": "分值", "qualitative": "定性",
        "included": "是否计入", "pending": "是否待复核", "platform_linkage": "平台承接证据",
        "evidence_source": "主要证据来源",
        "evidence_snippet": "证据摘要", "decision_reason": "判断说明", "secondary_ids": "次卖点ID",
        "secondary_labels": "次卖点", "no_match_reason": "未命中原因", "normalized_creative_hash": "去重创意ID",
        "normalized_duplicate_count": "相同文案发布次数",
    }
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(cn_fields.values()))
        writer.writeheader()
        for result in results:
            writer.writerow({cn_fields[key]: json.dumps(result[key], ensure_ascii=False) if isinstance(result[key], list) else result[key] for key in cn_fields})

    OUT_SUMMARY.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary["publication_metrics"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
