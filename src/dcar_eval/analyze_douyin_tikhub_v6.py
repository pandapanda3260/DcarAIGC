#!/usr/bin/env python3
"""Merge TikHub exposure/comments into the final three-scene channel report."""

from __future__ import annotations

import csv
import json
import re
from collections import Counter
from statistics import mean
from typing import Any

import rebuild_channel_evaluation_v4 as v4
from collect_tikhub_douyin_enrichment_v6 import valid_unique_comments
from three_proposition_scoring import audience_conclusion, acquisition_conclusion, dcd_acquisition_score
from project_paths import CURRENT_REPORTS_DIR, DOUYIN_PROCESSED_DIR, TIKHUB_CACHE_DIR


RUN_DATE = "2026-08-02"
CACHE = TIKHUB_CACHE_DIR
OUT_ROWS = CURRENT_REPORTS_DIR / f"抖音438条内容渠道评估_v6_TikHub补充_{RUN_DATE}.csv"
OUT_USERS = DOUYIN_PROCESSED_DIR / f"抖音评论匿名用户评分_v6_TikHub_{RUN_DATE}.jsonl"
OUT_JSON = CURRENT_REPORTS_DIR / f"双渠道结构化结论_v6_TikHub补充_{RUN_DATE}.json"
OUT_REPORT = CURRENT_REPORTS_DIR / f"双渠道结构化结论报告_v6_TikHub补充_{RUN_DATE}.md"


OWNER_OR_TRANSACTION = re.compile(
    r"(?:"
    r"(?:我|家里|我家|老爸|我爸|老妈|我妈|老公|老婆|朋友|同事).{0,12}(?:车|开了|买了|提了|公里|油耗|续航|保养|维修|轮胎|电池)|"
    r"(?:开了|用了|买了|提了|跑了).{0,8}(?:\d+|十|几).{0,4}(?:年|公里|万公里)|"
    r"\d+年.{0,12}(?:万公里|没修|保养|老伙计)|\d+万公里|十几万公里|几十万公里|"
    r"提车|买.{0,2}车|购车|换车|卖车|二手车|落地(?:价)?|报价|优惠|现车|四儿子|4s|4S|车险|上牌|过户|年检|车贷|"
    r"哪里.{0,5}(?:提|买)|在哪.{0,5}(?:提|买)|帮我.{0,5}(?:提|买)|找你.{0,5}(?:买|提)|"
    r"准备买|打算买|想买|要买|能买吗|值得买吗|怎么选|选哪|求购|订车|定车|直接订|订一辆|订一台|提一辆|提一台|试驾|"
    r"修车|保养|补胎|换胎|胎压|充电桩|加油|百公里|出险|事故车|验车"
    r")",
    re.I,
)
TECHNICAL_AUTO = re.compile(
    r"(?:车型|配置|版本|排量|发动机|变速箱|底盘|悬架|动力|马力|扭矩|油耗|续航|充电|电池|"
    r"空间|后排|后备箱|气囊|L2|四驱|两驱|前驱|后驱|轮胎|胎压|刹车|车机|座舱|中控|减震|"
    r"升档|降档|离合|转速|机油|变速箱油|空调|燃油泵|灭火器|充气泵|软管|方向盘|驾驶感受|"
    r"内饰|减速带|水基|二氧化碳|三大件|公里|"
    r"丰田|本田|大众|宝马|奔驰|奥迪|比亚迪|吉利|奇瑞|长安|五菱|特斯拉|理想|蔚来|小鹏|"
    r"问界|小米|红旗|广汽|传祺|凯迪拉克|雷克萨斯|路虎|保时捷|卡罗拉|轩逸|秦L|锋兰达|"
    r"雅阁|宝马3|A6L|A4L|CR-?V|H6|C200L?|E300|X5|Q7|CT5|途观|汉兰达|凯美瑞|迈腾|"
    r"科鲁兹|君威|锐界|哈弗|帕杰罗|天逸|雪铁龙|铃木|海豹|瑞虎|飞度|奥拓|速腾|森林人|"
    r"桑塔纳|凌志|极氪|零跑|鸿蒙智行|问界|SU7|YU7|SUV|MPV|轿车|"
    r"混动|增程|纯电|燃油|日系|德系|国产车)"
)
GENERIC_AUTO = re.compile(
    r"(?:汽车|这车|好车|车子|车辆|豪车|电车|油车|方向盘|驾驶|司机|开车|新车|老车|二手|"
    r"懂车帝|车主|车(?!祸|站|间|位))"
)
CONTEXT_AUTO_SIGNAL = re.compile(
    r"(?:买|卖|提|开|用|换|公里|年了|价格|贵|便宜|多少|哪款|什么版|牌子|内饰|真皮|塑料|"
    r"漂亮|好看|帅|空间|质量|稳定|省油|国产|合资|进口|三大件|销量|速度|刹车|方向|悬挂|"
    r"轮胎|充气|灭火|水基|手动|自动|挡|新能源|二手|一辆|一台|落地|四儿子|4儿子)"
)
PERSONAL_AUTO_ACTION = re.compile(
    r"(?:我|我家|家里|我爸|我妈|老爸|老妈|老公|老婆|朋友|同事).{0,25}(?:买|提|开|换|卖|用|修|保养)"
)
DCD_ACTION = re.compile(
    r"(?:懂车帝.{0,10}(?:查|看|搜|下载|打开|使用|用|预约|入口)|"
    r"(?:下载|打开|去|用).{0,8}懂车帝|在懂车帝.{0,8}(?:查|看|搜|预约))",
    re.I,
)
HIGH_ACTION = re.compile(
    r"(?:哪里.{0,6}(?:提|买)|在哪.{0,6}(?:提|买)|帮我.{0,6}(?:提|买)|找你.{0,6}(?:提|买)|"
    r"准备买|打算买|想买|我要买|要买|必须.{0,3}买|真心.{0,3}买|求购|订车|定车|直接订|订一辆|订一台|来一辆|来一台|报价|落地多少|多少钱|优惠多少|有现车|"
    r"怎么预约|帮我估价|卖车|置换|能买吗|值得买吗)"
)
INFO_ACTION = re.compile(
    r"(?:哪个配置|什么配置|说配置|配置呢|怎么选|选哪个|对比|油耗多少|续航多少|怎么样|如何|"
    r"为什么|哪款|几款|多少公里|多少万|多长|长度|尺寸|多大|行不行|靠谱吗|好不好|有没有|在哪里|在哪儿|哪种)"
)


def pct(n: int | float, d: int | float) -> float:
    return round(float(n) * 100 / float(d), 2) if d else 0.0


def audience_user_score(text: str, *, context_automotive: bool = False) -> int:
    has_auto_reference = bool(TECHNICAL_AUTO.search(text) or GENERIC_AUTO.search(text))
    if OWNER_OR_TRANSACTION.search(text) or (PERSONAL_AUTO_ACTION.search(text) and has_auto_reference):
        return 100
    if context_automotive and HIGH_ACTION.search(text):
        return 100
    if TECHNICAL_AUTO.search(text):
        return 70
    if context_automotive and INFO_ACTION.search(text):
        return 70
    if GENERIC_AUTO.search(text):
        return 30
    if context_automotive and CONTEXT_AUTO_SIGNAL.search(text):
        return 30
    return 0


def action_user_score(text: str, *, context_automotive: bool = False) -> int:
    if DCD_ACTION.search(text):
        return 100
    if HIGH_ACTION.search(text):
        return 80
    if INFO_ACTION.search(text) and (
        context_automotive or TECHNICAL_AUTO.search(text) or GENERIC_AUTO.search(text)
    ):
        return 50
    return 0


def load_stats() -> dict[str, dict[str, int]]:
    output: dict[str, dict[str, int]] = {}
    for path in sorted((CACHE / "statistics").glob("batch_*.json")):
        record = json.loads(path.read_text(encoding="utf-8"))
        for item in record.get("statistics") or []:
            output[str(item["aweme_id"])] = {
                "play_count": int(item.get("play_count") or 0),
                "digg_count": int(item.get("digg_count") or 0),
                "share_count": int(item.get("share_count") or 0),
            }
    return output


def load_comment_users(aweme_id: str) -> dict[str, str]:
    pages = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted((CACHE / "comments" / aweme_id).glob("page_*.json"))
    ]
    return valid_unique_comments(pages)


def first_comment_page(aweme_id: str) -> dict[str, Any]:
    path = CACHE / "comments" / aweme_id / "page_001.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def recalibrate_content(score: int | None, audience_score: int | None) -> tuple[int | None, float]:
    if score is None or audience_score is None:
        return score, 0.0
    adjustment = max(-5.0, min(5.0, 0.10 * (audience_score - score)))
    return round(max(0, min(100, score + adjustment))), round(adjustment, 2)


def build_rows() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    taxonomy = v4.build_taxonomy()
    label_map = {item["id"]: item for item in taxonomy["labels"]}
    rows = v4.rebuild_douyin(label_map)
    stats = load_stats()
    user_rows: list[dict[str, Any]] = []
    for row in rows:
        aweme_id = str(row["aweme_id"])
        item_stats = stats.get(aweme_id) or {}
        row["play_count_tikhub"] = int(item_stats.get("play_count") or 0)
        row["play_count_valid"] = row["play_count_tikhub"] > 0
        row["digg_count_tikhub"] = int(item_stats.get("digg_count") or 0)
        row["share_count_tikhub"] = int(item_stats.get("share_count") or 0)
        users = load_comment_users(aweme_id)
        first = first_comment_page(aweme_id)
        row["comment_reported_total_tikhub"] = int(first.get("reported_total") or 0)
        row["valid_unique_commenters"] = len(users)
        row["comment_pages_fetched"] = len(list((CACHE / "comments" / aweme_id).glob("page_*.json")))
        scoreable = len(users) >= 20
        row["audience_sample_status"] = "scorable" if scoreable else "below_20_valid_users"
        context_automotive = int(row.get("content_auto_score") or 0) >= 70
        score_counts = Counter(
            audience_user_score(text, context_automotive=context_automotive)
            for text in users.values()
        )
        action_counts = Counter(
            action_user_score(text, context_automotive=context_automotive)
            for text in users.values()
        )
        if scoreable:
            row["audience_auto_score"] = round(
                sum(score * count for score, count in score_counts.items()) / len(users)
            )
            row["audience_auto_qualitative"] = audience_conclusion(row["audience_auto_score"])
            row["action_intent_score"] = round(
                sum(score * count for score, count in action_counts.items()) / len(users)
            )
            dcd_fit = int(row.get("score") or 0) if row.get("included") else 0
            row["dcd_task_fit_score"] = dcd_fit
            row["acquisition_effect_estimate"] = dcd_acquisition_score(
                content_score=int(row.get("content_auto_score") or 0),
                audience_score=row["audience_auto_score"],
                dcd_fit_score=dcd_fit,
                action_intent_score=row["action_intent_score"],
            )
            row["acquisition_effect_qualitative"] = acquisition_conclusion(
                row["acquisition_effect_estimate"]
            )
        else:
            row["audience_auto_score"] = None
            row["audience_auto_qualitative"] = "有效独立评论用户少于20人"
            row["action_intent_score"] = None
            row["dcd_task_fit_score"] = None
            row["acquisition_effect_estimate"] = None
            row["acquisition_effect_qualitative"] = "暂不可计算"
        row["content_auto_score_v6"], row["content_comment_adjustment"] = recalibrate_content(
            row.get("content_auto_score"), row.get("audience_auto_score")
        )
        for key, text in users.items():
            user_rows.append({
                "aweme_id": aweme_id,
                "user_key": key,
                "text": text,
                "audience_auto_score": audience_user_score(
                    text, context_automotive=context_automotive
                ),
                "action_intent_score": action_user_score(
                    text, context_automotive=context_automotive
                ),
            })
    return rows, user_rows


def exposure_metric(rows: list[dict[str, Any]], predicate) -> dict[str, Any]:
    valid = [row for row in rows if row.get("play_count_valid")]
    total_exposure = sum(int(row["play_count_tikhub"]) for row in valid)
    value = sum(int(row["play_count_tikhub"]) for row in valid if predicate(row))
    return {"exposure": value, "share": pct(value, total_exposure)}


def avg_score(rows: list[dict[str, Any]], field: str) -> int | None:
    values = [int(row[field]) for row in rows if row.get(field) is not None]
    return round(mean(values)) if values else None


def score_text(score: int | None, kind: str) -> str:
    if score is None:
        return "暂不可计算"
    if kind == "audience":
        return audience_conclusion(score) or ""
    if kind == "acquisition":
        return acquisition_conclusion(score) or ""
    return v4.auto_qualitative(score)


def channel_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    identifiable = [row for row in rows if row.get("evidence_level") in {"V2", "V3"}]
    covered = [row for row in rows if row.get("included")]
    core = [row for row in covered if row.get("primary_tier") == "core"]
    other = [row for row in covered if row.get("primary_tier") == "other"]
    valid_play = [row for row in rows if row.get("play_count_valid")]
    total_play = sum(int(row["play_count_tikhub"]) for row in valid_play)
    scorable = [row for row in rows if row.get("audience_auto_score") is not None]
    content_scores = [row["content_auto_score_v6"] for row in rows if row.get("content_auto_score_v6") is not None]
    content_avg = round(mean(content_scores))
    return {
        "scope": "仅抖音内容链接；30个随机账号样本中的438条发布",
        "total_publications": total,
        "count_dimension": {
            "identifiable": len(identifiable), "identifiable_share": pct(len(identifiable), total),
            "selling_point_covered": len(covered), "selling_point_covered_share": pct(len(covered), total),
            "core": len(core), "core_share": pct(len(core), total),
            "other": len(other), "other_share": pct(len(other), total),
        },
        "exposure_dimension": {
            "valid_publications": len(valid_play),
            "valid_publication_share": pct(len(valid_play), total),
            "total_valid_exposure": total_play,
            "identifiable": exposure_metric(rows, lambda row: row.get("evidence_level") in {"V2", "V3"}),
            "selling_point_covered": exposure_metric(rows, lambda row: row.get("included")),
            "core": exposure_metric(rows, lambda row: row.get("included") and row.get("primary_tier") == "core"),
            "other": exposure_metric(rows, lambda row: row.get("included") and row.get("primary_tier") == "other"),
        },
        "verticality": {
            "content_automotive": {
                "score": content_avg,
                "qualitative": score_text(content_avg, "content"),
                "automotive_publications": sum(int(score) >= 70 for score in content_scores),
                "evaluable": len(content_scores),
            },
            "audience_automotive": {
                "score": avg_score(scorable, "audience_auto_score"),
                "qualitative": score_text(avg_score(scorable, "audience_auto_score"), "audience"),
                "scorable_publications": len(scorable),
                "total_valid_unique_commenters": sum(int(row["valid_unique_commenters"]) for row in scorable),
                "scope_note": "仅代表达到20名有效独立评论用户门槛的内容；不是438条全量受众结论",
            },
            "acquisition_effect_estimate": {
                "score": avg_score(scorable, "acquisition_effect_estimate"),
                "qualitative": score_text(avg_score(scorable, "acquisition_effect_estimate"), "acquisition"),
                "scorable_publications": len(scorable),
                "scope_note": "内容侧实验前预估，不是懂车帝实际新增效果",
            },
            "actual_acquisition_effect": {
                "status": "not_computable",
                "score": None,
                "reason": "TikHub不返回懂车帝落地页点击、安装、登录和确认新增归因数据",
            },
        },
    }


def scene_summary(rows: list[dict[str, Any]], total_valid_exposure: int) -> dict[str, Any]:
    result: dict[str, Any] = {}
    total = len(rows)
    for scene in v4.SCENES:
        scene_rows = [row for row in rows if row.get("included") and row.get("business_scene") == scene]
        core_rows = [row for row in scene_rows if row.get("primary_tier") == "core"]
        valid_play = [row for row in scene_rows if row.get("play_count_valid")]
        exposure = sum(int(row["play_count_tikhub"]) for row in valid_play)
        core_exposure = sum(
            int(row["play_count_tikhub"]) for row in core_rows if row.get("play_count_valid")
        )
        comment_rows = [row for row in scene_rows if row.get("audience_auto_score") is not None]
        content_score = avg_score(scene_rows, "content_auto_score_v6")
        audience_score = avg_score(comment_rows, "audience_auto_score")
        acquisition_score = avg_score(comment_rows, "acquisition_effect_estimate")
        result[scene] = {
            "publication_n": len(scene_rows),
            "count_share_all": pct(len(scene_rows), total),
            "core_n": len(core_rows),
            "core_count_share_all": pct(len(core_rows), total),
            "valid_play_n": len(valid_play),
            "exposure": exposure,
            "exposure_share_all": pct(exposure, total_valid_exposure),
            "core_exposure": core_exposure,
            "core_exposure_share_all": pct(core_exposure, total_valid_exposure),
            "content_automotive_score": content_score,
            "content_automotive_qualitative": score_text(content_score, "content"),
            "audience_automotive_score": audience_score,
            "audience_automotive_qualitative": score_text(audience_score, "audience"),
            "comment_scorable_n": len(comment_rows),
            "acquisition_effect_estimate": acquisition_score,
            "acquisition_effect_qualitative": score_text(acquisition_score, "acquisition"),
        }
    return result


def xhs_unchanged() -> dict[str, Any]:
    taxonomy = v4.build_taxonomy()
    label_map = {item["id"]: item for item in taxonomy["labels"]}
    _, d = v4.xhs_sample_rows(label_map)
    return {
        "scope": "仅小红书内容链接；338条唯一链接",
        "status": "unchanged_from_v4",
        "count_dimension": d["all_publication_selling_point_metrics"],
        "exposure_dimension": d["all_publication_exposure_metrics"],
        "content_automotive": d["content_automotive"],
        "audience_automotive": d["audience_automotive"],
        "actual_acquisition_effect": d["actual_acquisition_effect"],
    }


def format_number(value: int) -> str:
    return f"{value:,}"


def build_report(data: dict[str, Any]) -> str:
    d = data["channels"]["douyin"]
    c = d["summary"]["count_dimension"]
    e = d["summary"]["exposure_dimension"]
    v = d["summary"]["verticality"]
    lines = [
        "# 双渠道结构化结论报告 v6.0（TikHub补充）",
        "",
        f"生成日期：{RUN_DATE}  ",
        "固定结构：渠道 → 卖点条数 → 卖点曝光 → 内容垂直度 → 三个业务场景 → 内容明细文件。  ",
        "抖音播放量和评论来自TikHub同轮采集；卖点与内容判断继续使用完整视频/图文证据，不使用标题单独口径。",
        "",
        "## 【抖音渠道】",
        "",
        f"范围：{d['summary']['scope']}。",
        "",
        "### 1、卖点分布——条数维度",
        "",
        "| 指标 | 结果 | 定性 |",
        "|---|---:|---|",
        f"| 可识别内容 | {c['identifiable']}/438 · {c['identifiable_share']}% | 基本全部具备完整媒体证据 |",
        f"| 卖点覆盖 | {c['selling_point_covered']}/438 · {c['selling_point_covered_share']}% | 多数发布表达正式卖点 |",
        f"| 核心卖点覆盖 | {c['core']}/438 · {c['core_share']}% | 低于60%-70%生产目标 |",
        f"| 其他卖点覆盖 | {c['other']}/438 · {c['other_share']}% | 其他卖点占比偏高 |",
        "",
        "### 2、卖点分布——曝光维度",
        "",
        f"有效播放量覆盖{e['valid_publications']}/438条（{e['valid_publication_share']}%），合计{format_number(e['total_valid_exposure'])}次，达到90%可计算门槛。",
        "",
        "| 指标 | 曝光量 | 占有效总曝光 | 定性 |",
        "|---|---:|---:|---|",
        f"| 可识别内容 | {format_number(e['identifiable']['exposure'])} | {e['identifiable']['share']}% | 几乎全部曝光可识别 |",
        f"| 卖点覆盖 | {format_number(e['selling_point_covered']['exposure'])} | {e['selling_point_covered']['share']}% | 卖点内容条数多，但曝光贡献不足一半 |",
        f"| 核心卖点覆盖 | {format_number(e['core']['exposure'])} | {e['core']['share']}% | 核心卖点曝光明显不足 |",
        f"| 其他卖点覆盖 | {format_number(e['other']['exposure'])} | {e['other']['share']}% | 卖点曝光主要来自其他卖点 |",
        "",
        "### 3、内容垂直度",
        "",
        "| 命题 | 分数/结果 | 定性 | 数据边界 |",
        "|---|---:|---|---|",
        f"| 内容汽车性 | {v['content_automotive']['score']}/100 | {v['content_automotive']['qualitative']} | {v['content_automotive']['automotive_publications']}/{v['content_automotive']['evaluable']}条达到70分 |",
        f"| 互动受众汽车性 | {v['audience_automotive']['score']}/100 | {v['audience_automotive']['qualitative']} | {v['audience_automotive']['scorable_publications']}/438条、{v['audience_automotive']['total_valid_unique_commenters']}名有效用户；高评论内容样本 |",
        f"| 懂车帝拉新效果预估 | {v['acquisition_effect_estimate']['score']}/100 | {v['acquisition_effect_estimate']['qualitative']} | 同一批{v['acquisition_effect_estimate']['scorable_publications']}条评论达标内容 |",
        "| 懂车帝实际拉新效果 | — | 仍不可计算 | TikHub没有懂车帝侧点击、安装、登录及确认新增归因 |",
        "",
        "### 4、三个业务场景",
        "",
        "| 场景 | 卖点条数/全发布 | 核心条数/全发布 | 卖点曝光/总曝光 | 核心曝光/总曝光 | 内容汽车性 | 互动受众汽车性 | 拉新效果预估 |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for scene in v4.SCENES:
        s = d["scenes"][scene]
        audience = (
            f"{s['audience_automotive_score']}/100 · {s['comment_scorable_n']}条"
            if s["audience_automotive_score"] is not None else "— · 0条达标"
        )
        acquisition = (
            f"{s['acquisition_effect_estimate']}/100 · {s['comment_scorable_n']}条"
            if s["acquisition_effect_estimate"] is not None else "— · 0条达标"
        )
        lines.append(
            f"| {scene} | {s['publication_n']}/438 · {s['count_share_all']}% | "
            f"{s['core_n']}/438 · {s['core_count_share_all']}% | "
            f"{format_number(s['exposure'])} · {s['exposure_share_all']}% | "
            f"{format_number(s['core_exposure'])} · {s['core_exposure_share_all']}% | "
            f"{s['content_automotive_score']}/100 | {audience} | {acquisition} |"
        )
    lines.extend([
        "",
        "> 互动受众和拉新预估只代表达到20名有效独立评论用户门槛的内容。二手车场景本轮没有达标内容，不能写成0分。",
        "",
        "### 5、抖音核心结论",
        "",
        f"1. 核心卖点占发布条数{c['core_share']}%，但只贡献{e['core']['share']}%有效曝光；问题不只在生产占比，也在核心内容的流量效率。",
        f"2. 其他卖点贡献{e['other']['share']}%曝光，远高于核心卖点；当前高流量内容并未有效沉淀为核心业务表达。",
        f"3. 评论达标内容的互动受众汽车性为{v['audience_automotive']['score']}/100，拉新效果预估为{v['acquisition_effect_estimate']['score']}/100；两项都受高评论内容选择偏差限制。",
        "4. TikHub可以补齐抖音公开播放量和评论，但不能证明跨App实际新增；实际拉新仍需懂车帝侧归因。",
        "",
        "## 【小红书渠道】",
        "",
        "本轮没有新增小红书采集，结论保持v4：338条链接中只有10条完成全媒体卖点标注，条数和曝光维度仍不可上卷；内容汽车性88/100为方向性估计，互动受众15/100仅代表评论达标的5+5样本，实际拉新未测试。",
        "",
        "## 配套文件",
        "",
        f"- `{OUT_ROWS.name}`：抖音438条逐条结果",
        f"- `{OUT_USERS.name}`：匿名评论用户评分证据",
        f"- `{OUT_JSON.name}`：结构化汇总",
        "- `tikhub_douyin_enrichment_2026-08-02/`：可复用TikHub采集缓存",
        "- `懂车帝内容评估判断标准与流程_v4_终版.md`：终版规则",
    ])
    return "\n".join(lines) + "\n"


def write_outputs(rows: list[dict[str, Any]], user_rows: list[dict[str, Any]]) -> dict[str, Any]:
    summary = channel_summary(rows)
    scenes = scene_summary(rows, summary["exposure_dimension"]["total_valid_exposure"])
    data = {
        "report_version": "channel-evaluation-v6.0-tikhub",
        "generated_at": RUN_DATE,
        "channels": {
            "douyin": {"summary": summary, "scenes": scenes},
            "xiaohongshu": xhs_unchanged(),
        },
        "important_boundary": {
            "tikhub_can_supply": ["抖音公开播放量", "抖音公开评论文本与评论用户"],
            "tikhub_cannot_supply": ["懂车帝落地页点击", "下载/激活", "登录", "确认新增用户", "跨App归因"],
        },
    }
    fields = [
        "account_sample_index", "quality_label", "account_name", "uid", "aweme_id", "share_url",
        "create_time_cn", "desc", "business_scene", "primary_id", "primary_label", "primary_tier",
        "score", "qualitative", "included", "evidence_level", "play_count_tikhub",
        "play_count_valid", "digg_count_tikhub", "share_count_tikhub", "comment_reported_total_tikhub",
        "comment_pages_fetched", "valid_unique_commenters", "audience_sample_status", "content_auto_score_v6",
        "content_comment_adjustment", "audience_auto_score", "audience_auto_qualitative",
        "dcd_task_fit_score", "action_intent_score", "acquisition_effect_estimate",
        "acquisition_effect_qualitative",
    ]
    with OUT_ROWS.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    with OUT_USERS.open("w", encoding="utf-8") as handle:
        for row in user_rows:
            handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    OUT_JSON.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    OUT_REPORT.write_text(build_report(data), encoding="utf-8")
    return data


def main() -> int:
    rows, user_rows = build_rows()
    data = write_outputs(rows, user_rows)
    print(json.dumps(data["channels"]["douyin"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
