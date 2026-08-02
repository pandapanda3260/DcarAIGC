#!/usr/bin/env python3
"""Generate the shareable v3 DCar selling-point conclusion image."""

from __future__ import annotations

import json
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parent
SUMMARY = ROOT / "douyin_selling_point_summary_v3_video_2026-08-01.json"
COMPARISON = ROOT / "douyin_caption_vs_video_comparison_v3_2026-08-01.json"
TAXONOMY = ROOT / "business_selling_points_v3_final.json"
OUT = ROOT / "抖音30账号业务卖点核心结论_v3_视频终版_2026-08-01.png"
FONT_REGULAR = "/System/Library/Fonts/Hiragino Sans GB.ttc"
FONT_MEDIUM = "/System/Library/Fonts/STHeiti Medium.ttc"

W, H = 1440, 2560
BG = "#F3F6FA"
CARD = "#FFFFFF"
NAVY = "#18324A"
BLUE = "#2F6BFF"
TEAL = "#17A589"
ORANGE = "#F59E0B"
RED = "#E45D68"
GRAY = "#AAB3C2"
LIGHT = "#E9EDF3"
MUTED = "#687588"
INK = "#17212B"


def font(size: int, medium: bool = False) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(FONT_MEDIUM if medium else FONT_REGULAR, size=size)


def rounded(draw: ImageDraw.ImageDraw, box: tuple[int, int, int, int], radius: int = 28, fill: str = CARD) -> None:
    draw.rounded_rectangle(box, radius=radius, fill=fill)


def width(draw: ImageDraw.ImageDraw, value: str, fnt: ImageFont.FreeTypeFont) -> int:
    return int(draw.textbbox((0, 0), value, font=fnt)[2])


def wrap(draw: ImageDraw.ImageDraw, value: str, fnt: ImageFont.FreeTypeFont, max_width: int) -> list[str]:
    lines, current = [], ""
    for char in value:
        trial = current + char
        if current and width(draw, trial, fnt) > max_width:
            lines.append(current)
            current = char
        else:
            current = trial
    if current:
        lines.append(current)
    return lines


def paragraph(draw: ImageDraw.ImageDraw, x: int, y: int, value: str, fnt: ImageFont.FreeTypeFont, max_width: int, fill: str = INK, gap: int = 9) -> int:
    for line in wrap(draw, value, fnt, max_width):
        draw.text((x, y), line, font=fnt, fill=fill)
        y += fnt.size + gap
    return y


def section(draw: ImageDraw.ImageDraw, y: int, number: str, title: str) -> None:
    draw.rounded_rectangle((82, y + 2, 126, y + 46), radius=12, fill=NAVY)
    draw.text((95, y + 7), number, font=font(24, True), fill="#FFFFFF")
    draw.text((144, y), title, font=font(34, True), fill=INK)


def metric(draw: ImageDraw.ImageDraw, x: int, y: int, w: int, value: str, detail: str, label: str, color: str) -> None:
    rounded(draw, (x, y, x + w, y + 160), 24, "#F8FAFC")
    draw.rounded_rectangle((x, y, x + 10, y + 160), radius=5, fill=color)
    draw.text((x + 28, y + 17), value, font=font(49, True), fill=INK)
    draw.text((x + 30, y + 82), detail, font=font(21), fill=MUTED)
    draw.text((x + 30, y + 118), label, font=font(23, True), fill=INK)


def stacked(draw: ImageDraw.ImageDraw, x: int, y: int, w: int, h: int, values: list[tuple[int, str]], total: int) -> None:
    cursor = x
    for index, (value, color) in enumerate(values):
        segment = w - (cursor - x) if index == len(values) - 1 else round(w * value / total) if total else 0
        draw.rectangle((cursor, y, cursor + segment, y + h), fill=color)
        cursor += segment


def main() -> None:
    summary = json.loads(SUMMARY.read_text(encoding="utf-8"))
    comparison = json.loads(COMPARISON.read_text(encoding="utf-8"))
    taxonomy = json.loads(TAXONOMY.read_text(encoding="utf-8"))
    labels = {item["id"]: item for item in taxonomy["labels"]}
    pub = summary["publication_metrics"]
    dedup = summary["deduplicated_creative_metrics"]
    by_quality = summary["by_quality_label"]

    image = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(image)

    draw.rectangle((0, 0, W, 300), fill=NAVY)
    draw.rounded_rectangle((72, 54, 292, 98), radius=22, fill="#2D4964")
    draw.text((96, 61), "v3.0 视频终判", font=font(22, True), fill="#FFFFFF")
    draw.text((72, 122), "懂车帝抖音内容卖点终版", font=font(57, True), fill="#FFFFFF")
    draw.text((74, 205), "30个账号｜438条作品｜414视频ASR + 24图文原图 + OCR/画面复核", font=font(24), fill="#D5E2EC")
    draw.text((1110, 205), "2026.08.01", font=font(25), fill="#D5E2EC")

    rounded(draw, (48, 328, 1392, 450), 28, CARD)
    draw.text((82, 354), "终版口径", font=font(25, True), fill=BLUE)
    draw.text((82, 397), "卖点 = 用户通过懂车帝能完成什么，而不是这条汽车内容讲了什么。", font=font(30, True), fill=INK)

    rounded(draw, (48, 478, 1392, 820), 30, CARD)
    section(draw, 510, "1", "只看标题不够：视频会改变最终判断")
    card_w = 292
    metric(draw, 82, 580, card_w, f"{comparison['exact_primary_label_agreement_pct']:.1f}%", f"{comparison['exact_primary_label_agreement']} / {comparison['comparison_scope']}条", "主标签完全一致", BLUE)
    metric(draw, 398, 580, card_w, f"{comparison['caption_missed_video_found_pct']:.1f}%", f"{comparison['caption_missed_video_found']}条", "标题漏判，视频发现", TEAL)
    metric(draw, 714, 580, card_w, f"{comparison['caption_overcalled_video_rejected_pct']:.1f}%", f"{comparison['caption_overcalled_video_rejected']}条", "标题命中，视频否定", RED)
    metric(draw, 1030, 580, card_w, f"{comparison['both_hit_but_label_changed_pct']:.1f}%", f"{comparison['both_hit_but_label_changed']}条", "都命中但标签改变", ORANGE)
    draw.text((82, 760), "标题/文案只保留为初筛；终版以口播、字幕、关键画面和平台承接证据为准。", font=font(22), fill=MUTED)

    rounded(draw, (48, 848, 1392, 1272), 30, CARD)
    section(draw, 880, "2", "终版总体结果：核心卖点看全部发布口径")
    metric(draw, 82, 950, card_w, f"{pub['final_evaluable_pct']:.1f}%", f"{pub['final_evaluable']} / {pub['total']}条", "视频证据可终判", TEAL)
    metric(draw, 398, 950, card_w, f"{pub['unified_coverage_pct']:.1f}%", f"{pub['included']} / {pub['final_evaluable']}条", "统一卖点覆盖", BLUE)
    metric(draw, 714, 950, card_w, f"{pub['core_share_all_publications_pct']:.1f}%", f"{pub['core']} / {pub['total']}条", "核心卖点 / 全部", NAVY)
    target_gap = max(0.0, 60.0 - pub["core_share_all_publications_pct"])
    metric(draw, 1030, 950, card_w, f"{target_gap:.1f}pp", "距离目标下限60%", "核心卖点缺口", RED)
    core = pub["core"]
    other = pub["included"] - core
    incomplete = pub["evidence_incomplete"]
    pending = pub["pending"]
    no_match = pub["no_match"]
    stacked(draw, 82, 1148, 1240, 42, [(core, NAVY), (other, BLUE), (pending, ORANGE), (no_match, GRAY), (incomplete, RED)], pub["total"])
    legends = [(f"核心 {core}", NAVY), (f"其他卖点 {other}", BLUE), (f"待复核 {pending}", ORANGE), (f"未命中 {no_match}", GRAY), (f"证据不足 {incomplete}", RED)]
    x = 82
    for label, color in legends:
        draw.ellipse((x, 1214, x + 17, 1231), fill=color)
        draw.text((x + 25, 1206), label, font=font(20), fill=MUTED)
        x += 238

    rounded(draw, (48, 1300, 1392, 1718), 30, CARD)
    section(draw, 1332, "3", "统一卖点分布：C1–C4已正式并入“其他卖点”")
    primary_counts = sorted(pub["primary_counts"].items(), key=lambda item: item[1], reverse=True)[:6]
    max_count = max((count for _, count in primary_counts), default=1)
    y = 1405
    for point_id, count in primary_counts:
        meta = labels[point_id]
        tier = "核心" if meta["tier"] == "core" else "其他"
        name = meta["label"].replace("通过懂车帝", "").replace("通过AI小懂", "AI小懂")
        name = name if len(name) <= 25 else name[:24] + "…"
        draw.text((82, y), f"{point_id}  {name}", font=font(22, True), fill=INK)
        draw.rounded_rectangle((670, y + 4, 1205, y + 31), radius=14, fill=LIGHT)
        bar = max(8, round(535 * count / max_count))
        color = NAVY if tier == "核心" else TEAL
        draw.rounded_rectangle((670, y + 4, 670 + bar, y + 31), radius=14, fill=color)
        draw.text((1230, y - 2), f"{count}条", font=font(22, True), fill=INK)
        y += 44
    draw.text((82, 1680), "核心集合仍是 E1、E2、X1、X2、X3、M1、M2、M3；次卖点不参与核心比例。", font=font(21), fill=MUTED)

    rounded(draw, (48, 1746, 1392, 2098), 30, CARD)
    section(draw, 1778, "4", "三类账号对比")
    quality_accounts = {"精品IP号": 8, "原创号": 11, "混剪号": 11}
    y = 1855
    for quality in ("精品IP号", "原创号", "混剪号"):
        item = by_quality[quality]
        draw.text((82, y), f"{quality} · {quality_accounts[quality]}个账号", font=font(24, True), fill=INK)
        draw.rounded_rectangle((370, y + 4, 1100, y + 36), radius=16, fill=LIGHT)
        core_w = round(730 * item["core_share_all_publications_pct"] / 100)
        draw.rounded_rectangle((370, y + 4, 370 + max(6, core_w), y + 36), radius=16, fill=NAVY)
        draw.text((1130, y - 1), f"核心 {item['core_share_all_publications_pct']:.1f}%", font=font(22, True), fill=INK)
        draw.text((370, y + 45), f"统一卖点覆盖 {item['unified_coverage_pct']:.1f}%｜证据不足 {item['evidence_incomplete']}条", font=font(20), fill=MUTED)
        y += 86

    rounded(draw, (48, 2126, 1392, 2472), 30, NAVY)
    draw.text((82, 2160), "业务结论", font=font(36, True), fill="#FFFFFF")
    conclusions = [
        f"当前核心卖点占全部发布 {pub['core_share_all_publications_pct']:.1f}%，不能用“已命中内容内部核心占比”替代60%–70%目标。",
        f"去重创意口径核心占比 {dedup['core_share_all_publications_pct']:.1f}%；发布重复与创意供给问题需分开治理。",
        "每条脚本先选唯一主卖点，再明确用户任务、懂车帝承接、用户收益和视频内证据。",
        "没有完整视频证据的内容不强判；标题只做候选筛选，不能作为终版统计。",
    ]
    y = 2220
    for index, value in enumerate(conclusions, 1):
        draw.ellipse((84, y + 8, 108, y + 32), fill=TEAL if index < 4 else ORANGE)
        draw.text((90, y + 6), str(index), font=font(16, True), fill="#FFFFFF")
        paragraph(draw, 126, y, value, font(23), 1190, fill="#EDF4F8", gap=7)
        y += 58

    draw.text((72, 2506), "证据归档：438条原始作品媒体、ASR、OCR、关键帧与逐条判定均已本地保存，可复核、可重跑。", font=font(20), fill=MUTED)
    draw.text((1192, 2506), "DCar", font=font(22, True), fill=NAVY)
    image.save(OUT, quality=95)
    print(OUT)


if __name__ == "__main__":
    main()
