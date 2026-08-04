#!/usr/bin/env python3
"""Generate a shareable PNG for the v4 dual-channel conclusion."""

from __future__ import annotations

import json
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parent
SUMMARY = ROOT / "channel_evaluation_summary_v4_2026-08-02.json"
OUTPUT = ROOT / "双渠道核心结论_v4_全发布口径_2026-08-02.png"
FONT_CANDIDATES = (
    Path("/System/Library/Fonts/PingFang.ttc"),
    Path("/System/Library/Fonts/STHeiti Light.ttc"),
    Path("/Library/Fonts/Arial Unicode.ttf"),
)

W, H = 1800, 1900
BG = "#F5F7FB"
INK = "#152033"
MUTED = "#657087"
LINE = "#DDE3ED"
WHITE = "#FFFFFF"
BLUE = "#2F6BFF"
BLUE_LIGHT = "#E9F0FF"
GREEN = "#18A675"
GREEN_LIGHT = "#E5F7F1"
ORANGE = "#F59E0B"
ORANGE_LIGHT = "#FFF4D8"
RED = "#DB4B4B"
RED_LIGHT = "#FDECEC"
PURPLE = "#7C5CFC"


def font(size: int, *, bold: bool = False) -> ImageFont.FreeTypeFont:
    path = next((candidate for candidate in FONT_CANDIDATES if candidate.exists()), None)
    if path is None:
        raise FileNotFoundError("No Chinese font found")
    index = 1 if bold else 0
    try:
        return ImageFont.truetype(str(path), size=size, index=index)
    except OSError:
        return ImageFont.truetype(str(path), size=size)


F_TITLE = font(52, bold=True)
F_SUB = font(23)
F_SECTION = font(32, bold=True)
F_KPI = font(46, bold=True)
F_LABEL = font(24, bold=True)
F_BODY = font(22)
F_SMALL = font(19)
F_BAR = font(20, bold=True)


def rounded(draw: ImageDraw.ImageDraw, box: tuple[int, int, int, int], fill: str, radius: int = 24, outline: str | None = None) -> None:
    draw.rounded_rectangle(box, radius=radius, fill=fill, outline=outline, width=2 if outline else 1)


def text(draw: ImageDraw.ImageDraw, xy: tuple[int, int], value: str, fnt: ImageFont.FreeTypeFont, fill: str = INK, anchor: str | None = None) -> None:
    draw.text(xy, value, font=fnt, fill=fill, anchor=anchor)


def wrap(draw: ImageDraw.ImageDraw, value: str, fnt: ImageFont.FreeTypeFont, max_width: int) -> list[str]:
    lines: list[str] = []
    current = ""
    for char in value:
        trial = current + char
        if current and draw.textlength(trial, font=fnt) > max_width:
            lines.append(current)
            current = char
        else:
            current = trial
    if current:
        lines.append(current)
    return lines


def paragraph(draw: ImageDraw.ImageDraw, xy: tuple[int, int], value: str, fnt: ImageFont.FreeTypeFont, max_width: int, fill: str = MUTED, gap: int = 10) -> int:
    x, y = xy
    bbox = draw.textbbox((0, 0), "国", font=fnt)
    line_height = bbox[3] - bbox[1]
    for line in wrap(draw, value, fnt, max_width):
        text(draw, (x, y), line, fnt, fill)
        y += line_height + gap
    return y


def pill(draw: ImageDraw.ImageDraw, x: int, y: int, label: str, fill: str, color: str) -> int:
    width = int(draw.textlength(label, font=F_SMALL)) + 34
    rounded(draw, (x, y, x + width, y + 42), fill, 21)
    text(draw, (x + 17, y + 9), label, F_SMALL, color)
    return x + width + 12


def kpi(draw: ImageDraw.ImageDraw, x: int, y: int, w: int, value: str, label: str, note: str, accent: str, light: str) -> None:
    rounded(draw, (x, y, x + w, y + 150), WHITE, 22, LINE)
    draw.rounded_rectangle((x, y, x + 8, y + 150), radius=4, fill=accent)
    text(draw, (x + 28, y + 20), value, F_KPI, accent)
    text(draw, (x + 28, y + 79), label, F_LABEL, INK)
    text(draw, (x + 28, y + 117), note, F_SMALL, MUTED)


def metric_bar(draw: ImageDraw.ImageDraw, x: int, y: int, width: int, label: str, count: int, total: int, color: str) -> None:
    ratio = count / total if total else 0
    text(draw, (x, y), label, F_BODY, INK)
    text(draw, (x + width, y), f"{count}条  {ratio * 100:.1f}%", F_BAR, INK, anchor="ra")
    top = y + 39
    rounded(draw, (x, top, x + width, top + 22), "#E9EDF4", 11)
    if ratio > 0:
        rounded(draw, (x, top, x + max(22, int(width * ratio)), top + 22), color, 11)


def status_row(draw: ImageDraw.ImageDraw, x: int, y: int, width: int, name: str, result: str, note: str, color: str, light: str) -> None:
    rounded(draw, (x, y, x + width, y + 86), WHITE, 18, LINE)
    rounded(draw, (x + 16, y + 15, x + 226, y + 71), light, 15)
    text(draw, (x + 32, y + 29), name, F_LABEL, color)
    text(draw, (x + 250, y + 18), result, F_LABEL, INK)
    text(draw, (x + 250, y + 51), note, F_SMALL, MUTED)


def main() -> None:
    summary = json.loads(SUMMARY.read_text(encoding="utf-8"))
    dy = summary["channels"]["douyin"]
    dm = dy["count_dimension"]
    image = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(image)

    text(draw, (80, 64), "懂车帝双渠道内容与卖点评估", F_TITLE)
    text(draw, (80, 132), "v4.0终版 · 全部发布口径 · 2026-08-02", F_SUB, MUTED)
    px = 80
    px = pill(draw, px, 184, "仅完整媒体证据", BLUE_LIGHT, BLUE)
    px = pill(draw, px, 184, "抖音/小红书分开统计", GREEN_LIGHT, GREEN)
    px = pill(draw, px, 184, "不使用标题单独口径", ORANGE_LIGHT, "#A86800")
    pill(draw, px, 184, "拉新效果不以潜力替代", RED_LIGHT, RED)

    # Douyin
    rounded(draw, (60, 258, 1740, 1026), WHITE, 28, LINE)
    text(draw, (92, 292), "抖音渠道", F_SECTION)
    text(draw, (255, 302), "438条发布 · 30个随机账号样本", F_SUB, MUTED)

    card_w = 380
    kpi(draw, 92, 356, card_w, f"{dm['selling_point_covered_pct_all']:.1f}%", "卖点覆盖", f"{dm['selling_point_covered']}/438条", BLUE, BLUE_LIGHT)
    kpi(draw, 492, 356, card_w, f"{dm['core_pct_all']:.1f}%", "核心卖点覆盖", f"{dm['core']}/438条 · 目标60%-70%", ORANGE, ORANGE_LIGHT)
    kpi(draw, 892, 356, card_w, f"{dm['content_automotive']['score']}/100", "内容汽车性", f"汽车内容{dm['content_automotive']['automotive_publications_pct_all']:.1f}%", GREEN, GREEN_LIGHT)
    kpi(draw, 1292, 356, 354, "7条", "纠正误标", "片尾/中后段短植入", RED, RED_LIGHT)

    text(draw, (92, 548), "条数维度", F_LABEL)
    metric_bar(draw, 92, 594, 710, "可识别内容", dm["identifiable"], 438, PURPLE)
    metric_bar(draw, 92, 678, 710, "卖点覆盖", dm["selling_point_covered"], 438, BLUE)
    metric_bar(draw, 92, 762, 710, "核心卖点", dm["core"], 438, ORANGE)
    metric_bar(draw, 92, 846, 710, "其他卖点", dm["other"], 438, GREEN)

    text(draw, (870, 548), "三个业务场景（卖点命中322条）", F_LABEL)
    scene_colors = {"二手车": ORANGE, "新车": BLUE, "媒体-AI小懂": GREEN}
    sy = 602
    for scene in ("二手车", "新车", "媒体-AI小懂"):
        n = dm["scene_counts"][scene]
        core = dm["scene_core_counts"][scene]
        other = dm["scene_other_counts"][scene]
        text(draw, (870, sy), scene, F_BODY)
        text(draw, (1618, sy), f"{n}条 · 核心{core} / 其他{other}", F_BAR, anchor="ra")
        top = sy + 40
        rounded(draw, (870, top, 1618, top + 24), "#E9EDF4", 12)
        rounded(draw, (870, top, 870 + max(24, int(748 * n / 322)), top + 24), scene_colors[scene], 12)
        sy += 104

    rounded(draw, (870, 916, 1618, 978), RED_LIGHT, 16)
    text(draw, (894, 935), "曝光：不可计算（438条play_count均为占位值0）", F_BODY, RED)

    # Xiaohongshu
    rounded(draw, (60, 1062, 1740, 1624), WHITE, 28, LINE)
    text(draw, (92, 1096), "小红书渠道", F_SECTION)
    text(draw, (286, 1106), "338条唯一内容链接", F_SUB, MUTED)

    kpi(draw, 92, 1160, card_w, "10/338", "完整卖点标注", "仅2.96%，不能上卷", RED, RED_LIGHT)
    kpi(draw, 492, 1160, card_w, "306/338", "浏览量字段", "覆盖90.53%", BLUE, BLUE_LIGHT)
    kpi(draw, 892, 1160, card_w, "5/338", "标签×浏览量交叉", "仅1.48%，曝光不可算", ORANGE, ORANGE_LIGHT)
    kpi(draw, 1292, 1160, 354, "3/10", "样本卖点覆盖", "核心0条 · 仅诊断", PURPLE, "#F0EDFF")

    status_row(draw, 92, 1352, 500, "内容汽车性", "88/100", "方向性后分层估计，不是全量逐条结果", GREEN, GREEN_LIGHT)
    status_row(draw, 616, 1352, 500, "互动受众汽车性", "15/100", "评论达标5+5样本，存在替补偏差", ORANGE, ORANGE_LIGHT)
    status_row(draw, 1140, 1352, 506, "懂车帝拉新效果", "尚未测试", "没有懂车帝侧归因新增数据", RED, RED_LIGHT)

    rounded(draw, (92, 1466, 1646, 1576), ORANGE_LIGHT, 18)
    paragraph(
        draw,
        (118, 1491),
        "小红书当前只能输出样本诊断，不能输出渠道级条数/曝光卖点分布。需补齐其余328条的全部图片或视频主体证据。",
        F_BODY,
        1498,
        fill="#875A00",
        gap=6,
    )

    # Bottom conclusion
    rounded(draw, (60, 1660, 1740, 1836), "#152033", 28)
    text(draw, (92, 1692), "本轮可直接用于决策的结论", F_SECTION, WHITE)
    text(draw, (92, 1747), "抖音：核心卖点42.24%，显著低于60%-70%生产目标；新车X3贡献了184条核心内容。", F_BODY, WHITE)
    text(draw, (92, 1791), "小红书：数据覆盖不足，先补全内容证据，再谈渠道卖点分布；实际拉新效果需等待懂车帝侧数据。", F_BODY, WHITE)

    image.save(OUTPUT, optimize=True)
    print(OUTPUT)


if __name__ == "__main__":
    main()
