#!/usr/bin/env python3
"""Generate the shareable v6 TikHub-enriched channel conclusion image."""

from __future__ import annotations

import json
from pathlib import Path

from project_paths import CURRENT_REPORTS_DIR

from PIL import Image, ImageDraw, ImageFont


DATA = CURRENT_REPORTS_DIR / "双渠道结构化结论_v6_TikHub补充_2026-08-02.json"
OUT = CURRENT_REPORTS_DIR / "双渠道核心结论_v6_TikHub补充_2026-08-02.png"
W, H = 1800, 2100
BG = "#F4F7FC"
WHITE = "#FFFFFF"
INK = "#172033"
MUTED = "#6B7890"
BORDER = "#D9E2F0"
TRACK = "#E9EEF6"
BLUE = "#2F6BFF"
PURPLE = "#7A5AF8"
GREEN = "#18A979"
ORANGE = "#F59E0B"
RED = "#E94B55"
NAVY = "#162238"


FONT_PATHS = [
    "/System/Library/Fonts/PingFang.ttc",
    "/System/Library/Fonts/STHeiti Medium.ttc",
    "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
]


def font(size: int, index: int = 0) -> ImageFont.FreeTypeFont:
    for path in FONT_PATHS:
        try:
            return ImageFont.truetype(path, size=size, index=index)
        except OSError:
            continue
    return ImageFont.load_default()


F_TITLE = font(54)
F_H1 = font(34)
F_H2 = font(25)
F_BODY = font(21)
F_SMALL = font(17)
F_BIG = font(48)


def rounded(draw: ImageDraw.ImageDraw, box, fill, radius=22, outline=None, width=1):
    draw.rounded_rectangle(box, radius=radius, fill=fill, outline=outline, width=width)


def text(draw: ImageDraw.ImageDraw, xy, value, fnt, fill=INK, anchor=None):
    draw.text(xy, str(value), font=fnt, fill=fill, anchor=anchor)


def bar(draw, x, y, width, share, label, value, color):
    text(draw, (x, y), label, F_BODY)
    text(draw, (x + width, y), value, F_BODY, MUTED, "ra")
    rounded(draw, (x, y + 34, x + width, y + 54), TRACK, 10)
    rounded(draw, (x, y + 34, x + max(8, width * share / 100), y + 54), color, 10)


def stat_card(draw, box, value, label, note, color):
    x1, y1, x2, y2 = box
    rounded(draw, box, WHITE, 20, BORDER, 2)
    rounded(draw, (x1, y1, x1 + 8, y2), color, 4)
    text(draw, (x1 + 28, y1 + 24), value, F_BIG, color)
    text(draw, (x1 + 28, y1 + 86), label, F_BODY)
    text(draw, (x1 + 28, y1 + 120), note, F_SMALL, MUTED)


def main() -> int:
    data = json.loads(DATA.read_text(encoding="utf-8"))
    d = data["channels"]["douyin"]
    s = d["summary"]
    c = s["count_dimension"]
    e = s["exposure_dimension"]
    v = s["verticality"]
    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)

    text(draw, (68, 60), "懂车帝双渠道内容与卖点评估", F_TITLE)
    text(draw, (70, 126), "v6.0 TikHub补充 · 全部发布口径 · 2026-08-02", F_BODY, MUTED)
    badges = [
        ("完整媒体证据", BLUE, "#EAF0FF"),
        ("真实播放量", GREEN, "#E5F7F1"),
        ("评论有效用户门槛≥20", ORANGE, "#FFF3D7"),
        ("实际新增仍需懂车帝归因", RED, "#FDEBED"),
    ]
    bx = 70
    for label, color, fill in badges:
        tw = draw.textlength(label, font=F_SMALL)
        rounded(draw, (bx, 170, bx + tw + 30, 212), fill, 21)
        text(draw, (bx + 15, 180), label, F_SMALL, color)
        bx += tw + 44

    rounded(draw, (52, 242, 1748, 850), WHITE, 24, BORDER, 2)
    text(draw, (80, 272), "抖音渠道 · 438条发布", F_H1)
    text(draw, (80, 326), "条数维度", F_H2)
    text(draw, (900, 326), "曝光维度", F_H2)
    text(draw, (900, 362), f"426/438条有效播放（97.26%） · 合计 {e['total_valid_exposure']:,} 次", F_SMALL, MUTED)

    count_rows = [
        ("可识别内容", c["identifiable_share"], f"{c['identifiable']}条  {c['identifiable_share']}%", PURPLE),
        ("卖点覆盖", c["selling_point_covered_share"], f"{c['selling_point_covered']}条  {c['selling_point_covered_share']}%", BLUE),
        ("核心卖点", c["core_share"], f"{c['core']}条  {c['core_share']}%", ORANGE),
        ("其他卖点", c["other_share"], f"{c['other']}条  {c['other_share']}%", GREEN),
    ]
    exp_rows = [
        ("可识别内容曝光", e["identifiable"]["share"], f"{e['identifiable']['share']}%", PURPLE),
        ("卖点覆盖曝光", e["selling_point_covered"]["share"], f"{e['selling_point_covered']['share']}%", BLUE),
        ("核心卖点曝光", e["core"]["share"], f"{e['core']['share']}%", ORANGE),
        ("其他卖点曝光", e["other"]["share"], f"{e['other']['share']}%", GREEN),
    ]
    for i, row in enumerate(count_rows):
        bar(draw, 82, 392 + i * 96, 700, row[1], row[0], row[2], row[3])
    for i, row in enumerate(exp_rows):
        bar(draw, 902, 412 + i * 96, 760, row[1], row[0], row[2], row[3])

    rounded(draw, (52, 884, 1748, 1164), WHITE, 24, BORDER, 2)
    text(draw, (80, 914), "内容垂直度与拉新", F_H1)
    cards = [
        (f"{v['content_automotive']['score']}/100", "内容汽车性", "主体属于汽车内容", GREEN),
        (f"{v['audience_automotive']['score']}/100", "互动受众汽车性", "67条达标内容 · 用户混合", ORANGE),
        (f"{v['acquisition_effect_estimate']['score']}/100", "懂车帝拉新效果预估", "下载理由不足 · 暂不建议", PURPLE),
        ("—", "懂车帝实际拉新效果", "TikHub没有跨App新增归因", RED),
    ]
    for i, card in enumerate(cards):
        x = 80 + i * 410
        stat_card(draw, (x, 974, x + 376, 1138), *card)

    rounded(draw, (52, 1198, 1748, 1595), WHITE, 24, BORDER, 2)
    text(draw, (80, 1228), "三个业务场景", F_H1)
    headers = ["场景", "卖点条数", "曝光贡献", "核心曝光", "互动受众", "拉新预估"]
    xs = [92, 414, 690, 970, 1235, 1495]
    for x, h in zip(xs, headers):
        text(draw, (x, 1290), h, F_SMALL, MUTED)
    for idx, scene in enumerate(v4_scenes := ["二手车", "新车", "媒体-AI小懂"]):
        row = d["scenes"][scene]
        y = 1342 + idx * 78
        if idx:
            draw.line((82, y - 22, 1670, y - 22), fill=BORDER, width=1)
        text(draw, (92, y), scene, F_BODY)
        text(draw, (414, y), f"{row['publication_n']}/438 · {row['count_share_all']}%", F_BODY)
        text(draw, (690, y), f"{row['exposure_share_all']}%", F_BODY)
        text(draw, (970, y), f"{row['core_exposure_share_all']}%", F_BODY)
        audience = "—" if row["audience_automotive_score"] is None else f"{row['audience_automotive_score']}/100"
        acquisition = "—" if row["acquisition_effect_estimate"] is None else f"{row['acquisition_effect_estimate']}/100"
        text(draw, (1235, y), audience, F_BODY)
        text(draw, (1495, y), acquisition, F_BODY)
    text(draw, (82, 1554), "注：二手车场景没有达到20名有效独立评论用户的内容，不能写成0分。", F_SMALL, MUTED)

    rounded(draw, (52, 1628, 1748, 1816), "#FFF4D8", 24)
    text(draw, (80, 1660), "本轮最重要的判断", F_H1, "#8A5A00")
    text(draw, (82, 1716), "核心卖点占发布条数 42.24%，却只贡献 5.20% 有效曝光；核心内容既生产不足，流量效率也明显偏低。", F_BODY, "#744E08")
    text(draw, (82, 1760), "互动受众 40/100、拉新预估 34/100 只代表67条高评论内容；不能替代懂车帝实际新增。", F_BODY, "#744E08")

    rounded(draw, (52, 1850, 1748, 2042), NAVY, 24)
    text(draw, (80, 1880), "小红书渠道 · 本轮未新增采集", F_H1, WHITE)
    text(draw, (82, 1934), "338条链接仅10条完成全媒体卖点标注：条数和曝光分布仍不可上卷。", F_BODY, "#D8E1F0")
    text(draw, (82, 1976), "内容汽车性88/100为方向性估计；互动受众15/100仅代表5+5样本；实际拉新尚未测试。", F_BODY, "#D8E1F0")

    img.save(OUT, optimize=True)
    print(OUT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
