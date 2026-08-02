"""Build the frozen 150-pair duplicate calibration set from the migrated corpus."""

from __future__ import annotations

import argparse
import itertools
import json
from collections import Counter, defaultdict, deque
from pathlib import Path
from typing import Any, Dict, List, Tuple

from v8.duplicates import CALIBRATION_PATH, _current_fingerprints, compare_fingerprints
from v8.storage import DEFAULT_DB, connect


MANUAL_POSITIVES = {
    ("FDR3VR", "FVEEW7"): "人工画面对照：抖音与小红书为同一油门踏板/AEB 素材",
    ("E89DU6", "9ENK4R"): "跨平台标题、主题与关键帧一致：轮胎 vs 转向拉杆",
}
MANUAL_NEGATIVES = {
    ("8N2ANE", "DQXSD9"): "人工画面对照：夜间道路与蓝色车辆是不同视频",
    ("ER7PUF", "LCEE3H"): "不同命题：一万元家用车与首车能源类型",
    ("C2UBT3", "LCEE3H"): "不同命题：老车车龄与首车能源类型",
    ("S62ANS", "R2TX6G"): "不同车型与脚本：宝马 X3 与奔腾 B70",
    ("87S3LH", "39TZJP"): "不同车型与脚本：本田思域与本田皓影",
    ("RXULL9", "Y5MR6X"): "不同车型与脚本：凯迪拉克 CT5 与大众 CC",
    ("SEMV6N", "WY3GY6"): "不同车型与脚本：卡宴/卡罗拉与亚洲龙",
    ("ZZJDH2", "GQ4H8A"): "不同命题：二手车购买与速腾报价",
    ("C8DQ47", "V69UTG"): "不同车型与脚本：吉利帝豪与长安糯玉米",
    ("ZJZT5M", "R2TX6G"): "不同车型与脚本：凯美瑞与奔腾 B70",
    ("PTZ4CJ", "9TVT5E"): "不同命题：锋兰达报价与懂车人车型选择",
}


def _ordered_pair(left: str, right: str) -> Tuple[str, str]:
    return (min(left, right), max(left, right))


def build_dataset(*, db_path: Path = DEFAULT_DB) -> Dict[str, Any]:
    with connect(db_path) as connection:
        fingerprints = _current_fingerprints(connection)
        metadata = {
            int(row["content_id"]): dict(
                connection.execute(
                    "SELECT link_id,platform,title FROM content_items WHERE id=?",
                    (row["content_id"],),
                ).fetchone()
            )
            for row in fingerprints
        }
    by_link = {str(metadata[int(row["content_id"])]["link_id"]): row for row in fingerprints}
    for pair in [*MANUAL_POSITIVES, *MANUAL_NEGATIVES]:
        if any(link_id not in by_link for link_id in pair):
            raise RuntimeError(f"manual calibration pair is absent: {pair}")

    text_groups: Dict[str, List[str]] = defaultdict(list)
    media_groups: Dict[str, List[str]] = defaultdict(list)
    for row in fingerprints:
        link_id = str(metadata[int(row["content_id"])]["link_id"])
        if row["text_sha256"]:
            text_groups[str(row["text_sha256"])].append(link_id)
        for media_sha256 in json.loads(str(row["media_sha256_json"])):
            media_groups[str(media_sha256)].append(link_id)

    objective_groups: List[deque[Tuple[str, str, str]]] = []
    for kind, groups in (("正文 SHA-256", text_groups), ("媒体字节 SHA-256", media_groups)):
        for identity, members in sorted(groups.items()):
            if len(members) < 2:
                continue
            pairs = deque(
                (left, right, f"{kind} 完全一致：{identity[:12]}")
                for left, right in itertools.combinations(sorted(set(members)), 2)
            )
            if pairs:
                objective_groups.append(pairs)

    positive_pairs: List[Dict[str, Any]] = []
    used = set()
    for pair, rationale in MANUAL_POSITIVES.items():
        identity = _ordered_pair(*pair)
        used.add(identity)
        positive_pairs.append(
            {"left_link_id": pair[0], "right_link_id": pair[1], "label": "duplicate", "rationale": rationale}
        )
    while len(positive_pairs) < 75:
        made_progress = False
        for group in objective_groups:
            while group:
                left, right, rationale = group.popleft()
                identity = _ordered_pair(left, right)
                if identity in used:
                    continue
                used.add(identity)
                positive_pairs.append(
                    {"left_link_id": left, "right_link_id": right, "label": "duplicate", "rationale": rationale}
                )
                made_progress = True
                break
            if len(positive_pairs) == 75:
                break
        if not made_progress:
            raise RuntimeError("not enough objective positive pairs")

    negative_pairs: List[Dict[str, Any]] = []
    negative_used = set(used)
    usage: Counter[str] = Counter()
    for pair, rationale in MANUAL_NEGATIVES.items():
        identity = _ordered_pair(*pair)
        negative_used.add(identity)
        usage.update(pair)
        negative_pairs.append(
            {"left_link_id": pair[0], "right_link_id": pair[1], "label": "distinct", "rationale": rationale}
        )

    candidates: List[Tuple[Tuple[float, float, float], str, str, str]] = []
    for index, left in enumerate(fingerprints):
        for right in fingerprints[index + 1:]:
            left_link = str(metadata[int(left["content_id"])]["link_id"])
            right_link = str(metadata[int(right["content_id"])]["link_id"])
            identity = _ordered_pair(left_link, right_link)
            if identity in negative_used:
                continue
            result = compare_fingerprints(left, right)
            if result["confirmed"] or result["exact_media"] or result["exact_text"]:
                continue
            similarities = result["similarities"]
            text_similarity = float(similarities.get("text") or 0)
            ocr_similarity = float(similarities.get("ocr") or 0)
            asr_similarity = float(similarities.get("asr") or 0)
            distance = result["phash_distance"]
            if max(text_similarity, ocr_similarity) >= 0.78:
                continue
            if distance is not None and float(distance) <= 6.0:
                continue
            left_title = str(metadata[int(left["content_id"])]["title"] or "")
            right_title = str(metadata[int(right["content_id"])]["title"] or "")
            if left_title == right_title:
                continue
            visual_risk = 1.0 - float(distance) / 64.0 if distance is not None else 0.0
            risk = (asr_similarity, visual_risk, max(text_similarity, ocr_similarity))
            rationale = (
                "高难负样本：旧 ASR 高相似，但正文、OCR、媒体指纹与内容命题不共同成立"
            )
            candidates.append((risk, left_link, right_link, rationale))
    for _, left, right, rationale in sorted(candidates, reverse=True):
        if len(negative_pairs) == 75:
            break
        if usage[left] >= 4 or usage[right] >= 4:
            continue
        identity = _ordered_pair(left, right)
        if identity in negative_used:
            continue
        negative_used.add(identity)
        usage.update((left, right))
        negative_pairs.append(
            {"left_link_id": left, "right_link_id": right, "label": "distinct", "rationale": rationale}
        )
    if len(negative_pairs) != 75:
        raise RuntimeError(f"not enough defensible negative pairs: {len(negative_pairs)}")

    pairs = positive_pairs + negative_pairs
    if len({_ordered_pair(item["left_link_id"], item["right_link_id"]) for item in pairs}) != 150:
        raise RuntimeError("calibration pairs are not unique")
    return {
        "version": "duplicate-calibration-v1",
        "fingerprint_version": str(fingerprints[0]["fingerprint_version"]),
        "pair_count": 150,
        "positive_count": 75,
        "negative_count": 75,
        "label_policy": (
            "正样本由媒体字节/规范化正文完全一致或人工画面对照确认；"
            "负样本由人工明确不同命题对与 ASR 污染高难对组成。"
        ),
        "pairs": pairs,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--output", type=Path, default=CALIBRATION_PATH)
    args = parser.parse_args()
    dataset = build_dataset(db_path=args.db)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(dataset, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(args.output), **{key: dataset[key] for key in ("pair_count", "positive_count", "negative_count")}}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
