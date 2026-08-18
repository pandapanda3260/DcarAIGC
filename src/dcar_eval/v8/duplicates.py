"""High-precision duplicate fingerprints, calibration, and canonical relations."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import subprocess
import tempfile
from collections import Counter
from importlib.metadata import version as package_version
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

import imagehash  # type: ignore[import-untyped]
from PIL import Image

from .media import _resolved, _run_processing_slot, file_sha256
from .storage import (
    DEFAULT_DB,
    PROJECT_ROOT,
    connect,
    is_formal_database_path,
    now_utc,
    transaction,
)


FINGERPRINT_VERSION = (
    "duplicate-fingerprint-v1|ImageHash==4.3.2|Pillow==12.3.0|phash64|simhash64"
)
RELATION_METHOD = "fingerprint_v1"
CALIBRATION_PATH = PROJECT_ROOT / "config" / "duplicate_calibration_v1.json"
FINGERPRINT_ROOT = PROJECT_ROOT / "data" / "cache" / "v8" / "duplicates"
THRESHOLDS: Dict[str, float] = {
    "phash_strong_distance": 3.0,
    "phash_confirm_distance": 6.0,
    "visual_semantic_min": 0.84,
    "precision_release_min": 0.95,
}


class DuplicateDetectionError(RuntimeError):
    pass


def _fingerprint_root_for_database(db_path: Path) -> Path:
    if is_formal_database_path(db_path, formal_database=DEFAULT_DB):
        return FINGERPRINT_ROOT
    return db_path.parent / "duplicate-fingerprints"


def duplicate_metric_decision(
    total: int,
    fingerprint_count: int,
    calibration_ready: bool,
    *,
    threshold: float,
) -> tuple[str, Optional[float], str]:
    """Decide whether duplicate rate is publishable from explicit prerequisites."""

    if total == 0:
        return "not_applicable", None, "统计范围内没有内容"
    coverage = round(fingerprint_count * 100 / total, 2)
    if not calibration_ready:
        return (
            "not_calculable",
            coverage,
            "重复内容感知指纹尚未完成定标，重复率暂不可计算",
        )
    if coverage < threshold:
        return (
            "below_threshold",
            coverage,
            f"感知指纹覆盖率为 {coverage:.2f}%，低于 {threshold:g}% 发布阈值",
        )
    return "available", coverage, ""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _normalize_text(value: str) -> str:
    return "".join(re.findall(r"[\u4e00-\u9fffA-Za-z0-9]+", value.lower()))


def _simhash(value: str) -> Optional[str]:
    normalized = _normalize_text(value)
    if len(normalized) < 12:
        return None
    width = 3 if len(normalized) >= 20 else 2
    counts = Counter(normalized[index:index + width] for index in range(len(normalized) - width + 1))
    vector = [0] * 64
    for token, raw_weight in counts.items():
        digest = hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest()
        bits = int.from_bytes(digest, "big")
        weight = min(raw_weight, 3)
        for bit in range(64):
            vector[bit] += weight if bits & (1 << bit) else -weight
    result = sum(1 << bit for bit, score in enumerate(vector) if score >= 0)
    return f"{result:016x}"


def _simhash_similarity(left: Optional[str], right: Optional[str]) -> Optional[float]:
    if not left or not right:
        return None
    distance = (int(left, 16) ^ int(right, 16)).bit_count()
    return round(1.0 - distance / 64.0, 6)


def _latest_artifact(
    connection: sqlite3.Connection, content_id: int, artifact_types: Sequence[str]
) -> Optional[sqlite3.Row]:
    placeholders = ",".join("?" for _ in artifact_types)
    return connection.execute(
        f"""
        SELECT * FROM evidence_artifacts
        WHERE content_id=? AND artifact_type IN ({placeholders})
          AND status='available' AND sha256 IS NOT NULL
        ORDER BY id DESC LIMIT 1
        """,
        (content_id, *artifact_types),
    ).fetchone()


def _artifact_text(row: Optional[sqlite3.Row], keys: Sequence[str]) -> str:
    if row is None:
        return ""
    value = _read_json(_resolved(str(row["local_path"])))
    for key in keys:
        text = value.get(key)
        if isinstance(text, str) and text.strip():
            return text.strip()
    values = value.get("texts")
    if isinstance(values, list):
        parts = [
            str(item.get("text") or "").strip() if isinstance(item, dict) else str(item).strip()
            for item in values
        ]
        return "\n".join(part for part in parts if part)
    return ""


def _manifest_media_paths(path: Path) -> List[Path]:
    value = _read_json(path)
    candidates: List[str] = []
    video_path = value.get("video_path")
    if isinstance(video_path, str) and video_path:
        candidates.append(video_path)
    for key in ("image_paths", "images"):
        items = value.get(key)
        if not isinstance(items, list):
            continue
        for item in items:
            if isinstance(item, str):
                candidates.append(item)
            elif isinstance(item, dict) and item.get("path"):
                candidates.append(str(item["path"]))
    frames = value.get("frames")
    if isinstance(frames, list):
        candidates.extend(
            str(item["path"])
            for item in frames
            if isinstance(item, dict) and item.get("path")
        )
    result: List[Path] = []
    for raw in dict.fromkeys(candidates):
        resolved = _resolved(raw)
        if resolved.is_file():
            result.append(resolved)
    return result


def _probe_duration(path: Path) -> float:
    completed = subprocess.run(
        [
            "ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", str(path),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    try:
        return float(completed.stdout.strip()) if completed.returncode == 0 else 0.0
    except ValueError:
        return 0.0


def _video_frame_paths(path: Path, target: Path) -> List[Path]:
    duration = _probe_duration(path)
    if duration <= 0:
        return []
    target.mkdir(parents=True, exist_ok=True)
    output: List[Path] = []
    for index, fraction in enumerate((0.10, 0.30, 0.50, 0.70, 0.90)):
        timestamp = min(max(0.05, duration * fraction), max(0.05, duration - 0.05))
        frame = target / f"frame-{index:02d}.jpg"
        completed = subprocess.run(
            [
                "ffmpeg", "-y", "-nostdin", "-loglevel", "error", "-ss", str(timestamp),
                "-i", str(path), "-frames:v", "1", "-vf", "scale=384:-1", "-q:v", "3",
                str(frame),
            ],
            check=False,
            capture_output=True,
            timeout=60,
        )
        if completed.returncode == 0 and frame.is_file() and frame.stat().st_size > 512:
            output.append(frame)
    return output


def _image_phash(path: Path) -> Optional[str]:
    try:
        with Image.open(path) as source:
            value = str(imagehash.phash(source.convert("RGB"), hash_size=8, highfreq_factor=4))
    except (OSError, ValueError):
        return None
    ones = int(value, 16).bit_count()
    return value if 8 <= ones <= 56 else None


def _media_fingerprints(media_path: Optional[Path]) -> tuple[List[str], List[str]]:
    if media_path is None or not media_path.is_file():
        return [], []
    paths = _manifest_media_paths(media_path) if media_path.suffix.lower() == ".json" else [media_path]
    media_sha256 = [file_sha256(path) for path in paths]
    phashes: List[str] = []
    with tempfile.TemporaryDirectory(prefix="dcar-duplicate-") as temporary:
        for index, path in enumerate(paths[:8]):
            if path.suffix.lower() in {".mp4", ".mov", ".m4v", ".webm"}:
                frames = _video_frame_paths(path, Path(temporary) / f"video-{index}")
                phashes.extend(value for frame in frames if (value := _image_phash(frame)))
            else:
                value = _image_phash(path)
                if value:
                    phashes.append(value)
    return list(dict.fromkeys(media_sha256)), phashes


def _source_inputs(connection: sqlite3.Connection, content_id: int) -> Dict[str, Any]:
    content = connection.execute(
        "SELECT * FROM content_items WHERE id=?", (content_id,)
    ).fetchone()
    if content is None:
        raise DuplicateDetectionError(f"content {content_id} does not exist")
    media = _latest_artifact(connection, content_id, ("media", "media_manifest"))
    asr = _latest_artifact(connection, content_id, ("asr", "transcript", "media_transcript"))
    ocr = _latest_artifact(connection, content_id, ("ocr", "media_ocr"))
    text = f"{content['title']}\n{content['body']}"
    return {
        "content": dict(content),
        "text": text,
        "asr_text": _artifact_text(asr, ("text", "combined_text")),
        "ocr_text": _artifact_text(ocr, ("combined_text", "text")),
        "media_path": _resolved(str(media["local_path"])) if media is not None else None,
        "source": {
            "fingerprint_version": FINGERPRINT_VERSION,
            "text_sha256": hashlib.sha256(_normalize_text(text).encode("utf-8")).hexdigest(),
            "media_artifact_sha256": str(media["sha256"]) if media is not None else None,
            "asr_artifact_sha256": str(asr["sha256"]) if asr is not None else None,
            "ocr_artifact_sha256": str(ocr["sha256"]) if ocr is not None else None,
        },
    }


def _current_source_state(
    connection: sqlite3.Connection, content_id: int
) -> tuple[Dict[str, Any], str]:
    """Return the exact inputs and source digest used by fingerprint creation."""

    inputs = _source_inputs(connection, content_id)
    return inputs, _sha256_json(inputs["source"])


def fingerprint_content(content_id: int, *, db_path: Path = DEFAULT_DB) -> Dict[str, Any]:
    if package_version("ImageHash") != "4.3.2" or package_version("Pillow") != "12.3.0":
        raise DuplicateDetectionError("duplicate processor dependency version mismatch")
    with connect(db_path) as connection:
        inputs, source_sha256 = _current_source_state(connection, content_id)
    content = inputs["content"]
    output_root = _fingerprint_root_for_database(db_path)
    target = output_root / f"{content['link_id']}.json"

    def produce() -> Path:
        media_sha256, phashes = _media_fingerprints(inputs["media_path"])
        normalized_text = _normalize_text(inputs["text"])
        normalized_asr = _normalize_text(inputs["asr_text"])
        normalized_ocr = _normalize_text(inputs["ocr_text"])
        payload = {
            "schema_version": "duplicate-fingerprint-v1",
            "fingerprint_version": FINGERPRINT_VERSION,
            "content_id": content_id,
            "source_sha256": source_sha256,
            "text_sha256": (
                hashlib.sha256(normalized_text.encode("utf-8")).hexdigest()
                if len(normalized_text) >= 12 else None
            ),
            "media_sha256": media_sha256,
            "frame_phashes": phashes,
            "text_simhash": _simhash(inputs["text"]),
            "asr_simhash": _simhash(inputs["asr_text"]),
            "ocr_simhash": _simhash(inputs["ocr_text"]),
            "text_char_count": len(normalized_text),
            "asr_char_count": len(normalized_asr),
            "ocr_char_count": len(normalized_ocr),
            "created_at": now_utc(),
        }
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.name}.tmp")
        temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
        temporary.replace(target)
        return target

    artifact = _run_processing_slot(
        db_path=db_path,
        content_id=content_id,
        source_sha256=source_sha256,
        processor_type="duplicate_fingerprint",
        processor_version=FINGERPRINT_VERSION,
        artifact_type="duplicate_fingerprint",
        produce=produce,
    )
    payload = _read_json(_resolved(artifact.local_path))
    if payload.get("source_sha256") != source_sha256:
        raise DuplicateDetectionError("cached duplicate fingerprint has stale source")
    with connect(db_path) as connection, transaction(connection):
        connection.execute(
            """
            INSERT INTO duplicate_fingerprints(
                content_id, fingerprint_version, source_sha256, text_sha256,
                media_sha256_json, frame_phashes_json, text_simhash, asr_simhash,
                ocr_simhash, text_char_count, asr_char_count, ocr_char_count,
                artifact_id, payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(content_id, fingerprint_version, source_sha256) DO UPDATE SET
                artifact_id=excluded.artifact_id, payload_json=excluded.payload_json
            """,
            (
                content_id, FINGERPRINT_VERSION, source_sha256, payload.get("text_sha256"),
                _canonical_json(payload.get("media_sha256") or []),
                _canonical_json(payload.get("frame_phashes") or []),
                payload.get("text_simhash"), payload.get("asr_simhash"),
                payload.get("ocr_simhash"), int(payload.get("text_char_count") or 0),
                int(payload.get("asr_char_count") or 0), int(payload.get("ocr_char_count") or 0),
                artifact.id, _canonical_json(payload), str(payload.get("created_at") or now_utc()),
            ),
        )
    return payload


def _phash_distance(left: Sequence[str], right: Sequence[str]) -> tuple[Optional[float], int]:
    if not left or not right:
        return None, 0
    left_values = [int(value, 16) for value in left]
    right_values = [int(value, 16) for value in right]
    left_nearest = [min((value ^ other).bit_count() for other in right_values) for value in left_values]
    right_nearest = [min((value ^ other).bit_count() for other in left_values) for value in right_values]
    distance = (sum(left_nearest) / len(left_nearest) + sum(right_nearest) / len(right_nearest)) / 2
    return round(distance, 6), min(len(left_values), len(right_values))


def compare_fingerprints(left: Mapping[str, Any], right: Mapping[str, Any]) -> Dict[str, Any]:
    left_media = set(json.loads(str(left["media_sha256_json"])))
    right_media = set(json.loads(str(right["media_sha256_json"])))
    left_phash = list(json.loads(str(left["frame_phashes_json"])))
    right_phash = list(json.loads(str(right["frame_phashes_json"])))
    phash_distance, phash_match_count = _phash_distance(left_phash, right_phash)
    similarities = {
        "text": _simhash_similarity(left.get("text_simhash"), right.get("text_simhash")),
        "asr": _simhash_similarity(left.get("asr_simhash"), right.get("asr_simhash")),
        "ocr": _simhash_similarity(left.get("ocr_simhash"), right.get("ocr_simhash")),
    }
    semantic_values = [value for value in similarities.values() if value is not None]
    semantic_max = max(semantic_values, default=0.0)
    exact_media = bool(left_media & right_media)
    exact_text = bool(left.get("text_sha256") and left.get("text_sha256") == right.get("text_sha256"))
    diverse_visual = len(set(left_phash)) >= 2 and len(set(right_phash)) >= 2
    strong_visual = bool(
        phash_distance is not None
        and phash_match_count >= 3
        and diverse_visual
        and phash_distance <= THRESHOLDS["phash_strong_distance"]
    )
    visual_semantic = bool(
        phash_distance is not None
        and phash_match_count >= 2
        and phash_distance <= THRESHOLDS["phash_confirm_distance"]
        and semantic_max >= THRESHOLDS["visual_semantic_min"]
    )
    reasons = [
        name
        for name, matched in (
            ("media_sha256", exact_media), ("text_sha256", exact_text),
            ("phash_strong", strong_visual), ("phash_plus_semantic", visual_semantic),
        )
        if matched
    ]
    confirmed = bool(reasons)
    confidence = 1.0 if exact_media or exact_text else max(
        1.0 - (phash_distance or 64.0) / 64.0 if phash_distance is not None else 0.0,
        semantic_max,
    )
    return {
        "confirmed": confirmed,
        "confidence": round(confidence, 6),
        "reasons": reasons,
        "exact_media": exact_media,
        "exact_text": exact_text,
        "phash_distance": phash_distance,
        "phash_match_count": phash_match_count,
        "similarities": similarities,
    }


def _current_fingerprints(connection: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT df.*, c.link_id, c.published_at, c.imported_at
        FROM duplicate_fingerprints df
        JOIN content_items c ON c.id=df.content_id
        WHERE df.fingerprint_version=? AND df.id=(
            SELECT df2.id FROM duplicate_fingerprints df2
            WHERE df2.content_id=df.content_id AND df2.fingerprint_version=df.fingerprint_version
            ORDER BY df2.created_at DESC,df2.id DESC LIMIT 1
        )
        ORDER BY df.content_id
        """,
        (FINGERPRINT_VERSION,),
    ).fetchall()
    return [dict(row) for row in rows]


def calibration_ready(*, db_path: Path = DEFAULT_DB) -> bool:
    with connect(db_path) as connection:
        row = connection.execute(
            """
            SELECT status FROM duplicate_calibration_runs
            WHERE fingerprint_version=? AND thresholds_json=?
            ORDER BY created_at DESC,id DESC LIMIT 1
            """,
            (FINGERPRINT_VERSION, _canonical_json(THRESHOLDS)),
        ).fetchone()
    return bool(row is not None and row["status"] == "passed")


def calibrate(
    dataset_path: Path = CALIBRATION_PATH, *, db_path: Path = DEFAULT_DB
) -> Dict[str, Any]:
    dataset = _read_json(dataset_path)
    pairs = dataset.get("pairs")
    if not isinstance(pairs, list):
        raise DuplicateDetectionError("calibration dataset has no pairs")
    dataset_sha256 = file_sha256(dataset_path)
    with connect(db_path) as connection:
        fingerprints = {str(row["link_id"]): row for row in _current_fingerprints(connection)}
    true_positive = false_positive = predicted_positive = actual_positive = 0
    outcomes: List[Dict[str, Any]] = []
    seen_pairs: set[tuple[str, str]] = set()
    for pair in pairs:
        if not isinstance(pair, dict):
            raise DuplicateDetectionError("invalid calibration pair")
        left_id = str(pair.get("left_link_id") or "")
        right_id = str(pair.get("right_link_id") or "")
        label = str(pair.get("label") or "")
        if label not in {"duplicate", "distinct"} or left_id not in fingerprints or right_id not in fingerprints:
            raise DuplicateDetectionError(f"invalid calibration pair: {left_id}/{right_id}/{label}")
        identity = (min(left_id, right_id), max(left_id, right_id))
        if left_id == right_id or identity in seen_pairs:
            raise DuplicateDetectionError(f"duplicate calibration pair: {left_id}/{right_id}")
        seen_pairs.add(identity)
        result = compare_fingerprints(fingerprints[left_id], fingerprints[right_id])
        predicted = bool(result["confirmed"])
        actual = label == "duplicate"
        predicted_positive += int(predicted)
        actual_positive += int(actual)
        true_positive += int(predicted and actual)
        false_positive += int(predicted and not actual)
        outcomes.append({"left_link_id": left_id, "right_link_id": right_id, "label": label, **result})
    precision = true_positive / predicted_positive if predicted_positive else 0.0
    recall = true_positive / actual_positive if actual_positive else 0.0
    positive_count = actual_positive
    negative_count = len(pairs) - actual_positive
    passed = bool(
        len(pairs) == 150
        and positive_count == 75
        and negative_count == 75
        and precision >= THRESHOLDS["precision_release_min"]
    )
    run_id = f"DUPCAL-{dataset_sha256[:12].upper()}"
    created_at = now_utc()
    with connect(db_path) as connection, transaction(connection):
        connection.execute(
            """
            INSERT INTO duplicate_calibration_runs(
                id, calibration_version, fingerprint_version, dataset_sha256,
                pair_count, positive_count, negative_count, predicted_positive_count,
                true_positive_count, false_positive_count, precision, recall,
                thresholds_json, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(calibration_version, fingerprint_version, dataset_sha256) DO UPDATE SET
                predicted_positive_count=excluded.predicted_positive_count,
                true_positive_count=excluded.true_positive_count,
                false_positive_count=excluded.false_positive_count,
                precision=excluded.precision, recall=excluded.recall,
                thresholds_json=excluded.thresholds_json, status=excluded.status,
                created_at=excluded.created_at
            """,
            (
                run_id, str(dataset.get("version") or "duplicate-calibration-v1"),
                FINGERPRINT_VERSION, dataset_sha256, len(pairs), positive_count,
                negative_count, predicted_positive, true_positive, false_positive,
                round(precision, 6), round(recall, 6), _canonical_json(THRESHOLDS),
                "passed" if passed else "failed", created_at,
            ),
        )
    return {
        "run_id": run_id, "status": "passed" if passed else "failed",
        "pair_count": len(pairs), "positive_count": positive_count,
        "negative_count": negative_count, "predicted_positive_count": predicted_positive,
        "true_positive_count": true_positive, "false_positive_count": false_positive,
        "precision": round(precision, 6), "recall": round(recall, 6),
        "outcomes": outcomes,
    }


def rebuild_duplicate_relations(*, db_path: Path = DEFAULT_DB) -> Dict[str, Any]:
    if not calibration_ready(db_path=db_path):
        raise DuplicateDetectionError("duplicate detector calibration has not passed")
    with connect(db_path) as connection:
        fingerprints = _current_fingerprints(connection)
    parent = {int(row["content_id"]): int(row["content_id"]) for row in fingerprints}

    def find(value: int) -> int:
        while parent[value] != value:
            parent[value] = parent[parent[value]]
            value = parent[value]
        return value

    def union(left: int, right: int) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    edges: List[Dict[str, Any]] = []
    for index, left in enumerate(fingerprints):
        for right in fingerprints[index + 1:]:
            result = compare_fingerprints(left, right)
            if not result["confirmed"]:
                continue
            left_id, right_id = int(left["content_id"]), int(right["content_id"])
            union(left_id, right_id)
            edges.append({"left": left_id, "right": right_id, **result})
    groups: Dict[int, List[Dict[str, Any]]] = {}
    for row in fingerprints:
        groups.setdefault(find(int(row["content_id"])), []).append(row)
    relations: List[tuple[int, int, float, str]] = []
    for members in groups.values():
        if len(members) < 2:
            continue
        original_row = min(
            members,
            key=lambda row: (str(row.get("published_at") or row.get("imported_at") or ""), int(row["content_id"])),
        )
        original = int(original_row["content_id"])
        member_ids = {int(row["content_id"]) for row in members}
        for member in members:
            duplicate = int(member["content_id"])
            if duplicate == original:
                continue
            candidates = [
                edge for edge in edges
                if duplicate in {edge["left"], edge["right"]}
                and edge["left"] in member_ids and edge["right"] in member_ids
            ]
            best = max(candidates, key=lambda edge: float(edge["confidence"])) if candidates else None
            evidence = {
                "fingerprint_version": FINGERPRINT_VERSION,
                "thresholds": THRESHOLDS,
                "cluster_members": sorted(member_ids),
                "best_edge": best,
            }
            relations.append(
                (duplicate, original, float(best["confidence"] if best else 1.0), _canonical_json(evidence))
            )
    with connect(db_path) as connection, transaction(connection):
        connection.execute(
            "DELETE FROM duplicate_relations WHERE method IN ('text_sha256', ?)",
            (RELATION_METHOD,),
        )
        for duplicate, original, confidence, evidence_json in relations:
            connection.execute(
                """
                INSERT INTO duplicate_relations(
                    duplicate_content_id, original_content_id, method, confidence,
                    evidence_json, status, created_at
                ) VALUES (?, ?, ?, ?, ?, 'confirmed', ?)
                """,
                (duplicate, original, RELATION_METHOD, confidence, evidence_json, now_utc()),
            )
    return {
        "fingerprints": len(fingerprints), "confirmed_edges": len(edges),
        "duplicate_groups": sum(len(members) > 1 for members in groups.values()),
        "duplicate_relations": len(relations),
    }


def refresh_content_duplicates(
    content_id: int, *, db_path: Path = DEFAULT_DB
) -> Dict[str, Any]:
    fingerprint = fingerprint_content(content_id, db_path=db_path)
    relations = (
        rebuild_duplicate_relations(db_path=db_path)
        if calibration_ready(db_path=db_path)
        else None
    )
    return {
        "content_id": content_id,
        "source_sha256": fingerprint["source_sha256"],
        "calibration_ready": relations is not None,
        "relations": relations,
    }


def _pending_content_ids(*, limit: Optional[int], db_path: Path) -> List[int]:
    with connect(db_path) as connection:
        content_rows = connection.execute(
            "SELECT id FROM content_items ORDER BY id"
        ).fetchall()
        fingerprint_rows = connection.execute(
            """
            SELECT content_id,source_sha256 FROM duplicate_fingerprints
            WHERE fingerprint_version=?
            """,
            (FINGERPRINT_VERSION,),
        ).fetchall()
        completed = {
            (int(row["content_id"]), str(row["source_sha256"]))
            for row in fingerprint_rows
        }
        if limit == 0:
            return []
        pending: List[int] = []
        for row in content_rows:
            content_id = int(row["id"])
            _, source_sha256 = _current_source_state(connection, content_id)
            if (content_id, source_sha256) in completed:
                continue
            pending.append(content_id)
            if limit is not None and limit > 0 and len(pending) >= limit:
                break
    return pending


def run_duplicate_fingerprint_queue(
    *, limit: Optional[int] = 200, db_path: Path = DEFAULT_DB
) -> Dict[str, Any]:
    content_ids = _pending_content_ids(limit=limit, db_path=db_path)
    failures: List[Dict[str, Any]] = []
    processed = 0
    for content_id in content_ids:
        try:
            fingerprint_content(content_id, db_path=db_path)
            processed += 1
        except Exception as exc:
            failures.append(
                {"content_id": content_id, "error": f"{type(exc).__name__}: {exc}"[:500]}
            )
    queue_drained = not _pending_content_ids(limit=1, db_path=db_path)
    ready = calibration_ready(db_path=db_path)
    relations: Optional[Dict[str, Any]] = None
    if processed and not failures and queue_drained and ready:
        relations = rebuild_duplicate_relations(db_path=db_path)
    return {
        "candidates": len(content_ids), "processed": processed, "failed": len(failures),
        "failures": failures, "relations": relations,
        "calibration_ready": ready,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("fingerprint", "calibrate", "rebuild", "status"))
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--dataset", type=Path, default=CALIBRATION_PATH)
    parser.add_argument("--limit", type=int, default=0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "fingerprint":
        result: Any = run_duplicate_fingerprint_queue(
            limit=None if args.limit == 0 else args.limit, db_path=args.db
        )
    elif args.command == "calibrate":
        result = calibrate(args.dataset, db_path=args.db)
    elif args.command == "rebuild":
        result = rebuild_duplicate_relations(db_path=args.db)
    else:
        with connect(args.db) as connection:
            result = {
                "fingerprints": int(connection.execute("SELECT COUNT(*) FROM duplicate_fingerprints").fetchone()[0]),
                "relations": int(connection.execute("SELECT COUNT(*) FROM duplicate_relations WHERE status='confirmed'").fetchone()[0]),
                "calibration_ready": calibration_ready(db_path=args.db),
            }
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
