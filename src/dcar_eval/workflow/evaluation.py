"""Unified v5 row evaluation from cached full-publication evidence."""

from __future__ import annotations

import csv
import json
import re
import sqlite3
from collections import Counter
from pathlib import Path
from statistics import mean
from typing import Any

import analyze_douyin_tikhub_v6 as douyin_v6
import label_douyin_video_evidence_v3 as labeler
import rebuild_channel_evaluation_v4 as v4
from collect_rnote_pilot import collection_status
from three_proposition_scoring import (
    acquisition_conclusion,
    audience_conclusion,
    content_conclusion,
    dcd_acquisition_score,
)

from .contracts import PROJECT_ROOT
from .privacy import CommentHasher
from .storage import now_iso, transaction


RULE_VERSION = "dcar-evaluation-v5.0"
TAXONOMY_PATH = PROJECT_ROOT / "config/business_selling_points_v4_final.json"
DOUYIN_ROWS = PROJECT_ROOT / "reports/current/抖音438条内容渠道评估_v6_TikHub补充_2026-08-02.csv"
RNOTE_NOTES = PROJECT_ROOT / "data/cache/rnote/notes"
RNOTE_MEDIA = PROJECT_ROOT / "data/cache/rnote/media"


def _read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    output = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            value = json.loads(line)
            if isinstance(value, dict):
                output.append(value)
    return output


def _bool(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _int(value: Any) -> int | None:
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return None


def _qualitative_selling(score: int, included: bool, pending: bool) -> str:
    return labeler.qualitative(score, included, pending)


def classify_xhs_evidence(
    content: dict[str, Any],
    manifest: dict[str, Any],
    transcript: dict[str, Any],
    ocr: dict[str, Any],
) -> tuple[str, str, bool]:
    video_expected = str(content.get("note_type") or "").lower() == "video"
    video_available = bool(manifest.get("video_expected") and manifest.get("video_path"))
    media_complete = manifest.get("status") == "complete" and (
        not video_expected or video_available
    )
    transcript_ok = transcript.get("status") == "success" and len(str(transcript.get("text") or "")) >= 15
    ocr_complete = ocr.get("status") == "success" and int(ocr.get("source_count") or 0) > 0
    video_ocr_ok = ocr_complete and len(re.sub(r"\s+", "", str(ocr.get("combined_text") or ""))) >= 15
    if video_expected and media_complete and transcript_ok and video_ocr_ok:
        return "V3", "完整视频、ASR和连续关键帧OCR可用", True
    if video_expected and media_complete and (transcript_ok or video_ocr_ok):
        return "V2", "完整视频及ASR或连续关键帧OCR可覆盖主叙事", True
    if not video_expected and media_complete and ocr_complete:
        return "V2", "全部原图已下载并完成OCR扫描", True
    text = f"{content.get('title') or ''}\n{content.get('desc') or ''}".strip()
    if text:
        return "V1", "只有标题、正文或话题，完整媒体证据不足", False
    return "V0", "内容主体和有效文字均不可用", False


def _xhs_evidence(note_id: str, content: dict[str, Any]) -> tuple[str, str, dict[str, Any], dict[str, Any], bool]:
    media_root = RNOTE_MEDIA / note_id
    manifest = _read_json(media_root / "manifest.json", {}) or {}
    transcript = _read_json(media_root / "transcript.json", {}) or {}
    ocr = _read_json(media_root / "ocr.json", {}) or {}
    evidence, note, media_exists = classify_xhs_evidence(content, manifest, transcript, ocr)
    return evidence, note, transcript, ocr, media_exists


def _automotive_score(text: str, *, selling_included: bool) -> int:
    normalized = labeler.canonical(text)
    term_count = v4.unique_term_count(normalized, v4.AUTO_TERMS)
    if selling_included:
        return max(88, min(100, 76 + term_count * 3))
    if term_count >= 7:
        return 95
    if term_count >= 4:
        return 86
    if term_count >= 2:
        return 74
    if term_count == 1:
        return 48
    return 8


def _comment_users(
    platform: str,
    content_id: str,
    records: list[dict[str, Any]],
    hasher: CommentHasher,
) -> dict[str, str]:
    values: dict[str, list[str]] = {}
    for row in records:
        if platform == "xiaohongshu" and not row.get("valid_for_audience"):
            continue
        cached_key = str(row.get("user_hash") or row.get("user_key") or "")
        text = " ".join(str(row.get("text") or "").split())
        if not cached_key or not text:
            continue
        key = hasher.user_key(platform, content_id, cached_key)
        texts = values.setdefault(key, [])
        if text not in texts and len(texts) < 3:
            texts.append(text)
    return {key: "；".join(texts) for key, texts in values.items() if texts}


def _score_comments(
    users: dict[str, str],
    *,
    content_automotive_score: int | None,
) -> tuple[int | None, int | None, list[dict[str, Any]]]:
    context = content_automotive_score is not None and content_automotive_score >= 70
    rows = [
        {
            "anonymous_user_key": key,
            "audience_automotive_score": douyin_v6.audience_user_score(text, context_automotive=context),
            "action_intent_score": douyin_v6.action_user_score(text, context_automotive=context),
        }
        for key, text in users.items()
    ]
    if len(rows) < 20:
        return None, None, rows
    return (
        round(mean(row["audience_automotive_score"] for row in rows)),
        round(mean(row["action_intent_score"] for row in rows)),
        rows,
    )


def _douyin_evaluations(connection: sqlite3.Connection, hasher: CommentHasher) -> list[tuple[int, dict[str, Any], list[dict[str, Any]]]]:
    with DOUYIN_ROWS.open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    item_ids = {
        row["platform_content_id"]: int(row["id"])
        for row in connection.execute("SELECT id, platform_content_id FROM content_items WHERE platform='douyin'")
    }
    taxonomy = json.loads(TAXONOMY_PATH.read_text(encoding="utf-8"))
    label_map = {item["id"]: item for item in taxonomy["labels"]}
    output = []
    for source in rows:
        content_id = str(source["aweme_id"])
        included = _bool(source.get("included"))
        primary_id = str(source.get("primary_id") or "")
        selling_score = _int(source.get("score")) or 0
        content_score = _int(source.get("content_auto_score_v6"))
        audience_score = _int(source.get("audience_auto_score"))
        action_score = _int(source.get("action_intent_score"))
        task_fit = _int(source.get("dcd_task_fit_score"))
        comment_pages = [
            _read_json(path, {}) or {}
            for path in sorted((PROJECT_ROOT / f"data/cache/tikhub/2026-08-02/comments/{content_id}").glob("page_*.json"))
        ]
        legacy_users = douyin_v6.valid_unique_comments(comment_pages)
        users = {
            hasher.user_key("douyin", content_id, key): text
            for key, text in legacy_users.items()
        }
        _, _, user_rows = _score_comments(users, content_automotive_score=content_score)
        potential = dcd_acquisition_score(
            content_score=content_score or 0,
            audience_score=audience_score,
            dcd_fit_score=task_fit,
            action_intent_score=action_score,
        ) if content_score is not None else None
        meta = label_map.get(primary_id, {})
        output.append((item_ids[content_id], {
            "evaluation_status": "evaluated" if source.get("evidence_level") in {"V2", "V3"} else "insufficient_evidence",
            "evidence_level": str(source.get("evidence_level") or "V0"),
            "evidence_summary": "抖音完整媒体、ASR与OCR历史终版证据",
            "primary_selling_point_id": primary_id,
            "primary_selling_point_label": str(source.get("primary_label") or meta.get("label") or ""),
            "primary_tier": str(source.get("primary_tier") or meta.get("tier") or ""),
            "business_scene": str(source.get("business_scene") or meta.get("business_scene") or ""),
            "selling_point_score": selling_score,
            "selling_point_qualitative": str(source.get("qualitative") or ""),
            "selling_point_included": included,
            "pending_review": False,
            "secondary_selling_point_ids_json": "[]",
            "no_match_reason": "" if included else "未达到正式卖点命中条件",
            "content_automotive_score": content_score,
            "content_automotive_qualitative": content_conclusion(content_score) if content_score is not None else "暂不可计算",
            "valid_unique_commenters": len(users),
            "comment_sample_status": "scorable" if len(users) >= 20 else "below_minimum",
            "audience_automotive_score": audience_score,
            "audience_automotive_qualitative": audience_conclusion(audience_score) or "暂不可计算",
            "dcar_task_fit_score": task_fit,
            "action_intent_score": action_score,
            "acquisition_potential": potential,
            "acquisition_potential_qualitative": acquisition_conclusion(potential) or "暂不可计算",
        }, user_rows))
    return output


def _xhs_evaluations(connection: sqlite3.Connection, hasher: CommentHasher) -> list[tuple[int, dict[str, Any], list[dict[str, Any]]]]:
    taxonomy = json.loads(TAXONOMY_PATH.read_text(encoding="utf-8"))
    label_map = {item["id"]: item for item in taxonomy["labels"]}
    output = []
    items = connection.execute(
        "SELECT id, platform_content_id FROM content_items WHERE platform='xiaohongshu' ORDER BY platform_content_id"
    ).fetchall()
    for item in items:
        item_id = int(item["id"])
        note_id = str(item["platform_content_id"])
        content = _read_json(RNOTE_NOTES / note_id / "content.json", {}) or {}
        metadata = _read_json(RNOTE_NOTES / note_id / "collection.json", {}) or {}
        comments = _read_jsonl(RNOTE_NOTES / note_id / "comments.jsonl")
        evidence, evidence_note, transcript, ocr, media_exists = _xhs_evidence(note_id, content)
        desc = "\n".join(value for value in (str(content.get("title") or ""), str(content.get("desc") or "")) if value)
        row = {
            "aweme_id": note_id,
            "desc": desc,
            "content_type": "image_text" if not (content.get("video_urls") or []) else "video",
            "media_type": 2 if not (content.get("video_urls") or []) else 4,
        }
        matches = labeler.match_points(row, transcript, ocr, evidence, {}) if evidence in {"V2", "V3"} else []
        primary = matches[0] if matches else None
        selling_score = int(primary["score"]) if primary else 0
        included = bool(primary and selling_score >= 75 and evidence in {"V2", "V3"})
        pending = bool(primary and 60 <= selling_score < 75)
        secondary = [match["id"] for match in matches[1:] if match["score"] >= 60][:2]
        primary_id = str(primary["id"] if primary else "")
        meta = label_map.get(primary_id, {})
        text = f"{desc}\n{transcript.get('text') or ''}\n{ocr.get('combined_text') or ''}"
        no_match = "" if included or pending else labeler.no_match_reason(labeler.canonical(text))[1]
        if evidence in {"V2", "V3"}:
            content_score = _automotive_score(text, selling_included=included)
        else:
            content_score = None
        users = _comment_users("xiaohongshu", note_id, comments, hasher)
        audience_score, action_score, user_rows = _score_comments(
            users, content_automotive_score=content_score
        )
        if content_score is not None and audience_score is not None:
            adjustment = max(-5.0, min(5.0, 0.10 * (audience_score - content_score)))
            content_score = round(max(0, min(100, content_score + adjustment)))
        status = collection_status(content if content else None, metadata)
        comment_status = str(status.get("comment_sample_status") or "technical_missing")
        if len(users) >= 20 and comment_status != "technical_missing":
            comment_status = "scorable"
        else:
            audience_score = None
            action_score = None
        task_fit = selling_score if included else 0 if evidence in {"V2", "V3"} else None
        potential = (
            dcd_acquisition_score(
                content_score=content_score,
                audience_score=audience_score,
                dcd_fit_score=task_fit,
                action_intent_score=action_score,
            )
            if content_score is not None
            else None
        )
        scene_row = {
            "primary_id": primary_id,
            "desc": desc,
            "asr_text": transcript.get("text") or "",
            "ocr_text": ocr.get("combined_text") or "",
            "visual_review_summary": "",
        }
        output.append((item_id, {
            "evaluation_status": "evaluated" if evidence in {"V2", "V3"} else "insufficient_evidence" if content else "failed",
            "evidence_level": evidence,
            "evidence_summary": evidence_note,
            "primary_selling_point_id": primary_id,
            "primary_selling_point_label": str(meta.get("label") or ""),
            "primary_tier": str(meta.get("tier") or ""),
            "business_scene": v4.scene_for_label(scene_row),
            "selling_point_score": selling_score if evidence in {"V2", "V3"} else None,
            "selling_point_qualitative": _qualitative_selling(selling_score, included, pending) if evidence in {"V2", "V3"} else "暂不可计算",
            "selling_point_included": included,
            "pending_review": pending or evidence in {"V0", "V1"},
            "secondary_selling_point_ids_json": json.dumps(secondary, ensure_ascii=False),
            "no_match_reason": no_match,
            "content_automotive_score": content_score,
            "content_automotive_qualitative": content_conclusion(content_score) if content_score is not None else "暂不可计算",
            "valid_unique_commenters": len(users) if comment_status != "technical_missing" else None,
            "comment_sample_status": comment_status,
            "audience_automotive_score": audience_score,
            "audience_automotive_qualitative": audience_conclusion(audience_score) or "暂不可计算",
            "dcar_task_fit_score": task_fit,
            "action_intent_score": action_score,
            "acquisition_potential": potential,
            "acquisition_potential_qualitative": acquisition_conclusion(potential) or "暂不可计算",
        }, user_rows))
    return output


def evaluate_all(connection: sqlite3.Connection) -> dict[str, Any]:
    hasher = CommentHasher()
    taxonomy = json.loads(TAXONOMY_PATH.read_text(encoding="utf-8"))
    rows = _douyin_evaluations(connection, hasher) + _xhs_evaluations(connection, hasher)
    evaluated_at = now_iso()
    columns = [
        "evaluation_status", "evidence_level", "evidence_summary",
        "primary_selling_point_id", "primary_selling_point_label", "primary_tier",
        "business_scene", "selling_point_score", "selling_point_qualitative",
        "selling_point_included", "pending_review", "secondary_selling_point_ids_json",
        "no_match_reason", "content_automotive_score", "content_automotive_qualitative",
        "valid_unique_commenters", "comment_sample_status", "audience_automotive_score",
        "audience_automotive_qualitative", "dcar_task_fit_score", "action_intent_score",
        "acquisition_potential", "acquisition_potential_qualitative",
    ]
    with transaction(connection):
        connection.execute("DELETE FROM comment_user_scores")
        for content_item_id, evaluation, users in rows:
            values = [evaluation.get(column) for column in columns]
            connection.execute(
                f"""
                INSERT INTO evaluations(
                    content_item_id, rule_version, taxonomy_version, {', '.join(columns)}, evaluated_at
                ) VALUES (?, ?, ?, {', '.join('?' for _ in columns)}, ?)
                ON CONFLICT(content_item_id) DO UPDATE SET
                    rule_version=excluded.rule_version,
                    taxonomy_version=excluded.taxonomy_version,
                    {', '.join(f'{column}=excluded.{column}' for column in columns)},
                    evaluated_at=excluded.evaluated_at
                """,
                [content_item_id, RULE_VERSION, taxonomy["taxonomy_version"], *values, evaluated_at],
            )
            for user in users:
                connection.execute(
                    """
                    INSERT INTO comment_user_scores(
                        content_item_id, anonymous_user_key, audience_automotive_score,
                        action_intent_score, evaluated_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        content_item_id, user["anonymous_user_key"],
                        user["audience_automotive_score"], user["action_intent_score"], evaluated_at,
                    ),
                )
    summary = {}
    for platform in ("douyin", "xiaohongshu"):
        row = connection.execute(
            """
            SELECT COUNT(*) AS total,
                   SUM(e.evidence_level IN ('V2','V3')) AS identifiable,
                   SUM(e.selling_point_included) AS selling_point_covered,
                   SUM(e.primary_tier='core' AND e.selling_point_included) AS core,
                   SUM(e.audience_automotive_score IS NOT NULL) AS audience_scorable,
                   SUM(e.acquisition_potential IS NOT NULL) AS acquisition_scorable
            FROM content_items c JOIN evaluations e ON e.content_item_id=c.id
            WHERE c.platform=?
            """,
            (platform,),
        ).fetchone()
        summary[platform] = dict(row)
    return {"evaluated": len(rows), "channels": summary, "evaluated_at": evaluated_at}
