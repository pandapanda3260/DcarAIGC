"""Database-native incremental v8 evaluation and append-only manual review."""

from __future__ import annotations

import hashlib
import importlib
import json
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from .storage import DEFAULT_DB, PROJECT_ROOT, connect, now_utc, transaction


RULE_VERSION = "evaluation-v6"
EVIDENCE_VERSION = "evidence-v1"
INCLUDE_MIN = 75
REVIEW_MIN = 60


class EvaluationError(RuntimeError):
    pass


@dataclass(frozen=True)
class EvaluationResult:
    evaluation_id: int
    evidence_envelope_id: int
    content_id: int
    evidence_sha256: str
    evidence_level: str
    created: bool


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _resolved_path(local_path: str) -> Path:
    path = Path(local_path)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _read_json(path: Optional[Path]) -> Dict[str, Any]:
    if path is None or not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _chinese_count(text: str) -> int:
    return len(re.findall(r"[\u4e00-\u9fff]", text))


def _artifact(
    connection: sqlite3.Connection,
    content_id: int,
    artifact_types: Sequence[str],
) -> Optional[sqlite3.Row]:
    placeholders = ",".join("?" for _ in artifact_types)
    return connection.execute(
        f"""
        SELECT * FROM evidence_artifacts
        WHERE content_id=? AND artifact_type IN ({placeholders}) AND status='available'
          AND sha256 IS NOT NULL
        ORDER BY id DESC LIMIT 1
        """,
        (content_id, *artifact_types),
    ).fetchone()


def _artifact_components(connection: sqlite3.Connection, content_id: int) -> Dict[str, Any]:
    detail = connection.execute(
        """
        SELECT sha256 FROM provider_raw_responses
        WHERE content_id=? ORDER BY captured_at DESC, id DESC LIMIT 1
        """,
        (content_id,),
    ).fetchone()
    if detail is None:
        detail = _artifact(connection, content_id, ("provider_content", "public_content"))
    media = _artifact(connection, content_id, ("media", "media_manifest"))
    asr = _artifact(connection, content_id, ("asr", "transcript", "media_transcript"))
    ocr = _artifact(connection, content_id, ("ocr", "media_ocr"))
    comments = connection.execute(
        """
        SELECT sha256 FROM comment_evidence_versions
        WHERE content_id=? ORDER BY captured_at DESC, id DESC LIMIT 1
        """,
        (content_id,),
    ).fetchone()
    manual_rows = connection.execute(
        """
        SELECT evidence_type, text_value, local_path, sha256
        FROM manual_evidence WHERE content_id=? ORDER BY id
        """,
        (content_id,),
    ).fetchall()
    manual_payload = [dict(row) for row in manual_rows]
    return {
        "detail_raw_sha256": str(detail["sha256"]) if detail is not None else None,
        "media_sha256": str(media["sha256"]) if media is not None else None,
        "asr_sha256": str(asr["sha256"]) if asr is not None else None,
        "ocr_sha256": str(ocr["sha256"]) if ocr is not None else None,
        "comments_version_sha256": str(comments["sha256"]) if comments is not None else None,
        "manual_evidence_sha256": sha256_json(manual_payload) if manual_payload else None,
        "media_path": _resolved_path(str(media["local_path"])) if media is not None else None,
        "asr_path": _resolved_path(str(asr["local_path"])) if asr is not None else None,
        "ocr_path": _resolved_path(str(ocr["local_path"])) if ocr is not None else None,
        "manual_rows": manual_payload,
    }


def build_evidence_envelope(
    connection: sqlite3.Connection, content_id: int
) -> tuple[int, str, Dict[str, Any]]:
    content = connection.execute(
        "SELECT * FROM content_items WHERE id=?", (content_id,)
    ).fetchone()
    if content is None:
        raise EvaluationError(f"content {content_id} does not exist")
    artifacts = _artifact_components(connection, content_id)
    text_sha = sha256_json(
        {
            "title": content["title"],
            "body": content["body"],
            "account_uid": content["raw_account_uid"],
            "account_name": content["raw_account_name"],
        }
    )
    components = {
        "detail_raw_sha256": artifacts["detail_raw_sha256"],
        "text_sha256": text_sha,
        "media_sha256": artifacts["media_sha256"],
        "asr_sha256": artifacts["asr_sha256"],
        "ocr_sha256": artifacts["ocr_sha256"],
        "comments_version_sha256": artifacts["comments_version_sha256"],
        "manual_evidence_sha256": artifacts["manual_evidence_sha256"],
    }
    evidence_sha = sha256_json(components)
    connection.execute(
        """
        INSERT INTO evidence_envelopes(
            content_id, schema_version, detail_raw_sha256, text_sha256,
            media_sha256, asr_sha256, ocr_sha256, comments_version_sha256,
            manual_evidence_sha256, evidence_sha256, components_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(content_id, evidence_sha256) DO NOTHING
        """,
        (
            content_id, EVIDENCE_VERSION, components["detail_raw_sha256"], text_sha,
            components["media_sha256"], components["asr_sha256"], components["ocr_sha256"],
            components["comments_version_sha256"], components["manual_evidence_sha256"],
            evidence_sha, canonical_json(components), now_utc(),
        ),
    )
    row = connection.execute(
        "SELECT id FROM evidence_envelopes WHERE content_id=? AND evidence_sha256=?",
        (content_id, evidence_sha),
    ).fetchone()
    if row is None:
        raise RuntimeError("evidence envelope upsert returned no row")
    return int(row["id"]), evidence_sha, artifacts


def _media_available(content_type: str, media_path: Optional[Path]) -> bool:
    if media_path is None or not media_path.is_file():
        return False
    if media_path.suffix.lower() == ".json":
        value = _read_json(media_path)
        if content_type == "video":
            path = Path(str(value.get("video_path") or ""))
            return bool(value.get("status") in {"complete", "partial"} and path.is_file())
        return bool(value.get("status") in {"complete", "partial"} and value.get("image_paths"))
    return media_path.stat().st_size > 1024


def _evidence_level(
    *,
    content_type: str,
    text: str,
    media_path: Optional[Path],
    asr: Mapping[str, Any],
    ocr: Mapping[str, Any],
    manual_rows: Sequence[Mapping[str, Any]],
) -> tuple[str, str]:
    manual_visual = any(
        row.get("evidence_type") in {"visual_summary", "media_observation"}
        and _chinese_count(str(row.get("text_value") or "")) >= 8
        for row in manual_rows
    )
    media = _media_available(content_type, media_path)
    asr_text = str(asr.get("text") or "")
    ocr_text = str(ocr.get("combined_text") or "")
    asr_ok = asr.get("status") == "success" and _chinese_count(asr_text) >= 15
    ocr_count = int(ocr.get("ocr_observation_count") or ocr.get("source_count") or 0)
    ocr_ok = ocr.get("status") == "success" and _chinese_count(ocr_text) >= 15 and ocr_count > 0
    if content_type == "video" and media and asr_ok and ocr_ok:
        return "V3", "完整视频、固定模型 ASR 和连续关键帧 OCR 共同覆盖"
    if media and (asr_ok or ocr_ok or manual_visual):
        return "V2", "媒体与 ASR、OCR 或人工画面证据可覆盖主叙事"
    if content_type != "video" and ocr_ok:
        return "V2", "图文 OCR 覆盖可用媒体证据"
    if manual_visual:
        return "V2", "人工画面证据覆盖主叙事"
    if text.strip():
        return "V1", "只有标题、正文或话题，完整媒体证据不足"
    return "V0", "内容主体和有效文字均不可用"


def _current_taxonomy(connection: sqlite3.Connection) -> tuple[str, Dict[str, Dict[str, Any]]]:
    taxonomy = connection.execute(
        """
        SELECT * FROM taxonomy_versions WHERE status='published'
        ORDER BY published_at DESC, created_at DESC LIMIT 1
        """
    ).fetchone()
    if taxonomy is None:
        raise EvaluationError("no published selling-point taxonomy")
    rows = connection.execute(
        "SELECT * FROM selling_points WHERE taxonomy_id=? AND enabled=1",
        (taxonomy["id"],),
    ).fetchall()
    return str(taxonomy["version"]), {str(row["code"]): dict(row) for row in rows}


def _direction_for(code: Optional[str], text: str, fallback: str) -> str:
    if code and code.startswith("E"):
        return "used_car"
    if code and code.startswith("X"):
        return "new_car"
    if code and code.startswith("M"):
        return "media"
    if code == "C1":
        return "used_car" if "二手" in text else "media"
    if code == "C2":
        return "used_car" if "二手" in text else "new_car"
    if code == "C3":
        return "media"
    if code == "C4":
        return "new_car"
    return fallback if fallback in {"new_car", "used_car", "media", "other"} else "unknown"


AUTO_TERMS = (
    "汽车", "车型", "新车", "二手车", "车价", "买车", "卖车", "用车", "试驾",
    "驾驶", "保养", "维修", "发动机", "变速箱", "轮胎", "刹车", "续航",
    "油耗", "配置", "车主", "底盘", "车况", "懂车帝", "AI小懂",
)


def _automotive_score(text: str, *, selling_included: bool) -> int:
    count = sum(1 for term in AUTO_TERMS if term.lower() in text.lower())
    if selling_included:
        return max(88, min(100, 76 + count * 3))
    if count >= 7:
        return 95
    if count >= 4:
        return 86
    if count >= 2:
        return 74
    if count == 1:
        return 48
    return 8


def _match_points(
    *,
    content: sqlite3.Row,
    asr: Dict[str, Any],
    ocr: Dict[str, Any],
    evidence_level: str,
    taxonomy: Mapping[str, Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    labeler = importlib.import_module("label_douyin_video_evidence_v3")
    row = {
        "desc": "\n".join(value for value in (content["title"], content["body"]) if value),
        "content_type": content["content_type"],
        "media_type": 4 if content["content_type"] == "video" else 2,
    }
    matches = list(labeler.match_points(row, asr, ocr, evidence_level, {}))
    text = f"{row['desc']}\n{asr.get('text') or ''}\n{ocr.get('combined_text') or ''}"
    existing = {str(item["id"]) for item in matches}
    for code, point in taxonomy.items():
        if code in existing:
            continue
        try:
            positive = json.loads(str(point.get("positive_evidence_json") or "[]"))
        except json.JSONDecodeError:
            positive = []
        terms = [str(term) for term in positive if str(term).strip()]
        hits = [term for term in terms if term.lower() in text.lower()]
        if hits:
            score = min(90, 60 + 10 * len(hits))
            matches.append(
                {
                    "id": code,
                    "score": score,
                    "reason": "命中卖点标准中的正向证据词",
                    "source": "数据库词表",
                    "evidence_snippet": "、".join(hits[:5]),
                    "dimensions": {"taxonomy_evidence": score},
                }
            )
    return sorted(matches, key=lambda item: (-int(item.get("score") or 0), str(item.get("id") or "")))


def _comment_scores(
    connection: sqlite3.Connection, content_id: int
) -> tuple[Optional[int], Optional[int], int]:
    rows = connection.execute(
        """
        SELECT audience_automotive_score, action_intent_score
        FROM comment_user_scores WHERE content_id=?
        """,
        (content_id,),
    ).fetchall()
    if len(rows) < 20:
        return None, None, len(rows)
    return (
        round(mean(int(row["audience_automotive_score"]) for row in rows)),
        round(mean(int(row["action_intent_score"]) for row in rows)),
        len(rows),
    )


def upsert_comment_user_scores(
    content_id: int,
    evidence_version_id: int,
    rows: Iterable[Mapping[str, Any]],
    *,
    db_path: Path = DEFAULT_DB,
) -> int:
    values = list(rows)
    evaluated_at = now_utc()
    with connect(db_path) as connection, transaction(connection):
        for row in values:
            connection.execute(
                """
                INSERT INTO comment_user_scores(
                    content_id, evidence_version_id, anonymous_user_key,
                    audience_automotive_score, action_intent_score, evaluated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(content_id, anonymous_user_key) DO UPDATE SET
                    evidence_version_id=excluded.evidence_version_id,
                    audience_automotive_score=excluded.audience_automotive_score,
                    action_intent_score=excluded.action_intent_score,
                    evaluated_at=excluded.evaluated_at
                """,
                (
                    content_id, evidence_version_id, row["anonymous_user_key"],
                    int(row["audience_automotive_score"]), int(row["action_intent_score"]),
                    evaluated_at,
                ),
            )
    return len(values)


def _acquisition_score(
    content_score: Optional[int],
    audience_score: Optional[int],
    selling_score: Optional[int],
    action_score: Optional[int],
) -> Optional[int]:
    if content_score is None:
        return None
    scorer = importlib.import_module("three_proposition_scoring")
    value = scorer.dcd_acquisition_score(
        content_score=content_score,
        audience_score=audience_score,
        dcd_fit_score=selling_score,
        action_intent_score=action_score,
    )
    return int(value) if value is not None else None


def evaluate_content(
    content_id: int,
    *,
    db_path: Path = DEFAULT_DB,
    source: str = "automatic",
    manual_override: Optional[Mapping[str, Any]] = None,
) -> EvaluationResult:
    if source not in {"automatic", "manual_review"}:
        raise EvaluationError(f"unsupported evaluation source: {source}")
    with connect(db_path) as connection, transaction(connection):
        content = connection.execute(
            "SELECT * FROM content_items WHERE id=?", (content_id,)
        ).fetchone()
        if content is None:
            raise EvaluationError(f"content {content_id} does not exist")
        envelope_id, evidence_sha, artifacts = build_evidence_envelope(connection, content_id)
        taxonomy_version, taxonomy = _current_taxonomy(connection)
        existing = connection.execute(
            """
            SELECT * FROM evaluation_versions
            WHERE content_id=? AND rule_version=? AND taxonomy_version=? AND evidence_sha256=?
            """,
            (content_id, RULE_VERSION, taxonomy_version, evidence_sha),
        ).fetchone()
        if existing is not None:
            return EvaluationResult(
                evaluation_id=int(existing["id"]),
                evidence_envelope_id=envelope_id,
                content_id=content_id,
                evidence_sha256=evidence_sha,
                evidence_level=str(existing["evidence_level"]),
                created=False,
            )

        asr = _read_json(artifacts["asr_path"])
        ocr = _read_json(artifacts["ocr_path"])
        manual_rows = artifacts["manual_rows"]
        manual_text = "\n".join(str(row.get("text_value") or "") for row in manual_rows)
        body_text = "\n".join(
            value for value in (str(content["title"] or ""), str(content["body"] or ""), manual_text)
            if value
        )
        evidence_level, evidence_summary = _evidence_level(
            content_type=str(content["content_type"]),
            text=body_text,
            media_path=artifacts["media_path"],
            asr=asr,
            ocr=ocr,
            manual_rows=manual_rows,
        )
        matches = _match_points(
            content=content,
            asr=asr,
            ocr=ocr,
            evidence_level=evidence_level,
            taxonomy=taxonomy,
        ) if evidence_level in {"V2", "V3"} else []
        primary = matches[0] if matches else None
        primary_code = str(primary["id"]) if primary else None
        selling_score = int(primary["score"]) if primary else None
        included = bool(primary and selling_score is not None and selling_score >= INCLUDE_MIN)
        pending_review = bool(
            evidence_level in {"V0", "V1"}
            or (selling_score is not None and REVIEW_MIN <= selling_score < INCLUDE_MIN)
        )
        direction = _direction_for(
            primary_code,
            f"{body_text}\n{asr.get('text') or ''}\n{ocr.get('combined_text') or ''}",
            str(content["evaluation_content_direction"] or "unknown"),
        )
        content_score = (
            _automotive_score(
                f"{body_text}\n{asr.get('text') or ''}\n{ocr.get('combined_text') or ''}",
                selling_included=included,
            )
            if evidence_level in {"V2", "V3"}
            else None
        )

        override = dict(manual_override or {})
        decision = str(override.get("decision") or "")
        if decision == "terminal_unavailable":
            evidence_level, evidence_summary = "V0", "人工确认内容不可用"
            primary_code, selling_score, included, content_score = None, None, False, None
            pending_review = False
            matches = []
        elif decision == "insufficient_evidence":
            evidence_level, evidence_summary = "V1", "人工确认现有证据不足"
            primary_code, selling_score, included, content_score = None, None, False, None
            pending_review = False
            matches = []
        elif decision == "override":
            primary_code = override.get("primary_selling_point_code") or None
            if primary_code is not None and primary_code not in taxonomy:
                raise EvaluationError(f"unknown selling point: {primary_code}")
            selling_score = int(override.get("selling_point_score") or (90 if primary_code else 0))
            included = bool(override.get("selling_point_included", bool(primary_code)))
            if override.get("content_automotive_score") is not None:
                content_score = int(override["content_automotive_score"])
            if override.get("content_direction") is not None:
                direction = str(override["content_direction"])
            pending_review = False
        elif decision == "confirm":
            pending_review = False
            matches = (
                [{"id": primary_code, "score": selling_score, "reason": "人工复核覆盖", "source": "manual"}]
                if primary_code else []
            )
        if direction not in {"new_car", "used_car", "media", "other", "unknown"}:
            raise EvaluationError(f"invalid content direction: {direction}")
        if content_score is not None and not 0 <= content_score <= 100:
            raise EvaluationError("content automotive score must be 0..100")
        if selling_score is not None and not 0 <= selling_score <= 100:
            raise EvaluationError("selling point score must be 0..100")

        audience_score, action_score, valid_commenters = _comment_scores(connection, content_id)
        task_fit = selling_score if included else 0 if content_score is not None else None
        acquisition_score = _acquisition_score(
            content_score, audience_score, task_fit, action_score
        )
        payload = {
            "evaluation_status": "evaluated" if evidence_level in {"V2", "V3"} else "insufficient_evidence",
            "evidence_level": evidence_level,
            "evidence_summary": evidence_summary,
            "primary_selling_point_id": primary_code or "",
            "selling_point_score": selling_score,
            "selling_point_included": included,
            "pending_review": pending_review,
            "content_direction": direction,
            "content_automotive_score": content_score,
            "audience_automotive_score": audience_score,
            "action_intent_score": action_score,
            "valid_unique_commenters": valid_commenters,
            "acquisition_potential": acquisition_score,
            "matches": matches[:3],
            "evaluation_source": source,
        }
        cursor = connection.execute(
            """
            INSERT INTO evaluation_versions(
                content_id, evidence_envelope_id, rule_version, taxonomy_version,
                evidence_sha256, evaluation_source, evaluation_status, evidence_level,
                primary_selling_point_code, selling_point_score, selling_point_included,
                content_direction, content_automotive_score, audience_automotive_score,
                acquisition_potential_score, pending_review, payload_json, evaluated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                content_id, envelope_id, RULE_VERSION, taxonomy_version, evidence_sha,
                source, payload["evaluation_status"], evidence_level, primary_code,
                selling_score, int(included), direction, content_score, audience_score,
                acquisition_score, int(pending_review), canonical_json(payload), now_utc(),
            ),
        )
        if cursor.lastrowid is None:
            raise RuntimeError("evaluation insert returned no id")
        evaluation_id = int(cursor.lastrowid)
        for index, match in enumerate(matches[:3]):
            code = str(match.get("id") or "")
            if not code or code not in taxonomy:
                continue
            connection.execute(
                """
                INSERT INTO evaluation_matches(
                    evaluation_id, selling_point_code, match_role, score, evidence_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    evaluation_id, code, "primary" if index == 0 else "secondary",
                    int(match.get("score") or 0), canonical_json(match),
                ),
            )
        connection.execute(
            "UPDATE content_items SET evaluation_content_direction=? WHERE id=?",
            (direction, content_id),
        )
    return EvaluationResult(
        evaluation_id=evaluation_id,
        evidence_envelope_id=envelope_id,
        content_id=content_id,
        evidence_sha256=evidence_sha,
        evidence_level=evidence_level,
        created=True,
    )


def incremental_candidates(*, db_path: Path = DEFAULT_DB) -> List[int]:
    with connect(db_path) as connection:
        taxonomy_version, _ = _current_taxonomy(connection)
        rows = connection.execute(
            """
            SELECT c.id
            FROM content_items c
            LEFT JOIN evidence_envelopes ee ON ee.id=(
                SELECT ee2.id FROM evidence_envelopes ee2
                WHERE ee2.content_id=c.id ORDER BY ee2.created_at DESC, ee2.id DESC LIMIT 1
            )
            LEFT JOIN evaluation_versions ev ON ev.id=(
                SELECT ev2.id FROM evaluation_versions ev2
                WHERE ev2.content_id=c.id ORDER BY ev2.evaluated_at DESC, ev2.id DESC LIMIT 1
            )
            WHERE ee.id IS NULL OR ev.id IS NULL OR ev.evidence_envelope_id<>ee.id
               OR ev.taxonomy_version<>?
               OR c.updated_at>ee.created_at
               OR EXISTS (
                    SELECT 1 FROM evidence_artifacts ea
                    WHERE ea.content_id=c.id AND ea.created_at>ee.created_at
               )
               OR EXISTS (
                    SELECT 1 FROM comment_evidence_versions cev
                    WHERE cev.content_id=c.id AND cev.created_at>ee.created_at
               )
               OR EXISTS (
                    SELECT 1 FROM manual_evidence me
                    WHERE me.content_id=c.id AND me.created_at>ee.created_at
               )
            ORDER BY c.id
            """,
            (taxonomy_version,),
        ).fetchall()
    return [int(row["id"]) for row in rows]


def evaluate_incremental(*, db_path: Path = DEFAULT_DB, limit: Optional[int] = None) -> Dict[str, Any]:
    candidates = incremental_candidates(db_path=db_path)
    if limit is not None:
        candidates = candidates[:limit]
    created = 0
    reused = 0
    results: List[Dict[str, Any]] = []
    for content_id in candidates:
        result = evaluate_content(content_id, db_path=db_path)
        created += int(result.created)
        reused += int(not result.created)
        results.append(
            {
                "content_id": content_id,
                "evaluation_id": result.evaluation_id,
                "evidence_level": result.evidence_level,
                "created": result.created,
            }
        )
    return {
        "candidates": len(candidates),
        "created": created,
        "reused": reused,
        "results": results,
    }


def resolve_review(
    queue_id: int,
    *,
    decision: str,
    reason: str,
    reviewer: str,
    evidence_type: str,
    evidence_text: str,
    overrides: Optional[Mapping[str, Any]] = None,
    db_path: Path = DEFAULT_DB,
) -> EvaluationResult:
    allowed = {"confirm", "override", "insufficient_evidence", "terminal_unavailable"}
    if decision not in allowed:
        raise EvaluationError(f"invalid review decision: {decision}")
    if not reason.strip() or not reviewer.strip() or not evidence_text.strip():
        raise EvaluationError("reason, reviewer and manual evidence are required")
    override_values = dict(overrides or {})
    if decision == "override":
        score = override_values.get("selling_point_score")
        verticality = override_values.get("content_automotive_score")
        direction = override_values.get("content_direction")
        if score is not None and not 0 <= int(score) <= 100:
            raise EvaluationError("selling point score must be 0..100")
        if verticality is not None and not 0 <= int(verticality) <= 100:
            raise EvaluationError("content automotive score must be 0..100")
        if direction is not None and direction not in {
            "new_car", "used_car", "media", "other", "unknown",
        }:
            raise EvaluationError("invalid content direction")
    with connect(db_path) as connection, transaction(connection):
        queue = connection.execute(
            "SELECT * FROM review_queue WHERE id=?", (queue_id,)
        ).fetchone()
        if queue is None:
            raise EvaluationError(f"review queue {queue_id} does not exist")
        if queue["status"] in {"resolved", "terminal_failed"}:
            raise EvaluationError(f"review queue {queue_id} is already {queue['status']}")
        previous = connection.execute(
            """
            SELECT id FROM evaluation_versions WHERE content_id=?
            ORDER BY evaluated_at DESC, id DESC LIMIT 1
            """,
            (queue["content_id"],),
        ).fetchone()
        review_cursor = connection.execute(
            """
            INSERT INTO evaluation_reviews(
                queue_id, content_id, previous_evaluation_id, decision,
                reason, reviewer, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                queue_id, queue["content_id"], previous["id"] if previous else None,
                decision, reason.strip(), reviewer.strip(), now_utc(),
            ),
        )
        if review_cursor.lastrowid is None:
            raise RuntimeError("review insert returned no id")
        review_id = int(review_cursor.lastrowid)
        evidence_sha = hashlib.sha256(evidence_text.strip().encode("utf-8")).hexdigest()
        connection.execute(
            """
            INSERT INTO manual_evidence(
                review_id, content_id, evidence_type, text_value, sha256, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                review_id, queue["content_id"], evidence_type,
                evidence_text.strip(), evidence_sha, now_utc(),
            ),
        )
    manual_override = {"decision": decision, **override_values}
    try:
        result = evaluate_content(
            int(queue["content_id"]),
            db_path=db_path,
            source="manual_review",
            manual_override=manual_override,
        )
    except Exception:
        with connect(db_path) as connection, transaction(connection):
            connection.execute("DELETE FROM evaluation_reviews WHERE id=?", (review_id,))
        raise
    with connect(db_path) as connection, transaction(connection):
        next_status = "terminal_failed" if decision == "terminal_unavailable" else "resolved"
        connection.execute(
            """
            UPDATE evaluation_reviews SET resulting_evaluation_id=? WHERE id=?
            """,
            (result.evaluation_id, review_id),
        )
        connection.execute(
            """
            UPDATE review_queue
            SET evaluation_id=?, status=?, resolved_at=?, updated_at=? WHERE id=?
            """,
            (result.evaluation_id, next_status, now_utc(), now_utc(), queue_id),
        )
    return result
