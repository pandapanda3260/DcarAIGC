"""Database-native incremental v8 evaluation and append-only manual review."""

from __future__ import annotations

import hashlib
import importlib
import json
import re
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence

from .evaluation_selectors import review_anchor_evaluation
from .matcher_dsl import MatcherDslError, MaterializedMatcher, POINT_IDS
from .storage import (
    DEFAULT_DB,
    PROJECT_ROOT,
    SchemaMigrationError,
    connect,
    ensure_legacy_evaluation_release,
    now_utc,
    transaction,
)
from .taxonomy import TaxonomyError, serialize_point_row


RULE_VERSION = "evaluation-v7"
V8_RULE_VERSION = "evaluation-v8"
EVIDENCE_VERSION = "evidence-v1"
TEXT_EVIDENCE_VERSION = "text-evidence-v2"
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


@dataclass(frozen=True)
class ReviewReopenResult:
    event_id: int
    queue_id: int
    content_id: int
    base_evaluation_id: int


@dataclass(frozen=True)
class _EvaluationRuntime:
    release: Mapping[str, Any]
    taxonomy_version: str
    taxonomy: Dict[str, Dict[str, Any]]
    allowed_scenes: Dict[str, set[str]]
    matcher: MaterializedMatcher | None


def _synchronize_gray_review_queue(
    connection: sqlite3.Connection,
    *,
    content_id: int,
    evaluation_id: int,
    release_id: str,
    evidence_level: str,
    pending_review: bool,
) -> None:
    """Keep the active release queue aligned with its latest automatic result."""

    release = connection.execute(
        "SELECT status FROM evaluation_releases WHERE id=?", (release_id,)
    ).fetchone()
    if release is None:
        raise EvaluationError(f"evaluation release does not exist: {release_id}")
    if str(release["status"]) != "active":
        return
    latest = connection.execute(
        """
        SELECT id,evaluation_source FROM evaluation_versions
        WHERE content_id=? AND release_id=? AND invalidated_at IS NULL
        ORDER BY evaluated_at DESC,id DESC LIMIT 1
        """,
        (content_id, release_id),
    ).fetchone()
    if (
        latest is None
        or int(latest["id"]) != evaluation_id
        or str(latest["evaluation_source"]) != "automatic"
    ):
        return

    queue = connection.execute(
        """
        SELECT * FROM review_queue
        WHERE content_id=? AND reason_code='evaluation_gray_zone'
        """,
        (content_id,),
    ).fetchone()
    is_gray = pending_review and evidence_level in {"V2", "V3"}
    active_statuses = {"pending", "manual_required", "in_review"}
    if not is_gray:
        if queue is None or str(queue["status"]) not in active_statuses:
            return
        captured_at = now_utc()
        connection.execute(
            """
            UPDATE review_queue
            SET evaluation_id=?,status='resolved',assigned_to='system:evaluation',
                resolved_at=?,updated_at=?
            WHERE id=? AND status IN ('pending','manual_required','in_review')
            """,
            (evaluation_id, captured_at, captured_at, queue["id"]),
        )
        return

    captured_at = now_utc()
    if queue is None:
        connection.execute(
            """
            INSERT INTO review_queue(
                content_id,evaluation_id,reason_code,status,created_at,updated_at
            ) VALUES (?,?,'evaluation_gray_zone','pending',?,?)
            """,
            (content_id, evaluation_id, captured_at, captured_at),
        )
        return
    status = str(queue["status"])
    if (
        queue["evaluation_id"] is not None
        and int(queue["evaluation_id"]) == evaluation_id
        and status in active_statuses
    ):
        return
    if status in {"resolved", "terminal_failed"}:
        previous_review = connection.execute(
            """
            SELECT id FROM evaluation_reviews
            WHERE queue_id=? AND resulting_evaluation_id IS NOT NULL
            ORDER BY id DESC LIMIT 1
            """,
            (queue["id"],),
        ).fetchone()
        connection.execute(
            """
            INSERT INTO review_reopen_events(
                queue_id,content_id,previous_review_id,base_evaluation_id,
                reopened_by,reason,created_at
            ) VALUES (?,?,?,?,?,?,?)
            """,
            (
                queue["id"],
                content_id,
                previous_review["id"] if previous_review is not None else None,
                evaluation_id,
                "system:evaluation",
                "current release automatic evaluation entered gray zone",
                captured_at,
            ),
        )
        connection.execute(
            """
            UPDATE review_queue
            SET evaluation_id=?,status='pending',assigned_to=NULL,
                resolved_at=NULL,updated_at=?
            WHERE id=?
            """,
            (evaluation_id, captured_at, queue["id"]),
        )
        return

    connection.execute(
        "UPDATE review_queue SET evaluation_id=?,updated_at=? WHERE id=?",
        (evaluation_id, captured_at, queue["id"]),
    )


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


def _gray_review_queue_sync_plan(connection: sqlite3.Connection) -> Dict[str, Any]:
    releases = connection.execute(
        "SELECT id,rule_version FROM evaluation_releases WHERE status='active'"
    ).fetchall()
    if len(releases) != 1:
        raise EvaluationError("exactly one active release is required")
    release = releases[0]
    rows = connection.execute(
        """
        WITH ranked AS (
            SELECT id,content_id,evidence_level,pending_review,evaluation_source,
                   ROW_NUMBER() OVER (
                       PARTITION BY content_id
                       ORDER BY evaluated_at DESC,id DESC
                   ) selector_rank
            FROM evaluation_versions
            WHERE release_id=? AND invalidated_at IS NULL
        )
        SELECT r.content_id,r.id evaluation_id,r.evidence_level,r.pending_review,
               q.id queue_id,q.evaluation_id queue_evaluation_id,q.status queue_status
        FROM ranked r
        LEFT JOIN review_queue q
          ON q.content_id=r.content_id AND q.reason_code='evaluation_gray_zone'
        WHERE r.selector_rank=1 AND r.evaluation_source='automatic'
          AND (
              q.id IS NOT NULL
              OR (r.pending_review=1 AND r.evidence_level IN ('V2','V3'))
          )
        ORDER BY r.content_id
        """,
        (release["id"],),
    ).fetchall()
    targets: List[Dict[str, Any]] = []
    synchronized = 0
    gray_evaluation_count = 0
    active_statuses = {"pending", "manual_required", "in_review"}
    for row in rows:
        queue_id = row["queue_id"]
        queue_evaluation_id = row["queue_evaluation_id"]
        queue_status = (
            str(row["queue_status"]) if row["queue_status"] is not None else None
        )
        is_gray = bool(row["pending_review"]) and str(row["evidence_level"]) in {
            "V2",
            "V3",
        }
        gray_evaluation_count += int(is_gray)
        if not is_gray:
            if queue_id is None or queue_status not in active_statuses:
                continue
            action = "resolve"
        elif queue_id is None:
            action = "create"
        elif (
            queue_evaluation_id is not None
            and int(queue_evaluation_id) == int(row["evaluation_id"])
            and queue_status in active_statuses
        ):
            synchronized += 1
            continue
        elif queue_status in {"resolved", "terminal_failed"}:
            action = "reopen"
        else:
            action = "reanchor"
        targets.append(
            {
                "content_id": int(row["content_id"]),
                "evaluation_id": int(row["evaluation_id"]),
                "evidence_level": str(row["evidence_level"]),
                "pending_review": bool(row["pending_review"]),
                "queue_id": int(queue_id) if queue_id is not None else None,
                "queue_evaluation_id": int(queue_evaluation_id)
                if queue_evaluation_id is not None
                else None,
                "queue_status": queue_status,
                "action": action,
            }
        )
    action_counts = {
        action: sum(int(item["action"] == action) for item in targets)
        for action in ("create", "reopen", "reanchor", "resolve")
    }
    payload = {
        "schema_version": "gray-review-queue-sync-v2",
        "release_id": str(release["id"]),
        "rule_version": str(release["rule_version"]),
        "targets": targets,
    }
    return {
        **payload,
        "plan_sha256": sha256_json(payload),
        "gray_evaluation_count": gray_evaluation_count,
        "synchronized_count": synchronized,
        "target_count": len(targets),
        "action_counts": action_counts,
    }


def plan_gray_review_queue_sync(*, db_path: Path = DEFAULT_DB) -> Dict[str, Any]:
    """Build a deterministic, strictly read-only plan for current v8 gray rows."""

    resolved = db_path.resolve()
    if not resolved.is_file():
        raise EvaluationError(f"database does not exist: {resolved}")
    connection = sqlite3.connect(f"{resolved.as_uri()}?mode=ro", uri=True, timeout=10)
    connection.row_factory = sqlite3.Row
    try:
        return _gray_review_queue_sync_plan(connection)
    finally:
        connection.close()


def apply_gray_review_queue_sync(
    *,
    expected_plan_sha256: str,
    db_path: Path = DEFAULT_DB,
) -> Dict[str, Any]:
    """Apply one currently confirmed queue plan atomically."""

    resolved = db_path.resolve()
    if not resolved.is_file():
        raise EvaluationError(f"database does not exist: {resolved}")
    with connect(resolved) as connection, transaction(connection):
        plan = _gray_review_queue_sync_plan(connection)
        if expected_plan_sha256 != str(plan["plan_sha256"]):
            raise EvaluationError("gray review queue sync plan hash changed")
        changes_before = connection.total_changes
        for item in plan["targets"]:
            _synchronize_gray_review_queue(
                connection,
                content_id=int(item["content_id"]),
                evaluation_id=int(item["evaluation_id"]),
                release_id=str(plan["release_id"]),
                evidence_level=str(item["evidence_level"]),
                pending_review=bool(item["pending_review"]),
            )
        applied_changes = connection.total_changes - changes_before
        remaining = _gray_review_queue_sync_plan(connection)
        if int(remaining["target_count"]) != 0:
            raise EvaluationError("gray review queue sync did not converge")
    return {
        "release_id": plan["release_id"],
        "plan_sha256": plan["plan_sha256"],
        "target_count": plan["target_count"],
        "action_counts": plan["action_counts"],
        "applied_count": plan["target_count"],
        "sqlite_changes": applied_changes,
        "remaining_target_count": 0,
        "reused": int(plan["target_count"]) == 0,
    }


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


def _artifact_components(
    connection: sqlite3.Connection, content_id: int
) -> Dict[str, Any]:
    detail = connection.execute(
        """
        SELECT sha256 FROM provider_raw_responses
        WHERE content_id=?
          AND operation IN (
              'douyin_video_detail',
              'xiaohongshu_note_detail',
              'xiaohongshu_video_detail'
          )
        ORDER BY captured_at DESC, id DESC LIMIT 1
        """,
        (content_id,),
    ).fetchone()
    if detail is None:
        detail = _artifact(
            connection, content_id, ("provider_content", "public_content")
        )
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
        "comments_version_sha256": str(comments["sha256"])
        if comments is not None
        else None,
        "manual_evidence_sha256": sha256_json(manual_payload)
        if manual_payload
        else None,
        "media_path": _resolved_path(str(media["local_path"]))
        if media is not None
        else None,
        "asr_path": _resolved_path(str(asr["local_path"])) if asr is not None else None,
        "ocr_path": _resolved_path(str(ocr["local_path"])) if ocr is not None else None,
        "manual_rows": manual_payload,
    }


def _current_evidence_state(
    connection: sqlite3.Connection, content_id: int
) -> tuple[Dict[str, Any], Dict[str, Any], str]:
    """Build the exact hash inputs shared by candidate detection and evaluation."""

    content = connection.execute(
        "SELECT * FROM content_items WHERE id=?", (content_id,)
    ).fetchone()
    if content is None:
        raise EvaluationError(f"content {content_id} does not exist")
    artifacts = _artifact_components(connection, content_id)
    text_sha = sha256_json(
        {
            "version": TEXT_EVIDENCE_VERSION,
            "title": content["title"],
            "body": content["body"],
            "content_type": content["content_type"],
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
    return artifacts, components, sha256_json(components)


def build_evidence_envelope(
    connection: sqlite3.Connection, content_id: int
) -> tuple[int, str, Dict[str, Any]]:
    artifacts, components, evidence_sha = _current_evidence_state(
        connection, content_id
    )
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
            content_id,
            EVIDENCE_VERSION,
            components["detail_raw_sha256"],
            components["text_sha256"],
            components["media_sha256"],
            components["asr_sha256"],
            components["ocr_sha256"],
            components["comments_version_sha256"],
            components["manual_evidence_sha256"],
            evidence_sha,
            canonical_json(components),
            now_utc(),
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
            return bool(
                value.get("status") in {"complete", "partial"} and path.is_file()
            )
        return bool(
            value.get("status") in {"complete", "partial"} and value.get("image_paths")
        )
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
    ocr_ok = (
        ocr.get("status") == "success"
        and _chinese_count(ocr_text) >= 15
        and ocr_count > 0
    )
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


def _resolve_evaluation_release(
    connection: sqlite3.Connection,
    *,
    source: str,
    release_id: str | None,
) -> sqlite3.Row:
    if release_id is None:
        rows = connection.execute(
            "SELECT * FROM evaluation_releases WHERE status='active' ORDER BY id"
        ).fetchall()
        if len(rows) > 1:
            raise EvaluationError("multiple active evaluation releases exist")
        if rows:
            release = rows[0]
        else:
            release_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM evaluation_releases"
                ).fetchone()[0]
            )
            if release_count:
                raise EvaluationError("no active evaluation release exists")
            taxonomies = connection.execute(
                "SELECT * FROM taxonomy_versions WHERE status='published' ORDER BY id"
            ).fetchall()
            if len(taxonomies) != 1:
                raise EvaluationError(
                    "exactly one published taxonomy is required for legacy bootstrap"
                )
            try:
                release = ensure_legacy_evaluation_release(
                    connection,
                    rule_version=RULE_VERSION,
                    taxonomy_version=str(taxonomies[0]["version"]),
                )
            except SchemaMigrationError as exc:
                raise EvaluationError(str(exc)) from exc
    else:
        release = connection.execute(
            "SELECT * FROM evaluation_releases WHERE id=?", (release_id,)
        ).fetchone()
        if release is None:
            raise EvaluationError(f"evaluation release does not exist: {release_id}")

    status = str(release["status"])
    rule_version = str(release["rule_version"])
    if release_id is not None and (
        source != "automatic"
        or status != "backfilling"
        or rule_version != V8_RULE_VERSION
    ):
        raise EvaluationError(
            "explicit release evaluation requires an evaluation-v8 "
            "release in backfilling status"
        )
    if source == "manual_review" and status != "active":
        raise EvaluationError("manual review evaluations require the active release")
    if source == "automatic" and status not in {"active", "backfilling"}:
        raise EvaluationError(
            f"automatic evaluation cannot write release in status {status}"
        )
    if rule_version not in {RULE_VERSION, V8_RULE_VERSION}:
        raise EvaluationError(
            f"unsupported evaluation rule version: {release['rule_version']}"
        )
    if status == "backfilling" and rule_version != V8_RULE_VERSION:
        raise EvaluationError("only evaluation-v8 releases may be backfilled")
    return release


def _load_release_runtime(
    connection: sqlite3.Connection, release: sqlite3.Row
) -> _EvaluationRuntime:
    taxonomy_rows = connection.execute(
        "SELECT * FROM taxonomy_versions WHERE version=?",
        (release["taxonomy_version"],),
    ).fetchall()
    if len(taxonomy_rows) != 1:
        raise EvaluationError("evaluation release taxonomy does not exist uniquely")
    taxonomy_row = taxonomy_rows[0]
    expected_taxonomy_status = (
        "published" if str(release["status"]) == "active" else "draft"
    )
    if str(taxonomy_row["status"]) != expected_taxonomy_status:
        raise EvaluationError(
            f"{release['status']} release requires a {expected_taxonomy_status} taxonomy"
        )
    point_rows = connection.execute(
        "SELECT * FROM selling_points WHERE taxonomy_id=? ORDER BY code",
        (taxonomy_row["id"],),
    ).fetchall()
    taxonomy: Dict[str, Dict[str, Any]] = {}
    allowed_scenes: Dict[str, set[str]] = {}
    matcher: MaterializedMatcher | None = None
    rule_version = str(release["rule_version"])
    if rule_version == V8_RULE_VERSION:
        codes = {str(row["code"]) for row in point_rows}
        if (
            len(point_rows) != len(POINT_IDS)
            or codes != POINT_IDS
            or any(int(row["enabled"]) != 1 for row in point_rows)
        ):
            raise EvaluationError(
                "evaluation-v8 taxonomy must contain exactly 25 enabled approved points"
            )
        materialized_rules: Dict[str, Mapping[str, Any]] = {}
        try:
            for row in point_rows:
                code = str(row["code"])
                point = serialize_point_row(connection, taxonomy_row, row)
                rule = point["matcher_rule"]
                if not isinstance(rule, Mapping):
                    raise EvaluationError(f"selling point {code} has no matcher rule")
                taxonomy[code] = point
                allowed_scenes[code] = set(point["scenes"])
                materialized_rules[code] = rule
            matcher = MaterializedMatcher(materialized_rules)
        except (TaxonomyError, MatcherDslError) as exc:
            raise EvaluationError(f"invalid release matcher snapshot: {exc}") from exc
        if matcher.matcher_rule_sha256 != str(release["matcher_rule_sha256"]):
            raise EvaluationError("release matcher hash does not match taxonomy rules")
    else:
        for row in point_rows:
            if int(row["enabled"]) != 1:
                continue
            code = str(row["code"])
            taxonomy[code] = dict(row)
            allowed_scenes[code] = {
                str(scene["scene"])
                for scene in connection.execute(
                    """
                    SELECT scene FROM selling_point_scenes
                    WHERE selling_point_id=? ORDER BY scene
                    """,
                    (row["id"],),
                ).fetchall()
            }
    return _EvaluationRuntime(
        release=dict(release),
        taxonomy_version=str(taxonomy_row["version"]),
        taxonomy=taxonomy,
        allowed_scenes=allowed_scenes,
        matcher=matcher,
    )


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
    return (
        fallback if fallback in {"new_car", "used_car", "media", "other"} else "unknown"
    )


AUTO_TERMS = (
    "汽车",
    "车型",
    "新车",
    "二手车",
    "车价",
    "买车",
    "卖车",
    "用车",
    "试驾",
    "驾驶",
    "保养",
    "维修",
    "发动机",
    "变速箱",
    "轮胎",
    "刹车",
    "续航",
    "油耗",
    "配置",
    "车主",
    "底盘",
    "车况",
    "懂车帝",
    "AI小懂",
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
        "desc": "\n".join(
            value for value in (content["title"], content["body"]) if value
        ),
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
    return sorted(
        matches,
        key=lambda item: (-int(item.get("score") or 0), str(item.get("id") or "")),
    )


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
    scorer = importlib.import_module("three_proposition_scoring")
    if len(rows) < int(scorer.MIN_VALID_COMMENTERS):
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
                    content_id,
                    evidence_version_id,
                    row["anonymous_user_key"],
                    int(row["audience_automotive_score"]),
                    int(row["action_intent_score"]),
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


def upgrade_evaluations_to_current_rule(
    *,
    db_path: Path = DEFAULT_DB,
    content_ids: Optional[Sequence[int]] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Compatibility endpoint retained as a hard-disabled audit surface.

    A release change always requires a real evaluation. Copying an older row into
    a newer rule version is forbidden because it preserves stale taxonomy codes.
    """

    del db_path, content_ids, limit
    return {
        "candidates": 0,
        "created": 0,
        "reused": 0,
        "content_ids": [],
        "disabled": True,
    }


@contextmanager
def _evaluation_write_scope(
    db_path: Path, connection: Optional[sqlite3.Connection]
) -> Iterator[sqlite3.Connection]:
    if connection is not None:
        if not connection.in_transaction:
            raise RuntimeError("shared evaluation connection must be in a transaction")
        yield connection
        return
    with connect(db_path) as owned_connection, transaction(owned_connection):
        yield owned_connection


def _parent_evaluation_projection(
    connection: sqlite3.Connection, parent: sqlite3.Row
) -> Dict[str, Any]:
    try:
        parsed_payload = json.loads(str(parent["payload_json"]))
    except json.JSONDecodeError as exc:
        raise EvaluationError("parent evaluation payload is invalid") from exc
    if not isinstance(parsed_payload, dict):
        raise EvaluationError("parent evaluation payload is not an object")

    matches: List[Dict[str, Any]] = []
    match_rows = connection.execute(
        """
        SELECT selling_point_code,scene,match_role,score,evidence_json
        FROM evaluation_matches WHERE evaluation_id=?
        ORDER BY CASE match_role WHEN 'primary' THEN 0 ELSE 1 END,rowid
        """,
        (parent["id"],),
    ).fetchall()
    for row in match_rows:
        try:
            parsed_match = json.loads(str(row["evidence_json"]))
        except json.JSONDecodeError as exc:
            raise EvaluationError("parent evaluation match is invalid") from exc
        if not isinstance(parsed_match, dict):
            raise EvaluationError("parent evaluation match is not an object")
        match = dict(parsed_match)
        match.update(
            {
                "id": str(row["selling_point_code"]),
                "scene": str(row["scene"]),
                "score": int(row["score"] or 0),
            }
        )
        matches.append(match)

    primary_code = (
        str(parent["primary_selling_point_code"])
        if parent["primary_selling_point_code"] is not None
        else None
    )
    if bool(primary_code) != bool(matches) or (
        primary_code is not None and str(matches[0]["id"]) != primary_code
    ):
        raise EvaluationError(
            "parent evaluation primary fields and matches are inconsistent"
        )
    return {
        "payload": dict(parsed_payload),
        "evaluation_status": str(parent["evaluation_status"]),
        "evidence_level": str(parent["evidence_level"]),
        "evidence_summary": str(parsed_payload.get("evidence_summary") or ""),
        "primary_code": primary_code,
        "selling_score": int(parent["selling_point_score"])
        if parent["selling_point_score"] is not None
        else None,
        "included": bool(parent["selling_point_included"]),
        "direction": str(parent["content_direction"]),
        "content_score": int(parent["content_automotive_score"])
        if parent["content_automotive_score"] is not None
        else None,
        "audience_score": int(parent["audience_automotive_score"])
        if parent["audience_automotive_score"] is not None
        else None,
        "action_score": int(parsed_payload["action_intent_score"])
        if parsed_payload.get("action_intent_score") is not None
        else None,
        "valid_commenters": int(parsed_payload.get("valid_unique_commenters") or 0),
        "acquisition_score": int(parent["acquisition_potential_score"])
        if parent["acquisition_potential_score"] is not None
        else None,
        "matches": matches,
    }


def _evaluate_content(
    content_id: int,
    *,
    db_path: Path = DEFAULT_DB,
    source: str = "automatic",
    manual_override: Optional[Mapping[str, Any]] = None,
    review_id: Optional[int] = None,
    parent_evaluation_id: Optional[int] = None,
    _release_id: str | None = None,
    _connection: Optional[sqlite3.Connection] = None,
) -> EvaluationResult:
    if source not in {"automatic", "manual_review"}:
        raise EvaluationError(f"unsupported evaluation source: {source}")
    override = dict(manual_override or {})
    unknown_override_fields = set(override) - {
        "decision",
        "primary_selling_point_code",
        "selling_point_score",
        "selling_point_included",
        "content_automotive_score",
        "content_direction",
    }
    if unknown_override_fields:
        raise EvaluationError(
            f"unknown manual override fields: {sorted(unknown_override_fields)}"
        )
    decision = str(override.get("decision") or "")
    if source == "automatic" and manual_override is not None:
        raise EvaluationError("automatic evaluation does not accept manual_override")
    if source == "manual_review" and decision not in {
        "confirm",
        "override",
        "insufficient_evidence",
        "terminal_unavailable",
    }:
        raise EvaluationError("manual review evaluation requires a valid decision")
    if source == "manual_review" and review_id is None:
        raise EvaluationError("manual review evaluation requires review_id")
    if source == "manual_review" and parent_evaluation_id is None:
        raise EvaluationError("manual review evaluation requires parent_evaluation_id")
    if source != "manual_review" and review_id is not None:
        raise EvaluationError("review_id is only valid for manual review evaluations")
    if source != "manual_review" and parent_evaluation_id is not None:
        raise EvaluationError(
            "parent_evaluation_id is only valid for manual review evaluations"
        )
    with _evaluation_write_scope(db_path, _connection) as connection:
        content = connection.execute(
            "SELECT * FROM content_items WHERE id=?", (content_id,)
        ).fetchone()
        if content is None:
            raise EvaluationError(f"content {content_id} does not exist")
        release = _resolve_evaluation_release(
            connection, source=source, release_id=_release_id
        )
        runtime = _load_release_runtime(connection, release)
        taxonomy = runtime.taxonomy
        taxonomy_version = runtime.taxonomy_version
        parent: sqlite3.Row | None = None
        if source == "manual_review":
            review = connection.execute(
                """
                SELECT content_id,previous_evaluation_id
                FROM evaluation_reviews WHERE id=?
                """,
                (review_id,),
            ).fetchone()
            parent = connection.execute(
                "SELECT * FROM evaluation_versions WHERE id=?",
                (parent_evaluation_id,),
            ).fetchone()
            if review is None or parent is None:
                raise EvaluationError("manual review lineage does not exist")
            if (
                int(review["content_id"]) != content_id
                or int(parent["content_id"]) != content_id
                or review["previous_evaluation_id"] != parent_evaluation_id
                or str(parent["release_id"]) != str(release["id"])
            ):
                raise EvaluationError("manual review lineage does not match content")
        artifacts, _, evidence_sha = _current_evidence_state(connection, content_id)
        if source == "automatic":
            existing = connection.execute(
                """
                SELECT * FROM evaluation_versions
                WHERE content_id=? AND release_id=? AND evidence_sha256=?
                  AND evaluation_source='automatic'
                """,
                (content_id, release["id"], evidence_sha),
            ).fetchone()
        else:
            existing = connection.execute(
                """
                SELECT * FROM evaluation_versions
                WHERE release_id=? AND review_id=?
                  AND evaluation_source='manual_review'
                """,
                (release["id"], review_id),
            ).fetchone()
        if existing is not None:
            if existing["invalidated_at"] is not None:
                raise EvaluationError(
                    "an invalidated evaluation already owns this release idempotency key; "
                    "create a new release before reevaluating"
                )
            existing_envelope_id = existing["evidence_envelope_id"]
            if existing_envelope_id is None:
                raise EvaluationError("existing evaluation has no evidence envelope")
            if source == "automatic":
                _synchronize_gray_review_queue(
                    connection,
                    content_id=int(existing["content_id"]),
                    evaluation_id=int(existing["id"]),
                    release_id=str(release["id"]),
                    evidence_level=str(existing["evidence_level"]),
                    pending_review=bool(existing["pending_review"]),
                )
            return EvaluationResult(
                evaluation_id=int(existing["id"]),
                evidence_envelope_id=int(existing_envelope_id),
                content_id=int(existing["content_id"]),
                evidence_sha256=str(existing["evidence_sha256"]),
                evidence_level=str(existing["evidence_level"]),
                created=False,
            )
        envelope_id, persisted_evidence_sha, artifacts = build_evidence_envelope(
            connection, content_id
        )
        if persisted_evidence_sha != evidence_sha:
            raise EvaluationError("evidence changed during evaluation transaction")

        asr = _read_json(artifacts["asr_path"])
        ocr = _read_json(artifacts["ocr_path"])
        manual_rows = artifacts["manual_rows"]
        manual_text = "\n".join(str(row.get("text_value") or "") for row in manual_rows)
        manual_visual_text = "\n".join(
            str(row.get("text_value") or "")
            for row in manual_rows
            if row.get("evidence_type") in {"visual_summary", "media_observation"}
        )
        manual_desc_text = "\n".join(
            str(row.get("text_value") or "")
            for row in manual_rows
            if row.get("evidence_type") not in {"visual_summary", "media_observation"}
        )
        body_text = "\n".join(
            value
            for value in (
                str(content["title"] or ""),
                str(content["body"] or ""),
                manual_text,
            )
            if value
        )
        matcher_desc = "\n".join(
            value
            for value in (
                str(content["title"] or ""),
                str(content["body"] or ""),
                manual_desc_text,
            )
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
        matches: List[Dict[str, Any]]
        if source == "manual_review":
            matches = []
            included_min, review_min = INCLUDE_MIN, REVIEW_MIN
        elif evidence_level not in {"V2", "V3"}:
            matches = []
            included_min, review_min = INCLUDE_MIN, REVIEW_MIN
        elif runtime.matcher is None:
            matches = _match_points(
                content=content,
                asr=asr,
                ocr=ocr,
                evidence_level=evidence_level,
                taxonomy=taxonomy,
            )
            included_min, review_min = INCLUDE_MIN, REVIEW_MIN
        else:
            all_matches = runtime.matcher.match_points(
                {
                    "desc": matcher_desc,
                    "content_type": content["content_type"],
                    "media_type": 4 if content["content_type"] == "video" else 2,
                },
                asr,
                ocr,
                evidence_level,
                {"summary": manual_visual_text},
            )
            included_min = int(runtime.matcher.thresholds["included_min"])
            review_min = int(runtime.matcher.thresholds["review_min"])
            max_secondary = int(runtime.matcher.thresholds["max_secondary"])
            matches = (
                [
                    all_matches[0],
                    *[
                        item
                        for item in all_matches[1:]
                        if int(item["score"]) >= review_min
                    ][:max_secondary],
                ]
                if all_matches
                else []
            )
        primary = matches[0] if matches else None
        primary_code = str(primary["id"]) if primary else None
        selling_score = int(primary["score"]) if primary else None
        included = bool(
            primary and selling_score is not None and selling_score >= included_min
        )
        pending_review = bool(
            evidence_level in {"V0", "V1"}
            or (
                selling_score is not None and review_min <= selling_score < included_min
            )
        )
        direction = (
            str(primary["scene"])
            if primary is not None and runtime.matcher is not None
            else _direction_for(
                primary_code,
                f"{body_text}\n{asr.get('text') or ''}\n{ocr.get('combined_text') or ''}",
                str(content["evaluation_content_direction"] or "unknown"),
            )
        )
        content_score = (
            _automotive_score(
                f"{body_text}\n{asr.get('text') or ''}\n{ocr.get('combined_text') or ''}",
                selling_included=included,
            )
            if evidence_level in {"V2", "V3"}
            else None
        )

        parent_projection: Dict[str, Any] | None = None
        if decision == "terminal_unavailable":
            evidence_level, evidence_summary = "V0", "人工确认内容不可用"
            primary_code, selling_score, included, content_score = (
                None,
                None,
                False,
                None,
            )
            pending_review = False
            matches = []
        elif decision == "insufficient_evidence":
            evidence_level, evidence_summary = "V1", "人工确认现有证据不足"
            primary_code, selling_score, included, content_score = (
                None,
                None,
                False,
                None,
            )
            pending_review = False
            matches = []
        elif decision == "override":
            override_fields = {
                "primary_selling_point_code",
                "selling_point_score",
                "selling_point_included",
                "content_automotive_score",
                "content_direction",
            }
            present_fields = override_fields.intersection(override)
            if not present_fields:
                raise EvaluationError("override requires at least one explicit field")
            if parent is None:
                raise EvaluationError("override requires a parent evaluation")
            parent_projection = _parent_evaluation_projection(connection, parent)
            evidence_level = str(parent_projection["evidence_level"])
            evidence_summary = str(parent_projection["evidence_summary"])
            primary_code = parent_projection["primary_code"]
            selling_score = parent_projection["selling_score"]
            included = bool(parent_projection["included"])
            direction = str(parent_projection["direction"])
            content_score = parent_projection["content_score"]
            matches = [dict(match) for match in parent_projection["matches"]]

            primary_field_present = "primary_selling_point_code" in override
            requested_primary = (
                override.get("primary_selling_point_code")
                if primary_field_present
                else primary_code
            )
            next_primary = str(requested_primary or "").strip() or None
            if next_primary is not None and next_primary not in taxonomy:
                raise EvaluationError(f"unknown selling point: {next_primary}")
            primary_changed = next_primary != primary_code
            primary_code = next_primary

            if primary_field_present and primary_code is None:
                if override.get("selling_point_score") not in {None, 0} or override.get(
                    "selling_point_included"
                ) not in {None, False}:
                    raise EvaluationError(
                        "clearing the primary selling point requires score 0 and included false"
                    )
                selling_score = 0
                included = False
                matches = []
            else:
                if "selling_point_score" in override:
                    if override["selling_point_score"] is None:
                        raise EvaluationError(
                            "selling_point_score cannot be null while a primary point exists"
                        )
                    selling_score = int(override["selling_point_score"])
                elif primary_code is not None and selling_score is None:
                    selling_score = 90
                if "selling_point_included" in override:
                    if override["selling_point_included"] is None:
                        raise EvaluationError("selling_point_included cannot be null")
                    included = bool(override["selling_point_included"])

            if "content_automotive_score" in override:
                content_value = override["content_automotive_score"]
                content_score = (
                    int(content_value) if content_value is not None else None
                )
            if "content_direction" in override:
                if override["content_direction"] is None:
                    raise EvaluationError("content_direction cannot be null")
                direction = str(override["content_direction"])

            if primary_code is not None:
                if primary_changed or not matches:
                    matches = [
                        {
                            "id": primary_code,
                            "score": selling_score,
                            "scene": direction,
                            "reason": "人工复核覆盖",
                            "source": "manual",
                        }
                    ]
                else:
                    if "selling_point_score" in override:
                        matches[0]["score"] = selling_score
                        matches[0]["reason"] = "人工复核覆盖"
                        matches[0]["source"] = "manual"
                    if "content_direction" in override:
                        matches[0]["scene"] = direction
                        matches[0]["reason"] = "人工复核覆盖"
                        matches[0]["source"] = "manual"
            pending_review = False
        elif decision == "confirm":
            if parent is None:
                raise EvaluationError("confirm requires a parent evaluation")
            parent_projection = _parent_evaluation_projection(connection, parent)
            evidence_level = str(parent_projection["evidence_level"])
            evidence_summary = str(parent_projection["evidence_summary"])
            primary_code = parent_projection["primary_code"]
            selling_score = parent_projection["selling_score"]
            included = bool(parent_projection["included"])
            direction = str(parent_projection["direction"])
            content_score = parent_projection["content_score"]
            matches = [dict(match) for match in parent_projection["matches"]]
            pending_review = False
        if direction not in {"new_car", "used_car", "media", "other", "unknown"}:
            raise EvaluationError(f"invalid content direction: {direction}")
        if content_score is not None and not 0 <= content_score <= 100:
            raise EvaluationError("content automotive score must be 0..100")
        if selling_score is not None and not 0 <= selling_score <= 100:
            raise EvaluationError("selling point score must be 0..100")
        if primary_code is None and (
            included or selling_score not in {None, 0} or matches
        ):
            raise EvaluationError(
                "an evaluation without a primary selling point cannot be included, "
                "scored, or matched"
            )
        if primary_code is not None and (
            not matches or str(matches[0].get("id") or "") != primary_code
        ):
            raise EvaluationError(
                "primary selling point fields and matches are inconsistent"
            )
        if matches and direction not in {"new_car", "used_car", "media"}:
            raise EvaluationError("matched selling points require an E/X/M scene")
        if (
            decision == "override"
            and primary_code is not None
            and direction not in runtime.allowed_scenes[primary_code]
        ):
            allowed_scene_text = ", ".join(sorted(runtime.allowed_scenes[primary_code]))
            raise EvaluationError(
                f"selling point {primary_code} does not allow content direction "
                f"{direction}; allowed: {allowed_scene_text}"
            )
        for match in matches:
            code = str(match.get("id") or "")
            scene = str(match.get("scene") or direction)
            if code not in taxonomy:
                raise EvaluationError(f"matcher emitted unknown selling point: {code}")
            if (
                runtime.matcher is not None
                and scene not in runtime.allowed_scenes[code]
            ):
                raise EvaluationError(
                    f"selling point {code} does not allow matcher scene {scene}"
                )
            match["scene"] = scene

        if parent_projection is not None:
            audience_score = parent_projection["audience_score"]
            action_score = parent_projection["action_score"]
            valid_commenters = int(parent_projection["valid_commenters"])
            if decision == "override":
                task_fit = (
                    selling_score
                    if included
                    else 0
                    if content_score is not None
                    else None
                )
                acquisition_score = _acquisition_score(
                    content_score, audience_score, task_fit, action_score
                )
            else:
                acquisition_score = parent_projection["acquisition_score"]
            evaluation_status = str(parent_projection["evaluation_status"])
            payload = dict(parent_projection["payload"])
            payload.update(
                {
                    "evaluation_status": evaluation_status,
                    "evidence_level": evidence_level,
                    "evidence_summary": evidence_summary,
                    "primary_selling_point_id": primary_code or "",
                    "selling_point_score": selling_score,
                    "selling_point_included": included,
                    "pending_review": False,
                    "content_direction": direction,
                    "content_automotive_score": content_score,
                    "audience_automotive_score": audience_score,
                    "action_intent_score": action_score,
                    "valid_unique_commenters": valid_commenters,
                    "acquisition_potential": acquisition_score,
                    "matches": matches[:3],
                    "evaluation_source": source,
                    "release_id": str(release["id"]),
                }
            )
        else:
            audience_score, action_score, valid_commenters = _comment_scores(
                connection, content_id
            )
            task_fit = (
                selling_score if included else 0 if content_score is not None else None
            )
            acquisition_score = _acquisition_score(
                content_score, audience_score, task_fit, action_score
            )
            evaluation_status = (
                "evaluated"
                if evidence_level in {"V2", "V3"}
                else "insufficient_evidence"
            )
            payload = {
                "evaluation_status": evaluation_status,
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
                "release_id": str(release["id"]),
            }
        cursor = connection.execute(
            """
            INSERT INTO evaluation_versions(
                content_id, evidence_envelope_id, release_id, parent_evaluation_id,
                review_id, rule_version, taxonomy_version, matcher_rule_sha256,
                evidence_sha256, evaluation_source, evaluation_status, evidence_level,
                primary_selling_point_code, selling_point_score, selling_point_included,
                content_direction, content_automotive_score, audience_automotive_score,
                acquisition_potential_score, pending_review, payload_json, evaluated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                content_id,
                envelope_id,
                release["id"],
                parent_evaluation_id,
                review_id,
                release["rule_version"],
                taxonomy_version,
                release["matcher_rule_sha256"],
                evidence_sha,
                source,
                payload["evaluation_status"],
                evidence_level,
                primary_code,
                selling_score,
                int(included),
                direction,
                content_score,
                audience_score,
                acquisition_score,
                int(pending_review),
                canonical_json(payload),
                now_utc(),
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
                    evaluation_id, selling_point_code, scene, match_role, score, evidence_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    evaluation_id,
                    code,
                    match["scene"],
                    "primary" if index == 0 else "secondary",
                    int(match.get("score") or 0),
                    canonical_json(match),
                ),
            )
        if source == "automatic":
            _synchronize_gray_review_queue(
                connection,
                content_id=content_id,
                evaluation_id=evaluation_id,
                release_id=str(release["id"]),
                evidence_level=evidence_level,
                pending_review=pending_review,
            )
        if str(release["status"]) == "active":
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


def evaluate_content(
    content_id: int,
    *,
    db_path: Path = DEFAULT_DB,
    source: str = "automatic",
    manual_override: Optional[Mapping[str, Any]] = None,
    review_id: Optional[int] = None,
    parent_evaluation_id: Optional[int] = None,
) -> EvaluationResult:
    return _evaluate_content(
        content_id,
        db_path=db_path,
        source=source,
        manual_override=manual_override,
        review_id=review_id,
        parent_evaluation_id=parent_evaluation_id,
    )


def evaluate_release_content(
    content_id: int,
    *,
    release_id: str,
    db_path: Path = DEFAULT_DB,
) -> EvaluationResult:
    """Evaluate one content item into an explicit v8 backfilling release."""

    if not release_id.strip():
        raise EvaluationError("release_id is required")
    return _evaluate_content(
        content_id,
        db_path=db_path,
        source="automatic",
        _release_id=release_id,
    )


def incremental_candidates(*, db_path: Path = DEFAULT_DB) -> List[int]:
    with connect(db_path) as connection:
        content_rows = connection.execute(
            "SELECT id FROM content_items ORDER BY id"
        ).fetchall()
        release = connection.execute(
            "SELECT id FROM evaluation_releases WHERE status='active'"
        ).fetchone()
        current_rows = (
            []
            if release is None
            else connection.execute(
                """
                SELECT content_id,evidence_sha256
                FROM (
                    SELECT content_id,evidence_sha256,
                           ROW_NUMBER() OVER (
                               PARTITION BY content_id
                               ORDER BY evaluated_at DESC,id DESC
                           ) selector_rank
                    FROM evaluation_versions
                    WHERE release_id=? AND invalidated_at IS NULL
                )
                WHERE selector_rank=1
                """,
                (release["id"],),
            ).fetchall()
        )
        current_evidence = {
            (int(row["content_id"]), str(row["evidence_sha256"]))
            for row in current_rows
        }
        invalidated_automatic_evidence = {
            (int(row["content_id"]), str(row["evidence_sha256"]))
            for row in (
                []
                if release is None
                else connection.execute(
                    """
                    SELECT content_id,evidence_sha256
                    FROM evaluation_versions
                    WHERE release_id=? AND evaluation_source='automatic'
                      AND invalidated_at IS NOT NULL
                    """,
                    (release["id"],),
                ).fetchall()
            )
        }
        candidates: List[int] = []
        blocked: List[int] = []
        for row in content_rows:
            content_id = int(row["id"])
            _, _, evidence_sha256 = _current_evidence_state(connection, content_id)
            evidence_key = (content_id, evidence_sha256)
            if evidence_key in current_evidence:
                continue
            if evidence_key in invalidated_automatic_evidence:
                blocked.append(content_id)
                continue
            candidates.append(content_id)
        if blocked:
            sample = ",".join(str(content_id) for content_id in blocked[:10])
            raise EvaluationError(
                "active release contains invalidated automatic idempotency keys for "
                f"current evidence; create a new release before reevaluating: {sample}"
            )
    return candidates


def evaluate_incremental(
    *, db_path: Path = DEFAULT_DB, limit: Optional[int] = None
) -> Dict[str, Any]:
    rule_upgrade = upgrade_evaluations_to_current_rule(db_path=db_path, limit=limit)
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
        "rule_upgrade": rule_upgrade,
        "candidates": len(candidates),
        "created": created,
        "reused": reused,
        "results": results,
    }


def reopen_review(
    queue_id: int,
    *,
    reason: str,
    reopened_by: str,
    db_path: Path = DEFAULT_DB,
) -> ReviewReopenResult:
    """Reopen a resolved queue with an explicit append-only audit event."""

    if not reason.strip() or not reopened_by.strip():
        raise EvaluationError("reopen reason and operator are required")
    with connect(db_path) as connection, transaction(connection):
        queue = connection.execute(
            "SELECT * FROM review_queue WHERE id=?", (queue_id,)
        ).fetchone()
        if queue is None:
            raise EvaluationError(f"review queue {queue_id} does not exist")
        if queue["status"] != "resolved":
            raise EvaluationError(
                f"review queue {queue_id} must be resolved before reopening"
            )
        current = review_anchor_evaluation(connection, int(queue["content_id"]))
        if current is None:
            raise EvaluationError("review content has no current evaluation")
        previous_review = connection.execute(
            """
            SELECT id FROM evaluation_reviews
            WHERE queue_id=? AND resulting_evaluation_id IS NOT NULL
            ORDER BY id DESC LIMIT 1
            """,
            (queue_id,),
        ).fetchone()
        created_at = now_utc()
        cursor = connection.execute(
            """
            INSERT INTO review_reopen_events(
                queue_id, content_id, previous_review_id, base_evaluation_id,
                reopened_by, reason, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                queue_id,
                queue["content_id"],
                previous_review["id"] if previous_review is not None else None,
                current["id"],
                reopened_by.strip(),
                reason.strip(),
                created_at,
            ),
        )
        if cursor.lastrowid is None:
            raise RuntimeError("review reopen insert returned no id")
        updated = connection.execute(
            """
            UPDATE review_queue
            SET evaluation_id=?, status='in_review', assigned_to=?,
                resolved_at=NULL, updated_at=?
            WHERE id=? AND status='resolved'
            """,
            (current["id"], reopened_by.strip(), created_at, queue_id),
        )
        if updated.rowcount != 1:
            raise EvaluationError("复核任务状态已更新，请刷新后重试")
        return ReviewReopenResult(
            event_id=int(cursor.lastrowid),
            queue_id=queue_id,
            content_id=int(queue["content_id"]),
            base_evaluation_id=int(current["id"]),
        )


def resolve_review(
    queue_id: int,
    *,
    decision: str,
    reason: str,
    reviewer: str,
    evidence_type: str,
    evidence_text: str,
    base_evaluation_id: int,
    overrides: Optional[Mapping[str, Any]] = None,
    db_path: Path = DEFAULT_DB,
) -> EvaluationResult:
    allowed = {"confirm", "override", "insufficient_evidence", "terminal_unavailable"}
    if decision not in allowed:
        raise EvaluationError(f"invalid review decision: {decision}")
    if not reason.strip() or not reviewer.strip() or not evidence_text.strip():
        raise EvaluationError("reason, reviewer and manual evidence are required")
    if base_evaluation_id is None or base_evaluation_id < 1:
        raise EvaluationError("base_evaluation_id is required")
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
            "new_car",
            "used_car",
            "media",
            "other",
            "unknown",
        }:
            raise EvaluationError("invalid content direction")
    with connect(db_path) as connection, transaction(connection):
        queue = connection.execute(
            "SELECT * FROM review_queue WHERE id=?", (queue_id,)
        ).fetchone()
        if queue is None:
            raise EvaluationError(f"review queue {queue_id} does not exist")
        queue_evaluation_id = (
            int(queue["evaluation_id"]) if queue["evaluation_id"] is not None else None
        )
        if queue_evaluation_id != base_evaluation_id:
            raise EvaluationError("评估证据已更新，请刷新证据后重新复核")
        previous = review_anchor_evaluation(connection, int(queue["content_id"]))
        current_evaluation_id = int(previous["id"]) if previous is not None else None
        if current_evaluation_id != base_evaluation_id:
            raise EvaluationError("评估证据已更新，请刷新证据后重新复核")
        if queue["status"] in {"resolved", "terminal_failed"}:
            raise EvaluationError(
                f"review queue {queue_id} is already {queue['status']}"
            )
        if decision == "override":
            primary_code = override_values.get("primary_selling_point_code")
            direction = override_values.get("content_direction")
            if primary_code and direction:
                allowed_scenes = {
                    str(row["scene"])
                    for row in connection.execute(
                        """
                        SELECT sps.scene
                        FROM taxonomy_versions tv
                        JOIN selling_points sp ON sp.taxonomy_id=tv.id
                        JOIN selling_point_scenes sps
                          ON sps.selling_point_id=sp.id
                        WHERE tv.id=(
                            SELECT id FROM taxonomy_versions
                            WHERE status='published'
                            ORDER BY published_at DESC, created_at DESC LIMIT 1
                        ) AND sp.code=?
                        """,
                        (primary_code,),
                    ).fetchall()
                }
                if allowed_scenes and direction not in allowed_scenes:
                    allowed_scene_text = ", ".join(sorted(allowed_scenes))
                    raise EvaluationError(
                        f"selling point {primary_code} does not allow content "
                        f"direction {direction}; allowed: {allowed_scene_text}"
                    )
        review_cursor = connection.execute(
            """
            INSERT INTO evaluation_reviews(
                queue_id, content_id, previous_evaluation_id, decision,
                reason, reviewer, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                queue_id,
                queue["content_id"],
                previous["id"] if previous else None,
                decision,
                reason.strip(),
                reviewer.strip(),
                now_utc(),
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
                review_id,
                queue["content_id"],
                evidence_type,
                evidence_text.strip(),
                evidence_sha,
                now_utc(),
            ),
        )
        manual_override = {"decision": decision, **override_values}
        result = _evaluate_content(
            int(queue["content_id"]),
            db_path=db_path,
            source="manual_review",
            manual_override=manual_override,
            review_id=review_id,
            parent_evaluation_id=current_evaluation_id,
            _connection=connection,
        )
        next_status = (
            "terminal_failed" if decision == "terminal_unavailable" else "resolved"
        )
        review_updated = connection.execute(
            """
            UPDATE evaluation_reviews SET resulting_evaluation_id=? WHERE id=?
            """,
            (result.evaluation_id, review_id),
        )
        if review_updated.rowcount != 1:
            raise RuntimeError("review result update did not affect exactly one row")
        completed_at = now_utc()
        updated = connection.execute(
            """
            UPDATE review_queue SET evaluation_id=?, status=?, resolved_at=?, updated_at=?
            WHERE id=? AND evaluation_id=?
              AND status IN ('pending','in_review','manual_required')
            """,
            (
                result.evaluation_id,
                next_status,
                completed_at,
                completed_at,
                queue_id,
                base_evaluation_id,
            ),
        )
        if updated.rowcount != 1:
            raise EvaluationError("评估证据已更新，请刷新证据后重新复核")
        return result
