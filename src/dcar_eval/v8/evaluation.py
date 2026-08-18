"""Database-native incremental v8 evaluation (fully automatic, no manual review)."""

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

from .matcher_dsl import (
    V5_1_POINT_SPEC,
    V5_2_POINT_SPEC,
    MatcherDslError,
    MaterializedMatcher,
)
from .storage import (
    BACKFILL_SOURCE_GROUPS,
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
V9_RULE_VERSION = "evaluation-v9"
EVIDENCE_VERSION = "evidence-v1"
TEXT_EVIDENCE_VERSION = "text-evidence-v2"
INCLUDE_MIN = 75
REVIEW_MIN = 60
V8_TAXONOMY_POINT_SPECS = {
    "selling-points-v5.1": V5_1_POINT_SPEC,
    "selling-points-v5.2": V5_2_POINT_SPEC,
}


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
class _EvaluationRuntime:
    release: Mapping[str, Any]
    taxonomy_version: str
    taxonomy: Dict[str, Dict[str, Any]]
    allowed_scenes: Dict[str, set[str]]
    matcher: MaterializedMatcher | None


_MATERIALIZED_RULE_VERSIONS = {V8_RULE_VERSION, V9_RULE_VERSION}
_V9_TAXONOMY_VERSION = "selling-points-v5.2"


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


def _artifact_components(
    connection: sqlite3.Connection,
    content_id: int,
    *,
    rule_version: str,
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
    return {
        "detail_raw_sha256": str(detail["sha256"]) if detail is not None else None,
        "media_sha256": str(media["sha256"]) if media is not None else None,
        "asr_sha256": str(asr["sha256"]) if asr is not None else None,
        "ocr_sha256": str(ocr["sha256"]) if ocr is not None else None,
        "comments_version_sha256": str(comments["sha256"])
        if comments is not None
        else None,
        # v16 起系统无人工复核域：人工证据不再存在。哈希公式保留该键并恒为
        # None——evaluation-v9 本就排除人工证据，既有 evidence_sha256 因此
        # 逐字不变，不会触发全库“证据变化”重评。
        "manual_evidence_sha256": None,
        "media_path": _resolved_path(str(media["local_path"]))
        if media is not None
        else None,
        "asr_path": _resolved_path(str(asr["local_path"])) if asr is not None else None,
        "ocr_path": _resolved_path(str(ocr["local_path"])) if ocr is not None else None,
    }


def _current_evidence_state(
    connection: sqlite3.Connection,
    content_id: int,
    *,
    rule_version: str,
) -> tuple[Dict[str, Any], Dict[str, Any], str]:
    """Build the exact hash inputs shared by candidate detection and evaluation."""

    content = connection.execute(
        "SELECT * FROM content_items WHERE id=?", (content_id,)
    ).fetchone()
    if content is None:
        raise EvaluationError(f"content {content_id} does not exist")
    artifacts = _artifact_components(
        connection, content_id, rule_version=rule_version
    )
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
    connection: sqlite3.Connection,
    content_id: int,
    *,
    rule_version: str,
) -> tuple[int, str, Dict[str, Any]]:
    artifacts, components, evidence_sha = _current_evidence_state(
        connection, content_id, rule_version=rule_version
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
) -> tuple[str, str]:
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
    if media and (asr_ok or ocr_ok):
        return "V2", "媒体与 ASR 或 OCR 可覆盖主叙事"
    if content_type != "video" and ocr_ok:
        return "V2", "图文 OCR 覆盖可用媒体证据"
    if text.strip():
        return "V1", "只有标题、正文或话题，完整媒体证据不足"
    return "V0", "内容主体和有效文字均不可用"


def _resolve_evaluation_release(
    connection: sqlite3.Connection,
    *,
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
        status != "backfilling"
        or rule_version not in _MATERIALIZED_RULE_VERSIONS
    ):
        raise EvaluationError(
            "explicit release evaluation requires an evaluation-v8 or evaluation-v9 "
            "release in backfilling status"
        )
    if status not in {"active", "backfilling"}:
        raise EvaluationError(
            f"automatic evaluation cannot write release in status {status}"
        )
    if rule_version not in {RULE_VERSION, V8_RULE_VERSION, V9_RULE_VERSION}:
        raise EvaluationError(
            f"unsupported evaluation rule version: {release['rule_version']}"
        )
    if status == "backfilling" and rule_version not in _MATERIALIZED_RULE_VERSIONS:
        raise EvaluationError(
            "only evaluation-v8 or evaluation-v9 releases may be backfilled"
        )
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
    rule_version = str(release["rule_version"])
    expected_taxonomy_status = (
        "published"
        if str(release["status"]) == "active"
        or (
            rule_version == V9_RULE_VERSION
            and str(release["status"]) in {"backfilling", "ready"}
        )
        else "draft"
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
    if rule_version in _MATERIALIZED_RULE_VERSIONS:
        taxonomy_version = str(taxonomy_row["version"])
        if (
            rule_version == V9_RULE_VERSION
            and taxonomy_version != _V9_TAXONOMY_VERSION
        ):
            raise EvaluationError(
                "evaluation-v9 requires published selling-points-v5.2 taxonomy"
            )
        point_spec = V8_TAXONOMY_POINT_SPECS.get(taxonomy_version)
        if point_spec is None:
            raise EvaluationError(
                f"{rule_version} has no approved point contract for {taxonomy_version}"
            )
        point_ids = set(point_spec)
        codes = {str(row["code"]) for row in point_rows}
        if (
            len(point_rows) != len(point_ids)
            or codes != point_ids
            or any(int(row["enabled"]) != 1 for row in point_rows)
        ):
            raise EvaluationError(
                f"{rule_version} taxonomy must contain exactly "
                f"{len(point_ids)} enabled approved points for {taxonomy_version}"
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
            matcher = MaterializedMatcher(materialized_rules, point_spec=point_spec)
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


def _evaluate_content(
    content_id: int,
    *,
    db_path: Path = DEFAULT_DB,
    expected_active_release_id: str | None = None,
    _release_id: str | None = None,
    _connection: Optional[sqlite3.Connection] = None,
) -> EvaluationResult:
    if expected_active_release_id is not None:
        if not expected_active_release_id.strip():
            raise EvaluationError("expected_active_release_id must not be empty")
        if _release_id is not None:
            raise EvaluationError(
                "expected_active_release_id cannot be combined with explicit release"
            )
    with _evaluation_write_scope(db_path, _connection) as connection:
        content = connection.execute(
            "SELECT * FROM content_items WHERE id=?", (content_id,)
        ).fetchone()
        if content is None:
            raise EvaluationError(f"content {content_id} does not exist")
        release = _resolve_evaluation_release(connection, release_id=_release_id)
        if (
            expected_active_release_id is not None
            and str(release["id"]) != expected_active_release_id
        ):
            raise EvaluationError(
                "active evaluation release changed before evaluation write: "
                f"expected {expected_active_release_id}, got {release['id']}"
            )
        runtime = _load_release_runtime(connection, release)
        if runtime.matcher is None:
            raise EvaluationError(
                "automatic evaluation requires a materialized release matcher; "
                "legacy matcher fallback is disabled"
            )
        taxonomy = runtime.taxonomy
        taxonomy_version = runtime.taxonomy_version
        rule_version = str(release["rule_version"])
        artifacts, _, evidence_sha = _current_evidence_state(
            connection, content_id, rule_version=rule_version
        )
        existing = connection.execute(
            """
            SELECT * FROM evaluation_versions
            WHERE content_id=? AND release_id=? AND evidence_sha256=?
              AND evaluation_source='automatic'
            """,
            (content_id, release["id"], evidence_sha),
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
            return EvaluationResult(
                evaluation_id=int(existing["id"]),
                evidence_envelope_id=int(existing_envelope_id),
                content_id=int(existing["content_id"]),
                evidence_sha256=str(existing["evidence_sha256"]),
                evidence_level=str(existing["evidence_level"]),
                created=False,
            )
        envelope_id, persisted_evidence_sha, artifacts = build_evidence_envelope(
            connection, content_id, rule_version=rule_version
        )
        if persisted_evidence_sha != evidence_sha:
            raise EvaluationError("evidence changed during evaluation transaction")

        asr = _read_json(artifacts["asr_path"])
        ocr = _read_json(artifacts["ocr_path"])
        body_text = "\n".join(
            value
            for value in (
                str(content["title"] or ""),
                str(content["body"] or ""),
            )
            if value
        )
        matcher_desc = body_text
        evidence_level, evidence_summary = _evidence_level(
            content_type=str(content["content_type"]),
            text=body_text,
            media_path=artifacts["media_path"],
            asr=asr,
            ocr=ocr,
        )
        matches: List[Dict[str, Any]]
        if evidence_level not in {"V2", "V3"}:
            matches = []
            included_min, review_min = INCLUDE_MIN, REVIEW_MIN
        else:
            if runtime.matcher is None:
                raise EvaluationError("materialized release matcher is unavailable")
            all_matches = runtime.matcher.match_points(
                {
                    "desc": matcher_desc,
                    "content_type": content["content_type"],
                    "media_type": 4 if content["content_type"] == "video" else 2,
                },
                asr,
                ocr,
                evidence_level,
                {"summary": ""},
            )
            included_min = int(runtime.matcher.thresholds["included_min"])
            # DSL 阈值键名 review_min 属于已发布规则合同（改名会变
            # matcher_rule_sha256），保留原键名，仅表示 60 分弱匹配下限。
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
        direction = (
            str(primary["scene"])
            if primary is not None
            else str(content["evaluation_content_direction"] or "unknown")
        )
        if direction not in {"new_car", "used_car", "media", "other"}:
            direction = "unknown"
        content_score = (
            _automotive_score(
                f"{body_text}\n{asr.get('text') or ''}\n{ocr.get('combined_text') or ''}",
                selling_included=included,
            )
            if evidence_level in {"V2", "V3"}
            else None
        )

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
            "content_direction": direction,
            "content_automotive_score": content_score,
            "audience_automotive_score": audience_score,
            "action_intent_score": action_score,
            "valid_unique_commenters": valid_commenters,
            "acquisition_potential": acquisition_score,
            "matches": matches[:3],
            "evaluation_source": "automatic",
            "release_id": str(release["id"]),
        }
        cursor = connection.execute(
            """
            INSERT INTO evaluation_versions(
                content_id, evidence_envelope_id, release_id,
                rule_version, taxonomy_version, matcher_rule_sha256,
                evidence_sha256, evaluation_source, evaluation_status, evidence_level,
                primary_selling_point_code, selling_point_score, selling_point_included,
                content_direction, content_automotive_score, audience_automotive_score,
                acquisition_potential_score, payload_json, evaluated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                content_id,
                envelope_id,
                release["id"],
                release["rule_version"],
                taxonomy_version,
                release["matcher_rule_sha256"],
                evidence_sha,
                "automatic",
                payload["evaluation_status"],
                evidence_level,
                primary_code,
                selling_score,
                int(included),
                direction,
                content_score,
                audience_score,
                acquisition_score,
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
    expected_active_release_id: str | None = None,
) -> EvaluationResult:
    return _evaluate_content(
        content_id,
        db_path=db_path,
        expected_active_release_id=expected_active_release_id,
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
        _release_id=release_id,
    )


def incremental_candidates(*, db_path: Path = DEFAULT_DB) -> List[int]:
    with connect(db_path) as connection:
        content_rows = connection.execute(
            f"""
            SELECT id FROM content_items
            WHERE COALESCE(source_group,'') NOT IN (
                {','.join('?' for _ in BACKFILL_SOURCE_GROUPS)}
            )
            ORDER BY id
            """,
            BACKFILL_SOURCE_GROUPS,
        ).fetchall()
        release = connection.execute(
            "SELECT id,rule_version FROM evaluation_releases WHERE status='active'"
        ).fetchone()
        active_rule_version = (
            str(release["rule_version"]) if release is not None else RULE_VERSION
        )
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
            _, _, evidence_sha256 = _current_evidence_state(
                connection, content_id, rule_version=active_rule_version
            )
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
