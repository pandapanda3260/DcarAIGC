"""One-way, idempotent migration from the frozen v7 workbench into v8."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple

from .storage import DEFAULT_DB, PROJECT_ROOT, connect, initialize_database, now_utc, transaction


LEGACY_DB = PROJECT_ROOT / "app" / "data" / "web_mvp.sqlite3"
BASELINE_PATH = PROJECT_ROOT / "config" / "v8_migration_baseline.json"
TAXONOMY_PATH = PROJECT_ROOT / "config" / "business_selling_points_v4_final.json"
LINK_ALPHABET = "23456789ABCDEFGHJKLMNPQRSTUVWXYZ"
LINK_DIGITS = "23456789"
LINK_LETTERS = "ABCDEFGHJKLMNPQRSTUVWXYZ"


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_json(value: Any) -> str:
    return sha256_bytes(canonical_json(value).encode("utf-8"))


def lastrowid(cursor: sqlite3.Cursor) -> int:
    value = cursor.lastrowid
    if value is None:
        raise RuntimeError("SQLite INSERT did not return a row id")
    return int(value)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def sha256_path(path: Path, cache: Dict[str, Tuple[int, str]]) -> Tuple[int, str]:
    key = str(path.resolve())
    if key in cache:
        return cache[key]
    if path.is_file():
        result = (path.stat().st_size, sha256_file(path))
    elif path.is_dir():
        digest = hashlib.sha256()
        total = 0
        for child in sorted(item for item in path.rglob("*") if item.is_file()):
            relative = child.relative_to(path).as_posix()
            child_size, child_sha = sha256_path(child, cache)
            total += child_size
            digest.update(relative.encode("utf-8"))
            digest.update(b"\0")
            digest.update(child_sha.encode("ascii"))
            digest.update(b"\0")
        result = (total, digest.hexdigest())
    else:
        result = (0, "")
    cache[key] = result
    return result


def normalize_timestamp(raw_value: Any) -> Optional[str]:
    value = str(raw_value or "").strip()
    if not value or value == "0":
        return None
    if len(value) == 10 and value.isdigit():
        parsed = datetime.fromtimestamp(int(value), tz=timezone.utc)
    else:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"unsupported timestamp: {value}") from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        parsed = parsed.astimezone(timezone.utc)
    return parsed.isoformat(timespec="seconds").replace("+00:00", "Z")


def iso_week(timestamp: str) -> str:
    parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    year, week, _ = parsed.isocalendar()
    return f"{year}-W{week:02d}"


def account_type(value: Any) -> str:
    return {
        "精品IP号": "boutique_ip",
        "原创号": "original",
        "混剪号": "mixed_edit",
        "二创矩阵号": "mixed_edit",
    }.get(str(value or "").strip(), "unknown")


def content_direction(value: Any) -> str:
    return {
        "新车": "new_car",
        "二手车": "used_car",
        "媒体-AI小懂": "media",
        "媒体": "media",
        "其他": "other",
    }.get(str(value or "").strip(), "unknown")


def _encode_link_material(material: str) -> str:
    digest = hashlib.sha256(material.encode("utf-8")).digest()
    bits = int.from_bytes(digest[:4], "big") >> 2
    value = "".join(LINK_ALPHABET[(bits >> shift) & 31] for shift in range(25, -1, -5))
    if all(character in LINK_LETTERS for character in value):
        value = value[:-1] + LINK_DIGITS[digest[6] % len(LINK_DIGITS)]
    elif all(character in LINK_DIGITS for character in value):
        value = value[:-1] + LINK_LETTERS[digest[6] % len(LINK_LETTERS)]
    return value


def generate_link_id(connection: sqlite3.Connection, identity: str) -> str:
    nonce = 0
    while True:
        material = identity if nonce == 0 else f"{identity}|{nonce}"
        candidate = _encode_link_material(material)
        row = connection.execute("SELECT 1 FROM content_items WHERE link_id=?", (candidate,)).fetchone()
        alias = connection.execute("SELECT 1 FROM content_aliases WHERE alias_link_id=?", (candidate,)).fetchone()
        if row is None and alias is None:
            return candidate
        nonce += 1


def _read_json(path: Path) -> Mapping[str, Any]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, Mapping) else {}


def _captured_at_for_comments(platform: str, content_id: str, imported_at: str) -> Tuple[str, Path]:
    if platform == "xiaohongshu":
        note_root = PROJECT_ROOT / "data" / "cache" / "rnote" / "notes" / content_id
        collection = _read_json(note_root / "collection.json")
        comments = collection.get("comments") if isinstance(collection.get("comments"), Mapping) else {}
        captured_text = normalize_timestamp(comments.get("collected_at") if comments else None)
        return captured_text or normalize_timestamp(imported_at) or now_utc(), note_root / "comments.jsonl"
    comment_root = PROJECT_ROOT / "data" / "cache" / "tikhub" / "2026-08-02" / "comments" / content_id
    files = [item for item in comment_root.rglob("*") if item.is_file()] if comment_root.exists() else []
    if files:
        captured_time = datetime.fromtimestamp(max(item.stat().st_mtime for item in files), tz=timezone.utc)
        return captured_time.isoformat(timespec="seconds").replace("+00:00", "Z"), comment_root
    return normalize_timestamp(imported_at) or now_utc(), comment_root


def _project_relative(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
    except ValueError:
        return str(path)


def _insert_taxonomy(connection: sqlite3.Connection, captured_at: str) -> None:
    source = json.loads(TAXONOMY_PATH.read_text(encoding="utf-8"))
    taxonomy_id = "selling-points-v5.0"
    connection.execute(
        """
        INSERT INTO taxonomy_versions(
            id, version, status, definition, source_path, source_sha256, created_at, published_at
        ) VALUES (?, ?, 'published', ?, ?, ?, ?, ?)
        """,
        (
            taxonomy_id, taxonomy_id, str(source.get("definition") or ""),
            _project_relative(TAXONOMY_PATH), sha256_file(TAXONOMY_PATH), captured_at, captured_at,
        ),
    )
    scene_map = {"新车": "new_car", "二手车": "used_car", "媒体-AI小懂": "media"}
    for label in source.get("labels") or []:
        cursor = connection.execute(
            """
            INSERT INTO selling_points(taxonomy_id, code, tier, label)
            VALUES (?, ?, ?, ?)
            """,
            (taxonomy_id, label["id"], label["tier"], label["label"]),
        )
        point_id = lastrowid(cursor)
        scenes = label.get("business_scene_options") or [label.get("business_scene")]
        for scene in scenes:
            if scene in scene_map:
                connection.execute(
                    "INSERT INTO selling_point_scenes(selling_point_id, scene) VALUES (?, ?)",
                    (point_id, scene_map[scene]),
                )


def _register_artifact(
    connection: sqlite3.Connection,
    *,
    content_id: int,
    artifact_type: str,
    path: Path,
    status: str,
    legacy_fingerprint: str,
    captured_at: str,
    hash_cache: Dict[str, Tuple[int, str]],
) -> Optional[str]:
    available = status == "available" and path.exists()
    size, digest = sha256_path(path, hash_cache) if available else (None, None)
    connection.execute(
        """
        INSERT INTO evidence_artifacts(
            content_id, artifact_type, local_path, status, byte_size, sha256,
            legacy_fingerprint, captured_at, metadata_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(content_id, artifact_type, local_path) DO UPDATE SET
            status=excluded.status,
            byte_size=excluded.byte_size,
            sha256=excluded.sha256,
            legacy_fingerprint=excluded.legacy_fingerprint,
            captured_at=excluded.captured_at
        """,
        (
            content_id, artifact_type, _project_relative(path), status, size, digest,
            legacy_fingerprint or None, captured_at, "{}", captured_at,
        ),
    )
    return digest


def _evaluation_payload(row: sqlite3.Row) -> Dict[str, Any]:
    excluded = {
        "id", "platform", "platform_content_id", "canonical_url", "source_group",
        "source_label", "account_uid", "account_name", "account_quality", "caption",
        "content_type", "published_at", "exposure_value", "exposure_status", "source_path",
        "source_line", "imported_at",
    }
    return {key: row[key] for key in row.keys() if key not in excluded}


def _file_sha(path: Path) -> str:
    return sha256_file(path) if path.exists() else sha256_json({"missing": str(path)})


def migrate(
    legacy_db: Path = LEGACY_DB,
    target_db: Path = DEFAULT_DB,
    baseline_path: Path = BASELINE_PATH,
) -> Dict[str, Any]:
    baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
    migration_id = baseline["baseline_id"]
    destination = connect(target_db)
    initialize_database(destination)
    existing = destination.execute(
        "SELECT status, summary_json FROM migration_audit WHERE id=?", (migration_id,)
    ).fetchone()
    if existing is not None and existing["status"] == "succeeded":
        destination.close()
        return json.loads(existing["summary_json"])

    source = sqlite3.connect(legacy_db)
    source.row_factory = sqlite3.Row
    captured_at = now_utc()
    hash_cache: Dict[str, Tuple[int, str]] = {}
    summary: Dict[str, Any] = {}
    try:
        with transaction(destination):
            destination.execute(
                """
                INSERT INTO migration_audit(
                    id, baseline_id, source_database, source_sha256, status, started_at
                ) VALUES (?, ?, ?, ?, 'running', ?)
                """,
                (
                    migration_id, migration_id, _project_relative(legacy_db),
                    _file_sha(legacy_db), captured_at,
                ),
            )
            destination.execute(
                """
                INSERT INTO import_batches(
                    id, entity_type, source_name, status, total_rows, created_at
                ) VALUES (?, 'legacy_migration', ?, 'previewed', ?, ?)
                """,
                (migration_id, _project_relative(legacy_db), baseline["content"]["total"], captured_at),
            )
            _insert_taxonomy(destination, captured_at)
            budget = baseline["legacy_media_backfill"]
            destination.execute(
                """
                INSERT INTO provider_budget_batches(
                    id, purpose, provider, operation, currency, verified_unit_price,
                    max_billable_requests, max_amount, pilot_size, daily_quota,
                    price_verified_at, status, created_at, updated_at
                ) VALUES (?, ?, 'Rnote', 'xiaohongshu_video_detail', 'USD', ?, ?, ?, ?, ?, ?, 'draft', ?, ?)
                """,
                (
                    "legacy-xhs-media-backfill-v1", "legacy_media_backfill",
                    budget["unit_price_usd"], budget["paid_refresh_candidates"],
                    budget["hard_budget_usd"], budget["pilot_size"], budget["daily_attempt_quota"],
                    captured_at, captured_at, captured_at,
                ),
            )

            rows = source.execute(
                """
                SELECT c.*, e.*
                FROM content_items c
                JOIN evaluations e ON e.content_item_id=c.id
                ORDER BY c.id
                """
            ).fetchall()
            old_to_new: Dict[int, int] = {}
            evidence_by_content: Dict[int, Dict[str, Optional[str]]] = {}
            weeks: Dict[str, int] = {}
            for source_row_number, row in enumerate(rows, start=1):
                old_id = int(row["id"])
                platform = str(row["platform"])
                platform_content_id = str(row["platform_content_id"])
                identity = f"{platform}:{platform_content_id}"
                link_id = generate_link_id(destination, identity)
                normalized_published = normalize_timestamp(row["published_at"])
                imported_at = normalize_timestamp(row["imported_at"]) or captured_at
                cursor = destination.execute(
                    """
                    INSERT INTO content_items(
                        link_id, platform, platform_content_id, canonical_url,
                        raw_account_uid, raw_account_name, legacy_account_type,
                        title, content_type, published_at, published_at_raw,
                        evaluation_content_direction, source_group, source_label,
                        source_path, source_line, imported_at, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        link_id, platform, platform_content_id, row["canonical_url"],
                        row["account_uid"] or None, row["account_name"] or None,
                        account_type(row["account_quality"]), row["caption"], row["content_type"] or "unknown",
                        normalized_published, row["published_at"] or None,
                        content_direction(row["business_scene"]), row["source_group"], row["source_label"],
                        row["source_path"], row["source_line"], imported_at, imported_at, captured_at,
                    ),
                )
                new_id = lastrowid(cursor)
                old_to_new[old_id] = new_id
                destination.execute(
                    """
                    INSERT INTO content_identities(
                        content_id, identity_kind, identity_value, platform_identity_key, is_primary, created_at
                    ) VALUES (?, 'platform_content_id', ?, ?, 1, ?)
                    """,
                    (new_id, platform_content_id, identity, captured_at),
                )
                destination.execute(
                    """
                    INSERT INTO import_rows(
                        batch_id, source_row, status, entity_id, identity_key, raw_json
                    ) VALUES (?, ?, 'inserted', ?, ?, ?)
                    """,
                    (
                        migration_id, source_row_number, new_id, identity,
                        canonical_json({"source_path": row["source_path"], "source_line": row["source_line"]}),
                    ),
                )
                destination.execute(
                    """
                    INSERT INTO migration_row_audit(
                        migration_id, source_table, source_pk, field_name,
                        raw_value, normalized_value, status, reason
                    ) VALUES (?, 'content_items', ?, 'published_at', ?, ?, ?, ?)
                    """,
                    (
                        migration_id, str(old_id), row["published_at"] or None, normalized_published,
                        "normalized" if normalized_published else "missing",
                        "" if normalized_published else "source timestamp is empty or zero",
                    ),
                )

                detail_status = "succeeded" if normalized_published else "terminal_failed"
                destination.execute(
                    """
                    INSERT INTO fetch_slots(
                        content_id, stage, window_key, provider, adapter_version, status,
                        attempt_count, last_error_code, created_at, updated_at, finished_at
                    ) VALUES (?, 'detail', 'lifetime', 'legacy-cache', 'migration-v1', ?, 1, ?, ?, ?, ?)
                    """,
                    (
                        new_id, detail_status,
                        None if normalized_published else "legacy_detail_incomplete",
                        captured_at, captured_at, captured_at,
                    ),
                )
                metric_status = "succeeded" if row["exposure_value"] is not None else "terminal_failed"
                destination.execute(
                    """
                    INSERT INTO fetch_slots(
                        content_id, stage, window_key, provider, adapter_version, status,
                        attempt_count, last_error_code, created_at, updated_at, finished_at
                    ) VALUES (?, 'metrics', 'legacy', 'legacy-cache', 'migration-v1', ?, 1, ?, ?, ?, ?)
                    """,
                    (
                        new_id, metric_status,
                        None if row["exposure_value"] is not None else "legacy_metrics_missing",
                        captured_at, captured_at, captured_at,
                    ),
                )
                if row["exposure_value"] is not None:
                    destination.execute(
                        """
                        INSERT INTO content_metric_snapshots(
                            content_id, captured_at, window_key, view_count, status, source, metadata_json
                        ) VALUES (?, ?, 'legacy', ?, 'available', 'migrated_historical', ?)
                        """,
                        (
                            new_id, imported_at, int(row["exposure_value"]),
                            canonical_json({"trend_eligible": False, "original_status": row["exposure_status"]}),
                        ),
                    )

                comments_captured_at, comments_path = _captured_at_for_comments(
                    platform, platform_content_id, imported_at
                )
                week = iso_week(comments_captured_at)
                weeks[week] = weeks.get(week, 0) + 1
                comment_size, comment_sha = sha256_path(comments_path, hash_cache)
                destination.execute(
                    """
                    INSERT INTO comment_evidence_versions(
                        content_id, captured_at, iso_week, source, local_path,
                        sha256, status, created_at
                    ) VALUES (?, ?, ?, 'legacy-cache', ?, ?, 'available', ?)
                    """,
                    (
                        new_id, comments_captured_at, week, _project_relative(comments_path),
                        comment_sha, captured_at,
                    ),
                )
                destination.execute(
                    """
                    INSERT INTO fetch_slots(
                        content_id, stage, window_key, provider, adapter_version, status,
                        attempt_count, created_at, updated_at, finished_at
                    ) VALUES (?, 'comments', ?, 'legacy-cache', 'migration-v1', 'succeeded', 1, ?, ?, ?)
                    """,
                    (new_id, week, captured_at, captured_at, captured_at),
                )
                evidence_by_content[new_id] = {"comments_version_sha256": comment_sha}

            asset_rows = source.execute(
                """
                SELECT content_item_id, evidence_type, local_path, status,
                       COALESCE(byte_size, 0) AS byte_size, fingerprint, indexed_at
                FROM evidence_assets
                ORDER BY content_item_id, evidence_type
                """
            ).fetchall()
            for asset in asset_rows:
                new_id = old_to_new[int(asset["content_item_id"])]
                path = PROJECT_ROOT / str(asset["local_path"])
                indexed_at = normalize_timestamp(asset["indexed_at"]) or captured_at
                digest = _register_artifact(
                    destination,
                    content_id=new_id,
                    artifact_type=str(asset["evidence_type"]),
                    path=path,
                    status=str(asset["status"]),
                    legacy_fingerprint=str(asset["fingerprint"] or ""),
                    captured_at=indexed_at,
                    hash_cache=hash_cache,
                )
                if digest:
                    components = evidence_by_content[new_id]
                    artifact_type = str(asset["evidence_type"])
                    if artifact_type in {"provider_content", "public_content"}:
                        components["detail_raw_sha256"] = digest
                    elif artifact_type in {"media"}:
                        components["media_sha256"] = digest
                    elif artifact_type in {"transcript", "media_transcript"}:
                        components["asr_sha256"] = digest
                    elif artifact_type in {"ocr", "media_ocr"}:
                        components["ocr_sha256"] = digest

            evaluation_by_old_id: Dict[int, int] = {}
            for row in rows:
                old_id = int(row["id"])
                new_id = old_to_new[old_id]
                components = evidence_by_content[new_id]
                components["text_sha256"] = sha256_json(
                    {
                        "title": str(row["caption"] or ""),
                        "body": "",
                        "account_uid": str(row["account_uid"] or ""),
                        "account_name": str(row["account_name"] or ""),
                    }
                )
                envelope = {
                    key: components.get(key)
                    for key in (
                        "detail_raw_sha256", "text_sha256", "media_sha256", "asr_sha256",
                        "ocr_sha256", "comments_version_sha256", "manual_evidence_sha256",
                    )
                }
                evidence_sha = sha256_json(envelope)
                envelope_cursor = destination.execute(
                    """
                    INSERT INTO evidence_envelopes(
                        content_id, schema_version, detail_raw_sha256, text_sha256,
                        media_sha256, asr_sha256, ocr_sha256, comments_version_sha256,
                        manual_evidence_sha256, evidence_sha256, components_json, created_at
                    ) VALUES (?, 'evidence-v1', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        new_id, envelope["detail_raw_sha256"], envelope["text_sha256"],
                        envelope["media_sha256"], envelope["asr_sha256"], envelope["ocr_sha256"],
                        envelope["comments_version_sha256"], envelope["manual_evidence_sha256"],
                        evidence_sha, canonical_json(envelope), captured_at,
                    ),
                )
                payload = _evaluation_payload(row)
                evaluation_cursor = destination.execute(
                    """
                    INSERT INTO evaluation_versions(
                        content_id, evidence_envelope_id, rule_version, taxonomy_version,
                        evidence_sha256, evaluation_source, evaluation_status, evidence_level,
                        primary_selling_point_code, selling_point_score, selling_point_included,
                        content_direction, content_automotive_score, audience_automotive_score,
                        acquisition_potential_score, pending_review, payload_json, evaluated_at
                    ) VALUES (?, ?, 'evaluation-v6', 'selling-points-v5.0', ?, 'migrated_from_v5',
                              ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        new_id, lastrowid(envelope_cursor), evidence_sha,
                        row["evaluation_status"], row["evidence_level"],
                        row["primary_selling_point_id"] or None, row["selling_point_score"],
                        int(row["selling_point_included"]), content_direction(row["business_scene"]),
                        row["content_automotive_score"], row["audience_automotive_score"],
                        row["acquisition_potential"], int(row["pending_review"]),
                        canonical_json(payload), normalize_timestamp(row["evaluated_at"]) or captured_at,
                    ),
                )
                evaluation_id = lastrowid(evaluation_cursor)
                evaluation_by_old_id[old_id] = evaluation_id
                primary = str(row["primary_selling_point_id"] or "")
                if primary:
                    destination.execute(
                        """
                        INSERT INTO evaluation_matches(
                            evaluation_id, selling_point_code, match_role, score, evidence_json
                        ) VALUES (?, ?, 'primary', ?, '{}')
                        """,
                        (evaluation_id, primary, row["selling_point_score"]),
                    )
                try:
                    secondary = json.loads(row["secondary_selling_point_ids_json"] or "[]")
                except json.JSONDecodeError:
                    secondary = []
                for code in secondary:
                    if code and code != primary:
                        destination.execute(
                            """
                            INSERT OR IGNORE INTO evaluation_matches(
                                evaluation_id, selling_point_code, match_role, evidence_json
                            ) VALUES (?, ?, 'secondary', '{}')
                            """,
                            (evaluation_id, str(code)),
                        )

                evidence_level = str(row["evidence_level"])
                pending = int(row["pending_review"])
                if pending:
                    if evidence_level == "V0":
                        queue_status, reason = "terminal_failed", "legacy_content_unavailable"
                    elif evidence_level == "V1":
                        local_video = (
                            PROJECT_ROOT / "data" / "cache" / "rnote" / "media"
                            / str(row["platform_content_id"]) / "video.mp4"
                        )
                        reason = (
                            "stale_local_evidence"
                            if local_video.exists() and local_video.stat().st_size > 1024
                            else "media_evidence_missing"
                        )
                        queue_status = "pending"
                    else:
                        queue_status, reason = "pending", "evaluation_gray_zone"
                    destination.execute(
                        """
                        INSERT INTO review_queue(
                            content_id, evaluation_id, reason_code, status,
                            created_at, updated_at, resolved_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            new_id, evaluation_id, reason, queue_status, captured_at, captured_at,
                            captured_at if queue_status == "terminal_failed" else None,
                        ),
                    )

            score_rows = source.execute(
                "SELECT * FROM comment_user_scores ORDER BY content_item_id, anonymous_user_key"
            ).fetchall()
            comment_versions = {
                int(row["content_id"]): int(row["id"])
                for row in destination.execute(
                    "SELECT id, content_id FROM comment_evidence_versions"
                ).fetchall()
            }
            for score in score_rows:
                new_id = old_to_new[int(score["content_item_id"])]
                destination.execute(
                    """
                    INSERT INTO comment_user_scores(
                        content_id, evidence_version_id, anonymous_user_key,
                        audience_automotive_score, action_intent_score, evaluated_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        new_id, comment_versions.get(new_id), score["anonymous_user_key"],
                        score["audience_automotive_score"], score["action_intent_score"],
                        normalize_timestamp(score["evaluated_at"]) or captured_at,
                    ),
                )

            review_rows = source.execute("SELECT * FROM manual_reviews ORDER BY id").fetchall()
            for review in review_rows:
                new_id = old_to_new[int(review["content_item_id"])]
                review_cursor = destination.execute(
                    """
                    INSERT INTO evaluation_reviews(
                        content_id, previous_evaluation_id, decision, reason, reviewer, created_at
                    ) VALUES (?, ?, 'migrated_patch', ?, ?, ?)
                    """,
                    (
                        new_id, evaluation_by_old_id[int(review["content_item_id"])],
                        review["reason"], review["reviewer"],
                        normalize_timestamp(review["created_at"]) or captured_at,
                    ),
                )
                patch_json = str(review["patch_json"])
                destination.execute(
                    """
                    INSERT INTO manual_evidence(
                        review_id, content_id, evidence_type, text_value, sha256, created_at
                    ) VALUES (?, ?, 'legacy_review_patch', ?, ?, ?)
                    """,
                    (
                        lastrowid(review_cursor), new_id, patch_json,
                        sha256_bytes(patch_json.encode("utf-8")),
                        normalize_timestamp(review["created_at"]) or captured_at,
                    ),
                )

            summary = {
                "migration_id": migration_id,
                "content_items": len(rows),
                "evidence_artifacts": int(destination.execute("SELECT COUNT(*) FROM evidence_artifacts").fetchone()[0]),
                "evaluation_versions": int(destination.execute("SELECT COUNT(*) FROM evaluation_versions").fetchone()[0]),
                "comment_evidence_versions": int(destination.execute("SELECT COUNT(*) FROM comment_evidence_versions").fetchone()[0]),
                "comment_user_scores": len(score_rows),
                "comment_weeks": dict(sorted(weeks.items())),
                "review_queue": int(destination.execute("SELECT COUNT(*) FROM review_queue").fetchone()[0]),
                "manual_review_history": len(review_rows),
                "provider_calls": 0,
            }
            destination.execute(
                """
                UPDATE import_batches
                SET status='committed', inserted_rows=?, committed_at=?
                WHERE id=?
                """,
                (len(rows), captured_at, migration_id),
            )
            destination.execute(
                """
                UPDATE migration_audit
                SET status='succeeded', summary_json=?, completed_at=?
                WHERE id=?
                """,
                (canonical_json(summary), captured_at, migration_id),
            )
    finally:
        source.close()
        destination.close()
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--legacy-db", type=Path, default=LEGACY_DB)
    parser.add_argument("--target-db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--baseline", type=Path, default=BASELINE_PATH)
    arguments = parser.parse_args()
    print(json.dumps(migrate(arguments.legacy_db, arguments.target_db, arguments.baseline), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
