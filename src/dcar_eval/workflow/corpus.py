"""Import the frozen Douyin and Xiaohongshu corpora without transient secrets."""

from __future__ import annotations

import csv
import hashlib
import json
import re
import sqlite3
from pathlib import Path
from typing import Any, Iterable

from .contracts import PROJECT_ROOT
from .storage import now_iso, transaction


DOUYIN_SOURCE = Path("data/inputs/douyin/douyin_30_account_content_sample_2026-08-01.jsonl")
DOUYIN_ENRICHMENT = Path("reports/current/抖音438条内容渠道评估_v6_TikHub补充_2026-08-02.csv")
XHS_UNIQUE_SOURCE = Path("data/inputs/xiaohongshu/notes_unique.csv")
XHS_AUDIT_SOURCE = Path("data/inputs/xiaohongshu/notes_all.csv")
XHS_PROVIDER_CACHE = Path("data/cache/rnote/notes")
XHS_ID_RE = re.compile(r"^[0-9a-f]{24}$", re.I)
DOUYIN_ID_RE = re.compile(r"^\d{10,24}$")


def project_relative(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(path.resolve())


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _positive_int(value: Any) -> int | None:
    try:
        result = int(float(str(value).strip()))
    except (TypeError, ValueError):
        return None
    return result if result > 0 else None


def _upsert_content(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    columns = [
        "platform", "platform_content_id", "canonical_url", "source_group", "source_label",
        "account_uid", "account_name", "account_quality", "caption", "content_type",
        "published_at", "exposure_value", "exposure_status", "source_path", "source_line",
        "imported_at",
    ]
    values = [row.get(column) for column in columns]
    updates = ", ".join(
        f"{column}=excluded.{column}" for column in columns if column not in {"platform", "platform_content_id"}
    )
    connection.execute(
        f"INSERT INTO content_items ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)}) "
        f"ON CONFLICT(platform, platform_content_id) DO UPDATE SET {updates}",
        values,
    )


def import_douyin(connection: sqlite3.Connection, root: Path = PROJECT_ROOT) -> int:
    source = root / DOUYIN_SOURCE
    enrichment_path = root / DOUYIN_ENRICHMENT
    enriched = {
        row["aweme_id"]: row for row in _read_csv(enrichment_path)
    } if enrichment_path.exists() else {}
    rows = _read_jsonl(source)
    seen: set[str] = set()
    imported_at = now_iso()
    with transaction(connection):
        for line_number, raw in enumerate(rows, 1):
            aweme_id = str(raw.get("aweme_id") or "").strip()
            if not DOUYIN_ID_RE.fullmatch(aweme_id):
                raise ValueError(f"invalid Douyin aweme_id at line {line_number}")
            if aweme_id in seen:
                raise ValueError(f"duplicate Douyin aweme_id: {aweme_id}")
            seen.add(aweme_id)
            detail = enriched.get(aweme_id, {})
            exposure = _positive_int(detail.get("play_count_tikhub")) if _truthy(detail.get("play_count_valid")) else None
            _upsert_content(connection, {
                "platform": "douyin",
                "platform_content_id": aweme_id,
                "canonical_url": f"https://www.douyin.com/video/{aweme_id}",
                "source_group": "30-account-random-sample",
                "source_label": "",
                "account_uid": str(raw.get("uid") or ""),
                "account_name": str(raw.get("account_name") or ""),
                "account_quality": str(raw.get("quality_label") or ""),
                "caption": str(raw.get("desc") or ""),
                "content_type": str(raw.get("content_type") or ""),
                "published_at": str(raw.get("create_time_cn") or ""),
                "exposure_value": exposure,
                "exposure_status": "valid" if exposure is not None else "missing",
                "source_path": project_relative(source, root),
                "source_line": line_number,
                "imported_at": imported_at,
            })
    return len(seen)


def import_xiaohongshu(connection: sqlite3.Connection, root: Path = PROJECT_ROOT) -> tuple[int, int]:
    unique_source = root / XHS_UNIQUE_SOURCE
    audit_source = root / XHS_AUDIT_SOURCE
    unique_rows = _read_csv(unique_source)
    audit_rows = _read_csv(audit_source)
    seen: set[str] = set()
    imported_at = now_iso()
    with transaction(connection):
        for line_number, raw in enumerate(unique_rows, 2):
            note_id = str(raw.get("note_id") or "").strip().lower()
            if not XHS_ID_RE.fullmatch(note_id):
                raise ValueError(f"invalid Xiaohongshu note_id at line {line_number}")
            if note_id in seen:
                raise ValueError(f"duplicate Xiaohongshu note_id: {note_id}")
            seen.add(note_id)
            cached_content_path = root / XHS_PROVIDER_CACHE / note_id / "content.json"
            cached_content: dict[str, Any] = {}
            if cached_content_path.exists():
                try:
                    value = json.loads(cached_content_path.read_text(encoding="utf-8"))
                    if isinstance(value, dict):
                        cached_content = value
                except (OSError, json.JSONDecodeError):
                    cached_content = {}
            exposure = _positive_int(raw.get("vv"))
            cached_caption = "\n".join(
                value for value in (
                    str(cached_content.get("title") or "").strip(),
                    str(cached_content.get("desc") or "").strip(),
                ) if value
            )
            _upsert_content(connection, {
                "platform": "xiaohongshu",
                "platform_content_id": note_id,
                "canonical_url": f"https://www.xiaohongshu.com/explore/{note_id}",
                "source_group": "existing-link-corpus",
                "source_label": str(raw.get("gold_label") or ""),
                "account_uid": "",
                "account_name": str(raw.get("account_name") or ""),
                "account_quality": "",
                "caption": cached_caption or str(raw.get("share_title") or ""),
                "content_type": str(cached_content.get("note_type") or ""),
                "published_at": str(cached_content.get("published_at") or ""),
                "exposure_value": exposure,
                "exposure_status": "valid" if exposure is not None else "missing",
                "source_path": project_relative(unique_source, root),
                "source_line": line_number,
                "imported_at": imported_at,
            })
        for line_number, raw in enumerate(audit_rows, 2):
            status = str(raw.get("parse_status") or "unknown")
            reason = ""
            if status == "invalid_creator_page":
                reason = "URL is not a content note"
            elif status.endswith("_duplicate"):
                reason = "duplicate content note"
            connection.execute(
                """
                INSERT INTO content_import_audit(
                    platform, source_path, source_line, platform_content_id, status,
                    duplicate_of, reason, imported_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(platform, source_path, source_line) DO UPDATE SET
                    platform_content_id=excluded.platform_content_id,
                    status=excluded.status,
                    duplicate_of=excluded.duplicate_of,
                    reason=excluded.reason,
                    imported_at=excluded.imported_at
                """,
                (
                    "xiaohongshu", project_relative(audit_source, root), line_number,
                    str(raw.get("note_id") or "").lower(), status,
                    str(raw.get("duplicate_of") or "").lower(), reason, imported_at,
                ),
            )
    return len(seen), len(audit_rows)


def corpus_digest(connection: sqlite3.Connection) -> str:
    rows = connection.execute(
        """
        SELECT platform, platform_content_id, canonical_url, source_group, source_label,
               account_uid, account_name, account_quality, caption, content_type,
               published_at, exposure_value, exposure_status
        FROM content_items ORDER BY platform, platform_content_id
        """
    ).fetchall()
    payload = [dict(row) for row in rows]
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
