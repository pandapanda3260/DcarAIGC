"""Index reusable local evidence and compute a no-network preflight."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .contracts import PROJECT_ROOT
from .corpus import project_relative
from .storage import now_iso, transaction


DOUYIN_TYPES = ("media", "transcript", "ocr", "provider_statistics", "comments")
XHS_TYPES = (
    "public_screen", "public_content", "provider_content", "comments",
    "media_manifest", "media_ocr", "media_transcript",
)


def _path_fingerprint(path: Path) -> tuple[int | None, str]:
    if not path.exists():
        return None, ""
    if path.is_dir():
        files = sorted(item for item in path.rglob("*") if item.is_file())
        size = sum(item.stat().st_size for item in files)
        material = "|".join(f"{item.name}:{item.stat().st_size}:{item.stat().st_mtime_ns}" for item in files)
    else:
        stat = path.stat()
        size = stat.st_size
        material = f"{path.name}:{stat.st_size}:{stat.st_mtime_ns}"
    return size, hashlib.sha256(material.encode("utf-8")).hexdigest()


def _upsert_asset(
    connection: sqlite3.Connection,
    *,
    content_item_id: int,
    evidence_type: str,
    path: Path,
    root: Path,
) -> None:
    available = path.exists() and (not path.is_dir() or any(item.is_file() for item in path.rglob("*")))
    size, fingerprint = _path_fingerprint(path) if available else (None, "")
    connection.execute(
        """
        INSERT INTO evidence_assets(
            content_item_id, evidence_type, local_path, status, byte_size, fingerprint, indexed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(content_item_id, evidence_type) DO UPDATE SET
            local_path=excluded.local_path,
            status=excluded.status,
            byte_size=excluded.byte_size,
            fingerprint=excluded.fingerprint,
            indexed_at=excluded.indexed_at
        """,
        (
            content_item_id, evidence_type, project_relative(path, root),
            "available" if available else "missing", size, fingerprint, now_iso(),
        ),
    )


def index_evidence(connection: sqlite3.Connection, root: Path = PROJECT_ROOT) -> int:
    rows = connection.execute(
        "SELECT id, platform, platform_content_id FROM content_items ORDER BY id"
    ).fetchall()
    douyin_stats = root / "reports/current/抖音438条内容渠道评估_v6_TikHub补充_2026-08-02.csv"
    with transaction(connection):
        for row in rows:
            item_id = int(row["id"])
            content_id = str(row["platform_content_id"])
            if row["platform"] == "douyin":
                paths = {
                    "media": root / f"data/cache/douyin_media/media/{content_id}.mp4",
                    "transcript": root / f"data/cache/douyin_media/transcripts/{content_id}.json",
                    "ocr": root / f"data/cache/douyin_media/ocr/{content_id}.json",
                    "provider_statistics": douyin_stats,
                    "comments": root / f"data/cache/tikhub/2026-08-02/comments/{content_id}",
                }
            else:
                note_root = root / f"data/cache/rnote/notes/{content_id}"
                paths = {
                    "public_screen": note_root / "public_screen.json",
                    "public_content": note_root / "public_content.json",
                    "provider_content": note_root / "content.json",
                    "comments": note_root / "comments.jsonl",
                    "media_manifest": root / f"data/cache/rnote/media/{content_id}/manifest.json",
                    "media_ocr": root / f"data/cache/rnote/media/{content_id}/ocr.json",
                    "media_transcript": root / f"data/cache/rnote/media/{content_id}/transcript.json",
                }
            for evidence_type, path in paths.items():
                _upsert_asset(
                    connection,
                    content_item_id=item_id,
                    evidence_type=evidence_type,
                    path=path,
                    root=root,
                )
    return len(rows)


def preflight(connection: sqlite3.Connection) -> dict[str, Any]:
    result: dict[str, Any] = {"mode": "cache_index_only", "provider_calls": 0, "channels": {}}
    for platform, evidence_types in (("douyin", DOUYIN_TYPES), ("xiaohongshu", XHS_TYPES)):
        total = int(connection.execute(
            "SELECT COUNT(*) FROM content_items WHERE platform = ?", (platform,)
        ).fetchone()[0])
        evidence: dict[str, Any] = {}
        for evidence_type in evidence_types:
            available = int(connection.execute(
                """
                SELECT COUNT(*) FROM evidence_assets e
                JOIN content_items c ON c.id = e.content_item_id
                WHERE c.platform = ? AND e.evidence_type = ? AND e.status = 'available'
                """,
                (platform, evidence_type),
            ).fetchone()[0])
            evidence[evidence_type] = {
                "available": available,
                "missing": total - available,
                "coverage_percentage": round(available * 100 / total, 2) if total else None,
            }
        result["channels"][platform] = {"content_items": total, "evidence": evidence}
    xhs = result["channels"]["xiaohongshu"]["evidence"]
    result["paid_refresh_gap"] = {
        "xiaohongshu_provider_content_missing": xhs["provider_content"]["missing"],
        "xiaohongshu_comments_missing": xhs["comments"]["missing"],
        "note": "Counts only; no monetary estimate and no provider call was made.",
    }
    return result


def evidence_digest(connection: sqlite3.Connection) -> str:
    rows = connection.execute(
        """
        SELECT c.platform, c.platform_content_id, e.evidence_type, e.status, e.fingerprint
        FROM evidence_assets e JOIN content_items c ON c.id = e.content_item_id
        ORDER BY c.platform, c.platform_content_id, e.evidence_type
        """
    ).fetchall()
    payload = [dict(row) for row in rows]
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def save_snapshot(connection: sqlite3.Connection, summary: dict[str, Any]) -> str:
    content_sha = _query_digest(connection, "content_items")
    evidence_sha = evidence_digest(connection)
    snapshot_id = f"corpus-{content_sha[:12]}-{evidence_sha[:12]}"
    counts = {
        row["platform"]: int(row["count"])
        for row in connection.execute(
            "SELECT platform, COUNT(*) AS count FROM content_items GROUP BY platform"
        ).fetchall()
    }
    connection.execute(
        """
        INSERT OR REPLACE INTO corpus_snapshots(
            id, created_at, douyin_count, xiaohongshu_count,
            content_sha256, evidence_sha256, summary_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            snapshot_id, now_iso(), counts.get("douyin", 0), counts.get("xiaohongshu", 0),
            content_sha, evidence_sha, json.dumps(summary, ensure_ascii=False, sort_keys=True),
        ),
    )
    connection.commit()
    return snapshot_id


def _query_digest(connection: sqlite3.Connection, table: str) -> str:
    if table != "content_items":
        raise ValueError("unsupported digest table")
    rows = connection.execute(
        """
        SELECT platform, platform_content_id, canonical_url, source_group, source_label,
               account_uid, account_name, account_quality, caption, content_type,
               published_at, exposure_value, exposure_status
        FROM content_items ORDER BY platform, platform_content_id
        """
    ).fetchall()
    return hashlib.sha256(
        json.dumps([dict(row) for row in rows], ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
