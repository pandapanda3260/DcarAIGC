#!/usr/bin/env python3
"""One-time repair: normalize douyin first-level comment parent markers.

Defect
------
TikHub douyin comment payloads mark first-level comments with
``reply_id = "0"``. The pre-fix sanitizer persisted that literal into
``comments.parent_comment_id``, while every first-level filter in the
interaction-user domain (``audience_rate``, ``audience_classifier``) selects
``parent_comment_id IS NULL``. Result: the whole douyin user universe U
collapsed to 0 and ``automotive_user_rate`` reported ``missing``
(页面显示"有效样本不足"), even for windows with hundreds of captured
first-level comments.

This script rewrites the historical rows once; the sanitizer fix
(``identity.normalized_parent_comment_id``) keeps new captures clean.

Scope and safety
----------------
* Only rows with ``parent_comment_id IN ('', '0')`` are touched — '0' can
  never be a real comment id on either platform (douyin cids are int64
  strings, xiaohongshu ids are hex strings).
* ``parent_comment_id`` participates in no UNIQUE constraint, so the update
  cannot collide.
* Dry-run by default and read-only in that mode; ``--apply`` copies the
  database (plus -wal/-shm sidecars) to the backup directory first, runs one
  IMMEDIATE transaction, re-verifies zero markers remain and finishes with
  ``PRAGMA quick_check``. Any failed check rolls back.

Operator flow (production, per v8.4 runbook: operations are Mark's)
-------------------------------------------------------------------
1. Stop the web service.
2. ``python3 scripts/repair_comment_parent_zero.py --db app/data/dcar_insight.sqlite3``
   (inspect the dry-run JSON)
3. Re-run with ``--apply``.
4. Restart the service; douyin 互动用户汽车兴趣占比 should now show
   覆盖不足/暂不发布 (below_threshold, 定标前的正确形态) instead of
   有效样本不足 (missing).
"""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Sequence
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SHANGHAI = ZoneInfo("Asia/Shanghai")
NO_PARENT_MARKERS = ("", "0")


def _connect(db_path: Path, *, read_only: bool) -> sqlite3.Connection:
    if read_only:
        connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    else:
        connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection


def _utc_text(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _overview_windows(now: datetime) -> Dict[str, tuple[str, str]]:
    current = now.astimezone(SHANGHAI)
    today = datetime.combine(current.date(), time.min, tzinfo=SHANGHAI)
    this_week = today - timedelta(days=today.weekday())
    bounds = {
        "yesterday": (today - timedelta(days=1), today),
        "this_week": (this_week, current),
        "last_week": (this_week - timedelta(days=7), this_week),
    }
    return {k: (_utc_text(s), _utc_text(e)) for k, (s, e) in bounds.items()}


def _marker_counts(connection: sqlite3.Connection) -> Dict[str, Any]:
    rows = connection.execute(
        """
        SELECT ci.platform,
               SUM(CASE WHEN c.parent_comment_id='0' THEN 1 ELSE 0 END) zero_marker,
               SUM(CASE WHEN c.parent_comment_id='' THEN 1 ELSE 0 END) empty_marker,
               SUM(CASE WHEN c.parent_comment_id IS NULL THEN 1 ELSE 0 END) already_null,
               COUNT(*) total
        FROM comments c
        JOIN comment_evidence_versions cev ON cev.id=c.evidence_version_id
        JOIN content_items ci ON ci.id=cev.content_id
        GROUP BY ci.platform
        """
    ).fetchall()
    orphan = connection.execute(
        """
        SELECT COUNT(*) FROM comments
        WHERE parent_comment_id IN ('', '0')
          AND evidence_version_id NOT IN (SELECT id FROM comment_evidence_versions)
        """
    ).fetchone()[0]
    return {
        "per_platform": [dict(row) for row in rows],
        "orphan_marker_rows": int(orphan),
    }


def _window_user_universe(connection: sqlite3.Connection) -> Dict[str, Any]:
    """Distinct first-level interaction users per overview window/platform."""

    result: Dict[str, Any] = {}
    for window, (start, end) in _overview_windows(datetime.now(SHANGHAI)).items():
        per_platform = {}
        for row in connection.execute(
            """
            SELECT ci.platform,
                   COUNT(DISTINCT CASE WHEN c.parent_comment_id IS NULL
                         THEN c.interaction_user_id END) u,
                   SUM(CASE WHEN c.parent_comment_id IS NULL THEN 1 ELSE 0 END) l1,
                   SUM(CASE WHEN c.parent_comment_id IS NULL
                        AND c.interaction_user_id IS NOT NULL THEN 1 ELSE 0 END) l1_stable
            FROM comments c
            JOIN comment_evidence_versions cev ON cev.id=c.evidence_version_id
            JOIN content_items ci ON ci.id=cev.content_id
            WHERE ci.published_at >= ? AND ci.published_at < ?
            GROUP BY ci.platform
            """,
            (start, end),
        ):
            l1 = int(row["l1"] or 0)
            stable = int(row["l1_stable"] or 0)
            per_platform[str(row["platform"])] = {
                "first_level_comments": l1,
                "first_level_with_identity": stable,
                "identity_coverage_percentage": (
                    round(stable * 100 / l1, 2) if l1 else None
                ),
                "distinct_users": int(row["u"] or 0),
            }
        result[window] = per_platform
    return result


def _backup(db_path: Path, backup_dir: Path) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(SHANGHAI).strftime("%Y%m%dT%H%M%S%z")
    target = backup_dir / f"{db_path.stem}.before_parent_zero_repair.{stamp}{db_path.suffix}"
    if target.exists():
        raise SystemExit(f"backup target already exists: {target}")
    shutil.copy2(db_path, target)
    for suffix in ("-wal", "-shm"):
        sidecar = db_path.with_name(db_path.name + suffix)
        if sidecar.exists():
            shutil.copy2(sidecar, target.with_name(target.name + suffix))
    return target


def repair(db_path: Path, *, apply: bool, backup_dir: Path) -> Dict[str, Any]:
    if not db_path.exists():
        raise SystemExit(f"database not found: {db_path}")
    connection = _connect(db_path, read_only=not apply)
    try:
        before = _marker_counts(connection)
        affected = sum(
            int(row["zero_marker"] or 0) + int(row["empty_marker"] or 0)
            for row in before["per_platform"]
        ) + before["orphan_marker_rows"]
        summary: Dict[str, Any] = {
            "command": "repair-comment-parent-zero",
            "db": str(db_path),
            "mode": "apply" if apply else "dry_run",
            "rows_to_normalize": affected,
            "before": before,
        }
        if not apply:
            summary["ok"] = True
            summary["hint"] = "re-run with --apply to write (backup is automatic)"
            return summary

        backup_path = _backup(db_path, backup_dir)
        summary["backup"] = str(backup_path)
        connection.execute("BEGIN IMMEDIATE")
        cursor = connection.execute(
            "UPDATE comments SET parent_comment_id=NULL"
            " WHERE parent_comment_id IN (?, ?)",
            NO_PARENT_MARKERS,
        )
        remaining = connection.execute(
            "SELECT COUNT(*) FROM comments WHERE parent_comment_id IN (?, ?)",
            NO_PARENT_MARKERS,
        ).fetchone()[0]
        if int(remaining) != 0:
            connection.rollback()
            raise SystemExit(f"post-update verification failed: {remaining} markers left")
        connection.commit()
        check = connection.execute("PRAGMA quick_check").fetchone()[0]
        if str(check) != "ok":
            raise SystemExit(f"quick_check failed after commit: {check} (backup: {backup_path})")
        summary.update(
            {
                "ok": True,
                "rows_normalized": cursor.rowcount,
                "quick_check": str(check),
                "after": _marker_counts(connection),
                "window_user_universe_after": _window_user_universe(connection),
            }
        )
        return summary
    finally:
        connection.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--db",
        type=Path,
        default=PROJECT_ROOT / "app" / "data" / "dcar_insight.sqlite3",
        help="sqlite database to repair (default: app/data/dcar_insight.sqlite3)",
    )
    parser.add_argument(
        "--backup-dir",
        type=Path,
        default=PROJECT_ROOT / "app" / "data" / "backups",
        help="where the pre-repair copy is written (default: app/data/backups)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write the normalization (default: read-only dry run)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    summary = repair(
        args.db.resolve(), apply=bool(args.apply), backup_dir=args.backup_dir.resolve()
    )
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2))
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
