#!/usr/bin/env python3
"""Backfill interaction users for pre-v8.4 comments via v1 fallback keys.

背景（2026-08-07 决策，Mark）
------------------------------
v8.4 之前采集的评论只有内容级匿名键 ``content-user-hmac-v1``，原始平台 UID
按隐私设计从未落盘，平台级 ``platform-user-hmac-v2`` 键永远无法重建。这让
上周等历史窗口的互动用户全集 U 恒为 0，"互动用户汽车兴趣占比"长期显示
"有效样本不足"。

本脚本把这些评论按 v1 键回填成 ``interaction_users``（key_version 如实标注
``content-user-hmac-v1``），并回填 ``comments.interaction_user_id``。口径说明：
v1 键是内容级的，同一人评论多条内容会计为多个用户（同一内容内正常去重）；
audience_quality.user_key_version 会如实标注混合口径。新采集不受影响，仍只写
v2 键。

前置：schema v11（interaction-user-v1-fallback-keys）。``--apply`` 时若数据库
还在 v10，会先自动执行 v11 迁移（storage.initialize_database），再回填。

Operator flow（生产由 Mark 执行）:
    # 1) 停 web 服务
    python3 scripts/backfill_legacy_interaction_identity.py --db app/data/dcar_insight.sqlite3           # 干跑
    python3 scripts/backfill_legacy_interaction_identity.py --db app/data/dcar_insight.sqlite3 --apply   # 备份+迁移+回填
    python3 scripts/run_audience_classifier.py --db app/data/dcar_insight.sqlite3 --apply                # 补分类
    # 2) 重启服务
"""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Sequence
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = PROJECT_ROOT / "src" / "dcar_eval"
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from v8.storage import (  # type: ignore[import-not-found,import-untyped]  # noqa: E402
    connect,
    initialize_database,
    now_utc,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")
FALLBACK_KEY_VERSION = "content-user-hmac-v1"

_CANDIDATES_SQL = """
FROM comments c
JOIN comment_evidence_versions cev ON cev.id = c.evidence_version_id
JOIN content_items ci ON ci.id = cev.content_id
WHERE c.interaction_user_id IS NULL
  AND c.anonymous_user_key IS NOT NULL
  AND c.anonymous_user_key <> ''
"""


def _candidate_stats(connection: sqlite3.Connection) -> Dict[str, Any]:
    rows = connection.execute(
        f"""
        SELECT ci.platform,
               COUNT(*) comments,
               SUM(CASE WHEN c.parent_comment_id IS NULL THEN 1 ELSE 0 END) first_level,
               COUNT(DISTINCT ci.platform || '|' || c.anonymous_user_key) distinct_v1_keys
        {_CANDIDATES_SQL}
        GROUP BY ci.platform ORDER BY ci.platform
        """
    ).fetchall()
    return {"per_platform": [dict(row) for row in rows]}


def _backup(db_path: Path, backup_dir: Path) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(SHANGHAI).strftime("%Y%m%dT%H%M%S.%f%z")
    target = backup_dir / f"{db_path.stem}.before_v1_identity_backfill.{stamp}{db_path.suffix}"
    if target.exists():
        raise SystemExit(f"backup target already exists: {target}")
    shutil.copy2(db_path, target)
    for suffix in ("-wal", "-shm"):
        sidecar = db_path.with_name(db_path.name + suffix)
        if sidecar.exists():
            shutil.copy2(sidecar, target.with_name(target.name + suffix))
    return target


def backfill(db_path: Path, *, apply: bool, backup_dir: Path) -> Dict[str, Any]:
    if not db_path.exists():
        raise SystemExit(f"database not found: {db_path}")
    summary: Dict[str, Any] = {
        "command": "backfill-legacy-interaction-identity",
        "db": str(db_path),
        "mode": "apply" if apply else "dry_run",
        "fallback_key_version": FALLBACK_KEY_VERSION,
    }
    if not apply:
        connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            summary["candidates"] = _candidate_stats(connection)
            summary["ok"] = True
            summary["hint"] = "re-run with --apply to migrate schema (if needed) and backfill"
            return summary
        finally:
            connection.close()

    summary["backup"] = str(_backup(db_path, backup_dir))
    connection = connect(db_path)
    try:
        initialize_database(connection)  # applies v11 migration when pending
        summary["candidates_before"] = _candidate_stats(connection)
        captured_at = now_utc()
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            f"""
            INSERT INTO interaction_users(
                platform, pseudonymous_user_key, key_version,
                first_seen_at, last_seen_at
            )
            SELECT ci.platform, c.anonymous_user_key, ?,
                   MIN(COALESCE(c.published_at, cev.captured_at, ?)),
                   MAX(COALESCE(c.published_at, cev.captured_at, ?))
            {_CANDIDATES_SQL}
            GROUP BY ci.platform, c.anonymous_user_key
            ON CONFLICT(platform, key_version, pseudonymous_user_key) DO UPDATE SET
                first_seen_at=MIN(interaction_users.first_seen_at, excluded.first_seen_at),
                last_seen_at=MAX(interaction_users.last_seen_at, excluded.last_seen_at)
            """,
            (FALLBACK_KEY_VERSION, captured_at, captured_at),
        )
        cursor = connection.execute(
            """
            UPDATE comments SET interaction_user_id = (
                SELECT iu.id
                FROM interaction_users iu
                JOIN comment_evidence_versions cev ON cev.id = comments.evidence_version_id
                JOIN content_items ci ON ci.id = cev.content_id
                WHERE iu.platform = ci.platform
                  AND iu.key_version = ?
                  AND iu.pseudonymous_user_key = comments.anonymous_user_key
            )
            WHERE interaction_user_id IS NULL
              AND anonymous_user_key IS NOT NULL
              AND anonymous_user_key <> ''
              AND EXISTS (
                  SELECT 1 FROM comment_evidence_versions cev2
                  WHERE cev2.id = comments.evidence_version_id
              )
            """,
            (FALLBACK_KEY_VERSION,),
        )
        remaining = connection.execute(
            f"SELECT COUNT(*) {_CANDIDATES_SQL}"
        ).fetchone()[0]
        if int(remaining) != 0:
            connection.rollback()
            raise SystemExit(f"backfill verification failed: {remaining} comments still unlinked")
        orphan_users = connection.execute(
            """
            SELECT COUNT(*) FROM interaction_users iu
            WHERE iu.key_version = ?
              AND NOT EXISTS (
                  SELECT 1 FROM comments c WHERE c.interaction_user_id = iu.id
              )
            """,
            (FALLBACK_KEY_VERSION,),
        ).fetchone()[0]
        connection.commit()
        check = connection.execute("PRAGMA quick_check").fetchone()[0]
        if str(check) != "ok":
            raise SystemExit(f"quick_check failed after commit: {check}")
        summary.update(
            {
                "ok": True,
                "comments_linked": cursor.rowcount,
                "fallback_users_without_comments": int(orphan_users),
                "quick_check": str(check),
                "users_by_key_version": [
                    dict(row)
                    for row in connection.execute(
                        """
                        SELECT platform, key_version, COUNT(*) users
                        FROM interaction_users GROUP BY 1, 2 ORDER BY 1, 2
                        """
                    )
                ],
                "next_step": "python3 scripts/run_audience_classifier.py --db <db> --apply",
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
        help="sqlite database (default: app/data/dcar_insight.sqlite3)",
    )
    parser.add_argument(
        "--backup-dir",
        type=Path,
        default=PROJECT_ROOT / "app" / "data" / "backups",
        help="pre-backfill copy location (default: app/data/backups)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="run schema migration (if pending) and write the backfill",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    summary = backfill(
        args.db.resolve(), apply=bool(args.apply), backup_dir=args.backup_dir.resolve()
    )
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2))
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
