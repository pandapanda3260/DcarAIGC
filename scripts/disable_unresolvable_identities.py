#!/usr/bin/env python3
"""Disable accounts whose only platform identity is unresolvable upstream.

背景（2026-08-07）：`discovery_coverage` 门槛为 100%，但 3 个手工录入的抖音
UID 在 TikHub 上游持续返回 `10001`（查无此账号——UID 录错或账号已注销），
discovery 永远不可能成功，导致每份报告的发现覆盖卡在 <100%、终态只能是
"部分完成"。

本脚本只停用满足**全部**安全条件的账号（enabled -> 0）：

1. 账号当前 enabled=1；
2. 该账号从未有过成功的 discovery 抓取；
3. 最近一次 discovery 错误信息包含指定的上游错误码（默认 10001）；
4. 该账号名下 0 条内容、且只有这一个平台身份（停用无任何连带影响）。

修好 UID 后在账号页重新启用即可恢复监控。Dry-run by default; ``--apply``
写入（自动备份）。
"""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Sequence
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SHANGHAI = ZoneInfo("Asia/Shanghai")

_CANDIDATE_SQL = """
SELECT a.id account_id, a.operator_name, api.platform, api.uid, api.nickname,
       (SELECT fs.last_error_message FROM fetch_slots fs
         WHERE fs.account_id=a.id AND fs.stage='discovery'
         ORDER BY fs.id DESC LIMIT 1) last_error
FROM accounts a
JOIN account_platform_identities api ON api.account_id=a.id
WHERE a.enabled=1
  AND NOT EXISTS (
      SELECT 1 FROM fetch_slots fs
      WHERE fs.account_id=a.id AND fs.stage='discovery' AND fs.status='succeeded'
  )
  AND (SELECT COUNT(*) FROM account_platform_identities x
        WHERE x.account_id=a.id) = 1
  AND (SELECT COUNT(*) FROM content_items c WHERE c.account_id=a.id) = 0
"""


def _now_utc_text() -> str:
    return (
        datetime.now(tz=ZoneInfo("UTC")).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def _candidates(connection: sqlite3.Connection, error_marker: str) -> list:
    rows = [dict(row) for row in connection.execute(_CANDIDATE_SQL)]
    return [
        row for row in rows if error_marker in str(row.get("last_error") or "")
    ]


def _backup(db_path: Path, backup_dir: Path) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(SHANGHAI).strftime("%Y%m%dT%H%M%S.%f%z")
    target = backup_dir / f"{db_path.stem}.before_identity_disable.{stamp}{db_path.suffix}"
    shutil.copy2(db_path, target)
    for suffix in ("-wal", "-shm"):
        sidecar = db_path.with_name(db_path.name + suffix)
        if sidecar.exists():
            shutil.copy2(sidecar, target.with_name(target.name + suffix))
    return target


def run(db_path: Path, *, apply: bool, backup_dir: Path, error_marker: str) -> Dict[str, Any]:
    if not db_path.exists():
        raise SystemExit(f"database not found: {db_path}")
    summary: Dict[str, Any] = {
        "command": "disable-unresolvable-identities",
        "db": str(db_path),
        "mode": "apply" if apply else "dry_run",
        "error_marker": error_marker,
    }
    if not apply:
        connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            summary["candidates"] = _candidates(connection, error_marker)
            summary["ok"] = True
            summary["hint"] = "re-run with --apply to disable these accounts (backup automatic)"
            return summary
        finally:
            connection.close()

    summary["backup"] = str(_backup(db_path, backup_dir))
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("BEGIN IMMEDIATE")
        candidates = _candidates(connection, error_marker)
        stamp = _now_utc_text()
        for row in candidates:
            connection.execute(
                "UPDATE accounts SET enabled=0, updated_at=? WHERE id=? AND enabled=1",
                (stamp, row["account_id"]),
            )
        connection.commit()
        summary.update(
            {
                "ok": True,
                "disabled": candidates,
                "disabled_count": len(candidates),
                "note": "修正 UID 后在账号页重新启用即可恢复监控",
            }
        )
        return summary
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--db",
        type=Path,
        default=PROJECT_ROOT / "app" / "data" / "dcar_insight.sqlite3",
    )
    parser.add_argument(
        "--backup-dir",
        type=Path,
        default=PROJECT_ROOT / "app" / "data" / "backups",
    )
    parser.add_argument(
        "--error-marker",
        default="10001",
        help="仅停用最近 discovery 错误包含该标记的账号（默认 TikHub 10001）",
    )
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    summary = run(
        args.db.resolve(),
        apply=bool(args.apply),
        backup_dir=args.backup_dir.resolve(),
        error_marker=str(args.error_marker),
    )
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2))
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
