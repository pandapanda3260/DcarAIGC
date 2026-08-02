"""Build or refresh the local corpus and reusable evidence index."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .cache_index import index_evidence, preflight, save_snapshot
from .contracts import PROJECT_ROOT
from .corpus import import_douyin, import_xiaohongshu
from .storage import connect, migrate


DEFAULT_DB = PROJECT_ROOT / "app/data/web_mvp.sqlite3"


def bootstrap(db_path: Path = DEFAULT_DB, root: Path = PROJECT_ROOT) -> dict:
    with connect(db_path) as connection:
        schema_version = migrate(connection)
        douyin_count = import_douyin(connection, root)
        xhs_count, xhs_audit_count = import_xiaohongshu(connection, root)
        indexed_count = index_evidence(connection, root)
        summary = preflight(connection)
        snapshot_id = save_snapshot(connection, summary)
    return {
        "schema_version": schema_version,
        "snapshot_id": snapshot_id,
        "douyin_count": douyin_count,
        "xiaohongshu_count": xhs_count,
        "xiaohongshu_audit_count": xhs_audit_count,
        "indexed_content_count": indexed_count,
        "preflight": summary,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = bootstrap(args.db)
    payload = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload, encoding="utf-8")
    print(payload, end="")


if __name__ == "__main__":
    main()

