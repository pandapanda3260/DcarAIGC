#!/usr/bin/env python3
"""Import a frozen normalized Dongchedi trim catalog (dry-run by default)."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Optional, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = PROJECT_ROOT / "src" / "dcar_eval"
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from v8.spu_catalog_import import (  # noqa: E402
    SpuCatalogImportError,
    execute_import,
)
from v8.storage import DEFAULT_DB  # noqa: E402


DEFAULT_MAPPING = PROJECT_ROOT / "config" / "dongchedi_spu_series_map_v1.json"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="抓取器生成并冻结的 attested normalized JSON",
    )
    parser.add_argument(
        "--mapping",
        type=Path,
        default=DEFAULT_MAPPING,
        help="用于核验来源工件 mapping_sha256 与官方车系白名单",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help="目标 schema-v14 SQLite 数据库（默认正式本地库）",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="正式写入；缺省只生成只读 dry-run 计划",
    )
    parser.add_argument(
        "--expect-plan-sha256",
        default=None,
        help="dry-run 输出的 plan_sha256；--apply 时必填",
    )
    parser.add_argument(
        "--backup-dir",
        type=Path,
        default=None,
        help="在线备份目录（默认目标数据库同级 backups/）",
    )
    parser.add_argument(
        "--skip-backup",
        action="store_true",
        help="仅候选数据库可用；正式 DEFAULT_DB 永久禁止跳过备份",
    )
    parser.add_argument(
        "--receipt",
        type=Path,
        default=None,
        help="可选：将同一份 JSON receipt 原子写入指定路径",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        receipt = execute_import(
            arguments.input,
            db_path=arguments.db,
            apply=bool(arguments.apply),
            expected_plan_sha256=arguments.expect_plan_sha256,
            backup_dir=arguments.backup_dir,
            skip_backup=bool(arguments.skip_backup),
            receipt_path=arguments.receipt,
            mapping_path=arguments.mapping,
        )
    except (SpuCatalogImportError, OSError, sqlite3.Error) as error:
        print(
            json.dumps(
                {
                    "schema_version": "dcar-dongchedi-spu-trim-import-error-v1",
                    "ok": False,
                    "error": str(error),
                },
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
            ),
            file=sys.stderr,
        )
        return 1
    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
