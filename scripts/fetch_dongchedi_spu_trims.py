#!/usr/bin/env python3
"""抓取懂车帝近期款型并生成可审计的标准化 JSON；不写数据库。"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = PROJECT_ROOT / "src" / "dcar_eval"
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from v8.dongchedi_spu_source import (  # noqa: E402
    DEFAULT_CITY_NAME,
    DEFAULT_OFFLINE_MIN_MODEL_YEAR,
    DongchediSourceError,
    fetch_normalized_catalog,
    write_json_atomic,
)
from v8.storage import DEFAULT_DB  # noqa: E402


DEFAULT_MAPPING = PROJECT_ROOT / "config" / "dongchedi_spu_series_map_v1.json"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "从懂车帝官网 Web 后端抓取近期款型，标准化后写 JSON。"
            "本命令只读车型库、不写 SQLite。"
        )
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="标准化 JSON 输出路径；先写同目录临时文件再原子替换",
    )
    parser.add_argument("--mapping", type=Path, default=DEFAULT_MAPPING)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--city-name", default=DEFAULT_CITY_NAME)
    parser.add_argument(
        "--offline-min-model-year",
        type=int,
        default=DEFAULT_OFFLINE_MIN_MODEL_YEAR,
    )
    parser.add_argument("--max-model-year", type=int, default=date.today().year)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--retries", type=int, default=3)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    output = args.output.resolve()
    resolved_db = args.db.resolve()
    protected = {
        args.mapping.resolve(),
        resolved_db,
        *(Path(f"{resolved_db}{suffix}") for suffix in ("-wal", "-shm", "-journal")),
    }
    if output in protected:
        print(
            "拒绝执行：--output 不能覆盖映射文件或 SQLite 数据库/sidecar",
            file=sys.stderr,
        )
        return 2
    print(
        "开始抓取：全部当前年及以前在售 + "
        f"{args.offline_min_model_year}–{args.max_model_year} 年停售；"
        "不含预售、未来年款/城市层；不会写数据库。",
        flush=True,
    )
    try:
        payload = fetch_normalized_catalog(
            mapping_path=args.mapping,
            db_path=args.db,
            offline_min_model_year=args.offline_min_model_year,
            max_model_year=args.max_model_year,
            city_name=args.city_name,
            workers=args.workers,
            timeout=args.timeout,
            retries=args.retries,
        )
        write_json_atomic(args.output, payload)
    except (DongchediSourceError, OSError) as exc:
        print(f"抓取失败：{exc}", file=sys.stderr)
        return 1

    summary = payload["summary"]
    print("标准化文件已生成：")
    print(f"  路径：{args.output.resolve()}")
    print(f"  款型：{summary['rows']}")
    print(
        f"  来源层：在售 {summary['online_rows']}，近期停售 {summary['offline_rows']}"
    )
    print(f"  已覆盖本地车系：{summary['resolved_series_slugs']}")
    print(f"  显式未解决车系：{summary['unresolved_series_slugs']}")
    print(f"  内容 SHA-256：{payload['catalog_sha256']}")
    print("提示：来源是官网内部 Web 接口，并非承诺稳定的开放 API。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
