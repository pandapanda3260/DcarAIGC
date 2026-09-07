"""Bounded, live-WAL content reads for the authenticated workbench BFF.

Reuse the isolated API's query semantics without entering its application
lifespan. This process never owns a Writer lease or starts capture/schedulers.
"""
from __future__ import annotations

import sys
sys.dont_write_bytecode = True

import argparse
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import signal
from typing import Any

MAX_INPUT_BYTES = 8192
QUERY_TIMEOUT_SECONDS = 6.0
PROCESS_TIMEOUT_SECONDS = 10


class RequestError(ValueError):
    pass


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise RequestError("查询字段不能重复。")
        result[key] = value
    return result


def read_request(stream: Any) -> dict[str, Any]:
    body = stream.read(MAX_INPUT_BYTES + 1)
    if len(body) > MAX_INPUT_BYTES:
        raise RequestError("查询条件过长，请缩短后重试。")
    try:
        value = json.loads(
            body.decode("utf-8"), object_pairs_hook=_unique_object,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
    except RequestError:
        raise
    except (ValueError, UnicodeError, RecursionError) as error:
        raise RequestError("查询条件格式无效。") from error
    if not isinstance(value, dict):
        raise RequestError("查询条件必须是对象。")
    return value


def load_backend(backend_root: Path, db_path: Path, project_root: Path) -> tuple[Any, Any]:
    root = backend_root.resolve(strict=True)
    api_file = root / "src/dcar_eval/v8/api.py"
    if not api_file.is_file():
        raise RuntimeError("backend unavailable")
    # Never inherit a writer/scheduler mode from the page-service environment.
    os.environ.update({
        "DCAR_READ_ONLY": "1", "DCAR_SCHEDULER_ENABLED": "0",
        "DCAR_STARTUP_CATCHUP_ENABLED": "0", "DCAR_SCHEDULER_START_PAUSED": "0",
        "DCAR_V8_ISOLATED_CANDIDATE": "0", "DCAR_LLM_DISABLED": "1",
        "DCAR_V8_DB": str(db_path), "DCAR_PROJECT_ROOT": str(project_root),
    })
    os.environ.pop("DCAR_DAILY_CAPTURE_RECONCILE_FROM", None)
    sys.path[:0] = [str(root / "src/dcar_eval"), str(root)]
    # Import constructs route definitions and hashes candidate code; no lifespan
    # is entered and no database connection or executor is started by it.
    with redirect_stdout(io.StringIO()):
        from v8 import api, storage
    if Path(api.__file__).resolve() != api_file.resolve():
        raise RuntimeError("unexpected backend source")
    return api, storage


def validate_request(value: dict[str, Any], api: Any) -> Any:
    if set(value) - set(api.ContentSearchRequest.model_fields):
        raise RequestError("查询条件包含不支持的字段。")
    try:
        payload = api.ContentSearchRequest.model_validate(value, strict=True)
    except ValueError as error:
        # Keep field inputs and internal validation details out of the response.
        messages = [item.get("msg", "") for item in error.errors()] if hasattr(error, "errors") else []
        for message in messages:
            for friendly in (
                "开始日期不能晚于结束日期。",
                "发布时间日期无效，请使用 YYYY-MM-DD 格式的有效日期。",
                "发布时间日期超出可查询范围。",
            ):
                if friendly in message:
                    raise RequestError(friendly) from error
        raise RequestError("查询条件无效，请检查字段格式和分页参数。") from error
    if (payload.page - 1) * payload.page_size > 9223372036854775807:
        raise RequestError("页码超出可查询范围。")
    return payload


def search(payload: Any, db_path: Path, api: Any, storage: Any) -> dict[str, Any]:
    # The DB is always read-only. Media availability retains the local gateway's
    # ledger projection; its media routes still recheck authorization and bytes.
    with storage.live_wal_read_only_connections():
        return api._content_search(
            payload, db_path=db_path, read_only=True,
            local_media_read_only=False, query_timeout_seconds=QUERY_TIMEOUT_SECONDS,
        )


def verify_database(db_path: Path, project_root: Path) -> None:
    from v8.runtime_database import DatabaseAccessMode, resolve_installed_database_access
    resolve_installed_database_access(
        DatabaseAccessMode.FORMAL_READ, database=db_path, project_root=project_root,
    )


def _timeout(_signal: int, _frame: Any) -> None:
    raise TimeoutError("content search timed out")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--backend-root", type=Path, required=True)
    parser.add_argument("--project-root", type=Path, required=True)
    args = parser.parse_args(argv)
    signal.signal(signal.SIGALRM, _timeout)
    signal.alarm(PROCESS_TIMEOUT_SECONDS)
    status = 0
    try:
        value = read_request(sys.stdin.buffer)
        if not all(path.is_absolute() for path in (args.db, args.backend_root, args.project_root)):
            raise RuntimeError("absolute configuration paths required")
        api, storage = load_backend(args.backend_root, args.db, args.project_root)
        payload = validate_request(value, api)
        verify_database(args.db, args.project_root)
        result = search(payload, args.db, api, storage)
        output = json.dumps(result, ensure_ascii=False, allow_nan=False)
    except RequestError as error:
        status = 1
        output = json.dumps({"error": {"status": 422, "detail": str(error)}}, ensure_ascii=False)
    except Exception:
        status = 1
        output = json.dumps({"error": {"status": 503, "detail": "内容读取暂时不可用，请稍后重试。"}}, ensure_ascii=False)
    finally:
        signal.alarm(0)
    print(output)
    return status


if __name__ == "__main__":
    raise SystemExit(main())
