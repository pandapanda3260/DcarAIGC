#!/usr/bin/env python3
"""One-shot paid Douyin detail refresh for an isolated Step4 source clone.

The default mode is a read-only plan.  ``--apply`` permits exactly one billed
TikHub detail request for exactly one frozen Douyin video.  The request only
materializes a new raw response and media_source manifest in isolated roots;
it never downloads media, evaluates content, fingerprints content, or edits
the content row.  A durable opening event makes the paid request permanently
one-shot: an interrupted request without a committed raw response is blocked
instead of automatically retried.
"""

# ruff: noqa: E402 -- direct execution bootstraps repo imports after disabling pyc.

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import math
import os
import platform
import re
import sqlite3
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import ExitStack, closing, contextmanager
from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence

sys.dont_write_bytecode = True

if __package__ in {None, ""}:
    repository_root = Path(__file__).resolve().parents[1]
    for candidate in (repository_root, repository_root / "src/dcar_eval"):
        value = str(candidate)
        if value not in sys.path:
            sys.path.insert(0, value)

from scripts import run_local_analysis_canary as local_controller
from v8 import capture as capture_module
from v8 import media as media_module
from v8 import providers as providers_module
from v8 import storage as storage_module


SCHEMA_VERSION = 2
COMPLETION_KIND = "paid-source-refresh-v2"
PROVIDER = "TikHub"
OPERATION = "douyin_video_detail"
ADAPTER_VERSION = "tikhub-media-source-refresh-v8.1"
STAGE = "media_source_refresh"
DETAIL_PATH = "/api/v1/douyin/app/v3/fetch_one_video"
ENDPOINT_INFO_PATH = "/api/v1/tikhub/user/get_endpoint_info"
USER_INFO_PATH = "/api/v1/tikhub/user/get_user_info"
UNIT_PRICE = 0.001
MAX_JSON_RESPONSE_BYTES = 32 * 1024 * 1024
MAX_PRICE_EVIDENCE_AGE_SECONDS = 15 * 60
MAX_HANDOFF_AGE_SECONDS = 5 * 60
TRANSPORT_PROFILE: Mapping[str, Any] = {
    "client": "python-urllib",
    "https_handler": "urllib.request.HTTPSHandler",
    "proxy": "disabled",
    "redirects": "disabled",
    "retries": 0,
    "timeout_seconds": 45,
    "tls_check_hostname": True,
    "tls_context": "ssl.create_default_context",
    "tls_verify_mode": "CERT_REQUIRED",
    "user_agent": "DCar-Insight-v8-paid-source-refresh/2.0",
}
BLOCKED_ERROR_CODES = frozenset(
    {
        "metadata_record_failed",
        "metadata_price_probe_failed",
        "paid_source_refresh_failed",
        "detail_transport_closed",
    }
)
SAFE_ERROR_TYPES = frozenset(
    {
        "AssertionError",
        "ConnectionError",
        "ConnectionRefusedError",
        "ConnectionResetError",
        "Exception",
        "HTTPError",
        "JSONDecodeError",
        "LocalAnalysisCanaryError",
        "OSError",
        "PaidSourceRefreshError",
        "RuntimeError",
        "SSLError",
        "TimeoutError",
        "TypeError",
        "UnicodeDecodeError",
        "URLError",
        "ValueError",
    }
)
ALLOWED_DELTA_TABLES = frozenset(
    {
        "provider_budget_batches",
        "provider_usage",
        "fetch_slots",
        "fetch_attempts",
        "provider_raw_responses",
        "evidence_artifacts",
    }
)
AUTOINCREMENT_DELTA_TABLES = frozenset(
    {
        "provider_usage",
        "fetch_slots",
        "fetch_attempts",
        "provider_raw_responses",
        "evidence_artifacts",
    }
)
RECEIPT_FIELDS = frozenset(
    {
        "version",
        "completion_kind",
        "status",
        "completed_at",
        "run_root",
        "contract_sha256",
        "intent_sha256",
        "state_sha256",
        "network_ledger_sha256",
        "metadata_ledger_sha256",
        "database",
        "request",
        "capture",
        "budget",
        "raw_response",
        "media_source",
        "url_provenance",
        "output_inventory",
        "critical_unchanged",
        "provider_call_history",
    }
)
COMPLETION_FIELDS = RECEIPT_FIELDS | {"receipt_sha256"}


class PaidSourceRefreshError(RuntimeError):
    pass


@dataclass(frozen=True)
class RefreshPaths:
    source_database: Path
    source_completion: Path
    database: Path
    raw_root: Path
    media_root: Path
    run_root: Path
    local_paths: local_controller.CanaryPaths
    contract: Path
    metadata_contract: Path
    metadata_ledger: Path
    intent: Path
    ledger: Path
    state: Path
    receipt: Path
    completion: Path


def _now_text() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _aware_timestamp(value: Any, *, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise PaidSourceRefreshError(f"{label}无效") from exc
    if parsed.tzinfo is None:
        raise PaidSourceRefreshError(f"{label}缺少时区")
    return parsed


def _canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _json_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _request_url_sha256(path: str, query: Mapping[str, str]) -> str:
    url = f"{providers_module.TIKHUB_BASE}{path}"
    if query:
        url = f"{url}?{urllib.parse.urlencode(query)}"
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def _sha256_file(path: Path) -> str:
    return local_controller._sha256_file(path)


def _read_json(path: Path, *, label: str) -> Mapping[str, Any]:
    return local_controller._read_json(path, label=label)


def _write_json(path: Path, value: Mapping[str, Any], *, immutable: bool) -> str:
    return local_controller._write_json(path, value, immutable=immutable)


def _cleanup_final_temp(path: Path, *, label: str) -> None:
    local_controller._cleanup_duplicate_final_temp(path, label=label)


def _recover_ledger_temp(
    paths: RefreshPaths,
    *,
    contract: Mapping[str, Any],
    intent_sha256: str,
) -> None:
    temporary = paths.ledger.with_name(f".{paths.ledger.name}.tmp")
    if not os.path.lexists(temporary):
        return
    candidate = _read_json(temporary, label="paid refresh ledger临时文件")
    _validate_bound_ledger(candidate, contract=contract, terminal=False)
    contract_sha256 = _sha256_file(paths.contract)
    if (
        candidate.get("contract_sha256") != contract_sha256
        or candidate.get("intent_sha256") != intent_sha256
    ):
        raise PaidSourceRefreshError("paid refresh ledger临时文件合同漂移")
    if paths.ledger.exists():
        current = _read_json(paths.ledger, label="paid refresh ledger")
        _validate_bound_ledger(current, contract=contract, terminal=False)
        current_events = list(current["events"])
        candidate_events = list(candidate["events"])
        if (
            current.get("contract_sha256") != contract_sha256
            or current.get("intent_sha256") != intent_sha256
            or current.get("request_id") != candidate.get("request_id")
            or len(candidate_events) < len(current_events)
            or candidate_events[: len(current_events)] != current_events
            or (
                current.get("balance_check") is not None
                and current.get("balance_check") != candidate.get("balance_check")
            )
        ):
            raise PaidSourceRefreshError("paid refresh ledger临时前缀漂移")
    os.replace(temporary, paths.ledger)
    local_controller._fsync_directory(paths.ledger.parent)


def _file_evidence(path: Path, *, label: str) -> Mapping[str, Any]:
    metadata = local_controller._private_file(path, label=label)
    return {
        "path": str(path),
        "sha256": _sha256_file(path),
        "byte_size": metadata.st_size,
        "inode": metadata.st_ino,
        "nlink": metadata.st_nlink,
    }


def _paths(
    *,
    source_db_path: Path,
    source_completion_path: Path,
    db_path: Path,
    raw_root: Path,
    media_root: Path,
    run_root: Path,
) -> RefreshPaths:
    local_paths = local_controller._paths(
        source_db_path=source_db_path,
        source_completion_path=source_completion_path,
        db_path=db_path,
        media_root=media_root,
        run_root=run_root,
    )
    lexical_raw = Path(os.path.abspath(raw_root))
    local_controller._assert_no_symlink_components(
        lexical_raw, label="paid refresh raw_root"
    )
    canonical_raw = local_controller._filesystem_canonical_path(lexical_raw)
    return RefreshPaths(
        source_database=local_paths.source_database,
        source_completion=local_paths.source_completion,
        database=local_paths.database,
        raw_root=canonical_raw,
        media_root=local_paths.media_root,
        run_root=local_paths.run_root,
        local_paths=local_paths,
        contract=local_paths.run_root / "refresh-contract.json",
        metadata_contract=local_paths.run_root / "metadata-contract.json",
        metadata_ledger=local_paths.run_root / "metadata-ledger.json",
        intent=local_paths.run_root / "refresh-intent.json",
        ledger=local_paths.run_root / "provider-ledger.json",
        state=local_paths.run_root / "refresh-state.json",
        receipt=local_paths.run_root / "refresh-receipt.json",
        completion=local_paths.run_root / "completion.json",
    )


def _validate_paths(paths: RefreshPaths, *, database_must_exist: bool) -> None:
    local_controller._validate_paths(
        paths.local_paths, work_database_must_exist=database_must_exist
    )
    local_controller._assert_no_symlink_components(
        paths.raw_root, label="paid refresh raw_root"
    )
    local_controller._private_directory(
        paths.raw_root.parent, label="paid refresh raw_root parent"
    )
    canonical_raw = capture_module.RAW_ROOT.resolve()
    canonical_media = media_module.MEDIA_ROOT.resolve()
    for protected in (canonical_raw, canonical_media):
        if local_controller._overlap(paths.raw_root, protected):
            raise PaidSourceRefreshError(
                "paid refresh raw_root不得指向canonical缓存"
            )
    for other in (paths.database, paths.media_root, paths.run_root):
        if local_controller._overlap(paths.raw_root, other):
            raise PaidSourceRefreshError("paid refresh输出根不得相同或相互包含")


def _validate_parent_separation(
    paths: RefreshPaths, source_evidence: Mapping[str, Any]
) -> None:
    local_controller._validate_step3_separation(
        paths.local_paths, source_evidence
    )
    contract = source_evidence["contract"]
    protected = (
        paths.source_database.parent,
        paths.source_completion.parent,
        Path(str(contract["media_root"])),
        Path(str(contract["derived_raw_root"])),
    )
    for root in protected:
        if local_controller._overlap(paths.raw_root, root):
            raise PaidSourceRefreshError(
                "paid refresh raw_root不得位于Step3证据树或其父域"
            )


def _prepare_roots(paths: RefreshPaths) -> None:
    for root, label in (
        (paths.raw_root, "paid raw_root"),
        (paths.media_root, "paid media_root"),
        (paths.run_root, "paid run_root"),
    ):
        if not os.path.lexists(root):
            root.mkdir(mode=0o700)
            local_controller._fsync_directory(root.parent)
        local_controller._private_directory(root, label=label)


@contextmanager
def _claims(paths: RefreshPaths) -> Iterator[None]:
    raw_claim = local_controller._claim_path(paths.raw_root, label="paid-raw")
    with ExitStack() as stack:
        stack.enter_context(local_controller._all_claims(paths.local_paths))
        stack.enter_context(local_controller._exclusive_claim(raw_claim))
        yield


def _ordered_one(content_ids: Sequence[int]) -> int:
    values = [int(value) for value in content_ids]
    if len(values) != 1 or values[0] <= 0:
        raise PaidSourceRefreshError("paid refresh必须且只能指定一个正整数content ID")
    return values[0]


def _task_identity(
    *, source_db_sha256: str, source_completion_sha256: str, content_id: int
) -> Mapping[str, str]:
    seed = {
        "completion_kind": COMPLETION_KIND,
        "source_db_sha256": source_db_sha256,
        "source_completion_sha256": source_completion_sha256,
        "content_id": content_id,
        "operation": OPERATION,
    }
    digest = _json_sha256(seed)
    task_id = f"paid-source-refresh-{digest[:24]}"
    budget_digest = hashlib.sha256(task_id.encode("utf-8")).hexdigest()[:16]
    return {
        "task_id": task_id,
        "budget_id": f"task-{budget_digest}-tikhub-{OPERATION}-v1",
        "window_key": f"paid-source-refresh-v2-{digest[:16]}",
    }


def _code_snapshot() -> list[Mapping[str, Any]]:
    files = (
        Path(__file__).resolve(),
        Path(local_controller.__file__).resolve(),
        Path(capture_module.__file__).resolve(),
        Path(media_module.__file__).resolve(),
        Path(providers_module.__file__).resolve(),
        Path(storage_module.__file__).resolve(),
    )
    return [
        {
            "path": str(path),
            "sha256": _sha256_file(path),
            "byte_size": path.stat().st_size,
        }
        for path in files
    ]


def _runtime_snapshot() -> Mapping[str, Any]:
    executable = Path(sys.executable).resolve(strict=True)
    metadata = executable.stat()
    if not executable.is_file() or metadata.st_size <= 0:
        raise PaidSourceRefreshError("paid runtime executable不是非空regular file")
    implementation = platform.python_implementation()
    if implementation != "CPython":
        raise PaidSourceRefreshError("paid refresh仅冻结CPython runtime")
    return {
        "implementation": implementation,
        "python_version": platform.python_version(),
        "openssl_version": ssl.OPENSSL_VERSION,
        "executable": {
            "path": str(executable),
            "sha256": _sha256_file(executable),
            "byte_size": metadata.st_size,
        },
    }


def _provider_call_history() -> Mapping[str, Any]:
    return {
        "calls": {
            "endpoint_info": 3,
            "user_info": 1,
            "metadata": 4,
            "detail": 1,
            "total": 5,
        },
        "amounts": {
            "currency": "USD",
            "endpoint_info": 0.0,
            "user_info": 0.0,
            "metadata": 0.0,
            "detail": UNIT_PRICE,
            "total": UNIT_PRICE,
            "basis": "successful_detail_provider_usage_exact",
        },
    }


def _provider_call_accounting(
    *, endpoint_info_calls: int, user_info_calls: int, detail_calls: int
) -> Mapping[str, Any]:
    if (
        endpoint_info_calls not in {0, 3}
        or user_info_calls not in {0, 1}
        or detail_calls not in {0, 1}
    ):
        raise PaidSourceRefreshError("paid provider call accounting current漂移")
    metadata_calls = endpoint_info_calls + user_info_calls
    return {
        "current": {
            "endpoint_info": endpoint_info_calls,
            "user_info": user_info_calls,
            "metadata": metadata_calls,
            "detail": detail_calls,
            "total": metadata_calls + detail_calls,
        },
        "historical": _provider_call_history(),
    }


def _table_names(connection: sqlite3.Connection) -> list[str]:
    return [
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    ]


def _row_values(row: sqlite3.Row) -> Mapping[str, Any]:
    return {key: local_controller._json_value(row[key]) for key in row.keys()}


def _finite_number(value: Any, *, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PaidSourceRefreshError(f"{label}不是有限数值")
    number = float(value)
    if not math.isfinite(number):
        raise PaidSourceRefreshError(f"{label}不是有限数值")
    return number


def _exact_decimal(value: Any, *, label: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PaidSourceRefreshError(f"{label}不是有限数值")
    number = Decimal(str(value))
    if not number.is_finite():
        raise PaidSourceRefreshError(f"{label}不是有限数值")
    return number


def _exact_integer(value: Any, *, label: str) -> int:
    if type(value) is not int:
        raise PaidSourceRefreshError(f"{label}不是exact integer")
    return value


def _rows(connection: sqlite3.Connection, table: str) -> list[Mapping[str, Any]]:
    escaped = table.replace('"', '""')
    return [
        _row_values(row)
        for row in connection.execute(f'SELECT * FROM "{escaped}" ORDER BY rowid')
    ]


def _schema_evidence(connection: sqlite3.Connection) -> Mapping[str, Any]:
    schema = [
        {
            "type": str(row["type"]),
            "name": str(row["name"]),
            "tbl_name": str(row["tbl_name"]),
            "sql": local_controller._json_value(row["sql"]),
        }
        for row in connection.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_schema "
            "ORDER BY type,name,tbl_name"
        )
    ]
    pragmas = {
        name: local_controller._json_value(
            connection.execute(f"PRAGMA {name}").fetchone()[0]
        )
        for name in (
            "application_id",
            "auto_vacuum",
            "encoding",
            "page_size",
            "user_version",
        )
    }
    return {
        "sqlite_schema": schema,
        "sqlite_schema_sha256": _json_sha256(schema),
        "pragmas": pragmas,
        "pragmas_sha256": _json_sha256(pragmas),
    }


def _database_baseline(path: Path) -> Mapping[str, Any]:
    with closing(local_controller._immutable_connection(path)) as connection:
        tables = _table_names(connection)
        protected = {
            table: {
                "rows": len(values),
                "sha256": _json_sha256(values),
            }
            for table in tables
            if table not in ALLOWED_DELTA_TABLES
            for values in [_rows(connection, table)]
        }
        allowed = {
            table: {
                "rows": len(values),
                "sha256": _json_sha256(values),
            }
            for table in sorted(ALLOWED_DELTA_TABLES)
            for values in [_rows(connection, table)]
        }
        sequences = {
            str(row["name"]): _exact_integer(
                row["seq"], label=f"database baseline sqlite_sequence {row['name']}"
            )
            for row in connection.execute(
                "SELECT name,seq FROM sqlite_sequence ORDER BY name"
            )
        }
        schema = _schema_evidence(connection)
    return {
        "protected": protected,
        "protected_sha256": _json_sha256(protected),
        "allowed": allowed,
        "allowed_sha256": _json_sha256(allowed),
        "sqlite_sequence": sequences,
        **schema,
    }


def _new_rows(
    before: sqlite3.Connection, after: sqlite3.Connection, table: str, key: str
) -> list[Mapping[str, Any]]:
    before_rows = _rows(before, table)
    after_rows = _rows(after, table)
    before_by_key = {str(row[key]): row for row in before_rows}
    after_by_key = {str(row[key]): row for row in after_rows}
    if any(after_by_key.get(value) != row for value, row in before_by_key.items()):
        raise PaidSourceRefreshError(f"paid refresh改写了{table}既有行")
    missing = set(before_by_key) - set(after_by_key)
    if missing:
        raise PaidSourceRefreshError(f"paid refresh删除了{table}既有行")
    return [row for value, row in after_by_key.items() if value not in before_by_key]


def _validate_database_prefix(
    paths: RefreshPaths, contract: Mapping[str, Any]
) -> Mapping[str, list[Mapping[str, Any]]]:
    parent_path = Path(str(contract["base_source"]["database"]["path"]))
    with (
        closing(local_controller._immutable_connection(parent_path)) as before,
        closing(local_controller._immutable_connection(paths.database)) as after,
    ):
        if (
            _table_names(before) != _table_names(after)
            or _schema_evidence(before) != _schema_evidence(after)
        ):
            raise PaidSourceRefreshError("paid refresh前向DB schema/pragma漂移")
        for table in _table_names(before):
            if table not in ALLOWED_DELTA_TABLES and _rows(before, table) != _rows(
                after, table
            ):
                raise PaidSourceRefreshError(
                    f"paid refresh前向DB保护表漂移：{table}"
                )
        new_rows = {
            table: _new_rows(before, after, table, "id")
            for table in ALLOWED_DELTA_TABLES
        }
        if any(len(rows) > 1 for rows in new_rows.values()):
            raise PaidSourceRefreshError("paid refresh前向DB新增行数量越界")
        presence = tuple(bool(new_rows[table]) for table in (
            "provider_budget_batches",
            "provider_usage",
            "fetch_slots",
            "fetch_attempts",
            "provider_raw_responses",
            "evidence_artifacts",
        ))
        if presence not in {
            (False, False, False, False, False, False),
            (True, False, False, False, False, False),
            (True, False, True, True, False, False),
            (True, True, True, True, False, False),
            (True, True, True, True, True, False),
            (True, True, True, True, True, True),
        }:
            raise PaidSourceRefreshError("paid refresh前向DB状态组合不合法")
        before_sequences = {
            str(row["name"]): _exact_integer(
                row["seq"], label=f"baseline sqlite_sequence {row['name']}"
            )
            for row in before.execute(
                "SELECT name,seq FROM sqlite_sequence ORDER BY name"
            )
        }
        after_sequences = {
            str(row["name"]): _exact_integer(
                row["seq"], label=f"current sqlite_sequence {row['name']}"
            )
            for row in after.execute(
                "SELECT name,seq FROM sqlite_sequence ORDER BY name"
            )
        }
        for name in set(before_sequences) | set(after_sequences):
            expected = before_sequences.get(name, 0) + (
                1
                if name in AUTOINCREMENT_DELTA_TABLES and new_rows[name]
                else 0
            )
            if after_sequences.get(name, 0) != expected:
                raise PaidSourceRefreshError(
                    f"paid refresh前向sqlite_sequence漂移：{name}"
                )

    content_id = int(contract["target"]["id"])
    budget_row = next(iter(new_rows["provider_budget_batches"]), None)
    usage_row = next(iter(new_rows["provider_usage"]), None)
    slot_row = next(iter(new_rows["fetch_slots"]), None)
    attempt_row = next(iter(new_rows["fetch_attempts"]), None)
    raw_row = next(iter(new_rows["provider_raw_responses"]), None)
    artifact_row = next(iter(new_rows["evidence_artifacts"]), None)
    if budget_row is not None and (
        budget_row.get("id") != contract["budget"]["budget_id"]
        or budget_row.get("purpose") != contract["budget"]["purpose"]
        or budget_row.get("provider") != PROVIDER
        or budget_row.get("operation") != OPERATION
        or budget_row.get("currency") != "USD"
        or _exact_decimal(
            budget_row.get("verified_unit_price"), label="budget unit price"
        )
        != Decimal(str(UNIT_PRICE))
        or budget_row.get("price_verified_at")
        != contract["budget"]["price_verified_at"]
        or _exact_integer(
            budget_row.get("max_billable_requests"),
            label="budget max billable requests",
        )
        != 1
        or _exact_decimal(
            budget_row.get("max_amount"), label="budget max amount"
        )
        != Decimal(str(UNIT_PRICE))
        or _exact_integer(budget_row.get("pilot_size"), label="budget pilot size")
        != 1
        or _exact_integer(
            budget_row.get("daily_quota"), label="budget daily quota"
        )
        != 1
        or _exact_integer(
            budget_row.get("consumed_requests"), label="budget consumed requests"
        )
        != (1 if usage_row is not None else 0)
        or _exact_decimal(
            budget_row.get("consumed_amount"), label="budget consumed amount"
        )
        != (Decimal(str(UNIT_PRICE)) if usage_row is not None else Decimal("0"))
        or budget_row.get("status") != "approved"
    ):
        raise PaidSourceRefreshError("paid refresh前向budget行漂移")
    if slot_row is not None and (
        _exact_integer(slot_row.get("content_id"), label="slot content id")
        != content_id
        or slot_row.get("stage") != STAGE
        or slot_row.get("window_key") != contract["route"]["window_key"]
        or slot_row.get("provider") != PROVIDER
        or slot_row.get("adapter_version") != ADAPTER_VERSION
        or _exact_integer(slot_row.get("attempt_count"), label="slot attempt count")
        != 1
        or slot_row.get("status")
        not in {"running", "succeeded", "terminal_failed"}
    ):
        raise PaidSourceRefreshError("paid refresh前向slot行漂移")
    if attempt_row is not None and (
        _exact_integer(attempt_row.get("slot_id"), label="attempt slot id")
        != _exact_integer(slot_row.get("id"), label="slot id")
        or _exact_integer(
            attempt_row.get("attempt_number"), label="attempt number"
        )
        != 1
    ):
        raise PaidSourceRefreshError("paid refresh前向attempt行漂移")
    if usage_row is not None:
        try:
            usage_details = json.loads(str(usage_row["details_json"]))
        except json.JSONDecodeError as exc:
            raise PaidSourceRefreshError("paid refresh前向usage JSON漂移") from exc
        if (
            usage_row.get("task_id") != contract["budget"]["task_id"]
            or usage_row.get("budget_batch_id")
            != contract["budget"]["budget_id"]
            or usage_row.get("provider") != PROVIDER
            or usage_row.get("operation") != OPERATION
            or _exact_integer(
                usage_row.get("request_attempts"), label="usage request attempts"
            )
            != 1
            or _exact_integer(
                usage_row.get("billed_requests"), label="usage billed requests"
            )
            != 1
            or usage_row.get("currency") != "USD"
            or _exact_decimal(usage_row.get("amount"), label="usage amount")
            != Decimal(str(UNIT_PRICE))
            or usage_details.get("state") not in {"reserved", "completed"}
        ):
            raise PaidSourceRefreshError("paid refresh前向usage行漂移")
        if raw_row is None:
            if slot_row.get("status") == "running":
                if _canonical_bytes(usage_details) != _canonical_bytes(
                    {"state": "reserved"}
                ):
                    raise PaidSourceRefreshError(
                        "paid refresh running且未落raw的usage不是精确reservation"
                    )
            elif slot_row.get("status") == "terminal_failed":
                expected_transport_details = {
                    "billing_basis": "conservative_upper_bound",
                    "outcome": "transport_failed",
                    "slot_id": _exact_integer(slot_row.get("id"), label="slot id"),
                    "state": "completed",
                }
                if (
                    _canonical_bytes(usage_details)
                    != _canonical_bytes(expected_transport_details)
                    or attempt_row.get("http_status") is not None
                    or _exact_integer(
                        attempt_row.get("billed"), label="transport attempt billed"
                    )
                    != 1
                    or _exact_decimal(
                        attempt_row.get("amount"), label="transport attempt amount"
                    )
                    != Decimal(str(UNIT_PRICE))
                    or attempt_row.get("currency") != "USD"
                    or attempt_row.get("error_code") != "transport_failed"
                    or slot_row.get("last_error_code") != "transport_failed"
                    or not str(attempt_row.get("response_finished_at") or "")
                    or not str(slot_row.get("finished_at") or "")
                ):
                    raise PaidSourceRefreshError("paid refresh transport_failed终态漂移")
            else:
                raise PaidSourceRefreshError("paid refresh无raw的slot状态漂移")
    if raw_row is not None:
        if (
            _exact_integer(
                raw_row.get("fetch_attempt_id"), label="raw fetch attempt id"
            )
            != _exact_integer(attempt_row.get("id"), label="attempt id")
            or _exact_integer(raw_row.get("content_id"), label="raw content id")
            != content_id
            or raw_row.get("provider") != PROVIDER
            or raw_row.get("operation") != OPERATION
            or _exact_integer(raw_row.get("http_status"), label="raw http status")
            != 200
            or raw_row.get("source") not in {"live", "live_applied"}
        ):
            raise PaidSourceRefreshError("paid refresh前向raw行漂移")
        raw_path = _raw_path(raw_row).resolve()
        raw_file = _file_evidence(raw_path, label="paid refresh前向raw")
        if (
            not local_controller._is_within(raw_path, paths.raw_root)
            or raw_file["sha256"] != raw_row.get("sha256")
            or raw_file["byte_size"]
            != _exact_integer(raw_row.get("byte_size"), label="raw byte size")
        ):
            raise PaidSourceRefreshError("paid refresh前向raw文件漂移")
        if slot_row.get("status") == "succeeded":
            expected_usage_details = {
                "http_status": 200,
                "slot_id": _exact_integer(slot_row.get("id"), label="slot id"),
                "state": "completed",
            }
            expected_error = (None, None)
        elif slot_row.get("status") == "terminal_failed":
            expected_usage_details = {
                "http_status": 200,
                "outcome": "rejected_source",
                "slot_id": _exact_integer(slot_row.get("id"), label="slot id"),
                "state": "completed",
            }
            expected_error = ("rejected_source", "rejected_source")
        else:
            raise PaidSourceRefreshError("paid refresh有raw但slot未终结")
        if (
            _canonical_bytes(usage_details) != _canonical_bytes(expected_usage_details)
            or _exact_integer(
                attempt_row.get("http_status"), label="attempt http status"
            )
            != 200
            or _exact_integer(attempt_row.get("billed"), label="attempt billed")
            != 1
            or _exact_decimal(attempt_row.get("amount"), label="attempt amount")
            != Decimal(str(UNIT_PRICE))
            or attempt_row.get("currency") != "USD"
            or attempt_row.get("error_code") != expected_error[0]
            or slot_row.get("last_error_code") != expected_error[1]
        ):
            raise PaidSourceRefreshError("paid refresh前向charged终态漂移")
        ledger = _read_json(paths.ledger, label="paid refresh前向ledger")
        _validate_bound_ledger(ledger, contract=contract, terminal=True)
        if (
            ledger["events"][1].get("outcome") != "response_received"
            or ledger["events"][1].get("response_json_sha256")
            != raw_file["sha256"]
        ):
            raise PaidSourceRefreshError("paid refresh前向raw未绑定HTTP响应")
    if artifact_row is not None:
        if raw_row is None or slot_row.get("status") != "succeeded":
            raise PaidSourceRefreshError("paid refresh前向artifact缺少成功raw")
        committed = _committed_raw(paths, contract)
        if committed is None:
            raise PaidSourceRefreshError("paid refresh前向artifact缺少committed raw")
        raw_response_id, data, _ = committed
        urls, source_sha256 = media_module._media_source_identity(
            "video", data.get("media_urls") or []
        )
        expected_path = (
            paths.media_root
            / str(contract["target"]["link_id"])
            / "sources"
            / f"source-{raw_response_id}-{source_sha256[:12]}.json"
        )
        artifact_file = _file_evidence(
            expected_path, label="paid refresh前向manifest"
        )
        try:
            manifest = json.loads(expected_path.read_bytes())
            metadata = json.loads(str(artifact_row["metadata_json"]))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PaidSourceRefreshError("paid refresh前向manifest JSON漂移") from exc
        if (
            _exact_integer(
                artifact_row.get("content_id"), label="artifact content id"
            )
            != content_id
            or artifact_row.get("artifact_type") != "media_source"
            or artifact_row.get("status") != "available"
            or artifact_row.get("processor_version")
            != media_module.MEDIA_SOURCE_VERSION
            or Path(str(artifact_row.get("local_path"))).resolve() != expected_path
            or artifact_row.get("sha256") != artifact_file["sha256"]
            or _exact_integer(
                artifact_row.get("byte_size"), label="artifact byte size"
            )
            != artifact_file["byte_size"]
            or manifest.get("media_kind") != "video"
            or manifest.get("urls") != urls
            or manifest.get("source_sha256") != source_sha256
            or _exact_integer(
                manifest.get("raw_response_id"), label="manifest raw response id"
            )
            != raw_response_id
            or not isinstance(metadata, Mapping)
            or set(metadata)
            != {"media_kind", "source_count", "source_sha256", "raw_response_id"}
            or metadata.get("media_kind") != "video"
            or _exact_integer(
                metadata.get("source_count"),
                label="forward manifest metadata source count",
            )
            != len(urls)
            or metadata.get("source_sha256") != source_sha256
            or _exact_integer(
                metadata.get("raw_response_id"),
                label="forward manifest metadata raw response id",
            )
            != raw_response_id
        ):
            raise PaidSourceRefreshError("paid refresh前向manifest证据漂移")
    return new_rows


def _validate_content_target(
    connection: sqlite3.Connection, content_id: int
) -> Mapping[str, Any]:
    row = connection.execute(
        "SELECT * FROM content_items WHERE id=?", (content_id,)
    ).fetchone()
    if row is None:
        raise PaidSourceRefreshError(f"content不存在：{content_id}")
    value = _row_values(row)
    if (
        value.get("platform") != "douyin"
        or value.get("content_type") != "video"
        or value.get("source_group") not in storage_module.BACKFILL_SOURCE_GROUPS
        or not str(value.get("platform_content_id") or "")
        or re.fullmatch(r"[A-Za-z0-9]{6}", str(value.get("link_id") or ""))
        is None
    ):
        raise PaidSourceRefreshError("paid refresh目标不是冻结Douyin history video")
    return value


def _source_plan(
    connection: sqlite3.Connection,
    *,
    content_id: int,
    source_evidence: Mapping[str, Any],
) -> Mapping[str, Any]:
    contract = source_evidence["contract"]
    row = next(
        item
        for item in contract["explicit_target_rows"]
        if int(item[0]) == content_id
    )
    prior = local_controller._source_snapshot(
        connection,
        content_id,
        step3_media_root=Path(str(contract["media_root"])),
        step3_derived_raw_root=Path(str(contract["derived_raw_root"])),
        target_contract_row=row,
    )
    target = _validate_content_target(connection, content_id)
    existing = connection.execute(
        "SELECT COUNT(*) FROM fetch_slots WHERE content_id=? AND stage=?",
        (content_id, STAGE),
    ).fetchone()[0]
    if int(existing) != 0:
        raise PaidSourceRefreshError("Step3源库已含media_source_refresh slot")
    return {"target": target, "prior_source": prior}


def _extract_endpoint_fields(
    payload: Any, *, expected_endpoint: str, expected_cost: float
) -> Mapping[str, Any]:
    if not isinstance(payload, Mapping):
        raise PaidSourceRefreshError("endpoint-info不是JSON object")
    params = payload.get("params")
    row = payload.get("data")
    if (
        type(payload.get("code")) is not int
        or payload.get("code") != 200
        or payload.get("router") != ENDPOINT_INFO_PATH
        or not isinstance(params, Mapping)
        or set(params) != {"endpoint"}
        or params.get("endpoint") != expected_endpoint
        or not isinstance(row, Mapping)
        or row.get("endpoint_uri") != expected_endpoint
    ):
        raise PaidSourceRefreshError("endpoint-info params/data未绑定exact detail endpoint")
    if "endpoint_cost" not in row:
        raise PaidSourceRefreshError("endpoint-info价格字段缺失")
    cost_decimal = _exact_decimal(
        row["endpoint_cost"], label="endpoint-info价格"
    )
    expected_decimal = Decimal(str(expected_cost))
    if cost_decimal != expected_decimal:
        raise PaidSourceRefreshError(
            f"endpoint-info价格漂移：{cost_decimal!r}!={expected_decimal!r}"
        )
    cost = float(cost_decimal)
    if row.get("endpoint_type") != "self-operated":
        raise PaidSourceRefreshError("endpoint-info不是self-operated终态")
    return {
        "requested_endpoint": expected_endpoint,
        "endpoint_uri": expected_endpoint,
        "endpoint_cost": cost,
        "self_operated": True,
        "rate_limit": row.get("rate_limit"),
    }


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_args: Any, **_kwargs: Any) -> None:
        return None


def _exact_json_request(
    url: str,
    *,
    expected_path: str,
    expected_query: Mapping[str, str],
    authorization: str | None,
    maximum_bytes: int = MAX_JSON_RESPONSE_BYTES,
) -> tuple[Any, Mapping[str, Any]]:
    parsed = urllib.parse.urlsplit(url)
    if (
        parsed.scheme != "https"
        or (parsed.hostname or "").lower() != "api.tikhub.io"
        or parsed.port not in (None, 443)
        or parsed.path != expected_path
        or dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
        != dict(expected_query)
        or parsed.fragment
    ):
        raise PaidSourceRefreshError("TikHub请求未命中冻结exact endpoint")
    headers = {
        "Accept": "application/json",
        "User-Agent": str(TRANSPORT_PROFILE["user_agent"]),
    }
    if authorization is not None:
        headers["Authorization"] = f"Bearer {authorization}"
    request = urllib.request.Request(url, headers=headers, method="GET")
    tls_context = ssl.create_default_context()
    if (
        tls_context.check_hostname is not True
        or tls_context.verify_mode != ssl.CERT_REQUIRED
    ):
        raise PaidSourceRefreshError("TikHub TLS default context验证策略漂移")
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        _NoRedirect(),
        urllib.request.HTTPSHandler(context=tls_context),
    )
    try:
        response = opener.open(
            request, timeout=_exact_integer(TRANSPORT_PROFILE["timeout_seconds"], label="transport timeout")
        )
    except urllib.error.HTTPError as exc:
        raise PaidSourceRefreshError(f"TikHub HTTP {exc.code}") from exc
    with response:
        if response.geturl() != url or _exact_integer(
            response.status, label="TikHub HTTP status"
        ) != 200:
            raise PaidSourceRefreshError("TikHub响应URL或status漂移")
        mime = str(response.headers.get("Content-Type") or "").split(";", 1)[0]
        if mime.lower() != "application/json":
            raise PaidSourceRefreshError("TikHub响应不是application/json")
        declared_text = response.headers.get("Content-Length")
        declared: int | None = None
        if declared_text is not None:
            try:
                declared = int(declared_text)
            except ValueError as exc:
                raise PaidSourceRefreshError("TikHub Content-Length无效") from exc
            if declared < 0 or declared > maximum_bytes:
                raise PaidSourceRefreshError("TikHub Content-Length越界")
        body = bytearray()
        while True:
            block = response.read(min(65536, maximum_bytes - len(body) + 1))
            if not block:
                break
            body.extend(block)
            if len(body) > maximum_bytes:
                raise PaidSourceRefreshError("TikHub响应超过最大字节数")
        if declared is not None and len(body) != declared:
            raise PaidSourceRefreshError("TikHub响应长度与Content-Length不一致")
    try:
        payload = json.loads(bytes(body).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PaidSourceRefreshError("TikHub响应不是合法UTF-8 JSON") from exc
    return payload, {
        "url_sha256": hashlib.sha256(url.encode()).hexdigest(),
        "response_sha256": hashlib.sha256(body).hexdigest(),
        "response_bytes": len(body),
        "http_status": 200,
        "mime_type": "application/json",
    }


def _default_endpoint_info() -> Mapping[str, Any]:
    raise PaidSourceRefreshError(
        "v2禁止无metadata ledger的endpoint-info；请通过run_refresh fresh root执行"
    )


def _validate_price_evidence(value: Mapping[str, Any]) -> Mapping[str, Any]:
    if set(value) != {"checked_at", "records", "records_sha256"}:
        raise PaidSourceRefreshError("endpoint-info证据字段漂移")
    records = value.get("records")
    expected = (
        (ENDPOINT_INFO_PATH, 0.0, "1/second"),
        (USER_INFO_PATH, 0.0, "1/second"),
        (DETAIL_PATH, UNIT_PRICE, "10/second"),
    )
    if (
        not isinstance(records, list)
        or len(records) != len(expected)
        or value.get("records_sha256") != _json_sha256(records)
    ):
        raise PaidSourceRefreshError("endpoint-info记录集合漂移")
    for record, (endpoint, cost, rate_limit) in zip(records, expected):
        if not isinstance(record, Mapping) or set(record) != {
            "queried_endpoint",
            "response",
            "fields",
        }:
            raise PaidSourceRefreshError("endpoint-info record形状漂移")
        response = record.get("response")
        fields = record.get("fields")
        if (
            record.get("queried_endpoint") != endpoint
            or not isinstance(response, Mapping)
            or set(response)
            != {
                "url_sha256",
                "response_sha256",
                "response_bytes",
                "http_status",
                "mime_type",
            }
            or response.get("url_sha256")
            != _request_url_sha256(
                ENDPOINT_INFO_PATH, {"endpoint": endpoint}
            )
            or type(response.get("http_status")) is not int
            or response.get("http_status") != 200
            or response.get("mime_type") != "application/json"
            or type(response.get("response_bytes")) is not int
            or response.get("response_bytes") <= 0
            or not isinstance(fields, Mapping)
            or set(fields)
            != {
                "requested_endpoint",
                "endpoint_uri",
                "endpoint_cost",
                "self_operated",
                "rate_limit",
            }
            or fields.get("requested_endpoint") != endpoint
            or fields.get("endpoint_uri") != endpoint
            or _exact_decimal(
                fields.get("endpoint_cost"), label="endpoint-info价格证据"
            )
            != Decimal(str(cost))
            or fields.get("self_operated") is not True
            or fields.get("rate_limit") != rate_limit
        ):
            raise PaidSourceRefreshError("endpoint-info价格证据不精确")
        for digest in (
            response.get("url_sha256"),
            response.get("response_sha256"),
        ):
            if (
                not isinstance(digest, str)
                or re.fullmatch(r"[0-9a-f]{64}", digest) is None
            ):
                raise PaidSourceRefreshError("endpoint-info SHA证据无效")
    return dict(value)


def _metadata_identity(
    paths: RefreshPaths,
    *,
    expected_source_db_sha256: str,
    expected_source_completion_sha256: str,
    content_id: int,
) -> Mapping[str, Any]:
    return {
        "completion_kind": COMPLETION_KIND,
        "run_root": str(paths.run_root),
        "source_db_sha256": expected_source_db_sha256,
        "source_completion_sha256": expected_source_completion_sha256,
        "content_id": content_id,
    }


def _metadata_request_plan(identity: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    definitions = (
        ("endpoint_info", ENDPOINT_INFO_PATH, {"endpoint": ENDPOINT_INFO_PATH}, "none", 0.0, MAX_JSON_RESPONSE_BYTES),
        ("endpoint_info", ENDPOINT_INFO_PATH, {"endpoint": USER_INFO_PATH}, "none", 0.0, MAX_JSON_RESPONSE_BYTES),
        ("endpoint_info", ENDPOINT_INFO_PATH, {"endpoint": DETAIL_PATH}, "none", UNIT_PRICE, MAX_JSON_RESPONSE_BYTES),
        ("user_info", USER_INFO_PATH, {}, "bearer", 0.0, 1024 * 1024),
    )
    output: list[Mapping[str, Any]] = []
    identity_sha256 = _json_sha256(identity)
    for ordinal, (
        purpose,
        endpoint,
        query,
        authorization,
        expected_cost,
        maximum_bytes,
    ) in enumerate(definitions):
        request = {
            "ordinal": ordinal,
            "purpose": purpose,
            "endpoint": endpoint,
            "query": query,
            "authorization": authorization,
            "expected_cost": expected_cost,
            "maximum_bytes": maximum_bytes,
        }
        output.append(
            {
                **request,
                "request_id": _json_sha256(
                    {"identity_sha256": identity_sha256, "request": request}
                )[:32],
            }
        )
    return output


def _metadata_contract_value(
    paths: RefreshPaths,
    *,
    expected_source_db_sha256: str,
    expected_source_completion_sha256: str,
    content_id: int,
) -> Mapping[str, Any]:
    identity = _metadata_identity(
        paths,
        expected_source_db_sha256=expected_source_db_sha256,
        expected_source_completion_sha256=expected_source_completion_sha256,
        content_id=content_id,
    )
    request_plan = _metadata_request_plan(identity)
    return {
        "version": SCHEMA_VERSION,
        "completion_kind": COMPLETION_KIND,
        "created_at": _now_text(),
        "identity": identity,
        "request_plan": request_plan,
        "request_plan_sha256": _json_sha256(request_plan),
        "transport_profile": dict(TRANSPORT_PROFILE),
        "transport_profile_sha256": _json_sha256(TRANSPORT_PROFILE),
        "code": _code_snapshot(),
        "runtime": _runtime_snapshot(),
    }


def _validate_metadata_contract(
    paths: RefreshPaths,
    value: Mapping[str, Any],
    *,
    expected_source_db_sha256: str,
    expected_source_completion_sha256: str,
    content_id: int,
) -> None:
    expected_identity = _metadata_identity(
        paths,
        expected_source_db_sha256=expected_source_db_sha256,
        expected_source_completion_sha256=expected_source_completion_sha256,
        content_id=content_id,
    )
    expected_plan = _metadata_request_plan(expected_identity)
    if (
        set(value)
        != {
            "version",
            "completion_kind",
            "created_at",
            "identity",
            "request_plan",
            "request_plan_sha256",
            "transport_profile",
            "transport_profile_sha256",
            "code",
            "runtime",
        }
        or value.get("version") != SCHEMA_VERSION
        or value.get("completion_kind") != COMPLETION_KIND
        or value.get("identity") != expected_identity
        or value.get("request_plan") != expected_plan
        or value.get("request_plan_sha256") != _json_sha256(expected_plan)
        or value.get("transport_profile") != TRANSPORT_PROFILE
        or value.get("transport_profile_sha256") != _json_sha256(TRANSPORT_PROFILE)
        or value.get("code") != _code_snapshot()
        or value.get("runtime") != _runtime_snapshot()
    ):
        raise PaidSourceRefreshError("paid metadata contract形状/身份/transport漂移")
    _aware_timestamp(value["created_at"], label="paid metadata contract时间")


def _metadata_ledger_value(
    *,
    metadata_contract_sha256: str,
    metadata_contract: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]] = (),
    price_evidence: Mapping[str, Any] | None = None,
    contract_sha256: str | None = None,
) -> Mapping[str, Any]:
    rows = list(events)
    return {
        "version": SCHEMA_VERSION,
        "completion_kind": COMPLETION_KIND,
        "metadata_contract_sha256": metadata_contract_sha256,
        "request_plan_sha256": metadata_contract["request_plan_sha256"],
        "transport_profile": dict(TRANSPORT_PROFILE),
        "contract_sha256": contract_sha256,
        "price_evidence": dict(price_evidence) if price_evidence is not None else None,
        "events": rows,
        "events_sha256": _json_sha256(rows),
    }


def _metadata_opening(plan: Mapping[str, Any], *, index: int) -> Mapping[str, Any]:
    return {
        "index": index,
        "phase": "opening",
        "request_id": plan["request_id"],
        "purpose": plan["purpose"],
        "endpoint": plan["endpoint"],
        "query_sha256": _json_sha256(plan["query"]),
        "authorization": plan["authorization"],
        "created_at": _now_text(),
    }


def _metadata_terminal(
    plan: Mapping[str, Any],
    *,
    index: int,
    evidence: Mapping[str, Any] | None = None,
    error: BaseException | None = None,
) -> Mapping[str, Any]:
    common = {
        "index": index,
        "phase": "terminal",
        "request_id": plan["request_id"],
        "purpose": plan["purpose"],
        "endpoint": plan["endpoint"],
        "finished_at": _now_text(),
    }
    if error is not None:
        return {
            **common,
            "outcome": "failed",
            "error_type": _safe_error_type(error),
            "error_code": (
                "endpoint_info_failed"
                if plan["purpose"] == "endpoint_info"
                else "user_info_failed"
            ),
        }
    key = "record" if plan["purpose"] == "endpoint_info" else "balance_evidence"
    return {**common, "outcome": "response_received", key: dict(evidence or {})}


def _validate_metadata_ledger(
    value: Mapping[str, Any],
    *,
    metadata_contract: Mapping[str, Any],
    metadata_contract_sha256: str,
) -> None:
    events = value.get("events")
    plan = list(metadata_contract["request_plan"])
    if (
        set(value)
        != {
            "version",
            "completion_kind",
            "metadata_contract_sha256",
            "request_plan_sha256",
            "transport_profile",
            "contract_sha256",
            "price_evidence",
            "events",
            "events_sha256",
        }
        or value.get("version") != SCHEMA_VERSION
        or value.get("completion_kind") != COMPLETION_KIND
        or value.get("metadata_contract_sha256") != metadata_contract_sha256
        or value.get("request_plan_sha256") != metadata_contract["request_plan_sha256"]
        or value.get("transport_profile") != TRANSPORT_PROFILE
        or not isinstance(events, list)
        or len(events) > 8
        or value.get("events_sha256") != _json_sha256(events)
        or (
            value.get("contract_sha256") is not None
            and (
                not isinstance(value.get("contract_sha256"), str)
                or re.fullmatch(r"[0-9a-f]{64}", str(value["contract_sha256"])) is None
            )
        )
    ):
        raise PaidSourceRefreshError("paid metadata ledger形状/绑定漂移")
    per_request: dict[str, list[Mapping[str, Any]]] = {
        str(row["request_id"]): [] for row in plan
    }
    failed_seen = False
    for index, event in enumerate(events):
        if not isinstance(event, Mapping) or event.get("index") != index:
            raise PaidSourceRefreshError("paid metadata ledger事件index漂移")
        request_id = str(event.get("request_id") or "")
        if request_id not in per_request:
            raise PaidSourceRefreshError("paid metadata ledger request_id漂移")
        request_events = per_request[request_id]
        row = next(item for item in plan if item["request_id"] == request_id)
        if failed_seen:
            raise PaidSourceRefreshError("paid metadata失败后仍存在后续事件")
        common = (
            event.get("purpose") == row["purpose"]
            and event.get("endpoint") == row["endpoint"]
        )
        if event.get("phase") == "opening":
            if (
                not common
                or request_events
                or set(event)
                != {
                    "index",
                    "phase",
                    "request_id",
                    "purpose",
                    "endpoint",
                    "query_sha256",
                    "authorization",
                    "created_at",
                }
                or event.get("query_sha256") != _json_sha256(row["query"])
                or event.get("authorization") != row["authorization"]
            ):
                raise PaidSourceRefreshError("paid metadata opening事件漂移")
            _aware_timestamp(event["created_at"], label="paid metadata opening时间")
        elif event.get("phase") == "terminal":
            if not common or len(request_events) != 1 or request_events[0].get("phase") != "opening":
                raise PaidSourceRefreshError("paid metadata terminal缺少唯一opening")
            outcome = event.get("outcome")
            if outcome == "failed":
                if set(event) != {
                    "index",
                    "phase",
                    "request_id",
                    "purpose",
                    "endpoint",
                    "outcome",
                    "error_type",
                    "error_code",
                    "finished_at",
                } or event.get("error_type") not in SAFE_ERROR_TYPES or event.get(
                    "error_code"
                ) != (
                    "endpoint_info_failed"
                    if row["purpose"] == "endpoint_info"
                    else "user_info_failed"
                ):
                    raise PaidSourceRefreshError("paid metadata failure事件漂移")
                failed_seen = True
            elif outcome == "response_received":
                evidence_key = (
                    "record" if row["purpose"] == "endpoint_info" else "balance_evidence"
                )
                if set(event) != {
                    "index",
                    "phase",
                    "request_id",
                    "purpose",
                    "endpoint",
                    "outcome",
                    evidence_key,
                    "finished_at",
                } or not isinstance(event.get(evidence_key), Mapping):
                    raise PaidSourceRefreshError("paid metadata response事件漂移")
            else:
                raise PaidSourceRefreshError("paid metadata terminal outcome漂移")
            _aware_timestamp(event["finished_at"], label="paid metadata terminal时间")
        else:
            raise PaidSourceRefreshError("paid metadata事件phase漂移")
        request_events.append(event)

    price_requests = per_request.values()
    first_three = list(price_requests)[:3]
    price_complete = all(
        len(rows) == 2 and rows[1].get("outcome") == "response_received"
        for rows in first_three
    )
    price_evidence = value.get("price_evidence")
    if price_complete:
        if not isinstance(price_evidence, Mapping):
            raise PaidSourceRefreshError("paid metadata价格终态缺少price evidence")
        validated = _validate_price_evidence(price_evidence)
        terminal_records = [dict(rows[1]["record"]) for rows in first_three]
        if validated["records"] != terminal_records:
            raise PaidSourceRefreshError("paid metadata价格事件与evidence漂移")
    elif price_evidence is not None:
        raise PaidSourceRefreshError("paid metadata价格未闭包却已有evidence")
    user_rows = list(per_request.values())[3]
    if user_rows and not price_complete:
        raise PaidSourceRefreshError("paid metadata user-info早于价格闭包")
    if len(user_rows) == 2 and user_rows[1].get("outcome") == "response_received":
        if value.get("contract_sha256") is None:
            raise PaidSourceRefreshError("paid metadata user-info未绑定refresh contract")
        user_price_record = validated["records"][1]
        _validate_balance_evidence(
            user_rows[1]["balance_evidence"],
            price_record_sha256=_json_sha256(user_price_record),
            price_checked_at=str(validated["checked_at"]),
            require_fresh=False,
        )


def _write_metadata_ledger(
    paths: RefreshPaths,
    metadata_contract: Mapping[str, Any],
    value: Mapping[str, Any],
) -> None:
    _validate_metadata_ledger(
        value,
        metadata_contract=metadata_contract,
        metadata_contract_sha256=_sha256_file(paths.metadata_contract),
    )
    _write_json(paths.metadata_ledger, value, immutable=False)


def _recover_metadata_ledger_temp(
    paths: RefreshPaths, metadata_contract: Mapping[str, Any]
) -> None:
    temporary = paths.metadata_ledger.with_name(f".{paths.metadata_ledger.name}.tmp")
    if not os.path.lexists(temporary):
        return
    contract_sha = _sha256_file(paths.metadata_contract)
    candidate = _read_json(temporary, label="paid metadata ledger临时文件")
    _validate_metadata_ledger(
        candidate,
        metadata_contract=metadata_contract,
        metadata_contract_sha256=contract_sha,
    )
    if paths.metadata_ledger.exists():
        current = _read_json(paths.metadata_ledger, label="paid metadata ledger")
        _validate_metadata_ledger(
            current,
            metadata_contract=metadata_contract,
            metadata_contract_sha256=contract_sha,
        )
        current_events = list(current["events"])
        candidate_events = list(candidate["events"])
        if (
            len(candidate_events) < len(current_events)
            or candidate_events[: len(current_events)] != current_events
            or (
                current.get("price_evidence") is not None
                and current.get("price_evidence") != candidate.get("price_evidence")
            )
            or (
                current.get("contract_sha256") is not None
                and current.get("contract_sha256") != candidate.get("contract_sha256")
            )
        ):
            raise PaidSourceRefreshError("paid metadata ledger临时前缀漂移")
    os.replace(temporary, paths.metadata_ledger)
    local_controller._fsync_directory(paths.metadata_ledger.parent)


def _ensure_metadata_records(
    paths: RefreshPaths,
    *,
    expected_source_db_sha256: str,
    expected_source_completion_sha256: str,
    content_id: int,
) -> Mapping[str, Any]:
    temporary = paths.metadata_contract.with_name(
        f".{paths.metadata_contract.name}.tmp"
    )
    if not paths.metadata_contract.exists() and os.path.lexists(temporary):
        local_controller._recover_immutable_json_temp(
            paths.metadata_contract,
            label="paid metadata contract",
            validator=lambda value: _validate_metadata_contract(
                paths,
                value,
                expected_source_db_sha256=expected_source_db_sha256,
                expected_source_completion_sha256=expected_source_completion_sha256,
                content_id=content_id,
            ),
        )
    elif paths.metadata_contract.exists():
        _cleanup_final_temp(paths.metadata_contract, label="paid metadata contract")
    else:
        tree = _physical_tree(paths.run_root, label="paid metadata contract prefix")
        allowed = {
            paths.local_paths.copy_intent.name,
            paths.local_paths.copy_receipt.name,
            paths.state.name,
        }
        if tree["directories"] or any(name not in allowed for name in tree["files"]):
            raise PaidSourceRefreshError("paid metadata contract前缀存在未知记录")
        _write_json(
            paths.metadata_contract,
            _metadata_contract_value(
                paths,
                expected_source_db_sha256=expected_source_db_sha256,
                expected_source_completion_sha256=expected_source_completion_sha256,
                content_id=content_id,
            ),
            immutable=True,
        )
    metadata_contract = _read_json(
        paths.metadata_contract, label="paid metadata contract"
    )
    _validate_metadata_contract(
        paths,
        metadata_contract,
        expected_source_db_sha256=expected_source_db_sha256,
        expected_source_completion_sha256=expected_source_completion_sha256,
        content_id=content_id,
    )
    ledger_temporary = paths.metadata_ledger.with_name(
        f".{paths.metadata_ledger.name}.tmp"
    )
    if not paths.metadata_ledger.exists() and not os.path.lexists(ledger_temporary):
        tree = _physical_tree(paths.run_root, label="paid metadata ledger prefix")
        allowed = {
            paths.local_paths.copy_intent.name,
            paths.local_paths.copy_receipt.name,
            paths.metadata_contract.name,
            paths.state.name,
        }
        if tree["directories"] or any(name not in allowed for name in tree["files"]):
            raise PaidSourceRefreshError("paid metadata ledger前缀存在未知记录")
        _write_json(
            paths.metadata_ledger,
            _metadata_ledger_value(
                metadata_contract_sha256=_sha256_file(paths.metadata_contract),
                metadata_contract=metadata_contract,
            ),
            immutable=True,
        )
    else:
        _recover_metadata_ledger_temp(paths, metadata_contract)
    ledger = _read_json(paths.metadata_ledger, label="paid metadata ledger")
    _validate_metadata_ledger(
        ledger,
        metadata_contract=metadata_contract,
        metadata_contract_sha256=_sha256_file(paths.metadata_contract),
    )
    return metadata_contract


def _default_endpoint_info_record(endpoint: str, cost: float) -> Mapping[str, Any]:
    query = {"endpoint": endpoint}
    url = (
        f"{providers_module.TIKHUB_BASE}{ENDPOINT_INFO_PATH}?"
        + urllib.parse.urlencode(query)
    )
    payload, transcript = _exact_json_request(
        url,
        expected_path=ENDPOINT_INFO_PATH,
        expected_query=query,
        authorization=None,
    )
    return {
        "queried_endpoint": endpoint,
        "response": transcript,
        "fields": _extract_endpoint_fields(
            payload, expected_endpoint=endpoint, expected_cost=cost
        ),
    }


def _collect_price_evidence(
    paths: RefreshPaths,
    metadata_contract: Mapping[str, Any],
    *,
    endpoint_info_fetcher: Callable[[], Mapping[str, Any]] | None,
) -> Mapping[str, Any]:
    ledger = _read_json(paths.metadata_ledger, label="paid metadata ledger")
    _validate_metadata_ledger(
        ledger,
        metadata_contract=metadata_contract,
        metadata_contract_sha256=_sha256_file(paths.metadata_contract),
    )
    if ledger.get("price_evidence") is not None:
        return _validate_price_evidence(ledger["price_evidence"])
    if ledger["events"]:
        raise PaidSourceRefreshError("endpoint-info metadata request已opening/失败，必须使用fresh v2 root")
    plans = list(metadata_contract["request_plan"][:3])
    events = list(ledger["events"])
    try:
        if endpoint_info_fetcher is not None:
            for plan in plans:
                events.append(_metadata_opening(plan, index=len(events)))
            ledger = {**ledger, "events": events, "events_sha256": _json_sha256(events)}
            _write_metadata_ledger(paths, metadata_contract, ledger)
            price = _validate_price_evidence(dict(endpoint_info_fetcher()))
            for plan, record in zip(plans, price["records"]):
                events.append(
                    _metadata_terminal(plan, index=len(events), evidence=record)
                )
                ledger = {
                    **ledger,
                    "price_evidence": (
                        price if int(plan["ordinal"]) == 2 else ledger["price_evidence"]
                    ),
                    "events": events,
                    "events_sha256": _json_sha256(events),
                }
                _write_metadata_ledger(paths, metadata_contract, ledger)
        else:
            records: list[Mapping[str, Any]] = []
            for plan in plans:
                events.append(_metadata_opening(plan, index=len(events)))
                ledger = {
                    **ledger,
                    "events": events,
                    "events_sha256": _json_sha256(events),
                }
                _write_metadata_ledger(paths, metadata_contract, ledger)
                if records:
                    time.sleep(1.1)
                record = _default_endpoint_info_record(
                    str(plan["query"]["endpoint"]), float(plan["expected_cost"])
                )
                records.append(record)
                final_price = None
                if int(plan["ordinal"]) == 2:
                    final_price = _validate_price_evidence(
                        {
                            "checked_at": _now_text(),
                            "records": records,
                            "records_sha256": _json_sha256(records),
                        }
                    )
                events.append(
                    _metadata_terminal(plan, index=len(events), evidence=record)
                )
                ledger = {
                    **ledger,
                    "price_evidence": final_price,
                    "events": events,
                    "events_sha256": _json_sha256(events),
                }
                _write_metadata_ledger(paths, metadata_contract, ledger)
            price = _validate_price_evidence(dict(ledger["price_evidence"]))
        ledger = {
            **ledger,
            "price_evidence": price,
            "events": events,
            "events_sha256": _json_sha256(events),
        }
        _write_metadata_ledger(paths, metadata_contract, ledger)
        return price
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        current = _read_json(paths.metadata_ledger, label="paid metadata ledger")
        current_events = list(current["events"])
        open_plans = [
            plan
            for plan in plans
            if any(
                event.get("request_id") == plan["request_id"]
                and event.get("phase") == "opening"
                for event in current_events
            )
            and not any(
                event.get("request_id") == plan["request_id"]
                and event.get("phase") == "terminal"
                for event in current_events
            )
        ]
        if open_plans:
            current_events.append(
                _metadata_terminal(open_plans[0], index=len(current_events), error=exc)
            )
            current = {
                **current,
                "events": current_events,
                "events_sha256": _json_sha256(current_events),
            }
            _write_metadata_ledger(paths, metadata_contract, current)
        raise


def _bind_metadata_refresh_contract(
    paths: RefreshPaths, metadata_contract: Mapping[str, Any]
) -> Mapping[str, Any]:
    ledger = _read_json(paths.metadata_ledger, label="paid metadata ledger")
    contract_sha = _sha256_file(paths.contract)
    if ledger.get("contract_sha256") is None:
        ledger = {**ledger, "contract_sha256": contract_sha}
        _write_metadata_ledger(paths, metadata_contract, ledger)
    elif ledger.get("contract_sha256") != contract_sha:
        raise PaidSourceRefreshError("paid metadata ledger refresh contract绑定漂移")
    return ledger


def _ensure_user_info(
    paths: RefreshPaths,
    metadata_contract: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
    key_loader: Callable[[], str] | None,
    balance_checker: Callable[[str, str], Mapping[str, Any]] | None,
) -> tuple[str, Mapping[str, Any], int]:
    ledger = _bind_metadata_refresh_contract(paths, metadata_contract)
    plan = metadata_contract["request_plan"][3]
    user_events = [
        event for event in ledger["events"] if event.get("request_id") == plan["request_id"]
    ]
    user_price_record = contract["price_evidence"]["records"][1]
    user_price_sha256 = _json_sha256(user_price_record)
    if len(user_events) == 2 and user_events[1].get("outcome") == "response_received":
        balance = _validate_balance_evidence(
            user_events[1]["balance_evidence"],
            price_record_sha256=user_price_sha256,
            price_checked_at=str(contract["price_evidence"]["checked_at"]),
        )
        key = (key_loader or _load_key)()
        if not key:
            raise PaidSourceRefreshError("TikHub key为空")
        return key, balance, 0
    if user_events:
        raise PaidSourceRefreshError("user-info metadata request已opening/失败，必须使用fresh v2 root")
    key = (key_loader or _load_key)()
    if not key:
        raise PaidSourceRefreshError("TikHub key为空")
    events = list(ledger["events"])
    events.append(_metadata_opening(plan, index=len(events)))
    ledger = {**ledger, "events": events, "events_sha256": _json_sha256(events)}
    _write_metadata_ledger(paths, metadata_contract, ledger)
    try:
        balance = _validate_balance_evidence(
            dict((balance_checker or _default_balance_check)(key, user_price_sha256)),
            price_record_sha256=user_price_sha256,
            price_checked_at=str(contract["price_evidence"]["checked_at"]),
        )
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        events.append(_metadata_terminal(plan, index=len(events), error=exc))
        ledger = {**ledger, "events": events, "events_sha256": _json_sha256(events)}
        _write_metadata_ledger(paths, metadata_contract, ledger)
        raise
    events.append(_metadata_terminal(plan, index=len(events), evidence=balance))
    ledger = {**ledger, "events": events, "events_sha256": _json_sha256(events)}
    _write_metadata_ledger(paths, metadata_contract, ledger)
    return key, balance, 1


def _load_key() -> str:
    return providers_module._load_key(
        providers_module.TIKHUB_KEY_FILE, "TIKHUB_API_KEY"
    )


def _default_balance_check(key: str, price_evidence_sha256: str) -> Mapping[str, Any]:
    url = f"{providers_module.TIKHUB_BASE}{USER_INFO_PATH}"
    payload, transcript = _exact_json_request(
        url,
        expected_path=USER_INFO_PATH,
        expected_query={},
        authorization=key,
        maximum_bytes=1024 * 1024,
    )
    if (
        not isinstance(payload, Mapping)
        or type(payload.get("code")) is not int
        or payload.get("code") != 200
    ):
        raise PaidSourceRefreshError("TikHub user-info认证失败")
    user_data = payload.get("user_data")
    if not isinstance(user_data, Mapping):
        raise PaidSourceRefreshError("TikHub user-info缺少exact user_data")
    if "balance" not in user_data or "free_credit" not in user_data:
        raise PaidSourceRefreshError("TikHub user-info余额字段缺失")
    balance_decimal = _exact_decimal(
        user_data["balance"], label="TikHub balance"
    )
    free_credit_decimal = _exact_decimal(
        user_data["free_credit"], label="TikHub free_credit"
    )
    if (
        balance_decimal < 0
        or free_credit_decimal < 0
        or balance_decimal + free_credit_decimal < Decimal(str(UNIT_PRICE))
        or (
            "account_disabled" in user_data
            and user_data["account_disabled"] is not False
        )
        or ("is_active" in user_data and user_data["is_active"] is not True)
    ):
        raise PaidSourceRefreshError("TikHub认证通过但余额证据不足")
    return {
        "checked_at": _now_text(),
        "endpoint": USER_INFO_PATH,
        "endpoint_cost": 0.0,
        "balance_sufficient": True,
        "response_sha256": transcript["response_sha256"],
        "response_bytes": transcript["response_bytes"],
        "price_evidence_sha256": price_evidence_sha256,
    }


def _default_detail_fetch(
    platform_content_id: str, key: str
) -> tuple[capture_module.ProviderResult, Mapping[str, Any]]:
    query = {"aweme_id": platform_content_id}
    url = (
        f"{providers_module.TIKHUB_BASE}{DETAIL_PATH}?"
        + urllib.parse.urlencode(query)
    )
    payload, transcript = _exact_json_request(
        url,
        expected_path=DETAIL_PATH,
        expected_query=query,
        authorization=key,
    )
    return payload, {
        **transcript,
        "response_json_sha256": hashlib.sha256(
            capture_module.canonical_json_bytes(
                capture_module._scrub_secrets(payload)
            )
        ).hexdigest(),
        "endpoint": DETAIL_PATH,
        "aweme_id": platform_content_id,
    }


def _provider_item(payload: Any, platform_content_id: str) -> Mapping[str, Any]:
    data = providers_module._tikhub_douyin_data(payload)
    item = providers_module._find_aweme(data, platform_content_id)
    if item is None:
        raise PaidSourceRefreshError("live raw缺少请求的Douyin item")
    return item


def _music_urls(item: Mapping[str, Any]) -> list[str]:
    music = item.get("music")
    if not isinstance(music, Mapping):
        return []
    output: list[str] = []

    def walk(value: Any) -> None:
        if isinstance(value, str) and value.lower().startswith(("http://", "https://")):
            normalized = media_module._normalize_media_url(value)
            if normalized is not None and normalized not in output:
                output.append(normalized)
        elif isinstance(value, Mapping):
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(music)
    return output


def _validate_live_payload(
    *,
    payload: Any,
    data: Mapping[str, Any],
    target: Mapping[str, Any],
) -> Mapping[str, Any]:
    platform_content_id = str(target["platform_content_id"])
    item = _provider_item(payload, platform_content_id)
    if data.get("content_type") != "video":
        raise PaidSourceRefreshError("live detail不是video内容")
    live_title = str(data.get("title") or "")
    live_body = str(data.get("body") or "")
    frozen_descriptions = {
        str(value)
        for value in (target.get("title"), target.get("body"))
        if str(value or "")
    }
    if (
        not live_title
        or live_title != live_body
        or live_title not in frozen_descriptions
    ):
        raise PaidSourceRefreshError("live detail文字与冻结content发生变化")
    video_urls, source_sha256 = media_module._media_source_identity(
        "video", data.get("media_urls") or []
    )
    if not video_urls:
        raise PaidSourceRefreshError("live detail没有可用video URL")
    music_urls = _music_urls(item)
    intersection = sorted(set(video_urls).intersection(music_urls))
    if intersection:
        raise PaidSourceRefreshError("live video URL与music.play_url重叠")
    url_rows = [
        local_controller._safe_url(
            url,
            media_kind="video",
            platform=str(target["platform"]),
            provider=PROVIDER,
            operation=OPERATION,
        )
        for url in video_urls
    ]
    allowed = [str(row["url"]) for row in url_rows if row["network_allowed"]]
    if not allowed:
        raise PaidSourceRefreshError("live detail没有allowlisted HTTPS video URL")
    return {
        "video_urls": video_urls,
        "video_urls_sha256": _json_sha256(video_urls),
        "music_urls": music_urls,
        "music_urls_sha256": _json_sha256(music_urls),
        "intersection_count": 0,
        "allowed_urls": allowed,
        "allowed_urls_sha256": _json_sha256(allowed),
        "source_sha256": source_sha256,
        "item_sha256": _json_sha256(item),
    }


def _raw_path(row: Mapping[str, Any]) -> Path:
    raw = Path(str(row["local_path"]))
    return raw if raw.is_absolute() else storage_module.PROJECT_ROOT / raw


def _validate_refresh_materialization(
    paths: RefreshPaths,
    contract: Mapping[str, Any],
) -> Mapping[str, Any]:
    target = contract["target"]
    content_id = int(target["id"])
    route = contract["route"]
    with closing(local_controller._immutable_connection(paths.database)) as connection:
        slots = connection.execute(
            "SELECT * FROM fetch_slots WHERE content_id=? AND stage=? ORDER BY id",
            (content_id, STAGE),
        ).fetchall()
        if len(slots) != 1:
            raise PaidSourceRefreshError("paid refresh slot数量不精确")
        slot = slots[0]
        attempts = connection.execute(
            "SELECT * FROM fetch_attempts WHERE slot_id=? ORDER BY attempt_number",
            (int(slot["id"]),),
        ).fetchall()
        if len(attempts) != 1:
            raise PaidSourceRefreshError("paid refresh attempt数量不精确")
        attempt = attempts[0]
        raws = connection.execute(
            "SELECT * FROM provider_raw_responses WHERE fetch_attempt_id=? ORDER BY id",
            (int(attempt["id"]),),
        ).fetchall()
        if len(raws) != 1:
            raise PaidSourceRefreshError("paid refresh raw数量不精确")
        raw = raws[0]
        if (
            str(slot["stage"]) != STAGE
            or str(slot["window_key"]) != route["window_key"]
            or str(slot["provider"]) != PROVIDER
            or str(slot["adapter_version"]) != ADAPTER_VERSION
            or str(slot["status"]) != "succeeded"
            or _exact_integer(slot["attempt_count"], label="slot attempt count")
            != 1
            or _exact_integer(attempt["attempt_number"], label="attempt number")
            != 1
            or _exact_integer(attempt["http_status"], label="attempt http status")
            != 200
            or _exact_integer(attempt["billed"], label="attempt billed") != 1
            or _exact_decimal(attempt["amount"], label="attempt amount")
            != Decimal(str(UNIT_PRICE))
            or str(attempt["currency"] or "") != "USD"
            or attempt["error_code"] is not None
            or attempt["error_message"] is not None
            or _exact_integer(raw["content_id"], label="raw content id")
            != content_id
            or str(raw["provider"]) != PROVIDER
            or str(raw["operation"]) != OPERATION
            or str(raw["source"]) != "live_applied"
            or _exact_integer(raw["http_status"], label="raw http status") != 200
        ):
            raise PaidSourceRefreshError("paid refresh slot/attempt/raw语义漂移")
        raw_path = _raw_path(raw).resolve()
        local_controller._assert_no_symlink_components(
            raw_path, label="paid refresh raw response"
        )
        raw_file = _file_evidence(raw_path, label="paid refresh raw response")
        if (
            not local_controller._is_within(raw_path, paths.raw_root)
            or raw_file["sha256"] != raw["sha256"]
            or raw_file["byte_size"]
            != _exact_integer(raw["byte_size"], label="raw byte size")
        ):
            raise PaidSourceRefreshError("paid refresh raw文件/DB/root绑定漂移")
        try:
            raw_body = json.loads(raw_path.read_bytes())
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PaidSourceRefreshError("paid refresh raw不是合法JSON") from exc
        parsed = providers_module._parse_douyin_stage_payload(
            "detail", str(target["platform_content_id"]), raw_body, status=200
        )
        provenance = _validate_live_payload(
            payload=raw_body, data=parsed.data, target=target
        )
        artifacts = connection.execute(
            "SELECT * FROM evidence_artifacts WHERE content_id=? "
            "AND artifact_type='media_source' ORDER BY id",
            (content_id,),
        ).fetchall()
        prior_ids = {int(value) for value in contract["baseline_artifact_ids"]}
        new_artifacts = [row for row in artifacts if int(row["id"]) not in prior_ids]
        if len(new_artifacts) != 1:
            raise PaidSourceRefreshError("paid refresh media_source新增行数量不精确")
        artifact = new_artifacts[0]
        artifact_path_raw = Path(str(artifact["local_path"]))
        artifact_path = (
            artifact_path_raw
            if artifact_path_raw.is_absolute()
            else storage_module.PROJECT_ROOT / artifact_path_raw
        ).resolve()
        local_controller._assert_no_symlink_components(
            artifact_path, label="paid refresh media_source"
        )
        artifact_file = _file_evidence(
            artifact_path, label="paid refresh media_source"
        )
        if (
            str(artifact["status"]) != "available"
            or str(artifact["processor_version"]) != media_module.MEDIA_SOURCE_VERSION
            or not local_controller._is_within(artifact_path, paths.media_root)
            or artifact_file["sha256"] != artifact["sha256"]
            or artifact_file["byte_size"]
            != _exact_integer(artifact["byte_size"], label="artifact byte size")
        ):
            raise PaidSourceRefreshError("paid refresh media_source文件/DB/root漂移")
        try:
            manifest = json.loads(artifact_path.read_bytes())
            metadata = json.loads(str(artifact["metadata_json"]))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PaidSourceRefreshError("paid refresh media_source JSON无效") from exc
        expected_manifest_keys = {
            "schema_version",
            "media_kind",
            "urls",
            "source_sha256",
            "raw_response_id",
            "captured_at",
        }
        expected_metadata = {
            "media_kind": "video",
            "source_count": len(provenance["video_urls"]),
            "source_sha256": provenance["source_sha256"],
            "raw_response_id": int(raw["id"]),
        }
        if (
            not isinstance(manifest, Mapping)
            or set(manifest) != expected_manifest_keys
            or manifest["schema_version"] != media_module.MEDIA_SOURCE_VERSION
            or manifest["media_kind"] != "video"
            or manifest["urls"] != provenance["video_urls"]
            or manifest["source_sha256"] != provenance["source_sha256"]
            or _exact_integer(
                manifest["raw_response_id"], label="manifest raw response id"
            )
            != _exact_integer(raw["id"], label="raw id")
            or not isinstance(metadata, Mapping)
            or set(metadata)
            != {"media_kind", "source_count", "source_sha256", "raw_response_id"}
            or metadata.get("media_kind") != expected_metadata["media_kind"]
            or _exact_integer(
                metadata.get("source_count"), label="artifact metadata source count"
            )
            != expected_metadata["source_count"]
            or metadata.get("source_sha256") != expected_metadata["source_sha256"]
            or _exact_integer(
                metadata.get("raw_response_id"),
                label="artifact metadata raw response id",
            )
            != expected_metadata["raw_response_id"]
        ):
            raise PaidSourceRefreshError("paid refresh media_source正文/metadata漂移")
        budget_rows = connection.execute(
            "SELECT * FROM provider_budget_batches WHERE id=?",
            (contract["budget"]["budget_id"],),
        ).fetchall()
        usage_rows = connection.execute(
            "SELECT * FROM provider_usage WHERE task_id=? ORDER BY id",
            (contract["budget"]["task_id"],),
        ).fetchall()
        if len(budget_rows) != 1 or len(usage_rows) != 1:
            raise PaidSourceRefreshError("paid refresh预算/usage数量不精确")
        budget = budget_rows[0]
        usage = usage_rows[0]
        try:
            usage_details = json.loads(str(usage["details_json"]))
        except json.JSONDecodeError as exc:
            raise PaidSourceRefreshError("paid refresh usage details无效") from exc
        if (
            str(budget["provider"]) != PROVIDER
            or str(budget["operation"]) != OPERATION
            or str(budget["purpose"]) != contract["budget"]["purpose"]
            or str(budget["currency"]) != "USD"
            or _exact_decimal(
                budget["verified_unit_price"], label="budget verified unit price"
            )
            != Decimal(str(UNIT_PRICE))
            or str(budget["price_verified_at"])
            != contract["budget"]["price_verified_at"]
            or _exact_integer(
                budget["max_billable_requests"], label="budget max billable requests"
            )
            != 1
            or _exact_decimal(budget["max_amount"], label="budget max amount")
            != Decimal(str(UNIT_PRICE))
            or _exact_integer(budget["pilot_size"], label="budget pilot size") != 1
            or _exact_integer(budget["daily_quota"], label="budget daily quota")
            != 1
            or _exact_integer(
                budget["consumed_requests"], label="budget consumed requests"
            )
            != 1
            or _exact_decimal(
                budget["consumed_amount"], label="budget consumed amount"
            )
            != Decimal(str(UNIT_PRICE))
            or str(budget["status"]) != "approved"
            or str(usage["budget_batch_id"]) != str(budget["id"])
            or str(usage["provider"]) != PROVIDER
            or str(usage["operation"]) != OPERATION
            or _exact_integer(
                usage["request_attempts"], label="usage request attempts"
            )
            != 1
            or _exact_integer(
                usage["billed_requests"], label="usage billed requests"
            )
            != 1
            or str(usage["currency"]) != "USD"
            or _exact_decimal(usage["amount"], label="usage amount")
            != Decimal(str(UNIT_PRICE))
            or _canonical_bytes(usage_details)
            != _canonical_bytes(
                {
                    "http_status": 200,
                    "slot_id": _exact_integer(slot["id"], label="slot id"),
                    "state": "completed",
                }
            )
        ):
            raise PaidSourceRefreshError("paid refresh预算或usage语义漂移")
        return {
            "slot": _row_values(slot),
            "attempt": _row_values(attempt),
            "raw_response": _row_values(raw),
            "raw_response_file": raw_file,
            "raw_response_body_sha256": _json_sha256(raw_body),
            "raw_item_sha256": provenance["item_sha256"],
            "artifact": _row_values(artifact),
            "artifact_file": artifact_file,
            "artifact_body": manifest,
            "url_provenance": provenance,
            "budget": _row_values(budget),
            "usage": _row_values(usage),
            "usage_details_sha256": _json_sha256(usage_details),
        }


def _validate_database_delta(
    paths: RefreshPaths,
    contract: Mapping[str, Any],
    materialized: Mapping[str, Any],
) -> Mapping[str, Any]:
    parent_path = Path(str(contract["base_source"]["database"]["path"]))
    with (
        closing(local_controller._immutable_connection(parent_path)) as before,
        closing(local_controller._immutable_connection(paths.database)) as after,
    ):
        before_tables = _table_names(before)
        after_tables = _table_names(after)
        if (
            before_tables != after_tables
            or _schema_evidence(before) != _schema_evidence(after)
        ):
            raise PaidSourceRefreshError("paid refresh数据库schema表集合漂移")
        protected: dict[str, Mapping[str, Any]] = {}
        for table in before_tables:
            if table in ALLOWED_DELTA_TABLES:
                continue
            before_rows = _rows(before, table)
            after_rows = _rows(after, table)
            if before_rows != after_rows:
                raise PaidSourceRefreshError(f"paid refresh保护表发生变化：{table}")
            protected[table] = {
                "rows": len(after_rows),
                "sha256": _json_sha256(after_rows),
            }
        new_rows = {
            table: _new_rows(
                before,
                after,
                table,
                "id",
            )
            for table in ALLOWED_DELTA_TABLES
        }
        if any(len(rows) != 1 for rows in new_rows.values()):
            raise PaidSourceRefreshError("paid refresh六张allowed表不是各新增一行")
        expected_ids = {
            "provider_budget_batches": str(materialized["budget"]["id"]),
            "provider_usage": str(materialized["usage"]["id"]),
            "fetch_slots": str(materialized["slot"]["id"]),
            "fetch_attempts": str(materialized["attempt"]["id"]),
            "provider_raw_responses": str(materialized["raw_response"]["id"]),
            "evidence_artifacts": str(materialized["artifact"]["id"]),
        }
        for table, rows in new_rows.items():
            if str(rows[0]["id"]) != expected_ids[table]:
                raise PaidSourceRefreshError(f"paid refresh {table}新增行身份漂移")
        before_sequences = {
            str(row["name"]): _exact_integer(
                row["seq"], label=f"baseline sqlite_sequence {row['name']}"
            )
            for row in before.execute("SELECT name,seq FROM sqlite_sequence ORDER BY name")
        }
        after_sequences = {
            str(row["name"]): _exact_integer(
                row["seq"], label=f"current sqlite_sequence {row['name']}"
            )
            for row in after.execute("SELECT name,seq FROM sqlite_sequence ORDER BY name")
        }
        names = set(before_sequences) | set(after_sequences)
        for name in names:
            before_value = before_sequences.get(name, 0)
            after_value = after_sequences.get(name, 0)
            expected = before_value + (1 if name in AUTOINCREMENT_DELTA_TABLES else 0)
            if after_value != expected:
                raise PaidSourceRefreshError(
                    f"paid refresh sqlite_sequence非精确增量：{name}"
                )
    baseline = contract.get("database_baseline")
    current_baseline = _database_baseline(parent_path)
    if baseline != current_baseline:
        raise PaidSourceRefreshError("paid refresh parent数据库baseline漂移")
    return {
        "protected": protected,
        "protected_sha256": _json_sha256(protected),
        "new_rows": new_rows,
        "new_rows_sha256": _json_sha256(new_rows),
        "sqlite_sequence_before": before_sequences,
        "sqlite_sequence_after": after_sequences,
    }


def _inventory(paths: RefreshPaths) -> Mapping[str, Any]:
    raw_inventory = local_controller._inventory(paths.raw_root)
    media_inventory = local_controller._inventory(paths.media_root)
    run_inventory = local_controller._inventory(paths.run_root)
    if raw_inventory["files"] != 1 or media_inventory["files"] != 1:
        raise PaidSourceRefreshError("paid refresh raw/media输出不唯一")
    local_controller._require_clean_database(paths.database)
    return {
        "raw": raw_inventory,
        "media": media_inventory,
        "run": run_inventory,
    }


def _physical_tree(root: Path, *, label: str) -> Mapping[str, list[str]]:
    local_controller._private_directory(root, label=label)
    files: list[str] = []
    directories: list[str] = []
    for current, directory_names, file_names in os.walk(root, followlinks=False):
        current_path = Path(current)
        for name in list(directory_names):
            path = current_path / name
            if path.is_symlink() or not path.is_dir():
                raise PaidSourceRefreshError(f"{label}含非私有目录：{path}")
            directories.append(str(path.relative_to(root)))
        for name in file_names:
            path = current_path / name
            local_controller._private_file(path, label=f"{label}文件")
            files.append(str(path.relative_to(root)))
    return {"files": sorted(files), "directories": sorted(directories)}


def _expected_parent_directories(path: Path, root: Path) -> list[str]:
    output: list[str] = []
    current = path.parent
    while current != root:
        if not local_controller._is_within(current, root):
            raise PaidSourceRefreshError("paid refresh输出路径逃逸")
        output.append(str(current.relative_to(root)))
        current = current.parent
    return sorted(output)


def _manifest_candidate(
    paths: RefreshPaths,
    contract: Mapping[str, Any],
    *,
    raw_response_id: int,
    data: Mapping[str, Any],
) -> tuple[Path, list[str], str]:
    urls, source_sha256 = media_module._media_source_identity(
        "video", data.get("media_urls") or []
    )
    candidate = (
        paths.media_root
        / str(contract["target"]["link_id"])
        / "sources"
        / f"source-{raw_response_id}-{source_sha256[:12]}.json"
    )
    return candidate, urls, source_sha256


def _validate_orphan_manifest_file(
    path: Path,
    *,
    urls: Sequence[str],
    source_sha256: str,
    raw_response_id: int,
    label: str,
) -> Mapping[str, Any]:
    local_controller._private_file(path, label=label)
    try:
        body = json.loads(path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PaidSourceRefreshError(f"{label}不是JSON") from exc
    if not isinstance(body, Mapping):
        raise PaidSourceRefreshError(f"{label}不是JSON object")
    try:
        captured_at = datetime.fromisoformat(
            str(body.get("captured_at") or "").replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise PaidSourceRefreshError(f"{label} captured_at无效") from exc
    if (
        set(body)
        != {
            "schema_version",
            "media_kind",
            "urls",
            "source_sha256",
            "raw_response_id",
            "captured_at",
        }
        or body.get("schema_version") != media_module.MEDIA_SOURCE_VERSION
        or body.get("media_kind") != "video"
        or body.get("urls") != list(urls)
        or body.get("source_sha256") != source_sha256
        or _exact_integer(
            body.get("raw_response_id"), label=f"{label} raw response id"
        )
        != raw_response_id
        or captured_at.tzinfo is None
    ):
        raise PaidSourceRefreshError(f"{label}语义漂移")
    return body


def _validate_precontract_run_root(paths: RefreshPaths) -> None:
    tree = _physical_tree(paths.run_root, label="paid refresh precontract run root")
    expected = sorted(
        [
            paths.local_paths.copy_intent.name,
            paths.local_paths.copy_receipt.name,
        ]
    )
    if tree != {"files": expected, "directories": []}:
        raise PaidSourceRefreshError("paid refresh precontract run root存在未知项")


def _validate_pristine_record_prefix(
    paths: RefreshPaths,
    contract: Mapping[str, Any],
    *,
    allowed_run_names: Sequence[str],
) -> None:
    if local_controller._database_sidecars(paths.database):
        raise PaidSourceRefreshError("paid refresh记录恢复前数据库存在sidecar")
    if local_controller._database_identity(paths.database) != contract["database"]:
        raise PaidSourceRefreshError("paid refresh记录恢复前数据库身份漂移")
    rows = _validate_database_prefix(paths, contract)
    if any(rows.values()):
        raise PaidSourceRefreshError("paid refresh记录恢复前DB已存在provider活动")
    if _physical_tree(paths.raw_root, label="paid refresh pristine raw root") != {
        "files": [],
        "directories": [],
    } or _physical_tree(paths.media_root, label="paid refresh pristine media root") != {
        "files": [],
        "directories": [],
    }:
        raise PaidSourceRefreshError("paid refresh记录恢复前输出根非空")
    if _physical_tree(paths.run_root, label="paid refresh pristine run root") != {
        "files": sorted(allowed_run_names),
        "directories": [],
    }:
        raise PaidSourceRefreshError("paid refresh记录恢复前run root漂移")


def _validate_output_prefix(
    paths: RefreshPaths,
    contract: Mapping[str, Any],
    new_rows: Mapping[str, list[Mapping[str, Any]]],
) -> None:
    artifact_row = next(iter(new_rows["evidence_artifacts"]), None)
    raw_row = next(iter(new_rows["provider_raw_responses"]), None)
    slot_row = next(iter(new_rows["fetch_slots"]), None)

    run_tree = _physical_tree(paths.run_root, label="paid refresh run root")
    allowed_run_names = {
        paths.local_paths.copy_intent.name,
        paths.local_paths.copy_receipt.name,
        paths.metadata_contract.name,
        paths.metadata_ledger.name,
        paths.contract.name,
        paths.intent.name,
        paths.ledger.name,
        paths.state.name,
        paths.receipt.name,
        paths.completion.name,
    }
    if artifact_row is not None:
        allowed_run_names.update(
            {
                f".{paths.state.name}.tmp",
                f".{paths.receipt.name}.tmp",
                f".{paths.completion.name}.tmp",
            }
        )
    if run_tree["directories"] or any(
        name not in allowed_run_names for name in run_tree["files"]
    ):
        raise PaidSourceRefreshError("paid refresh run root物理prefix存在未知项")
    for required in (
        paths.local_paths.copy_intent.name,
        paths.local_paths.copy_receipt.name,
        paths.metadata_contract.name,
        paths.metadata_ledger.name,
        paths.contract.name,
        paths.intent.name,
        paths.ledger.name,
    ):
        if required not in run_tree["files"]:
            raise PaidSourceRefreshError("paid refresh run root缺少必需记录")
    if artifact_row is None and any(
        name in run_tree["files"]
        for name in (paths.receipt.name, paths.completion.name)
    ):
        raise PaidSourceRefreshError("paid refresh未完成DB却已有receipt/completion")

    raw_tree = _physical_tree(paths.raw_root, label="paid refresh raw root")
    if raw_row is None:
        expected_raw = {"files": [], "directories": []}
    else:
        raw_path = _raw_path(raw_row).resolve()
        expected_raw = {
            "files": [str(raw_path.relative_to(paths.raw_root))],
            "directories": _expected_parent_directories(raw_path, paths.raw_root),
        }
    if raw_tree != expected_raw:
        raise PaidSourceRefreshError("paid refresh raw root物理prefix漂移")

    media_tree = _physical_tree(paths.media_root, label="paid refresh media root")
    expected_media = {"files": [], "directories": []}
    if artifact_row is not None:
        artifact_path_raw = Path(str(artifact_row["local_path"]))
        artifact_path = (
            artifact_path_raw
            if artifact_path_raw.is_absolute()
            else storage_module.PROJECT_ROOT / artifact_path_raw
        ).resolve()
        expected_media = {
            "files": [str(artifact_path.relative_to(paths.media_root))],
            "directories": _expected_parent_directories(
                artifact_path, paths.media_root
            ),
        }
    elif raw_row is not None and slot_row is not None and slot_row.get(
        "status"
    ) == "succeeded":
        committed = _committed_raw(paths, contract)
        if committed is None:
            raise PaidSourceRefreshError("paid refresh orphan manifest缺少committed raw")
        raw_response_id, data, _ = committed
        candidate, urls, source_sha256 = _manifest_candidate(
            paths,
            contract,
            raw_response_id=raw_response_id,
            data=data,
        )
        temporary = candidate.with_name(f".{candidate.name}.tmp")
        directories = _expected_parent_directories(candidate, paths.media_root)
        expected_media = {"files": [], "directories": directories}
        if candidate.exists():
            _validate_orphan_manifest_file(
                candidate,
                urls=urls,
                source_sha256=source_sha256,
                raw_response_id=raw_response_id,
                label="paid refresh orphan manifest",
            )
            expected_media = {
                "files": [str(candidate.relative_to(paths.media_root))],
                "directories": directories,
            }
        elif temporary.exists():
            _validate_orphan_manifest_file(
                temporary,
                urls=urls,
                source_sha256=source_sha256,
                raw_response_id=raw_response_id,
                label="paid refresh orphan manifest temp",
            )
            expected_media = {
                "files": [str(temporary.relative_to(paths.media_root))],
                "directories": directories,
            }
        elif not media_tree["files"] and set(media_tree["directories"]).issubset(
            directories
        ):
            expected_media = media_tree
    if media_tree != expected_media:
        raise PaidSourceRefreshError("paid refresh media root物理prefix漂移")


def _ledger_value(
    *,
    contract_sha256: str,
    intent_sha256: str,
    request_id: str,
    balance_check: Mapping[str, Any] | None = None,
    events: Sequence[Mapping[str, Any]] = (),
) -> Mapping[str, Any]:
    rows = list(events)
    return {
        "version": SCHEMA_VERSION,
        "completion_kind": COMPLETION_KIND,
        "contract_sha256": contract_sha256,
        "intent_sha256": intent_sha256,
        "request_id": request_id,
        "attempt_consumed": bool(rows),
        "balance_check": dict(balance_check) if balance_check is not None else None,
        "events": rows,
        "events_sha256": _json_sha256(rows),
    }


def _validate_ledger(value: Mapping[str, Any], *, terminal: bool) -> None:
    expected_keys = {
        "version",
        "completion_kind",
        "contract_sha256",
        "intent_sha256",
        "request_id",
        "attempt_consumed",
        "balance_check",
        "events",
        "events_sha256",
    }
    events = value.get("events")
    if (
        set(value) != expected_keys
        or value.get("version") != SCHEMA_VERSION
        or value.get("completion_kind") != COMPLETION_KIND
        or not isinstance(events, list)
        or value.get("events_sha256") != _json_sha256(events)
        or bool(value.get("attempt_consumed")) != bool(events)
    ):
        raise PaidSourceRefreshError("paid refresh provider ledger形状漂移")
    if len(events) > 2 or (terminal and len(events) != 2):
        raise PaidSourceRefreshError("paid refresh provider ledger事件数量漂移")
    if (events and not isinstance(value.get("balance_check"), Mapping)) or (
        not events and value.get("balance_check") is not None
    ):
        raise PaidSourceRefreshError("paid refresh provider ledger余额证据漂移")
    if events:
        opening = events[0]
        if (
            set(opening)
            != {"index", "phase", "request_id", "endpoint", "aweme_id", "created_at"}
            or opening.get("index") != 0
            or opening.get("phase") != "opening"
            or opening.get("request_id") != value.get("request_id")
            or opening.get("endpoint") != DETAIL_PATH
            or not str(opening.get("aweme_id") or "")
        ):
            raise PaidSourceRefreshError("paid refresh opening事件漂移")
        _aware_timestamp(opening["created_at"], label="paid refresh opening时间")
    if len(events) == 2:
        terminal_event = events[1]
        success_keys = {
            "index",
            "phase",
            "request_id",
            "endpoint",
            "outcome",
            "url_sha256",
            "response_sha256",
            "response_json_sha256",
            "response_bytes",
            "http_status",
            "mime_type",
            "aweme_id",
            "finished_at",
        }
        failed_keys = {
            "index",
            "phase",
            "request_id",
            "endpoint",
            "outcome",
            "error_type",
            "error_code",
            "finished_at",
        }
        common_invalid = (
            terminal_event.get("index") != 1
            or terminal_event.get("phase") != "terminal"
            or terminal_event.get("request_id") != value.get("request_id")
            or terminal_event.get("endpoint") != DETAIL_PATH
        )
        outcome = terminal_event.get("outcome")
        if common_invalid or (
            outcome == "response_received"
            and (
                set(terminal_event) != success_keys
                or type(terminal_event.get("http_status")) is not int
                or terminal_event.get("http_status") != 200
                or type(terminal_event.get("response_bytes")) is not int
                or terminal_event.get("response_bytes") <= 0
                or terminal_event.get("mime_type") != "application/json"
                or terminal_event.get("aweme_id") != opening.get("aweme_id")
                or any(
                    not isinstance(terminal_event.get(name), str)
                    or re.fullmatch(
                        r"[0-9a-f]{64}", str(terminal_event.get(name))
                    )
                    is None
                    for name in (
                        "url_sha256",
                        "response_sha256",
                        "response_json_sha256",
                    )
                )
            )
        ) or (
            outcome == "transport_failed"
            and (
                set(terminal_event) != failed_keys
                or terminal_event.get("error_type")
                not in SAFE_ERROR_TYPES | {"RecoveredTransportFailure"}
                or terminal_event.get("error_code") != "detail_transport_failed"
            )
        ) or outcome not in {"response_received", "transport_failed"}:
            raise PaidSourceRefreshError("paid refresh terminal事件漂移")
        _aware_timestamp(
            terminal_event["finished_at"], label="paid refresh terminal时间"
        )


def _validate_bound_ledger(
    value: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
    terminal: bool,
) -> None:
    _validate_ledger(value, terminal=terminal)
    events = list(value["events"])
    expected_aweme_id = str(contract["target"]["platform_content_id"])
    if events and events[0].get("aweme_id") != expected_aweme_id:
        raise PaidSourceRefreshError("paid refresh ledger未绑定contract target")
    if events:
        user_price_record = contract["price_evidence"]["records"][1]
        _validate_balance_evidence(
            value["balance_check"],
            price_record_sha256=_json_sha256(user_price_record),
            price_checked_at=str(contract["price_evidence"]["checked_at"]),
            require_fresh=False,
        )
    if len(events) == 2 and events[1].get("outcome") == "response_received":
        if (
            events[1].get("aweme_id") != expected_aweme_id
            or events[1].get("url_sha256")
            != _request_url_sha256(
                DETAIL_PATH, {"aweme_id": expected_aweme_id}
            )
        ):
            raise PaidSourceRefreshError("paid refresh ledger未绑定exact detail URL")


def _parent_source_evidence(
    paths: RefreshPaths,
    *,
    content_id: int,
    expected_source_db_sha256: str,
    expected_source_completion_sha256: str,
) -> Mapping[str, Any]:
    return local_controller._source_completion_evidence(
        paths.local_paths,
        content_ids=[content_id],
        expected_source_db_sha256=expected_source_db_sha256,
        expected_source_completion_sha256=expected_source_completion_sha256,
    )


def _build_contract(
    paths: RefreshPaths,
    *,
    content_id: int,
    source_evidence: Mapping[str, Any],
    plan: Mapping[str, Any],
    price_evidence: Mapping[str, Any],
    identity: Mapping[str, str],
) -> Mapping[str, Any]:
    metadata_contract = _read_json(
        paths.metadata_contract, label="paid metadata contract"
    )
    metadata_ledger = _read_json(paths.metadata_ledger, label="paid metadata ledger")
    _validate_metadata_ledger(
        metadata_ledger,
        metadata_contract=metadata_contract,
        metadata_contract_sha256=_sha256_file(paths.metadata_contract),
    )
    if (
        metadata_ledger.get("price_evidence") != price_evidence
        or len(metadata_ledger["events"]) != 6
        or any(
            event.get("outcome") != "response_received"
            for event in metadata_ledger["events"]
            if event.get("phase") == "terminal"
        )
    ):
        raise PaidSourceRefreshError("paid refresh contract缺少metadata价格强闭包")
    baseline = _database_baseline(paths.database)
    with closing(local_controller._immutable_connection(paths.database)) as connection:
        baseline_artifact_ids = [
            int(row[0])
            for row in connection.execute(
                "SELECT id FROM evidence_artifacts ORDER BY id"
            )
        ]
    database = local_controller._database_identity(paths.database)
    return {
        "version": SCHEMA_VERSION,
        "completion_kind": COMPLETION_KIND,
        "created_at": _now_text(),
        "run_root": str(paths.run_root),
        "source_database": source_evidence["database"],
        "base_source": source_evidence,
        "database": database,
        "copy_records": {
            "intent_sha256": _sha256_file(paths.local_paths.copy_intent),
            "receipt_sha256": _sha256_file(paths.local_paths.copy_receipt),
        },
        "roots": {
            "raw_root": str(paths.raw_root),
            "media_root": str(paths.media_root),
        },
        "metadata": {
            "metadata_contract_path": str(paths.metadata_contract),
            "metadata_contract_sha256": _sha256_file(paths.metadata_contract),
            "metadata_ledger_path": str(paths.metadata_ledger),
            "price_prefix_sha256": _json_sha256(metadata_ledger["events"]),
            "request_plan_sha256": metadata_contract["request_plan_sha256"],
            "transport_profile_sha256": metadata_contract[
                "transport_profile_sha256"
            ],
        },
        "target": plan["target"],
        "prior_source": plan["prior_source"],
        "route": {
            "provider": PROVIDER,
            "method": "GET",
            "endpoint": DETAIL_PATH,
            "query_key": "aweme_id",
            "operation": OPERATION,
            "adapter_version": ADAPTER_VERSION,
            "stage": STAGE,
            "window_key": identity["window_key"],
        },
        "budget": {
            "task_id": identity["task_id"],
            "budget_id": identity["budget_id"],
            "purpose": f"paid_source_refresh_{identity['task_id'].rsplit('-', 1)[-1]}",
            "currency": "USD",
            "verified_unit_price": UNIT_PRICE,
            "price_verified_at": price_evidence["checked_at"],
            "max_billable_requests": 1,
            "max_amount": UNIT_PRICE,
            "pilot_size": 1,
            "daily_quota": 1,
        },
        "price_evidence": _validate_price_evidence(price_evidence),
        "database_baseline": baseline,
        "baseline_artifact_ids": baseline_artifact_ids,
        "max_handoff_age_seconds": MAX_HANDOFF_AGE_SECONDS,
        "code": _code_snapshot(),
    }


def _validate_contract(
    paths: RefreshPaths,
    contract: Mapping[str, Any],
    *,
    content_id: int,
    expected_source_db_sha256: str,
    expected_source_completion_sha256: str,
) -> Mapping[str, Any]:
    expected_keys = {
        "version",
        "completion_kind",
        "created_at",
        "run_root",
        "source_database",
        "base_source",
        "database",
        "copy_records",
        "roots",
        "metadata",
        "target",
        "prior_source",
        "route",
        "budget",
        "price_evidence",
        "database_baseline",
        "baseline_artifact_ids",
        "max_handoff_age_seconds",
        "code",
    }
    if (
        set(contract) != expected_keys
        or contract.get("version") != SCHEMA_VERSION
        or contract.get("completion_kind") != COMPLETION_KIND
        or contract.get("run_root") != str(paths.run_root)
        or contract.get("roots")
        != {"raw_root": str(paths.raw_root), "media_root": str(paths.media_root)}
        or contract.get("max_handoff_age_seconds") != MAX_HANDOFF_AGE_SECONDS
        or contract.get("code") != _code_snapshot()
    ):
        raise PaidSourceRefreshError("paid refresh contract形状/代码/路径漂移")
    metadata_contract = _read_json(
        paths.metadata_contract, label="paid metadata contract"
    )
    _validate_metadata_contract(
        paths,
        metadata_contract,
        expected_source_db_sha256=expected_source_db_sha256,
        expected_source_completion_sha256=expected_source_completion_sha256,
        content_id=content_id,
    )
    metadata_ledger = _read_json(paths.metadata_ledger, label="paid metadata ledger")
    _validate_metadata_ledger(
        metadata_ledger,
        metadata_contract=metadata_contract,
        metadata_contract_sha256=_sha256_file(paths.metadata_contract),
    )
    metadata = contract.get("metadata")
    if (
        not isinstance(metadata, Mapping)
        or metadata
        != {
            "metadata_contract_path": str(paths.metadata_contract),
            "metadata_contract_sha256": _sha256_file(paths.metadata_contract),
            "metadata_ledger_path": str(paths.metadata_ledger),
            "price_prefix_sha256": _json_sha256(metadata_ledger["events"][:6]),
            "request_plan_sha256": metadata_contract["request_plan_sha256"],
            "transport_profile_sha256": metadata_contract[
                "transport_profile_sha256"
            ],
        }
        or metadata_ledger.get("price_evidence") != contract.get("price_evidence")
        or metadata_ledger.get("contract_sha256")
        not in {None, _json_sha256(contract)}
    ):
        raise PaidSourceRefreshError("paid refresh contract metadata绑定漂移")
    parent = _parent_source_evidence(
        paths,
        content_id=content_id,
        expected_source_db_sha256=expected_source_db_sha256,
        expected_source_completion_sha256=expected_source_completion_sha256,
    )
    if contract.get("base_source") != parent or contract.get("source_database") != parent.get("database"):
        raise PaidSourceRefreshError("paid refresh parent Step3证据漂移")
    local_controller._validate_copy_records(paths.local_paths, contract)
    database = contract.get("database")
    metadata = local_controller._private_file(paths.database, label="paid refresh clone")
    if (
        not isinstance(database, Mapping)
        or database.get("path") != str(paths.database)
        or _exact_integer(database.get("inode"), label="clone inode")
        != metadata.st_ino
        or _exact_integer(database.get("nlink"), label="clone link count") != 1
    ):
        raise PaidSourceRefreshError("paid refresh clone基线身份漂移")
    with closing(local_controller._immutable_connection(paths.database)) as connection:
        target = _validate_content_target(connection, content_id)
    if target != contract.get("target"):
        raise PaidSourceRefreshError("paid refresh target content发生变化")
    route = contract.get("route")
    budget = contract.get("budget")
    if (
        not isinstance(route, Mapping)
        or route.get("provider") != PROVIDER
        or route.get("method") != "GET"
        or route.get("endpoint") != DETAIL_PATH
        or route.get("query_key") != "aweme_id"
        or route.get("operation") != OPERATION
        or route.get("adapter_version") != ADAPTER_VERSION
        or route.get("stage") != STAGE
        or not isinstance(route.get("window_key"), str)
        or not isinstance(budget, Mapping)
        or set(budget)
        != {
            "task_id",
            "budget_id",
            "purpose",
            "currency",
            "verified_unit_price",
            "price_verified_at",
            "max_billable_requests",
            "max_amount",
            "pilot_size",
            "daily_quota",
        }
        or budget.get("currency") != "USD"
        or _exact_decimal(
            budget.get("verified_unit_price"), label="contract verified unit price"
        )
        != Decimal(str(UNIT_PRICE))
        or _exact_integer(
            budget.get("max_billable_requests"),
            label="contract max billable requests",
        )
        != 1
        or _exact_decimal(
            budget.get("max_amount"), label="contract max amount"
        )
        != Decimal(str(UNIT_PRICE))
        or _exact_integer(budget.get("pilot_size"), label="contract pilot size")
        != 1
        or _exact_integer(budget.get("daily_quota"), label="contract daily quota")
        != 1
    ):
        raise PaidSourceRefreshError("paid refresh route/budget合同漂移")
    _validate_price_evidence(contract["price_evidence"])
    if contract.get("database_baseline") != _database_baseline(paths.source_database):
        raise PaidSourceRefreshError("paid refresh数据库baseline漂移")
    return parent


def _intent_value(
    *,
    paths: RefreshPaths,
    contract_sha256: str,
    contract: Mapping[str, Any],
) -> Mapping[str, Any]:
    return {
        "version": SCHEMA_VERSION,
        "completion_kind": COMPLETION_KIND,
        "created_at": contract["created_at"],
        "contract_sha256": contract_sha256,
        "content_id": int(contract["target"]["id"]),
        "task_id": contract["budget"]["task_id"],
        "budget_id": contract["budget"]["budget_id"],
        "before_database": contract["database"],
        "provider_call_limit": 1,
        "run_root": str(paths.run_root),
    }


def _validate_intent(
    paths: RefreshPaths,
    intent: Mapping[str, Any],
    contract: Mapping[str, Any],
) -> None:
    if (
        set(intent)
        != {
            "version",
            "completion_kind",
            "created_at",
            "contract_sha256",
            "content_id",
            "task_id",
            "budget_id",
            "before_database",
            "provider_call_limit",
            "run_root",
        }
        or intent.get("version") != SCHEMA_VERSION
        or intent.get("completion_kind") != COMPLETION_KIND
        or intent.get("created_at") != contract["created_at"]
        or intent.get("contract_sha256") != _sha256_file(paths.contract)
        or int(intent.get("content_id") or -1) != int(contract["target"]["id"])
        or intent.get("task_id") != contract["budget"]["task_id"]
        or intent.get("budget_id") != contract["budget"]["budget_id"]
        or intent.get("before_database") != contract["database"]
        or intent.get("provider_call_limit") != 1
        or intent.get("run_root") != str(paths.run_root)
    ):
        raise PaidSourceRefreshError("paid refresh intent漂移")


def _committed_raw(
    paths: RefreshPaths, contract: Mapping[str, Any]
) -> tuple[int, Any, Mapping[str, Any]] | None:
    content_id = int(contract["target"]["id"])
    with closing(local_controller._immutable_connection(paths.database)) as connection:
        row = connection.execute(
            """
            SELECT fs.status slot_status,fs.attempt_count,fs.id slot_id,
                   fa.id attempt_id,fa.attempt_number,fa.http_status,fa.billed,
                   fa.amount,fa.currency,pr.*
            FROM fetch_slots fs
            JOIN fetch_attempts fa ON fa.slot_id=fs.id
            JOIN provider_raw_responses pr ON pr.fetch_attempt_id=fa.id
            WHERE fs.content_id=? AND fs.stage=? AND fs.window_key=?
            ORDER BY pr.id
            """,
            (content_id, STAGE, contract["route"]["window_key"]),
        ).fetchall()
    if not row:
        return None
    if len(row) != 1:
        raise PaidSourceRefreshError("paid refresh committed raw数量漂移")
    value = row[0]
    if (
        value["slot_status"] != "succeeded"
        or _exact_integer(value["attempt_count"], label="committed slot attempt count")
        != 1
        or _exact_integer(value["attempt_number"], label="committed attempt number")
        != 1
        or _exact_integer(value["http_status"], label="committed http status")
        != 200
        or _exact_integer(value["billed"], label="committed billed") != 1
        or _exact_decimal(value["amount"], label="committed amount")
        != Decimal(str(UNIT_PRICE))
        or str(value["currency"] or "") != "USD"
        or str(value["provider"]) != PROVIDER
        or str(value["operation"]) != OPERATION
        or str(value["source"]) not in {"live", "live_applied"}
    ):
        raise PaidSourceRefreshError("paid refresh committed raw不是唯一成功终态")
    raw_path = _raw_path(value).resolve()
    evidence = _file_evidence(raw_path, label="paid refresh committed raw")
    if (
        not local_controller._is_within(raw_path, paths.raw_root)
        or evidence["sha256"] != value["sha256"]
        or evidence["byte_size"]
        != _exact_integer(value["byte_size"], label="committed raw byte size")
    ):
        raise PaidSourceRefreshError("paid refresh committed raw文件漂移")
    try:
        body = json.loads(raw_path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PaidSourceRefreshError("paid refresh committed raw不是JSON") from exc
    parsed = providers_module._parse_douyin_stage_payload(
        "detail", str(contract["target"]["platform_content_id"]), body, status=200
    )
    provenance = _validate_live_payload(
        payload=body, data=parsed.data, target=contract["target"]
    )
    return int(value["id"]), parsed.data, provenance


def _provider_activity(paths: RefreshPaths, contract: Mapping[str, Any]) -> Mapping[str, int]:
    with closing(local_controller._immutable_connection(paths.database)) as connection:
        return {
            "slots": int(
                connection.execute(
                    "SELECT COUNT(*) FROM fetch_slots WHERE content_id=? AND stage=?",
                    (int(contract["target"]["id"]), STAGE),
                ).fetchone()[0]
            ),
            "attempts": int(
                connection.execute(
                    "SELECT COUNT(*) FROM fetch_attempts fa JOIN fetch_slots fs "
                    "ON fs.id=fa.slot_id WHERE fs.content_id=? AND fs.stage=?",
                    (int(contract["target"]["id"]), STAGE),
                ).fetchone()[0]
            ),
            "usage": int(
                connection.execute(
                    "SELECT COUNT(*) FROM provider_usage WHERE task_id=?",
                    (contract["budget"]["task_id"],),
                ).fetchone()[0]
            ),
        }


def _require_price_fresh(value: Mapping[str, Any]) -> None:
    try:
        checked = datetime.fromisoformat(str(value["checked_at"]).replace("Z", "+00:00"))
    except (KeyError, ValueError) as exc:
        raise PaidSourceRefreshError("endpoint-info checked_at无效") from exc
    if checked.tzinfo is None:
        raise PaidSourceRefreshError("endpoint-info checked_at缺少时区")
    age = (datetime.now(timezone.utc) - checked.astimezone(timezone.utc)).total_seconds()
    if age < -30 or age > MAX_PRICE_EVIDENCE_AGE_SECONDS:
        raise PaidSourceRefreshError("endpoint-info价格证据已过期，必须使用新root重建合同")


def _require_handoff_fresh(completed_at: Any, *, maximum_age_seconds: Any) -> None:
    if (
        isinstance(maximum_age_seconds, bool)
        or maximum_age_seconds != MAX_HANDOFF_AGE_SECONDS
    ):
        raise PaidSourceRefreshError("paid source handoff时效合同漂移")
    try:
        completed = datetime.fromisoformat(str(completed_at).replace("Z", "+00:00"))
    except ValueError as exc:
        raise PaidSourceRefreshError("paid source handoff completed_at无效") from exc
    if completed.tzinfo is None:
        raise PaidSourceRefreshError("paid source handoff completed_at缺少时区")
    age = (
        datetime.now(timezone.utc) - completed.astimezone(timezone.utc)
    ).total_seconds()
    if age < -30 or age > MAX_HANDOFF_AGE_SECONDS:
        raise PaidSourceRefreshError("paid source handoff已超出短时安全窗口，必须新建refresh root")


def _validate_balance_evidence(
    value: Mapping[str, Any],
    *,
    price_record_sha256: str,
    price_checked_at: str,
    require_fresh: bool = True,
) -> Mapping[str, Any]:
    if set(value) != {
        "checked_at",
        "endpoint",
        "endpoint_cost",
        "balance_sufficient",
        "response_sha256",
        "response_bytes",
        "price_evidence_sha256",
    }:
        raise PaidSourceRefreshError("user-info余额门字段漂移")
    try:
        checked = datetime.fromisoformat(
            str(value["checked_at"]).replace("Z", "+00:00")
        )
        price_checked = datetime.fromisoformat(
            str(price_checked_at).replace("Z", "+00:00")
        )
    except (KeyError, ValueError) as exc:
        raise PaidSourceRefreshError("user-info余额门时间无效") from exc
    if checked.tzinfo is None or price_checked.tzinfo is None:
        raise PaidSourceRefreshError("user-info余额门时间缺少时区")
    now = datetime.now(timezone.utc)
    checked_utc = checked.astimezone(timezone.utc)
    price_utc = price_checked.astimezone(timezone.utc)
    if (
        (
            require_fresh
            and (now - checked_utc).total_seconds()
            > MAX_PRICE_EVIDENCE_AGE_SECONDS
        )
        or (require_fresh and (checked_utc - now).total_seconds() > 30)
        or (price_utc - checked_utc).total_seconds() > 30
        or (checked_utc - price_utc).total_seconds()
        > MAX_PRICE_EVIDENCE_AGE_SECONDS
        or value.get("endpoint") != USER_INFO_PATH
        or _finite_number(
            value.get("endpoint_cost"), label="user-info endpoint cost"
        )
        != 0.0
        or value.get("balance_sufficient") is not True
        or value.get("price_evidence_sha256") != price_record_sha256
        or not isinstance(value.get("response_sha256"), str)
        or re.fullmatch(r"[0-9a-f]{64}", str(value["response_sha256"])) is None
        or isinstance(value.get("response_bytes"), bool)
        or not isinstance(value.get("response_bytes"), int)
        or int(value["response_bytes"]) <= 0
    ):
        raise PaidSourceRefreshError("user-info余额门证据漂移")
    return dict(value)


def _create_budget(paths: RefreshPaths, contract: Mapping[str, Any]) -> None:
    budget = contract["budget"]
    created_at = storage_module.now_utc()
    with (
        closing(storage_module.connect(paths.database)) as connection,
        storage_module.transaction(connection),
    ):
        if connection.execute(
            "SELECT 1 FROM provider_budget_batches WHERE id=? OR purpose=?",
            (budget["budget_id"], budget["purpose"]),
        ).fetchone() is not None:
            raise PaidSourceRefreshError("paid refresh专用budget已存在")
        connection.execute(
            """
            INSERT INTO provider_budget_batches(
                id,purpose,provider,operation,currency,verified_unit_price,
                max_billable_requests,max_amount,pilot_size,daily_quota,
                price_verified_at,status,created_at,updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,'approved',?,?)
            """,
            (
                budget["budget_id"],
                budget["purpose"],
                PROVIDER,
                OPERATION,
                "USD",
                UNIT_PRICE,
                1,
                UNIT_PRICE,
                1,
                1,
                budget["price_verified_at"],
                created_at,
                created_at,
            ),
        )


def _commit_successful_capture(
    paths: RefreshPaths,
    contract: Mapping[str, Any],
    *,
    claim: capture_module.SlotClaim,
    usage_id: int,
    result: capture_module.ProviderResult,
) -> capture_module.CaptureOutcome:
    if (
        result.http_status != 200
        or result.billed is not True
        or not isinstance(result.raw_response, Mapping)
    ):
        raise PaidSourceRefreshError("paid detail不是HTTP200 billed JSON终态")
    with (
        closing(storage_module.connect(paths.database)) as connection,
        storage_module.transaction(connection),
    ):
        raw_id = capture_module._store_raw_response(
            connection,
            claim=claim,
            operation=OPERATION,
            value=result.raw_response,
            http_status=200,
            raw_root=paths.raw_root,
        )
        capture_module._settle_budget(
            connection,
            usage_id=usage_id,
            budget_id=contract["budget"]["budget_id"],
            unit_price=UNIT_PRICE,
            billed=True,
            details={"state": "completed", "slot_id": claim.slot_id, "http_status": 200},
        )
        finished_at = storage_module.now_utc()
        connection.execute(
            """
            UPDATE fetch_attempts SET response_finished_at=?,http_status=200,
                billed=1,amount=?,currency='USD',error_code=NULL,error_message=NULL
            WHERE id=?
            """,
            (finished_at, UNIT_PRICE, claim.attempt_id),
        )
        connection.execute(
            """
            UPDATE fetch_slots SET status='succeeded',finished_at=?,updated_at=?,
                last_error_code=NULL,last_error_message=NULL WHERE id=?
            """,
            (finished_at, finished_at, claim.slot_id),
        )
    return capture_module.CaptureOutcome(
        slot_id=claim.slot_id,
        attempt_id=claim.attempt_id,
        raw_response_id=raw_id,
        data=result.data,
        billed=True,
        amount=UNIT_PRICE,
        currency="USD",
    )


def _commit_rejected_capture(
    paths: RefreshPaths,
    contract: Mapping[str, Any],
    *,
    claim: capture_module.SlotClaim,
    usage_id: int,
    raw_response: Mapping[str, Any],
    error: BaseException,
) -> None:
    with (
        closing(storage_module.connect(paths.database)) as connection,
        storage_module.transaction(connection),
    ):
        capture_module._store_raw_response(
            connection,
            claim=claim,
            operation=OPERATION,
            value=raw_response,
            http_status=200,
            raw_root=paths.raw_root,
        )
        capture_module._settle_budget(
            connection,
            usage_id=usage_id,
            budget_id=contract["budget"]["budget_id"],
            unit_price=UNIT_PRICE,
            billed=True,
            details={
                "state": "completed",
                "outcome": "rejected_source",
                "slot_id": claim.slot_id,
                "http_status": 200,
            },
        )
        finished_at = storage_module.now_utc()
        connection.execute(
            """
            UPDATE fetch_attempts SET response_finished_at=?,http_status=200,
                billed=1,amount=?,currency='USD',error_code='rejected_source',
                error_message=NULL WHERE id=?
            """,
            (finished_at, UNIT_PRICE, claim.attempt_id),
        )
        connection.execute(
            """
            UPDATE fetch_slots SET status='terminal_failed',finished_at=?,updated_at=?,
                last_error_code='rejected_source',last_error_message=NULL WHERE id=?
            """,
            (finished_at, finished_at, claim.slot_id),
        )


def _commit_transport_failed_capture(
    paths: RefreshPaths,
    contract: Mapping[str, Any],
    *,
    error: BaseException,
    claim: capture_module.SlotClaim | None = None,
    usage_id: int | None = None,
) -> tuple[int, int, int]:
    details = {
        "billing_basis": "conservative_upper_bound",
        "outcome": "transport_failed",
        "slot_id": claim.slot_id if claim is not None else None,
        "state": "completed",
    }
    with (
        closing(storage_module.connect(paths.database)) as connection,
        storage_module.transaction(connection),
    ):
        rows = connection.execute(
            """
            SELECT fs.id slot_id,fs.status slot_status,fs.last_error_code,
                   fa.id attempt_id,fa.http_status,fa.billed,fa.amount,
                   fa.currency,fa.error_code,pu.id usage_id,pu.details_json,
                   pbb.consumed_requests,pbb.consumed_amount
            FROM fetch_slots fs
            JOIN fetch_attempts fa ON fa.slot_id=fs.id
            JOIN provider_usage pu ON pu.task_id=? AND pu.budget_batch_id=?
            JOIN provider_budget_batches pbb ON pbb.id=pu.budget_batch_id
            WHERE fs.content_id=? AND fs.stage=? AND fs.window_key=?
            """,
            (
                contract["budget"]["task_id"],
                contract["budget"]["budget_id"],
                int(contract["target"]["id"]),
                STAGE,
                contract["route"]["window_key"],
            ),
        ).fetchall()
        if len(rows) != 1:
            raise PaidSourceRefreshError("transport_failed闭包缺少唯一slot/attempt/usage")
        row = rows[0]
        resolved_slot_id = _exact_integer(row["slot_id"], label="transport slot id")
        resolved_attempt_id = _exact_integer(
            row["attempt_id"], label="transport attempt id"
        )
        resolved_usage_id = _exact_integer(row["usage_id"], label="transport usage id")
        if (
            (claim is not None and claim.slot_id != resolved_slot_id)
            or (claim is not None and claim.attempt_id != resolved_attempt_id)
            or (usage_id is not None and usage_id != resolved_usage_id)
        ):
            raise PaidSourceRefreshError("transport_failed闭包行身份漂移")
        details["slot_id"] = resolved_slot_id
        try:
            usage_details = json.loads(str(row["details_json"]))
        except json.JSONDecodeError as exc:
            raise PaidSourceRefreshError("transport_failed usage JSON漂移") from exc
        already_closed = (
            row["slot_status"] == "terminal_failed"
            and row["last_error_code"] == "transport_failed"
            and row["error_code"] == "transport_failed"
            and row["http_status"] is None
            and _exact_integer(row["billed"], label="transport billed") == 1
            and _exact_decimal(row["amount"], label="transport amount")
            == Decimal(str(UNIT_PRICE))
            and row["currency"] == "USD"
            and usage_details == details
            and _exact_integer(
                row["consumed_requests"], label="transport consumed requests"
            )
            == 1
            and _exact_decimal(
                row["consumed_amount"], label="transport consumed amount"
            )
            == Decimal(str(UNIT_PRICE))
        )
        if already_closed:
            return resolved_slot_id, resolved_attempt_id, resolved_usage_id
        if (
            row["slot_status"] != "running"
            or row["last_error_code"] is not None
            or row["error_code"] is not None
            or row["http_status"] is not None
            or usage_details != {"state": "reserved"}
            or _exact_integer(
                row["consumed_requests"], label="reserved consumed requests"
            )
            != 1
            or _exact_decimal(
                row["consumed_amount"], label="reserved consumed amount"
            )
            != Decimal(str(UNIT_PRICE))
        ):
            raise PaidSourceRefreshError("transport_failed闭包前缀不是唯一reservation")
        capture_module._settle_budget(
            connection,
            usage_id=resolved_usage_id,
            budget_id=contract["budget"]["budget_id"],
            unit_price=UNIT_PRICE,
            billed=True,
            details=details,
        )
        finished_at = storage_module.now_utc()
        attempt_cursor = connection.execute(
            """
            UPDATE fetch_attempts SET response_finished_at=?,http_status=NULL,
                billed=1,amount=?,currency='USD',error_code='transport_failed',
                error_message=NULL WHERE id=? AND error_code IS NULL
            """,
            (finished_at, UNIT_PRICE, resolved_attempt_id),
        )
        slot_cursor = connection.execute(
            """
            UPDATE fetch_slots SET status='terminal_failed',finished_at=?,updated_at=?,
                last_error_code='transport_failed',last_error_message=NULL
            WHERE id=? AND status='running'
            """,
            (finished_at, finished_at, resolved_slot_id),
        )
        if attempt_cursor.rowcount != 1 or slot_cursor.rowcount != 1:
            raise PaidSourceRefreshError("transport_failed闭包CAS失败")
    return resolved_slot_id, resolved_attempt_id, resolved_usage_id


def _materialize_manifest(
    paths: RefreshPaths,
    contract: Mapping[str, Any],
    *,
    raw_response_id: int,
    data: Mapping[str, Any],
) -> None:
    content_id = int(contract["target"]["id"])
    expected_path, urls, source_sha256 = _manifest_candidate(
        paths,
        contract,
        raw_response_id=raw_response_id,
        data=data,
    )
    with closing(local_controller._immutable_connection(paths.database)) as connection:
        rows = connection.execute(
            "SELECT * FROM evidence_artifacts WHERE content_id=? "
            "AND artifact_type='media_source' AND local_path IN (?,?)",
            (
                content_id,
                str(expected_path),
                str(
                    expected_path.relative_to(storage_module.PROJECT_ROOT)
                    if local_controller._is_within(
                        expected_path, storage_module.PROJECT_ROOT
                    )
                    else expected_path
                ),
            ),
        ).fetchall()
    if rows:
        if len(rows) != 1:
            raise PaidSourceRefreshError("paid refresh既有manifest artifact不唯一")
        row = rows[0]
        file = _file_evidence(expected_path, label="paid refresh既有manifest")
        try:
            body = json.loads(expected_path.read_bytes())
            metadata = json.loads(str(row["metadata_json"]))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PaidSourceRefreshError("paid refresh既有manifest JSON漂移") from exc
        if (
            str(row["status"]) != "available"
            or str(row["processor_version"]) != media_module.MEDIA_SOURCE_VERSION
            or str(row["sha256"]) != file["sha256"]
            or _exact_integer(row["byte_size"], label="manifest artifact byte size")
            != file["byte_size"]
            or body.get("media_kind") != "video"
            or body.get("urls") != urls
            or body.get("source_sha256") != source_sha256
            or _exact_integer(
                body.get("raw_response_id"), label="manifest raw response id"
            )
            != raw_response_id
            or not isinstance(metadata, Mapping)
            or set(metadata)
            != {"media_kind", "source_count", "source_sha256", "raw_response_id"}
            or metadata.get("media_kind") != "video"
            or _exact_integer(
                metadata.get("source_count"), label="manifest metadata source count"
            )
            != len(urls)
            or metadata.get("source_sha256") != source_sha256
            or _exact_integer(
                metadata.get("raw_response_id"),
                label="manifest metadata raw response id",
            )
            != raw_response_id
        ):
            raise PaidSourceRefreshError("paid refresh既有manifest证据漂移")
    else:
        temporary = expected_path.with_name(f".{expected_path.name}.tmp")
        if not expected_path.exists() and os.path.lexists(temporary):
            _validate_orphan_manifest_file(
                temporary,
                urls=urls,
                source_sha256=source_sha256,
                raw_response_id=raw_response_id,
                label="paid refresh orphan manifest temp",
            )
            os.replace(temporary, expected_path)
            local_controller._fsync_directory(expected_path.parent)
        artifact = media_module.store_media_source_manifest(
            content_id,
            media_kind="video",
            urls=urls,
            raw_response_id=raw_response_id,
            db_path=paths.database,
            media_root=paths.media_root,
        )
        if artifact is None:
            raise PaidSourceRefreshError("paid refresh未生成media_source")
    with closing(local_controller._immutable_connection(paths.database)) as connection:
        source_row = connection.execute(
            "SELECT source FROM provider_raw_responses WHERE id=? AND content_id=? "
            "AND provider=? AND operation=?",
            (raw_response_id, content_id, PROVIDER, OPERATION),
        ).fetchone()
    if source_row is None or str(source_row["source"]) not in {"live", "live_applied"}:
        raise PaidSourceRefreshError("paid refresh raw source状态无法精确收口")
    if str(source_row["source"]) == "live":
        with (
            closing(storage_module.connect(paths.database)) as connection,
            storage_module.transaction(connection),
        ):
            cursor = connection.execute(
                "UPDATE provider_raw_responses SET source='live_applied' "
                "WHERE id=? AND content_id=? AND provider=? AND operation=? "
                "AND source='live'",
                (raw_response_id, content_id, PROVIDER, OPERATION),
            )
            if cursor.rowcount != 1:
                raise PaidSourceRefreshError("paid refresh raw source CAS失败")


def _request_summary(
    ledger: Mapping[str, Any], contract: Mapping[str, Any]
) -> Mapping[str, Any]:
    _validate_bound_ledger(ledger, contract=contract, terminal=True)
    events = ledger["events"]
    if events[1].get("outcome") != "response_received":
        raise PaidSourceRefreshError("paid refresh成功闭包缺少response_received")
    return {
        "request_id": ledger["request_id"],
        "event_count": 2,
        "opening_event_sha256": _json_sha256(events[0]),
        "terminal_event_sha256": _json_sha256(events[1]),
        "events_sha256": ledger["events_sha256"],
        "network_attempts": 1,
        "balance_check": ledger["balance_check"],
    }


def _completion_records(
    paths: RefreshPaths,
    *,
    contract: Mapping[str, Any],
    intent_sha256: str,
    ledger: Mapping[str, Any],
    materialized: Mapping[str, Any],
    delta: Mapping[str, Any],
) -> Mapping[str, Any]:
    after_database = local_controller._database_identity(paths.database)
    completed_at = str(ledger["events"][1]["finished_at"])
    output = {
        "raw": local_controller._inventory(paths.raw_root),
        "media": local_controller._inventory(paths.media_root),
    }
    state = {
        "version": SCHEMA_VERSION,
        "completion_kind": COMPLETION_KIND,
        "phase": "succeeded",
        "updated_at": completed_at,
        "contract_sha256": _sha256_file(paths.contract),
        "intent_sha256": intent_sha256,
        "network_ledger_sha256": _sha256_file(paths.ledger),
        "metadata_ledger_sha256": _sha256_file(paths.metadata_ledger),
        "attempt_consumed": True,
        "database": after_database,
    }
    state_body = _canonical_bytes(state)
    if paths.state.exists() and paths.state.read_bytes() == state_body:
        state_sha256 = hashlib.sha256(state_body).hexdigest()
    else:
        state_sha256 = _write_json(paths.state, state, immutable=False)
    request = _request_summary(ledger, contract)
    capture = {
        "slot_id": int(materialized["slot"]["id"]),
        "attempt_id": int(materialized["attempt"]["id"]),
        "attempt_number": 1,
        "stage": STAGE,
        "window_key": contract["route"]["window_key"],
        "provider": PROVIDER,
        "adapter_version": ADAPTER_VERSION,
        "operation": OPERATION,
        "http_status": 200,
        "billed": True,
        "amount": UNIT_PRICE,
        "currency": "USD",
        "raw_response_id": int(materialized["raw_response"]["id"]),
    }
    budget = {
        "task_id": contract["budget"]["task_id"],
        "budget_batch_id": contract["budget"]["budget_id"],
        "verified_unit_price": UNIT_PRICE,
        "max_billable_requests": 1,
        "max_amount": UNIT_PRICE,
        "daily_quota": 1,
        "consumed_requests": 1,
        "consumed_amount": UNIT_PRICE,
        "provider_usage_id": int(materialized["usage"]["id"]),
        "request_attempts": 1,
        "billed_requests": 1,
        "amount": UNIT_PRICE,
        "details_json_sha256": materialized["usage_details_sha256"],
    }
    raw_response = {
        "row": materialized["raw_response"],
        "file": materialized["raw_response_file"],
        "body_sha256": materialized["raw_response_body_sha256"],
        "item_sha256": materialized["raw_item_sha256"],
    }
    media_source = {
        "row": materialized["artifact"],
        "file": materialized["artifact_file"],
        "logical_source_sha256": materialized["artifact_body"]["source_sha256"],
        "urls_count": len(materialized["artifact_body"]["urls"]),
        "urls_sha256": _json_sha256(materialized["artifact_body"]["urls"]),
        "raw_response_id": int(materialized["raw_response"]["id"]),
        "metadata_sha256": _json_sha256(
            json.loads(str(materialized["artifact"]["metadata_json"]))
        ),
    }
    receipt = {
        "version": SCHEMA_VERSION,
        "completion_kind": COMPLETION_KIND,
        "status": "succeeded",
        "completed_at": completed_at,
        "run_root": str(paths.run_root),
        "contract_sha256": _sha256_file(paths.contract),
        "intent_sha256": intent_sha256,
        "state_sha256": state_sha256,
        "network_ledger_sha256": _sha256_file(paths.ledger),
        "metadata_ledger_sha256": _sha256_file(paths.metadata_ledger),
        "database": after_database,
        "request": request,
        "capture": capture,
        "budget": budget,
        "raw_response": raw_response,
        "media_source": media_source,
        "url_provenance": materialized["url_provenance"],
        "output_inventory": output,
        "critical_unchanged": delta,
        "provider_call_history": _provider_call_history(),
    }
    receipt_sha256 = _write_json(paths.receipt, receipt, immutable=True)
    completion = {
        **receipt,
        "receipt_sha256": receipt_sha256,
    }
    completion_sha256 = _write_json(paths.completion, completion, immutable=True)
    return {
        "receipt": receipt,
        "receipt_sha256": receipt_sha256,
        "completion": completion,
        "completion_sha256": completion_sha256,
    }


def _validate_success_records(
    paths: RefreshPaths,
    contract: Mapping[str, Any],
    *,
    expected_source_db_sha256: str,
    expected_source_completion_sha256: str,
    transient_run_names: frozenset[str] = frozenset(),
) -> Mapping[str, Any]:
    content_id = int(contract["target"]["id"])
    _validate_contract(
        paths,
        contract,
        content_id=content_id,
        expected_source_db_sha256=expected_source_db_sha256,
        expected_source_completion_sha256=expected_source_completion_sha256,
    )
    _validate_parent_separation(paths, contract["base_source"])
    intent = _read_json(paths.intent, label="paid refresh intent")
    _validate_intent(paths, intent, contract)
    ledger = _read_json(paths.ledger, label="paid refresh provider ledger")
    if (
        ledger.get("contract_sha256") != _sha256_file(paths.contract)
        or ledger.get("intent_sha256") != _sha256_file(paths.intent)
    ):
        raise PaidSourceRefreshError("paid refresh ledger未绑定contract/intent")
    _validate_bound_ledger(ledger, contract=contract, terminal=True)
    metadata_contract = _read_json(
        paths.metadata_contract, label="paid metadata contract"
    )
    metadata_ledger = _read_json(paths.metadata_ledger, label="paid metadata ledger")
    _validate_metadata_ledger(
        metadata_ledger,
        metadata_contract=metadata_contract,
        metadata_contract_sha256=_sha256_file(paths.metadata_contract),
    )
    if (
        metadata_ledger.get("contract_sha256") != _sha256_file(paths.contract)
        or len(metadata_ledger["events"]) != 8
        or any(
            event.get("outcome") != "response_received"
            for event in metadata_ledger["events"]
            if event.get("phase") == "terminal"
        )
    ):
        raise PaidSourceRefreshError("paid metadata成功闭包漂移")
    materialized = _validate_refresh_materialization(paths, contract)
    if (
        ledger["events"][1].get("response_json_sha256")
        != materialized["raw_response_file"]["sha256"]
    ):
        raise PaidSourceRefreshError("paid HTTP JSON响应未语义绑定committed raw")
    delta = _validate_database_delta(paths, contract, materialized)
    receipt = _read_json(paths.receipt, label="paid refresh receipt")
    completion = _read_json(paths.completion, label="paid refresh completion")
    if set(receipt) != RECEIPT_FIELDS or set(completion) != COMPLETION_FIELDS:
        raise PaidSourceRefreshError("paid refresh receipt/completion字段漂移")
    expected_capture = {
        "slot_id": int(materialized["slot"]["id"]),
        "attempt_id": int(materialized["attempt"]["id"]),
        "attempt_number": 1,
        "stage": STAGE,
        "window_key": contract["route"]["window_key"],
        "provider": PROVIDER,
        "adapter_version": ADAPTER_VERSION,
        "operation": OPERATION,
        "http_status": 200,
        "billed": True,
        "amount": UNIT_PRICE,
        "currency": "USD",
        "raw_response_id": int(materialized["raw_response"]["id"]),
    }
    expected_budget = {
        "task_id": contract["budget"]["task_id"],
        "budget_batch_id": contract["budget"]["budget_id"],
        "verified_unit_price": UNIT_PRICE,
        "max_billable_requests": 1,
        "max_amount": UNIT_PRICE,
        "daily_quota": 1,
        "consumed_requests": 1,
        "consumed_amount": UNIT_PRICE,
        "provider_usage_id": int(materialized["usage"]["id"]),
        "request_attempts": 1,
        "billed_requests": 1,
        "amount": UNIT_PRICE,
        "details_json_sha256": materialized["usage_details_sha256"],
    }
    expected_raw = {
        "row": materialized["raw_response"],
        "file": materialized["raw_response_file"],
        "body_sha256": materialized["raw_response_body_sha256"],
        "item_sha256": materialized["raw_item_sha256"],
    }
    expected_media = {
        "row": materialized["artifact"],
        "file": materialized["artifact_file"],
        "logical_source_sha256": materialized["artifact_body"]["source_sha256"],
        "urls_count": len(materialized["artifact_body"]["urls"]),
        "urls_sha256": _json_sha256(materialized["artifact_body"]["urls"]),
        "raw_response_id": int(materialized["raw_response"]["id"]),
        "metadata_sha256": _json_sha256(
            json.loads(str(materialized["artifact"]["metadata_json"]))
        ),
    }
    if (
        _canonical_bytes(completion)
        != _canonical_bytes(
            {**receipt, "receipt_sha256": _sha256_file(paths.receipt)}
        )
        or type(receipt.get("version")) is not int
        or receipt.get("version") != SCHEMA_VERSION
        or receipt.get("completion_kind") != COMPLETION_KIND
        or receipt.get("status") != "succeeded"
        or receipt.get("completed_at") != ledger["events"][1]["finished_at"]
        or receipt.get("run_root") != str(paths.run_root)
        or receipt.get("contract_sha256") != _sha256_file(paths.contract)
        or receipt.get("intent_sha256") != _sha256_file(paths.intent)
        or receipt.get("network_ledger_sha256") != _sha256_file(paths.ledger)
        or receipt.get("metadata_ledger_sha256")
        != _sha256_file(paths.metadata_ledger)
        or _canonical_bytes(receipt.get("database"))
        != _canonical_bytes(local_controller._database_identity(paths.database))
        or _canonical_bytes(receipt.get("request"))
        != _canonical_bytes(_request_summary(ledger, contract))
        or _canonical_bytes(receipt.get("capture"))
        != _canonical_bytes(expected_capture)
        or _canonical_bytes(receipt.get("budget")) != _canonical_bytes(expected_budget)
        or _canonical_bytes(receipt.get("raw_response"))
        != _canonical_bytes(expected_raw)
        or _canonical_bytes(receipt.get("media_source"))
        != _canonical_bytes(expected_media)
        or _canonical_bytes(receipt.get("url_provenance"))
        != _canonical_bytes(materialized["url_provenance"])
        or _canonical_bytes(receipt.get("critical_unchanged"))
        != _canonical_bytes(delta)
        or _canonical_bytes(receipt.get("provider_call_history"))
        != _canonical_bytes(_provider_call_history())
        or _canonical_bytes(receipt.get("output_inventory"))
        != _canonical_bytes(
            {
                "raw": local_controller._inventory(paths.raw_root),
                "media": local_controller._inventory(paths.media_root),
            }
        )
    ):
        raise PaidSourceRefreshError("paid refresh receipt/completion证据漂移")
    state = _read_json(paths.state, label="paid refresh state")
    if (
        set(state)
        != {
            "version",
            "completion_kind",
            "phase",
            "updated_at",
            "contract_sha256",
            "intent_sha256",
            "network_ledger_sha256",
            "metadata_ledger_sha256",
            "attempt_consumed",
            "database",
        }
        or type(state.get("version")) is not int
        or state.get("version") != SCHEMA_VERSION
        or state.get("completion_kind") != COMPLETION_KIND
        or state.get("phase") != "succeeded"
        or state.get("updated_at") != ledger["events"][1]["finished_at"]
        or state.get("attempt_consumed") is not True
        or state.get("contract_sha256") != _sha256_file(paths.contract)
        or state.get("intent_sha256") != _sha256_file(paths.intent)
        or state.get("network_ledger_sha256") != _sha256_file(paths.ledger)
        or state.get("metadata_ledger_sha256")
        != _sha256_file(paths.metadata_ledger)
        or _canonical_bytes(state.get("database"))
        != _canonical_bytes(local_controller._database_identity(paths.database))
        or receipt.get("state_sha256") != _sha256_file(paths.state)
    ):
        raise PaidSourceRefreshError("paid refresh state证据漂移")
    local_controller._require_clean_database(paths.database)
    allowed_run_names = {
        paths.local_paths.copy_intent.name,
        paths.local_paths.copy_receipt.name,
        paths.metadata_contract.name,
        paths.metadata_ledger.name,
        paths.contract.name,
        paths.intent.name,
        paths.ledger.name,
        paths.state.name,
        paths.receipt.name,
        paths.completion.name,
    }
    actual_names = {path.name for path in paths.run_root.iterdir()}
    if actual_names != allowed_run_names | set(transient_run_names):
        raise PaidSourceRefreshError("paid refresh run root文件集合漂移")
    if not transient_run_names:
        local_controller._inventory(paths.run_root)
    _validate_output_prefix(paths, contract, delta["new_rows"])
    return {
        "contract": contract,
        "intent": intent,
        "ledger": ledger,
        "receipt": receipt,
        "completion": completion,
        "materialized": materialized,
        "delta": delta,
    }


def plan_refresh(
    *,
    source_db_path: Path,
    source_completion_path: Path,
    expected_source_db_sha256: str,
    expected_source_completion_sha256: str,
    db_path: Path,
    raw_root: Path,
    media_root: Path,
    run_root: Path,
    content_ids: Sequence[int],
    endpoint_info_fetcher: Callable[[], Mapping[str, Any]] | None = None,
) -> Mapping[str, Any]:
    del endpoint_info_fetcher  # v2 plan is intentionally and unconditionally zero-network.
    content_id = _ordered_one(content_ids)
    paths = _paths(
        source_db_path=source_db_path,
        source_completion_path=source_completion_path,
        db_path=db_path,
        raw_root=raw_root,
        media_root=media_root,
        run_root=run_root,
    )
    _validate_paths(paths, database_must_exist=False)
    if paths.database.exists() or paths.local_paths.copy_partial.exists():
        raise PaidSourceRefreshError("paid refresh plan要求全新work database")
    for root, label in (
        (paths.raw_root, "raw_root"),
        (paths.media_root, "media_root"),
        (paths.run_root, "run_root"),
    ):
        if os.path.lexists(root):
            local_controller._private_directory(root, label=f"plan {label}")
            if any(root.iterdir()):
                raise PaidSourceRefreshError(f"paid refresh plan要求空输出根：{root}")
    source_evidence = _parent_source_evidence(
        paths,
        content_id=content_id,
        expected_source_db_sha256=expected_source_db_sha256,
        expected_source_completion_sha256=expected_source_completion_sha256,
    )
    _validate_parent_separation(paths, source_evidence)
    with closing(local_controller._immutable_connection(paths.source_database)) as connection:
        source_plan = _source_plan(
            connection, content_id=content_id, source_evidence=source_evidence
        )
    identity = _task_identity(
        source_db_sha256=expected_source_db_sha256,
        source_completion_sha256=expected_source_completion_sha256,
        content_id=content_id,
    )
    metadata_identity = _metadata_identity(
        paths,
        expected_source_db_sha256=expected_source_db_sha256,
        expected_source_completion_sha256=expected_source_completion_sha256,
        content_id=content_id,
    )
    metadata_plan = _metadata_request_plan(metadata_identity)
    return {
        "ok": True,
        "status": "planned",
        "apply": False,
        "completion_kind": COMPLETION_KIND,
        "content_id": content_id,
        "target": source_plan["target"],
        "prior_source_sha256": source_plan["prior_source"]["artifact_body"][
            "source_sha256"
        ],
        "source_database": source_evidence["database"],
        "source_completion_sha256": source_evidence["sha256"],
        "work_database_path": str(paths.database),
        "raw_root": str(paths.raw_root),
        "media_root": str(paths.media_root),
        "run_root": str(paths.run_root),
        "route": {
            "provider": PROVIDER,
            "operation": OPERATION,
            "endpoint": DETAIL_PATH,
            "adapter_version": ADAPTER_VERSION,
            "stage": STAGE,
            "window_key": identity["window_key"],
        },
        "budget": {
            "task_id": identity["task_id"],
            "budget_id": identity["budget_id"],
            "max_billable_requests": 1,
            "max_amount": UNIT_PRICE,
            "daily_quota": 1,
        },
        "metadata_request_plan": metadata_plan,
        "metadata_calls_planned": len(metadata_plan),
        "transport_profile": dict(TRANSPORT_PROFILE),
        "provider_calls_planned": 1,
        "total_provider_calls_planned": len(metadata_plan) + 1,
    }


def _blocked_state_bindings(paths: RefreshPaths) -> Mapping[str, Any]:
    activity: Mapping[str, int] = {"slots": 0, "attempts": 0, "usage": 0}
    if paths.database.exists() and paths.contract.exists():
        with contextlib.suppress(Exception):
            contract = _read_json(paths.contract, label="paid refresh contract")
            activity = _provider_activity(paths, contract)
    attempt_consumed = False
    if paths.ledger.exists():
        with contextlib.suppress(Exception):
            attempt_consumed = bool(
                _read_json(paths.ledger, label="paid refresh provider ledger").get(
                    "attempt_consumed"
                )
            )
    metadata_attempt_consumed = False
    if paths.metadata_ledger.exists():
        with contextlib.suppress(Exception):
            metadata_attempt_consumed = bool(
                _read_json(paths.metadata_ledger, label="paid metadata ledger").get(
                    "events"
                )
            )
    return {
        "contract_sha256": (
            _sha256_file(paths.contract) if paths.contract.exists() else None
        ),
        "intent_sha256": _sha256_file(paths.intent) if paths.intent.exists() else None,
        "network_ledger_sha256": (
            _sha256_file(paths.ledger) if paths.ledger.exists() else None
        ),
        "metadata_contract_sha256": (
            _sha256_file(paths.metadata_contract)
            if paths.metadata_contract.exists()
            else None
        ),
        "metadata_ledger_sha256": (
            _sha256_file(paths.metadata_ledger)
            if paths.metadata_ledger.exists()
            else None
        ),
        "attempt_consumed": attempt_consumed,
        "metadata_attempt_consumed": metadata_attempt_consumed,
        "provider_activity": activity,
    }


def _safe_error_type(error: BaseException) -> str:
    value = type(error).__name__
    return value if value in SAFE_ERROR_TYPES else "Exception"


def _blocked_state_value(
    paths: RefreshPaths, *, error: BaseException, error_code: str
) -> Mapping[str, Any]:
    if error_code not in BLOCKED_ERROR_CODES:
        raise PaidSourceRefreshError("paid blocked state error code漂移")
    return {
        "version": SCHEMA_VERSION,
        "completion_kind": COMPLETION_KIND,
        "phase": "blocked",
        "updated_at": _now_text(),
        **_blocked_state_bindings(paths),
        "error_type": _safe_error_type(error),
        "error_code": error_code,
    }


def _validate_blocked_state(paths: RefreshPaths, value: Mapping[str, Any]) -> None:
    expected_keys = {
        "version",
        "completion_kind",
        "phase",
        "updated_at",
        "contract_sha256",
        "intent_sha256",
        "network_ledger_sha256",
        "metadata_contract_sha256",
        "metadata_ledger_sha256",
        "attempt_consumed",
        "metadata_attempt_consumed",
        "provider_activity",
        "error_type",
        "error_code",
    }
    bindings = _blocked_state_bindings(paths)
    if (
        set(value) != expected_keys
        or value.get("version") != SCHEMA_VERSION
        or value.get("completion_kind") != COMPLETION_KIND
        or value.get("phase") != "blocked"
        or any(value.get(name) != expected for name, expected in bindings.items())
        or value.get("error_type") not in SAFE_ERROR_TYPES
        or value.get("error_code") not in BLOCKED_ERROR_CODES
    ):
        raise PaidSourceRefreshError("paid blocked state形状/绑定漂移")
    _aware_timestamp(value["updated_at"], label="paid blocked state时间")


def _validate_succeeded_state_prefix(
    paths: RefreshPaths, value: Mapping[str, Any]
) -> None:
    contract = _read_json(paths.contract, label="paid refresh contract")
    ledger = _read_json(paths.ledger, label="paid refresh provider ledger")
    _validate_bound_ledger(ledger, contract=contract, terminal=True)
    if (
        set(value)
        != {
            "version",
            "completion_kind",
            "phase",
            "updated_at",
            "contract_sha256",
            "intent_sha256",
            "network_ledger_sha256",
            "metadata_ledger_sha256",
            "attempt_consumed",
            "database",
        }
        or value.get("version") != SCHEMA_VERSION
        or value.get("completion_kind") != COMPLETION_KIND
        or value.get("phase") != "succeeded"
        or value.get("updated_at") != ledger["events"][1]["finished_at"]
        or value.get("attempt_consumed") is not True
        or value.get("contract_sha256") != _sha256_file(paths.contract)
        or value.get("intent_sha256") != _sha256_file(paths.intent)
        or value.get("network_ledger_sha256") != _sha256_file(paths.ledger)
        or value.get("metadata_ledger_sha256")
        != _sha256_file(paths.metadata_ledger)
        or _canonical_bytes(value.get("database"))
        != _canonical_bytes(local_controller._database_identity(paths.database))
    ):
        raise PaidSourceRefreshError("paid succeeded state prefix证据漂移")


def _completion_supersedes_blocked_temp(
    paths: RefreshPaths,
    temporary: Path,
    *,
    expected_source_db_sha256: str,
    expected_source_completion_sha256: str,
) -> bool:
    if not paths.completion.exists():
        return False
    _validate_success_records(
        paths,
        _read_json(paths.contract, label="paid refresh contract"),
        expected_source_db_sha256=expected_source_db_sha256,
        expected_source_completion_sha256=expected_source_completion_sha256,
        transient_run_names=frozenset({temporary.name}),
    )
    temporary.unlink()
    local_controller._fsync_directory(temporary.parent)
    return True


def _recover_blocked_state_temp(
    paths: RefreshPaths,
    *,
    expected_source_db_sha256: str,
    expected_source_completion_sha256: str,
    content_id: int,
) -> None:
    temporary = paths.state.with_name(f".{paths.state.name}.tmp")
    if not os.path.lexists(temporary):
        return
    candidate = _read_json(temporary, label="paid blocked state临时文件")
    if candidate.get("phase") != "blocked":
        return
    _validate_blocked_state(paths, candidate)
    _validate_v2_generation_prefix(
        paths,
        expected_source_db_sha256=expected_source_db_sha256,
        expected_source_completion_sha256=expected_source_completion_sha256,
        content_id=content_id,
    )
    if _completion_supersedes_blocked_temp(
        paths,
        temporary,
        expected_source_db_sha256=expected_source_db_sha256,
        expected_source_completion_sha256=expected_source_completion_sha256,
    ):
        return
    if paths.state.exists():
        current = _read_json(paths.state, label="paid blocked state final")
        if current.get("phase") == "blocked":
            _validate_blocked_state(paths, current)
        elif current.get("phase") == "succeeded":
            _validate_succeeded_state_prefix(paths, current)
        else:
            raise PaidSourceRefreshError("blocked state temp final phase漂移")
    if _completion_supersedes_blocked_temp(
        paths,
        temporary,
        expected_source_db_sha256=expected_source_db_sha256,
        expected_source_completion_sha256=expected_source_completion_sha256,
    ):
        return
    try:
        os.replace(temporary, paths.state)
    except FileNotFoundError:
        if os.path.lexists(temporary) or not paths.state.exists():
            raise
        promoted = _read_json(paths.state, label="coordinator promoted blocked state")
        _validate_blocked_state(paths, promoted)
        if _canonical_bytes(promoted) != _canonical_bytes(candidate):
            raise PaidSourceRefreshError("blocked state并发提升内容漂移")
    local_controller._fsync_directory(paths.state.parent)


def _blocked_state(
    paths: RefreshPaths,
    *,
    contract_sha256: str,
    intent_sha256: str,
    error: BaseException,
    error_code: str = "paid_source_refresh_failed",
) -> None:
    del contract_sha256, intent_sha256
    value = _blocked_state_value(paths, error=error, error_code=error_code)
    _write_json(paths.state, value, immutable=False)


def _metadata_blocked_state(
    paths: RefreshPaths,
    *,
    error: BaseException,
    error_code: str = "metadata_record_failed",
) -> None:
    value = _blocked_state_value(paths, error=error, error_code=error_code)
    _write_json(paths.state, value, immutable=False)


def _generation_records(paths: RefreshPaths) -> tuple[Path, ...]:
    return (
        paths.local_paths.copy_intent,
        paths.local_paths.copy_receipt,
        paths.metadata_contract,
        paths.metadata_ledger,
        paths.contract,
        paths.intent,
        paths.ledger,
        paths.state,
        paths.receipt,
        paths.completion,
    )


def _validate_v2_generation_prefix(
    paths: RefreshPaths,
    *,
    expected_source_db_sha256: str,
    expected_source_completion_sha256: str,
    content_id: int,
) -> None:
    local_controller._private_directory(
        paths.run_root, label="paid generation gate run root"
    )
    entries = list(paths.run_root.iterdir())
    records = _generation_records(paths)
    allowed_names = {path.name for path in records}
    allowed_names.update(f".{path.name}.tmp" for path in records)
    unknown = {path.name for path in entries} - allowed_names
    if unknown:
        raise PaidSourceRefreshError("v2 run root存在未知前缀记录")
    for entry in entries:
        try:
            local_controller._private_file(
                entry, label="paid generation gate record"
            )
        except Exception as exc:
            raise PaidSourceRefreshError(
                "paid generation gate记录不是私有单链接普通文件"
            ) from exc
    anchors = (
        paths.metadata_contract,
        paths.metadata_contract.with_name(f".{paths.metadata_contract.name}.tmp"),
        paths.contract,
        paths.contract.with_name(f".{paths.contract.name}.tmp"),
    )
    present_anchors = [path for path in anchors if os.path.lexists(path)]
    if not present_anchors:
        raise PaidSourceRefreshError(
            "state-only/copy-only/未知旧前缀缺少v2 generation anchor"
        )
    paid_records = records[2:]
    for record in paid_records:
        for candidate_path in (
            record,
            record.with_name(f".{record.name}.tmp"),
        ):
            if not os.path.lexists(candidate_path):
                continue
            candidate = _read_json(candidate_path, label="paid v2 generation record")
            if (
                candidate.get("version") != SCHEMA_VERSION
                or candidate.get("completion_kind") != COMPLETION_KIND
            ):
                raise PaidSourceRefreshError(
                    "旧paid-source-refresh-v1 root禁止混用；必须使用fresh v2 root"
                )
    metadata_anchors = {
        paths.metadata_contract,
        paths.metadata_contract.with_name(f".{paths.metadata_contract.name}.tmp"),
    }
    for anchor in present_anchors:
        candidate = _read_json(anchor, label="paid v2 generation anchor")
        if anchor in metadata_anchors:
            _validate_metadata_contract(
                paths,
                candidate,
                expected_source_db_sha256=expected_source_db_sha256,
                expected_source_completion_sha256=expected_source_completion_sha256,
                content_id=content_id,
            )
        else:
            _validate_contract(
                paths,
                candidate,
                content_id=content_id,
                expected_source_db_sha256=expected_source_db_sha256,
                expected_source_completion_sha256=expected_source_completion_sha256,
            )


def _read_only_generation_gate(
    paths: RefreshPaths,
    *,
    expected_source_db_sha256: str,
    expected_source_completion_sha256: str,
    content_id: int,
) -> None:
    existing_output = paths.database.exists()
    for root, label in (
        (paths.raw_root, "paid generation gate raw root"),
        (paths.media_root, "paid generation gate media root"),
    ):
        if os.path.lexists(root):
            local_controller._private_directory(root, label=label)
            existing_output = existing_output or any(root.iterdir())
    if not paths.run_root.exists():
        if existing_output:
            raise PaidSourceRefreshError("缺少v2 generation anchor的既有输出禁止复用")
        return
    entries = list(paths.run_root.iterdir())
    if not entries:
        if existing_output:
            raise PaidSourceRefreshError("空run root伴随既有输出，禁止推断generation")
        return
    _validate_v2_generation_prefix(
        paths,
        expected_source_db_sha256=expected_source_db_sha256,
        expected_source_completion_sha256=expected_source_completion_sha256,
        content_id=content_id,
    )


def run_refresh(
    *,
    source_db_path: Path,
    source_completion_path: Path,
    expected_source_db_sha256: str,
    expected_source_completion_sha256: str,
    db_path: Path,
    raw_root: Path,
    media_root: Path,
    run_root: Path,
    content_ids: Sequence[int],
    endpoint_info_fetcher: Callable[[], Mapping[str, Any]] | None = None,
    balance_checker: Callable[[str, str], Mapping[str, Any]] | None = None,
    detail_fetcher: Callable[[str, str], tuple[Any, Mapping[str, Any]]] | None = None,
    key_loader: Callable[[], str] | None = None,
    after_fetch_hook: Callable[[], None] | None = None,
) -> Mapping[str, Any]:
    content_id = _ordered_one(content_ids)
    paths = _paths(
        source_db_path=source_db_path,
        source_completion_path=source_completion_path,
        db_path=db_path,
        raw_root=raw_root,
        media_root=media_root,
        run_root=run_root,
    )
    existing_completion = paths.completion.exists()
    _validate_paths(paths, database_must_exist=paths.database.exists())
    initial_sidecars = local_controller._database_sidecars(paths.database)
    if initial_sidecars and not (paths.contract.exists() and paths.intent.exists()):
        raise PaidSourceRefreshError(
            "首次或无intent的paid refresh数据库存在未知sidecar"
        )
    price_evidence: Mapping[str, Any] | None = None
    endpoint_info_calls_current = 0
    user_info_calls_current = 0
    source_evidence = _parent_source_evidence(
        paths,
        content_id=content_id,
        expected_source_db_sha256=expected_source_db_sha256,
        expected_source_completion_sha256=expected_source_completion_sha256,
    )
    _validate_parent_separation(paths, source_evidence)
    identity = _task_identity(
        source_db_sha256=expected_source_db_sha256,
        source_completion_sha256=expected_source_completion_sha256,
        content_id=content_id,
    )
    _read_only_generation_gate(
        paths,
        expected_source_db_sha256=expected_source_db_sha256,
        expected_source_completion_sha256=expected_source_completion_sha256,
        content_id=content_id,
    )
    with _claims(paths):
        _recover_blocked_state_temp(
            paths,
            expected_source_db_sha256=expected_source_db_sha256,
            expected_source_completion_sha256=expected_source_completion_sha256,
            content_id=content_id,
        )
        _prepare_roots(paths)
        early_contract_temporary = paths.contract.with_name(
            f".{paths.contract.name}.tmp"
        )
        for candidate_path, label in (
            (paths.contract, "paid refresh existing contract"),
            (early_contract_temporary, "paid refresh contract temp"),
        ):
            if os.path.lexists(candidate_path):
                candidate = _read_json(candidate_path, label=label)
                if (
                    candidate.get("version") != SCHEMA_VERSION
                    or candidate.get("completion_kind") != COMPLETION_KIND
                ):
                    raise PaidSourceRefreshError(
                        "旧paid-source-refresh-v1 root禁止混用；必须使用fresh v2 root"
                    )
        if (
            not paths.metadata_contract.exists()
            and not os.path.lexists(
                paths.metadata_contract.with_name(
                    f".{paths.metadata_contract.name}.tmp"
                )
            )
        ):
            legacy_markers = {
                paths.intent.name,
                paths.ledger.name,
                paths.receipt.name,
                paths.completion.name,
            }
            existing_names = {path.name for path in paths.run_root.iterdir()}
            if existing_names.intersection(legacy_markers):
                raise PaidSourceRefreshError(
                    "缺少v2 metadata contract的旧/未知root禁止混用"
                )
        if not paths.contract.exists():
            if any(paths.raw_root.iterdir()) or any(paths.media_root.iterdir()):
                raise PaidSourceRefreshError("首次paid refresh输出根非空")
            local_controller._ensure_work_copy(
                paths.local_paths,
                content_ids=[content_id],
                source_evidence=source_evidence,
                expected_source_db_sha256=expected_source_db_sha256,
                expected_source_completion_sha256=expected_source_completion_sha256,
            )
        for record, label in (
            (paths.local_paths.copy_intent, "database copy intent"),
            (paths.local_paths.copy_receipt, "database copy receipt"),
        ):
            _cleanup_final_temp(record, label=label)
        try:
            metadata_contract = _ensure_metadata_records(
                paths,
                expected_source_db_sha256=expected_source_db_sha256,
                expected_source_completion_sha256=expected_source_completion_sha256,
                content_id=content_id,
            )
        except Exception as exc:
            with contextlib.suppress(Exception):
                _metadata_blocked_state(paths, error=exc)
            raise
        contract_temporary = paths.contract.with_name(f".{paths.contract.name}.tmp")
        if not paths.contract.exists() and os.path.lexists(contract_temporary):
            local_controller._recover_immutable_json_temp(
                paths.contract,
                label="paid refresh contract",
                validator=lambda value: _validate_contract(
                    paths,
                    value,
                    content_id=content_id,
                    expected_source_db_sha256=expected_source_db_sha256,
                    expected_source_completion_sha256=expected_source_completion_sha256,
                ),
            )
        elif paths.contract.exists():
            _cleanup_final_temp(paths.contract, label="paid refresh contract")
        existing_completion = paths.completion.exists()
        if existing_completion:
            for record, label in (
                (paths.metadata_contract, "paid metadata contract"),
                (paths.metadata_ledger, "paid metadata ledger"),
                (paths.intent, "paid refresh intent"),
                (paths.ledger, "paid refresh ledger"),
                (paths.state, "paid refresh state"),
                (paths.receipt, "paid refresh receipt"),
                (paths.completion, "paid refresh completion"),
            ):
                _cleanup_final_temp(record, label=label)
            contract = _read_json(paths.contract, label="paid refresh contract")
            evidence = _validate_success_records(
                paths,
                contract,
                expected_source_db_sha256=expected_source_db_sha256,
                expected_source_completion_sha256=expected_source_completion_sha256,
            )
            return {
                "ok": True,
                "status": "succeeded",
                "completion_kind": COMPLETION_KIND,
                "idempotent": True,
                "provider_calls": 0,
                "metadata_calls": 0,
                "total_provider_calls": 0,
                "provider_call_accounting": {
                    **_provider_call_accounting(
                        endpoint_info_calls=0,
                        user_info_calls=0,
                        detail_calls=0,
                    ),
                    "historical": evidence["completion"]["provider_call_history"],
                },
                "content_id": content_id,
                "completion_sha256": _sha256_file(paths.completion),
                "receipt_sha256": _sha256_file(paths.receipt),
                "database": evidence["completion"]["database"],
            }
        fresh_contract = not paths.contract.exists()
        if fresh_contract:
            with closing(local_controller._immutable_connection(paths.database)) as connection:
                source_plan = _source_plan(
                    connection,
                    content_id=content_id,
                    source_evidence=source_evidence,
                )
            try:
                metadata_before_price = _read_json(
                    paths.metadata_ledger, label="paid metadata ledger before price"
                )
                if metadata_before_price.get("price_evidence") is None:
                    endpoint_info_calls_current = 3
                price_evidence = _collect_price_evidence(
                    paths,
                    metadata_contract,
                    endpoint_info_fetcher=endpoint_info_fetcher,
                )
            except Exception as exc:
                with contextlib.suppress(Exception):
                    _metadata_blocked_state(
                        paths,
                        error=exc,
                        error_code="metadata_price_probe_failed",
                    )
                if isinstance(exc, PaidSourceRefreshError):
                    raise
                raise PaidSourceRefreshError(
                    f"paid metadata price probe blocked：{type(exc).__name__}: {exc}"
                ) from exc
            contract = _build_contract(
                paths,
                content_id=content_id,
                source_evidence=source_evidence,
                plan=source_plan,
                price_evidence=price_evidence or {},
                identity=identity,
            )
            _write_json(paths.contract, contract, immutable=True)
            _bind_metadata_refresh_contract(paths, metadata_contract)
            intent = _intent_value(
                paths=paths,
                contract_sha256=_sha256_file(paths.contract),
                contract=contract,
            )
            intent_sha256 = _write_json(paths.intent, intent, immutable=True)
            request_id = hashlib.sha256(
                f"{_sha256_file(paths.contract)}:{intent_sha256}".encode()
            ).hexdigest()[:32]
            ledger = _ledger_value(
                contract_sha256=_sha256_file(paths.contract),
                intent_sha256=intent_sha256,
                request_id=request_id,
            )
            _write_json(paths.ledger, ledger, immutable=True)
        else:
            contract = _read_json(paths.contract, label="paid refresh contract")
            _validate_contract(
                paths,
                contract,
                content_id=content_id,
                expected_source_db_sha256=expected_source_db_sha256,
                expected_source_completion_sha256=expected_source_completion_sha256,
            )
            _bind_metadata_refresh_contract(paths, metadata_contract)
            intent_temporary = paths.intent.with_name(f".{paths.intent.name}.tmp")
            if not paths.intent.exists() and os.path.lexists(intent_temporary):
                local_controller._recover_immutable_json_temp(
                    paths.intent,
                    label="paid refresh intent",
                    validator=lambda value: _validate_intent(
                        paths, value, contract
                    ),
                )
            elif paths.intent.exists():
                _cleanup_final_temp(paths.intent, label="paid refresh intent")
            else:
                _validate_pristine_record_prefix(
                    paths,
                    contract,
                    allowed_run_names=(
                        paths.local_paths.copy_intent.name,
                        paths.local_paths.copy_receipt.name,
                        paths.metadata_contract.name,
                        paths.metadata_ledger.name,
                        paths.contract.name,
                        *((paths.state.name,) if paths.state.exists() else ()),
                    ),
                )
                _write_json(
                    paths.intent,
                    _intent_value(
                        paths=paths,
                        contract_sha256=_sha256_file(paths.contract),
                        contract=contract,
                    ),
                    immutable=True,
                )
            intent = _read_json(paths.intent, label="paid refresh intent")
            _validate_intent(paths, intent, contract)
            intent_sha256 = _sha256_file(paths.intent)
            ledger_temporary = paths.ledger.with_name(f".{paths.ledger.name}.tmp")
            if not paths.ledger.exists() and not os.path.lexists(ledger_temporary):
                _validate_pristine_record_prefix(
                    paths,
                    contract,
                    allowed_run_names=(
                        paths.local_paths.copy_intent.name,
                        paths.local_paths.copy_receipt.name,
                        paths.metadata_contract.name,
                        paths.metadata_ledger.name,
                        paths.contract.name,
                        paths.intent.name,
                        *((paths.state.name,) if paths.state.exists() else ()),
                    ),
                )
                request_id = hashlib.sha256(
                    f"{_sha256_file(paths.contract)}:{intent_sha256}".encode()
                ).hexdigest()[:32]
                _write_json(
                    paths.ledger,
                    _ledger_value(
                        contract_sha256=_sha256_file(paths.contract),
                        intent_sha256=intent_sha256,
                        request_id=request_id,
                    ),
                    immutable=True,
                )
            else:
                _recover_ledger_temp(
                    paths,
                    contract=contract,
                    intent_sha256=intent_sha256,
                )
            ledger = _read_json(paths.ledger, label="paid refresh provider ledger")
            if (
                ledger.get("contract_sha256") != _sha256_file(paths.contract)
                or ledger.get("intent_sha256") != intent_sha256
            ):
                raise PaidSourceRefreshError("paid refresh ledger合同绑定漂移")
            current_sidecars = local_controller._database_sidecars(paths.database)
            if current_sidecars:
                local_controller._validate_recoverable_sidecars(paths.local_paths)
                local_controller._finalize_database(paths.database)
                _validate_contract(
                    paths,
                    contract,
                    content_id=content_id,
                    expected_source_db_sha256=expected_source_db_sha256,
                    expected_source_completion_sha256=expected_source_completion_sha256,
                )
        for record, label in (
            (paths.local_paths.copy_intent, "database copy intent"),
            (paths.local_paths.copy_receipt, "database copy receipt"),
        ):
            _cleanup_final_temp(record, label=label)
        _validate_bound_ledger(ledger, contract=contract, terminal=False)
        prefix_rows = _validate_database_prefix(paths, contract)
        _validate_output_prefix(paths, contract, prefix_rows)
        committed = _committed_raw(paths, contract)
        activity = _provider_activity(paths, contract)
        detail_events = list(ledger["events"])
        transport_terminal = (
            len(detail_events) == 2
            and detail_events[1].get("outcome") == "transport_failed"
        )
        if (
            committed is None
            and detail_events
            and all(value == 1 for value in activity.values())
            and (len(detail_events) == 1 or transport_terminal)
        ):
            recovery_error = PaidSourceRefreshError(
                "recovered interrupted/failed detail transport"
            )
            _commit_transport_failed_capture(
                paths,
                contract,
                error=recovery_error,
            )
            local_controller._finalize_database(paths.database)
            if len(detail_events) == 1:
                failed = {
                    "index": 1,
                    "phase": "terminal",
                    "request_id": ledger["request_id"],
                    "endpoint": DETAIL_PATH,
                    "outcome": "transport_failed",
                    "error_type": "RecoveredTransportFailure",
                    "error_code": "detail_transport_failed",
                    "finished_at": _now_text(),
                }
                ledger = {**ledger, "events": [detail_events[0], failed]}
                ledger = {
                    **ledger,
                    "events_sha256": _json_sha256(ledger["events"]),
                }
                _write_json(paths.ledger, ledger, immutable=False)
            prefix_rows = _validate_database_prefix(paths, contract)
            _validate_output_prefix(paths, contract, prefix_rows)
            with contextlib.suppress(Exception):
                _blocked_state(
                    paths,
                    contract_sha256=_sha256_file(paths.contract),
                    intent_sha256=intent_sha256,
                    error=recovery_error,
                    error_code="detail_transport_closed",
                )
            raise PaidSourceRefreshError(
                "detail transport已按$0.001保守上限强闭包，fresh v2 root方可继续"
            )
        if committed is None and (
            bool(ledger["events"])
            or any(activity.values())
        ):
            raise PaidSourceRefreshError(
                "paid request已经opening/attempt但无完整committed raw，永久禁止自动重呼"
            )
        try:
            provider_calls = 0
            if committed is None:
                _require_price_fresh(contract["price_evidence"])
                key, balance, user_info_calls_current = _ensure_user_info(
                    paths,
                    metadata_contract,
                    contract=contract,
                    key_loader=key_loader,
                    balance_checker=balance_checker,
                )
                opening = {
                    "index": 0,
                    "phase": "opening",
                    "request_id": ledger["request_id"],
                    "endpoint": DETAIL_PATH,
                    "aweme_id": str(contract["target"]["platform_content_id"]),
                    "created_at": _now_text(),
                }
                ledger = _ledger_value(
                    contract_sha256=_sha256_file(paths.contract),
                    intent_sha256=intent_sha256,
                    request_id=str(ledger["request_id"]),
                    balance_check=balance,
                    events=[opening],
                )
                _write_json(paths.ledger, ledger, immutable=False)
                _create_budget(paths, contract)
                claim = capture_module.claim_content_slot(
                    db_path=paths.database,
                    content_id=content_id,
                    stage=STAGE,
                    window_key=contract["route"]["window_key"],
                    provider=PROVIDER,
                    adapter_version=ADAPTER_VERSION,
                )
                with (
                    closing(storage_module.connect(paths.database)) as connection,
                    storage_module.transaction(connection),
                ):
                    usage_id, unit_price, currency = capture_module._reserve_budget(
                        connection,
                        budget_id=contract["budget"]["budget_id"],
                        provider=PROVIDER,
                        operation=OPERATION,
                        task_id=contract["budget"]["task_id"],
                        task_max_amount=UNIT_PRICE,
                    )
                if (
                    _exact_decimal(unit_price, label="reserved unit price")
                    != Decimal(str(UNIT_PRICE))
                    or currency != "USD"
                ):
                    raise PaidSourceRefreshError("paid refresh reserve价格/币种漂移")
                fetcher = detail_fetcher or _default_detail_fetch
                provider_calls = 1
                try:
                    payload, transcript = fetcher(
                        str(contract["target"]["platform_content_id"]), key
                    )
                except Exception as exc:
                    failed = {
                        "index": 1,
                        "phase": "terminal",
                        "request_id": ledger["request_id"],
                        "endpoint": DETAIL_PATH,
                        "outcome": "transport_failed",
                        "error_type": _safe_error_type(exc),
                        "error_code": "detail_transport_failed",
                        "finished_at": _now_text(),
                    }
                    ledger = {**ledger, "events": [opening, failed]}
                    ledger = {**ledger, "events_sha256": _json_sha256(ledger["events"])}
                    _write_json(paths.ledger, ledger, immutable=False)
                    _commit_transport_failed_capture(
                        paths,
                        contract,
                        error=exc,
                        claim=claim,
                        usage_id=usage_id,
                    )
                    local_controller._finalize_database(paths.database)
                    raise
                expected_transcript = {
                    "url_sha256",
                    "response_sha256",
                    "response_json_sha256",
                    "response_bytes",
                    "http_status",
                    "mime_type",
                    "endpoint",
                    "aweme_id",
                }
                if (
                    set(transcript) != expected_transcript
                    or transcript.get("endpoint") != DETAIL_PATH
                    or transcript.get("aweme_id")
                    != str(contract["target"]["platform_content_id"])
                    or type(transcript.get("http_status")) is not int
                    or transcript.get("http_status") != 200
                    or transcript.get("mime_type") != "application/json"
                    or type(transcript.get("response_bytes")) is not int
                    or transcript.get("response_bytes") <= 0
                ):
                    transcript_error = PaidSourceRefreshError(
                        "detail网络transcript漂移"
                    )
                    failed = {
                        "index": 1,
                        "phase": "terminal",
                        "request_id": ledger["request_id"],
                        "endpoint": DETAIL_PATH,
                        "outcome": "transport_failed",
                        "error_type": _safe_error_type(transcript_error),
                        "error_code": "detail_transport_failed",
                        "finished_at": _now_text(),
                    }
                    ledger = {**ledger, "events": [opening, failed]}
                    ledger = {
                        **ledger,
                        "events_sha256": _json_sha256(ledger["events"]),
                    }
                    _write_json(paths.ledger, ledger, immutable=False)
                    _commit_transport_failed_capture(
                        paths,
                        contract,
                        error=transcript_error,
                        claim=claim,
                        usage_id=usage_id,
                    )
                    local_controller._finalize_database(paths.database)
                    raise transcript_error
                terminal_event = {
                    "index": 1,
                    "phase": "terminal",
                    "request_id": ledger["request_id"],
                    "endpoint": DETAIL_PATH,
                    "outcome": "response_received",
                    **transcript,
                    "finished_at": _now_text(),
                }
                ledger = _ledger_value(
                    contract_sha256=_sha256_file(paths.contract),
                    intent_sha256=intent_sha256,
                    request_id=str(ledger["request_id"]),
                    balance_check=balance,
                    events=[opening, terminal_event],
                )
                _write_json(paths.ledger, ledger, immutable=False)
                try:
                    result = providers_module._parse_douyin_stage_payload(
                        "detail",
                        str(contract["target"]["platform_content_id"]),
                        payload,
                        status=200,
                    )
                    _validate_live_payload(
                        payload=result.raw_response,
                        data=result.data,
                        target=contract["target"],
                    )
                except Exception as exc:
                    if isinstance(payload, Mapping):
                        _commit_rejected_capture(
                            paths,
                            contract,
                            claim=claim,
                            usage_id=usage_id,
                            raw_response=payload,
                            error=exc,
                        )
                        local_controller._finalize_database(paths.database)
                    raise
                outcome = _commit_successful_capture(
                    paths,
                    contract,
                    claim=claim,
                    usage_id=usage_id,
                    result=result,
                )
                local_controller._finalize_database(paths.database)
                if after_fetch_hook is not None:
                    after_fetch_hook()
                committed = _committed_raw(paths, contract)
                if committed is None or int(outcome.raw_response_id) != committed[0]:
                    raise PaidSourceRefreshError("paid fetch未形成唯一committed raw")
            raw_response_id, data, _provenance = committed
            _materialize_manifest(
                paths,
                contract,
                raw_response_id=raw_response_id,
                data=data,
            )
            local_controller._finalize_database(paths.database)
            prefix_rows = _validate_database_prefix(paths, contract)
            _validate_output_prefix(paths, contract, prefix_rows)
            materialized = _validate_refresh_materialization(paths, contract)
            delta = _validate_database_delta(paths, contract, materialized)
            ledger = _read_json(paths.ledger, label="paid refresh provider ledger")
            _validate_bound_ledger(ledger, contract=contract, terminal=True)
            records = _completion_records(
                paths,
                contract=contract,
                intent_sha256=intent_sha256,
                ledger=ledger,
                materialized=materialized,
                delta=delta,
            )
            _validate_success_records(
                paths,
                contract,
                expected_source_db_sha256=expected_source_db_sha256,
                expected_source_completion_sha256=expected_source_completion_sha256,
            )
            return {
                "ok": True,
                "status": "succeeded",
                "completion_kind": COMPLETION_KIND,
                "idempotent": False,
                "provider_calls": provider_calls,
                "metadata_calls": endpoint_info_calls_current
                + user_info_calls_current,
                "total_provider_calls": endpoint_info_calls_current
                + user_info_calls_current
                + provider_calls,
                "provider_call_accounting": _provider_call_accounting(
                    endpoint_info_calls=endpoint_info_calls_current,
                    user_info_calls=user_info_calls_current,
                    detail_calls=provider_calls,
                ),
                "content_id": content_id,
                "completion_sha256": records["completion_sha256"],
                "receipt_sha256": records["receipt_sha256"],
                "database": local_controller._database_identity(paths.database),
            }
        except Exception as exc:
            with contextlib.suppress(Exception):
                local_controller._finalize_database(paths.database)
            with contextlib.suppress(Exception):
                _blocked_state(
                    paths,
                    contract_sha256=_sha256_file(paths.contract),
                    intent_sha256=intent_sha256,
                    error=exc,
                )
            if isinstance(exc, PaidSourceRefreshError):
                raise
            raise PaidSourceRefreshError(
                f"paid source refresh blocked：{type(exc).__name__}: {exc}"
            ) from exc


def _compatible_source_snapshot(
    connection: sqlite3.Connection,
    *,
    content_id: int,
    evidence: Mapping[str, Any],
) -> Mapping[str, Any]:
    contract = evidence["paid_contract"]
    expected = evidence["source_snapshot"]
    content = connection.execute(
        "SELECT * FROM content_items WHERE id=?", (content_id,)
    ).fetchone()
    if content is None:
        raise PaidSourceRefreshError("paid source content不存在")
    stable_content = {
        key: local_controller._json_value(content[key])
        for key in content.keys()
        if key not in local_controller.CONTENT_MUTABLE_COLUMNS
    }
    artifact_id = int(expected["artifact"]["id"])
    raw_id = int(expected["raw_response"]["id"])
    artifact = connection.execute(
        "SELECT * FROM evidence_artifacts WHERE id=? AND content_id=?",
        (artifact_id, content_id),
    ).fetchone()
    raw = connection.execute(
        "SELECT * FROM provider_raw_responses WHERE id=? AND content_id=?",
        (raw_id, content_id),
    ).fetchone()
    if artifact is None or raw is None:
        raise PaidSourceRefreshError("paid source raw/artifact行缺失")
    artifact_row = _row_values(artifact)
    raw_row = _row_values(raw)
    artifact_file = _file_evidence(
        Path(str(expected["artifact_file"]["path"])), label="paid handoff manifest"
    )
    raw_file = _file_evidence(
        Path(str(expected["raw_response_file"]["path"])), label="paid handoff raw"
    )
    if (
        stable_content != expected["content"]
        or artifact_row != expected["artifact"]
        or raw_row != expected["raw_response"]
        or artifact_file != expected["artifact_file"]
        or raw_file != expected["raw_response_file"]
    ):
        raise PaidSourceRefreshError("paid source handoff行或文件漂移")
    try:
        manifest = json.loads(Path(str(artifact_file["path"])).read_bytes())
        raw_body = json.loads(Path(str(raw_file["path"])).read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PaidSourceRefreshError("paid source handoff JSON漂移") from exc
    parsed = providers_module._parse_douyin_stage_payload(
        "detail", str(contract["target"]["platform_content_id"]), raw_body, status=200
    )
    provenance = _validate_live_payload(
        payload=raw_body, data=parsed.data, target=contract["target"]
    )
    urls = list(manifest.get("urls") or []) if isinstance(manifest, Mapping) else []
    if (
        manifest != expected["artifact_body"]
        or provenance != expected["url_provenance"]
        or _json_sha256(raw_body) != expected["raw_response_body_sha256"]
    ):
        raise PaidSourceRefreshError("paid source handoff正文/provenance漂移")
    result = {
        "content": stable_content,
        "artifact": artifact_row,
        "artifact_body": manifest,
        "artifact_file": artifact_file,
        "urls": [
            local_controller._safe_url(
                url,
                media_kind="video",
                platform=str(contract["target"]["platform"]),
                provider=str(contract["route"]["provider"]),
                operation=str(contract["route"]["operation"]),
            )
            for url in urls
        ],
        "urls_sha256": _json_sha256(urls),
        "download_urls": list(provenance["allowed_urls"]),
        "download_urls_sha256": provenance["allowed_urls_sha256"],
        "image_groups": [],
        "image_groups_sha256": _json_sha256([]),
        "raw_response": raw_row,
        "raw_response_file": raw_file,
        "raw_response_body_sha256": _json_sha256(raw_body),
        "url_provenance": provenance,
        "paid_refresh": {
            "completion_sha256": evidence["sha256"],
            "contract_sha256": evidence["contract"]["sha256"],
            "receipt_sha256": evidence["receipt"]["sha256"],
            "budget_id": contract["budget"]["budget_id"],
            "task_id": contract["budget"]["task_id"],
        },
    }
    if result != expected:
        raise PaidSourceRefreshError("paid source snapshot重算漂移")
    return result


def source_snapshot_from_evidence(
    connection: sqlite3.Connection,
    content_id: int,
    evidence: Mapping[str, Any],
) -> Mapping[str, Any]:
    if evidence.get("completion_kind") != COMPLETION_KIND:
        raise PaidSourceRefreshError("不是paid-source-refresh-v1 evidence")
    return _compatible_source_snapshot(
        connection, content_id=content_id, evidence=evidence
    )


def validate_completion_for_local_analysis(
    *,
    source_db_path: Path,
    source_completion_path: Path,
    expected_source_db_sha256: str,
    expected_source_completion_sha256: str,
    content_ids: Sequence[int],
) -> Mapping[str, Any]:
    content_id = _ordered_one(content_ids)
    source_db = Path(os.path.abspath(source_db_path)).resolve()
    completion_path = Path(os.path.abspath(source_completion_path)).resolve()
    local_controller._require_clean_database(source_db)
    completion_file = _file_evidence(
        completion_path, label="paid source completion"
    )
    database_file = _file_evidence(source_db, label="paid source database")
    if (
        not re.fullmatch(r"[0-9a-f]{64}", expected_source_db_sha256)
        or database_file["sha256"] != expected_source_db_sha256
        or not re.fullmatch(r"[0-9a-f]{64}", expected_source_completion_sha256)
        or completion_file["sha256"] != expected_source_completion_sha256
    ):
        raise PaidSourceRefreshError("paid source外部expected SHA未命中")
    completion = _read_json(completion_path, label="paid source completion")
    if (
        set(completion) != COMPLETION_FIELDS
        or not isinstance(completion.get("contract_sha256"), str)
        or re.fullmatch(r"[0-9a-f]{64}", str(completion["contract_sha256"]))
        is None
        or completion.get("completion_kind") != COMPLETION_KIND
        or completion.get("status") != "succeeded"
        or type(completion.get("version")) is not int
        or completion.get("version") != SCHEMA_VERSION
    ):
        raise PaidSourceRefreshError("paid source completion不是成功typed终态")
    run_root = Path(str(completion.get("run_root") or "")).resolve()
    if completion_path != run_root / "completion.json":
        raise PaidSourceRefreshError("paid source completion不位于其run_root")
    contract_path = run_root / "refresh-contract.json"
    contract_file = _file_evidence(contract_path, label="paid source contract")
    if contract_file["sha256"] != completion["contract_sha256"]:
        raise PaidSourceRefreshError("paid source completion未绑定contract文件")
    contract = _read_json(contract_path, label="paid source contract")
    base = contract.get("base_source")
    roots = contract.get("roots")
    if not isinstance(base, Mapping) or not isinstance(roots, Mapping):
        raise PaidSourceRefreshError("paid source contract lineage/roots缺失")
    paths = _paths(
        source_db_path=Path(str(base["database"]["path"])),
        source_completion_path=Path(str(base["path"])),
        db_path=source_db,
        raw_root=Path(str(roots["raw_root"])),
        media_root=Path(str(roots["media_root"])),
        run_root=run_root,
    )
    _validate_paths(paths, database_must_exist=True)
    if paths.completion != completion_path:
        raise PaidSourceRefreshError("paid source completion路径canonical漂移")
    evidence = _validate_success_records(
        paths,
        contract,
        expected_source_db_sha256=str(base["database"]["sha256"]),
        expected_source_completion_sha256=str(base["sha256"]),
    )
    if completion.get("database") != local_controller._database_identity(source_db):
        raise PaidSourceRefreshError("paid completion未绑定当前source DB")
    materialized = evidence["materialized"]
    stable_content = {
        key: value
        for key, value in contract["target"].items()
        if key not in local_controller.CONTENT_MUTABLE_COLUMNS
    }
    manifest = materialized["artifact_body"]
    urls = list(manifest["urls"])
    source_snapshot = {
        "content": stable_content,
        "artifact": materialized["artifact"],
        "artifact_body": manifest,
        "artifact_file": materialized["artifact_file"],
        "urls": [
            local_controller._safe_url(
                url,
                media_kind="video",
                platform=str(contract["target"]["platform"]),
                provider=str(contract["route"]["provider"]),
                operation=str(contract["route"]["operation"]),
            )
            for url in urls
        ],
        "urls_sha256": _json_sha256(urls),
        "download_urls": list(materialized["url_provenance"]["allowed_urls"]),
        "download_urls_sha256": materialized["url_provenance"][
            "allowed_urls_sha256"
        ],
        "image_groups": [],
        "image_groups_sha256": _json_sha256([]),
        "raw_response": materialized["raw_response"],
        "raw_response_file": materialized["raw_response_file"],
        "raw_response_body_sha256": materialized["raw_response_body_sha256"],
        "url_provenance": materialized["url_provenance"],
        "paid_refresh": {
            "completion_sha256": completion_file["sha256"],
            "contract_sha256": _sha256_file(paths.contract),
            "receipt_sha256": _sha256_file(paths.receipt),
            "budget_id": contract["budget"]["budget_id"],
            "task_id": contract["budget"]["task_id"],
        },
    }
    result = {
        "completion_kind": COMPLETION_KIND,
        "completed_at": completion["completed_at"],
        "max_handoff_age_seconds": contract["max_handoff_age_seconds"],
        "path": str(completion_path),
        "sha256": completion_file["sha256"],
        "byte_size": completion_file["byte_size"],
        "contract": {
            "path": str(paths.contract),
            "sha256": _sha256_file(paths.contract),
            "byte_size": paths.contract.stat().st_size,
            "run_root": str(paths.run_root),
            "raw_root": str(paths.raw_root),
            "media_root": str(paths.media_root),
            "base_source": base,
            "content_id": content_id,
            "route": contract["route"],
            "budget": contract["budget"],
        },
        "receipt": {
            "path": str(paths.receipt),
            "sha256": _sha256_file(paths.receipt),
            "byte_size": paths.receipt.stat().st_size,
        },
        "database": {
            "path": database_file["path"],
            "sha256": database_file["sha256"],
            "bytes": database_file["byte_size"],
            "inode": database_file["inode"],
            "nlink": database_file["nlink"],
        },
        "base_source": base,
        "target_count": 1,
        "explicit_ids_membership_sha256": _json_sha256([content_id]),
        "paid_contract": contract,
        "source_snapshot": source_snapshot,
    }
    with closing(local_controller._immutable_connection(source_db)) as connection:
        _compatible_source_snapshot(
            connection, content_id=content_id, evidence=result
        )
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="One-shot paid Douyin source refresh against a disposable clone."
    )
    parser.add_argument("--source-db", required=True, type=Path)
    parser.add_argument("--source-completion", required=True, type=Path)
    parser.add_argument("--expected-source-db-sha256", required=True)
    parser.add_argument("--expected-source-completion-sha256", required=True)
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--raw-root", required=True, type=Path)
    parser.add_argument("--media-root", required=True, type=Path)
    parser.add_argument("--run-root", required=True, type=Path)
    parser.add_argument("--content-id", required=True, action="append", type=int)
    parser.add_argument("--apply", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    keywords = {
        "source_db_path": arguments.source_db,
        "source_completion_path": arguments.source_completion,
        "expected_source_db_sha256": arguments.expected_source_db_sha256,
        "expected_source_completion_sha256": arguments.expected_source_completion_sha256,
        "db_path": arguments.db,
        "raw_root": arguments.raw_root,
        "media_root": arguments.media_root,
        "run_root": arguments.run_root,
        "content_ids": arguments.content_id,
    }
    try:
        result = run_refresh(**keywords) if arguments.apply else plan_refresh(**keywords)
    except Exception as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "status": "blocked",
                    "error_type": _safe_error_type(exc),
                    "error_code": "paid_source_refresh_blocked",
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
