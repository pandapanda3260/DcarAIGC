#!/usr/bin/env python3
"""Run the zero-provider full-history cache replay in durable clone batches.

This controller never publishes a database.  It adds a durable intent/receipt
layer around ``materialize_full_history_discovery_cache`` so an interrupted
clone run can be reconciled and resumed without replaying completed content.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import shutil
import sqlite3
import stat
import sys
import time
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence

if __package__ in {None, ""}:
    repository_root = str(Path(__file__).resolve().parents[1])
    if repository_root not in sys.path:
        sys.path.insert(0, repository_root)

from scripts import materialize_full_history_discovery_cache as replay
from v8 import capture as capture_module
from v8 import identity as identity_module
from v8 import media as media_module
from v8 import operations as operations_module
from v8 import storage as storage_module


ALLOWED_CHANGED_TABLES = {
    "content_items",
    "evidence_artifacts",
    "fetch_attempts",
    "fetch_slots",
    "provider_raw_responses",
}
APPEND_CHANGED_TABLES = (
    "evidence_artifacts",
    "fetch_attempts",
    "fetch_slots",
    "provider_raw_responses",
)
DEFAULT_CANARY_SIZE = 100
DEFAULT_BATCH_SIZE = 500
DEFAULT_MIN_FREE_BYTES = 10 * 1024**3


class BatchReplayError(RuntimeError):
    pass


@dataclass(frozen=True)
class BatchPaths:
    run_root: Path
    batches: Path
    contract: Path
    completion: Path
    claim: Path


def _now_text() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


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


def _private_file(path: Path, *, label: str) -> os.stat_result:
    try:
        metadata = path.lstat()
    except FileNotFoundError as exc:
        raise BatchReplayError(f"{label}不存在：{path}") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or path.is_symlink()
        or metadata.st_nlink != 1
    ):
        raise BatchReplayError(f"{label}不是私有单链接普通文件：{path}")
    return metadata


def _private_directory(path: Path, *, label: str) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError as exc:
        raise BatchReplayError(f"{label}不存在：{path}") from exc
    if not stat.S_ISDIR(metadata.st_mode) or path.is_symlink():
        raise BatchReplayError(f"{label}不是私有目录：{path}")


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_json(path: Path, value: Mapping[str, Any]) -> str:
    return _atomic_bytes(path, _canonical_bytes(value))


def _atomic_bytes(path: Path, body: bytes, *, mode: int = 0o600) -> str:
    digest = replay._sha256_bytes(body)
    if os.path.lexists(path):
        _private_file(path, label="持久记录")
        if path.read_bytes() != body:
            raise BatchReplayError(f"持久记录已存在但内容漂移：{path}")
        with path.open("rb") as handle:
            os.fsync(handle.fileno())
        _fsync_directory(path.parent)
        return digest
    temporary = path.with_name(f".{path.name}.tmp")
    if os.path.lexists(temporary):
        _private_file(temporary, label="持久记录临时文件")
        temporary.unlink()
        _fsync_directory(path.parent)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(temporary, flags, mode)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    _fsync_directory(path.parent)
    return digest


def _read_json(path: Path, *, label: str) -> Mapping[str, Any]:
    _private_file(path, label=label)
    try:
        value = json.loads(path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BatchReplayError(f"{label}不是合法 JSON：{path}") from exc
    if not isinstance(value, Mapping):
        raise BatchReplayError(f"{label}必须是 JSON object：{path}")
    return value


def _paths(run_root: Path) -> BatchPaths:
    root = run_root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    _private_directory(root, label="批处理运行目录")
    batches = root / "batches"
    batches.mkdir(exist_ok=True)
    _private_directory(batches, label="批次记录目录")
    return BatchPaths(
        run_root=root,
        batches=batches,
        contract=root / "run-contract.json",
        completion=root / "completion.json",
        claim=root / ".batch-materialization.claim",
    )


def _database_claim_path(db_path: Path) -> Path:
    return db_path.resolve().with_name(
        f".{db_path.resolve().name}.full-history-cache-batches.claim"
    )


def _output_claim_path(root: Path) -> Path:
    resolved = root.resolve()
    parent = resolved.parent
    parent.mkdir(parents=True, exist_ok=True)
    _private_directory(parent, label="输出根父目录")
    digest = replay._sha256_bytes(str(resolved).encode("utf-8"))[:24]
    return parent / f".dcar-full-history-cache-output-{digest}.claim"


def _safe_resolve_input(path: Path, *, label: str) -> Path:
    lexical = Path(os.path.abspath(path))
    if os.path.lexists(lexical) and lexical.is_symlink():
        raise BatchReplayError(f"{label}不得是符号链接：{lexical}")
    return lexical.resolve()


def _assert_run_root_isolated(
    run_root: Path,
    *,
    db_path: Path,
    derived_raw_root: Path,
    media_root: Path,
) -> None:
    root = run_root.resolve()
    other_roots = (derived_raw_root.resolve(), media_root.resolve())
    if any(
        root == other
        or root.is_relative_to(other)
        or other.is_relative_to(root)
        for other in other_roots
    ):
        raise BatchReplayError("批处理运行目录不得与数据输出目录相同或相互包含")
    database = db_path.resolve()
    if root == database or database.is_relative_to(root):
        raise BatchReplayError("批处理运行目录不得包含数据库副本")
    canonical_roots = (
        replay.DEFAULT_RAW_ROOT.resolve(),
        replay.DEFAULT_MEDIA_ROOT.resolve(),
    )
    if any(
        root == canonical
        or root.is_relative_to(canonical)
        or canonical.is_relative_to(root)
        for canonical in canonical_roots
    ):
        raise BatchReplayError("批处理运行目录不得指向或包含正式缓存根")


@contextmanager
def _exclusive_claim(path: Path) -> Iterator[None]:
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise BatchReplayError("批处理 claim 不是私有单链接普通文件")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise BatchReplayError("已有另一个批处理进程持有 claim") from exc
        path_metadata = path.lstat()
        if (metadata.st_dev, metadata.st_ino) != (
            path_metadata.st_dev,
            path_metadata.st_ino,
        ):
            raise BatchReplayError("批处理 claim 在加锁期间被替换")
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _code_contract() -> Mapping[str, Any]:
    paths = {
        "batch_controller": Path(__file__).resolve(),
        "capture": Path(capture_module.__file__).resolve(),
        "cache_replay": Path(replay.__file__).resolve(),
        "identity": Path(identity_module.__file__).resolve(),
        "providers": Path(replay.providers.__file__).resolve(),
        "media": Path(media_module.__file__).resolve(),
        "operations": Path(operations_module.__file__).resolve(),
        "storage": Path(storage_module.__file__).resolve(),
    }
    return {
        name: {
            "path": str(path),
            "bytes": _private_file(path, label=f"{name} 源码").st_size,
            "sha256": replay._file_sha256(path),
        }
        for name, path in sorted(paths.items())
    }


def _materialize_code_snapshots(
    run_root: Path, *, code: Mapping[str, Any]
) -> Mapping[str, Any]:
    root = run_root / "code-snapshots"
    root.mkdir(exist_ok=True)
    _private_directory(root, label="代码快照目录")
    snapshots: dict[str, Any] = {}
    for name, evidence_value in sorted(code.items()):
        evidence = dict(evidence_value)
        source = Path(str(evidence["path"]))
        body = source.read_bytes()
        if (
            len(body) != int(evidence["bytes"])
            or replay._sha256_bytes(body) != evidence["sha256"]
        ):
            raise BatchReplayError(f"代码在快照期间发生漂移：{name}")
        target = root / f"{name}.py"
        digest = _atomic_bytes(target, body, mode=0o444)
        target.chmod(0o444)
        snapshots[name] = {
            "path": str(target),
            "bytes": len(body),
            "sha256": digest,
        }
    _fsync_directory(root)
    return snapshots


def _validate_code_snapshots(
    value: Any, *, run_root: Path, code: Mapping[str, Any]
) -> None:
    if not isinstance(value, Mapping) or not value:
        raise BatchReplayError("批处理合同缺少代码快照")
    if set(value) != set(code):
        raise BatchReplayError("批处理代码快照集合漂移")
    expected_parent = (run_root / "code-snapshots").resolve()
    for name, evidence_value in value.items():
        if not isinstance(evidence_value, Mapping):
            raise BatchReplayError(f"代码快照证据非法：{name}")
        path = Path(str(evidence_value.get("path") or ""))
        if path.parent.resolve() != expected_parent or path.name != f"{name}.py":
            raise BatchReplayError(f"代码快照路径越界：{name}")
        metadata = _private_file(path, label=f"{name} 代码快照")
        if (
            metadata.st_size != int(evidence_value.get("bytes") or -1)
            or replay._file_sha256(path) != evidence_value.get("sha256")
            or int(evidence_value.get("bytes") or -1)
            != int(code[name]["bytes"])
            or evidence_value.get("sha256") != code[name]["sha256"]
            or metadata.st_mode & 0o222
        ):
            raise BatchReplayError(f"代码快照漂移：{name}")


def _json_scalar(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"bytes_hex": value.hex()}
    return value


def _rows_hash(
    connection: sqlite3.Connection,
    query: str,
    parameters: Sequence[Any] = (),
) -> tuple[int, str]:
    digest = hashlib.sha256()
    count = 0
    for row in connection.execute(query, tuple(parameters)):
        digest.update(
            json.dumps(
                [_json_scalar(value) for value in row],
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        )
        digest.update(b"\n")
        count += 1
    return count, digest.hexdigest()


def _table_hash(connection: sqlite3.Connection, table: str) -> tuple[int, str]:
    info = connection.execute(f'PRAGMA table_info("{table}")').fetchall()
    columns = [str(row[1]) for row in info]
    primary = [str(row[1]) for row in info if int(row[5]) > 0]
    order = primary or columns
    suffix = ""
    if order:
        suffix = " ORDER BY " + ",".join(f'"{column}"' for column in order)
    return _rows_hash(connection, f'SELECT * FROM "{table}"{suffix}')


def _immutable_connection(db_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(
        f"file:{db_path.resolve()}?mode=ro&immutable=1", uri=True
    )
    connection.row_factory = sqlite3.Row
    return connection


def _database_file_snapshot(db_path: Path) -> Mapping[str, Any]:
    metadata = _private_file(db_path.resolve(), label="批处理数据库")
    sidecars = [
        str(Path(f"{db_path.resolve()}{suffix}"))
        for suffix in ("-wal", "-shm", "-journal")
        if os.path.lexists(Path(f"{db_path.resolve()}{suffix}"))
    ]
    if sidecars:
        raise BatchReplayError("批处理数据库存在 sidecar：" + ",".join(sidecars))
    return {
        "path": str(db_path.resolve()),
        "bytes": metadata.st_size,
        "sha256": replay._file_sha256(db_path.resolve()),
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "nlink": metadata.st_nlink,
    }


def _validate_database_identity(
    db_path: Path, *, expected: Mapping[str, Any]
) -> None:
    metadata = _private_file(db_path.resolve(), label="批处理数据库")
    if (
        str(db_path.resolve()) != str(expected.get("path"))
        or metadata.st_dev != int(expected.get("device") or -1)
        or metadata.st_ino != int(expected.get("inode") or -1)
        or metadata.st_nlink != 1
    ):
        raise BatchReplayError("批处理数据库文件身份漂移")


def _hash_values(digest: Any, values: Sequence[Any]) -> None:
    digest.update(
        json.dumps(
            [_json_scalar(value) for value in values],
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    )
    digest.update(b"\n")


def _allowed_non_target_hashes(
    connection: sqlite3.Connection, *, target_ids: set[int]
) -> Mapping[str, list[Any]]:
    output: dict[str, list[Any]] = {}
    queries = {
        "evidence_artifacts": (
            "SELECT * FROM evidence_artifacts ORDER BY id",
            False,
        ),
        "fetch_attempts": (
            """
            SELECT fs.content_id AS scope_content_id,fa.*
            FROM fetch_attempts fa
            JOIN fetch_slots fs ON fs.id=fa.slot_id
            ORDER BY fa.id
            """,
            True,
        ),
        "fetch_slots": ("SELECT * FROM fetch_slots ORDER BY id", False),
        "provider_raw_responses": (
            "SELECT * FROM provider_raw_responses ORDER BY id",
            False,
        ),
    }
    for table, (query, prefixed_scope) in queries.items():
        digest = hashlib.sha256()
        count = 0
        for row in connection.execute(query):
            scope_value = row[0] if prefixed_scope else row["content_id"]
            if scope_value is not None and int(scope_value) in target_ids:
                continue
            values = list(row)[1:] if prefixed_scope else list(row)
            _hash_values(digest, values)
            count += 1
        output[table] = [count, digest.hexdigest()]
    return output


TARGET_CONTRACT_FIELDS = (
    "content_id",
    "platform",
    "expected_detail_operation",
    "source_discovery_raw_id",
    "source_discovery_operation",
    "source_discovery_sha256",
    "source_discovery_captured_at",
    "expected_detail_data_sha256",
    "expected_detail_raw_sha256",
    "expected_detail_raw_bytes",
)


def _chunks(values: Sequence[int], size: int = 500) -> Iterator[list[int]]:
    for offset in range(0, len(values), size):
        yield list(values[offset : offset + size])


def _expected_detail_data(
    candidate: replay.Candidate, *, source_captured_at: str
) -> Mapping[str, Any]:
    """Mirror the exact zero-provider detail payload written by providers."""

    item = candidate.item
    media_urls = [
        str(value)
        for value in item.get("media_urls") or []
        if isinstance(value, str) and media_module.is_supported_media_url(value)
    ]
    return {
        "title": str(item.get("title") or ""),
        "body": str(item.get("body") or ""),
        "published_at": item.get("published_at"),
        "account_uid": str(item.get("account_uid") or candidate.account_uid),
        "account_name": str(item.get("account_name") or ""),
        "content_type": str(item.get("content_type") or "unknown"),
        "media_urls": media_urls,
        "_evidence_captured_at": source_captured_at,
    }


def _expected_detail_raw_body(
    candidate: replay.Candidate,
    *,
    source_sha256: str,
    source_captured_at: str,
) -> Mapping[str, Any]:
    return {
        "stage": "detail",
        "data": _expected_detail_data(
            candidate, source_captured_at=source_captured_at
        ),
        "derived_from_operation": candidate.operation,
        "source_raw_response_id": candidate.raw_response_id,
        "source_sha256": source_sha256,
        "source_captured_at": source_captured_at,
    }


def _build_target_contract(
    plan: replay.ReplayPlan, *, db_path: Path
) -> Mapping[str, Any]:
    if int(plan.summary["history"]["media_only_ready"]) != 0:
        raise BatchReplayError("批处理首次合同不允许已有成功详情的 media_only 目标")
    candidates = sorted(plan.candidates, key=lambda item: item.content_id)
    target = {item.content_id for item in candidates}
    if len(target) != len(candidates):
        raise BatchReplayError("批处理首次目标内容 ID 重复")
    raw_ids = sorted({item.raw_response_id for item in candidates})
    raw_evidence: dict[int, tuple[str, str, str]] = {}
    connection = _immutable_connection(db_path)
    try:
        for row in connection.execute(
            """
            SELECT content_id FROM fetch_slots
            WHERE content_id IS NOT NULL AND stage='detail' AND window_key='lifetime'
            ORDER BY content_id
            """
        ):
            if int(row["content_id"]) in target:
                raise BatchReplayError("批处理首次目标已存在详情槽，必须单独审计")
        for batch in _chunks(raw_ids):
            placeholders = ",".join("?" for _ in batch)
            for row in connection.execute(
                f"""
                SELECT id,operation,sha256,source,captured_at
                FROM provider_raw_responses
                WHERE id IN ({placeholders}) ORDER BY id
                """,
                tuple(batch),
            ):
                if str(row["source"]) != "live_applied":
                    raise BatchReplayError("来源 discovery raw 不是 live_applied")
                raw_evidence[int(row["id"])] = (
                    str(row["operation"]),
                    str(row["sha256"]),
                    str(row["captured_at"]),
                )
    finally:
        connection.close()
    if set(raw_evidence) != set(raw_ids):
        raise BatchReplayError("批处理来源 discovery raw 证据不完整")
    operation_by_platform = {
        "douyin": "douyin_video_detail",
        "xiaohongshu": "xiaohongshu_note_detail",
    }
    rows: list[list[Any]] = []
    for candidate in candidates:
        expected_operation = operation_by_platform.get(candidate.platform)
        source_operation, source_sha, source_captured_at = raw_evidence[
            candidate.raw_response_id
        ]
        if expected_operation is None or source_operation != candidate.operation:
            raise BatchReplayError(f"批处理来源平台/operation 漂移：{candidate.content_id}")
        expected_data = _expected_detail_data(
            candidate, source_captured_at=source_captured_at
        )
        expected_raw = capture_module.canonical_json_bytes(
            _expected_detail_raw_body(
                candidate,
                source_sha256=source_sha,
                source_captured_at=source_captured_at,
            )
        )
        rows.append(
            [
                candidate.content_id,
                candidate.platform,
                expected_operation,
                candidate.raw_response_id,
                source_operation,
                source_sha,
                source_captured_at,
                replay._object_sha256(expected_data),
                hashlib.sha256(expected_raw).hexdigest(),
                len(expected_raw),
            ]
        )
    return {
        "fields": list(TARGET_CONTRACT_FIELDS),
        "rows": rows,
        "rows_sha256": replay._object_sha256(rows),
    }


def _target_contract_map(contract: Mapping[str, Any]) -> Mapping[int, Mapping[str, Any]]:
    value = contract.get("target_contract")
    if not isinstance(value, Mapping):
        raise BatchReplayError("批处理合同缺少逐内容来源合同")
    fields = list(value.get("fields") or [])
    rows = list(value.get("rows") or [])
    if (
        fields != list(TARGET_CONTRACT_FIELDS)
        or replay._object_sha256(rows) != value.get("rows_sha256")
    ):
        raise BatchReplayError("批处理逐内容来源合同漂移")
    output: dict[int, Mapping[str, Any]] = {}
    previous = 0
    for raw_row in rows:
        if not isinstance(raw_row, list) or len(raw_row) != len(fields):
            raise BatchReplayError("批处理逐内容来源行非法")
        row = dict(zip(fields, raw_row))
        content_id = int(row["content_id"])
        if content_id <= previous or content_id in output:
            raise BatchReplayError("批处理逐内容来源行未严格按 ID 排序")
        previous = content_id
        output[content_id] = row
    expected = [int(value) for value in contract.get("target_ids", [])]
    if list(output) != expected:
        raise BatchReplayError("批处理逐内容来源集合与目标集合不一致")
    return output


def _allowed_prefix_hashes(
    connection: sqlite3.Connection, *, max_ids: Mapping[str, Any]
) -> Mapping[str, list[Any]]:
    return {
        table: list(
            _rows_hash(
                connection,
                f'SELECT * FROM "{table}" WHERE id<=? ORDER BY id',
                (int(max_ids[table]),),
            )
        )
        for table in APPEND_CHANGED_TABLES
    }


def _database_baseline(
    db_path: Path, *, target_ids: Sequence[int]
) -> Mapping[str, Any]:
    target = set(int(value) for value in target_ids)
    connection = _immutable_connection(db_path)
    try:
        quick = [str(row[0]) for row in connection.execute("PRAGMA quick_check")]
        foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()
        if quick != ["ok"] or foreign_keys:
            raise BatchReplayError("批处理数据库健康检查失败")
        tables = [
            str(row[0])
            for row in connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
                """
            )
        ]
        schema = _rows_hash(
            connection,
            """
            SELECT type,name,tbl_name,sql FROM sqlite_master
            WHERE name NOT LIKE 'sqlite_%'
            ORDER BY type,name
            """,
        )
        protected = {
            table: list(_table_hash(connection, table))
            for table in tables
            if table not in ALLOWED_CHANGED_TABLES
        }
        info = connection.execute('PRAGMA table_info("content_items")').fetchall()
        stable_columns = [
            str(row[1])
            for row in info
            if str(row[1]) not in {"title", "body", "updated_at"}
        ]
        stable_query = "SELECT " + ",".join(
            f'"{column}"' for column in stable_columns
        ) + " FROM content_items ORDER BY id"
        content_stable = _rows_hash(connection, stable_query)
        non_target_digest = hashlib.sha256()
        non_target_count = 0
        for row in connection.execute("SELECT * FROM content_items ORDER BY id"):
            if int(row["id"]) in target:
                continue
            non_target_digest.update(
                json.dumps(
                    [_json_scalar(value) for value in row],
                    ensure_ascii=False,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode("utf-8")
            )
            non_target_digest.update(b"\n")
            non_target_count += 1
        sequences = {
            str(row["name"]): int(row["seq"])
            for row in connection.execute("SELECT name,seq FROM sqlite_sequence")
            if str(row["name"]) not in ALLOWED_CHANGED_TABLES
        }
        allowed_counts = {
            table: int(
                connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            )
            for table in sorted(ALLOWED_CHANGED_TABLES)
        }
        allowed_max_ids = {
            table: int(
                connection.execute(
                    f'SELECT COALESCE(MAX(id),0) FROM "{table}"'
                ).fetchone()[0]
            )
            for table in APPEND_CHANGED_TABLES
        }
        allowed_prefix = _allowed_prefix_hashes(
            connection, max_ids=allowed_max_ids
        )
        allowed_non_target = _allowed_non_target_hashes(
            connection, target_ids=target
        )
    finally:
        connection.close()
    return {
        "schema": list(schema),
        "protected_tables": protected,
        "protected_sequences": sequences,
        "content_stable": list(content_stable),
        "non_target_content_full": [
            non_target_count,
            non_target_digest.hexdigest(),
        ],
        "allowed_table_counts": allowed_counts,
        "allowed_max_ids": allowed_max_ids,
        "allowed_prefix": allowed_prefix,
        "allowed_non_target": allowed_non_target,
    }


def _target_text_hash(
    plan: replay.ReplayPlan, *, db_path: Path
) -> Mapping[str, Any]:
    incoming = {item.content_id: item.item for item in plan.candidates}
    connection = _immutable_connection(db_path)
    rows: list[list[Any]] = []
    try:
        for content_id in sorted(incoming):
            current = connection.execute(
                "SELECT title,body FROM content_items WHERE id=?", (content_id,)
            ).fetchone()
            if current is None:
                raise BatchReplayError(f"目标内容不存在：{content_id}")
            item = incoming[content_id]
            title = str(item.get("title") or current["title"] or "")
            body = str(item.get("body") or current["body"] or "")
            rows.append([content_id, title, body])
    finally:
        connection.close()
    return {"rows": len(rows), "sha256": replay._object_sha256(rows)}


def _current_target_text_hash(
    db_path: Path, *, target_ids: Sequence[int]
) -> Mapping[str, Any]:
    connection = _immutable_connection(db_path)
    rows: list[list[Any]] = []
    try:
        for content_id in sorted(int(value) for value in target_ids):
            row = connection.execute(
                "SELECT title,body FROM content_items WHERE id=?", (content_id,)
            ).fetchone()
            if row is None:
                raise BatchReplayError(f"目标内容不存在：{content_id}")
            rows.append([content_id, str(row["title"] or ""), str(row["body"] or "")])
    finally:
        connection.close()
    return {"rows": len(rows), "sha256": replay._object_sha256(rows)}


def _critical_snapshot(db_path: Path, *, target_ids: Sequence[int]) -> Mapping[str, Any]:
    target = set(int(value) for value in target_ids)
    connection = _immutable_connection(db_path)
    try:
        quick = [str(row[0]) for row in connection.execute("PRAGMA quick_check")]
        foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()
        if quick != ["ok"] or foreign_keys:
            raise BatchReplayError("批处理数据库健康检查失败")
        schema = _rows_hash(
            connection,
            """
            SELECT type,name,tbl_name,sql FROM sqlite_master
            WHERE name NOT LIKE 'sqlite_%'
            ORDER BY type,name
            """,
        )
        tables = [
            str(row[0])
            for row in connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
                """
            )
        ]
        protected = {
            table: list(_table_hash(connection, table))
            for table in tables
            if table not in ALLOWED_CHANGED_TABLES
        }
        sequences = {
            str(row["name"]): int(row["seq"])
            for row in connection.execute("SELECT name,seq FROM sqlite_sequence")
            if str(row["name"]) not in ALLOWED_CHANGED_TABLES
        }
        info = connection.execute('PRAGMA table_info("content_items")').fetchall()
        stable_columns = [
            str(row[1])
            for row in info
            if str(row[1]) not in {"title", "body", "updated_at"}
        ]
        stable_query = "SELECT " + ",".join(
            f'"{column}"' for column in stable_columns
        ) + " FROM content_items ORDER BY id"
        content_stable = _rows_hash(connection, stable_query)
        non_target_digest = hashlib.sha256()
        non_target_count = 0
        for row in connection.execute("SELECT * FROM content_items ORDER BY id"):
            if int(row["id"]) in target:
                continue
            non_target_digest.update(
                json.dumps(
                    [_json_scalar(value) for value in row],
                    ensure_ascii=False,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode("utf-8")
            )
            non_target_digest.update(b"\n")
            non_target_count += 1
        allowed_non_target = _allowed_non_target_hashes(
            connection, target_ids=target
        )
    finally:
        connection.close()
    return {
        "schema": list(schema),
        "protected_tables": protected,
        "protected_sequences": sequences,
        "content_stable": list(content_stable),
        "non_target_content_full": [
            non_target_count,
            non_target_digest.hexdigest(),
        ],
        "allowed_non_target": allowed_non_target,
    }


def _disk_gate(path: Path, *, min_free_bytes: int) -> Mapping[str, int]:
    usage = shutil.disk_usage(path)
    if usage.free < min_free_bytes:
        raise BatchReplayError(
            f"磁盘剩余空间不足：free={usage.free}, required={min_free_bytes}"
        )
    return {"total": usage.total, "used": usage.used, "free": usage.free}


def _disk_anchor(path: Path) -> Path:
    candidate = path.resolve()
    while not os.path.lexists(candidate):
        if candidate == candidate.parent:
            raise BatchReplayError(f"找不到磁盘门禁父目录：{path}")
        candidate = candidate.parent
    metadata = candidate.lstat()
    if stat.S_ISREG(metadata.st_mode):
        candidate = candidate.parent
    if not stat.S_ISDIR(candidate.lstat().st_mode) or candidate.is_symlink():
        raise BatchReplayError(f"磁盘门禁路径不是安全目录：{candidate}")
    return candidate


def _disk_gates(
    values: Mapping[str, Path], *, min_free_bytes: int
) -> Mapping[str, Mapping[str, Any]]:
    output: dict[str, Mapping[str, Any]] = {}
    checked_devices: dict[int, Mapping[str, Any]] = {}
    for label, path in sorted(values.items()):
        anchor = _disk_anchor(path)
        device = anchor.stat().st_dev
        evidence = checked_devices.get(device)
        if evidence is None:
            usage = _disk_gate(anchor, min_free_bytes=min_free_bytes)
            evidence = {"anchor": str(anchor), "device": device, **usage}
            checked_devices[device] = evidence
        output[label] = evidence
    return output


def _require_empty_output_root(path: Path, *, label: str) -> None:
    if not os.path.lexists(path):
        return
    _private_directory(path, label=label)
    if any(path.iterdir()):
        raise BatchReplayError(f"{label}首次运行前必须为空：{path}")


def _validate_run_root_inventory(paths: BatchPaths) -> None:
    allowed = {
        paths.batches.name,
        paths.claim.name,
        paths.contract.name,
        paths.completion.name,
        "code-snapshots",
    }
    recoverable_temps = {
        f".{paths.contract.name}.tmp",
        f".{paths.completion.name}.tmp",
    }
    for candidate in paths.run_root.iterdir():
        if candidate.name in allowed:
            continue
        if candidate.name in recoverable_temps:
            _private_file(candidate, label="运行记录原子写临时文件")
            candidate.unlink()
            _fsync_directory(paths.run_root)
            continue
        raise BatchReplayError(f"批处理运行目录存在未知文件：{candidate.name}")
    snapshots = paths.run_root / "code-snapshots"
    if not os.path.lexists(snapshots):
        return
    _private_directory(snapshots, label="代码快照目录")
    names = set(_code_contract())
    allowed_snapshot_names = {f"{name}.py" for name in names}
    recoverable_snapshot_temps = {f".{name}.py.tmp" for name in names}
    for candidate in snapshots.iterdir():
        if candidate.name in allowed_snapshot_names:
            _private_file(candidate, label="代码快照")
            continue
        if candidate.name in recoverable_snapshot_temps:
            _private_file(candidate, label="代码快照原子写临时文件")
            candidate.unlink()
            _fsync_directory(snapshots)
            continue
        raise BatchReplayError(f"代码快照目录存在未知文件：{candidate.name}")


def _resolve_stored_path(stored: str) -> Path:
    candidate = Path(stored)
    if candidate.is_absolute():
        return candidate.resolve()
    return (replay.PROJECT_ROOT / candidate).resolve()


def _validate_batch_artifacts(
    db_path: Path,
    *,
    contract: Mapping[str, Any],
    content_ids: Sequence[int],
    derived_raw_root: Path,
    media_root: Path,
    allow_live_raw_content_ids: set[int] | None = None,
) -> Mapping[str, Any]:
    target_evidence = _target_contract_map(contract)
    allow_live_raw = {
        int(value) for value in (allow_live_raw_content_ids or set())
    }
    requested_content_ids = {int(value) for value in content_ids}
    if not allow_live_raw.issubset(requested_content_ids):
        raise BatchReplayError("live raw 临时授权超出本次制品集合")
    rows: list[list[Any]] = []
    connection = _immutable_connection(db_path)
    try:
        for content_id in sorted(int(value) for value in content_ids):
            detail = connection.execute(
                """
                SELECT fs.status,fa.billed,fa.amount,pr.id raw_id,pr.source,
                       pr.operation,pr.local_path raw_path,pr.sha256 raw_sha,
                       pr.byte_size raw_bytes
                FROM fetch_slots fs
                JOIN fetch_attempts fa ON fa.slot_id=fs.id
                JOIN provider_raw_responses pr ON pr.fetch_attempt_id=fa.id
                WHERE fs.content_id=? AND fs.stage='detail'
                  AND fs.window_key='lifetime' AND fs.status='succeeded'
                ORDER BY fa.attempt_number DESC,pr.id DESC LIMIT 1
                """,
                (content_id,),
            ).fetchone()
            artifact = connection.execute(
                """
                SELECT ea.id,ea.local_path,ea.sha256,ea.byte_size,
                       ea.metadata_json,ea.processor_version,c.link_id
                FROM evidence_artifacts ea
                JOIN content_items c ON c.id=ea.content_id
                WHERE ea.content_id=? AND ea.artifact_type='media_source'
                  AND ea.status='available'
                ORDER BY ea.id DESC LIMIT 1
                """,
                (content_id,),
            ).fetchone()
            if detail is None or artifact is None:
                raise BatchReplayError(f"批次内容缺少详情或媒体证据：{content_id}")
            if (
                int(detail["billed"] or 0) != 0
                or float(detail["amount"] or 0.0) != 0.0
                or (
                    str(detail["source"]) != "derived_applied"
                    and not (
                        content_id in allow_live_raw
                        and str(detail["source"]) == "live"
                    )
                )
            ):
                raise BatchReplayError(f"批次内容不是零付费派生响应：{content_id}")
            raw_path = _resolve_stored_path(str(detail["raw_path"]))
            artifact_path = _resolve_stored_path(str(artifact["local_path"]))
            evidence = target_evidence.get(content_id)
            if evidence is None:
                raise BatchReplayError(f"批次内容不在冻结来源合同：{content_id}")
            expected_operation = str(evidence["expected_detail_operation"])
            if not raw_path.is_relative_to(derived_raw_root.resolve()):
                raise BatchReplayError(f"派生 raw 路径越界：{raw_path}")
            if not artifact_path.is_relative_to(media_root.resolve()):
                raise BatchReplayError(f"媒体证据路径越界：{artifact_path}")
            raw_stat = _private_file(raw_path, label="派生 raw")
            artifact_stat = _private_file(artifact_path, label="媒体证据")
            if (
                raw_stat.st_size != int(detail["raw_bytes"])
                or replay._file_sha256(raw_path) != str(detail["raw_sha"])
                or str(detail["raw_sha"])
                != str(evidence["expected_detail_raw_sha256"])
                or int(detail["raw_bytes"])
                != int(evidence["expected_detail_raw_bytes"])
                or artifact_stat.st_size != int(artifact["byte_size"])
                or replay._file_sha256(artifact_path) != str(artifact["sha256"])
            ):
                raise BatchReplayError(f"批次证据文件哈希或字节数漂移：{content_id}")
            raw_filename_suffix = f"-{str(detail['raw_sha'])[:12]}.json"
            if (
                str(detail["operation"]) != expected_operation
                or raw_path.parent.name != expected_operation
                or raw_path.parent.parent.name != str(content_id)
                or raw_path.parent.parent.parent
                != derived_raw_root.resolve() / "tikhub"
                or not raw_path.name.startswith("attempt-")
                or not raw_path.name.endswith(raw_filename_suffix)
            ):
                raise BatchReplayError(f"派生 raw 路径形状或 operation 漂移：{content_id}")
            try:
                raw_body = json.loads(raw_path.read_bytes())
                artifact_body = json.loads(artifact_path.read_bytes())
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise BatchReplayError(f"批次证据文件不是合法 JSON：{content_id}") from exc
            if (
                not isinstance(raw_body, Mapping)
                or not isinstance(raw_body.get("data"), Mapping)
                or not isinstance(artifact_body, Mapping)
                or set(raw_body)
                != {
                    "stage",
                    "data",
                    "derived_from_operation",
                    "source_raw_response_id",
                    "source_sha256",
                    "source_captured_at",
                }
                or raw_body.get("stage") != "detail"
                or int(raw_body.get("source_raw_response_id") or 0)
                != int(evidence["source_discovery_raw_id"])
                or str(raw_body.get("source_sha256") or "")
                != str(evidence["source_discovery_sha256"])
                or str(raw_body.get("source_captured_at") or "")
                != str(evidence["source_discovery_captured_at"])
                or str(raw_body.get("derived_from_operation") or "")
                != str(evidence["source_discovery_operation"])
                or replay._object_sha256(raw_body["data"])
                != str(evidence["expected_detail_data_sha256"])
            ):
                raise BatchReplayError(f"派生 raw 未绑定冻结 discovery 来源：{content_id}")
            metadata = json.loads(str(artifact["metadata_json"] or "{}"))
            urls = artifact_body.get("urls")
            raw_data = raw_body["data"]
            media_kind = (
                "video" if str(raw_data.get("content_type")) == "video" else "image"
            )
            expected_urls, expected_source_sha = media_module._media_source_identity(
                media_kind,
                [
                    str(url)
                    for url in raw_data.get("media_urls") or []
                    if isinstance(url, str)
                ],
            )
            expected_artifact_name = (
                f"source-{int(detail['raw_id'])}-"
                f"{str(metadata.get('source_sha256') or '')[:12]}.json"
            )
            captured_at = str(artifact_body.get("captured_at") or "")
            try:
                parsed_captured_at = datetime.fromisoformat(
                    captured_at.replace("Z", "+00:00")
                )
            except ValueError as exc:
                raise BatchReplayError(
                    f"媒体证据 captured_at 非法：{content_id}"
                ) from exc
            if (
                set(metadata)
                != {"media_kind", "source_count", "source_sha256", "raw_response_id"}
                or int(metadata.get("source_count") or 0) != len(expected_urls)
                or int(metadata.get("raw_response_id") or 0)
                != int(detail["raw_id"])
                or str(artifact["processor_version"])
                != "provider-media-source-v8.1"
                or artifact_path
                != media_root.resolve()
                / str(artifact["link_id"])
                / "sources"
                / expected_artifact_name
                or artifact_body.get("schema_version")
                != "provider-media-source-v8.1"
                or set(artifact_body)
                != {
                    "schema_version",
                    "media_kind",
                    "urls",
                    "source_sha256",
                    "raw_response_id",
                    "captured_at",
                }
                or parsed_captured_at.tzinfo is None
                or int(artifact_body.get("raw_response_id") or 0)
                != int(detail["raw_id"])
                or str(artifact_body.get("source_sha256") or "")
                != str(metadata.get("source_sha256") or "")
                or str(metadata.get("source_sha256") or "")
                != expected_source_sha
                or str(metadata.get("media_kind") or "") != media_kind
                or artifact_body.get("media_kind") != media_kind
                or urls != expected_urls
                or not isinstance(urls, list)
                or not urls
                or any(
                    not isinstance(url, str)
                    or not media_module.is_supported_media_url(url)
                    for url in urls
                )
            ):
                raise BatchReplayError(f"媒体证据未绑定详情 raw：{content_id}")
            rows.append(
                [
                    content_id,
                    int(detail["raw_id"]),
                    str(detail["raw_sha"]),
                    int(artifact["id"]),
                    str(artifact["sha256"]),
                ]
            )
    finally:
        connection.close()
    return {
        "contents": len(rows),
        "row_fields": [
            "content_id",
            "detail_raw_response_id",
            "detail_raw_sha256",
            "media_artifact_id",
            "media_artifact_sha256",
        ],
        "rows_sha256": replay._object_sha256(rows),
    }


def _validate_allowed_append_scope(
    contract: Mapping[str, Any],
    *,
    db_path: Path,
    derived_raw_root: Path,
    media_root: Path,
    require_complete: bool = False,
    authorized_content_ids: set[int] | None = None,
    subset_only: bool = False,
    allow_unapplied_raw_content_ids: set[int] | None = None,
) -> Mapping[str, Any]:
    target_evidence = _target_contract_map(contract)
    target = set(target_evidence)
    authorized = (
        target
        if authorized_content_ids is None
        else {int(value) for value in authorized_content_ids}
    )
    if not authorized.issubset(target):
        raise BatchReplayError("白名单增量授权内容超出冻结目标")
    allow_unapplied = {
        int(value) for value in (allow_unapplied_raw_content_ids or set())
    }
    if not allow_unapplied.issubset(authorized):
        raise BatchReplayError("临时未应用 raw 授权超出当前批次")
    baseline = contract["baseline"]
    baseline_counts = baseline["allowed_table_counts"]
    baseline_max_ids = baseline["allowed_max_ids"]
    rows: dict[str, list[list[Any]]] = {
        table: [] for table in APPEND_CHANGED_TABLES
    }
    slot_by_id: dict[int, sqlite3.Row] = {}
    slot_by_content: dict[int, int] = {}
    attempts_by_slot: dict[int, list[sqlite3.Row]] = {}
    successful_attempts: dict[int, int] = {}
    raw_by_id: dict[int, sqlite3.Row] = {}
    raw_by_content: dict[int, int] = {}
    artifact_by_content: dict[int, int] = {}
    connection = _immutable_connection(db_path)
    try:
        current_counts = {
            table: int(
                connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            )
            for table in APPEND_CHANGED_TABLES
        }
        for row in connection.execute(
            """
            SELECT fs.*,ci.platform
            FROM fetch_slots fs
            LEFT JOIN content_items ci ON ci.id=fs.content_id
            WHERE fs.id>? ORDER BY fs.id
            """,
            (int(baseline_max_ids["fetch_slots"]),),
        ):
            content_id = int(row["content_id"] or 0)
            slot_id = int(row["id"])
            if content_id not in authorized:
                if subset_only and content_id in target:
                    continue
                raise BatchReplayError(f"新增 fetch_slot 超出目标范围：{row['id']}")
            if (
                row["account_id"] is not None
                or str(row["stage"]) != "detail"
                or str(row["window_key"]) != "lifetime"
                or str(row["provider"]) != "TikHub"
                or str(row["adapter_version"])
                != "tikhub-discovery-derived-v8.1"
                or str(row["platform"])
                != str(target_evidence[content_id]["platform"])
                or str(row["status"]) not in {"succeeded", "retryable_failed"}
                or row["started_at"] is None
                or row["finished_at"] is None
                or content_id in slot_by_content
            ):
                raise BatchReplayError(f"新增 fetch_slot 超出目标范围：{row['id']}")
            slot_by_id[slot_id] = row
            slot_by_content[content_id] = slot_id
            rows["fetch_slots"].append(
                [slot_id, content_id, str(row["status"]), str(row["platform"])]
            )

        for row in connection.execute(
            """
            SELECT fa.*,fs.content_id,fs.stage,fs.window_key,fs.provider,
                   fs.adapter_version
            FROM fetch_attempts fa
            JOIN fetch_slots fs ON fs.id=fa.slot_id
            WHERE fa.id>? ORDER BY fa.id
            """,
            (int(baseline_max_ids["fetch_attempts"]),),
        ):
            content_id = int(row["content_id"] or 0)
            attempt_id = int(row["id"])
            slot_id = int(row["slot_id"])
            error_code = str(row["error_code"] or "")
            succeeded = error_code == ""
            if content_id not in authorized:
                if subset_only and content_id in target:
                    continue
                raise BatchReplayError(
                    f"新增 fetch_attempt 超出零付费目标范围：{row['id']}"
                )
            if (
                slot_id not in slot_by_id
                or str(row["stage"]) != "detail"
                or str(row["window_key"]) != "lifetime"
                or str(row["provider"]) != "TikHub"
                or str(row["adapter_version"])
                != "tikhub-discovery-derived-v8.1"
                or int(row["billed"] or 0) != 0
                or float(row["amount"] or 0.0) != 0.0
                or str(row["currency"] or "") != ""
                or row["response_finished_at"] is None
                or (
                    succeeded
                    and (int(row["http_status"] or 0) != 200 or row["error_message"])
                )
                or (
                    not succeeded
                    and (
                        error_code
                        not in {"batch_interrupted", "unhandled_adapter_error"}
                        or row["http_status"] is not None
                        or not str(row["error_message"] or "")
                    )
                )
            ):
                raise BatchReplayError(f"新增 fetch_attempt 超出零付费目标范围：{row['id']}")
            attempts_by_slot.setdefault(slot_id, []).append(row)
            if succeeded:
                if content_id in successful_attempts:
                    raise BatchReplayError(f"目标内容出现多个成功详情尝试：{content_id}")
                successful_attempts[content_id] = attempt_id
            rows["fetch_attempts"].append(
                [
                    attempt_id,
                    content_id,
                    slot_id,
                    int(row["attempt_number"]),
                    error_code,
                ]
            )

        for content_id, slot_id in slot_by_content.items():
            slot = slot_by_id[slot_id]
            slot_attempts = attempts_by_slot.get(slot_id, [])
            numbers = sorted(int(row["attempt_number"]) for row in slot_attempts)
            latest = max(
                slot_attempts,
                key=lambda row: int(row["attempt_number"]),
                default=None,
            )
            latest_error = str(latest["error_code"] or "") if latest else ""
            if (
                numbers != list(range(1, len(numbers) + 1))
                or int(slot["attempt_count"]) != len(numbers)
                or latest is None
                or (
                    str(slot["status"]) == "succeeded"
                    and (
                        content_id not in successful_attempts
                        or int(latest["id"]) != successful_attempts[content_id]
                        or latest_error
                        or slot["last_error_code"] is not None
                        or slot["last_error_message"] is not None
                    )
                )
                or (
                    str(slot["status"]) == "retryable_failed"
                    and (
                        content_id in successful_attempts
                        or not latest_error
                        or str(slot["last_error_code"] or "") != latest_error
                    )
                )
            ):
                raise BatchReplayError(f"目标详情槽与尝试链不一致：{content_id}")

        for row in connection.execute(
            """
            SELECT pr.*
            FROM provider_raw_responses pr
            WHERE pr.id>? ORDER BY pr.id
            """,
            (int(baseline_max_ids["provider_raw_responses"]),),
        ):
            content_id = int(row["content_id"] or 0)
            raw_id = int(row["id"])
            attempt_id = int(row["fetch_attempt_id"] or 0)
            expected = target_evidence.get(content_id, {})
            expected_operation = str(expected.get("expected_detail_operation") or "")
            raw_path = _resolve_stored_path(str(row["local_path"]))
            if content_id not in authorized:
                if subset_only and content_id in target:
                    continue
                raise BatchReplayError(
                    f"新增 provider raw 超出派生目标范围：{row['id']}"
                )
            if (
                row["account_id"] is not None
                or attempt_id != successful_attempts.get(content_id)
                or str(row["provider"]) != "TikHub"
                or str(row["operation"]) != expected_operation
                or (
                    str(row["source"]) != "derived_applied"
                    and not (
                        content_id in allow_unapplied
                        and str(row["source"]) == "live"
                    )
                )
                or int(row["http_status"] or 0) != 200
                or int(row["byte_size"] or 0)
                != int(expected.get("expected_detail_raw_bytes") or 0)
                or str(row["sha256"])
                != str(expected.get("expected_detail_raw_sha256") or "")
                or not raw_path.is_relative_to(derived_raw_root.resolve())
                or content_id in raw_by_content
            ):
                raise BatchReplayError(f"新增 provider raw 超出派生目标范围：{row['id']}")
            raw_by_id[raw_id] = row
            raw_by_content[content_id] = raw_id
            rows["provider_raw_responses"].append(
                [raw_id, content_id, str(row["sha256"]), str(raw_path)]
            )

        for row in connection.execute(
            """
            SELECT * FROM evidence_artifacts
            WHERE id>? ORDER BY id
            """,
            (int(baseline_max_ids["evidence_artifacts"]),),
        ):
            content_id = int(row["content_id"] or 0)
            artifact_id = int(row["id"])
            artifact_path = _resolve_stored_path(str(row["local_path"]))
            try:
                metadata = json.loads(str(row["metadata_json"] or ""))
            except json.JSONDecodeError as exc:
                raise BatchReplayError(
                    f"新增媒体证据 metadata 非法：{artifact_id}"
                ) from exc
            raw_id = int(metadata.get("raw_response_id") or 0)
            if content_id not in authorized:
                if subset_only and content_id in target:
                    continue
                raise BatchReplayError(f"新增媒体证据超出目标范围：{row['id']}")
            if (
                str(row["artifact_type"]) != "media_source"
                or str(row["status"]) != "available"
                or str(row["processor_version"]) != "provider-media-source-v8.1"
                or row["legacy_fingerprint"] is not None
                or int(row["byte_size"] or 0) <= 0
                or len(str(row["sha256"])) != 64
                or raw_id != raw_by_content.get(content_id)
                or raw_id not in raw_by_id
                or not artifact_path.is_relative_to(media_root.resolve())
                or content_id in artifact_by_content
            ):
                raise BatchReplayError(f"新增媒体证据超出目标范围：{row['id']}")
            artifact_by_content[content_id] = artifact_id
            rows["evidence_artifacts"].append(
                [artifact_id, content_id, str(row["sha256"]), str(artifact_path)]
            )
    finally:
        connection.close()

    if not subset_only:
        for table in APPEND_CHANGED_TABLES:
            expected_count = int(baseline_counts[table]) + len(rows[table])
            if current_counts[table] != expected_count:
                raise BatchReplayError(f"{table} 存在删除、越界插入或 ID 链漂移")
    if set(raw_by_content) != set(successful_attempts):
        raise BatchReplayError("成功详情尝试与派生 raw 不是一一对应")
    if not set(artifact_by_content).issubset(raw_by_content):
        raise BatchReplayError("媒体证据未绑定本批处理派生 raw")
    if require_complete and not (
        set(slot_by_content)
        == set(successful_attempts)
        == set(raw_by_content)
        == set(artifact_by_content)
        == target
    ):
        raise BatchReplayError("最终白名单增量未完整覆盖全部目标")
    return {
        table: {
            "new_rows": len(values),
            "rows_sha256": replay._object_sha256(values),
        }
        for table, values in sorted(rows.items())
    }


def _private_output_inventory(root: Path, *, label: str) -> Mapping[Path, list[Any]]:
    if not os.path.lexists(root):
        raise BatchReplayError(f"{label}不存在：{root}")
    _private_directory(root, label=label)
    output: dict[Path, list[Any]] = {}
    for current_root, directory_names, file_names in os.walk(
        root, topdown=True, followlinks=False
    ):
        current = Path(current_root)
        _private_directory(current, label=f"{label}子目录")
        for name in directory_names:
            _private_directory(current / name, label=f"{label}子目录")
        for name in file_names:
            path = current / name
            metadata = _private_file(path, label=f"{label}文件")
            output[path.resolve()] = [metadata.st_size, replay._file_sha256(path)]
    return output


def _validate_output_inventory(
    contract: Mapping[str, Any],
    *,
    db_path: Path,
    derived_raw_root: Path,
    media_root: Path,
) -> Mapping[str, Any]:
    expected_raw, expected_media = _expected_output_inventory(
        contract, db_path=db_path
    )
    actual_raw = _optional_output_inventory(
        derived_raw_root.resolve(), label="派生 raw 输出目录"
    )
    actual_media = _optional_output_inventory(
        media_root.resolve(), label="媒体证据输出目录"
    )
    if actual_raw != expected_raw:
        raise BatchReplayError("派生 raw 文件系统清单与数据库不一致")
    if actual_media != expected_media:
        raise BatchReplayError("媒体证据文件系统清单与数据库不一致")
    return _output_inventory_evidence(actual_raw, actual_media)


def _output_inventory_evidence(
    raw: Mapping[Path, list[Any]], media: Mapping[Path, list[Any]]
) -> Mapping[str, Any]:
    return {
        "derived_raw": {
            "files": len(raw),
            "inventory_sha256": replay._object_sha256(
                [[str(path), *evidence] for path, evidence in sorted(raw.items())]
            ),
        },
        "media": {
            "files": len(media),
            "inventory_sha256": replay._object_sha256(
                [[str(path), *evidence] for path, evidence in sorted(media.items())]
            ),
        },
    }


def _expected_output_inventory(
    contract: Mapping[str, Any],
    *,
    db_path: Path,
    content_ids: set[int] | None = None,
) -> tuple[dict[Path, list[Any]], dict[Path, list[Any]]]:
    baseline_max_ids = contract["baseline"]["allowed_max_ids"]
    expected_raw: dict[Path, list[Any]] = {}
    expected_media: dict[Path, list[Any]] = {}
    connection = _immutable_connection(db_path)
    try:
        for row in connection.execute(
            """
            SELECT content_id,local_path,byte_size,sha256
            FROM provider_raw_responses
            WHERE id>? ORDER BY id
            """,
            (int(baseline_max_ids["provider_raw_responses"]),),
        ):
            if content_ids is not None and int(row["content_id"] or 0) not in content_ids:
                continue
            expected_raw[_resolve_stored_path(str(row["local_path"]))] = [
                int(row["byte_size"]),
                str(row["sha256"]),
            ]
        for row in connection.execute(
            """
            SELECT content_id,local_path,byte_size,sha256
            FROM evidence_artifacts
            WHERE id>? ORDER BY id
            """,
            (int(baseline_max_ids["evidence_artifacts"]),),
        ):
            if content_ids is not None and int(row["content_id"] or 0) not in content_ids:
                continue
            expected_media[_resolve_stored_path(str(row["local_path"]))] = [
                int(row["byte_size"]),
                str(row["sha256"]),
            ]
    finally:
        connection.close()
    return expected_raw, expected_media


def _optional_output_inventory(root: Path, *, label: str) -> Mapping[Path, list[Any]]:
    if not os.path.lexists(root):
        return {}
    return _private_output_inventory(root.resolve(), label=label)


def _refresh_plan_database(
    plan: replay.ReplayPlan, *, db_path: Path
) -> replay.ReplayPlan:
    summary = dict(plan.summary)
    summary["database_snapshot"] = _database_file_snapshot(db_path)
    return replace(plan, summary=summary)


def _advance_plan(
    plan: replay.ReplayPlan, *, completed_ids: Sequence[int], db_path: Path
) -> replay.ReplayPlan:
    completed = {int(value) for value in completed_ids}
    summary = dict(plan.summary)
    summary["database_snapshot"] = _database_file_snapshot(db_path)
    return replace(
        plan,
        summary=summary,
        candidates=tuple(
            item for item in plan.candidates if item.content_id not in completed
        ),
        already_materialized_ids=tuple(
            sorted(set(plan.already_materialized_ids) | completed)
        ),
    )


def _recover_interrupted_detail_slots(
    db_path: Path, *, content_ids: Sequence[int]
) -> list[Mapping[str, Any]]:
    values = sorted({int(value) for value in content_ids})
    if not values:
        return []
    placeholders = ",".join("?" for _ in values)
    connection = sqlite3.connect(db_path, timeout=30)
    connection.row_factory = sqlite3.Row
    recovered: list[Mapping[str, Any]] = []
    captured_at = _now_text()
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("BEGIN IMMEDIATE")
        rows = connection.execute(
            f"""
            SELECT fs.id slot_id,fs.content_id,fs.status,fs.provider,
                   fs.adapter_version,fs.last_error_code,
                   fa.id attempt_id,fa.response_finished_at,
                   EXISTS(
                       SELECT 1 FROM provider_raw_responses pr
                       WHERE pr.fetch_attempt_id=fa.id
                   ) raw_exists
            FROM fetch_slots fs
            JOIN fetch_attempts fa ON fa.slot_id=fs.id
            WHERE fs.content_id IN ({placeholders})
              AND fs.stage='detail' AND fs.window_key='lifetime'
              AND fa.attempt_number=(
                  SELECT MAX(fa2.attempt_number) FROM fetch_attempts fa2
                  WHERE fa2.slot_id=fs.id
              )
            ORDER BY fs.content_id
            """,
            tuple(values),
        ).fetchall()
        for row in rows:
            status = str(row["status"])
            if status == "running":
                if (
                    str(row["provider"]) != "TikHub"
                    or str(row["adapter_version"])
                    != "tikhub-discovery-derived-v8.1"
                    or bool(row["raw_exists"])
                    or row["response_finished_at"] is not None
                ):
                    raise BatchReplayError(
                        f"运行中详情槽不符合批处理恢复边界：{row['content_id']}"
                    )
                connection.execute(
                    """
                    UPDATE fetch_attempts
                    SET response_finished_at=?,error_code='batch_interrupted',
                        error_message='批处理进程在派生 raw 落库前中断'
                    WHERE id=?
                    """,
                    (captured_at, int(row["attempt_id"])),
                )
                connection.execute(
                    """
                    UPDATE fetch_slots
                    SET status='retryable_failed',
                        last_error_code='batch_interrupted',
                        last_error_message='批处理进程在派生 raw 落库前中断',
                        finished_at=?,updated_at=?
                    WHERE id=? AND status='running'
                    """,
                    (captured_at, captured_at, int(row["slot_id"])),
                )
                recovered.append(
                    {
                        "content_id": int(row["content_id"]),
                        "slot_id": int(row["slot_id"]),
                        "attempt_id": int(row["attempt_id"]),
                        "transition": "running_to_retryable_failed",
                    }
                )
            elif (
                status == "retryable_failed"
                and str(row["last_error_code"] or "") == "batch_interrupted"
            ):
                recovered.append(
                    {
                        "content_id": int(row["content_id"]),
                        "slot_id": int(row["slot_id"]),
                        "attempt_id": int(row["attempt_id"]),
                        "transition": "already_recovered",
                    }
                )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    replay._finalize_disposable_database(db_path)
    return recovered


def _recover_pending_live_raw_with_artifact(
    db_path: Path,
    *,
    contract: Mapping[str, Any],
    content_ids: Sequence[int],
    derived_raw_root: Path,
    media_root: Path,
) -> list[Mapping[str, Any]]:
    values = sorted({int(value) for value in content_ids})
    if not values:
        return []
    candidates: list[tuple[int, int]] = []
    seen_content_ids: set[int] = set()
    connection = _immutable_connection(db_path)
    try:
        for batch in _chunks(values):
            placeholders = ",".join("?" for _ in batch)
            for row in connection.execute(
                f"""
                SELECT fs.content_id,pr.id raw_response_id
                FROM fetch_slots fs
                JOIN fetch_attempts fa ON fa.slot_id=fs.id
                JOIN provider_raw_responses pr ON pr.fetch_attempt_id=fa.id
                WHERE fs.content_id IN ({placeholders})
                  AND fs.stage='detail' AND fs.window_key='lifetime'
                  AND fs.status='succeeded' AND pr.source='live'
                  AND EXISTS(
                      SELECT 1 FROM evidence_artifacts ea
                      WHERE ea.content_id=fs.content_id
                        AND ea.artifact_type='media_source'
                        AND ea.status='available'
                  )
                ORDER BY fs.content_id,fa.attempt_number DESC,pr.id DESC
                """,
                tuple(batch),
            ):
                pair = (int(row["content_id"]), int(row["raw_response_id"]))
                if pair[0] not in seen_content_ids:
                    seen_content_ids.add(pair[0])
                    candidates.append(pair)
    finally:
        connection.close()
    candidate_ids = [content_id for content_id, _raw_id in candidates]
    if not candidate_ids:
        return []
    _validate_batch_artifacts(
        db_path,
        contract=contract,
        content_ids=candidate_ids,
        derived_raw_root=derived_raw_root,
        media_root=media_root,
        allow_live_raw_content_ids=set(candidate_ids),
    )
    connection = sqlite3.connect(db_path, timeout=30)
    try:
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("BEGIN IMMEDIATE")
        for _content_id, raw_id in candidates:
            cursor = connection.execute(
                """
                UPDATE provider_raw_responses SET source='derived_applied'
                WHERE id=? AND source='live'
                """,
                (raw_id,),
            )
            if cursor.rowcount != 1:
                raise BatchReplayError("pending live raw 恢复发生并发漂移")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    replay._finalize_disposable_database(db_path)
    return [
        {
            "content_id": content_id,
            "raw_response_id": raw_id,
            "transition": "live_to_derived_applied_after_artifact_commit",
        }
        for content_id, raw_id in candidates
    ]


def _record_paths(paths: BatchPaths, index: int) -> tuple[Path, Path]:
    prefix = f"batch-{index:06d}"
    return paths.batches / f"{prefix}.intent.json", paths.batches / f"{prefix}.receipt.json"


def _cleanup_record_paths(
    paths: BatchPaths, index: int, cleanup_round: int
) -> tuple[Path, Path]:
    if cleanup_round <= 0:
        raise BatchReplayError("输出孤儿清理轮次非法")
    prefix = f"batch-{index:06d}.output-cleanup-{cleanup_round:06d}"
    return paths.batches / f"{prefix}.intent.json", paths.batches / f"{prefix}.receipt.json"


def _cleanup_record_state(
    paths: BatchPaths, index: int
) -> list[tuple[int, Path, Path, bool]]:
    records: list[tuple[int, Path, Path, bool]] = []
    cleanup_round = 1
    while True:
        intent_path, receipt_path = _cleanup_record_paths(
            paths, index, cleanup_round
        )
        has_intent = os.path.lexists(intent_path)
        has_receipt = os.path.lexists(receipt_path)
        if not has_intent and not has_receipt:
            break
        if has_receipt and not has_intent:
            raise BatchReplayError("输出孤儿清理 receipt 缺少 intent")
        if records and not records[-1][3]:
            raise BatchReplayError("输出孤儿清理轮次越过未完成轮次")
        records.append((cleanup_round, intent_path, receipt_path, has_receipt))
        if not has_receipt:
            break
        cleanup_round += 1
    return records


def _validate_owned_orphan_rows(
    rows: Sequence[Any],
    *,
    contract: Mapping[str, Any],
    content_ids: set[int],
    db_path: Path,
    derived_raw_root: Path,
    media_root: Path,
    recreated_expected_paths: set[Path] | None = None,
    historical_cleanup: bool = False,
) -> None:
    target_evidence = _target_contract_map(contract)
    if not content_ids or not content_ids.issubset(target_evidence):
        raise BatchReplayError("输出孤儿清理内容集合超出 pending intent")
    recreated_expected = {
        path.resolve() for path in (recreated_expected_paths or set())
    }
    link_to_content: dict[str, int] = {}
    connection = _immutable_connection(db_path)
    try:
        for batch in _chunks(sorted(content_ids)):
            placeholders = ",".join("?" for _ in batch)
            for row in connection.execute(
                f"SELECT id,link_id FROM content_items WHERE id IN ({placeholders})",
                tuple(batch),
            ):
                link_to_content[str(row["link_id"])] = int(row["id"])
        if set(link_to_content.values()) != content_ids:
            raise BatchReplayError("输出孤儿清理内容 link_id 不完整")
        raw_cache: dict[int, sqlite3.Row] = {}
        for raw_row in rows:
            if not isinstance(raw_row, list) or len(raw_row) != 4:
                raise BatchReplayError("输出孤儿清理行非法")
            label, raw_path, raw_bytes, raw_sha = raw_row
            try:
                expected_bytes = int(raw_bytes)
            except (TypeError, ValueError) as exc:
                raise BatchReplayError("输出孤儿清理字节数非法") from exc
            path = Path(str(raw_path))
            sha256 = str(raw_sha)
            if (
                not path.is_absolute()
                or expected_bytes < 0
                or len(sha256) != 64
                or any(value not in "0123456789abcdef" for value in sha256)
            ):
                raise BatchReplayError("输出孤儿清理文件证据非法")
            is_atomic_temp = path.name.startswith(".") and path.name.endswith(
                ".tmp"
            )
            body: Mapping[str, Any] | None = None
            if os.path.lexists(path) and path.resolve() not in recreated_expected:
                metadata = _private_file(path, label="待清理输出孤儿文件")
                if (
                    metadata.st_size != expected_bytes
                    or replay._file_sha256(path) != sha256
                ):
                    raise BatchReplayError("待清理输出孤儿文件证据漂移")
                if not is_atomic_temp:
                    try:
                        value = json.loads(path.read_bytes())
                    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                        raise BatchReplayError(
                            "待清理输出孤儿文件不是合法 JSON"
                        ) from exc
                    if not isinstance(value, Mapping):
                        raise BatchReplayError(
                            "待清理输出孤儿文件不是 JSON object"
                        )
                    body = value
            if label == "derived_raw":
                root = derived_raw_root.resolve()
                if not path.is_relative_to(root):
                    raise BatchReplayError("待清理派生 raw 路径越界")
                relative = path.relative_to(root)
                if len(relative.parts) != 4 or relative.parts[0] != "tikhub":
                    raise BatchReplayError("待清理派生 raw 路径形状非法")
                try:
                    content_id = int(relative.parts[1])
                except ValueError as exc:
                    raise BatchReplayError("待清理派生 raw 内容 ID 非法") from exc
                evidence = target_evidence.get(content_id)
                raw_name = relative.parts[3]
                if is_atomic_temp:
                    if not raw_name.startswith(".") or not raw_name.endswith(
                        ".json.tmp"
                    ):
                        raise BatchReplayError("待清理派生 raw 临时路径非法")
                    raw_name = raw_name[1:-4]
                name_parts = Path(raw_name).stem.split("-")
                if (
                    content_id not in content_ids
                    or evidence is None
                    or relative.parts[2] != evidence["expected_detail_operation"]
                    or Path(raw_name).suffix != ".json"
                    or len(name_parts) != 3
                    or name_parts[0] != "attempt"
                    or not name_parts[1].isdigit()
                    or int(name_parts[1]) <= 0
                    or len(name_parts[2]) != 12
                    or any(
                        value not in "0123456789abcdef" for value in name_parts[2]
                    )
                    or name_parts[2]
                    != str(evidence["expected_detail_raw_sha256"])[:12]
                    or (
                        not is_atomic_temp
                        and (
                            sha256 != evidence["expected_detail_raw_sha256"]
                            or expected_bytes
                            != int(evidence["expected_detail_raw_bytes"])
                        )
                    )
                    or (
                        is_atomic_temp
                        and expected_bytes
                        > int(evidence["expected_detail_raw_bytes"])
                    )
                ):
                    raise BatchReplayError("待清理派生 raw 不属于 pending 内容")
                attempt = connection.execute(
                    """
                    SELECT fa.id,fa.attempt_number,fs.provider,
                           fs.adapter_version,COUNT(pr.id) raw_rows,
                           (
                               SELECT MAX(fa2.attempt_number)
                               FROM fetch_attempts fa2
                               WHERE fa2.slot_id=fs.id
                           ) latest_attempt_number
                    FROM fetch_slots fs
                    JOIN fetch_attempts fa ON fa.slot_id=fs.id
                    LEFT JOIN provider_raw_responses pr
                      ON pr.fetch_attempt_id=fa.id
                    WHERE fs.content_id=? AND fs.stage='detail'
                      AND fs.window_key='lifetime'
                      AND fa.attempt_number=?
                    GROUP BY fa.id,fa.attempt_number,fs.provider,
                             fs.adapter_version
                    """,
                    (content_id, int(name_parts[1])),
                ).fetchone()
                if (
                    attempt is None
                    or attempt["provider"] != "TikHub"
                    or attempt["adapter_version"]
                    != "tikhub-discovery-derived-v8.1"
                    or int(attempt["raw_rows"] or 0) != 0
                    or (
                        not historical_cleanup
                        and int(attempt["attempt_number"])
                        != int(attempt["latest_attempt_number"])
                    )
                ):
                    raise BatchReplayError(
                        "待清理派生 raw 文件未绑定 pending attempt"
                    )
                if body is not None and (
                    set(body)
                    != {
                        "stage",
                        "data",
                        "derived_from_operation",
                        "source_raw_response_id",
                        "source_sha256",
                        "source_captured_at",
                    }
                    or body.get("stage") != "detail"
                    or not isinstance(body.get("data"), Mapping)
                    or int(body.get("source_raw_response_id") or 0)
                    != int(evidence["source_discovery_raw_id"])
                    or body.get("derived_from_operation")
                    != evidence["source_discovery_operation"]
                    or body.get("source_sha256")
                    != evidence["source_discovery_sha256"]
                    or body.get("source_captured_at")
                    != evidence["source_discovery_captured_at"]
                    or replay._object_sha256(body["data"])
                    != evidence["expected_detail_data_sha256"]
                ):
                    raise BatchReplayError("待清理派生 raw 来源证据非法")
                continue
            if label != "media":
                raise BatchReplayError("待清理输出孤儿 root 非法")
            root = media_root.resolve()
            if not path.is_relative_to(root):
                raise BatchReplayError("待清理媒体证据路径越界")
            relative = path.relative_to(root)
            media_name = relative.parts[-1]
            if is_atomic_temp:
                if not media_name.startswith(".") or not media_name.endswith(
                    ".json.tmp"
                ):
                    raise BatchReplayError("待清理媒体证据临时路径非法")
                media_name = media_name[1:-4]
            name_parts = Path(media_name).stem.split("-")
            if (
                len(relative.parts) != 3
                or relative.parts[1] != "sources"
                or Path(media_name).suffix != ".json"
                or len(name_parts) != 3
                or name_parts[0] != "source"
                or not name_parts[1].isdigit()
                or len(name_parts[2]) != 12
            ):
                raise BatchReplayError("待清理媒体证据路径形状非法")
            content_id = link_to_content.get(relative.parts[0])
            raw_id = int(name_parts[1])
            if content_id not in content_ids:
                raise BatchReplayError("待清理媒体证据不属于 pending 内容")
            raw = raw_cache.get(raw_id)
            if raw is None:
                raw = connection.execute(
                    """
                    SELECT content_id,source,operation,local_path,sha256,byte_size
                    FROM provider_raw_responses WHERE id=?
                    """,
                    (raw_id,),
                ).fetchone()
                if raw is not None:
                    raw_cache[raw_id] = raw
            evidence = target_evidence[content_id]
            if (
                raw is None
                or int(raw["content_id"] or 0) != content_id
                or raw["source"] not in {"live", "derived_applied"}
                or raw["operation"] != evidence["expected_detail_operation"]
            ):
                raise BatchReplayError("待清理媒体证据未绑定派生 raw")
            detail_path = _resolve_stored_path(str(raw["local_path"]))
            detail_stat = _private_file(detail_path, label="媒体孤儿来源 raw")
            if (
                detail_stat.st_size != int(raw["byte_size"])
                or replay._file_sha256(detail_path) != raw["sha256"]
            ):
                raise BatchReplayError("媒体孤儿来源 raw 文件漂移")
            try:
                detail_body = json.loads(detail_path.read_bytes())
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise BatchReplayError("媒体孤儿来源 raw 不是合法 JSON") from exc
            detail_data = detail_body.get("data") if isinstance(detail_body, Mapping) else None
            if (
                not isinstance(detail_data, Mapping)
                or replay._object_sha256(detail_data)
                != evidence["expected_detail_data_sha256"]
            ):
                raise BatchReplayError("媒体孤儿来源 raw 数据漂移")
            media_kind = (
                "video"
                if str(detail_data.get("content_type")) == "video"
                else "image"
            )
            expected_urls, expected_source_sha = media_module._media_source_identity(
                media_kind,
                [
                    str(url)
                    for url in detail_data.get("media_urls") or []
                    if isinstance(url, str)
                ],
            )
            if name_parts[2] != expected_source_sha[:12]:
                raise BatchReplayError("待清理媒体证据文件名来源哈希漂移")
            if body is not None:
                captured_at = str(body.get("captured_at") or "")
                try:
                    parsed_captured_at = datetime.fromisoformat(
                        captured_at.replace("Z", "+00:00")
                    )
                except ValueError as exc:
                    raise BatchReplayError("待清理媒体证据时间戳非法") from exc
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
                    or body.get("schema_version")
                    != "provider-media-source-v8.1"
                    or body.get("media_kind") != media_kind
                    or body.get("urls") != expected_urls
                    or not expected_urls
                    or any(
                        not media_module.is_supported_media_url(url)
                        for url in expected_urls
                    )
                    or body.get("source_sha256") != expected_source_sha
                    or int(body.get("raw_response_id") or 0) != raw_id
                    or parsed_captured_at.tzinfo is None
                ):
                    raise BatchReplayError("待清理媒体证据内容漂移")
    finally:
        connection.close()


def _output_orphan_rows(
    contract: Mapping[str, Any],
    *,
    db_path: Path,
    derived_raw_root: Path,
    media_root: Path,
) -> list[list[Any]]:
    expected_raw, expected_media = _expected_output_inventory(
        contract, db_path=db_path
    )
    actual_raw = _optional_output_inventory(
        derived_raw_root.resolve(), label="派生 raw 输出目录"
    )
    actual_media = _optional_output_inventory(
        media_root.resolve(), label="媒体证据输出目录"
    )
    rows: list[list[Any]] = []
    for label, expected, actual in (
        ("derived_raw", expected_raw, actual_raw),
        ("media", expected_media, actual_media),
    ):
        for path, evidence in expected.items():
            if actual.get(path) != evidence:
                raise BatchReplayError(f"已登记{label}文件缺失或漂移：{path}")
        for path in sorted(set(actual) - set(expected)):
            rows.append([label, str(path), *actual[path]])
    return rows


def _reconcile_pending_output_orphans(
    paths: BatchPaths,
    *,
    batch_index: int,
    contract: Mapping[str, Any],
    db_path: Path,
    derived_raw_root: Path,
    media_root: Path,
) -> Mapping[str, Any] | None:
    intent_path, _batch_receipt_path = _record_paths(paths, batch_index)
    batch_intent_sha = replay._file_sha256(intent_path)
    batch_intent = _read_json(intent_path, label="批次 intent")
    content_ids = {int(value) for value in batch_intent.get("content_ids", [])}
    contract_sha = replay._file_sha256(paths.contract)
    current_rows = _output_orphan_rows(
        contract,
        db_path=db_path,
        derived_raw_root=derived_raw_root,
        media_root=media_root,
    )
    if current_rows:
        _validate_owned_orphan_rows(
            current_rows,
            contract=contract,
            content_ids=content_ids,
            db_path=db_path,
            derived_raw_root=derived_raw_root,
            media_root=media_root,
        )
    current_owned_evidence: dict[Path, tuple[str, int, str]] = {}
    for raw_row in current_rows:
        if not isinstance(raw_row, list) or len(raw_row) != 4:
            raise BatchReplayError("当前输出孤儿证据非法")
        label, raw_path, raw_bytes, raw_sha = raw_row
        path = Path(str(raw_path)).resolve()
        if path in current_owned_evidence:
            raise BatchReplayError("当前输出孤儿路径重复")
        current_owned_evidence[path] = (
            str(label),
            int(raw_bytes),
            str(raw_sha),
        )
    record_state = _cleanup_record_state(paths, batch_index)
    completed_rounds: list[Mapping[str, Any]] = []
    for cleanup_round, _round_intent, _round_receipt, has_receipt in record_state:
        if not has_receipt:
            break
        completed_rounds.append(
            _validate_cleanup_round(
                paths,
                batch_index=batch_index,
                cleanup_round=cleanup_round,
                contract=contract,
                batch_intent_sha=batch_intent_sha,
                db_path=db_path,
                derived_raw_root=derived_raw_root,
                media_root=media_root,
                superseded_owned_evidence=current_owned_evidence,
            )
        )
    pending_record = record_state[-1] if record_state and not record_state[-1][3] else None
    if pending_record is not None:
        cleanup_round, cleanup_intent_path, cleanup_receipt_path, _ = pending_record
        cleanup_intent = _read_json(
            cleanup_intent_path, label="输出孤儿清理 intent"
        )
    else:
        if not current_rows:
            return _cleanup_chain_summary(completed_rounds)
        cleanup_round = len(completed_rounds) + 1
        cleanup_intent_path, cleanup_receipt_path = _cleanup_record_paths(
            paths, batch_index, cleanup_round
        )
        previous_receipt_sha = (
            completed_rounds[-1]["receipt_file_sha256"]
            if completed_rounds
            else None
        )
        cleanup_intent = {
            "version": 1,
            "batch_index": batch_index,
            "cleanup_round": cleanup_round,
            "batch_intent_sha256": batch_intent_sha,
            "contract_sha256": contract_sha,
            "previous_cleanup_receipt_sha256": previous_receipt_sha,
            "files": {
                "fields": ["root", "path", "bytes", "sha256"],
                "rows": current_rows,
                "rows_sha256": replay._object_sha256(current_rows),
            },
        }
        _atomic_json(cleanup_intent_path, cleanup_intent)
    previous_receipt_sha = (
        completed_rounds[-1]["receipt_file_sha256"] if completed_rounds else None
    )
    files = cleanup_intent.get("files")
    planned_rows = list(files.get("rows") or []) if isinstance(files, Mapping) else []
    if (
        int(cleanup_intent.get("version") or 0) != 1
        or int(cleanup_intent.get("batch_index") or 0) != batch_index
        or int(cleanup_intent.get("cleanup_round") or 0) != cleanup_round
        or cleanup_intent.get("batch_intent_sha256") != batch_intent_sha
        or cleanup_intent.get("contract_sha256") != contract_sha
        or cleanup_intent.get("previous_cleanup_receipt_sha256")
        != previous_receipt_sha
        or not isinstance(files, Mapping)
        or files.get("fields") != ["root", "path", "bytes", "sha256"]
        or files.get("rows_sha256") != replay._object_sha256(planned_rows)
        or not planned_rows
    ):
        raise BatchReplayError("输出孤儿清理 intent 漂移")
    planned_by_path: dict[Path, list[Any]] = {}
    roots = {
        "derived_raw": derived_raw_root.resolve(),
        "media": media_root.resolve(),
    }
    for raw_row in planned_rows:
        if not isinstance(raw_row, list) or len(raw_row) != 4:
            raise BatchReplayError("输出孤儿清理行非法")
        label, raw_path, raw_bytes, raw_sha = raw_row
        try:
            size = int(raw_bytes)
        except (TypeError, ValueError) as exc:
            raise BatchReplayError("输出孤儿清理字节数非法") from exc
        root = roots.get(str(label))
        path = Path(str(raw_path))
        if (
            root is None
            or not path.is_absolute()
            or not path.is_relative_to(root)
            or path in planned_by_path
            or size < 0
            or len(str(raw_sha)) != 64
            or any(value not in "0123456789abcdef" for value in str(raw_sha))
        ):
            raise BatchReplayError("输出孤儿清理路径或证据非法")
        planned_by_path[path] = [size, str(raw_sha)]
    _validate_owned_orphan_rows(
        planned_rows,
        contract=contract,
        content_ids=content_ids,
        db_path=db_path,
        derived_raw_root=derived_raw_root,
        media_root=media_root,
    )
    for raw_row in current_rows:
        path = Path(str(raw_row[1]))
        if planned_by_path.get(path) != [int(raw_row[2]), str(raw_row[3])]:
            raise BatchReplayError("输出孤儿清理后出现未授权文件")
    for path, evidence in sorted(planned_by_path.items()):
        if not os.path.lexists(path):
            continue
        metadata = _private_file(path, label="待清理输出孤儿文件")
        if (
            metadata.st_size != evidence[0]
            or replay._file_sha256(path) != evidence[1]
        ):
            raise BatchReplayError("待清理输出孤儿文件证据漂移")
        path.unlink()
        _fsync_directory(path.parent)
    remaining = _output_orphan_rows(
        contract,
        db_path=db_path,
        derived_raw_root=derived_raw_root,
        media_root=media_root,
    )
    if remaining:
        raise BatchReplayError("输出孤儿文件清理未完成")
    cleanup_receipt = {
        "version": 1,
        "status": "succeeded",
        "batch_index": batch_index,
        "cleanup_round": cleanup_round,
        "batch_intent_sha256": batch_intent_sha,
        "cleanup_intent_sha256": replay._file_sha256(cleanup_intent_path),
        "previous_cleanup_receipt_sha256": previous_receipt_sha,
        "files": files,
    }
    _atomic_json(cleanup_receipt_path, cleanup_receipt)
    completed_rounds.append(
        _validate_cleanup_round(
            paths,
            batch_index=batch_index,
            cleanup_round=cleanup_round,
            contract=contract,
            batch_intent_sha=batch_intent_sha,
            db_path=db_path,
            derived_raw_root=derived_raw_root,
            media_root=media_root,
        )
    )
    return _cleanup_chain_summary(completed_rounds)


def _validate_cleanup_round(
    paths: BatchPaths,
    *,
    batch_index: int,
    cleanup_round: int,
    contract: Mapping[str, Any],
    batch_intent_sha: str,
    db_path: Path,
    derived_raw_root: Path,
    media_root: Path,
    superseded_owned_evidence: Mapping[
        Path, tuple[str, int, str]
    ] | None = None,
) -> Mapping[str, Any] | None:
    cleanup_intent_path, cleanup_receipt_path = _cleanup_record_paths(
        paths, batch_index, cleanup_round
    )
    if not os.path.lexists(cleanup_intent_path) or not os.path.lexists(
        cleanup_receipt_path
    ):
        raise BatchReplayError("输出孤儿清理证据链不完整")
    cleanup_intent = _read_json(cleanup_intent_path, label="输出孤儿清理 intent")
    cleanup_receipt = _read_json(
        cleanup_receipt_path, label="输出孤儿清理 receipt"
    )
    files = cleanup_intent.get("files")
    rows = list(files.get("rows") or []) if isinstance(files, Mapping) else []
    previous_receipt_sha = (
        replay._file_sha256(
            _cleanup_record_paths(paths, batch_index, cleanup_round - 1)[1]
        )
        if cleanup_round > 1
        else None
    )
    if (
        int(cleanup_intent.get("version") or 0) != 1
        or int(cleanup_intent.get("batch_index") or 0) != batch_index
        or int(cleanup_intent.get("cleanup_round") or 0) != cleanup_round
        or cleanup_intent.get("batch_intent_sha256") != batch_intent_sha
        or cleanup_intent.get("contract_sha256")
        != replay._file_sha256(paths.contract)
        or cleanup_intent.get("previous_cleanup_receipt_sha256")
        != previous_receipt_sha
        or not isinstance(files, Mapping)
        or files.get("fields") != ["root", "path", "bytes", "sha256"]
        or not rows
        or files.get("rows_sha256") != replay._object_sha256(rows)
    ):
        raise BatchReplayError("输出孤儿清理 intent 证据漂移")
    roots = {
        "derived_raw": Path(str(contract["derived_raw_root"])).resolve(),
        "media": Path(str(contract["media_root"])).resolve(),
    }
    seen_paths: set[Path] = set()
    for raw_row in rows:
        if not isinstance(raw_row, list) or len(raw_row) != 4:
            raise BatchReplayError("输出孤儿清理行非法")
        label, raw_path, raw_bytes, raw_sha = raw_row
        try:
            size = int(raw_bytes)
        except (TypeError, ValueError) as exc:
            raise BatchReplayError("输出孤儿清理字节数非法") from exc
        root = roots.get(str(label))
        path = Path(str(raw_path))
        if (
            root is None
            or not path.is_absolute()
            or not path.is_relative_to(root)
            or path in seen_paths
            or size < 0
            or len(str(raw_sha)) != 64
            or any(value not in "0123456789abcdef" for value in str(raw_sha))
        ):
            raise BatchReplayError("输出孤儿清理路径或证据非法")
        seen_paths.add(path)
    batch_intent = _read_json(
        _record_paths(paths, batch_index)[0], label="批次 intent"
    )
    expected_raw, expected_media = _expected_output_inventory(
        contract, db_path=db_path
    )
    expected_outputs = {**expected_raw, **expected_media}
    recreated_expected: set[Path] = set()
    superseded_owned: set[Path] = set()
    current_owned = superseded_owned_evidence or {}
    for raw_row in rows:
        path = Path(str(raw_row[1])).resolve()
        current_evidence = current_owned.get(path)
        if current_evidence is not None:
            current_label, current_bytes, current_sha = current_evidence
            if current_label != str(raw_row[0]):
                raise BatchReplayError("后续输出孤儿 root 与历史清理记录冲突")
            metadata = _private_file(path, label="后续待清理输出孤儿文件")
            if (
                metadata.st_size != current_bytes
                or replay._file_sha256(path) != current_sha
            ):
                raise BatchReplayError("后续待清理输出孤儿文件证据漂移")
            superseded_owned.add(path)
            continue
        expected = expected_outputs.get(path)
        if expected is None:
            continue
        metadata = _private_file(path, label="清理后重建的登记文件")
        actual = [metadata.st_size, replay._file_sha256(path)]
        if actual != expected:
            raise BatchReplayError("清理后重建的登记文件漂移")
        recreated_expected.add(path)
    _validate_owned_orphan_rows(
        rows,
        contract=contract,
        content_ids={int(value) for value in batch_intent.get("content_ids", [])},
        db_path=db_path,
        derived_raw_root=derived_raw_root,
        media_root=media_root,
        recreated_expected_paths=recreated_expected | superseded_owned,
        historical_cleanup=True,
    )
    expected_receipt = {
        "version": 1,
        "status": "succeeded",
        "batch_index": batch_index,
        "cleanup_round": cleanup_round,
        "batch_intent_sha256": batch_intent_sha,
        "cleanup_intent_sha256": replay._file_sha256(cleanup_intent_path),
        "previous_cleanup_receipt_sha256": previous_receipt_sha,
        "files": files,
    }
    if cleanup_receipt != expected_receipt:
        raise BatchReplayError("输出孤儿清理 receipt 证据漂移")
    return {
        "cleanup_round": cleanup_round,
        "intent_file_sha256": replay._file_sha256(cleanup_intent_path),
        "receipt_file_sha256": replay._file_sha256(cleanup_receipt_path),
        "files": len(rows),
        "rows_sha256": files["rows_sha256"],
    }


def _cleanup_chain_summary(
    rounds: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any] | None:
    if not rounds:
        return None
    rows = [
        [
            int(value["cleanup_round"]),
            str(value["intent_file_sha256"]),
            str(value["receipt_file_sha256"]),
            int(value["files"]),
            str(value["rows_sha256"]),
        ]
        for value in rounds
    ]
    if [row[0] for row in rows] != list(range(1, len(rows) + 1)):
        raise BatchReplayError("输出孤儿清理轮次不连续")
    return {
        "rounds": len(rows),
        "files": sum(int(row[3]) for row in rows),
        "round_fields": [
            "cleanup_round",
            "intent_file_sha256",
            "receipt_file_sha256",
            "files",
            "rows_sha256",
        ],
        "round_rows": rows,
        "round_rows_sha256": replay._object_sha256(rows),
    }


def _validate_cleanup_evidence(
    paths: BatchPaths,
    *,
    batch_index: int,
    contract: Mapping[str, Any],
    batch_intent_sha: str,
    db_path: Path,
    derived_raw_root: Path,
    media_root: Path,
) -> Mapping[str, Any] | None:
    rounds: list[Mapping[str, Any]] = []
    for cleanup_round, _intent_path, _receipt_path, has_receipt in (
        _cleanup_record_state(paths, batch_index)
    ):
        if not has_receipt:
            raise BatchReplayError("输出孤儿清理证据链尚有未完成轮次")
        rounds.append(
            _validate_cleanup_round(
                paths,
                batch_index=batch_index,
                cleanup_round=cleanup_round,
                contract=contract,
                batch_intent_sha=batch_intent_sha,
                db_path=db_path,
                derived_raw_root=derived_raw_root,
                media_root=media_root,
            )
        )
    return _cleanup_chain_summary(rounds)


def _load_batch_records(paths: BatchPaths) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]:
    allowed_names: set[str] = set()
    intents: list[Mapping[str, Any]] = []
    receipts: list[Mapping[str, Any]] = []
    index = 1
    while True:
        intent_path, receipt_path = _record_paths(paths, index)
        has_intent = os.path.lexists(intent_path)
        has_receipt = os.path.lexists(receipt_path)
        if not has_intent and not has_receipt:
            break
        allowed_names.add(intent_path.name)
        if not has_intent:
            raise BatchReplayError("批次 receipt 缺少对应 intent")
        intents.append(_read_json(intent_path, label="批次 intent"))
        if has_receipt:
            allowed_names.add(receipt_path.name)
            receipts.append(_read_json(receipt_path, label="批次 receipt"))
        elif index != len(intents):
            raise BatchReplayError("批次记录出现非末尾 pending intent")
        cleanup_records = _cleanup_record_state(paths, index)
        for _round, cleanup_intent_path, cleanup_receipt_path, has_cleanup_receipt in (
            cleanup_records
        ):
            allowed_names.add(cleanup_intent_path.name)
            if has_cleanup_receipt:
                allowed_names.add(cleanup_receipt_path.name)
        if has_receipt and cleanup_records and not cleanup_records[-1][3]:
            raise BatchReplayError("已完成批次仍有未完成输出清理轮次")
        index += 1
    if len(intents) - len(receipts) > 1:
        raise BatchReplayError("存在多个未完成批次 intent")
    if len(intents) == len(receipts):
        next_intent, _next_receipt = _record_paths(paths, len(intents) + 1)
        recoverable_temps = {f".{next_intent.name}.tmp"}
    else:
        _pending_intent, pending_receipt = _record_paths(paths, len(intents))
        cleanup_records = _cleanup_record_state(paths, len(intents))
        recoverable_temps = {f".{pending_receipt.name}.tmp"}
        if cleanup_records and not cleanup_records[-1][3]:
            recoverable_temps.add(f".{cleanup_records[-1][2].name}.tmp")
        else:
            next_round = len(cleanup_records) + 1
            cleanup_intent, _cleanup_receipt = _cleanup_record_paths(
                paths, len(intents), next_round
            )
            recoverable_temps.add(f".{cleanup_intent.name}.tmp")
    for candidate in paths.batches.iterdir():
        if candidate.name in allowed_names:
            continue
        if candidate.name in recoverable_temps:
            _private_file(candidate, label="批次原子写临时文件")
            candidate.unlink()
            _fsync_directory(paths.batches)
            continue
        raise BatchReplayError(f"批次目录存在未知文件：{candidate.name}")
    return intents, receipts


def _validate_batch_chain(
    paths: BatchPaths,
    *,
    contract: Mapping[str, Any],
    intents: Sequence[Mapping[str, Any]],
    receipts: Sequence[Mapping[str, Any]],
    db_path: Path,
    derived_raw_root: Path,
    media_root: Path,
) -> set[int]:
    target = {int(value) for value in contract["target_ids"]}
    contract_sha = replay._file_sha256(paths.contract)
    completed: set[int] = set()
    previous_database = contract["database"]
    previous_receipt_sha256: str | None = None
    for offset, intent in enumerate(intents, start=1):
        content_ids = [int(value) for value in intent.get("content_ids", [])]
        if (
            int(intent.get("version") or 0) != 1
            or int(intent.get("batch_index") or 0) != offset
            or content_ids != sorted(set(content_ids))
            or not set(content_ids).issubset(target)
            or replay._object_sha256(content_ids)
            != intent.get("content_ids_sha256")
            or intent.get("contract_sha256") != contract_sha
            or intent.get("before_database") != previous_database
            or intent.get("previous_receipt_sha256")
            != previous_receipt_sha256
            or completed & set(content_ids)
        ):
            raise BatchReplayError(f"批次 intent 链不合法：{offset}")
        if offset > len(receipts):
            continue
        receipt = receipts[offset - 1]
        intent_sha = replay._file_sha256(_record_paths(paths, offset)[0])
        recovered = {int(value) for value in receipt.get("recovered_content_ids", [])}
        processed = {int(value) for value in receipt.get("processed_content_ids", [])}
        applied = receipt.get("apply") or {}
        if (
            int(receipt.get("version") or 0) != 1
            or int(receipt.get("batch_index") or 0) != offset
            or [int(value) for value in receipt.get("content_ids", [])]
            != content_ids
            or receipt.get("content_ids_sha256")
            != intent.get("content_ids_sha256")
            or receipt.get("intent_sha256")
            != intent_sha
            or recovered & processed
            or recovered | processed != set(content_ids)
            or applied.get("status") != "succeeded"
            or int(applied.get("provider_calls") or 0) != 0
            or int(applied.get("processed") or 0) != len(processed)
            or applied.get("processed_ids_sha256")
            != replay._object_sha256(sorted(processed))
            or applied.get("provider_usage_before") != contract["provider_usage"]
            or applied.get("provider_usage_after") != contract["provider_usage"]
            or int((receipt.get("artifacts") or {}).get("contents") or 0)
            != len(content_ids)
        ):
            raise BatchReplayError(f"批次 receipt 链不合法：{offset}")
        cleanup_evidence = _validate_cleanup_evidence(
            paths,
            batch_index=offset,
            contract=contract,
            batch_intent_sha=intent_sha,
            db_path=db_path,
            derived_raw_root=derived_raw_root,
            media_root=media_root,
        )
        if receipt.get("output_cleanup") != cleanup_evidence:
            raise BatchReplayError(f"批次输出孤儿清理证据漂移：{offset}")
        after_database = receipt.get("after_database")
        if (
            not isinstance(after_database, Mapping)
            or str(after_database.get("path"))
            != str(contract["database"]["path"])
            or int(after_database.get("device") or -1)
            != int(contract["database"]["device"])
            or int(after_database.get("inode") or -1)
            != int(contract["database"]["inode"])
            or int(after_database.get("nlink") or -1) != 1
            or int(after_database.get("bytes") or 0) <= 0
            or len(str(after_database.get("sha256") or "")) != 64
        ):
            raise BatchReplayError(f"批次 receipt 数据库证据非法：{offset}")
        previous_database = after_database
        previous_receipt_sha256 = replay._file_sha256(
            _record_paths(paths, offset)[1]
        )
        completed.update(content_ids)
    return completed


def _batch_chain_evidence(
    paths: BatchPaths,
    *,
    intents: Sequence[Mapping[str, Any]],
    receipts: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any]:
    if len(intents) != len(receipts):
        raise BatchReplayError("批次链尚有未完成 intent")
    rows: list[list[Any]] = []
    for index in range(1, len(intents) + 1):
        intent_path, receipt_path = _record_paths(paths, index)
        cleanup_rows: list[list[Any]] = []
        for cleanup_round, cleanup_intent_path, cleanup_receipt_path, has_receipt in (
            _cleanup_record_state(paths, index)
        ):
            if not has_receipt:
                raise BatchReplayError("输出孤儿清理证据链不完整")
            cleanup_rows.append(
                [
                    cleanup_round,
                    replay._file_sha256(cleanup_intent_path),
                    replay._file_sha256(cleanup_receipt_path),
                ]
            )
        rows.append(
            [
                index,
                replay._file_sha256(intent_path),
                replay._file_sha256(receipt_path),
                len(cleanup_rows),
                replay._object_sha256(cleanup_rows),
            ]
        )
    return {
        "fields": [
            "batch_index",
            "intent_file_sha256",
            "receipt_file_sha256",
            "cleanup_rounds",
            "cleanup_file_chain_sha256",
        ],
        "rows": rows,
        "rows_sha256": replay._object_sha256(rows),
    }


def _validate_receipt_apply(
    receipt: Mapping[str, Any], *, contract: Mapping[str, Any]
) -> None:
    processed = [int(value) for value in receipt.get("processed_content_ids", [])]
    recovered = [int(value) for value in receipt.get("recovered_content_ids", [])]
    applied = receipt.get("apply")
    if not isinstance(applied, Mapping):
        raise BatchReplayError("批次 receipt 缺少 apply 证据")
    results = applied.get("results")
    if not isinstance(results, list):
        raise BatchReplayError("批次 receipt apply results 非法")
    result_ids: list[int] = []
    for result in results:
        if not isinstance(result, Mapping):
            raise BatchReplayError("批次 receipt apply result 非法")
        content_id = int(result.get("content_id") or 0)
        completed_stages = {
            *list(result.get("created") or []),
            *list(result.get("replayed") or []),
            *list(result.get("already_succeeded") or []),
        }
        if (
            result.get("mode") not in {"detail_and_media", "media_only"}
            or result.get("failed")
            or "detail" not in completed_stages
        ):
            raise BatchReplayError(f"批次 apply result 未完成详情：{content_id}")
        result_ids.append(content_id)
    storage = applied.get("storage")
    if (
        result_ids != processed
        or int(applied.get("processed") or 0) != len(processed)
        or applied.get("processed_ids_sha256")
        != replay._object_sha256(processed)
        or int(applied.get("provider_calls") or 0) != 0
        or applied.get("provider_usage_before") != contract["provider_usage"]
        or applied.get("provider_usage_after") != contract["provider_usage"]
        or not isinstance(storage, Mapping)
        or storage.get("journal_mode") != "delete"
        or not isinstance(storage.get("checkpoint"), list)
        or not storage["checkpoint"]
        or int(storage["checkpoint"][0]) != 0
    ):
        raise BatchReplayError("批次 receipt apply 证据不完整")
    if processed and (
        applied.get("already_materialized_requested") != []
        or str(applied.get("derived_raw_root"))
        != str(contract["derived_raw_root"])
        or str(applied.get("media_root")) != str(contract["media_root"])
    ):
        raise BatchReplayError("批次 receipt apply 输出根或重复集合漂移")
    recovery = receipt.get("interrupted_slot_recovery")
    if not isinstance(recovery, list):
        raise BatchReplayError("批次 receipt 中断恢复证据非法")
    recovery_content_ids: list[int] = []
    for row in recovery:
        if (
            not isinstance(row, Mapping)
            or int(row.get("slot_id") or 0) <= 0
            or int(row.get("attempt_id") or 0) <= 0
            or row.get("transition")
            not in {"running_to_retryable_failed", "already_recovered"}
        ):
            raise BatchReplayError("批次 receipt 中断恢复行非法")
        recovery_content_ids.append(int(row.get("content_id") or 0))
    if (
        len(recovery_content_ids) != len(set(recovery_content_ids))
        or not set(recovery_content_ids).issubset(processed)
        or set(processed) & set(recovered)
    ):
        raise BatchReplayError("批次 receipt 中断恢复集合漂移")
    raw_recovery = receipt.get("raw_application_recovery")
    if not isinstance(raw_recovery, list):
        raise BatchReplayError("批次 receipt raw 应用恢复证据非法")
    raw_recovery_ids: list[int] = []
    raw_response_ids: list[int] = []
    for row in raw_recovery:
        if (
            not isinstance(row, Mapping)
            or int(row.get("raw_response_id") or 0) <= 0
            or row.get("transition")
            != "live_to_derived_applied_after_artifact_commit"
        ):
            raise BatchReplayError("批次 receipt raw 应用恢复行非法")
        raw_recovery_ids.append(int(row.get("content_id") or 0))
        raw_response_ids.append(int(row["raw_response_id"]))
    if (
        len(raw_recovery_ids) != len(set(raw_recovery_ids))
        or len(raw_response_ids) != len(set(raw_response_ids))
        or not set(raw_recovery_ids).issubset(recovered)
    ):
        raise BatchReplayError("批次 receipt raw 应用恢复集合漂移")


def _validate_receipt_disk(
    receipt: Mapping[str, Any], *, contract: Mapping[str, Any]
) -> None:
    disk = receipt.get("disk")
    expected_labels = {
        "database",
        "derived_raw_root",
        "media_root",
        "run_root",
    }
    if not isinstance(disk, Mapping) or set(disk) != expected_labels:
        raise BatchReplayError("批次 receipt 磁盘证据集合漂移")
    for label, evidence in disk.items():
        if not isinstance(evidence, Mapping):
            raise BatchReplayError(f"批次 receipt 磁盘证据非法：{label}")
        try:
            total = int(evidence["total"])
            used = int(evidence["used"])
            free = int(evidence["free"])
            device = int(evidence["device"])
            anchor = str(evidence["anchor"])
        except (KeyError, TypeError, ValueError) as exc:
            raise BatchReplayError(
                f"批次 receipt 磁盘证据字段非法：{label}"
            ) from exc
        if (
            total <= 0
            or used < 0
            or free < int(contract["min_free_bytes"])
            or used > total
            or free > total
            or device < 0
            or not Path(anchor).is_absolute()
        ):
            raise BatchReplayError(f"批次 receipt 磁盘门禁证据非法：{label}")


def _validate_receipt_evidence_at_completion(
    paths: BatchPaths,
    *,
    contract: Mapping[str, Any],
    receipts: Sequence[Mapping[str, Any]],
    db_path: Path,
    derived_raw_root: Path,
    media_root: Path,
) -> Mapping[str, Any]:
    cumulative: set[int] = set()
    rows: list[list[Any]] = []
    for index, receipt in enumerate(receipts, start=1):
        content_ids = [int(value) for value in receipt.get("content_ids", [])]
        cumulative.update(content_ids)
        _validate_receipt_apply(receipt, contract=contract)
        _validate_receipt_disk(receipt, contract=contract)
        expected_artifacts = _validate_batch_artifacts(
            db_path,
            contract=contract,
            content_ids=content_ids,
            derived_raw_root=derived_raw_root,
            media_root=media_root,
        )
        expected_critical = {
            "protected": contract["critical_baseline"],
            "allowed_prefix": contract["baseline"]["allowed_prefix"],
            "allowed_append_scope": _validate_allowed_append_scope(
                contract,
                db_path=db_path,
                derived_raw_root=derived_raw_root,
                media_root=media_root,
                authorized_content_ids=set(cumulative),
                subset_only=True,
            ),
        }
        expected_raw, expected_media = _expected_output_inventory(
            contract,
            db_path=db_path,
            content_ids=set(cumulative),
        )
        expected_output_inventory = _output_inventory_evidence(
            expected_raw, expected_media
        )
        if receipt.get("artifacts") != expected_artifacts:
            raise BatchReplayError(f"批次 receipt 制品证据漂移：{index}")
        if receipt.get("critical_unchanged") != expected_critical:
            raise BatchReplayError(f"批次 receipt 数据边界证据漂移：{index}")
        if receipt.get("output_inventory") != expected_output_inventory:
            raise BatchReplayError(f"批次 receipt 输出清单证据漂移：{index}")
        rows.append(
            [
                index,
                replay._object_sha256(expected_artifacts),
                replay._object_sha256(expected_critical),
                replay._object_sha256(expected_output_inventory),
            ]
        )
    return {
        "fields": [
            "batch_index",
            "artifacts_evidence_sha256",
            "critical_evidence_sha256",
            "output_inventory_sha256",
        ],
        "rows": rows,
        "rows_sha256": replay._object_sha256(rows),
        "receipt_files_sha256": _batch_chain_evidence(
            paths,
            intents=[
                _read_json(_record_paths(paths, index)[0], label="批次 intent")
                for index in range(1, len(receipts) + 1)
            ],
            receipts=receipts,
        )["rows_sha256"],
    }


def _initial_contract(
    plan: replay.ReplayPlan,
    *,
    db_path: Path,
    derived_raw_root: Path,
    media_root: Path,
    canary_size: int,
    batch_size: int,
    min_free_bytes: int,
    run_root: Path,
) -> Mapping[str, Any]:
    target_ids = [item.content_id for item in plan.candidates]
    baseline = _database_baseline(db_path, target_ids=target_ids)
    target_contract = _build_target_contract(plan, db_path=db_path)
    code = _code_contract()
    code_snapshots = _materialize_code_snapshots(run_root, code=code)
    return {
        "version": 1,
        "created_at": _now_text(),
        "database": _database_file_snapshot(db_path),
        "derived_raw_root": str(derived_raw_root.resolve()),
        "media_root": str(media_root.resolve()),
        "canary_size": canary_size,
        "batch_size": batch_size,
        "min_free_bytes": min_free_bytes,
        "target_ids": target_ids,
        "target_ids_sha256": replay._object_sha256(target_ids),
        "target_count": len(target_ids),
        "target_contract": target_contract,
        "initial_plan_summary": plan.summary,
        "expected_target_text": _target_text_hash(plan, db_path=db_path),
        "baseline": baseline,
        "critical_baseline": {
            "schema": baseline["schema"],
            "protected_tables": baseline["protected_tables"],
            "protected_sequences": baseline["protected_sequences"],
            "content_stable": baseline["content_stable"],
            "non_target_content_full": baseline["non_target_content_full"],
            "allowed_non_target": baseline["allowed_non_target"],
        },
        "provider_usage": replay._usage_snapshot(db_path, immutable=True),
        "code": code,
        "code_snapshots": code_snapshots,
    }


def _validate_contract(
    contract: Mapping[str, Any],
    *,
    db_path: Path,
    derived_raw_root: Path,
    media_root: Path,
    canary_size: int,
    batch_size: int,
    min_free_bytes: int,
    run_root: Path,
) -> None:
    expected = {
        "derived_raw_root": str(derived_raw_root.resolve()),
        "media_root": str(media_root.resolve()),
        "canary_size": canary_size,
        "batch_size": batch_size,
        "min_free_bytes": min_free_bytes,
    }
    for key, value in expected.items():
        if contract.get(key) != value:
            raise BatchReplayError(f"批处理合同参数漂移：{key}")
    if str((contract.get("database") or {}).get("path")) != str(db_path.resolve()):
        raise BatchReplayError("批处理数据库路径漂移")
    if contract.get("code") != _code_contract():
        raise BatchReplayError("批处理代码 SHA256 漂移")
    _validate_code_snapshots(
        contract.get("code_snapshots"),
        run_root=run_root,
        code=contract["code"],
    )
    _target_contract_map(contract)


def _verify_critical_unchanged(
    contract: Mapping[str, Any],
    *,
    db_path: Path,
    target_ids: Sequence[int],
    derived_raw_root: Path,
    media_root: Path,
    authorized_content_ids: set[int],
    allow_unapplied_raw_content_ids: set[int] | None = None,
) -> Mapping[str, Any]:
    current = _critical_snapshot(db_path, target_ids=target_ids)
    if current != contract.get("critical_baseline"):
        raise BatchReplayError("批处理改变了关键禁止变更数据")
    if replay._usage_snapshot(
        db_path, immutable=True
    ) != contract.get("provider_usage"):
        raise BatchReplayError("批处理改变了 provider 用量或预算")
    connection = _immutable_connection(db_path)
    try:
        allowed_prefix = _allowed_prefix_hashes(
            connection, max_ids=contract["baseline"]["allowed_max_ids"]
        )
    finally:
        connection.close()
    if allowed_prefix != contract["baseline"]["allowed_prefix"]:
        raise BatchReplayError("批处理改变了白名单表的历史前缀行")
    return {
        "protected": current,
        "allowed_prefix": allowed_prefix,
        "allowed_append_scope": _validate_allowed_append_scope(
            contract,
            db_path=db_path,
            derived_raw_root=derived_raw_root,
            media_root=media_root,
            authorized_content_ids=authorized_content_ids,
            allow_unapplied_raw_content_ids=allow_unapplied_raw_content_ids,
        ),
    }


def _verify_complete(
    contract: Mapping[str, Any],
    *,
    db_path: Path,
    plan: replay.ReplayPlan,
    derived_raw_root: Path,
    media_root: Path,
) -> Mapping[str, Any]:
    target_ids = [int(value) for value in contract["target_ids"]]
    done = set(plan.already_materialized_ids) & set(target_ids)
    ready = {item.content_id for item in plan.candidates} & set(target_ids)
    if ready or done != set(target_ids):
        raise BatchReplayError("批处理目标尚未全部完成")
    baseline = _database_baseline(db_path, target_ids=target_ids)
    for key in (
        "schema",
        "protected_tables",
        "protected_sequences",
        "content_stable",
        "non_target_content_full",
        "allowed_non_target",
    ):
        if baseline[key] != contract["baseline"][key]:
            raise BatchReplayError(f"最终数据库非白名单证据漂移：{key}")
    target_text = _current_target_text_hash(db_path, target_ids=target_ids)
    if target_text != contract["expected_target_text"]:
        raise BatchReplayError("最终标题/正文与冻结缓存预期不一致")
    artifacts = _validate_batch_artifacts(
        db_path,
        contract=contract,
        content_ids=target_ids,
        derived_raw_root=derived_raw_root,
        media_root=media_root,
    )
    allowed_scope = _validate_allowed_append_scope(
        contract,
        db_path=db_path,
        derived_raw_root=derived_raw_root,
        media_root=media_root,
        require_complete=True,
    )
    output_inventory = _validate_output_inventory(
        contract,
        db_path=db_path,
        derived_raw_root=derived_raw_root,
        media_root=media_root,
    )
    return {
        "target_count": len(target_ids),
        "ready": len(ready),
        "completed": len(done),
        "missing_media_urls": int(plan.summary["history"]["missing_media_urls"]),
        "inconsistent_detail_slot": int(
            plan.summary["history"]["inconsistent_detail_slot"]
        ),
        "database": _database_file_snapshot(db_path),
        "target_text": target_text,
        "artifacts": artifacts,
        "allowed_append_scope": allowed_scope,
        "output_inventory": output_inventory,
        "allowed_table_counts_before": contract["baseline"][
            "allowed_table_counts"
        ],
        "allowed_table_counts_after": baseline["allowed_table_counts"],
    }


def run_batches(
    *,
    db_path: Path,
    derived_raw_root: Path,
    media_root: Path,
    run_root: Path,
    replay_contract: replay.ReplayContract = replay.PRODUCTION_CONTRACT,
    canary_size: int = DEFAULT_CANARY_SIZE,
    batch_size: int = DEFAULT_BATCH_SIZE,
    min_free_bytes: int = DEFAULT_MIN_FREE_BYTES,
    max_batches: int | None = None,
    before_batch_apply: Callable[[int, Sequence[int]], None] | None = None,
    after_batch_applied: Callable[[int, Mapping[str, Any]], None] | None = None,
) -> Mapping[str, Any]:
    if canary_size <= 0 or batch_size <= 0:
        raise BatchReplayError("canary_size 和 batch_size 必须为正整数")
    if max_batches is not None and max_batches <= 0:
        raise BatchReplayError("max_batches 必须为正整数")
    db_path = _safe_resolve_input(db_path, label="批处理数据库")
    derived_raw_root = _safe_resolve_input(
        derived_raw_root, label="派生 raw 输出目录"
    )
    media_root = _safe_resolve_input(media_root, label="媒体证据输出目录")
    run_root = _safe_resolve_input(run_root, label="批处理运行目录")
    _assert_run_root_isolated(
        run_root,
        db_path=db_path,
        derived_raw_root=derived_raw_root,
        media_root=media_root,
    )
    replay._assert_disposable_database(db_path)
    replay._assert_isolated_output_roots(
        db_path=db_path,
        derived_raw_root=derived_raw_root,
        media_root=media_root,
    )
    paths = _paths(run_root)
    created_receipts: list[Mapping[str, Any]] = []
    claim_paths = {
        _database_claim_path(db_path),
        _output_claim_path(derived_raw_root),
        _output_claim_path(media_root),
        paths.claim,
    }
    with ExitStack() as claim_stack:
        for claim_path in sorted(claim_paths, key=str):
            claim_stack.enter_context(_exclusive_claim(claim_path))
        _validate_run_root_inventory(paths)
        if os.path.lexists(paths.completion) and not os.path.lexists(paths.contract):
            raise BatchReplayError("completion 缺少对应批处理合同")
        disk_paths = {
            "database": db_path,
            "derived_raw_root": derived_raw_root,
            "media_root": media_root,
            "run_root": paths.run_root,
        }
        _disk_gates(disk_paths, min_free_bytes=min_free_bytes)
        contract: Mapping[str, Any] | None = None
        intents: list[Mapping[str, Any]] = []
        receipts: list[Mapping[str, Any]] = []
        interrupted_slot_recovery: list[Mapping[str, Any]] = []
        raw_application_recovery: list[Mapping[str, Any]] = []
        pending_output_cleanup: Mapping[str, Any] | None = None
        if os.path.lexists(paths.contract):
            contract = _read_json(paths.contract, label="批处理合同")
            _validate_contract(
                contract,
                db_path=db_path,
                derived_raw_root=derived_raw_root,
                media_root=media_root,
                canary_size=canary_size,
                batch_size=batch_size,
                min_free_bytes=min_free_bytes,
                run_root=paths.run_root,
            )
            _validate_database_identity(db_path, expected=contract["database"])
            intents, receipts = _load_batch_records(paths)
            chain_completed = _validate_batch_chain(
                paths,
                contract=contract,
                intents=intents,
                receipts=receipts,
                db_path=db_path,
                derived_raw_root=derived_raw_root,
                media_root=media_root,
            )
            if receipts:
                _validate_receipt_evidence_at_completion(
                    paths,
                    contract=contract,
                    receipts=receipts,
                    db_path=db_path,
                    derived_raw_root=derived_raw_root,
                    media_root=media_root,
                )
            has_pending = bool(intents and len(intents) == len(receipts) + 1)
            if not has_pending:
                expected_database = (
                    receipts[-1]["after_database"]
                    if receipts
                    else contract["database"]
                )
                if _database_file_snapshot(db_path) != expected_database:
                    raise BatchReplayError("批处理数据库与最新 receipt 不一致")
            if has_pending:
                interrupted_slot_recovery = _recover_interrupted_detail_slots(
                    db_path,
                    content_ids=[int(value) for value in intents[-1]["content_ids"]],
                )
                pending_output_cleanup = _reconcile_pending_output_orphans(
                    paths,
                    batch_index=int(intents[-1]["batch_index"]),
                    contract=contract,
                    db_path=db_path,
                    derived_raw_root=derived_raw_root,
                    media_root=media_root,
                )
                raw_application_recovery = _recover_pending_live_raw_with_artifact(
                    db_path,
                    contract=contract,
                    content_ids=[int(value) for value in intents[-1]["content_ids"]],
                    derived_raw_root=derived_raw_root,
                    media_root=media_root,
                )
            _validate_output_inventory(
                contract,
                db_path=db_path,
                derived_raw_root=derived_raw_root,
                media_root=media_root,
            )
            authorized_content_ids = set(chain_completed)
            if has_pending:
                authorized_content_ids.update(
                    int(value) for value in intents[-1]["content_ids"]
                )
            _verify_critical_unchanged(
                contract,
                db_path=db_path,
                target_ids=[int(value) for value in contract["target_ids"]],
                derived_raw_root=derived_raw_root,
                media_root=media_root,
                authorized_content_ids=authorized_content_ids,
                allow_unapplied_raw_content_ids=(
                    {
                        int(value)
                        for value in intents[-1]["content_ids"]
                    }
                    if has_pending
                    else set()
                ),
            )
        plan = replay.build_replay_plan(db_path=db_path, contract=replay_contract)
        if plan.summary.get("status") != "ready":
            raise BatchReplayError("缓存物化计划未通过")
        if contract is None:
            _require_empty_output_root(
                derived_raw_root, label="派生 raw 输出目录"
            )
            _require_empty_output_root(media_root, label="媒体证据输出目录")
            contract = _initial_contract(
                plan,
                db_path=db_path,
                derived_raw_root=derived_raw_root,
                media_root=media_root,
                canary_size=canary_size,
                batch_size=batch_size,
                min_free_bytes=min_free_bytes,
                run_root=paths.run_root,
            )
            _atomic_json(paths.contract, contract)
            intents, receipts = _load_batch_records(paths)
            chain_completed = _validate_batch_chain(
                paths,
                contract=contract,
                intents=intents,
                receipts=receipts,
                db_path=db_path,
                derived_raw_root=derived_raw_root,
                media_root=media_root,
            )
        target_ids = [int(value) for value in contract["target_ids"]]
        target = set(target_ids)
        ready = {item.content_id for item in plan.candidates} & target
        done = set(plan.already_materialized_ids) & target
        if ready | done != target or ready & done:
            raise BatchReplayError("当前数据库无法与冻结目标集合对账")
        if not chain_completed.issubset(done):
            raise BatchReplayError("已完成 receipt 与当前数据库不一致")
        unreceipted_done = done - chain_completed
        pending_ids = (
            {int(value) for value in intents[-1]["content_ids"]}
            if intents and len(intents) == len(receipts) + 1
            else set()
        )
        if not unreceipted_done.issubset(pending_ids):
            raise BatchReplayError("存在未被 pending intent 授权的已物化目标")
        if intents and len(intents) == len(receipts) + 1:
            pending = intents[-1]
            index = int(pending["batch_index"])
        else:
            pending = None
            index = len(intents) + 1

        while True:
            if max_batches is not None and len(created_receipts) >= max_batches:
                break
            ready = {item.content_id for item in plan.candidates} & target
            done = set(plan.already_materialized_ids) & target
            _disk_gates(disk_paths, min_free_bytes=min_free_bytes)
            if pending is None:
                if not ready:
                    break
                interrupted_slot_recovery = []
                raw_application_recovery = []
                pending_output_cleanup = None
                size = canary_size if not receipts and not created_receipts else batch_size
                content_ids = sorted(ready)[:size]
                intent = {
                    "version": 1,
                    "batch_index": index,
                    "content_ids": content_ids,
                    "content_ids_sha256": replay._object_sha256(content_ids),
                    "before_database": _database_file_snapshot(db_path),
                    "contract_sha256": replay._file_sha256(paths.contract),
                    "previous_receipt_sha256": (
                        replay._file_sha256(_record_paths(paths, index - 1)[1])
                        if index > 1
                        else None
                    ),
                }
                intent_path, receipt_path = _record_paths(paths, index)
                _atomic_json(intent_path, intent)
            else:
                intent = pending
                content_ids = [int(value) for value in intent["content_ids"]]
                intent_path, receipt_path = _record_paths(paths, index)
                if replay._object_sha256(content_ids) != intent.get(
                    "content_ids_sha256"
                ):
                    raise BatchReplayError("pending intent 内容 ID 哈希漂移")
            intended = set(content_ids)
            recovered = sorted(intended & done)
            remaining = sorted(intended & ready)
            if set(recovered) | set(remaining) != intended:
                raise BatchReplayError("pending intent 无法与当前数据库对账")
            started = time.monotonic()
            if before_batch_apply is not None:
                before_batch_apply(index, remaining)
            if remaining:
                batch_plan = _refresh_plan_database(plan, db_path=db_path)
                applied = replay.apply_replay_plan(
                    batch_plan,
                    db_path=db_path,
                    derived_raw_root=derived_raw_root,
                    media_root=media_root,
                    content_ids=remaining,
                )
            else:
                storage = replay._finalize_disposable_database(db_path)
                usage = replay._usage_snapshot(db_path, immutable=True)
                applied = {
                    "status": "succeeded",
                    "processed": 0,
                    "processed_ids_sha256": replay._object_sha256([]),
                    "provider_calls": 0,
                    "provider_usage_before": usage,
                    "provider_usage_after": usage,
                    "storage": storage,
                    "results": [],
                }
            if after_batch_applied is not None:
                after_batch_applied(index, applied)
            artifacts = _validate_batch_artifacts(
                db_path,
                contract=contract,
                content_ids=content_ids,
                derived_raw_root=derived_raw_root,
                media_root=media_root,
            )
            output_inventory = _validate_output_inventory(
                contract,
                db_path=db_path,
                derived_raw_root=derived_raw_root,
                media_root=media_root,
            )
            critical = _verify_critical_unchanged(
                contract,
                db_path=db_path,
                target_ids=target_ids,
                derived_raw_root=derived_raw_root,
                media_root=media_root,
                authorized_content_ids=set(chain_completed) | intended,
            )
            receipt = {
                "version": 1,
                "batch_index": index,
                "content_ids": content_ids,
                "content_ids_sha256": replay._object_sha256(content_ids),
                "intent_sha256": replay._file_sha256(intent_path),
                "recovered_content_ids": recovered,
                "processed_content_ids": remaining,
                "interrupted_slot_recovery": interrupted_slot_recovery,
                "raw_application_recovery": raw_application_recovery,
                "output_cleanup": pending_output_cleanup,
                "apply": applied,
                "artifacts": artifacts,
                "output_inventory": output_inventory,
                "critical_unchanged": critical,
                "after_database": _database_file_snapshot(db_path),
                "elapsed_seconds": round(time.monotonic() - started, 6),
                "disk": _disk_gates(disk_paths, min_free_bytes=min_free_bytes),
            }
            _atomic_json(receipt_path, receipt)
            created_receipts.append(receipt)
            receipts.append(receipt)
            chain_completed.update(content_ids)
            plan = _advance_plan(plan, completed_ids=content_ids, db_path=db_path)
            pending = None
            interrupted_slot_recovery = []
            raw_application_recovery = []
            pending_output_cleanup = None
            index += 1

        final_plan = replay.build_replay_plan(db_path=db_path, contract=replay_contract)
        final_ready = {item.content_id for item in final_plan.candidates} & target
        status = "partial" if final_ready else "succeeded"
        if status == "partial" and os.path.lexists(paths.completion):
            raise BatchReplayError("批处理未完成但 completion 已存在")
        final_intents, final_receipts = _load_batch_records(paths)
        final_chain_completed = _validate_batch_chain(
            paths,
            contract=contract,
            intents=final_intents,
            receipts=final_receipts,
            db_path=db_path,
            derived_raw_root=derived_raw_root,
            media_root=media_root,
        )
        if status == "succeeded" and (
            len(final_intents) != len(final_receipts)
            or final_chain_completed != target
        ):
            raise BatchReplayError("完成前批次 intent/receipt 链未覆盖全部目标")
        receipts = list(final_receipts)
        batch_chain: Mapping[str, Any] | None = None
        receipt_evidence: Mapping[str, Any] | None = None
        if status == "succeeded":
            batch_chain = _batch_chain_evidence(
                paths,
                intents=final_intents,
                receipts=final_receipts,
            )
            receipt_evidence = _validate_receipt_evidence_at_completion(
                paths,
                contract=contract,
                receipts=final_receipts,
                db_path=db_path,
                derived_raw_root=derived_raw_root,
                media_root=media_root,
            )
        result: dict[str, Any] = {
            "status": status,
            "target_count": len(target_ids),
            "completed": len(target - final_ready),
            "remaining": len(final_ready),
            "receipts_total": len(receipts),
            "receipts_created": len(created_receipts),
            "run_root": str(paths.run_root),
            "database": _database_file_snapshot(db_path),
            "provider_calls": 0,
        }
        if status == "succeeded":
            if batch_chain is None or receipt_evidence is None:
                raise BatchReplayError("完成前缺少批次证据链")
            result["completion"] = _verify_complete(
                contract,
                db_path=db_path,
                plan=final_plan,
                derived_raw_root=derived_raw_root,
                media_root=media_root,
            )
            completion_record = {
                "version": 1,
                "status": "succeeded",
                "target_count": len(target_ids),
                "receipts_total": len(receipts),
                "run_root": str(paths.run_root),
                "database": result["database"],
                "contract_sha256": replay._file_sha256(paths.contract),
                "batch_chain": batch_chain,
                "receipt_evidence": receipt_evidence,
                "completion": result["completion"],
            }
            result["completion_record_sha256"] = _atomic_json(
                paths.completion, completion_record
            )
        return result


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--derived-raw-root", type=Path, required=True)
    parser.add_argument("--media-root", type=Path, required=True)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--canary-size", type=int, default=DEFAULT_CANARY_SIZE)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--min-free-bytes", type=int, default=DEFAULT_MIN_FREE_BYTES)
    parser.add_argument("--max-batches", type=int)
    values = parser.parse_args(argv)
    try:
        output = run_batches(
            db_path=values.db,
            derived_raw_root=values.derived_raw_root,
            media_root=values.media_root,
            run_root=values.run_root,
            canary_size=values.canary_size,
            batch_size=values.batch_size,
            min_free_bytes=values.min_free_bytes,
            max_batches=values.max_batches,
        )
    except (BatchReplayError, replay.CacheReplayError) as exc:
        print(json.dumps({"status": "blocked", "error": str(exc)}, ensure_ascii=False))
        return 2
    print(json.dumps(output, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if output["status"] == "succeeded" else 3


if __name__ == "__main__":
    raise SystemExit(main())
