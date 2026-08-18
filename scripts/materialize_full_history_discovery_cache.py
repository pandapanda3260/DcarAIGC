#!/usr/bin/env python3
"""Materialize cached full-history discovery detail without provider calls.

The command is deliberately clone-first.  ``--apply`` refuses the formal
database and requires isolated raw/artifact roots.  It replays only the exact
already-captured account-post pages and never creates or consumes a provider
budget.
"""

from __future__ import annotations

import argparse
import functools
import gc
import hashlib
import json
import os
import socket
import sqlite3
import stat
import urllib.request
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence
from unittest.mock import patch

from v8 import providers
from v8.media import is_supported_media_url
from v8.storage import PROJECT_ROOT, is_formal_database_path


FORMAL_DB = PROJECT_ROOT / "app/data/dcar_insight.sqlite3"
DEFAULT_MANIFEST = (
    PROJECT_ROOT
    / "data/cache/v8/range_backfill/fh-91564efd25-discover-01.contents.json"
)
DEFAULT_RAW_ROOT = PROJECT_ROOT / "data/cache/v8/raw_responses"
DEFAULT_MEDIA_ROOT = PROJECT_ROOT / "data/cache/v8/media"


class CacheReplayError(RuntimeError):
    pass


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _object_sha256(value: Any) -> str:
    return _sha256_bytes(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    )


@dataclass(frozen=True)
class ReplayContract:
    manifest_path: Path
    raw_root: Path
    window_key_like: str
    metrics_window_key: str
    manifest_sha256: str | None = None
    expected_manifest_count: int | None = None
    expected_raw_pages: int | None = None
    expected_raw_items: int | None = None
    expected_pair_sha256: str | None = None
    expected_authoritative_view_items: int | None = None


PRODUCTION_CONTRACT = ReplayContract(
    manifest_path=DEFAULT_MANIFEST,
    raw_root=DEFAULT_RAW_ROOT,
    window_key_like="range:2010-01-01:20260807T230000:%",
    metrics_window_key="2026-08-07",
    manifest_sha256=(
        "1320d6fde3f2b046353585ddfb770607899623cea92cb8fc98b7254a7c93cc00"
    ),
    expected_manifest_count=61_197,
    expected_raw_pages=3_155,
    expected_raw_items=61_197,
    expected_pair_sha256=(
        "efc1693151553cb2c7fa829a312d806faaa5d82e0fa8f325d859485fd52e63ca"
    ),
    expected_authoritative_view_items=0,
)


@dataclass(frozen=True)
class RawItem:
    account_id: int
    platform: str
    raw_response_id: int
    operation: str
    value: Mapping[str, Any]


@dataclass(frozen=True)
class Candidate:
    content_id: int
    account_id: int
    platform: str
    platform_content_id: str
    source_group: str
    account_uid: str
    raw_response_id: int
    operation: str
    item: Mapping[str, Any]
    mode: str


@dataclass(frozen=True)
class ReplayPlan:
    summary: Mapping[str, Any]
    candidates: tuple[Candidate, ...]
    metrics_window_key: str
    history_content_ids: tuple[int, ...]
    already_materialized_ids: tuple[int, ...]


def _require_private_regular_file(path: Path, *, label: str) -> os.stat_result:
    try:
        metadata = path.lstat()
    except FileNotFoundError as exc:
        raise CacheReplayError(f"{label}不存在：{path}") from exc
    if not stat.S_ISREG(metadata.st_mode) or path.is_symlink():
        raise CacheReplayError(f"{label}不是普通文件：{path}")
    if metadata.st_nlink != 1:
        raise CacheReplayError(f"{label}存在硬链接：{path}")
    return metadata


def _require_clean_database_snapshot(db_path: Path) -> None:
    _require_private_regular_file(db_path.resolve(), label="数据库快照")
    leftovers = [
        str(Path(f"{db_path.resolve()}{suffix}"))
        for suffix in ("-wal", "-shm", "-journal")
        if os.path.lexists(Path(f"{db_path.resolve()}{suffix}"))
    ]
    if leftovers:
        raise CacheReplayError(
            "数据库快照仍有 SQLite sidecar，必须先冻结并完成 checkpoint："
            + ",".join(leftovers)
        )


def _load_manifest(contract: ReplayContract) -> dict[int, Mapping[str, Any]]:
    path = contract.manifest_path.resolve()
    _require_private_regular_file(path, label="内容清单")
    body = path.read_bytes()
    if contract.manifest_sha256 is not None:
        actual = _sha256_bytes(body)
        if actual != contract.manifest_sha256:
            raise CacheReplayError("内容清单 SHA256 与冻结合同不一致")
    try:
        value = json.loads(body)
    except json.JSONDecodeError as exc:
        raise CacheReplayError("内容清单不是合法 JSON") from exc
    rows = value.get("contents") if isinstance(value, Mapping) else None
    if not isinstance(rows, list):
        raise CacheReplayError("内容清单缺少 contents 数组")
    entries: dict[int, Mapping[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise CacheReplayError("内容清单存在非法行")
        try:
            content_id = int(row["content_id"])
        except (KeyError, TypeError, ValueError) as exc:
            raise CacheReplayError("内容清单存在非法 content_id") from exc
        if content_id in entries:
            raise CacheReplayError("内容清单存在重复 content_id")
        entries[content_id] = dict(row)
    if (
        contract.expected_manifest_count is not None
        and len(entries) != contract.expected_manifest_count
    ):
        raise CacheReplayError("内容清单数量与冻结合同不一致")
    return entries


def _resolve_raw_path(stored: str, raw_root: Path) -> Path:
    candidate = Path(stored)
    path = (candidate if candidate.is_absolute() else PROJECT_ROOT / candidate).resolve()
    root = raw_root.resolve()
    if not path.is_relative_to(root):
        raise CacheReplayError(f"原始响应路径越界：{stored}")
    return path


def _parse_raw_items(
    *, db_path: Path, contract: ReplayContract
) -> tuple[dict[tuple[str, str], RawItem], Mapping[str, Any]]:
    uri = f"file:{db_path.resolve()}?mode=ro&immutable=1"
    with sqlite3.connect(uri, uri=True) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            """
            SELECT fs.id AS slot_id, fs.account_id, pr.id AS raw_response_id,
                   pr.local_path, pr.operation, pr.sha256, pr.byte_size
            FROM fetch_slots fs
            JOIN fetch_attempts fa ON fa.slot_id=fs.id
            JOIN provider_raw_responses pr ON pr.fetch_attempt_id=fa.id
            WHERE fs.content_id IS NULL AND fs.account_id IS NOT NULL
              AND fs.window_key LIKE ?
              AND fs.status='succeeded' AND pr.source='live_applied'
            ORDER BY fs.id, fa.id, pr.id
            """,
            (contract.window_key_like,),
        ).fetchall()
    if contract.expected_raw_pages is not None and len(rows) != contract.expected_raw_pages:
        raise CacheReplayError("成功原始页数与冻结合同不一致")

    raw_items: dict[tuple[str, str], RawItem] = {}
    platform_counts = {"douyin": 0, "xiaohongshu": 0}
    authoritative_view_items = 0
    for row in rows:
        operation = str(row["operation"])
        if operation == "douyin_user_posts":
            platform = "douyin"
        elif operation == "xiaohongshu_user_posts":
            platform = "xiaohongshu"
        else:
            raise CacheReplayError(f"原始响应包含未知 operation：{operation}")
        path = _resolve_raw_path(str(row["local_path"]), contract.raw_root)
        metadata = _require_private_regular_file(path, label="原始响应")
        body = path.read_bytes()
        if metadata.st_size != int(row["byte_size"]):
            raise CacheReplayError(f"原始响应字节数漂移：{path}")
        if _sha256_bytes(body) != str(row["sha256"]):
            raise CacheReplayError(f"原始响应 SHA256 漂移：{path}")
        try:
            payload = json.loads(body)
            if platform == "douyin":
                page = providers._parse_douyin_discovery_payload(payload).data
            else:
                page = providers._parse_xhs_discovery_payload(payload)
        except Exception as exc:
            raise CacheReplayError(f"原始响应解析失败：{path}") from exc
        items = page.get("items") if isinstance(page, Mapping) else None
        if not isinstance(items, list):
            raise CacheReplayError("原始响应没有合法 items 数组")
        for value in items:
            if not isinstance(value, Mapping):
                raise CacheReplayError("原始响应存在非法内容行")
            item = dict(value)
            published_at = providers._timestamp_iso(item.get("published_at"))
            if item.get("published_at") is not None and published_at is None:
                raise CacheReplayError("原始响应发布时间无法规范化")
            if published_at is not None:
                item["published_at"] = published_at
            item_platform = str(item.get("platform") or "")
            platform_content_id = str(item.get("platform_content_id") or "")
            if item_platform != platform or not platform_content_id:
                raise CacheReplayError("原始响应内容身份漂移")
            key = (platform, platform_content_id)
            if key in raw_items:
                raise CacheReplayError("原始响应出现重复平台内容身份")
            raw_items[key] = RawItem(
                account_id=int(row["account_id"]),
                platform=platform,
                raw_response_id=int(row["raw_response_id"]),
                operation=operation,
                value=item,
            )
            platform_counts[platform] += 1
            metrics = item.get("metrics")
            view_count = metrics.get("view_count") if isinstance(metrics, Mapping) else None
            authoritative_view_items += int(
                isinstance(view_count, (int, float))
                and not isinstance(view_count, bool)
                and view_count > 0
            )
    if contract.expected_raw_items is not None and len(raw_items) != contract.expected_raw_items:
        raise CacheReplayError("原始响应内容数与冻结合同不一致")
    pairs = [list(pair) for pair in sorted(raw_items)]
    pair_sha = _object_sha256(pairs)
    if contract.expected_pair_sha256 is not None and pair_sha != contract.expected_pair_sha256:
        raise CacheReplayError("原始响应平台内容集合与冻结合同不一致")
    if (
        contract.expected_authoritative_view_items is not None
        and authoritative_view_items != contract.expected_authoritative_view_items
    ):
        raise CacheReplayError("原始响应权威播放量数量与冻结合同不一致")
    return raw_items, {
        "raw_pages": len(rows),
        "raw_items": len(raw_items),
        "platform_counts": platform_counts,
        "pair_sha256": pair_sha,
        "authoritative_view_items": authoritative_view_items,
    }


def _chunks(values: Sequence[int], size: int = 500) -> Iterator[Sequence[int]]:
    for offset in range(0, len(values), size):
        yield values[offset : offset + size]


def build_replay_plan(
    *, db_path: Path, contract: ReplayContract = PRODUCTION_CONTRACT
) -> ReplayPlan:
    _require_clean_database_snapshot(db_path)
    database_sha_before = _file_sha256(db_path.resolve())
    entries = _load_manifest(contract)
    raw_items, raw_summary = _parse_raw_items(db_path=db_path, contract=contract)
    rows: list[sqlite3.Row] = []
    uri = f"file:{db_path.resolve()}?mode=ro&immutable=1"
    with sqlite3.connect(uri, uri=True) as connection:
        connection.row_factory = sqlite3.Row
        for batch in _chunks(sorted(entries)):
            placeholders = ",".join("?" for _ in batch)
            rows.extend(
                connection.execute(
                    f"""
                    SELECT c.id,c.account_id,c.platform,c.platform_content_id,
                           c.source_group,c.raw_account_uid,c.published_at,
                           c.title,c.body,
                           EXISTS(
                               SELECT 1 FROM evidence_artifacts ea
                               WHERE ea.content_id=c.id
                                 AND ea.artifact_type='media_source'
                                 AND ea.status='available'
                           ) AS has_media_source,
                           (
                               SELECT fs.status FROM fetch_slots fs
                               WHERE fs.content_id=c.id AND fs.stage='detail'
                                 AND fs.window_key='lifetime'
                               ORDER BY fs.id DESC LIMIT 1
                           ) AS detail_slot_status
                    FROM content_items c WHERE c.id IN ({placeholders})
                    ORDER BY c.id
                    """,
                    tuple(batch),
                ).fetchall()
            )
    if len(rows) != len(entries):
        raise CacheReplayError("内容清单与数据库行数不一致")

    database_pairs: set[tuple[str, str]] = set()
    candidates: list[Candidate] = []
    counts = {
        "history_total": 0,
        "already_materialized": 0,
        "ready": 0,
        "media_only_ready": 0,
        "missing_media_urls": 0,
        "inconsistent_detail_slot": 0,
    }
    by_source_group: dict[str, dict[str, int]] = {}
    by_platform: dict[str, dict[str, int]] = {}
    missing_media_ids: list[int] = []
    inconsistent_ids: list[int] = []
    text_conflict_ids: list[int] = []
    text_expansion_rows: list[list[Any]] = []
    history_ids: list[int] = []
    already_materialized_ids: list[int] = []
    for row in rows:
        content_id = int(row["id"])
        platform = str(row["platform"])
        platform_content_id = str(row["platform_content_id"])
        database_pairs.add((platform, platform_content_id))
        manifest = entries[content_id]
        if str(manifest.get("platform") or "") != platform:
            raise CacheReplayError("内容清单平台与数据库不一致")
        raw = raw_items.get((platform, platform_content_id))
        if raw is None:
            raise CacheReplayError("内容清单存在未被原始响应覆盖的内容")
        if int(row["account_id"]) != raw.account_id:
            raise CacheReplayError("原始响应账号与数据库账号不一致")
        raw_published_at = str(raw.value.get("published_at") or "")
        database_published_at = str(row["published_at"] or "")
        if raw_published_at and raw_published_at != database_published_at:
            raise CacheReplayError("原始响应发布时间与数据库不一致")
        source_group = str(row["source_group"] or "")
        if source_group not in {"history-backfill", "history-archive"}:
            continue
        history_ids.append(content_id)
        counts["history_total"] += 1
        group_counts = by_source_group.setdefault(
            source_group,
            {
                "total": 0,
                "ready": 0,
                "media_only_ready": 0,
                "missing_media_urls": 0,
                "done": 0,
            },
        )
        platform_group = by_platform.setdefault(
            platform,
            {
                "total": 0,
                "ready": 0,
                "media_only_ready": 0,
                "missing_media_urls": 0,
                "done": 0,
            },
        )
        group_counts["total"] += 1
        platform_group["total"] += 1
        if bool(row["has_media_source"]):
            counts["already_materialized"] += 1
            group_counts["done"] += 1
            platform_group["done"] += 1
            already_materialized_ids.append(content_id)
            continue
        media_urls = [
            str(value)
            for value in raw.value.get("media_urls") or []
            if isinstance(value, str) and is_supported_media_url(value)
        ]
        if not media_urls:
            counts["missing_media_urls"] += 1
            group_counts["missing_media_urls"] += 1
            platform_group["missing_media_urls"] += 1
            missing_media_ids.append(content_id)
            continue
        detail_slot_status = str(row["detail_slot_status"] or "")
        if detail_slot_status not in {"", "succeeded", "retryable_failed"}:
            counts["inconsistent_detail_slot"] += 1
            inconsistent_ids.append(content_id)
            continue
        current_title = str(row["title"] or "")
        current_body = str(row["body"] or "")
        raw_title = str(raw.value.get("title") or "")
        raw_body = str(raw.value.get("body") or "")
        title_changes = bool(raw_title and raw_title != current_title)
        body_changes = bool(raw_body and raw_body != current_body)
        title_conflicts = bool(
            title_changes and current_title and current_title not in raw_title
        )
        body_conflicts = bool(
            body_changes and current_body and current_body not in raw_body
        )
        if title_conflicts or body_conflicts:
            text_conflict_ids.append(content_id)
        elif title_changes or body_changes:
            text_expansion_rows.append(
                [
                    content_id,
                    title_changes,
                    body_changes,
                    len(current_title),
                    len(raw_title),
                    len(current_body),
                    len(raw_body),
                ]
            )
        mode = "media_only" if detail_slot_status == "succeeded" else "detail_and_media"
        counts["ready"] += 1
        group_counts["ready"] += 1
        platform_group["ready"] += 1
        if mode == "media_only":
            counts["media_only_ready"] += 1
            group_counts["media_only_ready"] += 1
            platform_group["media_only_ready"] += 1
        candidates.append(
            Candidate(
                content_id=content_id,
                account_id=int(row["account_id"]),
                platform=platform,
                platform_content_id=platform_content_id,
                source_group=source_group,
                account_uid=str(raw.value.get("account_uid") or row["raw_account_uid"] or ""),
                raw_response_id=raw.raw_response_id,
                operation=raw.operation,
                item=raw.value,
                mode=mode,
            )
        )
    if database_pairs != set(raw_items):
        raise CacheReplayError("数据库清单内容集合与原始响应集合不一致")
    database_sha_after = _file_sha256(db_path.resolve())
    if database_sha_before != database_sha_after:
        raise CacheReplayError("数据库快照在计划生成期间发生变化")
    candidates.sort(key=lambda item: item.content_id)
    candidate_ids = [item.content_id for item in candidates]
    summary = {
        "status": (
            "ready"
            if not inconsistent_ids and not text_conflict_ids
            else "blocked"
        ),
        "manifest": {
            "path": str(contract.manifest_path.resolve()),
            "entries": len(entries),
            "file_sha256": _file_sha256(contract.manifest_path.resolve()),
        },
        "raw": raw_summary,
        "history": counts,
        "by_source_group": by_source_group,
        "by_platform": by_platform,
        "candidate_ids_sha256": _object_sha256(candidate_ids),
        "history_ids_sha256": _object_sha256(sorted(history_ids)),
        "already_materialized_ids_sha256": _object_sha256(
            sorted(already_materialized_ids)
        ),
        "missing_media_ids_sha256": _object_sha256(sorted(missing_media_ids)),
        "inconsistent_detail_slot_ids": sorted(inconsistent_ids),
        "content_text_expansion": {
            "candidate_rows_changed": len(text_expansion_rows),
            "title_fields_changed": sum(bool(row[1]) for row in text_expansion_rows),
            "body_fields_changed": sum(bool(row[2]) for row in text_expansion_rows),
            "rows_sha256": _object_sha256(text_expansion_rows),
            "row_fields": [
                "content_id",
                "title_changes",
                "body_changes",
                "current_title_length",
                "raw_title_length",
                "current_body_length",
                "raw_body_length",
            ],
            "conflict_ids": sorted(text_conflict_ids),
            "contract": (
                "existing non-empty title/body must be contained in the cached "
                "raw value before replacement"
            ),
        },
        "provider_calls_planned": 0,
        "provider_budget_required": False,
        "database_snapshot": {
            "path": str(db_path.resolve()),
            "bytes": db_path.stat().st_size,
            "sha256": database_sha_after,
        },
    }
    return ReplayPlan(
        summary=summary,
        candidates=tuple(candidates),
        metrics_window_key=contract.metrics_window_key,
        history_content_ids=tuple(sorted(history_ids)),
        already_materialized_ids=tuple(sorted(already_materialized_ids)),
    )


def _usage_snapshot(
    db_path: Path, *, immutable: bool = False
) -> Mapping[str, Any]:
    immutable_query = "&immutable=1" if immutable else ""
    connection = sqlite3.connect(
        f"file:{db_path.resolve()}?mode=ro{immutable_query}",
        uri=True,
        timeout=30,
    )
    connection.row_factory = sqlite3.Row
    try:
        usage = connection.execute(
            """
            SELECT COUNT(*) AS rows, COALESCE(SUM(billed_requests),0) AS billed,
                   COALESCE(ROUND(SUM(amount),6),0) AS amount,
                   COALESCE(MAX(id),0) AS max_id FROM provider_usage
            """
        ).fetchone()
        budgets = connection.execute(
            """
            SELECT COUNT(*) AS rows, COALESCE(SUM(consumed_requests),0) AS requests,
                   COALESCE(ROUND(SUM(consumed_amount),6),0) AS amount
            FROM provider_budget_batches
            """
        ).fetchone()
    finally:
        connection.close()
    return {"provider_usage": dict(usage), "provider_budgets": dict(budgets)}


@contextmanager
def _zero_network() -> Iterator[None]:
    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        raise CacheReplayError("缓存重放禁止任何网络调用")

    with patch("socket.create_connection", forbidden), patch(
        "socket.getaddrinfo", forbidden
    ), patch.object(socket.socket, "connect", forbidden), patch.object(
        socket.socket, "connect_ex", forbidden
    ), patch.object(socket.socket, "sendto", forbidden), patch.object(
        urllib.request, "urlopen", forbidden
    ):
        yield


@contextmanager
def _derived_only_provider_calls(derived_raw_root: Path) -> Iterator[None]:
    original = providers.execute_content_fetch

    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        raise CacheReplayError("缓存重放触发了真实 provider 入口")

    def guarded_content_fetch(**kwargs: Any) -> Any:
        call = kwargs.get("call")
        if not isinstance(call, functools.partial):
            raise CacheReplayError("缓存重放只允许本地 derived result")
        if call.func is not providers._derived_discovery_result:
            raise CacheReplayError("缓存重放调用了非 derived provider adapter")
        if kwargs.get("provider") != "TikHub" or kwargs.get(
            "adapter_version"
        ) != "tikhub-discovery-derived-v8.1":
            raise CacheReplayError("缓存重放 provider 元数据不符合零付费合同")
        if kwargs.get("stage") != "detail" or kwargs.get("window_key") != "lifetime":
            raise CacheReplayError("缓存重放只允许 detail/lifetime 派生槽")
        if any(
            kwargs.get(key) is not None
            for key in ("budget_id", "task_id", "task_max_amount")
        ):
            raise CacheReplayError("缓存重放不得绑定 provider 预算")
        if Path(kwargs.get("raw_root", "")).resolve() != derived_raw_root.resolve():
            raise CacheReplayError("derived raw 未写入隔离目录")
        return original(**kwargs)

    with patch.object(providers, "execute_content_fetch", guarded_content_fetch), patch.object(
        providers, "execute_account_fetch", forbidden
    ), patch.object(providers, "_request_json", forbidden), patch.object(
        providers, "_douyin_reference_call", forbidden
    ), patch.object(providers, "_douyin_discovery_call", forbidden), patch.object(
        providers, "_xhs_discovery_call", forbidden
    ), patch.object(providers, "_douyin_call", forbidden), patch.object(
        providers, "_xhs_call", forbidden
    ), patch.object(providers, "_load_key", forbidden), patch.object(
        providers, "_budget_for_call", forbidden
    ):
        yield


def _assert_disposable_database(db_path: Path) -> None:
    resolved = db_path.resolve()
    _require_private_regular_file(resolved, label="数据库副本")
    if is_formal_database_path(resolved, formal_database=FORMAL_DB):
        raise CacheReplayError("缓存重放 apply 禁止直接写正式数据库")


def _assert_isolated_output_roots(
    *, db_path: Path, derived_raw_root: Path, media_root: Path
) -> None:
    roots = (derived_raw_root.resolve(), media_root.resolve())
    if (
        roots[0] == roots[1]
        or roots[0].is_relative_to(roots[1])
        or roots[1].is_relative_to(roots[0])
    ):
        raise CacheReplayError("derived raw 与 media 隔离目录不得相同或相互包含")
    canonical_roots = (DEFAULT_RAW_ROOT.resolve(), DEFAULT_MEDIA_ROOT.resolve())
    for root in roots:
        if any(
            root == canonical
            or root.is_relative_to(canonical)
            or canonical.is_relative_to(root)
            for canonical in canonical_roots
        ):
            raise CacheReplayError("隔离输出目录不得指向或包含正式缓存根")
        if root == db_path.resolve() or db_path.resolve().is_relative_to(root):
            raise CacheReplayError("隔离输出目录不得包含数据库副本")


def _finalize_disposable_database(db_path: Path) -> Mapping[str, Any]:
    _assert_disposable_database(db_path)
    gc.collect()
    storage: tuple[tuple[Any, ...], str] | None = None
    last_error = ""
    for _attempt in range(3):
        connection = sqlite3.connect(db_path, timeout=30)
        try:
            connection.execute("PRAGMA busy_timeout=30000")
            checkpoint = tuple(
                connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            )
            if checkpoint and int(checkpoint[0]) != 0:
                last_error = f"checkpoint 未完成：{checkpoint}"
            else:
                journal_mode = str(
                    connection.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
                ).lower()
                if journal_mode != "delete":
                    last_error = f"journal_mode={journal_mode}"
                else:
                    storage = (checkpoint, journal_mode)
        except sqlite3.OperationalError as exc:
            last_error = str(exc)
        finally:
            connection.close()
        gc.collect()
        if storage is not None:
            break
    if storage is None:
        raise CacheReplayError(f"数据库副本未完成安全收尾：{last_error}")
    checkpoint, journal_mode = storage
    leftovers = [
        str(Path(f"{db_path.resolve()}{suffix}"))
        for suffix in ("-wal", "-shm", "-journal")
        if os.path.lexists(Path(f"{db_path.resolve()}{suffix}"))
    ]
    if leftovers:
        raise CacheReplayError("数据库副本收尾后仍有 sidecar：" + ",".join(leftovers))
    return {"checkpoint": list(checkpoint), "journal_mode": journal_mode}


def apply_replay_plan(
    plan: ReplayPlan,
    *,
    db_path: Path,
    derived_raw_root: Path,
    media_root: Path,
    limit: int | None = None,
    content_ids: Sequence[int] = (),
) -> Mapping[str, Any]:
    _assert_disposable_database(db_path)
    planned_database_sha = str(
        (plan.summary.get("database_snapshot") or {}).get("sha256") or ""
    )
    if (
        not planned_database_sha
        or _file_sha256(db_path.resolve()) != planned_database_sha
    ):
        raise CacheReplayError("数据库副本在计划与执行之间发生变化")
    if plan.summary.get("status") != "ready":
        raise CacheReplayError("缓存重放计划存在不一致项，禁止 apply")
    requested = {int(value) for value in content_ids}
    known = set(plan.history_content_ids)
    missing_requested = sorted(requested - known)
    if missing_requested:
        raise CacheReplayError(f"指定内容不在待处理集合：{missing_requested[:10]}")
    selected = [
        item for item in plan.candidates if not requested or item.content_id in requested
    ]
    already_materialized_requested = sorted(
        requested & set(plan.already_materialized_ids)
    )
    if limit is not None:
        if limit <= 0:
            raise CacheReplayError("limit 必须为正整数")
        selected = selected[:limit]
    derived_raw_root = derived_raw_root.resolve()
    media_root = media_root.resolve()
    _assert_isolated_output_roots(
        db_path=db_path,
        derived_raw_root=derived_raw_root,
        media_root=media_root,
    )
    for root in (derived_raw_root, media_root):
        if root.exists() and (root.is_symlink() or not root.is_dir()):
            raise CacheReplayError(f"隔离输出根目录不安全：{root}")
        root.mkdir(parents=True, exist_ok=True)

    before = _usage_snapshot(db_path)
    results: list[Mapping[str, Any]] = []
    failure: BaseException | None = None
    try:
        with _zero_network(), _derived_only_provider_calls(derived_raw_root):
            for candidate in selected:
                result = providers._materialize_discovery_stages(
                    content_id=candidate.content_id,
                    item=candidate.item,
                    account_uid=candidate.account_uid,
                    metrics_window_key=plan.metrics_window_key,
                    discovery_operation=candidate.operation,
                    source_raw_response_id=candidate.raw_response_id,
                    db_path=db_path,
                    materialize_detail=True,
                    materialize_metrics=False,
                    derived_raw_root=derived_raw_root,
                    media_root=media_root,
                )
                if result.get("failed") or "detail" not in {
                    *result.get("created", []),
                    *result.get("replayed", []),
                    *result.get("already_succeeded", []),
                }:
                    raise CacheReplayError(
                        f"内容 {candidate.content_id} 缓存详情物化失败：{result}"
                    )
                results.append(
                    {
                        "content_id": candidate.content_id,
                        **result,
                        "mode": candidate.mode,
                    }
                )
        after = _usage_snapshot(db_path)
        if before != after:
            raise CacheReplayError("缓存重放意外改变了 provider 用量或预算")
    except BaseException as exc:
        failure = exc.with_traceback(None)
    if failure is not None:
        try:
            _finalize_disposable_database(db_path)
        except Exception as finalize_error:
            raise CacheReplayError(
                f"缓存重放失败且副本收尾失败：{failure}; {finalize_error}"
            ) from failure
        raise failure
    storage = _finalize_disposable_database(db_path)

    processed_ids = [int(item["content_id"]) for item in results]
    return {
        "status": "succeeded",
        "processed": len(results),
        "already_materialized_requested": already_materialized_requested,
        "processed_ids_sha256": _object_sha256(processed_ids),
        "provider_calls": 0,
        "provider_usage_before": before,
        "provider_usage_after": after,
        "storage": storage,
        "derived_raw_root": str(derived_raw_root),
        "media_root": str(media_root),
        "results": results,
    }


def _write_output(path: Path | None, value: Mapping[str, Any]) -> None:
    rendered = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if path is not None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered, encoding="utf-8")
    print(rendered, end="")


def _validate_output_path(path: Path | None, *, db_path: Path) -> None:
    if path is None:
        return
    lexical = Path(os.path.abspath(path))
    if os.path.lexists(lexical):
        metadata = lexical.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or lexical.is_symlink()
            or metadata.st_nlink != 1
        ):
            raise CacheReplayError("输出 JSON 现有路径不安全")
    resolved = lexical.resolve()
    database = db_path.resolve()
    if resolved == database or resolved.parent != database.parent:
        raise CacheReplayError("输出 JSON 必须与隔离数据库副本位于同一目录")
    if is_formal_database_path(lexical, formal_database=FORMAL_DB):
        raise CacheReplayError("输出 JSON 禁止覆盖正式数据库")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True, help="只读审计或隔离副本")
    parser.add_argument("--apply", action="store_true", help="仅允许写隔离数据库副本")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--content-id", type=int, action="append", default=[])
    parser.add_argument("--derived-raw-root", type=Path)
    parser.add_argument("--media-root", type=Path)
    parser.add_argument("--output", type=Path)
    values = parser.parse_args(argv)
    safe_output: Path | None = None
    try:
        _assert_disposable_database(values.db)
        _validate_output_path(values.output, db_path=values.db)
        safe_output = values.output
        plan = build_replay_plan(db_path=values.db)
        output: Mapping[str, Any] = {"mode": "dry_run", **dict(plan.summary)}
        if values.apply:
            if values.derived_raw_root is None or values.media_root is None:
                raise CacheReplayError(
                    "apply 必须显式提供 --derived-raw-root 和 --media-root"
                )
            applied = apply_replay_plan(
                plan,
                db_path=values.db,
                derived_raw_root=values.derived_raw_root,
                media_root=values.media_root,
                limit=values.limit,
                content_ids=values.content_id,
            )
            output = {"mode": "apply", "plan": plan.summary, "apply": applied}
        _write_output(safe_output, output)
        return 0
    except CacheReplayError as exc:
        _write_output(safe_output, {"status": "blocked", "error": str(exc)})
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
