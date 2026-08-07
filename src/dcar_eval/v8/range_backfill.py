"""Bounded, resumable Beijing-time range backfill using active TikHub adapters."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from threading import Event
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

from .capture import BudgetBlocked, ProviderResult
from .duplicates import refresh_content_duplicates
from .evaluation import evaluate_content
from .media import process_content_media
from .providers import (
    TIKHUB_PRICE,
    TIKHUB_XHS_PRICE,
    discover_account_content,
    update_content_data,
)
from .storage import (
    BACKFILL_SOURCE_GROUPS,
    DEFAULT_DB,
    HISTORY_ARCHIVE_SOURCE_GROUP,
    HISTORY_BACKFILL_SOURCE_GROUP,
    PROJECT_ROOT,
    connect,
    now_utc,
    transaction,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")
DEFAULT_START = datetime(2026, 7, 20, 0, 0, tzinfo=SHANGHAI)
#: 全量历史模式的区间起点：早于双平台任何账号的可能建号时间，
#: 使发现翻页只受 has_more/max_pages/预算约束，不受时间过滤截断。
FULL_HISTORY_START = datetime(2010, 1, 1, 0, 0, tzinfo=SHANGHAI)
DEFAULT_MAX_AMOUNT = 9.0
STATE_ROOT = PROJECT_ROOT / "data" / "cache" / "v8" / "range_backfill"
#: 长跑阶段每处理多少条内容刷新一次状态文件（可观测且崩溃后有据可查）。
PROGRESS_FLUSH_EVERY = 200
#: 压缩模式下状态/输出里保留的失败明细上限。
COMPACT_FAILURE_LIMIT = 200
#: 评论翻页的单页容量估计（用于报价，非计费口径）：抖音约 20 条/页、
#: 小红书约 10 条/页；1000 条为 paged-comments-v2 采集上限。
COMMENT_PAGE_SIZE = {"douyin": 20, "xiaohongshu": 10}
COMMENT_CAP = 1000
BLOCKING_CODES = {
    "provider_balance_blocked",
    "provider_auth_blocked",
    "budget_blocked",
    "BudgetBlocked",
}


class RangeBackfillError(RuntimeError):
    pass


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise RangeBackfillError("区间时间必须包含时区")
    return value.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return _utc(value).isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_datetime(value: str) -> datetime:
    text = value.strip()
    if not text:
        raise RangeBackfillError("时间不能为空")
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SHANGHAI)
    return parsed


def task_id_for(start: datetime, end: datetime) -> str:
    local_start = start.astimezone(SHANGHAI).strftime("%Y%m%dT%H%M%S")
    local_end = end.astimezone(SHANGHAI).strftime("%Y%m%dT%H%M%S")
    return f"two-week-backfill-{local_start}-{local_end}-bjt"


def _cursor_digest(cursor: Any) -> str:
    payload = json.dumps(
        cursor, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def discovery_window_key(
    *, start: datetime, end: datetime, platform: str, cursor: Any
) -> str:
    return (
        f"range:{start.astimezone(SHANGHAI).date().isoformat()}:"
        f"{end.astimezone(SHANGHAI).strftime('%Y%m%dT%H%M%S')}:"
        f"{platform}:{_cursor_digest(cursor)}"
    )


def _task_usage(task_id: str, *, db_path: Path) -> Dict[str, Any]:
    with connect(db_path) as connection:
        row = connection.execute(
            """
            SELECT COALESCE(SUM(request_attempts),0) attempts,
                   COALESCE(SUM(billed_requests),0) billed_requests,
                   COALESCE(SUM(amount),0) amount
            FROM provider_usage WHERE task_id=?
            """,
            (task_id,),
        ).fetchone()
    return {
        "attempts": int(row["attempts"]),
        "billed_requests": int(row["billed_requests"]),
        "amount": round(float(row["amount"]), 6),
        "currency": "USD",
    }


def _write_state(
    *,
    task_id: str,
    start: datetime,
    end: datetime,
    max_amount: float,
    phase: str,
    status: str,
    details: Mapping[str, Any],
    state_root: Path,
) -> Path:
    state_root.mkdir(parents=True, exist_ok=True)
    target = state_root / f"{task_id}.json"
    existing: Dict[str, Any] = {}
    if target.is_file():
        loaded = json.loads(target.read_text(encoding="utf-8"))
        if isinstance(loaded, dict):
            existing = loaded
        contract = (
            existing.get("start"), existing.get("end"), existing.get("max_amount")
        )
        expected = (_iso(start), _iso(end), max_amount)
        if contract != expected:
            raise RangeBackfillError("已有补抓状态文件与本次区间或预算不一致")
    captured_at = now_utc()
    value = {
        **existing,
        "task_id": task_id,
        "timezone": "Asia/Shanghai",
        "start": _iso(start),
        "end": _iso(end),
        "max_amount": max_amount,
        "phase": phase,
        "status": status,
        "details": dict(details),
        "created_at": existing.get("created_at") or captured_at,
        "updated_at": captured_at,
    }
    temporary = target.with_name(f".{target.name}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(target)
    return target


def _enabled_identities(
    *, db_path: Path, platforms: Optional[Sequence[str]], account_limit: Optional[int]
) -> List[Dict[str, Any]]:
    parameters: List[Any] = []
    where = ["a.enabled=1", "api.platform IN ('douyin','xiaohongshu')"]
    if platforms:
        selected = sorted(set(platforms))
        invalid = set(selected) - {"douyin", "xiaohongshu"}
        if invalid:
            raise RangeBackfillError(f"不支持的平台：{','.join(sorted(invalid))}")
        where.append(f"api.platform IN ({','.join('?' for _ in selected)})")
        parameters.extend(selected)
    sql = f"""
        SELECT api.account_id, api.platform, api.uid, api.nickname
        FROM account_platform_identities api
        JOIN accounts a ON a.id=api.account_id
        WHERE {' AND '.join(where)}
        ORDER BY api.platform, api.account_id
    """
    if account_limit is not None:
        if account_limit <= 0:
            raise RangeBackfillError("account_limit 必须为正数")
        sql += " LIMIT ?"
        parameters.append(account_limit)
    with connect(db_path) as connection:
        rows = connection.execute(sql, parameters).fetchall()
    return [dict(row) for row in rows]


def tag_history_scopes(
    *,
    start: datetime,
    end: datetime,
    archive_before: datetime,
    db_path: Path = DEFAULT_DB,
    platforms: Optional[Sequence[str]] = None,
    apply_changes: bool = False,
) -> Dict[str, Any]:
    """把回溯批量入库的内容按证据窗切成两组标记，保护每日闸门与增量评估。

    published < archive_before → ``history-archive``（仅入库+指标，永不自动评估）；
    archive_before ≤ published ≤ end → ``history-backfill``（待 local-evidence
    完成媒体+评估后清除标记回归常规链路）。只触碰从未产生过任何评估版本、
    且 source_group 为空的启用账号内容——既有已评估语料的生命周期不受影响。
    幂等：重复执行不会改写已标记或已评估的行。
    """

    start_utc, end_utc = _utc(start), _utc(end)
    archive_utc = _utc(archive_before)
    if start_utc >= end_utc:
        raise RangeBackfillError("标记区间开始时间必须早于结束时间")
    selected_platforms = sorted(set(platforms or ("douyin", "xiaohongshu")))
    invalid = set(selected_platforms) - {"douyin", "xiaohongshu"}
    if invalid:
        raise RangeBackfillError(f"不支持的平台：{','.join(sorted(invalid))}")
    platform_clause = ",".join("?" for _ in selected_platforms)
    segments = []
    if archive_utc > start_utc:
        segments.append(
            (
                HISTORY_ARCHIVE_SOURCE_GROUP,
                _iso(start),
                "c.published_at>=? AND c.published_at<?",
                (_iso(start), _iso(min(archive_before, end))),
            )
        )
    if archive_utc <= end_utc:
        segments.append(
            (
                HISTORY_BACKFILL_SOURCE_GROUP,
                _iso(max(archive_before, start)),
                "c.published_at>=? AND c.published_at<=?",
                (_iso(max(archive_before, start)), _iso(end)),
            )
        )
    captured_at = now_utc()
    output: Dict[str, Any] = {
        "status": "succeeded" if apply_changes else "dry_run",
        "start": _iso(start),
        "end": _iso(end),
        "archive_before": _iso(archive_before),
        "platforms": selected_platforms,
        "segments": {},
    }
    with connect(db_path) as connection, transaction(connection):
        for tag, _, published_clause, published_params in segments:
            candidate_sql = f"""
                SELECT c.id FROM content_items c
                JOIN accounts a ON a.id=c.account_id
                WHERE a.enabled=1
                  AND c.platform IN ({platform_clause})
                  AND COALESCE(c.source_group,'')=''
                  AND {published_clause}
                  AND NOT EXISTS (
                    SELECT 1 FROM evaluation_versions ev WHERE ev.content_id=c.id
                  )
            """
            parameters = [*selected_platforms, *published_params]
            candidates = [
                int(row["id"])
                for row in connection.execute(candidate_sql, parameters).fetchall()
            ]
            applied = 0
            if apply_changes and candidates:
                connection.executemany(
                    "UPDATE content_items SET source_group=?, updated_at=? WHERE id=?",
                    [(tag, captured_at, content_id) for content_id in candidates],
                )
                applied = len(candidates)
            already = connection.execute(
                f"""
                SELECT COUNT(*) FROM content_items c
                WHERE c.platform IN ({platform_clause}) AND c.source_group=?
                """,
                (*selected_platforms, tag),
            ).fetchone()[0]
            output["segments"][tag] = {
                "candidates": len(candidates),
                "applied": applied,
                "tagged_total": int(already),
            }
    return output


def _compact_discovery_results(
    results: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    compacted: List[Dict[str, Any]] = []
    for item in results:
        pages = list(item.get("pages") or [])
        failed = [
            page for page in pages if page.get("status") in {"failed", "partial"}
        ]
        compacted.append(
            {
                **{
                    key: item.get(key)
                    for key in ("account_id", "platform", "uid", "nickname")
                },
                "pages_processed": len(pages),
                "inserted": sum(int(page.get("inserted") or 0) for page in pages),
                "updated": sum(int(page.get("updated") or 0) for page in pages),
                "failed_pages": failed[:COMPACT_FAILURE_LIMIT],
                "failed_pages_truncated": len(failed) > COMPACT_FAILURE_LIMIT,
                "stopped_reason": item.get("stopped_reason"),
            }
        )
    return compacted


def _compact_content_results(
    results: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    status_counts: Dict[str, int] = {}
    failures: List[Dict[str, Any]] = []
    total_failures = 0
    cost = 0.0
    for item in results:
        status = str(item.get("status") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
        cost += float(item.get("provider_cost") or 0.0)
        failed_stages = [
            stage
            for stage in item.get("stages") or []
            if stage.get("status") == "failed"
        ]
        if failed_stages:
            total_failures += 1
            if len(failures) < COMPACT_FAILURE_LIMIT:
                failures.append(
                    {
                        "content_id": item.get("content_id"),
                        "stages": [
                            {
                                "stage": stage.get("stage"),
                                "error_code": stage.get("error_code"),
                                "message": str(stage.get("message") or "")[:200],
                            }
                            for stage in failed_stages
                        ],
                    }
                )
    return {
        "status_counts": status_counts,
        "provider_cost": round(cost, 6),
        "failed_contents": total_failures,
        "failures": failures,
        "failures_truncated": total_failures > len(failures),
    }


def _process_content_batch(
    content_ids: Sequence[int],
    *,
    processor: Callable[[int], Dict[str, Any]],
    workers: int,
    flush: Optional[Callable[[List[Dict[str, Any]]], None]] = None,
    flush_every: int = PROGRESS_FLUSH_EVERY,
) -> tuple[List[Dict[str, Any]], Optional[str]]:
    """按条隔离异常地跑批：串行或有界并发，命中熔断码后停止调度新条目。"""

    if workers < 1 or workers > 8:
        raise RangeBackfillError("workers 必须在 1 到 8 之间")
    stop = Event()
    blocking_holder: List[str] = []

    def guarded(content_id: int) -> Dict[str, Any]:
        if stop.is_set():
            return {
                "content_id": content_id,
                "status": "skipped_after_block",
                "stages": [],
            }
        try:
            result = processor(content_id)
        except Exception as exc:  # noqa: BLE001 - 长跑批处理需按条隔离异常
            result = {
                "content_id": content_id,
                "status": "partial",
                "stages": [
                    {
                        "stage": "unhandled",
                        "status": "failed",
                        "error_code": getattr(
                            exc, "error_code", type(exc).__name__
                        ),
                        "message": str(exc)[:500],
                    }
                ],
            }
        blocking = next(
            (
                str(stage.get("error_code"))
                for stage in result.get("stages", [])
                if stage.get("error_code") in BLOCKING_CODES
            ),
            None,
        )
        if blocking:
            blocking_holder.append(blocking)
            stop.set()
        return result

    results: List[Dict[str, Any]] = []
    if workers == 1:
        for content_id in content_ids:
            results.append(guarded(content_id))
            if flush is not None and len(results) % flush_every == 0:
                flush(results)
            if stop.is_set():
                break
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for result in pool.map(guarded, content_ids):
                results.append(result)
                if flush is not None and len(results) % flush_every == 0:
                    flush(results)
    return results, (blocking_holder[0] if blocking_holder else None)


def run_discovery_backfill(
    *,
    start: datetime,
    end: datetime,
    task_id: str,
    max_amount: float,
    db_path: Path = DEFAULT_DB,
    platforms: Optional[Sequence[str]] = None,
    account_limit: Optional[int] = None,
    max_pages_per_account: int = 20,
    call_override: Optional[
        Callable[[str, Mapping[str, Any]], ProviderResult]
    ] = None,
    state_root: Path = STATE_ROOT,
    archive_before: Optional[datetime] = None,
    workers: int = 1,
    compact: bool = False,
) -> Dict[str, Any]:
    start_utc, end_utc = _utc(start), _utc(end)
    if start_utc >= end_utc:
        raise RangeBackfillError("补抓开始时间必须早于结束时间")
    if max_amount <= 0 or max_pages_per_account <= 0:
        raise RangeBackfillError("预算与每账号页数上限必须为正数")
    if workers < 1 or workers > 8:
        raise RangeBackfillError("workers 必须在 1 到 8 之间")
    if archive_before is not None:
        _utc(archive_before)
    identities = _enabled_identities(
        db_path=db_path, platforms=platforms, account_limit=account_limit
    )
    blocking_stop = Event()

    def discover_identity(identity: Mapping[str, Any]) -> Dict[str, Any]:
        cursor: Any = None
        seen_cursors: set[str] = set()
        pages: List[Dict[str, Any]] = []
        local_stop: Optional[str] = None
        for page_number in range(1, max_pages_per_account + 1):
            if blocking_stop.is_set():
                break
            cursor_key = _cursor_digest(cursor)
            if cursor_key in seen_cursors:
                local_stop = "cursor_repeated"
                break
            seen_cursors.add(cursor_key)
            window_key = discovery_window_key(
                start=start, end=end, platform=str(identity["platform"]), cursor=cursor
            )
            try:
                page = discover_account_content(
                    int(identity["account_id"]), str(identity["platform"]),
                    str(identity["uid"]), as_of=end.astimezone(SHANGHAI).date(),
                    cursor=cursor, window_key=window_key,
                    published_start=start_utc, published_end=end_utc,
                    task_id=task_id, task_max_amount=max_amount,
                    db_path=db_path, call_override=call_override,
                )
            except Exception as exc:
                code = getattr(exc, "error_code", type(exc).__name__)
                pages.append(
                    {
                        "page": page_number, "status": "failed",
                        "error_code": code, "message": str(exc),
                    }
                )
                if code in BLOCKING_CODES or isinstance(exc, BudgetBlocked):
                    local_stop = str(code)
                    blocking_stop.set()
                break
            pages.append({"page": page_number, **page})
            published_values = [
                datetime.fromisoformat(value.replace("Z", "+00:00"))
                for value in page.get("page_published_at") or []
            ]
            if published_values and min(published_values) <= start_utc:
                break
            next_cursor = page.get("next_cursor")
            if not page.get("has_more") or next_cursor in (None, ""):
                break
            cursor = next_cursor
        return {**identity, "pages": pages, "stopped_reason": local_stop}

    results: List[Dict[str, Any]] = []
    if workers == 1:
        for identity in identities:
            results.append(discover_identity(identity))
            if blocking_stop.is_set():
                break
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            results = [
                item
                for item in pool.map(discover_identity, identities)
                if item["pages"] or not blocking_stop.is_set()
            ]
    stopped_reason = next(
        (
            item.get("stopped_reason")
            for item in results
            if item.get("stopped_reason") in BLOCKING_CODES
        ),
        next(
            (
                item.get("stopped_reason")
                for item in results
                if item.get("stopped_reason")
            ),
            None,
        ),
    )
    usage = _task_usage(task_id, db_path=db_path)
    failed_pages = sum(
        page.get("status") in {"failed", "partial"}
        for item in results for page in item["pages"]
    )
    status = (
        "blocked"
        if stopped_reason in BLOCKING_CODES
        else "partial"
        if failed_pages
        else "succeeded"
    )
    output = {
        "task_id": task_id,
        "status": status,
        "start": _iso(start),
        "end": _iso(end),
        "accounts_considered": len(identities),
        "accounts_processed": sum(1 for item in results if item["pages"]),
        "pages_processed": sum(len(item["pages"]) for item in results),
        "failed_pages": failed_pages,
        "inserted": sum(
            int(page.get("inserted") or 0)
            for item in results for page in item["pages"]
        ),
        "updated": sum(
            int(page.get("updated") or 0)
            for item in results for page in item["pages"]
        ),
        "stopped_reason": stopped_reason,
        "usage": usage,
        "results": _compact_discovery_results(results) if compact else results,
    }
    if archive_before is not None:
        # 无论本次发现是否被熔断，已入库的内容都必须立即分组标记，
        # 否则当晚的媒体截止闸门与增量评估会被批量入库冲垮。
        output["history_scopes"] = tag_history_scopes(
            start=start, end=end, archive_before=archive_before,
            db_path=db_path, platforms=platforms, apply_changes=True,
        )
    state_path = _write_state(
        task_id=task_id, start=start, end=end, max_amount=max_amount,
        phase="discovery", status=status, details=output, state_root=state_root,
    )
    output["state_path"] = str(state_path)
    return output


def pending_content_ids(
    *, start: datetime, end: datetime, as_of: datetime, db_path: Path,
    limit: Optional[int] = None,
    platforms: Optional[Sequence[str]] = None,
    stages: Optional[Sequence[str]] = None,
    history_only: bool = False,
) -> List[int]:
    selected_platforms = sorted(set(platforms or ("douyin", "xiaohongshu")))
    invalid_platforms = set(selected_platforms) - {"douyin", "xiaohongshu"}
    if invalid_platforms:
        raise RangeBackfillError(
            f"不支持的平台：{','.join(sorted(invalid_platforms))}"
        )
    selected_stages = list(dict.fromkeys(stages or ("detail", "metrics", "comments")))
    invalid_stages = set(selected_stages) - {"detail", "metrics", "comments"}
    if invalid_stages or not selected_stages:
        raise RangeBackfillError("stages 必须包含 detail、metrics、comments 中至少一项")
    day_key = as_of.astimezone(SHANGHAI).date().isoformat()
    iso = as_of.astimezone(SHANGHAI).date().isocalendar()
    week_key = f"{iso.year}-W{iso.week:02d}"
    window_parameters: List[Any] = []
    missing_clauses: List[str] = []
    if "detail" in selected_stages:
        missing_clauses.append(
            """NOT EXISTS (
              SELECT 1 FROM fetch_slots f WHERE f.content_id=c.id
                AND f.stage='detail' AND f.window_key='lifetime'
                AND f.status='succeeded'
            )"""
        )
    if "metrics" in selected_stages:
        missing_clauses.append(
            """NOT EXISTS (
              SELECT 1 FROM fetch_slots f WHERE f.content_id=c.id
                AND f.stage='metrics' AND f.window_key=? AND f.status='succeeded'
            )"""
        )
        window_parameters.append(day_key)
    if "comments" in selected_stages:
        missing_clauses.append(
            """NOT EXISTS (
              SELECT 1 FROM comment_capture_runs r WHERE r.content_id=c.id
                AND r.window_key=? AND r.status='succeeded'
            )"""
        )
        window_parameters.append(week_key)
    # history_only：付费阶段只覆盖本次回溯新入库（history-* 标记）的内容，
    # 既有语料的详情/指标/评论由每日调度按既定节奏维护，避免重复计费。
    history_clause = (
        f" AND c.source_group IN ({','.join('?' for _ in BACKFILL_SOURCE_GROUPS)})"
        if history_only
        else ""
    )
    sql = f"""
        SELECT c.id
        FROM content_items c
        JOIN accounts a ON a.id=c.account_id
        WHERE a.enabled=1
          AND c.platform IN ({','.join('?' for _ in selected_platforms)})
          AND c.published_at>=? AND c.published_at<=?{history_clause}
          AND ({' OR '.join(missing_clauses)})
        ORDER BY c.published_at DESC, c.id DESC
    """
    parameters: List[Any] = [
        *selected_platforms, _iso(start), _iso(end),
        *(BACKFILL_SOURCE_GROUPS if history_only else ()),
        *window_parameters,
    ]
    if limit is not None:
        if limit <= 0:
            raise RangeBackfillError("limit 必须为正数")
        sql += " LIMIT ?"
        parameters.append(limit)
    with connect(db_path) as connection:
        rows = connection.execute(sql, parameters).fetchall()
    return [int(row["id"]) for row in rows]


def repair_discovery_placeholder_metrics(
    *,
    start: datetime,
    end: datetime,
    as_of: datetime,
    db_path: Path = DEFAULT_DB,
    platforms: Optional[Sequence[str]] = None,
    limit: Optional[int] = None,
    apply_changes: bool = False,
) -> Dict[str, Any]:
    """Reopen only metrics slots closed by non-authoritative discovery zeros."""

    selected_platforms = sorted(set(platforms or ("douyin", "xiaohongshu")))
    invalid = set(selected_platforms) - {"douyin", "xiaohongshu"}
    if invalid:
        raise RangeBackfillError(f"不支持的平台：{','.join(sorted(invalid))}")
    if limit is not None and limit <= 0:
        raise RangeBackfillError("limit 必须为正数")
    day_key = as_of.astimezone(SHANGHAI).date().isoformat()
    parameters: List[Any] = [
        day_key, _iso(start), _iso(end), *selected_platforms,
    ]
    sql = f"""
        SELECT fs.id slot_id, fs.content_id, fs.attempt_count,
               c.platform, c.link_id, c.published_at,
               ms.id snapshot_id, ms.metadata_json
        FROM fetch_slots fs
        JOIN content_items c ON c.id=fs.content_id
        JOIN content_metric_snapshots ms
          ON ms.content_id=c.id AND ms.window_key=fs.window_key
         AND ms.source=c.platform
        WHERE fs.stage='metrics' AND fs.window_key=? AND fs.status='succeeded'
          AND fs.adapter_version='tikhub-discovery-derived-v8.1'
          AND c.published_at>=? AND c.published_at<?
          AND c.platform IN ({','.join('?' for _ in selected_platforms)})
          AND COALESCE(ms.view_count,0)=0
        ORDER BY c.platform,c.published_at DESC,c.id DESC
    """
    if limit is not None:
        sql += " LIMIT ?"
        parameters.append(limit)
    with connect(db_path) as connection:
        rows = connection.execute(sql, parameters).fetchall()
    counts = {
        platform: sum(str(row["platform"]) == platform for row in rows)
        for platform in selected_platforms
    }
    output = {
        "status": "dry_run" if not apply_changes else "succeeded",
        "window_key": day_key,
        "start": _iso(start),
        "end": _iso(end),
        "candidates": len(rows),
        "by_platform": counts,
        "content_ids": [int(row["content_id"]) for row in rows],
        "applied": 0,
    }
    if not apply_changes or not rows:
        return output

    repaired_at = now_utc()
    with connect(db_path) as connection, transaction(connection):
        for row in rows:
            try:
                metadata = json.loads(str(row["metadata_json"] or "{}"))
            except json.JSONDecodeError:
                metadata = {}
            if not isinstance(metadata, dict):
                metadata = {}
            metadata.update(
                {
                    "exposure_observation": "missing_or_placeholder",
                    "repair_reason": "invalid_discovery_exposure",
                    "repaired_at": repaired_at,
                }
            )
            connection.execute(
                """
                UPDATE content_metric_snapshots
                SET view_count=NULL, status='missing', metadata_json=?
                WHERE id=?
                """,
                (
                    json.dumps(
                        metadata, ensure_ascii=False, sort_keys=True,
                        separators=(",", ":"),
                    ),
                    row["snapshot_id"],
                ),
            )
            connection.execute(
                """
                UPDATE fetch_slots
                SET status='retryable_failed',
                    last_error_code='invalid_discovery_exposure',
                    last_error_message='作品列表曝光为零占位，等待专用统计接口',
                    finished_at=?, updated_at=?
                WHERE id=? AND status='succeeded'
                """,
                (repaired_at, repaired_at, row["slot_id"]),
            )
    output["applied"] = len(rows)
    return output


def run_repaired_metrics_backfill(
    *,
    start: datetime,
    end: datetime,
    as_of: datetime,
    task_id: str,
    max_amount: float,
    db_path: Path = DEFAULT_DB,
    platforms: Optional[Sequence[str]] = None,
    limit: Optional[int] = None,
    call_override: Optional[
        Callable[[str, Mapping[str, Any]], ProviderResult]
    ] = None,
    state_root: Path = STATE_ROOT,
    workers: int = 1,
    compact: bool = False,
) -> Dict[str, Any]:
    selected_platforms = sorted(set(platforms or ("douyin", "xiaohongshu")))
    invalid = set(selected_platforms) - {"douyin", "xiaohongshu"}
    if invalid:
        raise RangeBackfillError(f"不支持的平台：{','.join(sorted(invalid))}")
    if max_amount <= 0 or (limit is not None and limit <= 0):
        raise RangeBackfillError("预算与 limit 必须为正数")
    day_key = as_of.astimezone(SHANGHAI).date().isoformat()
    parameters: List[Any] = [
        day_key, _iso(start), _iso(end), *selected_platforms,
    ]
    sql = f"""
        SELECT fs.content_id
        FROM fetch_slots fs JOIN content_items c ON c.id=fs.content_id
        WHERE fs.stage='metrics' AND fs.window_key=?
          AND fs.status='retryable_failed'
          AND fs.last_error_code='invalid_discovery_exposure'
          AND c.published_at>=? AND c.published_at<?
          AND c.platform IN ({','.join('?' for _ in selected_platforms)})
        ORDER BY c.platform,c.published_at DESC,c.id DESC
    """
    if limit is not None:
        sql += " LIMIT ?"
        parameters.append(limit)
    with connect(db_path) as connection:
        content_ids = [
            int(row["content_id"])
            for row in connection.execute(sql, parameters).fetchall()
        ]

    def flush_progress(partial_results: List[Dict[str, Any]]) -> None:
        _write_state(
            task_id=task_id, start=start, end=end, max_amount=max_amount,
            phase="metrics_repair", status="running",
            details={
                "candidates": len(content_ids),
                "processed": len(partial_results),
                "summary": _compact_content_results(partial_results),
            },
            state_root=state_root,
        )

    results, stopped_reason = _process_content_batch(
        content_ids,
        processor=lambda content_id: update_content_data(
            content_id,
            as_of=as_of.astimezone(SHANGHAI).date(),
            db_path=db_path,
            call_override=call_override,
            stages=["metrics"],
            process_media=False,
            task_id=task_id,
            task_max_amount=max_amount,
        ),
        workers=workers,
        flush=flush_progress,
    )
    usage = _task_usage(task_id, db_path=db_path)
    status = "blocked" if stopped_reason else (
        "partial" if any(item["status"] == "partial" for item in results)
        else "succeeded"
    )
    output = {
        "task_id": task_id,
        "status": status,
        "window_key": day_key,
        "candidates": len(content_ids),
        "processed": sum(
            item["status"] != "skipped_after_block" for item in results
        ),
        "stopped_reason": stopped_reason,
        "usage": usage,
        "results": _compact_content_results(results) if compact else results,
    }
    state_path = _write_state(
        task_id=task_id, start=start, end=end, max_amount=max_amount,
        phase="metrics_repair", status=status, details=output,
        state_root=state_root,
    )
    output["state_path"] = str(state_path)
    return output


def run_content_backfill(
    *,
    start: datetime,
    end: datetime,
    task_id: str,
    max_amount: float,
    db_path: Path = DEFAULT_DB,
    limit: Optional[int] = None,
    platforms: Optional[Sequence[str]] = None,
    stages: Optional[Sequence[str]] = None,
    call_override: Optional[
        Callable[[str, Mapping[str, Any]], ProviderResult]
    ] = None,
    state_root: Path = STATE_ROOT,
    workers: int = 1,
    compact: bool = False,
    history_only: bool = False,
) -> Dict[str, Any]:
    selected_stages = list(dict.fromkeys(stages or ("detail", "metrics", "comments")))
    content_ids = pending_content_ids(
        start=start, end=end, as_of=end, db_path=db_path, limit=limit,
        platforms=platforms, stages=selected_stages, history_only=history_only,
    )

    def flush_progress(partial_results: List[Dict[str, Any]]) -> None:
        _write_state(
            task_id=task_id, start=start, end=end, max_amount=max_amount,
            phase="content", status="running",
            details={
                "candidates": len(content_ids),
                "processed": len(partial_results),
                "stages": selected_stages,
                "summary": _compact_content_results(partial_results),
            },
            state_root=state_root,
        )

    results, stopped_reason = _process_content_batch(
        content_ids,
        processor=lambda content_id: update_content_data(
            content_id, as_of=end.astimezone(SHANGHAI).date(), db_path=db_path,
            call_override=call_override, stages=selected_stages,
            process_media=False, task_id=task_id, task_max_amount=max_amount,
        ),
        workers=workers,
        flush=flush_progress,
    )
    usage = _task_usage(task_id, db_path=db_path)
    status = "blocked" if stopped_reason else (
        "partial" if any(item["status"] == "partial" for item in results)
        else "succeeded"
    )
    output = {
        "task_id": task_id,
        "status": status,
        "history_only": history_only,
        "candidates": len(content_ids),
        "processed": sum(
            item["status"] != "skipped_after_block" for item in results
        ),
        "zero_comment_evidence": {
            "candidates": 0,
            "succeeded": 0,
            "handled_by": "canonical_comments_stage",
        },
        "stopped_reason": stopped_reason,
        "usage": usage,
        "results": _compact_content_results(results) if compact else results,
    }
    state_path = _write_state(
        task_id=task_id, start=start, end=end, max_amount=max_amount,
        phase="content", status=status, details=output, state_root=state_root,
    )
    output["state_path"] = str(state_path)
    return output


def run_local_evidence_backfill(
    *,
    start: datetime,
    end: datetime,
    task_id: str,
    max_amount: float,
    db_path: Path = DEFAULT_DB,
    limit: int = 100,
    state_root: Path = STATE_ROOT,
    platforms: Optional[Sequence[str]] = None,
    tagged_only: bool = False,
    compact: bool = False,
) -> Dict[str, Any]:
    """媒体+评估+感知重复的本地证据推进；history-archive 内容按口径永不进入。

    ``tagged_only=True`` 时只处理 ``history-backfill`` 标记的内容（最新优先），
    单条完整走完媒体/评估/重复后当场清除标记，使其回归常规增量链路；
    媒体可重试失败（如媒体源过期）保留标记，等待重试或付费刷新后再清。
    """

    selected_platforms = sorted(set(platforms or ("douyin", "xiaohongshu")))
    invalid = set(selected_platforms) - {"douyin", "xiaohongshu"}
    if invalid:
        raise RangeBackfillError(f"不支持的平台：{','.join(sorted(invalid))}")
    platform_clause = ",".join("?" for _ in selected_platforms)
    scope_clause = (
        "c.source_group=?"
        if tagged_only
        else "COALESCE(c.source_group,'')!=?"
    )
    scope_parameter = (
        HISTORY_BACKFILL_SOURCE_GROUP if tagged_only else HISTORY_ARCHIVE_SOURCE_GROUP
    )
    with connect(db_path) as connection:
        rows = connection.execute(
            f"""
            SELECT c.id, c.source_group FROM content_items c
            JOIN accounts a ON a.id=c.account_id
            WHERE a.enabled=1 AND c.platform IN ({platform_clause})
              AND {scope_clause}
              AND c.published_at>=? AND c.published_at<=?
            ORDER BY c.published_at DESC, c.id DESC LIMIT ?
            """,
            (*selected_platforms, scope_parameter, _iso(start), _iso(end), limit),
        ).fetchall()
    results: List[Dict[str, Any]] = []
    tags_cleared = 0
    for row in rows:
        content_id = int(row["id"])
        was_tagged = str(row["source_group"] or "") == HISTORY_BACKFILL_SOURCE_GROUP
        try:
            media = process_content_media(content_id, db_path=db_path)
            evaluation = evaluate_content(content_id, db_path=db_path)
            duplicates = refresh_content_duplicates(content_id, db_path=db_path)
            tag_released = False
            if was_tagged:
                with connect(db_path) as connection, transaction(connection):
                    cleared = connection.execute(
                        """
                        UPDATE content_items SET source_group='', updated_at=?
                        WHERE id=? AND source_group=?
                        """,
                        (now_utc(), content_id, HISTORY_BACKFILL_SOURCE_GROUP),
                    )
                    tag_released = cleared.rowcount > 0
                tags_cleared += int(tag_released)
            results.append(
                {
                    "content_id": content_id, "status": str(media.get("status")),
                    "media": media, "evaluation_id": evaluation.evaluation_id,
                    "evaluation_created": evaluation.created,
                    "duplicates": duplicates,
                    "backfill_tag_cleared": tag_released,
                }
            )
        except Exception as exc:
            results.append(
                {
                    "content_id": content_id, "status": "retryable_failed",
                    "error": f"{type(exc).__name__}: {exc}"[:500],
                    "backfill_tag_cleared": False,
                }
            )
    failed = sum(item["status"] == "retryable_failed" for item in results)
    if compact:
        compact_results: Any = {
            "status_counts": {},
            "failures": [
                {
                    "content_id": item["content_id"],
                    "error": item.get("error"),
                }
                for item in results
                if item["status"] == "retryable_failed"
            ][:COMPACT_FAILURE_LIMIT],
        }
        for item in results:
            compact_results["status_counts"][item["status"]] = (
                compact_results["status_counts"].get(item["status"], 0) + 1
            )
    output = {
        "task_id": task_id,
        "status": "partial" if failed else "succeeded",
        "candidates": len(rows),
        "processed": len(results),
        "failed": failed,
        "tagged_only": tagged_only,
        "tags_cleared": tags_cleared,
        "usage": _task_usage(task_id, db_path=db_path),
        "results": compact_results if compact else results,
    }
    state_path = _write_state(
        task_id=task_id, start=start, end=end, max_amount=max_amount,
        phase="local_evidence", status=output["status"], details=output,
        state_root=state_root,
    )
    output["state_path"] = str(state_path)
    return output


def summarize_range_status(
    *,
    start: datetime,
    end: datetime,
    db_path: Path = DEFAULT_DB,
    platforms: Optional[Sequence[str]] = None,
    archive_before: Optional[datetime] = None,
    history_only: bool = False,
) -> Dict[str, Any]:
    """只读体检+报价：区间内容底数、标记分布、剩余付费工作量与成本估算。

    成本估算口径（与计费常量同源，仅供审批参考，不是合同价）：
    抖音统计/评论 $0.001/次页，小红书详情/评论 $0.01/次页；评论页数按
    最近一次快照 declared comment_count 折算（抖音≈20 条/页、小红书≈10 条/页，
    1000 条采集上限），未知评论数按 1 页保守计。
    """

    start_utc, end_utc = _utc(start), _utc(end)
    if start_utc >= end_utc:
        raise RangeBackfillError("区间开始时间必须早于结束时间")
    selected_platforms = sorted(set(platforms or ("douyin", "xiaohongshu")))
    invalid = set(selected_platforms) - {"douyin", "xiaohongshu"}
    if invalid:
        raise RangeBackfillError(f"不支持的平台：{','.join(sorted(invalid))}")
    platform_clause = ",".join("?" for _ in selected_platforms)
    output: Dict[str, Any] = {
        "status": "succeeded",
        "start": _iso(start),
        "end": _iso(end),
        "archive_before": _iso(archive_before) if archive_before else None,
        "history_only": history_only,
        "platforms": {},
        "pending": {},
        "estimated_costs_usd": {},
    }
    history_clause = (
        f" AND c.source_group IN ({','.join('?' for _ in BACKFILL_SOURCE_GROUPS)})"
        if history_only
        else ""
    )
    history_parameters = BACKFILL_SOURCE_GROUPS if history_only else ()
    with connect(db_path) as connection:
        for platform in selected_platforms:
            totals = connection.execute(
                """
                SELECT COUNT(*) n, MIN(c.published_at) oldest,
                       MAX(c.published_at) newest
                FROM content_items c JOIN accounts a ON a.id=c.account_id
                WHERE a.enabled=1 AND c.platform=?
                  AND c.published_at>=? AND c.published_at<=?
                """,
                (platform, _iso(start), _iso(end)),
            ).fetchone()
            by_month = connection.execute(
                """
                SELECT substr(c.published_at,1,7) month, COUNT(*) n
                FROM content_items c JOIN accounts a ON a.id=c.account_id
                WHERE a.enabled=1 AND c.platform=?
                  AND c.published_at>=? AND c.published_at<=?
                GROUP BY 1 ORDER BY 1
                """,
                (platform, _iso(start), _iso(end)),
            ).fetchall()
            tags = connection.execute(
                """
                SELECT COALESCE(c.source_group,'') source_group, COUNT(*) n
                FROM content_items c JOIN accounts a ON a.id=c.account_id
                WHERE a.enabled=1 AND c.platform=?
                  AND c.published_at>=? AND c.published_at<=?
                GROUP BY 1
                """,
                (platform, _iso(start), _iso(end)),
            ).fetchall()
            output["platforms"][platform] = {
                "content_total": int(totals["n"]),
                "oldest_published_at": totals["oldest"],
                "newest_published_at": totals["newest"],
                "by_month": {str(row["month"]): int(row["n"]) for row in by_month},
                "source_groups": {
                    str(row["source_group"] or ""): int(row["n"]) for row in tags
                },
            }
        comment_rows = connection.execute(
            f"""
            SELECT c.id, c.platform,
              (
                SELECT ms.comment_count FROM content_metric_snapshots ms
                WHERE ms.content_id=c.id ORDER BY ms.id DESC LIMIT 1
              ) declared_comment_count
            FROM content_items c JOIN accounts a ON a.id=c.account_id
            WHERE a.enabled=1 AND c.platform IN ({platform_clause})
              AND c.published_at>=? AND c.published_at<=?{history_clause}
              AND NOT EXISTS (
                SELECT 1 FROM comment_capture_runs r
                WHERE r.content_id=c.id AND r.status='succeeded'
              )
            """,
            (*selected_platforms, _iso(start), _iso(end), *history_parameters),
        ).fetchall()
    for stage in ("detail", "metrics", "comments"):
        for platform in selected_platforms:
            pending = pending_content_ids(
                start=start, end=end, as_of=end, db_path=db_path,
                platforms=[platform], stages=[stage], history_only=history_only,
            )
            output["pending"].setdefault(stage, {})[platform] = len(pending)
    comment_pages = {platform: 0 for platform in selected_platforms}
    unknown_counts = {platform: 0 for platform in selected_platforms}
    for row in comment_rows:
        platform = str(row["platform"])
        declared = row["declared_comment_count"]
        page_size = COMMENT_PAGE_SIZE[platform]
        if declared is None:
            unknown_counts[platform] += 1
            comment_pages[platform] += 1
        else:
            capped = min(int(declared), COMMENT_CAP)
            comment_pages[platform] += max(1, math.ceil(capped / page_size))
    estimates: Dict[str, Any] = {}
    if "douyin" in selected_platforms:
        estimates["douyin_metrics"] = round(
            output["pending"].get("metrics", {}).get("douyin", 0) * TIKHUB_PRICE, 4
        )
        estimates["douyin_detail"] = round(
            output["pending"].get("detail", {}).get("douyin", 0) * TIKHUB_PRICE, 4
        )
        estimates["douyin_comments"] = round(
            comment_pages["douyin"] * TIKHUB_PRICE, 4
        )
    if "xiaohongshu" in selected_platforms:
        estimates["xiaohongshu_detail"] = round(
            output["pending"].get("detail", {}).get("xiaohongshu", 0)
            * TIKHUB_XHS_PRICE,
            4,
        )
        estimates["xiaohongshu_comments"] = round(
            comment_pages["xiaohongshu"] * TIKHUB_XHS_PRICE, 4
        )
        estimates["xiaohongshu_metrics_note"] = (
            "小红书统计接口曝光自 2026-08-02 起为平台侧缺口，"
            "本估算不含小红书 metrics 付费调用；接口恢复后另行评估"
        )
    estimates["comment_pages"] = comment_pages
    estimates["comment_declared_unknown"] = unknown_counts
    estimates["total_excluding_xhs_metrics"] = round(
        sum(value for value in estimates.values() if isinstance(value, (int, float))),
        4,
    )
    output["estimated_costs_usd"] = estimates
    return output


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "phase",
        choices=(
            "discover", "content", "local-evidence",
            "repair-metrics", "fetch-repaired-metrics",
            "tag", "status",
        ),
    )
    parser.add_argument("--start")
    parser.add_argument(
        "--full-history", action="store_true",
        help="全量历史模式：区间起点固定为 2010-01-01（北京时间），与 --start 互斥",
    )
    parser.add_argument("--end", required=True)
    parser.add_argument(
        "--archive-before",
        help="证据窗边界（北京时间）：早于该时刻发布的内容标记 history-archive "
        "仅入库；之后的标记 history-backfill 待本地证据完成后回归常规链路",
    )
    parser.add_argument("--task-id")
    parser.add_argument("--max-amount", type=float, default=DEFAULT_MAX_AMOUNT)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--max-pages", type=int, default=20)
    parser.add_argument(
        "--workers", type=int, default=1,
        help="discover/content/fetch-repaired-metrics 阶段的有界并发（1-8）",
    )
    parser.add_argument(
        "--compact", action="store_true",
        help="输出与状态文件按汇总+失败明细压缩，适用于全量历史规模",
    )
    parser.add_argument(
        "--tagged-only", action="store_true",
        help="local-evidence 仅处理 history-backfill 标记内容并在完成后清标",
    )
    parser.add_argument(
        "--history-only", action="store_true",
        help="content/status 仅覆盖 history-* 标记内容：既有语料不重复计费，"
        "由每日调度按既定节奏维护",
    )
    parser.add_argument("--platform", action="append", choices=("douyin", "xiaohongshu"))
    parser.add_argument("--stage", action="append", choices=("detail", "metrics", "comments"))
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    values = parser.parse_args(argv)
    if values.full_history and values.start:
        parser.error("--full-history 与 --start 互斥，只能二选一")
    start = (
        FULL_HISTORY_START
        if values.full_history
        else _parse_datetime(values.start or DEFAULT_START.isoformat())
    )
    end = _parse_datetime(values.end)
    archive_before = (
        _parse_datetime(values.archive_before) if values.archive_before else None
    )
    task_id = values.task_id or task_id_for(start, end)
    if values.phase == "discover":
        result = run_discovery_backfill(
            start=start, end=end, task_id=task_id, max_amount=values.max_amount,
            db_path=values.db, platforms=values.platform,
            account_limit=values.limit, max_pages_per_account=values.max_pages,
            archive_before=archive_before, workers=values.workers,
            compact=values.compact,
        )
    elif values.phase == "content":
        result = run_content_backfill(
            start=start, end=end, task_id=task_id, max_amount=values.max_amount,
            db_path=values.db, limit=values.limit, platforms=values.platform,
            stages=values.stage, workers=values.workers, compact=values.compact,
            history_only=values.history_only,
        )
    elif values.phase == "local-evidence":
        result = run_local_evidence_backfill(
            start=start, end=end, task_id=task_id, max_amount=values.max_amount,
            db_path=values.db, limit=values.limit or 100,
            platforms=values.platform, tagged_only=values.tagged_only,
            compact=values.compact,
        )
    elif values.phase == "repair-metrics":
        result = repair_discovery_placeholder_metrics(
            start=start, end=end, as_of=end, db_path=values.db,
            platforms=values.platform, limit=values.limit,
            apply_changes=values.apply,
        )
    elif values.phase == "tag":
        if archive_before is None:
            parser.error("tag 阶段必须提供 --archive-before")
        result = tag_history_scopes(
            start=start, end=end, archive_before=archive_before,
            db_path=values.db, platforms=values.platform,
            apply_changes=values.apply,
        )
    elif values.phase == "status":
        result = summarize_range_status(
            start=start, end=end, db_path=values.db,
            platforms=values.platform, archive_before=archive_before,
            history_only=values.history_only,
        )
    else:
        result = run_repaired_metrics_backfill(
            start=start, end=end, as_of=end, task_id=task_id,
            max_amount=values.max_amount, db_path=values.db,
            platforms=values.platform, limit=values.limit,
            workers=values.workers, compact=values.compact,
        )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result["status"] not in {"blocked", "partial"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
