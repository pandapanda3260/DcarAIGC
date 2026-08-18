"""Bounded, resumable Beijing-time range backfill using active TikHub adapters."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from threading import Event
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

from .capture import BudgetBlocked, ProviderResult
from .duplicates import fingerprint_content
from .evaluation import evaluate_content
from .media import process_content_media
from .media_state import MediaTerminalDetail, media_terminal_state_details
from .metric_observations import persist_metric_observation
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
    is_formal_database_path,
    now_utc,
    transaction,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")
#: 全量历史模式的区间起点：早于双平台任何账号的可能建号时间，
#: 使发现翻页只受 has_more/max_pages/预算约束，不受时间过滤截断。
FULL_HISTORY_START = datetime(2010, 1, 1, 0, 0, tzinfo=SHANGHAI)
DEFAULT_MAX_AMOUNT = 9.0
STATE_ROOT = PROJECT_ROOT / "data" / "cache" / "v8" / "range_backfill"
DEFAULT_OPERATOR_FREEZE_LOCK = PROJECT_ROOT / "runtime" / "operator-freeze.lock"
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


def _active_evaluation_release_id(connection: sqlite3.Connection) -> str:
    rows = connection.execute(
        "SELECT id FROM evaluation_releases WHERE status='active' ORDER BY id"
    ).fetchall()
    if len(rows) != 1:
        raise RangeBackfillError(
            "本地证据回溯要求且仅允许一个 active evaluation release"
        )
    return str(rows[0]["id"])


def _pinned_media_terminal_detail(
    *,
    db_path: Path,
    release_id: str,
    content_id: int,
) -> MediaTerminalDetail:
    with connect(db_path) as connection:
        connection.execute("BEGIN")
        current_release_id = _active_evaluation_release_id(connection)
        if current_release_id != release_id:
            raise RangeBackfillError(
                "active evaluation release 在本地证据处理期间发生切换："
                f"{release_id} -> {current_release_id}"
            )
        detail = media_terminal_state_details(
            connection,
            release_id,
            [content_id],
        ).get(content_id)
        if detail is None:
            raise RangeBackfillError(f"媒体终态 selector 未返回内容：{content_id}")
        connection.commit()
    return detail


def _release_history_backfill_tag(
    *,
    db_path: Path,
    release_id: str,
    content_id: int,
) -> bool:
    with connect(db_path) as connection, transaction(connection):
        current_release_id = _active_evaluation_release_id(connection)
        if current_release_id != release_id:
            raise RangeBackfillError(
                "active evaluation release 在清除 history-backfill 标记前发生切换："
                f"{release_id} -> {current_release_id}"
            )
        detail = media_terminal_state_details(
            connection,
            release_id,
            [content_id],
        ).get(content_id)
        if detail is None or detail.state not in {
            "complete",
            "terminal_insufficient",
        }:
            reason = detail.reason if detail is not None else "selector_missing"
            raise RangeBackfillError(
                "媒体终态在清除 history-backfill 标记前发生漂移："
                f"{reason}"
            )
        cleared = connection.execute(
            """
            UPDATE content_items SET source_group='', updated_at=?
            WHERE id=? AND source_group=?
            """,
            (now_utc(), content_id, HISTORY_BACKFILL_SOURCE_GROUP),
        )
        return cleared.rowcount > 0


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise RangeBackfillError("区间时间必须包含时区")
    return value.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return _utc(value).isoformat(timespec="auto").replace("+00:00", "Z")


def _parse_datetime(value: str) -> datetime:
    text = value.strip()
    if not text:
        raise RangeBackfillError("时间不能为空")
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise RangeBackfillError("区间时间必须包含时区")
    return parsed


def task_id_for(start: datetime, end: datetime) -> str:
    local_start = start.astimezone(SHANGHAI).strftime("%Y%m%dT%H%M%S")
    local_end = end.astimezone(SHANGHAI).strftime("%Y%m%dT%H%M%S")
    return f"two-week-backfill-{local_start}-{local_end}-bjt"


def _selected_platforms(platforms: Optional[Sequence[str]]) -> List[str]:
    selected = sorted(set(platforms or ("douyin", "xiaohongshu")))
    invalid = set(selected) - {"douyin", "xiaohongshu"}
    if invalid:
        raise RangeBackfillError(f"不支持的平台：{','.join(sorted(invalid))}")
    if not selected:
        raise RangeBackfillError("至少选择一个平台")
    return selected


def _require_formal_mutation_freeze(*, db_path: Path) -> None:
    if not is_formal_database_path(db_path, formal_database=DEFAULT_DB):
        return
    freeze_value = os.environ.get("DCAR_OPERATOR_FREEZE_LOCK") or os.environ.get(
        "DCAR_FREEZE_LOCK"
    )
    freeze_lock = Path(freeze_value or DEFAULT_OPERATOR_FREEZE_LOCK).expanduser()
    if not freeze_lock.is_file() or freeze_lock.is_symlink():
        raise RangeBackfillError(
            "正式数据库变更要求有效的 operator freeze lock："
            f"{freeze_lock}"
        )


def _campaign_contract(
    *,
    task_id: str,
    start: datetime,
    end: datetime,
    as_of: datetime,
    max_amount: float,
    max_pages: int,
    platforms: Sequence[str],
) -> Dict[str, Any]:
    if not task_id.strip():
        raise RangeBackfillError("task_id 不能为空")
    if _utc(start) >= _utc(end):
        raise RangeBackfillError("补抓开始时间必须早于结束时间")
    _utc(as_of)
    if not math.isfinite(max_amount) or max_amount <= 0:
        raise RangeBackfillError("预算必须为有限正数")
    if max_pages <= 0:
        raise RangeBackfillError("每账号页数上限必须为正数")
    return {
        "task_id": task_id,
        "start": _iso(start),
        "end": _iso(end),
        "as_of": _iso(as_of),
        "max_amount": max_amount,
        "max_pages": max_pages,
        "platforms": list(platforms),
    }


def _prepare_campaign_contract(
    *,
    task_id: str,
    start: datetime,
    end: datetime,
    as_of: datetime,
    max_amount: float,
    max_pages: int,
    platforms: Sequence[str],
    phase: str,
    state_root: Path,
) -> Path:
    contract = _campaign_contract(
        task_id=task_id,
        start=start,
        end=end,
        as_of=as_of,
        max_amount=max_amount,
        max_pages=max_pages,
        platforms=platforms,
    )
    state_root.mkdir(parents=True, exist_ok=True)
    target = state_root / f"{task_id}.json"
    existing: Dict[str, Any] = {}
    if target.is_file():
        loaded = json.loads(target.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            raise RangeBackfillError("已有补抓状态文件不是 JSON object")
        existing = loaded
        if existing.get("contract") != contract:
            raise RangeBackfillError("已有补抓状态文件与本次完整合同不一致")
    phase_contracts = dict(existing.get("phase_contracts") or {})
    phase_contract = {**contract, "phase": phase}
    recorded_phase_contract = phase_contracts.get(phase)
    if recorded_phase_contract is not None and recorded_phase_contract != phase_contract:
        raise RangeBackfillError("已有补抓阶段合同与本次调用不一致")
    phase_contracts[phase] = phase_contract
    captured_at = now_utc()
    value = {
        **existing,
        "task_id": task_id,
        "timezone": "Asia/Shanghai",
        "contract": contract,
        "phase_contracts": phase_contracts,
        "phase": existing.get("phase") or phase,
        "status": existing.get("status") or "prepared",
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
    as_of: datetime,
    max_amount: float,
    max_pages: int,
    platforms: Sequence[str],
    phase: str,
    status: str,
    details: Mapping[str, Any],
    state_root: Path,
) -> Path:
    target = _prepare_campaign_contract(
        task_id=task_id,
        start=start,
        end=end,
        as_of=as_of,
        max_amount=max_amount,
        max_pages=max_pages,
        platforms=platforms,
        phase=phase,
        state_root=state_root,
    )
    loaded = json.loads(target.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise RangeBackfillError("已有补抓状态文件不是 JSON object")
    existing: Dict[str, Any] = loaded
    captured_at = now_utc()
    value = {
        **existing,
        "task_id": task_id,
        "timezone": "Asia/Shanghai",
        "start": _iso(start),
        "end": _iso(end),
        "as_of": _iso(as_of),
        "max_amount": max_amount,
        "max_pages": max_pages,
        "platforms": list(platforms),
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


def _write_discovery_content_manifest(
    *,
    task_id: str,
    start: datetime,
    end: datetime,
    results: Sequence[Mapping[str, Any]],
    state_root: Path,
    db_path: Path,
) -> Dict[str, Any]:
    """Persist the exact content cohort independently of transient source tags.

    ``history-backfill`` is deliberately cleared after V2/V3 evidence succeeds.
    Keeping first/latest upsert actions in a campaign-side manifest makes the
    inserted cohort auditable without adding another production database field.
    """

    state_root.mkdir(parents=True, exist_ok=True)
    target = state_root / f"{task_id}.contents.json"
    entries: Dict[int, Dict[str, Any]] = {}
    if target.is_file():
        loaded = json.loads(target.read_text(encoding="utf-8"))
        if isinstance(loaded, dict):
            for entry in loaded.get("contents") or []:
                if isinstance(entry, dict) and entry.get("content_id") is not None:
                    entries[int(entry["content_id"])] = dict(entry)
    for identity in results:
        for page in identity.get("pages") or []:
            for change in page.get("content_changes") or []:
                if not isinstance(change, Mapping) or change.get("content_id") is None:
                    continue
                content_id = int(change["content_id"])
                action = str(change.get("action") or "unknown")
                existing = entries.get(content_id, {})
                entries[content_id] = {
                    **existing,
                    "content_id": content_id,
                    "first_action": existing.get("first_action") or action,
                    "latest_action": action,
                }
    ordered_ids = sorted(entries)
    with connect(db_path) as connection:
        for offset in range(0, len(ordered_ids), 500):
            batch = ordered_ids[offset:offset + 500]
            placeholders = ",".join("?" for _ in batch)
            for row in connection.execute(
                f"""
                SELECT id,link_id,platform,source_group,published_at
                FROM content_items WHERE id IN ({placeholders})
                """,
                batch,
            ).fetchall():
                entry = entries[int(row["id"])]
                entry.update(
                    {
                        "link_id": str(row["link_id"]),
                        "platform": str(row["platform"]),
                        "source_group": str(row["source_group"] or ""),
                        "published_at": row["published_at"],
                    }
                )
    contents = [entries[content_id] for content_id in ordered_ids]
    payload = {
        "task_id": task_id,
        "start": _iso(start),
        "end": _iso(end),
        "updated_at": now_utc(),
        "contents": contents,
    }
    temporary = target.with_name(f".{target.name}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(target)
    return {
        "path": str(target),
        "contents": len(contents),
        "first_inserted": sum(
            entry.get("first_action") == "inserted" for entry in contents
        ),
        "first_updated": sum(
            entry.get("first_action") == "updated" for entry in contents
        ),
    }


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
    content_ids: Optional[Sequence[int]] = None,
) -> Dict[str, Any]:
    """把回溯批量入库的内容按证据窗切成两组标记，保护每日闸门与增量评估。

    published < archive_before → ``history-archive``（仅入库+指标，永不自动评估）；
    archive_before ≤ published ≤ end → ``history-backfill``（待 local-evidence
    完成媒体+评估后清除标记回归常规链路）。只触碰从未产生过任何评估版本、
    且 source_group 为空的启用账号内容——既有已评估语料的生命周期不受影响。
    幂等：重复执行不会改写已标记或已评估的行。
    """

    if apply_changes:
        _require_formal_mutation_freeze(db_path=db_path)
    start_utc, end_utc = _utc(start), _utc(end)
    archive_utc = _utc(archive_before)
    if start_utc >= end_utc:
        raise RangeBackfillError("标记区间开始时间必须早于结束时间")
    selected_platforms = _selected_platforms(platforms)
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
        "restricted_content_ids": (
            len(set(int(value) for value in content_ids))
            if content_ids is not None
            else None
        ),
        "segments": {},
    }
    with connect(db_path) as connection, transaction(connection):
        content_scope_clause = ""
        if content_ids is not None:
            connection.execute(
                "CREATE TEMP TABLE campaign_history_scope_ids(id INTEGER PRIMARY KEY)"
            )
            connection.executemany(
                "INSERT OR IGNORE INTO campaign_history_scope_ids(id) VALUES (?)",
                [(int(value),) for value in content_ids],
            )
            content_scope_clause = (
                " AND c.id IN (SELECT id FROM campaign_history_scope_ids)"
            )
        for tag, _, published_clause, published_params in segments:
            candidate_sql = f"""
                SELECT c.id FROM content_items c
                JOIN accounts a ON a.id=c.account_id
                WHERE a.enabled=1
                  AND c.platform IN ({platform_clause})
                  AND COALESCE(c.source_group,'')=''
                  AND {published_clause}
                  {content_scope_clause}
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
                "completed": bool(item.get("completed")),
                "completion_reason": item.get("completion_reason"),
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
    as_of: datetime,
    require_live_detail: bool = False,
    skip_existing_derived_stages: bool = False,
) -> Dict[str, Any]:
    _require_formal_mutation_freeze(db_path=db_path)
    start_utc, end_utc = _utc(start), _utc(end)
    if start_utc >= end_utc:
        raise RangeBackfillError("补抓开始时间必须早于结束时间")
    if max_amount <= 0 or max_pages_per_account <= 0:
        raise RangeBackfillError("预算与每账号页数上限必须为正数")
    if workers < 1 or workers > 8:
        raise RangeBackfillError("workers 必须在 1 到 8 之间")
    if archive_before is not None:
        _utc(archive_before)
    selected_platforms = _selected_platforms(platforms)
    _prepare_campaign_contract(
        task_id=task_id,
        start=start,
        end=end,
        as_of=as_of,
        max_amount=max_amount,
        max_pages=max_pages_per_account,
        platforms=selected_platforms,
        phase="discover",
        state_root=state_root,
    )
    identities = _enabled_identities(
        db_path=db_path, platforms=selected_platforms, account_limit=account_limit
    )
    blocking_stop = Event()

    def initial_source_group(published_at: str) -> str:
        if archive_before is None:
            return ""
        published = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        return (
            HISTORY_ARCHIVE_SOURCE_GROUP
            if published < _utc(archive_before)
            else HISTORY_BACKFILL_SOURCE_GROUP
        )

    def discover_identity(identity: Mapping[str, Any]) -> Dict[str, Any]:
        cursor: Any = None
        seen_cursors: set[str] = set()
        pages: List[Dict[str, Any]] = []
        local_stop: Optional[str] = None
        completion_reason: Optional[str] = None
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
                    str(identity["uid"]),
                    as_of=as_of.astimezone(SHANGHAI).date(),
                    cursor=cursor, window_key=window_key,
                    published_start=start_utc, published_end=end_utc,
                    task_id=task_id, task_max_amount=max_amount,
                    db_path=db_path, call_override=call_override,
                    materialize_discovery_detail=not require_live_detail,
                    materialize_existing_discovery_stages=(
                        not skip_existing_derived_stages
                    ),
                    new_content_source_group=initial_source_group,
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
                completion_reason = "range_start_reached"
                break
            next_cursor = page.get("next_cursor")
            if not page.get("has_more"):
                completion_reason = "provider_exhausted"
                break
            if next_cursor in (None, ""):
                local_stop = "missing_next_cursor"
                break
            cursor = next_cursor
        else:
            # Reaching the safety cap while the provider still advertises more
            # pages is not a completed account.  Callers must raise the cap (or
            # add a continuation strategy) instead of silently claiming full
            # history coverage.
            if pages and pages[-1].get("has_more"):
                local_stop = "page_limit_reached"
        return {
            **identity,
            "pages": pages,
            "completed": completion_reason is not None,
            "completion_reason": completion_reason,
            "stopped_reason": local_stop,
        }

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
    accounts_completed = sum(bool(item.get("completed")) for item in results)
    status = (
        "blocked"
        if stopped_reason in BLOCKING_CODES
        else "partial"
        if failed_pages or stopped_reason or accounts_completed != len(identities)
        else "succeeded"
    )
    output = {
        "task_id": task_id,
        "status": status,
        "start": _iso(start),
        "end": _iso(end),
        "as_of": _iso(as_of),
        "require_live_detail": require_live_detail,
        "skip_existing_derived_stages": skip_existing_derived_stages,
        "accounts_considered": len(identities),
        "accounts_processed": sum(1 for item in results if item["pages"]),
        "accounts_completed": accounts_completed,
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
        inserted_content_ids = sorted(
            {
                int(change["content_id"])
                for item in results
                for page in item.get("pages") or []
                for change in page.get("content_changes") or []
                if isinstance(change, Mapping)
                and change.get("content_id") is not None
                and str(change.get("action")) == "inserted"
            }
        )
        output["history_scopes"] = tag_history_scopes(
            start=start, end=end, archive_before=archive_before,
            db_path=db_path, platforms=selected_platforms, apply_changes=True,
            content_ids=inserted_content_ids,
        )
    output["content_manifest"] = _write_discovery_content_manifest(
        task_id=task_id,
        start=start,
        end=end,
        results=results,
        state_root=state_root,
        db_path=db_path,
    )
    state_path = _write_state(
        task_id=task_id, start=start, end=end, as_of=as_of,
        max_amount=max_amount, max_pages=max_pages_per_account,
        platforms=selected_platforms, phase="discover", status=status,
        details=output, state_root=state_root,
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
    # 既有语料缺口不在本战役内自动重买；需独立审计、报价和授权。
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
    history_only: bool = False,
) -> Dict[str, Any]:
    """Reopen only metrics slots closed by non-authoritative discovery zeros."""

    if apply_changes:
        _require_formal_mutation_freeze(db_path=db_path)
    selected_platforms = _selected_platforms(platforms)
    if limit is not None and limit <= 0:
        raise RangeBackfillError("limit 必须为正数")
    day_key = as_of.astimezone(SHANGHAI).date().isoformat()
    parameters: List[Any] = [
        day_key, _iso(start), _iso(end), *selected_platforms,
    ]
    history_clause = (
        f" AND c.source_group IN ({','.join('?' for _ in BACKFILL_SOURCE_GROUPS)})"
        if history_only
        else ""
    )
    if history_only:
        parameters.extend(BACKFILL_SOURCE_GROUPS)
    sql = f"""
        SELECT fs.id slot_id, fs.content_id, fs.attempt_count,
               c.platform, c.link_id, c.published_at,
               ms.id snapshot_id, ms.captured_at snapshot_captured_at,
               ms.window_key snapshot_window_key, ms.view_count,
               ms.comment_count, ms.like_count, ms.share_count,
               ms.collect_count, ms.source snapshot_source,
               ms.raw_response_id, ms.metadata_json
        FROM fetch_slots fs
        JOIN content_items c ON c.id=fs.content_id
        JOIN content_metric_snapshots ms
          ON ms.content_id=c.id AND ms.window_key=fs.window_key
         AND ms.source=c.platform
        WHERE fs.stage='metrics' AND fs.window_key=? AND fs.status='succeeded'
          AND fs.adapter_version='tikhub-discovery-derived-v8.1'
          AND c.published_at>=? AND c.published_at<?
          AND c.platform IN ({','.join('?' for _ in selected_platforms)})
          {history_clause}
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
        "history_only": history_only,
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
            persist_metric_observation(
                connection,
                content_id=int(row["content_id"]),
                captured_at=str(row["snapshot_captured_at"]),
                window_key=str(row["snapshot_window_key"]),
                view_count=None,
                comment_count=row["comment_count"],
                like_count=row["like_count"],
                share_count=row["share_count"],
                collect_count=row["collect_count"],
                status="missing",
                source=str(row["snapshot_source"]),
                raw_response_id=row["raw_response_id"],
                metadata_json=json.dumps(
                    metadata,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                observation_origin="system_correction",
                snapshot_mode="replace",
                recorded_at=repaired_at,
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
    history_only: bool = False,
    contract_max_pages: int = 20,
) -> Dict[str, Any]:
    _require_formal_mutation_freeze(db_path=db_path)
    selected_platforms = _selected_platforms(platforms)
    if max_amount <= 0 or (limit is not None and limit <= 0):
        raise RangeBackfillError("预算与 limit 必须为正数")
    _prepare_campaign_contract(
        task_id=task_id,
        start=start,
        end=end,
        as_of=as_of,
        max_amount=max_amount,
        max_pages=contract_max_pages,
        platforms=selected_platforms,
        phase="fetch-repaired-metrics",
        state_root=state_root,
    )
    day_key = as_of.astimezone(SHANGHAI).date().isoformat()
    parameters: List[Any] = [
        day_key, _iso(start), _iso(end), *selected_platforms,
    ]
    history_clause = (
        f" AND c.source_group IN ({','.join('?' for _ in BACKFILL_SOURCE_GROUPS)})"
        if history_only
        else ""
    )
    if history_only:
        parameters.extend(BACKFILL_SOURCE_GROUPS)
    sql = f"""
        SELECT fs.content_id
        FROM fetch_slots fs JOIN content_items c ON c.id=fs.content_id
        WHERE fs.stage='metrics' AND fs.window_key=?
          AND fs.status='retryable_failed'
          AND fs.last_error_code='invalid_discovery_exposure'
          AND c.published_at>=? AND c.published_at<?
          AND c.platform IN ({','.join('?' for _ in selected_platforms)})
          {history_clause}
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
            task_id=task_id, start=start, end=end, as_of=as_of,
            max_amount=max_amount, max_pages=contract_max_pages,
            platforms=selected_platforms, phase="fetch-repaired-metrics",
            status="running",
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
        "history_only": history_only,
        "candidates": len(content_ids),
        "processed": sum(
            item["status"] != "skipped_after_block" for item in results
        ),
        "stopped_reason": stopped_reason,
        "usage": usage,
        "results": _compact_content_results(results) if compact else results,
    }
    state_path = _write_state(
        task_id=task_id, start=start, end=end, as_of=as_of,
        max_amount=max_amount, max_pages=contract_max_pages,
        platforms=selected_platforms, phase="fetch-repaired-metrics",
        status=status, details=output, state_root=state_root,
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
    as_of: datetime,
    contract_max_pages: int = 20,
) -> Dict[str, Any]:
    _require_formal_mutation_freeze(db_path=db_path)
    selected_platforms = _selected_platforms(platforms)
    selected_stages = list(dict.fromkeys(stages or ("detail", "metrics", "comments")))
    _prepare_campaign_contract(
        task_id=task_id,
        start=start,
        end=end,
        as_of=as_of,
        max_amount=max_amount,
        max_pages=contract_max_pages,
        platforms=selected_platforms,
        phase="content",
        state_root=state_root,
    )
    content_ids = pending_content_ids(
        start=start, end=end, as_of=as_of, db_path=db_path, limit=limit,
        platforms=selected_platforms, stages=selected_stages, history_only=history_only,
    )

    def flush_progress(partial_results: List[Dict[str, Any]]) -> None:
        _write_state(
            task_id=task_id, start=start, end=end, as_of=as_of,
            max_amount=max_amount, max_pages=contract_max_pages,
            platforms=selected_platforms, phase="content", status="running",
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
            content_id,
            as_of=as_of.astimezone(SHANGHAI).date(),
            db_path=db_path,
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
        "as_of": _iso(as_of),
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
        task_id=task_id, start=start, end=end, as_of=as_of,
        max_amount=max_amount, max_pages=contract_max_pages,
        platforms=selected_platforms, phase="content", status=status,
        details=output, state_root=state_root,
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
    as_of: datetime,
    contract_max_pages: int = 20,
) -> Dict[str, Any]:
    """媒体+评估+感知重复的本地证据推进；history-archive 内容按口径永不进入。

    ``tagged_only=True`` 时只处理 ``history-backfill`` 标记的内容（最新优先），
    单条由共享媒体终态 selector 判定：complete 完成重复指纹并清标，
    terminal_insufficient 不伪造重复指纹但稳定清标；pending 与
    terminal_failed 均保留标记，等待重试或未来显式刷新策略。
    """

    _require_formal_mutation_freeze(db_path=db_path)
    selected_platforms = _selected_platforms(platforms)
    if limit <= 0:
        raise RangeBackfillError("limit 必须为正数")
    _prepare_campaign_contract(
        task_id=task_id,
        start=start,
        end=end,
        as_of=as_of,
        max_amount=max_amount,
        max_pages=contract_max_pages,
        platforms=selected_platforms,
        phase="local-evidence",
        state_root=state_root,
    )
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
        pinned_release_id = _active_evaluation_release_id(connection)
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
            pre_detail = _pinned_media_terminal_detail(
                db_path=db_path,
                release_id=pinned_release_id,
                content_id=content_id,
            )
            if pre_detail.state == "terminal_failed":
                results.append(
                    {
                        "content_id": content_id,
                        "status": "terminal_failed",
                        "media_terminal_state": pre_detail.state,
                        "media_terminal_reason": pre_detail.reason,
                        "error": f"媒体处理已终止：{pre_detail.reason}",
                        "backfill_tag_cleared": False,
                    }
                )
                continue
            if pre_detail.state in {"complete", "terminal_insufficient"}:
                duplicate_fingerprint = (
                    fingerprint_content(content_id, db_path=db_path)
                    if pre_detail.state == "complete"
                    else None
                )
                tag_released = False
                if was_tagged:
                    tag_released = _release_history_backfill_tag(
                        db_path=db_path,
                        release_id=pinned_release_id,
                        content_id=content_id,
                    )
                    tags_cleared += int(tag_released)
                results.append(
                    {
                        "content_id": content_id,
                        "status": pre_detail.state,
                        "media_terminal_state": pre_detail.state,
                        "media_terminal_reason": pre_detail.reason,
                        "duplicate_fingerprint": duplicate_fingerprint,
                        "backfill_tag_cleared": tag_released,
                        "resumed_from_terminal_state": True,
                    }
                )
                continue
            try:
                media = process_content_media(content_id, db_path=db_path)
            except Exception as media_error:
                detail = _pinned_media_terminal_detail(
                    db_path=db_path,
                    release_id=pinned_release_id,
                    content_id=content_id,
                )
                if detail.state == "terminal_failed":
                    results.append(
                        {
                            "content_id": content_id,
                            "status": "terminal_failed",
                            "media_terminal_state": detail.state,
                            "media_terminal_reason": detail.reason,
                            "error": f"{type(media_error).__name__}: {media_error}"[
                                :500
                            ],
                            "backfill_tag_cleared": False,
                        }
                    )
                    continue
                raise
            media_status = str(media.get("status") or "")
            if media_status != "evidence_ready":
                detail = _pinned_media_terminal_detail(
                    db_path=db_path,
                    release_id=pinned_release_id,
                    content_id=content_id,
                )
                if detail.state == "terminal_failed":
                    results.append(
                        {
                            "content_id": content_id,
                            "status": "terminal_failed",
                            "media": media,
                            "media_terminal_state": detail.state,
                            "media_terminal_reason": detail.reason,
                            "error": f"媒体处理已终止：{detail.reason}",
                            "backfill_tag_cleared": False,
                        }
                    )
                    continue
                raise RangeBackfillError(
                    "媒体证据尚未就绪："
                    f"{media_status or 'unknown'}（{detail.reason}）"
                )
            evaluation = evaluate_content(
                content_id,
                db_path=db_path,
                expected_active_release_id=pinned_release_id,
            )
            detail = _pinned_media_terminal_detail(
                db_path=db_path,
                release_id=pinned_release_id,
                content_id=content_id,
            )
            if detail.state == "pending":
                raise RangeBackfillError(
                    "媒体证据 DAG 尚未终结："
                    f"{detail.reason}（evaluation={evaluation.evaluation_id}）"
                )
            if detail.state == "terminal_failed":
                results.append(
                    {
                        "content_id": content_id,
                        "status": "terminal_failed",
                        "media": media,
                        "evaluation_id": evaluation.evaluation_id,
                        "evaluation_created": evaluation.created,
                        "evaluation_evidence_level": evaluation.evidence_level,
                        "media_terminal_state": detail.state,
                        "media_terminal_reason": detail.reason,
                        "error": f"媒体处理已终止：{detail.reason}",
                        "backfill_tag_cleared": False,
                    }
                )
                continue
            duplicate_fingerprint = (
                fingerprint_content(content_id, db_path=db_path)
                if detail.state == "complete"
                else None
            )
            tag_released = False
            if was_tagged:
                tag_released = _release_history_backfill_tag(
                    db_path=db_path,
                    release_id=pinned_release_id,
                    content_id=content_id,
                )
                tags_cleared += int(tag_released)
            results.append(
                {
                    "content_id": content_id,
                    "status": detail.state,
                    "media": media,
                    "evaluation_id": evaluation.evaluation_id,
                    "evaluation_created": evaluation.created,
                    "evaluation_evidence_level": evaluation.evidence_level,
                    "media_terminal_state": detail.state,
                    "media_terminal_reason": detail.reason,
                    "duplicate_fingerprint": duplicate_fingerprint,
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
    failed = sum(
        item["status"] in {"retryable_failed", "terminal_failed"}
        for item in results
    )
    if compact:
        compact_results: Any = {
            "status_counts": {},
            "failures": [
                {
                    "content_id": item["content_id"],
                    "error": item.get("error"),
                }
                for item in results
                if item["status"] in {"retryable_failed", "terminal_failed"}
            ][:COMPACT_FAILURE_LIMIT],
        }
        for item in results:
            compact_results["status_counts"][item["status"]] = (
                compact_results["status_counts"].get(item["status"], 0) + 1
            )
    terminal_status = "partial" if failed else "succeeded"
    output = {
        "task_id": task_id,
        "status": terminal_status,
        "candidates": len(rows),
        "processed": len(results),
        "failed": failed,
        "evaluation_release_id": pinned_release_id,
        "tagged_only": tagged_only,
        "tags_cleared": tags_cleared,
        "usage": _task_usage(task_id, db_path=db_path),
        "results": compact_results if compact else results,
    }
    state_path = _write_state(
        task_id=task_id, start=start, end=end, as_of=as_of,
        max_amount=max_amount, max_pages=contract_max_pages,
        platforms=selected_platforms, phase="local-evidence",
        status=terminal_status, details=output, state_root=state_root,
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
    as_of: Optional[datetime] = None,
) -> Dict[str, Any]:
    """只读体检+报价：区间内容底数、标记分布、剩余付费工作量与成本估算。

    成本估算口径（与计费常量同源，仅供审批参考，不是合同价）：
    抖音统计/评论 $0.001/次页，小红书详情/评论 $0.01/次页；评论页数按
    最近一次快照 declared comment_count 折算（抖音≈20 条/页、小红书≈10 条/页，
    1000 条采集上限）。未知评论数同时给出 1 页下界与采集上限的上界；
    自动预算使用上界，不把最低价误当保守报价。
    """

    start_utc, end_utc = _utc(start), _utc(end)
    effective_as_of = as_of or end
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
        "as_of": _iso(effective_as_of),
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
                start=start, end=end, as_of=effective_as_of, db_path=db_path,
                platforms=[platform], stages=[stage], history_only=history_only,
            )
            output["pending"].setdefault(stage, {})[platform] = len(pending)
    comment_pages_lower = {platform: 0 for platform in selected_platforms}
    comment_pages_upper = {platform: 0 for platform in selected_platforms}
    unknown_counts = {platform: 0 for platform in selected_platforms}
    for row in comment_rows:
        platform = str(row["platform"])
        declared = row["declared_comment_count"]
        page_size = COMMENT_PAGE_SIZE[platform]
        if declared is None:
            unknown_counts[platform] += 1
            comment_pages_lower[platform] += 1
            comment_pages_upper[platform] += math.ceil(COMMENT_CAP / page_size)
        else:
            capped = min(int(declared), COMMENT_CAP)
            pages = max(1, math.ceil(capped / page_size))
            comment_pages_lower[platform] += pages
            comment_pages_upper[platform] += pages
    estimates: Dict[str, Any] = {}
    if "douyin" in selected_platforms:
        estimates["douyin_metrics"] = round(
            output["pending"].get("metrics", {}).get("douyin", 0) * TIKHUB_PRICE, 4
        )
        estimates["douyin_detail"] = round(
            output["pending"].get("detail", {}).get("douyin", 0) * TIKHUB_PRICE, 4
        )
        estimates["douyin_comments_lower"] = round(
            comment_pages_lower["douyin"] * TIKHUB_PRICE, 4
        )
        estimates["douyin_comments"] = round(
            comment_pages_upper["douyin"] * TIKHUB_PRICE, 4
        )
    if "xiaohongshu" in selected_platforms:
        estimates["xiaohongshu_detail"] = round(
            output["pending"].get("detail", {}).get("xiaohongshu", 0)
            * TIKHUB_XHS_PRICE,
            4,
        )
        estimates["xiaohongshu_comments_lower"] = round(
            comment_pages_lower["xiaohongshu"] * TIKHUB_XHS_PRICE, 4
        )
        estimates["xiaohongshu_comments"] = round(
            comment_pages_upper["xiaohongshu"] * TIKHUB_XHS_PRICE, 4
        )
        estimates["xiaohongshu_metrics_note"] = (
            "小红书统计接口曝光自 2026-08-02 起为平台侧缺口，"
            "本估算不含小红书 metrics 付费调用；接口恢复后另行评估"
        )
    # Backward-compatible ``comment_pages`` is the approval-safe upper bound.
    estimates["comment_pages"] = comment_pages_upper
    estimates["comment_pages_lower"] = comment_pages_lower
    estimates["comment_pages_upper"] = comment_pages_upper
    estimates["comment_declared_unknown"] = unknown_counts
    estimates["total_excluding_xhs_metrics"] = round(
        float(estimates.get("douyin_metrics", 0))
        + float(estimates.get("douyin_detail", 0))
        + float(estimates.get("douyin_comments", 0))
        + float(estimates.get("xiaohongshu_detail", 0))
        + float(estimates.get("xiaohongshu_comments", 0)),
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
        "--as-of",
        required=True,
        help="当前累计指标/评论的实际采集截面（必须含时区）；"
        "与内容发布截止 --end 分离",
    )
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
        help="content/status 仅覆盖 history-* 标记内容；"
        "既有语料缺口需独立审计与授权",
    )
    parser.add_argument(
        "--require-live-detail",
        action="store_true",
        help="发现阶段不用列表页派生详情关闭 lifetime 槽；"
        "保留给后续真实 detail 接口补全正文/媒体源",
    )
    parser.add_argument(
        "--skip-existing-derived-stages",
        action="store_true",
        help="发现只更新既有内容身份，不用列表页派生值覆盖其详情/"
        "指标槽；避免 history-only 战役把存量内容的最新曝光改成缺失",
    )
    parser.add_argument("--platform", action="append", choices=("douyin", "xiaohongshu"))
    parser.add_argument("--stage", action="append", choices=("detail", "metrics", "comments"))
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    values = parser.parse_args(argv)
    if values.full_history and values.start:
        parser.error("--full-history 与 --start 互斥，只能二选一")
    if not values.full_history and not values.start:
        parser.error("非 --full-history 模式必须显式提供 --start")
    start = (
        FULL_HISTORY_START
        if values.full_history
        else _parse_datetime(values.start)
    )
    end = _parse_datetime(values.end)
    as_of = _parse_datetime(values.as_of)
    archive_before = (
        _parse_datetime(values.archive_before) if values.archive_before else None
    )
    task_id = values.task_id or task_id_for(start, end)
    selected_platforms = _selected_platforms(values.platform)
    if values.apply and values.phase in {"repair-metrics", "tag"}:
        _prepare_campaign_contract(
            task_id=task_id,
            start=start,
            end=end,
            as_of=as_of,
            max_amount=values.max_amount,
            max_pages=values.max_pages,
            platforms=selected_platforms,
            phase=values.phase,
            state_root=STATE_ROOT,
        )
    if values.phase == "discover":
        result = run_discovery_backfill(
            start=start, end=end, task_id=task_id, max_amount=values.max_amount,
            db_path=values.db, platforms=selected_platforms,
            account_limit=values.limit, max_pages_per_account=values.max_pages,
            archive_before=archive_before, workers=values.workers,
            compact=values.compact,
            as_of=as_of,
            require_live_detail=values.require_live_detail,
            skip_existing_derived_stages=values.skip_existing_derived_stages,
        )
    elif values.phase == "content":
        result = run_content_backfill(
            start=start, end=end, task_id=task_id, max_amount=values.max_amount,
            db_path=values.db, limit=values.limit, platforms=selected_platforms,
            stages=values.stage, workers=values.workers, compact=values.compact,
            history_only=values.history_only,
            as_of=as_of,
            contract_max_pages=values.max_pages,
        )
    elif values.phase == "local-evidence":
        result = run_local_evidence_backfill(
            start=start, end=end, task_id=task_id, max_amount=values.max_amount,
            db_path=values.db, limit=values.limit or 100,
            platforms=selected_platforms, tagged_only=values.tagged_only,
            compact=values.compact, as_of=as_of,
            contract_max_pages=values.max_pages,
        )
    elif values.phase == "repair-metrics":
        result = repair_discovery_placeholder_metrics(
            start=start, end=end, as_of=as_of, db_path=values.db,
            platforms=selected_platforms, limit=values.limit,
            apply_changes=values.apply, history_only=values.history_only,
        )
    elif values.phase == "tag":
        if archive_before is None:
            parser.error("tag 阶段必须提供 --archive-before")
        result = tag_history_scopes(
            start=start, end=end, archive_before=archive_before,
            db_path=values.db, platforms=selected_platforms,
            apply_changes=values.apply,
        )
    elif values.phase == "status":
        result = summarize_range_status(
            start=start, end=end, db_path=values.db,
            platforms=selected_platforms, archive_before=archive_before,
            history_only=values.history_only,
            as_of=as_of,
        )
    else:
        result = run_repaired_metrics_backfill(
            start=start, end=end, as_of=as_of, task_id=task_id,
            max_amount=values.max_amount, db_path=values.db,
            platforms=selected_platforms, limit=values.limit,
            workers=values.workers, compact=values.compact,
            history_only=values.history_only,
            contract_max_pages=values.max_pages,
        )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result["status"] not in {"blocked", "partial"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
