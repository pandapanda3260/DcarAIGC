"""Bounded, resumable Beijing-time range backfill using active TikHub adapters."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

from .capture import BudgetBlocked, ProviderResult
from .duplicates import refresh_content_duplicates
from .evaluation import evaluate_content
from .media import process_content_media
from .providers import (
    discover_account_content,
    materialize_zero_comment_evidence,
    update_content_data,
)
from .storage import DEFAULT_DB, PROJECT_ROOT, connect, now_utc, transaction


SHANGHAI = ZoneInfo("Asia/Shanghai")
DEFAULT_START = datetime(2026, 7, 20, 0, 0, tzinfo=SHANGHAI)
DEFAULT_MAX_AMOUNT = 9.0
STATE_ROOT = PROJECT_ROOT / "data" / "cache" / "v8" / "range_backfill"
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
) -> Dict[str, Any]:
    start_utc, end_utc = _utc(start), _utc(end)
    if start_utc >= end_utc:
        raise RangeBackfillError("补抓开始时间必须早于结束时间")
    if max_amount <= 0 or max_pages_per_account <= 0:
        raise RangeBackfillError("预算与每账号页数上限必须为正数")
    identities = _enabled_identities(
        db_path=db_path, platforms=platforms, account_limit=account_limit
    )
    results: List[Dict[str, Any]] = []
    stopped_reason: Optional[str] = None
    for identity in identities:
        cursor: Any = None
        seen_cursors: set[str] = set()
        pages: List[Dict[str, Any]] = []
        for page_number in range(1, max_pages_per_account + 1):
            cursor_key = _cursor_digest(cursor)
            if cursor_key in seen_cursors:
                stopped_reason = "cursor_repeated"
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
                    stopped_reason = str(code)
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
        results.append({**identity, "pages": pages})
        if stopped_reason in BLOCKING_CODES:
            break
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
        "accounts_processed": len(results),
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
        "results": results,
    }
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
              SELECT 1 FROM fetch_slots f WHERE f.content_id=c.id
                AND f.stage='comments' AND f.window_key=? AND f.status='succeeded'
            )"""
        )
        window_parameters.append(week_key)
    sql = f"""
        SELECT c.id
        FROM content_items c
        JOIN accounts a ON a.id=c.account_id
        WHERE a.enabled=1
          AND c.platform IN ({','.join('?' for _ in selected_platforms)})
          AND c.published_at>=? AND c.published_at<=?
          AND ({' OR '.join(missing_clauses)})
        ORDER BY c.published_at DESC, c.id DESC
    """
    parameters: List[Any] = [
        *selected_platforms, _iso(start), _iso(end), *window_parameters,
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
    results: List[Dict[str, Any]] = []
    stopped_reason: Optional[str] = None
    for content_id in content_ids:
        result = update_content_data(
            content_id,
            as_of=as_of.astimezone(SHANGHAI).date(),
            db_path=db_path,
            call_override=call_override,
            stages=["metrics"],
            process_media=False,
            task_id=task_id,
            task_max_amount=max_amount,
        )
        results.append(result)
        blocking = next(
            (
                str(item.get("error_code")) for item in result.get("stages", [])
                if item.get("error_code") in BLOCKING_CODES
            ),
            None,
        )
        if blocking:
            stopped_reason = blocking
            break
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
        "processed": len(results),
        "stopped_reason": stopped_reason,
        "usage": usage,
        "results": results,
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
) -> Dict[str, Any]:
    selected_stages = list(dict.fromkeys(stages or ("detail", "metrics", "comments")))
    day_key = end.astimezone(SHANGHAI).date().isoformat()
    with connect(db_path) as connection:
        zero_comment_rows = connection.execute(
            """
            SELECT c.id
            FROM content_items c
            JOIN accounts a ON a.id=c.account_id
            JOIN content_metric_snapshots m
              ON m.content_id=c.id AND m.window_key=?
             AND m.status='available' AND m.comment_count=0
            WHERE a.enabled=1 AND c.platform IN ('douyin','xiaohongshu')
              AND c.published_at>=? AND c.published_at<=?
              AND NOT EXISTS (
                SELECT 1 FROM fetch_slots f
                WHERE f.content_id=c.id AND f.stage='comments'
                  AND f.window_key=? AND f.status='succeeded'
              )
            ORDER BY c.published_at DESC,c.id DESC
            """,
            (
                day_key,
                _iso(start),
                _iso(end),
                f"{end.astimezone(SHANGHAI).date().isocalendar().year}-W"
                f"{end.astimezone(SHANGHAI).date().isocalendar().week:02d}",
            ),
        ).fetchall() if "comments" in selected_stages else []
    zero_comment_results = [
        materialize_zero_comment_evidence(
            int(row["id"]),
            as_of=end.astimezone(SHANGHAI).date(),
            db_path=db_path,
        )
        for row in zero_comment_rows
    ]
    content_ids = pending_content_ids(
        start=start, end=end, as_of=end, db_path=db_path, limit=limit,
        platforms=platforms, stages=selected_stages,
    )
    results: List[Dict[str, Any]] = []
    stopped_reason: Optional[str] = None
    for content_id in content_ids:
        result = update_content_data(
            content_id, as_of=end.astimezone(SHANGHAI).date(), db_path=db_path,
            call_override=call_override, stages=selected_stages,
            process_media=False, task_id=task_id, task_max_amount=max_amount,
        )
        results.append(result)
        blocking = next(
            (
                str(item.get("error_code")) for item in result.get("stages", [])
                if item.get("error_code") in BLOCKING_CODES
            ),
            None,
        )
        if blocking:
            stopped_reason = blocking
            break
    usage = _task_usage(task_id, db_path=db_path)
    status = "blocked" if stopped_reason else (
        "partial" if any(item["status"] == "partial" for item in results)
        else "succeeded"
    )
    output = {
        "task_id": task_id,
        "status": status,
        "candidates": len(content_ids),
        "processed": len(results),
        "zero_comment_evidence": {
            "candidates": len(zero_comment_rows),
            "succeeded": sum(
                item["status"] in {"succeeded", "already_succeeded", "replayed"}
                for item in zero_comment_results
            ),
        },
        "stopped_reason": stopped_reason,
        "usage": usage,
        "results": results,
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
) -> Dict[str, Any]:
    with connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT c.id FROM content_items c
            JOIN accounts a ON a.id=c.account_id
            WHERE a.enabled=1 AND c.platform IN ('douyin','xiaohongshu')
              AND c.published_at>=? AND c.published_at<=?
            ORDER BY c.published_at DESC, c.id DESC LIMIT ?
            """,
            (_iso(start), _iso(end), limit),
        ).fetchall()
    results: List[Dict[str, Any]] = []
    for row in rows:
        content_id = int(row["id"])
        try:
            media = process_content_media(content_id, db_path=db_path)
            evaluation = evaluate_content(content_id, db_path=db_path)
            duplicates = refresh_content_duplicates(content_id, db_path=db_path)
            results.append(
                {
                    "content_id": content_id, "status": str(media.get("status")),
                    "media": media, "evaluation_id": evaluation.evaluation_id,
                    "evaluation_created": evaluation.created,
                    "duplicates": duplicates,
                }
            )
        except Exception as exc:
            results.append(
                {
                    "content_id": content_id, "status": "retryable_failed",
                    "error": f"{type(exc).__name__}: {exc}"[:500],
                }
            )
    failed = sum(item["status"] == "retryable_failed" for item in results)
    output = {
        "task_id": task_id,
        "status": "partial" if failed else "succeeded",
        "candidates": len(rows),
        "processed": len(results),
        "failed": failed,
        "usage": _task_usage(task_id, db_path=db_path),
        "results": results,
    }
    state_path = _write_state(
        task_id=task_id, start=start, end=end, max_amount=max_amount,
        phase="local_evidence", status=output["status"], details=output,
        state_root=state_root,
    )
    output["state_path"] = str(state_path)
    return output


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "phase",
        choices=(
            "discover", "content", "local-evidence",
            "repair-metrics", "fetch-repaired-metrics",
        ),
    )
    parser.add_argument("--start", default=DEFAULT_START.isoformat())
    parser.add_argument("--end", required=True)
    parser.add_argument("--task-id")
    parser.add_argument("--max-amount", type=float, default=DEFAULT_MAX_AMOUNT)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--max-pages", type=int, default=20)
    parser.add_argument("--platform", action="append", choices=("douyin", "xiaohongshu"))
    parser.add_argument("--stage", action="append", choices=("detail", "metrics", "comments"))
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    values = parser.parse_args(argv)
    start = _parse_datetime(values.start)
    end = _parse_datetime(values.end)
    task_id = values.task_id or task_id_for(start, end)
    if values.phase == "discover":
        result = run_discovery_backfill(
            start=start, end=end, task_id=task_id, max_amount=values.max_amount,
            db_path=values.db, platforms=values.platform,
            account_limit=values.limit, max_pages_per_account=values.max_pages,
        )
    elif values.phase == "content":
        result = run_content_backfill(
            start=start, end=end, task_id=task_id, max_amount=values.max_amount,
            db_path=values.db, limit=values.limit, platforms=values.platform,
            stages=values.stage,
        )
    elif values.phase == "local-evidence":
        result = run_local_evidence_backfill(
            start=start, end=end, task_id=task_id, max_amount=values.max_amount,
            db_path=values.db, limit=values.limit or 100,
        )
    elif values.phase == "repair-metrics":
        result = repair_discovery_placeholder_metrics(
            start=start, end=end, as_of=end, db_path=values.db,
            platforms=values.platform, limit=values.limit,
            apply_changes=values.apply,
        )
    else:
        result = run_repaired_metrics_backfill(
            start=start, end=end, as_of=end, task_id=task_id,
            max_amount=values.max_amount, db_path=values.db,
            platforms=values.platform, limit=values.limit,
        )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result["status"] not in {"blocked", "partial"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
