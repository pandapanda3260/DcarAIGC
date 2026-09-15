"""Read-only media debt and deterministic, platform-fair local work ordering.

The projection never changes a slot, sends a provider request, or opens raw
provider files. A debt item is an explanation of the current evidence DAG,
not another persisted state machine.
"""
from __future__ import annotations

from collections import Counter, defaultdict, deque
from datetime import timedelta
import hashlib
import json
import sqlite3
from typing import Any, Mapping, Sequence

from .source_routing import parse_time
from .storage import now_utc

PLATFORMS = ("douyin", "xiaohongshu", "kuaishou", "wechat_channels")
REASONS = {
    "source_missing": ("来源", "没有已保存的可用媒体来源"),
    "download_pending": ("下载", "等待下载已保存的媒体来源"),
    "download_terminal_failed": ("下载", "当前来源下载失败，需核查来源或刷新"),
    "frames_pending": ("分析", "等待提取画面"),
    "asr_pending": ("分析", "等待转写语音"),
    "ocr_pending": ("分析", "等待识别画面文字"),
    "evaluation_pending": ("结论", "等待生成内容评估"),
    "frames_terminal_failed": ("分析", "画面提取失败"),
    "asr_terminal_failed": ("分析", "语音转写失败"),
    "ocr_terminal_failed": ("分析", "画面文字识别失败"),
    "restore_required": ("原件", "原件已归档，可免费恢复后继续处理"),
    "expired_non_replayable": ("原件", "原件已到期，重新获取需独立任务"),
    "original_unavailable": ("原件", "原件不可用，需核查登记与完整性"),
    "managed_source_pending": ("原件", "新来源正在等待实例登记"),
    "local_processing_busy": ("调度", "本地处理繁忙，等待下一轮"),
    "content_type_unresolved": ("类型", "作品类型尚未核验，需补充类型证据"),
    "media_capability_unverified": ("类型", "该平台的此类媒体尚未通过处理验证"),
    "decryption_material_missing": ("解密", "视频解密材料缺失，需同次响应的完整来源"),
    "decryption_failed": ("解密", "视频解密校验失败，需核查同源材料"),
    "decryption_runtime_unavailable": ("解密", "本地视频解密运行组件不可用"),
    "evaluation_release_required": ("结论", "尚未启用内容评估版本"),
}


def local_media_supported(platform: str, content_type: str) -> bool:
    return platform in PLATFORMS and (
        content_type == "video" or content_type == "image" and platform in {"douyin", "xiaohongshu"}
    )


def refresh_reason_label(reason: str) -> str:
    codes = reason.replace(";", ":").split(":")
    if "billing_unknown" in codes:
        return "本次请求的计费结果尚未确认，需核查账单与原始响应；不会自动再次付费。"
    if "content_unavailable" in codes:
        return "数据源本次未返回该作品，已发送一次请求；不会自动再次付费。"
    return "本次任务尚有待核查事项，请查看内容依据；不会自动重复付费请求。" if reason else ""


def _object(value: Any) -> dict[str, Any]:
    try:
        result = json.loads(value) if isinstance(value, str) else value
    except (ValueError, TypeError):
        return {}
    return result if isinstance(result, dict) else {}


def fair_local_order(rows: Sequence[Mapping[str, Any]], states: Mapping[int, Any], *,
                     last_platform: str | None = None) -> list[dict[str, Any]]:
    """At most four source-download picks before one oldest waiting item.

    Rotate platforms before choosing their download-priority queue, using the
    last persisted attempted platform. A one-item time budget must not let one
    platform's downloads starve another platform's source recovery or analysis.
    Explicit source expiry is respected; missing expiry never invents a TTL.
    """
    values = [dict(row) for row in rows]
    waiting = sorted(values, key=lambda r: (str(r.get("first_waiting_at") or r.get("created_at") or r.get("published_at") or ""), int(r["id"])))
    priority: dict[str, deque] = defaultdict(deque)
    normal: dict[str, deque] = defaultdict(deque)
    for row in sorted(values, key=lambda r: (not bool(r.get("source_expires_at")),
            str(r.get("source_expires_at") or r.get("source_captured_at") or r.get("created_at") or ""), int(r["id"]))):
        state = states.get(int(row["id"]))
        reason = getattr(state, "reason", None) or (state.get("reason") if isinstance(state, Mapping) else None)
        (priority if reason == "download_pending" else normal)[str(row["platform"])].append(row)
    platforms = list(PLATFORMS)
    cursor = (platforms.index(last_platform) + 1) % len(platforms) if last_platform in platforms else 0
    used: set[int] = set()
    result: list[dict[str, Any]] = []
    streak = 0
    while len(used) < len(values):
        picked = None
        if streak >= 4:
            picked = next((r for r in waiting if int(r["id"]) not in used), None)
            streak = 0
        else:
            for delta in range(len(platforms)):
                index = (cursor + delta) % len(platforms)
                for queues in (priority, normal):
                    queue = queues[platforms[index]]
                    while queue and int(queue[0]["id"]) in used:
                        queue.popleft()
                    if queue:
                        picked = queue.popleft()
                        cursor = (index + 1) % len(platforms)
                        streak = streak + 1 if queues is priority else 0
                        break
                if picked is not None:
                    break
        if picked is None:
            break
        used.add(int(picked["id"]))
        result.append(picked)
    return result


class LocalMediaSelector:
    """One oldest normal-debt slot per five committed claims, across ticks.

    History contains actual attempt IDs, never inferred past media stages.
    A skipped/backoff candidate does not consume the pending scheduling slot.
    """

    def __init__(self, rows: Sequence[Mapping[str, Any]], states: Mapping[int, Any], *,
                 last_platform: str | None = None, claimed_count: int = 0,
                 last_attempt_ids: Mapping[int, int] | None = None):
        if type(claimed_count) is not int or claimed_count < 0:
            raise ValueError("claimed_count must be a nonnegative integer")
        values = [dict(row) for row in rows]
        attempts = last_attempt_ids or {}

        def waiting_key(row):
            cid = int(row["id"])
            if cid in attempts:
                return (1, int(attempts[cid]), cid)
            return (0, str(row.get("first_waiting_at") or row.get("created_at")
                           or row.get("published_at") or ""), cid)

        waiting = sorted(values, key=waiting_key)
        normal = []
        for row in waiting:
            state = states.get(int(row["id"]))
            reason = getattr(state, "reason", None) or (state.get("reason") if isinstance(state, Mapping) else None)
            if reason != "download_pending":
                normal.append(row)
        self._regular = deque(fair_local_order(values, states, last_platform=last_platform))
        self._normal = deque(normal)
        self._oldest = deque(waiting)
        self._remaining = {int(row["id"]) for row in values}
        self._claim_pending = False
        self.claimed_count = claimed_count

    def _take(self, queue):
        while queue:
            row = queue.popleft()
            cid = int(row["id"])
            if cid in self._remaining:
                self._remaining.remove(cid)
                return row
        return None

    def next_candidate(self) -> dict[str, Any] | None:
        self._claim_pending = False
        if self.claimed_count % 5 == 4:
            row = self._take(self._normal)
            if row is None:
                row = self._take(self._oldest)
        else:
            row = self._take(self._regular)
        self._claim_pending = row is not None
        return row

    def record_claim(self) -> None:
        if not self._claim_pending:
            raise ValueError("no selected candidate awaiting a successful claim")
        self.claimed_count += 1
        self._claim_pending = False


def list_pending_media_work(connection: sqlite3.Connection, *, limit: int = 100, offset: int = 0,
                            platform: str | None = None, reason: str | None = None,
                            account_query: str | None = None, stage: str | None = None,
                            at: str | None = None) -> dict[str, Any]:
    from .content_scope import canonical_content_predicate
    from .media_state import media_terminal_state_details

    if type(limit) is not int or not 1 <= limit <= 500 or type(offset) is not int or offset < 0:
        raise ValueError("invalid media work pagination")
    if platform not in (None, "", *PLATFORMS) or reason not in (None, "", *REASONS):
        raise ValueError("invalid media work filter")
    if stage not in (None, "", *{value[0] for value in REASONS.values()}) or (account_query is not None and len(account_query) > 128):
        raise ValueError("invalid media work filter")
    timestamp = at or now_utc()
    start = (parse_time(timestamp) - timedelta(days=30)).isoformat()
    release = connection.execute("SELECT id FROM evaluation_releases WHERE status='active'").fetchone()
    # Same forward local-analysis window; merely opening the view never starts
    # historical work. Unknown types remain visible even though the worker skips them.
    rows = connection.execute(
        "SELECT c.*,i.nickname account_name,e.sha256 source_generation,e.captured_at source_captured_at,"
        "e.metadata_json source_metadata FROM content_items c LEFT JOIN account_platform_identities i "
        "ON i.account_id=c.account_id AND i.platform=c.platform "
        "LEFT JOIN evidence_artifacts e ON e.id=(SELECT max(s.id) FROM evidence_artifacts s "
        "WHERE s.content_id=c.id AND s.artifact_type='media_source' AND s.status='available') "
        "WHERE c.platform IN ('douyin','xiaohongshu','kuaishou','wechat_channels') "
        "AND COALESCE(c.source_group,'') NOT IN ('history-backfill','history-archive') "
        "AND julianday(c.published_at) BETWEEN julianday(?) AND julianday(?) AND "
        + canonical_content_predicate(connection, "c") +
        (" AND c.platform=?" if platform else "") + " ORDER BY c.published_at,c.id",
        (start, timestamp, platform) if platform else (start, timestamp),
    ).fetchall()
    result: list[dict[str, Any]] = []
    for pos in range(0, len(rows), 300):
        batch = rows[pos:pos + 300]
        ids = [int(row["id"]) for row in batch]
        states = media_terminal_state_details(connection, str(release["id"]), ids) if release else {}
        for source in batch:
            row = dict(source)
            cid = int(row["id"])
            state = states.get(cid)
            if state is not None and state.state in {"complete", "terminal_insufficient"}:
                continue
            why = state.reason if state else "evaluation_release_required"
            if row["content_type"] not in {"video", "image"}:
                why = "content_type_unresolved"
            elif not local_media_supported(row["platform"], row["content_type"]):
                why = "media_capability_unverified"
            metadata = _object(row.get("source_metadata"))
            generation = row.get("source_generation") or "missing"
            attempts = connection.execute(
                "SELECT started_at,completed_at,details_json FROM scheduler_runs "
                "WHERE job_id='local_content_analysis' AND json_valid(details_json) "
                "AND json_extract(details_json,'$.identity.content_id')=? ORDER BY id DESC", (cid,),
            ).fetchall()
            same = []
            for attempt in attempts:
                body = _object(attempt["details_json"])
                identity = _object(body.get("identity"))
                source_identity = _object(identity.get("source"))
                if (source_identity.get("sha256") or "missing") == generation:
                    same.append((attempt, _object(body.get("checkpoint"))))
            if same and same[0][1].get("reason") in {
                "decryption_failed", "decryption_material_missing", "decryption_runtime_unavailable", "local_processing_busy",
            }:
                why = same[0][1]["reason"]
            item_stage, explanation = REASONS.get(why, ("处理", "媒体处理尚未完成，请查看依据"))
            can_restore = why == "restore_required"
            needs_source = why in {"source_missing", "download_terminal_failed", "decryption_material_missing", "expired_non_replayable"}
            free_retry = why in {"download_pending", "frames_pending", "asr_pending", "ocr_pending", "evaluation_pending", "local_processing_busy",
                                 "frames_terminal_failed", "asr_terminal_failed", "ocr_terminal_failed"}
            bundle_id = None
            if can_restore:
                from .media import _managed_bundle
                bundle = _managed_bundle(connection, cid)
                bundle_id = bundle["manifest"]["bundle_id"] if bundle else None
            item = {"content_id": cid, "platform": row["platform"], "title": row["title"],
                "canonical_url": row["canonical_url"], "account_id": row["account_id"],
                "account_uid": row.get("raw_account_uid") or "",
                "account_name": row.get("account_name") or row.get("raw_account_name") or "",
                "stage": item_stage, "reason": why, "reason_label": explanation,
                "source_generation": generation,
                "work_key": hashlib.sha256(f"{cid}:{generation}:{why}".encode()).hexdigest(),
                "first_waiting_at": same[-1][0]["started_at"] if same else row["created_at"],
                "last_failed_at": same[0][0]["completed_at"] if same else None,
                "source_captured_at": row.get("source_captured_at"),
                "source_raw_response_id": metadata.get("raw_response_id"),
                "free_retry_available": free_retry, "can_restore": bool(can_restore and bundle_id),
                "bundle_id": bundle_id, "paid_refresh_required": needs_source,
                "paid_refresh_available": needs_source,
                "next_action": "restore" if can_restore else "refresh_source" if needs_source else "retry_local" if free_retry else "inspect",
                "provider_calls": 0}
            result.append(item)
    needle = (account_query or "").strip().casefold()
    result = [item for item in result if (not stage or item["stage"] == stage) and (not needle or
        any(needle in str(item[key] or "").casefold() for key in ("account_name", "account_uid", "account_id")))]
    counts = dict(Counter(item["reason"] for item in result))
    filtered = [item for item in result if not reason or item["reason"] == reason]
    refresh_tasks = []
    if connection.execute("PRAGMA user_version").fetchone()[0] >= 23:
        from .capture_commands import read_command
        proposals = connection.execute("SELECT p.*,c.title FROM media_source_refresh_proposals p JOIN content_items c ON c.id=p.content_id WHERE p.status IN ('queued','expired') AND julianday(p.created_at)>=julianday(?) "
            + ("AND p.platform=? " if platform else "") + "ORDER BY p.created_at DESC,p.id DESC LIMIT 100",
            (start, platform) if platform else (start,)).fetchall()
        for proposal in proposals:
            command = read_command(connection, run_id=int(proposal["command_id"]), content_id=int(proposal["content_id"]), at=timestamp) if proposal["command_id"] else None
            refresh_tasks.append({"task_id": proposal["id"], "content_id": proposal["content_id"],
                "title": proposal["title"], "platform": proposal["platform"], "created_at": proposal["created_at"],
                "max_amount": proposal["max_amount"], "currency": proposal["currency"], "run_id": int(proposal["command_id"]) if proposal["command_id"] else None,
                "status": command["status"] if command else proposal["status"],
                "expires_at": proposal["expires_at"], "can_requote": bool(command and command.get("can_requote")),
                "reason": command["reason"] if command else "",
                "reason_label": refresh_reason_label(command["reason"] if command else ""), "provider_calls": 0})
    return {"items": filtered[offset:offset + limit], "total": len(filtered), "counts": counts,
            "refresh_tasks": refresh_tasks,
            "limit": limit, "offset": offset, "checked_at": timestamp,
            "scope": {"published_from": start, "published_through": timestamp, "history_included": False},
            "provider_calls": 0}
