"""Bounded Xiaohongshu legacy-media backfill using Rnote video detail."""

from __future__ import annotations

import argparse
import json
import re
import socket
import ssl
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from .capture import (
    BudgetBlocked,
    CaptureError,
    ProviderResult,
    SlotUnavailable,
    activate_pilot_budget,
    ensure_content_slot,
    evaluate_pilot_gate,
    execute_content_fetch,
)
from .media import (
    MEDIA_ROOT,
    MediaProcessingError,
    compile_ocr_binary,
    download_video_sources,
    ingest_existing_video_evidence,
    process_video_evidence,
    register_artifact,
)
from .storage import DEFAULT_DB, PROJECT_ROOT, connect, now_utc, transaction


BUDGET_ID = "legacy-xhs-media-backfill-v1"
PROVIDER = "Rnote"
OPERATION = "xiaohongshu_video_detail"
ADAPTER_VERSION = "rnote-video-v8.0"
ENDPOINT = "https://rnote.dev/api/v2/crawler/note/video"
KEY_FILE = Path("/Users/mark/Documents/key/DcarKey/Rnote.env.local")
VERIFIED_UNIT_PRICE = 0.008


def load_key(path: Path) -> str:
    if not path.is_file():
        raise CaptureError(
            f"Rnote key file not found: {path}",
            retryable=False,
            error_code="credential_missing",
        )
    text = path.read_text(encoding="utf-8-sig").strip()
    if "=" in text:
        name, text = text.split("=", 1)
        if name.strip() not in {"RNOTE_API_KEY", "X_API_KEY", "API_KEY"}:
            raise CaptureError(
                "Rnote key file has an unsupported variable name",
                retryable=False,
                error_code="credential_invalid",
            )
        text = text.strip().strip("\"'")
    if not text.startswith("sk-") or any(character.isspace() for character in text):
        raise CaptureError(
            "Rnote key file is invalid",
            retryable=False,
            error_code="credential_invalid",
        )
    return text


def first_value(mapping: Mapping[str, Any], names: Iterable[str]) -> Any:
    for name in names:
        value = mapping.get(name)
        if value not in (None, "", [], {}):
            return value
    return None


def _https_url(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    value = value.strip()
    if value.startswith("http://"):
        return "https://" + value[7:]
    return value if value.startswith("https://") else ""


def collect_media_urls(value: Any, path: str = "", depth: int = 0) -> List[Dict[str, str]]:
    if depth > 8:
        return []
    result: List[Dict[str, str]] = []
    if isinstance(value, str):
        url = _https_url(value)
        if url and any(
            token in path.lower() for token in ("video", "stream", "master", "h264", "h265", "url")
        ):
            result.append({"path": path, "url": url})
    elif isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}" if path else str(key)
            result.extend(collect_media_urls(child, child_path, depth + 1))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            result.extend(collect_media_urls(child, f"{path}[{index}]", depth + 1))
    unique: List[Dict[str, str]] = []
    seen: set[str] = set()
    for item in result:
        if item["url"] not in seen:
            seen.add(item["url"])
            unique.append(item)
    return unique


def find_note(payload: Any, note_id: str) -> tuple[Dict[str, Any], Dict[str, Any]]:
    containers = payload if isinstance(payload, list) else [payload]
    for container in containers:
        if not isinstance(container, dict):
            continue
        candidates = container.get("note_list")
        if not isinstance(candidates, list):
            candidates = [container]
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            returned_id = first_value(candidate, ("id", "note_id", "noteId"))
            if str(returned_id).lower() == note_id.lower():
                return candidate, container
    raise KeyError(note_id)


def _safe_json(value: bytes) -> Any:
    try:
        return json.loads(value.decode("utf-8", "replace"))
    except json.JSONDecodeError:
        return {"parse_error": "response was not valid JSON"}


def _provider_message(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    value = payload.get("error") or payload.get("detail") or payload.get("message")
    if isinstance(value, dict):
        value = value.get("message") or value.get("detail") or value.get("msg")
    return str(value or "")


def _unwrap(payload: Any) -> Any:
    if not isinstance(payload, dict):
        raise CaptureError(
            "Rnote returned a non-object response",
            retryable=True,
            error_code="invalid_response",
            http_status=200,
            billed=True,
            raw_response=payload,
        )
    if payload.get("success") is False:
        message = _provider_message(payload) or "success=false"
        terminal = bool(re.search(r"(?:not found|deleted|private|不存在|已删除|不可见)", message, re.I))
        raise CaptureError(
            f"Rnote semantic error: {message}",
            retryable=not terminal,
            error_code="content_unavailable" if terminal else "semantic_error",
            http_status=200,
            billed=bool(payload.get("billed", True)),
            raw_response=payload,
        )
    layer = payload.get("data")
    if isinstance(layer, dict) and "success" in layer:
        if layer.get("success") is False or layer.get("code") not in (None, 0, "0"):
            message = str(layer.get("msg") or layer.get("message") or layer.get("code"))
            terminal = bool(re.search(r"(?:not found|deleted|private|不存在|已删除|不可见)", message, re.I))
            raise CaptureError(
                f"Rnote upstream error: {message}",
                retryable=not terminal,
                error_code="content_unavailable" if terminal else "upstream_error",
                http_status=200,
                billed=bool(payload.get("billed", True)),
                raw_response=payload,
            )
        layer = layer.get("data")
    if layer is None:
        raise CaptureError(
            "Rnote response contains no data",
            retryable=True,
            error_code="empty_response",
            http_status=200,
            billed=bool(payload.get("billed", True)),
            raw_response=payload,
        )
    return layer


def _epoch_utc(value: Any) -> Optional[str]:
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        return None
    if seconds <= 0:
        return None
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def normalize_video_detail(payload: Any, note_id: str) -> Dict[str, Any]:
    data = _unwrap(payload)
    try:
        note, _container = find_note(data, note_id)
    except Exception as exc:
        raise CaptureError(
            "Rnote detail did not contain the requested note",
            retryable=False,
            error_code="content_unavailable",
            http_status=200,
            billed=bool(payload.get("billed", True)) if isinstance(payload, dict) else True,
            raw_response=payload,
        ) from exc
    sections = [
        note.get(name)
        for name in (
            "video", "video_info", "video_info_v2", "videoInfo",
            "media_stream", "mediaStream", "stream",
        )
        if note.get(name) is not None
    ]
    candidates: List[Dict[str, str]] = []
    for index, section in enumerate(sections):
        candidates.extend(collect_media_urls(section, f"media[{index}]"))
    urls = list(dict.fromkeys(item["url"] for item in candidates if item["url"].startswith("https://")))
    return {
        "note_id": note_id,
        "title": str(first_value(note, ("title", "note_title", "noteTitle")) or ""),
        "body": str(first_value(note, ("desc", "description", "content")) or ""),
        "published_at": _epoch_utc(first_value(note, ("time", "publish_time", "published_at"))),
        "video_urls": urls,
    }


class RnoteVideoAdapter:
    def __init__(self, api_key: str, *, timeout: float = 60.0) -> None:
        self.api_key = api_key
        self.timeout = timeout

    def fetch(self, note_id: str) -> ProviderResult:
        query = urllib.parse.urlencode({"note_id": note_id})
        request = urllib.request.Request(
            f"{ENDPOINT}?{query}",
            headers={
                "Accept": "application/json",
                "X-API-Key": self.api_key,
                "User-Agent": "DCar-Insight-v8/1.0",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                status = int(response.status)
                payload = _safe_json(response.read())
        except urllib.error.HTTPError as exc:
            status = int(exc.code)
            payload = _safe_json(exc.read())
            provider_blocked = status in {401, 402, 403}
            terminal = status in {400, 404, 410, 422}
            raise CaptureError(
                f"Rnote HTTP {status}: {_provider_message(payload) or 'request failed'}",
                retryable=provider_blocked or (not terminal and (status in {408, 429} or status >= 500)),
                error_code=(
                    "provider_balance_blocked" if status == 402
                    else "provider_auth_blocked" if provider_blocked
                    else f"http_{status}"
                ),
                http_status=status,
                billed=False,
                raw_response=payload,
            ) from exc
        except (
            urllib.error.URLError,
            TimeoutError,
            ConnectionError,
            socket.gaierror,
            ssl.SSLError,
        ) as exc:
            raise CaptureError(
                f"Rnote transport error: {type(exc).__name__}",
                retryable=True,
                error_code="transport_error",
            ) from exc
        normalized = normalize_video_detail(payload, note_id)
        return ProviderResult(
            data=normalized,
            raw_response=payload,
            http_status=status,
            billed=bool(payload.get("billed", True)) if isinstance(payload, dict) else True,
        )


def backfill_candidates(*, db_path: Path = DEFAULT_DB) -> List[Dict[str, Any]]:
    with connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT c.id content_id, c.link_id, c.platform_content_id note_id,
                   c.canonical_url, rq.reason_code, rq.status review_status
            FROM review_queue rq
            JOIN content_items c ON c.id=rq.content_id
            WHERE rq.reason_code IN ('stale_local_evidence','media_evidence_missing')
            ORDER BY CASE rq.reason_code WHEN 'stale_local_evidence' THEN 0 ELSE 1 END,
                     c.id
            """
        ).fetchall()
    return [dict(row) for row in rows]


def prepare_backfill_slots(*, db_path: Path = DEFAULT_DB) -> Dict[str, int]:
    candidates = backfill_candidates(db_path=db_path)
    paid = [item for item in candidates if item["reason_code"] == "media_evidence_missing"]
    with connect(db_path) as connection, transaction(connection):
        for item in paid:
            ensure_content_slot(
                connection,
                content_id=int(item["content_id"]),
                stage="media_source_refresh",
                window_key="lifetime",
                provider=PROVIDER,
                adapter_version=ADAPTER_VERSION,
            )
    return {"queue_total": len(candidates), "local": len(candidates) - len(paid), "paid": len(paid)}


def ingest_local_backfill(*, db_path: Path = DEFAULT_DB) -> int:
    candidates = backfill_candidates(db_path=db_path)
    local = [item for item in candidates if item["reason_code"] == "stale_local_evidence"]
    completed = 0
    for item in local:
        note_id = str(item["note_id"])
        root = PROJECT_ROOT / "data" / "cache" / "rnote" / "media" / note_id
        ingest_existing_video_evidence(
            int(item["content_id"]),
            media_path=root / "video.mp4",
            asr_path=root / "transcript.json",
            ocr_path=root / "ocr.json",
            db_path=db_path,
        )
        completed += 1
    return completed


def _source_manifest_path(link_id: str) -> Path:
    return MEDIA_ROOT / link_id / "source.json"


def _save_source_manifest(
    *,
    db_path: Path,
    content_id: int,
    link_id: str,
    normalized: Mapping[str, Any],
    raw_response_id: int,
) -> Path:
    target = _source_manifest_path(link_id)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(".source.json.tmp")
    temporary.write_text(
        json.dumps(
            {**normalized, "raw_response_id": raw_response_id},
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    temporary.replace(target)
    with connect(db_path) as connection, transaction(connection):
        register_artifact(
            connection,
            content_id=content_id,
            artifact_type="media_source",
            path=target,
            processor_version=ADAPTER_VERSION,
        )
    return target


def _update_detail_fields(
    *, db_path: Path, content_id: int, normalized: Mapping[str, Any]
) -> None:
    with connect(db_path) as connection, transaction(connection):
        connection.execute(
            """
            UPDATE content_items
            SET title=CASE WHEN title='' THEN ? ELSE title END,
                body=CASE WHEN body='' THEN ? ELSE body END,
                published_at=COALESCE(published_at, ?), updated_at=?
            WHERE id=?
            """,
            (
                str(normalized.get("title") or ""),
                str(normalized.get("body") or ""),
                normalized.get("published_at"),
                now_utc(),
                content_id,
            ),
        )


def _mark_review(
    content_id: int,
    status: str,
    *,
    db_path: Path,
) -> None:
    with connect(db_path) as connection, transaction(connection):
        resolved = now_utc() if status in {"resolved", "terminal_failed"} else None
        connection.execute(
            """
            UPDATE review_queue
            SET status=?, resolved_at=?, updated_at=?
            WHERE content_id=? AND reason_code='media_evidence_missing'
            """,
            (status, resolved, now_utc(), content_id),
        )


def _read_source_manifest(link_id: str) -> Optional[Dict[str, Any]]:
    path = _source_manifest_path(link_id)
    if not path.is_file():
        return None
    value = json.loads(path.read_text(encoding="utf-8"))
    return value if isinstance(value, dict) else None


def process_paid_candidate(
    item: Mapping[str, Any],
    *,
    adapter: RnoteVideoAdapter,
    db_path: Path = DEFAULT_DB,
) -> str:
    content_id = int(item["content_id"])
    link_id = str(item["link_id"])
    note_id = str(item["note_id"])
    normalized = _read_source_manifest(link_id)
    if normalized is None:
        try:
            outcome = execute_content_fetch(
                content_id=content_id,
                stage="media_source_refresh",
                window_key="lifetime",
                provider=PROVIDER,
                adapter_version=ADAPTER_VERSION,
                operation=OPERATION,
                budget_id=BUDGET_ID,
                db_path=db_path,
                call=lambda: adapter.fetch(note_id),
            )
        except CaptureError as exc:
            if exc.error_code in {"provider_balance_blocked", "provider_auth_blocked"}:
                raise
            if not exc.retryable:
                _mark_review(content_id, "terminal_failed", db_path=db_path)
                return "terminal_failed"
            return "retryable_failed"
        normalized = dict(outcome.data)
        _save_source_manifest(
            db_path=db_path,
            content_id=content_id,
            link_id=link_id,
            normalized=normalized,
            raw_response_id=outcome.raw_response_id,
        )
        _update_detail_fields(db_path=db_path, content_id=content_id, normalized=normalized)
    urls = [str(url) for url in normalized.get("video_urls", []) if isinstance(url, str)]
    if not urls:
        _mark_review(content_id, "terminal_failed", db_path=db_path)
        return "terminal_failed"
    try:
        media = download_video_sources(content_id, urls, db_path=db_path)
        process_video_evidence(content_id, PROJECT_ROOT / media.local_path, db_path=db_path)
    except MediaProcessingError:
        return "retryable_failed"
    _mark_review(content_id, "resolved", db_path=db_path)
    return "evidence_ready"


def _pilot_items(*, db_path: Path) -> List[Dict[str, Any]]:
    return [
        item for item in backfill_candidates(db_path=db_path)
        if item["reason_code"] == "media_evidence_missing"
    ][:20]


def pilot_status(*, db_path: Path = DEFAULT_DB) -> Dict[str, Any]:
    items = _pilot_items(db_path=db_path)
    ids = [int(item["content_id"]) for item in items]
    if not ids:
        return {"attempted": 0, "media_recovered": 0, "evidence_ready": 0, "terminal_failed": 0}
    placeholders = ",".join("?" for _ in ids)
    with connect(db_path) as connection:
        attempted = int(
            connection.execute(
                f"""
                SELECT COUNT(*) FROM fetch_slots
                WHERE content_id IN ({placeholders}) AND stage='media_source_refresh'
                  AND attempt_count > 0
                """,
                ids,
            ).fetchone()[0]
        )
        media_recovered = int(
            connection.execute(
                f"""
                SELECT COUNT(DISTINCT content_id) FROM evidence_artifacts
                WHERE content_id IN ({placeholders}) AND artifact_type='media'
                  AND status='available' AND processor_version='provider-media-v8.0'
                """,
                ids,
            ).fetchone()[0]
        )
        evidence_ready = int(
            connection.execute(
                f"""
                SELECT COUNT(*) FROM review_queue
                WHERE content_id IN ({placeholders}) AND reason_code='media_evidence_missing'
                  AND status='resolved'
                """,
                ids,
            ).fetchone()[0]
        )
        terminal = int(
            connection.execute(
                f"""
                SELECT COUNT(*) FROM review_queue
                WHERE content_id IN ({placeholders}) AND reason_code='media_evidence_missing'
                  AND status='terminal_failed'
                """,
                ids,
            ).fetchone()[0]
        )
    return {
        "attempted": attempted,
        "media_recovered": media_recovered,
        "evidence_ready": evidence_ready,
        "terminal_failed": terminal,
    }


def run_pilot(*, db_path: Path = DEFAULT_DB, key_file: Path = KEY_FILE) -> Dict[str, Any]:
    with connect(db_path) as connection:
        budget = connection.execute(
            "SELECT status FROM provider_budget_batches WHERE id=?", (BUDGET_ID,)
        ).fetchone()
    if budget is None:
        raise BudgetBlocked("backfill budget is missing")
    if budget["status"] == "draft":
        activate_pilot_budget(
            BUDGET_ID, expected_unit_price=VERIFIED_UNIT_PRICE, db_path=db_path
        )
    adapter = RnoteVideoAdapter(load_key(key_file))
    results: Dict[str, int] = {}
    for item in _pilot_items(db_path=db_path):
        try:
            item_status = process_paid_candidate(item, adapter=adapter, db_path=db_path)
        except CaptureError as exc:
            if exc.error_code in {"provider_balance_blocked", "provider_auth_blocked"}:
                results[exc.error_code] = results.get(exc.error_code, 0) + 1
                break
            raise
        except (BudgetBlocked, SlotUnavailable):
            item_status = "blocked"
        results[item_status] = results.get(item_status, 0) + 1
    pilot_metrics = pilot_status(db_path=db_path)
    with connect(db_path) as connection:
        budget_status = str(
            connection.execute(
                "SELECT status FROM provider_budget_batches WHERE id=?", (BUDGET_ID,)
            ).fetchone()[0]
        )
    gate: Optional[Dict[str, Any]] = None
    provider_blocked = any(
        key in results for key in ("provider_balance_blocked", "provider_auth_blocked")
    )
    if pilot_metrics["attempted"] == 20 and budget_status == "suspended" and not provider_blocked:
        gate = evaluate_pilot_gate(
            BUDGET_ID,
            attempted=20,
            media_recovered=int(pilot_metrics["media_recovered"]),
            evidence_ready=int(pilot_metrics["evidence_ready"]),
            db_path=db_path,
        )
    return {"results": results, "status": pilot_metrics, "gate": gate}


def run_daily_backfill_batch(
    *,
    limit: int = 20,
    db_path: Path = DEFAULT_DB,
    key_file: Path = KEY_FILE,
) -> Dict[str, Any]:
    """Run the bounded automatic backfill without bypassing the pilot or daily quota."""

    if limit <= 0:
        raise ValueError("limit must be positive")
    with connect(db_path) as connection:
        budget = connection.execute(
            "SELECT * FROM provider_budget_batches WHERE id=?", (BUDGET_ID,)
        ).fetchone()
    if budget is None:
        return {"status": "skipped", "reason": "backfill budget is missing", "attempted": 0}
    budget_status = str(budget["status"])
    if budget_status in {"draft", "pilot"}:
        return {"status": "pilot", **run_pilot(db_path=db_path, key_file=key_file)}
    if budget_status != "approved":
        return {
            "status": "skipped",
            "reason": f"backfill budget is {budget_status}",
            "attempted": 0,
        }
    items = [
        item for item in backfill_candidates(db_path=db_path)
        if item["reason_code"] == "media_evidence_missing"
        and item["review_status"] in {"pending", "manual_required"}
    ][: min(limit, int(budget["daily_quota"]))]
    if not items:
        return {"status": "succeeded", "attempted": 0, "results": {}}
    adapter = RnoteVideoAdapter(load_key(key_file))
    results: Dict[str, int] = {}
    attempted = 0
    for item in items:
        try:
            item_status = process_paid_candidate(item, adapter=adapter, db_path=db_path)
            attempted += 1
        except CaptureError as exc:
            results[exc.error_code] = results.get(exc.error_code, 0) + 1
            if exc.error_code in {"provider_balance_blocked", "provider_auth_blocked"}:
                break
            continue
        except (BudgetBlocked, SlotUnavailable) as exc:
            results[type(exc).__name__] = results.get(type(exc).__name__, 0) + 1
            break
        results[item_status] = results.get(item_status, 0) + 1
    return {
        "status": "partial" if any(key.endswith("blocked") for key in results) else "succeeded",
        "attempted": attempted,
        "results": results,
        "routes": route_summary(db_path=db_path),
    }


def route_remaining_manual_after_failed_gate(*, db_path: Path = DEFAULT_DB) -> int:
    with connect(db_path) as connection, transaction(connection):
        budget = connection.execute(
            "SELECT status FROM provider_budget_batches WHERE id=?", (BUDGET_ID,)
        ).fetchone()
        if budget is None or budget["status"] != "suspended":
            raise BudgetBlocked("remaining items can only be routed after a failed suspended pilot")
        cursor = connection.execute(
            """
            UPDATE review_queue SET status='manual_required', updated_at=?
            WHERE reason_code='media_evidence_missing' AND status='pending'
            """,
            (now_utc(),),
        )
    return int(cursor.rowcount)


def route_provider_blocked_manual(*, db_path: Path = DEFAULT_DB) -> int:
    """Repair and route a batch when provider auth/balance prevents a valid pilot."""

    with connect(db_path) as connection, transaction(connection):
        blocked = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM fetch_attempts
                WHERE error_code IN ('http_401','http_402','http_403',
                                     'provider_balance_blocked','provider_auth_blocked')
                """
            ).fetchone()[0]
        )
        if blocked == 0:
            raise BudgetBlocked("no provider auth or balance failure is recorded")
        connection.execute(
            """
            UPDATE fetch_slots SET status='retryable_failed',
                last_error_code=CASE last_error_code
                    WHEN 'http_402' THEN 'provider_balance_blocked'
                    WHEN 'http_401' THEN 'provider_auth_blocked'
                    WHEN 'http_403' THEN 'provider_auth_blocked'
                    ELSE last_error_code END,
                updated_at=?
            WHERE id IN (
                SELECT slot_id FROM fetch_attempts
                WHERE error_code IN ('http_401','http_402','http_403',
                                     'provider_balance_blocked','provider_auth_blocked')
            )
            """,
            (now_utc(),),
        )
        connection.execute(
            """
            UPDATE fetch_attempts SET error_code=CASE error_code
                WHEN 'http_402' THEN 'provider_balance_blocked'
                WHEN 'http_401' THEN 'provider_auth_blocked'
                WHEN 'http_403' THEN 'provider_auth_blocked'
                ELSE error_code END
            WHERE error_code IN ('http_401','http_402','http_403')
            """
        )
        cursor = connection.execute(
            """
            UPDATE review_queue SET status='manual_required', resolved_at=NULL, updated_at=?
            WHERE reason_code='media_evidence_missing'
              AND status IN ('pending','terminal_failed')
            """,
            (now_utc(),),
        )
        connection.execute(
            "UPDATE provider_budget_batches SET status='suspended', updated_at=? WHERE id=?",
            (now_utc(), BUDGET_ID),
        )
    return int(cursor.rowcount)


def route_summary(*, db_path: Path = DEFAULT_DB) -> Dict[str, int]:
    with connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT status, COUNT(*) count FROM review_queue
            WHERE reason_code IN ('stale_local_evidence','media_evidence_missing')
            GROUP BY status
            """
        ).fetchall()
    return {str(row["status"]): int(row["count"]) for row in rows}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=("prepare", "pilot", "status", "route-failed-pilot", "route-provider-blocked"),
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--key-file", type=Path, default=KEY_FILE)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "prepare":
        compile_ocr_binary()
        result: Any = {
            "slots": prepare_backfill_slots(db_path=args.db),
            "local_evidence_ready": ingest_local_backfill(db_path=args.db),
            "routes": route_summary(db_path=args.db),
        }
    elif args.command == "pilot":
        result = run_pilot(db_path=args.db, key_file=args.key_file)
    elif args.command == "route-failed-pilot":
        result = {
            "routed": route_remaining_manual_after_failed_gate(db_path=args.db),
            "routes": route_summary(db_path=args.db),
        }
    elif args.command == "route-provider-blocked":
        result = {
            "routed": route_provider_blocked_manual(db_path=args.db),
            "routes": route_summary(db_path=args.db),
        }
    else:
        result = {"pilot": pilot_status(db_path=args.db), "routes": route_summary(db_path=args.db)}
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
