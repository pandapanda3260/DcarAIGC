"""Read-only HTTP projections for source-bound media, plus leased originals.

Original and preview identities never share a URL. Listing evidence does not
enqueue work, refresh provider URLs, or mutate a business fact.
"""

from __future__ import annotations

import hashlib
import json
import mimetypes
import os
import re
import sqlite3
import stat
from pathlib import Path
from typing import Any, Mapping

from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.types import Receive, Scope, Send

from . import media, media_lifecycle as lifecycle, media_retention as retention
from .media_completion import PREVIEW_VERSION
from .source_routing import parse_time
from .storage import connect, now_utc


MESSAGES = {
    "original_available": "原件可用。",
    "original_archived": "原件已归档，可申请本地恢复；预览与已保存结论仍可查看。",
    "original_restoring": "原件恢复已入队，等待本地处理；不会重新抓取或产生供应商费用。",
    "original_expiry_pending": "已到删除时间，等待安全删除；不能再恢复原件。",
    "original_purge_in_progress": "原件正在安全删除或等待删除重试，不能再恢复。",
    "original_expired": "原件已到期删除，不能重放；预览与已保存结论仍保留。",
    "replica_original_omitted": "线上只读副本不包含原件，请在本地查看原件状态。",
    "original_missing": "已登记的原件意外缺失，请联系管理员核查；不会自动付费重抓。",
    "original_integrity_error": "媒体身份或完整性校验失败，已停止提供原件，请联系管理员。",
    "managed_source_pending": "当前来源尚未完成新实例登记，不会回退到旧来源。",
    "media_bundle_busy": "原件正在由另一个本地作业处理，请稍后查看。",
    "explicit_reacquire_contract_not_bound": "尚未建立独立的新媒体获取任务及授权，已拒绝付费重新获取。",
}


def error_payload(code: str, *, availability: Mapping[str, Any] | None = None) -> dict[str, Any]:
    code = "original_expired" if code == "expired_non_replayable" else code
    return {"detail": MESSAGES.get(code, MESSAGES["original_integrity_error"]),
            "code": code, "can_restore": bool(availability and availability.get("can_restore")),
            "media_lifecycle": dict(availability) if availability is not None else None}


def error_response(error: Exception, *, availability: Mapping[str, Any] | None = None) -> JSONResponse:
    code = str(getattr(error, "error_code", "original_integrity_error"))
    statuses = {"original_archived": 409, "original_restoring": 202,
                "original_expiry_pending": 409, "original_purge_in_progress": 409,
                "original_expired": 410, "expired_non_replayable": 410,
                "replica_original_omitted": 409, "original_missing": 404,
                "bundle_not_found": 404, "media_member_not_found": 404,
                "restore_source_mismatch": 404,
                "media_bundle_busy": 409, "managed_source_pending": 409,
                "explicit_reacquire_contract_not_bound": 409}
    return JSONResponse(error_payload(code, availability=availability), status_code=statuses.get(code, 503))


def _require(value: Any, code: str) -> None:
    if not value:
        raise lifecycle.LifecycleError(code)


def _object(value: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(value)
    except (ValueError, TypeError) as error:
        raise lifecycle.LifecycleError("media_metadata_invalid") from error
    _require(isinstance(parsed, dict), "media_metadata_invalid")
    return parsed


def _bound_bundle(connection: sqlite3.Connection, content_id: int,
                  *, artifact_id: int | None = None) -> dict[str, Any] | None:
    if artifact_id is None:
        bundle = media._managed_bundle(connection, content_id)
        if bundle is None and media._has_managed_history(connection, content_id):
            raise lifecycle.LifecycleError("managed_source_pending")
        return bundle
    row = connection.execute("SELECT * FROM evidence_artifacts WHERE id=? AND content_id=?",
                             (artifact_id, content_id)).fetchone()
    if row is None:
        raise lifecycle.LifecycleError("media_member_not_found")
    namespace = _object(row["metadata_json"]).get("media_lifecycle")
    if not isinstance(namespace, dict) or not namespace.get("bundle_id"):
        return None
    bundle = lifecycle.load_bundle(connection, namespace["bundle_id"])
    _require(bundle["manifest"]["content_id"] == content_id, "media_member_not_found")
    _require(namespace.get("control_artifact_id") == bundle["control_artifact_id"], "media_artifact_binding_changed")
    lifecycle.original_artifact(connection, bundle)
    return bundle


def availability(connection: sqlite3.Connection, bundle: Mapping[str, Any], *, read_only: bool) -> dict[str, Any]:
    db_path = Path(connection.execute("PRAGMA database_list").fetchone()[2])
    view = retention._availability({**bundle, "db_path": db_path}, at=now_utc(), replica=read_only)
    request = bundle["state"].get("restore_request")
    if not read_only and request and request.get("status") == "pending" and view["reason"] == "original_archived":
        view.update(reason="original_restoring", http_status=202, can_restore=False)
    view.update(read_only=read_only, restore_request=request,
                protections=bundle["state"].get("protections", {}),
                completion_gate_aged=bundle["state"].get("completion_gate_aged"),
                last_error=bundle["state"].get("last_error"),
                registered_at=bundle["manifest"]["registered_at"],
                original_artifact_id=bundle["manifest"]["original_artifact"]["artifact_id"],
                original_member_count=bundle["manifest"]["member_count"],
                original_bytes=bundle["manifest"]["byte_size"],
                evidence_cutoff=bundle["state"].get("completion_receipt"),
                can_reprocess=not read_only and view["http_status"] == 200,
                can_reacquire=False, reacquire_reason="explicit_reacquire_contract_not_bound")
    return view


def _checked_bytes(path: Path, sha256: Any, byte_size: Any) -> bytes:
    _require(isinstance(sha256, str) and re.fullmatch(r"[0-9a-f]{64}", sha256)
             and type(byte_size) is int and byte_size > 0, "media_file_identity_missing")
    evidence = media._read_private_file_evidence(path, label="HTTP evidence", capture_body=True)
    _require(evidence.sha256 == sha256 and evidence.byte_size == byte_size, "media_file_identity_changed")
    return bytes(evidence.body or b"")


def _preview_members(connection: sqlite3.Connection, bundle: Mapping[str, Any],
                     *, artifact_id: int | None = None) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    rows = connection.execute(
        "SELECT * FROM evidence_artifacts WHERE content_id=? AND artifact_type='media_preview_manifest' "
        "AND status='available' ORDER BY id DESC", (bundle["manifest"]["content_id"],),
    ).fetchall()
    selected = None
    for row in rows:
        namespace = _object(row["metadata_json"]).get("media_lifecycle", {})
        if namespace.get("bundle_id") == bundle["bundle_id"] and (artifact_id is None or row["id"] == artifact_id):
            selected = dict(row)
            break
    if selected is None:
        return None, []
    expected = {"media_lifecycle": {"bundle_id": bundle["bundle_id"],
                "control_artifact_id": bundle["control_artifact_id"], "manifest_sha256": bundle["manifest_sha256"]}}
    _require(_object(selected["metadata_json"]) == expected and selected["processor_version"] == PREVIEW_VERSION,
             "preview_artifact_binding_changed")
    root = Path(bundle["evidence_root"])
    path = lifecycle._path(selected["local_path"])
    _require(path.is_relative_to(root), "preview_path_outside_bundle")
    manifest = _object(_checked_bytes(path, selected["sha256"], selected["byte_size"]))
    _require(manifest.get("contract_version") == PREVIEW_VERSION
             and manifest.get("bundle_id") == bundle["bundle_id"]
             and manifest.get("manifest_sha256") == bundle["manifest_sha256"]
             and manifest.get("media_kind") == bundle["manifest"]["media_kind"], "preview_manifest_binding_changed")
    members = manifest.get("members")
    if not isinstance(members, list) or not members:
        raise lifecycle.LifecycleError("preview_members_missing")
    originals = {item["member_id"]: item for item in bundle["manifest"]["members"]}
    paths: set[str] = set()
    result = []
    for index, member in enumerate(members):
        _require(isinstance(member, dict) and type(member.get("index")) is int and member["index"] == index,
                 "preview_member_index_changed")
        original = originals.get(member.get("source_member_id"))
        if original is None or member.get("source_sha256") != original["sha256"]:
            raise lifecycle.LifecycleError("preview_source_identity_changed")
        relative = lifecycle._member_path(member.get("relative_path"))
        _require(relative not in paths, "preview_member_path_duplicated")
        paths.add(relative)
        child = root / relative
        _checked_bytes(child, member.get("sha256"), member.get("byte_size"))
        result.append({**member, "path": child, "source_index": original["index"]})
    return selected, result


def managed_evidence(connection: sqlite3.Connection, content_id: int, *, read_only: bool) -> dict[str, Any] | None:
    bundle = _bound_bundle(connection, content_id)
    if bundle is None:
        return None
    original = lifecycle.original_artifact(connection, bundle)
    view = availability(connection, bundle, read_only=read_only)
    items = [{"artifact_id": original["id"], "index": member["index"], "member_id": member["member_id"],
              "bundle_id": bundle["bundle_id"], "sha256": member["sha256"], "byte_size": member["byte_size"],
              "kind": member["kind"], "name": Path(member["relative_path"]).name,
              "url": f"/api/v8/contents/{content_id}/evidence/files/{original['id']}/{member['index']}",
              "available": view["http_status"] == 200}
             for member in bundle["manifest"]["members"] if member["kind"] in {"image", "video"}]
    preview, preview_members = _preview_members(connection, bundle)
    previews = [] if preview is None else [
        {"artifact_id": preview["id"], "index": member["index"], "kind": "image",
         "bundle_id": bundle["bundle_id"], "member_id": member["source_member_id"],
         "original_index": member["source_index"], "name": member["path"].name,
         "url": f"/api/v8/contents/{content_id}/evidence/previews/{preview['id']}/{member['index']}",
         "sha256": member["sha256"], "byte_size": member["byte_size"], "available": True}
        for member in preview_members]
    derived: dict[str, Any] = {}
    for kind in ("asr", "ocr"):
        _, row = media.managed_bound_artifact(connection, content_id, (kind,))
        derived[kind + "_payload"] = _object(_checked_bytes(lifecycle._path(row["local_path"]), row["sha256"], row["byte_size"])) if row else {"status": "missing"}
    return {"media": items, "previews": previews, "media_lifecycle": view,
            "media_availability": {"status": "available" if view["http_status"] == 200 else "omitted" if read_only else "unavailable",
                                   "reason": MESSAGES.get(view["reason"], MESSAGES["original_integrity_error"]), "code": view["reason"]},
            **derived}


def preview_response(connection: sqlite3.Connection, content_id: int, artifact_id: int, index: int) -> Response:
    bundle = _bound_bundle(connection, content_id, artifact_id=artifact_id)
    if bundle is None:
        raise lifecycle.LifecycleError("media_member_not_found")
    artifact, members = _preview_members(connection, bundle, artifact_id=artifact_id)
    _require(artifact and 0 <= index < len(members), "media_member_not_found")
    member = members[index]
    body = _checked_bytes(member["path"], member["sha256"], member["byte_size"])
    return Response(body, media_type="image/jpeg", headers={"ETag": '"' + member["sha256"] + '"',
                    "Cache-Control": "private, no-store", "X-Dcar-Bundle": bundle["bundle_id"]})


_IMAGE_MAGIC = (
    (b"\xff\xd8\xff", "image/jpeg"),
    (b"\x89PNG\r\n\x1a\n", "image/png"),
    (b"GIF87a", "image/gif"),
    (b"GIF89a", "image/gif"),
)


def sniff_media_type(header: bytes) -> str | None:
    """Map the first bytes of a verified original to a media type.

    Bundle manifests record a member ``kind`` but not its container format,
    and legacy image originals are stored as ``image-NNN.bin``; the response
    must not fall back to ``application/octet-stream`` for those.  Patterns
    follow the WHATWG MIME Sniffing image table plus MP4/WebM containers.
    """
    for magic, media_type in _IMAGE_MAGIC:
        if header.startswith(magic):
            return media_type
    if header[:4] == b"RIFF" and header[8:12] == b"WEBP":
        return "image/webp"
    if header[4:8] == b"ftyp":
        return "video/mp4"
    if header.startswith(b"\x1a\x45\xdf\xa3"):
        return "video/webm"
    return None


def media_content_type(path: Path, header: bytes | None = None) -> str:
    """Content-Type for an evidence original: suffix first, then magic bytes."""
    guessed = mimetypes.guess_type(str(path))[0]
    if guessed and path.suffix.lower() != ".bin":
        return guessed
    if header is None:
        try:
            with Path(path).open("rb") as handle:
                header = handle.read(16)
        except OSError:
            header = b""
    return sniff_media_type(header) or "application/octet-stream"


class LeasedOriginalResponse(Response):
    """Keep the shared lease and verified descriptor through final ASGI body."""

    def __init__(self, *, db_path: Path, content_id: int, artifact_id: int,
                 bundle_id: str, index: int) -> None:
        super().__init__(b"")
        self.db_path, self.content_id = db_path, content_id
        self.artifact_id, self.bundle_id, self.index = artifact_id, bundle_id, index

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        started = False

        async def tracked_send(message):
            nonlocal started
            if message["type"] == "http.response.start":
                started = True
            await send(message)

        try:
            with retention.media_read_lease(self.content_id, db_path=self.db_path, purpose="file_response", bundle_id=self.bundle_id) as bundle:
                if bundle is None:
                    raise lifecycle.LifecycleError("media_member_not_found")
                with connect(self.db_path) as connection:
                    original = lifecycle.original_artifact(connection, bundle)
                    _require(original["id"] == self.artifact_id, "media_member_not_found")
                members = bundle["manifest"]["members"]
                _require(0 <= self.index < len(members), "media_member_not_found")
                member = members[self.index]
                path = retention._member_path(bundle["originals_root"], member)
                descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
                with os.fdopen(descriptor, "rb") as handle:
                    info = os.fstat(handle.fileno())
                    _require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size == member["byte_size"], "media_file_identity_changed")
                    header = handle.read(16)
                    handle.seek(0)
                    digest = hashlib.sha256()
                    while chunk := handle.read(1024 * 1024):
                        digest.update(chunk)
                    _require(digest.hexdigest() == member["sha256"], "media_file_identity_changed")
                    after = os.fstat(handle.fileno())
                    _require((info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
                             == (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns),
                             "media_file_changed_during_response")
                    handle.seek(0)
                    size = int(member["byte_size"])
                    start, end, status_code = 0, size - 1, 200
                    headers = {"Accept-Ranges": "bytes", "ETag": '"' + member["sha256"] + '"',
                               "Cache-Control": "private, no-store", "X-Dcar-Bundle": self.bundle_id}
                    range_value = next((value.decode("latin-1") for key, value in scope.get("headers", []) if key.lower() == b"range"), None)
                    if range_value:
                        match = re.fullmatch(r"bytes=(\d*)-(\d*)", range_value)
                        if not match or not any(match.groups()):
                            await Response(status_code=416, headers={"Content-Range": f"bytes */{size}"})(scope, receive, tracked_send)
                            return
                        left, right = match.groups()
                        if left:
                            start, end = int(left), min(int(right), size - 1) if right else size - 1
                        else:
                            start, end = max(0, size - int(right)), size - 1
                        if start > end or start >= size:
                            await Response(status_code=416, headers={"Content-Range": f"bytes */{size}"})(scope, receive, tracked_send)
                            return
                        status_code = 206
                        headers["Content-Range"] = f"bytes {start}-{end}/{size}"
                    headers["Content-Length"] = str(end - start + 1)
                    handle.seek(start)

                    def chunks():
                        remaining = end - start + 1
                        while remaining:
                            body = handle.read(min(256 * 1024, remaining))
                            if not body:
                                raise lifecycle.LifecycleError("media_file_changed_during_response")
                            remaining -= len(body)
                            yield body

                    content_type = media_content_type(path, header)
                    response = Response(b"", status_code=status_code, headers=headers, media_type=content_type) if scope.get("method") == "HEAD" else StreamingResponse(chunks(), status_code=status_code, headers=headers, media_type=content_type)
                    await response(scope, receive, tracked_send)
        except (lifecycle.LifecycleError, media.MediaProcessingError, OSError, ValueError) as error:
            if started:
                raise
            await error_response(error)(scope, receive, send)


def original_response(connection: sqlite3.Connection, content_id: int, artifact_id: int, index: int,
                      *, db_path: Path, read_only: bool) -> Response | None:
    bundle = _bound_bundle(connection, content_id, artifact_id=artifact_id)
    if bundle is None:
        return None
    original = lifecycle.original_artifact(connection, bundle)
    _require(original["id"] == artifact_id and 0 <= index < len(bundle["manifest"]["members"]), "media_member_not_found")
    view = availability(connection, bundle, read_only=read_only)
    if view["http_status"] != 200:
        return error_response(lifecycle.LifecycleError(view["reason"]), availability=view)
    return LeasedOriginalResponse(db_path=db_path, content_id=content_id, artifact_id=artifact_id,
                                  bundle_id=bundle["bundle_id"], index=index)


def _blocker_category(reason: str) -> str:
    for category, markers in (
        ("preview", ("preview",)), ("fingerprint", ("duplicate", "fingerprint")),
        ("evaluation", ("evaluation", "release", "envelope")),
        ("asr", ("asr", "transcript")), ("ocr", ("ocr", "frame", "image_group")),
        ("source", ("source", "raw", "download")),
        ("storage", ("archive", "disk", "space", "capacity")),
    ):
        if any(marker in reason.lower() for marker in markers):
            return category
    return "other"


def _latest_processing(connection: sqlite3.Connection, bundle: Mapping[str, Any]) -> dict[str, Any] | None:
    hashes = {media.managed_slot_source(bundle, str(bundle["manifest"]["original_artifact"]["sha256"]))}
    for row in connection.execute(
        "SELECT metadata_json FROM evidence_artifacts WHERE content_id=?",
        (bundle["manifest"]["content_id"],),
    ):
        namespace = _object(row["metadata_json"]).get("media_lifecycle", {})
        if namespace.get("bundle_id") == bundle["bundle_id"] and namespace.get("input_sha256"):
            hashes.add(media.managed_slot_source(bundle, namespace["input_sha256"]))
    placeholders = ",".join("?" for _ in hashes)
    row = connection.execute(
        f"""SELECT processor_type,status,attempt_count,updated_at FROM media_processing_slots
            WHERE content_id=? AND (id=? OR source_sha256 IN ({placeholders}))
            ORDER BY julianday(updated_at) DESC,id DESC LIMIT 1""",
        (bundle["manifest"]["content_id"], bundle["manifest"]["download_slot"]["id"], *sorted(hashes)),
    ).fetchone()
    return dict(row) if row is not None else None


def lifecycle_status(connection: sqlite3.Connection, *, read_only: bool) -> dict[str, Any]:
    """Registered-manifest inventory; never inspect an archive on a replica."""
    timestamp = now_utc()
    from .artifact_paths import installed_snapshot
    snapshot = installed_snapshot() if read_only else None
    captured_at = snapshot["manifest"]["created_at"] if snapshot else None
    lag = max(0, int((parse_time(timestamp) - parse_time(captured_at)).total_seconds())) if captured_at else None
    value = retention.lifecycle_summary(connection)
    totals = {"registered_bytes": 0, "queued": 0, "restoring": 0,
              "expiry_pending": 0, "purging": 0, "protected": 0, "blocked": 0}
    deadlines: list[str] = []
    manual = {item["bundle_id"]: item for item in value["manual_todos"]}
    expiry_debt: list[dict[str, Any]] = []
    groups: dict[str, dict[str, Any]] = {}
    active = lifecycle.activation(connection)
    activation_status = (
        None
        if active is None
        else {
            "mode": active.get("mode"),
            "revision": active.get("revision"),
            "activation_id": active.get("activation_id"),
            "release": active.get("release"),
            "activated_at": active.get("activated_at"),
            "updated_at": active.get("updated_at"),
            "canary_count": len(active.get("canary_content_ids") or []),
        }
    )
    for bundle_id in retention._registered_ids(connection):
        bundle = lifecycle.load_bundle(connection, bundle_id)
        state, manifest = bundle["state"], bundle["manifest"]
        # These sizes are mandatory and hash-bound by load_bundle. Missing sizes
        # fail the manifest contract; they cannot silently become zero bytes.
        registered_bytes = int(manifest["byte_size"])
        totals["registered_bytes"] += registered_bytes
        if (state.get("restore_request") or {}).get("status") == "pending":
            totals["queued"] += 1
        if state.get("operation_state") == "restoring":
            totals["restoring"] += 1
        if state.get("operation_state") == "purging":
            totals["purging"] += 1
        if state.get("protections"):
            totals["protected"] += 1
        if state.get("last_error") or state.get("operation_state") == "blocked":
            totals["blocked"] += 1
        due = state.get("delete_due_at")
        if due and state.get("storage_state") != "expired":
            deadlines.append(due)
            overdue = int((parse_time(timestamp) - parse_time(due)).total_seconds())
            if overdue >= 0:
                totals["expiry_pending"] += 1
                reason = ("purge_failed" if state.get("last_error") else "purging") if state.get("operation_state") == "purging" else (
                    "protected" if state.get("protections") else "restore_in_flight" if state.get("operation_state") == "restoring"
                    else "lifecycle_paused" if not active or active.get("mode") != "active" else "awaiting_retention_worker")
                expiry_debt.append({
                    "bundle_id": bundle_id, "content_id": manifest["content_id"], "link_id": manifest["link_id"],
                    "account_id": manifest["account_id"], "platform": manifest["platform"],
                    "registered_bytes": registered_bytes, "delete_due_at": due, "overdue_seconds": overdue,
                    "operation_state": state.get("operation_state"), "delay_reason": reason,
                    "protected": bool(state.get("protections")), "last_error": state.get("last_error"),
                })
        if bundle_id in manual:
            account = connection.execute(
                "SELECT nickname FROM account_platform_identities WHERE account_id=? AND platform=? AND uid=?",
                (manifest["account_id"], manifest["platform"], manifest["account_uid"]),
            ).fetchone()
            blockers = list(state.get("completion_blockers") or [])
            if not blockers:
                blockers = ["completion_not_sealed"]
            manual[bundle_id].update(
                content_id=manifest["content_id"], link_id=manifest["link_id"],
                account_id=manifest["account_id"], platform=manifest["platform"], account_uid=manifest["account_uid"],
                account_name=account["nickname"] or None if account else None,
                registered_at=manifest["registered_at"], registered_bytes=registered_bytes,
                member_count=manifest["member_count"], protections=state.get("protections", {}),
                latest_processing=_latest_processing(connection, bundle), blockers=blockers,
            )
            for category in {_blocker_category(str(reason)) for reason in blockers}:
                group = groups.setdefault(category, {"category": category, "count": 0, "registered_bytes": 0})
                group["count"] += 1
                group["registered_bytes"] += registered_bytes
    jobs = [dict(row) for row in connection.execute(
        """SELECT id,job_id,status,started_at,completed_at,
                  json_extract(details_json,'$.summary.reason') reason FROM scheduler_runs
           WHERE job_id IN ('media_archive','media_restore','media_retention','media_hot_release','media_aged')
             AND id IN (SELECT MAX(id) FROM scheduler_runs GROUP BY job_id) ORDER BY job_id""")]
    archive_health = "not_mounted_in_replica" if read_only else "not_activated"
    if active and not read_only:
        try:
            lifecycle._directory(Path(active["archive_root"]["path"]))
            archive_health = "available"
        except (lifecycle.LifecycleError, OSError, KeyError, TypeError):
            archive_health = "unavailable"
    return {**value, "read_only": read_only, "snapshot_only": read_only, "totals": totals,
            "snapshot_captured_at": captured_at, "snapshot_lag_seconds": lag,
            "manual_todos": sorted(manual.values(), key=lambda item: (item["registered_at"], item["bundle_id"])),
            "manual_count": len(manual), "manual_bytes": sum(item["registered_bytes"] for item in manual.values()),
            "manual_longest_age_seconds": max((int(item["age_seconds"]) for item in manual.values()), default=0),
            "blocker_groups": sorted(groups.values(), key=lambda item: item["category"]),
            "expiry_debt": sorted(expiry_debt, key=lambda item: (parse_time(item["delete_due_at"]), item["bundle_id"])),
            "expiry_debt_bytes": sum(item["registered_bytes"] for item in expiry_debt),
            "expiry_debt_longest_overdue_seconds": max((item["overdue_seconds"] for item in expiry_debt), default=0),
            "earliest_delete_due_at": min(deadlines, key=parse_time) if deadlines else None,
            "latest_jobs": jobs, "archive_root_health": archive_health,
            "activation": activation_status}
