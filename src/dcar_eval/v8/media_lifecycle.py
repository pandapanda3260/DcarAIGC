"""Future-only media ownership, activation and lifecycle CAS contracts.

This module registers immutable evidence. It does not archive, restore or
delete original media. Physical operations belong to media_retention.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Mapping

from . import durable_runs
from .media_policy import AGED_DAYS as AGED_DAYS
from .media_policy import POLICY, RETENTION_HOURS, load_media_policy
from .storage import DEFAULT_DB, PROJECT_ROOT, connect, is_formal_database_path, now_utc, transaction

POLICY_VERSION = "media-retention-v1"
MANIFEST_VERSION = "media-lifecycle-manifest-v1"
INTENT_VERSION = "media-download-intent-v1"
ACTIVATION_JOB_ID = "media_lifecycle_activation"
INTENT_JOB_ID = "media_download_intent"
FIXTURE_PROOF_CONTRACT = "media-retention-fixture-proof-v1"
DEFAULT_ARCHIVE_ROOT = Path(POLICY["archive_root"])
DEFAULT_MEDIA_ROOT = PROJECT_ROOT / "data" / "cache" / "v8" / "media"


class LifecycleError(RuntimeError):
    def __init__(self, error_code: str, message: str | None = None) -> None:
        self.error_code = error_code
        super().__init__(message or error_code)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _time(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, TypeError, ValueError) as exc:
        raise LifecycleError("invalid_lifecycle_time") from exc
    if parsed.tzinfo is None:
        raise LifecycleError("invalid_lifecycle_time")
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _object(value: str) -> dict[str, Any]:
    try:
        result = json.loads(value)
    except (TypeError, json.JSONDecodeError) as exc:
        raise LifecycleError("invalid_lifecycle_json") from exc
    if not isinstance(result, dict):
        raise LifecycleError("invalid_lifecycle_json")
    return result


def _sha(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _identifier(value: Any) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{32}", value) is None:
        raise LifecycleError("invalid_bundle_id")
    return value


def _require_transaction(connection: sqlite3.Connection) -> None:
    if not connection.in_transaction:
        raise LifecycleError("lifecycle_transaction_required")


def _formal(connection: sqlite3.Connection) -> bool:
    return any(row[2] and is_formal_database_path(Path(row[2]))
               for row in connection.execute("PRAGMA database_list"))


def _path(value: str | Path) -> Path:
    from .artifact_paths import resolve
    return resolve(value, fallback_root=PROJECT_ROOT)


def _relative(path: Path) -> str:
    try:
        return path.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return str(path)


def _directory(path: Path) -> dict[str, int]:
    from .artifact_paths import replica_directory

    try:
        current = path.lstat()
        if path.absolute() != path.resolve(strict=True):
            raise LifecycleError("lifecycle_path_alias")
    except OSError as exc:
        raise LifecycleError("lifecycle_root_missing") from exc
    replica = replica_directory(path, fallback_root=PROJECT_ROOT)
    allowed = (current.st_uid in {0, os.getuid()} and stat.S_IMODE(current.st_mode) in {0o700, 0o750, 0o500, 0o550}) if replica else (
                   stat.S_IMODE(current.st_mode) == 0o700 and current.st_uid == os.getuid())
    if not stat.S_ISDIR(current.st_mode) or not allowed:
        raise LifecycleError("lifecycle_root_not_private")
    return {"device": current.st_dev, "inode": current.st_ino, "mode": stat.S_IMODE(current.st_mode)}


def _file(path: Path, *, root: Path | None = None, private: bool = True) -> dict[str, Any]:
    from .artifact_paths import replica_file

    replica = replica_file(path, fallback_root=PROJECT_ROOT)
    if root is not None:
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise LifecycleError("lifecycle_path_escape") from exc
    try:
        if path.absolute() != path.resolve(strict=True):
            raise LifecycleError("lifecycle_path_alias")
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, "rb") as handle:
            initial = os.fstat(handle.fileno())
            allowed = (initial.st_uid in {0, os.getuid()} and stat.S_IMODE(initial.st_mode) in {0o600, 0o640, 0o400, 0o440}) if replica else (
                initial.st_uid == os.getuid() and (not private or stat.S_IMODE(initial.st_mode) == 0o600))
            if not stat.S_ISREG(initial.st_mode) or initial.st_nlink != 1 or not allowed:
                raise LifecycleError("lifecycle_file_not_private")
            digest = hashlib.sha256()
            while body := handle.read(1024 * 1024):
                digest.update(body)
            final = os.fstat(handle.fileno())
        named = path.lstat()
    except OSError as exc:
        raise LifecycleError("lifecycle_file_unavailable") from exc
    def identity(item: os.stat_result) -> tuple[int, int, int, int]:
        return item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns
    if identity(initial) != identity(final) or identity(initial) != identity(named):
        raise LifecycleError("lifecycle_file_changed")
    if replica is not None and (digest.hexdigest() != replica["sha256"] or initial.st_size != replica["byte_size"]):
        raise LifecycleError("replica_artifact_hash_mismatch")
    return {"sha256": digest.hexdigest(), "byte_size": initial.st_size,
            "device": initial.st_dev, "inode": initial.st_ino}


def _activation_rows(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        "SELECT id,details_json FROM scheduler_runs WHERE job_id=? AND status='succeeded' ORDER BY id DESC",
        (ACTIVATION_JOB_ID,),
    ).fetchall()
    result = []
    for row in rows:
        data = _object(row["details_json"])
        if data.get("contract_version") != POLICY_VERSION:
            raise LifecycleError("activation_contract_mismatch")
        result.append({**data, "activation_run_id": row["id"]})
    return result


def activation(connection: sqlite3.Connection) -> dict[str, Any] | None:
    rows = _activation_rows(connection)
    return rows[0] if rows else None


def _fixture_proofs(connection: sqlite3.Connection, proofs: Mapping[str, Any] | None,
                    *, require_canary: bool, record: Mapping[str, Any] | None = None) -> None:
    # A caller-supplied boolean is never a production authorization.
    if _formal(connection):
        from .media_consumer_proofs import verify_production
        try:
            if record is None:
                raise ValueError("production_lifecycle_proofs_not_bound")
            verify_production(connection, record, require_canary=require_canary)
        except (ValueError, OSError, KeyError, TypeError) as error:
            raise LifecycleError("production_lifecycle_proofs_not_bound", str(error)) from error
        return
    required = {"mac_consumers", "server_pairing"}
    if require_canary:
        required.add("canary_restore")
    if (not isinstance(proofs, Mapping)
            or proofs.get("contract_version") != FIXTURE_PROOF_CONTRACT
            or proofs.get("fixture_only") is not True
            or any(proofs.get(key) is not True for key in required)):
        raise LifecycleError("lifecycle_activation_proof_required")


def activate(connection: sqlite3.Connection, *, mode: str, activation_id: str,
             release: str, rules_sha256: str, archive_root: Path | None = None,
             canary_content_ids: tuple[int, ...] = (), proofs: Mapping[str, Any] | None = None,
             now: str | None = None) -> dict[str, Any]:
    """Append a non-destructive activation receipt; never initialize media."""
    _require_transaction(connection)
    if mode not in {"enrollment_only", "active", "paused"}:
        raise LifecycleError("invalid_activation_mode")
    if (not activation_id or not release or not _sha(rules_sha256)
            or any(type(value) is not int or value <= 0 for value in canary_content_ids)):
        raise LifecycleError("invalid_activation_identity")
    if _formal(connection) and rules_sha256 != load_media_policy()["sha256"]:
        raise LifecycleError("activation_rules_hash_mismatch")
    previous = activation(connection)
    root = Path(archive_root or (previous or {}).get("archive_root", {}).get("path", DEFAULT_ARCHIVE_ROOT))
    if _formal(connection) and root != DEFAULT_ARCHIVE_ROOT:
        raise LifecycleError("production_archive_root_fixed")
    root_identity = {"path": str(root), **_directory(root)}
    timestamp = _iso(_time(now or now_utc()))
    if _formal(connection) and now is not None:
        raise LifecycleError("production_clock_override_forbidden")
    if previous:
        if any(previous[key] != value for key, value in {
            "activation_id": activation_id, "release": release,
            "rules_sha256": rules_sha256, "archive_root": root_identity,
        }.items()):
            raise LifecycleError("activation_identity_changed")
        payload = {key: value for key, value in previous.items() if key != "activation_run_id"}
        if canary_content_ids and sorted(set(canary_content_ids)) != payload["canary_content_ids"]:
            raise LifecycleError("activation_canary_scope_changed")
        if proofs is not None:
            payload["proofs"] = dict(proofs)
        payload["revision"] += 1
    else:
        payload = {
            "contract_version": POLICY_VERSION, "activation_id": activation_id,
            "activated_at": timestamp, "release": release, "rules_sha256": rules_sha256,
            "archive_root": root_identity, "canary_content_ids": sorted(set(canary_content_ids)),
            "proofs": dict(proofs or {}), "revision": 0,
            "preexisting_started_slots": [row[0] for row in connection.execute(
                "SELECT id FROM media_processing_slots WHERE processor_type='download' AND attempt_count>0"
            )],
            "artifact_high_watermark": connection.execute("SELECT COALESCE(MAX(id),0) FROM evidence_artifacts").fetchone()[0],
        }
    payload.update({"mode": mode, "updated_at": timestamp})
    if mode == "active":
        _fixture_proofs(connection, payload["proofs"], require_canary=True, record=payload)
    cursor = connection.execute(
        "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,completed_at,details_json) VALUES (?,?,'succeeded',?,?,?)",
        (ACTIVATION_JOB_ID, f"{POLICY_VERSION}:{activation_id}:{payload['revision']}", timestamp, timestamp, _json(payload)),
    )
    return {**payload, "activation_run_id": cursor.lastrowid}


def archive_root_for_bundle(connection: sqlite3.Connection, bundle: Mapping[str, Any],
                            override: Path | None = None) -> Path:
    bound_id = bundle["manifest"]["activation_id"]
    record = next((row for row in _activation_rows(connection) if row["activation_id"] == bound_id), None)
    if record is None:
        raise LifecycleError("bundle_activation_missing")
    root = Path(record["archive_root"]["path"])
    if override is not None:
        if _formal(connection):
            raise LifecycleError("production_archive_root_fixed")
        if Path(override) != root:
            raise LifecycleError("archive_root_binding_changed")
    if _directory(root) != {key: value for key, value in record["archive_root"].items() if key != "path"}:
        raise LifecycleError("archive_root_binding_changed")
    return root


def require_destructive_activation(connection: sqlite3.Connection, bundle: Mapping[str, Any],
                                   *, now: str | None = None) -> dict[str, Any]:
    record = activation(connection)
    if record is None or record["activation_id"] != bundle["manifest"]["activation_id"]:
        raise LifecycleError("bundle_activation_missing")
    if record["mode"] == "paused":
        raise LifecycleError("lifecycle_paused")
    if record["mode"] == "enrollment_only" and bundle["manifest"]["content_id"] not in record["canary_content_ids"]:
        raise LifecycleError("lifecycle_canary_scope_required")
    _fixture_proofs(connection, record["proofs"], require_canary=record["mode"] == "active", record=record)
    archive_root_for_bundle(connection, bundle)
    if now is not None:
        _time(now)
        if _formal(connection):
            raise LifecycleError("production_clock_override_forbidden")
    return record


def _artifact(connection: sqlite3.Connection, artifact_id: int) -> dict[str, Any]:
    row = connection.execute("SELECT * FROM evidence_artifacts WHERE id=?", (artifact_id,)).fetchone()
    if row is None:
        raise LifecycleError("bundle_artifact_missing")
    return dict(row)


def _artifact_identity(row: Mapping[str, Any]) -> dict[str, Any]:
    return {"artifact_id": row["id"], **{key: row[key] for key in (
        "artifact_type", "sha256", "byte_size", "captured_at", "created_at", "processor_version", "local_path",
    )}}


def _verified_json(row: Mapping[str, Any], *, private: bool = True) -> dict[str, Any]:
    path = _path(row["local_path"])
    evidence = _file(path, private=private)
    if evidence["sha256"] != row["sha256"] or evidence["byte_size"] != row["byte_size"]:
        raise LifecycleError("lifecycle_artifact_bytes_changed")
    body = path.read_bytes()
    if hashlib.sha256(body).hexdigest() != row["sha256"]:
        raise LifecycleError("lifecycle_artifact_bytes_changed")
    return _object(body.decode("utf-8"))


def _member_path(value: Any) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise LifecycleError("invalid_member_path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in value.split("/")):
        raise LifecycleError("invalid_member_path")
    return str(path)


def _validate_manifest(manifest: Mapping[str, Any]) -> None:
    bundle_id = _identifier(manifest.get("bundle_id"))
    if (manifest.get("contract_version") != MANIFEST_VERSION
            or manifest.get("policy_version") != POLICY_VERSION
            or manifest.get("download_instance_id") != bundle_id
            or manifest.get("archive_key") != f"objects/{bundle_id}"):
        raise LifecycleError("bundle_manifest_contract_mismatch")
    members = manifest.get("members")
    if not isinstance(members, list) or not members:
        raise LifecycleError("bundle_members_missing")
    paths: set[str] = set()
    for index, member in enumerate(members):
        if (not isinstance(member, dict) or member.get("index") != index
                or member.get("member_id") != f"m{index:04d}"
                or not _sha(member.get("sha256"))
                or type(member.get("byte_size")) is not int or member["byte_size"] <= 0
                or member.get("kind") not in {"video", "image", "image_candidate"}):
            raise LifecycleError("bundle_member_identity_invalid")
        relative = _member_path(member.get("relative_path"))
        if relative in paths:
            raise LifecycleError("bundle_member_path_shared")
        paths.add(relative)
    if (manifest.get("member_count") != len(members)
            or manifest.get("byte_size") != sum(row["byte_size"] for row in members)):
        raise LifecycleError("bundle_member_totals_invalid")


def load_bundle(connection: sqlite3.Connection, bundle_id: str) -> dict[str, Any]:
    """Resolve immutable identity without requiring hot original bytes."""
    _identifier(bundle_id)
    rows = connection.execute(
        "SELECT * FROM evidence_artifacts WHERE artifact_type='media_lifecycle_manifest' "
        "AND json_valid(metadata_json) AND json_extract(metadata_json,'$.media_lifecycle.bundle_id')=?",
        (bundle_id,),
    ).fetchall()
    if len(rows) != 1:
        raise LifecycleError("bundle_not_found" if not rows else "bundle_identity_duplicated")
    row = dict(rows[0])
    if row["status"] != "available":
        raise LifecycleError("bundle_manifest_unavailable")
    manifest = _verified_json(row)
    _validate_manifest(manifest)
    state = _object(row["metadata_json"]).get("media_lifecycle")
    if (not isinstance(state, dict) or state.get("bundle_id") != bundle_id
            or state.get("manifest_sha256") != row["sha256"]
            or state.get("source_artifact_id") != manifest["source"]["artifact_id"]
            or state.get("source_sha256") != manifest["source"]["sha256"]
            or manifest["content_id"] != row["content_id"]
            or manifest["registered_at"] != row["created_at"]
            or type(state.get("revision")) is not int or state["revision"] < 0):
        raise LifecycleError("bundle_control_identity_changed")
    evidence_root = _path(row["local_path"]).parent
    instance_root = evidence_root.parent
    if (evidence_root.name != "evidence" or instance_root.name != bundle_id
            or instance_root.parent.name != manifest["link_id"]
            or instance_root.parent.parent.name != "managed-v1"
            or Path(row["local_path"]).name != f"lifecycle-manifest-{row['sha256']}.json"):
        raise LifecycleError("bundle_working_path_changed")
    _directory(evidence_root)
    _validate_state(state, manifest)
    return {
        "bundle_id": bundle_id, "control_artifact_id": row["id"],
        "manifest": manifest, "state": state, "manifest_sha256": row["sha256"],
        "created_at": row["created_at"], "instance_root": instance_root,
        "originals_root": instance_root / "originals", "evidence_root": evidence_root,
    }


def current_bundle(connection: sqlite3.Connection, content_id: int) -> dict[str, Any] | None:
    source = connection.execute(
        "SELECT id,sha256 FROM evidence_artifacts WHERE content_id=? AND artifact_type='media_source' ORDER BY id DESC LIMIT 1",
        (content_id,),
    ).fetchone()
    if source is None:
        return None
    row = connection.execute(
        "SELECT metadata_json FROM evidence_artifacts WHERE content_id=? AND artifact_type='media_lifecycle_manifest' "
        "AND json_valid(metadata_json) AND json_extract(metadata_json,'$.media_lifecycle.source_artifact_id')=? "
        "AND json_extract(metadata_json,'$.media_lifecycle.source_sha256')=? ORDER BY id DESC LIMIT 1",
        (content_id, source["id"], source["sha256"]),
    ).fetchone()
    return load_bundle(connection, _object(row[0])["media_lifecycle"]["bundle_id"]) if row else None


def original_artifact(connection: sqlite3.Connection, bundle: Mapping[str, Any]) -> dict[str, Any]:
    frozen = bundle["manifest"]["original_artifact"]
    row = _artifact(connection, frozen["artifact_id"])
    if _artifact_identity(row) != frozen or row["content_id"] != bundle["manifest"]["content_id"]:
        raise LifecycleError("bundle_original_identity_changed")
    namespace = _object(row["metadata_json"]).get("media_lifecycle", {})
    if (namespace.get("bundle_id") != bundle["bundle_id"]
            or namespace.get("control_artifact_id") != bundle["control_artifact_id"]
            or namespace.get("manifest_sha256") != bundle["manifest_sha256"]):
        raise LifecycleError("bundle_original_namespace_changed")
    return row


def _validate_state(state: Mapping[str, Any], manifest: Mapping[str, Any]) -> None:
    if state.get("storage_state") not in {"hot", "archived", "expired"}:
        raise LifecycleError("invalid_storage_state")
    if state.get("operation_state") not in {"idle", "archiving", "restoring", "purging", "blocked"}:
        raise LifecycleError("invalid_operation_state")
    first, due = state.get("archive_verified_at"), state.get("delete_due_at")
    if first is None:
        if due is not None or state["storage_state"] != "hot":
            raise LifecycleError("archive_time_required")
    elif (due is None or _time(due) != _time(first) + timedelta(hours=RETENTION_HOURS)
          or state["storage_state"] == "hot"):
        raise LifecycleError("retention_deadline_invalid")
    if state["storage_state"] == "expired":
        if not isinstance(due, str):
            raise LifecycleError("archive_time_required")
        if state.get("deleted_at") is None or _time(state["deleted_at"]) < _time(due):
            raise LifecycleError("expiry_before_deadline")
    elif state.get("deleted_at") is not None:
        raise LifecycleError("deletion_completion_state_invalid")
    if not isinstance(state.get("protections"), (dict, list)):
        raise LifecycleError("invalid_lifecycle_protections")
    if state.get("archive_key") != manifest["archive_key"]:
        raise LifecycleError("archive_key_changed")


def update_state(connection: sqlite3.Connection, bundle: Mapping[str, Any],
                 changes: Mapping[str, Any], expected_revision: int,
                 claim: durable_runs.DurableClaim | None = None) -> dict[str, Any]:
    """CAS the control metadata; identity, first T and deadline never change."""
    _require_transaction(connection)
    if claim is not None:
        owner = durable_runs.assert_owner(connection, claim)
        if owner.get("identity", {}).get("bundle_id") != bundle["bundle_id"]:
            raise LifecycleError("lifecycle_owner_scope_mismatch")
    current = load_bundle(connection, bundle["bundle_id"])
    before = current["state"]
    if (current["control_artifact_id"] != bundle["control_artifact_id"]
            or current["manifest_sha256"] != bundle["manifest_sha256"]
            or before["revision"] != expected_revision):
        raise LifecycleError("lifecycle_revision_conflict")
    for key in ("bundle_id", "activation_id", "source_artifact_id", "source_sha256", "manifest_sha256", "archive_key", "revision"):
        if key in changes and changes[key] != before[key]:
            raise LifecycleError("lifecycle_identity_immutable")
    for key in ("archive_verified_at", "delete_due_at"):
        if before.get(key) is not None and key in changes and changes[key] != before[key]:
            raise LifecycleError("retention_time_immutable")
    after = {**before, **dict(changes), "revision": before["revision"] + 1}
    if before["storage_state"] == "expired" and after["storage_state"] != "expired":
        raise LifecycleError("expired_bundle_immutable")
    if (before["operation_state"] == "purging" and after["operation_state"] != "purging"
            and after["storage_state"] != "expired"):
        raise LifecycleError("purging_cannot_reopen")
    if before.get("archive_verified_at") is None and after.get("archive_verified_at") is not None:
        if not isinstance(after.get("archive_receipt"), Mapping) or not after["archive_receipt"]:
            raise LifecycleError("archive_receipt_required")
        if _formal(connection) and abs((_time(after["archive_verified_at"]) - _time(now_utc())).total_seconds()) > 5:
            raise LifecycleError("archive_time_not_current")
    _validate_state(after, current["manifest"])
    row = _artifact(connection, current["control_artifact_id"])
    metadata = _object(row["metadata_json"])
    metadata["media_lifecycle"] = after
    changed = connection.execute(
        "UPDATE evidence_artifacts SET metadata_json=? WHERE id=? AND metadata_json=? AND sha256=? AND created_at=?",
        (_json(metadata), row["id"], row["metadata_json"], current["manifest_sha256"], current["created_at"]),
    )
    if changed.rowcount != 1:
        raise LifecycleError("lifecycle_revision_conflict")
    return {**current, "state": after}


def _source_binding(connection: sqlite3.Connection, content_id: int,
                    source_artifact_id: int) -> tuple[dict[str, Any], dict[str, Any]]:
    source = _artifact(connection, source_artifact_id)
    content_row = connection.execute("SELECT * FROM content_items WHERE id=?", (content_id,)).fetchone()
    current = connection.execute(
        "SELECT id FROM evidence_artifacts WHERE content_id=? AND artifact_type='media_source' ORDER BY id DESC LIMIT 1",
        (content_id,),
    ).fetchone()
    if (content_row is None or current is None or current[0] != source_artifact_id
            or source["content_id"] != content_id or source["artifact_type"] != "media_source"
            or source["status"] != "available" or not _sha(source["sha256"])):
        raise LifecycleError("download_source_identity_changed")
    content = dict(content_row)
    if content["content_type"] not in {"video", "image"}:
        raise LifecycleError("download_media_kind_unresolved")
    metadata = _object(source["metadata_json"])
    if metadata.get("media_kind") != content["content_type"]:
        raise LifecycleError("download_source_kind_mismatch")
    _verified_json(source, private=False)
    raw_id = metadata.get("raw_response_id")
    raw = connection.execute("SELECT sha256 FROM provider_raw_responses WHERE id=?", (raw_id,)).fetchone() if raw_id is not None else None
    binding = {**_artifact_identity(source), "raw_response_id": raw_id,
               "raw_sha256": raw[0] if raw else None, "logical_sha256": metadata.get("source_sha256")}
    return content, binding


def _intent_view(payload: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    for key in ("media_root", "instance_root", "originals_root", "evidence_root"):
        result[key] = Path(result[key])
    return result


def _prepare_directories(intent: Mapping[str, Any]) -> None:
    root = Path(intent["media_root"])
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.absolute() != root.resolve(strict=True) or not root.is_dir():
        raise LifecycleError("lifecycle_path_alias")
    for path in (root / "managed-v1", root / "managed-v1" / intent["link_id"],
                 Path(intent["instance_root"]), Path(intent["originals_root"]), Path(intent["evidence_root"])):
        path.mkdir(mode=0o700, exist_ok=True)
        _directory(path)


def prepare_download(content_id: int, source_artifact_id: int, media_root: Path,
                     db_path: Path = DEFAULT_DB, preclaimed_slot_id: int | None = None,
                     *, reacquire_request_id: str | None = None,
                     download_source_sha256: str | None = None) -> dict[str, Any] | None:
    """Persist an instance before new bytes; never enroll cached/old files."""
    root = Path(media_root).absolute()
    with connect(db_path) as connection, transaction(connection):
        record = activation(connection)
        if record is None:
            return None
        if _formal(connection) and root != DEFAULT_MEDIA_ROOT:
            raise LifecycleError("production_media_root_fixed")
        content, source = _source_binding(connection, content_id, source_artifact_id)
        if download_source_sha256 is not None and not _sha(download_source_sha256):
            raise LifecycleError("download_slot_source_invalid")
        source_aliases = {source["sha256"]}
        if _sha(source["logical_sha256"]):
            source_aliases.add(source["logical_sha256"])
        if download_source_sha256 is not None:
            source_aliases.add(download_source_sha256)
        if re.fullmatch(r"[A-Za-z0-9]{6}", content["link_id"]) is None:
            raise LifecycleError("invalid_media_link_id")
        identity = {"content_id": content_id, "source_artifact_id": source_artifact_id,
                    "source_sha256": source["sha256"], "reacquire_request_id": reacquire_request_id}
        key = INTENT_VERSION + ":" + hashlib.sha256(_json(identity).encode()).hexdigest()
        existing = connection.execute(
            "SELECT id,details_json FROM scheduler_runs WHERE job_id=? AND scheduled_for=?" + durable_runs.root_run_predicate(connection),
            (INTENT_JOB_ID, key),
        ).fetchone()
        if existing:
            payload = _object(existing["details_json"])
            if (payload.get("contract_version") != INTENT_VERSION or payload.get("source") != source
                    or payload["media_root"] != str(root)):
                raise LifecycleError("download_intent_identity_changed")
            if payload.get("control_artifact_id"):
                bundle = load_bundle(connection, payload["bundle_id"])
                raise LifecycleError(
                    "expired_non_replayable" if bundle["state"]["storage_state"] == "expired"
                    else "original_archived" if bundle["state"]["archive_verified_at"] else "managed_download_already_registered"
                )
            if (payload.get("preclaimed_slot_id") is not None and preclaimed_slot_id is not None
                    and payload["preclaimed_slot_id"] != preclaimed_slot_id):
                raise LifecycleError("download_intent_slot_changed")
            if (download_source_sha256 is not None
                    and payload.get("download_input_sha256") not in {None, download_source_sha256}):
                raise LifecycleError("download_intent_slot_changed")
        else:
            if record["mode"] not in {"enrollment_only", "active"}:
                return None
            if record["mode"] == "enrollment_only" and content_id not in record["canary_content_ids"]:
                return None
            if reacquire_request_id is not None:
                # The explicit paid reacquire workflow needs its own approved
                # request and fresh slot contract; ordinary retries cannot act as it.
                raise LifecycleError("explicit_reacquire_contract_not_bound")
            slots = connection.execute(
                "SELECT * FROM media_processing_slots WHERE content_id=? AND processor_type='download'",
                (content_id,),
            ).fetchall()
            slots = [row for row in slots if row["source_sha256"] in source_aliases]
            slot_source = download_source_sha256 or source.get("logical_sha256") or source["sha256"]
            if preclaimed_slot_id is not None:
                selected = next((row for row in slots if row["id"] == preclaimed_slot_id), None)
                if selected is None or selected["status"] != "running":
                    raise LifecycleError("download_slot_not_claimed")
                slot_source = selected["source_sha256"]
            started_before = set(record["preexisting_started_slots"])
            if any(row["id"] in started_before or row["status"] == "succeeded" for row in slots):
                return None
            prior = connection.execute(
                "SELECT metadata_json FROM evidence_artifacts WHERE content_id=? AND artifact_type IN ('media','media_manifest')",
                (content_id,),
            ).fetchall()
            if any(_object(row[0]).get("source_sha256") == source["logical_sha256"]
                   for row in prior if source["logical_sha256"] is not None):
                return None
            bundle_id = uuid.uuid4().hex
            instance = root / "managed-v1" / content["link_id"] / bundle_id
            timestamp = now_utc()
            payload = {
                "contract_version": INTENT_VERSION, **identity, "source": source,
                "activation_id": record["activation_id"], "policy_version": POLICY_VERSION,
                "bundle_id": bundle_id, "download_instance_id": bundle_id,
                "link_id": content["link_id"], "platform": content["platform"],
                "platform_content_id": content["platform_content_id"], "account_id": content["account_id"],
                "account_uid": content["raw_account_uid"], "media_kind": content["content_type"],
                "media_root": str(root), "instance_root": str(instance),
                "originals_root": str(instance / "originals"), "evidence_root": str(instance / "evidence"),
                "prepared_at": timestamp, "preclaimed_slot_id": preclaimed_slot_id,
                "download_source_sha256": slot_source,
                "download_input_sha256": download_source_sha256,
                "artifact_high_watermark": connection.execute("SELECT COALESCE(MAX(id),0) FROM evidence_artifacts").fetchone()[0],
                "control_artifact_id": None,
            }
            cursor = connection.execute(
                "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,details_json) VALUES (?,?,'partial',?,?)",
                (INTENT_JOB_ID, key, timestamp, _json(payload)),
            )
            payload["intent_run_id"] = cursor.lastrowid
            connection.execute("UPDATE scheduler_runs SET details_json=? WHERE id=?", (_json(payload), cursor.lastrowid))
    # A directory failure leaves the same durable intent for safe retry.
    _prepare_directories(payload)
    return _intent_view(payload)


def _exclusive_json(path: Path, body: bytes) -> None:
    _directory(path.parent)
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    except FileExistsError:
        evidence = _file(path)
        if evidence["sha256"] != hashlib.sha256(body).hexdigest() or evidence["byte_size"] != len(body):
            raise LifecycleError("immutable_lifecycle_file_conflict")
        return
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(body)
        handle.flush()
        os.fsync(handle.fileno())
    directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _original_files(root: Path) -> set[Path]:
    _directory(root)
    files: set[Path] = set()
    for child in root.iterdir():
        mode = child.lstat().st_mode
        if stat.S_ISDIR(mode):
            files.update(_original_files(child))
        elif stat.S_ISREG(mode):
            _file(child, root=root)
            files.add(child)
        else:
            raise LifecycleError("unowned_original_path")
    return files


def _download_members(intent: Mapping[str, Any], original: Mapping[str, Any]) -> list[dict[str, Any]]:
    root = Path(intent["originals_root"])
    entries: list[dict[str, Any]] = []
    if intent["media_kind"] == "video":
        entries.append({"path": _path(original["local_path"]), "kind": "video"})
    else:
        if _path(original["local_path"]).parent != Path(intent["evidence_root"]):
            raise LifecycleError("image_manifest_must_remain_online")
        manifest = _verified_json(original)
        paths, frames, groups = manifest.get("image_paths"), manifest.get("frames"), manifest.get("groups")
        if (manifest.get("status") != "complete" or not isinstance(paths, list) or not paths
                or not isinstance(frames, list) or not isinstance(groups, list)
                or len(paths) != len(frames) or len(paths) != len(groups)
                or manifest.get("source_count") != len(paths)):
            raise LifecycleError("image_bundle_incomplete")
        for index, path in enumerate(paths):
            group, frame = groups[index], frames[index]
            if (not isinstance(group, dict) or not isinstance(frame, dict)
                    or type(group.get("group_index")) is not int or group["group_index"] != index
                    or frame.get("path") != path or group.get("image_path") != path
                    or frame.get("sha256") != group.get("selected_response_sha256")):
                raise LifecycleError("image_member_order_changed")
            entries.append({"path": _path(path), "kind": "image", "group_index": group["group_index"],
                            "sha256": frame["sha256"], "byte_size": group["selected_byte_size"]})
        additional = _object(original["metadata_json"]).get("media_lifecycle_download_members", [])
        if not isinstance(additional, list):
            raise LifecycleError("invalid_additional_image_members")
        for candidate in additional:
            if (not isinstance(candidate, dict) or candidate.get("kind") != "image_candidate"
                    or type(candidate.get("group_index")) is not int
                    or type(candidate.get("candidate_index")) is not int):
                raise LifecycleError("invalid_additional_image_members")
            entries.append({**candidate, "path": _path(candidate["path"])})
    members = []
    paths_seen: set[Path] = set()
    for index, entry in enumerate(entries):
        path = entry["path"]
        evidence = _file(path, root=root)
        if path in paths_seen:
            raise LifecycleError("bundle_member_path_shared")
        paths_seen.add(path)
        if any(key in entry and entry[key] != evidence[key] for key in ("sha256", "byte_size")):
            raise LifecycleError("download_member_hash_changed")
        members.append({
            "member_id": f"m{index:04d}", "index": index, "relative_path": path.relative_to(root).as_posix(),
            "sha256": evidence["sha256"], "byte_size": evidence["byte_size"], "kind": entry["kind"],
            **{key: entry[key] for key in ("group_index", "candidate_index") if key in entry},
        })
    if _original_files(root) != paths_seen:
        raise LifecycleError("unregistered_original_members")
    return members


def register_download(connection: sqlite3.Connection, intent: Mapping[str, Any],
                      artifact_id: int, slot_id: int) -> dict[str, Any]:
    """Seal ownership in the same transaction that succeeds the download slot."""
    _require_transaction(connection)
    row = connection.execute("SELECT * FROM scheduler_runs WHERE id=? AND job_id=?",
                             (intent["intent_run_id"], INTENT_JOB_ID)).fetchone()
    if row is None:
        raise LifecycleError("download_intent_missing")
    payload = _object(row["details_json"])
    supplied = dict(intent)
    persisted = _intent_view(payload)
    for key in ("control_artifact_id", "registered_at", "registered_artifact_id", "registered_slot_id"):
        supplied.pop(key, None)
        persisted.pop(key, None)
    if persisted != supplied:
        raise LifecycleError("download_intent_identity_changed")
    if payload.get("control_artifact_id"):
        bundle = load_bundle(connection, payload["bundle_id"])
        if bundle["manifest"]["original_artifact"]["artifact_id"] != artifact_id or bundle["manifest"]["download_slot"]["id"] != slot_id:
            raise LifecycleError("registered_download_identity_changed")
        original_artifact(connection, bundle)
        return bundle
    content, source = _source_binding(connection, payload["content_id"], payload["source_artifact_id"])
    if source != payload["source"] or any(content[key] != payload[target] for key, target in (
        ("link_id", "link_id"), ("platform", "platform"), ("platform_content_id", "platform_content_id"),
        ("account_id", "account_id"), ("raw_account_uid", "account_uid"), ("content_type", "media_kind"),
    )):
        raise LifecycleError("download_source_identity_changed")
    slot = connection.execute("SELECT * FROM media_processing_slots WHERE id=?", (slot_id,)).fetchone()
    original = _artifact(connection, artifact_id)
    expected_type = "media" if payload["media_kind"] == "video" else "media_manifest"
    if (slot is None or slot["content_id"] != payload["content_id"] or slot["processor_type"] != "download"
            or slot["status"] != "succeeded" or slot["output_artifact_id"] != artifact_id
            or slot["source_sha256"] != payload["download_source_sha256"]
            or slot["attempt_count"] < 1 or _time(slot["updated_at"]) < _time(payload["prepared_at"])
            or (payload["preclaimed_slot_id"] is not None and payload["preclaimed_slot_id"] != slot_id)
            or original["content_id"] != payload["content_id"] or original["artifact_type"] != expected_type
            or original["status"] != "available" or original["id"] <= payload["artifact_high_watermark"]
            or original["processor_version"] != slot["processor_version"]
            or _time(original["created_at"]) < _time(payload["prepared_at"])):
        raise LifecycleError("download_registration_binding_invalid")
    metadata = _object(original["metadata_json"])
    if (_sha(source["logical_sha256"])
            and metadata.get("source_sha256") != source["logical_sha256"]):
        raise LifecycleError("download_registration_binding_invalid")
    if "media_lifecycle" in metadata:
        raise LifecycleError("original_already_owned")
    original_file = _file(_path(original["local_path"]))
    if original_file["sha256"] != original["sha256"] or original_file["byte_size"] != original["byte_size"]:
        raise LifecycleError("download_original_hash_changed")
    members = _download_members(payload, original)
    timestamp = now_utc()
    manifest = {
        "contract_version": MANIFEST_VERSION, "policy_version": POLICY_VERSION,
        **{key: payload[key] for key in ("activation_id", "bundle_id", "download_instance_id", "content_id", "link_id", "platform", "platform_content_id", "account_id", "account_uid", "media_kind")},
        "source": source, "original_artifact": _artifact_identity(original),
        "download_slot": {"id": slot_id, "source_sha256": slot["source_sha256"], "processor_version": slot["processor_version"]},
        "registered_at": timestamp, "archive_key": f"objects/{payload['bundle_id']}",
        "members": members, "member_count": len(members), "byte_size": sum(item["byte_size"] for item in members),
    }
    _validate_manifest(manifest)
    body = (_json(manifest) + "\n").encode()
    digest = hashlib.sha256(body).hexdigest()
    path = Path(payload["evidence_root"]) / f"lifecycle-manifest-{digest}.json"
    _exclusive_json(path, body)
    state = {
        "bundle_id": payload["bundle_id"], "activation_id": payload["activation_id"],
        "source_artifact_id": source["artifact_id"], "source_sha256": source["sha256"],
        "manifest_sha256": digest, "archive_key": manifest["archive_key"], "revision": 0,
        "storage_state": "hot", "operation_state": "idle", "protections": {},
        "completion_receipt": None, "archive_verified_at": None, "delete_due_at": None, "deleted_at": None,
    }
    cursor = connection.execute(
        "INSERT INTO evidence_artifacts(content_id,artifact_type,local_path,status,byte_size,sha256,captured_at,processor_version,metadata_json,created_at) "
        "VALUES (?,'media_lifecycle_manifest',?,'available',?,?,?,?,?,?)",
        (payload["content_id"], _relative(path), len(body), digest, timestamp, MANIFEST_VERSION, _json({"media_lifecycle": state}), timestamp),
    )
    control_id = cursor.lastrowid
    metadata["media_lifecycle"] = {"bundle_id": payload["bundle_id"], "control_artifact_id": control_id,
                                   "manifest_sha256": digest, "policy_version": POLICY_VERSION}
    connection.execute("UPDATE evidence_artifacts SET metadata_json=? WHERE id=?", (_json(metadata), artifact_id))
    payload["control_artifact_id"] = control_id
    payload["registered_at"] = timestamp
    payload["registered_artifact_id"] = artifact_id
    payload["registered_slot_id"] = slot_id
    connection.execute("UPDATE scheduler_runs SET status='succeeded',completed_at=?,details_json=? WHERE id=?",
                       (timestamp, _json(payload), payload["intent_run_id"]))
    return load_bundle(connection, payload["bundle_id"])
