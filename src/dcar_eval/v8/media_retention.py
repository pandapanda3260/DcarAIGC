"""Local-only, manifest-owned media archive, leases, restore and expiry.

There is deliberately no cache walk, paid fallback, trash phase or clock override
for the formal database.  SQLite records intent before every original unlink;
filesystem operations use private, no-follow files and a fenced durable run.
"""

from __future__ import annotations

import contextvars
import copy
import fcntl
import hashlib
import json
import os
import re
import shutil
import sqlite3
import stat
import subprocess
import tempfile
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

from PIL import Image

from . import durable_runs, media
from .media_policy import AGED_DAYS, HOT_HOURS, MIN_FREE_BYTES, POLICY, RETENTION_HOURS
from .source_routing import parse_time
from .storage import DEFAULT_DB, connect, is_formal_database_path, now_utc, transaction

RETENTION_VERSION = str(POLICY["contract_version"])
LIFECYCLE_JOB_IDS = frozenset(
    {"media_archive", "media_retention", "media_restore"}
)
_LOCAL_LOCK = threading.Lock()
_LOCK_COUNTS: dict[str, tuple[int, bool]] = {}
_LEASES: contextvars.ContextVar[dict[str, dict[str, Any]]] = contextvars.ContextVar(
    "media_original_leases", default={}
)
_PURPOSES = frozenset({
    "completion", "processing", "process_content_media", "process_video_evidence",
    "process_image_evidence", "download_video_sources", "download_image_sources",
    "fingerprint", "evaluation", "evidence", "file_response", "reprocess", "restore",
    "archive", "retention", "snapshot", "canary", "manual",
    "media_download", "media_processing", "duplicate_fingerprint", "media_evaluation", "retained_evidence_evaluation",
})


def _error(reason: str) -> Exception:
    from .media_lifecycle import LifecycleError

    return LifecycleError(reason)


def _require(condition: Any, reason: str) -> None:
    if not condition:
        raise _error(reason)


def _now(db_path: Path, at: str | None = None) -> str:
    _require(at is None or not is_formal_database_path(db_path), "formal_clock_override_forbidden")
    return parse_time(at or now_utc()).isoformat(timespec="seconds").replace("+00:00", "Z")


def _later(value: str, **delta: int) -> str:
    return (parse_time(value) + timedelta(**delta)).isoformat(timespec="seconds").replace("+00:00", "Z")


def _step_now(db_path: Path, fixture_time: str) -> str:
    """Internal timestamps are not a public production clock override."""
    return _now(db_path, None if is_formal_database_path(db_path) else fixture_time)


def _check_clock(bundle: Mapping[str, Any], at: str) -> None:
    state = bundle["state"]
    for key in ("archive_verified_at", "last_operation_at"):
        if state.get(key):
            _require(parse_time(at) >= parse_time(state[key]), "lifecycle_clock_moved_backwards")


def _private_directory(path: Path, *, root: Path, create: bool = False) -> None:
    path, root = Path(path).absolute(), Path(root).absolute()
    _require(".." not in path.parts and path.is_relative_to(root), "lifecycle_path_outside_root")
    media._require_no_symlink_below_root(path, root=root, label="lifecycle directory")
    if create:
        descriptor = media._open_private_output_parent(path / ".sentinel", root=root, label="lifecycle directory")
        os.close(descriptor)
    for candidate in (root, *[root.joinpath(*path.relative_to(root).parts[:i])
                               for i in range(1, len(path.relative_to(root).parts) + 1)]):
        info = candidate.lstat()
        _require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid()
                 and stat.S_IMODE(info.st_mode) == 0o700, "lifecycle_directory_not_private")


def _runtime_root(db_path: Path) -> Path:
    _require(os.environ.get("DCAR_READ_ONLY", "0").strip() != "1", "read_only_lifecycle_operation")
    path = _canonical_db_path(db_path)
    root = path.parent / ".media-lifecycle-runtime" / hashlib.sha256(str(path).encode()).hexdigest()[:16]
    _private_directory(root, root=path.parent / ".media-lifecycle-runtime", create=True)
    return root


def _canonical_db_path(db_path: Path) -> Path:
    # APFS firmlink spellings of the formal file must share a lock namespace.
    path = DEFAULT_DB.resolve() if is_formal_database_path(db_path) else Path(db_path).resolve()
    _require(not path.exists() or path.stat().st_nlink == 1, "lifecycle_database_hardlink_unsafe")
    return path


@contextmanager
def _file_lock(path: Path, *, exclusive: bool) -> Iterator[None]:
    """Nonblocking flock plus thread bookkeeping (including BSD flock semantics)."""
    key = str(path)
    with _LOCAL_LOCK:
        readers, writer = _LOCK_COUNTS.get(key, (0, False))
        _require(not writer and not (exclusive and readers), "media_bundle_busy")
        _LOCK_COUNTS[key] = (readers + (not exclusive), exclusive)
    descriptor: int | None = None
    try:
        descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        info = os.fstat(descriptor)
        current = path.lstat()
        _require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_uid == os.getuid()
                 and stat.S_IMODE(info.st_mode) == 0o600
                 and (info.st_dev, info.st_ino) == (current.st_dev, current.st_ino), "media_lock_unsafe")
        try:
            fcntl.flock(descriptor, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise _error("media_bundle_busy") from error
        yield
    finally:
        if descriptor is not None:
            os.close(descriptor)
        with _LOCAL_LOCK:
            readers, writer = _LOCK_COUNTS.get(key, (0, False))
            remaining = (readers - (not exclusive), False if exclusive else writer)
            if remaining == (0, False):
                _LOCK_COUNTS.pop(key, None)
            else:
                _LOCK_COUNTS[key] = remaining


@contextmanager
def _brief_lock(path: Path) -> Iterator[None]:
    """Serialize tiny runtime ledger updates without rejecting concurrent readers."""
    from .media_lifecycle import LifecycleError

    deadline = time.monotonic() + 5
    while True:
        lock = _file_lock(path, exclusive=True)
        try:
            lock.__enter__()
        except LifecycleError as error:
            if error.error_code != "media_bundle_busy" or time.monotonic() >= deadline:
                raise
            time.sleep(0.005)
            continue
        try:
            yield
        finally:
            lock.__exit__(None, None, None)
        return


def _bundle_id(bundle: Mapping[str, Any]) -> str:
    value = str(bundle["manifest"]["bundle_id"])
    _safe_id(value)
    return value


def _safe_id(value: str) -> None:
    _require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{32}", value), "bundle_id_unsafe")


@contextmanager
def _local_processor_lock() -> Iterator[None]:
    from .pipeline import LOCAL_PROCESSING_LOCK

    _require(LOCAL_PROCESSING_LOCK.acquire(blocking=False), "local_processing_busy")
    try:
        yield
    finally:
        LOCAL_PROCESSING_LOCK.release()


def _member_path(root: Path, member: Mapping[str, Any]) -> Path:
    relative = Path(str(member["relative_path"]))
    _require(not relative.is_absolute() and relative.parts
             and not any(part in {"", ".", ".."} for part in relative.parts), "member_path_unsafe")
    target = Path(root) / relative
    media._require_no_symlink_below_root(target, root=root, label="lifecycle member")
    return target


def _read_member(root: Path, member: Mapping[str, Any]) -> Any:
    target = _member_path(root, member)
    _private_directory(target.parent, root=root)
    evidence = media._read_private_file_evidence(target, label="lifecycle member")
    info = target.lstat()
    _require(info.st_uid == os.getuid() and stat.S_IMODE(info.st_mode) == 0o600, "member_not_private")
    _require(evidence.sha256 == member["sha256"] and evidence.byte_size == member["byte_size"], "member_identity_changed")
    return evidence


def _members(bundle: Mapping[str, Any]) -> list[dict[str, Any]]:
    members = bundle["manifest"].get("members")
    _require(isinstance(members, list) and members, "bundle_members_missing")
    _require(len({str(item["member_id"]) for item in members}) == len(members)
             and len({item["relative_path"] for item in members}) == len(members), "bundle_members_ambiguous")
    return members


def _all_members(root: Path, bundle: Mapping[str, Any]) -> list[Any]:
    members = _members(bundle)
    evidence = [_read_member(root, item) for item in members]
    expected = {str(item["relative_path"]) for item in members}
    _require(_pack_paths(root) == expected, "archive_pack_member_count_mismatch")
    return evidence


def _pack_paths(root: Path) -> set[str]:
    found: set[str] = set()
    # Inventory ONLY this registered private bundle, never the cache or an
    # account directory. Extra/partial staging files cannot be adopted silently.
    def visit(directory: Path) -> None:
        _private_directory(directory, root=root)
        for child in directory.iterdir():
            info = child.lstat()
            if stat.S_ISDIR(info.st_mode):
                visit(child)
            elif stat.S_ISREG(info.st_mode) and info.st_nlink == 1:
                found.add(child.relative_to(root).as_posix())
            else:
                raise _error("archive_pack_unowned_path")
    visit(root)
    return found


def _read_json(path: Path) -> dict[str, Any]:
    evidence = media._read_private_file_evidence(path, label="lifecycle JSON", capture_body=True)
    value = json.loads(evidence.body or b"")
    _require(isinstance(value, dict), "lifecycle_json_not_object")
    return value


def _runtime_json(path: Path, value: Mapping[str, Any]) -> None:
    """Mutable, locked runtime bookkeeping; never an immutable evidence receipt."""
    root = path.parent
    _private_directory(root, root=root)
    old = path.lstat() if os.path.lexists(path) else None
    if old is not None:
        _require(stat.S_ISREG(old.st_mode) and old.st_nlink == 1 and old.st_uid == os.getuid()
                 and stat.S_IMODE(old.st_mode) == 0o600, "runtime_ledger_unsafe")
    temp = root / ("." + uuid.uuid4().hex + ".tmp")
    body = (json.dumps(dict(value), sort_keys=True, allow_nan=False) + "\n").encode()
    descriptor = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())
    if old is None:
        directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            media._rename_exclusive_at(directory, temp.name, path.name)
        finally:
            os.close(directory)
    else:
        current = path.lstat()
        _require((old.st_dev, old.st_ino) == (current.st_dev, current.st_ino), "runtime_ledger_replaced")
        os.replace(temp, path)
    media._fsync_directory(root)


def _volume(path: Path) -> tuple[str, Path]:
    target = Path(path).absolute()
    _require(".." not in target.parts, "capacity_path_unsafe")
    while not target.exists():
        _require(target != target.parent, "capacity_volume_missing")
        target = target.parent
    _require(not target.is_symlink(), "capacity_path_alias")
    return str(target.stat().st_dev), target


def _live_reservations(ledger: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, reservation in ledger.items():
        _require(isinstance(reservation, dict) and type(reservation.get("pid")) is int
                 and reservation["pid"] > 0 and isinstance(reservation.get("volumes"), dict)
                 and all(isinstance(key, str) and type(value) is int and value >= 0
                         for key, value in reservation["volumes"].items()), "capacity_ledger_invalid")
        try:
            os.kill(reservation["pid"], 0)
        except ProcessLookupError:
            continue  # Only demonstrably dead processes lose their reservation.
        except PermissionError:
            pass
        result[key] = reservation
    return result


@contextmanager
def media_space_reservation(*, db_path: Path, allocations: Sequence[tuple[Path, int]],
                            purpose: str) -> Iterator[None]:
    """Account for all in-flight byte maxima on each actual filesystem."""
    root = _runtime_root(db_path)
    amounts: dict[str, int] = {}
    paths: dict[str, Path] = {}
    for path, amount in allocations:
        _require(type(amount) is int and amount >= 0, "capacity_reservation_invalid")
        volume, existing = _volume(path)
        amounts[volume] = amounts.get(volume, 0) + amount
        paths[volume] = existing
    ledger_path = root / "capacity.json"
    token = uuid.uuid4().hex
    with _brief_lock(root / "capacity.lock"):
        ledger = _live_reservations(_read_json(ledger_path) if ledger_path.exists() else {})
        for volume, amount in amounts.items():
            reserved = sum(int(entry["volumes"].get(volume, 0)) for entry in ledger.values())
            _require(shutil.disk_usage(paths[volume]).free - reserved - amount >= MIN_FREE_BYTES,
                     "media_space_below_reserve")
        ledger[token] = {"pid": os.getpid(), "purpose": purpose, "volumes": amounts}
        _runtime_json(ledger_path, ledger)
    try:
        yield
    finally:
        with _brief_lock(root / "capacity.lock"):
            ledger = _read_json(ledger_path)
            ledger.pop(token, None)
            _runtime_json(ledger_path, ledger)


def _load(bundle_id: str, db_path: Path) -> dict[str, Any]:
    from .media_lifecycle import load_bundle

    with connect(db_path) as connection:
        return load_bundle(connection, bundle_id)


def _update(bundle: dict[str, Any], changes: Mapping[str, Any], *, db_path: Path,
            claim: durable_runs.DurableClaim | None = None) -> dict[str, Any]:
    from .media_lifecycle import load_bundle, update_state

    with connect(db_path) as connection, transaction(connection):
        update_state(connection, bundle, dict(changes), expected_revision=bundle["state"]["revision"], claim=claim)
        return load_bundle(connection, _bundle_id(bundle))


def _availability(bundle: Mapping[str, Any], *, at: str, replica: bool = False) -> dict[str, Any]:
    state = bundle["state"]
    base = {"bundle_id": _bundle_id(bundle), "state": state.get("storage_state", "hot"),
            "operation_state": state.get("operation_state"), "can_restore": False,
            "archive_verified_at": state.get("archive_verified_at"),
            "delete_due_at": state.get("delete_due_at"), "deleted_at": state.get("deleted_at")}
    if state.get("storage_state") == "expired":
        return {**base, "reason": "original_expired", "http_status": 410}
    if state.get("operation_state") == "purging":
        return {**base, "reason": "original_purge_in_progress", "http_status": 409}
    if state.get("delete_due_at") and parse_time(at) >= parse_time(state["delete_due_at"]):
        return {**base, "reason": "original_expiry_pending", "http_status": 409}
    if replica:
        return {**base, "reason": "replica_original_omitted", "http_status": 409}
    if state.get("operation_state") == "restoring":
        return {**base, "reason": "original_restoring", "http_status": 202}
    try:
        _check_clock(bundle, at)
        present = [os.path.lexists(_member_path(bundle["originals_root"], item)) for item in _members(bundle)]
        if all(present):
            _all_members(bundle["originals_root"], bundle)
            return {**base, "reason": "original_available", "http_status": 200}
        if any(present) and not state.get("archive_verified_at"):
            return {**base, "reason": "original_missing", "http_status": 404}
        if state.get("storage_state") == "archived" and state.get("archive_verified_at"):
            from .media_lifecycle import archive_root_for_bundle

            # A writer may promise restore only after checking its real local copy.
            with connect(Path(bundle["db_path"])) as connection:
                archive_root = archive_root_for_bundle(connection, bundle)
            _verify_archive_receipt(bundle, Path(bundle["db_path"]))
            _all_members(archive_root / str(state["archive_key"]), bundle)
            return {**base, "reason": "original_archived", "http_status": 409, "can_restore": True}
        return {**base, "reason": "original_missing", "http_status": 404}
    except (OSError, ValueError, RuntimeError):
        return {**base, "reason": "original_integrity_error", "http_status": 503}


def original_availability(connection: sqlite3.Connection, content_id: int, *, at: str | None = None,
                          replica: bool = False) -> dict[str, Any]:
    """Read-only projection. It neither restores nor mutates a slot or a fact."""
    from .media_lifecycle import current_bundle

    bundle = current_bundle(connection, content_id)
    if bundle is None:
        return {"bundle_id": None, "state": "legacy", "can_restore": False,
                "reason": "legacy_media", "http_status": 200}
    db_path = Path(connection.execute("PRAGMA database_list").fetchone()[2])
    bundle["db_path"] = db_path
    return _availability(bundle, at=_now(db_path, at),
                         replica=replica or os.environ.get("DCAR_READ_ONLY", "0").strip() == "1")


@contextmanager
def media_read_lease(content_id: int, *, db_path: Path = DEFAULT_DB, purpose: str,
                     require_original: bool = True, bundle_id: str | None = None) -> Iterator[dict[str, Any] | None]:
    """Hold shared ownership for an entire original consumer, including streaming.

    Same-thread nested processing inherits an already-granted lease, so passing
    the deadline mid-operation cannot interrupt that operation. Derivative-only
    leases do not grant permission to obtain new original bytes.
    """
    from .media_lifecycle import current_bundle, load_bundle

    _require(purpose in _PURPOSES, "media_lease_purpose_invalid")
    with connect(db_path) as connection:
        bundle = load_bundle(connection, bundle_id) if bundle_id is not None else current_bundle(connection, content_id)
        _require(bundle is None or bundle["manifest"]["content_id"] == content_id, "lease_content_mismatch")
    if bundle is None or not require_original:
        yield bundle
        return
    key = str(_canonical_db_path(db_path)) + ":" + _bundle_id(bundle)
    active = _LEASES.get().get(key)
    if active is not None and active["thread"] == threading.get_ident():
        processing = {"processing", "process_content_media", "process_video_evidence", "process_image_evidence",
                      "media_processing", "media_download", "media_evaluation", "duplicate_fingerprint",
                      "fingerprint", "evaluation", "reprocess"}
        _require(purpose == active["purpose"] or purpose in processing and active["purpose"] in processing,
                 "new_consumer_cannot_inherit_lease")
        yield active["bundle"]
        return
    root = _runtime_root(db_path)
    with _file_lock(root / (_bundle_id(bundle) + ".lock"), exclusive=False):
        bundle = _load(_bundle_id(bundle), db_path)
        bundle["db_path"] = db_path
        availability = _availability(bundle, at=_now(db_path))
        if availability["reason"] == "original_expired":
            raise _error("expired_non_replayable")
        _require(availability["http_status"] == 200, str(availability["reason"]))
        token = _LEASES.set({**_LEASES.get(), key: {"thread": threading.get_ident(), "bundle": bundle,
                                                 "purpose": purpose, "acquired_at": now_utc()}})
        try:
            yield bundle
        finally:
            _LEASES.reset(token)
            # No business DB write on GET. The last-reader event is local-only
            # runtime bookkeeping, serialized independently of shared readers.
            with _brief_lock(root / (_bundle_id(bundle) + ".activity.lock")):
                _runtime_json(root / (_bundle_id(bundle) + ".activity.json"), {"finished_at": now_utc()})


def _publish_json(bundle: Mapping[str, Any], operation: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    root = Path(bundle["evidence_root"])
    spool = media._private_json_spool(payload)
    try:
        path = root / "lifecycle" / operation / (spool.sha256 + ".json")
        if os.path.lexists(path):
            evidence = media._read_private_file_evidence(path, label="lifecycle receipt")
            _require(evidence.sha256 == spool.sha256 and evidence.byte_size == spool.byte_size, "receipt_collision")
        else:
            evidence = media._publish_spooled_response(
                spool, path, staging=path.with_name("." + uuid.uuid4().hex + ".tmp"),
                trusted_root=root, label="lifecycle receipt",
            )
        return {"path": media._relative(path), "sha256": evidence.sha256, "byte_size": evidence.byte_size}
    finally:
        spool.close()


def _register_receipt(connection: sqlite3.Connection, bundle: Mapping[str, Any],
                      receipt: dict[str, Any]) -> dict[str, Any]:
    artifact = media.register_artifact(
        connection, content_id=int(bundle["manifest"]["content_id"]), artifact_type="media_lifecycle_receipt",
        path=media._resolved(receipt["path"]), processor_version=RETENTION_VERSION,
        metadata={"media_lifecycle": {"bundle_id": _bundle_id(bundle),
                                      "control_artifact_id": bundle["control_artifact_id"]}},
    )
    return {**receipt, "artifact_id": artifact.id}


def _receipt_update(bundle: dict[str, Any], key: str, receipt: dict[str, Any], changes: Mapping[str, Any],
                    *, db_path: Path, claim: durable_runs.DurableClaim) -> dict[str, Any]:
    from .media_lifecycle import load_bundle, update_state

    with connect(db_path) as connection, transaction(connection):
        durable_runs.assert_owner(connection, claim)
        registered = _register_receipt(connection, bundle, receipt)
        update_state(connection, bundle, {**dict(changes), key: registered},
                     expected_revision=bundle["state"]["revision"], claim=claim)
        return load_bundle(connection, _bundle_id(bundle))


def _copy_member(source_root: Path, target_root: Path, member: Mapping[str, Any]) -> Any:
    source = _read_member(source_root, member)
    target = _member_path(target_root, member)
    if os.path.lexists(target):
        return _read_member(target_root, member)
    spool_handle = tempfile.TemporaryFile(mode="w+b")
    try:
        descriptor = os.open(source.path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            _require((os.fstat(descriptor).st_dev, os.fstat(descriptor).st_ino) == (source.device, source.inode),
                     "copy_source_replaced")
            with os.fdopen(descriptor, "rb", closefd=False) as stream:
                shutil.copyfileobj(stream, spool_handle, 1024 * 1024)
        finally:
            os.close(descriptor)
        spool_handle.flush()
        os.fsync(spool_handle.fileno())
        media._assert_private_file_evidence_current(source, label="copy source")
        spool_handle.seek(0)
        spool = media._SpooledResponse(spool_handle, source.byte_size, source.sha256, source.header)
        media._publish_spooled_response(spool, target, staging=target.with_name("." + uuid.uuid4().hex + ".tmp"),
                                       trusted_root=target_root, label="lifecycle copy")
        return _read_member(target_root, member)
    finally:
        spool_handle.close()


def _decode_member(root: Path, member: Mapping[str, Any]) -> None:
    evidence = _read_member(root, member)
    kind = str(member["kind"])
    if kind in {"image", "image_candidate"}:
        with Image.open(evidence.path) as image:
            image.load()
            _require(image.width > 0 and image.height > 0, "restore_image_decode_failed")
    elif kind == "video":
        probe = subprocess.run(["ffprobe", "-v", "error", "-show_streams", "-of", "json", str(evidence.path)],
                               capture_output=True, text=True, timeout=120, check=False)
        _require(probe.returncode == 0 and any(item.get("codec_type") == "video"
                 for item in json.loads(probe.stdout).get("streams", [])), "restore_video_probe_failed")
        decoded = subprocess.run(["ffmpeg", "-nostdin", "-v", "error", "-xerror", "-i", str(evidence.path),
                                  "-f", "null", "-"], capture_output=True, timeout=3600, check=False)
        _require(decoded.returncode == 0, "restore_video_decode_failed")
    else:
        raise _error("bundle_media_kind_unsupported")
    media._assert_private_file_evidence_current(evidence, label="decoded restore member")


def _before_unlink(_path: Path, _member: Mapping[str, Any]) -> None:
    """Fault-injection seam after durable intent, before the final identity check."""


def _after_unlink(_path: Path, _member: Mapping[str, Any]) -> None:
    """Fault-injection seam after fsynced unlink, before its SQLite settlement."""


def _unlink_member(root: Path, member: Mapping[str, Any]) -> None:
    evidence = _read_member(root, member)
    directory = os.open(evidence.path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        media._assert_bound_output_parent(directory, evidence.path.parent, root=root, label="original unlink")
        _before_unlink(evidence.path, member)
        media._assert_private_file_evidence_current(evidence, label="original unlink")
        media._assert_bound_output_parent(directory, evidence.path.parent, root=root, label="original unlink")
        os.unlink(evidence.path.name, dir_fd=directory)
        os.fsync(directory)
        _after_unlink(evidence.path, member)
    finally:
        os.close(directory)


def _assert_ownership(connection: sqlite3.Connection, bundle: Mapping[str, Any]) -> None:
    from .media_lifecycle import original_artifact

    original_artifact(connection, bundle)
    root = Path(bundle["originals_root"])
    _private_directory(root, root=Path(bundle["instance_root"]))
    allowed = {int(bundle["manifest"]["original_artifact"]["artifact_id"])}
    for member in _members(bundle):
        local_path = media._relative(_member_path(root, member))
        for row in connection.execute("SELECT id FROM evidence_artifacts WHERE local_path=?", (local_path,)):
            _require(int(row["id"]) in allowed, "original_shared_by_other_artifact")
    # Registered instances have private, nonshared directories. A second
    # lifecycle control claiming this instance is always a conflict.
    rows = connection.execute(
        "SELECT id,metadata_json FROM evidence_artifacts WHERE artifact_type='media_lifecycle_manifest' "
        "AND id<>? AND local_path LIKE ?", (bundle["control_artifact_id"], media._relative(bundle["instance_root"]) + "/%"),
    ).fetchall()
    _require(not rows, "original_shared_by_other_bundle")


def _protected(bundle: Mapping[str, Any]) -> bool:
    protections = bundle["state"].get("protections", {})
    if isinstance(protections, dict):
        return any(bool(value) and not (isinstance(value, dict) and value.get("resolved_at"))
                   for value in protections.values())
    return bool(protections)


def _completion(bundle: Mapping[str, Any], db_path: Path) -> dict[str, Any]:
    from .media_completion import verify_completion

    result = verify_completion(bundle, db_path=db_path)
    _require(result["ready"], "completion_evidence_unverified:" + ",".join(result["blockers"]))
    return result


def _verify_archive_receipt(bundle: Mapping[str, Any], db_path: Path) -> None:
    reference = bundle["state"].get("archive_receipt")
    _require(isinstance(reference, dict) and reference.get("artifact_id"), "archive_receipt_missing")
    path = media._resolved(str(reference["path"]))
    _require(path.is_relative_to(bundle["evidence_root"]), "archive_receipt_outside_bundle")
    evidence = media._read_private_file_evidence(path, label="archive receipt", capture_body=True)
    _require(evidence.sha256 == reference["sha256"] and evidence.byte_size == reference["byte_size"], "archive_receipt_changed")
    body = json.loads(evidence.body or b"")
    _require(body.get("contract") == RETENTION_VERSION and body.get("operation") == "archive_full_restore_verified"
             and body.get("bundle_id") == _bundle_id(bundle) and body.get("manifest_sha256") == bundle["manifest_sha256"]
             and body.get("archive_key") == bundle["state"]["archive_key"] and body.get("members") == _members(bundle)
             and body.get("full_decode") is True
             and body.get("completion_receipt") == bundle["state"]["completion_receipt"], "archive_receipt_binding_changed")
    with connect(db_path) as connection:
        row = connection.execute("SELECT * FROM evidence_artifacts WHERE id=?", (reference["artifact_id"],)).fetchone()
        _require(row is not None and row["content_id"] == bundle["manifest"]["content_id"]
                 and row["artifact_type"] == "media_lifecycle_receipt" and row["status"] == "available"
                 and row["sha256"] == reference["sha256"] and row["byte_size"] == reference["byte_size"]
                 and row["local_path"] == reference["path"], "archive_receipt_artifact_changed")
    _require(parse_time(body["verified_at"]) <= parse_time(bundle["state"]["archive_verified_at"]), "archive_receipt_time_invalid")


def _root_for(bundle: Mapping[str, Any], db_path: Path, override: Path | None = None) -> Path:
    from .media_lifecycle import archive_root_for_bundle

    with connect(db_path) as connection:
        return Path(archive_root_for_bundle(connection, bundle, override=override))


def _require_release(bundle: Mapping[str, Any], db_path: Path, at: str) -> dict[str, Any]:
    from .media_lifecycle import require_destructive_activation

    with connect(db_path) as connection:
        return require_destructive_activation(connection, bundle, now=None if is_formal_database_path(db_path) else at)


def _claim(operation: str, bundle: Mapping[str, Any], db_path: Path, at: str,
           request_id: str | None = None) -> durable_runs.DurableClaim | None:
    identity = {"contract": RETENTION_VERSION, "bundle_id": _bundle_id(bundle),
                "manifest_sha256": bundle["manifest_sha256"], "operation": operation}
    if request_id:
        identity["request_id"] = request_id
    job = "media_" + operation
    scan_id = durable_runs.scan_identity(job, identity)
    with connect(db_path) as connection:
        row = connection.execute("SELECT id,status,details_json FROM scheduler_runs WHERE job_id=? AND scheduled_for=?" + durable_runs.root_run_predicate(connection),
                                 (job, "scan:" + scan_id)).fetchone()
    if row is not None and row["status"] == "running":
        details = json.loads(row["details_json"])
        # Caller holds both global and bundle EX locks, proving no lifecycle
        # operation is still executing. Resume its durable checkpoint, not seed.
        durable_runs.recover_run(int(row["id"]), expected_attempt_id=int(details["owner"]["attempt_id"]),
                                db_path=db_path, reason="lifecycle_exclusive_lock_recovered", now=at)
    return durable_runs.claim_run(job, identity, db_path=db_path, now=at)


def _finish(claim: durable_runs.DurableClaim, *, db_path: Path, at: str, result: Mapping[str, Any],
            complete: bool) -> None:
    with connect(db_path) as connection, transaction(connection):
        durable_runs.checkpoint(connection, claim, {"complete": complete, "result": dict(result)}, now=at)
    durable_runs.finish_run(claim, status="succeeded" if complete else "partial", db_path=db_path,
                            now=at, summary=result, next_resume_at=None if complete else _later(at, minutes=5))


def _settle_members(bundle: dict[str, Any], *, roots: Sequence[tuple[str, Path]],
                     phase: str, claim: durable_runs.DurableClaim, db_path: Path, at: str,
                     member_ids: Sequence[str] | None = None) -> dict[str, Any]:
    for location, root in roots:
        for member in _members(bundle):
            if member_ids is not None and member["member_id"] not in member_ids:
                continue
            key = f"{phase}:{location}:{member['member_id']}"
            intents = copy.deepcopy(bundle["state"].get("delete_intents", {}))
            settled = copy.deepcopy(bundle["state"].get("deleted_members", {}))
            path = _member_path(root, member)
            if key in settled:
                _require(not os.path.lexists(path), "settled_original_reappeared")
                continue
            if not os.path.lexists(path):
                _require(key in intents, "original_missing_without_delete_intent")
            else:
                _read_member(root, member)
                if key not in intents:
                    intents[key] = {"member_id": member["member_id"], "location": location,
                                    "sha256": member["sha256"], "byte_size": member["byte_size"],
                                    "run_id": claim.scheduler_run_id, "attempt_id": claim.attempt_id, "at": at}
                    bundle = _update(bundle, {"delete_intents": intents}, db_path=db_path, claim=claim)
                with connect(db_path) as connection, transaction(connection):
                    durable_runs.assert_owner(connection, claim)
                    from .media_lifecycle import load_bundle

                    current = load_bundle(connection, _bundle_id(bundle))
                    _require(not _protected(current), "bundle_protected")
                    _assert_ownership(connection, current)
                    _unlink_member(root, member)
            settled[key] = {**intents[key], "settled_at": _step_now(db_path, at), "absent": True}
            bundle = _update(bundle, {"deleted_members": settled}, db_path=db_path, claim=claim)
    return bundle


def _mark_video_available(bundle: Mapping[str, Any], available: bool, *, db_path: Path,
                          claim: durable_runs.DurableClaim) -> None:
    original = bundle["manifest"]["original_artifact"]
    if original["artifact_type"] != "media":
        return
    with connect(db_path) as connection, transaction(connection):
        durable_runs.assert_owner(connection, claim)
        row = connection.execute("SELECT * FROM evidence_artifacts WHERE id=?", (original["artifact_id"],)).fetchone()
        _require(row is not None and row["sha256"] == original["sha256"], "original_artifact_identity_changed")
        connection.execute("UPDATE evidence_artifacts SET status=? WHERE id=?",
                           ("available" if available else "missing", original["artifact_id"]))


def _perform_archive(bundle: dict[str, Any], *, claim: durable_runs.DurableClaim,
                     db_path: Path, at: str | None, release_hot: bool, archive_root: Path | None) -> dict[str, Any]:
    timestamp = _now(db_path, at)
    _check_clock(bundle, timestamp)
    _require(not _protected(bundle), "bundle_protected")
    _completion(bundle, db_path)
    root = _root_for(bundle, db_path, archive_root)
    key = "objects/" + _bundle_id(bundle)
    target = root / key
    if not bundle["state"].get("archive_verified_at"):
        with connect(db_path) as connection:
            _assert_ownership(connection, bundle)
        _all_members(bundle["originals_root"], bundle)
        bundle = _update(bundle, {"operation_state": "archiving", "archive_key": key,
                                  "last_operation_at": timestamp}, db_path=db_path, claim=claim)
        staging = root / "objects" / ("." + _bundle_id(bundle) + ".staging")
        restore_test = root / "staging" / (_bundle_id(bundle) + "-verify")
        total = sum(item["byte_size"] for item in _members(bundle))
        with media_space_reservation(db_path=db_path, allocations=[(root, total * 2), (Path(tempfile.gettempdir()).resolve(), total)],
                                     purpose="archive_full_restore"):
            _private_directory(target.parent, root=root, create=True)
            copy_root = target if target.exists() else staging
            _private_directory(copy_root, root=root, create=True)
            _private_directory(restore_test, root=root, create=True)
            for member in _members(bundle):
                _copy_member(bundle["originals_root"], copy_root, member)
                _copy_member(copy_root, restore_test, member)
                _decode_member(restore_test, member)
            _all_members(copy_root, bundle)
            if copy_root == staging:
                directory = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                try:
                    media._assert_bound_output_parent(directory, target.parent, root=root, label="archive publish")
                    media._rename_exclusive_at(directory, staging.name, target.name)
                    os.fsync(directory)
                finally:
                    os.close(directory)
            for member in _members(bundle):
                _unlink_member(restore_test, member)
            # Only the exact known test directory is removed, and only if empty.
            try:
                restore_test.rmdir()
                media._fsync_directory(restore_test.parent)
            except OSError:
                pass
        verified = _now(db_path, at)
        receipt = _publish_json(bundle, "archive", {
            "contract": RETENTION_VERSION, "operation": "archive_full_restore_verified",
            "bundle_id": _bundle_id(bundle), "manifest_sha256": bundle["manifest_sha256"],
            "archive_key": key, "verified_at": verified, "run_id": claim.scheduler_run_id,
            "members": _members(bundle), "full_decode": True, "completion_receipt": bundle["state"]["completion_receipt"],
        })
        # Receipt is already durable. T is captured at the successful control
        # transaction, never at download, copy start or a prior failed attempt.
        from .media_lifecycle import load_bundle, update_state

        with connect(db_path) as connection, transaction(connection):
            durable_runs.assert_owner(connection, claim)
            registered = _register_receipt(connection, bundle, receipt)
            first = _now(db_path, at)
            update_state(connection, bundle, {"storage_state": "archived", "operation_state": "idle",
                         "archive_verified_at": first, "delete_due_at": _later(first, hours=RETENTION_HOURS),
                         "archive_receipt": registered, "last_operation_at": first},
                         expected_revision=bundle["state"]["revision"], claim=claim)
            bundle = load_bundle(connection, _bundle_id(bundle))
    _all_members(target, bundle)
    _verify_archive_receipt(bundle, db_path)
    if not release_hot:
        return {"status": "verified_copy_only", "bundle_id": _bundle_id(bundle), "released": False}
    _require(parse_time(_now(db_path, at)) < parse_time(bundle["state"]["delete_due_at"]), "retention_due")
    _require_release(bundle, db_path, _now(db_path, at))
    bundle = _settle_members(bundle, roots=[("hot", Path(bundle["originals_root"]))], phase="initial_release",
                             claim=claim, db_path=db_path, at=_now(db_path, at))
    _mark_video_available(bundle, False, db_path=db_path, claim=claim)
    receipt = _publish_json(bundle, "hot-release", {"contract": RETENTION_VERSION, "bundle_id": _bundle_id(bundle),
                            "operation": "initial_hot_release", "settled": bundle["state"].get("deleted_members", {}),
                            "completed_at": _now(db_path, at)})
    bundle = _receipt_update(bundle, "hot_release_receipt", receipt, {"operation_state": "idle", "hot_present": False},
                             db_path=db_path, claim=claim)
    return {"status": "archived", "bundle_id": _bundle_id(bundle), "released": True,
            "archive_verified_at": bundle["state"]["archive_verified_at"], "delete_due_at": bundle["state"]["delete_due_at"]}


def _perform_restore(bundle: dict[str, Any], *, claim: durable_runs.DurableClaim, db_path: Path,
                     at: str | None, archive_root: Path | None) -> dict[str, Any]:
    timestamp = _now(db_path, at)
    _check_clock(bundle, timestamp)
    state = bundle["state"]
    _require(state.get("storage_state") == "archived", "original_not_archived")
    _require(state.get("operation_state") != "purging" and state.get("delete_due_at")
             and parse_time(timestamp) < parse_time(state["delete_due_at"]), "retention_due")
    root = _root_for(bundle, db_path, archive_root) / str(state["archive_key"])
    _verify_archive_receipt(bundle, db_path)
    _all_members(root, bundle)
    _completion(bundle, db_path)
    with connect(db_path) as connection:
        _assert_ownership(connection, bundle)
    bundle = _update(bundle, {"operation_state": "restoring", "last_operation_at": timestamp}, db_path=db_path, claim=claim)
    total = sum(item["byte_size"] for item in _members(bundle))
    with media_space_reservation(db_path=db_path, allocations=[(bundle["originals_root"], total),
                                 (Path(tempfile.gettempdir()).resolve(), total)], purpose="restore"):
        for member in _members(bundle):
            _copy_member(root, bundle["originals_root"], member)
            _decode_member(bundle["originals_root"], member)
    finished = _now(db_path, at)
    receipt = _publish_json(bundle, "restore", {"contract": RETENTION_VERSION, "operation": "restore",
                            "bundle_id": _bundle_id(bundle), "run_id": claim.scheduler_run_id,
                            "manifest_sha256": bundle["manifest_sha256"], "members": _members(bundle),
                            "finished_at": finished, "archive_verified_at": state["archive_verified_at"],
                            "delete_due_at": state["delete_due_at"], "full_decode": True})
    request = copy.deepcopy(bundle["state"].get("restore_request"))
    if request:
        request.update({"status": "succeeded", "completed_at": finished})
    bundle = _receipt_update(bundle, "restore_receipt", receipt, {
        "operation_state": "idle", "hot_present": True, "last_restore_at": finished,
        "hot_release_due_at": min(_later(finished, hours=HOT_HOURS), str(state["delete_due_at"])),
        "last_operation_at": finished, "restore_request": request,
    }, db_path=db_path, claim=claim)
    _mark_video_available(bundle, True, db_path=db_path, claim=claim)
    return {"status": "restored", "bundle_id": _bundle_id(bundle), "delete_due_at": state["delete_due_at"]}


def _hot_due(bundle: Mapping[str, Any], db_path: Path) -> str | None:
    state = bundle["state"]
    if not state.get("hot_present") or not state.get("last_restore_at"):
        return None
    last = str(state["last_restore_at"])
    activity = _runtime_root(db_path) / (_bundle_id(bundle) + ".activity.json")
    if activity.exists():
        recorded = str(_read_json(activity)["finished_at"])
        if parse_time(recorded) > parse_time(last):
            last = recorded
    return min(_later(last, hours=HOT_HOURS), str(state["delete_due_at"]))


def _perform_hot_release(bundle: dict[str, Any], *, claim: durable_runs.DurableClaim, db_path: Path,
                         at: str | None, archive_root: Path | None) -> dict[str, Any]:
    timestamp = _now(db_path, at)
    _check_clock(bundle, timestamp)
    _require(not _protected(bundle), "bundle_protected")
    _require(bundle["state"].get("operation_state") != "purging", "original_purge_in_progress")
    due = _hot_due(bundle, db_path)
    _require(due is not None and parse_time(timestamp) >= parse_time(due), "restored_hot_not_due")
    _require(parse_time(timestamp) < parse_time(bundle["state"]["delete_due_at"]), "retention_due")
    _require_release(bundle, db_path, timestamp)
    _completion(bundle, db_path)
    _verify_archive_receipt(bundle, db_path)
    root = _root_for(bundle, db_path, archive_root) / str(bundle["state"]["archive_key"])
    _all_members(root, bundle)
    phase = "restored_release:" + str(bundle["state"]["restore_receipt"]["sha256"])
    bundle = _settle_members(bundle, roots=[("hot", Path(bundle["originals_root"]))], phase=phase,
                             claim=claim, db_path=db_path, at=timestamp)
    _mark_video_available(bundle, False, db_path=db_path, claim=claim)
    receipt = _publish_json(bundle, "hot-release", {"contract": RETENTION_VERSION, "bundle_id": _bundle_id(bundle),
        "operation": "restored_hot_release", "restore_receipt": bundle["state"]["restore_receipt"],
        "settled": bundle["state"].get("deleted_members", {}), "completed_at": _now(db_path, at)})
    _receipt_update(bundle, "hot_release_receipt", receipt, {"hot_present": False, "operation_state": "idle",
                    "last_operation_at": _now(db_path, at)}, db_path=db_path, claim=claim)
    return {"status": "hot_released", "bundle_id": _bundle_id(bundle)}


def _perform_purge(bundle: dict[str, Any], *, claim: durable_runs.DurableClaim, db_path: Path,
                   at: str | None, archive_root: Path | None) -> dict[str, Any]:
    timestamp = _now(db_path, at)
    _check_clock(bundle, timestamp)
    state = bundle["state"]
    if state.get("storage_state") == "expired":
        return {"status": "expired", "bundle_id": _bundle_id(bundle), "deleted_at": state["deleted_at"]}
    _require(state.get("delete_due_at") and parse_time(timestamp) >= parse_time(state["delete_due_at"]), "retention_not_due")
    _require(not _protected(bundle), "bundle_protected")
    active = _require_release(bundle, db_path, timestamp)
    _require(active.get("mode") == "active", "permanent_delete_activation_required")
    _completion(bundle, db_path)
    root = _root_for(bundle, db_path, archive_root) / str(state["archive_key"])
    _verify_archive_receipt(bundle, db_path)
    with connect(db_path) as connection:
        _assert_ownership(connection, bundle)
    # On resume only remaining members must exist. Durable per-member intent is
    # the only explanation accepted for a missing not-yet-settled member.
    intents = state.get("delete_intents", {})
    known_paths = {member["relative_path"] for member in _members(bundle)}
    _require(_pack_paths(root) <= known_paths and _pack_paths(bundle["originals_root"]) <= known_paths,
             "archive_pack_unowned_path")
    for member in _members(bundle):
        key = f"purge:archive:{member['member_id']}"
        if os.path.lexists(_member_path(root, member)):
            _read_member(root, member)
        else:
            _require(state.get("operation_state") == "purging" and key in intents,
                     "original_missing_without_delete_intent")
    hot_members = state.get("purge_hot_members")
    if state.get("operation_state") != "purging":
        hot_members = []
        for member in _members(bundle):
            if os.path.lexists(_member_path(bundle["originals_root"], member)):
                _read_member(bundle["originals_root"], member)
                hot_members.append(member["member_id"])
            else:
                # Initial or later hot release must have durably explained it.
                _require(any(key.endswith(":hot:" + str(member["member_id"]))
                             for key in state.get("delete_intents", {})), "hot_missing_without_release_intent")
        bundle = _update(bundle, {"operation_state": "purging", "purge_hot_members": hot_members,
                                  "purge_started_at": timestamp, "last_operation_at": timestamp},
                         db_path=db_path, claim=claim)
    bundle = _settle_members(bundle, roots=[("archive", root)], phase="purge", claim=claim, db_path=db_path, at=timestamp)
    # Only previously frozen present hot members are deletion targets. Missing
    # cold members retain their earlier release settlement; do not invent one.
    for member in _members(bundle):
        if member["member_id"] not in (hot_members or []):
            _require(not os.path.lexists(_member_path(bundle["originals_root"], member)), "unexpected_hot_original")
    if hot_members:
        bundle = _settle_members(bundle, roots=[("hot", Path(bundle["originals_root"]))], phase="purge",
                                 claim=claim, db_path=db_path, at=timestamp, member_ids=hot_members)
    _mark_video_available(bundle, False, db_path=db_path, claim=claim)
    finished = _now(db_path, at)
    receipt = _publish_json(bundle, "deletion", {"contract": RETENTION_VERSION, "operation": "permanent_delete",
        "bundle_id": _bundle_id(bundle), "manifest_sha256": bundle["manifest_sha256"],
        "archive_verified_at": state["archive_verified_at"], "delete_due_at": state["delete_due_at"],
        "deleted_at": finished, "members": _members(bundle), "settled": bundle["state"].get("deleted_members", {})})
    bundle = _receipt_update(bundle, "deletion_receipt", receipt, {"storage_state": "expired", "operation_state": "idle",
        "deleted_at": finished, "last_operation_at": finished, "hot_present": False}, db_path=db_path, claim=claim)
    return {"status": "expired", "bundle_id": _bundle_id(bundle), "deleted_at": finished,
            "archive_bytes_deleted": sum(item["byte_size"] for item in _members(bundle))}


def _execute(operation: str, bundle_id: str, *, db_path: Path, at: str | None = None,
             archive_root: Path | None = None, request_id: str | None = None, release_hot: bool = True) -> dict[str, Any]:
    from .media_lifecycle import LifecycleError

    _safe_id(bundle_id)
    timestamp = _now(db_path, at)
    root = _runtime_root(db_path)
    claim: durable_runs.DurableClaim | None = None
    try:
        with _local_processor_lock(), _file_lock(root / "lifecycle.lock", exclusive=True), _file_lock(root / (bundle_id + ".lock"), exclusive=True):
            bundle = _load(bundle_id, db_path)
            _bundle_id(bundle)
            identity_suffix = request_id
            if operation == "archive" and not release_hot:
                identity_suffix = "enrollment-copy-only"
            claim = _claim(operation, bundle, db_path, timestamp, identity_suffix)
            if claim is None:
                return {"status": "not_claimed", "bundle_id": bundle_id}
            try:
                if operation == "archive":
                    result = _perform_archive(bundle, claim=claim, db_path=db_path, at=at,
                                              release_hot=release_hot, archive_root=archive_root)
                elif operation == "restore":
                    result = _perform_restore(bundle, claim=claim, db_path=db_path, at=at, archive_root=archive_root)
                elif operation == "retention":
                    result = _perform_purge(bundle, claim=claim, db_path=db_path, at=at, archive_root=archive_root)
                elif operation == "hot_release":
                    result = _perform_hot_release(bundle, claim=claim, db_path=db_path, at=at, archive_root=archive_root)
                else:
                    raise _error("lifecycle_operation_unknown")
                _finish(claim, db_path=db_path, at=_now(db_path, at), result=result, complete=True)
                return {**result, "run_id": claim.scheduler_run_id}
            except (LifecycleError, media.MediaProcessingError, OSError, ValueError, sqlite3.Error,
                    subprocess.SubprocessError) as error:
                # Never erase a purging state or its member debt on a failure.
                bundle = _load(bundle_id, db_path)
                reason = str(error)
                if operation == "restore" and reason == "retention_due":
                    request = copy.deepcopy(bundle["state"].get("restore_request"))
                    if request:
                        request.update({"status": "failed", "reason": reason, "completed_at": _now(db_path, at)})
                    _update(bundle, {"restore_request": request, "last_error": reason}, db_path=db_path, claim=claim)
                    result = {"status": "failed", "reason": reason, "bundle_id": bundle_id, "terminal": True}
                    with connect(db_path) as connection, transaction(connection):
                        durable_runs.checkpoint(connection, claim, {"complete": False, "result": result}, now=_now(db_path, at))
                    durable_runs.finish_run(claim, status="failed", db_path=db_path, now=_now(db_path, at), summary=result)
                    return {**result, "run_id": claim.scheduler_run_id}
                bundle = _update(bundle, {"last_error": reason, "last_error_at": _now(db_path, at)}, db_path=db_path, claim=claim)
                result = {"status": "partial", "reason": reason, "bundle_id": bundle_id}
                _finish(claim, db_path=db_path, at=_now(db_path, at), result=result, complete=False)
                return {**result, "run_id": claim.scheduler_run_id}
    except LifecycleError as error:
        if claim is not None:
            raise
        return {"status": "blocked", "reason": str(error), "bundle_id": bundle_id}


def archive_bundle(bundle_id: str, *, db_path: Path = DEFAULT_DB, at: str | None = None,
                   release_hot: bool = True, archive_root: Path | None = None) -> dict[str, Any]:
    from .media_completion import seal_completion

    _now(db_path, at)
    result = seal_completion(bundle_id, db_path=db_path, at=at)
    if not result["ready"]:
        from .media_lifecycle import LifecycleError

        try:
            root = _runtime_root(db_path)
            with _file_lock(root / (bundle_id + ".lock"), exclusive=False):
                bundle = _load(bundle_id, db_path)
                _update(bundle, {"completion_blockers": list(result["blockers"]),
                                  "last_completion_check_at": _now(db_path, at)}, db_path=db_path)
        except LifecycleError:
            pass  # An active owner or corrupt control must not be overwritten.
        return {"status": "blocked", "bundle_id": bundle_id, "reason": "completion_gate",
                "blockers": result["blockers"]}
    return _execute("archive", bundle_id, db_path=db_path, at=at, release_hot=release_hot, archive_root=archive_root)


def restore_bundle(bundle_id: str, *, db_path: Path = DEFAULT_DB, at: str | None = None,
                   request_id: str | None = None, archive_root: Path | None = None) -> dict[str, Any]:
    return _execute("restore", bundle_id, db_path=db_path, at=at, request_id=request_id, archive_root=archive_root)


def purge_bundle(bundle_id: str, *, db_path: Path = DEFAULT_DB, at: str | None = None,
                 archive_root: Path | None = None) -> dict[str, Any]:
    return _execute("retention", bundle_id, db_path=db_path, at=at, archive_root=archive_root)


def release_restored_hot(bundle_id: str, *, db_path: Path = DEFAULT_DB, at: str | None = None) -> dict[str, Any]:
    bundle = _load(bundle_id, db_path)
    reference = bundle["state"].get("restore_receipt")
    _require(isinstance(reference, dict), "restore_receipt_missing")
    return _execute("hot_release", bundle_id, db_path=db_path, at=at, request_id=str(reference["sha256"]))


def request_restore(content_id: int, bundle_id: str, purpose: str, *, db_path: Path = DEFAULT_DB) -> dict[str, Any]:
    """Enqueue only. No copy, network, member/enabled gate or paid retry slot."""
    from .media_lifecycle import load_bundle, update_state

    _safe_id(bundle_id)
    _require(purpose in _PURPOSES, "media_restore_purpose_invalid")
    timestamp = _now(db_path)
    root = _runtime_root(db_path)
    with _file_lock(root / (bundle_id + ".lock"), exclusive=False):
        with connect(db_path) as connection, transaction(connection):
            bundle = load_bundle(connection, bundle_id)
            _require(bundle["manifest"]["content_id"] == content_id, "restore_source_mismatch")
            bundle["db_path"] = db_path
            availability = _availability(bundle, at=timestamp)
            current = bundle["state"].get("restore_request")
            request: dict[str, Any]
            if current and current.get("status") == "pending" and availability["reason"] in {"original_archived", "original_restoring"}:
                request = dict(current)
            else:
                _require(availability["can_restore"], str(availability["reason"]))
                request = {"request_id": uuid.uuid4().hex, "purpose": purpose, "status": "pending", "requested_at": timestamp}
                update_state(connection, bundle, {"restore_request": request}, expected_revision=bundle["state"]["revision"])
        return _queue_restore_run(bundle_id, request, db_path=db_path, timestamp=timestamp)


def _queue_restore_run(bundle_id: str, request: dict[str, Any], *, db_path: Path, timestamp: str) -> dict[str, Any]:
    """Caller retains the bundle shared lock until the non-running run is durable."""
    # Durable run is created/released as interrupted, so it can be picked up by
    # the local worker. A queued request does not own an original read lease.
    bundle = _load(bundle_id, db_path)
    identity = {"contract": RETENTION_VERSION, "bundle_id": bundle_id, "manifest_sha256": bundle["manifest_sha256"],
                "operation": "restore", "request_id": request["request_id"]}
    scheduled_for = "scan:" + durable_runs.scan_identity("media_restore", identity)
    with connect(db_path) as connection:
        existing = connection.execute("SELECT id FROM scheduler_runs WHERE job_id='media_restore' AND scheduled_for=?" + durable_runs.root_run_predicate(connection),
                                      (scheduled_for,)).fetchone()
    if existing is None:
        claim = durable_runs.claim_run("media_restore", identity, db_path=db_path, now=timestamp,
                                      initial_checkpoint={"queued_only": True})
        if claim is not None:
            durable_runs.finish_run(claim, status="interrupted", db_path=db_path, now=timestamp,
                                    summary={"reason": "queued_for_local_worker"})
        with connect(db_path) as connection:
            existing = connection.execute("SELECT id FROM scheduler_runs WHERE job_id='media_restore' AND scheduled_for=?" + durable_runs.root_run_predicate(connection),
                                          (scheduled_for,)).fetchone()
    if existing is None:
        raise _error("restore_queue_claim_failed")
    bundle = _load(bundle_id, db_path)
    # A fast local worker may have already finished; never overwrite it pending.
    current = bundle["state"].get("restore_request")
    _require(current and current["request_id"] == request["request_id"], "restore_request_changed")
    request = {**current, "run_id": existing["id"]}
    _update(bundle, {"restore_request": request}, db_path=db_path)
    return {**request, "http_status": 202}


def _registered_ids(connection: sqlite3.Connection) -> list[str]:
    result: list[str] = []
    for row in connection.execute("SELECT metadata_json FROM evidence_artifacts WHERE artifact_type='media_lifecycle_manifest' ORDER BY id"):
        state = json.loads(row["metadata_json"]).get("media_lifecycle", {})
        if state.get("bundle_id"):
            result.append(str(state["bundle_id"]))
    return result


def age_incomplete_bundles(*, db_path: Path = DEFAULT_DB, at: str | None = None) -> dict[str, Any]:
    """All registered bundles, before completion filtering; never historical files."""
    from .media_lifecycle import load_bundle, update_state

    timestamp = _now(db_path, at)
    added: list[str] = []
    with connect(db_path) as connection, transaction(connection):
        for bundle_id in _registered_ids(connection):
            bundle = load_bundle(connection, bundle_id)
            if bundle["state"].get("completion_receipt") or bundle["state"].get("completion_gate_aged"):
                continue
            row = connection.execute("SELECT created_at FROM evidence_artifacts WHERE id=?", (bundle["control_artifact_id"],)).fetchone()
            _require(row is not None, "bundle_control_missing")
            if parse_time(timestamp) - parse_time(row["created_at"]) < timedelta(days=AGED_DAYS):
                continue
            protections = copy.deepcopy(bundle["state"].get("protections") or {})
            _require(isinstance(protections, dict), "bundle_protections_invalid")
            todo = {"reason": "completion_gate_aged", "first_listed_at": timestamp, "registered_at": row["created_at"],
                    "member_count": len(_members(bundle)), "registered_bytes": sum(item["byte_size"] for item in _members(bundle)),
                    "last_error": bundle["state"].get("last_error"), "resolution": None}
            protections["completion_gate_aged"] = {"created_at": timestamp, "reason": "completion_gate_aged"}
            update_state(connection, bundle, {"completion_gate_aged": todo, "protections": protections},
                         expected_revision=bundle["state"]["revision"])
            added.append(bundle_id)
    return {"added": added, "reason": "completion_gate_aged", "checked_at": timestamp}


def lifecycle_summary(connection: sqlite3.Connection, *, at: str | None = None) -> dict[str, Any]:
    from .media_lifecycle import load_bundle

    db_path = Path(connection.execute("PRAGMA database_list").fetchone()[2])
    timestamp = _now(db_path, at)
    counts: dict[str, int] = {"hot": 0, "archived": 0, "expired": 0}
    manual: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for bundle_id in _registered_ids(connection):
        bundle = load_bundle(connection, bundle_id)
        state = bundle["state"]
        storage = str(state.get("storage_state", "hot"))
        counts[storage] = counts.get(storage, 0) + 1
        if state.get("completion_gate_aged"):
            manual.append({"bundle_id": bundle_id, **state["completion_gate_aged"],
                           "protected": _protected(bundle), "evidence_ready": bool(state.get("completion_receipt")),
                           "age_seconds": max(0, int((parse_time(timestamp) - parse_time(bundle["created_at"])).total_seconds())),
                           "blockers": state.get("completion_blockers", []), "last_error": state.get("last_error")})
        if state.get("last_error"):
            failures.append({"bundle_id": bundle_id, "reason": state["last_error"], "operation": state.get("operation_state")})
    return {"contract": RETENTION_VERSION, "counts": counts, "manual_todos": manual,
            "failures": failures, "as_of": timestamp, "capacity_basis": "registered_manifest_bytes_not_df"}


def run_lifecycle_jobs(*, db_path: Path = DEFAULT_DB, at: str | None = None,
                       include_archive: bool = True, include_purge: bool = True, limit: int = 20) -> dict[str, Any]:
    """One bounded local pass: expiry debt, restores, then new archives.

    All job entrypoints share this priority. Busy bundles yield instead of
    blocking another consumer; each operation has its own durable fence.
    """
    from .media_lifecycle import activation, load_bundle

    timestamp = _now(db_path, at)
    with connect(db_path) as connection:
        active = activation(connection)
        if active is None or active.get("mode") != "active":
            return {"status": "skipped", "reason": "media_activation_required"}
    aged = age_incomplete_bundles(db_path=db_path, at=at) if include_archive else {"added": []}
    with connect(db_path) as connection:
        bundles = [load_bundle(connection, identity) for identity in _registered_ids(connection)]
    results: list[dict[str, Any]] = []
    due = sorted((bundle for bundle in bundles if bundle["state"].get("storage_state") != "expired"
                  and bundle["state"].get("delete_due_at")
                  and (bundle["state"].get("operation_state") == "purging"
                       or parse_time(timestamp) >= parse_time(bundle["state"]["delete_due_at"]))),
                 key=lambda item: item["state"]["delete_due_at"])
    due_ids = {_bundle_id(bundle) for bundle in due}
    for bundle in due:
        if len(results) >= limit:
            break
        request = copy.deepcopy(bundle["state"].get("restore_request"))
        if request and request.get("status") == "pending":
            # Acquire the operation locks before retiring a merely queued
            # restore. An in-flight pre-deadline restore is allowed to finish.
            results.append(restore_bundle(_bundle_id(bundle), db_path=db_path, at=at, request_id=request["request_id"]))
        if len(results) >= limit:
            break
        if include_purge or bundle["state"].get("operation_state") == "purging":
            results.append(purge_bundle(_bundle_id(bundle), db_path=db_path, at=at))
    for snapshot in bundles:
        if len(results) >= limit:
            break
        identity = _bundle_id(snapshot)
        if identity in due_ids or snapshot["state"].get("storage_state") == "expired":
            continue
        bundle = _load(identity, db_path)
        request = bundle["state"].get("restore_request")
        if request and request.get("status") == "pending":
            results.append(restore_bundle(identity, db_path=db_path, at=at, request_id=request["request_id"]))
    if include_archive:
        for snapshot in bundles:
            if len(results) >= limit:
                break
            identity = _bundle_id(snapshot)
            if identity in due_ids or snapshot["state"].get("storage_state") == "expired":
                continue
            bundle = _load(identity, db_path)
            if _protected(bundle):
                continue
            if active["mode"] == "enrollment_only" and bundle["manifest"]["content_id"] not in active["canary_content_ids"]:
                continue
            if not bundle["state"].get("archive_verified_at"):
                results.append(archive_bundle(identity, db_path=db_path, at=at,
                                               release_hot=active["mode"] == "active"))
            elif bundle["state"].get("hot_present"):
                hot_due = _hot_due(bundle, db_path)
                if hot_due and parse_time(timestamp) >= parse_time(hot_due):
                    results.append(release_restored_hot(identity, db_path=db_path, at=at))
            elif active["mode"] == "active" and not bundle["state"].get("hot_release_receipt"):
                results.append(archive_bundle(identity, db_path=db_path, at=at))
    return {"status": "partial" if any(item["status"] in {"blocked", "partial"} for item in results) else "succeeded",
            "results": results, "aged": aged, "checked_at": timestamp}


def install_lifecycle_jobs(scheduler: Any, *, db_path: Path = DEFAULT_DB) -> None:
    """Only an activated local writer receives lifecycle timers."""
    from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-untyped]
    from apscheduler.triggers.interval import IntervalTrigger  # type: ignore[import-untyped]

    from .media_lifecycle import activation

    if os.environ.get("DCAR_READ_ONLY", "0").strip() == "1":
        return
    with connect(db_path) as connection:
        active = activation(connection)
    if active is None or active.get("mode") != "active":
        return
    for name, trigger, archive, purge in (
        ("media_archive", CronTrigger(minute=POLICY["archive_minute"], timezone="Asia/Shanghai"), True, False),
        ("media_retention", CronTrigger(minute=POLICY["retention_minute"], timezone="Asia/Shanghai"), False, True),
        ("media_restore", IntervalTrigger(minutes=POLICY["restore_interval_minutes"], timezone="Asia/Shanghai"), False, False),
    ):
        startup = {"next_run_time": datetime.now(timezone.utc)} if purge else {}
        scheduler.add_job(run_lifecycle_jobs, trigger, id=name, replace_existing=True,
                          kwargs={"db_path": db_path, "include_archive": archive, "include_purge": purge},
                          coalesce=True, max_instances=1, misfire_grace_time=None, **startup)
