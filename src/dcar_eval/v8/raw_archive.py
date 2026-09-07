"""Schema-20 raw blobs, verified archives and conservative retention.

Response rows keep their own provenance; only physical entity bytes deduplicate.
Archive operations never create a missing mount point or fall back to the live
disk. Existing raw files are inventory-only and are never automatically removed.
Callers must hold the application writer lock for mutations. No network I/O is
performed here, and a storage failure must not be reported as billing unknown.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import secrets
import shutil
import sqlite3
import stat
import tarfile
from datetime import date, datetime, timedelta, timezone
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Any, Iterator
from zoneinfo import ZoneInfo

import zstandard

from . import raw_evidence
from .storage import PROJECT_ROOT, write_lock

ARCHIVE_ROOT = Path("/Volumes/DcarAIGC-RawArchive/provider-raw-v1")
CODEC_VERSION = "zstd-3-v1"
BEIJING = ZoneInfo("Asia/Shanghai")
MIN_ARCHIVE_FREE_BYTES = 150 * 1024**3

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS provider_raw_blobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_sha256 TEXT NOT NULL CHECK(length(entity_sha256)=64),
    entity_size INTEGER NOT NULL CHECK(entity_size>=0),
    codec TEXT NOT NULL CHECK(codec IN ('identity','zstd')),
    codec_version TEXT NOT NULL,
    stored_sha256 TEXT NOT NULL CHECK(length(stored_sha256)=64),
    stored_size INTEGER NOT NULL CHECK(stored_size>=0),
    hot_path TEXT NOT NULL,
    hot_owned INTEGER NOT NULL DEFAULT 0 CHECK(hot_owned IN (0,1)),
    hot_state TEXT NOT NULL DEFAULT 'present'
        CHECK(hot_state IN ('present','evicted','deleted')),
    raw_stored_at TEXT,
    recorded_at TEXT NOT NULL,
    UNIQUE(entity_sha256,codec_version)
);
CREATE TABLE IF NOT EXISTS raw_archives (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    business_day TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL UNIQUE CHECK(length(manifest_sha256)=64),
    manifest_json TEXT NOT NULL,
    local_path TEXT NOT NULL UNIQUE,
    storage_device INTEGER NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('building','verified','deleted')),
    archive_sha256 TEXT,
    byte_size INTEGER,
    recorded_at TEXT NOT NULL,
    verified_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_raw_blobs_hot_due
    ON provider_raw_blobs(hot_owned,hot_state,raw_stored_at,id);
CREATE TABLE IF NOT EXISTS raw_archive_members (
    archive_id INTEGER NOT NULL REFERENCES raw_archives(id) ON DELETE RESTRICT,
    raw_blob_id INTEGER NOT NULL REFERENCES provider_raw_blobs(id) ON DELETE RESTRICT,
    member_path TEXT NOT NULL,
    stored_sha256 TEXT NOT NULL,
    stored_size INTEGER NOT NULL,
    PRIMARY KEY(archive_id,raw_blob_id),
    UNIQUE(archive_id,member_path)
);
CREATE INDEX IF NOT EXISTS idx_raw_archive_members_blob
    ON raw_archive_members(raw_blob_id,archive_id);
CREATE TABLE IF NOT EXISTS raw_retention_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_blob_id INTEGER REFERENCES provider_raw_blobs(id) ON DELETE RESTRICT,
    raw_response_id INTEGER REFERENCES provider_raw_responses(id) ON DELETE RESTRICT,
    archive_id INTEGER REFERENCES raw_archives(id) ON DELETE RESTRICT,
    event_type TEXT NOT NULL,
    pin_key TEXT,
    reason TEXT NOT NULL,
    evidence_json TEXT NOT NULL DEFAULT '{}',
    recorded_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_raw_retention_events_blob
    ON raw_retention_events(raw_blob_id,id);
CREATE TRIGGER IF NOT EXISTS trg_provider_raw_blobs_identity_immutable
BEFORE UPDATE ON provider_raw_blobs
WHEN NEW.id IS NOT OLD.id OR NEW.entity_sha256 IS NOT OLD.entity_sha256
  OR NEW.entity_size IS NOT OLD.entity_size OR NEW.codec IS NOT OLD.codec
  OR NEW.codec_version IS NOT OLD.codec_version OR NEW.stored_sha256 IS NOT OLD.stored_sha256
  OR NEW.stored_size IS NOT OLD.stored_size OR NEW.hot_path IS NOT OLD.hot_path
  OR NEW.hot_owned IS NOT OLD.hot_owned OR NEW.raw_stored_at IS NOT OLD.raw_stored_at
  OR NEW.recorded_at IS NOT OLD.recorded_at
BEGIN SELECT RAISE(ABORT,'raw_blob_identity_immutable'); END;
CREATE TRIGGER IF NOT EXISTS trg_provider_raw_blobs_state_monotone
BEFORE UPDATE OF hot_state ON provider_raw_blobs
WHEN NEW.hot_state IS NOT OLD.hot_state AND NOT (
  (OLD.hot_state='present' AND NEW.hot_state='evicted'
  AND EXISTS(SELECT 1 FROM raw_archive_members m JOIN raw_archives a ON a.id=m.archive_id
             WHERE m.raw_blob_id=OLD.id AND a.state='verified')
  AND EXISTS(SELECT 1 FROM raw_retention_events e
             WHERE e.raw_blob_id=OLD.id AND e.event_type='hot_evict_intent'))
  OR (OLD.hot_state='present' AND NEW.hot_state='deleted'
      AND OLD.hot_owned=1 AND OLD.raw_stored_at IS NOT NULL
      AND EXISTS(SELECT 1 FROM raw_retention_events e WHERE e.raw_blob_id=OLD.id
        AND e.event_type='local_retention_retire_intent' AND e.archive_id IS NULL
        AND json_valid(e.evidence_json)
        AND json_extract(e.evidence_json,'$.schema')='raw-local-retirement-intent-v1'
        AND json_extract(e.evidence_json,'$.policy')='local-seven-complete-beijing-days-v1'
        AND json_extract(e.evidence_json,'$.raw_blob_id')=OLD.id
        AND json_extract(e.evidence_json,'$.protection_checked')=1
        AND json_type(e.evidence_json,'$.paths')='array'
        AND json_array_length(e.evidence_json,'$.paths')>0
        AND julianday(date(json_extract(e.evidence_json,'$.at'),'+8 hours'))
          -julianday(date(OLD.raw_stored_at,'+8 hours'))>=8
        AND e.id=(SELECT max(p.id) FROM raw_retention_events p WHERE p.raw_blob_id=OLD.id
          AND p.event_type IN ('local_retention_retire_intent','blob_rehydrated'))))
  OR (OLD.hot_state='evicted' AND NEW.hot_state='deleted'
      AND EXISTS(SELECT 1 FROM raw_retention_events e
                 WHERE e.raw_blob_id=OLD.id AND e.event_type='retention_retire_intent'))
  OR (OLD.hot_state='deleted' AND NEW.hot_state='present'
      AND (SELECT event_type FROM raw_retention_events e WHERE e.raw_blob_id=OLD.id
           AND e.event_type IN ('retention_retire_intent','local_retention_retire_intent','blob_rehydrated') ORDER BY e.id DESC LIMIT 1)='blob_rehydrated'))
BEGIN SELECT RAISE(ABORT,'raw_blob_state_not_monotone_or_unprotected'); END;
CREATE TRIGGER IF NOT EXISTS trg_provider_raw_blobs_no_delete
BEFORE DELETE ON provider_raw_blobs
BEGIN SELECT RAISE(ABORT,'raw_blob_ledger_no_delete'); END;
CREATE TRIGGER IF NOT EXISTS trg_raw_archives_identity_immutable
BEFORE UPDATE ON raw_archives
WHEN NEW.id IS NOT OLD.id OR NEW.business_day IS NOT OLD.business_day
  OR NEW.manifest_sha256 IS NOT OLD.manifest_sha256 OR NEW.manifest_json IS NOT OLD.manifest_json
  OR NEW.local_path IS NOT OLD.local_path OR NEW.storage_device IS NOT OLD.storage_device
  OR NEW.recorded_at IS NOT OLD.recorded_at
BEGIN SELECT RAISE(ABORT,'raw_archive_identity_immutable'); END;
CREATE TRIGGER IF NOT EXISTS trg_raw_archives_state_monotone
BEFORE UPDATE ON raw_archives
WHEN NOT (
  (NEW.state IS OLD.state AND NEW.archive_sha256 IS OLD.archive_sha256
     AND NEW.byte_size IS OLD.byte_size AND NEW.verified_at IS OLD.verified_at)
  OR (OLD.state='building' AND NEW.state='verified'
     AND OLD.archive_sha256 IS NULL AND OLD.byte_size IS NULL AND OLD.verified_at IS NULL
     AND NEW.archive_sha256 IS NOT NULL AND NEW.byte_size IS NOT NULL
     AND length(NEW.archive_sha256)=64 AND NEW.byte_size>=0 AND NEW.verified_at IS NOT NULL)
  OR (OLD.state='verified' AND NEW.state='deleted'
      AND NEW.archive_sha256 IS OLD.archive_sha256 AND NEW.byte_size IS OLD.byte_size
      AND NEW.verified_at IS OLD.verified_at
      AND EXISTS(SELECT 1 FROM raw_retention_events e
                 WHERE e.archive_id=OLD.id AND e.event_type='archive_retire_intent')))
BEGIN SELECT RAISE(ABORT,'raw_archive_state_not_monotone'); END;
CREATE TRIGGER IF NOT EXISTS trg_raw_archives_no_delete
BEFORE DELETE ON raw_archives
BEGIN SELECT RAISE(ABORT,'raw_archive_ledger_no_delete'); END;
CREATE TRIGGER IF NOT EXISTS trg_raw_archive_members_no_update
BEFORE UPDATE ON raw_archive_members
BEGIN SELECT RAISE(ABORT,'raw_archive_members_append_only'); END;
CREATE TRIGGER IF NOT EXISTS trg_raw_archive_members_no_delete
BEFORE DELETE ON raw_archive_members
BEGIN SELECT RAISE(ABORT,'raw_archive_members_append_only'); END;
CREATE TRIGGER IF NOT EXISTS trg_raw_retention_events_no_update
BEFORE UPDATE ON raw_retention_events BEGIN
    SELECT RAISE(ABORT,'raw_retention_events_append_only'); END;
CREATE TRIGGER IF NOT EXISTS trg_raw_retention_events_no_delete
BEFORE DELETE ON raw_retention_events BEGIN
    SELECT RAISE(ABORT,'raw_retention_events_append_only'); END;
"""


class RawArchiveError(raw_evidence.RawEvidenceError):
    """Storage or lifecycle gate failed; the provider circuit is unaffected."""


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@contextmanager
def _transaction(connection: sqlite3.Connection) -> Iterator[None]:
    """Commit a writer phase without entering the application's closing scope."""
    if connection.in_transaction:
        raise RawArchiveError("raw maintenance transaction must be standalone")
    # Standalone writer: hold the process write lock across BEGIN..COMMIT so
    # ``storage.transaction`` holders never wait on the SQLite file lock.
    with write_lock():
        connection.execute("BEGIN IMMEDIATE")
        try:
            yield
            connection.commit()
        except BaseException:
            connection.rollback()
            raise


def _rows(connection: sqlite3.Connection, sql: str, args: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    cursor = connection.execute(sql, args)
    columns = [item[0] for item in cursor.description or ()]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _row(connection: sqlite3.Connection, table: str, row_id: int) -> dict[str, Any]:
    if table not in {"provider_raw_blobs", "provider_raw_responses", "raw_archives"}:
        raise ValueError("unsupported raw table")
    found = _rows(connection, f"SELECT * FROM {table} WHERE id=?", (row_id,))
    if not found:
        raise RawArchiveError(f"{table} row is missing")
    return found[0]


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _event(connection: sqlite3.Connection, event_type: str, reason: str, *,
           blob_id: int | None = None, response_id: int | None = None,
           archive_id: int | None = None, pin_key: str | None = None,
           evidence: dict[str, Any] | None = None) -> None:
    connection.execute(
        """INSERT INTO raw_retention_events(raw_blob_id,raw_response_id,archive_id,
            event_type,pin_key,reason,evidence_json,recorded_at) VALUES(?,?,?,?,?,?,?,?)""",
        (blob_id, response_id, archive_id, event_type, pin_key, reason,
         json.dumps(evidence or {}, sort_keys=True), _now()),
    )


def _safe_existing(path: Path) -> Path:
    """Reject symlinks in every component, including a vanished mount alias."""
    path = path.absolute()
    if ".." in path.parts:
        raise RawArchiveError("raw path must not traverse parents")
    for component in (path, *path.parents):
        if component.is_symlink():
            raise RawArchiveError("raw path must not traverse a symlink")
    if not path.exists():
        raise RawArchiveError("raw path is missing")
    return path


def _read_bytes(path: Path, limit: int = raw_evidence.MAX_STORED_BYTES) -> bytes:
    _safe_existing(path)
    return raw_evidence._read_single_regular(path, max_bytes=limit, allow_legacy_read_mode=True)


def _source_path(value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _decode_blob(blob: dict[str, Any], stored: bytes) -> bytes:
    if len(stored) != blob["stored_size"] or _digest(stored) != blob["stored_sha256"]:
        raise RawArchiveError("raw blob stored checksum/size mismatch")
    if blob["entity_size"] > raw_evidence.MAX_RAW_BYTES:
        raise RawArchiveError("raw blob entity exceeds safety limit")
    try:
        entity = (zstandard.ZstdDecompressor().decompress(
            stored, max_output_size=max(1, int(blob["entity_size"])))
            if blob["codec"] == "zstd" else stored)
    except zstandard.ZstdError as error:
        raise RawArchiveError("raw blob cannot decompress") from error
    if len(entity) != blob["entity_size"] or _digest(entity) != blob["entity_sha256"]:
        raise RawArchiveError("raw blob entity checksum/size mismatch")
    return entity


def put_blob(connection: sqlite3.Connection, entity: bytes, *, live_root: Path,
             raw_stored_at: str) -> int:
    """Atomically persist a new zstd blob, sharing only identical entities."""
    day_for_timestamp(raw_stored_at)
    raw_stored_at = datetime.fromisoformat(raw_stored_at.replace("Z", "+00:00")).astimezone(
        timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    stored = raw_evidence.compress_entity_bytes(entity)
    entity_hash = _digest(entity)
    existing = _rows(connection,
        "SELECT * FROM provider_raw_blobs WHERE entity_sha256=? AND codec_version=?",
        (entity_hash, CODEC_VERSION))
    if existing:
        if existing[0]["hot_state"] == "deleted":
            blob = existing[0]
            if blob["stored_sha256"] != _digest(stored) or blob["stored_size"] != len(stored):
                raise RawArchiveError("retired blob codec bytes changed")
            expected = live_root / entity_hash[:2] / f"{entity_hash}.{CODEC_VERSION}.zst"
            if Path(blob["hot_path"]) != expected:
                raise RawArchiveError("retired blob may only rehydrate at its owned CAS path")
            raw_evidence.write_quarantine_evidence(expected, stored, evidence_root=live_root)
            _decode_blob(blob, _read_bytes(expected))
            _event(connection, "blob_rehydrated", "new_response_after_D31", blob_id=blob["id"],
                   evidence={"raw_stored_at": raw_stored_at})
            connection.execute("UPDATE provider_raw_blobs SET hot_state='present' WHERE id=?", (blob["id"],))
            return int(blob["id"])
        if read_blob(connection, int(existing[0]["id"])) != entity:
            raise RawArchiveError("deduplicated blob conflicts with entity")
        return int(existing[0]["id"])
    path = live_root / entity_hash[:2] / f"{entity_hash}.{CODEC_VERSION}.zst"
    receipt = raw_evidence.write_quarantine_evidence(path, stored, evidence_root=live_root)
    if _read_bytes(receipt.path) != stored:
        raise RawArchiveError("new raw blob readback failed")
    cursor = connection.execute(
        """INSERT INTO provider_raw_blobs(entity_sha256,entity_size,codec,codec_version,
            stored_sha256,stored_size,hot_path,hot_owned,raw_stored_at,recorded_at)
            VALUES(?,?,'zstd',?,?,?,?,1,?,?)""",
        (entity_hash, len(entity), CODEC_VERSION, receipt.sha256, receipt.byte_size,
         str(receipt.path), raw_stored_at, _now()),
    )
    assert cursor.lastrowid is not None
    return int(cursor.lastrowid)


def register_response_blob(connection: sqlite3.Connection, response_id: int, *,
                           live_root: Path, raw_stored_at: str) -> int:
    """Attach a verified response without changing its ID or transport lineage."""
    response = _row(connection, "provider_raw_responses", response_id)
    loaded = raw_evidence.read_raw_evidence(_safe_existing(_source_path(response["local_path"])),
        expected_stored_sha256=response["sha256"], expected_stored_size=response["byte_size"])
    if response["raw_blob_id"] is not None:
        blob_id = int(response["raw_blob_id"])
        if read_blob(connection, blob_id) != loaded.entity_bytes:
            raise RawArchiveError("response identity already points to different bytes")
        return blob_id
    blob_id = put_blob(connection, loaded.entity_bytes, live_root=live_root,
                       raw_stored_at=raw_stored_at)
    raw_stored_at = datetime.fromisoformat(raw_stored_at.replace("Z", "+00:00")).astimezone(
        timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    connection.execute("UPDATE provider_raw_responses SET raw_blob_id=?,raw_stored_at=? WHERE id=?",
                       (blob_id, raw_stored_at, response_id))
    return blob_id


def _legacy_source_path(value: str, *, legacy_project_root: Path) -> Path:
    path = Path(value)
    if ".." in path.parts:
        raise RawArchiveError("legacy raw path must not traverse parents")
    if not path.is_absolute():
        path = legacy_project_root / path
    # Existing absolute DB identities may point at legitimate diagnostic roots.
    # Read exactly that path under the original safety contract, never search.
    return _safe_existing(path)


def _legacy_copy_root(path: Path) -> Path:
    if not path.is_absolute() or ".." in path.parts:
        raise RawArchiveError("migration blob root must be explicit and absolute")
    _safe_existing(path.parent)
    try:
        path.mkdir(mode=0o700)
        raw_evidence._fsync_directory(path.parent)
    except FileExistsError:
        pass
    _safe_existing(path)
    metadata = path.stat()
    if (not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) != 0o700):
        raise RawArchiveError("migration blob root must be current-user private directory")
    return path


def _materialize_legacy_hardlink(loaded: raw_evidence.LoadedRawEvidence, *,
                               migration_blob_root: Path) -> Path:
    receipt = loaded.receipt
    target = migration_blob_root / f"{receipt.entity_sha256}.legacy-identity-v1.json"
    raw_evidence.write_quarantine_evidence(target, loaded.entity_bytes,
                                           evidence_root=migration_blob_root)
    verified = raw_evidence.read_raw_evidence(target,
        expected_stored_sha256=receipt.entity_sha256, expected_stored_size=receipt.entity_size)
    if verified.entity_bytes != loaded.entity_bytes:
        raise RawArchiveError("legacy hardlink copy readback failed")
    return target


def migrate_legacy(connection: sqlite3.Connection, *,
                   legacy_project_root: Path,
                   migration_blob_root: Path | None = None) -> dict[str, Any]:
    """Inventory all response IDs; quarantine unverifiable rows without merging.

    The enclosing schema migration creates the five new response columns first.
    Old raw_stored_at is deliberately left NULL; filesystem mtime is not evidence.
    No legacy file is moved, recompressed or deleted during migration.
    The caller must bind the original installation root in its migration receipt;
    an isolated checkout must never silently become the historical raw root.
    Historical hard links require an explicit private materialization root. Only
    verified single-link copies are linked into the runtime replay ledger.
    """
    if not legacy_project_root.is_absolute():
        raise RawArchiveError("legacy project root must be explicit and absolute")
    legacy_project_root = _safe_existing(legacy_project_root.resolve(strict=True))
    if not legacy_project_root.is_dir():
        raise RawArchiveError("legacy project root must be an existing directory")
    root_stat = legacy_project_root.stat()
    if migration_blob_root is not None:
        migration_blob_root = _legacy_copy_root(migration_blob_root)
    report: dict[str, Any] = {"responses": 0, "linked": 0, "already_linked": 0,
        "quarantined": 0, "quarantined_response_ids": [],
        "legacy_project_root": str(legacy_project_root),
        "legacy_project_root_device": root_stat.st_dev,
        "legacy_project_root_inode": root_stat.st_ino,
        "verified_absolute_source_parents": [],
        "migration_blob_root": str(migration_blob_root) if migration_blob_root else None,
        "copied_hardlink_count": 0, "copied_hardlink_bytes": 0,
        "copied_hardlink_blobs": []}
    absolute_source_parents: set[str] = set()
    materialized: dict[str, dict[str, Any]] = {}
    responses = _rows(connection, "SELECT * FROM provider_raw_responses ORDER BY id")
    for response in responses:
        report["responses"] += 1
        response_id = int(response["id"])
        if response["raw_blob_id"] is not None:
            report["already_linked"] += 1
            continue
        try:
            path = _legacy_source_path(response["local_path"],
                                       legacy_project_root=legacy_project_root)
            loaded = raw_evidence.read_legacy_migration_evidence(path,
                expected_stored_sha256=response["sha256"],
                expected_stored_size=response["byte_size"])
            receipt = loaded.receipt
            codec_version = f"legacy-{receipt.codec}-v1"
            codec, stored_sha256, stored_size = receipt.codec, receipt.stored_sha256, receipt.stored_size
            blob_path = path
            hardlink = path.stat().st_nlink > 1
            if hardlink:
                if migration_blob_root is None:
                    raise RawArchiveError("legacy_hardlink_requires_copy")
                blob_path = _materialize_legacy_hardlink(loaded,
                    migration_blob_root=migration_blob_root)
                codec_version = "legacy-identity-copy-v1"
                codec, stored_sha256, stored_size = "identity", receipt.entity_sha256, receipt.entity_size
            existing = _rows(connection,
                "SELECT * FROM provider_raw_blobs WHERE entity_sha256=? AND codec_version=?",
                (receipt.entity_sha256, codec_version))
            if existing:
                blob = existing[0]
                blob_id = int(blob["id"])
                if read_blob(connection, blob_id) != loaded.entity_bytes:
                    raise RawArchiveError("legacy shared blob differs from response")
            else:
                cursor = connection.execute(
                    """INSERT INTO provider_raw_blobs(entity_sha256,entity_size,codec,
                        codec_version,stored_sha256,stored_size,hot_path,hot_owned,recorded_at)
                        VALUES(?,?,?,?,?,?,?,0,?)""",
                    (receipt.entity_sha256, receipt.entity_size, codec, codec_version,
                     stored_sha256, stored_size, str(blob_path), _now()))
                assert cursor.lastrowid is not None
                blob_id = int(cursor.lastrowid)
            connection.execute(
                """UPDATE provider_raw_responses SET raw_blob_id=?,
                    paid_scope_identity=COALESCE(paid_scope_identity,?),
                    sequence=COALESCE(sequence,0) WHERE id=?""",
                (blob_id, f"legacy:raw:{response_id}", response_id))
            report["linked"] += 1
            if hardlink:
                materialized[str(blob_path)] = {"path": str(blob_path),
                    "sha256": receipt.entity_sha256, "byte_size": receipt.entity_size,
                    "raw_blob_id": blob_id}
            if Path(response["local_path"]).is_absolute():
                absolute_source_parents.add(str(path.parent))
        except (OSError, ValueError, raw_evidence.RawEvidenceError) as error:
            report["quarantined"] += 1
            report["quarantined_response_ids"].append(response_id)
            _event(connection, "migration_quarantine", str(error), response_id=response_id,
                   evidence={"local_path": response["local_path"], "sha256": response["sha256"],
                             "legacy_project_root": str(legacy_project_root)})
    report["status"] = "verified" if not report["quarantined"] else "quarantined"
    report["verified_absolute_source_parents"] = sorted(absolute_source_parents)
    report["copied_hardlink_blobs"] = [materialized[key] for key in sorted(materialized)]
    report["copied_hardlink_count"] = len(materialized)
    report["copied_hardlink_bytes"] = sum(item["byte_size"] for item in materialized.values())
    return report


def day_for_timestamp(timestamp: str) -> date:
    try:
        value = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError as error:
        raise RawArchiveError("raw timestamp is invalid") from error
    if value.tzinfo is None:
        raise RawArchiveError("raw timestamp must include timezone")
    return value.astimezone(BEIJING).date()


def lifecycle_stage(raw_stored_at: str | None, *, at: str) -> str:
    """The current Beijing day plus seven complete days remain hot."""
    if raw_stored_at is None:
        return "legacy_inventory"
    age = (day_for_timestamp(at) - day_for_timestamp(raw_stored_at)).days
    if age < 0:
        raise RawArchiveError("raw timestamp is in the future")
    return "hot" if age < 8 else "archive" if age < 31 else "retention"


def validate_archive_volume(*, archive_root: Path = ARCHIVE_ROOT, live_root: Path,
                            daily_stored_p95: int = 0) -> dict[str, Any]:
    """Validate a mounted independent device; never mkdir the mount point.

    Alternative roots are for explicit mounted-volume deployments and isolated
    fixtures, not a fallback. Merely having a directory is insufficient.
    """
    root = _safe_existing(archive_root)
    live = _safe_existing(live_root)
    if not root.is_dir() or not live.is_dir() or daily_stored_p95 < 0:
        raise RawArchiveError("archive volume configuration is invalid")
    device = root.stat().st_dev
    if device in {live.stat().st_dev, Path("/").stat().st_dev}:
        raise RawArchiveError("archive must be an independent persistent volume")
    mount = root
    while mount.parent != mount and mount.parent.stat().st_dev == device:
        mount = mount.parent
    if not os.path.ismount(mount):
        raise RawArchiveError("archive volume mount is not present")
    required = max(MIN_ARCHIVE_FREE_BYTES, (31 * daily_stored_p95 * 3 + 1) // 2)
    free = shutil.disk_usage(root).free
    if free < required:
        raise RawArchiveError("archive volume has insufficient reserved capacity")
    return {"schema": "raw-archive-volume-v1", "root": str(root),
            "mount": str(mount), "device": device, "free_bytes": free,
            "required_free_bytes": required, "verified_at": _now()}


def _archive_root_for(row: dict[str, Any]) -> Path:
    return Path(row["local_path"]).parent


def _verified_member(connection: sqlite3.Connection, blob: dict[str, Any], *,
                     live_root: Path | None = None) -> bytes:
    members = _rows(connection,
        """SELECT a.*,m.member_path FROM raw_archive_members m JOIN raw_archives a
           ON a.id=m.archive_id WHERE m.raw_blob_id=? AND a.state='verified' ORDER BY a.id DESC""",
        (blob["id"],))
    for archive in members:
        root = _archive_root_for(archive)
        _safe_existing(root)
        if root.stat().st_dev != archive["storage_device"]:
            raise RawArchiveError("archive device changed or is unmounted")
        if live_root is not None:
            validate_archive_volume(archive_root=root, live_root=live_root)
        path = _safe_existing(Path(archive["local_path"]))
        try:
            with tarfile.open(path, mode="r:") as reader:
                member = reader.getmember(archive["member_path"])
                if not member.isfile() or member.size != blob["stored_size"]:
                    raise RawArchiveError("archive member type/size mismatch")
                stream = reader.extractfile(member)
                if stream is None:
                    raise RawArchiveError("archive member is unreadable")
                stored = stream.read(int(blob["stored_size"]) + 1)
            _decode_blob(blob, stored)
            return stored
        except (tarfile.TarError, KeyError, OSError) as error:
            raise RawArchiveError("archive member readback failed") from error
    raise RawArchiveError("no verified archive protects raw blob")


def read_blob(connection: sqlite3.Connection, blob_id: int) -> bytes:
    blob = _row(connection, "provider_raw_blobs", blob_id)
    if blob["hot_state"] == "present":
        return _decode_blob(blob, _read_bytes(Path(blob["hot_path"])))
    if blob["hot_state"] == "evicted":
        return _decode_blob(blob, _verified_member(connection, blob))
    raise RawArchiveError("raw_expired: blob has been intentionally retired")


def read_response_entity(connection: sqlite3.Connection, response_id: int) -> bytes:
    response = _row(connection, "provider_raw_responses", response_id)
    if response.get("raw_blob_id") is not None:
        blob = _row(connection, "provider_raw_blobs", int(response["raw_blob_id"]))
        response_identity = (response["sha256"], response["byte_size"])
        if response_identity not in {(blob["stored_sha256"], blob["stored_size"]),
                                     (blob["entity_sha256"], blob["entity_size"])}:
            raise RawArchiveError("raw response identity differs from linked blob")
        return read_blob(connection, int(response["raw_blob_id"]))
    return raw_evidence.read_raw_evidence(_safe_existing(_source_path(response["local_path"])),
        expected_stored_sha256=response["sha256"],
        expected_stored_size=response["byte_size"]).entity_bytes


def record_transport_receipt(connection: sqlite3.Connection, *, attempt_id: int,
                             receipt: dict[str, Any], raw_response_id: int | None = None) -> int:
    """Register actual transport evidence once, never synthesize missing times.

    Complete raw is linked separately from quarantine. Encoded-byte attributes
    belong to this response's transport row, never the shared physical blob.
    """
    if not connection.in_transaction:
        raise RawArchiveError("transport receipt requires a writer transaction")
    connection.execute("SAVEPOINT raw_transport_receipt")
    try:
        result = _record_transport_receipt(connection, attempt_id=attempt_id,
                                          receipt=receipt, raw_response_id=raw_response_id)
        connection.execute("RELEASE SAVEPOINT raw_transport_receipt")
        return result
    except BaseException:
        connection.execute("ROLLBACK TO SAVEPOINT raw_transport_receipt")
        connection.execute("RELEASE SAVEPOINT raw_transport_receipt")
        raise


def _record_transport_receipt(connection: sqlite3.Connection, *, attempt_id: int,
                              receipt: dict[str, Any], raw_response_id: int | None) -> int:
    for key in ("request_started_at", "response_finished_at"):
        if not isinstance(receipt.get(key), str):
            raise RawArchiveError(f"transport receipt is missing {key}")
        day_for_timestamp(receipt[key])
    encoded_hash = receipt.get("http_encoded_sha256")
    encoded_size = receipt.get("http_encoded_bytes")
    if (not isinstance(encoded_hash, str) or len(encoded_hash) != 64
            or type(encoded_size) is not int or encoded_size < 0
            or not isinstance(receipt.get("transport_route_id"), str)):
        raise RawArchiveError("transport encoded-byte evidence is incomplete")
    payload = raw_evidence.canonical_json_bytes({"fetch_attempt_id": attempt_id, "transport": receipt})
    digest = _digest(payload)
    existing = connection.execute(
        "SELECT id,receipt_sha256 FROM fetch_transport_receipts WHERE fetch_attempt_id=?",
        (attempt_id,)).fetchone()
    if existing:
        if existing[1] != digest:
            raise RawArchiveError("transport attempt already has different evidence")
        transport_id = int(existing[0])
    else:
        cursor = connection.execute(
            """INSERT INTO fetch_transport_receipts(fetch_attempt_id,request_started_at,
                headers_received_at,response_finished_at,content_encoding,content_length,
                http_encoded_bytes,entity_bytes,stored_bytes,encoded_sha256,entity_sha256,
                stored_sha256,clean_eof,length_match,gzip_crc_ok,json_parse_ok,error_class,
                route_id,payload_json,receipt_sha256) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (attempt_id, receipt["request_started_at"], receipt.get("headers_received_at"),
             receipt["response_finished_at"], receipt.get("content_encoding"), receipt.get("content_length"),
             encoded_size, receipt.get("entity_bytes"), receipt.get("stored_bytes"), encoded_hash,
             receipt.get("entity_sha256"), receipt.get("stored_sha256"), int(receipt.get("clean_eof") is True),
             receipt.get("length_match"), receipt.get("gzip_crc_ok"), int(receipt.get("json_parse_ok") is True),
             receipt.get("error_code"), receipt["transport_route_id"], payload.decode(), digest))
        assert cursor.lastrowid is not None
        transport_id = int(cursor.lastrowid)
    if raw_response_id is not None:
        response = _row(connection, "provider_raw_responses", raw_response_id)
        if response["fetch_attempt_id"] != attempt_id:
            raise RawArchiveError("transport receipt belongs to another response attempt")
        if response["transport_receipt_id"] not in {None, transport_id}:
            raise RawArchiveError("raw response already binds another transport receipt")
        entity = read_response_entity(connection, raw_response_id)
        if receipt.get("entity_sha256") != _digest(entity) or receipt.get("entity_bytes") != len(entity):
            raise RawArchiveError("raw entity differs from transport evidence")
        connection.execute("UPDATE provider_raw_responses SET transport_receipt_id=? WHERE id=?",
                           (transport_id, raw_response_id))
    elif receipt.get("quarantine_path") is not None or receipt.get("zero_body") is True:
        path = receipt.get("quarantine_path")
        size = receipt.get("partial_bytes") if receipt.get("quarantine_kind") == "transport_partial" else receipt.get("quarantine_bytes", 0)
        checksum = receipt.get("partial_sha256") if receipt.get("quarantine_kind") == "transport_partial" else receipt.get("quarantine_sha256")
        if path is not None:
            content = _read_bytes(Path(path), raw_evidence.MAX_RAW_BYTES)
            if len(content) != size or _digest(content) != checksum:
                raise RawArchiveError("quarantine bytes differ from transport evidence")
        else:
            size, checksum = 0, _digest(b"")
        connection.execute(
            """INSERT OR IGNORE INTO transport_quarantine_members
                (transport_receipt_id,path,byte_size,sha256) VALUES(?,?,?,?)""",
            (transport_id, path, size, checksum))
    return transport_id


def _manifest(connection: sqlite3.Connection, day: date) -> dict[str, Any]:
    start = datetime.combine(day, datetime.min.time(), BEIJING).astimezone(timezone.utc)
    end = start + timedelta(days=1)
    blobs = _rows(connection,
        """SELECT b.* FROM provider_raw_blobs b WHERE hot_owned=1 AND hot_state='present'
           AND COALESCE((SELECT json_extract(e.evidence_json,'$.raw_stored_at') FROM raw_retention_events e
             WHERE e.raw_blob_id=b.id AND e.event_type='blob_rehydrated' ORDER BY e.id DESC LIMIT 1),raw_stored_at)>=?
           AND COALESCE((SELECT json_extract(e.evidence_json,'$.raw_stored_at') FROM raw_retention_events e
             WHERE e.raw_blob_id=b.id AND e.event_type='blob_rehydrated' ORDER BY e.id DESC LIMIT 1),raw_stored_at)<? ORDER BY id""",
        (start.strftime("%Y-%m-%dT%H:%M:%SZ"), end.strftime("%Y-%m-%dT%H:%M:%SZ")))
    return {"schema": "raw-archive-manifest-v1", "business_day": day.isoformat(),
            "members": [{"blob_id": b["id"], "member_path": f"blobs/{b['id']}.blob",
                         "entity_sha256": b["entity_sha256"], "entity_size": b["entity_size"],
                         "stored_sha256": b["stored_sha256"], "stored_size": b["stored_size"],
                         "codec": b["codec"], "codec_version": b["codec_version"]} for b in blobs]}


def _stored_blob(connection: sqlite3.Connection, blob: dict[str, Any]) -> bytes:
    if blob["hot_state"] == "present":
        stored = _read_bytes(Path(blob["hot_path"]))
        _decode_blob(blob, stored)
        return stored
    if blob["hot_state"] == "evicted":
        return _verified_member(connection, blob)
    raise RawArchiveError("raw_expired: blob has been intentionally retired")


def _hash_file(path: Path) -> tuple[str, int]:
    _safe_existing(path)
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    digest, size = hashlib.sha256(), 0
    try:
        info = os.fstat(descriptor)
        with os.fdopen(descriptor, "rb", closefd=False) as reader:
            while chunk := reader.read(1024 * 1024):
                digest.update(chunk)
                size += len(chunk)
        after = os.fstat(descriptor)
        if (info.st_ino, info.st_size, info.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns):
            raise RawArchiveError("archive changed during checksum")
    finally:
        os.close(descriptor)
    return digest.hexdigest(), size


def verify_archive(connection: sqlite3.Connection, archive_id: int) -> dict[str, Any]:
    """Full manifest/member readback, including entity decompression checks."""
    archive = _row(connection, "raw_archives", archive_id)
    if archive["state"] == "deleted":
        raise RawArchiveError("archive is already retired")
    path = _safe_existing(Path(archive["local_path"]))
    if path.parent.stat().st_dev != archive["storage_device"]:
        raise RawArchiveError("archive storage identity changed")
    checksum, size = _hash_file(path)
    if archive["archive_sha256"] is not None and (checksum != archive["archive_sha256"] or size != archive["byte_size"]):
        raise RawArchiveError("archive checksum differs from ledger")
    manifest_bytes = archive["manifest_json"].encode()
    if _digest(manifest_bytes) != archive["manifest_sha256"]:
        raise RawArchiveError("archive manifest differs from ledger")
    manifest = json.loads(manifest_bytes)
    ledger_members = _rows(connection,
        """SELECT raw_blob_id,member_path,stored_sha256,stored_size FROM raw_archive_members
           WHERE archive_id=? ORDER BY raw_blob_id""", (archive_id,))
    expected_members = [{"raw_blob_id": item["blob_id"], "member_path": item["member_path"],
                         "stored_sha256": item["stored_sha256"], "stored_size": item["stored_size"]}
                        for item in manifest["members"]]
    if ledger_members != expected_members:
        raise RawArchiveError("archive member ledger differs from manifest")
    expected = {"manifest.json", *(member["member_path"] for member in manifest["members"])}
    try:
        with tarfile.open(path, "r:") as reader:
            members = reader.getmembers()
            if len(members) != len(expected) or {m.name for m in members} != expected:
                raise RawArchiveError("archive has missing/extra/duplicate members")
            for member in members:
                if not member.isfile():
                    raise RawArchiveError("archive contains non-file member")
            manifest_member = reader.getmember("manifest.json")
            if manifest_member.size != len(manifest_bytes):
                raise RawArchiveError("archive manifest size mismatch")
            manifest_stream = reader.extractfile(manifest_member)
            if manifest_stream is None or manifest_stream.read(len(manifest_bytes) + 1) != manifest_bytes:
                raise RawArchiveError("archive manifest checksum mismatch")
            for item in manifest["members"]:
                member = reader.getmember(item["member_path"])
                if member.size != item["stored_size"]:
                    raise RawArchiveError("archive member size mismatch")
                stream = reader.extractfile(member)
                if stream is None:
                    raise RawArchiveError("archive member is missing")
                _decode_blob(item, stream.read(item["stored_size"] + 1))
    except (tarfile.TarError, OSError) as error:
        raise RawArchiveError("archive is incomplete or corrupt") from error
    return {"archive_id": archive_id, "archive_sha256": checksum,
            "byte_size": size, "members": len(manifest["members"]), "status": "verified"}


def archive_day(connection: sqlite3.Connection, business_day: str, *, at: str,
                live_root: Path, archive_root: Path = ARCHIVE_ROOT,
                daily_stored_p95: int = 0) -> dict[str, Any]:
    """Write a deterministic uncompressed tar; zstd members stay compressed.

    This is a standalone writer operation: intent is committed before filesystem
    work. A crash after publication is completed by verify/reconcile, never by
    repurchasing or overwriting data. Maintenance evicts verified hot copies D8.
    """
    if connection.in_transaction:
        raise RawArchiveError("archive requires a standalone writer transaction")
    day = date.fromisoformat(business_day)
    if (day_for_timestamp(at) - day).days < 8:
        raise RawArchiveError("raw day is still in the hot window")
    volume = validate_archive_volume(archive_root=archive_root, live_root=live_root,
                                     daily_stored_p95=daily_stored_p95)
    archive_root = Path(volume["root"])
    manifest = _manifest(connection, day)
    if not manifest["members"]:
        return {"status": "empty", "members": 0}
    return _write_archive_manifest(connection, manifest, volume=volume,
        live_root=live_root, daily_stored_p95=daily_stored_p95)


def _write_archive_manifest(connection: sqlite3.Connection, manifest: dict[str, Any], *,
                            volume: dict[str, Any], live_root: Path,
                            daily_stored_p95: int = 0, source_archive_id: int | None = None) -> dict[str, Any]:
    """Publish a daily/full or retained-subset manifest without replacing bytes."""
    business_day = manifest["business_day"]
    archive_root = Path(volume["root"])
    encoded = raw_evidence.canonical_json_bytes(manifest)
    digest = _digest(encoded)
    path = archive_root / f"{business_day}.{digest}.tar"
    with write_lock():
        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.execute(
                """INSERT OR IGNORE INTO raw_archives(business_day,manifest_sha256,manifest_json,
                   local_path,storage_device,state,recorded_at) VALUES(?,?,?,?,?,'building',?)""",
                (business_day, digest, encoded.decode(), str(path), volume["device"], _now()))
            archive = _rows(connection, "SELECT * FROM raw_archives WHERE manifest_sha256=?", (digest,))[0]
            archive_id = int(archive["id"])
            for member in manifest["members"]:
                connection.execute(
                    """INSERT OR IGNORE INTO raw_archive_members(archive_id,raw_blob_id,member_path,
                       stored_sha256,stored_size) VALUES(?,?,?,?,?)""",
                    (archive_id, member["blob_id"], member["member_path"],
                     member["stored_sha256"], member["stored_size"]))
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
    if not path.exists():
        temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(temporary, flags, 0o600)
        try:
            with ExitStack() as stack, os.fdopen(descriptor, "wb", closefd=False) as output:
                source = None
                if source_archive_id is not None:
                    source_row = _row(connection, "raw_archives", source_archive_id)
                    source = stack.enter_context(tarfile.open(_safe_existing(Path(source_row["local_path"])), mode="r:"))
                with tarfile.open(fileobj=output, mode="w:", format=tarfile.USTAR_FORMAT) as writer:
                    info = tarfile.TarInfo("manifest.json")
                    info.mode, info.size, info.mtime = 0o600, len(encoded), 0
                    writer.addfile(info, io.BytesIO(encoded))
                    for member in manifest["members"]:
                        blob = _row(connection, "provider_raw_blobs", member["blob_id"])
                        if source is None:
                            stored = _stored_blob(connection, blob)
                        else:
                            source_member = source.getmember(member["member_path"])
                            stream = source.extractfile(source_member)
                            if not source_member.isfile() or source_member.size != blob["stored_size"] or stream is None:
                                raise RawArchiveError("retention source member changed")
                            stored = stream.read(int(blob["stored_size"])+1)
                            _decode_blob(blob, stored)
                        info = tarfile.TarInfo(member["member_path"])
                        info.mode, info.size, info.mtime = 0o600, len(stored), 0
                        writer.addfile(info, io.BytesIO(stored))
                output.flush()
                os.fsync(descriptor)
        finally:
            os.close(descriptor)
        # Revalidate the mounted identity immediately before publication.
        if validate_archive_volume(archive_root=archive_root, live_root=live_root,
                daily_stored_p95=daily_stored_p95)["device"] != volume["device"]:
            raise RawArchiveError("archive volume changed during write")
        reservation = os.open(path, flags, 0o600)
        os.close(reservation)
        os.replace(temporary, path)
        raw_evidence._fsync_directory(path.parent)
    return reconcile_archive(connection, archive_id)


def reconcile_archive(connection: sqlite3.Connection, archive_id: int) -> dict[str, Any]:
    """Recover publication-before-ledger crashes using complete readback."""
    if connection.in_transaction:
        raise RawArchiveError("archive reconcile requires standalone transaction")
    receipt = verify_archive(connection, archive_id)
    with _transaction(connection):
        connection.execute(
            """UPDATE raw_archives SET state='verified',archive_sha256=?,byte_size=?,
               verified_at=? WHERE id=? AND state='building'""",
            (receipt["archive_sha256"], receipt["byte_size"], _now(), archive_id))
        _event(connection, "archive_verified", "full_manifest_readback", archive_id=archive_id,
               evidence=receipt)
    return receipt


def restore_blob(connection: sqlite3.Connection, blob_id: int, *, destination_root: Path) -> dict[str, Any]:
    """Restore a verified copy with no replacement or archive path extraction."""
    blob = _row(connection, "provider_raw_blobs", blob_id)
    stored = _verified_member(connection, blob)
    destination = destination_root / f"{blob_id}.{blob['stored_sha256']}.blob"
    restored = raw_evidence.write_quarantine_evidence(destination, stored, evidence_root=destination_root)
    _decode_blob(blob, _read_bytes(restored.path))
    receipt = {"blob_id": blob_id, "path": str(restored.path), "sha256": restored.sha256,
               "byte_size": restored.byte_size, "status": "verified_copy"}
    _event(connection, "restore_verified", "restore_copy_readback", blob_id=blob_id, evidence=receipt)
    return receipt


def due_archive_days(connection: sqlite3.Connection, *, at: str) -> list[str]:
    """Bound archive scheduling by days that still have unarchived owned blobs."""
    rows = _rows(connection,
        """SELECT b.id,b.raw_stored_at FROM provider_raw_blobs b
           WHERE b.hot_owned=1 AND b.hot_state='present' AND b.raw_stored_at IS NOT NULL
           AND NOT EXISTS(SELECT 1 FROM raw_archive_members m JOIN raw_archives a
               ON a.id=m.archive_id WHERE m.raw_blob_id=b.id AND a.state='verified')""")
    stamps = [_effective_hot_time(connection, row) for row in rows]
    return sorted({day_for_timestamp(str(stamp)).isoformat() for stamp in stamps
                   if lifecycle_stage(stamp, at=at) in {"archive", "retention"}})


def _effective_hot_time(connection: sqlite3.Connection, blob: dict[str, Any]) -> str | None:
    renewed = connection.execute("SELECT json_extract(evidence_json,'$.raw_stored_at') FROM raw_retention_events WHERE raw_blob_id=? AND event_type='blob_rehydrated' ORDER BY id DESC LIMIT 1", (blob["id"],)).fetchone()
    return str(renewed[0]) if renewed else blob["raw_stored_at"]


def pin_blob(connection: sqlite3.Connection, blob_id: int, *, pin_key: str, reason: str) -> None:
    if _row(connection, "provider_raw_blobs", blob_id)["hot_state"] == "deleted":
        raise RawArchiveError("cannot pin intentionally expired evidence")
    if not pin_key or not reason:
        raise ValueError("pin requires identity and reason")
    _event(connection, "pin", reason, blob_id=blob_id, pin_key=pin_key)


def release_pin(connection: sqlite3.Connection, blob_id: int, *, pin_key: str, reason: str) -> None:
    _row(connection, "provider_raw_blobs", blob_id)
    if not pin_key or not reason:
        raise ValueError("pin release requires identity and reason")
    _event(connection, "unpin", reason, blob_id=blob_id, pin_key=pin_key)


def retention_reasons(connection: sqlite3.Connection, blob_id: int, *, at: str) -> list[str]:
    blob = _row(connection, "provider_raw_blobs", blob_id)
    reasons = []
    if lifecycle_stage(blob["raw_stored_at"], at=at) != "retention":
        reasons.append("before_D31_or_legacy_inventory")
    if int(blob["entity_sha256"], 16) % 100 == 0:
        reasons.append("deterministic_one_percent_sample")
    if not blob["hot_owned"]:
        reasons.append("legacy_file_not_owned")
    pins = _rows(connection,
        """SELECT e.reason FROM raw_retention_events e JOIN
           (SELECT pin_key,max(id) id FROM raw_retention_events WHERE raw_blob_id=?
            AND event_type IN ('pin','unpin') GROUP BY pin_key) p ON p.id=e.id
           WHERE e.event_type='pin'""", (blob_id,))
    reasons.extend(f"pin:{pin['reason']}" for pin in pins)
    if connection.execute("SELECT 1 FROM provider_raw_responses WHERE raw_blob_id=? LIMIT 1", (blob_id,)).fetchone():
        reasons.append("response_reference")
    return reasons


def delete_unreferenced_hot(connection: sqlite3.Connection, blob_id: int, *, at: str,
                            live_root: Path) -> dict[str, Any]:
    """Evict only an owned D31 orphan protected by verified archive, never data.

    Referenced evidence is not destroyed to meet a capacity target. Consumers
    must explicitly release references/holds before this operation is eligible.
    The archive copy and response lineage ledger remain intact.
    """
    if connection.in_transaction:
        raise RawArchiveError("retention requires a standalone writer transaction")
    with write_lock():
        connection.execute("BEGIN IMMEDIATE")
        try:
            reasons = retention_reasons(connection, blob_id, at=at)
            if reasons:
                raise RawArchiveError("raw retention blocked: " + ",".join(reasons))
            blob = _row(connection, "provider_raw_blobs", blob_id)
            archives = _rows(connection,
                """SELECT a.id FROM raw_archives a JOIN raw_archive_members m ON m.archive_id=a.id
                   WHERE m.raw_blob_id=? AND a.state='verified' ORDER BY a.id DESC LIMIT 1""", (blob_id,))
            if not archives:
                raise RawArchiveError("no verified archive protects raw blob")
            verify_archive(connection, int(archives[0]["id"]))
            stored = _verified_member(connection, blob, live_root=live_root)
            path = Path(blob["hot_path"])
            if not path.absolute().is_relative_to(live_root.absolute()):
                raise RawArchiveError("hot deletion path lies outside live root")
            if path.exists() and _read_bytes(path) != stored:
                raise RawArchiveError("hot copy differs from archive")
            _event(connection, "hot_evict_intent", "verified_archive_orphan_D31", blob_id=blob_id)
            # Publish the archive read route before unlink. A crash leaves a safe
            # extra copy; replay never observes a missing hot path as successful.
            connection.execute("UPDATE provider_raw_blobs SET hot_state='evicted' WHERE id=?", (blob_id,))
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
    if path.exists():
        path.unlink()
        raw_evidence._fsync_directory(path.parent)
    with _transaction(connection):
        _event(connection, "hot_evicted", "verified_archive_orphan_D31", blob_id=blob_id)
    return {"blob_id": blob_id, "status": "hot_evicted", "archive_preserved": True}


def evict_archived_hot(connection: sqlite3.Connection, blob_id: int, *, at: str,
                        live_root: Path, verified_archive_id: int | None = None) -> dict[str, Any]:
    """D8 removes only an owned extra copy; pins and response IDs remain readable."""
    if connection.in_transaction:
        raise RawArchiveError("hot eviction requires a standalone writer transaction")
    blob = _row(connection, "provider_raw_blobs", blob_id)
    if not blob["hot_owned"] or lifecycle_stage(_effective_hot_time(connection, blob), at=at) not in {"archive", "retention"}:
        raise RawArchiveError("hot eviction requires owned inventory outside seven complete Beijing days")
    if blob["hot_state"] == "deleted":
        return {"blob_id": blob_id, "status": "raw_expired", "removed_bytes": 0}
    archives = _rows(connection, """SELECT a.id FROM raw_archives a JOIN raw_archive_members m ON m.archive_id=a.id
        WHERE m.raw_blob_id=? AND a.state='verified' ORDER BY a.id DESC""", (blob_id,))
    if not archives:
        raise RawArchiveError("no verified archive protects raw blob")
    archive_id = int(archives[0]["id"])
    if verified_archive_id != archive_id:
        verify_archive(connection, archive_id)
    stored = _verified_member(connection, blob, live_root=live_root)
    paths = {Path(blob["hot_path"]).absolute()}
    # Only schema20 responses with a known write receipt may own extra loose
    # files. Unknown-time legacy inventory is never swept by this path.
    for response in _rows(connection, "SELECT local_path FROM provider_raw_responses WHERE raw_blob_id=? AND raw_stored_at IS NOT NULL", (blob_id,)):
        path = _source_path(str(response["local_path"])).absolute()
        if path.is_relative_to(live_root.absolute()):
            paths.add(path)
    checks: list[tuple[Path, int, str]] = []
    for path in sorted(paths):
        if not path.is_relative_to(live_root.absolute()):
            raise RawArchiveError("hot deletion path lies outside live root")
        if path.exists() or path.is_symlink():
            if path == Path(blob["hot_path"]).absolute():
                body = _read_bytes(path)
                if body != stored:
                    raise RawArchiveError("hot copy differs from verified archive")
            else:
                loaded = raw_evidence.read_raw_evidence(_safe_existing(path))
                if loaded.entity_bytes != _decode_blob(blob, stored):
                    raise RawArchiveError("loose response differs from verified archive")
                body = _read_bytes(path)
            checks.append((path, len(body), _digest(body)))
    with _transaction(connection):
        _event(connection, "hot_evict_intent", "verified_archive_D8", blob_id=blob_id,
            archive_id=archive_id, evidence={"paths": [str(path) for path, _, _ in checks]})
        connection.execute("UPDATE provider_raw_blobs SET hot_state='evicted' WHERE id=?", (blob_id,))
    removed = 0
    for path, size, checksum in checks:
        # Recheck immediately before unlink; no broad cleanup/glob deletion.
        current = _read_bytes(path)
        if len(current) != size or _digest(current) != checksum:
            raise RawArchiveError("hot deletion target changed after route switch")
        path.unlink()
        raw_evidence._fsync_directory(path.parent)
        removed += size
    with _transaction(connection):
        _event(connection, "hot_evicted", "verified_archive_D8", blob_id=blob_id,
            archive_id=archive_id, evidence={"removed_bytes": removed})
    return {"blob_id": blob_id, "status": "hot_evicted", "removed_bytes": removed,
            "archive_id": archive_id, "archive_preserved": True}


def _tables(connection: sqlite3.Connection) -> set[str]:
    return {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def protected_retention_reasons(connection: sqlite3.Connection, blob_id: int, *, at: str) -> list[str]:
    """D31 protection is evidence-specific, not an eternal ordinary FK hold."""
    reasons = [value for value in retention_reasons(connection, blob_id, at=at) if value != "response_reference"]
    if lifecycle_stage(_effective_hot_time(connection, _row(connection, "provider_raw_blobs", blob_id)), at=at) != "retention":
        reasons.append("recent_rehydrated_body")
    responses = _rows(connection, "SELECT * FROM provider_raw_responses WHERE raw_blob_id=?", (blob_id,))
    tables = _tables(connection)
    for response in responses:
        raw_id, attempt_id, scope = response["id"], response["fetch_attempt_id"], response["paid_scope_identity"]
        if lifecycle_stage(response["raw_stored_at"], at=at) != "retention":
            reasons.append("recent_or_unknown_response_time")
        if {"provider_usage_settlements", "provider_usage_settlement_events"} <= tables:
            if connection.execute("""SELECT 1 FROM provider_usage_settlements s
                JOIN provider_usage_settlement_events e ON e.settlement_id=s.id
                WHERE s.scope_identity=? AND e.id=(SELECT max(id) FROM provider_usage_settlement_events WHERE settlement_id=s.id)
                AND e.state='charged_unverified' LIMIT 1""", (scope,)).fetchone():
                reasons.append("charged_unverified")
        if "provider_usage" in tables:
            # Terminal settlements override historical unknown ledger state.
            terminal = """AND NOT EXISTS(SELECT 1 FROM provider_usage_settlements s
                JOIN provider_usage_settlement_events e ON e.settlement_id=s.id
                WHERE s.provider_usage_id=u.id AND e.id=(SELECT max(id) FROM provider_usage_settlement_events WHERE settlement_id=s.id)
                AND e.state IN ('charged_verified','refunded'))""" if {"provider_usage_settlements", "provider_usage_settlement_events"} <= tables else ""
            slot_match = "OR json_extract(u.details_json,'$.slot_id')=(SELECT slot_id FROM fetch_attempts WHERE id=?)" if "fetch_attempts" in tables else ""
            if connection.execute("""SELECT 1 FROM provider_usage u WHERE json_valid(u.details_json)
                AND json_extract(u.details_json,'$.state') IN ('billing_unknown','charged_unverified','reserved')
                AND (json_extract(u.details_json,'$.raw_response_id')=? OR json_extract(u.details_json,'$.attempt_id')=?
                  OR json_extract(u.details_json,'$.paid_scope_identity')=? """ + slot_match + ") " + terminal + " LIMIT 1",
                (raw_id, attempt_id, scope, *((attempt_id,) if slot_match else ()))).fetchone():
                reasons.append("unresolved_usage")
        if {"fetch_dead_letters", "capture_work_items"} <= tables:
            if connection.execute("""SELECT 1 FROM fetch_dead_letters d JOIN capture_work_items w ON w.id=d.work_id
                WHERE w.state!='terminal' AND ((? IS NOT NULL AND w.content_id=?)
                OR (? IS NULL AND w.account_id=?)) LIMIT 1""",
                (response["content_id"], response["content_id"], response["content_id"], response["account_id"])).fetchone():
                reasons.append("open_dead_letter")
        if "operational_alerts" in tables:
            if connection.execute("""SELECT 1 FROM operational_alerts a WHERE a.status='open' AND (
                json_extract(a.scope_json,'$.raw_blob_id')=? OR json_extract(a.scope_json,'$.raw_response_id')=?
                OR json_extract(a.scope_json,'$.paid_scope_identity')=?
                OR (? IS NOT NULL AND json_extract(a.scope_json,'$.content_id')=?)
                OR (? IS NULL AND json_extract(a.scope_json,'$.account_id')=?)) LIMIT 1""",
                (blob_id, raw_id, scope, response["content_id"], response["content_id"], response["content_id"], response["account_id"])).fetchone():
                reasons.append("open_quality_or_incident_investigation")
    return sorted(set(reasons))


def retire_archive(connection: sqlite3.Connection, archive_id: int, *, at: str,
                   live_root: Path, daily_stored_p95: int = 0) -> dict[str, Any]:
    """D31 compact protected members, publish read routes, then retire old tar."""
    if connection.in_transaction:
        raise RawArchiveError("archive retirement requires standalone writer transaction")
    original = _row(connection, "raw_archives", archive_id)
    if (day_for_timestamp(at) - date.fromisoformat(original["business_day"])).days < 31:
        raise RawArchiveError("archive is not D31 eligible")
    volume = validate_archive_volume(archive_root=_archive_root_for(original), live_root=live_root,
                                     daily_stored_p95=daily_stored_p95)
    if original["state"] == "deleted":
        return _cleanup_retired_archive(connection, original)
    verify_archive(connection, archive_id)
    manifest = json.loads(original["manifest_json"])
    protection = {member["blob_id"]: protected_retention_reasons(connection, member["blob_id"], at=at)
                  for member in manifest["members"]}
    retained = [member for member in manifest["members"] if protection[member["blob_id"]]]
    discarded = [member for member in manifest["members"] if not protection[member["blob_id"]]]
    if not discarded:
        return {"archive_id": archive_id, "status": "protected", "protected": protection, "removed_bytes": 0}
    replacement = _write_archive_manifest(connection, {**manifest, "members": retained}, volume=volume,
        live_root=live_root, daily_stored_p95=daily_stored_p95, source_archive_id=archive_id) if retained else None
    # D8 route must be committed before a blob can reach its final retention state.
    for member in manifest["members"]:
        blob = _row(connection, "provider_raw_blobs", member["blob_id"])
        if blob["hot_state"] == "present" or Path(blob["hot_path"]).exists():
            evict_archived_hot(connection, member["blob_id"], at=at, live_root=live_root,
                               verified_archive_id=archive_id)
    with write_lock():
        connection.execute("BEGIN IMMEDIATE")
        try:
            # A hold introduced during long archive I/O cancels retirement, leaving
            # the verified replacement as an extra safe copy for the next tick.
            for member in discarded:
                if protected_retention_reasons(connection, member["blob_id"], at=at):
                    raise RawArchiveError("retention protection changed before retirement")
            for member in discarded:
                blob_id = int(member["blob_id"])
                _event(connection, "retention_retire_intent", "D31_non_sample_unheld", blob_id=blob_id,
                       archive_id=archive_id, evidence={"at": at})
                connection.execute("UPDATE provider_raw_blobs SET hot_state='deleted' WHERE id=?", (blob_id,))
            _event(connection, "archive_retire_intent", "D31_compacted_verified", archive_id=archive_id,
                evidence={"retired_blob_ids": [m["blob_id"] for m in discarded],
                          "replacement_archive_id": replacement["archive_id"] if replacement else None,
                          "preserved_blob_ids": [m["blob_id"] for m in retained]})
            connection.execute("UPDATE raw_archives SET state='deleted' WHERE id=?", (archive_id,))
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
    result = _cleanup_retired_archive(connection, _row(connection, "raw_archives", archive_id))
    return {**result, "retired_blobs": len(discarded), "retained_blobs": len(retained),
            "replacement_archive_id": replacement["archive_id"] if replacement else None}


def _cleanup_retired_archive(connection: sqlite3.Connection, archive: dict[str, Any]) -> dict[str, Any]:
    intent = connection.execute("SELECT 1 FROM raw_retention_events WHERE archive_id=? AND event_type='archive_retire_intent'", (archive["id"],)).fetchone()
    if archive["state"] != "deleted" or not intent:
        raise RawArchiveError("archive has no committed retirement intent")
    path = Path(archive["local_path"])
    if _safe_existing(_archive_root_for(archive)).stat().st_dev != archive["storage_device"]:
        raise RawArchiveError("archive volume changed before retired-file cleanup")
    removed = 0
    if path.exists() or path.is_symlink():
        checksum, size = _hash_file(path)
        if checksum != archive["archive_sha256"] or size != archive["byte_size"]:
            raise RawArchiveError("retired archive changed before cleanup")
        path.unlink()
        raw_evidence._fsync_directory(path.parent)
        removed = size
    with _transaction(connection):
        _event(connection, "archive_retired", "D31_physical_cleanup", archive_id=archive["id"], evidence={"removed_bytes": removed})
    return {"archive_id": archive["id"], "status": "retired", "removed_bytes": removed, "ledger_preserved": True}


def maintenance_tick(connection: sqlite3.Connection, *, at: str, live_root: Path,
                      archive_root: Path = ARCHIVE_ROOT, daily_stored_p95: int = 0,
                      max_days: int = 1, max_blobs: int = 256) -> dict[str, Any]:
    """Bounded D8/D31 maintenance; caller owns writer lock, never an HTTP path."""
    if connection.in_transaction:
        raise RawArchiveError("raw maintenance requires standalone writer connection")
    if type(max_days) is not int or not 1 <= max_days <= 7 or type(max_blobs) is not int or not 1 <= max_blobs <= 4096:
        raise ValueError("raw maintenance batch is outside fixed bounds")
    result: dict[str, Any] = {"schema": "raw-maintenance-v1", "at": at, "status": "complete",
        "archives": [], "hot_evictions": [], "retention": [], "provider_calls": 0}
    try:
        result["volume"] = validate_archive_volume(archive_root=archive_root, live_root=live_root,
            daily_stored_p95=daily_stored_p95)
        for day in due_archive_days(connection, at=at)[:max_days]:
            result["archives"].append(archive_day(connection, day, at=at, live_root=live_root,
                archive_root=archive_root, daily_stored_p95=daily_stored_p95))
        verified: set[int] = set()
        blobs = _rows(connection, "SELECT * FROM provider_raw_blobs WHERE hot_owned=1 AND hot_state!='deleted' AND raw_stored_at IS NOT NULL ORDER BY raw_stored_at,id")
        for blob in blobs:
            if len(result["hot_evictions"]) >= max_blobs:
                break
            if lifecycle_stage(_effective_hot_time(connection, blob), at=at) == "hot":
                continue
            if blob["hot_state"] == "evicted" and not Path(blob["hot_path"]).exists():
                last_cleanup = connection.execute("SELECT event_type FROM raw_retention_events WHERE raw_blob_id=? AND event_type IN ('hot_evict_intent','hot_evicted') ORDER BY id DESC LIMIT 1", (blob["id"],)).fetchone()
                if last_cleanup and last_cleanup[0] == "hot_evicted":
                    continue
            found = connection.execute("""SELECT a.id FROM raw_archives a JOIN raw_archive_members m ON m.archive_id=a.id
                WHERE m.raw_blob_id=? AND a.state='verified' ORDER BY a.id DESC LIMIT 1""", (blob["id"],)).fetchone()
            if found is None:
                continue
            archive_id = int(found[0])
            if archive_id not in verified:
                verify_archive(connection, archive_id)
                verified.add(archive_id)
            result["hot_evictions"].append(evict_archived_hot(connection, blob["id"], at=at,
                live_root=live_root, verified_archive_id=archive_id))
        archives = _rows(connection, """SELECT * FROM raw_archives a WHERE state IN ('verified','deleted')
            ORDER BY COALESCE((SELECT max(e.id) FROM raw_retention_events e WHERE e.archive_id=a.id
                AND e.event_type='retention_checked'),0),business_day,id""")
        for archive in archives:
            if len(result["retention"]) >= max_days:
                break
            if (day_for_timestamp(at) - date.fromisoformat(archive["business_day"])).days < 31:
                continue
            if archive["state"] == "deleted" and not Path(archive["local_path"]).exists():
                continue
            result["retention"].append(retire_archive(connection, archive["id"], at=at,
                live_root=live_root, daily_stored_p95=daily_stored_p95))
            with _transaction(connection):
                _event(connection, "retention_checked", "D31_policy_checked", archive_id=archive["id"],
                       evidence=result["retention"][-1])
    except (RawArchiveError, raw_evidence.RawEvidenceError, OSError) as error:
        result.update(status="blocked", reason=str(error), error_class=type(error).__name__)
    with _transaction(connection):
        _event(connection, "maintenance_receipt", result["status"], evidence=result)
    return result
