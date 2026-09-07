"""Crash-safe immutable provider response evidence.

This module deliberately has no database or provider dependencies.  Callers
must acquire a paid-send claim before network I/O, persist the exact decoded
HTTP entity bytes here, and only then register the returned stored-byte
receipt in SQLite.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import zstandard


RAW_EVIDENCE_SCHEMA = "provider-raw-sidecar-v1"
PAID_SEND_CLAIM_SCHEMA = "paid-send-claim-v1"
ZSTD_LEVEL = 3
MAX_RAW_BYTES = 512 * 1024 * 1024
# Zstd's documented compression bound for inputs at least 128 KiB is
# ``size + (size >> 8)``. Keep a small fixed margin so the reader can always
# consume a valid maximum-size entity even when compression expands it.
MAX_STORED_BYTES = MAX_RAW_BYTES + (MAX_RAW_BYTES >> 8) + 64 * 1024
MAX_SIDECAR_BYTES = 64 * 1024

_HEX_DIGITS = frozenset("0123456789abcdef")
_SAFE_COMPONENT = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_SIDECAR_KEYS = frozenset(
    {
        "codec",
        "entity_sha256",
        "entity_size",
        "operation",
        "paid_scope_identity",
        "provider",
        "raw_filename",
        "response_identity",
        "schema",
        "sequence",
        "stored_sha256",
        "stored_size",
        "zstd_level",
    }
)


class RawEvidenceError(RuntimeError):
    """Stored evidence is unsafe, incomplete, or fails its receipt contract."""

    error_code = "storage_hard"


class RawEvidenceConflict(RawEvidenceError):
    """An immutable path already exists with a different or partial value."""


class PaidSendClaimHeld(RawEvidenceError):
    """A paid-scope identity is already claimed and must not be sent again."""

    error_code = "paid_identity_hold"


@dataclass(frozen=True)
class RawEvidenceReceipt:
    """Hashes and sizes for the HTTP entity and its stored representation."""

    path: Path
    sidecar_path: Path | None
    codec: str
    entity_sha256: str
    entity_size: int
    stored_sha256: str
    stored_size: int
    provider: str | None = None
    operation: str | None = None
    response_identity: str | None = None
    paid_scope_identity: str | None = None
    sequence: int | None = None


@dataclass(frozen=True)
class LoadedRawEvidence:
    """Verified raw bytes and their immutable receipt."""

    entity_bytes: bytes
    receipt: RawEvidenceReceipt


@dataclass(frozen=True)
class PaidSendClaim:
    """A durable, exclusive claim that authorizes one paid send attempt."""

    path: Path
    paid_scope_identity: str
    sequence: int
    sha256: str
    byte_size: int


@dataclass(frozen=True)
class QuarantineReceipt:
    """Immutable receipt for bytes that never became canonical JSON."""

    path: Path
    sha256: str
    byte_size: int


@dataclass(frozen=True)
class ImmutableJsonReceipt:
    """Receipt for a small immutable JSON control/evidence sidecar."""

    path: Path
    sha256: str
    byte_size: int


def canonical_json_bytes(value: object) -> bytes:
    """Serialize a receipt/identity value deterministically."""

    try:
        rendered = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("value must be canonical JSON") from exc
    return (rendered + "\n").encode("utf-8")


def sidecar_path_for(path: Path) -> Path:
    """Return the immutable metadata path for a compressed raw response."""

    return path.with_name(f"{path.name}.metadata.json")


def compress_entity_bytes(entity_bytes: bytes) -> bytes:
    """Return the deterministic stored representation used by new raw rows."""

    if not isinstance(entity_bytes, bytes):
        raise TypeError("entity_bytes must be bytes")
    if len(entity_bytes) > MAX_RAW_BYTES:
        raise RawEvidenceError("HTTP entity exceeds the raw evidence size limit")
    return zstandard.ZstdCompressor(level=ZSTD_LEVEL).compress(entity_bytes)


def write_zstd_raw_evidence(
    path: Path,
    entity_bytes: bytes,
    *,
    provider: str,
    operation: str,
    response_identity: str,
    paid_scope_identity: str,
    sequence: int,
    evidence_root: Path | None = None,
) -> RawEvidenceReceipt:
    """Persist exact HTTP entity bytes as zstd level 3 plus an immutable sidecar.

    Existing complete evidence is accepted only when every identity and byte
    receipt matches.  A one-sided artifact or different content is a hard
    conflict; neither raw bytes nor sidecar are ever overwritten.
    """

    if path.name in {"", ".", ".."} or not path.name.endswith(".json.zst"):
        raise ValueError("compressed raw evidence path must end with .json.zst")
    _require_name(provider, field="provider")
    _require_name(operation, field="operation")
    _require_sha256(response_identity, field="response_identity")
    _require_sha256(paid_scope_identity, field="paid_scope_identity")
    _require_sequence(sequence)
    if not isinstance(entity_bytes, bytes):
        raise TypeError("entity_bytes must be bytes")
    if len(entity_bytes) > MAX_RAW_BYTES:
        raise RawEvidenceError("HTTP entity exceeds the raw evidence size limit")

    path = _confined_file_path(path, evidence_root=evidence_root)
    stored_bytes = compress_entity_bytes(entity_bytes)
    entity_sha256 = _sha256(entity_bytes)
    stored_sha256 = _sha256(stored_bytes)
    sidecar_path = sidecar_path_for(path)
    sidecar = {
        "codec": "zstd",
        "entity_sha256": entity_sha256,
        "entity_size": len(entity_bytes),
        "operation": operation,
        "paid_scope_identity": paid_scope_identity,
        "provider": provider,
        "raw_filename": path.name,
        "response_identity": response_identity,
        "schema": RAW_EVIDENCE_SCHEMA,
        "sequence": sequence,
        "stored_sha256": stored_sha256,
        "stored_size": len(stored_bytes),
        "zstd_level": ZSTD_LEVEL,
    }
    sidecar_bytes = canonical_json_bytes(sidecar)

    raw_exists = _path_exists(path)
    sidecar_exists = _path_exists(sidecar_path)
    if raw_exists or sidecar_exists:
        if not raw_exists or not sidecar_exists:
            raise RawEvidenceConflict("raw evidence and sidecar are incomplete")
        loaded = read_raw_evidence(path)
        expected = RawEvidenceReceipt(
            path=path,
            sidecar_path=sidecar_path,
            codec="zstd",
            entity_sha256=entity_sha256,
            entity_size=len(entity_bytes),
            stored_sha256=stored_sha256,
            stored_size=len(stored_bytes),
            provider=provider,
            operation=operation,
            response_identity=response_identity,
            paid_scope_identity=paid_scope_identity,
            sequence=sequence,
        )
        if loaded.receipt != expected or loaded.entity_bytes != entity_bytes:
            raise RawEvidenceConflict("raw evidence identity already has another value")
        return expected

    _publish_no_replace(path, stored_bytes, evidence_root=evidence_root)
    try:
        _publish_no_replace(
            sidecar_path,
            sidecar_bytes,
            evidence_root=evidence_root,
        )
    except BaseException as exc:
        raise RawEvidenceConflict(
            "compressed raw evidence exists without its immutable sidecar"
        ) from exc
    loaded = read_raw_evidence(path)
    if loaded.entity_bytes != entity_bytes:
        raise RawEvidenceError("raw evidence changed after publication")
    return loaded.receipt


def read_raw_evidence(
    path: Path,
    *,
    expected_stored_sha256: str | None = None,
    expected_stored_size: int | None = None,
) -> LoadedRawEvidence:
    """Safely read legacy ``.json`` or sidecar-verified ``.json.zst`` evidence."""

    legacy_json = path.name.endswith(".json") and not path.name.endswith(
        ".json.zst"
    )
    stored_bytes = _read_single_regular(
        path,
        max_bytes=MAX_RAW_BYTES if legacy_json else MAX_STORED_BYTES,
        allow_legacy_read_mode=legacy_json,
    )
    stored_sha256 = _sha256(stored_bytes)
    if expected_stored_sha256 is not None:
        _require_sha256(expected_stored_sha256, field="expected_stored_sha256")
        if stored_sha256 != expected_stored_sha256:
            raise RawEvidenceError("stored raw SHA-256 does not match database receipt")
    if expected_stored_size is not None:
        if (
            isinstance(expected_stored_size, bool)
            or not isinstance(expected_stored_size, int)
            or expected_stored_size < 0
        ):
            raise ValueError("expected_stored_size must be non-negative")
        if len(stored_bytes) != expected_stored_size:
            raise RawEvidenceError(
                "stored raw byte size does not match database receipt"
            )

    if legacy_json:
        receipt = RawEvidenceReceipt(
            path=path,
            sidecar_path=None,
            codec="identity",
            entity_sha256=stored_sha256,
            entity_size=len(stored_bytes),
            stored_sha256=stored_sha256,
            stored_size=len(stored_bytes),
        )
        return LoadedRawEvidence(entity_bytes=stored_bytes, receipt=receipt)
    if not path.name.endswith(".json.zst"):
        raise RawEvidenceError("raw evidence path has an unsupported suffix")

    sidecar_path = sidecar_path_for(path)
    sidecar_bytes = _read_single_regular(sidecar_path, max_bytes=MAX_SIDECAR_BYTES)
    sidecar = _decode_sidecar(sidecar_bytes, path=path)
    if sidecar["stored_sha256"] != stored_sha256:
        raise RawEvidenceError("compressed raw SHA-256 does not match sidecar")
    if sidecar["stored_size"] != len(stored_bytes):
        raise RawEvidenceError("compressed raw size does not match sidecar")
    entity_size = _sidecar_int(sidecar, "entity_size")
    if entity_size > MAX_RAW_BYTES:
        raise RawEvidenceError("sidecar entity size exceeds the safety limit")
    try:
        entity_bytes = zstandard.ZstdDecompressor().decompress(
            stored_bytes,
            max_output_size=entity_size,
        )
    except zstandard.ZstdError as exc:
        raise RawEvidenceError(
            "compressed raw evidence cannot be decompressed"
        ) from exc
    if len(entity_bytes) != entity_size:
        raise RawEvidenceError("HTTP entity size does not match sidecar")
    if _sha256(entity_bytes) != sidecar["entity_sha256"]:
        raise RawEvidenceError("HTTP entity SHA-256 does not match sidecar")

    receipt = RawEvidenceReceipt(
        path=path,
        sidecar_path=sidecar_path,
        codec="zstd",
        entity_sha256=str(sidecar["entity_sha256"]),
        entity_size=entity_size,
        stored_sha256=stored_sha256,
        stored_size=len(stored_bytes),
        provider=str(sidecar["provider"]),
        operation=str(sidecar["operation"]),
        response_identity=str(sidecar["response_identity"]),
        paid_scope_identity=str(sidecar["paid_scope_identity"]),
        sequence=_sidecar_int(sidecar, "sequence"),
    )
    return LoadedRawEvidence(entity_bytes=entity_bytes, receipt=receipt)


def read_legacy_migration_evidence(
    path: Path,
    *,
    expected_stored_sha256: str,
    expected_stored_size: int,
) -> LoadedRawEvidence:
    """Inventory historical JSON only, with a mandatory pre-existing receipt.

    A historical owned/non-shared-writable JSON inode may have hard links. This
    exception is deliberately absent from runtime reads and all raw writers.
    Its exact bytes and complete stat identity are checked while the read-only
    descriptor is still open; neither link nor permissions are changed.
    """
    _require_sha256(expected_stored_sha256, field="expected_stored_sha256")
    if (isinstance(expected_stored_size, bool)
            or not isinstance(expected_stored_size, int) or expected_stored_size < 0):
        raise ValueError("expected_stored_size must be non-negative")
    if path.name.endswith(".json.zst"):
        # Compressed files belong to the new evidence contract: no link exception.
        return read_raw_evidence(path, expected_stored_sha256=expected_stored_sha256,
                                 expected_stored_size=expected_stored_size)
    if not path.name.endswith(".json"):
        raise RawEvidenceError("raw evidence path has an unsupported suffix")
    stored = _read_regular_evidence(
        path, max_bytes=MAX_RAW_BYTES, allow_legacy_read_mode=True,
        migration_receipt=(expected_stored_sha256, expected_stored_size))
    receipt = RawEvidenceReceipt(
        path=path, sidecar_path=None, codec="identity",
        entity_sha256=expected_stored_sha256, entity_size=len(stored),
        stored_sha256=expected_stored_sha256, stored_size=len(stored))
    return LoadedRawEvidence(entity_bytes=stored, receipt=receipt)


def read_raw_json(
    path: Path,
    *,
    expected_stored_sha256: str | None = None,
    expected_stored_size: int | None = None,
) -> Any:
    """Read verified legacy/compressed evidence and decode one JSON value."""

    loaded = read_raw_evidence(
        path,
        expected_stored_sha256=expected_stored_sha256,
        expected_stored_size=expected_stored_size,
    )
    try:
        return json.loads(loaded.entity_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RawEvidenceError("raw evidence is not valid UTF-8 JSON") from exc


def claim_paid_send(
    claim_root: Path,
    *,
    paid_scope_identity: str,
    sequence: int,
    claim: Mapping[str, object],
) -> PaidSendClaim:
    """Acquire a durable one-winner claim before a paid network send.

    Creation uses ``O_EXCL`` directly.  Any pre-existing file, including a
    partial file left by a crash, is a permanent hold for automatic sends.
    """

    _require_sha256(paid_scope_identity, field="paid_scope_identity")
    _require_sequence(sequence)
    directory = _ensure_private_directory(
        claim_root / paid_scope_identity[:2],
        evidence_root=claim_root,
    )
    path = directory / f"{paid_scope_identity}.sequence-{sequence:08d}.claim.json"
    body = canonical_json_bytes(
        {
            "claim": dict(claim),
            "paid_scope_identity": paid_scope_identity,
            "schema": PAID_SEND_CLAIM_SCHEMA,
            "sequence": sequence,
        }
    )
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError as exc:
        raise PaidSendClaimHeld(f"paid send claim already exists: {path}") from exc
    try:
        _write_all(descriptor, body)
        os.fsync(descriptor)
    except BaseException:
        # The incomplete file is intentionally retained as a fail-closed hold.
        raise
    finally:
        os.close(descriptor)
    _fsync_directory(directory)
    if _read_single_regular(path, max_bytes=MAX_SIDECAR_BYTES) != body:
        raise RawEvidenceError("paid send claim changed after publication")
    return PaidSendClaim(
        path=path,
        paid_scope_identity=paid_scope_identity,
        sequence=sequence,
        sha256=_sha256(body),
        byte_size=len(body),
    )


def write_quarantine_evidence(
    path: Path,
    value: bytes,
    *,
    evidence_root: Path | None = None,
) -> QuarantineReceipt:
    """Persist partial encoded bytes without exposing them as provider raw."""

    if not isinstance(value, bytes):
        raise TypeError("quarantine evidence must be bytes")
    if len(value) > MAX_RAW_BYTES:
        raise RawEvidenceError("quarantine evidence exceeds the safety size limit")
    path = _confined_file_path(path, evidence_root=evidence_root)
    digest = _sha256(value)
    if _path_exists(path):
        existing = _read_single_regular(path, max_bytes=MAX_RAW_BYTES)
        if existing != value:
            raise RawEvidenceConflict("quarantine identity already has another value")
    else:
        _publish_no_replace(path, value, evidence_root=evidence_root)
    return QuarantineReceipt(path=path, sha256=digest, byte_size=len(value))


def write_immutable_json_receipt(
    path: Path,
    value: Mapping[str, object],
    *,
    evidence_root: Path | None = None,
) -> ImmutableJsonReceipt:
    """Persist one small canonical JSON receipt without replacement."""

    body = canonical_json_bytes(dict(value))
    if len(body) > MAX_SIDECAR_BYTES:
        raise RawEvidenceError("immutable JSON receipt exceeds the safety size limit")
    path = _confined_file_path(path, evidence_root=evidence_root)
    if _path_exists(path):
        if _read_single_regular(path, max_bytes=MAX_SIDECAR_BYTES) != body:
            raise RawEvidenceConflict("immutable JSON receipt already has another value")
    else:
        _publish_no_replace(path, body, evidence_root=evidence_root)
    return ImmutableJsonReceipt(
        path=path,
        sha256=_sha256(body),
        byte_size=len(body),
    )


def _publish_no_replace(
    path: Path,
    value: bytes,
    *,
    evidence_root: Path | None = None,
) -> None:
    """Publish bytes with temp/fsync/no-replace reservation/rename/dir-fsync."""

    path = _confined_file_path(path, evidence_root=evidence_root)
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(12)}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(temporary, flags, 0o600)
    try:
        _write_all(descriptor, value)
        os.fsync(descriptor)
    except BaseException:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise
    finally:
        os.close(descriptor)

    reserved = False
    try:
        reservation = os.open(path, flags, 0o600)
        reserved = True
        os.close(reservation)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    except BaseException:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        if reserved:
            # Keep the reserved destination as a fail-closed crash marker.
            _fsync_directory(path.parent)
        raise


def _write_all(descriptor: int, value: bytes) -> None:
    view = memoryview(value)
    written = 0
    while written < len(view):
        count = os.write(descriptor, view[written:])
        if count <= 0:
            raise RawEvidenceError("short write while persisting immutable evidence")
        written += count


def _read_single_regular(
    path: Path,
    *,
    max_bytes: int,
    allow_legacy_read_mode: bool = False,
) -> bytes:
    return _read_regular_evidence(path, max_bytes=max_bytes,
                                  allow_legacy_read_mode=allow_legacy_read_mode)


def _stat_identity(value: os.stat_result) -> tuple[int, ...]:
    # Reading may update atime; all identity, size and safety attributes must stay.
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns,
            value.st_ctime_ns, value.st_nlink, value.st_uid, value.st_gid, value.st_mode)


def _read_regular_evidence(
    path: Path,
    *,
    max_bytes: int,
    allow_legacy_read_mode: bool = False,
    migration_receipt: tuple[str, int] | None = None,
) -> bytes:
    try:
        before = path.lstat()
    except FileNotFoundError as exc:
        raise RawEvidenceError(f"evidence file is missing: {path}") from exc
    if stat.S_ISLNK(before.st_mode):
        raise RawEvidenceError(f"evidence file must not be a symlink: {path}")
    flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise RawEvidenceError(
            f"evidence file cannot be opened safely: {path}"
        ) from exc
    try:
        opened = os.fstat(descriptor)
        mode = stat.S_IMODE(opened.st_mode)
        invalid_mode = (
            bool(mode & 0o022)
            if allow_legacy_read_mode
            else mode != 0o600
        )
        if (
            not stat.S_ISREG(opened.st_mode)
            or (opened.st_nlink != 1 and migration_receipt is None)
            or opened.st_uid != os.geteuid()
            or invalid_mode
        ):
            raise RawEvidenceError(
                "evidence file must be current-user, non-writable-by-others, "
                "regular, and single-link"
            )
        if _stat_identity(before) != _stat_identity(opened):
            raise RawEvidenceError("evidence file changed while it was opened")
        if opened.st_size > max_bytes:
            raise RawEvidenceError("evidence file exceeds its safety size limit")
        if migration_receipt is not None and opened.st_size != migration_receipt[1]:
            raise RawEvidenceError("stored raw byte size does not match database receipt")
        chunks: list[bytes] = []
        remaining = opened.st_size
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1024 * 1024))
            if not chunk:
                raise RawEvidenceError("evidence file ended before its recorded size")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise RawEvidenceError("evidence file grew while it was read")
        stored = b"".join(chunks)
        if migration_receipt is not None and _sha256(stored) != migration_receipt[0]:
            raise RawEvidenceError("stored raw SHA-256 does not match database receipt")
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    try:
        named = path.lstat()
    except FileNotFoundError as exc:
        raise RawEvidenceError("evidence file disappeared while it was read") from exc
    if not (_stat_identity(opened) == _stat_identity(after) == _stat_identity(named)):
        raise RawEvidenceError("evidence file changed while it was read")
    return stored


def _decode_sidecar(value: bytes, *, path: Path) -> dict[str, object]:
    try:
        decoded = json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RawEvidenceError("raw evidence sidecar is not valid JSON") from exc
    if not isinstance(decoded, dict) or frozenset(decoded) != _SIDECAR_KEYS:
        raise RawEvidenceError("raw evidence sidecar shape is invalid")
    sidecar = {str(key): item for key, item in decoded.items()}
    if sidecar["schema"] != RAW_EVIDENCE_SCHEMA:
        raise RawEvidenceError("raw evidence sidecar schema is unsupported")
    if sidecar["codec"] != "zstd" or sidecar["zstd_level"] != ZSTD_LEVEL:
        raise RawEvidenceError("raw evidence compression contract is invalid")
    if sidecar["raw_filename"] != path.name:
        raise RawEvidenceError("raw evidence sidecar is bound to another file")
    try:
        for field in (
            "entity_sha256",
            "stored_sha256",
            "response_identity",
            "paid_scope_identity",
        ):
            _require_sha256(sidecar[field], field=field)
        _require_name(sidecar["provider"], field="provider")
        _require_name(sidecar["operation"], field="operation")
        _sidecar_int(sidecar, "entity_size")
        _sidecar_int(sidecar, "stored_size")
        _require_sequence(_sidecar_int(sidecar, "sequence"))
    except ValueError as exc:
        raise RawEvidenceError("raw evidence sidecar fields are invalid") from exc
    return sidecar


def _sidecar_int(sidecar: Mapping[str, object], field: str) -> int:
    value = sidecar[field]
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RawEvidenceError(f"raw evidence sidecar {field} is invalid")
    return value


def _confined_file_path(path: Path, *, evidence_root: Path | None) -> Path:
    if path.name in {"", ".", ".."} or "/" in path.name or "\\" in path.name:
        raise RawEvidenceError("evidence filename is unsafe")
    parent = _ensure_private_directory(path.parent, evidence_root=evidence_root)
    return parent / path.name


def _ensure_private_directory(
    path: Path,
    *,
    evidence_root: Path | None = None,
) -> Path:
    if evidence_root is not None:
        root = Path(os.path.abspath(os.fspath(evidence_root)))
        candidate = Path(os.path.abspath(os.fspath(path)))
        try:
            relative = candidate.relative_to(root)
        except ValueError as exc:
            raise RawEvidenceError("evidence path escapes its configured root") from exc
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        _validate_private_directory(root)
        current = root
        for component in relative.parts:
            current = current / component
            try:
                metadata = current.lstat()
            except FileNotFoundError:
                try:
                    current.mkdir(mode=0o700)
                except FileExistsError:
                    pass
                metadata = current.lstat()
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
                raise RawEvidenceError(f"evidence directory is unsafe: {current}")
            if metadata.st_uid != os.geteuid():
                raise RawEvidenceError(
                    f"evidence directory has another owner: {current}"
                )
            os.chmod(current, 0o700)
        return candidate

    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    _validate_private_directory(path)
    return path


def _validate_private_directory(path: Path) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError as exc:
        raise RawEvidenceError(f"evidence directory is missing: {path}") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise RawEvidenceError(f"evidence directory is unsafe: {path}")
    if metadata.st_uid != os.geteuid():
        raise RawEvidenceError(f"evidence directory has another owner: {path}")
    os.chmod(path, 0o700)


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | os.O_CLOEXEC
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _path_exists(path: Path) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    return True


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _require_sha256(value: object, *, field: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in _HEX_DIGITS for character in value)
    ):
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")
    return value


def _require_name(value: object, *, field: str) -> str:
    if (
        not isinstance(value, str)
        or _SAFE_COMPONENT.fullmatch(value) is None
        or value in {".", ".."}
    ):
        raise ValueError(f"{field} must be one safe path component")
    return value


def require_path_component(value: object, *, field: str) -> str:
    """Validate a provider-controlled value before using it in an evidence path."""

    return _require_name(value, field=field)


def _require_sequence(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("sequence must be a non-negative integer")
    return value
