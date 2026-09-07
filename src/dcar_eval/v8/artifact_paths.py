"""Read-only artifact relocation from an installed, hash-bound snapshot.

No signed file or database path is rewritten. Only exact transport/managed
members in the installed manifest can be relocated from the writer root.
This does not map the external original archive or grant write permissions.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from contextlib import contextmanager
from contextvars import ContextVar
from functools import lru_cache
from pathlib import Path, PurePosixPath
from typing import Any, Iterator

from .snapshot_contract import ARTIFACT_POLICY, MANAGED_ORIGINALS_CONTRACT, validate_descriptor

PROJECT_ROOT = Path(__file__).resolve().parents[3]
ACTIVE_SNAPSHOT = Path("/var/lib/dcar-aigc/runtime/active-snapshot.json")
_READ_ROOT: ContextVar[Path | None] = ContextVar("dcar_read_artifact_root", default=None)
_SHA = re.compile(r"[0-9a-f]{64}")
_SNAPSHOT = re.compile(r"[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}")
MAX_SNAPSHOT_RECEIPT_BYTES = 1024 * 1024
MAX_SNAPSHOT_MANIFEST_BYTES = 256 * 1024 * 1024
RUNTIME_EVIDENCE_ALIAS_CONTRACT = "runtime-evidence-alias-v1"
RUNTIME_EVIDENCE_DIRECTORY = "data/cache/v8/runtime_evidence"


def runtime_evidence_source(value: str, state_root: str) -> str:
    """Allow only the formal, immutable receipt namespaces, never arbitrary files."""
    root, source = Path(state_root), Path(value)
    if (not root.is_absolute() or root.parts[-3:] != ("Library", "Application Support", "DcarAIGC")
            or not source.is_absolute() or any(char in value for char in ("\0", "\n", "\r", "\\"))
            or ".." in root.parts or ".." in source.parts):
        raise ArtifactPathError("snapshot_runtime_evidence_source_invalid")
    try:
        relative = source.relative_to(root).as_posix()
    except ValueError as exc:
        raise ArtifactPathError("snapshot_runtime_evidence_source_invalid") from exc
    ordinary = r"evidence/(?:scan-verification-v2|profile-day-coverage-v3)\.[0-9a-f]{64}\.json"
    transport = (r"(?:data/current-hold-control|rollouts/[A-Za-z0-9][A-Za-z0-9_.-]*)/"
                 r"transport-receipt\.(?:cohort|campaign|member_permit|campaign_terminal|qualification|"
                 r"accounting_terminal|route_verdict)\.[0-9a-f]{64}\.json")
    if re.fullmatch(ordinary, relative) is None and re.fullmatch(transport, relative) is None:
        raise ArtifactPathError("snapshot_runtime_evidence_source_invalid")
    return relative


def runtime_evidence_aliases(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Validate aliases against exact required transport identities in the manifest."""
    contract = manifest.get("runtime_evidence_aliases")
    if contract is None:
        return {}
    if (not isinstance(contract, dict) or contract.get("contract_version") != RUNTIME_EVIDENCE_ALIAS_CONTRACT
            or not isinstance(contract.get("writer_state_root"), str) or not isinstance(contract.get("files"), list)):
        raise ArtifactPathError("snapshot_runtime_evidence_alias_invalid")
    required = {row["project_path"]: row for row in manifest["files"]
                if str(row.get("project_path", "")).startswith(RUNTIME_EVIDENCE_DIRECTORY + "/")}
    aliases: dict[str, dict[str, Any]] = {}
    for row in contract["files"]:
        if (not isinstance(row, dict) or set(row) != {"source_path", "project_path", "sha256", "byte_size"}
                or not isinstance(row.get("source_path"), str) or not isinstance(row.get("sha256"), str)
                or _SHA.fullmatch(row["sha256"]) is None or type(row.get("byte_size")) is not int
                or row["byte_size"] < 0):
            raise ArtifactPathError("snapshot_runtime_evidence_alias_invalid")
        runtime_evidence_source(row["source_path"], contract["writer_state_root"])
        expected = RUNTIME_EVIDENCE_DIRECTORY + "/" + row["sha256"] + ".json"
        target = required.get(expected)
        if (row["project_path"] != expected or target is None or row["source_path"] in aliases
                or any(target.get(key) != row[key] for key in ("sha256", "byte_size"))):
            raise ArtifactPathError("snapshot_runtime_evidence_alias_unbound")
        aliases[row["source_path"]] = row
    return aliases


class ArtifactPathError(ValueError):
    pass


@contextmanager
def using_artifact_root(root: Path) -> Iterator[None]:
    """Builder-only, context-local root for reading a detached SQLite backup.

The caller still validates every path/hash. This changes no process environment,
database or module globals and must never wrap a file-writing lifecycle action.
    """
    root = Path(root)
    if not root.is_absolute() or root != root.resolve(strict=True):
        raise ArtifactPathError("artifact_root_alias")
    token = _READ_ROOT.set(root)
    try:
        yield
    finally:
        _READ_ROOT.reset(token)


def _identity(path: Path) -> tuple[int, int, int, int, int]:
    info = path.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
            or info.st_uid not in {0, os.getuid()} or info.st_mode & 0o022
            or path != path.resolve(strict=True)):
        raise ArtifactPathError("snapshot_receipt_not_protected")
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def _object(
    path: Path, *, maximum_bytes: int, expected_sha256: str | None = None
) -> dict[str, Any]:
    before = _identity(path)
    if before[2] > maximum_bytes:
        raise ArtifactPathError("snapshot_receipt_oversized")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as stream:
        body = stream.read()
    if before != _identity(path) or (expected_sha256 is not None and hashlib.sha256(body).hexdigest() != expected_sha256):
        raise ArtifactPathError("snapshot_receipt_changed")
    value = json.loads(body)
    if not isinstance(value, dict):
        raise ArtifactPathError("snapshot_receipt_invalid")
    return value


def _project_path(value: Any) -> str:
    if not isinstance(value, str) or not value or any(char in value for char in ("\x00", "\n", "\r", "\\")):
        raise ArtifactPathError("snapshot_artifact_path_invalid")
    path = PurePosixPath(value)
    if (path.is_absolute() or any(part in {"", ".", ".."} for part in value.split("/"))
            or not (path.parts[:2] == ("data", "cache") and len(path.parts) > 2
                    or path.parts[:1] == ("reports",) and len(path.parts) > 1)):
        raise ArtifactPathError("snapshot_artifact_path_invalid")
    return path.as_posix()


@lru_cache(maxsize=4)
def _context(receipt_name: str, receipt_identity: tuple[int, int, int, int, int],
             manifest_name: str, manifest_identity: tuple[int, int, int, int, int]) -> dict[str, Any]:
    del receipt_identity, manifest_identity
    receipt = _object(
        Path(receipt_name), maximum_bytes=MAX_SNAPSHOT_RECEIPT_BYTES
    )
    digest = receipt.get("manifest_sha256")
    if not isinstance(digest, str) or _SHA.fullmatch(digest) is None:
        raise ArtifactPathError("snapshot_manifest_hash_missing")
    manifest = _object(
        Path(manifest_name),
        maximum_bytes=MAX_SNAPSHOT_MANIFEST_BYTES,
        expected_sha256=digest,
    )
    if (manifest.get("schema") != "dcar-read-replica-snapshot-v2"
            or manifest.get("snapshot_id") != receipt.get("snapshot_id")
            or manifest.get("artifact_policy") != ARTIFACT_POLICY
            or manifest.get("runtime_identity") != receipt.get("runtime_identity")):
        raise ArtifactPathError("snapshot_manifest_binding_mismatch")
    databases = {row["name"]: row["sha256"] for row in manifest.get("databases", [])}
    if not databases or databases != receipt.get("database_sha256"):
        raise ArtifactPathError("snapshot_database_binding_mismatch")
    validate_descriptor(manifest.get("snapshot_contract"))
    writer_root = Path(str(manifest.get("writer_project_root", "")))
    if (not writer_root.is_absolute() or ".." in writer_root.parts
            or len(writer_root.parts) < 3
            or receipt.get("writer_project_root") != str(writer_root)):
        raise ArtifactPathError("snapshot_writer_root_invalid")
    originals = manifest.get("managed_originals")
    if not isinstance(originals, dict) or originals.get("contract_version") != MANAGED_ORIGINALS_CONTRACT or not isinstance(originals.get("bundles"), list):
        raise ArtifactPathError("managed_originals_contract_missing")
    files: dict[str, dict[str, Any]] = {}
    members: dict[str, dict[str, Any]] = {}
    for row in [*manifest["files"], *manifest.get("optional_reuse_files", [])]:
        name = _project_path(row["project_path"])
        if (name in files or not isinstance(row.get("sha256"), str) or _SHA.fullmatch(row["sha256"]) is None
                or type(row.get("byte_size")) is not int or row["byte_size"] < 0):
            raise ArtifactPathError("snapshot_artifact_identity_invalid")
        files[name] = row
    for bundle in originals["bundles"]:
        for row in bundle["members"]:
            name = _project_path(row["project_path"])
            if name in files or name in members:
                raise ArtifactPathError("managed_original_was_transferred")
            members[name] = row
    directories = {str(parent) for name in files for parent in PurePosixPath(name).parents if str(parent) != "."}
    return {"receipt": receipt, "manifest": manifest, "writer_root": writer_root,
            "files": files, "members": members, "directories": directories,
            "runtime_evidence_aliases": runtime_evidence_aliases(manifest)}


def installed_snapshot() -> dict[str, Any] | None:
    if os.environ.get("DCAR_READ_ONLY", "0").strip() != "1" or _READ_ROOT.get() is not None:
        return None
    receipt = Path(os.environ.get("DCAR_ACTIVE_SNAPSHOT", str(ACTIVE_SNAPSHOT)))
    if not receipt.is_absolute():
        raise ArtifactPathError("snapshot_receipt_path_invalid")
    if not receipt.exists():
        if receipt.is_symlink():
            raise ArtifactPathError("snapshot_receipt_not_protected")
        return None
    receipt_identity = _identity(receipt)
    value = _object(receipt, maximum_bytes=MAX_SNAPSHOT_RECEIPT_BYTES)
    # Legacy v1 installations retain their existing read behavior. A claimed v2
    # installation, however, cannot fall back when its proof is incomplete.
    if (value.get("artifact_policy") or {}).get("name") != "thin-server-v2":
        return None
    snapshot_id = value.get("snapshot_id")
    if not isinstance(snapshot_id, str) or _SNAPSHOT.fullmatch(snapshot_id) is None:
        raise ArtifactPathError("snapshot_id_invalid")
    manifest = receipt.parent / "snapshot-history" / snapshot_id / "manifest.json"
    if value.get("manifest_path") != str(manifest):
        raise ArtifactPathError("snapshot_manifest_path_invalid")
    return _context(str(receipt), receipt_identity, str(manifest), _identity(manifest))


def resolve(value: str | Path, *, fallback_root: Path | None = None) -> Path:
    path = Path(value)
    root = _READ_ROOT.get() or fallback_root or PROJECT_ROOT
    context = installed_snapshot()
    if context is not None:
        alias = context["runtime_evidence_aliases"].get(str(path))
        if alias is not None:
            return root / alias["project_path"]
        relative: str | None = None
        if path.is_absolute():
            for prefix in (context["writer_root"], root):
                if path.is_relative_to(prefix):
                    relative = path.relative_to(prefix).as_posix()
                    break
        else:
            relative = path.as_posix()
        if relative is not None:
            name = _project_path(relative)
            if name not in context["files"] and name not in context["members"] and name not in context["directories"]:
                raise ArtifactPathError("snapshot_artifact_unlisted")
            return root / name
    return path if path.is_absolute() else root / path


def replica_file(path: Path, *, fallback_root: Path | None = None) -> dict[str, Any] | None:
    fallback_root = fallback_root or PROJECT_ROOT
    context = installed_snapshot()
    if context is None or not path.is_relative_to(fallback_root):
        return None
    return context["files"].get(path.relative_to(fallback_root).as_posix())


def replica_directory(path: Path, *, fallback_root: Path | None = None) -> bool:
    fallback_root = fallback_root or PROJECT_ROOT
    context = installed_snapshot()
    return bool(context is not None and path.is_relative_to(fallback_root)
                and path.relative_to(fallback_root).as_posix() in context["directories"])
