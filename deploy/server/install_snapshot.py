#!/usr/bin/env python3
"""Verify, atomically install, or roll back a Dcar read-replica snapshot."""

from __future__ import annotations

import argparse
import errno
import fcntl
import grp
import hashlib
import json
import os
import pwd
import re
import shutil
import sqlite3
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Iterator, Mapping, Optional, Sequence


BUNDLE_SCHEMA = "dcar-read-replica-snapshot-v2"
ARTIFACT_POLICY = {
    "name": "thin-server-v1",
    "included": "reports-and-small-text-evidence",
    "optional_reuse": "active-same-path-size-sha256-only",
    "on_optional_missing_or_mismatch": "omitted",
    "delete_unlisted": False,
}
DATABASE_NAMES = frozenset({"dcar_insight.sqlite3", "web_mvp.sqlite3"})
SNAPSHOT_ID_RE = re.compile(r"[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}")
SHA256_RE = re.compile(r"[0-9a-f]{64}")
ServiceAction = Callable[[str], None]
SmokeCheck = Callable[[], None]


class SnapshotInstallError(RuntimeError):
    """A snapshot operation could not be completed safely."""


@dataclass(frozen=True)
class InstallConfig:
    database_root: Path
    cache_root: Path
    reports_root: Path
    runtime_root: Path
    service: str = "dcar-api.service"
    health_url: str = "http://127.0.0.1:8765/api/v8/health"
    overview_url: str = "http://127.0.0.1:8765/api/v8/overview"
    scheduler_url: str = "http://127.0.0.1:8765/api/v8/scheduler"
    request_timeout_seconds: float = 15.0
    start_wait_seconds: float = 45.0
    owner_uid: int = field(default_factory=os.getuid)
    owner_gid: int = field(default_factory=os.getgid)

    @property
    def history_root(self) -> Path:
        return self.runtime_root / "snapshot-history"

    @property
    def lock_path(self) -> Path:
        return self.runtime_root / "snapshot-install.lock"

    @property
    def active_manifest_path(self) -> Path:
        return self.runtime_root / "active-snapshot.json"


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _fsync_file(path: Path) -> None:
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _ensure_private_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.is_symlink() or not path.is_dir():
        raise SnapshotInstallError(f"required directory is unsafe: {path}")


def _apply_owner(path: Path, config: InstallConfig) -> None:
    try:
        os.chown(path, config.owner_uid, config.owner_gid, follow_symlinks=False)
    except PermissionError as exc:
        if (config.owner_uid, config.owner_gid) != (os.getuid(), os.getgid()):
            raise SnapshotInstallError(
                f"cannot set replica ownership on {path}; run installer with sudo"
            ) from exc


def _ensure_managed_directory(path: Path, config: InstallConfig) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=0o750)
    if path.is_symlink() or not path.is_dir():
        raise SnapshotInstallError(f"managed directory is unsafe: {path}")
    os.chmod(path, 0o750, follow_symlinks=False)
    _apply_owner(path, config)


def _ensure_managed_parent(root: Path, target: Path, config: InstallConfig) -> None:
    _ensure_managed_directory(root, config)
    try:
        relative_parent = target.parent.resolve().relative_to(root.resolve())
    except ValueError as exc:
        raise SnapshotInstallError(f"managed target escapes its root: {target}") from exc
    current = root
    for part in relative_parent.parts:
        current = current / part
        _ensure_managed_directory(current, config)


def _apply_file_access(path: Path, config: InstallConfig) -> None:
    if path.is_symlink() or not path.is_file():
        raise SnapshotInstallError(f"managed file is unsafe: {path}")
    os.chmod(path, 0o640, follow_symlinks=False)
    _apply_owner(path, config)


@contextmanager
def _install_lock(config: InstallConfig) -> Iterator[None]:
    _ensure_private_directory(config.runtime_root)
    descriptor = os.open(config.lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _read_manifest(bundle: Path) -> dict[str, Any]:
    bundle = bundle.resolve()
    if bundle.is_symlink() or not bundle.is_dir():
        raise SnapshotInstallError(f"bundle is not a regular directory: {bundle}")
    manifest_path = bundle / "manifest.json"
    checksum_path = bundle / "manifest.sha256"
    if (
        manifest_path.is_symlink()
        or checksum_path.is_symlink()
        or not manifest_path.is_file()
        or not checksum_path.is_file()
    ):
        raise SnapshotInstallError("bundle manifest files are missing or unsafe")
    checksum_fields = checksum_path.read_text(encoding="ascii").strip().split()
    if len(checksum_fields) != 2 or checksum_fields[1] != "manifest.json":
        raise SnapshotInstallError("manifest.sha256 has an invalid shape")
    expected_manifest_sha = checksum_fields[0]
    if not SHA256_RE.fullmatch(expected_manifest_sha):
        raise SnapshotInstallError("manifest.sha256 does not contain a SHA-256")
    if _sha256(manifest_path) != expected_manifest_sha:
        raise SnapshotInstallError("manifest SHA-256 mismatch")
    try:
        value = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SnapshotInstallError("manifest JSON is invalid") from exc
    if not isinstance(value, dict) or value.get("schema") != BUNDLE_SCHEMA:
        raise SnapshotInstallError("unsupported snapshot bundle schema")
    snapshot_id = value.get("snapshot_id")
    if not isinstance(snapshot_id, str) or not SNAPSHOT_ID_RE.fullmatch(snapshot_id):
        raise SnapshotInstallError("snapshot_id is invalid")
    return value


def _bundle_member(bundle: Path, value: str) -> Path:
    if not value or "\x00" in value or "\\" in value:
        raise SnapshotInstallError("bundle path is unsafe")
    relative = PurePosixPath(value)
    if relative.is_absolute() or any(
        part in {"", ".", ".."} for part in relative.parts
    ):
        raise SnapshotInstallError(f"bundle path is unsafe: {value}")
    candidate = (bundle / Path(*relative.parts)).resolve()
    root = bundle.resolve()
    if root not in candidate.parents:
        raise SnapshotInstallError(f"bundle path escapes its root: {value}")
    return candidate


def _validate_sqlite(
    path: Path, *, expected_user_version: Optional[int]
) -> dict[str, Any]:
    connection = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    try:
        quick_rows = [
            str(row[0]) for row in connection.execute("PRAGMA quick_check").fetchall()
        ]
        if quick_rows != ["ok"]:
            raise SnapshotInstallError(
                f"SQLite quick_check failed for {path.name}: {quick_rows[:5]}"
            )
        foreign_key_rows = connection.execute("PRAGMA foreign_key_check").fetchall()
        if foreign_key_rows:
            raise SnapshotInstallError(
                f"SQLite foreign_key_check failed for {path.name}: "
                f"{len(foreign_key_rows)} violation(s)"
            )
        user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    finally:
        connection.close()
    if expected_user_version is not None and user_version != expected_user_version:
        raise SnapshotInstallError(
            f"SQLite schema mismatch for {path.name}: {user_version}, "
            f"expected {expected_user_version}"
        )
    return {
        "quick_check": "ok",
        "foreign_key_violations": 0,
        "user_version": user_version,
    }


def _artifact_target(config: InstallConfig, item: Mapping[str, Any]) -> Path:
    root_name = item.get("root")
    relative_value = item.get("path")
    if root_name not in {"cache", "reports"} or not isinstance(relative_value, str):
        raise SnapshotInstallError("artifact manifest entry is invalid")
    relative = PurePosixPath(relative_value)
    if relative.is_absolute() or any(
        part in {"", ".", ".."} for part in relative.parts
    ):
        raise SnapshotInstallError(f"artifact path is unsafe: {relative_value}")
    root = config.cache_root if root_name == "cache" else config.reports_root
    candidate = (root / Path(*relative.parts)).resolve()
    resolved_root = root.resolve()
    if resolved_root not in candidate.parents:
        raise SnapshotInstallError(f"artifact path escapes its root: {relative_value}")
    return candidate


def _staged_artifact_target(bundle: Path, item: Mapping[str, Any]) -> Path:
    root_name = item.get("root")
    relative_value = item.get("path")
    if root_name not in {"cache", "reports"} or not isinstance(relative_value, str):
        raise SnapshotInstallError("artifact manifest entry is invalid")
    relative = PurePosixPath(relative_value)
    if relative.is_absolute() or any(
        part in {"", ".", ".."} for part in relative.parts
    ):
        raise SnapshotInstallError(f"artifact path is unsafe: {relative_value}")
    staged_root = bundle.parent / "artifacts" / str(root_name)
    candidate = (staged_root / Path(*relative.parts)).resolve()
    resolved_root = staged_root.resolve()
    if resolved_root not in candidate.parents:
        raise SnapshotInstallError(
            f"staged artifact path escapes its root: {relative_value}"
        )
    return candidate


def verify_bundle(
    bundle: Path, config: InstallConfig, *, verify_artifacts: bool = True
) -> dict[str, Any]:
    bundle = bundle.resolve()
    manifest = _read_manifest(bundle)
    if manifest.get("artifact_policy") != ARTIFACT_POLICY:
        raise SnapshotInstallError("snapshot artifact policy is missing or unsupported")
    raw_databases = manifest.get("databases")
    if not isinstance(raw_databases, list) or not raw_databases:
        raise SnapshotInstallError("snapshot contains no databases")
    seen_names: set[str] = set()
    for item in raw_databases:
        if not isinstance(item, dict):
            raise SnapshotInstallError("database manifest entry is invalid")
        name = item.get("name")
        bundle_path = item.get("bundle_path")
        expected_sha = item.get("sha256")
        expected_size = item.get("byte_size")
        expected_user_version = item.get("user_version")
        if (
            name not in DATABASE_NAMES
            or name in seen_names
            or not isinstance(bundle_path, str)
            or not isinstance(expected_sha, str)
            or not SHA256_RE.fullmatch(expected_sha)
            or not isinstance(expected_size, int)
            or expected_size <= 0
            or not isinstance(expected_user_version, int)
        ):
            raise SnapshotInstallError("database manifest entry is invalid")
        seen_names.add(name)
        source = _bundle_member(bundle, bundle_path)
        if source.is_symlink() or not source.is_file():
            raise SnapshotInstallError(f"database payload is missing or unsafe: {name}")
        if source.stat().st_size != expected_size or _sha256(source) != expected_sha:
            raise SnapshotInstallError(f"database payload drifted: {name}")
        validation = _validate_sqlite(
            source, expected_user_version=expected_user_version
        )
        if (
            item.get("quick_check") != validation["quick_check"]
            or item.get("foreign_key_violations")
            != validation["foreign_key_violations"]
        ):
            raise SnapshotInstallError(f"database validation manifest drifted: {name}")
    if "dcar_insight.sqlite3" not in seen_names:
        raise SnapshotInstallError("snapshot omits dcar_insight.sqlite3")
    raw_files = manifest.get("files")
    if not isinstance(raw_files, list):
        raise SnapshotInstallError("artifact manifest is invalid")
    if manifest.get("file_count") != len(raw_files):
        raise SnapshotInstallError("artifact count does not match manifest")
    seen_files: set[tuple[str, str]] = set()
    byte_total = 0
    file_set_digest = hashlib.sha256()
    for item in raw_files:
        if not isinstance(item, dict):
            raise SnapshotInstallError("artifact manifest entry is invalid")
        root_name = item.get("root")
        relative_path = item.get("path")
        expected_sha = item.get("sha256")
        expected_size = item.get("byte_size")
        if (
            root_name not in {"cache", "reports"}
            or not isinstance(relative_path, str)
            or not isinstance(expected_sha, str)
            or not SHA256_RE.fullmatch(expected_sha)
            or not isinstance(expected_size, int)
            or expected_size < 0
        ):
            raise SnapshotInstallError("artifact manifest entry is invalid")
        identity = (root_name, relative_path)
        if identity in seen_files:
            raise SnapshotInstallError(f"duplicate artifact entry: {relative_path}")
        seen_files.add(identity)
        byte_total += expected_size
        file_set_digest.update(
            (f"{root_name}\0{relative_path}\0{expected_size}\0{expected_sha}\n").encode(
                "utf-8"
            )
        )
        target = _staged_artifact_target(bundle, item)
        if verify_artifacts:
            if target.is_symlink() or not target.is_file():
                raise SnapshotInstallError(f"artifact is missing or unsafe: {target}")
            if (
                target.stat().st_size != expected_size
                or _sha256(target) != expected_sha
            ):
                raise SnapshotInstallError(f"artifact drifted: {target}")
    if manifest.get("file_byte_size") != byte_total:
        raise SnapshotInstallError("artifact byte total does not match manifest")
    if manifest.get("file_set_sha256") != file_set_digest.hexdigest():
        raise SnapshotInstallError("artifact set SHA-256 does not match manifest")
    raw_optional = manifest.get("optional_reuse_files")
    if not isinstance(raw_optional, list):
        raise SnapshotInstallError("optional-reuse artifact manifest is invalid")
    if manifest.get("optional_reuse_file_count") != len(raw_optional):
        raise SnapshotInstallError(
            "optional-reuse artifact count does not match manifest"
        )
    seen_optional: set[tuple[str, str]] = set()
    optional_byte_total = 0
    optional_set_digest = hashlib.sha256()
    for item in raw_optional:
        if not isinstance(item, dict):
            raise SnapshotInstallError("optional-reuse artifact entry is invalid")
        root_name = item.get("root")
        relative_path = item.get("path")
        expected_sha = item.get("sha256")
        expected_size = item.get("byte_size")
        reason = item.get("reason")
        if (
            root_name not in {"cache", "reports"}
            or not isinstance(relative_path, str)
            or not isinstance(expected_sha, str)
            or not SHA256_RE.fullmatch(expected_sha)
            or not isinstance(expected_size, int)
            or expected_size < 0
            or reason not in {"large_binary", "source_missing"}
        ):
            raise SnapshotInstallError("optional-reuse artifact entry is invalid")
        identity = (str(root_name), relative_path)
        if identity in seen_optional or identity in seen_files:
            raise SnapshotInstallError(
                f"duplicate optional-reuse artifact entry: {relative_path}"
            )
        seen_optional.add(identity)
        optional_byte_total += expected_size
        optional_set_digest.update(
            (f"{root_name}\0{relative_path}\0{expected_size}\0{expected_sha}\n").encode(
                "utf-8"
            )
        )
        # This only validates path safety. Optional-reuse files are deliberately
        # absent from staging and are never required for installation.
        _artifact_target(config, item)
    if manifest.get("optional_reuse_byte_size") != optional_byte_total:
        raise SnapshotInstallError(
            "optional-reuse artifact byte total does not match manifest"
        )
    if manifest.get("optional_reuse_set_sha256") != optional_set_digest.hexdigest():
        raise SnapshotInstallError(
            "optional-reuse artifact set SHA-256 does not match manifest"
        )
    return manifest


def _default_service_action(service: str) -> ServiceAction:
    def action(verb: str) -> None:
        if verb not in {"start", "stop"}:
            raise SnapshotInstallError(f"unsupported service action: {verb}")
        completed = subprocess.run(
            ["systemctl", verb, service],
            check=False,
            capture_output=True,
            text=True,
            timeout=90,
        )
        if completed.returncode != 0:
            detail = completed.stderr.strip()[-1000:]
            raise SnapshotInstallError(f"systemctl {verb} {service} failed: {detail}")

    return action


def _read_json_url(url: str, timeout: float) -> dict[str, Any]:
    request = urllib.request.Request(
        url, headers={"Accept": "application/json", "User-Agent": "DcarSnapshot/1"}
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            if int(response.status) != 200:
                raise SnapshotInstallError(
                    f"smoke endpoint returned {response.status}: {url}"
                )
            value = json.loads(response.read().decode("utf-8"))
    except (
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        urllib.error.URLError,
    ) as exc:
        raise SnapshotInstallError(f"smoke endpoint failed: {url}") from exc
    if not isinstance(value, dict):
        raise SnapshotInstallError(f"smoke endpoint returned non-object JSON: {url}")
    return value


def _default_smoke_check(
    config: InstallConfig, manifest: Optional[Mapping[str, Any]] = None
) -> SmokeCheck:
    expected_freshness = manifest.get("freshness", {}) if manifest else {}
    expected_main = (
        next(
            (
                item
                for item in manifest.get("databases", [])
                if item.get("name") == "dcar_insight.sqlite3"
            ),
            None,
        )
        if manifest
        else None
    )

    def check() -> None:
        deadline = time.monotonic() + config.start_wait_seconds
        last_error: Optional[Exception] = None
        while time.monotonic() < deadline:
            try:
                health = _read_json_url(
                    config.health_url, config.request_timeout_seconds
                )
                if health.get("status") != "ok":
                    raise SnapshotInstallError("health endpoint is not ok")
                database_state = health.get("database_state")
                if not isinstance(database_state, dict):
                    raise SnapshotInstallError("health omitted database identity")
                overview = _read_json_url(
                    config.overview_url, config.request_timeout_seconds
                )
                if (
                    overview.get("status") != "ready"
                    or not isinstance(overview.get("windows"), dict)
                    or not isinstance(overview.get("data_freshness"), dict)
                ):
                    raise SnapshotInstallError(
                        "overview did not read the active database"
                    )
                scheduler = _read_json_url(
                    config.scheduler_url, config.request_timeout_seconds
                )
                catchup = scheduler.get("startup_catchup")
                if (
                    health.get("read_only") is not True
                    or scheduler.get("read_only") is not True
                ):
                    raise SnapshotInstallError("replica API is not in read-only mode")
                if scheduler.get("requested") or scheduler.get("enabled"):
                    raise SnapshotInstallError(
                        "replica scheduler is unexpectedly enabled"
                    )
                if not isinstance(catchup, dict) or catchup.get("requested"):
                    raise SnapshotInstallError(
                        "replica startup catch-up is unexpectedly enabled"
                    )
                if expected_main is not None:
                    if database_state.get("sha256") != expected_main.get("sha256"):
                        raise SnapshotInstallError(
                            "replica database SHA-256 is not the staged snapshot"
                        )
                    if database_state.get("user_version") != expected_main.get(
                        "user_version"
                    ):
                        raise SnapshotInstallError(
                            "replica schema is not the staged snapshot"
                        )
                    if database_state.get("content_count") != expected_freshness.get(
                        "content_count"
                    ):
                        raise SnapshotInstallError(
                            "replica content count is not the staged snapshot"
                        )
                    if database_state.get(
                        "latest_published_at"
                    ) != expected_freshness.get("latest_published_at"):
                        raise SnapshotInstallError(
                            "replica freshness is not the staged snapshot"
                        )
                return
            except Exception as exc:  # retry during a bounded service startup window
                last_error = exc
                time.sleep(1.0)
        raise SnapshotInstallError(f"post-install smoke check failed: {last_error}")

    return check


def _checkpoint_database(path: Path) -> None:
    if not path.exists():
        return
    if path.is_symlink() or not path.is_file():
        raise SnapshotInstallError(f"active database is unsafe: {path}")
    connection = sqlite3.connect(path)
    try:
        row = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    finally:
        connection.close()
    if row is not None and int(row[0]) != 0:
        raise SnapshotInstallError(f"WAL checkpoint remained busy for {path.name}")


def _copy_file_durable(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with source.open("rb") as reader, target.open("xb") as writer:
        shutil.copyfileobj(reader, writer, length=1024 * 1024)
        writer.flush()
        os.fsync(writer.fileno())
    shutil.copystat(source, target, follow_symlinks=False)


def _backup_active_databases(config: InstallConfig, destination: Path) -> list[str]:
    _ensure_private_directory(destination)
    names: list[str] = []
    for name in sorted(DATABASE_NAMES):
        active = config.database_root / name
        if not active.exists():
            continue
        _checkpoint_database(active)
        _copy_file_durable(active, destination / name)
        names.append(name)
        for suffix in ("-wal", "-shm"):
            sidecar = active.with_name(active.name + suffix)
            if sidecar.exists():
                if sidecar.is_symlink() or not sidecar.is_file():
                    raise SnapshotInstallError(f"database sidecar is unsafe: {sidecar}")
                os.replace(sidecar, destination / sidecar.name)
    _fsync_directory(destination)
    return names


def _database_payloads(bundle: Path, manifest: Mapping[str, Any]) -> dict[str, Path]:
    payloads: dict[str, Path] = {}
    for item in manifest["databases"]:
        payloads[str(item["name"])] = _bundle_member(bundle, str(item["bundle_path"]))
    return payloads


def _atomic_replace_database(config: InstallConfig, name: str, source: Path) -> None:
    if name not in DATABASE_NAMES:
        raise SnapshotInstallError(f"unsupported active database name: {name}")
    _ensure_managed_directory(config.database_root, config)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{name}.incoming-", dir=config.database_root
    )
    temporary = Path(temporary_name)
    try:
        with source.open("rb") as reader, os.fdopen(descriptor, "wb") as writer:
            shutil.copyfileobj(reader, writer, length=1024 * 1024)
            writer.flush()
            os.fsync(writer.fileno())
        _apply_file_access(temporary, config)
        os.replace(temporary, config.database_root / name)
        _fsync_directory(config.database_root)
    finally:
        if temporary.exists():
            temporary.unlink()


def _restore_databases(
    config: InstallConfig,
    backup_dir: Path,
    names: Sequence[str],
    *,
    remove_absent: Sequence[str] = (),
) -> None:
    for name in names:
        source = backup_dir / name
        if not source.is_file() or source.is_symlink():
            raise SnapshotInstallError(
                f"rollback database is missing or unsafe: {source}"
            )
        _atomic_replace_database(config, name, source)
    for name in remove_absent:
        if name not in names:
            target = config.database_root / name
            if target.exists():
                quarantine = backup_dir / f"removed-{name}"
                os.replace(target, quarantine)
    _fsync_directory(config.database_root)


def _artifact_backup_target(backup_dir: Path, item: Mapping[str, Any]) -> Path:
    root_name = item.get("root")
    relative_value = item.get("path")
    if root_name not in {"cache", "reports"} or not isinstance(relative_value, str):
        raise SnapshotInstallError("artifact change entry is invalid")
    relative = PurePosixPath(relative_value)
    if relative.is_absolute() or any(
        part in {"", ".", ".."} for part in relative.parts
    ):
        raise SnapshotInstallError(f"artifact change path is unsafe: {relative_value}")
    root = backup_dir / "artifacts" / str(root_name)
    candidate = (root / Path(*relative.parts)).resolve()
    if root.resolve() not in candidate.parents:
        raise SnapshotInstallError(
            f"artifact backup path escapes its root: {relative_value}"
        )
    return candidate


def _atomic_activate_artifact(
    source: Path, target: Path, *, root: Path, config: InstallConfig
) -> None:
    if source.is_symlink() or not source.is_file():
        raise SnapshotInstallError(f"staged artifact is missing or unsafe: {source}")
    if target.exists() and (target.is_symlink() or not target.is_file()):
        raise SnapshotInstallError(f"active artifact is unsafe: {target}")
    _ensure_managed_parent(root, target, config)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.incoming-", dir=target.parent
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    temporary.unlink()
    try:
        try:
            os.link(source, temporary, follow_symlinks=False)
        except OSError as exc:
            if exc.errno not in {errno.EXDEV, errno.EPERM, errno.EACCES}:
                raise
            _copy_file_durable(source, temporary)
        _fsync_file(temporary)
        _apply_file_access(temporary, config)
        os.replace(temporary, target)
        _fsync_directory(target.parent)
    finally:
        if temporary.exists():
            temporary.unlink()


def _artifact_change(item: Mapping[str, Any], *, had_previous: bool) -> dict[str, Any]:
    return {
        "root": str(item["root"]),
        "path": str(item["path"]),
        "had_previous": had_previous,
    }


def _install_artifacts(
    bundle: Path,
    manifest: Mapping[str, Any],
    config: InstallConfig,
    backup_dir: Path,
) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    for item in manifest["files"]:
        target = _artifact_target(config, item)
        expected_size = int(item["byte_size"])
        expected_sha = str(item["sha256"])
        if target.exists():
            if target.is_symlink() or not target.is_file():
                raise SnapshotInstallError(f"active artifact is unsafe: {target}")
            if (
                target.stat().st_size == expected_size
                and _sha256(target) == expected_sha
            ):
                root = (
                    config.cache_root
                    if item["root"] == "cache"
                    else config.reports_root
                )
                _ensure_managed_parent(root, target, config)
                _apply_file_access(target, config)
                continue
            backup = _artifact_backup_target(backup_dir, item)
            _copy_file_durable(target, backup)
            changes.append(_artifact_change(item, had_previous=True))
        else:
            changes.append(_artifact_change(item, had_previous=False))
    _write_json_atomic(backup_dir / "artifact-changes.json", {"changes": changes})
    try:
        for change in changes:
            source = _staged_artifact_target(bundle, change)
            target = _artifact_target(config, change)
            root = config.cache_root if change["root"] == "cache" else config.reports_root
            _atomic_activate_artifact(source, target, root=root, config=config)
    except Exception:
        _restore_artifacts(config, backup_dir, changes)
        raise
    return changes


def _optional_reuse_summary(
    manifest: Mapping[str, Any], config: InstallConfig
) -> dict[str, int]:
    reused_count = 0
    reused_bytes = 0
    omitted_count = 0
    omitted_bytes = 0
    for item in manifest["optional_reuse_files"]:
        target = _artifact_target(config, item)
        expected_size = int(item["byte_size"])
        expected_sha = str(item["sha256"])
        reusable = (
            target.is_file()
            and not target.is_symlink()
            and target.stat().st_size == expected_size
            and _sha256(target) == expected_sha
        )
        if reusable:
            reused_count += 1
            reused_bytes += expected_size
        else:
            omitted_count += 1
            omitted_bytes += expected_size
    return {
        "reused_count": reused_count,
        "reused_bytes": reused_bytes,
        "omitted_count": omitted_count,
        "omitted_bytes": omitted_bytes,
    }


def _restore_artifacts(
    config: InstallConfig,
    backup_dir: Path,
    changes: Sequence[Mapping[str, Any]],
) -> None:
    for change in changes:
        target = _artifact_target(config, change)
        if change.get("had_previous") is True:
            backup = _artifact_backup_target(backup_dir, change)
            if backup.is_symlink() or not backup.is_file():
                raise SnapshotInstallError(
                    f"rollback artifact is missing or unsafe: {backup}"
                )
            root = config.cache_root if change["root"] == "cache" else config.reports_root
            _atomic_activate_artifact(backup, target, root=root, config=config)
        elif change.get("had_previous") is False:
            if target.exists():
                if target.is_symlink() or not target.is_file():
                    raise SnapshotInstallError(
                        f"active rollback artifact is unsafe: {target}"
                    )
                target.unlink()
                _fsync_directory(target.parent)
        else:
            raise SnapshotInstallError("artifact change entry has invalid state")


def _read_artifact_changes(backup_dir: Path) -> list[dict[str, Any]]:
    path = backup_dir / "artifact-changes.json"
    if not path.exists():
        return []
    if path.is_symlink() or not path.is_file():
        raise SnapshotInstallError(f"artifact change ledger is unsafe: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SnapshotInstallError(
            f"artifact change ledger is invalid: {path}"
        ) from exc
    changes = value.get("changes") if isinstance(value, dict) else None
    if not isinstance(changes, list) or not all(
        isinstance(item, dict) for item in changes
    ):
        raise SnapshotInstallError(f"artifact change ledger is invalid: {path}")
    return changes


def _backup_artifact_targets(
    config: InstallConfig,
    destination: Path,
    identities: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    for item in identities:
        target = _artifact_target(config, item)
        if target.exists():
            if target.is_symlink() or not target.is_file():
                raise SnapshotInstallError(f"active artifact is unsafe: {target}")
            _copy_file_durable(target, _artifact_backup_target(destination, item))
            changes.append(_artifact_change(item, had_previous=True))
        else:
            changes.append(_artifact_change(item, had_previous=False))
    _write_json_atomic(destination / "artifact-changes.json", {"changes": changes})
    return changes


def _write_json_atomic(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        payload = (
            json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if temporary.exists():
            temporary.unlink()


def install_bundle(
    bundle: Path,
    config: InstallConfig,
    *,
    service_action: Optional[ServiceAction] = None,
    smoke_check: Optional[SmokeCheck] = None,
) -> dict[str, Any]:
    bundle = bundle.resolve()
    service_action = service_action or _default_service_action(config.service)
    with _install_lock(config):
        manifest = verify_bundle(bundle, config, verify_artifacts=True)
        optional_reuse = _optional_reuse_summary(manifest, config)
        install_smoke_check = smoke_check or _default_smoke_check(config, manifest)
        rollback_smoke_check = smoke_check or _default_smoke_check(config)
        snapshot_id = str(manifest["snapshot_id"])
        history_dir = config.history_root / snapshot_id
        if history_dir.exists():
            raise SnapshotInstallError(f"snapshot was already installed: {snapshot_id}")
        _ensure_private_directory(config.history_root)
        payloads = _database_payloads(bundle, manifest)
        service_stopped = False
        backup_completed = False
        old_names: list[str] = []
        artifact_changes: list[dict[str, Any]] = []
        try:
            service_action("stop")
            service_stopped = True
            _ensure_private_directory(history_dir)
            old_names = _backup_active_databases(config, history_dir)
            backup_completed = True
            artifact_changes = _install_artifacts(
                bundle,
                manifest,
                config,
                history_dir,
            )
            for name, source in sorted(payloads.items()):
                _atomic_replace_database(config, name, source)
            service_action("start")
            service_stopped = False
            install_smoke_check()
        except Exception as install_error:
            rollback_error: Optional[Exception] = None
            try:
                if backup_completed:
                    if not service_stopped:
                        service_action("stop")
                    _restore_databases(
                        config,
                        history_dir,
                        old_names,
                        remove_absent=tuple(payloads),
                    )
                    _restore_artifacts(config, history_dir, artifact_changes)
                    service_action("start")
                    rollback_smoke_check()
                elif service_stopped:
                    service_action("start")
                    rollback_smoke_check()
            except Exception as exc:
                rollback_error = exc
            if rollback_error is not None:
                raise SnapshotInstallError(
                    f"install failed ({install_error}); automatic rollback also failed "
                    f"({rollback_error})"
                ) from install_error
            raise SnapshotInstallError(
                f"install failed and the previous databases were restored: {install_error}"
            ) from install_error
        receipt = {
            "schema": "dcar-read-replica-install-receipt-v1",
            "snapshot_id": snapshot_id,
            "installed_at": _utc_now(),
            "database_sha256": {
                str(item["name"]): str(item["sha256"]) for item in manifest["databases"]
            },
            "previous_databases": old_names,
            "artifact_changes": len(artifact_changes),
            "artifact_policy": manifest["artifact_policy"],
            "included_artifact_count": int(manifest["file_count"]),
            "included_artifact_bytes": int(manifest["file_byte_size"]),
            "optional_reuse": optional_reuse,
        }
        _write_json_atomic(history_dir / "install-receipt.json", receipt)
        _write_json_atomic(config.active_manifest_path, receipt)
        return receipt


def _rollback_candidates(
    config: InstallConfig, *, require_install_receipt: bool
) -> list[Path]:
    if not config.history_root.is_dir():
        return []
    return sorted(
        (
            path
            for path in config.history_root.iterdir()
            if path.is_dir()
            and not path.is_symlink()
            and SNAPSHOT_ID_RE.fullmatch(path.name)
            and (
                not require_install_receipt or (path / "install-receipt.json").is_file()
            )
        ),
        key=lambda path: path.name,
        reverse=True,
    )


def rollback_snapshot(
    config: InstallConfig,
    *,
    snapshot_id: Optional[str] = None,
    service_action: Optional[ServiceAction] = None,
    smoke_check: Optional[SmokeCheck] = None,
) -> dict[str, Any]:
    service_action = service_action or _default_service_action(config.service)
    smoke_check = smoke_check or _default_smoke_check(config)
    with _install_lock(config):
        if snapshot_id is not None and not SNAPSHOT_ID_RE.fullmatch(snapshot_id):
            raise SnapshotInstallError("rollback snapshot_id is invalid")
        candidates = _rollback_candidates(
            config,
            require_install_receipt=snapshot_id is None,
        )
        source = next(
            (
                path
                for path in candidates
                if snapshot_id is None or path.name == snapshot_id
            ),
            None,
        )
        if source is None:
            raise SnapshotInstallError("no matching rollback snapshot exists")
        names = [name for name in sorted(DATABASE_NAMES) if (source / name).is_file()]
        if not names:
            raise SnapshotInstallError(
                f"rollback snapshot contains no databases: {source}"
            )
        source_artifact_changes = _read_artifact_changes(source)
        rollback_id = "rollback-" + datetime.now(timezone.utc).strftime(
            "%Y%m%dT%H%M%SZ"
        )
        safety_backup = config.history_root / rollback_id
        _ensure_private_directory(safety_backup)
        service_stopped = False
        backup_completed = False
        current_names: list[str] = []
        current_artifact_changes: list[dict[str, Any]] = []
        try:
            service_action("stop")
            service_stopped = True
            current_names = _backup_active_databases(config, safety_backup)
            backup_completed = True
            current_artifact_changes = _backup_artifact_targets(
                config,
                safety_backup,
                source_artifact_changes,
            )
            _restore_databases(
                config,
                source,
                names,
                remove_absent=tuple(sorted(DATABASE_NAMES)),
            )
            _restore_artifacts(config, source, source_artifact_changes)
            service_action("start")
            service_stopped = False
            smoke_check()
        except Exception as rollback_error:
            recovery_error: Optional[Exception] = None
            try:
                if backup_completed:
                    if not service_stopped:
                        service_action("stop")
                    _restore_databases(
                        config,
                        safety_backup,
                        current_names,
                        remove_absent=tuple(sorted(DATABASE_NAMES)),
                    )
                    _restore_artifacts(
                        config,
                        safety_backup,
                        current_artifact_changes,
                    )
                    service_action("start")
                    smoke_check()
                elif service_stopped:
                    service_action("start")
                    smoke_check()
            except Exception as exc:
                recovery_error = exc
            if recovery_error is not None:
                raise SnapshotInstallError(
                    f"rollback failed ({rollback_error}); current-state recovery also failed "
                    f"({recovery_error})"
                ) from rollback_error
            raise SnapshotInstallError(
                f"rollback failed and the current databases were restored: {rollback_error}"
            ) from rollback_error
        receipt = {
            "schema": "dcar-read-replica-rollback-receipt-v1",
            "rolled_back_at": _utc_now(),
            "restored_from_snapshot": source.name,
            "safety_backup": safety_backup.name,
            "database_names": names,
        }
        _write_json_atomic(safety_backup / "rollback-receipt.json", receipt)
        _write_json_atomic(config.active_manifest_path, receipt)
        return receipt


def _common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--database-root", type=Path, default=Path("/var/lib/dcar-aigc/db")
    )
    parser.add_argument(
        "--cache-root", type=Path, default=Path("/var/lib/dcar-aigc/cache")
    )
    parser.add_argument(
        "--reports-root", type=Path, default=Path("/var/lib/dcar-aigc/reports")
    )
    parser.add_argument(
        "--runtime-root", type=Path, default=Path("/var/lib/dcar-aigc/runtime")
    )
    parser.add_argument("--service", default="dcar-api.service")
    parser.add_argument("--health-url", default="http://127.0.0.1:8765/api/v8/health")
    parser.add_argument(
        "--overview-url", default="http://127.0.0.1:8765/api/v8/overview"
    )
    parser.add_argument(
        "--scheduler-url", default="http://127.0.0.1:8765/api/v8/scheduler"
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    verify = commands.add_parser("verify", help="Verify a staged bundle and artifacts.")
    verify.add_argument("--bundle", type=Path, required=True)
    _common_arguments(verify)
    install = commands.add_parser("install", help="Install a staged bundle atomically.")
    install.add_argument("--bundle", type=Path, required=True)
    _common_arguments(install)
    rollback = commands.add_parser(
        "rollback", help="Restore a prior active database set."
    )
    rollback.add_argument("--snapshot-id")
    _common_arguments(rollback)
    return parser


def _config(arguments: argparse.Namespace) -> InstallConfig:
    try:
        owner_uid = pwd.getpwnam("root").pw_uid
        owner_gid = grp.getgrnam("dcar-aigc").gr_gid
    except KeyError as exc:
        raise SnapshotInstallError(
            "required replica account/group is missing: root:dcar-aigc"
        ) from exc
    return InstallConfig(
        database_root=arguments.database_root.resolve(),
        cache_root=arguments.cache_root.resolve(),
        reports_root=arguments.reports_root.resolve(),
        runtime_root=arguments.runtime_root.resolve(),
        service=arguments.service,
        health_url=arguments.health_url,
        overview_url=arguments.overview_url,
        scheduler_url=arguments.scheduler_url,
        owner_uid=owner_uid,
        owner_gid=owner_gid,
    )


def main() -> int:
    arguments = _parser().parse_args()
    try:
        config = _config(arguments)
        if arguments.command == "verify":
            manifest = verify_bundle(arguments.bundle, config, verify_artifacts=True)
            result: Mapping[str, Any] = {
                "status": "verified",
                "snapshot_id": manifest["snapshot_id"],
                "database_names": [item["name"] for item in manifest["databases"]],
                "file_count": manifest["file_count"],
            }
        elif arguments.command == "install":
            result = install_bundle(arguments.bundle, config)
        else:
            result = rollback_snapshot(config, snapshot_id=arguments.snapshot_id)
    except SnapshotInstallError as exc:
        raise SystemExit(f"snapshot operation refused: {exc}") from exc
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
