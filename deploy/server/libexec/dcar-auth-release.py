#!/usr/bin/env python3
"""Deploy or roll back an auth-overlay candidate under the snapshot lock.

The candidate is prepared out of band (normally a physical copy of the current
production release with reviewed auth/Web files overlaid). This helper never
copies a repository tree. It proves candidate smoke first, then serializes
stop/migrate/symlink-switch/start with the snapshot installer.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import importlib.machinery
import importlib.util
import json
import os
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable, Iterator, NamedTuple, Sequence


SERVICES = (
    "dcar-auth.service",
    "dcar-douyin-control.service",
    "dcar-web.service",
    "dcar-api.service",
)
BACKUP_TIMER = "dcar-auth-backup.timer"
BACKUP_SERVICE = "dcar-auth-backup.service"
RECEIPT_SCHEMA = "dcar-auth-release-v1"
AUTH_SCHEMA_VERSION = 3
MIB = 1024 * 1024
MIN_FREE_BYTES = 2 * 1024 * MIB
MIN_FREE_PERCENT = 3


class ReleaseError(RuntimeError):
    pass


class ReleaseConfig(NamedTuple):
    candidate_release: Path
    current_link: Path
    releases_root: Path
    runtime_root: Path
    auth_database: Path
    backup_dir: Path
    htpasswd: Path
    change_log: Path
    receipt: Path
    superadmin: str | None
    run_user: str
    systemctl: Path
    pre_smoke_urls: tuple[str, ...]
    post_smoke_urls: tuple[str, ...]
    systemd_dir: Path = Path("/etc/systemd/system")
    libexec_dir: Path = Path("/usr/local/libexec")

    @property
    def lock_path(self) -> Path:
        return self.runtime_root / "snapshot-install.lock"


def _backup_module() -> Any:
    helper = Path(__file__).with_name("dcar-auth-backup.py")
    if not helper.is_file():
        helper = Path(__file__).with_name("dcar-auth-backup")
    specification = importlib.util.spec_from_file_location(
        "dcar_auth_backup_for_release",
        helper,
        loader=importlib.machinery.SourceFileLoader(
            "dcar_auth_backup_for_release", str(helper)
        ),
    )
    if specification is None or specification.loader is None:
        raise ReleaseError("cannot load auth backup helper")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def _assert_directory(path: Path, label: str) -> Path:
    if path.is_symlink() or not path.is_dir():
        raise ReleaseError(f"{label} is not a safe directory")
    return path.resolve()


def _assert_regular(path: Path, label: str) -> os.stat_result:
    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise ReleaseError(f"{label} does not exist") from exc
    if path.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise ReleaseError(f"{label} is not a safe regular file")
    return info


def _assert_executable(path: Path, label: str) -> None:
    if not path.is_file() or not os.access(path, os.X_OK):
        raise ReleaseError(f"{label} is not executable")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        for chunk in iter(lambda: os.read(descriptor, 1024 * 1024), b""):
            digest.update(chunk)
    finally:
        os.close(descriptor)
    return digest.hexdigest()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise OSError("short write")
        view = view[written:]


def _atomic_json(path: Path, payload: dict[str, object]) -> None:
    if not path.parent.is_dir() or path.parent.is_symlink():
        raise ReleaseError("release receipt directory is not safe")
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(name)
    try:
        os.fchmod(descriptor, 0o600)
        encoded = (
            json.dumps(
                payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")
            )
            + "\n"
        ).encode("utf-8")
        _write_all(descriptor, encoded)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


@contextlib.contextmanager
def _release_lock(lock_path: Path) -> Iterator[None]:
    if not lock_path.parent.is_dir() or lock_path.parent.is_symlink():
        raise ReleaseError("snapshot runtime directory is not safe")
    descriptor = os.open(
        lock_path,
        os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ReleaseError("snapshot install lock is unsafe")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ReleaseError("snapshot install lock is busy") from exc
        yield
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _release_target(path: Path, releases_root: Path, label: str) -> Path:
    root = _assert_directory(releases_root, "releases root")
    candidate = _assert_directory(path, label)
    if candidate.parent != root:
        raise ReleaseError(f"{label} must be one direct child of releases root")
    return candidate


def _current_target(current_link: Path, releases_root: Path) -> Path:
    if not current_link.is_symlink():
        raise ReleaseError("current release is not a symlink")
    return _release_target(current_link.resolve(), releases_root, "previous release")


def _validate_candidate(candidate: Path) -> None:
    _assert_executable(candidate / ".venv/bin/python", "candidate Python interpreter")
    required = (
        candidate / "src/dcar_eval/dcar_auth/admin.py",
        candidate / "deploy/server/nginx/login.html",
        candidate / "deploy/server/libexec/dcar-auth-backup.py",
    )
    for path in required:
        _assert_regular(path, f"candidate file {path.relative_to(candidate)}")


def _http_smoke(
    urls: Sequence[str],
    timeout: float = 10.0,
    *,
    readiness_timeout: float = 30.0,
    retry_interval: float = 1.0,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Wait for Type=simple services within one deadline shared by all URLs.

    Connection failures and 5xx can occur before a just-started service is
    ready. Other non-2xx responses are definitive failures, not startup delays.
    Only response headers are needed: reading an arbitrary response body could
    extend the readiness deadline on a slowly streaming endpoint.
    """
    if not urls:
        raise ReleaseError("at least one smoke URL is required")
    if timeout <= 0 or readiness_timeout <= 0 or retry_interval <= 0:
        raise ReleaseError("smoke timeouts and retry interval must be positive")
    deadline = monotonic() + readiness_timeout
    for url in urls:
        request = urllib.request.Request(url, method="GET")
        last_failure = "readiness deadline exhausted before request"
        while True:
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise ReleaseError(
                    f"smoke readiness deadline ({readiness_timeout:g}s) exceeded "
                    f"for {url}: {last_failure}"
                )
            try:
                with urllib.request.urlopen(
                    request, timeout=min(timeout, remaining)
                ) as response:
                    status = int(response.status)
            except urllib.error.HTTPError as exc:
                status = exc.code
                exc.close()
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                status = None
                last_failure = type(exc).__name__
            if status is not None:
                if 200 <= status < 300:
                    if monotonic() <= deadline:
                        break
                    last_failure = f"HTTP {status} arrived after readiness deadline"
                elif 500 <= status < 600:
                    last_failure = f"HTTP {status}"
                else:
                    raise ReleaseError(f"smoke returned HTTP {status} for {url}")
            remaining = deadline - monotonic()
            if remaining > 0:
                sleep(min(retry_interval, remaining))


def _run_checked(
    command: Sequence[str],
    *,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        list(command),
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    if completed.returncode != 0:
        executable = Path(command[0]).name
        raise ReleaseError(
            f"{executable} exited {completed.returncode}: "
            f"{completed.stderr.strip()[:500]}"
        )
    return completed


def _systemctl_action(config: ReleaseConfig, action: str) -> None:
    if action == "stop":
        for unit in (BACKUP_TIMER, BACKUP_SERVICE):
            if _systemctl_unit_loaded(config, unit):
                _run_checked([str(config.systemctl), "stop", unit])
        _run_checked([str(config.systemctl), "stop", *SERVICES])
    elif action == "start":
        _run_checked([str(config.systemctl), "start", *reversed(SERVICES)])
    elif action == "start-timer":
        _run_checked([str(config.systemctl), "enable", "--now", BACKUP_TIMER])
    elif action == "reload":
        _run_checked([str(config.systemctl), "daemon-reload"])
    elif action == "disable-timer":
        if _systemctl_unit_loaded(config, BACKUP_TIMER):
            _run_checked([str(config.systemctl), "disable", "--now", BACKUP_TIMER])
    else:
        raise ReleaseError(f"unsupported service action {action}")


def _systemctl_unit_loaded(config: ReleaseConfig, unit: str) -> bool:
    command = [str(config.systemctl), "show", unit, "--property=LoadState", "--value"]
    result = subprocess.run(
        command, capture_output=True, text=True, check=False, stdin=subprocess.DEVNULL
    )
    if result.stdout.strip() == "not-found":
        return False
    if result.returncode != 0:
        raise ReleaseError(f"cannot inspect systemd unit {unit}")
    return True


def _admin_command(config: ReleaseConfig, arguments: Sequence[str]) -> list[str]:
    candidate = config.candidate_release
    command = [
        str(candidate / ".venv/bin/python"),
        "-m",
        "dcar_auth.admin",
        "--db",
        str(config.auth_database),
        "--change-log",
        str(config.change_log),
        *arguments,
    ]
    if os.geteuid() == 0 and config.run_user:
        return ["/usr/sbin/runuser", "--user", config.run_user, "--", *command]
    return command


def _run_admin(config: ReleaseConfig, arguments: Sequence[str]) -> str:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(config.candidate_release / "src/dcar_eval")
    return _run_checked(_admin_command(config, arguments), env=environment).stdout


def _auth_identity(path: Path) -> dict[str, int]:
    connection = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    try:
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if version != AUTH_SCHEMA_VERSION:
            raise ReleaseError(f"auth database is schema {version}, expected {AUTH_SCHEMA_VERSION}")
        active_users = int(
            connection.execute(
                "SELECT COUNT(*) FROM auth_users WHERE status='active'"
            ).fetchone()[0]
        )
        superadmins = int(
            connection.execute(
                "SELECT COUNT(*) FROM auth_users "
                "WHERE status='active' AND role='superadmin'"
            ).fetchone()[0]
        )
        return {
            "user_version": version,
            "active_users": active_users,
            "superadmins": superadmins,
        }
    except sqlite3.Error as exc:
        raise ReleaseError("auth database lacks the current account identity") from exc
    finally:
        connection.close()


def _switch_link(current_link: Path, target: Path) -> None:
    temporary = current_link.with_name(f".{current_link.name}.{os.getpid()}.next")
    if temporary.exists() or temporary.is_symlink():
        raise ReleaseError("temporary current symlink already exists")
    try:
        os.symlink(target, temporary)
        os.replace(temporary, current_link)
        _fsync_directory(current_link.parent)
    finally:
        if temporary.is_symlink():
            temporary.unlink()


def _install_nonempty_htpasswd(source: Path, destination: Path) -> None:
    info = _assert_regular(source, "exported htpasswd")
    if info.st_size <= 0:
        raise ReleaseError("refusing to install an empty htpasswd export")
    lines = source.read_text(encoding="utf-8").splitlines()
    if not lines or any(":" not in line or line.startswith(":") for line in lines):
        raise ReleaseError("exported htpasswd is empty or malformed")
    destination_info = _assert_regular(destination, "rollback htpasswd")
    descriptor, name = tempfile.mkstemp(
        prefix=f".{destination.name}.", dir=destination.parent
    )
    temporary = Path(name)
    try:
        with source.open("rb") as reader, os.fdopen(descriptor, "wb") as writer:
            descriptor = -1
            for block in iter(lambda: reader.read(1024 * 1024), b""):
                writer.write(block)
            writer.flush()
            os.fsync(writer.fileno())
        os.chmod(temporary, stat.S_IMODE(destination_info.st_mode))
        with contextlib.suppress(PermissionError):
            os.chown(temporary, destination_info.st_uid, destination_info.st_gid)
        os.replace(temporary, destination)
        _fsync_directory(destination.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _initial_receipt(
    config: ReleaseConfig,
    candidate: Path,
    previous: Path,
    source_version: int,
) -> dict[str, object]:
    return {
        "schema": RECEIPT_SCHEMA,
        "status": "pre_smoke_passed",
        "candidate_release": str(candidate),
        "previous_release": str(previous),
        "source_user_version": source_version,
        "candidate_switched": False,
        "services_started": False,
        "accounts_imported": False,
        "pre_migration_backup": None,
        "post_migration_backup": None,
        "installed_files": [],
        "backup_timer_was_enabled": (
            config.systemd_dir / "timers.target.wants" / BACKUP_TIMER
        ).is_symlink(),
        "auth_database": str(config.auth_database),
        "htpasswd": str(config.htpasswd),
        "backup_dir": str(config.backup_dir),
        "systemd_dir": str(config.systemd_dir),
        "libexec_dir": str(config.libexec_dir),
        "current_link": str(config.current_link),
        "auth_database_sha256_before": _sha256(config.auth_database),
    }


def _deployment_files(config: ReleaseConfig) -> list[tuple[Path, Path, int]]:
    candidate = config.candidate_release
    return [
        (candidate / "deploy/server/systemd" / name, config.systemd_dir / name, 0o644)
        for name in ("dcar-auth.service", BACKUP_SERVICE, BACKUP_TIMER)
    ] + [
        (
            candidate / "deploy/server/libexec" / f"{name}.py",
            config.libexec_dir / name,
            0o755,
        )
        for name in ("dcar-auth-backup", "dcar-auth-release")
    ]


def _disk_usage(path: Path) -> tuple[int, int, int]:
    """Return device, total bytes and bytes available to the service user.

    f_bavail deliberately excludes filesystem blocks reserved for root. The
    release helper runs as root but the database and audit log writer do not.
    """
    directory = _assert_directory(path, "disk preflight directory")
    volume = os.statvfs(directory)
    return (
        directory.stat().st_dev,
        volume.f_blocks * volume.f_frsize,
        volume.f_bavail * volume.f_frsize,
    )


def _check_disk_headroom(
    config: ReleaseConfig,
    *,
    operation: str,
    selected_backup: Path | None = None,
    writes_htpasswd: bool = False,
) -> list[dict[str, object]]:
    """Budget temporary writes per actual filesystem, without a bypass flag.

    Keep the larger of 2 GiB or 3% free *after* conservative operation-specific
    writes. Existing candidate/dependencies are already allocated and therefore
    reflected in f_bavail; this does not budget or delete a release clone.
    """
    if operation not in {"deploy", "rollback", "restore"}:
        raise ReleaseError("unknown disk preflight operation")
    database_bytes = _assert_regular(config.auth_database, "auth database").st_size
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = config.auth_database.with_name(config.auth_database.name + suffix)
        if sidecar.exists() or sidecar.is_symlink():
            database_bytes += _assert_regular(sidecar, "auth database sidecar").st_size
    restored_bytes = (
        _assert_regular(selected_backup, "selected auth backup").st_size
        if selected_backup is not None
        else database_bytes
    )
    working_bytes = max(database_bytes, restored_bytes)
    # Schema growth + transaction journal, or a restore candidate + its journal.
    budgets: list[tuple[Path, str, int]] = [
        (config.auth_database.parent, "auth_database", 2 * working_bytes + 64 * MIB),
        (config.change_log.parent, "change_log", working_bytes + 16 * MIB),
    ]
    if writes_htpasswd:
        budgets.append((config.htpasswd.parent, "htpasswd", working_bytes + MIB))
    if operation != "restore":
        budgets.append((config.current_link.parent, "current_link", MIB))
    if operation == "deploy":
        # Before migration: one current DB. Afterwards: allow 2x schema growth.
        backup_bytes = 3 * database_bytes + 64 * MIB
    else:
        # Offline recovery preserves the current DB and all sidecars first.
        backup_bytes = database_bytes + 64 * MIB
    budgets.append((config.backup_dir, "auth_backups", backup_bytes))
    saved_files_bytes = 4 * MIB
    if operation != "restore":
        targets = {target for _, target, _ in _deployment_files(config)}
        targets.update(config.systemd_dir / unit for unit in SERVICES)
        for target in targets:
            if target.exists() or target.is_symlink():
                saved_files_bytes += _assert_regular(
                    target, "old deployment file"
                ).st_size
    budgets.append(
        (config.receipt.parent, "receipt_and_saved_files", saved_files_bytes)
    )
    if operation != "restore":
        for source, target, _ in _deployment_files(config):
            budgets.append(
                (
                    target.parent,
                    f"install:{target.name}",
                    _assert_regular(source, "candidate deployment file").st_size,
                )
            )
    volumes: dict[int, dict[str, Any]] = {}
    for path, label, budget in budgets:
        device, total, available = _disk_usage(path)
        volume = volumes.setdefault(
            device,
            {
                "device": device,
                "paths": [],
                "total_bytes": total,
                "available_bytes": available,
                "planned_write_bytes": 0,
                "reserve_bytes": max(
                    MIN_FREE_BYTES, (total * MIN_FREE_PERCENT + 99) // 100
                ),
            },
        )
        volume["available_bytes"] = min(volume["available_bytes"], available)
        volume["planned_write_bytes"] += budget
        volume["paths"].append({"path": str(path), "purpose": label, "bytes": budget})
    for volume in volumes.values():
        required = volume["reserve_bytes"] + volume["planned_write_bytes"]
        volume["required_available_bytes"] = required
        if volume["available_bytes"] < required:
            paths = ", ".join(sorted({item["path"] for item in volume["paths"]}))
            raise ReleaseError(
                f"insufficient disk headroom on {paths}: available "
                f"{volume['available_bytes']} bytes, required {required} bytes "
                f"(planned writes {volume['planned_write_bytes']}; preserve at least "
                "2 GiB and 3% free). Services have not been stopped; add capacity "
                "or move verified expendable artifacts after operator review"
            )
    return list(volumes.values())


def _atomic_copy(
    source: Path,
    destination: Path,
    mode: int,
    uid: int | None = None,
    gid: int | None = None,
) -> None:
    _assert_regular(source, "file installation source")
    _assert_directory(destination.parent, "file installation directory")
    if destination.exists() or destination.is_symlink():
        _assert_regular(destination, "file installation target")
    descriptor, name = tempfile.mkstemp(
        prefix=f".{destination.name}.", dir=destination.parent
    )
    temporary = Path(name)
    try:
        with source.open("rb") as reader, os.fdopen(descriptor, "wb") as writer:
            descriptor = -1
            shutil.copyfileobj(reader, writer)
            writer.flush()
            os.fsync(writer.fileno())
        os.chmod(temporary, mode)
        if os.geteuid() == 0 and uid is not None and gid is not None:
            os.chown(temporary, uid, gid)
        os.replace(temporary, destination)
        _fsync_directory(destination.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _save_deployment_files(config: ReleaseConfig) -> list[dict[str, object]]:
    directory = config.receipt.with_suffix(".files")
    directory.mkdir(mode=0o700)
    records: list[dict[str, object]] = []
    # Save all four old service units, including the three not changed by this
    # auth overlay, so the rollback receipt pins the complete service contract.
    targets = {target for _, target, _ in _deployment_files(config)}
    targets.update(config.systemd_dir / unit for unit in SERVICES)
    for index, target in enumerate(sorted(targets)):
        record: dict[str, object] = {"target": str(target), "existed": False}
        if target.exists() or target.is_symlink():
            info = _assert_regular(target, "old deployment file")
            saved = directory / f"{index}-{target.name}"
            _atomic_copy(target, saved, 0o600)
            record.update(
                existed=True,
                saved=str(saved),
                sha256=_sha256(saved),
                mode=stat.S_IMODE(info.st_mode),
                uid=info.st_uid,
                gid=info.st_gid,
            )
        records.append(record)
    _fsync_directory(directory)
    return records


def _restore_deployment_files(config: ReleaseConfig, records: Any) -> None:
    allowed = {target for _, target, _ in _deployment_files(config)}
    allowed.update(config.systemd_dir / unit for unit in SERVICES)
    if (
        not isinstance(records, list)
        or {Path(str(item["target"])) for item in records} != allowed
    ):
        raise ReleaseError("receipt deployment-file inventory is invalid")
    # Validate everything before replacing even one file.
    for item in records:
        if item.get("existed"):
            source = Path(str(item["saved"]))
            if source.parent != config.receipt.with_suffix(".files"):
                raise ReleaseError("saved deployment file is outside receipt directory")
            _assert_regular(source, "saved deployment file")
            if _sha256(source) != item["sha256"]:
                raise ReleaseError("saved deployment file sha256 mismatch")
    for item in records:
        target = Path(str(item["target"]))
        if item.get("existed"):
            _atomic_copy(
                Path(str(item["saved"])),
                target,
                int(item["mode"]),
                int(item["uid"]),
                int(item["gid"]),
            )
        else:
            target.unlink(missing_ok=True)
            _fsync_directory(target.parent)


def deploy_release(
    config: ReleaseConfig,
    *,
    smoke_check: Callable[[Sequence[str]], None] = _http_smoke,
    service_action: Callable[[ReleaseConfig, str], None] = _systemctl_action,
    admin_runner: Callable[[ReleaseConfig, Sequence[str]], str] = _run_admin,
    checkpoint: Callable[[str], None] | None = None,
) -> dict[str, object]:
    if config.receipt.exists() or config.receipt.is_symlink():
        raise ReleaseError("release receipt path must be new")
    candidate = _release_target(
        config.candidate_release, config.releases_root, "candidate release"
    )
    config = config._replace(candidate_release=candidate)
    previous = _current_target(config.current_link, config.releases_root)
    if candidate == previous:
        raise ReleaseError("candidate release is already current")
    _validate_candidate(candidate)
    for source, target, _ in _deployment_files(config):
        _assert_regular(source, "candidate deployment file")
        _assert_directory(target.parent, "deployment target directory")
    _assert_regular(config.auth_database, "auth database")
    if not config.backup_dir.is_dir() or config.backup_dir.is_symlink():
        raise ReleaseError("auth backup directory is not safe")
    source_version = int(_backup_module().database_version(config.auth_database))
    if source_version not in (0, 1, 2, 3):
        raise ReleaseError(f"unsupported source user_version {source_version}")
    if source_version == 0:
        _assert_regular(config.htpasswd, "legacy htpasswd")
    if source_version in (0, 1) and not config.superadmin:
        raise ReleaseError("--superadmin is required for schema 0 or 1")

    # Candidate checks happen while every production service is still running.
    disk_preflight = _check_disk_headroom(config, operation="deploy")
    smoke_check(config.pre_smoke_urls)
    if checkpoint is not None:
        checkpoint("after_pre_smoke")

    payload = _initial_receipt(config, candidate, previous, source_version)
    payload["disk_preflight"] = disk_preflight
    with _release_lock(config.lock_path):
        if config.receipt.exists() or config.receipt.is_symlink():
            raise ReleaseError("release receipt path must still be new under the lock")
        if _current_target(config.current_link, config.releases_root) != previous:
            raise ReleaseError("current release changed after candidate smoke")
        payload["disk_locked"] = _check_disk_headroom(config, operation="deploy")
        _atomic_json(config.receipt, payload)
        try:
            service_action(config, "stop")
            payload["status"] = "services_stopped"
            _atomic_json(config.receipt, payload)
            if checkpoint is not None:
                checkpoint("after_services_stopped")

            backup = _backup_module()
            if backup.database_version(config.auth_database) != source_version:
                raise ReleaseError("auth database schema changed after candidate smoke")
            payload["installed_files"] = _save_deployment_files(config)
            _atomic_json(config.receipt, payload)
            before = backup.create_backup(
                config.auth_database, config.backup_dir, source_version
            )
            payload["pre_migration_backup"] = {
                "path": before["path"],
                "manifest": before["manifest"],
                "sha256": before["sha256"],
            }
            payload["status"] = "pre_migration_backup_created"
            _atomic_json(config.receipt, payload)

            admin_runner(config, ["migrate"])
            payload["status"] = "database_migrated"
            _atomic_json(config.receipt, payload)
            if source_version == 0:
                admin_runner(
                    config,
                    ["import-htpasswd", "--source", str(config.htpasswd)],
                )
                payload["accounts_imported"] = True
                payload["status"] = "accounts_imported"
                _atomic_json(config.receipt, payload)
            identity = _auth_identity(config.auth_database)
            if identity["superadmins"] == 0:
                if not config.superadmin:
                    raise ReleaseError("auth database has no active superadmin")
                admin_runner(config, ["set-role", config.superadmin, "superadmin"])
                identity = _auth_identity(config.auth_database)
            if identity["active_users"] <= 0 or identity["superadmins"] <= 0:
                raise ReleaseError("auth database requires an active user and superadmin")
            payload["auth_identity"] = identity
            after = backup.create_backup(config.auth_database, config.backup_dir, AUTH_SCHEMA_VERSION)
            payload["post_migration_backup"] = {
                "path": after["path"],
                "manifest": after["manifest"],
                "sha256": after["sha256"],
            }
            payload["status"] = "post_migration_backup_created"
            _atomic_json(config.receipt, payload)
            if checkpoint is not None:
                checkpoint("after_migration")

            payload["status"] = "installing_units"
            _atomic_json(config.receipt, payload)
            for source, target, mode in _deployment_files(config):
                _atomic_copy(source, target, mode)
            service_action(config, "reload")
            _switch_link(config.current_link, candidate)
            payload["candidate_switched"] = True
            payload["status"] = "candidate_switched"
            _atomic_json(config.receipt, payload)
            if checkpoint is not None:
                checkpoint("after_switch")

            service_action(config, "start")
            payload["services_started"] = True
            payload["status"] = "services_started"
            _atomic_json(config.receipt, payload)
            smoke_check(config.post_smoke_urls)
            service_action(config, "start-timer")
            payload["status"] = "succeeded"
            payload["auth_database_sha256_after"] = _sha256(config.auth_database)
            _atomic_json(config.receipt, payload)
            return payload
        except Exception as exc:
            payload["status"] = (
                "failed_after_switch"
                if payload["candidate_switched"]
                else "failed_before_switch"
            )
            payload["failure"] = f"{type(exc).__name__}: {str(exc)[:500]}"
            if payload["candidate_switched"]:
                with contextlib.suppress(Exception):
                    service_action(config, "stop")
                payload["services_started"] = False
            else:
                current_version = int(
                    _backup_module().database_version(config.auth_database)
                )
                # Legacy schema-0 code ignores user_version. Schema-1/2 code
                # must never restart against schema 3: it lacks new-user gates.
                if source_version == 0 or current_version == source_version:
                    with contextlib.suppress(Exception):
                        if payload["installed_files"]:
                            _restore_deployment_files(
                                config, payload["installed_files"]
                            )
                            service_action(config, "reload")
                        service_action(config, "start")
                        if payload["backup_timer_was_enabled"]:
                            service_action(config, "start-timer")
                        payload["services_started"] = True
                else:
                    payload["manual_recovery_required"] = f"schema{source_version}_to{current_version}"
            _atomic_json(config.receipt, payload)
            raise ReleaseError(
                f"release failed at {payload['status']}; see {config.receipt}"
            ) from exc


def _read_receipt(path: Path) -> dict[str, Any]:
    _assert_regular(path, "release receipt")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReleaseError("release receipt is invalid JSON") from exc
    if not isinstance(payload, dict) or payload.get("schema") != RECEIPT_SCHEMA:
        raise ReleaseError("release receipt has an invalid schema")
    return payload


def rollback_release(
    config: ReleaseConfig,
    *,
    smoke_check: Callable[[Sequence[str]], None] = _http_smoke,
    service_action: Callable[[ReleaseConfig, str], None] = _systemctl_action,
    admin_runner: Callable[[ReleaseConfig, Sequence[str]], str] = _run_admin,
) -> dict[str, object]:
    payload = _read_receipt(config.receipt)
    for name in (
        "auth_database",
        "htpasswd",
        "backup_dir",
        "systemd_dir",
        "libexec_dir",
        "current_link",
    ):
        if payload.get(name) != str(getattr(config, name)):
            raise ReleaseError(f"{name} argument does not match release receipt")
    if payload.get("status") not in {
        "failed_after_switch",
        "failed_before_switch",
        "services_started",
        "succeeded",
    }:
        raise ReleaseError("receipt is not at a code-rollback phase")
    candidate = _release_target(
        Path(str(payload.get("candidate_release"))),
        config.releases_root,
        "receipt candidate release",
    )
    previous = _release_target(
        Path(str(payload.get("previous_release"))),
        config.releases_root,
        "receipt previous release",
    )
    requested_candidate = _release_target(
        config.candidate_release, config.releases_root, "candidate release"
    )
    if requested_candidate != candidate:
        raise ReleaseError("candidate argument does not match release receipt")
    config = config._replace(candidate_release=candidate)
    source_version = payload.get("source_user_version")
    if type(source_version) is not int or source_version not in (0, 1, 2, 3):
        raise ReleaseError("receipt source_user_version is invalid")
    active_release = _current_target(config.current_link, config.releases_root)
    if active_release not in (candidate, previous):
        raise ReleaseError("current release no longer matches receipt candidate")

    selected_backup = None
    if source_version in (1, 2):
        before = payload.get("pre_migration_backup")
        if not isinstance(before, dict):
            raise ReleaseError(f"receipt lacks the schema-{source_version} pre-migration backup")
        selected_backup = Path(str(before["path"]))
    disk_preflight = _check_disk_headroom(
        config,
        operation="rollback",
        selected_backup=selected_backup,
        writes_htpasswd=source_version == 0 and bool(payload.get("candidate_switched")),
    )

    with _release_lock(config.lock_path):
        if _current_target(config.current_link, config.releases_root) != active_release:
            raise ReleaseError(
                "current release changed before rollback acquired the lock"
            )
        if _read_receipt(config.receipt) != payload:
            raise ReleaseError(
                "release receipt changed before rollback acquired the lock"
            )
        payload["rollback_disk_preflight"] = disk_preflight
        payload["rollback_disk_locked"] = _check_disk_headroom(
            config,
            operation="rollback",
            selected_backup=selected_backup,
            writes_htpasswd=source_version == 0
            and bool(payload.get("candidate_switched")),
        )
        service_action(config, "stop")
        if source_version in (1, 2):
            before = payload.get("pre_migration_backup")
            if not isinstance(before, dict):
                raise ReleaseError(f"receipt lacks the schema-{source_version} pre-migration backup")
            safety = _backup_module().restore_backup_pair(
                Path(str(before["path"])), config.auth_database, config.backup_dir, source_version
            )
            payload["rollback_restore"] = safety
            _revoke_restored_credentials(config.auth_database, source_version)
        if source_version == 0 and payload.get("candidate_switched"):
            export_path = config.auth_database.parent / ".htpasswd.rollback-export"
            export_path.unlink(missing_ok=True)
            try:
                admin_runner(
                    config,
                    ["export-htpasswd", "--output", str(export_path)],
                )
                _install_nonempty_htpasswd(export_path, config.htpasswd)
            finally:
                export_path.unlink(missing_ok=True)
        if not payload.get("backup_timer_was_enabled"):
            service_action(config, "disable-timer")
        if payload.get("installed_files"):
            _restore_deployment_files(config, payload["installed_files"])
            service_action(config, "reload")
        if active_release != previous:
            _switch_link(config.current_link, previous)
        if source_version in (1, 2):
            payload["status"] = "rolled_back_reconciliation_required"
            payload["services_started"] = False
            _atomic_json(config.receipt, payload)
            return payload
        service_action(config, "start")
        smoke_check(config.post_smoke_urls)
        if payload.get("backup_timer_was_enabled"):
            service_action(config, "start-timer")
        payload["status"] = "rolled_back"
        payload["services_started"] = True
        payload["rollback_release"] = str(previous)
        payload["rollback_auth_database_sha256"] = _sha256(config.auth_database)
        _atomic_json(config.receipt, payload)
        return payload


def _revoke_restored_credentials(database: Path, version: int) -> None:
    connection = sqlite3.connect(database)
    try:
        with connection:
            connection.execute("DELETE FROM auth_sessions")
            if version in (1, 2, 3):
                connection.execute(
                    "UPDATE auth_challenges SET invalidated_at=COALESCE(invalidated_at, "
                    "CAST(strftime('%s','now') AS INTEGER))"
                )
    finally:
        connection.close()


def restore_release(
    config: ReleaseConfig,
    selected_backup: Path,
    expected_version: int,
    *,
    service_action: Callable[[ReleaseConfig, str], None] = _systemctl_action,
    admin_runner: Callable[[ReleaseConfig, Sequence[str]], str] = _run_admin,
) -> dict[str, object]:
    """Restore and migrate offline; identity reconciliation always precedes start."""
    if config.receipt.exists() or config.receipt.is_symlink():
        raise ReleaseError("restore receipt path must be new")
    candidate = _release_target(
        config.candidate_release, config.releases_root, "restore release"
    )
    if _current_target(config.current_link, config.releases_root) != candidate:
        raise ReleaseError(
            "restore requires the selected current-schema release to be current"
        )
    config = config._replace(candidate_release=candidate)
    backup = _backup_module()
    pair = backup.verify_backup_pair(selected_backup, expected_version)
    _validate_candidate(config.candidate_release)
    if expected_version == 0:
        _assert_regular(config.htpasswd, "trusted htpasswd for schema-0 restore")
        if not config.superadmin:
            raise ReleaseError("schema-0 restore requires --superadmin")
    disk_preflight = _check_disk_headroom(
        config, operation="restore", selected_backup=selected_backup
    )
    with _release_lock(config.lock_path):
        if config.receipt.exists() or config.receipt.is_symlink():
            raise ReleaseError("restore receipt path must still be new under the lock")
        if _current_target(config.current_link, config.releases_root) != candidate:
            raise ReleaseError(
                "current release changed before restore acquired the lock"
            )
        disk_locked = _check_disk_headroom(
            config, operation="restore", selected_backup=selected_backup
        )
        service_action(config, "stop")
        result = backup.restore_backup_pair(
            selected_backup, config.auth_database, config.backup_dir, expected_version
        )
        _atomic_json(config.receipt, dict(result, schema=RECEIPT_SCHEMA))
        _revoke_restored_credentials(config.auth_database, expected_version)
        admin_runner(config, ["migrate"])
        if expected_version == 0:
            admin_runner(config, ["import-htpasswd", "--source", str(config.htpasswd)])
            admin_runner(config, ["set-role", str(config.superadmin), "superadmin"])
        # Logs are evidence for identity checks, never blindly replay grants or
        # old passwords. Even a clean log cannot prove restored credentials safe.
        result.update(
            schema=RECEIPT_SCHEMA,
            status="restored_reconciliation_required",
            services_started=False,
            reconcile_since=pair.stamp,
            change_log=str(config.change_log),
            current_user_version=AUTH_SCHEMA_VERSION,
            disk_preflight=disk_preflight,
            disk_locked=disk_locked,
        )
        _atomic_json(config.receipt, result)
        return result


def _configuration(arguments: argparse.Namespace) -> ReleaseConfig:
    candidate = arguments.candidate_release
    return ReleaseConfig(
        candidate_release=candidate,
        current_link=arguments.current_link,
        releases_root=arguments.releases_root,
        runtime_root=arguments.runtime_root,
        auth_database=arguments.auth_database,
        backup_dir=arguments.backup_dir,
        htpasswd=arguments.htpasswd,
        change_log=arguments.change_log,
        receipt=arguments.receipt,
        superadmin=arguments.superadmin,
        run_user=arguments.run_user,
        systemctl=arguments.systemctl,
        pre_smoke_urls=tuple(arguments.pre_smoke_url),
        post_smoke_urls=tuple(arguments.post_smoke_url),
        systemd_dir=arguments.systemd_dir,
        libexec_dir=arguments.libexec_dir,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--candidate-release", type=Path, required=True)
    common.add_argument(
        "--current-link", type=Path, default=Path("/var/www/dcar-aigc/current")
    )
    common.add_argument(
        "--releases-root",
        type=Path,
        default=Path("/var/www/dcar-aigc/releases"),
    )
    common.add_argument(
        "--runtime-root", type=Path, default=Path("/var/lib/dcar-aigc/runtime")
    )
    common.add_argument(
        "--auth-database",
        type=Path,
        default=Path("/var/lib/dcar-aigc/auth/sessions.sqlite3"),
    )
    common.add_argument(
        "--backup-dir",
        type=Path,
        default=Path("/var/backups/dcar-aigc/auth"),
    )
    common.add_argument(
        "--htpasswd", type=Path, default=Path("/etc/nginx/.htpasswd-dcar")
    )
    common.add_argument(
        "--change-log",
        type=Path,
        default=Path("/var/lib/dcar-aigc/auth/auth-changes.log"),
    )
    common.add_argument("--receipt", type=Path, required=True)
    common.add_argument("--superadmin")
    common.add_argument("--run-user", default="dcar-aigc")
    common.add_argument("--systemctl", type=Path, default=Path("/usr/bin/systemctl"))
    common.add_argument("--pre-smoke-url", action="append", default=[])
    common.add_argument("--post-smoke-url", action="append", default=[])
    common.add_argument("--systemd-dir", type=Path, default=Path("/etc/systemd/system"))
    common.add_argument("--libexec-dir", type=Path, default=Path("/usr/local/libexec"))

    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("deploy", parents=[common])
    commands.add_parser("rollback", parents=[common])
    restore = commands.add_parser("restore", parents=[common])
    restore.add_argument("--backup", type=Path, required=True)
    restore.add_argument(
        "--expect-user-version", type=int, choices=(0, 1, 2, 3), required=True
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    config = _configuration(arguments)
    try:
        if arguments.command == "deploy":
            result = deploy_release(config)
        elif arguments.command == "rollback":
            result = rollback_release(config)
        else:
            result = restore_release(
                config, arguments.backup, arguments.expect_user_version
            )
    except (OSError, ReleaseError, sqlite3.Error) as exc:
        print(f"auth release failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 3 if str(result["status"]).endswith("reconciliation_required") else 0


if __name__ == "__main__":
    raise SystemExit(main())
