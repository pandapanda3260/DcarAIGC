"""Resolve the installed DCar writer database and its access contract.

This module is deliberately side-effect free at import time.  Reading the
installed LaunchAgent, inspecting database identities and acquiring the writer
lock happen only through explicit function calls.
"""

from __future__ import annotations

import argparse
import fcntl
import os
import plistlib
import pwd
import sqlite3
import stat
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Iterator, Mapping, Sequence


WRITER_LABEL = "cn.tj.dcar.writer-worker"
WRITER_PLIST_NAME = f"{WRITER_LABEL}.plist"


class RuntimeDatabaseError(RuntimeError):
    """The requested database access is not authorized by installed runtime."""


class DatabaseAccessMode(str, Enum):
    WRITER = "writer"
    FORMAL_MUTATION = "formal_mutation"
    FORMAL_READ = "formal_read"
    ISOLATED_CANDIDATE = "isolated_candidate"


@dataclass(frozen=True, slots=True)
class FileIdentity:
    device: int
    inode: int
    nlink: int
    uid: int
    mode: int

    @classmethod
    def from_stat(cls, value: os.stat_result) -> "FileIdentity":
        return cls(
            device=int(value.st_dev),
            inode=int(value.st_ino),
            nlink=int(value.st_nlink),
            uid=int(value.st_uid),
            mode=stat.S_IMODE(value.st_mode),
        )


@dataclass(frozen=True, slots=True)
class InstalledWriterContract:
    home: Path
    plist_path: Path
    project_root: Path
    program: Path
    database: Path
    writer_lock: Path
    payload: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class ResolvedDatabaseAccess:
    access_mode: DatabaseAccessMode
    database: Path
    database_identity: FileIdentity
    project_root: Path | None
    writer_lock: Path | None
    installed: InstalledWriterContract | None

    def health_identity(self) -> dict[str, object]:
        return {
            "canonical_path": str(self.database),
            "device": self.database_identity.device,
            "inode": self.database_identity.inode,
            "nlink": self.database_identity.nlink,
            "access_mode": self.access_mode.value,
        }


@dataclass(frozen=True, slots=True)
class WriterLockLease:
    path: Path
    identity: FileIdentity

    def health_identity(self, *, held: bool) -> dict[str, object]:
        return {
            "path": str(self.path),
            "device": self.identity.device,
            "inode": self.identity.inode,
            "held": held,
        }


_PROCESS_WRITER_LEASES: dict[int, tuple[int, ResolvedDatabaseAccess, WriterLockLease]] = {}
_PROCESS_WRITER_LEASE_GUARD = threading.RLock()


def require_current_process_writer_lock(connection: sqlite3.Connection) -> dict[str, object]:
    """Prove ownership of this DB's live writer lease, not another process's lock.

    Diagnostic control uses this in addition to durable command/member permits.
    Formal-mutation CLI leases are intentionally not writer-runner authority.
    """
    row = next((row for row in connection.execute("PRAGMA database_list") if row[1] == "main"), None)
    if row is None or not row[2]:
        raise RuntimeDatabaseError("diagnostic runner requires an on-disk writer database")
    database = Path(row[2])
    try:
        database_stat = database.stat()
    except OSError as exc:
        raise RuntimeDatabaseError("diagnostic writer database identity is unavailable") from exc
    with _PROCESS_WRITER_LEASE_GUARD:
        matches = []
        for descriptor, (pid, access, lease) in _PROCESS_WRITER_LEASES.items():
            if pid != os.getpid() or access.access_mode is not DatabaseAccessMode.WRITER:
                continue
            if (database_stat.st_dev, database_stat.st_ino) != (
                access.database_identity.device, access.database_identity.inode,
            ):
                continue
            try:
                descriptor_stat = os.fstat(descriptor)
                path_stat = lease.path.lstat()
            except OSError as exc:
                raise RuntimeDatabaseError("diagnostic writer lease descriptor is unavailable") from exc
            if (
                not stat.S_ISREG(path_stat.st_mode)
                or FileIdentity.from_stat(descriptor_stat) != lease.identity
                or FileIdentity.from_stat(path_stat) != lease.identity
                or not os.path.samestat(database_stat, access.database.stat())
            ):
                raise RuntimeDatabaseError("diagnostic writer lease identity changed")
            matches.append({
                "contract_version": "current-process-writer-lease-v1",
                "pid": pid, "database_path": str(access.database),
                "database_device": access.database_identity.device,
                "database_inode": access.database_identity.inode,
                "lock_path": str(lease.path), "lock_device": lease.identity.device,
                "lock_inode": lease.identity.inode,
            })
        if len(matches) != 1:
            raise RuntimeDatabaseError("current process does not own this database writer lease")
        return matches[0]


def same_file_identity(left: Path, right: Path) -> bool:
    """Compare existing paths by device/inode, including APFS path aliases."""

    left_value = Path(left).expanduser()
    right_value = Path(right).expanduser()
    try:
        left_stat = left_value.stat()
    except (FileNotFoundError, NotADirectoryError):
        left_stat = None
    try:
        right_stat = right_value.stat()
    except (FileNotFoundError, NotADirectoryError):
        right_stat = None
    if left_stat is not None and right_stat is not None:
        return os.path.samestat(left_stat, right_stat)
    return left_value.resolve(strict=False) == right_value.resolve(strict=False)


def _current_home() -> Path:
    try:
        return Path(pwd.getpwuid(os.getuid()).pw_dir).resolve(strict=True)
    except (KeyError, OSError, RuntimeError) as error:
        raise RuntimeDatabaseError("cannot resolve the current OS user's home") from error


def _has_symlink_component(path: Path) -> bool:
    return any(candidate.is_symlink() for candidate in (path, *path.parents))


def _require_absolute_path(value: object, *, label: str) -> Path:
    if not isinstance(value, (str, os.PathLike)):
        raise RuntimeDatabaseError(f"{label} is missing")
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise RuntimeDatabaseError(f"{label} must be absolute")
    return path


def _require_database_file(
    path: Path, *, label: str, allow_root_owner: bool = False
) -> tuple[Path, FileIdentity]:
    lexical = Path(path).expanduser()
    if lexical.is_symlink() or not lexical.is_file():
        raise RuntimeDatabaseError(f"{label} must be an existing regular non-symlink file")
    canonical = lexical.resolve(strict=True)
    value = canonical.stat()
    if not stat.S_ISREG(value.st_mode) or value.st_size <= 0:
        raise RuntimeDatabaseError(f"{label} must be a non-empty regular file")
    identity = FileIdentity.from_stat(value)
    allowed_owner_uids = {os.geteuid(), 0} if allow_root_owner else {os.geteuid()}
    if identity.uid not in allowed_owner_uids:
        owner = "the current user or root" if allow_root_owner else "the current user"
        raise RuntimeDatabaseError(f"{label} must be owned by {owner}")
    if identity.mode & 0o022:
        raise RuntimeDatabaseError(f"{label} must not be group/world writable")
    if identity.nlink != 1:
        raise RuntimeDatabaseError(f"{label} must be a single-link file")
    return canonical, identity


def load_installed_writer_contract(
    *,
    required: bool = True,
    home: Path | None = None,
) -> InstalledWriterContract | None:
    """Load and validate the installed Mac writer contract without DB access."""

    resolved_home = (home.expanduser().resolve(strict=True) if home is not None else _current_home())
    plist_path = resolved_home / "Library/LaunchAgents" / WRITER_PLIST_NAME
    if not plist_path.exists() and not plist_path.is_symlink():
        if required:
            raise RuntimeDatabaseError("installed writer LaunchAgent is missing")
        return None
    if _has_symlink_component(plist_path) or not plist_path.is_file():
        raise RuntimeDatabaseError("installed writer LaunchAgent path is unsafe")
    metadata = plist_path.stat()
    if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) & 0o022:
        raise RuntimeDatabaseError("installed writer LaunchAgent ownership or mode is unsafe")
    try:
        payload = plistlib.loads(plist_path.read_bytes())
    except (OSError, TypeError, ValueError) as error:
        raise RuntimeDatabaseError("installed writer LaunchAgent is invalid") from error
    if not isinstance(payload, dict) or payload.get("Label") != WRITER_LABEL:
        raise RuntimeDatabaseError("installed writer LaunchAgent label is invalid")
    project_root = _require_absolute_path(
        payload.get("WorkingDirectory"), label="installed writer project root"
    )
    if project_root.is_symlink() or not project_root.is_dir():
        raise RuntimeDatabaseError("installed writer project root is unsafe")
    project_root = project_root.resolve(strict=True)
    environment = payload.get("EnvironmentVariables")
    if not isinstance(environment, dict):
        raise RuntimeDatabaseError("installed writer environment is invalid")
    if environment.get("DCAR_PROJECT_ROOT") != str(project_root):
        raise RuntimeDatabaseError("installed writer environment project root is invalid")
    source_root = project_root
    if environment.get("DCAR_WRITER_SOURCE_ROOT"):
        source_root = _require_absolute_path(environment["DCAR_WRITER_SOURCE_ROOT"],
                                             label="installed writer source root")
        if (not source_root.is_dir() or _has_symlink_component(source_root)
                or source_root == project_root or source_root.is_relative_to(project_root)
                or project_root.is_relative_to(source_root)):
            raise RuntimeDatabaseError("installed writer source root must be independent and safe")
    program_arguments = payload.get("ProgramArguments")
    expected_program = source_root / "deploy/macos/run_writer_worker.sh"
    if program_arguments != [str(expected_program)]:
        raise RuntimeDatabaseError("installed writer program does not match its source root")
    database = _require_absolute_path(
        environment.get("DCAR_V8_DB"), label="installed writer database"
    )
    writer_lock = _require_absolute_path(
        environment.get("DCAR_WRITER_LOCK"), label="installed writer lock"
    )
    for path, label in ((database, "database"), (writer_lock, "writer lock")):
        resolved = path.resolve(strict=False)
        if resolved == project_root or project_root in resolved.parents:
            raise RuntimeDatabaseError(f"installed writer {label} must stay outside the project")
    return InstalledWriterContract(
        home=resolved_home,
        plist_path=plist_path,
        project_root=project_root,
        program=expected_program,
        database=database,
        writer_lock=writer_lock,
        payload=payload,
    )


def _require_same_installed_database(
    candidate: Path, installed: InstalledWriterContract, *, label: str
) -> None:
    lexical = _require_absolute_path(candidate, label=label)
    if lexical.is_symlink():
        raise RuntimeDatabaseError(f"{label} must not be a symlink")
    if not lexical.is_file():
        raise RuntimeDatabaseError(f"{label} is missing")
    if not same_file_identity(lexical, installed.database):
        raise RuntimeDatabaseError(f"{label} does not identify the installed writer database")


def is_installed_formal_database(
    path: Path,
    *,
    required: bool = False,
    home: Path | None = None,
    installed: InstalledWriterContract | None = None,
) -> bool:
    contract = installed or load_installed_writer_contract(required=required, home=home)
    if contract is None:
        return False
    return same_file_identity(Path(path), contract.database)


def resolve_installed_database_access(
    access_mode: DatabaseAccessMode | str,
    *,
    database: Path | None,
    project_root: Path,
    environ: Mapping[str, str] | None = None,
    home: Path | None = None,
    installed: InstalledWriterContract | None = None,
) -> ResolvedDatabaseAccess:
    """Authorize one writer/formal mutation/formal read against installed DB."""

    mode = DatabaseAccessMode(access_mode)
    if mode is DatabaseAccessMode.ISOLATED_CANDIDATE:
        raise RuntimeDatabaseError("isolated candidates use resolve_isolated_candidate")
    values = os.environ if environ is None else environ
    contract = installed or load_installed_writer_contract(required=True, home=home)
    assert contract is not None
    requested_root = _require_absolute_path(project_root, label="project root")
    if requested_root.resolve(strict=True) != contract.project_root:
        raise RuntimeDatabaseError("project root does not match the installed writer")
    env_database_value = values.get("DCAR_V8_DB")
    if mode is DatabaseAccessMode.WRITER and not env_database_value:
        raise RuntimeDatabaseError("writer access requires DCAR_V8_DB")
    if database is None and not env_database_value:
        raise RuntimeDatabaseError("formal database access requires an explicit database")
    if database is not None:
        _require_same_installed_database(database, contract, label="explicit database")
    if env_database_value:
        _require_same_installed_database(
            Path(env_database_value), contract, label="DCAR_V8_DB"
        )
    canonical_database, database_identity = _require_database_file(
        contract.database, label="installed writer database"
    )
    env_root = values.get("DCAR_PROJECT_ROOT")
    if env_root and Path(env_root).expanduser().resolve(strict=True) != contract.project_root:
        raise RuntimeDatabaseError("DCAR_PROJECT_ROOT does not match the installed writer")
    if mode is DatabaseAccessMode.WRITER:
        if not env_root:
            raise RuntimeDatabaseError("writer access requires the installed DCAR_PROJECT_ROOT")
        env_lock = values.get("DCAR_WRITER_LOCK")
        if not env_lock:
            raise RuntimeDatabaseError("writer access requires DCAR_WRITER_LOCK")
        requested_lock = _require_absolute_path(env_lock, label="DCAR_WRITER_LOCK")
        if requested_lock.resolve(strict=False) != contract.writer_lock.resolve(strict=False):
            raise RuntimeDatabaseError("DCAR_WRITER_LOCK does not match the installed writer")
    if mode is not DatabaseAccessMode.WRITER and values.get("DCAR_WRITER_LOCK"):
        requested_lock = _require_absolute_path(
            values["DCAR_WRITER_LOCK"], label="DCAR_WRITER_LOCK"
        )
        if requested_lock.resolve(strict=False) != contract.writer_lock.resolve(strict=False):
            raise RuntimeDatabaseError("DCAR_WRITER_LOCK does not match the installed writer")
    return ResolvedDatabaseAccess(
        access_mode=mode,
        database=canonical_database,
        database_identity=database_identity,
        project_root=contract.project_root,
        # Preserve the installed lexical path so the lock opener can reject a
        # symlink instead of silently following it to an otherwise valid inode.
        writer_lock=contract.writer_lock,
        installed=contract,
    )


def resolve_read_only_replica(database: Path) -> ResolvedDatabaseAccess:
    """Resolve an explicit sealed replica without an installed writer lock.

    A deployed snapshot is a formal read replica but is intentionally not the
    same inode as the installed Mac writer database.  Its installation receipt
    remains the authority for snapshot lineage; this resolver only establishes
    the local file identity reported by health.
    """

    candidate = _require_absolute_path(database, label="read-only replica database")
    canonical, identity = _require_database_file(
        candidate,
        label="read-only replica database",
        allow_root_owner=True,
    )
    return ResolvedDatabaseAccess(
        access_mode=DatabaseAccessMode.FORMAL_READ,
        database=canonical,
        database_identity=identity,
        project_root=None,
        writer_lock=None,
        installed=None,
    )


def resolve_isolated_candidate(
    database: Path,
    *,
    home: Path | None = None,
    installed: InstalledWriterContract | None = None,
) -> ResolvedDatabaseAccess:
    """Resolve an explicit non-formal database while preserving test fixtures."""

    candidate = _require_absolute_path(database, label="isolated candidate database")
    if candidate.is_symlink():
        raise RuntimeDatabaseError("isolated candidate database must not be a symlink")
    contract = installed
    if contract is None:
        contract = load_installed_writer_contract(required=False, home=home)
    if contract is not None and same_file_identity(candidate, contract.database):
        raise RuntimeDatabaseError("installed writer database cannot be an isolated candidate")
    if not candidate.is_file():
        raise RuntimeDatabaseError("isolated candidate database must already exist")
    canonical, identity = _require_database_file(
        candidate, label="isolated candidate database"
    )
    return ResolvedDatabaseAccess(
        access_mode=DatabaseAccessMode.ISOLATED_CANDIDATE,
        database=canonical,
        database_identity=identity,
        project_root=None,
        writer_lock=None,
        installed=contract,
    )


def resolve_isolated_fixture(database: Path) -> ResolvedDatabaseAccess:
    """Resolve a directly constructed local/test database capability.

    Production environment and CLI entry points must use
    ``resolve_isolated_candidate`` so an installed writer inode cannot be
    relabeled as isolated.  This narrow helper preserves in-process fixtures
    whose ApiConfig is constructed directly rather than from the environment.
    """

    candidate = _require_absolute_path(database, label="isolated fixture database")
    canonical, identity = _require_database_file(
        candidate, label="isolated fixture database"
    )
    return ResolvedDatabaseAccess(
        access_mode=DatabaseAccessMode.ISOLATED_CANDIDATE,
        database=canonical,
        database_identity=identity,
        project_root=None,
        writer_lock=None,
        installed=None,
    )


def _open_verified_writer_lock(access: ResolvedDatabaseAccess) -> tuple[int, WriterLockLease]:
    if access.writer_lock is None or access.installed is None:
        raise RuntimeDatabaseError("installed writer lock is missing from access contract")
    path = access.writer_lock
    parent = path.parent
    if parent.is_symlink() or not parent.is_dir():
        raise RuntimeDatabaseError("installed writer lock parent is unsafe")
    if path.is_symlink():
        raise RuntimeDatabaseError("installed writer lock must not be a symlink")
    descriptor = os.open(
        path,
        os.O_RDWR | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        value = os.fstat(descriptor)
        identity = FileIdentity.from_stat(value)
        if (
            not stat.S_ISREG(value.st_mode)
            or identity.nlink != 1
            or identity.uid != os.geteuid()
            or identity.mode != 0o600
        ):
            raise RuntimeDatabaseError(
                "installed writer lock must be a current-user 0600 regular "
                "single-link file"
            )
        current = path.stat()
        if not os.path.samestat(value, current):
            raise RuntimeDatabaseError("installed writer lock identity changed")
        return descriptor, WriterLockLease(path=path, identity=identity)
    except BaseException:
        os.close(descriptor)
        raise


def observe_writer_lock(access: ResolvedDatabaseAccess) -> dict[str, object]:
    """Observe the installed lock without taking ownership of writer work."""

    descriptor, lease = _open_verified_writer_lock(access)
    acquired = False
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
            held = False
        except BlockingIOError:
            held = True
        return lease.health_identity(held=held)
    finally:
        if acquired:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


@contextmanager
def acquire_writer_lock(
    access: ResolvedDatabaseAccess,
) -> Iterator[WriterLockLease]:
    """Acquire the installed writer lock for writer/formal-mutation access."""

    if access.access_mode not in {
        DatabaseAccessMode.WRITER,
        DatabaseAccessMode.FORMAL_MUTATION,
    }:
        raise RuntimeDatabaseError("this database access mode cannot acquire writer lock")
    descriptor, lease = _open_verified_writer_lock(access)
    acquired = False
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeDatabaseError("installed writer lock is already held") from error
        acquired = True
        if access.access_mode is DatabaseAccessMode.WRITER:
            os.ftruncate(descriptor, 0)
            os.write(descriptor, f"pid={os.getpid()}\n".encode("ascii"))
            os.fsync(descriptor)
        with _PROCESS_WRITER_LEASE_GUARD:
            _PROCESS_WRITER_LEASES[descriptor] = (os.getpid(), access, lease)
        yield lease
    finally:
        with _PROCESS_WRITER_LEASE_GUARD:
            _PROCESS_WRITER_LEASES.pop(descriptor, None)
        if acquired:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


@contextmanager
def hold_formal_mutation(
    database: Path,
    *,
    project_root: Path,
    environ: Mapping[str, str] | None = None,
    home: Path | None = None,
    installed: InstalledWriterContract | None = None,
) -> Iterator[ResolvedDatabaseAccess]:
    """Resolve the installed formal database and hold its writer lock.

    Receipt-driven maintenance commands use this as their outermost boundary:
    the installed LaunchAgent contract is checked before receipt or SQLite I/O,
    and the same verified lock inode remains held for the entire mutation.
    """

    access = resolve_installed_database_access(
        DatabaseAccessMode.FORMAL_MUTATION,
        database=database,
        project_root=project_root,
        environ=environ,
        home=home,
        installed=installed,
    )
    with acquire_writer_lock(access):
        yield access


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--access",
        choices=tuple(mode.value for mode in DatabaseAccessMode),
        required=True,
    )
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--project-root", type=Path)
    parser.add_argument("--check", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    mode = DatabaseAccessMode(arguments.access)
    if not arguments.check:
        raise SystemExit("runtime database resolver supports only --check")
    if mode is DatabaseAccessMode.ISOLATED_CANDIDATE:
        access = resolve_isolated_candidate(arguments.db)
    else:
        if arguments.project_root is None:
            raise SystemExit("installed database access requires --project-root")
        access = resolve_installed_database_access(
            mode,
            database=arguments.db,
            project_root=arguments.project_root,
        )
        if mode is DatabaseAccessMode.WRITER:
            lock = observe_writer_lock(access)
            if lock["held"]:
                raise RuntimeDatabaseError("installed writer lock is already held")
    print(
        f"validated {access.access_mode.value} database identity "
        f"{access.database_identity.device}:{access.database_identity.inode}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
