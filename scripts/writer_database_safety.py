"""Side-effect-free contracts and shared offline database safety primitives.

The CLI state machines retain their checkpoints and recovery ownership.
Declaring a schema contract does not enable its migration implementation.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import os
import re
import stat
import subprocess
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
from pathlib import Path
from typing import Any, Iterator, Literal, Protocol


class OfflineContractError(RuntimeError):
    """No implemented operation matches the exact offline schema contract."""


@dataclass(frozen=True)
class SchemaIdentity:
    version: int
    migration: str


@dataclass(frozen=True)
class OfflineDatabaseContract:
    source: SchemaIdentity
    candidate: SchemaIdentity
    backup_receipt_schema: str
    migration_receipt_schema: str
    install_receipt_schema: str
    restore_receipt_schema: str
    allowed_differences_schema: str
    lock_payload: bytes


LEGACY_V15_V16 = OfflineDatabaseContract(
    source=SchemaIdentity(15, "spu-llm-assist"),
    candidate=SchemaIdentity(16, "remove-manual-review"),
    backup_receipt_schema="dcar-v16-offline-backup-v1",
    migration_receipt_schema="dcar-v16-offline-migration-v1",
    install_receipt_schema="dcar-writer-database-candidate-install-v1",
    restore_receipt_schema="dcar-writer-database-v15-restore-v1",
    allowed_differences_schema="dcar-v16-offline-allowed-differences-v1",
    lock_payload=b"dcar-v16-offline-migration-lock-v1\n",
)

MATRIX_V17_V18 = OfflineDatabaseContract(
    source=SchemaIdentity(17, "optional-account-phone"),
    candidate=SchemaIdentity(18, "matrix-roster-source-routing"),
    backup_receipt_schema="dcar-v18-offline-backup-v1",
    migration_receipt_schema="dcar-v18-offline-migration-v1",
    install_receipt_schema="dcar-writer-database-v18-install-v1",
    restore_receipt_schema="dcar-writer-database-v17-restore-v1",
    allowed_differences_schema="dcar-v18-offline-allowed-differences-v1",
    lock_payload=b"dcar-v18-offline-migration-lock-v1\n",
)

DUAL_V18_V19 = OfflineDatabaseContract(
    source=SchemaIdentity(18, "matrix-roster-source-routing"),
    candidate=SchemaIdentity(19, "dual-acquisition-profile-roster-v1"),
    backup_receipt_schema="dcar-v19-offline-backup-v1",
    migration_receipt_schema="dcar-v19-offline-migration-v1",
    install_receipt_schema="dcar-writer-database-v19-install-v1",
    restore_receipt_schema="dcar-writer-database-v18-restore-v1",
    allowed_differences_schema="dcar-v19-offline-allowed-differences-v1",
    lock_payload=b"dcar-v19-offline-migration-lock-v1\n",
)

INTEGRATED_V19_V20 = OfflineDatabaseContract(
    source=SchemaIdentity(19, "dual-acquisition-profile-roster-v1"),
    candidate=SchemaIdentity(20, "integrated-video-capture-v25"),
    backup_receipt_schema="dcar-v20-offline-backup-v1",
    migration_receipt_schema="dcar-v20-offline-migration-v1",
    install_receipt_schema="dcar-writer-database-v20-install-v1",
    restore_receipt_schema="dcar-writer-database-v19-restore-v1",
    allowed_differences_schema="dcar-v20-offline-allowed-differences-v1",
    lock_payload=b"dcar-v20-offline-migration-lock-v1\n",
)

OFFLINE_CONTRACTS = (LEGACY_V15_V16, MATRIX_V17_V18, DUAL_V18_V19, INTEGRATED_V19_V20)
IMPLEMENTED_MIGRATIONS = frozenset({(15, 16), (17, 18), (18, 19), (19, 20)})

# CLI operations can overlap in test harnesses or orchestration. A sealed
# per-call context keeps versioned receipts separate without changing
# process-wide version constants. Import and --help never inspect a database.
_ACTIVE_CONTRACT: ContextVar[OfflineDatabaseContract] = ContextVar(
    "writer_offline_contract", default=LEGACY_V15_V16,
)


def current_contract() -> OfflineDatabaseContract:
    return _ACTIVE_CONTRACT.get()


def requires_code_identity(contract: OfflineDatabaseContract) -> bool:
    """Return whether receipts must bind to the checked-out implementation."""

    require_implemented_contract(contract)
    return contract is not LEGACY_V15_V16


def _implemented_version_help(*, backup: bool) -> str:
    transitions = sorted(IMPLEMENTED_MIGRATIONS)
    if backup:
        sources = ", ".join(str(source) for source, _ in transitions)
        return f"verified backup requires exact --from one of: {sources}"
    pairs = ", ".join(f"{source}->{candidate}" for source, candidate in transitions)
    return f"offline migration requires an exact implemented transition: {pairs}"


def code_identity(project_root: Path) -> dict[str, str]:
    """Bind a new-contract receipt to the actual checked-out implementation.

    Only digests are returned; patches and untracked source contents never
    enter receipts. Release directories use a separately verified manifest
    at deployment, not an invented Git identity here.
    """
    def git(*arguments: str) -> bytes:
        result = subprocess.run(
            ["git", "-C", str(project_root), *arguments], check=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        return result.stdout

    try:
        head = git("rev-parse", "HEAD").decode("ascii").strip()
        patch = git("diff", "--binary", "HEAD", "--", ".")
        digest = hashlib.sha256(patch)
        for entry in sorted(git(
            "ls-files", "--others", "--exclude-standard", "-z", "--",
            "src", "scripts", "deploy", "app/web", "config",
        ).split(b"\0")):
            if not entry:
                continue
            path = project_root / os.fsdecode(entry)
            if path.is_symlink() or not path.is_file():
                raise OfflineContractError("untracked executable source is not a regular file")
            digest.update(entry + b"\0" + bytes.fromhex(sha256_file(path)))
        if git("rev-parse", "HEAD").decode("ascii").strip() != head:
            raise OfflineContractError("Git HEAD changed while identifying migration code")
        return {"git_head": head, "working_tree_sha256": digest.hexdigest()}
    except (subprocess.CalledProcessError, UnicodeError) as error:
        raise OfflineContractError("cannot identify the offline migration code") from error


def require_code_identity(value: object, project_root: Path) -> None:
    if not isinstance(value, dict) or value != code_identity(project_root):
        raise OfflineContractError("receipt code identity differs from the checked-out implementation")


@contextmanager
def using_contract(contract: OfflineDatabaseContract) -> Iterator[OfflineDatabaseContract]:
    require_implemented_contract(contract)
    token = _ACTIVE_CONTRACT.set(contract)
    try:
        yield contract
    finally:
        _ACTIVE_CONTRACT.reset(token)


def _receipt_contract_header(
    path: Path,
    expected_sha256: str,
    kind: Literal["backup", "migration", "install", "restore"],
) -> OfflineDatabaseContract:
    from v8.receipt_sizes import receipt_read_limit, validate_receipt_size
    allow_large = kind == "migration"
    limit = receipt_read_limit(allow_schema20_migration=allow_large)
    if re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is None:
        raise OfflineContractError("receipt SHA-256 must be exact lowercase hex")
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        identity = os.fstat(descriptor)
        if not stat.S_ISREG(identity.st_mode) or identity.st_nlink != 1:
            raise OfflineContractError("receipt must be a regular single-link file")
        if identity.st_size > limit:
            raise OfflineContractError("receipt is unexpectedly large")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            payload = handle.read(limit + 1)
        if hashlib.sha256(payload).hexdigest() != expected_sha256:
            raise OfflineContractError("receipt SHA-256 does not match expectation")
        value = json.loads(payload)
        if not isinstance(value, dict):
            raise OfflineContractError("receipt must be a JSON object")
        try:
            validate_receipt_size(value, len(payload), allow_schema20_migration=allow_large)
        except ValueError as error:
            raise OfflineContractError(str(error)) from error
        return contract_for_receipt(value.get("schema_version"), kind=kind)
    finally:
        os.close(descriptor)


def bind_operation_contract(
    kind: Literal["backup", "migration", "install", "restore"],
    *,
    error_type: type[RuntimeError],
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Select only an explicit from/to or a hash-bound versioned receipt."""

    def decorate(function: Callable[..., Any]) -> Callable[..., Any]:
        signature = inspect.signature(function)

        @wraps(function)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            arguments = signature.bind(*args, **kwargs)
            arguments.apply_defaults()
            values = arguments.arguments
            try:
                # The test boundary precedes even receipt I/O. Use the CLI's
                # configured default so its independently tested guard keeps
                # the same meaning after contract selection is factored out.
                if os.environ.get("DCAR_TEST_DENY_FORMAL_DB") == "1":
                    from v8.storage import DEFAULT_DB, is_formal_database_path

                    target = values.get("formal_database", values.get("source_database"))
                    if target is not None and is_formal_database_path(
                        Path(target),
                        formal_database=function.__globals__.get("DEFAULT_DB", DEFAULT_DB),
                    ):
                        raise OfflineContractError(
                            "test process attempted to open the formal DCar database"
                        )
                if kind in {"backup", "migration"}:
                    source = values["from_version"]
                    try:
                        contract = contract_for_versions(
                            source, values.get("to_version", source + 1),
                            require_implemented=False,
                        )
                    except OfflineContractError as error:
                        required = _implemented_version_help(backup=kind == "backup")
                        raise OfflineContractError(required) from error
                else:
                    prefix: Literal["migration", "backup"] = (
                        "migration" if kind == "install" else "backup"
                    )
                    contract = _receipt_contract_header(
                        Path(values[f"{prefix}_receipt"]),
                        values[f"expected_{prefix}_receipt_sha256"],
                        prefix,
                    )
                with using_contract(contract):
                    return function(*args, **kwargs)
            except (OfflineContractError, OSError, ValueError) as error:
                raise error_type(str(error)) from error

        return wrapped

    return decorate


class AllowedDifferencesVerifier(Protocol):
    """Recompute a version-specific lineage proof from source and candidate.

    The implementation must verify every preserved projection and every
    allowed change. It must not accept a caller-supplied receipt as proof.
    """

    def __call__(
        self,
        source: Path,
        candidate: Path,
        *,
        contract: OfflineDatabaseContract,
    ) -> Mapping[str, Any]: ...


def require_implemented_contract(
    contract: OfflineDatabaseContract,
) -> OfflineDatabaseContract:
    if contract not in OFFLINE_CONTRACTS:
        raise OfflineContractError("offline contract is not an exact sealed contract")
    if (contract.source.version, contract.candidate.version) not in IMPLEMENTED_MIGRATIONS:
        raise OfflineContractError(
            f"offline {contract.source.version}->{contract.candidate.version} "
            "contract is declared but its migration and lineage verifier "
            "are not implemented"
        )
    return contract


def contract_for_versions(
    source: int,
    candidate: int,
    *,
    require_implemented: bool = True,
) -> OfflineDatabaseContract:
    for contract in OFFLINE_CONTRACTS:
        if (contract.source.version, contract.candidate.version) == (source, candidate):
            return (
                require_implemented_contract(contract)
                if require_implemented else contract
            )
    raise OfflineContractError(f"unsupported offline schema transition {source}->{candidate}")


def contract_for_receipt(
    schema: object,
    *,
    kind: Literal["backup", "migration", "install", "restore"],
    require_implemented: bool = True,
) -> OfflineDatabaseContract:
    for contract in OFFLINE_CONTRACTS:
        if schema == getattr(contract, f"{kind}_receipt_schema"):
            return (
                require_implemented_contract(contract)
                if require_implemented else contract
            )
    raise OfflineContractError(f"unsupported {kind} receipt schema")


@dataclass(frozen=True)
class FileIdentity:
    device: int
    inode: int
    link_count: int
    mode: int
    size: int
    mtime_ns: int


class MigrationLockLease:
    """Keep final inode binding verification inside the commit boundary."""

    def __init__(
        self,
        *,
        identity: FileIdentity,
        verify_binding: Callable[[], None],
    ) -> None:
        self.identity = identity
        self._verify_binding = verify_binding
        self.commit_verified = False
        self.binding_failed = False

    def verify_for_commit(self) -> None:
        if self.commit_verified:
            return
        try:
            self._verify_binding()
        except BaseException:
            self.binding_failed = True
            raise
        self.commit_verified = True


def identity_from_stat(value: os.stat_result) -> FileIdentity:
    return FileIdentity(
        device=value.st_dev,
        inode=value.st_ino,
        link_count=value.st_nlink,
        mode=stat.S_IMODE(value.st_mode),
        size=value.st_size,
        mtime_ns=value.st_mtime_ns,
    )


def file_identity(path: Path) -> FileIdentity:
    return identity_from_stat(path.lstat())


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fsync_file(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def assert_lock_binding(
    path: Path,
    parent: Path,
    parent_descriptor: int,
    descriptor: int,
    *,
    error_type: type[RuntimeError],
) -> None:
    parent_fd_value = os.fstat(parent_descriptor)
    parent_path_value = parent.stat()
    if (parent_fd_value.st_dev, parent_fd_value.st_ino) != (
        parent_path_value.st_dev,
        parent_path_value.st_ino,
    ):
        raise error_type("migration lock parent identity changed")
    file_fd_value = os.fstat(descriptor)
    file_path_value = os.stat(
        path.name, dir_fd=parent_descriptor, follow_symlinks=False,
    )
    if (file_fd_value.st_dev, file_fd_value.st_ino) != (
        file_path_value.st_dev,
        file_path_value.st_ino,
    ):
        raise error_type("migration lock path identity changed")


def write_json_exclusive(
    path: Path,
    value: Mapping[str, Any],
    *,
    error_type: type[RuntimeError] = RuntimeError,
    sync_directory: Callable[[Path], None] = fsync_directory,
    on_created: Callable[[FileIdentity], None] | None = None,
) -> FileIdentity:
    payload = (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    created_identity = identity_from_stat(os.fstat(descriptor))
    try:
        if on_created is not None:
            on_created(created_identity)
        written = 0
        while written < len(payload):
            count = os.write(descriptor, payload[written:])
            if count <= 0:
                raise OSError("receipt write made no progress")
            written += count
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        current = file_identity(path)
        if (current.device, current.inode) != (
            created_identity.device,
            created_identity.inode,
        ):
            raise OSError("receipt path identity changed during write")
        sync_directory(path.parent)
        return current
    except BaseException as error:
        close_error: BaseException | None = None
        if descriptor >= 0:
            try:
                os.close(descriptor)
            except BaseException as exc:
                close_error = exc
            finally:
                descriptor = -1
        try:
            if (path.exists() or path.is_symlink()):
                current = file_identity(path)
                if (current.device, current.inode) == (
                    created_identity.device,
                    created_identity.inode,
                ):
                    path.unlink()
                    sync_directory(path.parent)
        except BaseException as cleanup_error:
            raise error_type(
                f"receipt write failed and cleanup was not durable: {cleanup_error}"
            ) from error
        if close_error is not None:
            raise error_type(
                f"receipt write failed while closing its file: {close_error}"
            ) from error
        raise
    finally:
        if descriptor >= 0:
            os.close(descriptor)
