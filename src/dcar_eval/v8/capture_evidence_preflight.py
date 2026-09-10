"""Per-boundary installed evidence preparation, with live dependency fencing.

The expensive receipt/archive proof runs on a closed-after-use WAL reader,
before either paid boundary acquires the Writer lock. Only that proof is reused:
its SQL inputs and file/source identities are checked again in the transaction.
Authorization, owner, drain, route and budget decisions are never cached here.
"""
from __future__ import annotations

import os
import importlib.util
import sqlite3
import re
import subprocess
import sys
from copy import deepcopy
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Mapping, cast

from .capture_authorizations import AuthorizationError
from .source_routing import parse_time
from .storage import PROJECT_ROOT, connect, live_wal_read_only_connections, now_utc


class EvidencePreflightChanged(AuthorizationError):
    """Discard a stale preparation; never run the cold proof under a write lock."""


def _version(path: Path) -> tuple[Any, ...]:
    try:
        value = path.lstat()
        resolved = path.resolve(strict=True)
        target = resolved.stat()
        return (str(resolved), *(getattr(value, key) for key in (
            "st_dev", "st_ino", "st_nlink", "st_mode", "st_uid", "st_gid", "st_size",
            "st_mtime_ns", "st_ctime_ns")), *(getattr(target, key) for key in (
            "st_dev", "st_ino", "st_nlink", "st_mode", "st_uid", "st_gid", "st_size",
            "st_mtime_ns", "st_ctime_ns")))
    except FileNotFoundError:
        return ("missing", str(path.resolve(strict=False)))


_READ_FILES: ContextVar[dict[Path, tuple[Any, ...]] | None] = ContextVar("capture_evidence_read_files", default=None)
_PREPARATION_AT: ContextVar[str | None] = ContextVar("capture_evidence_preparation_at", default=None)
_IMPORT_SOURCES: ContextVar[set[Path] | None] = ContextVar("capture_evidence_import_sources", default=None)


def evidence_time(actual: str) -> str:
    """One live-time anchor for nested proof readers, without changing history."""
    return _PREPARATION_AT.get() or actual


def observe_file(path: Path) -> None:
    """Also called by content-hash readers whose cache hit does not open a file."""
    files = _READ_FILES.get()
    if files is None:
        return
    path = Path(os.path.abspath(path))
    current = _version(path)
    if path in files and files[path] != current:
        raise EvidencePreflightChanged("Installed evidence changed during preparation")
    files[path] = current


def _audit_read(event: str, arguments: tuple[Any, ...]) -> None:
    if event != "open" or _READ_FILES.get() is None:
        return
    name, _mode, flags = arguments
    if isinstance(name, (str, bytes, os.PathLike)) and not flags & (os.O_WRONLY | os.O_RDWR):
        path = Path(os.path.abspath(os.fsdecode(name)))
        observe_file(path)
        # Distinguish source actually loaded by Python from source merely read
        # by a receipt validator. Only the former can supersede rejected .pyc.
        frame = sys._getframe(1)
        sources = _IMPORT_SOURCES.get()
        if (sources is not None and path.suffix == ".py" and frame.f_code.co_name == "get_data"
                and "importlib._bootstrap_external" in frame.f_code.co_filename):
            sources.add(path)


def _discard_unread_import_caches(files: dict[Path, tuple[Any, ...]]) -> None:
    # On a missing/stale .pyc, Python reads/compiles its source and rebuilds the
    # cache. That rejected cache is not executed. Valid/sourceless bytecode is
    # retained, even if another validator separately reads a same-named .py.
    sources = _IMPORT_SOURCES.get() or set()
    for path, version in list(files.items()):
        if path.suffix != ".pyc" or path.parent.name != "__pycache__":
            continue
        try:
            source = Path(importlib.util.source_from_cache(str(path)))
        except ValueError:
            continue
        if source in sources and source in files and files[source][0] != "missing" and _version(source) == files[source]:
            del files[path]


# Register once on module import; inactive threads pay only the ContextVar read.
# Unlike monkeypatching file readers, this includes nested archive/JSON readers.
sys.addaudithook(_audit_read)


def _source_identity(root: Path) -> tuple[Any, ...]:
    def git(*args: str) -> bytes:
        return subprocess.run(["git", "-C", str(root), *args], check=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10).stdout
    # Include the complete index representation (staged content/mode), branch,
    # HEAD and every untracked addition, not merely the sealed critical subset.
    index = git("ls-files", "--stage", "-z")
    names = git("ls-files", "--cached", "--others", "--exclude-standard", "-z")
    paths = sorted(set(os.fsdecode(name) for name in names.split(b"\0") if name))
    return (str(root.resolve(strict=True)), git("rev-parse", "HEAD", "HEAD^{tree}"),
            git("symbolic-ref", "--quiet", "HEAD"), index, names,
            tuple((name, _version(root / name)) for name in paths))


class _Rows:
    def __init__(self, rows: list[Any], description: Any):
        self._rows, self.description, self._offset = rows, description, 0

    def fetchone(self) -> Any:
        rows = self.fetchmany(1)
        return rows[0] if rows else None

    def fetchmany(self, size: int = 1) -> list[Any]:
        result = self._rows[self._offset:self._offset + size]
        self._offset += len(result)
        return result

    def fetchall(self) -> list[Any]:
        return self.fetchmany(len(self._rows))

    def __iter__(self) -> Iterator[Any]:
        while (row := self.fetchone()) is not None:
            yield row


class _ReadInputs:
    """One consistent read snapshot; duplicate selectors do not repeat scans."""
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection
        self.queries: dict[tuple[Any, ...], tuple[list[Any], Any]] = {}

    def execute(self, sql: str, parameters: Any = ()) -> _Rows:
        if not sql.lstrip().upper().startswith(("SELECT ", "PRAGMA ", "WITH ")):
            raise EvidencePreflightChanged("Installed evidence preparation must be read-only")
        if sql.lstrip().upper().startswith("PRAGMA ") and not re.fullmatch(
            r"\s*PRAGMA\s+(?:user_version|database_list|schema_version|(?:table_info|table_xinfo|index_list|index_info|foreign_key_list)\s*\([^;=]+\))\s*;?\s*",
            sql, re.IGNORECASE,
        ):
            raise EvidencePreflightChanged("Unrecognized installed evidence PRAGMA is forbidden")
        arguments = tuple(parameters.items()) if isinstance(parameters, dict) else tuple(parameters)
        key = (sql, arguments, isinstance(parameters, dict))
        if key not in self.queries:
            cursor = self.connection.execute(sql, parameters)
            self.queries[key] = (cursor.fetchall(), cursor.description)
        rows, description = self.queries[key]
        return _Rows(rows, description)

    def __getattr__(self, name: str) -> Any:
        # No cursor escape: all SQL dependencies must pass through execute.
        if name in {"cursor", "executescript", "executemany"}:
            raise EvidencePreflightChanged("Unrecorded installed evidence SQL is forbidden")
        return getattr(self.connection, name)


def _fresh_argument(value: Any, prepared_at: str, at: str) -> Any:
    if isinstance(value, str):
        try:
            if parse_time(value) == parse_time(prepared_at):
                return at
        except (ValueError, TypeError):
            pass
    return value


@dataclass
class _Prepared:
    database: Path
    database_inode: tuple[int, int]
    project: Path
    at: str
    environment: Mapping[str, str]
    files: dict[Path, tuple[Any, ...]]
    source: tuple[Any, ...]
    queries: Mapping[tuple[Any, ...], tuple[list[Any], Any]]
    evidence: dict[str, Any]
    checked: tuple[int, int, str] | None = field(default=None)

    def validate(self, connection: sqlite3.Connection, *, at: str) -> None:
        from .runtime_database import require_current_process_writer_lock
        if not connection.in_transaction:
            raise EvidencePreflightChanged("Prepared evidence requires the paid write transaction")
        if parse_time(at) < parse_time(self.at):
            raise EvidencePreflightChanged("Writer clock moved behind prepared evidence")
        require_current_process_writer_lock(connection)
        row = next(row for row in connection.execute("PRAGMA database_list") if row[1] == "main")
        path = Path(row[2]).resolve(strict=True)
        metadata = path.stat()
        if (path != self.database or (metadata.st_dev, metadata.st_ino) != self.database_inode
                or _environment() != self.environment or _source_identity(self.project) != self.source):
            raise EvidencePreflightChanged("Loaded writer, database or source changed after preparation")
        for name, expected in self.files.items():
            if _version(name) != expected:
                raise EvidencePreflightChanged("Installed evidence file changed after preparation")
        # A/B may mutate their own accounting between installed-proof calls.
        # Recheck after such writes; never reuse the mutable DB verdict across
        # transactions, threads or the A/B boundary.
        marker = (id(connection), connection.total_changes, at)
        if self.checked == marker:
            return
        for (sql, arguments, named), (rows, description) in self.queries.items():
            parameters = ({key: _fresh_argument(value, self.at, at) for key, value in arguments}
                          if named else tuple(_fresh_argument(value, self.at, at) for value in arguments))
            cursor = connection.execute(sql, parameters)
            if cursor.description != description or [tuple(row) for row in cursor.fetchall()] != [tuple(row) for row in rows]:
                raise EvidencePreflightChanged("Installed evidence database inputs changed after preparation")
        self.checked = marker


_PREPARED: ContextVar[_Prepared | None] = ContextVar("capture_installed_evidence_prepared", default=None)


def _environment() -> dict[str, str]:
    # Bind the whole process configuration, including installed-plist selectors.
    return dict(os.environ)


@contextmanager
def prepare_installed_evidence(database: Path, *, enabled: bool = True) -> Iterator[None]:
    from . import capture_release
    if not enabled or not os.environ.get("DCAR_LOADED_BUILD_ID"):
        yield
        return
    files: dict[Path, tuple[Any, ...]] = {}
    root = PROJECT_ROOT.resolve(strict=True)
    source, environment = _source_identity(root), _environment()
    path = database.resolve(strict=True)
    metadata = path.stat()
    at = now_utc()
    token = _READ_FILES.set(files)
    clock_token = _PREPARATION_AT.set(at)
    imports_token = _IMPORT_SOURCES.set(set())
    try:
        with live_wal_read_only_connections(), connect(path, read_only=True) as connection:
            connection.execute("BEGIN")
            inputs = _ReadInputs(connection)
            evidence = capture_release._installed_evidence_uncached(cast(sqlite3.Connection, inputs), at=at)
        if source != _source_identity(root) or environment != _environment():
            raise EvidencePreflightChanged("Writer source or configuration changed during preparation")
        _discard_unread_import_caches(files)
        for name, expected in files.items():
            if _version(name) != expected:
                raise EvidencePreflightChanged("Installed evidence changed during preparation")
    finally:
        _IMPORT_SOURCES.reset(imports_token)
        _PREPARATION_AT.reset(clock_token)
        _READ_FILES.reset(token)
    prepared = _Prepared(path, (metadata.st_dev, metadata.st_ino), root, at, environment,
                         files, source, inputs.queries, evidence)
    installed = _PREPARED.set(prepared)
    try:
        yield
    finally:
        _PREPARED.reset(installed)


@contextmanager
def evidence_boundary(connection: sqlite3.Connection) -> Iterator[None]:
    prepared = _PREPARED.get()
    if prepared is not None:
        # Do the unique SQL rechecks before the caller records claim/send time.
        prepared.checked = None
        prepared.validate(connection, at=now_utc())
    try:
        yield
    finally:
        if prepared is not None:
            prepared.checked = None


def installed_evidence(connection: sqlite3.Connection, *, at: str,
                       maintenance_only: bool) -> dict[str, Any] | None:
    prepared = _PREPARED.get()
    if prepared is None:
        return None
    if maintenance_only:
        raise EvidencePreflightChanged("Paid preparation cannot substitute maintenance evidence")
    prepared.validate(connection, at=at)
    from . import capture_release, provider_budget
    from .profile_activations import activation_at
    if activation_at(connection, at) != prepared.evidence["active"]:
        raise EvidencePreflightChanged("Current activation changed after evidence preparation")
    if capture_release.forward_recovery._route() != prepared.evidence["manifest"]:
        raise EvidencePreflightChanged("Provider route configuration changed after preparation")
    capture_release._release_tools().validate_storage_policy(
        prepared.evidence["storage_policy"],
        require_forecast=prepared.evidence["deployment"]["status"] == "accepted")
    provider_budget.require_storage_ready(connection)
    return deepcopy(prepared.evidence)
