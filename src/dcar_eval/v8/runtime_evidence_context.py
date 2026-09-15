"""One request's inheritance proof, prepared outside the SQLite write lock.

This is not an authority cache. Only the release's immutable inheritance result
is reused. Every use replays its database read set. All file generations are
fenced on transaction entry and before commit; account, activation, gate, route, budget and lease checks remain
in their original live callers. A changed preparation fails closed without a
cold verification in the transaction. No proof survives the boundary context.

Keep this module stdlib-only: the sealed bootstrap imports the release verifier
in an isolated namespace, where no paid-boundary context can exist.
"""
from __future__ import annotations

from .runtime_phase_timing import phase, timed

from contextlib import closing, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import sys
import threading
import uuid
from typing import Any, Iterator, Mapping
from types import MappingProxyType


class RuntimeEvidenceChanged(ValueError):
    error_code = "capture_authorization_blocked"


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeEvidenceChanged(message)


def _time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _now() -> str:
    # Match the existing paid-boundary clock's second precision. A subsecond
    # preparation timestamp would falsely reject a fresh same-second A/B time.
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _loaded_source_root() -> Path:
    return Path(__file__).resolve(strict=True).parents[3]


def _generation(path: Path) -> tuple[Any, ...]:
    # ctime is essential: in-place edits can restore size, inode and mtime.
    try:
        value = path.lstat()
        target = path.stat() if stat.S_ISLNK(value.st_mode) else value
        return tuple(getattr(item, key) for item in (value, target) for key in (
            "st_dev", "st_ino", "st_nlink", "st_mode", "st_uid", "st_gid", "st_size",
            "st_mtime_ns", "st_ctime_ns"))
    except FileNotFoundError:
        return ("missing",)


_OBSERVED: ContextVar[dict[Path, tuple[Any, ...]] | None] = ContextVar("runtime_inheritance_reads", default=None)


def _observe(path: Path) -> None:
    files = _OBSERVED.get()
    if files is None:
        return
    path = Path(os.path.abspath(path))
    current = _generation(path)
    _require(path not in files or files[path] == current, "Inheritance file changed during preparation")
    files[path] = current


def _audit(event: str, arguments: tuple[Any, ...]) -> None:
    if event != "open" or _OBSERVED.get() is None:
        return
    name, _mode, flags = arguments
    if isinstance(name, (str, bytes, os.PathLike)) and not flags & (os.O_WRONLY | os.O_RDWR):
        _observe(Path(os.fsdecode(name)))


sys.addaudithook(_audit)


class _Rows:
    def __init__(self, rows, description):
        self.rows, self.description, self.offset = rows, description, 0

    def fetchone(self):
        rows = self.fetchmany(1)
        return rows[0] if rows else None

    def fetchmany(self, size=1):
        rows = self.rows[self.offset:self.offset + size]
        self.offset += len(rows)
        return rows

    def fetchall(self):
        return self.fetchmany(len(self.rows))

    def __iter__(self):
        while (row := self.fetchone()) is not None:
            yield row


class _ReadSet:
    """Record the release verifier's actual live DB dependencies, never writes."""
    def __init__(self, connection):
        self.connection = connection
        self.queries = {}

    def execute(self, sql, parameters=()):
        _require(sql.lstrip().upper().startswith(("SELECT ", "PRAGMA ", "WITH ")),
                 "Inheritance preparation only accepts read statements")
        if sql.lstrip().upper().startswith("PRAGMA "):
            _require(re.fullmatch(r"\s*PRAGMA\s+(?:user_version|schema_version|database_list|foreign_keys|recursive_triggers|"
                r"(?:table_info|table_xinfo|index_list|index_info|index_xinfo|foreign_key_list)\s*\([^;=]+\))\s*;?\s*",
                sql, re.IGNORECASE) is not None, "Unrecognized inheritance PRAGMA")
        named = isinstance(parameters, Mapping)
        args = tuple(parameters.items()) if named else tuple(parameters)
        if _legacy_catalog_structure_read(sql, args, sys._getframe(1)):
            # Frozen ancestors still receive the exact row their own verifier
            # expects. Only its dependency is projected to the two predicates
            # that this identified function actually validates. The closed
            # read snapshot makes the two SELECTs observe the same state.
            cursor = self.connection.execute(sql, parameters)
            original = cursor.fetchall(), cursor.description
            key = _CATALOG_STRUCTURE_SQL, (), False
            if key not in self.queries:
                cursor = self.connection.execute(_CATALOG_STRUCTURE_SQL)
                self.queries[key] = cursor.fetchall(), cursor.description
            return _Rows(*original)
        key = sql, args, named
        if key not in self.queries:
            cursor = self.connection.execute(sql, parameters)
            self.queries[key] = cursor.fetchall(), cursor.description
        return _Rows(*self.queries[key])

    def __getattr__(self, name):
        _require(name not in {"cursor", "executescript", "executemany", "commit", "rollback"},
                 "Unrecorded inheritance SQL is forbidden")
        return getattr(self.connection, name)


_LEGACY_CATALOG_STRUCTURE_SQL = "SELECT revision,projection_depth FROM capture_catalog_revision WHERE id=1"
_CATALOG_STRUCTURE_SQL = ("SELECT (typeof(revision) IN ('integer','real') AND revision>=0) AS revision_valid,"
                          "projection_depth=0 AS projection_idle "
                          "FROM capture_catalog_revision WHERE id=1")
_LEGACY_SCHEMA_SHA256 = "9c1a71ce18995715279e9af95cdebbca2866a9061dfb1a741cd183b73be7ea04"


def _legacy_catalog_structure_read(sql, arguments, caller) -> bool:
    """Narrow bridge for the already source-verified S3/S5/S6 schema function.

    Each immutable ancestor imports its own schema module, so changing today's
    SELECT alone cannot repair its read set. Do not generalize this to SQL from
    another caller: a business proof may depend on the exact revision number.
    The caller's existing source verification and generation fences are still
    required; this digest grants neither inheritance nor operating authority.
    """
    if sql != _LEGACY_CATALOG_STRUCTURE_SQL or arguments or caller.f_code.co_name != "validate_structure":
        return False
    function = caller.f_globals.get("validate_structure")
    name = caller.f_globals.get("__file__")
    if getattr(function, "__code__", None) is not caller.f_code or not isinstance(name, str):
        return False
    path = Path(name)
    if path.name != "schema_v23.py" or caller.f_code.co_firstlineno != 60:
        return False
    try:
        return (path.resolve(strict=True) == Path(caller.f_code.co_filename).resolve(strict=True)
                and hashlib.sha256(path.read_bytes()).hexdigest() == _LEGACY_SCHEMA_SHA256)
    except OSError:
        return False


def _dependencies(value, files, manifests, seen):
    """Explicit receipt graph covers cached backups which do not emit open().

    This collection grants no validity. The unchanged full release verifier
    validates the receipt/source bytes below. Historical unused source trees
    are retained as receipt dependencies; source trees read by the verifier
    additionally receive their complete manifest and directory fence.
    """
    if isinstance(value, Mapping):
        if value.get("contract") == "writer-source-tree-v1":
            manifests[Path(value["source_root"])] = value
        name = value.get("path")
        if isinstance(name, str) and Path(name).is_absolute() and isinstance(value.get("sha256"), str):
            path = Path(name)
            if path not in seen:
                seen.add(path)
                files[path] = _generation(path)
                if path.suffix in {".sqlite3", ".sqlite", ".db"}:
                    # SQLite opens sidecars in C, without Python's open audit.
                    # A backup WAL/journal can change its read view even when
                    # the hashed main database file has not changed.
                    for suffix in ("-wal", "-journal"):
                        sidecar = Path(str(path) + suffix)
                        files[sidecar] = _generation(sidecar)
                if path.suffix == ".json" and path.is_file() and path.stat().st_size <= 16 * 1024 * 1024:
                    try:
                        nested = json.loads(path.read_bytes())
                    except (ValueError, UnicodeError):
                        nested = None
                    _require(files[path] == _generation(path), "Receipt changed while binding dependencies")
                    _dependencies(nested, files, manifests, seen)
        for child in value.values():
            _dependencies(child, files, manifests, seen)
    elif isinstance(value, (tuple, list)):
        for child in value:
            _dependencies(child, files, manifests, seen)


def _source_fence(root: Path, manifest: Mapping[str, Any]) -> dict[Path, tuple[Any, ...]]:
    git = root / ".git"
    _require(git.is_dir() and git.resolve(strict=True) == git
        and not (git / "commondir").exists() and not (git / "objects/info/alternates").exists(),
        "Paid inheritance requires independent sealed Git directories")
    result = {root: _generation(root)}
    for row in manifest["files"]:
        path = root / row["path"]
        result[path] = _generation(path)
        for parent in path.parents:
            if parent == root:
                break
            if parent.is_relative_to(root):
                result[parent] = _generation(parent)
    # Directory generations detect new and deleted files (including ignored
    # executable additions). Include Git objects/refs/config/index: running Git
    # again under the write lock would recreate the original bottleneck.
    for relative in (".git", "src", "scripts", "config", "deploy"):
        base = root / relative
        result[base] = _generation(base)
        for directory, folders, names in os.walk(base, followlinks=False):
            result[Path(directory)] = _generation(Path(directory))
            for name in folders:
                result[Path(directory) / name] = _generation(Path(directory) / name)
            if relative == ".git":
                for name in names:
                    path = Path(directory) / name
                    result[path] = _generation(path)
    return result


@timed("json.prepared_canonical")
def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


@dataclass(frozen=True, repr=False)
class PreparedInheritance:
    database: Path
    inode: tuple[int, int]
    source: Path
    build: str
    build_ref: str
    install_path: Path
    at: str
    environment: Mapping = field(repr=False)
    files: Mapping = field(repr=False)
    queries: tuple = field(repr=False)
    proof: str = field(repr=False)
    owner_thread: int
    schema_version: int = 23
    frozen_files: Mapping = field(default_factory=lambda: MappingProxyType({}), repr=False)
    critical_digests: Mapping = field(default_factory=lambda: MappingProxyType({}), repr=False)
    logical_at: str | None = None

    @timed("prepared.validate")
    def validate(self, connection, *, at: str, check_files: bool = True) -> None:
        import threading
        from .runtime_database import require_current_process_writer_lock
        with phase('prepared.bindings'):
            _require(connection.in_transaction and threading.get_ident() == self.owner_thread,
                     "Inheritance reuse requires its owned transaction boundary")
            _require(_time(at) >= _time(self.at), "Writer clock moved behind inheritance preparation")
            require_current_process_writer_lock(connection)
            path = Path(next(row[2] for row in connection.execute("PRAGMA database_list") if row[1] == "main")).resolve(strict=True)
            metadata = path.stat()
            _require(path == self.database and (metadata.st_dev, metadata.st_ino) == self.inode
                and dict(os.environ) == self.environment, "Writer installation or database changed after preparation")
        if check_files:
            with phase("prepared.files", files=len(self.files)):
                for name, expected in self.files.items():
                    _require(_generation(name) == expected, "Inheritance dependency changed after preparation")
        # No total_changes/TTL shortcut: exact reads also fence SAVEPOINT rollback.
        with phase("prepared.db_replay", queries=len(self.queries)):
            for (sql, args, named), (rows, description) in self.queries:
                parameters = dict(args) if named else args
                with phase("prepared.db_execute", sql_sha256=hashlib.sha256(sql.encode()).hexdigest()):
                    cursor = connection.execute(sql, parameters)
                with phase("prepared.db_fetch") as counts:
                    actual = [tuple(row) for row in cursor.fetchall()]
                    counts["rows"] = len(actual)
                with phase("prepared.db_compare"):
                    _require(cursor.description == description and actual == [tuple(row) for row in rows],
                             "Inheritance database proof changed after preparation")


_PREPARED: ContextVar[PreparedInheritance | None] = ContextVar("runtime_prepared_inheritance", default=None)
_BOUNDARY: ContextVar[Any] = ContextVar("runtime_inheritance_write_boundary", default=None)


def prepared_file_bytes(path: Path, *, private: bool = False, limit: int = 16 * 1024 * 1024) -> bytes | None:
    """Read this boundary's verified bytes, retaining a live fence on the path.

    A prepared value is a single request's snapshot, never a cross-request
    cache. Callers still validate their expected digest and business authority.
    Unknown files take their original verified read path.
    """
    prepared = _PREPARED.get()
    if prepared is None or prepared.schema_version != 24 or _BOUNDARY.get() is None:
        return None
    name = Path(os.path.abspath(path))
    body = prepared.frozen_files.get(name)
    if body is None:
        return None
    expected = prepared.files.get(name)
    _require(expected is not None and _generation(name) == expected,
             "Prepared runtime file changed")
    _require(len(body) <= limit and (not private or stat.S_IMODE(expected[3]) == 0o600),
             "Prepared runtime file does not meet the caller's contract")
    return body


def prepared_critical_inventory(source: Path, critical: Mapping[str, str]) -> bool:
    """The surrounding entry/exit fences protect the already hashed source."""
    prepared = _PREPARED.get()
    if prepared is None or prepared.schema_version != 24 or _BOUNDARY.get() is None:
        return False
    _require(source == prepared.source and dict(critical) == prepared.critical_digests,
             "Prepared critical code inventory changed")
    return True


def _freeze_current_files(files, critical, source):
    """Freeze small receipts and current executable bytes with fd identity CAS."""
    from .runtime_paths import _raw_file
    bodies = {}
    for path, expected in files.items():
        if expected == ("missing",) or not stat.S_ISREG(expected[3]):
            continue
        relative = path.relative_to(source).as_posix() if path.is_relative_to(source) else None
        if relative not in critical and path.suffix not in {".json", ".plist"}:
            continue
        # _raw_file verifies lstat -> opened fd -> lstat, ownership, size and
        # non-symlink/single-link identity; digest checks remain explicit.
        body = _raw_file(path, limit=16 * 1024 * 1024)
        _require(_generation(path) == expected, "File changed while freezing prepared bytes")
        if relative in critical:
            _require(hashlib.sha256(body).hexdigest() == critical[relative],
                     "Prepared critical file SHA differs from the loaded build")
        bodies[path] = body
    _require(all(source / name in bodies for name in critical),
             "Prepared snapshot omits loaded critical code")
    return MappingProxyType(bodies)


class _PathRootIndex:
    """Match native Path roots lexically, without resolving or reading paths.

    Separate anchors keep relative '.' from matching absolute paths. Components
    retain '..' and distinct names such as 'source'/'source-other', just as
    Path.is_relative_to does. A trie avoids comparing every path to every root.
    """
    def __init__(self, roots):
        self.anchors = {}
        for root in roots:
            node = self.anchors.setdefault(os.path.normcase(root.anchor), {})
            for part in root.parts[1 if root.anchor else 0:]:
                node = node.setdefault(os.path.normcase(part), {})
            node[None] = root

    def matching_roots(self, path):
        node = self.anchors.get(os.path.normcase(path.anchor))
        if node is None:
            return
        if None in node:
            yield node[None]
        for part in path.parts[1 if path.anchor else 0:]:
            node = node.get(os.path.normcase(part))
            if node is None:
                return
            if None in node:
                yield node[None]

    def contains(self, path):
        return next(self.matching_roots(path), None) is not None


def _observed_source_roots(roots, observed):
    index = _PathRootIndex(roots)
    return {root for name in observed for root in index.matching_roots(name)}


def _schema24_commit_files(files, manifests, source):
    """Historical source/Git facts were consumed by the closed proof snapshot.

    Current source files/directories/refs, receipts, authorization files and
    installation bindings remain fenced. Historical trees and content-addressed
    Git object traversal cannot affect bytes already consumed by this proof.
    They are fully verified again when the next request prepares its snapshot.
    """
    ancestors = tuple(root for root in manifests if root != source)
    git_objects = source / ".git" / "objects"
    excluded = _PathRootIndex((*ancestors, git_objects))
    return {name: generation for name, generation in files.items()
            if not excluded.contains(name)}


@contextmanager
def inheritance_boundary(connection):
    """Bind the one prepared proof to the following single A or B transaction."""
    prepared = _PREPARED.get()
    if prepared is None:
        yield
        return
    _require(_BOUNDARY.get() is None, "Paid inheritance write boundaries cannot be nested")
    token = _BOUNDARY.set(connection)
    try:
        with phase("prepared.boundary_entry"):
            prepared.validate(connection, at=_now())
        yield
        # A source/receipt change during the remaining live admission checks
        # rolls this transaction back, including a not-yet-committed send.
        with phase("prepared.boundary_exit"):
            prepared.validate(connection, at=_now())
    finally:
        _BOUNDARY.reset(token)


def reuse_inheritance(*, connection, build, build_ref, install_path, database, source, at):
    prepared = _PREPARED.get()
    if connection is None or prepared is None:
        return None
    # Never turn a changed prepared boundary into a cold proof under its lock.
    _require(connection is _BOUNDARY.get() and _canonical(build) == prepared.build and _canonical(build_ref) == prepared.build_ref
        and Path(install_path) == prepared.install_path and Path(database) == prepared.database
        and Path(source) == prepared.source, "Prepared inheritance belongs to another loaded build")
    # The outer boundary fences all files on entry and again before commit.
    # Intermediate reuse rechecks all DB inputs, never total_changes or a TTL.
    # A changed file after this check still aborts the enclosing transaction;
    # the physical provider call is strictly after that successful commit.
    with phase("prepared.reuse_db"):
        if prepared.logical_at is not None:
            # A local read scope retains the caller's explicit business clock.
            # The physical entry/exit clock must still follow preparation;
            # using the old business time for that check would reject every
            # preparation which crossed a second boundary.
            _require(at == prepared.logical_at, "Prepared logical scope time changed")
            validation_at = _now()
        else:
            validation_at = at or _now()
        prepared.validate(connection, at=validation_at, check_files=False)
    # Authorization time is checked afresh by the original live decision path.
    with phase("json.prepared_decode", bytes=len(prepared.proof)):
        return json.loads(prepared.proof)


_REQUEST_CONTRACT = "runtime-proof-preparation-request-v1"
_RESULT_CONTRACT = "runtime-proof-preparation-result-v1"


def _preparation_request(database: Path, *, logical_at: str | None) -> dict[str, Any]:
    path = Path(database).resolve(strict=True)
    metadata = path.stat()
    return {"contract": _REQUEST_CONTRACT, "request_id": uuid.uuid4().hex,
            "database": str(path), "inode": (metadata.st_dev, metadata.st_ino),
            "source": str(_loaded_source_root()), "environment": dict(os.environ),
            "logical_at": logical_at}


def _validate_request(request: Any) -> tuple[Path, Path, dict[str, str]]:
    _require(type(request) is dict and set(request) == {
        "contract", "request_id", "database", "inode", "source", "environment", "logical_at"},
        "Malformed inheritance preparation request")
    _require(request["contract"] == _REQUEST_CONTRACT
             and isinstance(request["request_id"], str)
             and re.fullmatch(r"[0-9a-f]{32}", request["request_id"]) is not None,
             "Inheritance preparation request identity differs")
    _require(isinstance(request["database"], str) and isinstance(request["source"], str)
             and type(request["inode"]) is tuple and len(request["inode"]) == 2
             and all(type(value) is int for value in request["inode"]),
             "Inheritance preparation paths or inode differ")
    environment = request["environment"]
    _require(type(environment) is dict and all(type(k) is str and type(v) is str
             for k, v in environment.items()) and environment == dict(os.environ),
             "Writer environment changed around inheritance preparation")
    logical_at = request["logical_at"]
    _require(logical_at is None or isinstance(logical_at, str), "Invalid logical preparation time")
    if logical_at is not None:
        _require(_time(logical_at).utcoffset() is not None, "Logical preparation time requires a timezone")
    path, source = Path(request["database"]), Path(request["source"])
    metadata = path.stat()
    _require(path.is_absolute() and path.resolve(strict=True) == path
             and (metadata.st_dev, metadata.st_ino) == request["inode"]
             and source == _loaded_source_root(),
             "Writer database or loaded source changed around inheritance preparation")
    return path, source, environment


def _build_prepared(request: dict[str, Any]) -> PreparedInheritance | None:
    """Perform the original complete verification using only closed readers."""
    from . import four_platform_flow_release as release
    path, expected_source, environment = _validate_request(request)
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as reader:
        try:
            reader.row_factory = sqlite3.Row
            reader.execute("PRAGMA foreign_keys=ON")
            reader.execute("PRAGMA recursive_triggers=ON")
            reader.execute("PRAGMA query_only=ON")
            schema_version = reader.execute("PRAGMA user_version").fetchone()[0]
            if schema_version not in {23, 24}:
                # The schema20–22 contracts remain unchanged.
                prepared = None
            else:
                metadata = path.stat()
                build_path = Path(environment["DCAR_LOADED_BUILD_RECEIPT"])
                install_path = Path(environment["DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT"])
                build_ref = release.reference(build_path)
                build = release.payload_at(build_ref, "sealed-build-receipt-v1")
                _require(environment["DCAR_LOADED_BUILD_ID"] == "sha256:" + build_ref["sha256"],
                         "Prepared build is not the loaded Writer")
                source = Path(build["source_root"])
                _require(source == Path(environment["DCAR_WRITER_SOURCE_ROOT"])
                    and source == expected_source, "Prepared source is not the loaded installed Writer")
                files, manifests = {}, {}
                _dependencies({"build": build_ref, "install": release.reference(install_path)}, files, manifests, set())
                from .runtime_database import load_installed_writer_contract
                installed = load_installed_writer_contract(required=True)
                _require(installed is not None and installed.database.resolve() == path,
                         "Prepared database differs from the installed Writer")
                files[installed.plist_path] = _generation(installed.plist_path)
                # Capture the complete source fence *before* byte verification.
                fences = {root: _source_fence(root, tree) for root, tree in manifests.items() if root.is_dir()}
                observed = {}
                token = _OBSERVED.set(observed)
                at = _now()
                reader.execute("BEGIN")
                inputs = _ReadSet(reader)
                try:
                    if schema_version == 24:
                        from . import duplicate_index_release as verifier
                    else:
                        verifier = release
                    from .pure_source_normalization import normalization_request
                    from .pure_source_inventory import inventory_request
                    with normalization_request(), inventory_request(manifests, observed, generation=_generation):
                        proof = verifier.verify_inheritance(build=build, build_ref=build_ref, install_path=install_path,
                            database=path, source=source, at=request["logical_at"] or at, connection=inputs)
                finally:
                    _OBSERVED.reset(token)
                # Include every actually read ancestor source. A cached backup
                # is covered by the explicit receipt graph regardless of open.
                observed_roots = _observed_source_roots(fences, observed)
                for root, fence in fences.items():
                    if root == source or root in observed_roots:
                        files.update(fence)
                for name, generation in observed.items():
                    _require(name not in files or files[name] == generation,
                             "Inheritance dependency changed across verification")
                    files[name] = generation
                _require(environment == dict(os.environ), "Writer environment changed during preparation")
                for name, expected in files.items():
                    _require(_generation(name) == expected, "Inheritance dependencies changed during preparation")
                critical, frozen = {}, MappingProxyType({})
                if schema_version == 24:
                    critical = dict(build["critical_files"])
                    files = _schema24_commit_files(files, manifests, source)
                    frozen = _freeze_current_files(files, critical, source)
                prepared = PreparedInheritance(path, (metadata.st_dev, metadata.st_ino), source,
                    _canonical(build), _canonical(build_ref), install_path, at, MappingProxyType(environment), MappingProxyType(files),
                    tuple((key, (tuple(tuple(row) for row in rows), description))
                        for key, (rows, description) in inputs.queries.items()), _canonical(proof), threading.get_ident(),
                    schema_version, frozen, MappingProxyType(critical), request["logical_at"])
        finally:
            reader.close()
    return prepared


def build_prepared_wire(request: dict[str, Any]) -> dict[str, Any]:
    """Spawn-worker entry: fresh proof, no SQLite handles or authority in IPC.

    The child does not acquire or validate the parent's Writer lease. Its
    result can only be consumed by an owned parent transaction, which replays
    the read set and all remaining live fences before and after use.
    """
    prepared = _build_prepared(request)
    _validate_request(request)
    payload = None
    if prepared is not None:
        payload = {"database": str(prepared.database), "inode": prepared.inode,
            "source": str(prepared.source), "build": prepared.build, "build_ref": prepared.build_ref,
            "install_path": str(prepared.install_path), "at": prepared.at,
            "logical_at": prepared.logical_at, "environment": dict(prepared.environment),
            "files": tuple((str(name), generation) for name, generation in prepared.files.items()),
            "queries": prepared.queries, "proof": prepared.proof,
            "schema_version": prepared.schema_version,
            "frozen_files": tuple((str(name), body) for name, body in prepared.frozen_files.items()),
            "critical_digests": dict(prepared.critical_digests)}
    return {"contract": _RESULT_CONTRACT, "request_id": request["request_id"], "prepared": payload}


def _prepared_from_wire(request: dict[str, Any], result: Any) -> PreparedInheritance | None:
    """Rebind worker data to this parent thread, never to the child's identity."""
    path, source, environment = _validate_request(request)
    _require(type(result) is dict and set(result) == {"contract", "request_id", "prepared"}
             and result["contract"] == _RESULT_CONTRACT and result["request_id"] == request["request_id"],
             "Inheritance preparation response belongs to another request")
    payload = result["prepared"]
    if payload is None:
        # None is the original legacy-schema result, never a failed-worker
        # fallback. Check it without a lease, write, or migration.
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as reader:
            reader.execute("PRAGMA query_only=ON")
            _require(reader.execute("PRAGMA user_version").fetchone()[0] not in {23, 24},
                     "Prepared proof is missing for the installed schema")
        return None
    _require(type(payload) is dict and set(payload) == {
        "database", "inode", "source", "build", "build_ref", "install_path", "at", "logical_at",
        "environment", "files", "queries", "proof", "schema_version", "frozen_files", "critical_digests"},
        "Malformed prepared inheritance response")
    _require(payload["database"] == str(path) and payload["inode"] == request["inode"]
             and payload["source"] == str(source) and payload["environment"] == environment
             and payload["logical_at"] == request["logical_at"]
             and payload["install_path"] == environment["DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT"]
             and type(payload["schema_version"]) is int and payload["schema_version"] in {23, 24},
             "Prepared response installation bindings differ")
    for key in ("build", "build_ref", "proof"):
        _require(isinstance(payload[key], str), "Prepared response JSON is invalid")
    build, build_ref = json.loads(payload["build"]), json.loads(payload["build_ref"])
    _require(type(build) is dict and type(build_ref) is dict
             and _canonical(build) == payload["build"] and _canonical(build_ref) == payload["build_ref"]
             and build.get("source_root") == str(source)
             and build_ref.get("path") == environment["DCAR_LOADED_BUILD_RECEIPT"]
             and environment["DCAR_LOADED_BUILD_ID"] == "sha256:" + str(build_ref.get("sha256")),
             "Prepared response build differs from the loaded Writer")
    _require(isinstance(payload["at"], str) and _time(payload["at"]).utcoffset() is not None,
             "Prepared response clock is invalid")
    _require(type(payload["files"]) is tuple and type(payload["frozen_files"]) is tuple
             and type(payload["queries"]) is tuple and type(payload["critical_digests"]) is dict,
             "Prepared response collection types differ")
    files, frozen = {}, {}
    for row in payload["files"]:
        _require(type(row) is tuple and len(row) == 2 and type(row[0]) is str
                 and Path(row[0]).is_absolute() and type(row[1]) is tuple,
                 "Prepared file generation is invalid")
        name, generation = Path(row[0]), row[1]
        _require(name not in files and (generation == ("missing",) or
                 (len(generation) == 18 and all(type(value) is int for value in generation))),
                 "Prepared file generation is duplicated or invalid")
        files[name] = generation
    for row in payload["frozen_files"]:
        _require(type(row) is tuple and len(row) == 2 and type(row[0]) is str
                 and type(row[1]) is bytes and len(row[1]) <= 16 * 1024 * 1024,
                 "Prepared frozen bytes are invalid")
        name, body = Path(row[0]), row[1]
        _require(name in files and name not in frozen, "Prepared frozen file is unbound or duplicated")
        frozen[name] = body
    critical = payload["critical_digests"]
    _require(all(type(k) is str and type(v) is str for k, v in critical.items())
             and (payload["schema_version"] != 24 or critical == build.get("critical_files")),
             "Prepared critical inventory differs from the loaded build")
    _validate_query_wire(payload["queries"])
    return PreparedInheritance(path, request["inode"], source, payload["build"], payload["build_ref"],
        Path(payload["install_path"]), payload["at"], MappingProxyType(dict(environment)),
        MappingProxyType(files), payload["queries"], payload["proof"], threading.get_ident(),
        payload["schema_version"], MappingProxyType(frozen), MappingProxyType(dict(critical)), payload["logical_at"])


def _validate_query_wire(queries: tuple) -> None:
    def scalar(value):
        return value is None or type(value) in {str, bytes, int, float, bool}
    for item in queries:
        _require(type(item) is tuple and len(item) == 2, "Malformed prepared query")
        key, value = item
        _require(type(key) is tuple and len(key) == 3 and type(key[0]) is str
                 and type(key[1]) is tuple and type(key[2]) is bool
                 and type(value) is tuple and len(value) == 2,
                 "Malformed prepared query binding")
        sql, args, named = key
        _require(sql.lstrip().upper().startswith(("SELECT ", "PRAGMA ", "WITH ")),
                 "Prepared query is not a read statement")
        _require(all((type(arg) is tuple and len(arg) == 2 and type(arg[0]) is str and scalar(arg[1]))
                 if named else scalar(arg) for arg in args), "Invalid prepared query arguments")
        rows, description = value
        _require(type(rows) is tuple and all(type(row) is tuple and all(scalar(cell) for cell in row)
                 for row in rows) and (description is None or (type(description) is tuple
                 and all(type(column) is tuple and len(column) == 7 and all(scalar(cell) for cell in column)
                         for column in description))), "Invalid prepared query rows or metadata")


@contextmanager
def prepare_inheritance(database: Path, *, enabled: bool = True, lane: str = "shared",
                        logical_at: str | None = None) -> Iterator[PreparedInheritance | None]:
    if not enabled or not os.environ.get("DCAR_LOADED_BUILD_ID"):
        yield None
        return
    _require(lane in {"shared", "local"}, "Unknown inheritance preparation lane")
    path = Path(database).resolve(strict=True)
    prepared = _PREPARED.get()
    if prepared is not None:
        # A work borrows only its same-thread proof between A/reserve/B;
        # never another logical scope or an active transaction boundary.
        _require(_BOUNDARY.get() is None and prepared.owner_thread == threading.get_ident()
                 and prepared.database == path and prepared.logical_at == logical_at,
                 "Prepared inheritance belongs to another work boundary, thread, database or logical time")
        yield prepared
        return
    request = _preparation_request(path, logical_at=logical_at)
    from . import runtime_proof_workers
    with phase("prepared.readonly_build", lane=lane):
        result = (runtime_proof_workers.prepare(request, lane=lane) if runtime_proof_workers.enabled()
                  else build_prepared_wire(request))
    prepared = _prepared_from_wire(request, result)
    # All readers are closed. No write transaction is held while awaiting a
    # process slot or its proof; the caller's existing soft budget keeps running.
    token = _PREPARED.set(prepared)
    try:
        yield prepared
    finally:
        _PREPARED.reset(token)
