#!/usr/bin/env python3
"""Seal and verify the immutable R0 build and runtime-root receipts.

The command is intentionally narrow.  It does not stop services, checkpoint or
migrate SQLite, create backups, or change an installed LaunchAgent.  An
operator must first stop the writer and publisher and prepare a private,
project-external evidence parent.  ``seal`` then proves that the installed
writer lock is available, reads the formal database in read-only mode, and
creates three durable receipts without overwriting an existing path.

The default remains the pre-migration schema18 source.  An explicit
``--formal-schema 19`` seals a same-schema code update against the installed
schema19 database and the previous build/migration/install receipt chain.
``--allow-working-tree`` additionally binds all nonignored untracked files and
retains private binary patches and file contents outside the project. Verification
uses the recorded mode; it never treats that working tree as a clean commit.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import sqlite3
import stat
import subprocess
import sys
import tarfile
from collections.abc import Iterator, Mapping, Sequence
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCRIPT_ROOT = Path(__file__).resolve().parent
SOURCE_ROOT = SCRIPT_ROOT.parent
PACKAGE_ROOT = SOURCE_ROOT / "src" / "dcar_eval"
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

import writer_database_safety as database_safety  # noqa: E402

from v8.runtime_database import (  # noqa: E402
    DatabaseAccessMode,
    InstalledWriterContract,
    ResolvedDatabaseAccess,
    acquire_writer_lock,
    load_installed_writer_contract,
    resolve_installed_database_access,
)
from v8.storage import SCHEMA_VERSION, LATEST_SCHEMA_VERSION  # noqa: E402


RUNTIME_ROOT_CONTRACT = "runtime-root-binding-v1"
TEST_RESULTS_CONTRACT = "test-results-v1"
SEALED_BUILD_CONTRACT = "sealed-build-receipt-v1"
RUNTIME_ROOT_FILENAME = f"{RUNTIME_ROOT_CONTRACT}.json"
TEST_RESULTS_FILENAME = f"{TEST_RESULTS_CONTRACT}.json"
SEALED_BUILD_FILENAME = f"{SEALED_BUILD_CONTRACT}.json"
WORKING_TREE_CONTRACT = "working-tree-source-v1"
WORKING_TREE_ARCHIVE = f"{WORKING_TREE_CONTRACT}.tar"
EXPECTED_CODE_SCHEMA = 19
EXPECTED_FORMAL_SCHEMA = 18
EXPECTED_FORMAL_MIGRATION = "matrix-roster-source-routing"
EXPECTED_TARGET_MIGRATION = "dual-acquisition-profile-roster-v1"
EXPECTED_REPORT_VERSION = "dcar-content-operations-report-v8.9"
EXPECTED_RULE_VERSION = "evaluation-v9"
EXPECTED_EVIDENCE_VERSION = "evidence-v2"
MAX_RECEIPT_BYTES = 8 * 1024 * 1024
REQUIRED_TEST_RESULTS = frozenset(
    {"backend", "frontend", "lint", "typecheck", "ruff", "mypy"}
)

ROOT_PATHS = {
    "raw": Path("data/cache/v8/raw_responses"),
    "media": Path("data/cache/v8/media"),
    "reports": Path("reports/runs/v8"),
    "runtime": Path("runtime"),
    "app_data": Path("app/data"),
}

LEGACY_CRITICAL_FILES = (
    Path("scripts/seal_r0_receipts.py"),
    Path("scripts/writer_database_safety.py"),
    Path("scripts/migrate_v8_schema.py"),
    Path("scripts/install_writer_database_candidate.py"),
    Path("scripts/restore_writer_database_backup.py"),
    Path("src/dcar_eval/v8/storage.py"),
    Path("src/dcar_eval/v8/runtime_database.py"),
    Path("src/dcar_eval/v8/schema_v19.py"),
    Path("src/dcar_eval/v8/profile_control.py"),
    Path("src/dcar_eval/v8/paid_drain.py"),
    Path("src/dcar_eval/v8/pipeline.py"),
    Path("deploy/macos/run_writer_worker.sh"),
    Path("deploy/macos/publish_snapshot.py"),
    Path("config/report_contract_v8_9.json"),
)
PRE_ACCOUNT_STATUS_CRITICAL_FILES = LEGACY_CRITICAL_FILES + tuple(Path(f"src/dcar_eval/v8/{name}.py") for name in (
    "automatic_scope", "scheduler", "api", "scan_receipts", "forward_recovery", "provider_budget", "transport_runner", "transport_accounting",
))
ACCOUNT_STATUS_CRITICAL_FILES = tuple(Path(f"src/dcar_eval/v8/{name}.py") for name in (
    "account_operating_status", "account_operating_receipts", "statistics_scope",
    "operations", "report_inputs", "spu_audience", "system_roster",
))
CRITICAL_FILES = PRE_ACCOUNT_STATUS_CRITICAL_FILES + ACCOUNT_STATUS_CRITICAL_FILES
V20_CRITICAL_FILES = CRITICAL_FILES + (
    Path("scripts/v20_release_contract.py"), Path("scripts/build_server_snapshot.py"),
) + tuple(Path(f"src/dcar_eval/v8/{name}.py") for name in (
    "schema_v20", "capture_planning", "capture_runtime", "capture_quality", "durable_runs",
    "metric_field_facts", "usage_settlements", "raw_archive", "local_raw_retention", "transport_tail",
))


V20_LEGACY_CRITICAL_FILES = V20_CRITICAL_FILES
V20_CRITICAL_FILES += (Path("src/dcar_eval/v8/capture_code_successor.py"),
                       Path("src/dcar_eval/v8/account_code_successor.py"))

class R0ReceiptError(RuntimeError):
    """The R0 evidence cannot be sealed or no longer verifies."""


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def _schema_contract(formal_schema: int, code_schema: int = EXPECTED_CODE_SCHEMA) -> dict[str, object]:
    if type(formal_schema) is not int or type(code_schema) is not int or (formal_schema, code_schema) not in {(18, 19), (19, 19), (19, 20), (20, 20)}:
        raise R0ReceiptError("schema pair must be explicitly 18->19, 19->19, 19->20 or 20->20")
    migrations = {18: EXPECTED_FORMAL_MIGRATION, 19: EXPECTED_TARGET_MIGRATION, 20: "integrated-video-capture-v25"}
    result: dict[str, object] = {
        "code_schema": code_schema,
        "formal_schema": formal_schema,
        "transition": f"{formal_schema}-to-{code_schema}",
        "source_migration": migrations[formal_schema],
        "target_migration": migrations[code_schema],
    }
    if formal_schema == code_schema:
        result["operation"] = "code_update"
    return result


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _sha256_file(path: Path) -> str:
    return database_safety.sha256_file(path)


def _identity(path: Path, *, directory: bool) -> dict[str, object]:
    lexical = Path(path).expanduser()
    if lexical.is_symlink():
        raise R0ReceiptError(f"receipt identity path must not be a symlink: {lexical}")
    if directory and not lexical.is_dir():
        raise R0ReceiptError(f"required receipt directory is missing: {lexical}")
    if not directory and not lexical.is_file():
        raise R0ReceiptError(f"required receipt file is missing: {lexical}")
    canonical = lexical.resolve(strict=True)
    value = canonical.stat()
    expected = stat.S_ISDIR(value.st_mode) if directory else stat.S_ISREG(value.st_mode)
    unsafe_links = value.st_nlink < 1 if directory else value.st_nlink != 1
    if not expected or value.st_uid != os.geteuid() or unsafe_links:
        raise R0ReceiptError(f"receipt identity path is unsafe: {lexical}")
    return {
        "path": str(canonical),
        "device": int(value.st_dev),
        "inode": int(value.st_ino),
        "nlink": int(value.st_nlink),
        "uid": int(value.st_uid),
        "gid": int(value.st_gid),
        "mode": stat.S_IMODE(value.st_mode),
    }


def _file_record(path: Path, *, include_hash: bool) -> dict[str, object]:
    result = _identity(path, directory=False)
    value = path.resolve(strict=True).stat()
    result["size"] = int(value.st_size)
    if include_hash:
        result["sha256"] = _sha256_file(path.resolve(strict=True))
    return result


def _parse_test_results(specifications: Sequence[str]) -> dict[str, Path]:
    results: dict[str, Path] = {}
    for specification in specifications:
        name, separator, raw_path = specification.partition("=")
        if (
            not separator
            or not name
            or not raw_path
            or not all(
                character.isalnum() or character in {"-", "_"} for character in name
            )
            or name in results
        ):
            raise R0ReceiptError(
                "test results must be unique NAME=LOG_PATH specifications"
            )
        results[name] = Path(raw_path).expanduser()
    missing = sorted(REQUIRED_TEST_RESULTS - results.keys())
    if missing:
        raise R0ReceiptError(f"required test results are missing: {','.join(missing)}")
    return results


def _test_log_record(name: str, path: Path) -> dict[str, object]:
    marker = f"DCAR_TEST_RESULT name={name} exit=0".encode("ascii")
    lexical = path.expanduser()
    if lexical.is_symlink():
        raise R0ReceiptError(f"test result log must not be a symlink: {lexical}")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(lexical, flags)
    except OSError as error:
        raise R0ReceiptError(
            f"test result log is missing or unsafe: {lexical}"
        ) from error
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_uid != os.geteuid()
            or metadata.st_nlink != 1
            or metadata.st_size <= 0
        ):
            raise R0ReceiptError(
                "test result log must be non-empty, current-user, "
                f"single-link, and 0600: {lexical}"
            )
        digest = hashlib.sha256()
        marker_count = 0
        last_line = b""
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            for line in handle:
                digest.update(line)
                last_line = line.removesuffix(b"\n").removesuffix(b"\r")
                if last_line == marker:
                    marker_count += 1
            final_metadata = os.fstat(handle.fileno())
        try:
            current = os.lstat(lexical)
        except OSError as error:
            raise R0ReceiptError(
                f"test result log identity changed while reading it: {lexical}"
            ) from error
        if not os.path.samestat(metadata, final_metadata) or not os.path.samestat(
            metadata, current
        ):
            raise R0ReceiptError(
                f"test result log identity changed while reading it: {lexical}"
            )
        if marker_count != 1 or last_line != marker:
            raise R0ReceiptError(
                f"test result log must end with one unique passing marker for {name}"
            )
        return {
            "path": str(Path(lexical).resolve(strict=True)),
            "sha256": digest.hexdigest(),
            "bytes": int(metadata.st_size),
            "device": int(metadata.st_dev),
            "inode": int(metadata.st_ino),
            "nlink": int(metadata.st_nlink),
            "uid": int(metadata.st_uid),
            "gid": int(metadata.st_gid),
            "mode": stat.S_IMODE(metadata.st_mode),
            "status": "passed",
            "exit_code": 0,
            "marker": marker.decode("ascii"),
        }
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _test_results_payload(
    project_root: Path,
    *,
    paths: Mapping[str, Path],
    git_record: Mapping[str, object] | None = None,
) -> dict[str, object]:
    git_value = dict(git_record or _git_record(project_root))
    return {
        "status": "passed",
        "git": git_value if git_value.get("mode") == WORKING_TREE_CONTRACT else {
            "head": git_value["head"], "tree": git_value["tree"]},
        "required_results": sorted(REQUIRED_TEST_RESULTS),
        "results": {
            name: _test_log_record(name, paths[name]) for name in sorted(paths)
        },
    }


def _verify_test_results_payload(
    project_root: Path, payload: Mapping[str, object]
) -> dict[str, object]:
    results = payload.get("results")
    if not isinstance(results, dict) or not all(
        isinstance(name, str) and isinstance(record, dict)
        for name, record in results.items()
    ):
        raise R0ReceiptError("test results receipt shape is invalid")
    if not REQUIRED_TEST_RESULTS.issubset(results):
        raise R0ReceiptError("test results receipt omits a required result")
    try:
        paths = {name: Path(record["path"]) for name, record in results.items()}
    except (KeyError, TypeError) as error:
        raise R0ReceiptError("test results receipt path is invalid") from error
    git_value = payload.get("git")
    if not isinstance(git_value, dict):
        raise R0ReceiptError("test results Git identity is invalid")
    from v8.capture_code_successor import historical_test_git
    historical = historical_test_git(project_root, git_value)
    current = _test_results_payload(project_root, paths=paths, git_record=historical or _git_record(
        project_root, allow_working_tree=_working_tree_mode(git_value)))
    if dict(payload) != current:
        raise R0ReceiptError("test result, Git identity, or log evidence drifted")
    return current


def _entry_kind(value: os.stat_result) -> str:
    if stat.S_ISREG(value.st_mode):
        return "file"
    if stat.S_ISDIR(value.st_mode):
        return "directory"
    if stat.S_ISLNK(value.st_mode):
        return "symlink"
    return "other"


def _walk_metadata(root: Path) -> Iterator[tuple[str, os.stat_result]]:
    pending = [root]
    while pending:
        current = pending.pop()
        try:
            children = sorted(os.scandir(current), key=lambda entry: entry.name)
        except OSError as error:
            raise R0ReceiptError(f"cannot inventory runtime root: {current}") from error
        directories: list[Path] = []
        for child in children:
            child_path = Path(child.path)
            try:
                value = child.stat(follow_symlinks=False)
            except OSError as error:
                raise R0ReceiptError(
                    f"cannot stat runtime-root entry: {child_path}"
                ) from error
            relative = child_path.relative_to(root).as_posix()
            yield relative, value
            if stat.S_ISDIR(value.st_mode) and not stat.S_ISLNK(value.st_mode):
                directories.append(child_path)
        pending.extend(reversed(directories))


def _inventory(root: Path, *, recursive: bool = True) -> dict[str, object]:
    root = root.resolve(strict=True)
    counts = {"regular_files": 0, "directories": 0, "symlinks": 0, "other": 0}
    logical_bytes = 0
    digest = hashlib.sha256()
    entries = (
        _walk_metadata(root)
        if recursive
        else (
            (entry.name, entry.stat(follow_symlinks=False))
            for entry in sorted(os.scandir(root), key=lambda value: value.name)
            # Only the shallow project inventory excludes Git internals and
            # ignored test output. Recursive business-root evidence stays exact.
            if entry.name not in {".git", "tmp"}
        )
    )
    for relative, value in entries:
        kind = _entry_kind(value)
        if kind == "file":
            counts["regular_files"] += 1
            logical_bytes += int(value.st_size)
        elif kind == "directory":
            counts["directories"] += 1
        elif kind == "symlink":
            counts["symlinks"] += 1
        else:
            counts["other"] += 1
        digest.update(
            _canonical_json(
                [
                    relative,
                    kind,
                    int(value.st_size),
                    int(value.st_mtime_ns),
                    stat.S_IMODE(value.st_mode),
                    int(value.st_ino),
                ]
            )
            + b"\n"
        )
    return {
        **counts,
        "logical_bytes": logical_bytes,
        "metadata_manifest_sha256": digest.hexdigest(),
    }


def _git(*arguments: str, project_root: Path) -> bytes:
    try:
        result = subprocess.run(
            ["git", "-C", str(project_root), *arguments],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as error:
        raise R0ReceiptError("cannot inspect the sealed Git checkout") from error
    return result.stdout


def _working_tree_mode(git_record: Mapping[str, object]) -> bool:
    mode = git_record.get("mode")
    if mode is not None and mode != WORKING_TREE_CONTRACT:
        raise R0ReceiptError("unsupported sealed Git mode")
    return mode == WORKING_TREE_CONTRACT


def _source_patches(project_root: Path) -> dict[str, bytes]:
    return {
        "staged.patch": _git("diff", "--binary", "--no-ext-diff", "--no-textconv", "--cached", "HEAD", "--", ".", project_root=project_root),
        "unstaged.patch": _git("diff", "--binary", "--no-ext-diff", "--no-textconv", "--", ".", project_root=project_root),
    }


def _untracked_source(project_root: Path, name: str) -> tuple[Path, os.stat_result]:
    relative = Path(name)
    path = project_root / relative
    if (relative.is_absolute() or not name or any(part in {".", "..", ".git"} for part in relative.parts)
            or any(character in name for character in ("\x00", "\r", "\n", "\\"))
            or path.is_symlink() or path.resolve(strict=True) != path):
        raise R0ReceiptError("untracked source path is unsafe")
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1 or metadata.st_uid != os.geteuid():
        raise R0ReceiptError("untracked source must be an owned single-link regular file")
    return path, metadata


def _working_tree_manifest(project_root: Path) -> dict[str, object]:
    patches = _source_patches(project_root)
    files = []
    for entry in sorted(_git("ls-files", "--others", "--exclude-standard", "-z", project_root=project_root).split(b"\0")):
        if not entry:
            continue
        name = os.fsdecode(entry)
        path, metadata = _untracked_source(project_root, name)
        files.append({"path": name, "sha256": _sha256_file(path), "bytes": metadata.st_size,
                      "mode": stat.S_IMODE(metadata.st_mode)})
    return {"patches": {name: {"sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)}
                        for name, body in patches.items()}, "untracked_files": files}


def _git_record(project_root: Path, *, allow_working_tree: bool = False) -> dict[str, object]:
    status = _git(
        "status", "--porcelain=v1", "--untracked-files=all", project_root=project_root
    )
    if status and not allow_working_tree:
        raise R0ReceiptError("R0 seal requires a completely clean Git checkout")
    head = _git("rev-parse", "HEAD", project_root=project_root).decode("ascii").strip()
    tree = (
        _git("rev-parse", "HEAD^{tree}", project_root=project_root)
        .decode("ascii")
        .strip()
    )
    branch = (
        _git("symbolic-ref", "--quiet", "--short", "HEAD", project_root=project_root)
        .decode("utf-8")
        .strip()
    )
    if not branch:
        raise R0ReceiptError("R0 seal requires a named Git branch")
    try:
        code_identity = database_safety.code_identity(project_root)
    except database_safety.OfflineContractError as error:
        raise R0ReceiptError(str(error)) from error
    if code_identity.get("git_head") != head:
        raise R0ReceiptError("Git HEAD changed while computing code identity")
    result: dict[str, object] = {
        "head": head,
        "tree": tree,
        "branch": branch,
        "status_porcelain_sha256": hashlib.sha256(status).hexdigest(),
        "code_identity": code_identity,
    }
    if allow_working_tree:
        result.update(mode=WORKING_TREE_CONTRACT, working_tree=_working_tree_manifest(project_root))
        if _git("status", "--porcelain=v1", "--untracked-files=all", project_root=project_root) != status:
            raise R0ReceiptError("Git status changed while computing source identity")
    return result


def _critical_files(project_root: Path, paths: Sequence[Path] = CRITICAL_FILES) -> dict[str, str]:
    result: dict[str, str] = {}
    if set(paths) not in (set(CRITICAL_FILES), set(PRE_ACCOUNT_STATUS_CRITICAL_FILES),
                          set(LEGACY_CRITICAL_FILES), set(V20_CRITICAL_FILES)):
        raise R0ReceiptError("sealed critical file inventory is invalid")
    for relative in paths:
        path = project_root / relative
        if path.is_symlink() or not path.is_file():
            raise R0ReceiptError(
                f"critical build file is missing or unsafe: {relative}"
            )
        result[relative.as_posix()] = _sha256_file(path)
    return result


def _write_source_archive(project_root: Path, path: Path, git_record: Mapping[str, object]) -> None:
    """Private reconstruction evidence: HEAD + staged patch + unstaged patch + files."""
    manifest = git_record["working_tree"]
    assert isinstance(manifest, dict)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        with tarfile.open(fileobj=stream, mode="w", format=tarfile.PAX_FORMAT) as archive:
            for name, body in _source_patches(project_root).items():
                if manifest["patches"][name] != {"sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)}:
                    raise R0ReceiptError("tracked source changed before archival")
                info = tarfile.TarInfo(name)
                info.size, info.mode = len(body), 0o600
                archive.addfile(info, io.BytesIO(body))
            for item in manifest["untracked_files"]:
                source, metadata = _untracked_source(project_root, item["path"])
                if metadata.st_size != item["bytes"] or stat.S_IMODE(metadata.st_mode) != item["mode"]:
                    raise R0ReceiptError("untracked source changed before archival")
                with source.open("rb") as handle:
                    info = tarfile.TarInfo("untracked/" + item["path"])
                    info.size, info.mode = item["bytes"], item["mode"]
                    archive.addfile(info, handle)
        stream.flush()
        os.fsync(stream.fileno())
    _source_archive_record(path, git_record)


def _source_archive_record(path: Path, git_record: Mapping[str, object]) -> dict[str, object]:
    record = _file_record(path, include_hash=True)
    if record["mode"] != 0o600:
        raise R0ReceiptError("source archive must have mode 0600")
    manifest = git_record["working_tree"]
    assert isinstance(manifest, dict)
    expected = {name: {**value, "mode": 0o600} for name, value in manifest["patches"].items()}
    expected.update({"untracked/" + item["path"]: item for item in manifest["untracked_files"]})
    seen = set()
    try:
        with tarfile.open(path, "r:") as archive:
            for member in archive:
                value = expected.get(member.name)
                if (member.name in seen or value is None or not member.isfile()
                        or member.size != value["bytes"] or member.mode != value["mode"]):
                    raise R0ReceiptError("source archive inventory differs")
                handle = archive.extractfile(member)
                assert handle is not None
                digest = hashlib.sha256()
                with handle:
                    while chunk := handle.read(1024 * 1024):
                        digest.update(chunk)
                if digest.hexdigest() != value["sha256"]:
                    raise R0ReceiptError("source archive content differs")
                seen.add(member.name)
    except (OSError, tarfile.TarError) as error:
        raise R0ReceiptError("source archive is unreadable") from error
    if seen != set(expected) or record != _file_record(path, include_hash=True):
        raise R0ReceiptError("source archive identity or inventory differs")
    return record


def _report_contract(project_root: Path) -> dict[str, object]:
    path = project_root / "config/report_contract_v8_9.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise R0ReceiptError("report v8.9 contract is unreadable") from error
    if (
        not isinstance(value, dict)
        or value.get("report_version") != EXPECTED_REPORT_VERSION
        or value.get("rule_version") != EXPECTED_RULE_VERSION
        or value.get("evidence_version") != EXPECTED_EVIDENCE_VERSION
    ):
        raise R0ReceiptError("report v8.9 contract identity differs")
    return {
        "path": str(path.resolve(strict=True)),
        "sha256": _sha256_file(path),
        "report_version": value["report_version"],
        "rule_version": value["rule_version"],
        "evidence_version": value["evidence_version"],
    }


def _installed_record(contract: InstalledWriterContract) -> dict[str, object]:
    environment = contract.payload.get("EnvironmentVariables")
    if not isinstance(environment, dict):
        raise R0ReceiptError("installed writer environment is invalid")
    selected_environment = {
        key: environment.get(key)
        for key in (
            "DCAR_PROJECT_ROOT",
            "DCAR_V8_DB",
            "DCAR_WRITER_LOCK",
            "DCAR_V8_REPORTS_ROOT",
        )
    }
    return {
        "label": contract.payload.get("Label"),
        "working_directory": str(contract.project_root),
        "program_arguments": contract.payload.get("ProgramArguments"),
        "environment": selected_environment,
        "plist": _file_record(contract.plist_path, include_hash=True),
        "database": _identity(contract.database, directory=False),
        "database_parent": _identity(contract.database.parent, directory=True),
        "writer_lock": _identity(contract.writer_lock, directory=False),
    }


def _database_holders(paths: Sequence[Path]) -> list[str]:
    command = Path("/usr/sbin/lsof")
    if not command.is_file():
        command = Path("/usr/bin/lsof")
    if not command.is_file():
        raise R0ReceiptError("lsof is required to prove the formal database is idle")
    existing = [str(path) for path in paths if path.exists() or path.is_symlink()]
    if not existing:
        return []
    result = subprocess.run(
        [str(command), "-nP", "-Fpcfn", "--", *existing],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode not in {0, 1}:
        raise R0ReceiptError("lsof could not inspect the formal database")
    if result.returncode == 1:
        return []
    return [line for line in result.stdout.splitlines() if line]


def _require_no_database_holders(database: Path) -> None:
    candidates = [
        database,
        Path(f"{database}-wal"),
        Path(f"{database}-shm"),
        Path(f"{database}-journal"),
    ]
    holders = _database_holders(candidates)
    if holders:
        raise R0ReceiptError("formal database or sidecar still has an open holder")


def _sidecar_record(database: Path) -> dict[str, object]:
    result: dict[str, object] = {}
    for suffix in ("-wal", "-shm"):
        path = Path(f"{database}{suffix}")
        key = suffix.removeprefix("-")
        if path.exists() or path.is_symlink():
            result[key] = _file_record(path, include_hash=True)
        else:
            result[key] = {"present": False}
    journal = Path(f"{database}-journal")
    if journal.exists() or journal.is_symlink():
        raise R0ReceiptError("formal database has a rollback journal")
    result["journal"] = {"present": False}
    return result


def _database_record(
    database: Path, *, formal_schema: int = EXPECTED_FORMAL_SCHEMA, code_schema: int = EXPECTED_CODE_SCHEMA,
) -> dict[str, object]:
    schema_contract = _schema_contract(formal_schema, code_schema)
    database_before = _file_record(database, include_hash=True)
    sidecars_before = _sidecar_record(database)
    wal_before = sidecars_before["wal"]
    if isinstance(wal_before, dict) and int(wal_before.get("size", 0)) != 0:
        raise R0ReceiptError("formal database has an uncheckpointed WAL")
    # A plain mode=ro connection to a WAL database may create empty -wal/-shm
    # files even after the writer has stopped.  R0 validation must be genuinely
    # non-mutating, so require a checkpointed source and bypass WAL locking.
    uri = f"{database.resolve(strict=True).as_uri()}?mode=ro&immutable=1"
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=30)
        try:
            connection.execute("PRAGMA query_only=ON")
            user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            quick = [str(row[0]) for row in connection.execute("PRAGMA quick_check")]
            integrity = [
                str(row[0]) for row in connection.execute("PRAGMA integrity_check")
            ]
            foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()
            migration = connection.execute(
                "SELECT version,name FROM schema_migrations ORDER BY version DESC LIMIT 1"
            ).fetchone()
        finally:
            connection.close()
    except sqlite3.Error as error:
        raise R0ReceiptError("formal database read-only validation failed") from error
    if (
        user_version != formal_schema
        or migration != (formal_schema, schema_contract["source_migration"])
        or quick != ["ok"]
        or integrity != ["ok"]
        or foreign_keys
    ):
        raise R0ReceiptError(
            f"formal database is not a clean schema{formal_schema} source"
        )
    result = _file_record(database, include_hash=True)
    sidecars = _sidecar_record(database)
    if result != database_before or sidecars != sidecars_before:
        raise R0ReceiptError(
            "formal database or sidecar changed during read-only validation"
        )
    result.update(
        {
            "user_version": user_version,
            "migration": {"version": int(migration[0]), "name": str(migration[1])},
            "quick_check": quick,
            "integrity_check": integrity,
            "foreign_key_error_count": len(foreign_keys),
            "sidecars": sidecars,
        }
    )
    return result


def _resolve_formal(
    project_root: Path, *, home: Path | None
) -> tuple[InstalledWriterContract, ResolvedDatabaseAccess]:
    try:
        installed = load_installed_writer_contract(required=True, home=home)
        assert installed is not None
        access = resolve_installed_database_access(
            DatabaseAccessMode.FORMAL_MUTATION,
            database=installed.database,
            project_root=project_root,
            environ={},
            home=home,
            installed=installed,
        )
    except (OSError, RuntimeError) as error:
        raise R0ReceiptError(str(error)) from error
    return installed, access


def _runtime_payload(
    project_root: Path,
    *,
    installed: InstalledWriterContract,
    formal_schema: int = EXPECTED_FORMAL_SCHEMA,
    code_schema: int = EXPECTED_CODE_SCHEMA,
) -> dict[str, object]:
    _require_no_database_holders(installed.database)
    formal_database = _database_record(installed.database, formal_schema=formal_schema, code_schema=code_schema)
    _require_no_database_holders(installed.database)
    roots: dict[str, object] = {
        "project": {
            "identity": _identity(project_root, directory=True),
            "inventory": _inventory(project_root, recursive=False),
        }
    }
    for name, relative in ROOT_PATHS.items():
        root = project_root / relative
        roots[name] = {
            "identity": _identity(root, directory=True),
            "inventory": _inventory(root),
        }
    return {
        "action": "retain",
        "mutation_counts": {
            "moved_files": 0,
            "copied_bytes": 0,
            "deleted_files": 0,
        },
        "project_root": str(project_root),
        "roots": roots,
        "installed_runtime": _installed_record(installed),
        "formal_database": formal_database,
    }


def _envelope(
    contract_version: str, payload: Mapping[str, object]
) -> dict[str, object]:
    return {
        "contract_version": contract_version,
        "created_at": _utc_now(),
        "payload": dict(payload),
        "payload_sha256": _digest(payload),
    }


def _validate_envelope(value: object, *, contract_version: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {
        "contract_version",
        "created_at",
        "payload",
        "payload_sha256",
    }:
        raise R0ReceiptError("R0 receipt envelope shape is invalid")
    if value.get("contract_version") != contract_version:
        raise R0ReceiptError("R0 receipt contract differs")
    created_at = value.get("created_at")
    try:
        datetime.fromisoformat(str(created_at).replace("Z", "+00:00"))
    except ValueError as error:
        raise R0ReceiptError("R0 receipt timestamp is invalid") from error
    payload = value.get("payload")
    if not isinstance(payload, dict) or value.get("payload_sha256") != _digest(payload):
        raise R0ReceiptError("R0 receipt payload SHA-256 differs")
    return payload


def _write_exclusive(path: Path, value: Mapping[str, object]) -> None:
    payload = (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
        + b"\n"
    )
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError as error:
        raise R0ReceiptError(f"refusing to overwrite R0 receipt: {path}") from error
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(path, 0o600)
        value_stat = path.lstat()
        if (
            not stat.S_ISREG(value_stat.st_mode)
            or stat.S_IMODE(value_stat.st_mode) != 0o600
            or value_stat.st_uid != os.geteuid()
            or value_stat.st_nlink != 1
        ):
            raise R0ReceiptError("new R0 receipt has an unsafe identity")
        directory_descriptor = os.open(
            path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0),
        )
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    except BaseException:
        # Preserve an uncertain file for audit.  O_EXCL prevents a later run
        # from silently replacing evidence that may already be durable.
        raise


def _read_private_json(path: Path, *, allow_schema20_migration: bool = False) -> dict[str, Any]:
    from v8.receipt_sizes import receipt_read_limit, validate_receipt_size
    limit = receipt_read_limit(allow_schema20_migration=allow_schema20_migration)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise R0ReceiptError(f"R0 receipt is missing or unsafe: {path}") from error
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_uid != os.geteuid()
            or metadata.st_nlink != 1
            or metadata.st_size > limit
        ):
            raise R0ReceiptError("R0 receipt permissions or identity are unsafe")
        current = path.stat()
        if not os.path.samestat(metadata, current):
            raise R0ReceiptError("R0 receipt identity changed while opening it")
        chunks: list[bytes] = []
        remaining = limit + 1
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        if len(payload) > limit:
            raise R0ReceiptError("R0 receipt is unexpectedly large")
    finally:
        os.close(descriptor)
    try:
        value = json.loads(payload)
    except (UnicodeError, ValueError) as error:
        raise R0ReceiptError("R0 receipt JSON is invalid") from error
    if not isinstance(value, dict):
        raise R0ReceiptError("R0 receipt JSON is not an object")
    try:
        validate_receipt_size(value, len(payload), allow_schema20_migration=allow_schema20_migration)
    except ValueError as error:
        raise R0ReceiptError(str(error)) from error
    return value


def _read_receipt(path: Path, *, contract_version: str) -> dict[str, Any]:
    return _validate_envelope(
        _read_private_json(path), contract_version=contract_version
    )


def _external_receipt(
    path: Path, project_root: Path, *, allow_schema20_migration: bool = False
) -> tuple[dict[str, Any], dict[str, str]]:
    path = path.expanduser()
    if (
        not path.is_absolute()
        or path.is_symlink()
        or path != path.resolve(strict=True)
        or path == project_root
        or project_root in path.parents
    ):
        raise R0ReceiptError("postmigration receipt must be canonical and project-external")
    value = _read_private_json(path, allow_schema20_migration=allow_schema20_migration)
    return value, {"path": str(path), "sha256": _sha256_file(path)}


def _postmigration_lineage(
    project_root: Path,
    *,
    installed: InstalledWriterContract,
    install_receipt: Path | None,
    target_schema: int = 19,
) -> dict[str, object]:
    """Bind a code-only update to real prior receipts, never invent an install."""

    if install_receipt is None:
        raise R0ReceiptError(f"schema{target_schema} install receipt is required")
    if target_schema not in {19, 20}:
        raise R0ReceiptError("install lineage requires exact schema19 or schema20")
    source_schema = target_schema - 1
    pair = _schema_contract(source_schema, target_schema)
    try:
        environment = installed.payload["EnvironmentVariables"]
        if not isinstance(environment, dict):
            raise R0ReceiptError("installed writer environment is invalid")
        previous_path = Path(environment["DCAR_LOADED_BUILD_RECEIPT"])
        previous, previous_ref = _external_receipt(previous_path, project_root)
        previous = _validate_envelope(previous, contract_version=SEALED_BUILD_CONTRACT)
        previous_schema = previous["schema_contract"]["formal_schema"]
        if (
            previous.get("status") != "succeeded"
            or previous["schema_contract"] != _schema_contract(previous_schema, previous["schema_contract"]["code_schema"])
            or previous["installed_runtime"]["database"]["path"] != str(installed.database)
        ):
            raise R0ReceiptError("previous loaded build receipt binding differs")
        install, install_ref = _external_receipt(install_receipt, project_root)
        if (
            install["schema_version"] != f"dcar-writer-database-v{target_schema}-install-v1"
            or install["status"] != "installed"
            or install["formal_database"] != str(installed.database)
            or install["installed"]["validation"] != {
                "schema_version": target_schema,
                "schema_migration": pair["target_migration"],
                "quick_check": "ok",
                "integrity_check": "ok",
                "foreign_key_violation_count": 0,
            }
        ):
            raise R0ReceiptError(f"schema{target_schema} install receipt binding differs")
        current = _identity(installed.database, directory=False)
        if any(
            install["installed"]["file"][key] != current[key]
            for key in ("device", "inode")
        ):
            raise R0ReceiptError("installed database identity differs from install receipt")
        migration, migration_ref = _external_receipt(
            Path(install["migration_receipt"]["path"]), project_root,
            allow_schema20_migration=target_schema == 20,
        )
        if (
            migration_ref["sha256"] != install["expected"]["migration_receipt_sha256"]
            or migration_ref["sha256"] != install["migration_receipt"]["file"]["sha256"]
        ):
            raise R0ReceiptError("migration receipt SHA-256 differs from install receipt")
        if (
            migration["schema_version"] != f"dcar-v{target_schema}-offline-migration-v1"
            or migration["status"] != "candidate_ready"
            or migration["from_version"] != source_schema
            or migration["to_version"] != target_schema
            or migration["from_migration"] != pair["source_migration"]
            or migration["to_migration"] != pair["target_migration"]
            or migration["formal_source"]["path"] != str(installed.database)
            or migration["code_identity"] != install["code_identity"]
            or migration["candidate"]["file"]["sha256"] != install["expected"]["candidate_sha256"]
            or migration["candidate"]["file"]["sha256"] != install["installed"]["file"]["sha256"]
        ):
            raise R0ReceiptError("migration and install receipt bindings differ")
        if previous_schema == source_schema:
            if previous["git"]["code_identity"] != install["code_identity"]:
                raise R0ReceiptError("previous build does not bind migration code")
        else:
            prior = previous["postmigration_lineage"]
            if prior["install_receipt"] != install_ref or prior["migration_receipt"] != migration_ref:
                raise R0ReceiptError("previous code update has a different install lineage")
    except (KeyError, TypeError, ValueError, OSError) as error:
        raise R0ReceiptError("postmigration receipt lineage is missing or invalid") from error
    return {
        "previous_build_receipt": previous_ref,
        "install_receipt": install_ref,
        "migration_receipt": migration_ref,
    }


def _build_payload(
    project_root: Path,
    *,
    installed: InstalledWriterContract,
    runtime_receipt_path: Path,
    test_results_receipt_path: Path,
    formal_schema: int = EXPECTED_FORMAL_SCHEMA,
    code_schema: int = EXPECTED_CODE_SCHEMA,
    install_receipt: Path | None = None,
    candidate_database: Path | None = None,
    migration_receipt: Path | None = None,
    deployment_id: str | None = None,
    allow_working_tree: bool = False,
    critical_paths: Sequence[Path] | None = None,
    code_successor_plan: Path | None = None,
) -> dict[str, object]:
    if (code_schema == 19 and SCHEMA_VERSION != 19) or (code_schema == 20 and LATEST_SCHEMA_VERSION != 20):
        raise R0ReceiptError("loaded code does not implement the selected schema")
    runtime_payload = _read_receipt(
        runtime_receipt_path, contract_version=RUNTIME_ROOT_CONTRACT
    )
    schema_contract = _schema_contract(formal_schema, code_schema)
    if (
        runtime_payload["formal_database"]["user_version"] != formal_schema
        or runtime_payload["formal_database"]["migration"] != {
            "version": formal_schema,
            "name": schema_contract["source_migration"],
        }
    ):
        raise R0ReceiptError("formal schema selection differs from runtime receipt")
    test_results_payload = _read_receipt(
        test_results_receipt_path, contract_version=TEST_RESULTS_CONTRACT
    )
    _verify_test_results_payload(project_root, test_results_payload)
    result: dict[str, object] = {
        "status": "succeeded",
        "git": _git_record(project_root, allow_working_tree=allow_working_tree),
        "critical_files": _critical_files(project_root, critical_paths or (V20_CRITICAL_FILES if code_schema == 20 else CRITICAL_FILES)),
        "schema_contract": schema_contract,
        "report_contract": _report_contract(project_root),
        "installed_runtime": _installed_record(installed),
        "runtime_root_receipt": {
            "path": str(runtime_receipt_path.resolve(strict=True)),
            "sha256": _sha256_file(runtime_receipt_path),
            "payload_sha256": _digest(runtime_payload),
        },
        "test_results_receipt": {
            "path": str(test_results_receipt_path.resolve(strict=True)),
            "sha256": _sha256_file(test_results_receipt_path),
            "payload_sha256": _digest(test_results_payload),
        },
    }
    if allow_working_tree:
        git_record = result["git"]
        assert isinstance(git_record, dict)
        if test_results_payload.get("git") != git_record:
            raise R0ReceiptError("working-tree tests do not bind the sealed source")
        result["source_archive"] = _source_archive_record(runtime_receipt_path.parent / WORKING_TREE_ARCHIVE, git_record)
    if formal_schema in {19, 20}:
        result["postmigration_lineage"] = _postmigration_lineage(
            project_root, installed=installed, install_receipt=install_receipt, target_schema=formal_schema,
        )
    if code_schema == 20:
        if not allow_working_tree or "source_archive" not in result:
            raise R0ReceiptError("schema20 requires a hash-bound source reconstruction archive")
        from v20_release_contract import ReleaseContractError, validate_deployment_receipt, validate_migration_candidate

        selected = candidate_database if formal_schema == 19 else installed.database
        if selected is None:
            raise R0ReceiptError("19->20 sealing requires an explicit candidate database")
        try:
            if formal_schema == 19:
                if migration_receipt is None:
                    raise R0ReceiptError("19->20 sealing requires the real offline migration receipt")
                deployment = validate_migration_candidate(source=installed.database, candidate=selected,
                                                          receipt=migration_receipt, project_root=project_root)
            else:
                with closing(sqlite3.connect(f"{selected.resolve(strict=True).as_uri()}?mode=ro&immutable=1", uri=True)) as connection, connection:
                    connection.row_factory = sqlite3.Row
                    if code_successor_plan is not None:
                        from v8.capture_code_successor import _ref, using_plan
                        plan_reference = _ref(code_successor_plan)
                        with using_plan(connection, plan_reference, project_root=project_root, at=_utc_now()) as checked:
                            deployment = checked["deployment"]
                            current_archive = result["source_archive"]
                            assert isinstance(current_archive, dict)
                            if checked["plan"]["source_archive"]["sha256"] != current_archive["sha256"]:
                                raise R0ReceiptError("successor plan binds another source archive")
                        result["code_successor_plan"] = plan_reference
                    else:
                        deployment = validate_deployment_receipt(connection, deployment_id=deployment_id, project_root=project_root)
            if deployment["status"] == "failed":
                raise R0ReceiptError("failed deployment evidence cannot seal a build")
            source_archive = result["source_archive"]
            assert isinstance(source_archive, dict)
            if formal_schema == 20 and code_successor_plan is None and deployment["evidence"]["source_archive"]["sha256"] != source_archive["sha256"]:
                raise R0ReceiptError("deployment evidence binds a different source archive")
            result["deployment_readiness"] = deployment
        except (ReleaseContractError, sqlite3.Error) as error:
            raise R0ReceiptError(str(error)) from error
    return result


def _private_evidence_parent(evidence_dir: Path, project_root: Path) -> Path:
    if not evidence_dir.is_absolute():
        raise R0ReceiptError("evidence directory must be absolute")
    if evidence_dir.exists() or evidence_dir.is_symlink():
        raise R0ReceiptError("evidence directory already exists")
    parent = evidence_dir.parent
    identity = _identity(parent, directory=True)
    if identity["mode"] != 0o700:
        raise R0ReceiptError("evidence parent must have mode 0700")
    canonical_parent = parent.resolve(strict=True)
    if canonical_parent == project_root or project_root in canonical_parent.parents:
        raise R0ReceiptError("evidence directory must stay outside the project")
    return canonical_parent


def _create_evidence_dir(evidence_dir: Path, parent: Path) -> None:
    try:
        os.mkdir(evidence_dir, 0o700)
    except FileExistsError as error:
        raise R0ReceiptError("evidence directory already exists") from error
    value = evidence_dir.lstat()
    if (
        not stat.S_ISDIR(value.st_mode)
        or stat.S_IMODE(value.st_mode) != 0o700
        or value.st_uid != os.geteuid()
        or value.st_nlink < 2
    ):
        raise R0ReceiptError("new evidence directory has an unsafe identity")
    descriptor = os.open(
        parent,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@contextmanager
def _formal_lease(
    project_root: Path, *, home: Path | None
) -> Iterator[tuple[InstalledWriterContract, ResolvedDatabaseAccess]]:
    installed, access = _resolve_formal(project_root, home=home)
    try:
        with acquire_writer_lock(access):
            yield installed, access
    except RuntimeError as error:
        if isinstance(error, R0ReceiptError):
            raise
        raise R0ReceiptError(str(error)) from error


def seal(
    *,
    project_root: Path,
    evidence_dir: Path,
    expected_head: str,
    test_results: Sequence[str],
    home: Path | None,
    formal_schema: int = EXPECTED_FORMAL_SCHEMA,
    code_schema: int = EXPECTED_CODE_SCHEMA,
    install_receipt: Path | None = None,
    candidate_database: Path | None = None,
    migration_receipt: Path | None = None,
    deployment_id: str | None = None,
    allow_working_tree: bool = False,
    code_successor_plan: Path | None = None,
) -> dict[str, str]:
    _schema_contract(formal_schema, code_schema)
    allow_working_tree = allow_working_tree or code_schema == 20
    if formal_schema == 18 and install_receipt is not None:
        raise R0ReceiptError("install receipt requires explicit formal schema19")
    project_root = project_root.expanduser().resolve(strict=True)
    evidence_dir = evidence_dir.expanduser()
    git_before = _git_record(project_root, allow_working_tree=allow_working_tree)
    if git_before["head"] != expected_head:
        raise R0ReceiptError("expected sealed Git HEAD differs")
    parent = _private_evidence_parent(evidence_dir, project_root)
    test_paths = _parse_test_results(test_results)
    tests_before = _test_results_payload(
        project_root, paths=test_paths, git_record=git_before
    )
    with _formal_lease(project_root, home=home) as (installed, _access):
        runtime_payload = _runtime_payload(
            project_root, installed=installed, formal_schema=formal_schema, code_schema=code_schema,
        )
        if formal_schema in {19, 20}:
            _postmigration_lineage(
                project_root, installed=installed, install_receipt=install_receipt, target_schema=formal_schema,
            )
        if _git_record(project_root, allow_working_tree=allow_working_tree) != git_before:
            raise R0ReceiptError("Git identity changed while gathering R0 evidence")
        if (
            _test_results_payload(project_root, paths=test_paths, git_record=git_before)
            != tests_before
        ):
            raise R0ReceiptError("test result log changed while gathering R0 evidence")
        _create_evidence_dir(evidence_dir, parent)
        if allow_working_tree:
            _write_source_archive(project_root, evidence_dir / WORKING_TREE_ARCHIVE, git_before)
        runtime_path = evidence_dir / RUNTIME_ROOT_FILENAME
        _write_exclusive(
            runtime_path, _envelope(RUNTIME_ROOT_CONTRACT, runtime_payload)
        )
        test_results_path = evidence_dir / TEST_RESULTS_FILENAME
        _write_exclusive(
            test_results_path, _envelope(TEST_RESULTS_CONTRACT, tests_before)
        )
        build_payload = _build_payload(
            project_root,
            installed=installed,
            runtime_receipt_path=runtime_path,
            test_results_receipt_path=test_results_path,
            formal_schema=formal_schema,
            code_schema=code_schema, candidate_database=candidate_database, migration_receipt=migration_receipt, deployment_id=deployment_id,
            install_receipt=install_receipt,
            allow_working_tree=allow_working_tree,
            code_successor_plan=code_successor_plan,
        )
        if build_payload["git"] != git_before:
            raise R0ReceiptError(
                "Git identity changed before sealing the build receipt"
            )
        build_path = evidence_dir / SEALED_BUILD_FILENAME
        _write_exclusive(build_path, _envelope(SEALED_BUILD_CONTRACT, build_payload))
    verify(
        project_root=project_root,
        evidence_dir=evidence_dir,
        home=home,
        formal_schema=formal_schema,
        code_schema=code_schema, candidate_database=candidate_database, migration_receipt=migration_receipt, deployment_id=deployment_id,
        install_receipt=install_receipt,
    )
    return {
        "runtime_root_receipt": str(runtime_path),
        "runtime_root_receipt_sha256": _sha256_file(runtime_path),
        "test_results_receipt": str(test_results_path),
        "test_results_receipt_sha256": _sha256_file(test_results_path),
        "sealed_build_receipt": str(build_path),
        "sealed_build_receipt_sha256": _sha256_file(build_path),
    }


def verify(
    *, project_root: Path, evidence_dir: Path, home: Path | None,
    formal_schema: int = EXPECTED_FORMAL_SCHEMA,
    code_schema: int = EXPECTED_CODE_SCHEMA,
    install_receipt: Path | None = None,
    candidate_database: Path | None = None,
    migration_receipt: Path | None = None,
    deployment_id: str | None = None,
    code_successor_plan: Path | None = None,
) -> dict[str, object]:
    _schema_contract(formal_schema, code_schema)
    if formal_schema == 18 and install_receipt is not None:
        raise R0ReceiptError("install receipt requires explicit formal schema19")
    project_root = project_root.expanduser().resolve(strict=True)
    evidence_dir = evidence_dir.expanduser().resolve(strict=True)
    directory = _identity(evidence_dir, directory=True)
    if directory["mode"] != 0o700:
        raise R0ReceiptError("evidence directory must have mode 0700")
    runtime_path = evidence_dir / RUNTIME_ROOT_FILENAME
    test_results_path = evidence_dir / TEST_RESULTS_FILENAME
    build_path = evidence_dir / SEALED_BUILD_FILENAME
    runtime_payload = _read_receipt(
        runtime_path, contract_version=RUNTIME_ROOT_CONTRACT
    )
    if runtime_payload["formal_database"]["user_version"] != formal_schema:
        raise R0ReceiptError("formal schema selection differs from runtime receipt")
    test_results_payload = _read_receipt(
        test_results_path, contract_version=TEST_RESULTS_CONTRACT
    )
    _verify_test_results_payload(project_root, test_results_payload)
    build_payload = _read_receipt(build_path, contract_version=SEALED_BUILD_CONTRACT)
    git_value = build_payload.get("git")
    critical = build_payload.get("critical_files")
    if not isinstance(git_value, dict) or not isinstance(critical, dict):
        raise R0ReceiptError("sealed build Git or critical file identity is invalid")
    allow_working_tree = _working_tree_mode(git_value)
    required_critical = V20_CRITICAL_FILES if code_schema == 20 else CRITICAL_FILES
    if allow_working_tree and set(critical) != {path.as_posix() for path in required_critical}:
        raise R0ReceiptError("working-tree critical file inventory is incomplete")
    with _formal_lease(project_root, home=home) as (installed, _access):
        current_runtime = _runtime_payload(
            project_root, installed=installed, formal_schema=formal_schema, code_schema=code_schema,
        )
        if runtime_payload != current_runtime:
            raise R0ReceiptError("runtime-root binding identity or inventory drifted")
        current_build = _build_payload(
            project_root,
            installed=installed,
            runtime_receipt_path=runtime_path,
            test_results_receipt_path=test_results_path,
            formal_schema=formal_schema,
            code_schema=code_schema, candidate_database=candidate_database, migration_receipt=migration_receipt, deployment_id=deployment_id,
            install_receipt=install_receipt,
            allow_working_tree=allow_working_tree,
            critical_paths=tuple(Path(name) for name in critical),
            code_successor_plan=code_successor_plan or (Path(build_payload["code_successor_plan"]["path"])
                                                       if "code_successor_plan" in build_payload else None),
        )
        if build_payload != current_build:
            raise R0ReceiptError("sealed build identity or dependency drifted")
    git_record = current_build["git"]
    assert isinstance(git_record, dict)
    return {
        "status": "verified",
        "runtime_root_receipt_sha256": _sha256_file(runtime_path),
        "test_results_receipt_sha256": _sha256_file(test_results_path),
        "sealed_build_receipt_sha256": _sha256_file(build_path),
        "git_head": git_record["head"],
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("seal", "verify"):
        subparser = subparsers.add_parser(command)
        subparser.add_argument("--project-root", type=Path, required=True)
        subparser.add_argument("--evidence-dir", type=Path, required=True)
        subparser.add_argument("--home", type=Path)
        subparser.add_argument("--formal-schema", type=int, choices=(18, 19, 20), default=18)
        subparser.add_argument("--code-schema", type=int, choices=(19, 20), default=19)
        subparser.add_argument("--candidate-db", type=Path,
                               help="Explicit schema20 candidate for a 19->20 code seal; never the installed source.")
        subparser.add_argument("--migration-receipt", type=Path, help="Exact offline 19->20 candidate receipt.")
        subparser.add_argument("--deployment-id", help="Exact persisted schema20 bounded deployment receipt.")
        subparser.add_argument("--code-successor-plan", type=Path,
                               help="Exact private planner-budget post-accepted successor plan; never a new acceptance.")
        subparser.add_argument(
            "--install-receipt", type=Path,
            help="Required with --formal-schema 19 or 20; binds the original migration/install.",
        )
        if command == "seal":
            subparser.add_argument("--allow-working-tree", action="store_true",
                                   help="Seal exact uncommitted source with a private reconstruction archive; default requires clean Git.")
            subparser.add_argument("--expected-head", required=True)
            subparser.add_argument(
                "--test-result",
                action="append",
                required=True,
                dest="test_results",
                metavar="NAME=LOG_PATH",
            )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    result: Mapping[str, object]
    try:
        if arguments.command == "seal":
            result = seal(
                project_root=arguments.project_root,
                evidence_dir=arguments.evidence_dir,
                expected_head=arguments.expected_head,
                test_results=arguments.test_results,
                home=arguments.home,
                formal_schema=arguments.formal_schema,
                code_schema=arguments.code_schema, candidate_database=arguments.candidate_db, migration_receipt=arguments.migration_receipt, deployment_id=arguments.deployment_id,
                install_receipt=arguments.install_receipt,
                allow_working_tree=arguments.allow_working_tree,
                code_successor_plan=arguments.code_successor_plan,
            )
        else:
            result = verify(
                project_root=arguments.project_root,
                evidence_dir=arguments.evidence_dir,
                home=arguments.home,
                formal_schema=arguments.formal_schema,
                code_schema=arguments.code_schema, candidate_database=arguments.candidate_db, migration_receipt=arguments.migration_receipt, deployment_id=arguments.deployment_id,
                install_receipt=arguments.install_receipt,
                code_successor_plan=arguments.code_successor_plan,
            )
    except R0ReceiptError as error:
        raise SystemExit(str(error)) from error
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
