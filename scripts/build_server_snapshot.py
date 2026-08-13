#!/usr/bin/env python3
"""Build a fail-closed read-replica bundle from live SQLite databases.

The database files are created with SQLite's online backup API.  Artifact files
are not copied into the bundle: the bundle contains hash-checked rsync file
lists and a manifest which the server-side installer verifies before changing
the active database.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Mapping, Optional


BUNDLE_SCHEMA = "dcar-read-replica-snapshot-v2"
DATABASE_NAMES = frozenset({"dcar_insight.sqlite3", "web_mvp.sqlite3"})
ARTIFACT_POLICY_NAME = "thin-server-v1"
OPTIONAL_REUSE_EVIDENCE_TYPES = ("media",)
TEXT_SUFFIXES = frozenset({".json", ".jsonl", ".txt", ".md", ".csv", ".srt", ".vtt"})
REQUIRED_RUNTIME_ARTIFACTS = (
    "data/cache/.comment_hash_salt",
    "data/cache/.platform_user_salt",
)
SHA256_RE = re.compile(r"[0-9a-f]{64}")


class SnapshotBuildError(RuntimeError):
    """The requested bundle could not be proved internally consistent."""


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


def _connect_read_only(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        is not None
    )


def _validate_database(
    path: Path, *, expected_user_version: Optional[int]
) -> dict[str, Any]:
    with _connect_read_only(path) as connection:
        quick_rows = [
            str(row[0]) for row in connection.execute("PRAGMA quick_check").fetchall()
        ]
        if quick_rows != ["ok"]:
            raise SnapshotBuildError(
                f"SQLite quick_check failed for {path.name}: {quick_rows[:5]}"
            )
        foreign_key_rows = connection.execute("PRAGMA foreign_key_check").fetchall()
        if foreign_key_rows:
            raise SnapshotBuildError(
                f"SQLite foreign_key_check failed for {path.name}: "
                f"{len(foreign_key_rows)} violation(s)"
            )
        user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if expected_user_version is not None and user_version != expected_user_version:
            raise SnapshotBuildError(
                f"unexpected schema for {path.name}: {user_version}, "
                f"expected {expected_user_version}"
            )
        page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
    return {
        "quick_check": "ok",
        "foreign_key_violations": 0,
        "user_version": user_version,
        "page_count": page_count,
        "page_size": page_size,
    }


def _online_backup(
    source_path: Path,
    target_path: Path,
    *,
    expected_user_version: Optional[int],
) -> dict[str, Any]:
    if not source_path.is_file() or source_path.is_symlink():
        raise SnapshotBuildError(f"database is not a regular file: {source_path}")
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with _connect_read_only(source_path) as source:
        target = sqlite3.connect(target_path)
        try:
            source.backup(target, pages=4096, sleep=0.01)
            target.execute("PRAGMA journal_mode=DELETE")
            target.commit()
        finally:
            target.close()
    validation = _validate_database(
        target_path, expected_user_version=expected_user_version
    )
    _fsync_file(target_path)
    return {
        "name": target_path.name,
        "bundle_path": f"databases/{target_path.name}",
        "byte_size": target_path.stat().st_size,
        "sha256": _sha256(target_path),
        **validation,
    }


def _normalize_project_relative(value: str) -> tuple[str, str, str]:
    if not value or "\x00" in value or "\n" in value or "\r" in value:
        raise SnapshotBuildError("artifact path contains an unsafe character")
    if "\\" in value:
        raise SnapshotBuildError(f"artifact path must use POSIX separators: {value}")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise SnapshotBuildError(f"artifact path is not project-relative: {value}")
    if path.parts[:2] == ("data", "cache") and len(path.parts) > 2:
        return "cache", str(PurePosixPath(*path.parts[2:])), str(path)
    if path.parts[0] == "reports" and len(path.parts) > 1:
        return "reports", str(PurePosixPath(*path.parts[1:])), str(path)
    raise SnapshotBuildError(
        f"online artifact must be below data/cache or reports: {value}"
    )


def _iter_project_paths(value: Any, *, project_root: Path) -> Iterable[str]:
    if isinstance(value, str):
        if value.startswith("data/cache/") or value.startswith("reports/"):
            yield value
        elif Path(value).is_absolute():
            try:
                relative = Path(value).resolve().relative_to(project_root.resolve())
            except (OSError, ValueError):
                return
            canonical = relative.as_posix()
            if canonical.startswith("data/cache/") or canonical.startswith("reports/"):
                yield canonical
        return
    if isinstance(value, list):
        for item in value:
            yield from _iter_project_paths(item, project_root=project_root)
        return
    if isinstance(value, Mapping):
        for item in value.values():
            yield from _iter_project_paths(item, project_root=project_root)


def _read_json_paths(path: Path, *, project_root: Path) -> list[str]:
    if path.suffix.lower() != ".json":
        return []
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SnapshotBuildError(f"artifact JSON cannot be parsed: {path}") from exc
    return list(_iter_project_paths(value, project_root=project_root))


def _add_artifact(
    files: dict[tuple[str, str], dict[str, Any]],
    pending_json: list[Path],
    *,
    project_root: Path,
    relative_path: str,
    expected_sha256: Optional[str] = None,
    expected_byte_size: Optional[int] = None,
    disposition_reason: Optional[str] = None,
) -> None:
    root_name, root_relative, canonical = _normalize_project_relative(relative_path)
    candidate = (project_root / canonical).resolve()
    root = project_root.resolve()
    if (
        root not in candidate.parents
        or candidate.is_symlink()
        or not candidate.is_file()
    ):
        raise SnapshotBuildError(
            f"referenced artifact is missing or unsafe: {canonical}"
        )
    byte_size = candidate.stat().st_size
    sha256 = _sha256(candidate)
    if expected_sha256 and SHA256_RE.fullmatch(expected_sha256):
        if sha256 != expected_sha256:
            raise SnapshotBuildError(f"artifact SHA-256 drifted: {canonical}")
    if expected_byte_size is not None and expected_byte_size >= 0:
        if byte_size != expected_byte_size:
            raise SnapshotBuildError(f"artifact byte size drifted: {canonical}")
    key = (root_name, root_relative)
    existing = files.get(key)
    item = {
        "root": root_name,
        "path": root_relative,
        "project_path": canonical,
        "byte_size": byte_size,
        "sha256": sha256,
    }
    if disposition_reason is not None:
        item["reason"] = disposition_reason
    if existing is not None and existing != item:
        raise SnapshotBuildError(f"artifact identity conflict: {canonical}")
    if existing is None:
        files[key] = item
        if candidate.suffix.lower() == ".json":
            pending_json.append(candidate)


def _add_registered_optional(
    files: dict[tuple[str, str], dict[str, Any]],
    *,
    relative_path: str,
    expected_sha256: str,
    expected_byte_size: Optional[int],
    reason: str,
) -> None:
    root_name, root_relative, canonical = _normalize_project_relative(relative_path)
    if not SHA256_RE.fullmatch(expected_sha256):
        raise SnapshotBuildError(
            f"optional artifact lacks a registered SHA-256: {canonical}"
        )
    if expected_byte_size is None or expected_byte_size < 0:
        raise SnapshotBuildError(
            f"optional artifact lacks a registered byte size: {canonical}"
        )
    key = (root_name, root_relative)
    item = {
        "root": root_name,
        "path": root_relative,
        "project_path": canonical,
        "byte_size": expected_byte_size,
        "sha256": expected_sha256,
        "reason": reason,
    }
    existing = files.get(key)
    if existing is not None and existing != item:
        raise SnapshotBuildError(f"artifact identity conflict: {canonical}")
    files[key] = item


def _collect_artifacts(
    snapshot_db: Path, *, project_root: Path
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    files: dict[tuple[str, str], dict[str, Any]] = {}
    optional_reuse: dict[tuple[str, str], dict[str, Any]] = {}
    pending_json: list[Path] = []
    for relative_path in REQUIRED_RUNTIME_ARTIFACTS:
        _add_artifact(
            files,
            pending_json,
            project_root=project_root,
            relative_path=relative_path,
        )
    with _connect_read_only(snapshot_db) as connection:
        if _table_exists(connection, "report_files"):
            for row in connection.execute(
                """
                SELECT local_path,sha256,byte_size FROM report_files
                WHERE status='available' ORDER BY local_path
                """
            ):
                _add_artifact(
                    files,
                    pending_json,
                    project_root=project_root,
                    relative_path=str(row["local_path"]),
                    expected_sha256=str(row["sha256"] or ""),
                    expected_byte_size=int(row["byte_size"]),
                )
        if _table_exists(connection, "report_revisions"):
            for row in connection.execute(
                "SELECT report_json_path,report_sha256 FROM report_revisions"
            ):
                _add_artifact(
                    files,
                    pending_json,
                    project_root=project_root,
                    relative_path=str(row["report_json_path"]),
                    expected_sha256=str(row["report_sha256"] or ""),
                )
        if _table_exists(connection, "evidence_artifacts"):
            for row in connection.execute(
                """
                SELECT artifact_type,local_path,sha256,byte_size
                FROM evidence_artifacts
                WHERE status='available' ORDER BY local_path
                """
            ):
                artifact_type = str(row["artifact_type"])
                suffix = PurePosixPath(str(row["local_path"])).suffix.lower()
                destination = (
                    optional_reuse
                    if artifact_type in OPTIONAL_REUSE_EVIDENCE_TYPES
                    else files
                )
                if destination is files and not (
                    suffix in TEXT_SUFFIXES
                    or (artifact_type == "comments" and suffix == "")
                ):
                    destination = optional_reuse
                if artifact_type in OPTIONAL_REUSE_EVIDENCE_TYPES:
                    _add_registered_optional(
                        optional_reuse,
                        relative_path=str(row["local_path"]),
                        expected_sha256=str(row["sha256"] or ""),
                        expected_byte_size=(
                            int(row["byte_size"])
                            if row["byte_size"] is not None
                            else None
                        ),
                        reason="large_binary",
                    )
                    continue
                _root_name, _root_relative, canonical = _normalize_project_relative(
                    str(row["local_path"])
                )
                local_candidate = project_root / canonical
                if local_candidate.is_symlink():
                    raise SnapshotBuildError(
                        f"referenced artifact is an unsafe symlink: {canonical}"
                    )
                if not local_candidate.is_file():
                    _add_registered_optional(
                        optional_reuse,
                        relative_path=str(row["local_path"]),
                        expected_sha256=str(row["sha256"] or ""),
                        expected_byte_size=(
                            int(row["byte_size"])
                            if row["byte_size"] is not None
                            else None
                        ),
                        reason="source_missing",
                    )
                    continue
                _add_artifact(
                    destination,
                    pending_json if destination is files else [],
                    project_root=project_root,
                    relative_path=str(row["local_path"]),
                    expected_sha256=str(row["sha256"] or ""),
                    expected_byte_size=(
                        int(row["byte_size"]) if row["byte_size"] is not None else None
                    ),
                )
    parsed_json: set[Path] = set()
    while pending_json:
        artifact = pending_json.pop()
        if artifact in parsed_json:
            continue
        parsed_json.add(artifact)
        for referenced in _read_json_paths(artifact, project_root=project_root):
            root_name, root_relative, _canonical = _normalize_project_relative(
                referenced
            )
            if (root_name, root_relative) in optional_reuse:
                continue
            if root_name == "reports" or PurePosixPath(referenced).suffix.lower() in TEXT_SUFFIXES:
                _add_artifact(
                    files,
                    pending_json,
                    project_root=project_root,
                    relative_path=referenced,
                )
            else:
                _add_artifact(
                    optional_reuse,
                    [],
                    project_root=project_root,
                    relative_path=referenced,
                    disposition_reason="large_binary",
                )
    for identity in tuple(optional_reuse):
        if identity in files:
            optional_reuse.pop(identity)
    return (
        sorted(files.values(), key=lambda item: (item["root"], item["path"])),
        sorted(
            optional_reuse.values(), key=lambda item: (item["root"], item["path"])
        ),
    )


def _freshness(snapshot_db: Path) -> dict[str, Any]:
    output: dict[str, Any] = {}
    with _connect_read_only(snapshot_db) as connection:
        if _table_exists(connection, "content_items"):
            row = connection.execute(
                """
                SELECT COUNT(*) content_count,
                       MAX(published_at) latest_published_at,
                       MAX(imported_at) latest_imported_at
                FROM content_items
                """
            ).fetchone()
            output.update(dict(row) if row is not None else {})
        if _table_exists(connection, "scheduler_runs"):
            output["scheduler_jobs"] = [
                dict(row)
                for row in connection.execute(
                    """
                    SELECT s.job_id,s.scheduled_for,s.status,s.completed_at
                    FROM scheduler_runs s JOIN (
                        SELECT job_id,MAX(scheduled_for) scheduled_for
                        FROM scheduler_runs GROUP BY job_id
                    ) latest
                      ON latest.job_id=s.job_id
                     AND latest.scheduled_for=s.scheduled_for
                    ORDER BY s.job_id
                    """
                )
            ]
    return output


def _artifact_set_sha256(files: Iterable[Mapping[str, Any]]) -> str:
    digest = hashlib.sha256()
    for item in files:
        digest.update(
            (
                f"{item['root']}\0{item['path']}\0{item['byte_size']}\0"
                f"{item['sha256']}\n"
            ).encode("utf-8")
        )
    return digest.hexdigest()


def _write_from0_lists(bundle_root: Path, files: Iterable[Mapping[str, Any]]) -> None:
    grouped: dict[str, list[str]] = {"cache": [], "reports": []}
    for item in files:
        grouped[str(item["root"])].append(str(item["path"]))
    for root_name, values in grouped.items():
        target = bundle_root / f"{root_name}-files-from0"
        payload = b"".join(value.encode("utf-8") + b"\0" for value in values)
        target.write_bytes(payload)
        _fsync_file(target)


def build_snapshot(
    *,
    project_root: Path,
    database: Path,
    output: Path,
    legacy_database: Optional[Path] = None,
    expected_user_version: Optional[int] = None,
) -> dict[str, Any]:
    project_root = project_root.resolve()
    database = database.resolve()
    legacy_database = legacy_database.resolve() if legacy_database else None
    output = output.resolve()
    if output.exists():
        raise SnapshotBuildError(f"output already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(
        tempfile.mkdtemp(prefix=f".{output.name}.", dir=str(output.parent))
    )
    try:
        database_dir = temporary / "databases"
        database_dir.mkdir(mode=0o700)
        databases = [
            _online_backup(
                database,
                database_dir / "dcar_insight.sqlite3",
                expected_user_version=expected_user_version,
            )
        ]
        if legacy_database is not None:
            databases.append(
                _online_backup(
                    legacy_database,
                    database_dir / "web_mvp.sqlite3",
                    expected_user_version=None,
                )
            )
        files, optional_reuse_files = _collect_artifacts(
            database_dir / "dcar_insight.sqlite3", project_root=project_root
        )
        _write_from0_lists(temporary, files)
        main_database = databases[0]
        snapshot_id = (
            datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            + "-"
            + str(main_database["sha256"])[:12]
        )
        manifest = {
            "schema": BUNDLE_SCHEMA,
            "snapshot_id": snapshot_id,
            "created_at": _utc_now(),
            "databases": databases,
            "freshness": _freshness(database_dir / "dcar_insight.sqlite3"),
            "artifact_policy": {
                "name": ARTIFACT_POLICY_NAME,
                "included": "reports-and-small-text-evidence",
                "optional_reuse": "active-same-path-size-sha256-only",
                "on_optional_missing_or_mismatch": "omitted",
                "delete_unlisted": False,
            },
            "files": files,
            "file_count": len(files),
            "file_byte_size": sum(int(item["byte_size"]) for item in files),
            "file_set_sha256": _artifact_set_sha256(files),
            "optional_reuse_files": optional_reuse_files,
            "optional_reuse_file_count": len(optional_reuse_files),
            "optional_reuse_byte_size": sum(
                int(item["byte_size"]) for item in optional_reuse_files
            ),
            "optional_reuse_set_sha256": _artifact_set_sha256(
                optional_reuse_files
            ),
        }
        manifest_bytes = (
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        manifest_path = temporary / "manifest.json"
        manifest_path.write_bytes(manifest_bytes)
        (temporary / "manifest.sha256").write_text(
            hashlib.sha256(manifest_bytes).hexdigest() + "  manifest.json\n",
            encoding="ascii",
        )
        _fsync_file(manifest_path)
        _fsync_file(temporary / "manifest.sha256")
        _fsync_directory(database_dir)
        _fsync_directory(temporary)
        os.replace(temporary, output)
        _fsync_directory(output.parent)
        return manifest
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--legacy-db", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expected-user-version", type=int)
    return parser


def main() -> int:
    arguments = _parser().parse_args()
    try:
        manifest = build_snapshot(
            project_root=arguments.project_root,
            database=arguments.db,
            legacy_database=arguments.legacy_db,
            output=arguments.output,
            expected_user_version=arguments.expected_user_version,
        )
    except SnapshotBuildError as exc:
        raise SystemExit(f"snapshot build refused: {exc}") from exc
    print(
        json.dumps(
            {
                "snapshot_id": manifest["snapshot_id"],
                "database_sha256": manifest["databases"][0]["sha256"],
                "file_count": manifest["file_count"],
                "file_byte_size": manifest["file_byte_size"],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
