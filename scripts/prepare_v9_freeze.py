#!/usr/bin/env python3
"""Create a read-only, auditable freeze bundle before the v9 migration."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = PROJECT_ROOT / "src" / "dcar_eval"
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from v8.evaluation import V8_RULE_VERSION, _current_evidence_state  # noqa: E402


DEFAULT_DB = PROJECT_ROOT / "app" / "data" / "dcar_insight.sqlite3"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "app" / "data" / "backups"
DEFAULT_FREEZE_LOCK = PROJECT_ROOT / "runtime" / "operator-freeze.lock"
KEY_TABLES = (
    "content_items",
    "evaluation_versions",
    "evaluation_matches",
    "evidence_artifacts",
    "evidence_envelopes",
    "fetch_slots",
    "fetch_attempts",
    "provider_raw_responses",
    "provider_usage",
    "report_tasks",
    "report_revisions",
    "report_files",
    "scheduler_runs",
)


class FreezeError(RuntimeError):
    pass


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=lambda item: {"bytes_hex": bytes(item).hex()},
    ).encode("utf-8")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_jsonl(path: Path) -> str:
    return _sha256_file(path)


def _fingerprint_disk_path(path: Path) -> dict[str, Any]:
    if path.is_file():
        return {
            "disk_state": "file",
            "actual_byte_size": path.stat().st_size,
            "actual_sha256": _sha256_file(path),
            "actual_file_count": 1,
        }
    if path.is_dir():
        digest = hashlib.sha256()
        total = 0
        files = sorted(child for child in path.rglob("*") if child.is_file())
        for child in files:
            relative = child.relative_to(path).as_posix()
            child_size = child.stat().st_size
            child_sha = _sha256_file(child)
            total += child_size
            digest.update(relative.encode("utf-8"))
            digest.update(b"\0")
            digest.update(child_sha.encode("ascii"))
            digest.update(b"\0")
        return {
            "disk_state": "directory",
            "actual_byte_size": total,
            "actual_sha256": digest.hexdigest(),
            "actual_file_count": len(files),
        }
    return {
        "disk_state": "missing",
        "actual_byte_size": None,
        "actual_sha256": None,
        "actual_file_count": None,
    }


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _table_names(connection: sqlite3.Connection) -> list[str]:
    return [
        str(row[0])
        for row in connection.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        )
    ]


def _logical_snapshot(connection: sqlite3.Connection) -> dict[str, Any]:
    """Hash every business row; mtime/WAL metadata is deliberately excluded."""

    snapshot: dict[str, Any] = {}
    for table in _table_names(connection):
        quoted = _quote(table)
        columns = [
            str(row[1]) for row in connection.execute(f"PRAGMA table_info({quoted})")
        ]
        order = ",".join(_quote(column) for column in columns)
        digest = hashlib.sha256()
        count = 0
        maximums: dict[str, Any] = {}
        for candidate in (
            "id",
            "updated_at",
            "created_at",
            "evaluated_at",
            "recorded_at",
            "completed_at",
            "claimed_at",
        ):
            if candidate in columns:
                maximums[f"max_{candidate}"] = connection.execute(
                    f"SELECT MAX({_quote(candidate)}) FROM {quoted}"
                ).fetchone()[0]
        for row in connection.execute(f"SELECT * FROM {quoted} ORDER BY {order}"):
            digest.update(_canonical(list(row)))
            digest.update(b"\n")
            count += 1
        snapshot[table] = {
            "count": count,
            "rows_sha256": digest.hexdigest(),
            **maximums,
        }
    return snapshot


def _snapshot_sha256(snapshot: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical(snapshot)).hexdigest()


def _connect_read_only(path: Path) -> sqlite3.Connection:
    try:
        connection = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("SELECT 1").fetchone()
        return connection
    except sqlite3.OperationalError as exc:
        message = str(exc).lower()
        if "readonly" in message or "recovery" in message:
            raise FreezeError(
                "read-only WAL recovery is required; the live writer is not safely frozen"
            ) from exc
        raise


def _port_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(0.2)
        return probe.connect_ex(("127.0.0.1", port)) == 0


def _database_writer_handles(database: Path) -> list[dict[str, Any]]:
    targets = [database, Path(str(database) + "-wal"), Path(str(database) + "-shm")]
    existing = [str(path) for path in targets if path.exists()]
    if not existing:
        return []
    result = subprocess.run(
        ["lsof", "-nP", *existing],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode not in (0, 1):
        raise FreezeError(
            f"lsof failed while checking database handles: {result.stderr}"
        )
    writers: list[dict[str, Any]] = []
    for line in result.stdout.splitlines()[1:]:
        parts = line.split()
        if len(parts) < 9:
            continue
        descriptor = parts[3]
        if descriptor.endswith("u") or descriptor.endswith("w"):
            writers.append(
                {
                    "command": parts[0],
                    "pid": int(parts[1]),
                    "descriptor": descriptor,
                    "path": parts[-1],
                }
            )
    return writers


def _git_state() -> dict[str, Any]:
    def run(*args: str) -> str:
        result = subprocess.run(
            ["git", *args],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.rstrip("\n")

    return {
        "head": run("rev-parse", "HEAD"),
        "status_short": run("status", "--short").splitlines(),
    }


def _rows(connection: sqlite3.Connection, query: str) -> list[dict[str, Any]]:
    return [dict(row) for row in connection.execute(query).fetchall()]


def _database_summary(connection: sqlite3.Connection) -> dict[str, Any]:
    tables = set(_table_names(connection))
    summary: dict[str, Any] = {}
    if "content_items" in tables:
        summary["content_high_water"] = dict(
            connection.execute(
                "SELECT COUNT(*) count, MAX(id) max_id FROM content_items"
            ).fetchone()
        )
        summary["manual_content_direction"] = _rows(
            connection,
            """
            SELECT manual_content_direction value, COUNT(*) count
            FROM content_items GROUP BY manual_content_direction
            ORDER BY manual_content_direction
            """,
        )
    if "evaluation_versions" in tables:
        summary["evaluation_versions"] = _rows(
            connection,
            """
            SELECT rule_version, taxonomy_version, evaluation_source,
                   COUNT(*) count, SUM(invalidated_at IS NULL) valid_count
            FROM evaluation_versions
            GROUP BY rule_version, taxonomy_version, evaluation_source
            ORDER BY rule_version, taxonomy_version, evaluation_source
            """,
        )
    if "fetch_slots" in tables:
        summary["fetch_slots"] = _rows(
            connection,
            """
            SELECT stage, status, COUNT(*) count FROM fetch_slots
            GROUP BY stage, status ORDER BY stage, status
            """,
        )
    if "provider_usage" in tables:
        summary["provider_usage"] = dict(
            connection.execute(
                """
                SELECT COUNT(*) row_count,
                       COALESCE(SUM(billed_requests), 0) billed_requests,
                       ROUND(COALESCE(SUM(amount), 0), 6) amount,
                       MIN(currency) min_currency, MAX(currency) max_currency,
                       MAX(id) max_id, MAX(recorded_at) max_recorded_at
                FROM provider_usage
                """
            ).fetchone()
        )
    if "scheduler_runs" in tables:
        summary["scheduler_runs"] = _rows(
            connection,
            """
            SELECT id, job_id, scheduled_for, status, started_at, completed_at,
                   details_json FROM scheduler_runs ORDER BY id
            """,
        )
    if {"report_tasks", "report_revisions"} <= tables:
        report_rows = _rows(
            connection,
            """
            SELECT rr.*, rt.creation_source, rt.task_status,
                   rt.started_at task_started_at, rt.completed_at task_completed_at
            FROM report_revisions rr
            JOIN report_tasks rt ON rt.id=rr.task_id
            ORDER BY rr.task_id, rr.revision
            """,
        )
        run_by_task: dict[str, list[int]] = {}
        for run in summary.get("scheduler_runs", []):
            try:
                details = json.loads(str(run["details_json"] or "{}"))
            except json.JSONDecodeError:
                continue
            task_id = details.get("task_id") if isinstance(details, dict) else None
            if task_id:
                run_by_task.setdefault(str(task_id), []).append(int(run["id"]))
        for report in report_rows:
            report["scheduler_run_ids"] = run_by_task.get(str(report["task_id"]), [])
        summary["report_revisions"] = report_rows
        summary["unsafe_automatic_report_revisions"] = [
            report
            for report in report_rows
            if report["creation_source"] == "automatic"
            and report["invalidated_at"] is None
            and report["contract_version"] == "dcar-content-operations-report-v8.3"
            and report["rule_version"] == "evaluation-v7"
            and report["taxonomy_version"] == "selling-points-v5.0"
        ]
    return summary


def _require_active_v8_release(
    connection: sqlite3.Connection,
) -> dict[str, Any]:
    if "evaluation_releases" not in set(_table_names(connection)):
        raise FreezeError(
            "historical v9 freeze requires an active evaluation-v8 release"
        )
    releases = connection.execute(
        "SELECT * FROM evaluation_releases WHERE status='active' ORDER BY id"
    ).fetchall()
    if len(releases) != 1 or str(releases[0]["rule_version"]) != V8_RULE_VERSION:
        raise FreezeError(
            "historical v9 freeze requires exactly one active evaluation-v8 release"
        )
    return dict(releases[0])


def _write_content_evidence_inventory(
    connection: sqlite3.Connection, path: Path
) -> dict[str, Any]:
    counts: Counter[str] = Counter()
    content_ids = [
        int(row[0])
        for row in connection.execute("SELECT id FROM content_items ORDER BY id")
    ]
    with path.open("w", encoding="utf-8") as handle:
        for content_id in content_ids:
            _, components, evidence_sha256 = _current_evidence_state(
                connection, content_id, rule_version=V8_RULE_VERSION
            )
            envelopes = {
                str(row[0])
                for row in connection.execute(
                    """
                    SELECT evidence_sha256 FROM evidence_envelopes
                    WHERE content_id=?
                    """,
                    (content_id,),
                )
            }
            state = (
                "exact"
                if evidence_sha256 in envelopes
                else "stale"
                if envelopes
                else "absent"
            )
            counts[state] += 1
            handle.write(
                json.dumps(
                    {
                        "content_id": content_id,
                        "current_evidence_sha256": evidence_sha256,
                        "envelope_state": state,
                        "components": components,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
                + "\n"
            )
    return {
        "row_count": len(content_ids),
        "min_content_id": min(content_ids) if content_ids else None,
        "max_content_id": max(content_ids) if content_ids else None,
        "envelope_states": dict(sorted(counts.items())),
        "sha256": _sha256_jsonl(path),
    }


def _resolved_artifact_path(local_path: str) -> Path:
    path = Path(local_path)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _write_artifact_inventory(
    connection: sqlite3.Connection, path: Path
) -> dict[str, Any]:
    counts: Counter[str] = Counter()
    cache: dict[str, dict[str, Any]] = {}
    rows = connection.execute(
        """
        SELECT id, content_id, artifact_type, local_path, status,
               byte_size, sha256, created_at
        FROM evidence_artifacts ORDER BY id
        """
    )
    row_count = 0
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            row_count += 1
            local_path = str(row["local_path"])
            resolved = _resolved_artifact_path(local_path)
            cache_key = str(resolved)
            actual = cache.get(cache_key)
            if actual is None:
                actual = _fingerprint_disk_path(resolved)
                cache[cache_key] = actual
            expected_sha = str(row["sha256"]) if row["sha256"] else None
            expected_size = (
                int(row["byte_size"]) if row["byte_size"] is not None else None
            )
            if row["status"] == "available":
                if actual["disk_state"] == "missing":
                    integrity = "available_missing_on_disk"
                elif expected_sha != actual["actual_sha256"]:
                    integrity = "sha256_mismatch"
                elif expected_size != actual["actual_byte_size"]:
                    integrity = "byte_size_mismatch"
                else:
                    integrity = "verified"
            else:
                integrity = "declared_non_available"
            counts[integrity] += 1
            handle.write(
                json.dumps(
                    {
                        **dict(row),
                        **actual,
                        "integrity": integrity,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
                + "\n"
            )
    return {
        "row_count": row_count,
        "unique_local_paths": len(cache),
        "integrity_states": dict(sorted(counts.items())),
        "sha256": _sha256_jsonl(path),
    }


def _backup_database(source: sqlite3.Connection, destination: Path) -> dict[str, Any]:
    backup = sqlite3.connect(destination)
    try:
        source.backup(backup)
    finally:
        backup.close()
    check = sqlite3.connect(
        f"file:{destination.resolve()}?mode=ro&immutable=1", uri=True
    )
    try:
        integrity = str(check.execute("PRAGMA integrity_check").fetchone()[0])
        foreign_key_violations = [
            list(row) for row in check.execute("PRAGMA foreign_key_check").fetchall()
        ]
    finally:
        check.close()
    if integrity != "ok" or foreign_key_violations:
        raise FreezeError(
            f"backup validation failed: integrity={integrity}, "
            f"foreign_keys={len(foreign_key_violations)}"
        )
    return {
        "path": destination.name,
        "byte_size": destination.stat().st_size,
        "sha256": _sha256_file(destination),
        "integrity_check": integrity,
        "foreign_key_violations": foreign_key_violations,
    }


def create_freeze_bundle(
    *,
    database: Path,
    output_root: Path,
    freeze_lock: Path,
    protected_ports: Iterable[int] = (4173, 4174, 8765),
    require_no_writer_handles: bool = True,
) -> Path:
    database = database.resolve()
    freeze_lock = freeze_lock.resolve()
    if not freeze_lock.is_file():
        raise FreezeError(f"operator freeze lock is missing: {freeze_lock}")
    open_ports = [port for port in protected_ports if _port_open(port)]
    if not database.is_file():
        raise FreezeError(f"database does not exist: {database}")
    writer_handles = (
        _database_writer_handles(database) if require_no_writer_handles else []
    )
    if writer_handles:
        raise FreezeError(
            f"database writer handles are still open: {json.dumps(writer_handles)}"
        )

    output_root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".v9-freeze-", dir=output_root))
    source = _connect_read_only(database)
    try:
        before_changes = source.total_changes
        active_release = _require_active_v8_release(source)
        started_at = _utc_now()
        before = _logical_snapshot(source)
        logical_sha = _snapshot_sha256(before)
        stamp = started_at.replace("-", "").replace(":", "").replace("Z", "Z")
        final = output_root / f"v9-freeze-{stamp}-{logical_sha[:12]}"
        if final.exists():
            raise FreezeError(f"freeze bundle already exists: {final}")

        database_backup = _backup_database(source, temporary / database.name)
        content_path = temporary / "content_evidence.jsonl"
        content_summary = _write_content_evidence_inventory(source, content_path)
        artifacts_path = temporary / "evidence_artifacts.jsonl"
        artifacts_summary = _write_artifact_inventory(source, artifacts_path)
        after = _logical_snapshot(source)
        if before != after:
            raise FreezeError("business database changed while freeze bundle was built")
        if source.total_changes != before_changes:
            raise FreezeError("freeze bundle attempted to write the source database")

        source_stat = database.stat()
        wal_path = Path(str(database) + "-wal")
        wal_stat = wal_path.stat() if wal_path.exists() else None
        manifest = {
            "schema_version": "dcar-v9-freeze-manifest-v1",
            "started_at": started_at,
            "completed_at": _utc_now(),
            "source_database": str(database),
            "freeze_lock": str(freeze_lock),
            "application_ports_open": open_ports,
            "application_ports_note": (
                "reference-only; isolated services on other databases are allowed"
            ),
            "source_database_writer_handles": writer_handles,
            "source_file_reference": {
                "byte_size": source_stat.st_size,
                "mtime_ns": source_stat.st_mtime_ns,
                "wal_byte_size": wal_stat.st_size if wal_stat else 0,
                "wal_mtime_ns": wal_stat.st_mtime_ns if wal_stat else None,
                "note": "mtime and WAL metadata are reference-only, not freeze gates",
            },
            "git": _git_state(),
            "logical_snapshot_sha256": logical_sha,
            "table_snapshot": before,
            "database_summary": _database_summary(source),
            "active_evaluation_release": active_release,
            "content_evidence_inventory": {
                "path": content_path.name,
                **content_summary,
            },
            "evidence_artifact_inventory": {
                "path": artifacts_path.name,
                **artifacts_summary,
            },
            "database_backup": database_backup,
            "source_total_changes": source.total_changes,
        }
        manifest_path = temporary / "manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, final)
        return final
    except Exception:
        (temporary / "FAILED").touch(exist_ok=True)
        raise
    finally:
        source.close()


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DEFAULT_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--freeze-lock", type=Path, default=DEFAULT_FREEZE_LOCK)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    bundle = create_freeze_bundle(
        database=args.database,
        output_root=args.output_root,
        freeze_lock=args.freeze_lock,
    )
    print(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
