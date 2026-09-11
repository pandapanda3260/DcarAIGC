"""Readiness of an installed snapshot reader, independent of Writer authority.

Startup hashes the database once. Requests reuse that verified hash only while
the database and its SQLite sidecars retain their exact physical versions.
No raw archive, paid permit or Writer receipt chain is traversed here.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Mapping

from . import artifact_paths
from .runtime_database import DatabaseAccessMode


def database_version(path: Path) -> tuple[Any, ...]:
    resolved = path.resolve(strict=True)
    versions: list[tuple[int, int, int, int, int] | None] = []
    for item in (resolved, Path(str(resolved) + "-wal"), Path(str(resolved) + "-shm")):
        try:
            info = item.stat()
            versions.append((info.st_dev, info.st_ino, info.st_size,
                             info.st_mtime_ns, info.st_ctime_ns))
        except FileNotFoundError:
            if item == resolved:
                raise
            versions.append(None)
    return (str(resolved), *versions)


def snapshot_readiness(*, config: Any, state: Any,
                       compatibility: Mapping[str, Any]) -> dict[str, Any] | None:
    """None retains the legacy local reader path; a claimed snapshot fails closed."""
    if not config.read_only:
        return None
    claimed = (os.environ.get("DCAR_READ_ONLY", "0").strip() == "1"
               or bool(os.environ.get("DCAR_ACTIVE_SNAPSHOT")))
    try:
        installed = artifact_paths.installed_snapshot()
    except (OSError, ValueError, TypeError, KeyError):
        return {"ready": False, "reason": "snapshot_receipt_invalid"}
    if installed is None:
        return {"ready": False, "reason": "snapshot_receipt_missing"} if claimed else None
    if (config.scheduler_enabled or config.startup_catchup_enabled
            or config.runtime_access_mode not in {None, DatabaseAccessMode.FORMAL_READ}
            or any(bool(getattr(state, key, False)) for key in (
                "writer_lock_held", "scheduler_requested", "scheduler_enabled", "scheduler_running"))):
        return {"ready": False, "reason": "snapshot_reader_role_conflict"}
    receipt = installed["receipt"]
    result = {"ready": False, "reason": None, "snapshot_id": receipt.get("snapshot_id"),
              "installed_at": receipt.get("installed_at"),
              "activation_status": receipt.get("activation_status")}
    if not compatibility.get("compatible"):
        return {**result, "reason": "database_incompatible"}
    if (receipt.get("schema") != "dcar-read-replica-install-receipt-v1"
            or receipt.get("activation_status") != "succeeded"):
        return {**result, "reason": "snapshot_install_not_verified"}
    try:
        unchanged = database_version(config.db_path) == getattr(state, "reader_database_version", None)
    except OSError:
        unchanged = False
    if (not unchanged or not getattr(state, "database_sha256", None)
            or receipt.get("database_sha256", {}).get("dcar_insight.sqlite3") != state.database_sha256
            or not getattr(state, "reader_runtime_identity", None)
            or receipt.get("runtime_identity") != state.reader_runtime_identity):
        return {**result, "reason": "snapshot_database_changed"}
    return {**result, "ready": True}
