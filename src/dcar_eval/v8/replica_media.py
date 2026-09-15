"""Read legacy image manifests only through the installed snapshot allowlist."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from . import artifact_paths, media_lifecycle


def manifest_paths(row: Mapping[str, Any], *, project_root: Path) -> list[Path | None]:
    def checked(value: str) -> tuple[Path, dict]:
        path = artifact_paths.resolve(value, fallback_root=project_root)
        record = artifact_paths.replica_file(path, fallback_root=project_root)
        if record is None:
            raise ValueError("media child is not in the installed snapshot")
        # This also rejects aliases, writable files, hard links and stale bytes.
        evidence = media_lifecycle._file(path)
        if any(evidence[key] != record[key] for key in ("sha256", "byte_size")):
            raise ValueError("media child differs from the installed snapshot")
        return path, record

    try:
        if artifact_paths.installed_snapshot() is None:
            return []
        path, record = checked(str(row["local_path"]))
        if (record["sha256"] != row["sha256"] or record["byte_size"] != row["byte_size"]
                or record["byte_size"] > 16 * 1024 * 1024):
            return []
        def identity():
            s = path.stat()
            return s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns
        before = identity()
        body = path.read_bytes()
        if identity() != before or hashlib.sha256(body).hexdigest() != record["sha256"]:
            return []
        value = json.loads(body)
        if not isinstance(value, dict):
            return []
        candidates = ([value["video_path"]] if value.get("video_path") else [])
        candidates += value.get("image_paths", [])
        if not isinstance(candidates, list) or any(not isinstance(v, str) for v in candidates):
            return []
    except (OSError, ValueError, TypeError, KeyError, media_lifecycle.LifecycleError):
        return []
    result = []
    for candidate in candidates:
        try:
            result.append(checked(candidate)[0])
        except (OSError, ValueError, media_lifecycle.LifecycleError):
            # Keep the original image indexes when an individual child is absent.
            result.append(None)
    return result
