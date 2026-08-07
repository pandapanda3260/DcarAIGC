"""Versioned local media processing shared by Douyin and Xiaohongshu."""

from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from importlib.metadata import version as package_version
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional

from huggingface_hub import snapshot_download

from .storage import DEFAULT_DB, PROJECT_ROOT, connect, now_utc, transaction


CONFIG_PATH = PROJECT_ROOT / "config" / "media_processor_v8.json"
MEDIA_ROOT = PROJECT_ROOT / "data" / "cache" / "v8" / "media"
OCR_SOURCE = PROJECT_ROOT / "src" / "dcar_eval" / "vision_ocr.swift"
MEDIA_SOURCE_VERSION = "provider-media-source-v8.1"
VIDEO_DOWNLOAD_VERSION = "provider-media-download-v8.1"
IMAGE_DOWNLOAD_VERSION = "provider-image-download-v8.1"
LEGACY_VIDEO_DOWNLOAD_VERSION = "provider-media-download-v8.0"
LEGACY_IMAGE_DOWNLOAD_VERSION = "provider-image-download-v8.0"
MAX_MEDIA_DOWNLOAD_ATTEMPTS = 3
MAX_MEDIA_PROCESSING_ATTEMPTS = 3
MEDIA_DOWNLOAD_WORKERS = 6
MEDIA_QUEUE_BATCH_LIMIT = 500
STALE_MEDIA_SLOT_SECONDS = 2 * 60 * 60
BOUNDED_PROCESSOR_TYPES = frozenset({"download", "frames", "asr", "ocr"})


class MediaProcessingError(RuntimeError):
    pass


class TerminalMediaSlotError(MediaProcessingError):
    """The same evidence source exhausted its bounded processing attempts."""


def is_supported_media_url(value: str) -> bool:
    """Allow HTTPS sources and the HTTP-only Xiaohongshu media CDN."""

    try:
        parsed = urllib.parse.urlparse(value)
    except ValueError:
        return False
    if parsed.scheme == "https" and bool(parsed.hostname):
        return True
    hostname = (parsed.hostname or "").lower()
    return parsed.scheme == "http" and hostname.endswith(
        (".rednotecdn.com", ".xhscdn.com")
    )


def _normalize_media_url(value: str) -> Optional[str]:
    """Apply safe URL normalization without rewriting signed query strings."""

    raw = value.strip()
    try:
        parsed = urllib.parse.urlsplit(raw)
        hostname = (parsed.hostname or "").lower()
        port = parsed.port
    except ValueError:
        return None
    scheme = parsed.scheme.lower()
    if not hostname or parsed.username is not None or parsed.password is not None:
        return None
    if scheme == "https":
        pass
    elif scheme == "http" and hostname.endswith((".rednotecdn.com", ".xhscdn.com")):
        pass
    else:
        return None
    host = f"[{hostname}]" if ":" in hostname else hostname
    if port is not None and not (
        (scheme == "https" and port == 443) or (scheme == "http" and port == 80)
    ):
        host = f"{host}:{port}"
    return urllib.parse.urlunsplit(
        (scheme, host, parsed.path or "/", parsed.query, "")
    )


def _media_source_identity(
    media_kind: str, urls: Iterable[str]
) -> tuple[List[str], str]:
    """Return usable de-duplicated URLs and a stable kind-aware source digest."""

    if media_kind not in {"video", "image"}:
        raise MediaProcessingError(f"unsupported media kind: {media_kind}")
    values: List[str] = []
    seen: set[str] = set()
    for raw in urls:
        normalized = _normalize_media_url(str(raw))
        if normalized is None or normalized in seen:
            continue
        seen.add(normalized)
        values.append(normalized)
    canonical = {
        "media_kind": media_kind,
        "urls": sorted(values),
    }
    source_sha256 = hashlib.sha256(
        json.dumps(
            canonical,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    return values, source_sha256


def _legacy_media_source_sha256(urls: Iterable[str]) -> str:
    """Reproduce the v8.0 URL-only identity for compatibility checks."""

    return hashlib.sha256(
        json.dumps(list(urls), ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()


def _download_versions(media_kind: str) -> set[str]:
    if media_kind == "video":
        return {VIDEO_DOWNLOAD_VERSION, LEGACY_VIDEO_DOWNLOAD_VERSION}
    if media_kind == "image":
        return {IMAGE_DOWNLOAD_VERSION, LEGACY_IMAGE_DOWNLOAD_VERSION}
    raise MediaProcessingError(f"unsupported media kind: {media_kind}")


@dataclass(frozen=True)
class Artifact:
    id: int
    content_id: int
    artifact_type: str
    local_path: str
    sha256: str
    processor_version: str


def load_media_config(path: Path = CONFIG_PATH) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def processor_versions() -> Dict[str, str]:
    config = load_media_config()
    return {
        "frames": str(config["frames"]["processor_version"]),
        "asr": str(config["asr"]["processor_version"]),
        "ocr": str(config["ocr"]["processor_version"]),
        "ocr_merge": f"ocr-merge|{config['ocr']['processor_version']}",
    }


def ocr_binary_path() -> Path:
    return PROJECT_ROOT / str(load_media_config()["ocr"]["binary"])


def compile_ocr_binary() -> Path:
    target = ocr_binary_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    completed = subprocess.run(
        ["swiftc", "-O", str(OCR_SOURCE), "-o", str(target)],
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )
    if completed.returncode != 0:
        raise MediaProcessingError(f"vision_ocr compile failed: {completed.stderr[-500:]}")
    os.chmod(target, 0o755)
    return target


@lru_cache(maxsize=2)
def pinned_whisper_model_path(*, local_files_only: bool = False) -> Path:
    config = load_media_config()["asr"]
    installed = package_version("mlx-whisper")
    if installed != config["library_version"]:
        raise MediaProcessingError(
            f"mlx-whisper version mismatch: installed {installed}, expected {config['library_version']}"
        )
    path = snapshot_download(
        repo_id=str(config["model_id"]),
        revision=str(config["model_revision"]),
        local_files_only=local_files_only,
    )
    return Path(path)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _relative(path: Path) -> str:
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(resolved)


def _resolved(local_path: str) -> Path:
    path = Path(local_path)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _read_json_object(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _safe_float(value: Any) -> Optional[float]:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return round(parsed, 4) if math.isfinite(parsed) else None


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def register_artifact(
    connection: sqlite3.Connection,
    *,
    content_id: int,
    artifact_type: str,
    path: Path,
    processor_version: str,
    captured_at: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Artifact:
    if not path.is_file():
        raise MediaProcessingError(f"artifact does not exist: {path}")
    sha256 = file_sha256(path)
    local_path = _relative(path)
    created_at = now_utc()
    connection.execute(
        """
        INSERT INTO evidence_artifacts(
            content_id, artifact_type, local_path, status, byte_size, sha256,
            captured_at, processor_version, metadata_json, created_at
        ) VALUES (?, ?, ?, 'available', ?, ?, ?, ?, ?, ?)
        ON CONFLICT(content_id, artifact_type, local_path) DO UPDATE SET
            status='available', byte_size=excluded.byte_size, sha256=excluded.sha256,
            captured_at=excluded.captured_at, processor_version=excluded.processor_version,
            metadata_json=excluded.metadata_json
        """,
        (
            content_id, artifact_type, local_path, path.stat().st_size, sha256,
            captured_at or created_at, processor_version,
            json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True), created_at,
        ),
    )
    row = connection.execute(
        """
        SELECT id FROM evidence_artifacts
        WHERE content_id=? AND artifact_type=? AND local_path=?
        """,
        (content_id, artifact_type, local_path),
    ).fetchone()
    if row is None:
        raise RuntimeError("artifact upsert returned no row")
    return Artifact(
        id=int(row["id"]),
        content_id=content_id,
        artifact_type=artifact_type,
        local_path=local_path,
        sha256=sha256,
        processor_version=processor_version,
    )


def _claim_processing_slot(
    connection: sqlite3.Connection,
    *,
    content_id: int,
    source_sha256: str,
    processor_type: str,
    processor_version: str,
    source_aliases: Iterable[str] = (),
) -> tuple[int, Optional[Artifact]]:
    identities = list(dict.fromkeys([source_sha256, *source_aliases]))
    if processor_type == "download":
        placeholders = ",".join("?" for _ in identities)
        allowed_versions = (
            _download_versions("video")
            if processor_version == VIDEO_DOWNLOAD_VERSION
            else _download_versions("image")
        )
        version_placeholders = ",".join("?" for _ in allowed_versions)
        matching_rows = connection.execute(
            f"""
            SELECT m.*, e.artifact_type, e.local_path, e.sha256 artifact_sha256,
                   e.processor_version artifact_processor_version
            FROM media_processing_slots m
            LEFT JOIN evidence_artifacts e ON e.id=m.output_artifact_id
            WHERE m.content_id=? AND m.source_sha256 IN ({placeholders})
              AND m.processor_type='download'
              AND m.processor_version IN ({version_placeholders})
            ORDER BY m.id DESC
            """,
            (content_id, *identities, *sorted(allowed_versions)),
        ).fetchall()
        succeeded = next(
            (item for item in matching_rows if item["status"] == "succeeded"), None
        )
        if succeeded is not None:
            if succeeded["output_artifact_id"] is None or not succeeded["local_path"]:
                raise MediaProcessingError("successful media slot has no output artifact")
            return int(succeeded["id"]), Artifact(
                id=int(succeeded["output_artifact_id"]),
                content_id=content_id,
                artifact_type=str(succeeded["artifact_type"]),
                local_path=str(succeeded["local_path"]),
                sha256=str(succeeded["artifact_sha256"]),
                processor_version=str(
                    succeeded["artifact_processor_version"]
                    or succeeded["processor_version"]
                ),
            )
        exhausted = next(
            (
                item
                for item in matching_rows
                if item["status"] == "terminal_failed"
                or int(item["attempt_count"]) >= MAX_MEDIA_DOWNLOAD_ATTEMPTS
            ),
            None,
        )
        if exhausted is not None:
            raise TerminalMediaSlotError(
                f"media download slot {exhausted['id']} is terminal for this source"
            )
        row = next(
            (
                item
                for item in matching_rows
                if item["source_sha256"] == source_sha256
                and item["processor_version"] == processor_version
            ),
            None,
        )
    else:
        row = connection.execute(
            """
            SELECT m.*, e.artifact_type, e.local_path, e.sha256 artifact_sha256,
                   e.processor_version artifact_processor_version
            FROM media_processing_slots m
            LEFT JOIN evidence_artifacts e ON e.id=m.output_artifact_id
            WHERE m.content_id=? AND m.source_sha256=?
              AND m.processor_type=? AND m.processor_version=?
            """,
            (content_id, source_sha256, processor_type, processor_version),
        ).fetchone()
    if row is not None and row["status"] == "succeeded":
        if row["output_artifact_id"] is None or not row["local_path"]:
            raise MediaProcessingError("successful media slot has no output artifact")
        return int(row["id"]), Artifact(
            id=int(row["output_artifact_id"]),
            content_id=content_id,
            artifact_type=str(row["artifact_type"]),
            local_path=str(row["local_path"]),
            sha256=str(row["artifact_sha256"]),
            processor_version=str(
                row["artifact_processor_version"] or row["processor_version"]
            ),
        )
    if row is not None and (
        row["status"] == "terminal_failed"
        or (
            processor_type in BOUNDED_PROCESSOR_TYPES
            and int(row["attempt_count"]) >= MAX_MEDIA_PROCESSING_ATTEMPTS
        )
    ):
        raise TerminalMediaSlotError(
            f"media {processor_type} slot {row['id']} is terminal for this processor version"
        )
    if row is not None and row["status"] == "running":
        raise MediaProcessingError(f"media slot {row['id']} is already running")
    captured_at = now_utc()
    if row is None:
        cursor = connection.execute(
            """
            INSERT INTO media_processing_slots(
                content_id, source_sha256, processor_type, processor_version,
                status, attempt_count, created_at, updated_at
            ) VALUES (?, ?, ?, ?, 'running', 1, ?, ?)
            """,
            (content_id, source_sha256, processor_type, processor_version, captured_at, captured_at),
        )
        if cursor.lastrowid is None:
            raise RuntimeError("media slot insert returned no id")
        return int(cursor.lastrowid), None
    connection.execute(
        """
        UPDATE media_processing_slots
        SET status='running', attempt_count=attempt_count+1,
            error_message=NULL, updated_at=? WHERE id=?
        """,
        (captured_at, row["id"]),
    )
    return int(row["id"]), None


def _run_processing_slot(
    *,
    db_path: Path,
    content_id: int,
    source_sha256: str,
    processor_type: str,
    processor_version: str,
    artifact_type: str,
    produce: Callable[[], Path],
    metadata: Optional[Dict[str, Any]] = None,
    source_aliases: Iterable[str] = (),
) -> Artifact:
    with connect(db_path) as connection, transaction(connection):
        slot_id, cached = _claim_processing_slot(
            connection,
            content_id=content_id,
            source_sha256=source_sha256,
            processor_type=processor_type,
            processor_version=processor_version,
            source_aliases=source_aliases,
        )
    if cached is not None:
        return cached
    try:
        output_path = produce()
        with connect(db_path) as connection, transaction(connection):
            artifact = register_artifact(
                connection,
                content_id=content_id,
                artifact_type=artifact_type,
                path=output_path,
                processor_version=processor_version,
                metadata=metadata,
            )
            connection.execute(
                """
                UPDATE media_processing_slots
                SET status='succeeded', output_artifact_id=?, updated_at=? WHERE id=?
                """,
                (artifact.id, now_utc(), slot_id),
            )
        return artifact
    except Exception as exc:
        with connect(db_path) as connection, transaction(connection):
            connection.execute(
                """
                UPDATE media_processing_slots
                SET status=CASE
                        WHEN processor_type IN ('download','frames','asr','ocr')
                             AND attempt_count>=?
                        THEN 'terminal_failed'
                        ELSE 'retryable_failed'
                    END,
                    error_message=?, updated_at=? WHERE id=?
                """,
                (
                    MAX_MEDIA_DOWNLOAD_ATTEMPTS,
                    f"{type(exc).__name__}: {exc}"[:500],
                    now_utc(),
                    slot_id,
                ),
            )
        raise


def _probe_duration(path: Path) -> float:
    completed = subprocess.run(
        [
            "ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "default=nw=1:nk=1", str(path),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    try:
        return max(0.0, float(completed.stdout.strip()))
    except ValueError:
        return 0.0


def _valid_media(path: Path) -> bool:
    return path.is_file() and path.stat().st_size > 1024 and _probe_duration(path) > 0


def _download_video(urls: Iterable[str], target: Path) -> Path:
    if _valid_media(target):
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    errors: List[str] = []
    for index, url in enumerate(dict.fromkeys(urls)):
        candidate = target.with_name(f".{target.name}.candidate-{index}")
        candidate.unlink(missing_ok=True)
        request = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "video/*,*/*;q=0.8"},
        )
        try:
            with urllib.request.urlopen(request, timeout=90) as response, candidate.open("wb") as handle:
                while True:
                    block = response.read(1024 * 1024)
                    if not block:
                        break
                    handle.write(block)
            if _valid_media(candidate):
                candidate.replace(target)
                return target
            errors.append(f"candidate {index} was not a playable video")
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as exc:
            errors.append(f"candidate {index}: {type(exc).__name__}")
        finally:
            candidate.unlink(missing_ok=True)
    raise MediaProcessingError("media download failed: " + " | ".join(errors[-3:]))


def download_video_sources(
    content_id: int,
    urls: Iterable[str],
    *,
    db_path: Path = DEFAULT_DB,
) -> Artifact:
    values, source_sha256 = _media_source_identity("video", urls)
    if not values:
        raise MediaProcessingError("provider returned no supported video source")
    legacy_sha256 = _legacy_media_source_sha256(values)
    with connect(db_path) as connection:
        content = connection.execute(
            "SELECT link_id FROM content_items WHERE id=?", (content_id,)
        ).fetchone()
    if content is None:
        raise MediaProcessingError(f"unknown content {content_id}")
    target = (
        MEDIA_ROOT
        / str(content["link_id"])
        / "downloads"
        / source_sha256
        / "source.mp4"
    )
    return _run_processing_slot(
        db_path=db_path,
        content_id=content_id,
        source_sha256=source_sha256,
        processor_type="download",
        processor_version=VIDEO_DOWNLOAD_VERSION,
        artifact_type="media",
        produce=lambda: _download_video(values, target),
        metadata={"source_count": len(values), "source_sha256": source_sha256},
        source_aliases=(legacy_sha256,),
    )


def store_media_source_manifest(
    content_id: int,
    *,
    media_kind: str,
    urls: Iterable[str],
    raw_response_id: int,
    db_path: Path = DEFAULT_DB,
) -> Optional[Artifact]:
    """Persist normalized provider media URLs for the later local-compute jobs."""

    values, source_sha256 = _media_source_identity(media_kind, urls)
    if not values:
        return None
    with connect(db_path) as connection:
        content = connection.execute(
            "SELECT link_id FROM content_items WHERE id=?", (content_id,)
        ).fetchone()
    if content is None:
        raise MediaProcessingError(f"unknown content {content_id}")
    target = (
        MEDIA_ROOT
        / str(content["link_id"])
        / "sources"
        / f"source-{raw_response_id}-{source_sha256[:12]}.json"
    )
    payload = {
        "schema_version": MEDIA_SOURCE_VERSION,
        "media_kind": media_kind,
        "urls": values,
        "source_sha256": source_sha256,
        "raw_response_id": raw_response_id,
        "captured_at": now_utc(),
    }
    if target.exists():
        existing = _read_json_object(target)
        immutable_fields = ("media_kind", "urls", "source_sha256", "raw_response_id")
        if any(existing.get(key) != payload[key] for key in immutable_fields):
            raise MediaProcessingError(f"media source manifest collision: {target}")
    else:
        _atomic_json(target, payload)
    with connect(db_path) as connection, transaction(connection):
        return register_artifact(
            connection,
            content_id=content_id,
            artifact_type="media_source",
            path=target,
            processor_version=MEDIA_SOURCE_VERSION,
            metadata={
                "media_kind": media_kind,
                "source_count": len(values),
                "source_sha256": source_sha256,
                "raw_response_id": raw_response_id,
            },
        )


def _valid_image(path: Path) -> bool:
    if not path.is_file() or path.stat().st_size <= 512:
        return False
    header = path.read_bytes()[:16]
    return bool(
        header.startswith(b"\xff\xd8\xff")
        or header.startswith(b"\x89PNG\r\n\x1a\n")
        or (header.startswith(b"RIFF") and header[8:12] == b"WEBP")
        or header.startswith((b"GIF87a", b"GIF89a"))
    )


def _download_images(urls: Iterable[str], target_dir: Path) -> Path:
    values = list(dict.fromkeys(urls))
    target_dir.mkdir(parents=True, exist_ok=True)
    paths: List[Path] = []
    errors: List[str] = []
    for index, url in enumerate(values):
        target = target_dir / f"image-{index:03d}.bin"
        if _valid_image(target):
            paths.append(target)
            continue
        request = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "image/*,*/*;q=0.8"},
        )
        temporary = target.with_name(f".{target.name}.tmp")
        temporary.unlink(missing_ok=True)
        try:
            with urllib.request.urlopen(request, timeout=60) as response, temporary.open("wb") as handle:
                while True:
                    block = response.read(1024 * 1024)
                    if not block:
                        break
                    handle.write(block)
            if not _valid_image(temporary):
                errors.append(f"image {index} was not a supported image")
                continue
            temporary.replace(target)
            paths.append(target)
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as exc:
            errors.append(f"image {index}: {type(exc).__name__}")
        finally:
            temporary.unlink(missing_ok=True)
    if not paths:
        raise MediaProcessingError("image download failed: " + " | ".join(errors[-3:]))
    manifest = target_dir / "manifest.json"
    _atomic_json(
        manifest,
        {
            "status": "complete" if len(paths) == len(values) else "partial",
            "image_paths": [_relative(path) for path in paths],
            "frames": [
                {"path": _relative(path), "sha256": file_sha256(path)} for path in paths
            ],
            "errors": errors,
        },
    )
    return manifest


def download_image_sources(
    content_id: int,
    urls: Iterable[str],
    *,
    db_path: Path = DEFAULT_DB,
) -> Artifact:
    values, source_sha256 = _media_source_identity("image", urls)
    if not values:
        raise MediaProcessingError("provider returned no supported image source")
    legacy_sha256 = _legacy_media_source_sha256(values)
    with connect(db_path) as connection:
        content = connection.execute(
            "SELECT link_id FROM content_items WHERE id=?", (content_id,)
        ).fetchone()
    if content is None:
        raise MediaProcessingError(f"unknown content {content_id}")
    target_dir = (
        MEDIA_ROOT
        / str(content["link_id"])
        / "downloads"
        / source_sha256
        / "images"
    )
    return _run_processing_slot(
        db_path=db_path,
        content_id=content_id,
        source_sha256=source_sha256,
        processor_type="download",
        processor_version=IMAGE_DOWNLOAD_VERSION,
        artifact_type="media_manifest",
        produce=lambda: _download_images(values, target_dir),
        metadata={"source_count": len(values), "source_sha256": source_sha256},
        source_aliases=(legacy_sha256,),
    )


def _extract_frames(media_path: Path, target_dir: Path) -> Path:
    config = load_media_config()["frames"]
    duration = _probe_duration(media_path)
    if duration <= 0:
        raise MediaProcessingError("media has no readable duration")
    count = min(int(config["maximum_frames"]), max(6, round(duration / 3)))
    target_dir.mkdir(parents=True, exist_ok=True)
    frame_paths: List[Path] = []
    for index in range(count):
        timestamp = min(max(0.1, (index + 0.5) * duration / count), max(0.1, duration - 0.1))
        target = target_dir / f"frame-{index:03d}.jpg"
        completed = subprocess.run(
            [
                "ffmpeg", "-y", "-nostdin", "-loglevel", "error", "-ss", str(timestamp),
                "-i", str(media_path), "-frames:v", "1",
                "-vf", f"scale={int(config['target_width'])}:-1", "-q:v", "3", str(target),
            ],
            check=False,
            capture_output=True,
            timeout=60,
        )
        if completed.returncode == 0 and target.is_file() and target.stat().st_size > 1024:
            frame_paths.append(target)
    if not frame_paths:
        raise MediaProcessingError("no frames were extracted")
    contact_sheet = target_dir / "contact-sheet.jpg"
    subprocess.run(
        [
            "ffmpeg", "-y", "-nostdin", "-loglevel", "error", "-framerate", "1",
            "-i", str(target_dir / "frame-%03d.jpg"), "-vf",
            "scale=480:-1,tile=4x6:padding=4:margin=4", "-frames:v", "1", str(contact_sheet),
        ],
        check=False,
        capture_output=True,
        timeout=90,
    )
    manifest = target_dir / "frames.json"
    _atomic_json(
        manifest,
        {
            "status": "success",
            "duration_seconds": round(duration, 3),
            "frames": [
                {"path": _relative(path), "sha256": file_sha256(path)} for path in frame_paths
            ],
            "contact_sheet": _relative(contact_sheet) if contact_sheet.is_file() else None,
        },
    )
    return manifest


def _frame_paths(manifest_path: Path) -> List[Path]:
    value = json.loads(manifest_path.read_text(encoding="utf-8"))
    return [_resolved(str(item["path"])) for item in value.get("frames", [])]


def _run_ocr(manifest_path: Path, target: Path) -> Path:
    binary = ocr_binary_path()
    if not binary.is_file():
        raise MediaProcessingError(f"OCR binary is missing: {binary}")
    frames = _frame_paths(manifest_path)
    completed = subprocess.run(
        [str(binary), *[str(path) for path in frames]],
        check=False,
        capture_output=True,
        text=True,
        timeout=max(90, len(frames) * 12),
    )
    observations: List[Dict[str, Any]] = []
    texts: List[str] = []
    for line in completed.stdout.splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(item, dict):
            continue
        observations.append(item)
        text = "\n".join(str(item.get("text") or "").splitlines()).strip()
        if text and text not in texts:
            texts.append(text)
    if completed.returncode != 0 or len(observations) != len(frames):
        raise MediaProcessingError(
            f"OCR incomplete: return={completed.returncode} observations={len(observations)}/{len(frames)}"
        )
    _atomic_json(
        target,
        {
            "status": "success",
            "processor_version": processor_versions()["ocr"],
            "source_count": len(frames),
            "ocr_observation_count": len(observations),
            "combined_text": "\n".join(texts),
            "observations": observations,
        },
    )
    return target


def _run_asr(media_path: Path, target: Path) -> Path:
    import mlx_whisper  # type: ignore[import-untyped]

    config = load_media_config()["asr"]
    model_path = pinned_whisper_model_path()
    started = time.monotonic()
    try:
        raw = mlx_whisper.transcribe(
            str(media_path),
            path_or_hf_repo=str(model_path),
            language=str(config["language"]),
            verbose=None,
            word_timestamps=False,
            initial_prompt="汽车，懂车帝，AI小懂，二手车，新车，选车，买车，卖车，试驾，保养，维修，车型，价格，配置。",
            condition_on_previous_text=True,
        )
    except RuntimeError as exc:
        if not str(exc).startswith("Failed to load audio"):
            raise
        _atomic_json(
            target,
            {
                "status": "unavailable",
                "reason": "audio_decode_failed",
                "processor_version": processor_versions()["asr"],
                "model_id": config["model_id"],
                "model_revision": config["model_revision"],
                "language": config["language"],
                "text": "",
                "segments": [],
            },
        )
        return target
    segments = [
        {
            "start": _safe_float(item.get("start")),
            "end": _safe_float(item.get("end")),
            "text": str(item.get("text") or "").strip(),
            "avg_logprob": _safe_float(item.get("avg_logprob")),
            "no_speech_prob": _safe_float(item.get("no_speech_prob")),
        }
        for item in raw.get("segments", [])
        if isinstance(item, dict)
    ]
    _atomic_json(
        target,
        {
            "status": "success",
            "processor_version": processor_versions()["asr"],
            "model_id": config["model_id"],
            "model_revision": config["model_revision"],
            "language": raw.get("language") or config["language"],
            "text": str(raw.get("text") or "").strip(),
            "segments": segments,
            "elapsed_seconds": round(time.monotonic() - started, 3),
        },
    )
    return target


def process_video_evidence(
    content_id: int,
    media_path: Path,
    *,
    db_path: Path = DEFAULT_DB,
) -> Dict[str, Artifact]:
    if not _valid_media(media_path):
        raise MediaProcessingError(f"invalid media: {media_path}")
    versions = processor_versions()
    with connect(db_path) as connection, transaction(connection):
        media = register_artifact(
            connection,
            content_id=content_id,
            artifact_type="media",
            path=media_path,
            processor_version="provider-media-v8.0",
        )
        content = connection.execute(
            "SELECT link_id FROM content_items WHERE id=?", (content_id,)
        ).fetchone()
        if content is None:
            raise MediaProcessingError(f"unknown content {content_id}")
        link_id = str(content["link_id"])
    content_root = MEDIA_ROOT / link_id
    frames = _run_processing_slot(
        db_path=db_path,
        content_id=content_id,
        source_sha256=media.sha256,
        processor_type="frames",
        processor_version=versions["frames"],
        artifact_type="frames_manifest",
        produce=lambda: _extract_frames(media_path, content_root / "frames"),
    )
    asr = _run_processing_slot(
        db_path=db_path,
        content_id=content_id,
        source_sha256=media.sha256,
        processor_type="asr",
        processor_version=versions["asr"],
        artifact_type="asr",
        produce=lambda: _run_asr(media_path, content_root / "asr.json"),
    )
    ocr = _run_processing_slot(
        db_path=db_path,
        content_id=content_id,
        source_sha256=frames.sha256,
        processor_type="ocr",
        processor_version=versions["ocr"],
        artifact_type="ocr",
        produce=lambda: _run_ocr(_resolved(frames.local_path), content_root / "ocr.json"),
    )
    return {"media": media, "frames": frames, "asr": asr, "ocr": ocr}


def process_image_evidence(
    content_id: int,
    manifest_path: Path,
    *,
    db_path: Path = DEFAULT_DB,
) -> Dict[str, Artifact]:
    if not manifest_path.is_file():
        raise MediaProcessingError(f"image manifest does not exist: {manifest_path}")
    versions = processor_versions()
    with connect(db_path) as connection, transaction(connection):
        media_manifest = register_artifact(
            connection,
            content_id=content_id,
            artifact_type="media_manifest",
            path=manifest_path,
            processor_version=IMAGE_DOWNLOAD_VERSION,
        )
        content = connection.execute(
            "SELECT link_id FROM content_items WHERE id=?", (content_id,)
        ).fetchone()
        if content is None:
            raise MediaProcessingError(f"unknown content {content_id}")
    target = MEDIA_ROOT / str(content["link_id"]) / "ocr.json"
    ocr = _run_processing_slot(
        db_path=db_path,
        content_id=content_id,
        source_sha256=media_manifest.sha256,
        processor_type="ocr",
        processor_version=versions["ocr"],
        artifact_type="ocr",
        produce=lambda: _run_ocr(manifest_path, target),
    )
    return {"media": media_manifest, "ocr": ocr}


def _latest_media_source(content_id: int, *, db_path: Path) -> Optional[Dict[str, Any]]:
    with connect(db_path) as connection:
        row = connection.execute(
            """
            SELECT local_path FROM evidence_artifacts
            WHERE content_id=? AND artifact_type='media_source' AND status='available'
            ORDER BY id DESC LIMIT 1
            """,
            (content_id,),
        ).fetchone()
    if row is None:
        return None
    value = _read_json_object(_resolved(str(row["local_path"])))
    media_kind = str(value.get("media_kind") or "")
    raw_urls = value.get("urls")
    if not isinstance(raw_urls, list):
        return None
    try:
        urls, source_sha256 = _media_source_identity(media_kind, raw_urls)
    except MediaProcessingError:
        return None
    if not urls:
        return None
    return {
        **value,
        "media_kind": media_kind,
        "urls": urls,
        "source_sha256": source_sha256,
    }


def _matching_download_slots(
    connection: sqlite3.Connection,
    *,
    content_id: int,
    source: Dict[str, Any],
) -> List[sqlite3.Row]:
    urls = [str(value) for value in source.get("urls", [])]
    identities = list(
        dict.fromkeys(
            [
                str(source["source_sha256"]),
                _legacy_media_source_sha256(urls),
            ]
        )
    )
    placeholders = ",".join("?" for _ in identities)
    versions = sorted(_download_versions(str(source["media_kind"])))
    version_placeholders = ",".join("?" for _ in versions)
    return connection.execute(
        f"""
        SELECT * FROM media_processing_slots
        WHERE content_id=? AND processor_type='download'
          AND source_sha256 IN ({placeholders})
          AND processor_version IN ({version_placeholders})
        ORDER BY id DESC
        """,
        (content_id, *identities, *versions),
    ).fetchall()


def _effective_download_slot(rows: Iterable[sqlite3.Row]) -> Optional[sqlite3.Row]:
    values = list(rows)
    succeeded = next((row for row in values if row["status"] == "succeeded"), None)
    if succeeded is not None:
        return succeeded
    terminal = next(
        (
            row
            for row in values
            if row["status"] == "terminal_failed"
            or int(row["attempt_count"]) >= MAX_MEDIA_DOWNLOAD_ATTEMPTS
        ),
        None,
    )
    return terminal or (values[0] if values else None)


def get_media_source_state(
    content_id: int, *, db_path: Path = DEFAULT_DB
) -> Optional[Dict[str, Any]]:
    """Return the latest normalized source and its effective same-source download slot."""

    source = _latest_media_source(content_id, db_path=db_path)
    if source is None:
        return None
    with connect(db_path) as connection:
        row = _effective_download_slot(
            _matching_download_slots(
                connection,
                content_id=content_id,
                source=source,
            )
        )
    slot: Optional[Dict[str, Any]] = None
    if row is not None:
        status = str(row["status"])
        if status != "succeeded" and (
            status == "terminal_failed"
            or int(row["attempt_count"]) >= MAX_MEDIA_DOWNLOAD_ATTEMPTS
        ):
            status = "terminal_failed"
        slot = {
            "id": int(row["id"]),
            "processor_version": str(row["processor_version"]),
            "status": status,
            "attempt_count": int(row["attempt_count"]),
            "output_artifact_id": (
                int(row["output_artifact_id"])
                if row["output_artifact_id"] is not None
                else None
            ),
        }
    raw_response_id = source.get("raw_response_id")
    return {
        "raw_response_id": int(raw_response_id) if raw_response_id is not None else None,
        "media_kind": str(source["media_kind"]),
        "urls": list(source["urls"]),
        "source_sha256": str(source["source_sha256"]),
        "download_slot": slot,
    }


def _existing_complete_evidence(
    content_id: int, *, db_path: Path
) -> Optional[Dict[str, int]]:
    with connect(db_path) as connection:
        content = connection.execute(
            "SELECT content_type FROM content_items WHERE id=?", (content_id,)
        ).fetchone()
        rows = connection.execute(
            """
            SELECT id,artifact_type FROM evidence_artifacts
            WHERE content_id=? AND status='available'
              AND artifact_type IN (
                  'media','media_manifest','asr','transcript','media_transcript','ocr','media_ocr'
              )
            ORDER BY id DESC
            """,
            (content_id,),
        ).fetchall()
    if content is None:
        return None
    latest: Dict[str, int] = {}
    for row in rows:
        artifact_type = str(row["artifact_type"])
        category = (
            "media" if artifact_type in {"media", "media_manifest"}
            else "asr" if artifact_type in {"asr", "transcript", "media_transcript"}
            else "ocr"
        )
        latest.setdefault(category, int(row["id"]))
    required = {"media", "ocr", "asr"} if content["content_type"] == "video" else {"media", "ocr"}
    return latest if required <= latest.keys() else None


def process_content_media(
    content_id: int,
    *,
    download_only: bool = False,
    db_path: Path = DEFAULT_DB,
) -> Dict[str, Any]:
    source = _latest_media_source(content_id, db_path=db_path)
    if source is None:
        existing = _existing_complete_evidence(content_id, db_path=db_path)
        if existing is not None:
            return {
                "content_id": content_id,
                "status": "evidence_ready",
                "source": "existing_local_evidence",
                "artifacts": existing,
            }
        return {"content_id": content_id, "status": "no_source"}
    media_kind = str(source.get("media_kind") or "")
    urls = [str(value) for value in source.get("urls", []) if isinstance(value, str)]
    if media_kind == "video":
        media = download_video_sources(content_id, urls, db_path=db_path)
        if download_only:
            return {
                "content_id": content_id,
                "status": "downloaded",
                "media_kind": media_kind,
                "artifact_id": media.id,
            }
        artifacts = process_video_evidence(
            content_id, _resolved(media.local_path), db_path=db_path
        )
    elif media_kind == "image":
        media = download_image_sources(content_id, urls, db_path=db_path)
        if download_only:
            return {
                "content_id": content_id,
                "status": "downloaded",
                "media_kind": media_kind,
                "artifact_id": media.id,
            }
        artifacts = process_image_evidence(
            content_id, _resolved(media.local_path), db_path=db_path
        )
    else:
        raise MediaProcessingError(f"invalid media source kind for content {content_id}")
    return {
        "content_id": content_id,
        "status": "evidence_ready",
        "media_kind": media_kind,
        "artifacts": {name: artifact.id for name, artifact in artifacts.items()},
    }


def recover_stale_media_processing_slots(
    *,
    db_path: Path = DEFAULT_DB,
    processor_types: Optional[Iterable[str]] = None,
    processor_version_by_type: Optional[Dict[str, str]] = None,
    stale_after_seconds: int = STALE_MEDIA_SLOT_SECONDS,
) -> Dict[str, int]:
    """CAS-recover stale workers and normalize exhausted retryable slots."""

    selected_types = (
        set(processor_types) if processor_types is not None else set(BOUNDED_PROCESSOR_TYPES)
    )
    invalid = selected_types - BOUNDED_PROCESSOR_TYPES
    if invalid:
        raise ValueError(f"unsupported bounded processor types: {sorted(invalid)}")
    recovered_at = now_utc()
    cutoff = (
        datetime.now(timezone.utc) - timedelta(seconds=max(1, stale_after_seconds))
    ).isoformat(timespec="seconds").replace("+00:00", "Z")
    counts = {
        "stale_candidates": 0,
        "recovered": 0,
        "retryable_failed": 0,
        "terminal_failed": 0,
        "cas_conflicts": 0,
        "exhausted_normalized": 0,
    }
    if not selected_types:
        return counts
    placeholders = ",".join("?" for _ in selected_types)
    ordered_types = sorted(selected_types)
    with connect(db_path) as connection, transaction(connection):
        stale_rows = connection.execute(
            f"""
            SELECT id,processor_type,attempt_count,updated_at
            FROM media_processing_slots
            WHERE status='running' AND updated_at<=?
              AND processor_type IN ({placeholders})
            ORDER BY id
            """,
            (cutoff, *ordered_types),
        ).fetchall()
        counts["stale_candidates"] = len(stale_rows)
        for row in stale_rows:
            status = (
                "terminal_failed"
                if int(row["attempt_count"]) >= MAX_MEDIA_PROCESSING_ATTEMPTS
                else "retryable_failed"
            )
            cursor = connection.execute(
                """
                UPDATE media_processing_slots
                SET status=?, error_message=?, updated_at=?
                WHERE id=? AND status='running' AND updated_at=?
                """,
                (
                    status,
                    f"stale running slot recovered after {stale_after_seconds} seconds",
                    recovered_at,
                    row["id"],
                    row["updated_at"],
                ),
            )
            if cursor.rowcount == 1:
                counts["recovered"] += 1
                counts[status] += 1
            else:
                counts["cas_conflicts"] += 1

        exhausted_rows = connection.execute(
            f"""
            SELECT id,processor_type,processor_version,updated_at
            FROM media_processing_slots
            WHERE status='retryable_failed' AND attempt_count>=?
              AND processor_type IN ({placeholders})
            ORDER BY id
            """,
            (MAX_MEDIA_PROCESSING_ATTEMPTS, *ordered_types),
        ).fetchall()
        for row in exhausted_rows:
            expected_version = (
                processor_version_by_type.get(str(row["processor_type"]))
                if processor_version_by_type is not None
                else None
            )
            if expected_version is not None and row["processor_version"] != expected_version:
                continue
            cursor = connection.execute(
                """
                UPDATE media_processing_slots
                SET status='terminal_failed',
                    error_message=COALESCE(error_message, 'attempt limit exhausted'),
                    updated_at=?
                WHERE id=? AND status='retryable_failed' AND updated_at=?
                  AND attempt_count>=?
                """,
                (
                    recovered_at,
                    row["id"],
                    row["updated_at"],
                    MAX_MEDIA_PROCESSING_ATTEMPTS,
                ),
            )
            if cursor.rowcount == 1:
                counts["exhausted_normalized"] += 1
            else:
                counts["cas_conflicts"] += 1
    return counts


def _download_slot_identity(source: Dict[str, Any]) -> Optional[tuple[str, str]]:
    media_kind = str(source.get("media_kind") or "")
    raw_urls = source.get("urls", [])
    if not isinstance(raw_urls, list) or media_kind not in {"video", "image"}:
        return None
    values, source_sha256 = _media_source_identity(media_kind, raw_urls)
    if not values:
        return None
    processor_version = (
        VIDEO_DOWNLOAD_VERSION if media_kind == "video" else IMAGE_DOWNLOAD_VERSION
    )
    return source_sha256, processor_version


def _queue_content_ids(
    *,
    stage: str,
    limit: int,
    db_path: Path,
    published_start: Optional[str] = None,
    published_end: Optional[str] = None,
) -> List[int]:
    if stage not in {"download", "process"}:
        raise ValueError(f"unknown media queue stage: {stage}")
    with connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT c.id,c.content_type,
                   (SELECT source.local_path
                    FROM evidence_artifacts source
                    WHERE source.content_id=c.id
                      AND source.artifact_type='media_source'
                      AND source.status='available'
                    ORDER BY source.id DESC LIMIT 1) AS source_path
            FROM content_items c
            WHERE EXISTS (
                SELECT 1 FROM evidence_artifacts source
                WHERE source.content_id=c.id AND source.artifact_type='media_source'
                  AND source.status='available'
            )
              AND (? IS NULL OR c.published_at>=?)
              AND (? IS NULL OR c.published_at<?)
            ORDER BY (c.published_at IS NULL) ASC, c.published_at DESC, c.id DESC
            """,
            (published_start, published_start, published_end, published_end),
        ).fetchall()
        slot_rows = connection.execute(
            """
            SELECT id,content_id,source_sha256,processor_type,processor_version,
                   status,attempt_count,output_artifact_id
            FROM media_processing_slots
            """
        ).fetchall()
        artifact_rows = connection.execute(
            "SELECT id,sha256 FROM evidence_artifacts WHERE status='available'"
        ).fetchall()
    slots_by_content: Dict[int, List[sqlite3.Row]] = {}
    for slot_row in slot_rows:
        slots_by_content.setdefault(int(slot_row["content_id"]), []).append(slot_row)
    artifact_sha256s = {
        int(artifact["id"]): str(artifact["sha256"]) for artifact in artifact_rows
    }
    current_versions = processor_versions() if stage == "process" else {}

    def slot_for(
        content_slots: Iterable[sqlite3.Row],
        *,
        source_sha256: str,
        processor_type: str,
        processor_version: str,
    ) -> Optional[sqlite3.Row]:
        return next(
            (
                item
                for item in sorted(content_slots, key=lambda value: int(value["id"]), reverse=True)
                if item["source_sha256"] == source_sha256
                and item["processor_type"] == processor_type
                and item["processor_version"] == processor_version
            ),
            None,
        )

    def slot_is_terminal(slot: Optional[sqlite3.Row]) -> bool:
        return slot is not None and (
            slot["status"] == "terminal_failed"
            or int(slot["attempt_count"]) >= MAX_MEDIA_PROCESSING_ATTEMPTS
        )

    def slot_is_active(slot: Optional[sqlite3.Row]) -> bool:
        return slot is not None and slot["status"] == "running"

    selected: List[int] = []
    for row in rows:
        content_id = int(row["id"])
        source = _read_json_object(_resolved(str(row["source_path"] or "")))
        identity = _download_slot_identity(source)
        if identity is None:
            continue
        source_sha256, _download_version = identity
        media_kind = str(source.get("media_kind") or "")
        raw_urls = source.get("urls", [])
        if not isinstance(raw_urls, list):
            continue
        normalized_urls, _ = _media_source_identity(media_kind, raw_urls)
        source_identities = {
            source_sha256,
            _legacy_media_source_sha256(normalized_urls),
        }
        allowed_download_versions = _download_versions(media_kind)
        content_slots = slots_by_content.get(content_id, [])
        download_slots = [
            item
            for item in content_slots
            if item["processor_type"] == "download"
            and item["source_sha256"] in source_identities
            and item["processor_version"] in allowed_download_versions
        ]
        effective_download = _effective_download_slot(download_slots)
        if stage == "download":
            if effective_download is not None and (
                effective_download["status"] in {"succeeded", "running", "terminal_failed"}
                or int(effective_download["attempt_count"]) >= MAX_MEDIA_DOWNLOAD_ATTEMPTS
            ):
                continue
        else:
            if effective_download is None or effective_download["status"] != "succeeded":
                continue
            output_artifact_id = effective_download["output_artifact_id"]
            if output_artifact_id is None:
                continue
            media_sha256 = artifact_sha256s.get(int(output_artifact_id))
            if media_sha256 is None:
                continue
            if row["content_type"] == "video":
                frames = slot_for(
                    content_slots,
                    source_sha256=media_sha256,
                    processor_type="frames",
                    processor_version=current_versions["frames"],
                )
                asr = slot_for(
                    content_slots,
                    source_sha256=media_sha256,
                    processor_type="asr",
                    processor_version=current_versions["asr"],
                )
                required_slots: List[Optional[sqlite3.Row]] = [frames, asr]
                if frames is not None and frames["status"] == "succeeded":
                    frame_artifact_id = frames["output_artifact_id"]
                    frames_sha256 = (
                        artifact_sha256s.get(int(frame_artifact_id))
                        if frame_artifact_id is not None
                        else None
                    )
                    if frames_sha256 is None:
                        continue
                    required_slots.append(
                        slot_for(
                            content_slots,
                            source_sha256=frames_sha256,
                            processor_type="ocr",
                            processor_version=current_versions["ocr"],
                        )
                    )
            else:
                required_slots = [
                    slot_for(
                        content_slots,
                        source_sha256=media_sha256,
                        processor_type="ocr",
                        processor_version=current_versions["ocr"],
                    )
                ]
            if all(
                item is not None and item["status"] == "succeeded"
                for item in required_slots
            ):
                continue
            if any(slot_is_terminal(item) or slot_is_active(item) for item in required_slots):
                continue
        selected.append(content_id)
        if len(selected) >= limit:
            break
    return selected


def run_media_download_queue(
    *,
    limit: int = MEDIA_QUEUE_BATCH_LIMIT,
    max_workers: int = MEDIA_DOWNLOAD_WORKERS,
    db_path: Path = DEFAULT_DB,
    published_start: Optional[str] = None,
    published_end: Optional[str] = None,
) -> Dict[str, Any]:
    if limit < 0:
        raise ValueError("media queue limit must be non-negative")
    stale_recovery = recover_stale_media_processing_slots(
        db_path=db_path,
        processor_types=("download",),
    )
    probed_content_ids = _queue_content_ids(
        stage="download", limit=limit + 1, db_path=db_path,
        published_start=published_start, published_end=published_end,
    )
    truncated = len(probed_content_ids) > limit
    content_ids = probed_content_ids[:limit]

    def download(content_id: int) -> Dict[str, Any]:
        try:
            return process_content_media(content_id, download_only=True, db_path=db_path)
        except Exception as exc:
            state = get_media_source_state(content_id, db_path=db_path)
            slot = state.get("download_slot") if state is not None else None
            status = (
                "terminal_failed"
                if isinstance(slot, dict) and slot.get("status") == "terminal_failed"
                else "retryable_failed"
            )
            return {
                "content_id": content_id,
                "status": status,
                "error": f"{type(exc).__name__}: {exc}"[:500],
            }

    workers = max(1, min(max_workers, len(content_ids))) if content_ids else 1
    with ThreadPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(download, content_ids))
    retryable_failed = sum(item["status"] == "retryable_failed" for item in results)
    terminal_failed = sum(item["status"] == "terminal_failed" for item in results)
    return {
        "candidates": len(content_ids),
        "downloaded": sum(item["status"] == "downloaded" for item in results),
        "retryable_failed": retryable_failed,
        "terminal_failed": terminal_failed,
        "failed": retryable_failed + terminal_failed,
        "truncated": truncated,
        "has_more": truncated,
        "stale_recovery": stale_recovery,
        "results": results,
    }


def run_media_processing_queue(
    *,
    limit: int = MEDIA_QUEUE_BATCH_LIMIT,
    db_path: Path = DEFAULT_DB,
    published_start: Optional[str] = None,
    published_end: Optional[str] = None,
) -> Dict[str, Any]:
    if limit < 0:
        raise ValueError("media queue limit must be non-negative")
    versions = processor_versions()
    stale_recovery = recover_stale_media_processing_slots(
        db_path=db_path,
        processor_types=("frames", "asr", "ocr"),
        processor_version_by_type={
            "frames": versions["frames"],
            "asr": versions["asr"],
            "ocr": versions["ocr"],
        },
    )
    probed_content_ids = _queue_content_ids(
        stage="process", limit=limit + 1, db_path=db_path,
        published_start=published_start, published_end=published_end,
    )
    truncated = len(probed_content_ids) > limit
    content_ids = probed_content_ids[:limit]
    if content_ids and not ocr_binary_path().is_file():
        compile_ocr_binary()
    results: List[Dict[str, Any]] = []
    for content_id in content_ids:
        try:
            results.append(process_content_media(content_id, db_path=db_path))
        except Exception as exc:
            with connect(db_path) as connection:
                row = connection.execute(
                    """
                    SELECT status,attempt_count FROM media_processing_slots
                    WHERE content_id=? AND (
                        (processor_type='frames' AND processor_version=?)
                        OR (processor_type='asr' AND processor_version=?)
                        OR (processor_type='ocr' AND processor_version=?)
                    )
                    ORDER BY updated_at DESC,id DESC LIMIT 1
                    """,
                    (content_id, versions["frames"], versions["asr"], versions["ocr"]),
                ).fetchone()
            status = (
                "terminal_failed"
                if isinstance(exc, TerminalMediaSlotError)
                or (
                    row is not None
                    and (
                        row["status"] == "terminal_failed"
                        or int(row["attempt_count"]) >= MAX_MEDIA_PROCESSING_ATTEMPTS
                    )
                )
                else "retryable_failed"
            )
            results.append(
                {
                    "content_id": content_id,
                    "status": status,
                    "error": f"{type(exc).__name__}: {exc}"[:500],
                }
            )
    retryable_failed = sum(item["status"] == "retryable_failed" for item in results)
    terminal_failed = sum(item["status"] == "terminal_failed" for item in results)
    return {
        "candidates": len(content_ids),
        "evidence_ready": sum(item["status"] == "evidence_ready" for item in results),
        "retryable_failed": retryable_failed,
        "terminal_failed": terminal_failed,
        "failed": retryable_failed + terminal_failed,
        "truncated": truncated,
        "has_more": truncated,
        "stale_recovery": stale_recovery,
        "results": results,
    }


def ingest_existing_video_evidence(
    content_id: int,
    *,
    media_path: Path,
    asr_path: Path,
    ocr_path: Path,
    db_path: Path = DEFAULT_DB,
) -> Dict[str, Artifact]:
    """Register complete legacy evidence while correcting the old OCR text aggregation bug."""

    if not all(path.is_file() for path in (media_path, asr_path, ocr_path)):
        raise MediaProcessingError("existing evidence set is incomplete")
    legacy_ocr = json.loads(ocr_path.read_text(encoding="utf-8"))
    texts: List[str] = []
    for item in legacy_ocr.get("observations", []):
        if not isinstance(item, dict):
            continue
        text = str(item.get("text") or "").strip()
        if text and text not in texts:
            texts.append(text)
    corrected = MEDIA_ROOT / f"legacy-{content_id}" / "ocr.json"
    _atomic_json(
        corrected,
        {
            **legacy_ocr,
            "combined_text": "\n".join(texts),
            "processor_version": "legacy-ocr-normalized-v8.0",
        },
    )
    with connect(db_path) as connection, transaction(connection):
        media = register_artifact(
            connection, content_id=content_id, artifact_type="media", path=media_path,
            processor_version="legacy-media-ingest-v8.0",
        )
        asr = register_artifact(
            connection, content_id=content_id, artifact_type="asr", path=asr_path,
            processor_version=processor_versions()["asr"],
        )
        ocr = register_artifact(
            connection, content_id=content_id, artifact_type="ocr", path=corrected,
            processor_version="legacy-ocr-normalized-v8.0",
        )
        connection.execute(
            """
            UPDATE review_queue SET status='resolved', resolved_at=?, updated_at=?
            WHERE content_id=? AND reason_code='stale_local_evidence'
            """,
            (now_utc(), now_utc(), content_id),
        )
    return {"media": media, "asr": asr, "ocr": ocr}


def media_artifact_coverage(
    content_ids: Iterable[int], *, db_path: Path = DEFAULT_DB
) -> Dict[int, set[str]]:
    values = list(content_ids)
    if not values:
        return {}
    placeholders = ",".join("?" for _ in values)
    with connect(db_path) as connection:
        rows = connection.execute(
            f"""
            SELECT content_id, artifact_type FROM evidence_artifacts
            WHERE status='available' AND content_id IN ({placeholders})
            """,
            values,
        ).fetchall()
    output: Dict[int, set[str]] = {content_id: set() for content_id in values}
    for row in rows:
        output[int(row["content_id"])].add(str(row["artifact_type"]))
    return output
