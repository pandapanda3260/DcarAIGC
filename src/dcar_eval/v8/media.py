"""Versioned local media processing shared by Douyin and Xiaohongshu."""

from __future__ import annotations

import ctypes
import errno
import hashlib
import json
import math
import os
import sqlite3
import stat
import subprocess
import sys
import tempfile
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
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional

from huggingface_hub import snapshot_download

from .storage import DEFAULT_DB, PROJECT_ROOT, connect, now_utc, transaction


CONFIG_PATH = PROJECT_ROOT / "config" / "media_processor_v8.json"
MEDIA_ROOT = PROJECT_ROOT / "data" / "cache" / "v8" / "media"
OCR_SOURCE = PROJECT_ROOT / "src" / "dcar_eval" / "vision_ocr.swift"
MEDIA_SOURCE_VERSION = "provider-media-source-v8.1"
VIDEO_DOWNLOAD_VERSION = "provider-media-download-v8.1"
IMAGE_DOWNLOAD_VERSION = "provider-image-download-v8.3"
IMAGE_MANIFEST_VERSION = "provider-image-manifest-v8.3"
LEGACY_VIDEO_DOWNLOAD_VERSION = "provider-media-download-v8.0"
LEGACY_IMAGE_DOWNLOAD_VERSION = "provider-image-download-v8.0"
MAX_MEDIA_DOWNLOAD_ATTEMPTS = 3
MAX_MEDIA_PROCESSING_ATTEMPTS = 3
MEDIA_DOWNLOAD_WORKERS = 6
MEDIA_QUEUE_BATCH_LIMIT = 500
STALE_MEDIA_SLOT_SECONDS = 2 * 60 * 60
BOUNDED_PROCESSOR_TYPES = frozenset({"download", "frames", "asr", "ocr"})
DEFAULT_MAX_MEDIA_DOWNLOAD_BYTES = 512 * 1024 * 1024
DEFAULT_MAX_VIDEO_DURATION_SECONDS = 30 * 60
_XHS_IMAGE_HOST_SUFFIXES = (".rednotecdn.com", ".xhscdn.com")
_XHS_IMAGE_STABLE_QUERY_KEYS = ("ap", "origin", "sign", "src", "t")
_XHS_IMAGE_PREVIEW_TRANSFORMS = frozenset(
    {
        "imageView2/2/w/576/format/webp/q/87|imageMogr2/strip",
        "redImage/frame/0",
    }
)
_XHS_IMAGE_DETAIL_TRANSFORMS = frozenset(
    {"imageView2/2/w/1440/format/webp"}
)


class MediaProcessingError(RuntimeError):
    pass


class TerminalMediaSlotError(MediaProcessingError):
    """The same evidence source exhausted its bounded processing attempts."""


class _ResponseTargetCollisionError(MediaProcessingError):
    """A response path was occupied before this request could create it."""


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


def _image_url_sha256(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def _safe_image_path(path: str) -> bool:
    if not path.startswith("/") or path == "/" or "\\" in path or "\x00" in path:
        return False
    try:
        decoded = urllib.parse.unquote(path)
    except (UnicodeDecodeError, ValueError):
        return False
    return all(part not in {"", ".", ".."} for part in decoded.split("/")[1:])


def _xhs_image_variant(url: str) -> Optional[Dict[str, Any]]:
    """Recognize only the frozen XHS 576-preview / 1440-detail URL pair."""

    try:
        parsed = urllib.parse.urlsplit(url)
        host = (parsed.hostname or "").lower()
        port = parsed.port
        pairs: List[tuple[str, str, bool]] = []
        for component in parsed.query.split("&"):
            if not component:
                return None
            raw_key, separator, raw_value = component.partition("=")
            pairs.append(
                (
                    urllib.parse.unquote_plus(raw_key),
                    urllib.parse.unquote_plus(raw_value),
                    bool(separator),
                )
            )
    except (UnicodeDecodeError, ValueError):
        return None
    if (
        parsed.scheme != "https"
        or not host.endswith(_XHS_IMAGE_HOST_SUFFIXES)
        or port not in {None, 443}
        or parsed.fragment
        or not _safe_image_path(parsed.path)
    ):
        return None
    values_by_key: Dict[str, List[str]] = {}
    transforms: List[str] = []
    for key, value, had_equals in pairs:
        if key in {*_XHS_IMAGE_STABLE_QUERY_KEYS, "sc"}:
            if not had_equals:
                return None
            values_by_key.setdefault(key, []).append(value)
        elif value == "" and not had_equals:
            transforms.append(key)
        else:
            return None
    if set(values_by_key) != {*_XHS_IMAGE_STABLE_QUERY_KEYS, "sc"} or any(
        len(values) != 1 for values in values_by_key.values()
    ):
        return None
    sc = values_by_key["sc"][0]
    if sc == "USR_PRV" and frozenset(transforms) == _XHS_IMAGE_PREVIEW_TRANSFORMS:
        if len(transforms) != len(_XHS_IMAGE_PREVIEW_TRANSFORMS):
            return None
        profile = "preview-576-webp-v1"
        priority = 1
    elif sc == "USR_DTL" and frozenset(transforms) == _XHS_IMAGE_DETAIL_TRANSFORMS:
        if len(transforms) != len(_XHS_IMAGE_DETAIL_TRANSFORMS):
            return None
        profile = "detail-1440-webp-v1"
        priority = 0
    else:
        return None
    stable_query = [
        [key, values_by_key[key][0]] for key in _XHS_IMAGE_STABLE_QUERY_KEYS
    ]
    return {
        "scheme": "https",
        "host": host,
        "port": 443,
        "path": parsed.path,
        "stable_query": stable_query,
        "profile": profile,
        "priority": priority,
    }


def image_source_groups(
    urls: Iterable[str], *, platform: str
) -> List[Dict[str, Any]]:
    """Derive conservative logical-image fallback groups from frozen source URLs.

    The source URL list and its source digest remain unchanged. Only the exact XHS
    preview/detail pattern observed in the frozen history cache is grouped; every
    other URL remains an independent singleton.
    """

    raw_values = list(urls)
    if any(type(value) is not str for value in raw_values):
        raise MediaProcessingError("image source group URLs must be exact strings")
    values, _source_sha256 = _media_source_identity("image", raw_values)
    if raw_values != values:
        raise MediaProcessingError(
            "image source group URLs contain duplicate or noncanonical values"
        )
    provisional: Dict[tuple[Any, ...], List[Dict[str, Any]]] = {}
    first_indexes: Dict[tuple[Any, ...], int] = {}
    for source_index, url in enumerate(values):
        variant = _xhs_image_variant(url) if platform == "xiaohongshu" else None
        if variant is None:
            key: tuple[Any, ...] = ("exact-url-v1", url)
            profile = "exact-url-v1"
            priority = 0
            identity: Dict[str, Any] = {
                "kind": "exact-url-v1",
                "url_sha256": _image_url_sha256(url),
            }
        else:
            key = (
                "xhs-preview-detail-v1",
                variant["scheme"],
                variant["host"],
                variant["port"],
                variant["path"],
                tuple(tuple(item) for item in variant["stable_query"]),
            )
            profile = str(variant["profile"])
            priority = int(variant["priority"])
            identity = {
                "kind": "xhs-preview-detail-v1",
                "platform": "xiaohongshu",
                "scheme": variant["scheme"],
                "host": variant["host"],
                "port": variant["port"],
                "path": variant["path"],
                "stable_query": variant["stable_query"],
            }
        provisional.setdefault(key, []).append(
            {
                "source_index": source_index,
                "profile": profile,
                "priority": priority,
                "url": url,
                "url_sha256": _image_url_sha256(url),
                "identity": identity,
            }
        )
        first_indexes.setdefault(key, source_index)

    normalized: List[tuple[int, Dict[str, Any], List[Dict[str, Any]]]] = []
    for key, candidates in provisional.items():
        profiles = [str(candidate["profile"]) for candidate in candidates]
        if key[0] == "xhs-preview-detail-v1" and sorted(profiles) == [
            "detail-1440-webp-v1",
            "preview-576-webp-v1",
        ]:
            ordered = sorted(
                candidates,
                key=lambda candidate: (
                    int(candidate["priority"]),
                    int(candidate["source_index"]),
                ),
            )
            normalized.append((first_indexes[key], dict(ordered[0]["identity"]), ordered))
            continue
        for candidate in candidates:
            normalized.append(
                (
                    int(candidate["source_index"]),
                    {
                        "kind": "exact-url-v1",
                        "url_sha256": candidate["url_sha256"],
                    },
                    [
                        {
                            **candidate,
                            "profile": "exact-url-v1",
                            "priority": 0,
                        }
                    ],
                )
            )

    groups: List[Dict[str, Any]] = []
    for group_index, (_first, identity, candidates) in enumerate(
        sorted(normalized, key=lambda item: item[0])
    ):
        groups.append(
            {
                "group_index": group_index,
                "identity": identity,
                "candidates": [
                    {
                        "source_index": int(candidate["source_index"]),
                        "profile": str(candidate["profile"]),
                        "url": str(candidate["url"]),
                        "url_sha256": str(candidate["url_sha256"]),
                    }
                    for candidate in candidates
                ],
            }
        )
    return groups


def validate_frozen_image_groups(
    urls: Iterable[str],
    groups: Iterable[Mapping[str, Any]],
    *,
    platform: str,
) -> List[Dict[str, Any]]:
    """Validate an externally frozen logical-image grouping without I/O."""

    raw_values = list(urls)
    if any(type(value) is not str for value in raw_values):
        raise MediaProcessingError("frozen image source URLs must be exact strings")
    values, _source_sha256 = _media_source_identity("image", raw_values)
    if raw_values != values:
        raise MediaProcessingError(
            "frozen image source URLs contain duplicate or invalid values"
        )
    if not isinstance(groups, (list, tuple)):
        raise MediaProcessingError("frozen image groups must be a sequence")
    normalized: List[Dict[str, Any]] = []
    seen_source_indexes: set[int] = set()
    next_source_index = 0
    group_index_blocks: List[tuple[List[int], List[int]]] = []
    for expected_group_index, raw_group in enumerate(groups):
        if not isinstance(raw_group, Mapping) or set(raw_group) != {
            "group_index",
            "identity",
            "candidates",
        }:
            raise MediaProcessingError("frozen image group shape is invalid")
        if (
            type(raw_group["group_index"]) is not int
            or int(raw_group["group_index"]) != expected_group_index
        ):
            raise MediaProcessingError("frozen image group index is not consecutive")
        identity = raw_group["identity"]
        if not isinstance(identity, Mapping) or not identity:
            raise MediaProcessingError("frozen image group identity is invalid")
        identity_kind = identity.get("kind")
        if type(identity_kind) is not str:
            raise MediaProcessingError(
                "frozen image group identity fields are invalid"
            )
        if identity_kind == "douyin-discovery-image-v1":
            if (
                set(identity) != {"kind", "platform", "image_index"}
                or type(identity.get("platform")) is not str
                or type(identity.get("image_index")) is not int
            ):
                raise MediaProcessingError(
                    "frozen image group identity fields are invalid"
                )
        elif identity_kind == "exact-url-v1":
            if (
                set(identity) != {"kind", "url_sha256"}
                or type(identity.get("url_sha256")) is not str
            ):
                raise MediaProcessingError(
                    "frozen image group identity fields are invalid"
                )
        elif identity_kind == "xhs-preview-detail-v1":
            stable_query = identity.get("stable_query")
            if (
                set(identity)
                != {
                    "kind",
                    "platform",
                    "scheme",
                    "host",
                    "port",
                    "path",
                    "stable_query",
                }
                or any(
                    type(identity.get(key)) is not str
                    for key in ("platform", "scheme", "host", "path")
                )
                or type(identity.get("port")) is not int
                or type(stable_query) is not list
                or any(
                    type(pair) is not list
                    or len(pair) != 2
                    or any(type(value) is not str for value in pair)
                    for pair in stable_query
                )
            ):
                raise MediaProcessingError(
                    "frozen image group identity fields are invalid"
                )
        else:
            raise MediaProcessingError("frozen image group identity is invalid")
        try:
            json.dumps(identity, ensure_ascii=False, sort_keys=True, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise MediaProcessingError(
                "frozen image group identity is not canonical JSON"
            ) from exc
        raw_candidates = raw_group["candidates"]
        if not isinstance(raw_candidates, list) or not raw_candidates:
            raise MediaProcessingError("frozen image group has no candidates")
        candidates: List[Dict[str, Any]] = []
        group_source_indexes: List[int] = []
        for raw_candidate in raw_candidates:
            if not isinstance(raw_candidate, Mapping) or set(raw_candidate) != {
                "source_index",
                "profile",
                "url",
                "url_sha256",
            }:
                raise MediaProcessingError(
                    "frozen image candidate shape is invalid"
                )
            source_index = raw_candidate["source_index"]
            if type(source_index) is not int or not (0 <= source_index < len(values)):
                raise MediaProcessingError(
                    "frozen image candidate source index is out of range"
                )
            if source_index in seen_source_indexes:
                raise MediaProcessingError(
                    "frozen image groups contain duplicate source index"
                )
            seen_source_indexes.add(source_index)
            group_source_indexes.append(source_index)
            url = raw_candidate["url"]
            if type(url) is not str or url != values[source_index]:
                raise MediaProcessingError(
                    "frozen image candidate URL does not match source index"
                )
            url_sha256 = raw_candidate["url_sha256"]
            if (
                type(url_sha256) is not str
                or url_sha256 != _image_url_sha256(url)
            ):
                raise MediaProcessingError(
                    "frozen image candidate URL SHA is invalid"
                )
            profile = raw_candidate["profile"]
            if type(profile) is not str or not profile:
                raise MediaProcessingError(
                    "frozen image candidate profile is invalid"
                )
            candidates.append(
                {
                    "source_index": source_index,
                    "profile": profile,
                    "url": url,
                    "url_sha256": url_sha256,
                }
            )
        expected_indexes = list(
            range(next_source_index, next_source_index + len(group_source_indexes))
        )
        group_index_blocks.append((group_source_indexes, expected_indexes))
        next_source_index += len(group_source_indexes)
        normalized.append(
            {
                "group_index": expected_group_index,
                "identity": dict(identity),
                "candidates": candidates,
            }
        )
    missing = sorted(set(range(len(values))) - seen_source_indexes)
    if missing:
        raise MediaProcessingError("frozen image groups have missing source index")
    if not normalized:
        raise MediaProcessingError("image download has no logical source groups")
    kinds = {group["identity"]["kind"] for group in normalized}
    if platform == "douyin" and kinds == {"douyin-discovery-image-v1"}:
        for group_index, (group, (actual, expected)) in enumerate(
            zip(normalized, group_index_blocks, strict=True)
        ):
            if group["identity"] != {
                "kind": "douyin-discovery-image-v1",
                "platform": "douyin",
                "image_index": group_index,
            }:
                raise MediaProcessingError(
                    "frozen Douyin image group identity is invalid"
                )
            if actual != expected:
                raise MediaProcessingError(
                    "frozen Douyin image candidate order is invalid"
                )
            if any(
                candidate["profile"] != "douyin-discovery-candidate-v1"
                for candidate in group["candidates"]
            ):
                raise MediaProcessingError(
                    "frozen Douyin image candidate profile is invalid"
                )
    elif platform in {"douyin", "xiaohongshu"}:
        if "douyin-discovery-image-v1" in kinds:
            raise MediaProcessingError(
                "frozen Douyin image groups do not match content platform"
            )
        expected_groups = image_source_groups(values, platform=platform)
        if normalized != expected_groups:
            raise MediaProcessingError(
                "frozen XHS/exact image identity, profile, or candidate order is invalid"
            )
    else:
        raise MediaProcessingError("frozen image groups require a supported platform")
    return normalized


def _canonical_json_bytes(value: Any) -> bytes:
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise MediaProcessingError(
            "image evidence is not canonical JSON"
        ) from exc


def image_groups_sha256(groups: Iterable[Mapping[str, Any]]) -> str:
    return hashlib.sha256(_canonical_json_bytes(list(groups))).hexdigest()


def image_download_binding_sha256(
    source_sha256: str, groups_sha256: str
) -> str:
    if any(
        type(value) is not str
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
        for value in (source_sha256, groups_sha256)
    ):
        raise MediaProcessingError("image download binding input SHA is invalid")
    payload = {
        "kind": "provider-image-download-binding-v8.3",
        "source_sha256": source_sha256,
        "image_groups_sha256": groups_sha256,
    }
    return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def douyin_image_source_groups(
    urls: Iterable[str], candidate_groups: Iterable[Iterable[str]]
) -> List[Dict[str, Any]]:
    """Bind Douyin discovery ``images[]`` candidate groups to frozen URLs."""

    raw_values = list(urls)
    if any(type(value) is not str for value in raw_values):
        raise MediaProcessingError("Douyin frozen image URLs must be exact strings")
    values, _source_sha256 = _media_source_identity("image", raw_values)
    if raw_values != values:
        raise MediaProcessingError(
            "Douyin frozen image URLs contain duplicate or invalid values"
        )
    if not isinstance(candidate_groups, (list, tuple)):
        raise MediaProcessingError("Douyin discovery image groups must be a sequence")
    raw_groups: List[List[str]] = []
    for raw_group in candidate_groups:
        if not isinstance(raw_group, (list, tuple)) or not raw_group:
            raise MediaProcessingError(
                "Douyin discovery image groups contain an empty group"
            )
        group = list(raw_group)
        if any(type(value) is not str for value in group):
            raise MediaProcessingError(
                "Douyin discovery image candidates must be exact strings"
            )
        normalized_group, _group_sha256 = _media_source_identity("image", group)
        if group != normalized_group:
            raise MediaProcessingError(
                "Douyin discovery image group contains duplicate or invalid URLs"
            )
        raw_groups.append(group)
    flattened = [url for group in raw_groups for url in group]
    if flattened != values:
        raise MediaProcessingError(
            "Douyin discovery image groups do not exactly flatten to frozen URLs"
        )
    source_index = 0
    groups: List[Dict[str, Any]] = []
    for group_index, group in enumerate(raw_groups):
        candidates: List[Dict[str, Any]] = []
        for url in group:
            candidates.append(
                {
                    "source_index": source_index,
                    "profile": "douyin-discovery-candidate-v1",
                    "url": url,
                    "url_sha256": _image_url_sha256(url),
                }
            )
            source_index += 1
        groups.append(
            {
                "group_index": group_index,
                "identity": {
                    "kind": "douyin-discovery-image-v1",
                    "platform": "douyin",
                    "image_index": group_index,
                },
                "candidates": candidates,
            }
        )
    return validate_frozen_image_groups(values, groups, platform="douyin")


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
        return {IMAGE_DOWNLOAD_VERSION}
    raise MediaProcessingError(f"unsupported media kind: {media_kind}")


@dataclass(frozen=True)
class Artifact:
    id: int
    content_id: int
    artifact_type: str
    local_path: str
    sha256: str
    processor_version: str


@dataclass(frozen=True)
class _PrivateFileEvidence:
    path: Path
    device: int
    inode: int
    byte_size: int
    mtime_ns: int
    ctime_ns: int
    sha256: str
    header: bytes
    body: Optional[bytes] = None


@dataclass
class _SpooledResponse:
    handle: Any
    byte_size: int
    sha256: str
    header: bytes

    def close(self) -> None:
        self.handle.close()


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


def _read_private_file_evidence(
    path: Path, *, label: str, capture_body: bool = False
) -> _PrivateFileEvidence:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except (FileNotFoundError, OSError) as exc:
        raise MediaProcessingError(f"{label} cannot be opened safely") from exc
    chunks: List[bytes] = []
    digest = hashlib.sha256()
    header = b""
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_size <= 0
        ):
            raise MediaProcessingError(f"{label} must be a private regular file")
        while True:
            block = os.read(descriptor, 1024 * 1024)
            if not block:
                break
            if len(header) < 16:
                header = (header + block)[:16]
            digest.update(block)
            if capture_body:
                chunks.append(block)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    identity_before = (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
        before.st_nlink,
    )
    identity_after = (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
        after.st_nlink,
    )
    try:
        path_metadata = path.lstat()
    except FileNotFoundError as exc:
        raise MediaProcessingError(f"{label} path changed during read") from exc
    identity_path = (
        path_metadata.st_dev,
        path_metadata.st_ino,
        path_metadata.st_size,
        path_metadata.st_mtime_ns,
        path_metadata.st_ctime_ns,
        path_metadata.st_nlink,
    )
    if identity_before != identity_after or identity_after != identity_path:
        raise MediaProcessingError(f"{label} path changed during read")
    return _PrivateFileEvidence(
        path=path,
        device=after.st_dev,
        inode=after.st_ino,
        byte_size=after.st_size,
        mtime_ns=after.st_mtime_ns,
        ctime_ns=after.st_ctime_ns,
        sha256=digest.hexdigest(),
        header=header,
        body=b"".join(chunks) if capture_body else None,
    )


def _read_private_file_evidence_at(
    parent_descriptor: int,
    name: str,
    *,
    path: Path,
    label: str,
    capture_body: bool = False,
) -> _PrivateFileEvidence:
    """Read one private file without resolving its lexical ancestors again."""

    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(name, flags, dir_fd=parent_descriptor)
    except (FileNotFoundError, OSError) as exc:
        raise MediaProcessingError(f"{label} cannot be opened safely") from exc
    chunks: List[bytes] = []
    digest = hashlib.sha256()
    header = b""
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_size <= 0
        ):
            raise MediaProcessingError(f"{label} must be a private regular file")
        while True:
            block = os.read(descriptor, 1024 * 1024)
            if not block:
                break
            if len(header) < 16:
                header = (header + block)[:16]
            digest.update(block)
            if capture_body:
                chunks.append(block)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    path_metadata = os.stat(
        name, dir_fd=parent_descriptor, follow_symlinks=False
    )
    identity_before = (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
        before.st_nlink,
    )
    identity_after = (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
        after.st_nlink,
    )
    identity_path = (
        path_metadata.st_dev,
        path_metadata.st_ino,
        path_metadata.st_size,
        path_metadata.st_mtime_ns,
        path_metadata.st_ctime_ns,
        path_metadata.st_nlink,
    )
    if identity_before != identity_after or identity_after != identity_path:
        raise MediaProcessingError(f"{label} path changed during read")
    return _PrivateFileEvidence(
        path=path,
        device=after.st_dev,
        inode=after.st_ino,
        byte_size=after.st_size,
        mtime_ns=after.st_mtime_ns,
        ctime_ns=after.st_ctime_ns,
        sha256=digest.hexdigest(),
        header=header,
        body=b"".join(chunks) if capture_body else None,
    )


def _assert_private_file_evidence_current(
    expected: _PrivateFileEvidence, *, label: str
) -> _PrivateFileEvidence:
    current = _read_private_file_evidence(expected.path, label=label)
    if (
        current.device,
        current.inode,
        current.byte_size,
        current.mtime_ns,
        current.ctime_ns,
        current.sha256,
    ) != (
        expected.device,
        expected.inode,
        expected.byte_size,
        expected.mtime_ns,
        expected.ctime_ns,
        expected.sha256,
    ):
        raise MediaProcessingError(f"{label} evidence changed after validation")
    return current


def _relative(path: Path) -> str:
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(resolved)


def _resolved(local_path: str) -> Path:
    path = Path(local_path)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _validated_link_id(value: Any) -> str:
    if (
        type(value) is not str
        or len(value) != 6
        or any(
            character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
            for character in value
        )
    ):
        raise MediaProcessingError("content link_id is not a safe six-character basename")
    return value


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
    captured_value = captured_at or created_at
    metadata_value = json.dumps(
        metadata or {}, ensure_ascii=False, sort_keys=True
    )
    if not connection.in_transaction:
        connection.execute("BEGIN IMMEDIATE")
    row = connection.execute(
        """
        SELECT id FROM evidence_artifacts
        WHERE content_id=? AND artifact_type=? AND local_path=?
        """,
        (content_id, artifact_type, local_path),
    ).fetchone()
    if row is not None:
        artifact_id = int(row["id"])
        connection.execute(
            """
            UPDATE evidence_artifacts
            SET status='available', byte_size=?, sha256=?, captured_at=?,
                processor_version=?, metadata_json=?
            WHERE id=?
            """,
            (
                path.stat().st_size,
                sha256,
                captured_value,
                processor_version,
                metadata_value,
                artifact_id,
            ),
        )
    else:
        cursor = connection.execute(
            """
            INSERT INTO evidence_artifacts(
                content_id, artifact_type, local_path, status, byte_size, sha256,
                captured_at, processor_version, metadata_json, created_at
            ) VALUES (?, ?, ?, 'available', ?, ?, ?, ?, ?, ?)
            """,
            (
                content_id,
                artifact_type,
                local_path,
                path.stat().st_size,
                sha256,
                captured_value,
                processor_version,
                metadata_value,
                created_at,
            ),
        )
        if cursor.lastrowid is None:
            raise RuntimeError("artifact insert returned no row")
        artifact_id = int(cursor.lastrowid)
    return Artifact(
        id=artifact_id,
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
    claim_validator: Optional[Callable[[sqlite3.Connection], None]] = None,
    commit_validator: Optional[Callable[[sqlite3.Connection], None]] = None,
    expected_output_path: Optional[Path] = None,
    expected_output_root: Optional[Path] = None,
    cached_validator: Optional[
        Callable[[sqlite3.Connection, Artifact, _PrivateFileEvidence], None]
    ] = None,
    preclaimed_slot_id: Optional[int] = None,
) -> Artifact:
    _before_processing_slot_claim(content_id, processor_type)
    with connect(db_path) as connection, transaction(connection):
        if claim_validator is not None:
            claim_validator(connection)
        if preclaimed_slot_id is None:
            slot_id, cached = _claim_processing_slot(
                connection,
                content_id=content_id,
                source_sha256=source_sha256,
                processor_type=processor_type,
                processor_version=processor_version,
                source_aliases=source_aliases,
            )
        else:
            slot_id = preclaimed_slot_id
            cached = None
        claimed_row = connection.execute(
            """
            SELECT id,content_id,source_sha256,processor_type,processor_version,
                   status,attempt_count,updated_at,output_artifact_id
            FROM media_processing_slots WHERE id=?
            """,
            (slot_id,),
        ).fetchone()
        if claimed_row is None:
            raise MediaProcessingError("claimed media slot disappeared")
        if preclaimed_slot_id is not None and (
            claimed_row["content_id"] != content_id
            or claimed_row["source_sha256"] != source_sha256
            or claimed_row["processor_type"] != processor_type
            or claimed_row["processor_version"] != processor_version
            or claimed_row["status"] != "running"
            or claimed_row["output_artifact_id"] is not None
        ):
            raise MediaProcessingError("preclaimed media slot identity drifted")
        if cached is not None:
            artifact_row = connection.execute(
                "SELECT * FROM evidence_artifacts WHERE id=?", (cached.id,)
            ).fetchone()
            legacy_video_cache = (
                processor_type == "download"
                and artifact_type == "media"
                and claimed_row["processor_version"]
                == LEGACY_VIDEO_DOWNLOAD_VERSION
            )
            expected_metadata_json = (
                artifact_row["metadata_json"]
                if legacy_video_cache and artifact_row is not None
                else json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True)
            )
            if expected_output_path is not None and not legacy_video_cache:
                if expected_output_root is not None:
                    _require_no_symlink_below_root(
                        expected_output_path,
                        root=expected_output_root,
                        label=f"cached {artifact_type} output",
                    )
                expected_local_path = _relative(expected_output_path)
            else:
                expected_local_path = cached.local_path
            if (
                str(claimed_row["status"] or "") != "succeeded"
                or claimed_row["output_artifact_id"] != cached.id
                or artifact_row is None
                or type(artifact_row["content_id"]) is not int
                or artifact_row["content_id"] != content_id
                or type(artifact_row["artifact_type"]) is not str
                or artifact_row["artifact_type"] != artifact_type
                or type(artifact_row["local_path"]) is not str
                or artifact_row["local_path"] != expected_local_path
                or artifact_row["local_path"] != cached.local_path
                or artifact_row["status"] != "available"
                or type(artifact_row["byte_size"]) is not int
                or artifact_row["byte_size"] <= 0
                or not _valid_sha256(artifact_row["sha256"])
                or artifact_row["sha256"] != cached.sha256
                or type(artifact_row["processor_version"]) is not str
                or artifact_row["processor_version"]
                != claimed_row["processor_version"]
                or artifact_row["processor_version"] != cached.processor_version
                or type(artifact_row["metadata_json"]) is not str
                or artifact_row["metadata_json"] != expected_metadata_json
            ):
                raise MediaProcessingError(
                    f"cached {processor_type} slot artifact evidence drifted"
                )
            try:
                cached_metadata = json.loads(artifact_row["metadata_json"])
            except json.JSONDecodeError as exc:
                raise MediaProcessingError(
                    f"cached {processor_type} slot metadata is invalid"
                ) from exc
            if not isinstance(cached_metadata, dict):
                raise MediaProcessingError(
                    f"cached {processor_type} slot metadata is invalid"
                )
            cached_path = _resolved(artifact_row["local_path"])
            cached_evidence = _read_private_file_evidence(
                cached_path,
                label=f"cached {artifact_type} output",
                capture_body=artifact_type
                in {"frames_manifest", "asr", "ocr", "media_manifest"},
            )
            if (
                cached_evidence.byte_size != artifact_row["byte_size"]
                or cached_evidence.sha256 != artifact_row["sha256"]
            ):
                raise MediaProcessingError(
                    f"cached {processor_type} slot file evidence drifted"
                )
            if cached_evidence.body is not None:
                try:
                    cached_body = json.loads(cached_evidence.body)
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise MediaProcessingError(
                        f"cached {artifact_type} output is not valid JSON"
                    ) from exc
                if not isinstance(cached_body, dict):
                    raise MediaProcessingError(
                        f"cached {artifact_type} output is not a JSON object"
                    )
            if cached_validator is not None:
                cached_validator(connection, cached, cached_evidence)
    if cached is not None:
        return cached
    claimed_identity = {
        "id": int(claimed_row["id"]),
        "content_id": int(claimed_row["content_id"]),
        "source_sha256": str(claimed_row["source_sha256"]),
        "processor_type": str(claimed_row["processor_type"]),
        "processor_version": str(claimed_row["processor_version"]),
        "status": str(claimed_row["status"]),
        "attempt_count": int(claimed_row["attempt_count"]),
        "updated_at": str(claimed_row["updated_at"]),
        "output_artifact_id": claimed_row["output_artifact_id"],
    }
    if (
        claimed_identity["content_id"] != content_id
        or claimed_identity["source_sha256"] != source_sha256
        or claimed_identity["processor_type"] != processor_type
        or claimed_identity["processor_version"] != processor_version
        or claimed_identity["status"] != "running"
        or claimed_identity["attempt_count"] <= 0
        or claimed_identity["output_artifact_id"] is not None
    ):
        raise MediaProcessingError("claimed media slot identity is invalid")
    try:
        output_path = produce()
        _before_processing_slot_commit(content_id, processor_type)
        with connect(db_path) as connection, transaction(connection):
            if commit_validator is not None:
                commit_validator(connection)
            artifact = register_artifact(
                connection,
                content_id=content_id,
                artifact_type=artifact_type,
                path=output_path,
                processor_version=processor_version,
                metadata=metadata,
            )
            cursor = connection.execute(
                """
                UPDATE media_processing_slots SET
                    status='succeeded', output_artifact_id=?, updated_at=?
                WHERE id=? AND content_id=? AND source_sha256=?
                  AND processor_type=? AND processor_version=?
                  AND status='running' AND attempt_count=? AND updated_at=?
                  AND output_artifact_id IS NULL
                """,
                (
                    artifact.id,
                    now_utc(),
                    claimed_identity["id"],
                    claimed_identity["content_id"],
                    claimed_identity["source_sha256"],
                    claimed_identity["processor_type"],
                    claimed_identity["processor_version"],
                    claimed_identity["attempt_count"],
                    claimed_identity["updated_at"],
                ),
            )
            if cursor.rowcount != 1:
                raise MediaProcessingError(
                    "claimed media slot identity changed before success commit"
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
                    error_message=?, updated_at=?
                WHERE id=? AND content_id=? AND source_sha256=?
                  AND processor_type=? AND processor_version=?
                  AND status='running' AND attempt_count=? AND updated_at=?
                  AND output_artifact_id IS NULL
                """,
                (
                    MAX_MEDIA_DOWNLOAD_ATTEMPTS,
                    f"{type(exc).__name__}: {exc}"[:500],
                    now_utc(),
                    claimed_identity["id"],
                    claimed_identity["content_id"],
                    claimed_identity["source_sha256"],
                    claimed_identity["processor_type"],
                    claimed_identity["processor_version"],
                    claimed_identity["attempt_count"],
                    claimed_identity["updated_at"],
                ),
            )
        raise


def _before_processing_slot_claim(
    _content_id: int, _processor_type: str
) -> None:
    """Test seam immediately before a processing-slot claim transaction."""


def _before_processing_slot_commit(
    _content_id: int, _processor_type: str
) -> None:
    """Test seam immediately before a processing-slot success transaction."""


def _probe_duration(path: Path, *, inherited_descriptor: Optional[int] = None) -> float:
    pass_fds = () if inherited_descriptor is None else (inherited_descriptor,)
    completed = subprocess.run(
        [
            "ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "default=nw=1:nk=1", str(path),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
        pass_fds=pass_fds,
    )
    try:
        return max(0.0, float(completed.stdout.strip()))
    except ValueError:
        return 0.0


def _has_video_stream(
    path: Path, *, inherited_descriptor: Optional[int] = None
) -> bool:
    pass_fds = () if inherited_descriptor is None else (inherited_descriptor,)
    completed = subprocess.run(
        [
            "ffprobe", "-v", "error", "-select_streams", "v:0",
            "-show_entries", "stream=codec_type", "-of", "default=nw=1:nk=1",
            str(path),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
        pass_fds=pass_fds,
    )
    return completed.returncode == 0 and "video" in {
        line.strip().lower() for line in completed.stdout.splitlines()
    }


def _valid_media(
    path: Path,
    *,
    maximum_duration_seconds: Optional[float] = None,
    inherited_descriptor: Optional[int] = None,
) -> bool:
    if inherited_descriptor is None:
        if not path.is_file() or path.stat().st_size <= 1024:
            return False
    else:
        metadata = os.fstat(inherited_descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size <= 1024:
            return False
    duration = _probe_duration(
        path, inherited_descriptor=inherited_descriptor
    )
    if duration <= 0:
        return False
    return bool(
        (
            maximum_duration_seconds is None
            or duration <= maximum_duration_seconds
        )
        and _has_video_stream(
            path, inherited_descriptor=inherited_descriptor
        )
    )


def _write_bounded_response(
    response: Any,
    *,
    maximum_bytes: Optional[int],
) -> _SpooledResponse:
    raw_length = response.headers.get("Content-Length") if response.headers else None
    if maximum_bytes is not None and raw_length:
        try:
            declared_length = int(raw_length)
        except (TypeError, ValueError):
            declared_length = 0
        if declared_length > maximum_bytes:
            raise MediaProcessingError(
                f"media response exceeds byte limit: {declared_length}>{maximum_bytes}"
            )
    handle = tempfile.TemporaryFile(mode="w+b")
    descriptor = handle.fileno()
    total = 0
    digest = hashlib.sha256()
    header = b""
    failure: Optional[BaseException] = None
    try:
        try:
            while True:
                block = response.read(1024 * 1024)
                if not block:
                    break
                total += len(block)
                if maximum_bytes is not None and total > maximum_bytes:
                    raise MediaProcessingError(
                        f"media response exceeds byte limit: {total}>{maximum_bytes}"
                    )
                if len(header) < 16:
                    header = (header + block)[:16]
                digest.update(block)
                view = memoryview(block)
                while view:
                    written = os.write(descriptor, view)
                    if written <= 0:
                        raise MediaProcessingError(
                            "media response write failed"
                        )
                    view = view[written:]
        except BaseException as exc:
            failure = exc
        os.fsync(descriptor)
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 0
            or metadata.st_size != total
        ):
            raise MediaProcessingError(
                "media response target is not a private regular file"
            )
    except BaseException:
        handle.close()
        raise
    evidence = _SpooledResponse(
        handle=handle,
        byte_size=metadata.st_size,
        sha256=digest.hexdigest(),
        header=header,
    )
    if failure is not None:
        try:
            setattr(failure, "_media_response_spool", evidence)
        except (AttributeError, TypeError):
            evidence.close()
        raise failure
    return evidence


def _after_bounded_response_write(_target: Path) -> None:
    """Test seam after response fd evidence freezes and before caller use."""


def _after_private_staging_chunk(_staging: Path, _byte_size: int) -> None:
    """Test seam after a private staging chunk is durably addressable."""


def _stage_spooled_response(
    response: _SpooledResponse,
    staging: Path,
    *,
    parent_descriptor: int,
    trusted_root: Path,
    label: str,
) -> _PrivateFileEvidence:
    """Copy an anonymous spool to a fixed, recovery-owned staging name."""

    _assert_bound_output_parent(
        parent_descriptor,
        staging.parent,
        root=trusted_root,
        label=f"{label} staging",
    )
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(
            staging.name, flags, 0o600, dir_fd=parent_descriptor
        )
    except FileExistsError as exc:
        raise _ResponseTargetCollisionError(
            f"{label} staging path already exists"
        ) from exc
    except OSError as exc:
        if os.path.lexists(staging):
            raise _ResponseTargetCollisionError(
                f"{label} staging path is occupied or aliased"
            ) from exc
        raise
    digest = hashlib.sha256()
    total = 0
    metadata: Optional[os.stat_result] = None
    try:
        response.handle.seek(0)
        while True:
            block = response.handle.read(1024 * 1024)
            if not block:
                break
            digest.update(block)
            total += len(block)
            view = memoryview(block)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise MediaProcessingError(f"{label} staging write failed")
                view = view[written:]
            _after_private_staging_chunk(staging, total)
        os.fsync(descriptor)
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_size != total
        ):
            raise MediaProcessingError(
                f"{label} staging inode is not a private regular file"
            )
    finally:
        os.close(descriptor)
    if total != response.byte_size or digest.hexdigest() != response.sha256:
        raise MediaProcessingError(f"{label} spool changed before staging")
    _assert_bound_output_parent(
        parent_descriptor,
        staging.parent,
        root=trusted_root,
        label=f"{label} staging",
    )
    staged = _read_private_file_evidence_at(
        parent_descriptor,
        staging.name,
        path=staging,
        label=f"{label} staging",
    )
    if (
        metadata is None
        or staged.device != metadata.st_dev
        or staged.inode != metadata.st_ino
        or staged.byte_size != response.byte_size
        or staged.sha256 != response.sha256
        or staged.header != response.header
    ):
        raise MediaProcessingError(f"{label} staging path changed after write")
    return staged


def _rename_exclusive_at(
    parent_descriptor: int,
    source_name: str,
    target_name: str,
) -> None:
    """Atomically rename within one directory without replacing the target."""

    library = ctypes.CDLL(None, use_errno=True)
    source = os.fsencode(source_name)
    target = os.fsencode(target_name)
    if sys.platform == "darwin":
        rename = library.renameatx_np
        rename.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        rename.restype = ctypes.c_int
        result = rename(
            parent_descriptor,
            source,
            parent_descriptor,
            target,
            0x00000004,  # RENAME_EXCL from Darwin sys/stdio.h.
        )
    elif sys.platform.startswith("linux"):
        rename = library.renameat2
        rename.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        rename.restype = ctypes.c_int
        result = rename(
            parent_descriptor,
            source,
            parent_descriptor,
            target,
            0x00000001,  # RENAME_NOREPLACE from Linux fs.h.
        )
    else:
        raise MediaProcessingError(
            "atomic no-clobber media publish is unsupported on this platform"
        )
    if result == 0:
        return
    error_number = ctypes.get_errno()
    if error_number in {errno.EEXIST, errno.ENOTEMPTY}:
        raise _ResponseTargetCollisionError(
            "media publish target already exists"
        )
    raise OSError(error_number, os.strerror(error_number), target_name)


def _publish_private_staging(
    staging: _PrivateFileEvidence,
    target: Path,
    *,
    parent_descriptor: int,
    trusted_root: Path,
    label: str,
) -> _PrivateFileEvidence:
    """Move a complete fixed staging inode to its final name in one step."""

    if staging.path.parent != target.parent:
        raise MediaProcessingError(f"{label} staging directory drifted")
    _assert_bound_output_parent(
        parent_descriptor,
        target.parent,
        root=trusted_root,
        label=label,
    )
    read_flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        read_flags |= os.O_NOFOLLOW
    staging_descriptor: Optional[int] = None
    try:
        try:
            staging_descriptor = os.open(
                staging.path.name, read_flags, dir_fd=parent_descriptor
            )
        except OSError as exc:
            raise MediaProcessingError(
                f"{label} staging cannot be opened safely"
            ) from exc
        before = os.fstat(staging_descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_mtime_ns,
                before.st_ctime_ns,
            )
            != (
                staging.device,
                staging.inode,
                staging.byte_size,
                staging.mtime_ns,
                staging.ctime_ns,
            )
        ):
            raise MediaProcessingError(f"{label} staging identity drifted")
        _rename_exclusive_at(
            parent_descriptor, staging.path.name, target.name
        )
        os.fsync(parent_descriptor)
        os.lseek(staging_descriptor, 0, os.SEEK_SET)
        digest = hashlib.sha256()
        header = b""
        while True:
            block = os.read(staging_descriptor, 1024 * 1024)
            if not block:
                break
            if len(header) < 16:
                header = (header + block)[:16]
            digest.update(block)
        after = os.fstat(staging_descriptor)
        target_metadata = os.stat(
            target.name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
        if (
            after.st_dev != staging.device
            or after.st_ino != staging.inode
            or after.st_size != staging.byte_size
            or after.st_nlink != 1
            or digest.hexdigest() != staging.sha256
            or header != staging.header
            or target_metadata.st_dev != after.st_dev
            or target_metadata.st_ino != after.st_ino
            or target_metadata.st_size != after.st_size
            or target_metadata.st_nlink != 1
        ):
            raise MediaProcessingError(f"{label} changed during atomic publish")
    finally:
        if staging_descriptor is not None:
            os.close(staging_descriptor)
    _assert_bound_output_parent(
        parent_descriptor,
        target.parent,
        root=trusted_root,
        label=label,
    )
    published = _read_private_file_evidence_at(
        parent_descriptor,
        target.name,
        path=target,
        label=label,
    )
    if (
        published.device != staging.device
        or published.inode != staging.inode
        or published.byte_size != staging.byte_size
        or published.sha256 != staging.sha256
        or published.header != staging.header
    ):
        raise MediaProcessingError(f"{label} changed after atomic publish")
    return published


def _publish_spooled_response(
    response: _SpooledResponse,
    target: Path,
    *,
    staging: Path,
    trusted_root: Path,
    label: str,
) -> _PrivateFileEvidence:
    """Publish through a complete allowlisted staging inode and atomic rename."""

    parent_descriptor = _open_private_output_parent(
        target, root=trusted_root, label=label
    )
    try:
        staged = _stage_spooled_response(
            response,
            staging,
            parent_descriptor=parent_descriptor,
            trusted_root=trusted_root,
            label=label,
        )
        return _publish_private_staging(
            staged,
            target,
            parent_descriptor=parent_descriptor,
            trusted_root=trusted_root,
            label=label,
        )
    finally:
        os.close(parent_descriptor)


def _private_json_spool(value: Mapping[str, Any]) -> _SpooledResponse:
    encoded = (
        json.dumps(
            dict(value),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    handle = tempfile.TemporaryFile(mode="w+b")
    descriptor = handle.fileno()
    try:
        view = memoryview(encoded)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise MediaProcessingError("private JSON spool write failed")
            view = view[written:]
        os.fsync(descriptor)
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 0
            or metadata.st_size != len(encoded)
        ):
            raise MediaProcessingError("private JSON spool is invalid")
    except BaseException:
        handle.close()
        raise
    return _SpooledResponse(
        handle=handle,
        byte_size=len(encoded),
        sha256=hashlib.sha256(encoded).hexdigest(),
        header=encoded[:16],
    )


def _before_image_manifest_publish(_manifest: Path) -> None:
    """Test seam before the image manifest's exclusive final publish."""


def _publish_private_image_manifest(
    manifest: Path,
    body: Mapping[str, Any],
    *,
    trusted_root: Path,
) -> None:
    _before_image_manifest_publish(manifest)
    staging = manifest.with_name(f".{manifest.name}.tmp")
    spool = _private_json_spool(body)
    try:
        _publish_spooled_response(
            spool,
            manifest,
            staging=staging,
            trusted_root=trusted_root,
            label="published image manifest",
        )
    finally:
        spool.close()


def _download_video(
    urls: Iterable[str],
    target: Path,
    *,
    urlopen_fn: Optional[Callable[..., Any]] = None,
    maximum_bytes: Optional[int] = None,
    require_exact_response_url: bool = False,
    reuse_existing: bool = True,
    maximum_duration_seconds: Optional[float] = None,
    trusted_root: Optional[Path] = None,
) -> Path:
    if not reuse_existing and os.path.lexists(target):
        raise MediaProcessingError("video response target already exists")
    if _valid_media(target, maximum_duration_seconds=maximum_duration_seconds):
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    errors: List[str] = []
    for index, url in enumerate(dict.fromkeys(urls)):
        candidate = target.with_name(f".{target.name}.candidate-{index}")
        if os.path.lexists(candidate):
            raise MediaProcessingError(
                "video response candidate path is occupied or aliased"
            )
        request = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "video/*,*/*;q=0.8"},
        )
        spool: Optional[_SpooledResponse] = None
        failure_spool: Optional[_SpooledResponse] = None
        try:
            opener = urlopen_fn or urllib.request.urlopen
            with opener(request, timeout=90) as response:
                final_url = str(response.geturl() or url)
                if require_exact_response_url and final_url != url:
                    raise MediaProcessingError(
                        f"media response URL changed: {url!r} -> {final_url!r}"
                    )
                spool = _write_bounded_response(
                    response,
                    maximum_bytes=maximum_bytes,
                )
            _after_bounded_response_write(candidate)
            if os.path.lexists(candidate):
                raise _ResponseTargetCollisionError(
                    "video response candidate path was occupied after spool"
                )
            descriptor = spool.handle.fileno()
            descriptor_path = Path(f"/dev/fd/{descriptor}")
            if _valid_media(
                descriptor_path,
                maximum_duration_seconds=maximum_duration_seconds,
                inherited_descriptor=descriptor,
            ):
                if os.path.lexists(target):
                    raise _ResponseTargetCollisionError(
                        "video response target changed during download"
                    )
                _publish_spooled_response(
                    spool,
                    target,
                    staging=candidate,
                    trusted_root=(
                        target.parent if trusted_root is None else trusted_root
                    ),
                    label="published video response",
                )
                return target
            errors.append(f"candidate {index} was not a playable video")
        except _ResponseTargetCollisionError as exc:
            raise MediaProcessingError(
                "video response path is occupied or aliased"
            ) from exc
        except (
            urllib.error.URLError,
            TimeoutError,
            ConnectionError,
            OSError,
            MediaProcessingError,
        ) as exc:
            partial_spool = getattr(exc, "_media_response_spool", None)
            if isinstance(partial_spool, _SpooledResponse):
                failure_spool = partial_spool
                _after_bounded_response_write(candidate)
            if os.path.lexists(candidate):
                raise MediaProcessingError(
                    "failed video response candidate path is occupied or aliased"
                ) from exc
            errors.append(f"candidate {index}: {type(exc).__name__}")
        finally:
            if spool is not None:
                spool.close()
            if failure_spool is not None and failure_spool is not spool:
                failure_spool.close()
    raise MediaProcessingError("media download failed: " + " | ".join(errors[-3:]))


def download_video_sources(
    content_id: int,
    urls: Iterable[str],
    *,
    db_path: Path = DEFAULT_DB,
    media_root: Optional[Path] = None,
    urlopen_fn: Optional[Callable[..., Any]] = None,
    maximum_bytes: Optional[int] = None,
    require_exact_response_url: bool = False,
    download_urls: Optional[Iterable[str]] = None,
    reuse_existing: bool = True,
    maximum_duration_seconds: Optional[float] = None,
    _slot_source_sha256: Optional[str] = None,
    _preclaimed_slot_id: Optional[int] = None,
) -> Artifact:
    values, source_sha256 = _media_source_identity("video", urls)
    if not values:
        raise MediaProcessingError("provider returned no supported video source")
    selected_values = (
        values
        if download_urls is None
        else _media_source_identity("video", download_urls)[0]
    )
    if not selected_values or any(value not in values for value in selected_values):
        raise MediaProcessingError("download video URLs are not an exact source subset")
    legacy_sha256 = _legacy_media_source_sha256(values)
    with connect(db_path) as connection:
        content = connection.execute(
            "SELECT link_id,platform,content_type FROM content_items WHERE id=?",
            (content_id,),
        ).fetchone()
        source_rows = connection.execute(
            """
            SELECT * FROM evidence_artifacts
            WHERE content_id=? AND artifact_type='media_source'
              AND status='available'
            ORDER BY id DESC
            """,
            (content_id,),
        ).fetchall()
    if content is None:
        raise MediaProcessingError(f"unknown content {content_id}")
    if type(content["platform"]) is not str or content["content_type"] != "video":
        raise MediaProcessingError("video download content identity drifted")
    source_artifact_id: Optional[int] = None
    if source_rows:
        source_urls, source_manifest_sha256 = _validated_video_media_source(
            source_rows[0]
        )
        if source_urls != values or source_manifest_sha256 != source_sha256:
            raise MediaProcessingError(
                "video download URLs do not match current media_source"
            )
        source_artifact_id = int(source_rows[0]["id"])
    effective_media_root = media_root if media_root is not None else MEDIA_ROOT
    link_id = _validated_link_id(content["link_id"])
    target = (
        effective_media_root
        / link_id
        / "downloads"
        / source_sha256
        / "source.mp4"
    )
    _require_no_symlink_below_root(
        target, root=effective_media_root, label="video download target"
    )

    def revalidate_context(connection: sqlite3.Connection) -> None:
        current_content = connection.execute(
            "SELECT link_id,platform,content_type FROM content_items WHERE id=?",
            (content_id,),
        ).fetchone()
        current_sources = connection.execute(
            """
            SELECT * FROM evidence_artifacts
            WHERE content_id=? AND artifact_type='media_source'
              AND status='available'
            ORDER BY id DESC
            """,
            (content_id,),
        ).fetchall()
        if (
            current_content is None
            or current_content["link_id"] != content["link_id"]
            or current_content["platform"] != content["platform"]
            or current_content["content_type"] != "video"
            or (
                source_artifact_id is None
                and bool(current_sources)
            )
            or (
                source_artifact_id is not None
                and (
                    not current_sources
                    or current_sources[0]["id"] != source_artifact_id
                )
            )
        ):
            raise MediaProcessingError(
                "video download content/source identity changed"
            )
        if source_artifact_id is not None:
            current_urls, current_sha256 = _validated_video_media_source(
                current_sources[0]
            )
            if current_urls != values or current_sha256 != source_sha256:
                raise MediaProcessingError(
                    "video download current media_source changed"
                )

    def produce() -> Path:
        if (
            urlopen_fn is None
            and maximum_bytes is None
            and not require_exact_response_url
            and download_urls is None
            and reuse_existing
            and maximum_duration_seconds is None
        ):
            return _download_video(values, target)
        return _download_video(
            selected_values,
            target,
            urlopen_fn=urlopen_fn,
            maximum_bytes=maximum_bytes,
            require_exact_response_url=require_exact_response_url,
            reuse_existing=reuse_existing,
            maximum_duration_seconds=maximum_duration_seconds,
        )

    effective_slot_source_sha256 = (
        _slot_source_sha256
        or (
            str(source_rows[0]["sha256"])
            if source_rows
            and source_rows[0]["processor_version"] == MEDIA_SOURCE_VERSION
            and _valid_sha256(source_rows[0]["sha256"])
            else source_sha256
        )
    )
    return _run_processing_slot(
        db_path=db_path,
        content_id=content_id,
        source_sha256=effective_slot_source_sha256,
        processor_type="download",
        processor_version=VIDEO_DOWNLOAD_VERSION,
        artifact_type="media",
        produce=produce,
        metadata={"source_count": len(values), "source_sha256": source_sha256},
        source_aliases=(
            (legacy_sha256,)
            if effective_slot_source_sha256 == source_sha256
            else ()
        ),
        expected_output_path=target,
        expected_output_root=effective_media_root,
        claim_validator=revalidate_context,
        commit_validator=revalidate_context,
        preclaimed_slot_id=_preclaimed_slot_id,
    )


def _before_media_source_manifest_commit(_content_id: int) -> None:
    """Test seam after source bytes are staged and before the binding transaction."""


def _before_staged_media_source_quarantine(_target: Path) -> None:
    """Test seam after staged-file validation and before its atomic quarantine."""


def _before_media_source_directory_chain(_target: Path) -> None:
    """Test seam before no-follow creation of the media-source parent chain."""


def _before_media_source_final_create(_target: Path) -> None:
    """Test seam immediately before the source path's exclusive create."""


def _same_private_file_evidence(
    left: _PrivateFileEvidence, right: _PrivateFileEvidence
) -> bool:
    return (
        left.device,
        left.inode,
        left.byte_size,
        left.mtime_ns,
        left.sha256,
    ) == (
        right.device,
        right.inode,
        right.byte_size,
        right.mtime_ns,
        right.sha256,
    )


def _quarantine_unregistered_media_source(
    expected: _PrivateFileEvidence,
) -> None:
    """Fail safely without mutating a path after registration is rejected.

    The staged manifest itself is durable recovery evidence: a later invocation
    can validate and register it. Leaving it in place avoids every lexical
    rename/unlink race in the failure path.
    """

    _assert_private_file_evidence_current(
        expected, label="staged media source cleanup"
    )
    _before_staged_media_source_quarantine(expected.path)
    _assert_private_file_evidence_current(
        expected, label="staged media source cleanup"
    )


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _open_private_output_parent(
    target: Path,
    *,
    root: Path,
    label: str,
) -> int:
    """Open/create a target parent through a no-follow chain from its root."""

    lexical_root = Path(os.path.abspath(root))
    lexical_target_parent = Path(os.path.abspath(target.parent))
    try:
        relative_parent = lexical_target_parent.relative_to(lexical_root)
    except ValueError as exc:
        raise MediaProcessingError(
            f"{label} parent is outside its trusted root"
        ) from exc
    _require_no_symlink_below_root(
        target, root=lexical_root, label=label
    )
    if lexical_root == lexical_root.parent:
        raise MediaProcessingError(f"{label} root cannot be filesystem root")
    directory_flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        directory_flags |= os.O_DIRECTORY
    if hasattr(os, "O_NOFOLLOW"):
        directory_flags |= os.O_NOFOLLOW
    try:
        current_fd = os.open(lexical_root.parent, directory_flags)
    except OSError as exc:
        raise MediaProcessingError(
            f"{label} root parent cannot be opened safely"
        ) from exc
    components = (lexical_root.name, *relative_parent.parts)
    try:
        for component in components:
            if component in {"", ".", ".."}:
                raise MediaProcessingError(
                    f"{label} directory component is unsafe"
                )
            try:
                os.mkdir(component, mode=0o700, dir_fd=current_fd)
            except FileExistsError:
                pass
            try:
                child_fd = os.open(
                    component, directory_flags, dir_fd=current_fd
                )
            except OSError as exc:
                raise MediaProcessingError(
                    f"{label} directory chain contains an alias"
                ) from exc
            metadata = os.fstat(child_fd)
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or metadata.st_uid != os.getuid()
            ):
                os.close(child_fd)
                raise MediaProcessingError(
                    f"{label} directory is not privately owned"
                )
            os.close(current_fd)
            current_fd = child_fd
        return current_fd
    except BaseException:
        os.close(current_fd)
        raise


def _assert_bound_output_parent(
    parent_descriptor: int,
    parent: Path,
    *,
    root: Path,
    label: str,
) -> None:
    """Require the lexical parent to remain the held no-follow directory."""

    _require_no_symlink_below_root(parent, root=root, label=label)
    descriptor_metadata = os.fstat(parent_descriptor)
    try:
        path_metadata = parent.lstat()
    except FileNotFoundError as exc:
        raise MediaProcessingError(f"{label} parent disappeared") from exc
    if (
        not stat.S_ISDIR(descriptor_metadata.st_mode)
        or not stat.S_ISDIR(path_metadata.st_mode)
        or parent.is_symlink()
        or descriptor_metadata.st_uid != os.getuid()
        or path_metadata.st_uid != os.getuid()
        or descriptor_metadata.st_dev != path_metadata.st_dev
        or descriptor_metadata.st_ino != path_metadata.st_ino
    ):
        raise MediaProcessingError(f"{label} parent binding changed")


def _open_private_media_source_parent(
    target: Path, *, media_root: Path
) -> int:
    """Open/create ``root/link/sources`` through no-follow directory fds."""

    return _open_private_output_parent(
        target,
        root=media_root,
        label="media source target",
    )


def _stage_private_media_source_json(
    target: Path,
    payload: Mapping[str, Any],
    *,
    media_root: Path,
) -> _PrivateFileEvidence:
    """Stage and atomically publish a source manifest without clobbering."""

    _before_media_source_directory_chain(target)
    parent_fd = _open_private_media_source_parent(target, media_root=media_root)
    staging_path = target.with_name(f".{target.name}.tmp")
    try:
        if not stat.S_ISDIR(os.fstat(parent_fd).st_mode):
            raise MediaProcessingError("media source parent is not a directory")
        staged_evidence: _PrivateFileEvidence
        try:
            os.stat(
                staging_path.name,
                dir_fd=parent_fd,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            spool = _private_json_spool(payload)
            try:
                _stage_spooled_response(
                    spool,
                    staging_path,
                    parent_descriptor=parent_fd,
                    trusted_root=media_root,
                    label="media source manifest",
                )
            finally:
                spool.close()
            staged_evidence = _read_private_file_evidence_at(
                parent_fd,
                staging_path.name,
                path=staging_path,
                label="staged media source manifest",
                capture_body=True,
            )
        else:
            staged_evidence = _read_private_file_evidence_at(
                parent_fd,
                staging_path.name,
                path=staging_path,
                label="existing media source staging",
                capture_body=True,
            )
            try:
                staged_payload = json.loads(staged_evidence.body or b"")
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise MediaProcessingError(
                    "media source staging is incomplete or invalid"
                ) from exc
            if (
                not isinstance(staged_payload, dict)
                or set(staged_payload) != set(payload)
                or staged_payload.get("schema_version")
                != payload["schema_version"]
                or type(staged_payload.get("media_kind")) is not str
                or staged_payload.get("media_kind") != payload["media_kind"]
                or type(staged_payload.get("urls")) is not list
                or any(
                    type(value) is not str for value in staged_payload["urls"]
                )
                or staged_payload.get("urls") != payload["urls"]
                or type(staged_payload.get("source_sha256")) is not str
                or staged_payload.get("source_sha256")
                != payload["source_sha256"]
                or type(staged_payload.get("raw_response_id")) is not int
                or staged_payload.get("raw_response_id")
                != payload["raw_response_id"]
                or type(staged_payload.get("captured_at")) is not str
                or not staged_payload.get("captured_at")
            ):
                raise MediaProcessingError(
                    "media source staging does not match the requested source"
                )
        _before_media_source_final_create(target)
        published = _publish_private_staging(
            staged_evidence,
            target,
            parent_descriptor=parent_fd,
            trusted_root=media_root,
            label="published media source",
        )
        published_evidence = _read_private_file_evidence_at(
            parent_fd,
            target.name,
            path=target,
            label="published media source",
            capture_body=True,
        )
    finally:
        os.close(parent_fd)
    _require_no_symlink_below_root(
        target, root=media_root, label="published media source"
    )
    if (
        not _same_private_file_evidence(published_evidence, published)
        or published_evidence.body != staged_evidence.body
    ):
        raise MediaProcessingError("media source changed during atomic publish")
    return published_evidence


def store_media_source_manifest(
    content_id: int,
    *,
    media_kind: str,
    urls: Iterable[str],
    raw_response_id: int,
    db_path: Path = DEFAULT_DB,
    media_root: Optional[Path] = None,
) -> Optional[Artifact]:
    """Persist normalized provider media URLs for the later local-compute jobs."""

    if type(content_id) is not int or content_id <= 0:
        raise MediaProcessingError("content_id must be an exact positive integer")
    if type(media_kind) is not str or media_kind not in {"video", "image"}:
        raise MediaProcessingError("media_kind must be an exact supported string")
    if type(raw_response_id) is not int or raw_response_id <= 0:
        raise MediaProcessingError(
            "raw_response_id must be an exact positive integer"
        )
    if type(urls) is not list or any(type(value) is not str for value in urls):
        raise MediaProcessingError("media source URLs must be an exact string list")
    values, source_sha256 = _media_source_identity(media_kind, urls)
    if not values:
        return None
    with connect(db_path) as connection:
        content = connection.execute(
            "SELECT link_id,content_type FROM content_items WHERE id=?",
            (content_id,),
        ).fetchone()
    if content is None:
        raise MediaProcessingError(f"unknown content {content_id}")
    if type(content["content_type"]) is not str or content["content_type"] != media_kind:
        raise MediaProcessingError(
            "media source kind does not match content type"
        )
    effective_media_root = media_root if media_root is not None else MEDIA_ROOT
    link_id = _validated_link_id(content["link_id"])
    target = (
        effective_media_root
        / link_id
        / "sources"
        / f"source-{raw_response_id}-{source_sha256[:12]}.json"
    )
    _require_no_symlink_below_root(
        target, root=effective_media_root, label="media source target"
    )
    payload = {
        "schema_version": MEDIA_SOURCE_VERSION,
        "media_kind": media_kind,
        "urls": values,
        "source_sha256": source_sha256,
        "raw_response_id": raw_response_id,
        "captured_at": now_utc(),
    }
    created_target = False
    created_evidence: Optional[_PrivateFileEvidence] = None
    target_evidence: _PrivateFileEvidence
    if os.path.lexists(target):
        target_evidence = _read_private_file_evidence(
            target, label="existing media source", capture_body=True
        )
        try:
            existing = json.loads(target_evidence.body or b"")
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MediaProcessingError(
                f"media source manifest collision: {target}"
            ) from exc
        if (
            not isinstance(existing, dict)
            or set(existing) != set(payload)
            or existing.get("schema_version") != MEDIA_SOURCE_VERSION
            or type(existing.get("media_kind")) is not str
            or existing.get("media_kind") != media_kind
            or type(existing.get("urls")) is not list
            or any(type(value) is not str for value in existing["urls"])
            or existing.get("urls") != values
            or type(existing.get("source_sha256")) is not str
            or existing.get("source_sha256") != source_sha256
            or type(existing.get("raw_response_id")) is not int
            or existing.get("raw_response_id") != raw_response_id
            or type(existing.get("captured_at")) is not str
            or not existing.get("captured_at")
        ):
            raise MediaProcessingError(f"media source manifest collision: {target}")
    else:
        target_evidence = _stage_private_media_source_json(
            target, payload, media_root=effective_media_root
        )
        created_target = True
        created_evidence = target_evidence
    try:
        _before_media_source_manifest_commit(content_id)
        with connect(db_path) as connection, transaction(connection):
            current = connection.execute(
                "SELECT link_id,content_type FROM content_items WHERE id=?",
                (content_id,),
            ).fetchone()
            if (
                current is None
                or current["link_id"] != content["link_id"]
                or current["content_type"] != media_kind
            ):
                raise MediaProcessingError(
                    "media source content identity changed before registration"
                )
            _assert_private_file_evidence_current(
                target_evidence, label="media source before registration"
            )
            artifact = register_artifact(
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
            _assert_private_file_evidence_current(
                target_evidence, label="media source after registration"
            )
            artifact_row = connection.execute(
                "SELECT * FROM evidence_artifacts WHERE id=?", (artifact.id,)
            ).fetchone()
            expected_metadata_json = json.dumps(
                {
                    "media_kind": media_kind,
                    "source_count": len(values),
                    "source_sha256": source_sha256,
                    "raw_response_id": raw_response_id,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
            if (
                artifact_row is None
                or artifact_row["content_id"] != content_id
                or artifact_row["artifact_type"] != "media_source"
                or artifact_row["local_path"] != _relative(target)
                or artifact_row["status"] != "available"
                or artifact_row["byte_size"] != target_evidence.byte_size
                or artifact_row["sha256"] != target_evidence.sha256
                or artifact_row["processor_version"] != MEDIA_SOURCE_VERSION
                or artifact_row["metadata_json"] != expected_metadata_json
            ):
                raise MediaProcessingError(
                    "registered media source evidence drifted"
                )
            return artifact
    except BaseException:
        if created_target:
            if created_evidence is None:
                raise MediaProcessingError(
                    "staged media source cleanup evidence is missing"
                )
            _quarantine_unregistered_media_source(created_evidence)
        raise


def _valid_image(path: Path) -> bool:
    if not path.is_file() or path.stat().st_size <= 512:
        return False
    with path.open("rb") as handle:
        header = handle.read(16)
    return _valid_image_header(header)


def _valid_image_header(header: bytes) -> bool:
    return bool(
        header.startswith(b"\xff\xd8\xff")
        or header.startswith(b"\x89PNG\r\n\x1a\n")
        or (header.startswith(b"RIFF") and header[8:12] == b"WEBP")
        or header.startswith((b"GIF87a", b"GIF89a"))
    )


def _prepare_image_download_directory(
    target_dir: Path,
    *,
    groups: Iterable[Mapping[str, Any]],
    reuse_existing: bool,
) -> None:
    if os.path.lexists(target_dir):
        metadata = target_dir.lstat()
        if not stat.S_ISDIR(metadata.st_mode) or target_dir.is_symlink():
            raise MediaProcessingError(
                "image download target is not a private directory"
            )
    else:
        target_dir.mkdir(parents=True, exist_ok=False)
    allowed_names = {"manifest.json", ".manifest.json.tmp"}
    candidate_names: set[str] = {".manifest.json.tmp"}
    for group in groups:
        group_index = int(group["group_index"])
        name = f"image-{group_index:03d}.bin"
        allowed_names.add(name)
        candidate_names.add(f".{name}.tmp")
        allowed_names.add(f".{name}.tmp")
        for attempt_index, _candidate in enumerate(group["candidates"]):
            attempt_name = f".{name}.attempt-{attempt_index}.tmp"
            candidate_names.add(attempt_name)
            allowed_names.add(attempt_name)
    entries = list(target_dir.iterdir())
    for entry in entries:
        if entry.name not in allowed_names:
            raise MediaProcessingError(
                f"image download target contains unknown entry: {entry.name}"
            )
        metadata = entry.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or entry.is_symlink()
            or metadata.st_nlink != 1
        ):
            raise MediaProcessingError(
                f"image download target entry is not a private file: {entry.name}"
            )
        if entry.name in candidate_names:
            raise MediaProcessingError(
                f"image response candidate path is occupied: {entry.name}"
            )
    if not reuse_existing:
        for entry in entries:
            if entry.name in allowed_names and entry.name not in candidate_names:
                entry.unlink()


def _download_images(
    urls: Iterable[str],
    target_dir: Path,
    *,
    platform: str,
    frozen_image_groups: Optional[Iterable[Mapping[str, Any]]] = None,
    urlopen_fn: Optional[Callable[..., Any]] = None,
    maximum_bytes: Optional[int] = None,
    require_exact_response_url: bool = False,
    reuse_existing: bool = True,
    trusted_root: Optional[Path] = None,
) -> Path:
    raw_values = list(urls)
    if any(type(value) is not str for value in raw_values):
        raise MediaProcessingError("image download URLs must be exact strings")
    values, source_sha256 = _media_source_identity("image", raw_values)
    if raw_values != values:
        raise MediaProcessingError(
            "image download URLs contain duplicate or noncanonical values"
        )
    if platform == "douyin" and frozen_image_groups is None:
        raise MediaProcessingError(
            "Douyin image download requires frozen discovery groups"
        )
    groups = (
        image_source_groups(values, platform=platform)
        if frozen_image_groups is None
        else validate_frozen_image_groups(
            values, frozen_image_groups, platform=platform
        )
    )
    groups_sha256 = image_groups_sha256(groups)
    download_binding_sha256 = image_download_binding_sha256(
        source_sha256, groups_sha256
    )
    if not groups:
        raise MediaProcessingError("image download has no logical source groups")
    _prepare_image_download_directory(
        target_dir,
        groups=groups,
        reuse_existing=reuse_existing,
    )
    paths: List[Path] = []
    selected_spools: List[tuple[Path, _SpooledResponse]] = []
    manifest_groups: List[Dict[str, Any]] = []
    incomplete_errors: List[str] = []
    for group in groups:
        group_index = int(group["group_index"])
        target = target_dir / f"image-{group_index:03d}.bin"
        attempts: List[Dict[str, Any]] = []
        selected_candidate: Optional[Mapping[str, Any]] = None
        selected_sha256: Optional[str] = None
        selected_size = 0
        if os.path.lexists(target):
            raise MediaProcessingError(
                "image response target already exists before download"
            )
        for attempt_index, candidate in enumerate(group["candidates"]):
            url = str(candidate["url"])
            temporary = target.with_name(
                f".{target.name}.attempt-{attempt_index}.tmp"
            )
            if os.path.lexists(temporary):
                raise MediaProcessingError(
                    "image response candidate path is occupied or aliased"
                )
            response_opened = False
            request = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "image/*,*/*;q=0.8",
                },
            )
            spool: Optional[_SpooledResponse] = None
            failure_spool: Optional[_SpooledResponse] = None
            retain_selected_spool = False
            try:
                opener = urlopen_fn or urllib.request.urlopen
                with opener(request, timeout=60) as response:
                    response_opened = True
                    final_url = str(response.geturl() or url)
                    if require_exact_response_url and final_url != url:
                        raise MediaProcessingError(
                            f"image response URL changed: {url!r} -> {final_url!r}"
                        )
                    spool = _write_bounded_response(
                        response,
                        maximum_bytes=maximum_bytes,
                    )
                _after_bounded_response_write(temporary)
                if os.path.lexists(temporary):
                    raise _ResponseTargetCollisionError(
                        "image response candidate path was occupied after spool"
                    )
                response_sha256 = spool.sha256
                response_size = spool.byte_size
                if (
                    response_size <= 512
                    or not _valid_image_header(spool.header)
                ):
                    attempts.append(
                        {
                            "attempt_index": attempt_index,
                            "source_index": int(candidate["source_index"]),
                            "profile": str(candidate["profile"]),
                            "url_sha256": str(candidate["url_sha256"]),
                            "outcome": "unsupported_image",
                            "response_sha256": response_sha256,
                            "byte_size": response_size,
                            "error": "unsupported_image",
                        }
                    )
                    continue
                if os.path.lexists(target):
                    raise _ResponseTargetCollisionError(
                        "image response target changed during download"
                    )
                selected_spools.append((target, spool))
                retain_selected_spool = True
                selected_candidate = candidate
                selected_sha256 = str(response_sha256)
                selected_size = response_size
                attempts.append(
                    {
                        "attempt_index": attempt_index,
                        "source_index": int(candidate["source_index"]),
                        "profile": str(candidate["profile"]),
                        "url_sha256": str(candidate["url_sha256"]),
                        "outcome": "selected",
                        "response_sha256": selected_sha256,
                        "byte_size": selected_size,
                        "error": None,
                    }
                )
                break
            except _ResponseTargetCollisionError as exc:
                raise MediaProcessingError(
                    "image response path is occupied or aliased"
                ) from exc
            except (
                urllib.error.URLError,
                TimeoutError,
                ConnectionError,
                OSError,
                MediaProcessingError,
            ) as exc:
                response_size = 0
                failure_response_sha256: Optional[str] = None
                partial_spool = getattr(exc, "_media_response_spool", None)
                if isinstance(partial_spool, _SpooledResponse):
                    failure_spool = partial_spool
                    _after_bounded_response_write(temporary)
                    response_size = partial_spool.byte_size
                    failure_response_sha256 = partial_spool.sha256
                if os.path.lexists(temporary):
                    raise MediaProcessingError(
                        "failed image response candidate path is occupied or aliased"
                    ) from exc
                if failure_response_sha256 is None and response_opened:
                    failure_response_sha256 = hashlib.sha256(b"").hexdigest()
                attempts.append(
                    {
                        "attempt_index": attempt_index,
                        "source_index": int(candidate["source_index"]),
                        "profile": str(candidate["profile"]),
                        "url_sha256": str(candidate["url_sha256"]),
                        "outcome": "request_failed",
                        "response_sha256": failure_response_sha256,
                        "byte_size": response_size,
                        "error": type(exc).__name__,
                    }
                )
            finally:
                if spool is not None and not retain_selected_spool:
                    spool.close()
                if failure_spool is not None and failure_spool is not spool:
                    failure_spool.close()
        if selected_candidate is None or selected_sha256 is None:
            incomplete_errors.append(f"logical image group {group_index} exhausted")
            continue
        paths.append(target)
        source_order_candidates = sorted(
            group["candidates"], key=lambda candidate: int(candidate["source_index"])
        )
        manifest_groups.append(
            {
                "group_index": group_index,
                "identity": group["identity"],
                "source_url_sha256s": [
                    str(candidate["url_sha256"])
                    for candidate in source_order_candidates
                ],
                "selected_url_sha256": str(selected_candidate["url_sha256"]),
                "selected_response_sha256": selected_sha256,
                "selected_byte_size": selected_size,
                "image_path": _relative(target),
                "attempts": attempts,
            }
        )
    if len(paths) != len(groups) or incomplete_errors:
        for _path, selected_spool in selected_spools:
            selected_spool.close()
        raise MediaProcessingError(
            "image download incomplete: " + " | ".join(incomplete_errors[-3:])
        )
    try:
        for target, selected_spool in selected_spools:
            staging = target.with_name(f".{target.name}.tmp")
            _publish_spooled_response(
                selected_spool,
                target,
                staging=staging,
                trusted_root=(
                    target_dir if trusted_root is None else trusted_root
                ),
                label="published image response",
            )
    finally:
        for _target, selected_spool in selected_spools:
            selected_spool.close()
    manifest = target_dir / "manifest.json"
    _publish_private_image_manifest(
        manifest,
        {
            "schema_version": IMAGE_MANIFEST_VERSION,
            "status": "complete",
            "source_url_count": len(values),
            "source_count": len(groups),
            "source_sha256": source_sha256,
            "image_groups_sha256": groups_sha256,
            "download_binding_sha256": download_binding_sha256,
            "image_paths": [_relative(path) for path in paths],
            "frames": [
                {"path": _relative(path), "sha256": file_sha256(path)}
                for path in paths
            ],
            "groups": manifest_groups,
        },
        trusted_root=(target_dir if trusted_root is None else trusted_root),
    )
    return manifest


def download_image_sources(
    content_id: int,
    urls: Iterable[str],
    *,
    db_path: Path = DEFAULT_DB,
    media_root: Optional[Path] = None,
    urlopen_fn: Optional[Callable[..., Any]] = None,
    maximum_bytes: Optional[int] = None,
    require_exact_response_url: bool = False,
    download_urls: Optional[Iterable[str]] = None,
    frozen_image_groups: Optional[Iterable[Mapping[str, Any]]] = None,
    reuse_existing: bool = True,
    _slot_source_sha256: Optional[str] = None,
    _preclaimed_slot_id: Optional[int] = None,
) -> Artifact:
    raw_values = list(urls)
    if any(type(value) is not str for value in raw_values):
        raise MediaProcessingError("image download URLs must be exact strings")
    values, source_sha256 = _media_source_identity("image", raw_values)
    if raw_values != values:
        raise MediaProcessingError(
            "image download URLs contain duplicate or noncanonical values"
        )
    if not values:
        raise MediaProcessingError("provider returned no supported image source")
    if download_urls is None:
        selected_values = values
    else:
        raw_download_values = list(download_urls)
        if any(type(value) is not str for value in raw_download_values):
            raise MediaProcessingError(
                "selected image download URLs must be exact strings"
            )
        selected_values = _media_source_identity("image", raw_download_values)[0]
        if selected_values != raw_download_values:
            raise MediaProcessingError(
                "selected image download URLs contain duplicate or noncanonical values"
            )
    if selected_values != values:
        raise MediaProcessingError("image download requires every frozen source URL")
    with connect(db_path) as connection:
        content = connection.execute(
            "SELECT link_id,platform,content_type FROM content_items WHERE id=?",
            (content_id,),
        ).fetchone()
        source_rows = connection.execute(
            """
            SELECT * FROM evidence_artifacts
            WHERE content_id=? AND artifact_type='media_source'
              AND status='available'
            ORDER BY id DESC
            """,
            (content_id,),
        ).fetchall()
    if content is None:
        raise MediaProcessingError(f"unknown content {content_id}")
    if type(content["platform"]) is not str or content["content_type"] != "image":
        raise MediaProcessingError("image download content identity drifted")
    platform = content["platform"]
    if platform == "douyin" and frozen_image_groups is None:
        raise MediaProcessingError(
            "Douyin image download requires frozen discovery groups"
        )
    if frozen_image_groups is None:
        groups = image_source_groups(values, platform=platform)
    else:
        groups = validate_frozen_image_groups(
            values, frozen_image_groups, platform=platform
        )
    if not source_rows:
        raise MediaProcessingError(
            "image download requires current media_source evidence"
        )
    source_urls, source_manifest_sha256 = _validated_image_media_source(
        source_rows[0]
    )
    if source_urls != values or source_manifest_sha256 != source_sha256:
        raise MediaProcessingError(
            "image download URLs do not match current media_source"
        )
    source_artifact_id = int(source_rows[0]["id"])
    groups_sha256 = image_groups_sha256(groups)
    download_binding_sha256 = image_download_binding_sha256(
        source_sha256, groups_sha256
    )
    effective_media_root = media_root if media_root is not None else MEDIA_ROOT
    link_id = _validated_link_id(content["link_id"])
    target_dir = (
        effective_media_root
        / link_id
        / "downloads"
        / download_binding_sha256
        / "images"
    )
    _require_no_symlink_below_root(
        target_dir, root=effective_media_root, label="image download target"
    )

    def revalidate_context(connection: sqlite3.Connection) -> None:
        current_content = connection.execute(
            "SELECT link_id,platform,content_type FROM content_items WHERE id=?",
            (content_id,),
        ).fetchone()
        current_sources = connection.execute(
            """
            SELECT * FROM evidence_artifacts
            WHERE content_id=? AND artifact_type='media_source'
              AND status='available'
            ORDER BY id DESC
            """,
            (content_id,),
        ).fetchall()
        if (
            current_content is None
            or current_content["link_id"] != content["link_id"]
            or current_content["platform"] != platform
            or current_content["content_type"] != "image"
            or not current_sources
            or current_sources[0]["id"] != source_artifact_id
        ):
            raise MediaProcessingError(
                "image download content/source identity changed"
            )
        current_urls, current_sha256 = _validated_image_media_source(
            current_sources[0]
        )
        if current_urls != values or current_sha256 != source_sha256:
            raise MediaProcessingError(
                "image download current media_source changed"
            )
        validate_frozen_image_groups(values, groups, platform=platform)

    def produce() -> Path:
        if (
            urlopen_fn is None
            and maximum_bytes is None
            and not require_exact_response_url
            and download_urls is None
            and reuse_existing
        ):
            return _download_images(
                values,
                target_dir,
                platform=platform,
                frozen_image_groups=groups,
            )
        return _download_images(
            selected_values,
            target_dir,
            platform=platform,
            frozen_image_groups=groups,
            urlopen_fn=urlopen_fn,
            maximum_bytes=maximum_bytes,
            require_exact_response_url=require_exact_response_url,
            reuse_existing=reuse_existing,
        )

    expected_metadata = {
        "source_count": len(groups),
        "source_url_count": len(values),
        "source_sha256": source_sha256,
        "image_groups_sha256": groups_sha256,
        "download_binding_sha256": download_binding_sha256,
    }

    def validate_cached(
        connection: sqlite3.Connection,
        artifact: Artifact,
        manifest_evidence: _PrivateFileEvidence,
    ) -> None:
        _validate_cached_image_download(
            connection,
            artifact=artifact,
            manifest_evidence=manifest_evidence,
            content_id=content_id,
            source_urls=values,
            platform=platform,
            frozen_image_groups=groups,
            expected_manifest=target_dir / "manifest.json",
            expected_metadata=expected_metadata,
        )

    effective_slot_source_sha256 = (
        _slot_source_sha256
        or (
            str(source_rows[0]["sha256"])
            if source_rows[0]["processor_version"] == MEDIA_SOURCE_VERSION
            and _valid_sha256(source_rows[0]["sha256"])
            else download_binding_sha256
        )
    )
    return _run_processing_slot(
        db_path=db_path,
        content_id=content_id,
        source_sha256=effective_slot_source_sha256,
        processor_type="download",
        processor_version=IMAGE_DOWNLOAD_VERSION,
        artifact_type="media_manifest",
        produce=produce,
        metadata=expected_metadata,
        expected_output_path=target_dir / "manifest.json",
        expected_output_root=effective_media_root,
        cached_validator=validate_cached,
        claim_validator=revalidate_context,
        commit_validator=revalidate_context,
        preclaimed_slot_id=_preclaimed_slot_id,
    )


def _extract_frames(media_path: Path, target_dir: Path) -> Path:
    config = load_media_config()["frames"]
    duration = _probe_duration(media_path)
    if duration <= 0:
        raise MediaProcessingError("media has no readable duration")
    count = min(int(config["maximum_frames"]), max(6, round(duration / 3)))
    target_dir.mkdir(parents=True, exist_ok=True)
    frame_paths: List[Path] = []
    frame_temporary_paths: List[Path] = []
    for index in range(count):
        timestamp = min(max(0.1, (index + 0.5) * duration / count), max(0.1, duration - 0.1))
        target = target_dir / f"frame-{index:03d}.jpg"
        temporary = target.with_name(f".{target.stem}.tmp{target.suffix}")
        temporary.unlink(missing_ok=True)
        completed = subprocess.run(
            [
                "ffmpeg", "-y", "-nostdin", "-loglevel", "error", "-ss", str(timestamp),
                "-i", str(media_path), "-frames:v", "1",
                "-vf", f"scale={int(config['target_width'])}:-1", "-q:v", "3", str(temporary),
            ],
            check=False,
            capture_output=True,
            timeout=60,
        )
        if (
            completed.returncode == 0
            and temporary.is_file()
            and temporary.stat().st_size > 1024
            and _valid_image(temporary)
        ):
            frame_paths.append(target)
            frame_temporary_paths.append(temporary)
        else:
            temporary.unlink(missing_ok=True)
    if not frame_paths:
        raise MediaProcessingError("no frames were extracted")
    contact_sheet = target_dir / "contact-sheet.jpg"
    contact_temporary = contact_sheet.with_name(
        f".{contact_sheet.stem}.tmp{contact_sheet.suffix}"
    )
    contact_temporary.unlink(missing_ok=True)
    subprocess.run(
        [
            "ffmpeg", "-y", "-nostdin", "-loglevel", "error", "-framerate", "1",
            "-i", str(target_dir / ".frame-%03d.tmp.jpg"), "-vf",
            "scale=480:-1,tile=4x6:padding=4:margin=4", "-frames:v", "1", str(contact_temporary),
        ],
        check=False,
        capture_output=True,
        timeout=90,
    )
    contact_ready = bool(
        contact_temporary.is_file()
        and contact_temporary.stat().st_size > 1024
        and _valid_image(contact_temporary)
    )
    if not contact_ready:
        contact_temporary.unlink(missing_ok=True)
    manifest = target_dir / "frames.json"
    _atomic_json(
        manifest,
        {
            "status": "success",
            "duration_seconds": round(duration, 3),
            "frames": [
                {"path": _relative(path), "sha256": file_sha256(temporary)}
                for path, temporary in zip(
                    frame_paths, frame_temporary_paths, strict=True
                )
            ],
            "contact_sheet": _relative(contact_sheet) if contact_ready else None,
        },
    )
    for target, temporary in zip(
        frame_paths, frame_temporary_paths, strict=True
    ):
        temporary.replace(target)
    if contact_ready:
        contact_temporary.replace(contact_sheet)
    return manifest


def _frame_paths(manifest_path: Path) -> List[Path]:
    value = json.loads(manifest_path.read_text(encoding="utf-8"))
    return [_resolved(str(item["path"])) for item in value.get("frames", [])]


def _run_ocr(
    manifest_path: Path,
    target: Path,
    *,
    binary_path: Optional[Path] = None,
    validated_frame_paths: Optional[Iterable[Path]] = None,
) -> Path:
    binary = binary_path if binary_path is not None else ocr_binary_path()
    if not binary.is_file():
        raise MediaProcessingError(f"OCR binary is missing: {binary}")
    frames = (
        _frame_paths(manifest_path)
        if validated_frame_paths is None
        else list(validated_frame_paths)
    )
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


def _validate_ocr_output_body(
    body: Any,
    *,
    expected_source_count: int,
) -> None:
    observations = body.get("observations") if isinstance(body, Mapping) else None
    normalized_texts: List[str] = []
    if isinstance(observations, list):
        for item in observations:
            if not isinstance(item, Mapping):
                continue
            text = "\n".join(str(item.get("text") or "").splitlines()).strip()
            if text and text not in normalized_texts:
                normalized_texts.append(text)
    if (
        not isinstance(body, Mapping)
        or set(body)
        != {
            "status",
            "processor_version",
            "source_count",
            "ocr_observation_count",
            "combined_text",
            "observations",
        }
        or body.get("status") != "success"
        or body.get("processor_version") != processor_versions()["ocr"]
        or type(body.get("source_count")) is not int
        or body["source_count"] != expected_source_count
        or type(body.get("ocr_observation_count")) is not int
        or not isinstance(observations, list)
        or body["ocr_observation_count"] != len(observations)
        or len(observations) != expected_source_count
        or type(body.get("combined_text")) is not str
        or body["combined_text"] != "\n".join(normalized_texts)
        or any(not isinstance(item, Mapping) for item in observations)
    ):
        raise MediaProcessingError("OCR output body contract drifted")


def _cached_json_body(
    evidence: _PrivateFileEvidence, *, label: str
) -> Dict[str, Any]:
    try:
        body = json.loads(evidence.body or b"")
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MediaProcessingError(f"{label} body is invalid") from exc
    if type(body) is not dict:
        raise MediaProcessingError(f"{label} body is not an exact object")
    return body


def _validate_video_frames_output(
    body: Any,
    *,
    manifest_path: Path,
    content_root: Path,
    media_root: Path,
    maximum_duration_seconds: Optional[float],
) -> List[Path]:
    frames = body.get("frames") if type(body) is dict else None
    duration = body.get("duration_seconds") if type(body) is dict else None
    if (
        type(body) is not dict
        or set(body) != {"status", "duration_seconds", "frames", "contact_sheet"}
        or body.get("status") != "success"
        or type(duration) is not float
        or not math.isfinite(duration)
        or duration <= 0
        or (
            maximum_duration_seconds is not None
            and duration > maximum_duration_seconds
        )
        or type(frames) is not list
        or not frames
        or len(frames) > int(load_media_config()["frames"]["maximum_frames"])
    ):
        raise MediaProcessingError("cached frames manifest contract drifted")
    expected_directory = content_root / "frames"
    _require_no_symlink_below_root(
        manifest_path,
        root=media_root,
        label="cached frames manifest",
    )
    frame_paths: List[Path] = []
    for index, item in enumerate(frames):
        if (
            type(item) is not dict
            or set(item) != {"path", "sha256"}
            or type(item.get("path")) is not str
            or not _valid_sha256(item.get("sha256"))
        ):
            raise MediaProcessingError("cached frame entry contract drifted")
        expected_path = expected_directory / f"frame-{index:03d}.jpg"
        actual_path = _resolved(item["path"])
        if actual_path.resolve() != expected_path.resolve():
            raise MediaProcessingError("cached frame path drifted")
        _require_no_symlink_below_root(
            actual_path,
            root=media_root,
            label="cached frame",
        )
        evidence = _read_private_file_evidence(
            actual_path, label="cached frame"
        )
        if (
            evidence.byte_size <= 512
            or not _valid_image_header(evidence.header)
            or evidence.sha256 != item["sha256"]
        ):
            raise MediaProcessingError("cached frame file evidence drifted")
        frame_paths.append(actual_path)
    contact_sheet = body.get("contact_sheet")
    if contact_sheet is not None:
        if type(contact_sheet) is not str:
            raise MediaProcessingError("cached contact sheet path drifted")
        contact_path = _resolved(contact_sheet)
        expected_contact = expected_directory / "contact-sheet.jpg"
        if contact_path.resolve() != expected_contact.resolve():
            raise MediaProcessingError("cached contact sheet path drifted")
        _require_no_symlink_below_root(
            contact_path,
            root=media_root,
            label="cached contact sheet",
        )
        contact_evidence = _read_private_file_evidence(
            contact_path, label="cached contact sheet"
        )
        if (
            contact_evidence.byte_size <= 512
            or not _valid_image_header(contact_evidence.header)
        ):
            raise MediaProcessingError("cached contact sheet evidence drifted")
    return frame_paths


def _validate_asr_output_body(body: Any) -> None:
    config = load_media_config()["asr"]
    common_keys = {
        "status",
        "processor_version",
        "model_id",
        "model_revision",
        "language",
        "text",
        "segments",
    }
    if type(body) is not dict or type(body.get("status")) is not str:
        raise MediaProcessingError("cached ASR body contract drifted")
    status = body["status"]
    if status == "success":
        expected_keys = common_keys | {"elapsed_seconds"}
        elapsed = body.get("elapsed_seconds")
        if (
            type(elapsed) is not float
            or not math.isfinite(elapsed)
            or elapsed < 0
        ):
            raise MediaProcessingError("cached ASR elapsed time drifted")
    elif status == "unavailable":
        expected_keys = common_keys | {"reason"}
        if (
            body.get("reason") != "audio_decode_failed"
            or body.get("language") != config["language"]
            or body.get("text") != ""
            or body.get("segments") != []
        ):
            raise MediaProcessingError("cached ASR unavailable body drifted")
    else:
        raise MediaProcessingError("cached ASR status drifted")
    segments = body.get("segments")
    if (
        set(body) != expected_keys
        or body.get("processor_version") != processor_versions()["asr"]
        or body.get("model_id") != config["model_id"]
        or body.get("model_revision") != config["model_revision"]
        or type(body.get("language")) is not str
        or not body["language"]
        or type(body.get("text")) is not str
        or body["text"] != body["text"].strip()
        or type(segments) is not list
    ):
        raise MediaProcessingError("cached ASR body contract drifted")
    segment_keys = {"start", "end", "text", "avg_logprob", "no_speech_prob"}
    for segment in segments:
        if (
            type(segment) is not dict
            or set(segment) != segment_keys
            or type(segment.get("text")) is not str
            or segment["text"] != segment["text"].strip()
        ):
            raise MediaProcessingError("cached ASR segment contract drifted")
        for field in {"start", "end", "avg_logprob", "no_speech_prob"}:
            value = segment.get(field)
            if value is not None and (
                type(value) is not float or not math.isfinite(value)
            ):
                raise MediaProcessingError("cached ASR segment value drifted")
        start = segment.get("start")
        end = segment.get("end")
        no_speech_probability = segment.get("no_speech_prob")
        if (
            (start is not None and start < 0)
            or (end is not None and end < 0)
            or (start is not None and end is not None and end < start)
            or (
                no_speech_probability is not None
                and not 0 <= no_speech_probability <= 1
            )
        ):
            raise MediaProcessingError("cached ASR segment semantics drifted")


def _run_asr(
    media_path: Path,
    target: Path,
    *,
    model_path: Optional[Path] = None,
) -> Path:
    import mlx_whisper  # type: ignore[import-untyped]

    config = load_media_config()["asr"]
    effective_model_path = (
        model_path if model_path is not None else pinned_whisper_model_path()
    )
    started = time.monotonic()
    try:
        raw = mlx_whisper.transcribe(
            str(media_path),
            path_or_hf_repo=str(effective_model_path),
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
    media_root: Optional[Path] = None,
    whisper_model_path: Optional[Path] = None,
    ocr_binary: Optional[Path] = None,
    maximum_duration_seconds: Optional[float] = None,
) -> Dict[str, Artifact]:
    if not _valid_media(
        media_path, maximum_duration_seconds=maximum_duration_seconds
    ):
        raise MediaProcessingError(f"invalid media: {media_path}")
    versions = processor_versions()
    with connect(db_path) as connection, transaction(connection):
        existing_media = connection.execute(
            """
            SELECT * FROM evidence_artifacts
            WHERE content_id=? AND artifact_type='media' AND local_path=?
            """,
            (content_id, _relative(media_path)),
        ).fetchone()
        if existing_media is None:
            media = register_artifact(
                connection,
                content_id=content_id,
                artifact_type="media",
                path=media_path,
                processor_version="provider-media-v8.0",
            )
        else:
            media_evidence = _read_private_file_evidence(
                media_path, label="registered video media"
            )
            if (
                existing_media["status"] != "available"
                or type(existing_media["byte_size"]) is not int
                or existing_media["byte_size"] != media_evidence.byte_size
                or existing_media["sha256"] != media_evidence.sha256
                or existing_media["processor_version"]
                not in {
                    "provider-media-v8.0",
                    VIDEO_DOWNLOAD_VERSION,
                    LEGACY_VIDEO_DOWNLOAD_VERSION,
                }
            ):
                raise MediaProcessingError("registered video media evidence drifted")
            media = Artifact(
                id=int(existing_media["id"]),
                content_id=content_id,
                artifact_type="media",
                local_path=str(existing_media["local_path"]),
                sha256=str(existing_media["sha256"]),
                processor_version=str(existing_media["processor_version"]),
            )
        content = connection.execute(
            "SELECT link_id FROM content_items WHERE id=?", (content_id,)
        ).fetchone()
        if content is None:
            raise MediaProcessingError(f"unknown content {content_id}")
        link_id = _validated_link_id(content["link_id"])
    effective_media_root = media_root if media_root is not None else MEDIA_ROOT
    content_root = effective_media_root / link_id
    media_input_evidence = _read_private_file_evidence(
        media_path, label="video processing media input"
    )

    def validate_media_input(connection: sqlite3.Connection) -> None:
        current_content = connection.execute(
            "SELECT link_id,content_type FROM content_items WHERE id=?",
            (content_id,),
        ).fetchone()
        current_media = connection.execute(
            "SELECT * FROM evidence_artifacts WHERE id=?",
            (media.id,),
        ).fetchone()
        if (
            current_content is None
            or current_content["link_id"] != link_id
            or current_content["content_type"] != "video"
            or current_media is None
            or type(current_media["content_id"]) is not int
            or current_media["content_id"] != content_id
            or current_media["artifact_type"] != "media"
            or current_media["local_path"] != _relative(media_path)
            or current_media["local_path"] != media.local_path
            or current_media["status"] != "available"
            or type(current_media["byte_size"]) is not int
            or current_media["byte_size"] != media_input_evidence.byte_size
            or current_media["sha256"] != media_input_evidence.sha256
            or current_media["sha256"] != media.sha256
            or current_media["processor_version"] != media.processor_version
            or current_media["processor_version"]
            not in {
                "provider-media-v8.0",
                VIDEO_DOWNLOAD_VERSION,
                LEGACY_VIDEO_DOWNLOAD_VERSION,
            }
        ):
            raise MediaProcessingError("video processing media input drifted")
        _assert_private_file_evidence_current(
            media_input_evidence, label="video processing media input"
        )

    def validate_cached_frames(
        connection: sqlite3.Connection,
        _artifact: Artifact,
        evidence: _PrivateFileEvidence,
    ) -> None:
        validate_media_input(connection)
        body = _cached_json_body(evidence, label="cached frames manifest")
        _validate_video_frames_output(
            body,
            manifest_path=evidence.path,
            content_root=content_root,
            media_root=effective_media_root,
            maximum_duration_seconds=maximum_duration_seconds,
        )

    frames = _run_processing_slot(
        db_path=db_path,
        content_id=content_id,
        source_sha256=media.sha256,
        processor_type="frames",
        processor_version=versions["frames"],
        artifact_type="frames_manifest",
        produce=lambda: _extract_frames(media_path, content_root / "frames"),
        expected_output_path=content_root / "frames" / "frames.json",
        expected_output_root=effective_media_root,
        cached_validator=validate_cached_frames,
    )

    def validate_current_frames(connection: sqlite3.Connection) -> List[Path]:
        validate_media_input(connection)
        expected_manifest = content_root / "frames" / "frames.json"
        current_frames = connection.execute(
            "SELECT * FROM evidence_artifacts WHERE id=?", (frames.id,)
        ).fetchone()
        frame_slots = connection.execute(
            """
            SELECT * FROM media_processing_slots
            WHERE content_id=? AND source_sha256=? AND processor_type='frames'
              AND processor_version=?
            """,
            (content_id, media.sha256, versions["frames"]),
        ).fetchall()
        if (
            current_frames is None
            or type(current_frames["content_id"]) is not int
            or current_frames["content_id"] != content_id
            or current_frames["artifact_type"] != "frames_manifest"
            or current_frames["local_path"] != _relative(expected_manifest)
            or current_frames["local_path"] != frames.local_path
            or current_frames["status"] != "available"
            or type(current_frames["byte_size"]) is not int
            or current_frames["byte_size"] <= 0
            or current_frames["sha256"] != frames.sha256
            or current_frames["processor_version"] != versions["frames"]
            or current_frames["processor_version"] != frames.processor_version
            or current_frames["metadata_json"] != "{}"
            or len(frame_slots) != 1
            or frame_slots[0]["status"] != "succeeded"
            or frame_slots[0]["output_artifact_id"] != frames.id
        ):
            raise MediaProcessingError("current frames artifact closure drifted")
        _require_no_symlink_below_root(
            expected_manifest,
            root=effective_media_root,
            label="current frames manifest",
        )
        evidence = _read_private_file_evidence(
            expected_manifest,
            label="current frames manifest",
            capture_body=True,
        )
        if (
            evidence.byte_size != current_frames["byte_size"]
            or evidence.sha256 != current_frames["sha256"]
        ):
            raise MediaProcessingError("current frames file evidence drifted")
        body = _cached_json_body(evidence, label="current frames manifest")
        return _validate_video_frames_output(
            body,
            manifest_path=expected_manifest,
            content_root=content_root,
            media_root=effective_media_root,
            maximum_duration_seconds=maximum_duration_seconds,
        )

    def validate_cached_asr(
        connection: sqlite3.Connection,
        _artifact: Artifact,
        evidence: _PrivateFileEvidence,
    ) -> None:
        validate_media_input(connection)
        _validate_asr_output_body(
            _cached_json_body(evidence, label="cached ASR output")
        )

    asr = _run_processing_slot(
        db_path=db_path,
        content_id=content_id,
        source_sha256=media.sha256,
        processor_type="asr",
        processor_version=versions["asr"],
        artifact_type="asr",
        produce=(
            (lambda: _run_asr(media_path, content_root / "asr.json"))
            if whisper_model_path is None
            else lambda: _run_asr(
                media_path,
                content_root / "asr.json",
                model_path=whisper_model_path,
            )
        ),
        expected_output_path=content_root / "asr.json",
        expected_output_root=effective_media_root,
        cached_validator=validate_cached_asr,
    )

    def validate_cached_video_ocr(
        connection: sqlite3.Connection,
        _artifact: Artifact,
        evidence: _PrivateFileEvidence,
    ) -> None:
        frame_paths = validate_current_frames(connection)
        _validate_ocr_output_body(
            _cached_json_body(evidence, label="cached video OCR output"),
            expected_source_count=len(frame_paths),
        )

    ocr = _run_processing_slot(
        db_path=db_path,
        content_id=content_id,
        source_sha256=frames.sha256,
        processor_type="ocr",
        processor_version=versions["ocr"],
        artifact_type="ocr",
        produce=(
            (lambda: _run_ocr(
                _resolved(frames.local_path),
                content_root / "ocr.json",
                binary_path=ocr_binary,
            ))
            if ocr_binary is not None
            else lambda: _run_ocr(
                _resolved(frames.local_path), content_root / "ocr.json"
            )
        ),
        expected_output_path=content_root / "ocr.json",
        expected_output_root=effective_media_root,
        cached_validator=validate_cached_video_ocr,
    )
    return {"media": media, "frames": frames, "asr": asr, "ocr": ocr}


def _valid_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _private_regular_file(path: Path, *, label: str) -> os.stat_result:
    if path.is_symlink():
        raise MediaProcessingError(f"{label} must not be a symlink")
    try:
        metadata = path.lstat()
    except FileNotFoundError as exc:
        raise MediaProcessingError(f"{label} does not exist") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_size <= 0
    ):
        raise MediaProcessingError(f"{label} must be a private regular file")
    return metadata


def _require_no_symlink_below_root(
    path: Path, *, root: Path, label: str
) -> None:
    lexical_root = Path(os.path.abspath(root))
    lexical_path = Path(os.path.abspath(path))
    canonical_root = lexical_root.resolve()
    try:
        lexical_relative = lexical_path.relative_to(lexical_root)
        component_root = lexical_root
    except ValueError:
        try:
            lexical_relative = lexical_path.relative_to(canonical_root)
            component_root = canonical_root
        except ValueError as exc:
            raise MediaProcessingError(
                f"{label} path is outside media root"
            ) from exc
    for ancestor in (lexical_root, *lexical_root.parents):
        if not ancestor.is_symlink():
            continue
        if ancestor == Path("/var") and ancestor.resolve() == Path("/private/var"):
            continue
        raise MediaProcessingError(
            f"{label} media root ancestry must not contain a symlink: {ancestor}"
        )
    current = component_root
    for part in lexical_relative.parts:
        current /= part
        if current.is_symlink():
            raise MediaProcessingError(
                f"{label} path contains symlink component: {current}"
            )
    canonical_path = lexical_path.resolve()
    try:
        canonical_path.relative_to(canonical_root)
    except ValueError as exc:
        raise MediaProcessingError(f"{label} path is outside media root") from exc


def _validate_current_grouped_image_manifest(
    manifest_path: Path,
    manifest_body: Mapping[str, Any],
    *,
    source_urls: List[str],
    platform: str,
    frozen_image_groups: Optional[Iterable[Mapping[str, Any]]] = None,
) -> List[_PrivateFileEvidence]:
    if platform == "douyin" and frozen_image_groups is None:
        raise MediaProcessingError(
            "Douyin image manifest requires frozen discovery groups"
        )
    expected_groups = (
        image_source_groups(source_urls, platform=platform)
        if frozen_image_groups is None
        else validate_frozen_image_groups(
            source_urls, frozen_image_groups, platform=platform
        )
    )
    source_sha256 = _media_source_identity("image", source_urls)[1]
    groups_sha256 = image_groups_sha256(expected_groups)
    download_binding_sha256 = image_download_binding_sha256(
        source_sha256, groups_sha256
    )
    body_groups = manifest_body.get("groups")
    frames = manifest_body.get("frames")
    image_paths = manifest_body.get("image_paths")
    if (
        set(manifest_body)
        != {
            "schema_version",
            "status",
            "source_url_count",
            "source_count",
            "source_sha256",
            "image_groups_sha256",
            "download_binding_sha256",
            "image_paths",
            "frames",
            "groups",
        }
        or manifest_body.get("schema_version") != IMAGE_MANIFEST_VERSION
        or manifest_body.get("status") != "complete"
        or type(manifest_body.get("source_url_count")) is not int
        or manifest_body["source_url_count"] != len(source_urls)
        or type(manifest_body.get("source_count")) is not int
        or manifest_body["source_count"] != len(expected_groups)
        or not _valid_sha256(manifest_body.get("source_sha256"))
        or manifest_body.get("source_sha256") != source_sha256
        or not _valid_sha256(manifest_body.get("image_groups_sha256"))
        or manifest_body.get("image_groups_sha256") != groups_sha256
        or not _valid_sha256(manifest_body.get("download_binding_sha256"))
        or manifest_body.get("download_binding_sha256")
        != download_binding_sha256
        or not expected_groups
        or not isinstance(body_groups, list)
        or not isinstance(frames, list)
        or not isinstance(image_paths, list)
        or len(body_groups) != len(expected_groups)
        or len(frames) != len(expected_groups)
        or len(image_paths) != len(expected_groups)
    ):
        raise MediaProcessingError("image manifest logical-group evidence drifted")
    expected_files = [
        manifest_path.parent / f"image-{index:03d}.bin"
        for index in range(len(expected_groups))
    ]
    expected_paths = [_relative(path) for path in expected_files]
    if image_paths != expected_paths:
        raise MediaProcessingError("image manifest logical-group paths drifted")
    attempt_keys = {
        "attempt_index",
        "source_index",
        "profile",
        "url_sha256",
        "outcome",
        "response_sha256",
        "byte_size",
        "error",
    }
    group_keys = {
        "group_index",
        "identity",
        "source_url_sha256s",
        "selected_url_sha256",
        "selected_response_sha256",
        "selected_byte_size",
        "image_path",
        "attempts",
    }
    image_evidence: List[_PrivateFileEvidence] = []
    for index, (expected_group, body_group, frame, image_path) in enumerate(
        zip(expected_groups, body_groups, frames, expected_files, strict=True)
    ):
        if (
            not isinstance(body_group, Mapping)
            or set(body_group) != group_keys
            or type(body_group.get("group_index")) is not int
            or body_group["group_index"] != index
            or _canonical_json_bytes(body_group.get("identity"))
            != _canonical_json_bytes(expected_group["identity"])
        ):
            raise MediaProcessingError("image manifest logical-group identity drifted")
        candidates = list(expected_group["candidates"])
        source_order = sorted(
            candidates, key=lambda candidate: int(candidate["source_index"])
        )
        attempts = body_group.get("attempts")
        if (
            body_group.get("source_url_sha256s")
            != [str(candidate["url_sha256"]) for candidate in source_order]
            or not isinstance(attempts, list)
            or not attempts
            or len(attempts) > len(candidates)
        ):
            raise MediaProcessingError("image manifest logical-group attempts drifted")
        for attempt_index, (attempt, candidate) in enumerate(
            zip(attempts, candidates, strict=False)
        ):
            if (
                not isinstance(attempt, Mapping)
                or set(attempt) != attempt_keys
                or type(attempt.get("attempt_index")) is not int
                or attempt["attempt_index"] != attempt_index
                or type(attempt.get("source_index")) is not int
                or attempt["source_index"] != int(candidate["source_index"])
                or attempt.get("profile") != candidate["profile"]
                or attempt.get("url_sha256") != candidate["url_sha256"]
                or attempt.get("outcome")
                not in {"request_failed", "unsupported_image", "selected"}
            ):
                raise MediaProcessingError("image manifest candidate evidence drifted")
            outcome = str(attempt["outcome"])
            byte_size = attempt.get("byte_size")
            if outcome == "request_failed":
                if (
                    type(byte_size) is not int
                    or byte_size < 0
                    or not (
                        (
                            attempt.get("response_sha256") is None
                            and byte_size == 0
                        )
                        or (
                            _valid_sha256(attempt.get("response_sha256"))
                            and (
                                byte_size > 0
                                or attempt.get("response_sha256")
                                == hashlib.sha256(b"").hexdigest()
                            )
                        )
                    )
                    or not isinstance(attempt.get("error"), str)
                    or not str(attempt["error"])
                ):
                    raise MediaProcessingError("image request failure evidence drifted")
            elif outcome == "unsupported_image":
                if (
                    not _valid_sha256(attempt.get("response_sha256"))
                    or type(byte_size) is not int
                    or byte_size <= 0
                    or attempt.get("error") != "unsupported_image"
                ):
                    raise MediaProcessingError("unsupported image evidence drifted")
            elif (
                attempt_index != len(attempts) - 1
                or attempt.get("error") is not None
                or not _valid_sha256(attempt.get("response_sha256"))
                or type(byte_size) is not int
                or byte_size <= 0
            ):
                raise MediaProcessingError("selected image evidence drifted")
        selected = attempts[-1]
        if (
            selected.get("outcome") != "selected"
            or not _valid_sha256(body_group.get("selected_url_sha256"))
            or body_group.get("selected_url_sha256") != selected["url_sha256"]
            or not _valid_sha256(body_group.get("selected_response_sha256"))
            or body_group.get("selected_response_sha256")
            != selected["response_sha256"]
            or type(body_group.get("selected_byte_size")) is not int
            or body_group.get("selected_byte_size") != selected["byte_size"]
            or type(body_group.get("image_path")) is not str
            or body_group.get("image_path") != expected_paths[index]
            or not isinstance(frame, Mapping)
            or set(frame) != {"path", "sha256"}
            or type(frame.get("path")) is not str
            or frame.get("path") != expected_paths[index]
            or not _valid_sha256(frame.get("sha256"))
        ):
            raise MediaProcessingError("selected image projection drifted")
        selected_evidence = _read_private_file_evidence(
            image_path, label="selected image file"
        )
        if (
            selected_evidence.byte_size <= 512
            or not _valid_image_header(selected_evidence.header)
            or frame.get("sha256") != selected_evidence.sha256
            or selected_evidence.sha256
            != body_group["selected_response_sha256"]
            or selected_evidence.byte_size != body_group["selected_byte_size"]
        ):
            raise MediaProcessingError("selected image file evidence drifted")
        image_evidence.append(selected_evidence)
    return image_evidence


def _validate_cached_image_download(
    connection: sqlite3.Connection,
    *,
    artifact: Artifact,
    manifest_evidence: _PrivateFileEvidence,
    content_id: int,
    source_urls: List[str],
    platform: str,
    frozen_image_groups: Iterable[Mapping[str, Any]],
    expected_manifest: Path,
    expected_metadata: Mapping[str, Any],
) -> None:
    """Close a cached v8.3 image slot over its artifact, body, and files."""

    groups = validate_frozen_image_groups(
        source_urls, frozen_image_groups, platform=platform
    )
    source_sha256 = _media_source_identity("image", source_urls)[1]
    groups_sha256 = image_groups_sha256(groups)
    binding_sha256 = image_download_binding_sha256(source_sha256, groups_sha256)
    row = connection.execute(
        "SELECT * FROM evidence_artifacts WHERE id=?", (artifact.id,)
    ).fetchone()
    source_row = connection.execute(
        """
        SELECT sha256 FROM evidence_artifacts
        WHERE content_id=? AND artifact_type='media_source'
          AND status='available' AND processor_version=?
        ORDER BY id DESC LIMIT 1
        """,
        (content_id, MEDIA_SOURCE_VERSION),
    ).fetchone()
    accepted_slot_sources = {binding_sha256}
    if source_row is not None and _valid_sha256(source_row["sha256"]):
        accepted_slot_sources.add(str(source_row["sha256"]))
    placeholders = ",".join("?" for _ in accepted_slot_sources)
    slots = connection.execute(
        f"""
        SELECT * FROM media_processing_slots
        WHERE content_id=? AND source_sha256 IN ({placeholders})
          AND processor_type='download'
          AND processor_version=? AND output_artifact_id=?
        ORDER BY CASE WHEN source_sha256=? THEN 0 ELSE 1 END,id DESC
        """,
        (
            content_id,
            *sorted(accepted_slot_sources),
            IMAGE_DOWNLOAD_VERSION,
            artifact.id,
            str(source_row["sha256"]) if source_row is not None else binding_sha256,
        ),
    ).fetchall()
    if (
        row is None
        or not slots
        or slots[0]["status"] != "succeeded"
        or type(slots[0]["attempt_count"]) is not int
        or slots[0]["attempt_count"] <= 0
        or row["content_id"] != content_id
        or row["artifact_type"] != "media_manifest"
        or row["status"] != "available"
        or row["processor_version"] != IMAGE_DOWNLOAD_VERSION
        or row["local_path"] != _relative(expected_manifest)
        or row["local_path"] != artifact.local_path
        or row["byte_size"] != manifest_evidence.byte_size
        or row["sha256"] != manifest_evidence.sha256
        or row["metadata_json"]
        != json.dumps(dict(expected_metadata), ensure_ascii=False, sort_keys=True)
    ):
        raise MediaProcessingError("cached image download closure drifted")
    try:
        manifest_body = json.loads(manifest_evidence.body or b"")
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MediaProcessingError("cached image manifest body is invalid") from exc
    if not isinstance(manifest_body, dict):
        raise MediaProcessingError("cached image manifest body is invalid")
    _validate_current_grouped_image_manifest(
        manifest_evidence.path,
        manifest_body,
        source_urls=source_urls,
        platform=platform,
        frozen_image_groups=groups,
    )
    _assert_private_file_evidence_current(
        manifest_evidence, label="cached image manifest"
    )


def _validated_media_source(
    source_row: sqlite3.Row,
    *,
    expected_media_kind: str,
) -> tuple[List[str], str]:
    if str(source_row["processor_version"] or "") != MEDIA_SOURCE_VERSION:
        raise MediaProcessingError("download is not bound to current source")
    lexical_source_path = _resolved(str(source_row["local_path"]))
    if lexical_source_path.is_symlink():
        raise MediaProcessingError("media source must not be a symlink")
    try:
        source_path = lexical_source_path.resolve(strict=True)
    except FileNotFoundError as exc:
        raise MediaProcessingError("media source does not exist") from exc
    source_evidence = _read_private_file_evidence(
        source_path, label=f"{expected_media_kind} media source", capture_body=True
    )
    try:
        source_artifact_metadata = json.loads(
            str(source_row["metadata_json"] or "")
        )
    except json.JSONDecodeError as exc:
        raise MediaProcessingError("media source metadata is invalid") from exc
    try:
        source_body = json.loads(source_evidence.body or b"")
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MediaProcessingError("media source body is invalid") from exc
    if not isinstance(source_body, dict):
        raise MediaProcessingError("media source body is invalid")
    raw_urls = source_body.get("urls")
    if not isinstance(raw_urls, list) or not all(
        type(value) is str for value in raw_urls
    ):
        raise MediaProcessingError("media source URLs drifted")
    source_urls, source_sha256 = _media_source_identity(
        expected_media_kind, raw_urls
    )
    expected_source_metadata = {
        "media_kind": expected_media_kind,
        "source_count": len(source_urls),
        "source_sha256": source_sha256,
        "raw_response_id": source_body.get("raw_response_id"),
    }
    if (
        set(source_body)
        != {
            "schema_version",
            "media_kind",
            "urls",
            "source_sha256",
            "raw_response_id",
            "captured_at",
        }
        or source_body.get("schema_version") != MEDIA_SOURCE_VERSION
        or type(source_body.get("media_kind")) is not str
        or source_body.get("media_kind") != expected_media_kind
        or source_body.get("urls") != source_urls
        or source_body.get("source_sha256") != source_sha256
        or type(source_body.get("raw_response_id")) is not int
        or int(source_body["raw_response_id"]) <= 0
        or not isinstance(source_body.get("captured_at"), str)
        or not str(source_body["captured_at"])
        or source_artifact_metadata != expected_source_metadata
        or type(source_row["byte_size"]) is not int
        or source_row["byte_size"] != source_evidence.byte_size
        or str(source_row["sha256"] or "") != source_evidence.sha256
    ):
        raise MediaProcessingError("media source evidence drifted")
    return source_urls, source_sha256


def _validated_image_media_source(
    source_row: sqlite3.Row,
) -> tuple[List[str], str]:
    return _validated_media_source(source_row, expected_media_kind="image")


def _validated_video_media_source(
    source_row: sqlite3.Row,
) -> tuple[List[str], str]:
    return _validated_media_source(source_row, expected_media_kind="video")


def _validate_current_image_manifest_source(
    connection: sqlite3.Connection,
    *,
    content_id: int,
    content: sqlite3.Row,
    manifest_path: Path,
    manifest_body: Mapping[str, Any],
    download_binding_sha256: str,
    frozen_image_groups: Optional[Iterable[Mapping[str, Any]]] = None,
) -> List[_PrivateFileEvidence]:
    """Bind a current image manifest to its immutable media-source evidence."""

    if str(content["content_type"] or "") != "image":
        raise MediaProcessingError("image manifest source requires image content")
    platform = content["platform"]
    if type(platform) is not str:
        raise MediaProcessingError("image manifest source platform drifted")
    if platform == "douyin" and frozen_image_groups is None:
        raise MediaProcessingError(
            "Douyin image manifest requires frozen discovery groups"
        )
    source_rows = connection.execute(
        """
        SELECT * FROM evidence_artifacts
        WHERE content_id=? AND artifact_type='media_source' AND status='available'
        ORDER BY id DESC
        """,
        (content_id,),
    ).fetchall()
    if not source_rows:
        raise MediaProcessingError("image manifest is not bound to current source")
    source_urls, source_sha256 = _validated_image_media_source(source_rows[0])
    groups = (
        image_source_groups(
            source_urls, platform=platform
        )
        if frozen_image_groups is None
        else validate_frozen_image_groups(
            source_urls,
            frozen_image_groups,
            platform=platform,
        )
    )
    binding_sha256 = image_download_binding_sha256(
        source_sha256, image_groups_sha256(groups)
    )
    if binding_sha256 != download_binding_sha256:
        raise MediaProcessingError(
            "image manifest download binding does not match current source slot"
        )
    return _validate_current_grouped_image_manifest(
        manifest_path,
        manifest_body,
        source_urls=source_urls,
        platform=platform,
        frozen_image_groups=groups,
    )


def _revalidate_image_ocr_inputs(
    connection: sqlite3.Connection,
    *,
    content_id: int,
    manifest_artifact_id: int,
    manifest_evidence: _PrivateFileEvidence,
    manifest_body: Mapping[str, Any],
    artifact_metadata: Mapping[str, Any],
    slot_source_sha256: str,
    download_binding_sha256: str,
    frozen_image_groups: Optional[Iterable[Mapping[str, Any]]],
    expected_frames: List[_PrivateFileEvidence],
    media_root: Path,
) -> List[Path]:
    row = connection.execute(
        "SELECT * FROM evidence_artifacts WHERE id=?",
        (manifest_artifact_id,),
    ).fetchone()
    if row is None:
        raise MediaProcessingError("image manifest artifact disappeared")
    try:
        current_metadata = json.loads(str(row["metadata_json"] or ""))
    except json.JSONDecodeError as exc:
        raise MediaProcessingError("image manifest artifact metadata changed") from exc
    if (
        row["content_id"] != content_id
        or row["artifact_type"] != "media_manifest"
        or row["status"] != "available"
        or row["processor_version"] != IMAGE_DOWNLOAD_VERSION
        or row["local_path"] != _relative(manifest_evidence.path)
        or type(row["byte_size"]) is not int
        or row["byte_size"] != manifest_evidence.byte_size
        or row["sha256"] != manifest_evidence.sha256
        or _canonical_json_bytes(current_metadata)
        != _canonical_json_bytes(artifact_metadata)
    ):
        raise MediaProcessingError("image manifest artifact changed before OCR commit")
    content = connection.execute(
        "SELECT link_id,platform,content_type FROM content_items WHERE id=?",
        (content_id,),
    ).fetchone()
    if content is None:
        raise MediaProcessingError(f"unknown content {content_id}")
    slots = connection.execute(
        """
        SELECT * FROM media_processing_slots
        WHERE content_id=? AND processor_type='download'
          AND processor_version=? AND output_artifact_id=?
        """,
        (content_id, IMAGE_DOWNLOAD_VERSION, manifest_artifact_id),
    ).fetchall()
    current_slots = [
        slot for slot in slots if slot["source_sha256"] == slot_source_sha256
    ]
    if (
        len(current_slots) != 1
        or current_slots[0]["status"] != "succeeded"
        or type(current_slots[0]["attempt_count"]) is not int
        or current_slots[0]["attempt_count"] <= 0
    ):
        raise MediaProcessingError("image download slot changed before OCR commit")
    link_id = content["link_id"]
    if type(link_id) is not str or Path(link_id).name != link_id or link_id in {"", ".", ".."}:
        raise MediaProcessingError("image content link_id is not a safe basename")
    expected_manifest = (
        media_root
        / link_id
        / "downloads"
        / download_binding_sha256
        / "images"
        / "manifest.json"
    )
    _require_no_symlink_below_root(
        manifest_evidence.path, root=media_root, label="image manifest"
    )
    _require_no_symlink_below_root(
        expected_manifest, root=media_root, label="expected image manifest"
    )
    if manifest_evidence.path.resolve(strict=True) != expected_manifest.resolve(
        strict=True
    ):
        raise MediaProcessingError("image manifest path changed before OCR commit")
    _assert_private_file_evidence_current(
        manifest_evidence, label="image manifest"
    )
    current_frames = _validate_current_image_manifest_source(
        connection,
        content_id=content_id,
        content=content,
        manifest_path=manifest_evidence.path,
        manifest_body=manifest_body,
        download_binding_sha256=download_binding_sha256,
        frozen_image_groups=frozen_image_groups,
    )
    if len(current_frames) != len(expected_frames):
        raise MediaProcessingError("image frame set changed before OCR commit")
    for expected, current in zip(expected_frames, current_frames, strict=True):
        if (
            current.path,
            current.device,
            current.inode,
            current.byte_size,
            current.mtime_ns,
            current.ctime_ns,
            current.sha256,
        ) != (
            expected.path,
            expected.device,
            expected.inode,
            expected.byte_size,
            expected.mtime_ns,
            expected.ctime_ns,
            expected.sha256,
        ):
            raise MediaProcessingError("image frame evidence changed before OCR commit")
    return [frame.path for frame in current_frames]


def process_image_evidence(
    content_id: int,
    manifest_path: Path,
    *,
    db_path: Path = DEFAULT_DB,
    media_root: Optional[Path] = None,
    ocr_binary: Optional[Path] = None,
    frozen_image_groups: Optional[Iterable[Mapping[str, Any]]] = None,
) -> Dict[str, Artifact]:
    effective_media_root = media_root if media_root is not None else MEDIA_ROOT
    if frozen_image_groups is None:
        with connect(db_path) as connection:
            platform_row = connection.execute(
                "SELECT platform FROM content_items WHERE id=?", (content_id,)
            ).fetchone()
        if platform_row is not None and platform_row["platform"] == "douyin":
            raise MediaProcessingError(
                "Douyin image manifest requires frozen discovery groups"
            )
    lexical_manifest = (
        manifest_path if manifest_path.is_absolute() else PROJECT_ROOT / manifest_path
    )
    _require_no_symlink_below_root(
        lexical_manifest,
        root=effective_media_root,
        label="image manifest",
    )
    if lexical_manifest.is_symlink():
        raise MediaProcessingError("image manifest must not be a symlink")
    try:
        resolved_manifest = lexical_manifest.resolve(strict=True)
    except FileNotFoundError as exc:
        raise MediaProcessingError(
            f"image manifest does not exist: {manifest_path}"
        ) from exc
    manifest_evidence = _read_private_file_evidence(
        resolved_manifest, label="image manifest", capture_body=True
    )
    manifest_sha256 = manifest_evidence.sha256
    manifest_local_path = _relative(resolved_manifest)
    try:
        body = json.loads(manifest_evidence.body or b"")
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MediaProcessingError("image manifest body is invalid") from exc
    if not isinstance(body, dict):
        raise MediaProcessingError("image manifest body is invalid")
    versions = processor_versions()
    with connect(db_path) as connection, transaction(connection):
        row = connection.execute(
            """
            SELECT * FROM evidence_artifacts
            WHERE content_id=? AND artifact_type='media_manifest' AND local_path=?
            """,
            (content_id, manifest_local_path),
        ).fetchone()
        if row is None:
            raise MediaProcessingError(
                "unregistered or legacy image manifest requires explicit migration"
            )
        try:
            artifact_metadata = json.loads(str(row["metadata_json"] or ""))
        except json.JSONDecodeError as exc:
            raise MediaProcessingError("image manifest artifact metadata is invalid") from exc
        source_count = body.get("source_count")
        source_url_count = body.get("source_url_count")
        groups = body.get("groups")
        frames = body.get("frames")
        image_paths = body.get("image_paths")
        expected_metadata_keys = {
            "source_count",
            "source_url_count",
            "source_sha256",
            "image_groups_sha256",
            "download_binding_sha256",
        }
        if (
            str(row["status"] or "") != "available"
            or str(row["processor_version"] or "") != IMAGE_DOWNLOAD_VERSION
            or type(row["byte_size"]) is not int
            or row["byte_size"] != manifest_evidence.byte_size
            or str(row["sha256"] or "") != manifest_sha256
            or not isinstance(artifact_metadata, dict)
            or type(source_count) is not int
            or source_count <= 0
            or type(source_url_count) is not int
            or source_url_count < source_count
            or body.get("schema_version") != IMAGE_MANIFEST_VERSION
            or body.get("status") != "complete"
            or not isinstance(groups, list)
            or len(groups) != source_count
            or not isinstance(frames, list)
            or len(frames) != source_count
            or not isinstance(image_paths, list)
            or len(image_paths) != source_count
        ):
            raise MediaProcessingError(
                "image manifest artifact metadata or file evidence drifted"
            )
        content = connection.execute(
            "SELECT link_id,platform,content_type FROM content_items WHERE id=?",
            (content_id,),
        ).fetchone()
        if content is None:
            raise MediaProcessingError(f"unknown content {content_id}")
        slots = connection.execute(
            """
            SELECT * FROM media_processing_slots
            WHERE content_id=? AND processor_type='download'
              AND processor_version=? AND output_artifact_id=?
            """,
            (content_id, IMAGE_DOWNLOAD_VERSION, int(row["id"])),
        ).fetchall()
        source_row = connection.execute(
            """
            SELECT sha256 FROM evidence_artifacts
            WHERE content_id=? AND artifact_type='media_source'
              AND status='available' AND processor_version=?
            ORDER BY id DESC LIMIT 1
            """,
            (content_id, MEDIA_SOURCE_VERSION),
        ).fetchone()
        current_slots = [
            slot
            for slot in slots
            if source_row is not None and slot["source_sha256"] == source_row["sha256"]
        ]
        if (
            source_row is None
            or not _valid_sha256(source_row["sha256"])
            or len(current_slots) != 1
            or str(current_slots[0]["status"] or "") != "succeeded"
            or int(current_slots[0]["attempt_count"] or 0) <= 0
        ):
            raise MediaProcessingError(
                "image manifest is not bound to a succeeded current source download slot"
            )
        slot_source_sha256 = str(current_slots[0]["source_sha256"])
        download_binding_sha256 = str(body.get("download_binding_sha256") or "")
        link_id = str(content["link_id"] or "")
        if not link_id or Path(link_id).name != link_id or link_id in {".", ".."}:
            raise MediaProcessingError("image content link_id is not a safe basename")
        expected_manifest = (
            effective_media_root
            / link_id
            / "downloads"
            / download_binding_sha256
            / "images"
            / "manifest.json"
        )
        _require_no_symlink_below_root(
            lexical_manifest,
            root=effective_media_root,
            label="image manifest",
        )
        _require_no_symlink_below_root(
            expected_manifest,
            root=effective_media_root,
            label="expected image manifest",
        )
        if resolved_manifest != expected_manifest.resolve():
            raise MediaProcessingError(
                "image manifest path is not bound to download slot"
            )
        expected_metadata = {
            "source_count": source_count,
            "source_url_count": source_url_count,
            "source_sha256": body.get("source_sha256"),
            "image_groups_sha256": body.get("image_groups_sha256"),
            "download_binding_sha256": download_binding_sha256,
        }
        metadata_is_current = (
            set(artifact_metadata) == expected_metadata_keys
            and _canonical_json_bytes(artifact_metadata)
            == _canonical_json_bytes(expected_metadata)
            and _valid_sha256(body.get("source_sha256"))
            and _valid_sha256(body.get("image_groups_sha256"))
            and _valid_sha256(body.get("download_binding_sha256"))
            and body.get("download_binding_sha256") == download_binding_sha256
            and len(download_binding_sha256) == 64
            and all(
                character in "0123456789abcdef"
                for character in download_binding_sha256
            )
        )
        if not metadata_is_current:
            raise MediaProcessingError(
                "image manifest artifact metadata or file evidence drifted"
            )
        frame_evidence = _validate_current_image_manifest_source(
            connection,
            content_id=content_id,
            content=content,
            manifest_path=resolved_manifest,
            manifest_body=body,
            download_binding_sha256=download_binding_sha256,
            frozen_image_groups=frozen_image_groups,
        )
        media_manifest = Artifact(
            id=int(row["id"]),
            content_id=content_id,
            artifact_type="media_manifest",
            local_path=manifest_local_path,
            sha256=manifest_sha256,
            processor_version=IMAGE_DOWNLOAD_VERSION,
        )
    target = effective_media_root / str(content["link_id"]) / "ocr.json"

    def revalidate(connection: sqlite3.Connection) -> List[Path]:
        return _revalidate_image_ocr_inputs(
            connection,
            content_id=content_id,
            manifest_artifact_id=media_manifest.id,
            manifest_evidence=manifest_evidence,
            manifest_body=body,
            artifact_metadata=artifact_metadata,
            slot_source_sha256=slot_source_sha256,
            download_binding_sha256=download_binding_sha256,
            frozen_image_groups=frozen_image_groups,
            expected_frames=frame_evidence,
            media_root=effective_media_root,
        )

    def produce_ocr() -> Path:
        with connect(db_path) as connection, transaction(connection):
            validated_frame_paths = revalidate(connection)
        return _run_ocr(
            resolved_manifest,
            target,
            binary_path=ocr_binary,
            validated_frame_paths=validated_frame_paths,
        )

    def revalidate_without_result(connection: sqlite3.Connection) -> None:
        revalidate(connection)

    def validate_cached_ocr(
        _connection: sqlite3.Connection,
        _artifact: Artifact,
        evidence: _PrivateFileEvidence,
    ) -> None:
        try:
            cached_body = json.loads(evidence.body or b"")
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MediaProcessingError("cached OCR output body is invalid") from exc
        _validate_ocr_output_body(
            cached_body,
            expected_source_count=len(frame_evidence),
        )

    ocr = _run_processing_slot(
        db_path=db_path,
        content_id=content_id,
        source_sha256=media_manifest.sha256,
        processor_type="ocr",
        processor_version=versions["ocr"],
        artifact_type="ocr",
        produce=produce_ocr,
        claim_validator=revalidate_without_result,
        commit_validator=revalidate_without_result,
        expected_output_path=target,
        expected_output_root=effective_media_root,
        cached_validator=validate_cached_ocr,
    )
    return {"media": media_manifest, "ocr": ocr}


def _latest_media_source_artifact(
    content_id: int, *, db_path: Path
) -> Optional[Dict[str, Any]]:
    """Return the latest source ledger row without opening its local file."""

    with connect(db_path) as connection:
        row = connection.execute(
            """
            SELECT * FROM evidence_artifacts
            WHERE content_id=? AND artifact_type='media_source' AND status='available'
            ORDER BY id DESC LIMIT 1
            """,
            (content_id,),
        ).fetchone()
    return dict(row) if row is not None else None


def _latest_media_source(content_id: int, *, db_path: Path) -> Optional[Dict[str, Any]]:
    artifact = _latest_media_source_artifact(content_id, db_path=db_path)
    if artifact is None:
        return None
    value = _read_json_object(_resolved(str(artifact["local_path"])))
    media_kind_value = value.get("media_kind")
    if type(media_kind_value) is not str:
        return None
    media_kind = media_kind_value
    raw_urls = value.get("urls")
    if not isinstance(raw_urls, list) or any(
        type(item) is not str for item in raw_urls
    ):
        return None
    try:
        urls, source_sha256 = _media_source_identity(media_kind, raw_urls)
    except MediaProcessingError:
        return None
    if not urls or urls != raw_urls:
        return None
    return {
        **value,
        "media_kind": media_kind,
        "urls": urls,
        "source_sha256": source_sha256,
        "source_artifact_id": int(artifact["id"]),
        "source_artifact_sha256": str(artifact["sha256"]),
        "source_processor_version": str(artifact["processor_version"]),
    }


def _mark_claimed_download_failed(
    *, db_path: Path, slot_id: int, error: BaseException
) -> None:
    with connect(db_path) as connection, transaction(connection):
        connection.execute(
            """
            UPDATE media_processing_slots
            SET status=CASE WHEN attempt_count>=? THEN 'terminal_failed'
                            ELSE 'retryable_failed' END,
                error_message=?,updated_at=?
            WHERE id=? AND processor_type='download' AND status='running'
              AND output_artifact_id IS NULL
            """,
            (
                MAX_MEDIA_DOWNLOAD_ATTEMPTS,
                f"{type(error).__name__}: {error}"[:500],
                now_utc(),
                slot_id,
            ),
        )


def _claim_current_source_download(
    *, content_id: int, source: Mapping[str, Any], db_path: Path
) -> tuple[int, Optional[Artifact]]:
    artifact_sha256 = source.get("sha256")
    if not _valid_sha256(artifact_sha256):
        raise MediaProcessingError("current media source ledger sha256 is invalid")
    if source.get("processor_version") != MEDIA_SOURCE_VERSION:
        raise MediaProcessingError("download is not bound to current source")
    with connect(db_path) as connection, transaction(connection):
        current = connection.execute(
            """
            SELECT id,sha256,processor_version FROM evidence_artifacts
            WHERE content_id=? AND artifact_type='media_source' AND status='available'
            ORDER BY id DESC LIMIT 1
            """,
            (content_id,),
        ).fetchone()
        content = connection.execute(
            "SELECT content_type FROM content_items WHERE id=?", (content_id,)
        ).fetchone()
        if (
            current is None
            or current["id"] != source.get("id")
            or current["sha256"] != artifact_sha256
            or current["processor_version"] != MEDIA_SOURCE_VERSION
            or content is None
            or content["content_type"] not in {"video", "image"}
        ):
            raise MediaProcessingError("current media source ledger identity changed")
        processor_version = (
            VIDEO_DOWNLOAD_VERSION
            if content["content_type"] == "video"
            else IMAGE_DOWNLOAD_VERSION
        )
        return _claim_processing_slot(
            connection,
            content_id=content_id,
            source_sha256=str(artifact_sha256),
            processor_type="download",
            processor_version=processor_version,
        )


def _legacy_download_succeeded(
    *,
    content_id: int,
    media_kind: str,
    urls: List[str],
    platform: str,
    frozen_image_groups: Optional[Iterable[Mapping[str, Any]]],
    db_path: Path,
) -> bool:
    logical_sha256 = _media_source_identity(media_kind, urls)[1]
    if media_kind == "image":
        groups = (
            image_source_groups(urls, platform=platform)
            if frozen_image_groups is None
            else validate_frozen_image_groups(
                urls, frozen_image_groups, platform=platform
            )
        )
        identities = [
            image_download_binding_sha256(
                logical_sha256, image_groups_sha256(groups)
            )
        ]
        versions = [IMAGE_DOWNLOAD_VERSION]
    else:
        identities = [logical_sha256, _legacy_media_source_sha256(urls)]
        versions = sorted(_download_versions("video"))
    identity_placeholders = ",".join("?" for _ in identities)
    version_placeholders = ",".join("?" for _ in versions)
    with connect(db_path) as connection:
        row = connection.execute(
            f"""
            SELECT 1 FROM media_processing_slots
            WHERE content_id=? AND processor_type='download'
              AND source_sha256 IN ({identity_placeholders})
              AND processor_version IN ({version_placeholders})
              AND status='succeeded' AND output_artifact_id IS NOT NULL
            LIMIT 1
            """,
            (content_id, *identities, *versions),
        ).fetchone()
    return row is not None


def _complete_claimed_download_from_artifact(
    *, db_path: Path, slot_id: int, source_artifact: Mapping[str, Any], artifact: Artifact
) -> None:
    with connect(db_path) as connection, transaction(connection):
        current = connection.execute(
            """
            SELECT id,sha256,processor_version FROM evidence_artifacts
            WHERE content_id=? AND artifact_type='media_source' AND status='available'
            ORDER BY id DESC LIMIT 1
            """,
            (artifact.content_id,),
        ).fetchone()
        if (
            current is None
            or current["id"] != source_artifact.get("id")
            or current["sha256"] != source_artifact.get("sha256")
            or current["processor_version"] != MEDIA_SOURCE_VERSION
        ):
            raise MediaProcessingError("media source changed during slot migration")
        cursor = connection.execute(
            """
            UPDATE media_processing_slots
            SET status='succeeded',output_artifact_id=?,error_message=NULL,updated_at=?
            WHERE id=? AND content_id=? AND source_sha256=?
              AND processor_type='download' AND status='running'
              AND output_artifact_id IS NULL
            """,
            (
                artifact.id,
                now_utc(),
                slot_id,
                artifact.content_id,
                source_artifact["sha256"],
            ),
        )
        if cursor.rowcount != 1:
            raise MediaProcessingError("claimed download slot changed during migration")


def _matching_download_slots(
    connection: sqlite3.Connection,
    *,
    content_id: int,
    source: Dict[str, Any],
) -> List[sqlite3.Row]:
    media_kind = str(source["media_kind"])
    content = connection.execute(
        "SELECT content_type FROM content_items WHERE id=?", (content_id,)
    ).fetchone()
    artifact_sha256 = source.get("source_artifact_sha256")
    if (
        content is None
        or content["content_type"] != media_kind
        or not _valid_sha256(artifact_sha256)
        or source.get("source_processor_version") != MEDIA_SOURCE_VERSION
    ):
        return []
    processor_version = (
        VIDEO_DOWNLOAD_VERSION if media_kind == "video" else IMAGE_DOWNLOAD_VERSION
    )
    return connection.execute(
        """
        SELECT * FROM media_processing_slots
        WHERE content_id=? AND processor_type='download'
          AND source_sha256=? AND processor_version=?
        ORDER BY id DESC
        """,
        (content_id, artifact_sha256, processor_version),
    ).fetchall()


def _effective_download_slot(rows: Iterable[sqlite3.Row]) -> Optional[sqlite3.Row]:
    values = list(rows)
    return max(values, key=lambda row: int(row["id"])) if values else None


def get_media_source_state(
    content_id: int, *, db_path: Path = DEFAULT_DB
) -> Optional[Dict[str, Any]]:
    """Return the latest normalized source and its effective same-source download slot."""

    source = _latest_media_source(content_id, db_path=db_path)
    if source is None:
        return None
    with connect(db_path) as connection:
        content = connection.execute(
            "SELECT content_type FROM content_items WHERE id=?", (content_id,)
        ).fetchone()
        if (
            content is None
            or type(content["content_type"]) is not str
            or content["content_type"] != source["media_kind"]
        ):
            return None
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
        "source_artifact_sha256": str(source["source_artifact_sha256"]),
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
    media_root: Optional[Path] = None,
    whisper_model_path: Optional[Path] = None,
    ocr_binary: Optional[Path] = None,
    urlopen_fn: Optional[Callable[..., Any]] = None,
    maximum_download_bytes: Optional[int] = None,
    require_exact_response_url: bool = False,
    download_urls: Optional[Iterable[str]] = None,
    frozen_image_groups: Optional[Iterable[Mapping[str, Any]]] = None,
    reuse_existing_downloads: bool = True,
    maximum_video_duration_seconds: Optional[float] = None,
) -> Dict[str, Any]:
    source_artifact = _latest_media_source_artifact(content_id, db_path=db_path)
    if source_artifact is None:
        existing = _existing_complete_evidence(content_id, db_path=db_path)
        if existing is not None:
            return {
                "content_id": content_id,
                "status": "evidence_ready",
                "source": "existing_local_evidence",
                "artifacts": existing,
            }
        return {"content_id": content_id, "status": "no_source"}
    if source_artifact.get("processor_version") != MEDIA_SOURCE_VERSION:
        return {
            "content_id": content_id,
            "status": "legacy_source_skipped",
            "source_processor_version": str(
                source_artifact.get("processor_version") or ""
            ),
        }
    with connect(db_path) as connection:
        content = connection.execute(
            "SELECT content_type,platform FROM content_items WHERE id=?",
            (content_id,),
        ).fetchone()
    if (
        content is None
        or type(content["content_type"]) is not str
        or type(content["platform"]) is not str
        or content["content_type"] not in {"video", "image"}
    ):
        raise MediaProcessingError(
            "current media source kind does not match content type"
        )
    media_kind = str(content["content_type"])
    try:
        source_metadata = json.loads(str(source_artifact.get("metadata_json") or ""))
    except json.JSONDecodeError as exc:
        raise MediaProcessingError("current media source metadata is invalid") from exc
    if (
        not isinstance(source_metadata, dict)
        or source_metadata.get("media_kind") != media_kind
    ):
        raise MediaProcessingError(
            "current media source kind does not match content type"
        )
    slot_id, cached_media = _claim_current_source_download(
        content_id=content_id,
        source=source_artifact,
        db_path=db_path,
    )
    if cached_media is not None:
        media = cached_media
    else:
        try:
            with connect(db_path) as connection:
                source_row = connection.execute(
                    "SELECT * FROM evidence_artifacts WHERE id=?",
                    (source_artifact["id"],),
                ).fetchone()
            if source_row is None:
                raise MediaProcessingError("claimed media source disappeared")
            urls, _logical_source_sha256 = _validated_media_source(
                source_row, expected_media_kind=media_kind
            )
            effective_groups = frozen_image_groups
            if (
                media_kind == "image"
                and effective_groups is None
                and content["platform"] == "douyin"
            ):
                raise MediaProcessingError(
                    "Douyin image processing requires frozen discovery groups"
                )
            reuse_legacy = _legacy_download_succeeded(
                content_id=content_id,
                media_kind=media_kind,
                urls=urls,
                platform=str(content["platform"]),
                frozen_image_groups=effective_groups,
                db_path=db_path,
            )
            if media_kind == "video":
                media = download_video_sources(
                    content_id,
                    urls,
                    db_path=db_path,
                    media_root=media_root,
                    urlopen_fn=urlopen_fn,
                    maximum_bytes=maximum_download_bytes,
                    require_exact_response_url=require_exact_response_url,
                    download_urls=download_urls,
                    reuse_existing=reuse_existing_downloads,
                    maximum_duration_seconds=maximum_video_duration_seconds,
                    _slot_source_sha256=(
                        _logical_source_sha256
                        if reuse_legacy
                        else str(source_artifact["sha256"])
                    ),
                    _preclaimed_slot_id=None if reuse_legacy else slot_id,
                )
            else:
                media = download_image_sources(
                    content_id,
                    urls,
                    db_path=db_path,
                    media_root=media_root,
                    urlopen_fn=urlopen_fn,
                    maximum_bytes=maximum_download_bytes,
                    require_exact_response_url=require_exact_response_url,
                    download_urls=download_urls,
                    frozen_image_groups=effective_groups,
                    reuse_existing=reuse_existing_downloads,
                    _slot_source_sha256=(
                        image_download_binding_sha256(
                            _logical_source_sha256,
                            image_groups_sha256(
                                validate_frozen_image_groups(
                                    urls,
                                    effective_groups
                                    or image_source_groups(
                                        urls, platform=str(content["platform"])
                                    ),
                                    platform=str(content["platform"]),
                                )
                            ),
                        )
                        if reuse_legacy
                        else str(source_artifact["sha256"])
                    ),
                    _preclaimed_slot_id=None if reuse_legacy else slot_id,
                )
            if reuse_legacy:
                _complete_claimed_download_from_artifact(
                    db_path=db_path,
                    slot_id=slot_id,
                    source_artifact=source_artifact,
                    artifact=media,
                )
        except Exception as exc:
            _mark_claimed_download_failed(db_path=db_path, slot_id=slot_id, error=exc)
            raise
    if download_only:
        return {
            "content_id": content_id,
            "status": "downloaded",
            "media_kind": media_kind,
            "artifact_id": media.id,
        }
    if media_kind == "video":
        artifacts = process_video_evidence(
            content_id,
            _resolved(media.local_path),
            db_path=db_path,
            media_root=media_root,
            whisper_model_path=whisper_model_path,
            ocr_binary=ocr_binary,
            maximum_duration_seconds=maximum_video_duration_seconds,
        )
    elif media_kind == "image":
        effective_groups = frozen_image_groups
        if effective_groups is None:
            if content["platform"] == "douyin":
                raise MediaProcessingError(
                    "Douyin image processing requires frozen discovery groups"
                )
        artifacts = process_image_evidence(
            content_id,
            _resolved(media.local_path),
            db_path=db_path,
            media_root=media_root,
            ocr_binary=ocr_binary,
            frozen_image_groups=effective_groups,
        )
    else:
        raise MediaProcessingError(f"invalid media source kind for content {content_id}")
    return {
        "content_id": content_id,
        "status": "evidence_ready",
        "media_kind": media_kind,
        "artifacts": {name: artifact.id for name, artifact in artifacts.items()},
    }


def _empty_stale_recovery_counts() -> Dict[str, int]:
    return {
        "stale_candidates": 0,
        "recovered": 0,
        "retryable_failed": 0,
        "terminal_failed": 0,
        "cas_conflicts": 0,
        "exhausted_normalized": 0,
    }


def _validated_recovery_media_source(
    connection: sqlite3.Connection,
    *,
    content_id: int,
) -> tuple[sqlite3.Row, Dict[str, Any], List[str], str]:
    rows = connection.execute(
        """
        SELECT * FROM evidence_artifacts
        WHERE content_id=? AND artifact_type='media_source' AND status='available'
        ORDER BY id DESC
        """,
        (content_id,),
    ).fetchall()
    if not rows:
        raise MediaProcessingError("current media source is missing")
    row = rows[0]
    path_value = row["local_path"]
    if type(path_value) is not str:
        raise MediaProcessingError("current media source path drifted")
    evidence = _read_private_file_evidence(
        _resolved(path_value), label="current recovery media source", capture_body=True
    )
    try:
        body = json.loads(evidence.body or b"")
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MediaProcessingError("current media source body is invalid") from exc
    if not isinstance(body, dict):
        raise MediaProcessingError("current media source body is invalid")
    media_kind = body.get("media_kind")
    raw_urls = body.get("urls")
    if (
        type(media_kind) is not str
        or media_kind not in {"video", "image"}
        or not isinstance(raw_urls, list)
        or any(type(value) is not str for value in raw_urls)
    ):
        raise MediaProcessingError("current media source shape drifted")
    urls, source_sha256 = _media_source_identity(media_kind, raw_urls)
    expected_metadata = {
        "media_kind": media_kind,
        "source_count": len(urls),
        "source_sha256": source_sha256,
        "raw_response_id": body.get("raw_response_id"),
    }
    if (
        set(body)
        != {
            "schema_version",
            "media_kind",
            "urls",
            "source_sha256",
            "raw_response_id",
            "captured_at",
        }
        or body.get("schema_version") != MEDIA_SOURCE_VERSION
        or raw_urls != urls
        or body.get("source_sha256") != source_sha256
        or type(body.get("raw_response_id")) is not int
        or body["raw_response_id"] <= 0
        or type(body.get("captured_at")) is not str
        or not body["captured_at"]
        or row["processor_version"] != MEDIA_SOURCE_VERSION
        or type(row["byte_size"]) is not int
        or row["byte_size"] != evidence.byte_size
        or row["sha256"] != evidence.sha256
        or row["metadata_json"]
        != json.dumps(expected_metadata, ensure_ascii=False, sort_keys=True)
    ):
        raise MediaProcessingError("current media source evidence drifted")
    return (
        row,
        {
            **body,
            "source_artifact_id": int(row["id"]),
            "source_artifact_sha256": str(row["sha256"]),
            "source_processor_version": str(row["processor_version"]),
        },
        urls,
        source_sha256,
    )


def _validated_recovery_artifact(
    connection: sqlite3.Connection,
    *,
    artifact_id: int,
    content_id: int,
    artifact_type: str,
    processor_version: str,
    expected_path: Path,
    expected_root: Path,
    expected_metadata: Mapping[str, Any],
) -> Optional[Artifact]:
    row = connection.execute(
        "SELECT * FROM evidence_artifacts WHERE id=?", (artifact_id,)
    ).fetchone()
    if row is None:
        return None
    try:
        _require_no_symlink_below_root(
            expected_path, root=expected_root, label=f"current {artifact_type} artifact"
        )
        evidence = _read_private_file_evidence(
            expected_path,
            label=f"current {artifact_type} artifact",
            capture_body=artifact_type in {"frames_manifest", "media_manifest"},
        )
    except MediaProcessingError:
        return None
    if (
        row["content_id"] != content_id
        or row["artifact_type"] != artifact_type
        or row["status"] != "available"
        or row["local_path"] != _relative(expected_path)
        or row["processor_version"] != processor_version
        or type(row["byte_size"]) is not int
        or row["byte_size"] != evidence.byte_size
        or row["sha256"] != evidence.sha256
        or row["metadata_json"]
        != json.dumps(dict(expected_metadata), ensure_ascii=False, sort_keys=True)
    ):
        return None
    if evidence.body is not None:
        try:
            body = json.loads(evidence.body)
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
        if not isinstance(body, dict):
            return None
    return Artifact(
        id=int(row["id"]),
        content_id=content_id,
        artifact_type=artifact_type,
        local_path=str(row["local_path"]),
        sha256=str(row["sha256"]),
        processor_version=str(row["processor_version"]),
    )


def _current_recovery_download(
    connection: sqlite3.Connection,
    *,
    content_id: int,
    require_succeeded: bool,
) -> Optional[tuple[str, str, Optional[Artifact]]]:
    content = connection.execute(
        "SELECT link_id,platform,content_type FROM content_items WHERE id=?",
        (content_id,),
    ).fetchone()
    if content is None:
        return None
    try:
        source_row, body, urls, flat_source_sha256 = _validated_recovery_media_source(
            connection, content_id=content_id
        )
        link_id = _validated_link_id(content["link_id"])
    except MediaProcessingError:
        return None
    media_kind = body["media_kind"]
    platform = str(content["platform"] or "")
    if media_kind != str(content["content_type"] or ""):
        return None
    groups: Optional[List[Dict[str, Any]]] = None
    if media_kind == "image":
        if platform == "douyin":
            return None
        groups = image_source_groups(urls, platform=platform)
        groups_sha256 = image_groups_sha256(groups)
        logical_source_sha256 = image_download_binding_sha256(
            flat_source_sha256, groups_sha256
        )
        processor_version = IMAGE_DOWNLOAD_VERSION
    else:
        logical_source_sha256 = flat_source_sha256
        processor_version = VIDEO_DOWNLOAD_VERSION
    source_artifact_sha256 = str(source_row["sha256"] or "")
    if not _valid_sha256(source_artifact_sha256):
        return None
    if not require_succeeded:
        return source_artifact_sha256, processor_version, None
    slots = connection.execute(
        """
        SELECT * FROM media_processing_slots
        WHERE content_id=? AND source_sha256=? AND processor_type='download'
          AND processor_version=?
        """,
        (content_id, source_artifact_sha256, processor_version),
    ).fetchall()
    if (
        len(slots) != 1
        or slots[0]["status"] != "succeeded"
        or type(slots[0]["output_artifact_id"]) is not int
        or type(slots[0]["attempt_count"]) is not int
        or slots[0]["attempt_count"] <= 0
    ):
        return None
    output_artifact_id = slots[0]["output_artifact_id"]
    if media_kind == "video":
        artifact = _validated_recovery_artifact(
            connection,
            artifact_id=output_artifact_id,
            content_id=content_id,
            artifact_type="media",
            processor_version=VIDEO_DOWNLOAD_VERSION,
            expected_path=(
                MEDIA_ROOT
                / link_id
                / "downloads"
                / logical_source_sha256
                / "source.mp4"
            ),
            expected_root=MEDIA_ROOT,
            expected_metadata={
                "source_count": len(urls),
                "source_sha256": flat_source_sha256,
            },
        )
    else:
        expected_manifest = (
            MEDIA_ROOT
            / link_id
            / "downloads"
            / logical_source_sha256
            / "images"
            / "manifest.json"
        )
        artifact_row = connection.execute(
            "SELECT * FROM evidence_artifacts WHERE id=?", (output_artifact_id,)
        ).fetchone()
        if artifact_row is None or artifact_row["local_path"] != _relative(
            expected_manifest
        ):
            return None
        try:
            _require_no_symlink_below_root(
                expected_manifest,
                root=MEDIA_ROOT,
                label="current image download manifest",
            )
            manifest_evidence = _read_private_file_evidence(
                expected_manifest,
                label="current image download manifest",
                capture_body=True,
            )
            artifact = Artifact(
                id=int(artifact_row["id"]),
                content_id=content_id,
                artifact_type="media_manifest",
                local_path=str(artifact_row["local_path"]),
                sha256=str(artifact_row["sha256"]),
                processor_version=str(artifact_row["processor_version"]),
            )
            _validate_cached_image_download(
                connection,
                artifact=artifact,
                manifest_evidence=manifest_evidence,
                content_id=content_id,
                source_urls=urls,
                platform=platform,
                frozen_image_groups=groups or [],
                expected_manifest=expected_manifest,
                expected_metadata={
                    "source_count": len(groups or []),
                    "source_url_count": len(urls),
                    "source_sha256": flat_source_sha256,
                    "image_groups_sha256": image_groups_sha256(groups or []),
                    "download_binding_sha256": logical_source_sha256,
                },
            )
        except MediaProcessingError:
            return None
    if artifact is None:
        return None
    return source_artifact_sha256, processor_version, artifact


def _current_recovery_frames_artifact(
    connection: sqlite3.Connection,
    *,
    content_id: int,
    media_artifact: Artifact,
) -> Optional[Artifact]:
    version = processor_versions()["frames"]
    slot = connection.execute(
        """
        SELECT * FROM media_processing_slots
        WHERE content_id=? AND source_sha256=? AND processor_type='frames'
          AND processor_version=?
        """,
        (content_id, media_artifact.sha256, version),
    ).fetchone()
    if (
        slot is None
        or slot["status"] != "succeeded"
        or type(slot["output_artifact_id"]) is not int
        or type(slot["attempt_count"]) is not int
        or slot["attempt_count"] <= 0
    ):
        return None
    content = connection.execute(
        "SELECT link_id FROM content_items WHERE id=?", (content_id,)
    ).fetchone()
    if content is None:
        return None
    try:
        link_id = _validated_link_id(content["link_id"])
    except MediaProcessingError:
        return None
    return _validated_recovery_artifact(
        connection,
        artifact_id=slot["output_artifact_id"],
        content_id=content_id,
        artifact_type="frames_manifest",
        processor_version=version,
        expected_path=MEDIA_ROOT / link_id / "frames" / "frames.json",
        expected_root=MEDIA_ROOT,
        expected_metadata={},
    )


def _generic_recovery_slot_allowed(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    processor_version_by_type: Optional[Dict[str, str]],
) -> bool:
    processor_type = str(row["processor_type"] or "")
    expected_version = (
        processor_version_by_type.get(processor_type)
        if processor_version_by_type is not None
        else None
    )
    if expected_version is not None and row["processor_version"] != expected_version:
        return False
    content = connection.execute(
        "SELECT platform,content_type FROM content_items WHERE id=?",
        (row["content_id"],),
    ).fetchone()
    if content is None:
        return False
    if content["platform"] == "douyin" and content["content_type"] == "image":
        return False
    current_download = _current_recovery_download(
        connection,
        content_id=int(row["content_id"]),
        require_succeeded=processor_type != "download",
    )
    if current_download is None:
        return False
    download_source_sha256, download_version, download_artifact = current_download
    if processor_type == "download":
        return (
            row["source_sha256"] == download_source_sha256
            and row["processor_version"] == download_version
        )
    if expected_version is None or download_artifact is None:
        return False
    if processor_type in {"frames", "asr"}:
        return row["source_sha256"] == download_artifact.sha256
    if processor_type == "ocr":
        if content["content_type"] == "image":
            return row["source_sha256"] == download_artifact.sha256
        frames_artifact = _current_recovery_frames_artifact(
            connection,
            content_id=int(row["content_id"]),
            media_artifact=download_artifact,
        )
        return (
            frames_artifact is not None
            and row["source_sha256"] == frames_artifact.sha256
        )
    return False


def recover_stale_media_processing_slots(
    *,
    db_path: Path = DEFAULT_DB,
    processor_types: Optional[Iterable[str]] = None,
    processor_version_by_type: Optional[Dict[str, str]] = None,
    stale_after_seconds: int = STALE_MEDIA_SLOT_SECONDS,
    content_ids: Optional[Iterable[int]] = None,
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
    counts = _empty_stale_recovery_counts()
    if not selected_types:
        return counts
    scoped_content_ids = None if content_ids is None else list(dict.fromkeys(content_ids))
    if scoped_content_ids == []:
        return counts
    placeholders = ",".join("?" for _ in selected_types)
    ordered_types = sorted(selected_types)
    content_clause = ""
    content_parameters: List[Any] = []
    if scoped_content_ids is not None:
        content_placeholders = ",".join("?" for _ in scoped_content_ids)
        content_clause = f" AND content_id IN ({content_placeholders})"
        content_parameters = list(scoped_content_ids)
    with connect(db_path) as connection, transaction(connection):
        stale_rows = connection.execute(
            f"""
            SELECT id,content_id,source_sha256,processor_type,processor_version,
                   status,attempt_count,updated_at,output_artifact_id
            FROM media_processing_slots
            WHERE status='running' AND updated_at<=?
              AND processor_type IN ({placeholders})
              {content_clause}
            ORDER BY id
            """,
            (cutoff, *ordered_types, *content_parameters),
        ).fetchall()
        counts["stale_candidates"] = len(stale_rows)
        for row in stale_rows:
            if (
                row["output_artifact_id"] is not None
                or not _generic_recovery_slot_allowed(
                    connection,
                    row,
                    processor_version_by_type=processor_version_by_type,
                )
            ):
                continue
            status = (
                "terminal_failed"
                if int(row["attempt_count"]) >= MAX_MEDIA_PROCESSING_ATTEMPTS
                else "retryable_failed"
            )
            cursor = connection.execute(
                """
                UPDATE media_processing_slots
                SET status=?, error_message=?, updated_at=?
                WHERE id=? AND content_id=? AND source_sha256=?
                  AND processor_type=? AND processor_version=?
                  AND status='running' AND attempt_count=? AND updated_at=?
                  AND output_artifact_id IS NULL
                """,
                (
                    status,
                    f"stale running slot recovered after {stale_after_seconds} seconds",
                    recovered_at,
                    row["id"],
                    row["content_id"],
                    row["source_sha256"],
                    row["processor_type"],
                    row["processor_version"],
                    row["attempt_count"],
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
            SELECT id,content_id,source_sha256,processor_type,processor_version,
                   status,attempt_count,updated_at,output_artifact_id
            FROM media_processing_slots
            WHERE status='retryable_failed' AND attempt_count>=?
              AND processor_type IN ({placeholders})
              {content_clause}
            ORDER BY id
            """,
            (
                MAX_MEDIA_PROCESSING_ATTEMPTS,
                *ordered_types,
                *content_parameters,
            ),
        ).fetchall()
        for row in exhausted_rows:
            if (
                row["output_artifact_id"] is not None
                or not _generic_recovery_slot_allowed(
                    connection,
                    row,
                    processor_version_by_type=processor_version_by_type,
                )
            ):
                continue
            cursor = connection.execute(
                """
                UPDATE media_processing_slots
                SET status='terminal_failed',
                    error_message=COALESCE(error_message, 'attempt limit exhausted'),
                    updated_at=?
                WHERE id=? AND status='retryable_failed' AND updated_at=?
                  AND content_id=? AND source_sha256=?
                  AND processor_type=? AND processor_version=?
                  AND attempt_count=? AND output_artifact_id IS NULL
                """,
                (
                    recovered_at,
                    row["id"],
                    row["updated_at"],
                    row["content_id"],
                    row["source_sha256"],
                    row["processor_type"],
                    row["processor_version"],
                    row["attempt_count"],
                ),
            )
            if cursor.rowcount == 1:
                counts["exhausted_normalized"] += 1
            else:
                counts["cas_conflicts"] += 1
    return counts


def _download_slot_identity(
    source: Dict[str, Any], *, platform: str, content_type: str
) -> Optional[tuple[str, str]]:
    media_kind = str(source.get("media_kind") or "")
    raw_urls = source.get("urls", [])
    if (
        not isinstance(raw_urls, list)
        or any(type(value) is not str for value in raw_urls)
        or media_kind not in {"video", "image"}
        or type(content_type) is not str
        or media_kind != content_type
    ):
        return None
    values, source_sha256 = _media_source_identity(media_kind, raw_urls)
    if not values or values != raw_urls:
        return None
    processor_version = (
        VIDEO_DOWNLOAD_VERSION if media_kind == "video" else IMAGE_DOWNLOAD_VERSION
    )
    if media_kind == "image":
        if platform == "douyin":
            return None
        groups = image_source_groups(values, platform=platform)
        source_sha256 = image_download_binding_sha256(
            source_sha256, image_groups_sha256(groups)
        )
    return source_sha256, processor_version


def _queue_content_ids(
    *,
    stage: str,
    limit: int,
    db_path: Path,
    scope_content_ids: Optional[Iterable[int]] = None,
) -> List[int]:
    if stage not in {"download", "process"}:
        raise ValueError(f"unknown media queue stage: {stage}")
    scoped_ids = (
        None
        if scope_content_ids is None
        else list(dict.fromkeys(int(value) for value in scope_content_ids))
    )
    if scoped_ids == []:
        return []
    scope_clause = ""
    scope_parameters: List[Any] = []
    if scoped_ids is not None:
        scope_placeholders = ",".join("?" for _ in scoped_ids)
        scope_clause = f" WHERE c.id IN ({scope_placeholders})"
        scope_parameters = list(scoped_ids)
    with connect(db_path) as connection:
        rows = connection.execute(
            f"""
            SELECT c.id,c.content_type,c.platform,
                   source.id AS source_artifact_id,
                   source.sha256 AS source_artifact_sha256,
                   source.processor_version AS source_processor_version
            FROM content_items c
            JOIN evidence_artifacts source ON source.id=(
                SELECT current_source.id FROM evidence_artifacts current_source
                WHERE current_source.content_id=c.id
                  AND current_source.artifact_type='media_source'
                  AND current_source.status='available'
                ORDER BY current_source.id DESC LIMIT 1
            )
            {scope_clause}
            ORDER BY (c.published_at IS NULL) ASC, c.published_at DESC, c.id DESC
            """,
            scope_parameters,
        ).fetchall()
        slot_scope_clause = ""
        slot_scope_parameters: List[Any] = []
        if scoped_ids is not None:
            slot_scope_clause = (
                " WHERE content_id IN ("
                + ",".join("?" for _ in scoped_ids)
                + ")"
            )
            slot_scope_parameters = list(scoped_ids)
        slot_rows = connection.execute(
            f"""
            SELECT id,content_id,source_sha256,processor_type,processor_version,
                   status,attempt_count,output_artifact_id
            FROM media_processing_slots
            {slot_scope_clause}
            """,
            slot_scope_parameters,
        ).fetchall()
        artifact_scope_clause = ""
        artifact_scope_parameters: List[Any] = []
        if scoped_ids is not None:
            artifact_scope_clause = (
                " AND content_id IN ("
                + ",".join("?" for _ in scoped_ids)
                + ")"
            )
            artifact_scope_parameters = list(scoped_ids)
        artifact_rows = connection.execute(
            "SELECT id,sha256 FROM evidence_artifacts "
            f"WHERE status='available'{artifact_scope_clause}",
            artifact_scope_parameters,
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
        platform = str(row["platform"] or "")
        media_kind = str(row["content_type"] or "")
        source_sha256 = row["source_artifact_sha256"]
        if (
            media_kind not in {"video", "image"}
            or row["source_processor_version"] != MEDIA_SOURCE_VERSION
            or not _valid_sha256(source_sha256)
            or (media_kind == "image" and platform == "douyin")
        ):
            continue
        download_version = (
            VIDEO_DOWNLOAD_VERSION if media_kind == "video" else IMAGE_DOWNLOAD_VERSION
        )
        content_slots = slots_by_content.get(content_id, [])
        download_slots = [
            item
            for item in content_slots
            if item["processor_type"] == "download"
            and item["source_sha256"] == source_sha256
            and item["processor_version"] == download_version
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


def _queue_recovery_scope_content_ids(
    *,
    stage: str,
    limit: int,
    db_path: Path,
    scope_content_ids: Optional[Iterable[int]] = None,
) -> List[int]:
    processor_types = (
        ("download",) if stage == "download" else ("frames", "asr", "ocr")
    )
    placeholders = ",".join("?" for _ in processor_types)
    scoped_ids = (
        None
        if scope_content_ids is None
        else list(dict.fromkeys(int(value) for value in scope_content_ids))
    )
    if scoped_ids == []:
        return []
    scope_clause = ""
    scope_parameters: List[Any] = []
    if scoped_ids is not None:
        scope_clause = " AND c.id IN (" + ",".join("?" for _ in scoped_ids) + ")"
        scope_parameters = list(scoped_ids)
    with connect(db_path) as connection:
        rows = connection.execute(
            f"""
            SELECT DISTINCT c.id,c.published_at
            FROM content_items c
            JOIN media_processing_slots slot ON slot.content_id=c.id
            WHERE slot.processor_type IN ({placeholders})
              AND slot.status IN ('running','retryable_failed')
              AND EXISTS (
                  SELECT 1 FROM evidence_artifacts source
                  WHERE source.content_id=c.id
                    AND source.artifact_type='media_source'
                    AND source.status='available'
              )
              {scope_clause}
            ORDER BY (c.published_at IS NULL) ASC,c.published_at DESC,c.id DESC
            LIMIT ?
            """,
            (
                *processor_types,
                *scope_parameters,
                limit,
            ),
        ).fetchall()
    return [int(row["id"]) for row in rows]


def run_media_download_queue(
    *,
    limit: int = MEDIA_QUEUE_BATCH_LIMIT,
    max_workers: int = MEDIA_DOWNLOAD_WORKERS,
    db_path: Path = DEFAULT_DB,
    scope_content_ids: Optional[Iterable[int]] = None,
) -> Dict[str, Any]:
    if limit < 0:
        raise ValueError("media queue limit must be non-negative")
    if limit == 0:
        return {
            "candidates": 0,
            "downloaded": 0,
            "retryable_failed": 0,
            "terminal_failed": 0,
            "failed": 0,
            "truncated": False,
            "has_more": False,
            "stale_recovery": _empty_stale_recovery_counts(),
            "results": [],
        }
    recovery_scope = _queue_recovery_scope_content_ids(
        stage="download",
        limit=limit,
        db_path=db_path,
        scope_content_ids=scope_content_ids,
    )
    stale_recovery = recover_stale_media_processing_slots(
        db_path=db_path,
        processor_types=("download",),
        content_ids=recovery_scope,
    )
    probed_content_ids = _queue_content_ids(
        stage="download",
        limit=limit + 1,
        db_path=db_path,
        scope_content_ids=scope_content_ids,
    )
    truncated = len(probed_content_ids) > limit
    content_ids = probed_content_ids[:limit]

    def download(content_id: int) -> Dict[str, Any]:
        try:
            return process_content_media(content_id, download_only=True, db_path=db_path)
        except Exception as exc:
            with connect(db_path) as connection:
                slot = connection.execute(
                    """
                    SELECT slot.status,slot.attempt_count
                    FROM evidence_artifacts source
                    JOIN media_processing_slots slot
                      ON slot.content_id=source.content_id
                     AND slot.source_sha256=source.sha256
                     AND slot.processor_type='download'
                    WHERE source.content_id=? AND source.artifact_type='media_source'
                      AND source.status='available'
                    ORDER BY source.id DESC,slot.id DESC LIMIT 1
                    """,
                    (content_id,),
                ).fetchone()
            status = (
                "terminal_failed"
                if slot is not None
                and (
                    slot["status"] == "terminal_failed"
                    or int(slot["attempt_count"]) >= MAX_MEDIA_DOWNLOAD_ATTEMPTS
                )
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
    scope_content_ids: Optional[Iterable[int]] = None,
) -> Dict[str, Any]:
    if limit < 0:
        raise ValueError("media queue limit must be non-negative")
    if limit == 0:
        return {
            "candidates": 0,
            "evidence_ready": 0,
            "retryable_failed": 0,
            "terminal_failed": 0,
            "failed": 0,
            "truncated": False,
            "has_more": False,
            "stale_recovery": _empty_stale_recovery_counts(),
            "results": [],
        }
    versions = processor_versions()
    recovery_scope = _queue_recovery_scope_content_ids(
        stage="process",
        limit=limit,
        db_path=db_path,
        scope_content_ids=scope_content_ids,
    )
    stale_recovery = recover_stale_media_processing_slots(
        db_path=db_path,
        processor_types=("frames", "asr", "ocr"),
        processor_version_by_type={
            "frames": versions["frames"],
            "asr": versions["asr"],
            "ocr": versions["ocr"],
        },
        content_ids=recovery_scope,
    )
    probed_content_ids = _queue_content_ids(
        stage="process",
        limit=limit + 1,
        db_path=db_path,
        scope_content_ids=scope_content_ids,
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
