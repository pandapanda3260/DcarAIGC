#!/usr/bin/env python3
"""Run explicit local-only analysis canaries against a disposable database clone.

The controller is intentionally not a production publisher.  It binds media
outputs to an isolated root, freezes every input URL and source manifest, blocks
all provider/Hugging Face network paths, and writes an intent/receipt pair that
can be resumed after an ordinary processing failure.
"""

# ruff: noqa: E402 -- direct execution bootstraps repo imports after disabling pyc.

from __future__ import annotations

import argparse
import contextlib
import fcntl
import gc
import hashlib
import importlib
import ipaddress
import json
import math
import os
import re
import shutil
import socket
import sqlite3
import stat
import subprocess
import sys
import threading
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from contextlib import ExitStack, closing, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Iterator, Mapping, Sequence
from unittest.mock import patch

sys.dont_write_bytecode = True
os.environ["HF_HUB_OFFLINE"] = "1"
os.environ["TRANSFORMERS_OFFLINE"] = "1"

if __package__ in {None, ""}:
    repository_root = Path(__file__).resolve().parents[1]
    for candidate in (repository_root, repository_root / "src/dcar_eval"):
        value = str(candidate)
        if value not in sys.path:
            sys.path.insert(0, value)

from v8 import capture as capture_module
from v8 import duplicates as duplicates_module
from v8 import evaluation as evaluation_module
from v8 import evaluation_selectors as evaluation_selectors_module
from v8 import matcher_dsl as matcher_dsl_module
from v8 import media as media_module
from v8 import providers as providers_module
from v8 import storage as storage_module
from v8 import taxonomy as taxonomy_module


SCHEMA_VERSION = "local-analysis-canary-v1"
MAX_DOWNLOAD_BYTES = media_module.DEFAULT_MAX_MEDIA_DOWNLOAD_BYTES
MAX_VIDEO_DURATION_SECONDS = media_module.DEFAULT_MAX_VIDEO_DURATION_SECONDS
MANAGED_TARGET_TABLES = frozenset(
    {
        "evidence_artifacts",
        "evidence_envelopes",
        "media_processing_slots",
        "evaluation_versions",
        "evaluation_matches",
        "duplicate_fingerprints",
    }
)
CONTENT_MUTABLE_COLUMNS = frozenset({"evaluation_content_direction"})
SOURCE_BODY_KEYS = frozenset(
    {
        "schema_version",
        "media_kind",
        "urls",
        "source_sha256",
        "raw_response_id",
        "captured_at",
    }
)
SOURCE_METADATA_KEYS = frozenset(
    {"media_kind", "source_count", "source_sha256", "raw_response_id"}
)
PROCESSOR_TYPES = frozenset(
    {"download", "frames", "asr", "ocr", "duplicate_fingerprint"}
)
ALLOWED_MEDIA_CDN_SUFFIXES = frozenset(
    {
        "douyinstatic.com",
        "douyinpic.com",
        "douyinvod.com",
        "rednotecdn.com",
        "xhscdn.com",
    }
)
DOUYIN_DIRECT_VIDEO_CDN_HOSTS = frozenset(
    {
        "v5-dy-ov-experiment.zjcdn.com",
        "v5-hl-zenl-ov.zjcdn.com",
    }
)
DOUYIN_DIRECT_VIDEO_PATH = re.compile(
    r"/[0-9a-f]{32}/[0-9a-f]{8}/video/tos/cn/tos-cn-ve-15/"
    r"[A-Za-z0-9]{38}/"
)
GENERATED_ARTIFACT_TYPES = frozenset(
    {
        "media",
        "media_manifest",
        "frames_manifest",
        "asr",
        "ocr",
        "duplicate_fingerprint",
    }
)
STEP3_TARGET_CONTRACT_FIELDS = (
    "content_id",
    "platform",
    "expected_detail_operation",
    "source_discovery_raw_id",
    "source_discovery_operation",
    "source_discovery_sha256",
    "source_discovery_captured_at",
    "expected_detail_data_sha256",
    "expected_detail_raw_sha256",
    "expected_detail_raw_bytes",
)


class LocalAnalysisCanaryError(RuntimeError):
    pass


@dataclass(frozen=True)
class CanaryPaths:
    source_database: Path
    source_completion: Path
    database: Path
    media_root: Path
    fingerprint_root: Path
    run_root: Path
    copy_partial: Path
    copy_intent: Path
    copy_receipt: Path
    contract: Path
    intent: Path
    receipt: Path
    state: Path
    running_recovery: Path
    output_recovery: Path
    network_ledger: Path
    progress: Path


def _now_text() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _canonical_bytes(value: Any) -> bytes:
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


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _sha256_descriptor(descriptor: int) -> str:
    digest = hashlib.sha256()
    os.lseek(descriptor, 0, os.SEEK_SET)
    while True:
        block = os.read(descriptor, 1024 * 1024)
        if not block:
            break
        digest.update(block)
    return digest.hexdigest()


def _unlink_verified_private_file(
    path: Path,
    *,
    expected_device: int,
    expected_inode: int,
    expected_byte_size: int,
    expected_sha256: str,
) -> None:
    quarantine = path.with_name(
        f".{path.name}.cleanup-{expected_sha256[:16]}.quarantine"
    )
    if os.path.lexists(quarantine):
        raise LocalAnalysisCanaryError("待清理owned output quarantine已存在")
    flags = os.O_RDONLY | os.O_NONBLOCK
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_dev != expected_device
            or metadata.st_ino != expected_inode
            or metadata.st_size != expected_byte_size
            or _sha256_descriptor(descriptor) != expected_sha256
        ):
            raise LocalAnalysisCanaryError("待清理owned output fd证据漂移")
        current = path.lstat()
        if (
            current.st_dev != metadata.st_dev
            or current.st_ino != metadata.st_ino
            or current.st_size != metadata.st_size
            or path.is_symlink()
        ):
            raise LocalAnalysisCanaryError("待清理owned output路径身份漂移")
        os.replace(path, quarantine)
        _fsync_directory(path.parent)
        quarantined = quarantine.lstat()
        if (
            quarantined.st_dev != metadata.st_dev
            or quarantined.st_ino != metadata.st_ino
            or quarantined.st_size != metadata.st_size
            or quarantine.is_symlink()
        ):
            if not os.path.lexists(path):
                os.replace(quarantine, path)
                _fsync_directory(path.parent)
            raise LocalAnalysisCanaryError(
                "待清理owned output quarantine身份漂移"
            )
        quarantine.unlink()
    finally:
        os.close(descriptor)
    _fsync_directory(path.parent)


def _resume_verified_output_quarantine(
    path: Path,
    *,
    expected_byte_size: int,
    expected_sha256: str,
) -> None:
    quarantine = path.with_name(
        f".{path.name}.cleanup-{expected_sha256[:16]}.quarantine"
    )
    if not os.path.lexists(quarantine):
        return
    if os.path.lexists(path):
        raise LocalAnalysisCanaryError(
            "owned output原路径与quarantine同时存在"
        )
    metadata = _private_file(quarantine, label="owned output quarantine")
    if (
        metadata.st_size != expected_byte_size
        or _sha256_file(quarantine) != expected_sha256
    ):
        raise LocalAnalysisCanaryError("owned output quarantine证据漂移")
    quarantine.unlink()
    _fsync_directory(quarantine.parent)


def _json_sha256(value: Any) -> str:
    return _sha256_bytes(_canonical_bytes(value))


def _compact_json_sha256(value: Any) -> str:
    return _sha256_bytes(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    )


def _private_file(path: Path, *, label: str) -> os.stat_result:
    try:
        metadata = path.lstat()
    except FileNotFoundError as exc:
        raise LocalAnalysisCanaryError(f"{label}不存在：{path}") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or path.is_symlink()
        or metadata.st_nlink != 1
    ):
        raise LocalAnalysisCanaryError(f"{label}不是私有单链接普通文件：{path}")
    return metadata


def _private_directory(path: Path, *, label: str) -> os.stat_result:
    try:
        metadata = path.lstat()
    except FileNotFoundError as exc:
        raise LocalAnalysisCanaryError(f"{label}不存在：{path}") from exc
    if not stat.S_ISDIR(metadata.st_mode) or path.is_symlink():
        raise LocalAnalysisCanaryError(f"{label}不是私有目录：{path}")
    return metadata


def _assert_no_symlink_components(path: Path, *, label: str) -> None:
    lexical = Path(os.path.abspath(path))
    current = lexical
    while not os.path.lexists(current):
        if current.parent == current:
            break
        current = current.parent
    while True:
        if current.is_symlink():
            raise LocalAnalysisCanaryError(f"{label}路径包含符号链接：{current}")
        if current.parent == current:
            break
        current = current.parent


@dataclass(frozen=True)
class _DiscoveryRawCacheEntry:
    database_evidence: tuple[Any, ...]
    path: Path
    file_identity: tuple[int, ...]
    body: Mapping[str, Any]


class _DiscoveryRawCache:
    """Cache large discovery payloads while rechecking their immutable identity."""

    def __init__(self) -> None:
        self._entries: dict[int, _DiscoveryRawCacheEntry] = {}
        self.file_load_count = 0
        self.cache_hit_count = 0

    @staticmethod
    def _database_evidence(row: sqlite3.Row) -> tuple[Any, ...]:
        return tuple(
            _json_value(row[key])
            for key in (
                "id",
                "account_id",
                "content_id",
                "provider",
                "operation",
                "local_path",
                "sha256",
                "byte_size",
                "http_status",
                "captured_at",
                "source",
            )
        )

    @staticmethod
    def _file_identity(metadata: os.stat_result) -> tuple[int, ...]:
        return (
            int(metadata.st_dev),
            int(metadata.st_ino),
            int(metadata.st_mode),
            int(metadata.st_nlink),
            int(metadata.st_size),
            int(metadata.st_mtime_ns),
            int(metadata.st_ctime_ns),
        )

    def load(self, row: sqlite3.Row) -> Mapping[str, Any]:
        raw_id = int(row["id"])
        database_evidence = self._database_evidence(row)
        raw_local_path = Path(str(row["local_path"]))
        lexical_path = (
            raw_local_path
            if raw_local_path.is_absolute()
            else storage_module.PROJECT_ROOT / raw_local_path
        )
        _assert_no_symlink_components(
            lexical_path, label=f"discovery raw {raw_id}"
        )
        path = lexical_path.resolve()
        _assert_no_symlink_components(
            capture_module.RAW_ROOT, label="capture RAW_ROOT"
        )
        capture_raw_root = capture_module.RAW_ROOT.resolve()
        if not _is_within(path, capture_raw_root):
            raise LocalAnalysisCanaryError(
                "discovery raw未落在capture RAW_ROOT"
            )
        metadata = _private_file(path, label=f"discovery raw {raw_id}")
        file_identity = self._file_identity(metadata)
        flags = os.O_RDONLY | os.O_NONBLOCK
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(path, flags)
        except OSError as exc:
            raise LocalAnalysisCanaryError(
                f"discovery raw无法安全打开：{path}"
            ) from exc
        try:
            opened_identity = self._file_identity(os.fstat(descriptor))
            if opened_identity != file_identity:
                raise LocalAnalysisCanaryError(
                    "discovery raw path与打开descriptor身份不一致"
                )
            cached = self._entries.get(raw_id)
            cached_body: Mapping[str, Any] | None = None
            if cached is not None:
                if (
                    cached.database_evidence != database_evidence
                    or cached.path != path
                    or cached.file_identity != opened_identity
                ):
                    raise LocalAnalysisCanaryError(
                        "discovery raw cache命中时DB或文件身份漂移"
                    )
                cached_body = cached.body
            else:
                digest = hashlib.sha256()
                blocks: list[bytes] = []
                while True:
                    block = os.read(descriptor, 1024 * 1024)
                    if not block:
                        break
                    digest.update(block)
                    blocks.append(block)
                raw_bytes = b"".join(blocks)
                if self._file_identity(os.fstat(descriptor)) != opened_identity:
                    raise LocalAnalysisCanaryError(
                        "discovery raw读取期间descriptor身份漂移"
                    )
        finally:
            os.close(descriptor)
        _assert_no_symlink_components(
            lexical_path, label=f"discovery raw {raw_id} post-read"
        )
        if lexical_path.resolve() != path or not _is_within(
            path, capture_raw_root
        ):
            raise LocalAnalysisCanaryError("discovery raw读取期间路径漂移")
        after_metadata = _private_file(path, label=f"discovery raw {raw_id}")
        if self._file_identity(after_metadata) != opened_identity:
            raise LocalAnalysisCanaryError("discovery raw读取期间文件身份漂移")
        if cached_body is not None:
            self.cache_hit_count += 1
            return cached_body
        if (
            int(row["byte_size"] or -1) != len(raw_bytes)
            or int(row["byte_size"] or -1) != metadata.st_size
            or str(row["sha256"] or "") != digest.hexdigest()
        ):
            raise LocalAnalysisCanaryError(
                "discovery raw DB SHA/bytes与文件不一致"
            )
        try:
            body = json.loads(raw_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise LocalAnalysisCanaryError("discovery raw不是合法JSON") from exc
        if not isinstance(body, Mapping):
            raise LocalAnalysisCanaryError("discovery raw正文必须是JSON object")
        entry = _DiscoveryRawCacheEntry(
            database_evidence=database_evidence,
            path=path,
            file_identity=opened_identity,
            body=body,
        )
        self._entries[raw_id] = entry
        self.file_load_count += 1
        return entry.body


def _filesystem_canonical_path(path: Path) -> Path:
    lexical = Path(os.path.abspath(path))
    anchor = lexical
    suffix: list[str] = []
    while not os.path.lexists(anchor):
        if anchor.parent == anchor:
            break
        suffix.append(anchor.name)
        anchor = anchor.parent
    canonical_anchor = anchor.resolve()
    if hasattr(fcntl, "F_GETPATH") and os.path.lexists(anchor):
        # O_NONBLOCK prevents an untrusted FIFO/device path from hanging the
        # preflight before the later regular-file/directory type gates reject
        # it.
        flags = os.O_RDONLY | os.O_NONBLOCK
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(anchor, flags)
        try:
            # Python's fcntl wrapper accepts at most 1024 bytes on Darwin;
            # MAXPATHLEN is also 1024 there, which is the buffer F_GETPATH
            # expects.
            raw = fcntl.fcntl(descriptor, fcntl.F_GETPATH, b"\0" * 1024)
        finally:
            os.close(descriptor)
        canonical_anchor = Path(os.fsdecode(raw.split(b"\0", 1)[0]))
    result = canonical_anchor
    for component in reversed(suffix):
        result /= component
    return result


def _filesystem_comparison_key(path: Path) -> tuple[str, ...]:
    canonical = _filesystem_canonical_path(path)
    return tuple(
        unicodedata.normalize("NFC", component).casefold()
        for component in canonical.parts
    )


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_atomic(path: Path, body: bytes, *, immutable: bool) -> str:
    digest = _sha256_bytes(body)
    path.parent.mkdir(parents=True, exist_ok=True)
    if os.path.lexists(path):
        _private_file(path, label="运行记录")
        if immutable:
            if path.read_bytes() != body:
                raise LocalAnalysisCanaryError(f"运行记录内容漂移：{path}")
            with path.open("rb") as handle:
                os.fsync(handle.fileno())
            _fsync_directory(path.parent)
            return digest
    temporary = path.with_name(f".{path.name}.tmp")
    if os.path.lexists(temporary):
        _private_file(temporary, label="运行记录临时文件")
        temporary_body = temporary.read_bytes()
        if temporary_body == body:
            os.replace(temporary, path)
            _fsync_directory(path.parent)
            return digest
        if immutable and body.startswith(temporary_body):
            temporary.unlink()
            _fsync_directory(path.parent)
        else:
            raise LocalAnalysisCanaryError(f"运行记录临时文件证据漂移：{temporary}")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(temporary, flags, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    _fsync_directory(path.parent)
    return digest


def _write_json(path: Path, value: Mapping[str, Any], *, immutable: bool) -> str:
    return _write_atomic(path, _canonical_bytes(value), immutable=immutable)


def _read_json(path: Path, *, label: str) -> Mapping[str, Any]:
    _private_file(path, label=label)
    try:
        value = json.loads(path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LocalAnalysisCanaryError(f"{label}不是合法JSON：{path}") from exc
    if not isinstance(value, Mapping):
        raise LocalAnalysisCanaryError(f"{label}必须是JSON object：{path}")
    return value


def _paths(
    *,
    source_db_path: Path,
    source_completion_path: Path,
    db_path: Path,
    media_root: Path,
    run_root: Path,
) -> CanaryPaths:
    lexical_values = (
        (Path(os.path.abspath(source_db_path)), "Step3源数据库"),
        (Path(os.path.abspath(source_completion_path)), "Step3 completion"),
        (Path(os.path.abspath(db_path)), "数据库"),
        (Path(os.path.abspath(media_root)), "媒体输出根"),
        (Path(os.path.abspath(run_root)), "运行记录根"),
    )
    for lexical, label in lexical_values:
        _assert_no_symlink_components(lexical, label=label)
    source_database = _filesystem_canonical_path(lexical_values[0][0])
    source_completion = _filesystem_canonical_path(lexical_values[1][0])
    database = _filesystem_canonical_path(lexical_values[2][0])
    media = _filesystem_canonical_path(lexical_values[3][0])
    run = _filesystem_canonical_path(lexical_values[4][0])
    fingerprint = database.parent / "duplicate-fingerprints"
    return CanaryPaths(
        source_database=source_database,
        source_completion=source_completion,
        database=database,
        media_root=media,
        fingerprint_root=_filesystem_canonical_path(fingerprint),
        run_root=run,
        copy_partial=database.with_name(f".{database.name}.copy-partial"),
        copy_intent=run / "copy-intent.json",
        copy_receipt=run / "copy-receipt.json",
        contract=run / "run-contract.json",
        intent=run / "intent.json",
        receipt=run / "receipt.json",
        state=run / "state.json",
        running_recovery=run / "running-recovery.json",
        output_recovery=run / "output-recovery.json",
        network_ledger=run / "network-ledger.json",
        progress=run / "progress.json",
    )


def _overlap(left: Path, right: Path) -> bool:
    left_key = _filesystem_comparison_key(left)
    right_key = _filesystem_comparison_key(right)
    return (
        left_key == right_key
        or left_key[: len(right_key)] == right_key
        or right_key[: len(left_key)] == left_key
    )


def _validate_paths(paths: CanaryPaths, *, work_database_must_exist: bool) -> None:
    for value, label in (
        (paths.source_database, "Step3源数据库"),
        (paths.source_completion, "Step3 completion"),
        (paths.database, "数据库"),
        (paths.media_root, "媒体输出根"),
        (paths.fingerprint_root, "指纹输出根"),
        (paths.run_root, "运行记录根"),
    ):
        _assert_no_symlink_components(value, label=label)
    _private_file(paths.source_database, label="Step3源数据库")
    _private_file(paths.source_completion, label="Step3 completion")
    formal = storage_module.DEFAULT_DB.resolve()
    if paths.database == formal or paths.source_database == formal:
        raise LocalAnalysisCanaryError("禁止直接处理正式数据库")
    work_database_exists = os.path.lexists(paths.database)
    copy_partial_exists = os.path.lexists(paths.copy_partial)
    copy_intent_temporary = paths.copy_intent.with_name(
        f".{paths.copy_intent.name}.tmp"
    )
    copy_intent_registered = paths.copy_intent.exists() or os.path.lexists(
        copy_intent_temporary
    )
    if os.path.lexists(copy_intent_temporary):
        _private_file(
            copy_intent_temporary, label="待恢复database copy intent临时文件"
        )
    if copy_partial_exists:
        if not copy_intent_registered:
            raise LocalAnalysisCanaryError("未绑定copy intent的partial database")
        _private_file(paths.copy_partial, label="待恢复partial database")
    if work_database_exists and copy_partial_exists:
        raise LocalAnalysisCanaryError("work database与partial database同时存在")
    if work_database_must_exist:
        _private_file(paths.database, label="待处理数据库clone")
    elif work_database_exists:
        if not copy_intent_registered:
            raise LocalAnalysisCanaryError("首次canary要求全新不存在的work database")
        _private_file(paths.database, label="待恢复数据库copy")
    if formal.exists():
        if work_database_exists and storage_module.is_formal_database_path(
            paths.database
        ):
            raise LocalAnalysisCanaryError("数据库clone与正式数据库指向同一文件")
        if storage_module.is_formal_database_path(paths.source_database):
            raise LocalAnalysisCanaryError("Step3源数据库与正式数据库指向同一文件")
    if work_database_exists and os.path.samefile(
        paths.database, paths.source_database
    ):
        raise LocalAnalysisCanaryError("work database不得与Step3源数据库同文件")
    canonical_media = media_module.MEDIA_ROOT.resolve()
    canonical_fingerprint = duplicates_module.FINGERPRINT_ROOT.resolve()
    for candidate, label in (
        (paths.media_root, "媒体输出根"),
        (paths.fingerprint_root, "指纹输出根"),
        (paths.run_root, "运行记录根"),
    ):
        if _overlap(candidate, canonical_media) or _overlap(
            candidate, canonical_fingerprint
        ):
            raise LocalAnalysisCanaryError(f"{label}不得指向正式canonical缓存")
    if any(
        _overlap(left, right)
        for index, left in enumerate(
            (paths.media_root, paths.fingerprint_root, paths.run_root)
        )
        for right in (paths.media_root, paths.fingerprint_root, paths.run_root)[
            index + 1 :
        ]
    ):
        raise LocalAnalysisCanaryError("媒体、指纹、运行记录根不得相同或相互包含")
    database_key = _filesystem_comparison_key(paths.database)
    if any(
        database_key[: len(root_key)] == root_key
        for root_key in (
            _filesystem_comparison_key(paths.media_root),
            _filesystem_comparison_key(paths.run_root),
        )
    ):
        raise LocalAnalysisCanaryError("数据库不得位于媒体或运行记录根内")
    for parent, label in (
        (paths.database.parent, "数据库父目录"),
        (paths.source_database.parent, "Step3源数据库父目录"),
        (paths.source_completion.parent, "Step3 completion父目录"),
        (paths.media_root.parent, "媒体输出父目录"),
        (paths.fingerprint_root.parent, "指纹输出父目录"),
        (paths.run_root.parent, "运行记录父目录"),
    ):
        _private_directory(parent, label=label)


def _claim_path(target: Path, *, label: str) -> Path:
    identity = "/".join(_filesystem_comparison_key(target))
    digest = _sha256_bytes(identity.encode("utf-8"))[:20]
    return target.parent / f".dcar-local-analysis-{label}-{digest}.claim"


def _global_claim_path() -> Path:
    # Do not anchor this lock in TMPDIR: two invocations with different
    # environments must still serialize.  A single global apply lock is
    # deliberately conservative and closes cross-role as well as parent/child
    # output-root overlap between independent runs.
    for parent in (Path("/var/tmp"), Path("/tmp")):
        if parent.is_dir():
            return parent / "dcar-local-analysis-canary.global.claim"
    raise LocalAnalysisCanaryError("找不到固定全局排他claim目录")


@contextmanager
def _exclusive_claim(path: Path) -> Iterator[None]:
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise LocalAnalysisCanaryError(f"排他claim不是私有普通文件：{path}")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise LocalAnalysisCanaryError(f"已有进程占用排他claim：{path}") from exc
        yield
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


@contextmanager
def _all_claims(paths: CanaryPaths) -> Iterator[None]:
    claims = {
        _claim_path(paths.database, label="database"),
        _claim_path(paths.media_root, label="media"),
        _claim_path(paths.fingerprint_root, label="fingerprint"),
        _claim_path(paths.run_root, label="run"),
    }
    with ExitStack() as stack:
        stack.enter_context(_exclusive_claim(_global_claim_path()))
        for claim in sorted(claims, key=str):
            stack.enter_context(_exclusive_claim(claim))
        yield


def _prepare_roots(paths: CanaryPaths) -> None:
    for root, label in (
        (paths.media_root, "媒体输出根"),
        (paths.fingerprint_root, "指纹输出根"),
        (paths.run_root, "运行记录根"),
    ):
        if not os.path.lexists(root):
            root.mkdir(mode=0o700)
            _fsync_directory(root.parent)
        _private_directory(root, label=label)


def _require_clean_database(path: Path) -> None:
    for suffix in ("-wal", "-shm", "-journal"):
        if os.path.lexists(Path(f"{path}{suffix}")):
            raise LocalAnalysisCanaryError(f"数据库clone存在sidecar：{path}{suffix}")


def _database_sidecars(path: Path) -> list[Path]:
    return [
        candidate
        for suffix in ("-wal", "-shm", "-journal")
        if os.path.lexists(candidate := Path(f"{path}{suffix}"))
    ]


def _validate_recoverable_sidecars(paths: CanaryPaths) -> None:
    for sidecar in _database_sidecars(paths.database):
        _private_file(sidecar, label="待恢复数据库sidecar")
        if sidecar.parent != paths.database.parent:
            raise LocalAnalysisCanaryError("数据库sidecar路径逃逸")


def _finalize_database(path: Path) -> None:
    gc.collect()
    connection = sqlite3.connect(path, timeout=30)
    try:
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        mode = str(connection.execute("PRAGMA journal_mode=DELETE").fetchone()[0])
        if mode.lower() != "delete":
            raise LocalAnalysisCanaryError(f"数据库无法切回DELETE journal：{mode}")
        if str(connection.execute("PRAGMA quick_check").fetchone()[0]) != "ok":
            raise LocalAnalysisCanaryError("数据库quick_check失败")
        if int(connection.execute("PRAGMA foreign_key_check").fetchone() is not None):
            raise LocalAnalysisCanaryError("数据库foreign key check失败")
    finally:
        connection.close()
    _require_clean_database(path)


def _immutable_connection(path: Path) -> sqlite3.Connection:
    uri = f"file:{urllib.parse.quote(str(path))}?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _json_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"blob_sha256": _sha256_bytes(value), "byte_size": len(value)}
    return value


def _digest_query(
    connection: sqlite3.Connection,
    query: str,
    parameters: Sequence[Any] = (),
) -> Mapping[str, Any]:
    cursor = connection.execute(query, parameters)
    columns = [str(item[0]) for item in cursor.description or ()]
    digest = hashlib.sha256()
    count = 0
    for row in cursor:
        body = _canonical_bytes(
            {column: _json_value(row[index]) for index, column in enumerate(columns)}
        )
        digest.update(body)
        count += 1
    return {"rows": count, "sha256": digest.hexdigest()}


def _table_columns(connection: sqlite3.Connection, table: str) -> list[str]:
    escaped = table.replace('"', '""')
    return [
        str(row["name"])
        for row in connection.execute(f'PRAGMA table_info("{escaped}")')
    ]


def _quoted(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _id_filter(content_ids: Sequence[int], *, negate: bool) -> tuple[str, list[int]]:
    placeholders = ",".join("?" for _ in content_ids)
    operator = "NOT IN" if negate else "IN"
    return f"content_id {operator} ({placeholders})", list(content_ids)


def _protected_snapshot(
    connection: sqlite3.Connection, content_ids: Sequence[int]
) -> Mapping[str, Any]:
    tables = [
        str(row["name"])
        for row in connection.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        )
    ]
    result: dict[str, Any] = {}
    for table in tables:
        escaped = _quoted(table)
        if table == "content_items":
            columns = _table_columns(connection, table)
            stable = [column for column in columns if column not in CONTENT_MUTABLE_COLUMNS]
            result["content_items_stable"] = _digest_query(
                connection,
                f"SELECT {','.join(_quoted(column) for column in stable)} "
                f"FROM {escaped} ORDER BY rowid",
            )
            placeholders = ",".join("?" for _ in content_ids)
            result["content_items_non_target"] = _digest_query(
                connection,
                f"SELECT * FROM {escaped} WHERE id NOT IN ({placeholders}) ORDER BY rowid",
                content_ids,
            )
        elif table in MANAGED_TARGET_TABLES:
            columns = _table_columns(connection, table)
            if "content_id" in columns:
                clause, values = _id_filter(content_ids, negate=True)
                result[f"{table}_non_target"] = _digest_query(
                    connection,
                    f"SELECT * FROM {escaped} WHERE {clause} ORDER BY rowid",
                    values,
                )
            elif table == "evaluation_matches":
                placeholders = ",".join("?" for _ in content_ids)
                result["evaluation_matches_non_target"] = _digest_query(
                    connection,
                    """
                    SELECT m.* FROM evaluation_matches m
                    JOIN evaluation_versions v ON v.id=m.evaluation_id
                    WHERE v.content_id NOT IN ("""
                    + placeholders
                    + ") ORDER BY m.rowid",
                    content_ids,
                )
        else:
            result[table] = _digest_query(
                connection, f"SELECT * FROM {escaped} ORDER BY rowid"
            )
    return result


def _target_snapshot(
    connection: sqlite3.Connection, content_ids: Sequence[int]
) -> Mapping[str, Any]:
    result: dict[str, Any] = {}
    placeholders = ",".join("?" for _ in content_ids)
    for table in sorted(MANAGED_TARGET_TABLES):
        columns = _table_columns(connection, table)
        if "content_id" in columns:
            result[table] = _digest_query(
                connection,
                f"SELECT * FROM {_quoted(table)} WHERE content_id IN ({placeholders}) "
                "ORDER BY rowid",
                content_ids,
            )
        elif table == "evaluation_matches":
            result[table] = _digest_query(
                connection,
                """
                SELECT m.* FROM evaluation_matches m
                JOIN evaluation_versions v ON v.id=m.evaluation_id
                WHERE v.content_id IN ("""
                + placeholders
                + ") ORDER BY m.rowid",
                content_ids,
            )
    return result


def _target_rows(
    connection: sqlite3.Connection, content_ids: Sequence[int]
) -> Mapping[str, list[Mapping[str, Any]]]:
    result: dict[str, list[Mapping[str, Any]]] = {}
    placeholders = ",".join("?" for _ in content_ids)
    for table in sorted(MANAGED_TARGET_TABLES):
        columns = _table_columns(connection, table)
        if "content_id" in columns:
            cursor = connection.execute(
                f"SELECT * FROM {_quoted(table)} WHERE content_id IN ({placeholders}) "
                "ORDER BY rowid",
                content_ids,
            )
        elif table == "evaluation_matches":
            cursor = connection.execute(
                """
                SELECT m.* FROM evaluation_matches m
                JOIN evaluation_versions v ON v.id=m.evaluation_id
                WHERE v.content_id IN ("""
                + placeholders
                + ") ORDER BY m.rowid",
                content_ids,
            )
        else:
            continue
        names = [str(value[0]) for value in cursor.description or ()]
        result[table] = [
            {name: _json_value(row[index]) for index, name in enumerate(names)}
            for row in cursor
        ]
    return result


def _sqlite_sequence_snapshot(connection: sqlite3.Connection) -> Mapping[str, int]:
    exists = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='sqlite_sequence'"
    ).fetchone()
    if exists is None:
        return {}
    return {
        str(row["name"]): int(row["seq"])
        for row in connection.execute("SELECT name,seq FROM sqlite_sequence ORDER BY name")
    }


def _resolve_artifact_path(raw: str) -> Path:
    path = Path(raw)
    return path.resolve() if path.is_absolute() else (storage_module.PROJECT_ROOT / path).resolve()


def _safe_url(
    url: str,
    *,
    media_kind: str,
    platform: str,
    provider: str,
    operation: str,
) -> Mapping[str, Any]:
    try:
        parsed = urllib.parse.urlsplit(url)
        hostname = (parsed.hostname or "").lower()
        port = parsed.port
    except ValueError as exc:
        raise LocalAnalysisCanaryError(f"媒体URL无效：{url}") from exc
    if (
        not hostname
        or parsed.username is not None
        or parsed.password is not None
        or hostname == "localhost"
        or hostname.endswith(".local")
    ):
        raise LocalAnalysisCanaryError(f"媒体URL主机不安全：{url}")
    try:
        address = ipaddress.ip_address(hostname.strip("[]"))
    except ValueError:
        address = None
    if address is not None:
        raise LocalAnalysisCanaryError(f"媒体URL不得使用IP literal：{url}")
    if parsed.scheme.lower() != "https" or port not in (None, 443):
        raise LocalAnalysisCanaryError(f"媒体URL必须是HTTPS:443：{url}")
    if not media_module.is_supported_media_url(url):
        raise LocalAnalysisCanaryError(f"媒体URL协议或域名不受支持：{url}")
    lowered_path = parsed.path.lower()
    if media_kind == "video" and (
        lowered_path.endswith((".mp3", ".m4a", ".aac", ".wav", ".flac"))
        or "images_no_sound_volume_audio_file" in lowered_path
    ):
        raise LocalAnalysisCanaryError(f"视频source实际是音频占位文件：{url}")
    network_allowed = any(
        hostname == suffix or hostname.endswith(f".{suffix}")
        for suffix in ALLOWED_MEDIA_CDN_SUFFIXES
    )
    if (
        not network_allowed
        and platform == "douyin"
        and media_kind == "video"
        and provider == "TikHub"
        and operation == "douyin_video_detail"
        and hostname in DOUYIN_DIRECT_VIDEO_CDN_HOSTS
        and port is None
        and not parsed.fragment
        and bool(parsed.query)
        and DOUYIN_DIRECT_VIDEO_PATH.fullmatch(parsed.path) is not None
    ):
        network_allowed = True
    return {
        "url": url,
        "host": hostname,
        "url_sha256": _sha256_bytes(url.encode()),
        "network_allowed": network_allowed,
        "deny_reason": None if network_allowed else "host_not_in_media_cdn_allowlist",
    }


def _source_snapshot(
    connection: sqlite3.Connection,
    content_id: int,
    *,
    step3_media_root: Path,
    step3_derived_raw_root: Path,
    target_contract_row: Sequence[Any],
    discovery_raw_cache: _DiscoveryRawCache | None = None,
) -> Mapping[str, Any]:
    content = connection.execute(
        "SELECT * FROM content_items WHERE id=?", (content_id,)
    ).fetchone()
    if content is None:
        raise LocalAnalysisCanaryError(f"content不存在：{content_id}")
    source_group = content["source_group"]
    if type(source_group) is not str:
        raise LocalAnalysisCanaryError(
            f"content {content_id} source_group不是字符串"
        )
    if source_group not in storage_module.BACKFILL_SOURCE_GROUPS:
        raise LocalAnalysisCanaryError(
            f"content {content_id} 不属于冻结history source_group：{source_group!r}"
        )
    platform = content["platform"]
    if type(platform) is not str:
        raise LocalAnalysisCanaryError(f"content {content_id} platform不是字符串")
    expected_operation = {
        "douyin": "douyin_video_detail",
        "xiaohongshu": "xiaohongshu_note_detail",
    }.get(platform)
    if expected_operation is None:
        raise LocalAnalysisCanaryError(
            f"content {content_id} 平台不属于冻结双平台：{platform!r}"
        )
    link_id = content["link_id"]
    if type(link_id) is not str:
        raise LocalAnalysisCanaryError(f"content {content_id} link_id不是字符串")
    if re.fullmatch(r"[A-Za-z0-9]{6}", link_id) is None:
        raise LocalAnalysisCanaryError(
            f"content {content_id} link_id不是安全单段basename：{link_id!r}"
        )
    artifact = connection.execute(
        """
        SELECT * FROM evidence_artifacts
        WHERE content_id=? AND artifact_type='media_source' AND status='available'
        ORDER BY id DESC LIMIT 1
        """,
        (content_id,),
    ).fetchone()
    if artifact is None:
        raise LocalAnalysisCanaryError(f"content {content_id} 缺少media_source")
    if type(artifact["local_path"]) is not str:
        raise LocalAnalysisCanaryError("media_source local_path不是字符串")
    raw_artifact_path = Path(artifact["local_path"])
    lexical_artifact_path = (
        raw_artifact_path
        if raw_artifact_path.is_absolute()
        else storage_module.PROJECT_ROOT / raw_artifact_path
    )
    _assert_no_symlink_components(
        lexical_artifact_path, label=f"content {content_id} media_source"
    )
    path = lexical_artifact_path.resolve()
    metadata = _private_file(path, label=f"content {content_id} media_source")
    if not _is_within(path, step3_media_root):
        raise LocalAnalysisCanaryError("media_source未落在冻结Step3 media_root")
    body_bytes = path.read_bytes()
    try:
        body = json.loads(body_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LocalAnalysisCanaryError("media_source不是合法JSON") from exc
    if not isinstance(body, Mapping) or set(body) != SOURCE_BODY_KEYS:
        raise LocalAnalysisCanaryError("media_source正文形状不精确")
    if (
        type(body.get("schema_version")) is not str
        or body.get("schema_version") != media_module.MEDIA_SOURCE_VERSION
    ):
        raise LocalAnalysisCanaryError("media_source schema_version不匹配")
    media_kind = body.get("media_kind")
    if type(media_kind) is not str:
        raise LocalAnalysisCanaryError("media_source media_kind不是字符串")
    raw_urls = body.get("urls")
    if not isinstance(raw_urls, list) or not all(type(item) is str for item in raw_urls):
        raise LocalAnalysisCanaryError("media_source URLs不是字符串数组")
    raw_response_id = body.get("raw_response_id")
    if type(raw_response_id) is not int or raw_response_id <= 0:
        raise LocalAnalysisCanaryError("media_source raw_response_id不是正整数")
    if type(body.get("captured_at")) is not str or not body.get("captured_at"):
        raise LocalAnalysisCanaryError("media_source captured_at不是非空字符串")
    raw = connection.execute(
        "SELECT * FROM provider_raw_responses WHERE id=?",
        (raw_response_id,),
    ).fetchone()
    if raw is None:
        raise LocalAnalysisCanaryError("media_source绑定的raw_response不存在")
    if (
        int(raw["content_id"] or -1) != content_id
        or str(raw["operation"] or "") != expected_operation
        or str(raw["source"] or "") != "derived_applied"
        or int(raw["http_status"] or -1) != 200
    ):
        raise LocalAnalysisCanaryError(
            f"content {content_id} derived raw operation/source/content绑定不精确"
        )
    url_rows = [
        _safe_url(
            url,
            media_kind=media_kind,
            platform=platform,
            provider=str(raw["provider"] or ""),
            operation=str(raw["operation"] or ""),
        )
        for url in raw_urls
    ]
    if not any(bool(row["network_allowed"]) for row in url_rows):
        raise LocalAnalysisCanaryError("media_source没有允许直连的冻结CDN URL")
    download_urls = [
        str(row["url"]) for row in url_rows if bool(row["network_allowed"])
    ]
    urls, logical_sha = media_module._media_source_identity(media_kind, raw_urls)
    if urls != raw_urls or str(body.get("source_sha256")) != logical_sha:
        raise LocalAnalysisCanaryError("media_source URL或logical SHA不精确")
    expected_kind = str(content["content_type"] or "")
    if media_kind != expected_kind or media_kind not in {"video", "image"}:
        raise LocalAnalysisCanaryError(
            f"content/media kind冲突：{expected_kind!r}/{media_kind!r}"
        )
    if media_kind == "image" and len(download_urls) != len(raw_urls):
        raise LocalAnalysisCanaryError("图片source必须全部是允许直连的冻结CDN URL")
    image_groups: list[Mapping[str, Any]] = []
    body_sha = _sha256_bytes(body_bytes)
    if type(artifact["sha256"]) is not str or artifact["sha256"] != body_sha:
        raise LocalAnalysisCanaryError("media_source DB SHA与正文不一致")
    if type(artifact["byte_size"]) is not int or artifact["byte_size"] != metadata.st_size:
        raise LocalAnalysisCanaryError("media_source DB byte_size与文件不一致")
    try:
        artifact_metadata = json.loads(str(artifact["metadata_json"] or "{}"))
    except json.JSONDecodeError as exc:
        raise LocalAnalysisCanaryError("media_source metadata_json无效") from exc
    expected_metadata = {
        "media_kind": media_kind,
        "source_count": len(urls),
        "source_sha256": logical_sha,
        "raw_response_id": raw_response_id,
    }
    if (
        not isinstance(artifact_metadata, Mapping)
        or set(artifact_metadata) != SOURCE_METADATA_KEYS
        or type(artifact_metadata.get("source_count")) is not int
        or type(artifact_metadata.get("raw_response_id")) is not int
        or _canonical_bytes(artifact_metadata) != _canonical_bytes(expected_metadata)
    ):
        raise LocalAnalysisCanaryError("media_source metadata与正文不一致")
    raw_local_path = Path(str(raw["local_path"]))
    raw_lexical_path = (
        raw_local_path
        if raw_local_path.is_absolute()
        else storage_module.PROJECT_ROOT / raw_local_path
    )
    _assert_no_symlink_components(
        raw_lexical_path, label=f"content {content_id} derived raw"
    )
    raw_path = raw_lexical_path.resolve()
    raw_metadata = _private_file(raw_path, label=f"content {content_id} derived raw")
    if not _is_within(raw_path, step3_derived_raw_root):
        raise LocalAnalysisCanaryError("derived raw未落在冻结Step3 derived_raw_root")
    raw_bytes = raw_path.read_bytes()
    raw_sha = _sha256_bytes(raw_bytes)
    if (
        str(raw["sha256"] or "") != raw_sha
        or int(raw["byte_size"] or -1) != raw_metadata.st_size
    ):
        raise LocalAnalysisCanaryError("derived raw DB SHA/bytes与文件不一致")
    try:
        raw_body = json.loads(raw_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LocalAnalysisCanaryError("derived raw不是合法JSON") from exc
    if not isinstance(raw_body, Mapping) or set(raw_body) != {
        "data",
        "derived_from_operation",
        "source_captured_at",
        "source_raw_response_id",
        "source_sha256",
        "stage",
    }:
        raise LocalAnalysisCanaryError("derived raw envelope形状不精确")
    expected_source_operation = {
        "douyin": "douyin_user_posts",
        "xiaohongshu": "xiaohongshu_user_posts",
    }[platform]
    raw_data = raw_body.get("data")
    content_title = content["title"]
    content_body = content["body"]
    if type(content_title) is not str or type(content_body) is not str:
        raise LocalAnalysisCanaryError("content title/body不是字符串")
    if (
        raw_body.get("stage") != "detail"
        or raw_body.get("derived_from_operation") != expected_source_operation
        or not isinstance(raw_data, Mapping)
        or type(raw_data.get("content_type")) is not str
        or raw_data.get("content_type") != media_kind
        or not isinstance(raw_data.get("media_urls"), list)
        or any(type(value) is not str for value in raw_data["media_urls"])
        or raw_data.get("media_urls") != urls
        or type(raw_data.get("title")) is not str
        or raw_data.get("title") != content_title
        or type(raw_data.get("body")) is not str
        or raw_data.get("body") != content_body
    ):
        raise LocalAnalysisCanaryError(
            f"content {content_id} derived raw data/platform/media/text合同漂移"
        )
    if len(target_contract_row) != len(STEP3_TARGET_CONTRACT_FIELDS):
        raise LocalAnalysisCanaryError("Step3 explicit target row形状漂移")
    frozen_target = dict(zip(STEP3_TARGET_CONTRACT_FIELDS, target_contract_row))
    if (
        type(frozen_target["content_id"]) is not int
        or type(frozen_target["source_discovery_raw_id"]) is not int
        or type(frozen_target["expected_detail_raw_bytes"]) is not int
        or any(
            type(frozen_target[key]) is not str
            for key in (
                "platform",
                "expected_detail_operation",
                "source_discovery_operation",
                "source_discovery_sha256",
                "source_discovery_captured_at",
                "expected_detail_data_sha256",
                "expected_detail_raw_sha256",
            )
        )
    ):
        raise LocalAnalysisCanaryError("Step3 explicit target row类型漂移")
    discovery_raw = connection.execute(
        """
        SELECT id,account_id,content_id,provider,operation,local_path,sha256,
               byte_size,http_status,captured_at,source
        FROM provider_raw_responses WHERE id=?
        """,
        (frozen_target["source_discovery_raw_id"],),
    ).fetchone()
    if discovery_raw is None:
        raise LocalAnalysisCanaryError("Step3 target绑定的discovery raw不存在")
    if (
        frozen_target["content_id"] != content_id
        or frozen_target["platform"] != platform
        or frozen_target["expected_detail_operation"] != expected_operation
        or type(raw_body.get("source_raw_response_id")) is not int
        or raw_body.get("source_raw_response_id")
        != frozen_target["source_discovery_raw_id"]
        or type(raw_body.get("derived_from_operation")) is not str
        or raw_body.get("derived_from_operation")
        != frozen_target["source_discovery_operation"]
        or type(raw_body.get("source_sha256")) is not str
        or raw_body.get("source_sha256")
        != frozen_target["source_discovery_sha256"]
        or type(raw_body.get("source_captured_at")) is not str
        or raw_body.get("source_captured_at")
        != frozen_target["source_discovery_captured_at"]
        or _compact_json_sha256(raw_data)
        != frozen_target["expected_detail_data_sha256"]
        or raw_sha != frozen_target["expected_detail_raw_sha256"]
        or raw_metadata.st_size != frozen_target["expected_detail_raw_bytes"]
    ):
        raise LocalAnalysisCanaryError(
            f"content {content_id} derived raw未绑定Step3 target合同"
        )
    if (
        type(content["account_id"]) is not int
        or content["account_id"] <= 0
        or type(discovery_raw["account_id"]) is not int
        or discovery_raw["account_id"] <= 0
        or type(discovery_raw["id"]) is not int
        or discovery_raw["id"] != frozen_target["source_discovery_raw_id"]
        or discovery_raw["account_id"] != content["account_id"]
        or discovery_raw["content_id"] is not None
        or raw["account_id"] is not None
        or not str(discovery_raw["provider"] or "")
        or str(discovery_raw["provider"] or "") != str(raw["provider"] or "")
        or str(discovery_raw["operation"] or "")
        != str(frozen_target["source_discovery_operation"])
        or str(discovery_raw["sha256"] or "")
        != str(frozen_target["source_discovery_sha256"])
        or int(discovery_raw["byte_size"] or -1) <= 0
        or int(discovery_raw["http_status"] or -1) != 200
        or str(discovery_raw["captured_at"] or "")
        != str(frozen_target["source_discovery_captured_at"])
        or str(discovery_raw["source"] or "") != "live_applied"
    ):
        raise LocalAnalysisCanaryError(
            f"content {content_id} discovery raw数据库证据漂移"
        )
    if media_kind == "image":
        if platform == "douyin":
            cache = discovery_raw_cache or _DiscoveryRawCache()
            discovery_body = cache.load(discovery_raw)
            discovery_data = discovery_body.get("data")
            aweme_list = (
                discovery_data.get("aweme_list")
                if isinstance(discovery_data, Mapping)
                else None
            )
            if not isinstance(aweme_list, list) or not all(
                isinstance(item, Mapping) for item in aweme_list
            ):
                raise LocalAnalysisCanaryError(
                    "Douyin discovery raw缺少直接data.aweme_list数组"
                )
            platform_content_id = content["platform_content_id"]
            if type(platform_content_id) is not str or not platform_content_id:
                raise LocalAnalysisCanaryError(
                    f"content {content_id} platform_content_id不是非空字符串"
                )
            matching_awemes = [
                item
                for item in aweme_list
                if type(item.get("aweme_id")) is str
                and item.get("aweme_id") == platform_content_id
            ]
            if len(matching_awemes) != 1:
                raise LocalAnalysisCanaryError(
                    f"content {content_id} discovery raw未唯一aweme_id命中"
                )
            raw_images = matching_awemes[0].get("images")
            if not isinstance(raw_images, list) or not raw_images or not all(
                isinstance(image, Mapping) for image in raw_images
            ):
                raise LocalAnalysisCanaryError(
                    "Douyin discovery aweme images不是非空object数组"
                )
            exact_candidate_groups: list[list[str]] = []
            for image in raw_images:
                download_list = image.get("download_url_list")
                url_list = image.get("url_list")
                if (
                    not isinstance(download_list, list)
                    or not all(type(value) is str for value in download_list)
                    or not isinstance(url_list, list)
                    or not all(type(value) is str for value in url_list)
                    or not download_list + url_list
                ):
                    raise LocalAnalysisCanaryError(
                        "Douyin discovery image候选字段不是字符串数组或候选为空"
                    )
                exact_candidate_groups.append(
                    list(dict.fromkeys([*download_list, *url_list]))
                )
            candidate_groups = providers_module._douyin_image_url_groups(
                matching_awemes[0]
            )
            if (
                len(candidate_groups) != len(raw_images)
                or candidate_groups != exact_candidate_groups
            ):
                raise LocalAnalysisCanaryError(
                    "Douyin discovery image候选被provider归一化或静默跳过"
                )
            try:
                image_groups = media_module.douyin_image_source_groups(
                    download_urls, candidate_groups
                )
            except media_module.MediaProcessingError as exc:
                raise LocalAnalysisCanaryError(
                    "Douyin discovery images未精确扁平绑定media_source: "
                    f"{exc}"
                ) from exc
        else:
            image_groups = media_module.image_source_groups(
                download_urls, platform=platform
            )
            image_groups = media_module.validate_frozen_image_groups(
                download_urls,
                image_groups,
                platform=platform,
            )
        if not image_groups:
            raise LocalAnalysisCanaryError("图片source没有可处理的冻结逻辑图组")
    stable_content = {
        key: _json_value(content[key])
        for key in content.keys()
        if key not in CONTENT_MUTABLE_COLUMNS
    }
    return {
        "content": stable_content,
        "artifact": {key: _json_value(artifact[key]) for key in artifact.keys()},
        "artifact_body": body,
        "artifact_file": {
            "path": str(path),
            "sha256": body_sha,
            "byte_size": metadata.st_size,
            "inode": metadata.st_ino,
            "nlink": metadata.st_nlink,
        },
        "urls": url_rows,
        "urls_sha256": _json_sha256(urls),
        "download_urls": download_urls,
        "download_urls_sha256": _json_sha256(download_urls),
        "image_groups": image_groups,
        "image_groups_sha256": (
            media_module.image_groups_sha256(image_groups)
            if media_kind == "image"
            else None
        ),
        "raw_response": {key: _json_value(raw[key]) for key in raw.keys()},
        "raw_response_file": {
            "path": str(raw_path),
            "sha256": raw_sha,
            "byte_size": raw_metadata.st_size,
            "inode": raw_metadata.st_ino,
            "nlink": raw_metadata.st_nlink,
        },
        "raw_response_body_sha256": _json_sha256(raw_body),
        "step3_target_contract": frozen_target,
    }


def _source_snapshots(
    connection: sqlite3.Connection,
    content_ids: Sequence[int],
    *,
    step3_media_root: Path,
    step3_derived_raw_root: Path,
    target_contract_rows: Sequence[Sequence[Any]],
    allow_target_subset: bool = False,
    discovery_raw_cache: _DiscoveryRawCache | None = None,
) -> list[Mapping[str, Any]]:
    target_by_id: dict[int, Sequence[Any]] = {}
    for row in target_contract_rows:
        if (
            not isinstance(row, Sequence)
            or isinstance(row, (str, bytes))
            or not row
            or type(row[0]) is not int
            or int(row[0]) <= 0
        ):
            raise LocalAnalysisCanaryError("Step3 explicit target row形状漂移")
        content_id = int(row[0])
        if content_id in target_by_id:
            raise LocalAnalysisCanaryError(
                "Step3 explicit target rows含重复content_id"
            )
        target_by_id[content_id] = row
    requested_ids = list(content_ids)
    if len(set(requested_ids)) != len(requested_ids):
        raise LocalAnalysisCanaryError("请求content IDs含重复值")
    requested = set(requested_ids)
    covered = set(target_by_id)
    if (
        (allow_target_subset and not requested.issubset(covered))
        or (not allow_target_subset and covered != requested)
    ):
        raise LocalAnalysisCanaryError("Step3 explicit target rows未覆盖当前IDs")
    cache = discovery_raw_cache or _DiscoveryRawCache()
    return [
        _source_snapshot(
            connection,
            content_id,
            step3_media_root=step3_media_root,
            step3_derived_raw_root=step3_derived_raw_root,
            target_contract_row=target_by_id[content_id],
            discovery_raw_cache=cache,
        )
        for content_id in content_ids
    ]


def _completion_source_snapshots(
    connection: sqlite3.Connection,
    content_ids: Sequence[int],
    source_evidence: Mapping[str, Any],
    *,
    allow_step3_target_subset: bool = False,
) -> list[Mapping[str, Any]]:
    if source_evidence.get("completion_kind") is None:
        step3_contract = source_evidence["contract"]
        return _source_snapshots(
            connection,
            content_ids,
            step3_media_root=Path(str(step3_contract["media_root"])),
            step3_derived_raw_root=Path(str(step3_contract["derived_raw_root"])),
            target_contract_rows=step3_contract["explicit_target_rows"],
            allow_target_subset=allow_step3_target_subset,
        )
    paid = _paid_refresh_module()
    if source_evidence.get("completion_kind") != paid.COMPLETION_KIND:
        raise LocalAnalysisCanaryError("source evidence completion_kind漂移")
    try:
        return [
            paid.source_snapshot_from_evidence(
                connection, content_id, source_evidence
            )
            for content_id in content_ids
        ]
    except paid.PaidSourceRefreshError as exc:
        raise LocalAnalysisCanaryError(f"paid source snapshot阻断：{exc}") from exc


def _completion_source_snapshot(
    connection: sqlite3.Connection,
    content_id: int,
    source_evidence: Mapping[str, Any],
) -> Mapping[str, Any]:
    return _completion_source_snapshots(
        connection,
        [content_id],
        source_evidence,
        allow_step3_target_subset=True,
    )[0]


def _code_snapshot() -> list[Mapping[str, Any]]:
    files = (
        Path(__file__).resolve(),
        Path(__file__).resolve().with_name("run_paid_source_refresh_canary.py"),
        Path(capture_module.__file__).resolve(),
        Path(media_module.__file__).resolve(),
        Path(evaluation_module.__file__).resolve(),
        Path(evaluation_selectors_module.__file__).resolve(),
        Path(matcher_dsl_module.__file__).resolve(),
        Path(taxonomy_module.__file__).resolve(),
        Path(duplicates_module.__file__).resolve(),
        Path(providers_module.__file__).resolve(),
        Path(storage_module.__file__).resolve(),
        Path(evaluation_module.__file__).resolve().parent.parent
        / "three_proposition_scoring.py",
        media_module.CONFIG_PATH.resolve(),
    )
    return [
        {"path": str(path), "sha256": _sha256_file(path), "byte_size": path.stat().st_size}
        for path in files
    ]


def _whisper_model_inventory(model: Path) -> Mapping[str, Any]:
    repo_root = next(
        (parent for parent in (model, *model.parents) if (parent / "blobs").is_dir()),
        None,
    )
    if repo_root is None:
        raise LocalAnalysisCanaryError("Whisper snapshot不位于可验证HF repo cache")
    blobs_root = (repo_root / "blobs").resolve()
    rows: list[Mapping[str, Any]] = []
    for lexical in sorted(model.rglob("*"), key=lambda path: str(path.relative_to(model))):
        metadata = lexical.lstat()
        if stat.S_ISDIR(metadata.st_mode):
            if lexical.is_symlink():
                raise LocalAnalysisCanaryError("Whisper snapshot包含目录symlink")
            continue
        relative = str(lexical.relative_to(model))
        if lexical.is_symlink():
            target_text = os.readlink(lexical)
            try:
                resolved = lexical.resolve(strict=True)
            except (FileNotFoundError, RuntimeError) as exc:
                raise LocalAnalysisCanaryError("Whisper snapshot symlink无效") from exc
            resolved_metadata = resolved.stat()
            if (
                not stat.S_ISREG(resolved_metadata.st_mode)
                or not _is_within(resolved, blobs_root)
            ):
                raise LocalAnalysisCanaryError(
                    "Whisper snapshot symlink未绑定同repo blobs普通文件"
                )
            rows.append(
                {
                    "relative_path": relative,
                    "kind": "symlink",
                    "target": target_text,
                    "resolved_path": str(resolved),
                    "sha256": _sha256_file(resolved),
                    "byte_size": resolved_metadata.st_size,
                    "inode": resolved_metadata.st_ino,
                    "nlink": resolved_metadata.st_nlink,
                }
            )
            continue
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise LocalAnalysisCanaryError(
                "Whisper snapshot包含非私有普通文件或未知节点"
            )
        rows.append(
            {
                "relative_path": relative,
                "kind": "regular",
                "sha256": _sha256_file(lexical),
                "byte_size": metadata.st_size,
                "inode": metadata.st_ino,
                "nlink": metadata.st_nlink,
            }
        )
    if not rows:
        raise LocalAnalysisCanaryError("Whisper snapshot文件清单为空")
    return {
        "repo_root": str(repo_root.resolve()),
        "blobs_root": str(blobs_root),
        "files": len(rows),
        "rows_sha256": _json_sha256(rows),
        "rows": rows,
    }


def _local_tools(*, require_whisper: bool) -> Mapping[str, Any]:
    binary = media_module.ocr_binary_path().resolve()
    _private_file(binary, label="OCR binary")
    if not os.access(binary, os.X_OK):
        raise LocalAnalysisCanaryError(f"OCR binary不可执行：{binary}")
    result: dict[str, Any] = {
        "ocr_binary": {
            "path": str(binary),
            "sha256": _sha256_file(binary),
            "byte_size": binary.stat().st_size,
        }
    }
    for executable_name in ("ffmpeg", "ffprobe"):
        resolved = shutil.which(executable_name)
        if resolved is None:
            raise LocalAnalysisCanaryError(f"缺少本地{executable_name} binary")
        executable = Path(resolved).resolve()
        metadata = _private_file(executable, label=f"{executable_name} binary")
        if not os.access(executable, os.X_OK):
            raise LocalAnalysisCanaryError(f"{executable_name} binary不可执行")
        result[executable_name] = {
            "path": str(executable),
            "sha256": _sha256_file(executable),
            "byte_size": metadata.st_size,
        }
    if require_whisper:
        try:
            model = media_module.pinned_whisper_model_path(
                local_files_only=True
            ).resolve()
        except Exception as exc:
            raise LocalAnalysisCanaryError(
                "冻结Whisper模型未在本机缓存，禁止远程下载"
            ) from exc
        _private_directory(model, label="本地Whisper模型")
        config = media_module.load_media_config()["asr"]
        result["whisper"] = {
            "path": str(model),
            "model_id": str(config["model_id"]),
            "revision": str(config["model_revision"]),
            "inventory": _whisper_model_inventory(model),
        }
    return result


def _database_identity(path: Path) -> Mapping[str, Any]:
    metadata = _private_file(path, label="数据库clone")
    return {
        "path": str(path),
        "sha256": _sha256_file(path),
        "byte_size": metadata.st_size,
        "inode": metadata.st_ino,
        "nlink": metadata.st_nlink,
    }


def _validate_step3_batch_files(
    completion: Mapping[str, Any],
    *,
    batches_root: Path,
    receipts_total: int,
    contract: Mapping[str, Any],
    contract_sha256: str,
) -> Mapping[str, Any]:
    batch_chain = completion.get("batch_chain")
    receipt_evidence = completion.get("receipt_evidence")
    chain_fields = [
        "batch_index",
        "intent_file_sha256",
        "receipt_file_sha256",
        "cleanup_rounds",
        "cleanup_file_chain_sha256",
    ]
    evidence_fields = [
        "batch_index",
        "artifacts_evidence_sha256",
        "critical_evidence_sha256",
        "output_inventory_sha256",
    ]
    if not isinstance(batch_chain, Mapping) or not isinstance(
        receipt_evidence, Mapping
    ):
        raise LocalAnalysisCanaryError("Step3 completion缺少batch/receipt证据链")
    chain_rows = batch_chain.get("rows")
    evidence_rows = receipt_evidence.get("rows")
    if (
        batch_chain.get("fields") != chain_fields
        or not isinstance(chain_rows, list)
        or len(chain_rows) != receipts_total
        or batch_chain.get("rows_sha256") != _compact_json_sha256(chain_rows)
        or receipt_evidence.get("fields") != evidence_fields
        or not isinstance(evidence_rows, list)
        or len(evidence_rows) != receipts_total
        or receipt_evidence.get("rows_sha256")
        != _compact_json_sha256(evidence_rows)
        or receipt_evidence.get("receipt_files_sha256")
        != batch_chain.get("rows_sha256")
    ):
        raise LocalAnalysisCanaryError("Step3 batch/receipt证据链形状或SHA漂移")
    target_ids = contract.get("target_ids")
    initial_database = contract.get("database")
    provider_usage = contract.get("provider_usage")
    if (
        not isinstance(target_ids, list)
        or not all(type(value) is int and value > 0 for value in target_ids)
        or len(target_ids) != len(set(target_ids))
        or not isinstance(initial_database, Mapping)
        or not isinstance(provider_usage, Mapping)
    ):
        raise LocalAnalysisCanaryError("Step3 run contract缺少batch链基线")
    intent_fields = {
        "version",
        "batch_index",
        "content_ids",
        "content_ids_sha256",
        "contract_sha256",
        "before_database",
        "previous_receipt_sha256",
    }
    receipt_fields = {
        "version",
        "batch_index",
        "content_ids",
        "content_ids_sha256",
        "intent_sha256",
        "recovered_content_ids",
        "processed_content_ids",
        "interrupted_slot_recovery",
        "raw_application_recovery",
        "output_cleanup",
        "apply",
        "artifacts",
        "output_inventory",
        "critical_unchanged",
        "after_database",
        "elapsed_seconds",
        "disk",
    }
    allowed_names: set[str] = set()
    target_id_set = set(target_ids)
    completed_ids: set[int] = set()
    previous_database: Mapping[str, Any] = initial_database
    previous_receipt_sha256: str | None = None
    for index, row in enumerate(chain_rows, start=1):
        if not isinstance(row, list) or len(row) != len(chain_fields):
            raise LocalAnalysisCanaryError("Step3 batch chain row形状漂移")
        if int(row[0]) != index:
            raise LocalAnalysisCanaryError("Step3 batch chain index不连续")
        intent_path = batches_root / f"batch-{index:06d}.intent.json"
        receipt_path = batches_root / f"batch-{index:06d}.receipt.json"
        for path, label in (
            (intent_path, "Step3 batch intent"),
            (receipt_path, "Step3 batch receipt"),
        ):
            _private_file(path, label=label)
            allowed_names.add(path.name)
        intent_sha = _sha256_file(intent_path)
        receipt_sha = _sha256_file(receipt_path)
        if row[1] != intent_sha or row[2] != receipt_sha:
            raise LocalAnalysisCanaryError("Step3 batch intent/receipt文件SHA漂移")
        intent = _read_json(intent_path, label="Step3 batch intent")
        receipt = _read_json(receipt_path, label="Step3 batch receipt")
        raw_content_ids = intent.get("content_ids")
        if not isinstance(raw_content_ids, list) or not all(
            type(value) is int and value > 0 for value in raw_content_ids
        ):
            raise LocalAnalysisCanaryError("Step3 batch intent content IDs形状漂移")
        content_ids = list(raw_content_ids)
        raw_recovered = receipt.get("recovered_content_ids")
        raw_processed = receipt.get("processed_content_ids")
        if (
            not isinstance(raw_recovered, list)
            or not isinstance(raw_processed, list)
            or not all(
                type(value) is int and value > 0
                for value in [*raw_recovered, *raw_processed]
            )
        ):
            raise LocalAnalysisCanaryError("Step3 batch receipt content IDs形状漂移")
        recovered = set(raw_recovered)
        processed = set(raw_processed)
        applied = receipt.get("apply")
        artifacts = receipt.get("artifacts")
        after_database = receipt.get("after_database")
        if not isinstance(applied, Mapping) or not isinstance(artifacts, Mapping):
            raise LocalAnalysisCanaryError(
                "Step3 batch receipt apply/artifact证据无效"
            )
        try:
            applied_provider_calls = int(applied["provider_calls"])
            applied_processed = int(applied["processed"])
            artifact_contents = int(artifacts["contents"])
        except (KeyError, TypeError, ValueError) as exc:
            raise LocalAnalysisCanaryError(
                "Step3 batch receipt apply/artifact证据无效"
            ) from exc
        if (
            set(intent) != intent_fields
            or set(receipt) != receipt_fields
            or intent.get("version") != 1
            or receipt.get("version") != 1
            or intent.get("batch_index") != index
            or receipt.get("batch_index") != index
            or content_ids != sorted(set(content_ids))
            or not set(content_ids).issubset(target_id_set)
            or completed_ids.intersection(content_ids)
            or intent.get("content_ids_sha256")
            != _compact_json_sha256(content_ids)
            or intent.get("contract_sha256") != contract_sha256
            or intent.get("before_database") != previous_database
            or intent.get("previous_receipt_sha256")
            != previous_receipt_sha256
            or receipt.get("content_ids") != content_ids
            or receipt.get("content_ids_sha256")
            != intent.get("content_ids_sha256")
            or receipt.get("intent_sha256") != intent_sha
            or len(recovered) != len(raw_recovered)
            or len(processed) != len(raw_processed)
            or recovered.intersection(processed)
            or recovered.union(processed) != set(content_ids)
            or not isinstance(applied, Mapping)
            or applied.get("status") != "succeeded"
            or applied_provider_calls != 0
            or applied_processed != len(processed)
            or applied.get("processed_ids_sha256")
            != _compact_json_sha256(sorted(processed))
            or applied.get("provider_usage_before") != provider_usage
            or applied.get("provider_usage_after") != provider_usage
            or not isinstance(artifacts, Mapping)
            or artifact_contents != len(content_ids)
            or not isinstance(receipt.get("critical_unchanged"), Mapping)
            or not isinstance(receipt.get("output_inventory"), Mapping)
            or not isinstance(receipt.get("interrupted_slot_recovery"), list)
            or not isinstance(receipt.get("raw_application_recovery"), list)
            or not isinstance(receipt.get("disk"), Mapping)
            or not isinstance(after_database, Mapping)
        ):
            raise LocalAnalysisCanaryError("Step3 batch intent/receipt语义链漂移")
        expected_database_path = initial_database.get("path")
        if (
            after_database.get("path") != expected_database_path
            or after_database.get("device") != initial_database.get("device")
            or after_database.get("inode") != initial_database.get("inode")
            or after_database.get("nlink") != 1
            or type(after_database.get("bytes")) is not int
            or int(after_database["bytes"]) <= 0
            or not isinstance(after_database.get("sha256"), str)
            or re.fullmatch(r"[0-9a-f]{64}", after_database["sha256"])
            is None
        ):
            raise LocalAnalysisCanaryError("Step3 batch receipt数据库证据非法")
        cleanup_rows: list[list[Any]] = []
        cleanup_summary_rows: list[list[Any]] = []
        cleanup_rounds = int(row[3])
        if cleanup_rounds < 0:
            raise LocalAnalysisCanaryError("Step3 cleanup rounds无效")
        previous_cleanup_receipt_sha256: str | None = None
        for cleanup_round in range(1, cleanup_rounds + 1):
            prefix = f"batch-{index:06d}.output-cleanup-{cleanup_round:06d}"
            cleanup_intent = batches_root / f"{prefix}.intent.json"
            cleanup_receipt = batches_root / f"{prefix}.receipt.json"
            for path in (cleanup_intent, cleanup_receipt):
                _private_file(path, label="Step3 cleanup record")
                allowed_names.add(path.name)
            cleanup_intent_sha = _sha256_file(cleanup_intent)
            cleanup_receipt_sha = _sha256_file(cleanup_receipt)
            cleanup_intent_body = _read_json(
                cleanup_intent, label="Step3 cleanup intent"
            )
            cleanup_receipt_body = _read_json(
                cleanup_receipt, label="Step3 cleanup receipt"
            )
            files = cleanup_intent_body.get("files")
            file_rows = files.get("rows") if isinstance(files, Mapping) else None
            if (
                set(cleanup_intent_body)
                != {
                    "version",
                    "batch_index",
                    "cleanup_round",
                    "batch_intent_sha256",
                    "contract_sha256",
                    "previous_cleanup_receipt_sha256",
                    "files",
                }
                or cleanup_intent_body.get("version") != 1
                or cleanup_intent_body.get("batch_index") != index
                or cleanup_intent_body.get("cleanup_round") != cleanup_round
                or cleanup_intent_body.get("batch_intent_sha256") != intent_sha
                or cleanup_intent_body.get("contract_sha256")
                != contract_sha256
                or cleanup_intent_body.get("previous_cleanup_receipt_sha256")
                != previous_cleanup_receipt_sha256
                or not isinstance(files, Mapping)
                or files.get("fields") != ["root", "path", "bytes", "sha256"]
                or not isinstance(file_rows, list)
                or not file_rows
                or files.get("rows_sha256") != _compact_json_sha256(file_rows)
                or set(cleanup_receipt_body)
                != {
                    "version",
                    "status",
                    "batch_index",
                    "cleanup_round",
                    "batch_intent_sha256",
                    "cleanup_intent_sha256",
                    "previous_cleanup_receipt_sha256",
                    "files",
                }
                or cleanup_receipt_body.get("version") != 1
                or cleanup_receipt_body.get("status") != "succeeded"
                or cleanup_receipt_body.get("batch_index") != index
                or cleanup_receipt_body.get("cleanup_round") != cleanup_round
                or cleanup_receipt_body.get("batch_intent_sha256") != intent_sha
                or cleanup_receipt_body.get("cleanup_intent_sha256")
                != cleanup_intent_sha
                or cleanup_receipt_body.get("previous_cleanup_receipt_sha256")
                != previous_cleanup_receipt_sha256
                or cleanup_receipt_body.get("files") != files
            ):
                raise LocalAnalysisCanaryError("Step3 cleanup语义链漂移")
            cleanup_rows.append(
                [cleanup_round, cleanup_intent_sha, cleanup_receipt_sha]
            )
            cleanup_summary_rows.append(
                [
                    cleanup_round,
                    cleanup_intent_sha,
                    cleanup_receipt_sha,
                    len(file_rows),
                    files["rows_sha256"],
                ]
            )
            previous_cleanup_receipt_sha256 = cleanup_receipt_sha
        if row[4] != _compact_json_sha256(cleanup_rows):
            raise LocalAnalysisCanaryError("Step3 cleanup文件链SHA漂移")
        expected_cleanup = (
            None
            if not cleanup_summary_rows
            else {
                "rounds": len(cleanup_summary_rows),
                "files": sum(int(value[3]) for value in cleanup_summary_rows),
                "round_fields": [
                    "cleanup_round",
                    "intent_file_sha256",
                    "receipt_file_sha256",
                    "files",
                    "rows_sha256",
                ],
                "round_rows": cleanup_summary_rows,
                "round_rows_sha256": _compact_json_sha256(
                    cleanup_summary_rows
                ),
            }
        )
        evidence_row = evidence_rows[index - 1]
        expected_evidence_row = [
            index,
            _compact_json_sha256(artifacts),
            _compact_json_sha256(receipt["critical_unchanged"]),
            _compact_json_sha256(receipt["output_inventory"]),
        ]
        if (
            receipt.get("output_cleanup") != expected_cleanup
            or evidence_row != expected_evidence_row
        ):
            raise LocalAnalysisCanaryError("Step3 receipt completion证据漂移")
        completed_ids.update(content_ids)
        previous_database = after_database
        previous_receipt_sha256 = receipt_sha
    actual_names: set[str] = set()
    for path in batches_root.iterdir():
        if path.is_symlink() or not path.is_file():
            raise LocalAnalysisCanaryError("Step3 batches包含非普通文件")
        _private_file(path, label="Step3 batch record")
        actual_names.add(path.name)
    if actual_names != allowed_names:
        raise LocalAnalysisCanaryError("Step3 batches文件全集与completion链不一致")
    if (
        completed_ids != target_id_set
        or previous_database != completion.get("database")
    ):
        raise LocalAnalysisCanaryError("Step3 batch链未精确闭合completion终态")
    return {
        "batch_chain_rows_sha256": batch_chain["rows_sha256"],
        "receipt_evidence_rows_sha256": receipt_evidence["rows_sha256"],
        "receipt_files_sha256": receipt_evidence["receipt_files_sha256"],
        "files": len(allowed_names),
    }


def _step3_source_completion_evidence(
    paths: CanaryPaths,
    *,
    content_ids: Sequence[int],
    expected_source_db_sha256: str,
    expected_source_completion_sha256: str,
) -> Mapping[str, Any]:
    _require_clean_database(paths.source_database)
    completion_metadata = _private_file(
        paths.source_completion, label="Step3 completion"
    )
    try:
        completion = json.loads(paths.source_completion.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LocalAnalysisCanaryError("Step3 completion不是合法JSON") from exc
    completion_sha256 = _sha256_file(paths.source_completion)
    source_sha256 = _sha256_file(paths.source_database)
    if not re.fullmatch(r"[0-9a-f]{64}", expected_source_db_sha256) or (
        source_sha256 != expected_source_db_sha256
    ):
        raise LocalAnalysisCanaryError("Step3源数据库未命中外部expected SHA256")
    if not re.fullmatch(r"[0-9a-f]{64}", expected_source_completion_sha256) or (
        completion_sha256 != expected_source_completion_sha256
    ):
        raise LocalAnalysisCanaryError("Step3 completion未命中外部expected SHA256")
    if not isinstance(completion, Mapping) or completion.get("status") != "succeeded":
        raise LocalAnalysisCanaryError("Step3 completion不是succeeded终态")
    database = completion.get("database")
    nested_database = (
        completion.get("completion", {}).get("database")
        if isinstance(completion.get("completion"), Mapping)
        else None
    )
    if not isinstance(database, Mapping) or database != nested_database:
        raise LocalAnalysisCanaryError("Step3 completion数据库证据不一致")
    source_metadata = _private_file(paths.source_database, label="Step3源数据库")
    expected = {
        "path": str(paths.source_database),
        "sha256": source_sha256,
        "bytes": source_metadata.st_size,
        "inode": source_metadata.st_ino,
        "nlink": source_metadata.st_nlink,
    }
    for key, value in expected.items():
        if database.get(key) != value:
            raise LocalAnalysisCanaryError(
                f"Step3 completion与源数据库{key}证据漂移"
            )
    completed = completion.get("completion")
    if not isinstance(completed, Mapping):
        raise LocalAnalysisCanaryError("Step3 completion数量证据缺失")
    try:
        completed_count = int(completed["completed"])
        ready_count = int(completed["ready"])
        target_count = int(completion["target_count"])
        receipts_total = int(completion["receipts_total"])
    except (KeyError, TypeError, ValueError) as exc:
        raise LocalAnalysisCanaryError("Step3 completion数量证据无效") from exc
    if completed_count != target_count or ready_count != 0 or receipts_total <= 0:
        raise LocalAnalysisCanaryError("Step3 completion数量或ready终态不完整")
    raw_run_root = completion.get("run_root")
    if not isinstance(raw_run_root, str) or not raw_run_root:
        raise LocalAnalysisCanaryError("Step3 completion缺少run_root")
    run_root = Path(os.path.abspath(raw_run_root)).resolve()
    _assert_no_symlink_components(run_root, label="Step3 run_root")
    _private_directory(run_root, label="Step3 run_root")
    if paths.source_completion.parent != run_root:
        raise LocalAnalysisCanaryError("Step3 completion不位于其冻结run_root")
    step3_contract_path = run_root / "run-contract.json"
    step3_contract_metadata = _private_file(
        step3_contract_path, label="Step3 run contract"
    )
    step3_contract_sha256 = _sha256_file(step3_contract_path)
    if (
        not isinstance(completion.get("contract_sha256"), str)
        or completion["contract_sha256"] != step3_contract_sha256
    ):
        raise LocalAnalysisCanaryError("Step3 completion未绑定当前run contract")
    step3_contract = _read_json(step3_contract_path, label="Step3 run contract")
    target_ids = step3_contract.get("target_ids")
    if (
        not isinstance(target_ids, list)
        or not all(isinstance(value, int) and value > 0 for value in target_ids)
        or len(target_ids) != target_count
        or int(step3_contract.get("target_count", -1)) != target_count
    ):
        raise LocalAnalysisCanaryError("Step3 run contract target_ids形状或数量漂移")
    target_ids_sha256 = _sha256_bytes(
        json.dumps(
            target_ids,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    if step3_contract.get("target_ids_sha256") != target_ids_sha256:
        raise LocalAnalysisCanaryError("Step3 run contract target_ids SHA漂移")
    target_id_set = set(target_ids)
    missing_ids = [value for value in content_ids if value not in target_id_set]
    if missing_ids:
        raise LocalAnalysisCanaryError(
            f"explicit IDs不属于Step3冻结target_ids：{missing_ids}"
        )
    target_contract = step3_contract.get("target_contract")
    if not isinstance(target_contract, Mapping):
        raise LocalAnalysisCanaryError("Step3 run contract缺少逐内容target合同")
    target_fields = target_contract.get("fields")
    target_rows = target_contract.get("rows")
    if (
        target_fields != list(STEP3_TARGET_CONTRACT_FIELDS)
        or not isinstance(target_rows, list)
        or len(target_rows) != target_count
        or target_contract.get("rows_sha256")
        != _compact_json_sha256(target_rows)
    ):
        raise LocalAnalysisCanaryError("Step3 target合同形状/数量/SHA漂移")
    explicit_set = set(content_ids)
    explicit_target_rows: list[list[Any]] = []
    seen_target_ids: set[int] = set()
    for raw_row in target_rows:
        if not isinstance(raw_row, list) or len(raw_row) != len(target_fields):
            raise LocalAnalysisCanaryError("Step3 target合同row形状漂移")
        row_content_id = int(raw_row[0])
        if row_content_id in seen_target_ids:
            raise LocalAnalysisCanaryError("Step3 target合同content ID重复")
        seen_target_ids.add(row_content_id)
        if row_content_id in explicit_set:
            explicit_target_rows.append(list(raw_row))
    if seen_target_ids != target_id_set or {int(row[0]) for row in explicit_target_rows} != explicit_set:
        raise LocalAnalysisCanaryError("Step3 target合同未精确覆盖冻结target IDs")
    explicit_target_rows.sort(key=lambda row: content_ids.index(int(row[0])))
    media_root_value = step3_contract.get("media_root")
    derived_root_value = step3_contract.get("derived_raw_root")
    if not isinstance(media_root_value, str) or not isinstance(
        derived_root_value, str
    ):
        raise LocalAnalysisCanaryError("Step3 run contract缺少物理输出根")
    step3_media_root = Path(os.path.abspath(media_root_value)).resolve()
    step3_derived_raw_root = Path(os.path.abspath(derived_root_value)).resolve()
    for root, label in (
        (step3_media_root, "Step3 media_root"),
        (step3_derived_raw_root, "Step3 derived_raw_root"),
    ):
        _assert_no_symlink_components(root, label=label)
        _private_directory(root, label=label)
    batches_root = run_root / "batches"
    _private_directory(batches_root, label="Step3 batches root")
    batch_evidence = _validate_step3_batch_files(
        completion,
        batches_root=batches_root,
        receipts_total=receipts_total,
        contract=step3_contract,
        contract_sha256=step3_contract_sha256,
    )
    return {
        "path": str(paths.source_completion),
        "sha256": completion_sha256,
        "byte_size": completion_metadata.st_size,
        "contract": {
            "path": str(step3_contract_path),
            "sha256": step3_contract_sha256,
            "byte_size": step3_contract_metadata.st_size,
            "target_count": target_count,
            "target_ids_sha256": target_ids_sha256,
            "target_contract_rows_sha256": target_contract["rows_sha256"],
            "target_contract_fields": list(target_fields),
            "explicit_target_rows": explicit_target_rows,
            "explicit_target_rows_sha256": _compact_json_sha256(
                explicit_target_rows
            ),
            "media_root": str(step3_media_root),
            "derived_raw_root": str(step3_derived_raw_root),
        },
        "explicit_ids_membership_sha256": _json_sha256(list(content_ids)),
        "target_count": target_count,
        "receipts_total": receipts_total,
        "batch_evidence": batch_evidence,
        "database": dict(database),
    }


def _paid_refresh_module() -> ModuleType:
    return importlib.import_module("scripts.run_paid_source_refresh_canary")


def _source_completion_evidence(
    paths: CanaryPaths,
    *,
    content_ids: Sequence[int],
    expected_source_db_sha256: str,
    expected_source_completion_sha256: str,
) -> Mapping[str, Any]:
    """Dispatch typed source completions without weakening the Step3 validator."""

    _private_file(paths.source_completion, label="source completion")
    try:
        header = json.loads(paths.source_completion.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LocalAnalysisCanaryError("source completion不是合法JSON") from exc
    if not isinstance(header, Mapping):
        raise LocalAnalysisCanaryError("source completion不是JSON object")
    completion_kind = header.get("completion_kind")
    if completion_kind is None:
        return _step3_source_completion_evidence(
            paths,
            content_ids=content_ids,
            expected_source_db_sha256=expected_source_db_sha256,
            expected_source_completion_sha256=expected_source_completion_sha256,
        )
    paid = _paid_refresh_module()
    if completion_kind != paid.COMPLETION_KIND:
        raise LocalAnalysisCanaryError(
            f"source completion_kind不受支持：{completion_kind!r}"
        )
    try:
        return paid.validate_completion_for_local_analysis(
            source_db_path=paths.source_database,
            source_completion_path=paths.source_completion,
            expected_source_db_sha256=expected_source_db_sha256,
            expected_source_completion_sha256=expected_source_completion_sha256,
            content_ids=content_ids,
        )
    except paid.PaidSourceRefreshError as exc:
        raise LocalAnalysisCanaryError(f"paid source completion阻断：{exc}") from exc


def _require_paid_source_handoff_fresh(
    source_evidence: Mapping[str, Any],
) -> None:
    paid = _paid_refresh_module()
    if source_evidence.get("completion_kind") != paid.COMPLETION_KIND:
        return
    try:
        paid._require_handoff_fresh(
            source_evidence.get("completed_at"),
            maximum_age_seconds=source_evidence.get(
                "max_handoff_age_seconds"
            ),
        )
    except paid.PaidSourceRefreshError as exc:
        raise LocalAnalysisCanaryError(f"paid source首次handoff阻断：{exc}") from exc


def _copy_source_database(
    paths: CanaryPaths,
    *,
    source_evidence: Mapping[str, Any],
) -> None:
    if os.path.lexists(paths.database):
        raise LocalAnalysisCanaryError("work database在O_EXCL复制前已存在")
    if os.path.lexists(paths.copy_partial):
        raise LocalAnalysisCanaryError("partial database在O_EXCL复制前已存在")
    source_flags = os.O_RDONLY
    target_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        source_flags |= os.O_NOFOLLOW
        target_flags |= os.O_NOFOLLOW
    source_descriptor = os.open(paths.source_database, source_flags)
    try:
        target_descriptor = os.open(paths.copy_partial, target_flags, 0o600)
    except BaseException:
        os.close(source_descriptor)
        raise
    with (
        os.fdopen(source_descriptor, "rb") as source,
        os.fdopen(target_descriptor, "wb") as target,
    ):
        for block in iter(lambda: source.read(1024 * 1024), b""):
            target.write(block)
        target.flush()
        os.fsync(target.fileno())
    _fsync_directory(paths.database.parent)
    source = source_evidence["database"]
    copied = _database_identity(paths.copy_partial)
    if (
        copied["sha256"] != source["sha256"]
        or copied["byte_size"] != source["bytes"]
        or copied["nlink"] != 1
        or copied["inode"] == source["inode"]
    ):
        raise LocalAnalysisCanaryError("Step3源数据库未形成独立精确work copy")
    os.replace(paths.copy_partial, paths.database)
    _fsync_directory(paths.database.parent)


def _is_exact_source_prefix(candidate: Path, source: Path) -> bool:
    candidate_metadata = _private_file(candidate, label="待恢复partial database")
    source_metadata = _private_file(source, label="Step3源数据库")
    if candidate_metadata.st_size > source_metadata.st_size:
        return False
    remaining = candidate_metadata.st_size
    with candidate.open("rb") as candidate_handle, source.open("rb") as source_handle:
        while remaining:
            size = min(1024 * 1024, remaining)
            candidate_block = candidate_handle.read(size)
            source_block = source_handle.read(size)
            if len(candidate_block) != size or candidate_block != source_block:
                return False
            remaining -= size
        return candidate_handle.read(1) == b""


def _recover_owned_partial_copy(
    paths: CanaryPaths, *, source_evidence: Mapping[str, Any]
) -> None:
    candidates = [
        path for path in (paths.database, paths.copy_partial) if os.path.lexists(path)
    ]
    if len(candidates) > 1:
        raise LocalAnalysisCanaryError("work database与partial database同时存在")
    if not candidates:
        return
    candidate = candidates[0]
    source = source_evidence["database"]
    metadata = _private_file(candidate, label="待恢复partial database")
    if os.path.samefile(candidate, paths.source_database):
        raise LocalAnalysisCanaryError("partial database与Step3源数据库同inode")
    if (
        metadata.st_size == int(source["bytes"])
        and _sha256_file(candidate) == source["sha256"]
        and candidate == paths.database
    ):
        return
    if not _is_exact_source_prefix(candidate, paths.source_database):
        raise LocalAnalysisCanaryError(
            "已登记partial database不是Step3源数据库精确前缀"
        )
    candidate.unlink()
    _fsync_directory(candidate.parent)


def _validate_step3_separation(
    paths: CanaryPaths, source_evidence: Mapping[str, Any]
) -> None:
    step3_contract = source_evidence["contract"]
    protected_roots = (
        Path(str(step3_contract["media_root"])).resolve(),
        Path(str(step3_contract["derived_raw_root"])).resolve(),
        Path(str(source_evidence["path"])).resolve().parent,
        Path(str(source_evidence["database"]["path"])).resolve().parent,
    )
    candidates = (
        paths.database,
        paths.media_root,
        paths.fingerprint_root,
        paths.run_root,
    )
    for candidate in candidates:
        for protected in protected_roots:
            if _overlap(candidate, protected):
                raise LocalAnalysisCanaryError(
                    f"Step4输出不得位于Step3源证据树或其父域：{candidate}"
                )


def _validate_source_separation(
    paths: CanaryPaths, source_evidence: Mapping[str, Any]
) -> None:
    if source_evidence.get("completion_kind") is None:
        _validate_step3_separation(paths, source_evidence)
        return
    paid = _paid_refresh_module()
    if source_evidence.get("completion_kind") != paid.COMPLETION_KIND:
        raise LocalAnalysisCanaryError("source evidence completion_kind漂移")
    base_source = source_evidence.get("base_source")
    if not isinstance(base_source, Mapping):
        raise LocalAnalysisCanaryError("paid source evidence缺少Step3 base_source")
    _validate_step3_separation(paths, base_source)
    contract = source_evidence.get("contract")
    if not isinstance(contract, Mapping):
        raise LocalAnalysisCanaryError("paid source evidence缺少contract")
    protected_roots = (
        Path(str(contract["raw_root"])).resolve(),
        Path(str(contract["media_root"])).resolve(),
        Path(str(contract["run_root"])).resolve(),
        paths.source_database.parent,
        paths.source_completion.parent,
    )
    candidates = (
        paths.database,
        paths.media_root,
        paths.fingerprint_root,
        paths.run_root,
    )
    for candidate in candidates:
        for protected in protected_roots:
            if _overlap(candidate, protected):
                raise LocalAnalysisCanaryError(
                    f"Step4输出不得位于paid source证据树或其父域：{candidate}"
                )


def _disk_capacity(paths: CanaryPaths) -> Mapping[str, Any]:
    minimum_bytes = max(1024**3, 2 * MAX_DOWNLOAD_BYTES)
    minimum_inodes = 10_000
    rows: list[Mapping[str, Any]] = []
    seen_devices: set[int] = set()
    for parent in (
        paths.database.parent,
        paths.media_root.parent,
        paths.fingerprint_root.parent,
        paths.run_root.parent,
    ):
        metadata = parent.stat()
        if metadata.st_dev in seen_devices:
            continue
        seen_devices.add(metadata.st_dev)
        values = os.statvfs(parent)
        available_bytes = int(values.f_bavail * values.f_frsize)
        available_inodes = int(values.f_favail)
        row = {
            "device": metadata.st_dev,
            "anchor": str(parent),
            "available_bytes": available_bytes,
            "available_inodes": available_inodes,
        }
        rows.append(row)
        if available_bytes < minimum_bytes:
            raise LocalAnalysisCanaryError(
                f"Step4设备可用空间不足：{parent} {available_bytes}<{minimum_bytes}"
            )
        if available_inodes >= 0 and available_inodes < minimum_inodes:
            raise LocalAnalysisCanaryError(
                f"Step4设备可用inode不足：{parent} {available_inodes}<{minimum_inodes}"
            )
    return {
        "minimum_bytes_per_device": minimum_bytes,
        "minimum_inodes_per_device": minimum_inodes,
        "devices": rows,
    }


def _copy_intent_value(
    paths: CanaryPaths,
    *,
    content_ids: Sequence[int],
    source_evidence: Mapping[str, Any],
    expected_source_db_sha256: str,
    expected_source_completion_sha256: str,
) -> Mapping[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "operation": "copy-step3-source-to-analysis-work",
        "source_database": source_evidence["database"],
        "source_completion_sha256": expected_source_completion_sha256,
        "expected_source_database_sha256": expected_source_db_sha256,
        "work_database_path": str(paths.database),
        "content_ids": list(content_ids),
        "content_ids_sha256": _json_sha256(list(content_ids)),
    }


def _ensure_work_copy(
    paths: CanaryPaths,
    *,
    content_ids: Sequence[int],
    source_evidence: Mapping[str, Any],
    expected_source_db_sha256: str,
    expected_source_completion_sha256: str,
) -> None:
    copy_intent = _copy_intent_value(
        paths,
        content_ids=content_ids,
        source_evidence=source_evidence,
        expected_source_db_sha256=expected_source_db_sha256,
        expected_source_completion_sha256=expected_source_completion_sha256,
    )
    copy_intent_sha256 = _write_json(
        paths.copy_intent, copy_intent, immutable=True
    )
    _recover_owned_partial_copy(paths, source_evidence=source_evidence)
    if not paths.database.exists():
        _copy_source_database(paths, source_evidence=source_evidence)
    else:
        work = _database_identity(paths.database)
        source = source_evidence["database"]
        if (
            work["sha256"] != source["sha256"]
            or work["byte_size"] != source["bytes"]
            or work["nlink"] != 1
            or work["inode"] == source["inode"]
        ):
            raise LocalAnalysisCanaryError(
                "无contract的既有work database不是精确source copy"
            )
    _require_clean_database(paths.database)
    copy_receipt = {
        "schema_version": SCHEMA_VERSION,
        "operation": "copy-step3-source-to-analysis-work",
        "status": "succeeded",
        "copy_intent_sha256": copy_intent_sha256,
        "source_database": source_evidence["database"],
        "work_database": _database_identity(paths.database),
    }
    _write_json(paths.copy_receipt, copy_receipt, immutable=True)


def _validate_copy_records(paths: CanaryPaths, contract: Mapping[str, Any]) -> None:
    copy_intent = _read_json(paths.copy_intent, label="database copy intent")
    copy_receipt = _read_json(paths.copy_receipt, label="database copy receipt")
    if copy_receipt.get("status") != "succeeded":
        raise LocalAnalysisCanaryError("database copy receipt不是成功终态")
    if copy_receipt.get("copy_intent_sha256") != _sha256_file(paths.copy_intent):
        raise LocalAnalysisCanaryError("database copy receipt未绑定copy intent")
    if copy_intent.get("work_database_path") != str(paths.database):
        raise LocalAnalysisCanaryError("database copy intent路径漂移")
    if copy_intent.get("source_database") != contract.get("source_database"):
        raise LocalAnalysisCanaryError("database copy intent源证据漂移")
    frozen = contract.get("copy_records")
    expected = {
        "intent_sha256": _sha256_file(paths.copy_intent),
        "receipt_sha256": _sha256_file(paths.copy_receipt),
    }
    if frozen != expected:
        raise LocalAnalysisCanaryError("run contract未精确绑定database copy记录")
    receipt_work = copy_receipt.get("work_database")
    database = contract.get("database")
    if receipt_work != database:
        raise LocalAnalysisCanaryError("database copy receipt未绑定contract基线DB")


def _existing_analysis_counts(
    connection: sqlite3.Connection, content_ids: Sequence[int]
) -> Mapping[str, int]:
    placeholders = ",".join("?" for _ in content_ids)
    artifact_placeholders = ",".join("?" for _ in GENERATED_ARTIFACT_TYPES)
    return {
        "slots": int(
            connection.execute(
                "SELECT COUNT(*) FROM media_processing_slots WHERE content_id IN ("
                + placeholders
                + ")",
                content_ids,
            ).fetchone()[0]
        ),
        "artifacts": int(
            connection.execute(
                "SELECT COUNT(*) FROM evidence_artifacts WHERE content_id IN ("
                + placeholders
                + ") AND artifact_type IN ("
                + artifact_placeholders
                + ")",
                (*content_ids, *sorted(GENERATED_ARTIFACT_TYPES)),
            ).fetchone()[0]
        ),
        "evaluations": int(
            connection.execute(
                "SELECT COUNT(*) FROM evaluation_versions WHERE content_id IN ("
                + placeholders
                + ") AND invalidated_at IS NULL",
                content_ids,
            ).fetchone()[0]
        ),
        "fingerprints": int(
            connection.execute(
                "SELECT COUNT(*) FROM duplicate_fingerprints WHERE content_id IN ("
                + placeholders
                + ")",
                content_ids,
            ).fetchone()[0]
        ),
    }


def _build_contract(
    paths: CanaryPaths,
    content_ids: Sequence[int],
    *,
    source_evidence: Mapping[str, Any],
) -> Mapping[str, Any]:
    with closing(_immutable_connection(paths.database)) as connection:
        existing = _existing_analysis_counts(connection, content_ids)
        if any(existing.values()):
            raise LocalAnalysisCanaryError(
                f"首次canary只接受尚未分析的explicit IDs：{existing}"
            )
        sources = _completion_source_snapshots(
            connection, content_ids, source_evidence
        )
        protected = _protected_snapshot(connection, content_ids)
        baseline_targets = _target_snapshot(connection, content_ids)
        baseline_target_rows = _target_rows(connection, content_ids)
        baseline_sqlite_sequence = _sqlite_sequence_snapshot(connection)
        active_release = connection.execute(
            "SELECT * FROM evaluation_releases WHERE status='active'"
        ).fetchone()
        if active_release is None:
            raise LocalAnalysisCanaryError("clone缺少active evaluation release")
        baseline_artifacts = [
            int(row[0])
            for row in connection.execute(
                "SELECT id FROM evidence_artifacts WHERE content_id IN ("
                + ",".join("?" for _ in content_ids)
                + ") ORDER BY id",
                content_ids,
            )
        ]
    require_whisper = any(
        source["artifact_body"]["media_kind"] == "video" for source in sources
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "database": _database_identity(paths.database),
        "copy_records": {
            "intent_sha256": _sha256_file(paths.copy_intent),
            "receipt_sha256": _sha256_file(paths.copy_receipt),
        },
        "source_database": source_evidence["database"],
        "source_completion": source_evidence,
        "media_root": str(paths.media_root),
        "fingerprint_root": str(paths.fingerprint_root),
        "run_root": str(paths.run_root),
        "content_ids": list(content_ids),
        "content_ids_sha256": _json_sha256(list(content_ids)),
        "code": _code_snapshot(),
        "tools": _local_tools(require_whisper=require_whisper),
        "sources": sources,
        "sources_sha256": _json_sha256(sources),
        "active_release": dict(active_release),
        "protected": protected,
        "protected_sha256": _json_sha256(protected),
        "baseline_targets": baseline_targets,
        "baseline_target_rows": baseline_target_rows,
        "baseline_target_rows_sha256": _json_sha256(baseline_target_rows),
        "baseline_sqlite_sequence": baseline_sqlite_sequence,
        "baseline_artifact_ids": baseline_artifacts,
        "maximum_download_bytes": MAX_DOWNLOAD_BYTES,
        "maximum_video_duration_seconds": MAX_VIDEO_DURATION_SECONDS,
    }


def _process_identity(pid: int) -> str | None:
    if pid <= 0:
        return None
    completed = subprocess.run(
        ["ps", "-p", str(pid), "-o", "lstart="],
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )
    value = completed.stdout.strip()
    if completed.returncode != 0 or not value:
        return None
    return _sha256_bytes(value.encode("utf-8"))


def _current_owner() -> Mapping[str, Any]:
    pid = os.getpid()
    identity = _process_identity(pid)
    if identity is None:
        raise LocalAnalysisCanaryError("无法冻结controller进程身份")
    return {"pid": pid, "process_identity_sha256": identity}


def _validate_sidecar_recovery_records(
    paths: CanaryPaths, content_ids: Sequence[int]
) -> None:
    contract = _read_json(paths.contract, label="sidecar recovery contract")
    intent = _read_json(paths.intent, label="sidecar recovery intent")
    if contract.get("schema_version") != SCHEMA_VERSION:
        raise LocalAnalysisCanaryError("sidecar recovery contract schema不匹配")
    if list(contract.get("content_ids") or []) != list(content_ids):
        raise LocalAnalysisCanaryError("sidecar recovery content IDs漂移")
    database = contract.get("database")
    if not isinstance(database, Mapping) or database.get("path") != str(paths.database):
        raise LocalAnalysisCanaryError("sidecar recovery数据库路径漂移")
    metadata = _private_file(paths.database, label="sidecar recovery数据库")
    if int(database.get("inode") or -1) != metadata.st_ino or metadata.st_nlink != 1:
        raise LocalAnalysisCanaryError("sidecar recovery数据库身份漂移")
    if intent.get("contract_sha256") != _sha256_file(paths.contract):
        raise LocalAnalysisCanaryError("sidecar recovery intent未绑定contract")
    if list(intent.get("content_ids") or []) != list(content_ids):
        raise LocalAnalysisCanaryError("sidecar recovery intent IDs漂移")
    if intent.get("before_database") != database:
        raise LocalAnalysisCanaryError("sidecar recovery intent基线数据库漂移")
    owner = intent.get("owner")
    if not isinstance(owner, Mapping) or not owner.get("process_identity_sha256"):
        raise LocalAnalysisCanaryError("sidecar recovery intent缺少owner身份")


def _validate_frozen_inputs(
    paths: CanaryPaths,
    contract: Mapping[str, Any],
    content_ids: Sequence[int],
    *,
    expected_source_db_sha256: str,
    expected_source_completion_sha256: str,
) -> None:
    if list(contract.get("code") or []) != _code_snapshot():
        raise LocalAnalysisCanaryError("controller依赖代码SHA漂移")
    current_source_evidence = _source_completion_evidence(
        paths,
        content_ids=content_ids,
        expected_source_db_sha256=expected_source_db_sha256,
        expected_source_completion_sha256=expected_source_completion_sha256,
    )
    _validate_source_separation(paths, current_source_evidence)
    if contract.get("source_completion") != current_source_evidence:
        raise LocalAnalysisCanaryError("Step3源数据库或completion证据漂移")
    if contract.get("source_database") != current_source_evidence.get("database"):
        raise LocalAnalysisCanaryError("Step3源数据库合同漂移")
    frozen_sources = contract.get("sources")
    if not isinstance(frozen_sources, list):
        raise LocalAnalysisCanaryError("run contract sources缺失")
    require_whisper = any(
        isinstance(source, Mapping)
        and isinstance(source.get("artifact_body"), Mapping)
        and source["artifact_body"].get("media_kind") == "video"
        for source in frozen_sources
    )
    if contract.get("tools") != _local_tools(require_whisper=require_whisper):
        raise LocalAnalysisCanaryError("本地模型或OCR工具漂移")
    with closing(_immutable_connection(paths.database)) as connection:
        current_sources = _completion_source_snapshots(
            connection, content_ids, current_source_evidence
        )
    if current_sources != frozen_sources:
        raise LocalAnalysisCanaryError("content/source_group/media_source证据漂移")


def _validate_contract(
    paths: CanaryPaths,
    contract: Mapping[str, Any],
    content_ids: Sequence[int],
    *,
    expected_source_db_sha256: str,
    expected_source_completion_sha256: str,
) -> None:
    if contract.get("schema_version") != SCHEMA_VERSION:
        raise LocalAnalysisCanaryError("run contract schema不匹配")
    expected_paths = {
        "media_root": str(paths.media_root),
        "fingerprint_root": str(paths.fingerprint_root),
        "run_root": str(paths.run_root),
    }
    if any(contract.get(key) != value for key, value in expected_paths.items()):
        raise LocalAnalysisCanaryError("run contract输出路径漂移")
    if list(contract.get("content_ids") or []) != list(content_ids):
        raise LocalAnalysisCanaryError("explicit content IDs与run contract漂移")
    database = contract.get("database")
    if not isinstance(database, Mapping) or database.get("path") != str(paths.database):
        raise LocalAnalysisCanaryError("run contract数据库路径漂移")
    metadata = _private_file(paths.database, label="数据库clone")
    if int(database.get("inode") or -1) != metadata.st_ino or metadata.st_nlink != 1:
        raise LocalAnalysisCanaryError("数据库clone身份漂移")
    _validate_copy_records(paths, contract)
    _validate_frozen_inputs(
        paths,
        contract,
        content_ids,
        expected_source_db_sha256=expected_source_db_sha256,
        expected_source_completion_sha256=expected_source_completion_sha256,
    )
    with closing(_immutable_connection(paths.database)) as connection:
        current_protected = _protected_snapshot(connection, content_ids)
        if current_protected != contract.get("protected"):
            raise LocalAnalysisCanaryError("clone保护表或非目标行发生变化")


def _inventory(root: Path) -> Mapping[str, Any]:
    rows: list[Mapping[str, Any]] = []
    for path in sorted(root.rglob("*"), key=lambda item: str(item.relative_to(root))):
        relative = str(path.relative_to(root))
        metadata = path.lstat()
        if path.is_symlink():
            raise LocalAnalysisCanaryError(f"输出根包含symlink：{path}")
        if stat.S_ISDIR(metadata.st_mode):
            continue
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise LocalAnalysisCanaryError(f"输出根包含非私有普通文件：{path}")
        if path.name.startswith(".") or path.name.endswith((".tmp", ".candidate")):
            raise LocalAnalysisCanaryError(f"输出根包含临时或未知文件：{path}")
        rows.append(
            {
                "path": relative,
                "byte_size": metadata.st_size,
                "sha256": _sha256_file(path),
            }
        )
    return {"files": len(rows), "rows_sha256": _json_sha256(rows), "rows": rows}


def _validate_run_root(paths: CanaryPaths, *, allow_atomic_temps: bool) -> None:
    allowed = {
        paths.copy_intent.name,
        paths.copy_receipt.name,
        paths.contract.name,
        paths.intent.name,
        paths.receipt.name,
        paths.state.name,
        paths.running_recovery.name,
        paths.output_recovery.name,
        paths.network_ledger.name,
        paths.progress.name,
    }
    atomic_temps = {f".{name}.tmp" for name in allowed}
    for path in paths.run_root.iterdir():
        if path.name in allowed:
            _private_file(path, label="运行记录")
            continue
        if allow_atomic_temps and path.name in atomic_temps:
            _private_file(path, label="运行记录临时文件")
            continue
        raise LocalAnalysisCanaryError(f"运行记录根包含未知文件：{path}")


def _is_within(path: Path, root: Path) -> bool:
    path_key = _filesystem_comparison_key(path)
    root_key = _filesystem_comparison_key(root)
    return path_key[: len(root_key)] == root_key


def _validate_generated_artifacts(
    connection: sqlite3.Connection,
    *,
    contract: Mapping[str, Any],
    paths: CanaryPaths,
    content_ids: Sequence[int],
) -> tuple[dict[int, dict[str, sqlite3.Row]], set[Path], set[Path]]:
    baseline = {int(value) for value in contract.get("baseline_artifact_ids") or []}
    placeholders = ",".join("?" for _ in content_ids)
    rows = connection.execute(
        "SELECT * FROM evidence_artifacts WHERE content_id IN ("
        + placeholders
        + ") ORDER BY id",
        content_ids,
    ).fetchall()
    by_content: dict[int, dict[str, sqlite3.Row]] = {
        int(content_id): {} for content_id in content_ids
    }
    source_by_id = {
        int(source["content"]["id"]): source for source in contract["sources"]
    }
    media_files: set[Path] = set()
    fingerprint_files: set[Path] = set()
    for row in rows:
        if int(row["id"]) in baseline:
            continue
        content_id = int(row["content_id"])
        artifact_type = str(row["artifact_type"])
        if artifact_type not in GENERATED_ARTIFACT_TYPES:
            raise LocalAnalysisCanaryError(
                f"目标content出现非白名单新增artifact：{artifact_type}"
            )
        if str(row["status"] or "") != "available":
            raise LocalAnalysisCanaryError(
                f"新增{artifact_type} artifact不是available终态"
            )
        if artifact_type in by_content[content_id]:
            raise LocalAnalysisCanaryError(
                f"content {content_id} 出现重复{artifact_type} artifact"
            )
        path = _resolve_artifact_path(str(row["local_path"]))
        metadata = _private_file(path, label="新增分析artifact")
        if not (
            _is_within(path, paths.media_root)
            or _is_within(path, paths.fingerprint_root)
        ):
            raise LocalAnalysisCanaryError(f"新增artifact越过隔离输出根：{path}")
        if int(row["byte_size"] or -1) != metadata.st_size:
            raise LocalAnalysisCanaryError(f"新增artifact byte_size不一致：{path}")
        if str(row["sha256"] or "") != _sha256_file(path):
            raise LocalAnalysisCanaryError(f"新增artifact SHA不一致：{path}")
        if artifact_type == "media_manifest":
            source = source_by_id[content_id]
            expected_manifest_path = (
                paths.media_root
                / str(source["content"]["link_id"])
                / "downloads"
                / _source_image_download_binding(source)
                / "images"
                / "manifest.json"
            ).resolve()
            if path != expected_manifest_path:
                raise LocalAnalysisCanaryError(
                    "image manifest artifact路径未绑定download binding"
                )
            try:
                artifact_metadata = json.loads(str(row["metadata_json"] or "{}"))
            except json.JSONDecodeError as exc:
                raise LocalAnalysisCanaryError(
                    "image manifest artifact metadata不是合法JSON"
                ) from exc
            expected_metadata = {
                "source_count": len(_source_image_groups(source)),
                "source_url_count": len(_source_urls(source)),
                "source_sha256": str(source["artifact_body"]["source_sha256"]),
                "image_groups_sha256": str(source["image_groups_sha256"]),
                "download_binding_sha256": _source_image_download_binding(
                    source
                ),
            }
            if _canonical_bytes(artifact_metadata) != _canonical_bytes(
                expected_metadata
            ):
                raise LocalAnalysisCanaryError(
                    "image manifest artifact metadata未绑定逻辑图组"
                )
        by_content[content_id][artifact_type] = row
        if _is_within(path, paths.media_root):
            media_files.add(path)
        else:
            fingerprint_files.add(path)
    return by_content, media_files, fingerprint_files


def _json_artifact(row: sqlite3.Row, *, label: str) -> tuple[Path, Mapping[str, Any]]:
    path = _resolve_artifact_path(str(row["local_path"]))
    try:
        body = json.loads(path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LocalAnalysisCanaryError(f"{label}不是合法JSON") from exc
    if not isinstance(body, Mapping):
        raise LocalAnalysisCanaryError(f"{label}必须是JSON object")
    return path, body


def _source_image_groups(source: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    if str(source["artifact_body"]["media_kind"]) != "image":
        raise LocalAnalysisCanaryError("非图片source不得读取逻辑图组")
    groups = source.get("image_groups")
    if not isinstance(groups, list) or not groups:
        raise LocalAnalysisCanaryError("contract缺少冻结逻辑图组")
    if source.get("image_groups_sha256") != media_module.image_groups_sha256(
        groups
    ):
        raise LocalAnalysisCanaryError("contract逻辑图组SHA漂移")
    try:
        normalized = media_module.validate_frozen_image_groups(
            _source_urls(source),
            groups,
            platform=str(source["content"]["platform"]),
        )
    except media_module.MediaProcessingError as exc:
        raise LocalAnalysisCanaryError(
            f"contract冻结逻辑图组无效：{exc}"
        ) from exc
    if normalized != groups:
        raise LocalAnalysisCanaryError("contract冻结逻辑图组规范化漂移")
    return normalized


def _source_image_download_binding(source: Mapping[str, Any]) -> str:
    groups = _source_image_groups(source)
    return media_module.image_download_binding_sha256(
        str(source["artifact_body"]["source_sha256"]),
        media_module.image_groups_sha256(groups),
    )


def _image_manifest_output_paths(
    manifest_path: Path,
    body: Mapping[str, Any],
    *,
    source: Mapping[str, Any],
    media_root: Path,
) -> set[Path]:
    expected_groups = _source_image_groups(source)
    source_urls = _source_urls(source)
    body_groups = body.get("groups")
    frames = body.get("frames")
    image_paths = body.get("image_paths")
    if (
        set(body)
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
        or body.get("schema_version") != media_module.IMAGE_MANIFEST_VERSION
        or body.get("status") != "complete"
        or type(body.get("source_url_count")) is not int
        or body["source_url_count"] != len(source_urls)
        or type(body.get("source_count")) is not int
        or body["source_count"] != len(expected_groups)
        or type(body.get("source_sha256")) is not str
        or body.get("source_sha256")
        != str(source["artifact_body"]["source_sha256"])
        or type(body.get("image_groups_sha256")) is not str
        or body.get("image_groups_sha256")
        != str(source["image_groups_sha256"])
        or type(body.get("download_binding_sha256")) is not str
        or body.get("download_binding_sha256")
        != _source_image_download_binding(source)
        or not isinstance(image_paths, list)
        or not isinstance(frames, list)
        or not isinstance(body_groups, list)
        or len(image_paths) != len(expected_groups)
        or len(frames) != len(expected_groups)
        or len(body_groups) != len(expected_groups)
    ):
        raise LocalAnalysisCanaryError("image download manifest形状或计数不精确")
    expected_files = [
        manifest_path.parent / f"image-{index:03d}.bin"
        for index in range(len(expected_groups))
    ]
    expected_paths = [media_module._relative(path) for path in expected_files]
    if image_paths != expected_paths:
        raise LocalAnalysisCanaryError("image manifest paths未绑定逻辑图组顺序")
    result = {manifest_path}
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
    for index, (expected_group, body_group, frame, frame_path) in enumerate(
        zip(expected_groups, body_groups, frames, expected_files, strict=True)
    ):
        if (
            not isinstance(body_group, Mapping)
            or set(body_group) != group_keys
            or type(body_group.get("group_index")) is not int
            or body_group["group_index"] != index
            or _canonical_bytes(body_group.get("identity"))
            != _canonical_bytes(expected_group["identity"])
        ):
            raise LocalAnalysisCanaryError("image manifest逻辑图组identity漂移")
        candidates = list(expected_group["candidates"])
        source_order = sorted(
            candidates, key=lambda candidate: int(candidate["source_index"])
        )
        expected_source_hashes = [
            str(candidate["url_sha256"]) for candidate in source_order
        ]
        attempts = body_group.get("attempts")
        if (
            body_group.get("source_url_sha256s") != expected_source_hashes
            or not isinstance(attempts, list)
            or not attempts
            or len(attempts) > len(candidates)
        ):
            raise LocalAnalysisCanaryError("image manifest候选或attempt集合漂移")
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
                raise LocalAnalysisCanaryError("image manifest attempt未绑定候选顺序")
            outcome = str(attempt["outcome"])
            if outcome == "request_failed":
                failed_sha256 = attempt.get("response_sha256")
                failed_byte_size = attempt.get("byte_size")
                if (
                    type(failed_byte_size) is not int
                    or failed_byte_size < 0
                    or not (
                        (failed_sha256 is None and failed_byte_size == 0)
                        or (
                            type(failed_sha256) is str
                            and re.fullmatch(r"[0-9a-f]{64}", failed_sha256)
                            is not None
                            and (
                                failed_byte_size > 0
                                or failed_sha256 == _sha256_bytes(b"")
                            )
                        )
                    )
                    or not isinstance(attempt.get("error"), str)
                    or not str(attempt["error"])
                ):
                    raise LocalAnalysisCanaryError("image request_failed attempt证据无效")
            elif outcome == "unsupported_image":
                if (
                    re.fullmatch(
                        r"[0-9a-f]{64}", str(attempt.get("response_sha256") or "")
                    )
                    is None
                    or int(attempt.get("byte_size") or 0) <= 0
                    or attempt.get("error") != "unsupported_image"
                ):
                    raise LocalAnalysisCanaryError("image unsupported attempt证据无效")
            elif (
                attempt_index != len(attempts) - 1
                or attempt.get("error") is not None
                or re.fullmatch(
                    r"[0-9a-f]{64}", str(attempt.get("response_sha256") or "")
                )
                is None
                or int(attempt.get("byte_size") or 0) <= 0
            ):
                raise LocalAnalysisCanaryError("image selected attempt证据无效")
        selected = attempts[-1]
        if (
            selected.get("outcome") != "selected"
            or body_group.get("selected_url_sha256") != selected["url_sha256"]
            or body_group.get("selected_response_sha256")
            != selected["response_sha256"]
            or type(body_group.get("selected_byte_size")) is not int
            or type(selected.get("byte_size")) is not int
            or body_group["selected_byte_size"] != selected["byte_size"]
            or type(body_group.get("image_path")) is not str
            or body_group.get("image_path") != expected_paths[index]
        ):
            raise LocalAnalysisCanaryError("image manifest selected响应投影漂移")
        if (
            not isinstance(frame, Mapping)
            or set(frame) != {"path", "sha256"}
            or type(frame.get("path")) is not str
            or type(frame.get("sha256")) is not str
        ):
            raise LocalAnalysisCanaryError("image manifest frame形状不精确")
        metadata = _private_file(frame_path, label="image download file")
        if (
            frame.get("path") != expected_paths[index]
            or not _is_within(frame_path, media_root)
            or not media_module._valid_image(frame_path)
            or str(frame.get("sha256")) != _sha256_file(frame_path)
            or str(frame["sha256"]) != body_group["selected_response_sha256"]
            or metadata.st_size != int(body_group["selected_byte_size"])
        ):
            raise LocalAnalysisCanaryError("image download file未绑定selected响应")
        result.add(frame_path)
    return result


def _manifest_output_paths(
    row: sqlite3.Row,
    *,
    media_kind: str,
    media_root: Path,
    source: Mapping[str, Any] | None = None,
) -> set[Path]:
    manifest_path, body = _json_artifact(row, label=f"{media_kind} manifest")
    result = {manifest_path}
    if media_kind == "image":
        if source is None:
            raise LocalAnalysisCanaryError("image manifest缺少冻结source合同")
        return _image_manifest_output_paths(
            manifest_path,
            body,
            source=source,
            media_root=media_root,
        )
    if set(body) != {"status", "duration_seconds", "frames", "contact_sheet"}:
        raise LocalAnalysisCanaryError("frames manifest形状不精确")
    frames = body.get("frames")
    if body.get("status") != "success" or not isinstance(frames, list) or not frames:
        raise LocalAnalysisCanaryError("frames manifest不是完整成功终态")
    for frame in frames:
        if not isinstance(frame, Mapping) or set(frame) != {"path", "sha256"}:
            raise LocalAnalysisCanaryError("frames manifest行形状不精确")
        frame_path = _resolve_artifact_path(str(frame["path"]))
        _private_file(frame_path, label="extracted frame")
        if not _is_within(frame_path, media_root):
            raise LocalAnalysisCanaryError("extracted frame越过隔离媒体根")
        if str(frame["sha256"]) != _sha256_file(frame_path):
            raise LocalAnalysisCanaryError("extracted frame SHA漂移")
        result.add(frame_path)
    contact_sheet = body.get("contact_sheet")
    if contact_sheet is not None:
        contact_path = _resolve_artifact_path(str(contact_sheet))
        _private_file(contact_path, label="contact sheet")
        if not _is_within(contact_path, media_root):
            raise LocalAnalysisCanaryError("contact sheet越过隔离媒体根")
        result.add(contact_path)
    return result


def _expected_inventory_rows(root: Path, files: set[Path]) -> list[Mapping[str, Any]]:
    rows: list[Mapping[str, Any]] = []
    for path in sorted(files, key=lambda item: str(item.relative_to(root))):
        metadata = _private_file(path, label="DB/manifest可达输出")
        if not _is_within(path, root):
            raise LocalAnalysisCanaryError("DB/manifest可达输出越过隔离根")
        rows.append(
            {
                "path": str(path.relative_to(root)),
                "byte_size": metadata.st_size,
                "sha256": _sha256_file(path),
            }
        )
    return rows


def _validate_download_provenance(
    *,
    content_id: int,
    source: Mapping[str, Any],
    artifacts: Mapping[str, sqlite3.Row],
    ledger: "_NetworkLedger",
) -> None:
    events = ledger.transcript(content_id)
    successful = [
        event
        for event in events
        if event.get("outcome") == "succeeded"
        and int(event.get("bytes") or 0) > 0
        and re.fullmatch(
            r"[0-9a-f]{64}", str(event.get("response_sha256") or "")
        )
    ]
    media_kind = str(source["artifact_body"]["media_kind"])
    if media_kind == "video":
        artifact = artifacts.get("media")
        if artifact is None:
            return
        matching = [
            event
            for event in successful
            if event["response_sha256"] == str(artifact["sha256"])
            and int(event["bytes"]) == int(artifact["byte_size"])
        ]
        if not matching:
            raise LocalAnalysisCanaryError(
                "video download artifact未绑定冻结URL响应体SHA"
            )
        return
    artifact = artifacts.get("media_manifest")
    if artifact is None:
        return
    body = _read_json(
        _resolve_artifact_path(str(artifact["local_path"])),
        label="image download manifest",
    )
    body_groups = body.get("groups")
    expected_groups = _source_image_groups(source)
    if not isinstance(body_groups, list) or len(body_groups) != len(expected_groups):
        raise LocalAnalysisCanaryError("image download provenance组数不精确")
    expected_events: list[Mapping[str, Any]] = []
    for expected_group, body_group in zip(
        expected_groups, body_groups, strict=True
    ):
        if not isinstance(body_group, Mapping):
            raise LocalAnalysisCanaryError("image download provenance组无效")
        attempts = body_group.get("attempts")
        candidates = expected_group["candidates"]
        if not isinstance(attempts, list) or not attempts:
            raise LocalAnalysisCanaryError("image download provenance attempts缺失")
        for attempt, candidate in zip(attempts, candidates, strict=False):
            url_sha256 = str(candidate["url_sha256"])
            outcome = str(attempt.get("outcome") or "")
            if outcome in {"selected", "unsupported_image"}:
                expected_events.append(
                    {
                        "url_sha256": url_sha256,
                        "outcome": "succeeded",
                        "response_sha256": attempt.get("response_sha256"),
                        "bytes": attempt.get("byte_size"),
                        "error_type": None,
                    }
                )
            elif outcome == "request_failed":
                expected_events.append(
                    {
                        "url_sha256": url_sha256,
                        "outcome": "failed",
                        "response_sha256": attempt.get("response_sha256"),
                        "bytes": attempt.get("byte_size"),
                        "error_type": attempt.get("error"),
                    }
                )
            else:
                raise LocalAnalysisCanaryError("image download attempt outcome漂移")
    if len(events) != len(expected_events):
        raise LocalAnalysisCanaryError(
            "image download manifest/network event不是一一双射"
        )
    for expected, event in zip(expected_events, events, strict=True):
        event_error = event.get("error")
        expected_error_type = expected["error_type"]
        if (
            event.get("url_sha256") != expected["url_sha256"]
            or event.get("outcome") != expected["outcome"]
            or event.get("response_sha256") != expected["response_sha256"]
            or event.get("bytes") != expected["bytes"]
            or (expected_error_type is None and event_error is not None)
            or (
                expected_error_type is not None
                and (
                    type(event_error) is not str
                    or event_error.split(":", 1)[0] != expected_error_type
                )
            )
        ):
            raise LocalAnalysisCanaryError(
                "image download manifest/network event顺序或响应投影漂移"
            )


def _validate_prewrite_outputs(
    paths: CanaryPaths,
    *,
    contract: Mapping[str, Any],
    content_ids: Sequence[int],
    completed_ids: Sequence[int],
    ledger: "_NetworkLedger",
    slot_attempt_expectations: Sequence[Mapping[str, Any]],
) -> None:
    media_inventory = _inventory(paths.media_root)
    fingerprint_inventory = _inventory(paths.fingerprint_root)
    expectations = _normalize_slot_attempt_expectations(
        slot_attempt_expectations,
        contract=contract,
        content_ids=content_ids,
    )
    completed = set(completed_ids)
    with closing(_immutable_connection(paths.database)) as connection:
        artifacts_by_content, media_files, fingerprint_files = (
            _validate_generated_artifacts(
                connection,
                contract=contract,
                paths=paths,
                content_ids=content_ids,
            )
        )
        release = connection.execute(
            "SELECT id,rule_version FROM evaluation_releases WHERE status='active'"
        ).fetchone()
        if release is None:
            raise LocalAnalysisCanaryError("写前检查缺少active release")
        source_by_id = {
            int(source["content"]["id"]): source for source in contract["sources"]
        }
        versions = media_module.processor_versions()
        for content_id in content_ids:
            source = source_by_id[content_id]
            kind = str(source["artifact_body"]["media_kind"])
            artifacts = artifacts_by_content[content_id]
            names = set(artifacts)
            prefixes = (
                [
                    set(),
                    {"media"},
                    {"media", "frames_manifest"},
                    {"media", "frames_manifest", "asr"},
                    {"media", "frames_manifest", "asr", "ocr"},
                    {
                        "media",
                        "frames_manifest",
                        "asr",
                        "ocr",
                        "duplicate_fingerprint",
                    },
                ]
                if kind == "video"
                else [
                    set(),
                    {"media_manifest"},
                    {"media_manifest", "ocr"},
                    {"media_manifest", "ocr", "duplicate_fingerprint"},
                ]
            )
            if names not in prefixes:
                raise LocalAnalysisCanaryError(
                    f"content {content_id} 写前artifact不是合法处理前缀"
                )
            full_names = prefixes[-1]
            if content_id in completed and names != full_names:
                raise LocalAnalysisCanaryError("durable progress对应artifact未完成")
            expected_versions = {
                "media": "provider-media-v8.0",
                "media_manifest": media_module.IMAGE_DOWNLOAD_VERSION,
                "frames_manifest": str(versions["frames"]),
                "asr": str(versions["asr"]),
                "ocr": str(versions["ocr"]),
                "duplicate_fingerprint": duplicates_module.FINGERPRINT_VERSION,
            }
            for name, row in artifacts.items():
                if str(row["processor_version"] or "") != expected_versions[name]:
                    raise LocalAnalysisCanaryError("写前artifact processor version漂移")
            download_name = "media" if kind == "video" else "media_manifest"
            if download_name in artifacts:
                _validate_download_provenance(
                    content_id=content_id,
                    source=source,
                    artifacts=artifacts,
                    ledger=ledger,
                )
            if "frames_manifest" in artifacts:
                media_files.update(
                    _manifest_output_paths(
                        artifacts["frames_manifest"],
                        media_kind="video",
                        media_root=paths.media_root,
                    )
                )
            if "media_manifest" in artifacts:
                media_files.update(
                    _manifest_output_paths(
                        artifacts["media_manifest"],
                        media_kind="image",
                        media_root=paths.media_root,
                        source=source,
                    )
                )
            slots = connection.execute(
                "SELECT * FROM media_processing_slots WHERE content_id=? ORDER BY id",
                (content_id,),
            ).fetchall()
            for slot in slots:
                processor_type = str(slot["processor_type"])
                status = str(slot["status"])
                expectation = expectations.get((content_id, processor_type))
                if status in {"running", "retryable_failed"}:
                    if expectation is None or int(slot["attempt_count"]) != int(
                        expectation["from_attempt_count"]
                    ):
                        raise LocalAnalysisCanaryError("写前未完成slot缺少attempt ledger")
                elif status != "succeeded":
                    raise LocalAnalysisCanaryError("写前slot状态不属于可恢复闭包")
                if status == "succeeded" and slot["output_artifact_id"] is None:
                    raise LocalAnalysisCanaryError("写前succeeded slot缺少artifact")
            active = connection.execute(
                "SELECT COUNT(*) FROM evaluation_versions "
                "WHERE content_id=? AND invalidated_at IS NULL",
                (content_id,),
            ).fetchone()[0]
            if active:
                if names - {"duplicate_fingerprint"} != full_names - {
                    "duplicate_fingerprint"
                }:
                    raise LocalAnalysisCanaryError("写前evaluation早于完整媒体证据")
                _validate_current_evaluation(
                    connection,
                    content_id=content_id,
                    release_id=str(release["id"]),
                    rule_version=str(release["rule_version"]),
                )
            if "duplicate_fingerprint" in names and not active:
                raise LocalAnalysisCanaryError("写前fingerprint缺少active evaluation")
    if media_inventory["rows"] != _expected_inventory_rows(paths.media_root, media_files):
        raise LocalAnalysisCanaryError("写前媒体输出不等于DB/manifest可达闭包")
    if fingerprint_inventory["rows"] != _expected_inventory_rows(
        paths.fingerprint_root, fingerprint_files
    ):
        raise LocalAnalysisCanaryError("写前指纹输出不等于DB可达闭包")


def _validate_content_processing(
    connection: sqlite3.Connection,
    *,
    source: Mapping[str, Any],
    artifacts: Mapping[str, sqlite3.Row],
    paths: CanaryPaths,
    slot_attempt_expectations: Mapping[tuple[int, str], Mapping[str, Any]],
) -> tuple[set[Path], set[Path]]:
    content_id = int(source["content"]["id"])
    media_kind = str(source["artifact_body"]["media_kind"])
    expected_artifact_types = (
        {"media", "frames_manifest", "asr", "ocr", "duplicate_fingerprint"}
        if media_kind == "video"
        else {"media_manifest", "ocr", "duplicate_fingerprint"}
    )
    if set(artifacts) != expected_artifact_types:
        raise LocalAnalysisCanaryError(
            f"content {content_id} 分析artifact集合不精确：{sorted(artifacts)}"
        )
    versions = media_module.processor_versions()
    expected_artifact_versions = (
        {
            "media": "provider-media-v8.0",
            "frames_manifest": str(versions["frames"]),
            "asr": str(versions["asr"]),
            "ocr": str(versions["ocr"]),
            "duplicate_fingerprint": duplicates_module.FINGERPRINT_VERSION,
        }
        if media_kind == "video"
        else {
            "media_manifest": media_module.IMAGE_DOWNLOAD_VERSION,
            "ocr": str(versions["ocr"]),
            "duplicate_fingerprint": duplicates_module.FINGERPRINT_VERSION,
        }
    )
    for artifact_type, version in expected_artifact_versions.items():
        if str(artifacts[artifact_type]["processor_version"] or "") != version:
            raise LocalAnalysisCanaryError(
                f"content {content_id} {artifact_type} processor version漂移"
            )
    expected_slot_types = (
        {"download", "frames", "asr", "ocr", "duplicate_fingerprint"}
        if media_kind == "video"
        else {"download", "ocr", "duplicate_fingerprint"}
    )
    slots = connection.execute(
        "SELECT * FROM media_processing_slots WHERE content_id=? ORDER BY id",
        (content_id,),
    ).fetchall()
    if len(slots) != len(expected_slot_types) or {
        str(row["processor_type"]) for row in slots
    } != expected_slot_types:
        raise LocalAnalysisCanaryError(
            f"content {content_id} 必需slot集合不精确"
        )
    slot_by_type = {str(row["processor_type"]): row for row in slots}
    media_artifact = artifacts["media" if media_kind == "video" else "media_manifest"]
    frames_artifact = artifacts.get("frames_manifest")
    ocr_source_artifact = (
        frames_artifact if media_kind == "video" else media_artifact
    )
    if ocr_source_artifact is None:
        raise LocalAnalysisCanaryError(
            f"content {content_id} OCR source artifact缺失"
        )
    with contextlib.suppress(Exception):
        _, fingerprint_sha = duplicates_module._current_source_state(
            connection, content_id
        )
    if "fingerprint_sha" not in locals():
        raise LocalAnalysisCanaryError(
            f"content {content_id} 无法重算duplicate fingerprint source"
        )
    expected_slots: dict[str, tuple[str, str, int]] = {
        "download": (
            (
                str(source["artifact_body"]["source_sha256"])
                if media_kind == "video"
                else _source_image_download_binding(source)
            ),
            (
                media_module.VIDEO_DOWNLOAD_VERSION
                if media_kind == "video"
                else media_module.IMAGE_DOWNLOAD_VERSION
            ),
            int(media_artifact["id"]),
        ),
        "ocr": (
            str(ocr_source_artifact["sha256"]),
            str(versions["ocr"]),
            int(artifacts["ocr"]["id"]),
        ),
        "duplicate_fingerprint": (
            str(fingerprint_sha),
            duplicates_module.FINGERPRINT_VERSION,
            int(artifacts["duplicate_fingerprint"]["id"]),
        ),
    }
    if media_kind == "video":
        expected_slots.update(
            {
                "frames": (
                    str(media_artifact["sha256"]),
                    str(versions["frames"]),
                    int(artifacts["frames_manifest"]["id"]),
                ),
                "asr": (
                    str(media_artifact["sha256"]),
                    str(versions["asr"]),
                    int(artifacts["asr"]["id"]),
                ),
            }
        )
    for processor_type, (source_sha, version, artifact_id) in expected_slots.items():
        row = slot_by_type[processor_type]
        attempt_expectation = slot_attempt_expectations.get(
            (content_id, processor_type)
        )
        expected_attempt_count = 1
        if attempt_expectation is not None:
            if (
                int(attempt_expectation["slot_id"]) != int(row["id"])
                or str(attempt_expectation["source_sha256"]) != source_sha
                or str(attempt_expectation["processor_version"]) != version
            ):
                raise LocalAnalysisCanaryError(
                    f"content {content_id} {processor_type} 恢复slot身份漂移"
                )
            expected_attempt_count = int(
                attempt_expectation["expected_attempt_count"]
            )
        if (
            str(row["status"]) != "succeeded"
            or str(row["source_sha256"]) != source_sha
            or str(row["processor_version"]) != version
            or int(row["output_artifact_id"] or -1) != artifact_id
            or int(row["attempt_count"] or 0) != expected_attempt_count
            or row["error_message"] not in (None, "")
        ):
            raise LocalAnalysisCanaryError(
                f"content {content_id} {processor_type} slot证据不精确"
            )
    fingerprint = connection.execute(
        "SELECT * FROM duplicate_fingerprints WHERE content_id=? "
        "AND fingerprint_version=? AND source_sha256=?",
        (content_id, duplicates_module.FINGERPRINT_VERSION, fingerprint_sha),
    ).fetchall()
    if (
        len(fingerprint) != 1
        or int(fingerprint[0]["artifact_id"]) != int(
            artifacts["duplicate_fingerprint"]["id"]
        )
    ):
        raise LocalAnalysisCanaryError(
            f"content {content_id} current fingerprint行不精确"
        )
    fingerprint_path, fingerprint_body = _json_artifact(
        artifacts["duplicate_fingerprint"], label="duplicate fingerprint artifact"
    )
    try:
        fingerprint_payload = json.loads(str(fingerprint[0]["payload_json"]))
    except json.JSONDecodeError as exc:
        raise LocalAnalysisCanaryError("duplicate fingerprint payload_json无效") from exc
    fingerprint_keys = {
        "schema_version",
        "fingerprint_version",
        "content_id",
        "source_sha256",
        "text_sha256",
        "media_sha256",
        "frame_phashes",
        "text_simhash",
        "asr_simhash",
        "ocr_simhash",
        "text_char_count",
        "asr_char_count",
        "ocr_char_count",
        "created_at",
    }
    compact_fingerprint_json = _canonical_bytes(fingerprint_body).decode(
        "utf-8"
    ).removesuffix("\n")
    fingerprint_row = fingerprint[0]
    if (
        not isinstance(fingerprint_payload, Mapping)
        or set(fingerprint_body) != fingerprint_keys
        or fingerprint_payload != fingerprint_body
        or str(fingerprint_row["payload_json"]) != compact_fingerprint_json
        or int(fingerprint_body.get("content_id") or -1) != content_id
        or fingerprint_body.get("fingerprint_version")
        != duplicates_module.FINGERPRINT_VERSION
        or fingerprint_body.get("source_sha256") != fingerprint_sha
        or fingerprint_row["text_sha256"] != fingerprint_body["text_sha256"]
        or str(fingerprint_row["media_sha256_json"])
        != _canonical_bytes(fingerprint_body["media_sha256"])
        .decode("utf-8")
        .removesuffix("\n")
        or str(fingerprint_row["frame_phashes_json"])
        != _canonical_bytes(fingerprint_body["frame_phashes"])
        .decode("utf-8")
        .removesuffix("\n")
        or fingerprint_row["text_simhash"] != fingerprint_body["text_simhash"]
        or fingerprint_row["asr_simhash"] != fingerprint_body["asr_simhash"]
        or fingerprint_row["ocr_simhash"] != fingerprint_body["ocr_simhash"]
        or int(fingerprint_row["text_char_count"])
        != int(fingerprint_body["text_char_count"])
        or int(fingerprint_row["asr_char_count"])
        != int(fingerprint_body["asr_char_count"])
        or int(fingerprint_row["ocr_char_count"])
        != int(fingerprint_body["ocr_char_count"])
        or str(fingerprint_row["created_at"]) != str(fingerprint_body["created_at"])
        or _resolve_artifact_path(
            str(artifacts["duplicate_fingerprint"]["local_path"])
        )
        != fingerprint_path
    ):
        raise LocalAnalysisCanaryError(
            f"content {content_id} duplicate fingerprint正文/DB语义不一致"
        )
    media_files = {
        _resolve_artifact_path(str(row["local_path"]))
        for name, row in artifacts.items()
        if name != "duplicate_fingerprint"
    }
    fingerprint_files = {
        _resolve_artifact_path(str(artifacts["duplicate_fingerprint"]["local_path"]))
    }
    if media_kind == "video":
        media_files.update(
            _manifest_output_paths(
                artifacts["frames_manifest"],
                media_kind="video",
                media_root=paths.media_root,
            )
        )
        _, asr_body = _json_artifact(artifacts["asr"], label="ASR artifact")
        if asr_body.get("status") not in {"success", "unavailable"}:
            raise LocalAnalysisCanaryError("ASR artifact不是允许终态")
    else:
        media_files.update(
            _manifest_output_paths(
                artifacts["media_manifest"],
                media_kind="image",
                media_root=paths.media_root,
                source=source,
            )
        )
    _, ocr_body = _json_artifact(artifacts["ocr"], label="OCR artifact")
    if ocr_body.get("status") != "success":
        raise LocalAnalysisCanaryError("OCR artifact不是success终态")
    return media_files, fingerprint_files


def _normalize_slot_attempt_expectations(
    rows: Sequence[Mapping[str, Any]],
    *,
    contract: Mapping[str, Any],
    content_ids: Sequence[int],
) -> dict[tuple[int, str], Mapping[str, Any]]:
    source_by_id = {
        int(source["content"]["id"]): source
        for source in contract.get("sources") or []
        if isinstance(source, Mapping) and isinstance(source.get("content"), Mapping)
    }
    target_ids = set(content_ids)
    normalized: dict[tuple[int, str], Mapping[str, Any]] = {}
    expected_keys = {
        "slot_id",
        "content_id",
        "source_sha256",
        "processor_type",
        "processor_version",
        "from_attempt_count",
        "expected_attempt_count",
    }
    for raw in rows:
        if not isinstance(raw, Mapping) or set(raw) != expected_keys:
            raise LocalAnalysisCanaryError("running recovery attempt证据形状不精确")
        row: dict[str, Any] = {
            "slot_id": int(raw["slot_id"]),
            "content_id": int(raw["content_id"]),
            "source_sha256": str(raw["source_sha256"]),
            "processor_type": str(raw["processor_type"]),
            "processor_version": str(raw["processor_version"]),
            "from_attempt_count": int(raw["from_attempt_count"]),
            "expected_attempt_count": int(raw["expected_attempt_count"]),
        }
        content_id = int(row["content_id"])
        processor_type = str(row["processor_type"])
        source = source_by_id.get(content_id)
        if content_id not in target_ids or source is None:
            raise LocalAnalysisCanaryError("running recovery attempt不属于当前targets")
        media_kind = str(source["artifact_body"]["media_kind"])
        allowed_types = (
            {"download", "frames", "asr", "ocr", "duplicate_fingerprint"}
            if media_kind == "video"
            else {"download", "ocr", "duplicate_fingerprint"}
        )
        if processor_type not in allowed_types:
            raise LocalAnalysisCanaryError("running recovery processor不属于目标媒体类型")
        if (
            int(row["slot_id"]) <= 0
            or int(row["from_attempt_count"]) <= 0
            or int(row["expected_attempt_count"])
            != int(row["from_attempt_count"]) + 1
            or int(row["expected_attempt_count"])
            > media_module.MAX_MEDIA_PROCESSING_ATTEMPTS
            or not re.fullmatch(r"[0-9a-f]{64}", str(row["source_sha256"]))
            or not str(row["processor_version"])
        ):
            raise LocalAnalysisCanaryError("running recovery attempt计数或身份无效")
        key = (content_id, processor_type)
        if key in normalized:
            raise LocalAnalysisCanaryError("running recovery attempt证据重复")
        normalized[key] = row
    return normalized


def _validate_current_evaluation(
    connection: sqlite3.Connection,
    *,
    content_id: int,
    release_id: str,
    rule_version: str,
) -> sqlite3.Row:
    try:
        artifacts, components, evidence_sha256 = (
            evaluation_module._current_evidence_state(
                connection, content_id, rule_version=rule_version
            )
        )
    except Exception as exc:
        raise LocalAnalysisCanaryError(
            f"content {content_id} 无法重算current evidence state"
        ) from exc
    rows = connection.execute(
        """
        SELECT ev.*,ee.content_id envelope_content_id
        FROM evaluation_versions ev
        JOIN evidence_envelopes ee ON ee.id=ev.evidence_envelope_id
        WHERE ev.content_id=? AND ev.invalidated_at IS NULL
        ORDER BY ev.id
        """,
        (content_id,),
    ).fetchall()
    if len(rows) != 1:
        raise LocalAnalysisCanaryError(
            f"content {content_id} current-evidence evaluation集合不精确"
        )
    evaluation = rows[0]
    envelope = connection.execute(
        "SELECT * FROM evidence_envelopes WHERE id=?",
        (int(evaluation["evidence_envelope_id"]),),
    ).fetchone()
    content = connection.execute(
        "SELECT * FROM content_items WHERE id=?",
        (content_id,),
    ).fetchone()
    if envelope is None or content is None:
        raise LocalAnalysisCanaryError(
            f"content {content_id} evaluation envelope/content缺失"
        )
    try:
        components_body = json.loads(str(envelope["components_json"]))
    except json.JSONDecodeError as exc:
        raise LocalAnalysisCanaryError("evaluation envelope components_json无效") from exc
    try:
        payload = json.loads(str(evaluation["payload_json"]))
    except json.JSONDecodeError as exc:
        raise LocalAnalysisCanaryError("evaluation payload_json无效") from exc
    payload_keys = {
        "evaluation_status",
        "evidence_level",
        "evidence_summary",
        "primary_selling_point_id",
        "selling_point_score",
        "selling_point_included",
        "content_direction",
        "content_automotive_score",
        "audience_automotive_score",
        "action_intent_score",
        "valid_unique_commenters",
        "acquisition_potential",
        "matches",
        "evaluation_source",
        "release_id",
    }
    if not isinstance(payload, Mapping) or set(payload) != payload_keys:
        raise LocalAnalysisCanaryError("evaluation payload形状不精确")
    payload_matches = payload.get("matches")
    if not isinstance(payload_matches, list) or len(payload_matches) > 3:
        raise LocalAnalysisCanaryError("evaluation payload matches形状无效")
    match_rows = connection.execute(
        """
        SELECT selling_point_code,scene,match_role,score,evidence_json
        FROM evaluation_matches WHERE evaluation_id=?
        ORDER BY CASE match_role WHEN 'primary' THEN 0 ELSE 1 END,rowid
        """,
        (int(evaluation["id"]),),
    ).fetchall()
    if len(match_rows) != len(payload_matches):
        raise LocalAnalysisCanaryError("evaluation matches数量与payload不一致")
    for index, (match, row) in enumerate(
        zip(payload_matches, match_rows, strict=True)
    ):
        try:
            evidence = json.loads(str(row["evidence_json"]))
        except json.JSONDecodeError as exc:
            raise LocalAnalysisCanaryError("evaluation match evidence无效") from exc
        if (
            not isinstance(match, Mapping)
            or evidence != match
            or str(row["selling_point_code"]) != str(match.get("id") or "")
            or str(row["scene"]) != str(match.get("scene") or "")
            or str(row["match_role"])
            != ("primary" if index == 0 else "secondary")
            or int(row["score"]) != int(match.get("score") or 0)
        ):
            raise LocalAnalysisCanaryError("evaluation match未精确投影payload")
    primary_match = payload_matches[0] if payload_matches else None
    expected_primary_code = (
        str(primary_match.get("id") or "")
        if isinstance(primary_match, Mapping)
        else ""
    )
    expected_primary_score = (
        int(primary_match.get("score") or 0)
        if isinstance(primary_match, Mapping)
        else None
    )
    audience_score, action_score, valid_commenters = (
        evaluation_module._comment_scores(connection, content_id)
    )
    asr = evaluation_module._read_json(artifacts["asr_path"])
    ocr = evaluation_module._read_json(artifacts["ocr_path"])
    body_text = "\n".join(
        value
        for value in (
            str(content["title"] or ""),
            str(content["body"] or ""),
        )
        if value
    )
    expected_evidence_level, expected_evidence_summary = (
        evaluation_module._evidence_level(
            content_type=str(content["content_type"]),
            text=body_text,
            media_path=artifacts["media_path"],
            asr=asr,
            ocr=ocr,
        )
    )
    component_columns = (
        "detail_raw_sha256",
        "text_sha256",
        "media_sha256",
        "asr_sha256",
        "ocr_sha256",
        "comments_version_sha256",
        "manual_evidence_sha256",
    )
    if (
        str(evaluation["release_id"]) != release_id
        or str(evaluation["rule_version"]) != rule_version
        or str(evaluation["evaluation_source"]) != "automatic"
        or str(evaluation["evaluation_status"]) != "evaluated"
        or str(evaluation["evidence_level"]) not in {"V2", "V3"}
        or str(evaluation["evidence_level"]) != expected_evidence_level
        or payload["evidence_summary"] != expected_evidence_summary
        or str(payload["evaluation_status"]) != str(evaluation["evaluation_status"])
        or str(payload["evidence_level"]) != str(evaluation["evidence_level"])
        or str(payload["evaluation_source"]) != str(evaluation["evaluation_source"])
        or str(payload["release_id"]) != str(evaluation["release_id"])
        or str(payload["primary_selling_point_id"] or "")
        != str(evaluation["primary_selling_point_code"] or "")
        or str(payload["primary_selling_point_id"] or "")
        != expected_primary_code
        or payload["selling_point_score"] != evaluation["selling_point_score"]
        or payload["selling_point_score"] != expected_primary_score
        or int(bool(payload["selling_point_included"]))
        != int(evaluation["selling_point_included"])
        or str(payload["content_direction"] or "")
        != str(evaluation["content_direction"] or "")
        or payload["content_automotive_score"]
        != evaluation["content_automotive_score"]
        or payload["audience_automotive_score"]
        != evaluation["audience_automotive_score"]
        or payload["audience_automotive_score"] != audience_score
        or payload["action_intent_score"] != action_score
        or int(payload["valid_unique_commenters"]) != valid_commenters
        or payload["acquisition_potential"]
        != evaluation["acquisition_potential_score"]
        or int(evaluation["envelope_content_id"]) != content_id
        or str(evaluation["evidence_sha256"]) != evidence_sha256
        or str(envelope["schema_version"]) != evaluation_module.EVIDENCE_VERSION
        or int(envelope["content_id"]) != content_id
        or str(envelope["evidence_sha256"]) != evidence_sha256
        or components_body != components
        or any(envelope[column] != components[column] for column in component_columns)
        or (
            rule_version == evaluation_module.V9_RULE_VERSION
            and (
                components["manual_evidence_sha256"] is not None
                or envelope["manual_evidence_sha256"] is not None
            )
        )
        or str(content["evaluation_content_direction"] or "")
        != str(evaluation["content_direction"] or "")
    ):
        raise LocalAnalysisCanaryError(
            f"content {content_id} evaluation未绑定current evidence精确状态"
        )
    return evaluation


def _validate_processed_results(
    paths: CanaryPaths,
    contract: Mapping[str, Any],
    content_ids: Sequence[int],
    results: Sequence[Mapping[str, Any]],
) -> None:
    if [int(result["content_id"]) for result in results] != list(content_ids):
        raise LocalAnalysisCanaryError("processed result顺序未绑定冻结IDs")
    with closing(_immutable_connection(paths.database)) as connection:
        release = connection.execute(
            "SELECT id,rule_version FROM evaluation_releases WHERE status='active'"
        ).fetchone()
        if release is None:
            raise LocalAnalysisCanaryError("processed result缺少active release")
        artifacts_by_content, _media_files, _fingerprint_files = (
            _validate_generated_artifacts(
                connection,
                contract=contract,
                paths=paths,
                content_ids=content_ids,
            )
        )
        source_by_id = {
            int(source["content"]["id"]): source for source in contract["sources"]
        }
        for result in results:
            content_id = int(result["content_id"])
            source = source_by_id[content_id]
            media_kind = str(source["artifact_body"]["media_kind"])
            artifacts = artifacts_by_content[content_id]
            expected_media_artifacts = (
                {
                    "media": int(artifacts["media"]["id"]),
                    "frames": int(artifacts["frames_manifest"]["id"]),
                    "asr": int(artifacts["asr"]["id"]),
                    "ocr": int(artifacts["ocr"]["id"]),
                }
                if media_kind == "video"
                else {
                    "media": int(artifacts["media_manifest"]["id"]),
                    "ocr": int(artifacts["ocr"]["id"]),
                }
            )
            expected_media = {
                "content_id": content_id,
                "status": "evidence_ready",
                "media_kind": media_kind,
                "artifacts": expected_media_artifacts,
            }
            if result["media"] != expected_media:
                raise LocalAnalysisCanaryError(
                    f"content {content_id} progress media结果未精确投影DB"
                )
            current_evaluation = _validate_current_evaluation(
                connection,
                content_id=content_id,
                release_id=str(release["id"]),
                rule_version=str(release["rule_version"]),
            )
            evaluation_result = result["evaluation"]
            if (
                int(evaluation_result["evaluation_id"])
                != int(current_evaluation["id"])
                or str(evaluation_result["evidence_sha256"])
                != str(current_evaluation["evidence_sha256"])
                or str(evaluation_result["evidence_level"])
                != str(current_evaluation["evidence_level"])
            ):
                raise LocalAnalysisCanaryError(
                    f"content {content_id} progress evaluation结果未精确投影DB"
                )
            try:
                _inputs, fingerprint_source_sha256 = (
                    duplicates_module._current_source_state(connection, content_id)
                )
            except Exception as exc:
                raise LocalAnalysisCanaryError(
                    f"content {content_id} 无法重算progress fingerprint source"
                ) from exc
            if (
                str(result["fingerprint_source_sha256"])
                != fingerprint_source_sha256
            ):
                raise LocalAnalysisCanaryError(
                    f"content {content_id} progress fingerprint结果未精确投影DB"
                )


def _validate_target_baseline_and_sequences(
    connection: sqlite3.Connection,
    *,
    contract: Mapping[str, Any],
    content_ids: Sequence[int],
    active_evaluation_ids: set[int],
) -> None:
    baseline = contract.get("baseline_target_rows")
    if (
        not isinstance(baseline, Mapping)
        or contract.get("baseline_target_rows_sha256") != _json_sha256(baseline)
    ):
        raise LocalAnalysisCanaryError("target baseline rows合同漂移")
    current = _target_rows(connection, content_ids)
    deltas: dict[str, list[Mapping[str, Any]]] = {}
    for table in sorted(MANAGED_TARGET_TABLES):
        baseline_rows = baseline.get(table)
        current_rows = current.get(table)
        if not isinstance(baseline_rows, list) or not isinstance(current_rows, list):
            raise LocalAnalysisCanaryError(f"target baseline缺少表：{table}")
        remaining = Counter(_canonical_bytes(row) for row in current_rows)
        for row in baseline_rows:
            key = _canonical_bytes(row)
            if remaining[key] <= 0:
                raise LocalAnalysisCanaryError(f"target baseline row被改写或删除：{table}")
            remaining[key] -= 1
        delta_keys = [key for key, count in remaining.items() for _ in range(count)]
        deltas[table] = [json.loads(key) for key in delta_keys]
    expected_slot_count = 0
    expected_artifact_count = 0
    for source in contract["sources"]:
        if source["artifact_body"]["media_kind"] == "video":
            expected_slot_count += 5
            expected_artifact_count += 5
        else:
            expected_slot_count += 3
            expected_artifact_count += 3
    if (
        len(deltas["media_processing_slots"]) != expected_slot_count
        or len(deltas["evidence_artifacts"]) != expected_artifact_count
        or len(deltas["evaluation_versions"]) != len(content_ids)
        or len(deltas["evidence_envelopes"]) != len(content_ids)
        or len(deltas["duplicate_fingerprints"]) != len(content_ids)
        or any(
            int(row["id"]) not in active_evaluation_ids
            for row in deltas["evaluation_versions"]
        )
        or any(
            int(row["evaluation_id"]) not in active_evaluation_ids
            for row in deltas["evaluation_matches"]
        )
    ):
        raise LocalAnalysisCanaryError("target managed rows出现非预期增删改")
    baseline_sequences = contract.get("baseline_sqlite_sequence")
    if not isinstance(baseline_sequences, Mapping):
        raise LocalAnalysisCanaryError("sqlite_sequence baseline合同缺失")
    current_sequences = _sqlite_sequence_snapshot(connection)
    managed_with_ids = MANAGED_TARGET_TABLES - {"evaluation_matches"}
    for table, sequence in current_sequences.items():
        baseline_sequence = int(baseline_sequences.get(table, 0))
        if table not in managed_with_ids:
            if table not in baseline_sequences or sequence != baseline_sequence:
                raise LocalAnalysisCanaryError("非managed sqlite_sequence发生变化")
            continue
        maximum_id = int(
            connection.execute(
                f"SELECT COALESCE(MAX(id),0) FROM {_quoted(table)}"
            ).fetchone()[0]
        )
        if sequence != max(baseline_sequence, maximum_id):
            raise LocalAnalysisCanaryError(f"managed sqlite_sequence非精确增量：{table}")
    if any(name not in current_sequences for name in baseline_sequences):
        raise LocalAnalysisCanaryError("sqlite_sequence baseline行消失")


def _validate_target_success(
    paths: CanaryPaths,
    contract: Mapping[str, Any],
    content_ids: Sequence[int],
    *,
    slot_attempt_expectations: Sequence[Mapping[str, Any]] = (),
    network_ledger: "_NetworkLedger | None" = None,
) -> Mapping[str, Any]:
    normalized_expectations = _normalize_slot_attempt_expectations(
        slot_attempt_expectations,
        contract=contract,
        content_ids=content_ids,
    )
    with closing(_immutable_connection(paths.database)) as connection:
        protected = _protected_snapshot(connection, content_ids)
        if protected != contract.get("protected"):
            raise LocalAnalysisCanaryError("成功后保护表或非目标行发生变化")
        sources = _completion_source_snapshots(
            connection, content_ids, contract["source_completion"]
        )
        if sources != contract.get("sources"):
            raise LocalAnalysisCanaryError("成功后source_group或media_source发生变化")
        release = connection.execute(
            "SELECT id,rule_version FROM evaluation_releases WHERE status='active'"
        ).fetchone()
        if release is None:
            raise LocalAnalysisCanaryError("成功后缺少active release")
        eligible = evaluation_selectors_module.formal_eligible_release_evaluations(
            connection, str(release["id"]), content_ids
        )
        if set(eligible) != set(content_ids):
            missing = sorted(set(content_ids) - set(eligible))
            raise LocalAnalysisCanaryError(
                f"目标evaluation未达到formal eligible：{missing}"
            )
        active_evaluation_ids: set[int] = set()
        for content_id in content_ids:
            current_evaluation = _validate_current_evaluation(
                connection,
                content_id=content_id,
                release_id=str(release["id"]),
                rule_version=str(release["rule_version"]),
            )
            active_evaluation_ids.add(int(current_evaluation["id"]))
        artifacts_by_content, media_files, fingerprint_files = (
            _validate_generated_artifacts(
                connection,
                contract=contract,
                paths=paths,
                content_ids=content_ids,
            )
        )
        source_by_id = {
            int(source["content"]["id"]): source for source in sources
        }
        for content_id in content_ids:
            if network_ledger is not None:
                _validate_download_provenance(
                    content_id=content_id,
                    source=source_by_id[content_id],
                    artifacts=artifacts_by_content[content_id],
                    ledger=network_ledger,
                )
            content_media, content_fingerprints = _validate_content_processing(
                connection,
                source=source_by_id[content_id],
                artifacts=artifacts_by_content[content_id],
                paths=paths,
                slot_attempt_expectations=normalized_expectations,
            )
            media_files.update(content_media)
            fingerprint_files.update(content_fingerprints)
        _validate_target_baseline_and_sequences(
            connection,
            contract=contract,
            content_ids=content_ids,
            active_evaluation_ids=active_evaluation_ids,
        )
        targets = _target_snapshot(connection, content_ids)
    media_inventory = _inventory(paths.media_root)
    fingerprint_inventory = _inventory(paths.fingerprint_root)

    def expected_rows(root: Path, files: set[Path]) -> list[Mapping[str, Any]]:
        rows: list[Mapping[str, Any]] = []
        for path in sorted(files, key=lambda item: str(item.relative_to(root))):
            _private_file(path, label="DB/manifest可达输出")
            if not _is_within(path, root):
                raise LocalAnalysisCanaryError("DB/manifest可达输出越过隔离根")
            rows.append(
                {
                    "path": str(path.relative_to(root)),
                    "byte_size": path.stat().st_size,
                    "sha256": _sha256_file(path),
                }
            )
        return rows

    expected_media_rows = expected_rows(paths.media_root, media_files)
    expected_fingerprint_rows = expected_rows(
        paths.fingerprint_root, fingerprint_files
    )
    if media_inventory["rows"] != expected_media_rows:
        raise LocalAnalysisCanaryError(
            "媒体输出全集不等于DB/manifest精确可达集合"
        )
    if fingerprint_inventory["rows"] != expected_fingerprint_rows:
        raise LocalAnalysisCanaryError(
            "指纹输出全集不等于DB/manifest精确可达集合"
        )
    if not expected_media_rows or not expected_fingerprint_rows:
        raise LocalAnalysisCanaryError("隔离媒体或指纹输出为空")
    return {
        "protected": protected,
        "sources_sha256": _json_sha256(sources),
        "targets": targets,
        "media_inventory": media_inventory,
        "fingerprint_inventory": fingerprint_inventory,
        "slot_attempt_expectations": [
            dict(normalized_expectations[key]) for key in sorted(normalized_expectations)
        ],
    }


def _recover_owned_running_slots(
    paths: CanaryPaths,
    content_ids: Sequence[int],
    *,
    intent_exists: bool,
    intent: Mapping[str, Any],
    contract: Mapping[str, Any],
) -> Mapping[str, Any]:
    placeholders = ",".join("?" for _ in content_ids)
    contract_sha256 = _sha256_file(paths.contract)
    intent_sha256 = _sha256_file(paths.intent)
    owner = intent.get("owner")
    if not isinstance(owner, Mapping):
        raise LocalAnalysisCanaryError("intent缺少attempt recovery owner身份")

    def validate_record(value: Mapping[str, Any]) -> None:
        _validate_running_recovery_record(
            value,
            contract=contract,
            content_ids=content_ids,
            contract_sha256=contract_sha256,
            intent_sha256=intent_sha256,
        )

    _recover_chained_json_temp(
        paths.running_recovery,
        label="attempt recovery",
        previous_field="previous_recovery_sha256",
        validator=validate_record,
    )
    if paths.running_recovery.exists():
        recovery = dict(
            _read_json(paths.running_recovery, label="attempt recovery")
        )
    else:
        recovery = {
            "schema_version": SCHEMA_VERSION,
            "operation": "recover-owned-media-attempts",
            "contract_sha256": contract_sha256,
            "intent_sha256": intent_sha256,
            "content_ids_sha256": _json_sha256(list(content_ids)),
            "rounds": [],
            "previous_recovery_sha256": None,
        }
    history_rows, expectations = _validate_running_recovery_record(
        recovery,
        contract=contract,
        content_ids=content_ids,
        contract_sha256=contract_sha256,
        intent_sha256=intent_sha256,
    )
    latest_by_key = {
        (int(row["content_id"]), str(row["processor_type"])): row
        for row in history_rows
    }
    new_rows: list[Mapping[str, Any]] = []
    with closing(_immutable_connection(paths.database)) as connection:
        candidates = connection.execute(
            """
            SELECT id,content_id,source_sha256,processor_type,processor_version,
                   status,output_artifact_id,attempt_count,error_message,updated_at
            FROM media_processing_slots WHERE content_id IN ("""
            + placeholders
            + ") AND status IN ('running','retryable_failed') ORDER BY id",
            content_ids,
        ).fetchall()
        if candidates and not intent_exists:
            raise LocalAnalysisCanaryError("首次canary发现既有未完成媒体slot")
        for row in candidates:
            key = (int(row["content_id"]), str(row["processor_type"]))
            latest = latest_by_key.get(key)
            attempt = int(row["attempt_count"])
            already_frozen = latest is not None and attempt == int(
                latest["from_attempt_count"]
            )
            if already_frozen:
                if latest is None:
                    raise LocalAnalysisCanaryError("attempt recovery冻结记录缺失")
                _assert_frozen_running_slot_identity(row, latest)
                _validate_owned_running_slot(
                    connection,
                    row=row,
                    contract=contract,
                    paths=paths,
                    require_stale=str(row["status"]) == "running",
                )
                continue
            if latest is not None and attempt != int(latest["expected_attempt_count"]):
                raise LocalAnalysisCanaryError("attempt recovery计数不是精确相邻态")
            if attempt >= media_module.MAX_MEDIA_PROCESSING_ATTEMPTS:
                raise LocalAnalysisCanaryError("媒体slot已达到最大尝试次数")
            if str(row["status"]) == "running":
                if _process_identity(int(owner.get("pid") or -1)) == owner.get(
                    "process_identity_sha256"
                ):
                    raise LocalAnalysisCanaryError("running媒体slot的原controller进程仍存活")
                _validate_owned_running_slot(
                    connection,
                    row=row,
                    contract=contract,
                    paths=paths,
                    require_stale=True,
                )
            else:
                _validate_owned_running_slot(
                    connection,
                    row=row,
                    contract=contract,
                    paths=paths,
                    require_stale=False,
                )
            new_rows.append(
                {
                    "slot_id": int(row["id"]),
                    "content_id": int(row["content_id"]),
                    "source_sha256": str(row["source_sha256"]),
                    "processor_type": str(row["processor_type"]),
                    "processor_version": str(row["processor_version"]),
                    "from_attempt_count": attempt,
                    "expected_attempt_count": attempt + 1,
                    "from_updated_at": str(row["updated_at"]),
                    "from_status": str(row["status"]),
                    "from_error_message": row["error_message"],
                }
            )
    with closing(_immutable_connection(paths.database)) as connection:
        for frozen in latest_by_key.values():
            row = connection.execute(
                "SELECT * FROM media_processing_slots WHERE id=?",
                (int(frozen["slot_id"]),),
            ).fetchone()
            if row is None:
                raise LocalAnalysisCanaryError("attempt recovery slot消失")
            _assert_frozen_running_slot_identity(row, frozen)
            status = str(row["status"])
            _validate_owned_running_slot(
                connection,
                row=row,
                contract=contract,
                paths=paths,
                require_stale=status == "running",
                require_no_output=status != "succeeded",
            )
    if new_rows:
        previous_round = (
            _json_sha256(recovery["rounds"][-1]) if recovery["rounds"] else None
        )
        recovery["rounds"].append(
            {
                "round_index": len(recovery["rounds"]) + 1,
                "previous_round_sha256": previous_round,
                "rows": new_rows,
                "rows_sha256": _json_sha256(new_rows),
            }
        )
        recovery["previous_recovery_sha256"] = (
            _sha256_file(paths.running_recovery)
            if paths.running_recovery.exists()
            else None
        )
        _write_json(paths.running_recovery, recovery, immutable=False)
        history_rows, expectations = _validate_running_recovery_record(
            recovery,
            contract=contract,
            content_ids=content_ids,
            contract_sha256=contract_sha256,
            intent_sha256=intent_sha256,
        )
        latest_by_key = {
            (int(row["content_id"]), str(row["processor_type"])): row
            for row in history_rows
        }
    recovered_now = 0
    requires_recovery_write = False
    with closing(_immutable_connection(paths.database)) as connection:
        for frozen in latest_by_key.values():
            row = connection.execute(
                "SELECT * FROM media_processing_slots WHERE id=?",
                (int(frozen["slot_id"]),),
            ).fetchone()
            if row is None:
                raise LocalAnalysisCanaryError("attempt recovery slot消失")
            _assert_frozen_running_slot_identity(row, frozen)
            status = str(row["status"])
            attempt = int(row["attempt_count"])
            source_attempt = int(frozen["from_attempt_count"])
            expected_attempt = int(frozen["expected_attempt_count"])
            if status == "running" and attempt == source_attempt:
                if str(row["updated_at"]) != str(frozen["from_updated_at"]):
                    raise LocalAnalysisCanaryError("attempt recovery running时间漂移")
                requires_recovery_write = True
            elif status == "retryable_failed" and attempt == source_attempt:
                if row["output_artifact_id"] is not None:
                    raise LocalAnalysisCanaryError("retryable slot已有output artifact")
            elif status == "succeeded" and attempt == expected_attempt:
                if row["output_artifact_id"] is None or row["error_message"] not in (
                    None,
                    "",
                ):
                    raise LocalAnalysisCanaryError("completed attempt recovery证据漂移")
            else:
                raise LocalAnalysisCanaryError("attempt recovery不在精确相邻状态")
            _validate_owned_running_slot(
                connection,
                row=row,
                contract=contract,
                paths=paths,
                require_stale=status == "running",
                require_no_output=status != "succeeded",
            )
    if requires_recovery_write:
        with closing(storage_module.connect(paths.database)) as connection:
            with storage_module.transaction(connection):
                for frozen in latest_by_key.values():
                    row = connection.execute(
                        "SELECT * FROM media_processing_slots WHERE id=?",
                        (int(frozen["slot_id"]),),
                    ).fetchone()
                    if row is None:
                        raise LocalAnalysisCanaryError("attempt recovery slot消失")
                    _assert_frozen_running_slot_identity(row, frozen)
                    status = str(row["status"])
                    attempt = int(row["attempt_count"])
                    source_attempt = int(frozen["from_attempt_count"])
                    expected_attempt = int(frozen["expected_attempt_count"])
                    _validate_owned_running_slot(
                        connection,
                        row=row,
                        contract=contract,
                        paths=paths,
                        require_stale=status == "running",
                        require_no_output=status != "succeeded",
                    )
                    if status == "running" and attempt == source_attempt:
                        if str(row["updated_at"]) != str(frozen["from_updated_at"]):
                            raise LocalAnalysisCanaryError(
                                "attempt recovery running时间漂移"
                            )
                        cursor = connection.execute(
                            """
                            UPDATE media_processing_slots
                            SET status='retryable_failed',
                                error_message='owned canary interrupted before receipt',updated_at=?
                            WHERE id=? AND content_id=? AND source_sha256=?
                              AND processor_type=? AND processor_version=?
                              AND status='running' AND attempt_count=? AND updated_at=?
                              AND output_artifact_id IS NULL
                              AND error_message IS ?
                            """,
                            (
                                storage_module.now_utc(),
                                int(row["id"]),
                                int(frozen["content_id"]),
                                str(frozen["source_sha256"]),
                                str(frozen["processor_type"]),
                                str(frozen["processor_version"]),
                                source_attempt,
                                str(frozen["from_updated_at"]),
                                frozen["from_error_message"],
                            ),
                        )
                        if cursor.rowcount != 1:
                            raise LocalAnalysisCanaryError("attempt recovery CAS失败")
                        recovered_now += 1
                    elif status == "retryable_failed" and attempt == source_attempt:
                        if row["output_artifact_id"] is not None:
                            raise LocalAnalysisCanaryError(
                                "retryable slot已有output artifact"
                            )
                    elif status == "succeeded" and attempt == expected_attempt:
                        if row["output_artifact_id"] is None or row[
                            "error_message"
                        ] not in (None, ""):
                            raise LocalAnalysisCanaryError(
                                "completed attempt recovery证据漂移"
                            )
                    else:
                        raise LocalAnalysisCanaryError(
                            "attempt recovery不在精确相邻状态"
                        )
    if not history_rows:
        return {
            "running_candidates": 0,
            "recovered": 0,
            "recovered_now": 0,
            "terminal": 0,
            "running_recovery_sha256": None,
            "slot_attempt_expectations": [],
        }
    return {
        "running_candidates": sum(
            row["from_status"] == "running" for row in history_rows
        ),
        "recovered": len(history_rows),
        "recovered_now": recovered_now,
        "terminal": 0,
        "recovery_rounds": len(recovery["rounds"]),
        "running_recovery_sha256": _sha256_file(paths.running_recovery),
        "slot_attempt_expectations": expectations,
    }


def _validate_running_recovery_record(
    recovery: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
    content_ids: Sequence[int],
    contract_sha256: str,
    intent_sha256: str,
) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]:
    if set(recovery) != {
        "schema_version",
        "operation",
        "contract_sha256",
        "intent_sha256",
        "content_ids_sha256",
        "rounds",
        "previous_recovery_sha256",
    } or (
        recovery.get("schema_version") != SCHEMA_VERSION
        or recovery.get("operation") != "recover-owned-media-attempts"
        or recovery.get("contract_sha256") != contract_sha256
        or recovery.get("intent_sha256") != intent_sha256
        or recovery.get("content_ids_sha256") != _json_sha256(list(content_ids))
    ):
        raise LocalAnalysisCanaryError("running recovery记录合同漂移")
    rounds = recovery.get("rounds")
    if not isinstance(rounds, list):
        raise LocalAnalysisCanaryError("attempt recovery rounds形状无效")
    row_keys = {
        "slot_id",
        "content_id",
        "source_sha256",
        "processor_type",
        "processor_version",
        "from_attempt_count",
        "expected_attempt_count",
        "from_updated_at",
        "from_status",
        "from_error_message",
    }
    rows: list[Mapping[str, Any]] = []
    previous_round_sha: str | None = None
    latest: dict[tuple[int, str], Mapping[str, Any]] = {}
    for round_index, round_value in enumerate(rounds, start=1):
        if (
            not isinstance(round_value, Mapping)
            or set(round_value)
            != {"round_index", "previous_round_sha256", "rows", "rows_sha256"}
            or int(round_value["round_index"]) != round_index
            or round_value["previous_round_sha256"] != previous_round_sha
            or not isinstance(round_value["rows"], list)
            or not round_value["rows"]
            or round_value["rows_sha256"] != _json_sha256(round_value["rows"])
        ):
            raise LocalAnalysisCanaryError("attempt recovery round证据漂移")
        for raw in round_value["rows"]:
            if not isinstance(raw, Mapping) or set(raw) != row_keys:
                raise LocalAnalysisCanaryError("attempt recovery row形状不精确")
            if (
                type(raw["slot_id"]) is not int
                or raw["slot_id"] <= 0
                or type(raw["content_id"]) is not int
                or raw["content_id"] not in content_ids
                or type(raw["source_sha256"]) is not str
                or not re.fullmatch(r"[0-9a-f]{64}", raw["source_sha256"])
                or type(raw["processor_type"]) is not str
                or not raw["processor_type"]
                or type(raw["processor_version"]) is not str
                or not raw["processor_version"]
                or type(raw["from_attempt_count"]) is not int
                or raw["from_attempt_count"] <= 0
                or type(raw["expected_attempt_count"]) is not int
                or raw["expected_attempt_count"]
                != raw["from_attempt_count"] + 1
                or type(raw["from_updated_at"]) is not str
                or type(raw["from_status"]) is not str
                or (
                    raw["from_error_message"] is not None
                    and type(raw["from_error_message"]) is not str
                )
            ):
                raise LocalAnalysisCanaryError("attempt recovery row类型或身份漂移")
            _parse_timestamp(raw["from_updated_at"])
            row = {key: raw[key] for key in row_keys}
            if row["from_status"] not in {"running", "retryable_failed"}:
                raise LocalAnalysisCanaryError("attempt recovery来源状态无效")
            key = (int(row["content_id"]), str(row["processor_type"]))
            previous = latest.get(key)
            if previous is not None and (
                int(row["slot_id"]) != int(previous["slot_id"])
                or int(row["from_attempt_count"])
                != int(previous["expected_attempt_count"])
            ):
                raise LocalAnalysisCanaryError("attempt recovery历史不是精确相邻链")
            latest[key] = row
            rows.append(row)
        previous_round_sha = _json_sha256(round_value)
    expectations = [
        {
            key: row[key]
            for key in (
                "slot_id",
                "content_id",
                "source_sha256",
                "processor_type",
                "processor_version",
                "from_attempt_count",
                "expected_attempt_count",
            )
        }
        for row in latest.values()
    ]
    normalized = _normalize_slot_attempt_expectations(
        expectations,
        contract=contract,
        content_ids=content_ids,
    )
    normalized_rows: list[Mapping[str, Any]] = [
        dict(normalized[key]) for key in sorted(normalized)
    ]
    if sorted(
        expectations,
        key=lambda row: (int(row["content_id"]), str(row["processor_type"])),
    ) != normalized_rows:
        raise LocalAnalysisCanaryError("attempt recovery规范化漂移")
    return rows, normalized_rows


def _owned_output_paths(
    paths: CanaryPaths,
    *,
    source: Mapping[str, Any],
    processor_type: str,
) -> tuple[str, set[Path]]:
    content = source["content"]
    content_id = int(content["id"])
    link_id = str(content["link_id"])
    media_kind = str(source["artifact_body"]["media_kind"])
    source_sha256 = str(source["artifact_body"]["source_sha256"])
    if processor_type == "download":
        if media_kind == "video":
            parent = paths.media_root / link_id / "downloads" / source_sha256
            target = parent / "source.mp4"
            candidates = {
                target.with_name(f".{target.name}.candidate-{index}")
                for index in range(len(_source_urls(source)))
            }
            return "media", {target, *candidates}
        parent = (
            paths.media_root
            / link_id
            / "downloads"
            / _source_image_download_binding(source)
            / "images"
        )
        values: set[Path] = {
            parent / "manifest.json",
            parent / ".manifest.json.tmp",
        }
        for index in range(len(_source_image_groups(source))):
            target = parent / f"image-{index:03d}.bin"
            values.update({target, target.with_name(f".{target.name}.tmp")})
        return "media", values
    if processor_type == "frames":
        parent = paths.media_root / link_id / "frames"
        maximum_frames = int(
            media_module.load_media_config()["frames"]["maximum_frames"]
        )
        values = {
            parent / "frames.json",
            parent / ".frames.json.tmp",
            parent / "contact-sheet.jpg",
            parent / ".contact-sheet.tmp.jpg",
        }
        for index in range(maximum_frames):
            values.update(
                {
                    parent / f"frame-{index:03d}.jpg",
                    parent / f".frame-{index:03d}.tmp.jpg",
                }
            )
        return "media", values
    if processor_type in {"asr", "ocr"}:
        target = paths.media_root / link_id / f"{processor_type}.json"
        return "media", {target, target.with_name(f".{target.name}.tmp")}
    if processor_type == "duplicate_fingerprint":
        target = paths.fingerprint_root / f"{link_id}.json"
        return "fingerprint", {target, target.with_name(f".{target.name}.tmp")}
    raise LocalAnalysisCanaryError(
        f"content {content_id} processor没有owned output路径：{processor_type}"
    )


def _validate_frames_final_orphan(
    path: Path, *, paths: CanaryPaths, source: Mapping[str, Any]
) -> set[Path]:
    body = _read_json(path, label="orphan frames manifest")
    frames = body.get("frames")
    maximum_frames = int(
        media_module.load_media_config()["frames"]["maximum_frames"]
    )
    if (
        set(body) != {"status", "duration_seconds", "frames", "contact_sheet"}
        or body.get("status") != "success"
        or not isinstance(body.get("duration_seconds"), (int, float))
        or not 0 < float(body["duration_seconds"]) <= MAX_VIDEO_DURATION_SECONDS
        or not isinstance(frames, list)
        or not 0 < len(frames) <= maximum_frames
    ):
        raise LocalAnalysisCanaryError("frames final orphan语义无效")
    _root_name, allowed = _owned_output_paths(
        paths, source=source, processor_type="frames"
    )
    seen: set[Path] = set()
    closure = {path}
    for frame in frames:
        if not isinstance(frame, Mapping) or set(frame) != {"path", "sha256"}:
            raise LocalAnalysisCanaryError("frames final orphan frame形状无效")
        candidate = media_module._resolved(str(frame["path"])).resolve()
        temporary = candidate.with_name(
            f".{candidate.stem}.tmp{candidate.suffix}"
        )
        actual = candidate if candidate.exists() else temporary
        if (
            re.fullmatch(r"frame-\d{3}\.jpg", candidate.name) is None
            or candidate not in allowed
            or temporary not in allowed
            or candidate in seen
        ):
            raise LocalAnalysisCanaryError("frames final orphan路径未绑定owned集合")
        seen.add(candidate)
        _private_file(actual, label="orphan frame")
        if not media_module._valid_image(actual):
            raise LocalAnalysisCanaryError("frames final orphan frame不是有效图片")
        if frame["sha256"] != _sha256_file(actual):
            raise LocalAnalysisCanaryError("frames final orphan frame SHA漂移")
        closure.add(actual)
    contact_sheet = body.get("contact_sheet")
    if contact_sheet is not None:
        contact_path = media_module._resolved(str(contact_sheet)).resolve()
        contact_temporary = contact_path.with_name(
            f".{contact_path.stem}.tmp{contact_path.suffix}"
        )
        contact_actual = (
            contact_path if contact_path.exists() else contact_temporary
        )
        if (
            contact_path.name != "contact-sheet.jpg"
            or contact_path not in allowed
            or contact_temporary not in allowed
        ):
            raise LocalAnalysisCanaryError("frames contact sheet路径漂移")
        _private_file(contact_actual, label="orphan contact sheet")
        if not media_module._valid_image(contact_actual):
            raise LocalAnalysisCanaryError("frames contact sheet不是有效图片")
        closure.add(contact_actual)
    return closure


def _validate_asr_final_orphan(path: Path) -> None:
    body = _read_json(path, label="orphan ASR")
    config = media_module.load_media_config()["asr"]
    common = {
        "status",
        "processor_version",
        "model_id",
        "model_revision",
        "language",
        "text",
        "segments",
    }
    if body.get("status") == "success":
        expected_keys = common | {"elapsed_seconds"}
        if (
            type(body.get("elapsed_seconds")) not in {int, float}
            or not math.isfinite(float(body["elapsed_seconds"]))
            or float(body["elapsed_seconds"]) < 0
        ):
            raise LocalAnalysisCanaryError("ASR final orphan耗时无效")
    elif body.get("status") == "unavailable":
        expected_keys = common | {"reason"}
        if body.get("reason") != "audio_decode_failed":
            raise LocalAnalysisCanaryError("ASR final orphan unavailable原因无效")
    else:
        raise LocalAnalysisCanaryError("ASR final orphan状态无效")
    segments = body.get("segments")
    if (
        set(body) != expected_keys
        or body.get("processor_version") != media_module.processor_versions()["asr"]
        or body.get("model_id") != config["model_id"]
        or body.get("model_revision") != config["model_revision"]
        or type(body.get("language")) is not str
        or type(body.get("text")) is not str
        or not isinstance(segments, list)
    ):
        raise LocalAnalysisCanaryError("ASR final orphan合同漂移")
    segment_keys = {"start", "end", "text", "avg_logprob", "no_speech_prob"}
    if any(
        not isinstance(segment, Mapping)
        or set(segment) != segment_keys
        or type(segment.get("text")) is not str
        or any(
            type(segment.get(field)) not in {int, float}
            or not math.isfinite(float(segment[field]))
            for field in {"start", "end", "avg_logprob", "no_speech_prob"}
        )
        for segment in segments
    ):
        raise LocalAnalysisCanaryError("ASR final orphan segments形状无效")


def _validate_ocr_final_orphan(
    path: Path,
    *,
    paths: CanaryPaths,
    source: Mapping[str, Any],
    expected_source_sha256: str,
) -> None:
    body = _read_json(path, label="orphan OCR")
    link_id = str(source["content"]["link_id"])
    media_kind = str(source["artifact_body"]["media_kind"])
    frames_path = (
        paths.media_root / link_id / "frames" / "frames.json"
        if media_kind == "video"
        else paths.media_root
        / link_id
        / "downloads"
        / _source_image_download_binding(source)
        / "images"
        / "manifest.json"
    )
    _private_file(frames_path, label="OCR source manifest")
    if _sha256_file(frames_path) != expected_source_sha256:
        raise LocalAnalysisCanaryError("OCR final orphan未绑定输入manifest SHA")
    frames_body = _read_json(frames_path, label="OCR source frames manifest")
    frames = frames_body.get("frames")
    observations = body.get("observations")
    normalized_texts: list[str] = []
    if isinstance(observations, list):
        for item in observations:
            if not isinstance(item, Mapping):
                continue
            text = "\n".join(str(item.get("text") or "").splitlines()).strip()
            if text and text not in normalized_texts:
                normalized_texts.append(text)
    if (
        set(body)
        != {
            "status",
            "processor_version",
            "source_count",
            "ocr_observation_count",
            "combined_text",
            "observations",
        }
        or body.get("status") != "success"
        or body.get("processor_version") != media_module.processor_versions()["ocr"]
        or not isinstance(frames, list)
        or type(body.get("source_count")) is not int
        or body["source_count"] != len(frames)
        or not isinstance(observations, list)
        or type(body.get("ocr_observation_count")) is not int
        or body["ocr_observation_count"] != len(observations)
        or len(observations) != len(frames)
        or type(body.get("combined_text")) is not str
        or body["combined_text"] != "\n".join(normalized_texts)
        or any(not isinstance(item, Mapping) for item in observations)
    ):
        raise LocalAnalysisCanaryError("OCR final orphan合同漂移")


def _validate_fingerprint_final_orphan(
    path: Path,
    *,
    paths: CanaryPaths,
    content_id: int,
    expected_source_sha256: str,
) -> None:
    body = _read_json(path, label="orphan duplicate fingerprint")
    with closing(_immutable_connection(paths.database)) as connection:
        inputs, source_sha256 = duplicates_module._current_source_state(
            connection, content_id
        )
    media_sha256, frame_phashes = duplicates_module._media_fingerprints(
        inputs["media_path"]
    )
    normalized_text = duplicates_module._normalize_text(inputs["text"])
    normalized_asr = duplicates_module._normalize_text(inputs["asr_text"])
    normalized_ocr = duplicates_module._normalize_text(inputs["ocr_text"])
    expected = {
        "schema_version": "duplicate-fingerprint-v1",
        "fingerprint_version": duplicates_module.FINGERPRINT_VERSION,
        "content_id": content_id,
        "source_sha256": source_sha256,
        "text_sha256": (
            _sha256_bytes(normalized_text.encode("utf-8"))
            if len(normalized_text) >= 12
            else None
        ),
        "media_sha256": media_sha256,
        "frame_phashes": frame_phashes,
        "text_simhash": duplicates_module._simhash(inputs["text"]),
        "asr_simhash": duplicates_module._simhash(inputs["asr_text"]),
        "ocr_simhash": duplicates_module._simhash(inputs["ocr_text"]),
        "text_char_count": len(normalized_text),
        "asr_char_count": len(normalized_asr),
        "ocr_char_count": len(normalized_ocr),
    }
    created_at = body.get("created_at")
    if (
        set(body) != set(expected) | {"created_at"}
        or source_sha256 != expected_source_sha256
        or any(body.get(key) != value for key, value in expected.items())
    ):
        raise LocalAnalysisCanaryError("fingerprint final orphan输入投影漂移")
    _parse_timestamp(created_at)


def _validate_output_recovery_record(
    value: Mapping[str, Any],
    *,
    paths: CanaryPaths,
    contract: Mapping[str, Any],
    contract_sha256: str,
    intent_sha256: str,
) -> list[Mapping[str, Any]]:
    if set(value) != {
        "schema_version",
        "operation",
        "contract_sha256",
        "intent_sha256",
        "rounds",
        "previous_recovery_sha256",
    } or (
        value.get("schema_version") != SCHEMA_VERSION
        or value.get("operation") != "recover-owned-output-partials"
        or value.get("contract_sha256") != contract_sha256
        or value.get("intent_sha256") != intent_sha256
    ):
        raise LocalAnalysisCanaryError("output recovery记录合同漂移")
    rounds = value.get("rounds")
    if not isinstance(rounds, list):
        raise LocalAnalysisCanaryError("output recovery rounds无效")
    source_by_id = {
        int(source["content"]["id"]): source for source in contract["sources"]
    }
    row_keys = {
        "root",
        "path",
        "content_id",
        "slot_id",
        "processor_type",
        "processor_version",
        "source_sha256",
        "attempt_count",
        "byte_size",
        "sha256",
    }
    allowed_attempt_rows: set[tuple[int, int, str, str, str, int]] = set()
    if rounds:
        if not paths.running_recovery.exists():
            raise LocalAnalysisCanaryError(
                "output recovery缺少running attempt recovery证据"
            )
        running_value = _read_json(
            paths.running_recovery, label="output recovery attempt chain"
        )
        running_rows, _expectations = _validate_running_recovery_record(
            running_value,
            contract=contract,
            content_ids=[
                int(source["content"]["id"]) for source in contract["sources"]
            ],
            contract_sha256=contract_sha256,
            intent_sha256=intent_sha256,
        )
        allowed_attempt_rows = {
            (
                int(row["slot_id"]),
                int(row["content_id"]),
                str(row["processor_type"]),
                str(row["processor_version"]),
                str(row["source_sha256"]),
                int(row["from_attempt_count"]),
            )
            for row in running_rows
        }
    rows: list[Mapping[str, Any]] = []
    previous_round_sha256: str | None = None
    for round_index, round_value in enumerate(rounds, start=1):
        if (
            not isinstance(round_value, Mapping)
            or set(round_value)
            != {"round_index", "previous_round_sha256", "rows", "rows_sha256"}
            or int(round_value["round_index"]) != round_index
            or round_value["previous_round_sha256"] != previous_round_sha256
            or not isinstance(round_value["rows"], list)
            or not round_value["rows"]
            or round_value["rows_sha256"] != _json_sha256(round_value["rows"])
        ):
            raise LocalAnalysisCanaryError("output recovery round证据漂移")
        for raw in round_value["rows"]:
            if not isinstance(raw, Mapping) or set(raw) != row_keys:
                raise LocalAnalysisCanaryError("output recovery row形状不精确")
            content_id = int(raw["content_id"])
            processor_type = str(raw["processor_type"])
            source = source_by_id.get(content_id)
            if source is None:
                raise LocalAnalysisCanaryError("output recovery不属于合同target")
            expected_root, allowed = _owned_output_paths(
                paths, source=source, processor_type=processor_type
            )
            root = paths.media_root if expected_root == "media" else paths.fingerprint_root
            candidate = root / str(raw["path"])
            if (
                raw["root"] != expected_root
                or candidate not in allowed
                or int(raw["slot_id"]) <= 0
                or int(raw["attempt_count"]) <= 0
                or int(raw["byte_size"]) < 0
                or not re.fullmatch(r"[0-9a-f]{64}", str(raw["sha256"]))
                or (
                    int(raw["slot_id"]),
                    content_id,
                    processor_type,
                    str(raw["processor_version"]),
                    str(raw["source_sha256"]),
                    int(raw["attempt_count"]),
                )
                not in allowed_attempt_rows
            ):
                raise LocalAnalysisCanaryError("output recovery row ownership漂移")
            rows.append(dict(raw))
        previous_round_sha256 = _json_sha256(round_value)
    return rows


def _after_output_recovery_writer_lock(
    _connection: sqlite3.Connection, _paths: CanaryPaths
) -> None:
    """Test seam after the writer reservation and before recovery validation."""


def _writable_connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=30)
    connection.row_factory = sqlite3.Row
    try:
        storage_module.configure_connection_safety(connection)
    except Exception:
        connection.close()
        raise
    return connection


def _recover_owned_output_partials(
    paths: CanaryPaths,
    *,
    contract: Mapping[str, Any],
    content_ids: Sequence[int],
    slot_attempt_expectations: Sequence[Mapping[str, Any]],
    network_ledger: "_NetworkLedger",
) -> Mapping[str, Any]:
    guard = _writable_connection(paths.database)
    try:
        guard.execute("PRAGMA busy_timeout=30000")
        guard.execute("BEGIN IMMEDIATE")
        _after_output_recovery_writer_lock(guard, paths)
        result = _recover_owned_output_partials_under_writer_lock(
            paths,
            contract=contract,
            content_ids=content_ids,
            slot_attempt_expectations=slot_attempt_expectations,
            network_ledger=network_ledger,
            writer_connection=guard,
        )
        guard.commit()
        return result
    except BaseException:
        guard.rollback()
        raise
    finally:
        guard.close()


def _recover_owned_output_partials_under_writer_lock(
    paths: CanaryPaths,
    *,
    contract: Mapping[str, Any],
    content_ids: Sequence[int],
    slot_attempt_expectations: Sequence[Mapping[str, Any]],
    network_ledger: "_NetworkLedger",
    writer_connection: sqlite3.Connection,
) -> Mapping[str, Any]:
    contract_sha256 = _sha256_file(paths.contract)
    intent_sha256 = _sha256_file(paths.intent)

    def validate(value: Mapping[str, Any]) -> None:
        _validate_output_recovery_record(
            value,
            paths=paths,
            contract=contract,
            contract_sha256=contract_sha256,
            intent_sha256=intent_sha256,
        )

    _recover_chained_json_temp(
        paths.output_recovery,
        label="output recovery",
        previous_field="previous_recovery_sha256",
        validator=validate,
    )
    if paths.output_recovery.exists():
        recovery = dict(_read_json(paths.output_recovery, label="output recovery"))
    else:
        recovery = {
            "schema_version": SCHEMA_VERSION,
            "operation": "recover-owned-output-partials",
            "contract_sha256": contract_sha256,
            "intent_sha256": intent_sha256,
            "rounds": [],
            "previous_recovery_sha256": None,
        }
    history = _validate_output_recovery_record(
        recovery,
        paths=paths,
        contract=contract,
        contract_sha256=contract_sha256,
        intent_sha256=intent_sha256,
    )
    latest_by_key = {
        (str(row["root"]), str(row["path"])): row for row in history
    }
    expectations = _normalize_slot_attempt_expectations(
        slot_attempt_expectations,
        contract=contract,
        content_ids=content_ids,
    )
    with contextlib.nullcontext(writer_connection) as connection:
        placeholders = ",".join("?" for _ in content_ids)
        live_slots = connection.execute(
            "SELECT * FROM media_processing_slots WHERE content_id IN ("
            + placeholders
            + ") ORDER BY id",
            content_ids,
        ).fetchall()
        live_by_id = {int(row["id"]): row for row in live_slots}
        expected_slot_ids: set[int] = set()
        for expectation in expectations.values():
            expected_slot_ids.add(int(expectation["slot_id"]))
            live = connection.execute(
                "SELECT * FROM media_processing_slots WHERE id=?",
                (int(expectation["slot_id"]),),
            ).fetchone()
            if (
                live is None
                or live["content_id"] != expectation["content_id"]
                or live["source_sha256"] != expectation["source_sha256"]
                or live["processor_type"] != expectation["processor_type"]
                or live["processor_version"] != expectation["processor_version"]
            ):
                raise LocalAnalysisCanaryError(
                    "output recovery写前live slot身份漂移"
                )
            unfinished = (
                live["status"] in {"running", "retryable_failed"}
                and live["attempt_count"] == expectation["from_attempt_count"]
                and live["output_artifact_id"] is None
            )
            succeeded_adjacent = (
                live["status"] == "succeeded"
                and live["attempt_count"] == expectation["expected_attempt_count"]
                and live["output_artifact_id"] is not None
                and live["error_message"] in (None, "")
            )
            if not unfinished and not succeeded_adjacent:
                raise LocalAnalysisCanaryError(
                    "output recovery写前attempt相邻状态漂移"
                )
            _validate_owned_running_slot(
                connection,
                row=live,
                contract=contract,
                paths=paths,
                require_stale=False,
                require_no_output=unfinished,
            )
        for live in live_slots:
            if (
                live["status"] in {"running", "retryable_failed"}
                and int(live["id"]) not in expected_slot_ids
            ):
                raise LocalAnalysisCanaryError(
                    "output recovery写前存在无attempt合同的live slot"
                )
        for frozen in history:
            live = live_by_id.get(int(frozen["slot_id"]))
            if (
                live is None
                or live["content_id"] != frozen["content_id"]
                or live["source_sha256"] != frozen["source_sha256"]
                or live["processor_type"] != frozen["processor_type"]
                or live["processor_version"] != frozen["processor_version"]
            ):
                raise LocalAnalysisCanaryError(
                    "output recovery历史与live slot身份漂移"
                )
    for row in history:
        root = (
            paths.media_root
            if row["root"] == "media"
            else paths.fingerprint_root
        )
        _resume_verified_output_quarantine(
            root / str(row["path"]),
            expected_byte_size=int(row["byte_size"]),
            expected_sha256=str(row["sha256"]),
        )
    source_by_id = {
        int(source["content"]["id"]): source for source in contract["sources"]
    }
    allowed_by_path: dict[Path, Mapping[str, Any]] = {}
    with contextlib.nullcontext(writer_connection) as connection:
        artifacts_by_content, reachable_media, reachable_fingerprint = (
            _validate_generated_artifacts(
                connection,
                contract=contract,
                paths=paths,
                content_ids=content_ids,
            )
        )
        for content_id, artifacts in artifacts_by_content.items():
            source = source_by_id[content_id]
            media_kind = str(source["artifact_body"]["media_kind"])
            if "frames_manifest" in artifacts:
                reachable_media.update(
                    _manifest_output_paths(
                        artifacts["frames_manifest"],
                        media_kind="video",
                        media_root=paths.media_root,
                    )
                )
            if "media_manifest" in artifacts:
                reachable_media.update(
                    _manifest_output_paths(
                        artifacts["media_manifest"],
                        media_kind=media_kind,
                        media_root=paths.media_root,
                        source=source,
                    )
                )
        placeholders = ",".join("?" for _ in content_ids)
        incomplete = connection.execute(
            "SELECT * FROM media_processing_slots WHERE content_id IN ("
            + placeholders
            + ") AND status IN ('running','retryable_failed') ORDER BY id",
            content_ids,
        ).fetchall()
        for slot in incomplete:
            key = (int(slot["content_id"]), str(slot["processor_type"]))
            incomplete_expectation = expectations.get(key)
            if (
                incomplete_expectation is None
                or int(incomplete_expectation["slot_id"]) != int(slot["id"])
                or int(incomplete_expectation["from_attempt_count"])
                != int(slot["attempt_count"])
            ):
                raise LocalAnalysisCanaryError(
                    "未完成slot输出缺少精确attempt recovery证据"
                )
            _validate_owned_running_slot(
                connection,
                row=slot,
                contract=contract,
                paths=paths,
                require_stale=False,
            )
            root_name, allowed = _owned_output_paths(
                paths,
                source=source_by_id[int(slot["content_id"])],
                processor_type=str(slot["processor_type"]),
            )
            for candidate in allowed:
                allowed_by_path[candidate] = {
                    "root": root_name,
                    "content_id": int(slot["content_id"]),
                    "slot_id": int(slot["id"]),
                    "processor_type": str(slot["processor_type"]),
                    "processor_version": str(slot["processor_version"]),
                    "source_sha256": str(slot["source_sha256"]),
                    "attempt_count": int(slot["attempt_count"]),
                }
    reachable = reachable_media | reachable_fingerprint
    cleanup_rows: list[Mapping[str, Any]] = []
    cleanup_paths: list[Path] = []
    cleanup_identity: dict[Path, tuple[int, int]] = {}
    for root_name, root in (("media", paths.media_root), ("fingerprint", paths.fingerprint_root)):
        for candidate in sorted(root.rglob("*"), key=str):
            metadata = candidate.lstat()
            if stat.S_ISDIR(metadata.st_mode) and not candidate.is_symlink():
                continue
            if (
                candidate.is_symlink()
                or not stat.S_ISREG(metadata.st_mode)
                or metadata.st_nlink != 1
            ):
                raise LocalAnalysisCanaryError(
                    f"恢复前输出根包含alias或非私有文件：{candidate}"
                )
            if candidate in reachable:
                continue
            ownership = allowed_by_path.get(candidate)
            if ownership is None or ownership["root"] != root_name:
                raise LocalAnalysisCanaryError(
                    f"恢复前输出根包含非owned文件：{candidate}"
                )
            current_sha256 = _sha256_file(candidate)
            current_row = {
                **ownership,
                "path": str(candidate.relative_to(root)),
                "byte_size": metadata.st_size,
                "sha256": current_sha256,
            }
            historical = latest_by_key.get(
                (root_name, str(current_row["path"]))
            )
            historical_exact = historical == current_row
            if historical is not None and not historical_exact:
                if (
                    int(historical["slot_id"]) != int(current_row["slot_id"])
                    or int(historical["attempt_count"])
                    >= int(current_row["attempt_count"])
                ):
                    raise LocalAnalysisCanaryError(
                        "同attempt待清理owned output字节被替换"
                    )
            source = source_by_id[int(ownership["content_id"])]
            processor_type = str(ownership["processor_type"])
            if (
                not historical_exact
                and processor_type == "download"
                and not candidate.name.startswith(".")
            ):
                transcript = network_ledger.transcript(int(ownership["content_id"]))
                successful = [
                    event for event in transcript if event["outcome"] == "succeeded"
                ]
                if candidate.name == "source.mp4":
                    if not any(
                        event["response_sha256"] == current_sha256
                        and int(event["bytes"]) == metadata.st_size
                        for event in successful
                    ):
                        raise LocalAnalysisCanaryError(
                            "download final orphan未绑定成功network响应"
                        )
                    with _execution_guards(
                        _source_urls(source),
                        media_kind="video",
                        maximum_bytes=int(contract["maximum_download_bytes"]),
                        tools=contract["tools"],
                    ):
                        if not media_module._valid_media(
                            candidate,
                            maximum_duration_seconds=float(
                                contract["maximum_video_duration_seconds"]
                            ),
                        ):
                            raise LocalAnalysisCanaryError(
                                "download final orphan不是冻结上限内有效视频"
                            )
                elif re.fullmatch(r"image-\d{3}\.bin", candidate.name):
                    index = int(candidate.stem.split("-")[1])
                    groups = _source_image_groups(source)
                    group_hashes = (
                        {
                            str(item["url_sha256"])
                            for item in groups[index]["candidates"]
                        }
                        if index < len(groups)
                        else set()
                    )
                    if not group_hashes or not any(
                        event["url_sha256"] in group_hashes
                        and event["response_sha256"] == current_sha256
                        and int(event["bytes"]) == metadata.st_size
                        for event in successful
                    ):
                        raise LocalAnalysisCanaryError(
                            "image final orphan未绑定冻结URL响应"
                        )
                    if not media_module._valid_image(candidate):
                        raise LocalAnalysisCanaryError(
                            "image final orphan不是受支持图片"
                        )
                elif candidate.name == "manifest.json":
                    manifest = _read_json(candidate, label="orphan image manifest")
                    _image_manifest_output_paths(
                        candidate,
                        manifest,
                        source=source,
                        media_root=paths.media_root,
                    )
            elif not historical_exact and processor_type == "frames":
                frames_manifest = candidate.parent / "frames.json"
                is_atomic_frame_temp = bool(
                    re.fullmatch(r"\.frame-\d{3}\.tmp\.jpg", candidate.name)
                    or candidate.name == ".contact-sheet.tmp.jpg"
                    or candidate.name == ".frames.json.tmp"
                )
                if frames_manifest.exists():
                    if candidate.name == ".frames.json.tmp":
                        raise LocalAnalysisCanaryError(
                            "frames final与atomic temp同时存在"
                        )
                    frames_closure = _validate_frames_final_orphan(
                        frames_manifest, paths=paths, source=source
                    )
                    if candidate not in frames_closure:
                        raise LocalAnalysisCanaryError(
                            "frames output不属于manifest闭包"
                        )
                elif not is_atomic_frame_temp:
                    raise LocalAnalysisCanaryError(
                        "frames final orphan缺少完整manifest绑定"
                    )
            elif not historical_exact and processor_type == "asr":
                if candidate.name == "asr.json":
                    _validate_asr_final_orphan(candidate)
            elif not historical_exact and processor_type == "ocr":
                if candidate.name == "ocr.json":
                    _validate_ocr_final_orphan(
                        candidate,
                        paths=paths,
                        source=source,
                        expected_source_sha256=str(ownership["source_sha256"]),
                    )
            elif (
                not historical_exact
                and processor_type == "duplicate_fingerprint"
                and candidate.name == f"{source['content']['link_id']}.json"
            ):
                _validate_fingerprint_final_orphan(
                    candidate,
                    paths=paths,
                    content_id=int(ownership["content_id"]),
                    expected_source_sha256=str(ownership["source_sha256"]),
                )
            cleanup_paths.append(candidate)
            cleanup_identity[candidate] = (metadata.st_dev, metadata.st_ino)
            cleanup_rows.append(current_row)
    new_rows = [
        row
        for row in cleanup_rows
        if latest_by_key.get((str(row["root"]), str(row["path"]))) != row
    ]
    if new_rows:
        recovery["rounds"].append(
            {
                "round_index": len(recovery["rounds"]) + 1,
                "previous_round_sha256": (
                    _json_sha256(recovery["rounds"][-1])
                    if recovery["rounds"]
                    else None
                ),
                "rows": new_rows,
                "rows_sha256": _json_sha256(new_rows),
            }
        )
        recovery["previous_recovery_sha256"] = (
            _sha256_file(paths.output_recovery)
            if paths.output_recovery.exists()
            else None
        )
        _write_json(paths.output_recovery, recovery, immutable=False)
        history = _validate_output_recovery_record(
            recovery,
            paths=paths,
            contract=contract,
            contract_sha256=contract_sha256,
            intent_sha256=intent_sha256,
        )
    cleanup_by_path = {
        (
            paths.media_root if row["root"] == "media" else paths.fingerprint_root
        )
        / str(row["path"]): row
        for row in cleanup_rows
    }
    for candidate in cleanup_paths:
        row = cleanup_by_path[candidate]
        device, inode = cleanup_identity[candidate]
        _unlink_verified_private_file(
            candidate,
            expected_device=device,
            expected_inode=inode,
            expected_byte_size=int(row["byte_size"]),
            expected_sha256=str(row["sha256"]),
        )
    if not history:
        return {
            "output_recovered": 0,
            "output_recovery_rounds": 0,
            "output_recovery_sha256": None,
        }
    return {
        "output_recovered": len(history),
        "output_recovery_rounds": len(recovery["rounds"]),
        "output_recovery_sha256": _sha256_file(paths.output_recovery),
    }


def _parse_timestamp(value: Any) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise LocalAnalysisCanaryError(f"running slot时间无效：{value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _assert_frozen_running_slot_identity(
    row: sqlite3.Row, frozen: Mapping[str, Any]
) -> None:
    if (
        row["id"] != frozen["slot_id"]
        or row["content_id"] != frozen["content_id"]
        or row["source_sha256"] != frozen["source_sha256"]
        or row["processor_type"] != frozen["processor_type"]
        or row["processor_version"] != frozen["processor_version"]
    ):
        raise LocalAnalysisCanaryError("attempt recovery live slot身份漂移")
    status = row["status"]
    attempt = row["attempt_count"]
    source_attempt = frozen["from_attempt_count"]
    expected_attempt = frozen["expected_attempt_count"]
    if status == frozen["from_status"] and attempt == source_attempt:
        if (
            row["updated_at"] != frozen["from_updated_at"]
            or row["error_message"] != frozen["from_error_message"]
            or row["output_artifact_id"] is not None
        ):
            raise LocalAnalysisCanaryError("attempt recovery live来源状态漂移")
    elif (
        frozen["from_status"] == "running"
        and status == "retryable_failed"
        and attempt == source_attempt
    ):
        if (
            row["error_message"] != "owned canary interrupted before receipt"
            or row["output_artifact_id"] is not None
        ):
            raise LocalAnalysisCanaryError("attempt recovery retryable状态漂移")
    elif status == "succeeded" and attempt == expected_attempt:
        if row["output_artifact_id"] is None or row["error_message"] not in (None, ""):
            raise LocalAnalysisCanaryError("attempt recovery succeeded状态漂移")
    else:
        raise LocalAnalysisCanaryError("attempt recovery live相邻状态漂移")


def _current_contract_upstream_sha(
    connection: sqlite3.Connection,
    *,
    contract: Mapping[str, Any],
    source: Mapping[str, Any],
    paths: CanaryPaths,
    processor_type: str,
) -> str:
    content_id = int(source["content"]["id"])
    link_id = str(source["content"]["link_id"])
    media_kind = str(source["artifact_body"]["media_kind"])
    artifacts_by_content, _media_files, _fingerprint_files = (
        _validate_generated_artifacts(
            connection,
            contract=contract,
            paths=paths,
            content_ids=[content_id],
        )
    )
    artifacts = artifacts_by_content[content_id]
    download_name = "media" if media_kind == "video" else "media_manifest"
    download_artifact = artifacts.get(download_name)
    if download_artifact is None:
        raise LocalAnalysisCanaryError("running slot缺少current download artifact")
    download_source_sha = (
        str(source["artifact_body"]["source_sha256"])
        if media_kind == "video"
        else _source_image_download_binding(source)
    )
    download_version = (
        media_module.VIDEO_DOWNLOAD_VERSION
        if media_kind == "video"
        else media_module.IMAGE_DOWNLOAD_VERSION
    )
    download_slots = connection.execute(
        """
        SELECT * FROM media_processing_slots
        WHERE content_id=? AND source_sha256=? AND processor_type='download'
          AND processor_version=?
        """,
        (content_id, download_source_sha, download_version),
    ).fetchall()
    if (
        len(download_slots) != 1
        or download_slots[0]["status"] != "succeeded"
        or download_slots[0]["output_artifact_id"] != download_artifact["id"]
        or type(download_slots[0]["attempt_count"]) is not int
        or download_slots[0]["attempt_count"] <= 0
        or download_artifact["processor_version"] != download_version
    ):
        raise LocalAnalysisCanaryError("running slot current download闭包漂移")
    if media_kind == "video":
        expected_path = (
            paths.media_root
            / link_id
            / "downloads"
            / download_source_sha
            / "source.mp4"
        ).resolve()
        if _resolve_artifact_path(str(download_artifact["local_path"])) != expected_path:
            raise LocalAnalysisCanaryError("running slot video download路径漂移")
        expected_metadata = {
            "source_count": len(_source_urls(source)),
            "source_sha256": download_source_sha,
        }
        try:
            metadata = json.loads(str(download_artifact["metadata_json"]))
        except json.JSONDecodeError as exc:
            raise LocalAnalysisCanaryError(
                "running slot video download metadata无效"
            ) from exc
        if _canonical_bytes(metadata) != _canonical_bytes(expected_metadata):
            raise LocalAnalysisCanaryError(
                "running slot video download metadata漂移"
            )
    else:
        _manifest_output_paths(
            download_artifact,
            media_kind="image",
            media_root=paths.media_root,
            source=source,
        )
    if processor_type in {"frames", "asr"} or media_kind == "image":
        return str(download_artifact["sha256"])
    frames_artifact = artifacts.get("frames_manifest")
    if frames_artifact is None:
        raise LocalAnalysisCanaryError("running OCR缺少current frames artifact")
    versions = media_module.processor_versions()
    frames_slots = connection.execute(
        """
        SELECT * FROM media_processing_slots
        WHERE content_id=? AND source_sha256=? AND processor_type='frames'
          AND processor_version=?
        """,
        (content_id, download_artifact["sha256"], versions["frames"]),
    ).fetchall()
    expected_frames_path = (
        paths.media_root / link_id / "frames" / "frames.json"
    ).resolve()
    if (
        len(frames_slots) != 1
        or frames_slots[0]["status"] != "succeeded"
        or frames_slots[0]["output_artifact_id"] != frames_artifact["id"]
        or frames_artifact["processor_version"] != versions["frames"]
        or _resolve_artifact_path(str(frames_artifact["local_path"]))
        != expected_frames_path
    ):
        raise LocalAnalysisCanaryError("running OCR current frames闭包漂移")
    _manifest_output_paths(
        frames_artifact, media_kind="video", media_root=paths.media_root
    )
    return str(frames_artifact["sha256"])


def _validate_succeeded_owned_slot_output(
    connection: sqlite3.Connection,
    *,
    row: sqlite3.Row,
    contract: Mapping[str, Any],
    source: Mapping[str, Any],
    paths: CanaryPaths,
) -> None:
    content_id = int(source["content"]["id"])
    link_id = str(source["content"]["link_id"])
    processor_type = str(row["processor_type"])
    if processor_type == "download":
        _current_contract_upstream_sha(
            connection,
            contract=contract,
            source=source,
            paths=paths,
            processor_type="frames",
        )
        return
    artifact_type_by_processor = {
        "frames": "frames_manifest",
        "asr": "asr",
        "ocr": "ocr",
        "duplicate_fingerprint": "duplicate_fingerprint",
    }
    artifact_type = artifact_type_by_processor.get(processor_type)
    if artifact_type is None:
        raise LocalAnalysisCanaryError("succeeded recovery processor无输出合同")
    artifacts_by_content, _media_files, _fingerprint_files = (
        _validate_generated_artifacts(
            connection,
            contract=contract,
            paths=paths,
            content_ids=[content_id],
        )
    )
    artifact = artifacts_by_content[content_id].get(artifact_type)
    versions = media_module.processor_versions()
    expected_version = (
        duplicates_module.FINGERPRINT_VERSION
        if processor_type == "duplicate_fingerprint"
        else str(versions[processor_type])
    )
    expected_path = (
        paths.fingerprint_root / f"{link_id}.json"
        if processor_type == "duplicate_fingerprint"
        else paths.media_root / link_id / "frames" / "frames.json"
        if processor_type == "frames"
        else paths.media_root / link_id / f"{processor_type}.json"
    ).resolve()
    if (
        artifact is None
        or row["output_artifact_id"] != artifact["id"]
        or artifact["processor_version"] != expected_version
        or artifact["metadata_json"] != "{}"
        or _resolve_artifact_path(str(artifact["local_path"])) != expected_path
    ):
        raise LocalAnalysisCanaryError(
            "succeeded recovery slot output artifact闭包漂移"
        )
    if processor_type == "frames":
        _validate_frames_final_orphan(expected_path, paths=paths, source=source)
    elif processor_type == "asr":
        _validate_asr_final_orphan(expected_path)
    elif processor_type == "ocr":
        _validate_ocr_final_orphan(
            expected_path,
            paths=paths,
            source=source,
            expected_source_sha256=str(row["source_sha256"]),
        )
    else:
        _validate_fingerprint_final_orphan(
            expected_path,
            paths=paths,
            content_id=content_id,
            expected_source_sha256=str(row["source_sha256"]),
        )


def _validate_owned_running_slot(
    connection: sqlite3.Connection,
    *,
    row: sqlite3.Row,
    contract: Mapping[str, Any],
    paths: CanaryPaths,
    require_stale: bool = True,
    require_no_output: bool = True,
) -> None:
    if require_no_output and row["output_artifact_id"] is not None:
        raise LocalAnalysisCanaryError("running slot已有output artifact，拒绝自动恢复")
    if int(row["attempt_count"]) <= 0:
        raise LocalAnalysisCanaryError("running slot attempt_count无效")
    if require_stale:
        cutoff = datetime.now(timezone.utc) - timedelta(
            seconds=media_module.STALE_MEDIA_SLOT_SECONDS
        )
        if _parse_timestamp(row["updated_at"]) > cutoff:
            raise LocalAnalysisCanaryError("running媒体slot尚未达到陈旧恢复边界")
    content_id = int(row["content_id"])
    source_by_id = {
        int(source["content"]["id"]): source
        for source in contract.get("sources") or []
        if isinstance(source, Mapping) and isinstance(source.get("content"), Mapping)
    }
    source = source_by_id.get(content_id)
    if source is None:
        raise LocalAnalysisCanaryError("running slot不属于当前intent target")
    processor_type = str(row["processor_type"])
    processor_version = str(row["processor_version"])
    source_sha = str(row["source_sha256"])
    media_kind = str(source["artifact_body"]["media_kind"])
    versions = media_module.processor_versions()
    if processor_type == "download":
        expected_sha = (
            str(source["artifact_body"]["source_sha256"])
            if media_kind == "video"
            else _source_image_download_binding(source)
        )
        expected_version = (
            media_module.VIDEO_DOWNLOAD_VERSION
            if media_kind == "video"
            else media_module.IMAGE_DOWNLOAD_VERSION
        )
    elif processor_type in {"frames", "asr"}:
        expected_sha = _current_contract_upstream_sha(
            connection,
            contract=contract,
            source=source,
            paths=paths,
            processor_type=processor_type,
        )
        expected_version = str(versions[processor_type])
    elif processor_type == "ocr":
        expected_sha = _current_contract_upstream_sha(
            connection,
            contract=contract,
            source=source,
            paths=paths,
            processor_type=processor_type,
        )
        expected_version = str(versions["ocr"])
    elif processor_type == "duplicate_fingerprint":
        _, expected_sha = duplicates_module._current_source_state(
            connection, content_id
        )
        expected_version = duplicates_module.FINGERPRINT_VERSION
    else:
        raise LocalAnalysisCanaryError(
            f"running slot processor不属于canary：{processor_type}"
        )
    if source_sha != expected_sha or processor_version != expected_version:
        raise LocalAnalysisCanaryError(
            "running slot source/processor不属于当前intent精确证据"
        )
    if str(row["status"]) == "succeeded":
        _validate_succeeded_owned_slot_output(
            connection,
            row=row,
            contract=contract,
            source=source,
            paths=paths,
        )


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_args: Any, **_kwargs: Any) -> None:
        return None


def _recover_chained_json_temp(
    path: Path,
    *,
    label: str,
    previous_field: str,
    validator: Callable[[Mapping[str, Any]], None],
) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    if not os.path.lexists(temporary):
        return
    candidate = _read_json(temporary, label=f"{label}临时文件")
    validator(candidate)
    expected_previous = _sha256_file(path) if path.exists() else None
    if candidate.get(previous_field) != expected_previous:
        raise LocalAnalysisCanaryError(f"{label}临时文件前序SHA漂移")
    os.replace(temporary, path)
    _fsync_directory(path.parent)


class _NetworkLedger:
    def __init__(
        self,
        path: Path,
        *,
        contract_sha256: str,
        intent_sha256: str,
        content_ids: Sequence[int],
        maximum_bytes: int,
        sources: Mapping[int, Mapping[str, Any]],
        recover_incomplete: bool = True,
    ) -> None:
        if (
            not content_ids
            or any(type(content_id) is not int for content_id in content_ids)
            or len(set(content_ids)) != len(content_ids)
            or type(maximum_bytes) is not int
            or maximum_bytes <= 0
            or type(recover_incomplete) is not bool
        ):
            raise LocalAnalysisCanaryError(
                "network ledger初始化ID/bytes/bool类型无效"
            )
        self.path = path
        self.contract_sha256 = contract_sha256
        self.intent_sha256 = intent_sha256
        self.content_ids = list(content_ids)
        self.maximum_bytes = maximum_bytes
        self._source_urls = {
            content_id: {
                str(url): str(row["host"])
                for row in sources[content_id]["urls"]
                for url in [row["url"]]
                if str(url) in set(_source_urls(sources[content_id]))
            }
            for content_id in self.content_ids
        }

        def validate(value: Mapping[str, Any]) -> None:
            self._validate(value)

        _recover_chained_json_temp(
            path,
            label="network ledger",
            previous_field="previous_ledger_sha256",
            validator=validate,
        )
        if path.exists():
            self.value = dict(_read_json(path, label="network ledger"))
            self._validate(self.value)
            changed = False
            for event in self.value["events"]:
                if event["outcome"] in {"opening", "opened"}:
                    if not recover_incomplete:
                        raise LocalAnalysisCanaryError(
                            "success receipt绑定了非终态network event"
                        )
                    event["outcome"] = "interrupted"
                    event["error"] = "controller interrupted before response close"
                    changed = True
            if changed:
                self._persist()
        else:
            self.value = {
                "schema_version": SCHEMA_VERSION,
                "contract_sha256": contract_sha256,
                "intent_sha256": intent_sha256,
                "content_ids": self.content_ids,
                "maximum_bytes": self.maximum_bytes,
                "events": [],
                "total_bytes": 0,
                "budget_consumed_bytes": 0,
                "overrun": False,
                "update_index": 0,
                "previous_ledger_sha256": None,
            }
            _write_json(path, self.value, immutable=False)

    def _validate(self, value: Mapping[str, Any]) -> None:
        content_ids = value.get("content_ids")
        maximum_bytes = value.get("maximum_bytes")
        if set(value) != {
            "schema_version",
            "contract_sha256",
            "intent_sha256",
            "content_ids",
            "maximum_bytes",
            "events",
            "total_bytes",
            "budget_consumed_bytes",
            "overrun",
            "update_index",
            "previous_ledger_sha256",
        } or (
            value.get("schema_version") != SCHEMA_VERSION
            or value.get("contract_sha256") != self.contract_sha256
            or value.get("intent_sha256") != self.intent_sha256
            or not isinstance(content_ids, list)
            or any(type(content_id) is not int for content_id in content_ids)
            or content_ids != self.content_ids
            or type(maximum_bytes) is not int
            or maximum_bytes != self.maximum_bytes
        ):
            raise LocalAnalysisCanaryError("network ledger合同漂移")
        events = value.get("events")
        if not isinstance(events, list):
            raise LocalAnalysisCanaryError("network ledger events形状无效")
        total = 0
        charged = 0
        event_keys = {
            "event_index",
            "content_id",
            "url_sha256",
            "host",
            "status",
            "mime",
            "declared_bytes",
            "bytes",
            "charged_bytes",
            "response_sha256",
            "outcome",
            "error",
        }
        for index, event in enumerate(events, start=1):
            if not isinstance(event, Mapping) or set(event) != event_keys:
                raise LocalAnalysisCanaryError("network ledger event形状漂移")
            event_index = event.get("event_index")
            content_id = event.get("content_id")
            status = event.get("status")
            declared_bytes = event.get("declared_bytes")
            byte_count = event.get("bytes")
            charged_count = event.get("charged_bytes")
            response_sha256 = event.get("response_sha256")
            if (
                type(event_index) is not int
                or event_index != index
                or type(content_id) is not int
                or content_id not in self._source_urls
            ):
                raise LocalAnalysisCanaryError("network ledger event顺序或content漂移")
            matching = [
                (url, host)
                for url, host in self._source_urls[content_id].items()
                if _sha256_bytes(url.encode("utf-8")) == event["url_sha256"]
            ]
            if len(matching) != 1 or matching[0][1] != event["host"]:
                raise LocalAnalysisCanaryError("network ledger URL/host未绑定冻结source")
            if (
                type(event.get("url_sha256")) is not str
                or re.fullmatch(r"[0-9a-f]{64}", event["url_sha256"])
                is None
                or type(event.get("host")) is not str
                or (status is not None and type(status) is not int)
                or (declared_bytes is not None and type(declared_bytes) is not int)
                or (declared_bytes is not None and declared_bytes < 0)
                or type(byte_count) is not int
                or type(charged_count) is not int
                or (event.get("mime") is not None and type(event["mime"]) is not str)
                or (event.get("error") is not None and type(event["error"]) is not str)
                or type(event.get("outcome")) is not str
            ):
                raise LocalAnalysisCanaryError("network ledger event精确类型漂移")
            if event["outcome"] not in {
                "opening",
                "opened",
                "succeeded",
                "failed",
                "interrupted",
            }:
                raise LocalAnalysisCanaryError("network ledger outcome无效")
            if (
                byte_count < 0
                or charged_count < byte_count
                or (
                    response_sha256 is not None
                    and (
                        type(response_sha256) is not str
                        or re.fullmatch(r"[0-9a-f]{64}", response_sha256)
                        is None
                    )
                )
                or (event["outcome"] == "succeeded" and response_sha256 is None)
            ):
                raise LocalAnalysisCanaryError("network ledger bytes无效")
            outcome = event["outcome"]
            error = event.get("error")
            if (
                (
                    outcome == "opening"
                    and (
                        status is not None
                        or event.get("mime") is not None
                        or declared_bytes is not None
                        or byte_count != 0
                        or response_sha256 is not None
                        or error is not None
                    )
                )
                or (
                    outcome == "opened"
                    and (
                        status is None
                        or not 200 <= status < 300
                        or response_sha256 is not None
                        or error is not None
                    )
                )
                or (
                    outcome == "succeeded"
                    and (
                        status is None
                        or not 200 <= status < 300
                        or response_sha256 is None
                        or error is not None
                    )
                )
                or (
                    outcome == "failed"
                    and (type(error) is not str or not error)
                )
                or (
                    outcome == "interrupted"
                    and error != "controller interrupted before response close"
                )
            ):
                raise LocalAnalysisCanaryError(
                    "network ledger outcome终态语义漂移"
                )
            total += byte_count
            charged += charged_count
        total_bytes = value.get("total_bytes")
        budget_consumed_bytes = value.get("budget_consumed_bytes")
        overrun = value.get("overrun")
        update_index = value.get("update_index")
        previous = value.get("previous_ledger_sha256")
        if (
            type(total_bytes) is not int
            or total != total_bytes
            or type(budget_consumed_bytes) is not int
            or charged != budget_consumed_bytes
            or type(overrun) is not bool
            or overrun != (charged > self.maximum_bytes)
            or type(update_index) is not int
            or update_index < len(events)
            or (
                previous is not None
                and (
                    type(previous) is not str
                    or re.fullmatch(r"[0-9a-f]{64}", previous) is None
                )
            )
        ):
            raise LocalAnalysisCanaryError("network ledger累计下载证据漂移")

    def _persist(self) -> None:
        previous = _sha256_file(self.path) if self.path.exists() else None
        self.value["previous_ledger_sha256"] = previous
        self.value["update_index"] = int(self.value["update_index"]) + 1
        self.value["total_bytes"] = sum(
            int(event["bytes"]) for event in self.value["events"]
        )
        self.value["budget_consumed_bytes"] = sum(
            int(event["charged_bytes"]) for event in self.value["events"]
        )
        self.value["overrun"] = (
            int(self.value["budget_consumed_bytes"]) > self.maximum_bytes
        )
        self._validate(self.value)
        _write_json(self.path, self.value, immutable=False)

    @property
    def total_bytes(self) -> int:
        return int(self.value["total_bytes"])

    @property
    def budget_consumed_bytes(self) -> int:
        return int(self.value["budget_consumed_bytes"])

    @property
    def overrun(self) -> bool:
        return bool(self.value["overrun"])

    def begin(self, content_id: int, url: str) -> int:
        if type(content_id) is not int:
            raise LocalAnalysisCanaryError("network ledger content ID类型无效")
        host = self._source_urls.get(content_id, {}).get(url)
        if host is None:
            raise LocalAnalysisCanaryError("network ledger拒绝非冻结content URL")
        event_index = len(self.value["events"]) + 1
        self.value["events"].append(
            {
                "event_index": event_index,
                "content_id": content_id,
                "url_sha256": _sha256_bytes(url.encode("utf-8")),
                "host": host,
                "status": None,
                "mime": None,
                "declared_bytes": None,
                "bytes": 0,
                "charged_bytes": 0,
                "response_sha256": None,
                "outcome": "opening",
                "error": None,
            }
        )
        self._persist()
        return event_index

    def update(self, event_index: int, **changes: Any) -> None:
        if (
            type(event_index) is not int
            or event_index <= 0
            or event_index > len(self.value["events"])
        ):
            raise LocalAnalysisCanaryError("network ledger event index无效")
        event = self.value["events"][event_index - 1]
        unknown = set(changes) - {
            "status",
            "mime",
            "declared_bytes",
            "response_sha256",
            "outcome",
            "error",
        }
        if unknown:
            raise LocalAnalysisCanaryError("network ledger update字段无效")
        event.update(changes)
        self._persist()

    def reserve(self, event_index: int, byte_count: int) -> None:
        if type(event_index) is not int or type(byte_count) is not int:
            raise LocalAnalysisCanaryError("network ledger reserve精确类型无效")
        if byte_count <= 0:
            return
        event = self.value["events"][event_index - 1]
        event["charged_bytes"] = int(event["charged_bytes"]) + byte_count
        self._persist()
        if self.overrun:
            raise LocalAnalysisCanaryError("整个canary累计下载超过冻结上限")

    def consume(self, event_index: int, byte_count: int) -> None:
        if type(event_index) is not int or type(byte_count) is not int:
            raise LocalAnalysisCanaryError("network ledger consume精确类型无效")
        if byte_count <= 0:
            return
        event = self.value["events"][event_index - 1]
        if int(event["bytes"]) + byte_count > int(event["charged_bytes"]):
            event["charged_bytes"] = int(event["bytes"]) + byte_count
            event["bytes"] = int(event["bytes"]) + byte_count
            self._persist()
            raise LocalAnalysisCanaryError("network ledger读取字节超过预留预算")
        event["bytes"] = int(event["bytes"]) + byte_count
        self._persist()

    def transcript(self, content_id: int) -> list[Mapping[str, Any]]:
        if type(content_id) is not int:
            raise LocalAnalysisCanaryError("network ledger transcript ID类型无效")
        return [
            dict(event)
            for event in self.value["events"]
            if int(event["content_id"]) == content_id
        ]

    def require_terminal(self) -> None:
        if any(
            event["outcome"] in {"opening", "opened"}
            for event in self.value["events"]
        ):
            raise LocalAnalysisCanaryError("network ledger存在未收口event")


class _ProgressLedger:
    def __init__(
        self,
        path: Path,
        *,
        contract_sha256: str,
        intent_sha256: str,
        content_ids: Sequence[int],
        network_ledger: _NetworkLedger,
    ) -> None:
        self.path = path
        self.contract_sha256 = contract_sha256
        self.intent_sha256 = intent_sha256
        self.content_ids = list(content_ids)
        self.network_ledger = network_ledger

        def validate(value: Mapping[str, Any]) -> None:
            self._validate(value)

        _recover_chained_json_temp(
            path,
            label="analysis progress",
            previous_field="previous_progress_sha256",
            validator=validate,
        )
        if path.exists():
            self.value = dict(_read_json(path, label="analysis progress"))
            self._validate(self.value)
        else:
            self.value = {
                "schema_version": SCHEMA_VERSION,
                "contract_sha256": contract_sha256,
                "intent_sha256": intent_sha256,
                "content_ids": self.content_ids,
                "completed_ids": [],
                "results": [],
                "database": None,
                "network_ledger_sha256": _sha256_file(network_ledger.path),
                "update_index": 0,
                "previous_progress_sha256": None,
            }
            _write_json(path, self.value, immutable=False)

    def _validate(self, value: Mapping[str, Any]) -> None:
        if set(value) != {
            "schema_version",
            "contract_sha256",
            "intent_sha256",
            "content_ids",
            "completed_ids",
            "results",
            "database",
            "network_ledger_sha256",
            "update_index",
            "previous_progress_sha256",
        } or (
            value.get("schema_version") != SCHEMA_VERSION
            or value.get("contract_sha256") != self.contract_sha256
            or value.get("intent_sha256") != self.intent_sha256
            or list(value.get("content_ids") or []) != self.content_ids
        ):
            raise LocalAnalysisCanaryError("analysis progress合同漂移")
        completed = value.get("completed_ids")
        results = value.get("results")
        if (
            not isinstance(completed, list)
            or not isinstance(results, list)
            or completed != self.content_ids[: len(completed)]
            or len(completed) != len(results)
            or [
                int(result.get("content_id") or -1)
                for result in results
                if isinstance(result, Mapping)
            ]
            != completed
            or int(value.get("update_index") or 0) != len(completed)
        ):
            raise LocalAnalysisCanaryError("analysis progress完成前缀漂移")
        for result in results:
            if not isinstance(result, Mapping) or set(result) != {
                "content_id",
                "media",
                "evaluation",
                "fingerprint_source_sha256",
                "network_bytes",
                "network_transcript",
                "network_transcript_sha256",
            }:
                raise LocalAnalysisCanaryError("analysis progress result形状不精确")
            transcript = result.get("network_transcript")
            content_id = int(result["content_id"])
            media_result = result.get("media")
            evaluation_result = result.get("evaluation")
            if (
                not isinstance(media_result, Mapping)
                or set(media_result)
                != {"content_id", "status", "media_kind", "artifacts"}
                or int(media_result.get("content_id") or -1) != content_id
                or media_result.get("status") != "evidence_ready"
                or media_result.get("media_kind") not in {"video", "image"}
                or not isinstance(media_result.get("artifacts"), Mapping)
                or set(media_result["artifacts"])
                != (
                    {"media", "frames", "asr", "ocr"}
                    if media_result["media_kind"] == "video"
                    else {"media", "ocr"}
                )
                or any(
                    not isinstance(artifact_id, int) or artifact_id <= 0
                    for artifact_id in media_result["artifacts"].values()
                )
                or not isinstance(evaluation_result, Mapping)
                or set(evaluation_result)
                != {
                    "evaluation_id",
                    "evidence_sha256",
                    "evidence_level",
                    "created",
                }
                or not isinstance(evaluation_result.get("evaluation_id"), int)
                or int(evaluation_result["evaluation_id"]) <= 0
                or not re.fullmatch(
                    r"[0-9a-f]{64}", str(evaluation_result.get("evidence_sha256"))
                )
                or evaluation_result.get("evidence_level") not in {"V2", "V3"}
                or not isinstance(evaluation_result.get("created"), bool)
                or not re.fullmatch(
                    r"[0-9a-f]{64}",
                    str(result.get("fingerprint_source_sha256")),
                )
                or not isinstance(transcript, list)
                or result.get("network_transcript_sha256") != _json_sha256(transcript)
                or transcript != self.network_ledger.transcript(content_id)
                or int(result.get("network_bytes") or 0)
                != sum(int(event["bytes"]) for event in transcript)
            ):
                raise LocalAnalysisCanaryError("analysis progress网络证据漂移")
        database = value.get("database")
        if completed:
            if (
                not isinstance(database, Mapping)
                or set(database)
                != {"path", "sha256", "byte_size", "inode", "nlink"}
                or not Path(str(database.get("path") or "")).is_absolute()
                or not re.fullmatch(r"[0-9a-f]{64}", str(database.get("sha256")))
                or not isinstance(database.get("byte_size"), int)
                or int(database["byte_size"]) <= 0
                or not isinstance(database.get("inode"), int)
                or int(database["inode"]) <= 0
                or database.get("nlink") != 1
            ):
                raise LocalAnalysisCanaryError("analysis progress缺少精确数据库证据")
        elif database is not None:
            raise LocalAnalysisCanaryError("analysis progress空前缀不得绑定数据库")
        ledger_sha = value.get("network_ledger_sha256")
        if not re.fullmatch(r"[0-9a-f]{64}", str(ledger_sha)):
            raise LocalAnalysisCanaryError("analysis progress ledger SHA无效")

    @property
    def results(self) -> list[Mapping[str, Any]]:
        return [dict(value) for value in self.value["results"]]

    @property
    def completed_ids(self) -> list[int]:
        return [int(value) for value in self.value["completed_ids"]]

    @property
    def database(self) -> Mapping[str, Any] | None:
        value = self.value["database"]
        return dict(value) if isinstance(value, Mapping) else None

    def append(self, result: Mapping[str, Any], *, database: Mapping[str, Any]) -> None:
        next_index = len(self.value["completed_ids"])
        if next_index >= len(self.content_ids) or int(result["content_id"]) != self.content_ids[next_index]:
            raise LocalAnalysisCanaryError("analysis progress只能按冻结ID顺序追加")
        previous = _sha256_file(self.path)
        self.value["completed_ids"].append(int(result["content_id"]))
        self.value["results"].append(dict(result))
        self.value["database"] = dict(database)
        self.value["network_ledger_sha256"] = _sha256_file(self.network_ledger.path)
        self.value["previous_progress_sha256"] = previous
        self.value["update_index"] = int(self.value["update_index"]) + 1
        self._validate(self.value)
        _write_json(self.path, self.value, immutable=False)

    def checkpoint_network(self) -> None:
        """Bind a handled failure checkpoint to the latest durable network head."""

        ledger_sha256 = _sha256_file(self.network_ledger.path)
        if self.value["network_ledger_sha256"] == ledger_sha256:
            return
        previous = _sha256_file(self.path)
        self.value["network_ledger_sha256"] = ledger_sha256
        self.value["previous_progress_sha256"] = previous
        self._validate(self.value)
        _write_json(self.path, self.value, immutable=False)


@dataclass
class _DownloadBudget:
    maximum_bytes: int
    consumed_bytes: int = 0
    ledger: _NetworkLedger | None = None

    @property
    def remaining_bytes(self) -> int:
        return self.maximum_bytes - self.consumed_bytes

    def consume(self, byte_count: int, *, event_index: int | None = None) -> None:
        if self.ledger is not None:
            if event_index is None:
                raise LocalAnalysisCanaryError("持久化下载预算缺少event index")
            self.ledger.consume(event_index, byte_count)
            self.consumed_bytes = self.ledger.budget_consumed_bytes
            return
        self.consumed_bytes += max(0, byte_count)
        if self.consumed_bytes > self.maximum_bytes:
            raise LocalAnalysisCanaryError("整个canary累计下载超过冻结上限")

    def reserve(self, byte_count: int, *, event_index: int | None = None) -> None:
        if self.ledger is None:
            return
        if event_index is None:
            raise LocalAnalysisCanaryError("持久化下载预留缺少event index")
        self.ledger.reserve(event_index, byte_count)
        self.consumed_bytes = self.ledger.budget_consumed_bytes


class _CumulativeResponse:
    def __init__(
        self,
        response: Any,
        guard: "ExactUrlNetworkGuard",
        request_url: str,
        event_index: int | None,
    ) -> None:
        self._response = response
        self._guard = guard
        self._finished = False
        self._event_index = event_index
        self._declared_bytes: int | None = None
        self._response_digest = hashlib.sha256()
        raw_length = response.headers.get("Content-Length") if response.headers else None
        content_type = (
            str(response.headers.get("Content-Type") or "").split(";", 1)[0].lower()
            if response.headers
            else ""
        )
        response_status = int(getattr(response, "status", 0) or 0)
        if not 200 <= response_status < 300:
            guard._update_event(
                event_index,
                status=response_status,
                mime=content_type or None,
                outcome="failed",
                error="media response status is not 2xx",
            )
            response.close()
            raise LocalAnalysisCanaryError(
                f"canary媒体响应HTTP状态无效：{response_status}"
            )
        if guard.media_kind == "video" and content_type.startswith("audio/"):
            guard._update_event(
                event_index,
                status=response_status,
                mime=content_type,
                outcome="failed",
                error="video source returned audio MIME",
            )
            response.close()
            raise media_module.MediaProcessingError(
                "视频source响应实际是audio MIME"
            )
        declared: int | None = None
        if raw_length is not None:
            try:
                declared = int(str(raw_length).strip())
            except (TypeError, ValueError):
                guard._update_event(
                    event_index,
                    status=response_status,
                    mime=content_type or None,
                    declared_bytes=None,
                    outcome="failed",
                    error="invalid Content-Length",
                )
                response.close()
                raise LocalAnalysisCanaryError("canary媒体响应Content-Length无效")
            if declared < 0:
                guard._update_event(
                    event_index,
                    status=response_status,
                    mime=content_type or None,
                    declared_bytes=None,
                    outcome="failed",
                    error="negative Content-Length",
                )
                response.close()
                raise LocalAnalysisCanaryError("canary媒体响应Content-Length为负")
            if declared > guard.remaining_bytes:
                guard._update_event(
                    event_index,
                    status=response_status,
                    mime=content_type or None,
                    declared_bytes=declared,
                    outcome="failed",
                    error="declared response exceeds remaining budget",
                )
                response.close()
                raise LocalAnalysisCanaryError(
                    "canary媒体响应声明长度超过run剩余累计上限"
                )
            self._declared_bytes = declared
            if declared:
                guard.reserve(declared, event_index=event_index)
        self._entry: dict[str, Any] = {
            "url_sha256": _sha256_bytes(request_url.encode("utf-8")),
            "host": (urllib.parse.urlsplit(request_url).hostname or "").lower(),
            "status": response_status,
            "mime": content_type or None,
            "declared_bytes": declared,
            "bytes": 0,
        }
        guard._update_event(
            event_index,
            status=self._entry["status"],
            mime=self._entry["mime"],
            declared_bytes=self._entry["declared_bytes"],
            outcome="opened",
            error=None,
        )

    def read(self, size: int = -1) -> bytes:
        effective_size = size
        if self._guard.has_persistent_ledger and self._declared_bytes is None:
            remaining = self._guard.remaining_bytes
            if remaining <= 0:
                raise LocalAnalysisCanaryError("整个canary累计下载已达到冻结上限")
            effective_size = remaining if size < 0 else min(size, remaining)
            self._guard.reserve(effective_size, event_index=self._event_index)
        body = self._response.read(effective_size)
        self._guard.consume(len(body), event_index=self._event_index)
        self._response_digest.update(body)
        self._entry["bytes"] = int(self._entry["bytes"]) + len(body)
        return body

    def _finish(self, *, error: BaseException | None = None) -> None:
        if not self._finished:
            incomplete = bool(
                error is None
                and self._declared_bytes is not None
                and int(self._entry["bytes"]) != self._declared_bytes
            )
            if incomplete:
                error = LocalAnalysisCanaryError(
                    "媒体响应在声明Content-Length前提前结束"
                )
            self._guard._finish_event(
                self._event_index,
                self._entry,
                response_sha256=self._response_digest.hexdigest(),
                error=error,
            )
            self._finished = True
            if incomplete:
                assert error is not None
                raise error

    def __getattr__(self, name: str) -> Any:
        return getattr(self._response, name)

    def __enter__(self) -> "_CumulativeResponse":
        return self

    def __exit__(self, *args: Any) -> Any:
        try:
            return self._response.__exit__(*args)
        finally:
            error = args[1] if len(args) >= 2 and isinstance(args[1], BaseException) else None
            self._finish(error=error)


class ExactUrlNetworkGuard:
    def __init__(
        self,
        urls: Sequence[str],
        *,
        media_kind: str,
        maximum_bytes: int,
        budget: _DownloadBudget | None = None,
        ledger: _NetworkLedger | None = None,
        content_id: int | None = None,
    ) -> None:
        self.urls = frozenset(urls)
        self.hosts = frozenset(
            (urllib.parse.urlsplit(url).hostname or "").lower() for url in urls
        )
        self._local = threading.local()
        self._ledger = ledger
        self._content_id = content_id
        if (ledger is None) != (content_id is None):
            raise LocalAnalysisCanaryError("network ledger/content ID必须成对提供")
        self._budget = budget or _DownloadBudget(
            maximum_bytes,
            consumed_bytes=ledger.total_bytes if ledger is not None else 0,
            ledger=ledger,
        )
        if ledger is not None and self._budget.ledger is not ledger:
            raise LocalAnalysisCanaryError("network guard预算未绑定同一持久化ledger")
        if self._budget.maximum_bytes != maximum_bytes:
            raise LocalAnalysisCanaryError("网络guard累计预算与run contract不一致")
        self.media_kind = media_kind
        self._transcript: list[Mapping[str, Any]] = []
        self._opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}), _NoRedirect()
        )

    @property
    def remaining_bytes(self) -> int:
        return self._budget.remaining_bytes

    @property
    def has_persistent_ledger(self) -> bool:
        return self._ledger is not None

    @property
    def consumed_bytes(self) -> int:
        return self._budget.consumed_bytes

    @property
    def transcript(self) -> list[Mapping[str, Any]]:
        if self._ledger is not None and self._content_id is not None:
            return self._ledger.transcript(self._content_id)
        return list(self._transcript)

    def consume(self, byte_count: int, *, event_index: int | None = None) -> None:
        self._budget.consume(byte_count, event_index=event_index)

    def reserve(self, byte_count: int, *, event_index: int | None = None) -> None:
        self._budget.reserve(byte_count, event_index=event_index)

    def _update_event(self, event_index: int | None, **changes: Any) -> None:
        if self._ledger is not None:
            if event_index is None:
                raise LocalAnalysisCanaryError("network ledger更新缺少event index")
            self._ledger.update(event_index, **changes)

    def _finish_event(
        self,
        event_index: int | None,
        entry: Mapping[str, Any],
        *,
        response_sha256: str,
        error: BaseException | None,
    ) -> None:
        if self._ledger is not None:
            if event_index is None:
                raise LocalAnalysisCanaryError("network ledger终态缺少event index")
            self._ledger.update(
                event_index,
                response_sha256=response_sha256,
                outcome="failed" if error is not None else "succeeded",
                error=(f"{type(error).__name__}: {error}"[:500] if error else None),
            )
            return
        self._transcript.append(
            {**dict(entry), "response_sha256": response_sha256}
        )

    def _request_url(self, request: Any) -> str:
        if isinstance(request, urllib.request.Request):
            return str(request.full_url)
        return str(request)

    def open(self, request: Any, data: Any = None, timeout: float = 90) -> Any:
        url = self._request_url(request)
        if data is not None or url not in self.urls:
            raise LocalAnalysisCanaryError(f"网络tripwire拒绝非冻结URL：{url}")
        if self._ledger is not None and self.remaining_bytes <= 0:
            raise LocalAnalysisCanaryError("整个canary累计下载已达到或超过冻结上限")
        event_index = (
            self._ledger.begin(int(self._content_id), url)
            if self._ledger is not None and self._content_id is not None
            else None
        )
        self._local.active = True
        self._local.url = url
        try:
            response = self._opener.open(request, data=None, timeout=timeout)
            return _CumulativeResponse(response, self, url, event_index)
        except BaseException as exc:
            if self._ledger is not None and event_index is not None:
                self._ledger.update(
                    event_index,
                    outcome="failed",
                    error=f"{type(exc).__name__}: {exc}"[:500],
                )
            raise
        finally:
            self._local.active = False
            self._local.url = None

    def allow_socket(self, address: Any) -> None:
        if not getattr(self._local, "active", False):
            raise LocalAnalysisCanaryError("网络tripwire拒绝urllib之外的socket连接")
        host = str(address[0] if isinstance(address, tuple) else address).strip("[]").lower()
        try:
            ip = ipaddress.ip_address(host)
        except ValueError:
            if host not in self.hosts:
                raise LocalAnalysisCanaryError(f"网络tripwire拒绝主机：{host}")
            return
        if not ip.is_global:
            raise LocalAnalysisCanaryError(f"网络tripwire拒绝非公网地址：{ip}")


def _deny(*_args: Any, **_kwargs: Any) -> Any:
    raise LocalAnalysisCanaryError("provider/HuggingFace远程调用被canary硬拒绝")


def _module_function_patches(stack: ExitStack, module: ModuleType) -> None:
    for name, value in vars(module).items():
        if callable(value) and getattr(value, "__module__", None) == module.__name__:
            stack.enter_context(patch.object(module, name, _deny))


@contextmanager
def _execution_guards(
    urls: Sequence[str],
    *,
    media_kind: str,
    maximum_bytes: int,
    tools: Mapping[str, Any],
    budget: _DownloadBudget | None = None,
    ledger: _NetworkLedger | None = None,
    content_id: int | None = None,
) -> Iterator[ExactUrlNetworkGuard]:
    guard = ExactUrlNetworkGuard(
        urls,
        media_kind=media_kind,
        maximum_bytes=maximum_bytes,
        budget=budget,
        ledger=ledger,
        content_id=content_id,
    )
    original_create = socket.create_connection
    original_getaddrinfo = socket.getaddrinfo
    original_connect = socket.socket.connect
    original_connect_ex = socket.socket.connect_ex
    original_popen = subprocess.Popen

    def guarded_create(address: Any, *args: Any, **kwargs: Any) -> Any:
        guard.allow_socket(address)
        return original_create(address, *args, **kwargs)

    def guarded_connect(instance: socket.socket, address: Any) -> Any:
        guard.allow_socket(address)
        return original_connect(instance, address)

    def guarded_connect_ex(instance: socket.socket, address: Any) -> Any:
        guard.allow_socket(address)
        return original_connect_ex(instance, address)

    def guarded_getaddrinfo(host: Any, *args: Any, **kwargs: Any) -> Any:
        normalized = str(host).strip("[]").lower()
        if not getattr(guard._local, "active", False) or normalized not in guard.hosts:
            raise LocalAnalysisCanaryError(
                f"网络tripwire拒绝非冻结DNS查询：{normalized}"
            )
        return original_getaddrinfo(host, *args, **kwargs)

    def deny_datagram(*_args: Any, **_kwargs: Any) -> Any:
        raise LocalAnalysisCanaryError("网络tripwire拒绝UDP/sendmsg旁路")

    def guarded_popen(command: Any, *args: Any, **kwargs: Any) -> Any:
        if kwargs.get("shell") or kwargs.get("executable") is not None:
            raise LocalAnalysisCanaryError("网络tripwire拒绝shell/executable子进程")
        if not isinstance(command, (list, tuple)) or not command:
            raise LocalAnalysisCanaryError("网络tripwire仅允许argv形式本地子进程")
        executable = (
            command[0]
            if isinstance(command, (list, tuple)) and command
            else command
        )
        value = str(executable)
        resolved_value = shutil.which(value) if not Path(value).is_absolute() else value
        if resolved_value is None:
            raise LocalAnalysisCanaryError(f"网络tripwire无法解析子进程：{value}")
        resolved_path = str(Path(resolved_value).resolve())
        allowed_tools = {
            str(Path(str(tool["path"])).resolve()): (name, tool)
            for name in ("ffmpeg", "ffprobe", "ocr_binary")
            if isinstance((tool := tools.get(name)), Mapping)
        }
        command_values = (
            [str(item) for item in command]
            if isinstance(command, (list, tuple))
            else [str(command)]
        )
        frozen = allowed_tools.get(resolved_path)
        protocol_argument = re.compile(
            r"(?i)^(?:https?|tcp|udp|rtmp|rtsp|srt|ftp|concat|crypto|data):"
        )
        if frozen is None or any(
            "://" in item or protocol_argument.match(item)
            for item in command_values[1:]
        ):
            raise LocalAnalysisCanaryError(
                f"网络tripwire拒绝非媒体本地子进程：{value}"
            )
        tool_name, frozen_tool = frozen
        if any(
            item in {"-protocol_whitelist", "-protocol_blacklist"}
            for item in command_values[1:]
        ):
            raise LocalAnalysisCanaryError("网络tripwire拒绝调用方覆盖媒体协议门")
        executable_path = Path(resolved_path)
        metadata = _private_file(executable_path, label="冻结本地子进程")
        if (
            _sha256_file(executable_path) != frozen_tool.get("sha256")
            or metadata.st_size != int(frozen_tool.get("byte_size") or -1)
        ):
            raise LocalAnalysisCanaryError("冻结本地子进程SHA/bytes漂移")
        protocol_guard = (
            ["-protocol_whitelist", "file,pipe"]
            if tool_name in {"ffmpeg", "ffprobe"}
            else []
        )
        rewritten = [resolved_path, *protocol_guard, *command_values[1:]]
        return original_popen(rewritten, *args, **kwargs)

    with ExitStack() as stack:
        stack.enter_context(patch("urllib.request.urlopen", guard.open))
        stack.enter_context(patch("socket.create_connection", guarded_create))
        stack.enter_context(patch("socket.getaddrinfo", guarded_getaddrinfo))
        stack.enter_context(patch.object(socket.socket, "connect", guarded_connect))
        stack.enter_context(patch.object(socket.socket, "connect_ex", guarded_connect_ex))
        stack.enter_context(patch.object(socket.socket, "sendto", deny_datagram))
        if hasattr(socket.socket, "sendmsg"):
            stack.enter_context(patch.object(socket.socket, "sendmsg", deny_datagram))
        stack.enter_context(patch("subprocess.Popen", guarded_popen))
        stack.enter_context(patch.object(media_module, "snapshot_download", _deny))
        _module_function_patches(stack, providers_module)
        for name in ("execute_account_fetch", "execute_content_fetch"):
            if hasattr(capture_module, name):
                stack.enter_context(patch.object(capture_module, name, _deny))
        yield guard


def _state_value(
    paths: CanaryPaths,
    *,
    status: str,
    contract_sha256: str,
    intent_sha256: str,
    error: str | None = None,
) -> Mapping[str, Any]:
    previous_sha = _sha256_file(paths.state) if paths.state.exists() else None
    network_ledger = _read_json(paths.network_ledger, label="state network ledger")
    progress = _read_json(paths.progress, label="state analysis progress")
    network_events = list(network_ledger["events"])
    completed_ids = list(progress["completed_ids"])
    progress_results = list(progress["results"])
    return {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "updated_at": _now_text(),
        "contract_sha256": contract_sha256,
        "intent_sha256": intent_sha256,
        "receipt_sha256": _sha256_file(paths.receipt) if paths.receipt.exists() else None,
        "database": _database_identity(paths.database),
        "network_ledger_sha256": _sha256_file(paths.network_ledger),
        "network_total_bytes": int(network_ledger["total_bytes"]),
        "download_budget_consumed_bytes": int(
            network_ledger["budget_consumed_bytes"]
        ),
        "network_event_count": len(network_events),
        "network_events_prefix_sha256": _json_sha256(network_events),
        "progress_sha256": _sha256_file(paths.progress),
        "progress_completed_count": len(completed_ids),
        "progress_results_prefix_sha256": _json_sha256(
            {"completed_ids": completed_ids, "results": progress_results}
        ),
        "previous_state_sha256": previous_sha,
        "error": error,
    }


def _validate_intent_value(
    paths: CanaryPaths,
    contract: Mapping[str, Any],
    value: Mapping[str, Any],
    content_ids: Sequence[int],
) -> None:
    if set(value) != {
        "schema_version",
        "created_at",
        "owner",
        "contract_sha256",
        "content_ids",
        "content_ids_sha256",
        "before_database",
    }:
        raise LocalAnalysisCanaryError("intent形状不精确")
    owner = value.get("owner")
    if (
        value.get("schema_version") != SCHEMA_VERSION
        or value.get("contract_sha256") != _sha256_file(paths.contract)
        or list(value.get("content_ids") or []) != list(content_ids)
        or value.get("content_ids_sha256") != _json_sha256(list(content_ids))
        or value.get("before_database") != contract.get("database")
        or not isinstance(owner, Mapping)
        or set(owner) != {"pid", "process_identity_sha256"}
        or int(owner.get("pid") or 0) <= 0
        or not re.fullmatch(
            r"[0-9a-f]{64}", str(owner.get("process_identity_sha256") or "")
        )
    ):
        raise LocalAnalysisCanaryError("intent合同或owner身份漂移")
    _parse_timestamp(value.get("created_at"))


def _validate_state_value(
    paths: CanaryPaths,
    value: Mapping[str, Any],
    *,
    contract_sha256: str,
    intent_sha256: str,
    allow_retryable_predecessor: bool = False,
    allow_retryable_checkpoint_extension: bool = False,
) -> None:
    if set(value) != {
        "schema_version",
        "status",
        "updated_at",
        "contract_sha256",
        "intent_sha256",
        "receipt_sha256",
        "database",
        "network_ledger_sha256",
        "network_total_bytes",
        "download_budget_consumed_bytes",
        "network_event_count",
        "network_events_prefix_sha256",
        "progress_sha256",
        "progress_completed_count",
        "progress_results_prefix_sha256",
        "previous_state_sha256",
        "error",
    }:
        raise LocalAnalysisCanaryError("analysis state形状不精确")
    status = str(value.get("status") or "")
    receipt_sha256 = value.get("receipt_sha256")
    expected_receipt_sha256 = (
        _sha256_file(paths.receipt) if paths.receipt.exists() else None
    )
    previous = value.get("previous_state_sha256")
    database = value.get("database")
    current_database = _database_identity(paths.database)
    network_ledger = _read_json(paths.network_ledger, label="state network ledger")
    progress = _read_json(paths.progress, label="state analysis progress")
    current_ledger_sha256 = _sha256_file(paths.network_ledger)
    current_progress_sha256 = _sha256_file(paths.progress)
    current_events = list(network_ledger.get("events") or [])
    current_completed_ids = list(progress.get("completed_ids") or [])
    current_results = list(progress.get("results") or [])
    state_event_count = (
        int(value["network_event_count"])
        if type(value.get("network_event_count")) is int
        else -1
    )
    state_completed_count = (
        int(value["progress_completed_count"])
        if type(value.get("progress_completed_count")) is int
        else -1
    )
    retryable_predecessor_database = bool(
        allow_retryable_predecessor
        and status == "retryable_failed"
        and isinstance(database, Mapping)
        and set(database) == {"path", "sha256", "byte_size", "inode", "nlink"}
        and database.get("path") == str(paths.database)
        and re.fullmatch(r"[0-9a-f]{64}", str(database.get("sha256") or ""))
        and int(database.get("byte_size") or -1) >= 0
        and int(database.get("inode") or -1) == int(current_database["inode"])
        and int(database.get("nlink") or -1) == 1
    )
    retryable_checkpoint_database = bool(
        allow_retryable_checkpoint_extension
        and status == "retryable_failed"
        and isinstance(database, Mapping)
        and set(database) == {"path", "sha256", "byte_size", "inode", "nlink"}
        and database.get("path") == str(paths.database)
        and re.fullmatch(r"[0-9a-f]{64}", str(database.get("sha256") or ""))
        and int(database.get("byte_size") or -1) >= 0
        and int(database.get("inode") or -1) == int(current_database["inode"])
        and int(database.get("nlink") or -1) == 1
    )
    retryable_predecessor_checkpoint = bool(
        allow_retryable_predecessor
        and status == "retryable_failed"
        and re.fullmatch(
            r"[0-9a-f]{64}", str(value.get("network_ledger_sha256") or "")
        )
        and re.fullmatch(
            r"[0-9a-f]{64}", str(value.get("progress_sha256") or "")
        )
        and re.fullmatch(
            r"[0-9a-f]{64}",
            str(value.get("network_events_prefix_sha256") or ""),
        )
        and re.fullmatch(
            r"[0-9a-f]{64}",
            str(value.get("progress_results_prefix_sha256") or ""),
        )
        and isinstance(value.get("network_total_bytes"), int)
        and int(value["network_total_bytes"]) >= 0
        and isinstance(value.get("download_budget_consumed_bytes"), int)
        and int(value["download_budget_consumed_bytes"])
        >= int(value["network_total_bytes"])
        and isinstance(value.get("network_event_count"), int)
        and int(value["network_event_count"]) >= 0
        and isinstance(value.get("progress_completed_count"), int)
        and int(value["progress_completed_count"]) >= 0
    )
    retryable_checkpoint_extension = bool(
        allow_retryable_checkpoint_extension
        and status == "retryable_failed"
        and re.fullmatch(
            r"[0-9a-f]{64}", str(value.get("network_ledger_sha256") or "")
        )
        and re.fullmatch(
            r"[0-9a-f]{64}", str(value.get("progress_sha256") or "")
        )
        and state_event_count >= 0
        and len(current_events) >= state_event_count
        and re.fullmatch(
            r"[0-9a-f]{64}",
            str(value.get("network_events_prefix_sha256") or ""),
        )
        and _json_sha256(current_events[:state_event_count])
        == value.get("network_events_prefix_sha256")
        and sum(int(event.get("bytes") or 0) for event in current_events[:state_event_count])
        == int(value.get("network_total_bytes") or 0)
        and sum(
            int(event.get("charged_bytes") or 0)
            for event in current_events[:state_event_count]
        )
        == int(value.get("download_budget_consumed_bytes") or 0)
        and state_completed_count >= 0
        and len(current_completed_ids) >= state_completed_count
        and len(current_results) >= state_completed_count
        and re.fullmatch(
            r"[0-9a-f]{64}",
            str(value.get("progress_results_prefix_sha256") or ""),
        )
        and _json_sha256(
            {
                "completed_ids": current_completed_ids[:state_completed_count],
                "results": current_results[:state_completed_count],
            }
        )
        == value.get("progress_results_prefix_sha256")
        and (
            len(current_completed_ids) > state_completed_count
            or progress.get("network_ledger_sha256")
            in {
                value.get("network_ledger_sha256"),
                current_ledger_sha256,
            }
        )
    )
    exact_checkpoint = bool(
        value.get("network_ledger_sha256") == current_ledger_sha256
        and int(value.get("network_total_bytes") or 0)
        == int(network_ledger.get("total_bytes") or 0)
        and int(value.get("download_budget_consumed_bytes") or 0)
        == int(network_ledger.get("budget_consumed_bytes") or 0)
        and state_event_count == len(current_events)
        and value.get("network_events_prefix_sha256")
        == _json_sha256(current_events)
        and value.get("progress_sha256") == current_progress_sha256
        and state_completed_count == len(current_completed_ids)
        and value.get("progress_results_prefix_sha256")
        == _json_sha256(
            {"completed_ids": current_completed_ids, "results": current_results}
        )
        and progress.get("network_ledger_sha256") == current_ledger_sha256
    )
    if (
        value.get("schema_version") != SCHEMA_VERSION
        or status not in {"retryable_failed", "succeeded"}
        or value.get("contract_sha256") != contract_sha256
        or value.get("intent_sha256") != intent_sha256
        or not (
            retryable_predecessor_checkpoint
            or exact_checkpoint
            or retryable_checkpoint_extension
        )
        or (
            allow_retryable_predecessor
            and status == "retryable_failed"
            and not retryable_predecessor_checkpoint
        )
        or (
            database != current_database
            and not retryable_predecessor_database
            and not retryable_checkpoint_database
        )
        or (
            previous is not None
            and not re.fullmatch(r"[0-9a-f]{64}", str(previous))
        )
    ):
        raise LocalAnalysisCanaryError("analysis state合同或数据库证据漂移")
    _parse_timestamp(value.get("updated_at"))
    if status == "succeeded":
        if (
            expected_receipt_sha256 is None
            or receipt_sha256 != expected_receipt_sha256
            or value.get("error") is not None
        ):
            raise LocalAnalysisCanaryError("success state未绑定当前receipt")
    elif (
        receipt_sha256 is not None
        or not isinstance(value.get("error"), str)
        or not str(value.get("error"))
    ):
        raise LocalAnalysisCanaryError("retryable state证据不精确")


def _recover_immutable_json_temp(
    path: Path,
    *,
    label: str,
    validator: Callable[[Mapping[str, Any]], None],
) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    if not os.path.lexists(temporary):
        return
    candidate = _read_json(temporary, label=f"{label}临时文件")
    validator(candidate)
    if path.exists():
        current = _read_json(path, label=label)
        validator(current)
        if temporary.read_bytes() != path.read_bytes():
            raise LocalAnalysisCanaryError(f"{label}临时文件与终态内容漂移")
        temporary.unlink()
    else:
        os.replace(temporary, path)
    _fsync_directory(path.parent)


def _cleanup_duplicate_final_temp(path: Path, *, label: str) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    if not path.exists() or not os.path.lexists(temporary):
        return
    _private_file(path, label=label)
    _private_file(temporary, label=f"{label}临时文件")
    if temporary.read_bytes() != path.read_bytes():
        raise LocalAnalysisCanaryError(f"{label}临时文件与既有终态漂移")
    temporary.unlink()
    _fsync_directory(path.parent)


def _recover_state_temp(
    paths: CanaryPaths,
    *,
    contract_sha256: str,
    intent_sha256: str,
    receipt: Mapping[str, Any] | None,
) -> None:
    temporary = paths.state.with_name(f".{paths.state.name}.tmp")
    if not os.path.lexists(temporary):
        return
    if receipt is None:
        _recover_chained_json_temp(
            paths.state,
            label="analysis state",
            previous_field="previous_state_sha256",
            validator=lambda value: _validate_state_value(
                paths,
                value,
                contract_sha256=contract_sha256,
                intent_sha256=intent_sha256,
                allow_retryable_predecessor=False,
            ),
        )
        return
    candidate = _read_json(temporary, label="analysis state临时文件")
    pre_receipt_sha256 = receipt.get("pre_receipt_state_sha256")
    if (
        candidate.get("status") != "succeeded"
        or candidate.get("previous_state_sha256") != pre_receipt_sha256
    ):
        raise LocalAnalysisCanaryError("receipt后state临时文件不是精确成功相邻态")
    _validate_state_value(
        paths,
        candidate,
        contract_sha256=contract_sha256,
        intent_sha256=intent_sha256,
        allow_retryable_predecessor=False,
    )
    if pre_receipt_sha256 is None:
        if paths.state.exists():
            raise LocalAnalysisCanaryError("receipt声明无前序state但终态文件已存在")
    elif (
        not paths.state.exists()
        or _sha256_file(paths.state) != pre_receipt_sha256
    ):
        raise LocalAnalysisCanaryError("receipt前序state文件证据漂移")
    os.replace(temporary, paths.state)
    _fsync_directory(paths.state.parent)


def _validate_receipt(
    paths: CanaryPaths,
    contract: Mapping[str, Any],
    receipt: Mapping[str, Any],
    content_ids: Sequence[int],
) -> None:
    if set(receipt) != {
        "schema_version",
        "status",
        "created_at",
        "contract_sha256",
        "intent_sha256",
        "content_ids",
        "processed",
        "startup_recovery",
        "provider_calls",
        "network_bytes_total",
        "download_budget_consumed_bytes",
        "network_ledger_sha256",
        "progress_sha256",
        "network_transcript_sha256",
        "pre_receipt_state_sha256",
        "after_database",
        "evidence",
    }:
        raise LocalAnalysisCanaryError("receipt形状不精确")
    _parse_timestamp(receipt.get("created_at"))
    if (
        receipt.get("schema_version") != SCHEMA_VERSION
        or receipt.get("status") != "succeeded"
    ):
        raise LocalAnalysisCanaryError("receipt不是成功终态")
    if receipt.get("contract_sha256") != _sha256_file(paths.contract):
        raise LocalAnalysisCanaryError("receipt未绑定当前contract")
    if receipt.get("intent_sha256") != _sha256_file(paths.intent):
        raise LocalAnalysisCanaryError("receipt未绑定当前intent")
    if receipt.get("after_database") != _database_identity(paths.database):
        raise LocalAnalysisCanaryError("receipt数据库证据漂移")
    pre_receipt_state_sha256 = receipt.get("pre_receipt_state_sha256")
    if (
        pre_receipt_state_sha256 is not None
        and not re.fullmatch(r"[0-9a-f]{64}", str(pre_receipt_state_sha256))
    ):
        raise LocalAnalysisCanaryError("receipt前序state SHA无效")
    if list(receipt.get("content_ids") or []) != list(content_ids):
        raise LocalAnalysisCanaryError("receipt content IDs漂移")
    processed = receipt.get("processed")
    if not isinstance(processed, list) or [
        int(item.get("content_id") or -1)
        for item in processed
        if isinstance(item, Mapping)
    ] != list(content_ids):
        raise LocalAnalysisCanaryError("receipt processed IDs不精确")
    if int(receipt.get("provider_calls") or 0) != 0:
        raise LocalAnalysisCanaryError("receipt provider_calls不是0")
    if any(
        record.with_name(f".{record.name}.tmp").exists()
        for record in (paths.network_ledger, paths.progress)
    ):
        raise LocalAnalysisCanaryError("success receipt后存在未收口ledger/progress临时文件")
    if (
        not paths.network_ledger.exists()
        or receipt.get("network_ledger_sha256")
        != _sha256_file(paths.network_ledger)
        or not paths.progress.exists()
        or receipt.get("progress_sha256") != _sha256_file(paths.progress)
    ):
        raise LocalAnalysisCanaryError("receipt未绑定持久化network/progress证据")
    source_by_id = {
        int(source["content"]["id"]): source for source in contract["sources"]
    }
    ledger = _NetworkLedger(
        paths.network_ledger,
        contract_sha256=_sha256_file(paths.contract),
        intent_sha256=_sha256_file(paths.intent),
        content_ids=content_ids,
        maximum_bytes=int(contract["maximum_download_bytes"]),
        sources=source_by_id,
        recover_incomplete=False,
    )
    progress = _ProgressLedger(
        paths.progress,
        contract_sha256=_sha256_file(paths.contract),
        intent_sha256=_sha256_file(paths.intent),
        content_ids=content_ids,
        network_ledger=ledger,
    )
    if (
        progress.results != processed
        or progress.completed_ids != list(content_ids)
        or progress.database != receipt["after_database"]
        or progress.value["network_ledger_sha256"]
        != receipt["network_ledger_sha256"]
    ):
        raise LocalAnalysisCanaryError("receipt processed未精确绑定durable progress")
    _validate_processed_results(paths, contract, content_ids, processed)
    total_network_bytes = 0
    transcript_rows: list[Mapping[str, Any]] = []
    for item in processed:
        transcript = item.get("network_transcript")
        if not isinstance(transcript, list) or item.get(
            "network_transcript_sha256"
        ) != _json_sha256(transcript):
            raise LocalAnalysisCanaryError("receipt network transcript漂移")
        byte_count = int(item.get("network_bytes") or 0)
        if byte_count < 0:
            raise LocalAnalysisCanaryError("receipt network bytes无效")
        total_network_bytes += byte_count
        transcript_rows.append(
            {"content_id": int(item["content_id"]), "transcript": transcript}
        )
    if (
        int(receipt.get("network_bytes_total") or 0) != total_network_bytes
        or total_network_bytes != ledger.total_bytes
        or int(receipt.get("download_budget_consumed_bytes") or 0)
        != ledger.budget_consumed_bytes
        or ledger.overrun
        or receipt.get("network_transcript_sha256")
        != _json_sha256(transcript_rows)
    ):
        raise LocalAnalysisCanaryError("receipt run级网络证据漂移")
    startup_recovery = receipt.get("startup_recovery")
    if not isinstance(startup_recovery, Mapping):
        raise LocalAnalysisCanaryError("receipt缺少startup recovery证据")
    base_recovery_keys = {
        "running_candidates",
        "recovered",
        "recovered_now",
        "terminal",
        "running_recovery_sha256",
        "slot_attempt_expectations",
        "output_recovered",
        "output_recovery_rounds",
        "output_recovery_sha256",
    }
    if frozenset(startup_recovery) not in {
        frozenset(base_recovery_keys),
        frozenset(base_recovery_keys | {"recovery_rounds"}),
    } or any(
        not isinstance(startup_recovery.get(key), int)
        or int(startup_recovery[key]) < 0
        for key in ("running_candidates", "recovered", "recovered_now", "terminal")
    ):
        raise LocalAnalysisCanaryError("receipt startup recovery形状不精确")
    recovery_sha = startup_recovery.get("running_recovery_sha256")
    if recovery_sha is None:
        if dict(startup_recovery) != {
            "running_candidates": 0,
            "recovered": 0,
            "recovered_now": 0,
            "terminal": 0,
            "running_recovery_sha256": None,
            "slot_attempt_expectations": [],
            "output_recovered": 0,
            "output_recovery_rounds": 0,
            "output_recovery_sha256": None,
        } or paths.running_recovery.exists():
            raise LocalAnalysisCanaryError("receipt空running recovery证据漂移")
    elif (
        not paths.running_recovery.exists()
        or recovery_sha != _sha256_file(paths.running_recovery)
    ):
        raise LocalAnalysisCanaryError("receipt running recovery文件证据漂移")
    expectations = startup_recovery.get("slot_attempt_expectations") or []
    if not isinstance(expectations, list):
        raise LocalAnalysisCanaryError("receipt slot attempt expectations无效")
    if recovery_sha is not None:
        recovery = _read_json(paths.running_recovery, label="receipt attempt recovery")
        history_rows, recovered_expectations = _validate_running_recovery_record(
            recovery,
            contract=contract,
            content_ids=content_ids,
            contract_sha256=_sha256_file(paths.contract),
            intent_sha256=_sha256_file(paths.intent),
        )
        if (
            expectations != recovered_expectations
            or int(startup_recovery["recovered"]) != len(history_rows)
            or int(startup_recovery["running_candidates"])
            != sum(row["from_status"] == "running" for row in history_rows)
            or int(startup_recovery.get("recovery_rounds") or 0)
            != len(recovery["rounds"])
            or int(startup_recovery["recovered_now"])
            > len(history_rows)
            or int(startup_recovery["terminal"]) != 0
        ):
            raise LocalAnalysisCanaryError("receipt attempt recovery聚合证据漂移")
    output_recovery_sha256 = startup_recovery.get("output_recovery_sha256")
    output_recovered = startup_recovery.get("output_recovered")
    output_recovery_rounds = startup_recovery.get("output_recovery_rounds")
    if (
        not isinstance(output_recovered, int)
        or output_recovered < 0
        or not isinstance(output_recovery_rounds, int)
        or output_recovery_rounds < 0
    ):
        raise LocalAnalysisCanaryError("receipt output recovery聚合无效")
    if output_recovery_sha256 is None:
        if (
            output_recovered != 0
            or output_recovery_rounds != 0
            or paths.output_recovery.exists()
        ):
            raise LocalAnalysisCanaryError("receipt空output recovery证据漂移")
    else:
        if (
            not paths.output_recovery.exists()
            or output_recovery_sha256 != _sha256_file(paths.output_recovery)
        ):
            raise LocalAnalysisCanaryError("receipt output recovery文件SHA漂移")
        output_recovery = _read_json(
            paths.output_recovery, label="receipt output recovery"
        )
        output_rows = _validate_output_recovery_record(
            output_recovery,
            paths=paths,
            contract=contract,
            contract_sha256=_sha256_file(paths.contract),
            intent_sha256=_sha256_file(paths.intent),
        )
        if (
            len(output_rows) != output_recovered
            or len(output_recovery["rounds"]) != output_recovery_rounds
        ):
            raise LocalAnalysisCanaryError("receipt output recovery聚合证据漂移")
    evidence = _validate_target_success(
        paths,
        contract,
        content_ids,
        slot_attempt_expectations=expectations,
        network_ledger=ledger,
    )
    if receipt.get("evidence") != evidence:
        raise LocalAnalysisCanaryError("receipt分析/物理证据漂移")


def _validate_receipt_temp_candidate(
    paths: CanaryPaths,
    contract: Mapping[str, Any],
    receipt: Mapping[str, Any],
    content_ids: Sequence[int],
) -> None:
    _validate_receipt(paths, contract, receipt, content_ids)
    pre_receipt_sha256 = receipt.get("pre_receipt_state_sha256")
    if pre_receipt_sha256 is None:
        if paths.state.exists():
            raise LocalAnalysisCanaryError(
                "receipt临时文件声明无前序state但文件已存在"
            )
        return
    if (
        not paths.state.exists()
        or _sha256_file(paths.state) != pre_receipt_sha256
    ):
        raise LocalAnalysisCanaryError("receipt临时文件前序state SHA漂移")
    state = _read_json(paths.state, label="receipt前序analysis state")
    _validate_state_value(
        paths,
        state,
        contract_sha256=_sha256_file(paths.contract),
        intent_sha256=_sha256_file(paths.intent),
        allow_retryable_predecessor=True,
    )
    if state.get("status") != "retryable_failed":
        raise LocalAnalysisCanaryError("receipt临时文件前序state不是retryable")


def _source_urls(source: Mapping[str, Any]) -> list[str]:
    urls = source.get("download_urls")
    if not isinstance(urls, list) or not all(isinstance(value, str) for value in urls):
        raise LocalAnalysisCanaryError("contract缺少冻结download URL列表")
    if source.get("download_urls_sha256") != _json_sha256(urls):
        raise LocalAnalysisCanaryError("contract download URL列表SHA漂移")
    return list(urls)


def _ordered_content_ids(content_ids: Sequence[int]) -> list[int]:
    ordered_ids = list(content_ids)
    if any(type(value) is not int or value <= 0 for value in ordered_ids):
        raise LocalAnalysisCanaryError("至少需要一个正整数 --content-id")
    if not ordered_ids:
        raise LocalAnalysisCanaryError("至少需要一个正整数 --content-id")
    if len(set(ordered_ids)) != len(ordered_ids):
        raise LocalAnalysisCanaryError("--content-id 不得重复")
    return ordered_ids


def plan_canary(
    *,
    source_db_path: Path,
    source_completion_path: Path,
    expected_source_db_sha256: str,
    expected_source_completion_sha256: str,
    db_path: Path,
    media_root: Path,
    run_root: Path,
    content_ids: Sequence[int],
) -> Mapping[str, Any]:
    """Validate and freeze the proposed canary without creating any files."""

    ordered_ids = _ordered_content_ids(content_ids)
    paths = _paths(
        source_db_path=source_db_path,
        source_completion_path=source_completion_path,
        db_path=db_path,
        media_root=media_root,
        run_root=run_root,
    )
    _validate_paths(paths, work_database_must_exist=False)
    if (
        paths.database.exists()
        or paths.copy_partial.exists()
        or _database_sidecars(paths.database)
    ):
        raise LocalAnalysisCanaryError(
            "只读plan要求全新work database且无partial/sidecar"
        )
    for root, label in (
        (paths.media_root, "media_root"),
        (paths.fingerprint_root, "fingerprint_root"),
        (paths.run_root, "run_root"),
    ):
        if os.path.lexists(root):
            _private_directory(root, label=f"plan {label}")
            if any(root.iterdir()):
                raise LocalAnalysisCanaryError(f"只读plan要求空输出根：{root}")
    source_evidence = _source_completion_evidence(
        paths,
        content_ids=ordered_ids,
        expected_source_db_sha256=expected_source_db_sha256,
        expected_source_completion_sha256=expected_source_completion_sha256,
    )
    _require_paid_source_handoff_fresh(source_evidence)
    _validate_source_separation(paths, source_evidence)
    disk = _disk_capacity(paths)
    with closing(_immutable_connection(paths.source_database)) as connection:
        sources = _completion_source_snapshots(
            connection, ordered_ids, source_evidence
        )
        release = connection.execute(
            "SELECT id FROM evaluation_releases WHERE status='active'"
        ).fetchone()
        if release is None:
            raise LocalAnalysisCanaryError("Step3源数据库缺少active evaluation release")
        existing = _existing_analysis_counts(connection, ordered_ids)
        if any(existing.values()):
            raise LocalAnalysisCanaryError(
                f"只读plan只接受未分析explicit IDs：{existing}"
            )
    require_whisper = any(
        source["artifact_body"]["media_kind"] == "video" for source in sources
    )
    tools = _local_tools(require_whisper=require_whisper)
    code = _code_snapshot()
    return {
        "ok": True,
        "status": "planned",
        "apply": False,
        "content_ids": ordered_ids,
        "content_ids_sha256": _json_sha256(ordered_ids),
        "source_database": source_evidence["database"],
        "source_completion": source_evidence,
        "work_database_path": str(paths.database),
        "media_root": str(paths.media_root),
        "fingerprint_root": str(paths.fingerprint_root),
        "run_root": str(paths.run_root),
        "sources_sha256": _json_sha256(sources),
        "source_summary": [
            {
                "content_id": int(source["content"]["id"]),
                "media_kind": source["artifact_body"]["media_kind"],
                "url_count": len(source["urls"]),
                "network_allowed_url_count": sum(
                    bool(row["network_allowed"]) for row in source["urls"]
                ),
                "logical_image_count": (
                    len(source["image_groups"])
                    if source["artifact_body"]["media_kind"] == "image"
                    else None
                ),
                "hosts": sorted({str(row["host"]) for row in source["urls"]}),
                "network_denied_hosts": sorted(
                    {
                        str(row["host"])
                        for row in source["urls"]
                        if not bool(row["network_allowed"])
                    }
                ),
                "urls_sha256": source["urls_sha256"],
            }
            for source in sources
        ],
        "maximum_download_bytes_total": MAX_DOWNLOAD_BYTES,
        "disk": disk,
        "tools": tools,
        "code": code,
        "code_sha256": _json_sha256(code),
    }


def run_canary(
    *,
    source_db_path: Path,
    source_completion_path: Path,
    expected_source_db_sha256: str,
    expected_source_completion_sha256: str,
    db_path: Path,
    media_root: Path,
    run_root: Path,
    content_ids: Sequence[int],
) -> Mapping[str, Any]:
    ordered_ids = _ordered_content_ids(content_ids)
    paths = _paths(
        source_db_path=source_db_path,
        source_completion_path=source_completion_path,
        db_path=db_path,
        media_root=media_root,
        run_root=run_root,
    )
    existing_contract = paths.contract.exists()
    _validate_paths(paths, work_database_must_exist=existing_contract)
    source_evidence = _source_completion_evidence(
        paths,
        content_ids=ordered_ids,
        expected_source_db_sha256=expected_source_db_sha256,
        expected_source_completion_sha256=expected_source_completion_sha256,
    )
    _validate_source_separation(paths, source_evidence)
    _disk_capacity(paths)
    initial_sidecars = _database_sidecars(paths.database)
    if initial_sidecars and not (paths.contract.exists() and paths.intent.exists()):
        raise LocalAnalysisCanaryError(
            "首次或无intent的数据库clone存在未知sidecar，拒绝恢复"
        )
    with _all_claims(paths):
        current_sidecars = _database_sidecars(paths.database)
        if current_sidecars:
            if not (paths.contract.exists() and paths.intent.exists()):
                raise LocalAnalysisCanaryError("持锁后发现无合同绑定的数据库sidecar")
            _validate_sidecar_recovery_records(paths, ordered_ids)
            _validate_recoverable_sidecars(paths)
            _finalize_database(paths.database)
        elif paths.database.exists():
            _require_clean_database(paths.database)
        _prepare_roots(paths)
        _validate_run_root(paths, allow_atomic_temps=True)
        for record, label in (
            (paths.copy_intent, "database copy intent"),
            (paths.copy_receipt, "database copy receipt"),
            (paths.contract, "run contract"),
        ):
            _cleanup_duplicate_final_temp(record, label=label)
        first_contract = not paths.contract.exists()
        allowed_copy_records = {paths.copy_intent.name, paths.copy_receipt.name}
        allowed_first_records = allowed_copy_records | {
            f".{paths.copy_intent.name}.tmp",
            f".{paths.copy_receipt.name}.tmp",
            f".{paths.contract.name}.tmp",
        }
        run_entries = {path.name for path in paths.run_root.iterdir()}
        if first_contract and (
            any(paths.media_root.iterdir())
            or any(paths.fingerprint_root.iterdir())
            or not run_entries.issubset(allowed_first_records)
        ):
            raise LocalAnalysisCanaryError("首次canary要求三个隔离输出根为空")
        if first_contract:
            _require_paid_source_handoff_fresh(source_evidence)
            _ensure_work_copy(
                paths,
                content_ids=ordered_ids,
                source_evidence=source_evidence,
                expected_source_db_sha256=expected_source_db_sha256,
                expected_source_completion_sha256=expected_source_completion_sha256,
            )
            contract = _build_contract(
                paths,
                ordered_ids,
                source_evidence=source_evidence,
            )
            contract_sha = _write_json(paths.contract, contract, immutable=True)
        else:
            contract = _read_json(paths.contract, label="run contract")
            contract_sha = _sha256_file(paths.contract)
        _validate_contract(
            paths,
            contract,
            ordered_ids,
            expected_source_db_sha256=expected_source_db_sha256,
            expected_source_completion_sha256=expected_source_completion_sha256,
        )
        _recover_immutable_json_temp(
            paths.intent,
            label="intent",
            validator=lambda value: _validate_intent_value(
                paths, contract, value, ordered_ids
            ),
        )
        if paths.intent.exists():
            recovered_intent = _read_json(paths.intent, label="intent")
            _validate_intent_value(
                paths, contract, recovered_intent, ordered_ids
            )
            _recover_immutable_json_temp(
                paths.receipt,
                label="success receipt",
                validator=lambda value: _validate_receipt_temp_candidate(
                    paths, contract, value, ordered_ids
                ),
            )
            recovered_receipt = (
                _read_json(paths.receipt, label="success receipt")
                if paths.receipt.exists()
                else None
            )
            if recovered_receipt is not None:
                _validate_receipt(
                    paths, contract, recovered_receipt, ordered_ids
                )
            _recover_state_temp(
                paths,
                contract_sha256=contract_sha,
                intent_sha256=_sha256_file(paths.intent),
                receipt=recovered_receipt,
            )
            if paths.state.exists():
                recovered_state = _read_json(paths.state, label="analysis state")
                _validate_state_value(
                    paths,
                    recovered_state,
                    contract_sha256=contract_sha,
                    intent_sha256=_sha256_file(paths.intent),
                    allow_retryable_predecessor=recovered_receipt is not None,
                    allow_retryable_checkpoint_extension=(
                        recovered_receipt is None
                    ),
                )
                if (
                    not paths.receipt.exists()
                    and recovered_state.get("status") != "retryable_failed"
                ):
                    raise LocalAnalysisCanaryError(
                        "无receipt时analysis state不是retryable终态"
                    )
        if paths.receipt.exists():
            receipt = _read_json(paths.receipt, label="success receipt")
            _validate_receipt(paths, contract, receipt, ordered_ids)
            if paths.state.exists():
                state = _read_json(paths.state, label="success state")
                _validate_state_value(
                    paths,
                    state,
                    contract_sha256=contract_sha,
                    intent_sha256=_sha256_file(paths.intent),
                    allow_retryable_predecessor=True,
                )
                if (
                    state.get("status") == "retryable_failed"
                    and _sha256_file(paths.state)
                    != receipt.get("pre_receipt_state_sha256")
                ) or (
                    state.get("status") == "succeeded"
                    and state.get("previous_state_sha256")
                    != receipt.get("pre_receipt_state_sha256")
                ):
                    raise LocalAnalysisCanaryError(
                        "success receipt与前序state链不一致"
                    )
            elif receipt.get("pre_receipt_state_sha256") is not None:
                raise LocalAnalysisCanaryError("success receipt绑定的前序state缺失")
            if not paths.state.exists() or state.get("status") == "retryable_failed":
                state = _state_value(
                    paths,
                    status="succeeded",
                    contract_sha256=contract_sha,
                    intent_sha256=_sha256_file(paths.intent),
                )
                _write_json(paths.state, state, immutable=False)
            _validate_run_root(paths, allow_atomic_temps=False)
            return {
                "ok": True,
                "status": "succeeded",
                "idempotent": True,
                "content_ids": ordered_ids,
                "provider_calls": 0,
                "receipt_sha256": _sha256_file(paths.receipt),
            }
        intent_exists = paths.intent.exists()
        existing_intent = (
            _read_json(paths.intent, label="intent") if intent_exists else None
        )
        intent = {
            "schema_version": SCHEMA_VERSION,
            "created_at": (
                existing_intent.get("created_at")
                if existing_intent is not None
                else _now_text()
            ),
            "owner": (
                existing_intent.get("owner")
                if existing_intent is not None
                else _current_owner()
            ),
            "contract_sha256": contract_sha,
            "content_ids": ordered_ids,
            "content_ids_sha256": _json_sha256(ordered_ids),
            "before_database": contract["database"],
        }
        intent_sha = _write_json(paths.intent, intent, immutable=True)
        _validate_intent_value(paths, contract, intent, ordered_ids)
        startup_recovery = _recover_owned_running_slots(
            paths,
            ordered_ids,
            intent_exists=intent_exists,
            intent=intent,
            contract=contract,
        )
        _finalize_database(paths.database)
        _validate_contract(
            paths,
            contract,
            ordered_ids,
            expected_source_db_sha256=expected_source_db_sha256,
            expected_source_completion_sha256=expected_source_completion_sha256,
        )
        sources = {
            int(source["content"]["id"]): source for source in contract["sources"]
        }
        tools = contract["tools"]
        ocr_binary = Path(str(tools["ocr_binary"]["path"]))
        if not paths.network_ledger.exists() and not paths.progress.exists():
            _require_paid_source_handoff_fresh(source_evidence)
        network_ledger = _NetworkLedger(
            paths.network_ledger,
            contract_sha256=contract_sha,
            intent_sha256=intent_sha,
            content_ids=ordered_ids,
            maximum_bytes=int(contract["maximum_download_bytes"]),
            sources=sources,
        )
        progress = _ProgressLedger(
            paths.progress,
            contract_sha256=contract_sha,
            intent_sha256=intent_sha,
            content_ids=ordered_ids,
            network_ledger=network_ledger,
        )
        if not network_ledger.value["events"] and not progress.completed_ids:
            _require_paid_source_handoff_fresh(source_evidence)
        startup_recovery = {
            **startup_recovery,
            **_recover_owned_output_partials(
                paths,
                contract=contract,
                content_ids=ordered_ids,
                slot_attempt_expectations=startup_recovery[
                    "slot_attempt_expectations"
                ],
                network_ledger=network_ledger,
            ),
        }
        _validate_prewrite_outputs(
            paths,
            contract=contract,
            content_ids=ordered_ids,
            completed_ids=progress.completed_ids,
            ledger=network_ledger,
            slot_attempt_expectations=startup_recovery[
                "slot_attempt_expectations"
            ],
        )
        results: list[Mapping[str, Any]] = progress.results
        completed_ids = set(progress.completed_ids)
        run_download_budget = _DownloadBudget(
            int(contract["maximum_download_bytes"]),
            consumed_bytes=network_ledger.budget_consumed_bytes,
            ledger=network_ledger,
        )
        try:
            for content_id in ordered_ids:
                if content_id in completed_ids:
                    continue
                source = sources[content_id]
                with closing(_immutable_connection(paths.database)) as connection:
                    before_source = _completion_source_snapshot(
                        connection, content_id, contract["source_completion"]
                    )
                if before_source != source:
                    raise LocalAnalysisCanaryError(
                        f"content {content_id} 网络前source证据漂移"
                    )
                urls = _source_urls(source)
                maximum_bytes = int(contract["maximum_download_bytes"])
                before_network_bytes = run_download_budget.consumed_bytes
                with _execution_guards(
                    urls,
                    media_kind=str(source["artifact_body"]["media_kind"]),
                    maximum_bytes=maximum_bytes,
                    tools=tools,
                    budget=run_download_budget,
                    ledger=network_ledger,
                    content_id=content_id,
                ) as network:
                    media_result = media_module.process_content_media(
                        content_id,
                        db_path=paths.database,
                        media_root=paths.media_root,
                        whisper_model_path=(
                            Path(str(tools["whisper"]["path"]))
                            if "whisper" in tools
                            else None
                        ),
                        ocr_binary=ocr_binary,
                        urlopen_fn=network.open,
                        maximum_download_bytes=maximum_bytes,
                        require_exact_response_url=True,
                        download_urls=urls,
                        frozen_image_groups=(
                            _source_image_groups(source)
                            if str(source["artifact_body"]["media_kind"])
                            == "image"
                            else None
                        ),
                        reuse_existing_downloads=False,
                        maximum_video_duration_seconds=int(
                            contract["maximum_video_duration_seconds"]
                        ),
                    )
                    if media_result.get("status") != "evidence_ready":
                        raise LocalAnalysisCanaryError(
                            f"content {content_id} 未达到evidence_ready：{media_result}"
                        )
                    evaluation = evaluation_module.evaluate_content(
                        content_id, db_path=paths.database
                    )
                    if evaluation.evidence_level not in {"V2", "V3"}:
                        raise LocalAnalysisCanaryError(
                            f"content {content_id} evaluation证据等级不足："
                            f"{evaluation.evidence_level}"
                        )
                    fingerprint = duplicates_module.fingerprint_content(
                        content_id, db_path=paths.database
                    )
                with closing(_immutable_connection(paths.database)) as connection:
                    current_source = _completion_source_snapshot(
                        connection, content_id, contract["source_completion"]
                    )
                if current_source != source:
                    raise LocalAnalysisCanaryError(
                        f"content {content_id} 处理过程中source证据漂移"
                    )
                result = {
                    "content_id": content_id,
                    "media": media_result,
                    "evaluation": {
                        "evaluation_id": evaluation.evaluation_id,
                        "evidence_sha256": evaluation.evidence_sha256,
                        "evidence_level": evaluation.evidence_level,
                        "created": evaluation.created,
                    },
                    "fingerprint_source_sha256": fingerprint.get("source_sha256"),
                    "network_bytes": sum(
                        int(event["bytes"]) for event in network.transcript
                    ),
                    "network_transcript": list(network.transcript),
                    "network_transcript_sha256": _json_sha256(
                        list(network.transcript)
                    ),
                }
                if network.consumed_bytes < before_network_bytes:
                    raise LocalAnalysisCanaryError("run累计下载预算发生回退")
                _finalize_database(paths.database)
                progress.append(result, database=_database_identity(paths.database))
                results = progress.results
        except Exception as exc:
            with contextlib.suppress(Exception):
                _finalize_database(paths.database)
            progress.checkpoint_network()
            state = _state_value(
                paths,
                status="retryable_failed",
                contract_sha256=contract_sha,
                intent_sha256=intent_sha,
                error=f"{type(exc).__name__}: {exc}",
            )
            _write_json(paths.state, state, immutable=False)
            if isinstance(exc, LocalAnalysisCanaryError):
                raise
            raise LocalAnalysisCanaryError(
                f"本地分析canary失败，可用同一contract恢复：{type(exc).__name__}: {exc}"
            ) from exc
        _finalize_database(paths.database)
        _validate_frozen_inputs(
            paths,
            contract,
            ordered_ids,
            expected_source_db_sha256=expected_source_db_sha256,
            expected_source_completion_sha256=expected_source_completion_sha256,
        )
        _disk_capacity(paths)
        network_ledger.require_terminal()
        evidence = _validate_target_success(
            paths,
            contract,
            ordered_ids,
            slot_attempt_expectations=startup_recovery[
                "slot_attempt_expectations"
            ],
            network_ledger=network_ledger,
        )
        after_database = _database_identity(paths.database)
        if (
            progress.completed_ids != ordered_ids
            or progress.database != after_database
        ):
            raise LocalAnalysisCanaryError(
                "成功progress未精确绑定最终数据库identity"
            )
        _validate_processed_results(paths, contract, ordered_ids, results)
        receipt = {
            "schema_version": SCHEMA_VERSION,
            "status": "succeeded",
            "created_at": _now_text(),
            "contract_sha256": contract_sha,
            "intent_sha256": intent_sha,
            "content_ids": ordered_ids,
            "processed": results,
            "startup_recovery": startup_recovery,
            "provider_calls": 0,
            "network_bytes_total": network_ledger.total_bytes,
            "download_budget_consumed_bytes": (
                network_ledger.budget_consumed_bytes
            ),
            "network_ledger_sha256": _sha256_file(paths.network_ledger),
            "progress_sha256": _sha256_file(paths.progress),
            "network_transcript_sha256": _json_sha256(
                [
                    {
                        "content_id": int(item["content_id"]),
                        "transcript": item["network_transcript"],
                    }
                    for item in results
                ]
            ),
            "pre_receipt_state_sha256": (
                _sha256_file(paths.state) if paths.state.exists() else None
            ),
            "after_database": after_database,
            "evidence": evidence,
        }
        receipt_sha = _write_json(paths.receipt, receipt, immutable=True)
        state = _state_value(
            paths,
            status="succeeded",
            contract_sha256=contract_sha,
            intent_sha256=intent_sha,
        )
        _write_json(paths.state, state, immutable=False)
        _validate_run_root(paths, allow_atomic_temps=False)
        return {
            "ok": True,
            "status": "succeeded",
            "idempotent": False,
            "content_ids": ordered_ids,
            "provider_calls": 0,
            "receipt_sha256": receipt_sha,
        }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run explicit local-only analysis against a disposable clone."
    )
    parser.add_argument("--source-db", required=True, type=Path)
    parser.add_argument("--source-completion", required=True, type=Path)
    parser.add_argument("--expected-source-db-sha256", required=True)
    parser.add_argument("--expected-source-completion-sha256", required=True)
    parser.add_argument("--db", required=True, type=Path, help="New O_EXCL work database")
    parser.add_argument("--media-root", required=True, type=Path)
    parser.add_argument("--run-root", required=True, type=Path)
    parser.add_argument(
        "--content-id",
        required=True,
        action="append",
        type=int,
        help="Exact content ID; repeat for a multi-content canary.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply after the default read-only plan succeeds.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        keyword_arguments = {
            "source_db_path": arguments.source_db,
            "source_completion_path": arguments.source_completion,
            "expected_source_db_sha256": arguments.expected_source_db_sha256,
            "expected_source_completion_sha256": (
                arguments.expected_source_completion_sha256
            ),
            "db_path": arguments.db,
            "media_root": arguments.media_root,
            "run_root": arguments.run_root,
            "content_ids": arguments.content_id,
        }
        result = (
            run_canary(**keyword_arguments)
            if arguments.apply
            else plan_canary(**keyword_arguments)
        )
    except LocalAnalysisCanaryError as exc:
        print(
            json.dumps(
                {"ok": False, "status": "blocked", "error": str(exc)},
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
