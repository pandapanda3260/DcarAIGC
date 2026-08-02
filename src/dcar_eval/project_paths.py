"""Canonical local paths shared by the batch pipeline and the Web app."""

from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = PROJECT_ROOT / "config"
CONFIG_ARCHIVE_DIR = CONFIG_DIR / "archive"

DATA_DIR = PROJECT_ROOT / "data"
INPUT_DIR = DATA_DIR / "inputs"
DOUYIN_INPUT_DIR = INPUT_DIR / "douyin"
XHS_INPUT_DIR = INPUT_DIR / "xiaohongshu"

CACHE_DIR = DATA_DIR / "cache"
DOUYIN_PUBLIC_CACHE_DIR = CACHE_DIR / "douyin_public"
DOUYIN_MEDIA_CACHE_DIR = CACHE_DIR / "douyin_media"
TIKHUB_CACHE_DIR = CACHE_DIR / "tikhub" / "2026-08-02"
RNOTE_CACHE_DIR = CACHE_DIR / "rnote"
RAW_RESPONSE_CACHE_DIR = CACHE_DIR / "raw_responses"

PROCESSED_DIR = DATA_DIR / "processed"
CURRENT_PROCESSED_DIR = PROCESSED_DIR / "current"
DOUYIN_PROCESSED_DIR = CURRENT_PROCESSED_DIR / "douyin"
XHS_PROCESSED_DIR = CURRENT_PROCESSED_DIR / "xiaohongshu"
ARCHIVE_PROCESSED_DIR = PROCESSED_DIR / "archive"

REPORTS_DIR = PROJECT_ROOT / "reports"
CURRENT_REPORTS_DIR = REPORTS_DIR / "current"
ARCHIVE_REPORTS_DIR = REPORTS_DIR / "archive"

RUNTIME_DIR = PROJECT_ROOT / "runtime"
DEPENDENCY_DIR = RUNTIME_DIR / "dependencies"
DOUYIN_DEPENDENCY_DIR = DEPENDENCY_DIR / "douyin"
VIDEO_DEPENDENCY_DIR = DEPENDENCY_DIR / "video"


def ensure_runtime_directories() -> None:
    """Create writable output directories without touching collected evidence."""

    for directory in (
        DOUYIN_PROCESSED_DIR,
        XHS_PROCESSED_DIR,
        CURRENT_REPORTS_DIR,
        DOUYIN_PUBLIC_CACHE_DIR,
        DOUYIN_MEDIA_CACHE_DIR,
        TIKHUB_CACHE_DIR,
        RNOTE_CACHE_DIR,
        RAW_RESPONSE_CACHE_DIR,
    ):
        directory.mkdir(parents=True, exist_ok=True)
