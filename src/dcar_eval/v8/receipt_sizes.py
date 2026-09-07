"""Bounded JSON sizes; only the explicit schema20 raw migration may be larger."""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

DEFAULT_RECEIPT_BYTES = 8 * 1024 * 1024
SCHEMA20_MIGRATION_RECEIPT_BYTES = 64 * 1024 * 1024


def receipt_read_limit(*, allow_schema20_migration: bool = False) -> int:
    return SCHEMA20_MIGRATION_RECEIPT_BYTES if allow_schema20_migration else DEFAULT_RECEIPT_BYTES


def validate_receipt_size(value: Any, byte_size: int, *, allow_schema20_migration: bool = False) -> None:
    if byte_size < 0 or byte_size > receipt_read_limit(allow_schema20_migration=allow_schema20_migration):
        raise ValueError("JSON release receipt exceeds the contract size")
    if byte_size <= DEFAULT_RECEIPT_BYTES:
        return
    expected = {"schema_version": "dcar-v20-offline-migration-v1", "status": "candidate_ready",
        "from_version": 19, "to_version": 20, "from_migration": "dual-acquisition-profile-roster-v1",
        "to_migration": "integrated-video-capture-v25"}
    if (not isinstance(value, Mapping)
            or any(value.get(key) != item for key, item in expected.items())
            or any(type(value.get(key)) is not int for key in ("from_version", "to_version"))
            or not isinstance(value.get("legacy_raw"), Mapping)
            or value["legacy_raw"].get("contract_version") != "v25-legacy-raw-migration-v1"):
        raise ValueError("only the exact schema20 migration receipt may exceed 8 MiB")
