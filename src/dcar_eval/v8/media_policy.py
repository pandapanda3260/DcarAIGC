"""One frozen media-retention rule file shared by every lifecycle consumer."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

POLICY_PATH = Path(__file__).resolve().parents[3] / "config" / "media_retention_v1.json"


def load_media_policy(path: Path = POLICY_PATH) -> dict[str, Any]:
    body = path.read_bytes()
    policy = json.loads(body)
    expected = {
        "contract_version": "media-retention-v1", "archive_retention_hours": 72,
        "restored_hot_hours": 24, "completion_gate_aged_days": 14,
        "minimum_free_bytes_per_volume": 10 * 1024**3,
        "archive_minute": 40, "retention_minute": 30, "restore_interval_minutes": 5,
        "lifecycle_concurrency": 1, "private_directory_mode": "0700", "private_file_mode": "0600",
        "trash_phase": False, "historical_enrollment": False, "automatic_paid_reacquire": False,
        "managed_directory": "managed-v1",
        "activation_modes": ["enrollment_only", "active", "paused"],
        "archive_root": "/Users/mark/Documents/DcarAIGC_MediaArchive",
        "preview": {"format": "JPEG", "maximum_edge": 256, "upscale": False, "quality": 70, "maximum_bytes": 100 * 1024},
    }
    if not isinstance(policy, dict) or policy != expected:
        raise ValueError("media-retention-v1 rule contract changed; a versioned upgrade is required")
    return {**policy, "sha256": hashlib.sha256(body).hexdigest()}


POLICY = load_media_policy()
RETENTION_HOURS = int(POLICY["archive_retention_hours"])
HOT_HOURS = int(POLICY["restored_hot_hours"])
AGED_DAYS = int(POLICY["completion_gate_aged_days"])
MIN_FREE_BYTES = int(POLICY["minimum_free_bytes_per_volume"])
