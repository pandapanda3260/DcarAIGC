"""One-off, bounded 2026-08-17..27 TikHub campaign; never manages services.

Run only after the owning task has installed the patch, backed up schema18,
stopped the writer/publisher and handed over the maintenance window. Each
invocation performs ONE phase. ``status`` is read-only; mutations need --apply.
The campaign's $50 ceiling, first successful metric observation and content
cohort survive day changes. This is not an alternative scheduler or migration.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import io
import json
import math
import os
import re
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime
from functools import partial
from importlib.metadata import version
from pathlib import Path
from threading import BoundedSemaphore, Event
from typing import Any, Iterable, Iterator, Mapping, Sequence

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src" / "dcar_eval"))

from v8 import account_roster, capture, duplicates, evaluation, media, provider_budget, providers, range_backfill as rb  # noqa: E402
from v8 import media_policy  # noqa: E402
from v8 import raw_evidence  # noqa: E402
from v8 import storage  # noqa: E402

PRODUCTION_ROOT = Path("/Users/mark/Projects/DcarAIGC")
FORMAL_DB = PRODUCTION_ROOT / "app/data/dcar_insight.sqlite3"
FREEZE_LOCK = PRODUCTION_ROOT / "runtime/operator-freeze.lock"
WRITER_LOCK = Path("/Users/mark/Library/Application Support/DcarAIGC/runtime/writer-worker.lock")
TASK_ID = "tikhub-backfill-20260817-20260827-v1"
START = datetime.fromisoformat("2026-08-17T00:00:00+08:00")
END = datetime.fromisoformat("2026-08-27T23:59:59+08:00")
PREVIOUS_MAX_AMOUNT = 30.0
MAX_AMOUNT = 50.0
MAX_PAGES = 100
PLATFORMS = ["douyin", "xiaohongshu"]
RELEASE_ID = "evaluation-v9__selling-points-v5.2"
CHECKPOINT_SIZE = 100
CYCLE_LIMIT = 200
CAPTURE_WORKERS = 10
FRESH_DOWNLOAD_WORKERS = 16
LOCAL_MEDIA_WORKERS = 8
LOCAL_FINALIZE_BATCH_SIZE = 32
LOCAL_LIMIT = 500
LEGACY_SCHEMA = 17
TARGET_SCHEMA = 18
LEGACY_STATE_SHA256 = "620d79fb2aa0926b88a14dbe5263e578b36647f4624cdf7acbade96b087d2c83"
MEDIA_SOURCE_LINK_REPAIR_MESSAGE = (
    "CampaignRepair: verified media_source hardlink removed; "
    "non-network attempts refunded"
)
DERIVED_LINK_REPAIR_MESSAGE = (
    "CampaignRepair: verified derived-evidence hardlink removed; "
    "non-compute attempt refunded"
)
INVALID_VIDEO_REPAIR_MESSAGE = (
    "CampaignRepair: verified undecodable video quarantined; "
    "download attempt refunded"
)
TERMINAL_DOWNLOAD_REPAIR_MESSAGE = (
    "CampaignRepair: exact current-source terminal download failure reset"
)
TERMINAL_DOWNLOAD_REPAIR_ERRORS = {
    (
        "image",
        "MediaProcessingError: image download incomplete: "
        "logical image group 0 exhausted",
    ),
    (
        "video",
        "MediaProcessingError: media download failed: "
        "candidate 0 was not a playable video | "
        "candidate 1 was not a playable video",
    ),
}


class CampaignError(RuntimeError):
    pass


class _LocalVideoResponse(io.BufferedReader):
    """Expose a locally synthesized MP4 through the downloader's HTTP shape."""

    def __init__(self, path: Path, url: str):
        super().__init__(path.open("rb"))
        self._url = url
        self.headers = {
            "Content-Length": str(path.stat().st_size),
            "Content-Type": "video/mp4",
        }

    def geturl(self) -> str:
        return self._url


def deny_network(*_args: Any, **_kwargs: Any) -> Any:
    raise CampaignError("cached/local-only phase must not download media")


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


class Campaign:
    def __init__(
        self,
        db_path: Path,
        run_root: Path,
        as_of: datetime,
        *,
        task_id: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        max_amount: float | None = None,
    ):
        rb._utc(as_of)
        self.task_id = TASK_ID if task_id is None else task_id
        self.start = START if start is None else start
        self.end = END if end is None else end
        self.max_amount = MAX_AMOUNT if max_amount is None else max_amount
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", self.task_id) is None:
            raise CampaignError("task_id must be a path-safe campaign identifier")
        if rb._utc(self.start) >= rb._utc(self.end):
            raise CampaignError("campaign start must be before end")
        if not math.isfinite(self.max_amount) or self.max_amount <= 0:
            raise CampaignError("campaign max_amount must be a finite positive value")
        self.db_path = db_path.expanduser().resolve(strict=True)
        self.run_root = run_root.expanduser().resolve()
        self.as_of = as_of
        self.state: dict[str, Any] = {}
        self._locked = False
        self._deadline: float | None = None
        self._stop_paid = Event()

    @property
    def state_path(self) -> Path:
        return self.run_root / "campaign.json"

    @property
    def day(self) -> str:
        return self.as_of.astimezone(rb.SHANGHAI).date().isoformat()

    @property
    def day_root(self) -> Path:
        return self.run_root / self.day

    def _is_formal(self) -> bool:
        return storage.is_formal_database_path(
            self.db_path,
            formal_database=FORMAL_DB,
        )

    @contextmanager
    def _read(self) -> Iterator[sqlite3.Connection]:
        if os.environ.get("DCAR_TEST_DENY_FORMAL_DB") == "1" and self._is_formal():
            raise CampaignError("test process attempted to open the formal DCar database")
        connection = sqlite3.connect(self.db_path.as_uri() + "?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA recursive_triggers=ON")
            connection.execute("PRAGMA query_only=ON")
            connection.execute("BEGIN")
            yield connection
        finally:
            connection.rollback()
            connection.close()

    def _contract(self) -> dict[str, Any]:
        with self._read() as connection:
            compatibility = storage.schema_compatibility_state(
                connection, supported_versions=frozenset({TARGET_SCHEMA})
            )
            schema = compatibility["user_version"]
            if not compatibility["compatible"]:
                raise CampaignError(
                    f"campaign requires consistent schema{TARGET_SCHEMA}: "
                    f"{compatibility}; no automatic migration"
                )
            releases = connection.execute("SELECT * FROM evaluation_releases WHERE status='active'").fetchall()
            if len(releases) != 1 or releases[0]["id"] != RELEASE_ID:
                raise CampaignError("campaign requires the single active v9/v5.2 release")
            runtime = evaluation._load_release_runtime(connection, releases[0])
            if runtime.matcher is None:
                raise CampaignError("active release matcher is unavailable")
            roster = account_roster.current_snapshot(connection)
            if roster is None:
                raise CampaignError("campaign requires an accepted complete Matrix roster")
            identities = [list(row) for row in connection.execute("""
                SELECT i.id,i.account_id,i.platform,i.uid FROM account_platform_identities i
                JOIN accounts a ON a.id=i.account_id
                JOIN account_roster_members m ON m.account_identity_id=i.id
                WHERE m.snapshot_id=? AND a.enabled=1
                  AND i.platform IN ('douyin','xiaohongshu')
                ORDER BY i.id,i.account_id,i.platform,i.uid
            """, (roster["id"],))]
            if not identities or any(not str(row[3]).strip() for row in identities):
                raise CampaignError("current enabled roster identities are empty or contain an empty uid")
            excluded = connection.execute("""
                SELECT a.id FROM accounts a WHERE a.enabled=1 AND NOT EXISTS (
                  SELECT 1 FROM account_platform_identities i
                  JOIN account_roster_members m ON m.account_identity_id=i.id
                  WHERE i.account_id=a.id AND m.snapshot_id=?
                    AND i.platform IN ('douyin','xiaohongshu')) ORDER BY a.id
            """, (roster["id"],)).fetchall()
            release = {key: releases[0][key] for key in
                       ("id", "rule_version", "taxonomy_version", "matcher_rule_sha256")}
        stat = self.db_path.stat()
        return {
            "task_id": self.task_id,
            "start": rb._iso(self.start),
            "end": rb._iso(self.end),
            "archive_before": rb._iso(self.start),
            "max_amount": self.max_amount,
            "max_pages": MAX_PAGES, "platforms": PLATFORMS,
            "database": str(self.db_path), "device": stat.st_dev, "inode": stat.st_ino,
            "schema": schema, "release": release,
            "roster": {
                "snapshot_id": int(roster["id"]),
                "members_sha256": str(roster["members_sha256"]),
                "source_sha256": str(roster["source_sha256"]),
                "accepted_at": str(roster["accepted_at"]),
            },
            "identities": identities, "identity_sha256": _digest(identities),
            "excluded_account_ids": [int(row["id"]) for row in excluded],
        }

    def _save(self) -> None:
        self._require_window()
        self.state["updated_at"] = storage.now_utc()
        target = self.state_path.with_suffix(".json.tmp")
        target.write_text(json.dumps(self.state, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
        target.chmod(0o600)
        target.replace(self.state_path)

    def raise_task_budget(self) -> dict[str, Any]:
        """Apply the owner-approved one-way $30 -> $50 campaign ceiling."""

        self._require_window()
        if (
            self.task_id != TASK_ID
            or self.start != START
            or self.end != END
            or self.max_amount != MAX_AMOUNT
            or MAX_AMOUNT != 50.0
            or PREVIOUS_MAX_AMOUNT != 30.0
        ):
            raise CampaignError("unexpected campaign budget migration constants")
        if not self.state_path.is_file() or self.state_path.is_symlink():
            raise CampaignError("campaign state is unavailable for budget migration")
        self.state = json.loads(self.state_path.read_text())
        saved_days = self.state.get("days")
        saved_as_of = (
            saved_days.get(self.day) if isinstance(saved_days, dict) else None
        )
        if not isinstance(saved_as_of, str):
            raise CampaignError("campaign day has no fixed as_of")
        self.as_of = rb._parse_datetime(saved_as_of)
        target_contract = self._contract()
        recorded_contract = self.state.get("contract")
        if not isinstance(recorded_contract, dict):
            raise CampaignError("campaign contract is missing")
        recorded_fixed = {
            key: value for key, value in recorded_contract.items()
            if key != "max_amount"
        }
        target_fixed = {
            key: value for key, value in target_contract.items()
            if key != "max_amount"
        }
        recorded_amount = float(recorded_contract.get("max_amount", -1))
        if (
            recorded_fixed != target_fixed
            or recorded_amount not in {PREVIOUS_MAX_AMOUNT, self.max_amount}
        ):
            raise CampaignError(
                "campaign contract changed outside the approved budget ceiling"
            )

        range_state_path = self.day_root / f"{self.task_id}.json"
        if not range_state_path.is_file() or range_state_path.is_symlink():
            raise CampaignError("range campaign state is unavailable")
        range_state = json.loads(range_state_path.read_text())
        expected_range = rb._campaign_contract(
            task_id=self.task_id,
            start=self.start,
            end=self.end,
            as_of=self.as_of,
            max_amount=self.max_amount,
            max_pages=MAX_PAGES,
            platforms=PLATFORMS,
        )
        recorded_range = range_state.get("contract")
        if not isinstance(recorded_range, dict):
            raise CampaignError("range campaign contract is missing")
        for key, expected in expected_range.items():
            if key == "max_amount":
                if float(recorded_range.get(key, -1)) not in {
                    PREVIOUS_MAX_AMOUNT,
                    self.max_amount,
                }:
                    raise CampaignError("range campaign budget is not migratable")
            elif recorded_range.get(key) != expected:
                raise CampaignError("range campaign fixed contract changed")
        phase_contracts = range_state.get("phase_contracts")
        if not isinstance(phase_contracts, dict):
            raise CampaignError("range phase contracts are missing")
        for phase, phase_contract in phase_contracts.items():
            if not isinstance(phase_contract, dict):
                raise CampaignError("range phase contract is malformed")
            expected_phase = {**expected_range, "phase": phase}
            for key, expected in expected_phase.items():
                if key == "max_amount":
                    if float(phase_contract.get(key, -1)) not in {
                        PREVIOUS_MAX_AMOUNT,
                        self.max_amount,
                    }:
                        raise CampaignError(
                            "range phase budget is not migratable"
                        )
                elif phase_contract.get(key) != expected:
                    raise CampaignError("range phase fixed contract changed")

        task_digest = hashlib.sha256(self.task_id.encode()).hexdigest()[:16]
        prefix = f"task-{task_digest}-tikhub-"
        migrated_at = storage.now_utc()
        updated_budget_ids: list[str] = []
        with storage.connect(self.db_path) as connection, storage.transaction(
            connection
        ):
            rows = connection.execute(
                "SELECT * FROM provider_budget_batches WHERE id LIKE ? ORDER BY id",
                (f"{prefix}%",),
            ).fetchall()
            if not rows:
                raise CampaignError("campaign task budget batches are missing")
            usage_amount = float(connection.execute(
                "SELECT COALESCE(SUM(amount),0) FROM provider_usage WHERE task_id=?",
                (self.task_id,),
            ).fetchone()[0])
            if usage_amount > self.max_amount + 1e-9:
                raise CampaignError("campaign usage already exceeds the new ceiling")
            for row in rows:
                old_amount = float(row["max_amount"])
                if old_amount not in {PREVIOUS_MAX_AMOUNT, self.max_amount}:
                    raise CampaignError("task budget batch has an unexpected ceiling")
                max_requests = max(
                    1,
                    math.floor(
                        (self.max_amount + 1e-9)
                        / float(row["verified_unit_price"])
                    ),
                )
                if (
                    int(row["consumed_requests"]) > max_requests
                    or float(row["consumed_amount"])
                    > self.max_amount + 1e-9
                ):
                    raise CampaignError("task budget consumption exceeds new limits")
                if old_amount == self.max_amount:
                    if (
                        int(row["max_billable_requests"]) != max_requests
                        or int(row["daily_quota"]) != max_requests
                        or row["status"] not in {"approved", "pilot"}
                    ):
                        raise CampaignError("migrated task budget batch drifted")
                    continue
                if row["status"] not in {"approved", "pilot", "exhausted"}:
                    raise CampaignError("task budget batch is not eligible for increase")
                next_status = (
                    "approved" if row["status"] == "exhausted" else row["status"]
                )
                cursor = connection.execute(
                    """
                    UPDATE provider_budget_batches
                    SET max_billable_requests=?,max_amount=?,daily_quota=?,
                      status=?,updated_at=?
                    WHERE id=? AND max_amount=? AND consumed_requests=?
                      AND consumed_amount=? AND status=?
                    """,
                    (
                        max_requests,
                        self.max_amount,
                        max_requests,
                        next_status,
                        migrated_at,
                        row["id"],
                        PREVIOUS_MAX_AMOUNT,
                        row["consumed_requests"],
                        row["consumed_amount"],
                        row["status"],
                    ),
                )
                if cursor.rowcount != 1:
                    raise CampaignError("task budget batch changed during migration")
                updated_budget_ids.append(str(row["id"]))

        range_state["contract"] = expected_range
        range_state["max_amount"] = self.max_amount
        range_state["phase_contracts"] = {
            phase: {**contract, "max_amount": self.max_amount}
            for phase, contract in phase_contracts.items()
        }
        range_state["updated_at"] = migrated_at
        range_temporary = range_state_path.with_name(
            f".{range_state_path.name}.tmp"
        )
        range_temporary.write_text(
            json.dumps(range_state, ensure_ascii=False, indent=2, sort_keys=True)
            + "\n"
        )
        range_temporary.chmod(0o600)
        range_temporary.replace(range_state_path)

        receipt = {
            "from_amount": PREVIOUS_MAX_AMOUNT,
            "to_amount": self.max_amount,
            "task_usage_amount": round(usage_amount, 6),
            "budget_batch_ids": [str(row["id"]) for row in rows],
            "updated_budget_batch_ids": updated_budget_ids,
            "completed_at": migrated_at,
        }
        migrations = self.state.setdefault("budget_migrations", [])
        already_recorded = any(
            item.get("from_amount") == PREVIOUS_MAX_AMOUNT
            and item.get("to_amount") == self.max_amount
            for item in migrations
        )
        if not already_recorded:
            migrations.append(receipt)
        self.state["contract"] = target_contract
        self._save()
        return {
            **receipt,
            "status": (
                "already_succeeded"
                if already_recorded and not updated_budget_ids
                else "succeeded"
            ),
        }

    def _require_window(self) -> None:
        if not self._locked:
            raise CampaignError("mutation requires Campaign.window() and the exclusive writer lock")

    def _prepare(self) -> None:
        for package, expected in (("ImageHash", "4.3.2"), ("Pillow", "12.3.0")):
            if version(package) != expected:
                raise CampaignError(f"fingerprint dependency must be {package}=={expected}")
        contract = self._contract()
        if self.state_path.exists():
            self.state = json.loads(self.state_path.read_text())
            if self.state.get("contract") != contract:
                raise CampaignError("campaign database/release/accepted-roster contract changed")
        else:
            with self._read() as connection:
                if connection.execute(
                    "SELECT 1 FROM provider_usage WHERE task_id=? LIMIT 1",
                    (self.task_id,),
                ).fetchone():
                    raise CampaignError("task already has usage; restore its original campaign state, do not rebase")
                baseline = [int(row[0]) for row in connection.execute("""
                    SELECT id FROM content_items WHERE platform IN ('douyin','xiaohongshu')
                    AND published_at>=? AND published_at<=? ORDER BY id
                """, (rb._iso(self.start), rb._iso(self.end)))]
                eligible = [int(row[0]) for row in connection.execute("""
                    SELECT DISTINCT c.id FROM content_items c
                    JOIN accounts a ON a.id=c.account_id
                    JOIN account_platform_identities i
                      ON i.account_id=c.account_id AND i.platform=c.platform
                    JOIN account_roster_members m ON m.account_identity_id=i.id
                    WHERE m.snapshot_id=? AND a.enabled=1
                    AND c.platform IN ('douyin','xiaohongshu')
                    AND c.published_at>=? AND c.published_at<=? ORDER BY c.id
                """, (
                    contract["roster"]["snapshot_id"],
                    rb._iso(self.start),
                    rb._iso(self.end),
                ))]
                terminal = rb.media_terminal_state_details(connection, RELEASE_ID, eligible)
            self.state = {
                "contract": contract, "baseline_ids": baseline,
                "existing_pending_ids": [cid for cid in eligible if terminal[cid].state not in {"complete", "terminal_insufficient"}],
                "new_ids": [], "visited_ids": [], "relation_pending_ids": [],
                "discovery_complete": False, "days": {}, "results": {},
                "refresh_intents": {}, "created_at": storage.now_utc(),
            }
        # One immutable as_of per day, with separate range state roots on day 2+.
        saved_as_of = self.state["days"].setdefault(self.day, rb._iso(self.as_of))
        self.as_of = rb._parse_datetime(saved_as_of)
        self._save()
        self._merge_manifests()

    def _eligible_content_ids(self, contract: Mapping[str, Any]) -> set[int]:
        with self._read() as connection:
            return {int(row[0]) for row in connection.execute("""
                SELECT DISTINCT c.id FROM content_items c
                JOIN accounts a ON a.id=c.account_id
                JOIN account_platform_identities i
                  ON i.account_id=c.account_id AND i.platform=c.platform
                JOIN account_roster_members m ON m.account_identity_id=i.id
                WHERE m.snapshot_id=? AND a.enabled=1 AND TRIM(i.uid)<>''
                  AND c.platform IN ('douyin','xiaohongshu')
                  AND c.published_at>=? AND c.published_at<=?
                ORDER BY c.id
            """, (
                contract["roster"]["snapshot_id"],
                rb._iso(self.start),
                rb._iso(self.end),
            ))}

    @staticmethod
    def _state_content_ids(state: Mapping[str, Any]) -> set[int]:
        ids: set[int] = set()
        for key in (
            "baseline_ids", "existing_pending_ids", "new_ids", "visited_ids",
            "relation_pending_ids",
        ):
            values = state.get(key, [])
            if not isinstance(values, list):
                raise CampaignError(f"legacy campaign {key} must be a list")
            ids.update(int(value) for value in values)
        for key in ("results", "refresh_intents"):
            value = state.get(key, {})
            if not isinstance(value, dict):
                raise CampaignError(f"legacy campaign {key} must be an object")
            mappings = value.values() if key == "results" else (value,)
            for mapping in mappings:
                if not isinstance(mapping, dict):
                    raise CampaignError(f"legacy campaign {key} entries must be objects")
                ids.update(int(content_id) for content_id in mapping)
        return ids

    @staticmethod
    def _filtered_results(value: Mapping[str, Any], eligible: set[int]) -> dict[str, Any]:
        filtered: dict[str, Any] = {}
        for phase, results in value.items():
            if not isinstance(results, dict):
                raise CampaignError("legacy campaign result entries must be objects")
            filtered[str(phase)] = {
                str(content_id): result for content_id, result in results.items()
                if int(content_id) in eligible
            }
        return filtered

    def migrate_legacy_state(self, legacy_run_root: Path) -> dict[str, Any]:
        """Adopt the one verified schema17 campaign state into a fresh schema18 root."""
        self._require_window()
        if (
            self.task_id != TASK_ID
            or self.start != START
            or self.end != END
            or self.max_amount != MAX_AMOUNT
        ):
            raise CampaignError(
                "legacy state migration is restricted to the original campaign"
            )
        legacy_root = legacy_run_root.expanduser().resolve(strict=True)
        if legacy_root == self.run_root:
            raise CampaignError("legacy and schema18 campaign roots must be different")
        source = legacy_root / "campaign.json"
        if not source.is_file() or source.is_symlink():
            raise CampaignError("legacy campaign state must be a regular file")
        body = source.read_bytes()
        source_sha256 = hashlib.sha256(body).hexdigest()
        if source_sha256 != LEGACY_STATE_SHA256:
            raise CampaignError("legacy campaign state sha256 is not the approved partial receipt")

        contract = self._contract()
        if int(contract["schema"]) != TARGET_SCHEMA:
            raise CampaignError("schema18 campaign contract is unavailable")
        if self.state_path.exists():
            existing = json.loads(self.state_path.read_text())
            migrations = existing.get("contract_migrations", [])
            if (
                existing.get("contract") == contract
                and any(item.get("source_sha256") == source_sha256 for item in migrations)
            ):
                self.state = existing
                return {
                    "status": "already_succeeded",
                    "source_sha256": source_sha256,
                    "new_contents": len(existing.get("new_ids", [])),
                }
            raise CampaignError("schema18 campaign root already contains another state")

        legacy = json.loads(body)
        legacy_contract = legacy.get("contract")
        if not isinstance(legacy_contract, dict):
            raise CampaignError("legacy campaign contract is missing")
        fixed_keys = (
            "task_id", "start", "end", "archive_before", "max_amount",
            "max_pages", "platforms", "database", "release",
        )
        if (
            int(legacy_contract.get("schema", -1)) != LEGACY_SCHEMA
            or legacy_contract.get("roster") not in (None, {})
            or any(legacy_contract.get(key) != contract.get(key) for key in fixed_keys)
        ):
            raise CampaignError("legacy campaign fixed contract does not match schema18 campaign")
        legacy_identities = legacy_contract.get("identities")
        if not isinstance(legacy_identities, list) or any(
            not isinstance(row, list) or len(row) != 3 or not str(row[2]).strip()
            for row in legacy_identities
        ):
            raise CampaignError("legacy identity contract is malformed")
        legacy_identity_keys = {(int(row[0]), str(row[1]), str(row[2])) for row in legacy_identities}
        current_identity_keys = {
            (int(row[1]), str(row[2]), str(row[3])) for row in contract["identities"]
        }
        if not current_identity_keys or not (current_identity_keys & legacy_identity_keys):
            raise CampaignError("accepted schema18 identities do not overlap the verified legacy scope")

        all_ids = self._state_content_ids(legacy)
        with self._read() as connection:
            if not connection.execute(
                "SELECT 1 FROM provider_usage WHERE task_id=? LIMIT 1",
                (self.task_id,),
            ).fetchone():
                raise CampaignError("legacy campaign has no matching provider usage ledger")
            if all_ids:
                placeholders = ",".join("?" for _ in all_ids)
                rows = connection.execute(
                    f"SELECT id,published_at FROM content_items WHERE id IN ({placeholders})",  # noqa: S608
                    tuple(sorted(all_ids)),
                ).fetchall()
                if len(rows) != len(all_ids) or any(
                    not (
                        rb._iso(self.start)
                        <= str(row["published_at"])
                        <= rb._iso(self.end)
                    )
                    for row in rows
                ):
                    raise CampaignError("legacy campaign content is missing or outside the fixed range")

        eligible = self._eligible_content_ids(contract)
        baseline = [int(cid) for cid in legacy.get("baseline_ids", [])]
        accepted_baseline = sorted(set(baseline) & eligible)
        with self._read() as connection:
            terminal = rb.media_terminal_state_details(connection, RELEASE_ID, accepted_baseline)
        pending = [
            cid for cid in accepted_baseline
            if terminal[cid].state not in {"complete", "terminal_insufficient"}
        ]
        old_new = {int(cid) for cid in legacy.get("new_ids", [])}
        old_visited = {int(cid) for cid in legacy.get("visited_ids", [])}
        new_ids = sorted(old_new & eligible)
        visited_ids = sorted(old_visited & eligible)
        old_refresh = legacy.get("refresh_intents", {})
        migrated_at = storage.now_utc()
        self.state = {
            **legacy,
            "contract": contract,
            "baseline_ids": baseline,
            "existing_pending_ids": pending,
            "new_ids": new_ids,
            "visited_ids": visited_ids,
            "relation_pending_ids": sorted(
                {int(cid) for cid in legacy.get("relation_pending_ids", [])} & eligible
            ),
            "results": self._filtered_results(legacy.get("results", {}), eligible),
            "refresh_intents": {
                str(cid): value for cid, value in old_refresh.items() if int(cid) in eligible
            },
            "manifest_sha256": {},
            "contract_migrations": [
                *legacy.get("contract_migrations", []),
                {
                    "from_schema": LEGACY_SCHEMA,
                    "to_schema": int(contract["schema"]),
                    "source_root": str(legacy_root),
                    "source_sha256": source_sha256,
                    "legacy_contract_sha256": _digest(legacy_contract),
                    "schema18_contract_sha256": _digest(contract),
                    "legacy_new_contents": len(old_new),
                    "accepted_new_contents": len(new_ids),
                    "excluded_new_contents": len(old_new - eligible),
                    "legacy_identity_count": len(legacy_identity_keys),
                    "accepted_identity_count": len(current_identity_keys),
                    "retained_identity_count": len(legacy_identity_keys & current_identity_keys),
                    "added_identity_count": len(current_identity_keys - legacy_identity_keys),
                    "removed_identity_count": len(legacy_identity_keys - current_identity_keys),
                    "migrated_at": migrated_at,
                },
            ],
        }
        self._save()
        return {
            "status": "succeeded",
            "source_sha256": source_sha256,
            "baseline_contents": len(baseline),
            "new_contents": len(new_ids),
            "excluded_new_contents": len(old_new - eligible),
            "existing_pending_contents": len(pending),
            "visited_contents": len(visited_ids),
        }

    def repair_discovery_raw_links(self) -> dict[str, Any]:
        """Break only campaign discovery hardlinks created by the schema18 rollback copy."""
        self._require_window()
        contract = self._contract()
        sample_window = rb.discovery_window_key(
            start=self.start,
            end=self.end,
            platform="douyin",
            cursor=None,
        )
        prefix = sample_window.rsplit(":", 2)[0] + ":%"
        with self._read() as connection:
            rows = connection.execute("""
                SELECT DISTINCT pr.id,pr.local_path,pr.sha256,pr.byte_size
                FROM fetch_slots fs
                JOIN fetch_attempts fa ON fa.slot_id=fs.id
                JOIN provider_raw_responses pr ON pr.fetch_attempt_id=fa.id
                JOIN account_platform_identities i ON i.account_id=fs.account_id
                JOIN account_roster_members m ON m.account_identity_id=i.id
                JOIN accounts a ON a.id=i.account_id
                WHERE m.snapshot_id=? AND a.enabled=1 AND TRIM(i.uid)<>''
                  AND fs.stage='discovery' AND fs.status='succeeded'
                  AND fs.provider='TikHub' AND fs.window_key LIKE ?
                  AND ((i.platform='douyin' AND fs.window_key LIKE '%:douyin:%')
                    OR (i.platform='xiaohongshu' AND fs.window_key LIKE '%:xiaohongshu:%'))
                  AND pr.id=(
                    SELECT pr2.id FROM fetch_attempts fa2
                    JOIN provider_raw_responses pr2 ON pr2.fetch_attempt_id=fa2.id
                    WHERE fa2.slot_id=fs.id
                    ORDER BY fa2.attempt_number DESC,pr2.id DESC LIMIT 1
                  )
                ORDER BY pr.id
            """, (contract["roster"]["snapshot_id"], prefix)).fetchall()
        raw_root = capture.RAW_ROOT.resolve(strict=True)
        repaired = 0
        already_single = 0
        repaired_bytes = 0
        for row in rows:
            local_path = Path(str(row["local_path"]))
            candidate = local_path if local_path.is_absolute() else storage.PROJECT_ROOT / local_path
            resolved = candidate.parent.resolve(strict=True) / candidate.name
            if not resolved.is_relative_to(raw_root):
                raise CampaignError("campaign discovery raw is outside the canonical raw root")
            descriptor = os.open(resolved, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(descriptor, "rb") as stream:
                before = os.fstat(stream.fileno())
                if not stat.S_ISREG(before.st_mode):
                    raise CampaignError("campaign discovery raw is not a regular file")
                if before.st_nlink == 1:
                    already_single += 1
                    continue
                body = stream.read()
            if (
                len(body) != int(row["byte_size"])
                or not hashlib.sha256(body).hexdigest() == str(row["sha256"])
            ):
                raise CampaignError("campaign discovery hardlink failed size/SHA-256 validation")
            current = os.stat(resolved, follow_symlinks=False)
            identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_nlink)
            if identity != (
                current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns,
                current.st_nlink,
            ):
                raise CampaignError("campaign discovery hardlink changed before replacement")
            temporary = resolved.with_name(f".{resolved.name}.dealias-{os.getpid()}")
            temporary_descriptor = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                0o600,
            )
            replaced = False
            try:
                with os.fdopen(temporary_descriptor, "wb") as stream:
                    stream.write(body)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, resolved)
                replaced = True
                parent_descriptor = os.open(resolved.parent, os.O_RDONLY | os.O_CLOEXEC)
                try:
                    os.fsync(parent_descriptor)
                finally:
                    os.close(parent_descriptor)
            finally:
                if not replaced:
                    temporary.unlink(missing_ok=True)
            verified, _ = capture._read_verified_raw_response(row)
            if verified != resolved:
                raise CampaignError("campaign discovery raw replacement path changed")
            repaired += 1
            repaired_bytes += len(body)
        receipt = {
            "status": "succeeded",
            "checked": len(rows),
            "repaired": repaired,
            "already_single": already_single,
            "repaired_bytes": repaired_bytes,
            "completed_at": storage.now_utc(),
        }
        self.state["discovery_raw_link_repair"] = receipt
        self._save()
        return receipt

    def repair_media_source_links(self) -> dict[str, Any]:
        """Repair only campaign media-source hardlinks that exhausted local downloads."""
        self._require_window()
        existing = self.state.get("media_source_link_repair")
        cohort = sorted(set(self.state["new_ids"]) | set(self.state["existing_pending_ids"]))
        cohort_json = json.dumps(cohort, separators=(",", ":"))
        with self._read() as connection:
            rows = connection.execute(
                """
                WITH cohort(content_id) AS (
                  SELECT CAST(value AS INTEGER) FROM json_each(?)
                ), active_source AS (
                  SELECT e.* FROM evidence_artifacts e
                  JOIN cohort c ON c.content_id=e.content_id
                  WHERE e.artifact_type='media_source' AND e.status='available'
                    AND e.id=(
                      SELECT e2.id FROM evidence_artifacts e2
                      WHERE e2.content_id=e.content_id
                        AND e2.artifact_type='media_source'
                        AND e2.status='available'
                      ORDER BY e2.id DESC LIMIT 1
                    )
                )
                SELECT e.id artifact_id,e.content_id,e.local_path,e.byte_size,e.sha256,
                  e.processor_version source_processor_version,c.content_type,
                  s.id slot_id,s.source_sha256,s.processor_version slot_processor_version,
                  s.status,s.attempt_count,s.output_artifact_id,s.error_message,s.updated_at
                FROM active_source e
                JOIN content_items c ON c.id=e.content_id
                JOIN media_processing_slots s
                  ON s.content_id=e.content_id AND s.source_sha256=e.sha256
                WHERE e.processor_version=? AND s.processor_type='download'
                  AND s.processor_version=CASE c.content_type
                    WHEN 'video' THEN ? WHEN 'image' THEN ? END
                  AND s.output_artifact_id IS NULL AND (
                    (s.status='terminal_failed' AND s.attempt_count>=?
                      AND s.error_message='MediaProcessingError: ' || c.content_type
                        || ' media source must be a private regular file')
                    OR (s.status='retryable_failed' AND s.attempt_count>0
                      AND s.error_message='MediaProcessingError: ' || c.content_type
                        || ' media source must be a private regular file')
                    OR (s.status='retryable_failed' AND s.attempt_count=0
                      AND s.error_message=?)
                  )
                ORDER BY e.content_id
                """,
                (
                    cohort_json,
                    media.MEDIA_SOURCE_VERSION,
                    media.VIDEO_DOWNLOAD_VERSION,
                    media.IMAGE_DOWNLOAD_VERSION,
                    media.MAX_MEDIA_DOWNLOAD_ATTEMPTS,
                    MEDIA_SOURCE_LINK_REPAIR_MESSAGE,
                ),
            ).fetchall()
        if not rows and isinstance(existing, dict) and existing.get("status") == "succeeded":
            return existing
        media_root = media.MEDIA_ROOT.resolve(strict=True)
        dealiased = 0
        already_private = 0
        repaired_bytes = 0
        for row in rows:
            resolved = media._resolved(str(row["local_path"]))
            media._require_no_symlink_below_root(
                resolved, root=media_root, label="campaign media source"
            )
            resolved = resolved.parent.resolve(strict=True) / resolved.name
            if not resolved.is_relative_to(media_root):
                raise CampaignError("campaign media source is outside the canonical media root")
            descriptor = os.open(
                resolved,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
            )
            with os.fdopen(descriptor, "rb") as stream:
                before = os.fstat(stream.fileno())
                if (
                    not stat.S_ISREG(before.st_mode)
                    or before.st_nlink < 1
                    or before.st_size <= 0
                    or before.st_uid != os.getuid()
                ):
                    raise CampaignError("campaign media source is not a private regular file candidate")
                body = stream.read()
            if (
                len(body) != int(row["byte_size"])
                or hashlib.sha256(body).hexdigest() != str(row["sha256"])
            ):
                raise CampaignError("campaign media source failed size/SHA-256 validation")
            if before.st_nlink == 1:
                already_private += 1
            else:
                current = os.stat(resolved, follow_symlinks=False)
                identity = (
                    before.st_dev,
                    before.st_ino,
                    before.st_size,
                    before.st_mtime_ns,
                    before.st_nlink,
                )
                if identity != (
                    current.st_dev,
                    current.st_ino,
                    current.st_size,
                    current.st_mtime_ns,
                    current.st_nlink,
                ):
                    raise CampaignError("campaign media source changed before replacement")
                temporary = resolved.with_name(
                    f".{resolved.name}.dealias-{os.getpid()}-{row['artifact_id']}"
                )
                temporary_descriptor = os.open(
                    temporary,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                    0o600,
                )
                replaced = False
                try:
                    with os.fdopen(temporary_descriptor, "wb") as stream:
                        stream.write(body)
                        stream.flush()
                        os.fsync(stream.fileno())
                    os.replace(temporary, resolved)
                    replaced = True
                    parent_descriptor = os.open(
                        resolved.parent, os.O_RDONLY | os.O_CLOEXEC
                    )
                    try:
                        os.fsync(parent_descriptor)
                    finally:
                        os.close(parent_descriptor)
                finally:
                    if not replaced:
                        temporary.unlink(missing_ok=True)
                dealiased += 1
                repaired_bytes += len(body)
            evidence = media._read_private_file_evidence(
                resolved, label="repaired campaign media source"
            )
            if (
                evidence.byte_size != int(row["byte_size"])
                or evidence.sha256 != str(row["sha256"])
            ):
                raise CampaignError("repaired campaign media source identity changed")

        repaired_at = storage.now_utc()
        slots_reset = 0
        already_reset = 0
        with storage.connect(self.db_path) as connection, storage.transaction(connection):
            for row in rows:
                if (
                    row["status"] == "retryable_failed"
                    and int(row["attempt_count"]) == 0
                    and row["error_message"] == MEDIA_SOURCE_LINK_REPAIR_MESSAGE
                ):
                    already_reset += 1
                    continue
                cursor = connection.execute(
                    """
                    UPDATE media_processing_slots
                    SET status='retryable_failed',attempt_count=0,
                      error_message=?,updated_at=?
                    WHERE id=? AND content_id=? AND source_sha256=?
                      AND processor_type='download' AND processor_version=?
                      AND status=? AND attempt_count=?
                      AND output_artifact_id IS NULL AND error_message=? AND updated_at=?
                    """,
                    (
                        MEDIA_SOURCE_LINK_REPAIR_MESSAGE,
                        repaired_at,
                        row["slot_id"],
                        row["content_id"],
                        row["source_sha256"],
                        row["slot_processor_version"],
                        row["status"],
                        row["attempt_count"],
                        row["error_message"],
                        row["updated_at"],
                    ),
                )
                if cursor.rowcount != 1:
                    raise CampaignError("campaign media download slot changed before repair")
                slots_reset += 1
        content_ids = [int(row["content_id"]) for row in rows]
        receipt = {
            "status": "succeeded",
            "checked": len(rows),
            "dealiased": dealiased,
            "already_private": already_private,
            "slots_reset": slots_reset,
            "already_reset": already_reset,
            "slots_ready": len(rows),
            "attempts_refunded": sum(int(row["attempt_count"]) for row in rows),
            "repaired_bytes": repaired_bytes,
            "content_ids": content_ids,
            "slot_ids": [int(row["slot_id"]) for row in rows],
            "completed_at": repaired_at,
        }
        download_results = self.state["results"].setdefault("download", {})
        for content_id in content_ids:
            download_results[str(content_id)] = {
                "content_id": content_id,
                "status": "repair_ready",
                "reason": "verified_media_source_hardlink_removed",
            }
        if isinstance(existing, dict) and existing.get("status") == "succeeded":
            self.state.setdefault("media_source_link_repair_history", []).append(existing)
        self.state["media_source_link_repair"] = receipt
        self._save()
        return receipt

    def repair_terminal_downloads(self) -> dict[str, Any]:
        """Reset only the two reviewed current-source terminal download failures."""

        self._require_window()
        existing = self.state.get("terminal_download_repair")
        cohort = sorted(
            set(self.state["new_ids"]) | set(self.state["existing_pending_ids"])
        )
        cohort_json = json.dumps(cohort, separators=(",", ":"))
        allowed_messages = sorted(
            message for _content_type, message in TERMINAL_DOWNLOAD_REPAIR_ERRORS
        )
        with self._read() as connection:
            rows = connection.execute(
                """
                WITH cohort(content_id) AS (
                  SELECT CAST(value AS INTEGER) FROM json_each(?)
                ), active_source AS (
                  SELECT e.* FROM evidence_artifacts e
                  JOIN cohort campaign ON campaign.content_id=e.content_id
                  WHERE e.artifact_type='media_source' AND e.status='available'
                    AND e.processor_version=?
                    AND e.id=(
                      SELECT e2.id FROM evidence_artifacts e2
                      WHERE e2.content_id=e.content_id
                        AND e2.artifact_type='media_source'
                        AND e2.status='available'
                      ORDER BY e2.id DESC LIMIT 1
                    )
                )
                SELECT c.content_type,s.id slot_id,s.content_id,s.source_sha256,
                  s.processor_version,s.status,s.attempt_count,
                  s.output_artifact_id,s.error_message,s.updated_at
                FROM active_source source
                JOIN content_items c ON c.id=source.content_id
                JOIN media_processing_slots s
                  ON s.content_id=source.content_id
                  AND s.source_sha256=source.sha256
                WHERE s.processor_type='download'
                  AND s.processor_version=CASE c.content_type
                    WHEN 'video' THEN ? WHEN 'image' THEN ? END
                  AND s.status='terminal_failed' AND s.attempt_count=?
                  AND s.output_artifact_id IS NULL
                  AND s.error_message IN (?,?)
                ORDER BY s.content_id,s.id
                """,
                (
                    cohort_json,
                    media.MEDIA_SOURCE_VERSION,
                    media.VIDEO_DOWNLOAD_VERSION,
                    media.IMAGE_DOWNLOAD_VERSION,
                    media.MAX_MEDIA_DOWNLOAD_ATTEMPTS,
                    *allowed_messages,
                ),
            ).fetchall()
            exact_rows = [
                row
                for row in rows
                if (str(row["content_type"]), str(row["error_message"]))
                in TERMINAL_DOWNLOAD_REPAIR_ERRORS
            ]
            terminal = rb.media_terminal_state_details(
                connection,
                RELEASE_ID,
                [int(row["content_id"]) for row in exact_rows],
            )
            rows = [
                row
                for row in exact_rows
                if terminal[int(row["content_id"])].state == "terminal_failed"
                and terminal[int(row["content_id"])].reason
                == "download_terminal_failed"
            ]
        if not rows and isinstance(existing, dict) and existing.get("status") == "succeeded":
            return existing

        repaired_at = storage.now_utc()
        with storage.connect(self.db_path) as connection, storage.transaction(connection):
            for row in rows:
                cursor = connection.execute(
                    """
                    UPDATE media_processing_slots
                    SET status='retryable_failed',attempt_count=0,
                      error_message=?,updated_at=?
                    WHERE id=? AND content_id=? AND source_sha256=?
                      AND processor_type='download' AND processor_version=?
                      AND status='terminal_failed' AND attempt_count=?
                      AND output_artifact_id IS NULL
                      AND error_message=? AND updated_at=?
                    """,
                    (
                        TERMINAL_DOWNLOAD_REPAIR_MESSAGE,
                        repaired_at,
                        row["slot_id"],
                        row["content_id"],
                        row["source_sha256"],
                        row["processor_version"],
                        row["attempt_count"],
                        row["error_message"],
                        row["updated_at"],
                    ),
                )
                if cursor.rowcount != 1:
                    raise CampaignError(
                        "exact terminal download slot changed before repair"
                    )

        content_ids = [int(row["content_id"]) for row in rows]
        receipt = {
            "status": "succeeded",
            "checked": len(exact_rows),
            "slots_reset": len(rows),
            "attempts_refunded": sum(int(row["attempt_count"]) for row in rows),
            "content_ids": content_ids,
            "slot_ids": [int(row["slot_id"]) for row in rows],
            "completed_at": repaired_at,
        }
        download_results = self.state["results"].setdefault("download", {})
        for content_id in content_ids:
            download_results[str(content_id)] = {
                "content_id": content_id,
                "status": "repair_ready",
                "reason": "exact_current_source_terminal_download_reset",
            }
        if isinstance(existing, dict) and existing.get("status") == "succeeded":
            self.state.setdefault("terminal_download_repair_history", []).append(
                existing
            )
        self.state["terminal_download_repair"] = receipt
        self._save()
        return receipt

    def _dealias_content_media_tree(
        self, content_id: int, link_id: str
    ) -> tuple[int, int, list[str]]:
        self._require_window()
        media_root = media.MEDIA_ROOT.resolve(strict=True)
        validated_link_id = media._validated_link_id(link_id)
        content_root = media_root / validated_link_id
        media._require_no_symlink_below_root(
            content_root, root=media_root, label="campaign content media tree"
        )
        content_root = content_root.resolve(strict=True)
        if not content_root.is_dir() or not content_root.is_relative_to(media_root):
            raise CampaignError("campaign content media tree is invalid")
        repaired = 0
        repaired_bytes = 0
        repaired_paths: list[str] = []
        for directory, names, filenames in os.walk(content_root, followlinks=False):
            directory_path = Path(directory)
            for name in names:
                if (directory_path / name).is_symlink():
                    raise CampaignError("campaign content media tree contains a symlink")
            for name in filenames:
                resolved = directory_path / name
                before_path = os.stat(resolved, follow_symlinks=False)
                if stat.S_ISLNK(before_path.st_mode):
                    raise CampaignError("campaign content media file is a symlink")
                if not stat.S_ISREG(before_path.st_mode) or before_path.st_nlink == 1:
                    continue
                descriptor = os.open(
                    resolved, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC
                )
                with os.fdopen(descriptor, "rb") as stream:
                    before = os.fstat(stream.fileno())
                    if (
                        not stat.S_ISREG(before.st_mode)
                        or before.st_nlink <= 1
                        or before.st_size <= 0
                        or before.st_uid != os.getuid()
                    ):
                        raise CampaignError(
                            "campaign linked media file identity changed"
                        )
                    body = stream.read()
                digest = hashlib.sha256(body).hexdigest()
                current = os.stat(resolved, follow_symlinks=False)
                identity = (
                    before.st_dev,
                    before.st_ino,
                    before.st_size,
                    before.st_mtime_ns,
                    before.st_nlink,
                )
                if identity != (
                    current.st_dev,
                    current.st_ino,
                    current.st_size,
                    current.st_mtime_ns,
                    current.st_nlink,
                ):
                    raise CampaignError(
                        "campaign linked media file changed before replacement"
                    )
                temporary = resolved.with_name(
                    f".{resolved.name}.dealias-{os.getpid()}-{content_id}"
                )
                temporary_descriptor = os.open(
                    temporary,
                    os.O_WRONLY
                    | os.O_CREAT
                    | os.O_EXCL
                    | os.O_NOFOLLOW
                    | os.O_CLOEXEC,
                    0o600,
                )
                replaced = False
                try:
                    with os.fdopen(temporary_descriptor, "wb") as stream:
                        stream.write(body)
                        stream.flush()
                        os.fsync(stream.fileno())
                    os.replace(temporary, resolved)
                    replaced = True
                    media._fsync_directory(resolved.parent)
                finally:
                    if not replaced:
                        temporary.unlink(missing_ok=True)
                evidence = media._read_private_file_evidence(
                    resolved, label="repaired campaign content media file"
                )
                if evidence.byte_size != len(body) or evidence.sha256 != digest:
                    raise CampaignError(
                        "repaired campaign content media identity changed"
                    )
                repaired += 1
                repaired_bytes += len(body)
                repaired_paths.append(str(resolved))
        return repaired, repaired_bytes, repaired_paths

    def repair_local_failures(self) -> dict[str, Any]:
        """Repair only exact local failures observed by this campaign."""
        self._require_window()
        existing = self.state.get("local_failure_repair")
        local_results = self.state["results"].setdefault("local", {})
        linked_ids: list[int] = []
        invalid_video_ids: list[int] = []
        for key, result in local_results.items():
            content_id = int(key)
            if (
                result.get("error_code") == "MediaProcessingError"
                and result.get("error")
                == "cached frames_manifest output must be a private regular file"
            ) or (
                result.get("status") == "repair_intent"
                and result.get("repair_kind") == "derived_evidence_hardlink"
            ):
                linked_ids.append(content_id)
            if (
                result.get("error_code") == "MediaProcessingError"
                and (
                    result.get("error") == "no frames were extracted"
                    or str(result.get("error") or "").startswith(
                        "invalid media: "
                    )
                )
            ) or (
                result.get("status") == "repair_intent"
                and result.get("repair_kind") == "invalid_video_download"
            ):
                invalid_video_ids.append(content_id)
        if not linked_ids and not invalid_video_ids:
            if isinstance(existing, dict) and existing.get("status") == "succeeded":
                return existing
            receipt = {
                "status": "succeeded",
                "derived_checked": 0,
                "derived_dealiased": 0,
                "ocr_slots_reset": 0,
                "invalid_video_checked": 0,
                "videos_quarantined": 0,
                "download_slots_reset": 0,
                "completed_at": storage.now_utc(),
            }
            self.state["local_failure_repair"] = receipt
            self._save()
            return receipt

        for content_id in linked_ids:
            local_results[str(content_id)].update(
                status="repair_intent", repair_kind="derived_evidence_hardlink"
            )
        for content_id in invalid_video_ids:
            local_results[str(content_id)].update(
                status="repair_intent", repair_kind="invalid_video_download"
            )
        self._save()

        versions = media.processor_versions()
        derived_dealiased = 0
        derived_bytes = 0
        ocr_slots_reset = 0
        for content_id in sorted(set(linked_ids)):
            with self._read() as connection:
                row = connection.execute(
                    """
                    SELECT e.id artifact_id,e.content_id,e.local_path,e.byte_size,
                      e.sha256,e.processor_version,c.link_id,f.id frames_slot_id,
                      o.id ocr_slot_id,o.status ocr_status,
                      o.attempt_count ocr_attempt_count,
                      o.output_artifact_id ocr_output_artifact_id,
                      o.error_message ocr_error_message,o.updated_at ocr_updated_at
                    FROM media_processing_slots f
                    JOIN evidence_artifacts e ON e.id=f.output_artifact_id
                    JOIN content_items c ON c.id=f.content_id
                    LEFT JOIN media_processing_slots o
                      ON o.content_id=f.content_id AND o.source_sha256=e.sha256
                     AND o.processor_type='ocr' AND o.processor_version=?
                    WHERE f.content_id=? AND f.processor_type='frames'
                      AND f.processor_version=? AND f.status='succeeded'
                      AND e.artifact_type='frames_manifest'
                      AND e.status='available' AND e.processor_version=?
                    ORDER BY f.id DESC LIMIT 1
                    """,
                    (
                        versions["ocr"],
                        content_id,
                        versions["frames"],
                        versions["frames"],
                    ),
                ).fetchone()
            if row is None:
                raise CampaignError(
                    f"local hardlink repair lost frames evidence for {content_id}"
                )
            changed, byte_size, _ = self._dealias_content_media_tree(
                content_id, str(row["link_id"])
            )
            manifest = media._read_private_file_evidence(
                media._resolved(str(row["local_path"])),
                label="repaired campaign frames manifest",
            )
            if (
                manifest.byte_size != int(row["byte_size"])
                or manifest.sha256 != str(row["sha256"])
            ):
                raise CampaignError("repaired frames manifest identity changed")
            derived_dealiased += changed
            derived_bytes += byte_size
            if row["ocr_slot_id"] is not None:
                if (
                    row["ocr_status"] == "retryable_failed"
                    and int(row["ocr_attempt_count"]) == 0
                    and row["ocr_error_message"] == DERIVED_LINK_REPAIR_MESSAGE
                ):
                    pass
                elif (
                    row["ocr_status"] == "running"
                    and int(row["ocr_attempt_count"]) == 1
                    and row["ocr_output_artifact_id"] is None
                ):
                    with storage.connect(self.db_path) as connection, storage.transaction(
                        connection
                    ):
                        cursor = connection.execute(
                            """
                            UPDATE media_processing_slots
                            SET status='retryable_failed',attempt_count=0,
                              error_message=?,updated_at=?
                            WHERE id=? AND content_id=? AND source_sha256=?
                              AND processor_type='ocr' AND processor_version=?
                              AND status='running' AND attempt_count=1
                              AND output_artifact_id IS NULL AND updated_at=?
                            """,
                            (
                                DERIVED_LINK_REPAIR_MESSAGE,
                                storage.now_utc(),
                                row["ocr_slot_id"],
                                content_id,
                                row["sha256"],
                                versions["ocr"],
                                row["ocr_updated_at"],
                            ),
                        )
                        if cursor.rowcount != 1:
                            raise CampaignError(
                                "campaign OCR slot changed before hardlink repair"
                            )
                    ocr_slots_reset += 1
                elif row["ocr_status"] != "succeeded":
                    raise CampaignError(
                        f"unexpected OCR slot state during repair: {row['ocr_status']}"
                    )
            local_results[str(content_id)] = {
                "content_id": content_id,
                "status": "repair_ready",
                "repair_kind": "derived_evidence_hardlink",
            }
            self._save()

        quarantine_root = self.run_root / "quarantine" / "invalid-video-downloads"
        quarantine_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        resolved_run_root = self.run_root.resolve(strict=True)
        quarantine_root = quarantine_root.resolve(strict=True)
        if not quarantine_root.is_relative_to(resolved_run_root):
            raise CampaignError("campaign quarantine escaped the run root")
        videos_quarantined = 0
        download_slots_reset = 0
        quarantined_bytes = 0
        quarantine_paths: list[str] = []
        for content_id in sorted(set(invalid_video_ids)):
            with self._read() as connection:
                row = connection.execute(
                    """
                    WITH current_source AS (
                      SELECT * FROM evidence_artifacts
                      WHERE content_id=? AND artifact_type='media_source'
                        AND status='available'
                      ORDER BY id DESC LIMIT 1
                    ), current_media AS (
                      SELECT * FROM evidence_artifacts
                      WHERE content_id=? AND artifact_type='media'
                      ORDER BY id DESC LIMIT 1
                    )
                    SELECT e.id artifact_id,e.content_id,e.local_path,e.byte_size,
                      e.sha256,e.status artifact_status,e.processor_version,
                      e.metadata_json,s.id slot_id,s.source_sha256,
                      s.status slot_status,s.attempt_count,s.output_artifact_id,
                      s.error_message,s.updated_at,f.id frames_slot_id,
                      f.status frames_status,f.error_message frames_error_message
                    FROM current_source src
                    JOIN current_media e ON e.content_id=src.content_id
                    JOIN media_processing_slots s
                      ON s.content_id=src.content_id
                     AND s.source_sha256=src.sha256
                     AND s.processor_type='download'
                     AND s.processor_version=?
                    LEFT JOIN media_processing_slots f
                      ON f.content_id=e.content_id AND f.source_sha256=e.sha256
                     AND f.processor_type='frames' AND f.processor_version=?
                    """,
                    (
                        content_id,
                        content_id,
                        media.VIDEO_DOWNLOAD_VERSION,
                        versions["frames"],
                    ),
                ).fetchone()
            if row is None or row["processor_version"] != media.VIDEO_DOWNLOAD_VERSION:
                raise CampaignError(
                    f"invalid-video repair lost current download for {content_id}"
                )
            try:
                metadata = json.loads(str(row["metadata_json"]))
            except json.JSONDecodeError as exc:
                raise CampaignError("invalid-video artifact metadata is corrupt") from exc
            if not isinstance(metadata, dict) or "media_lifecycle" in metadata:
                raise CampaignError("managed media is outside this repair boundary")
            already_reset = (
                row["slot_status"] == "retryable_failed"
                and int(row["attempt_count"]) == 0
                and row["output_artifact_id"] is None
                and row["error_message"] == INVALID_VIDEO_REPAIR_MESSAGE
                and row["artifact_status"] == "failed"
            )
            preflight_invalid = (
                row["frames_slot_id"] is None
                and local_results[str(content_id)].get("error")
                == f"invalid media: {media._resolved(str(row['local_path']))}"
            )
            if not already_reset and not (
                row["slot_status"] == "succeeded"
                and int(row["attempt_count"]) >= 1
                and row["output_artifact_id"] == row["artifact_id"]
                and row["artifact_status"] == "available"
                and (
                    preflight_invalid
                    or (
                        row["frames_status"]
                        in {"retryable_failed", "terminal_failed"}
                        and row["frames_error_message"]
                        == "MediaProcessingError: no frames were extracted"
                    )
                )
            ):
                raise CampaignError(
                    f"invalid-video repair state changed for {content_id}"
                )
            active = media._resolved(str(row["local_path"]))
            media_root = media.MEDIA_ROOT.resolve(strict=True)
            media._require_no_symlink_below_root(
                active, root=media_root, label="campaign invalid video"
            )
            active = active.parent.resolve(strict=True) / active.name
            if not active.is_relative_to(media_root):
                raise CampaignError("invalid video is outside the canonical media root")
            quarantine = quarantine_root / (
                f"{content_id}-{str(row['sha256'])[:16]}.mp4"
            )
            moved = False
            if os.path.lexists(active):
                if os.path.lexists(quarantine):
                    raise CampaignError("invalid video active and quarantine paths both exist")
                evidence = media._read_private_file_evidence(
                    active, label="campaign invalid video"
                )
                if (
                    evidence.byte_size != int(row["byte_size"])
                    or evidence.sha256 != str(row["sha256"])
                ):
                    raise CampaignError("invalid video failed size/SHA-256 validation")
                if media._valid_media(active):
                    raise CampaignError("video became decodable before quarantine")
                os.replace(active, quarantine)
                media._fsync_directory(active.parent)
                media._fsync_directory(quarantine.parent)
                moved = True
            elif os.path.lexists(quarantine):
                evidence = media._read_private_file_evidence(
                    quarantine, label="quarantined campaign video"
                )
                if (
                    evidence.byte_size != int(row["byte_size"])
                    or evidence.sha256 != str(row["sha256"])
                ):
                    raise CampaignError("quarantined video identity changed")
            else:
                raise CampaignError("invalid video disappeared before quarantine")
            try:
                if not already_reset:
                    repaired_at = storage.now_utc()
                    with storage.connect(self.db_path) as connection, storage.transaction(
                        connection
                    ):
                        artifact_cursor = connection.execute(
                            """
                            UPDATE evidence_artifacts SET status='failed'
                            WHERE id=? AND content_id=? AND artifact_type='media'
                              AND local_path=? AND status='available'
                              AND byte_size=? AND sha256=? AND processor_version=?
                            """,
                            (
                                row["artifact_id"],
                                content_id,
                                row["local_path"],
                                row["byte_size"],
                                row["sha256"],
                                media.VIDEO_DOWNLOAD_VERSION,
                            ),
                        )
                        slot_cursor = connection.execute(
                            """
                            UPDATE media_processing_slots
                            SET status='retryable_failed',attempt_count=0,
                              output_artifact_id=NULL,error_message=?,updated_at=?
                            WHERE id=? AND content_id=? AND source_sha256=?
                              AND processor_type='download' AND processor_version=?
                              AND status='succeeded' AND attempt_count=?
                              AND output_artifact_id=? AND updated_at=?
                            """,
                            (
                                INVALID_VIDEO_REPAIR_MESSAGE,
                                repaired_at,
                                row["slot_id"],
                                content_id,
                                row["source_sha256"],
                                media.VIDEO_DOWNLOAD_VERSION,
                                row["attempt_count"],
                                row["artifact_id"],
                                row["updated_at"],
                            ),
                        )
                        if artifact_cursor.rowcount != 1 or slot_cursor.rowcount != 1:
                            raise CampaignError(
                                "invalid video ledger changed before quarantine commit"
                            )
                    download_slots_reset += 1
            except Exception:
                if moved and not os.path.lexists(active):
                    os.replace(quarantine, active)
                    media._fsync_directory(active.parent)
                    media._fsync_directory(quarantine.parent)
                raise
            videos_quarantined += int(moved)
            quarantined_bytes += int(row["byte_size"]) if moved else 0
            quarantine_paths.append(str(quarantine))
            self.state["results"].setdefault("download", {})[
                str(content_id)
            ] = {
                "content_id": content_id,
                "status": "repair_ready",
                "reason": "verified_undecodable_video_quarantined",
            }
            local_results[str(content_id)] = {
                "content_id": content_id,
                "status": "repair_ready",
                "repair_kind": "invalid_video_download",
            }
            self._save()

        completed_at = storage.now_utc()
        receipt = {
            "status": "succeeded",
            "derived_checked": len(set(linked_ids)),
            "derived_dealiased": derived_dealiased,
            "derived_repaired_bytes": derived_bytes,
            "ocr_slots_reset": ocr_slots_reset,
            "invalid_video_checked": len(set(invalid_video_ids)),
            "videos_quarantined": videos_quarantined,
            "quarantined_bytes": quarantined_bytes,
            "download_slots_reset": download_slots_reset,
            "quarantine_paths": quarantine_paths,
            "content_ids": sorted(set(linked_ids) | set(invalid_video_ids)),
            "completed_at": completed_at,
        }
        if isinstance(existing, dict) and existing.get("status") == "succeeded":
            self.state.setdefault("local_failure_repair_history", []).append(existing)
        self.state["local_failure_repair"] = receipt
        self._save()
        return receipt

    @contextmanager
    def window(self, *, prepare: bool = True) -> Iterator["Campaign"]:
        if self._locked:
            raise CampaignError("nested maintenance window")
        formal = self._is_formal()
        if formal:
            if os.environ.get("DCAR_TEST_DENY_FORMAL_DB") == "1":
                raise CampaignError("test process attempted to open the formal DCar database")
            if not storage.same_database_path(REPO, PRODUCTION_ROOT):
                raise CampaignError("isolated checkout may not write the formal database; integrate first")
            if not FREEZE_LOCK.is_file() or FREEZE_LOCK.is_symlink():
                raise CampaignError("formal campaign requires the canonical operator freeze lock")
            if not WRITER_LOCK.is_file() or WRITER_LOCK.is_symlink():
                raise CampaignError("formal campaign requires the canonical writer lock")
            media.pinned_whisper_model_path(local_files_only=True)
            if not media.ocr_binary_path().is_file() or any(shutil.which(tool) is None for tool in ("ffmpeg", "ffprobe")):
                raise CampaignError("prepare local OCR/ffmpeg/ffprobe before paying for discovery")
        self.run_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        lock_path = WRITER_LOCK if formal else self.run_root / "window.lock"
        descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
        default_network_slots = capture.TIKHUB_NETWORK_SLOTS
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise CampaignError("another writer owns this maintenance window") from exc
            self._locked = True
            self._stop_paid.clear()
            # The maintenance campaign is the only writer in this process, so
            # it may use its explicitly tested network bound without changing
            # the daily worker's conservative process-wide default.
            capture.TIKHUB_NETWORK_SLOTS = BoundedSemaphore(CAPTURE_WORKERS)
            if prepare:
                self._prepare()
            yield self
        finally:
            capture.TIKHUB_NETWORK_SLOTS = default_network_slots
            self._locked = False
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def _phase_contract(self, phase: str) -> None:
        self._require_window()
        rb._prepare_campaign_contract(
            task_id=self.task_id,
            start=self.start,
            end=self.end,
            as_of=self.as_of,
            max_amount=self.max_amount,
            max_pages=MAX_PAGES,
            platforms=PLATFORMS,
            phase=phase, state_root=self.day_root,
        )

    def _check_dispatch(self, *, in_flight: bool = False) -> None:
        self._require_window()
        if self._stop_paid.is_set() and not in_flight:
            raise CampaignError("provider/budget blocked; no further paid calls in this window")
        if self._deadline is not None and time.monotonic() >= self._deadline:
            raise CampaignError("maintenance window dispatch deadline reached")
        if self._is_formal() and self.day != datetime.now(rb.SHANGHAI).date().isoformat():
            raise CampaignError("Beijing capture day changed; resume with a new daily state root")

    def _call(self, operation: str, item: Mapping[str, Any], *, override: Any = None) -> capture.ProviderResult:
        # capture.py has already crossed its durable send boundary before this
        # callback runs. A peer may have opened the circuit in the meantime;
        # cancelling here would create a false billing-unknown receipt. Let the
        # bounded in-flight call finish and stop only subsequent claims.
        self._check_dispatch(in_flight=True)
        try:
            return self._dispatch(operation, item, override=override)
        except Exception as exc:
            if getattr(exc, "error_code", None) in rb.BLOCKING_CODES:
                self._stop_paid.set()
            raise

    def _dispatch(self, operation: str, item: Mapping[str, Any], *, override: Any = None) -> capture.ProviderResult:
        if override is not None:
            return override(operation, item)
        key = providers._load_key(providers.TIKHUB_KEY_FILE, "TIKHUB_API_KEY")
        platform = str(item["platform"])
        if operation == "resolve_account":
            return providers._douyin_reference_call(str(item["uid"]), key)
        if operation == "discover_content":
            if platform == "douyin":
                with self._read() as connection:
                    row = connection.execute("""
                        SELECT reference_value FROM account_provider_references
                        WHERE account_identity_id=? AND provider='TikHub' AND reference_kind='sec_user_id'
                    """, (item["id"],)).fetchone()
                if row is None:
                    raise CampaignError("discovery has no resolved sec_user_id")
                return providers._douyin_discovery_call(str(row[0]), key, item.get("cursor") or 0)
            return providers._xhs_discovery_call(str(item["uid"]), key, item.get("cursor") or "")
        if operation not in {
            "detail",
            "metrics",
            "douyin_web_detail",
            "douyin_high_quality_video",
        }:
            raise CampaignError(f"unapproved operation: {operation}")
        if platform == "douyin":
            if operation == "douyin_web_detail":
                return providers._douyin_web_detail_call(
                    str(item["platform_content_id"]), key
                )
            if operation == "douyin_high_quality_video":
                return providers._douyin_high_quality_video_call(
                    str(item["platform_content_id"]), key
                )
            return providers._douyin_call(operation, str(item["platform_content_id"]), key)
        if operation in {"douyin_web_detail", "douyin_high_quality_video"}:
            raise CampaignError("Douyin media fallback cannot serve another platform")
        return providers._xhs_call(operation, str(item["platform_content_id"]), key, str(item["content_type"]))

    def _merge_manifests(self) -> None:
        visited = set(self.state["visited_ids"])
        touched: set[int] = set()
        signatures = self.state.setdefault("manifest_sha256", {})
        for path in sorted(
            self.run_root.glob(f"20??-??-??/{self.task_id}.contents.json")
        ):
            body = path.read_bytes()
            signature = hashlib.sha256(body).hexdigest()
            manifest = json.loads(body)
            if (
                manifest.get("task_id"),
                manifest.get("start"),
                manifest.get("end"),
            ) != (
                self.task_id,
                rb._iso(self.start),
                rb._iso(self.end),
            ):
                raise CampaignError("discovery manifest contract mismatch")
            manifest_ids = {int(item["content_id"]) for item in manifest["contents"]}
            visited.update(manifest_ids)
            key = str(path.relative_to(self.run_root))
            if signatures.get(key) != signature:
                touched.update(manifest_ids)
                signatures[key] = signature
        self.state["visited_ids"] = sorted(visited)
        # A crash after upsert but before manifest write can replay as 'updated'.
        # Baseline exclusion, not first_action alone, preserves those new IDs.
        self.state["new_ids"] = sorted(set(self.state["new_ids"]) | (visited - set(self.state["baseline_ids"])))
        with self._read() as connection:
            existing_fingerprints = {int(row[0]) for row in connection.execute("SELECT DISTINCT content_id FROM duplicate_fingerprints")}
        self.state["relation_pending_ids"] = sorted(set(self.state["relation_pending_ids"]) | (touched & existing_fingerprints))
        self._save()

    def discover(self, call_override: Any = None) -> dict[str, Any]:
        self._phase_contract("discover")
        if self.state["discovery_complete"]:
            return {"status": "already_succeeded", "usage": self.usage()}
        result = rb.run_discovery_backfill(
            start=self.start,
            end=self.end,
            as_of=self.as_of,
            task_id=self.task_id,
            max_amount=self.max_amount,
            db_path=self.db_path,
            platforms=PLATFORMS,
            max_pages_per_account=MAX_PAGES, state_root=self.day_root,
            archive_before=self.start, require_live_detail=True,
            skip_existing_derived_stages=True, workers=4,
            call_override=partial(self._call, override=call_override),
            resume_completed=True,
        )
        self._merge_manifests()
        self.state["discovery_complete"] = result["status"] == "succeeded"
        self.state["discovery"] = {key: result[key] for key in (
            "status", "accounts_considered", "accounts_completed", "pages_processed", "failed_pages", "stopped_reason"
        )}
        self._save()
        # upsert deletes old fingerprint relations even when derived stages skip.
        self.repair_relations()
        return result

    def _content(self, content_id: int) -> dict[str, Any]:
        self._require_window()
        with self._read() as connection:
            row = connection.execute("SELECT * FROM content_items WHERE id=?", (content_id,)).fetchone()
            release = rb._active_evaluation_release_id(connection)
        if release != RELEASE_ID:
            raise CampaignError("active release changed during campaign")
        if (
            row is None
            or row["platform"] not in PLATFORMS
            or not (
                rb._iso(self.start)
                <= str(row["published_at"])
                <= rb._iso(self.end)
            )
        ):
            raise CampaignError(f"content {content_id} is outside the fixed campaign range")
        return dict(row)

    def _slots(self, content_id: int) -> list[dict[str, Any]]:
        with self._read() as connection:
            return [dict(row) for row in connection.execute(
                "SELECT * FROM fetch_slots WHERE content_id=? ORDER BY window_key,id", (content_id,))]

    def _paid_content_authorized(self, content: Mapping[str, Any]) -> bool:
        content_id = int(content["id"])
        return (
            content_id in self.state["new_ids"]
            and content_id not in self.state["baseline_ids"]
        ) or (
            content_id in self.state["existing_pending_ids"]
            and content["source_group"] == storage.HISTORY_BACKFILL_SOURCE_GROUP
        )

    def _paid_media_authorized(self, content: Mapping[str, Any]) -> bool:
        """Authorize media recovery for the campaign's frozen evidence debt."""

        content_id = int(content["id"])
        return self._paid_content_authorized(content) or content_id in self.state[
            "existing_pending_ids"
        ]

    def _restore(self, content: Mapping[str, Any], slot: Mapping[str, Any]) -> dict[str, Any]:
        stage, window = str(slot["stage"]), str(slot["window_key"])
        ready = providers._stage_storage_exists(content_id=int(content["id"]), platform=str(content["platform"]), stage=stage, window_key=window, db_path=self.db_path)
        applied = providers._slot_raw_is_applied(content_id=int(content["id"]), stage=stage, window_key=window, db_path=self.db_path)
        if ready and (applied or slot["provider"] == "legacy-cache"):
            return {"stage": stage, "status": "already_succeeded", "window_key": window, "amount": 0.0}
        outcome = providers._replay_content_stage(
            content, stage=stage, window_key=window,
            operation=providers.STAGE_CONFIG[(str(content["platform"]), stage)][2], db_path=self.db_path,
        )
        providers._store_stage_result(content, stage, window, outcome, db_path=self.db_path)
        return {"stage": stage, "status": "replayed", "window_key": window, "amount": 0.0}

    def capture_one(self, content_id: int, call_override: Any = None) -> dict[str, Any]:
        content = self._content(content_id)
        if not self._paid_content_authorized(content):
            raise CampaignError(
                "paid content capture is restricted to new content or exact "
                "history-backfill debt"
            )
        slots = self._slots(content_id)
        detail = next((s for s in slots if s["stage"] == "detail" and s["window_key"] == "lifetime" and s["status"] == "succeeded"), None)
        metric = next((s for s in slots if s["stage"] == "metrics" and s["status"] == "succeeded"), None)
        restored = [self._restore(content, s) for s in (detail, metric) if s is not None]
        stages = [] if detail is not None else ["detail"]
        as_of = self.as_of.astimezone(rb.SHANGHAI).date()
        if metric is None:
            if any(s["stage"] == "metrics" and s["status"] in {"running", "terminal_failed"} for s in slots):
                raise CampaignError("previous metric attempt is running/terminal; a new date must not bypass it")
            stages.append("metrics")
            if content["platform"] == "xiaohongshu":
                stages = ["detail", "metrics"]
                if detail is not None:
                    raw = capture.load_succeeded_raw_response(content_id=content_id, stage="detail", window_key="lifetime", db_path=self.db_path)
                    as_of = rb._parse_datetime(raw.captured_at).astimezone(rb.SHANGHAI).date()
        if not stages:
            return {"content_id": content_id, "status": "succeeded", "stages": restored, "provider_cost": 0.0}
        if content["source_group"] != storage.HISTORY_BACKFILL_SOURCE_GROUP:
            raise CampaignError("new content must retain history-backfill until local completion")
        self._check_dispatch()
        roster = self.state["contract"]["roster"]
        with provider_budget.paid_scope(
            "history", roster_snapshot_id=int(roster["snapshot_id"]),
            roster_snapshot_hash=str(roster["members_sha256"]),
        ):
            result = providers.update_content_data(
                content_id, db_path=self.db_path, as_of=as_of, stages=stages,
                process_media=False,
                task_id=self.task_id,
                task_max_amount=self.max_amount,
                call_override=partial(self._call, override=call_override),
            )
        result["stages"] = restored + list(result["stages"])
        return result

    def _raw(self, raw_id: int) -> Any:
        with self._read() as connection:
            row = connection.execute("SELECT local_path,sha256,byte_size FROM provider_raw_responses WHERE id=?", (raw_id,)).fetchone()
        if row is None:
            raise CampaignError("source raw response is missing")
        path = Path(row["local_path"])
        resolved = path if path.is_absolute() else storage.PROJECT_ROOT / path
        try:
            return raw_evidence.read_raw_json(
                resolved,
                expected_stored_sha256=str(row["sha256"]),
                expected_stored_size=int(row["byte_size"]),
            )
        except (raw_evidence.RawEvidenceError, ValueError) as exc:
            raise CampaignError("source raw response integrity check failed") from exc

    def _groups(self, content: Mapping[str, Any], source: Mapping[str, Any] | None) -> list[dict[str, Any]] | None:
        if content["content_type"] != "image" or source is None:
            return None
        if content["platform"] != "douyin":
            return media.image_source_groups(source["urls"], platform=str(content["platform"]))
        raw_id = source.get("raw_response_id")
        seen: set[int] = set()
        while raw_id is not None and int(raw_id) not in seen and len(seen) < 4:
            seen.add(int(raw_id))
            raw = self._raw(int(raw_id))
            item = providers._find_aweme(raw, str(content["platform_content_id"]))
            if item is not None and item.get("images"):
                return media.douyin_image_source_groups(source["urls"], providers._douyin_image_url_groups(item))
            raw_id = raw.get("source_raw_response_id") if isinstance(raw, dict) else None
        raise CampaignError("Douyin image source lacks matching frozen raw images[] groups; no paid fallback")

    def _douyin_audio_post_parts(
        self,
        content: Mapping[str, Any],
        source: Mapping[str, Any],
    ) -> tuple[int, list[str], list[str]]:
        raw_id = source.get("raw_response_id")
        seen: set[int] = set()
        item: Mapping[str, Any] | None = None
        evidence_raw_id: int | None = None
        while raw_id is not None and int(raw_id) not in seen and len(seen) < 4:
            evidence_raw_id = int(raw_id)
            seen.add(evidence_raw_id)
            raw = self._raw(evidence_raw_id)
            item = providers._find_aweme(
                raw, str(content["platform_content_id"])
            )
            if item is not None:
                break
            raw_id = (
                raw.get("source_raw_response_id")
                if isinstance(raw, dict)
                else None
            )
        if item is None or evidence_raw_id is None:
            raise CampaignError("Douyin audio-post source lacks matching raw evidence")
        if (
            item.get("aweme_type") != 163
            or item.get("media_type") != 43
            or item.get("images") not in (None, [])
        ):
            raise CampaignError("Douyin source is not a proven audio-only post")
        audio_urls = providers._douyin_media_urls(item, "video")
        if audio_urls != list(source["urls"]):
            raise CampaignError("Douyin audio-post URLs changed from frozen source")
        video = item.get("video")
        if not isinstance(video, Mapping):
            raise CampaignError("Douyin audio-post raw has no video envelope")
        cover_urls: list[str] = []
        for name in ("origin_cover", "cover"):
            value = video.get(name)
            if not isinstance(value, Mapping):
                continue
            urls = value.get("url_list")
            if isinstance(urls, list):
                cover_urls.extend(
                    url
                    for url in urls
                    if isinstance(url, str) and url.startswith("https://")
                )
        cover_urls = list(dict.fromkeys(cover_urls))
        if not cover_urls:
            raise CampaignError("Douyin audio-post raw has no frozen cover URL")
        return evidence_raw_id, audio_urls, cover_urls

    @staticmethod
    def _download_audio_post_part(
        urls: Sequence[str],
        target: Path,
        *,
        accept: str,
        validator: Any,
        urlopen_fn: Any,
    ) -> None:
        opener = urlopen_fn or urllib.request.urlopen
        failures: list[str] = []
        for url in urls:
            valid = False
            request = urllib.request.Request(
                url,
                headers={"User-Agent": "Mozilla/5.0", "Accept": accept},
            )
            try:
                with opener(request, timeout=90) as response:
                    declared = response.headers.get("Content-Length")
                    if declared is not None and int(declared) > media.DEFAULT_MAX_MEDIA_DOWNLOAD_BYTES:
                        raise CampaignError("audio-post source exceeds byte limit")
                    with target.open("xb") as output:
                        total = 0
                        while True:
                            block = response.read(1024 * 1024)
                            if not block:
                                break
                            total += len(block)
                            if total > media.DEFAULT_MAX_MEDIA_DOWNLOAD_BYTES:
                                raise CampaignError(
                                    "audio-post source exceeds byte limit"
                                )
                            output.write(block)
                    valid = bool(validator(target))
                    if valid:
                        return
                    failures.append("invalid media body")
            except Exception as exc:
                failures.append(type(exc).__name__)
            finally:
                if target.exists() and not valid:
                    target.unlink()
        raise CampaignError(
            "Douyin audio-post part download failed: "
            + " | ".join(failures[-3:])
        )

    @staticmethod
    def _has_audio_stream(path: Path) -> bool:
        completed = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "a:0",
                "-show_entries",
                "stream=codec_type",
                "-of",
                "default=nw=1:nk=1",
                str(path),
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        return completed.returncode == 0 and "audio" in {
            line.strip().lower() for line in completed.stdout.splitlines()
        }

    @staticmethod
    def _audio_duration(path: Path) -> float:
        completed = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "a:0",
                "-show_entries",
                "stream=duration",
                "-of",
                "default=nw=1:nk=1",
                str(path),
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

    @staticmethod
    def _has_decodable_audio(path: Path) -> bool:
        completed = subprocess.run(
            [
                "ffmpeg",
                "-nostdin",
                "-v",
                "error",
                "-xerror",
                "-i",
                str(path),
                "-map",
                "0:a:0",
                "-f",
                "null",
                "-",
            ],
            check=False,
            capture_output=True,
            timeout=180,
        )
        return completed.returncode == 0

    def _materialize_douyin_audio_post_video(
        self,
        content: Mapping[str, Any],
        source: Mapping[str, Any],
        *,
        urlopen_fn: Any,
    ) -> dict[str, Any]:
        cid = int(content["id"])
        evidence_raw_id, audio_urls, cover_urls = self._douyin_audio_post_parts(
            content, source
        )
        with self._read() as connection:
            source_raw = connection.execute(
                "SELECT sha256,captured_at,operation FROM provider_raw_responses WHERE id=?",
                (evidence_raw_id,),
            ).fetchone()
        if source_raw is None:
            raise CampaignError("Douyin audio-post evidence raw disappeared")
        window_key = "audio-post-synthesis"
        operation = "douyin_audio_post_synthesis_derived"
        slot = next(
            (
                row
                for row in self._slots(cid)
                if row["stage"] == "detail" and row["window_key"] == window_key
            ),
            None,
        )
        if slot is not None and slot["status"] == "succeeded":
            raw = capture.load_succeeded_raw_response(
                content_id=cid,
                stage="detail",
                window_key=window_key,
                operation=operation,
                db_path=self.db_path,
            )
            data = raw.value.get("data")
            if not isinstance(data, dict):
                raise CampaignError("audio-post derived raw has no normalized data")
            outcome = capture.CaptureOutcome(
                raw.slot_id,
                0,
                raw.raw_response_id,
                {**data, "_evidence_captured_at": raw.captured_at},
                False,
                0.0,
                "USD",
            )
        else:
            normalized = {
                "title": str(content.get("title") or ""),
                "body": str(content.get("body") or ""),
                "published_at": content.get("published_at"),
                "account_uid": str(content.get("raw_account_uid") or ""),
                "account_name": str(content.get("raw_account_name") or ""),
                "content_type": "video",
                "media_urls": audio_urls,
            }
            derived = capture.ProviderResult(
                {**normalized, "_evidence_captured_at": source_raw["captured_at"]},
                {
                    "stage": "detail",
                    "data": normalized,
                    "derived_from_operation": str(source_raw["operation"]),
                    "source_raw_response_id": evidence_raw_id,
                    "source_sha256": str(source_raw["sha256"]),
                    "source_captured_at": str(source_raw["captured_at"]),
                    "audio_post_synthesis": {
                        "aweme_type": 163,
                        "media_type": 43,
                        "audio_urls": audio_urls,
                        "cover_urls": cover_urls,
                        "method": "static-cover-plus-original-audio-v1",
                    },
                },
                200,
                False,
            )
            outcome = capture.execute_derived_content_fetch(
                content_id=cid,
                stage="detail",
                window_key=window_key,
                provider="TikHub",
                adapter_version="douyin-audio-post-synthesis-v1",
                operation=operation,
                result=derived,
                source_raw_response_id=evidence_raw_id,
                db_path=self.db_path,
                raw_root=(
                    capture.RAW_ROOT / self.task_id / "audio-post-synthesis"
                ),
                allow_terminal_retry=True,
            )
        providers._store_stage_result(
            content,
            "detail",
            window_key,
            outcome,
            db_path=self.db_path,
            applied_source="derived_applied",
        )
        derived_source = media.get_media_source_state(cid, db_path=self.db_path)
        if derived_source is None or derived_source["raw_response_id"] != outcome.raw_response_id:
            raise CampaignError("audio-post derived source did not become current")
        with tempfile.TemporaryDirectory(prefix="dcar-audio-post-") as temporary:
            root = Path(temporary)
            audio_path = root / "source.m4a"
            cover_path = root / "cover.img"
            video_path = root / "canonical.mp4"
            self._download_audio_post_part(
                audio_urls,
                audio_path,
                accept="audio/*,*/*;q=0.8",
                validator=self._has_audio_stream,
                urlopen_fn=urlopen_fn,
            )
            self._download_audio_post_part(
                cover_urls,
                cover_path,
                accept="image/*,*/*;q=0.8",
                validator=media._valid_image,
                urlopen_fn=urlopen_fn,
            )
            input_audio_duration = self._audio_duration(audio_path)
            if (
                input_audio_duration <= 0
                or not self._has_decodable_audio(audio_path)
            ):
                raise CampaignError("Douyin audio-post source audio is not decodable")
            completed = subprocess.run(
                [
                    "ffmpeg",
                    "-n",
                    "-nostdin",
                    "-v",
                    "error",
                    "-loop",
                    "1",
                    "-framerate",
                    "1",
                    "-i",
                    str(cover_path),
                    "-i",
                    str(audio_path),
                    "-map",
                    "0:v:0",
                    "-map",
                    "1:a:0",
                    "-c:v",
                    "libx264",
                    "-preset",
                    "veryfast",
                    "-tune",
                    "stillimage",
                    "-vf",
                    "scale=trunc(iw/2)*2:trunc(ih/2)*2,format=yuv420p",
                    "-c:a",
                    "copy",
                    "-shortest",
                    "-movflags",
                    "+faststart",
                    "-r",
                    "1",
                    str(video_path),
                ],
                check=False,
                capture_output=True,
                timeout=180,
            )
            output_audio_duration = self._audio_duration(video_path)
            duration_tolerance = max(0.25, input_audio_duration * 0.01)
            if (
                completed.returncode != 0
                or not media._valid_media(video_path)
                or not self._has_audio_stream(video_path)
                or not self._has_decodable_audio(video_path)
                or abs(output_audio_duration - input_audio_duration)
                > duration_tolerance
            ):
                raise CampaignError("Douyin audio-post MP4 synthesis failed")

            def synthesized_open(request: Any, timeout: int = 90) -> _LocalVideoResponse:
                del timeout
                requested_url = str(request.full_url)
                if requested_url not in audio_urls:
                    raise CampaignError("synthesized video requested an unknown source URL")
                return _LocalVideoResponse(video_path, requested_url)

            downloaded = media.process_content_media(
                cid,
                db_path=self.db_path,
                download_only=True,
                reuse_existing_downloads=True,
                urlopen_fn=synthesized_open,
            )
        if downloaded.get("status") != "downloaded":
            raise CampaignError("Douyin audio-post synthesized download did not close")
        current = media.get_media_source_state(cid, db_path=self.db_path)
        if (
            current is None
            or not isinstance(current.get("download_slot"), dict)
            or current["download_slot"].get("status") != "succeeded"
        ):
            raise CampaignError("Douyin audio-post synthesized media is not durable")
        self.state["refresh_intents"][str(cid)].update(
            status="succeeded",
            audio_synthesis_raw_response_id=outcome.raw_response_id,
            media_artifact_id=downloaded.get("artifact_id"),
        )
        self._save()
        return current

    def _refresh(
        self,
        content: Mapping[str, Any],
        before: Mapping[str, Any],
        call_override: Any,
        *,
        use_douyin_web_detail: bool = False,
        urlopen_fn: Any = None,
    ) -> dict[str, Any]:
        cid = int(content["id"])
        if not self._paid_media_authorized(content):
            raise CampaignError(
                "content is not authorized for paid campaign source refresh"
            )
        if use_douyin_web_detail and (
            content["platform"] != "douyin" or content["content_type"] != "video"
        ):
            raise CampaignError(
                "Douyin Web detail override requires an authorized Douyin video"
            )
        if not any(s["stage"] == "detail" and s["status"] == "succeeded" for s in self._slots(cid)):
            raise CampaignError("source refresh requires a successfully purchased detail")
        provider, _, operation, price = providers.STAGE_CONFIG[(str(content["platform"]), "detail")]
        prior = next(
            (
                slot
                for slot in self._slots(cid)
                if slot["stage"] == "media_source_refresh"
                and slot["window_key"] == "lifetime"
            ),
            None,
        )
        intent = self.state["refresh_intents"].get(str(cid))
        if prior is not None and prior["status"] == "succeeded":
            raw = capture.load_succeeded_raw_response(content_id=cid, stage="media_source_refresh", window_key="lifetime", operation=operation, db_path=self.db_path)
            if content["platform"] == "douyin":
                parsed = providers._parse_douyin_stage_payload("detail", str(content["platform_content_id"]), raw.value, status=raw.http_status or 200)
            else:
                parsed = providers._parse_xhs_stage_payload("detail", str(content["platform_content_id"]), str(content["content_type"]), raw.value, status=raw.http_status or 200)
            outcome = capture.CaptureOutcome(raw.slot_id, 0, raw.raw_response_id, {**parsed.data, "_evidence_captured_at": raw.captured_at}, False, 0.0, "USD")
        else:
            if intent is not None or (prior is not None and prior["attempt_count"]):
                raise CampaignError("source refresh already attempted or outcome uncertain; automatic second purchase forbidden")
            self._check_dispatch()
            self.state["refresh_intents"][str(cid)] = {"status": "intent", "source_sha256": before["source_sha256"], "created_at": storage.now_utc()}
            self._save()  # Persist BEFORE the billable request; never reset on resume.
            budget = providers._budget_for_call(
                provider=provider,
                operation=operation,
                price=price,
                task_id=self.task_id,
                task_max_amount=self.max_amount,
                db_path=self.db_path,
            )
            roster = self.state["contract"]["roster"]
            with provider_budget.paid_scope(
                "history", roster_snapshot_id=int(roster["snapshot_id"]),
                roster_snapshot_hash=str(roster["members_sha256"]),
            ):
                refresh_operation = (
                    "douyin_web_detail" if use_douyin_web_detail else "detail"
                )
                platform_content_id = str(content["platform_content_id"])
                if content["platform"] == "douyin":
                    request_params = {"aweme_id": platform_content_id}
                else:
                    _, request_params = providers._xhs_request(
                        "detail",
                        platform_content_id,
                        str(content["content_type"]),
                    )
                outcome = capture.execute_content_fetch(
                    content_id=cid, stage="media_source_refresh", window_key="lifetime",
                    provider=provider, adapter_version=(
                        "tikhub-douyin-web-media-source-refresh-v8.1"
                        if use_douyin_web_detail
                        else "tikhub-media-source-refresh-v8.1"
                        if content["platform"] == "douyin"
                        else "tikhub-xhs-app-v2-media-source-refresh-v8.1"
                    ),
                    operation=operation,
                    call=partial(
                        self._call,
                        refresh_operation,
                        content,
                        override=call_override,
                    ),
                    db_path=self.db_path,
                    # Separate request provenance from the original detail slot:
                    # both stages can have attempt-001 and byte-identical payloads.
                    raw_root=(
                        capture.RAW_ROOT
                        / self.task_id
                        / "media-source-refresh"
                    ),
                    budget_id=budget,
                    task_id=self.task_id,
                    task_max_amount=self.max_amount,
                    allow_terminal_retry=False,
                    paid_request_identity=providers._paid_request_identity(
                        operation=operation,
                        platform=str(content["platform"]),
                        subject=platform_content_id,
                        params=request_params,
                        cursor=None,
                        due_bucket="lifetime",
                    ),
                )
        providers._store_stage_result(content, "detail", "lifetime", outcome, db_path=self.db_path)
        source = media.get_media_source_state(cid, db_path=self.db_path)
        old_sha = (intent or self.state["refresh_intents"].get(str(cid)) or {}).get("source_sha256", before["source_sha256"])
        if (source is None or source["raw_response_id"] != outcome.raw_response_id or not source["urls"]
                or not all(media.is_supported_media_url(url) for url in source["urls"])
                or source["source_sha256"] == old_sha):
            if use_douyin_web_detail and source is not None:
                self.state["refresh_intents"][str(cid)].update(
                    status="web_source_unusable",
                    web_raw_response_id=outcome.raw_response_id,
                )
                self._save()
                return self._high_quality_refresh(
                    content,
                    source,
                    call_override,
                    urlopen_fn=urlopen_fn,
                )
            raise CampaignError("paid refresh did not provide a valid, different media source")
        self._groups(content, source)  # Freeze the NEW raw groups, never reuse old groups.
        self.state["refresh_intents"][str(cid)] = {"status": "succeeded", "source_sha256": old_sha, "raw_response_id": outcome.raw_response_id}
        self._save()
        return source

    def _high_quality_refresh(
        self,
        content: Mapping[str, Any],
        before: Mapping[str, Any],
        call_override: Any,
        *,
        urlopen_fn: Any = None,
    ) -> dict[str, Any]:
        cid = int(content["id"])
        if (
            not self._paid_media_authorized(content)
            or content["platform"] != "douyin"
            or content["content_type"] != "video"
        ):
            raise CampaignError(
                "high-quality source fallback requires an authorized Douyin video"
            )
        intent = self.state["refresh_intents"].get(str(cid))
        if intent is None or intent.get("status") not in {
            "web_source_unusable",
            "high_quality_intent",
        }:
            raise CampaignError(
                "high-quality source fallback requires a rejected Web source"
            )
        window_key = "high-quality"
        operation = "douyin_video_high_quality_play_url"
        prior = next(
            (
                slot
                for slot in self._slots(cid)
                if slot["stage"] == "media_source_refresh"
                and slot["window_key"] == window_key
            ),
            None,
        )
        if (
            prior is not None
            and prior["status"] == "retryable_failed"
            and prior["attempt_count"] >= 1
            and prior["last_error_code"] == "provider_retry_requested"
            and intent.get("status") == "high_quality_intent"
        ):
            try:
                return self._materialize_douyin_audio_post_video(
                    content, before, urlopen_fn=urlopen_fn
                )
            except CampaignError as error:
                if str(error) != "Douyin audio-post source lacks matching raw evidence":
                    raise
                raise CampaignError(
                    "high-quality source fallback is held pending compensation authorization"
                ) from error
        if prior is not None and prior["status"] == "succeeded":
            raw = capture.load_succeeded_raw_response(
                content_id=cid,
                stage="media_source_refresh",
                window_key=window_key,
                operation=operation,
                db_path=self.db_path,
            )
            parsed = providers._parse_douyin_high_quality_video_payload(
                str(content["platform_content_id"]),
                raw.value,
                status=raw.http_status or 200,
            )
            outcome = capture.CaptureOutcome(
                raw.slot_id,
                0,
                raw.raw_response_id,
                {**parsed.data, "_evidence_captured_at": raw.captured_at},
                False,
                0.0,
                "USD",
            )
        else:
            if prior is not None:
                raise CampaignError(
                    "high-quality source fallback is held pending compensation authorization"
                )
            self._check_dispatch()
            self.state["refresh_intents"][str(cid)].update(
                status="high_quality_intent"
            )
            self._save()
            budget = providers._budget_for_call(
                provider="TikHub",
                operation=operation,
                price=providers.TIKHUB_DOUYIN_HIGH_QUALITY_PRICE,
                task_id=self.task_id,
                task_max_amount=self.max_amount,
                db_path=self.db_path,
            )
            roster = self.state["contract"]["roster"]
            with provider_budget.paid_scope(
                "history",
                roster_snapshot_id=int(roster["snapshot_id"]),
                roster_snapshot_hash=str(roster["members_sha256"]),
            ):
                outcome = capture.execute_content_fetch(
                    content_id=cid,
                    stage="media_source_refresh",
                    window_key=window_key,
                    provider="TikHub",
                    adapter_version="tikhub-douyin-high-quality-source-v8.1",
                    operation=operation,
                    call=partial(
                        self._call,
                        "douyin_high_quality_video",
                        content,
                        override=call_override,
                    ),
                    db_path=self.db_path,
                    raw_root=(
                        capture.RAW_ROOT
                        / self.task_id
                        / "high-quality-media-source"
                    ),
                    budget_id=budget,
                    task_id=self.task_id,
                    task_max_amount=self.max_amount,
                    allow_terminal_retry=False,
                    paid_request_identity=providers._paid_request_identity(
                        operation=operation,
                        platform="douyin",
                        subject=str(content["platform_content_id"]),
                        params={
                            "aweme_id": str(content["platform_content_id"]),
                            "share_url": (
                                "https://www.douyin.com/video/"
                                + str(content["platform_content_id"])
                            ),
                            "region": "CN",
                        },
                        cursor=None,
                        due_bucket=window_key,
                    ),
                )
        providers._store_stage_result(
            content, "detail", window_key, outcome, db_path=self.db_path
        )
        source = media.get_media_source_state(cid, db_path=self.db_path)
        original_sha = str(intent["source_sha256"])
        if (
            source is None
            or source["raw_response_id"] != outcome.raw_response_id
            or not source["urls"]
            or not all(
                media.is_supported_media_url(url) for url in source["urls"]
            )
            or source["source_sha256"] == original_sha
        ):
            raise CampaignError(
                "high-quality fallback did not provide a valid, different video source"
            )
        self.state["refresh_intents"][str(cid)].update(
            status="succeeded",
            high_quality_raw_response_id=outcome.raw_response_id,
        )
        self._save()
        return source

    def download_one(self, content_id: int, *, allow_refresh: bool = False, call_override: Any = None, urlopen_fn: Any = None) -> dict[str, Any]:
        content = self._content(content_id)
        if content_id not in set(self.state["new_ids"]) | set(self.state["existing_pending_ids"]):
            raise CampaignError("download is outside the missing-evidence cohort")
        terminal = rb._pinned_media_terminal_detail(db_path=self.db_path, release_id=RELEASE_ID, content_id=content_id)
        if terminal.state in {"complete", "terminal_insufficient"}:
            return {"content_id": content_id, "status": "already_succeeded"}
        paid_refresh_authorized = self._paid_media_authorized(content)
        baseline_video = (
            content_id in self.state["baseline_ids"]
            and content["content_type"] == "video"
        )
        old_video = False
        if content_id in self.state["baseline_ids"] and not paid_refresh_authorized:
            allow_refresh = False
        if baseline_video:
            with self._read() as connection:
                cached = connection.execute("SELECT 1 FROM evidence_artifacts WHERE content_id=? AND artifact_type='media' AND status='available' LIMIT 1", (content_id,)).fetchone()
            if cached is not None:
                old_video = True
                allow_refresh = False
                urlopen_fn = deny_network
            elif not paid_refresh_authorized:
                raise CampaignError("existing video cache missing; redownload/paid refresh not authorized")
        source = media.get_media_source_state(content_id, db_path=self.db_path)
        intent = self.state["refresh_intents"].get(str(content_id))
        if intent is not None and source is not None and intent["status"] in {
            "intent",
            "web_source_unusable",
            "high_quality_intent",
        }:
            if intent["status"] in {
                "web_source_unusable",
                "high_quality_intent",
            }:
                high_quality_slot = next(
                    (
                        slot
                        for slot in self._slots(content_id)
                        if slot["stage"] == "media_source_refresh"
                        and slot["window_key"] == "high-quality"
                    ),
                    None,
                )
                if (
                    high_quality_slot is not None
                    and high_quality_slot["status"] == "retryable_failed"
                    and not allow_refresh
                    and not (
                        high_quality_slot["attempt_count"] >= 1
                        and high_quality_slot["last_error_code"]
                        == "provider_retry_requested"
                        and intent["status"] == "high_quality_intent"
                    )
                ):
                    raise CampaignError(
                        "retryable high-quality source refresh requires explicit paid refresh authorization"
                    )
                source = self._high_quality_refresh(
                    content,
                    source,
                    call_override,
                    urlopen_fn=urlopen_fn,
                )
            else:
                refresh_slot = next(
                    (
                        slot
                        for slot in self._slots(content_id)
                        if slot["stage"] == "media_source_refresh"
                        and slot["window_key"] == "lifetime"
                    ),
                    None,
                )
                resume_web_detail = bool(
                    refresh_slot is not None
                    and refresh_slot["adapter_version"]
                    == "tikhub-douyin-web-media-source-refresh-v8.1"
                    and refresh_slot["status"] in {"retryable_failed", "succeeded"}
                )
                if (
                    resume_web_detail
                    and refresh_slot is not None
                    and refresh_slot["status"] == "retryable_failed"
                    and not allow_refresh
                ):
                    raise CampaignError(
                        "retryable Web source refresh requires explicit paid refresh authorization"
                    )
                source = self._refresh(
                    content,
                    source,
                    call_override,
                    use_douyin_web_detail=resume_web_detail,
                    urlopen_fn=urlopen_fn,
                )
        groups = self._groups(content, source)
        try:
            result = media.process_content_media(content_id, db_path=self.db_path, download_only=True, reuse_existing_downloads=True, urlopen_fn=urlopen_fn, frozen_image_groups=groups)
        except Exception as exc:
            # HTTP expiry/unavailable evidence only; disk, grouping, ASR/OCR and
            # generic transport failures must never trigger a paid detail call.
            # TikHub App V3 can expose an audio-only URL for a Douyin video;
            # that precise failure gets one Web-detail refresh instead of
            # purchasing the same App V3 source again.
            source_expired = bool(re.search(r"\bHTTP(?: Error)?[ :]+(?:403|404|410)\b", str(exc), re.IGNORECASE))
            xhs_image_source_exhausted = (
                isinstance(exc, media.MediaProcessingError)
                and content["platform"] == "xiaohongshu"
                and content["content_type"] == "image"
                and bool(
                    re.fullmatch(
                        r"image download incomplete: logical image group \d+ exhausted",
                        str(exc),
                    )
                )
            )
            use_douyin_web_detail = (
                not source_expired
                and isinstance(exc, media.MediaProcessingError)
                and content["platform"] == "douyin"
                and content["content_type"] == "video"
                and self._paid_media_authorized(content)
                and bool(
                    re.search(
                        r"\bnot a playable video\b", str(exc), re.IGNORECASE
                    )
                )
            )
            if not (
                allow_refresh
                and not old_video
                and source is not None
                and (
                    source_expired
                    or xhs_image_source_exhausted
                    or use_douyin_web_detail
                )
            ):
                raise
            source = self._refresh(
                content,
                source,
                call_override,
                use_douyin_web_detail=use_douyin_web_detail,
                urlopen_fn=urlopen_fn,
            )
            result = media.process_content_media(content_id, db_path=self.db_path, download_only=True, reuse_existing_downloads=True, urlopen_fn=urlopen_fn, frozen_image_groups=self._groups(content, source))
        if result["status"] not in {"downloaded", "evidence_ready"}:
            raise CampaignError(f"download did not complete: {result['status']}")
        return result

    def repair_relations(self) -> dict[str, Any]:
        self._require_window()
        pending = set(self.state["relation_pending_ids"])
        ready: list[int] = []
        with self._read() as connection:
            for cid in sorted(pending):
                _, source_sha = duplicates._current_source_state(connection, cid)
                row = connection.execute("SELECT source_sha256 FROM duplicate_fingerprints WHERE content_id=? AND fingerprint_version=? ORDER BY created_at DESC,id DESC LIMIT 1", (cid, duplicates.FINGERPRINT_VERSION)).fetchone()
                if row is not None and row[0] == source_sha:
                    ready.append(cid)
        if not ready:
            return {"restored": 0, "pending": len(pending)}
        result = duplicates.update_duplicate_relations_incremental(ready, db_path=self.db_path)
        self.state["relation_pending_ids"] = sorted(pending - set(ready))
        self._save()
        return {"restored": len(ready), "pending": len(self.state["relation_pending_ids"]), "relations": result}

    def _prepare_local_media_one(
        self,
        content_id: int,
        *,
        whisper_model_path: Path | None,
        ocr_binary: Path | None,
    ) -> dict[str, Any]:
        """Prepare local media evidence without campaign-state mutation."""

        content = self._content(content_id)
        if content_id not in set(self.state["new_ids"]) | set(self.state["existing_pending_ids"]):
            raise CampaignError("local analysis is outside the missing-evidence cohort")
        terminal = rb._pinned_media_terminal_detail(db_path=self.db_path, release_id=RELEASE_ID, content_id=content_id)
        result: dict[str, Any] = {"content_id": content_id}
        if terminal.reason == "source_missing":
            with self._read() as connection:
                provider_unavailable = content_id in (
                    rb.provider_terminal_unavailable_content_ids(
                        connection, [content_id]
                    )
                )
            if provider_unavailable:
                return {**result, "status": "provider_unavailable"}
        if terminal.state == "terminal_failed":
            return {**result, "status": "terminal_failed", "reason": terminal.reason}
        if terminal.state in {"complete", "terminal_insufficient"} or terminal.reason == "evaluation_pending":
            return {**result, "status": "ready"}
        source = media.get_media_source_state(content_id, db_path=self.db_path)
        slot = (source or {}).get("download_slot") or {}
        if slot.get("status") != "succeeded":
            return {**result, "status": "download_pending", "reason": terminal.reason}
        if content["content_type"] == "video" and whisper_model_path is None:
            raise CampaignError("local video preparation requires a pinned whisper model")
        if ocr_binary is None:
            raise CampaignError("local media preparation requires the pinned OCR binary")
        processed = media.process_content_media(
            content_id,
            db_path=self.db_path,
            download_only=False,
            reuse_existing_downloads=True,
            urlopen_fn=deny_network,
            frozen_image_groups=self._groups(content, source),
            whisper_model_path=whisper_model_path,
            ocr_binary=ocr_binary,
        )
        if processed["status"] != "evidence_ready":
            raise CampaignError(f"local media not ready: {processed['status']}")
        return {**result, "status": "ready"}

    def _evaluate_prepared_local_one(
        self, content_id: int, prepared: Mapping[str, Any]
    ) -> tuple[dict[str, Any], Any | None]:
        self._content(content_id)
        if content_id not in set(self.state["new_ids"]) | set(self.state["existing_pending_ids"]):
            raise CampaignError("local analysis is outside the missing-evidence cohort")
        if prepared.get("content_id") != content_id:
            raise CampaignError("local preparation result content mismatch")
        provider_unavailable = prepared.get("status") == "provider_unavailable"
        if prepared.get("status") != "ready" and not provider_unavailable:
            return dict(prepared), None
        result: dict[str, Any] = {"content_id": content_id}
        terminal = rb._pinned_media_terminal_detail(db_path=self.db_path, release_id=RELEASE_ID, content_id=content_id)
        if provider_unavailable:
            evaluated = evaluation.evaluate_content(
                content_id,
                db_path=self.db_path,
                expected_active_release_id=RELEASE_ID,
            )
            result.update(
                evaluation_id=evaluated.evaluation_id,
                evaluation_created=evaluated.created,
            )
            terminal = rb._pinned_media_terminal_detail(
                db_path=self.db_path,
                release_id=RELEASE_ID,
                content_id=content_id,
            )
            if terminal.state != "terminal_insufficient":
                raise CampaignError(
                    "provider-unavailable evaluation did not reach "
                    f"terminal_insufficient: {terminal.reason}"
                )
            return result, terminal
        if terminal.state == "terminal_failed":
            return {**result, "status": "terminal_failed", "reason": terminal.reason}, None
        if terminal.state not in {"complete", "terminal_insufficient"}:
            if terminal.reason != "evaluation_pending":
                raise CampaignError(
                    f"local media preparation did not reach evaluation gate: {terminal.reason}"
                )
            evaluated = evaluation.evaluate_content(content_id, db_path=self.db_path, expected_active_release_id=RELEASE_ID)
            result.update(evaluation_id=evaluated.evaluation_id, evaluation_created=evaluated.created)
            terminal = rb._pinned_media_terminal_detail(db_path=self.db_path, release_id=RELEASE_ID, content_id=content_id)
        if terminal.state not in {"complete", "terminal_insufficient"}:
            return {**result, "status": terminal.state, "reason": terminal.reason}, None
        return result, terminal

    def _finalize_local_one(
        self, content_id: int, prepared: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Evaluate, fingerprint and clear history state on the caller thread."""

        result, terminal = self._evaluate_prepared_local_one(content_id, prepared)
        if terminal is None:
            return result
        if terminal.state == "complete":
            # Keep relation debt before fingerprinting: a crash can occur after
            # fingerprint commit but before the queue's relation update.
            self.state["relation_pending_ids"] = sorted(set(self.state["relation_pending_ids"]) | {content_id})
            self._save()
            fp = duplicates.run_duplicate_fingerprint_queue(scope_content_ids=[content_id], limit=1, db_path=self.db_path)
            if fp["failed"] or fp["has_more"] or not fp["calibration_ready"]:
                raise CampaignError("current-source fingerprint/duplicate calibration not ready")
            if fp["fingerprinted_content_ids"]:
                self.state["relation_pending_ids"] = sorted(set(self.state["relation_pending_ids"]) - {content_id})
            else:
                self.repair_relations()
        result["backfill_tag_cleared"] = rb._release_history_backfill_tag(db_path=self.db_path, release_id=RELEASE_ID, content_id=content_id)
        result["status"] = terminal.state
        return result

    def _finalize_local_group(
        self, prepared_by_id: Mapping[int, Mapping[str, Any]]
    ) -> dict[int, dict[str, Any]]:
        """Evaluate serially, then fingerprint and repair relations once."""

        results: dict[int, dict[str, Any]] = {}
        terminal_states: dict[int, str] = {}
        for content_id in sorted(prepared_by_id):
            try:
                result, terminal = self._evaluate_prepared_local_one(
                    content_id, prepared_by_id[content_id]
                )
                results[content_id] = result
                if terminal is not None:
                    terminal_states[content_id] = str(terminal.state)
            except Exception as exc:
                results[content_id] = {
                    "content_id": content_id,
                    "status": "failed",
                    "error_code": getattr(exc, "error_code", type(exc).__name__),
                    "error": str(exc)[:500],
                }

        complete_ids = sorted(
            content_id for content_id, state in terminal_states.items()
            if state == "complete"
        )
        relation_ready: set[int] = set()
        if complete_ids:
            self.state["relation_pending_ids"] = sorted(
                set(self.state["relation_pending_ids"]) | set(complete_ids)
            )
            self._save()  # Durable debt before any fingerprint can commit.
            try:
                fp = duplicates.run_duplicate_fingerprint_queue(
                    scope_content_ids=complete_ids,
                    limit=len(complete_ids),
                    db_path=self.db_path,
                )
                if not fp["calibration_ready"]:
                    raise CampaignError("duplicate detector calibration is not ready")
                fresh = {int(cid) for cid in fp["fingerprinted_content_ids"]}
                self.state["relation_pending_ids"] = sorted(
                    set(self.state["relation_pending_ids"]) - fresh
                )
                # A resumed group can contain already-current fingerprints;
                # repair them in one existing incremental relation call.
                self.repair_relations()
                self._save()
                relation_ready = set(complete_ids) - set(
                    self.state["relation_pending_ids"]
                )
                failures = {
                    int(item["content_id"]): str(item.get("error") or "")
                    for item in fp["failures"]
                }
                for content_id in set(complete_ids) - relation_ready:
                    message = failures.get(
                        content_id, "current-source fingerprint did not complete"
                    )
                    results[content_id] = {
                        **results[content_id],
                        "status": "failed",
                        "error_code": "duplicate_fingerprint_incomplete",
                        "error": message[:500],
                    }
            except Exception as exc:
                # Fingerprints may already be durable while relation recovery
                # or the following state save failed.  Restore the full debt
                # before the caller's group checkpoint can persist state.
                self.state["relation_pending_ids"] = sorted(
                    set(self.state["relation_pending_ids"]) | set(complete_ids)
                )
                try:
                    self._save()
                except Exception:
                    pass  # Keep the original relation/fingerprint error below.
                for content_id in complete_ids:
                    results[content_id] = {
                        **results[content_id],
                        "status": "failed",
                        "error_code": getattr(
                            exc, "error_code", type(exc).__name__
                        ),
                        "error": str(exc)[:500],
                    }

        for content_id, terminal_state in terminal_states.items():
            if terminal_state == "complete" and content_id not in relation_ready:
                continue
            try:
                results[content_id]["backfill_tag_cleared"] = (
                    rb._release_history_backfill_tag(
                        db_path=self.db_path,
                        release_id=RELEASE_ID,
                        content_id=content_id,
                    )
                )
                results[content_id]["status"] = terminal_state
            except Exception as exc:
                results[content_id].update(
                    status="failed",
                    error_code=getattr(exc, "error_code", type(exc).__name__),
                    error=str(exc)[:500],
                )
        return results

    def local_one(self, content_id: int) -> dict[str, Any]:
        """Backward-compatible sequential local completion for one content."""

        content = self._content(content_id)
        terminal = rb._pinned_media_terminal_detail(
            db_path=self.db_path,
            release_id=RELEASE_ID,
            content_id=content_id,
        )
        needs_media = (
            terminal.state
            not in {"terminal_failed", "complete", "terminal_insufficient"}
            and terminal.reason != "evaluation_pending"
        )
        model = (
            media.pinned_whisper_model_path(local_files_only=True)
            if needs_media and content["content_type"] == "video"
            else None
        )
        prepared = self._prepare_local_media_one(
            content_id,
            whisper_model_path=model,
            ocr_binary=media.ocr_binary_path() if needs_media else None,
        )
        return self._finalize_local_one(content_id, prepared)

    def usage(self) -> dict[str, Any]:
        with self._read() as connection:
            rows = [dict(row) for row in connection.execute("""
                SELECT operation,SUM(request_attempts) attempts,SUM(billed_requests) billed_requests,
                  ROUND(SUM(amount),6) amount FROM provider_usage WHERE task_id=? GROUP BY operation
            """, (self.task_id,))]
        return {
            "task_id": self.task_id,
            "amount": round(sum(row["amount"] for row in rows), 6),
            "max_amount": self.max_amount,
            "currency": "USD",
            "operations": rows,
        }

    @staticmethod
    def _content_rows(
        connection: sqlite3.Connection, content_ids: Iterable[int]
    ) -> dict[int, sqlite3.Row]:
        requested = sorted({int(content_id) for content_id in content_ids})
        rows: dict[int, sqlite3.Row] = {}
        for offset in range(0, len(requested), 500):
            batch = requested[offset : offset + 500]
            placeholders = ",".join("?" for _ in batch)
            rows.update({
                int(row["id"]): row
                for row in connection.execute(
                    f"SELECT id,platform,source_group,content_type FROM content_items WHERE id IN ({placeholders})",
                    batch,
                )
            })
        if len(rows) != len(requested):
            raise CampaignError("campaign content disappeared before processing")
        return rows

    @staticmethod
    def _stable_metric_gap_ids(
        connection: sqlite3.Connection, content_ids: Iterable[int]
    ) -> set[int]:
        requested = sorted({int(content_id) for content_id in content_ids})
        matches: set[int] = set()
        for offset in range(0, len(requested), 500):
            batch = requested[offset : offset + 500]
            placeholders = ",".join("?" for _ in batch)
            matches.update(int(row[0]) for row in connection.execute(
                f"""
                SELECT content_id FROM fetch_slots
                WHERE content_id IN ({placeholders}) AND stage='metrics'
                  AND status='retryable_failed' AND attempt_count>=3
                  AND last_error_code='invalid_response'
                  AND last_error_message=
                    'TikHub statistics omitted play_count for requested content'
                """,
                batch,
            ))
        return matches

    @staticmethod
    def _available_media_source_ids(
        connection: sqlite3.Connection, content_ids: Iterable[int]
    ) -> set[int]:
        requested = sorted({int(content_id) for content_id in content_ids})
        matches: set[int] = set()
        for offset in range(0, len(requested), 500):
            batch = requested[offset : offset + 500]
            placeholders = ",".join("?" for _ in batch)
            matches.update(int(row[0]) for row in connection.execute(
                f"""
                SELECT DISTINCT content_id FROM evidence_artifacts
                WHERE content_id IN ({placeholders})
                  AND artifact_type='media_source' AND status='available'
                """,
                batch,
            ))
        return matches

    def _cycle_actionable(self, phase: str) -> list[int]:
        candidates = set(self.state["new_ids"]) | set(self.state["existing_pending_ids"])
        results = self.state["results"].setdefault(phase, {})
        if phase == "download":
            content = self.state["results"].setdefault("content", {})
            with self._read() as connection:
                source_ready = self._available_media_source_ids(connection, candidates)
            return [
                cid for cid in sorted(candidates)
                if results.get(str(cid), {}).get("status")
                not in {"downloaded", "evidence_ready", "already_succeeded"}
                and (
                    results.get(str(cid), {}).get("status") in {"failed", "repair_ready"}
                    or (
                        cid in source_ready
                        and (
                            cid in self.state["existing_pending_ids"]
                            or content.get(str(cid), {}).get("status")
                            in {
                                "succeeded",
                                "already_succeeded",
                                "partial",
                                "metrics_deferred_to_daily",
                            }
                        )
                    )
                )
            ]
        with self._read() as connection:
            terminal = rb.media_terminal_state_details(connection, RELEASE_ID, sorted(candidates))
        return [
            cid for cid in sorted(candidates)
            if results.get(str(cid), {}).get("status")
            not in {"complete", "terminal_insufficient"}
            and (
                results.get(str(cid), {}).get("status") == "repair_ready"
                or terminal[cid].state in {"complete", "terminal_insufficient"}
                or terminal[cid].reason in {"frames_pending", "asr_pending", "ocr_pending", "evaluation_pending"}
            )
        ]

    def status(self) -> dict[str, Any]:
        contract = self._contract()
        state = json.loads(self.state_path.read_text()) if self.state_path.exists() else {}
        with self._read() as connection:
            daily = [dict(row) for row in connection.execute("""
                SELECT date(published_at,'+8 hours') day,platform,COUNT(*) contents
                FROM content_items WHERE platform IN ('douyin','xiaohongshu')
                AND published_at>=? AND published_at<=? GROUP BY day,platform ORDER BY day,platform
            """, (rb._iso(self.start), rb._iso(self.end)))]
            quote = {"douyin_detail": 0, "douyin_metrics": 0, "xiaohongshu_detail": 0}
            quote_ids = set(state.get("new_ids", [])) | set(
                state.get("existing_pending_ids", [])
            )
            content_rows = self._content_rows(connection, quote_ids)
            stable_metric_gaps = self._stable_metric_gap_ids(
                connection, content_rows
            )
            for cid, row in content_rows.items():
                if row["source_group"] != storage.HISTORY_BACKFILL_SOURCE_GROUP:
                    continue
                stages = {slot[0] for slot in connection.execute(
                    "SELECT stage FROM fetch_slots WHERE content_id=? AND status='succeeded'", (cid,))}
                if "detail" not in stages:
                    quote[f"{row['platform']}_detail"] += 1
                if (
                    row["platform"] == "douyin"
                    and "metrics" not in stages
                    and cid not in stable_metric_gaps
                ):
                    quote["douyin_metrics"] += 1
            estimate = round((quote["douyin_detail"] + quote["douyin_metrics"]) * providers.TIKHUB_PRICE + quote["xiaohongshu_detail"] * providers.TIKHUB_XHS_PRICE, 6)
        return {"initialized": bool(state), "contract_matches": not state or state.get("contract") == contract,
                "enabled_identities": len(contract["identities"]), "excluded_account_ids": contract["excluded_account_ids"],
                "baseline_contents": len(state.get("baseline_ids", [])), "new_contents": len(state.get("new_ids", [])),
                "discovery_complete": state.get("discovery_complete", False), "relation_pending": len(state.get("relation_pending_ids", [])),
                "purchase_estimate": {"requests": quote, "amount": estimate, "currency": "USD", "comments": 0,
                                      "scope": "history-backfill content debt; excludes discovery and at-most-once source refresh"},
                "daily_counts": daily, "usage": self.usage(), "results": state.get("results", {})}

    def _pipeline_fresh_content_batch(
        self,
        batch: Sequence[int],
        captured: Mapping[int, Mapping[str, Any]],
        download: Any,
    ) -> tuple[dict[int, dict[str, Any]], dict[int, dict[str, Any]]]:
        """Overlap fresh downloads and media preparation; finalize only after both settle."""

        with self._read() as connection:
            source_ready = self._available_media_source_ids(connection, batch)
        downloadable = [
            cid for cid in batch
            if captured[cid].get("status") == "succeeded"
            or (
                cid in source_ready
                and captured[cid].get("status")
                in {"partial", "metrics_deferred_to_daily"}
            )
        ]
        if not downloadable:
            return {}, {}
        with self._read() as connection:
            rows = self._content_rows(connection, downloadable)
        ocr_binary = media.ocr_binary_path()
        whisper_model = (
            media.pinned_whisper_model_path(local_files_only=True)
            if any(row["content_type"] == "video" for row in rows.values())
            else None
        )
        downloaded: dict[int, dict[str, Any]] = {}
        prepared: dict[int, dict[str, Any]] = {}
        preparation_errors: dict[int, Exception] = {}
        with ThreadPoolExecutor(
            max_workers=min(FRESH_DOWNLOAD_WORKERS, len(downloadable))
        ) as download_pool, ThreadPoolExecutor(
            max_workers=min(LOCAL_MEDIA_WORKERS, len(downloadable))
        ) as local_pool:
            downloads = {
                download_pool.submit(download, cid): cid for cid in downloadable
            }
            preparations: dict[int, Any] = {}
            for future in as_completed(downloads):
                cid = downloads[future]
                result = future.result()
                downloaded[cid] = result
                if result.get("status") in {
                    "downloaded", "evidence_ready", "already_succeeded"
                }:
                    preparations[cid] = local_pool.submit(
                        self._prepare_local_media_one,
                        cid,
                        whisper_model_path=whisper_model,
                        ocr_binary=ocr_binary,
                    )
            # All download futures are settled here.  Settle every preparation
            # before evaluation/fingerprint/relation transactions begin.
            for cid, future in preparations.items():
                try:
                    prepared[cid] = future.result()
                except Exception as exc:
                    preparation_errors[cid] = exc

        local_results: dict[int, dict[str, Any]] = {}
        prepared_ids = sorted(prepared)
        for offset in range(0, len(prepared_ids), LOCAL_FINALIZE_BATCH_SIZE):
            group = {
                cid: prepared[cid]
                for cid in prepared_ids[
                    offset : offset + LOCAL_FINALIZE_BATCH_SIZE
                ]
            }
            try:
                local_results.update(self._finalize_local_group(group))
            except Exception as exc:
                local_results.update({
                    cid: {
                        "content_id": cid,
                        "status": "failed",
                        "error_code": getattr(
                            exc, "error_code", type(exc).__name__
                        ),
                        "error": str(exc)[:500],
                    }
                    for cid in group
                })
        local_results.update({
            cid: {
                "content_id": cid,
                "status": "failed",
                "error_code": getattr(exc, "error_code", type(exc).__name__),
                "error": str(exc)[:500],
            }
            for cid, exc in preparation_errors.items()
        })
        return downloaded, local_results

    def run_phase(self, phase: str, *, limit: int = LOCAL_LIMIT, call_override: Any = None, urlopen_fn: Any = None, max_seconds: float = 21600, scope_content_ids: Iterable[int] | None = None, allow_download_refresh: bool = True, pipeline_fresh_local: bool = False) -> dict[str, Any]:
        self._require_window()
        if limit <= 0 or not math.isfinite(max_seconds) or max_seconds <= 0 or (phase == "local" and limit > LOCAL_LIMIT):
            raise CampaignError("positive bounds required; local windows are limited to 500 contents")
        self._deadline = time.monotonic() + max_seconds
        if phase == "discover":
            return self.discover(call_override=call_override)
        if phase not in {"content", "download", "local"}:
            raise CampaignError(f"unsupported phase: {phase}")
        if pipeline_fresh_local and phase != "content":
            raise CampaignError("fresh local pipeline is cycle-content only")
        if not self.state.get("discovery_complete"):
            raise CampaignError("downstream phases require completed discovery")
        self._phase_contract(phase)
        candidates = set(self.state["new_ids"])
        if phase == "content":
            candidates.update(self.state["existing_pending_ids"])
            with self._read() as connection:
                content_rows = self._content_rows(connection, candidates)
            candidates = {
                cid for cid, row in content_rows.items()
                if row["source_group"] == storage.HISTORY_BACKFILL_SOURCE_GROUP
            }
        else:
            candidates.update(self.state["existing_pending_ids"])
        if scope_content_ids is not None:
            requested = {int(content_id) for content_id in scope_content_ids}
            if not requested <= candidates:
                raise CampaignError("phase scope escaped the campaign cohort")
            candidates = requested
        results = self.state["results"].setdefault(phase, {})
        if phase == "content":
            with self._read() as connection:
                stable_metric_gaps = self._stable_metric_gap_ids(
                    connection, candidates
                )
            for content_id in stable_metric_gaps:
                prior = results.get(str(content_id))
                if prior is not None and prior.get("status") == "partial":
                    prior["status"] = "metrics_deferred_to_daily"
                    prior["reason"] = "play_count_omitted_after_three_attempts"
        done = {"succeeded", "already_succeeded", "metrics_deferred_to_daily"} if phase == "content" else {"downloaded", "evidence_ready", "already_succeeded"} if phase == "download" else {"complete", "terminal_insufficient"}
        repair_ready = {
            cid for cid in candidates
            if phase in {"download", "local"}
            and results.get(str(cid), {}).get("status") == "repair_ready"
        }
        local_ready: set[int] = set()
        local_terminal: dict[int, Any] = {}
        if phase == "local":
            with self._read() as connection:
                local_terminal = rb.media_terminal_state_details(
                    connection, RELEASE_ID, sorted(candidates)
                )
            local_ready = {
                cid for cid, detail in local_terminal.items()
                if detail.state in {"complete", "terminal_insufficient"}
                or (
                    detail.state == "pending"
                    and detail.reason in {
                        "frames_pending",
                        "asr_pending",
                        "ocr_pending",
                        "evaluation_pending",
                    }
                )
            }
        source_ready_partial: set[int] = set()
        if phase == "download":
            with self._read() as connection:
                source_ready = self._available_media_source_ids(
                    connection, candidates
                )
            content_results = self.state["results"].get("content", {})
            source_ready_partial = {
                cid for cid in candidates
                if cid in source_ready
                and content_results.get(str(cid), {}).get("status")
                in {"partial", "metrics_deferred_to_daily"}
            }
        selected = [
            cid for cid in sorted(
                candidates,
                key=lambda value: (
                    0 if value in repair_ready
                    else 1 if value in local_ready
                    else 2 if value in source_ready_partial
                    else 3,
                    value,
                ),
            )
            if results.get(str(cid), {}).get("status") not in done
        ][:limit]
        local_whisper_model: Path | None = None
        local_ocr_binary: Path | None = None
        if phase == "local":
            media_pending = {
                cid
                for cid in selected
                if local_terminal[cid].state == "pending"
                and local_terminal[cid].reason
                in {"frames_pending", "asr_pending", "ocr_pending"}
            }
            if media_pending:
                local_ocr_binary = media.ocr_binary_path()
                with self._read() as connection:
                    local_content_rows = self._content_rows(
                        connection, media_pending
                    )
                if any(
                    row["content_type"] == "video"
                    for row in local_content_rows.values()
                ):
                    # Resolve the pinned local snapshot once on the caller
                    # thread; worker threads must never race HF cache setup.
                    local_whisper_model = media.pinned_whisper_model_path(
                        local_files_only=True
                    )
        processed = 0
        processed_content_ids: list[int] = []
        blocked = False

        def guarded_capture(cid: int) -> dict[str, Any]:
            if self._stop_paid.is_set():
                return {"content_id": cid, "status": "skipped_after_block"}
            try:
                result = self.capture_one(cid, call_override=call_override)
            except Exception as exc:
                result = {"content_id": cid, "status": "failed", "error_code": getattr(exc, "error_code", type(exc).__name__), "error": str(exc)[:500]}
            codes = {result.get("error_code")} | {item.get("error_code") for item in result.get("stages", [])}
            if codes & rb.BLOCKING_CODES:
                self._stop_paid.set()
            return result

        def guarded_fresh_download(cid: int) -> dict[str, Any]:
            try:
                # Detail was just captured in this phase, so its media URL
                # should still be fresh.  Keep paid stale-source recovery out
                # of the worker pool; the serial download phase owns it.
                return self.download_one(
                    cid,
                    allow_refresh=False,
                    call_override=call_override,
                    urlopen_fn=urlopen_fn,
                )
            except Exception as exc:
                return {
                    "content_id": cid,
                    "status": "failed",
                    "error_code": getattr(exc, "error_code", type(exc).__name__),
                    "error": str(exc)[:500],
                }

        for offset in range(0, len(selected), CHECKPOINT_SIZE):
            if time.monotonic() >= self._deadline:
                break
            batch = selected[offset:offset + CHECKPOINT_SIZE]
            if phase == "local":
                with ThreadPoolExecutor(
                    max_workers=min(LOCAL_MEDIA_WORKERS, len(batch))
                ) as pool:
                    for group_offset in range(0, len(batch), LOCAL_MEDIA_WORKERS):
                        if time.monotonic() >= self._deadline:
                            break
                        group = batch[
                            group_offset : group_offset + LOCAL_MEDIA_WORKERS
                        ]
                        futures = {
                            cid: pool.submit(
                                self._prepare_local_media_one,
                                cid,
                                whisper_model_path=local_whisper_model,
                                ocr_binary=local_ocr_binary,
                            )
                            for cid in group
                        }
                        prepared: dict[int, dict[str, Any]] = {}
                        preparation_errors: dict[int, Exception] = {}
                        # Settle the complete group before finalization so media
                        # workers cannot write SQLite while evaluation holds a
                        # longer transaction on the caller thread.
                        for cid in group:
                            try:
                                prepared[cid] = futures[cid].result()
                            except Exception as exc:
                                preparation_errors[cid] = exc
                        try:
                            finalized = self._finalize_local_group(prepared)
                        except Exception as exc:
                            finalized = {
                                cid: {
                                    "content_id": cid,
                                    "status": "failed",
                                    "error_code": getattr(
                                        exc, "error_code", type(exc).__name__
                                    ),
                                    "error": str(exc)[:500],
                                }
                                for cid in prepared
                            }
                        for cid in group:
                            if cid in preparation_errors:
                                preparation_error = preparation_errors[cid]
                                result = {
                                    "content_id": cid,
                                    "status": "failed",
                                    "error_code": getattr(
                                        preparation_error,
                                        "error_code",
                                        type(preparation_error).__name__,
                                    ),
                                    "error": str(preparation_error)[:500],
                                }
                            else:
                                result = finalized[cid]
                            results[str(cid)] = result
                            processed += 1
                            processed_content_ids.append(cid)
                            codes = {result.get("error_code")} | {
                                item.get("error_code")
                                for item in result.get("stages", [])
                            }
                            if codes & rb.BLOCKING_CODES:
                                self._stop_paid.set()
                                blocked = True
                        self._save()
                        if blocked:
                            break
                if blocked or time.monotonic() >= self._deadline:
                    break
                continue
            captured: dict[int, dict[str, Any]] = {}
            downloaded: dict[int, dict[str, Any]] = {}
            fresh_local: dict[int, dict[str, Any]] = {}
            if phase == "content":
                with ThreadPoolExecutor(max_workers=CAPTURE_WORKERS) as pool:
                    captured = dict(zip(batch, pool.map(guarded_capture, batch)))
                if pipeline_fresh_local:
                    downloaded, fresh_local = self._pipeline_fresh_content_batch(
                        batch, captured, guarded_fresh_download
                    )
                else:
                    downloadable = [
                        cid for cid in batch
                        if captured[cid]["status"] == "succeeded"
                        and time.monotonic() < self._deadline
                    ]
                    if downloadable:
                        with ThreadPoolExecutor(
                            max_workers=min(FRESH_DOWNLOAD_WORKERS, len(downloadable))
                        ) as pool:
                            downloaded = dict(zip(
                                downloadable,
                                pool.map(guarded_fresh_download, downloadable),
                            ))
            for cid in batch:
                if phase != "content" and time.monotonic() >= self._deadline:
                    break
                try:
                    if phase == "content":
                        result = captured[cid]
                        fresh_download = downloaded.get(cid)
                        if fresh_download is not None:
                            self.state["results"].setdefault("download", {})[
                                str(cid)
                            ] = fresh_download
                            if fresh_download.get("error_code") in rb.BLOCKING_CODES:
                                self._stop_paid.set()
                                blocked = True
                        fresh_local_result = fresh_local.get(cid)
                        if fresh_local_result is not None:
                            self.state["results"].setdefault("local", {})[
                                str(cid)
                            ] = fresh_local_result
                            if fresh_local_result.get("error_code") in rb.BLOCKING_CODES:
                                self._stop_paid.set()
                                blocked = True
                    elif phase == "download":
                        result = self.download_one(
                            cid,
                            allow_refresh=allow_download_refresh,
                            call_override=call_override,
                            urlopen_fn=urlopen_fn,
                        )
                    else:
                        raise AssertionError("local phase must use paired preparation")
                except Exception as exc:
                    result = {"content_id": cid, "status": "failed", "error_code": getattr(exc, "error_code", type(exc).__name__), "error": str(exc)[:500]}
                results[str(cid)] = result
                processed += 1
                processed_content_ids.append(cid)
                codes = {result.get("error_code")} | {item.get("error_code") for item in result.get("stages", [])}
                if codes & rb.BLOCKING_CODES:
                    self._stop_paid.set()
                    blocked = True
                    if phase != "content":
                        break
            self._save()
            if blocked:
                break
        self._save()
        remaining = sum(results.get(str(cid), {}).get("status") not in done for cid in candidates)
        return {"phase": phase, "status": "blocked" if blocked else "partial" if remaining else "succeeded", "processed": processed, "processed_content_ids": processed_content_ids, "remaining": remaining, "usage": self.usage()}

    def run_cycle(self, *, limit: int = CHECKPOINT_SIZE, max_seconds: float = 21600) -> dict[str, Any]:
        """Close cached debt, then capture and fully analyze one bounded batch."""
        self._require_window()
        if not 0 < limit <= CYCLE_LIMIT or not math.isfinite(max_seconds) or max_seconds <= 0:
            raise CampaignError(
                f"cycle requires 1..{CYCLE_LIMIT} contents and positive time"
            )
        anchor = media.MEDIA_ROOT
        while not anchor.exists() and anchor != anchor.parent:
            anchor = anchor.parent
        capacity = shutil.disk_usage(anchor)
        if capacity.free < media_policy.MIN_FREE_BYTES:
            raise CampaignError("media capacity below 10 GiB; no capture or delete")
        if not self.state.get("discovery_complete"):
            raise CampaignError("cycle requires completed discovery")
        deadline = time.monotonic() + max_seconds
        attempted_ids: list[int] = []
        analyzed_ids: set[int] = set()
        blocked_reason: str | None = None
        def drain(phase: str, *, allow_refresh: bool) -> bool:
            nonlocal blocked_reason
            while time.monotonic() < deadline:
                before = self._cycle_actionable(phase)
                if not before:
                    return True
                batch = before[:CHECKPOINT_SIZE]
                result = self.run_phase(
                    phase,
                    limit=len(batch),
                    max_seconds=max(0.001, deadline - time.monotonic()),
                    scope_content_ids=batch,
                    allow_download_refresh=allow_refresh,
                )
                if phase == "local":
                    analyzed_ids.update(
                        cid for cid in result["processed_content_ids"]
                        if self.state["results"]["local"].get(str(cid), {}).get("status")
                        in {"complete", "terminal_insufficient"}
                    )
                after = set(self._cycle_actionable(phase))
                if result["status"] == "blocked":
                    blocked_reason = f"{phase}_phase_blocked"
                    return False
                if not (set(batch) - after):
                    blocked_reason = f"{phase}_made_no_progress"
                    return False
            blocked_reason = f"{phase}_window_expired"
            return False
        if drain("download", allow_refresh=False):
            drain("local", allow_refresh=False)
        if blocked_reason is None and time.monotonic() < deadline:
            content = self.run_phase(
                "content",
                limit=limit,
                max_seconds=max(0.001, deadline - time.monotonic()),
                pipeline_fresh_local=True,
            )
            attempted_ids = [int(cid) for cid in content["processed_content_ids"]]
            analyzed_ids.update(
                cid for cid in attempted_ids
                if self.state["results"].setdefault("local", {}).get(
                    str(cid), {}
                ).get("status") in {"complete", "terminal_insufficient"}
            )
            if content["status"] == "blocked":
                blocked_reason = "content_phase_blocked"
        if blocked_reason is None:
            if drain("download", allow_refresh=True):
                drain("local", allow_refresh=False)
        usage_after = self.usage()
        content_done = {"succeeded", "already_succeeded", "metrics_deferred_to_daily"}
        download_done = {"downloaded", "evidence_ready", "already_succeeded"}
        content_results = self.state["results"].setdefault("content", {})
        download_results = self.state["results"].setdefault("download", {})
        captured_downloaded = sum(
            content_results.get(str(content_id), {}).get("status") in content_done | {"partial"}
            and download_results.get(str(content_id), {}).get("status") in download_done
            for content_id in attempted_ids
        )
        local_results = self.state["results"].setdefault("local", {})
        with self._read() as connection:
            capture_rows = self._content_rows(
                connection,
                set(self.state["new_ids"])
                | set(self.state["existing_pending_ids"]),
            )
        remaining = {
            "capture": sum(
                content_results.get(str(content_id), {}).get("status") not in content_done
                and row["source_group"] == storage.HISTORY_BACKFILL_SOURCE_GROUP
                for content_id, row in capture_rows.items()
            ),
            "download_actionable": len(self._cycle_actionable("download")),
            "analysis": sum(
                local_results.get(str(cid), {}).get("status")
                not in {"complete", "terminal_insufficient"}
                for cid in set(self.state["new_ids"]) | set(self.state["existing_pending_ids"])
            ),
        }
        status = "blocked" if blocked_reason else (
            "succeeded" if not any(remaining.values()) else "partial"
        )
        if status == "partial" and not attempted_ids and not analyzed_ids:
            blocked_reason = "cycle_made_no_progress"
            status = "blocked"
        return {
            "phase": "cycle",
            "status": status,
            "blocked_reason": blocked_reason,
            "attempted": len(attempted_ids),
            "captured_downloaded": captured_downloaded,
            "analyzed": len(analyzed_ids),
            "usage": usage_after,
            "remaining": remaining,
        }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=(
        "status", "migrate-state", "raise-budget", "repair-raw-links", "repair-media-source-links",
        "repair-terminal-downloads", "repair-local-failures", "discover", "content",
        "download", "local", "cycle",
    ))
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--legacy-run-root", type=Path)
    parser.add_argument("--as-of", type=rb._parse_datetime, required=True)
    parser.add_argument("--task-id", default=TASK_ID)
    parser.add_argument("--start", type=rb._parse_datetime, default=START)
    parser.add_argument("--end", type=rb._parse_datetime, default=END)
    parser.add_argument("--max-amount", type=float, default=MAX_AMOUNT)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--window-hours", type=float, default=6)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    try:
        campaign = Campaign(
            args.db,
            args.run_root,
            args.as_of,
            task_id=args.task_id,
            start=args.start,
            end=args.end,
            max_amount=args.max_amount,
        )
        if args.phase == "status":
            result = campaign.status()
        elif args.phase == "migrate-state":
            if not args.apply:
                raise CampaignError("mutating phases require --apply and an owner-approved maintenance window")
            if args.legacy_run_root is None:
                raise CampaignError("migrate-state requires --legacy-run-root")
            with campaign.window(prepare=False):
                result = campaign.migrate_legacy_state(args.legacy_run_root)
        elif args.phase == "raise-budget":
            if not args.apply:
                raise CampaignError(
                    "mutating phases require --apply and an owner-approved maintenance window"
                )
            with campaign.window(prepare=False):
                result = campaign.raise_task_budget()
        elif args.phase == "repair-raw-links":
            if not args.apply:
                raise CampaignError("mutating phases require --apply and an owner-approved maintenance window")
            with campaign.window():
                result = campaign.repair_discovery_raw_links()
        elif args.phase == "repair-media-source-links":
            if not args.apply:
                raise CampaignError("mutating phases require --apply and an owner-approved maintenance window")
            with campaign.window():
                result = campaign.repair_media_source_links()
        elif args.phase == "repair-terminal-downloads":
            if not args.apply:
                raise CampaignError(
                    "mutating phases require --apply and an owner-approved "
                    "maintenance window"
                )
            with campaign.window():
                result = campaign.repair_terminal_downloads()
        elif args.phase == "repair-local-failures":
            if not args.apply:
                raise CampaignError("mutating phases require --apply and an owner-approved maintenance window")
            with campaign.window():
                result = campaign.repair_local_failures()
        else:
            if not args.apply:
                raise CampaignError("mutating phases require --apply and an owner-approved maintenance window")
            with campaign.window():
                if args.phase == "cycle":
                    result = campaign.run_cycle(limit=args.limit if args.limit is not None else CHECKPOINT_SIZE, max_seconds=args.window_hours * 3600)
                else:
                    result = campaign.run_phase(args.phase, limit=args.limit if args.limit is not None else LOCAL_LIMIT, max_seconds=args.window_hours * 3600)
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 0 if result.get("status", "succeeded") in {"succeeded", "already_succeeded"} else 2
    except Exception as exc:
        print(json.dumps({"status": "blocked", "error": f"{type(exc).__name__}: {exc}"}, ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
