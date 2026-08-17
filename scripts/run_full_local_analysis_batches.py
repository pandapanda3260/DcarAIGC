#!/usr/bin/env python3
"""Accumulate local-only media analysis in one isolated Step3 work database.

This controller is deliberately not a publisher.  Its default mode is a
read-only plan.  ``--apply`` creates one O_EXCL clone of a *completed* Step3
database and advances that clone through immutable batch/item receipts.  It
never invokes discovery, range backfill, a scheduler, or a provider API.

The production v3 contract is intentionally narrow: one worker, batches of at
most 25, the frozen 51,749 Step3 universe, 17,147 statically downloadable
items, 34,602 statically deferred items, and an explicit disclosure of the 39
history rows that Step3 could not materialise.  A completion from this script
can therefore never claim that the full history is complete.
"""

# ruff: noqa: E402 -- direct execution bootstraps repository imports first.

from __future__ import annotations

import argparse
import contextlib
import fcntl
import json
import os
import hashlib
import re
import shutil
import sqlite3
import stat
import sys
import tempfile
from collections import Counter
from contextlib import closing
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence

sys.dont_write_bytecode = True
os.environ["HF_HUB_OFFLINE"] = "1"
os.environ["TRANSFORMERS_OFFLINE"] = "1"

if __package__ in {None, ""}:
    repository_root = Path(__file__).resolve().parents[1]
    for candidate in (repository_root, repository_root / "src/dcar_eval"):
        value = str(candidate)
        if value not in sys.path:
            sys.path.insert(0, value)

from scripts import run_local_analysis_canary as local
from v8 import duplicates as duplicates_module
from v8 import evaluation as evaluation_module
from v8 import media as media_module


SCHEMA_VERSION = "full-local-analysis-batches-v3"
FIRST_BATCH_IDS = (15809, 15810, 17182)
BATCH_DOWNLOAD_CAP_BYTES = 512 * 1024 * 1024
MANAGED_SEQUENCES = frozenset(
    {
        "duplicate_fingerprints",
        "evaluation_matches",
        "evaluation_versions",
        "evidence_artifacts",
        "evidence_envelopes",
        "media_processing_slots",
        "review_queue",
        "review_reopen_events",
    }
)
ALLOWED_SOURCE_GROUPS = frozenset({"history-archive", "history-backfill"})
STATIC_DEFER_REASON = "no_allowlisted_frozen_cdn_url"
ORDERING_POLICY = (
    "first_batch_then_step3_stable_images_then_step3_stable_videos_v1"
)
AUDIT_POLICY = {
    "logical_head": "sha256_previous_plus_batch_delta_v1",
    "item_delta": "owned_target_output_network_receipt_v1",
    "logical_database_checkpoint": (
        "logical_database_checkpoint_pilot_every_100_and_final_v1"
    ),
}
RESUME_GUARD_POLICY = "completed_target_and_recursive_output_chain_v1"
RUNTIME_DEFERRED_POLICY = "sha256_previous_plus_batch_delta_v1"
REVIEW_PENDING_POLICY = "sha256_previous_plus_batch_delta_v1"
INSUFFICIENT_EVIDENCE_POLICY = "sha256_previous_plus_batch_delta_v1"
ITEM_TERMINAL_STATUSES = frozenset(
    {
        "succeeded",
        "review_pending",
        "insufficient_evidence",
        "deferred",
    }
)
DEFERRED_ATTEMPT_ANCHOR_SCHEMA = "durable-deferred-attempt-anchor-v1"
DEFERRED_ATTEMPT_ANCHOR_PREFIX = (
    f"{DEFERRED_ATTEMPT_ANCHOR_SCHEMA}:"
)
# Stay below SQLite's historical 999-variable ceiling while avoiding one
# managed-table query per completed item.
TARGET_GUARD_CHUNK_SIZE = 500


class FullLocalAnalysisError(RuntimeError):
    """A global invariant failed; no later item may run."""


class ContentDeferredError(RuntimeError):
    """A controlled media/CDN failure that may defer one item and continue."""


@dataclass(frozen=True)
class HistoryProfile:
    universe_count: int = 51_749
    eligible_count: int = 17_147
    static_deferred_count: int = 34_602
    missing_universe_count: int = 39
    first_batch_ids: tuple[int, ...] = FIRST_BATCH_IDS
    image_batch_size: int = 25
    video_batch_size: int = 3


PRODUCTION_PROFILE = HistoryProfile()


@dataclass(frozen=True)
class BatchPaths:
    source_database: Path
    source_completion: Path
    database: Path
    media_root: Path
    fingerprint_root: Path
    run_root: Path
    batches_root: Path
    items_root: Path
    progress_root: Path
    completions_root: Path
    network_root: Path
    contract: Path
    lock: Path
    local_paths: local.CanaryPaths


@dataclass(frozen=True)
class RuntimeContext:
    contract_sha256: str
    target_row_map: Mapping[int, Sequence[Any]]
    logical_global_heads: Mapping[str, str]
    processing_order: tuple[int, ...]
    batch_ids_by_cursor: Mapping[int, tuple[int, ...]]
    eligible_baseline_by_id: Mapping[int, Mapping[str, Any]]
    discovery_raw_cache: local._DiscoveryRawCache = field(
        default_factory=local._DiscoveryRawCache,
        compare=False,
        repr=False,
    )
    verified_full_closures: set[str] = field(
        default_factory=set, compare=False, repr=False
    )
    resume_guards_by_count: dict[int, Mapping[str, Any]] = field(
        default_factory=dict, compare=False, repr=False
    )
    remaining_heads_by_count: dict[int, str] = field(
        default_factory=dict, compare=False, repr=False
    )
    processing_prefix_heads_by_count: dict[int, str] = field(
        default_factory=dict, compare=False, repr=False
    )
    derived_sequences_by_count: dict[int, Mapping[str, int]] = field(
        default_factory=dict, compare=False, repr=False
    )
    logical_checkpoints_by_count: dict[int, Mapping[str, Any]] = field(
        default_factory=dict, compare=False, repr=False
    )
    expected_output_closures_by_count: dict[int, Mapping[str, Any]] = field(
        default_factory=dict, compare=False, repr=False
    )
    item_ownership_rows_by_ordinal: dict[int, Mapping[str, Any]] = field(
        default_factory=dict, compare=False, repr=False
    )
    status_counts_by_count: dict[int, Mapping[str, int]] = field(
        default_factory=dict, compare=False, repr=False
    )
    validated_histories: set[str] = field(
        default_factory=set, compare=False, repr=False
    )
    managed_sequence_head: dict[str, int] = field(
        default_factory=dict, compare=False, repr=False
    )
    sidecar_readset: dict[str, Any] = field(
        default_factory=dict, compare=False, repr=False
    )
    sidecar_expected_global: dict[str, Any] = field(
        default_factory=dict, compare=False, repr=False
    )


def _processing_batch_map(
    contract: Mapping[str, Any], processing_order: tuple[int, ...]
) -> Mapping[int, tuple[int, ...]]:
    result: dict[int, tuple[int, ...]] = {}
    profile = contract["profile"]
    summaries = contract["source_summaries"]
    for cursor in range(len(processing_order)):
        if cursor == 0:
            size = len(profile["first_batch_ids"])
            batch = processing_order[:size]
        else:
            first_id = processing_order[cursor]
            kind = str(summaries[str(first_id)]["media_kind"])
            size = (
                int(profile["image_batch_size"])
                if kind == "image"
                else int(profile["video_batch_size"])
            )
            batch = tuple(
                content_id
                for content_id in processing_order[cursor : cursor + size]
                if str(summaries[str(content_id)]["media_kind"]) == kind
            )
        if not batch:
            raise FullLocalAnalysisError("processing batch map包含空batch")
        kinds = {
            str(summaries[str(content_id)]["media_kind"])
            for content_id in batch
        }
        if len(kinds) != 1:
            raise FullLocalAnalysisError(
                "processing batch map不得混合media kind"
            )
        result[cursor] = tuple(batch)
    return result


def _eligible_baseline_map(
    contract: Mapping[str, Any],
) -> Mapping[int, Mapping[str, Any]]:
    rows = contract["eligible_target_baseline"]
    result = {int(row["content_id"]): row for row in rows}
    if len(result) != len(rows):
        raise FullLocalAnalysisError("eligible target baseline ID重复")
    return result


def _runtime_context(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    *,
    target_row_map: Mapping[int, Sequence[Any]] | None = None,
) -> RuntimeContext:
    processing_order = tuple(
        int(value) for value in contract["processing_order"]
    )
    runtime = RuntimeContext(
        contract_sha256=local._sha256_file(paths.contract),
        target_row_map=(
            target_row_map
            if target_row_map is not None
            else _target_row_map(contract["source_completion"])
        ),
        logical_global_heads=_logical_global_heads(contract),
        processing_order=processing_order,
        batch_ids_by_cursor=_processing_batch_map(
            contract, processing_order
        ),
        eligible_baseline_by_id=_eligible_baseline_map(contract),
    )
    runtime.remaining_heads_by_count.update(
        _remaining_target_heads(contract, runtime.contract_sha256)
    )
    runtime.processing_prefix_heads_by_count[0] = _json_sha(
        {
            "contract_sha256": runtime.contract_sha256,
            "chain": "processing_prefix",
        }
    )
    runtime.derived_sequences_by_count[0] = {
        str(name): int(value)
        for name, value in contract["sequence_baseline"].items()
    }
    runtime.status_counts_by_count[0] = {
        "succeeded": 0,
        "review_pending": 0,
        "insufficient_evidence": 0,
        "deferred": 0,
    }
    runtime.resume_guards_by_count[0] = _resume_guard_initial(
        contract,
        runtime.contract_sha256,
        remaining_head=runtime.remaining_heads_by_count[0],
    )
    _logical_database_checkpoint(
        contract,
        runtime,
        batch_index=0,
        completed_count=0,
    )
    return runtime


def _validate_profile(profile: HistoryProfile) -> None:
    integer_fields = (
        "universe_count",
        "eligible_count",
        "static_deferred_count",
        "missing_universe_count",
        "image_batch_size",
        "video_batch_size",
    )
    if any(type(getattr(profile, name)) is not int for name in integer_fields):
        raise FullLocalAnalysisError("profile整数类型必须精确")
    if (
        profile.universe_count <= 0
        or profile.eligible_count <= 0
        or profile.static_deferred_count < 0
        or profile.missing_universe_count < 0
        or profile.eligible_count + profile.static_deferred_count
        != profile.universe_count
        or not 1 <= profile.image_batch_size <= 25
        or not 1 <= profile.video_batch_size <= 25
        or not isinstance(profile.first_batch_ids, tuple)
        or not profile.first_batch_ids
        or any(type(value) is not int or value <= 0 for value in profile.first_batch_ids)
        or len(set(profile.first_batch_ids)) != len(profile.first_batch_ids)
    ):
        raise FullLocalAnalysisError("profile数量/batch/first IDs合同无效")


def _profile_value(profile: HistoryProfile) -> Mapping[str, Any]:
    _validate_profile(profile)
    value = asdict(profile)
    value["first_batch_ids"] = list(profile.first_batch_ids)
    return value


def _paths(
    *,
    source_db_path: Path,
    source_completion_path: Path,
    db_path: Path,
    media_root: Path,
    run_root: Path,
) -> BatchPaths:
    base = local._paths(
        source_db_path=source_db_path,
        source_completion_path=source_completion_path,
        db_path=db_path,
        media_root=media_root,
        run_root=run_root,
    )
    return BatchPaths(
        source_database=base.source_database,
        source_completion=base.source_completion,
        database=base.database,
        media_root=base.media_root,
        fingerprint_root=base.fingerprint_root,
        run_root=base.run_root,
        batches_root=base.run_root / "batches",
        items_root=base.run_root / "items",
        progress_root=base.run_root / "progress",
        completions_root=base.run_root / "completions",
        network_root=base.run_root / "network",
        contract=base.contract,
        lock=base.run_root / ".controller.lock",
        local_paths=base,
    )


def _json_sha(value: Any) -> str:
    return local._json_sha256(value)


def _audit_policy_value() -> Mapping[str, str]:
    return dict(AUDIT_POLICY)


def _write_exclusive(path: Path, value: Mapping[str, Any]) -> str:
    """Create/recover one immutable atomic JSON record.

    The audited primitive writes ``.<name>.tmp``, fsyncs it, renames it, then
    fsyncs the parent.  A restart may promote only the exact same candidate;
    any conflicting final/temp pair is fail-closed.
    """

    try:
        return local._write_json(path, value, immutable=True)
    except local.LocalAnalysisCanaryError as exc:
        raise FullLocalAnalysisError(str(exc)) from exc


def _read_json(path: Path, *, label: str) -> Mapping[str, Any]:
    try:
        return local._read_json(path, label=label)
    except local.LocalAnalysisCanaryError as exc:
        raise FullLocalAnalysisError(str(exc)) from exc


def _validate_batch_arguments(
    *, through_batch: int, workers: int, max_new_batches: int = 1
) -> None:
    if type(through_batch) is not int or through_batch <= 0:
        raise FullLocalAnalysisError("through_batch必须是正整数绝对索引")
    if type(workers) is not int or workers != 1:
        raise FullLocalAnalysisError("v1仅允许workers=1")
    if type(max_new_batches) is not int or not 1 <= max_new_batches <= 25:
        raise FullLocalAnalysisError("max_new_batches必须是1..25精确整数")


def _source_contract(paths: BatchPaths) -> Mapping[str, Any]:
    value = _read_json(
        paths.source_completion.parent / "run-contract.json",
        label="Step3 run contract",
    )
    return value


def _missing_universe_evidence(
    step3_contract: Mapping[str, Any], profile: HistoryProfile
) -> Mapping[str, Any]:
    summary = step3_contract.get("initial_plan_summary")
    if profile.missing_universe_count == 0 and not isinstance(summary, Mapping):
        return {
            "history_total": profile.universe_count,
            "missing_media_urls": 0,
            "missing_media_ids_sha256": _json_sha([]),
        }
    if not isinstance(summary, Mapping) or not isinstance(
        summary.get("history"), Mapping
    ):
        raise FullLocalAnalysisError("Step3合同缺少missing-universe披露")
    history = summary["history"]
    expected_total = profile.universe_count + profile.missing_universe_count
    if (
        type(history.get("history_total")) is not int
        or history.get("history_total") != expected_total
        or type(history.get("missing_media_urls")) is not int
        or history.get("missing_media_urls") != profile.missing_universe_count
        or not isinstance(summary.get("missing_media_ids_sha256"), str)
    ):
        raise FullLocalAnalysisError("Step3 missing-universe数量证据漂移")
    return {
        "history_total": expected_total,
        "missing_media_urls": profile.missing_universe_count,
        "missing_media_ids_sha256": summary["missing_media_ids_sha256"],
        "by_source_group": summary.get("by_source_group"),
    }


def _target_row_map(source_evidence: Mapping[str, Any]) -> dict[int, list[Any]]:
    rows = source_evidence.get("contract", {}).get("explicit_target_rows")
    if not isinstance(rows, list):
        raise FullLocalAnalysisError("Step3 evidence缺少冻结target rows")
    result: dict[int, list[Any]] = {}
    for row in rows:
        if not isinstance(row, list) or not row or type(row[0]) is not int:
            raise FullLocalAnalysisError("Step3 target row形状漂移")
        content_id = int(row[0])
        if content_id in result:
            raise FullLocalAnalysisError("Step3 target row包含重复content ID")
        result[content_id] = row
    return result


def _source_snapshot(
    connection: sqlite3.Connection,
    content_id: int,
    *,
    source_evidence: Mapping[str, Any],
    row_map: Mapping[int, Sequence[Any]],
    discovery_raw_cache: local._DiscoveryRawCache | None = None,
) -> Mapping[str, Any]:
    step3 = source_evidence["contract"]
    try:
        row = row_map[content_id]
    except KeyError as exc:
        raise FullLocalAnalysisError(
            f"Step3 target row缺失：{content_id}"
        ) from exc
    try:
        return local._source_snapshot(
            connection,
            content_id,
            step3_media_root=Path(str(step3["media_root"])),
            step3_derived_raw_root=Path(str(step3["derived_raw_root"])),
            target_contract_row=row,
            discovery_raw_cache=discovery_raw_cache,
        )
    except local.LocalAnalysisCanaryError:
        raise


def _classify_universe(
    connection: sqlite3.Connection,
    target_ids: Sequence[int],
    *,
    source_evidence: Mapping[str, Any],
    row_map: Mapping[int, Sequence[Any]] | None = None,
    discovery_raw_cache: local._DiscoveryRawCache | None = None,
) -> tuple[list[int], list[Mapping[str, Any]], Mapping[int, Mapping[str, Any]]]:
    """Validate every frozen source and split only the audited CDN denial."""

    rows = row_map if row_map is not None else _target_row_map(source_evidence)
    cache = discovery_raw_cache or local._DiscoveryRawCache()
    eligible: list[int] = []
    deferred: list[Mapping[str, Any]] = []
    source_summaries: dict[int, Mapping[str, Any]] = {}
    for content_id in target_ids:
        try:
            source = _source_snapshot(
                connection,
                content_id,
                source_evidence=source_evidence,
                row_map=rows,
                discovery_raw_cache=cache,
            )
        except local.LocalAnalysisCanaryError as exc:
            message = str(exc)
            if message == "media_source没有允许直连的冻结CDN URL":
                reason = STATIC_DEFER_REASON
            elif message.startswith("媒体URL必须是HTTPS:443："):
                reason = "non_https_media_url"
            elif message.startswith("视频source实际是音频占位文件："):
                reason = "audio_placeholder"
            else:
                raise FullLocalAnalysisError(
                    f"content {content_id} source验证阻断：{exc}"
                ) from exc
            deferred.append(
                {"content_id": content_id, "reason": reason}
            )
            continue
        eligible.append(content_id)
        source_summaries[content_id] = {
            "content_id": content_id,
            "source_sha256": source["artifact_body"]["source_sha256"],
            "raw_response_body_sha256": source["raw_response_body_sha256"],
            "download_urls_sha256": source["download_urls_sha256"],
            "media_kind": source["artifact_body"]["media_kind"],
            "image_groups_sha256": (
                source["image_groups_sha256"]
                if source["artifact_body"]["media_kind"] == "image"
                else None
            ),
        }
    return eligible, deferred, source_summaries


def _processing_order(
    eligible_ids: Sequence[int],
    profile: HistoryProfile,
    source_summaries: Mapping[int, Mapping[str, Any]],
) -> list[int]:
    eligible = set(eligible_ids)
    if any(content_id not in eligible for content_id in profile.first_batch_ids):
        raise FullLocalAnalysisError("冻结首批IDs未全部命中static eligible")
    anchors = set(profile.first_batch_ids)
    images = [
        content_id
        for content_id in eligible_ids
        if content_id not in anchors
        and source_summaries[content_id]["media_kind"] == "image"
    ]
    videos = [
        content_id
        for content_id in eligible_ids
        if content_id not in anchors
        and source_summaries[content_id]["media_kind"] == "video"
    ]
    if len(images) + len(videos) + len(anchors) != len(eligible_ids):
        raise FullLocalAnalysisError("eligible media kind集合不精确")
    return [*profile.first_batch_ids, *images, *videos]


def _source_groups_snapshot(
    connection: sqlite3.Connection, target_ids: Sequence[int]
) -> Mapping[str, Any]:
    target_set = set(target_ids)
    rows = [
        [int(row["id"]), str(row["source_group"] or "")]
        for row in connection.execute(
            "SELECT id,source_group FROM content_items ORDER BY id"
        )
        if int(row["id"]) in target_set
    ]
    if len(rows) != len(target_ids) or any(
        row[1] not in ALLOWED_SOURCE_GROUPS for row in rows
    ):
        raise FullLocalAnalysisError("history archive/backfill标签集合漂移")
    counts: dict[str, int] = {value: 0 for value in sorted(ALLOWED_SOURCE_GROUPS)}
    for _, source_group in rows:
        counts[str(source_group)] += 1
    return {
        "policy": "preserve_exact",
        "counts": counts,
        "rows": len(rows),
        "rows_sha256": _json_sha(rows),
    }


def _provider_snapshot(connection: sqlite3.Connection) -> Mapping[str, Any]:
    result: dict[str, Any] = {}
    for table in ("provider_usage", "provider_budget_batches"):
        result[table] = local._digest_query(
            connection, f'SELECT * FROM "{table}" ORDER BY rowid'
        )
    return result


def _schema_snapshot(connection: sqlite3.Connection) -> Mapping[str, Any]:
    return {
        "objects": local._digest_query(
            connection,
            """SELECT type,name,tbl_name,sql FROM sqlite_master
               ORDER BY type,name,tbl_name""",
        ),
        "schema_version": int(
            connection.execute("PRAGMA schema_version").fetchone()[0]
        ),
        "user_version": int(
            connection.execute("PRAGMA user_version").fetchone()[0]
        ),
        "application_id": int(
            connection.execute("PRAGMA application_id").fetchone()[0]
        ),
    }


def _sequence_snapshot(connection: sqlite3.Connection) -> Mapping[str, int]:
    return local._sqlite_sequence_snapshot(connection)


def _protected_snapshot(
    connection: sqlite3.Connection, target_ids: Sequence[int]
) -> Mapping[str, Any]:
    """Scalable equivalent of local._protected_snapshot.

    SQLite builds in the field commonly cap bound variables at 32,766.  The
    production universe is larger, so this version streams each table once and
    applies the target membership test in Python.  It is used only at startup
    and batch checkpoints, never per item.
    """

    target_set = set(target_ids)

    def digest_cursor(
        cursor: sqlite3.Cursor,
        include: Callable[[sqlite3.Row], bool] | None = None,
    ) -> Mapping[str, Any]:
        columns = [str(value[0]) for value in cursor.description or ()]
        digest = hashlib.sha256()
        count = 0
        for row in cursor:
            if include is not None and not include(row):
                continue
            digest.update(
                local._canonical_bytes(
                    {
                        column: local._json_value(row[index])
                        for index, column in enumerate(columns)
                    }
                )
            )
            count += 1
        return {"rows": count, "sha256": digest.hexdigest()}

    tables = [
        str(row["name"])
        for row in connection.execute(
            """SELECT name FROM sqlite_master
               WHERE type='table' AND name NOT LIKE 'sqlite_%'
               ORDER BY name"""
        )
    ]
    result: dict[str, Any] = {}
    for table in tables:
        quoted = local._quoted(table)
        if table == "content_items":
            columns = local._table_columns(connection, table)
            stable = [
                column
                for column in columns
                if column not in local.CONTENT_MUTABLE_COLUMNS
            ]
            result["content_items_stable"] = digest_cursor(
                connection.execute(
                    f"SELECT {','.join(local._quoted(value) for value in stable)} "
                    f"FROM {quoted} ORDER BY rowid"
                )
            )
            result["content_items_non_target"] = digest_cursor(
                connection.execute(f"SELECT * FROM {quoted} ORDER BY rowid"),
                lambda row: int(row["id"]) not in target_set,
            )
            continue
        if table in local.MANAGED_TARGET_TABLES:
            columns = local._table_columns(connection, table)
            if "content_id" in columns:
                result[f"{table}_non_target"] = digest_cursor(
                    connection.execute(f"SELECT * FROM {quoted} ORDER BY rowid"),
                    lambda row: int(row["content_id"]) not in target_set,
                )
            elif table == "evaluation_matches":
                result["evaluation_matches_non_target"] = digest_cursor(
                    connection.execute(
                        """SELECT m.*,v.content_id AS __target_content_id
                           FROM evaluation_matches m
                           JOIN evaluation_versions v ON v.id=m.evaluation_id
                           ORDER BY m.rowid"""
                    ),
                    lambda row: int(row["__target_content_id"]) not in target_set,
                )
            continue
        result[table] = digest_cursor(
            connection.execute(f"SELECT * FROM {quoted} ORDER BY rowid")
        )
    return result


def _assert_sequence_boundary(
    before: Mapping[str, int], after: Mapping[str, int]
) -> None:
    for name in set(before) | set(after):
        old = int(before.get(name, 0))
        new = int(after.get(name, 0))
        if name in MANAGED_SEQUENCES:
            if new < old:
                raise FullLocalAnalysisError(
                    f"managed sqlite_sequence发生回退：{name}"
                )
        elif new != old:
            raise FullLocalAnalysisError(
                f"protected sqlite_sequence发生漂移：{name}"
            )


def _validate_exact_sequence_mapping(value: Any, *, label: str) -> None:
    if (
        not isinstance(value, Mapping)
        or any(
            type(name) is not str
            or not name
            or type(sequence) is not int
            or sequence < 0
            for name, sequence in value.items()
        )
    ):
        raise FullLocalAnalysisError(f"{label} sequence精确类型漂移")


def _output_inventory(paths: BatchPaths) -> Mapping[str, Any]:
    return {
        "media": _strict_inventory(paths.media_root),
        "fingerprints": _strict_inventory(paths.fingerprint_root),
    }


def _output_ownership(paths: BatchPaths) -> Mapping[str, Any]:
    """Hash only top-level ownership metadata, never output file contents."""

    result: dict[str, Any] = {}
    for label, root, expected_directory in (
        ("media", paths.media_root, True),
        ("fingerprints", paths.fingerprint_root, False),
    ):
        try:
            local._private_directory(root, label="输出ownership根")
        except local.LocalAnalysisCanaryError as exc:
            raise FullLocalAnalysisError(str(exc)) from exc
        rows: list[Mapping[str, Any]] = []
        for path in sorted(root.iterdir(), key=lambda value: value.name):
            if expected_directory:
                try:
                    metadata = local._private_directory(
                        path, label="media ownership目录"
                    )
                except local.LocalAnalysisCanaryError as exc:
                    raise FullLocalAnalysisError(str(exc)) from exc
                rows.append(
                    {
                        "name": path.name,
                        "kind": "directory",
                        "device": metadata.st_dev,
                        "inode": metadata.st_ino,
                        "mode": stat.S_IMODE(metadata.st_mode),
                    }
                )
            else:
                try:
                    metadata = local._private_file(
                        path, label="fingerprint ownership文件"
                    )
                except local.LocalAnalysisCanaryError as exc:
                    raise FullLocalAnalysisError(str(exc)) from exc
                rows.append(
                    {
                        "name": path.name,
                        "kind": "file",
                        "device": metadata.st_dev,
                        "inode": metadata.st_ino,
                        "nlink": metadata.st_nlink,
                        "mode": stat.S_IMODE(metadata.st_mode),
                        "byte_size": metadata.st_size,
                    }
                )
        result[label] = {
            "entries": len(rows),
            "rows_sha256": _json_sha(rows),
            "rows": rows,
        }
    return result


def _database_closure(path: Path, *, full_content: bool) -> Mapping[str, Any]:
    binding = _database_binding(path)
    return {
        "coverage": (
            "full_content_checkpoint" if full_content else "owned_delta"
        ),
        "binding": binding,
        "content_sha256": local._sha256_file(path) if full_content else None,
    }


def _output_closure(
    paths: BatchPaths, *, full_content: bool
) -> Mapping[str, Any]:
    del full_content
    return {
        "coverage": "owned_delta",
        "ownership": _output_ownership(paths),
        "content_inventory": None,
    }


def _validate_checkpoint_closure_shape(
    *, database: Any, outputs: Any
) -> str:
    if not isinstance(database, Mapping) or not isinstance(outputs, Mapping):
        raise FullLocalAnalysisError("checkpoint closure必须是object")
    _validate_logical_database_checkpoint(database)
    output_coverage = outputs.get("coverage")
    ownership = outputs.get("ownership")
    inventory = outputs.get("content_inventory")
    if (
        set(outputs) != {"coverage", "ownership", "content_inventory"}
        or output_coverage != "owned_delta"
        or not isinstance(ownership, Mapping)
        or inventory is not None
    ):
        raise FullLocalAnalysisError("output checkpoint closure形状漂移")
    if set(ownership) != {"media", "fingerprints"} or any(
        not isinstance(value, Mapping)
        or set(value) != {"entries", "rows_sha256", "rows"}
        or type(value.get("entries")) is not int
        or value["entries"] < 0
        or not isinstance(value.get("rows_sha256"), str)
        or not isinstance(value.get("rows"), list)
        or value["entries"] != len(value["rows"])
        or value.get("rows_sha256") != _json_sha(value.get("rows"))
        for value in ownership.values()
    ):
        raise FullLocalAnalysisError("output ownership closure形状/hash漂移")
    for label, rows in (
        ("media", ownership["media"]["rows"]),
        ("fingerprints", ownership["fingerprints"]["rows"]),
    ):
        expected_keys = (
            {"name", "kind", "device", "inode", "mode"}
            if label == "media"
            else {
                "name",
                "kind",
                "device",
                "inode",
                "nlink",
                "mode",
                "byte_size",
            }
        )
        expected_kind = "directory" if label == "media" else "file"
        if any(
            not isinstance(row, Mapping)
            or set(row) != expected_keys
            or type(row.get("name")) is not str
            or not row["name"]
            or row.get("kind") != expected_kind
            or any(
                type(row.get(name)) is not int or row[name] < 0
                for name in expected_keys - {"name", "kind"}
            )
            for row in rows
        ):
            raise FullLocalAnalysisError(
                "output ownership rows精确类型漂移"
            )
    return str(output_coverage)


def _validate_checkpoint_closure_current(
    paths: BatchPaths,
    *,
    database: Mapping[str, Any],
    outputs: Mapping[str, Any],
    verify_full_content: bool,
    runtime: RuntimeContext | None = None,
) -> None:
    _validate_checkpoint_closure_shape(
        database=database, outputs=outputs
    )
    del verify_full_content, runtime

    ownership = outputs["ownership"]
    if local._canonical_bytes(ownership) != local._canonical_bytes(
        _output_ownership(paths)
    ):
        raise FullLocalAnalysisError("output top-level ownership漂移")


def _strict_inventory(root: Path) -> Mapping[str, Any]:
    try:
        local._private_directory(root, label="输出根")
    except local.LocalAnalysisCanaryError as exc:
        raise FullLocalAnalysisError(str(exc)) from exc
    inventory = local._inventory(root)
    for directory in sorted(
        (path for path in root.rglob("*") if path.is_dir()), reverse=True
    ):
        local._private_directory(directory, label="输出子目录")
        if not any(directory.iterdir()):
            raise FullLocalAnalysisError(f"输出根包含空目录：{directory}")
    return inventory


def _directory_binding(path: Path) -> Mapping[str, Any]:
    try:
        metadata = local._private_directory(path, label="冻结目录")
    except local.LocalAnalysisCanaryError as exc:
        raise FullLocalAnalysisError(str(exc)) from exc
    return {
        "path": str(path),
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "nlink": metadata.st_nlink,
        "mode": stat.S_IMODE(metadata.st_mode),
    }


def _root_bindings(paths: BatchPaths) -> Mapping[str, Mapping[str, Any]]:
    return {
        "media": _directory_binding(paths.media_root),
        "fingerprints": _directory_binding(paths.fingerprint_root),
        "run": _directory_binding(paths.run_root),
        "batches": _directory_binding(paths.batches_root),
        "items": _directory_binding(paths.items_root),
        "progress": _directory_binding(paths.progress_root),
        "completions": _directory_binding(paths.completions_root),
        "network": _directory_binding(paths.network_root),
    }


def _validate_root_bindings(
    paths: BatchPaths, expected: Mapping[str, Any]
) -> None:
    if not isinstance(expected, Mapping) or set(expected) != {
        "media",
        "fingerprints",
        "run",
        "batches",
        "items",
        "progress",
        "completions",
        "network",
    }:
        raise FullLocalAnalysisError("root bindings合同缺失")
    current = _root_bindings(paths)
    for name, frozen in expected.items():
        if not isinstance(frozen, Mapping) or set(frozen) != {
            "path",
            "device",
            "inode",
            "nlink",
            "mode",
        }:
            raise FullLocalAnalysisError(f"root binding形状漂移：{name}")
        exact_keys = ("path", "device", "inode", "mode")
        if any(current[name][key] != frozen[key] for key in exact_keys):
            raise FullLocalAnalysisError(f"root path/device/inode/mode漂移：{name}")
        # APFS directory nlink changes with ordinary entry creation.  Freeze it
        # as audit evidence, but use dev+inode+mode for replacement detection.
        if type(frozen["nlink"]) is not int or frozen["nlink"] <= 0:
            raise FullLocalAnalysisError(f"root nlink evidence无效：{name}")


def _database_binding(path: Path) -> Mapping[str, Any]:
    metadata = local._private_file(path, label="work database")
    return {
        "path": str(path),
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "nlink": metadata.st_nlink,
        "byte_size": metadata.st_size,
    }


def _item_output_inventory(
    paths: BatchPaths, source: Mapping[str, Any]
) -> Mapping[str, Any]:
    link_id = str(source["content"]["link_id"])
    media_path = paths.media_root / link_id
    fingerprint_path = paths.fingerprint_root / f"{link_id}.json"
    media = (
        _strict_inventory(media_path)
        if media_path.exists()
        else {"files": 0, "rows_sha256": _json_sha([]), "rows": []}
    )
    if fingerprint_path.exists():
        metadata = local._private_file(
            fingerprint_path, label="item fingerprint output"
        )
        fingerprints = {
            "files": 1,
            "rows": [
                {
                    "path": fingerprint_path.name,
                    "byte_size": metadata.st_size,
                    "sha256": local._sha256_file(fingerprint_path),
                }
            ],
        }
        fingerprints["rows_sha256"] = _json_sha(fingerprints["rows"])
    else:
        fingerprints = {"files": 0, "rows_sha256": _json_sha([]), "rows": []}
    return {"media": media, "fingerprints": fingerprints}


def _validate_resume_guard_value(
    value: Any, *, completed_count: int | None = None
) -> None:
    if (
        not isinstance(value, Mapping)
        or set(value)
        != {
            "policy",
            "completed_count",
            "target_head_sha256",
            "remaining_target_head_sha256",
            "output_head_sha256",
            "output_hashed_files",
            "output_hashed_bytes",
        }
        or value.get("policy") != RESUME_GUARD_POLICY
        or type(value.get("completed_count")) is not int
        or value["completed_count"] < 0
        or (
            completed_count is not None
            and value["completed_count"] != completed_count
        )
        or type(value.get("target_head_sha256")) is not str
        or re.fullmatch(r"[0-9a-f]{64}", value["target_head_sha256"])
        is None
        or type(value.get("remaining_target_head_sha256")) is not str
        or re.fullmatch(
            r"[0-9a-f]{64}", value["remaining_target_head_sha256"]
        )
        is None
        or type(value.get("output_head_sha256")) is not str
        or re.fullmatch(r"[0-9a-f]{64}", value["output_head_sha256"])
        is None
        or type(value.get("output_hashed_files")) is not int
        or value["output_hashed_files"] < 0
        or type(value.get("output_hashed_bytes")) is not int
        or value["output_hashed_bytes"] < 0
    ):
        raise FullLocalAnalysisError("resume guard字段/精确类型漂移")


def _remaining_target_heads(
    contract: Mapping[str, Any], contract_sha256: str
) -> Mapping[int, str]:
    leaves = contract["eligible_target_baseline"]
    head = _json_sha(
        {
            "policy": RESUME_GUARD_POLICY,
            "contract_sha256": contract_sha256,
            "chain": "remaining_target_end",
        }
    )
    result = {len(leaves): head}
    for index in range(len(leaves) - 1, -1, -1):
        head = _json_sha(
            {
                "policy": RESUME_GUARD_POLICY,
                "ordinal": index + 1,
                "leaf": leaves[index],
                "next": head,
            }
        )
        result[index] = head
    return result


def _resume_guard_initial(
    contract: Mapping[str, Any],
    contract_sha256: str,
    *,
    remaining_head: str | None = None,
) -> Mapping[str, Any]:
    return {
        "policy": RESUME_GUARD_POLICY,
        "completed_count": 0,
        "target_head_sha256": _json_sha(
            {"contract_sha256": contract_sha256, "chain": "targets"}
        ),
        "remaining_target_head_sha256": (
            remaining_head
            if remaining_head is not None
            else _remaining_target_heads(contract, contract_sha256)[0]
        ),
        "output_head_sha256": _json_sha(
            {"contract_sha256": contract_sha256, "chain": "outputs"}
        ),
        "output_hashed_files": 0,
        "output_hashed_bytes": 0,
    }


def _completed_target_projection(
    connection: sqlite3.Connection, content_ids: Sequence[int]
) -> tuple[
    Mapping[int, Mapping[str, Sequence[Mapping[str, Any]]]],
    Mapping[int, Any],
    Mapping[int, str],
]:
    rows_by_id: dict[int, dict[str, list[Mapping[str, Any]]]] = {
        int(content_id): {
            table: [] for table in sorted(local.MANAGED_TARGET_TABLES)
        }
        for content_id in content_ids
    }
    directions: dict[int, Any] = {}
    link_ids: dict[int, str] = {}
    for start in range(0, len(content_ids), TARGET_GUARD_CHUNK_SIZE):
        chunk = [
            int(value)
            for value in content_ids[start : start + TARGET_GUARD_CHUNK_SIZE]
        ]
        if not chunk:
            continue
        combined = local._target_rows(connection, chunk)
        evaluation_owner = {
            int(row["id"]): int(row["content_id"])
            for row in combined.get("evaluation_versions", [])
        }
        for table, rows in combined.items():
            for row in rows:
                owner = (
                    int(row["content_id"])
                    if "content_id" in row
                    else evaluation_owner.get(int(row["evaluation_id"]))
                )
                if owner not in rows_by_id:
                    raise FullLocalAnalysisError(
                        f"resume guard managed row owner漂移：{table}"
                    )
                rows_by_id[owner][table].append(row)
        placeholders = ",".join("?" for _ in chunk)
        content_rows = connection.execute(
            "SELECT id,link_id,evaluation_content_direction FROM content_items "
            f"WHERE id IN ({placeholders}) ORDER BY id",
            chunk,
        ).fetchall()
        for row in content_rows:
            content_id = int(row["id"])
            directions[content_id] = local._json_value(
                row["evaluation_content_direction"]
            )
            link_ids[content_id] = str(row["link_id"])
    expected_ids = set(int(value) for value in content_ids)
    if set(directions) != expected_ids or set(link_ids) != expected_ids:
        raise FullLocalAnalysisError("resume guard completed content集合漂移")
    return rows_by_id, directions, link_ids


def _eligible_target_leaves(
    connection: sqlite3.Connection, content_ids: Sequence[int]
) -> list[Mapping[str, Any]]:
    rows_by_id, directions, link_ids = _completed_target_projection(
        connection, content_ids
    )
    return [
        {
            "ordinal": ordinal,
            "content_id": int(content_id),
            "target_rows_sha256": _json_sha(rows_by_id[int(content_id)]),
            "content_direction": directions[int(content_id)],
            "link_id": link_ids[int(content_id)],
        }
        for ordinal, content_id in enumerate(content_ids, 1)
    ]


def _validate_eligible_baseline_ids(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    *,
    content_ids: Sequence[int],
    projection: tuple[
        Mapping[int, Mapping[str, Sequence[Mapping[str, Any]]]],
        Mapping[int, Any],
        Mapping[int, str],
    ]
    | None = None,
) -> None:
    requested_ids = [int(value) for value in content_ids]
    expected_by_id = {
        int(row["content_id"]): row
        for row in contract["eligible_target_baseline"]
    }
    if len(set(requested_ids)) != len(requested_ids) or any(
        content_id not in expected_by_id for content_id in requested_ids
    ):
        raise FullLocalAnalysisError("eligible baseline content集合漂移")
    if projection is None:
        if requested_ids:
            with closing(
                local._immutable_connection(paths.database)
            ) as connection:
                rows_by_id, directions, link_ids = _completed_target_projection(
                    connection, requested_ids
                )
        else:
            rows_by_id, directions, link_ids = {}, {}, {}
    else:
        rows_by_id, directions, link_ids = projection
    current = [
        {
            "ordinal": int(expected_by_id[content_id]["ordinal"]),
            "content_id": content_id,
            "target_rows_sha256": _json_sha(rows_by_id[content_id]),
            "content_direction": directions[content_id],
            "link_id": link_ids[content_id],
        }
        for content_id in requested_ids
    ]
    expected = [expected_by_id[content_id] for content_id in requested_ids]
    if local._canonical_bytes(current) != local._canonical_bytes(expected):
        raise FullLocalAnalysisError(
            "eligible target偏离冻结initial baseline"
        )


def _validate_remaining_eligible_baseline(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    *,
    completed_count: int,
    projection: tuple[
        Mapping[int, Mapping[str, Sequence[Mapping[str, Any]]]],
        Mapping[int, Any],
        Mapping[int, str],
    ]
    | None = None,
) -> None:
    processing_order = contract["processing_order"]
    if completed_count < 0 or completed_count > len(processing_order):
        raise FullLocalAnalysisError("remaining eligible baseline ordinal漂移")
    _validate_eligible_baseline_ids(
        paths,
        contract,
        content_ids=processing_order[completed_count:],
        projection=projection,
    )


def _extend_resume_guards(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    receipts: Sequence[Mapping[str, Any]],
    runtime: RuntimeContext,
    *,
    projection: tuple[
        Mapping[int, Mapping[str, Sequence[Mapping[str, Any]]]],
        Mapping[int, Any],
        Mapping[int, str],
    ]
    | None = None,
) -> None:
    if not runtime.resume_guards_by_count:
        runtime.resume_guards_by_count[0] = _resume_guard_initial(
            contract,
            runtime.contract_sha256,
            remaining_head=runtime.remaining_heads_by_count[0],
        )
    completed = max(runtime.resume_guards_by_count)
    if completed > len(receipts):
        raise FullLocalAnalysisError("resume guard内存head超前")
    suffix = receipts[completed:]
    if not suffix:
        return
    _extend_resume_guard_suffix(
        paths,
        contract,
        suffix,
        start_ordinal=completed + 1,
        runtime=runtime,
        projection=projection,
    )


def _extend_resume_guard_suffix(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    receipts: Sequence[Mapping[str, Any]],
    *,
    start_ordinal: int,
    runtime: RuntimeContext,
    projection: tuple[
        Mapping[int, Mapping[str, Sequence[Mapping[str, Any]]]],
        Mapping[int, Any],
        Mapping[int, str],
    ]
    | None = None,
) -> None:
    if not receipts:
        return
    completed = max(runtime.resume_guards_by_count)
    if start_ordinal != completed + 1:
        raise FullLocalAnalysisError("resume guard suffix起点漂移")
    content_ids = [int(receipt["content_id"]) for receipt in receipts]
    if projection is None:
        with closing(local._immutable_connection(paths.database)) as connection:
            rows_by_id, directions, link_ids = _completed_target_projection(
                connection, content_ids
            )
    else:
        rows_by_id, directions, link_ids = projection
    state = dict(runtime.resume_guards_by_count[completed])
    sequence_state = dict(runtime.derived_sequences_by_count[completed])
    processing_head = runtime.processing_prefix_heads_by_count[completed]
    status_counts = dict(runtime.status_counts_by_count[completed])
    for ordinal, receipt in enumerate(receipts, start_ordinal):
        content_id = int(receipt["content_id"])
        current_rows = rows_by_id[content_id]
        current_direction = directions[content_id]
        current_outputs = _item_output_inventory(
            paths, {"content": {"link_id": link_ids[content_id]}}
        )
        after = receipt["after"]
        if (
            _json_sha(current_rows) != after["target_rows_sha256"]
            or _target_sequence_projection(current_rows)
            != after["target_sequences"]
            or local._canonical_bytes(current_direction)
            != local._canonical_bytes(after["content_direction"])
            or local._canonical_bytes(current_outputs)
            != local._canonical_bytes(after["outputs"])
        ):
            raise FullLocalAnalysisError(
                f"resume guard item {ordinal} 当前target/output漂移"
            )
        target_leaf = {
            "ordinal": ordinal,
            "content_id": content_id,
            "status": receipt["status"],
            "target_rows_sha256": _json_sha(current_rows),
            "target_sequences": _target_sequence_projection(current_rows),
            "content_direction": current_direction,
        }
        output_leaf = {
            "ordinal": ordinal,
            "content_id": content_id,
            "outputs": current_outputs,
        }
        output_rows = [
            row
            for label in ("media", "fingerprints")
            for row in current_outputs[label]["rows"]
        ]
        state = {
            "policy": RESUME_GUARD_POLICY,
            "completed_count": ordinal,
            "target_head_sha256": _json_sha(
                {
                    "previous": state["target_head_sha256"],
                    "leaf": target_leaf,
                }
            ),
            "remaining_target_head_sha256": runtime.remaining_heads_by_count[
                ordinal
            ],
            "output_head_sha256": _json_sha(
                {
                    "previous": state["output_head_sha256"],
                    "leaf": output_leaf,
                }
            ),
            "output_hashed_files": state["output_hashed_files"]
            + len(output_rows),
            "output_hashed_bytes": state["output_hashed_bytes"]
            + sum(int(row["byte_size"]) for row in output_rows),
        }
        _validate_resume_guard_value(state, completed_count=ordinal)
        runtime.resume_guards_by_count[ordinal] = state
        processing_head = _json_sha(
            {
                "previous": processing_head,
                "ordinal": ordinal,
                "content_id": content_id,
            }
        )
        runtime.processing_prefix_heads_by_count[ordinal] = processing_head
        for table, rows in current_rows.items():
            identifiers = [
                int(row["id"]) for row in rows if "id" in row
            ]
            if table in MANAGED_SEQUENCES and identifiers:
                sequence_state[table] = max(
                    int(sequence_state.get(table, 0)), max(identifiers)
                )
        runtime.derived_sequences_by_count[ordinal] = dict(sequence_state)
        status = str(receipt["status"])
        if status not in status_counts:
            raise FullLocalAnalysisError("receipt status无法累计completion prefix")
        status_counts[status] = int(status_counts[status]) + 1
        runtime.status_counts_by_count[ordinal] = dict(status_counts)


def _validate_resume_guard_history(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    receipts: Sequence[Mapping[str, Any]],
    batch_receipts: Sequence[Mapping[str, Any]],
    runtime: RuntimeContext,
    *,
    completions: Sequence[Mapping[str, Any]] | None = None,
    projection: tuple[
        Mapping[int, Mapping[str, Sequence[Mapping[str, Any]]]],
        Mapping[int, Any],
        Mapping[int, str],
    ]
    | None = None,
) -> None:
    all_ids = [int(value) for value in contract["processing_order"]]
    if projection is None and all_ids:
        with closing(local._immutable_connection(paths.database)) as connection:
            projection = _completed_target_projection(connection, all_ids)
    elif projection is None:
        projection = ({}, {}, {})
    item_intents = _item_intent_files(paths)
    pending_current = len(item_intents) == len(receipts) + 1
    remaining_start = len(receipts) + (1 if pending_current else 0)
    try:
        _validate_remaining_eligible_baseline(
            paths,
            contract,
            completed_count=remaining_start,
            projection=projection,
        )
    except FullLocalAnalysisError as exc:
        if pending_current:
            raise FullLocalAnalysisError(
                "pending item的unstarted suffix偏离冻结initial baseline"
            ) from exc
        raise
    _extend_resume_guards(
        paths,
        contract,
        receipts,
        runtime,
        projection=projection,
    )
    completion_values = (
        list(completions)
        if completions is not None
        else [
            _read_json(path, label="resume guard completion")
            for path in _completion_files(paths)
        ]
    )
    if len(completion_values) > len(batch_receipts):
        raise FullLocalAnalysisError("resume guard completion超前于batch")
    for sequence, completion in enumerate(completion_values, 1):
        completed_count = int(
            batch_receipts[sequence - 1]["item_receipts"][-1][0]
        )
        expected = runtime.resume_guards_by_count.get(completed_count)
        recorded = completion.get("resume_guard")
        _validate_resume_guard_value(
            recorded, completed_count=completed_count
        )
        if (
            expected is None
            or local._canonical_bytes(recorded)
            != local._canonical_bytes(expected)
        ):
            raise FullLocalAnalysisError(
                f"resume guard completion {sequence} 与历史内容漂移"
            )


def _logical_global_heads(contract: Mapping[str, Any]) -> Mapping[str, str]:
    return {
        "provider": _json_sha(contract["provider_baseline"]),
        "protected": _json_sha(contract["protected_baseline"]),
        "source": _json_sha(
            {
                "source_completion_sha256": _json_sha(
                    contract["source_completion"]
                ),
                "source_summaries_sha256": contract[
                    "source_summaries_sha256"
                ],
                "source_groups": contract["source_groups"],
                "missing_universe": contract["missing_universe"],
            }
        ),
        "schema": _json_sha(contract["schema_baseline"]),
    }


def _logical_database_checkpoint(
    contract: Mapping[str, Any],
    runtime: RuntimeContext,
    *,
    batch_index: int,
    completed_count: int,
) -> Mapping[str, Any]:
    if type(batch_index) is not int or type(completed_count) is not int:
        raise FullLocalAnalysisError("logical database checkpoint索引类型漂移")
    cached = runtime.logical_checkpoints_by_count.get(completed_count)
    if cached is not None:
        _validate_logical_database_checkpoint(
            cached,
            batch_index=batch_index,
            completed_count=completed_count,
        )
        return cached
    guard = runtime.resume_guards_by_count.get(completed_count)
    processing = runtime.processing_prefix_heads_by_count.get(completed_count)
    sequences = runtime.derived_sequences_by_count.get(completed_count)
    if guard is None or processing is None or sequences is None:
        raise FullLocalAnalysisError(
            "logical database checkpoint缺少可重派生prefix状态"
        )
    heads = runtime.logical_global_heads
    value = {
        "contract_sha": runtime.contract_sha256,
        "batch_index": batch_index,
        "completed_count": completed_count,
        "processing_prefix_sha": processing,
        "resume_guard_sha": _json_sha(guard),
        "target_head": guard["target_head_sha256"],
        "output_head": guard["output_head_sha256"],
        "remaining_head": guard["remaining_target_head_sha256"],
        "provider": heads["provider"],
        "protected": heads["protected"],
        "source": heads["source"],
        "schema": heads["schema"],
        "derived_sequence": {
            str(name): int(sequence)
            for name, sequence in sorted(sequences.items())
        },
    }
    result = {**value, "logical_head": _json_sha(value)}
    _validate_logical_database_checkpoint(
        result,
        batch_index=batch_index,
        completed_count=completed_count,
    )
    runtime.logical_checkpoints_by_count[completed_count] = result
    return result


def _validate_logical_database_checkpoint(
    value: Any,
    *,
    batch_index: int | None = None,
    completed_count: int | None = None,
    expected: Mapping[str, Any] | None = None,
) -> None:
    keys = {
        "contract_sha",
        "batch_index",
        "completed_count",
        "processing_prefix_sha",
        "resume_guard_sha",
        "target_head",
        "output_head",
        "remaining_head",
        "provider",
        "protected",
        "source",
        "schema",
        "derived_sequence",
        "logical_head",
    }
    digest_fields = keys - {
        "batch_index",
        "completed_count",
        "derived_sequence",
    }
    if (
        not isinstance(value, Mapping)
        or set(value) != keys
        or type(value.get("batch_index")) is not int
        or type(value.get("completed_count")) is not int
        or value["batch_index"] < 0
        or value["completed_count"] < 0
        or (batch_index is not None and value["batch_index"] != batch_index)
        or (
            completed_count is not None
            and value["completed_count"] != completed_count
        )
        or any(
            type(value.get(name)) is not str
            or re.fullmatch(r"[0-9a-f]{64}", value[name]) is None
            for name in digest_fields
        )
        or not isinstance(value.get("derived_sequence"), Mapping)
        or any(
            not isinstance(name, str)
            or type(sequence) is not int
            or sequence < 0
            for name, sequence in value.get("derived_sequence", {}).items()
        )
    ):
        raise FullLocalAnalysisError(
            "logical database checkpoint字段/精确类型漂移"
        )
    core = {name: value[name] for name in keys - {"logical_head"}}
    if value["logical_head"] != _json_sha(core):
        raise FullLocalAnalysisError("logical database checkpoint head漂移")
    if expected is not None and local._canonical_bytes(value) != local._canonical_bytes(
        expected
    ):
        raise FullLocalAnalysisError(
            "logical database checkpoint未从当前prefix精确重派生"
        )


def _validate_historical_logical_checkpoints(
    contract: Mapping[str, Any],
    batch_receipts: Sequence[Mapping[str, Any]],
    runtime: RuntimeContext,
) -> None:
    for batch_index, receipt in enumerate(batch_receipts, 1):
        completed_count = int(receipt["item_receipts"][-1][0])
        expected = _logical_database_checkpoint(
            contract,
            runtime,
            batch_index=batch_index,
            completed_count=completed_count,
        )
        recorded = receipt.get("after", {}).get("database")
        _validate_logical_database_checkpoint(
            recorded,
            batch_index=batch_index,
            completed_count=completed_count,
            expected=expected,
        )
        if receipt.get("after", {}).get("sequences") != expected[
            "derived_sequence"
        ]:
            raise FullLocalAnalysisError(
                f"batch {batch_index} sequences未按prefix独立派生"
            )
        checkpoint = receipt.get("audit", {}).get("full_checkpoint")
        if checkpoint is not None:
            _validate_logical_database_checkpoint(
                checkpoint,
                batch_index=batch_index,
                completed_count=completed_count,
                expected=expected,
            )


def _validate_current_sequence_prefix(
    paths: BatchPaths,
    receipts: Sequence[Mapping[str, Any]],
    runtime: RuntimeContext,
    sequences: Mapping[str, int],
) -> None:
    derived_sequence = runtime.derived_sequences_by_count[len(receipts)]
    pending_item = len(_item_intent_files(paths)) == len(receipts) + 1
    if pending_item:
        pending_intent = _read_json(
            _item_intent_files(paths)[-1],
            label="logical pending item sequence baseline",
        )
        pending_sequences = pending_intent.get("before", {}).get("sequences")
        _validate_exact_sequence_mapping(
            pending_sequences, label="pending item before"
        )
        if local._canonical_bytes(pending_sequences) != local._canonical_bytes(
            derived_sequence
        ):
            raise FullLocalAnalysisError(
                "pending item sequence baseline未按completed prefix派生"
            )
    else:
        _validate_exact_sequence_mapping(
            sequences, label="current sqlite_sequence"
        )
    if not pending_item and local._canonical_bytes(
        sequences
    ) != local._canonical_bytes(derived_sequence):
        raise FullLocalAnalysisError(
            "current sqlite_sequence未按completed prefix精确派生"
        )


def _database_identity(path: Path) -> Mapping[str, Any]:
    return local._database_identity(path)


def _item_analysis_counts(
    connection: sqlite3.Connection, content_id: int
) -> Mapping[str, int]:
    return local._existing_analysis_counts(connection, [content_id])


def _database_readset_snapshot(path: Path) -> Mapping[str, Any]:
    result: dict[str, Any] = {}
    for label, candidate in (
        ("main", path),
        ("wal", Path(f"{path}-wal")),
        ("shm", Path(f"{path}-shm")),
    ):
        if not os.path.lexists(candidate):
            result[label] = {"exists": False}
            continue
        try:
            metadata = local._private_file(
                candidate, label=f"sidecar-aware只读{label}"
            )
        except local.LocalAnalysisCanaryError as exc:
            raise FullLocalAnalysisError(str(exc)) from exc
        result[label] = {
            "exists": True,
            "path": str(candidate),
            "device": metadata.st_dev,
            "inode": metadata.st_ino,
            "nlink": metadata.st_nlink,
            "mode": stat.S_IMODE(metadata.st_mode),
            "byte_size": metadata.st_size,
            "mtime_ns": metadata.st_mtime_ns,
            "ctime_ns": metadata.st_ctime_ns,
            "sha256": local._sha256_file(candidate),
        }
    return result


@contextlib.contextmanager
def _invocation_read_connection(
    paths: BatchPaths,
    runtime: RuntimeContext,
) -> Iterator[tuple[sqlite3.Connection, Path]]:
    sidecars = local._database_sidecars(paths.database)
    journal = Path(f"{paths.database}-journal")
    if journal in sidecars:
        raise FullLocalAnalysisError(
            "invocation只读门不接受rollback journal sidecar"
        )
    wal = Path(f"{paths.database}-wal")
    shm = Path(f"{paths.database}-shm")
    has_wal_state = wal in sidecars or shm in sidecars
    if not has_wal_state:
        with closing(local._immutable_connection(paths.database)) as connection:
            yield connection, paths.database
        return
    if wal not in sidecars or shm not in sidecars:
        raise FullLocalAnalysisError("WAL只读门要求wal/shm同时存在")
    before = _database_readset_snapshot(paths.database)
    runtime.sidecar_readset.clear()
    runtime.sidecar_readset.update(before)
    pending_error: BaseException | None = None
    with tempfile.TemporaryDirectory(
        prefix="dcar-full-local-wal-readonly-"
    ) as temporary_root:
        clone = Path(temporary_root) / "work.sqlite3"
        for source, target in (
            (paths.database, clone),
            (wal, Path(f"{clone}-wal")),
            (shm, Path(f"{clone}-shm")),
        ):
            shutil.copyfile(source, target)
        copied = {
            label: {
                "byte_size": candidate.stat().st_size,
                "sha256": local._sha256_file(candidate),
            }
            for label, candidate in (
                ("main", clone),
                ("wal", Path(f"{clone}-wal")),
                ("shm", Path(f"{clone}-shm")),
            )
        }
        if any(
            copied[label]
            != {
                "byte_size": int(before[label]["byte_size"]),
                "sha256": str(before[label]["sha256"]),
            }
            for label in copied
        ):
            raise FullLocalAnalysisError("WAL只读副本字节未精确绑定原证据")
        after_copy = _database_readset_snapshot(paths.database)
        if local._canonical_bytes(after_copy) != local._canonical_bytes(before):
            raise FullLocalAnalysisError(
                "WAL证据在只读副本构建期间发生漂移"
            )
        replay = sqlite3.connect(clone, timeout=30)
        try:
            checkpoint = replay.execute(
                "PRAGMA wal_checkpoint(TRUNCATE)"
            ).fetchone()
            if checkpoint is None or int(checkpoint[0]) != 0:
                raise FullLocalAnalysisError("WAL只读副本无法完整replay")
            mode = str(
                replay.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
            )
            if mode.lower() != "delete":
                raise FullLocalAnalysisError("WAL只读副本无法闭合journal")
            if str(replay.execute("PRAGMA quick_check").fetchone()[0]) != "ok":
                raise FullLocalAnalysisError("WAL只读副本quick_check失败")
        finally:
            replay.close()
        connection = local._immutable_connection(clone)
        try:
            connection.execute("PRAGMA query_only=ON")
            if int(connection.execute("PRAGMA query_only").fetchone()[0]) != 1:
                raise FullLocalAnalysisError("WAL副本只读门未进入query_only")
            yield connection, clone
        except BaseException as exc:
            pending_error = exc
            raise
        finally:
            connection.close()
            after = _database_readset_snapshot(paths.database)
            if local._canonical_bytes(after) != local._canonical_bytes(before):
                stability_error = FullLocalAnalysisError(
                    "WAL sidecar-aware只读期间main/wal/shm内容或stat漂移"
                )
                if pending_error is None:
                    raise stability_error
                raise stability_error from pending_error


def _invocation_global_readonly_gate(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    *,
    batch_receipts: Sequence[Mapping[str, Any]],
    item_receipts: Sequence[Mapping[str, Any]],
    completions: Sequence[Mapping[str, Any]],
    runtime: RuntimeContext,
    item_receipt_overrides: Mapping[int, Path] | None = None,
) -> Mapping[str, Any]:
    """Validate every cross-domain invariant before any recovery write."""

    with _invocation_read_connection(paths, runtime) as (
        connection,
        validation_database,
    ):
        validation_paths = replace(
            paths,
            database=validation_database,
            local_paths=replace(
                paths.local_paths,
                database=validation_database,
            ),
        )
        provider = _provider_snapshot(connection)
        protected = _protected_snapshot(connection, contract["eligible_ids"])
        source_groups = _source_groups_snapshot(
            connection, contract["target_ids"]
        )
        schema = _schema_snapshot(connection)
        sequences = _sequence_snapshot(connection)
        all_ids = [int(value) for value in contract["processing_order"]]
        projection = (
            _completed_target_projection(connection, all_ids)
            if all_ids
            else ({}, {}, {})
        )
        if provider != contract["provider_baseline"]:
            raise FullLocalAnalysisError(
                "invocation恢复写前provider usage/budget发生漂移"
            )
        if protected != contract["protected_baseline"]:
            raise FullLocalAnalysisError(
                "invocation恢复写前protected/unknown/link数据发生漂移"
            )
        if source_groups != contract["source_groups"]:
            raise FullLocalAnalysisError(
                "invocation恢复写前history source标签发生漂移"
            )
        if schema != contract["schema_baseline"]:
            raise FullLocalAnalysisError(
                "invocation恢复写前database schema发生漂移"
            )
        _assert_sequence_boundary(contract["sequence_baseline"], sequences)
        _validate_resume_guard_history(
            paths,
            contract,
            item_receipts,
            batch_receipts,
            runtime,
            completions=completions,
            projection=projection,
        )
        _validate_historical_logical_checkpoints(
            contract,
            batch_receipts,
            runtime,
        )
        _validate_current_sequence_prefix(
            paths,
            item_receipts,
            runtime,
            sequences,
        )
        _validate_pending_network_recovery_read_only(
            validation_paths,
            contract,
            item_receipts=item_receipts,
            runtime=runtime,
        )
        _validate_historical_item_receipts_semantic(
            validation_paths,
            contract,
            item_receipts=item_receipts,
            runtime=runtime,
            receipt_overrides=item_receipt_overrides,
        )
        _validate_invocation_output_ownership(
            paths,
            contract,
            batch_receipts=batch_receipts,
            item_receipts=item_receipts,
            projection=projection,
        )
        if runtime.sidecar_readset:
            runtime.sidecar_expected_global.clear()
            runtime.sidecar_expected_global.update(
                {
                    "provider": provider,
                    "protected": protected,
                    "source_groups": source_groups,
                    "schema": schema,
                    "sequences": sequences,
                    "eligible_projection_sha256": _json_sha(
                        [
                            {
                                "ordinal": ordinal,
                                "content_id": content_id,
                                "target_rows_sha256": _json_sha(
                                    projection[0][content_id]
                                ),
                                "content_direction": projection[1][content_id],
                                "link_id": projection[2][content_id],
                            }
                            for ordinal, content_id in enumerate(all_ids, 1)
                        ]
                    ),
                }
            )
    return {
        "provider": provider,
        "protected_sha256": _json_sha(protected),
        "source_groups_sha256": _json_sha(source_groups),
        "schema_sha256": _json_sha(schema),
        "sequences": sequences,
    }


def _validate_invocation_output_ownership(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    *,
    batch_receipts: Sequence[Mapping[str, Any]],
    item_receipts: Sequence[Mapping[str, Any]],
    projection: tuple[
        Mapping[int, Mapping[str, Sequence[Mapping[str, Any]]]],
        Mapping[int, Any],
        Mapping[int, str],
    ],
) -> None:
    """Reject unknown output ownership before any recovery transition writes."""

    if batch_receipts:
        latest = batch_receipts[-1]
        completed_count = int(latest["item_receipts"][-1][0])
        outputs = latest["after"]["outputs"]
        _validate_checkpoint_closure_shape(
            database=latest["after"]["database"], outputs=outputs
        )
        baseline = outputs["ownership"]
    else:
        completed_count = 0
        empty_rows: list[Mapping[str, Any]] = []
        baseline = {
            label: {
                "entries": 0,
                "rows_sha256": _json_sha(empty_rows),
                "rows": [],
            }
            for label in ("media", "fingerprints")
        }
    if completed_count > len(item_receipts):
        raise FullLocalAnalysisError(
            "invocation output ownership缺少已闭batch item前缀"
        )

    expected_names = {
        label: {str(row["name"]) for row in baseline[label]["rows"]}
        for label in ("media", "fingerprints")
    }
    link_ids = projection[2]
    for index in range(completed_count, len(item_receipts)):
        receipt = item_receipts[index]
        content_id = int(receipt["content_id"])
        link_id = link_ids[content_id]
        if (paths.media_root / link_id).exists():
            expected_names["media"].add(link_id)
        for row in receipt["after"]["outputs"]["fingerprints"]["rows"]:
            expected_names["fingerprints"].add(str(row["path"]))

    item_intents = _item_intent_files(paths)
    if len(item_intents) == len(item_receipts) + 1:
        pending = _read_json(
            item_intents[-1], label="invocation pending output intent"
        )
        content_id = int(pending["content_id"])
        link_id = link_ids[content_id]
        source = {"content": {"link_id": link_id}}
        pending_outputs = _item_output_inventory(paths, source)
        if (paths.media_root / link_id).exists():
            expected_names["media"].add(link_id)
        for row in pending_outputs["fingerprints"]["rows"]:
            expected_names["fingerprints"].add(str(row["path"]))
    elif len(item_intents) != len(item_receipts):
        raise FullLocalAnalysisError(
            "invocation output ownership对应item状态机漂移"
        )

    current = _output_ownership(paths)
    for label in ("media", "fingerprints"):
        current_rows = {
            str(row["name"]): row for row in current[label]["rows"]
        }
        baseline_rows = {
            str(row["name"]): row for row in baseline[label]["rows"]
        }
        if set(current_rows) != expected_names[label]:
            raise FullLocalAnalysisError("output top-level ownership漂移")
        if any(
            local._canonical_bytes(current_rows.get(name))
            != local._canonical_bytes(row)
            for name, row in baseline_rows.items()
        ):
            raise FullLocalAnalysisError("output top-level ownership漂移")


def _validate_finalized_sidecar_projection(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    runtime: RuntimeContext,
) -> None:
    expected = runtime.sidecar_expected_global
    if not expected:
        raise FullLocalAnalysisError("WAL finalize缺少只读副本预期投影")
    if local._database_sidecars(paths.database):
        raise FullLocalAnalysisError("WAL finalize后sidecar仍存在")
    with closing(local._immutable_connection(paths.database)) as connection:
        all_ids = [int(value) for value in contract["processing_order"]]
        projection = (
            _completed_target_projection(connection, all_ids)
            if all_ids
            else ({}, {}, {})
        )
        current = {
            "provider": _provider_snapshot(connection),
            "protected": _protected_snapshot(
                connection, contract["eligible_ids"]
            ),
            "source_groups": _source_groups_snapshot(
                connection, contract["target_ids"]
            ),
            "schema": _schema_snapshot(connection),
            "sequences": _sequence_snapshot(connection),
            "eligible_projection_sha256": _json_sha(
                [
                    {
                        "ordinal": ordinal,
                        "content_id": content_id,
                        "target_rows_sha256": _json_sha(
                            projection[0][content_id]
                        ),
                        "content_direction": projection[1][content_id],
                        "link_id": projection[2][content_id],
                    }
                    for ordinal, content_id in enumerate(all_ids, 1)
                ]
            ),
        }
    if local._canonical_bytes(current) != local._canonical_bytes(expected):
        raise FullLocalAnalysisError(
            "WAL finalize后main DB投影不等于只读副本预期"
        )
    runtime.sidecar_readset.clear()
    runtime.sidecar_expected_global.clear()


def _assert_global_invariants(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    *,
    before_sequences: Mapping[str, int] | None = None,
    full_content: bool = False,
) -> Mapping[str, Any]:
    if local._database_sidecars(paths.database):
        raise FullLocalAnalysisError("数据库/WAL未闭合到item receipt")
    with closing(local._immutable_connection(paths.database)) as connection:
        provider = _provider_snapshot(connection)
        protected = _protected_snapshot(connection, contract["eligible_ids"])
        source_groups = _source_groups_snapshot(connection, contract["target_ids"])
        schema = _schema_snapshot(connection)
        sequences = _sequence_snapshot(connection)
    if provider != contract["provider_baseline"]:
        raise FullLocalAnalysisError("provider usage/budget发生漂移")
    if protected != contract["protected_baseline"]:
        raise FullLocalAnalysisError("protected/unknown/link数据发生漂移")
    if source_groups != contract["source_groups"]:
        raise FullLocalAnalysisError("history archive/backfill标签发生漂移")
    if schema != contract["schema_baseline"]:
        raise FullLocalAnalysisError("database schema发生漂移")
    if before_sequences is not None:
        _assert_sequence_boundary(before_sequences, sequences)
    return {
        "database": _database_closure(
            paths.database, full_content=full_content
        ),
        "provider": provider,
        "protected_sha256": _json_sha(protected),
        "schema_sha256": _json_sha(schema),
        "sequences": sequences,
        "outputs": _output_closure(paths, full_content=full_content),
    }


def _batch_checkpoint_state(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    *,
    batch_index: int,
    completed_count: int,
    full_content: bool,
    runtime: RuntimeContext,
) -> Mapping[str, Any]:
    """Build a batch closure after invocation-start global invariants passed."""

    if local._database_sidecars(paths.database):
        raise FullLocalAnalysisError("数据库/WAL未闭合到batch receipt")
    database = _logical_database_checkpoint(
        contract,
        runtime,
        batch_index=batch_index,
        completed_count=completed_count,
    )
    return {
        "database": database,
        "provider": contract["provider_baseline"],
        "protected_sha256": contract["protected_baseline_sha256"],
        "sequences": database["derived_sequence"],
        "outputs": _output_closure(paths, full_content=full_content),
    }


def _validate_invocation_end_state(
    end_state: Mapping[str, Any], latest_batch: Mapping[str, Any]
) -> None:
    expected = latest_batch["after"]
    database = expected["database"]
    if (
        end_state["provider"] != expected["provider"]
        or end_state["protected_sha256"] != expected["protected_sha256"]
        or end_state["sequences"] != database["derived_sequence"]
        or local._canonical_bytes(end_state["outputs"]["ownership"])
        != local._canonical_bytes(expected["outputs"]["ownership"])
    ):
        raise FullLocalAnalysisError(
            "invocation结束全局provider/protected/source闭包漂移"
        )


def _validate_invocation_managed_appends(
    paths: BatchPaths,
    *,
    content_ids: Sequence[int],
    runtime: RuntimeContext,
) -> None:
    """Account for only rows appended since the previous invocation gate."""

    if not runtime.managed_sequence_head:
        raise FullLocalAnalysisError("invocation managed sequence水位缺失")
    allowed_ids = set(content_ids)
    with closing(local._immutable_connection(paths.database)) as connection:
        current_sequences = _sequence_snapshot(connection)
        target_rows = local._target_rows(connection, content_ids)
        evaluation_owner = {
            int(row["id"]): int(row["content_id"])
            for row in target_rows.get("evaluation_versions", [])
        }
        for table in sorted(local.MANAGED_TARGET_TABLES):
            previous = int(runtime.managed_sequence_head.get(table, 0))
            current = int(current_sequences.get(table, 0))
            if current < previous:
                raise FullLocalAnalysisError(
                    f"protected managed sequence倒退：{table}"
                )
            if current == previous:
                continue
            columns = local._table_columns(connection, table)
            if "id" not in columns:
                raise FullLocalAnalysisError(
                    f"protected managed append缺少ID：{table}"
                )
            cursor = connection.execute(
                f"SELECT * FROM {local._quoted(table)} "
                "WHERE id>? AND id<=? ORDER BY id",
                (previous, current),
            )
            names = [str(value[0]) for value in cursor.description or ()]
            rows = [
                {
                    name: local._json_value(row[index])
                    for index, name in enumerate(names)
                }
                for row in cursor
            ]
            if [int(row["id"]) for row in rows] != list(
                range(previous + 1, current + 1)
            ):
                raise FullLocalAnalysisError(
                    f"protected managed append ID区间不闭合：{table}"
                )
            expected = Counter(
                local._canonical_bytes(row)
                for row in target_rows.get(table, [])
            )
            for row in rows:
                owner = (
                    int(row["content_id"])
                    if "content_id" in row
                    else evaluation_owner.get(int(row["evaluation_id"]))
                )
                key = local._canonical_bytes(row)
                if owner not in allowed_ids or expected[key] <= 0:
                    raise FullLocalAnalysisError(
                        f"protected managed append owner/row漂移：{table}"
                    )
                expected[key] -= 1
    runtime.managed_sequence_head.clear()
    runtime.managed_sequence_head.update(
        {
            table: int(current_sequences.get(table, 0))
            for table in local.MANAGED_TARGET_TABLES
        }
    )


def _code_snapshot() -> Mapping[str, Any]:
    controller = Path(__file__).resolve()
    return {
        "controller": {
            "path": str(controller),
            "sha256": local._sha256_file(controller),
            "byte_size": controller.stat().st_size,
        },
        "local_dependencies": local._code_snapshot(),
    }


def _build_contract(
    paths: BatchPaths,
    *,
    source_evidence: Mapping[str, Any],
    target_ids: Sequence[int],
    eligible_ids: Sequence[int],
    static_deferred: Sequence[Mapping[str, Any]],
    source_summaries: Mapping[int, Mapping[str, Any]],
    missing: Mapping[str, Any],
    profile: HistoryProfile,
    tools: Mapping[str, Any],
    target_row_map: Mapping[int, Sequence[Any]],
) -> Mapping[str, Any]:
    order = _processing_order(eligible_ids, profile, source_summaries)
    with closing(local._immutable_connection(paths.database)) as connection:
        current_eligible, current_deferred, current_summaries = _classify_universe(
            connection,
            target_ids,
            source_evidence=source_evidence,
            row_map=target_row_map,
        )
        if (
            current_eligible != list(eligible_ids)
            or current_deferred != list(static_deferred)
            or current_summaries != source_summaries
        ):
            raise FullLocalAnalysisError(
                "copy后static source分类与只读plan发生漂移"
            )
        existing = local._existing_analysis_counts(connection, eligible_ids)
        if any(existing.values()):
            raise FullLocalAnalysisError(
                f"首次累计clone要求eligible尚未分析：{existing}"
            )
        provider = _provider_snapshot(connection)
        # Only eligible IDs are mutable. Static-deferred history rows remain
        # inside the protected projection exactly as they existed in Step3.
        protected = _protected_snapshot(connection, eligible_ids)
        source_groups = _source_groups_snapshot(connection, target_ids)
        schema = _schema_snapshot(connection)
        sequences = _sequence_snapshot(connection)
        eligible_target_baseline = _eligible_target_leaves(connection, order)
    return {
        "schema_version": SCHEMA_VERSION,
        "mode": "manual-full-history",
        "publication_allowed": False,
        "provider_calls_allowed": 0,
        "workers": 1,
        "batch_download_cap_bytes": BATCH_DOWNLOAD_CAP_BYTES,
        "ordering_policy": ORDERING_POLICY,
        "audit_policy": _audit_policy_value(),
        "profile": _profile_value(profile),
        "source_database": source_evidence["database"],
        "source_completion": source_evidence,
        "target_ids": list(target_ids),
        "target_ids_sha256": _json_sha(list(target_ids)),
        "eligible_ids": list(eligible_ids),
        "eligible_ids_sha256": _json_sha(list(eligible_ids)),
        "eligible_target_baseline": eligible_target_baseline,
        "eligible_target_baseline_sha256": _json_sha(
            eligible_target_baseline
        ),
        "static_deferred": list(static_deferred),
        "static_deferred_sha256": _json_sha(list(static_deferred)),
        "processing_order": order,
        "processing_order_sha256": _json_sha(order),
        "source_summaries": {
            str(key): source_summaries[key] for key in sorted(source_summaries)
        },
        "source_summaries_sha256": _json_sha(
            [[key, source_summaries[key]] for key in sorted(source_summaries)]
        ),
        "missing_universe": missing,
        "history_labels": "preserve_exact",
        "source_groups": source_groups,
        "schema_baseline": schema,
        "schema_baseline_sha256": _json_sha(schema),
        "provider_baseline": provider,
        "protected_baseline": protected,
        "protected_baseline_sha256": _json_sha(protected),
        "sequence_baseline": sequences,
        "database_baseline": _database_identity(paths.database),
        "media_root": str(paths.media_root),
        "fingerprint_root": str(paths.fingerprint_root),
        "run_root": str(paths.run_root),
        "root_bindings": _root_bindings(paths),
        "output_baseline": _output_inventory(paths),
        "tools": tools,
        "code": _code_snapshot(),
        "copy_records": {
            "intent_sha256": local._sha256_file(paths.local_paths.copy_intent),
            "receipt_sha256": local._sha256_file(paths.local_paths.copy_receipt),
        },
    }


def _validate_contract(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    *,
    expected_source_db_sha256: str,
    expected_source_completion_sha256: str,
    profile: HistoryProfile,
) -> None:
    expected_keys = {
        "schema_version",
        "mode",
        "publication_allowed",
        "provider_calls_allowed",
        "workers",
        "batch_download_cap_bytes",
        "ordering_policy",
        "audit_policy",
        "profile",
        "source_database",
        "source_completion",
        "target_ids",
        "target_ids_sha256",
        "eligible_ids",
        "eligible_ids_sha256",
        "eligible_target_baseline",
        "eligible_target_baseline_sha256",
        "static_deferred",
        "static_deferred_sha256",
        "processing_order",
        "processing_order_sha256",
        "source_summaries",
        "source_summaries_sha256",
        "missing_universe",
        "history_labels",
        "source_groups",
        "schema_baseline",
        "schema_baseline_sha256",
        "provider_baseline",
        "protected_baseline",
        "protected_baseline_sha256",
        "sequence_baseline",
        "database_baseline",
        "media_root",
        "fingerprint_root",
        "run_root",
        "root_bindings",
        "output_baseline",
        "tools",
        "code",
        "copy_records",
    }
    if (
        set(contract) != expected_keys
        or contract.get("schema_version") != SCHEMA_VERSION
        or contract.get("mode") != "manual-full-history"
        or contract.get("publication_allowed") is not False
        or type(contract.get("provider_calls_allowed")) is not int
        or contract.get("provider_calls_allowed") != 0
        or type(contract.get("workers")) is not int
        or contract.get("workers") != 1
        or type(contract.get("batch_download_cap_bytes")) is not int
        or contract.get("batch_download_cap_bytes") != BATCH_DOWNLOAD_CAP_BYTES
        or contract.get("ordering_policy") != ORDERING_POLICY
        or contract.get("audit_policy") != _audit_policy_value()
        or contract.get("profile") != _profile_value(profile)
        or contract.get("media_root") != str(paths.media_root)
        or contract.get("fingerprint_root") != str(paths.fingerprint_root)
        or contract.get("run_root") != str(paths.run_root)
    ):
        raise FullLocalAnalysisError("global contract模式/版本/profile漂移")
    _validate_root_bindings(paths, contract.get("root_bindings", {}))
    empty_outputs = {
        "media": {"files": 0, "rows_sha256": _json_sha([]), "rows": []},
        "fingerprints": {
            "files": 0,
            "rows_sha256": _json_sha([]),
            "rows": [],
        },
    }
    if contract.get("output_baseline") != empty_outputs:
        raise FullLocalAnalysisError("global contract输出baseline必须精确为空")
    target_ids = contract.get("target_ids")
    eligible_ids = contract.get("eligible_ids")
    eligible_target_baseline = contract.get("eligible_target_baseline")
    deferred = contract.get("static_deferred")
    if (
        not isinstance(target_ids, list)
        or any(type(value) is not int for value in target_ids)
        or len(set(target_ids)) != len(target_ids)
        or len(target_ids) != profile.universe_count
        or contract.get("target_ids_sha256") != _json_sha(target_ids)
        or not isinstance(eligible_ids, list)
        or any(type(value) is not int for value in eligible_ids)
        or len(set(eligible_ids)) != len(eligible_ids)
        or len(eligible_ids) != profile.eligible_count
        or contract.get("eligible_ids_sha256") != _json_sha(eligible_ids)
        or not isinstance(eligible_target_baseline, list)
        or len(eligible_target_baseline) != len(eligible_ids)
        or any(
            not isinstance(row, Mapping)
            or set(row)
            != {
                "ordinal",
                "content_id",
                "target_rows_sha256",
                "content_direction",
                "link_id",
            }
            or type(row.get("ordinal")) is not int
            or row.get("ordinal") != ordinal
            or type(row.get("content_id")) is not int
            or type(row.get("target_rows_sha256")) is not str
            or re.fullmatch(r"[0-9a-f]{64}", row["target_rows_sha256"])
            is None
            or type(row.get("link_id")) is not str
            for ordinal, row in enumerate(eligible_target_baseline, 1)
        )
        or contract.get("eligible_target_baseline_sha256")
        != _json_sha(eligible_target_baseline)
        or not isinstance(deferred, list)
        or any(
            not isinstance(row, Mapping)
            or set(row) != {"content_id", "reason"}
            or type(row.get("content_id")) is not int
            or row.get("reason")
            not in {
                STATIC_DEFER_REASON,
                "non_https_media_url",
                "audio_placeholder",
            }
            for row in deferred
        )
        or len(deferred) != profile.static_deferred_count
        or contract.get("static_deferred_sha256") != _json_sha(deferred)
    ):
        raise FullLocalAnalysisError("global contract universe分类漂移")
    if set(target_ids) != set(eligible_ids) | {
        int(row["content_id"]) for row in deferred
    } or set(eligible_ids) & {int(row["content_id"]) for row in deferred}:
        raise FullLocalAnalysisError("eligible/deferred未精确分割Step3 universe")
    summaries = contract.get("source_summaries")
    if not isinstance(summaries, Mapping):
        raise FullLocalAnalysisError("global contract source summaries缺失")
    expected_summary_keys = {str(value) for value in eligible_ids}
    summary_row_keys = {
        "content_id",
        "source_sha256",
        "raw_response_body_sha256",
        "download_urls_sha256",
        "media_kind",
        "image_groups_sha256",
    }
    if set(summaries) != expected_summary_keys:
        raise FullLocalAnalysisError("global contract source summaries合同漂移")
    for key, row in summaries.items():
        if (
            not isinstance(row, Mapping)
            or set(row) != summary_row_keys
            or type(row.get("content_id")) is not int
            or row["content_id"] != int(key)
            or any(
                type(row.get(field)) is not str
                or re.fullmatch(r"[0-9a-f]{64}", row[field]) is None
                for field in (
                    "source_sha256",
                    "raw_response_body_sha256",
                    "download_urls_sha256",
                )
            )
            or type(row.get("media_kind")) is not str
            or row["media_kind"] not in {"image", "video"}
            or (
                row["media_kind"] == "image"
                and (
                    type(row.get("image_groups_sha256")) is not str
                    or re.fullmatch(
                        r"[0-9a-f]{64}", row["image_groups_sha256"]
                    )
                    is None
                )
            )
            or (
                row["media_kind"] == "video"
                and row.get("image_groups_sha256") is not None
            )
        ):
            raise FullLocalAnalysisError(
                "global contract source summaries行合同漂移"
            )
    if (
        contract.get("source_summaries_sha256")
        != _json_sha(
            [[int(key), summaries[key]] for key in sorted(summaries, key=int)]
        )
        or contract.get("processing_order_sha256")
        != _json_sha(contract.get("processing_order"))
        or contract.get("protected_baseline_sha256")
        != _json_sha(contract.get("protected_baseline"))
        or contract.get("schema_baseline_sha256")
        != _json_sha(contract.get("schema_baseline"))
        or contract.get("history_labels") != "preserve_exact"
    ):
        raise FullLocalAnalysisError("global contract hashes/history labels漂移")
    schema_baseline = contract.get("schema_baseline")
    if (
        not isinstance(schema_baseline, Mapping)
        or set(schema_baseline)
        != {"objects", "schema_version", "user_version", "application_id"}
        or not isinstance(schema_baseline.get("objects"), Mapping)
        or set(schema_baseline["objects"]) != {"rows", "sha256"}
        or type(schema_baseline["objects"].get("rows")) is not int
        or type(schema_baseline["objects"].get("sha256")) is not str
        or any(
            type(schema_baseline.get(key)) is not int
            for key in ("schema_version", "user_version", "application_id")
        )
    ):
        raise FullLocalAnalysisError("global contract schema baseline漂移")
    normalized_summaries = {
        int(key): value for key, value in summaries.items()
    }
    if contract.get("processing_order") != _processing_order(
        eligible_ids, profile, normalized_summaries
    ):
        raise FullLocalAnalysisError("global contract处理顺序漂移")
    if [
        int(row["content_id"]) for row in eligible_target_baseline
    ] != contract.get("processing_order"):
        raise FullLocalAnalysisError("global contract eligible baseline顺序漂移")
    source = contract.get("source_completion")
    if not isinstance(source, Mapping):
        raise FullLocalAnalysisError("global contract缺少Step3 evidence")
    current = local._source_completion_evidence(
        paths.local_paths,
        content_ids=target_ids,
        expected_source_db_sha256=expected_source_db_sha256,
        expected_source_completion_sha256=expected_source_completion_sha256,
    )
    if current != source:
        raise FullLocalAnalysisError("Step3 completion/database/contract发生漂移")
    if contract.get("source_database") != source.get("database"):
        raise FullLocalAnalysisError("source database evidence漂移")
    expected_missing = _missing_universe_evidence(_source_contract(paths), profile)
    if contract.get("missing_universe") != expected_missing:
        raise FullLocalAnalysisError("missing universe语义漂移")
    if contract.get("code") != _code_snapshot():
        raise FullLocalAnalysisError("冻结代码完整集合发生漂移")
    require_whisper = any(
        value.get("media_kind") == "video"
        for value in normalized_summaries.values()
    )
    if local._local_tools(require_whisper=require_whisper) != contract.get("tools"):
        raise FullLocalAnalysisError("冻结本地tools/config发生漂移")
    binding = _database_binding(paths.database)
    baseline_binding = contract.get("database_baseline")
    if (
        not isinstance(baseline_binding, Mapping)
        or any(
            binding[key] != baseline_binding[key]
            for key in ("path", "inode", "nlink")
        )
    ):
        raise FullLocalAnalysisError("work DB path/inode/private binding漂移")
    copy_records = contract.get("copy_records")
    if copy_records != {
        "intent_sha256": local._sha256_file(paths.local_paths.copy_intent),
        "receipt_sha256": local._sha256_file(paths.local_paths.copy_receipt),
    }:
        raise FullLocalAnalysisError("database copy记录漂移")
    has_batch_intent = any(
        path.name.endswith(".intent.json") and not path.name.startswith(".")
        for path in paths.batches_root.iterdir()
    )
    if not has_batch_intent and _output_inventory(paths) != empty_outputs:
        raise FullLocalAnalysisError("contract后首batch前输出根必须保持为空")


def _prepare_roots(paths: BatchPaths) -> None:
    local._prepare_roots(paths.local_paths)
    for root in (
        paths.batches_root,
        paths.items_root,
        paths.progress_root,
        paths.completions_root,
        paths.network_root,
    ):
        if not os.path.lexists(root):
            root.mkdir(mode=0o700)
            local._fsync_directory(root.parent)
        local._private_directory(root, label="累计分析记录目录")


def _require_existing_roots(paths: BatchPaths) -> None:
    for root in (
        paths.media_root,
        paths.fingerprint_root,
        paths.run_root,
        paths.batches_root,
        paths.items_root,
        paths.progress_root,
        paths.completions_root,
        paths.network_root,
    ):
        try:
            local._private_directory(root, label="existing run冻结目录")
        except local.LocalAnalysisCanaryError as exc:
            raise FullLocalAnalysisError(str(exc)) from exc
    try:
        local._private_file(paths.lock, label="existing controller lock")
    except local.LocalAnalysisCanaryError as exc:
        raise FullLocalAnalysisError(str(exc)) from exc


def _validate_numbered_directory_physical(
    root: Path,
    *,
    suffixes: Sequence[str],
    allow_atomic_temps: bool,
) -> None:
    try:
        local._private_directory(root, label="累计分析物理记录目录")
    except local.LocalAnalysisCanaryError as exc:
        raise FullLocalAnalysisError(str(exc)) from exc
    for path in root.iterdir():
        try:
            local._private_file(path, label="累计分析物理记录")
        except local.LocalAnalysisCanaryError as exc:
            raise FullLocalAnalysisError(str(exc)) from exc
        name = path.name
        if name.startswith(".") and name.endswith(".tmp"):
            if not allow_atomic_temps:
                raise FullLocalAnalysisError(f"plan拒绝待恢复atomic temp：{path}")
            name = name[1:-4]
        matched = next(
            (suffix for suffix in suffixes if name.endswith(suffix)), None
        )
        if matched is None:
            raise FullLocalAnalysisError(f"记录目录包含未知文件：{path}")
        prefix = name[: -len(matched)]
        if len(prefix) != 6 or not prefix.isdigit() or int(prefix) <= 0:
            raise FullLocalAnalysisError(f"记录物理序号非法：{path}")


def _preflight_existing_physical(
    paths: BatchPaths, *, allow_atomic_temps: bool
) -> None:
    """Pure read-only physical namespace gate for an existing run."""

    _require_existing_roots(paths)
    allowed_files = {
        paths.local_paths.copy_intent.name,
        paths.local_paths.copy_receipt.name,
        paths.contract.name,
        paths.lock.name,
    }
    allowed_dirs = {
        paths.batches_root.name,
        paths.items_root.name,
        paths.progress_root.name,
        paths.completions_root.name,
        paths.network_root.name,
    }
    allowed_temps = {f".{name}.tmp" for name in allowed_files}
    for path in paths.run_root.iterdir():
        if path.name in allowed_dirs:
            try:
                local._private_directory(path, label="existing run子目录")
            except local.LocalAnalysisCanaryError as exc:
                raise FullLocalAnalysisError(str(exc)) from exc
        elif path.name in allowed_files:
            try:
                local._private_file(path, label="existing run记录")
            except local.LocalAnalysisCanaryError as exc:
                raise FullLocalAnalysisError(str(exc)) from exc
        elif allow_atomic_temps and path.name in allowed_temps:
            try:
                local._private_file(path, label="existing run atomic temp")
            except local.LocalAnalysisCanaryError as exc:
                raise FullLocalAnalysisError(str(exc)) from exc
        else:
            raise FullLocalAnalysisError(f"run root包含未知记录：{path}")
    _validate_numbered_directory_physical(
        paths.items_root,
        suffixes=(".intent.json", ".receipt.json"),
        allow_atomic_temps=allow_atomic_temps,
    )
    _validate_numbered_directory_physical(
        paths.batches_root,
        suffixes=(".intent.json", ".receipt.json"),
        allow_atomic_temps=allow_atomic_temps,
    )
    _validate_numbered_directory_physical(
        paths.progress_root,
        suffixes=(".progress.json",),
        allow_atomic_temps=allow_atomic_temps,
    )
    _validate_numbered_directory_physical(
        paths.completions_root,
        suffixes=(".completion.json",),
        allow_atomic_temps=allow_atomic_temps,
    )
    _validate_numbered_directory_physical(
        paths.network_root,
        suffixes=(".network.json",),
        allow_atomic_temps=allow_atomic_temps,
    )
    _preflight_output_roots(paths)
    for sidecar in local._database_sidecars(paths.database):
        try:
            local._private_file(sidecar, label="existing work DB sidecar")
        except local.LocalAnalysisCanaryError as exc:
            raise FullLocalAnalysisError(str(exc)) from exc


def _preflight_output_roots(paths: BatchPaths) -> None:
    """Read-only output gate with one exact pending-item empty-dir exception."""

    for root in (paths.media_root, paths.fingerprint_root):
        try:
            local._private_directory(root, label="existing输出根")
        except local.LocalAnalysisCanaryError as exc:
            raise FullLocalAnalysisError(str(exc)) from exc
        for path in root.rglob("*"):
            try:
                if path.is_dir():
                    local._private_directory(path, label="existing输出子目录")
                else:
                    local._private_file(path, label="existing输出文件")
            except local.LocalAnalysisCanaryError as exc:
                raise FullLocalAnalysisError(str(exc)) from exc
            if path.name.startswith(".") or path.name.endswith(
                (".tmp", ".candidate")
            ):
                raise FullLocalAnalysisError(f"输出根包含临时或未知文件：{path}")
    # Top-level shape is a cheap ownership gate; content bytes are checked only
    # at the disclosed full checkpoints below.
    _output_ownership(paths)
    allowed_empty: set[Path] = set()
    intents = _item_intent_files(paths)
    receipt_ordinals = sorted(
        int(path.name[:6])
        for path in paths.items_root.iterdir()
        if path.name.endswith(".receipt.json") and not path.name.startswith(".")
    )
    receipts_are_prefix = receipt_ordinals == list(
        range(1, len(receipt_ordinals) + 1)
    )
    if receipts_are_prefix and len(intents) == len(receipt_ordinals) + 1:
        ordinal = len(intents)
        intent = _read_json(intents[-1], label="pending output intent")
        ledger_path = _network_path(paths, ordinal)
        if ledger_path.exists():
            ledger = _read_json(ledger_path, label="pending output ledger")
            if ledger.get("events"):
                with closing(local._immutable_connection(paths.database)) as connection:
                    row = connection.execute(
                        "SELECT link_id FROM content_items WHERE id=?",
                        (int(intent["content_id"]),),
                    ).fetchone()
                if row is None:
                    raise FullLocalAnalysisError("pending output content不存在")
                allowed_empty.add(paths.media_root / str(row["link_id"]))
    actual_empty: set[Path] = set()
    for root in (paths.media_root, paths.fingerprint_root):
        for directory in root.rglob("*"):
            if not directory.is_dir():
                continue
            try:
                local._private_directory(directory, label="existing输出子目录")
            except local.LocalAnalysisCanaryError as exc:
                raise FullLocalAnalysisError(str(exc)) from exc
            if not any(directory.iterdir()):
                actual_empty.add(directory)
    if actual_empty - allowed_empty:
        raise FullLocalAnalysisError(
            f"输出根包含未知空目录：{sorted(map(str, actual_empty - allowed_empty))}"
        )


def _recover_record_temps(
    paths: BatchPaths, contract: Mapping[str, Any]
) -> None:
    roots_and_suffixes = (
        (paths.items_root, (".intent.json", ".receipt.json")),
        (paths.batches_root, (".intent.json", ".receipt.json")),
        (paths.progress_root, (".progress.json",)),
        (paths.completions_root, (".completion.json",)),
    )
    batch_intents = _batch_intent_files(paths)
    batch_receipts = _batch_receipt_files(paths)
    item_intents = _item_intent_files(paths)
    # A complete historical item receipt temp may be bound by later immutable
    # progress/batch records.  Its final-file set can therefore contain a gap
    # until the downstream-bound recovery below promotes it.
    item_receipts = sorted(
        path
        for path in paths.items_root.iterdir()
        if not path.name.startswith(".")
        and path.name.endswith(".receipt.json")
    )
    complete_item_temp_overrides = _complete_record_temp_overrides(
        paths.items_root, ".receipt.json"
    )
    progress = _progress_paths(paths)
    completions = _completion_files(paths)
    pending_batch_ordinals: set[int] = set()
    if len(batch_intents) == len(batch_receipts) + 1:
        pending = _read_json(
            batch_intents[-1], label="temp precursor pending batch"
        )
        pending_batch_ordinals = {
            int(value) for value in pending["item_ordinals"]
        }

    recoverable_missing_final = {
        (paths.batches_root, ".intent.json"):
            len(batch_intents) == len(batch_receipts),
        (paths.batches_root, ".receipt.json"):
            len(batch_intents) == len(batch_receipts) + 1,
        (paths.items_root, ".intent.json"):
            len(item_intents) == len(item_receipts),
        (paths.items_root, ".receipt.json"):
            len(item_intents) == len(item_receipts) + 1,
        (paths.progress_root, ".progress.json"): True,
        (paths.completions_root, ".completion.json"): True,
    }
    expected_missing_final = {
        (paths.batches_root, ".intent.json"): len(batch_intents) + 1,
        (paths.batches_root, ".receipt.json"): len(batch_receipts) + 1,
        (paths.items_root, ".intent.json"): len(item_intents) + 1,
        (paths.items_root, ".receipt.json"): len(item_receipts) + 1,
        (paths.progress_root, ".progress.json"): len(progress) + 1,
        (paths.completions_root, ".completion.json"):
            len(completions) + 1,
    }
    validated: list[tuple[Path, Path, bool]] = []
    for root, suffixes in roots_and_suffixes:
        for candidate in sorted(root.iterdir()):
            if not candidate.name.startswith(".") or not candidate.name.endswith(
                ".tmp"
            ):
                continue
            final_name = candidate.name[1:-4]
            suffix = next(
                (
                    value
                    for value in suffixes
                    if final_name.endswith(value)
                ),
                None,
            )
            if suffix is None:
                raise FullLocalAnalysisError(
                    f"记录目录包含未知atomic temp：{candidate}"
                )
            prefix = final_name.split(".", 1)[0]
            if len(prefix) != 6 or not prefix.isdigit():
                raise FullLocalAnalysisError(
                    f"atomic temp序号非法：{candidate}"
                )
            final = root / final_name
            if not final.exists():
                ordinal = int(prefix)
                key = (root, suffix)
                precursor_ok = (
                    recoverable_missing_final[key]
                    and ordinal == expected_missing_final[key]
                )
                if key == (paths.batches_root, ".intent.json"):
                    if precursor_ok:
                        try:
                            precursor_ok = bool(
                                _batch_at_cursor(
                                    contract, len(item_receipts)
                                )
                            )
                        except (FullLocalAnalysisError, IndexError):
                            precursor_ok = False
                elif key == (paths.batches_root, ".receipt.json"):
                    precursor_ok = precursor_ok and _batch_paths(
                        paths, ordinal
                    )[0].exists()
                elif key == (paths.items_root, ".intent.json"):
                    precursor_ok = (
                        precursor_ok
                        and ordinal in pending_batch_ordinals
                    )
                elif key == (paths.items_root, ".receipt.json"):
                    precursor_ok = (
                        (
                            precursor_ok
                            or ordinal in complete_item_temp_overrides
                        )
                        and _item_paths(paths, ordinal)[0].exists()
                    )
                elif key == (paths.progress_root, ".progress.json"):
                    precursor_ok = precursor_ok and _item_paths(
                        paths, ordinal
                    )[1].exists()
                else:
                    precursor_ok = precursor_ok and _batch_paths(
                        paths, ordinal
                    )[1].exists()
                if not precursor_ok:
                    raise FullLocalAnalysisError(
                        "atomic temp缺少状态机前驱或不是唯一next："
                        f"{candidate}"
                    )
                # The owning state-machine transition recomputes the exact
                # canonical body and hands it to _write_atomic.  That primitive
                # safely replaces a legal truncated prefix; parsing here would
                # turn a real mid-write SIGKILL into an unrecoverable error.
                validated.append((candidate, final, False))
                continue
            try:
                local._private_file(final, label="immutable record final")
                temporary_body = candidate.read_bytes()
                final_body = final.read_bytes()
            except local.LocalAnalysisCanaryError as exc:
                raise FullLocalAnalysisError(str(exc)) from exc
            if temporary_body != final_body and not final_body.startswith(
                temporary_body
            ):
                raise FullLocalAnalysisError(
                    f"atomic temp不是既有final的合法写入前缀：{candidate}"
                )
            validated.append((candidate, final, True))

    # Validate the entire namespace before deleting even a redundant temp so a
    # future/unowned partial is preserved byte-for-byte on a global block.
    for candidate, _final, final_exists in validated:
        if not final_exists:
            continue
        candidate.unlink()
        local._fsync_directory(candidate.parent)


def _discard_nondurable_network_temps(paths: BatchPaths) -> None:
    """Discard exact-name network write temps without parsing partial JSON.

    Network access starts only after the ledger write has been renamed and
    fsynced.  Therefore a remaining temp was never the durable authorization
    for a request; an existing final remains authoritative, while a missing
    final is safely rebuilt as an empty ledger when execution resumes.
    """

    intents = _item_intent_files(paths)
    receipts = _receipt_files(paths)
    candidates: list[Path] = []
    missing_final_ordinals: list[int] = []
    for candidate in sorted(paths.network_root.iterdir()):
        if not candidate.name.startswith(".") or not candidate.name.endswith(
            ".network.json.tmp"
        ):
            continue
        final_name = candidate.name[1:-4]
        prefix = final_name[: -len(".network.json")]
        if len(prefix) != 6 or not prefix.isdigit() or int(prefix) <= 0:
            raise FullLocalAnalysisError(
                f"network atomic temp序号非法：{candidate}"
            )
        try:
            local._private_file(candidate, label="network atomic temp")
        except local.LocalAnalysisCanaryError as exc:
            raise FullLocalAnalysisError(str(exc)) from exc
        ordinal = int(prefix)
        final = paths.network_root / final_name
        if final.exists():
            if ordinal > len(intents):
                raise FullLocalAnalysisError(
                    f"network atomic temp缺少状态机前驱：{candidate}"
                )
        else:
            missing_final_ordinals.append(ordinal)
        candidates.append(candidate)
    unique_next = len(receipts) + 1
    if missing_final_ordinals and (
        missing_final_ordinals != [unique_next]
        or len(intents) != unique_next
        or _item_paths(paths, unique_next)[1].exists()
    ):
        raise FullLocalAnalysisError(
            "network atomic temp不是pending item状态机唯一next"
        )
    # Validate the complete network namespace before removing any temp.  A
    # future/unowned temp must remain byte-for-byte available for audit.
    for candidate in candidates:
        candidate.unlink()
        local._fsync_directory(paths.network_root)


def _recover_downstream_bound_complete_item_temps(
    paths: BatchPaths, contract: Mapping[str, Any]
) -> None:
    """Promote only a complete historical receipt bound by immutable children."""

    for temporary in sorted(paths.items_root.glob(".*.receipt.json.tmp")):
        final_name = temporary.name[1:-4]
        prefix = final_name[: -len(".receipt.json")]
        if len(prefix) != 6 or not prefix.isdigit():
            raise FullLocalAnalysisError(f"item receipt temp序号非法：{temporary}")
        ordinal = int(prefix)
        final = paths.items_root / final_name
        if final.exists():
            continue
        try:
            candidate = _read_json(temporary, label="complete item receipt temp")
        except FullLocalAnalysisError:
            # A genuinely truncated latest record is rebuilt from DB/output.
            continue
        intent_path = _item_paths(paths, ordinal)[0]
        if not intent_path.exists() or ordinal > len(contract["processing_order"]):
            raise FullLocalAnalysisError("item receipt temp缺少冻结intent/ordinal")
        intent = _read_json(intent_path, label="temp-bound item intent")
        expected_id = int(contract["processing_order"][ordinal - 1])
        expected_previous = (
            local._sha256_file(_item_paths(paths, ordinal - 1)[1])
            if ordinal > 1
            else None
        )
        if (
            set(candidate)
            != {
                "schema_version",
                "ordinal",
                "batch_index",
                "content_id",
                "status",
                "recovered_after_commit",
                "intent_sha256",
                "previous_item_receipt_sha256",
                "provider_calls",
                "result",
                "failure",
                "after",
            }
            or candidate.get("schema_version") != SCHEMA_VERSION
            or candidate.get("ordinal") != ordinal
            or type(candidate.get("batch_index")) is not int
            or candidate.get("content_id") != expected_id
            or candidate.get("intent_sha256") != local._sha256_file(intent_path)
            or candidate.get("previous_item_receipt_sha256") != expected_previous
            or candidate.get("status") not in ITEM_TERMINAL_STATUSES
            or type(candidate.get("recovered_after_commit")) is not bool
            or type(candidate.get("provider_calls")) is not int
            or candidate.get("provider_calls") != 0
            or not isinstance(candidate.get("result"), Mapping)
            or not isinstance(candidate.get("after"), Mapping)
            or intent.get("content_id") != expected_id
        ):
            raise FullLocalAnalysisError("item receipt temp静态语义漂移")
        candidate_sha = local._sha256_file(temporary)
        bound = False
        progress_path = paths.progress_root / f"{ordinal:06d}.progress.json"
        if progress_path.exists():
            progress = _read_json(progress_path, label="temp-bound progress")
            bound = progress.get("item_receipt_sha256") == candidate_sha
        next_intent = _item_paths(paths, ordinal + 1)[0]
        if next_intent.exists():
            value = _read_json(next_intent, label="temp-bound next intent")
            bound = bound or (
                value.get("previous_item_receipt_sha256") == candidate_sha
            )
        for batch_receipt in paths.batches_root.glob("*.receipt.json"):
            value = _read_json(batch_receipt, label="temp-bound batch receipt")
            bound = bound or [ordinal, candidate_sha] in value.get(
                "item_receipts", []
            )
        if not bound:
            continue
        os.replace(temporary, final)
        local._fsync_directory(paths.items_root)


def _recover_validate_network_ledgers(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    runtime: RuntimeContext,
) -> None:
    _discard_nondurable_network_temps(paths)
    intents, receipts = _paired_chain(paths)
    allowed = {
        f"{index:06d}.network.json" for index in range(1, len(intents) + 1)
    }
    allowed_temps = {f".{name}.tmp" for name in allowed}
    for path in paths.network_root.iterdir():
        if path.name not in allowed | allowed_temps:
            raise FullLocalAnalysisError(f"network root包含未知文件：{path}")
        local._private_file(path, label="network ledger记录")
    with closing(local._immutable_connection(paths.database)) as connection:
        for ordinal, intent_path in enumerate(intents, 1):
            intent = _read_json(intent_path, label="network-bound item intent")
            content_id = int(intent["content_id"])
            ledger_path = _network_path(paths, ordinal)
            receipt_temp = _item_paths(paths, ordinal)[1].with_name(
                f".{_item_paths(paths, ordinal)[1].name}.tmp"
            )
            if not ledger_path.exists():
                if ordinal <= len(receipts) or os.path.lexists(receipt_temp):
                    raise FullLocalAnalysisError(
                        "durable item receipt/final temp缺少network ledger"
                    )
                continue
            try:
                source = _source_snapshot(
                    connection,
                    content_id,
                    source_evidence=contract["source_completion"],
                    row_map=runtime.target_row_map,
                    discovery_raw_cache=runtime.discovery_raw_cache,
                )
            except local.LocalAnalysisCanaryError as exc:
                raise FullLocalAnalysisError(
                    f"network ledger source漂移：{exc}"
                ) from exc
            ledger = local._NetworkLedger(
                ledger_path,
                contract_sha256=runtime.contract_sha256,
                intent_sha256=local._sha256_file(intent_path),
                content_ids=[content_id],
                maximum_bytes=int(intent["network_maximum_bytes"]),
                sources={content_id: source},
                recover_incomplete=ordinal > len(receipts),
            )
            if ordinal > len(receipts) and ledger.value["events"]:
                owned_directory = paths.media_root / str(
                    source["content"]["link_id"]
                )
                if owned_directory.exists():
                    local._private_directory(
                        owned_directory, label="interrupted item owned media dir"
                    )
                    if any(owned_directory.iterdir()):
                        # Non-empty recovery is validated later against exact DB
                        # artifacts; it must never be silently cleaned here.
                        pass
                    else:
                        owned_directory.rmdir()
                        local._fsync_directory(paths.media_root)
            if ordinal <= len(receipts):
                ledger.require_terminal()
                receipt = _read_json(
                    _item_paths(paths, ordinal)[1], label="network-bound receipt"
                )
                if (
                    receipt.get("after", {}).get("network_ledger_sha256")
                    != local._sha256_file(ledger.path)
                ):
                    raise FullLocalAnalysisError(
                        "item receipt未绑定durable network ledger终态"
                    )


def _recover_pending_item_receipt_temp(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    runtime: RuntimeContext,
) -> None:
    """Rebuild one interrupted immutable item receipt from exact materialization."""

    intents = _item_intent_files(paths)
    missing = [
        ordinal
        for ordinal in range(1, len(intents) + 1)
        if not _item_paths(paths, ordinal)[1].exists()
        and os.path.lexists(
            _item_paths(paths, ordinal)[1].with_name(
                f".{_item_paths(paths, ordinal)[1].name}.tmp"
            )
        )
    ]
    if not missing:
        return
    if len(missing) != 1 or missing[0] != len(intents):
        raise FullLocalAnalysisError("item receipt temp缺口不是唯一最新ordinal")
    ordinal = missing[0]
    intent_path = intents[ordinal - 1]
    receipt_path = _item_paths(paths, ordinal)[1]
    temporary = receipt_path.with_name(f".{receipt_path.name}.tmp")
    if receipt_path.exists() or not os.path.lexists(temporary):
        return
    intent = _read_json(intent_path, label="temp-bound item intent")
    content_id = int(intent["content_id"])
    ledger_path = _network_path(paths, ordinal)
    if not ledger_path.exists():
        raise FullLocalAnalysisError("item receipt temp缺少durable network ledger")
    with closing(local._immutable_connection(paths.database)) as connection:
        try:
            source = _source_snapshot(
                connection,
                content_id,
                source_evidence=contract["source_completion"],
                row_map=runtime.target_row_map,
                discovery_raw_cache=runtime.discovery_raw_cache,
            )
        except local.LocalAnalysisCanaryError as exc:
            raise FullLocalAnalysisError(str(exc)) from exc
    ledger = local._NetworkLedger(
        ledger_path,
        contract_sha256=runtime.contract_sha256,
        intent_sha256=local._sha256_file(intent_path),
        content_ids=[content_id],
        maximum_bytes=int(intent["network_maximum_bytes"]),
        sources={content_id: source},
        recover_incomplete=False,
    )
    recovered = _recover_item_receipt(
        paths,
        contract,
        ordinal=ordinal,
        intent=intent,
        intent_path=intent_path,
        source=source,
        ledger=ledger,
        runtime=runtime,
    )
    if recovered is None:
        raise FullLocalAnalysisError(
            "item receipt temp不存在可确定重建的DB/output终态"
        )


def _validate_network_ledgers_read_only(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    runtime: RuntimeContext,
) -> None:
    """Validate existing ledgers without promotion, creation, or interruption."""

    intents, receipts = _paired_chain(paths)
    with closing(local._immutable_connection(paths.database)) as connection:
        for ordinal, intent_path in enumerate(intents, 1):
            ledger_path = _network_path(paths, ordinal)
            if not ledger_path.exists():
                if ordinal <= len(receipts):
                    raise FullLocalAnalysisError(
                        "completed item receipt缺少network ledger"
                    )
                continue
            intent = _read_json(intent_path, label="read-only network intent")
            content_id = int(intent["content_id"])
            try:
                source = _source_snapshot(
                    connection,
                    content_id,
                    source_evidence=contract["source_completion"],
                    row_map=runtime.target_row_map,
                    discovery_raw_cache=runtime.discovery_raw_cache,
                )
            except local.LocalAnalysisCanaryError as exc:
                raise FullLocalAnalysisError(
                    f"read-only network source漂移：{exc}"
                ) from exc
            value = _read_json(ledger_path, label="read-only network ledger")
            validator = object.__new__(local._NetworkLedger)
            validator.path = ledger_path
            validator.contract_sha256 = runtime.contract_sha256
            validator.intent_sha256 = local._sha256_file(intent_path)
            validator.content_ids = [content_id]
            validator.maximum_bytes = int(intent["network_maximum_bytes"])
            validator._source_urls = {
                content_id: {
                    str(url): str(row["host"])
                    for row in source["urls"]
                    for url in [row["url"]]
                    if str(url) in set(local._source_urls(source))
                }
            }
            try:
                validator._validate(value)
            except local.LocalAnalysisCanaryError as exc:
                raise FullLocalAnalysisError(str(exc)) from exc
            if ordinal <= len(receipts):
                receipt = _read_json(
                    receipts[ordinal - 1], label="read-only network receipt"
                )
                if any(
                    event.get("outcome") in {"opening", "opened"}
                    for event in value["events"]
                ) or receipt.get("after", {}).get(
                    "network_ledger_sha256"
                ) != local._sha256_file(ledger_path):
                    raise FullLocalAnalysisError(
                        "completed receipt network ledger终态/SHA漂移"
                    )


def _validate_precontract_prefix(paths: BatchPaths) -> None:
    if any(paths.media_root.iterdir()) or any(paths.fingerprint_root.iterdir()):
        raise FullLocalAnalysisError("copy完成到contract前输出根必须为空")
    allowed_files = {
        paths.local_paths.copy_intent.name,
        paths.local_paths.copy_receipt.name,
        paths.lock.name,
    }
    allowed_dirs = {
        paths.batches_root.name,
        paths.items_root.name,
        paths.progress_root.name,
        paths.completions_root.name,
        paths.network_root.name,
    }
    allowed_temps = {
        f".{paths.local_paths.copy_intent.name}.tmp",
        f".{paths.local_paths.copy_receipt.name}.tmp",
        f".{paths.contract.name}.tmp",
    }
    for path in paths.run_root.iterdir():
        if path.name in allowed_dirs:
            local._private_directory(path, label="precontract记录目录")
            if any(path.iterdir()):
                raise FullLocalAnalysisError("precontract记录子目录必须为空")
        elif path.name in allowed_files | allowed_temps:
            local._private_file(path, label="precontract记录")
        else:
            raise FullLocalAnalysisError(f"precontract prefix含未知文件：{path}")
    if local._database_sidecars(paths.database):
        raise FullLocalAnalysisError("precontract work DB不得存在sidecar")


@contextlib.contextmanager
def _claim(paths: BatchPaths, *, create_roots: bool) -> Iterator[None]:
    if create_roots:
        _prepare_roots(paths)
    else:
        _require_existing_roots(paths)
    flags = os.O_RDWR | (os.O_CREAT if create_roots else 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(paths.lock, flags, 0o600)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise FullLocalAnalysisError("controller lock不是私有普通文件")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise FullLocalAnalysisError("已有累计分析controller运行") from exc
        yield
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _numbered_files(
    root: Path, suffix: str, *, allowed_suffixes: Sequence[str] | None = None
) -> list[Path]:
    allowed = tuple(allowed_suffixes or (suffix,))
    rows: list[tuple[int, Path]] = []
    for path in root.iterdir():
        local._private_file(path, label="累计分析链记录")
        if path.name.startswith(".") and path.name.endswith(".tmp"):
            final_name = path.name[1:-4]
            if any(final_name.endswith(candidate) for candidate in allowed):
                continue
        matched = next(
            (candidate for candidate in allowed if path.name.endswith(candidate)),
            None,
        )
        if matched is None:
            raise FullLocalAnalysisError(f"记录目录包含未知文件：{path}")
        if matched != suffix:
            continue
        prefix = path.name[: -len(suffix)]
        if not prefix.isdigit() or len(prefix) != 6:
            raise FullLocalAnalysisError(f"记录文件序号非法：{path}")
        rows.append((int(prefix), path))
    rows.sort()
    if [index for index, _ in rows] != list(range(1, len(rows) + 1)):
        raise FullLocalAnalysisError(f"记录链序号不连续：{root}")
    return [path for _, path in rows]


def _item_paths(paths: BatchPaths, ordinal: int) -> tuple[Path, Path]:
    stem = f"{ordinal:06d}"
    return (
        paths.items_root / f"{stem}.intent.json",
        paths.items_root / f"{stem}.receipt.json",
    )


def _batch_paths(paths: BatchPaths, index: int) -> tuple[Path, Path]:
    stem = f"{index:06d}"
    return (
        paths.batches_root / f"{stem}.intent.json",
        paths.batches_root / f"{stem}.receipt.json",
    )


def _progress_paths(paths: BatchPaths) -> list[Path]:
    return _numbered_files(paths.progress_root, ".progress.json")


def _receipt_files(paths: BatchPaths) -> list[Path]:
    return _numbered_files(
        paths.items_root,
        ".receipt.json",
        allowed_suffixes=(".intent.json", ".receipt.json"),
    )


def _item_intent_files(paths: BatchPaths) -> list[Path]:
    return _numbered_files(
        paths.items_root,
        ".intent.json",
        allowed_suffixes=(".intent.json", ".receipt.json"),
    )


def _batch_receipt_files(paths: BatchPaths) -> list[Path]:
    return _numbered_files(
        paths.batches_root,
        ".receipt.json",
        allowed_suffixes=(".intent.json", ".receipt.json"),
    )


def _batch_intent_files(paths: BatchPaths) -> list[Path]:
    return _numbered_files(
        paths.batches_root,
        ".intent.json",
        allowed_suffixes=(".intent.json", ".receipt.json"),
    )


def _paired_chain(paths: BatchPaths) -> tuple[list[Path], list[Path]]:
    intents = _item_intent_files(paths)
    receipts = _receipt_files(paths)
    if len(intents) not in {len(receipts), len(receipts) + 1}:
        raise FullLocalAnalysisError("per-item intent/receipt物理链不闭合")
    return intents, receipts


def _complete_record_temp_overrides(
    root: Path, suffix: str
) -> Mapping[int, Path]:
    """Return parseable missing-final temps without mutating the namespace."""

    result: dict[int, Path] = {}
    for candidate in sorted(root.glob(f".*{suffix}.tmp")):
        final_name = candidate.name[1:-4]
        prefix = final_name[: -len(suffix)]
        if (
            len(prefix) != 6
            or not prefix.isdigit()
            or (root / final_name).exists()
        ):
            continue
        try:
            _read_json(candidate, label="read-only complete record temp")
        except FullLocalAnalysisError:
            continue
        result[int(prefix)] = candidate
    return result


def _effective_receipt_paths(
    root: Path,
    suffix: str,
    *,
    intent_count: int,
    overrides: Mapping[int, Path] | None,
) -> list[Path]:
    """Build one receipt prefix, optionally filling a gap from a read-only temp."""

    by_ordinal: dict[int, Path] = {}
    for candidate in root.iterdir():
        if candidate.name.startswith(".") or not candidate.name.endswith(suffix):
            continue
        prefix = candidate.name[: -len(suffix)]
        if len(prefix) != 6 or not prefix.isdigit():
            raise FullLocalAnalysisError(f"receipt记录序号非法：{candidate}")
        local._private_file(candidate, label="receipt链记录")
        by_ordinal[int(prefix)] = candidate
    for ordinal, candidate in (overrides or {}).items():
        if type(ordinal) is not int or ordinal <= 0 or ordinal in by_ordinal:
            raise FullLocalAnalysisError("read-only receipt temp override漂移")
        by_ordinal[ordinal] = candidate
    ordinals = sorted(by_ordinal)
    if ordinals != list(range(1, len(ordinals) + 1)) or len(ordinals) not in {
        intent_count,
        max(0, intent_count - 1),
    }:
        raise FullLocalAnalysisError("intent/receipt有效物理链不闭合")
    return [by_ordinal[ordinal] for ordinal in ordinals]


def _validate_item_chain(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    runtime: RuntimeContext | None = None,
    *,
    receipt_overrides: Mapping[int, Path] | None = None,
) -> list[Mapping[str, Any]]:
    intents = _item_intent_files(paths)
    receipts = _effective_receipt_paths(
        paths.items_root,
        ".receipt.json",
        intent_count=len(intents),
        overrides=receipt_overrides,
    )
    result: list[Mapping[str, Any]] = []
    previous_receipt: str | None = None
    contract_sha = (
        runtime.contract_sha256
        if runtime is not None
        else local._sha256_file(paths.contract)
    )
    for ordinal, receipt_path in enumerate(receipts, 1):
        intent_path = intents[ordinal - 1]
        intent = _read_json(intent_path, label="item intent")
        receipt = _read_json(receipt_path, label="item receipt")
        expected_id = int(contract["processing_order"][ordinal - 1])
        intent_keys = {
            "schema_version",
            "ordinal",
            "batch_index",
            "content_id",
            "contract_sha256",
            "previous_item_receipt_sha256",
            "source_summary",
            "network_maximum_bytes",
            "before",
        }
        receipt_keys = {
            "schema_version",
            "ordinal",
            "batch_index",
            "content_id",
            "status",
            "recovered_after_commit",
            "intent_sha256",
            "previous_item_receipt_sha256",
            "provider_calls",
            "result",
            "failure",
            "after",
        }
        before = intent.get("before")
        target_rows = before.get("target_rows") if isinstance(before, Mapping) else None
        if isinstance(before, Mapping):
            _validate_exact_sequence_mapping(
                before.get("sequences"), label=f"item intent {ordinal} before"
            )
        if isinstance(receipt.get("after"), Mapping):
            _validate_exact_sequence_mapping(
                receipt["after"].get("target_sequences"),
                label=f"item receipt {ordinal} target",
            )
            _validate_exact_sequence_mapping(
                receipt["after"].get("sequences"),
                label=f"item receipt {ordinal} after",
            )
        if (
            set(intent) != intent_keys
            or set(receipt) != receipt_keys
            or intent.get("schema_version") != SCHEMA_VERSION
            or type(intent.get("ordinal")) is not int
            or intent.get("ordinal") != ordinal
            or type(intent.get("batch_index")) is not int
            or type(intent.get("content_id")) is not int
            or intent.get("content_id") != expected_id
            or intent.get("contract_sha256") != contract_sha
            or intent.get("previous_item_receipt_sha256") != previous_receipt
            or intent.get("source_summary")
            != contract["source_summaries"][str(expected_id)]
            or type(intent.get("network_maximum_bytes")) is not int
            or not 0 < intent["network_maximum_bytes"] <= BATCH_DOWNLOAD_CAP_BYTES
            or not isinstance(before, Mapping)
            or set(before)
            != {
                "target_rows",
                "target_rows_sha256",
                "target_sequences",
                "content_direction",
                "sequences",
                "outputs",
                "item_counts",
            }
            or not isinstance(target_rows, Mapping)
            or before.get("target_rows_sha256")
            != _json_sha(target_rows)
            or before.get("target_sequences")
            != _target_sequence_projection(target_rows)
            or receipt.get("intent_sha256") != local._sha256_file(intent_path)
            or type(receipt.get("content_id")) is not int
            or receipt.get("content_id") != expected_id
            or type(receipt.get("ordinal")) is not int
            or receipt.get("ordinal") != ordinal
            or type(receipt.get("batch_index")) is not int
            or receipt.get("batch_index") != intent.get("batch_index")
            or receipt.get("previous_item_receipt_sha256") != previous_receipt
            or receipt.get("status") not in ITEM_TERMINAL_STATUSES
            or type(receipt.get("recovered_after_commit")) is not bool
            or type(receipt.get("provider_calls")) is not int
            or receipt.get("provider_calls") != 0
            or not isinstance(receipt.get("result"), Mapping)
            or not isinstance(receipt.get("after"), Mapping)
            or set(receipt["after"])
            != {
                "target_rows_sha256",
                "target_sequences",
                "content_direction",
                "sequences",
                "outputs",
                "network_ledger_sha256",
                "network_budget_consumed_bytes",
            }
            or not isinstance(receipt["after"].get("target_sequences"), Mapping)
            or type(
                receipt["after"].get("network_budget_consumed_bytes")
            ) is not int
            or receipt["after"]["network_budget_consumed_bytes"] < 0
            or (
                receipt.get("status")
                in {"succeeded", "review_pending", "insufficient_evidence"}
                and receipt.get("failure") is not None
            )
            or (
                receipt.get("status") == "deferred"
                and not isinstance(receipt.get("failure"), Mapping)
            )
            or (
                receipt.get("status")
                in {"succeeded", "review_pending", "insufficient_evidence"}
                and set(receipt["result"])
                != {
                    "content_id",
                    "media",
                    "evaluation",
                    "fingerprint_source_sha256",
                    "network_transcript",
                    "network_transcript_sha256",
                    "validated",
                }
            )
            or (
                receipt.get("status") == "deferred"
                and set(receipt["result"]) != {"deferred", "validated"}
            )
            or (
                isinstance(receipt.get("failure"), Mapping)
                and set(receipt["failure"]) != {"type", "message"}
            )
        ):
            raise FullLocalAnalysisError("per-item intent/receipt语义链漂移")
        previous_receipt = local._sha256_file(receipt_path)
        result.append(receipt)
    return result


def _validate_progress_chain(
    paths: BatchPaths, receipts: Sequence[Mapping[str, Any]]
) -> str | None:
    files = _progress_paths(paths)
    if len(files) > len(receipts):
        raise FullLocalAnalysisError("global progress超前于item receipts")
    previous: str | None = None
    for index, path in enumerate(files, 1):
        value = _read_json(path, label="global progress")
        receipt_path = _item_paths(paths, index)[1]
        if (
            set(value)
            != {
                "schema_version",
                "sequence",
                "previous_progress_sha256",
                "item_receipt_sha256",
                "content_id",
                "status",
            }
            or value.get("schema_version") != SCHEMA_VERSION
            or type(value.get("sequence")) is not int
            or value.get("sequence") != index
            or value.get("previous_progress_sha256") != previous
            or value.get("item_receipt_sha256")
            != local._sha256_file(receipt_path)
            or type(value.get("content_id")) is not int
            or value.get("content_id") != receipts[index - 1]["content_id"]
            or value.get("status") != receipts[index - 1]["status"]
        ):
            raise FullLocalAnalysisError("global progress hash链漂移")
        previous = local._sha256_file(path)
    return previous


def _catch_up_progress(
    paths: BatchPaths, receipts: Sequence[Mapping[str, Any]]
) -> str | None:
    previous = _validate_progress_chain(paths, receipts)
    existing = len(_progress_paths(paths))
    for index in range(existing + 1, len(receipts) + 1):
        receipt = receipts[index - 1]
        receipt_path = _item_paths(paths, index)[1]
        value = {
            "schema_version": SCHEMA_VERSION,
            "sequence": index,
            "previous_progress_sha256": previous,
            "item_receipt_sha256": local._sha256_file(receipt_path),
            "content_id": receipt["content_id"],
            "status": receipt["status"],
        }
        target = paths.progress_root / f"{index:06d}.progress.json"
        previous = _write_exclusive(target, value)
    return previous


def _append_progress_for_receipt(
    paths: BatchPaths, receipt: Mapping[str, Any]
) -> str:
    sequence = int(receipt["ordinal"])
    target = paths.progress_root / f"{sequence:06d}.progress.json"
    receipt_path = _item_paths(paths, sequence)[1]
    previous = (
        local._sha256_file(
            paths.progress_root / f"{sequence - 1:06d}.progress.json"
        )
        if sequence > 1
        else None
    )
    value = {
        "schema_version": SCHEMA_VERSION,
        "sequence": sequence,
        "previous_progress_sha256": previous,
        "item_receipt_sha256": local._sha256_file(receipt_path),
        "content_id": receipt["content_id"],
        "status": receipt["status"],
    }
    return _write_exclusive(target, value)


def _validate_success_state(
    connection: sqlite3.Connection, content_id: int
) -> Mapping[str, Any]:
    evaluation = connection.execute(
        """
        SELECT id,evidence_level,evidence_sha256 FROM evaluation_versions
        WHERE content_id=? AND invalidated_at IS NULL
        ORDER BY id DESC LIMIT 1
        """,
        (content_id,),
    ).fetchone()
    fingerprint = connection.execute(
        """SELECT id,source_sha256 FROM duplicate_fingerprints
           WHERE content_id=? ORDER BY id DESC LIMIT 1""",
        (content_id,),
    ).fetchone()
    if (
        evaluation is None
        or str(evaluation["evidence_level"]) not in {"V2", "V3"}
        or fingerprint is None
    ):
        raise FullLocalAnalysisError(
            f"content {content_id} DB变化未闭合到V2/V3+fingerprint"
        )
    return {
        "evaluation_id": int(evaluation["id"]),
        "evidence_level": str(evaluation["evidence_level"]),
        "evidence_sha256": str(evaluation["evidence_sha256"]),
        "fingerprint_id": int(fingerprint["id"]),
        "fingerprint_source_sha256": str(fingerprint["source_sha256"]),
    }


def _item_baseline_contract(
    intent: Mapping[str, Any], source: Mapping[str, Any]
) -> Mapping[str, Any]:
    before = intent["before"]
    target_rows = before["target_rows"]
    artifact_ids = [
        int(row["id"]) for row in target_rows.get("evidence_artifacts", [])
    ]
    return {
        "sources": [source],
        "baseline_artifact_ids": artifact_ids,
        "baseline_target_rows": target_rows,
        "baseline_target_rows_sha256": _json_sha(target_rows),
        "baseline_sqlite_sequence": before["sequences"],
    }


def _validate_review_pending_evaluation(
    connection: sqlite3.Connection,
    *,
    content_id: int,
    release: sqlite3.Row,
) -> tuple[sqlite3.Row, sqlite3.Row]:
    if (
        type(release["id"]) is not str
        or not release["id"]
        or type(release["rule_version"]) is not str
        or release["rule_version"] != evaluation_module.V8_RULE_VERSION
        or type(release["taxonomy_version"]) is not str
        or type(release["matcher_rule_sha256"]) is not str
        or re.fullmatch(r"[0-9a-f]{64}", release["matcher_rule_sha256"])
        is None
        or type(release["status"]) is not str
        or release["status"] != "active"
    ):
        raise FullLocalAnalysisError(
            "review_pending active release形状无效"
        )
    try:
        runtime = evaluation_module._load_release_runtime(connection, release)
    except Exception as exc:
        raise FullLocalAnalysisError(
            "review_pending active release matcher无法重建"
        ) from exc
    if (
        runtime.matcher is None
        or runtime.matcher.matcher_rule_sha256
        != release["matcher_rule_sha256"]
    ):
        raise FullLocalAnalysisError(
            "review_pending active release matcher闭包漂移"
        )
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
        raise FullLocalAnalysisError(
            f"content {content_id} review_pending active evaluation集合不精确"
        )
    evaluation = rows[0]
    try:
        artifacts, components, evidence_sha256 = (
            evaluation_module._current_evidence_state(
                connection,
                content_id,
                rule_version=str(release["rule_version"]),
            )
        )
    except Exception as exc:
        raise FullLocalAnalysisError(
            f"content {content_id} review_pending无法重算current evidence"
        ) from exc
    envelope = connection.execute(
        "SELECT * FROM evidence_envelopes WHERE id=?",
        (evaluation["evidence_envelope_id"],),
    ).fetchone()
    content = connection.execute(
        "SELECT * FROM content_items WHERE id=?",
        (content_id,),
    ).fetchone()
    if envelope is None or content is None:
        raise FullLocalAnalysisError(
            f"content {content_id} review_pending envelope/content缺失"
        )
    try:
        components_body = json.loads(envelope["components_json"])
        payload = json.loads(evaluation["payload_json"])
    except (TypeError, json.JSONDecodeError) as exc:
        raise FullLocalAnalysisError(
            "review_pending evaluation JSON无效"
        ) from exc
    payload_keys = {
        "evaluation_status",
        "evidence_level",
        "evidence_summary",
        "primary_selling_point_id",
        "selling_point_score",
        "selling_point_included",
        "pending_review",
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
        raise FullLocalAnalysisError(
            "review_pending evaluation payload形状不精确"
        )
    payload_matches = payload["matches"]
    if not isinstance(payload_matches, list) or len(payload_matches) > 3:
        raise FullLocalAnalysisError(
            "review_pending evaluation matches形状无效"
        )
    match_rows = connection.execute(
        """
        SELECT selling_point_code,scene,match_role,score,evidence_json
        FROM evaluation_matches WHERE evaluation_id=?
        ORDER BY CASE match_role WHEN 'primary' THEN 0 ELSE 1 END,rowid
        """,
        (evaluation["id"],),
    ).fetchall()
    if len(match_rows) != len(payload_matches):
        raise FullLocalAnalysisError(
            "review_pending evaluation matches数量漂移"
        )
    for index, (match, row) in enumerate(
        zip(payload_matches, match_rows, strict=True)
    ):
        try:
            evidence = json.loads(row["evidence_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise FullLocalAnalysisError(
                "review_pending evaluation match evidence无效"
            ) from exc
        if (
            not isinstance(match, Mapping)
            or type(match.get("id")) is not str
            or not match["id"]
            or type(match.get("scene")) is not str
            or type(match.get("score")) is not int
            or type(row["selling_point_code"]) is not str
            or row["selling_point_code"] != match["id"]
            or type(row["scene"]) is not str
            or row["scene"] != match["scene"]
            or type(row["match_role"]) is not str
            or row["match_role"]
            != ("primary" if index == 0 else "secondary")
            or type(row["score"]) is not int
            or row["score"] != match["score"]
            or evidence != match
        ):
            raise FullLocalAnalysisError(
                "review_pending evaluation match未精确投影payload"
            )
    primary_match = payload_matches[0] if payload_matches else None
    expected_primary_code = (
        primary_match["id"] if isinstance(primary_match, Mapping) else ""
    )
    expected_primary_score = (
        primary_match["score"] if isinstance(primary_match, Mapping) else None
    )
    audience_score, action_score, valid_commenters = (
        evaluation_module._comment_scores(connection, content_id)
    )
    asr = evaluation_module._read_json(artifacts["asr_path"])
    ocr = evaluation_module._read_json(artifacts["ocr_path"])
    manual_rows = artifacts["manual_rows"]
    manual_text = "\n".join(
        str(row.get("text_value") or "") for row in manual_rows
    )
    manual_visual_text = "\n".join(
        str(row.get("text_value") or "")
        for row in manual_rows
        if row.get("evidence_type")
        in {"visual_summary", "media_observation"}
    )
    manual_desc_text = "\n".join(
        str(row.get("text_value") or "")
        for row in manual_rows
        if row.get("evidence_type")
        not in {"visual_summary", "media_observation"}
    )
    body_text = "\n".join(
        value
        for value in (
            str(content["title"] or ""),
            str(content["body"] or ""),
            manual_text,
        )
        if value
    )
    expected_level, expected_summary = evaluation_module._evidence_level(
        content_type=str(content["content_type"]),
        text=body_text,
        media_path=artifacts["media_path"],
        asr=asr,
        ocr=ocr,
        manual_rows=manual_rows,
    )
    matcher_desc = "\n".join(
        value
        for value in (
            str(content["title"] or ""),
            str(content["body"] or ""),
            manual_desc_text,
        )
        if value
    )
    thresholds = runtime.matcher.thresholds
    if (
        not isinstance(thresholds, Mapping)
        or set(thresholds)
        != {"included_min", "review_min", "max_secondary"}
        or any(type(thresholds[key]) is not int for key in thresholds)
        or not 0 <= thresholds["review_min"] < thresholds["included_min"] <= 100
        or thresholds["max_secondary"] != 2
    ):
        raise FullLocalAnalysisError(
            "review_pending matcher threshold闭包漂移"
        )
    try:
        all_matches = runtime.matcher.match_points(
            {
                "desc": matcher_desc,
                "content_type": content["content_type"],
                "media_type": 4 if content["content_type"] == "video" else 2,
            },
            asr,
            ocr,
            expected_level,
            {"summary": manual_visual_text},
        )
    except Exception as exc:
        raise FullLocalAnalysisError(
            "review_pending matcher重派生失败"
        ) from exc
    expected_matches = (
        [
            all_matches[0],
            *[
                item
                for item in all_matches[1:]
                if type(item.get("score")) is int
                and item["score"] >= thresholds["review_min"]
            ][: thresholds["max_secondary"]],
        ]
        if all_matches
        else []
    )
    primary_scene = (
        str(expected_matches[0].get("scene") or "")
        if expected_matches
        and isinstance(expected_matches[0], Mapping)
        else ""
    )
    normalized_matches: list[Mapping[str, Any]] = []
    if not isinstance(runtime.taxonomy, Mapping) or not isinstance(
        runtime.allowed_scenes, Mapping
    ):
        raise FullLocalAnalysisError(
            "review_pending matcher taxonomy闭包无效"
        )
    for match in expected_matches:
        if not isinstance(match, Mapping):
            raise FullLocalAnalysisError(
                "review_pending matcher match形状无效"
            )
        code = str(match.get("id") or "")
        scene = str(match.get("scene") or primary_scene)
        allowed_scenes = runtime.allowed_scenes.get(code)
        if code not in runtime.taxonomy or not isinstance(
            allowed_scenes, set
        ):
            raise FullLocalAnalysisError(
                "review_pending matcher selling point不在active taxonomy"
            )
        if scene not in allowed_scenes:
            raise FullLocalAnalysisError(
                "review_pending matcher scene不在active taxonomy合同"
            )
        normalized = dict(match)
        normalized["scene"] = scene
        normalized_matches.append(normalized)
    expected_matches = normalized_matches
    expected_primary = expected_matches[0] if expected_matches else None
    if (
        expected_primary is None
        or type(expected_primary.get("id")) is not str
        or type(expected_primary.get("score")) is not int
        or type(expected_primary.get("scene")) is not str
    ):
        raise FullLocalAnalysisError(
            "review_pending matcher未重派生出精确primary"
        )
    expected_score = expected_primary["score"]
    expected_included = expected_score >= thresholds["included_min"]
    expected_pending = (
        thresholds["review_min"]
        <= expected_score
        < thresholds["included_min"]
    )
    if expected_included or not expected_pending:
        raise FullLocalAnalysisError(
            "review_pending matcher结果不在gray threshold"
        )
    expected_direction = expected_primary["scene"]
    if expected_direction not in {"new_car", "used_car", "media"}:
        raise FullLocalAnalysisError(
            "review_pending matcher scene无效"
        )
    expected_content_score = evaluation_module._automotive_score(
        f"{body_text}\n{asr.get('text') or ''}\n{ocr.get('combined_text') or ''}",
        selling_included=expected_included,
    )
    task_fit = expected_score if expected_included else 0
    expected_acquisition = evaluation_module._acquisition_score(
        expected_content_score,
        audience_score,
        task_fit,
        action_score,
    )
    expected_payload = {
        "evaluation_status": "evaluated",
        "evidence_level": expected_level,
        "evidence_summary": expected_summary,
        "primary_selling_point_id": expected_primary["id"],
        "selling_point_score": expected_score,
        "selling_point_included": False,
        "pending_review": True,
        "content_direction": expected_direction,
        "content_automotive_score": expected_content_score,
        "audience_automotive_score": audience_score,
        "action_intent_score": action_score,
        "valid_unique_commenters": valid_commenters,
        "acquisition_potential": expected_acquisition,
        "matches": expected_matches,
        "evaluation_source": "automatic",
        "release_id": release["id"],
    }
    component_columns = (
        "detail_raw_sha256",
        "text_sha256",
        "media_sha256",
        "asr_sha256",
        "ocr_sha256",
        "comments_version_sha256",
        "manual_evidence_sha256",
    )
    evaluation_integer_fields = (
        "id",
        "content_id",
        "evidence_envelope_id",
        "envelope_content_id",
        "selling_point_included",
        "pending_review",
    )
    if (
        any(type(evaluation[field]) is not int for field in evaluation_integer_fields)
        or evaluation["id"] <= 0
        or evaluation["content_id"] != content_id
        or evaluation["envelope_content_id"] != content_id
        or type(evaluation["release_id"]) is not str
        or evaluation["release_id"] != release["id"]
        or type(evaluation["rule_version"]) is not str
        or evaluation["rule_version"] != release["rule_version"]
        or type(evaluation["taxonomy_version"]) is not str
        or evaluation["taxonomy_version"] != release["taxonomy_version"]
        or type(evaluation["matcher_rule_sha256"]) is not str
        or evaluation["matcher_rule_sha256"]
        != release["matcher_rule_sha256"]
        or type(evaluation["evaluation_source"]) is not str
        or evaluation["evaluation_source"] != "automatic"
        or type(evaluation["evaluation_status"]) is not str
        or evaluation["evaluation_status"] != "evaluated"
        or evaluation["parent_evaluation_id"] is not None
        or evaluation["review_id"] is not None
        or evaluation["invalidated_at"] is not None
        or evaluation["invalidation_reason"] is not None
        or type(evaluation["evaluated_at"]) is not str
        or not evaluation["evaluated_at"]
        or type(evaluation["evidence_level"]) is not str
        or evaluation["evidence_level"] not in {"V2", "V3"}
        or evaluation["evidence_level"] != expected_level
        or evaluation["pending_review"] != 1
        or evaluation["selling_point_included"] not in {0, 1}
        or evaluation["selling_point_included"] != int(expected_included)
        or evaluation["primary_selling_point_code"] != expected_primary["id"]
        or evaluation["selling_point_score"] != expected_score
        or evaluation["content_direction"] != expected_direction
        or evaluation["content_automotive_score"] != expected_content_score
        or evaluation["audience_automotive_score"] != audience_score
        or evaluation["acquisition_potential_score"] != expected_acquisition
        or payload != expected_payload
        or evaluation["payload_json"]
        != evaluation_module.canonical_json(expected_payload)
        or payload["pending_review"] is not True
        or type(payload["selling_point_included"]) is not bool
        or int(payload["selling_point_included"])
        != evaluation["selling_point_included"]
        or type(payload["valid_unique_commenters"]) is not int
        or payload["valid_unique_commenters"] != valid_commenters
        or payload["evaluation_status"] != evaluation["evaluation_status"]
        or payload["evidence_level"] != evaluation["evidence_level"]
        or payload["evidence_summary"] != expected_summary
        or payload_matches != expected_matches
        or payload["evaluation_source"] != evaluation["evaluation_source"]
        or payload["release_id"] != evaluation["release_id"]
        or payload["primary_selling_point_id"]
        != (evaluation["primary_selling_point_code"] or "")
        or payload["primary_selling_point_id"] != expected_primary_code
        or payload["selling_point_score"] != evaluation["selling_point_score"]
        or payload["selling_point_score"] != expected_primary_score
        or payload["content_direction"]
        != (evaluation["content_direction"] or "")
        or payload["content_automotive_score"]
        != evaluation["content_automotive_score"]
        or payload["audience_automotive_score"]
        != evaluation["audience_automotive_score"]
        or payload["audience_automotive_score"] != audience_score
        or payload["action_intent_score"] != action_score
        or payload["acquisition_potential"]
        != evaluation["acquisition_potential_score"]
        or type(envelope["content_id"]) is not int
        or envelope["content_id"] != content_id
        or type(envelope["schema_version"]) is not str
        or envelope["schema_version"] != evaluation_module.EVIDENCE_VERSION
        or type(envelope["evidence_sha256"]) is not str
        or envelope["evidence_sha256"] != evidence_sha256
        or evaluation["evidence_sha256"] != evidence_sha256
        or components_body != components
        or any(envelope[column] != components[column] for column in component_columns)
        or content["evaluation_content_direction"]
        != evaluation["content_direction"]
    ):
        raise FullLocalAnalysisError(
            f"content {content_id} review_pending evaluation闭包漂移"
        )
    queue_rows = connection.execute(
        "SELECT * FROM review_queue WHERE content_id=? ORDER BY id",
        (content_id,),
    ).fetchall()
    if len(queue_rows) != 1:
        raise FullLocalAnalysisError(
            f"content {content_id} review_pending queue集合不精确"
        )
    queue = queue_rows[0]
    queue_integer_fields = ("id", "content_id", "evaluation_id", "priority")
    if (
        any(type(queue[field]) is not int for field in queue_integer_fields)
        or queue["id"] <= 0
        or queue["content_id"] != content_id
        or queue["evaluation_id"] != evaluation["id"]
        or type(queue["reason_code"]) is not str
        or queue["reason_code"] != "evaluation_gray_zone"
        or queue["priority"] != 50
        or type(queue["status"]) is not str
        or queue["status"] != "pending"
        or queue["assigned_to"] is not None
        or queue["resolved_at"] is not None
        or type(queue["created_at"]) is not str
        or not queue["created_at"]
        or type(queue["updated_at"]) is not str
        or not queue["updated_at"]
        or queue["created_at"] != queue["updated_at"]
        or connection.execute(
            "SELECT COUNT(*) FROM review_reopen_events WHERE queue_id=?",
            (queue["id"],),
        ).fetchone()[0]
        != 0
    ):
        raise FullLocalAnalysisError(
            f"content {content_id} review_pending queue闭包漂移"
        )
    return evaluation, queue


def _validate_review_pending_result(
    connection: sqlite3.Connection,
    *,
    content_id: int,
    source: Mapping[str, Any],
    result: Mapping[str, Any],
    artifacts: Mapping[str, sqlite3.Row],
    evaluation: sqlite3.Row,
    ledger: local._NetworkLedger,
) -> None:
    kind = source["artifact_body"]["media_kind"]
    expected_media_artifacts = (
        {
            "media": artifacts["media"]["id"],
            "frames": artifacts["frames_manifest"]["id"],
            "asr": artifacts["asr"]["id"],
            "ocr": artifacts["ocr"]["id"],
        }
        if kind == "video"
        else {
            "media": artifacts["media_manifest"]["id"],
            "ocr": artifacts["ocr"]["id"],
        }
    )
    expected_media = {
        "content_id": content_id,
        "status": "evidence_ready",
        "media_kind": kind,
        "artifacts": expected_media_artifacts,
    }
    transcript = ledger.transcript(content_id)
    evaluation_result = result.get("evaluation")
    if (
        set(result)
        != {
            "content_id",
            "media",
            "evaluation",
            "fingerprint_source_sha256",
            "network_transcript",
            "network_transcript_sha256",
        }
        or type(result["content_id"]) is not int
        or result["content_id"] != content_id
        or result["media"] != expected_media
        or not isinstance(evaluation_result, Mapping)
        or set(evaluation_result)
        != {"evaluation_id", "evidence_level", "evidence_sha256", "created"}
        or type(evaluation_result["evaluation_id"]) is not int
        or evaluation_result["evaluation_id"] != evaluation["id"]
        or evaluation_result["evidence_level"] != evaluation["evidence_level"]
        or evaluation_result["evidence_sha256"] != evaluation["evidence_sha256"]
        or type(evaluation_result["created"]) is not bool
        or evaluation_result["created"] is not True
        or result["network_transcript"] != transcript
        or result["network_transcript_sha256"] != _json_sha(transcript)
    ):
        raise FullLocalAnalysisError(
            f"content {content_id} review_pending result未精确投影DB/network"
        )
    try:
        _inputs, fingerprint_source = duplicates_module._current_source_state(
            connection, content_id
        )
    except Exception as exc:
        raise FullLocalAnalysisError(
            f"content {content_id} review_pending fingerprint source无法重算"
        ) from exc
    if result["fingerprint_source_sha256"] != fingerprint_source:
        raise FullLocalAnalysisError(
            f"content {content_id} review_pending fingerprint result漂移"
        )


def _validate_item_review_pending_strong(
    paths: BatchPaths,
    *,
    intent: Mapping[str, Any],
    source: Mapping[str, Any],
    result: Mapping[str, Any],
    ledger: local._NetworkLedger,
) -> Mapping[str, Any]:
    content_id = int(intent["content_id"])
    mini = _item_baseline_contract(intent, source)
    with closing(local._immutable_connection(paths.database)) as connection:
        releases = connection.execute(
            "SELECT * FROM evaluation_releases WHERE status='active' ORDER BY id"
        ).fetchall()
        if (
            len(releases) != 1
            or type(releases[0]["id"]) is not str
            or not releases[0]["id"]
        ):
            raise FullLocalAnalysisError(
                "review_pending要求唯一active release"
            )
        release_id = releases[0]["id"]
        eligible = local.evaluation_selectors_module.formal_eligible_release_evaluations(
            connection, release_id, [content_id]
        )
        if set(eligible):
            raise FullLocalAnalysisError(
                "review_pending evaluation不得进入formal selector"
            )
        evaluation, queue = _validate_review_pending_evaluation(
            connection,
            content_id=content_id,
            release=releases[0],
        )
        artifacts_by_content, media_files, fingerprint_files = (
            local._validate_generated_artifacts(
                connection,
                contract=mini,
                paths=paths.local_paths,
                content_ids=[content_id],
            )
        )
        artifacts = artifacts_by_content[content_id]
        local._validate_download_provenance(
            content_id=content_id,
            source=source,
            artifacts=artifacts,
            ledger=ledger,
        )
        extra_media, extra_fingerprints = local._validate_content_processing(
            connection,
            source=source,
            artifacts=artifacts,
            paths=paths.local_paths,
            slot_attempt_expectations={},
        )
        media_files.update(extra_media)
        fingerprint_files.update(extra_fingerprints)
        current_rows = local._target_rows(connection, [content_id])
        queue_rows = current_rows.get("review_queue")
        baseline_rows = mini["baseline_target_rows"]
        if (
            not isinstance(queue_rows, list)
            or len(queue_rows) != 1
            or not isinstance(baseline_rows, Mapping)
            or baseline_rows.get("review_queue") != []
        ):
            raise FullLocalAnalysisError(
                "review_pending queue不是当前item唯一新增行"
            )
        adjusted_baseline = {
            table: list(rows) for table, rows in baseline_rows.items()
        }
        adjusted_baseline["review_queue"] = list(queue_rows)
        adjusted_contract = {
            **mini,
            "baseline_target_rows": adjusted_baseline,
            "baseline_target_rows_sha256": _json_sha(adjusted_baseline),
        }
        local._validate_target_baseline_and_sequences(
            connection,
            contract=adjusted_contract,
            content_ids=[content_id],
            active_evaluation_ids={evaluation["id"]},
        )
        _validate_review_pending_result(
            connection,
            content_id=content_id,
            source=source,
            result=result,
            artifacts=artifacts,
            evaluation=evaluation,
            ledger=ledger,
        )
    actual = _item_output_inventory(paths, source)
    link_id = source["content"]["link_id"]
    actual_media = {
        paths.media_root / link_id / row["path"]
        for row in actual["media"]["rows"]
    }
    actual_fingerprints = {
        paths.fingerprint_root / row["path"]
        for row in actual["fingerprints"]["rows"]
    }
    if actual_media != media_files or actual_fingerprints != fingerprint_files:
        raise FullLocalAnalysisError(
            "review_pending输出不等于DB/manifest精确可达闭包"
        )
    return {
        "formal_eligible": False,
        "review_pending": True,
        "evaluation_id": evaluation["id"],
        "queue_id": queue["id"],
        "media_files": len(media_files),
        "fingerprint_files": len(fingerprint_files),
        "target_rows_sha256": _json_sha(
            _item_target_rows(paths.database, content_id)
        ),
    }


def _validate_insufficient_media_processing(
    connection: sqlite3.Connection,
    *,
    source: Mapping[str, Any],
    artifacts: Mapping[str, sqlite3.Row],
    paths: local.CanaryPaths,
    ledger: local._NetworkLedger,
) -> set[Path]:
    """Validate the complete media prefix while forbidding fingerprint work."""

    content_id = int(source["content"]["id"])
    media_kind = str(source["artifact_body"]["media_kind"])
    expected_artifact_types = (
        {"media", "frames_manifest", "asr", "ocr"}
        if media_kind == "video"
        else {"media_manifest", "ocr"}
    )
    if set(artifacts) != expected_artifact_types:
        raise FullLocalAnalysisError(
            f"content {content_id} insufficient media artifact集合不精确"
        )
    versions = media_module.processor_versions()
    expected_artifact_versions = (
        {
            "media": "provider-media-v8.0",
            "frames_manifest": str(versions["frames"]),
            "asr": str(versions["asr"]),
            "ocr": str(versions["ocr"]),
        }
        if media_kind == "video"
        else {
            "media_manifest": media_module.IMAGE_DOWNLOAD_VERSION,
            "ocr": str(versions["ocr"]),
        }
    )
    if any(
        str(artifacts[name]["processor_version"] or "") != version
        for name, version in expected_artifact_versions.items()
    ):
        raise FullLocalAnalysisError(
            f"content {content_id} insufficient media processor version漂移"
        )
    expected_slot_types = (
        {"download", "frames", "asr", "ocr"}
        if media_kind == "video"
        else {"download", "ocr"}
    )
    slots = connection.execute(
        "SELECT * FROM media_processing_slots WHERE content_id=? ORDER BY id",
        (content_id,),
    ).fetchall()
    if len(slots) != len(expected_slot_types) or {
        str(row["processor_type"]) for row in slots
    } != expected_slot_types:
        raise FullLocalAnalysisError(
            f"content {content_id} insufficient media slot集合不精确"
        )
    slot_by_type = {str(row["processor_type"]): row for row in slots}
    media_artifact = artifacts[
        "media" if media_kind == "video" else "media_manifest"
    ]
    frames_artifact = artifacts.get("frames_manifest")
    ocr_source_artifact = (
        frames_artifact if media_kind == "video" else media_artifact
    )
    if ocr_source_artifact is None:
        raise FullLocalAnalysisError(
            f"content {content_id} insufficient OCR source artifact缺失"
        )
    expected_slots: dict[str, tuple[str, str, int]] = {
        "download": (
            (
                str(source["artifact_body"]["source_sha256"])
                if media_kind == "video"
                else local._source_image_download_binding(source)
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
        if (
            str(row["status"]) != "succeeded"
            or str(row["source_sha256"]) != source_sha
            or str(row["processor_version"]) != version
            or int(row["output_artifact_id"] or -1) != artifact_id
            or int(row["attempt_count"] or 0) != 1
            or row["error_message"] not in (None, "")
        ):
            raise FullLocalAnalysisError(
                f"content {content_id} insufficient media slot闭包漂移"
            )
    local._validate_download_provenance(
        content_id=content_id,
        source=source,
        artifacts=artifacts,
        ledger=ledger,
    )
    media_files = {
        local._resolve_artifact_path(str(row["local_path"]))
        for row in artifacts.values()
    }
    if media_kind == "video":
        media_files.update(
            local._manifest_output_paths(
                artifacts["frames_manifest"],
                media_kind="video",
                media_root=paths.media_root,
            )
        )
        _, asr_body = local._json_artifact(
            artifacts["asr"], label="insufficient ASR artifact"
        )
        if asr_body.get("status") not in {"success", "unavailable"}:
            raise FullLocalAnalysisError(
                "insufficient ASR artifact不是允许终态"
            )
    else:
        media_files.update(
            local._manifest_output_paths(
                artifacts["media_manifest"],
                media_kind="image",
                media_root=paths.media_root,
                source=source,
            )
        )
    _, ocr_body = local._json_artifact(
        artifacts["ocr"], label="insufficient OCR artifact"
    )
    if ocr_body.get("status") != "success":
        raise FullLocalAnalysisError(
            "insufficient OCR artifact不是success终态"
        )
    return media_files


def _validate_insufficient_target_baseline_and_sequences(
    connection: sqlite3.Connection,
    *,
    intent: Mapping[str, Any],
    source: Mapping[str, Any],
    evaluation_id: int,
) -> None:
    before = intent["before"]
    baseline = before["target_rows"]
    current = local._target_rows(
        connection, [int(intent["content_id"])]
    )
    deltas: dict[str, list[Mapping[str, Any]]] = {}
    for table in sorted(local.MANAGED_TARGET_TABLES):
        baseline_rows = baseline.get(table)
        current_rows = current.get(table)
        if not isinstance(baseline_rows, list) or not isinstance(
            current_rows, list
        ):
            raise FullLocalAnalysisError(
                f"insufficient target baseline缺少表：{table}"
            )
        remaining = Counter(
            local._canonical_bytes(row) for row in current_rows
        )
        for row in baseline_rows:
            key = local._canonical_bytes(row)
            if remaining[key] <= 0:
                raise FullLocalAnalysisError(
                    f"insufficient target baseline row被改写或删除：{table}"
                )
            remaining[key] -= 1
        deltas[table] = [
            json.loads(key)
            for key, count in remaining.items()
            for _ in range(count)
        ]
    media_kind = str(source["artifact_body"]["media_kind"])
    expected_media_rows = 4 if media_kind == "video" else 2
    if (
        len(deltas["media_processing_slots"]) != expected_media_rows
        or len(deltas["evidence_artifacts"]) != expected_media_rows
        or len(deltas["evaluation_versions"]) != 1
        or int(deltas["evaluation_versions"][0].get("id") or -1)
        != evaluation_id
        or len(deltas["evidence_envelopes"]) != 1
        or deltas["evaluation_matches"]
        or deltas["review_queue"]
        or deltas["review_reopen_events"]
        or deltas["duplicate_fingerprints"]
    ):
        raise FullLocalAnalysisError(
            "insufficient target managed rows出现非预期增删改"
        )
    baseline_sequences = before["sequences"]
    current_sequences = _sequence_snapshot(connection)
    managed_with_ids = local.MANAGED_TARGET_TABLES - {"evaluation_matches"}
    for table, sequence in current_sequences.items():
        baseline_sequence = int(baseline_sequences.get(table, 0))
        if table not in managed_with_ids:
            if table not in baseline_sequences or sequence != baseline_sequence:
                raise FullLocalAnalysisError(
                    "insufficient 非managed sqlite_sequence发生变化"
                )
            continue
        maximum_id = int(
            connection.execute(
                f'SELECT COALESCE(MAX(id),0) FROM "{table}"'
            ).fetchone()[0]
        )
        if sequence != max(baseline_sequence, maximum_id):
            raise FullLocalAnalysisError(
                f"insufficient managed sqlite_sequence非精确增量：{table}"
            )
    if any(name not in current_sequences for name in baseline_sequences):
        raise FullLocalAnalysisError(
            "insufficient sqlite_sequence baseline行消失"
        )


def _validate_insufficient_evaluation(
    connection: sqlite3.Connection,
    *,
    intent: Mapping[str, Any],
    content_id: int,
    release: sqlite3.Row,
) -> sqlite3.Row:
    if (
        type(release["id"]) is not str
        or not release["id"]
        or type(release["rule_version"]) is not str
        or release["rule_version"]
        not in {
            evaluation_module.V8_RULE_VERSION,
            evaluation_module.V9_RULE_VERSION,
        }
        or type(release["taxonomy_version"]) is not str
        or type(release["matcher_rule_sha256"]) is not str
        or re.fullmatch(r"[0-9a-f]{64}", release["matcher_rule_sha256"])
        is None
        or release["status"] != "active"
    ):
        raise FullLocalAnalysisError(
            "insufficient_evidence active release形状无效"
        )
    try:
        runtime = evaluation_module._load_release_runtime(connection, release)
    except Exception as exc:
        raise FullLocalAnalysisError(
            "insufficient_evidence active evaluator无法重建"
        ) from exc
    if (
        runtime.matcher is None
        or runtime.matcher.matcher_rule_sha256
        != release["matcher_rule_sha256"]
    ):
        raise FullLocalAnalysisError(
            "insufficient_evidence active evaluator闭包漂移"
        )
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
        raise FullLocalAnalysisError(
            f"content {content_id} insufficient active evaluation集合不精确"
        )
    evaluation = rows[0]
    try:
        artifacts, components, evidence_sha256 = (
            evaluation_module._current_evidence_state(
                connection,
                content_id,
                rule_version=str(release["rule_version"]),
            )
        )
    except Exception as exc:
        raise FullLocalAnalysisError(
            f"content {content_id} insufficient current evidence无法重算"
        ) from exc
    envelope = connection.execute(
        "SELECT * FROM evidence_envelopes WHERE id=?",
        (evaluation["evidence_envelope_id"],),
    ).fetchone()
    content = connection.execute(
        "SELECT * FROM content_items WHERE id=?", (content_id,)
    ).fetchone()
    if envelope is None or content is None:
        raise FullLocalAnalysisError(
            f"content {content_id} insufficient envelope/content缺失"
        )
    try:
        components_body = json.loads(str(envelope["components_json"]))
        payload = json.loads(str(evaluation["payload_json"]))
    except (TypeError, json.JSONDecodeError) as exc:
        raise FullLocalAnalysisError(
            "insufficient evaluation JSON无效"
        ) from exc
    payload_keys = {
        "evaluation_status",
        "evidence_level",
        "evidence_summary",
        "primary_selling_point_id",
        "selling_point_score",
        "selling_point_included",
        "pending_review",
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
    asr = evaluation_module._read_json(artifacts["asr_path"])
    ocr = evaluation_module._read_json(artifacts["ocr_path"])
    manual_rows = artifacts["manual_rows"]
    manual_text = "\n".join(
        str(row.get("text_value") or "") for row in manual_rows
    )
    body_text = "\n".join(
        value
        for value in (
            str(content["title"] or ""),
            str(content["body"] or ""),
            manual_text,
        )
        if value
    )
    expected_level, expected_summary = evaluation_module._evidence_level(
        content_type=str(content["content_type"]),
        text=body_text,
        media_path=artifacts["media_path"],
        asr=asr,
        ocr=ocr,
        manual_rows=manual_rows,
    )
    before_direction = intent["before"]["content_direction"]
    expected_direction = str(before_direction or "unknown")
    if expected_direction not in {"new_car", "used_car", "media", "other"}:
        expected_direction = "unknown"
    audience_score, action_score, valid_commenters = (
        evaluation_module._comment_scores(connection, content_id)
    )
    acquisition_score = evaluation_module._acquisition_score(
        None, audience_score, None, action_score
    )
    expected_pending_review = (
        release["rule_version"] == evaluation_module.V8_RULE_VERSION
    )
    expected_payload = {
        "evaluation_status": "insufficient_evidence",
        "evidence_level": expected_level,
        "evidence_summary": expected_summary,
        "primary_selling_point_id": "",
        "selling_point_score": None,
        "selling_point_included": False,
        "pending_review": expected_pending_review,
        "content_direction": expected_direction,
        "content_automotive_score": None,
        "audience_automotive_score": audience_score,
        "action_intent_score": action_score,
        "valid_unique_commenters": valid_commenters,
        "acquisition_potential": acquisition_score,
        "matches": [],
        "evaluation_source": "automatic",
        "release_id": release["id"],
    }
    component_columns = (
        "detail_raw_sha256",
        "text_sha256",
        "media_sha256",
        "asr_sha256",
        "ocr_sha256",
        "comments_version_sha256",
        "manual_evidence_sha256",
    )
    evaluation_matches = connection.execute(
        "SELECT COUNT(*) FROM evaluation_matches WHERE evaluation_id=?",
        (evaluation["id"],),
    ).fetchone()[0]
    queue_count = connection.execute(
        "SELECT COUNT(*) FROM review_queue WHERE content_id=?",
        (content_id,),
    ).fetchone()[0]
    reopen_count = connection.execute(
        """
        SELECT COUNT(*) FROM review_reopen_events r
        JOIN review_queue q ON q.id=r.queue_id WHERE q.content_id=?
        """,
        (content_id,),
    ).fetchone()[0]
    fingerprint_count = connection.execute(
        "SELECT COUNT(*) FROM duplicate_fingerprints WHERE content_id=?",
        (content_id,),
    ).fetchone()[0]
    if (
        not isinstance(payload, Mapping)
        or set(payload) != payload_keys
        or expected_level not in {"V0", "V1"}
        or type(evaluation["id"]) is not int
        or evaluation["id"] <= 0
        or type(evaluation["content_id"]) is not int
        or evaluation["content_id"] != content_id
        or evaluation["envelope_content_id"] != content_id
        or evaluation["release_id"] != release["id"]
        or evaluation["rule_version"] != release["rule_version"]
        or evaluation["taxonomy_version"] != release["taxonomy_version"]
        or evaluation["matcher_rule_sha256"]
        != release["matcher_rule_sha256"]
        or evaluation["evaluation_source"] != "automatic"
        or evaluation["evaluation_status"] != "insufficient_evidence"
        or evaluation["parent_evaluation_id"] is not None
        or evaluation["review_id"] is not None
        or evaluation["invalidated_at"] is not None
        or evaluation["invalidation_reason"] is not None
        or type(evaluation["evaluated_at"]) is not str
        or not evaluation["evaluated_at"]
        or evaluation["evidence_level"] != expected_level
        or evaluation["evidence_sha256"] != evidence_sha256
        or type(evaluation["pending_review"]) is not int
        or evaluation["pending_review"] != int(expected_pending_review)
        or type(evaluation["selling_point_included"]) is not int
        or evaluation["selling_point_included"] != 0
        or evaluation["primary_selling_point_code"] is not None
        or evaluation["selling_point_score"] is not None
        or evaluation["content_automotive_score"] is not None
        or evaluation["content_direction"] != expected_direction
        or evaluation["audience_automotive_score"] != audience_score
        or evaluation["acquisition_potential_score"] != acquisition_score
        or payload != expected_payload
        or evaluation["payload_json"]
        != evaluation_module.canonical_json(expected_payload)
        or payload["pending_review"] is not expected_pending_review
        or payload["selling_point_included"] is not False
        or envelope["schema_version"] != evaluation_module.EVIDENCE_VERSION
        or envelope["content_id"] != content_id
        or envelope["evidence_sha256"] != evidence_sha256
        or components_body != components
        or any(envelope[column] != components[column] for column in component_columns)
        or (
            release["rule_version"] == evaluation_module.V9_RULE_VERSION
            and (
                artifacts["manual_rows"]
                or components["manual_evidence_sha256"] is not None
                or envelope["manual_evidence_sha256"] is not None
            )
        )
        or content["evaluation_content_direction"] != expected_direction
        or evaluation_matches != 0
        or queue_count != 0
        or reopen_count != 0
        or fingerprint_count != 0
    ):
        raise FullLocalAnalysisError(
            f"content {content_id} insufficient evaluation闭包漂移"
        )
    return evaluation


def _validate_insufficient_result(
    *,
    content_id: int,
    source: Mapping[str, Any],
    result: Mapping[str, Any],
    artifacts: Mapping[str, sqlite3.Row],
    evaluation: sqlite3.Row,
    ledger: local._NetworkLedger,
) -> None:
    kind = str(source["artifact_body"]["media_kind"])
    expected_media_artifacts = (
        {
            "media": int(artifacts["media"]["id"]),
            "frames": int(artifacts["frames_manifest"]["id"]),
            "asr": int(artifacts["asr"]["id"]),
            "ocr": int(artifacts["ocr"]["id"]),
        }
        if kind == "video"
        else {
            "media": int(artifacts["media_manifest"]["id"]),
            "ocr": int(artifacts["ocr"]["id"]),
        }
    )
    evaluation_result = result.get("evaluation")
    transcript = ledger.transcript(content_id)
    if (
        set(result)
        != {
            "content_id",
            "media",
            "evaluation",
            "fingerprint_source_sha256",
            "network_transcript",
            "network_transcript_sha256",
        }
        or result.get("content_id") != content_id
        or result.get("media")
        != {
            "content_id": content_id,
            "status": "evidence_ready",
            "media_kind": kind,
            "artifacts": expected_media_artifacts,
        }
        or not isinstance(evaluation_result, Mapping)
        or set(evaluation_result)
        != {"evaluation_id", "evidence_level", "evidence_sha256", "created"}
        or evaluation_result["evaluation_id"] != evaluation["id"]
        or evaluation_result["evidence_level"] != evaluation["evidence_level"]
        or evaluation_result["evidence_sha256"]
        != evaluation["evidence_sha256"]
        or evaluation_result["created"] is not True
        or result.get("fingerprint_source_sha256") is not None
        or result.get("network_transcript") != transcript
        or result.get("network_transcript_sha256") != _json_sha(transcript)
    ):
        raise FullLocalAnalysisError(
            f"content {content_id} insufficient result未精确投影DB/network"
        )


def _validate_item_insufficient_evidence_strong(
    paths: BatchPaths,
    *,
    intent: Mapping[str, Any],
    source: Mapping[str, Any],
    result: Mapping[str, Any],
    ledger: local._NetworkLedger,
) -> Mapping[str, Any]:
    content_id = int(intent["content_id"])
    mini = _item_baseline_contract(intent, source)
    with closing(local._immutable_connection(paths.database)) as connection:
        releases = connection.execute(
            "SELECT * FROM evaluation_releases WHERE status='active' ORDER BY id"
        ).fetchall()
        if len(releases) != 1:
            raise FullLocalAnalysisError(
                "insufficient_evidence要求唯一active release"
            )
        release = releases[0]
        eligible = local.evaluation_selectors_module.formal_eligible_release_evaluations(
            connection, str(release["id"]), [content_id]
        )
        if set(eligible):
            raise FullLocalAnalysisError(
                "insufficient_evidence不得进入formal selector"
            )
        evaluation = _validate_insufficient_evaluation(
            connection,
            intent=intent,
            content_id=content_id,
            release=release,
        )
        artifacts_by_content, media_files, fingerprint_files = (
            local._validate_generated_artifacts(
                connection,
                contract=mini,
                paths=paths.local_paths,
                content_ids=[content_id],
            )
        )
        artifacts = artifacts_by_content[content_id]
        media_files.update(
            _validate_insufficient_media_processing(
                connection,
                source=source,
                artifacts=artifacts,
                paths=paths.local_paths,
                ledger=ledger,
            )
        )
        if fingerprint_files:
            raise FullLocalAnalysisError(
                "insufficient_evidence不得产生fingerprint artifact"
            )
        _validate_insufficient_target_baseline_and_sequences(
            connection,
            intent=intent,
            source=source,
            evaluation_id=int(evaluation["id"]),
        )
        _validate_insufficient_result(
            content_id=content_id,
            source=source,
            result=result,
            artifacts=artifacts,
            evaluation=evaluation,
            ledger=ledger,
        )
    actual = _item_output_inventory(paths, source)
    link_id = str(source["content"]["link_id"])
    actual_media = {
        paths.media_root / link_id / str(row["path"])
        for row in actual["media"]["rows"]
    }
    actual_fingerprints = {
        paths.fingerprint_root / str(row["path"])
        for row in actual["fingerprints"]["rows"]
    }
    if actual_media != media_files or actual_fingerprints:
        raise FullLocalAnalysisError(
            "insufficient_evidence输出不等于media-only精确可达闭包"
        )
    return {
        "formal_eligible": False,
        "insufficient_evidence": True,
        "evaluation_id": int(evaluation["id"]),
        "evidence_level": str(evaluation["evidence_level"]),
        "evidence_sha256": str(evaluation["evidence_sha256"]),
        "media_files": len(media_files),
        "fingerprint_files": 0,
        "target_rows_sha256": _json_sha(
            _item_target_rows(paths.database, content_id)
        ),
    }


def _validate_item_success_strong(
    paths: BatchPaths,
    *,
    intent: Mapping[str, Any],
    source: Mapping[str, Any],
    result: Mapping[str, Any],
    ledger: local._NetworkLedger,
) -> Mapping[str, Any]:
    content_id = int(intent["content_id"])
    mini = _item_baseline_contract(intent, source)
    with closing(local._immutable_connection(paths.database)) as connection:
        release = connection.execute(
            "SELECT id,rule_version FROM evaluation_releases WHERE status='active'"
        ).fetchone()
        if release is None:
            raise FullLocalAnalysisError("item success缺少active release")
        eligible = local.evaluation_selectors_module.formal_eligible_release_evaluations(
            connection, str(release["id"]), [content_id]
        )
        if set(eligible) != {content_id}:
            raise FullLocalAnalysisError("item evaluation未达到formal eligible")
        artifacts_by_content, media_files, fingerprint_files = (
            local._validate_generated_artifacts(
                connection,
                contract=mini,
                paths=paths.local_paths,
                content_ids=[content_id],
            )
        )
        artifacts = artifacts_by_content[content_id]
        local._validate_download_provenance(
            content_id=content_id,
            source=source,
            artifacts=artifacts,
            ledger=ledger,
        )
        extra_media, extra_fingerprints = local._validate_content_processing(
            connection,
            source=source,
            artifacts=artifacts,
            paths=paths.local_paths,
            slot_attempt_expectations={},
        )
        media_files.update(extra_media)
        fingerprint_files.update(extra_fingerprints)
        evaluation = local._validate_current_evaluation(
            connection,
            content_id=content_id,
            release_id=str(release["id"]),
            rule_version=str(release["rule_version"]),
        )
        local._validate_target_baseline_and_sequences(
            connection,
            contract=mini,
            content_ids=[content_id],
            active_evaluation_ids={int(evaluation["id"])},
        )
    local._validate_processed_results(
        paths.local_paths, mini, [content_id], [result]
    )
    actual = _item_output_inventory(paths, source)
    link_id = str(source["content"]["link_id"])
    actual_media = {
        paths.media_root / link_id / str(row["path"])
        for row in actual["media"]["rows"]
    }
    actual_fingerprints = {
        paths.fingerprint_root / str(row["path"])
        for row in actual["fingerprints"]["rows"]
    }
    if actual_media != media_files or actual_fingerprints != fingerprint_files:
        raise FullLocalAnalysisError("item输出不等于DB/manifest精确可达闭包")
    return {
        "formal_eligible": True,
        "evaluation_id": int(evaluation["id"]),
        "media_files": len(media_files),
        "fingerprint_files": len(fingerprint_files),
        "target_rows_sha256": _json_sha(
            _item_target_rows(paths.database, content_id)
        ),
    }


def _item_target_rows(path: Path, content_id: int) -> Mapping[str, Any]:
    with closing(local._immutable_connection(path)) as connection:
        return local._target_rows(connection, [content_id])


def _target_sequence_projection(
    rows: Mapping[str, Sequence[Mapping[str, Any]]],
) -> Mapping[str, int]:
    return {
        table: max(
            (int(row["id"]) for row in values if "id" in row),
            default=0,
        )
        for table, values in sorted(rows.items())
        if any("id" in row for row in values)
    }


def _content_direction(path: Path, content_id: int) -> Any:
    with closing(local._immutable_connection(path)) as connection:
        row = connection.execute(
            "SELECT evaluation_content_direction FROM content_items WHERE id=?",
            (content_id,),
        ).fetchone()
    if row is None:
        raise FullLocalAnalysisError(f"content {content_id} 不存在")
    return local._json_value(row["evaluation_content_direction"])


def _validate_item_deferred_exact(
    paths: BatchPaths,
    *,
    intent: Mapping[str, Any],
    source: Mapping[str, Any],
    ledger: local._NetworkLedger,
    require_global_sequence_head: bool = True,
) -> Mapping[str, Any]:
    content_id = int(intent["content_id"])
    before_rows = intent["before"]["target_rows"]

    def exact_delta(
        table: str,
        current_rows: Sequence[Mapping[str, Any]],
    ) -> list[Mapping[str, Any]]:
        remaining = Counter(local._canonical_bytes(row) for row in current_rows)
        for row in before_rows[table]:
            key = local._canonical_bytes(row)
            if remaining[key] <= 0:
                raise FullLocalAnalysisError(
                    f"deferred baseline row被改写或删除：{table}"
                )
            remaining[key] -= 1
        return [
            json.loads(key)
            for key, count in remaining.items()
            for _ in range(count)
        ]

    with closing(local._immutable_connection(paths.database)) as connection:
        current = local._target_rows(connection, [content_id])
        deltas = {
            table: exact_delta(table, current[table])
            for table in sorted(current)
        }
        forbidden = (
            "evaluation_versions",
            "evidence_envelopes",
            "duplicate_fingerprints",
            "evaluation_matches",
            "review_queue",
            "review_reopen_events",
        )
        if any(deltas[table] for table in forbidden):
            raise FullLocalAnalysisError(
                "deferred item不得写evaluation/fingerprint/review行"
            )
        if (
            _content_direction(paths.database, content_id)
            != intent["before"]["content_direction"]
        ):
            raise FullLocalAnalysisError("deferred item不得改写content direction")

        mini_contract = _item_baseline_contract(intent, source)
        try:
            artifacts_by_content, media_files, fingerprint_files = (
                local._validate_generated_artifacts(
                    connection,
                    contract=mini_contract,
                    paths=paths,
                    content_ids=[content_id],
                )
            )
        except local.LocalAnalysisCanaryError as exc:
            raise FullLocalAnalysisError(str(exc)) from exc
        artifacts = artifacts_by_content[content_id]
        kind = str(source["artifact_body"]["media_kind"])
        stages = (
            [
                ("download", "media"),
                ("frames", "frames_manifest"),
                ("asr", "asr"),
                ("ocr", "ocr"),
            ]
            if kind == "video"
            else [("download", "media_manifest"), ("ocr", "ocr")]
        )
        artifact_names = set(artifacts)
        artifact_prefixes = [
            {artifact for _, artifact in stages[:index]}
            for index in range(len(stages) + 1)
        ]
        if artifact_names not in artifact_prefixes:
            raise FullLocalAnalysisError(
                "deferred artifact不是local processing前缀"
            )
        prefix_length = artifact_prefixes.index(artifact_names)
        versions = media_module.processor_versions()
        artifact_versions = {
            "media": "provider-media-v8.0",
            "media_manifest": media_module.IMAGE_DOWNLOAD_VERSION,
            "frames_manifest": str(versions["frames"]),
            "asr": str(versions["asr"]),
            "ocr": str(versions["ocr"]),
        }
        for name, row in artifacts.items():
            if str(row["processor_version"] or "") != artifact_versions[name]:
                raise FullLocalAnalysisError(
                    "deferred artifact processor version漂移"
                )
            try:
                metadata = json.loads(str(row["metadata_json"] or "{}"))
            except json.JSONDecodeError as exc:
                raise FullLocalAnalysisError(
                    "deferred artifact metadata非法"
                ) from exc
            if not isinstance(metadata, Mapping):
                raise FullLocalAnalysisError("deferred artifact metadata形状非法")

        slot_rows = deltas["media_processing_slots"]
        slot_by_type: dict[str, Mapping[str, Any]] = {}
        for row in slot_rows:
            processor = str(row.get("processor_type") or "")
            if processor in slot_by_type:
                raise FullLocalAnalysisError("deferred slot processor重复")
            slot_by_type[processor] = row
        slot_attempt_expectations = [
            {
                "slot_id": int(row["id"]),
                "content_id": content_id,
                "source_sha256": str(row["source_sha256"]),
                "processor_type": str(row["processor_type"]),
                "processor_version": str(row["processor_version"]),
                "from_attempt_count": int(row["attempt_count"]),
                "expected_attempt_count": int(row["attempt_count"]) + 1,
            }
            for row in slot_rows
            if row.get("status") == "retryable_failed"
        ]
        scoped_paths = replace(
            paths.local_paths,
            media_root=paths.media_root / str(source["content"]["link_id"]),
            fingerprint_root=(
                paths.fingerprint_root
                / f"__item_{content_id}_deferred_fingerprint_scope__"
            ),
        )
        try:
            local._validate_prewrite_outputs(
                scoped_paths,
                contract=mini_contract,
                content_ids=[content_id],
                completed_ids=[],
                ledger=ledger,
                slot_attempt_expectations=slot_attempt_expectations,
            )
        except local.LocalAnalysisCanaryError as exc:
            raise FullLocalAnalysisError(
                f"deferred local prewrite prefix阻断：{exc}"
            ) from exc
        allowed_slot_types = {stage for stage, _ in stages[:prefix_length]}
        if prefix_length < len(stages):
            allowed_slot_types.add(stages[prefix_length][0])
        if set(slot_by_type) - allowed_slot_types:
            raise FullLocalAnalysisError("deferred slot不是local processing前缀")

        source_sha = (
            str(source["artifact_body"]["source_sha256"])
            if kind == "video"
            else local._source_image_download_binding(source)
        )
        expected_slot_versions = {
            "download": (
                media_module.VIDEO_DOWNLOAD_VERSION
                if kind == "video"
                else media_module.IMAGE_DOWNLOAD_VERSION
            ),
            "frames": str(versions["frames"]),
            "asr": str(versions["asr"]),
            "ocr": str(versions["ocr"]),
        }
        expected_slot_sources: dict[str, str] = {"download": source_sha}
        if "media" in artifacts:
            expected_slot_sources.update(
                {
                    "frames": str(artifacts["media"]["sha256"]),
                    "asr": str(artifacts["media"]["sha256"]),
                }
            )
        if "frames_manifest" in artifacts:
            expected_slot_sources["ocr"] = str(
                artifacts["frames_manifest"]["sha256"]
            )
        if "media_manifest" in artifacts:
            expected_slot_sources["ocr"] = str(
                artifacts["media_manifest"]["sha256"]
            )
        for index, (processor, artifact_name) in enumerate(stages):
            slot = slot_by_type.get(processor)
            if index < prefix_length:
                artifact = artifacts[artifact_name]
                if (
                    slot is None
                    or slot.get("status") != "succeeded"
                    or int(slot.get("output_artifact_id") or -1)
                    != int(artifact["id"])
                    or slot.get("error_message") not in (None, "")
                ):
                    raise FullLocalAnalysisError(
                        "deferred succeeded slot未精确绑定artifact"
                    )
            elif slot is not None:
                if (
                    index != prefix_length
                    or slot.get("status") != "retryable_failed"
                    or slot.get("output_artifact_id") is not None
                    or int(slot.get("attempt_count") or 0) != 1
                    or not str(slot.get("error_message") or "")
                ):
                    raise FullLocalAnalysisError(
                        "deferred failed slot不是唯一认可终态"
                    )
            if slot is not None and (
                str(slot.get("source_sha256") or "")
                != expected_slot_sources.get(processor)
                or str(slot.get("processor_version") or "")
                != expected_slot_versions[processor]
                or int(slot.get("attempt_count") or 0) != 1
            ):
                raise FullLocalAnalysisError(
                    "deferred slot source/version/attempt漂移"
                )

        download_name = "media" if kind == "video" else "media_manifest"
        if download_name in artifacts:
            try:
                local._validate_download_provenance(
                    content_id=content_id,
                    source=source,
                    artifacts=artifacts,
                    ledger=ledger,
                )
            except local.LocalAnalysisCanaryError as exc:
                raise FullLocalAnalysisError(str(exc)) from exc
        if "frames_manifest" in artifacts:
            media_files.update(
                local._manifest_output_paths(
                    artifacts["frames_manifest"],
                    media_kind="video",
                    media_root=paths.media_root,
                )
            )
        if "media_manifest" in artifacts:
            media_files.update(
                local._manifest_output_paths(
                    artifacts["media_manifest"],
                    media_kind="image",
                    media_root=paths.media_root,
                    source=source,
                )
            )

        before_sequences = intent["before"]["sequences"]
        expected_sequences = dict(before_sequences)
        for table in ("evidence_artifacts", "media_processing_slots"):
            ids = [int(row["id"]) for row in deltas[table]]
            if ids:
                expected_sequences[table] = max(
                    int(before_sequences.get(table, 0)), max(ids)
                )
        if (
            require_global_sequence_head
            and _sequence_snapshot(connection) != expected_sequences
        ):
            raise FullLocalAnalysisError("deferred sqlite_sequence不是精确前缀")
    actual_outputs = _item_output_inventory(paths, source)
    link_id = str(source["content"]["link_id"])
    actual_paths = {
        paths.media_root / link_id / str(row["path"])
        for row in actual_outputs["media"]["rows"]
    } | {
        paths.fingerprint_root / str(row["path"])
        for row in actual_outputs["fingerprints"]["rows"]
    }
    if actual_paths != media_files | fingerprint_files:
        raise FullLocalAnalysisError(
            "deferred item输出不等于partial artifact精确闭包"
        )
    return {
        "target_rows_sha256": _json_sha(current),
        "target_sequences": _target_sequence_projection(current),
        "content_direction": _content_direction(paths.database, content_id),
        "partial_slots": len(slot_rows),
        "partial_artifacts": len(artifacts),
        "outputs": actual_outputs,
    }


def _terminal_network_evidence(event: Mapping[str, Any]) -> Mapping[str, Any]:
    outcome = event.get("outcome")
    error = event.get("error")
    response_sha256 = event.get("response_sha256")
    status = event.get("status")
    mime = event.get("mime")
    declared_bytes = event.get("declared_bytes")
    byte_count = event.get("bytes")
    charged_count = event.get("charged_bytes")
    if (
        type(event.get("event_index")) is not int
        or type(byte_count) is not int
        or byte_count < 0
        or type(charged_count) is not int
        or charged_count < byte_count
        or (mime is not None and type(mime) is not str)
        or (
            declared_bytes is not None
            and (type(declared_bytes) is not int or declared_bytes < 0)
        )
    ):
        raise FullLocalAnalysisError(
            "deferred terminal event response evidence类型漂移"
        )
    if (
        outcome == "succeeded"
        and type(status) is int
        and 200 <= status < 300
        and isinstance(response_sha256, str)
        and re.fullmatch(r"[0-9a-f]{64}", response_sha256) is not None
        and error is None
        and type(byte_count) is int
        and byte_count >= 0
        and (
            declared_bytes is None
            or (
                type(declared_bytes) is int
                and declared_bytes >= 0
                and byte_count == declared_bytes
            )
        )
    ):
        evidence_class = "response_completed"
        error_policy = "must_be_null_after_complete_response"
    elif (
        outcome == "failed"
        and type(error) is str
        and bool(error)
        and type(status) is int
        and 200 <= status < 300
        and isinstance(response_sha256, str)
        and re.fullmatch(r"[0-9a-f]{64}", response_sha256) is not None
        and (
            declared_bytes is None
            or byte_count <= declared_bytes
        )
    ):
        evidence_class = "response_failed_after_open"
        error_policy = "must_be_nonempty_after_opened_response"
    elif (
        outcome == "failed"
        and type(error) is str
        and bool(error)
        and type(status) is int
        and 200 <= status < 300
        and response_sha256 is None
        and byte_count == 0
        and charged_count == 0
    ):
        evidence_class = "response_rejected_after_open"
        error_policy = "must_be_nonempty_before_response_body"
    elif (
        outcome == "failed"
        and type(error) is str
        and bool(error)
        and status is None
        and mime is None
        and declared_bytes is None
        and response_sha256 is None
        and byte_count == 0
        and charged_count == 0
    ):
        evidence_class = "transport_failed_before_response"
        error_policy = "must_be_nonempty_before_response_open"
    elif (
        outcome == "interrupted"
        and error == "controller interrupted before response close"
        and status is None
        and mime is None
        and declared_bytes is None
        and response_sha256 is None
        and byte_count == 0
        and charged_count == 0
    ):
        evidence_class = "opening_interrupted"
        error_policy = "exact_controller_interruption_marker"
    else:
        raise FullLocalAnalysisError(
            "deferred terminal event outcome/response/error语义漂移"
        )
    response_evidence = {
        "status": status,
        "mime": mime,
        "declared_bytes": declared_bytes,
        "bytes": byte_count,
        "charged_bytes": charged_count,
        "response_sha256": response_sha256,
    }
    return {
        "terminal_event_index": int(event["event_index"]),
        "terminal_event_outcome": str(outcome),
        "terminal_event_sha256": _json_sha(event),
        "terminal_evidence_class": evidence_class,
        "terminal_error_policy": error_policy,
        "terminal_error_sha256": (
            _json_sha(error) if error is not None else None
        ),
        "terminal_response_evidence_sha256": _json_sha(response_evidence),
    }


def _validate_deferred_failure_value(failure: Mapping[str, Any]) -> None:
    if set(failure) != {"type", "message"} or any(
        type(failure.get(key)) is not str or not failure[key]
        for key in ("type", "message")
    ):
        raise FullLocalAnalysisError("controlled deferred failure形状漂移")


def _new_item_slots_from_target_rows(
    *,
    intent: Mapping[str, Any],
    current_rows: Mapping[str, Sequence[Mapping[str, Any]]],
) -> list[Mapping[str, Any]]:
    before_rows = intent["before"]["target_rows"]["media_processing_slots"]
    before_keys = Counter(local._canonical_bytes(row) for row in before_rows)
    live_slots: list[Mapping[str, Any]] = []
    for row in current_rows["media_processing_slots"]:
        key = local._canonical_bytes(row)
        if before_keys[key] > 0:
            before_keys[key] -= 1
        else:
            live_slots.append(row)
    return live_slots


def _new_item_slots(
    paths: BatchPaths,
    *,
    intent: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    return _new_item_slots_from_target_rows(
        intent=intent,
        current_rows=_item_target_rows(
            paths.database, int(intent["content_id"])
        ),
    )


def _deferred_slot_identity(row: Mapping[str, Any]) -> Mapping[str, Any]:
    identity = {
        "id": row.get("id"),
        "content_id": row.get("content_id"),
        "source_sha256": row.get("source_sha256"),
        "processor_type": row.get("processor_type"),
        "processor_version": row.get("processor_version"),
        "status": row.get("status"),
        "output_artifact_id": row.get("output_artifact_id"),
        "attempt_count": row.get("attempt_count"),
    }
    if (
        type(identity["id"]) is not int
        or identity["id"] <= 0
        or type(identity["content_id"]) is not int
        or identity["content_id"] <= 0
        or type(identity["source_sha256"]) is not str
        or re.fullmatch(r"[0-9a-f]{64}", identity["source_sha256"]) is None
        or type(identity["processor_type"]) is not str
        or not identity["processor_type"]
        or type(identity["processor_version"]) is not str
        or not identity["processor_version"]
        or identity["status"] != "retryable_failed"
        or identity["output_artifact_id"] is not None
        or type(identity["attempt_count"]) is not int
        or identity["attempt_count"] != 1
    ):
        raise FullLocalAnalysisError("deferred durable attempt slot身份漂移")
    return identity


def _validate_deferred_slot_source_identity(
    row: Mapping[str, Any],
    *,
    intent: Mapping[str, Any],
    source: Mapping[str, Any],
) -> None:
    content_id = intent.get("content_id")
    source_content_id = source.get("content", {}).get("id")
    media_kind = source.get("artifact_body", {}).get("media_kind")
    flat_source_sha256 = source.get("artifact_body", {}).get("source_sha256")
    if media_kind == "video":
        expected_source_sha256 = flat_source_sha256
        expected_version = media_module.VIDEO_DOWNLOAD_VERSION
    elif media_kind == "image":
        expected_source_sha256 = local._source_image_download_binding(source)
        expected_version = media_module.IMAGE_DOWNLOAD_VERSION
    else:
        raise FullLocalAnalysisError("deferred source media kind漂移")
    if (
        type(content_id) is not int
        or type(source_content_id) is not int
        or source_content_id != content_id
        or type(expected_source_sha256) is not str
        or re.fullmatch(r"[0-9a-f]{64}", expected_source_sha256) is None
        or row.get("content_id") != content_id
        or row.get("source_sha256") != expected_source_sha256
        or row.get("processor_type") != "download"
        or row.get("processor_version") != expected_version
    ):
        raise FullLocalAnalysisError(
            "deferred durable attempt未绑定current source download identity"
        )


def _deferred_attempt_anchor_payload(
    *,
    intent: Mapping[str, Any],
    ledger: local._NetworkLedger,
    failure: Mapping[str, Any],
    slot: Mapping[str, Any],
) -> Mapping[str, Any]:
    _validate_deferred_failure_value(failure)
    ledger.require_terminal()
    content_id = int(intent["content_id"])
    events = ledger.transcript(content_id)
    if not events or any(
        event.get("outcome") not in {"succeeded", "failed", "interrupted"}
        for event in events
    ):
        raise FullLocalAnalysisError(
            "controlled deferred缺少完整terminal network transcript"
        )
    return {
        "schema_version": DEFERRED_ATTEMPT_ANCHOR_SCHEMA,
        "content_id": content_id,
        "intent_sha256": _json_sha(intent),
        "failure": dict(failure),
        "failure_sha256": _json_sha(failure),
        "network_ledger_sha256": local._sha256_file(ledger.path),
        "terminal_transcript_sha256": _json_sha(events),
        "terminal_evidence": _terminal_network_evidence(events[-1]),
        "slot_identity": _deferred_slot_identity(slot),
    }


def _deferred_attempt_anchor_text(payload: Mapping[str, Any]) -> str:
    return DEFERRED_ATTEMPT_ANCHOR_PREFIX + local._canonical_bytes(
        payload
    ).decode("utf-8").removesuffix("\n")


def _parse_deferred_attempt_anchor(row: Mapping[str, Any]) -> Mapping[str, Any]:
    raw = row.get("error_message")
    if type(raw) is not str or not raw.startswith(
        DEFERRED_ATTEMPT_ANCHOR_PREFIX
    ):
        raise FullLocalAnalysisError(
            "manual_required: retryable slot缺少durable processing attempt anchor"
        )
    encoded = raw[len(DEFERRED_ATTEMPT_ANCHOR_PREFIX) :]
    try:
        value = json.loads(encoded)
    except json.JSONDecodeError as exc:
        raise FullLocalAnalysisError(
            "durable processing attempt anchor JSON非法"
        ) from exc
    expected_keys = {
        "schema_version",
        "content_id",
        "intent_sha256",
        "failure",
        "failure_sha256",
        "network_ledger_sha256",
        "terminal_transcript_sha256",
        "terminal_evidence",
        "slot_identity",
    }
    if (
        not isinstance(value, Mapping)
        or set(value) != expected_keys
        or value.get("schema_version") != DEFERRED_ATTEMPT_ANCHOR_SCHEMA
        or _deferred_attempt_anchor_text(value) != raw
    ):
        raise FullLocalAnalysisError(
            "durable processing attempt anchor形状/编码漂移"
        )
    return value


def _anchored_retryable_slot(
    paths: BatchPaths,
    *,
    intent: Mapping[str, Any],
    source: Mapping[str, Any],
) -> Mapping[str, Any]:
    live_slots = _new_item_slots(paths, intent=intent)
    retryable = [
        row
        for row in live_slots
        if row.get("processor_type") == "download"
        and row.get("status") == "retryable_failed"
        and row.get("output_artifact_id") is None
        and type(row.get("attempt_count")) is int
        and row["attempt_count"] == 1
        and type(row.get("error_message")) is str
        and row["error_message"].startswith(DEFERRED_ATTEMPT_ANCHOR_PREFIX)
    ]
    if len(live_slots) != 1 or len(retryable) != 1:
        raise FullLocalAnalysisError(
            "manual_required: deferred未绑定唯一durable processing attempt"
        )
    _validate_deferred_slot_source_identity(
        retryable[0], intent=intent, source=source
    )
    return retryable[0]


def _validate_live_deferred_failure_binding(
    paths: BatchPaths,
    *,
    intent: Mapping[str, Any],
    source: Mapping[str, Any],
    ledger: local._NetworkLedger,
    failure: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Re-derive deferred semantics from the DB attempt anchor, not receipt."""

    retryable = _anchored_retryable_slot(
        paths, intent=intent, source=source
    )
    recorded_anchor = _parse_deferred_attempt_anchor(retryable)
    expected_anchor = _deferred_attempt_anchor_payload(
        intent=intent,
        ledger=ledger,
        failure=failure,
        slot=retryable,
    )
    if local._canonical_bytes(recorded_anchor) != local._canonical_bytes(
        expected_anchor
    ):
        raise FullLocalAnalysisError(
            "durable processing attempt anchor与DB/ledger/failure漂移"
        )
    terminal_evidence = expected_anchor["terminal_evidence"]
    return {
        "failure_sha256": _json_sha(failure),
        **terminal_evidence,
        "terminal_transcript_sha256": expected_anchor[
            "terminal_transcript_sha256"
        ],
        "network_ledger_sha256": expected_anchor["network_ledger_sha256"],
        "durable_attempt_anchor_schema": DEFERRED_ATTEMPT_ANCHOR_SCHEMA,
        "durable_attempt_anchor_sha256": _json_sha(expected_anchor),
        "retryable_slot_sha256": _json_sha(retryable),
    }


def _owned_empty_deferred_media_directories(
    paths: BatchPaths,
    *,
    intent: Mapping[str, Any],
    source: Mapping[str, Any],
) -> list[Path]:
    """Validate an item-owned, directory-only failed-download residue.

    A failed image download can remove every selected file and its manifest
    while leaving its deterministic directory chain behind.  The global
    inventory must continue to reject arbitrary empty directories, so this
    narrow cleanup accepts only the current item's frozen link/source path and
    only when its pre-item output was empty.  Validation completes before any
    directory is removed.
    """

    empty_inventory = {
        "files": 0,
        "rows_sha256": _json_sha([]),
        "rows": [],
    }
    before_media = intent.get("before", {}).get("outputs", {}).get("media")
    if local._canonical_bytes(before_media) != local._canonical_bytes(
        empty_inventory
    ):
        raise FullLocalAnalysisError(
            "controlled deferred owned media要求冻结的pre-item输出为空"
        )
    link_id = source.get("content", {}).get("link_id")
    flat_source_sha256 = source.get("artifact_body", {}).get("source_sha256")
    media_kind = source.get("artifact_body", {}).get("media_kind")
    source_sha256 = (
        flat_source_sha256
        if media_kind == "video"
        else local._source_image_download_binding(source)
        if media_kind == "image"
        else None
    )
    if (
        type(link_id) is not str
        or not link_id
        or Path(link_id).name != link_id
        or link_id in {".", ".."}
        or type(source_sha256) is not str
        or re.fullmatch(r"[0-9a-f]{64}", source_sha256) is None
        or media_kind not in {"image", "video"}
    ):
        raise FullLocalAnalysisError(
            "controlled deferred owned media path身份漂移"
        )
    item_root = paths.media_root / link_id
    downloads_root = item_root / "downloads"
    source_root = downloads_root / source_sha256
    allowed = {item_root, downloads_root, source_root}
    if media_kind == "image":
        allowed.add(source_root / "images")
    if not os.path.lexists(item_root):
        return []

    pending = [item_root]
    observed: set[Path] = set()
    while pending:
        directory = pending.pop()
        if directory not in allowed or directory in observed:
            raise FullLocalAnalysisError(
                "controlled deferred owned media包含未知目录"
            )
        try:
            local._private_directory(
                directory, label="controlled deferred owned media目录"
            )
        except local.LocalAnalysisCanaryError as exc:
            raise FullLocalAnalysisError(str(exc)) from exc
        observed.add(directory)
        with os.scandir(directory) as entries:
            children = list(entries)
        for entry in children:
            child = Path(entry.path)
            if entry.is_symlink() or not entry.is_dir(follow_symlinks=False):
                raise FullLocalAnalysisError(
                    "controlled deferred owned media包含文件/link/非目录证据"
                )
            if child not in allowed:
                raise FullLocalAnalysisError(
                    "controlled deferred owned media包含未知目录"
                )
            pending.append(child)
    return sorted(observed, key=lambda path: len(path.parts), reverse=True)


def _remove_owned_empty_deferred_media_directories(
    directories: Sequence[Path],
) -> None:
    for directory in directories:
        try:
            directory.rmdir()
        except OSError as exc:
            raise FullLocalAnalysisError(
                f"controlled deferred owned media目录不再为空：{directory}"
            ) from exc
        local._fsync_directory(directory.parent)


def _before_deferred_attempt_anchor_commit(_content_id: int) -> None:
    """Test seam immediately before the durable attempt CAS."""


def _after_deferred_attempt_anchor_commit(_content_id: int) -> None:
    """Test seam after anchor commit and before database finalization."""


def _writable_connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=30)
    connection.row_factory = sqlite3.Row
    try:
        local.storage_module.configure_connection_safety(connection)
    except Exception:
        connection.close()
        raise
    return connection


def _stamp_live_deferred_attempt_anchor(
    paths: BatchPaths,
    *,
    intent: Mapping[str, Any],
    source: Mapping[str, Any],
    ledger: local._NetworkLedger,
    failure: Mapping[str, Any],
) -> None:
    """Atomically bind one pipeline failure to its durable DB attempt row."""

    _validate_deferred_failure_value(failure)
    expected_error = f"{failure['type']}: {failure['message']}"[:500]
    preliminary_connection = sqlite3.connect(paths.database, timeout=30)
    preliminary_connection.row_factory = sqlite3.Row
    try:
        preliminary_rows = local._target_rows(
            preliminary_connection, [int(intent["content_id"])]
        )
    finally:
        preliminary_connection.close()
    preliminary_slots = _new_item_slots_from_target_rows(
        intent=intent, current_rows=preliminary_rows
    )
    preliminary_retryable = [
        row
        for row in preliminary_slots
        if row.get("processor_type") == "download"
        and row.get("status") == "retryable_failed"
        and row.get("output_artifact_id") is None
        and type(row.get("attempt_count")) is int
        and row["attempt_count"] == 1
        and row.get("error_message") == expected_error
    ]
    if len(preliminary_slots) != 1 or len(preliminary_retryable) != 1:
        raise FullLocalAnalysisError(
            "controlled deferred未精确绑定唯一retryable_failed download slot"
        )
    _validate_deferred_slot_source_identity(
        preliminary_retryable[0], intent=intent, source=source
    )
    directories = _owned_empty_deferred_media_directories(
        paths, intent=intent, source=source
    )
    _remove_owned_empty_deferred_media_directories(directories)
    _before_deferred_attempt_anchor_commit(int(intent["content_id"]))
    connection = _writable_connection(paths.database)
    try:
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("BEGIN IMMEDIATE")
        current_rows = local._target_rows(
            connection, [int(intent["content_id"])]
        )
        live_slots = _new_item_slots_from_target_rows(
            intent=intent, current_rows=current_rows
        )
        retryable = [
            row
            for row in live_slots
            if row.get("processor_type") == "download"
            and row.get("status") == "retryable_failed"
            and row.get("output_artifact_id") is None
            and type(row.get("attempt_count")) is int
            and row["attempt_count"] == 1
            and row.get("error_message") == expected_error
        ]
        if len(live_slots) != 1 or len(retryable) != 1:
            raise FullLocalAnalysisError(
                "controlled deferred未精确绑定唯一retryable_failed download slot"
            )
        row = retryable[0]
        _validate_deferred_slot_source_identity(
            row, intent=intent, source=source
        )
        payload = _deferred_attempt_anchor_payload(
            intent=intent,
            ledger=ledger,
            failure=failure,
            slot=row,
        )
        anchor = _deferred_attempt_anchor_text(payload)
        cursor = connection.execute(
            """UPDATE media_processing_slots
               SET error_message=?
               WHERE id=? AND content_id=? AND source_sha256=?
                 AND processor_type=? AND processor_version=?
                 AND status='retryable_failed' AND output_artifact_id IS NULL
                 AND attempt_count=1 AND error_message=?""",
            (
                anchor,
                int(row["id"]),
                int(row["content_id"]),
                str(row["source_sha256"]),
                str(row["processor_type"]),
                str(row["processor_version"]),
                expected_error,
            ),
        )
        if cursor.rowcount != 1:
            raise FullLocalAnalysisError(
                "durable processing attempt anchor CAS失败"
            )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    _after_deferred_attempt_anchor_commit(int(intent["content_id"]))


def _recover_anchored_deferred_failure(
    paths: BatchPaths,
    *,
    intent: Mapping[str, Any],
    source: Mapping[str, Any],
    ledger: local._NetworkLedger,
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    slot = _anchored_retryable_slot(
        paths, intent=intent, source=source
    )
    anchor = _parse_deferred_attempt_anchor(slot)
    failure = anchor.get("failure")
    if not isinstance(failure, Mapping):
        raise FullLocalAnalysisError(
            "durable processing attempt anchor缺少failure"
        )
    binding = _validate_live_deferred_failure_binding(
        paths,
        intent=intent,
        source=source,
        ledger=ledger,
        failure=failure,
    )
    directories = _owned_empty_deferred_media_directories(
        paths, intent=intent, source=source
    )
    _remove_owned_empty_deferred_media_directories(directories)
    return dict(failure), binding


def _controlled_failure(exc: BaseException) -> bool:
    if isinstance(exc, ContentDeferredError):
        return str(exc).startswith("media_not_evidence_ready:")
    if isinstance(exc, media_module.MediaProcessingError):
        message = str(exc).lower()
        return message.startswith(
            (
                "media download failed:",
                "image download incomplete:",
            )
        )
    return False


def _execute_item_pipeline(
    paths: BatchPaths,
    *,
    content_id: int,
    source: Mapping[str, Any],
    tools: Mapping[str, Any],
    ledger: local._NetworkLedger,
    budget: local._DownloadBudget,
) -> Mapping[str, Any]:
    urls = local._source_urls(source)
    maximum_bytes = budget.maximum_bytes
    ocr_binary = Path(str(tools["ocr_binary"]["path"]))
    with local._execution_guards(
        urls,
        media_kind=str(source["artifact_body"]["media_kind"]),
        maximum_bytes=maximum_bytes,
        tools=tools,
        budget=budget,
        ledger=ledger,
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
                local._source_image_groups(source)
                if str(source["artifact_body"]["media_kind"]) == "image"
                else None
            ),
            reuse_existing_downloads=False,
            maximum_video_duration_seconds=local.MAX_VIDEO_DURATION_SECONDS,
        )
        if media_result.get("status") != "evidence_ready":
            raise ContentDeferredError(
                f"media_not_evidence_ready:{media_result.get('status')}"
            )
        evaluation = evaluation_module.evaluate_content(
            content_id, db_path=paths.database
        )
        if evaluation.evidence_level not in {"V0", "V1", "V2", "V3"}:
            raise FullLocalAnalysisError(
                f"content {content_id} evaluation evidence level无效："
                f"{evaluation.evidence_level}"
            )
        fingerprint_source_sha256 = None
        if evaluation.evidence_level in {"V2", "V3"}:
            fingerprint = duplicates_module.fingerprint_content(
                content_id, db_path=paths.database
            )
            fingerprint_source_sha256 = fingerprint.get("source_sha256")
    return {
        "content_id": content_id,
        "media": media_result,
        "evaluation": {
            "evaluation_id": evaluation.evaluation_id,
            "evidence_level": evaluation.evidence_level,
            "evidence_sha256": evaluation.evidence_sha256,
            "created": evaluation.created,
        },
        "fingerprint_source_sha256": fingerprint_source_sha256,
        "network_transcript": ledger.transcript(content_id),
        "network_transcript_sha256": _json_sha(ledger.transcript(content_id)),
    }


def _after_item_database_commit(_content_id: int) -> None:
    """Test seam: production deliberately performs no action here."""


def _network_path(paths: BatchPaths, ordinal: int) -> Path:
    return paths.network_root / f"{ordinal:06d}.network.json"


def _network_value_for_read(path: Path) -> Mapping[str, Any] | None:
    if path.exists():
        return _read_json(path, label="item network ledger")
    temporary = path.with_name(f".{path.name}.tmp")
    if temporary.exists():
        return _read_json(temporary, label="item network ledger temp")
    return None


def _read_only_network_ledger(
    path: Path,
    *,
    contract_sha256: str,
    intent_sha256: str,
    content_id: int,
    maximum_bytes: int,
    source: Mapping[str, Any],
    require_terminal: bool = True,
) -> local._NetworkLedger:
    """Validate one immutable terminal ledger without invoking recovery code."""

    if not path.is_file():
        raise FullLocalAnalysisError(
            f"历史item缺少network ledger：{path}"
        )
    local._private_file(path, label="historical network ledger")
    value = _read_json(path, label="historical network ledger")
    ledger = object.__new__(local._NetworkLedger)
    ledger.path = path
    ledger.contract_sha256 = contract_sha256
    ledger.intent_sha256 = intent_sha256
    ledger.content_ids = [content_id]
    ledger.maximum_bytes = maximum_bytes
    ledger._source_urls = {  # noqa: SLF001 - audited pure validator setup
        content_id: {
            str(row["url"]): str(row["host"])
            for row in source["urls"]
            if str(row["url"]) in set(local._source_urls(source))
        }
    }
    ledger.value = dict(value)
    ledger._validate(ledger.value)  # noqa: SLF001 - validation without writes
    if require_terminal:
        ledger.require_terminal()
    return ledger


def _validate_pending_network_recovery_read_only(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    *,
    item_receipts: Sequence[Mapping[str, Any]],
    runtime: RuntimeContext,
) -> None:
    """Fail closed on a receiptless request before any recovery transition."""

    intents = _item_intent_files(paths)
    if len(intents) <= len(item_receipts):
        return
    if len(intents) != len(item_receipts) + 1:
        raise FullLocalAnalysisError("pending network item不是唯一next ordinal")
    ordinal = len(item_receipts) + 1
    intent_path = intents[-1]
    ledger_path = _network_path(paths, ordinal)
    if not ledger_path.exists():
        return
    intent = _read_json(intent_path, label="pending network intent preflight")
    content_id = int(intent["content_id"])
    with closing(local._immutable_connection(paths.database)) as connection:
        try:
            source = _source_snapshot(
                connection,
                content_id,
                source_evidence=contract["source_completion"],
                row_map=runtime.target_row_map,
                discovery_raw_cache=runtime.discovery_raw_cache,
            )
            current_sequences = _sequence_snapshot(connection)
        except local.LocalAnalysisCanaryError as exc:
            raise FullLocalAnalysisError(str(exc)) from exc
    ledger = _read_only_network_ledger(
        ledger_path,
        contract_sha256=runtime.contract_sha256,
        intent_sha256=local._sha256_file(intent_path),
        content_id=content_id,
        maximum_bytes=int(intent["network_maximum_bytes"]),
        source=source,
        require_terminal=False,
    )
    events = list(ledger.value["events"])
    if not events:
        return
    before = intent.get("before")
    if not isinstance(before, Mapping):
        raise FullLocalAnalysisError("pending network intent缺少before闭包")
    current_rows = _item_target_rows(paths.database, content_id)
    current_outputs = _item_output_inventory(paths, source)
    materialization_is_exactly_before = (
        _json_sha(current_rows) == before.get("target_rows_sha256")
        and _target_sequence_projection(current_rows)
        == before.get("target_sequences")
        and _content_direction(paths.database, content_id)
        == before.get("content_direction")
        and local._canonical_bytes(current_sequences)
        == local._canonical_bytes(before.get("sequences"))
        and local._canonical_bytes(current_outputs)
        == local._canonical_bytes(before.get("outputs"))
    )
    if materialization_is_exactly_before:
        raise FullLocalAnalysisError(
            "manual_required: durable network event缺少DB processing attempt; "
            "恢复写与请求重放均被禁止"
        )
    if any(
        event.get("outcome") in {"opening", "opened"} for event in events
    ):
        raise FullLocalAnalysisError(
            "manual_required: receiptless DB materialization绑定非终态network event"
        )
    ledger.require_terminal()
    try:
        result = _project_success_result(
            paths, content_id=content_id, source=source, ledger=ledger
        )
        _validate_item_success_strong(
            paths,
            intent=intent,
            source=source,
            result=result,
            ledger=ledger,
        )
        return
    except (KeyError, FullLocalAnalysisError, local.LocalAnalysisCanaryError):
        pass
    try:
        result = _project_success_result(
            paths, content_id=content_id, source=source, ledger=ledger
        )
        _validate_item_review_pending_strong(
            paths,
            intent=intent,
            source=source,
            result=result,
            ledger=ledger,
        )
        return
    except (KeyError, FullLocalAnalysisError, local.LocalAnalysisCanaryError):
        pass
    try:
        result = _project_insufficient_result(
            paths, content_id=content_id, source=source, ledger=ledger
        )
        _validate_item_insufficient_evidence_strong(
            paths,
            intent=intent,
            source=source,
            result=result,
            ledger=ledger,
        )
        return
    except (KeyError, FullLocalAnalysisError, local.LocalAnalysisCanaryError):
        pass
    try:
        _validate_item_deferred_exact(
            paths,
            intent=intent,
            source=source,
            ledger=ledger,
        )
        _recover_anchored_deferred_failure(
            paths,
            intent=intent,
            source=source,
            ledger=ledger,
        )
    except (FullLocalAnalysisError, local.LocalAnalysisCanaryError) as exc:
        raise FullLocalAnalysisError(
            "manual_required: receiptless deferred缺少独立durable attempt anchor"
        ) from exc


def _item_network(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    *,
    ordinal: int,
    content_id: int,
    intent_path: Path,
    source: Mapping[str, Any],
    maximum_bytes: int,
    runtime: RuntimeContext,
) -> tuple[local._NetworkLedger, local._DownloadBudget]:
    ledger = local._NetworkLedger(
        _network_path(paths, ordinal),
        contract_sha256=runtime.contract_sha256,
        intent_sha256=local._sha256_file(intent_path),
        content_ids=[content_id],
        maximum_bytes=maximum_bytes,
        sources={content_id: source},
    )
    budget = local._DownloadBudget(
        maximum_bytes,
        consumed_bytes=ledger.budget_consumed_bytes,
        ledger=ledger,
    )
    return ledger, budget


def _item_before(
    paths: BatchPaths,
    *,
    content_id: int,
    source: Mapping[str, Any],
) -> Mapping[str, Any]:
    with closing(local._immutable_connection(paths.database)) as connection:
        counts = _item_analysis_counts(connection, content_id)
        target_rows = local._target_rows(connection, [content_id])
        sequences = _sequence_snapshot(connection)
    if any(counts.values()):
        raise FullLocalAnalysisError(
            f"无receipt的content已存在分析行：{content_id} {counts}"
        )
    return {
        "target_rows": target_rows,
        "target_rows_sha256": _json_sha(target_rows),
        "target_sequences": _target_sequence_projection(target_rows),
        "content_direction": _content_direction(paths.database, content_id),
        "sequences": sequences,
        "outputs": _item_output_inventory(paths, source),
        "item_counts": counts,
    }


def _project_success_result(
    paths: BatchPaths,
    *,
    content_id: int,
    source: Mapping[str, Any],
    ledger: local._NetworkLedger,
) -> Mapping[str, Any]:
    with closing(local._immutable_connection(paths.database)) as connection:
        rows = connection.execute(
            "SELECT * FROM evidence_artifacts WHERE content_id=? ORDER BY id",
            (content_id,),
        ).fetchall()
        by_type = {str(row["artifact_type"]): row for row in rows}
        kind = str(source["artifact_body"]["media_kind"])
        media_artifacts = (
            {
                "media": int(by_type["media"]["id"]),
                "frames": int(by_type["frames_manifest"]["id"]),
                "asr": int(by_type["asr"]["id"]),
                "ocr": int(by_type["ocr"]["id"]),
            }
            if kind == "video"
            else {
                "media": int(by_type["media_manifest"]["id"]),
                "ocr": int(by_type["ocr"]["id"]),
            }
        )
        evaluation = connection.execute(
            """SELECT * FROM evaluation_versions
               WHERE content_id=? AND invalidated_at IS NULL
               ORDER BY id DESC LIMIT 1""",
            (content_id,),
        ).fetchone()
        if evaluation is None:
            raise FullLocalAnalysisError("恢复success缺少evaluation")
        _inputs, fingerprint_source = duplicates_module._current_source_state(
            connection, content_id
        )
    transcript = ledger.transcript(content_id)
    return {
        "content_id": content_id,
        "media": {
            "content_id": content_id,
            "status": "evidence_ready",
            "media_kind": kind,
            "artifacts": media_artifacts,
        },
        "evaluation": {
            "evaluation_id": int(evaluation["id"]),
            "evidence_level": str(evaluation["evidence_level"]),
            "evidence_sha256": str(evaluation["evidence_sha256"]),
            "created": True,
        },
        "fingerprint_source_sha256": fingerprint_source,
        "network_transcript": transcript,
        "network_transcript_sha256": _json_sha(transcript),
    }


def _project_insufficient_result(
    paths: BatchPaths,
    *,
    content_id: int,
    source: Mapping[str, Any],
    ledger: local._NetworkLedger,
) -> Mapping[str, Any]:
    result = dict(
        _project_success_result(
            paths,
            content_id=content_id,
            source=source,
            ledger=ledger,
        )
    )
    result["fingerprint_source_sha256"] = None
    return result


def _item_after(
    paths: BatchPaths,
    *,
    content_id: int,
    source: Mapping[str, Any],
    ledger: local._NetworkLedger,
) -> Mapping[str, Any]:
    with closing(local._immutable_connection(paths.database)) as connection:
        rows = local._target_rows(connection, [content_id])
        sequences = _sequence_snapshot(connection)
    return {
        "target_rows_sha256": _json_sha(rows),
        "target_sequences": _target_sequence_projection(rows),
        "content_direction": _content_direction(paths.database, content_id),
        "sequences": sequences,
        "outputs": _item_output_inventory(paths, source),
        "network_ledger_sha256": local._sha256_file(ledger.path),
        "network_budget_consumed_bytes": ledger.budget_consumed_bytes,
    }


def _revalidate_item_receipt_current(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    *,
    ordinal: int,
    require_global_sequence_head: bool,
    runtime: RuntimeContext,
    verify_owned_content: bool = True,
    read_only_ledger: bool = False,
    receipt_path_override: Path | None = None,
    expected_sequence_prefix: Mapping[str, int] | None = None,
) -> Mapping[str, int] | None:
    intent_path, final_receipt_path = _item_paths(paths, ordinal)
    receipt_path = receipt_path_override or final_receipt_path
    intent = _read_json(intent_path, label="item revalidation intent")
    receipt = _read_json(receipt_path, label="item revalidation receipt")
    content_id = int(receipt["content_id"])
    with closing(local._immutable_connection(paths.database)) as connection:
        try:
            source = _source_snapshot(
                connection,
                content_id,
                source_evidence=contract["source_completion"],
                row_map=runtime.target_row_map,
                discovery_raw_cache=runtime.discovery_raw_cache,
            )
        except local.LocalAnalysisCanaryError as exc:
            raise FullLocalAnalysisError(
                f"receipt content {content_id} source漂移：{exc}"
            ) from exc
    if read_only_ledger:
        ledger = _read_only_network_ledger(
            _network_path(paths, ordinal),
            contract_sha256=runtime.contract_sha256,
            intent_sha256=local._sha256_file(intent_path),
            content_id=content_id,
            maximum_bytes=int(intent["network_maximum_bytes"]),
            source=source,
        )
    else:
        ledger = local._NetworkLedger(
            _network_path(paths, ordinal),
            contract_sha256=runtime.contract_sha256,
            intent_sha256=local._sha256_file(intent_path),
            content_ids=[content_id],
            maximum_bytes=int(intent["network_maximum_bytes"]),
            sources={content_id: source},
            recover_incomplete=False,
        )
    current_rows = _item_target_rows(paths.database, content_id)
    current_target_sequences = _target_sequence_projection(current_rows)
    derived_sequence: Mapping[str, int] | None = None
    if expected_sequence_prefix is not None:
        _validate_exact_sequence_mapping(
            expected_sequence_prefix, label="historical expected prefix"
        )
        _validate_exact_sequence_mapping(
            intent["before"].get("sequences"),
            label=f"item receipt {ordinal} before",
        )
        if local._canonical_bytes(intent["before"]["sequences"]) != (
            local._canonical_bytes(expected_sequence_prefix)
        ):
            raise FullLocalAnalysisError(
                f"item receipt {ordinal} before sequence prefix漂移"
            )
        next_sequence = dict(expected_sequence_prefix)
        for table, sequence in current_target_sequences.items():
            if table in MANAGED_SEQUENCES:
                next_sequence[table] = max(
                    int(next_sequence.get(table, 0)), int(sequence)
                )
        derived_sequence = next_sequence
    current_outputs = (
        _item_output_inventory(paths, source) if verify_owned_content else None
    )
    current_sequences: Mapping[str, int] | None = None
    if require_global_sequence_head:
        with closing(local._immutable_connection(paths.database)) as connection:
            current_sequences = _sequence_snapshot(connection)
    after = receipt["after"]
    if (
        _json_sha(current_rows) != after["target_rows_sha256"]
        or current_target_sequences != after["target_sequences"]
        or _content_direction(paths.database, content_id)
        != after["content_direction"]
        or (
            verify_owned_content and current_outputs != after["outputs"]
        )
        or local._sha256_file(ledger.path)
        != after["network_ledger_sha256"]
        or ledger.budget_consumed_bytes
        != after["network_budget_consumed_bytes"]
        or (
            require_global_sequence_head
            and current_sequences != after["sequences"]
        )
        or (
            derived_sequence is not None
            and local._canonical_bytes(derived_sequence)
            != local._canonical_bytes(after["sequences"])
        )
    ):
        raise FullLocalAnalysisError(
            f"item receipt {ordinal} 与当前DB/output/sequence终态漂移"
        )
    if not verify_owned_content:
        return derived_sequence
    try:
        if receipt["status"] in {
            "succeeded",
            "review_pending",
            "insufficient_evidence",
        }:
            raw_result = dict(receipt["result"])
            recorded_validation = raw_result.pop("validated")
            transcript = ledger.transcript(content_id)
            if (
                raw_result.get("network_transcript") != transcript
                or raw_result.get("network_transcript_sha256")
                != _json_sha(transcript)
            ):
                raise FullLocalAnalysisError(
                    f"item receipt {ordinal} success transcript与ledger漂移"
                )
            if receipt["status"] == "succeeded":
                current_validation = _validate_item_success_strong(
                    paths,
                    intent=intent,
                    source=source,
                    result=raw_result,
                    ledger=ledger,
                )
            elif receipt["status"] == "review_pending":
                current_validation = _validate_item_review_pending_strong(
                    paths,
                    intent=intent,
                    source=source,
                    result=raw_result,
                    ledger=ledger,
                )
            else:
                current_validation = (
                    _validate_item_insufficient_evidence_strong(
                        paths,
                        intent=intent,
                        source=source,
                        result=raw_result,
                        ledger=ledger,
                    )
                )
        else:
            failure = receipt["failure"]
            if receipt["result"].get("deferred") != failure:
                raise FullLocalAnalysisError(
                    f"item receipt {ordinal} deferred/failure未精确绑定"
                )
            recorded_validation = receipt["result"]["validated"]
            current_validation = _validate_item_deferred_exact(
                paths,
                intent=intent,
                source=source,
                ledger=ledger,
                require_global_sequence_head=require_global_sequence_head,
            )
            current_validation = {
                **current_validation,
                "failure_binding": _validate_live_deferred_failure_binding(
                    paths,
                    intent=intent,
                    source=source,
                    ledger=ledger,
                    failure=failure,
                ),
            }
    except (FullLocalAnalysisError, local.LocalAnalysisCanaryError) as exc:
        raise FullLocalAnalysisError(
            f"item receipt {ordinal} 强终态重验失败：{exc}"
        ) from exc
    if current_validation != recorded_validation:
        raise FullLocalAnalysisError(
            f"item receipt {ordinal} validated投影漂移"
        )
    return derived_sequence


def _validate_historical_item_receipts_semantic(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    *,
    item_receipts: Sequence[Mapping[str, Any]],
    runtime: RuntimeContext,
    receipt_overrides: Mapping[int, Path] | None,
) -> None:
    """Strongly re-derive every published item before any recovery write."""

    sequence_prefix: Mapping[str, int] = {
        str(name): int(value)
        for name, value in contract["sequence_baseline"].items()
    }
    _validate_exact_sequence_mapping(
        contract["sequence_baseline"], label="contract baseline"
    )
    for ordinal in range(1, len(item_receipts) + 1):
        try:
            next_sequence = _revalidate_item_receipt_current(
                paths,
                contract,
                ordinal=ordinal,
                require_global_sequence_head=False,
                runtime=runtime,
                read_only_ledger=True,
                receipt_path_override=(receipt_overrides or {}).get(ordinal),
                expected_sequence_prefix=sequence_prefix,
            )
            if next_sequence is None:
                raise FullLocalAnalysisError(
                    "historical item sequence prefix未派生"
                )
            sequence_prefix = next_sequence
        except (FullLocalAnalysisError, local.LocalAnalysisCanaryError) as exc:
            raise FullLocalAnalysisError(
                f"历史item强终态重验失败 ordinal={ordinal}: {exc}"
            ) from exc


def _revalidate_batch_item_receipts(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    ordinals: Sequence[int],
    *,
    require_global_sequence_head: bool = True,
    runtime: RuntimeContext,
    strong_ordinals: frozenset[int] | None = None,
) -> None:
    if not ordinals:
        raise FullLocalAnalysisError("batch强终态重验缺少item prefix")
    strong = (
        frozenset(int(value) for value in ordinals)
        if strong_ordinals is None
        else strong_ordinals
    )
    for index, ordinal in enumerate(ordinals):
        _revalidate_item_receipt_current(
            paths,
            contract,
            ordinal=int(ordinal),
            require_global_sequence_head=(
                require_global_sequence_head and index == len(ordinals) - 1
            ),
            runtime=runtime,
            verify_owned_content=int(ordinal) in strong,
        )


def _write_item_receipt(
    paths: BatchPaths,
    *,
    ordinal: int,
    intent: Mapping[str, Any],
    intent_path: Path,
    status: str,
    recovered_after_commit: bool,
    result: Mapping[str, Any],
    failure: Mapping[str, Any] | None,
    source: Mapping[str, Any],
    ledger: local._NetworkLedger,
) -> Mapping[str, Any]:
    success_keys = {
        "content_id",
        "media",
        "evaluation",
        "fingerprint_source_sha256",
        "network_transcript",
        "network_transcript_sha256",
        "validated",
    }
    if (
        type(status) is not str
        or status not in ITEM_TERMINAL_STATUSES
        or type(recovered_after_commit) is not bool
        or not isinstance(result, Mapping)
        or (
            status in {"succeeded", "review_pending", "insufficient_evidence"}
            and (failure is not None or set(result) != success_keys)
        )
        or (
            status == "deferred"
            and (
                not isinstance(failure, Mapping)
                or set(failure) != {"type", "message"}
                or set(result) != {"deferred", "validated"}
            )
        )
    ):
        raise FullLocalAnalysisError("item receipt终态参数形状无效")
    ledger.require_terminal()
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "ordinal": ordinal,
        "batch_index": int(intent["batch_index"]),
        "content_id": int(intent["content_id"]),
        "status": status,
        "recovered_after_commit": recovered_after_commit,
        "intent_sha256": local._sha256_file(intent_path),
        "previous_item_receipt_sha256": intent[
            "previous_item_receipt_sha256"
        ],
        "provider_calls": 0,
        "result": result,
        "failure": failure,
        "after": _item_after(
            paths,
            content_id=int(intent["content_id"]),
            source=source,
            ledger=ledger,
        ),
    }
    _write_exclusive(_item_paths(paths, ordinal)[1], receipt)
    return receipt


def _recover_item_receipt(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    *,
    ordinal: int,
    intent: Mapping[str, Any],
    intent_path: Path,
    source: Mapping[str, Any],
    ledger: local._NetworkLedger,
    runtime: RuntimeContext,
) -> Mapping[str, Any] | None:
    content_id = int(intent["content_id"])
    receipt_temporary = _item_paths(paths, ordinal)[1].with_name(
        f".{_item_paths(paths, ordinal)[1].name}.tmp"
    )
    interrupted_immutable_write = os.path.lexists(receipt_temporary)
    if local._database_sidecars(paths.database):
        if (
            not runtime.sidecar_readset
            or local._canonical_bytes(
                _database_readset_snapshot(paths.database)
            )
            != local._canonical_bytes(runtime.sidecar_readset)
        ):
            raise FullLocalAnalysisError(
                "WAL finalize前原main/wal/shm证据发生漂移"
            )
        try:
            local._finalize_database(paths.database)
        except local.LocalAnalysisCanaryError as exc:
            raise FullLocalAnalysisError(
                f"DB/WAL无法恢复到item receipt：{exc}"
            ) from exc
        _validate_finalized_sidecar_projection(paths, contract, runtime)
    before = intent.get("before")
    if not isinstance(before, Mapping):
        raise FullLocalAnalysisError("item intent缺少before闭包")
    current_rows = _item_target_rows(paths.database, content_id)
    current_outputs = _item_output_inventory(paths, source)
    with closing(local._immutable_connection(paths.database)) as connection:
        current_sequences = _sequence_snapshot(connection)
    materialization_is_exactly_before = (
        _json_sha(current_rows) == before.get("target_rows_sha256")
        and _target_sequence_projection(current_rows)
        == before.get("target_sequences")
        and _content_direction(paths.database, content_id)
        == before.get("content_direction")
        and current_sequences == before.get("sequences")
        and current_outputs == before.get("outputs")
    )
    if materialization_is_exactly_before:
        return None
    result: Mapping[str, Any]
    terminal_status: str
    try:
        result = _project_success_result(
            paths, content_id=content_id, source=source, ledger=ledger
        )
        validated = _validate_item_success_strong(
            paths,
            intent=intent,
            source=source,
            result=result,
            ledger=ledger,
        )
        terminal_status = "succeeded"
    except (KeyError, FullLocalAnalysisError, local.LocalAnalysisCanaryError):
        try:
            result = _project_success_result(
                paths, content_id=content_id, source=source, ledger=ledger
            )
            validated = _validate_item_review_pending_strong(
                paths,
                intent=intent,
                source=source,
                result=result,
                ledger=ledger,
            )
            terminal_status = "review_pending"
        except (
            KeyError,
            FullLocalAnalysisError,
            local.LocalAnalysisCanaryError,
        ):
            try:
                result = _project_insufficient_result(
                    paths,
                    content_id=content_id,
                    source=source,
                    ledger=ledger,
                )
                validated = _validate_item_insufficient_evidence_strong(
                    paths,
                    intent=intent,
                    source=source,
                    result=result,
                    ledger=ledger,
                )
                terminal_status = "insufficient_evidence"
            except (
                KeyError,
                FullLocalAnalysisError,
                local.LocalAnalysisCanaryError,
            ):
                terminal_status = ""
    if not terminal_status:
        if not ledger.value["events"]:
            raise FullLocalAnalysisError(
                "DB/output变化未精确闭合认可分析终态且无network event"
            )
        try:
            deferred = _validate_item_deferred_exact(
                paths, intent=intent, source=source, ledger=ledger
            )
        except (FullLocalAnalysisError, local.LocalAnalysisCanaryError) as exc:
            raise FullLocalAnalysisError(
                "network terminal后DB/output不是认可deferred前缀"
            ) from exc
        try:
            failure, failure_binding = _recover_anchored_deferred_failure(
                paths,
                intent=intent,
                source=source,
                ledger=ledger,
            )
        except FullLocalAnalysisError as exc:
            raise FullLocalAnalysisError(
                "manual_required: network terminal后的DB前缀缺少durable "
                "processing attempt anchor"
            ) from exc
        deferred = {
            **deferred,
            "failure_binding": failure_binding,
        }
        return _write_item_receipt(
            paths,
            ordinal=ordinal,
            intent=intent,
            intent_path=intent_path,
            status="deferred",
            recovered_after_commit=not interrupted_immutable_write,
            result={"deferred": failure, "validated": deferred},
            failure=failure,
            source=source,
            ledger=ledger,
        )
    return _write_item_receipt(
        paths,
        ordinal=ordinal,
        intent=intent,
        intent_path=intent_path,
        status=terminal_status,
        recovered_after_commit=not interrupted_immutable_write,
        result={**result, "validated": validated},
        failure=None,
        source=source,
        ledger=ledger,
    )


def _run_item(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    *,
    ordinal: int,
    batch_index: int,
    batch_consumed_before: int,
    runtime: RuntimeContext,
) -> Mapping[str, Any] | None:
    content_id = int(contract["processing_order"][ordinal - 1])
    intent_path, receipt_path = _item_paths(paths, ordinal)
    previous_receipt = (
        local._sha256_file(_item_paths(paths, ordinal - 1)[1])
        if ordinal > 1
        else None
    )
    with closing(local._immutable_connection(paths.database)) as connection:
        try:
            source = _source_snapshot(
                connection,
                content_id,
                source_evidence=contract["source_completion"],
                row_map=runtime.target_row_map,
                discovery_raw_cache=runtime.discovery_raw_cache,
            )
        except local.LocalAnalysisCanaryError as exc:
            raise FullLocalAnalysisError(
                f"content {content_id} 处理前source漂移：{exc}"
            ) from exc
    if contract["source_summaries"][str(content_id)] != {
        "content_id": content_id,
        "source_sha256": source["artifact_body"]["source_sha256"],
        "raw_response_body_sha256": source["raw_response_body_sha256"],
        "download_urls_sha256": source["download_urls_sha256"],
        "media_kind": source["artifact_body"]["media_kind"],
        "image_groups_sha256": (
            source["image_groups_sha256"]
            if source["artifact_body"]["media_kind"] == "image"
            else None
        ),
    }:
        raise FullLocalAnalysisError(f"content {content_id} source摘要漂移")
    if type(batch_consumed_before) is not int or not (
        0 <= batch_consumed_before <= BATCH_DOWNLOAD_CAP_BYTES
    ):
        raise FullLocalAnalysisError("batch prior network budget证据无效")
    remaining = BATCH_DOWNLOAD_CAP_BYTES - batch_consumed_before
    if not intent_path.exists() and remaining <= 0:
        return None
    if intent_path.exists():
        intent = _read_json(intent_path, label="pending item intent")
        if (
            intent.get("ordinal") != ordinal
            or intent.get("batch_index") != batch_index
            or intent.get("content_id") != content_id
            or intent.get("previous_item_receipt_sha256") != previous_receipt
            or intent.get("contract_sha256") != runtime.contract_sha256
        ):
            raise FullLocalAnalysisError("pending item intent漂移")
        maximum_bytes = int(intent.get("network_maximum_bytes", -1))
        if maximum_bytes <= 0 or maximum_bytes != remaining:
            raise FullLocalAnalysisError("item intent共享下载预算漂移")
    else:
        maximum_bytes = remaining
        before = _item_before(
            paths, content_id=content_id, source=source
        )
        intent = {
            "schema_version": SCHEMA_VERSION,
            "ordinal": ordinal,
            "batch_index": batch_index,
            "content_id": content_id,
            "contract_sha256": runtime.contract_sha256,
            "previous_item_receipt_sha256": previous_receipt,
            "source_summary": contract["source_summaries"][str(content_id)],
            "network_maximum_bytes": maximum_bytes,
            "before": before,
        }
        _write_exclusive(intent_path, intent)
    ledger, budget = _item_network(
        paths,
        contract,
        ordinal=ordinal,
        content_id=content_id,
        intent_path=intent_path,
        source=source,
        maximum_bytes=maximum_bytes,
        runtime=runtime,
    )
    if receipt_path.exists():
        return _read_json(receipt_path, label="item receipt")
    recovered = _recover_item_receipt(
        paths,
        contract,
        ordinal=ordinal,
        intent=intent,
        intent_path=intent_path,
        source=source,
        ledger=ledger,
        runtime=runtime,
    )
    if recovered is not None:
        return recovered
    if ledger.value["events"]:
        raise FullLocalAnalysisError(
            "manual_required: durable network event缺少DB processing attempt; "
            "request replay与自动deferred均被禁止"
        )
    status = "succeeded"
    failure: Mapping[str, Any] | None = None
    try:
        result = _execute_item_pipeline(
            paths,
            content_id=content_id,
            source=source,
            tools=contract["tools"],
            ledger=ledger,
            budget=budget,
        )
        evidence_level = result.get("evaluation", {}).get("evidence_level")
        if evidence_level in {"V0", "V1"}:
            status = "insufficient_evidence"
        elif evidence_level not in {"V2", "V3"}:
            raise FullLocalAnalysisError(
                f"content {content_id} pipeline evaluation level漂移"
            )
    except BaseException as exc:
        if not _controlled_failure(exc):
            raise
        if not ledger.value["events"]:
            raise FullLocalAnalysisError(
                "controlled deferred缺少durable network event"
            ) from exc
        ledger.require_terminal()
        status = "deferred"
        failure = {"type": type(exc).__name__, "message": str(exc)}
        _stamp_live_deferred_attempt_anchor(
            paths,
            intent=intent,
            source=source,
            ledger=ledger,
            failure=failure,
        )
        result = {"deferred": failure}
    try:
        local._finalize_database(paths.database)
    except local.LocalAnalysisCanaryError as exc:
        raise FullLocalAnalysisError(f"item数据库finalize失败：{exc}") from exc
    _after_item_database_commit(content_id)
    ledger.require_terminal()
    if status == "succeeded":
        try:
            validated = _validate_item_success_strong(
                paths,
                intent=intent,
                source=source,
                result=result,
                ledger=ledger,
            )
        except (
            FullLocalAnalysisError,
            local.LocalAnalysisCanaryError,
        ):
            validated = _validate_item_review_pending_strong(
                paths,
                intent=intent,
                source=source,
                result=result,
                ledger=ledger,
            )
            status = "review_pending"
    elif status == "insufficient_evidence":
        validated = _validate_item_insufficient_evidence_strong(
            paths,
            intent=intent,
            source=source,
            result=result,
            ledger=ledger,
        )
    else:
        if failure is None:
            raise FullLocalAnalysisError("deferred item缺少failure证据")
        validated = _validate_item_deferred_exact(
            paths, intent=intent, source=source, ledger=ledger
        )
        validated = {
            **validated,
            "failure_binding": _validate_live_deferred_failure_binding(
                paths,
                intent=intent,
                source=source,
                ledger=ledger,
                failure=failure,
            ),
        }
    return _write_item_receipt(
        paths,
        ordinal=ordinal,
        intent=intent,
        intent_path=intent_path,
        status=status,
        recovered_after_commit=False,
        result={**result, "validated": validated},
        failure=failure,
        source=source,
        ledger=ledger,
    )


def _batch_at_cursor(
    contract: Mapping[str, Any],
    completed_items: int,
    runtime: RuntimeContext | None = None,
) -> list[int]:
    if runtime is not None:
        if completed_items >= len(runtime.processing_order):
            return []
        cached = runtime.batch_ids_by_cursor.get(completed_items)
        if cached is None:
            raise FullLocalAnalysisError(
                "completed cursor未命中冻结processing batch map"
            )
        return list(cached)
    order = contract["processing_order"]
    if completed_items >= len(order):
        return []
    profile = contract["profile"]
    if completed_items == 0:
        result = order[: len(profile["first_batch_ids"])]
    else:
        first_id = order[completed_items]
        kind = str(contract["source_summaries"][str(first_id)]["media_kind"])
        size = (
            int(profile["image_batch_size"])
            if kind == "image"
            else int(profile["video_batch_size"])
        )
        result = order[completed_items : completed_items + size]
        result = [
            content_id
            for content_id in result
            if str(contract["source_summaries"][str(content_id)]["media_kind"])
            == kind
        ]
    kinds = {
        str(contract["source_summaries"][str(content_id)]["media_kind"])
        for content_id in result
    }
    if len(kinds) > 1 or not result:
        raise FullLocalAnalysisError("batch必须非空且不得混合media kind")
    return result


def _absolute_batch_plan(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    *,
    completed_batches: int,
    completed_items: int,
    through_batch: int,
    runtime: RuntimeContext | None = None,
) -> Mapping[str, Any]:
    intents = _batch_intent_files(paths)
    receipts = _batch_receipt_files(paths)
    pending = len(intents) == len(receipts) + 1
    if pending:
        pending_index = completed_batches + 1
        if through_batch != pending_index:
            raise FullLocalAnalysisError(
                f"存在pending batch {pending_index}，绝对停点必须等于该索引"
            )
        intent = _read_json(intents[-1], label="pending batch intent")
        return {
            "mode": "resume_pending",
            "batch_index": pending_index,
            "content_ids": [int(value) for value in intent["content_ids"]],
            "new_batch": False,
        }
    if through_batch == completed_batches:
        return {
            "mode": "idempotent",
            "batch_index": completed_batches,
            "content_ids": [],
            "new_batch": False,
        }
    if through_batch != completed_batches + 1:
        raise FullLocalAnalysisError(
            "绝对停点只能等于当前完成batch或显式+1"
        )
    if runtime is not None:
        next_ids = list(
            runtime.batch_ids_by_cursor.get(completed_items, ())
        )
    else:
        next_ids = _batch_at_cursor(contract, completed_items)
    if not next_ids:
        raise FullLocalAnalysisError(
            "eligible已全部闭合；绝对停点只能保持当前completed batch"
        )
    return {
        "mode": "new_batch",
        "batch_index": through_batch,
        "content_ids": next_ids,
        "new_batch": True,
    }


def _validate_provider_snapshot_value(value: Any, *, label: str) -> None:
    expected_tables = {"provider_usage", "provider_budget_batches"}
    if not isinstance(value, Mapping) or set(value) != expected_tables:
        raise FullLocalAnalysisError(f"{label} provider snapshot形状漂移")
    for table, digest in value.items():
        if (
            type(table) is not str
            or not isinstance(digest, Mapping)
            or set(digest) != {"rows", "sha256"}
            or type(digest.get("rows")) is not int
            or digest["rows"] < 0
            or type(digest.get("sha256")) is not str
            or re.fullmatch(r"[0-9a-f]{64}", digest["sha256"]) is None
        ):
            raise FullLocalAnalysisError(
                f"{label} provider snapshot nested类型漂移"
            )


def _validate_item_output_inventory_value(value: Any, *, label: str) -> None:
    if not isinstance(value, Mapping) or set(value) != {
        "media",
        "fingerprints",
    }:
        raise FullLocalAnalysisError(f"{label} item output形状漂移")
    for output_label, inventory in value.items():
        if (
            type(output_label) is not str
            or not isinstance(inventory, Mapping)
            or set(inventory) != {"files", "rows_sha256", "rows"}
            or type(inventory.get("files")) is not int
            or inventory["files"] < 0
            or not isinstance(inventory.get("rows"), list)
            or inventory["files"] != len(inventory["rows"])
            or type(inventory.get("rows_sha256")) is not str
            or inventory["rows_sha256"] != _json_sha(inventory["rows"])
        ):
            raise FullLocalAnalysisError(
                f"{label} item output nested类型/hash漂移"
            )


def _validate_batch_protected_marker_value(
    value: Any,
    *,
    label: str,
) -> None:
    if (
        not isinstance(value, Mapping)
        or set(value)
        != {"policy", "content_ids", "planned_baselines_sha256"}
        or value.get("policy")
        != "invocation_global_guard_plus_planned_items_v1"
        or not isinstance(value.get("content_ids"), list)
        or any(type(content_id) is not int for content_id in value["content_ids"])
        or type(value.get("planned_baselines_sha256")) is not str
        or re.fullmatch(
            r"[0-9a-f]{64}", value["planned_baselines_sha256"]
        )
        is None
    ):
        raise FullLocalAnalysisError(f"{label} protected marker精确类型漂移")


def _validate_planned_baselines_value(
    value: Any,
    *,
    content_ids: Sequence[int],
    contract: Mapping[str, Any],
    frozen_by_id: Mapping[int, Mapping[str, Any]],
) -> None:
    if not isinstance(value, list) or len(value) != len(content_ids):
        raise FullLocalAnalysisError("batch planned baselines形状漂移")
    for expected_content_id, baseline in zip(
        content_ids, value, strict=True
    ):
        if (
            not isinstance(baseline, Mapping)
            or set(baseline)
            != {
                "content_id",
                "target_rows",
                "target_rows_sha256",
                "target_sequences",
                "content_direction",
                "outputs",
            }
            or type(baseline.get("content_id")) is not int
            or baseline["content_id"] != expected_content_id
            or not isinstance(baseline.get("target_rows"), Mapping)
            or type(baseline.get("target_rows_sha256")) is not str
            or baseline["target_rows_sha256"]
            != _json_sha(baseline["target_rows"])
        ):
            raise FullLocalAnalysisError(
                "batch planned baseline字段/target hash漂移"
            )
        _validate_exact_sequence_mapping(
            baseline["target_sequences"],
            label=f"planned baseline {expected_content_id}",
        )
        if local._canonical_bytes(
            baseline["target_sequences"]
        ) != local._canonical_bytes(
            _target_sequence_projection(baseline["target_rows"])
        ):
            raise FullLocalAnalysisError(
                "batch planned baseline target sequence未重派生"
            )
        _validate_item_output_inventory_value(
            baseline["outputs"],
            label=f"planned baseline {expected_content_id}",
        )
        frozen = frozen_by_id.get(expected_content_id)
        if frozen is None or any(
            (
                local._canonical_bytes(baseline["content_direction"])
                != local._canonical_bytes(frozen["content_direction"])
            )
            if name == "content_direction"
            else baseline[name] != frozen[name]
            for name in ("target_rows_sha256", "content_direction")
        ):
            raise FullLocalAnalysisError(
                "batch planned baseline偏离contract initial target"
            )
        if local._canonical_bytes(
            baseline["outputs"]
        ) != local._canonical_bytes(contract["output_baseline"]):
            raise FullLocalAnalysisError(
                "batch planned baseline不是冻结empty output"
            )


def _validate_batch_before_value(
    value: Any,
    *,
    batch_index: int,
    content_ids: Sequence[int],
    planned_baselines_sha256: str,
    expected_database: Mapping[str, Any],
    expected_sequences: Mapping[str, Any],
    expected_outputs: Mapping[str, Any],
    contract: Mapping[str, Any],
) -> None:
    if not isinstance(value, Mapping) or set(value) != {
        "database",
        "provider",
        "protected",
        "protected_sha256",
        "sequences",
        "outputs",
    }:
        raise FullLocalAnalysisError(
            f"batch intent {batch_index} before字段漂移"
        )
    _validate_logical_database_checkpoint(
        value["database"], expected=expected_database
    )
    _validate_provider_snapshot_value(
        value["provider"], label=f"batch intent {batch_index} before"
    )
    if local._canonical_bytes(value["provider"]) != local._canonical_bytes(
        contract["provider_baseline"]
    ):
        raise FullLocalAnalysisError(
            f"batch intent {batch_index} before provider漂移"
        )
    expected_protected = _batch_protected_marker(
        content_ids, planned_baselines_sha256
    )
    _validate_batch_protected_marker_value(
        value["protected"], label=f"batch intent {batch_index} before"
    )
    if (
        local._canonical_bytes(value["protected"])
        != local._canonical_bytes(expected_protected)
        or type(value.get("protected_sha256")) is not str
        or value["protected_sha256"] != _json_sha(expected_protected)
    ):
        raise FullLocalAnalysisError(
            f"batch intent {batch_index} before protected marker漂移"
        )
    _validate_exact_sequence_mapping(
        value["sequences"], label=f"batch intent {batch_index} before"
    )
    if local._canonical_bytes(value["sequences"]) != local._canonical_bytes(
        expected_sequences
    ):
        raise FullLocalAnalysisError(
            f"batch intent {batch_index} before sequence prefix漂移"
        )
    if set(value["outputs"]) == {"media", "fingerprints"}:
        _validate_item_output_inventory_value(
            value["outputs"], label=f"batch intent {batch_index} before"
        )
    else:
        _validate_checkpoint_closure_shape(
            database=value["database"], outputs=value["outputs"]
        )
    if local._canonical_bytes(value["outputs"]) != local._canonical_bytes(
        expected_outputs
    ):
        raise FullLocalAnalysisError(
            f"batch intent {batch_index} before output baseline漂移"
        )


def _validate_batch_after_value(
    value: Any,
    *,
    batch_index: int,
    contract: Mapping[str, Any],
    expected_sequences: Mapping[str, Any],
) -> None:
    if not isinstance(value, Mapping) or set(value) != {
        "database",
        "provider",
        "protected_sha256",
        "sequences",
        "outputs",
    }:
        raise FullLocalAnalysisError(f"batch receipt {batch_index} after字段漂移")
    _validate_checkpoint_closure_shape(
        database=value["database"], outputs=value["outputs"]
    )
    _validate_provider_snapshot_value(
        value["provider"], label=f"batch receipt {batch_index} after"
    )
    _validate_exact_sequence_mapping(
        value["sequences"], label=f"batch receipt {batch_index} after"
    )
    if (
        local._canonical_bytes(value["provider"])
        != local._canonical_bytes(contract["provider_baseline"])
        or type(value.get("protected_sha256")) is not str
        or value["protected_sha256"]
        != contract["protected_baseline_sha256"]
        or local._canonical_bytes(value["sequences"])
        != local._canonical_bytes(expected_sequences)
    ):
        raise FullLocalAnalysisError(
            f"batch receipt {batch_index} after全局闭包漂移"
        )


def _item_top_level_ownership_rows(
    paths: BatchPaths,
    *,
    ordinal: int,
    receipt: Mapping[str, Any],
    eligible_baseline_by_id: Mapping[int, Mapping[str, Any]],
    runtime: RuntimeContext | None,
) -> Mapping[str, Any]:
    if runtime is not None:
        cached = runtime.item_ownership_rows_by_ordinal.get(ordinal)
        if cached is not None:
            return cached
    content_id = receipt.get("content_id")
    if type(content_id) is not int:
        raise FullLocalAnalysisError("output prefix item content ID类型漂移")
    frozen = eligible_baseline_by_id.get(content_id)
    if frozen is None:
        raise FullLocalAnalysisError("output prefix item不属于eligible baseline")
    outputs = receipt.get("after", {}).get("outputs")
    _validate_item_output_inventory_value(
        outputs, label=f"item {ordinal} output prefix"
    )
    media_rows: list[Mapping[str, Any]] = []
    fingerprint_rows: list[Mapping[str, Any]] = []
    if outputs["media"]["rows"]:
        media_path = paths.media_root / str(frozen["link_id"])
        try:
            metadata = local._private_directory(
                media_path, label="historical item media ownership"
            )
        except local.LocalAnalysisCanaryError as exc:
            raise FullLocalAnalysisError(str(exc)) from exc
        media_rows.append(
            {
                "name": media_path.name,
                "kind": "directory",
                "device": metadata.st_dev,
                "inode": metadata.st_ino,
                "mode": stat.S_IMODE(metadata.st_mode),
            }
        )
    for output_row in outputs["fingerprints"]["rows"]:
        if (
            not isinstance(output_row, Mapping)
            or type(output_row.get("path")) is not str
            or Path(output_row["path"]).name != output_row["path"]
        ):
            raise FullLocalAnalysisError(
                "historical item fingerprint output path漂移"
            )
        fingerprint_path = paths.fingerprint_root / output_row["path"]
        try:
            metadata = local._private_file(
                fingerprint_path,
                label="historical item fingerprint ownership",
            )
        except local.LocalAnalysisCanaryError as exc:
            raise FullLocalAnalysisError(str(exc)) from exc
        fingerprint_rows.append(
            {
                "name": fingerprint_path.name,
                "kind": "file",
                "device": metadata.st_dev,
                "inode": metadata.st_ino,
                "nlink": metadata.st_nlink,
                "mode": stat.S_IMODE(metadata.st_mode),
                "byte_size": metadata.st_size,
            }
        )
    result = {
        "media": media_rows,
        "fingerprints": fingerprint_rows,
    }
    if runtime is not None:
        runtime.item_ownership_rows_by_ordinal[ordinal] = result
    return result


def _output_closure_from_ownership_maps(
    media_rows: Mapping[str, Mapping[str, Any]],
    fingerprint_rows: Mapping[str, Mapping[str, Any]],
) -> Mapping[str, Any]:
    ownership: dict[str, Mapping[str, Any]] = {}
    for label, row_map in (
        ("media", media_rows),
        ("fingerprints", fingerprint_rows),
    ):
        rows = [row_map[name] for name in sorted(row_map)]
        ownership[label] = {
            "entries": len(rows),
            "rows_sha256": _json_sha(rows),
            "rows": rows,
        }
    return {
        "coverage": "owned_delta",
        "ownership": ownership,
        "content_inventory": None,
    }


def _validate_batch_chain(
    paths: BatchPaths,
    runtime: RuntimeContext | None = None,
    *,
    batch_receipt_overrides: Mapping[int, Path] | None = None,
    item_receipt_overrides: Mapping[int, Path] | None = None,
) -> list[Mapping[str, Any]]:
    intents = _batch_intent_files(paths)
    receipts = _effective_receipt_paths(
        paths.batches_root,
        ".receipt.json",
        intent_count=len(intents),
        overrides=batch_receipt_overrides,
    )
    contract = _read_json(paths.contract, label="global contract")
    contract_sha256 = (
        runtime.contract_sha256
        if runtime is not None
        else local._sha256_file(paths.contract)
    )
    previous: str | None = None
    rows: list[Mapping[str, Any]] = []
    next_ordinal = 1
    previous_after: Mapping[str, Any] | None = None
    initial_database = (
        runtime.logical_checkpoints_by_count[0]
        if runtime is not None
        else None
    )
    eligible_baseline_by_id = (
        runtime.eligible_baseline_by_id
        if runtime is not None
        else _eligible_baseline_map(contract)
    )
    expected_media_ownership: dict[str, Mapping[str, Any]] = {}
    expected_fingerprint_ownership: dict[str, Mapping[str, Any]] = {}
    ownership_maps_ready = False
    previous_completed_count = 0
    for index, intent_path in enumerate(intents, 1):
        intent = _read_json(intent_path, label="batch intent")
        content_ids = intent.get("content_ids")
        item_ordinals = intent.get("item_ordinals")
        before = intent.get("before")
        planned_baselines_sha256 = intent.get("planned_baselines_sha256")
        if runtime is not None:
            expected_content_ids = list(
                runtime.batch_ids_by_cursor.get(next_ordinal - 1, ())
            )
        else:
            expected_content_ids = _batch_at_cursor(
                contract, next_ordinal - 1
            )
        expected_before_database = (
            initial_database
            if index == 1
            else (
                previous_after.get("database")
                if previous_after is not None
                else None
            )
        )
        expected_before_sequences = (
            contract["sequence_baseline"]
            if index == 1
            else (
                previous_after.get("sequences")
                if previous_after is not None
                else None
            )
        )
        expected_before_outputs = (
            contract["output_baseline"]
            if index == 1
            else (
                previous_after.get("outputs")
                if previous_after is not None
                else None
            )
        )
        if (
            not isinstance(content_ids, list)
            or not content_ids
            or any(type(value) is not int for value in content_ids)
            or not isinstance(item_ordinals, list)
            or any(type(value) is not int for value in item_ordinals)
            or not isinstance(intent.get("planned_baselines"), list)
            or type(planned_baselines_sha256) is not str
            or planned_baselines_sha256
            != _json_sha(intent["planned_baselines"])
            or not isinstance(expected_before_database, Mapping)
            or not isinstance(expected_before_sequences, Mapping)
            or not isinstance(expected_before_outputs, Mapping)
        ):
            raise FullLocalAnalysisError(
                f"batch intent {index} nested合同前置形状漂移"
            )
        _validate_planned_baselines_value(
            intent["planned_baselines"],
            content_ids=content_ids,
            contract=contract,
            frozen_by_id=eligible_baseline_by_id,
        )
        _validate_batch_before_value(
            before,
            batch_index=index,
            content_ids=content_ids,
            planned_baselines_sha256=planned_baselines_sha256,
            expected_database=expected_before_database,
            expected_sequences=expected_before_sequences,
            expected_outputs=expected_before_outputs,
            contract=contract,
        )
        if (
            set(intent)
            != {
                "schema_version",
                "batch_index",
                "contract_sha256",
                "previous_batch_receipt_sha256",
                "content_ids",
                "content_ids_sha256",
                "item_ordinals",
                "planned_baselines",
                "planned_baselines_sha256",
                "before",
            }
            or intent.get("schema_version") != SCHEMA_VERSION
            or type(intent.get("batch_index")) is not int
            or intent.get("batch_index") != index
            or intent.get("contract_sha256") != contract_sha256
            or intent.get("previous_batch_receipt_sha256") != previous
            or not isinstance(content_ids, list)
            or not content_ids
            or any(type(value) is not int for value in content_ids)
            or intent.get("content_ids_sha256") != _json_sha(content_ids)
            or not isinstance(item_ordinals, list)
            or item_ordinals
            != list(range(next_ordinal, next_ordinal + len(content_ids)))
            or content_ids != expected_content_ids
            or not isinstance(intent.get("planned_baselines"), list)
            or [
                row.get("content_id")
                for row in intent.get("planned_baselines", [])
                if isinstance(row, Mapping)
            ]
            != content_ids
            or intent.get("planned_baselines_sha256")
            != _json_sha(intent.get("planned_baselines"))
            or not isinstance(before, Mapping)
            or set(before)
            != {
                "database",
                "provider",
                "protected",
                "protected_sha256",
                "sequences",
                "outputs",
            }
            or before.get("provider") != contract["provider_baseline"]
            or before.get("protected_sha256")
            != _json_sha(before.get("protected"))
            or before.get("protected")
            != _batch_protected_marker(
                content_ids, planned_baselines_sha256
            )
            or not isinstance(before.get("database"), Mapping)
            or not isinstance(before.get("sequences"), Mapping)
            or not isinstance(before.get("outputs"), Mapping)
            or (
                index == 1
                and (
                    local._canonical_bytes(before.get("database"))
                    != local._canonical_bytes(initial_database)
                    or local._canonical_bytes(before.get("sequences"))
                    != local._canonical_bytes(contract["sequence_baseline"])
                )
            )
            or (
                previous_after is not None
                and any(
                    local._canonical_bytes(before.get(key))
                    != local._canonical_bytes(previous_after.get(key))
                    for key in ("database", "provider", "sequences", "outputs")
                )
            )
        ):
            raise FullLocalAnalysisError("batch intent字段/起点闭包漂移")
        if index > len(receipts):
            continue
        receipt_path = receipts[index - 1]
        receipt = _read_json(receipt_path, label="batch receipt")
        completed_content_ids = receipt.get("completed_content_ids")
        unstarted_content_ids = receipt.get("unstarted_content_ids")
        if (
            not isinstance(completed_content_ids, list)
            or not completed_content_ids
            or any(type(value) is not int for value in completed_content_ids)
            or not isinstance(unstarted_content_ids, list)
            or any(type(value) is not int for value in unstarted_content_ids)
        ):
            raise FullLocalAnalysisError(
                f"batch receipt {index} completed/unstarted IDs类型漂移"
            )
        completed_ordinals = item_ordinals[: len(completed_content_ids)]
        item_receipt_paths = [
            (item_receipt_overrides or {}).get(
                ordinal, _item_paths(paths, ordinal)[1]
            )
            for ordinal in completed_ordinals
        ]
        item_receipt_rows = [
            [ordinal, local._sha256_file(receipt_path)]
            for ordinal, receipt_path in zip(
                completed_ordinals, item_receipt_paths, strict=True
            )
        ]
        item_receipts = [
            _read_json(receipt_path, label="batch item receipt")
            for receipt_path in item_receipt_paths
        ]
        after = receipt.get("after")
        status = receipt.get("status")
        _validate_batch_after_value(
            after,
            batch_index=index,
            contract=contract,
            expected_sequences=item_receipts[-1]["after"]["sequences"],
        )
        if not isinstance(after, Mapping):
            raise FullLocalAnalysisError(
                f"batch receipt {index} after闭包形状无效"
            )
        completed_count = int(completed_ordinals[-1])
        expected_outputs = (
            runtime.expected_output_closures_by_count.get(completed_count)
            if runtime is not None
            else None
        )
        if expected_outputs is None:
            if not ownership_maps_ready:
                previous_outputs = (
                    runtime.expected_output_closures_by_count.get(
                        previous_completed_count
                    )
                    if runtime is not None and previous_completed_count
                    else None
                )
                if previous_outputs is not None:
                    _validate_checkpoint_closure_shape(
                        database=after["database"],
                        outputs=previous_outputs,
                    )
                    expected_media_ownership.update(
                        {
                            str(row["name"]): row
                            for row in previous_outputs["ownership"]["media"][
                                "rows"
                            ]
                        }
                    )
                    expected_fingerprint_ownership.update(
                        {
                            str(row["name"]): row
                            for row in previous_outputs["ownership"][
                                "fingerprints"
                            ]["rows"]
                        }
                    )
                ownership_maps_ready = True
            for ordinal, item_receipt in zip(
                completed_ordinals, item_receipts, strict=True
            ):
                owned = _item_top_level_ownership_rows(
                    paths,
                    ordinal=ordinal,
                    receipt=item_receipt,
                    eligible_baseline_by_id=eligible_baseline_by_id,
                    runtime=runtime,
                )
                for label, target in (
                    ("media", expected_media_ownership),
                    ("fingerprints", expected_fingerprint_ownership),
                ):
                    for row in owned[label]:
                        name = str(row["name"])
                        if name in target:
                            raise FullLocalAnalysisError(
                                "historical output prefix ownership名称碰撞"
                            )
                        target[name] = row
            expected_outputs = _output_closure_from_ownership_maps(
                expected_media_ownership,
                expected_fingerprint_ownership,
            )
            if runtime is not None:
                runtime.expected_output_closures_by_count[
                    completed_count
                ] = expected_outputs
        if expected_outputs is None:
            raise FullLocalAnalysisError(
                f"batch receipt {index} output prefix无法重派生"
            )
        if local._canonical_bytes(after["outputs"]) != local._canonical_bytes(
            expected_outputs
        ):
            raise FullLocalAnalysisError(
                f"batch receipt {index} after output prefix未独立重派生"
            )
        previous_completed_count = completed_count
        expected_audit = (
            _batch_audit_value(
                paths,
                contract,
                batch_index=index,
                completed_ordinals=completed_ordinals,
                after=after,
                contract_sha256=contract_sha256,
                item_receipt_overrides=item_receipt_overrides,
                batch_receipt_overrides=batch_receipt_overrides,
            )
            if isinstance(after, Mapping) and completed_ordinals
            else None
        )
        _validate_batch_audit_value(
            receipt.get("audit"),
            batch_index=index,
            expected=expected_audit,
        )
        if (
            set(receipt)
            != {
                "schema_version",
                "batch_index",
                "status",
                "intent_sha256",
                "previous_batch_receipt_sha256",
                "content_ids",
                "completed_content_ids",
                "unstarted_content_ids",
                "item_receipts",
                "item_receipts_sha256",
                "provider_calls",
                "after",
                "audit",
            }
            or receipt.get("schema_version") != SCHEMA_VERSION
            or type(receipt.get("batch_index")) is not int
            or receipt.get("batch_index") != index
            or status not in {"succeeded", "budget_exhausted_partial"}
            or receipt.get("intent_sha256") != local._sha256_file(intent_path)
            or receipt.get("previous_batch_receipt_sha256") != previous
            or type(receipt.get("provider_calls")) is not int
            or receipt.get("provider_calls") != 0
            or not isinstance(receipt.get("content_ids"), list)
            or any(
                type(value) is not int for value in receipt["content_ids"]
            )
            or local._canonical_bytes(receipt["content_ids"])
            != local._canonical_bytes(content_ids)
            or local._canonical_bytes(
                completed_content_ids + unstarted_content_ids
            )
            != local._canonical_bytes(content_ids)
            or (
                status == "succeeded" and unstarted_content_ids
            )
            or (
                status == "budget_exhausted_partial"
                and not unstarted_content_ids
            )
            or receipt.get("item_receipts_sha256")
            != _json_sha(receipt.get("item_receipts"))
            or not isinstance(receipt.get("item_receipts"), list)
            or any(
                not isinstance(row, list)
                or len(row) != 2
                or type(row[0]) is not int
                or type(row[1]) is not str
                or re.fullmatch(r"[0-9a-f]{64}", row[1]) is None
                for row in receipt["item_receipts"]
            )
            or local._canonical_bytes(receipt["item_receipts"])
            != local._canonical_bytes(item_receipt_rows)
            or any(
                item.get("batch_index") != index
                or item.get("ordinal") != ordinal
                or item.get("content_id") != content_id
                for item, ordinal, content_id in zip(
                    item_receipts,
                    completed_ordinals,
                    completed_content_ids,
                    strict=True,
                )
            )
            or not isinstance(after, Mapping)
            or set(after)
            != {
                "database",
                "provider",
                "protected_sha256",
                "sequences",
                "outputs",
            }
            or local._canonical_bytes(after.get("provider"))
            != local._canonical_bytes(contract["provider_baseline"])
            or after.get("protected_sha256")
            != contract["protected_baseline_sha256"]
            or local._canonical_bytes(after.get("sequences"))
            != local._canonical_bytes(
                item_receipts[-1]["after"]["sequences"]
            )
            or not isinstance(after.get("outputs"), Mapping)
        ):
            raise FullLocalAnalysisError("batch previous链漂移")
        next_ordinal += len(completed_content_ids)
        previous = local._sha256_file(receipt_path)
        previous_after = after
        rows.append(receipt)
    return rows


def _validate_batch_output_delta(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    intent: Mapping[str, Any],
    item_ordinals: Sequence[int],
) -> None:
    before = intent["before"]["outputs"]
    if set(before) == {"coverage", "ownership", "content_inventory"}:
        before_ownership = before["ownership"]
        expected_media = {
            str(row["name"])
            for row in before_ownership["media"]["rows"]
        }
        expected_fingerprints = {
            str(row["name"])
            for row in before_ownership["fingerprints"]["rows"]
        }
    else:
        before_ownership = None
        expected_media = {
            str(row["path"]).split("/", 1)[0]
            for row in before["media"]["rows"]
        }
        expected_fingerprints = {
            str(row["path"]) for row in before["fingerprints"]["rows"]
        }
    content_ids = [int(contract["processing_order"][value - 1]) for value in item_ordinals]
    with closing(local._immutable_connection(paths.database)) as connection:
        link_by_id = {
            int(row["id"]): str(row["link_id"])
            for row in connection.execute(
                "SELECT id,link_id FROM content_items ORDER BY id"
            )
            if int(row["id"]) in set(content_ids)
        }
    for ordinal, content_id in zip(item_ordinals, content_ids, strict=True):
        receipt = _read_json(_item_paths(paths, ordinal)[1], label="item receipt")
        outputs = receipt["after"]["outputs"]
        link_id = link_by_id[content_id]
        if outputs["media"]["rows"]:
            if link_id in expected_media:
                raise FullLocalAnalysisError("batch media output路径碰撞")
            expected_media.add(link_id)
        for row in outputs["fingerprints"]["rows"]:
            name = str(row["path"])
            if name in expected_fingerprints:
                raise FullLocalAnalysisError("batch fingerprint output路径碰撞")
            expected_fingerprints.add(name)
    actual = _output_ownership(paths)
    actual_media = {str(row["name"]) for row in actual["media"]["rows"]}
    actual_fingerprints = {
        str(row["name"]) for row in actual["fingerprints"]["rows"]
    }
    if (
        actual_media != expected_media
        or actual_fingerprints != expected_fingerprints
    ):
        raise FullLocalAnalysisError("batch输出delta不等于item receipt精确闭包")
    if before_ownership is not None:
        for label in ("media", "fingerprints"):
            previous_rows = {
                str(row["name"]): row
                for row in before_ownership[label]["rows"]
            }
            current_rows = {
                str(row["name"]): row for row in actual[label]["rows"]
            }
            if any(
                local._canonical_bytes(current_rows.get(name))
                != local._canonical_bytes(row)
                for name, row in previous_rows.items()
            ):
                raise FullLocalAnalysisError("既有output ownership发生漂移")


def _planned_item_baselines(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    content_ids: Sequence[int],
    runtime: RuntimeContext,
) -> list[Mapping[str, Any]]:
    result: list[Mapping[str, Any]] = []
    with closing(local._immutable_connection(paths.database)) as connection:
        for content_id in content_ids:
            rows = local._target_rows(connection, [content_id])
            source = _source_snapshot(
                connection,
                content_id,
                source_evidence=contract["source_completion"],
                row_map=runtime.target_row_map,
                discovery_raw_cache=runtime.discovery_raw_cache,
            )
            direction = local._json_value(
                connection.execute(
                    "SELECT evaluation_content_direction FROM content_items WHERE id=?",
                    (content_id,),
                ).fetchone()[0]
            )
            result.append(
                {
                    "content_id": content_id,
                    "target_rows": rows,
                    "target_rows_sha256": _json_sha(rows),
                    "target_sequences": _target_sequence_projection(rows),
                    "content_direction": direction,
                    "outputs": _item_output_inventory(paths, source),
                }
            )
    return result


def _batch_protected_marker(
    content_ids: Sequence[int], planned_baselines_sha256: str
) -> Mapping[str, Any]:
    return {
        "policy": "invocation_global_guard_plus_planned_items_v1",
        "content_ids": list(content_ids),
        "planned_baselines_sha256": planned_baselines_sha256,
    }


def _validate_unstarted_batch_suffix(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    intent: Mapping[str, Any],
    *,
    completed_count: int,
    runtime: RuntimeContext,
) -> None:
    baselines = intent["planned_baselines"]
    with closing(local._immutable_connection(paths.database)) as connection:
        for baseline in baselines[completed_count:]:
            content_id = int(baseline["content_id"])
            rows = local._target_rows(connection, [content_id])
            source = _source_snapshot(
                connection,
                content_id,
                source_evidence=contract["source_completion"],
                row_map=runtime.target_row_map,
                discovery_raw_cache=runtime.discovery_raw_cache,
            )
            direction = local._json_value(
                connection.execute(
                    "SELECT evaluation_content_direction FROM content_items WHERE id=?",
                    (content_id,),
                ).fetchone()[0]
            )
            if (
                rows != baseline["target_rows"]
                or _json_sha(rows) != baseline["target_rows_sha256"]
                or _target_sequence_projection(rows)
                != baseline["target_sequences"]
                or direction != baseline["content_direction"]
                or _item_output_inventory(paths, source) != baseline["outputs"]
            ):
                raise FullLocalAnalysisError(
                    f"unstarted suffix content {content_id} 被提前污染"
                )


def _requires_full_checkpoint(
    contract: Mapping[str, Any],
    *,
    batch_index: int,
    completed_ordinals: Sequence[int],
) -> bool:
    eligible_final = bool(
        completed_ordinals
        and int(completed_ordinals[-1]) == len(contract["eligible_ids"])
    )
    return batch_index == 1 or batch_index % 100 == 0 or eligible_final


def _batch_audit_value(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    *,
    batch_index: int,
    completed_ordinals: Sequence[int],
    after: Mapping[str, Any],
    contract_sha256: str,
    item_receipt_overrides: Mapping[int, Path] | None = None,
    batch_receipt_overrides: Mapping[int, Path] | None = None,
) -> Mapping[str, Any]:
    if batch_index > 1:
        previous_path = (batch_receipt_overrides or {}).get(
            batch_index - 1, _batch_paths(paths, batch_index - 1)[1]
        )
        previous_receipt = _read_json(
            previous_path,
            label="previous batch audit receipt",
        )
        previous_head = previous_receipt["audit"]["logical_head_sha256"]
        previous_full = int(
            previous_receipt["audit"]["latest_logical_checkpoint_batch"]
        )
    else:
        previous_head = _json_sha(
            {
                "contract_sha256": contract_sha256,
                "database_baseline": contract["database_baseline"],
                "output_baseline": contract["output_baseline"],
            }
        )
        previous_full = 0
    item_delta: list[Mapping[str, Any]] = []
    for ordinal in completed_ordinals:
        receipt_path = (item_receipt_overrides or {}).get(
            ordinal, _item_paths(paths, ordinal)[1]
        )
        receipt = _read_json(receipt_path, label="batch audit item receipt")
        item_after = receipt["after"]
        item_delta.append(
            {
                "ordinal": ordinal,
                "content_id": receipt["content_id"],
                "status": receipt["status"],
                "receipt_sha256": local._sha256_file(receipt_path),
                "target_rows_sha256": item_after["target_rows_sha256"],
                "target_sequences": item_after["target_sequences"],
                "content_direction": item_after["content_direction"],
                "outputs_sha256": _json_sha(item_after["outputs"]),
                "network_ledger_sha256": item_after[
                    "network_ledger_sha256"
                ],
            }
        )
    delta_sha = _json_sha(item_delta)
    logical_head = _json_sha(
        {
            "previous_logical_head_sha256": previous_head,
            "batch_index": batch_index,
            "batch_delta_sha256": delta_sha,
        }
    )
    full = _requires_full_checkpoint(
        contract,
        batch_index=batch_index,
        completed_ordinals=completed_ordinals,
    )
    return {
        "policy": _audit_policy_value(),
        "previous_logical_head_sha256": previous_head,
        "batch_delta": item_delta,
        "batch_delta_sha256": delta_sha,
        "logical_head_sha256": logical_head,
        "coverage": "logical_database_checkpoint" if full else "owned_delta",
        "latest_logical_checkpoint_batch": (
            batch_index if full else previous_full
        ),
        "full_checkpoint": after["database"] if full else None,
    }


def _validate_batch_audit_value(
    audit: Any,
    *,
    batch_index: int,
    expected: Mapping[str, Any] | None = None,
) -> None:
    keys = {
        "policy",
        "previous_logical_head_sha256",
        "batch_delta",
        "batch_delta_sha256",
        "logical_head_sha256",
        "coverage",
        "latest_logical_checkpoint_batch",
        "full_checkpoint",
    }
    if not isinstance(audit, Mapping) or set(audit) != keys:
        raise FullLocalAnalysisError("batch audit字段漂移")
    delta = audit.get("batch_delta")
    if (
        audit.get("policy") != _audit_policy_value()
        or not isinstance(audit.get("previous_logical_head_sha256"), str)
        or not isinstance(delta, list)
        or any(
            not isinstance(row, Mapping)
            or set(row)
            != {
                "ordinal",
                "content_id",
                "status",
                "receipt_sha256",
                "target_rows_sha256",
                "target_sequences",
                "content_direction",
                "outputs_sha256",
                "network_ledger_sha256",
            }
            or type(row.get("ordinal")) is not int
            or type(row.get("content_id")) is not int
            or row.get("status") not in ITEM_TERMINAL_STATUSES
            or not isinstance(row.get("receipt_sha256"), str)
            or not isinstance(row.get("target_rows_sha256"), str)
            or not isinstance(row.get("target_sequences"), Mapping)
            or any(
                not isinstance(name, str) or type(value) is not int
                for name, value in row.get("target_sequences", {}).items()
            )
            or not isinstance(row.get("outputs_sha256"), str)
            or not isinstance(row.get("network_ledger_sha256"), str)
            for row in delta
        )
        or not isinstance(audit.get("batch_delta_sha256"), str)
        or audit.get("batch_delta_sha256") != _json_sha(delta)
        or not isinstance(audit.get("logical_head_sha256"), str)
        or audit.get("logical_head_sha256")
        != _json_sha(
            {
                "previous_logical_head_sha256": audit[
                    "previous_logical_head_sha256"
                ],
                "batch_index": batch_index,
                "batch_delta_sha256": audit["batch_delta_sha256"],
            }
        )
        or audit.get("coverage")
        not in {"logical_database_checkpoint", "owned_delta"}
        or type(audit.get("latest_logical_checkpoint_batch")) is not int
        or audit["latest_logical_checkpoint_batch"] <= 0
    ):
        raise FullLocalAnalysisError("batch audit nested类型/hash链漂移")
    checkpoint = audit.get("full_checkpoint")
    if audit["coverage"] == "owned_delta":
        if checkpoint is not None:
            raise FullLocalAnalysisError("owned-delta audit不得伪装full checkpoint")
    else:
        try:
            _validate_logical_database_checkpoint(
                checkpoint,
                batch_index=batch_index,
            )
        except FullLocalAnalysisError as exc:
            raise FullLocalAnalysisError(
                "full checkpoint audit形状/类型漂移"
            ) from exc
    if expected is not None and local._canonical_bytes(audit) != local._canonical_bytes(
        expected
    ):
        raise FullLocalAnalysisError("batch audit重派生值漂移")


def _run_batch(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    *,
    batch_index: int,
    content_ids: Sequence[int],
    start_ordinal: int,
    runtime: RuntimeContext,
) -> Mapping[str, Any]:
    intent_path, receipt_path = _batch_paths(paths, batch_index)
    previous = (
        local._sha256_file(_batch_paths(paths, batch_index - 1)[1])
        if batch_index > 1
        else None
    )
    item_ordinals = list(range(start_ordinal, start_ordinal + len(content_ids)))
    if not intent_path.exists():
        _validate_eligible_baseline_ids(
            paths,
            contract,
            content_ids=content_ids,
        )
        planned_baselines = _planned_item_baselines(
            paths, contract, content_ids, runtime
        )
        planned_baselines_sha256 = _json_sha(planned_baselines)
        if batch_index == 1:
            before_database = runtime.logical_checkpoints_by_count[0]
            before_outputs = contract["output_baseline"]
        else:
            previous_receipt = _read_json(
                _batch_paths(paths, batch_index - 1)[1],
                label="previous batch closure",
            )
            before_database = previous_receipt["after"]["database"]
            before_outputs = previous_receipt["after"]["outputs"]
            _validate_checkpoint_closure_current(
                paths,
                database=before_database,
                outputs=before_outputs,
                verify_full_content=False,
                runtime=runtime,
            )
        with closing(local._immutable_connection(paths.database)) as connection:
            batch_sequences = _sequence_snapshot(connection)
        batch_protected = _batch_protected_marker(
            content_ids, planned_baselines_sha256
        )
        _write_exclusive(
            intent_path,
            {
                "schema_version": SCHEMA_VERSION,
                "batch_index": batch_index,
                "contract_sha256": runtime.contract_sha256,
                "previous_batch_receipt_sha256": previous,
                "content_ids": list(content_ids),
                "content_ids_sha256": _json_sha(list(content_ids)),
                "item_ordinals": item_ordinals,
                "planned_baselines": planned_baselines,
                "planned_baselines_sha256": planned_baselines_sha256,
                "before": {
                    "database": before_database,
                    "provider": contract["provider_baseline"],
                    "protected": batch_protected,
                    "protected_sha256": _json_sha(batch_protected),
                    "sequences": batch_sequences,
                    "outputs": before_outputs,
                },
            },
        )
    intent = _read_json(intent_path, label="batch intent")
    item_ordinals = [int(value) for value in intent.get("item_ordinals", [])]
    if (
        intent.get("content_ids") != list(content_ids)
        or item_ordinals
        != list(
            range(
                int(item_ordinals[0]) if item_ordinals else -1,
                (int(item_ordinals[0]) if item_ordinals else -1)
                + len(content_ids),
            )
        )
        or intent.get("previous_batch_receipt_sha256") != previous
    ):
        raise FullLocalAnalysisError("pending batch intent漂移")
    budget_exhausted = False
    batch_consumed = 0
    for position, ordinal in enumerate(item_ordinals):
        if _item_paths(paths, ordinal)[1].exists():
            _revalidate_item_receipt_current(
                paths,
                contract,
                ordinal=ordinal,
                require_global_sequence_head=False,
                runtime=runtime,
            )
        else:
            if position:
                _revalidate_batch_item_receipts(
                    paths,
                    contract,
                    item_ordinals[:position],
                    require_global_sequence_head=False,
                    runtime=runtime,
                    strong_ordinals=frozenset(),
                )
            # A pending current item may legitimately contain a crash-window
            # materialisation.  Its strictly future suffix may not change.
            _validate_unstarted_batch_suffix(
                paths,
                contract,
                intent,
                completed_count=position + 1,
                runtime=runtime,
            )
            receipt = _run_item(
                paths,
                contract,
                ordinal=ordinal,
                batch_index=batch_index,
                batch_consumed_before=batch_consumed,
                runtime=runtime,
            )
            if receipt is None:
                budget_exhausted = True
                break
            _append_progress_for_receipt(paths, receipt)
        current_receipt = _read_json(
            _item_paths(paths, ordinal)[1], label="batch budget item receipt"
        )
        consumed = current_receipt.get("after", {}).get(
            "network_budget_consumed_bytes"
        )
        if type(consumed) is not int or consumed < 0:
            raise FullLocalAnalysisError("item network budget receipt类型漂移")
        batch_consumed += consumed
        if batch_consumed > BATCH_DOWNLOAD_CAP_BYTES:
            raise FullLocalAnalysisError("共享batch下载预算超过冻结上限")
        completed_through = ordinal - item_ordinals[0] + 1
        pending_positions = [
            index
            for index, future_ordinal in enumerate(item_ordinals)
            if index >= completed_through
            and _item_paths(paths, future_ordinal)[0].exists()
            and not _item_paths(paths, future_ordinal)[1].exists()
        ]
        if len(pending_positions) > 1:
            raise FullLocalAnalysisError("batch存在多个pending item intent")
        _revalidate_batch_item_receipts(
            paths,
            contract,
            item_ordinals[:completed_through],
            require_global_sequence_head=not pending_positions,
            runtime=runtime,
            strong_ordinals=frozenset({ordinal}),
        )
        _validate_unstarted_batch_suffix(
            paths,
            contract,
            intent,
            completed_count=(
                pending_positions[0] + 1
                if pending_positions
                else completed_through
            ),
            runtime=runtime,
        )
    completed_ordinals = [
        ordinal
        for ordinal in item_ordinals
        if _item_paths(paths, ordinal)[1].exists()
    ]
    if completed_ordinals != item_ordinals[: len(completed_ordinals)]:
        raise FullLocalAnalysisError("batch item receipts不是完成前缀")
    if not completed_ordinals:
        raise FullLocalAnalysisError("共享预算不得生成零item空batch")
    completed_count = len(completed_ordinals)
    completed_content_ids = list(content_ids[:completed_count])
    unstarted_content_ids = list(content_ids[completed_count:])
    if budget_exhausted != bool(unstarted_content_ids):
        raise FullLocalAnalysisError("batch预算终态与未启动suffix不一致")
    _revalidate_batch_item_receipts(
        paths, contract, completed_ordinals, runtime=runtime
    )
    _validate_unstarted_batch_suffix(
        paths,
        contract,
        intent,
        completed_count=completed_count,
        runtime=runtime,
    )
    item_receipt_rows = [
        [ordinal, local._sha256_file(_item_paths(paths, ordinal)[1])]
        for ordinal in completed_ordinals
    ]
    if not receipt_path.exists():
        _validate_invocation_managed_appends(
            paths,
            content_ids=completed_content_ids,
            runtime=runtime,
        )
        completed_before = max(runtime.resume_guards_by_count)
        new_ordinals = [
            ordinal
            for ordinal in completed_ordinals
            if ordinal > completed_before
        ]
        if new_ordinals:
            if new_ordinals != list(
                range(completed_before + 1, new_ordinals[-1] + 1)
            ):
                raise FullLocalAnalysisError(
                    "batch logical prefix不是连续新增suffix"
                )
            _extend_resume_guard_suffix(
                paths,
                contract,
                [
                    _read_json(
                        _item_paths(paths, ordinal)[1],
                        label="batch logical item suffix",
                    )
                    for ordinal in new_ordinals
                ],
                start_ordinal=completed_before + 1,
                runtime=runtime,
            )
        full_content = _requires_full_checkpoint(
            contract,
            batch_index=batch_index,
            completed_ordinals=completed_ordinals,
        )
        final_state = _batch_checkpoint_state(
            paths,
            contract,
            batch_index=batch_index,
            completed_count=completed_ordinals[-1],
            full_content=full_content,
            runtime=runtime,
        )
        _validate_batch_output_delta(
            paths, contract, intent, completed_ordinals
        )
        audit = _batch_audit_value(
            paths,
            contract,
            batch_index=batch_index,
            completed_ordinals=completed_ordinals,
            after=final_state,
            contract_sha256=runtime.contract_sha256,
        )
        _write_exclusive(
            receipt_path,
            {
                "schema_version": SCHEMA_VERSION,
                "batch_index": batch_index,
                "status": (
                    "budget_exhausted_partial"
                    if unstarted_content_ids
                    else "succeeded"
                ),
                "intent_sha256": local._sha256_file(intent_path),
                "previous_batch_receipt_sha256": previous,
                "content_ids": list(content_ids),
                "completed_content_ids": completed_content_ids,
                "unstarted_content_ids": unstarted_content_ids,
                "item_receipts": item_receipt_rows,
                "item_receipts_sha256": _json_sha(item_receipt_rows),
                "provider_calls": 0,
                "after": final_state,
                "audit": audit,
            },
        )
    return _read_json(receipt_path, label="batch receipt")


def _validate_loaded_run_head(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    *,
    batch_receipts: Sequence[Mapping[str, Any]],
    receipts: Sequence[Mapping[str, Any]],
    completions: Sequence[Mapping[str, Any]],
    runtime: RuntimeContext,
    verify_full_content: bool,
) -> None:
    """Validate the already-loaded immutable heads without rereading history."""

    if "completion" not in runtime.validated_histories:
        _validate_runtime_deferred_history(
            contract,
            batch_receipts=batch_receipts,
            receipts=receipts,
            completions=completions,
            contract_sha256=runtime.contract_sha256,
        )
        _validate_review_pending_history(
            batch_receipts=batch_receipts,
            receipts=receipts,
            completions=completions,
            contract_sha256=runtime.contract_sha256,
        )
        _validate_insufficient_evidence_history(
            batch_receipts=batch_receipts,
            receipts=receipts,
            completions=completions,
            contract_sha256=runtime.contract_sha256,
        )
        runtime.validated_histories.add("completion")
    batch_intents = _batch_intent_files(paths)
    item_intents = _item_intent_files(paths)
    pending_batch = len(batch_intents) == len(batch_receipts) + 1
    pending_item = len(item_intents) == len(receipts) + 1
    if len(completions) != len(batch_receipts):
        raise FullLocalAnalysisError("completion数量未精确对应已完成batch")
    if pending_batch and not pending_item:
        pending_intent = _read_json(
            batch_intents[-1], label="pending loaded batch intent"
        )
        completed_pending_ordinals = [
            int(value)
            for value in pending_intent["item_ordinals"]
            if int(value) <= len(receipts)
        ]
        _validate_batch_output_delta(
            paths, contract, pending_intent, completed_pending_ordinals
        )
    latest = completions[-1] if completions else None
    if not batch_receipts:
        if latest is not None:
            raise FullLocalAnalysisError("无已完成batch却存在completion")
        if not batch_intents and (
            local._canonical_bytes(contract["database_baseline"])
            != local._canonical_bytes(_database_identity(paths.database))
            or local._canonical_bytes(contract["output_baseline"])
            != local._canonical_bytes(_output_inventory(paths))
        ):
            raise FullLocalAnalysisError("pilot前DB/output baseline漂移")
        return
    if latest is None:
        raise FullLocalAnalysisError("已完成batch缺少immutable completion")
    completed_item_count = int(batch_receipts[-1]["item_receipts"][-1][0])
    if len(receipts) < completed_item_count:
        raise FullLocalAnalysisError("completion对应的item receipt前缀不完整")
    expected_progress = local._sha256_file(
        paths.progress_root / f"{completed_item_count:06d}.progress.json"
    )
    status_counts = runtime.status_counts_by_count[completed_item_count]
    succeeded = int(status_counts["succeeded"])
    review_pending = int(status_counts["review_pending"])
    insufficient_evidence = int(
        status_counts["insufficient_evidence"]
    )
    deferred = int(status_counts["deferred"])
    completed_after = batch_receipts[-1]["after"]
    if (
        latest.get("sequence") != len(batch_receipts)
        or latest.get("status")
        != _completion_status_from_counts(
            contract,
            completed_count=completed_item_count,
            deferred_count=deferred,
            review_pending_count=review_pending,
            insufficient_evidence_count=insufficient_evidence,
            completed_batches=len(batch_receipts),
        )
        or latest.get("completed_batches") != len(batch_receipts)
        or latest.get("progress_head_sha256") != expected_progress
        or latest.get("eligible")
        != {
            "total": len(contract["eligible_ids"]),
            "attempted": completed_item_count,
            "succeeded": succeeded,
            "review_pending": review_pending,
            "runtime_deferred": deferred,
            "insufficient_evidence": insufficient_evidence,
        }
        or latest.get("static_deferred") != len(contract["static_deferred"])
        or latest.get("runtime_deferred", {}).get("count") != deferred
        or latest.get("review_pending", {}).get("count") != review_pending
        or latest.get("insufficient_evidence", {}).get("count")
        != insufficient_evidence
        or latest.get("missing_universe") != contract["missing_universe"]
        or local._canonical_bytes(latest.get("database"))
        != local._canonical_bytes(completed_after["database"])
        or local._canonical_bytes(latest.get("outputs"))
        != local._canonical_bytes(completed_after["outputs"])
        or local._canonical_bytes(latest.get("audit"))
        != local._canonical_bytes(batch_receipts[-1].get("audit"))
        or local._canonical_bytes(latest.get("resume_guard"))
        != local._canonical_bytes(
            runtime.resume_guards_by_count[completed_item_count]
        )
    ):
        raise FullLocalAnalysisError(
            "latest completion未精确绑定已加载batch head/DB/output"
        )
    if not pending_batch:
        _validate_checkpoint_closure_current(
            paths,
            database=latest["database"],
            outputs=latest["outputs"],
            verify_full_content=verify_full_content,
            runtime=runtime,
        )


def _runtime_deferred_initial(contract_sha256: str) -> str:
    return _json_sha(
        {
            "contract_sha256": contract_sha256,
            "chain": "runtime_deferred",
        }
    )


def _runtime_deferred_value(
    *,
    sequence: int,
    previous_head_sha256: str,
    previous_count: int,
    delta: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any]:
    batch_delta = list(delta)
    batch_delta_sha256 = _json_sha(batch_delta)
    return {
        "policy": RUNTIME_DEFERRED_POLICY,
        "count": previous_count + len(batch_delta),
        "previous_head_sha256": previous_head_sha256,
        "batch_delta": batch_delta,
        "batch_delta_sha256": batch_delta_sha256,
        "head_sha256": _json_sha(
            {
                "previous_head_sha256": previous_head_sha256,
                "completion_sequence": sequence,
                "batch_delta_sha256": batch_delta_sha256,
            }
        ),
    }


def _validate_runtime_deferred_value(
    value: Any,
    *,
    sequence: int,
    expected_previous_head: str | None = None,
    expected_previous_count: int | None = None,
) -> None:
    if not isinstance(value, Mapping) or set(value) != {
        "policy",
        "count",
        "previous_head_sha256",
        "batch_delta",
        "batch_delta_sha256",
        "head_sha256",
    }:
        raise FullLocalAnalysisError("runtime deferred evidence字段漂移")
    delta = value.get("batch_delta")
    previous = value.get("previous_head_sha256")
    if (
        value.get("policy") != RUNTIME_DEFERRED_POLICY
        or type(value.get("count")) is not int
        or value["count"] < 0
        or type(previous) is not str
        or re.fullmatch(r"[0-9a-f]{64}", previous) is None
        or not isinstance(delta, list)
        or any(
            not isinstance(row, Mapping)
            or set(row) != {"ordinal", "content_id", "failure"}
            or type(row.get("ordinal")) is not int
            or row["ordinal"] <= 0
            or type(row.get("content_id")) is not int
            or not isinstance(row.get("failure"), Mapping)
            or set(row["failure"]) != {"type", "message"}
            or any(
                type(row["failure"].get(key)) is not str
                or not row["failure"][key]
                for key in ("type", "message")
            )
            for row in delta
        )
        or type(value.get("batch_delta_sha256")) is not str
        or value["batch_delta_sha256"] != _json_sha(delta)
        or type(value.get("head_sha256")) is not str
        or value["head_sha256"]
        != _json_sha(
            {
                "previous_head_sha256": previous,
                "completion_sequence": sequence,
                "batch_delta_sha256": value["batch_delta_sha256"],
            }
        )
        or (
            expected_previous_head is not None
            and previous != expected_previous_head
        )
        or (
            expected_previous_count is not None
            and value["count"] != expected_previous_count + len(delta)
        )
    ):
        raise FullLocalAnalysisError(
            "runtime deferred evidence精确类型/hash链漂移"
        )


def _validate_runtime_deferred_history(
    contract: Mapping[str, Any],
    *,
    batch_receipts: Sequence[Mapping[str, Any]],
    receipts: Sequence[Mapping[str, Any]],
    completions: Sequence[Mapping[str, Any]],
    contract_sha256: str,
) -> None:
    previous_head = _runtime_deferred_initial(contract_sha256)
    previous_count = 0
    if len(completions) > len(batch_receipts):
        raise FullLocalAnalysisError("runtime deferred completion超前")
    for sequence, completion in enumerate(completions, 1):
        ordinals = [
            int(row[0])
            for row in batch_receipts[sequence - 1]["item_receipts"]
        ]
        expected_delta = [
            {
                "ordinal": ordinal,
                "content_id": receipts[ordinal - 1]["content_id"],
                "failure": receipts[ordinal - 1]["failure"],
            }
            for ordinal in ordinals
            if receipts[ordinal - 1]["status"] == "deferred"
        ]
        evidence = completion.get("runtime_deferred")
        _validate_runtime_deferred_value(
            evidence,
            sequence=sequence,
            expected_previous_head=previous_head,
            expected_previous_count=previous_count,
        )
        if not isinstance(evidence, Mapping):
            raise FullLocalAnalysisError("runtime deferred evidence形状无效")
        if local._canonical_bytes(evidence["batch_delta"]) != local._canonical_bytes(
            expected_delta
        ):
            raise FullLocalAnalysisError(
                "runtime deferred batch delta未绑定item receipts"
            )
        previous_head = str(evidence["head_sha256"])
        previous_count = int(evidence["count"])


def _review_pending_initial(contract_sha256: str) -> str:
    return _json_sha(
        {
            "contract_sha256": contract_sha256,
            "chain": "review_pending",
        }
    )


def _review_pending_value(
    *,
    sequence: int,
    previous_head_sha256: str,
    previous_count: int,
    delta: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any]:
    batch_delta = list(delta)
    batch_delta_sha256 = _json_sha(batch_delta)
    return {
        "policy": REVIEW_PENDING_POLICY,
        "count": previous_count + len(batch_delta),
        "previous_head_sha256": previous_head_sha256,
        "batch_delta": batch_delta,
        "batch_delta_sha256": batch_delta_sha256,
        "head_sha256": _json_sha(
            {
                "previous_head_sha256": previous_head_sha256,
                "completion_sequence": sequence,
                "batch_delta_sha256": batch_delta_sha256,
            }
        ),
    }


def _validate_review_pending_value(
    value: Any,
    *,
    sequence: int,
    expected_previous_head: str | None = None,
    expected_previous_count: int | None = None,
) -> None:
    if not isinstance(value, Mapping) or set(value) != {
        "policy",
        "count",
        "previous_head_sha256",
        "batch_delta",
        "batch_delta_sha256",
        "head_sha256",
    }:
        raise FullLocalAnalysisError("review_pending evidence字段漂移")
    delta = value.get("batch_delta")
    previous = value.get("previous_head_sha256")
    if (
        value.get("policy") != REVIEW_PENDING_POLICY
        or type(value.get("count")) is not int
        or value["count"] < 0
        or type(previous) is not str
        or re.fullmatch(r"[0-9a-f]{64}", previous) is None
        or not isinstance(delta, list)
        or any(
            not isinstance(row, Mapping)
            or set(row)
            != {
                "ordinal",
                "content_id",
                "evaluation_id",
                "queue_id",
                "review_binding_sha256",
            }
            or type(row.get("ordinal")) is not int
            or row["ordinal"] <= 0
            or type(row.get("content_id")) is not int
            or row["content_id"] <= 0
            or type(row.get("evaluation_id")) is not int
            or row["evaluation_id"] <= 0
            or type(row.get("queue_id")) is not int
            or row["queue_id"] <= 0
            or type(row.get("review_binding_sha256")) is not str
            or re.fullmatch(
                r"[0-9a-f]{64}", row["review_binding_sha256"]
            )
            is None
            for row in delta
        )
        or len({row["ordinal"] for row in delta}) != len(delta)
        or type(value.get("batch_delta_sha256")) is not str
        or value["batch_delta_sha256"] != _json_sha(delta)
        or type(value.get("head_sha256")) is not str
        or value["head_sha256"]
        != _json_sha(
            {
                "previous_head_sha256": previous,
                "completion_sequence": sequence,
                "batch_delta_sha256": value["batch_delta_sha256"],
            }
        )
        or (
            expected_previous_head is not None
            and previous != expected_previous_head
        )
        or (
            expected_previous_count is not None
            and value["count"] != expected_previous_count + len(delta)
        )
    ):
        raise FullLocalAnalysisError(
            "review_pending evidence精确类型/hash链漂移"
        )


def _review_pending_delta_row(
    receipt: Mapping[str, Any], *, ordinal: int
) -> Mapping[str, Any]:
    validated = receipt.get("result", {}).get("validated")
    if not isinstance(validated, Mapping):
        raise FullLocalAnalysisError(
            "review_pending receipt缺少validated投影"
        )
    return {
        "ordinal": ordinal,
        "content_id": receipt["content_id"],
        "evaluation_id": validated.get("evaluation_id"),
        "queue_id": validated.get("queue_id"),
        "review_binding_sha256": _json_sha(validated),
    }


def _review_pending_delta(
    receipts: Sequence[Mapping[str, Any]],
    ordinals: Sequence[int],
) -> list[Mapping[str, Any]]:
    result: list[Mapping[str, Any]] = []
    for ordinal in ordinals:
        receipt = receipts[ordinal - 1]
        if receipt["status"] == "review_pending":
            result.append(
                _review_pending_delta_row(receipt, ordinal=ordinal)
            )
    return result


def _validate_review_pending_history(
    *,
    batch_receipts: Sequence[Mapping[str, Any]],
    receipts: Sequence[Mapping[str, Any]],
    completions: Sequence[Mapping[str, Any]],
    contract_sha256: str,
) -> None:
    previous_head = _review_pending_initial(contract_sha256)
    previous_count = 0
    if len(completions) > len(batch_receipts):
        raise FullLocalAnalysisError("review_pending completion超前")
    for sequence, completion in enumerate(completions, 1):
        ordinals = [
            int(row[0])
            for row in batch_receipts[sequence - 1]["item_receipts"]
        ]
        expected_delta = _review_pending_delta(receipts, ordinals)
        evidence = completion.get("review_pending")
        _validate_review_pending_value(
            evidence,
            sequence=sequence,
            expected_previous_head=previous_head,
            expected_previous_count=previous_count,
        )
        if not isinstance(evidence, Mapping):
            raise FullLocalAnalysisError("review_pending evidence形状无效")
        if local._canonical_bytes(evidence["batch_delta"]) != (
            local._canonical_bytes(expected_delta)
        ):
            raise FullLocalAnalysisError(
                "review_pending batch delta未绑定item receipts"
            )
        previous_head = evidence["head_sha256"]
        previous_count = evidence["count"]


def _insufficient_evidence_initial(contract_sha256: str) -> str:
    return _json_sha(
        {
            "contract_sha256": contract_sha256,
            "chain": "insufficient_evidence",
        }
    )


def _insufficient_evidence_value(
    *,
    sequence: int,
    previous_head_sha256: str,
    previous_count: int,
    delta: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any]:
    batch_delta = list(delta)
    batch_delta_sha256 = _json_sha(batch_delta)
    return {
        "policy": INSUFFICIENT_EVIDENCE_POLICY,
        "count": previous_count + len(batch_delta),
        "previous_head_sha256": previous_head_sha256,
        "batch_delta": batch_delta,
        "batch_delta_sha256": batch_delta_sha256,
        "head_sha256": _json_sha(
            {
                "previous_head_sha256": previous_head_sha256,
                "completion_sequence": sequence,
                "batch_delta_sha256": batch_delta_sha256,
            }
        ),
    }


def _validate_insufficient_evidence_value(
    value: Any,
    *,
    sequence: int,
    expected_previous_head: str | None = None,
    expected_previous_count: int | None = None,
) -> None:
    keys = {
        "policy",
        "count",
        "previous_head_sha256",
        "batch_delta",
        "batch_delta_sha256",
        "head_sha256",
    }
    if not isinstance(value, Mapping) or set(value) != keys:
        raise FullLocalAnalysisError(
            "insufficient_evidence evidence字段漂移"
        )
    delta = value.get("batch_delta")
    previous = value.get("previous_head_sha256")
    row_keys = {
        "ordinal",
        "content_id",
        "evaluation_id",
        "evidence_level",
        "evidence_sha256",
        "insufficient_binding_sha256",
    }
    if (
        value.get("policy") != INSUFFICIENT_EVIDENCE_POLICY
        or type(value.get("count")) is not int
        or value["count"] < 0
        or type(previous) is not str
        or re.fullmatch(r"[0-9a-f]{64}", previous) is None
        or not isinstance(delta, list)
        or any(
            not isinstance(row, Mapping)
            or set(row) != row_keys
            or type(row.get("ordinal")) is not int
            or row["ordinal"] <= 0
            or type(row.get("content_id")) is not int
            or row["content_id"] <= 0
            or type(row.get("evaluation_id")) is not int
            or row["evaluation_id"] <= 0
            or row.get("evidence_level") not in {"V0", "V1"}
            or type(row.get("evidence_sha256")) is not str
            or re.fullmatch(r"[0-9a-f]{64}", row["evidence_sha256"])
            is None
            or type(row.get("insufficient_binding_sha256")) is not str
            or re.fullmatch(
                r"[0-9a-f]{64}", row["insufficient_binding_sha256"]
            )
            is None
            for row in delta
        )
        or len({row["ordinal"] for row in delta}) != len(delta)
        or value.get("batch_delta_sha256") != _json_sha(delta)
        or value.get("head_sha256")
        != _json_sha(
            {
                "previous_head_sha256": previous,
                "completion_sequence": sequence,
                "batch_delta_sha256": value.get("batch_delta_sha256"),
            }
        )
        or (
            expected_previous_head is not None
            and previous != expected_previous_head
        )
        or (
            expected_previous_count is not None
            and value["count"] != expected_previous_count + len(delta)
        )
    ):
        raise FullLocalAnalysisError(
            "insufficient_evidence evidence精确类型/hash链漂移"
        )


def _insufficient_evidence_delta_row(
    receipt: Mapping[str, Any], *, ordinal: int
) -> Mapping[str, Any]:
    result = receipt.get("result")
    if not isinstance(result, Mapping):
        raise FullLocalAnalysisError(
            "insufficient_evidence receipt缺少result投影"
        )
    evaluation = result.get("evaluation")
    validated = result.get("validated")
    if not isinstance(evaluation, Mapping) or not isinstance(
        validated, Mapping
    ):
        raise FullLocalAnalysisError(
            "insufficient_evidence receipt缺少evaluation/validated投影"
        )
    return {
        "ordinal": ordinal,
        "content_id": receipt["content_id"],
        "evaluation_id": validated.get("evaluation_id"),
        "evidence_level": evaluation.get("evidence_level"),
        "evidence_sha256": evaluation.get("evidence_sha256"),
        "insufficient_binding_sha256": _json_sha(
            {"evaluation": evaluation, "validated": validated}
        ),
    }


def _insufficient_evidence_delta(
    receipts: Sequence[Mapping[str, Any]],
    ordinals: Sequence[int],
) -> list[Mapping[str, Any]]:
    return [
        _insufficient_evidence_delta_row(
            receipts[ordinal - 1], ordinal=ordinal
        )
        for ordinal in ordinals
        if receipts[ordinal - 1]["status"] == "insufficient_evidence"
    ]


def _validate_insufficient_evidence_history(
    *,
    batch_receipts: Sequence[Mapping[str, Any]],
    receipts: Sequence[Mapping[str, Any]],
    completions: Sequence[Mapping[str, Any]],
    contract_sha256: str,
) -> None:
    previous_head = _insufficient_evidence_initial(contract_sha256)
    previous_count = 0
    if len(completions) > len(batch_receipts):
        raise FullLocalAnalysisError(
            "insufficient_evidence completion超前"
        )
    for sequence, completion in enumerate(completions, 1):
        ordinals = [
            int(row[0])
            for row in batch_receipts[sequence - 1]["item_receipts"]
        ]
        expected_delta = _insufficient_evidence_delta(receipts, ordinals)
        evidence = completion.get("insufficient_evidence")
        _validate_insufficient_evidence_value(
            evidence,
            sequence=sequence,
            expected_previous_head=previous_head,
            expected_previous_count=previous_count,
        )
        if not isinstance(evidence, Mapping):
            raise FullLocalAnalysisError(
                "insufficient_evidence evidence形状无效"
            )
        if local._canonical_bytes(evidence["batch_delta"]) != (
            local._canonical_bytes(expected_delta)
        ):
            raise FullLocalAnalysisError(
                "insufficient_evidence batch delta未绑定item receipts"
            )
        previous_head = evidence["head_sha256"]
        previous_count = evidence["count"]


def _completion_value(
    contract: Mapping[str, Any],
    receipts: Sequence[Mapping[str, Any]],
    runtime: RuntimeContext,
    *,
    batch_receipt: Mapping[str, Any],
    completion_sequence: int,
    previous_runtime_deferred: Mapping[str, Any] | None,
    previous_review_pending: Mapping[str, Any] | None,
    previous_insufficient_evidence: Mapping[str, Any] | None,
    progress_head_sha256: str | None,
    previous_completion_sha256: str | None,
) -> Mapping[str, Any]:
    completed_count = int(batch_receipt["item_receipts"][-1][0])
    if completed_count > len(receipts):
        raise FullLocalAnalysisError("completion item head超前")
    if completed_count not in runtime.resume_guards_by_count:
        raise FullLocalAnalysisError("completion缺少对应resume guard")
    previous_deferred_head = (
        str(previous_runtime_deferred["head_sha256"])
        if previous_runtime_deferred is not None
        else _runtime_deferred_initial(runtime.contract_sha256)
    )
    previous_deferred_count = (
        int(previous_runtime_deferred["count"])
        if previous_runtime_deferred is not None
        else 0
    )
    current_ordinals = [
        int(row[0]) for row in batch_receipt["item_receipts"]
    ]
    deferred_delta: list[Mapping[str, Any]] = []
    review_delta: list[Mapping[str, Any]] = []
    insufficient_delta: list[Mapping[str, Any]] = []
    for ordinal in current_ordinals:
        receipt = receipts[ordinal - 1]
        if receipt["status"] == "deferred":
            deferred_delta.append(
                {
                    "ordinal": ordinal,
                    "content_id": receipt["content_id"],
                    "failure": receipt["failure"],
                }
            )
        elif receipt["status"] == "review_pending":
            review_delta.append(
                _review_pending_delta_row(receipt, ordinal=ordinal)
            )
        elif receipt["status"] == "insufficient_evidence":
            insufficient_delta.append(
                _insufficient_evidence_delta_row(
                    receipt, ordinal=ordinal
                )
            )
    runtime_deferred = _runtime_deferred_value(
        sequence=completion_sequence,
        previous_head_sha256=previous_deferred_head,
        previous_count=previous_deferred_count,
        delta=deferred_delta,
    )
    runtime_deferred_count = int(runtime_deferred["count"])
    previous_review_head = (
        previous_review_pending["head_sha256"]
        if previous_review_pending is not None
        else _review_pending_initial(runtime.contract_sha256)
    )
    previous_review_count = (
        previous_review_pending["count"]
        if previous_review_pending is not None
        else 0
    )
    review_pending = _review_pending_value(
        sequence=completion_sequence,
        previous_head_sha256=previous_review_head,
        previous_count=previous_review_count,
        delta=review_delta,
    )
    review_pending_count = review_pending["count"]
    previous_insufficient_head = (
        previous_insufficient_evidence["head_sha256"]
        if previous_insufficient_evidence is not None
        else _insufficient_evidence_initial(runtime.contract_sha256)
    )
    previous_insufficient_count = (
        previous_insufficient_evidence["count"]
        if previous_insufficient_evidence is not None
        else 0
    )
    insufficient_evidence = _insufficient_evidence_value(
        sequence=completion_sequence,
        previous_head_sha256=previous_insufficient_head,
        previous_count=previous_insufficient_count,
        delta=insufficient_delta,
    )
    insufficient_evidence_count = insufficient_evidence["count"]
    status_counts = runtime.status_counts_by_count.get(completed_count)
    if status_counts is None:
        raise FullLocalAnalysisError("completion缺少prefix status counts")
    succeeded = int(status_counts["succeeded"])
    if int(status_counts["deferred"]) != runtime_deferred_count:
        raise FullLocalAnalysisError("completion deferred prefix count漂移")
    if int(status_counts["review_pending"]) != review_pending_count:
        raise FullLocalAnalysisError("completion review_pending prefix count漂移")
    if (
        int(status_counts["insufficient_evidence"])
        != insufficient_evidence_count
    ):
        raise FullLocalAnalysisError(
            "completion insufficient_evidence prefix count漂移"
        )
    eligible_complete = (
        completed_count == len(contract["eligible_ids"])
        and runtime_deferred_count == 0
        and review_pending_count == 0
        and insufficient_evidence_count == 0
    )
    pilot_complete = (
        completion_sequence == 1
        and completed_count == len(contract["profile"]["first_batch_ids"])
        and runtime_deferred_count == 0
        and review_pending_count == 0
        and insufficient_evidence_count == 0
    )
    status = (
        "eligible_complete"
        if eligible_complete
        else "pilot_complete"
        if pilot_complete
        else "partial"
    )
    value = {
        "schema_version": SCHEMA_VERSION,
        "sequence": completion_sequence,
        "status": status,
        "previous_completion_sha256": previous_completion_sha256,
        "contract_sha256": runtime.contract_sha256,
        "progress_head_sha256": progress_head_sha256,
        "eligible": {
            "total": len(contract["eligible_ids"]),
            "attempted": completed_count,
            "succeeded": succeeded,
            "review_pending": review_pending_count,
            "runtime_deferred": runtime_deferred_count,
            "insufficient_evidence": insufficient_evidence_count,
        },
        "static_deferred": len(contract["static_deferred"]),
        "runtime_deferred": runtime_deferred,
        "review_pending": review_pending,
        "insufficient_evidence": insufficient_evidence,
        "missing_universe": contract["missing_universe"],
        "provider_calls": 0,
        "completed_batches": completion_sequence,
        "full_history_complete": False,
        "publication_allowed": False,
        "database": batch_receipt["after"]["database"],
        "outputs": batch_receipt["after"]["outputs"],
        "audit": batch_receipt["audit"],
        "resume_guard": runtime.resume_guards_by_count[completed_count],
    }
    return value


def _write_completion(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    receipts: Sequence[Mapping[str, Any]],
    runtime: RuntimeContext,
    *,
    batch_receipt: Mapping[str, Any],
    completions: Sequence[Mapping[str, Any]],
    progress_head_sha256: str | None,
) -> Mapping[str, Any]:
    if len(receipts) not in runtime.resume_guards_by_count:
        _extend_resume_guards(paths, contract, receipts, runtime)
    completion_count = len(completions)
    previous = (
        local._sha256_file(
            paths.completions_root
            / f"{completion_count:06d}.completion.json"
        )
        if completion_count
        else None
    )
    value = _completion_value(
        contract,
        receipts,
        runtime,
        batch_receipt=batch_receipt,
        completion_sequence=completion_count + 1,
        previous_runtime_deferred=(
            completions[-1]["runtime_deferred"] if completions else None
        ),
        previous_review_pending=(
            completions[-1]["review_pending"] if completions else None
        ),
        previous_insufficient_evidence=(
            completions[-1]["insufficient_evidence"]
            if completions
            else None
        ),
        progress_head_sha256=progress_head_sha256,
        previous_completion_sha256=previous,
    )
    target = (
        paths.completions_root
        / f"{completion_count + 1:06d}.completion.json"
    )
    _write_exclusive(target, value)
    return value


def _completion_status(
    contract: Mapping[str, Any], receipts: Sequence[Mapping[str, Any]], completed_batches: int
) -> str:
    deferred_count = sum(row["status"] == "deferred" for row in receipts)
    review_pending_count = sum(
        row["status"] == "review_pending" for row in receipts
    )
    insufficient_evidence_count = sum(
        row["status"] == "insufficient_evidence" for row in receipts
    )
    return _completion_status_from_counts(
        contract,
        completed_count=len(receipts),
        deferred_count=deferred_count,
        review_pending_count=review_pending_count,
        insufficient_evidence_count=insufficient_evidence_count,
        completed_batches=completed_batches,
    )


def _completion_status_from_counts(
    contract: Mapping[str, Any],
    *,
    completed_count: int,
    deferred_count: int,
    review_pending_count: int,
    insufficient_evidence_count: int,
    completed_batches: int,
) -> str:
    if (
        completed_count == len(contract["eligible_ids"])
        and deferred_count == 0
        and review_pending_count == 0
        and insufficient_evidence_count == 0
    ):
        return "eligible_complete"
    if (
        completed_batches == 1
        and completed_count == len(contract["profile"]["first_batch_ids"])
        and deferred_count == 0
        and review_pending_count == 0
        and insufficient_evidence_count == 0
    ):
        return "pilot_complete"
    return "partial"


def _completion_files(paths: BatchPaths) -> list[Path]:
    return _numbered_files(paths.completions_root, ".completion.json")


def _validate_completion_value(
    value: Mapping[str, Any],
    *,
    sequence: int,
    previous: str | None,
    contract_sha256: str,
) -> None:
    expected_keys = {
        "schema_version",
        "sequence",
        "status",
        "previous_completion_sha256",
        "contract_sha256",
        "progress_head_sha256",
        "eligible",
        "static_deferred",
        "runtime_deferred",
        "review_pending",
        "insufficient_evidence",
        "missing_universe",
        "provider_calls",
        "completed_batches",
        "full_history_complete",
        "publication_allowed",
        "database",
        "outputs",
        "audit",
        "resume_guard",
    }
    eligible = value.get("eligible")
    runtime_deferred = value.get("runtime_deferred")
    review_pending = value.get("review_pending")
    insufficient_evidence = value.get("insufficient_evidence")
    database = value.get("database")
    outputs = value.get("outputs")
    audit = value.get("audit")
    resume_guard = value.get("resume_guard")
    _validate_batch_audit_value(audit, batch_index=sequence)
    _validate_resume_guard_value(resume_guard)
    _validate_runtime_deferred_value(runtime_deferred, sequence=sequence)
    _validate_review_pending_value(review_pending, sequence=sequence)
    _validate_insufficient_evidence_value(
        insufficient_evidence, sequence=sequence
    )
    _validate_checkpoint_closure_shape(database=database, outputs=outputs)
    if not isinstance(eligible, Mapping):
        raise FullLocalAnalysisError("immutable completion eligible形状无效")
    if not isinstance(runtime_deferred, Mapping):
        raise FullLocalAnalysisError(
            "immutable completion runtime_deferred形状无效"
        )
    if not isinstance(review_pending, Mapping):
        raise FullLocalAnalysisError(
            "immutable completion review_pending形状无效"
        )
    if not isinstance(insufficient_evidence, Mapping):
        raise FullLocalAnalysisError(
            "immutable completion insufficient_evidence形状无效"
        )
    eligible_types_valid = (
        set(eligible)
        == {
            "total",
            "attempted",
            "succeeded",
            "review_pending",
            "runtime_deferred",
            "insufficient_evidence",
        }
        and all(type(eligible.get(key)) is int for key in eligible)
        and all(eligible[key] >= 0 for key in eligible)
        and eligible["attempted"]
        == eligible["succeeded"]
        + eligible["review_pending"]
        + eligible["runtime_deferred"]
        + eligible["insufficient_evidence"]
        and eligible["attempted"] <= eligible["total"]
        and eligible["review_pending"] == review_pending["count"]
        and eligible["runtime_deferred"] == runtime_deferred["count"]
        and eligible["insufficient_evidence"]
        == insufficient_evidence["count"]
    )
    audit_types_valid = (
        isinstance(audit, Mapping)
        and set(audit)
        == {
            "policy",
            "previous_logical_head_sha256",
            "batch_delta",
            "batch_delta_sha256",
            "logical_head_sha256",
            "coverage",
            "latest_logical_checkpoint_batch",
            "full_checkpoint",
        }
        and audit.get("policy") == _audit_policy_value()
        and isinstance(audit.get("previous_logical_head_sha256"), str)
        and isinstance(audit.get("batch_delta"), list)
        and isinstance(audit.get("batch_delta_sha256"), str)
        and isinstance(audit.get("logical_head_sha256"), str)
        and audit.get("coverage")
        in {"logical_database_checkpoint", "owned_delta"}
        and type(audit.get("latest_logical_checkpoint_batch")) is int
        and (
            audit.get("full_checkpoint") is None
            or isinstance(audit.get("full_checkpoint"), Mapping)
        )
    )
    if (
        set(value) != expected_keys
        or value.get("schema_version") != SCHEMA_VERSION
        or type(value.get("sequence")) is not int
        or value.get("sequence") != sequence
        or value.get("previous_completion_sha256") != previous
        or value.get("contract_sha256") != contract_sha256
        or value.get("status")
        not in {"partial", "pilot_complete", "eligible_complete"}
        or type(value.get("provider_calls")) is not int
        or value.get("provider_calls") != 0
        or value.get("full_history_complete") is not False
        or value.get("publication_allowed") is not False
        or type(value.get("completed_batches")) is not int
        or value.get("completed_batches") != sequence
        or type(value.get("static_deferred")) is not int
        or value["static_deferred"] < 0
        or not eligible_types_valid
        or (
            value.get("status") in {"pilot_complete", "eligible_complete"}
            and (
                eligible["review_pending"] != 0
                or eligible["runtime_deferred"] != 0
                or eligible["insufficient_evidence"] != 0
            )
        )
        or not audit_types_valid
    ):
        raise FullLocalAnalysisError("immutable completion字段/类型漂移")


def _validate_completion_chain(
    paths: BatchPaths, runtime: RuntimeContext | None = None
) -> list[Mapping[str, Any]]:
    files = _completion_files(paths)
    previous: str | None = None
    values: list[Mapping[str, Any]] = []
    contract_sha = (
        runtime.contract_sha256
        if runtime is not None
        else local._sha256_file(paths.contract)
    )
    for sequence, path in enumerate(files, 1):
        value = _read_json(path, label="immutable completion")
        _validate_completion_value(
            value,
            sequence=sequence,
            previous=previous,
            contract_sha256=contract_sha,
        )
        previous = local._sha256_file(path)
        values.append(value)
    return values


def _validate_completion_history_exact(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    *,
    batch_receipts: Sequence[Mapping[str, Any]],
    receipts: Sequence[Mapping[str, Any]],
    completions: Sequence[Mapping[str, Any]],
    runtime: RuntimeContext,
) -> None:
    """Re-derive every published completion from immutable lower heads."""

    if len(completions) > len(batch_receipts):
        raise FullLocalAnalysisError("completion历史超前于batch receipts")
    expected_values: list[Mapping[str, Any]] = []
    for sequence, recorded in enumerate(completions, 1):
        batch_receipt = batch_receipts[sequence - 1]
        completed_count = int(batch_receipt["item_receipts"][-1][0])
        if completed_count > len(receipts):
            raise FullLocalAnalysisError("completion历史item head超前")
        progress_path = (
            paths.progress_root / f"{completed_count:06d}.progress.json"
        )
        if not progress_path.is_file():
            raise FullLocalAnalysisError("completion历史缺少progress head")
        previous = (
            local._sha256_file(
                paths.completions_root
                / f"{sequence - 1:06d}.completion.json"
            )
            if sequence > 1
            else None
        )
        expected = _completion_value(
            contract,
            receipts,
            runtime,
            batch_receipt=batch_receipt,
            completion_sequence=sequence,
            previous_runtime_deferred=(
                expected_values[-1]["runtime_deferred"]
                if expected_values
                else None
            ),
            previous_review_pending=(
                expected_values[-1]["review_pending"]
                if expected_values
                else None
            ),
            previous_insufficient_evidence=(
                expected_values[-1]["insufficient_evidence"]
                if expected_values
                else None
            ),
            progress_head_sha256=local._sha256_file(progress_path),
            previous_completion_sha256=previous,
        )
        if local._canonical_bytes(recorded) != local._canonical_bytes(expected):
            raise FullLocalAnalysisError(
                f"completion {sequence} 未按batch/item prefix精确重派生"
            )
        expected_values.append(expected)
    runtime.validated_histories.add("completion")


def _latest_completion(
    paths: BatchPaths, runtime: RuntimeContext | None = None
) -> Mapping[str, Any] | None:
    values = _validate_completion_chain(paths, runtime)
    return values[-1] if values else None


def _append_missing_completions(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    batch_receipts: Sequence[Mapping[str, Any]],
    item_receipts: Sequence[Mapping[str, Any]],
    runtime: RuntimeContext,
    *,
    completions: Sequence[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    """Append the exact missing completion suffix in batch order."""

    values = list(completions)
    appended: list[Mapping[str, Any]] = []
    for batch_count in range(len(values) + 1, len(batch_receipts) + 1):
        batch_receipt = batch_receipts[batch_count - 1]
        completed_count = int(batch_receipt["item_receipts"][-1][0])
        if completed_count > len(item_receipts):
            raise FullLocalAnalysisError("completion suffix item head超前")
        progress_path = (
            paths.progress_root / f"{completed_count:06d}.progress.json"
        )
        if not progress_path.is_file():
            raise FullLocalAnalysisError("completion suffix缺少progress head")
        value = _write_completion(
            paths,
            contract,
            item_receipts,
            runtime,
            batch_receipt=batch_receipt,
            completions=values,
            progress_head_sha256=local._sha256_file(progress_path),
        )
        values.append(value)
        appended.append(value)
    return appended


def _recover_missing_completions(
    paths: BatchPaths,
    contract: Mapping[str, Any],
    batch_receipts: Sequence[Mapping[str, Any]],
    item_receipts: Sequence[Mapping[str, Any]],
    runtime: RuntimeContext,
    *,
    completions: Sequence[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    """Close an exact provisional-batch -> completion suffix after global scan."""

    completion_count = len(completions)
    batch_count = len(batch_receipts)
    if completion_count == batch_count:
        return []
    no_pending_batch = len(_batch_intent_files(paths)) == batch_count
    no_pending_item = len(_item_intent_files(paths)) == len(item_receipts)
    if (
        completion_count > batch_count
        or not no_pending_batch
        or not no_pending_item
        or not batch_receipts
    ):
        raise FullLocalAnalysisError(
            "completion缺口不是可恢复的provisional batch receipt suffix"
        )
    if int(batch_receipts[-1]["item_receipts"][-1][0]) != len(item_receipts):
        raise FullLocalAnalysisError(
            "provisional batch receipt后completion恢复前DB/output闭包漂移"
        )
    _validate_checkpoint_closure_current(
        paths,
        database=batch_receipts[-1]["after"]["database"],
        outputs=batch_receipts[-1]["after"]["outputs"],
        verify_full_content=True,
        runtime=runtime,
    )
    return _append_missing_completions(
        paths,
        contract,
        batch_receipts,
        item_receipts,
        runtime,
        completions=completions,
    )


def _initial_plan(
    paths: BatchPaths,
    *,
    expected_source_db_sha256: str,
    expected_source_completion_sha256: str,
    profile: HistoryProfile,
    allow_copy_recovery: bool = False,
) -> Mapping[str, Any]:
    local._validate_paths(paths.local_paths, work_database_must_exist=False)
    if (
        (paths.database.exists() or local._database_sidecars(paths.database))
        and not allow_copy_recovery
    ):
        raise FullLocalAnalysisError("首次plan要求全新work database")
    for root in (paths.media_root, paths.fingerprint_root, paths.run_root):
        if os.path.lexists(root):
            local._private_directory(root, label="plan输出根")
            if any(root.iterdir()) and not allow_copy_recovery:
                raise FullLocalAnalysisError("首次plan要求输出根为空")
    step3 = _source_contract(paths)
    target_ids = step3.get("target_ids")
    if not isinstance(target_ids, list) or len(target_ids) != profile.universe_count:
        raise FullLocalAnalysisError("Step3 universe数量漂移")
    source_evidence = local._source_completion_evidence(
        paths.local_paths,
        content_ids=target_ids,
        expected_source_db_sha256=expected_source_db_sha256,
        expected_source_completion_sha256=expected_source_completion_sha256,
    )
    target_row_map = _target_row_map(source_evidence)
    missing = _missing_universe_evidence(step3, profile)
    with closing(local._immutable_connection(paths.source_database)) as connection:
        eligible, deferred, summaries = _classify_universe(
            connection,
            target_ids,
            source_evidence=source_evidence,
            row_map=target_row_map,
        )
        source_groups = _source_groups_snapshot(connection, target_ids)
    if (
        len(eligible) != profile.eligible_count
        or len(deferred) != profile.static_deferred_count
    ):
        raise FullLocalAnalysisError(
            "static eligible/deferred数量未命中冻结profile："
            f"{len(eligible)}/{len(deferred)}"
        )
    if profile == PRODUCTION_PROFILE:
        reasons = Counter(str(row["reason"]) for row in deferred)
        expected_reasons = Counter(
            {
                STATIC_DEFER_REASON: 31_266,
                "non_https_media_url": 3_334,
                "audio_placeholder": 2,
            }
        )
        if reasons != expected_reasons:
            raise FullLocalAnalysisError(
                f"static deferred原因分解漂移：{dict(reasons)}"
            )
    order = _processing_order(eligible, profile, summaries)
    require_whisper = any(
        summaries[content_id]["media_kind"] == "video" for content_id in eligible
    )
    tools = local._local_tools(require_whisper=require_whisper)
    return {
        "source_evidence": source_evidence,
        "target_ids": target_ids,
        "eligible_ids": eligible,
        "static_deferred": deferred,
        "source_summaries": summaries,
        "processing_order": order,
        "missing_universe": missing,
        "source_groups": source_groups,
        "tools": tools,
        "target_row_map": target_row_map,
    }


def plan_batches(
    *,
    source_db_path: Path,
    source_completion_path: Path,
    expected_source_db_sha256: str,
    expected_source_completion_sha256: str,
    db_path: Path,
    media_root: Path,
    run_root: Path,
    through_batch: int,
    workers: int = 1,
    max_new_batches: int = 1,
    profile: HistoryProfile = PRODUCTION_PROFILE,
) -> Mapping[str, Any]:
    """Return a zero-write plan for a new or existing accumulation run."""

    _validate_batch_arguments(
        through_batch=through_batch,
        workers=workers,
        max_new_batches=max_new_batches,
    )
    paths = _paths(
        source_db_path=source_db_path,
        source_completion_path=source_completion_path,
        db_path=db_path,
        media_root=media_root,
        run_root=run_root,
    )
    if paths.contract.exists():
        local._validate_paths(paths.local_paths, work_database_must_exist=True)
        _preflight_existing_physical(paths, allow_atomic_temps=False)
        contract = _read_json(paths.contract, label="global contract")
        _validate_contract(
            paths,
            contract,
            expected_source_db_sha256=expected_source_db_sha256,
            expected_source_completion_sha256=expected_source_completion_sha256,
            profile=profile,
        )
        runtime = _runtime_context(paths, contract)
        batch_receipts = _validate_batch_chain(paths, runtime)
        receipts = _validate_item_chain(paths, contract, runtime)
        completions = _validate_completion_chain(paths, runtime)
        _validate_resume_guard_history(
            paths,
            contract,
            receipts,
            batch_receipts,
            runtime,
            completions=completions,
        )
        _validate_historical_logical_checkpoints(
            contract,
            batch_receipts,
            runtime,
        )
        with closing(local._immutable_connection(paths.database)) as connection:
            _validate_current_sequence_prefix(
                paths,
                receipts,
                runtime,
                _sequence_snapshot(connection),
            )
        _validate_progress_chain(paths, receipts)
        _validate_completion_history_exact(
            paths,
            contract,
            batch_receipts=batch_receipts,
            receipts=receipts,
            completions=completions,
            runtime=runtime,
        )
        _validate_network_ledgers_read_only(paths, contract, runtime)
        _validate_loaded_run_head(
            paths,
            contract,
            batch_receipts=batch_receipts,
            receipts=receipts,
            completions=completions,
            runtime=runtime,
            verify_full_content=False,
        )
        _assert_global_invariants(paths, contract)
        completed_batches = len(_batch_receipt_files(paths))
        if (
            through_batch < completed_batches
            or through_batch - completed_batches > max_new_batches
        ):
            raise FullLocalAnalysisError(
                "through绝对停点超过max_new_batches授权；需显式+1逐步推进"
            )
        proposal = _absolute_batch_plan(
            paths,
            contract,
            completed_batches=completed_batches,
            completed_items=len(receipts),
            through_batch=(
                completed_batches
                if through_batch == completed_batches
                else completed_batches + 1
            ),
            runtime=runtime,
        )
        return {
            "ok": True,
            "status": "planned",
            "apply": False,
            "existing_run": True,
            "completed_items": len(receipts),
            "through_batch": through_batch,
            "max_new_batches": max_new_batches,
            "batch_action": proposal,
            "provider_calls_planned": 0,
            "full_history_complete": False,
        }
    initial = _initial_plan(
        paths,
        expected_source_db_sha256=expected_source_db_sha256,
        expected_source_completion_sha256=expected_source_completion_sha256,
        profile=profile,
    )
    virtual = {
        "processing_order": initial["processing_order"],
        "profile": _profile_value(profile),
        "source_summaries": {
            str(key): value for key, value in initial["source_summaries"].items()
        },
    }
    if through_batch != 1 or max_new_batches != 1:
        raise FullLocalAnalysisError("首次绝对停点必须是batch 1")
    return {
        "ok": True,
        "status": "planned",
        "apply": False,
        "existing_run": False,
        "universe_count": len(initial["target_ids"]),
        "eligible_count": len(initial["eligible_ids"]),
        "static_deferred_count": len(initial["static_deferred"]),
        "missing_universe": initial["missing_universe"],
        "processing_order_sha256": _json_sha(initial["processing_order"]),
        "through_batch": through_batch,
        "max_new_batches": max_new_batches,
        "batch_action": {
            "mode": "new_batch",
            "batch_index": 1,
            "content_ids": _batch_at_cursor(virtual, 0),
            "new_batch": True,
        },
        "provider_calls_planned": 0,
        "full_history_complete": False,
    }


def run_batches(
    *,
    source_db_path: Path,
    source_completion_path: Path,
    expected_source_db_sha256: str,
    expected_source_completion_sha256: str,
    db_path: Path,
    media_root: Path,
    run_root: Path,
    through_batch: int,
    workers: int = 1,
    max_new_batches: int = 1,
    profile: HistoryProfile = PRODUCTION_PROFILE,
) -> Mapping[str, Any]:
    _validate_batch_arguments(
        through_batch=through_batch,
        workers=workers,
        max_new_batches=max_new_batches,
    )
    paths = _paths(
        source_db_path=source_db_path,
        source_completion_path=source_completion_path,
        db_path=db_path,
        media_root=media_root,
        run_root=run_root,
    )
    existing = paths.contract.exists()
    if not existing and (through_batch != 1 or max_new_batches != 1):
        raise FullLocalAnalysisError(
            "首次pilot必须through_batch=1且max_new_batches=1"
        )
    initial = None
    if not existing:
        # This is intentionally evaluated before any claim/root creation so the
        # first phase remains the exact same zero-write plan the user reviewed.
        initial = _initial_plan(
            paths,
            expected_source_db_sha256=expected_source_db_sha256,
            expected_source_completion_sha256=expected_source_completion_sha256,
            profile=profile,
            allow_copy_recovery=(
                paths.local_paths.copy_intent.exists()
                or paths.database.exists()
            ),
        )
    local._validate_paths(paths.local_paths, work_database_must_exist=existing)
    if existing:
        _require_existing_roots(paths)
    with local._all_claims(paths.local_paths), _claim(
        paths, create_roots=not existing
    ):
        if existing:
            _preflight_existing_physical(paths, allow_atomic_temps=True)
        if not existing:
            if initial is None:
                raise FullLocalAnalysisError("首次apply缺少已验证的零写plan")
            _validate_precontract_prefix(paths)
            local._ensure_work_copy(
                paths.local_paths,
                content_ids=initial["target_ids"],
                source_evidence=initial["source_evidence"],
                expected_source_db_sha256=expected_source_db_sha256,
                expected_source_completion_sha256=expected_source_completion_sha256,
            )
            contract = _build_contract(
                paths,
                source_evidence=initial["source_evidence"],
                target_ids=initial["target_ids"],
                eligible_ids=initial["eligible_ids"],
                static_deferred=initial["static_deferred"],
                source_summaries=initial["source_summaries"],
                missing=initial["missing_universe"],
                profile=profile,
                tools=initial["tools"],
                target_row_map=initial["target_row_map"],
            )
            _write_exclusive(paths.contract, contract)
        else:
            contract = _read_json(paths.contract, label="global contract")
        _validate_contract(
            paths,
            contract,
            expected_source_db_sha256=expected_source_db_sha256,
            expected_source_completion_sha256=expected_source_completion_sha256,
            profile=profile,
        )
        runtime = _runtime_context(
            paths,
            contract,
            target_row_map=(
                initial["target_row_map"] if initial is not None else None
            ),
        )
        # Validate every already published completion against the current
        # completed DB rows and recursive output content before any recovery
        # transition can write a record or resume processing.
        record_temps_present = any(
            path.name.startswith(".") and path.name.endswith(".tmp")
            for root in (
                paths.items_root,
                paths.batches_root,
                paths.progress_root,
                paths.completions_root,
            )
            for path in root.iterdir()
        )
        item_temp_overrides = _complete_record_temp_overrides(
            paths.items_root, ".receipt.json"
        )
        batch_temp_overrides = _complete_record_temp_overrides(
            paths.batches_root, ".receipt.json"
        )
        batch_receipts = _validate_batch_chain(
            paths,
            runtime,
            batch_receipt_overrides=batch_temp_overrides,
            item_receipt_overrides=item_temp_overrides,
        )
        item_receipts = _validate_item_chain(
            paths,
            contract,
            runtime,
            receipt_overrides=item_temp_overrides,
        )
        completions = _validate_completion_chain(paths, runtime)
        invocation_start_state = _invocation_global_readonly_gate(
            paths,
            contract,
            batch_receipts=batch_receipts,
            item_receipts=item_receipts,
            completions=completions,
            runtime=runtime,
            item_receipt_overrides=item_temp_overrides,
        )
        _validate_completion_history_exact(
            paths,
            contract,
            batch_receipts=batch_receipts,
            receipts=item_receipts,
            completions=completions,
            runtime=runtime,
        )
        _recover_record_temps(paths, contract)
        _recover_downstream_bound_complete_item_temps(paths, contract)
        _recover_validate_network_ledgers(paths, contract, runtime)
        if record_temps_present:
            batch_receipts = _validate_batch_chain(paths, runtime)
            item_receipts = _validate_item_chain(paths, contract, runtime)
            completions = _validate_completion_chain(paths, runtime)
            _validate_completion_history_exact(
                paths,
                contract,
                batch_receipts=batch_receipts,
                receipts=item_receipts,
                completions=completions,
                runtime=runtime,
            )
        _catch_up_progress(paths, item_receipts)
        pending_item = len(_item_intent_files(paths)) == len(item_receipts) + 1
        sidecars = local._database_sidecars(paths.database)
        if sidecars:
            if not pending_item:
                raise FullLocalAnalysisError(
                    "无pending item intent的work DB sidecar拒绝恢复"
                )
            for sidecar in sidecars:
                local._private_file(sidecar, label="pending item DB sidecar")
            pending_intent = _read_json(
                _item_intent_files(paths)[-1],
                label="pending item sequence waterline",
            )
            runtime.managed_sequence_head.update(
                {
                    str(name): int(value)
                    for name, value in pending_intent["before"][
                        "sequences"
                    ].items()
                }
            )
        else:
            runtime.managed_sequence_head.update(
                {
                    str(name): int(value)
                    for name, value in invocation_start_state[
                        "sequences"
                    ].items()
                }
            )
        recovered_completions = _recover_missing_completions(
            paths,
            contract,
            batch_receipts,
            item_receipts,
            runtime,
            completions=completions,
        )
        completions.extend(recovered_completions)
        _validate_loaded_run_head(
            paths,
            contract,
            batch_receipts=batch_receipts,
            receipts=item_receipts,
            completions=completions,
            runtime=runtime,
            verify_full_content=True,
        )
        starting_batch_count = len(batch_receipts)
        if (
            through_batch < starting_batch_count
            or through_batch - starting_batch_count > max_new_batches
        ):
            raise FullLocalAnalysisError(
                "through绝对停点超过max_new_batches授权；需显式+1逐步推进"
            )
        idempotent_request = through_batch == starting_batch_count
        processed_this_invocation = 0
        completion = completions[-1] if completions else None
        while len(batch_receipts) < through_batch:
            proposal = _absolute_batch_plan(
                paths,
                contract,
                completed_batches=len(batch_receipts),
                completed_items=len(item_receipts),
                through_batch=len(batch_receipts) + 1,
                runtime=runtime,
            )
            ids = proposal["content_ids"]
            before_item_count = len(item_receipts)
            batch_receipt = _run_batch(
                paths,
                contract,
                batch_index=int(proposal["batch_index"]),
                content_ids=ids,
                start_ordinal=len(item_receipts) + 1,
                runtime=runtime,
            )
            completed_through = int(batch_receipt["item_receipts"][-1][0])
            if completed_through < len(item_receipts):
                raise FullLocalAnalysisError("new batch receipt item head倒退")
            for ordinal in range(len(item_receipts) + 1, completed_through + 1):
                item_receipts.append(
                    _read_json(
                        _item_paths(paths, ordinal)[1],
                        label="new item receipt append",
                    )
                )
            processed_this_invocation += len(item_receipts) - before_item_count
            batch_receipts.append(batch_receipt)
        _validate_remaining_eligible_baseline(
            paths,
            contract,
            completed_count=len(item_receipts),
        )
        end_state = _assert_global_invariants(paths, contract)
        _validate_invocation_end_state(end_state, batch_receipts[-1])
        _preflight_existing_physical(paths, allow_atomic_temps=False)
        completions.extend(
            _append_missing_completions(
                paths,
                contract,
                batch_receipts,
                item_receipts,
                runtime,
                completions=completions,
            )
        )
        completion = completions[-1] if completions else None
        if idempotent_request and completion is not None:
            status_counts = runtime.status_counts_by_count[len(item_receipts)]
            expected_status = _completion_status_from_counts(
                contract,
                completed_count=len(item_receipts),
                deferred_count=int(status_counts["deferred"]),
                review_pending_count=int(status_counts["review_pending"]),
                insufficient_evidence_count=int(
                    status_counts["insufficient_evidence"]
                ),
                completed_batches=starting_batch_count,
            )
            if (
                completion.get("status") != expected_status
                or completion.get("contract_sha256")
                != runtime.contract_sha256
                or completion.get("provider_calls") != 0
                or completion.get("full_history_complete") is not False
                or completion.get("publication_allowed") is not False
            ):
                raise FullLocalAnalysisError("幂等completion与当前闭包漂移")
        elif idempotent_request:
            raise FullLocalAnalysisError("幂等停点缺少immutable completion")
        if completion is None:
            raise FullLocalAnalysisError("目标停点缺少completion")
        _validate_loaded_run_head(
            paths,
            contract,
            batch_receipts=batch_receipts,
            receipts=item_receipts,
            completions=completions,
            runtime=runtime,
            verify_full_content=False,
        )
        return {
            "ok": True,
            "status": completion["status"],
            "idempotent": idempotent_request,
            "through_batch": through_batch,
            "max_new_batches": max_new_batches,
            "processed_this_invocation": processed_this_invocation,
            "eligible": completion["eligible"],
            "static_deferred": completion["static_deferred"],
            "missing_universe": completion["missing_universe"],
            "provider_calls": 0,
            "full_history_complete": False,
            "publication_allowed": False,
            "current_database": _database_identity(paths.database),
            "completion_sha256": local._sha256_file(
                paths.completions_root
                / f"{len(completions):06d}.completion.json"
            ),
            "resume_guard": completion["resume_guard"],
        }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Accumulate audited local analysis in one isolated work DB."
    )
    parser.add_argument("--source-db", required=True, type=Path)
    parser.add_argument("--source-completion", required=True, type=Path)
    parser.add_argument("--expected-source-db-sha256", required=True)
    parser.add_argument("--expected-source-completion-sha256", required=True)
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--media-root", required=True, type=Path)
    parser.add_argument("--run-root", required=True, type=Path)
    parser.add_argument(
        "--through-batch",
        "--stop-after-batch",
        dest="through_batch",
        required=True,
        type=int,
        help="Absolute batch index; repeat it for an idempotent stop.",
    )
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument(
        "--max-new-batches",
        type=int,
        default=1,
        help="Explicit per-invocation cap (1..25); pilot remains fixed at 1.",
    )
    parser.add_argument("--apply", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    kwargs = {
        "source_db_path": arguments.source_db,
        "source_completion_path": arguments.source_completion,
        "expected_source_db_sha256": arguments.expected_source_db_sha256,
        "expected_source_completion_sha256": (
            arguments.expected_source_completion_sha256
        ),
        "db_path": arguments.db,
        "media_root": arguments.media_root,
        "run_root": arguments.run_root,
        "through_batch": arguments.through_batch,
        "workers": arguments.workers,
        "max_new_batches": arguments.max_new_batches,
    }
    try:
        result = run_batches(**kwargs) if arguments.apply else plan_batches(**kwargs)
    except (FullLocalAnalysisError, local.LocalAnalysisCanaryError) as exc:
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
