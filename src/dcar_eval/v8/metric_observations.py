"""Atomic metric fact persistence with an append-only observation history."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Literal

from .storage import metric_observation_sha256, now_utc


MetricSnapshotMode = Literal["merge", "replace", "preserve_existing_exposure"]
MetricObservationOrigin = Literal[
    "provider_capture", "legacy_snapshot_baseline", "system_correction"
]


class MetricObservationError(RuntimeError):
    """Raised before a partial metric fact can escape its caller transaction."""


@dataclass(frozen=True)
class MetricPersistenceResult:
    observation_id: int
    snapshot_id: int
    observation_created: bool
    snapshot_changed: bool


def _subject_key(connection: sqlite3.Connection, content_id: int) -> str:
    row = connection.execute(
        """
        SELECT COALESCE(
                   (
                       SELECT ci.platform_identity_key
                       FROM content_identities ci
                       WHERE ci.content_id=c.id
                       ORDER BY ci.is_primary DESC,ci.id
                       LIMIT 1
                   ),
                   'link:' || c.link_id
               ) subject_key
        FROM content_items c
        WHERE c.id=?
        """,
        (content_id,),
    ).fetchone()
    if row is None or not str(row["subject_key"] or "").strip():
        raise MetricObservationError(
            f"metric observation content identity is missing: {content_id}"
        )
    return str(row["subject_key"])


def _validate_metadata(metadata_json: str) -> None:
    try:
        value = json.loads(metadata_json)
    except (TypeError, json.JSONDecodeError) as error:
        raise MetricObservationError("metric metadata must be valid JSON") from error
    if not isinstance(value, dict):
        raise MetricObservationError("metric metadata must be a JSON object")


def _write_latest_snapshot(
    connection: sqlite3.Connection,
    *,
    content_id: int,
    captured_at: str,
    window_key: str,
    view_count: int | None,
    comment_count: int | None,
    like_count: int | None,
    share_count: int | None,
    collect_count: int | None,
    status: str,
    source: str,
    raw_response_id: int | None,
    metadata_json: str,
    mode: MetricSnapshotMode,
) -> tuple[int, bool]:
    values = (
        content_id,
        captured_at,
        window_key,
        view_count,
        comment_count,
        like_count,
        share_count,
        collect_count,
        status,
        source,
        raw_response_id,
        metadata_json,
    )
    if mode == "merge":
        cursor = connection.execute(
            """
            INSERT INTO content_metric_snapshots(
                content_id,captured_at,window_key,view_count,comment_count,
                like_count,share_count,collect_count,status,source,
                raw_response_id,metadata_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(content_id,window_key,source) DO UPDATE SET
                captured_at=excluded.captured_at,
                view_count=excluded.view_count,
                comment_count=COALESCE(
                    excluded.comment_count,content_metric_snapshots.comment_count
                ),
                like_count=COALESCE(
                    excluded.like_count,content_metric_snapshots.like_count
                ),
                share_count=COALESCE(
                    excluded.share_count,content_metric_snapshots.share_count
                ),
                collect_count=COALESCE(
                    excluded.collect_count,content_metric_snapshots.collect_count
                ),
                status=excluded.status,
                raw_response_id=excluded.raw_response_id,
                metadata_json=excluded.metadata_json
            """,
            values,
        )
    elif mode == "replace":
        cursor = connection.execute(
            """
            INSERT INTO content_metric_snapshots(
                content_id,captured_at,window_key,view_count,comment_count,
                like_count,share_count,collect_count,status,source,
                raw_response_id,metadata_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(content_id,window_key,source) DO UPDATE SET
                captured_at=excluded.captured_at,
                view_count=excluded.view_count,
                comment_count=excluded.comment_count,
                like_count=excluded.like_count,
                share_count=excluded.share_count,
                collect_count=excluded.collect_count,
                status=excluded.status,
                raw_response_id=excluded.raw_response_id,
                metadata_json=excluded.metadata_json
            """,
            values,
        )
    elif mode == "preserve_existing_exposure":
        cursor = connection.execute(
            """
            INSERT INTO content_metric_snapshots(
                content_id,captured_at,window_key,view_count,comment_count,
                like_count,share_count,collect_count,status,source,
                raw_response_id,metadata_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(content_id,window_key,source) DO UPDATE SET
                captured_at=excluded.captured_at,
                view_count=excluded.view_count,
                comment_count=excluded.comment_count,
                like_count=excluded.like_count,
                share_count=excluded.share_count,
                collect_count=excluded.collect_count,
                status=excluded.status,
                raw_response_id=excluded.raw_response_id,
                metadata_json=excluded.metadata_json
            WHERE content_metric_snapshots.view_count IS NULL
            """,
            values,
        )
    else:
        raise MetricObservationError(f"unknown metric snapshot mode: {mode}")
    row = connection.execute(
        """
        SELECT id FROM content_metric_snapshots
        WHERE content_id=? AND window_key=? AND source=?
        """,
        (content_id, window_key, source),
    ).fetchone()
    if row is None:
        raise MetricObservationError("latest metric snapshot was not materialized")
    return int(row["id"]), cursor.rowcount > 0


def _insert_observation(
    connection: sqlite3.Connection,
    *,
    content_id: int,
    subject_key: str,
    captured_at: str,
    window_key: str,
    view_count: int | None,
    comment_count: int | None,
    like_count: int | None,
    share_count: int | None,
    collect_count: int | None,
    status: str,
    source: str,
    raw_response_id: int | None,
    metadata_json: str,
    observation_origin: MetricObservationOrigin,
    legacy_snapshot_id: int | None,
    recorded_at: str,
) -> tuple[int, bool]:
    digest = metric_observation_sha256(
        observation_origin=observation_origin,
        legacy_snapshot_id=legacy_snapshot_id,
        subject_key=subject_key,
        captured_at=captured_at,
        window_key=window_key,
        view_count=view_count,
        comment_count=comment_count,
        like_count=like_count,
        share_count=share_count,
        collect_count=collect_count,
        status=status,
        source=source,
        raw_response_id=raw_response_id,
        metadata_json=metadata_json,
    )
    cursor = connection.execute(
        """
        INSERT INTO content_metric_observations(
            content_id,subject_key,captured_at,window_key,
            view_count,comment_count,like_count,share_count,collect_count,
            status,source,raw_response_id,metadata_json,observation_origin,
            legacy_snapshot_id,observation_sha256,recorded_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(observation_sha256) DO NOTHING
        """,
        (
            content_id,
            subject_key,
            captured_at,
            window_key,
            view_count,
            comment_count,
            like_count,
            share_count,
            collect_count,
            status,
            source,
            raw_response_id,
            metadata_json,
            observation_origin,
            legacy_snapshot_id,
            digest,
            recorded_at,
        ),
    )
    row = connection.execute(
        "SELECT * FROM content_metric_observations WHERE observation_sha256=?",
        (digest,),
    ).fetchone()
    if row is None:
        raise MetricObservationError("metric observation was not materialized")
    expected = {
        "subject_key": subject_key,
        "captured_at": captured_at,
        "window_key": window_key,
        "view_count": view_count,
        "comment_count": comment_count,
        "like_count": like_count,
        "share_count": share_count,
        "collect_count": collect_count,
        "status": status,
        "source": source,
        "raw_response_id": raw_response_id,
        "metadata_json": metadata_json,
        "observation_origin": observation_origin,
        "legacy_snapshot_id": legacy_snapshot_id,
    }
    mismatched = [key for key, value in expected.items() if row[key] != value]
    if mismatched:
        raise MetricObservationError(
            "metric observation SHA-256 collision or payload drift: "
            f"{','.join(sorted(mismatched))}"
        )
    return int(row["id"]), cursor.rowcount > 0


def persist_metric_observation(
    connection: sqlite3.Connection,
    *,
    content_id: int,
    captured_at: str,
    window_key: str,
    view_count: int | None,
    comment_count: int | None,
    like_count: int | None,
    share_count: int | None,
    collect_count: int | None,
    status: str,
    source: str,
    raw_response_id: int | None,
    metadata_json: str,
    observation_origin: MetricObservationOrigin = "provider_capture",
    snapshot_mode: MetricSnapshotMode = "merge",
    recorded_at: str | None = None,
) -> MetricPersistenceResult:
    """Append one metric fact and update its latest projection atomically.

    The caller must already own a transaction. Exact replays reuse the same
    observation, while the snapshot write is still attempted so a lost latest
    projection can be rebuilt without another provider request.
    """

    if not connection.in_transaction:
        raise MetricObservationError(
            "metric persistence requires an active caller transaction"
        )
    if status not in {"available", "missing", "stale"}:
        raise MetricObservationError(f"invalid metric status: {status}")
    _validate_metadata(metadata_json)
    subject_key = _subject_key(connection, content_id)
    mutation_at = recorded_at or now_utc()

    if observation_origin == "legacy_snapshot_baseline":
        snapshot_id, snapshot_changed = _write_latest_snapshot(
            connection,
            content_id=content_id,
            captured_at=captured_at,
            window_key=window_key,
            view_count=view_count,
            comment_count=comment_count,
            like_count=like_count,
            share_count=share_count,
            collect_count=collect_count,
            status=status,
            source=source,
            raw_response_id=raw_response_id,
            metadata_json=metadata_json,
            mode=snapshot_mode,
        )
        observation_id, observation_created = _insert_observation(
            connection,
            content_id=content_id,
            subject_key=subject_key,
            captured_at=captured_at,
            window_key=window_key,
            view_count=view_count,
            comment_count=comment_count,
            like_count=like_count,
            share_count=share_count,
            collect_count=collect_count,
            status=status,
            source=source,
            raw_response_id=raw_response_id,
            metadata_json=metadata_json,
            observation_origin=observation_origin,
            legacy_snapshot_id=snapshot_id,
            recorded_at=mutation_at,
        )
    else:
        observation_id, observation_created = _insert_observation(
            connection,
            content_id=content_id,
            subject_key=subject_key,
            captured_at=captured_at,
            window_key=window_key,
            view_count=view_count,
            comment_count=comment_count,
            like_count=like_count,
            share_count=share_count,
            collect_count=collect_count,
            status=status,
            source=source,
            raw_response_id=raw_response_id,
            metadata_json=metadata_json,
            observation_origin=observation_origin,
            legacy_snapshot_id=None,
            recorded_at=mutation_at,
        )
        snapshot_id, snapshot_changed = _write_latest_snapshot(
            connection,
            content_id=content_id,
            captured_at=captured_at,
            window_key=window_key,
            view_count=view_count,
            comment_count=comment_count,
            like_count=like_count,
            share_count=share_count,
            collect_count=collect_count,
            status=status,
            source=source,
            raw_response_id=raw_response_id,
            metadata_json=metadata_json,
            mode=snapshot_mode,
        )
    return MetricPersistenceResult(
        observation_id=observation_id,
        snapshot_id=snapshot_id,
        observation_created=observation_created,
        snapshot_changed=snapshot_changed,
    )
