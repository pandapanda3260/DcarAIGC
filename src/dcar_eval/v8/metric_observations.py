"""Atomic metric fact persistence with an append-only observation history."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any, Literal

from .storage import metric_observation_sha256, now_utc
from .source_routing import (
    METRIC_FIELDS, correction_spec, effective_provider, normalize_provider,
    parse_time, select_content_metrics,
)


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


def _is_merged_legacy_snapshot(
    connection: sqlite3.Connection, *, legacy_id: object,
    content_id: int, window_key: str,
) -> bool:
    """Recognize a retired projection ID through immutable merged facts only."""
    if type(legacy_id) is not int or connection.execute(
        "SELECT 1 FROM content_metric_snapshots WHERE id=?", (legacy_id,)
    ).fetchone() is not None:
        return False
    if connection.execute(
        """SELECT 1 FROM content_aliases WHERE content_id=?
           AND reason='identity_upgrade_merge' LIMIT 1""", (content_id,),
    ).fetchone() is None:
        return False
    row = connection.execute(
        """SELECT * FROM content_metric_observations
           WHERE legacy_snapshot_id=? AND content_id=? AND window_key=?
             AND observation_origin='legacy_snapshot_baseline'""",
        (legacy_id, content_id, window_key),
    ).fetchone()
    if row is None:
        return False
    signed_fields = (
        "observation_origin", "legacy_snapshot_id", "subject_key", "captured_at",
        "window_key", *METRIC_FIELDS, "status", "source", "raw_response_id", "metadata_json",
    )
    return row["observation_sha256"] == metric_observation_sha256(
        **{field: row[field] for field in signed_fields}
    )


def _write_latest_snapshot(
    connection: sqlite3.Connection,
    *,
    content_id: int,
    window_key: str,
    projection: dict[str, object],
) -> tuple[int, bool]:
    columns = (
        "captured_at", *METRIC_FIELDS, "status", "source", "raw_response_id",
        "metadata_json",
    )
    existing = connection.execute(
        "SELECT * FROM content_metric_snapshots WHERE content_id=? AND window_key=?",
        (content_id, window_key),
    ).fetchone()
    legacy_id = projection.get("legacy_snapshot_id")
    if existing is not None and legacy_id is not None and existing["id"] != legacy_id:
        if not _is_merged_legacy_snapshot(
            connection, legacy_id=legacy_id, content_id=content_id, window_key=window_key,
        ):
            raise MetricObservationError("legacy snapshot identity would change")
    if existing is not None and all(existing[key] == projection[key] for key in columns):
        return int(existing["id"]), False
    connection.execute(
        """
        INSERT INTO content_metric_snapshots(
            id,content_id,captured_at,window_key,view_count,comment_count,
            like_count,share_count,collect_count,status,source,
            raw_response_id,metadata_json
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(content_id,window_key) DO UPDATE SET
            captured_at=excluded.captured_at,view_count=excluded.view_count,
            comment_count=excluded.comment_count,like_count=excluded.like_count,
            share_count=excluded.share_count,collect_count=excluded.collect_count,
            status=excluded.status,source=excluded.source,
            raw_response_id=excluded.raw_response_id,metadata_json=excluded.metadata_json
        """,
        (
            existing["id"] if existing is not None else legacy_id,
            content_id, projection["captured_at"], window_key,
            *(projection[key] for key in METRIC_FIELDS),
            projection["status"], projection["source"],
            projection["raw_response_id"], projection["metadata_json"],
        ),
    )
    row = connection.execute(
        "SELECT id FROM content_metric_snapshots WHERE content_id=? AND window_key=?",
        (content_id, window_key),
    ).fetchone()
    if row is None:
        raise MetricObservationError("latest metric snapshot was not materialized")
    return int(row["id"]), True


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
    provenance_columns = ""
    provenance_placeholders = ""
    provenance_values: tuple[Any, ...] = ()
    if connection.execute("PRAGMA user_version").fetchone()[0] in {20, 21}:
        from .metric_field_facts import observation_provenance
        provenance = observation_provenance(
            connection, content_id=content_id, source=source,
            raw_response_id=raw_response_id, metadata_json=metadata_json,
        )
        provenance_columns = "," + ",".join(provenance)
        provenance_placeholders = "," + ",".join("?" for _ in provenance)
        provenance_values = tuple(provenance.values())
    cursor = connection.execute(
        f"""
        INSERT INTO content_metric_observations(
            content_id,subject_key,captured_at,window_key,
            view_count,comment_count,like_count,share_count,collect_count,
            status,source,raw_response_id,metadata_json,observation_origin,
            legacy_snapshot_id,observation_sha256,recorded_at{provenance_columns}
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?{provenance_placeholders})
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
        ) + provenance_values,
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
    source: str | None = None,
    raw_response_id: int | None,
    metadata_json: str,
    observation_origin: MetricObservationOrigin = "provider_capture",
    snapshot_mode: MetricSnapshotMode = "merge",
    recorded_at: str | None = None,
    provider: str | None = None,
    platform: str | None = None,
) -> MetricPersistenceResult:
    """Append a fact then use the shared selector, never a provider row merge.

    Old raw/subject/window replays preserve the exact original observation.
    Parser changes require explicit, field-scoped system corrections.
    """
    if not connection.in_transaction:
        raise MetricObservationError("metric persistence requires an active caller transaction")
    if status not in {"available", "missing", "stale"}:
        raise MetricObservationError(f"invalid metric status: {status}")
    if snapshot_mode not in {"merge", "replace", "preserve_existing_exposure"}:
        raise MetricObservationError(f"unknown metric snapshot mode: {snapshot_mode}")
    _validate_metadata(metadata_json)
    subject_key = _subject_key(connection, content_id)
    mutation_at = recorded_at or now_utc()
    try:
        parse_time(captured_at)
        parse_time(mutation_at)
    except ValueError as error:
        raise MetricObservationError(str(error)) from error
    content = connection.execute(
        "SELECT platform,account_id FROM content_items WHERE id=?", (content_id,)
    ).fetchone()
    projection_platform = str(content["platform"])
    if platform is not None and platform != projection_platform:
        raise MetricObservationError("metric projection platform does not match content")
    existing = None
    if raw_response_id is not None and observation_origin == "provider_capture":
        existing = connection.execute(
            """
            SELECT * FROM content_metric_observations
            WHERE raw_response_id=? AND subject_key=? AND window_key=?
              AND observation_origin='provider_capture'
            ORDER BY recorded_at,id LIMIT 1
            """, (raw_response_id, subject_key, window_key),
        ).fetchone()
    legacy_snapshot_id = None
    if observation_origin == "system_correction":
        spec = correction_spec({"metadata_json": metadata_json})
        if spec is None:
            raise MetricObservationError("system correction requires target, stable rule, action and fields")
        target = connection.execute(
            "SELECT * FROM content_metric_observations WHERE id=?",
            (spec["target_observation_id"],),
        ).fetchone()
        if target is None or target["content_id"] != content_id or target["window_key"] != window_key:
            raise MetricObservationError("system correction target must match content and window")
        if parse_time(str(target["recorded_at"])) > parse_time(mutation_at):
            raise MetricObservationError("system correction cannot precede its target")
        if parse_time(captured_at) != parse_time(str(target["captured_at"])):
            raise MetricObservationError("system correction must retain original capture time")
        if raw_response_id != target["raw_response_id"]:
            raise MetricObservationError("system correction must retain original raw reference")
        target_raw = connection.execute(
            "SELECT provider FROM provider_raw_responses WHERE id=?", (raw_response_id,)
        ).fetchone()
        fact_source = (
            normalize_provider(target_raw["provider"]) if target_raw is not None else None
        ) or normalize_provider(target["source"]) or str(target["source"])
        existing = connection.execute(
            """
            SELECT * FROM content_metric_observations
            WHERE content_id=? AND window_key=? AND observation_origin='system_correction'
              AND json_extract(metadata_json,'$.correction.target_observation_id')=?
              AND json_extract(metadata_json,'$.correction.rule_id')=?
            ORDER BY id LIMIT 1
            """, (content_id, window_key, spec["target_observation_id"], spec["rule_id"]),
        ).fetchone()
        if existing is not None and (
            correction_spec(dict(existing)) != spec
            or any(existing[key] != value for key, value in zip(
                METRIC_FIELDS, (view_count, comment_count, like_count, share_count, collect_count)
            ))
        ):
            raise MetricObservationError("stable correction rule payload changed")
    elif observation_origin == "legacy_snapshot_baseline":
        fact_source = source or "migrated_historical"
        seed: dict[str, object] = {
            "captured_at": captured_at, "source": projection_platform,
            "status": "stale", "raw_response_id": raw_response_id,
            "metadata_json": metadata_json,
            "view_count": view_count, "comment_count": comment_count,
            "like_count": like_count, "share_count": share_count, "collect_count": collect_count,
        }
        legacy_snapshot_id, _ = _write_latest_snapshot(
            connection, content_id=content_id, window_key=window_key, projection=seed
        )
    elif observation_origin == "provider_capture":
        fact_source = ""
        if existing is None:
            raw = connection.execute(
                """
                SELECT r.*,
                       r.id raw_id,r.provider raw_provider,r.operation raw_operation,
                       r.content_id raw_content_id,r.account_id raw_account_id,
                       r.fetch_attempt_id raw_fetch_attempt_id,
                       a.id raw_attempt_id,a.slot_id raw_attempt_slot_id,
                       s.id raw_slot_id,s.stage raw_slot_stage,
                       s.provider raw_slot_provider,
                       s.content_id raw_slot_content_id,
                       s.account_id raw_slot_account_id
                FROM provider_raw_responses r
                LEFT JOIN fetch_attempts a ON a.id=r.fetch_attempt_id
                LEFT JOIN fetch_slots s ON s.id=a.slot_id
                WHERE r.id=?
                """,
                (raw_response_id,),
            ).fetchone()
            raw_values = dict(raw) if raw is not None else {}
            fact_source = effective_provider({
                "source": provider or source or projection_platform,
                "raw_response_id": raw_response_id,
                **{
                    key: raw_values.get(key)
                    for key in (
                        "raw_id", "raw_provider", "raw_operation",
                        "raw_content_id", "raw_account_id",
                        "raw_fetch_attempt_id", "raw_attempt_id",
                        "raw_attempt_slot_id", "raw_slot_id",
                        "raw_slot_stage", "raw_slot_provider",
                        "raw_slot_content_id", "raw_slot_account_id",
                    )
                },
                "content_id": content_id, "account_id": content["account_id"],
                "metadata_json": metadata_json,
            })
            if connection.execute("PRAGMA user_version").fetchone()[0] in {20, 21}:
                from .metric_field_facts import observation_provenance
                fact_source = str(observation_provenance(
                    connection, content_id=content_id, source=provider or source or projection_platform,
                    raw_response_id=raw_response_id, metadata_json=metadata_json,
                )["effective_provider"])
            if fact_source not in {"newrank_matrix", "tikhub"}:
                raise MetricObservationError("new metric capture requires verified Matrix or TikHub raw")
            if provider is not None and normalize_provider(provider) != fact_source:
                raise MetricObservationError("metric provider conflicts with raw provider")
    else:
        raise MetricObservationError(f"invalid observation origin: {observation_origin}")
    if existing is None:
        if parse_time(captured_at) > parse_time(mutation_at):
            raise MetricObservationError("metric capture cannot be later than recording time")
        if observation_origin == "provider_capture":
            payload = json.loads(metadata_json)
            fields = payload.get("fields", payload.get("field_status", {}))
            if not isinstance(fields, dict):
                raise MetricObservationError("metric field metadata must be an object")
            fields = fields.copy()
            values = [view_count, comment_count, like_count, share_count, collect_count]
            invalid = False
            for index, (name, value) in enumerate(zip(METRIC_FIELDS, values)):
                if value is not None and (
                    isinstance(value, bool) or not isinstance(value, int) or value < 0
                ):
                    values[index] = None
                    fields[name] = {
                        "status": "invalid", "reason": "expected_nonnegative_integer"
                    }
                    invalid = True
            if invalid:
                payload["fields"] = fields
                metadata_json = json.dumps(
                    payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                )
                view_count, comment_count, like_count, share_count, collect_count = values
        observation_id, observation_created = _insert_observation(
            connection, content_id=content_id, subject_key=subject_key,
            captured_at=captured_at, window_key=window_key,
            view_count=view_count, comment_count=comment_count, like_count=like_count,
            share_count=share_count, collect_count=collect_count, status=status,
            source=fact_source, raw_response_id=raw_response_id, metadata_json=metadata_json,
            observation_origin=observation_origin, legacy_snapshot_id=legacy_snapshot_id,
            recorded_at=mutation_at,
        )
    else:
        observation_id, observation_created = int(existing["id"]), False
    if connection.execute("PRAGMA user_version").fetchone()[0] in {20, 21}:
        from .metric_field_facts import ingest_observation, project_content
        ingest_observation(connection, observation_id, record_anomalies=observation_created)
        physical_projection = project_content(connection, content_id, cutoff_at=mutation_at,
                                              window_key=window_key, resolve_aliases=False)
        project_content(connection, content_id, cutoff_at=mutation_at)
        selected = physical_projection["business_projection"]
    else:
        selected = select_content_metrics(
            connection, [content_id], cutoff_at=mutation_at, window_key=window_key
        ).get(content_id)
    if selected is None:
        raise MetricObservationError("metric observation is not visible at projection cutoff")
    snapshot_id, snapshot_changed = _write_latest_snapshot(
        connection, content_id=content_id, window_key=window_key, projection=selected
    )
    return MetricPersistenceResult(
        observation_id=observation_id, snapshot_id=snapshot_id,
        observation_created=observation_created, snapshot_changed=snapshot_changed,
    )


def rebuild_metric_snapshots(
    connection: sqlite3.Connection,
    content_ids: list[int] | None = None,
    *,
    cutoff_at: str | None = None,
    window_key: str | None = None,
) -> dict[str, object]:
    """Rebuild canonical rows in place, preserving legacy snapshot IDs."""
    if not connection.in_transaction:
        raise MetricObservationError("metric rebuild requires an active caller transaction")
    parameters: list[object] = []
    where = ""
    if content_ids is not None:
        if not content_ids:
            return {"rebuilt": 0, "changed": 0, "snapshot_ids": []}
        where = f"WHERE content_id IN ({','.join('?' for _ in content_ids)})"
        parameters.extend(content_ids)
    if window_key is not None:
        where += (" AND " if where else "WHERE ") + "window_key=?"
        parameters.append(window_key)
    windows: dict[str, list[int]] = {}
    for row in connection.execute(
        f"SELECT DISTINCT content_id,window_key FROM content_metric_observations {where}",
        parameters,
    ):
        windows.setdefault(str(row["window_key"]), []).append(int(row["content_id"]))
    rebuilt: list[int] = []
    changed = 0
    at = cutoff_at or now_utc()
    for window, ids in windows.items():
        if connection.execute("PRAGMA user_version").fetchone()[0] in {20, 21}:
            from .metric_field_facts import project_content
            rows = {}
            for content_id in ids:
                physical = project_content(connection, content_id, cutoff_at=at, window_key=window,
                                           resolve_aliases=False)["business_projection"]
                project_content(connection, content_id, cutoff_at=at)
                if physical is not None:
                    rows[content_id] = physical
        else:
            rows = select_content_metrics(connection, ids, cutoff_at=at, window_key=window)
        for content_id, projection in rows.items():
            snapshot_id, modified = _write_latest_snapshot(
                connection, content_id=content_id, window_key=window, projection=projection
            )
            rebuilt.append(snapshot_id)
            changed += int(modified)
    return {"rebuilt": len(rebuilt), "changed": changed, "snapshot_ids": rebuilt}


def merge_metric_snapshots(
    connection: sqlite3.Connection, *, survivor_id: int, loser_id: int,
) -> None:
    """Coalesce projections, never facts, inside the content merge savepoint."""
    if not connection.in_transaction:
        raise MetricObservationError("metric merge requires an active caller transaction")
    surviving_windows = {
        str(row["window_key"]) for row in connection.execute(
            "SELECT window_key FROM content_metric_snapshots WHERE content_id=?",
            (survivor_id,),
        )
    }
    losing_snapshots = connection.execute(
        "SELECT id,window_key FROM content_metric_snapshots WHERE content_id=?",
        (loser_id,),
    ).fetchall()
    for row in losing_snapshots:
        if row["window_key"] not in surviving_windows:
            continue
        # Snapshot-only historical values are not disposable. Without both
        # sides' immutable facts there is no lossless canonical projection.
        for content_id in (survivor_id, loser_id):
            if connection.execute(
                """SELECT 1 FROM content_metric_observations
                   WHERE content_id=? AND window_key=? LIMIT 1""",
                (content_id, row["window_key"]),
            ).fetchone() is None:
                raise MetricObservationError("overlapping metric snapshots lack immutable facts")
    connection.execute(
        "UPDATE content_metric_observations SET content_id=? WHERE content_id=?",
        (survivor_id, loser_id),
    )
    for row in losing_snapshots:
        if row["window_key"] in surviving_windows:
            connection.execute("DELETE FROM content_metric_snapshots WHERE id=?", (row["id"],))
        else:
            connection.execute(
                "UPDATE content_metric_snapshots SET content_id=? WHERE id=?",
                (survivor_id, row["id"]),
            )
    rebuild_metric_snapshots(connection, [survivor_id])
