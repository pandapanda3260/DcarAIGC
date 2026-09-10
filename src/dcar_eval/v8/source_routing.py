"""Read-only, versioned per-field selection over immutable metric facts."""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

METRIC_FIELDS = (
    "view_count", "comment_count", "like_count", "share_count", "collect_count"
)
_METADATA_CACHE_KEY = "_source_routing_metadata"
_PROVIDER_CACHE_KEY = "_source_routing_provider"
_OPERATION_CACHE_KEY = "_source_routing_operation"
# Observation, original field state, proven source, effective field status.
_FieldCandidate = tuple[
    dict[str, Any], tuple[str, int | None, str], tuple[str, str | None], str
]
POLICY_VERSION = "source-routing-matrix-first-v2"
_BEIJING = ZoneInfo("Asia/Shanghai")
_POLICY_FILE = (
    Path(__file__).resolve().parents[3]
    / "config" / "source_routing_matrix_first_v2.json"
)


@lru_cache(maxsize=1)
def _policy() -> dict[str, Any]:
    result = json.loads(_POLICY_FILE.read_text(encoding="utf-8"))
    if result.get("policy_version") != POLICY_VERSION:
        raise ValueError("unsupported metric source routing policy")
    return result


def load_policy() -> dict[str, Any]:
    """A copy prevents callers from mutating the fixed process policy."""
    return json.loads(json.dumps(_policy()))


def parse_time(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("metric timestamps must include timezone")
    return result.astimezone(timezone.utc)


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        result = json.loads(value or "{}")
    except (TypeError, ValueError):
        return {}
    return result if isinstance(result, dict) else {}


def _observation_metadata(observation: Mapping[str, Any]) -> dict[str, Any]:
    cached = observation.get(_METADATA_CACHE_KEY)
    if isinstance(cached, dict):
        return cached
    result = _json_object(observation.get("metadata_json"))
    if isinstance(observation, dict):
        observation[_METADATA_CACHE_KEY] = result
    return result


def normalize_provider(value: Any) -> str | None:
    return {
        "tikhub": "tikhub",
        "newrank_matrix": "newrank_matrix",
        "douyinopenapi": "douyin_openapi",
        "douyin_openapi": "douyin_openapi",
    }.get(str(value or "").strip().lower())


def _proven_source(observation: Mapping[str, Any]) -> tuple[str, str | None]:
    """Resolve provider/operation from immutable evidence, never row origin.

    Matrix pages are durable multi-content responses and intentionally have no
    fetch slot.  TikHub/OpenAPI content facts must additionally prove their
    raw -> attempt -> slot lineage whenever that joined evidence is present.
    """

    cached = observation.get(_PROVIDER_CACHE_KEY)
    cached_operation = observation.get(_OPERATION_CACHE_KEY)
    if isinstance(cached, str) and (
        isinstance(cached_operation, str) or cached_operation is None
    ):
        return cached, cached_operation

    provider = "legacy_unknown"
    operation: str | None = None
    raw_provider = normalize_provider(observation.get("raw_provider"))
    raw_operation = str(observation.get("raw_operation") or "").strip()
    metadata = _observation_metadata(observation)
    declared_operation = str(
        metadata.get("derived_from_operation") or metadata.get("operation") or ""
    ).strip()
    source_provider = normalize_provider(observation.get("source"))
    operation_stages = _policy().get("provider_operation_stages", {})
    provider_operations = (
        operation_stages.get(raw_provider, {})
        if isinstance(operation_stages, dict) and raw_provider is not None
        else {}
    )
    configured_stages = (
        provider_operations.get(raw_operation)
        if isinstance(provider_operations, dict)
        else None
    )
    operation_stages_allowed = (
        {configured_stages}
        if isinstance(configured_stages, str)
        else set(configured_stages)
        if isinstance(configured_stages, list)
        and all(isinstance(value, str) for value in configured_stages)
        else set()
    )

    raw_subject_matches = (
        observation.get("raw_content_id") in (None, observation.get("content_id"))
        and (
            observation.get("raw_account_id") is None
            or observation.get("raw_account_id") == observation.get("account_id")
        )
    )
    source_matches = not (
        source_provider is not None and source_provider != raw_provider
    ) and not (
        observation.get("source")
        not in {"douyin", "xiaohongshu", "migrated_historical"}
        and source_provider is None
    )
    operation_matches = bool(
        raw_operation
        and operation_stages_allowed
        and (not declared_operation or declared_operation == raw_operation)
    )

    lineage_loaded = "raw_fetch_attempt_id" in observation
    lineage_matches = True
    if lineage_loaded and raw_provider != "newrank_matrix":
        raw_attempt_id = observation.get("raw_fetch_attempt_id")
        lineage_matches = bool(
            type(raw_attempt_id) is int
            and observation.get("raw_attempt_id") == raw_attempt_id
            and observation.get("raw_attempt_slot_id")
            == observation.get("raw_slot_id")
            and normalize_provider(observation.get("raw_slot_provider"))
            == raw_provider
            and observation.get("raw_slot_stage") in operation_stages_allowed
            and observation.get("raw_slot_content_id")
            in (None, observation.get("content_id"))
            and (
                observation.get("raw_slot_account_id") is None
                or observation.get("raw_slot_account_id")
                == observation.get("account_id")
            )
            and (
                observation.get("raw_content_id") is not None
                or observation.get("raw_account_id") is not None
            )
        )
        if not lineage_matches and observation.get("raw_attempt_slot_id") is None:
            # Schema20 batch requests prove membership before purchase. A
            # Detail/statistics bind exact content. Discovery instead binds the
            # requested account before purchase, never the returned works.
            batch_id = observation.get("raw_attempt_batch_id")
            lineage_matches = bool(
                type(raw_attempt_id) is int and observation.get("raw_attempt_id") == raw_attempt_id
                and type(batch_id) is int and observation.get("raw_batch_id") == batch_id
                and normalize_provider(observation.get("raw_batch_provider")) == raw_provider
                and observation.get("raw_batch_operation") == raw_operation
                and observation.get("raw_content_id") in (None, observation.get("content_id"))
                and (
                    observation.get("raw_batch_member_content_id") == observation.get("content_id")
                    or (
                        raw_operation in {"douyin_user_posts", "xiaohongshu_user_posts"}
                        and observation.get("raw_content_id") is None
                        and type(observation.get("raw_batch_member_account_id")) is int
                        and observation.get("raw_batch_member_account_id") == observation.get("account_id")
                        and observation.get("raw_account_id") == observation.get("account_id")
                    )
                )
            )

    if (
        observation.get("raw_response_id") is not None
        and observation.get("raw_id") is not None
        and raw_provider is not None
        and raw_subject_matches
        and source_matches
        and operation_matches
        and lineage_matches
    ):
        provider = raw_provider
        operation = raw_operation
    if isinstance(observation, dict):
        observation[_PROVIDER_CACHE_KEY] = provider
        observation[_OPERATION_CACHE_KEY] = operation
    return provider, operation


def effective_provider(observation: Mapping[str, Any]) -> str:
    return _proven_source(observation)[0]


def effective_operation(observation: Mapping[str, Any]) -> str | None:
    return _proven_source(observation)[1]


def _age_days(published_at: str | None, as_of: str) -> int | None:
    if not published_at:
        return None
    try:
        return (
            parse_time(as_of).astimezone(_BEIJING).date()
            - parse_time(published_at).astimezone(_BEIJING).date()
        ).days
    except ValueError:
        return None


def metric_freshness_seconds(published_at: str | None, *, as_of: str) -> int:
    age = _age_days(published_at, as_of)
    established = age is not None and age > int(_policy()["content_recent_days"])
    key = (
        "content_established_freshness_seconds" if established
        else "content_recent_freshness_seconds"
    )
    return int(_policy()[key])


def metric_refresh_due(
    content_id: int, published_at: str | None, *, as_of: str
) -> bool:
    age = _age_days(published_at, as_of)
    if age is None or age < 0 or age > int(_policy()["content_active_days"]):
        return False
    day = parse_time(as_of).astimezone(_BEIJING).date()
    return age <= int(_policy()["content_recent_days"]) or day.toordinal() % 3 == content_id % 3


def metric_cycle_key(
    content_id: int, published_at: str | None, *, as_of: str
) -> str:
    day = parse_time(as_of).astimezone(_BEIJING).date()
    age = _age_days(published_at, as_of)
    if age is not None and age > int(_policy()["content_recent_days"]):
        bucket = (day.toordinal() - content_id % 3) // 3
        return f"matrix-first:3d:{content_id % 3}:{bucket}"
    return f"matrix-first:1d:{day.isoformat()}"


def correction_spec(observation: Mapping[str, Any]) -> dict[str, Any] | None:
    correction = _observation_metadata(observation).get("correction")
    if not isinstance(correction, dict):
        return None
    target = correction.get("target_observation_id")
    fields = correction.get("fields")
    if not isinstance(target, int) or isinstance(target, bool) or target <= 0:
        return None
    if (
        not isinstance(fields, list) or not fields
        or any(value not in METRIC_FIELDS for value in fields)
    ):
        return None
    if (
        correction.get("action") not in {"invalidate", "replace"}
        or not str(correction.get("rule_id") or "").strip()
    ):
        return None
    return correction


def _field_state(
    observation: Mapping[str, Any], field: str, platform: str,
    *, proven_source: tuple[str, str | None] | None = None,
) -> tuple[str, int | None, str]:
    if platform == "xiaohongshu" and field == "view_count":
        return "not_applicable", None, "xiaohongshu_exposure_unsupported"
    metadata = _observation_metadata(observation)
    declared_fields = metadata.get("fields", metadata.get("field_status", {}))
    declaration = (
        declared_fields.get(field, {}) if isinstance(declared_fields, dict) else {}
    )
    if isinstance(declaration, str):
        declaration = {"status": declaration}
    if not isinstance(declaration, dict):
        declaration = {}
    requested = metadata.get("requested_fields")
    if isinstance(requested, list) and field not in requested:
        return "not_requested", None, "operation_did_not_request_field"
    declared = declaration.get("status")
    if declared == "not_applicable":
        return "invalid", None, "field_is_applicable_by_fixed_contract"
    if declared in {"not_requested", "missing", "invalid"}:
        return str(declared), None, str(declaration.get("reason") or declared)
    value = observation.get(field)
    if value is None:
        return "missing", None, "provider_field_missing"
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return "invalid", None, "expected_nonnegative_integer"
    provider, operation = proven_source or _proven_source(observation)
    operation = operation or ""
    audit_fields = _policy().get("audit_only_fields", {})
    if (
        provider == "tikhub"
        and isinstance(audit_fields, dict)
        and field in audit_fields.get(operation, [])
    ):
        return "audit_only", None, "provider_operation_field_is_audit_only"
    if (
        field == "view_count" and value == 0
        and provider == "tikhub"
        and operation in _policy()["tikhub_placeholder_zero_operations"]
    ):
        return "invalid", None, "tikhub_discovery_exposure_placeholder_zero"
    return "provided", value, str(declaration.get("reason") or "provided")


def _field_evidence(
    observation: Mapping[str, Any], field: str, platform: str,
    *, field_state: tuple[str, int | None, str] | None = None,
    proven_source: tuple[str, str | None] | None = None,
) -> dict[str, Any]:
    provider, operation = proven_source or _proven_source(observation)
    state, value, reason = field_state or _field_state(
        observation, field, platform, proven_source=(provider, operation)
    )
    evidence = {
        "value": value, "status": state, "freshness": "unknown",
        "effective_provider": provider,
        "effective_operation": operation,
        "observation_id": observation.get("id"),
        "raw_response_id": observation.get("raw_response_id"),
        "captured_at": observation.get("captured_at"),
        "recorded_at": observation.get("recorded_at"),
        "observation_sha256": observation.get("observation_sha256"),
        "reason": reason, "policy_version": POLICY_VERSION,
    }
    if state == "audit_only":
        evidence["observed_value"] = observation.get(field)
    return evidence


def _empty_field(field: str, platform: str) -> dict[str, Any]:
    unsupported = platform == "xiaohongshu" and field == "view_count"
    return {
        "value": None, "status": "not_applicable" if unsupported else "missing",
        "freshness": "unknown", "effective_provider": None,
        "observation_id": None, "raw_response_id": None,
        "captured_at": None, "recorded_at": None, "observation_sha256": None,
        "reason": "xiaohongshu_exposure_unsupported" if unsupported else "no_observation",
        "policy_version": POLICY_VERSION,
    }


def _observation_key(row: Mapping[str, Any]) -> tuple[datetime, datetime, int]:
    return (
        parse_time(str(row["captured_at"])),
        parse_time(str(row["recorded_at"])),
        int(row["id"] or 0),
    )


def _select_row(
    content: Mapping[str, Any],
    observations: list[dict[str, Any]],
    *,
    cutoff_at: str,
    metric_fields: tuple[str, ...] = METRIC_FIELDS,
) -> dict[str, Any] | None:
    if not observations:
        return None
    platform = str(content["platform"])
    cutoff = parse_time(cutoff_at)
    ttl = metric_freshness_seconds(content.get("published_at"), as_of=cutoff_at)
    observations.sort(key=_observation_key, reverse=True)
    by_id = {row["id"]: row for row in observations}
    invalidated: dict[str, set[int]] = defaultdict(set)
    valid_corrections: dict[int, dict[str, Any]] = {}
    for row in observations:
        if row.get("observation_origin") != "system_correction":
            continue
        correction = correction_spec(row)
        if correction is None:
            continue
        target = by_id.get(correction["target_observation_id"])
        if target is None or target["window_key"] != row["window_key"]:
            continue
        valid_corrections[row["id"]] = correction
        for field in correction["fields"]:
            invalidated[field].add(correction["target_observation_id"])

    selected: dict[str, dict[str, Any]] = {}
    sources = {row["id"]: _proven_source(row) for row in observations}
    providers = {source[0] for source in sources.values()}
    for field in metric_fields:
        if platform == "xiaohongshu" and field == "view_count":
            selected[field] = _empty_field(field, platform)
            continue
        # A field needs only a few candidates, regardless of history length.
        # Keep the original state so an invalidating correction preserves any
        # audit-only observed_value when its final evidence is constructed.
        first_candidate: _FieldCandidate | None = None
        first_requested: _FieldCandidate | None = None
        first_provided: _FieldCandidate | None = None
        newest_by_provider: dict[str, _FieldCandidate] = {}
        remaining_providers = providers.copy()
        for row in observations:
            if row["id"] in invalidated[field]:
                continue
            if row.get("observation_origin") == "system_correction":
                correction = valid_corrections.get(row["id"])
                if correction is None or field not in correction["fields"]:
                    continue
            else:
                correction = None
            source = sources[row["id"]]
            state = _field_state(row, field, platform, proven_source=source)
            status = (
                "invalid"
                if correction is not None and correction["action"] == "invalidate"
                else state[0]
            )
            candidate = (row, state, source, status)
            if first_candidate is None:
                first_candidate = candidate
            if first_requested is None and status != "not_requested":
                first_requested = candidate
            if first_provided is None and status == "provided":
                first_provided = candidate
            # An audit-only operation is evidence, not a statement that a
            # separately contracted operation's fact became missing/invalid.
            if status not in {"not_requested", "audit_only"}:
                newest_by_provider.setdefault(source[0], candidate)
                remaining_providers.discard(source[0])
            # Corrections were resolved across the complete history above.
            # Once every actual provider's newest requested fact and the
            # newest provided fallback are known, older rows cannot change
            # any choice or its latest_provider_status. Include legacy and
            # OpenAPI sources, even though they cannot supply a fresh value.
            if first_provided is not None and not remaining_providers:
                break
        chosen_candidate = None
        freshness = "unknown"
        freshness_selection_reason = None
        for provider in ("newrank_matrix", "tikhub"):
            newest = newest_by_provider.get(provider)
            if newest is None or newest[3] != "provided":
                continue
            row = newest[0]
            age = (cutoff - parse_time(str(row["captured_at"]))).total_seconds()
            if 0 <= age <= ttl and row["status"] != "stale":
                chosen_candidate = newest
                freshness = "fresh"
                freshness_selection_reason = (
                    "preferred_fresh_source" if provider == "newrank_matrix"
                    else "fixed_fallback_source"
                )
                break
        if chosen_candidate is None and first_provided is not None:
            # Historical values are visible, never authoritative after the
            # source's newest requested fact became missing or invalid.
            chosen_candidate = first_provided
            freshness = "stale"
            freshness_selection_reason = "historical_value_not_current"
        if chosen_candidate is None:
            chosen_candidate = first_requested or first_candidate
        if chosen_candidate is None:
            chosen = _empty_field(field, platform)
        else:
            row, state, source, _status = chosen_candidate
            chosen = _field_evidence(
                row, field, platform, field_state=state, proven_source=source
            )
            correction = valid_corrections.get(row["id"])
            if correction is not None and correction["action"] == "invalidate":
                chosen.update(
                    status="invalid", value=None,
                    reason=f"correction:{correction['rule_id']}",
                )
            chosen["freshness"] = freshness
            if freshness_selection_reason is not None:
                chosen["reason"] = freshness_selection_reason
        newest = newest_by_provider.get(str(chosen["effective_provider"]))
        is_latest_valid = (
            newest is not None
            and chosen["status"] == "provided"
            and newest[0]["id"] == chosen["observation_id"]
            and chosen["effective_provider"] in {"newrank_matrix", "tikhub"}
            and by_id[chosen["observation_id"]]["status"] != "stale"
        )
        chosen["is_latest_valid"] = is_latest_valid
        chosen["latest_provider_status"] = newest[3] if newest else None
        chosen["freshness_reason"] = (
            "within_refresh_cycle" if chosen["freshness"] == "fresh"
            else "refresh_cycle_expired" if is_latest_valid
            else "legacy_or_unknown_provider"
            if chosen["effective_provider"] not in {"newrank_matrix", "tikhub"}
            else "newer_source_missing_or_invalid"
            if newest is not None and newest[3] in {"missing", "invalid"}
            else "not_current_valid_fact"
        )
        selected[field] = chosen

    provided = [fact for fact in selected.values() if fact["status"] == "provided"]
    fresh = [fact for fact in provided if fact["freshness"] == "fresh"]
    exposure = selected["view_count"]
    if platform == "douyin":
        status = (
            "available" if exposure["freshness"] == "fresh"
            else "stale" if exposure["status"] == "provided" else "missing"
        )
    else:
        status = "available" if fresh else "stale" if provided else "missing"
    anchor = exposure if exposure["status"] == "provided" else (
        provided[0] if provided else _field_evidence(observations[0], "view_count", platform)
    )
    anchor_row = by_id.get(anchor["observation_id"], observations[0])
    metadata = _observation_metadata(observations[0]).copy()
    metadata.update({"policy_version": POLICY_VERSION, "fields": selected})
    raw_ids = {fact["raw_response_id"] for fact in provided}
    captured = min(
        (str(fact["captured_at"]) for fact in provided), key=parse_time,
        default=str(observations[0]["captured_at"]),
    )
    recorded = max(
        (str(fact["recorded_at"]) for fact in provided), key=parse_time,
        default=str(observations[0]["recorded_at"]),
    )
    result: dict[str, Any] = {
        "id": anchor["observation_id"], "observation_id": anchor["observation_id"],
        "content_id": int(content["id"]), "platform": platform, "source": platform,
        "status": status, "captured_at": captured, "recorded_at": recorded,
        "window_key": anchor_row["window_key"],
        "raw_response_id": next(iter(raw_ids)) if len(raw_ids) == 1 else None,
        "fields": selected,
        "metadata_json": json.dumps(
            metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ),
        "observation_origin": anchor_row.get("observation_origin"),
        "observation_sha256": anchor_row.get("observation_sha256"),
        "policy_version": POLICY_VERSION,
    }
    result["legacy_snapshot_id"] = next(
        (
            fact.get("legacy_snapshot_id") for fact in observations
            if fact.get("observation_origin") == "legacy_snapshot_baseline"
            and fact["window_key"] == anchor_row["window_key"]
        ),
        None,
    )
    result.update({field: selected[field]["value"] for field in metric_fields})
    return result


def select_content_metrics(
    connection: sqlite3.Connection,
    content_ids: Iterable[int],
    *,
    cutoff_at: str | None = None,
    knowledge_at: str | None = None,
    window_key: str | None = None,
    metric_fields: Iterable[str] = METRIC_FIELDS,
) -> dict[int, dict[str, Any]]:
    """Select facts known at both capture and recording cutoffs, in batches.

    Snapshot-only legacy rows are explicitly stale and unknown. They cannot be
    used for an explicit historical cutoff: their recording time is not known.
    """
    metric_field_set = set(metric_fields)
    requested_fields = tuple(field for field in METRIC_FIELDS if field in metric_field_set)
    unknown_fields = metric_field_set - set(METRIC_FIELDS)
    if unknown_fields:
        raise ValueError(f"unsupported metric fields: {sorted(unknown_fields)}")
    if not requested_fields or "view_count" not in requested_fields:
        raise ValueError("metric_fields must include view_count")
    current_read = cutoff_at is None
    cutoff = cutoff_at or (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )
    parse_time(cutoff)
    knowledge = knowledge_at or cutoff
    parse_time(knowledge)
    if connection.execute("PRAGMA user_version").fetchone()[0] in {20, 21}:
        from .metric_field_facts import select_metric_projections
        return select_metric_projections(
            connection, sorted({int(value) for value in content_ids}),
            cutoff_at=cutoff, knowledge_at=knowledge, window_key=window_key, metric_fields=requested_fields,
            current_read=current_read,
        )
    result: dict[int, dict[str, Any]] = {}
    ids = sorted({int(value) for value in content_ids})
    batch_size = min(connection.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER) - 3, 400)
    if batch_size < 1:
        raise ValueError("SQLite variable limit is too small for metric selection")
    for start in range(0, len(ids), batch_size):
        batch = ids[start:start + batch_size]
        placeholders = ",".join("?" for _ in batch)
        contents = {
            int(row["id"]): dict(row) for row in connection.execute(
                f"SELECT id,platform,published_at,account_id FROM content_items WHERE id IN ({placeholders})",
                batch,
            )
        }
        params: list[Any] = [*batch, cutoff, knowledge]
        window_filter = ""
        if window_key is not None:
            window_filter = "AND o.window_key=?"
            params.append(window_key)
        cursor = connection.execute(
            f"""
            SELECT o.*,r.id raw_id,r.provider raw_provider,r.operation raw_operation,
                   r.content_id raw_content_id,r.account_id raw_account_id,
                   r.fetch_attempt_id raw_fetch_attempt_id,
                   a.id raw_attempt_id,a.slot_id raw_attempt_slot_id,
                   s.id raw_slot_id,s.stage raw_slot_stage,s.provider raw_slot_provider,
                   s.content_id raw_slot_content_id,s.account_id raw_slot_account_id,
                   c.account_id
            FROM content_metric_observations o
            JOIN content_items c ON c.id=o.content_id
            LEFT JOIN provider_raw_responses r ON r.id=o.raw_response_id
            LEFT JOIN fetch_attempts a ON a.id=r.fetch_attempt_id
            LEFT JOIN fetch_slots s ON s.id=a.slot_id
            WHERE o.content_id IN ({placeholders})
              AND julianday(o.captured_at)<=julianday(?)
              AND julianday(o.recorded_at)<=julianday(?) {window_filter}
            ORDER BY o.content_id,o.captured_at DESC,o.id DESC
            """, params,
        )
        # Avoid sqlite3.Row's repeated linear column-name lookups when making
        # a mutable fact, and do not retain a second full fetched row list.
        cursor.row_factory = None
        columns = tuple(column[0] for column in cursor.description)
        grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for values in cursor:
            fact = dict(zip(columns, values))
            grouped[int(fact["content_id"])].append(fact)
        if current_read:
            missing = [value for value in batch if value not in grouped]
            if missing:
                missing_placeholders = ",".join("?" for _ in missing)
                fallback_params: list[Any] = [*missing, cutoff]
                fallback_filter = ""
                if window_key is not None:
                    fallback_filter = "AND s.window_key=?"
                    fallback_params.append(window_key)
                for row in connection.execute(
                    f"""
                    SELECT s.* FROM content_metric_snapshots s
                    WHERE s.content_id IN ({missing_placeholders})
                      AND julianday(s.captured_at)<=julianday(?)
                      {fallback_filter}
                      AND NOT EXISTS(
                          SELECT 1 FROM content_metric_observations o
                          WHERE o.content_id=s.content_id
                      )
                    ORDER BY s.captured_at DESC,s.id DESC
                    """, fallback_params,
                ):
                    if int(row["content_id"]) in grouped:
                        continue
                    fact = dict(row)
                    fact.update(
                        id=None, recorded_at=fact["captured_at"], status="stale",
                        source="legacy_unknown", raw_id=None,
                        observation_origin="snapshot_only_legacy_unknown",
                    )
                    grouped[int(row["content_id"])].append(fact)
        for content_id, facts in grouped.items():
            selected = _select_row(
                contents[content_id],
                facts,
                cutoff_at=cutoff,
                metric_fields=requested_fields,
            )
            if selected is not None:
                result[content_id] = selected
    return result
