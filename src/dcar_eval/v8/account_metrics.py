"""Append-only account statistics; account membership remains the roster's job."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import date, datetime, timedelta, timezone
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from .storage import now_utc


CONTRACT_VERSION = "account-metrics-matrix-first-v1"
ACCOUNT_FIELDS = (
    "follower_count", "platform_work_count", "total_likes",
    "total_likes_and_collects", "work_view_daily_increment",
    "work_like_daily_increment", "work_comment_daily_increment",
    "work_share_daily_increment", "collect_daily_increment",
)
FIELD_STATUSES = {"provided", "missing", "invalid", "not_applicable", "not_requested"}
_PROVIDERS = {"TikHub": "tikhub", "tikhub": "tikhub", "newrank_matrix": "newrank_matrix"}


class AccountMetricError(ValueError):
    pass


def _time(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError) as exc:
        raise AccountMetricError("invalid account metric timestamp") from exc
    if parsed.tzinfo is None:
        raise AccountMetricError("account metric timestamp needs a timezone")
    return parsed.astimezone(timezone.utc)


def _integer(value: Any, *, signed: bool = False) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        value = int(value)
    if not isinstance(value, int) or (not signed and value < 0):
        return None
    return value


def parse_tikhub_profile(
    payload: Any, *, platform: str, uid: str, http_status: int = 200
) -> dict[str, Any]:
    """The verified Douyin contract only supplies fans; XHS stays disabled."""
    if platform != "douyin":
        raise AccountMetricError("xiaohongshu profile contract is not enabled")
    if not isinstance(uid, str) or not uid:
        raise AccountMetricError("profile requires the exact platform UID string")
    outer = payload if isinstance(payload, dict) else {}
    envelope = outer.get("data")
    envelope = envelope if isinstance(envelope, dict) else {}
    data = envelope.get("data")
    data = data if isinstance(data, dict) else {}
    if (http_status != 200 or outer.get("code") != 200
            or envelope.get("status_code") != 0 or data.get("id_str") != uid):
        raise AccountMetricError("profile response status or account identity mismatch")
    follow = data.get("follow_info")
    follow = follow if isinstance(follow, dict) else {}
    count = _integer(follow.get("follower_count"))
    metric_status = (
        "missing" if follow.get("follower_count") is None
        else "provided" if count is not None else "invalid"
    )
    return {
        "identity": {"platform": platform, "uid": uid},
        "statistics_date": None,
        "basis": "realtime_profile",
        "metrics": {field: count if field == "follower_count" else None for field in ACCOUNT_FIELDS},
        "field_status": {
            field: {"status": metric_status if field == "follower_count" else "not_requested",
                    "reason": "verified_profile_fans" if field == "follower_count" else "contract_not_verified"}
            for field in ACCOUNT_FIELDS
        },
    }


def _validated_payload(
    value: Mapping[str, Any], *, platform: str, uid: str, provider: str
) -> dict[str, Any]:
    identity = value.get("identity")
    if not isinstance(identity, Mapping) or identity.get("platform") != platform or identity.get("uid") != uid:
        raise AccountMetricError("account metric subject does not match platform identity")
    if provider == "tikhub" and platform != "douyin":
        raise AccountMetricError("xiaohongshu profile contract is not enabled")
    statistics_date = value.get("statistics_date")
    basis = "matrix_daily" if provider == "newrank_matrix" else "realtime_profile"
    if provider == "newrank_matrix":
        try:
            if not isinstance(statistics_date, str) or date.fromisoformat(statistics_date).isoformat() != statistics_date:
                raise ValueError
        except ValueError as exc:
            raise AccountMetricError("Matrix account metrics require the returned rankDate") from exc
    elif statistics_date is not None:
        raise AccountMetricError("real-time profiles do not establish a historical statistics day")
    metrics, statuses = value.get("metrics"), value.get("field_status")
    if not isinstance(metrics, Mapping) or not isinstance(statuses, Mapping):
        raise AccountMetricError("account metric fields and statuses are required")
    fields: dict[str, Any] = {}
    for field in ACCOUNT_FIELDS:
        detail = statuses.get(field, {"status": "not_requested", "reason": "not_requested"})
        if not isinstance(detail, Mapping) or detail.get("status") not in FIELD_STATUSES:
            raise AccountMetricError("invalid account metric field status")
        status = str(detail["status"])
        parsed = _integer(metrics.get(field), signed=field.endswith("daily_increment"))
        if provider == "tikhub" and field != "follower_count" and status != "not_requested":
            raise AccountMetricError("profile field is outside the verified contract")
        if status == "provided" and parsed is None:
            raise AccountMetricError("provided account metric requires a valid integer")
        if field == "total_likes" and platform != "douyin":
            parsed, status = None, "not_applicable"
        if field in {"total_likes_and_collects"} and platform != "xiaohongshu":
            parsed, status = None, "not_applicable"
        if field == "work_view_daily_increment" and platform == "xiaohongshu":
            parsed, status = None, "not_applicable"
        if field == "collect_daily_increment":
            parsed, status = None, "not_requested"
        fields[field] = {"value": parsed if status == "provided" else None,
                         "status": status, "reason": str(detail.get("reason") or status)}
    identity_metadata = {
        field: value[field] for field in (
            "nickname", "avatar_url", "display_account_id", "description", "verify_type", "enterprise_verify_reason",
        ) if isinstance(value.get(field), str) and value[field].strip()
    }
    return {"identity": {"platform": platform, "uid": uid},
            "statistics_date": statistics_date, "basis": basis, "fields": fields,
            "identity_metadata": identity_metadata}


def persist_account_metric_observation(
    connection: sqlite3.Connection, *, account_identity_id: int, provider: str,
    raw_response_id: int, normalized: Mapping[str, Any], captured_at: str,
    recorded_at: str | None = None,
) -> dict[str, Any]:
    """A caller transaction owns both the raw response and the appended fact."""
    if not connection.in_transaction:
        raise AccountMetricError("account metrics require an active caller transaction")
    if provider not in {"newrank_matrix", "tikhub"}:
        raise AccountMetricError("unsupported account metric provider")
    identity = connection.execute(
        "SELECT * FROM account_platform_identities WHERE id=?", (account_identity_id,)
    ).fetchone()
    raw = connection.execute("SELECT * FROM provider_raw_responses WHERE id=?", (raw_response_id,)).fetchone()
    if identity is None or not identity["uid"] or raw is None:
        raise AccountMetricError("account metric identity or raw evidence is missing")
    if (_PROVIDERS.get(str(raw["provider"])) != provider or raw["content_id"] is not None
            or raw["account_id"] not in (None, identity["account_id"])):
        raise AccountMetricError("account metric raw provider or subject mismatch")
    allowed_operations = (
        {"matrix_account_list", "/api/matrix/v1/account/list"}
        if provider == "newrank_matrix"
        else {"douyin_user_profile", "douyin_uid_profile", "douyin_user_reference"}
    )
    if raw["operation"] not in allowed_operations:
        raise AccountMetricError("account metric raw operation is not the verified profile contract")
    if _time(captured_at) != _time(str(raw["captured_at"])):
        raise AccountMetricError("account metric captured time must match original raw evidence")
    mutation_at = recorded_at or now_utc()
    if _time(mutation_at) < _time(captured_at):
        raise AccountMetricError("account metric cannot be recorded before its capture")
    payload = _validated_payload(normalized, platform=str(identity["platform"]), uid=str(identity["uid"]), provider=provider)
    if payload["statistics_date"] and date.fromisoformat(payload["statistics_date"]) >= _time(captured_at).astimezone(ZoneInfo("Asia/Shanghai")).date():
        raise AccountMetricError("Matrix account statistics must describe a completed statistics day")
    payload["raw_sha256"] = str(raw["sha256"])
    payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    hash_material = [CONTRACT_VERSION, account_identity_id, provider, captured_at, raw_response_id, payload_json]
    digest = hashlib.sha256(json.dumps(hash_material, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
    existing = connection.execute(
        "SELECT * FROM account_metric_observations WHERE account_identity_id=? AND raw_response_id=?",
        (account_identity_id, raw_response_id),
    ).fetchone()
    if existing is not None:
        if existing["observation_sha256"] != digest:
            raise AccountMetricError("account metric replay changed its immutable payload")
        return {"id": int(existing["id"]), "created": False, "observation_sha256": digest}
    cursor = connection.execute(
        """INSERT INTO account_metric_observations(
            account_identity_id,source,captured_at,recorded_at,raw_response_id,
            contract_version,payload_json,observation_sha256
        ) VALUES (?,?,?,?,?,?,?,?)""",
        (account_identity_id, provider, captured_at, mutation_at, raw_response_id, CONTRACT_VERSION, payload_json, digest),
    )
    return {"id": int(cursor.lastrowid or 0), "created": True, "observation_sha256": digest}


def select_account_metrics(
    connection: sqlite3.Connection, account_identity_ids: Sequence[int], *, cutoff_at: str | None = None,
) -> dict[int, dict[str, Any]]:
    """Select each field at a two-time cutoff; stale values keep their own time."""
    from .source_routing import load_policy

    cutoff_at = cutoff_at or now_utc()
    cutoff = _time(cutoff_at)
    freshness_seconds = int(load_policy().get("account_freshness_seconds", 86400))
    output: dict[int, dict[str, Any]] = {}
    for identity_id in dict.fromkeys(account_identity_ids):
        rows = connection.execute(
            """SELECT * FROM account_metric_observations
            WHERE account_identity_id=? AND julianday(captured_at)<=julianday(?)
              AND julianday(recorded_at)<=julianday(?)
            ORDER BY julianday(captured_at) DESC,julianday(recorded_at) DESC,id DESC""",
            (identity_id, cutoff_at, cutoff_at),
        ).fetchall()
        observations = [dict(row) | {"payload": json.loads(row["payload_json"])} for row in rows]
        fields: dict[str, Any] = {}
        for field in ACCOUNT_FIELDS:
            candidates = []
            seen_providers: set[str] = set()
            for observation in observations:
                detail = observation["payload"]["fields"].get(field, {})
                if detail.get("status") == "not_requested":
                    continue
                source = str(observation["source"])
                is_latest = source not in seen_providers
                seen_providers.add(source)
                if detail.get("status") != "provided":
                    continue
                fresh = (is_latest and (cutoff - _time(observation["captured_at"])).total_seconds() <= freshness_seconds)
                statistics_date = observation["payload"].get("statistics_date")
                if statistics_date:
                    latest_day = cutoff.astimezone(ZoneInfo("Asia/Shanghai")).date() - timedelta(days=1)
                    fresh = fresh and date.fromisoformat(statistics_date) >= latest_day
                rank = (0 if fresh else 2) + (0 if source == "newrank_matrix" else 1)
                candidates.append((rank, observation, detail, fresh))
            if candidates:
                _, selected, detail, fresh = min(candidates, key=lambda item: item[0])
                fields[field] = {
                    **detail, "freshness": "fresh" if fresh else "stale",
                    "effective_provider": selected["source"], "observation_id": selected["id"],
                    "raw_response_id": selected["raw_response_id"], "captured_at": selected["captured_at"],
                    "recorded_at": selected["recorded_at"],
                    "statistics_date": selected["payload"]["statistics_date"],
                    "basis": selected["payload"]["basis"], "contract_version": CONTRACT_VERSION,
                }
            else:
                detail = next((o["payload"]["fields"][field] for o in observations
                               if o["payload"]["fields"].get(field, {}).get("status") != "not_requested"),
                              {"value": None, "status": "not_requested", "reason": "not_collected"})
                fields[field] = {**detail, "freshness": "unknown", "effective_provider": None,
                                 "observation_id": None, "raw_response_id": None,
                                 "captured_at": None, "recorded_at": None, "statistics_date": None}
        fans = fields["follower_count"]
        trend = {"status": "break", "reason": "no_comparable_consecutive_statistics", "delta": None}
        if fans.get("statistics_date") and fans.get("freshness") == "fresh":
            previous_date = (date.fromisoformat(fans["statistics_date"]) - timedelta(days=1)).isoformat()
            previous = next((o for o in observations
                             if o["payload"]["statistics_date"] == previous_date
                             and o["source"] == fans["effective_provider"]
                             and o["payload"]["basis"] == fans["basis"]), None)
            if previous is not None:
                previous_fans = previous["payload"]["fields"]["follower_count"]
                if previous_fans["status"] == "provided":
                    trend = {"status": "available", "reason": "consecutive_same_basis",
                             "delta": fans["value"] - previous_fans["value"]}
        values = {field: detail["value"] for field, detail in fields.items()}
        identity_metadata: dict[str, Any] = {}
        for observation in reversed(observations):
            if observation["source"] == "newrank_matrix":
                identity_metadata.update(observation["payload"].get("identity_metadata", {}))
        data_status = (fans["freshness"] if fans["value"] is not None else "not_collected")
        output[int(identity_id)] = {
            **values, "metric_fields": fields, "follower_trend": trend,
            "statistic_identity_metadata": identity_metadata,
            "data_date": fans.get("statistics_date") or fans.get("captured_at"),
            "data_status": "available" if data_status == "fresh" else data_status,
        }
    return output
