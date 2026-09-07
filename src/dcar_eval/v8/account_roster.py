"""Complete Matrix roster acceptance and shared paid-work membership checks."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
from contextlib import contextmanager, nullcontext
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, NoReturn
from urllib.parse import urlsplit, urlunsplit

from .profile_activations import PROFILE_FAMILIES as PROFILE_SOURCE_FAMILIES
from .storage import now_utc, write_lock

CONTRACT_VERSION = "matrix-full-roster-v1"
SYSTEM_CONTRACT_VERSION = "system-managed-roster-v1"
CANDIDATE_JOB = "matrix_roster_candidate"
SYSTEM_CANDIDATE_JOB = "system_roster_candidate"
MATRIX_SOURCE_FAMILY = "matrix"
SYSTEM_SOURCE_FAMILY = "system"
SOURCE_FAMILIES = frozenset({MATRIX_SOURCE_FAMILY, SYSTEM_SOURCE_FAMILY})
SOURCE_TYPES = frozenset(
    {"bootstrap_export", "manual_export", "api_fullroster", "system_managed"}
)
MATRIX_SOURCE_TYPES = frozenset(
    {"bootstrap_export", "manual_export", "api_fullroster"}
)
PLATFORMS = frozenset({"douyin", "xiaohongshu", "wechat_channels", "kuaishou"})
CONFIRMATION_INTERVAL = timedelta(minutes=10)
# No full roster API has been verified. A caller-supplied assertion is not proof.
VERIFIED_FULL_ROSTER_CONTRACTS: frozenset[str] = frozenset()


class RosterError(ValueError):
    def __init__(self, code: str, message: str, **details: Any) -> None:
        super().__init__(message)
        self.code = code
        self.details = details

    def as_dict(self) -> dict[str, Any]:
        return {"status": self.code, "message": str(self), **self.details}


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha(value: bytes | str) -> str:
    return hashlib.sha256(value.encode() if isinstance(value, str) else value).hexdigest()


def _inserted_id(cursor: sqlite3.Cursor) -> int:
    if cursor.lastrowid is None:
        raise RosterError("roster_write_failed", "Insert did not return an append-only ID")
    return cursor.lastrowid


def _time(value: str) -> datetime:
    if not isinstance(value, str):
        raise RosterError("invalid_source_time", "Timezone-aware source time is required")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise RosterError("invalid_source_time", "Timezone-aware source time is required") from exc
    if parsed.tzinfo is None:
        raise RosterError("invalid_source_time", "Timezone-aware source time is required")
    return parsed.astimezone(timezone.utc)


def _timestamp(value: str) -> str:
    return _time(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _source_family(source_type: Any) -> str:
    if source_type in MATRIX_SOURCE_TYPES:
        return MATRIX_SOURCE_FAMILY
    if source_type == "system_managed":
        return SYSTEM_SOURCE_FAMILY
    raise RosterError("invalid_source_type", "Unsupported roster source type")


def _supports_source_families(connection: sqlite3.Connection) -> bool:
    snapshot_columns = {
        str(row[1])
        for row in connection.execute("PRAGMA table_info(account_roster_snapshots)")
    }
    member_columns = {
        str(row[1])
        for row in connection.execute("PRAGMA table_info(account_roster_members)")
    }
    return "source_family" in snapshot_columns and "member_key" in member_columns


@contextmanager
def _atomic(connection: sqlite3.Connection) -> Iterator[None]:
    nested = connection.in_transaction
    # A standalone BEGIN IMMEDIATE must hold the process write lock for its
    # whole span (see storage.write_lock); a nested SAVEPOINT already runs
    # inside the caller's locked transaction.
    with nullcontext() if nested else write_lock():
        connection.execute("SAVEPOINT roster_write" if nested else "BEGIN IMMEDIATE")
        try:
            yield
        except Exception:
            if nested:
                connection.execute("ROLLBACK TO roster_write")
                connection.execute("RELEASE roster_write")
            else:
                connection.rollback()
            raise
        else:
            if nested:
                connection.execute("RELEASE roster_write")
            else:
                connection.commit()


def normalize_profile(platform: str, value: str) -> str:
    parsed = urlsplit(str(value).strip())
    host = (parsed.hostname or "").lower()
    patterns = {
        "douyin": ("douyin.com", r"/user/[^/]+"),
        "xiaohongshu": ("xiaohongshu.com", r"/user/profile/[0-9a-fA-F]{24}"),
        "kuaishou": ("kuaishou.com", r"/profile/[^/]+"),
        "wechat_channels": ("channels.weixin.qq.com", r"/[^?#]+"),
    }
    if platform not in patterns:
        raise RosterError("invalid_platform", "Unsupported roster platform")
    domain, pattern = patterns[platform]
    try:
        valid_port = parsed.port in {None, 80, 443}
    except ValueError:
        valid_port = False
    if (
        parsed.scheme not in {"http", "https"} or parsed.username or parsed.password
        or host not in {domain, "www." + domain} or not valid_port
        or not re.fullmatch(pattern, parsed.path.rstrip("/"))
    ):
        raise RosterError("invalid_profile", "A stable platform homepage is required")
    canonical_host = domain if platform == "wechat_channels" else "www." + domain
    return urlunsplit(("https", canonical_host, parsed.path.rstrip("/"), "", ""))


def _member(value: Mapping[str, Any], *, source_family: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise RosterError("invalid_member", "Each member must be an object")
    platform = str(value.get("platform") or "")
    official_id = value.get("matrix_account_id")
    if platform not in PLATFORMS:
        raise RosterError("invalid_stable_key", "A supported platform is required")
    uid = value.get("uid")
    if uid is not None and (not isinstance(uid, str) or not uid.strip()):
        raise RosterError("invalid_uid", "UID must be a string or null, never a rounded number")
    uid = uid.strip() if uid else None
    if platform == "douyin" and uid and not re.fullmatch(r"[0-9]{6,24}", uid):
        raise RosterError("invalid_uid", "Douyin requires a verified numeric UID")
    if platform == "xiaohongshu" and uid and not re.fullmatch(r"[0-9a-fA-F]{24}", uid):
        raise RosterError("invalid_uid", "Xiaohongshu requires a verified 24-character UID")
    profile_value = value.get("profile_ref")
    if source_family == MATRIX_SOURCE_FAMILY:
        if not isinstance(official_id, str) or not official_id.strip():
            raise RosterError(
                "invalid_stable_key",
                "Platform and real Matrix account ID are required",
            )
        profile_ref = normalize_profile(platform, str(profile_value or ""))
        official_id = official_id.strip()
        member_key = f"matrix:{platform}:{official_id}"
    elif source_family == SYSTEM_SOURCE_FAMILY:
        if official_id not in {None, ""}:
            raise RosterError(
                "invalid_stable_key",
                "System-managed members cannot carry a Matrix account ID",
            )
        if uid is None:
            raise RosterError(
                "invalid_stable_key",
                "System-managed members require a verified platform UID",
            )
        if profile_value in {None, ""}:
            profile_ref = None
        elif not isinstance(profile_value, str):
            raise RosterError("invalid_profile", "Profile reference must be text or null")
        else:
            profile_ref = normalize_profile(platform, profile_value)
        official_id = None
        member_key = f"uid:{platform}:{uid}"
    else:
        raise RosterError("invalid_source_family", "Unsupported roster source family")
    monitoring = value.get("monitoring_status", "unknown")
    authorization = value.get("authorization_status", "unknown")
    if monitoring not in {"unknown", "monitored", "not_monitored"} or authorization not in {"unknown", "authorized", "unauthorized"}:
        raise RosterError("invalid_account_state", "Monitoring and authorization are separate enums")
    started = value.get("monitoring_started_at")
    metadata = dict(value.get("metadata") or {})
    metadata["nickname"] = str(value.get("nickname") or "")
    sec_user_id = value.get("sec_user_id")
    if sec_user_id not in {None, ""}:
        if not isinstance(sec_user_id, str) or not sec_user_id.strip():
            raise RosterError("invalid_sec_user_id", "sec_user_id must be text")
        metadata["sec_user_id"] = sec_user_id.strip()
    if "display_account_id" not in metadata and metadata.get("unique_id"):
        metadata["display_account_id"] = metadata["unique_id"]
    metadata.pop("unique_id", None)
    return {
        "platform": platform,
        "member_key": member_key,
        "matrix_account_id": official_id,
        "profile_ref": profile_ref,
        "uid": uid,
        "nickname": str(value.get("nickname") or ""),
        "monitoring_status": monitoring, "authorization_status": authorization,
        "monitoring_started_at": _timestamp(started) if started else None,
        "metadata": metadata,
    }


def normalize_member(
    value: Mapping[str, Any], *, source_family: str
) -> dict[str, Any]:
    """Validate one roster member without persisting candidate evidence."""

    if source_family not in SOURCE_FAMILIES:
        raise RosterError(
            "invalid_source_family", "Unsupported roster source family"
        )
    return _member(value, source_family=source_family)


def validate_payload(
    payload: Mapping[str, Any], *, source_bytes: bytes, allow_empty: bool = False,
) -> dict[str, Any]:
    source_type = payload.get("source_type")
    if source_type not in SOURCE_TYPES:
        raise RosterError("invalid_source_type", "Unsupported complete-roster source")
    source_family = _source_family(source_type)
    scope = dict(payload.get("scope") or {})
    organization = str(scope.get("organization") or "").strip()
    expected_account_scope = (
        "all_added_accounts"
        if source_family == MATRIX_SOURCE_FAMILY
        else "all_managed_accounts"
    )
    if (
        not organization
        or scope.get("coverage") != "full"
        or scope.get("account_scope") != expected_account_scope
    ):
        raise RosterError("incomplete_scope", "Full organization and all-added-account scope evidence is required")
    platforms = scope.get("platforms")
    if not isinstance(platforms, list) or set(platforms) != PLATFORMS:
        raise RosterError("incomplete_scope", "Scope must explicitly cover every supported platform")
    proof = dict(payload.get("source_evidence") or {})
    if not isinstance(proof.get("source_name"), str) or not proof["source_name"].strip():
        raise RosterError("missing_source_name", "An original attachment name/format is required")
    if proof.get("source_format") == "matrix-ui-verified-export-v1" and source_type != "bootstrap_export":
        raise RosterError("invalid_source_type", "The verified historical UI export is bootstrap-only")
    if source_family == SYSTEM_SOURCE_FAMILY:
        if (
            proof.get("kind") != "system_roster_seal"
            or proof.get("evidence_kind") != "system_roster_manifest"
            or proof.get("source_format") != "system-roster-json-v1"
        ):
            raise RosterError(
                "missing_system_evidence",
                "System roster requires a sealed canonical manifest",
            )
        record_id = str(proof.get("seal_id") or "")
    elif source_type == "api_fullroster":
        if proof.get("validated_contract_id") not in VERIFIED_FULL_ROSTER_CONTRACTS:
            raise RosterError("manual_export_required", "No complete roster API contract has been verified")
        record_id = str(proof.get("request_id") or "")
    else:
        if proof.get("kind") != "official_export" or proof.get("evidence_kind") not in {
            "official_export_record", "operator_declaration", "official_export_metadata"
        }:
            raise RosterError("missing_export_evidence", "Official export evidence or explicit manual declaration is required")
        record_id = str(proof.get("export_record_id") or "")
    if not record_id or not str(proof.get("scope_evidence") or "").strip():
        raise RosterError("missing_export_evidence", "Export/request identity and full-scope evidence are required")
    captured = _timestamp(str(payload.get("source_captured_at") or ""))
    source_time_field = (
        "sealed_at" if source_family == SYSTEM_SOURCE_FAMILY else "exported_at"
    )
    exported = _timestamp(str(proof.get(source_time_field) or ""))
    if captured != exported:
        raise RosterError("source_time_mismatch", "Source time must be the actual export/request time")
    source_sha = _sha(source_bytes)
    if proof.get("source_sha256") != source_sha:
        raise RosterError("source_hash_mismatch", "Attached source bytes do not match the declared digest")
    rows, count = payload.get("members"), payload.get("declared_count")
    if (
        not isinstance(rows, list) or isinstance(count, bool) or not isinstance(count, int)
        or count != len(rows)
        or (
            not rows
            and not (
                allow_empty
                and source_type in {"manual_export", "system_managed"}
            )
        )
    ):
        raise RosterError("incomplete_roster", "A complete roster must match its total; initial empty rosters are forbidden")
    pagination = dict(payload.get("pagination") or {})
    expected = pagination.get("expected_pages")
    if (
        not isinstance(expected, int) or isinstance(expected, bool) or expected < 1
        or pagination.get("pages") != list(range(1, expected + 1))
        or pagination.get("terminal") is not True
        or pagination.get("declared_totals") != [count] * expected
    ):
        raise RosterError("incomplete_pagination", "All pages, stable totals and a terminal marker are required")
    members = sorted(
        (_member(row, source_family=source_family) for row in rows),
        key=lambda row: row["member_key"],
    )
    keys = [row["member_key"] for row in members]
    digest_keys: Any = (
        [(row["platform"], row["matrix_account_id"]) for row in members]
        if source_family == MATRIX_SOURCE_FAMILY
        else keys
    )
    profiles = [
        (row["platform"], row["profile_ref"])
        for row in members
        if row["profile_ref"]
    ]
    uids = [(row["platform"], row["uid"]) for row in members if row["uid"]]
    if (
        len(set(keys)) != count
        or len(set(profiles)) != len(profiles)
        or len(set(uids)) != len(uids)
    ):
        raise RosterError("duplicate_member", "Duplicate official keys, profiles or verified UIDs are forbidden")
    # Uploaded attachments are decoded by the API, but acceptance also binds the
    # immutable stable keys to the actual source bytes, not just a caller's hash.
    source_members = _source_member_keys(
        source_bytes,
        proof,
        source_family=source_family,
        allow_empty=allow_empty and count == 0,
    )
    expected_keys = {
        (
            row["platform"],
            row["matrix_account_id"]
            if source_family == MATRIX_SOURCE_FAMILY
            else row["uid"],
            row["profile_ref"] or "",
        )
        for row in members
    }
    if source_members != expected_keys:
        raise RosterError("source_member_mismatch", "Normalized members differ from the attached complete export")
    scope = {
        "organization": organization,
        "platforms": sorted(PLATFORMS),
        "coverage": "full",
        "account_scope": expected_account_scope,
    }
    scope_identity: Any = scope
    source_record_identity: list[Any] = [
        organization,
        proof.get("kind", "api"),
        record_id,
    ]
    if source_family == SYSTEM_SOURCE_FAMILY:
        scope_identity = {"source_family": source_family, **scope}
        source_record_identity.insert(0, source_family)
    source_record = _sha(_json(source_record_identity))
    return {
        "contract_version": (
            CONTRACT_VERSION
            if source_family == MATRIX_SOURCE_FAMILY
            else SYSTEM_CONTRACT_VERSION
        ),
        "source_family": source_family,
        "source_type": source_type,
        "scope": scope,
        "scope_key": _sha(_json(scope_identity)),
        "source_captured_at": captured, "source_sha256": source_sha,
        "source_record_key": source_record,
        "source_instance_id": _sha(_json([source_record, exported])),
        "source_evidence": proof, "declared_count": count,
        "members_sha256": _sha(_json(digest_keys)), "members": members, "pagination": pagination,
        "require_existing_identities": bool(payload.get("require_existing_identities", False)),
    }


def _source_member_keys(
    source: bytes,
    proof: Mapping[str, Any],
    *,
    source_family: str = MATRIX_SOURCE_FAMILY,
    allow_empty: bool = False,
) -> set[tuple[str, str, str]]:
    if source_family == SYSTEM_SOURCE_FAMILY:
        try:
            original = json.loads(source)
        except (TypeError, ValueError) as exc:
            raise RosterError("invalid_export", "System roster manifest is invalid JSON") from exc
        values = original.get("members") if isinstance(original, Mapping) else None
        if not isinstance(values, list):
            raise RosterError("invalid_export", "System roster manifest must contain members")
        members = [
            _member(value, source_family=SYSTEM_SOURCE_FAMILY) for value in values
        ]
        keys = [
            (row["platform"], str(row["uid"]), row["profile_ref"] or "")
            for row in members
        ]
        if len(set(keys)) != len(keys):
            raise RosterError("duplicate_member", "System manifest contains duplicate stable keys")
        return set(keys)
    if proof.get("source_format") == "matrix-ui-verified-export-v1":
        original = json.loads(source)
        if original.get("coverage") != "full" or not original.get("export_source", {}).get("read_only_download"):
            raise RosterError("incomplete_scope", "Bootstrap requires the verified official full export")
        values = [
            {"platform": {"抖音": "douyin", "小红书": "xiaohongshu"}.get(row["platform"], row["platform"]),
             "matrix_account_id": row["matrix_account_id"], "profile_ref": row["profile_url"]}
            for row in original["accounts"]
        ]
    else:
        from .account_roster_upload import decode_official_export
        values = decode_official_export(source, source_name=str(proof["source_name"]), allow_empty=allow_empty)
    keys = [
        (
            row["platform"],
            row["matrix_account_id"],
            normalize_profile(row["platform"], row["profile_ref"]),
        )
        for row in values
    ]
    if len(set(keys)) != len(keys):
        raise RosterError("duplicate_member", "Attached export contains duplicate stable keys")
    return set(keys)


def _verify_raw(connection: sqlite3.Connection, candidate: Mapping[str, Any]) -> None:
    raw = connection.execute("SELECT * FROM provider_raw_responses WHERE id=?", (candidate["raw_response_id"],)).fetchone()
    source_family = candidate["payload"].get(
        "source_family", _source_family(candidate["payload"]["source_type"])
    )
    expected_provider = (
        "newrank_matrix"
        if source_family == MATRIX_SOURCE_FAMILY
        else "local_roster"
    )
    expected_operation = (
        CANDIDATE_JOB
        if source_family == MATRIX_SOURCE_FAMILY
        else SYSTEM_CANDIDATE_JOB
    )
    if (
        raw is None
        or raw["provider"] != expected_provider
        or raw["operation"] != expected_operation
    ):
        raise RosterError("roster_raw_missing", "Roster original evidence is missing")
    path = Path(raw["local_path"])
    if not path.is_file() or path.is_symlink():
        raise RosterError("roster_raw_missing", "Original evidence file is missing or unsafe")
    source = path.read_bytes()
    if (
        _sha(source) != raw["sha256"] or len(source) != raw["byte_size"]
        or raw["sha256"] != candidate["payload"]["source_sha256"]
        or _sha(_json(candidate["payload"])) != candidate["payload_sha256"]
    ):
        raise RosterError("roster_raw_mismatch", "Roster source or normalized manifest digest does not match")
    proof = candidate["payload"]["source_evidence"]
    allow_empty = (
        candidate["payload"]["source_type"] == "manual_export"
        and candidate["payload"]["declared_count"] == 0
        and latest_family_snapshot(connection, source_family) is not None
    )
    expected_keys = {
        (
            row["platform"],
            row["matrix_account_id"]
            if source_family == MATRIX_SOURCE_FAMILY
            else row["uid"],
            row["profile_ref"] or "",
        )
        for row in candidate["payload"]["members"]
    }
    if proof.get("source_name") and _source_member_keys(
        source,
        proof,
        source_family=source_family,
        allow_empty=allow_empty,
    ) != expected_keys:
        raise RosterError("source_member_mismatch", "Normalized members differ from retained raw evidence")


def prepare_candidate(
    connection: sqlite3.Connection, payload: Mapping[str, Any], *, source_bytes: bytes,
    raw_root: Path, observed_at: str | None = None,
) -> dict[str, Any]:
    """Persist an official source and a candidate; never alter current members."""
    source_family = _source_family(payload.get("source_type"))
    if (
        source_family == SYSTEM_SOURCE_FAMILY
        and not _supports_source_families(connection)
    ):
        raise RosterError(
            "schema_upgrade_required",
            "System-managed rosters require schema 19",
        )
    normalized = validate_payload(
        payload,
        source_bytes=source_bytes,
        allow_empty=latest_family_snapshot(connection, source_family) is not None,
    )
    observed = _timestamp(observed_at or now_utc())
    if _time(normalized["source_captured_at"]) > _time(observed) + timedelta(minutes=5):
        raise RosterError("future_source_time", "Export time is in the future")
    if (
        source_family == SYSTEM_SOURCE_FAMILY
        and normalized["source_captured_at"] != observed
    ):
        raise RosterError(
            "source_time_mismatch",
            "System roster source time must equal its sealing time",
        )
    raw_root = Path(raw_root).resolve()
    raw_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = raw_root / (normalized["source_sha256"] + ".roster-source")
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        if path.is_symlink() or _sha(path.read_bytes()) != normalized["source_sha256"]:
            raise RosterError("roster_raw_mismatch", "Existing original evidence is inconsistent")
    else:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(source_bytes)
            stream.flush()
            os.fsync(stream.fileno())
    with _atomic(connection):
        for old in _candidates(connection, source_family=source_family):
            if old["payload"]["source_sha256"] == normalized["source_sha256"]:
                _verify_raw(connection, old)
                return {**candidate_diff(connection, int(old["candidate_id"])), "replayed": True}
            if old["payload"]["source_instance_id"] == normalized["source_instance_id"]:
                raise RosterError("source_instance_conflict", "An export/request record cannot be reused with changed bytes")
        provider = (
            "newrank_matrix"
            if source_family == MATRIX_SOURCE_FAMILY
            else "local_roster"
        )
        operation = (
            CANDIDATE_JOB
            if source_family == MATRIX_SOURCE_FAMILY
            else SYSTEM_CANDIDATE_JOB
        )
        raw_source = (
            "official_roster"
            if source_family == MATRIX_SOURCE_FAMILY
            else "system_roster"
        )
        raw = connection.execute(
            """INSERT INTO provider_raw_responses(provider,operation,local_path,sha256,byte_size,
                      captured_at,source) VALUES (?,?,?,?,?,?,?)""",
            (
                provider,
                operation,
                str(path),
                normalized["source_sha256"],
                len(source_bytes),
                observed,
                raw_source,
            ),
        )
        detail = {"status": "candidate", "payload": normalized, "raw_response_id": _inserted_id(raw),
                  "observed_at": observed, "payload_sha256": _sha(_json(normalized))}
        run = connection.execute(
            """INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,completed_at,details_json)
               VALUES (?,?,'partial',?,?,?)""",
            (operation, normalized["source_instance_id"], observed, observed, _json(detail)),
        )
        return candidate_diff(connection, _inserted_id(run))


def _resolve_identity(
    connection: sqlite3.Connection, member: Mapping[str, Any], *, raw_id: int,
    timestamp: str, require_existing: bool, source_family: str,
) -> int:
    candidates: set[int] = set()
    if source_family == MATRIX_SOURCE_FAMILY:
        for kind, value in (
            ("matrix_account_id", member["matrix_account_id"]),
            ("profile_url", member["profile_ref"]),
        ):
            for row in connection.execute(
                """SELECT r.account_identity_id,i.platform FROM account_provider_references r
                   JOIN account_platform_identities i ON i.id=r.account_identity_id
                   WHERE r.provider='newrank_matrix' AND r.reference_kind=?
                   AND r.reference_value=?""",
                (kind, value),
            ):
                if row["platform"] != member["platform"]:
                    raise RosterError(
                        "identity_conflict",
                        "Official reference belongs to another platform",
                    )
                candidates.add(row["account_identity_id"])
        for row in connection.execute(
            """SELECT DISTINCT account_identity_id FROM account_roster_members
               WHERE platform=? AND matrix_account_id=?""",
            (member["platform"], member["matrix_account_id"]),
        ):
            candidates.add(row["account_identity_id"])
    elif _supports_source_families(connection):
        for row in connection.execute(
            """SELECT DISTINCT account_identity_id FROM account_roster_members
               WHERE member_key=?""",
            (member["member_key"],),
        ):
            candidates.add(row["account_identity_id"])
    if member["uid"]:
        for row in connection.execute(
            "SELECT id FROM account_platform_identities WHERE platform=? AND uid=?",
            (member["platform"], member["uid"]),
        ):
            candidates.add(row["id"])
    reference_values: set[str] = set()
    if member["profile_ref"]:
        reference_values.update(
            {
                member["profile_ref"],
                urlsplit(member["profile_ref"]).path.rsplit("/", 1)[-1],
            }
        )
    sec_user_id = member["metadata"].get("sec_user_id")
    if sec_user_id:
        reference_values.add(str(sec_user_id))
    if reference_values:
        placeholders = ",".join("?" for _ in reference_values)
        for row in connection.execute(
            f"""SELECT DISTINCT r.account_identity_id,i.platform
                FROM account_provider_references r
                JOIN account_platform_identities i ON i.id=r.account_identity_id
                WHERE r.reference_kind IN
                    ('sec_uid','sec_user_id','user_id','profile_id','profile_url')
                AND r.reference_value IN ({placeholders})""",
            tuple(sorted(reference_values)),
        ):
            if row["platform"] != member["platform"]:
                raise RosterError(
                    "identity_conflict",
                    "Provider reference belongs to another platform",
                )
            candidates.add(row["account_identity_id"])
    if len(candidates) > 1:
        raise RosterError("identity_conflict", "Official ID, profile and UID resolve to different identities")
    if candidates:
        identity_id = next(iter(candidates))
        identity = connection.execute("SELECT * FROM account_platform_identities WHERE id=?", (identity_id,)).fetchone()
        if member["uid"] and identity["uid"] and member["uid"] != identity["uid"]:
            raise RosterError("identity_conflict", "An existing verified UID cannot be replaced")
        if member["uid"] and identity["uid"] is None:
            if require_existing:
                raise RosterError("identity_unresolved", "Bootstrap identity map must be complete before acceptance")
            connection.execute("UPDATE account_platform_identities SET uid=?,updated_at=? WHERE id=?",
                               (member["uid"], timestamp, identity_id))
    else:
        if require_existing:
            raise RosterError("identity_unresolved", "Bootstrap must match every row to an existing identity")
        enabled = int(
            source_family != MATRIX_SOURCE_FAMILY or member["uid"] is not None
        )
        account = connection.execute(
            """INSERT INTO accounts(phone,phone_normalized,enabled,created_at,updated_at)
               VALUES ('',NULL,?,?,?)""",
            (enabled, timestamp, timestamp),
        )
        identity_source = (
            "newrank_matrix"
            if source_family == MATRIX_SOURCE_FAMILY
            else "system_roster"
        )
        inserted = connection.execute(
            """INSERT INTO account_platform_identities(account_id,platform,uid,nickname,source,created_at,updated_at)
               VALUES (?,?,?,?,?,?,?)""",
            (
                account.lastrowid,
                member["platform"],
                member["uid"],
                member["nickname"],
                identity_source,
                timestamp,
                timestamp,
            ),
        )
        identity_id = _inserted_id(inserted)
    if source_family == MATRIX_SOURCE_FAMILY:
        for kind, value in (
            ("matrix_account_id", member["matrix_account_id"]),
            ("profile_url", member["profile_ref"]),
        ):
            old = connection.execute(
                """SELECT reference_value FROM account_provider_references
                   WHERE account_identity_id=? AND provider='newrank_matrix'
                   AND reference_kind=?""",
                (identity_id, kind),
            ).fetchone()
            if old is None:
                connection.execute(
                    """INSERT INTO account_provider_references(
                              account_identity_id,provider,reference_kind,reference_value,
                              source_raw_response_id,created_at,updated_at)
                       VALUES (?,'newrank_matrix',?,?,?,?,?)""",
                    (identity_id, kind, value, raw_id, timestamp, timestamp),
                )
            elif old["reference_value"] != value:
                if require_existing:
                    raise RosterError(
                        "identity_conflict",
                        "Bootstrap cannot rewrite an existing Matrix reference",
                    )
                # Earlier accepted members retain the immutable official-ID history.
                connection.execute(
                    """UPDATE account_provider_references
                       SET reference_value=?,source_raw_response_id=?,updated_at=?
                       WHERE account_identity_id=? AND provider='newrank_matrix'
                       AND reference_kind=?""",
                    (value, raw_id, timestamp, identity_id, kind),
                )
    elif sec_user_id:
        old = connection.execute(
            """SELECT reference_value FROM account_provider_references
               WHERE account_identity_id=? AND provider='tikhub' COLLATE NOCASE
               AND reference_kind='sec_user_id'""",
            (identity_id,),
        ).fetchall()
        if not old:
            connection.execute(
                """INSERT INTO account_provider_references(
                          account_identity_id,provider,reference_kind,reference_value,
                          source_raw_response_id,created_at,updated_at)
                   VALUES (?,'tikhub','sec_user_id',?,?,?,?)""",
                (identity_id, sec_user_id, raw_id, timestamp, timestamp),
            )
        elif any(row["reference_value"] != sec_user_id for row in old):
            raise RosterError(
                "identity_conflict",
                "An existing sec_user_id reference cannot be replaced",
            )
    return identity_id


def accept_candidate(connection: sqlite3.Connection, candidate_id: int, accepted_at: str | None = None) -> dict[str, Any]:
    """Publish the entire candidate atomically, including any new local identities."""
    timestamp = _timestamp(accepted_at or now_utc())
    with _atomic(connection):
        candidate = _candidate(connection, candidate_id)
        if candidate["status"] == "accepted":
            return {**candidate_diff(connection, candidate_id), "replayed": True}
        _verify_raw(connection, candidate)
        payload = candidate["payload"]
        source_family = payload.get(
            "source_family", _source_family(payload["source_type"])
        )
        if (
            source_family == SYSTEM_SOURCE_FAMILY
            and not _supports_source_families(connection)
        ):
            raise RosterError(
                "schema_upgrade_required",
                "System-managed rosters require schema 19",
            )
        if _time(timestamp) < _time(payload["source_captured_at"]):
            raise RosterError("invalid_acceptance_time", "Acceptance cannot precede the export")
        current = latest_family_snapshot(connection, source_family)
        if current:
            if payload["source_type"] == "bootstrap_export":
                raise RosterError("bootstrap_already_accepted", "Bootstrap is permitted only once")
            if _time(payload["source_captured_at"]) <= _time(current["source_captured_at"]):
                raise RosterError("stale_source", "An older source cannot replace the current roster")
        candidates = _candidates(connection, source_family=source_family)
        if any(row["candidate_id"] > candidate_id and row["payload"]["scope_key"] == payload["scope_key"]
               for row in candidates):
            raise RosterError("candidate_superseded", "A newer complete candidate must be considered first")
        diff = candidate_diff(connection, candidate_id)
        confirmation_id = None
        if diff["removed"] and source_family == MATRIX_SOURCE_FAMILY:
            for earlier in candidates:
                if earlier["candidate_id"] >= candidate_id or earlier["payload"]["scope_key"] != payload["scope_key"]:
                    continue
                previous = earlier["payload"]
                if previous["members_sha256"] != payload["members_sha256"]:
                    break
                if (
                    previous["source_instance_id"] != payload["source_instance_id"]
                    and previous["source_record_key"] != payload["source_record_key"]
                    and previous["source_sha256"] != payload["source_sha256"]
                    and _time(payload["source_captured_at"]) - _time(previous["source_captured_at"]) >= CONFIRMATION_INTERVAL
                    and (not current or _time(previous["source_captured_at"]) > _time(current["source_captured_at"]))
                ):
                    _verify_raw(connection, earlier)
                    confirmation_id = earlier["candidate_id"]
                    break
            if confirmation_id is None:
                candidate["status"] = "pending_removal_confirmation"
                connection.execute("UPDATE scheduler_runs SET details_json=? WHERE id=?", (_json(candidate), candidate_id))
                return {**diff, "status": "pending_removal_confirmation"}
        raw = connection.execute("SELECT local_path FROM provider_raw_responses WHERE id=?", (candidate["raw_response_id"],)).fetchone()
        metadata = _json(
            {
                "raw_response_id": candidate["raw_response_id"],
                "source_evidence": payload["source_evidence"],
                "candidate_id": candidate_id,
                "confirmation_candidate_id": confirmation_id,
                "manifest_sha256": candidate["payload_sha256"],
            }
        )
        snapshot_values = (
            payload["source_type"],
            payload["scope_key"],
            _json(payload["scope"]),
            payload["source_instance_id"],
            payload["source_captured_at"],
            timestamp,
            payload["declared_count"],
            len(payload["members"]),
            payload["members_sha256"],
            payload["source_sha256"],
            raw["local_path"],
            payload.get("contract_version", CONTRACT_VERSION),
            metadata,
        )
        if _supports_source_families(connection):
            snapshot = connection.execute(
                """INSERT INTO account_roster_snapshots(
                          source_family,source_type,scope_key,scope_json,source_instance_id,
                          source_captured_at,accepted_at,declared_count,member_count,
                          members_sha256,source_sha256,source_path,contract_version,metadata_json)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (source_family, *snapshot_values),
            )
        else:
            snapshot = connection.execute(
                """INSERT INTO account_roster_snapshots(
                          source_type,scope_key,scope_json,source_instance_id,
                          source_captured_at,accepted_at,declared_count,member_count,
                          members_sha256,source_sha256,source_path,contract_version,metadata_json)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                snapshot_values,
            )
        snapshot_id = _inserted_id(snapshot)
        seen: set[int] = set()
        for member in payload["members"]:
            identity_id = _resolve_identity(
                connection, member, raw_id=candidate["raw_response_id"], timestamp=timestamp,
                require_existing=payload["require_existing_identities"],
                source_family=source_family,
            )
            if identity_id in seen:
                raise RosterError("identity_conflict", "Two roster rows resolve to the same local identity")
            seen.add(identity_id)
            if _supports_source_families(connection):
                connection.execute(
                    """INSERT INTO account_roster_members(
                              snapshot_id,account_identity_id,platform,member_key,uid,
                              matrix_account_id,profile_ref,monitoring_status,
                              authorization_status,monitoring_started_at,metadata_json)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        snapshot_id,
                        identity_id,
                        member["platform"],
                        member["member_key"],
                        member["uid"],
                        member["matrix_account_id"],
                        member["profile_ref"],
                        member["monitoring_status"],
                        member["authorization_status"],
                        member["monitoring_started_at"],
                        _json(member["metadata"]),
                    ),
                )
            else:
                connection.execute(
                    """INSERT INTO account_roster_members(
                              snapshot_id,account_identity_id,platform,matrix_account_id,
                              profile_ref,monitoring_status,authorization_status,
                              monitoring_started_at,metadata_json)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (
                        snapshot_id,
                        identity_id,
                        member["platform"],
                        member["matrix_account_id"],
                        member["profile_ref"],
                        member["monitoring_status"],
                        member["authorization_status"],
                        member["monitoring_started_at"],
                        _json(member["metadata"]),
                    ),
                )
        candidate.update({"status": "accepted", "snapshot_id": snapshot_id,
                          "confirmation_candidate_id": confirmation_id, "accepted_diff": diff})
        connection.execute("UPDATE scheduler_runs SET status='succeeded',completed_at=?,details_json=? WHERE id=?",
                           (timestamp, _json(candidate), candidate_id))
        return {**diff, "status": "accepted", "snapshot_id": snapshot_id}


def _snapshot_result(row: sqlite3.Row | Mapping[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result["scope"] = json.loads(result.pop("scope_json"))
    result["metadata"] = json.loads(result.pop("metadata_json"))
    result.setdefault("source_family", MATRIX_SOURCE_FAMILY)
    return result


def snapshot_by_id(
    connection: sqlite3.Connection, snapshot_id: int
) -> dict[str, Any]:
    if type(snapshot_id) is not int or snapshot_id <= 0:
        raise RosterError("roster_evidence_mismatch", "Roster snapshot ID is invalid")
    row = connection.execute(
        "SELECT * FROM account_roster_snapshots WHERE id=?", (snapshot_id,)
    ).fetchone()
    if row is None:
        raise RosterError("roster_evidence_mismatch", "Roster snapshot does not exist")
    return _snapshot_result(row)


def latest_family_snapshot(
    connection: sqlite3.Connection,
    source_family: str,
    scope_key: str | None = None,
) -> dict[str, Any] | None:
    """Return the newest accepted snapshot within one administrative family."""
    if source_family not in SOURCE_FAMILIES:
        raise RosterError("invalid_source_family", "Unsupported roster source family")
    if not _supports_source_families(connection):
        if source_family != MATRIX_SOURCE_FAMILY:
            return None
        query = "SELECT * FROM account_roster_snapshots"
        args: tuple[Any, ...] = ()
    else:
        query = "SELECT * FROM account_roster_snapshots WHERE source_family=?"
        args = (source_family,)
    if scope_key:
        query += " AND scope_key=?" if " WHERE " in query else " WHERE scope_key=?"
        args += (scope_key,)
    row = connection.execute(query + " ORDER BY id DESC LIMIT 1", args).fetchone()
    return _snapshot_result(row) if row is not None else None


def runtime_snapshot(
    connection: sqlite3.Connection,
    activation: Mapping[str, Any] | sqlite3.Row | int,
) -> dict[str, Any]:
    """Resolve only the roster bound by an already selected activation."""
    if not _supports_source_families(connection):
        raise RosterError(
            "roster_activation_required", "Schema 18 has no activation ledger"
        )
    supplied = {} if isinstance(activation, int) else dict(activation)
    activation_id = (
        activation
        if isinstance(activation, int)
        else supplied.get("activation_id", supplied.get("id"))
    )
    if type(activation_id) is not int or activation_id <= 0:
        raise RosterError(
            "roster_activation_required", "A persisted acquisition activation is required"
        )
    from .profile_activations import ProfileActivationError, activation_by_id

    try:
        value = activation_by_id(connection, activation_id)
    except ProfileActivationError as error:
        raise RosterError("roster_activation_required", str(error)) from error
    if value.get("cancellation") is not None:
        raise RosterError(
            "roster_activation_cancelled", "Acquisition activation was cancelled"
        )
    for key in (
        "profile_id",
        "roster_snapshot_id",
        "roster_members_sha256",
        "activation_sha256",
    ):
        if key in supplied and supplied[key] != value[key]:
            raise RosterError(
                "roster_evidence_mismatch", "Supplied activation differs from the ledger"
            )
    snapshot_id = value.get("roster_snapshot_id")
    expected_hash = value.get("roster_members_sha256")
    legacy_hash = value.get("roster_snapshot_hash")
    if expected_hash is None:
        expected_hash = legacy_hash
    elif legacy_hash is not None and legacy_hash != expected_hash:
        raise RosterError(
            "roster_evidence_mismatch", "Activation roster hashes disagree"
        )
    if type(snapshot_id) is not int or not isinstance(expected_hash, str):
        raise RosterError(
            "roster_activation_required",
            "Activation must bind an exact roster snapshot and hash",
        )
    snapshot = snapshot_by_id(connection, snapshot_id)
    if snapshot["members_sha256"] != expected_hash:
        raise RosterError(
            "roster_evidence_mismatch", "Activation roster hash does not match"
        )
    profile_id = value.get("profile_id")
    if not isinstance(profile_id, str):
        raise RosterError(
            "roster_evidence_mismatch",
            "Activation profile and roster source family do not match",
        )
    expected_family = PROFILE_SOURCE_FAMILIES.get(profile_id)
    if expected_family is None or snapshot["source_family"] != expected_family:
        raise RosterError(
            "roster_evidence_mismatch",
            "Activation profile and roster source family do not match",
        )
    return {
        **snapshot,
        "runtime_activation_id": value["activation_id"],
        "runtime_profile_id": profile_id,
    }


def current_snapshot(
    connection: sqlite3.Connection, scope_key: str | None = None
) -> dict[str, Any] | None:
    """Legacy/admin Matrix view; runtime callers must use runtime_snapshot()."""
    return latest_family_snapshot(connection, MATRIX_SOURCE_FAMILY, scope_key)


def get_current_members(
    connection: sqlite3.Connection,
    snapshot_id: int | None = None,
    enabled_only: bool = False,
    *,
    source_family: str = MATRIX_SOURCE_FAMILY,
) -> list[dict[str, Any]]:
    if snapshot_id is None:
        snapshot = latest_family_snapshot(connection, source_family)
        if snapshot is None:
            return []
        snapshot_id = int(snapshot["id"])
    has_families = _supports_source_families(connection)
    rows = connection.execute(
        """SELECT m.*,i.account_id,"""
        + ("i.uid AS identity_uid," if has_families else "i.uid AS uid,")
        + """i.nickname,a.enabled
           FROM account_roster_members m
           JOIN account_platform_identities i ON i.id=m.account_identity_id
           JOIN accounts a ON a.id=i.account_id
           WHERE m.snapshot_id=?""" + (" AND a.enabled=1" if enabled_only else "") +
        (" ORDER BY m.member_key" if has_families else " ORDER BY m.platform,m.matrix_account_id"),
        (snapshot_id,),
    ).fetchall()
    result = []
    for row in rows:
        item = dict(row)
        item["identity_id"] = item["account_identity_id"]
        item["metadata"] = json.loads(item.pop("metadata_json"))
        item["nickname"] = item["metadata"].get("nickname") or item["nickname"]
        if has_families:
            identity_uid = item.pop("identity_uid")
            source_uid = item["uid"]
            if source_uid is not None and source_uid != identity_uid:
                raise RosterError(
                    "roster_evidence_mismatch",
                    "Roster member UID differs from its identity",
                )
            item["source_uid"] = source_uid
            item["uid"] = identity_uid
        result.append(item)
    return result


def require_active_member(
    connection: sqlite3.Connection, identity_id: int, snapshot_id: int | None = None,
    snapshot_hash: str | None = None, require_uid: bool = True,
    *,
    activation: Mapping[str, Any] | sqlite3.Row | int | None = None,
) -> dict[str, Any]:
    """Check at claim and immediately before each paid dispatch, not only in cron."""
    has_families = _supports_source_families(connection)
    if activation is not None:
        frozen = runtime_snapshot(connection, activation)
        if snapshot_id is not None and snapshot_id != frozen["id"]:
            raise RosterError(
                "roster_evidence_mismatch", "Frozen roster and activation disagree"
            )
    elif snapshot_id is not None:
        frozen = snapshot_by_id(connection, snapshot_id)
    elif has_families:
        raise RosterError(
            "roster_activation_required",
            "Paid work requires an activation-bound roster",
        )
    else:
        current = current_snapshot(connection)
        if current is None:
            raise RosterError("roster_not_ready", "No complete roster has been accepted")
        frozen = current
    if has_families and activation is None and snapshot_hash is None:
        raise RosterError(
            "roster_evidence_mismatch", "Frozen roster hash is required"
        )
    if snapshot_hash is not None and snapshot_hash != frozen["members_sha256"]:
        raise RosterError("roster_evidence_mismatch", "Frozen roster hash does not match")
    current = connection.execute(
        """SELECT m.*,i.uid,i.account_id,a.enabled FROM account_roster_members m
           JOIN account_platform_identities i ON i.id=m.account_identity_id
           JOIN accounts a ON a.id=i.account_id
           WHERE m.snapshot_id=? AND m.account_identity_id=?""", (frozen["id"], identity_id),
    ).fetchone()
    if current is None or not current["enabled"]:
        raise RosterError(
            "member_scope_changed",
            "Identity is not an enabled member of the active roster",
        )
    if require_uid and not current["uid"]:
        raise RosterError("identity_unresolved", "A verified platform UID is required for this paid operation")
    return {**dict(current), "roster_snapshot_id": frozen["id"],
            "roster_snapshot_hash": frozen["members_sha256"], "latest_roster_snapshot_id": frozen["id"]}


def _candidates(
    connection: sqlite3.Connection, *, source_family: str | None = None
) -> list[dict[str, Any]]:
    values = [
        {**json.loads(row["details_json"]), "candidate_id": row["id"]}
        for row in connection.execute(
            """SELECT id,details_json FROM scheduler_runs
               WHERE job_id IN (?,?) ORDER BY id DESC""",
            (CANDIDATE_JOB, SYSTEM_CANDIDATE_JOB),
        )
    ]
    if source_family is None:
        return values
    return [
        row
        for row in values
        if row["payload"].get(
            "source_family", _source_family(row["payload"]["source_type"])
        )
        == source_family
    ]


def _candidate(connection: sqlite3.Connection, candidate_id: int) -> dict[str, Any]:
    row = connection.execute(
        """SELECT details_json FROM scheduler_runs
           WHERE job_id IN (?,?) AND id=?""",
        (CANDIDATE_JOB, SYSTEM_CANDIDATE_JOB, candidate_id),
    ).fetchone()
    if row is None:
        raise RosterError("candidate_not_found", "Roster candidate does not exist")
    return {**json.loads(row["details_json"]), "candidate_id": candidate_id}


def candidate_diff(connection: sqlite3.Connection, candidate_id: int) -> dict[str, Any]:
    candidate = _candidate(connection, candidate_id)
    if candidate["status"] == "accepted" and candidate.get("accepted_diff"):
        return {**candidate["accepted_diff"], "status": "accepted", "snapshot_id": candidate["snapshot_id"]}
    payload = candidate["payload"]
    source_family = payload.get(
        "source_family", _source_family(payload["source_type"])
    )
    current = latest_family_snapshot(connection, source_family)
    if current is not None and current["scope_key"] != payload["scope_key"]:
        raise RosterError("scope_changed", "A different organization/scope cannot replace the current roster")
    old_members = (
        get_current_members(connection, snapshot_id=int(current["id"]))
        if current is not None
        else []
    )
    if source_family == MATRIX_SOURCE_FAMILY:
        old = {
            (row["platform"], row["matrix_account_id"]) for row in old_members
        }
        new = {
            (row["platform"], row["matrix_account_id"])
            for row in payload["members"]
        }
        added = [
            {"platform": platform, "matrix_account_id": stable_id}
            for platform, stable_id in sorted(new - old)
        ]
        removed = [
            {"platform": platform, "matrix_account_id": stable_id}
            for platform, stable_id in sorted(old - new)
        ]
    else:
        old = {(row["platform"], row["uid"]) for row in old_members}
        new = {(row["platform"], row["uid"]) for row in payload["members"]}
        added = [
            {
                "platform": platform,
                "uid": stable_id,
                "member_key": f"uid:{platform}:{stable_id}",
            }
            for platform, stable_id in sorted(new - old)
        ]
        removed = [
            {
                "platform": platform,
                "uid": stable_id,
                "member_key": f"uid:{platform}:{stable_id}",
            }
            for platform, stable_id in sorted(old - new)
        ]
    return {
        "candidate_id": candidate_id, "status": candidate["status"],
        "source_family": source_family,
        "source_type": payload["source_type"], "source_captured_at": payload["source_captured_at"],
        "members_sha256": payload["members_sha256"], "member_count": len(new),
        "added": added,
        "removed": removed,
        "current_snapshot_id": current["id"] if current else None,
        "snapshot_id": candidate.get("snapshot_id"),
    }


def account_summary(
    connection: sqlite3.Connection,
    *,
    source_family: str = MATRIX_SOURCE_FAMILY,
    snapshot_id: int | None = None,
) -> dict[str, Any]:
    snapshot = (
        snapshot_by_id(connection, snapshot_id)
        if snapshot_id is not None
        else latest_family_snapshot(connection, source_family)
    )
    if snapshot is not None and snapshot["source_family"] != source_family:
        raise RosterError(
            "roster_evidence_mismatch",
            "Requested roster snapshot belongs to another source family",
        )
    members = (
        get_current_members(connection, snapshot_id=int(snapshot["id"]))
        if snapshot is not None
        else []
    )
    candidates = _candidates(connection, source_family=source_family)
    pending = next((row for row in candidates if row["status"] == "pending_removal_confirmation"
                    and (snapshot is None or row["candidate_id"] > snapshot["metadata"].get("candidate_id", 0))), None)
    recent = pending or next((row for row in candidates if row["status"] == "accepted"), None)
    diff = candidate_diff(connection, recent["candidate_id"]) if recent else None
    system_managed = source_family == SYSTEM_SOURCE_FAMILY
    return {
        "ready": snapshot is not None, "snapshot": snapshot,
        "source_family": source_family,
        "snapshot_id": snapshot["id"] if snapshot else None,
        "source_type": snapshot["source_type"] if snapshot else None,
        "source_captured_at": snapshot["source_captured_at"] if snapshot else None,
        "accepted_at": snapshot["accepted_at"] if snapshot else None,
        "status": "current" if snapshot else "roster_not_ready",
        "sync_mode": "system_managed" if system_managed else "manual_export",
        "api_sync_status": "managed_locally" if system_managed else "manual_export_required",
        "current_count": len(members),
        "enabled_current_count": sum(bool(row["enabled"]) for row in members),
        "unresolved_count": sum(row["uid"] is None for row in members),
        "historical_identity_count": connection.execute("SELECT COUNT(*) FROM account_platform_identities").fetchone()[0] - len(members),
        "physical_account_count": connection.execute("SELECT COUNT(*) FROM accounts").fetchone()[0],
        "pending_removal_count": len(diff["removed"]) if diff and pending else 0,
        "message": (
            "Manage account membership in Dcar; accepted changes activate at the next scheduled roster boundary."
            if system_managed
            else "Upload a new complete official Matrix export; statistics account/list is not a roster."
        ),
        "diff": diff,
    }


def runtime_account_summary(
    connection: sqlite3.Connection, *, at: str | None = None
) -> dict[str, Any]:
    """Return the roster bound to the activation effective at the read instant."""

    if not _supports_source_families(connection):
        return {
            **account_summary(connection),
            "active_profile_id": "matrix_hybrid_v1",
            "activation_id": None,
            "pending_snapshot_id": None,
        }
    from .profile_activations import activation_at

    activation = activation_at(connection, at or now_utc())
    if activation is None:
        pending = account_summary(connection)
        return {
            **pending,
            "ready": False,
            "snapshot": None,
            "snapshot_id": None,
            "source_type": None,
            "source_captured_at": None,
            "accepted_at": None,
            "current_count": 0,
            "enabled_current_count": 0,
            "unresolved_count": 0,
            "status": "roster_activation_required",
            "active_profile_id": None,
            "activation_id": None,
            "pending_snapshot_id": pending.get("snapshot_id"),
        }
    snapshot = runtime_snapshot(connection, activation)
    source_family = PROFILE_SOURCE_FAMILIES[str(activation["profile_id"])]
    latest = latest_family_snapshot(connection, source_family)
    summary = account_summary(
        connection,
        source_family=source_family,
        snapshot_id=int(snapshot["id"]),
    )
    return {
        **summary,
        "active_profile_id": activation["profile_id"],
        "activation_id": activation["activation_id"],
        "pending_snapshot_id": (
            int(latest["id"])
            if latest is not None and int(latest["id"]) != int(snapshot["id"])
            else None
        ),
    }


def account_metadata(
    connection: sqlite3.Connection,
    account_id: int,
    *,
    source_family: str = MATRIX_SOURCE_FAMILY,
    snapshot_id: int | None = None,
    latest_if_unspecified: bool = True,
) -> dict[str, Any]:
    snapshot = (
        snapshot_by_id(connection, snapshot_id)
        if snapshot_id is not None
        else latest_family_snapshot(connection, source_family)
        if latest_if_unspecified
        else None
    )
    if snapshot is not None and snapshot["source_family"] != source_family:
        raise RosterError(
            "roster_evidence_mismatch",
            "Requested roster snapshot belongs to another source family",
        )
    identity = connection.execute("SELECT id,uid FROM account_platform_identities WHERE account_id=?", (account_id,)).fetchone()
    member = None
    if identity and snapshot:
        member = connection.execute("SELECT * FROM account_roster_members WHERE snapshot_id=? AND account_identity_id=?",
                                    (snapshot["id"], identity["id"])).fetchone()
    metadata = json.loads(member["metadata_json"]) if member else {}
    return {
        "roster_status": "current" if member else "historical",
        "identity_status": "unresolved" if identity and identity["uid"] is None else "resolved" if identity else "none",
        "roster_snapshot_id": snapshot["id"] if snapshot else None,
        "matrix_account_id": member["matrix_account_id"] if member else None,
        "profile_ref": member["profile_ref"] if member else None,
        "monitoring_status": member["monitoring_status"] if member else "unknown",
        "authorization_status": member["authorization_status"] if member else "unknown",
        "monitoring_started_at": member["monitoring_started_at"] if member else None,
        "nickname": metadata.get("nickname"),
        "avatar_url": metadata.get("avatar_url"), "unique_id": metadata.get("display_account_id"),
    }


def validate_bootstrap_extension(
    source_v17: sqlite3.Connection, candidate_v18: sqlite3.Connection,
) -> dict[str, Any]:
    """Prove only the additive accepted-bootstrap domain, before bare lineage.

    The caller must still prove every retained row and the 17->18 account split.
    Reference keys are composite triples, because that table has no integer ID.
    No supplied allowlist, manifest mapping or caller-supplied IDs are trusted.
    """
    def fail(message: str) -> NoReturn:
        raise RosterError("invalid_bootstrap_extension", message)

    def added(table: str) -> list[dict[str, Any]]:
        previous = {int(row["id"]) for row in source_v17.execute(f"SELECT id FROM {table}")}
        return [dict(row) for row in candidate_v18.execute(f"SELECT * FROM {table} ORDER BY id")
                if int(row["id"]) not in previous]

    snapshots = [dict(row) for row in candidate_v18.execute("SELECT * FROM account_roster_snapshots")]
    if len(snapshots) != 1 or snapshots[0]["source_type"] != "bootstrap_export":
        fail("Exactly one accepted bootstrap snapshot is required")
    if candidate_v18.execute("SELECT COUNT(*) FROM account_metric_observations").fetchone()[0] != 0:
        fail("Bootstrap cannot insert account metrics")
    new_raw, new_runs = added("provider_raw_responses"), added("scheduler_runs")
    if len(new_raw) != 1 or len(new_runs) != 1:
        fail("Bootstrap may append exactly one raw source and one candidate run")
    raw, run, snapshot = new_raw[0], new_runs[0], snapshots[0]
    if run["job_id"] != CANDIDATE_JOB or run["status"] != "succeeded":
        fail("Added run is not the successful bootstrap candidate")
    candidate = json.loads(run["details_json"])
    expected_detail_keys = {
        "status", "payload", "raw_response_id", "observed_at", "payload_sha256",
        "candidate_id", "snapshot_id", "confirmation_candidate_id", "accepted_diff",
    }
    if set(candidate) != expected_detail_keys:
        fail("Bootstrap candidate has an unexpected manifest shape")
    if (
        candidate["status"] != "accepted" or candidate["candidate_id"] != run["id"]
        or candidate["snapshot_id"] != snapshot["id"] or candidate["raw_response_id"] != raw["id"]
        or candidate["confirmation_candidate_id"] is not None
    ):
        fail("Candidate, raw and accepted snapshot are not consistently bound")
    _verify_raw(candidate_v18, candidate)
    payload = candidate["payload"]
    normalized = validate_payload(payload, source_bytes=Path(raw["local_path"]).read_bytes())
    if normalized != payload or not payload["require_existing_identities"] or payload["source_type"] != "bootstrap_export":
        fail("Bootstrap payload is not normalized, complete and existing-identity-only")
    proof = payload["source_evidence"]
    if proof.get("source_format") == "matrix-ui-verified-export-v1":
        # Independently rebuild the mapping from the original export and the
        # hashed Sheet2 evidence, never from the candidate's proposed ID list.
        from .account_roster_bootstrap import build_bootstrap_envelope
        evidence_path = Path(proof.get("uid_evidence_path", ""))
        workbook_path = Path(proof.get("official_workbook_path", ""))
        for path, digest in (
            (evidence_path, proof.get("uid_evidence_sha256")),
            (workbook_path, proof.get("official_workbook_sha256")),
        ):
            if not path.is_file() or path.is_symlink() or _sha(path.read_bytes()) != digest:
                fail("Bootstrap external export/UID evidence is missing or changed")
        rebuilt = build_bootstrap_envelope(Path(raw["local_path"]), evidence_path, source_v17)["payload"]
        rebuilt["source_evidence"]["source_name"] = proof["source_name"]
        if validate_payload(rebuilt, source_bytes=Path(raw["local_path"]).read_bytes()) != payload:
            fail("Bootstrap members or attributes differ from the independent raw-to-UID reconstruction")
    if (
        raw["provider"] != "newrank_matrix" or raw["operation"] != CANDIDATE_JOB
        or raw["source"] != "official_roster"
        or any(raw[key] is not None for key in ("fetch_attempt_id", "account_id", "content_id", "http_status"))
        or raw["captured_at"] != candidate["observed_at"]
        or run["started_at"] != candidate["observed_at"]
        or run["completed_at"] != snapshot["accepted_at"]
        or run["scheduled_for"] != payload["source_instance_id"]
        or _time(run["completed_at"]) < _time(run["started_at"])
    ):
        fail("Bootstrap raw/run fields or timestamps are outside the allowed footprint")
    expected_snapshot = {
        "id": snapshot["id"], "source_type": "bootstrap_export", "scope_key": payload["scope_key"],
        "scope_json": _json(payload["scope"]), "source_instance_id": payload["source_instance_id"],
        "source_captured_at": payload["source_captured_at"], "accepted_at": run["completed_at"],
        "declared_count": len(payload["members"]), "member_count": len(payload["members"]),
        "members_sha256": payload["members_sha256"], "source_sha256": payload["source_sha256"],
        "source_path": raw["local_path"], "contract_version": CONTRACT_VERSION,
        "metadata_json": _json({
            "raw_response_id": raw["id"], "source_evidence": payload["source_evidence"],
            "candidate_id": run["id"], "confirmation_candidate_id": None,
            "manifest_sha256": candidate["payload_sha256"],
        }),
    }
    if _supports_source_families(candidate_v18):
        expected_snapshot["source_family"] = MATRIX_SOURCE_FAMILY
    if snapshot != expected_snapshot:
        fail("Accepted snapshot fields do not match the immutable source and run")
    expected_diff = {
        "candidate_id": run["id"], "status": "candidate",
        "source_family": MATRIX_SOURCE_FAMILY,
        "source_type": "bootstrap_export",
        "source_captured_at": payload["source_captured_at"], "members_sha256": payload["members_sha256"],
        "member_count": len(payload["members"]),
        "added": [{"platform": row["platform"], "matrix_account_id": row["matrix_account_id"]} for row in payload["members"]],
        "removed": [], "current_snapshot_id": None, "snapshot_id": None,
    }
    if candidate["accepted_diff"] != expected_diff:
        fail("Bootstrap receipt does not contain the complete frozen initial difference")
    source_identities = {(row["platform"], row["uid"]): dict(row)
                         for row in source_v17.execute("SELECT * FROM account_platform_identities")}
    expected_members, expected_references = {}, {}
    source_refs = {
        (row["account_identity_id"], row["provider"], row["reference_kind"]): dict(row)
        for row in source_v17.execute("SELECT * FROM account_provider_references")
    }
    for member in payload["members"]:
        original = source_identities.get((member["platform"], member["uid"]))
        if original is None or not member["uid"] or original["id"] in expected_members:
            fail("Bootstrap does not map one-to-one to already verified source identities")
        identity_id = original["id"]
        present = candidate_v18.execute("SELECT * FROM account_platform_identities WHERE id=?", (identity_id,)).fetchone()
        if present is None or present["uid"] != original["uid"] or present["platform"] != original["platform"]:
            fail("A bootstrap identity was replaced or changed")
        expected_members[identity_id] = {
            "snapshot_id": snapshot["id"], "account_identity_id": identity_id,
            **{key: member[key] for key in (
                "platform", "matrix_account_id", "profile_ref", "monitoring_status",
                "authorization_status", "monitoring_started_at",
            )},
            "metadata_json": _json(member["metadata"]),
        }
        if _supports_source_families(candidate_v18):
            expected_members[identity_id].update(
                member_key=member["member_key"], uid=member["uid"]
            )
        for kind, value in (("matrix_account_id", member["matrix_account_id"]), ("profile_url", member["profile_ref"])):
            key = identity_id, "newrank_matrix", kind
            if key in source_refs:
                if source_refs[key]["reference_value"] != value:
                    fail("Bootstrap changed an existing Matrix reference")
            else:
                expected_references[key] = {
                    "account_identity_id": identity_id, "provider": "newrank_matrix",
                    "reference_kind": kind, "reference_value": value,
                    "source_raw_response_id": raw["id"], "created_at": run["completed_at"],
                    "updated_at": run["completed_at"],
                }
    actual_members = {row["account_identity_id"]: dict(row)
                      for row in candidate_v18.execute("SELECT * FROM account_roster_members")}
    if actual_members != expected_members:
        fail("Accepted members differ from the source-backed existing identity mapping")
    candidate_refs = {
        (row["account_identity_id"], row["provider"], row["reference_kind"]): dict(row)
        for row in candidate_v18.execute("SELECT * FROM account_provider_references")
    }
    if any(candidate_refs.get(key) != row for key, row in source_refs.items()):
        fail("Bootstrap changed or removed a retained provider reference")
    actual_added_refs = {key: row for key, row in candidate_refs.items() if key not in source_refs}
    if actual_added_refs != expected_references:
        fail("Bootstrap appended unexpected reference values, fields or timestamps")
    source_sequences = dict(source_v17.execute("SELECT name,seq FROM sqlite_sequence"))
    candidate_sequences = dict(candidate_v18.execute("SELECT name,seq FROM sqlite_sequence"))
    expected_sequences = {
        "provider_raw_responses": max(source_sequences.get("provider_raw_responses", 0),
                                      source_v17.execute("SELECT COALESCE(MAX(id),0) FROM provider_raw_responses").fetchone()[0]) + 1,
        "scheduler_runs": max(source_sequences.get("scheduler_runs", 0),
                              source_v17.execute("SELECT COALESCE(MAX(id),0) FROM scheduler_runs").fetchone()[0]) + 1,
        "account_roster_snapshots": 1,
    }
    if any(candidate_sequences.get(name) != expected for name, expected in expected_sequences.items()):
        fail("Bootstrap advanced an append-only sequence unexpectedly")
    if raw["id"] != expected_sequences["provider_raw_responses"] or run["id"] != expected_sequences["scheduler_runs"] or snapshot["id"] != 1:
        fail("Bootstrap row IDs are not the exact next append-only IDs")
    return {
        "added_rows": {
            "provider_raw_responses": [raw["id"]], "scheduler_runs": [run["id"]],
            "account_provider_references": [list(key) for key in sorted(expected_references)],
            "account_roster_snapshots": [snapshot["id"]],
        },
        "snapshot_id": snapshot["id"], "member_count": snapshot["member_count"],
        "members_sha256": snapshot["members_sha256"], "source_sha256": snapshot["source_sha256"],
        "sqlite_sequence": expected_sequences,
    }
