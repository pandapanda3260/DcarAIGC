"""Derive automatic capture eligibility from the account directory, read only.

The directory is the business authority. Old ``accounts.enabled`` values and
roster membership are deliberately not inputs. An installed release must still
bind the returned members to its immutable execution snapshot; this module does
not authorize a provider request, create a route or change a paid-send gate.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections import Counter, defaultdict
from typing import Any, Mapping


CONTRACT = "account-directory-capture-eligibility-v1"
SUPPORTED_PLATFORMS = frozenset({"douyin", "xiaohongshu"})
MEMBER_KEYS = ("account_identity_id", "account_id", "platform", "uid")
_SEC = re.compile(r"MS4wLjAB[A-Za-z0-9_-]{32,120}")
_PROFILE_OPERATIONS = frozenset({"douyin_uid_profile", "douyin_user_profile", "douyin_user_reference"})
REASON_LABELS = {
    "eligible": "可自动采集",
    "account_paused": "账号已暂停",
    "account_status_unmarked": "账号状态待标记",
    "account_status_invalid": "账号状态无效",
    "platform_unsupported": "暂不支持此平台采集",
    "identity_missing": "平台身份待完善",
    "identity_unverified": "平台身份待核验",
    "identity_conflict": "账号与平台身份不一致",
    "directory_conflict": "账号目录存在重复身份",
    "reference_missing": "缺少可用的账号定位信息",
    "reference_invalid": "账号定位信息格式无效",
    "reference_conflict": "账号定位信息对应多个身份",
    "reference_evidence_missing": "缺少账号定位依据",
    "reference_evidence_unavailable": "账号定位依据不可读取或校验未通过",
    "reference_identity_mismatch": "账号定位依据与平台身份不一致",
    "directory_required": "账号目录尚未就绪",
    "admission_evidence_invalid": "账号准入凭据校验未通过",
}


class DirectoryCaptureEligibilityError(ValueError):
    def __init__(self, code: str):
        self.code = self.error_code = code
        self.label = REASON_LABELS[code]
        super().__init__(self.label)


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _result(member: Mapping[str, Any], reason: str, **extra: Any) -> dict[str, Any]:
    return {**member, "eligible": reason == "eligible", "reason_code": reason,
            "reason_label": REASON_LABELS[reason], **extra}


def _objects(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _objects(child)
    elif isinstance(value, list):
        for child in value:
            yield from _objects(child)


def _prove_reference(connection: sqlite3.Connection, member: Mapping[str, Any],
                     reference: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
    response_id = reference["source_raw_response_id"]
    if type(response_id) is not int or response_id <= 0:
        return "reference_evidence_missing", {}
    row = connection.execute("SELECT * FROM provider_raw_responses WHERE id=?", (response_id,)).fetchone()
    if row is None:
        return "reference_evidence_missing", {}
    raw = dict(row)
    if (str(raw.get("provider", "")).lower() != "tikhub"
            or raw.get("operation") not in _PROFILE_OPERATIONS
            or raw.get("account_id") != member["account_id"]
            or raw.get("http_status") != 200):
        return "reference_identity_mismatch", {}
    try:
        from . import raw_archive, raw_evidence
        from .providers import _tikhub_douyin_data

        # This existing reader verifies size/hash and archived-blob identity,
        # never hydrates a file, writes a receipt or contacts a provider.
        try:
            entity = raw_archive.read_response_entity(connection, response_id)
        except raw_archive.RawArchiveError as error:
            # Some migrated profile responses retain their original JSON while
            # their compressed blob's old local path is absent. Use only that
            # exact stored response, with its original size/hash verified. A
            # corrupt or intentionally retired blob is not a fallback signal.
            if str(error) != "raw path is missing" or raw.get("raw_blob_id") is None:
                raise
            blob = connection.execute("SELECT hot_state FROM provider_raw_blobs WHERE id=?", (raw["raw_blob_id"],)).fetchone()
            if blob is None or blob["hot_state"] != "present":
                raise
            entity = raw_evidence.read_raw_evidence(raw_archive._source_path(raw["local_path"]),
                expected_stored_sha256=raw["sha256"], expected_stored_size=raw["byte_size"]).entity_bytes
        payload = json.loads(entity)
        data = (_tikhub_douyin_data(payload) if isinstance(payload, dict) and "code" in payload
                else payload.get("data") if isinstance(payload, dict) else None)
        if not isinstance(data, dict) or data.get("status_code") not in (None, 0, "0"):
            return "reference_identity_mismatch", {}
        sec = reference["reference_value"]
        found = False
        for value in _objects(data):
            references = {value[key] for key in ("sec_user_id", "sec_uid")
                          if isinstance(value.get(key), str) and value[key]}
            if sec not in references:
                continue
            identifiers = {value[key] for key in ("uid", "id_str", "user_id")
                           if isinstance(value.get(key), str) and value[key]}
            # UID and locator must identify the same object, not two unrelated
            # objects somewhere inside a successful provider response.
            if identifiers and (identifiers != {member["uid"]} or references != {sec}):
                return "reference_identity_mismatch", {}
            found |= identifiers == {member["uid"]} and references == {sec}
        if not found:
            return "reference_identity_mismatch", {}
    except Exception:
        # Bad/missing local evidence excludes this member; it never becomes a
        # paid reference lookup or a permissive fallback to the old roster.
        return "reference_evidence_unavailable", {}
    return "eligible", {"locator_kind": "sec_user_id",
        "locator_sha256": hashlib.sha256(sec.encode()).hexdigest(),
        "locator_evidence": {"kind": "provider_profile_raw", "raw_response_id": response_id,
                             "raw_sha256": raw["sha256"],
                             "entity_sha256": hashlib.sha256(entity).hexdigest()}}


def _derive(connection: sqlite3.Connection, *, identity_id: int | None = None) -> dict[str, Any]:
    tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if not {"account_directory_rows", "accounts", "account_platform_identities",
            "account_provider_references", "provider_raw_responses"} <= tables:
        raise DirectoryCaptureEligibilityError("directory_required")
    directories = [dict(row) for row in connection.execute("SELECT * FROM account_directory_rows ORDER BY id")]
    identities: dict[int, list[dict[str, Any]]] = defaultdict(list)
    identity_rows = [dict(row) for row in connection.execute("SELECT * FROM account_platform_identities ORDER BY id")]
    for row in identity_rows:
        identities[row["account_id"]].append(row)
    accounts = {row["id"]: dict(row) for row in connection.execute("SELECT * FROM accounts")}
    refs: dict[int, list[dict[str, Any]]] = defaultdict(list)
    reference_owners: dict[str, set[int]] = defaultdict(set)
    for row in connection.execute("SELECT * FROM account_provider_references WHERE lower(provider)='tikhub' AND reference_kind='sec_user_id'"):
        refs[row["account_identity_id"]].append(dict(row))
        reference_owners[row["reference_value"]].add(row["account_identity_id"])
    admissions: dict[int, dict[str, Any]] = {}
    if {"scheduler_runs", "scheduler_run_attempts"} <= tables:
        from .account_operating_receipts import AccountOperatingStatusError, load_admission_members
        try:
            admissions = load_admission_members(connection)
        except AccountOperatingStatusError as error:
            raise DirectoryCaptureEligibilityError("admission_evidence_invalid") from error
        for admitted_id, saved in admissions.items():
            sec = saved["member"].get("metadata", {}).get("sec_user_id")
            if isinstance(sec, str) and sec:
                reference_owners[sec].add(admitted_id)
    directory_keys = Counter((row["platform"], row.get("uid")) for row in directories if row.get("uid"))
    directory_accounts = Counter(row["account_id"] for row in directories if row.get("account_id") is not None)
    identity_keys = Counter((row["platform"], row.get("uid")) for row in identity_rows if row.get("uid"))
    eligible, excluded = [], []
    for entry in directories:
        account = accounts.get(entry.get("account_id"))
        candidates = identities.get(entry.get("account_id"), [])
        identity = candidates[0] if len(candidates) == 1 else None
        if identity_id is not None and not any(row["id"] == identity_id for row in candidates):
            continue
        member = {"directory_row_id": entry["id"], "account_id": entry.get("account_id"),
                  "identity_id": identity["id"] if identity else None,
                  "account_identity_id": identity["id"] if identity else None,
                  "platform": entry["platform"], "uid": entry.get("uid"),
                  "identity_status": entry["identity_status"],
                  "account_status": entry["account_status"], "monitoring_status": "monitored",
                  "created_at": account.get("created_at") if account else None,
                  "accepted_at": entry.get("imported_at")}
        status = entry["account_status"]
        if status == "paused":
            reason = "account_paused"
        elif status == "unmarked":
            reason = "account_status_unmarked"
        elif status not in {"daily", "weekly"}:
            reason = "account_status_invalid"
        elif entry["platform"] not in SUPPORTED_PLATFORMS:
            reason = "platform_unsupported"
        elif entry["identity_status"] == "identity_missing" or account is None:
            reason = "identity_missing"
        elif entry["identity_status"] != "existing_verified":
            reason = "identity_unverified"
        elif (directory_keys[(entry["platform"], entry.get("uid"))] != 1
              or directory_accounts[entry["account_id"]] != 1):
            reason = "directory_conflict"
        elif (identity is None or identity["platform"] != entry["platform"]
              or identity["uid"] != entry.get("uid")
              or identity_keys[(identity["platform"], identity["uid"])] != 1
              or not isinstance(identity["uid"], str)
              or re.fullmatch(r"[0-9]{6,24}" if identity["platform"] == "douyin" else r"[0-9a-fA-F]{24}", identity["uid"]) is None):
            reason = "identity_conflict"
        else:
            if entry["platform"] == "xiaohongshu":
                reason, proof = "eligible", {"locator_kind": "uid",
                    "locator_sha256": hashlib.sha256(identity["uid"].encode()).hexdigest(),
                    "locator_evidence": {"kind": "verified_directory_identity", "directory_row_id": entry["id"]}}
            else:
                references = refs[identity["id"]]
                values = {row["reference_value"] for row in references}
                admitted = admissions.get(identity["id"])
                admitted_member = admitted["member"] if admitted else {}
                admitted_sec = admitted_member.get("metadata", {}).get("sec_user_id")
                if isinstance(admitted_sec, str) and admitted_sec:
                    values.add(admitted_sec)
                if not references:
                    reason, proof = "reference_missing", {}
                elif len(values) != 1 or any(len(reference_owners[value]) != 1 for value in values):
                    reason, proof = "reference_conflict", {}
                elif not isinstance(references[0]["reference_value"], str) or not _SEC.fullmatch(references[0]["reference_value"]):
                    reason, proof = "reference_invalid", {}
                else:
                    # Legacy casing duplicates with the same value are safe
                    # only when at least one retains verifiable provenance.
                    reason, proof = "reference_evidence_missing", {}
                    for reference in sorted(references, key=lambda row: row["source_raw_response_id"] or 0, reverse=True):
                        reason, proof = _prove_reference(connection, member, reference)
                        if reason == "eligible":
                            break
                if reason in {"reference_missing", "reference_evidence_missing"} and admitted_sec:
                    if (len(values) != 1 or reference_owners[admitted_sec] != {identity["id"]}
                            or admitted["account_id"] != member["account_id"]
                            or admitted_member.get("platform") != member["platform"]
                            or admitted_member.get("uid") != member["uid"]):
                        reason, proof = "reference_conflict", {}
                    elif not isinstance(admitted_sec, str) or not _SEC.fullmatch(admitted_sec):
                        reason, proof = "reference_invalid", {}
                    else:
                        reason, proof = "eligible", {"locator_kind": "sec_user_id",
                            "locator_sha256": hashlib.sha256(admitted_sec.encode()).hexdigest(),
                            "locator_evidence": {"kind": "verified_account_admission",
                                                 "admission_sha256": _digest(admitted)}}
            if reason == "eligible":
                eligible.append(_result(member, reason, enabled=True, **proof))
                continue
        excluded.append(_result(member, reason, enabled=False))
    eligible.sort(key=lambda row: row["account_identity_id"])
    selection = [{**{key: row[key] for key in MEMBER_KEYS}, "locator_sha256": row["locator_sha256"]}
                 for row in eligible]
    return {"contract": CONTRACT, "eligible_members": eligible, "excluded_members": excluded,
            "selection_sha256": _digest(selection)}


def derive_capture_eligibility(connection: sqlite3.Connection) -> dict[str, Any]:
    """Account for every directory row without writes, network I/O or fallback."""
    return _derive(connection)


def require_directory_capture_member(connection: sqlite3.Connection, identity_id: int) -> dict[str, Any]:
    """Recheck one automatic target; manual content commands must not call this."""
    if type(identity_id) is not int or identity_id <= 0:
        raise DirectoryCaptureEligibilityError("identity_missing")
    result = _derive(connection, identity_id=identity_id)
    if len(result["eligible_members"]) == 1 and not result["excluded_members"]:
        return result["eligible_members"][0]
    reason = result["excluded_members"][0]["reason_code"] if result["excluded_members"] else "identity_missing"
    raise DirectoryCaptureEligibilityError(reason)
