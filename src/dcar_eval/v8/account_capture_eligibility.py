"""Derive automatic capture eligibility from the account directory, read only.

The directory defines the account list. Operating labels, old
``accounts.enabled`` values and roster membership are deliberately not inputs.
An installed release must still bind the returned members to its immutable
execution snapshot; this module does not authorize a provider request, create
a route or change a paid-send gate.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections import Counter, defaultdict
from typing import Any, Mapping

from .platform_adapters import valid_uid


CONTRACT = "account-directory-capture-eligibility-v1"
SUPPORTED_PLATFORMS = frozenset({"douyin", "xiaohongshu", "kuaishou", "wechat_channels"})
MEMBER_KEYS = ("account_identity_id", "account_id", "platform", "uid")
_SEC = re.compile(r"MS4wLjAB[A-Za-z0-9_-]{32,120}")
_PROFILE_OPERATIONS = frozenset({"douyin_uid_profile", "douyin_user_profile", "douyin_user_reference", "douyin_sec_profile", "douyin_display_profile"})
_XHS_PROFILE_ROUTE = "/api/v1/xiaohongshu/app_v2/get_user_info"
REASON_LABELS = {
    "eligible": "可自动采集",
    "account_paused": "账号已暂停",
    "account_status_unmarked": "账号状态待标记",
    "account_status_invalid": "账号状态无效",
    "platform_unsupported": "暂不支持此平台采集",
    "identity_missing": "缺少平台 UID 或账号主体",
    # Kept for historical plan receipts; new derivations report the missing
    # evidence itself instead of waiting for someone to flip an import label.
    "identity_unverified": "缺少可验证的主页资料",
    "identity_evidence_missing": "缺少可验证的主页资料",
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


def _read_profile_entity(connection: sqlite3.Connection, raw: Mapping[str, Any]) -> bytes:
    """Read the exact saved entity through the existing size/hash archive checks."""
    from . import raw_archive, raw_evidence

    try:
        return raw_archive.read_response_entity(connection, raw["id"])
    except raw_archive.RawArchiveError as error:
        # Migrated responses can retain the original JSON after an old blob
        # path disappears. Never fall back for corrupt or retired evidence.
        if str(error) != "raw path is missing" or raw.get("raw_blob_id") is None:
            raise
        blob = connection.execute("SELECT hot_state FROM provider_raw_blobs WHERE id=?", (raw["raw_blob_id"],)).fetchone()
        if blob is None or blob["hot_state"] != "present":
            raise
        return raw_evidence.read_raw_evidence(raw_archive._source_path(raw["local_path"]),
            expected_stored_sha256=raw["sha256"], expected_stored_size=raw["byte_size"]).entity_bytes


def _prove_reference(connection: sqlite3.Connection, member: Mapping[str, Any],
                     reference: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
    response_id = reference["source_raw_response_id"]
    if type(response_id) is not int or response_id <= 0:
        return "reference_evidence_missing", {}
    row = connection.execute("SELECT * FROM provider_raw_responses WHERE id=?", (response_id,)).fetchone()
    if row is None:
        return "reference_evidence_missing", {}
    raw = dict(row)
    if raw.get("intake_request_id") is not None:
        return _prove_intake_reference(connection, member, reference, raw)
    if (str(raw.get("provider", "")).lower() != "tikhub"
            or raw.get("operation") not in _PROFILE_OPERATIONS
            or not _raw_bound_to_member(connection, raw, member)
            or raw.get("http_status") != 200):
        return "reference_identity_mismatch", {}
    try:
        from .providers import _tikhub_douyin_data

        entity = _read_profile_entity(connection, raw)
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


def _prove_xhs_reference(connection: sqlite3.Connection, member: Mapping[str, Any],
                         reference: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
    """Require a successful full profile entity with exact request/response UID."""
    response_id = reference["source_raw_response_id"]
    if type(response_id) is not int or response_id <= 0:
        return "reference_evidence_missing", {}
    row = connection.execute("SELECT * FROM provider_raw_responses WHERE id=?", (response_id,)).fetchone()
    if row is None:
        return "reference_evidence_missing", {}
    raw = dict(row)
    if raw.get("intake_request_id") is not None:
        return _prove_intake_reference(connection, member, reference, raw)
    if (str(raw.get("provider", "")).lower() != "tikhub"
            or raw.get("operation") != "xiaohongshu_user_profile"
            or not _raw_bound_to_member(connection, raw, member)
            or raw.get("http_status") != 200
            or reference["reference_value"] != member["uid"]):
        return "reference_identity_mismatch", {}
    try:
        entity = _read_profile_entity(connection, raw)
        payload = json.loads(entity)
    except Exception:
        return "reference_evidence_unavailable", {}
    if not isinstance(payload, dict):
        return "reference_identity_mismatch", {}
    params, data = payload.get("params"), payload.get("data")
    if (type(payload.get("code")) is not int or payload["code"] != 200
            or payload.get("router") != _XHS_PROFILE_ROUTE
            or not isinstance(params, dict) or params.get("user_id") != member["uid"]
            or not isinstance(data, dict) or data.get("success") is not True
            or type(data.get("code")) is not int or data["code"] != 0):
        return "reference_identity_mismatch", {}
    profile = data.get("data")
    if not isinstance(profile, dict):
        return "reference_identity_mismatch", {}
    result = profile.get("result")
    if (profile.get("userid") != member["uid"]
            or any(profile[key] != member["uid"] for key in ("uid", "user_id", "id_str")
                   if profile.get(key) not in (None, ""))
            or not isinstance(profile.get("nickname"), str) or not profile["nickname"].strip()
            or not isinstance(result, dict) or result.get("success") is not True
            or type(result.get("code")) is not int or result["code"] != 0):
        return "reference_identity_mismatch", {}
    return "eligible", {"locator_kind": "uid",
        "locator_sha256": hashlib.sha256(member["uid"].encode()).hexdigest(),
        "locator_evidence": {"kind": "provider_profile_raw", "raw_response_id": response_id,
                             "raw_sha256": raw["sha256"],
                             "entity_sha256": hashlib.sha256(entity).hexdigest()}}


def _raw_bound_to_member(connection: sqlite3.Connection, raw: Mapping[str, Any], member: Mapping[str, Any]) -> bool:
    if raw.get("intake_request_id") is None and raw.get("account_id") == member["account_id"]:
        return True
    request_id = raw.get("intake_request_id")
    if raw.get("account_id") is not None or request_id is None:
        return False
    row = connection.execute("SELECT * FROM account_intake_requests WHERE id=?", (request_id,)).fetchone()
    if row is None:
        return False
    intake = dict(row)
    try:
        value = json.loads(intake["input_json"])
        result = json.loads(intake["result_json"])
        return (intake.get("account_id") == member["account_id"] and intake.get("account_identity_id") == member["account_identity_id"]
            and intake.get("platform") == member["platform"] and value.get("platform") == member["platform"]
            and _digest(value) == intake["input_sha256"] and intake.get("completed_at") is not None
            and result.get("status") == "ready" and result.get("uid") == member["uid"]
            and raw["id"] in result.get("source_raw_response_ids", []))
    except (ValueError, TypeError, KeyError):
        return False


def _prove_intake_reference(connection: sqlite3.Connection, member: Mapping[str, Any], reference: Mapping[str, Any],
                            raw: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
    if not _raw_bound_to_member(connection, raw, member):
        return "reference_identity_mismatch", {}
    try:
        from .platform_adapters import next_profile_request, normalize_profile
        intake = dict(connection.execute("SELECT * FROM account_intake_requests WHERE id=?", (raw["intake_request_id"],)).fetchone())
        value, result = json.loads(intake["input_json"]), json.loads(intake["result_json"])
        ids = result["profile"]["source_raw_response_ids"]
        if (not isinstance(ids, list) or not ids or len(ids) != len(set(ids))
                or set(ids) != set(result["source_raw_response_ids"])):
            return "reference_identity_mismatch", {}
        responses, sources = [], []
        target = next_profile_request(value)
        for response_id in ids:
            saved = dict(connection.execute("SELECT * FROM provider_raw_responses WHERE id=?", (response_id,)).fetchone())
            if (target is None or saved["intake_request_id"] != intake["id"] or not _raw_bound_to_member(connection, saved, member)
                    or str(saved.get("provider", "")).lower() != "tikhub" or saved.get("http_status") != 200
                    or saved["operation"] != target["operation"]):
                return "reference_identity_mismatch", {}
            entity = _read_profile_entity(connection, saved)
            payload = json.loads(entity)
            if (payload.get("router") != target["path"] or not isinstance(payload.get("params"), dict)
                    or any(payload["params"].get(key) != val for key, val in target["params"].items())):
                return "reference_identity_mismatch", {}
            responses.append({"operation": saved["operation"], "raw_response_id": saved["id"], "payload": payload})
            sources.append({"raw_response_id": saved["id"], "raw_sha256": saved["sha256"],
                            "entity_sha256": hashlib.sha256(entity).hexdigest()})
            target = next_profile_request(value, responses)
        if target is not None:
            return "reference_evidence_missing", {}
        profile = normalize_profile(member["platform"], value, responses[-1]["payload"], prior_responses=responses)
        kind = reference["reference_kind"]
        if (profile["uid"] != member["uid"] or profile["references"].get(kind) != reference["reference_value"]
                or profile["reference_raw_response_ids"].get(kind) != raw["id"]):
            return "reference_identity_mismatch", {}
    except Exception:
        return "reference_evidence_unavailable", {}
    locator_kind = "sec_user_id" if member["platform"] == "douyin" else "uid"
    locator = profile["references"]["sec_user_id"] if member["platform"] == "douyin" else member["uid"]
    return "eligible", {"locator_kind": locator_kind, "locator_sha256": hashlib.sha256(locator.encode()).hexdigest(),
        "locator_evidence": {"kind": "prepared_profile_chain", "intake_request_id": intake["id"],
                             "input_sha256": intake["input_sha256"], "sources": sources}}


def _prove_prepared_reference(connection: sqlite3.Connection, member: Mapping[str, Any],
                              reference: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
    """Revalidate canonical profile entities; intake's success flag is no proof."""
    response_id = reference["source_raw_response_id"]
    row = connection.execute("SELECT * FROM provider_raw_responses WHERE id=?", (response_id,)).fetchone()
    if row is None:
        return "reference_evidence_missing", {}
    raw = dict(row)
    if raw.get("intake_request_id") is not None:
        return _prove_intake_reference(connection, member, reference, raw)
    if (str(raw.get("provider", "")).lower() != "tikhub" or raw.get("http_status") != 200
            or raw.get("operation") != member["platform"] + "_user_profile"
            or not _raw_bound_to_member(connection, raw, member)
            or reference["reference_value"] != member["uid"]):
        return "reference_identity_mismatch", {}
    try:
        entity = _read_profile_entity(connection, raw)
        payload = json.loads(entity)
    except Exception:
        return "reference_evidence_unavailable", {}
    try:
        from .platform_adapters import normalize_profile, response_data, ROUTES
        if member["platform"] == "wechat_channels":
            data = response_data(payload, "wechat_channels")
            contact = data.get("contact")
            if (payload.get("router") != ROUTES["wechat_channels_user_profile"][1]
                    or (payload.get("params") or {}).get("username") != member["uid"]
                    or not isinstance(contact, dict) or contact.get("username") != member["uid"]
                    or not isinstance(contact.get("nickname"), str) or not contact["nickname"].strip()
                    or not isinstance(data.get("baseResponse"), dict)):
                return "reference_identity_mismatch", {}
        else:
            profile = normalize_profile(member["platform"], {"uid": member["uid"]}, payload)
            if profile["uid"] != member["uid"]:
                return "reference_identity_mismatch", {}
    except Exception:
        return "reference_identity_mismatch", {}
    return "eligible", {"locator_kind": "uid", "locator_sha256": hashlib.sha256(member["uid"].encode()).hexdigest(),
        "locator_evidence": {"kind": "provider_profile_raw", "raw_response_id": response_id,
                             "raw_sha256": raw["sha256"], "entity_sha256": hashlib.sha256(entity).hexdigest()}}


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
    xhs_refs: dict[int, list[dict[str, Any]]] = defaultdict(list)
    xhs_reference_owners: dict[str, set[int]] = defaultdict(set)
    for row in connection.execute("SELECT r.* FROM account_provider_references r "
            "LEFT JOIN account_platform_identities i ON i.id=r.account_identity_id "
            "WHERE lower(r.provider)='tikhub' AND r.reference_kind='user_id' "
            "AND (i.platform='xiaohongshu' OR i.id IS NULL)"):
        xhs_refs[row["account_identity_id"]].append(dict(row))
        xhs_reference_owners[row["reference_value"]].add(row["account_identity_id"])
    prepared_refs: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in connection.execute("SELECT r.* FROM account_provider_references r JOIN account_platform_identities i "
            "ON i.id=r.account_identity_id WHERE lower(r.provider)='tikhub' AND "
            "((i.platform='kuaishou' AND r.reference_kind IN ('user_id','kuaishou_user_id')) OR (i.platform='wechat_channels' AND r.reference_kind IN ('username','finder_username')))"):
        prepared_refs[row["account_identity_id"]].append(dict(row))
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
    prepared_xhs = set()
    if "account_intake_requests" in tables and connection.execute("PRAGMA user_version").fetchone()[0] in {23, 24}:
        # A materialized directory status cannot weaken a preparation's source
        # contract. Preserve this requirement even when its raw or reference is
        # later missing; only legacy identities without preparation keep their
        # historical directory/admission-only proof.
        prepared_xhs = {row[0] for row in connection.execute("""SELECT DISTINCT i.id
            FROM account_intake_requests r JOIN account_platform_identities i
              ON i.id=r.account_identity_id AND i.account_id=r.account_id AND i.platform=r.platform
            JOIN account_directory_rows d ON d.id=r.directory_row_id AND d.account_id=i.account_id
              AND d.platform=i.platform AND d.uid=i.uid
            WHERE r.platform='xiaohongshu' AND r.completed_at IS NOT NULL
              AND (json_extract(r.result_json,'$.action')='prepared'
                OR (json_extract(r.result_json,'$.action')='existing_profile_reused'
                  AND json_extract(r.result_json,'$.existing_profile_evidence.kind')
                    IN ('provider_profile_raw','prepared_profile_chain')))""")}
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
        if entry["platform"] not in SUPPORTED_PLATFORMS:
            reason = "platform_unsupported"
        elif account is None or not entry.get("uid"):
            reason = "identity_missing"
        elif (directory_keys[(entry["platform"], entry.get("uid"))] != 1
              or directory_accounts[entry["account_id"]] != 1):
            reason = "directory_conflict"
        elif (identity is None or identity["platform"] != entry["platform"]
              or identity["uid"] != entry.get("uid")
              or identity_keys[(identity["platform"], identity["uid"])] != 1
              or not isinstance(identity["uid"], str)
              or not valid_uid(identity["platform"], identity["uid"])):
            reason = "identity_conflict"
        else:
            if entry["platform"] == "xiaohongshu":
                admitted = admissions.get(identity["id"])
                # A well-formed imported UID is not proof that a public
                # profile resolved to that identity. Existing admissions are
                # checked by the immutable-receipt reader above. Unlike a
                # Douyin locator receipt, this proof needs no materialized
                # provider reference because XHS requests use the UID itself.
                if entry["identity_status"] == "existing_verified" and identity["id"] not in prepared_xhs:
                    evidence = {"kind": "verified_directory_identity", "directory_row_id": entry["id"]}
                elif (identity["id"] not in prepared_xhs and admitted is not None and admitted["account_id"] == member["account_id"]
                        and admitted["member"].get("platform") == member["platform"]
                        and admitted["member"].get("uid") == member["uid"]):
                    evidence = {"kind": "verified_account_identity_admission", "admission_sha256": _digest(admitted)}
                else:
                    evidence = None
                reason, proof = ("eligible", {"locator_kind": "uid",
                    "locator_sha256": hashlib.sha256(identity["uid"].encode()).hexdigest(),
                    "locator_evidence": evidence}) if evidence else ("identity_evidence_missing", {})
                if evidence is None and xhs_refs[identity["id"]]:
                    references = xhs_refs[identity["id"]]
                    values = {row["reference_value"] for row in references}
                    if values != {member["uid"]} or xhs_reference_owners[member["uid"]] != {identity["id"]}:
                        reason, proof = "reference_conflict", {}
                    else:
                        # Every attached source must agree. A newer successful
                        # record cannot hide a conflicting or corrupted one.
                        verified = []
                        for reference in sorted(references, key=lambda row: row["source_raw_response_id"] or 0, reverse=True):
                            reason, proof = _prove_xhs_reference(connection, member, reference)
                            if reason != "eligible":
                                break
                            verified.append(proof)
                        else:
                            reason, proof = "eligible", verified[0]
            elif entry["platform"] in {"kuaishou", "wechat_channels"}:
                reason, proof = "identity_evidence_missing", {}
                references = prepared_refs[identity["id"]]
                for reference in references:
                    reason, proof = _prove_prepared_reference(connection, member, reference)
                    if reason != "eligible":
                        break
            else:
                # Import labels do not substitute for or veto the exact
                # identity/locator proof below. Reading existing evidence can
                # resolve an imported identity without changing its DB row.
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


def identity_capture_evidence(connection: sqlite3.Connection, identity_id: int) -> dict[str, Any]:
    """Read the same identity/evidence result used by automatic capture.

    This never edits operating labels or grants provider-send permission.
    """
    base = {"identity_id": identity_id, "account_identity_id": identity_id}
    if type(identity_id) is not int or identity_id <= 0:
        return _result(base, "identity_missing", enabled=False)
    result = _derive(connection, identity_id=identity_id)
    members = result["eligible_members"] + result["excluded_members"]
    if len(members) == 1:
        return members[0]
    return _result(base, "directory_conflict" if members else "identity_missing", enabled=False)


def require_directory_capture_member(connection: sqlite3.Connection, identity_id: int) -> dict[str, Any]:
    """Recheck one automatic target; manual content commands must not call this."""
    if type(identity_id) is not int or identity_id <= 0:
        raise DirectoryCaptureEligibilityError("identity_missing")
    result = _derive(connection, identity_id=identity_id)
    if len(result["eligible_members"]) == 1 and not result["excluded_members"]:
        return result["eligible_members"][0]
    reason = result["excluded_members"][0]["reason_code"] if result["excluded_members"] else "identity_missing"
    raise DirectoryCaptureEligibilityError(reason)
