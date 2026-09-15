"""Offline import of complete, checksum-bound public profile responses.

This never grants execution authority or invents a paid attempt. The original
HTTP entity must be exactly recoverable from the saved response envelope.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import sqlite3
from typing import Any

CONTRACT = "local-account-profile-evidence-import-v1"
SOURCE = "local_profile_evidence_import"
ROUTES = {
    "/api/v1/douyin/web/fetch_user_profile_by_uid": ("douyin", "uid", "douyin_uid_profile", "sec_user_id"),
    "/api/v1/xiaohongshu/app_v2/get_user_info": ("xiaohongshu", "user_id", "xiaohongshu_user_profile", "user_id"),
}


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def envelopes(value: Any, pointer: str = "$"):
    if isinstance(value, dict):
        if "route" in value:
            yield pointer, value
        else:
            for key, child in value.items():
                yield from envelopes(child, f"{pointer}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from envelopes(child, f"{pointer}[{index}]")


def validate_envelope(envelope: dict) -> dict:
    route = envelope.get("route")
    if route not in ROUTES:
        raise ValueError("unsupported_profile_route")
    platform, parameter, operation, kind = ROUTES[route]
    receipt, payload = envelope.get("receipt"), envelope.get("payload")
    if not isinstance(receipt, dict) or not isinstance(payload, dict):
        raise ValueError("complete_response_missing")
    if (type(envelope.get("http_status")) is not int or envelope["http_status"] != 200
            or receipt.get("contract_version") != "provider-json-transport-v1"
            or receipt.get("status") != "succeeded" or receipt.get("http_status") != 200
            or receipt.get("clean_eof") is not True or receipt.get("json_parse_ok") is not True
            or receipt.get("error_code") is not None or receipt.get("zero_body") is not False
            or type(receipt.get("partial_bytes")) is not int or receipt["partial_bytes"] != 0
            or receipt.get("request_host") != "api.tikhub.io"
            or receipt.get("content_encoding") == "gzip" and receipt.get("gzip_crc_ok") is not True):
        raise ValueError("transport_not_successful")
    # This encoding is accepted only when BOTH original byte-boundary checks
    # match. A slim summary or altered/scrubbed payload cannot pass by resealing.
    entity = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
    if digest(entity) != receipt.get("entity_sha256") or len(entity) != receipt.get("entity_bytes"):
        raise ValueError("original_entity_checksum_mismatch")
    from datetime import datetime
    try:
        times = [datetime.fromisoformat(str(value).replace("Z", "+00:00")) for value in (
            receipt.get("request_started_at"), receipt.get("response_finished_at"), envelope.get("captured_at"))]
        if any(value.utcoffset() is None for value in times) or times != sorted(times):
            raise ValueError()
    except (ValueError, TypeError):
        raise ValueError("invalid_capture_time") from None
    params, inner_params = envelope.get("params"), payload.get("params")
    uid = params.get(parameter) if isinstance(params, dict) else None
    pattern = r"[0-9]{6,24}" if platform == "douyin" else r"[0-9a-fA-F]{24}"
    if (not isinstance(uid, str) or re.fullmatch(pattern, uid) is None
            or not isinstance(inner_params, dict) or inner_params.get(parameter) != uid
            or type(payload.get("code")) is not int or payload["code"] != 200 or payload.get("router") != route):
        raise ValueError("request_identity_mismatch")
    data = payload.get("data")
    if not isinstance(data, dict) or not isinstance(data.get("data"), dict):
        raise ValueError("profile_object_missing")
    user = data["data"]
    if platform == "douyin":
        identifiers = {user[key] for key in ("id_str", "uid", "user_id") if isinstance(user.get(key), str) and user[key]}
        refs = {user[key] for key in ("sec_uid", "sec_user_id") if isinstance(user.get(key), str) and user[key]}
        if type(data.get("status_code")) is not int or data["status_code"] != 0 or identifiers != {uid} or len(refs) != 1:
            raise ValueError("response_identity_mismatch")
        locator = next(iter(refs))
        if re.fullmatch(r"MS4wLjAB[A-Za-z0-9_-]{32,120}", locator) is None:
            raise ValueError("invalid_locator")
    else:
        if (data.get("success") is not True or type(data.get("code")) is not int or data["code"] != 0 or user.get("userid") != uid
                or any(user[key] != uid for key in ("uid", "user_id", "id_str") if user.get(key) not in (None, ""))
                or not isinstance(user.get("nickname"), str) or not user["nickname"].strip()
                or not isinstance(user.get("result"), dict) or user["result"].get("success") is not True
                or type(user["result"].get("code")) is not int or user["result"]["code"] != 0):
            raise ValueError("response_identity_mismatch")
        locator = uid
    return {"platform": platform, "uid": uid, "operation": operation, "reference_kind": kind,
            "reference_value": locator, "captured_at": envelope["captured_at"], "entity": entity,
            "entity_sha256": digest(entity), "envelope": envelope}


def plan_import(connection: sqlite3.Connection, inputs: list[Path]) -> tuple[list[dict], list[dict]]:
    plans, rows, seen = [], [], {}
    for path in inputs:
        if path.is_symlink() or not path.is_file() or path.stat().st_nlink != 1:
            raise ValueError("Input must be a regular unlinked file")
        data = path.read_bytes()
        for pointer, envelope in envelopes(json.loads(data)):
            row = {"source_file": str(path), "source_sha256": digest(data), "source_pointer": pointer,
                   "route": envelope.get("route")}
            rows.append(row)
            try:
                candidate = validate_envelope(envelope)
            except (ValueError, TypeError, KeyError) as error:
                row.update(status="skipped", reason=str(error))
                continue
            identities = connection.execute("SELECT * FROM account_platform_identities WHERE platform=? AND uid=?",
                (candidate["platform"], candidate["uid"])).fetchall()
            directories = connection.execute("SELECT * FROM account_directory_rows WHERE platform=? AND uid=?",
                (candidate["platform"], candidate["uid"])).fetchall()
            if len(identities) != 1 or len(directories) != 1 or identities[0]["account_id"] != directories[0]["account_id"]:
                row.update(status="skipped", reason="identity_not_uniquely_bound")
                continue
            identity, directory = dict(identities[0]), dict(directories[0])
            account_id, identity_id = identity["account_id"], identity["id"]
            if (connection.execute("SELECT count(*) FROM account_platform_identities WHERE account_id=?", (account_id,)).fetchone()[0] != 1
                    or connection.execute("SELECT count(*) FROM account_directory_rows WHERE account_id=?", (account_id,)).fetchone()[0] != 1):
                row.update(status="skipped", reason="account_binding_ambiguous")
                continue
            row.update(account_id=account_id, identity_id=identity_id, directory_row_id=directory["id"], platform=candidate["platform"])
            references = connection.execute("SELECT r.* FROM account_provider_references r JOIN account_platform_identities i "
                "ON i.id=r.account_identity_id WHERE lower(r.provider)='tikhub' AND r.reference_kind=? "
                "AND i.platform=? AND (r.account_identity_id=? OR r.reference_value=?)",
                (candidate["reference_kind"], candidate["platform"], identity_id, candidate["reference_value"])).fetchall()
            if any(ref["account_identity_id"] != identity_id or ref["reference_value"] != candidate["reference_value"] for ref in references):
                row.update(status="skipped", reason="existing_reference_conflict")
                continue
            if len(references) > 1:
                row.update(status="skipped", reason="multiple_reference_rows")
                continue
            if references and references[0]["source_raw_response_id"] is not None:
                from .account_capture_eligibility import identity_capture_evidence
                valid = identity_capture_evidence(connection, identity_id)["eligible"]
                row.update(status="unchanged" if valid else "skipped", reason="existing_evidence_valid" if valid else "existing_evidence_requires_repair")
                continue
            if identity_id in seen:
                if seen[identity_id] != candidate["reference_value"]:
                    raise ValueError("Input responses disagree about one identity; nothing imported")
                row.update(status="unchanged", reason="duplicate_input_identity")
                continue
            seen[identity_id] = candidate["reference_value"]
            row.update(status="planned", entity_sha256=candidate["entity_sha256"])
            plans.append({**candidate, "account_id": account_id, "identity_id": identity_id,
                          "reference_provider": references[0]["provider"] if references else None, "source": row})
    return plans, rows


def apply_import(connection: sqlite3.Connection, plans: list[dict], *, evidence_dir: Path) -> None:
    if not connection.in_transaction:
        raise ValueError("Evidence import requires one transaction")
    from .raw_evidence import write_immutable_json_receipt
    evidence_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    if evidence_dir != evidence_dir.resolve(strict=True):
        raise ValueError("Evidence directory must not traverse symlinks")
    for entry in plans:
        checksum = entry["entity_sha256"]
        body = evidence_dir / f"{checksum}.json"
        if body.exists():
            if body.is_symlink() or body.stat().st_nlink != 1 or body.read_bytes() != entry["entity"]:
                raise ValueError("Existing evidence file differs from original response")
        else:
            import os
            fd = os.open(body, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(fd, "wb") as handle:
                handle.write(entry["entity"])
                handle.flush()
                os.fsync(handle.fileno())
        write_immutable_json_receipt(evidence_dir / f"{checksum}.import.json",
            {"contract": CONTRACT, "source": entry["source"], "saved_response": entry["envelope"]}, evidence_root=evidence_dir)
        cursor = connection.execute("""INSERT INTO provider_raw_responses(account_id,provider,operation,local_path,
            sha256,byte_size,http_status,captured_at,source) VALUES(?,'TikHub',?,?,?,?,200,?,?)""",
            (entry["account_id"], entry["operation"], str(body), checksum, len(entry["entity"]), entry["captured_at"], SOURCE))
        raw_id = int(cursor.lastrowid)
        if entry["reference_provider"] is None:
            fields = {row[1] for row in connection.execute("PRAGMA table_info(account_provider_references)")}
            if "platform" in fields:
                connection.execute("""INSERT INTO account_provider_references(account_identity_id,platform,provider,reference_kind,
                    reference_value,source_raw_response_id,created_at,updated_at) VALUES(?,?,'TikHub',?,?,?,?,?)""",
                    (entry["identity_id"], entry["platform"], entry["reference_kind"], entry["reference_value"], raw_id, entry["captured_at"], entry["captured_at"]))
            else:
                connection.execute("""INSERT INTO account_provider_references(account_identity_id,provider,reference_kind,
                    reference_value,source_raw_response_id,created_at,updated_at) VALUES(?,'TikHub',?,?,?,?,?)""",
                    (entry["identity_id"], entry["reference_kind"], entry["reference_value"], raw_id, entry["captured_at"], entry["captured_at"]))
        else:
            connection.execute("UPDATE account_provider_references SET source_raw_response_id=? WHERE account_identity_id=? AND provider=? AND reference_kind=? AND source_raw_response_id IS NULL",
                (raw_id, entry["identity_id"], entry["reference_provider"], entry["reference_kind"]))
        from .account_capture_eligibility import identity_capture_evidence
        proof = identity_capture_evidence(connection, entry["identity_id"])
        if not proof["eligible"]:
            raise ValueError("Imported response did not pass existing identity/locator checks: " + proof["reason_code"])
        entry["source"].update(status="imported", raw_response_id=raw_id)
