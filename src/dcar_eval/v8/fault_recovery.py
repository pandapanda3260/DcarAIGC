"""Verify local evidence before a fault domain can resume ordinary work.

This module performs no provider requests. Campaigns own sample selection and
send authorization; recovery reads the frozen campaign and the actual ledger.
"""

from __future__ import annotations

import hashlib
import json
import math
import shutil
import sqlite3
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from .raw_evidence import read_raw_evidence, write_zstd_raw_evidence


class FaultRecoveryEvidenceError(ValueError):
    pass


def _time(value: Any) -> datetime:
    if not isinstance(value, str):
        raise FaultRecoveryEvidenceError("evidence time is missing")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise FaultRecoveryEvidenceError("evidence time requires a timezone")
    return parsed


def _proof(
    connection: sqlite3.Connection, receipt_id: Any, contract: str, at: str
) -> dict[str, Any]:
    if type(receipt_id) is not int or receipt_id <= 0:
        raise FaultRecoveryEvidenceError("a persisted receipt id is required")
    row = connection.execute(
        "SELECT status,details_json FROM scheduler_runs WHERE id=?", (receipt_id,)
    ).fetchone()
    value = json.loads(row["details_json"]) if row is not None else None
    if (
        row is None
        or row["status"] != "succeeded"
        or not isinstance(value, dict)
        or value.get("contract_version") != contract
        or _time(value.get("issued_at")) > _time(at)
        or _time(value.get("expires_at")) <= _time(at)
    ):
        raise FaultRecoveryEvidenceError("recovery receipt is absent, invalid or expired")
    return value


def _binding(current: Mapping[str, Any], evidence: Mapping[str, Any]) -> None:
    if (
        evidence.get("fault_generation") != current.get("generation")
        or evidence.get("fault_fingerprint") != current.get("state_fingerprint")
        or not current.get("generation")
        or not current.get("state_fingerprint")
    ):
        raise FaultRecoveryEvidenceError("recovery evidence belongs to another fault")


def _request_samples(
    connection: sqlite3.Connection,
    campaign: Mapping[str, Any],
    scopes_key: str,
    count: int,
    at: str,
) -> list[dict[str, Any]]:
    scopes = campaign.get(scopes_key)
    if (
        not isinstance(scopes, list)
        or len(scopes) != count
        or any(not isinstance(scope, str) or len(scope) != 64 for scope in scopes)
        or len(set(scopes)) != count
    ):
        raise FaultRecoveryEvidenceError("campaign sample is not the fixed unique cohort")
    samples = []
    for scope in scopes:
        rows = connection.execute(
            """SELECT * FROM provider_usage
               WHERE lower(provider)='tikhub' AND operation=?
                 AND request_attempts=1 AND json_valid(details_json)
                 AND json_extract(details_json,'$.paid_scope_identity')=?""",
            (campaign["operation"], scope),
        ).fetchall()
        if len(rows) != 1:
            raise FaultRecoveryEvidenceError("campaign scope has missing or duplicate sends")
        usage = rows[0]
        details = json.loads(usage["details_json"])
        transport = details.get("transport")
        due_by_scope = campaign.get("due_by_scope")
        if not isinstance(due_by_scope, dict):
            raise FaultRecoveryEvidenceError("campaign natural due manifest is missing")
        if (
            details.get("paid_sequence") != 0
            or details.get("state") != "completed"
            or _time(details.get("sent_at")) < _time(campaign["issued_at"])
            or _time(details.get("sent_at")) > _time(at)
            or _time(due_by_scope.get(scope)) > _time(details.get("sent_at"))
            or not isinstance(transport, dict)
            or transport.get("clean_eof") is not True
            or transport.get("length_match") is False
            or transport.get("gzip_crc_ok") is False
            or transport.get("json_parse_ok") is not True
            or transport.get("transport_route_id") != campaign.get("transport_route_id")
            or transport.get("route_generation") != campaign.get("route_generation")
            or type(transport.get("http_status")) is not int
            or not 200 <= transport["http_status"] < 300
        ):
            raise FaultRecoveryEvidenceError("campaign request did not complete its transport contract")
        raw = connection.execute(
            "SELECT * FROM provider_raw_responses WHERE id=?",
            (details.get("raw_response_id"),),
        ).fetchone()
        if (
            raw is None
            or str(raw["provider"]).lower() != "tikhub"
            or raw["operation"] != campaign["operation"]
            or raw["source"] not in {"live_applied", "derived_applied"}
        ):
            raise FaultRecoveryEvidenceError("campaign raw lacks durable materialization")
        loaded = read_raw_evidence(
            Path(raw["local_path"]),
            expected_stored_sha256=raw["sha256"],
            expected_stored_size=int(raw["byte_size"]),
        )
        if (
            loaded.receipt.paid_scope_identity != scope
            or loaded.receipt.sequence != 0
            or loaded.receipt.entity_sha256 != transport.get("entity_sha256")
        ):
            raise FaultRecoveryEvidenceError("campaign raw identity does not match the send")
        samples.append({"usage_id": int(usage["id"]), "sent_at": details["sent_at"]})
    return samples


def verify_operation_recovery(
    connection: sqlite3.Connection,
    current: Mapping[str, Any],
    evidence: Mapping[str, Any],
    at: str,
) -> dict[str, Any]:
    _binding(current, evidence)
    operation = str(current["operation"])
    fault_class = str(current["fault_class"])
    if fault_class in {"transport", "rate_limit"}:
        contract = (
            "transport-requalification-campaign-v1"
            if fault_class == "transport"
            else "rate-recovery-campaign-v1"
        )
        campaign = _proof(connection, evidence.get("campaign_id"), contract, at)
        _binding(current, campaign)
        if (
            campaign.get("provider") != "tikhub"
            or campaign.get("operation") != operation
            or not campaign.get("transport_route_id")
            or not campaign.get("route_generation")
        ):
            raise FaultRecoveryEvidenceError("campaign operation or route is invalid")
        half_open = _request_samples(connection, campaign, "half_open_scopes", 3, at)
        times = sorted(_time(sample["sent_at"]) for sample in half_open)
        if fault_class == "transport":
            if any((right - left).total_seconds() < 300 for left, right in zip(times, times[1:])):
                raise FaultRecoveryEvidenceError("transport half-open exceeds one request per five minutes")
            if set(campaign["half_open_scopes"]) & set(campaign.get("sample_scopes", [])):
                raise FaultRecoveryEvidenceError("half-open and qualification cohorts overlap")
            samples = _request_samples(connection, campaign, "sample_scopes", 200, at)
            if min(_time(sample["sent_at"]) for sample in samples) <= times[-1]:
                raise FaultRecoveryEvidenceError("qualification preceded the half-open gate")
            # At the fixed n=200 gate even one uncertain response has a Wilson
            # upper bound above 2%; every member must therefore be verified.
            z2 = 1.96 ** 2
            upper = z2 / (len(samples) + z2)
            if not math.isfinite(upper) or upper > 0.02:
                raise FaultRecoveryEvidenceError("transport Wilson gate failed")
            return {
                **dict(evidence), "operation": operation, "sample_size": 200,
                "uncertain_count": 0, "wilson_upper": upper,
                "partial_canonical": 0, "raw_readback_count": len(samples),
                "consecutive_complete": 3,
                "verified_usage_ids": [sample["usage_id"] for sample in samples],
            }
        quota = _proof(connection, evidence.get("quota_receipt_id"), "provider-quota-window-v1", at)
        if (
            quota.get("provider") != "tikhub"
            or quota.get("operation") != operation
            or quota.get("price_valid") is not True
            or times[0] < _time(quota.get("reset_at"))
        ):
            raise FaultRecoveryEvidenceError("rate recovery preceded the verified quota reset")
        return {**dict(evidence), "operation": operation, "reset_at": quota["reset_at"],
                "consecutive_natural_due": 3, "rate_errors": 0,
                "verified_usage_ids": [sample["usage_id"] for sample in half_open]}
    if fault_class == "field_contract":
        policy = _proof(connection, evidence.get("policy_receipt_id"), "field-policy-release-v1", at)
        _binding(current, policy)
        for key, contract in (("replay_receipt_id", "field-policy-replay-v1"),
                              ("canary_receipt_id", "field-policy-canary-v1")):
            proof = _proof(connection, policy.get(key), contract, at)
            if (proof.get("operation") != operation or proof.get("passed") is not True
                    or proof.get("policy_sha256") != policy.get("policy_sha256")):
                raise FaultRecoveryEvidenceError("field recovery lacks matching replay and canary")
        if policy.get("operation") != operation or not policy.get("policy_sha256"):
            raise FaultRecoveryEvidenceError("field policy operation is invalid")
        return {**dict(evidence), "operation": operation,
                "offline_replay_passed": True, "field_canary_passed": True}
    raise FaultRecoveryEvidenceError("unknown operation recovery domain")


def verify_storage_recovery(
    connection: sqlite3.Connection,
    current: Mapping[str, Any],
    evidence: Mapping[str, Any],
    at: str,
) -> dict[str, Any]:
    _binding(current, evidence)
    capacity = _proof(connection, evidence.get("capacity_receipt_id"), "storage_capacity_receipt_v1", at)
    root_value = capacity.get("raw_root")
    if capacity.get("admitted") is not True or not isinstance(root_value, str):
        raise FaultRecoveryEvidenceError("storage capacity was not admitted for a raw root")
    root = Path(root_value)
    if not root.is_absolute() or root.is_symlink() or not root.is_dir():
        raise FaultRecoveryEvidenceError("storage recovery requires the existing raw directory")
    affected_root = current.get("state_evidence", {}).get("raw_root")
    if affected_root is not None and str(root.resolve()) != affected_root:
        raise FaultRecoveryEvidenceError("storage proof covers another raw directory")
    disk = shutil.disk_usage(root)
    if disk.used / disk.total >= 0.90:
        raise FaultRecoveryEvidenceError("raw volume remains above the fail-closed threshold")
    with tempfile.TemporaryDirectory(prefix=".fault-recovery-", dir=root) as temporary:
        check_root = Path(temporary)
        body = json.dumps({"fault_generation": current["generation"]}).encode()
        digest = hashlib.sha256(body).hexdigest()
        path = check_root / "readback.json.zst"
        first = write_zstd_raw_evidence(
            path, body, provider="local", operation="storage_recovery",
            response_identity=digest, paid_scope_identity=digest,
            sequence=0, evidence_root=root,
        )
        repeated = write_zstd_raw_evidence(
            path, body, provider="local", operation="storage_recovery",
            response_identity=digest, paid_scope_identity=digest,
            sequence=0, evidence_root=root,
        )
        loaded = read_raw_evidence(path, expected_stored_sha256=first.stored_sha256,
                                   expected_stored_size=first.stored_size)
        if first != repeated or loaded.entity_bytes != body:
            raise FaultRecoveryEvidenceError("local storage durability check failed")
    return {**dict(evidence), "raw_root": str(root.resolve()),
            "idempotent_write_passed": True, "file_fsync_passed": True,
            "directory_fsync_passed": True, "hash_readback_passed": True,
            "entity_sha256": digest, "stored_sha256": first.stored_sha256}
