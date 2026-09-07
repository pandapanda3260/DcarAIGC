"""Local D8 raw retirement, with retained provenance and commit-before-unlink.

The caller owns the application writer lock. This module never calls providers,
creates storage roots, removes ledger rows, or sweeps historical loose files.
Filesystem-only current release proofs must be registered using raw_archive pins
before maintenance; ordinary response/metric foreign keys are provenance, not
permanent body-retention requirements.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import stat
from pathlib import Path
from typing import Any

from . import raw_archive as archive
from . import raw_evidence

POLICY = "local-seven-complete-beijing-days-v1"
STORAGE_SCHEMA = "raw-local-storage-v1"
MAINTENANCE_SCHEMA = "raw-local-maintenance-v1"
INTENT = "local_retention_retire_intent"
COMPLETED = "local_retention_retired"
TEMP_HEADROOM = 2 * raw_evidence.MAX_STORED_BYTES
APPLIED_SOURCES = frozenset({"live_applied", "derived_applied", "matrix_page_applied"})


def inspect_local_storage(*, live_root: Path, daily_stored_p95: int = 0) -> dict[str, Any]:
    """Read the existing local volume; zero forecast proves no future capacity.

    Reserve nine forecast days (eight retained days plus next day's peak), two
    maximum bounded raw writes, and the existing 90%-used admission boundary.
    Existing protected bodies already consume measured used space; future hold
    growth is unknown and must remain an operational observation, not zero.
    """
    if not live_root.is_absolute() or type(daily_stored_p95) is not int or daily_stored_p95 < 0:
        raise archive.RawArchiveError("local storage requires an absolute existing root and nonnegative forecast")
    root = archive._safe_existing(live_root)
    meta = root.stat()
    if not stat.S_ISDIR(meta.st_mode) or meta.st_uid != os.geteuid() or meta.st_mode & 0o022:
        raise archive.RawArchiveError("local raw root must be an owned non-shared directory")
    usage = shutil.disk_usage(root)
    required = max((usage.total + 9) // 10, 9 * daily_stored_p95 + TEMP_HEADROOM)
    if not os.access(root, os.R_OK | os.W_OK | os.X_OK):
        raise archive.RawArchiveError("local raw root lacks required access")
    used_ratio = (usage.total - usage.free) / usage.total
    admission_allowed = usage.free >= required and used_ratio < 0.90
    return {"schema": STORAGE_SCHEMA, "policy": POLICY, "root": str(root),
            "device": meta.st_dev, "inode": meta.st_ino, "total_bytes": usage.total,
            "free_bytes": usage.free, "used_ratio": used_ratio,
            "daily_stored_p95": daily_stored_p95, "required_free_bytes": required,
            "temporary_headroom_bytes": TEMP_HEADROOM, "forecast_known": daily_stored_p95 > 0,
            "future_protected_growth": "unknown", "physical_io_verified": False,
            "capacity_ready": admission_allowed and daily_stored_p95 > 0,
            "admission_allowed": admission_allowed,
            "health": "critical" if used_ratio >= 0.85 else "warning" if used_ratio >= 0.70 else "normal",
            "verified_at": archive._now()}


def qualify_local_storage(*, live_root: Path, daily_stored_p95: int = 0) -> dict[str, Any]:
    """Explicit writer-side bounded fsync/readback; never called by GET checks."""
    receipt = validate_local_storage(live_root=live_root, daily_stored_p95=daily_stored_p95)
    root = Path(receipt["root"])
    meta = root.stat()
    # A tiny actual fsync/readback probe is bounded independently of the forecast.
    directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    probe = ".local-storage-probe-" + os.urandom(12).hex()
    descriptor: int | None = None
    try:
        if (os.fstat(directory).st_dev, os.fstat(directory).st_ino) != (meta.st_dev, meta.st_ino):
            raise archive.RawArchiveError("local raw root changed during validation")
        descriptor = os.open(probe, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory)
        payload = os.urandom(4096)
        if os.write(descriptor, payload) != len(payload):
            raise archive.RawArchiveError("local raw write probe incomplete")
        os.fsync(descriptor)
        os.lseek(descriptor, 0, os.SEEK_SET)
        if os.read(descriptor, len(payload) + 1) != payload:
            raise archive.RawArchiveError("local raw readback probe differs")
    finally:
        if descriptor is not None:
            os.close(descriptor)
            os.unlink(probe, dir_fd=directory)
            os.fsync(directory)
        os.close(directory)
    receipt["physical_io_verified"] = True
    receipt["io_probe"] = {"schema": "raw-local-io-probe-v1", "sha256": hashlib.sha256(payload).hexdigest(),
        "byte_size": len(payload), "verified_at": archive._now(), "device": meta.st_dev, "inode": meta.st_ino}
    return receipt


def validate_local_storage(*, live_root: Path, daily_stored_p95: int = 0) -> dict[str, Any]:
    """Admission check; unknown forecasts remain explicitly physical-only."""
    receipt = inspect_local_storage(live_root=live_root, daily_stored_p95=daily_stored_p95)
    if not receipt["admission_allowed"]:
        raise archive.RawArchiveError("local raw storage has insufficient reserved capacity")
    return receipt


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    # Only callers' hard-coded schema names reach this internal helper.
    return {row[1] for row in conn.execute(f'PRAGMA table_info("{table}")')}


def _reference(value: Any, response: dict[str, Any], blob: dict[str, Any], *, broad: bool = False) -> bool:
    """Recognize typed identities in durable JSON, without substring ID matches."""
    if isinstance(value, list):
        return any(_reference(item, response, blob, broad=broad) for item in value)
    if not isinstance(value, dict):
        return False
    exact: dict[str, Any] = {"raw_response_id": response["id"], "source_raw_response_id": response["id"],
        "raw_id": response["id"], "raw_blob_id": blob["id"], "blob_id": blob["id"],
        "paid_scope_identity": response.get("paid_scope_identity"),
        "request_scope_identity": response.get("paid_scope_identity"),
        "slot_id": response.get("_slot_id"), "attempt_id": response.get("fetch_attempt_id"), "fetch_attempt_id": response.get("fetch_attempt_id"),
        "local_path": response["local_path"], "path": response["local_path"],
        "raw_sha256": response["sha256"], "source_sha256": response["sha256"]}
    if broad:
        exact.update(content_id=response.get("content_id"), account_id=response.get("account_id"))
    for key, expected in exact.items():
        if expected is not None and key in value and type(value[key]) is type(expected) and value[key] == expected:
            return True
    for key in ("raw_ids", "raw_response_ids", "source_raw_response_ids"):
        if isinstance(value.get(key), list) and response["id"] in value[key]:
            return True
    return any(_reference(item, response, blob, broad=broad) for item in value.values())


def _json(value: str) -> Any:
    try:
        return json.loads(value)
    except (ValueError, TypeError):
        return None


_ID_KEYS = frozenset({"raw_response_id", "source_raw_response_id", "raw_id", "raw_blob_id", "blob_id",
    "paid_scope_identity", "request_scope_identity", "scope_identity", "attempt_id", "fetch_attempt_id",
    "slot_id", "local_path", "path", "raw_sha256", "source_sha256", "content_id", "account_id"})


def _reference_keys(value: Any) -> set[tuple[str, Any]]:
    keys: set[tuple[str, Any]] = set()
    if isinstance(value, list):
        for item in value:
            keys.update(_reference_keys(item))
    elif isinstance(value, dict):
        for key, item in value.items():
            if key in _ID_KEYS and type(item) in {str, int}:
                keys.add((key, item))
            if key in {"raw_ids", "raw_response_ids", "source_raw_response_ids"} and isinstance(item, list):
                keys.update(("raw_response_id", raw_id) for raw_id in item if type(raw_id) is int)
            keys.update(_reference_keys(item))
    return keys


class _ReferenceIndex:
    """Read each relevant ledger once per tick and index exact typed references."""
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.cache: dict[str, tuple[list[dict[str, Any]], dict[tuple[str, Any], set[int]], set[int]]] = {}
        self.media: dict[int, set[str]] | None = None
        self.media_unknown: set[str] = set()
        self.media_scoped_unknown: dict[tuple[str, int], set[str]] = {}
        self.media_processed: set[int] = set()
        self.media_index_complete = False
        self.settled: dict[int, str] | None = None
        self.settlement_scopes: dict[str, set[str]] = {}

    def matches(self, sql: str, response: dict[str, Any], blob: dict[str, Any], *, broad: bool = False) -> list[dict[str, Any]]:
        if sql not in self.cache:
            rows = archive._rows(self.conn, sql)
            indexed: dict[tuple[str, Any], set[int]] = {}
            unknown: set[int] = set()
            for number, row in enumerate(rows):
                keys = _reference_keys(row)
                for column, value in row.items():
                    if column.endswith("_json"):
                        parsed = _json(value)
                        if parsed is None:
                            unknown.add(number)
                        else:
                            keys.update(_reference_keys(parsed))
                for reference_key in keys:
                    indexed.setdefault(reference_key, set()).add(number)
            self.cache[sql] = rows, indexed, unknown
        rows, indexed, unknown = self.cache[sql]
        wanted = {"raw_response_id": response["id"], "source_raw_response_id": response["id"], "raw_id": response["id"],
            "raw_blob_id": blob["id"], "blob_id": blob["id"], "paid_scope_identity": response.get("paid_scope_identity"),
            "request_scope_identity": response.get("paid_scope_identity"), "scope_identity": response.get("paid_scope_identity"),
            "attempt_id": response.get("fetch_attempt_id"), "fetch_attempt_id": response.get("fetch_attempt_id"),
            "slot_id": response.get("_slot_id"), "local_path": response["local_path"], "path": response["local_path"],
            "raw_sha256": response["sha256"], "source_sha256": response["sha256"]}
        if broad:
            wanted.update(content_id=response.get("content_id"), account_id=response.get("account_id"))
        positions = set(unknown)
        for key, value in wanted.items():
            if value is not None:
                positions.update(indexed.get((key, value), set()))
        return [rows[number] for number in sorted(positions)]


def _billing_reasons(conn: sqlite3.Connection, response: dict[str, Any], blob: dict[str, Any], tables: set[str], index: _ReferenceIndex) -> list[str]:
    reasons: list[str] = []
    scope = response.get("paid_scope_identity")
    if "provider_usage" not in tables:
        return ["billing_ledger_unavailable"]
    if index.settled is None:
        index.settled = {}
        if {"provider_usage_settlements", "provider_usage_settlement_events"} <= tables:
            for row in archive._rows(conn, """SELECT s.provider_usage_id,s.scope_identity,e.state FROM provider_usage_settlements s
                LEFT JOIN provider_usage_settlement_events e ON e.id=(SELECT max(id) FROM provider_usage_settlement_events WHERE settlement_id=s.id)"""):
                index.settled[int(row["provider_usage_id"])] = str(row["state"])
                if row["scope_identity"] is not None:
                    index.settlement_scopes.setdefault(row["scope_identity"], set()).add(str(row["state"]))
    settled = index.settled
    states = index.settlement_scopes.get(scope, set()) if scope is not None else set()
    if states - {"charged_verified", "refunded"}:
        reasons.append("unresolved_settlement")
    matched = bool(states)
    if response.get("_batch_id") is not None and "admission_reservations" in tables:
        if conn.execute("SELECT 1 FROM admission_reservations WHERE batch_id=? AND state NOT IN ('settled','released_unsent')", (response["_batch_id"],)).fetchone():
            reasons.append("unsettled_admission_reservation")
    for row in index.matches("SELECT id,details_json FROM provider_usage", response, blob):
        details = _json(row["details_json"])
        if not isinstance(details, dict):
            reasons.append("invalid_billing_evidence")
            continue
        if not _reference(details, response, blob):
            continue
        matched = True
        if settled.get(row["id"]) in {"charged_verified", "refunded"}:
            continue
        # Explicit final accounting only. Missing/unknown labels do not imply free.
        if details.get("state") not in {"charged_verified", "refunded", "unbilled", "zero_cost", "derived_zero_cost"}:
            reasons.append("billing_unresolved_or_unverified")
    if not matched:
        # Derived evidence has no paid dispatch of its own; its source is protected
        # separately by materialization/media dependencies, never inferred for live.
        if response.get("source") != "derived_applied":
            reasons.append("billing_evidence_missing")
    return reasons


def _materialized_response(conn: sqlite3.Connection, response: dict[str, Any], blob: dict[str, Any], *, tables: set[str], index: _ReferenceIndex) -> bool:
    if response.get("source") in APPLIED_SOURCES:
        return True
    if response.get("source") != "live" or not {"data_quality_receipts", "capture_work_items"} <= tables:
        return False
    for receipt in index.matches("SELECT * FROM data_quality_receipts", response, blob):
        proof = _json(receipt["payload_json"])
        if not isinstance(proof, dict):
            continue
        canonical = json.dumps(proof, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        if hashlib.sha256(canonical.encode()).hexdigest() != receipt["receipt_sha256"]:
            continue
        if proof.get("contract_version") == "capture-runtime-v1" and proof.get("all_raw_verified") is True and response["id"] in proof.get("raw_response_ids", []):
            work_id = proof.get("work_id")
            if receipt["scope_key"] == f"capture-scan:{work_id}" and conn.execute("SELECT 1 FROM capture_work_items WHERE id=? AND state='terminal' AND completed_at IS NOT NULL", (work_id,)).fetchone():
                return True
        if (proof.get("contract_version") != "douyin-statistics-batch-v1" or proof.get("complete") is not True or
            proof.get("raw_verified") is not True or proof.get("raw_response_id") != response["id"]):
            continue
        needed = {"fetch_request_batches", "fetch_request_batch_members", "fetch_request_member_dispositions", "fetch_attempts"}
        if not needed <= tables:
            continue
        batch_id = proof.get("batch_id")
        if receipt["scope_key"] != f"capture-batch:{batch_id}" or not conn.execute("SELECT 1 FROM fetch_attempts WHERE id=? AND request_batch_id=? AND response_finished_at IS NOT NULL", (response.get("fetch_attempt_id"), batch_id)).fetchone():
            continue
        members = archive._rows(conn, """SELECT m.id,m.content_id,d.disposition,d.raw_response_id
            FROM fetch_request_batch_members m LEFT JOIN fetch_request_member_dispositions d ON d.member_id=m.id WHERE m.batch_id=?""", (batch_id,))
        if not members or any(member["disposition"] != "valid" or member["raw_response_id"] != response["id"] for member in members):
            continue
        works = archive._rows(conn, "SELECT content_id,state,completed_at FROM capture_work_items WHERE json_extract(envelope_json,'$.request_batch_id')=?", (batch_id,))
        if ({work["content_id"] for work in works} != {member["content_id"] for member in members} or
            any(work["state"] != "terminal" or work["completed_at"] is None for work in works)):
            continue
        return True
    return False


def protection_reasons(conn: sqlite3.Connection, blob_id: int, *, at: str, _index: _ReferenceIndex | None = None) -> list[str]:
    """Fail closed on incomplete work, unknown provenance, holds and live proofs."""
    blob = archive._row(conn, "provider_raw_blobs", blob_id)
    reasons: list[str] = []
    if not blob["hot_owned"]:
        reasons.append("legacy_file_not_owned")
    stored_at = archive._effective_hot_time(conn, blob)
    if stored_at is None:
        reasons.append("unknown_raw_stored_at")
    elif archive.lifecycle_stage(stored_at, at=at) == "hot":
        reasons.append("before_D8")
    if reasons:
        return sorted(set(reasons))
    index = _index or _ReferenceIndex(conn)
    pins = archive._rows(conn, """SELECT e.reason FROM raw_retention_events e JOIN
        (SELECT max(id) id FROM raw_retention_events WHERE raw_blob_id=?
         AND event_type IN ('pin','unpin') GROUP BY pin_key) p ON p.id=e.id WHERE e.event_type='pin'""", (blob_id,))
    reasons.extend("pin:" + str(row["reason"]) for row in pins)
    tables = archive._tables(conn)
    responses = archive._rows(conn, "SELECT * FROM provider_raw_responses WHERE raw_blob_id=?", (blob_id,))
    if not responses:
        reasons.append("unattached_body_outcome_unknown")
    for response in responses:
        materialized = _materialized_response(conn, response, blob, tables=tables, index=index)
        if not materialized:
            reasons.append("source_unapplied_or_unknown")
        if response.get("raw_stored_at") is None:
            reasons.append("unknown_response_stored_at")
        elif archive.lifecycle_stage(response["raw_stored_at"], at=at) == "hot":
            reasons.append("recent_response")
        attempt_id = response.get("fetch_attempt_id")
        if attempt_id is not None and {"fetch_attempts", "fetch_slots"} <= tables:
            attempts = archive._rows(conn, "SELECT * FROM fetch_attempts WHERE id=?", (attempt_id,))
            if not attempts or not attempts[0]["response_finished_at"]:
                reasons.append("nonterminal_fetch_attempt_or_slot")
            elif attempts[0].get("slot_id") is None:
                response["_batch_id"] = attempts[0].get("request_batch_id")
                if not materialized or attempts[0].get("request_batch_id") is None:
                    reasons.append("nonterminal_batch_attempt")
            else:
                response["_slot_id"] = attempts[0]["slot_id"]
                slots = archive._rows(conn, "SELECT * FROM fetch_slots WHERE id=?", (attempts[0]["slot_id"],))
                if not slots or slots[0]["status"] not in {"succeeded", "terminal_failed"}:
                    reasons.append("nonterminal_fetch_attempt_or_slot")
                if slots and any(word in str(slots[0]["adapter_version"]) for word in ("tikhub-account-scan", "matrix-scan")):
                    reasons.append("scan_manifest_replay_dependency")
        elif attempt_id is not None:
            reasons.append("fetch_attempt_contract_unavailable")
        reasons.extend(_billing_reasons(conn, response, blob, tables, index))
        if "capture_work_items" in tables:
            for pending_work in index.matches("SELECT * FROM capture_work_items WHERE state!='terminal'", response, blob, broad=True):
                if _json(pending_work["envelope_json"]) is None:
                    reasons.append("capture_work_reference_unparseable")
                linked_attempt = False
                if {"fetch_request_executions", "fetch_request_batches"} <= tables and attempt_id is not None:
                    linked_attempt = conn.execute("""SELECT 1 FROM fetch_request_executions e
                        JOIN fetch_request_batches b ON b.id=e.batch_id WHERE e.fetch_attempt_id=? AND b.work_id=?""",
                        (attempt_id, pending_work["id"])).fetchone() is not None
                if linked_attempt or _reference(_json(pending_work["envelope_json"]), response, blob):
                    reasons.append("nonterminal_capture_work")
        if {"fetch_dead_letters", "capture_work_items"} <= tables:
            for dead in index.matches("""SELECT d.envelope_json,w.account_id,w.content_id FROM fetch_dead_letters d
                JOIN capture_work_items w ON w.id=d.work_id""", response, blob, broad=True):
                if (dead.get("content_id") is not None and dead["content_id"] == response.get("content_id") or
                    dead.get("account_id") is not None and dead["account_id"] == response.get("account_id") or
                    _reference(_json(dead["envelope_json"]), response, blob)):
                    reasons.append("dead_letter_requires_explicit_resolution")
        for table in ("scheduler_runs", "scheduler_run_attempts"):
            if table not in tables:
                continue
            for run in index.matches(f"SELECT status,details_json FROM {table}", response, blob, broad=True):
                details = _json(run["details_json"])
                if not isinstance(details, dict):
                    reasons.append("invalid_scheduler_checkpoint")
                elif _reference(details, response, blob, broad=run["status"] not in {"succeeded", "skipped"}):
                    cp = details.get("checkpoint", {})
                    if not isinstance(cp, dict):
                        reasons.append("invalid_scheduler_checkpoint")
                        continue
                    if (run["status"] not in {"succeeded", "skipped"} or cp.get("pending_raw") or
                        cp.get("pending_materialization") or cp.get("pending_page") or cp.get("complete") is not True):
                        reasons.append("scheduler_raw_or_materialization_checkpoint")
        if "operational_alerts" in tables:
            for alert in index.matches("SELECT scope_json,evidence_json FROM operational_alerts WHERE status='open'", response, blob, broad=True):
                if any(_json(value) is None for value in alert.values()):
                    reasons.append("alert_reference_unparseable")
                if any(_reference(_json(value), response, blob, broad=True) for value in alert.values()):
                    reasons.append("open_quality_or_incident_alert")
        # These are active physical-evidence contracts, not ordinary metric FKs.
        proof_tables = {"deployment_readiness_receipts": ("payload_json", "id IN (SELECT max(id) FROM deployment_readiness_receipts WHERE status!='failed' GROUP BY status)"),
            "provider_readiness_receipts": ("evidence_json", "status='ready' AND julianday(expires_at)>julianday('" + at + "')"),
            "capture_completion_events": ("payload_json", "state='completed' AND id=(SELECT max(id) FROM capture_completion_events)")}
        for table, (column, where) in proof_tables.items():
            if table in tables:
                for proof in index.matches(f"SELECT {column} FROM {table} WHERE {where}", response, blob):
                    if _json(proof[column]) is None:
                        reasons.append("active_proof_reference_unparseable")
                    if _reference(_json(proof[column]), response, blob):
                        reasons.append("active_release_or_continuity_proof")
        reasons.extend(_release_proof_reasons(conn, response, blob, tables=tables, at=at))
        reasons.extend(_media_proof_reasons(conn, response, blob, tables=tables, index=index))
        if "media_processing_slots" in tables and response.get("content_id") is not None:
            if conn.execute("SELECT 1 FROM media_processing_slots WHERE content_id=? AND status NOT IN ('succeeded','terminal_failed') LIMIT 1", (response["content_id"],)).fetchone():
                reasons.append("active_media_materialization")
        # Current media and sealed bundle proofs are discovered by explicit
        # raw identity/hash/path, including nested discovery-source references.
        for table in tables:
            if not (table.startswith("media_") or table.startswith("content_field") or table == "evidence_artifacts"):
                continue
            columns = _columns(conn, table)
            json_columns = sorted(col for col in columns if col.endswith("_json"))
            reference_columns = sorted(columns & {"raw_response_id", "source_raw_response_id", "source_sha256"})
            if not json_columns and not reference_columns:
                continue
            for item in index.matches(f'SELECT * FROM "{table}"', response, blob):
                if _reference(item, response, blob) or any(_reference(_json(item[col]), response, blob) for col in json_columns):
                    # Non-media scalar field provenance is not a physical consumer.
                    if table.startswith("content_field") and item.get("field_name", item.get("field")) not in {None, "media_source"}:
                        continue
                    reasons.append("media_source_or_sealed_completion_dependency")
    return sorted(set(reasons))


def _release_proof_reasons(conn: sqlite3.Connection, response: dict[str, Any], blob: dict[str, Any], *, tables: set[str], at: str) -> list[str]:
    reasons: list[str] = []
    if {"transport_continuity_permit_members", "transport_continuity_permits"} <= tables:
        for permit in archive._rows(conn, "SELECT * FROM transport_continuity_permits WHERE julianday(expires_at)>julianday(?)", (at,)):
            scope = response.get("paid_scope_identity")
            if scope is not None and conn.execute("SELECT 1 FROM transport_continuity_permit_members WHERE permit_id=? AND request_scope_identity=?", (permit["id"], scope)).fetchone():
                reasons.append("active_continuity_member")
            if _reference(_json(permit["payload_json"]), response, blob):
                reasons.append("active_continuity_proof")
            joins = {"provider_paid_scope_claims", "provider_request_start_events", "paid_provider_dispatch_events"}
            if joins <= tables and conn.execute("""SELECT 1 FROM transport_continuity_permit_members m
                JOIN provider_paid_scope_claims c ON c.scope_identity=m.request_scope_identity AND c.scope_kind='request' AND c.sequence=0
                JOIN provider_request_start_events e ON e.request_scope_claim_id=c.id
                JOIN paid_provider_dispatch_events sent ON sent.id=e.provider_send_marker_id
                JOIN paid_provider_dispatch_events d ON d.dispatch_id=sent.dispatch_id AND d.event_type='succeeded'
                WHERE m.permit_id=? AND d.raw_response_id=? LIMIT 1""", (permit["id"], response["id"])).fetchone():
                reasons.append("active_continuity_member")
    if {"scheduler_runs", "paid_provider_dispatch_events"} <= tables and "job_id" in _columns(conn, "scheduler_runs"):
        # Only the newest cohort per operation is current. Historic cohorts do
        # not create an eternal sampling rule for ordinary response bodies.
        cohorts: dict[str, dict[str, Any]] = {}
        for row in archive._rows(conn, "SELECT details_json FROM scheduler_runs WHERE job_id='transport_receipt:cohort' ORDER BY id DESC"):
            details = _json(row["details_json"])
            payload = details.get("payload") if isinstance(details, dict) else None
            if (not isinstance(payload, dict) or payload.get("contract_version") != "capture-operation-cohort-v1" or
                type(payload.get("start_high_watermark")) is not int or not isinstance(payload.get("operation"), str)):
                reasons.append("current_cohort_reference_unparseable")
                continue
            cohorts.setdefault(payload["operation"], payload)
        for operation, cohort in cohorts.items():
            if conn.execute("""SELECT 1 FROM (SELECT dispatch_id FROM paid_provider_dispatch_events
                WHERE id>? AND lower(provider)='tikhub' AND operation=? AND event_type='send_marked' ORDER BY id LIMIT 200) first
                JOIN paid_provider_dispatch_events terminal ON terminal.dispatch_id=first.dispatch_id
                WHERE terminal.raw_response_id=? LIMIT 1""", (cohort["start_high_watermark"], operation, response["id"])).fetchone():
                reasons.append("current_operation_qualification_cohort")
    return reasons


def _media_proof_reasons(conn: sqlite3.Connection, response: dict[str, Any], blob: dict[str, Any], *, tables: set[str], index: _ReferenceIndex) -> list[str]:
    if "evidence_artifacts" not in tables or "artifact_type" not in _columns(conn, "evidence_artifacts"):
        return []
    if index.media is None:
        index.media = {}
    artifact_columns = _columns(conn, "evidence_artifacts")
    join_content = "content_items" in tables and "content_id" in artifact_columns and "account_id" in _columns(conn, "content_items")
    sql = """SELECT a.*,r.id AS source_response_id,r.source AS source_state,
        r.raw_stored_at AS source_stored_at,r.byte_size AS source_byte_size,
        b.hot_owned AS source_hot_owned,b.raw_stored_at AS source_blob_stored_at,
        b.codec_version AS source_codec_version,b.entity_size AS source_entity_size"""
    if join_content:
        sql += ",c.account_id"
    sql += """ FROM evidence_artifacts a LEFT JOIN provider_raw_responses r ON r.id=
        CASE WHEN json_valid(a.metadata_json) THEN json_extract(a.metadata_json,'$.raw_response_id') END
        LEFT JOIN provider_raw_blobs b ON b.id=r.raw_blob_id"""
    if join_content:
        sql += " LEFT JOIN content_items c ON c.id=a.content_id"
    sql += " WHERE a.artifact_type IN ('media_source','media_lifecycle_manifest','media_completion_receipt')"
    # Valid sealed-file references can cross content/account boundaries. Parse
    # each once and index their exact raw IDs; only unknown proofs are scoped.
    if index.media_index_complete:
        reasons = index.media.get(response["id"], set()) | index.media_unknown
        for key in ("content_id", "account_id"):
            if type(response.get(key)) is int:
                reasons |= index.media_scoped_unknown.get((key, response[key]), set())
        return sorted(reasons)
    selected = archive._rows(conn, sql)
    byte_budget = 64 * 1024 * 1024
    file_budget = 4096
    sources_read: set[int] = set()
    # Metadata rows are cheap exact-ID provenance, not physical file reads.
    # A large historical media inventory must not exhaust the I/O budget.
    def unknown(reason: str, artifact: dict[str, Any]) -> None:
        scopes = [(key, artifact[key]) for key in ("content_id", "account_id") if type(artifact.get(key)) is int]
        if scopes:
            for key in scopes:
                index.media_scoped_unknown.setdefault(key, set()).add(reason)
        else:
            index.media_unknown.add(reason)
    for item in selected:
        if item["id"] in index.media_processed:
            continue
        index.media_processed.add(item["id"])
        metadata = _json(item["metadata_json"])
        if not isinstance(metadata, dict):
            unknown("media_reference_unparseable", item)
            continue
        source_id = metadata.get("raw_response_id")
        if type(source_id) is int:
            index.media.setdefault(source_id, set()).add("media_source_or_sealed_completion_dependency")
        if item["artifact_type"] == "media_source":
            if type(source_id) is int:
                try:
                    if item["source_response_id"] is None:
                        raise archive.RawArchiveError("media source response is missing")
                    legacy = (item["source_hot_owned"] == 0 and item["source_stored_at"] is None
                              and item["source_blob_stored_at"] is None
                              and str(item["source_codec_version"]).startswith("legacy-"))
                    # Migration froze these original bytes and their old ancestry
                    # before any new owned response existed. Keep their IDs, but
                    # never reread thousands of historical bodies to decide the
                    # lifetime of newly-owned inventory. They remain undeletable.
                    if legacy or source_id in sources_read:
                        continue
                    sources_read.add(source_id)
                    # Ordinary native detail has no derived-discovery ancestry.
                    if item["source_state"] == "derived_applied":
                        entity_size = int(item["source_entity_size"] if item["source_entity_size"] is not None else item["source_byte_size"])
                        if entity_size > 16 * 1024 * 1024:
                            raise archive.RawArchiveError("media derivation exceeds bounded proof read")
                        byte_budget -= entity_size
                        file_budget -= 1
                        if byte_budget < 0 or file_budget < 0:
                            index.media_unknown.add("media_reference_index_budget_exceeded")
                            break
                        source = _json(archive.read_response_entity(conn, source_id).decode())
                        if not isinstance(source, dict):
                            raise archive.RawArchiveError("media derivation is invalid")
                        parent = source.get("source_raw_response_id")
                        if type(parent) is int:
                            index.media.setdefault(parent, set()).add("media_discovery_ancestry_dependency")
                except (archive.RawArchiveError, OSError, UnicodeError):
                    unknown("media_discovery_ancestry_unreadable", item)
            continue
        try:
            path = archive._source_path(item["local_path"])
            archive._safe_existing(path)
            byte_budget -= path.stat().st_size
            file_budget -= 1
            if byte_budget < 0 or file_budget < 0:
                index.media_unknown.add("media_reference_index_budget_exceeded")
                break
            body = archive._read_bytes(path, limit=16 * 1024 * 1024)
            if item.get("sha256") and hashlib.sha256(body).hexdigest() != item["sha256"]:
                raise archive.RawArchiveError("media proof checksum changed")
            value = _json(body.decode())
            if not isinstance(value, dict):
                raise archive.RawArchiveError("media proof JSON invalid")
            raw_ids = {number for key, number in _reference_keys(value) if key in {"raw_response_id", "source_raw_response_id", "raw_id"} and type(number) is int}
            refs = value.get("database_refs", [])
            if not isinstance(refs, list):
                raise archive.RawArchiveError("media proof references invalid")
            raw_ids.update(ref["id"] for ref in refs if isinstance(ref, dict) and ref.get("table") == "provider_raw_responses" and type(ref.get("id")) is int)
            for raw_id in raw_ids:
                index.media.setdefault(raw_id, set()).add("media_source_or_sealed_completion_dependency")
        except (archive.RawArchiveError, OSError, UnicodeError):
            unknown("media_completion_proof_unreadable", item)
    index.media_index_complete = True
    reasons = index.media.get(response["id"], set()) | index.media_unknown
    for key in ("content_id", "account_id"):
        if type(response.get(key)) is int:
            reasons |= index.media_scoped_unknown.get((key, response[key]), set())
    return sorted(reasons)


def _file_evidence(path: Path, *, root: Path, checksum: str, size: int) -> dict[str, Any]:
    path = path.absolute()
    if not path.is_relative_to(root) or path == root:
        raise archive.RawArchiveError("local retirement path is outside live root")
    archive._safe_existing(path)
    for parent in path.parents:
        metadata = parent.stat()
        if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.geteuid() or metadata.st_mode & 0o022:
            raise archive.RawArchiveError("local retirement parent ownership is unproven")
        if parent == root:
            break
    metadata = path.stat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid() or metadata.st_nlink != 1:
        raise archive.RawArchiveError("local retirement requires owned single-link regular file")
    body = archive._read_bytes(path)
    if len(body) != size or hashlib.sha256(body).hexdigest() != checksum:
        raise archive.RawArchiveError("local retirement checksum or size changed")
    after = path.stat()
    if (metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise archive.RawArchiveError("local retirement inode changed during read")
    return {"path": str(path), "sha256": checksum, "size": size,
            "device": metadata.st_dev, "inode": metadata.st_ino,
            "parent_device": path.parent.stat().st_dev, "parent_inode": path.parent.stat().st_ino}


def _owned_paths(conn: sqlite3.Connection, blob: dict[str, Any], *, root: Path) -> list[dict[str, Any]]:
    expected = root / blob["entity_sha256"][:2] / f'{blob["entity_sha256"]}.{archive.CODEC_VERSION}.zst'
    if Path(blob["hot_path"]) != expected or blob["codec_version"] != archive.CODEC_VERSION:
        raise archive.RawArchiveError("local retirement requires an owned canonical CAS path")
    entity = archive.read_blob(conn, blob["id"])
    paths = {str(expected): _file_evidence(expected, root=root, checksum=blob["stored_sha256"], size=blob["stored_size"])}
    for response in archive._rows(conn, "SELECT * FROM provider_raw_responses WHERE raw_blob_id=?", (blob["id"],)):
        path = archive._source_path(response["local_path"]).absolute()
        if str(path) in paths:
            continue
        # Never invent ownership for a historical or external loose file.
        if response.get("raw_stored_at") is None or not path.is_relative_to(root):
            raise archive.RawArchiveError("loose response file ownership is unproven")
        loaded = raw_evidence.read_raw_evidence(archive._safe_existing(path), expected_stored_sha256=response["sha256"], expected_stored_size=response["byte_size"])
        if loaded.entity_bytes != entity:
            raise archive.RawArchiveError("loose response differs from owned blob")
        paths[str(path)] = _file_evidence(path, root=root, checksum=response["sha256"], size=response["byte_size"])
    return [paths[key] for key in sorted(paths)]


def _unlink_checked(evidence: dict[str, Any], *, root: Path) -> int:
    path = Path(evidence["path"])
    if not path.is_relative_to(root):
        raise archive.RawArchiveError("committed cleanup path escaped live root")
    archive._safe_existing(path.parent)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        parent = os.fstat(directory)
        if (parent.st_dev, parent.st_ino) != (evidence["parent_device"], evidence["parent_inode"]):
            raise archive.RawArchiveError("committed cleanup parent changed")
        try:
            metadata = os.stat(path.name, dir_fd=directory, follow_symlinks=False)
        except FileNotFoundError:
            return 0  # Crash after unlink but before completion event.
        if (not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1 or metadata.st_uid != os.geteuid() or
            (metadata.st_dev, metadata.st_ino) != (evidence["device"], evidence["inode"])):
            raise archive.RawArchiveError("committed cleanup inode or ownership changed")
        _file_evidence(path, root=root, checksum=evidence["sha256"], size=evidence["size"])
        current = os.stat(path.name, dir_fd=directory, follow_symlinks=False)
        if (current.st_dev, current.st_ino, current.st_mtime_ns) != (metadata.st_dev, metadata.st_ino, metadata.st_mtime_ns):
            raise archive.RawArchiveError("committed cleanup target changed")
        os.unlink(path.name, dir_fd=directory)
        os.fsync(directory)
        return int(evidence["size"])
    finally:
        os.close(directory)


def _cleanup(conn: sqlite3.Connection, blob_id: int, *, root: Path) -> dict[str, Any]:
    # The retirement intent was committed earlier. Keep this separate writer
    # transaction through physical unlink so no other connection can publish a
    # rehydrated current body between state verification and deletion.
    with archive._transaction(conn):
        return _cleanup_locked(conn, blob_id, root=root)


def _cleanup_locked(conn: sqlite3.Connection, blob_id: int, *, root: Path) -> dict[str, Any]:
    blob = archive._row(conn, "provider_raw_blobs", blob_id)
    event = archive._rows(conn, """SELECT id,event_type,evidence_json FROM raw_retention_events
        WHERE raw_blob_id=? AND event_type IN (?,?,'blob_rehydrated') ORDER BY id DESC LIMIT 1""", (blob_id, INTENT, COMPLETED))
    if blob["hot_state"] != "deleted" or not event or event[0]["event_type"] != INTENT:
        raise archive.RawArchiveError("local cleanup requires current committed retirement intent")
    intent = json.loads(event[0]["evidence_json"])
    meta = archive._safe_existing(root).stat()
    if (intent["root"], intent["root_device"], intent["root_inode"]) != (str(root), meta.st_dev, meta.st_ino):
        raise archive.RawArchiveError("local cleanup root identity changed")
    removed = sum(_unlink_checked(item, root=root) for item in intent["paths"])
    result = {"blob_id": blob_id, "status": "retired", "removed_bytes": removed, "ledger_preserved": True}
    archive._event(conn, COMPLETED, "D8_physical_cleanup", blob_id=blob_id, evidence={**result, "intent_event_id": event[0]["id"]})
    return result


def maintenance_tick(conn: sqlite3.Connection, *, at: str, live_root: Path,
                     daily_stored_p95: int = 0, max_blobs: int = 256) -> dict[str, Any]:
    """Bounded local cleanup. Capacity pressure never overrides evidence holds."""
    if conn.in_transaction:
        raise archive.RawArchiveError("local retention requires standalone writer connection")
    if type(max_blobs) is not int or not 1 <= max_blobs <= 4096:
        raise ValueError("local retention batch is outside fixed bounds")
    archive.day_for_timestamp(at)
    result: dict[str, Any] = {"schema": MAINTENANCE_SCHEMA, "policy": POLICY, "at": at,
        "status": "complete", "retired": [], "protected": [], "errors": [],
        "provider_calls": 0, "ledger_preserved": True}
    try:
        storage = inspect_local_storage(live_root=live_root, daily_stored_p95=daily_stored_p95)
        result["storage"] = storage
        root = Path(storage["root"])
        # Pending cleanup comes first. A precommit crash has no intent/state change;
        # a postcommit crash repeats only exact paths already authorized in SQLite.
        blobs = archive._rows(conn, """SELECT b.* FROM provider_raw_blobs b WHERE
            (b.hot_owned=1 AND b.hot_state='present' AND b.raw_stored_at IS NOT NULL
             AND julianday(date(b.raw_stored_at,'+8 hours'))<=julianday(date(?,'+8 hours','-8 days')))
            OR (b.hot_state='deleted' AND (SELECT event_type FROM raw_retention_events e WHERE e.raw_blob_id=b.id
                AND e.event_type IN ('local_retention_retire_intent','local_retention_retired','blob_rehydrated')
                ORDER BY e.id DESC LIMIT 1)='local_retention_retire_intent')
            ORDER BY (b.hot_state='deleted') DESC,
            COALESCE((SELECT max(e.id) FROM raw_retention_events e WHERE e.raw_blob_id=b.id AND e.event_type='local_retention_checked'),0),b.raw_stored_at,b.id
            LIMIT ?""", (at, max_blobs))
        index: _ReferenceIndex | None = None
        index_version: int | None = None
        for blob in blobs:
            try:
                if blob["hot_state"] == "deleted":
                    result["retired"].append(_cleanup(conn, blob["id"], root=root))
                    continue
                with archive._transaction(conn):
                    # Other connections in this Writer may commit new consumers
                    # between blobs. Rebuild all cached indexes in the new lock
                    # snapshot; our own retention commits need no rebuilding.
                    current_version = int(conn.execute("PRAGMA data_version").fetchone()[0])
                    if index is None or index_version != current_version:
                        index = _ReferenceIndex(conn)
                        index_version = current_version
                    reasons = protection_reasons(conn, blob["id"], at=at, _index=index)
                    if reasons:
                        item = {"blob_id": blob["id"], "reasons": reasons}
                        result["protected"].append(item)
                        archive._event(conn, "local_retention_checked", "protected", blob_id=blob["id"], evidence=item)
                        continue
                    paths = _owned_paths(conn, blob, root=root)
                    intent = {"schema": "raw-local-retirement-intent-v1", "policy": POLICY,
                        "raw_blob_id": blob["id"], "at": at, "protection_checked": True,
                        "root": str(root), "root_device": storage["device"], "root_inode": storage["inode"], "paths": paths}
                    archive._event(conn, INTENT, "D8_processed_accounted_unreferenced", blob_id=blob["id"], evidence=intent)
                    conn.execute("UPDATE provider_raw_blobs SET hot_state='deleted' WHERE id=?", (blob["id"],))
                result["retired"].append(_cleanup(conn, blob["id"], root=root))
            except (raw_evidence.RawEvidenceError, OSError, sqlite3.Error) as error:
                result["errors"].append({"blob_id": blob["id"], "reason": str(error), "error_class": type(error).__name__})
                result["status"] = "blocked"
    except (raw_evidence.RawEvidenceError, OSError, sqlite3.Error) as error:
        result.update(status="blocked", reason=str(error), error_class=type(error).__name__)
    if "storage" in result:
        try:
            result["storage_after"] = inspect_local_storage(live_root=live_root, daily_stored_p95=daily_stored_p95)
        except (archive.RawArchiveError, OSError) as error:
            result.update(status="blocked", reason=str(error), error_class=type(error).__name__)
    with archive._transaction(conn):
        archive._event(conn, "local_maintenance_receipt", result["status"], evidence=result)
    return result
