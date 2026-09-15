"""Local content recovery from an original, fully verified quarantined gzip.

Original HTTP/paid evidence is immutable. A separate durable owner applies the
already-received body and appends its result. This module never issues HTTP.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import sqlite3
from threading import Event, Thread
from typing import Any, Iterator

from . import capture, capture_planning as planning, durable_runs, provider_budget, provider_transport, raw_archive
from .runtime_database import require_current_process_writer_lock
from .storage import connect, now_utc, transaction

CONTRACT = "content-entity-recovery-v1"
JOB = "content_entity_recovery"
# Local replay currently applies a detail and its metric projection. Statistics
# responses need their own stage application and are not an offline repair input.
RECOVERABLE_OPERATIONS = frozenset(op for op in provider_transport.CONTENT_ENTITY_OPERATIONS if op.endswith("_detail"))
_OWNER: ContextVar[dict[str, Any] | None] = ContextVar("local_entity_recovery_owner", default=None)


class EntityRecoveryError(ValueError):
    error_code = "content_entity_recovery_invalid"


def _require(value: Any, message: str) -> None:
    if not value:
        raise EntityRecoveryError(message)


def _row(connection: sqlite3.Connection, table: str, identifier: int) -> dict:
    row = connection.execute("SELECT * FROM " + table + " WHERE id=?", (identifier,)).fetchone()
    _require(row is not None, "recovery source row is missing: " + table)
    return dict(row)


def _file_identity(path: Path) -> tuple:
    stat = path.lstat()
    _require(path.is_file() and not path.is_symlink() and stat.st_nlink == 1,
             "quarantine must remain one regular file")
    return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)


def _key(attempt_id: int, receipt: dict) -> str:
    return planning.digest({"contract_version": CONTRACT, "original_fetch_attempt_id": attempt_id,
        "original_transport_receipt_sha256": receipt["receipt_sha256"],
        "http_encoded_sha256": receipt["encoded_sha256"]})


def _read_phase(connection: sqlite3.Connection, key: str, phase: str) -> dict | None:
    rows = connection.execute("SELECT * FROM data_quality_receipts WHERE scope_key=?",
                              (f"{CONTRACT}:{key}:{phase}",)).fetchall()
    _require(len(rows) <= 1, "recovery phase has conflicting receipts")
    if not rows:
        return None
    row = dict(rows[0])
    payload = json.loads(row["payload_json"])
    _require(payload.get("contract_version") == CONTRACT and payload.get("recovery_key") == key
        and payload.get("phase") == phase and planning.digest(payload) == row["receipt_sha256"],
        "recovery receipt identity or digest changed")
    return {**row, "payload": payload}


def _append_phase(connection: sqlite3.Connection, payload: dict) -> dict:
    prior = _read_phase(connection, payload["recovery_key"], payload["phase"])
    if prior is not None:
        _require(prior["payload"] == payload, "recovery phase conflicts with existing evidence")
        return prior
    at = now_utc()
    connection.execute("INSERT INTO data_quality_receipts(scope_key,cutoff_at,payload_json,recorded_at,receipt_sha256) VALUES(?,?,?,?,?)",
        (f"{CONTRACT}:{payload['recovery_key']}:{payload['phase']}", payload["source_captured_at"],
         planning.canonical(payload), at, planning.digest(payload)))
    result = _read_phase(connection, payload["recovery_key"], payload["phase"])
    assert result is not None
    return result


def _source(connection: sqlite3.Connection, work_id: int, attempt_id: int) -> dict:
    """Only read exact original identities; no eligibility or payment retry."""
    work = _row(connection, "capture_work_items", work_id)
    _require(work["content_id"] is not None and work["operation"] in RECOVERABLE_OPERATIONS
        and not work["owner_token"] and (work["state"] == "paid_identity_hold"
        or work["state"] == "terminal" and work["reason"] == "recovered"), "source work is not an unowned content hold")
    content = _row(connection, "content_items", work["content_id"])
    identities = connection.execute("SELECT * FROM account_platform_identities WHERE account_id=? AND platform=?",
                                    (content["account_id"], content["platform"])).fetchall()
    _require(len(identities) == 1 and str(identities[0]["uid"]) == str(content["raw_account_uid"]),
             "content author is not the uniquely bound account")
    identity = dict(identities[0])
    envelope = json.loads(work["envelope_json"])
    attempt = _row(connection, "fetch_attempts", attempt_id)
    _require(attempt["slot_id"] is None and attempt["request_batch_id"] is not None
        and attempt["error_code"] == "transport_error", "recovery requires the original failed singleton send")
    batch = _row(connection, "fetch_request_batches", attempt["request_batch_id"])
    members = connection.execute("SELECT * FROM fetch_request_batch_members WHERE batch_id=?", (batch["id"],)).fetchall()
    _require(len(members) == 1, "recovery is only for one original content member")
    member = dict(members[0])
    _require(member["content_id"] == content["id"] and member["account_id"] == content["account_id"]
        and batch["operation"] == work["operation"] and batch["work_id"] is None
        and connection.execute("SELECT 1 FROM fetch_request_executions WHERE batch_id=? AND fetch_attempt_id=?",
                               (batch["id"], attempt_id)).fetchone(), "recovery request/member lineage differs")
    markers = connection.execute("SELECT * FROM paid_provider_dispatch_events WHERE fetch_attempt_id=? AND event_type='send_marked'",
                                 (attempt_id,)).fetchall()
    _require(len(markers) == 1, "recovery requires exactly one original send marker")
    sent = dict(markers[0])
    usage = _row(connection, "provider_usage", sent["provider_usage_id"])
    details = json.loads(usage["details_json"])
    original_run = _row(connection, "scheduler_runs", sent["scheduler_run_id"])
    _require(json.loads(original_run["details_json"]).get("identity", {}).get("work_identity") == work["work_identity"]
        and usage["task_id"] == envelope.get("task_id") and usage["request_attempts"] == 1
        and usage["operation"] == work["operation"] and usage["provider"].lower() == "tikhub"
        and details.get("paid_scope_identity") == batch["request_scope_identity"]
        and details.get("paid_sequence") == batch["sequence"], "recovery is not the original work's paid request")
    from .paid_identity import build_paid_request_identity
    document = details.get("paid_identity", {})
    request = build_paid_request_identity(**{key: document[key] for key in (
        "provider", "operation", "platform", "subject", "request_parameters", "cursor", "due_bucket", "request_window")},
        sequence=batch["sequence"])
    _require(request.document == document and request.scope_identity == batch["request_scope_identity"]
        and planning.canonical(document["request_parameters"]) == batch["parameters_json"]
        and document["subject"] == content["platform_content_id"]
        and document["platform"] == content["platform"], "original paid identity or parameters changed")
    slot = _row(connection, "fetch_slots", sent["fetch_slot_id"])
    _require(slot["content_id"] == content["id"] and slot["provider"].lower() == "tikhub",
             "original logical slot belongs to another content")
    dispositions = connection.execute("SELECT * FROM fetch_request_member_dispositions WHERE member_id=?", (member["id"],)).fetchall()
    _require(len(dispositions) == 1 and dispositions[0]["disposition"] == "unusable"
        and dispositions[0]["raw_response_id"] is None, "original unusable disposition changed")
    disposition = dict(dispositions[0])
    receipt_row = connection.execute("SELECT * FROM fetch_transport_receipts WHERE fetch_attempt_id=?", (attempt_id,)).fetchone()
    _require(receipt_row is not None, "original transport receipt is missing")
    receipt = dict(receipt_row)
    payload = json.loads(receipt["payload_json"])
    _require(hashlib.sha256(raw_archive.raw_evidence.canonical_json_bytes(payload)).hexdigest() == receipt["receipt_sha256"] and payload.get("fetch_attempt_id") == attempt_id,
             "original transport receipt digest changed")
    transport = payload["transport"]
    quarantine = connection.execute("SELECT * FROM transport_quarantine_members WHERE transport_receipt_id=?", (receipt["id"],)).fetchone()
    _require(quarantine is not None and quarantine["path"] == transport.get("quarantine_path")
        and quarantine["sha256"] == transport.get("http_encoded_sha256")
        and quarantine["byte_size"] == transport.get("http_encoded_bytes"), "quarantine binding differs")
    proof = {"contract_version": CONTRACT, "recovery_key": _key(attempt_id, receipt),
        "original_work_id": work_id, "original_fetch_attempt_id": attempt_id, "original_slot_id": slot["id"],
        "request_batch_id": batch["id"], "member_id": member["id"],
        "original_disposition_id": disposition["id"], "original_disposition_sha256": planning.digest(disposition),
        "transport_receipt_id": receipt["id"], "original_transport_receipt_sha256": receipt["receipt_sha256"],
        "quarantine_member_id": quarantine["id"], "provider": "TikHub", "operation": work["operation"],
        "paid_scope_identity": request.scope_identity, "sequence": request.sequence,
        "usage_id": usage["id"], "send_marker_id": sent["id"], "content_id": content["id"],
        "account_id": content["account_id"], "identity_id": identity["id"], "platform": content["platform"],
        "platform_content_id": content["platform_content_id"], "expected_author_uid": str(identity["uid"]),
        "http_status": transport.get("http_status"), "http_encoded_sha256": quarantine["sha256"],
        "http_encoded_bytes": quarantine["byte_size"], "source_captured_at": transport["response_finished_at"],
        "source_business_day": work["data_business_day"], "original_error_code": transport.get("error_code"),
        "clean_eof": False}
    return {"proof": proof, "work": work, "content": content, "slot": slot, "attempt": attempt,
            "transport": transport, "quarantine": dict(quarantine)}


@dataclass(frozen=True)
class PreparedEntityRecovery:
    source: dict
    response: provider_transport.JsonTransportResult
    parsed: capture.ProviderResult
    file_identity: tuple


def prepare_entity_recovery(connection: sqlite3.Connection, *, work_id: int, fetch_attempt_id: int) -> PreparedEntityRecovery:
    _require(not connection.in_transaction, "body validation must precede a write transaction")
    source = _source(connection, work_id, fetch_attempt_id)
    path = Path(source["quarantine"]["path"])
    identity = _file_identity(path)
    encoded = raw_archive._read_bytes(path, provider_transport.DEFAULT_MAX_ENCODED_BYTES)
    response = provider_transport.validate_saved_content_entity(encoded, source["transport"], operation=source["proof"]["operation"])
    _require(_file_identity(path) == identity, "quarantine changed during verification")
    from . import providers
    content = source["content"]
    parsed = providers._parse_content_payload(content["platform"], "detail", content["platform_content_id"],
        content["content_type"], response.payload, status=response.status, expected_uid=source["proof"]["expected_author_uid"])
    _require(parsed.data.get("content_type") in {"video", "image"}, "restored detail has no supported content type")
    source["proof"].update(entity_sha256=hashlib.sha256(response.entity_body).hexdigest(), entity_bytes=len(response.entity_body),
        entity_integrity_basis="gzip_single_member_crc", parser_version=CONTRACT,
        parser_result_sha256=planning.digest(parsed.data), business_identity_verified=True,
        quarantine_file_identity=list(identity))
    return PreparedEntityRecovery(source, response, parsed, identity)


def _verify_prepared(connection: sqlite3.Connection, prepared: PreparedEntityRecovery) -> None:
    proof = prepared.source["proof"]
    current = _source(connection, proof["original_work_id"], proof["original_fetch_attempt_id"])
    _require(all(proof[key] == value for key, value in current["proof"].items()), "original recovery identity changed")
    _require(_file_identity(Path(current["quarantine"]["path"])) == prepared.file_identity, "verified quarantine file changed")


def assert_local_recovery_owner(connection: sqlite3.Connection, *, content_id: int | None = None,
                                raw_response_id: int | None = None, required: bool = False) -> None:
    owner = _OWNER.get()
    if owner is None:
        _require(not required, "local recovery requires an owned durable repair attempt")
        return
    require_current_process_writer_lock(connection)
    claim = owner["claim"]
    prepared = owner["prepared"]
    proof = prepared.source["proof"]
    details = durable_runs.assert_owner(connection, claim)
    _require(details["identity"]["recovery_key"] == proof["recovery_key"], "repair owner has a different recovery identity")
    durable_runs.heartbeat(connection, claim, now=now_utc())
    provider_budget.assert_paid_scope_owner(connection)
    _verify_prepared(connection, prepared)
    _require(content_id in (None, proof["content_id"]), "recovery owner content changed")
    if raw_response_id is not None:
        raw = _row(connection, "provider_raw_responses", raw_response_id)
        _require(raw["content_id"] == proof["content_id"] and raw["fetch_attempt_id"] == proof["original_fetch_attempt_id"]
            and raw["paid_scope_identity"] == proof["paid_scope_identity"] and raw["sequence"] == proof["sequence"],
            "recovery owner raw target changed")


@contextmanager
def local_recovery_owner_context(prepared: PreparedEntityRecovery, claim: durable_runs.DurableClaim) -> Iterator[None]:
    _require(_OWNER.get() is None, "nested entity recovery is not permitted")
    token = _OWNER.set({"prepared": prepared, "claim": claim})
    try:
        yield
    finally:
        _OWNER.reset(token)


def recovery_storage_identity(connection: sqlite3.Connection, *, key: str, claim: capture.SlotClaim,
                              entity_bytes: bytes) -> str:
    assert_local_recovery_owner(connection, content_id=claim.content_id, required=True)
    owner = _OWNER.get()
    assert owner is not None
    proof = owner["prepared"].source["proof"]
    _require(key == proof["recovery_key"] and claim.attempt_id == proof["original_fetch_attempt_id"]
        and claim.paid_scope_identity == proof["paid_scope_identity"] and claim.paid_sequence == proof["sequence"]
        and entity_bytes is owner["prepared"].response.entity_body, "recovery storage identity differs")
    return proof["source_captured_at"]


def _pending_metadata(connection: sqlite3.Connection, raw_response_id: int) -> tuple[dict, dict]:
    raw = _row(connection, "provider_raw_responses", raw_response_id)
    receipt = _row(connection, "fetch_transport_receipts", raw["transport_receipt_id"])
    pending = _read_phase(connection, _key(raw["fetch_attempt_id"], receipt), "pending")
    _require(pending is not None, "failed transport has no verified entity recovery")
    proof = pending["payload"]
    source = _source(connection, proof["original_work_id"], proof["original_fetch_attempt_id"])
    _require(all(proof.get(key) == value for key, value in source["proof"].items())
        and proof["raw_response_id"] == raw_response_id and proof["original_fetch_attempt_id"] == raw["fetch_attempt_id"]
        and proof["transport_receipt_id"] == raw["transport_receipt_id"]
        and proof["paid_scope_identity"] == raw["paid_scope_identity"] and proof["sequence"] == raw["sequence"]
        and proof["operation"] == raw["operation"] and proof["content_id"] == raw["content_id"]
        and proof["business_identity_verified"] is True and proof["entity_integrity_basis"] == "gzip_single_member_crc",
        "recovery raw or original binding changed")
    blob = _row(connection, "provider_raw_blobs", raw["raw_blob_id"])
    _require(blob["entity_sha256"] == proof["entity_sha256"] and blob["entity_size"] == proof["entity_bytes"],
             "recovery blob registration changed")
    return pending, source


def required_recovery_quarantines(connection: sqlite3.Connection) -> list[dict]:
    """Return exact original files needed to read already recovered content."""
    if connection.execute("SELECT 1 FROM sqlite_master WHERE name='data_quality_receipts'").fetchone() is None:
        return []
    members = {}
    for row in connection.execute("SELECT payload_json FROM data_quality_receipts WHERE scope_key LIKE ?",
                                  (CONTRACT + ":%:pending",)):
        payload = json.loads(row[0])
        pending, source = _pending_metadata(connection, payload["raw_response_id"])
        _require(pending["payload"] == payload, "recovery pending receipt changed")
        member = source["quarantine"]
        _require(member["id"] not in members or members[member["id"]] == member,
                 "recovery quarantine registration conflicts")
        members[member["id"]] = member
    return list(members.values())


def _verified_pending(connection: sqlite3.Connection, raw_response_id: int) -> tuple[dict, dict]:
    pending, source = _pending_metadata(connection, raw_response_id)
    from . import artifact_paths
    original = Path(source["quarantine"]["path"])
    path = artifact_paths.resolve(original)
    registered = artifact_paths.replica_file(path)
    if registered is None:
        # Writer evidence retains its original inode and timestamps.
        _require(path == original and list(_file_identity(path)) == pending["payload"]["quarantine_file_identity"],
                 "recovery quarantine file generation changed")
    else:
        # A replica has different filesystem identities. Its sealed manifest
        # and original DB registration must bind the same encoded bytes.
        member = source["quarantine"]
        _require(all(registered.get(k) == member[k] for k in ("sha256", "byte_size")),
                 "replica recovery quarantine is not hash-bound")
        before = _file_identity(path)
        _require(before[2] == member["byte_size"] <= provider_transport.DEFAULT_MAX_ENCODED_BYTES,
                 "replica recovery quarantine bytes changed")
        # The receiver installs root-owned files for the unprivileged reader.
        # Reuse the manifest-bound replica verifier; the Writer-only raw reader
        # intentionally requires current-user ownership and must stay strict.
        from .media_lifecycle import _file as verify_replica_file
        verified = verify_replica_file(path)
        _require(before == _file_identity(path)
                 and all(verified[k] == member[k] for k in ("sha256", "byte_size")),
                 "replica recovery quarantine bytes changed")
    return pending, source


def verified_recovery_entity(connection: sqlite3.Connection, raw_response_id: int) -> dict:
    pending, source = _verified_pending(connection, raw_response_id)
    proof = pending["payload"]
    if not connection.in_transaction:
        entity = raw_archive.read_response_entity(connection, raw_response_id)
        _require(hashlib.sha256(entity).hexdigest() == proof["entity_sha256"] and len(entity) == proof["entity_bytes"],
                 "recovered raw blob differs from verified entity")
    # The immutable proof binds the initial strict gzip/parser validation. Later
    # readers verify its original/file generation and the content-addressed blob;
    # no second decompression of the HTTP gzip runs inside a Writer transaction.
    return {**source["transport"], "status": "succeeded", "error_code": None,
        "json_parse_ok": True, "json_parse_error": None, "gzip_crc_ok": True,
        "entity_complete": True, "entity_integrity_basis": "gzip_single_member_crc",
        "entity_validation_operation": proof["operation"], "framing_warning": "transport_incomplete_read",
        "entity_sha256": proof["entity_sha256"], "entity_bytes": proof["entity_bytes"],
        "recovered": True, "recovery_receipt_sha256": pending["receipt_sha256"]}


def recovered_member(connection: sqlite3.Connection, *, member_id: int, raw_response_id: int) -> bool:
    raw = _row(connection, "provider_raw_responses", raw_response_id)
    if raw["transport_receipt_id"] is None:
        return False
    receipt = _row(connection, "fetch_transport_receipts", raw["transport_receipt_id"])
    pending = _read_phase(connection, _key(raw["fetch_attempt_id"], receipt), "pending")
    if pending is None:
        return False
    verified, _ = _verified_pending(connection, raw_response_id)
    _require(verified["payload"]["member_id"] == member_id, "recovery belongs to another member")
    return True


def recover_content_entity(*, db_path: Path, work_id: int, fetch_attempt_id: int,
                           media_status: str = "media_source_unverified", media_root: Path | None = None) -> dict:
    """Restore one held content, without HTTP, under a newly claimed repair run."""
    _require(media_status in {"available", "media_source_unverified", "media_source_refresh_required"}, "invalid media evidence status")
    _require(provider_budget._SCOPE.get() == provider_budget.PaidScope(),
             "entity recovery must start outside another paid owner or authority scope")
    from . import providers, runtime_evidence_context
    from .capture_availability import record_detail_result
    with runtime_evidence_context.prepare_inheritance(db_path):
        with connect(db_path) as connection:
            require_current_process_writer_lock(connection)
            prepared = prepare_entity_recovery(connection, work_id=work_id, fetch_attempt_id=fetch_attempt_id)
            proof = prepared.source["proof"]
            key = proof["recovery_key"]
            done = _read_phase(connection, key, "done")
            if done is not None:
                verified_recovery_entity(connection, done["payload"]["raw_response_id"])
                return {**done["payload"], "idempotent": True}
        identity = {"contract_version": CONTRACT, "recovery_key": key, "purpose": "detail",
            "business_day": proof["source_business_day"], "content_id": proof["content_id"],
            "account_id": proof["account_id"], "identity_id": proof["identity_id"], "platform": proof["platform"],
            "uid": proof["expected_author_uid"], "original_work_id": work_id, "original_fetch_attempt_id": fetch_attempt_id}
        with connect(db_path) as connection, transaction(connection):
            _verify_prepared(connection, prepared)
            scheduled = "scan:" + durable_runs.scan_identity(JOB, {"contract_version": CONTRACT, "recovery_key": key})
            run = connection.execute("SELECT id FROM scheduler_runs WHERE job_id=? AND scheduled_for=?", (JOB, scheduled)).fetchone()
            if run is not None:
                durable_runs.recover_expired_leases(connection, owned_run_ids=[run["id"]])
            claim = durable_runs.claim_run_in_transaction(connection, JOB, identity, invocation_source="operator_retry",
                scope_key={"contract_version": CONTRACT, "recovery_key": key})
            _require(claim is not None, "entity recovery is already owned or completed")
        assert claim is not None
        stopped = Event()
        failures: list[Exception] = []
        def maintain():
            while not stopped.wait(durable_runs.HEARTBEAT_SECONDS):
                try:
                    with connect(db_path) as connection, transaction(connection, priority="heartbeat"):
                        if not stopped.is_set():
                            durable_runs.assert_owner(connection, claim)
                            durable_runs.heartbeat(connection, claim)
                except Exception as error:
                    failures.append(error)
                    return
        worker = Thread(target=maintain, name="entity-recovery-lease", daemon=True)
        worker.start()
        try:
            with provider_budget.paid_scope("detail", scheduler_run_id=claim.scheduler_run_id,
                    scheduler_attempt_id=claim.attempt_id, business_day=proof["source_business_day"]), \
                    local_recovery_owner_context(prepared, claim):
                with connect(db_path) as connection, transaction(connection):
                    assert_local_recovery_owner(connection, required=True)
                    pending = _read_phase(connection, key, "pending")
                    if pending is None:
                        original = prepared.source["attempt"]
                        slot = prepared.source["slot"]
                        source_claim = capture.SlotClaim(slot_id=slot["id"], attempt_id=fetch_attempt_id,
                            attempt_number=original["attempt_number"], content_id=proof["content_id"],
                            account_id=None, stage=slot["stage"], window_key=slot["window_key"], provider="TikHub",
                            adapter_version=slot["adapter_version"], paid_scope_identity=proof["paid_scope_identity"],
                            paid_sequence=proof["sequence"], request_batch_id=proof["request_batch_id"], singleton_batch=True)
                        path = Path(prepared.source["quarantine"]["path"])
                        _require(path.parents[2].name == "quarantine", "noncanonical quarantine root")
                        raw_id = capture._store_raw_response(connection, claim=source_claim, operation=proof["operation"],
                            value=prepared.response.payload, http_status=proof["http_status"], raw_root=path.parents[3],
                            entity_bytes=prepared.response.entity_body, entity_recovery_key=key)
                        connection.execute("UPDATE provider_raw_responses SET transport_receipt_id=? WHERE id=? AND transport_receipt_id IS NULL",
                                           (proof["transport_receipt_id"], raw_id))
                        pending = _append_phase(connection, {**proof, "phase": "pending", "raw_response_id": raw_id,
                            "repair_run_id": claim.scheduler_run_id, "created_by_repair_attempt_id": claim.attempt_id})
                    raw_id = pending["payload"]["raw_response_id"]
                    verified_recovery_entity(connection, raw_id)
                    durable_runs.checkpoint(connection, claim, {"pending_receipt_id": pending["id"], "raw_response_id": raw_id})
                outcome = capture.CaptureOutcome(slot_id=proof["original_slot_id"], attempt_id=0, raw_response_id=raw_id,
                    data={**prepared.parsed.data, "_evidence_captured_at": proof["source_captured_at"]}, billed=False, amount=0, currency="USD")
                providers._store_stage_result(prepared.source["content"], "detail", "lifetime", outcome, db_path=db_path,
                    media_root=media_root, defer_media_source=media_status == "media_source_refresh_required")
                with connect(db_path) as connection, transaction(connection):
                    _require(not failures, "repair lease maintenance failed")
                    assert_local_recovery_owner(connection, content_id=proof["content_id"], raw_response_id=raw_id, required=True)
                    record_detail_result(connection, content_id=proof["content_id"], raw_response_id=raw_id,
                                         available=True, recorded_at=now_utc())
                    observations = [row[0] for row in connection.execute("SELECT id FROM content_metric_observations WHERE content_id=? AND raw_response_id=?",
                                                                        (proof["content_id"], raw_id))]
                    _require(bool(observations) and _row(connection, "provider_raw_responses", raw_id)["source"] == "live_applied",
                             "recovered detail/metrics did not materialize")
                    at = now_utc()
                    final = {**proof, "phase": "done", "status": "recovered", "raw_response_id": raw_id, "pending_receipt_id": pending["id"],
                        "pending_receipt_sha256": pending["receipt_sha256"], "metric_observation_ids": observations,
                        "media_status": media_status, "repair_run_id": claim.scheduler_run_id,
                        "completed_by_repair_attempt_id": claim.attempt_id, "provider_calls": 0}
                    done = _append_phase(connection, final)
                    changed = connection.execute("UPDATE capture_work_items SET state='terminal',reason='recovered',completed_at=?,updated_at=? WHERE id=? AND state='paid_identity_hold' AND owner_token IS NULL",
                                                 (at, at, work_id)).rowcount
                    _require(changed == 1, "original work changed before recovery completion")
                    connection.execute("UPDATE fetch_slots SET status='succeeded',finished_at=?,updated_at=? WHERE id=?", (at, at, proof["original_slot_id"]))
                    connection.execute("UPDATE operational_alerts SET status='resolved',resolved_at=? WHERE dedupe_key=? AND status='open'", (at, f"capture-incomplete:{work_id}"))
                    durable_runs.checkpoint(connection, claim, {"complete": True, "done_receipt_id": done["id"]})
                    durable_runs.finish_run_in_transaction(connection, claim, status="succeeded", summary={"provider_calls": 0, "raw_response_id": raw_id})
                    return final
        except Exception:
            with connect(db_path) as connection, transaction(connection):
                try:
                    durable_runs.assert_owner(connection, claim)
                    durable_runs.finish_run_in_transaction(connection, claim, status="interrupted", summary={"provider_calls": 0})
                except durable_runs.LostOwnership:
                    pass
            raise
        finally:
            stopped.set()
            worker.join(timeout=1)
