"""Explicit, one-use compensation on the original durable work and logical due.

An accounting terminal is not retry permission. This module only enqueues
already-issued request/member grants, and both paid boundaries reread them.
No provider gate, activation, route or original paid identity is rewritten.
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Iterator, Mapping

from . import capture_authorizations as auth, capture_planning as planning, usage_settlements as ledger
from .paid_identity import PaidRequestIdentity, build_paid_request_identity
from .provider_budget import PaidScope, PaidScopeBlocked, PRICES_MICROUSD
from .runtime_database import require_current_process_writer_lock

CONTRACT = "capture-explicit-compensation-v1"
_WORK: ContextVar[int | None] = ContextVar("capture_compensation_work", default=None)


def _require(value: Any, message: str) -> None:
    if not value:
        raise PaidScopeBlocked("compensation_authorization_invalid", message)


def _identity(document: Mapping[str, Any], sequence: int) -> PaidRequestIdentity:
    return build_paid_request_identity(provider=document["provider"], operation=document["operation"],
        platform=document["platform"], subject=document["subject"], request_parameters=document["request_parameters"],
        cursor=document["cursor"], request_window=document["request_window"], due_bucket=document["due_bucket"], sequence=sequence)


def _grant(connection: sqlite3.Connection, issuance_id: int) -> dict[str, Any]:
    row = connection.execute("""SELECT a.*,s.provider_usage_id FROM compensation_authorization_issuances i
        JOIN compensation_authorizations a ON a.id=i.authorization_id
        JOIN provider_usage_settlements s ON s.id=a.original_settlement_id WHERE i.id=?""", (issuance_id,)).fetchone()
    _require(row is not None, "A real compensation issuance is required")
    assert row is not None
    return dict(row)


def _work(connection: sqlite3.Connection, work_id: int) -> tuple[dict[str, Any], dict[str, Any]]:
    row = connection.execute("SELECT * FROM capture_work_items WHERE id=?", (work_id,)).fetchone()
    _require(row is not None, "Compensation durable work does not exist")
    assert row is not None
    return dict(row), json.loads(row["envelope_json"])


def _validate_proof(connection: sqlite3.Connection, work_id: int, *, at: str,
                    require_running: bool = False, scope: PaidScope | None = None,
                    require_unused: bool = True) -> dict[str, Any]:
    work, envelope = _work(connection, work_id)
    proof = envelope.get("compensation")
    _require(isinstance(proof, dict), "Work has no explicit compensation proof")
    assert isinstance(proof, dict)
    proof = dict(proof)
    digest = proof.pop("proof_sha256", None)
    _require(digest == planning.digest(proof) and proof.get("contract") == CONTRACT
             and proof.get("work_id") == work_id, "Compensation work proof changed")
    receipt = connection.execute("SELECT payload_json FROM data_quality_receipts WHERE receipt_sha256=?",
                                  (digest,)).fetchone()
    _require(receipt is not None and json.loads(receipt[0]) == proof, "Immutable enqueue receipt is missing or changed")
    for key in ("account_id", "content_id", "operation", "assignment_id"):
        _require(work[key] == proof[key] and envelope.get(key) == proof[key], "Compensation work target/route changed")
    from .capture_runtime import _page_window

    _require(_page_window(envelope) == proof["request_document"]["due_bucket"], "Compensation logical due/cursor changed")
    bindings = auth.current_runtime_bindings(connection, work["operation"], at)
    _require(dict(bindings) == proof["runtime_bindings"], "Compensation build/profile/roster changed")
    assignment = planning.resolve_route(connection, account_id=work["account_id"], content_id=work["content_id"],
                                        operation=work["operation"], at=at)
    _require(assignment is not None and assignment["id"] == work["assignment_id"]
             and assignment["route"] == "integrated" and assignment["mode"] == "active",
             "Compensation cannot change or bypass the assigned route")
    if require_running:
        _require(work["state"] == "running" and work["owner_token"], "Compensation work has no live worker lease")
        if scope is not None:
            attempt = connection.execute("SELECT * FROM scheduler_run_attempts WHERE id=? AND scheduler_run_id=?",
                                         (scope.scheduler_attempt_id, scope.scheduler_run_id)).fetchone()
            _require(attempt is not None and attempt["status"] == "running"
                     and attempt["owner_token"] == work["owner_token"], "Compensation caller does not own its durable work")
    request = _identity(proof["request_document"], proof["sequence"])
    _require(request.scope_identity == proof["request_identity"]
             and ledger.member_identity(request.document) == proof["member_identity"], "Compensation request/member identity changed")
    if require_unused:
        auth.validate_authorization(connection, runtime_bindings=bindings, operation=work["operation"],
            request_identity=request.scope_identity, member_identities=(proof["member_identity"],),
            sequence=request.sequence, issuance_ids=proof["issuance_ids"], at=at,
            amount_microusd=PRICES_MICROUSD[work["operation"]])
    return {**proof, "proof_sha256": digest}


def enqueue_authorized_compensation(connection: sqlite3.Connection, *, work_id: int,
                                    request_issuance_id: int, member_issuance_id: int,
                                    at: str) -> dict[str, Any]:
    """Explicit writer call; retain one work, original due/cursor, and paid scope.

    The caller first issues grants through usage_settlements.authorize_compensation
    with concrete gap/raw-replay evidence. No automatic grant is created here.
    """
    _require(connection.in_transaction and connection.execute("PRAGMA user_version").fetchone()[0] == 20,
             "Compensation enqueue requires a schema20 writer transaction")
    require_current_process_writer_lock(connection)
    work, envelope = _work(connection, work_id)
    if envelope.get("compensation"):
        old = _validate_proof(connection, work_id, at=at, require_unused=False)
        if set(old["issuance_ids"].values()) == {request_issuance_id, member_issuance_id}:
            return {"work_id": work_id, "proof_sha256": old["proof_sha256"], "idempotent": True, "provider_calls": 0}
        _require(work["state"] == "paid_identity_hold", "Only a held prior compensation may receive a new issuance")
    _require(work["state"] in {"paid_identity_hold", "provider_blocked", "budget_deferred"},
             "Only explicit incomplete/held work may be compensated")
    request_grant, member_grant = _grant(connection, request_issuance_id), _grant(connection, member_issuance_id)
    _require(request_grant["scope_kind"] == "request" and member_grant["scope_kind"] == "member"
             and request_grant["original_settlement_id"] == member_grant["original_settlement_id"]
             and request_grant["next_sequence"] == member_grant["next_sequence"],
             "Compensation requires paired request/member grants for the same settled request")
    original = connection.execute("SELECT details_json FROM provider_usage WHERE id=?", (request_grant["provider_usage_id"],)).fetchone()
    details = json.loads(original[0])
    document = details.get("paid_identity")
    _require(isinstance(document, dict), "Original request document is absent; identity cannot be invented")
    document = dict(document)
    if request_grant["scope_identity"] != _identity(document, 0).scope_identity:
        # A missing statistics member may have an explicitly authorized singleton
        # request identity. The ledger verifies its original batch/member proof.
        document = ledger.singleton_compensation_document(connection,
            settlement_id=request_grant["original_settlement_id"], gap_evidence_ref=request_grant["gap_evidence_ref"])
    request = _identity(document, request_grant["next_sequence"])
    member_hash = ledger.member_identity(request.document)
    _require(request.scope_identity == request_grant["scope_identity"] and member_hash == member_grant["scope_identity"],
             "Granted request/member are not this exact singleton request")
    from .capture_runtime import _page_window

    _require(document["operation"] == work["operation"] and document["due_bucket"] == _page_window(envelope),
             "Original request operation/due is not this durable work")
    if work["content_id"] is not None:
        target = connection.execute("SELECT platform_content_id FROM content_items WHERE id=?", (work["content_id"],)).fetchone()
        _require(target is not None and str(target[0]) == document["subject"], "Compensation content identity differs")
    bindings = dict(auth.current_runtime_bindings(connection, work["operation"], at))
    issuances = {request.scope_identity: request_issuance_id, member_hash: member_issuance_id}
    proof = {"contract": CONTRACT, "work_id": work_id, "request_document": request.document,
        "request_identity": request.scope_identity, "member_identity": member_hash, "sequence": request.sequence,
        "issuance_ids": issuances, "runtime_bindings": bindings, "issued_at": planning.timestamp(at),
        **{key: work[key] for key in ("account_id", "content_id", "operation", "assignment_id")}}
    digest = planning.digest(proof)
    connection.execute("SAVEPOINT enqueue_compensation")
    try:
        connection.execute("INSERT INTO data_quality_receipts(scope_key,cutoff_at,payload_json,recorded_at,receipt_sha256) VALUES(?,?,?,?,?)",
            (f"compensation:{work_id}:{request.sequence}", planning.timestamp(at), planning.canonical(proof), planning.timestamp(at), digest))
        updated = {**envelope, "compensation": {**proof, "proof_sha256": digest}}
        # A prior shared batch is historical evidence, never repurchased wholesale.
        updated.pop("request_batch_id", None)
        connection.execute("UPDATE capture_work_items SET state='runnable',reason='explicit_compensation',envelope_json=?,due_at=?,updated_at=? WHERE id=?",
                           (planning.canonical(updated), planning.timestamp(at), planning.timestamp(at), work_id))
        _validate_proof(connection, work_id, at=at)
        connection.execute("RELEASE enqueue_compensation")
    except BaseException:
        connection.execute("ROLLBACK TO enqueue_compensation")
        connection.execute("RELEASE enqueue_compensation")
        raise
    return {"work_id": work_id, "proof_sha256": digest, "sequence": request.sequence, "idempotent": False, "provider_calls": 0}


@contextmanager
def execution_context(work_id: int) -> Iterator[None]:
    """Internal worker context is only an ID; DB proof and lease are mandatory."""
    token = _WORK.set(work_id)
    try:
        yield
    finally:
        _WORK.reset(token)


def active_work_id() -> int | None:
    return _WORK.get()


def prepare_request(connection: sqlite3.Connection, request: PaidRequestIdentity, *, scope: PaidScope,
                    at: str, exclude_usage_id: int | None = None) -> tuple[PaidRequestIdentity, dict[str, int], str | None]:
    work_id = _WORK.get()
    if work_id is None:
        _require(request.sequence == 0, "Nonzero sequence requires its explicit durable compensation work")
        return request, {}, None
    # The main A/B validator below includes its own reservation exclusion. Here
    # reread the immutable proof/lease without counting B's reservation twice.
    proof = _validate_proof(connection, work_id, at=at, require_running=True, scope=scope, require_unused=False)
    _require(request.document == proof["request_document"] and request.sequence in {0, proof["sequence"]},
             "Actual provider request changed from its authorized original document")
    compensated = _identity(request.document, proof["sequence"])
    auth.validate_authorization(connection, runtime_bindings=proof["runtime_bindings"], operation=request.document["operation"],
        request_identity=compensated.scope_identity, member_identities=(proof["member_identity"],),
        sequence=compensated.sequence, issuance_ids=proof["issuance_ids"], at=at,
        amount_microusd=PRICES_MICROUSD[request.document["operation"]], exclude_usage_id=exclude_usage_id)
    return compensated, dict(proof["issuance_ids"]), str(proof["proof_sha256"])


def replay_sequence(connection: sqlite3.Connection, *, content_id: int | None, account_id: int | None,
                    stage: str, window_key: str, operation: str | None) -> tuple[str, int] | None:
    """Read-only replay may use a consumed grant, but only its exact stored raw."""
    work_id = _WORK.get()
    if work_id is None:
        return None
    work, envelope = _work(connection, work_id)
    proof = envelope.get("compensation", {})
    _require(work["content_id"] == content_id and (content_id is not None or work["account_id"] == account_id)
             and envelope.get("capture_stage", envelope["stage"]) == stage
             and proof.get("request_document", {}).get("due_bucket") == window_key
             and work["operation"] == operation, "Compensation replay target differs")
    # Proof hash and immutable DB receipt, without demanding an unconsumed grant.
    digest = proof.get("proof_sha256")
    document = {key: value for key, value in proof.items() if key != "proof_sha256"}
    row = connection.execute("SELECT payload_json FROM data_quality_receipts WHERE receipt_sha256=?", (digest,)).fetchone()
    _require(digest == planning.digest(document) and row is not None and json.loads(row[0]) == document,
             "Compensation replay proof differs")
    return str(proof["request_identity"]), int(proof["sequence"])


def readiness(connection: sqlite3.Connection, envelope: Mapping[str, Any], *, at: str) -> tuple[str, str]:
    try:
        work_id = int(envelope["compensation"]["work_id"])
        proof = _validate_proof(connection, work_id, at=at, require_unused=False)
        raw = connection.execute("SELECT id FROM provider_raw_responses WHERE paid_scope_identity=? AND sequence=?",
                                 (proof["request_identity"], proof["sequence"])).fetchone()
        if raw is not None:
            from .raw_archive import read_response_entity

            read_response_entity(connection, int(raw[0]))
            return "runnable", ""  # Crash after C: exact paid result is replayed, never sent again.
        _validate_proof(connection, work_id, at=at)
    except (PaidScopeBlocked, ledger.SettlementError, ValueError) as error:
        return "paid_identity_hold", str(getattr(error, "error_code", "compensation_authorization_invalid"))
    return "runnable", ""


def run_authorized_work(db_path: Path, work_id: int, at: str) -> dict[str, Any]:
    from .capture_runtime import _run_single

    return _run_single(db_path, at, compensation_work_id=work_id)
