"""Durable initial detail/counter consumers of one KS/Channels request.

This is a work dependency, never a new paid identity or permission. The owner
keeps its original operation, parameters, due window, budget and send checks.
Only the first ordinary metric cycle participates; later cycles and explicit
media-refresh commands keep their independently authorized windows.
"""
from __future__ import annotations

import json
from pathlib import Path
import sqlite3
from typing import Any, Mapping

from . import capture, capture_planning as planning, capture_singletons, providers
from .storage import connect, transaction

FIELD = "shared_detail_request"
CONTRACT = "shared-initial-detail-request-v1"
PLATFORMS = frozenset({"kuaishou", "wechat_channels"})


def _fail(reason: str) -> None:
    raise capture.CaptureError(reason, retryable=False, billed=False,
        error_code="shared_request_identity_hold:" + reason)


def _identity(envelope: Mapping[str, Any]) -> str:
    return planning.digest({"provider":"tikhub", "operation":envelope["operation"],
        "subject":f"content:{envelope['content_id']}", "logical_due":envelope["logical_due"]})


def _ordinary(envelope: Mapping[str, Any]) -> bool:
    # A manual association may retain the original ordinary window. It cannot
    # turn that same request into a second purchase by changing only `kind`.
    return (envelope.get("kind") != "media_source_refresh" and not envelope.get("compensation")
        and ((envelope.get("stage") == "detail" and envelope.get("logical_due") == "lifetime")
             or (envelope.get("stage") == "metrics" and str(envelope.get("logical_due", "")).startswith("metrics:"))))


def _request(content: Mapping[str, Any], envelope: Mapping[str, Any]) -> dict[str, Any]:
    stage = envelope.get("source_stage", envelope["stage"])
    platform = content["platform"]
    if stage not in {"detail", "metrics"} or envelope["operation"] != providers.STAGE_CONFIG[(platform, stage)][2]:
        _fail("operation_changed")
    subject = providers._content_subject(content)
    spec = providers._extra_adapter(platform).request_spec(stage, subject)
    params = providers._content_request_params(platform, stage, subject, content["content_type"])
    return {"method":spec["method"], "path":spec["path"], "parameters":params,
        "platform":platform, "content_id":content["id"], "subject":str(content["platform_content_id"]),
        "uid":str(envelope.get("uid") or "")}


def _descriptor(row: Mapping[str, Any], content: Mapping[str, Any]) -> dict[str, Any]:
    envelope = json.loads(row["envelope_json"])
    if (_identity(envelope) != row["work_identity"] or row["operation"] != envelope["operation"]
            or row["content_id"] != envelope["content_id"] or content["platform"] != envelope["platform"]):
        _fail("work_changed")
    request = _request(content, envelope)
    if content.get("raw_account_uid") and str(content["raw_account_uid"]) != request["uid"]:
        _fail("account_identity_changed")
    identity = providers._paid_request_identity(operation=envelope["operation"],
        platform=envelope["platform"], subject=request["subject"], params=request["parameters"],
        cursor=None, due_bucket=envelope["logical_due"])
    return {"work_id":row["id"], "work_identity":row["work_identity"],
        "operation":envelope["operation"], "stage":envelope["stage"],
        "source_stage":envelope.get("source_stage", envelope["stage"]),
        "created_at":row["created_at"],
        "window":envelope["logical_due"], "paid_scope_identity":identity.scope_identity,
        "request":request}


def _validate(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        _fail("binding_missing")
    document = {key:item for key,item in value.items() if key != "sha256"}
    if value.get("contract_version") != CONTRACT or planning.digest(document) != value.get("sha256"):
        _fail("binding_changed")
    return value


def preserve(stored: Mapping[str, Any], incoming: Mapping[str, Any]) -> dict[str, Any]:
    """A stale execution snapshot cannot drop a concurrently frozen owner."""
    result = dict(incoming)
    current = stored.get(FIELD)
    supplied = incoming.get(FIELD)
    if current is not None:
        _validate(current)
        if supplied is not None and supplied != current:
            _fail("checkpoint_binding_conflict")
        result[FIELD] = current
    elif supplied is not None:
        _fail("checkpoint_unpersisted_binding")
    return result


def _activity(connection: sqlite3.Connection, descriptor: Mapping[str, Any], db_path: Path) -> tuple[bool, bool]:
    """Dispatch rows and the durable O_EXCL marker outrank usage projections."""
    slot = connection.execute("SELECT * FROM fetch_slots WHERE content_id=? AND stage=? AND window_key=?",
        (descriptor["request"]["content_id"], descriptor["stage"], descriptor["window"])).fetchone()
    marker = db_path.parent / "paid_send_claims" / descriptor["paid_scope_identity"][:2] / (
        descriptor["paid_scope_identity"] + ".sequence-00000000.claim.json")
    try:
        marker.lstat()
        sent = True
    except FileNotFoundError:
        sent = False
    if slot is None:
        return sent, sent
    sent = sent or connection.execute("SELECT 1 FROM paid_provider_dispatch_events WHERE fetch_slot_id=? AND event_type='send_marked' LIMIT 1", (slot["id"],)).fetchone() is not None
    # Legacy attempts retain slot_id; singleton attempts require their unique
    # dispatch relationship. Even an unbilled/failed attempt cannot buy again.
    sent = sent or connection.execute(f"SELECT 1 FROM fetch_attempts a WHERE {capture_singletons.attempt_slot_sql(connection, 'a')}=? LIMIT 1", (slot["id"],)).fetchone() is not None
    return sent, sent or slot["status"] not in {"pending", "retryable_failed"}


def bind(envelope: Mapping[str, Any], *, db_path: Path) -> dict[str, Any] | None:
    """Freeze/recheck the owner under one short writer transaction.

    Existing sent requests are adopted with their exact provenance. Two old
    physical owners are a conflict, never retrospectively counted as one.
    """
    if envelope.get("platform") not in PLATFORMS or envelope.get("stage") not in {"detail", "metrics"}:
        return None
    with connect(db_path) as connection, transaction(connection):
        if connection.execute("PRAGMA user_version").fetchone()[0] not in {23, 24}:
            return None
        row = connection.execute("SELECT * FROM capture_work_items WHERE work_identity=?", (_identity(envelope),)).fetchone()
        if row is None:
            if _ordinary(envelope):
                _fail("work_missing")
            return None
        stored = json.loads(row["envelope_json"])
        if not _ordinary(stored) and FIELD not in stored:
            return None
        content = dict(connection.execute("SELECT * FROM content_items WHERE id=?", (row["content_id"],)).fetchone())
        current = _descriptor(row, content)
        if _request(content, envelope) != current["request"] or envelope["logical_due"] != current["window"]:
            _fail("execution_subject_changed")
        rows = connection.execute("SELECT * FROM capture_work_items WHERE content_id=? AND provider='tikhub' ORDER BY id", (content["id"],)).fetchall()
        initial = []
        first_metrics = None
        for candidate in rows:
            value = json.loads(candidate["envelope_json"])
            if _ordinary(value) or FIELD in value:
                if value["stage"] == "detail" and value["logical_due"] == "lifetime":
                    initial.append(candidate)
                elif value["stage"] == "metrics" and first_metrics is None:
                    first_metrics = candidate
        if first_metrics is not None:
            # A first counter work created in a strictly later natural bucket
            # than an existing lifetime detail is a refresh, not initial fanout.
            # Its older detail raw must not close the new period as complete.
            due = json.loads(first_metrics["envelope_json"])["logical_due"]
            try:
                bucket = planning.timestamp(due.removeprefix("metrics:").rsplit(":",1)[0])
            except (ValueError, TypeError):
                _fail("metric_cycle_invalid")
            if not initial or all(planning.timestamp(item["created_at"]) >= bucket for item in initial):
                initial.append(first_metrics)
        if row["id"] not in {item["id"] for item in initial}:
            if FIELD in stored:
                _fail("phase_changed")
            return None
        descriptors = {item["id"]:_descriptor(item, content) for item in initial}
        if any(item["request"] != current["request"] for item in descriptors.values()):
            _fail("physical_parameters_conflict")
        bindings = [_validate(json.loads(item["envelope_json"])[FIELD]) for item in initial if FIELD in json.loads(item["envelope_json"])]
        activity = {key:_activity(connection, value, db_path) for key,value in descriptors.items()}
        for item in initial:
            # Missing old marker/index evidence cannot erase an explicit paid
            # HOLD. A waiting consumer's own HOLD is not a second producer.
            if item["state"] == "paid_identity_hold" and "shared_request" not in item["reason"]:
                activity[item["id"]] = (activity[item["id"]][0],True)
        sent = [key for key,value in activity.items() if value[0]]
        if len(sent) > 1:
            _fail("multiple_existing_physical_owners")
        if bindings:
            binding = bindings[0]
            if any(value != binding for value in bindings) or binding["owner"] != descriptors.get(binding["owner"]["work_id"]):
                _fail("owner_changed")
            if sent and sent[0] != binding["owner"]["work_id"]:
                _fail("non_owner_already_sent")
            if any(key != binding["owner"]["work_id"] and value[1] for key,value in activity.items()):
                _fail("non_owner_existing_reservation")
        else:
            reserved = [key for key,value in activity.items() if value[1]]
            if len(reserved) > 1:
                _fail("multiple_existing_reservations")
            chosen = (sent or reserved or [item["id"] for item in initial if item["state"] == "running"]
                      or [item["id"] for item in initial if item["state"] == "runnable"]
                      or [row["id"]])[0]
            document = {"contract_version":CONTRACT,"phase":"initial-detail-and-first-metric-cycle",
                "content_id":content["id"],"owner":descriptors[chosen]}
            binding = {**document,"sha256":planning.digest(document)}
        for candidate in initial:
            old = json.loads(candidate["envelope_json"])
            if FIELD not in old:
                updated = {**old,FIELD:binding}
                changed = connection.execute("UPDATE capture_work_items SET envelope_json=? WHERE id=? AND envelope_json=?",
                    (planning.canonical(updated),candidate["id"],candidate["envelope_json"]))
                if changed.rowcount != 1:
                    _fail("binding_cas_failed")
        return {"binding":binding,"is_owner":row["id"] == binding["owner"]["work_id"]}


def consume(envelope: Mapping[str, Any], relationship: Mapping[str, Any], *, db_path: Path) -> dict[str, Any] | None:
    """Replay exactly the owner's successful raw, including partial counters.

    A consumer never submits a second operation when replay is unavailable.
    The owner itself retains the existing crash recovery / paid HOLD path.
    """
    binding = _validate(relationship["binding"])
    if relationship["is_owner"]:
        return None
    owner = binding["owner"]
    try:
        raw = capture.load_succeeded_raw_response(db_path=db_path,content_id=binding["content_id"],
            stage=owner["stage"],window_key=owner["window"],operation=owner["operation"])
    except capture.SlotUnavailable:
        with connect(db_path) as connection:
            row = connection.execute("SELECT state FROM capture_work_items WHERE id=?", (owner["work_id"],)).fetchone()
            sent, _reserved = _activity(connection, owner, db_path)
        held = row is None or row[0] in {"terminal","paid_identity_hold"} or (sent and row[0] not in {"running","leased"})
        return {"complete":False,"continuation":not held,"envelope":dict(envelope),
            "evidence":{"shared_request":binding,"waiting_for_work_id":owner["work_id"],
                "message":"共享请求尚未得到可验证结果，保留原请求等待核对" if held else "正在等待同一作品的共享请求完成，将免费使用其原始结果"},
            "reason":"shared_request_identity_hold:owner_unresolved" if held else "shared_request_owner_pending",
            "provider_cost":0.0}
    with connect(db_path) as connection:
        content = dict(connection.execute("SELECT * FROM content_items WHERE id=?", (binding["content_id"],)).fetchone())
        indexed = connection.execute("SELECT paid_scope_identity,sequence,source FROM provider_raw_responses WHERE id=?", (raw.raw_response_id,)).fetchone()
        batch = connection.execute("SELECT operation,parameters_json FROM fetch_request_batches WHERE request_scope_identity=? AND sequence=0", (owner["paid_scope_identity"],)).fetchone()
        if (raw.provider != "TikHub" or raw.operation != owner["operation"]
                or indexed is None or indexed[0] != owner["paid_scope_identity"] or indexed[1] != 0
                or batch is None or batch[0] != owner["operation"]
                or json.loads(batch[1]) != owner["request"]["parameters"]
                or planning.timestamp(raw.captured_at) < planning.timestamp(owner["created_at"])):
            _fail("raw_owner_conflict")
    if isinstance(raw.value, Mapping) and raw.value.get("derived_from_operation"):
        _fail("derived_raw_has_no_physical_detail")
    parsed = providers._parse_content_payload(content["platform"],envelope["stage"],
        content["platform_content_id"],content["content_type"],raw.value,status=raw.http_status or 200,
        expected_uid=envelope["uid"])
    outcome = capture.CaptureOutcome(raw.slot_id,0,raw.raw_response_id,
        {**dict(parsed.data),"_evidence_captured_at":raw.captured_at},False,0.0,"USD")
    providers._store_stage_result(content,envelope["stage"],envelope["logical_due"],outcome,
        db_path=db_path,preserve_existing_content_fields=True)
    # Recheck the binding after projection, before reporting consumer completion.
    checked = bind(envelope,db_path=db_path)
    if checked is None or checked["binding"] != binding:
        _fail("binding_changed_during_projection")
    from .metric_source_policy import auto_collectable_fields
    metrics = parsed.data if envelope["stage"] == "metrics" else parsed.data.get("metrics", {})
    missing = sorted(field for field in auto_collectable_fields(content["platform"])
        if metrics.get(field) is None)
    return {"complete":True,"continuation":False,"envelope":dict(envelope),
        "evidence":{"raw_response_ids":[raw.raw_response_id],"all_raw_verified":True,
            "completion_kind":"shared_request_response","shared_request":binding,
            "producer_operation":raw.operation,"missing_metric_fields":missing},
        "reason":"shared_response_partial_metrics" if missing else "","provider_cost":0.0}


def verify_execution(envelope: Mapping[str, Any], content: Mapping[str, Any],
                     relationship: Mapping[str, Any] | None, *, db_path: Path) -> None:
    if relationship is None:
        return
    binding = _validate(relationship["binding"])
    if not relationship["is_owner"] or _request(content,envelope) != binding["owner"]["request"]:
        _fail("execution_parameters_changed")
    current = bind(envelope,db_path=db_path)
    if current != relationship:
        _fail("execution_owner_changed")


def wake_consumers(relationship: Mapping[str, Any] | None, *, db_path: Path, at: str) -> None:
    """Resume existing waits after owner materialization; create no new work."""
    if relationship is None or not relationship["is_owner"]:
        return
    binding = _validate(relationship["binding"])
    with connect(db_path) as connection, transaction(connection):
        rows = connection.execute("SELECT id,envelope_json FROM capture_work_items WHERE content_id=? AND state IN ('runnable','provider_blocked') AND reason='shared_request_owner_pending'", (binding["content_id"],)).fetchall()
        for row in rows:
            if json.loads(row["envelope_json"]).get(FIELD) != binding:
                _fail("waiting_consumer_binding_changed")
            connection.execute("UPDATE capture_work_items SET state='runnable',due_at=?,updated_at=? WHERE id=?", (planning.timestamp(at),planning.timestamp(at),row["id"]))
