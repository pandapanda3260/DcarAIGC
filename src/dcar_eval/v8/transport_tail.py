"""Exact, read-only post-START evidence sets for current-HOLD diagnostics."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from typing import Any

from . import capture, paid_drain, tikhub_scan
from .provider_budget import micro_usd
from .raw_evidence import MAX_SIDECAR_BYTES, canonical_json_bytes
from .source_routing import parse_time
from .transport_accounting import read_primary_member_accounting
from .transport_evidence import DiagnosticEvidenceError, _file
from .transport_owner_evidence import _claim, _no_paid_continuation, _owner_chain, _terminal_owner, read_primary_campaign_owners
from .transport_receipts import read_transport_receipt

CONTRACT_VERSION = "current-hold-diagnostic-tail-v1"
_HWM = {
    "usage_ids": "provider_usage_high_watermark",
    "fetch_attempt_ids": "fetch_attempt_high_watermark",
    "raw_response_ids": "raw_response_high_watermark",
    "dispatch_event_ids": "dispatch_event_high_watermark",
    "paid_run_ids": "scheduler_run_high_watermark",
    "paid_attempt_ids": "scheduler_attempt_high_watermark",
    "materialization_run_ids": "scheduler_run_high_watermark",
    "materialization_attempt_ids": "scheduler_attempt_high_watermark",
}


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DiagnosticEvidenceError(message)


def _local_attempt(connection: sqlite3.Connection, row: sqlite3.Row, *, job: str, at: str) -> dict[str, Any]:
    details = json.loads(row["details_json"])
    claim = _claim({"scheduler_run_id": row["scheduler_run_id"], "attempt_id": row["id"],
                    "attempt_number": row["attempt_number"], "owner_token": details["owner"]["token"],
                    "scan_id": details["scan_id"]})
    return _terminal_owner(connection, claim, job=job, at=at,
                           invocation=row["invocation_source"], historical=True) | {"invocation_source": row["invocation_source"]}


def _historical_local_pages(
    connection: sqlite3.Connection, start: paid_drain.DrainReceipt, *, at: str, already_owned: set[int],
) -> list[dict[str, Any]]:
    """Close only pre-START frozen pending pages; every later owner is accounted."""
    frozen = start.payload["frozen_dispatch"]
    pages = []
    for run in connection.execute(
        "SELECT * FROM scheduler_runs WHERE id<=? AND job_id IN ('tikhub_reconcile','history_recovery') "
        "AND EXISTS (SELECT 1 FROM scheduler_run_attempts a WHERE a.scheduler_run_id=scheduler_runs.id AND a.id>?)",
        (frozen["scheduler_run_high_watermark"], frozen["scheduler_attempt_high_watermark"]),
    ):
        if run["id"] in already_owned:
            continue
        attempts = connection.execute(
            "SELECT * FROM scheduler_run_attempts WHERE scheduler_run_id=? ORDER BY attempt_number,id", (run["id"],),
        ).fetchall()
        anchors = [row for row in attempts if row["id"] <= frozen["scheduler_attempt_high_watermark"]]
        _require(bool(anchors), "Historical local page has no pre-START attempt")
        anchor = _local_attempt(connection, anchors[-1], job=run["job_id"], at=start.created_at)
        base = anchor["details"]["checkpoint"]
        scope = anchor["details"]["identity"]
        pending = base.get("pending_materialization")
        _require(isinstance(pending, dict) and base["complete"] is False and base.get("pending_raw") is None
                 and scope.get("contract_version") == tikhub_scan.CONTRACT_VERSION,
                 "Historical owner has no frozen pre-START pending materialization")
        identity = pending["identity"]
        raw = connection.execute("SELECT * FROM provider_raw_responses WHERE id=?", (identity["raw_response_id"],)).fetchone()
        _require(raw is not None and raw["id"] <= frozen["raw_response_high_watermark"]
                 and raw["fetch_attempt_id"] <= frozen["fetch_attempt_high_watermark"]
                 and raw["provider"].lower() == "tikhub" and raw["account_id"] == scope["account_id"]
                 and raw["operation"] == {"douyin": "douyin_user_posts", "xiaohongshu": "xiaohongshu_user_posts"}.get(scope["platform"])
                 and raw["content_id"] is None and raw["source"] == "derived_applied" and raw["http_status"] == 200
                 and parse_time(raw["captured_at"]) <= parse_time(start.created_at),
                 "Local replay source is not a completed pre-START account raw")
        raw_path, raw_value = capture._read_verified_raw_response(raw, connection=connection)
        source_attempt = connection.execute("SELECT * FROM fetch_attempts WHERE id=?", (raw["fetch_attempt_id"],)).fetchone()
        slot = connection.execute("SELECT * FROM fetch_slots WHERE id=?", (source_attempt["slot_id"],)).fetchone()
        head = base["last_manifest"]
        manifest = json.loads(_file(head["path"], head["sha256"], head["byte_size"], limit=MAX_SIDECAR_BYTES))
        eligible = [item for item in manifest["items"] if item.get("reason") == "" and type(item.get("content_id")) is int]
        indexes = [item["index"] for item in eligible]
        receipt = {"kind": "page", "window_key": identity["window_key"], "raw_response_id": raw["id"],
                   "slot_id": slot["id"], "sha256": raw["sha256"], "captured_at": raw["captured_at"]}
        _require(identity == tikhub_scan._materialization_identity(
            _claim(anchor["claim"]), scope, receipt, generation=base["generation"],
            page_number=base["page_number"] - 1, eligible_indexes=indexes,
        ) and indexes == sorted(set(indexes)) and manifest["raw"] == receipt
            and manifest["scope"] == scope and manifest["scan_id"] == anchor["claim"]["scan_id"]
            and manifest["contract_version"] == tikhub_scan.CONTRACT_VERSION
            and manifest["generation"] == base["generation"] and manifest["page_number"] == base["page_number"] - 1
            and manifest["execution_next_cursor"] == base["cursor"]
            and manifest["provider_next_cursor"] == base["provider_next_cursor"]
            and pending["after_materialization"] == {"complete": manifest["completion_reason"] is not None,
                                                     "completion_reason": manifest["completion_reason"]}
            and slot["account_id"] == scope["account_id"] and slot["stage"] == "discovery"
            and slot["provider"].lower() == "tikhub" and slot["window_key"] == identity["window_key"]
            and source_attempt["http_status"] == 200 and source_attempt["error_code"] is None
            and parse_time(source_attempt["request_started_at"]) <= parse_time(raw["captured_at"])
            <= parse_time(source_attempt["response_finished_at"]) <= parse_time(start.created_at),
            "Historical materialization identity, manifest or source attempt changed")
        stored = capture.StoredRawResponse(slot["id"], raw["id"], raw["provider"], raw["operation"],
                                           raw_value, raw["http_status"], raw["captured_at"], raw["sha256"], raw_path)
        values, _, _, _ = tikhub_scan._page(stored, scope["platform"])
        for item in eligible:
            normalized = tikhub_scan._item(scope["platform"], values[item["index"]])
            published = tikhub_scan.normalize_timestamp(normalized.get("published_at"))
            content = connection.execute("SELECT * FROM content_items WHERE id=?", (item["content_id"],)).fetchone()
            _require(content is not None and content["account_id"] == scope["account_id"]
                     and content["platform"] == scope["platform"]
                     and content["platform_content_id"] == normalized["platform_content_id"]
                     and normalized["account_uid"] == scope["uid"]
                     and published is not None and parse_time(scope["window_start"]) <= parse_time(published) < parse_time(scope["window_end"]),
                     "Historical eligible content differs from its original raw")
        sources = [anchor] + [_local_attempt(connection, row, job=run["job_id"], at=at)
                              for row in attempts if row["id"] > frozen["scheduler_attempt_high_watermark"]]
        children = connection.execute(
            "SELECT id FROM scheduler_runs WHERE job_id=? AND json_extract(details_json,'$.identity.parent_scheduler_run_id')=? "
            "AND json_extract(details_json,'$.identity.raw_response_id')=?",
            (tikhub_scan.MATERIALIZATION_JOB, run["id"], raw["id"]),
        ).fetchall()
        _require(len(children) == 1, "Historical page has missing or extra materialization children")
        first = connection.execute("SELECT * FROM scheduler_run_attempts WHERE scheduler_run_id=? ORDER BY attempt_number,id LIMIT 1",
                                   (children[0]["id"],)).fetchone()
        child_chain = _owner_chain(connection, _local_attempt(connection, first, job=tikhub_scan.MATERIALIZATION_JOB, at=at),
                                   at=at, invocation="scheduled")
        later_children = [child for child in child_chain if child["claim"]["attempt_id"] > frozen["scheduler_attempt_high_watermark"]]
        by_parent = {child["details"]["checkpoint"]["parent_attempt_id"]: child for child in later_children}
        _require(len(by_parent) == len(later_children), "Local children duplicate their source owner")
        variable = {"pending_materialization", "complete", "completion_reason"}
        for index, source in enumerate(sources[1:], 1):
            details, state = source["details"], source["details"]["checkpoint"]
            previous = sources[index - 1]
            _require(details["identity"] == scope and source["claim"]["attempt_number"] == previous["claim"]["attempt_number"] + 1
                     and parse_time(previous["row"]["completed_at"]) <= parse_time(source["row"]["started_at"])
                     and {key: value for key, value in state.items() if key not in variable}
                     == {key: value for key, value in base.items() if key not in variable},
                     "Historical continuation changed the page or skipped an owner")
            if source["row"]["status"] == "failed":
                _require(details["summary"].get("reason") == "profile_superseded" and state == base
                         and source["claim"]["attempt_id"] not in by_parent and index < len(sources) - 1,
                         "Unexplained failed historical source attempt")
                continue
            _require(source["row"]["status"] in {"partial", "succeeded"}
                     and source["invocation_source"] == "operator_retry"
                     and source["claim"]["attempt_id"] in by_parent,
                     "Historical source has no exact local child attempt")
            if previous["row"]["status"] == "failed":
                _require(details.get("local_materialization_recovery") == {
                    "prior_attempt_id": previous["claim"]["attempt_id"],
                    "prior_details_sha256": tikhub_scan._digest(previous["details"]),
                    "reason": "historical_raw_epoch_gate", "recovered_at": source["row"]["started_at"],
                }, "Historical failed owner was not recovered from its immutable attempt")
            _require(state["pending_materialization"] == (None if index == len(sources) - 1 else pending)
                     and (index != len(sources) - 1 or all(state[key] == value for key, value in pending["after_materialization"].items())),
                     "Historical page is still pending or changed its completion")
        previous_indexes: list[int] = []
        previous_hashes: list[str] = []
        for child in child_chain:
            state = child["details"]["checkpoint"]
            _require(child["details"]["identity"] == identity, "Historical local child identity changed")
            if child["claim"]["attempt_id"] <= frozen["scheduler_attempt_high_watermark"]:
                previous_indexes = state.get("completed_indexes", [])
                previous_hashes = state.get("item_result_sha256", [])
                continue
            matches = [source for source in sources[1:] if source["claim"]["attempt_id"] == state["parent_attempt_id"]]
            _require(len(matches) == 1, "Historical child has an unexplained source owner")
            source = matches[0]
            offset = state.get("next_item_offset")
            _require(state.get("progress_contract_version") == tikhub_scan.MATERIALIZATION_PROGRESS_CONTRACT_VERSION
                     and type(offset) is int and 0 <= offset <= len(indexes)
                     and state["raw_response_id"] == raw["id"] and state["eligible_item_count"] == len(indexes)
                     and state["completed_indexes"] == indexes[:offset] and len(state["item_result_sha256"]) == offset
                     and state["completed_indexes"][:len(previous_indexes)] == previous_indexes
                     and state["item_result_sha256"][:len(previous_hashes)] == previous_hashes
                     and all(re.fullmatch(r"[0-9a-f]{64}", value) for value in state["item_result_sha256"])
                     and parse_time(source["row"]["started_at"]) <= parse_time(child["row"]["started_at"])
                     <= parse_time(child["row"]["completed_at"]) <= parse_time(source["row"]["completed_at"]),
                     "Historical local progress or source ownership changed")
            previous_indexes, previous_hashes = state["completed_indexes"], state["item_result_sha256"]
        final = child_chain[-1]
        _require(final["row"]["status"] == "succeeded" and final["details"]["checkpoint"]["complete"] is True
                 and previous_indexes == indexes and final["details"]["checkpoint"]["result_sha256"] == tikhub_scan._digest({
                     "raw_response_id": raw["id"], "eligible_indexes": indexes, "item_result_sha256": previous_hashes,
                 }), "Historical materialization has no exact terminal progress")
        paid_ids = [source["claim"]["attempt_id"] for source in sources[1:]]
        child_ids = [child["claim"]["attempt_id"] for child in later_children]
        _no_paid_continuation(connection, paid_ids + child_ids)
        pages.append({"raw_response_id": raw["id"], "paid_run_ids": [run["id"]], "paid_attempt_ids": paid_ids,
                      "materialization_run_ids": [children[0]["id"]], "materialization_attempt_ids": child_ids,
                      "materialization_identity": identity, "eligible_content_ids": [item["content_id"] for item in eligible],
                      "materialization_started_at": child_chain[0]["row"]["started_at"],
                      "source_time_kind": "logical-source-time",
                      "source_completed_at": sources[-1]["row"]["completed_at"]})
    return pages


def _derived_evidence(
    connection: sqlite3.Connection, raw: sqlite3.Row, source_members: dict[int, dict[str, Any]], *, at: str,
) -> int:
    """A local derived raw must close against an accepted page and zero-cost slot."""
    _require(raw["source"] == "derived_applied" and raw["http_status"] == 200,
             "Post-START raw is neither a member response nor applied local derivation")
    _, value = capture._read_verified_raw_response(raw, connection=connection)
    _require(isinstance(value, dict) and type(value.get("source_raw_response_id")) is int
             and value["source_raw_response_id"] in source_members, "Derived raw has no verified diagnostic source")
    member = source_members[value["source_raw_response_id"]]
    identity = member["materialization_identity"]
    source = connection.execute("SELECT * FROM provider_raw_responses WHERE id=?", (value["source_raw_response_id"],)).fetchone()
    attempt = connection.execute("SELECT * FROM fetch_attempts WHERE id=?", (raw["fetch_attempt_id"],)).fetchone()
    _require(attempt is not None, "Derived raw has no exact local attempt")
    slot = connection.execute("SELECT * FROM fetch_slots WHERE id=?", (attempt["slot_id"],)).fetchone()
    content = connection.execute("SELECT account_id,platform FROM content_items WHERE id=?", (raw["content_id"],)).fetchone()
    # Explicit-now historical replay terminalizes owners with a logical clock;
    # capture still records wall time. Only the fully closed historical chain
    # above may use verification time, never a fabricated source completion.
    local_end = at if member.get("source_time_kind") == "logical-source-time" else member["source_completed_at"]
    _require(slot is not None and content is not None
             and raw["content_id"] in member["eligible_content_ids"]
             and content["account_id"] == identity["account_id"] and content["platform"] == identity["platform"]
             and slot["content_id"] == raw["content_id"] and slot["provider"].lower() == "tikhub"
             and slot["stage"] in identity["derived_operations"]
             and slot["window_key"] == ("lifetime" if slot["stage"] == "detail" else
                 parse_time(source["captured_at"]).astimezone(tikhub_scan.SHANGHAI).date().isoformat())
             and slot["adapter_version"] == identity["derived_adapter_version"]
             and slot["status"] == "succeeded" and slot["attempt_count"] == attempt["attempt_number"]
             and raw["operation"] == identity["derived_operations"][slot["stage"]]
             and value.get("stage") == slot["stage"]
             and value.get("derived_from_operation") == source["operation"]
             and value.get("source_sha256") == source["sha256"]
             and value.get("source_captured_at") == source["captured_at"]
             and attempt["billed"] == 0 and micro_usd(attempt["amount"]) == 0
             and attempt["http_status"] == 200 and attempt["error_code"] is None
             and parse_time(member["materialization_started_at"]) <= parse_time(attempt["request_started_at"])
             <= parse_time(raw["captured_at"]) <= parse_time(attempt["response_finished_at"])
             # The materializer persists its slice's logical `now`. The source
             # owner closes using a fresh clock after all local writes finish.
             <= parse_time(local_end) and parse_time(member["source_completed_at"]) <= parse_time(at),
             "Derived raw lineage, local attempt or materialization window changed")
    _require(connection.execute(
        "SELECT 1 FROM provider_usage WHERE json_extract(details_json,'$.slot_id')=? "
        "AND json_extract(details_json,'$.attempt_number')=? LIMIT 1", (slot["id"], attempt["attempt_number"]),
    ).fetchone() is None and connection.execute(
        "SELECT 1 FROM paid_provider_dispatch_events WHERE fetch_attempt_id=? LIMIT 1", (attempt["id"],),
    ).fetchone() is None, "Local derivation has an unexplained paid reservation or dispatch")
    return int(attempt["id"])


def verify_current_hold_diagnostic_tail(
    connection: sqlite3.Connection, start: paid_drain.DrainReceipt, *, at: str,
) -> dict[str, Any]:
    """Compare all actual post-HWM resources to exact verified diagnostic sets.

    No label-based whitelist and no provider requests. Unknown sends must retain
    immutable conservative accounting; nonterminal/local orphan rows fail closed.
    """
    frozen = start.payload["frozen_dispatch"]
    _require(all(type(frozen.get(field)) is int and frozen[field] >= 0 for field in _HWM.values()),
             "Current HOLD lacks prospective exact diagnostic watermarks")
    expected: dict[str, set[int]] = {key: set() for key in _HWM}
    owners = []
    accounting_receipts = []
    source_members = {}
    rows = connection.execute(
        "SELECT id FROM scheduler_runs WHERE job_id='transport_receipt:campaign' "
        "AND json_extract(details_json,'$.payload.hold_binding.start_event_hash')=? ORDER BY id",
        (start.event_hash,),
    ).fetchall()
    for row in rows:
        campaign = read_transport_receipt(connection, row["id"])
        _require(campaign["payload"]["hold_binding"]["drain_id"] == start.drain_id,
                 "Diagnostic campaign belongs to another HOLD")
        owner = read_primary_campaign_owners(connection, row["id"], at=at)
        owners.append(owner)
        for key in ("paid_run_ids", "paid_attempt_ids", "materialization_run_ids", "materialization_attempt_ids"):
            values = set(owner[key])
            _require(not (values & expected[key]), "Campaigns share original paid owners")
            expected[key].update(values)
        for member in owner["members"]:
            evidence = read_primary_member_accounting(connection, member["member_receipt_id"], at=at)
            _require(evidence["accounting_terminal"], "Diagnostic unknown lacks terminal conservative accounting")
            for target, source_key in (("usage_ids", "usage_id"), ("fetch_attempt_ids", "fetch_attempt_id"),
                                       ("raw_response_ids", "raw_response_id")):
                value = evidence.get(source_key)
                if value is not None:
                    _require(value not in expected[target], "Diagnostic members share a paid resource")
                    expected[target].add(value)
            expected["dispatch_event_ids"].update(evidence.get("dispatch_event_ids", []))
            if evidence.get("accounting_receipt_id") is not None:
                accounting_receipts.append(evidence["accounting_receipt_id"])
            if member["state"] == "succeeded":
                source_members[member["raw_response_id"]] = member

    for page in _historical_local_pages(connection, start, at=at, already_owned=expected["paid_run_ids"]):
        _require(page["raw_response_id"] not in source_members, "Historical raw belongs to multiple owners")
        source_members[page["raw_response_id"]] = page
        for key in ("paid_run_ids", "paid_attempt_ids", "materialization_run_ids", "materialization_attempt_ids"):
            _require(not (set(page[key]) & expected[key]), "Historical local owners overlap diagnostic owners")
            expected[key].update(page[key])

    raw_rows = connection.execute(
        "SELECT * FROM provider_raw_responses WHERE lower(provider)='tikhub' AND id>? ORDER BY id",
        (frozen["raw_response_high_watermark"],),
    ).fetchall()
    for raw in raw_rows:
        if raw["id"] in expected["raw_response_ids"]:
            continue
        attempt_id = _derived_evidence(connection, raw, source_members, at=at)
        _require(attempt_id not in expected["fetch_attempt_ids"], "Local derivations share a fetch attempt")
        expected["fetch_attempt_ids"].add(attempt_id)
        expected["raw_response_ids"].add(raw["id"])

    actual = {
        "usage_ids": {row[0] for row in connection.execute("SELECT id FROM provider_usage WHERE lower(provider)='tikhub' AND id>?", (frozen["provider_usage_high_watermark"],))},
        "fetch_attempt_ids": {row[0] for row in connection.execute("SELECT a.id FROM fetch_attempts a JOIN fetch_slots s ON s.id=a.slot_id WHERE lower(s.provider)='tikhub' AND a.id>?", (frozen["fetch_attempt_high_watermark"],))},
        "raw_response_ids": {row["id"] for row in raw_rows},
        "dispatch_event_ids": {row[0] for row in connection.execute("SELECT id FROM paid_provider_dispatch_events WHERE id>?", (frozen["dispatch_event_high_watermark"],))},
        "paid_run_ids": set(), "paid_attempt_ids": set(),
        "materialization_run_ids": set(), "materialization_attempt_ids": set(),
    }
    for run in connection.execute("SELECT id,job_id,status,details_json FROM scheduler_runs ORDER BY id"):
        if run["job_id"] == tikhub_scan.MATERIALIZATION_JOB:
            if run["id"] > frozen["scheduler_run_high_watermark"]:
                actual["materialization_run_ids"].add(run["id"])
            actual["materialization_attempt_ids"].update(row[0] for row in connection.execute(
                "SELECT id FROM scheduler_run_attempts WHERE scheduler_run_id=? AND id>?",
                (run["id"], frozen["scheduler_attempt_high_watermark"]),
            ))
            continue
        if not paid_drain._paid_capable_run(run):
            continue
        if (str(run["job_id"]) in paid_drain._MATRIX_JOBS and run["status"] != "running"
                and paid_drain._network_requests(paid_drain._run_details(run)) == 0):
            continue  # Exact existing current-HOLD zero-network Matrix fence.
        if run["id"] > frozen["scheduler_run_high_watermark"]:
            actual["paid_run_ids"].add(run["id"])
        actual["paid_attempt_ids"].update(row[0] for row in connection.execute(
            "SELECT id FROM scheduler_run_attempts WHERE scheduler_run_id=? AND id>?",
            (run["id"], frozen["scheduler_attempt_high_watermark"]),
        ))
    for key, hwm in _HWM.items():
        expected[key] = {value for value in expected[key] if value > frozen[hwm]}
        _require(actual[key] == expected[key],
                 f"Unexplained post-START {key}: extra={sorted(actual[key] - expected[key])}, missing={sorted(expected[key] - actual[key])}")
    result = {"contract_version": CONTRACT_VERSION, "start_event_hash": start.event_hash,
              "checked_at": at, "verified_ids": {key: sorted(value) for key, value in expected.items()},
              "campaign_terminal_receipt_ids": [owner["terminal_receipt_id"] for owner in owners],
              "accounting_receipt_ids": sorted(accounting_receipts), "qualified": False}
    result["sha256"] = hashlib.sha256(canonical_json_bytes(result)).hexdigest()
    return result
