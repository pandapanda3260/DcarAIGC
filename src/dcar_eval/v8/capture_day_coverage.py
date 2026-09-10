"""Fixed-cutoff coverage for catalog-bound integrated discovery, read only.

A terminal work is not necessarily complete: bounded partial scans deliberately
finish unsuccessfully. Only complete attempts, quality, watermark and raw page
chains establish coverage. Historical catalog scopes never use today's roster.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, time, timedelta
from types import SimpleNamespace
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from . import capture_planning as planning, durable_runs, raw_archive, tikhub_scan
from .source_routing import parse_time

CONTRACT = "catalog-day-coverage-v1"
SOURCE_CONTRACT = "catalog-day-coverage-source-v1"
SNAPSHOT_CONTRACT = "account-catalog-capture-snapshot-v1"
BEIJING = ZoneInfo("Asia/Shanghai")
EPOCH_KEYS = ("activation_id", "activation_sha256", "profile_id", "roster_snapshot_id", "roster_members_sha256")
MEMBER_KEYS = ("identity_id", "account_id", "platform", "uid")
RUN_KEYS = ("id", "job_id", "scheduled_for", "root_run_id", "continuation_sequence", "charge_business_day")
RAW_KEYS = ("id", "fetch_attempt_id", "account_id", "content_id", "provider", "operation",
            "sha256", "byte_size", "captured_at", "http_status", "transport_receipt_id")
SCOPE_KEYS = ("contract_version", "identity_id", "account_id", "platform", "uid", "content_id", "stage",
              "capture_stage", "category", "source_stage", "operation", "logical_due", "assignment_id",
              "source_plan_id", "activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256",
              "catalog_plan_id", "window_start", "window_end")


def _require(value: bool, reason: str) -> None:
    if not value:
        raise ValueError(reason)


def _object(value: str) -> dict[str, Any]:
    result = json.loads(value)
    _require(isinstance(result, dict), "catalog_receipt_not_object")
    return result


def _record(connection: sqlite3.Connection, table: str, identity: int) -> dict[str, Any]:
    row = connection.execute(f"SELECT * FROM {table} WHERE id=?", (identity,)).fetchone()
    _require(row is not None, "catalog_source_missing:" + table)
    return dict(row)


def _plan(connection: sqlite3.Connection, identity: int, cutoff: str) -> dict[str, Any]:
    row = _record(connection, "capture_source_plans", identity)
    value = _object(row["payload_json"])
    snap = value.get("catalog_snapshot")
    _require(row["mode"] == "active" and value.get("shadow") is False
        and value.get("catalog_mode") == "active" and value.get("profile_id") == "integrated_route_v1"
        and value.get("contract_version") == "capture-runtime-v1"
        and value.get("business_day") == row["business_day"]
        and planning.digest(value) == row["plan_sha256"]
        and parse_time(row["created_at"]) <= parse_time(cutoff), "catalog_plan_invalid")
    _require(isinstance(snap, dict) and snap.get("contract") == SNAPSHOT_CONTRACT
        and snap.get("snapshot_sha256") == planning.digest({k: v for k, v in snap.items() if k != "snapshot_sha256"})
        and value.get("catalog_snapshot_sha256") == snap["snapshot_sha256"], "catalog_snapshot_invalid")
    eligible = snap["eligibility"]["eligible_members"]
    _require(isinstance(eligible, list) and bool(eligible), "catalog_empty_scope")
    ids = [item["identity_id"] for item in eligible]
    _require(all(type(i) is int and i > 0 for i in ids) and len(ids) == len(set(ids)), "catalog_identity_ambiguous")
    _require(all(m.get("eligible") is True and m.get("reason_code") == "eligible"
        and m.get("platform") in {"douyin", "xiaohongshu"}
        and m.get("account_identity_id") == m.get("identity_id") and bool(m.get("uid")) for m in eligible),
        "catalog_member_invalid")
    selection = [{**{k: m[k] for k in ("account_identity_id", "account_id", "platform", "uid")},
                  "locator_sha256": m["locator_sha256"]} for m in eligible]
    _require(ids == sorted(ids) and planning.digest(selection) == snap["eligibility"].get("selection_sha256"),
        "catalog_selection_changed")
    cohort = value["cohort"]
    _require(len(cohort) == len(eligible) and all(sum(all(c.get(k) == m[k] for k in MEMBER_KEYS)
        for c in cohort) == 1 for m in eligible), "catalog_cohort_differs")
    return {"id": identity, "created_at": row["created_at"], "plan_sha256": row["plan_sha256"], "payload": value}


def _scope(plan: Mapping[str, Any]) -> tuple[Any, ...]:
    p = plan["payload"]
    return tuple(p[key] for key in EPOCH_KEYS) + (_business_scope_sha256(plan),)


def _business_scope_sha256(plan: Mapping[str, Any]) -> str:
    snapshot = plan["payload"]["catalog_snapshot"]
    # A refreshed locator receipt or excluded row's label does not change the
    # collection obligation. Identity, locator value and cadence changes do.
    return planning.digest({"policy_sha256": snapshot["policy_sha256"], "members": [
        {k: member.get(k) for k in (*MEMBER_KEYS, "locator_sha256", "account_status")}
        for member in snapshot["eligibility"]["eligible_members"]]})


def _day_plans(connection: sqlite3.Connection, rows: list[dict[str, Any]], day: str,
               cutoff: str) -> list[dict[str, Any]]:
    lower = datetime.combine(date.fromisoformat(day), time.min, BEIJING)
    upper = lower + timedelta(days=1)
    prior = [r for r in rows if parse_time(r["created_at"]) <= lower]
    _require(bool(prior), "catalog_switch_day_scope_unknown")
    baseline = _plan(connection, prior[-1]["id"], cutoff)
    relevant = [r for r in rows if lower < parse_time(r["created_at"]) < upper]
    plans = [baseline] + [_plan(connection, r["id"], cutoff) for r in relevant]
    _require(any(p["payload"]["business_day"] == day for p in plans), "catalog_day_snapshot_missing")
    _require(all(_scope(p) == _scope(baseline) for p in plans), "catalog_day_scope_changed")
    return plans


def _plan_ref(plan: Mapping[str, Any]) -> dict[str, Any]:
    return {"id": plan["id"], "created_at": plan["created_at"], "plan_sha256": plan["plan_sha256"],
            "business_day": plan["payload"]["business_day"]}


def _input_ref(row: Mapping[str, Any]) -> dict[str, Any]:
    return {key: row[key] for key in ("id", "mode", "created_at", "business_day", "plan_sha256")} | {
        "payload_sha256": planning.digest(_object(row["payload_json"]))}


def _work_scope(row: Mapping[str, Any]) -> dict[str, Any]:
    env = _object(row["envelope_json"])
    return {"id": row["id"], "work_identity": row["work_identity"], "account_id": row["account_id"],
            "operation": row["operation"], "source_plan_id": row["source_plan_id"],
            "assignment_id": row["assignment_id"], "data_business_day": row["data_business_day"],
            "created_at": row["created_at"], "envelope_scope": {k: env.get(k) for k in SCOPE_KEYS}}


def _raw_ref(connection: sqlite3.Connection, raw_id: int) -> dict[str, Any]:
    row = _record(connection, "provider_raw_responses", raw_id)
    return {key: row[key] for key in RAW_KEYS}


def _run_binding(connection: sqlite3.Connection, run_id: int, details: Mapping[str, Any],
                 work: Mapping[str, Any], plan_id: int) -> dict[str, Any]:
    run = _record(connection, "scheduler_runs", run_id)
    root_identity = {"work_identity": work["work_identity"], "business_day": work["data_business_day"],
                     "catalog_plan_id": plan_id}
    expected = dict(root_identity)
    result = {"run": {k: run[k] for k in RUN_KEYS}}
    root = run
    if run["root_run_id"] is not None:
        root = _record(connection, "scheduler_runs", run["root_run_id"])
        result["root"] = {k: root[k] for k in RUN_KEYS}
        expected.update(data_business_day=work["data_business_day"], business_day=run["charge_business_day"],
            continuation={"root_run_id": root["id"], "sequence": run["continuation_sequence"],
                          "charge_business_day": run["charge_business_day"]})
        _require(type(run["continuation_sequence"]) is int and run["continuation_sequence"] > 0
            and run["charge_business_day"] >= work["data_business_day"], "catalog_child_lineage_invalid")
    _require(root["root_run_id"] is None and root["continuation_sequence"] is None
        and root["charge_business_day"] is None and run["job_id"] == root["job_id"] == "capture_integrated_work"
        and run["scheduled_for"] == root["scheduled_for"] == "scan:" + durable_runs.scan_identity("capture_integrated_work", root_identity)
        and details.get("identity") == expected
        and details.get("scan_id") == durable_runs.scan_identity("capture_integrated_work", expected),
        "catalog_scheduler_identity_invalid")
    return result


def verify_work(connection: sqlite3.Connection, work: Mapping[str, Any], *, plan: Mapping[str, Any],
                cutoff_at: str) -> dict[str, Any]:
    """Deep check one complete frozen integrated scan and its actual raw pages."""
    env = _object(work["envelope_json"])
    p = plan["payload"]
    _require(work["state"] == "terminal" and not work["reason"] and work["completed_at"]
        and parse_time(work["completed_at"]) <= parse_time(cutoff_at), "catalog_scan_not_complete_at_cutoff")
    _require(env.get("stage") == "discovery" and env.get("capture_stage") == "discovery"
        and env.get("source_stage") == "discovery" and env.get("category") == "reconcile"
        and env.get("content_id") is None and work["content_id"] is None
        and env.get("kind") is None and env.get("manual_command_run_id") is None
        and "compensation" not in env and env.get("catalog_plan_id") == plan["id"]
        and env.get("source_plan_id") == work["source_plan_id"] == plan["id"]
        and env.get("assignment_id") == work["assignment_id"] and env.get("account_id") == work["account_id"]
        and env.get("operation") == work["operation"] == env["platform"] + "_user_posts"
        and work["provider"] == "tikhub"
        and all(env[key] == p[key] for key in EPOCH_KEYS if key != "activation_sha256"), "catalog_work_scope_invalid")
    members = p["catalog_snapshot"]["eligibility"]["eligible_members"]
    _require(sum(all(env[key] == m[key] for key in MEMBER_KEYS) for m in members) == 1,
        "catalog_work_member_mismatch")
    expected = planning.digest({"provider": "tikhub", "operation": env["operation"],
        "subject": f"account:{env['identity_id']}", "logical_due": env["logical_due"]})
    _require(expected == work["work_identity"] and parse_time(env["window_start"]) < parse_time(env["window_end"])
        and parse_time(plan["created_at"]) <= parse_time(work["created_at"]), "catalog_work_identity_invalid")
    attempts = connection.execute("""SELECT a.* FROM scheduler_run_attempts a
        JOIN scheduler_runs r ON r.id=a.scheduler_run_id WHERE r.job_id='capture_integrated_work'
        AND a.status='succeeded' AND julianday(a.completed_at)<=julianday(?)
        AND json_extract(a.details_json,'$.identity.work_identity')=? ORDER BY a.id DESC""",
        (cutoff_at, work["work_identity"])).fetchall()
    matching = []
    for attempt in attempts:
        details = _object(attempt["details_json"])
        identity = details.get("identity", {})
        cp = details.get("checkpoint", {})
        if (details.get("contract_version") == durable_runs.CONTRACT_VERSION and details.get("complete") is True
                and cp.get("complete") is True and identity.get("catalog_plan_id") == plan["id"]
                and identity.get("data_business_day", identity.get("business_day")) == work["data_business_day"]
                and cp.get("last_result", {}).get("work_id") == work["id"]):
            try:
                run_binding = _run_binding(connection, attempt["scheduler_run_id"], details, work, plan["id"])
            except (ValueError, TypeError, KeyError):
                continue
            matching.append((attempt, details, cp["last_result"], run_binding))
    _require(bool(matching), "catalog_successful_attempt_missing")
    attempt, details, evidence, run_binding = matching[0]
    _require(evidence.get("contract_version") == "capture-runtime-v1" and evidence.get("complete") is True
        and evidence.get("terminal_cursor") is True and evidence.get("all_raw_verified") is True
        and evidence.get("cap_hit") is False and evidence.get("cursor_loop") is False
        and evidence.get("disposition", "complete") == "complete", "catalog_scan_partial")
    _require(all(evidence.get(k) == env[k] for k in ("identity_id", "account_id", "platform", "operation", "window_start", "window_end")),
        "catalog_quality_scope_mismatch")
    receipts = connection.execute("SELECT * FROM data_quality_receipts WHERE scope_key=? "
        "AND julianday(recorded_at)<=julianday(?) AND julianday(cutoff_at)<=julianday(?) ORDER BY id",
        (f"capture-scan:{work['id']}", cutoff_at, cutoff_at)).fetchall()
    quality = next((r for r in receipts if r["receipt_sha256"] == planning.digest(evidence)
        and planning.canonical(_object(r["payload_json"])) == planning.canonical(evidence)), None)
    _require(quality is not None, "catalog_quality_receipt_missing")
    watermark = connection.execute("SELECT * FROM capture_watermarks WHERE work_id=?", (work["id"],)).fetchone()
    wm_evidence = {k: v for k, v in evidence.items() if k not in {"contract_version", "work_id"}}
    _require(watermark is not None and watermark["provider"] == "tikhub"
        and watermark["operation"] == env["operation"] and watermark["scope_key"] == f"{env['platform']}:{env['uid']}"
        and parse_time(watermark["complete_through"]) == parse_time(env["window_end"])
        and parse_time(watermark["recorded_at"]) <= parse_time(cutoff_at)
        and planning.canonical(_object(watermark["evidence_json"])) == planning.canonical(wm_evidence),
        "catalog_watermark_mismatch")
    raw_ids = evidence.get("raw_response_ids")
    _require(isinstance(raw_ids, list) and 1 <= len(raw_ids) <= 32
        and all(type(i) is int for i in raw_ids) and len(set(raw_ids)) == len(raw_ids), "catalog_raw_chain_invalid")
    cursor: Any = 0 if env["platform"] == "douyin" else ""
    seen_cursors = set()
    counts = {key: 0 for key in ("seen", "valid", "missing", "invalid", "unavailable")}
    raw_refs = []
    for index, raw_id in enumerate(raw_ids):
        raw = _raw_ref(connection, raw_id)
        cursor_key = planning.canonical(cursor)
        _require(cursor_key not in seen_cursors, "catalog_cursor_loop")
        seen_cursors.add(cursor_key)
        _require(raw["provider"].lower() == "tikhub" and raw["operation"] == env["operation"]
            and raw["account_id"] == env["account_id"] and raw["content_id"] is None
            and parse_time(raw["captured_at"]) <= parse_time(cutoff_at)
            and parse_time(raw["captured_at"]) >= parse_time(work["created_at"])
            and raw["http_status"] == 200, "catalog_raw_scope_mismatch")
        fetch = _record(connection, "fetch_attempts", raw["fetch_attempt_id"])
        slot = _record(connection, "fetch_slots", fetch["slot_id"])
        expected_window = env["logical_due"] + ":cursor:" + planning.digest(cursor)[:24]
        _require(slot["account_id"] == env["account_id"] and slot["content_id"] is None
            and slot["stage"] == "discovery" and slot["window_key"] == expected_window,
            "catalog_raw_cursor_binding_mismatch")
        value = json.loads(raw_archive.read_response_entity(connection, raw_id))
        items, more, cursor, _ = tikhub_scan._page(SimpleNamespace(value=value), env["platform"])
        _require(more is (index < len(raw_ids) - 1), "catalog_pagination_not_closed")
        if more:
            _require(cursor not in (None, ""), "catalog_next_cursor_missing")
        for item in items:
            proof = tikhub_scan._item_evidence(env["platform"], item)
            counts["seen"] += 1
            counts["valid" if proof["event_tuple"] is not None else "invalid"] += 1
        raw_refs.append(raw)
    _require(counts["invalid"] == 0 and all(type(evidence.get(k)) is int and evidence[k] == v
        for k, v in counts.items()), "catalog_raw_counts_mismatch")
    attempt_ref = {k: attempt[k] for k in ("id", "scheduler_run_id", "attempt_number", "status", "started_at", "completed_at")}
    attempt_ref["details_sha256"] = planning.digest(details)
    return {"work_id": work["id"], "work_scope": _work_scope(work), "run_id": attempt["scheduler_run_id"],
        "attempt": attempt_ref, "run_binding": run_binding,
        "quality_receipt_id": quality["id"], "quality_receipt_sha256": quality["receipt_sha256"],
        "watermark_id": watermark["id"], "watermark_sha256": planning.digest(dict(watermark)),
        "raw": raw_refs, "window_start": env["window_start"], "window_end": env["window_end"],
        "identity_id": env["identity_id"], "source_plan_id": plan["id"]}


def catalog_day_coverage(connection: sqlite3.Connection, *, day: str, cutoff_at: str) -> dict[str, Any] | None:
    """Return a compatible day record, or None for a genuinely noncatalog epoch."""
    if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='capture_source_plans'").fetchone() is None:
        return None
    lower = datetime.combine(date.fromisoformat(day), time.min, BEIJING)
    upper = lower + timedelta(days=1)
    rows = [dict(r) for r in connection.execute("SELECT * FROM capture_source_plans "
        "WHERE julianday(created_at)<=julianday(?) ORDER BY created_at,id", (cutoff_at,))]
    catalog_rows = [r for r in rows if isinstance(_object(r["payload_json"]).get("catalog_snapshot"), dict)
                    or _object(r["payload_json"]).get("catalog_mode") == "active"]
    if not any(r["business_day"] <= day for r in catalog_rows):
        return None
    result: dict[str, Any] = {"date": day,"known":False,"complete":False,"partial_publishable":False,
        "coverage_contract":CONTRACT,"source_family":"system","eligible_identity_ids":[],"covered_identity_ids":[],
        "succeeded_identity_ids":[],"blocked_identity_ids":[],"not_applicable_identity_ids":[],
        "accounted_identity_ids":[],"required_identity_ids":[],"matrix_run_ids":[],"tikhub_run_ids":[],
        "terminal_blockers":{},"matrix_expected_windows":0,"matrix_complete_windows":0,"matrix_forbidden_run_ids":[],
        "integrated_work_ids":[],"integrated_run_ids":[],"catalog_source_plan_ids":[],"scan_references":[],
        "scan_errors":{},"diagnostic_scan_errors":{}}
    binding: dict[str, Any] = {"contract":SOURCE_CONTRACT,"date":day,"cutoff_at":cutoff_at,"plans":[],"works":[],
                             "plan_inputs":[_input_ref(r) for r in rows]}
    result["source_binding"] = binding
    # Preserve runtime authorization context even on a switch/unknown day.
    from .profile_activations import activation_at
    try:
        active = activation_at(connection, cutoff_at)
        if active is not None:
            result.update(activation_id=active["activation_id"],activation_sha256=active["activation_sha256"],
                profile_id=active["profile_id"],roster_snapshot_id=active["roster_snapshot_id"],
                roster_snapshot_hash=active["roster_members_sha256"])
            binding.update({k:active[k] for k in EPOCH_KEYS})
    except (ValueError,TypeError,KeyError,sqlite3.Error):
        pass
    try:
        plans = _day_plans(connection, rows, day, cutoff_at)
        baseline = plans[0]
        p = baseline["payload"]
        binding.update({k:p[k] for k in EPOCH_KEYS})
        binding.update(catalog_snapshot_sha256=p["catalog_snapshot_sha256"],
            catalog_scope_sha256=_business_scope_sha256(baseline),
            policy_sha256=p["catalog_snapshot"]["policy_sha256"],plans=[_plan_ref(v) for v in plans])
        ids = sorted(m["identity_id"] for m in p["catalog_snapshot"]["eligibility"]["eligible_members"])
        result.update(known=True,activation_id=p["activation_id"],activation_sha256=p["activation_sha256"],
            profile_id=p["profile_id"],roster_snapshot_id=p["roster_snapshot_id"],roster_snapshot_hash=p["roster_members_sha256"],
            catalog_snapshot_sha256=p["catalog_snapshot_sha256"],eligible_identity_ids=ids,required_identity_ids=ids,
            catalog_scope_sha256=_business_scope_sha256(baseline),
            catalog_source_plan_ids=[v["id"] for v in plans],anchor_scheduled_at=upper.isoformat(),round_run_id=None)
        works = connection.execute("SELECT * FROM capture_work_items WHERE json_extract(envelope_json,'$.stage')='discovery' "
            "AND json_type(envelope_json,'$.catalog_plan_id')='integer' AND julianday(created_at)<=julianday(?) ORDER BY id",
            (cutoff_at,)).fetchall()
        covered = set()
        failed = {}
        for work in works:
            env = _object(work["envelope_json"])
            if env.get("identity_id") not in ids or parse_time(env["window_start"]) > lower or parse_time(env["window_end"]) < upper:
                continue
            try:
                work_plan = _plan(connection,env["catalog_plan_id"],cutoff_at)
                _require(_scope(work_plan) == _scope(baseline), "catalog_scan_epoch_differs")
                proof = verify_work(connection,dict(work),plan=work_plan,cutoff_at=cutoff_at)
                if env["identity_id"] in covered:
                    continue
                covered.add(env["identity_id"])
                if work_plan["id"] not in {r["id"] for r in binding["plans"]}:
                    binding["plans"].append(_plan_ref(work_plan))
                binding["works"].append(proof)
            except (ValueError,TypeError,KeyError,OSError,RuntimeError,sqlite3.Error) as exc:
                failed[str(work["id"])]=(env["identity_id"],str(exc))
        result["scan_errors"] = {key: error for key,(identity,error) in failed.items() if identity not in covered}
        result["diagnostic_scan_errors"] = {key: error for key,(identity,error) in failed.items() if identity in covered}
        result.update(covered_identity_ids=sorted(covered),succeeded_identity_ids=sorted(covered),
            accounted_identity_ids=sorted(covered),blocked_identity_ids=sorted(set(ids)-covered),
            integrated_work_ids=[w["work_id"] for w in binding["works"]],
            integrated_run_ids=[w["run_id"] for w in binding["works"]],scan_references=binding["works"],
            complete=bool(ids) and covered==set(ids),success_percentage=round(100*len(covered)/len(ids),2),
            accounted_percentage=round(100*len(covered)/len(ids),2))
        result["reason"]="" if result["complete"] else "catalog_scan_window_or_evidence_gap"
    except (ValueError,TypeError,KeyError,OSError,RuntimeError,sqlite3.Error) as exc:
        result["reason"]=str(exc)
    binding.update(known=result["known"],complete=result["complete"],reason=result["reason"])
    binding["binding_sha256"]=planning.digest({k:v for k,v in binding.items() if k!="binding_sha256"})
    return result


def validate_source_binding(connection: sqlite3.Connection, binding: Mapping[str, Any], at: str) -> dict[str, Any]:
    """Verify frozen DB lineage without opening raw blobs or mutable run status.

    A later retry can change scheduler_runs; the successful frozen attempt,
    quality receipt and watermark remain the authority for this fixed cutoff.
    """
    _require(binding.get("contract")==SOURCE_CONTRACT and parse_time(binding["cutoff_at"])<=parse_time(at)
        and binding.get("binding_sha256")==planning.digest({k:v for k,v in binding.items() if k!="binding_sha256"}),
        "catalog_source_binding_invalid")
    rows=[dict(r) for r in connection.execute("SELECT * FROM capture_source_plans "
        "WHERE julianday(created_at)<=julianday(?) ORDER BY created_at,id",(binding["cutoff_at"],))]
    observed=[_input_ref(r) for r in rows]
    _require(observed==binding["plan_inputs"],"catalog_source_plan_inputs_changed")
    _require(binding.get("known") is True or binding.get("complete") is False,"catalog_unknown_cannot_complete")
    try:
        day_plans = _day_plans(connection, rows, binding["date"], binding["cutoff_at"])
        scope_reason = ""
    except (ValueError,TypeError,KeyError,OSError,RuntimeError,sqlite3.Error) as exc:
        day_plans, scope_reason = [], str(exc)
    _require(binding["known"] == bool(day_plans), "catalog_bound_scope_state_changed")
    if not day_plans:
        _require(binding["reason"] == scope_reason and not binding["plans"] and not binding["works"],
            "catalog_bound_unknown_reason_changed")
    bound_plans = {}
    for ref in binding["plans"]:
        plan = _plan(connection,ref["id"],binding["cutoff_at"])
        _require(_plan_ref(plan)==ref,"catalog_bound_plan_changed")
        bound_plans[plan["id"]] = plan
    required = set()
    if binding["known"]:
        _require(bool(bound_plans),"catalog_known_scope_missing")
        first = next(iter(bound_plans.values()))
        _require(all(_scope(p)==_scope(first) for p in bound_plans.values()),"catalog_bound_scope_changed")
        _require(all(_plan_ref(p) in binding["plans"] for p in day_plans)
            and binding["catalog_scope_sha256"] == _business_scope_sha256(first)
            and binding["catalog_snapshot_sha256"] == first["payload"]["catalog_snapshot_sha256"]
            and binding["policy_sha256"] == first["payload"]["catalog_snapshot"]["policy_sha256"]
            and all(binding[k] == first["payload"][k] for k in EPOCH_KEYS), "catalog_bound_epoch_changed")
        required = {m["identity_id"] for m in first["payload"]["catalog_snapshot"]["eligibility"]["eligible_members"]}
    succeeded = set()
    lower = datetime.combine(date.fromisoformat(binding["date"]),time.min,BEIJING)
    upper = lower + timedelta(days=1)
    for proof in binding["works"]:
        work=_record(connection,"capture_work_items",proof["work_id"])
        _require(_work_scope(work)==proof["work_scope"],"catalog_bound_work_changed")
        attempt=_record(connection,"scheduler_run_attempts",proof["attempt"]["id"])
        ref={k:attempt[k] for k in ("id","scheduler_run_id","attempt_number","status","started_at","completed_at")}
        ref["details_sha256"]=planning.digest(_object(attempt["details_json"]))
        _require(ref==proof["attempt"] and attempt["status"]=="succeeded","catalog_bound_attempt_changed")
        details = _object(attempt["details_json"])
        _require(proof["run_id"] == attempt["scheduler_run_id"]
            and _run_binding(connection,proof["run_id"],details,work,proof["source_plan_id"]) == proof["run_binding"],
            "catalog_bound_scheduler_changed")
        _require(details.get("contract_version") == durable_runs.CONTRACT_VERSION
            and details.get("complete") is True and details.get("checkpoint",{}).get("complete") is True
            and parse_time(attempt["completed_at"])<=parse_time(binding["cutoff_at"]),"catalog_bound_attempt_incomplete")
        quality=_record(connection,"data_quality_receipts",proof["quality_receipt_id"])
        evidence = _object(quality["payload_json"])
        _require(quality["receipt_sha256"]==proof["quality_receipt_sha256"]
            and planning.digest(evidence)==quality["receipt_sha256"]
            and evidence == details["checkpoint"].get("last_result")
            and evidence.get("work_id") == proof["work_id"] and evidence.get("complete") is True
            and quality["scope_key"] == f"capture-scan:{proof['work_id']}"
            and all(parse_time(quality[k]) <= parse_time(binding["cutoff_at"]) for k in ("recorded_at","cutoff_at")),
            "catalog_bound_quality_changed")
        _require(planning.digest(_record(connection,"capture_watermarks",proof["watermark_id"]))==proof["watermark_sha256"],
            "catalog_bound_watermark_changed")
        _require([_raw_ref(connection,r["id"]) for r in proof["raw"]]==proof["raw"],"catalog_bound_raw_changed")
        _require(proof["identity_id"] in required and proof["identity_id"] not in succeeded
            and proof["source_plan_id"] in bound_plans
            and proof["source_plan_id"] == work["source_plan_id"]
            and all(proof[k] == proof["work_scope"]["envelope_scope"][k] == evidence[k]
                for k in ("identity_id","window_start","window_end"))
            and parse_time(proof["window_start"])<=lower and parse_time(proof["window_end"])>=upper,
            "catalog_bound_coverage_scope_mismatch")
        succeeded.add(proof["identity_id"])
    _require(binding["complete"] == bool(binding["known"] and required and required==succeeded),
        "catalog_bound_complete_mismatch")
    return {"contract":SOURCE_CONTRACT,"binding_sha256":binding["binding_sha256"],
            "cutoff_at":binding["cutoff_at"],"known":binding["known"],"complete":binding["complete"],
            "reason":binding["reason"],"valid":True}
