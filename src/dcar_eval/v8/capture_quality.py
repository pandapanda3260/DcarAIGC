"""Bounded local maintenance and internal-only capture quality receipts.

Readiness, billable starts and data coverage are separate measurements. Missing
reconciliation denominators stay unknown; this module never opens a paid gate.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Any

from . import capture_planning as planning, usage_settlements
from .storage import DEFAULT_DB, connect, now_utc, transaction

CONTRACT = "capture-quality-v1"


def inventory_reconciliation(connection: Any, *, business_day: str, at: str) -> dict[str, Any]:
    """Compare verified complete-scan identities with storage; no provider I/O.

    A supplier's visible inventory is not an independent census of the platform.
    Overlap appearances are reported separately from duplicate stored videos.
    Old receipts without the typed identity list remain unknown, never empty.
    """
    day = datetime.fromisoformat(business_day).date()
    start = datetime.combine(day, datetime.min.time(), planning.BEIJING)
    end = start + timedelta(days=1)
    expected: dict[tuple[str, str], dict[str, Any]] = {}
    untyped = appearances = repeated = 0
    scopes: set[int] = set()
    rows = connection.execute("""SELECT m.evidence_json,m.work_id,w.envelope_json
        FROM capture_watermarks m JOIN capture_work_items w ON w.id=m.work_id
        WHERE m.recorded_at<=? AND julianday(json_extract(m.evidence_json,'$.window_start'))<julianday(?)
          AND julianday(json_extract(m.evidence_json,'$.window_end'))>julianday(?) ORDER BY m.id""",
        (planning.timestamp(at), end.isoformat(), start.isoformat())).fetchall()
    for row in rows:
        evidence = json.loads(row["evidence_json"])
        if evidence.get("inventory_contract") != "verified-provider-scan-inventory-v1":
            untyped += 1
            continue
        scopes.add(evidence["identity_id"])
        repeated += int(evidence["repeated_video_appearances"])
        for identifier, item in evidence["video_inventory"].items():
            published = datetime.fromisoformat(item["published_at"].replace("Z", "+00:00"))
            if not start <= published < end:
                continue
            appearances += 1
            key = (evidence["platform"], identifier)
            candidate = {**item, "account_id": evidence["account_id"], "work_id": row["work_id"]}
            if key not in expected or candidate["first_captured_at"] < expected[key]["first_captured_at"]:
                expected[key] = candidate
    missing: list[dict[str, Any]] = []
    duplicate = valid = late = 0
    delays: list[float] = []
    for (platform, identifier), item in sorted(expected.items()):
        stored = connection.execute("SELECT id,account_id,content_type,published_at FROM content_items WHERE platform=? AND platform_content_id=?",
                                    (platform, identifier)).fetchall()
        matching = [row for row in stored if row["account_id"] == item["account_id"] and row["content_type"] == "video"
                    and row["published_at"] is not None and planning.timestamp(row["published_at"]) == planning.timestamp(item["published_at"])]
        duplicate += max(0, len(stored)-1)
        if matching:
            valid += 1
        else:
            missing.append({"platform": platform, "platform_content_id": identifier, "work_id": item["work_id"],
                            "raw_response_id": item["raw_response_id"]})
        captured = datetime.fromisoformat(item["first_captured_at"].replace("Z", "+00:00"))
        published = datetime.fromisoformat(item["published_at"].replace("Z", "+00:00"))
        delay = (captured-published).total_seconds()
        if delay >= 0:
            delays.append(delay)
            late += int(delay > (10800 if platform == "xiaohongshu" else 7200))
    delays.sort()
    count = len(expected)
    return {"denominator_kind": "verified_provider_visible_inventory", "independent_platform_census": False,
        "complete_scan_accounts": len(scopes), "legacy_untyped_receipts": untyped,
        "expected": count, "valid": valid, "missing": len(missing), "missing_evidence": missing,
        "stored_duplicates": duplicate, "overlap_appearances": max(0, appearances-count)+repeated,
        "materialization_completeness": valid/count if count else None,
        "duplicate_rate": duplicate/(count+duplicate) if count else None,
        "discovery_latency_samples": len(delays), "discovery_p95_seconds": delays[max(0,(95*len(delays)+99)//100-1)] if delays else None,
        "late": late, "empty_inventory_is_full_coverage": False}


def local_retention_tick(*, db_path: Path = DEFAULT_DB, at: str | None = None) -> dict[str, Any]:
    """Apply only the explicitly installed local retention and capacity policy."""
    from . import capture_release, local_raw_retention
    from .runtime_database import require_current_process_writer_lock

    stamp = planning.timestamp(at or now_utc())
    current = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    bucket = current.replace(minute=current.minute // 5 * 5, second=0, microsecond=0)
    scope = "capture-local-retention-v1:" + planning.timestamp(bucket.isoformat())
    with connect(db_path) as connection:
        if connection.execute("PRAGMA user_version").fetchone()[0] != 20:
            return {"status": "skipped", "reason": "schema19_legacy", "provider_calls": 0}
        require_current_process_writer_lock(connection)
        prior = connection.execute("SELECT id FROM data_quality_receipts WHERE scope_key=? LIMIT 1", (scope,)).fetchone()
        if prior:
            return {"status": "already_recorded", "receipt_id": prior[0], "provider_calls": 0}
        try:
            installed = capture_release._installed_evidence(connection, at=stamp, maintenance_only=True)
            capacity = installed["storage_policy"]
            result = local_raw_retention.maintenance_tick(connection, at=stamp,
                live_root=Path(capacity["root"]),
                daily_stored_p95=capacity["daily_stored_p95"], max_blobs=256)
        except (ValueError, RuntimeError, OSError, KeyError, TypeError) as error:
            result = {"status": "blocked", "reason": str(error), "provider_calls": 0}
        with transaction(connection):
            _alert(connection, key="capture:local-retention", severity="P1", active=result["status"] == "blocked",
                   evidence=result, at=stamp)
            receipt = connection.execute("INSERT INTO data_quality_receipts(scope_key,cutoff_at,payload_json,recorded_at,receipt_sha256) VALUES(?,?,?,?,?)",
                (scope, stamp, planning.canonical(result), stamp, planning.digest({"scope": scope, "result": result})))
        return {**result, "receipt_id": receipt.lastrowid}


def _alert(connection: Any, *, key: str, severity: str, active: bool,
           evidence: dict[str, Any], at: str) -> None:
    row = connection.execute("SELECT id FROM operational_alerts WHERE dedupe_key=? AND status='open'", (key,)).fetchone()
    if active and row is None:
        connection.execute(
            "INSERT INTO operational_alerts(dedupe_key,severity,scope_json,evidence_json,owner,status,opened_at) "
            "VALUES (?,?,?,?,'project-release-owner','open',?)",
            (key, severity, planning.canonical({"pipeline": "integrated_route_v1"}), planning.canonical(evidence), at),
        )
    elif not active and row is not None:
        connection.execute("UPDATE operational_alerts SET status='resolved',resolved_at=? WHERE id=? AND status='open'",
                           (at, row[0]))


def measure(connection: Any, *, at: str) -> dict[str, Any]:
    """Return distinct work debt and raw/transport/accounting counters."""
    stamp = planning.timestamp(at)
    instant = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    day = instant.astimezone(planning.BEIJING).date()
    start = datetime.combine(day, datetime.min.time(), planning.BEIJING).astimezone(timezone.utc)
    end = start + timedelta(days=1)
    starts = connection.execute(
        "SELECT d.operation,count(*) starts,sum(CASE WHEN f.json_parse_ok=1 AND f.clean_eof=1 THEN 1 ELSE 0 END) complete_raw "
        "FROM provider_request_start_events e JOIN paid_provider_dispatch_events d ON d.id=e.provider_send_marker_id "
        "LEFT JOIN fetch_transport_receipts f ON f.fetch_attempt_id=d.fetch_attempt_id "
        "WHERE e.started_at>=? AND e.started_at<? GROUP BY d.operation",
        (start.isoformat(timespec="seconds").replace("+00:00", "Z"),
         end.isoformat(timespec="seconds").replace("+00:00", "Z")),
    ).fetchall()
    debt = planning.backlog(connection, at=stamp)
    for state in planning.WORK_STATES:
        debt.setdefault(state, {"count": 0, "oldest_seconds": 0})
    amounts = connection.execute(
        "SELECT currency,sum(amount_microunits) amount FROM provider_usage_settlements "
        "WHERE charge_business_day=? GROUP BY currency", (day.isoformat(),),
    ).fetchall()
    return {"contract_version": CONTRACT, "business_day": day.isoformat(), "cutoff_at": stamp,
            "debt": debt, "operations": [dict(row) for row in starts],
            "settled_conservative_amounts": [dict(row) for row in amounts],
            "coverage_complete": False, "video_completeness": None, "field_accuracy": None,
            "unknown_denominators": ["independent_expected_video_inventory", "simultaneous_platform_gold"],
            "blocked_in_sla_denominator": True, "blocked_in_runnable_backlog": False,
            "paid_gates_changed": False, "external_notifications": 0}


def maintenance_tick(*, db_path: Path = DEFAULT_DB, at: str | None = None) -> dict[str, Any]:
    stamp = planning.timestamp(at or now_utc())
    instant = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    bucket = instant.replace(minute=instant.minute // 5 * 5, second=0, microsecond=0)
    scope_key = "capture-maintenance:" + planning.timestamp(bucket.isoformat())
    with connect(db_path) as connection, transaction(connection):
        if connection.execute("PRAGMA user_version").fetchone()[0] != 20:
            return {"status": "skipped", "reason": "schema19_legacy", "provider_calls": 0}
        existing = connection.execute(
            "SELECT id,payload_json FROM data_quality_receipts WHERE scope_key=? ORDER BY id DESC LIMIT 1", (scope_key,),
        ).fetchone()
        if existing is not None:
            return {"status": "already_recorded", "receipt_id": existing[0], "provider_calls": 0}
        from .capture_runtime import recover_capture_leases

        recovered = recover_capture_leases(connection, at=stamp)
        settlement = usage_settlements.reconcile_due(connection, at=stamp)
        result = measure(connection, at=stamp)
        result.update(recovered_leases=recovered, settlement=settlement)
        runnable = result["debt"]["runnable"]
        critical = runnable["count"] > 2000 or runnable["oldest_seconds"] > 3600
        warning = runnable["count"] > 500 or runnable["oldest_seconds"] > 1800
        _alert(connection, key="capture:runnable_backlog:critical", severity="P1", active=critical,
               evidence=runnable, at=stamp)
        _alert(connection, key="capture:runnable_backlog:warning", severity="P2", active=warning and not critical,
               evidence=runnable, at=stamp)
        for state in ("provider_blocked", "paid_identity_hold", "budget_deferred"):
            item = result["debt"][state]
            _alert(connection, key="capture:debt:" + state, severity="P1" if state == "provider_blocked" else "P2",
                   active=item["count"] > 0 and item["oldest_seconds"] > 1800, evidence=item, at=stamp)
        receipt = connection.execute(
            "INSERT INTO data_quality_receipts(scope_key,cutoff_at,payload_json,recorded_at,receipt_sha256) VALUES (?,?,?,?,?)",
            (scope_key, stamp, planning.canonical(result), stamp, planning.digest({"scope": scope_key, "result": result})),
        )
    return {"status": "succeeded", "receipt_id": receipt.lastrowid, "provider_calls": 0, **result}


def reconcile_day(*, business_day: str, db_path: Path = DEFAULT_DB, at: str | None = None) -> dict[str, Any]:
    """D+1 work reconciliation; never substitute work count for video inventory."""
    stamp = planning.timestamp(at or now_utc())
    cutoff_day = datetime.fromisoformat(stamp.replace("Z", "+00:00")).astimezone(planning.BEIJING).date().isoformat()
    if business_day >= cutoff_day:
        raise ValueError("daily reconciliation requires a closed Beijing business day")
    scope_key = "capture-day:" + business_day
    with connect(db_path) as connection, transaction(connection):
        if connection.execute("PRAGMA user_version").fetchone()[0] != 20:
            return {"status": "skipped", "reason": "schema19_legacy"}
        rows = connection.execute("SELECT state,count(*) n FROM capture_work_items WHERE data_business_day=? GROUP BY state",
                                  (business_day,)).fetchall()
        expected = sum(int(row["n"]) for row in rows)
        terminal = next((int(row["n"]) for row in rows if row["state"] == "terminal"), 0)
        complete_scans = int(connection.execute(
            "SELECT count(*) FROM capture_watermarks m JOIN capture_work_items w ON w.id=m.work_id WHERE w.data_business_day=?",
            (business_day,),
        ).fetchone()[0])
        inventory = inventory_reconciliation(connection, business_day=business_day, at=stamp)
        result = {"contract_version": "capture-day-reconciliation-v1", "business_day": business_day,
                  "work_expected": expected, "work_terminal": terminal, "complete_scan_receipts": complete_scans,
                  "work_states": {str(row["state"]): int(row["n"]) for row in rows},
                  "expected_videos": None, "video_completeness": None,
                  "coverage_complete": False, "reason": "independent_video_denominator_required",
                  "full_day_acceptance_wait_required": False, "provider_inventory": inventory}
        identity = planning.digest({"scope": scope_key, "result": result})
        connection.execute(
            "INSERT OR IGNORE INTO data_quality_receipts(scope_key,cutoff_at,payload_json,recorded_at,receipt_sha256) VALUES (?,?,?,?,?)",
            (scope_key, stamp, planning.canonical(result), stamp, identity),
        )
    return result
