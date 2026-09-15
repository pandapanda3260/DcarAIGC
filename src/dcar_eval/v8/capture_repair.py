"""Bounded operator repair from the installed Writer's sealed bootstrap.

This entry owns the ordinary Writer lock while launchd's API is unloaded. A
plan selects existing work; it never grants the selected work paid eligibility.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import ContextVar, copy_context
import hashlib
import json
import os
from pathlib import Path
import signal
import stat
import sys
import time
from threading import Event
from typing import Any, Mapping

# ``python -m`` runs this file as __main__. Claim consumers import the package
# name, so both must share the same ContextVars and cancellation event.
if __name__ == "__main__":
    sys.modules["v8.capture_repair"] = sys.modules[__name__]

CONTRACT = "bounded-capture-repair-v1"
_ACTIVE_PLAN: ContextVar[Mapping[str, Any] | None] = ContextVar("capture_repair_plan", default=None)
_CANCELLED = Event()


class RepairRejected(RuntimeError):
    pass


def _require(value: Any, message: str) -> None:
    if not value:
        raise RepairRejected(message)


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
        separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _private_json(path: Path, expected: str) -> dict[str, Any]:
    _require(path.is_absolute() and path.resolve() == path and path.is_file()
        and not path.is_symlink(), "repair plan must be a canonical regular file")
    metadata = path.stat()
    _require(metadata.st_uid == os.geteuid() and metadata.st_nlink == 1
        and stat.S_IMODE(metadata.st_mode) in {0o400, 0o600}, "repair plan ownership or mode differs")
    body = path.read_bytes()
    _require(len(body) <= 1024 * 1024 and hashlib.sha256(body).hexdigest() == expected,
        "repair plan digest differs")
    value = json.loads(body)
    _require(isinstance(value, dict), "repair plan must be an object")
    return value


def validate_plan_document(plan: Mapping[str, Any], *, at: str) -> None:
    from .source_routing import parse_time
    _require(plan.get("contract_version") == CONTRACT and plan.get("mode") in {"publish_probe", "fixed_repair"},
        "unsupported repair plan")
    _require(isinstance(plan.get("repair_run_id"), str) and bool(plan["repair_run_id"])
        and isinstance(plan.get("authorization"), str) and bool(plan["authorization"].strip()),
        "repair authorization and identity are required")
    _require(parse_time(plan["issued_at"]) <= parse_time(at) < parse_time(plan["expires_at"]),
        "repair plan is not current")
    works = plan.get("works")
    _require(isinstance(works, list) and len(works) <= 4, "repair probe permits at most four existing works")
    ids = [item.get("id") for item in works if isinstance(item, dict)]
    _require(len(ids) == len(works) and all(type(item) is int and item > 0 for item in ids)
        and len(set(ids)) == len(ids), "invalid or duplicate repair work IDs")
    for item in works:
        _require(isinstance(item.get("work_identity"), str) and len(item["work_identity"]) == 64
            and isinstance(item.get("envelope_sha256"), str) and len(item["envelope_sha256"]) == 64
            and isinstance(item.get("operation"), str) and bool(item["operation"]),
            "repair target has no frozen identity")
    operations = plan.get("publish_operations")
    _require(isinstance(operations, list) and len(operations) <= 24
        and all(isinstance(op, str) and op for op in operations)
        and len(set(operations)) == len(operations), "invalid gate publication scope")
    _require(type(plan.get("window_seconds")) is int and 0 < plan["window_seconds"] <= 900,
        "repair maintenance window exceeds fifteen minutes")
    if plan["mode"] == "fixed_repair":
        from .capture_repair_fixed import STAGES
        _require(not works and isinstance(plan.get("fixed_plan"), dict), "fixed repair cannot claim ordinary work")
        selected = plan.get("fixed_stage_keys")
        _require(isinstance(selected, list) and len(set(selected)) == len(selected)
            and all(key in STAGES for key in selected), "fixed stage selection differs")
        retries = plan.get("fixed_retry_stage_keys", [])
        _require(retries in ([], ["ks_metrics"]) and not set(retries).intersection(selected),
            "only the one verified unbilled KS retry is allowed")
        recovery = plan.get("local_recovery")
        _require(recovery is None or (isinstance(recovery, dict)
            and recovery.get("work_id") == 12450 and recovery.get("fetch_attempt_id") == 142631
            and recovery.get("media_status") in {"available", "media_source_unverified", "media_source_refresh_required"}),
            "local entity recovery target differs")


@contextmanager
def execution_context(plan: Mapping[str, Any]):
    # JSON copy prevents the caller changing an identity after freezing it.
    token = _ACTIVE_PLAN.set(json.loads(json.dumps(plan)))
    try:
        yield
    finally:
        _ACTIVE_PLAN.reset(token)


def assert_exact_work(connection, work: Mapping[str, Any], *, at: str) -> None:
    """Validate selection in the actual claim transaction before any mutation."""
    from . import capture_planning as planning
    from .runtime_database import require_current_process_writer_lock
    plan = _ACTIVE_PLAN.get()
    _require(plan is not None, "exact repair work requires its sealed execution context")
    _require(not _CANCELLED.is_set(), "repair was cancelled before claim")
    validate_plan_document(plan, at=at)
    require_current_process_writer_lock(connection)
    target = next((item for item in plan["works"] if item["id"] == work["id"]), None)
    _require(target is not None, "work is outside the repair plan")
    envelope = json.loads(work["envelope_json"])
    _require(target.get("work_identity") == work["work_identity"]
        and target.get("operation") == work["operation"]
        and target.get("envelope_sha256") == _digest(envelope), "repair work changed after freezing")
    _require(work["state"] == "runnable" and work["due_at"] <= planning.timestamp(at),
        "repair work is no longer runnable and due")
    _require(envelope.get("stage") in {"detail", "metrics", "account_metrics", "profile_prepare"}
        and envelope.get("compensation") is None and envelope.get("manual_command_run_id") is None
        and work["operation"] != "douyin_video_statistics", "repair probe requires an ordinary single-request work")
    active = connection.execute("SELECT activation_sha256 FROM acquisition_profile_activations WHERE id=?",
        (plan["activation"]["activation_id"],)).fetchone()
    _require(active is not None and active[0] == plan["activation"]["activation_sha256"]
        and envelope.get("activation_id") == plan["activation"]["activation_id"], "repair activation differs")


def _write_receipt(path: Path, value: Mapping[str, Any]) -> None:
    body = json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False).encode() + b"\n"
    temporary = path.with_name(path.name + ".tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def run(plan: Mapping[str, Any], *, receipt_path: Path) -> dict[str, Any]:
    from .runtime_database import (DatabaseAccessMode, acquire_writer_lock,
        load_installed_writer_contract, require_current_process_writer_lock,
        resolve_installed_database_access)
    from .runtime_evidence_context import inheritance_boundary, prepare_inheritance
    from .storage import connect, now_utc, transaction, transaction_metrics_context
    from . import capture_release, capture_runtime
    from .profile_activations import activation_at

    validate_plan_document(plan, at=now_utc())
    _require(os.environ.get("DCAR_WRITER_ENTRY") == "repair", "repair must enter through sealed bootstrap")
    installed = load_installed_writer_contract(required=True)
    _require(installed is not None, "installed Writer is missing")
    db = installed.database
    _require(os.environ.get("DCAR_LOADED_BUILD_ID") == "sha256:" + plan["build_sha256"]
        and os.environ.get("DCAR_WRITER_SOURCE_ROOT") == plan["source_root"]
        and str(Path(__file__).resolve().parents[3]) == plan["source_root"], "repair loaded source or build differs")
    _require(str(db) == plan["database"]["path"] and db.stat().st_dev == plan["database"]["device"]
        and db.stat().st_ino == plan["database"]["inode"], "repair database identity differs")
    _require(receipt_path.is_absolute() and receipt_path.resolve() == receipt_path
        and receipt_path.parent == Path(os.environ["DCAR_WRITER_REPAIR_PLAN"]).parent,
        "repair receipt must be beside its private plan")
    access = resolve_installed_database_access(DatabaseAccessMode.WRITER, database=db,
        project_root=installed.project_root, installed=installed)
    started = time.monotonic()
    result: dict[str, Any] = {"contract_version": CONTRACT, "repair_run_id": plan["repair_run_id"],
        "status": "running", "build_sha256": plan["build_sha256"], "source_root": plan["source_root"],
        "started_at": now_utc(), "pid": os.getpid(), "gate_results": [], "work_results": []}
    with acquire_writer_lock(access), execution_context(plan):
        with connect(db) as connection:
            result["writer_identity"] = require_current_process_writer_lock(connection)
            active = activation_at(connection, now_utc())
            _require(active is not None and all(active.get(key) == value for key, value in plan["activation"].items()),
                "repair active generation changed")
            if plan["mode"] == "fixed_repair":
                from .capture_repair_fixed import validate_plan
                validate_plan(connection, plan["fixed_plan"], at=now_utc())
            before_sends = connection.execute("SELECT count(*) FROM paid_provider_dispatch_events WHERE event_type='send_marked'").fetchone()[0]
        # Publication grants only the existing operator scope, never HTTP calls.
        # Its complete inheritance is prepared once outside all write locks.
        with prepare_inheritance(db):
            for operation in plan["publish_operations"]:
                _require(not _CANCELLED.is_set(), "repair cancelled before gate publication")
                _require(time.monotonic() - started < plan["window_seconds"], "repair window exhausted during gate publication")
                with transaction_metrics_context(job_id="repair_gate_publish", operation=operation), \
                        connect(db) as connection, transaction(connection), inheritance_boundary(connection):
                    latest = connection.execute("SELECT state FROM capture_paid_send_gate_events WHERE provider='tikhub' AND operation=? ORDER BY id DESC LIMIT 1", (operation,)).fetchone()
                    item = capture_release.publish_operation_gate(connection, operation=operation, at=now_utc()) if latest is not None and latest[0] == "open" else {"status": "preserved_not_open", "provider_calls": 0}
                    _require(item.get("provider_calls") == 0, "gate publication unexpectedly sent a provider request")
                result["gate_results"].append({"operation": operation, "result": item})
                _write_receipt(receipt_path, result)
                print(json.dumps({"published_operation": operation, "provider_calls": 0}), flush=True)
        with connect(db) as connection:
            _require(connection.execute("SELECT count(*) FROM paid_provider_dispatch_events WHERE event_type='send_marked'").fetchone()[0] == before_sends,
                "gate publication changed the sending ledger")
        if plan["works"] and not _CANCELLED.is_set() and time.monotonic() - started + 180 < plan["window_seconds"]:
            def execute(target):
                try:
                    return capture_runtime._run_single(db, now_utc(), repair_work_id=target["id"])
                except Exception as error:
                    return {"work_id": target["id"], "status": "failed", "error_type": type(error).__name__,
                        "error": str(error)[:300]}
            with ThreadPoolExecutor(max_workers=len(plan["works"]), thread_name_prefix="capture-repair") as executor:
                futures = [executor.submit(copy_context().run, execute, item) for item in plan["works"]]
                result["work_results"] = [future.result() for future in futures]
        elif plan["works"]:
            result["work_results"] = [{"work_id": item["id"], "status": "window_deferred", "provider_calls": 0} for item in plan["works"]]
        if plan["mode"] == "fixed_repair":
            from . import capture_repair_fixed as fixed
            recovery = plan.get("local_recovery")
            if recovery is not None and not _CANCELLED.is_set():
                from .capture_transport_recovery import recover_content_entity
                try:
                    result["local_recovery"] = recover_content_entity(db_path=db, **recovery)
                except Exception as error:
                    result["local_recovery"] = {"status": "failed", "error_type": type(error).__name__,
                        "error": str(error)[:300], "provider_calls": 0}
                _write_receipt(receipt_path, result)
            selected = plan["fixed_stage_keys"]
            result["fixed_results"] = {}
            def stage(key):
                if _CANCELLED.is_set() or time.monotonic() - started + 180 >= plan["window_seconds"]:
                    return {"status": "window_deferred", "provider_calls": 0}
                try:
                    return fixed.run_stage(plan["fixed_plan"], key, db_path=db, cancelled=_CANCELLED)
                except Exception as error:
                    return {"status": "failed", "error_type": type(error).__name__, "error": str(error)[:300]}
            if "xhs_detail" in selected:
                result["fixed_results"]["xhs_detail"] = stage("xhs_detail")
                _write_receipt(receipt_path, result)
            metrics = [key for key in fixed.FIRST_METRICS if key in selected]
            if metrics:
                with ThreadPoolExecutor(max_workers=len(metrics), thread_name_prefix="fixed-metrics") as executor:
                    futures = {key: executor.submit(copy_context().run, stage, key) for key in metrics}
                    for key, future in futures.items():
                        result["fixed_results"][key] = future.result()
                _write_receipt(receipt_path, result)
            if "dy_statistics" in selected:
                result["fixed_results"]["dy_statistics"] = stage("dy_statistics")
                _write_receipt(receipt_path, result)
            if plan.get("fixed_retry_stage_keys"):
                result["retry_results"] = {}
                for key in plan["fixed_retry_stage_keys"]:
                    if _CANCELLED.is_set() or time.monotonic() - started + 180 >= plan["window_seconds"]:
                        item = {"status": "window_deferred", "provider_calls": 0}
                    else:
                        try:
                            item = fixed.run_unbilled_retry(plan["fixed_plan"], key,
                                db_path=db, cancelled=_CANCELLED)
                        except Exception as error:
                            item = {"status": "failed", "error_type": type(error).__name__,
                                "error": str(error)[:300]}
                    result["retry_results"][key] = item
                    _write_receipt(receipt_path, result)
        with connect(db) as connection:
            result["new_send_markers"] = connection.execute("SELECT count(*) FROM paid_provider_dispatch_events WHERE event_type='send_marked'").fetchone()[0] - before_sends
        result.update(status="completed", finished_at=now_utc(), elapsed_seconds=round(time.monotonic() - started, 3))
    result["writer_lease_released"] = True
    _write_receipt(receipt_path, result)
    return result


def main() -> int:
    os.umask(0o077)
    # Do not unwind an in-flight HTTP/SQLite scope on SIGINT/SIGTERM. Finish
    # its ledger, reject further claims, then leave the Writer lease normally.
    signal.signal(signal.SIGINT, lambda *_: _CANCELLED.set())
    signal.signal(signal.SIGTERM, lambda *_: _CANCELLED.set())
    path = Path(os.environ.get("DCAR_WRITER_REPAIR_PLAN", ""))
    try:
        plan = _private_json(path, os.environ.get("DCAR_WRITER_REPAIR_PLAN_SHA256", ""))
        result = run(plan, receipt_path=path.with_suffix(".result.json"))
    except Exception as error:
        print(json.dumps({"status": "failed", "error_type": type(error).__name__, "error": str(error)[:500]}), flush=True)
        return 1
    print(json.dumps({key: result[key] for key in ("status", "repair_run_id", "new_send_markers", "writer_lease_released")}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
