#!/usr/bin/env python3
"""Exercise real HTTP, duplicate work and capture claim boundaries on a disposable copy.

This mutates --db-copy, never copies or opens the installed database. The source
must already be schema24 with a ready generation and passing calibration. It
cannot prove installed paid authority: the formal inode and sealed inheritance
are replaced by a labelled offline preparation containing actual source hashes,
file generations and a closed database read set. Live paid/slot guards, real
SQLite transactions, process Writer lease, and entry/exit fences are unchanged.
No dispatcher is called; a socket audit denies every non-loopback connection.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import hashlib
import ipaddress
import json
import math
import os
from pathlib import Path
import socket
import sqlite3
import stat
import sys
import threading
import time
from types import MappingProxyType
from urllib.error import HTTPError
from urllib.parse import unquote, urlsplit
from urllib.request import Request, build_opener, ProxyHandler
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT / "src" / "dcar_eval"))


def distribution(values):
    values = sorted(values)
    return ({"count": len(values), "mean": sum(values) / len(values),
             "p50": values[math.ceil(len(values) * .50) - 1],
             "p95": values[math.ceil(len(values) * .95) - 1], "max": values[-1]}
            if values else {"count": 0})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-copy", required=True, type=Path,
                        help="Existing disposable schema24 database; this file WILL be mutated")
    parser.add_argument("--output", required=True, type=Path, help="New private output directory")
    parser.add_argument("--rounds", type=int, default=20)
    parser.add_argument("--http-rounds", type=int, default=20)
    parser.add_argument("--seed-limit", type=int, default=20)
    args = parser.parse_args()
    if not 1 <= args.rounds <= 1000 or not 1 <= args.http_rounds <= 1000 or not 1 <= args.seed_limit <= 20:
        parser.error("rounds must be 1..1000 and seed-limit 1..20")
    database = args.db_copy.expanduser().absolute()
    if database.is_symlink() or database.resolve(strict=True) != database:
        parser.error("db-copy must be a canonical absolute path without symlink parents")
    state = database.stat()
    if not stat.S_ISREG(state.st_mode) or state.st_nlink != 1 or state.st_uid != os.geteuid():
        parser.error("db-copy must be a current-user, single-link regular file")
    os.environ["DCAR_TEST_DENY_FORMAL_DB"] = "1"
    os.environ["DCAR_READ_ONLY"] = "0"
    from v8 import storage
    if storage.is_formal_database_path(database):
        parser.error("the installed/formal database is forbidden; supply a disposable copy")
    output = args.output.expanduser().absolute()
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    if output.resolve() != output:
        parser.error("output must not contain symlink parents")
    os.environ["DCAR_SQLITE_METRICS_FILE"] = str(output / "transactions.jsonl")
    denied = []
    dispatch_attempts = []

    def audit(event, values):
        if event == "sqlite3.connect":
            spelling = os.fsdecode(values[0])
            if spelling == ":memory:":
                # Schema24 normalizes declared DDL in a private in-memory DB.
                return
            target = Path(unquote(urlsplit(spelling).path) if spelling.startswith("file:") else spelling).resolve()
            if target != database or target.stat().st_ino != state.st_ino or target.stat().st_dev != state.st_dev:
                denied.append({"kind": "database", "target": str(target)})
                raise RuntimeError("offline integration forbids every other database")
        elif event == "socket.connect":
            address = values[1]
            try:
                allowed = isinstance(address, tuple) and ipaddress.ip_address(address[0]).is_loopback
            except ValueError:
                allowed = False
            if not allowed:
                denied.append({"kind": "network", "target": str(address)})
                raise RuntimeError("offline integration forbids provider/external network")

    sys.addaudithook(audit)
    import uvicorn
    from v8 import api, capture, duplicate_index as index, duplicate_runtime as runtime
    from v8 import runtime_database as database_runtime, runtime_evidence_context as evidence

    with storage.connect(database) as connection:
        storage.require_schema_compatibility(connection, supported_versions=frozenset({24}))
        generation = connection.execute("SELECT generation_id FROM duplicate_index_generations WHERE state='ready'").fetchone()
        if generation is None:
            raise RuntimeError("db-copy has no ready duplicate generation")
        seeds = [row[0] for row in connection.execute("SELECT content_id FROM duplicate_current_fingerprints "
            "WHERE generation_id=? AND input_status='available' ORDER BY content_id LIMIT ?", (generation[0], args.seed_limit))]
        if not seeds:
            raise RuntimeError("db-copy has no current fingerprints")
        # Replaying an already completed slot guarantees the claim cannot reserve
        # new paid work even when the live dispatch gate is open on this copy.
        slot = connection.execute("SELECT * FROM fetch_slots WHERE content_id IS NOT NULL "
            "AND provider='TikHub' COLLATE NOCASE AND stage IN ('detail','metrics','comments') "
            "AND status='succeeded' ORDER BY id DESC LIMIT 1").fetchone()
        slot = dict(slot) if slot is not None else None

    preparation_samples, capture_samples, boundary_samples, duplicate_samples = [], [], [], []
    inventories = {}

    @contextmanager
    def offline_preparation(path, *, enabled=True):
        """The sole authority substitution; never changes a live guard result."""
        if not enabled:
            raise RuntimeError("diagnostic bypass is not part of this offline benchmark")
        began = time.perf_counter()
        critical, files = {}, {}
        for name in sorted((ROOT / "src" / "dcar_eval" / "v8").glob("*.py")):
            files[name] = evidence._generation(name)
            critical[name.relative_to(ROOT).as_posix()] = hashlib.sha256(name.read_bytes()).hexdigest()
        frozen = evidence._freeze_current_files(files, critical, ROOT)
        with sqlite3.connect(database.as_uri() + "?mode=ro", uri=True) as reader:
            reader.execute("PRAGMA foreign_keys=ON")
            reader.execute("PRAGMA recursive_triggers=ON")
            reader.execute("PRAGMA query_only=ON")
            reader.execute("BEGIN")
            read_set = evidence._ReadSet(reader)
            for sql in ("PRAGMA user_version", "PRAGMA foreign_keys", "PRAGMA recursive_triggers",
                        "SELECT version,name FROM schema_migrations ORDER BY version"):
                read_set.execute(sql).fetchall()
            queries = tuple((key, (tuple(tuple(row) for row in rows), description))
                            for key, (rows, description) in read_set.queries.items())
            reader.rollback()
        inventory_digest = hashlib.sha256(json.dumps(critical, sort_keys=True).encode()).hexdigest()
        inventories[inventory_digest] = {"files": len(files), "bytes": sum(len(body) for body in frozen.values()),
                                       "critical_files": critical}
        prepared = evidence.PreparedInheritance(database=database, inode=(state.st_dev, state.st_ino),
            source=ROOT, build="{}", build_ref="{}", install_path=output / "offline-no-install-authority.json",
            at=evidence._now(), environment=MappingProxyType(dict(os.environ)), files=MappingProxyType(files),
            queries=queries, proof='{"contract":"offline-performance-substitution-v1","not_authority":true}',
            owner_thread=threading.get_ident(), schema_version=24, frozen_files=frozen,
            critical_digests=MappingProxyType(critical))
        preparation_samples.append({"seconds": time.perf_counter() - began, "inventory_sha256": inventory_digest,
                                    "read_queries": len(queries)})
        token = evidence._PREPARED.set(prepared)
        try:
            yield prepared
        finally:
            evidence._PREPARED.reset(token)

    writer_lock = output / "writer.lock"
    writer_lock.touch(mode=0o600)
    contract = database_runtime.InstalledWriterContract(home=output, plist_path=output / "offline.plist",
        project_root=ROOT, program=Path(__file__), database=database, writer_lock=writer_lock,
        payload={"offline_benchmark": True, "not_installed_authority": True})
    access = database_runtime.ResolvedDatabaseAccess(access_mode=database_runtime.DatabaseAccessMode.WRITER,
        database=database, database_identity=database_runtime.FileIdentity.from_stat(state),
        project_root=ROOT, writer_lock=writer_lock, installed=contract)
    config = api.ApiConfig(db_path=database, reports_root=output / "reports", legacy_db_path=output / "forbidden-legacy.sqlite3",
        operator_freeze_lock=output / "freeze", writer_lock=writer_lock, scheduler_enabled=False,
        startup_catchup_enabled=False, read_only=False, project_root=ROOT)
    bound = socket.socket()
    bound.bind(("127.0.0.1", 0))
    port = bound.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(api.create_app(config), log_level="error", access_log=False))
    server_thread = threading.Thread(target=lambda: server.run(sockets=[bound]), daemon=True)
    start_gate = threading.Barrier(6)

    def duplicate_worker():
        start_gate.wait(timeout=30)
        for round_id in range(args.rounds):
            began = time.perf_counter()
            try:
                with storage.transaction_metrics_context(job_id="offline_duplicate_inputs"), storage.connect(database) as c, storage.transaction(c):
                    for content_id in seeds:
                        index.mark_content_dirty(c, content_id, reason="canonical_changed")
                answer = runtime.drain_duplicate_work(db_path=database, limit=20, time_budget_seconds=5,
                                                     scope_content_ids=seeds)
                duplicate_samples.append({"round": round_id, "seconds": time.perf_counter() - began,
                    "relation_status": answer["relation_status"], "processed": answer["processed"],
                    "failed": answer["failed"], "pending": answer["pending"]})
            except Exception as error:
                duplicate_samples.append({"round": round_id, "seconds": time.perf_counter() - began,
                                          "error_type": type(error).__name__, "error": str(error)[:500]})

    def capture_worker():
        start_gate.wait(timeout=30)
        for round_id in range(args.rounds):
            began = time.perf_counter()
            try:
                # A complete no-dispatch boundary exercises entry AND exit; a
                # failed real claim below correctly rolls back before its exit.
                with offline_preparation(database), storage.transaction_metrics_context(job_id="offline_capture_boundary"), \
                        storage.connect(database) as c, storage.transaction(c), evidence.inheritance_boundary(c):
                    c.execute("SELECT id FROM content_items WHERE id=?", (seeds[0],)).fetchone()
                boundary_samples.append({"round": round_id, "seconds": time.perf_counter() - began, "status": "committed"})
            except Exception as error:
                boundary_samples.append({"round": round_id, "seconds": time.perf_counter() - began,
                                         "error_type": type(error).__name__, "error": str(error)[:500]})
            if slot is None:
                continue
            began = time.perf_counter()
            try:
                operation = {"detail": "douyin_video_detail", "metrics": "douyin_video_statistics",
                             "comments": "douyin_video_comments"}[slot["stage"]]
                claim = capture._claim_paid_tikhub(content_id=slot["content_id"], account_id=None, stage=slot["stage"],
                    window_key=slot["window_key"], provider="TikHub", adapter_version=slot["adapter_version"], operation=operation,
                    db_path=database, budget_id="offline-never-reserve", task_id=None, task_max_amount=None,
                    allow_terminal_retry=False)
                # A succeeded source slot must make this unreachable.
                capture_samples.append({"round": round_id, "seconds": time.perf_counter() - began,
                                        "unexpected_claim": str(claim)[:300]})
            except Exception as error:
                capture_samples.append({"round": round_id, "seconds": time.perf_counter() - began,
                    "first_unmet_gate": getattr(error, "error_code", type(error).__name__),
                    "error_type": type(error).__name__, "error": str(error)[:500],
                    "slot_guard_reached": isinstance(error, capture.SlotUnavailable)})

    routes = [("POST", "/api/v8/accounts/search", {"page": 1, "page_size": 20}),
              ("POST", "/api/v8/contents/search", {"page": 1, "page_size": 20}),
              ("GET", "/api/v8/overview", None), ("GET", "/api/v8/selling-points", None)]

    def http_worker(route):
        method, path, payload = route
        opener, samples = build_opener(ProxyHandler({})), []
        start_gate.wait(timeout=30)
        for round_id in range(args.http_rounds):
            began = time.perf_counter()
            request = Request(f"http://127.0.0.1:{port}{path}", method=method,
                data=json.dumps(payload).encode() if payload is not None else None,
                headers={"Content-Type": "application/json"})
            try:
                with opener.open(request, timeout=60) as response:
                    body, status = response.read(), response.status
                document = json.loads(body)
                samples.append({"round": round_id, "seconds": time.perf_counter() - began,
                                "http_status": status, "bytes": len(body),
                                "relation_states": dict(Counter(item.get("relation_status", "absent")
                                    for item in document.get("items", []) if isinstance(item, dict)))})
            except HTTPError as error:
                samples.append({"round": round_id, "seconds": time.perf_counter() - began,
                                "http_status": error.code, "error": error.read().decode()[:500]})
            except Exception as error:
                samples.append({"round": round_id, "seconds": time.perf_counter() - began,
                                "error_type": type(error).__name__, "error": str(error)[:500]})
        return path, samples

    def forbid_dispatch(*args, **kwargs):
        dispatch_attempts.append("forbidden")
        raise RuntimeError("offline integration never permits the paid dispatcher")

    with database_runtime.acquire_writer_lock(access), patch.object(capture, "prepare_inheritance", offline_preparation), \
            patch.object(capture, "_execute_claimed_fetch", forbid_dispatch), patch.object(capture, "_mark_paid_sent", forbid_dispatch):
        server_thread.start()
        deadline = time.monotonic() + 30
        while not server.started and server_thread.is_alive() and time.monotonic() < deadline:
            time.sleep(.025)
        if not server.started:
            server.should_exit = True
            raise RuntimeError("offline HTTP Writer API did not start; inspect startup error")
        try:
            with ThreadPoolExecutor(max_workers=6) as pool:
                duplicate_future = pool.submit(duplicate_worker)
                capture_future = pool.submit(capture_worker)
                http_futures = [pool.submit(http_worker, route) for route in routes]
                http_samples = dict(future.result() for future in http_futures)
                duplicate_future.result()
                capture_future.result()
        finally:
            server.should_exit = True
            server_thread.join(timeout=30)
            bound.close()

    metrics = [json.loads(line) for line in (output / "transactions.jsonl").read_text().splitlines()]
    grouped = defaultdict(list)
    for row in metrics:
        if row.get("phase") == "finish":
            grouped[row.get("job_id", "other")].append(row)
    report = {"contract": "offline-duplicate-http-capture-integration-v1", "database_copy": str(database),
        "database_identity": {"device": state.st_dev, "inode": state.st_ino}, "source": str(ROOT),
        "source_inventories": inventories, "schema_version": 24, "seed_content_ids": seeds,
        "http_transport": "real TCP loopback / uvicorn / actual Writer API handlers; scheduler disabled",
        "substitutions": ["Installed Writer contract replaced by copy-specific contract; real OS/process lease retained",
            "Paid inheritance authority replaced by labelled offline source hashes and closed schema read-set preparation",
            "Historical sealed Git, receipts and installed database inode inheritance are not verified by this benchmark",
            "Capture replays one succeeded content slot; live paid-drain and slot guards are unchanged; dispatcher never called",
            "Separate capture boundary commits a read-only transaction to exercise both real entry and exit fences",
            "Duplicate input writes are canonical_changed invalidations of existing fingerprints, not media extraction",
            "API startup recovery runs on the disposable copy; no scheduler, catch-up or paid execution is enabled",
            "Private in-memory SQLite is allowed for schema DDL normalization; every other disk database is denied"],
        "capture_scope": "guarded claim only; not full paid A/B acceptance", "capture_slot_present": slot is not None,
        "capture_first_unmet_gates": dict(Counter(row.get("first_unmet_gate", "unexpected_claim") for row in capture_samples)),
        "capture": {"seconds": distribution([row["seconds"] for row in capture_samples]), "samples": capture_samples},
        "prepared_boundary": {"seconds": distribution([row["seconds"] for row in boundary_samples]), "samples": boundary_samples},
        "preparation": {"seconds": distribution([row["seconds"] for row in preparation_samples]), "samples": preparation_samples},
        "duplicate": {"seconds": distribution([row["seconds"] for row in duplicate_samples]), "samples": duplicate_samples},
        "http": {route: {"seconds": distribution([row["seconds"] for row in samples]),
                          "statuses": dict(Counter(row.get("http_status", "error") for row in samples)), "samples": samples}
                 for route, samples in http_samples.items()},
        "transactions": {job: {"count": len(rows), "queue_wait_ms": distribution([row["queue_wait_ms"] for row in rows]),
                              "hold_ms": distribution([row["hold_ms"] for row in rows]),
                              "lock_hold_ms": distribution([row["lock_hold_ms"] for row in rows if row.get("lock_hold_ms") is not None])}
                         for job, rows in grouped.items()},
        "denied_external_access": denied, "provider_dispatch_calls": len(dispatch_attempts),
        "full_paid_acceptance": False}
    report["functional_checks_passed"] = (not denied and not dispatch_attempts and all(row.get("http_status") == 200 for samples in http_samples.values() for row in samples)
        and all(row.get("relation_status") == "ready" for row in duplicate_samples)
        and all(row.get("status") == "committed" for row in boundary_samples)
        and all("unexpected_claim" not in row for row in capture_samples))
    drain_times = report["duplicate"]["seconds"]
    transaction_gates = {}
    for job, values in report["transactions"].items():
        if job.startswith("duplicate_graph_") or job in {"tikhub_paid_claim", "offline_capture_boundary"}:
            timing = values["lock_hold_ms"]
            transaction_gates[job] = {"p95_ms": timing.get("p95"), "max_ms": timing.get("max"),
                                     "passed": timing.get("p95", float("inf")) <= 50 and timing.get("max", float("inf")) <= 250}
    report["performance_gates"] = {"relation_end_to_end": {"p95_seconds": drain_times.get("p95"),
        "max_seconds": drain_times.get("max"), "passed": drain_times.get("p95", float("inf")) <= 1
        and drain_times.get("max", float("inf")) <= 5}, "transaction_lock": transaction_gates,
        "notes": "1s/5s gates apply to relation work. HTTP latencies remain visible but are not that gate. "
                 "Fixture invalidation and API startup recovery are reported separately from runtime capture/graph transactions."}
    report["performance_checks_passed"] = (report["performance_gates"]["relation_end_to_end"]["passed"]
        and bool(transaction_gates) and all(value["passed"] for value in transaction_gates.values()))
    report["offline_checks_passed"] = report["functional_checks_passed"] and report["performance_checks_passed"]
    report["capture_claim_exercised"] = bool(capture_samples)
    (output / "integration.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"output": str(output / "integration.json"), "offline_checks_passed": report["offline_checks_passed"],
                      "capture_claim_exercised": report["capture_claim_exercised"],
                      "full_paid_acceptance": False}, ensure_ascii=False))
    return 0 if report["offline_checks_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
