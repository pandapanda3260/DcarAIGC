#!/usr/bin/env python3
"""Measure four concurrent relation requests against real capture claims on a copy.

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
from urllib.parse import unquote, urlsplit
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
    parser.add_argument("--capture-interval", type=float, default=.02)
    parser.add_argument("--seed-limit", type=int, default=20)
    args = parser.parse_args()
    if not 1 <= args.rounds <= 1000 or not 0 <= args.capture_interval <= 1 or not 1 <= args.seed_limit <= 20:
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
            if spelling == ":memory:":
                return  # SQLite canonicalizes the frozen schema DDL in memory.
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
    from v8 import capture, duplicate_index as index, duplicate_runtime as runtime
    from v8 import runtime_database as database_runtime, runtime_evidence_context as evidence

    with storage.connect(database) as connection:
        storage.require_schema_compatibility(connection, supported_versions=frozenset({24}))
        generation = connection.execute("SELECT generation_id FROM duplicate_index_generations WHERE state='ready'").fetchone()
        if generation is None:
            raise RuntimeError("db-copy has no ready duplicate generation")
        gid = generation[0]
        current = [dict(row) for row in connection.execute("""SELECT p.content_id,c.published_at,
            COALESCE(k.member_count,1) member_count,m.component_id
            FROM duplicate_current_fingerprints p JOIN content_items c ON c.id=p.content_id
            LEFT JOIN duplicate_component_members m ON m.generation_id=p.generation_id AND m.content_id=p.content_id
            LEFT JOIN duplicate_components k ON k.generation_id=m.generation_id AND k.component_id=m.component_id
            WHERE p.generation_id=? AND p.input_status='available' ORDER BY p.content_id""", (gid,))]
        if len(current) < 4 * args.seed_limit:
            raise RuntimeError("need four disjoint groups of current fingerprints")
        selected = set()
        groups = []
        largest = max(row["member_count"] for row in current)
        specifications = [("largest_component", lambda row: row["member_count"] == largest),
            ("component20", lambda row: row["member_count"] == 20),
            ("other_clustered", lambda row: row["member_count"] > 1),
            ("isolated", lambda row: row["member_count"] == 1)]
        for name, predicate in specifications:
            pool = [row for row in current if row["content_id"] not in selected and predicate(row)]
            pool += [row for row in current if row["content_id"] not in selected and not predicate(row)]
            rows = pool[:args.seed_limit]
            selected.update(row["content_id"] for row in rows)
            groups.append({"stratum": name, "content_ids": [row["content_id"] for row in rows],
                           "original_inputs": rows})
        seeds = sorted(selected)
        # Replaying an already completed slot guarantees the claim cannot reserve
        # new paid work even when the live dispatch gate is open on this copy.
        slot = connection.execute("SELECT * FROM fetch_slots WHERE content_id IS NOT NULL "
            "AND provider='TikHub' COLLATE NOCASE AND stage IN ('detail','metrics','comments') "
            "AND status='succeeded' ORDER BY id DESC LIMIT 1").fetchone()
        slot = dict(slot) if slot is not None else None

    preparation_samples, capture_samples, duplicate_samples, enqueue_samples = [], [], [], []
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

    if slot is None:
        raise RuntimeError("copy has no succeeded TikHub slot; real claim load is required")
    from v8 import schema_v24
    with storage.connect(database) as connection:
        schema_v24.validate_structure(connection)
    # A process starts once, then accepts concurrent requests. Report its initial
    # read-only startup independently instead of concealing it inside a warm-up.
    began = time.perf_counter()
    runtime.drain_duplicate_work(db_path=database, limit=0, scope_content_ids=[])
    cold_probe_ms = (time.perf_counter() - began) * 1000

    writer_lock = output / "writer.lock"
    writer_lock.touch(mode=0o600)
    contract = database_runtime.InstalledWriterContract(home=output, plist_path=output / "offline.plist",
        project_root=ROOT, program=Path(__file__), database=database, writer_lock=writer_lock,
        payload={"offline_benchmark": True, "not_installed_authority": True})
    access = database_runtime.ResolvedDatabaseAccess(access_mode=database_runtime.DatabaseAccessMode.WRITER,
        database=database, database_identity=database_runtime.FileIdentity.from_stat(state),
        project_root=ROOT, writer_lock=writer_lock, installed=contract)
    calls = []
    failures = []
    def claim_once(round_id, scenario):
        began = time.perf_counter()
        try:
            operation = {"detail": "douyin_video_detail", "metrics": "douyin_video_statistics",
                         "comments": "douyin_video_comments"}[slot["stage"]]
            claim = capture._claim_paid_tikhub(content_id=slot["content_id"], account_id=None,
                stage=slot["stage"], window_key=slot["window_key"], provider="TikHub",
                adapter_version=slot["adapter_version"], operation=operation, db_path=database,
                budget_id="offline-never-reserve", task_id=None, task_max_amount=None,
                allow_terminal_retry=False)
            sample = {"unexpected_claim": str(claim)[:300]}
        except Exception as error:
            sample = {"first_unmet_gate": getattr(error, "error_code", type(error).__name__),
                "error_type": type(error).__name__, "error": str(error)[:500],
                "slot_guard_reached": isinstance(error, capture.SlotUnavailable)}
        capture_samples.append({"scenario": scenario, "round": round_id, "seconds": time.perf_counter()-began, **sample})

    def request(group, round_id, gate, scenario):
        gate.wait(timeout=30)
        began = time.perf_counter()
        latest = {}
        count = 0
        while time.perf_counter()-began < 15:
            started = time.perf_counter()
            try:
                with storage.transaction_metrics_context(benchmark_scenario=scenario):
                    latest = runtime.drain_duplicate_work(db_path=database, limit=20,
                        time_budget_seconds=5, scope_content_ids=group["content_ids"])
            except Exception as error:
                latest = {"relation_status": "failed", "error": str(error)[:500],
                          "error_type": type(error).__name__}
            calls.append({"scenario": scenario, "round": round_id, "stratum": group["stratum"],
                "seconds": time.perf_counter()-started, "relation_status": latest["relation_status"]})
            count += 1
            if latest["relation_status"] in ("ready", "failed"):
                break
            time.sleep(.01)
        sample = {"scenario": scenario, "round": round_id, "stratum": group["stratum"], "seconds": time.perf_counter()-began,
            "calls": count, "relation_status": latest.get("relation_status"),
            "processed": latest.get("processed"), "pending": latest.get("pending"),
            "failed": latest.get("failed"), "content_ids": group["content_ids"]}
        duplicate_samples.append(sample)
        if latest.get("relation_status") != "ready":
            failures.append(latest)

    def capture_worker(round_id, gate, finished, scenario):
        gate.wait(timeout=30)
        while True:
            with storage.transaction_metrics_context(benchmark_scenario=scenario):
                claim_once(round_id, scenario)
            if finished.wait(args.capture_interval):
                break

    def forbid_dispatch(*args, **kwargs):
        dispatch_attempts.append("forbidden")
        raise RuntimeError("offline integration never permits the paid dispatcher")

    scenarios = {
        "four_single": [{**group, "content_ids": group["content_ids"][:1]} for group in groups],
        "batch20": [groups[0]],
        "four_batch20_extra_stress": groups,
    }
    with database_runtime.acquire_writer_lock(access), patch.object(capture, "prepare_inheritance", offline_preparation), \
            patch.object(capture, "_execute_claimed_fetch", forbid_dispatch), patch.object(capture, "_mark_paid_sent", forbid_dispatch):
        for scenario, active_groups in scenarios.items():
            active_seeds = sorted({cid for group in active_groups for cid in group["content_ids"]})
            for round_id in range(args.rounds):
                began = time.perf_counter()
                with storage.transaction_metrics_context(job_id="offline_duplicate_inputs", benchmark_scenario=scenario), storage.connect(database) as c, storage.transaction(c):
                    for content_id in active_seeds:
                        # Every selected date actually changes, including when a
                        # later scenario reuses one of the earlier single seeds.
                        c.execute("""UPDATE content_items SET published_at=CASE
                            WHEN published_at='2000-01-01T00:00:00+08:00' THEN '2099-01-01T00:00:00+08:00'
                            ELSE '2000-01-01T00:00:00+08:00' END WHERE id=?""", (content_id,))
                        index.mark_content_dirty(c, content_id, reason="canonical_changed")
                enqueue_samples.append({"scenario": scenario, "round": round_id,
                    "seconds": time.perf_counter()-began, "changed_inputs": len(active_seeds)})
                gate, finished = threading.Barrier(len(active_groups)+1), threading.Event()
                with ThreadPoolExecutor(max_workers=len(active_groups)+1) as pool:
                    capture_future = pool.submit(capture_worker, round_id, gate, finished, scenario)
                    try:
                        futures = [pool.submit(request, group, round_id, gate, scenario) for group in active_groups]
                        for future in futures:
                            future.result()
                    finally:
                        finished.set()
                        capture_future.result()

    with storage.connect(database) as connection:
        statuses = index.ready_status(connection, seeds)
        stale_endpoints = int(connection.execute("""SELECT COUNT(*) FROM duplicate_match_edges e
            JOIN duplicate_current_fingerprints l ON l.generation_id=e.generation_id AND l.content_id=e.left_content_id
            JOIN duplicate_current_fingerprints r ON r.generation_id=e.generation_id AND r.content_id=e.right_content_id
            WHERE e.generation_id=? AND (e.left_input_revision!=l.input_revision OR e.right_input_revision!=r.input_revision
                OR e.left_fingerprint_id!=l.fingerprint_id OR e.right_fingerprint_id!=r.fingerprint_id)""", (gid,)).fetchone()[0])
        postconditions = {"selected_all_ready": all(v["relation_status"] == "ready" for v in statuses.values()),
                          "stale_edge_endpoints": stale_endpoints}
    metrics = [json.loads(line) for line in (output / "transactions.jsonl").read_text().splitlines()]
    grouped = defaultdict(list)
    for row in metrics:
        if row.get("phase") == "finish":
            grouped[row.get("job_id", "other")].append(row)
    graph_finishes = [row for row in metrics if row.get("phase") == "finish"
                      and str(row.get("job_id", "")).startswith("duplicate_graph")]
    scenario_reports = {}
    for name, active_groups in scenarios.items():
        samples = [row for row in duplicate_samples if row["scenario"] == name]
        latency = distribution([row["seconds"]*1000 for row in samples])
        finishes = [row for row in graph_finishes if row.get("benchmark_scenario") == name]
        locks = distribution([row["lock_hold_ms"] for row in finishes if row.get("lock_hold_ms") is not None])
        latency_passed = (latency["p95"] <= 1000 and latency["max"] <= 5000 if name == "four_single"
                          else latency["max"] <= 5000 if name == "batch20" else None)
        scenario_reports[name] = {"concurrent_callers": len(active_groups),
            "items_per_request": len(active_groups[0]["content_ids"]), "requests": len(samples),
            "request_to_ready_ms": latency, "duplicate_lock_ms": locks,
            "latency_gate": "P95<=1000ms,max<=5000ms" if name == "four_single" else
                            "every20-itembatch<=5000ms" if name == "batch20" else "extra stress; no additional latency gate",
            "latency_passed": latency_passed,
            "duplicate_lock_passed": locks.get("count",0)>0 and locks["p95"]<=50 and locks["max"]<=250}
    holds = distribution([row["lock_hold_ms"] for row in graph_finishes if row.get("lock_hold_ms") is not None])
    report = {"contract": "offline-four-relation-real-capture-v2", "database_copy": str(database),
        "database_identity": {"device": state.st_dev, "inode": state.st_ino}, "source": str(ROOT),
        "source_inventories": inventories, "schema_version": 24, "generation_id": gid,
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), "postconditions": postconditions,
        "groups": groups, "rounds_per_scenario": args.rounds, "scenarios": scenario_reports,
        "process_start_read_only_probe_ms": cold_probe_ms,
        "substitutions": ["Installed Writer contract replaced by copy-specific contract; real OS/process lease retained",
            "Paid inheritance authority replaced by labelled offline source hashes and closed schema read-set preparation",
            "Historical sealed Git, receipts and installed database inode inheritance are not verified by this benchmark",
            "Capture replays one succeeded slot with real paid-drain and slot guards; dispatcher never called",
            "Succeeded-slot rejection rolls back before the successful-claim exit boundary; committed entry/exit has separate integration evidence",
            "Only the chosen SQLite file and in-memory DDL canonicalization are allowed; all non-loopback network denied",
            "Separate four-single, one-batch20 and extra four-batch20 scenarios; every selected published_at changes and revision invalidates in the same transaction",
            "Request-to-ready timing starts after the input commit and includes all coordinator and SQLite waiting",
            "Same-process callers exercise the local mutex and SQL fencing; cross-process fencing has separate correctness tests"],
        "capture_first_unmet_gates": dict(Counter(row.get("first_unmet_gate", "unexpected_claim") for row in capture_samples)),
        "capture": {"seconds": distribution([row["seconds"] for row in capture_samples]), "samples": capture_samples},
        "preparation": {"seconds": distribution([row["seconds"] for row in preparation_samples]), "samples": preparation_samples},
        "enqueue": {"seconds": distribution([row["seconds"] for row in enqueue_samples]), "samples": enqueue_samples},
        "duplicate": {"samples": duplicate_samples, "individual_calls": calls},
        "duplicate_transaction_lock_hold_ms": holds,
        "transactions": {job: {"count": len(rows), "queue_wait_ms": distribution([row["queue_wait_ms"] for row in rows]),
                              "hold_ms": distribution([row["hold_ms"] for row in rows]),
                              "lock_hold_ms": distribution([row["lock_hold_ms"] for row in rows if row.get("lock_hold_ms") is not None])}
                         for job, rows in grouped.items()},
        "denied_external_access": denied, "provider_dispatch_calls": len(dispatch_attempts),
        "full_paid_acceptance": False, "failures": failures}
    report["passed"] = (not denied and not dispatch_attempts and not failures and bool(capture_samples)
        and postconditions["selected_all_ready"] and stale_endpoints == 0
        and all(row.get("slot_guard_reached") for row in capture_samples)
        and all(scenario_reports[name]["latency_passed"] and scenario_reports[name]["duplicate_lock_passed"]
                for name in ("four_single", "batch20")))
    (output / "integration.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"output": str(output / "integration.json"), "passed": report["passed"],
        "scenarios": scenario_reports, "capture_claims": len(capture_samples),
        "full_paid_acceptance": False}, ensure_ascii=False))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
