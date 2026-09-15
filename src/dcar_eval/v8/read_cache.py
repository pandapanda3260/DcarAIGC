"""Bounded process-local read cache; no writer hooks or database mutations.

data_version is only a trigger to compare domain summaries, never a global
cache key. Summaries are intentionally not claimed to be transaction revisions:
large-table updates that preserve indexed sentinels are bounded by hard TTL.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from collections import OrderedDict
from concurrent.futures import Future
from dataclasses import dataclass
from pathlib import Path
from time import monotonic
from typing import Callable

from .storage import configure_connection_safety

ACCOUNT_TABLES = frozenset({
    "accounts", "account_directory_rows", "account_platform_identities",
    "account_provider_references", "account_roster_members", "account_roster_snapshots",
    "account_metric_observations", "account_state_events", "account_intake_requests",
    "account_preparation_owners", "acquisition_profile_activations", "capture_source_plans",
    "scheduler_runs", "scheduler_run_attempts", "capture_paid_send_gate_events", "capture_work_items",
    "content_items",
})
CONTENT_TABLES = ACCOUNT_TABLES | frozenset({
    "content_metric_observations", "content_metric_snapshots", "metric_observations",
    "evaluation_versions", "evaluation_matches", "evaluation_releases", "taxonomy_versions",
    "selling_points", "duplicate_relations", "duplicate_fingerprints",
    "duplicate_index_generations", "duplicate_current_fingerprints", "duplicate_fingerprint_media",
    "duplicate_fingerprint_frames", "duplicate_match_edges", "duplicate_components",
    "duplicate_component_members", "duplicate_dirty_work", "duplicate_work_staging",
    "content_spu_links", "content_audience_links", "content_scene_links", "spu_catalog",
    "audience_dim", "scene_dim", "spu_audience_map", "audience_scene_map", "spu_alias",
    "content_media_assets", "media_assets", "evidence_artifacts",
    "processor_runs", "content_processors", "content_analysis_jobs",
})
DOMAIN_TABLES = {
    "accounts": ACCOUNT_TABLES,
    "contents": CONTENT_TABLES,
    "overview": CONTENT_TABLES | frozenset({"profile_day_coverage_receipts", "duplicate_calibration_runs",
        "provider_raw_responses", "fetch_attempts", "fetch_slots", "comment_evidence_versions",
        "comment_capture_runs", "media_processing_slots", "runtime_receipt_revocations", "data_quality_receipts"}),
    "selling-points": CONTENT_TABLES,
    "spu": CONTENT_TABLES | frozenset({"spu_association_runs", "spu_aliases"}),
}
EXACT_TABLES = frozenset({"accounts", "account_directory_rows", "account_platform_identities",
                         "taxonomy_versions", "evaluation_releases", "selling_points", "duplicate_index_generations"})


class DatabaseRevision:
    def __init__(self, path: Path, validate: Callable[[], object] | None = None,
                 *, check_interval=5.0, clock=monotonic):
        self.path, self.validate = Path(path), validate
        self.check_interval, self.clock, self.next_check = check_interval, clock, 0.0
        self.lock = threading.RLock()
        self.connection: sqlite3.Connection | None = None
        self.identity = None
        self.epoch = 0
        self.version = None
        self.schema_version = None
        self.schema: dict[str, tuple[str, ...]] = {}
        self.indexed: dict[str, frozenset[str]] = {}
        self.summaries: dict[tuple[int, str], str] = {}
        self.table_summaries: dict[str, bytes] = {}

    def _open(self):
        stat = self.path.stat()
        identity = (stat.st_dev, stat.st_ino)
        if self.connection is not None and identity == self.identity:
            return
        if self.validate:
            self.validate()
        self.close()
        connection = sqlite3.connect(f"{self.path.resolve().as_uri()}?mode=ro", uri=True,
                                     timeout=2, check_same_thread=False)
        try:
            configure_connection_safety(connection)
            connection.execute("PRAGMA query_only=ON")
            connection.execute("PRAGMA busy_timeout=2000")
            self.schema_version = int(connection.execute("PRAGMA schema_version").fetchone()[0])
            self.schema = {
                str(row[0]): tuple(str(col[1]) for col in connection.execute(
                    'PRAGMA table_info("' + str(row[0]).replace('"', '""') + '")'))
                for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            self.indexed = {}
            for table in self.schema:
                quoted = table.replace('"', '""')
                info = list(connection.execute(f'PRAGMA table_info("{quoted}")'))
                keys = [str(row[1]) for row in info if row[5] == 1]
                for index in connection.execute(f'PRAGMA index_list("{quoted}")'):
                    if index[4]:  # partial indexes do not describe every row
                        continue
                    index_name = str(index[1]).replace('"', '""')
                    first = connection.execute(f'PRAGMA index_info("{index_name}")').fetchone()
                    if first and first[2]:
                        keys.append(str(first[2]))
                self.indexed[table] = frozenset(keys)
        except BaseException:
            connection.close()
            raise
        self.connection, self.identity = connection, identity
        self.epoch += 1
        self.version = None
        self.next_check = 0.0
        self.summaries.clear()
        self.table_summaries.clear()

    def close(self):
        with self.lock:
            if self.connection is not None:
                self.connection.close()
                self.connection = None

    def invalidate(self, domains):
        with self.lock:
            self.next_check = 0.0
            for key in list(self.summaries):
                if key[1] in domains:
                    self.summaries.pop(key)
            for table in set().union(*(DOMAIN_TABLES[d] for d in domains)):
                self.table_summaries.pop(table, None)

    def get(self, domain: str) -> tuple[int, str]:
        with self.lock:
            self._open()
            connection = self.connection
            assert connection is not None
            # Scheduler commits do not make every HTTP request inspect the
            # database. All domains share a bounded five-second observation
            # window; acknowledged gateway writes explicitly bypass it.
            if self.clock() >= self.next_check:
                version = int(connection.execute("PRAGMA data_version").fetchone()[0])
                if version != self.version:
                    schema_version = int(connection.execute("PRAGMA schema_version").fetchone()[0])
                    if schema_version != self.schema_version:
                        self.close()
                        return self.get(domain)
                    self.summaries.clear()
                    self.table_summaries.clear()
                    self.version = version
                self.next_check = self.clock() + self.check_interval
            version = self.version
            cached = self.summaries.get((version, domain))
            if cached is not None:
                return self.epoch, cached
            digest = hashlib.sha256()
            connection.execute("BEGIN")
            try:
                digest.update(str(connection.execute("PRAGMA schema_version").fetchone()[0]).encode())
                for table in sorted(DOMAIN_TABLES[domain] & self.schema.keys()):
                    columns = self.schema[table]
                    digest.update(table.encode())
                    if table in self.table_summaries:
                        digest.update(self.table_summaries[table])
                        continue
                    table_digest = hashlib.sha256()
                    if table in EXACT_TABLES:
                        # These small operator metadata tables need same-second
                        # edit detection, including legitimate metric decreases.
                        rows = connection.execute(f'SELECT * FROM "{table}" ORDER BY 1')
                    else:
                        # Separate MAX probes can use each leading index. A
                        # combined COUNT/MAX query scans huge metric tables.
                        # Missing sentinels and updates/deletes preserving them
                        # are explicitly covered by the hard thirty-second TTL.
                        sentinels = [column for column in columns if column in self.indexed[table]
                                     and (column == "id" or column.endswith("_at"))]
                        rows = [tuple(connection.execute(f'SELECT MAX("{column}") FROM "{table}"').fetchone())
                                for column in sentinels]
                    for row in rows:
                        table_digest.update(json.dumps(tuple(row), ensure_ascii=False, default=str,
                                                       separators=(",", ":")).encode())
                    summary = table_digest.digest()
                    self.table_summaries[table] = summary
                    digest.update(summary)
                result = digest.hexdigest()[:24]
            finally:
                connection.rollback()
            self.summaries[(version, domain)] = result
            return self.epoch, result


@dataclass(frozen=True)
class CachedResult:
    body: bytes
    revision: str


class ReadCacheBusy(RuntimeError):
    pass


class BoundedReadCache:
    def __init__(self, revision: DatabaseRevision, *, max_entries=128, max_bytes=16 * 1024 * 1024,
                 ttl_seconds=30.0, max_inflight=16, max_loaders=4, clock=monotonic):
        if not (max_entries > 0 and max_bytes > 0 and 0 < ttl_seconds <= 30 and max_inflight > 0 and max_loaders > 0):
            raise ValueError("invalid read cache limits")
        self.revision, self.clock = revision, clock
        self.max_entries, self.max_bytes, self.ttl = max_entries, max_bytes, ttl_seconds
        self.max_inflight = max_inflight
        self.loaders = threading.BoundedSemaphore(max_loaders)
        self.lock = threading.RLock()
        self.values: OrderedDict[tuple, tuple[float, CachedResult]] = OrderedDict()
        self.inflight: dict[tuple, Future] = {}
        self.generations: dict[str, int] = {}
        self.bytes = 0
        self.epoch = None

    def invalidate(self, domains):
        self.revision.invalidate(domains)
        with self.lock:
            for domain in domains:
                self.generations[domain] = self.generations.get(domain, 0) + 1
            for key in list(self.values):
                if key[0] in domains:
                    self.bytes -= len(self.values.pop(key)[1].body)

    def get(self, domain: str, key: tuple, loader: Callable[[], dict], timings: dict) -> CachedResult:
        started = self.clock()
        epoch, revision = self.revision.get(domain)
        timings["revision"] = (self.clock() - started) * 1000
        with self.lock:
            if self.epoch != epoch:
                self.values.clear()
                self.bytes = 0
                self.epoch = epoch
            generation = self.generations.get(domain, 0)
            cache_key = (domain, epoch, revision, generation, *key)
            now = self.clock()
            for expired in [k for k, (deadline, _) in self.values.items() if deadline <= now]:
                self.bytes -= len(self.values.pop(expired)[1].body)
            if cache_key in self.values:
                timings["cache"] = "hit"
                self.values.move_to_end(cache_key)
                return self.values[cache_key][1]
            future = self.inflight.get(cache_key)
            owner = future is None
            if owner:
                if len(self.inflight) >= self.max_inflight:
                    raise ReadCacheBusy("reader concurrency limit reached")
                future = Future()
                self.inflight[cache_key] = future
        assert future is not None
        if not owner:
            timings["cache"] = "coalesced"
            wait = self.clock()
            try:
                result = future.result(timeout=60)
            except TimeoutError as error:
                raise ReadCacheBusy("reader shared computation timed out") from error
            timings["wait"] = (self.clock() - wait) * 1000
            return result
        timings["cache"] = "miss"
        try:
            queue = self.clock()
            if not self.loaders.acquire(timeout=5):
                raise ReadCacheBusy("reader worker queue is full")
            try:
                timings["queue"] = (self.clock() - queue) * 1000
                compute = self.clock()
                value = loader()
                timings["compute"] = (self.clock() - compute) * 1000
                serialize = self.clock()
                body = json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
                timings["serialize"] = (self.clock() - serialize) * 1000
                # The outward revision identifies the actual response bytes;
                # a sentinel collision must not claim unchanged refreshed data.
                result = CachedResult(body, f"{epoch}-{revision}-{generation}-{hashlib.sha256(body).hexdigest()[:16]}")
            finally:
                self.loaders.release()
            # Invalidation during computation must never repopulate old data.
            with self.lock:
                if self.epoch == epoch and self.generations.get(domain, 0) == generation and len(body) <= self.max_bytes:
                    self.values[cache_key] = (compute + self.ttl, result)
                    self.bytes += len(body)
                    while len(self.values) > self.max_entries or self.bytes > self.max_bytes:
                        _, (_, evicted) = self.values.popitem(last=False)
                        self.bytes -= len(evicted.body)
            future.set_result(result)
            return result
        except BaseException as error:
            future.set_exception(error)
            raise
        finally:
            with self.lock:
                self.inflight.pop(cache_key, None)
