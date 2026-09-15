"""Durable, fenced duplicate graph work with bounded, resumable comparisons."""
from __future__ import annotations

import json
import hashlib
import os
import sqlite3
import threading
import weakref
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

from . import duplicate_graph as graph
from .storage import DEFAULT_DB, connect, live_wal_read_only_connections, now_utc, transaction, transaction_metrics_context

LEASE_SECONDS = 30
RENEW_SECONDS = 10
MAX_BATCH = 20
_LOCAL_COORDINATORS = weakref.WeakValueDictionary()
_LOCAL_COORDINATORS_GUARD = threading.Lock()


class DuplicateConflict(RuntimeError):
    """The read result no longer belongs to the current source or lease."""

    def __init__(self, message, *, work_ids=()):
        super().__init__(message)
        self.work_ids = tuple(work_ids)


@contextmanager
def _read(db_path: Path) -> Iterator[sqlite3.Connection]:
    with live_wal_read_only_connections(), connect(db_path, read_only=True) as connection:
        connection.execute("BEGIN")
        try:
            yield connection
        finally:
            connection.rollback()


def _after(seconds: float) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat(timespec="seconds").replace("+00:00", "Z")


def _generation(connection: sqlite3.Connection) -> dict[str, Any] | None:
    row = connection.execute("SELECT * FROM duplicate_index_generations WHERE state='ready'").fetchone()
    return dict(row) if row else None


def _chunks(values: Sequence[Any], size: int = 400):
    for start in range(0, len(values), size):
        yield values[start:start + size]


def _rows_for_ids(connection, table: str, generation: str, ids: Sequence[int], *, column="content_id"):
    rows = []
    for chunk in _chunks(sorted(set(ids))):
        rows.extend(dict(row) for row in connection.execute(
            f"SELECT * FROM {table} WHERE generation_id=? AND {column} IN ({','.join('?' for _ in chunk)})",
            (generation, *chunk)))
    return rows


def _work_identity(row):
    return None if row is None else (row["target_fingerprint_id"], row["target_input_revision"], row["reason"])


def _eligible(connection, generation: str, ids: Sequence[int] | None, limit: int):
    # Invalidations and tombstones can run without a fingerprint. Matching a
    # newly unavailable source is deferred by readiness even after invalidation.
    parameters: list[Any] = [generation, now_utc()]
    scope = ""
    if ids is not None:
        if not ids:
            return []
        # Scope normally contains <=20 ids. Chunk before callers pass SQL's
        # parameter limit; sorting globally below preserves deterministic order.
        if len(ids) > 400:
            combined = [row for chunk in _chunks(list(ids)) for row in _eligible(connection, generation, chunk, limit)]
            return sorted(combined, key=lambda row: (bool(json.loads(row["checkpoint_json"]).get("edges_complete")), row["content_id"]))[:limit]
        scope = " AND w.content_id IN (" + ",".join("?" for _ in ids) + ")"
        parameters.extend(ids)
    parameters.append(limit)
    return [dict(row) for row in connection.execute(
        "SELECT w.* FROM duplicate_dirty_work w WHERE w.generation_id=? "
        "AND w.status IN ('pending','retryable','claimed') AND (w.retry_at IS NULL OR w.retry_at<=?)" + scope +
        " ORDER BY COALESCE(json_extract(w.checkpoint_json,'$.edges_complete'),0),w.content_id LIMIT ?", parameters)]


def _fence(connection, generation: str, token: str):
    row = connection.execute("SELECT * FROM duplicate_index_generations WHERE generation_id=?", (generation,)).fetchone()
    if row is None or row["state"] != "ready" or row["lease_token"] != token or not row["lease_until"] or row["lease_until"] <= now_utc():
        raise DuplicateConflict("duplicate graph lease or generation changed")
    return dict(row)


def _acquire(db_path, generation: str):
    token = uuid.uuid4().hex
    with connect(db_path) as connection, transaction_metrics_context(job_id="duplicate_graph_claim"), transaction(connection):
        cursor = connection.execute("UPDATE duplicate_index_generations SET graph_worker_owner=?,lease_token=?,lease_until=? "
            "WHERE generation_id=? AND state='ready' AND (lease_token IS NULL OR lease_until<=?)",
            (f"{os.getpid()}:{token}", token, _after(LEASE_SECONDS), generation, now_utc()))
        return token if cursor.rowcount else None


def _renew(db_path, generation: str, token: str):
    with connect(db_path) as connection, transaction_metrics_context(job_id="duplicate_graph_renew"), transaction(connection):
        _fence(connection, generation, token)
        connection.execute("UPDATE duplicate_index_generations SET lease_until=? WHERE generation_id=? AND lease_token=?",
                           (_after(LEASE_SECONDS), generation, token))


def _release(db_path, generation: str, token: str):
    with connect(db_path) as connection, transaction_metrics_context(job_id="duplicate_graph_release"), transaction(connection):
        connection.execute("UPDATE duplicate_index_generations SET graph_worker_owner=NULL,lease_token=NULL,lease_until=NULL "
                           "WHERE generation_id=? AND lease_token=?", (generation, token))


def _match_snapshot(connection, generation: str, works, fallback_full_scan: bool):
    from . import duplicate_index as index
    seeds = [int(row["content_id"]) for row in works]
    reusable = {int(work["content_id"]) for work in works if json.loads(work["checkpoint_json"]).get("reuse_verified_edges")}
    candidates = index.query_candidate_ids(connection, [cid for cid in seeds if cid not in reusable], generation_id=generation,
                                           fallback_full_scan=fallback_full_scan)
    verified_comparisons = {}
    if reusable:
        old_edges = {}
        for column in ("left_content_id", "right_content_id"):
            for row in _rows_for_ids(connection, "duplicate_match_edges", generation, sorted(reusable), column=column):
                old_edges[(row["left_content_id"], row["right_content_id"])] = row
        endpoints = reusable.union(*(set(pair) for pair in old_edges))
        current = {row["content_id"]: row for row in _rows_for_ids(connection, "duplicate_current_fingerprints", generation, sorted(endpoints))}
        for seed in reusable:
            candidates[seed] = set()
            verified_comparisons[seed] = {}
        for (left, right), edge in old_edges.items():
            if any(cid not in current or current[cid]["input_status"] != "available" or
                   current[cid]["fingerprint_id"] != edge[f"{side}_fingerprint_id"] for side, cid in (("left", left), ("right", right))):
                continue
            for seed, other in ((left, right), (right, left)):
                if seed in reusable:
                    candidates[seed].add(other)
                    verified_comparisons[seed][other] = json.loads(edge["comparison_json"])
    ids = sorted(set(seeds).union(*(set(value) for value in candidates.values())))
    fingerprints = index.read_current_fingerprints(connection, ids, generation_id=generation)
    if not isinstance(fingerprints, Mapping):
        fingerprints = {int(row["content_id"]): row for row in fingerprints}
    pointers = {row["content_id"]: row for row in _rows_for_ids(connection, "duplicate_current_fingerprints", generation, ids)}
    digests, prior = {}, {}
    for work in works:
        cid, revision = int(work["content_id"]), int(work["target_input_revision"])
        digests[cid] = graph.work_digest(cid, pointers, sorted(candidates.get(cid, ())), revision)
        prior[cid] = {}
        for row in connection.execute("SELECT * FROM duplicate_work_staging WHERE generation_id=? AND work_content_id=? "
                "AND target_input_revision=? AND record_type='comparison' AND input_snapshot_digest=?",
                (generation, cid, revision, digests[cid])):
            value = json.loads(row["result_json"])
            prior[cid][int(value["other"])] = {"input_snapshot_digest": row["input_snapshot_digest"], "comparison": value["comparison"]}
    return {"generation_id": generation, "fingerprints": fingerprints, "pointers": pointers,
            "candidates": candidates, "digests": digests, "prior": prior, "works": works, "verified_comparisons": verified_comparisons}


def _persist_computation(db_path, snapshot, computation, token):
    generation = snapshot["generation_id"]
    prepared_updates = []
    for work in snapshot["works"]:
        cid, revision = int(work["content_id"]), int(work["target_input_revision"])
        result = computation["works"][cid]
        digest = result["input_snapshot_digest"]
        ordered = sorted(snapshot["candidates"].get(cid, ()))
        ordinals = {other: position for position, other in enumerate(ordered)}
        records = []
        for other, comparison in result["new_records"].items():
            body = {"other": other, "comparison": comparison,
                    "seed_pointer": graph.pointer_identity(snapshot["pointers"].get(cid)),
                    "other_pointer": graph.pointer_identity(snapshot["pointers"].get(other))}
            records.append((generation, cid, revision, ordinals[other] // graph.PAIR_CHUNK_SIZE, str(other), digest,
                            str(ordinals[other] + 1), graph.canonical_json(body), token, now_utc()))
        checkpoint_data = {"input_snapshot_digest": digest, "cursor": result["cursor"],
            "candidate_count": result["candidate_count"], "edges_complete": result["complete"]}
        if json.loads(work["checkpoint_json"]).get("reuse_verified_edges"):
            checkpoint_data["reuse_verified_edges"] = True
        checkpoint = graph.canonical_json(checkpoint_data)
        prepared_updates.append((work, cid, revision, digest, records, checkpoint))
    with connect(db_path) as connection, transaction_metrics_context(job_id="duplicate_graph_checkpoint"), transaction(connection):
        _fence(connection, generation, token)
        for work, cid, revision, digest, records, checkpoint in prepared_updates:
            actual = connection.execute("SELECT * FROM duplicate_dirty_work WHERE generation_id=? AND content_id=?", (generation, cid)).fetchone()
            if _work_identity(actual) != _work_identity(work):
                raise DuplicateConflict("duplicate work revision changed before checkpoint")
            pointer = connection.execute("SELECT * FROM duplicate_current_fingerprints WHERE generation_id=? AND content_id=?", (generation, cid)).fetchone()
            if graph.pointer_identity(dict(pointer) if pointer else None) != graph.pointer_identity(snapshot["pointers"].get(cid)):
                raise DuplicateConflict("duplicate seed changed before checkpoint")
            connection.execute("DELETE FROM duplicate_work_staging WHERE generation_id=? AND work_content_id=? "
                "AND (target_input_revision<>? OR (record_type='comparison' AND input_snapshot_digest<>?))", (generation, cid, revision, digest))
            connection.executemany("INSERT OR REPLACE INTO duplicate_work_staging "
                "(generation_id,work_content_id,target_input_revision,chunk_no,record_key,record_type,input_snapshot_digest,cursor,result_json,lease_token,created_at) "
                "VALUES(?,?,?,?,?,'comparison',?,?,?,?,?)", records)
            connection.execute("UPDATE duplicate_dirty_work SET checkpoint_json=?,status='pending',lease_token=?,retry_at=NULL "
                "WHERE generation_id=? AND content_id=? AND target_input_revision=?",
                (checkpoint, token, generation, cid, revision))


def _completed_comparisons(connection, generation: str, work):
    checkpoint = json.loads(work["checkpoint_json"])
    if not checkpoint.get("edges_complete"):
        return None
    records = []
    for row in connection.execute("SELECT * FROM duplicate_work_staging WHERE generation_id=? AND work_content_id=? "
            "AND target_input_revision=? AND record_type='comparison' AND input_snapshot_digest=?",
            (generation, work["content_id"], work["target_input_revision"], checkpoint["input_snapshot_digest"])):
        records.append(json.loads(row["result_json"]))
    if len(records) != checkpoint.get("candidate_count"):
        raise DuplicateConflict("duplicate comparison checkpoint is incomplete", work_ids=[work["content_id"]])
    return records



def _encode_graph_delta(delta):
    value = dict(delta)
    value.pop("_staged", None)
    value["edges"] = list(delta["edges"].values())
    value["projections"] = list(delta["projections"].values())
    value["members"] = list(delta["members"].items())
    snapshot = dict(delta["snapshot"])
    snapshot["pointers"] = list(snapshot["pointers"].values())
    snapshot["works"] = list(snapshot["works"].values())
    snapshot["members"] = list(snapshot["members"].items())
    snapshot["pointer_present"] = sorted(snapshot["pointer_present"])
    value["snapshot"] = snapshot
    return graph.canonical_json(value)


def _decode_graph_delta(body):
    value = json.loads(body)
    value["edges"] = {(row["left_content_id"], row["right_content_id"]): row for row in value["edges"]}
    value["projections"] = {(row["duplicate_content_id"], row["original_content_id"]): row for row in value["projections"]}
    value["members"] = {int(cid): component for cid, component in value["members"]}
    snapshot = value["snapshot"]
    snapshot["pointers"] = {row["content_id"]: row for row in snapshot["pointers"]}
    snapshot["works"] = {row["content_id"]: row for row in snapshot["works"]}
    snapshot["members"] = {int(cid): component for cid, component in snapshot["members"]}
    snapshot["pointer_present"] = set(snapshot["pointer_present"])
    value["_staged"] = True
    return value


def _cached_graph_delta(connection, generation, seed):
    work = connection.execute("SELECT * FROM duplicate_dirty_work WHERE generation_id=? AND content_id=?", (generation, seed)).fetchone()
    if not work:
        return None
    rows = list(connection.execute("SELECT * FROM duplicate_work_staging WHERE generation_id=? AND work_content_id=? "
        "AND target_input_revision=? AND record_type='graph' ORDER BY chunk_no", (generation, seed, work["target_input_revision"])))
    manifest = next((json.loads(row["result_json"]) for row in rows if row["record_key"] == "graph-manifest"), None)
    if manifest is None:
        return None
    pieces = [json.loads(row["result_json"])["piece"] for row in rows if row["record_key"] == "graph-payload"
              and row["input_snapshot_digest"] == manifest["sha256"]]
    if len(pieces) != manifest["pieces"]:
        raise DuplicateConflict("graph checkpoint is incomplete", work_ids=[seed])
    body = "".join(pieces)
    if hashlib.sha256(body.encode()).hexdigest() != manifest["sha256"]:
        raise DuplicateConflict("graph checkpoint digest changed", work_ids=[seed])
    return _decode_graph_delta(body)


def _stage_graph_delta(delta, seed, token, db_path):
    """Keep a completed, unpublished graph diff when the foreground budget ends.

    Graph snapshots are split into bounded 64KiB records. A manifest written last
    makes interrupted staging invisible; resume still executes all commit CAS.
    """
    body = _encode_graph_delta(delta)
    digest = hashlib.sha256(body.encode()).hexdigest()
    pieces = [body[offset:offset + 65536] for offset in range(0, len(body), 65536)]
    generation = delta["generation_id"]
    work = delta["snapshot"]["works"][seed]
    revision = work["target_input_revision"]
    records = [(position + 1, "graph-payload", {"piece": piece}) for position, piece in enumerate(pieces)]
    records.append((0, "graph-manifest", {"sha256": digest, "pieces": len(pieces)}))
    # JSON formatting and graph work are already finished before every lock.
    records = [(number, key, graph.canonical_json(value)) for number, key, value in records]
    for position, (number, key, value) in enumerate(records):
        with connect(db_path) as connection, transaction_metrics_context(job_id="duplicate_graph_stage"), transaction(connection):
            _fence(connection, generation, token)
            actual = connection.execute("SELECT * FROM duplicate_dirty_work WHERE generation_id=? AND content_id=?", (generation, seed)).fetchone()
            if _work_identity(actual) != _work_identity(work):
                raise DuplicateConflict("graph work changed before staging")
            if position == 0:
                connection.execute("DELETE FROM duplicate_work_staging WHERE generation_id=? AND work_content_id=? AND record_type='graph'", (generation, seed))
            connection.execute("INSERT INTO duplicate_work_staging(generation_id,work_content_id,target_input_revision,chunk_no,record_key,"
                "record_type,input_snapshot_digest,cursor,result_json,lease_token,created_at) VALUES(?,?,?,?,?,'graph',?,?,?,?,?)",
                (generation, seed, revision, number, key, digest, str(number), value, token, now_utc()))

def _publication_snapshot(connection, generation: str, seed: int):
    """Expand old components and new *positive* endpoints, not all candidates."""
    staged = _cached_graph_delta(connection, generation, seed)
    if staged is not None:
        return staged
    affected = {seed}
    components: dict[str, dict[str, Any]] = {}
    works: dict[int, dict[str, Any]] = {}
    replacements, matched_bindings, binding_owners = {}, {}, {}
    members: dict[int, str] = {}
    waiting = False
    while True:
        before = set(affected)
        for member in _rows_for_ids(connection, "duplicate_component_members", generation, sorted(affected)):
            component_id = member["component_id"]
            if component_id in components:
                continue
            row = connection.execute("SELECT * FROM duplicate_components WHERE generation_id=? AND component_id=?", (generation, component_id)).fetchone()
            if row is not None:
                components[component_id] = dict(row)
                for value in connection.execute("SELECT content_id FROM duplicate_component_members WHERE generation_id=? AND component_id=?", (generation, component_id)):
                    members[int(value[0])] = component_id
                    affected.add(int(value[0]))
        for work in _rows_for_ids(connection, "duplicate_dirty_work", generation, sorted(affected)):
            cid = int(work["content_id"])
            if work["status"] == "ready" or cid in works:
                continue
            works[cid] = work
            old_id = work["old_component_id"]
            if old_id and old_id not in components:
                row = connection.execute("SELECT * FROM duplicate_components WHERE generation_id=? AND component_id=?", (generation, old_id)).fetchone()
                if row:
                    components[old_id] = dict(row)
                    for member in connection.execute("SELECT content_id FROM duplicate_component_members WHERE generation_id=? AND component_id=?", (generation, old_id)):
                        members[int(member[0])] = old_id
                        affected.add(int(member[0]))
            records = _completed_comparisons(connection, generation, work)
            if records is None:
                waiting = True
                continue
            replacements[cid] = {}
            for value in records:
                other = int(value["other"])
                if value["comparison"]["confirmed"]:
                    affected.add(other)
                    replacements[cid][other] = value["comparison"]
                    for endpoint, identity in ((cid, value["seed_pointer"]), (other, value["other_pointer"])):
                        identity = tuple(identity) if identity else None
                        if endpoint in matched_bindings and matched_bindings[endpoint] != identity:
                            raise DuplicateConflict("staged duplicate endpoints disagree", work_ids=[cid, *binding_owners.get(endpoint, ())])
                        matched_bindings[endpoint] = identity
                        binding_owners.setdefault(endpoint, set()).add(cid)
        if affected == before:
            break
    if waiting:
        return None
    pointer_rows = _rows_for_ids(connection, "duplicate_current_fingerprints", generation, sorted(affected))
    pointers = {int(row["content_id"]): row for row in pointer_rows}
    for chunk in _chunks(sorted(affected)):
        for row in connection.execute("SELECT id,published_at,imported_at FROM content_items WHERE id IN (" + ",".join("?" for _ in chunk) + ")", chunk):
            if int(row["id"]) in pointers:
                pointers[int(row["id"])].update(published_at=row["published_at"], imported_at=row["imported_at"])
    # Deleted points still belong to the affected preimage but have no pointer.
    for cid in affected:
        pointers.setdefault(cid, {"content_id": cid, "input_status": "unavailable", "fingerprint_id": None,
                                  "source_sha256": None, "input_revision": None})
    for cid, expected in matched_bindings.items():
        if graph.pointer_identity(pointers.get(cid)) != expected:
            raise DuplicateConflict("staged duplicate endpoint is no longer current", work_ids=binding_owners[cid])
    edges = {}
    for column in ("left_content_id", "right_content_id"):
        for row in _rows_for_ids(connection, "duplicate_match_edges", generation, sorted(affected), column=column):
            edges[(row["left_content_id"], row["right_content_id"])] = row
    relations = {}
    for column in ("duplicate_content_id", "original_content_id"):
        for chunk in _chunks(sorted(affected)):
            for row in connection.execute("SELECT * FROM duplicate_relations WHERE method IN ('fingerprint_v1','text_sha256') AND " +
                    column + " IN (" + ",".join("?" for _ in chunk) + ")", chunk):
                relations[row["id"]] = dict(row)
    snapshot = {"generation_id": generation, "pointers": pointers, "pointer_present": set(row["content_id"] for row in pointer_rows),
                "components": components, "members": members, "works": works, "edges": list(edges.values()),
                "relations": list(relations.values()), "affected_ids": sorted(affected)}
    delta = graph.build_component_delta(snapshot, replacements)
    delta["snapshot"] = snapshot
    return delta



def _merge_graph_deltas(deltas):
    if len(deltas) == 1:
        return deltas[0]
    generation = deltas[0]["generation_id"]
    snapshot = {"generation_id": generation, "pointers": {}, "pointer_present": set(), "components": {},
                "members": {}, "works": {}, "edges": [], "relations": [], "affected_ids": []}
    old_edges, desired_edges, relations = {}, {}, {}
    for delta in deltas:
        source = delta["snapshot"]
        for key in ("pointers", "components", "members", "works"):
            snapshot[key].update(source[key])
        snapshot["pointer_present"].update(source["pointer_present"])
        old_edges.update({(row["left_content_id"], row["right_content_id"]): row for row in source["edges"]})
        desired_edges.update(delta["edges"])
        relations.update({row["id"]: row for row in source["relations"]})
    snapshot["edges"] = list(old_edges.values())
    snapshot["relations"] = list(relations.values())
    snapshot["affected_ids"] = sorted(snapshot["pointers"])
    # Two new seeds may join different members of the same old component. Union
    # their complete direct-edge results, then project the union once; merging
    # independently computed canonical stars would lose one of those joins.
    merged = graph.build_component_delta({**snapshot, "edges": list(desired_edges.values())}, {})
    merged["snapshot"] = snapshot
    merged["_staged"] = all(delta.get("_staged") for delta in deltas)
    return merged

def commit_graph_delta(delta: Mapping[str, Any], lease_token: str, *, db_path: Path = DEFAULT_DB):
    """Fence changed endpoints/components and publish a precomputed exact diff."""
    generation, snapshot = delta["generation_id"], delta["snapshot"]
    old_edges = {(row["left_content_id"], row["right_content_id"]): row for row in snapshot["edges"]}
    edge_fields = ("left_fingerprint_id", "right_fingerprint_id", "left_input_revision", "right_input_revision", "confidence", "comparison_json")
    old_relations = {(row["duplicate_content_id"], row["original_content_id"]): row
                     for row in snapshot["relations"] if row["method"] == "fingerprint_v1"}
    desired_relations = delta["projections"]
    edge_deletes = set(old_edges) - set(delta["edges"])
    edge_writes = {pair: row for pair, row in delta["edges"].items() if pair not in old_edges or
                   any(old_edges[pair][field] != row[field] for field in edge_fields)}
    relation_deletes = [row["id"] for pair, row in old_relations.items() if pair not in desired_relations]
    relation_writes = {pair: row for pair, row in desired_relations.items() if pair not in old_relations or
        any(old_relations[pair][field] != row[field] for field in ("confidence", "evidence_json", "status"))}
    text_deletes = [row["id"] for row in snapshot["relations"] if row["method"] == "text_sha256" and
        row["duplicate_content_id"] in delta["affected_ids"] and row["original_content_id"] in delta["affected_ids"]]
    now = now_utc()
    with connect(db_path) as connection, transaction_metrics_context(job_id="duplicate_graph_commit"), transaction(connection):
        generation_row = _fence(connection, generation, lease_token)
        for cid in snapshot["affected_ids"]:
            actual = connection.execute("SELECT * FROM duplicate_current_fingerprints WHERE generation_id=? AND content_id=?", (generation, cid)).fetchone()
            expected = snapshot["pointers"][cid] if cid in snapshot["pointer_present"] else None
            if graph.pointer_identity(dict(actual) if actual else None) != graph.pointer_identity(expected):
                raise DuplicateConflict("duplicate input changed before graph commit", work_ids=tuple(snapshot["works"]) if delta.get("_staged") else ())
        for component_id, expected in snapshot["components"].items():
            actual = connection.execute("SELECT component_revision FROM duplicate_components WHERE generation_id=? AND component_id=?", (generation, component_id)).fetchone()
            if not actual or actual[0] != expected["component_revision"]:
                raise DuplicateConflict("duplicate component changed before graph commit", work_ids=tuple(snapshot["works"]) if delta.get("_staged") else ())
        for cid, expected in snapshot["works"].items():
            actual = connection.execute("SELECT * FROM duplicate_dirty_work WHERE generation_id=? AND content_id=?", (generation, cid)).fetchone()
            if _work_identity(actual) != _work_identity(expected) or actual["checkpoint_json"] != expected["checkpoint_json"]:
                raise DuplicateConflict("duplicate work changed before graph commit", work_ids=tuple(snapshot["works"]) if delta.get("_staged") else ())
        revision = int(generation_row["graph_revision"]) + 1
        for left, right in edge_deletes:
            connection.execute("DELETE FROM duplicate_match_edges WHERE generation_id=? AND left_content_id=? AND right_content_id=?", (generation, left, right))
        for (left, right), row in edge_writes.items():
            connection.execute("INSERT INTO duplicate_match_edges(generation_id,left_content_id,right_content_id,left_fingerprint_id,right_fingerprint_id,"
                "left_input_revision,right_input_revision,confidence,comparison_json,edge_revision) VALUES(?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(generation_id,left_content_id,right_content_id) DO UPDATE SET left_fingerprint_id=excluded.left_fingerprint_id,"
                "right_fingerprint_id=excluded.right_fingerprint_id,left_input_revision=excluded.left_input_revision,right_input_revision=excluded.right_input_revision,"
                "confidence=excluded.confidence,comparison_json=excluded.comparison_json,edge_revision=excluded.edge_revision",
                (generation, left, right, *(row[field] for field in edge_fields), revision))
        # Delete moved membership before retiring components (FK cascade cannot
        # destroy a membership already moved to its replacement component).
        for cid, old_component in snapshot["members"].items():
            if delta["members"].get(cid) != old_component:
                connection.execute("DELETE FROM duplicate_component_members WHERE generation_id=? AND content_id=?", (generation, cid))
        for component_id in set(snapshot["components"]) - set(delta["components"]):
            connection.execute("DELETE FROM duplicate_components WHERE generation_id=? AND component_id=?", (generation, component_id))
        for component_id, row in delta["components"].items():
            old = snapshot["components"].get(component_id)
            membership_changed = {cid for cid, component in snapshot["members"].items() if component == component_id} != {
                cid for cid, component in delta["members"].items() if component == component_id}
            if old is None or old["state"] != "ready" or membership_changed or any(old[key] != row[key] for key in row):
                connection.execute("INSERT INTO duplicate_components VALUES(?,?,?,?,?,?,?) ON CONFLICT(generation_id,component_id) DO UPDATE SET "
                    "canonical_content_id=excluded.canonical_content_id,component_revision=excluded.component_revision,state='ready',"
                    "member_count=excluded.member_count,updated_at=excluded.updated_at",
                    (generation, component_id, row["canonical_content_id"], max(revision, int(old["component_revision"]) + 1 if old else 1), "ready", row["member_count"], now))
        for cid, component_id in delta["members"].items():
            if snapshot["members"].get(cid) != component_id:
                connection.execute("INSERT INTO duplicate_component_members VALUES(?,?,?)", (generation, cid, component_id))
        for rid in relation_deletes + text_deletes:
            connection.execute("DELETE FROM duplicate_relations WHERE id=?", (rid,))
        for pair, row in relation_writes.items():
            if pair in old_relations:
                connection.execute("UPDATE duplicate_relations SET confidence=?,evidence_json=?,status=? WHERE id=?",
                    (row["confidence"], row["evidence_json"], row["status"], old_relations[pair]["id"]))
            else:
                connection.execute("INSERT INTO duplicate_relations(duplicate_content_id,original_content_id,method,confidence,evidence_json,status,created_at) "
                    "VALUES(?,?,'fingerprint_v1',?,?,'confirmed',?)", (*pair, row["confidence"], row["evidence_json"], now))
        for cid, work in snapshot["works"].items():
            connection.execute("UPDATE duplicate_dirty_work SET status='ready',completed_input_revision=target_input_revision,completed_at=?,"
                "retry_at=NULL,last_error=NULL,lease_token=NULL,checkpoint_json='{}' WHERE generation_id=? AND content_id=? AND target_input_revision=?",
                (now, generation, cid, work["target_input_revision"]))
            connection.execute("DELETE FROM duplicate_work_staging WHERE generation_id=? AND work_content_id=?", (generation, cid))
        connection.execute("UPDATE duplicate_index_generations SET graph_revision=? WHERE generation_id=?", (revision, generation))
    return {"ready_content_ids": sorted(snapshot["works"]), "affected_content_ids": delta["affected_ids"],
            "confirmed_edges": len(delta["edges"]), "duplicate_groups": len(delta["components"]),
            "duplicate_relations": len(delta["projections"]), "inserted_fingerprint_relations": len(relation_writes),
            "deleted_fingerprint_relations": len(relation_deletes), "deleted_text_sha256_relations": len(text_deletes),
            "changed_rows": len(edge_deletes) + len(edge_writes) + len(relation_deletes) + len(relation_writes) + len(text_deletes),
            "committed_at": now_utc()}


def _result(connection, ids, *, error=None, publications=(), compared=0, elapsed=0.0, global_scope=False):
    from .duplicate_index import ready_status
    states = ready_status(connection, list(ids))
    results = [dict(states[cid], content_id=cid) for cid in ids]
    for row in results:
        row.setdefault("relation_status", row.get("status", "pending"))
        row.setdefault("retryable", row["relation_status"] == "pending")
        if error and not row.get("error_code") and row["relation_status"] != "ready":
            row["error_code"] = error
        row.setdefault("error_code", error)
    if error == "calibration_not_ready":
        for row in results:
            row.update(relation_status="pending", retryable=True, error_code=error)
    queue_pending = global_scope and connection.execute("SELECT 1 FROM duplicate_dirty_work w JOIN duplicate_index_generations g "
        "ON g.generation_id=w.generation_id LEFT JOIN duplicate_current_fingerprints p ON p.generation_id=w.generation_id AND p.content_id=w.content_id "
        "WHERE g.state='ready' AND (w.status<>'ready' OR p.input_status='unavailable') LIMIT 1").fetchone() is not None
    failed = [row for row in results if row["relation_status"] == "failed"]
    pending = [row for row in results if row["relation_status"] == "pending"]
    ready = [row["content_id"] for row in results if row["relation_status"] == "ready"]
    global_failed = global_pending = 0
    if global_scope:
        aggregate = connection.execute("SELECT SUM(w.status='failed'),SUM(w.status NOT IN ('ready','failed') OR (w.status='ready' AND p.input_status='unavailable')) "
            "FROM duplicate_dirty_work w JOIN duplicate_index_generations g ON g.generation_id=w.generation_id "
            "LEFT JOIN duplicate_current_fingerprints p ON p.generation_id=w.generation_id AND p.content_id=w.content_id WHERE g.state='ready'").fetchone()
        global_failed, global_pending = int(aggregate[0] or 0), int(aggregate[1] or 0)
    relation_status = "failed" if failed or global_failed else "pending" if pending or global_pending or error == "calibration_not_ready" else "ready"
    relations = None if relation_status != "ready" else {"seed_content_ids": list(ids), "compared_pairs": compared,
        "elapsed_seconds": elapsed, "committed_at": publications[-1]["committed_at"] if publications else None,
        **{key: sum(value.get(key, 0) for value in publications) for key in ("confirmed_edges", "duplicate_groups", "duplicate_relations",
            "inserted_fingerprint_relations", "deleted_fingerprint_relations", "deleted_text_sha256_relations", "changed_rows")}}
    active = _generation(connection)
    return {"generation_id": active["generation_id"] if active else None,
            "input_revision": results[0].get("input_revision") if len(results) == 1 else None,
            "relation_status": relation_status, "results": results, "processed": len(ready), "failed": max(len(failed), global_failed),
            "failures": failed, "pending": max(len(pending), global_pending), "calibration_ready": error != "calibration_not_ready",
            "relations": relations, "acknowledged_content_ids": sorted({cid for publication in publications for cid in publication["ready_content_ids"]}), "fingerprinted_content_ids": [row["content_id"] for row in results if row.get("fingerprint_available")], "has_more": bool(pending) or queue_pending,
            "retryable": bool(pending), "error_code": error, "compared_pairs": compared, "elapsed_seconds": elapsed}


def _record_failure(db_path, generation, works, token, error):
    conflict = isinstance(error, DuplicateConflict)
    invalid = isinstance(error, (ValueError, TypeError, KeyError))
    with connect(db_path) as connection, transaction_metrics_context(job_id="duplicate_graph_retry"), transaction(connection):
        _fence(connection, generation, token)
        for work in works:
            current = connection.execute("SELECT * FROM duplicate_dirty_work WHERE generation_id=? AND content_id=?", (generation, work["content_id"])).fetchone()
            if _work_identity(current) != _work_identity(work):
                continue
            attempts = int(current["attempt_count"]) + (0 if conflict else 1)
            terminal = invalid or attempts > 3
            delay = 60 if conflict else (60, 120, 600)[min(max(attempts - 1, 0), 2)]
            connection.execute("UPDATE duplicate_dirty_work SET status=?,attempt_count=?,retry_at=?,last_error=?,checkpoint_json='{}',lease_token=NULL "
                "WHERE generation_id=? AND content_id=? AND target_input_revision=?",
                ("failed" if terminal else "retryable", attempts, None if terminal else _after(delay),
                 f"{type(error).__name__}: {error}"[:1000], generation, work["content_id"], work["target_input_revision"]))
            connection.execute("DELETE FROM duplicate_work_staging WHERE generation_id=? AND work_content_id=? AND target_input_revision=?",
                               (generation, work["content_id"], work["target_input_revision"]))


def _drain_duplicate_work(*, limit: int = 20, time_budget_seconds: float = 5,
                         scope_content_ids: Sequence[int] | None = None, db_path: Path = DEFAULT_DB,
                         fallback_full_scan: bool | None = None) -> dict[str, Any]:
    """Consume indexed work only; never download, fingerprint, or call providers."""
    from .duplicates import FINGERPRINT_VERSION, THRESHOLDS
    from .duplicate_index_release import active_engine
    if fallback_full_scan is None:
        fallback_full_scan = active_engine() == "full_scan"
    started = time.monotonic()
    deadline = started + max(0.0, time_budget_seconds)
    limit = min(MAX_BATCH, max(0, limit))
    scope = None if scope_content_ids is None else sorted(set(map(int, scope_content_ids)))
    with _read(db_path) as connection:
        generation = _generation(connection)
        if generation is None:
            return {"relation_status": "pending", "results": [], "processed": 0, "failed": 0,
                    "failures": [], "pending": len(scope or []), "relations": None, "calibration_ready": False,
                    "fingerprinted_content_ids": [], "has_more": True, "retryable": True, "error_code": "generation_not_ready"}
        works = _eligible(connection, generation["generation_id"], scope, limit)
        ids = scope if scope is not None else [int(row["content_id"]) for row in works]
        calibration = connection.execute("SELECT status FROM duplicate_calibration_runs WHERE fingerprint_version=? AND thresholds_json=? "
            "ORDER BY created_at DESC,id DESC LIMIT 1", (FINGERPRINT_VERSION, graph.canonical_json(THRESHOLDS))).fetchone()
        if calibration is None or calibration[0] != "passed":
            return _result(connection, ids, error="calibration_not_ready", global_scope=scope is None)
        if not works or limit == 0 or time_budget_seconds <= 0:
            return _result(connection, ids, global_scope=scope is None)
        if generation["lease_token"] and generation["lease_until"] and generation["lease_until"] > now_utc():
            return _result(connection, ids, error="graph_worker_busy", global_scope=scope is None)
    generation_id = generation["generation_id"]
    token = _acquire(db_path, generation_id)
    if token is None:
        with _read(db_path) as connection:
            return _result(connection, ids, error="graph_worker_busy", global_scope=scope is None)
    publications, compared, error_code = [], 0, None
    cached_snapshot, cached_signature = None, None
    batch_ids = [int(row["content_id"]) for row in works]
    try:
        last_renewed = time.monotonic()
        conflicts = 0
        while True:
            try:
                with _read(db_path) as connection:
                    works = _eligible(connection, generation_id, batch_ids, limit)
                    current_generation = _generation(connection)
                    if current_generation is None or current_generation["generation_id"] != generation_id:
                        raise DuplicateConflict("active duplicate generation changed")
                    signature = (current_generation["index_revision"], tuple((work["content_id"], _work_identity(work)) for work in works))
                    if cached_snapshot is not None and signature == cached_signature:
                        snapshot = {**cached_snapshot, "works": works}
                    else:
                        snapshot = _match_snapshot(connection, generation_id, works, fallback_full_scan)
                if not works:
                    break
                if "prepared" not in snapshot:
                    from .duplicate_index import prepare_fingerprints
                    snapshot["prepared"] = prepare_fingerprints(snapshot["fingerprints"])
                computation = graph.compute_graph_delta(snapshot, works, deadline, max_pairs=graph.PAIR_CHUNK_SIZE)
                compared += computation["compared_pairs"]
                if time.monotonic() - last_renewed >= RENEW_SECONDS:
                    _renew(db_path, generation_id, token)
                    last_renewed = time.monotonic()
                _persist_computation(db_path, snapshot, computation, token)
                snapshot["prior"] = {cid: {other: {"input_snapshot_digest": result["input_snapshot_digest"], "comparison": value}
                    for other, value in result["records"].items()} for cid, result in computation["works"].items()}
                cached_snapshot, cached_signature = snapshot, signature
                if any(not result["complete"] for result in computation["works"].values()):
                    if time.monotonic() < deadline:
                        continue
                    break
                deltas, covered = [], set()
                with _read(db_path) as connection:
                    for cid, result in computation["works"].items():
                        if not result["complete"] or cid in covered:
                            continue
                        work = connection.execute("SELECT status FROM duplicate_dirty_work WHERE generation_id=? AND content_id=?", (generation_id, cid)).fetchone()
                        delta = _publication_snapshot(connection, generation_id, cid) if work and work[0] != "ready" else None
                        if delta:
                            covered.update(delta["snapshot"]["works"])
                            deltas.append(delta)
                if deltas:
                    delta = _merge_graph_deltas(deltas)
                    if time.monotonic() >= deadline and not delta.get("_staged"):
                        owner = next(cid for cid in batch_ids if cid in delta["snapshot"]["works"])
                        _stage_graph_delta(delta, owner, token, db_path)
                    else:
                        publications.append(commit_graph_delta(delta, token, db_path=db_path))
                if computation["budget_exhausted"] and time.monotonic() < deadline:
                    continue
                break
            except DuplicateConflict as error:
                cached_snapshot, cached_signature = None, None
                if error.work_ids:
                    try:
                        with connect(db_path) as connection, transaction(connection):
                            _fence(connection, generation_id, token)
                            for cid in error.work_ids:
                                connection.execute("DELETE FROM duplicate_work_staging WHERE generation_id=? AND work_content_id=?", (generation_id, cid))
                                connection.execute("UPDATE duplicate_dirty_work SET checkpoint_json='{}',status='pending',retry_at=NULL "
                                    "WHERE generation_id=? AND content_id=? AND status<>'ready'", (generation_id, cid))
                    except DuplicateConflict:
                        pass
                    error_code = "stale_comparison_checkpoint"
                    break
                conflicts += 1
                if conflicts <= 2 and time.monotonic() < deadline:
                    continue
                error_code = "source_or_lease_changed"
                try:
                    _record_failure(db_path, generation_id, works, token, error)
                except DuplicateConflict:
                    pass  # A replaced/expired owner may not mutate retry state.
                break
            except Exception as error:
                error_code = type(error).__name__
                try:
                    _record_failure(db_path, generation_id, works, token, error)
                except DuplicateConflict:
                    pass  # A replaced/expired owner may not mutate retry state.
                break
    finally:
        _release(db_path, generation_id, token)
    with _read(db_path) as connection:
        return _result(connection, ids, error=error_code, publications=publications,
                       compared=compared, elapsed=round(time.monotonic() - started, 6), global_scope=scope is None)


def drain_duplicate_work(*, limit: int = 20, time_budget_seconds: float = 5,
                         scope_content_ids: Sequence[int] | None = None, db_path: Path = DEFAULT_DB,
                         fallback_full_scan: bool | None = None) -> dict[str, Any]:
    """Coalesce in-process callers while retaining the cross-process DB lease.

    Waiting callers own no SQLite connection or write lock. Without this local
    coordinator, simultaneous HTTP callers continually reopen the large schema
    and poll the same lease while competing with the useful graph calculation.
    """
    started = time.monotonic()
    if limit <= 0 or time_budget_seconds <= 0:
        return _drain_duplicate_work(limit=0, time_budget_seconds=0, scope_content_ids=scope_content_ids,
            db_path=db_path, fallback_full_scan=fallback_full_scan)
    identity = Path(db_path).stat()
    key = (identity.st_dev, identity.st_ino)
    with _LOCAL_COORDINATORS_GUARD:
        coordinator = _LOCAL_COORDINATORS.get(key)
        if coordinator is None:
            coordinator = threading.Lock()
            _LOCAL_COORDINATORS[key] = coordinator
    acquired = coordinator.acquire(timeout=max(0.0, time_budget_seconds))
    waited = time.monotonic() - started
    try:
        result = _drain_duplicate_work(limit=limit if acquired else 0,
            time_budget_seconds=max(0.0, time_budget_seconds - waited), scope_content_ids=scope_content_ids,
            db_path=db_path, fallback_full_scan=fallback_full_scan)
    finally:
        if acquired:
            coordinator.release()
    result["coordination_wait_ms"] = round(waited * 1000, 3)
    result["elapsed_seconds"] = round(time.monotonic() - started, 6)
    return result
