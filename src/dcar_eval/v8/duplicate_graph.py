"""Pure duplicate comparison and connected-component projection for schema24.

This module never opens a database, reads media, or owns a writer lock. The
runtime supplies an immutable read snapshot and persists comparison checkpoints.
"""
from __future__ import annotations

import hashlib
import json
import time
from typing import Any, Mapping, Sequence


PAIR_CHUNK_SIZE = 512


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def pointer_identity(row: Mapping[str, Any] | None) -> tuple[Any, ...] | None:
    if row is None:
        return None
    return (row.get("fingerprint_id"), row.get("source_sha256"),
            row.get("input_revision"), row.get("input_status"))


def work_digest(seed: int, rows: Mapping[int, Mapping[str, Any]], candidates: Sequence[int],
                target_revision: int) -> str:
    value = {"seed": seed, "revision": target_revision,
             "inputs": [(cid, pointer_identity(rows.get(cid))) for cid in sorted({seed, *candidates})]}
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def compute_graph_delta(snapshot: Mapping[str, Any], work_items: Sequence[Mapping[str, Any]],
                        deadline: float | None = None, *, max_pairs: int | None = None) -> dict[str, Any]:
    """Compare a resumable batch; return unpublished records and completion flags.

    Input records are keyed by work id and validated snapshot digest. Results
    contain both negatives and positives: a persisted negative is real progress,
    not an invitation to repeat the same first 512 pairs after every restart.
    At least one chunk progresses; the caller checks its budget before calling.
    """
    from .duplicate_index import prepare_fingerprints, compare_prepared

    rows = snapshot["fingerprints"]
    prepared = snapshot.get("prepared")
    if prepared is None:
        prepared = prepare_fingerprints(list(rows.values()))
    if not isinstance(prepared, Mapping):
        prepared = {int(value.content_id): value for value in prepared}
    output: dict[int, dict[str, Any]] = {}
    cache: dict[tuple[int, int], dict[str, Any]] = {}
    for seed, values in snapshot.get("verified_comparisons", {}).items():
        for other, value in values.items():
            cache[(min(seed, other), max(seed, other))] = value
    advanced_pairs = set()
    compared = 0
    exhausted = False
    for work in work_items:
        cid = int(work["content_id"])
        candidates = sorted(set(snapshot["candidates"].get(cid, ())) - {cid})
        digest = snapshot["digests"][cid]
        prior = snapshot.get("prior", {}).get(cid, {})
        records: dict[int, dict[str, Any]] = {}
        for other, value in prior.items():
            other = int(other)
            if other in candidates and value.get("input_snapshot_digest") == digest:
                records[other] = value["comparison"]
                cache[(min(cid, other), max(cid, other))] = value["comparison"]
        remaining = [other for other in candidates if other not in records]
        new_records: dict[int, dict[str, Any]] = {}
        if not exhausted:
            for offset in range(0, len(remaining), PAIR_CHUNK_SIZE):
                for other in remaining[offset:offset + PAIR_CHUNK_SIZE]:
                    pair = (min(cid, other), max(cid, other))
                    result = cache.get(pair)
                    if result is None:
                        result = compare_prepared(prepared[pair[0]], prepared[pair[1]])
                        cache[pair] = result
                        compared += 1
                    records[other] = result
                    new_records[other] = result
                    advanced_pairs.add(pair)
                    if max_pairs is not None and len(advanced_pairs) >= max_pairs:
                        exhausted = True
                        break
                if exhausted:
                    break
                if deadline is not None and time.monotonic() >= deadline:
                    exhausted = True
                    break
        output[cid] = {"input_snapshot_digest": digest, "records": records,
                       "new_records": new_records, "complete": len(records) == len(candidates),
                       "candidate_count": len(candidates), "cursor": len(records)}
    return {"works": output, "compared_pairs": compared, "budget_exhausted": exhausted}


def build_component_delta(snapshot: Mapping[str, Any], replacements: Mapping[int, Mapping[int, Mapping[str, Any]]]) -> dict[str, Any]:
    """Replace changed endpoints' direct edges and rebuild only their old union.

    Old canonical stars are projection rows, never edges. All replacements must
    be complete before the runtime invokes this function; otherwise the old
    component remains dirty and no partial split is made visible.
    """
    from .duplicates import FINGERPRINT_VERSION, THRESHOLDS

    generation = snapshot["generation_id"]
    current = snapshot["pointers"]
    available = {cid for cid, row in current.items()
                 if row.get("input_status") == "available" and row.get("fingerprint_id") is not None}
    edges: dict[tuple[int, int], dict[str, Any]] = {}
    changed = set(replacements)
    for row in snapshot["edges"]:
        left, right = int(row["left_content_id"]), int(row["right_content_id"])
        if left not in available or right not in available or left in changed or right in changed:
            continue
        if any(row[f"{side}_fingerprint_id"] != current[cid]["fingerprint_id"]
               or row[f"{side}_input_revision"] != current[cid]["input_revision"]
               for side, cid in (("left", left), ("right", right))):
            continue
        edges[(left, right)] = dict(row)
    for seed, comparisons in replacements.items():
        if seed not in available:
            continue
        for other, result in comparisons.items():
            other = int(other)
            if not result["confirmed"] or other not in available:
                continue
            left, right = sorted((seed, other))
            edges[(left, right)] = {
                "generation_id": generation, "left_content_id": left, "right_content_id": right,
                "left_fingerprint_id": current[left]["fingerprint_id"],
                "right_fingerprint_id": current[right]["fingerprint_id"],
                "left_input_revision": current[left]["input_revision"],
                "right_input_revision": current[right]["input_revision"],
                "confidence": result["confidence"], "comparison_json": canonical_json(result),
            }
    adjacency = {cid: set() for cid in available}
    incident_edges = {cid: [] for cid in available}
    for (left, right), row in edges.items():
        adjacency[left].add(right)
        adjacency[right].add(left)
        incident_edges[left].append(row)
        incident_edges[right].append(row)
    groups = []
    unseen = set(available)
    for seed in sorted(available):
        if seed not in unseen:
            continue
        pending, members = [seed], set()
        while pending:
            cid = pending.pop()
            if cid in members:
                continue
            members.add(cid)
            pending.extend(adjacency[cid] - members)
        unseen.difference_update(members)
        if len(members) > 1:
            groups.append(sorted(members))
    components, memberships, projections = {}, {}, {}
    used_component_ids: set[str] = set()
    old_members = snapshot["members"]
    for group in groups:
        candidates = sorted({str(old_members[cid]) for cid in group if cid in old_members} - used_component_ids)
        component_id = candidates[0] if candidates else "cmp-" + hashlib.sha256(
            canonical_json([generation, group]).encode()).hexdigest()[:24]
        used_component_ids.add(component_id)
        canonical = min(group, key=lambda cid: (str(current[cid].get("published_at") or
                        current[cid].get("imported_at") or ""), cid))
        components[component_id] = {"canonical_content_id": canonical, "member_count": len(group)}
        memberships.update({cid: component_id for cid in group})
        for cid in group:
            if cid == canonical:
                continue
            best = min(incident_edges[cid], key=lambda row: (-float(row["confidence"]),
                       int(row["left_content_id"]), int(row["right_content_id"])))
            comparison = json.loads(best["comparison_json"])
            evidence = {"evidence_schema_version": "duplicate-relation-evidence-v2",
                        "generation_id": generation, "fingerprint_version": FINGERPRINT_VERSION,
                        "thresholds": THRESHOLDS,
                        "best_edge": {"left": best["left_content_id"], "right": best["right_content_id"], **comparison}}
            projections[(cid, canonical)] = {"duplicate_content_id": cid, "original_content_id": canonical,
                "confidence": float(best["confidence"]), "evidence_json": canonical_json(evidence), "status": "confirmed"}
    return {"generation_id": generation, "edges": edges, "components": components,
            "members": memberships, "projections": projections, "affected_ids": sorted(current),
            "compared_pairs": sum(len(value) for value in replacements.values())}
