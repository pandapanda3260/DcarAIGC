"""Persistent duplicate candidates; no media work, network, or implicit writes.

All mutation helpers use the caller's transaction. Current-source identity is
prepared from the authoritative media/content inputs by the caller, never from
the timestamp of a historical fingerprint.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

INDEX_CONTRACT_VERSION = "sqlite-phash64-mih4x16-r6-v1"
SQL_BATCH_SIZE = 400


class DuplicateIndexError(ValueError):
    pass


class DuplicateIndexUnavailable(DuplicateIndexError):
    pass


class DuplicateInputChanged(DuplicateIndexError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _chunks(values: Iterable[Any], size: int = SQL_BATCH_SIZE):
    values = list(values)
    for start in range(0, len(values), size):
        yield values[start:start + size]


def _dicts(cursor: sqlite3.Cursor) -> list[dict[str, Any]]:
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in cursor]


def _one(cursor: sqlite3.Cursor) -> dict[str, Any] | None:
    rows = _dicts(cursor)
    return rows[0] if rows else None


def _require_transaction(connection: sqlite3.Connection) -> None:
    if not connection.in_transaction:
        raise DuplicateIndexError("duplicate index mutation requires caller transaction")


def active_generation(connection: sqlite3.Connection) -> dict[str, Any] | None:
    if not connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='duplicate_index_generations'").fetchone():
        return None
    return _one(connection.execute("SELECT * FROM duplicate_index_generations WHERE state='ready'"))


def _generation(connection: sqlite3.Connection, generation_id: str | None) -> dict[str, Any]:
    generation = active_generation(connection) if generation_id is None else _one(connection.execute(
        "SELECT * FROM duplicate_index_generations WHERE generation_id=? AND state IN ('building','ready')", (generation_id,)))
    if generation is None:
        raise DuplicateIndexUnavailable("duplicate index generation is not ready")
    if generation["index_contract_version"] != INDEX_CONTRACT_VERSION:
        raise DuplicateIndexUnavailable("duplicate index contract version differs")
    from .duplicates import FINGERPRINT_VERSION, THRESHOLDS
    if generation["fingerprint_version"] != FINGERPRINT_VERSION or generation["rule_digest"] != hashlib.sha256(_json(THRESHOLDS).encode()).hexdigest():
        raise DuplicateIndexUnavailable("duplicate fingerprint or comparison rule version differs")
    return generation


def create_generation(connection: sqlite3.Connection, *, generation_id: str | None = None,
                      fingerprint_version: str | None = None, rule_digest: str | None = None,
                      state: str = "building") -> dict[str, Any]:
    from .duplicates import FINGERPRINT_VERSION, THRESHOLDS
    if state != "building":
        raise DuplicateIndexError("new generation must be built and verified before activation")
    fingerprint_version = fingerprint_version or FINGERPRINT_VERSION
    rule_digest = rule_digest or hashlib.sha256(_json(THRESHOLDS).encode()).hexdigest()
    generation_id = generation_id or hashlib.sha256(_json({"fingerprint_version": fingerprint_version,
        "rule_digest": rule_digest, "index_contract_version": INDEX_CONTRACT_VERSION}).encode()).hexdigest()
    existing = _one(connection.execute("SELECT * FROM duplicate_index_generations WHERE generation_id=?", (generation_id,)))
    if existing:
        if (existing["fingerprint_version"], existing["rule_digest"], existing["index_contract_version"]) != (
            fingerprint_version, rule_digest, INDEX_CONTRACT_VERSION):
            raise DuplicateIndexError("generation identity collision")
        return existing
    connection.execute("INSERT INTO duplicate_index_generations(generation_id,fingerprint_version,rule_digest,index_contract_version,state,created_at) VALUES(?,?,?,?,?,?)",
        (generation_id, fingerprint_version, rule_digest, INDEX_CONTRACT_VERSION, state, _now()))
    return _one(connection.execute("SELECT * FROM duplicate_index_generations WHERE generation_id=?", (generation_id,)))  # type: ignore[return-value]


def phash_to_blob(value: str) -> bytes:
    if not isinstance(value, str) or not value:
        raise DuplicateIndexError("pHash must be a nonempty hexadecimal string")
    try:
        integer = int(value, 16)
    except ValueError as exc:
        raise DuplicateIndexError("invalid pHash hexadecimal value") from exc
    if not 0 <= integer < (1 << 64):
        raise DuplicateIndexError("pHash must fit unsigned 64 bits")
    return integer.to_bytes(8, "big")


def phash_bands(value: bytes) -> tuple[int, int, int, int]:
    if not isinstance(value, bytes) or len(value) != 8:
        raise DuplicateIndexError("pHash must be an 8 byte BLOB")
    return tuple(int.from_bytes(value[offset:offset + 2], "big") for offset in (0, 2, 4, 6))  # type: ignore[return-value]


def band_neighbors(value: int) -> tuple[int, ...]:
    if not 0 <= value <= 65535:
        raise DuplicateIndexError("band outside 16 bits")
    return (value, *(value ^ (1 << bit) for bit in range(16)))


def _array(value: Any, name: str) -> list[str]:
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError) as exc:
        raise DuplicateIndexError("invalid fingerprint " + name) from exc
    if not isinstance(parsed, list) or any(not isinstance(item, str) for item in parsed):
        raise DuplicateIndexError("fingerprint " + name + " must contain strings")
    return parsed


@dataclass(frozen=True)
class PreparedFingerprint:
    content_id: int
    fingerprint_id: int | None
    input_revision: int
    source_sha256: str | None
    fingerprint_version: str | None
    text_sha256: str | None
    media_sha256: tuple[str, ...]
    frame_phashes: tuple[str, ...]
    frame_values: tuple[int, ...]
    text_simhash: int | None
    asr_simhash: int | None
    ocr_simhash: int | None
    published_at: str | None = None
    imported_at: str | None = None


def prepare_fingerprint(row: Mapping[str, Any]) -> PreparedFingerprint:
    row = dict(row)
    frames = tuple(_array(row["frame_phashes_json"], "frame_phashes_json"))
    simhashes = [int(str(row[name]), 16) if row.get(name) else None for name in ("text_simhash", "asr_simhash", "ocr_simhash")]
    return PreparedFingerprint(content_id=int(row.get("content_id", 0)),
        fingerprint_id=row.get("fingerprint_id", row.get("id")), input_revision=int(row.get("input_revision", 0)),
        source_sha256=row.get("source_sha256"), fingerprint_version=row.get("fingerprint_version"),
        text_sha256=row.get("text_sha256"), media_sha256=tuple(_array(row["media_sha256_json"], "media_sha256_json")),
        frame_phashes=frames, frame_values=tuple(int.from_bytes(phash_to_blob(value), "big") for value in frames),
        text_simhash=simhashes[0], asr_simhash=simhashes[1], ocr_simhash=simhashes[2],
        published_at=row.get("published_at"), imported_at=row.get("imported_at"))


def prepare_fingerprints(rows: Iterable[Mapping[str, Any]] | Mapping[int, Mapping[str, Any]]) -> dict[int, PreparedFingerprint]:
    values = rows.values() if isinstance(rows, Mapping) else rows
    prepared = [prepare_fingerprint(row) for row in values]
    return {row.content_id: row for row in prepared}


def compare_prepared(left: PreparedFingerprint, right: PreparedFingerprint) -> dict[str, Any]:
    """The original comparison contract, including repeat weighting and zero confidence behavior."""
    from .duplicates import THRESHOLDS
    distance = None
    count = 0
    if left.frame_values and right.frame_values:
        left_nearest = [min((value ^ other).bit_count() for other in right.frame_values) for value in left.frame_values]
        right_nearest = [min((value ^ other).bit_count() for other in left.frame_values) for value in right.frame_values]
        distance = round((sum(left_nearest) / len(left_nearest) + sum(right_nearest) / len(right_nearest)) / 2, 6)
        count = min(len(left.frame_values), len(right.frame_values))
    similarities = {}
    for name in ("text", "asr", "ocr"):
        a, b = getattr(left, name + "_simhash"), getattr(right, name + "_simhash")
        similarities[name] = None if a is None or b is None else round(1.0 - (a ^ b).bit_count() / 64.0, 6)
    semantic_max = max((value for value in similarities.values() if value is not None), default=0.0)
    exact_media = bool(set(left.media_sha256) & set(right.media_sha256))
    exact_text = bool(left.text_sha256 and left.text_sha256 == right.text_sha256)
    diverse = len(set(left.frame_phashes)) >= 2 and len(set(right.frame_phashes)) >= 2
    strong = bool(distance is not None and count >= 3 and diverse and distance <= THRESHOLDS["phash_strong_distance"])
    semantic = bool(distance is not None and count >= 2 and distance <= THRESHOLDS["phash_confirm_distance"] and semantic_max >= THRESHOLDS["visual_semantic_min"])
    reasons = [name for name, matched in (("media_sha256", exact_media), ("text_sha256", exact_text),
        ("phash_strong", strong), ("phash_plus_semantic", semantic)) if matched]
    confidence = 1.0 if exact_media or exact_text else max(
        1.0 - (distance or 64.0) / 64.0 if distance is not None else 0.0, semantic_max)
    return {"confirmed": bool(reasons), "confidence": round(confidence, 6), "reasons": reasons,
        "exact_media": exact_media, "exact_text": exact_text, "phash_distance": distance,
        "phash_match_count": count, "similarities": similarities}


def read_current_fingerprints(connection: sqlite3.Connection, ids: Iterable[int] | None = None, *,
                              generation_id: str | None = None) -> dict[int, dict[str, Any]]:
    generation = _generation(connection, generation_id)
    columns = """f.id,f.content_id,f.fingerprint_version,f.source_sha256,f.text_sha256,
        f.media_sha256_json,f.frame_phashes_json,f.text_simhash,f.asr_simhash,f.ocr_simhash,
        f.text_char_count,f.asr_char_count,f.ocr_char_count,p.fingerprint_id,p.input_revision,
        p.generation_id,p.input_status,c.link_id,c.published_at,c.imported_at"""
    base = f"""SELECT {columns} FROM duplicate_current_fingerprints p
        JOIN duplicate_fingerprints f ON f.id=p.fingerprint_id
        JOIN content_items c ON c.id=p.content_id
        WHERE p.generation_id=? AND p.input_status='available'
        AND f.content_id=p.content_id AND f.source_sha256=p.source_sha256 AND f.fingerprint_version=?"""
    result = {}
    chunks = [None] if ids is None else _chunks(sorted(set(int(value) for value in ids)))
    for chunk in chunks:
        sql, params = base, [generation["generation_id"], generation["fingerprint_version"]]
        if chunk is not None:
            sql += " AND p.content_id IN (" + ",".join("?" for _ in chunk) + ")"
            params.extend(chunk)
        for row in _dicts(connection.execute(sql, params)):
            result[int(row["content_id"])] = row
    return result


def query_candidate_ids(snapshot: sqlite3.Connection, seed_ids: Iterable[int], *,
                        generation_id: str | None = None, fallback_full_scan: bool = False) -> dict[int, set[int]]:
    """Complete candidate union from one caller-owned SQLite read snapshot."""
    generation = _generation(snapshot, generation_id)
    gid = generation["generation_id"]
    seeds = sorted(set(int(value) for value in seed_ids))
    result = {seed: set() for seed in seeds}
    if fallback_full_scan:
        current = {int(row[0]) for row in snapshot.execute("SELECT content_id FROM duplicate_current_fingerprints WHERE generation_id=? AND input_status='available'", (gid,))}
        return {seed: current - {seed} if seed in current else set() for seed in seeds}
    text_seeds: dict[str, set[int]] = defaultdict(set)
    media_seeds: dict[str, set[int]] = defaultdict(set)
    bands: list[dict[int, set[tuple[int, int]]]] = [defaultdict(set) for _ in range(4)]
    for chunk in _chunks(seeds):
        marks = ",".join("?" for _ in chunk)
        params = [gid, *chunk]
        for seed, token in snapshot.execute(f"SELECT content_id,text_sha256 FROM duplicate_current_fingerprints WHERE generation_id=? AND input_status='available' AND content_id IN ({marks})", params):
            if token:
                text_seeds[token].add(int(seed))
        for seed, token in snapshot.execute(f"""SELECT p.content_id,m.media_sha256 FROM duplicate_current_fingerprints p
            JOIN duplicate_fingerprint_media m ON m.generation_id=p.generation_id AND m.fingerprint_id=p.fingerprint_id
            WHERE p.generation_id=? AND p.input_status='available' AND p.content_id IN ({marks})""", params):
            media_seeds[token].add(int(seed))
        for seed, blob in snapshot.execute(f"""SELECT p.content_id,f.phash FROM duplicate_current_fingerprints p
            JOIN duplicate_fingerprint_frames f ON f.generation_id=p.generation_id AND f.fingerprint_id=p.fingerprint_id
            WHERE p.generation_id=? AND p.input_status='available' AND p.content_id IN ({marks})""", params):
            integer = int.from_bytes(blob, "big")
            for band_no, value in enumerate(phash_bands(blob)):
                for neighbor in band_neighbors(value):
                    bands[band_no][neighbor].add((int(seed), integer))
    for chunk in _chunks(text_seeds):
        marks = ",".join("?" for _ in chunk)
        for candidate, token in snapshot.execute(f"""SELECT content_id,text_sha256 FROM duplicate_current_fingerprints
            WHERE generation_id=? AND text_sha256 IS NOT NULL AND text_sha256<>'' AND text_sha256 IN ({marks})
            AND input_status='available'""", [gid, *chunk]):
            for seed in text_seeds[token]:
                result[seed].add(int(candidate))
    for chunk in _chunks(media_seeds):
        marks = ",".join("?" for _ in chunk)
        for candidate, token in snapshot.execute(f"""SELECT p.content_id,m.media_sha256 FROM duplicate_fingerprint_media m
            JOIN duplicate_current_fingerprints p ON p.generation_id=m.generation_id AND p.fingerprint_id=m.fingerprint_id
            WHERE m.generation_id=? AND m.media_sha256 IN ({marks}) AND p.input_status='available'""", [gid, *chunk]):
            for seed in media_seeds[token]:
                result[seed].add(int(candidate))
    for band_no, query_values in enumerate(bands):
        for chunk in _chunks(query_values):
            marks = ",".join("?" for _ in chunk)
            for candidate, blob, value in snapshot.execute(f"""SELECT p.content_id,f.phash,f.band{band_no}
                FROM duplicate_fingerprint_frames f INDEXED BY idx_duplicate_frame_band{band_no}
                JOIN duplicate_current_fingerprints p ON p.generation_id=f.generation_id AND p.fingerprint_id=f.fingerprint_id
                WHERE f.generation_id=? AND f.band{band_no} IN ({marks}) AND p.input_status='available'""", [gid, *chunk]):
                integer = int.from_bytes(blob, "big")
                for seed, query in query_values[value]:
                    if (integer ^ query).bit_count() <= 6:
                        result[seed].add(int(candidate))
    for seed in result:
        result[seed].discard(seed)
    return result


def _old_component(connection: sqlite3.Connection, gid: str, content_id: int) -> str | None:
    row = connection.execute("SELECT component_id FROM duplicate_component_members WHERE generation_id=? AND content_id=?", (gid, content_id)).fetchone()
    if row:
        return str(row[0])
    row = connection.execute("SELECT old_component_id FROM duplicate_dirty_work WHERE generation_id=? AND content_id=?", (gid, content_id)).fetchone()
    return row[0] if row else None


def _enqueue(connection: sqlite3.Connection, gid: str, content_id: int, fingerprint_id: int | None,
             revision: int, reason: str, old_component: str | None) -> None:
    connection.execute("""INSERT INTO duplicate_dirty_work(generation_id,content_id,target_fingerprint_id,
        target_input_revision,old_component_id,reason,status) VALUES(?,?,?,?,?,?,'pending')
        ON CONFLICT(generation_id,content_id) DO UPDATE SET target_fingerprint_id=excluded.target_fingerprint_id,
        target_input_revision=excluded.target_input_revision,old_component_id=COALESCE(excluded.old_component_id,duplicate_dirty_work.old_component_id),
        reason=excluded.reason,status='pending',attempt_count=0,retry_at=NULL,lease_token=NULL,checkpoint_json='{}',
        last_error=NULL,completed_input_revision=NULL,completed_at=NULL
        WHERE excluded.target_input_revision>duplicate_dirty_work.target_input_revision""",
        (gid, content_id, fingerprint_id, revision, old_component, reason))
    if old_component:
        connection.execute("UPDATE duplicate_components SET state='dirty',component_revision=component_revision+1,updated_at=? WHERE generation_id=? AND component_id=?",
            (_now(), gid, old_component))


def _next_revision(connection: sqlite3.Connection, gid: str, content_id: int, pointer: Mapping[str, Any] | None) -> int:
    work = connection.execute("SELECT target_input_revision FROM duplicate_dirty_work WHERE generation_id=? AND content_id=?", (gid, content_id)).fetchone()
    return max(int(pointer["input_revision"]) if pointer else 0, int(work[0]) if work else 0) + 1


def index_fingerprint(connection: sqlite3.Connection, *, content_id: int, fingerprint_id: int,
                      source_sha256: str, generation_id: str | None = None,
                      expected_input_revision: int | None = None) -> dict[str, Any]:
    """Atomically publish postings/current/dirty after caller validates live source.

    ``source_sha256`` must come from current source preparation and its authority
    must be revalidated by the caller in this transaction. The revision CAS
    rejects work prepared before an intervening invalidation.
    """
    _require_transaction(connection)
    generation = _generation(connection, generation_id)
    gid = generation["generation_id"]
    raw = _one(connection.execute("SELECT * FROM duplicate_fingerprints WHERE id=?", (fingerprint_id,)))
    if raw is None or (raw["content_id"], raw["source_sha256"], raw["fingerprint_version"]) != (content_id, source_sha256, generation["fingerprint_version"]):
        raise DuplicateInputChanged("fingerprint source/content/version binding differs")
    pointer = _one(connection.execute("SELECT * FROM duplicate_current_fingerprints WHERE generation_id=? AND content_id=?", (gid, content_id)))
    revision = int(pointer["input_revision"]) if pointer else 0
    if expected_input_revision is not None and revision != expected_input_revision:
        raise DuplicateInputChanged("fingerprint input revision changed")
    prepared = prepare_fingerprint(raw)
    if pointer and pointer["input_status"] == "available" and pointer["fingerprint_id"] == fingerprint_id and pointer["source_sha256"] == source_sha256:
        return {"generation_id": gid, "input_revision": revision, "fingerprint_id": fingerprint_id, "changed": False}
    # Parse and validate the entire immutable payload before the first posting.
    media_rows = [(gid, fingerprint_id, ordinal, token) for ordinal, token in enumerate(prepared.media_sha256)]
    frame_rows = [(gid, fingerprint_id, ordinal, phash_to_blob(token), *phash_bands(phash_to_blob(token))) for ordinal, token in enumerate(prepared.frame_phashes)]
    old_media = [tuple(row) for row in connection.execute("SELECT generation_id,fingerprint_id,media_ordinal,media_sha256 FROM duplicate_fingerprint_media WHERE generation_id=? AND fingerprint_id=? ORDER BY media_ordinal", (gid, fingerprint_id))]
    old_frames = [tuple(row) for row in connection.execute("SELECT generation_id,fingerprint_id,frame_ordinal,phash,band0,band1,band2,band3 FROM duplicate_fingerprint_frames WHERE generation_id=? AND fingerprint_id=? ORDER BY frame_ordinal", (gid, fingerprint_id))]
    if (old_media and old_media != media_rows) or (old_frames and old_frames != frame_rows):
        raise DuplicateIndexError("existing fingerprint postings differ from immutable source")
    if not old_media:
        connection.executemany("INSERT INTO duplicate_fingerprint_media VALUES(?,?,?,?)", media_rows)
    if not old_frames:
        connection.executemany("INSERT INTO duplicate_fingerprint_frames VALUES(?,?,?,?,?,?,?,?)", frame_rows)
    revision = _next_revision(connection, gid, content_id, pointer)
    old_component = _old_component(connection, gid, content_id)
    connection.execute("""INSERT INTO duplicate_current_fingerprints(generation_id,content_id,fingerprint_id,source_sha256,
        input_revision,text_sha256,input_status,activated_at) VALUES(?,?,?,?,?,?,'available',?)
        ON CONFLICT(generation_id,content_id) DO UPDATE SET fingerprint_id=excluded.fingerprint_id,
        source_sha256=excluded.source_sha256,input_revision=excluded.input_revision,text_sha256=excluded.text_sha256,
        input_status='available',activated_at=excluded.activated_at""",
        (gid, content_id, fingerprint_id, source_sha256, revision, prepared.text_sha256, _now()))
    _enqueue(connection, gid, content_id, fingerprint_id, revision, "fingerprint_available", old_component)
    connection.execute("UPDATE duplicate_index_generations SET index_revision=index_revision+1 WHERE generation_id=?", (gid,))
    return {"generation_id": gid, "input_revision": revision, "fingerprint_id": fingerprint_id, "changed": True}


def mark_content_dirty(connection: sqlite3.Connection, content_id: int, *, reason: str = "canonical_changed",
                       generation_id: str | None = None) -> dict[str, Any]:
    return invalidate_content(connection, content_id, reason=reason, source_changed=False, generation_id=generation_id)


def invalidate_content(connection: sqlite3.Connection, content_id: int, *, reason: str = "source_changed",
                       source_sha256: str | None = None, deleted: bool = False, source_changed: bool = True,
                       generation_id: str | None = None) -> dict[str, Any]:
    """Call before authority edits/deletion; tombstones retain the old graph scope."""
    generation = active_generation(connection) if generation_id is None else _generation(connection, generation_id)
    if generation is None:
        return {"changed": False, "reason": "generation_unavailable"}
    _require_transaction(connection)
    gid = generation["generation_id"]
    pointer = _one(connection.execute("SELECT * FROM duplicate_current_fingerprints WHERE generation_id=? AND content_id=?", (gid, content_id)))
    old_component = _old_component(connection, gid, content_id)
    revision = _next_revision(connection, gid, content_id, pointer)
    fingerprint_id = pointer["fingerprint_id"] if pointer and not source_changed and not deleted else None
    old_work = _one(connection.execute("SELECT * FROM duplicate_dirty_work WHERE generation_id=? AND content_id=?", (gid, content_id)))
    reuse_verified_edges = bool(fingerprint_id is not None and pointer["input_status"] == "available" and old_work
        and ((old_work["status"] == "ready" and old_work["completed_input_revision"] == pointer["input_revision"]
              and old_work["target_input_revision"] == pointer["input_revision"])
             or json.loads(old_work["checkpoint_json"]).get("reuse_verified_edges") is True))
    _enqueue(connection, gid, content_id, fingerprint_id, revision, reason, old_component)
    if reuse_verified_edges:
        connection.execute("UPDATE duplicate_dirty_work SET checkpoint_json=? WHERE generation_id=? AND content_id=?",
            (_json({"reuse_verified_edges": True}), gid, content_id))
    if deleted:
        connection.execute("DELETE FROM duplicate_current_fingerprints WHERE generation_id=? AND content_id=?", (gid, content_id))
    elif pointer and not source_changed:
        connection.execute("UPDATE duplicate_current_fingerprints SET input_revision=?,activated_at=? WHERE generation_id=? AND content_id=?", (revision, _now(), gid, content_id))
    else:
        if not connection.execute("SELECT 1 FROM content_items WHERE id=?", (content_id,)).fetchone():
            raise DuplicateInputChanged("content does not exist; deletion requires tombstone")
        connection.execute("""INSERT INTO duplicate_current_fingerprints(generation_id,content_id,fingerprint_id,source_sha256,
            input_revision,text_sha256,input_status,activated_at) VALUES(?,?,NULL,?,?,NULL,'unavailable',?)
            ON CONFLICT(generation_id,content_id) DO UPDATE SET fingerprint_id=NULL,source_sha256=excluded.source_sha256,
            input_revision=excluded.input_revision,text_sha256=NULL,input_status='unavailable',activated_at=excluded.activated_at""",
            (gid, content_id, source_sha256, revision, _now()))
    connection.execute("UPDATE duplicate_index_generations SET index_revision=index_revision+1 WHERE generation_id=?", (gid,))
    return {"generation_id": gid, "input_revision": revision, "fingerprint_id": fingerprint_id,
        "old_component_id": old_component, "changed": True}


def ready_status(connection: sqlite3.Connection, content_ids: Iterable[int], *, generation_id: str | None = None) -> dict[int, dict[str, Any]]:
    ids = sorted(set(int(value) for value in content_ids))
    generation = active_generation(connection) if generation_id is None else _generation(connection, generation_id)
    result = {ident: {"relation_status": "pending", "generation_id": generation["generation_id"] if generation else None,
        "input_revision": None, "fingerprint_available": False, "source_sha256": None,
        "error_code": None if generation else "generation_not_ready", "retryable": True} for ident in ids}
    if generation is None:
        return result
    try:
        _generation(connection, generation["generation_id"])
    except DuplicateIndexUnavailable:
        for status in result.values():
            status["error_code"] = "generation_contract_changed"
        return result
    for chunk in _chunks(ids):
        marks = ",".join("(?)" for _ in chunk)
        for row in _dicts(connection.execute(f"""WITH requested(content_id) AS (VALUES {marks})
            SELECT r.content_id,p.input_revision,p.input_status,p.source_sha256,w.status,w.last_error,
            w.target_input_revision,w.completed_input_revision,w.target_fingerprint_id,w.reason,
            co.state AS component_state,c.id AS existing_content_id,
            EXISTS(SELECT 1 FROM content_identity_merge_events e
              JOIN content_aliases alias ON alias.alias_link_id=c.link_id AND alias.content_id=e.winner_content_id
              JOIN duplicate_relations relation ON relation.duplicate_content_id=e.loser_content_id
                AND relation.original_content_id=e.winner_content_id AND relation.method='identity_merge'
                AND relation.status='confirmed'
              WHERE e.loser_content_id=r.content_id
                AND json_type(CASE WHEN json_valid(relation.evidence_json) THEN relation.evidence_json ELSE '{{}}' END,
                              '$.merge_event_id')='integer'
                AND json_extract(CASE WHEN json_valid(relation.evidence_json) THEN relation.evidence_json ELSE '{{}}' END,
                                 '$.merge_event_id')=e.id) AS identity_merge_proven
            FROM requested r LEFT JOIN duplicate_current_fingerprints p
            ON p.content_id=r.content_id AND p.generation_id=? LEFT JOIN duplicate_dirty_work w
            ON w.generation_id=? AND w.content_id=r.content_id
            LEFT JOIN content_items c ON c.id=r.content_id
            LEFT JOIN duplicate_component_members m ON m.generation_id=? AND m.content_id=r.content_id
            LEFT JOIN duplicate_components co ON co.generation_id=m.generation_id AND co.component_id=m.component_id
            """, [*chunk, generation["generation_id"], generation["generation_id"], generation["generation_id"]])):
            available = row["input_status"] == "available"
            ready = available and row["status"] == "ready" and row["target_input_revision"] == row["input_revision"] == row["completed_input_revision"] and row["component_state"] != "dirty"
            deleted_or_merged = (row["existing_content_id"] is None
                or (row["reason"] == "content_merged" and row["identity_merge_proven"]))
            deleted_ready = (deleted_or_merged and row["input_revision"] is None
                and row["target_fingerprint_id"] is None and row["status"] == "ready"
                and row["completed_input_revision"] == row["target_input_revision"]
                and row["component_state"] != "dirty")
            status = "ready" if ready or deleted_ready else "failed" if row["status"] == "failed" else "pending"
            result[int(row["content_id"])].update(input_revision=row["input_revision"] or row["target_input_revision"],
                fingerprint_available=available, source_sha256=row["source_sha256"], relation_status=status,
                error_code=row["last_error"], retryable=status == "pending")
    return result


def source_current(connection: sqlite3.Connection, content_id: int, *, fingerprint_version: str | None = None) -> dict[str, Any]:
    """Read real media/content authority; use outside a write transaction during preparation."""
    from .duplicates import FINGERPRINT_VERSION, _current_source_state
    _, source_sha256 = _current_source_state(connection, content_id)
    version = fingerprint_version or FINGERPRINT_VERSION
    row = _one(connection.execute("SELECT id FROM duplicate_fingerprints WHERE content_id=? AND fingerprint_version=? AND source_sha256=?", (content_id, version, source_sha256)))
    return {"content_id": content_id, "source_sha256": source_sha256,
        "fingerprint_id": row["id"] if row else None, "input_status": "available" if row else "unavailable"}


def validate_postings(connection: sqlite3.Connection, *, generation_id: str | None = None) -> dict[str, Any]:
    """Full ordinal/value validation for every currently available fingerprint."""
    generation = _generation(connection, generation_id)
    gid = generation["generation_id"]
    current = read_current_fingerprints(connection, generation_id=gid)
    media_count = frame_count = 0
    for content_id, raw in current.items():
        prepared = prepare_fingerprint(raw)
        fid = prepared.fingerprint_id
        media = [tuple(row) for row in connection.execute("SELECT media_ordinal,media_sha256 FROM duplicate_fingerprint_media WHERE generation_id=? AND fingerprint_id=? ORDER BY media_ordinal", (gid, fid))]
        expected_media = list(enumerate(prepared.media_sha256))
        frames = [tuple(row) for row in connection.execute("SELECT frame_ordinal,phash,band0,band1,band2,band3 FROM duplicate_fingerprint_frames WHERE generation_id=? AND fingerprint_id=? ORDER BY frame_ordinal", (gid, fid))]
        expected_frames = [(ordinal, phash_to_blob(value), *phash_bands(phash_to_blob(value))) for ordinal, value in enumerate(prepared.frame_phashes)]
        if media != expected_media or frames != expected_frames:
            raise DuplicateIndexError(f"fingerprint posting mismatch for content {content_id}")
        media_count += len(media)
        frame_count += len(frames)
    available_count = connection.execute("SELECT count(*) FROM duplicate_current_fingerprints WHERE generation_id=? AND input_status='available'", (gid,)).fetchone()[0]
    if available_count != len(current):
        raise DuplicateIndexError("current fingerprint binding coverage mismatch")
    return {"generation_id": gid, "current_fingerprints": len(current), "media_postings": media_count,
        "frame_postings": frame_count, "all_current_postings_verified": True}
