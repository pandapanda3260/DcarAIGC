"""Read-side contract for indexed fingerprints and published duplicate results.

An absent relation is a negative result only after its current work was ACKed.
These helpers never open a connection or mutate a database.
"""
from __future__ import annotations

import re
import sqlite3
from typing import Any, Sequence


def indexed_duplicates(connection: sqlite3.Connection) -> bool:
    return int(connection.execute("PRAGMA user_version").fetchone()[0]) == 24


def current_fingerprint_sql(connection: sqlite3.Connection, content_expression: str,
                            *, cutoff_at: str | None = None) -> str:
    if not indexed_duplicates(connection):
        return "1=1"
    cutoff = (" AND julianday(cf.created_at)<=julianday('" + cutoff_at.replace("'", "''") + "')"
              if cutoff_at is not None else "")
    return f"""EXISTS (SELECT 1 FROM duplicate_current_fingerprints cp
        JOIN duplicate_index_generations cg ON cg.generation_id=cp.generation_id
        JOIN duplicate_fingerprints cf ON cf.id=cp.fingerprint_id
        WHERE cg.state='ready' AND cp.content_id={content_expression}
          AND cp.input_status='available' AND cf.source_sha256=cp.source_sha256{cutoff})"""


def relation_ready_sql(connection: sqlite3.Connection, content_expression: str,
                       *, cutoff_at: str | None = None) -> str:
    """SQL for a trusted content-id expression, including isolated negative ACKs."""
    if not indexed_duplicates(connection):
        return "1=1"
    cutoff = (" AND julianday(dw.completed_at)<=julianday('" + cutoff_at.replace("'", "''") + "')"
              if cutoff_at is not None else "")
    return f"""EXISTS (
        SELECT 1 FROM duplicate_index_generations dg
        JOIN duplicate_current_fingerprints dp ON dp.generation_id=dg.generation_id
        JOIN duplicate_dirty_work dw ON dw.generation_id=dp.generation_id AND dw.content_id=dp.content_id
        LEFT JOIN duplicate_component_members dm ON dm.generation_id=dp.generation_id AND dm.content_id=dp.content_id
        LEFT JOIN duplicate_components dc ON dc.generation_id=dm.generation_id AND dc.component_id=dm.component_id
        WHERE dg.state='ready' AND dp.content_id={content_expression}
          AND dp.input_status='available' AND dp.fingerprint_id IS NOT NULL
          AND dw.status='ready' AND dw.target_input_revision=dp.input_revision
          AND dw.completed_input_revision=dp.input_revision
          AND dw.target_fingerprint_id=dp.fingerprint_id
          AND (dm.component_id IS NULL OR dc.state='ready'){cutoff})"""


def valid_relation_sql(connection: sqlite3.Connection, alias: str = "d",
                       *, cutoff_at: str | None = None) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", alias):
        raise ValueError("relation SQL alias must be an identifier")
    if not indexed_duplicates(connection):
        return "1=1"
    ready = relation_ready_sql(connection, f"{alias}.duplicate_content_id", cutoff_at=cutoff_at)
    original_ready = relation_ready_sql(connection, f"{alias}.original_content_id", cutoff_at=cutoff_at)
    # Identity/manual relationships retain their own existing authority.
    return f"""({alias}.method!='text_sha256' AND ({alias}.method!='fingerprint_v1' OR ({ready} AND {original_ready} AND EXISTS (
        SELECT 1 FROM duplicate_index_generations vg WHERE vg.state='ready'
        AND vg.generation_id=COALESCE(json_extract({alias}.evidence_json,'$.generation_id'),
                                     json_extract({alias}.evidence_json,'$.generation'))))))"""


def relation_complete_sql(connection: sqlite3.Connection, content_expression: str,
                          *, cutoff_at: str | None = None) -> str:
    """Coverage also accepts a fully ACKed, proven logical-merge tombstone.

    Fingerprint projection reads continue to require a current fingerprint via
    relation_ready_sql. A retained identity loser is completed work, but does
    not become available media or an independent negative fingerprint result.
    """
    current = relation_ready_sql(connection, content_expression, cutoff_at=cutoff_at)
    if not indexed_duplicates(connection):
        return current
    cutoff = (" AND julianday(mw.completed_at)<=julianday('" + cutoff_at.replace("'", "''") + "')"
              if cutoff_at is not None else "")
    return f"""({current} OR EXISTS (
        SELECT 1 FROM duplicate_index_generations mg
        JOIN duplicate_dirty_work mw ON mw.generation_id=mg.generation_id
        JOIN content_items mc ON mc.id=mw.content_id
        JOIN content_identity_merge_events me ON me.loser_content_id=mc.id
        JOIN content_aliases ma ON ma.alias_link_id=mc.link_id AND ma.content_id=me.winner_content_id
        JOIN duplicate_relations mr ON mr.duplicate_content_id=me.loser_content_id
          AND mr.original_content_id=me.winner_content_id AND mr.method='identity_merge' AND mr.status='confirmed'
        LEFT JOIN duplicate_component_members mm ON mm.generation_id=mg.generation_id AND mm.content_id=mc.id
        LEFT JOIN duplicate_components mcomp ON mcomp.generation_id=mm.generation_id AND mcomp.component_id=mm.component_id
        WHERE mg.state='ready' AND mc.id={content_expression} AND mw.reason='content_merged'
          AND mw.status='ready' AND mw.target_fingerprint_id IS NULL
          AND mw.completed_input_revision=mw.target_input_revision
          AND NOT EXISTS (SELECT 1 FROM duplicate_current_fingerprints mp
                          WHERE mp.generation_id=mg.generation_id AND mp.content_id=mc.id)
          AND (mm.component_id IS NULL OR mcomp.state='ready')
          AND json_type(CASE WHEN json_valid(mr.evidence_json) THEN mr.evidence_json ELSE '{{}}' END,'$.merge_event_id')='integer'
          AND json_extract(CASE WHEN json_valid(mr.evidence_json) THEN mr.evidence_json ELSE '{{}}' END,'$.merge_event_id')=me.id{cutoff}))"""


def relation_states(connection: sqlite3.Connection, content_ids: Sequence[int],
                    *, cutoff_at: str | None = None) -> dict[int, dict[str, Any]]:
    ids = list(dict.fromkeys(int(value) for value in content_ids))
    if not indexed_duplicates(connection):
        return {cid: {"content_id": cid, "relation_status": "ready"} for cid in ids}
    from .duplicate_index import ready_status
    states = ready_status(connection, ids)
    if cutoff_at is not None:
        for offset in range(0, len(ids), 400):
            batch = ids[offset:offset + 400]
            marks = ",".join("?" for _ in batch)
            eligible = {int(row[0]) for row in connection.execute(
                f"SELECT c.id FROM content_items c WHERE c.id IN ({marks}) AND "
                + relation_complete_sql(connection, "c.id", cutoff_at=cutoff_at), batch)}
            for cid in batch:
                if states[cid]["relation_status"] == "ready" and cid not in eligible:
                    states[cid]["relation_status"] = "pending"
    return states


def relation_coverage(connection: sqlite3.Connection, content_ids: Sequence[int],
                      *, cutoff_at: str | None = None) -> dict[str, Any]:
    ids = list(dict.fromkeys(int(value) for value in content_ids))
    if not indexed_duplicates(connection):
        return {}
    ready: set[int] = set()
    failed: set[int] = set()
    for offset in range(0, len(ids), 400):
        batch = ids[offset:offset + 400]
        marks = ",".join("?" for _ in batch)
        ready.update(int(row[0]) for row in connection.execute(
            f"SELECT c.id FROM content_items c WHERE c.id IN ({marks}) AND "
            + relation_complete_sql(connection, "c.id", cutoff_at=cutoff_at), batch))
        failed.update(int(row[0]) for row in connection.execute(
            "SELECT dw.content_id FROM duplicate_dirty_work dw "
            "JOIN duplicate_index_generations dg ON dg.generation_id=dw.generation_id "
            f"WHERE dg.state='ready' AND dw.status='failed' AND dw.content_id IN ({marks})", batch))
    return {"duplicate_relation_coverage": round(len(ready) * 100 / len(ids), 2) if ids else 100.0,
            "duplicate_relation_ready": len(ready), "duplicate_relation_pending": len(ids) - len(ready) - len(failed),
            "duplicate_relation_failed": len(failed), "duplicate_relation_total": len(ids)}
