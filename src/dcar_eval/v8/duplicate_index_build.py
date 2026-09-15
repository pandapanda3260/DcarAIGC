"""Offline index/true-edge construction from existing fingerprint facts only."""
from __future__ import annotations

import json
import sqlite3
import time
from typing import Callable

from . import duplicate_index as index, duplicate_graph as graph
from .storage import _require_non_formal_connection, now_utc


def _proven_merged_contents(connection: sqlite3.Connection) -> set[int]:
    """A schema20 logical deletion requires all three durable identity facts."""
    return {int(row[0]) for row in connection.execute("""SELECT e.loser_content_id
        FROM content_identity_merge_events e JOIN content_items c ON c.id=e.loser_content_id
        JOIN content_aliases alias ON alias.alias_link_id=c.link_id AND alias.content_id=e.winner_content_id
        JOIN duplicate_relations relation ON relation.duplicate_content_id=e.loser_content_id
          AND relation.original_content_id=e.winner_content_id AND relation.method='identity_merge'
          AND relation.status='confirmed'
        WHERE json_type(CASE WHEN json_valid(relation.evidence_json) THEN relation.evidence_json ELSE '{}' END,
                        '$.merge_event_id')='integer'
          AND json_extract(CASE WHEN json_valid(relation.evidence_json) THEN relation.evidence_json ELSE '{}' END,
                           '$.merge_event_id')=e.id""")}


def build_existing_fingerprints(connection: sqlite3.Connection, *, progress: Callable | None = None) -> dict:
    """Resume a building generation on an offline, frozen database candidate.

    The first pass publishes every current posting before any candidate query.
    The second pass visits each unordered pair only from its smaller endpoint.
    Existing canonical stars are used solely to compute a projection diff.
    """
    _require_non_formal_connection(connection)
    if connection.in_transaction or connection.execute('PRAGMA user_version').fetchone()[0] != 24:
        raise ValueError('offline building requires an idle schema24 candidate')
    from .duplicates import FINGERPRINT_VERSION, THRESHOLDS
    calibration = connection.execute('SELECT status FROM duplicate_calibration_runs WHERE fingerprint_version=? '
        'AND thresholds_json=? ORDER BY created_at DESC,id DESC LIMIT 1',
        (FINGERPRINT_VERSION, graph.canonical_json(THRESHOLDS))).fetchone()
    if calibration is None or calibration[0] != 'passed':
        raise ValueError('duplicate detector calibration has not passed')
    from .schema_v24 import migration_proof
    from .schema_v20 import row_digest
    # Resume is allowed only against the exact frozen business inputs. Existing
    # pointers/ACKs are not evidence that an externally edited candidate is safe.
    proof = migration_proof(connection)
    existing = index.active_generation(connection)
    for table, expected in proof['retained_tables'].items():
        if table == 'schema_migrations' or (existing and table == 'duplicate_relations'):
            continue
        if row_digest(connection, table, expected['columns']) != expected:
            raise ValueError('candidate business inputs changed since migration: ' + table)
    started = time.monotonic()
    connection.execute('BEGIN IMMEDIATE')
    try:
        generation = index.create_generation(connection)
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    gid = generation['generation_id']
    merged = _proven_merged_contents(connection)
    tombstones = {row[0] for row in connection.execute("SELECT content_id FROM duplicate_dirty_work "
        "WHERE generation_id=? AND reason='content_merged' AND target_fingerprint_id IS NULL", (gid,))}
    pointers = {row[0] for row in connection.execute('SELECT content_id FROM duplicate_current_fingerprints WHERE generation_id=?', (gid,))}
    if generation['state'] == 'ready':
        if merged & pointers or not merged <= tombstones:
            raise ValueError('completed offline generation has invalid merged-content tombstones')
        saved = connection.execute("SELECT result_json FROM duplicate_work_staging WHERE generation_id=? "
            "AND work_content_id=0 AND record_type='offline_build_report'", (gid,)).fetchone()
        if saved is None:
            raise ValueError('active generation is not a resumable offline build')
        report = json.loads(saved[0])
        if any(report[key] != value for key, value in index.validate_postings(connection, generation_id=gid).items()):
            raise ValueError('completed offline postings changed')
        return report
    ids = [row[0] for row in connection.execute('SELECT id FROM content_items ORDER BY id')]
    indexed = unavailable = 0
    # Source state must be the same function used by actual fingerprint creation.
    # This calls no downloader/ffmpeg/provider and never regenerates a fingerprint.
    for offset in range(0, len(ids), 100):
        chunk = ids[offset:offset + 100]
        states = [index.source_current(connection, cid) for cid in chunk if cid not in pointers and cid not in merged]
        connection.execute('BEGIN IMMEDIATE')
        try:
            for cid in chunk:
                if cid in merged and (cid not in tombstones or cid in pointers):
                    index.invalidate_content(connection, cid, reason='content_merged', deleted=True, generation_id=gid)
                    # A interrupted build from an older implementation may have
                    # already compared this identity. Such edges cannot survive
                    # its proven logical deletion in the resumed generation.
                    connection.execute('DELETE FROM duplicate_match_edges WHERE generation_id=? '
                        'AND (left_content_id=? OR right_content_id=?)', (gid, cid, cid))
            for state in states:
                if state['fingerprint_id'] is None:
                    index.invalidate_content(connection, state['content_id'], reason='migration_source_unavailable',
                                             source_sha256=state['source_sha256'], generation_id=gid)
                    unavailable += 1
                else:
                    index.index_fingerprint(connection, content_id=state['content_id'],
                                            fingerprint_id=state['fingerprint_id'], source_sha256=state['source_sha256'],
                                            generation_id=gid)
                    indexed += 1
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        if progress and offset % 1000 == 0:
            progress({'phase': 'postings', 'visited': min(offset + 100, len(ids)), 'total': len(ids)})
    posting_proof = index.validate_postings(connection, generation_id=gid)
    raw = index.read_current_fingerprints(connection, generation_id=gid)
    prepared = index.prepare_fingerprints(raw)
    pending = [row[0] for row in connection.execute("SELECT w.content_id FROM duplicate_dirty_work w JOIN duplicate_current_fingerprints p "
        "ON p.generation_id=w.generation_id AND p.content_id=w.content_id WHERE w.generation_id=? AND p.input_status='available' "
        "AND w.status!='ready' ORDER BY w.content_id", (gid,))]
    compared, candidates, matches = 0, 0, 0
    for offset in range(0, len(pending), 20):
        seeds = pending[offset:offset + 20]
        retrieved = index.query_candidate_ids(connection, seeds, generation_id=gid)
        edge_rows = []
        for seed in seeds:
            candidates += len(retrieved[seed])
            for other in sorted(retrieved[seed]):
                if other <= seed:
                    continue
                result = index.compare_prepared(prepared[seed], prepared[other])
                compared += 1
                if result['confirmed']:
                    edge_rows.append((gid, seed, other, prepared[seed].fingerprint_id, prepared[other].fingerprint_id,
                                      prepared[seed].input_revision, prepared[other].input_revision,
                                      result['confidence'], graph.canonical_json(result), 1))
        connection.execute('BEGIN IMMEDIATE')
        try:
            connection.executemany('INSERT INTO duplicate_match_edges VALUES(?,?,?,?,?,?,?,?,?,?)', edge_rows)
            connection.executemany("UPDATE duplicate_dirty_work SET status='ready',completed_input_revision=target_input_revision,completed_at=? "
                                   "WHERE generation_id=? AND content_id=?", [(now_utc(), gid, cid) for cid in seeds])
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        matches += len(edge_rows)
        if progress and offset % 200 == 0:
            progress({'phase': 'true_edges', 'visited': min(offset + 20, len(pending)), 'total': len(pending),
                      'compared_pairs': compared, 'confirmed_pairs': matches})
    pointers = {row['content_id']: dict(row) for row in connection.execute('SELECT p.*,c.published_at,c.imported_at FROM '
        'duplicate_current_fingerprints p JOIN content_items c ON c.id=p.content_id WHERE p.generation_id=?', (gid,))}
    edges = [dict(row) for row in connection.execute('SELECT * FROM duplicate_match_edges WHERE generation_id=?', (gid,))]
    delta = graph.build_component_delta({'generation_id': gid, 'pointers': pointers, 'edges': edges, 'members': {}}, {})
    old_relations = {(r['duplicate_content_id'], r['original_content_id']): dict(r) for r in connection.execute(
        "SELECT * FROM duplicate_relations WHERE method='fingerprint_v1'")}
    removed, inserted, updated, removed_text = [], [], [], []
    connection.execute('BEGIN IMMEDIATE')
    try:
        # Replacing only derived graph membership is safe while building. The
        # consumer cannot see this generation until the final atomic activation.
        connection.execute('DELETE FROM duplicate_component_members WHERE generation_id=?', (gid,))
        connection.execute('DELETE FROM duplicate_components WHERE generation_id=?', (gid,))
        connection.executemany('INSERT INTO duplicate_components VALUES(?,?,?,?,?,?,?)',
            [(gid, component, value['canonical_content_id'], 1, 'ready', value['member_count'], now_utc())
             for component, value in delta['components'].items()])
        connection.executemany('INSERT INTO duplicate_component_members VALUES(?,?,?)',
            [(gid, cid, component) for cid, component in delta['members'].items()])
        for pair, row in old_relations.items():
            if pair not in delta['projections']:
                connection.execute('DELETE FROM duplicate_relations WHERE id=?', (row['id'],))
                removed.append(pair)
        for pair, row in delta['projections'].items():
            before = old_relations.get(pair)
            if before is None:
                connection.execute("INSERT INTO duplicate_relations(duplicate_content_id,original_content_id,method,confidence,evidence_json,status,created_at) "
                    "VALUES(?,?,'fingerprint_v1',?,?,'confirmed',?)", (*pair, row['confidence'], row['evidence_json'], now_utc()))
                inserted.append(pair)
            elif any(before[k] != row[k] for k in ('confidence', 'evidence_json', 'status')):
                connection.execute('UPDATE duplicate_relations SET confidence=?,evidence_json=?,status=? WHERE id=?',
                    (row['confidence'], row['evidence_json'], row['status'], before['id']))
                updated.append(pair)
        removed_text = [row[0] for row in connection.execute("SELECT r.id FROM duplicate_relations r "
            "JOIN duplicate_current_fingerprints a ON a.content_id=r.duplicate_content_id AND a.generation_id=? "
            "JOIN duplicate_current_fingerprints b ON b.content_id=r.original_content_id AND b.generation_id=a.generation_id "
            "WHERE r.method='text_sha256' AND a.input_status='available' AND b.input_status='available'", (gid,))]
        connection.executemany('DELETE FROM duplicate_relations WHERE id=?', [(rid,) for rid in removed_text])
        # A missing current source is a durable media-pending state. There is no
        # old edge left to invalidate, so its graph cleanup ACK is complete.
        connection.execute("UPDATE duplicate_dirty_work SET status='ready',completed_input_revision=target_input_revision,completed_at=? "
                           "WHERE generation_id=?", (now_utc(), gid))
        if connection.execute('PRAGMA foreign_key_check').fetchone():
            raise ValueError('offline graph foreign key violation')
        connection.execute("UPDATE duplicate_index_generations SET state='ready',graph_revision=1,activated_at=? WHERE generation_id=?",
                           (now_utc(), gid))
        counts = {table: connection.execute('SELECT COUNT(*) FROM ' + table + ' WHERE generation_id=?', (gid,)).fetchone()[0]
                  for table in ('duplicate_current_fingerprints', 'duplicate_match_edges', 'duplicate_components', 'duplicate_component_members')}
        report = {**posting_proof, 'status': 'built', 'provider_calls': 0, 'fingerprints_regenerated': 0,
                  'merged_content_tombstones': len(merged),
                  'compared_pairs_this_run': compared, 'candidate_count_this_run': candidates, 'counts': counts,
                  'projection_delta': {'removed': removed, 'inserted': inserted, 'updated_evidence_v2': updated},
                  'removed_legacy_text_relation_ids': removed_text, 'elapsed_seconds': round(time.monotonic() - started, 3)}
        report = json.loads(graph.canonical_json(report))
        connection.execute("INSERT INTO duplicate_work_staging VALUES(?,0,0,0,'offline_build_report','offline_build_report',?,NULL,?,'offline',?)",
                           (gid, proof['receipt_sha256'], graph.canonical_json(report), now_utc()))
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    return report
