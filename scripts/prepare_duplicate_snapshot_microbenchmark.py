#!/usr/bin/env python3
"""Create a NONDEPLOYABLE index/graph benchmark from a frozen legacy snapshot.

This is deliberately not a schema migration receipt. Unmanaged source identity
is reconstructed by the baseline's exact AST source function from stored source
SHA fields; managed content without verified bundle bytes is explicitly omitted.
"""
from __future__ import annotations
import argparse, ast, hashlib, json, re, sqlite3, subprocess, sys, time
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src' / 'dcar_eval'))
from v8 import duplicate_index as index, duplicate_graph as graph, schema_v24
from v8.duplicates import FINGERPRINT_VERSION
from v8.storage import is_formal_database_path, now_utc


def stored_source_reader(baseline):
    body = subprocess.check_output(['git', 'show', f'{baseline}:src/dcar_eval/v8/duplicates.py'], cwd=ROOT)
    tree = ast.parse(body)
    names = {'_source_inputs', '_current_source_state', '_latest_artifact', '_normalize_text', '_canonical_json', '_sha256_json'}
    nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    if {node.name for node in nodes} != names:
        raise ValueError('baseline source function extraction incomplete')
    def unmanaged_only(connection, cid, kinds):
        if connection.execute("SELECT 1 FROM evidence_artifacts WHERE content_id=? AND (artifact_type='media_lifecycle_manifest' "
            "OR instr(metadata_json,'\"media_lifecycle\"')>0) LIMIT 1", (cid,)).fetchone():
            raise ValueError('managed source needs separate immutable bundle proof')
        return None, None
    namespace = dict(sqlite3=sqlite3, re=re, json=json, hashlib=hashlib, Optional=Optional, Sequence=Sequence, List=List,
        Dict=Dict, Mapping=Mapping, Any=Any, FINGERPRINT_VERSION=FINGERPRINT_VERSION, DuplicateDetectionError=ValueError,
        managed_bound_artifact=unmanaged_only, _resolved=Path, _artifact_text=lambda row, keys: '')
    # ASR/OCR text is never part of the source-digest object: their immutable SHA
    # fields are. Suppressing _artifact_text file reads cannot change that object.
    exec(compile(ast.Module(body=nodes, type_ignores=[]), 'baseline-source-identity', 'exec'), namespace)
    return namespace['_current_source_state'], hashlib.sha256(body).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True, type=lambda v: Path(v).resolve())
    parser.add_argument('--target', required=True, type=lambda v: Path(v).resolve())
    parser.add_argument('--report', required=True, type=lambda v: Path(v).resolve())
    parser.add_argument('--baseline-revision', default='b37aa36')
    args = parser.parse_args()
    if args.target.exists() or is_formal_database_path(args.target):
        raise ValueError('target must be a new non-formal microbenchmark database')
    args.target.parent.mkdir(parents=True, exist_ok=True)
    source_identity, baseline_sha = stored_source_reader(args.baseline_revision)
    with sqlite3.connect(args.source.as_uri() + '?mode=ro', uri=True) as origin, sqlite3.connect(args.target) as copied:
        origin.backup(copied)
    c = sqlite3.connect(args.target); c.row_factory = sqlite3.Row
    previous_version = c.execute('PRAGMA user_version').fetchone()[0]
    c.execute('PRAGMA foreign_keys=ON'); c.execute('PRAGMA journal_mode=WAL')
    c.execute('BEGIN IMMEDIATE'); schema_v24.create_tables(c); c.execute('PRAGMA user_version=24')
    gid = index.create_generation(c)['generation_id']; c.commit()
    managed = {row[0] for row in c.execute("SELECT DISTINCT content_id FROM evidence_artifacts WHERE artifact_type='media_lifecycle_manifest' OR instr(metadata_json,'\"media_lifecycle\"')>0")}
    historic = {row[0] for row in c.execute('SELECT DISTINCT content_id FROM duplicate_fingerprints WHERE fingerprint_version=?', (FINGERPRINT_VERSION,))}
    ids = [row[0] for row in c.execute('SELECT id FROM content_items ORDER BY id')]
    exclusions, available, states = {}, [], []
    for offset, cid in enumerate(ids):
        state = {'content_id': cid, 'source_sha256': None, 'fingerprint_id': None}
        reason = None
        if cid in managed:
            reason = 'managed_bundle_not_proven_in_snapshot'
        elif cid not in historic:
            reason = 'no_historical_fingerprint'
        else:
            _, state['source_sha256'] = source_identity(c, cid)
            row = c.execute('SELECT id FROM duplicate_fingerprints WHERE content_id=? AND fingerprint_version=? AND source_sha256=?',
                (cid, FINGERPRINT_VERSION, state['source_sha256'])).fetchone()
            if row:
                state['fingerprint_id'] = row[0]; available.append(cid)
            else:
                reason = 'no_fingerprint_matches_frozen_source'
        if reason:
            exclusions.setdefault(reason, []).append(cid)
        states.append((state, reason))
    for offset in range(0, len(states), 200):
        c.execute('BEGIN IMMEDIATE')
        for state, reason in states[offset:offset + 200]:
            if state['fingerprint_id']:
                index.index_fingerprint(c, **state, generation_id=gid)
            else:
                index.invalidate_content(c, state['content_id'], source_sha256=state['source_sha256'], reason=reason, generation_id=gid)
        c.commit()
    print(json.dumps({'phase': 'snapshot_postings', 'available': len(available), 'excluded': {k: len(v) for k,v in exclusions.items()}}), flush=True)
    raw = index.read_current_fingerprints(c, generation_id=gid); prepared = index.prepare_fingerprints(raw)
    comparisons = 0
    for offset in range(0, len(available), 20):
        seeds = available[offset:offset+20]; candidates = index.query_candidate_ids(c, seeds, generation_id=gid); edges = []
        for seed in seeds:
            for other in candidates[seed]:
                if seed >= other: continue
                result = index.compare_prepared(prepared[seed], prepared[other]); comparisons += 1
                if result['confirmed']:
                    edges.append((gid, seed, other, prepared[seed].fingerprint_id, prepared[other].fingerprint_id,
                        prepared[seed].input_revision, prepared[other].input_revision, result['confidence'], graph.canonical_json(result), 1))
        c.execute('BEGIN IMMEDIATE'); c.executemany('INSERT INTO duplicate_match_edges VALUES(?,?,?,?,?,?,?,?,?,?)', edges); c.commit()
        if offset % 1000 == 0:
            print(json.dumps({'phase':'snapshot_true_edges','visited':offset+len(seeds),'total':len(available),'compared':comparisons}),flush=True)
    pointers = {row['content_id']: dict(row) for row in c.execute('SELECT p.*,c.published_at,c.imported_at FROM duplicate_current_fingerprints p JOIN content_items c ON c.id=p.content_id WHERE generation_id=?',(gid,))}
    old_edges = [dict(row) for row in c.execute('SELECT * FROM duplicate_match_edges WHERE generation_id=?',(gid,))]
    delta = graph.build_component_delta({'generation_id':gid,'pointers':pointers,'edges':old_edges,'members':{}},{})
    c.execute('BEGIN IMMEDIATE')
    c.executemany('INSERT INTO duplicate_components VALUES(?,?,?,?,?,?,?)',[(gid,key,value['canonical_content_id'],1,'ready',value['member_count'],now_utc()) for key,value in delta['components'].items()])
    c.executemany('INSERT INTO duplicate_component_members VALUES(?,?,?)',[(gid,cid,key) for cid,key in delta['members'].items()])
    c.execute("DELETE FROM duplicate_relations WHERE method='fingerprint_v1'")
    c.executemany("INSERT INTO duplicate_relations(duplicate_content_id,original_content_id,method,confidence,evidence_json,status,created_at) VALUES(?,?,'fingerprint_v1',?,?,'confirmed',?)",
        [(left,right,row['confidence'],row['evidence_json'],now_utc()) for (left,right),row in delta['projections'].items()])
    c.execute("UPDATE duplicate_dirty_work SET status='ready',completed_input_revision=target_input_revision,completed_at=? WHERE generation_id=?",(now_utc(),gid))
    c.execute("UPDATE duplicate_index_generations SET state='ready',graph_revision=1,activated_at=? WHERE generation_id=?",(now_utc(),gid));c.commit()
    proof = index.validate_postings(c,generation_id=gid)
    report = {'schema':'nondeployable-snapshot-index-microbenchmark-v1','source':str(args.source),'source_schema':previous_version,'target':str(args.target),
        'baseline_revision':args.baseline_revision,'baseline_source_sha256':baseline_sha,'generation':gid,'postings':proof,'comparisons':comparisons,
        'direct_edges':len(old_edges),'components':len(delta['components']),'exclusions':exclusions,'provider_calls':0,'fingerprints_regenerated':0,
        'source_proof':'Baseline AST source digest from frozen title/body and latest available media/asr/ocr artifact SHA; exact matching (content,version,source) historical fingerprint only. No latest-fingerprint fallback.',
        'not_deployable':'This adds test tables to a schema19 snapshot copy; it is NOT an authorized 23-to-24 migration and carries no release receipt.'}
    args.report.write_text(json.dumps(report,indent=2)+'\n');c.close();print(json.dumps({'report':str(args.report),'status':'ready-for-microbenchmark'}),flush=True)

if __name__ == '__main__': main()
