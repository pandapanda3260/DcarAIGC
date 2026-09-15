#!/usr/bin/env python3
"""Read-only stratified duplicate-index differential; optional isolated-copy stress.

The reference is extracted from a pinned git revision, not the new comparison
facade. Input databases are never mutated. --stress-copy must name a new file.
"""
from __future__ import annotations

import argparse
import ast
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
import time
from typing import Any, Dict, Mapping, Optional, Sequence

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src' / 'dcar_eval'))
from v8 import duplicate_index as index, duplicate_graph as graph, duplicate_runtime as runtime
from v8.storage import connect, transaction, transaction_metrics_context, now_utc, is_formal_database_path


def summary(values):
    values = sorted(values)
    return {'count': len(values), 'mean': sum(values) / len(values),
            'p50': values[math.ceil(len(values) * .5) - 1],
            'p95': values[math.ceil(len(values) * .95) - 1], 'max': values[-1]} if values else {'count': 0}


def baseline_compare(revision, repository=ROOT):
    source = subprocess.check_output(['git', 'show', f'{revision}:src/dcar_eval/v8/duplicates.py'], cwd=repository)
    tree = ast.parse(source)
    names = {'_phash_distance', '_simhash_similarity', 'compare_fingerprints'}
    selected = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names
                or isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.target.id == 'THRESHOLDS']
    if {node.name for node in selected if isinstance(node, ast.FunctionDef)} != names:
        raise ValueError('baseline function extraction incomplete')
    reference = ast.Module(body=selected, type_ignores=[])
    # A new facade would import duplicate_index; reject this before executing.
    if any(isinstance(node, (ast.Import, ast.ImportFrom)) for node in ast.walk(reference)):
        raise ValueError('baseline contains imports; choose the pre-optimization commit')
    namespace = dict(json=json, Optional=Optional, Sequence=Sequence, Mapping=Mapping, Any=Any, Dict=Dict)
    exec(compile(reference, f'git:{revision}/duplicates.py', 'exec'), namespace)
    return namespace['compare_fingerprints'], hashlib.sha256(source).hexdigest()


def choose_seeds(raw, metadata, members, sizes, count):
    def ranked(ids):
        return sorted(ids, key=lambda cid: hashlib.sha256(f'duplicate-v24-strata:{cid}'.encode()).hexdigest())
    strata = {
        'isolated': [cid for cid in raw if cid not in members],
        'component20': [cid for cid in raw if sizes.get(members.get(cid)) == 20],
        'component97': [cid for cid in raw if sizes.get(members.get(cid)) == 97],
        'largest_component': [cid for cid in raw if sizes.get(members.get(cid)) == max(sizes.values(), default=0)],
        'image': [cid for cid in raw if metadata[cid]['content_type'] == 'image'],
        'video': [cid for cid in raw if metadata[cid]['content_type'] == 'video'],
        'repeated_frames': [cid for cid, row in raw.items() if len(json.loads(row['frame_phashes_json'])) > len(set(json.loads(row['frame_phashes_json'])))],
        'no_frames': [cid for cid, row in raw.items() if not json.loads(row['frame_phashes_json'])],
    }
    selected = set()
    quota = max(1, count // len(strata))
    for ids in strata.values():
        selected.update(ranked(ids)[:quota])
    for cid in ranked(set(raw) - selected):
        if len(selected) >= count:
            break
        selected.add(cid)
    selected = sorted(selected)[:count]
    return selected, {name: {'available': len(ids), 'selected': len(set(ids) & set(selected))} for name, ids in strata.items()}



def verify_calibration(raw, compare):
    dataset_path = ROOT / 'config' / 'duplicate_calibration_v1.json'
    body = dataset_path.read_bytes()
    dataset = json.loads(body)
    by_link = {row['link_id']: row for row in raw.values()}
    outcomes, missing, differences, inputs = [], [], [], {}
    tp = fp = positive = negative = predicted = 0
    for pair in dataset['pairs']:
        left, right = pair['left_link_id'], pair['right_link_id']
        if left not in by_link or right not in by_link:
            missing.append([left, right]); continue
        inputs[left], inputs[right] = by_link[left], by_link[right]
        original = compare(by_link[left], by_link[right])
        current = index.compare_prepared(index.prepare_fingerprint(by_link[left]), index.prepare_fingerprint(by_link[right]))
        if original != current:
            differences.append([left, right])
        is_positive = pair['label'] == 'duplicate'
        positive += is_positive; negative += not is_positive
        predicted += current['confirmed']
        tp += current['confirmed'] and is_positive
        fp += current['confirmed'] and not is_positive
        outcomes.append({'left': left, 'right': right, 'label': pair['label'], **current})
    precision = tp / predicted if predicted else 0
    return {'dataset': str(dataset_path), 'dataset_sha256': hashlib.sha256(body).hexdigest(), 'pair_count': len(outcomes),
        'positive_count': positive, 'negative_count': negative, 'precision': precision, 'recall': tp / positive if positive else 0,
        'missing_current_pairs': missing, 'comparison_differences': differences, 'outcomes': outcomes, 'frozen_fingerprint_inputs': inputs,
        'passed': len(outcomes) == 150 and positive == negative == 75 and precision >= .95 and not differences}


def legacy_calibration_inputs(database, revision, repository=ROOT):
    source = subprocess.check_output(['git', 'show', f'{revision}:src/dcar_eval/v8/duplicates.py'], cwd=repository)
    tree = ast.parse(source)
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == '_current_fingerprints')
    version = next(node for node in tree.body if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == 'FINGERPRINT_VERSION' for target in node.targets))
    from typing import List
    namespace = dict(sqlite3=sqlite3, List=List, Dict=Dict, Any=Any)
    exec(compile(ast.Module(body=[version, function], type_ignores=[]), 'baseline-calibration-selection', 'exec'), namespace)
    with sqlite3.connect(database.as_uri() + '?mode=ro', uri=True) as c:
        c.row_factory = sqlite3.Row
        c.execute('PRAGMA query_only=ON'); c.execute('BEGIN')
        values = namespace['_current_fingerprints'](c)
        c.rollback()
    return {row['content_id']: row for row in values}

def stress(source, target, seeds, metrics_dir, rounds):
    if target.exists() or is_formal_database_path(target):
        raise ValueError('stress-copy must be a new non-formal database path')
    target.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(source.as_uri() + '?mode=ro', uri=True) as origin, sqlite3.connect(target) as copied:
        origin.backup(copied)
        copied.execute('PRAGMA journal_mode=WAL')
    metrics_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
    metrics_path = metrics_dir / 'sqlite-transactions.jsonl'
    cold_started = time.perf_counter()
    runtime.drain_duplicate_work(db_path=target, scope_content_ids=[], limit=0)
    cold_start_probe_ms = (time.perf_counter() - cold_started) * 1000
    previous = os.environ.get('DCAR_SQLITE_METRICS_FILE')
    os.environ['DCAR_SQLITE_METRICS_FILE'] = str(metrics_path)
    with sqlite3.connect(target) as c:
        sizes = {cid: size for cid, size in c.execute('SELECT m.content_id,k.member_count FROM duplicate_component_members m JOIN duplicate_components k ON k.generation_id=m.generation_id AND k.component_id=m.component_id')}
    largest = max(sizes.values(), default=1)
    remaining = set(seeds)
    groups, group_labels = [], []
    for label, predicate in [('largest_component', lambda cid: sizes.get(cid, 1) == largest),
                             ('component20', lambda cid: sizes.get(cid, 1) == 20),
                             ('isolated', lambda cid: cid not in sizes), ('mixed', lambda cid: True)]:
        group = [cid for cid in seeds if cid in remaining and predicate(cid)][:20]
        if len(group) < 20:
            group.extend([cid for cid in seeds if cid in remaining and cid not in group][:20-len(group)])
        groups.append(group); group_labels.append(label); remaining.difference_update(group)
    latencies, calls, failures, mixed_writes = [], [], [], []
    stop = threading.Event()
    lock = threading.Lock()

    def mixed_writer():
        while not stop.is_set():
            with connect(target) as c, transaction_metrics_context(job_id='verification_mixed_metadata'), transaction(c):
                c.execute('UPDATE content_items SET updated_at=? WHERE id=?', (now_utc(), seeds[0]))
            mixed_writes.append(1)
            stop.wait(.02)

    def request(ids):
        start = time.perf_counter()
        latest = None
        for _ in range(500):
            call_start = time.perf_counter()
            latest = runtime.drain_duplicate_work(db_path=target, scope_content_ids=ids, time_budget_seconds=5)
            with lock:
                calls.append((time.perf_counter() - call_start) * 1000)
            if latest['relation_status'] in ('ready', 'failed'):
                break
            if time.perf_counter() - start >= 15:
                break
            time.sleep(.01)
        with lock:
            latencies.append((time.perf_counter() - start) * 1000)
            if latest['relation_status'] != 'ready':
                failures.append(latest)

    writer = threading.Thread(target=mixed_writer)
    try:
        writer.start()
        for round_number in range(rounds):
            with connect(target) as c, transaction_metrics_context(job_id='verification_enqueue'), transaction(c):
                for cid in sorted(set(sum(groups, []))):
                    index.mark_content_dirty(c, cid)
                    c.execute('UPDATE content_items SET published_at=? WHERE id=?',
                        ('2000-01-01T00:00:00Z' if round_number % 2 == 0 else '2099-01-01T00:00:00Z', cid))
            with ThreadPoolExecutor(max_workers=4) as pool:
                list(pool.map(request, groups))
    finally:
        stop.set(); writer.join()
        if previous is None:
            os.environ.pop('DCAR_SQLITE_METRICS_FILE', None)
        else:
            os.environ['DCAR_SQLITE_METRICS_FILE'] = previous
    records = [json.loads(line) for line in metrics_path.read_text().splitlines()]
    finishes = [row for row in records if row.get('phase') == 'finish' and str(row.get('job_id', '')).startswith('duplicate_graph')]
    return {'database': str(target), 'cold_start_read_only_probe_ms': cold_start_probe_ms, 'request_count': len(latencies), 'threads': 4, 'mutation': 'published_at alternates 2000/2099, with atomic input_revision invalidation',
        'groups': [{'label': label, 'content_ids': group, 'component_sizes': [sizes.get(cid, 1) for cid in group]} for label, group in zip(group_labels, groups)], 'mixed_metadata_writes': len(mixed_writes),
        'request_to_ready_ms': summary(latencies), 'individual_call_ms': summary(calls),
        'relation_transaction_hold_ms': summary([row['hold_ms'] for row in finishes]),
        'relation_transaction_queue_wait_ms': summary([row['queue_wait_ms'] for row in finishes]),
        'transaction_metrics_file': str(metrics_path), 'failures': failures,
        'passed': not failures and summary(latencies).get('p95', 0) <= 1000 and max(latencies, default=0) <= 5000
            and summary([row['hold_ms'] for row in finishes]).get('p95', 0) <= 50
            and max((row['hold_ms'] for row in finishes), default=0) <= 250,
        'limitation': 'Four real relation callers plus local metadata transactions on a copied SQLite DB; no providers, ASR/OCR, or installed formal permission guard load.'}



def verify_articulation_deletion(source, target, metrics_dir):
    """Delete a real articulation in a new copy, then verify the full split."""
    if target.exists() or is_formal_database_path(target):
        raise ValueError('deletion target must be new and non-formal')
    with sqlite3.connect(source.as_uri() + '?mode=ro', uri=True) as origin, sqlite3.connect(target) as copied:
        origin.backup(copied); copied.execute('PRAGMA journal_mode=WAL')
    with connect(target) as c:
        gid = index.active_generation(c)['generation_id']
        adjacency, all_edges = {}, {}
        for raw in c.execute('SELECT * FROM duplicate_match_edges WHERE generation_id=?', (gid,)):
            edge = dict(raw); left, right = edge['left_content_id'], edge['right_content_id']
            all_edges[(left, right)] = edge
            adjacency.setdefault(left, set()).add(right); adjacency.setdefault(right, set()).add(left)
        sizes = {cid: size for cid, size in c.execute('SELECT m.content_id,k.member_count FROM duplicate_component_members m JOIN duplicate_components k ON k.generation_id=m.generation_id AND k.component_id=m.component_id')}
        discovered, low, parent, cuts = {}, {}, {}, set()
        def walk(node):
            discovered[node] = low[node] = len(discovered) + 1; children = 0
            for other in adjacency[node]:
                if other not in discovered:
                    parent[other] = node; children += 1; walk(other); low[node] = min(low[node], low[other])
                    if node not in parent and children > 1 or node in parent and low[other] >= discovered[node]: cuts.add(node)
                elif parent.get(node) != other: low[node] = min(low[node], discovered[other])
        for cid in adjacency:
            if cid not in discovered: walk(cid)
        if not cuts:
            return {'passed': False, 'reason': 'frozen graph has no real articulation'}
        seed = min(cuts, key=lambda cid: (-sizes[cid], cid))
        component = c.execute('SELECT component_id FROM duplicate_component_members WHERE generation_id=? AND content_id=?', (gid, seed)).fetchone()[0]
        members = [row[0] for row in c.execute('SELECT content_id FROM duplicate_component_members WHERE generation_id=? AND component_id=?', (gid, component))]
        marks = ','.join('?' for _ in members)
        pointers = {row['content_id']: dict(row) for row in c.execute('SELECT p.*,x.published_at,x.imported_at FROM duplicate_current_fingerprints p JOIN content_items x ON x.id=p.content_id WHERE p.generation_id=? AND p.content_id IN ('+marks+')', [gid,*members])}
    pointers[seed] = dict(pointers[seed], input_status='unavailable', fingerprint_id=None)
    expected = graph.build_component_delta({'generation_id': gid, 'pointers': pointers,
        'edges': [edge for pair,edge in all_edges.items() if pair[0] in members or pair[1] in members],
        'members': {cid: component for cid in members}}, {seed:{}})
    metrics_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
    metrics_path = metrics_dir / 'sqlite-transactions.jsonl'
    previous = os.environ.get('DCAR_SQLITE_METRICS_FILE'); os.environ['DCAR_SQLITE_METRICS_FILE'] = str(metrics_path)
    try:
        physical_delete_error = None
        try:
            with connect(target) as c, transaction_metrics_context(job_id='verification_delete_articulation'), transaction(c):
                index.invalidate_content(c, seed, deleted=True, reason='content_deleted')
                c.execute('DELETE FROM content_items WHERE id=?', (seed,))
        except sqlite3.IntegrityError as error:
            # Existing immutable business evidence may forbid physical deletion.
            # Preserve those constraints and test the other approved removal:
            # source invalidation removes the node from the current graph.
            physical_delete_error = str(error)
            with connect(target) as c, transaction_metrics_context(job_id='verification_invalidate_articulation'), transaction(c):
                index.invalidate_content(c, seed, reason='source_changed')
        scope = [seed] + [cid for cid in members if cid != seed][:19]
        def request(_):
            started = time.perf_counter(); result = runtime.drain_duplicate_work(db_path=target, scope_content_ids=scope)
            return {'milliseconds': (time.perf_counter()-started)*1000, 'result': result}
        with ThreadPoolExecutor(max_workers=4) as pool: results = list(pool.map(request, range(4)))
        with connect(target) as c:
            actual_edges = {(row['left_content_id'],row['right_content_id']) for row in c.execute('SELECT * FROM duplicate_match_edges WHERE generation_id=? AND (left_content_id IN ('+marks+') OR right_content_id IN ('+marks+'))',[gid,*members,*members])}
            actual_projection = {(row['duplicate_content_id'],row['original_content_id']): (row['confidence'],row['evidence_json']) for row in c.execute("SELECT * FROM duplicate_relations WHERE method='fingerprint_v1' AND (duplicate_content_id IN ("+marks+") OR original_content_id IN ("+marks+"))",[*members,*members])}
            actual_component_sizes = sorted(row[0] for row in c.execute('SELECT DISTINCT k.member_count,k.component_id FROM duplicate_components k JOIN duplicate_component_members m ON m.generation_id=k.generation_id AND m.component_id=k.component_id WHERE k.generation_id=? AND m.content_id IN ('+marks+')',[gid,*members]))
            deleted = c.execute('SELECT 1 FROM content_items WHERE id=?',(seed,)).fetchone() is None
            pointer = c.execute('SELECT input_status,input_revision FROM duplicate_current_fingerprints WHERE generation_id=? AND content_id=?',(gid,seed)).fetchone()
            acknowledged = c.execute('SELECT status FROM duplicate_dirty_work WHERE generation_id=? AND content_id=?',(gid,seed)).fetchone()[0] == 'ready'
        expected_projection = {pair:(row['confidence'],row['evidence_json']) for pair,row in expected['projections'].items()}
        exact = actual_edges == set(expected['edges']) and actual_projection == expected_projection
        report = {'database':str(target),'deleted_content_id':seed,'original_component_size':sizes[seed],'original_degree':len(adjacency[seed]),
            'real_articulation_count_in_source':len(cuts),'physical_content_deleted':deleted,'physical_delete_rejection':physical_delete_error,'graph_cleanup_acknowledged':acknowledged,
            'source_unavailable':pointer is not None and pointer['input_status']=='unavailable','input_revision':pointer['input_revision'] if pointer else None,
            'remaining_component_sizes':actual_component_sizes,'expected_component_sizes':sorted(row['member_count'] for row in expected['components'].values()),
            'exact_edges_and_projection_match':exact,'request_ms':summary([row['milliseconds'] for row in results]),
            'results':[row['result'] for row in results],'transaction_metrics_file':str(metrics_path),
            'passed':acknowledged and exact and (deleted or pointer is not None and pointer['input_status']=='unavailable')
                and all(all(item['relation_status']==('pending' if physical_delete_error and item['content_id']==seed else 'ready') for item in row['result']['results']) for row in results)}
    finally:
        if previous is None: os.environ.pop('DCAR_SQLITE_METRICS_FILE',None)
        else: os.environ['DCAR_SQLITE_METRICS_FILE']=previous
    return report

def verify(args):
    compare, baseline_sha = baseline_compare(args.baseline_revision, args.baseline_repo)
    started = time.perf_counter()
    c = sqlite3.connect(args.database.as_uri() + '?mode=ro', uri=True)
    c.row_factory = sqlite3.Row
    c.execute('PRAGMA query_only=ON'); c.execute('BEGIN')
    if c.execute('PRAGMA user_version').fetchone()[0] != 24:
        raise ValueError('requires completed schema24 frozen candidate')
    generation = index.active_generation(c)
    if not generation:
        raise ValueError('building/incomplete generation cannot be verified as ready')
    gid = generation['generation_id']
    posting_proof = index.validate_postings(c, generation_id=gid)
    raw = index.read_current_fingerprints(c, generation_id=gid)
    current_calibration = verify_calibration(raw, compare)
    calibration = verify_calibration(legacy_calibration_inputs(args.calibration_database, args.baseline_revision, args.baseline_repo), compare) if args.calibration_database else current_calibration
    calibration['source_database'] = str(args.calibration_database or args.database)
    calibration['source_role'] = 'frozen historical regression inputs' if args.calibration_database else 'current index inputs'
    at = time.perf_counter(); prepared = index.prepare_fingerprints(raw); prepare_ms = (time.perf_counter() - at) * 1000
    metadata = {row['id']: dict(row) for row in c.execute('SELECT id,content_type,published_at,imported_at FROM content_items')}
    pointers = {row['content_id']: dict(row) for row in c.execute('SELECT * FROM duplicate_current_fingerprints WHERE generation_id=?', (gid,))}
    for cid, row in pointers.items():
        row.update(metadata[cid])
    member_rows = list(c.execute('SELECT content_id,component_id FROM duplicate_component_members WHERE generation_id=?', (gid,)))
    members = {row['content_id']: row['component_id'] for row in member_rows}
    by_component = {}
    for cid, component in members.items():
        by_component.setdefault(component, set()).add(cid)
    sizes = {component: len(values) for component, values in by_component.items()}
    edges = {(row['left_content_id'], row['right_content_id']): dict(row) for row in c.execute('SELECT * FROM duplicate_match_edges WHERE generation_id=?', (gid,))}
    incident = {}
    for pair, edge in edges.items():
        for endpoint in pair:
            incident.setdefault(endpoint, {})[pair] = edge
    seeds, strata = choose_seeds(raw, metadata, members, sizes, args.seed_count)
    results, failures = [], []
    for position, seed in enumerate(seeds):
        at = time.perf_counter(); candidates = index.query_candidate_ids(c, [seed], generation_id=gid)[seed]; query_ms = (time.perf_counter() - at) * 1000
        at = time.perf_counter(); repeated = index.query_candidate_ids(c, [seed], generation_id=gid)[seed]; warm_ms = (time.perf_counter() - at) * 1000
        if candidates != repeated:
            raise AssertionError('candidate query changed within frozen snapshot')
        at = time.perf_counter(); comparisons = {cid: index.compare_prepared(prepared[seed], prepared[cid]) for cid in candidates}; compare_ms = (time.perf_counter() - at) * 1000
        expected, semantic_diff = set(), []
        at = time.perf_counter()
        for other, row in raw.items():
            if other == seed:
                continue
            original = compare(raw[seed], row)
            if original['confirmed']:
                expected.add(other)
            if other in comparisons and comparisons[other] != original:
                semantic_diff.append(other)
        baseline_ms = (time.perf_counter() - at) * 1000
        actual = {cid for cid, row in comparisons.items() if row['confirmed']}
        if actual != expected or semantic_diff:
            failures.append({'seed': seed, 'missed': sorted(expected - actual), 'extra': sorted(actual - expected), 'semantic_diff': semantic_diff})
        at = time.perf_counter()
        affected = {seed, *actual}
        for cid in list(affected):
            affected.update(by_component.get(members.get(cid), ()))
        local_edges = {}
        for cid in affected:
            local_edges.update(incident.get(cid, {}))
        delta = graph.build_component_delta({'generation_id': gid, 'pointers': {cid: pointers[cid] for cid in affected},
            'members': {cid: members[cid] for cid in affected if cid in members}, 'edges': list(local_edges.values())}, {seed: comparisons})
        graph_ms = (time.perf_counter() - at) * 1000
        results.append({'content_id': seed, 'frames': len(prepared[seed].frame_phashes), 'candidate_count': len(candidates),
            'confirmed_count': len(actual), 'component_size': sizes.get(members.get(seed), 1),
            'affected_members': len(affected), 'affected_edges': len(delta['edges']), 'query_first_ms': query_ms,
            'query_warm_ms': warm_ms, 'compare_ms': compare_ms, 'local_graph_ms': graph_ms,
            'indexed_total_ms': query_ms + compare_ms + graph_ms, 'baseline_full_scan_ms': baseline_ms})
        if (position + 1) % 16 == 0:
            print(json.dumps({'phase': 'stratified', 'done': position + 1, 'total': len(seeds), 'failures': len(failures)}), flush=True)
    c.rollback(); c.close()
    report = {'schema': 'duplicate-index-independent-verification-v1', 'database': str(args.database), 'generation': gid,
        'baseline_revision': args.baseline_revision, 'baseline_source_sha256': baseline_sha, 'seed_count': len(seeds),
        'current_fingerprint_count': len(raw), 'strata': strata, 'posting_proof': posting_proof, 'calibration': calibration, 'current_calibration_pair_coverage': current_calibration['pair_count'],
        'baseline_pairs': len(seeds) * (len(raw) - 1), 'prepare_all_ms': prepare_ms, 'failures': failures,
        'statistics_ms': {key: summary([row[key] for row in results]) for key in ('query_first_ms', 'query_warm_ms', 'compare_ms', 'local_graph_ms', 'indexed_total_ms', 'baseline_full_scan_ms')},
        'rows': results, 'passed': not failures and calibration['passed'], 'elapsed_seconds': time.perf_counter() - started,
        'limitations': ['512 stratified seeds exhaustively compared with all current inputs; not exhaustive over every pair in the whole database.',
            'First query and repeated warm query are reported; OS page cache was not evicted.',
            'Pure local graph measurements use a preloaded frozen graph; separate stress measures actual read/transaction end-to-end work.'],
        'provider_calls': 0, 'fingerprints_regenerated': 0}
    report['correctness_passed'] = report['passed']
    report['indexed_latency_gate_passed'] = report['statistics_ms']['indexed_total_ms']['p95'] <= 100
    report['passed'] = report['passed'] and report['indexed_latency_gate_passed']
    if args.stress_copy:
        report['stress'] = stress(args.database, args.stress_copy, seeds, args.output.parent / (args.output.stem + '-metrics'), args.stress_rounds)
        report['passed'] = report['passed'] and report['stress']['passed']
    if args.delete_copy:
        report['articulation_deletion'] = verify_articulation_deletion(args.database, args.delete_copy, args.output.parent / (args.output.stem + '-delete-metrics'))
        report['passed'] = report['passed'] and report['articulation_deletion']['passed']
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'output': str(args.output), 'passed': report['passed'], 'elapsed_seconds': report['elapsed_seconds']}), flush=True)
    return 0 if report['passed'] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', type=lambda value: Path(value).resolve(), required=True)
    parser.add_argument('--baseline-revision', default='b37aa36')
    parser.add_argument('--baseline-repo', type=lambda value: Path(value).resolve(), default=ROOT)
    parser.add_argument('--seed-count', type=int, default=512)
    parser.add_argument('--output', type=lambda value: Path(value).resolve(), required=True)
    parser.add_argument('--calibration-database', type=lambda value: Path(value).resolve())
    parser.add_argument('--delete-copy', type=lambda value: Path(value).resolve())
    parser.add_argument('--stress-copy', type=lambda value: Path(value).resolve())
    parser.add_argument('--stress-rounds', type=int, default=5)
    args = parser.parse_args()
    if args.seed_count < 1:
        parser.error('--seed-count must be positive')
    return verify(args)


if __name__ == '__main__':
    raise SystemExit(main())
