#!/usr/bin/env python3
"""Exercise complete schema24 paid A/reservation/B on disposable local authority.

This uses real migrations, source/receipt verification, a real Writer lease,
operation gates, route checks, admission reservations and ledger send marks.
Only the existing fixture installation/source pins and test work readiness are
supplied. The provider dispatcher is replaced by the assertion-bearing A/B
callback and sockets are forbidden. No formal database is opened or altered.
"""
from __future__ import annotations
import argparse
from collections import Counter, defaultdict
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import time
import traceback
from urllib.parse import unquote, urlsplit
from unittest.mock import patch

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'src/dcar_eval'))


def distribution(values):
    values = sorted(values)
    return ({'count': len(values), 'p50': values[math.ceil(len(values) * .50) - 1],
             'p95': values[math.ceil(len(values) * .95) - 1], 'max': values[-1]}
            if values else {'count': 0})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True, help='New private output directory')
    parser.add_argument('--rounds', type=int, default=20)
    args = parser.parse_args()
    if not 1 <= args.rounds <= 100:
        parser.error('rounds must be 1..100')
    output = args.output.absolute()
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    if output.resolve() != output:
        parser.error('output may not have symlink parents')
    metrics = output / 'transactions.jsonl'
    denied_access = []
    formal = Path.home() / 'Library/Application Support/DcarAIGC/data/dcar_insight.sqlite3'
    formal_identity = (formal.stat().st_dev, formal.stat().st_ino) if formal.exists() else None

    def audit(event, values):
        if event == 'socket.connect':
            denied_access.append({'kind': 'socket', 'target': str(values[1])})
            raise RuntimeError('Complete A/B fixture forbids every network connection')
        if event == 'sqlite3.connect' and values[0] != ':memory:':
            name = os.fsdecode(values[0])
            target = Path(unquote(urlsplit(name).path) if name.startswith('file:') else name).absolute()
            identity = (target.stat().st_dev, target.stat().st_ino) if target.exists() else None
            if target == formal or (formal_identity is not None and identity == formal_identity):
                denied_access.append({'kind': 'formal_database', 'target': str(target)})
                raise RuntimeError('Complete A/B fixture forbids the formal database and all aliases')

    sys.addaudithook(audit)
    os.environ['DCAR_SQLITE_METRICS_FILE'] = str(metrics)
    from tests.test_v24_complete_paid_boundary import CompletePaidBoundaryTest
    case = CompletePaidBoundaryTest()
    case.mutations = ('none',) * args.rounds + ('source', 'database', 'budget', 'gate')
    report = {'contract': 'duplicate-full-paid-boundary-check-v1', 'source_root': str(ROOT),
        'source_files': {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
            for name in ('src/dcar_eval/v8/provider_budget.py', 'src/dcar_eval/v8/runtime_evidence_context.py',
                         'src/dcar_eval/v8/capture.py', 'tests/fixtures_v24_installed_paid.py',
                         'tests/test_v24_complete_paid_boundary.py')},
        'functional': False, 'provider_network_calls': 0,
        'scope': ['Actual temporary schema22→23→24 migrations and sealed real schema23 backup',
                  'Temporary fixture installation/source-location pins, not formal installed authority',
                  'Real claim and reserved_unsent admission, real send mark to sent_unsettled',
                  'Fresh source, DB read-set, operation-gate and budget revocations must reject B',
                  'Provider dispatcher replaced by assertion-bearing local A/B callback; sockets forbidden'],
        'rounds': args.rounds}
    began = time.perf_counter()
    try:
        case.setUp()
        report['fixture_setup_seconds'] = time.perf_counter() - began
        report['database_schema'] = case.f.connection.execute('PRAGMA user_version').fetchone()[0]
        from v8 import schema_v24
        report['migration_proof'] = schema_v24.migration_proof(case.f.connection)
        report['real_parent_backup_sha256'] = case.fixture.backup_ref['sha256']
        started = time.perf_counter()
        case.test_real24_full_claim_reserve_send_and_revoke_before_send()
        report['exercise_seconds'] = time.perf_counter() - started
        report['samples'] = case.boundary_samples
        report['cold_preparation_seconds'] = case.cold_preparation_seconds
        report['functional'] = True
    except BaseException:
        report['error'] = traceback.format_exc()
    finally:
        case.doCleanups()
    report['denied_access'] = denied_access
    if 'samples' not in report:
        report['samples'] = getattr(case, 'boundary_samples', [])
    rows = []
    if metrics.exists():
        for line in metrics.read_text().splitlines():
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if row.get('phase') == 'finish':
                rows.append(row)
    by_job = defaultdict(list)
    for row in rows:
        by_job[row.get('job_id', 'unattributed')].append(row)
    report['transactions'] = {name: {'count': len(items),
        'hold_ms': distribution([r.get('lock_hold_ms', r['hold_ms']) for r in items]),
        'wait_ms': distribution([r['queue_wait_ms'] for r in items]),
        'outcomes': dict(Counter(r.get('outcome') for r in items))} for name, items in by_job.items()}
    succeeded = [s for s in report.get('samples', []) if s['mutation'] == 'none']
    report['successful_full_requests'] = len(succeeded)
    report['claim_request_ms'] = distribution([s['claim_seconds'] * 1000 for s in succeeded])
    report['send_request_ms'] = distribution([s['send_seconds'] * 1000 for s in succeeded])
    report['whole_work_ms'] = distribution([s['whole_work_seconds'] * 1000 for s in succeeded])
    report['threshold_scope'] = 'Full A/B timings reported separately; original 1s/5s and 50/250ms gates apply to relation work/duplicate transactions.'
    report['passed'] = report['functional'] and len(succeeded) == args.rounds and not denied_access
    (output / 'full-boundary.json').write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(json.dumps({k: v for k, v in report.items() if k not in ('samples', 'migration_proof', 'source_files')}, ensure_ascii=False))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
