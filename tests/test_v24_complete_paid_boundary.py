"""Real schema24 claim, admission reservation and send mark, with no HTTP."""
from datetime import datetime, timezone
import hashlib
import json
import time
import unittest
from unittest.mock import patch

from tests.fixtures_v24_installed_paid import InstalledDuplicateFixture
from tests import test_v23_work_execution_preflight as work_fixture
from v8 import account_catalog_capture as catalog, capture, capture_authorizations as auth
from v8 import capture_release as gates, capture_runtime as runtime
from v8 import providers, runtime_evidence_context as evidence, storage


class CompletePaidBoundaryTest(unittest.TestCase):
    def setUp(self):
        fixture = self.fixture = InstalledDuplicateFixture(); fixture.setUp()
        self.addCleanup(fixture.doCleanups); self.f = fixture.f
        self.enterContext(patch('socket.socket.connect', side_effect=AssertionError('HTTP forbidden')))
        self.clock = datetime.now(timezone.utc).replace(microsecond=0)
        for module in (evidence, runtime, capture):
            self.enterContext(patch.object(module, '_now' if module is evidence else 'now_utc', side_effect=self.now))

    def now(self):
        return self.clock.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    add_work = work_fixture.WorkExecutionPreflightTest.add_work

    def test_real24_full_claim_reserve_send_and_revoke_before_send(self):
        from v8 import duplicate_index_release
        c = self.f.connection; operation = 'douyin_video_detail'
        self.assertEqual(c.execute('PRAGMA user_version').fetchone()[0], 24)
        with storage.transaction(c):
            gates.publish_operation_gate(c, operation=operation, at=self.now(), mirror_root=self.f.root / 'gates24')
        cold = []
        original = duplicate_index_release.verify_inheritance

        def verify(**kwargs):
            if type(kwargs['connection']).__name__ == '_ReadSet':
                started = time.perf_counter(); result = original(**kwargs)
                cold.append(time.perf_counter() - started)
                return result
            return original(**kwargs)

        def readiness(connection, envelope, *, at):
            self.assertIsNotNone(catalog.installed_policy(connection, at=at))
            return 'runnable', ''

        with auth.runtime_authority(gates.current_runtime_bindings), \
                patch.object(duplicate_index_release, 'verify_inheritance', side_effect=verify), \
                patch.object(runtime, '_readiness', side_effect=readiness), patch.object(runtime, '_verify_raws'):
            self.boundary_samples = []
            for number, mutation in enumerate(getattr(self, 'mutations', ('none', 'source', 'database', 'budget', 'gate')), 1):
                with self.subTest(mutation=mutation):
                    work, content, subject = self.add_work(number)
                    task = 'v24-full-boundary-' + str(number)
                    budget = providers._budget_for_call(provider='TikHub', operation=operation, price=.001,
                        task_id=task, task_max_amount=1, db_path=self.f.db)
                    captured = {'mutation': mutation}

                    def execute(envelope, **_):
                        prepared = evidence._PREPARED.get(); self.assertIsNotNone(prepared)
                        claim_started = time.perf_counter()
                        claim = capture._claim_paid_tikhub(content_id=content, account_id=None, stage='detail',
                            window_key='lifetime', provider='TikHub', adapter_version='fixture24', operation=operation,
                            db_path=self.f.db, budget_id=budget, task_id=task, task_max_amount=1,
                            allow_terminal_retry=False,
                            paid_request_identity=providers._paid_request_identity(operation=operation, platform='douyin',
                                subject=subject, params={'aweme_id': subject}, cursor=None, due_bucket='lifetime'))
                        captured['claim_seconds'] = time.perf_counter() - claim_started
                        captured['claim'] = claim
                        self.assertEqual(c.execute('SELECT state FROM admission_reservations WHERE batch_id=?',
                            (claim.request_batch_id,)).fetchone()[0], 'reserved_unsent')
                        source = self.fixture.source / 'src/dcar_eval/v8/provider_budget.py'
                        source_body = source.read_bytes()
                        if mutation == 'source':
                            source.write_bytes(source_body + b'\n# Revoke prepared source before send.\n')
                        elif mutation == 'database':
                            with storage.transaction(c):
                                c.execute('UPDATE capture_catalog_revision SET projection_depth=1 WHERE id=1')
                        elif mutation == 'budget':
                            with storage.transaction(c):
                                c.execute("UPDATE provider_budget_batches SET status='completed' WHERE id=?", (budget,))
                        elif mutation == 'gate':
                            with storage.transaction(c):
                                old = c.execute('SELECT * FROM capture_paid_send_gate_events WHERE operation=? ORDER BY id DESC LIMIT 1', (operation,)).fetchone()
                                row = {key: old[key] for key in ('provider', 'operation', 'state', 'reason', 'evidence_json', 'recorded_at')}
                                row.update(state='closed', reason='offline revoke', recorded_at=self.now())
                                c.execute(f"INSERT INTO capture_paid_send_gate_events({','.join(row)},event_sha256) VALUES({','.join('?' for _ in range(len(row)+1))})",
                                    (*row.values(), auth.digest(row)))
                        send_started = time.perf_counter()
                        if mutation == 'none':
                            marked = capture._mark_paid_sent(claim, operation=operation, budget_id=budget, db_path=self.f.db)
                            self.assertGreater(marked.attempt_id, 0)
                            self.assertEqual(c.execute('SELECT state FROM admission_reservations WHERE batch_id=?',
                                (claim.request_batch_id,)).fetchone()[0], 'sent_unsettled')
                        else:
                            with self.assertRaises(Exception) as denied:
                                capture._mark_paid_sent(claim, operation=operation, budget_id=budget, db_path=self.f.db)
                        if mutation != 'none':
                            captured['denial'] = {'type': type(denied.exception).__name__, 'message': str(denied.exception)}
                            if mutation == 'source':
                                self.assertIsInstance(denied.exception, evidence.RuntimeEvidenceChanged)
                            elif mutation == 'database':
                                self.assertIsInstance(denied.exception, (evidence.RuntimeEvidenceChanged, storage.SchemaMigrationError))
                        captured['send_seconds'] = time.perf_counter() - send_started
                        if mutation == 'source':
                            source.write_bytes(source_body)
                        elif mutation == 'database':
                            with storage.transaction(c):
                                c.execute('UPDATE capture_catalog_revision SET projection_depth=0 WHERE id=1')
                        self.assertIs(evidence._PREPARED.get(), prepared)
                        return {'complete': True, 'continuation': False, 'envelope': envelope,
                            'evidence': {'raw_response_ids': []}, 'reason': '', 'provider_cost': 0}

                    work_started = time.perf_counter()
                    with patch.object(runtime, '_execute_one', side_effect=execute):
                        result = runtime._run_single(self.f.db, self.now())
                    captured['whole_work_seconds'] = time.perf_counter() - work_started
                    self.assertEqual(result['status'], 'terminal', result)
                    self.assertIn('claim', captured, result)
                    claim = captured['claim']
                    self.assertEqual(c.execute("SELECT count(*) FROM paid_provider_dispatch_events WHERE provider_usage_id=? AND event_type='send_marked'",
                        (claim.reserved_usage_id,)).fetchone()[0], int(mutation == 'none'))
                    self.assertEqual(c.execute('SELECT request_attempts FROM provider_usage WHERE id=?',
                        (claim.reserved_usage_id,)).fetchone()[0], int(mutation == 'none'))
                    self.assertEqual(c.execute('SELECT count(*) FROM fetch_attempts WHERE request_batch_id=?',
                        (claim.request_batch_id,)).fetchone()[0], int(mutation == 'none'))
                    reservation_state = c.execute('SELECT state FROM admission_reservations WHERE batch_id=?',
                        (claim.request_batch_id,)).fetchone()[0]
                    self.assertIn(reservation_state, {'sent_unsettled'} if mutation == 'none'
                                  else {'reserved_unsent', 'released_unsent'})
                    captured['final_reservation_state'] = reservation_state
                    self.assertEqual(len(cold), number)
                    self.assertIsNone(evidence._PREPARED.get())
                    self.boundary_samples.append({key: value for key, value in captured.items() if key != 'claim'})
        self.cold_preparation_seconds = cold
        self.assertEqual(c.execute('SELECT count(*) FROM provider_raw_responses WHERE content_id IN (SELECT content_id FROM capture_work_items WHERE operation=?)',
            (operation,)).fetchone()[0], 0)
