"""Claim preparation with real schema23 lineage and real SQLite claim writes.

Only the provider-result and account readiness fixtures are supplied. Installed
catalog inheritance, source/read-set fences, durable claims and batch freezing
remain real; no provider network is permitted.
"""
from contextlib import contextmanager, nullcontext
import hashlib
import json
import sqlite3
import unittest
from unittest.mock import patch

from tests import test_four_platform_flow_release as fixtures
from tests import test_v23_capture_queue_fairness as queue_fixtures
from v8 import account_catalog_capture as catalog, capture_batches as batches
from v8 import capture_runtime as runtime, four_platform_flow_release as flow
from v8 import runtime_evidence_context as context, storage


class WorkClaimPreflightTest(unittest.TestCase):
    def test_real_lineage_stays_outside_claim_lock_and_claim_writes_keep_exit_fence(self):
        fixture = fixtures.FourPlatformFlowReleaseTest(); fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        f = fixture.f
        self.enterContext(patch('socket.socket.connect', side_effect=AssertionError('provider network forbidden')))
        self.enterContext(patch.object(context, '_loaded_source_root', return_value=fixture.source))
        with fixture.flow_runtime():
            at = storage.now_utc()
            c = f.connection
            account = c.execute('SELECT id FROM accounts ORDER BY id LIMIT 1').fetchone()[0]
            identity = c.execute('SELECT id FROM account_platform_identities WHERE account_id=? LIMIT 1', (account,)).fetchone()[0]
            active = runtime.activation_at(c, at)
            self.assertTrue(runtime.execution_profile_allowed(active))
            before_usage = c.execute('SELECT count(*) FROM provider_usage').fetchone()[0]
            before_attempts = c.execute('SELECT count(*) FROM fetch_attempts').fetchone()[0]
            def add_work(number, operation):
                with storage.transaction(c):
                    content = c.execute("INSERT INTO content_items(account_id,link_id,platform,platform_content_id,canonical_url,title,content_type,created_at,updated_at,imported_at) VALUES(?,?,'douyin',?,?,'offline claim fixture','video',?,?,?)",
                        (account, 'CLM'+str(number).zfill(3), str(7380000000000000000+number), 'https://www.douyin.com/video/'+str(7380000000000000000+number), at, at, at)).lastrowid
                    assignment = c.execute("INSERT INTO capture_route_assignments(scope_type,scope_key,provider,operation,content_id,generation,route,mode,effective_at,recorded_at,assignment_sha256) VALUES('content',?,'tikhub',?,?,1,'integrated','active',?,?,?)",
                        ('claim-'+str(number), operation, content, at, at, hashlib.sha256(str(number).encode()).hexdigest())).lastrowid
                    stage = 'metrics' if operation == batches.OPERATION else 'detail'
                    env = {'account_id':account,'identity_id':identity,'content_id':content,'platform':'douyin',
                        'stage':stage,'capture_stage':stage,'source_stage':stage,'category':stage,
                        'operation':operation,'assignment_id':assignment,'logical_due':'claim-'+str(number),
                        **{key:active[key] for key in ('activation_id','profile_id','roster_snapshot_id','roster_members_sha256')}}
                    work = c.execute("INSERT INTO capture_work_items(work_identity,assignment_id,account_id,content_id,provider,operation,due_at,data_business_day,state,reason,envelope_json,created_at,updated_at) VALUES(?,?,?,?,'tikhub',?,?,?,'runnable','',?,?,?)",
                        (hashlib.sha256(('work-'+str(number)).encode()).hexdigest(),assignment,account,content,operation,
                         runtime.planning.timestamp(at),runtime._business_day(at),json.dumps(env),at,at)).lastrowid
                return work, content
            cold = []
            original = flow.verify_inheritance
            def verify(**kwargs):
                if type(kwargs['connection']).__name__ == '_ReadSet':
                    # A second real connection can acquire SQLite's write lock
                    # while the expensive lineage verifier is running.
                    probe = sqlite3.connect(f.db, timeout=0)
                    try:
                        probe.execute('BEGIN IMMEDIATE'); probe.rollback()
                    finally:
                        probe.close()
                    cold.append(True)
                return original(**kwargs)
            seen = []
            def readiness(connection, envelope, *, at):
                self.assertIsNotNone(context._PREPARED.get())
                self.assertIsNotNone(catalog.installed_policy(connection, at=at))
                seen.append(envelope['content_id'])
                return 'runnable', ''
            def complete(envelope, **_):
                self.assertIsNotNone(context._PREPARED.get(), 'work proof must reach provider A/B')
                return {'complete':True,'continuation':False,'envelope':envelope,
                    'evidence':{'raw_response_ids':[]},'reason':'','provider_cost':0}
            def complete_batch(frozen, **_):
                self.assertIsNotNone(context._PREPARED.get())
                return {'complete':True,'members':[{'content_id':m['work']['content_id'],'disposition':'valid'} for m in frozen['members']],
                        'provider_cost':0,'provider_calls':0}
            with patch.object(flow, 'verify_inheritance', side_effect=verify), \
                    patch.object(runtime, '_readiness', side_effect=readiness), \
                    patch.object(runtime.planning, 'execution_route_context', return_value=nullcontext()), \
                    patch.object(runtime, '_execute_one', side_effect=complete) as send, \
                    patch.object(runtime, '_verify_raws'), \
                    patch.object(batches, 'execute_batch', side_effect=complete_batch) as batch_send:
                single, single_content = add_work(1, 'douyin_video_detail')
                self.assertEqual(runtime._run_single(f.db, at)['status'], 'terminal')
                pair, pair_content = add_work(2, batches.OPERATION)
                self.assertEqual(batches.run_one(f.db, at)['status'], 'terminal')
                self.assertEqual(len(cold), 2)
                self.assertEqual(seen, [single_content, pair_content])
                self.assertEqual(c.execute('SELECT state FROM capture_work_items WHERE id=?',(single,)).fetchone()[0], 'terminal')
                self.assertEqual(c.execute('SELECT state FROM capture_work_items WHERE id=?',(pair,)).fetchone()[0], 'terminal')
                self.assertEqual(c.execute('SELECT count(*) FROM fetch_request_batches WHERE work_id=?',(pair,)).fetchone()[0], 1)
                self.assertEqual(c.execute('SELECT count(*) FROM scheduler_runs WHERE job_id=?',(runtime.JOB,)).fetchone()[0], 2)
                self.assertEqual((send.call_count,batch_send.call_count), (1,1))
                # Mutations after readiness but before claim commit must roll
                # back the actual claim and prevent all provider execution.
                for number, mutation in ((3, 'database'), (4, 'source')):
                    work, _ = add_work(number, 'douyin_video_detail')
                    source = fixture.source/'src/dcar_eval/v8/provider_budget.py'
                    original_bytes = source.read_bytes()
                    def changed(connection, envelope, *, at):
                        result = readiness(connection, envelope, at=at)
                        if mutation == 'database':
                            connection.execute('UPDATE capture_catalog_revision SET projection_depth=1 WHERE id=1')
                        else:
                            source.write_bytes(original_bytes+b'\n# changed after admission\n')
                        return result
                    try:
                        with patch.object(runtime, '_readiness', side_effect=changed), \
                                self.assertRaises(context.RuntimeEvidenceChanged):
                            runtime._run_single(f.db, at)
                    finally:
                        if mutation == 'source': source.write_bytes(original_bytes)
                    self.assertEqual(tuple(c.execute('SELECT state,attempt_count FROM capture_work_items WHERE id=?',(work,)).fetchone()), ('runnable',0))
                    self.assertEqual(c.execute('SELECT count(*) FROM scheduler_runs WHERE job_id=?',(runtime.JOB,)).fetchone()[0], 2)
                    self.assertEqual((send.call_count,batch_send.call_count), (1,1))
                    with storage.transaction(c):
                        c.execute("UPDATE capture_work_items SET state='terminal',completed_at=? WHERE id=?", (at,work))
            self.assertEqual(c.execute('SELECT projection_depth FROM capture_catalog_revision WHERE id=1').fetchone()[0], 0)
            self.assertEqual(c.execute('SELECT count(*) FROM provider_usage').fetchone()[0], before_usage)
            self.assertEqual(c.execute('SELECT count(*) FROM fetch_attempts').fetchone()[0], before_attempts)

    def test_manual_quote_expiring_during_preparation_is_rejected_before_claim(self):
        fixture = queue_fixtures.CaptureQueueFairnessV23Test(); fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.add_manual(10)
        @contextmanager
        def delayed(_database):
            # No installed source in this smaller fixture. This isolates only
            # clock placement; manual expiry and SQLite state changes are real.
            fixture.clock = '2026-09-12T02:00:00Z'
            yield object()
        with patch.object(context, 'prepare_inheritance', delayed), \
                patch.object(queue_fixtures.refresh, 'now_utc', side_effect=lambda: fixture.clock), \
                fixture.executing(real_readiness=True), \
                patch.object(runtime, '_execute_one', side_effect=AssertionError('expired quote executed')):
            result = runtime._run_single(fixture.db, queue_fixtures.AT)
        self.assertEqual((result['status'],result['reason'],result['provider_calls']), ('terminal','media_source_refresh_expired',0))
        with storage.connect(fixture.db) as c:
            self.assertEqual(tuple(c.execute('SELECT state,attempt_count FROM capture_work_items WHERE id=10').fetchone()), ('terminal',0))
            self.assertEqual(c.execute('SELECT count(*) FROM scheduler_runs WHERE job_id=?',(runtime.JOB,)).fetchone()[0],0)
            self.assertEqual(c.execute('SELECT count(*) FROM provider_usage').fetchone()[0],0)
