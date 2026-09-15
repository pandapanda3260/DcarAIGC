"""One-work inheritance reuse with real schema23 A/B ledgers and live gates.

Only the fake provider result is supplied; A reserves and B marks the exact
request against a disposable installed database. No HTTP is issued.
"""
from contextvars import copy_context
from datetime import datetime, timedelta, timezone
import hashlib
import json
import sqlite3
from threading import Thread
import unittest
from unittest.mock import patch

from tests import test_four_platform_flow_release as fixtures
from v8 import account_catalog_capture as catalog, capture, capture_authorizations as auth
from v8 import capture_runtime as runtime, capture_release as gates
from v8 import four_platform_flow_release as flow, providers, runtime_evidence_context as evidence, storage


class WorkExecutionPreflightTest(unittest.TestCase):
    def setUp(self):
        fixture = self.fixture = fixtures.FourPlatformFlowReleaseTest(); fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.f = fixture.f
        self.enterContext(patch('socket.socket.connect', side_effect=AssertionError('HTTP forbidden')))
        self.enterContext(patch.object(evidence, '_loaded_source_root', return_value=fixture.source))
        self.clock = datetime.now(timezone.utc)
        self.enterContext(patch.object(evidence, '_now', side_effect=self.now))
        self.enterContext(patch.object(runtime, 'now_utc', side_effect=self.now))
        self.enterContext(patch.object(capture, 'now_utc', side_effect=self.now))

    def now(self):
        return self.clock.strftime('%Y-%m-%dT%H:%M:%SZ')

    def add_work(self, number):
        c = self.f.connection; at = self.now(); active = runtime.activation_at(c, at)
        account = self.f.member['account_id']; uid = self.f.member['uid']
        subject = str(7380000000000000000 + number)
        with storage.transaction(c):
            content = c.execute("INSERT INTO content_items(account_id,link_id,platform,platform_content_id,canonical_url,title,content_type,raw_account_uid,published_at,created_at,updated_at,imported_at) VALUES(?,?,'douyin',?,?,'offline','video',?,?,?,?,?)",
                (account, 'EXE'+str(number).zfill(3), subject,'https://www.douyin.com/video/'+subject,uid,at,at,at,at)).lastrowid
            assignment = c.execute("INSERT INTO capture_route_assignments(scope_type,scope_key,provider,operation,content_id,generation,route,mode,effective_at,recorded_at,assignment_sha256) VALUES('content',?,'tikhub','douyin_video_detail',?,1,'integrated','active',?,?,?)",
                (str(content),content,at,at,hashlib.sha256(str(number).encode()).hexdigest())).lastrowid
            env = {'account_id':account,'identity_id':self.f.member['account_identity_id'],'content_id':content,
                'platform':'douyin','uid':uid,'stage':'detail','capture_stage':'detail','source_stage':'detail',
                'category':'detail','operation':'douyin_video_detail','assignment_id':assignment,'logical_due':'lifetime',
                **{key:active[key] for key in ('activation_id','profile_id','roster_snapshot_id','roster_members_sha256')}}
            work = c.execute("INSERT INTO capture_work_items(work_identity,assignment_id,account_id,content_id,provider,operation,due_at,data_business_day,state,reason,envelope_json,created_at,updated_at) VALUES(?,?,?,?,'tikhub','douyin_video_detail',?,?,'runnable','',?,?,?)",
                (hashlib.sha256(('execution-'+str(number)).encode()).hexdigest(),assignment,account,content,
                 runtime.planning.timestamp(at),runtime._business_day(at),json.dumps(env),at,at)).lastrowid
        return work, content, subject

    def test_one_cold_proof_covers_real_reservation_send_and_live_changes_still_deny(self):
        with self.fixture.flow_runtime():
            self.clock = datetime.now(timezone.utc)
            c = self.f.connection; op = 'douyin_video_detail'
            with storage.transaction(c):
                gates.publish_operation_gate(c, operation=op, at=self.now(), mirror_root=self.f.root/'gates')
            cold = []; original = flow.verify_inheritance
            def verify(**kwargs):
                if type(kwargs['connection']).__name__ == '_ReadSet':
                    probe = sqlite3.connect(self.f.db, timeout=0)
                    try: probe.execute('BEGIN IMMEDIATE'); probe.rollback()
                    finally: probe.close()
                    result = original(**kwargs)
                    cold.append(True)
                    # A cold proof costs four simulated minutes. Repeating it
                    # at B would exceed the unchanged three-minute admission.
                    self.clock += timedelta(seconds=240)
                    return result
                return original(**kwargs)
            def readiness(connection, envelope, *, at):
                self.assertIsNotNone(catalog.installed_policy(connection, at=at))
                return 'runnable', ''
            with auth.runtime_authority(gates.current_runtime_bindings), \
                    patch.object(flow, 'verify_inheritance', side_effect=verify), \
                    patch.object(runtime, '_readiness', side_effect=readiness), \
                    patch.object(runtime, '_verify_raws'):
                for number, mutation in enumerate(('none','source','database','gate','budget'), 1):
                    with self.subTest(mutation=mutation):
                        work, content, subject = self.add_work(number)
                        task = 'offline-execution-'+str(number)
                        budget = providers._budget_for_call(provider='TikHub',operation=op,price=.001,
                            task_id=task,task_max_amount=1,db_path=self.f.db)
                        captured = {}
                        def execute(envelope, **_):
                            prepared = evidence._PREPARED.get(); self.assertIsNotNone(prepared)
                            claim = capture._claim_paid_tikhub(content_id=content,account_id=None,stage='detail',
                                window_key='lifetime',provider='TikHub',adapter_version='fixture',operation=op,
                                db_path=self.f.db,budget_id=budget,task_id=task,task_max_amount=1,
                                allow_terminal_retry=False,
                                paid_request_identity=providers._paid_request_identity(operation=op,platform='douyin',
                                    subject=subject,params={'aweme_id':subject},cursor=None,due_bucket='lifetime'))
                            captured['claim'] = claim
                            self.assertIs(evidence._PREPARED.get(), prepared)
                            self.assertEqual(c.execute('SELECT state FROM admission_reservations WHERE batch_id=?',(claim.request_batch_id,)).fetchone()[0], 'reserved_unsent')
                            source = self.fixture.source/'src/dcar_eval/v8/provider_budget.py'; body = source.read_bytes()
                            if mutation == 'source': source.write_bytes(body+b'\n# changed before B\n')
                            elif mutation == 'database':
                                with storage.transaction(c): c.execute('UPDATE capture_catalog_revision SET projection_depth=1 WHERE id=1')
                            elif mutation == 'gate':
                                with storage.transaction(c):
                                    old = c.execute('SELECT * FROM capture_paid_send_gate_events WHERE operation=? ORDER BY id DESC LIMIT 1',(op,)).fetchone()
                                    row = {key:old[key] for key in ('provider','operation','state','reason','evidence_json','recorded_at')}
                                    row.update(state='closed',reason='offline revoke',recorded_at=self.now())
                                    c.execute(f"INSERT INTO capture_paid_send_gate_events({','.join(row)},event_sha256) VALUES({','.join('?' for _ in range(len(row)+1))})",(*row.values(),auth.digest(row)))
                            elif mutation == 'budget':
                                with storage.transaction(c): c.execute("UPDATE provider_budget_batches SET status='completed' WHERE id=?",(budget,))
                            try:
                                if mutation == 'none':
                                    marked = capture._mark_paid_sent(claim,operation=op,budget_id=budget,db_path=self.f.db)
                                    captured['marked'] = marked
                                    self.assertGreater(marked.attempt_id,0)
                                    self.assertEqual(c.execute('SELECT state FROM admission_reservations WHERE batch_id=?',(claim.request_batch_id,)).fetchone()[0], 'sent_unsettled')
                                else:
                                    with self.assertRaises(Exception) as denied:
                                        capture._mark_paid_sent(claim,operation=op,budget_id=budget,db_path=self.f.db)
                                    captured['denied'] = denied.exception
                                    if mutation == 'source':
                                        self.assertIsInstance(denied.exception,evidence.RuntimeEvidenceChanged)
                                    elif mutation == 'database':
                                        self.assertIsInstance(denied.exception,(evidence.RuntimeEvidenceChanged,storage.SchemaMigrationError))
                            finally:
                                if mutation == 'source': source.write_bytes(body)
                                elif mutation == 'database':
                                    with storage.transaction(c): c.execute('UPDATE capture_catalog_revision SET projection_depth=0 WHERE id=1')
                            return {'complete':True,'continuation':False,'envelope':envelope,
                                    'evidence':{'raw_response_ids':[]},'reason':'','provider_cost':0}
                        with patch.object(runtime, '_execute_one', side_effect=execute):
                            result = runtime._run_single(self.f.db,self.now())
                        self.assertEqual(result['status'],'terminal',(result,[tuple(row) for row in c.execute('SELECT payload_json FROM data_quality_receipts WHERE scope_key=?',('capture-scan:'+str(work),))]))
                        self.assertEqual(len(cold),number)
                        claim = captured['claim']
                        self.assertEqual(c.execute("SELECT count(*) FROM paid_provider_dispatch_events WHERE provider_usage_id=? AND event_type='send_marked'",(claim.reserved_usage_id,)).fetchone()[0], int(mutation=='none'))
                        self.assertEqual(c.execute('SELECT request_attempts FROM provider_usage WHERE id=?',(claim.reserved_usage_id,)).fetchone()[0],int(mutation=='none'))
                        self.assertEqual(c.execute('SELECT count(*) FROM fetch_attempts WHERE request_batch_id=?',(claim.request_batch_id,)).fetchone()[0],int(mutation=='none'))
                        self.assertIsNone(evidence._PREPARED.get())
                        if mutation == 'gate':
                            with storage.transaction(c): gates.publish_operation_gate(c,operation=op,at=self.now(),mirror_root=self.f.root/'gates-restored')
            self.assertEqual(c.execute('SELECT count(*) FROM provider_raw_responses WHERE content_id IN (SELECT content_id FROM capture_work_items WHERE operation=?)',(op,)).fetchone()[0],0)

    def test_borrowed_context_stays_with_same_thread_database_and_no_write_boundary(self):
        with self.fixture.flow_runtime():
            self.clock = datetime.now(timezone.utc)
            with evidence.prepare_inheritance(self.f.db) as outer:
                with evidence.prepare_inheritance(self.f.db) as inner: self.assertIs(inner,outer)
                self.assertIs(evidence._PREPARED.get(),outer)
                wrong = self.f.root/'different.db'; wrong.touch()
                with self.assertRaises(evidence.RuntimeEvidenceChanged),evidence.prepare_inheritance(wrong): pass
                errors = []
                def other_thread():
                    try:
                        with evidence.prepare_inheritance(self.f.db): pass
                    except BaseException as error: errors.append(error)
                thread = Thread(target=copy_context().run,args=(other_thread,));thread.start();thread.join()
                self.assertEqual(len(errors),1);self.assertIsInstance(errors[0],evidence.RuntimeEvidenceChanged)
                with storage.transaction(self.f.connection),evidence.inheritance_boundary(self.f.connection):
                    with self.assertRaises(evidence.RuntimeEvidenceChanged),evidence.prepare_inheritance(self.f.db): pass
                for exception in (KeyboardInterrupt,SystemExit):
                    with self.assertRaises(exception),evidence.prepare_inheritance(self.f.db): raise exception()
                    self.assertIs(evidence._PREPARED.get(),outer)
            self.assertIsNone(evidence._PREPARED.get())
