"""Schema23 backlog scheduling using real SQLite claims and durable commands.

Provider execution and normal readiness are isolated boundary fixtures; actual
expired manual validation is tested separately. No provider traffic is allowed.
"""
from collections import Counter
from contextlib import ExitStack, nullcontext
from itertools import count
import hashlib
import json
from threading import Event, Lock
import unittest
from unittest.mock import patch

from tests import test_v23_media_source_refresh as media_fixture
from v8 import capture_commands, capture_manual, capture_runtime as runtime
from v8 import media_source_refresh as refresh, storage

AT = media_fixture.AT


class CaptureQueueFairnessV23Test(unittest.TestCase):
    def setUp(self):
        self.fixture = media_fixture.MediaSourceRefreshTest(); self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.db = self.fixture.db
        self.clock = AT
        self.enterContext(patch('socket.socket.connect', side_effect=AssertionError('network forbidden')))
        self.enterContext(patch.object(runtime, 'now_utc', side_effect=lambda: self.clock))
        self.enterContext(patch.object(runtime, '_V23_ROLLING_ROUNDS', count()))
        with storage.connect(self.db) as c:
            self.assertEqual(c.execute('PRAGMA user_version').fetchone()[0], 23)
            c.execute("INSERT INTO capture_route_assignments(id,scope_type,scope_key,provider,operation,generation,route,mode,effective_at,recorded_at,assignment_sha256) VALUES(1,'account','fixture','tikhub','douyin_video_detail',1,'integrated','active',?,?,?)", (AT, AT, 'f'*64))

    def add_work(self, identifier, platform='douyin', *, stage='detail', due=AT, extra=None, content_id=1):
        operation = platform + ('_user_posts' if stage == 'discovery' else '_video_detail')
        env = {'fixture_id':identifier,'account_id':1,'identity_id':1,'content_id':content_id,
            'platform':platform,'uid':'fixture','stage':stage,'capture_stage':stage,
            'source_stage':stage,'operation':operation,'logical_due':'fixture:'+str(identifier),
            'assignment_id':1,'category':'detail','activation_id':1,'roster_snapshot_id':None,
            'roster_members_sha256':'a'*64,'window_start':AT,'window_end':AT}
        if stage == 'profile_prepare':
            env.update(intake_request_id=identifier, preparation_plan_id=1,
                preparation_key='fixture-'+str(identifier),preparation_subject='fixture')
        env.update(extra or {})
        with storage.connect(self.db) as c:
            c.execute("""INSERT INTO capture_work_items(id,work_identity,assignment_id,account_id,content_id,
                provider,operation,due_at,data_business_day,state,reason,envelope_json,created_at,updated_at)
                VALUES(?,?,1,1,?,'tikhub',?,?,'2026-09-12','runnable','',?,?,?)""",
                (identifier,hashlib.sha256(str(identifier).encode()).hexdigest(),content_id,env['operation'],
                 runtime.planning.timestamp(due),json.dumps(env),AT,AT))

    def add_manual(self, identifier, *, at=AT):
        with storage.connect(self.db) as c:
            if identifier != 1:
                c.execute("""INSERT INTO content_items(id,account_id,link_id,platform,platform_content_id,
                    canonical_url,title,content_type,raw_account_uid,published_at,created_at,updated_at,imported_at)
                    SELECT ?,account_id,? ,platform,?,canonical_url,title,content_type,raw_account_uid,
                        published_at,created_at,updated_at,imported_at FROM content_items WHERE id=1""",
                    (identifier,'T'+str(identifier).zfill(5),str(7380000000000000000+identifier)))
                c.commit()
            quote = refresh.prepare_media_source_refresh(c,content_id=identifier,request_id='quote-'+str(identifier),
                source_generation='missing',at=at)
            accepted = refresh.execute_media_source_refresh(c,content_id=identifier,task_id=quote['task_id'],
                source_generation='missing',at=at)
            proposal = c.execute('SELECT * FROM media_source_refresh_proposals WHERE id=?',(quote['task_id'],)).fetchone()
        self.add_work(identifier,content_id=identifier,extra={'kind':'media_source_refresh',
            'manual_command_run_id':accepted['run_id'],'manual_command_run_ids':[accepted['run_id']],
            'task_id':quote['task_id'],'task_max_amount':quote['max_amount'],
            'logical_due':refresh.logical_due(proposal)})
        return accepted

    def executing(self, callback=None, *, real_readiness=False):
        stack=ExitStack()
        stack.enter_context(patch.object(runtime,'activation_at',return_value={'profile_id':'integrated_route_v1'}))
        if not real_readiness:
            stack.enter_context(patch.object(runtime,'_readiness',return_value=('runnable','')))
        stack.enter_context(patch.object(runtime.planning,'execution_route_context',return_value=nullcontext()))
        stack.enter_context(patch.object(runtime.planning,'advance_watermark'))
        stack.enter_context(patch.object(runtime,'_verify_raws'))
        def execute(envelope,**_kw):
            if callback: callback(envelope)
            return {'complete':True,'continuation':False,'envelope':envelope,
                'evidence':{'raw_response_ids':[]},'reason':'','provider_cost':0}
        stack.enter_context(patch.object(runtime,'_execute_one',side_effect=execute))
        return stack

    def test_thousand_older_douyin_items_do_not_starve_three_discoveries_or_preparation(self):
        for i in range(1,1001):self.add_work(i)
        for i,p in enumerate(('xiaohongshu','kuaishou','wechat_channels'),1001):
            self.add_work(i,p,stage='discovery')
        for i,p in enumerate(('douyin','xiaohongshu','kuaishou','wechat_channels'),1101):
            self.add_work(i,p,stage='profile_prepare')
        with storage.connect(self.db) as c:
            before=list(c.execute('SELECT id,work_identity,due_at FROM capture_work_items ORDER BY id'))
        seen=[]
        with self.executing(lambda e:seen.append((e['fixture_id'],e['platform'],e['stage']))):
            result=runtime.run_ready(self.db,max_items=1,rolling=True)
        self.assertEqual(len(seen),16)
        self.assertEqual(len({x[0] for x in seen}),16)
        self.assertTrue({1001,1002,1003,1101,1102,1103,1104}<={x[0] for x in seen})
        self.assertEqual(len(result['results']),16)
        with storage.connect(self.db) as c:
            self.assertEqual([tuple(x) for x in before],[tuple(x) for x in c.execute('SELECT id,work_identity,due_at FROM capture_work_items ORDER BY id')])
            self.assertEqual(c.execute('SELECT count(*) FROM provider_usage').fetchone()[0],0)

    def test_confirmed_manual_backlog_is_bounded_and_normal_platforms_still_run(self):
        for i in range(10,30):self.add_manual(i)
        for i,p in enumerate(('douyin','xiaohongshu','kuaishou','wechat_channels'),100):
            self.add_work(i,p)
            self.add_work(i+100,p,stage='profile_prepare')
        seen=[]
        with self.executing(lambda e:seen.append(e)):
            runtime.run_ready(self.db,max_items=1,rolling=True)
        self.assertEqual(sum(e.get('kind')=='media_source_refresh' for e in seen),2)
        self.assertEqual(seen[0]['kind'],'media_source_refresh')
        self.assertTrue({100,101,102,103,200,201,202,203}<={e['fixture_id'] for e in seen})

    def test_manual_only_queue_cannot_borrow_other_fourteen_slots(self):
        for i in range(10,30):self.add_manual(i)
        seen=[]
        with self.executing(lambda e:seen.append(e['fixture_id'])):
            result=runtime.run_ready(self.db,rolling=True)
        self.assertLessEqual(len(result['results']),16)
        self.assertEqual(len(seen),2)
        self.assertEqual(len(set(seen)),2)

    def test_expired_manual_is_finalized_by_real_validation_before_claim_or_send(self):
        self.add_manual(10)
        self.clock='2026-09-12T02:00:00Z'
        with patch.object(refresh,'now_utc',return_value=self.clock),self.executing(real_readiness=True), \
             patch.object(runtime,'_execute_one',side_effect=AssertionError('expired quote executed')):
            result=runtime._run_ready_one(self.db,'manual_media')
        self.assertEqual(result['status'],'terminal')
        self.assertEqual(result['reason'],'media_source_refresh_expired')
        self.assertEqual(result['provider_calls'],0)
        with storage.connect(self.db) as c:
            self.assertEqual(tuple(c.execute('SELECT state,attempt_count FROM capture_work_items WHERE id=10').fetchone()),('terminal',0))
            self.assertEqual(c.execute("SELECT count(*) FROM scheduler_runs WHERE job_id=?",(runtime.JOB,)).fetchone()[0],0)
            self.assertEqual(c.execute('SELECT count(*) FROM provider_usage').fetchone()[0],0)

    def test_valid_manual_precedes_expired_and_bad_scalar_binding_has_no_priority(self):
        self.add_manual(10)
        later='2026-09-12T02:00:00Z';self.add_manual(11,at=later)
        self.add_work(1)
        token=runtime._WORK_SELECTION_LANE.set('manual_media')
        try:
            with storage.connect(self.db) as c:
                self.assertEqual(runtime._select_runnable_work(c,later)['id'],11)
                envelope=json.loads(c.execute('SELECT envelope_json FROM capture_work_items WHERE id=11').fetchone()[0])
                envelope['manual_command_run_id']+=999
                c.execute('UPDATE capture_work_items SET envelope_json=? WHERE id=11',(json.dumps(envelope),))
                self.assertEqual(runtime._select_runnable_work(c,later)['id'],10)
                c.execute("UPDATE capture_work_items SET state='paid_identity_hold' WHERE id=10")
                self.assertEqual(runtime._select_runnable_work(c,later)['id'],1)
        finally:runtime._WORK_SELECTION_LANE.reset(token)

    def test_quote_expiring_after_claim_is_failed_terminal_without_paid_retry(self):
        accepted=self.add_manual(10)
        self.clock='2026-09-12T00:59:59Z'
        def send_boundary(_envelope):
            self.clock='2026-09-12T01:00:00Z'
            with storage.connect(self.db) as c:
                capture_manual.validate_command(c,accepted['run_id'],content_id=10,stage='detail')
            self.fail('expired quote passed the send boundary')
        with patch.object(refresh,'now_utc',side_effect=lambda:self.clock),self.executing(send_boundary):
            result=runtime._run_ready_one(self.db,'manual_media')
        self.assertEqual(result['status'],'terminal')
        self.assertFalse(result['complete'])
        self.assertEqual(result['reason'],'media_source_refresh_expired')
        with storage.connect(self.db) as c:
            self.assertEqual(c.execute('SELECT count(*) FROM provider_usage').fetchone()[0],0)
            self.assertEqual(c.execute('SELECT status FROM scheduler_runs WHERE job_id=?',(runtime.JOB,)).fetchone()[0],'failed')
            self.assertEqual(tuple(c.execute('SELECT state,attempt_count FROM capture_work_items WHERE id=10').fetchone()),('terminal',1))

    def test_expired_sent_unbilled_without_raw_remains_paid_identity_hold(self):
        self.add_manual(10)
        with storage.connect(self.db) as c:
            task_id=json.loads(c.execute('SELECT envelope_json FROM capture_work_items WHERE id=10').fetchone()[0])['task_id']
            c.execute("INSERT INTO provider_usage(task_id,provider,operation,request_attempts,billed_requests,recorded_at) VALUES(?,'TikHub','douyin_video_detail',1,0,?)",(task_id,AT))
        self.clock='2026-09-12T02:00:00Z'
        with patch.object(refresh,'now_utc',return_value=self.clock),self.executing(real_readiness=True), \
             patch.object(runtime,'_execute_one',side_effect=AssertionError('uncertain paid identity executed')):
            result=runtime._run_ready_one(self.db,'manual_media')
        self.assertEqual(result['status'],'paid_identity_hold')
        self.assertEqual(result['reason'],'media_source_refresh_request_limit')
        with storage.connect(self.db) as c:
            self.assertEqual(tuple(c.execute('SELECT state,attempt_count,completed_at FROM capture_work_items WHERE id=10').fetchone()),('paid_identity_hold',0,None))
            self.assertEqual(c.execute('SELECT count(*) FROM provider_usage').fetchone()[0],1)

    def test_real_claims_keep_four_concurrent_sixteen_total_and_refill(self):
        for i in range(1,25):self.add_work(i)
        started,refilled=Event(),Event();lock=Lock();seen=[];active=0;peak=0
        def execute(e):
            nonlocal active,peak
            with lock:
                index=len(seen);seen.append(e['fixture_id']);active+=1;peak=max(peak,active)
                if index==3:started.set()
            try:
                if index==0:self.assertTrue(refilled.wait(5))
                elif index<4:self.assertTrue(started.wait(5))
                else:refilled.set()
            finally:
                with lock:active-=1
        with self.executing(execute):result=runtime.run_ready(self.db,rolling=True)
        self.assertEqual(peak,4);self.assertEqual(active,0)
        self.assertEqual(len(seen),16);self.assertEqual(len(set(seen)),16)
        self.assertEqual(len(result['results']),16)
        with storage.connect(self.db) as c:
            self.assertEqual(c.execute('SELECT count(*) FROM scheduler_run_attempts').fetchone()[0],16)

    def test_continuous_lane_cycle_is_fair_across_rounds_and_restart_still_visits_every_lane(self):
        lanes=[]
        def dispatch(*_a,**_kw):
            lanes.append(runtime._WORK_SELECTION_LANE.get());return {'status':'terminal'}
        with patch.object(runtime,'run_one',side_effect=dispatch):
            for _ in range(9):runtime.run_ready(self.db,max_items=1,rolling=True)
        for offset in range(0,len(lanes),16):
            self.assertEqual(set(lanes[offset:offset+16]),set(runtime._V23_RUN_READY_LANES))
            self.assertLessEqual(lanes[offset:offset+16].count('manual_media'),2)
        self.assertEqual(set(Counter(lanes).values()),{16})
        self.assertIsNone(runtime._WORK_SELECTION_LANE.get())


if __name__=='__main__':unittest.main()
