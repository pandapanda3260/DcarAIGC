"""Disposable installed DB: exact direct authority, real A/B and no HTTP."""
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from threading import Barrier, Event, Lock
import unittest
from unittest.mock import patch

from tests import test_four_platform_flow_release as fixtures
from v8 import capture, capture_repair_fixed as fixed, capture_release as gates
from v8 import providers, runtime_evidence_context as evidence, storage


class FixedRepairTest(unittest.TestCase):
    def fixture(self):
        case = fixtures.FourPlatformFlowReleaseTest(); case.setUp(); self.addCleanup(case.doCleanups)
        self.enterContext(patch('socket.socket.connect', side_effect=AssertionError('HTTP forbidden')))
        self.enterContext(patch.object(evidence, '_loaded_source_root', return_value=case.source))
        self.enterContext(patch.object(capture, 'RAW_ROOT', case.f.root/'fixed-raw'))
        self.enterContext(case.flow_runtime())
        self.enterContext(patch.dict(os.environ, {'DCAR_WRITER_ENTRY': 'repair'}))
        c = case.f.connection; at = storage.now_utc()
        with storage.transaction(c):
            for cid, platform in {(s[0], s[1]) for s in fixed.STAGES.values()}:
                uid = 'fixture-' + platform
                account = c.execute("INSERT INTO accounts(phone,enabled,created_at,updated_at) VALUES('',0,?,?)", (at, at)).lastrowid
                c.execute("INSERT INTO account_platform_identities(account_id,platform,uid,created_at,updated_at) VALUES(?,?,?,?,?)", (account, platform, uid, at, at))
                c.execute("INSERT INTO content_items(id,account_id,link_id,platform,platform_content_id,canonical_url,title,content_type,raw_account_uid,published_at,created_at,updated_at,imported_at) VALUES(?,?,?,?,?,?,'offline','video',?,?,?,?,?)",
                    (cid, account, 'F'+str(cid), platform, str(7380000000000000000+cid), 'https://fixture.invalid/'+str(cid), uid, at, at, at, at))
            gates.publish_operation_gate(c, operation='douyin_video_detail', at=at, mirror_root=case.f.root/'gates')
        plan = fixed.freeze_plan(c, repair_run_id='offline-fixed', window_key='offline-fixed-window',
            authorization='disposable offline fixture only', at=at,
            expires_at=(datetime.now(timezone.utc)+timedelta(hours=1)).isoformat())
        return case, plan

    def test_plan_is_exact_cost_bounded_and_detects_live_identity_change(self):
        case, plan = self.fixture(); c = case.f.connection
        fixed.validate_plan(c, plan, at=storage.now_utc())
        self.assertEqual(len(plan['stages']), 6)
        self.assertEqual({s['target']['content_id'] for s in plan['stages'].values()}, {82116,82117,82078,82120})
        for change in ({'task_max_amount':2}, {'max_extra_requests_per_stage':2}, {'task_id':'different'}):
            with self.assertRaises(fixed.FixedRepairRejected): fixed.validate_plan(c, {**plan, **change}, at=storage.now_utc())
        with storage.transaction(c): c.execute("UPDATE content_items SET raw_account_uid='other' WHERE id=82117")
        with self.assertRaises(Exception): fixed.validate_plan(c, plan, at=storage.now_utc())
        self.assertEqual(c.execute('SELECT count(*) FROM provider_usage').fetchone()[0], 0)

    def test_direct_manual_owner_actual_send_and_persistent_success_reuse(self):
        case, plan = self.fixture(); c = case.f.connection; stage = plan['stages']['dy_detail_metrics']
        target = stage['target']; data = {'account_uid':target['uid'], 'content_type':'video',
            'metrics': {'comment_count':7,'like_count':9,'share_count':3,'collect_count':2}}
        payload = {'stage':'detail','data':data}; body = json.dumps(payload).encode()
        receipt = {'contract_version':'provider-json-transport-v1','transport_route_id':'fixture-route-v1',
            'route_generation':'route-config-sha256:fixture','http_stack':'fixture-stream-v1','request_host':'fixture.invalid',
            'status':'succeeded','error_code':None,'http_status':200,'content_encoding':'identity',
            'content_length':len(body),'clean_eof':True,'length_match':True,'gzip_crc_ok':None,
            'json_parse_ok':True,'entity_bytes':len(body),'entity_sha256':hashlib.sha256(body).hexdigest(),'zero_body':False,
            'request_started_at':storage.now_utc(),'response_finished_at':storage.now_utc(),'latency_ms':1,
            'http_encoded_bytes':len(body),'http_encoded_sha256':hashlib.sha256(body).hexdigest()}
        def provider(*args, **kwargs):
            self.assertEqual(c.execute("SELECT count(*) FROM scheduler_runs WHERE job_id=? AND status='interrupted'", (fixed.capture_commands.JOB,)).fetchone()[0], 0)
            self.assertEqual(c.execute("SELECT count(*) FROM scheduler_runs WHERE job_id=? AND status='running'", (fixed.JOB,)).fetchone()[0], 1)
            self.assertEqual(c.execute('SELECT count(*) FROM capture_work_items').fetchone()[0], 0)
            with self.assertRaises(fixed.FixedRepairRejected):
                fixed.run_stage(plan, 'dy_detail_metrics', db_path=case.f.db)
            return capture.ProviderResult(data,payload,200,True,body,receipt)
        with patch.object(providers,'_load_key',return_value='offline'), \
                patch.object(providers,'_freeze_tikhub_transport',return_value=None), \
                patch.object(providers,'_content_call',side_effect=provider) as call:
            first = fixed.run_stage(plan,'dy_detail_metrics',db_path=case.f.db)
            second = fixed.run_stage(plan,'dy_detail_metrics',db_path=case.f.db)
        self.assertEqual(call.call_count,1)
        self.assertEqual(first['status'],'succeeded'); self.assertEqual(second['provider_calls'],0)
        self.assertEqual(first['raw_response_id'],second['raw_response_id'])
        self.assertEqual(c.execute("SELECT count(*) FROM paid_provider_dispatch_events WHERE event_type='send_marked'").fetchone()[0],1)
        self.assertEqual(c.execute("SELECT request_attempts FROM provider_usage ORDER BY id DESC LIMIT 1").fetchone()[0],1)
        self.assertEqual(c.execute("SELECT count(*) FROM content_metric_snapshots WHERE content_id=82078 AND window_key=?", (stage['window_key'],)).fetchone()[0],1)
        self.assertEqual(c.execute('SELECT count(*) FROM capture_work_items').fetchone()[0],0)
        with storage.transaction(c):
            row = c.execute('SELECT id,details_json FROM scheduler_runs WHERE job_id=?', (fixed.JOB,)).fetchone()
            details = json.loads(row['details_json']); details['checkpoint']['result']['raw_response_id'] = 999999
            c.execute('UPDATE scheduler_runs SET details_json=? WHERE id=?', (json.dumps(details),row['id']))
        with self.assertRaises(fixed.FixedRepairRejected):
            fixed.run_stage(plan,'dy_detail_metrics',db_path=case.f.db)

    def test_closed_current_gate_blocks_before_send_and_ungranted_retry_rejected(self):
        case, plan = self.fixture(); c = case.f.connection
        with storage.transaction(c):
            row = dict(c.execute("SELECT * FROM capture_paid_send_gate_events WHERE operation='douyin_video_detail' ORDER BY id DESC LIMIT 1").fetchone())
            values = {k:row[k] for k in ('provider','operation','state','reason','evidence_json','recorded_at')}
            values.update(state='closed',reason='offline revoked',recorded_at=storage.now_utc())
            c.execute('INSERT INTO capture_paid_send_gate_events('+','.join(values)+',event_sha256) VALUES('+','.join('?' for _ in range(len(values)+1))+')',
                (*values.values(),fixed.capture_authorizations.digest(values)))
        with patch.object(providers,'_load_key',return_value='offline'), patch.object(providers,'_freeze_tikhub_transport',return_value=None), \
                patch.object(providers,'_content_call',side_effect=AssertionError('closed gate sent')):
            with self.assertRaises(Exception): fixed.run_stage(plan,'dy_detail_metrics',db_path=case.f.db)
        with self.assertRaises(Exception): fixed.run_authorized_retry(plan,'dy_detail_metrics',work_id=999999,db_path=case.f.db)
        self.assertEqual(c.execute("SELECT count(*) FROM paid_provider_dispatch_events WHERE event_type='send_marked'").fetchone()[0],0)

    def test_cancellation_after_real_A_reservation_prevents_B_send(self):
        case, plan = self.fixture(); c = case.f.connection; cancelled = Event()
        original = capture._mark_paid_sent
        def cancel_before_B(*args, **kwargs):
            self.assertEqual(c.execute("SELECT count(*) FROM admission_reservations WHERE state='reserved_unsent'").fetchone()[0], 1)
            cancelled.set()
            return original(*args, **kwargs)
        with patch.object(providers,'_load_key',return_value='offline'), patch.object(providers,'_freeze_tikhub_transport',return_value=None), \
                patch.object(providers,'_content_call',side_effect=AssertionError('cancelled request sent')), \
                patch.object(capture,'_mark_paid_sent',side_effect=cancel_before_B):
            with self.assertRaises(fixed.FixedRepairRejected):
                fixed.run_stage(plan,'dy_detail_metrics',db_path=case.f.db,cancelled=cancelled)
        self.assertEqual(c.execute("SELECT count(*) FROM paid_provider_dispatch_events WHERE event_type='send_marked'").fetchone()[0],0)
        self.assertEqual(c.execute('SELECT sum(request_attempts) FROM provider_usage').fetchone()[0],0)

    def test_four_first_metrics_reach_workers_together_and_extra_stats_follows(self):
        barrier = Barrier(4); lock = Lock(); seen = []; completed = []
        def run(plan,key,**kwargs):
            with lock: seen.append(key)
            if key in fixed.FIRST_METRICS:
                barrier.wait(timeout=5)
                with lock: completed.append(key)
            if key == 'dy_statistics': self.assertEqual(set(completed),set(fixed.FIRST_METRICS))
            return {'stage_key':key,'status':'succeeded','provider_calls':1}
        with patch.object(fixed,'run_stage',side_effect=run):
            result = fixed.run_fixed_stages({},db_path=None)
        self.assertEqual(seen[0],'xhs_detail'); self.assertEqual(seen[-1],'dy_statistics')
        self.assertEqual(len(result),6)


if __name__ == '__main__': unittest.main()
