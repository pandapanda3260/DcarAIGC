"""Real schema23 durable owners and local recovery; no network or paid replay."""
import gzip
import hashlib
import http.client
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from tests import test_v23_media_source_refresh as fixture_module
from tests.test_v8_kuaishou_adapter import detail_payload
from tests.test_v8_provider_transport import FakeOpener, FakeResponse, URL, clock
from tests.roster_fixture import accept_roster
from v8 import capture, capture_planning as planning, capture_singletons, durable_runs, paid_dispatch
from v8 import capture_transport_recovery as recovery, provider_budget, providers, provider_transport, raw_archive, storage
from v8.paid_drain import dispatch_state


class ContentEntityRecoveryTest(unittest.TestCase):
    def setUp(self):
        fixture = self.fixture = fixture_module.MediaSourceRefreshTest(); fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.db = fixture.db; self.root = self.db.parent
        self.enterContext(patch('socket.socket.connect', side_effect=AssertionError('network forbidden')))
        at = storage.now_utc(); subject = '5234567890123456789'; op = 'kuaishou_video_detail'
        with storage.connect(self.db) as c, storage.transaction(c):
            c.execute("UPDATE account_platform_identities SET platform='kuaishou',uid='001234' WHERE account_id=1")
            c.execute("UPDATE content_items SET platform='kuaishou',platform_content_id=?,raw_account_uid='001234',canonical_url=? WHERE id=1",
                      (subject, 'https://www.kuaishou.com/short-video/'+subject))
            accept_roster(c, accepted_at=at)
            activation = dispatch_state(c, at=at).activation_id
            identity = providers._paid_request_identity(operation=op, platform='kuaishou', subject=subject,
                params={'photo_id':subject}, cursor=None, due_bucket='lifetime')
            assignment = c.execute("INSERT INTO capture_route_assignments(scope_type,scope_key,provider,operation,content_id,generation,route,mode,effective_at,recorded_at,assignment_sha256) VALUES('content','fixture','tikhub',?,1,1,'integrated','active',?,?,?)",
                (op, at, at, 'b'*64)).lastrowid
            work_identity = 'c'*64
            self.work = c.execute("INSERT INTO capture_work_items(work_identity,assignment_id,account_id,content_id,provider,operation,due_at,data_business_day,state,reason,envelope_json,created_at,updated_at) VALUES(?,?,1,1,'tikhub',?,?,'2026-09-12','paid_identity_hold','paid_identity_hold:transport_error',?,?,?)",
                (work_identity, assignment, op, at, json.dumps({'task_id':'original-paid-content'}), at, at)).lastrowid
            original = durable_runs.claim_run_in_transaction(c,'original-content-work',{'work_identity':work_identity,'business_day':'2026-09-12'},now=at)
            self.slot = c.execute("INSERT INTO fetch_slots(content_id,stage,window_key,provider,adapter_version,status,attempt_count,created_at,updated_at) VALUES(1,'media_source_refresh','lifetime','TikHub','fixture','retryable_failed',1,?,?)",(at,at)).lastrowid
            self.batch = c.execute("INSERT INTO fetch_request_batches(request_scope_identity,sequence,provider,operation,parameters_json,created_at) VALUES(?,0,'tikhub',?,?,?)",(identity.scope_identity,op,planning.canonical({'photo_id':subject}),at)).lastrowid
            from v8.usage_settlements import member_identity
            self.member = c.execute("INSERT INTO fetch_request_batch_members(batch_id,member_scope_identity,sequence,content_id,account_id) VALUES(?,?,0,1,1)",(self.batch,member_identity(identity.document))).lastrowid
            self.attempt = c.execute("INSERT INTO fetch_attempts(slot_id,request_batch_id,attempt_number,request_started_at,response_finished_at,http_status,billed,amount,currency,error_code) VALUES(NULL,?,1,?,?,NULL,0,NULL,'USD','transport_error')",(self.batch,at,at)).lastrowid
            c.execute("INSERT INTO fetch_request_executions(batch_id,fetch_attempt_id,assignment_id,execution_identity,started_at) VALUES(?,?,?,?,?)",(self.batch,self.attempt,assignment,identity.execution_identity,at))
            details={'state':'billing_unknown','error_code':'transport_error','paid_identity':identity.document,
                     'paid_scope_identity':identity.scope_identity,'paid_sequence':0}
            self.usage = c.execute("INSERT INTO provider_usage(task_id,provider,operation,request_attempts,billed_requests,amount,currency,recorded_at,details_json) VALUES('original-paid-content','TikHub',?,1,1,.001,'USD',?,?)",(op,at,json.dumps(details))).lastrowid
            event = paid_dispatch.reserve_dispatch_in_transaction(c,provider='TikHub',operation=op,activation_id=activation,business_day='2026-09-12',scheduler_run_id=original.scheduler_run_id,scheduler_attempt_id=original.attempt_id,scope={'content_id':1},provider_usage_id=self.usage,fetch_slot_id=self.slot,created_at=at)
            paid_dispatch.mark_dispatch_sent_in_transaction(c,event.dispatch_id,fetch_attempt_id=self.attempt,created_at=at)
            paid_dispatch.finish_dispatch_in_transaction(c,event.dispatch_id,outcome='billing_unknown',created_at=at)
            durable_runs.finish_run_in_transaction(c,original,status='interrupted',now=at)
            capture_singletons.record_disposition(c,batch_id=self.batch,attempt_id=self.attempt,raw_response_id=None,disposition='unusable',reason='transport_error',at=at)
            entity=planning.canonical(detail_payload()).encode(); encoded=gzip.compress(entity,mtime=0)
            response=FakeResponse(b'',headers={'Content-Encoding':'gzip'},read_error=http.client.IncompleteRead(encoded,1))
            import urllib.request
            with self.assertRaises(provider_transport.ProviderTransportError) as failed:
                provider_transport.request_json(urllib.request.Request(URL),route_id='fixture',timeout=45,opener=FakeOpener(response),clock=clock((at,at,at)))
            claim=capture.SlotClaim(slot_id=self.slot,attempt_id=self.attempt,attempt_number=1,content_id=1,stage='media_source_refresh',window_key='lifetime',provider='TikHub',adapter_version='fixture',paid_scope_identity=identity.scope_identity,request_batch_id=self.batch,singleton_batch=True)
            receipt=capture._quarantine_transport_evidence(claim=claim,operation=op,raw_root=self.root/'raw',partial=encoded,complete_entity=None,transport_receipt=failed.exception.receipt)
            self.transport=raw_archive.record_transport_receipt(c,attempt_id=self.attempt,receipt=receipt)

    def snapshot(self):
        with storage.connect(self.db) as c:
            return {table: [tuple(row) for row in c.execute('SELECT * FROM '+table)] for table in
                ('fetch_transport_receipts','fetch_request_member_dispositions','paid_provider_dispatch_events','provider_usage','fetch_attempts')}

    def recover(self):
        return recovery.recover_content_entity(db_path=self.db,work_id=self.work,fetch_attempt_id=self.attempt,
                    media_status='media_source_refresh_required',media_root=self.root/'media')

    def test_real_owner_restores_once_without_rewriting_http_or_billing(self):
        before=self.snapshot()
        result=self.recover()
        self.assertEqual(result['phase'],'done')
        self.assertEqual(result['status'],'recovered')
        self.assertEqual(result['media_status'],'media_source_refresh_required')
        self.assertEqual(before,self.snapshot())
        again=self.recover();self.assertTrue(again['idempotent'])
        self.assertEqual(before,self.snapshot())
        with storage.connect(self.db) as c:
            self.assertEqual(tuple(c.execute('SELECT state,reason FROM capture_work_items WHERE id=?',(self.work,)).fetchone()),('terminal','recovered'))
            self.assertEqual(c.execute('SELECT count(*) FROM provider_raw_responses').fetchone()[0],1)
            self.assertEqual(capture_singletons.effective_disposition(c,member_id=self.member,raw_response_id=result['raw_response_id']),'valid_by_entity_recovery')
            receipt=raw_archive.response_entity_integrity(c,result['raw_response_id']);self.assertFalse(receipt['clean_eof'])
            from v8.metric_field_facts import observation_query
            row=c.execute(observation_query(c)+' WHERE o.raw_response_id=?',(result['raw_response_id'],)).fetchone()
            self.assertEqual(row['raw_batch_member_content_id'],1)
            self.assertEqual(row['effective_provider'],'tikhub')
            fact=c.execute("SELECT state,value FROM content_metric_field_facts WHERE observation_id=? AND field='view_count'",(row['id'],)).fetchone()
            self.assertEqual(tuple(fact),('provided',12))

    def test_available_source_is_registered_without_downloading(self):
        before=self.snapshot()
        result=recovery.recover_content_entity(db_path=self.db,work_id=self.work,fetch_attempt_id=self.attempt,
                    media_status='available',media_root=self.root/'media')
        self.assertEqual(result['media_status'],'available')
        self.assertEqual(before,self.snapshot())
        with storage.connect(self.db) as c:
            artifacts=c.execute("SELECT artifact_type,metadata_json FROM evidence_artifacts WHERE content_id=1").fetchall()
            sources=[r for r in artifacts if r['artifact_type']=='media_source']
            self.assertEqual(len(sources),1)
            self.assertEqual(json.loads(sources[0]['metadata_json'])['raw_response_id'],result['raw_response_id'])
            self.assertNotIn('video',[r['artifact_type'] for r in artifacts])

    def test_orphan_blob_before_pending_is_reused_on_resume(self):
        append=recovery._append_phase
        def fail_pending(connection,payload):
            if payload['phase']=='pending':raise RuntimeError('file archived before pending')
            return append(connection,payload)
        with patch.object(recovery,'_append_phase',side_effect=fail_pending):
            with self.assertRaisesRegex(RuntimeError,'file archived'):self.recover()
        with storage.connect(self.db) as c:
            self.assertEqual(c.execute('SELECT count(*) FROM provider_raw_responses').fetchone()[0],0)
        self.recover()
        with storage.connect(self.db) as c:
            self.assertEqual(c.execute('SELECT count(*) FROM provider_raw_responses').fetchone()[0],1)

    def test_wrong_author_or_changed_quarantine_stops_before_materialization(self):
        with storage.connect(self.db) as c,storage.transaction(c):
            c.execute("UPDATE content_items SET raw_account_uid='other-author' WHERE id=1")
        with self.assertRaisesRegex(recovery.EntityRecoveryError,'uniquely bound'):self.recover()
        with storage.connect(self.db) as c,storage.transaction(c):
            c.execute("UPDATE content_items SET raw_account_uid='001234' WHERE id=1")
            quarantine=Path(c.execute('SELECT path FROM transport_quarantine_members WHERE transport_receipt_id=?',(self.transport,)).fetchone()[0])
        body=quarantine.read_bytes();quarantine.write_bytes(body[:-1]+bytes([body[-1]^1]))
        with self.assertRaisesRegex(ValueError,'exact eligible'):self.recover()
        with storage.connect(self.db) as c:
            self.assertEqual(c.execute('SELECT count(*) FROM provider_raw_responses').fetchone()[0],0)

    def test_unrelated_parent_paid_authority_is_not_inherited(self):
        with provider_budget.paid_scope('detail',manual_command_run_id=19):
            with self.assertRaisesRegex(recovery.EntityRecoveryError,'another paid owner'):self.recover()

    def test_business_history_and_transport_circuit_keep_separate_meanings(self):
        from v8 import capture_manual
        with storage.connect(self.db) as c:
            prepared=recovery.prepare_entity_recovery(c,work_id=self.work,fetch_attempt_id=self.attempt)
        at=storage.now_utc()
        with storage.connect(self.db) as c,storage.transaction(c):
            row=c.execute('SELECT details_json FROM provider_usage WHERE id=?',(self.usage,)).fetchone()
            details=json.loads(row[0]);details.update(state='completed',error_code=None,sent_at=at,
                transport=prepared.response.receipt)
            c.execute('UPDATE provider_usage SET recorded_at=?,details_json=? WHERE id=?',(at,json.dumps(details),self.usage))
            self.assertEqual(capture_manual._transport_history(c,'kuaishou_video_detail')[1],[])
            self.assertEqual(provider_budget._transport_window_counts(c,operation='kuaishou_video_detail',at=at),(1,1))
            details['state']='billing_unknown'
            c.execute('UPDATE provider_usage SET details_json=? WHERE id=?',(json.dumps(details),self.usage))
            self.assertEqual(len(capture_manual._transport_history(c,'kuaishou_video_detail')[1]),1)

    def test_pending_resume_has_one_raw_and_new_real_repair_attempt(self):
        before=self.snapshot()
        with patch.object(providers,'_store_stage_result',side_effect=RuntimeError('after pending')):
            with self.assertRaisesRegex(RuntimeError,'after pending'):self.recover()
        self.assertEqual(before,self.snapshot())
        result=self.recover()
        with storage.connect(self.db) as c:
            self.assertEqual(c.execute('SELECT count(*) FROM provider_raw_responses').fetchone()[0],1)
            self.assertEqual(c.execute("SELECT count(*) FROM scheduler_run_attempts a JOIN scheduler_runs r ON r.id=a.scheduler_run_id WHERE r.job_id=?",(recovery.JOB,)).fetchone()[0],2)
            self.assertEqual(c.execute('SELECT count(*) FROM data_quality_receipts WHERE scope_key LIKE ?',(recovery.CONTRACT+':%',)).fetchone()[0],2)
        self.assertEqual(result['provider_calls'],0)

    def test_actual_inner_transaction_rejects_missing_or_expired_repair_owner(self):
        # No assertion is mocked: the actual local application transaction must reject.
        with storage.connect(self.db) as c:
            prepared=recovery.prepare_entity_recovery(c,work_id=self.work,fetch_attempt_id=self.attempt)
        with storage.connect(self.db) as c,storage.transaction(c):
            with self.assertRaisesRegex(recovery.EntityRecoveryError,'owned durable'):
                recovery.assert_local_recovery_owner(c,required=True)
        original=providers._store_stage_result
        def expire(*args,**kwargs):
            with patch.object(recovery, 'now_utc', return_value='2099-01-01T00:00:00Z'):
                return original(*args,**kwargs)
        with patch.object(providers,'_store_stage_result',side_effect=expire):
            with self.assertRaises(durable_runs.LostOwnership):self.recover()
        with storage.connect(self.db) as c:
            self.assertEqual(c.execute('SELECT count(*) FROM content_metric_observations').fetchone()[0],0)
            self.assertEqual(c.execute('SELECT state FROM capture_work_items WHERE id=?',(self.work,)).fetchone()[0],'paid_identity_hold')


if __name__=='__main__':unittest.main()
