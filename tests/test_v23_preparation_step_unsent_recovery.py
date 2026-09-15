"""Step-scoped unsent recovery and the real A/B cleanup/runtime retry boundary.

All databases, raw entities and dispatch ledgers are disposable. No HTTP is
allowed; installed policy/authority and quote are explicit fixture boundaries.
"""
from __future__ import annotations

from contextlib import contextmanager, ExitStack
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from tests import test_v8_preparation_never_sent_recovery as legacy
from tests import test_v8_intake_capture as raw_fixture
from v8 import account_intake, account_preparation as prep, capture, capture_runtime as runtime
from v8 import paid_dispatch, providers, usage_settlements, durable_runs
from v8 import provider_budget
from v8.storage import connect, initialize_database, transaction


@contextmanager
def fixture(value=None):
    f = legacy.NeverSentPreparationRecoveryTest()
    real_submit = account_intake.submit_account_intake
    def submit(connection, **kwargs):
        if value is not None:kwargs['value'] = value
        return real_submit(connection, **kwargs)
    try:
        with patch.object(legacy, 'initialize_database', side_effect=lambda c, **kw:initialize_database(c,target_version=23)), \
             patch.object(account_intake, 'submit_account_intake', side_effect=submit):
            f.setUp()
        assert f.db.execute('PRAGMA user_version').fetchone()[0] == 23
        f.work=dict(f.db.execute('SELECT * FROM capture_work_items WHERE id=?',(f.work['id'],)).fetchone())
        yield f
    finally:f.doCleanups()


def immutable_ledger(c):
    return {name:[tuple(r) for r in c.execute('SELECT * FROM '+name+' ORDER BY rowid')] for name in (
        'fetch_attempts','provider_raw_responses','fetch_transport_receipts','provider_usage',
        'paid_provider_dispatch_events','fetch_request_batches','fetch_request_batch_members',
        'admission_reservations','provider_usage_settlements','provider_usage_settlement_events','provider_paid_scope_claims','provider_request_start_events')}


def new_dispatch_owner(f):
    c=f.db
    f.base.run_id=c.execute("INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,details_json) VALUES('step-fixture',?,'running',?,'{}')",(str(c.total_changes),f.at)).lastrowid
    f.base.attempt_id=c.execute("INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,invocation_source,status,started_at,details_json) VALUES(?,1,'scheduled','running',?,'{}')",(f.base.run_id,f.at)).lastrowid


def seed_current_failed_step(f):
    """A second, distinct preparation step; previous rows are retained."""
    c=f.db
    f.work=dict(c.execute('SELECT * FROM capture_work_items WHERE intake_request_id=? ORDER BY id DESC LIMIT 1',(f.intake_id,)).fetchone())
    f.envelope=json.loads(f.work['envelope_json']);f.target=f.envelope['request']
    f.price_micro=provider_budget.PRICES_MICROUSD[f.target['operation']]
    f.budget_id=provider_budget.task_budget_id('never-sent-fixture','TikHub',f.target['operation'])
    c.execute("INSERT INTO provider_budget_batches(id,purpose,provider,operation,currency,verified_unit_price,max_billable_requests,max_amount,pilot_size,daily_quota,price_verified_at,status,created_at,updated_at) VALUES(?,?,'TikHub',?,'USD',?,1000,10,0,1000,?,'approved',?,?)",(f.budget_id,f.budget_id,f.target['operation'],f.price_micro/1000000,f.at,f.at,f.at))
    f.identity=providers._paid_request_identity(operation=f.target['operation'],platform=f.target['platform'],
        subject=f.target['subject'],params=f.target['params'],cursor=None,due_bucket=f.envelope['logical_due'])
    f.slot=capture.ensure_intake_slot(c,intake_request_id=f.intake_id,stage='profile_prepare',window_key=f.envelope['logical_due'],provider='TikHub',adapter_version='fixture')
    f.batch=c.execute("INSERT INTO fetch_request_batches(request_scope_identity,sequence,provider,operation,parameters_json,created_at) VALUES(?,0,'tikhub',?,?,?)",
        (f.identity.scope_identity,f.target['operation'],prep.planning.canonical(f.target['params']),f.at)).lastrowid
    f.member_scope=usage_settlements.member_identity(f.identity.document)
    f.member=c.execute('INSERT INTO fetch_request_batch_members(batch_id,member_scope_identity,sequence,intake_request_id) VALUES(?,?,0,?)',(f.batch,f.member_scope,f.intake_id)).lastrowid
    c.execute("INSERT INTO admission_reservations(batch_id,state,amount_microusd,charge_business_day,created_at,expires_at,updated_at) VALUES(?,'released_unsent',?,?,?,?,?)",
        (f.batch,f.price_micro,provider_budget.budget_day(f.at),f.at,f.at,f.at))
    new_dispatch_owner(f)
    f.dispatches=[f.add_dispatch(),f.add_dispatch()]
    c.execute("UPDATE scheduler_run_attempts SET status='partial',completed_at=? WHERE id=?",(f.at,f.base.attempt_id))
    c.execute("UPDATE fetch_slots SET status='terminal_failed',attempt_count=0,last_error_code='paid_identity_hold',last_error_message='batch reservation expired or changed' WHERE id=?",(f.slot,))
    c.execute("UPDATE capture_work_items SET state='paid_identity_hold',reason='paid_identity_hold' WHERE id=?",(f.work['id'],))
    f.work=dict(c.execute('SELECT * FROM capture_work_items WHERE id=?',(f.work['id'],)).fetchone())


def complete_resolver(f):
    """Store complete actual-format resolver bytes, with a singleton NULL slot attempt."""
    c=f.db;op=f.target['operation'];assert op=='wechat_channels_resolve'
    value={'code':200,'router':f.target['path'],'params':f.target['params'],
           'data':{'ret':0,'data':[{'items':[{'accTypeName':'视频号','jumpInfo':{'userName':'v2_012345abcdef@finder'}}]}]}}
    response=raw_fixture.IntakeCaptureTest.response(f,value)
    new_dispatch_owner(f)
    reserved=f.add_dispatch(closed=False,state='reserved',amount=.01)
    attempt=c.execute("INSERT INTO fetch_attempts(slot_id,request_batch_id,attempt_number,request_started_at,http_status,billed) VALUES(NULL,?,1,?,200,1)",(f.batch,f.at)).lastrowid
    paid_dispatch.mark_dispatch_sent_in_transaction(c,reserved.dispatch_id,fetch_attempt_id=attempt,created_at=f.at)
    claim=capture.SlotClaim(slot_id=f.slot,attempt_id=attempt,attempt_number=1,content_id=None,intake_request_id=f.intake_id,
        stage='profile_prepare',window_key=f.envelope['logical_due'],provider='TikHub',adapter_version='fixture',
        paid_scope_identity=f.identity.scope_identity,request_batch_id=f.batch,singleton_batch=True)
    raw=capture._store_raw_response(c,claim=claim,operation=op,value=value,http_status=200,
        raw_root=Path(f.base.temp.name).resolve()/'raw',entity_bytes=response.entity_body,transport_receipt=response.receipt)
    paid_dispatch.finish_dispatch_in_transaction(c,reserved.dispatch_id,outcome='succeeded',raw_response_id=raw,created_at=f.at)
    usage=c.execute('SELECT details_json FROM provider_usage WHERE id=?',(reserved.provider_usage_id,)).fetchone()
    details=json.loads(usage[0]);details.update(state='charged_unverified',sent_at=f.at)
    c.execute('UPDATE provider_usage SET request_attempts=1,billed_requests=1,details_json=? WHERE id=?',(prep.planning.canonical(details),reserved.provider_usage_id))
    settlement=usage_settlements.record_settlement(c,usage_id=reserved.provider_usage_id,at=f.at)
    assert settlement['state']=='charged_unverified'
    c.execute("UPDATE scheduler_run_attempts SET status='partial',completed_at=? WHERE id=?",(f.at,f.base.attempt_id))
    c.execute("UPDATE fetch_slots SET status='succeeded',attempt_count=1,last_error_code=NULL,last_error_message=NULL WHERE id=?",(f.slot,))
    c.execute("UPDATE capture_work_items SET state='terminal',reason='',completed_at=? WHERE id=?",(f.at,f.work['id']))
    result=json.loads(f.request()['result_json']);result['preparation_responses']=[{'operation':op,'raw_response_id':raw}]
    c.execute('UPDATE account_intake_requests SET result_json=? WHERE id=?',(prep.planning.canonical(result),f.intake_id))
    return raw


class PreparationStepUnsentTest(unittest.TestCase):
    def test_all_four_platforms_recover_same_step_once_without_touching_paid_ledger(self):
        cases=[{'platform':'douyin','uid':'123456789'},
               {'platform':'douyin','display_account_id':'fixtureid'},
               {'platform':'xiaohongshu','uid':'64abcdef0123456789abcdef'},
               {'platform':'kuaishou','uid':'987654321'},
               {'platform':'wechat_channels','display_account_id':'sphfixture'},
               {'platform':'wechat_channels','uid':'v2_012345abcdef@finder'}]
        for value in cases:
            with self.subTest(platform=value),fixture(value) as f:
                before=immutable_ledger(f.db);original=dict(f.work)
                proof=f.proof();self.assertIsNotNone(proof)
                with patch.object(prep,'readiness',return_value=('runnable','')) as readiness:
                    result=prep.enqueue_pending(f.db,active=f.active,at=f.at)
                self.assertEqual((result['created'],result['recovered_unsent']),(0,1));readiness.assert_called()
                work=dict(f.db.execute('SELECT * FROM capture_work_items WHERE id=?',(f.work['id'],)).fetchone())
                for key in ('id','work_identity','envelope_json','data_business_day','due_at','attempt_count'):
                    self.assertEqual(work[key],original[key])
                self.assertEqual(work['state'],'runnable');self.assertEqual(immutable_ledger(f.db),before)
                writes=f.db.total_changes
                prep.enqueue_pending(f.db,active=f.active,at=f.at)
                self.assertEqual(f.db.total_changes,writes)

    def test_wechat_channel_step_keeps_prior_resolver_raw_and_unverified_charge(self):
        with fixture({'platform':'wechat_channels','display_account_id':'sphfixture'}) as f:
            raw=complete_resolver(f)
            self.assertEqual(prep.enqueue_pending(f.db,active=f.active,at=f.at)['created'],1)
            seed_current_failed_step(f);self.assertEqual(f.target['operation'],'wechat_channels_channel_info')
            saved=json.loads(f.request()['result_json'])['preparation_responses'];before=immutable_ledger(f.db)
            with patch.object(prep,'readiness',return_value=('runnable','')):
                result=prep.enqueue_pending(f.db,active=f.active,at=f.at)
            self.assertEqual(result['recovered_unsent'],1);self.assertEqual(result['created'],0)
            self.assertEqual(json.loads(f.request()['result_json'])['preparation_responses'],saved)
            self.assertEqual(immutable_ledger(f.db),before)
            self.assertTrue(capture.raw_archive.read_response_entity(f.db,raw))
            self.assertEqual(f.db.execute('SELECT count(*) FROM provider_usage_settlements').fetchone()[0],1)
            self.assertEqual(f.db.execute("SELECT count(*) FROM provider_usage WHERE request_attempts=1 AND json_extract(details_json,'$.state')='charged_unverified'").fetchone()[0],1)
            self.assertEqual(f.db.execute('SELECT count(*) FROM capture_work_items WHERE intake_request_id=?',(f.intake_id,)).fetchone()[0],2)

    def test_changed_target_window_or_existing_scope_evidence_cannot_recover(self):
        for mutate in ('window','target','owner','attempt','singleton_attempt','unknown_usage','open_dispatch','send','exclusion','admission','amount','day','expiry','raw','missing_sequence','sequence_type','missing_document','missing_execution','wrong_execution','missing_event_execution','wrong_event_execution'):
            with self.subTest(mutate=mutate),fixture({'platform':'kuaishou','uid':'987654321'}) as f:
                c=f.db
                if mutate in ('window','target'):
                    env=dict(f.envelope)
                    if mutate=='window':env['logical_due']+=':different'
                    else:env['request']={**f.target,'subject':'111111'}
                    c.execute('UPDATE capture_work_items SET envelope_json=? WHERE id=?',(prep.planning.canonical(env),f.work['id']))
                elif mutate=='owner':c.execute("UPDATE capture_work_items SET owner_token='another-owner',state='running' WHERE id=?",(f.work['id'],))
                elif mutate=='attempt':c.execute('INSERT INTO fetch_attempts(slot_id,attempt_number,request_started_at) VALUES(?,1,?)',(f.slot,f.at))
                elif mutate=='singleton_attempt':c.execute('INSERT INTO fetch_attempts(slot_id,request_batch_id,attempt_number,request_started_at) VALUES(NULL,?,1,?)',(f.batch,f.at))
                elif mutate=='unknown_usage':f.add_dispatch(state='billing_unknown',attempts=1)
                elif mutate in ('open_dispatch','send'):
                    new_dispatch_owner(f)
                    event=f.add_dispatch(closed=False)
                    if mutate=='send':paid_dispatch.mark_dispatch_sent_in_transaction(c,event.dispatch_id,fetch_attempt_id=None,created_at=f.at)
                    c.execute("UPDATE scheduler_run_attempts SET status='partial',completed_at=? WHERE id=?",(f.at,f.base.attempt_id))
                elif mutate=='exclusion':c.execute("INSERT INTO legacy_paid_scope_exclusions(scope_identity,evidence_sha256,evidence_ref,reason,created_at) VALUES(?,?,'fixture','fixture',?)",(f.member_scope,'e'*64,f.at))
                elif mutate=='admission':c.execute("UPDATE admission_reservations SET state='sent_unsettled'")
                elif mutate=='amount':c.execute('UPDATE admission_reservations SET amount_microusd=amount_microusd+1')
                elif mutate=='day':c.execute("UPDATE admission_reservations SET charge_business_day='2000-01-01'")
                elif mutate=='expiry':c.execute("UPDATE admission_reservations SET expires_at='2099-01-01T00:00:00Z'")
                elif mutate in ('missing_sequence','sequence_type','missing_document','missing_execution','wrong_execution'):
                    usage=c.execute('SELECT id,details_json FROM provider_usage ORDER BY id DESC LIMIT 1').fetchone()
                    details=json.loads(usage['details_json'])
                    if mutate=='missing_sequence':details.pop('paid_sequence')
                    elif mutate=='sequence_type':details['paid_sequence']=False
                    elif mutate=='missing_execution':details.pop('paid_execution_identity')
                    elif mutate=='wrong_execution':details['paid_execution_identity']='f'*64
                    else:details.pop('paid_identity')
                    c.execute('UPDATE provider_usage SET details_json=? WHERE id=?',(prep.planning.canonical(details),usage['id']))
                elif mutate in ('missing_event_execution','wrong_event_execution'):
                    new_dispatch_owner(f)
                    usage_id=c.execute('SELECT id FROM provider_usage ORDER BY id DESC LIMIT 1').fetchone()[0]
                    cursor={'paid_scope_identity':f.identity.scope_identity,'sequence':0,'request':f.identity.document}
                    if mutate=='wrong_event_execution':cursor['paid_execution_identity']='f'*64
                    event=paid_dispatch.reserve_dispatch_in_transaction(c,provider='TikHub',operation=f.target['operation'],
                        activation_id=f.base.activation_id,business_day=provider_budget.budget_day(f.at),
                        scheduler_run_id=f.base.run_id,scheduler_attempt_id=f.base.attempt_id,
                        scope={'purpose':'reconcile',**{key:f.envelope[key] for key in prep.SCOPE_FIELDS}},
                        provider_usage_id=usage_id,fetch_slot_id=f.slot,cursor_identity=cursor,created_at=f.at)
                    paid_dispatch.close_dispatch_not_sent_in_transaction(c,event.dispatch_id,reason='fixture not sent',created_at=f.at)
                    c.execute("UPDATE scheduler_run_attempts SET status='partial',completed_at=? WHERE id=?",(f.at,f.base.attempt_id))
                elif mutate=='raw':
                    # A legacy raw may lack a current attempt binding. The
                    # exact paid scope alone must still prevent reopening.
                    c.execute("INSERT INTO provider_raw_responses(provider,operation,paid_scope_identity,sequence,intake_request_id,captured_at,sha256,local_path,byte_size) VALUES('TikHub',?,?,0,?,?,?,?,0)",(f.target['operation'],f.identity.scope_identity,f.intake_id,f.at,'a'*64,'fixture-unbound'))
                before=c.total_changes;self.assertIsNone(f.proof());self.assertEqual(c.total_changes,before)

    def test_recovery_rechecks_current_readiness_and_does_not_override_block(self):
        with fixture({'platform':'kuaishou','uid':'987654321'}) as f:
            with patch.object(prep,'readiness',return_value=('provider_blocked','provider_transport_blocked')):
                result=prep.enqueue_pending(f.db,active=f.active,at=f.at)
            self.assertEqual(result['recovered_unsent'],1)
            self.assertEqual(tuple(f.db.execute('SELECT state,reason FROM capture_work_items WHERE id=?',(f.work['id'],)).fetchone()),
                             ('provider_blocked','provider_transport_blocked'))
            self.assertEqual(f.db.execute('SELECT status FROM fetch_slots WHERE id=?',(f.slot,)).fetchone()[0],'retryable_failed')

    def test_old_charge_day_does_not_mask_a_closure_after_midnight(self):
        with fixture({'platform':'kuaishou','uid':'987654321'}) as f:
            after=(datetime.fromisoformat(f.at.replace('Z','+00:00'))+timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
            event=f.add_dispatch(closed=False)
            paid_dispatch.close_dispatch_not_sent_in_transaction(f.db,event.dispatch_id,reason='fixture expired',created_at=after)
            f.db.execute('UPDATE admission_reservations SET updated_at=? WHERE batch_id=?',(after,f.batch))
            self.assertIsNone(f.proof())

    def test_public_reason_is_queued_only_for_pure_expiry(self):
        request={'id':1,'result_json':'{}'}
        for reason,state in [('batch_reservation_expired','queued'),('paid_identity_hold','blocked'),('billing_unknown','blocked')]:
            result=account_intake.preparation_status(request,{'state':'provider_blocked','reason':reason,'due_at':'later'})
            self.assertEqual(result['state'],state)
            if reason=='batch_reservation_expired':self.assertEqual(result['reason_label'],'发送前预占已过期，等待原任务自动重试')


class PreparationExpiryRuntimeTest(unittest.TestCase):
    def runtime_attempt(self, f, *, mutation='expired', call_time=None):
        """Real run_one, durable claim, paid A/B, cleanup, and retry checkpoint.

        Only installation/policy and a verified quote are fixtures. SQL slot and
        batch checks, dispatch append, usage settlement and run finish are real.
        """
        budget_id=provider_budget.task_budget_id('step','TikHub',f.target['operation'])
        c=f.db;now=call_time or f.at;price=provider_budget.PRICES_MICROUSD[f.target['operation']]/1000000
        with transaction(c):
            c.execute("INSERT OR IGNORE INTO provider_budget_batches(id,purpose,provider,operation,currency,verified_unit_price,max_billable_requests,max_amount,pilot_size,daily_quota,price_verified_at,status,created_at,updated_at) VALUES(?,?,'TikHub',?,'USD',?,1000,10,0,1000,?,'approved',?,?)",
                (budget_id,budget_id,f.target['operation'],price,now,now,now))
        def reserve(connection,**kw):
            details={'state':'reserved','budget_day':provider_budget.budget_day(now),'scope':capture._paid_scope_payload(kw['dispatch_scope'])}
            usage=connection.execute("INSERT INTO provider_usage(task_id,budget_batch_id,provider,operation,request_attempts,billed_requests,currency,amount,recorded_at,details_json) VALUES('step',?,'TikHub',?,0,1,'USD',?,?,?)",
                (budget_id,f.target['operation'],price,now,prep.planning.canonical(details))).lastrowid
            connection.execute("UPDATE provider_budget_batches SET consumed_amount=consumed_amount+?,consumed_requests=consumed_requests+1 WHERE id=?",(price,budget_id))
            return usage,price,'USD'
        def frozen_scope(connection,**kw):
            return replace(kw.get('scope') or provider_budget._SCOPE.get(),platform=f.target['platform'],preparation_subject=f.target['subject'])
        def route(connection,**kw):
            row=connection.execute('SELECT * FROM capture_route_assignments WHERE id=?',(f.envelope['assignment_id'],)).fetchone()
            self.assertEqual(row['intake_request_id'],f.intake_id);self.assertEqual(row['operation'],kw['operation'])
            return dict(row)
        def execute(envelope,**kw):
            claim=capture._claim_paid_tikhub(content_id=None,account_id=None,intake_request_id=f.intake_id,
                stage='profile_prepare',window_key=envelope['logical_due'],provider='TikHub',adapter_version='fixture',
                operation=f.target['operation'],db_path=f.base.db,budget_id=budget_id,task_id='step',task_max_amount=10,
                allow_terminal_retry=False,paid_request_identity=f.identity)
            self.last_claim=claim
            with connect(f.base.db) as connection,transaction(connection):
                # Emulate the queue consuming its TTL while the durable owner
                # still has a fresh lease. Never change a formal admission.
                earlier=(datetime.fromisoformat(now.replace('Z','+00:00'))-timedelta(seconds=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
                connection.execute('UPDATE admission_reservations SET expires_at=? WHERE batch_id=?',(earlier,claim.request_batch_id))
                if mutation=='amount':connection.execute('UPDATE admission_reservations SET amount_microusd=amount_microusd+1 WHERE batch_id=?',(claim.request_batch_id,))
                elif mutation=='day':connection.execute("UPDATE admission_reservations SET charge_business_day='2000-01-01' WHERE batch_id=?",(claim.request_batch_id,))
                elif mutation=='state':connection.execute("UPDATE admission_reservations SET state='sent_unsettled' WHERE batch_id=?",(claim.request_batch_id,))
                elif mutation in ('metadata_identity','metadata_scope','metadata_sequence','metadata_execution'):
                    usage=connection.execute('SELECT details_json FROM provider_usage WHERE id=?',(claim.reserved_usage_id,)).fetchone()
                    detail=json.loads(usage[0]);key={'metadata_identity':'paid_identity','metadata_scope':'paid_scope_identity','metadata_sequence':'paid_sequence','metadata_execution':'paid_execution_identity'}[mutation]
                    detail[key]=1 if key=='paid_sequence' else {} if key=='paid_identity' else 'f'*64
                    connection.execute('UPDATE provider_usage SET details_json=? WHERE id=?',(prep.planning.canonical(detail),claim.reserved_usage_id))
                elif mutation=='scope_exclusion':connection.execute("INSERT INTO legacy_paid_scope_exclusions(scope_identity,evidence_sha256,evidence_ref,reason,created_at) VALUES(?,?,'fixture','fixture',?)",(claim.paid_scope_identity,'e'*64,now))
                elif mutation=='prior_send_claim':
                    prior_attempt=connection.execute('INSERT INTO fetch_attempts(slot_id,request_batch_id,attempt_number,request_started_at) VALUES(NULL,?,1,?)',(claim.request_batch_id,now)).lastrowid
                    marked=paid_dispatch.mark_dispatch_sent_in_transaction(connection,claim.dispatch_id,fetch_attempt_id=prior_attempt,created_at=now)
                    connection.execute('UPDATE provider_usage SET request_attempts=1 WHERE id=?',(claim.reserved_usage_id,))
                    usage_settlements.claim_paid_scope(connection,identity=claim.paid_scope_identity,marker_id=marked.event_id,scope_kind='request',sequence=0,at=now)
                    # A damaged mutable usage projection cannot erase the
                    # append-only send marker and purchased scope.
                    connection.execute('UPDATE provider_usage SET request_attempts=0 WHERE id=?',(claim.reserved_usage_id,))
            return capture._execute_claimed_fetch(claim=claim,operation=f.target['operation'],
                call=lambda:self.fail('expired reservation sent HTTP'),db_path=f.base.db,budget_id=budget_id,task_id='step',task_max_amount=10)
        checked={'budget_day':provider_budget.budget_day(now),'borrowed_from':None,'borrowing_proofs':[], 'validated_work_fingerprint':'fixture'}
        with ExitStack() as stack:
            for context in (
                patch.object(runtime,'activation_at',return_value=f.active),
                patch.object(runtime,'now_utc',return_value=now),patch.object(capture,'now_utc',return_value=now),
                patch.object(runtime,'_readiness',return_value=('runnable','')),
                patch.object(runtime,'_execute_one',side_effect=execute),
                patch.object(capture,'freeze_scope',side_effect=frozen_scope),
                patch.object(capture,'_reserve_budget',side_effect=reserve),
                patch.object(capture,'check_reservation',return_value=checked),
                patch('v8.capture_planning.require_send_route',side_effect=route),
                patch('v8.capture_authorizations.current_runtime_bindings',return_value={}),
                patch('v8.capture_authorizations.validate_authorization',return_value={'authority_sha256':'c'*64}),
            ):stack.enter_context(context)
            return runtime.run_one(f.base.db,at=now)

    def ready(self,f):
        with patch.object(prep,'readiness',return_value=('runnable','')):
            # This fixture's terminal legacy admission is replaced by a fresh
            # normal A reservation below; proof/recovery has separate tests.
            f.db.execute("UPDATE fetch_slots SET status='pending' WHERE id=?",(f.slot,))
            f.db.execute("UPDATE capture_work_items SET state='runnable',reason='' WHERE id=?",(f.work['id'],))
        f.db.commit()

    def test_pure_expiry_real_cleanup_and_run_one_defer_same_work_and_readmit_same_slot(self):
        with fixture({'platform':'xiaohongshu','uid':'64abcdef0123456789abcdef'}) as f:
            self.ready(f);before=(f.work['id'],f.slot,f.batch,f.envelope['logical_due'],f.identity.scope_identity)
            first=self.runtime_attempt(f)
            self.assertEqual((first['status'],first['reason']),('provider_blocked','batch_reservation_expired'))
            work=dict(f.db.execute('SELECT * FROM capture_work_items WHERE id=?',(f.work['id'],)).fetchone())
            self.assertGreater(work['due_at'],prep.planning.timestamp(f.at));self.assertIsNone(work['owner_token'])
            checkpoint=json.loads(f.db.execute("SELECT details_json FROM scheduler_runs WHERE job_id=? ORDER BY id DESC LIMIT 1",(runtime.JOB,)).fetchone()[0])['checkpoint']
            self.assertEqual(checkpoint['capture_consecutive_failures'],1)
            self.assertEqual(runtime._time(work['due_at'])-runtime._time(f.at),runtime.retry_backoff(1))
            slot=f.db.execute('SELECT * FROM fetch_slots WHERE id=?',(f.slot,)).fetchone()
            self.assertEqual((slot['status'],slot['attempt_count'],slot['last_error_code']),('pending',0,'batch_reservation_expired'))
            self.assertEqual(f.db.execute('SELECT state FROM admission_reservations WHERE batch_id=?',(f.batch,)).fetchone()[0],'released_unsent')
            self.assertEqual((self.last_claim.slot_id,self.last_claim.request_batch_id,self.last_claim.paid_scope_identity),(f.slot,f.batch,f.identity.scope_identity))
            for table in ('fetch_attempts','provider_raw_responses','provider_paid_scope_claims'):
                self.assertEqual(f.db.execute('SELECT count(*) FROM '+table).fetchone()[0],0)
            latest=paid_dispatch.dispatch_events(f.db,self.last_claim.dispatch_id)
            self.assertEqual([e.event_type for e in latest],['reserved','not_sent'])
            usage=f.db.execute('SELECT * FROM provider_usage WHERE id=?',(self.last_claim.reserved_usage_id,)).fetchone()
            self.assertEqual((usage['amount'],usage['request_attempts'],usage['billed_requests']),(0,0,0))
            self.assertEqual(json.loads(usage['details_json'])['state'],'not_sent')
            # The normal due reconsideration takes a fresh readiness pass;
            # it does not call the preparation legacy-HOLD repair helper.
            later=work['due_at'].replace('+00:00','Z')
            with transaction(f.db),patch.object(runtime,'_readiness',return_value=('runnable','')):
                runtime._plan_due(f.db,{'id':f.work['source_plan_id'],'cohort':[]},at=later)
            second=self.runtime_attempt(f,call_time=later)
            self.assertEqual((second['status'],second['reason']),('provider_blocked','batch_reservation_expired'))
            next_work=dict(f.db.execute('SELECT * FROM capture_work_items WHERE id=?',(f.work['id'],)).fetchone())
            self.assertGreater(next_work['due_at'],work['due_at']);self.assertEqual(next_work['envelope_json'],work['envelope_json'])
            checkpoint=json.loads(f.db.execute("SELECT details_json FROM scheduler_runs WHERE job_id=? ORDER BY id DESC LIMIT 1",(runtime.JOB,)).fetchone()[0])['checkpoint']
            self.assertEqual(checkpoint['capture_consecutive_failures'],2)
            self.assertEqual(runtime._time(next_work['due_at'])-runtime._time(later),runtime.retry_backoff(2))
            self.assertEqual(runtime.retry_backoff(2),runtime.retry_backoff(1)*2)
            self.assertEqual(before,(next_work['id'],self.last_claim.slot_id,self.last_claim.request_batch_id,f.envelope['logical_due'],self.last_claim.paid_scope_identity))
            self.assertEqual(f.db.execute("SELECT count(*) FROM fetch_dead_letters WHERE work_id=?",(f.work['id'],)).fetchone()[0],0)

    def test_amount_day_or_state_change_remain_hold_through_real_cleanup(self):
        for mutation in ('amount','day','state','metadata_identity','metadata_scope','metadata_sequence','metadata_execution','scope_exclusion','prior_send_claim'):
            with self.subTest(mutation=mutation),fixture({'platform':'kuaishou','uid':'987654321'}) as f:
                self.ready(f);result=self.runtime_attempt(f,mutation=mutation)
                self.assertEqual(result['status'],'paid_identity_hold');self.assertIn('paid_identity_hold',result['reason'])
                self.assertIn(f.db.execute('SELECT status FROM fetch_slots WHERE id=?',(f.slot,)).fetchone()[0],('terminal_failed','running') if mutation=='prior_send_claim' else ('terminal_failed',))
                self.assertEqual(f.db.execute('SELECT count(*) FROM fetch_attempts').fetchone()[0],1 if mutation=='prior_send_claim' else 0)
                self.assertEqual(f.db.execute('SELECT sum(request_attempts) FROM provider_usage').fetchone()[0],0)


if __name__=='__main__':unittest.main()
