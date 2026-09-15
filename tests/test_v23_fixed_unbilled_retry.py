"""Real temporary installed DB and paid ledgers; no network is allowed."""
from contextlib import contextmanager
import hashlib
import json
import unittest
from unittest.mock import patch

from tests import test_v23_capture_repair_fixed as fixed_fixture
from tests.test_v8_kuaishou_adapter import detail_payload
from v8 import capture, capture_repair_fixed as fixed, providers, storage


class FixedUnbilledRetryTest(unittest.TestCase):
    def fixture(self):
        base = fixed_fixture.FixedRepairTest(); self.addCleanup(base.doCleanups)
        case, old = base.fixture(); c = case.f.connection
        with storage.transaction(c):
            c.execute("UPDATE account_platform_identities SET uid='001234' WHERE account_id=(SELECT account_id FROM content_items WHERE id=82116)")
            c.execute("UPDATE content_items SET raw_account_uid='001234',platform_content_id='5234567890123456789' WHERE id=82116")
            fixed.capture_release.publish_operation_gate(c, operation='kuaishou_video_statistics', at=storage.now_utc(), mirror_root=case.f.root/'gates')
        plan = fixed.freeze_plan(c, repair_run_id=old['repair_run_id'],window_key=old['window_key'],authorization=old['authorization'],at=old['issued_at'],expires_at=old['expires_at'])
        return case, plan

    def response(self, *, failed, clean=True, explicit=True):
        payload = {'detail':{'code':400,'router':'/api/v1/kuaishou/app/fetch_one_video',
            'params':{'photo_id':'5234567890123456789'},
            'message':"Request failed. Please retry. You won't be charged for this request." if explicit else 'Invalid parameters'}} if failed else detail_payload()
        body = json.dumps(payload).encode(); status = 400 if failed else 200
        receipt = {'contract_version':'provider-json-transport-v1','transport_route_id':'fixture-route-v1',
            'route_generation':'route-config-sha256:fixture','http_stack':'fixture-stream-v1','request_host':'fixture.invalid',
            'status':'succeeded','error_code':None,'http_status':status,'content_encoding':'identity',
            'content_length':len(body),'clean_eof':clean,'length_match':True,'gzip_crc_ok':None,
            'json_parse_ok':True,'entity_bytes':len(body),'entity_sha256':hashlib.sha256(body).hexdigest(),'zero_body':False,
            'request_started_at':storage.now_utc(),'response_finished_at':storage.now_utc(),'latency_ms':1,
            'http_encoded_bytes':len(body),'http_encoded_sha256':hashlib.sha256(body).hexdigest()}
        if failed:
            raise capture.CaptureError('TikHub HTTP400',retryable=True,error_code='provider_retry_requested',billed=False,
                raw_response=payload,http_status=400,entity_bytes=body,transport_receipt=receipt)
        parsed = providers._parse_content_payload('kuaishou','metrics','5234567890123456789','video',payload,status=200,expected_uid='001234')
        return capture.ProviderResult(parsed.data,payload,200,True,body,receipt)

    @contextmanager
    def provider(self, callback):
        with patch.object(providers,'_load_key',return_value='offline'), patch.object(providers,'_freeze_tikhub_transport',return_value=None), \
                patch.object(providers,'_content_call',side_effect=callback) as mock:
            yield mock

    def fail_first(self, case, plan, **kwargs):
        with self.provider(lambda *a,**k: self.response(failed=True,**kwargs)):
            with self.assertRaises(capture.CaptureError if kwargs.get('clean',True) else capture.RawEvidenceError):
                fixed.run_stage(plan,'ks_metrics',db_path=case.f.db)

    def test_real_zero_settlement_paired_sequence_one_success_and_third_send_refused(self):
        case, plan = self.fixture(); c = case.f.connection
        self.fail_first(case,plan)
        original = dict(c.execute('SELECT * FROM provider_usage ORDER BY id DESC LIMIT 1').fetchone())
        with self.provider(lambda *a,**k:self.response(failed=False)) as call:
            result = fixed.run_unbilled_retry(plan,'ks_metrics',db_path=case.f.db)
            self.assertEqual(result['status'],'succeeded',result)
            self.assertEqual(call.call_count,1)
            self.assertEqual(result['actual_retry_provider_calls'],1)
            self.assertEqual(result['provider_calls'],1)
            self.assertEqual(result['actual_retry_provider_amount'],0.001)
            self.assertEqual(result['completion']['provider_calls'],0)
            with self.assertRaises(fixed.FixedRepairRejected): fixed.prepare_unbilled_retry(plan,'ks_metrics',db_path=case.f.db)
            reused = fixed.run_unbilled_retry(plan,'ks_metrics',db_path=case.f.db)
            self.assertEqual(reused['actual_retry_provider_calls'],0)
            self.assertEqual(call.call_count,1)
        self.assertEqual(dict(c.execute('SELECT * FROM provider_usage WHERE id=?',(original['id'],)).fetchone()),original)
        self.assertEqual(c.execute('SELECT amount_microunits FROM provider_usage_settlements WHERE provider_usage_id=?',(original['id'],)).fetchone()[0],0)
        claims = c.execute("SELECT sequence,scope_identity FROM provider_paid_scope_claims WHERE scope_kind='request' ORDER BY sequence").fetchall()
        self.assertEqual([r[0] for r in claims],[0,1]); self.assertEqual(claims[0][1],claims[1][1])
        self.assertEqual(c.execute("SELECT count(*) FROM paid_provider_dispatch_events WHERE event_type='send_marked'").fetchone()[0],2)
        self.assertEqual(c.execute('SELECT count(*) FROM authorization_issuance_consumptions').fetchone()[0],2)
        self.assertEqual(c.execute("SELECT count(*) FROM scheduler_runs WHERE job_id=? AND status='succeeded'",(fixed.JOB,)).fetchone()[0],1)
        with self.provider(lambda *a,**k: self.fail('local completion sent')):
            self.assertEqual(fixed.run_stage(plan,'ks_metrics',db_path=case.f.db)['provider_calls'],0)

    def test_second_failure_stops_without_third_request(self):
        case, plan = self.fixture(); c = case.f.connection; self.fail_first(case,plan)
        with self.provider(lambda *a,**k:self.response(failed=True)) as call:
            result = fixed.run_unbilled_retry(plan,'ks_metrics',db_path=case.f.db)
            self.assertEqual(result['status'],'blocked',result)
            self.assertEqual(result['actual_retry_provider_calls'],1)
            self.assertEqual(result['actual_retry_provider_amount'],0.0)
            with self.assertRaises(fixed.FixedRepairRejected): fixed.run_unbilled_retry(plan,'ks_metrics',db_path=case.f.db)
            self.assertEqual(call.call_count,1)
        self.assertEqual(c.execute("SELECT count(*) FROM paid_provider_dispatch_events WHERE event_type='send_marked'").fetchone()[0],2)

    def test_missing_explicit_unbilled_body_is_rejected_without_settlement_or_grants(self):
        case, plan = self.fixture(); c = case.f.connection; self.fail_first(case,plan,explicit=False)
        with self.assertRaises(fixed.FixedRepairRejected): fixed.prepare_unbilled_retry(plan,'ks_metrics',db_path=case.f.db)
        self.assertEqual(c.execute('SELECT count(*) FROM provider_usage_settlements').fetchone()[0],0)
        self.assertEqual(c.execute('SELECT count(*) FROM compensation_authorizations').fetchone()[0],0)

    def test_missing_clean_eof_is_rejected_without_grants(self):
        case, plan = self.fixture(); c = case.f.connection; self.fail_first(case,plan,clean=False)
        with self.assertRaises(Exception): fixed.prepare_unbilled_retry(plan,'ks_metrics',db_path=case.f.db)
        self.assertEqual(c.execute('SELECT count(*) FROM provider_usage_settlements').fetchone()[0],0)
        self.assertEqual(c.execute('SELECT count(*) FROM compensation_authorizations').fetchone()[0],0)

    def test_closed_gate_rolls_back_preparation(self):
        case, plan = self.fixture(); c = case.f.connection; self.fail_first(case,plan)
        with storage.transaction(c):
            row = dict(c.execute("SELECT * FROM capture_paid_send_gate_events WHERE operation='kuaishou_video_statistics' ORDER BY id DESC LIMIT 1").fetchone())
            values = {k:row[k] for k in ('provider','operation','state','reason','evidence_json','recorded_at')}
            values.update(state='closed',reason='offline revoked',recorded_at=storage.now_utc())
            c.execute('INSERT INTO capture_paid_send_gate_events('+','.join(values)+',event_sha256) VALUES('+','.join('?' for _ in range(len(values)+1))+')',(*values.values(),fixed.capture_authorizations.digest(values)))
        with self.assertRaises(Exception): fixed.prepare_unbilled_retry(plan,'ks_metrics',db_path=case.f.db)
        self.assertEqual(c.execute('SELECT count(*) FROM compensation_authorizations').fetchone()[0],0)
        self.assertEqual(c.execute('SELECT count(*) FROM provider_usage_settlements').fetchone()[0],0)


if __name__ == '__main__': unittest.main()
