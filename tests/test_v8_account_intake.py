"""Unified intake contracts, using only in-memory data and synthetic responses."""
from __future__ import annotations
import json
import unittest
from unittest.mock import patch
from tests import test_v8_account_summary_import as legacy
from tests.test_v8_account_summary_import import record, payload, UID, XHS, STAMP, LATER
from v8.account_intake import (submit_account_intake, preparation_inputs, apply_prepared_profile,
                               import_account_summary, normalize_intake)
from v8.schema_v22 import INTAKE_SQL, REFERENCE_SQL


class AccountIntakeTest(unittest.TestCase):
    def setUp(self):
        legacy.AccountSummaryImportTest.setUp(self)
        self.db.execute(INTAKE_SQL)
        self.db.execute(REFERENCE_SQL.replace('account_provider_references__v22','account_provider_references'))
        self.db.execute('CREATE TABLE provider_raw_responses(id INTEGER PRIMARY KEY,account_id INTEGER,intake_request_id INTEGER,sha256 TEXT)')
        self.db.commit()
        self.enterContext(patch('v8.account_capture_eligibility.identity_capture_evidence',return_value={'eligible':False}))
        self.db.execute('BEGIN')

    def submit(self,key='web-1',**value):
        return submit_account_intake(self.db,request_key=key,value={'platform':'douyin','uid':UID,**value},
                                     source={'kind':'web'},at=STAMP)

    def raw(self,intake_id,raw_id=1):
        self.db.execute('INSERT INTO provider_raw_responses VALUES(?,NULL,?,?)',(raw_id,intake_id,'a'*64))

    def apply(self,intake_id,raw_id=1,**profile):
        return apply_prepared_profile(self.db,intake_id,{'platform':'douyin','uid':UID,'nickname':'网络昵称',
            'references':{'sec_user_id':'MS4wLjAB'+'a'*40},**profile},raw_id,LATER)

    def test_uid_only_input_saves_directory_without_fabricating_subject(self):
        result=self.submit(account_status='paused',phone='00123456789')
        self.assertEqual(result['status'],'accepted')
        self.assertIsNone(result['account_id'])
        row=self.db.execute('SELECT * FROM account_directory_rows').fetchone()
        self.assertEqual((row['uid'],row['account_status'],row['phone']),(UID,'paused','00123456789'))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM accounts').fetchone()[0],0)
        self.assertEqual(len(preparation_inputs(self.db)),1)

    def test_same_request_is_zero_write_replay_and_changed_input_rejected(self):
        result=self.submit()
        before=self.db.total_changes
        again=self.submit()
        self.assertEqual(again['intake_id'],result['intake_id'])
        self.assertTrue(again['replayed'])
        self.assertEqual(self.db.total_changes,before)
        with self.assertRaisesRegex(ValueError,'不同内容'):
            self.submit(phone='00123456789')

    def test_web_and_excel_reuse_same_directory_and_do_not_create_subject(self):
        first=self.submit(phone='00123456789')
        result=import_account_summary(self.db,payload(record(verified=True,display='',手机号='')),imported_at=LATER)
        self.assertEqual(result['rows'][0]['directory_row_id'],first['directory_row_id'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM account_directory_rows').fetchone()[0],1)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM accounts').fetchone()[0],0)
        self.assertEqual(self.db.execute('SELECT phone FROM account_directory_rows').fetchone()[0],'00123456789')
        self.assertEqual(len({r['preparation_key'] for r in preparation_inputs(self.db)}),1)

    def test_batch_duplicate_is_retained_without_later_row_overwrite(self):
        result=import_account_summary(self.db,payload(record(运营人员='一'),record(3,运营人员='二')),imported_at=STAMP)
        self.assertEqual(result['counts']['review'],2)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM account_directory_rows').fetchone()[0],0)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM account_intake_requests').fetchone()[0],2)
        self.assertEqual(preparation_inputs(self.db),[])
        self.assertIn('一',self.db.execute('SELECT source_json FROM account_intake_requests ORDER BY id').fetchone()[0])

    def test_unusable_and_phone_only_rows_retain_sources_without_fake_platform(self):
        r=record(platform='',uid='',display='',verified=False,手机号='00123456789')
        r['metadata'].update(account_record_count=0,phone_record_count=1)
        result=import_account_summary(self.db,payload(r),imported_at=STAMP)
        self.assertEqual(result['rows'][0]['status'],'asset')
        self.assertEqual(result['rows'][0]['source_record']['raw']['手机号'],'00123456789')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM account_intake_requests').fetchone()[0],0)

    def test_profile_bind_creates_one_subject_and_does_not_mutate_raw_target(self):
        first=self.submit(account_status='paused')
        self.raw(first['intake_id'])
        before_raw=tuple(self.db.execute('SELECT * FROM provider_raw_responses').fetchone())
        result=self.apply(first['intake_id'])
        self.assertEqual(result['status'],'ready')
        self.assertEqual(result['directory_row_id'],first['directory_row_id'])
        self.assertEqual(tuple(self.db.execute('SELECT * FROM provider_raw_responses').fetchone()),before_raw)
        self.assertEqual(self.db.execute('SELECT enabled FROM accounts').fetchone()[0],0)
        self.assertEqual(self.db.execute('SELECT account_status FROM account_directory_rows').fetchone()[0],'paused')
        ref=self.db.execute('SELECT * FROM account_provider_references').fetchone()
        self.assertEqual((ref['platform'],ref['source_raw_response_id']),('douyin',1))
        before=self.db.total_changes
        self.assertTrue(self.apply(first['intake_id'])['replayed'])
        self.assertEqual(self.db.total_changes,before)
        self.assertEqual(preparation_inputs(self.db),[])

    def test_profile_identity_or_raw_request_mismatch_has_no_partial_writes(self):
        first=self.submit()
        self.raw(first['intake_id']+1)
        before=self.db.total_changes
        with self.assertRaisesRegex(ValueError,'不属于'):
            self.apply(first['intake_id'])
        self.assertEqual(self.db.total_changes,before)
        with self.assertRaisesRegex(ValueError,'UID 不一致'):
            self.apply(first['intake_id'],uid='987654321')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM accounts').fetchone()[0],0)

    def test_existing_subject_keeps_ids_and_later_manual_edits(self):
        self.db.commit()
        aid,iid=legacy.AccountSummaryImportTest.existing(self)
        self.db.execute('BEGIN')
        result=self.submit(phone='00987654321',operator_name='提交时运营')
        self.db.execute("UPDATE accounts SET operator_name='后来运营'")
        self.db.execute("UPDATE account_directory_rows SET operator_name='后来运营',account_status='paused'")
        self.raw(result['intake_id'])
        prepared=self.apply(result['intake_id'])
        self.assertEqual((prepared['account_id'],prepared['account_identity_id']),(aid,iid))
        self.assertEqual(self.db.execute('SELECT operator_name FROM accounts').fetchone()[0],'后来运营')
        self.assertEqual(self.db.execute('SELECT account_status FROM account_directory_rows').fetchone()[0],'paused')

    def test_cross_platform_equal_reference_values_do_not_merge(self):
        first=self.submit()
        second=self.submit('xhs',platform='xiaohongshu',uid=XHS,phone='00123456789')
        self.raw(first['intake_id'],1); self.raw(second['intake_id'],2)
        one=self.apply(first['intake_id'],references={'user_id':'same-reference'})
        two=self.apply(second['intake_id'],raw_id=2,platform='xiaohongshu',uid=XHS,references={'user_id':'same-reference'})
        self.assertNotEqual(one['account_id'],two['account_id'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM account_provider_references').fetchone()[0],2)

    def test_input_aliases_are_not_canonical_uids_and_numbers_require_text(self):
        ks=normalize_intake({'platform':'kuaishou','uid':'3xabcdefgh'})
        wx=normalize_intake({'platform':'wechat_channels','uid':'sphabcdefgh'})
        self.assertEqual(ks['uid'],''); self.assertEqual(ks['references']['eid'],'3xabcdefgh')
        self.assertEqual(wx['uid'],''); self.assertEqual(wx['display_account_id'],'sphabcdefgh')
        for field in ('uid','phone','display_account_id'):
            with self.subTest(field=field),self.assertRaises(ValueError):
                self.submit(**{field:123456789})

    def test_official_profile_derives_identity_and_rejects_mismatch(self):
        result=normalize_intake({'profile_url':'https://www.xiaohongshu.com/user/profile/'+XHS})
        self.assertEqual((result['platform'],result['uid']),('xiaohongshu',XHS))
        with self.assertRaises(ValueError):
            normalize_intake({'platform':'douyin','uid':UID,'profile_url':'https://www.douyin.com/user/987654321'})

    def test_multi_step_raw_evidence_must_all_belong_to_request(self):
        result=self.submit()
        self.raw(result['intake_id'],1);self.raw(result['intake_id']+1,2)
        with self.assertRaisesRegex(ValueError,'不属于'):
            self.apply(result['intake_id'],source_raw_response_ids=[1,2],reference_raw_response_ids={'sec_user_id':2})

    def test_derived_uid_input_hash_matches_saved_json_and_replays_original_input(self):
        from v8.capture_planning import digest
        self.db.commit();legacy.AccountSummaryImportTest.existing(self);self.db.execute('BEGIN')
        result=self.submit(uid='',display_account_id='old_car')
        saved=self.db.execute('SELECT * FROM account_intake_requests').fetchone()
        self.assertEqual(saved['input_sha256'],digest(json.loads(saved['input_json'])))
        self.assertEqual(json.loads(saved['input_json'])['uid'],UID)
        before=self.db.total_changes
        replay=self.submit(uid='',display_account_id='old_car')
        self.assertTrue(replay['replayed']);self.assertEqual(self.db.total_changes,before)
        self.assertEqual(replay['intake_id'],result['intake_id'])

    def test_success_finishes_equivalent_pending_requests_and_preserves_sources(self):
        first=self.submit('one',phone='00111111111')
        second=self.submit('two',phone='00222222222')
        self.raw(first['intake_id'])
        self.apply(first['intake_id'])
        self.assertEqual(preparation_inputs(self.db),[])
        rows=list(self.db.execute('SELECT result_json,source_json FROM account_intake_requests ORDER BY id'))
        self.assertEqual(json.loads(rows[1]['result_json'])['reused_from_intake_request_id'],first['intake_id'])
        self.assertEqual(self.db.execute('SELECT phone FROM account_directory_rows').fetchone()[0],'00222222222')
        replay=self.submit('two',phone='00222222222')
        self.assertTrue(replay['replayed']);self.assertEqual(replay['intake_id'],second['intake_id'])

    def test_two_batch_aliases_of_existing_account_do_not_overwrite_each_other(self):
        self.db.commit();legacy.AccountSummaryImportTest.existing(self);self.db.execute('BEGIN')
        one=record(display='',运营人员='一')
        two=record(3,uid='',display='old_car',verified=False,运营人员='二')
        result=import_account_summary(self.db,payload(one,two),imported_at=LATER)
        self.assertEqual(result['counts']['review'],2)
        self.assertEqual(self.db.execute('SELECT operator_name FROM accounts').fetchone()[0],'旧运营')

    def test_existing_verified_reference_conflict_rolls_back_input_fields(self):
        first=self.submit();self.raw(first['intake_id']);self.apply(first['intake_id'])
        oldphone=self.db.execute('SELECT phone FROM accounts').fetchone()[0]
        with self.assertRaisesRegex(ValueError,'定位不一致'):
            self.submit('bad-locator',profile_url='https://www.douyin.com/user/MS4wLjAB'+'b'*40,phone='00999999999')
        self.assertEqual(self.db.execute('SELECT phone FROM accounts').fetchone()[0],oldphone)

    def test_read_status_distinguishes_plan_error_lease_and_ready(self):
        from v8.account_intake import preparation_status
        result=self.submit()
        request=dict(self.db.execute('SELECT * FROM account_intake_requests').fetchone())
        self.assertEqual(preparation_status(request)['state'],'queued')
        blocked=json.loads(request['result_json']);blocked['preparation_error']='operation_price_unverified'
        self.assertIn('价格尚未核实',preparation_status({**request,'result_json':json.dumps(blocked)})['message'])
        work={'state':'running','reason':'','due_at':STAMP}
        self.assertEqual(preparation_status(request,work)['state'],'running')
        work={'state':'provider_blocked','reason':'provider_business_failure','due_at':LATER}
        self.assertEqual(preparation_status(request,work)['state'],'blocked')
        self.assertEqual(preparation_status(request,work)['due_at'],LATER)
        self.raw(result['intake_id']);prepared=self.apply(result['intake_id'])
        self.assertIn('尚不表示作品抓取已完成',prepared['message'])

    def test_directory_backfill_preserves_complete_original_directory_bytes(self):
        self.db.commit();aid,iid=legacy.AccountSummaryImportTest.existing(self);self.db.execute('BEGIN')
        row=self.db.execute('SELECT * FROM account_directory_rows').fetchone()
        before=tuple(row)
        result=submit_account_intake(self.db,request_key='backfill',value={'platform':'douyin','uid':UID,'display_account_id':'old_car'},
            source={'kind':'directory_backfill','directory_row_id':row['id']},at=LATER)
        self.assertEqual((result['account_id'],result['account_identity_id']),(aid,iid))
        self.assertEqual(tuple(self.db.execute('SELECT * FROM account_directory_rows').fetchone()),before)
        self.assertEqual(result['status'],'accepted')


class PreparationStatusTest(unittest.TestCase):
    def test_policy_wait_is_queued_and_locator_failure_is_chinese(self):
        from v8.account_intake import preparation_status
        waiting=preparation_status({'id':1,'result_json':json.dumps({'status':'accepted','preparation_error':'preparation_policy_unavailable'})})
        self.assertEqual(waiting['state'],'queued')
        self.assertEqual(waiting['label'],'待接入')
        failed=preparation_status({'id':1,'result_json':json.dumps({'status':'accepted','preparation_error':'locator_resolution_required'})})
        self.assertEqual(failed['state'],'blocked')
        self.assertNotIn('locator_resolution_required',failed['message'])
