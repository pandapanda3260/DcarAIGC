"""A new verified profile must repair a broken old reference, using real eligibility."""
from __future__ import annotations
import hashlib
import json
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import patch

from v8.account_capture_eligibility import identity_capture_evidence
from v8.account_intake import submit_account_intake, apply_prepared_profile
from v8.account_reference_storage import store_reference
from v8.operations import upsert_account
from v8.platform_adapters import normalize_profile
from v8.storage import connect, initialize_database, transaction

AT='2026-09-12T00:00:00Z'
LATER='2026-09-12T01:00:00Z'
UID='99887766'
SEC='MS4wLjAB'+'A'*68


class IntakeReferenceRepairTest(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup)
        self.root=Path(temporary.name).resolve();self.db=self.root/'accounts.sqlite3'
        self.connection=connect(self.db);self.addCleanup(self.connection.close)
        initialize_database(self.connection,target_version=22)
        self.enterContext(patch.object(socket.socket,'connect',side_effect=AssertionError('network forbidden')))
        self.aid=upsert_account({'platforms':[{'platform':'douyin','uid':UID,'nickname':'Synthetic'}]},db_path=self.db)['id']
        self.iid=self.connection.execute('SELECT id FROM account_platform_identities WHERE account_id=?',(self.aid,)).fetchone()[0]
        self.payload={'code':200,'router':'/api/v1/douyin/web/fetch_user_profile_by_uid','params':{'uid':UID},
            'data':{'status_code':0,'data':{'id_str':UID,'sec_user_id':SEC,'nickname':'Synthetic'}}}

    def raw(self,name,*,account_id=None,intake_id=None):
        body=json.dumps(self.payload,ensure_ascii=False,separators=(',',':')).encode()
        path=self.root/(name+'.json');path.write_bytes(body)
        raw_id=self.connection.execute('''INSERT INTO provider_raw_responses(account_id,intake_request_id,provider,operation,
            local_path,sha256,byte_size,http_status,captured_at) VALUES (?,?, 'TikHub','douyin_uid_profile',?,?,?,200,?)''',
            (account_id,intake_id,str(path),hashlib.sha256(body).hexdigest(),len(body),AT)).lastrowid
        return raw_id,path

    def test_verified_same_locator_replaces_broken_raw_pointer_and_enters_eligibility(self):
        with transaction(self.connection):
            old_id,old_path=self.raw('old',account_id=self.aid)
            store_reference(self.connection,account_identity_id=self.iid,platform='douyin',provider='TikHub',
                reference_kind='sec_user_id',reference_value=SEC,source_raw_response_id=old_id,
                created_at=AT,updated_at=AT)
        old_path.write_text('{}')
        with transaction(self.connection):
            accepted=submit_account_intake(self.connection,request_key='repair-broken-profile',value={'platform':'douyin','uid':UID},
                source={'kind':'fixture'},at=LATER)
            self.assertEqual(accepted['status'],'accepted')
            self.assertFalse(identity_capture_evidence(self.connection,self.iid)['eligible'])
            old_row=tuple(self.connection.execute('SELECT * FROM provider_raw_responses WHERE id=?',(old_id,)).fetchone())
            new_id,new_path=self.raw('new',intake_id=accepted['intake_id'])
            stored=json.loads(self.connection.execute('SELECT input_json FROM account_intake_requests WHERE id=?',(accepted['intake_id'],)).fetchone()[0])
            profile=normalize_profile('douyin',stored,self.payload,prior_responses=[{'operation':'douyin_uid_profile',
                'payload':self.payload,'raw_response_id':new_id}])
            ready=apply_prepared_profile(self.connection,accepted['intake_id'],profile,new_id,LATER)
            self.assertEqual(ready['status'],'ready')
            current=self.connection.execute('SELECT provider,source_raw_response_id,created_at FROM account_provider_references WHERE account_identity_id=?',(self.iid,)).fetchone()
            self.assertEqual(tuple(current),('TikHub',new_id,AT))
            eligibility=identity_capture_evidence(self.connection,self.iid)
            self.assertTrue(eligibility['eligible'],eligibility)
            self.assertEqual(tuple(self.connection.execute('SELECT * FROM provider_raw_responses WHERE id=?',(old_id,)).fetchone()),old_row)
            self.assertEqual(old_path.read_text(),'{}')
            self.assertTrue(new_path.exists())
            new_target=self.connection.execute('SELECT account_id,intake_request_id FROM provider_raw_responses WHERE id=?',(new_id,)).fetchone()
            self.assertEqual(tuple(new_target),(None,accepted['intake_id']))
            result=json.loads(self.connection.execute('SELECT result_json FROM account_intake_requests WHERE id=?',(accepted['intake_id'],)).fetchone()[0])
            self.assertEqual(result['reference_replacements'],[{'reference_kind':'sec_user_id','previous_raw_response_id':old_id,'current_raw_response_id':new_id}])
            before=self.connection.total_changes
            self.assertTrue(apply_prepared_profile(self.connection,accepted['intake_id'],profile,new_id,LATER)['replayed'])
            self.assertEqual(self.connection.total_changes,before)


    def test_same_verified_uid_can_update_changed_display_id_with_history(self):
        with transaction(self.connection):
            old_id,_=self.raw('old-display',account_id=self.aid)
            store_reference(self.connection,account_identity_id=self.iid,platform='douyin',provider='tikhub',
                reference_kind='display_account_id',reference_value='old_car',source_raw_response_id=old_id,
                created_at=AT,updated_at=AT)
            accepted=submit_account_intake(self.connection,request_key='changed-display',
                value={'platform':'douyin','uid':UID,'display_account_id':'old_car'},source={'kind':'fixture'},at=LATER)
            self.payload['data']['data']['unique_id']='new_car'
            raw_id,_=self.raw('new-display',intake_id=accepted['intake_id'])
            stored=json.loads(self.connection.execute('SELECT input_json FROM account_intake_requests WHERE id=?',(accepted['intake_id'],)).fetchone()[0])
            profile=normalize_profile('douyin',stored,self.payload,prior_responses=[{'operation':'douyin_uid_profile',
                'payload':self.payload,'raw_response_id':raw_id}])
            self.assertIn('display_account_id_change',profile['metadata'])
            ready=apply_prepared_profile(self.connection,accepted['intake_id'],profile,raw_id,LATER)
            self.assertEqual(ready['status'],'ready')
            current=self.connection.execute("SELECT reference_value,source_raw_response_id FROM account_provider_references WHERE account_identity_id=? AND reference_kind='display_account_id'",(self.iid,)).fetchone()
            self.assertEqual(tuple(current),('new_car',raw_id))
            self.assertTrue(identity_capture_evidence(self.connection,self.iid)['eligible'])
            self.assertEqual(self.connection.execute('SELECT display_account_id FROM account_directory_rows WHERE id=?',(ready['directory_row_id'],)).fetchone()[0],'new_car')
            self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM account_platform_identities').fetchone()[0],1)
            result=json.loads(self.connection.execute('SELECT result_json FROM account_intake_requests WHERE id=?',(accepted['intake_id'],)).fetchone()[0])
            replacement=next(row for row in result['reference_replacements'] if row['reference_kind']=='display_account_id')
            self.assertEqual((replacement['previous_value'],replacement['current_value']),('old_car','new_car'))
            self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM provider_raw_responses WHERE id=?',(old_id,)).fetchone()[0],1)
