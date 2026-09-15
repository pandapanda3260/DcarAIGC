"""Schema22 web intake uses the common service without synchronous lookup."""
import unittest
from unittest.mock import patch
from uuid import uuid4
from tests import test_v8_account_creation_api as legacy
from v8.account_directory import DIRECTORY_SCHEMA
from v8.schema_v22 import INTAKE_SQL
from v8.storage import connect

class AccountIntakeApiTest(unittest.TestCase):
    start_patch = legacy.AccountCreationApiTest.start_patch
    resolve = legacy.AccountCreationApiTest.resolve
    count = legacy.AccountCreationApiTest.count

    def setUp(self):
        legacy.AccountCreationApiTest.setUp(self)
        with connect(self.config.db_path) as connection:
            connection.execute(DIRECTORY_SCHEMA)
            connection.execute(INTAKE_SQL)
            connection.commit()

    def body(self,**values):
        return {'platform':'douyin','uid':'000987654321','account_status':'paused','request_id':str(uuid4()),**values}

    def test_uid_only_is_accepted_without_network_subject_or_activation(self):
        body=self.body()
        response=self.client.post('/api/v8/accounts',json=body)
        self.assertEqual(response.status_code,200,response.text)
        result=response.json()
        self.assertEqual(result['status'],'accepted')
        self.assertIsNone(result['account_id'])
        self.assertEqual(self.count('accounts'),1)
        self.lookup.assert_not_called(); self.schedule.assert_not_called()
        status=self.client.get('/api/v8/accounts/intake/'+str(result['intake_id']))
        self.assertEqual(status.status_code,200,status.text)
        self.assertEqual(status.json()['status'],'accepted')
        self.assertNotIn('source_json',status.json())
        duplicate=self.client.post('/api/v8/accounts',json=body)
        self.assertTrue(duplicate.json()['replayed'])
        self.assertEqual(self.count('account_intake_requests'),1)

    def test_four_platforms_and_original_disabled_import_endpoint(self):
        values=[('douyin','uid','000987654321'),('xiaohongshu','uid','1234567890abcdef12345678'),
                ('kuaishou','uid','000123456789'),('wechat_channels','display_account_id','sphabcdefgh')]
        for platform,key,value in values:
            with self.subTest(platform=platform):
                body=self.body(platform=platform,uid='');body[key]=value
                result=self.client.post('/api/v8/accounts',json=body)
                self.assertEqual(result.status_code,200,result.text)
                self.assertEqual(result.json()['status'],'accepted')
        disabled=self.client.post('/api/v8/accounts/import',json={})
        self.assertEqual(disabled.status_code,410,disabled.text)
        search=self.client.post('/api/v8/accounts/search',json={})
        self.assertEqual(search.status_code,200,search.text)
        self.assertEqual(search.json()['account_management_version'],3)

    def test_missing_identity_or_numeric_identifier_rejected_before_writes(self):
        for body in (self.body(uid=''),self.body(uid=123456789),self.body(platform='douyin',profile_url='https://www.xiaohongshu.com/user/profile/1234567890abcdef12345678')):
            with self.subTest(body=body):
                response=self.client.post('/api/v8/accounts',json=body)
                self.assertEqual(response.status_code,422,response.text)
        self.assertEqual(self.count('account_intake_requests'),0)

    def test_shortlink_expands_before_transaction_and_replays_without_network(self):
        body=self.body(platform=None,uid='',profile_url='https://v.douyin.com/abc/')
        def expand(url):
            with connect(self.config.db_path) as connection:
                connection.execute('BEGIN IMMEDIATE')
                connection.rollback()
            return 'https://www.douyin.com/user/000987654321'
        with patch('v8.account_profile_public.expand_public_profile_url',side_effect=expand) as lookup:
            first=self.client.post('/api/v8/accounts',json=body)
            self.assertEqual(first.status_code,200,first.text)
            second=self.client.post('/api/v8/accounts',json=body)
            self.assertEqual(second.status_code,200,second.text)
            self.assertTrue(second.json()['replayed'])
            lookup.assert_called_once()
            changed=self.client.post('/api/v8/accounts',json={**body,'profile_url':'https://v.douyin.com/def/'})
            self.assertEqual(changed.status_code,409,changed.text)
            lookup.assert_called_once()
        self.assertEqual(self.count('account_intake_requests'),1)

    def test_video_share_url_is_not_silently_accepted_as_homepage(self):
        response=self.client.post('/api/v8/accounts',json=self.body(platform='wechat_channels',uid='',profile_url='https://weixin.qq.com/sph/abc'))
        self.assertEqual(response.status_code,422,response.text)
        self.assertIn('作品分享',response.text)
        self.assertEqual(self.count('account_intake_requests'),0)
