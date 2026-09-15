"""HTTP admission and explicit single pending-link resolution on schema23."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from fastapi.testclient import TestClient
from tests.test_v8_api import _test_config
from v8 import api, content_identity
from v8.storage import connect, initialize_database

URL='https://www.kuaishou.com/short-video/3xwork'

class ContentIdentityApiV23Test(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.config=_test_config(Path(self.temp.name))
        with connect(self.config.db_path) as db:initialize_database(db,target_version=23)
        self.client=TestClient(api.create_app(self.config));self.addCleanup(self.client.close)
        self.enterContext(patch('socket.socket.connect',side_effect=AssertionError('network forbidden')))

    def test_pending_admission_then_explicit_resolution_and_replay(self):
        body={'platform':'kuaishou','canonical_url':'https://v.kuaishou.com/ABC','title':'keep'}
        with patch.object(content_identity,'expand',side_effect=content_identity.ContentIdentityError('identity_unresolved','try later')):
            response=self.client.post('/api/v8/contents',json=body)
        self.assertEqual(response.status_code,202,response.text)
        pending=response.json()['intake_id']
        self.assertEqual(self.client.get('/api/v8/contents/pending-links').json()['total'],1)
        def expand(platform,url):
            with connect(self.config.db_path) as writer:writer.execute('BEGIN IMMEDIATE');writer.rollback()
            return URL
        with patch.object(content_identity,'expand',side_effect=expand) as call:
            response=self.client.post(f'/api/v8/contents/pending-links/{pending}/resolve',json={})
            self.assertEqual(response.status_code,200,response.text)
            self.assertEqual(response.json()['status'],'resolved')
            call.side_effect=AssertionError('replay must not expand')
            repeat=self.client.post(f'/api/v8/contents/pending-links/{pending}/resolve',json={})
            self.assertEqual(repeat.status_code,200,repeat.text);self.assertTrue(repeat.json()['replayed'])
            pasted=self.client.post('/api/v8/contents',json=body)
            self.assertEqual(pasted.status_code,200,pasted.text)
            self.assertEqual(pasted.json()['id'],response.json()['content_id'])
            call.assert_called_once()
        self.assertEqual(self.client.get('/api/v8/contents/pending-links').json()['total'],0)
        with connect(self.config.db_path) as db:
            resolution=json.loads(db.execute('SELECT resolution_json FROM content_link_intakes').fetchone()[0])
            self.assertEqual(resolution['normalized']['canonical_url'],URL)
            self.assertEqual(db.execute('SELECT count(*) FROM content_items').fetchone()[0],1)

    def test_explicit_mismatch_and_nonofficial_host_never_create_pending(self):
        for body in ({'platform':'kuaishou','canonical_url':URL,'platform_content_id':'1234'},
                     {'platform':'kuaishou','canonical_url':'https://evil.test/short-video/3xwork'}):
            response=self.client.post('/api/v8/contents',json=body)
            self.assertEqual(response.status_code,409,response.text)
        with connect(self.config.db_path) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM content_items').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM content_link_intakes').fetchone()[0],0)
