"""Actual schema23 public URL, provider ID, pending link and alias contracts."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from v8 import content_identity as identity
from v8.operations import upsert_content, update_content, import_contents, OperationError, IdentityConflictError
from v8.storage import connect, initialize_database, transaction
from v8.content_scope import canonical_content_predicate

AT = '2026-09-12T05:00:00Z'
KS = '5234567890123456789'
URL = 'https://www.kuaishou.com/short-video/3xwork'


class ContentIdentityV23Test(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)/'fixture.sqlite3'
        with connect(self.path) as db:
            initialize_database(db,target_version=23)
        self.enterContext(patch('socket.socket.connect',side_effect=AssertionError('network forbidden')))

    def provider(self, **values):
        return upsert_content({'platform':'kuaishou','platform_content_id':KS,'canonical_url':URL,
            'account_uid':'001234', **values},db_path=self.path,verified_provider_identity=('kuaishou','001234'))

    def test_known_public_shapes_are_canonical_and_channels_keeps_nonce(self):
        cases = [('douyin','http://douyin.com/video/7123456789?utm=a','7123456789'),
            ('xiaohongshu','https://www.xiaohongshu.com/discovery/item/'+'A'*24,'a'*24),
            ('kuaishou',URL+'?share=tracking','3xwork'),
            ('wechat_channels','https://channels.weixin.qq.com/video/123?object_id=123&object_nonce_id=abc%2B%2F','123')]
        for platform,url,key in cases:
            parsed=identity.parse(platform,url)
            self.assertEqual(parsed['platform_content_id'],key)
            if platform=='wechat_channels':
                self.assertEqual(parsed['locator_references']['object_nonce_id'],'abc+/')
                self.assertIn('object_nonce_id=abc%2B%2F',parsed['canonical_url'])
        for value in (True,1.2,1e20):
            with self.assertRaises(identity.ContentIdentityError): identity.parse('kuaishou',URL,value)
        for url in ('https://evil.test/short-video/3xwork','https://www.kuaishou.com/x/short-video/3xwork',
                    'https://www.kuaishou.com/short-video/3xwork/trailing'):
            with self.assertRaises(identity.ContentIdentityError): identity.parse('kuaishou',url)

    def test_manual_mismatch_rejects_but_provider_alias_preserves_numeric_identity(self):
        with self.assertRaises(OperationError):
            upsert_content({'platform':'kuaishou','platform_content_id':KS,'canonical_url':URL},db_path=self.path)
        result=self.provider()
        with connect(self.path) as db:
            row=db.execute('SELECT * FROM content_items WHERE id=?',(result['id'],)).fetchone()
            self.assertEqual(row['platform_content_id'],KS);self.assertEqual(row['canonical_url'],URL)
            self.assertEqual(identity.alias_content(db,'kuaishou','3xwork')['id'],result['id'])
            normalized=identity.normalize_submission({'platform':'kuaishou','canonical_url':URL},connection=db)
            self.assertEqual(normalized['platform_content_id'],KS)
        pasted=upsert_content({'platform':'kuaishou','canonical_url':URL},db_path=self.path)
        explicit=upsert_content({'platform':'kuaishou','platform_content_id':KS,'canonical_url':URL},db_path=self.path)
        self.assertEqual(result['id'],pasted['id']);self.assertEqual(result['id'],explicit['id'])
        update_content(result['id'],{'canonical_url':URL+'?share=other'},db_path=self.path)
        with connect(self.path) as db:
            self.assertEqual(db.execute('SELECT platform_content_id FROM content_items').fetchone()[0],KS)
            self.assertEqual(db.execute('SELECT count(*) FROM content_items').fetchone()[0],1)
            self.assertEqual(db.execute('PRAGMA foreign_key_check').fetchall(),[])

    def test_manual_public_id_is_upgraded_in_place_by_verified_discovery(self):
        manual=upsert_content({'platform':'kuaishou','canonical_url':URL},db_path=self.path)
        result=self.provider()
        self.assertEqual(manual['id'],result['id'])
        with connect(self.path) as db:
            self.assertEqual(db.execute('SELECT platform_content_id FROM content_items').fetchone()[0],KS)
            self.assertEqual(identity.alias_content(db,'kuaishou','3xwork')['id'],manual['id'])

    def test_conflicting_existing_numeric_owner_cannot_be_stolen_by_alias(self):
        first=self.provider()
        with self.assertRaises(IdentityConflictError): self.provider(platform_content_id='999999999')
        with connect(self.path) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM content_items').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT platform_content_id FROM content_items').fetchone()[0],KS)
            self.assertEqual(identity.alias_content(db,'kuaishou','3xwork')['id'],first['id'])

    def test_short_alias_and_bulk_import_resolve_outside_write_lock(self):
        def expanded(platform,url):
            with connect(self.path) as writer:
                writer.execute('BEGIN IMMEDIATE');writer.rollback()
            return URL
        with patch.object(identity,'expand',side_effect=expanded) as call:
            result=import_contents([{'platform':'kuaishou','canonical_url':'https://v.kuaishou.com/ABC'},
                {'platform':'kuaishou','canonical_url':URL}],source_name='fixture',db_path=self.path)
            call.assert_called_once()
        self.assertEqual(result['inserted_rows'],1)
        self.assertEqual(result['rejected_rows'],1)
        # The duplicate row and its proved short-link alias are both retained
        # even though the later row supplies the imported fields.
        with connect(self.path) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM content_items').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT count(*) FROM import_rows').fetchone()[0],2)
            self.assertIsNotNone(db.execute('SELECT content_id FROM content_identities WHERE platform_identity_key=?',
                (identity.alias_url_key('kuaishou','https://v.kuaishou.com/ABC'),)).fetchone())
        with patch.object(identity,'expand',side_effect=expanded):
            with connect(self.path) as db:
                normalized=identity.normalize_submission({'platform':'kuaishou','canonical_url':'https://v.kuaishou.com/ABC'},connection=db)
            saved=upsert_content(normalized,db_path=self.path)
        with connect(self.path) as db:
            alias=db.execute('SELECT content_id FROM content_identities WHERE platform_identity_key=?',
                (identity.alias_url_key('kuaishou','https://v.kuaishou.com/ABC'),)).fetchone()
            self.assertEqual(alias[0],saved['id'])

    def test_pending_retry_preserves_input_and_is_zero_write_after_success(self):
        submitted={'platform':'kuaishou','canonical_url':'https://v.kuaishou.com/ABC','title':'keep'}
        with connect(self.path) as db,transaction(db):
            result=identity.enqueue_pending_link(db,submitted,reason='unresolved',at=AT)
            other=identity.enqueue_pending_link(db,{**submitted,'canonical_url':'https://v.kuaishou.com/DEF'},reason='unresolved',at=AT)
        with connect(self.path) as db,patch.object(identity,'expand',return_value=URL) as call:
            prepared=identity.prepare_pending_resolution(db,result['intake_id'])
            call.assert_called_once()
            with transaction(db): outcome=identity.apply_pending_resolution(db,prepared,at=AT)
            self.assertEqual(outcome['status'],'resolved')
            self.assertEqual(db.execute('SELECT input_json FROM content_link_intakes WHERE id=?',(result['intake_id'],)).fetchone()[0],json.dumps(submitted,ensure_ascii=False,sort_keys=True,separators=(',',':')))
            self.assertEqual(db.execute('SELECT status FROM content_link_intakes WHERE id=?',(other['intake_id'],)).fetchone()[0],'pending')
            before=db.total_changes
            call.side_effect=AssertionError('resolved replay must never request again')
            repeat=identity.prepare_pending_resolution(db,result['intake_id'])
            with transaction(db): repeated=identity.apply_pending_resolution(db,repeat,at=AT)
            self.assertTrue(repeated['replayed']);self.assertEqual(before,db.total_changes)

    def test_pending_conflict_and_concurrent_change_are_visible(self):
        submitted={'platform':'kuaishou','canonical_url':'https://v.kuaishou.com/ABC'}
        with connect(self.path) as db,transaction(db):result=identity.enqueue_pending_link(db,submitted,reason='unresolved',at=AT)
        with connect(self.path) as db:
            prepared=identity.prepare_pending_resolution(db,result['intake_id'],{'canonical_url':URL,'platform_content_id':'99999'})
            with transaction(db):outcome=identity.apply_pending_resolution(db,prepared,at=AT)
            self.assertEqual(outcome['status'],'conflict')
            prepared=identity.prepare_pending_resolution(db,result['intake_id'],{'canonical_url':URL})
            with transaction(db):
                db.execute('UPDATE content_link_intakes SET reason=? WHERE id=?',('other edit',result['intake_id']))
                with self.assertRaises(identity.ContentIdentityError):identity.apply_pending_resolution(db,prepared,at=AT)
            self.assertEqual(db.execute('SELECT count(*) FROM content_items').fetchone()[0],0)

    def test_unresolved_bulk_import_creates_pending_without_urlhash_content(self):
        with patch.object(identity,'expand',side_effect=identity.ContentIdentityError('identity_unresolved','try later')):
            outcome=import_contents([{'platform':'kuaishou','canonical_url':'https://v.kuaishou.com/ABC'}],source_name='fixture',db_path=self.path)
        self.assertEqual(outcome['pending_identity_rows'],1)
        with connect(self.path) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM content_link_intakes').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT count(*) FROM content_items').fetchone()[0],0)

    def test_report_uses_only_frozen_alias_relation_and_exports_provider_id(self):
        import csv
        import io
        from v8.report_export import _content_sheet, _CONTENT_REQUIRED_HEADERS
        fields=sorted(_CONTENT_REQUIRED_HEADERS | {'account_type','content_type','platform_content_id','platform_content_id_aliases'})
        value={name:'' for name in fields}
        value.update(content_id='1',link_id='ABCDEF',platform='kuaishou',canonical_url=URL,
            platform_content_id=KS,content_type='video',account_type='unknown',platform_content_id_aliases='["3xwork"]')
        def render(record,enrichment=None):
            output=io.StringIO();writer=csv.DictWriter(output,fieldnames=fields);writer.writeheader();writer.writerow(record)
            return _content_sheet(task={},payload=output.getvalue().encode(),content_enrichment=enrichment or {},selling_point_labels={})
        sheet,total=render(value)
        self.assertEqual(total,1);self.assertEqual(sheet.rows[1][2],KS)
        for frozen in ('[]','["3xother"]','broken'):
            with self.assertRaises(ValueError):render({**value,'platform_content_id_aliases':frozen})
        # A later learned DB alias cannot rewrite a historical CSV's meaning.
        with self.assertRaises(ValueError):render({**value,'platform_content_id_aliases':''},
            {'1':{'platform_content_id_aliases':['3xwork']}})

    def test_detail_raw_proves_numeric_alias_without_changing_frozen_primary(self):
        import hashlib
        from tests.test_v8_kuaishou_adapter import detail_payload
        from v8 import kuaishou_adapter
        original=upsert_content({'platform':'kuaishou','canonical_url':URL,'account_uid':'001234'},db_path=self.path)
        payload=detail_payload();payload['params']['photo_id']='3xwork'
        parsed=kuaishou_adapter.parse_stage('detail','3xwork',payload,expected_uid='001234')
        body=json.dumps(payload).encode();path=Path(self.temp.name).resolve()/'detail.json';path.write_bytes(body);path.chmod(0o600)
        with connect(self.path) as db,transaction(db):
            raw=db.execute("INSERT INTO provider_raw_responses(content_id,provider,operation,local_path,sha256,byte_size,http_status,captured_at) VALUES(?,'TikHub','kuaishou_video_detail',?,?,?,200,?)",
                (original['id'],str(path),hashlib.sha256(body).hexdigest(),len(body),AT)).lastrowid
            identity.record_provider_content_aliases(db,original['id'],parsed,raw,AT)
            self.assertEqual(db.execute('SELECT platform_content_id FROM content_items').fetchone()[0],'3xwork')
            self.assertEqual(identity.alias_content(db,'kuaishou',KS)['id'],original['id'])
            before=db.total_changes
            identity.record_provider_content_aliases(db,original['id'],parsed,raw,AT)
            self.assertEqual(before,db.total_changes)
            for changed in ({**parsed,'platform_content_id':'9999'},{**parsed,'account_uid':'9999'}):
                with self.assertRaises(identity.ContentIdentityError):identity.record_provider_content_aliases(db,original['id'],changed,raw,AT)
            path.write_bytes(body+b'broken')
            with self.assertRaises(identity.ContentIdentityError):identity.record_provider_content_aliases(db,original['id'],parsed,raw,AT)
