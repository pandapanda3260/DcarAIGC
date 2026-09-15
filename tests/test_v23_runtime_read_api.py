"""Real lifespan and business reads on disposable schema23 / legacy19 databases."""
from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from fastapi.testclient import TestClient
from tests import test_v8_api as fixtures
from tests import test_v23_current_metric_policy as report_fixtures
from v8 import api, report_inputs
from v8.contracts import FOUR_PLATFORM_REPORT_VERSION
from v8.storage import connect, initialize_database


class RuntimeReadApiV23Test(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.config = fixtures._test_config(Path(self.temp.name))
        fixtures._seed_read_model_database(self.config.db_path)
        fixtures._seed_legacy_database(self.config.legacy_db_path,Path(self.temp.name))
        stamp = (datetime.now(timezone.utc)-timedelta(hours=1)).isoformat()
        with connect(self.config.db_path) as db:
            initialize_database(db,target_version=23)
            db.execute("UPDATE content_items SET published_at=?,created_at=?,updated_at=?,imported_at=?",(stamp,stamp,stamp,stamp))
            for index,platform in enumerate(('xiaohongshu','kuaishou','wechat_channels'),2):
                account = db.execute("INSERT INTO accounts(phone,operator_name,created_at,updated_at) VALUES ('',?,?,?)",(platform,stamp,stamp)).lastrowid
                db.execute("INSERT INTO account_platform_identities(account_id,platform,uid,nickname,created_at,updated_at) VALUES (?,?,?,?,?,?)",(account,platform,f'fixture-{platform}',platform,stamp,stamp))
                db.execute("INSERT INTO content_items(link_id,platform,platform_content_id,canonical_url,account_id,title,content_type,published_at,created_at,updated_at,imported_at) VALUES (?,?,?,?,?,?,'video',?,?,?,?)",
                    (f'FLOW{index:02d}',platform,str(index),f'https://example.com/{platform}/{index}',account,f'fixture {platform}',stamp,stamp,stamp,stamp))
            db.commit()
        self.enterContext(patch('socket.socket.connect',side_effect=AssertionError('network forbidden')))

    def test_real_writable_lifespan_health_overview_accounts_and_content_search(self):
        app = api.create_app(self.config)
        with TestClient(app) as client:
            health = client.get('/api/v8/health')
            self.assertEqual(health.status_code,200,health.text)
            self.assertEqual(health.json()['database_state']['schema_compatibility']['user_version'],23)
            self.assertEqual(health.json()['report_version'],FOUR_PLATFORM_REPORT_VERSION)
            self.assertIsNotNone(app.state.sqlite_runtime_anchor)
            overview = client.get('/api/v8/overview')
            self.assertEqual(overview.status_code,200,overview.text)
            self.assertEqual(overview.json()['report_version'],FOUR_PLATFORM_REPORT_VERSION)
            accounts = client.post('/api/v8/accounts/search',json={})
            self.assertEqual(accounts.status_code,200,accounts.text)
            self.assertGreaterEqual(accounts.json()['total'],4)
            contents = client.post('/api/v8/contents/search',json={})
            self.assertEqual(contents.status_code,200,contents.text)
            self.assertEqual({row['platform'] for row in contents.json()['items']},{'douyin','xiaohongshu','kuaishou','wechat_channels'})
            for platform in ('douyin','xiaohongshu','kuaishou','wechat_channels'):
                result = client.post('/api/v8/contents/search',json={'platform':platform})
                self.assertEqual(result.status_code,200,result.text)
                self.assertEqual(result.json()['total'],1)
            exported = client.get('/api/v8/contents/export')
            self.assertEqual(exported.status_code,200,exported.text)
            self.assertIn('kuaishou',exported.text)
        self.assertIsNone(app.state.sqlite_runtime_anchor)

    def test_real_read_only_lifespan_reads_schema23_without_row_changes(self):
        with connect(self.config.db_path) as db:
            before = {table:db.execute(f'SELECT count(*) FROM {table}').fetchone()[0] for table in
                ('content_items','account_intake_requests','capture_paid_send_gate_events','paid_provider_dispatch_events')}
        config = replace(self.config,read_only=True)
        with TestClient(api.create_app(config)) as client:
            for path in ('/api/v8/health','/api/v8/overview'):
                response = client.get(path);self.assertEqual(response.status_code,200,response.text)
            for path in ('/api/v8/accounts/search','/api/v8/contents/search'):
                response = client.post(path,json={});self.assertEqual(response.status_code,200,response.text)
        with connect(self.config.db_path,read_only=True) as db:
            after = {table:db.execute(f'SELECT count(*) FROM {table}').fetchone()[0] for table in before}
        self.assertEqual(before,after)

    def test_pending_media_rejects_invalid_filters_with_client_error(self):
        with TestClient(api.create_app(self.config), raise_server_exceptions=False) as client:
            for query in ({'stage':'invalid'}, {'reason':'invalid'}, {'account_query':'x'*201}):
                response = client.get('/api/v8/media/pending-work', params=query)
                self.assertEqual(response.status_code, 422, response.text)


class FrozenReportDownloadApiV23Test(unittest.TestCase):
    def setUp(self):
        self.fixture = fixture = report_fixtures.CurrentReportIntegrationTest()
        fixture.setUp();self.addCleanup(fixture.doCleanups)
        self.fx = fx = fixture.fx.fx
        self.config = replace(fixtures._test_config(fx.db.parent,db_name=fx.db.name),reports_root=fx.root)
        self.enterContext(patch('socket.socket.connect',side_effect=AssertionError('network forbidden')))

    def test_real_schema23_worker_registers_full_pipeline_while_paused(self):
        from apscheduler.schedulers.base import STATE_PAUSED
        config = replace(self.config,scheduler_enabled=True,scheduler_start_paused=True,
            daily_capture_reconcile_from=date(2026,9,12),writer_lock=self.fx.db.parent/'temporary-writer.lock')
        app = api.create_app(config)
        with TestClient(app) as client:
            self.assertTrue(app.state.report_runtime_ready,app.state.report_runtime_error)
            self.assertEqual(app.state.scheduler.state,STATE_PAUSED)
            jobs = {job.id for job in app.state.scheduler.get_jobs()}
            self.assertTrue({'capture_v25_plan','capture_v25_execute','capture_v25_maintenance',
                'capture_v25_archive','capture_v25_reconcile','pipeline_reconcile'} <= jobs,jobs)
            self.assertTrue(app.state.writer_lock_held)
            self.assertFalse(app.state.startup_catchup_enabled)
            health = client.get('/api/v8/health')
            self.assertEqual(health.status_code,200,health.text)
            self.assertEqual(health.json()['automation']['scheduler_state'],'paused')
            self.assertEqual(health.json()['database_state']['schema_compatibility']['user_version'],23)

    def test_real_report_xlsx_download_uses_frozen_kuaishou_aliases_after_later_edit(self):
        with connect(self.fx.db) as db:
            db.execute("UPDATE content_items SET platform='kuaishou',platform_content_id='99887766',canonical_url='https://www.kuaishou.com/short-video/3xabc',updated_at='2026-09-11T20:00:00Z' WHERE id=1")
            db.execute("DELETE FROM content_identities WHERE content_id=1")
            for value in ('99887766','3xabc'):
                db.execute("INSERT INTO content_identities(content_id,identity_kind,identity_value,platform_identity_key,created_at) VALUES (1,'platform_content_id',?,?,'2026-09-11T20:00:00Z')",(value,'kuaishou:'+value))
            db.commit()
        task = self.fx.task(period='2026-09-11',at=report_fixtures.CUTOFF,automatic=True)
        report = self.fx.run_report(task,at=report_fixtures.CUTOFF)
        revision = int(report['metadata']['revision'])
        with connect(self.fx.db) as db:
            frozen = report_inputs.load_event(db,task['id'],report_inputs.INPUT_EVENT)
            db.execute("UPDATE content_items SET canonical_url='https://www.kuaishou.com/short-video/3xchanged',updated_at='2026-09-13T00:00:00Z' WHERE id=1")
            db.commit()
        with TestClient(api.create_app(self.config)) as client:
            response = client.get(f"/api/v8/tasks/{task['id']}/revisions/{revision}/download?format=xlsx")
            self.assertEqual(response.status_code,200,response.text)
            with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
                text = '\n'.join(archive.read(name).decode() for name in archive.namelist() if name.startswith('xl/worksheets/') and name.endswith('.xml'))
            self.assertIn('99887766',text)
            self.assertIn('3xabc',text)
            self.assertNotIn('3xchanged',text)
        with connect(self.fx.db) as db:
            self.assertEqual(report_inputs.load_event(db,task['id'],report_inputs.INPUT_EVENT),frozen)


class LegacySchema19ReadCompatibilityTest(unittest.TestCase):
    def test_explicitly_supported_legacy19_content_search_uses_its_original_policy(self):
        with tempfile.TemporaryDirectory() as temporary:
            config = fixtures._test_config(Path(temporary));fixtures._seed_read_model_database(config.db_path)
            with connect(config.db_path) as db:
                self.assertEqual(db.execute('PRAGMA user_version').fetchone()[0],19)
            with patch('socket.socket.connect',side_effect=AssertionError('network forbidden')):
                with TestClient(api.create_app(config),raise_server_exceptions=False) as client, patch.object(
                        api,'select_legacy_content_metrics',wraps=api.select_legacy_content_metrics) as legacy:
                    response = client.post('/api/v8/contents/search',json={})
                    self.assertEqual(response.status_code,200,response.text)
                    self.assertEqual(response.json()['total'],1)
                    self.assertTrue(legacy.called)
                    self.assertEqual(legacy.call_args.kwargs['policy_version'],'source-routing-matrix-first-v2')

    def test_all_field_fact_schemas_keep_strict_current_selection(self):
        with tempfile.TemporaryDirectory() as temporary:
            for version in (20,21,22,23):
                with self.subTest(schema=version):
                    config = fixtures._test_config(Path(temporary)/str(version))
                    fixtures._seed_read_model_database(config.db_path)
                    with connect(config.db_path) as db:
                        initialize_database(db,target_version=version)
                        with patch.object(api,'select_legacy_content_metrics',wraps=api.select_legacy_content_metrics) as legacy, patch.object(
                                api,'select_current_content_metrics',wraps=api.select_current_content_metrics) as current:
                            api.select_content_metrics(db,[1])
                        legacy.assert_not_called()
                        current.assert_called_once()


if __name__ == '__main__':
    unittest.main()
