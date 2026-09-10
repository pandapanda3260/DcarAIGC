"""Explicit commands use isolated target routes without changing auto membership."""
from __future__ import annotations

import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from tests import test_v8_capture_commands as command_fixture
from v8 import api, capture_batches, capture_commands, capture_runtime, capture_manual
from v8.operations import upsert_account, upsert_content
from v8.storage import connect, transaction


class ManualContentCommandsTest(unittest.TestCase):
    def setUp(self):
        self.fixture = command_fixture.ProcessCommandsTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.addCleanup(self.fixture.doCleanups)
        self.db, self.at = self.fixture.db, self.fixture.at
        self.account = upsert_account({'enabled': False, 'platforms': [
            {'platform': 'douyin', 'uid': '123456789', 'nickname': 'fixture'}]}, db_path=self.db)['id']
        self.contents = [upsert_content({'platform': 'douyin',
            'platform_content_id': str(7380000000000000001 + n),
            'canonical_url': f'https://www.douyin.com/video/{7380000000000000001 + n}',
            'account_uid': '123456789', 'title': 'retained', 'content_type': 'video',
            'published_at': '2026-09-01T00:00:00Z'}, db_path=self.db)['id'] for n in range(2)]
        self.active = {'activation_id': 4, 'profile_id': 'integrated_route_v1',
                       'roster_snapshot_id': 3, 'roster_members_sha256': 'a' * 64}
        self.enterContext(patch.object(capture_runtime, 'activation_at', return_value=self.active))
        self.enterContext(patch.object(capture_runtime, '_cohort_plan', side_effect=AssertionError('manual must not build automatic cohorts')))
        self.actual_readiness = capture_runtime._readiness
        self.enterContext(patch.object(capture_runtime, '_readiness', return_value=('runnable', '')))

    def submit(self, cid=None, **options):
        return capture_commands.submit_command(db_path=self.db, content_id=cid or self.contents[0],
            kind='metrics_update', at=self.at, allowed_groups=['statistics'],
            task_id='frozen-metric-task', task_max_amount=.1, cycle_key='fixed-cycle', **options)['run_id']

    def work(self, cid=None):
        with connect(self.db) as connection:
            return dict(connection.execute('SELECT * FROM capture_work_items WHERE content_id=?',
                (cid or self.contents[0],)).fetchone())

    def test_disabled_outside_roster_enqueues_one_metrics_operation_and_repeat_reuses(self):
        run_id = self.submit()
        self.assertEqual(self.submit(), run_id)
        result = capture_commands.process_commands(db_path=self.db, at=self.at)
        self.assertEqual(result['run_ids'], [run_id])
        work = self.work()
        envelope = json.loads(work['envelope_json'])
        self.assertEqual((work['operation'], envelope['stage']), ('douyin_video_statistics', 'metrics'))
        self.assertEqual(envelope['logical_due'], 'fixed-cycle:statistics')
        self.assertEqual(envelope['manual_command_run_id'], run_id)
        self.assertEqual((envelope['task_id'], envelope['task_max_amount']), ('frozen-metric-task', .1))
        self.assertIsNone(work['source_plan_id'])
        with connect(self.db) as connection:
            self.assertEqual(connection.execute('SELECT enabled FROM accounts WHERE id=?', (self.account,)).fetchone()[0], 0)
            self.assertEqual(connection.execute('SELECT count(*) FROM account_roster_members').fetchone()[0], 0)
            self.assertEqual(connection.execute('SELECT count(*) FROM capture_source_plans').fetchone()[0], 0)
            routes = connection.execute('SELECT scope_key FROM capture_route_assignments').fetchall()
            self.assertEqual([r[0] for r in routes], [f'manual-command:{run_id}:content:{self.contents[0]}'])
            self.assertIsNone(capture_runtime.planning.resolve_route(connection, account_id=self.account,
                content_id=self.contents[0], operation='douyin_video_statistics', at=self.at))
        self.assertEqual(capture_commands.process_commands(db_path=self.db, at=self.at)['count'], 0)

    def test_nonrunning_automatic_work_keeps_old_paid_bucket_when_taken_over(self):
        old_id = self.submit()
        capture_commands.process_commands(db_path=self.db, at=self.at)
        work = self.work()
        old = json.loads(work['envelope_json'])
        old.pop('manual_command_run_id')
        old.pop('manual_command_run_ids')
        old['catalog_plan_id'] = 123
        old['logical_due'] = 'older-frozen-cycle:statistics'
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE capture_work_items SET state='provider_blocked',reason='member_scope_changed',envelope_json=? WHERE id=?",
                               (json.dumps(old), work['id']))
        new_id = capture_commands.submit_command(db_path=self.db, content_id=self.contents[0], kind='manual_update',
            at=self.at, allowed_groups=['statistics'], cycle_key='new-cycle', task_id='new-task', task_max_amount=.2)['run_id']
        self.assertNotEqual(new_id, old_id)
        capture_commands.process_commands(db_path=self.db, at=self.at)
        after = self.work()
        envelope = json.loads(after['envelope_json'])
        self.assertEqual(after['id'], work['id'])
        self.assertEqual(after['work_identity'], work['work_identity'])
        self.assertEqual(envelope['logical_due'], 'older-frozen-cycle:statistics')
        self.assertEqual(envelope['manual_command_run_id'], new_id)
        self.assertNotIn('catalog_plan_id', envelope)
        self.assertEqual(after['state'], 'runnable')

    def test_running_and_paid_identity_hold_work_are_linked_without_reopening(self):
        for index, state in enumerate(('running', 'paid_identity_hold')):
            cid = self.contents[index]
            original_id = self.submit(cid)
            capture_commands.process_commands(db_path=self.db, at=self.at)
            work = self.work(cid)
            with connect(self.db) as connection, transaction(connection):
                connection.execute('UPDATE capture_work_items SET state=?,owner_token=? WHERE id=?',
                    (state, 'owner' if state == 'running' else None, work['id']))
            new_id = capture_commands.submit_command(db_path=self.db, content_id=cid, kind='metrics_update',
                at=self.at, allowed_groups=['statistics'], cycle_key='fixed-cycle', task_id='another-command')['run_id']
            capture_commands.process_commands(db_path=self.db, at=self.at)
            after = self.work(cid)
            envelope = json.loads(after['envelope_json'])
            self.assertEqual(after['state'], state)
            self.assertEqual(envelope['manual_command_run_id'], original_id)
            self.assertIn(new_id, envelope['manual_command_run_ids'])
            self.assertEqual(after['assignment_id'], work['assignment_id'])

    def test_explicit_metric_cycle_keeps_prior_cycle_hold_separate(self):
        original_id = self.submit()
        capture_commands.process_commands(db_path=self.db, at=self.at)
        old = self.work()
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE capture_work_items SET state='paid_identity_hold',reason='billing_unknown' WHERE id=?", (old['id'],))
        next_id = capture_commands.submit_command(db_path=self.db, content_id=self.contents[0], kind='metrics_update',
            at=self.at, allowed_groups=['statistics'], cycle_key='explicit-next-cycle', task_id='next-task')['run_id']
        capture_commands.process_commands(db_path=self.db, at=self.at)
        with connect(self.db) as connection:
            rows = connection.execute('SELECT * FROM capture_work_items ORDER BY id').fetchall()
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]['state'], 'paid_identity_hold')
            self.assertEqual(json.loads(rows[0]['envelope_json'])['manual_command_run_id'], original_id)
            self.assertEqual(rows[1]['state'], 'runnable')
            self.assertEqual(json.loads(rows[1]['envelope_json'])['manual_command_run_id'], next_id)
            self.assertEqual(json.loads(rows[1]['envelope_json'])['logical_due'], 'explicit-next-cycle:statistics')

    def test_manual_statistics_cannot_share_another_content_batch(self):
        for cid in self.contents:
            self.submit(cid)
        capture_commands.process_commands(db_path=self.db, at=self.at)
        with connect(self.db) as connection, transaction(connection):
            ids = [r[0] for r in connection.execute('SELECT id FROM capture_work_items ORDER BY id')]
            with self.assertRaisesRegex(ValueError, 'cannot authorize other batch members'):
                capture_batches.freeze_batch(connection, work_ids=ids, at=self.at)
            self.assertEqual(connection.execute('SELECT count(*) FROM fetch_request_batches').fetchone()[0], 0)

    def test_manual_statistics_uses_single_content_executor_and_replays_prior_raw(self):
        self.submit()
        capture_commands.process_commands(db_path=self.db, at=self.at)
        with patch.object(capture_runtime, '_run_single', return_value={'single': True}) as single, \
                patch.object(capture_batches, 'run_one', side_effect=AssertionError('manual does not form a batch')):
            self.assertEqual(capture_runtime.run_one(self.db, self.at), {'single': True})
            single.assert_called_once_with(self.db, self.at)
        envelope = json.loads(self.work()['envelope_json'])
        raw = {'code': 200, 'data': {'status_code': 0, 'statistics_list': [{
            'aweme_id': '7380000000000000001', 'play_count': 88, 'digg_count': 9, 'share_count': 5}]}}
        stored = SimpleNamespace(value=raw, http_status=200, slot_id=0, raw_response_id=77)
        with patch.object(capture_runtime.capture, 'load_succeeded_raw_response', return_value=stored), \
                patch.object(capture_runtime.providers, '_douyin_call', side_effect=AssertionError('success must replay free')), \
                patch.object(capture_runtime.providers, '_budget_for_call', return_value='existing-budget') as budget, \
                patch.object(capture_runtime.providers, '_store_stage_result') as store, \
                patch.object(capture_runtime.raw_archive, 'read_response_entity', return_value=json.dumps(raw).encode()):
            result = capture_runtime._content_request(envelope, db_path=self.db, at=self.at)
        self.assertTrue(result['complete'])
        self.assertEqual(result['provider_cost'], 0)
        self.assertEqual(result['evidence']['raw_response_ids'], [77])
        self.assertEqual(budget.call_args.kwargs['task_id'], 'frozen-metric-task')
        self.assertEqual(budget.call_args.kwargs['task_max_amount'], .1)
        self.assertEqual(store.call_args.args[1:3], ('metrics', 'fixed-cycle:statistics'))

    def test_identity_change_before_enqueue_fails_without_work_or_routes(self):
        run_id = self.submit()
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE content_items SET platform_content_id='7380000000000000099' WHERE id=?", (self.contents[0],))
        result = capture_commands.process_commands(db_path=self.db, at=self.at)
        self.assertEqual(result['failed_run_ids'], [run_id])
        with connect(self.db) as connection:
            self.assertEqual(connection.execute('SELECT count(*) FROM capture_work_items').fetchone()[0], 0)
            self.assertEqual(connection.execute('SELECT count(*) FROM capture_route_assignments').fetchone()[0], 0)

    def test_work_budget_cannot_differ_from_frozen_command(self):
        self.submit()
        capture_commands.process_commands(db_path=self.db, at=self.at)
        envelope = json.loads(self.work()['envelope_json'])
        envelope['task_max_amount'] = 9
        with connect(self.db) as connection:
            self.assertEqual(self.actual_readiness(connection, envelope, at=self.at),
                             ('paid_identity_hold', 'manual_task_budget_changed'))

    def test_command_projects_blocked_partial_running_and_runnable_truthfully(self):
        cid = self.contents[0]
        run_id = capture_commands.submit_command(db_path=self.db, content_id=cid, kind='metrics_update', at=self.at)['run_id']
        capture_commands.process_commands(db_path=self.db, at=self.at)
        with connect(self.db) as connection, transaction(connection):
            ids = [r[0] for r in connection.execute('SELECT id FROM capture_work_items ORDER BY id')]
            self.assertEqual(len(ids), 2)
            connection.execute("UPDATE capture_work_items SET state='provider_blocked',reason='provider_transport_blocked'")
            result = capture_commands.read_command(connection, run_id=run_id, content_id=cid)
            self.assertEqual(result['status'], 'blocked')
            self.assertIn('provider_transport_blocked', result['reason'])
            connection.execute("UPDATE capture_work_items SET state='terminal',completed_at=? WHERE id=?", (self.at, ids[0]))
            self.assertEqual(capture_commands.read_command(connection, run_id=run_id, content_id=cid)['status'], 'partial')
            connection.execute("UPDATE capture_work_items SET state='running',completed_at=NULL,owner_token='owner' WHERE id=?", (ids[0],))
            self.assertEqual(capture_commands.read_command(connection, run_id=run_id, content_id=cid)['status'], 'running')
            connection.execute("UPDATE capture_work_items SET state='runnable',owner_token=NULL WHERE id=?", (ids[0],))
            self.assertEqual(capture_commands.read_command(connection, run_id=run_id, content_id=cid)['status'], 'pending')

    def test_metrics_http_persists_options_and_get_reads_same_command(self):
        app = FastAPI()
        app.include_router(api.router)
        app.state.writer_lock_held = True
        config = SimpleNamespace(db_path=self.db, read_only=False)
        with patch.object(api, '_request_config', return_value=config), patch.object(api, '_connect_for_request', side_effect=lambda _: connect(self.db)), \
                patch.object(capture_commands, 'now_utc', return_value=self.at), TestClient(app) as client:
            url = f'/api/v8/contents/{self.contents[0]}/metrics/refresh'
            request = {'allowed_groups': ['statistics'], 'task_id': 'api-task', 'task_max_amount': .05, 'cycle_key': 'api-cycle'}
            first = client.post(url, json=request)
            self.assertEqual(first.status_code, 202, first.text)
            self.assertEqual(client.post(url, json=request).json()['run_id'], first.json()['run_id'])
            read = client.get(f"/api/v8/contents/{self.contents[0]}/metrics/commands/{first.json()['run_id']}")
            self.assertEqual(read.status_code, 200, read.text)
            self.assertEqual(read.json()['kind'], 'metrics_update')
            with connect(self.db) as connection:
                spec = capture_manual.validate_command(connection, first.json()['run_id'], content_id=self.contents[0])
                self.assertEqual(spec['task_max_amount'], .05)
                self.assertEqual([t['stage'] for t in spec['targets']], ['metrics'])
                self.assertEqual(connection.execute('SELECT count(*) FROM provider_usage').fetchone()[0], 0)
            invalid = client.post(url, json={'allowed_groups': ['not-a-group']})
            self.assertEqual(invalid.status_code, 409, invalid.text)
            app.state.writer_lock_held = False
            self.assertEqual(client.post(url, json=request).status_code, 503)
