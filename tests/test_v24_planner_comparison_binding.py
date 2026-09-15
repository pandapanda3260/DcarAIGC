"""Real planning comparison boundary tests; no production or provider access.

Planner interface:
  capture_runtime._plan_due(c, original_json_plan, *, at, comparison_plan=None)
The non-None entry must validate the exact current comparison binding before
any write, re-read the persisted signature, and strictly compare the original
mutable JSON once. None preserves the old path. Tests do not depend on the
name chosen for the private capture_metric_cycles entry-guard helper.

The small guard fixture has genuine schema24; the final integration fixture
has the real disposable installed schema22->23->24 inheritance/Writer lease.
Legacy HOLD fixtures below are explicitly cycle-semantics tests, not substitutes
for that installed schema24 boundary. No providers or production DB are used.
"""
from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import patch

from v8 import account_catalog_capture as catalog
from v8 import capture_metric_cycles as cycles
from v8 import capture_planning as planning
from v8 import capture_runtime as runtime
from v8 import runtime_evidence_context as evidence
from v8 import runtime_proof_workers, schema_v24, storage
from v8.profile_activations import activation_at
from v8.storage import connect, transaction
from tests import test_v23_metric_cycle_context as small
from tests import test_v8_catalog_capture_planner as profile_fixture
from tests import test_v8_metric_cycle_continuity as metric_fixture


def table_rows(connection):
    """All persisted fixture tables, including sequence/route/scope facts.

    Paired arms share one outer transaction and use rollback-to-savepoint, so
    generated IDs and all rows can be compared exactly, without normalization.
    """
    result = {}
    for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
        name = row[0]
        quoted = '"' + name.replace('"', '""') + '"'
        result[name] = sorted((tuple(value) for value in connection.execute('SELECT * FROM ' + quoted)),
            key=lambda value: repr(value))
    return result


class PlannerComparisonEntryV24Test(unittest.TestCase):
    def setUp(self):
        # Reuse the established real-schema plan fixture, then really migrate it.
        self.fx = small.MetricCycleContextV23Test(methodName='runTest')
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)
        self.c = self.fx.c
        schema_v24.migrate(self.c)
        self.c.commit()
        self.assertEqual(self.c.execute('PRAGMA user_version').fetchone()[0], 24)
        self.plan = self.insert_plan({**self.fx.body, 'cohort': []})
        self.c.commit()

    def insert_plan(self, body):
        generation = self.c.execute('SELECT COALESCE(MAX(generation),0)+1 FROM capture_source_plans').fetchone()[0]
        plan_id = self.c.execute('INSERT INTO capture_source_plans('
            'roster_change_id,business_day,generation,mode,payload_json,created_at,plan_sha256) '
            "VALUES(1,?,?,'active',?,?,?)", (body['business_day'], generation,
                planning.canonical(body), small.AT, planning.digest(body))).lastrowid
        return {'id': plan_id, **copy.deepcopy(body)}

    def rejected_without_writes(self, supplied, comparison, *, connection=None):
        connection = connection if connection is not None else self.c
        before, changes = table_rows(connection), connection.total_changes
        with self.assertRaises(ValueError):
            runtime._plan_due(connection, supplied, at=small.AT, comparison_plan=comparison)
        self.assertEqual(connection.total_changes, changes, 'guard rejected only after a write')
        self.assertEqual(table_rows(connection), before)

    def test_none_preserves_old_path_without_requiring_a_binding(self):
        with transaction(self.c):
            before = table_rows(self.c)
            result = runtime._plan_due(self.c, self.plan, at=small.AT)
            self.assertEqual((result['status'], result['created'], result['reconsidered']), ('planned', 0, 0))
            self.assertEqual(table_rows(self.c), before)

    def test_non_none_without_context_is_rejected_before_any_write(self):
        with transaction(self.c):
            self.rejected_without_writes(self.plan, copy.deepcopy(self.plan))

    def test_exact_bound_succeeds_but_equal_copy_is_not_a_binding(self):
        with transaction(self.c), cycles.planning_validation(self.c, plan=self.plan) as bound:
            result = runtime._plan_due(self.c, self.plan, at=small.AT, comparison_plan=bound)
            self.assertEqual(result['created'], 0)
            self.rejected_without_writes(self.plan, dict(bound))
            self.assertIsInstance(self.plan['cohort'], list)

    def test_expired_binding_cannot_be_reused_in_another_context(self):
        with transaction(self.c):
            with cycles.planning_validation(self.c, plan=self.plan) as expired:
                pass
            self.rejected_without_writes(self.plan, expired)
            with cycles.planning_validation(self.c, plan=self.plan) as current:
                self.assertIsNot(current, expired)
                self.rejected_without_writes(self.plan, expired)

    def test_same_plan_binding_from_another_connection_is_rejected(self):
        database = self.c.execute('PRAGMA database_list').fetchone()[2]
        with transaction(self.c), cycles.planning_validation(self.c, plan=self.plan) as bound:
            with connect(Path(database)) as other:
                other.execute('BEGIN')
                try:
                    self.rejected_without_writes(self.plan, bound, connection=other)
                finally:
                    other.rollback()

    def test_different_bound_plan_cannot_mix_into_original_plan(self):
        other = self.insert_plan({**self.fx.body, 'cohort': [], 'note': 'other source plan'})
        self.c.commit()
        with transaction(self.c), cycles.planning_validation(self.c, plan=other) as bound:
            self.rejected_without_writes(self.plan, bound)

    def test_same_id_mutation_after_binding_is_rejected_including_json_types(self):
        with transaction(self.c), cycles.planning_validation(self.c, plan=self.plan) as bound:
            for key, value in (('activation_id', True), ('activation_id', 1.0),
                    ('zero', -0.0), ('cohort', [{'identity_id': 99}]), ('new_field', 'changed')):
                with self.subTest(key=key, value=value):
                    changed = copy.deepcopy(self.plan)
                    changed[key] = value
                    self.rejected_without_writes(changed, bound)
            changed = copy.deepcopy(self.plan)
            changed['cohort'].append({'locator_evidence': {'sources': [{'raw_response_id': 7}]}})
            self.rejected_without_writes(changed, bound)

    def test_persisted_payload_and_matching_new_hash_still_invalidate_binding(self):
        with transaction(self.c), cycles.planning_validation(self.c, plan=self.plan) as bound:
            with self.assertRaisesRegex(sqlite3.IntegrityError, 'append-only'):
                self.c.execute('UPDATE capture_source_plans SET payload_json=? WHERE id=?', ('{}', self.plan['id']))
            # Disposable-only corruption fault; genuine schema guard was checked.
            self.fx.simulate_broken_schema_guard()
            changed = {key: value for key, value in self.plan.items() if key != 'id'}
            changed['activation_id'] = 2
            self.c.execute('UPDATE capture_source_plans SET payload_json=?,plan_sha256=? WHERE id=?',
                (planning.canonical(changed), planning.digest(changed), self.plan['id']))
            self.rejected_without_writes(self.plan, bound)

    def test_persisted_hash_mode_and_day_changes_are_rejected(self):
        for column, value in (('plan_sha256', 'f' * 64), ('mode', 'shadow'), ('business_day', '2026-09-13')):
            with self.subTest(column=column), transaction(self.c):
                self.c.execute('SAVEPOINT fault')
                try:
                    with cycles.planning_validation(self.c, plan=self.plan) as bound:
                        self.fx.simulate_broken_schema_guard()
                        self.c.execute('UPDATE capture_source_plans SET '+column+'=? WHERE id=?', (value, self.plan['id']))
                        self.rejected_without_writes(self.plan, bound)
                finally:
                    self.c.execute('ROLLBACK TO fault')
                    self.c.execute('RELEASE fault')

    def test_no_transaction_cannot_reuse_an_old_binding(self):
        with transaction(self.c), cycles.planning_validation(self.c, plan=self.plan) as bound:
            pass
        self.assertFalse(self.c.in_transaction)
        self.rejected_without_writes(self.plan, bound)


class BoundAccountMetricCycleSemanticsTest(unittest.TestCase):
    """Real temporary catalog/route/gate/HOLD SQL; legacy schema fixture only."""
    def setUp(self):
        self.fx = profile_fixture.CatalogCapturePlannerTest(methodName='runTest')
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)

    def bound_profile_work(self, at):
        plan = self.fx.plan(at=at)
        member = plan['cohort'][0]
        with connect(self.fx.db) as c, transaction(c):
            snapshot = plan['catalog_snapshot']
            with catalog.planning_validation(c, profile_fixture.ACCOUNT_CATALOG_POLICY,
                    snapshot, plan=plan) as bound:
                created = runtime._enqueue(c, bound, member, stage='account_metrics',
                    operation='douyin_uid_profile', logical_due='account-metrics:'+runtime._bucket(at, 6*3600), at=at)
            rows = [dict(row) for row in c.execute(
                "SELECT * FROM capture_work_items WHERE account_id=? AND operation='douyin_uid_profile' ORDER BY id", (self.fx.aid,))]
            return created, rows

    def test_new_six_hour_cycle_preserves_old_hold_and_stays_idempotent(self):
        old = self.fx.held_profile()
        created, rows = self.bound_profile_work('2026-09-02T18:10:00Z')
        self.assertTrue(created)
        self.assertEqual(rows[0], old)
        self.assertEqual(len(rows), 2)
        self.assertNotEqual(rows[0]['work_identity'], rows[1]['work_identity'])
        self.assertFalse(self.bound_profile_work('2026-09-02T18:11:00Z')[0])
        self.assertEqual(self.fx.immutable_counts(), self.fx.before)

    def test_closed_current_transport_gate_still_blocks_new_bound_work(self):
        old = self.fx.held_profile()
        at = '2026-09-02T18:10:00Z'
        with connect(self.fx.db) as c, transaction(c):
            gate = {'provider': 'tikhub', 'operation': 'douyin_uid_profile', 'state': 'closed',
                'reason': 'fixture hold', 'evidence_json': '{}', 'recorded_at': at}
            c.execute('INSERT INTO capture_paid_send_gate_events('
                'provider,operation,state,reason,evidence_json,recorded_at,event_sha256) VALUES(?,?,?,?,?,?,?)',
                (*gate.values(), planning.digest(gate)))
        created, rows = self.bound_profile_work(at)
        self.assertTrue(created)
        self.assertEqual(rows[0], old)
        self.assertEqual((rows[1]['state'], rows[1]['reason']), ('provider_blocked', 'provider_transport_blocked'))

    def test_live_member_validation_does_not_borrow_the_planning_snapshot(self):
        from v8.provider_budget import PaidScopeBlocked
        plan = self.fx.plan()
        with connect(self.fx.db) as c, transaction(c):
            with catalog.planning_validation(c, profile_fixture.ACCOUNT_CATALOG_POLICY,
                    plan['catalog_snapshot'], plan=plan) as bound:
                catalog.validate_plan_member(c, plan['id'], self.fx.iid, at=profile_fixture.AT,
                    policy=profile_fixture.ACCOUNT_CATALOG_POLICY, use_planning_cache=False)
                # Exact locator conflicts with the retained verified admission.
                # The send/admission path deliberately bypasses planning caches.
                c.execute("UPDATE account_provider_references SET reference_value=? WHERE "
                    "account_identity_id=? AND lower(provider)='tikhub' AND reference_kind='sec_user_id'",
                    ('MS4wLjAB'+'B'*64, self.fx.iid))
                self.assertTrue(cycles._same_plan(c, cycles._stored_plan(c, plan['id']), bound))
                with self.assertRaises(PaidScopeBlocked):
                    catalog.validate_plan_member(c, plan['id'], self.fx.iid, at=profile_fixture.AT,
                        policy=profile_fixture.ACCOUNT_CATALOG_POLICY, use_planning_cache=False)


class BoundMetricCycleLiveSemanticsTest(unittest.TestCase):
    """Small real SQL fixture: current activation/content decisions remain live."""
    def setUp(self):
        self.fx = metric_fixture.MetricCycleContinuityTest(methodName='runTest')
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)

    def test_original_nested_lists_remain_members_of_persisted_json_cohort(self):
        self.fx.member['locator_evidence'] = {'sources': [{'raw_response_id': 7, 'raw_sha256': 'c'*64}]}
        self.fx.save_plan()
        original = copy.deepcopy(self.fx.member)
        with cycles.planning_validation(self.fx.c, plan=self.fx.plan) as bound:
            self.fx.plan = bound
            self.assertIsInstance(bound['cohort'][0]['locator_evidence']['sources'], tuple)
            self.assertIsInstance(self.fx.member['locator_evidence']['sources'], list)
            self.assertFalse(self.fx.check())
            self.assertEqual(self.fx.member, original)
            # Demonstrate why _plan_due must keep its original JSON members.
            self.fx.member = bound['cohort'][0]
            self.assertTrue(self.fx.check())

    def test_activation_and_current_content_ownership_are_not_comparison_cache(self):
        with cycles.planning_validation(self.fx.c, plan=self.fx.plan) as bound:
            self.fx.plan = bound
            self.assertFalse(self.fx.check())
            self.fx.active['activation_id'] = 4
            self.assertTrue(self.fx.check())
            self.fx.active['activation_id'] = 3
            self.fx.c.execute('INSERT INTO content_identity_merge_events VALUES(1)')
            self.assertTrue(self.fx.check())


class InstalledPlannerComparisonV24Test(unittest.TestCase):
    """Full planner pair behind real disposable installed schema24 boundaries.

    This is not the external historical-copy profile with a static policy
    substitution. Installed policy, source proofs and parent entry/exit fences
    execute here. Location pins are exactly the existing fixture's pins.
    """
    def test_full_due_outputs_match_and_discovery_survives_with_nested_json(self):
        from tests.fixtures_v24_installed_paid import InstalledDuplicateFixture
        fixture = InstalledDuplicateFixture(methodName='runTest')
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.enterContext(patch('socket.socket.connect', side_effect=AssertionError('network forbidden')))
        self.enterContext(patch.object(runtime_proof_workers, 'enabled', return_value=False))
        f, c = fixture.f, fixture.f.connection
        self.assertEqual(c.execute('PRAGMA user_version').fetchone()[0], 24)
        at = storage.now_utc()
        # A controlled test instant, following the established installed
        # boundary tests; avoid a setup timestamp predating the prepared proof.
        self.enterContext(patch.object(evidence, '_now', return_value=at))
        self.enterContext(patch.object(runtime, 'now_utc', return_value=at))
        now = datetime.fromisoformat(at.replace('Z', '+00:00'))
        earlier = (now-timedelta(hours=2)).isoformat()
        published = (now-timedelta(days=1)).isoformat()

        # The installed fixture's old raw payload is deliberately a reduced
        # transport fixture, not full catalog identity evidence. Add a distinct
        # account through the established verified-admission receipt protocol;
        # keep its old roster, locator and raw facts untouched.
        from v8.account_operating_receipts import record_status_receipt
        with transaction(c):
            uid, platform, sec = '1234567899942', 'douyin', 'MS4wLjAB'+'C'*64
            aid = c.execute("INSERT INTO accounts(phone,enabled,created_at,updated_at) VALUES('',1,?,?)", (earlier, earlier)).lastrowid
            iid = c.execute('INSERT INTO account_platform_identities(account_id,platform,uid,created_at,updated_at) '
                'VALUES(?,?,?,?,?)', (aid, platform, uid, earlier, earlier)).lastrowid
            c.execute('INSERT INTO account_provider_references(account_identity_id,provider,platform,reference_kind,'
                "reference_value,created_at,updated_at) VALUES(?,'TikHub','douyin','sec_user_id',?,?,?)", (iid, sec, earlier, earlier))
            c.execute('INSERT INTO account_directory_rows(source_sha256,source_name,source_sheet,source_row,'
                'account_id,platform,uid,account_status,identity_status,raw_json,imported_at,updated_at) '
                "VALUES(?,'fixture','fixture',2,?,? ,?,'daily','existing_verified','{}',?,?)",
                ('e'*64, aid, platform, uid, earlier, earlier))
            before_enabled = bool(c.execute('SELECT enabled FROM accounts WHERE id=?', (aid,)).fetchone()[0])
            record_status_receipt(c, request_id='planner-bound-fixture-admission', account_id=aid,
                account_identity_id=iid, requested_status='daily', update_frequency='daily',
                request={'account_status':'daily','fields':{},'admission':{'member':{
                    'platform':platform,'uid':uid,'metadata':{'sec_user_id':sec}}}},
                actor='offline fixture', reason='verified local planner admission',
                before={'enabled':before_enabled,'update_frequency':None},
                after={'enabled':True,'update_frequency':'daily'},
                result={'id':aid,'status_request_id':'planner-bound-fixture-admission',
                    'account_status':'daily','enabled':True,'update_frequency':'daily'}, timestamp=at)

        # Prepare test data before the proof being checked below. The plan and
        # locator-list extension are append-only fixture evidence, not edits to
        # a previously validated source plan or changes to a production source.
        with evidence.prepare_inheritance(f.db), transaction(c), evidence.inheritance_boundary(c):
            active = activation_at(c, at)
            policy = catalog.installed_policy(c, at=at)
            original_plan = runtime._cohort_plan(c, active, at=at, shadow=False)
            exclusions = original_plan.get('catalog_snapshot', {}).get('eligibility', {}).get('excluded_members', [])
            self.assertTrue(original_plan['cohort'], [(m.get('reason_code'), m.get('platform')) for m in exclusions])
            body = copy.deepcopy({key: value for key, value in original_plan.items() if key != 'id'})
            member = body['cohort'][0]
            member['locator_evidence'] = {'sources': [{'raw_response_id': 7, 'raw_sha256': 'c'*64}]}
            row = c.execute('SELECT * FROM capture_source_plans WHERE id=?', (original_plan['id'],)).fetchone()
            generation = c.execute('SELECT MAX(generation)+1 FROM capture_source_plans WHERE '
                'roster_change_id=? AND business_day=?', (row['roster_change_id'], row['business_day'])).fetchone()[0]
            plan_id = c.execute('INSERT INTO capture_source_plans(roster_change_id,business_day,generation,mode,'
                'payload_json,created_at,plan_sha256) VALUES(?,?,?,?,?,?,?)',
                (row['roster_change_id'], row['business_day'], generation, row['mode'],
                    planning.canonical(body), earlier, planning.digest(body))).lastrowid
            plan = {'id': plan_id, **body}
            content_id = c.execute('INSERT INTO content_items(link_id,account_id,platform,platform_content_id,'
                'canonical_url,content_type,title,body,published_at,imported_at,created_at,updated_at) '
                'VALUES(?,?,?,?,?,?,?,?,?,?,?,?)', ('BND042', member['account_id'], member['platform'],
                    'planner-bound-fixture', 'https://example.invalid/planner-bound-fixture', 'video',
                    'fixture', 'fixture', published, at, at, at)).lastrowid

        original_json = planning.canonical(plan)
        outputs = []
        with evidence.prepare_inheritance(f.db) as prepared:
            self.assertIsNotNone(prepared)
            self.assertEqual(prepared.schema_version, 24)
            with transaction(c), evidence.inheritance_boundary(c):
                before = table_rows(c)
                for optimized in (False, True):
                    c.execute('SAVEPOINT paired_due')
                    try:
                        snapshot = plan['catalog_snapshot']
                        context = (catalog.planning_validation(c, policy, snapshot, plan=plan) if optimized
                            else catalog.planning_validation(c, policy, snapshot))
                        with context as bound:
                            result = runtime._plan_due(c, plan, at=at, comparison_plan=bound if optimized else None)
                        outputs.append((result, table_rows(c)))
                        self.assertEqual(planning.canonical(plan), original_json)
                    finally:
                        c.execute('ROLLBACK TO paired_due')
                        c.execute('RELEASE paired_due')
                    self.assertEqual(table_rows(c), before)
                self.assertEqual(outputs[0], outputs[1], 'complete planner database outputs changed')
                self.assertEqual(outputs[1][0]['provider_calls'], 0)
                names = [row['name'] for row in c.execute('PRAGMA table_info(capture_work_items)')]
                work = [dict(zip(names, row)) for row in outputs[1][1]['capture_work_items']]
                created = [row for row in work if row['source_plan_id'] == plan_id]
                envs = [json.loads(row['envelope_json']) for row in created]
                self.assertTrue(any(env['stage'] == 'discovery' and env.get('published_intervals') for env in envs))
                self.assertTrue(any(env['stage'] == 'account_metrics' for env in envs))
                self.assertTrue(any(env['stage'] == 'metrics' and env['content_id'] == content_id for env in envs))
                self.assertEqual(outputs[0][1]['provider_usage'], before['provider_usage'])
                self.assertEqual(outputs[1][1]['provider_usage'], before['provider_usage'])
        self.assertIsNone(evidence._PREPARED.get())
        self.assertIsNone(cycles._PLANNING_PLANS.get())


# The release checks also retain test_v24_capture_plan_preparation and the
# installed entry/exit revocation suite. A source/authority change must reject
# due work while preserving any preceding preparation commit. Directory
# eligibility intentionally belongs to the existing single planning transaction;
# later revision CAS and admission/send use_planning_cache=False remain live.
