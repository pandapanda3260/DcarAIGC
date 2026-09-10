"""Offline catalog-plan fences, live eligibility changes and two-member scope.

A small SQLite fixture supplies durable plans/owners/batches. The eligibility
resolver is replaced by another current SQLite table so tests change status and
identity after planning; actual directory qualification has its own test suite.
"""
from __future__ import annotations

import copy
import json
import os
import sqlite3
import unittest
from dataclasses import replace
from unittest.mock import patch

from v8 import account_catalog_capture as catalog, account_capture_eligibility as eligibility
from v8 import account_cleanup_runtime, capture, capture_batches as batches, capture_planning as planning, profile_activations, provider_budget
from v8.account_catalog_capture_release import ACCOUNT_CATALOG_POLICY as POLICY
from v8.provider_budget import PaidScope, PaidScopeBlocked

AT = '2026-09-10T00:00:00Z'


class AccountCatalogCaptureTest(unittest.TestCase):
    def setUp(self):
        self.connection = c = sqlite3.connect(':memory:')
        c.row_factory = sqlite3.Row
        self.addCleanup(c.close)
        c.executescript('''
            PRAGMA user_version=21;
            CREATE TABLE capture_source_plans(id INTEGER PRIMARY KEY,payload_json TEXT,plan_sha256 TEXT,mode TEXT);
            CREATE TABLE scheduler_runs(id INTEGER PRIMARY KEY,details_json TEXT);
            CREATE TABLE capture_work_items(id INTEGER PRIMARY KEY,work_identity TEXT,source_plan_id INTEGER,
                account_id INTEGER,content_id INTEGER,operation TEXT,envelope_json TEXT,state TEXT,owner_token TEXT,updated_at TEXT);
            CREATE TABLE fetch_request_batches(id INTEGER PRIMARY KEY,work_id INTEGER,request_scope_identity TEXT,
                sequence INTEGER,provider TEXT,operation TEXT,parameters_json TEXT,created_at TEXT);
            CREATE TABLE fetch_request_batch_members(id INTEGER PRIMARY KEY,batch_id INTEGER,member_scope_identity TEXT,
                sequence INTEGER,content_id INTEGER,account_id INTEGER);
            CREATE TABLE content_items(id INTEGER PRIMARY KEY,platform_content_id TEXT,platform TEXT);
            CREATE TABLE current_members(identity_id INTEGER PRIMARY KEY,account_id INTEGER,platform TEXT,
                uid TEXT,locator_sha256 TEXT,status TEXT);
        ''')
        self.members = []
        for n in (1, 2, 3):
            member = {'identity_id': n + 10, 'account_id': n + 100, 'platform': 'douyin',
                'uid': str(1000000 + n), 'locator_sha256': str(n) * 64, 'eligible': True,
                'directory_row_id': n, 'account_status': 'daily'}
            self.members.append(member)
            c.execute('INSERT INTO current_members VALUES(?,?,?,?,?,?)',
                (*[member[key] for key in catalog.IDENTITY_KEYS], 'daily'))
            c.execute('INSERT INTO content_items VALUES(?,?,?)', (1000 + n, str(7000000000000000000 + n), 'douyin'))
        self.resolver = self.enterContext(patch.object(eligibility, 'require_directory_capture_member', side_effect=self.resolve))
        self.enterContext(patch.dict(os.environ, {'DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT': '/offline/fixture-only'}))
        proof = {'contract': 'fixture-policy-proof'}
        proof['proof_sha256'] = planning.digest(proof)
        self.installed = self.enterContext(patch.object(account_cleanup_runtime, 'installed_evidence', return_value={
            'catalog_capture_policy': POLICY, 'catalog_capture_policy_sha256': planning.digest(POLICY),
            'catalog_capture_proof': proof}))
        derived = {'eligible_members': copy.deepcopy(self.members), 'excluded_members': []}
        with patch.object(eligibility, 'derive_capture_eligibility', return_value=derived):
            self.snapshot = catalog.freeze_snapshot(c, policy=POLICY)
        self.plan = {'activation_id': 4, 'activation_sha256': 'a' * 64, 'profile_id': 'integrated_route_v1', 'roster_snapshot_id': 7, 'shadow': False, 'catalog_mode': 'active',
            'roster_members_sha256': 'f' * 64, 'catalog_snapshot': self.snapshot}
        self.save_plan(1, self.plan)
        for n in (1, 2, 3):
            member = self.members[n - 1]
            envelope = {**{k: self.plan[k] for k in ('activation_id','profile_id','roster_snapshot_id','roster_members_sha256')},
                'identity_id': member['identity_id'], 'account_id': member['account_id'], 'content_id': 1000 + n,
                'operation': batches.OPERATION, 'catalog_plan_id': 1, 'logical_due': AT,
                'stage': 'metrics', 'source_stage': 'metrics', 'assignment_id': n, 'request_batch_id': 9}
            c.execute('INSERT INTO capture_work_items VALUES(?,?,?,?,?,?,?,?,?,?)',
                (n, 'work-' + str(n), 1, member['account_id'], 1000 + n, batches.OPERATION,
                 planning.canonical(envelope), 'running', 'owner-token', AT))
        c.execute('INSERT INTO scheduler_runs VALUES(1,?)', (planning.canonical({
            'checkpoint': {'work_id': 1}, 'identity': {'work_identity': 'work-1', 'catalog_plan_id': 1}}),))
        ids = [str(7000000000000000000 + n) for n in (1, 2)]
        request = batches._identity(ids, AT)
        c.execute('INSERT INTO fetch_request_batches VALUES(9,1,?,0,?,?,?,?)',
            (request.scope_identity, 'tikhub', batches.OPERATION, planning.canonical({'aweme_ids': ','.join(ids)}), AT))
        for n in (1, 2):
            ident = batches._identity([ids[n - 1]], AT)
            scope_id = batches.usage_settlements.member_identity(ident.document)
            c.execute('INSERT INTO fetch_request_batch_members VALUES(?,9,?,0,?,?)', (n, scope_id, 1000 + n, 100 + n))
        self.scope = PaidScope(scheduler_run_id=1, scheduler_owner_token='owner-token', catalog_plan_id=1,
            activation_id=4, roster_snapshot_id=7, roster_snapshot_hash='f' * 64)
        self.active = self.enterContext(patch.object(profile_activations, 'activation_at', return_value={
            key: self.plan[key] for key in ('activation_id','activation_sha256','profile_id','roster_snapshot_id','roster_members_sha256')}))
        c.commit()

    def resolve(self, connection, identity_id):
        row = connection.execute('SELECT * FROM current_members WHERE identity_id=?', (identity_id,)).fetchone()
        if row is None:
            raise eligibility.DirectoryCaptureEligibilityError('identity_missing')
        if row['status'] == 'paused':
            raise eligibility.DirectoryCaptureEligibilityError('account_paused')
        return {key: row[key] for key in catalog.IDENTITY_KEYS}

    def save_plan(self, plan_id, plan):
        self.connection.execute('INSERT OR REPLACE INTO capture_source_plans VALUES(?,?,?,?)',
            (plan_id, planning.canonical(plan), planning.digest(plan), plan.get('catalog_mode', 'active')))

    def envelope(self, work_id, **changes):
        old = self.connection.execute('SELECT envelope_json FROM capture_work_items WHERE id=?', (work_id,)).fetchone()[0]
        value = {**json.loads(old), **changes}
        self.connection.execute('UPDATE capture_work_items SET envelope_json=? WHERE id=?', (planning.canonical(value), work_id))

    def validate(self, identity=11, content=1001, scope=None):
        return catalog.validate_paid_target(self.connection, scope or self.scope,
            identity_id=identity, content_id=content, at=AT)

    def test_frozen_pair_accepts_both_members_but_no_third_member(self):
        self.assertEqual(self.validate()['account_id'], 101)
        self.assertEqual(self.validate(12, 1002)['account_id'], 102)
        with self.assertRaises(PaidScopeBlocked) as result:
            self.validate(13, 1003)
        self.assertEqual(result.exception.error_code, 'catalog_batch_invalid')

    def test_current_pause_and_identity_change_block_already_frozen_work(self):
        self.connection.execute("UPDATE current_members SET status='paused' WHERE identity_id=12")
        with self.assertRaises(PaidScopeBlocked) as result:
            self.validate(12, 1002)
        self.assertEqual(result.exception.error_code, 'account_paused')
        self.connection.execute("UPDATE current_members SET status='daily',locator_sha256='changed' WHERE identity_id=12")
        with self.assertRaises(PaidScopeBlocked) as result:
            self.validate(12, 1002)
        self.assertEqual(result.exception.error_code, 'catalog_identity_changed')

    def test_owner_plan_and_member_plan_cannot_be_substituted(self):
        self.save_plan(2, self.plan)
        with self.assertRaises(PaidScopeBlocked):
            self.validate(scope=replace(self.scope, catalog_plan_id=2))
        self.envelope(2, catalog_plan_id=2)
        with self.assertRaises(PaidScopeBlocked):
            self.validate(12, 1002)

    def test_frozen_batch_owner_and_target_cannot_be_substituted(self):
        self.connection.execute('UPDATE fetch_request_batches SET work_id=3 WHERE id=9')
        with self.assertRaises(PaidScopeBlocked):
            self.validate(12, 1002)
        self.connection.execute('UPDATE fetch_request_batches SET work_id=1 WHERE id=9')
        self.connection.execute("UPDATE capture_work_items SET owner_token='other-owner' WHERE id=2")
        with self.assertRaises(PaidScopeBlocked):
            self.validate(12, 1002)

    def test_single_work_cannot_borrow_other_content_in_same_eligible_account(self):
        self.connection.execute("UPDATE capture_work_items SET operation='douyin_video_detail' WHERE id=1")
        self.envelope(1, operation='douyin_video_detail', request_batch_id=None)
        self.assertEqual(self.validate()['account_id'], 101)
        with self.assertRaises(PaidScopeBlocked):
            self.validate(content=1002)
        with self.assertRaises(PaidScopeBlocked):
            self.validate(scope=replace(self.scope, manual_command_run_id=42))

    def test_modified_or_missing_plan_and_unknown_identity_fail_closed(self):
        self.connection.execute("UPDATE capture_source_plans SET plan_sha256='changed' WHERE id=1")
        with self.assertRaises(PaidScopeBlocked):
            self.validate()
        self.save_plan(1, self.plan)
        with self.assertRaises(PaidScopeBlocked):
            catalog.validate_plan_member(self.connection, 1, 99, at=AT)
        with self.assertRaises(PaidScopeBlocked):
            catalog.validate_plan_member(self.connection, 99, 11, at=AT)

    def test_shadow_plan_cannot_send_even_with_valid_hashes(self):
        self.save_plan(1, {**self.plan, 'shadow': True, 'catalog_mode': 'shadow'})
        with self.assertRaises(PaidScopeBlocked) as result:
            self.validate()
        self.assertEqual(result.exception.error_code, 'catalog_plan_changed')
        self.save_plan(1, self.plan)
        self.connection.execute("UPDATE capture_source_plans SET mode='shadow' WHERE id=1")
        with self.assertRaises(PaidScopeBlocked):
            self.validate()

    def test_plan_envelope_and_live_activation_must_agree(self):
        self.envelope(1, activation_id=99)
        with self.assertRaises(PaidScopeBlocked):
            self.validate()
        self.envelope(1, activation_id=4)
        self.active.return_value = {**self.active.return_value, 'activation_id': 99}
        with self.assertRaises(PaidScopeBlocked) as result:
            self.validate()
        self.assertEqual(result.exception.error_code, 'profile_superseded')

    def test_batch_freeze_preserves_pair_and_rejects_mixed_catalog_plans(self):
        self.connection.execute('BEGIN IMMEDIATE')
        value = batches.freeze_batch(self.connection, work_ids=[1, 2], at=AT)
        self.assertEqual(value['batch_id'], 9)
        self.assertEqual(len(value['members']), 2)
        self.envelope(2, catalog_plan_id=2)
        with self.assertRaisesRegex(ValueError, 'exclusive source plan'):
            batches.freeze_batch(self.connection, work_ids=[1, 2], at=AT)

    def test_planning_cache_is_connection_scoped_and_paid_checks_ignore_it(self):
        self.connection.execute('BEGIN IMMEDIATE')
        with catalog.planning_validation(self.connection, POLICY, self.snapshot):
            for _ in range(3):
                catalog.validate_plan_member(self.connection, 1, 11, at=AT)
            self.assertEqual(self.installed.call_count, 0)
            self.assertEqual(self.resolver.call_count, 0)
            self.validate()
            self.assertEqual(self.installed.call_count, 1)
            self.assertEqual(self.resolver.call_count, 1)
            self.connection.execute("UPDATE current_members SET status='paused' WHERE identity_id=11")
            with self.assertRaises(PaidScopeBlocked):
                self.validate()
        self.connection.rollback()
        self.assertIsNone(catalog._planning_cache(self.connection))
        self.validate()
        self.assertEqual(self.resolver.call_count, 3)
        with self.assertRaises(ValueError):
            with catalog.planning_validation(self.connection, POLICY, self.snapshot):
                pass

    def real_paid_scope(self):
        """Add the durable-attempt/identity tables used by the actual A/B gate."""
        c = self.connection
        c.executescript('''
            ALTER TABLE scheduler_runs ADD COLUMN status TEXT DEFAULT 'running';
            CREATE TABLE scheduler_run_attempts(id INTEGER PRIMARY KEY,scheduler_run_id INTEGER,
                status TEXT,details_json TEXT,attempt_number INTEGER);
            CREATE TABLE account_roster_snapshots(id INTEGER PRIMARY KEY,source_family TEXT);
            CREATE TABLE account_platform_identities(id INTEGER PRIMARY KEY,account_id INTEGER,uid TEXT,platform TEXT);
            ALTER TABLE content_items ADD COLUMN account_id INTEGER;
            ALTER TABLE content_items ADD COLUMN raw_account_uid TEXT;
        ''')
        identity = {'work_identity': 'work-1', 'catalog_plan_id': 1}
        owner = {'token': 'owner-token', 'attempt_id': 8, 'attempt_number': 1}
        details = {'contract_version': 'durable-run-v1', 'scan_id': 'catalog-scan',
            'identity': identity, 'owner': owner, 'checkpoint': {'work_id': 1}}
        c.execute('UPDATE scheduler_runs SET details_json=? WHERE id=1', (planning.canonical(details),))
        c.execute("INSERT INTO scheduler_run_attempts VALUES(8,1,'running',?,1)",
            (planning.canonical({'identity': identity, 'owner': owner, 'scan_id': 'catalog-scan'}),))
        for member in self.members:
            c.execute('INSERT INTO account_platform_identities VALUES(?,?,?,?)',
                (member['identity_id'], member['account_id'], member['uid'], member['platform']))
            c.execute('UPDATE content_items SET account_id=?,raw_account_uid=? WHERE id=?',
                (member['account_id'], member['uid'], member['directory_row_id'] + 1000))
        return replace(self.scope, scheduler_attempt_id=8, scheduler_scan_id='catalog-scan')

    def freeze(self, scope, content=1001):
        return provider_budget.freeze_scope(self.connection, scope=scope,
            content_id=content, account_id=None, stage='metrics')

    def test_actual_paid_gate_requires_live_attempt_without_legacy_membership(self):
        scope = self.real_paid_scope()
        with patch.object(provider_budget, 'require_active_member',
                side_effect=AssertionError('catalog must derive eligibility without old roster')):
            self.assertEqual(self.freeze(scope).identity_id, 11)
            self.assertEqual(self.freeze(scope, 1002).identity_id, 12)
            with self.assertRaises(PaidScopeBlocked):
                self.freeze(scope, 1003)
            for changed in (replace(scope, scheduler_attempt_id=None),
                            replace(scope, scheduler_owner_token='replaced-owner'),
                            replace(scope, scheduler_scan_id='replaced-scan')):
                with self.subTest(scope=changed), self.assertRaises(PaidScopeBlocked) as result:
                    self.freeze(changed)
                self.assertEqual(result.exception.error_code, 'attempt_owner_lost')
            self.connection.execute("UPDATE scheduler_run_attempts SET status='succeeded' WHERE id=8")
            with self.assertRaises(PaidScopeBlocked) as result:
                self.freeze(scope)
            self.assertEqual(result.exception.error_code, 'attempt_owner_lost')

    def test_paid_gate_cannot_use_catalog_plan_after_installed_policy_disappears(self):
        scope = self.real_paid_scope()
        self.assertEqual(self.freeze(scope).identity_id, 11)
        self.installed.return_value = {}
        with self.assertRaises(PaidScopeBlocked):
            self.freeze(scope)

    def test_actual_batch_gate_refuses_parameter_smuggling_and_third_member(self):
        scope = self.real_paid_scope()
        identities = tuple(batches._identity([str(7000000000000000000 + n)], AT) for n in (1, 2))
        def validate():
            return capture._validate_batch_members(self.connection, batch_id=9,
                identities=identities, assignments=(1, 2), scope=scope, at=AT, window_key=AT)
        # Route/readiness and settled-usage evidence are independent boundaries;
        # retain the real durable owner, catalog eligibility and member checks.
        with patch.object(planning, 'require_send_route',
                side_effect=lambda connection, *, scope, **kwargs: {'id': scope.account_id - 100}), \
                patch.object(batches.usage_settlements, 'require_scope_available'):
            self.assertEqual(len(validate()), 2)
            original = self.connection.execute('SELECT parameters_json FROM fetch_request_batches WHERE id=9').fetchone()[0]
            self.connection.execute('UPDATE fetch_request_batches SET parameters_json=? WHERE id=9',
                (planning.canonical({'aweme_ids': ','.join(str(7000000000000000000 + n) for n in (1, 2, 3))}),))
            with self.assertRaises(PaidScopeBlocked) as result:
                validate()
            self.assertEqual(result.exception.error_code, 'batch_identity_invalid')
            self.connection.execute('UPDATE fetch_request_batches SET parameters_json=? WHERE id=9', (original,))
            self.connection.execute("INSERT INTO fetch_request_batch_members VALUES(3,9,'extra',0,1003,103)")
            with self.assertRaises(PaidScopeBlocked) as result:
                validate()
            self.assertEqual(result.exception.error_code, 'batch_identity_invalid')

    def test_manual_and_catalog_paid_contexts_cannot_inherit_each_other(self):
        for outer, inner in (({'catalog_plan_id': 1}, {'manual_command_run_id': 42}),
                             ({'manual_command_run_id': 42}, {'catalog_plan_id': 1})):
            with self.subTest(outer=outer), provider_budget.paid_scope('metrics', **outer):
                with self.assertRaises(PaidScopeBlocked) as result:
                    with provider_budget.paid_scope('metrics', **inner):
                        self.fail('mixed manual and catalog scope entered')
                self.assertEqual(result.exception.error_code, 'paid_scope_mismatch')


if __name__ == '__main__':
    unittest.main()
