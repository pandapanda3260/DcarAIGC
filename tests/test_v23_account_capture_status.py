"""Read-model priority: stage contracts precede missing account locators."""
import sqlite3
import unittest
from unittest.mock import patch
from v8.account_catalog_capture import annotate_accounts, _public_platform_blocks
from v8.account_catalog_capture_release import ACCOUNT_CATALOG_POLICY_V2
from v8.capture_planning import canonical, digest
from v8.platform_adapters import PROFILE_OPERATIONS, POST_OPERATIONS


class AccountCaptureStatusTest(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:'); self.addCleanup(self.db.close)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA user_version=23')
        self.db.execute('CREATE TABLE capture_paid_send_gate_events(id INTEGER PRIMARY KEY,provider TEXT,operation TEXT,state TEXT,recorded_at TEXT)')
        self.db.execute('CREATE TABLE capture_source_plans(id INTEGER PRIMARY KEY,mode TEXT,payload_json TEXT,created_at TEXT)')

    def account(self, platform='kuaishou'):
        return {'id': -1, 'directory_row_id': 1, 'directory_platform': platform,
                'directory_uid': None, 'directory_identity_status': 'identity_missing', 'platforms': []}

    def gates(self, state='open'):
        for op in list(PROFILE_OPERATIONS.values()) + list(POST_OPERATIONS.values()):
            self.db.execute("INSERT INTO capture_paid_send_gate_events(provider,operation,state,recorded_at) VALUES('tikhub',?,?,'2026-01-01T00:00:00Z')", (op,state))

    def test_no_snapshot_shows_missing_source_before_missing_identity(self):
        accounts = [self.account(platform) for platform in PROFILE_OPERATIONS]
        before = self.db.total_changes
        with patch('v8.account_capture_eligibility.derive_capture_eligibility', side_effect=AssertionError('replica must not read raw')):
            annotate_accounts(self.db, accounts)
        self.assertTrue(all(a['automatic_capture']['reason_code'] == 'platform_source_unconfigured' for a in accounts))
        self.assertEqual(before, self.db.total_changes)
        self.gates()
        annotate_accounts(self.db, accounts)
        self.assertTrue(all(a['automatic_capture']['reason_code'] == 'identity_missing' for a in accounts))

    def test_closed_or_diagnostic_gate_is_not_disguised_as_identity_missing(self):
        for state, reason in [('closed','provider_transport_blocked'),('diagnostic_only','provider_diagnostic_only')]:
            self.gates(state)
            current = self.account()
            annotate_accounts(self.db, [current])
            self.assertEqual(current['automatic_capture']['reason_code'], reason)

    def test_old_runtime_policy_keeps_new_platform_disabled(self):
        self.gates()
        snapshot = {'policy_sha256':digest(ACCOUNT_CATALOG_POLICY_V2),
            'eligibility': {'eligible_members':[], 'excluded_members':[]}}
        self.db.execute("INSERT INTO capture_source_plans VALUES(1,'active',?,'2026-01-01')",
            (canonical({'catalog_mode':'active','shadow':False,'catalog_snapshot':snapshot}),))
        current = self.account()
        annotate_accounts(self.db, [current])
        self.assertEqual(current['automatic_capture']['reason_code'], 'platform_policy_unavailable')

    def test_no_snapshot_does_not_claim_prepared_account_is_capturable(self):
        self.gates()
        current = self.account(); current.update(id=1,directory_uid='1234',platforms=[{'id':1,'platform':'kuaishou','uid':'1234'}])
        annotate_accounts(self.db,[current])
        self.assertEqual(current['automatic_capture']['reason_code'],'capture_plan_pending')
        self.assertFalse(current['automatic_capture']['eligible'])
