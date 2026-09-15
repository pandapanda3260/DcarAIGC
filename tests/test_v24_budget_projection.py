"""Differential accounting checks for the covering index used by live guards."""
import json
import sqlite3
import unittest
from unittest.mock import patch

from v8 import provider_budget as budget, runtime_budget_projection as projection


class BudgetProjectionTest(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        self.addCleanup(self.connection.close)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute('CREATE TABLE provider_usage(id INTEGER PRIMARY KEY,provider TEXT,currency TEXT,'
                                'amount,recorded_at TEXT,operation TEXT,details_json TEXT)')
        self.connection.execute(projection.DDL)
        carry = dict(lifetime_unknown_count=0, lifetime_unknown_amount=0,
                     lifetime_unverified_count=0, lifetime_unverified_amount=0)
        self.enterContext(patch('v8.account_cleanup.budget_carry', return_value=carry))
        self.enterContext(patch.object(budget, 'circuit_state', return_value=None))

    def add(self, details, *, amount=.0123, at='2026-09-14T20:00:00Z', operation='douyin_video_detail'):
        self.connection.execute('INSERT INTO provider_usage VALUES(NULL,?,?,?,?,?,?)',
                                ('TikHub', 'USD', amount, at, operation, details))

    def result(self, version, excluded=None):
        self.connection.execute('PRAGMA user_version=' + str(version))
        try:
            return ('ok', budget.budget_summary(self.connection, at='2026-09-15T00:00:00+08:00',
                                               exclude_usage_id=excluded))
        except Exception as error:
            return ('error', type(error))

    def assert_parity(self, excluded=None):
        self.assertEqual(self.result(23, excluded), self.result(24, excluded))

    def test_all_states_days_categories_and_legacy_metadata(self):
        for state in ('reserved', 'sent', 'billing_unknown', 'charged_unverified', 'completed', None):
            for day in ('2026-09-15', '2026-08-01', '', None, False, True, 0, [], {}):
                self.add(json.dumps({'state': state, 'budget_day': day, 'category': 'detail',
                                     'budget_bucket': 'metrics', 'unused_provider_body': 'x' * 10000}))
        self.add('{}'); self.add(''); self.add(None)
        self.assert_parity()
        self.assert_parity(excluded=1)
        values = self.result(24)[1]
        self.assertGreater(values['billing_unknown']['unresolved_count'], values['billing_unknown']['budget_day_count'])

    def test_bad_historical_rows_still_fail_and_excluded_row_is_skipped(self):
        for value in ('broken JSON', '[]', 'null', 'true', '17'):
            self.connection.execute('DELETE FROM provider_usage')
            self.add(value, at='2020-01-01T00:00:00Z')
            self.assert_parity()
            self.assertEqual(self.result(24)[0], 'error')
            self.assert_parity(excluded=1)
            self.assertEqual(self.result(24, 1)[0], 'ok')
        for amount in (-1, 'not a number', .0000001):
            self.connection.execute('DELETE FROM provider_usage')
            self.add('{"budget_day":"2020-01-01"}', amount=amount)
            self.assert_parity()
            self.assertEqual(self.result(24)[0], 'error')

    def test_index_tracks_update_delete_and_savepoint_rollback(self):
        self.add('{"state":"reserved","budget_day":"2026-09-15"}')
        original = self.result(24)
        self.connection.execute('SAVEPOINT mutation')
        self.connection.execute('UPDATE provider_usage SET amount=.1,details_json=\'{"state":"billing_unknown"}\'')
        self.assert_parity(); self.assertNotEqual(original, self.result(24))
        self.connection.execute('ROLLBACK TO mutation')
        self.assertEqual(original, self.result(24))
        self.connection.execute('DELETE FROM provider_usage')
        self.assert_parity(); self.assertEqual(self.result(24)[1]['total_microusd'], 0)

    def test_query_uses_covering_projection_and_keeps_typed_fields(self):
        self.add('{"category":[],"state":"completed","budget_day":"2026-09-15"}')
        self.assert_parity()  # Unhashable category still raises in the original rule.
        query = ' '.join(row[3] for row in self.connection.execute('EXPLAIN QUERY PLAN ' + projection.SELECT_SQL))
        self.assertIn(projection.INDEX_NAME, query)
        self.assertNotIn('SCAN provider_usage', query)

    def test_value_parsing_is_local_to_one_call_and_keeps_amount_types(self):
        for amount in (1, 1.0, '1.000000'):
            for _ in range(10):
                self.add('{"state":"reserved","category":"detail","budget_day":"2026-09-15"}', amount=amount)
        self.assert_parity()
        details, money = budget._details, budget.micro_usd
        with patch.object(budget, '_details', wraps=details) as parsed, patch.object(budget, 'micro_usd', wraps=money) as amounts:
            self.assertEqual(self.result(24)[1]['total_microusd'], 30_000_000)
            self.assertEqual((parsed.call_count, amounts.call_count), (1, 3))
            self.result(24)
            self.assertEqual((parsed.call_count, amounts.call_count), (2, 6))

    def test_micro_precision_nonfinite_and_exclusion_remain_exact(self):
        for amount in ('NaN', 'Infinity', '-Infinity', '-0.000001', '0.0000001', None, b'1'):
            with self.subTest(amount=amount):
                self.connection.execute('DELETE FROM provider_usage')
                self.add('{"budget_day":"1999-01-01"}', amount=amount)
                self.assert_parity()
                self.assertEqual(self.result(24)[0], 'error')
                self.assertEqual(self.result(24, 1)[0], 'ok')
        for amount in ('0.000001', '-0.000000', '9007199254.740993'):
            self.connection.execute('DELETE FROM provider_usage')
            self.add('{"budget_day":"2026-09-15"}', amount=amount)
            self.assert_parity()
            self.assertEqual(self.result(24)[1]['total_microusd'], budget.micro_usd(amount))

    def test_day_and_exclusion_are_fresh_even_without_writes(self):
        self.add('{"state":"reserved","category":"detail"}', at='2026-09-14T15:59:59Z')
        self.add('{"state":"sent","category":"detail"}', at='2026-09-14T16:00:00Z')
        self.connection.execute('PRAGMA user_version=24')
        before = budget.budget_summary(self.connection, at='2026-09-14T15:59:59Z')
        after = budget.budget_summary(self.connection, at='2026-09-14T16:00:00Z', exclude_usage_id=1)
        self.assertEqual(before['budget_day'], '2026-09-14')
        self.assertEqual(after['budget_day'], '2026-09-15')
        self.assertEqual(after['total_microusd'], 12300)
        self.assertEqual(budget.budget_summary(self.connection, at='2026-09-14T16:00:00Z',
                                              exclude_usage_id=2)['total_microusd'], 0)
