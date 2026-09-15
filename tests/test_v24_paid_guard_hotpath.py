"""Differential live reads: no cached authority, ledger or cross-call budget."""
import json
import sqlite3
import unittest

from v8 import provider_budget as budget


class FaultReadTest(unittest.TestCase):
    def setUp(self):
        self.c = sqlite3.connect(':memory:')
        self.addCleanup(self.c.close)
        self.c.row_factory = sqlite3.Row
        self.c.executescript('''
            CREATE TABLE scheduler_runs(id INTEGER PRIMARY KEY,job_id TEXT,scheduled_for TEXT,
                root_run_id INTEGER,continuation_sequence INTEGER,charge_business_day TEXT,details_json TEXT);
            CREATE UNIQUE INDEX uq_scheduler_root_slot ON scheduler_runs(job_id,scheduled_for) WHERE root_run_id IS NULL;
            CREATE UNIQUE INDEX uq_scheduler_child_slot ON scheduler_runs(root_run_id,continuation_sequence,charge_business_day) WHERE root_run_id IS NOT NULL;
        ''')

    def add(self, job, details, child=False):
        number = self.c.execute('SELECT coalesce(max(id),0)+1 FROM scheduler_runs').fetchone()[0]
        return self.c.execute('INSERT INTO scheduler_runs VALUES(?,?,?,?,?,?,?)',
            (number, job, str(number), 1 if child else None, number, '2026-09-15',
             details if isinstance(details, str) else json.dumps(details))).lastrowid

    def result(self, version, function, *args, **kwargs):
        self.c.execute('PRAGMA user_version=' + str(version))
        try:
            return ('ok', function(self.c, *args, **kwargs))
        except Exception as error:
            return ('error', type(error), str(error))

    def parity(self, function, *args, **kwargs):
        old = self.result(23, function, *args, **kwargs)
        self.assertEqual(old, self.result(24, function, *args, **kwargs))
        return old

    def test_28_scopes_keep_root_child_latest_class_and_like_collation(self):
        scopes = [{'scope_kind': 'provider_hard', 'provider': 'tikhub'},
                  {'scope_kind': 'storage_hard', 'provider': 'all'}]
        scopes += [{'scope_kind': 'operation', 'provider': 'tikhub', 'operation': operation}
                   for operation in budget.PRICES_MICROUSD]
        self.assertEqual(len(scopes), 28)
        for scope in scopes:
            prefix = budget._fault_job_prefix(scope)
            old = {'contract_version': 'provider-fault-v2', **scope, 'fault_class': 'transport', 'open': False}
            self.add(prefix + 'old', old)
            newest = self.add(prefix + 'new', {**old, 'open': True}, child=True)
            self.add(prefix.upper() + 'mixed', {**old, 'fault_class': 'mixed'})
            # NULL jobs and unrelated malformed records must stay outside scope.
            self.add(None, 'not JSON', child=True)
            for case_sensitive in (False, True):
                self.c.execute('PRAGMA case_sensitive_like=' + str(int(case_sensitive)))
                result = self.parity(budget._v2_fault_states, scope)[1]
                self.assertEqual(next(row for row in result if row['fault_class'] == 'transport')['receipt_id'], newest)
                self.assertEqual(len(result), 1 if case_sensitive else 2)

    def test_current_legacy_and_bound_event_reselect_after_savepoint_rollback(self):
        scope = {'scope_kind': 'provider_hard', 'provider': 'tikhub'}
        legacy = self.add('provider_circuit:tikhub', {'open': True, 'reason': 'provider_outage'})
        first = self.parity(budget.fault_state, **scope)
        self.assertEqual(first[1]['contract_version'], 'provider-fault-v2-legacy-pending')
        self.c.execute('SAVEPOINT update_guard')
        event = self.add(budget._fault_job_prefix(scope) + 'resolved', {
            'contract_version': 'provider-fault-v2', **scope, 'fault_class': 'provider_outage',
            'open': False, 'state_evidence': {'legacy_circuit_receipt_id': legacy}}, child=True)
        resolved = self.parity(budget.fault_state, **scope)
        self.assertFalse(resolved[1]['open'])
        self.assertEqual(resolved[1]['receipt_id'], event)
        newer = self.add('provider_circuit:tikhub', {'open': False}, child=True)
        self.assertEqual(self.parity(budget._legacy_provider_circuit)[1]['receipt_id'], newer)
        self.c.execute('ROLLBACK TO update_guard')
        self.assertEqual(first, self.parity(budget.fault_state, **scope))
        self.assertEqual(self.parity(budget._legacy_provider_circuit)[1]['receipt_id'], legacy)

    def test_matched_malformed_json_and_non_object_preserve_exception(self):
        scope = {'scope_kind': 'storage_hard', 'provider': 'all'}
        for child in (False, True):
            for body in ('not JSON', '[]', 'null', '1'):
                with self.subTest(child=child, body=body):
                    self.c.execute('DELETE FROM scheduler_runs')
                    self.add(budget._fault_job_prefix(scope) + 'bad', body, child=child)
                    self.assertEqual(self.parity(budget._v2_fault_states, scope)[0], 'error')

    def test_query_plan_uses_both_existing_partial_indexes_without_ddl(self):
        self.c.execute('PRAGMA user_version=24')
        statements = []
        self.c.set_trace_callback(statements.append)
        budget._v2_fault_states(self.c, {'scope_kind': 'provider_hard', 'provider': 'tikhub'})
        self.c.set_trace_callback(None)
        sql = next(statement for statement in statements if 'UNION ALL' in statement)
        plan = ' '.join(row[3] for row in self.c.execute('EXPLAIN QUERY PLAN ' + sql))
        self.assertIn('uq_scheduler_root_slot', plan)
        self.assertIn('uq_scheduler_child_slot', plan)
        self.assertNotIn('CREATE ', ' '.join(statements))


if __name__ == '__main__':
    unittest.main()
