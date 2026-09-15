"""Real migrated WAL receipts; proof orchestration only is substituted.

Coverage uses the established one-member receipt fixture. This suite verifies
preparation placement, exact cutoff reuse and transactional rollback; it does
not represent an installed release-authority acceptance test.
"""
from contextlib import contextmanager, nullcontext
import threading
import os
import sqlite3
import unittest
from unittest.mock import patch

from tests import test_v8_runtime_receipts as fixtures
from v8 import runtime_receipts as receipts, runtime_evidence_context as evidence
from v8 import storage, capture_release, account_cleanup_runtime as cleanup


class RuntimeReceiptPreparationV24Test(unittest.TestCase):
    def setUp(self):
        fixture = self.fixture = fixtures.RuntimeReceiptsTest(methodName='runTest')
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.db = fixture.db
        self.writer = storage.connect(self.db)
        self.addCleanup(self.writer.close)
        storage.initialize_database(self.writer, target_version=24)
        self.writer.execute('UPDATE capture_catalog_revision SET revision=17 WHERE id=1')
        self.writer.commit()
        self.preparations = 0
        self.active_preparation = None
        self.events = []
        self.reject_exit = None
        self.read_connections = []
        self.enterContext(patch('socket.socket.connect', side_effect=AssertionError('network forbidden')))
        self.enterContext(patch.object(evidence, 'prepare_inheritance', side_effect=self.prepare))
        self.enterContext(patch.object(evidence, 'inheritance_boundary', side_effect=self.boundary))
        self.coverage = self.enterContext(patch('v8.scan_receipts.runtime_coverage', side_effect=self.make_coverage))
        original_write = receipts._write_evidence

        def write(*args, **kwargs):
            self.assertNotEqual(storage._SQLITE_WRITE_TRANSACTION_LOCK._owner, threading.get_ident())
            self.assertIsNotNone(self.active_preparation)
            for connection in self.read_connections:
                # storage's closing context has already released the read snapshot.
                with self.assertRaisesRegex(sqlite3.ProgrammingError, 'closed database'):
                    connection.execute('SELECT 1')
            return original_write(*args, **kwargs)

        self.enterContext(patch.object(receipts, '_write_evidence', side_effect=write))

    @contextmanager
    def prepare(self, path, *, logical_at):
        self.assertEqual(path, self.db)
        self.assertEqual(logical_at, fixtures.NOW)
        self.assertNotEqual(storage._SQLITE_WRITE_TRANSACTION_LOCK._owner, threading.get_ident())
        self.assertIsNone(self.active_preparation)
        self.preparations += 1
        self.active_preparation = self.preparations
        try:
            yield object()
        finally:
            self.active_preparation = None

    @contextmanager
    def boundary(self, connection):
        self.assertTrue(connection.in_transaction)
        self.assertIsNotNone(self.active_preparation)
        readonly = connection.execute('PRAGMA query_only').fetchone()[0] == 1
        if readonly:
            self.read_connections.append(connection)
            self.assertNotEqual(storage._SQLITE_WRITE_TRANSACTION_LOCK._owner, threading.get_ident())
        else:
            self.assertEqual(storage._SQLITE_WRITE_TRANSACTION_LOCK._owner, threading.get_ident())
        self.events.append(('entry', self.active_preparation, readonly))
        yield
        self.assertTrue(connection.in_transaction)
        self.events.append(('exit', self.active_preparation, readonly))
        if self.reject_exit == readonly:
            raise evidence.RuntimeEvidenceChanged('simulated exit revocation')

    def make_coverage(self, connection, *, at):
        self.assertEqual(at, fixtures.NOW)
        self.assertIsNotNone(self.active_preparation)
        self.assertEqual(connection.execute('PRAGMA query_only').fetchone()[0], 1)
        self.assertEqual(connection.execute('SELECT revision FROM capture_catalog_revision').fetchone()[0], 17)
        return self.fixture._coverage()

    def seal(self):
        return receipts.record_profile_day_coverage_receipt(
            db_path=self.db, cutoff_at='2026-09-01T09:00:00+08:00', evidence_root=self.fixture.evidence)

    def count(self):
        return self.writer.execute('SELECT COUNT(*) FROM profile_day_coverage_receipts').fetchone()[0]

    def test_original_cutoff_live_wal_and_same_proof_read_write(self):
        result = self.seal()
        self.assertEqual(result['recorded_at'], fixtures.NOW)
        self.assertEqual(self.events, [('entry', 1, True), ('exit', 1, True), ('entry', 1, False), ('exit', 1, False)])
        self.assertEqual(self.count(), 1)
        self.assertEqual(self.writer.execute('SELECT COUNT(*) FROM provider_usage').fetchone()[0], 0)

    def test_early_return_keeps_fence_before_snapshot_close(self):
        first = self.seal()
        self.events.clear()
        second = self.seal()
        self.assertEqual(first, second)
        self.assertEqual(self.events, [('entry', 2, True), ('exit', 2, True)])
        self.coverage.assert_called_once()
        self.assertEqual(self.count(), 1)

    def test_read_exit_revocation_writes_neither_evidence_nor_receipt(self):
        self.reject_exit = True
        with self.assertRaises(evidence.RuntimeEvidenceChanged):
            self.seal()
        self.assertEqual(self.count(), 0)
        self.assertFalse(self.fixture.evidence.exists())

    def test_write_exit_revocation_rolls_back_native_and_bridge(self):
        self.reject_exit = False
        with self.assertRaises(evidence.RuntimeEvidenceChanged):
            self.seal()
        self.assertEqual(self.count(), 0)
        self.assertEqual(self.writer.execute('SELECT COUNT(*) FROM scheduler_runs WHERE job_id=?', (receipts.DAY_RECEIPT_JOB,)).fetchone()[0], 0)
        self.assertEqual(len(list(self.fixture.evidence.glob('*.json'))), 1)

    def test_source_race_preserves_original_revision_cas(self):
        original = receipts._write_evidence

        def write(*args, **kwargs):
            result = original(*args, **kwargs)
            from v8 import account_states
            with storage.connect(self.db) as connection:
                account_states.set_account_enabled(
                    connection, self.fixture.identity_id, enabled=False,
                    effective_at=fixtures.NOW, actor='fixture', reason='source revision race',
                    created_at=fixtures.NOW, activation_id=self.fixture.active['activation_id'])
            return result

        with patch.object(receipts, '_write_evidence', side_effect=write):
            with self.assertRaisesRegex(receipts.RuntimeReceiptError, 'coverage source changed'):
                self.seal()
        self.assertEqual(self.count(), 0)

    def test_cleanup_release_direct_proof_observes_original_boundary_and_time(self):
        control = {'contract': receipts._CLEANUP_CONTROL_CONTRACT}
        previous = self.writer.execute('SELECT id,event_hash FROM pipeline_paid_drain_events ORDER BY id DESC LIMIT 1').fetchone()
        previous_id, previous_hash = tuple(previous) if previous is not None else (None, None)
        for sequence, event_type in enumerate(('start', 'sealed', 'release'), 1):
            event_hash = str(sequence) * 64
            event_id = self.writer.execute('''INSERT INTO pipeline_paid_drain_events(
                drain_id,target_activation_id,sequence,event_type,payload_json,contract_version,event_hash,
                created_at,previous_event_id,previous_event_hash)
                VALUES('fixture-day-release',?,?,?,?,'fixture',?,?,?,?)''',
                (self.fixture.active['activation_id'], sequence, event_type,
                 receipts._json({'control': control}) if event_type == 'release' else '{}',
                 event_hash, '2026-08-31T01:00:00Z', previous_id, previous_hash)).lastrowid
            previous_id, previous_hash = event_id, event_hash
        self.writer.commit()

        def installed(connection, *, at, maintenance_only):
            self.assertEqual(at, fixtures.NOW)
            self.assertTrue(maintenance_only)
            self.assertTrue(connection.in_transaction)
            self.assertEqual(connection.execute('PRAGMA query_only').fetchone()[0], 1)
            self.assertEqual(self.events[-1], ('entry', 1, True))
            return {'active': self.fixture.active}

        with patch.object(capture_release, '_installed_evidence', side_effect=installed) as proof, \
                patch.object(capture_release, '_native_control', return_value=event_id), \
                patch.object(cleanup, 'validate_control'), \
                patch.object(cleanup, 'validate_decision', return_value={'fixture': True}):
            result = self.seal()
        proof.assert_called_once()
        self.assertEqual(result['scope']['release_event_id'], event_id)

    def test_legacy_keeps_environment_selected_read_only_connection(self):
        legacy = fixtures.RuntimeReceiptsTest(methodName='runTest')
        legacy.setUp()
        self.addCleanup(legacy.doCleanups)
        observed = []

        def read_coverage(connection, *, at):
            observed.append(connection.execute('PRAGMA query_only').fetchone()[0])
            self.assertEqual(observed, [1])
            self.assertEqual(at, fixtures.NOW)
            raise RuntimeError('legacy read-only snapshot observed')

        with patch.dict(os.environ, {'DCAR_READ_ONLY': '1'}), \
                patch.object(evidence, 'inheritance_boundary', side_effect=lambda c: nullcontext()), \
                patch.object(evidence, 'prepare_inheritance', side_effect=AssertionError('legacy must not prepare')), \
                patch('v8.scan_receipts.runtime_coverage', side_effect=read_coverage):
            with self.assertRaisesRegex(RuntimeError, 'legacy read-only snapshot observed'):
                receipts.record_profile_day_coverage_receipt(db_path=legacy.db, cutoff_at=fixtures.NOW,
                    evidence_root=legacy.evidence)
        self.assertEqual(observed, [1])
        self.assertFalse(legacy.evidence.exists())

    def test_prepare_failure_never_opens_receipt_transaction(self):
        with patch.object(evidence, 'prepare_inheritance', side_effect=evidence.RuntimeEvidenceChanged('worker rejected')):
            with self.assertRaises(evidence.RuntimeEvidenceChanged):
                self.seal()
        self.assertEqual(self.events, [])
        self.assertEqual(self.count(), 0)


if __name__ == '__main__':
    unittest.main()
