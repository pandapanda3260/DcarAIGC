"""Real WAL plan storage with closed proof boundaries and original logical time."""
from contextlib import contextmanager
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from v8 import account_catalog_capture as catalog, capture_plan_reuse as reuse
from v8 import runtime_evidence_context as evidence, storage
from v8.account_catalog_capture_release import ACCOUNT_CATALOG_POLICY_V3 as POLICY
from v8.storage import connect, transaction
from tests.test_v23_capture_plan_reuse import ACTIVE, AT
from tests.test_v24_duplicate_index import create_schema24_fixture


class CapturePlanPreparationV24Test(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.db = Path(temporary.name) / 'plan.sqlite3'
        self.writer, _ = create_schema24_fixture(self.db)
        self.addCleanup(self.writer.close)
        # Keep a live writer open and a fresh committed generation in WAL.
        self.writer.execute('UPDATE capture_catalog_revision SET revision=11 WHERE id=1')
        self.writer.commit()
        self.preparations = 0
        self.active_preparation = None
        self.events = []
        self.reject_exit = None
        self.enterContext(patch('socket.socket.connect', side_effect=AssertionError('network forbidden')))
        self.enterContext(patch.object(reuse, 'activation_at', return_value=ACTIVE))
        self.enterContext(patch.object(catalog, 'installed_policy', side_effect=self.policy))
        self.enterContext(patch.object(evidence, 'prepare_inheritance', side_effect=self.prepare))
        self.enterContext(patch.object(evidence, 'inheritance_boundary', side_effect=self.boundary))

    @contextmanager
    def prepare(self, path, *, logical_at):
        self.assertEqual(path, self.db)
        self.assertEqual(logical_at, AT)
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
        self.assertIsNotNone(self.active_preparation)
        self.assertTrue(connection.in_transaction)
        readonly = connection.execute('PRAGMA query_only').fetchone()[0] == 1
        identifier = self.active_preparation
        if readonly:
            self.assertNotEqual(storage._SQLITE_WRITE_TRANSACTION_LOCK._owner, threading.get_ident())
        else:
            self.assertEqual(storage._SQLITE_WRITE_TRANSACTION_LOCK._owner, threading.get_ident())
        self.events.append(('entry', identifier, readonly))
        yield
        self.assertTrue(connection.in_transaction, 'exit fence must precede rollback/commit')
        self.assertEqual(self.active_preparation, identifier)
        self.events.append(('exit', identifier, readonly))
        if self.reject_exit == readonly:
            raise evidence.RuntimeEvidenceChanged('simulated changed proof at exit fence')

    def policy(self, connection, *, at):
        self.assertEqual(at, AT)
        self.assertIsNotNone(self.active_preparation)
        self.assertTrue(connection.in_transaction)
        self.assertEqual(connection.execute('PRAGMA query_only').fetchone()[0], 1)
        self.assertGreaterEqual(connection.execute('SELECT revision FROM capture_catalog_revision WHERE id=1').fetchone()[0], 11)
        return POLICY

    def count(self, table):
        return self.writer.execute('SELECT COUNT(*) FROM ' + table).fetchone()[0]

    def test_live_wal_read_and_write_share_one_preparation_with_original_time(self):
        result = reuse.ensure(self.db, at=AT)
        self.assertEqual(result['key']['catalog_revision'], 11)
        self.assertEqual(result['key']['business_day'], '2026-09-12')
        self.assertEqual(self.preparations, 1)
        self.assertEqual(self.events, [('entry', 1, True), ('exit', 1, True), ('entry', 1, False), ('exit', 1, False)])
        self.assertEqual(self.count('capture_source_plans'), 1)
        self.assertEqual(self.count('capture_plan_reuse'), 1)
        self.assertEqual(self.count('provider_usage'), 0)

    def test_stored_plan_return_still_fences_before_read_rollback(self):
        first = reuse.ensure(self.db, at=AT)
        self.events.clear()
        second = reuse.ensure(self.db, at=AT)
        self.assertEqual(first['plan']['id'], second['plan']['id'])
        self.assertTrue(second['reused'])
        self.assertEqual(self.preparations, 2)
        self.assertEqual(self.events, [('entry', 2, True), ('exit', 2, True)])

    def test_catalog_race_retries_with_a_new_preparation_and_keeps_cas(self):
        original = catalog.freeze_snapshot
        changed = []

        def freeze(connection, **kwargs):
            snapshot = original(connection, **kwargs)
            if not changed:
                changed.append(True)
                with connect(self.db) as writer, transaction(writer):
                    writer.execute('UPDATE capture_catalog_revision SET revision=revision+1 WHERE id=1')
            return snapshot

        with patch.object(catalog, 'freeze_snapshot', side_effect=freeze):
            result = reuse.ensure(self.db, at=AT)
        self.assertEqual(result['key']['catalog_revision'], 12)
        self.assertEqual(self.preparations, 2)
        self.assertEqual([row[1] for row in self.events if row[0] == 'entry'], [1, 1, 2, 2])
        self.assertEqual(self.count('capture_source_plans'), 1)

    def test_read_exit_revocation_creates_no_plan(self):
        self.reject_exit = True
        with self.assertRaises(evidence.RuntimeEvidenceChanged):
            reuse.ensure(self.db, at=AT)
        self.assertEqual(self.count('capture_source_plans'), 0)
        self.assertEqual(self.count('capture_plan_reuse'), 0)
        self.assertEqual(self.events, [('entry', 1, True), ('exit', 1, True)])

    def test_write_exit_revocation_rolls_back_plan_and_key(self):
        self.reject_exit = False
        with self.assertRaises(evidence.RuntimeEvidenceChanged):
            reuse.ensure(self.db, at=AT)
        self.assertEqual(self.count('capture_source_plans'), 0)
        self.assertEqual(self.count('capture_plan_reuse'), 0)
        self.assertEqual(self.count('provider_usage'), 0)

    def test_preparation_failure_does_not_enter_a_transaction_boundary(self):
        with patch.object(evidence, 'prepare_inheritance', side_effect=evidence.RuntimeEvidenceChanged('worker rejected proof')):
            with self.assertRaises(evidence.RuntimeEvidenceChanged):
                reuse.ensure(self.db, at=AT)
        self.assertEqual(self.events, [])
        self.assertEqual(self.count('capture_source_plans'), 0)


class PaidDiscoveryPlanPreparationV24Test(unittest.TestCase):
    def test_saved_page_metric_enqueue_borrows_real_installed_work_proof(self):
        from datetime import datetime, timedelta, timezone
        import hashlib
        from tests.fixtures_v24_installed_paid import InstalledDuplicateFixture
        from v8 import capture_runtime, providers, runtime_proof_workers
        fixture = InstalledDuplicateFixture(); fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        f = fixture.f
        self.enterContext(patch('socket.socket.connect', side_effect=AssertionError('network forbidden')))
        self.enterContext(patch.object(runtime_proof_workers, 'enabled', return_value=False))
        # This release fixture deliberately makes its disposable DB "formal".
        # Preserve the process-wide formal-DB deny flag and confine every
        # application connector to this exact file/inode instead.
        import os
        import sqlite3
        import sys
        from v8 import runtime_database
        identity = (f.db.stat().st_dev, f.db.stat().st_ino)
        original_connect = storage.connect

        def fixture_connect(path, *, read_only=None):
            selected = Path(path).resolve(strict=True)
            self.assertEqual(selected, f.db.resolve(strict=True))
            self.assertFalse(Path(path).is_symlink())
            self.assertEqual((selected.stat().st_dev, selected.stat().st_ino), identity)
            if read_only is None:
                read_only = os.environ.get('DCAR_READ_ONLY', '0') == '1'
            query = 'mode=ro' if read_only else 'mode=rw'
            if read_only and not storage._LIVE_WAL_READ_ONLY.get():
                query += '&immutable=1'
            connection = sqlite3.connect(selected.as_uri() + '?' + query, uri=True,
                factory=storage._ClosingSQLiteConnection)
            connection.row_factory = sqlite3.Row
            try:
                storage.configure_connection_safety(connection)
                storage.require_schema_compatibility(connection, supported_versions=frozenset({24}))
                runtime_database.require_current_process_writer_lock(connection)
                connection.execute('PRAGMA query_only=ON' if read_only else 'PRAGMA journal_mode=WAL')
                return connection
            except Exception:
                connection.close()
                raise

        for name, module in tuple(sys.modules.items()):
            if name.startswith('v8') and getattr(module, 'connect', None) is original_connect:
                self.enterContext(patch.object(module, 'connect', side_effect=fixture_connect))
        temporary_connect = storage.connect

        def restore_late_imports():
            # A lazily imported module must not retain this fixture connector
            # after the individual test's patch contexts have unwound.
            for name, module in tuple(sys.modules.items()):
                if name.startswith('v8') and getattr(module, 'connect', None) is temporary_connect:
                    module.connect = original_connect

        self.addCleanup(restore_late_imports)
        now = datetime.now(timezone.utc).isoformat()
        uid = f.member['uid']; account_id = f.member['account_id']
        page = {'items': [{'platform': 'douyin', 'platform_content_id': '7599999999999999924', 'account_uid': uid,
            'title': 'offline saved discovery item', 'body': 'saved local page',
            'canonical_url': 'https://www.douyin.com/video/7599999999999999924',
            'content_type': 'video', 'published_at': now, 'metrics': {}}]}
        encoded = providers.json.dumps(page).encode()
        raw_path = f.root / 'nested-discovery.json'; raw_path.write_bytes(encoded)
        with storage.transaction(f.connection):
            slot = f.connection.execute('''INSERT INTO fetch_slots(account_id,stage,window_key,provider,
                adapter_version,status,attempt_count,created_at,updated_at)
                VALUES(?,'discovery','nested-offline-page','TikHub','fixture','succeeded',1,?,?)''',
                (account_id, now, now)).lastrowid
            attempt = f.connection.execute('''INSERT INTO fetch_attempts(slot_id,attempt_number,
                request_started_at,response_finished_at,http_status,billed) VALUES(?,1,?,?,200,0)''',
                (slot, now, now)).lastrowid
            raw_id = f.connection.execute('''INSERT INTO provider_raw_responses(fetch_attempt_id,account_id,
                provider,operation,local_path,sha256,byte_size,http_status,captured_at,source)
                VALUES(?,?,'TikHub','douyin_user_posts',?,?,?,200,?,'offline_fixture')''',
                (attempt, account_id, str(raw_path), hashlib.sha256(encoded).hexdigest(), len(encoded), now)).lastrowid
        before_usage = f.connection.execute('SELECT COUNT(*) FROM provider_usage').fetchone()[0]
        with patch.object(evidence, 'build_prepared_wire', wraps=evidence.build_prepared_wire) as builds:
            with evidence.prepare_inheritance(f.db) as outer:
                self.assertIsNotNone(outer)
                self.assertIsNone(outer.logical_at)
                result = providers.materialize_account_discovery_page(
                    account_id=account_id, platform='douyin', account_uid=uid, page=page,
                    source_raw_response_id=raw_id, metrics_window_key='nested-offline-page',
                    discovery_operation='douyin_user_posts', provider='TikHub',
                    derived_adapter_version='fixture-derived',
                    derived_operations={'detail': 'douyin_discovery_detail', 'metrics': 'douyin_discovery_metrics'},
                    zero_view_is_authoritative=False, materialize_detail=False,
                    materialize_existing_stages=False, db_path=f.db,
                    derived_raw_root=f.root / 'nested-derived', media_root=f.root / 'nested-media')
                self.assertEqual(result['inserted'], 1, result)
                self.assertEqual(result['metric_followup']['status'], 'planned')
                self.assertEqual(result['metric_followup']['content_count'], 1)
                self.assertIs(evidence._PREPARED.get(), outer)
                self.assertEqual(builds.call_count, 1, 'nested local plan must not build a second proof')
                too_old = (datetime.fromisoformat(outer.at.replace('Z', '+00:00')) - timedelta(seconds=1)).isoformat()
                with self.assertRaisesRegex(evidence.RuntimeEvidenceChanged, 'clock moved'):
                    reuse.ensure(f.db, at=too_old)
                self.assertIs(evidence._PREPARED.get(), outer)
        self.assertIsNone(evidence._PREPARED.get())
        self.assertEqual(f.connection.execute('SELECT COUNT(*) FROM provider_usage').fetchone()[0], before_usage)


if __name__ == '__main__':
    unittest.main()
