"""Real spawn transport/lifecycle tests; every database and marker is private."""
from concurrent.futures import ThreadPoolExecutor
import json
import multiprocessing
import os
from pathlib import Path
import socket
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import uuid

from v8 import runtime_proof_workers as workers


_CALLS = 0


def _fixture_builder(request):
    """Importable child entry: no authority, production data, or provider calls."""
    global _CALLS
    _CALLS += 1
    options = json.loads(request['logical_at'] or '{}')
    action = options.get('action')
    result = {'contract': workers.RESULT_CONTRACT, 'request_id': request['request_id'],
              'prepared': {'pid': os.getpid(), 'calls': _CALLS,
                           'start_method': multiprocessing.get_start_method(),
                           'environment': dict(os.environ),
                           'bytecode_disabled': __import__('sys').dont_write_bytecode}}
    if action == 'hold':
        Path(options['started']).write_text(str(os.getpid()))
        deadline = time.monotonic() + 10
        while not Path(options['release']).exists():
            if time.monotonic() > deadline:
                raise RuntimeError('fixture release deadline exceeded')
            time.sleep(0.01)
    elif action == 'mutate_environment':
        os.environ['DCAR_PROOF_FIXTURE_MUTATION'] = 'child-only'
    elif action == 'error':
        class PrivateAncestorError(Exception):
            pass
        raise PrivateAncestorError('isolated fixture exception')
    elif action == 'crash':
        os._exit(17)
    elif action == 'wrong_request':
        result['request_id'] = 'f' * 32
    elif action == 'bare_none':
        return None
    elif action == 'legacy':
        result['prepared'] = None
    elif action == 'sqlite_read':
        connection = sqlite3.connect(Path(request['database']).as_uri() + '?mode=ro', uri=True)
        try:
            result['prepared']['rows'] = connection.execute('SELECT value FROM fixture').fetchall()
            try:
                connection.execute('INSERT INTO fixture VALUES (9)')
            except sqlite3.OperationalError as error:
                result['prepared']['write_error'] = str(error)
        finally:
            connection.close()
    elif action == 'sqlite_write':
        sqlite3.connect(request['database'])
    elif action == 'memory':
        connection = sqlite3.connect(':memory:')
        try:
            connection.execute('CREATE TABLE scratch (value INTEGER)')
            connection.execute('INSERT INTO scratch VALUES (7)')
            result['prepared']['rows'] = connection.execute('SELECT value FROM scratch').fetchall()
        finally:
            connection.close()
    elif action == 'network':
        connection = socket.socket()
        try:
            connection.connect(('127.0.0.1', 9))
        finally:
            connection.close()
    return result


def _fixture_entry(request):
    return workers._run_preparation(request, _builder=_fixture_builder)


class RuntimeProofWorkersTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='dcar-proof-spawn-')
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name).resolve()
        self.database = self.root / 'fixture.sqlite3'
        connection = sqlite3.connect(self.database)
        connection.execute('CREATE TABLE fixture (value INTEGER)')
        connection.execute('INSERT INTO fixture VALUES (3)')
        connection.commit()
        connection.close()
        self.enterContext(patch.object(workers, '_STATE', None))
        # LIFO: release any readonly fixture wait, reap children, then restore
        # the exact pre-test state. Product stop intentionally remains enabled.
        self.addCleanup(workers.stop)
        self.addCleanup(self.release_all)

    def release_all(self):
        for path in self.root.glob('release-*'):
            path.touch()

    def start(self):
        workers.start(self.database)
        workers._STATE.worker = _fixture_entry
        return workers._STATE

    def request(self, action=None, **options):
        if action is not None:
            options['action'] = action
        metadata = self.database.stat()
        return {'contract': workers.REQUEST_CONTRACT, 'request_id': uuid.uuid4().hex,
                'database': str(self.database), 'inode': (metadata.st_dev, metadata.st_ino),
                'source': str(self.root), 'environment': dict(os.environ),
                'logical_at': json.dumps(options) if options else None}

    def hold(self, label):
        started, release = self.root / f'started-{label}', self.root / f'release-{label}'
        # The release filename must exist in the cleanup inventory even before
        # release; use a separate registered list rather than create it early.
        self.addCleanup(release.touch)
        return self.request('hold', started=str(started), release=str(release)), started, release

    def wait_for(self, predicate, message='fixture did not reach expected state'):
        deadline = time.monotonic() + 5
        while not predicate():
            if time.monotonic() > deadline:
                self.fail(message)
            time.sleep(0.01)

    def test_unconfigured_distinct_from_stopped(self):
        self.assertFalse(workers.enabled())
        with self.assertRaises(workers.NotConfigured):
            workers.prepare(self.request())
        self.start()
        workers.stop()
        self.assertTrue(workers.enabled())
        with self.assertRaises(workers.Stopped):
            workers.prepare(self.request())

    def test_true_spawn_persistent_worker_runs_every_request(self):
        state = self.start()
        first = workers.prepare(self.request(), lane='local')['prepared']
        second = workers.prepare(self.request(), lane='local')['prepared']
        self.assertEqual(first['start_method'], 'spawn')
        self.assertNotEqual(first['pid'], os.getpid())
        self.assertEqual(first['pid'], second['pid'])
        self.assertEqual(second['calls'], first['calls'] + 1)
        self.assertTrue(second['bytecode_disabled'])
        self.assertEqual(len(state.executors['local']._processes), 1)
        self.assertEqual(len(state.executors['shared']._processes), 2)

    def test_same_database_start_idempotent_other_database_rejected(self):
        first = self.start()
        workers.start(self.database)
        self.assertIs(workers._STATE, first)
        other = self.root / 'other.sqlite3'
        other.touch()
        with self.assertRaisesRegex(workers.ProofWorkerError, 'another database'):
            workers.start(other)
        self.assertIs(workers._STATE, first)

    def test_stop_reaps_all_children_and_explicit_restart_creates_new_generation(self):
        state = self.start()
        children = [process for executor in state.executors.values() for process in executor._processes.values()]
        workers.stop()
        self.assertTrue(all(not process.is_alive() and process.exitcode is not None for process in children))
        replacement = self.start()
        self.assertIsNot(replacement, state)
        self.assertNotIn(workers.prepare(self.request(), lane='local')['prepared']['pid'],
                         {process.pid for process in children})

    def test_child_environment_reset_and_parent_environment_untouched(self):
        self.start()
        before = dict(os.environ)
        workers.prepare(self.request('mutate_environment'), lane='local')
        next_result = workers.prepare(self.request(), lane='local')['prepared']
        self.assertEqual(next_result['environment'], before)
        self.assertEqual(dict(os.environ), before)

    def test_environment_change_requires_explicit_restart(self):
        state = self.start()
        request = self.request()
        request['environment']['DCAR_PROOF_CHANGED_IMPORT_CONSTANT'] = 'changed'
        with self.assertRaisesRegex(workers.ProofWorkerError, 'environment changed'):
            workers.prepare(request)
        self.assertFalse(state.futures)
        with patch.dict(os.environ, {'DCAR_PROOF_CHANGED_IMPORT_CONSTANT': 'changed'}):
            workers.stop()
            self.start()
            result = workers.prepare(self.request(), lane='local')
            self.assertEqual(result['prepared']['environment']['DCAR_PROOF_CHANGED_IMPORT_CONSTANT'], 'changed')

    def test_request_shape_and_database_binding_rejected_before_submission(self):
        state = self.start()
        for field, value in [('contract', 'wrong'), ('request_id', 'bad'), ('database', '/elsewhere'),
                             ('source', 'relative'), ('inode', (True, 1)),
                             ('environment', {'bad=name': 'x'}), ('environment', {'x': '\0'})]:
            with self.subTest(field=field, value=value):
                request = self.request()
                request[field] = value
                with self.assertRaises(workers.ProofWorkerError):
                    workers.prepare(request)
                self.assertFalse(state.futures)
        with self.assertRaisesRegex(workers.ProofWorkerError, 'unknown'):
            workers.prepare(self.request(), lane='unbounded')

    def test_readonly_disk_sqlite_and_memory_validator_compatibility(self):
        self.start()
        result = workers.prepare(self.request('sqlite_read'), lane='local')['prepared']
        self.assertEqual(result['rows'], [(3,)])
        self.assertIn('readonly', result['write_error'])
        with self.assertRaisesRegex(workers.ProofWorkerError, 'mode=ro'):
            workers.prepare(self.request('sqlite_write'), lane='local')
        self.assertEqual(workers.prepare(self.request('memory'), lane='local')['prepared']['rows'], [(7,)])
        connection = sqlite3.connect(self.database)
        try:
            self.assertEqual(connection.execute('SELECT value FROM fixture').fetchall(), [(3,)])
        finally:
            connection.close()

    def test_network_denied_on_each_persistent_worker_request(self):
        self.start()
        for _ in range(2):
            with self.assertRaisesRegex(workers.ProofWorkerError, 'network transports'):
                workers.prepare(self.request('network'), lane='local')
        self.assertIsInstance(workers.prepare(self.request(), lane='local'), dict)

    def test_private_ancestor_exception_serializes_and_does_not_cache_failure(self):
        self.start()
        with self.assertRaisesRegex(workers.ProofWorkerError, 'PrivateAncestorError: isolated fixture'):
            workers.prepare(self.request('error'), lane='local')
        result = workers.prepare(self.request(), lane='local')
        self.assertEqual(result['prepared']['calls'], 2)

    def test_invalid_ipc_envelopes_fail_closed_legacy_none_is_explicit(self):
        self.start()
        for action in ('wrong_request', 'bare_none'):
            with self.subTest(action=action), self.assertRaisesRegex(workers.ProofWorkerError, 'invalid result envelope'):
                workers.prepare(self.request(action), lane='local')
        result = workers.prepare(self.request('legacy'), lane='local')
        self.assertIsNone(result['prepared'])
        self.assertTrue(workers.enabled())

    def test_worker_crash_poisoned_pool_requires_explicit_restart(self):
        self.start()
        with self.assertRaisesRegex(workers.ProofWorkerError, 'exited unexpectedly'):
            workers.prepare(self.request('crash'), lane='local')
        self.assertTrue(workers.enabled())
        with self.assertRaisesRegex(workers.ProofWorkerError, 'explicit restart'):
            workers.prepare(self.request(), lane='shared')
        workers.stop()

    def test_bounded_submissions_and_local_capacity_reserved(self):
        state = self.start()
        one, started_one, release_one = self.hold('one')
        two, started_two, release_two = self.hold('two')
        waiting, started_waiting, release_waiting = self.hold('waiting')
        executor = ThreadPoolExecutor(max_workers=4)
        self.addCleanup(executor.shutdown)
        # Release cleanup must precede thread-pool shutdown on any assertion.
        for release in (release_one, release_two, release_waiting):
            self.addCleanup(release.touch)
        first = executor.submit(workers.prepare, one)
        second = executor.submit(workers.prepare, two)
        self.wait_for(lambda: started_one.exists() and started_two.exists())
        queued = executor.submit(workers.prepare, waiting)
        local = executor.submit(workers.prepare, self.request(), lane='local')
        self.assertNotIn(local.result(timeout=5)['prepared']['pid'],
                         {int(started_one.read_text()), int(started_two.read_text())})
        self.assertFalse(started_waiting.exists())
        with state.condition:
            self.assertEqual(len(state.futures), 2)
        release_one.touch()
        self.wait_for(started_waiting.exists)
        for release in (release_two, release_waiting):
            release.touch()
        for future in (first, second, queued):
            self.assertIsInstance(future.result(timeout=5), dict)

    def test_stop_discards_running_result_and_unsubmitted_waiter(self):
        state = self.start()
        running, started, release = self.hold('stop')
        executor = ThreadPoolExecutor(max_workers=3)
        self.addCleanup(executor.shutdown)
        self.addCleanup(release.touch)
        first = executor.submit(workers.prepare, running, lane='local')
        self.wait_for(started.exists)
        waiting = executor.submit(workers.prepare, self.request(), lane='local')
        stopper = executor.submit(workers.stop)
        self.wait_for(lambda: state.closed)
        self.assertFalse(stopper.done())
        with self.assertRaises(workers.Stopped):
            waiting.result(timeout=2)
        release.touch()
        with self.assertRaises(workers.Stopped):
            first.result(timeout=5)
        stopper.result(timeout=5)
        self.assertTrue(workers.enabled())


if __name__ == '__main__':
    unittest.main()
