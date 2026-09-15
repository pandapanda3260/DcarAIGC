"""Bounded spawn workers for fresh read-only inheritance preparation.

Only the installed API explicitly starts these pools. The local lane cannot
queue behind capture work. Every submission runs the original preparation;
normal Python imports and the original verifier's generation-keyed backup
cache survive, but this module never caches proofs or file validity results.
"""
from __future__ import annotations

from concurrent.futures import CancelledError, ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
import multiprocessing
import os
from pathlib import Path
import sys
import threading
from urllib.parse import parse_qs, urlsplit


REQUEST_CONTRACT = 'runtime-proof-preparation-request-v1'
RESULT_CONTRACT = 'runtime-proof-preparation-result-v1'
_LIFECYCLE = threading.RLock()
_STATE = None
_AUDIT_INSTALLED = False
_REQUEST_KEYS = {'contract', 'request_id', 'database', 'inode', 'source', 'environment', 'logical_at'}


class ProofWorkerError(RuntimeError):
    error_code = 'capture_authorization_blocked'


class NotConfigured(ProofWorkerError):
    """Only this state allows the caller's original synchronous preparation."""


class Stopped(ProofWorkerError):
    """A configured pool stopped; never fall back to an in-lock cold proof."""


def _readonly_audit(event, arguments):
    if event in {'socket.connect', 'socket.connect_ex', 'socket.getaddrinfo', 'socket.sendto'}:
        raise ProofWorkerError('read-only proof workers cannot use network transports')
    if event != 'sqlite3.connect':
        return
    name = os.fsdecode(arguments[0])
    if name == ':memory:':
        return  # Pure schema validators construct temporary in-memory DDL.
    uri = urlsplit(name)
    options = parse_qs(uri.query, keep_blank_values=True)
    if uri.scheme == 'file' and (options.get('mode') == ['ro'] or
            uri.path == ':memory:' or options.get('mode') == ['memory']):
        return
    raise ProofWorkerError('read-only proof workers require mode=ro for disk SQLite')


def _initialize_worker():
    global _AUDIT_INSTALLED
    sys.dont_write_bytecode = True
    if not _AUDIT_INSTALLED:
        sys.addaudithook(_readonly_audit)
        _AUDIT_INSTALLED = True


def _ping():
    return os.getpid(), multiprocessing.get_start_method()


def _copy_request(request, database):
    if (type(request) is not dict or set(request) != _REQUEST_KEYS or
            request.get('contract') != REQUEST_CONTRACT or
            type(request.get('request_id')) is not str or len(request['request_id']) != 32 or
            any(c not in '0123456789abcdef' for c in request['request_id']) or
            request.get('database') != str(database) or type(request.get('source')) is not str or
            not Path(request['source']).is_absolute() or
            type(request.get('environment')) is not dict or
            any(type(k) is not str or type(v) is not str or not k or '=' in k or '\0' in k or '\0' in v
                for k, v in request['environment'].items()) or
            type(request.get('inode')) not in {tuple, list} or len(request['inode']) != 2 or
            any(type(value) is not int for value in request['inode']) or
            request.get('logical_at') is not None and type(request['logical_at']) is not str):
        raise ProofWorkerError('invalid read-only proof preparation request')
    return {**request, 'environment': dict(request['environment']), 'inode': tuple(request['inode'])}


def _validate_result(result, request):
    if (type(result) is not dict or set(result) != {'contract', 'request_id', 'prepared'} or
            result.get('contract') != RESULT_CONTRACT or result.get('request_id') != request['request_id'] or
            result['prepared'] is not None and type(result['prepared']) is not dict):
        raise ProofWorkerError('read-only proof worker returned an invalid result envelope')
    # The context validates every prepared field and request/build/DB binding.
    return result


def _run_preparation(request, *, _builder=None):
    # A process executes one task at a time. Replace the environment, including
    # deletions, before importing the builder or consuming any request inputs.
    try:
        os.environ.clear()
        os.environ.update(request['environment'])
        sys.dont_write_bytecode = True
        if _builder is None:
            from .runtime_evidence_context import build_prepared_wire
            _builder = build_prepared_wire
        # The private injection point lets spawn tests exercise the transport
        # without importing live authority or touching a production database.
        result = _builder(request)
        return _validate_result(result, request)
    except Exception as error:
        # Frozen ancestors may define exceptions in temporary module names;
        # normalize these before multiprocessing serializes the result.
        raise ProofWorkerError(f'read-only preparation failed: {type(error).__name__}: {error}') from None


class _Pools:
    def __init__(self, database):
        self.database = database
        # Imported verifier constants are process-local. A changed environment
        # requires an explicit new pool generation, even though every request
        # still installs and verifies its own captured environment.
        self.environment = dict(os.environ)
        self.owner_pid = os.getpid()
        self.condition = threading.Condition()
        self.closed = False
        self.failed = None
        self.executors = {}
        self.slots = {name: threading.BoundedSemaphore(count) for name, count in (('shared', 2), ('local', 1))}
        self.futures = set()
        self.worker = _run_preparation
        context = multiprocessing.get_context('spawn')
        try:
            for name, count in (('shared', 2), ('local', 1)):
                self.executors[name] = ProcessPoolExecutor(max_workers=count, mp_context=context,
                    initializer=_initialize_worker)
            warm = [self.executors[name].submit(_ping) for name, count in (('shared', 2), ('local', 1))
                    for _ in range(count)]
            for future in warm:
                pid, method = future.result(timeout=20)
                if pid == self.owner_pid or method != 'spawn':
                    raise ProofWorkerError('proof pool did not start isolated spawn workers')
        except BaseException:
            for executor in self.executors.values():
                executor.shutdown(wait=True, cancel_futures=True)
            raise

    def check(self):
        if self.owner_pid != os.getpid():
            raise ProofWorkerError('proof pools belong to another process')
        if self.closed:
            raise Stopped('read-only proof pools stopped')
        if self.failed is not None:
            raise ProofWorkerError('read-only proof pool failed; explicit restart is required')

    def prepare(self, request, lane):
        if lane not in self.executors:
            raise ProofWorkerError('unknown read-only proof lane')
        frozen = _copy_request(request, self.database)
        if frozen['environment'] != self.environment:
            raise ProofWorkerError('proof worker environment changed; explicit restart is required')
        slot = self.slots[lane]
        while True:
            with self.condition:
                self.check()
            # No Future or worker queue item exists while waiting for capacity.
            # The caller's existing monotonic budget continues throughout.
            if slot.acquire(timeout=0.1):
                break
        try:
            with self.condition:
                self.check()
                future = self.executors[lane].submit(self.worker, frozen)
                self.futures.add(future)
        except BaseException:
            slot.release()
            raise

        def complete(done):
            with self.condition:
                self.futures.discard(done)
                slot.release()
                self.condition.notify_all()
        future.add_done_callback(complete)
        try:
            result = future.result()
            with self.condition:
                self.check()  # Discard even successful results after stop.
            return _validate_result(result, frozen)
        except BrokenProcessPool as error:
            with self.condition:
                self.failed = str(error)
            raise ProofWorkerError('read-only proof worker exited unexpectedly') from error
        except CancelledError as error:
            raise Stopped('read-only proof preparation was discarded during stop') from error
        except BaseException:
            future.cancel()  # Running readonly work retains its slot until done.
            raise

    def stop(self):
        with self.condition:
            self.closed = True
            self.condition.notify_all()
            for future in tuple(self.futures):
                future.cancel()
        # Waiting callers see closed, running tasks finish readonly, and no
        # result can be consumed after closure. All children are then reaped.
        for executor in self.executors.values():
            executor.shutdown(wait=True, cancel_futures=True)


def start(database):
    """Explicit API lifecycle hook; it neither checks nor acquires a Writer lease."""
    global _STATE
    path = Path(database).resolve(strict=True)
    with _LIFECYCLE:
        if _STATE is not None and not _STATE.closed:
            _STATE.check()
            if _STATE.database != path:
                raise ProofWorkerError('read-only proof pools already serve another database')
            return
        _STATE = _Pools(path)


def stop():
    with _LIFECYCLE:
        if _STATE is not None:
            _STATE.stop()


def enabled():
    # Preserve configured-but-stopped state: racing callers must fail closed,
    # not mistake shutdown for permission to run a synchronous cold proof.
    with _LIFECYCLE:
        return _STATE is not None


def prepare(request, *, lane='shared'):
    with _LIFECYCLE:
        state = _STATE
    if state is None:
        raise NotConfigured('read-only proof pools have not been configured')
    return state.prepare(request, lane)
