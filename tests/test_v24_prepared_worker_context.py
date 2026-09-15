"""IPC ownership/freshness fences and local scope's original logical clock."""
from contextlib import contextmanager
import copy
from datetime import datetime, timezone
import os
import pickle
import queue
import sqlite3
import threading
import unittest
from unittest.mock import patch

from tests import test_v23_runtime_evidence_context as fixture_module
from v8 import pipeline, runtime_database, runtime_evidence_context as context
from v8 import runtime_proof_workers as workers


class PreparedWorkerContextTest(unittest.TestCase):
    def setUp(self):
        self.real_require_owner = runtime_database.require_current_process_writer_lock
        self.fixture = fixture_module.RuntimeEvidenceContextTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.f = self.fixture
        self.enterContext(patch.object(workers, "enabled", return_value=False))

    def wire(self, *, logical_at=None):
        request = context._preparation_request(self.f.db, logical_at=logical_at)
        return request, context.build_prepared_wire(request)

    def test_wire_contains_only_builtins_and_owner_is_parent_thread(self):
        request, result = self.wire()

        def check(value):
            self.assertIn(type(value), (dict, tuple, str, bytes, int, float, bool, type(None)))
            if type(value) is dict:
                for key, child in value.items():
                    check(key); check(child)
            elif type(value) is tuple:
                for child in value:
                    check(child)

        check(request); check(result)
        self.assertNotIn("owner_thread", result["prepared"])
        transported = pickle.loads(pickle.dumps(result))
        messages = queue.Queue()

        def receive():
            prepared = context._prepared_from_wire(request, transported)
            messages.put((threading.get_ident(), prepared))

        thread = threading.Thread(target=receive, name="fixture-parent-owner")
        thread.start(); thread.join(5)
        self.assertFalse(thread.is_alive())
        owner, prepared = messages.get_nowait()
        self.assertEqual(prepared.owner_thread, owner)
        self.assertNotEqual(owner, threading.get_ident())
        self.f.connection.execute("BEGIN")
        try:
            with self.assertRaisesRegex(context.RuntimeEvidenceChanged, "owned transaction"):
                prepared.validate(self.f.connection, at=context._now())
        finally:
            self.f.connection.rollback()
        with self.assertRaises(TypeError):
            prepared.files[self.f.code] = ("missing",)

    def test_wrong_request_installation_and_malformed_payload_fail_closed(self):
        request, result = self.wire()
        mutations = [
            lambda value: value.update(request_id="f" * 32),
            lambda value: value.update(prepared=None),
            lambda value: value["prepared"].update(owner_thread=123),
            lambda value: value["prepared"].update(inode=(1, 2)),
            lambda value: value["prepared"].update(source=str(self.f.root)),
            lambda value: value["prepared"].update(logical_at="2026-01-01T00:00:00Z"),
            lambda value: value["prepared"].update(files=((str(self.f.code), (1,)),)),
            lambda value: value["prepared"].update(queries=(("invalid", "query"),)),
        ]
        for number, mutate in enumerate(mutations):
            with self.subTest(number=number):
                changed = copy.deepcopy(result); mutate(changed)
                with self.assertRaises(context.RuntimeEvidenceChanged):
                    context._prepared_from_wire(request, changed)
        with patch.dict(os.environ, {"DCAR_WRITER_SOURCE_ROOT": str(self.f.root)}):
            with self.assertRaises(context.RuntimeEvidenceChanged):
                context._prepared_from_wire(request, result)

    def test_local_logical_clock_is_bound_separately_from_physical_clock(self):
        logical = "2026-09-09T00:00:00Z"
        with patch.object(context, "_now", return_value="2030-01-01T00:00:10Z"):
            with context.prepare_inheritance(self.f.db, lane="local", logical_at=logical) as prepared:
                self.assertEqual(prepared.at, "2030-01-01T00:00:10Z")
                self.assertEqual(prepared.logical_at, logical)
                self.assertEqual(self.f.verifier.call_args.kwargs["at"], logical)
                with self.f.boundary():
                    self.assertEqual(self.f.reuse(at=logical), {"immutable": "original"})
                    with self.assertRaisesRegex(context.RuntimeEvidenceChanged, "logical scope"):
                        self.f.reuse(at="2026-09-09T00:00:01Z")
                with patch.object(context, "_now", return_value="2030-01-01T00:00:09Z"):
                    with self.assertRaisesRegex(context.RuntimeEvidenceChanged, "clock moved"):
                        with self.f.boundary():
                            pass

    def test_nested_scope_cannot_change_logical_time_or_enter_active_boundary(self):
        logical = "2026-09-09T00:00:00Z"
        with context.prepare_inheritance(self.f.db, logical_at=logical) as prepared:
            with context.prepare_inheritance(self.f.db, logical_at=logical) as nested:
                self.assertIs(nested, prepared)
            self.assertEqual(self.f.calls, 1)
            with self.assertRaises(context.RuntimeEvidenceChanged):
                with context.prepare_inheritance(self.f.db):
                    pass
            with self.f.boundary():
                with self.assertRaises(context.RuntimeEvidenceChanged):
                    with context.prepare_inheritance(self.f.db, logical_at=logical):
                        pass
        with context.prepare_inheritance(self.f.db, logical_at=logical):
            pass
        self.assertEqual(self.f.calls, 2)

    def test_configured_worker_fault_does_not_fallback_or_leave_context(self):
        with patch.object(workers, "enabled", return_value=True), \
                patch.object(workers, "prepare", side_effect=RuntimeError("worker exited")) as worker, \
                patch.object(context, "build_prepared_wire", side_effect=AssertionError("unsafe fallback")):
            with self.assertRaisesRegex(RuntimeError, "worker exited"):
                with context.prepare_inheritance(self.f.db, lane="local"):
                    pass
        self.assertEqual(worker.call_args.kwargs, {"lane": "local"})
        self.assertIsNone(context._PREPARED.get())
        self.assertEqual(self.f.calls, 0)

    def test_transported_read_set_still_rejects_revocation_at_parent_entry(self):
        def transport(request, *, lane):
            response = pickle.loads(pickle.dumps(context.build_prepared_wire(request)))
            self.f.connection.execute("UPDATE proof SET value='revoked'")
            self.f.connection.commit()
            return response

        with patch.object(workers, "enabled", return_value=True), \
                patch.object(workers, "prepare", side_effect=transport):
            with context.prepare_inheritance(self.f.db):
                with self.assertRaisesRegex(context.RuntimeEvidenceChanged, "database proof changed"):
                    with self.f.boundary():
                        pass
        self.assertEqual(self.f.calls, 1)

    def test_readonly_reuse_requires_real_parent_writer_lease(self):
        lock = self.f.root / "fixture-owned.lock"; lock.touch(mode=0o600)
        installed = runtime_database.InstalledWriterContract(self.f.root, self.f.plist,
            self.f.root, self.f.source / "writer.py", self.f.db, lock, {})
        access = runtime_database.ResolvedDatabaseAccess(runtime_database.DatabaseAccessMode.WRITER,
            self.f.db, runtime_database.FileIdentity.from_stat(self.f.db.stat()), self.f.root, lock, installed)
        with context.prepare_inheritance(self.f.db) as prepared, \
                sqlite3.connect(self.f.db.as_uri() + "?mode=ro", uri=True) as reader, \
                patch.object(runtime_database, "require_current_process_writer_lock", self.real_require_owner):
            reader.execute("PRAGMA foreign_keys=ON"); reader.execute("PRAGMA recursive_triggers=ON")
            reader.execute("PRAGMA query_only=ON"); reader.execute("BEGIN")
            try:
                with self.assertRaises(runtime_database.RuntimeDatabaseError):
                    prepared.validate(reader, at=context._now())
                with runtime_database.acquire_writer_lock(access):
                    with context.inheritance_boundary(reader):
                        self.assertEqual(context.reuse_inheritance(connection=reader, build=self.f.build,
                            build_ref=self.f.build_ref, install_path=self.f.root / "install.json",
                            database=self.f.db, source=self.f.source, at=context._now()),
                            {"immutable": "original"})
            finally:
                reader.rollback()

    def test_local_policy_connection_reads_wal_and_preserves_business_time(self):
        database = self.f.root / "local-wal-fixture.sqlite3"
        writer = sqlite3.connect(database); self.addCleanup(writer.close)
        writer.executescript("PRAGMA journal_mode=WAL; PRAGMA user_version=24; "
                             "CREATE TABLE proof(value); INSERT INTO proof VALUES('committed WAL value');")
        logical = "2026-09-09T00:00:00Z"
        calls = []

        @contextmanager
        def preparation(database, **kwargs):
            self.assertFalse(self.f.connection.in_transaction)
            calls.append((database, kwargs))
            yield None

        with patch.object(context, "prepare_inheritance", side_effect=preparation):
            with pipeline._local_policy_connection(db_path=database, at=logical) as reader:
                self.assertTrue(reader.in_transaction)
                self.assertEqual(reader.execute("PRAGMA query_only").fetchone()[0], 1)
                self.assertEqual(reader.execute("SELECT value FROM proof").fetchone()[0], "committed WAL value")
                with self.assertRaises(sqlite3.OperationalError):
                    reader.execute("UPDATE proof SET value='forbidden'")
        self.assertEqual(calls, [(database, {"lane": "local", "logical_at": logical})])
        with self.assertRaises(sqlite3.ProgrammingError):
            reader.execute("SELECT 1")


class LocalPreparationBudgetTest(unittest.TestCase):
    def test_preparation_time_consumes_original_soft_budget(self):
        from tests import test_v24_duplicate_integration as media_fixture
        fixture = media_fixture.IndexedLocalAnalysisIntegrationTest()
        fixture.setUp(); self.addCleanup(fixture.doCleanups)
        fixture.fixture.content(1)
        clock = [0.0]
        seen = []

        @contextmanager
        def slow_preparation(database, **kwargs):
            seen.append((database, kwargs))
            clock[0] = 56.0
            yield None

        with patch.object(context, "prepare_inheritance", side_effect=slow_preparation), \
                patch.object(pipeline, "monotonic", side_effect=lambda: clock[0]), \
                patch.object(pipeline, "run_local_batch", side_effect=AssertionError("soft budget exhausted")) as media, \
                patch.object(pipeline, "run_duplicate_relation_update",
                    return_value={"relation_status": "pending", "results": []}) as drain:
            result = fixture.fixture.run_tick()
        self.assertEqual(result["processed"], 0)
        self.assertEqual(result["candidates"], 1)
        media.assert_not_called()
        self.assertEqual(seen, [(fixture.db, {"lane": "local", "logical_at": "2026-09-09T00:00:00Z"})])
        self.assertEqual(drain.call_args.kwargs["time_budget_seconds"], 4.0)
        fixture.fixture.assert_no_provider()


if __name__ == "__main__":
    unittest.main()
