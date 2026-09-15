"""Each schema23 planning write phase owns a separately fenced inheritance proof."""
from contextlib import ExitStack, contextmanager
import os
from pathlib import Path
import sqlite3
import sys
import threading
import unittest
from unittest.mock import patch

from tests import test_v23_runtime_evidence_context as fixtures
from v8 import account_preparation as preparation, capture_runtime as runtime
from v8 import capture_plan_reuse as plans, runtime_evidence_context as evidence, storage


AT = "2026-09-13T03:20:00Z"
LATER = "2026-09-13T03:20:10Z"
ACTIVE = {"activation_id": 1, "profile_id": "integrated_route_v1"}


class CapturePlanningInheritanceBoundaryTest(unittest.TestCase):
    def setUp(self):
        self.f = fixtures.RuntimeEvidenceContextTest(methodName="runTest")
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        self.f.connection.execute("CREATE TABLE planned(phase,at)")
        self.f.connection.commit()
        original = self.f.verifier.side_effect

        def prove(**kwargs):
            self.assertNotEqual(storage._SQLITE_WRITE_TRANSACTION_LOCK._owner, threading.get_ident())
            return original(**kwargs)

        self.f.verifier.side_effect = prove
        self.enterContext(patch.object(evidence, "_now", return_value=AT))
        self.enterContext(patch.object(runtime, "now_utc", return_value=LATER))
        self.enterContext(patch.object(runtime, "connect", self.connection))
        self.enterContext(patch.object(runtime, "activation_at", return_value=ACTIVE))
        self.enterContext(patch("v8.account_directory_reconciliation.reconcile_directory", return_value={}))
        self.profile = self.enterContext(patch.object(preparation, "prepare_profile_reuse", side_effect=self.profile_proof))
        self.enqueue = self.enterContext(patch.object(preparation, "enqueue_pending", side_effect=self.enqueue_pending))
        self.current = self.enterContext(patch.object(plans, "current", return_value=True))
        self.ensure = self.enterContext(patch.object(plans, "ensure", side_effect=self.stored_plan))
        self.enterContext(patch.object(runtime, "_plan_due", side_effect=self.plan_due))
        self.enterContext(patch("v8.account_catalog_capture.installed_policy", side_effect=self.policy))

    @contextmanager
    def connection(self, database):
        connection = sqlite3.connect(database)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA recursive_triggers=ON")
        try:
            yield connection
        finally:
            connection.close()

    def profile_proof(self, connection, *, active, at):
        self.assertNotEqual(storage._SQLITE_WRITE_TRANSACTION_LOCK._owner, threading.get_ident())
        return {"at": at}

    def policy(self, connection, *, at, **_):
        self.assertTrue(connection.in_transaction)
        self.assertEqual(evidence.reuse_inheritance(connection=connection, build=self.f.build,
            build_ref=self.f.build_ref, install_path=Path(self.f.install_ref["path"]),
            database=self.f.db, source=self.f.source, at=at), {"immutable": "original"})
        return {"account_preparation": preparation.CONTRACT}

    def enqueue_pending(self, connection, *, at, reuse_proof, **_):
        self.assertEqual(reuse_proof["at"], at)
        self.assertIsNotNone(preparation._policy(connection, at))
        connection.execute("INSERT INTO planned VALUES('preparation',?)", (at,))
        return {"created": 1}

    def stored_plan(self, *_, **__):
        self.assertIsNone(evidence._PREPARED.get(), "proof leaked across the preparation commit")
        return {"plan": {"id": 7, "cohort": []}, "policy": {}, "reused": True}

    def plan_due(self, connection, plan, *, at):
        with preparation.planning_reconsideration(connection, at=at):
            connection.execute("INSERT INTO planned VALUES('due',?)", (at,))
        return {"status": "planned", "plan_id": plan["id"], "provider_calls": 0}

    def run_tick(self):
        return runtime._plan_tick_v23(self.f.db, at=AT, shadow=False)

    def phases(self):
        return [tuple(row) for row in self.f.connection.execute("SELECT * FROM planned")]

    def test_both_policy_paths_prove_outside_lock_and_reuse_the_persisted_plan(self):
        result = self.run_tick()
        self.assertEqual((result["plan_id"], result["plan_reused"]), (7, True))
        self.assertEqual(self.f.calls, 2)
        self.assertEqual(self.phases(), [("preparation", LATER), ("due", LATER)])
        self.assertIsNone(evidence._PREPARED.get())

    def inject_before_writer(self, phase, callback):
        calls = 0

        @contextmanager
        def transaction(connection):
            nonlocal calls
            calls += 1
            if calls == phase:
                callback()
            with storage.transaction(connection):
                yield

        return patch.object(runtime, "transaction", transaction)

    def test_catalog_or_authority_readset_change_before_preparation_is_rejected(self):
        def changed():
            self.f.connection.execute("UPDATE proof SET value='revoked'")
            self.f.connection.commit()

        with self.inject_before_writer(2, changed), self.assertRaises(evidence.RuntimeEvidenceChanged):
            self.run_tick()
        self.assertEqual(self.f.calls, 1)
        self.assertEqual(self.phases(), [])
        self.assertEqual(self.enqueue.call_count, 0)

    def test_file_change_before_due_has_no_cold_fallback_and_preserves_prior_commit(self):
        with self.inject_before_writer(3, lambda: self.f.code.write_text("changed=True\n")), \
                self.assertRaises(evidence.RuntimeEvidenceChanged):
            self.run_tick()
        self.assertEqual(self.f.calls, 2)
        self.assertEqual(self.phases(), [("preparation", LATER)])

    def test_change_during_due_rolls_back_at_exit(self):
        def changed(connection, plan, **kwargs):
            result = self.plan_due(connection, plan, **kwargs)
            self.f.plist.write_bytes(b"another writer")
            return result

        with patch.object(runtime, "_plan_due", side_effect=changed), self.assertRaises(evidence.RuntimeEvidenceChanged):
            self.run_tick()
        self.assertEqual(self.phases(), [("preparation", LATER)])
        self.assertEqual(self.f.calls, 2)

    def test_due_phase_uses_live_clock_and_defers_changed_plan_key_without_rebuilding(self):
        self.current.side_effect = lambda connection, prepared, *, at: at == AT
        result = self.run_tick()
        self.assertEqual(result["reason"], "catalog_changed_during_enqueue")
        self.assertEqual(self.current.call_args.kwargs["at"], LATER)
        self.assertEqual(self.ensure.call_count, 1)
        self.assertEqual(self.phases(), [("preparation", LATER)])

    def test_activation_changed_during_preflight_does_not_enqueue(self):
        with patch.object(runtime, "activation_at", side_effect=[ACTIVE, None]):
            result = self.run_tick()
        self.assertEqual(result["reason"], "activation_changed_during_preparation")
        self.assertEqual(self.phases(), [])
        self.assertEqual(self.ensure.call_count, 0)


class InstalledCapturePlanningInheritanceBoundaryTest(unittest.TestCase):
    @contextmanager
    def fixture_connections(self, database):
        """Keep formal-DB denial enabled and permit only this owned fixture.

        The installed release deliberately names its disposable database as
        formal. Retain real schema, WAL/read-only behavior and Writer lease
        checks while binding every application connector to its exact inode.
        """
        from v8 import runtime_database

        expected = database.resolve(strict=True)
        identity = (expected.stat().st_dev, expected.stat().st_ino)
        original_connect = storage.connect

        def fixture_connect(path, *, read_only=None):
            selected = Path(path).resolve(strict=True)
            self.assertEqual(selected, expected)
            self.assertFalse(Path(path).is_symlink())
            self.assertEqual((selected.stat().st_dev, selected.stat().st_ino), identity)
            if read_only is None:
                read_only = os.environ.get('DCAR_READ_ONLY', '0') == '1'
            query = 'mode=ro' if read_only else 'mode=rw'
            if read_only and not storage._LIVE_WAL_READ_ONLY.get():
                query += '&immutable=1'
            connection = sqlite3.connect(selected.as_uri()+'?'+query, uri=True,
                factory=storage._ClosingSQLiteConnection)
            connection.row_factory = sqlite3.Row
            try:
                storage.configure_connection_safety(connection)
                storage.require_schema_compatibility(connection, supported_versions=frozenset({23}))
                runtime_database.require_current_process_writer_lock(connection)
                connection.execute('PRAGMA query_only=ON' if read_only else 'PRAGMA journal_mode=WAL')
                return connection
            except Exception:
                connection.close()
                raise

        with ExitStack() as patches:
            for name, module in tuple(sys.modules.items()):
                if name.startswith('v8') and getattr(module, 'connect', None) is original_connect:
                    patches.enter_context(patch.object(module, 'connect', side_effect=fixture_connect))
            temporary_connect = storage.connect
            try:
                yield
            finally:
                # Modules loaded inside the proof may import the temporary
                # storage connector. Do not leak it into later test cases.
                for name, module in tuple(sys.modules.items()):
                    if name.startswith('v8') and getattr(module, 'connect', None) is temporary_connect:
                        module.connect = original_connect

    def test_real_schema23_frozen_parent_keeps_all_cold_verification_outside_planning_writes(self):
        from tests import test_four_platform_flow_release as installed
        from v8 import four_platform_flow_release as flow

        fixture = installed.FourPlatformFlowReleaseTest(methodName="runTest")
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.enterContext(patch("socket.socket.connect", side_effect=AssertionError("network forbidden")))
        self.enterContext(patch.object(evidence, "_loaded_source_root", return_value=fixture.source))
        real_verify = flow.verify_inheritance
        phases = []

        def prove(**kwargs):
            locked = storage._SQLITE_WRITE_TRANSACTION_LOCK._owner == threading.get_ident()
            if locked:
                self.assertIsNotNone(evidence._PREPARED.get())
                self.assertIs(kwargs["connection"], evidence._BOUNDARY.get())
                phases.append("reuse")
            else:
                phases.append("cold")
            return real_verify(**kwargs)

        with fixture.flow_runtime() as installed_plan, self.fixture_connections(fixture.f.db):
            at = fixture.installer.now()
            before = fixture.f.connection.execute("SELECT count(*) FROM provider_usage").fetchone()[0]
            with patch.object(flow, "verify_inheritance", side_effect=prove), \
                    patch.object(evidence, "_now", return_value=at), patch.object(runtime, "now_utc", return_value=at):
                first = runtime.plan_tick(fixture.f.db, at=at)
                second = runtime.plan_tick(fixture.f.db, at=at)
                original_profile = preparation.prepare_profile_reuse

                def changed_catalog(connection, **kwargs):
                    proof = original_profile(connection, **kwargs)
                    with storage.connect(fixture.f.db) as writer, storage.transaction(writer):
                        writer.execute("UPDATE capture_catalog_revision SET revision=revision+1 WHERE id=1")
                    return proof

                before_plans = fixture.f.connection.execute("SELECT count(*) FROM capture_source_plans").fetchone()[0]
                with patch.object(preparation, "prepare_profile_reuse", side_effect=changed_catalog):
                    changed = runtime.plan_tick(fixture.f.db, at=at)
                self.assertEqual(changed["reason"], "preparation_reuse_proof_changed", changed)
                self.assertEqual(fixture.f.connection.execute("SELECT count(*) FROM capture_source_plans").fetchone()[0], before_plans)

                build = flow.payload_at(installed_plan["child_build"], "sealed-build-receipt-v1")
                source_file = fixture.source / "src/dcar_eval/v8/capture_runtime.py"
                authority_file = Path(build["four_platform_flow_successor"]["operation_authorization"]["path"])
                for target in (source_file, authority_file):
                    with self.subTest(changed_file=target.name):
                        original_bytes = target.read_bytes()
                        committed = []
                        original_transaction = runtime.transaction

                        @contextmanager
                        def changed_file(connection):
                            # Reconciliation and preparation have committed.
                            # Invalidate the second phase's already-built proof.
                            if len(committed) == 2:
                                target.write_bytes(original_bytes + b"\n ")
                            with original_transaction(connection):
                                yield
                            committed.append(True)

                        try:
                            with patch.object(runtime, "transaction", changed_file), \
                                    self.assertRaises(evidence.RuntimeEvidenceChanged):
                                runtime.plan_tick(fixture.f.db, at=at)
                            self.assertEqual(len(committed), 2, "due rejection must preserve the earlier preparation commit")
                        finally:
                            target.write_bytes(original_bytes)
            self.assertEqual(first["status"], "planned", first)
            self.assertEqual(second["status"], "planned", second)
            self.assertTrue(second["plan_reused"], second)
            self.assertEqual(first["plan_id"], second["plan_id"])
            self.assertGreaterEqual(phases.count("reuse"), 2)
            self.assertEqual(fixture.f.connection.execute("SELECT count(*) FROM provider_usage").fetchone()[0], before)


if __name__ == "__main__":
    unittest.main()
