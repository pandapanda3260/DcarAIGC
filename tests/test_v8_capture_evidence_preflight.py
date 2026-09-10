from __future__ import annotations

import os
import hashlib
import importlib.util
import sqlite3
import subprocess
import tempfile
import threading
import time
import sys
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

from v8 import capture_evidence_preflight as preflight, capture_release as release
from v8.profile_activations import activation_at as real_activation_at
from v8.storage import transaction

NOW = "2026-09-07T04:00:00Z"
LATER = "2026-09-07T04:00:10Z"


class CaptureEvidencePreflightTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.project = root / "source"
        self.project.mkdir()
        self.code = self.project / "code.py"
        self.code.write_text("value = 1\n")
        self.git("init", "-b", "main")
        self.git("add", "code.py")
        self.git("-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-m", "fixture")
        self.database = root / "live.sqlite3"
        self.receipt = root / "receipt.json"
        self.receipt.write_text('{"passed":true}')
        self.receipt.chmod(0o600)
        with self.connection() as connection:
            connection.executescript("""
                PRAGMA journal_mode=WAL;
                PRAGMA user_version=20;
                CREATE TABLE proof(id INTEGER PRIMARY KEY,kind TEXT,value TEXT,effective_at TEXT);
                INSERT INTO proof VALUES(1,'deployment','accepted','2026-09-07T00:00:00Z');
                INSERT INTO proof VALUES(2,'roster','roster-a','2026-09-07T00:00:00Z');
                INSERT INTO proof VALUES(3,'control','open','2026-09-07T00:00:00Z');
                CREATE TABLE owner(id INTEGER PRIMARY KEY,expires REAL,ticks INTEGER);
                INSERT INTO owner VALUES(1,0,0);
                CREATE TABLE budget(remaining INTEGER);
                INSERT INTO budget VALUES(5);
            """)
        self.calls = 0
        self.delay = 0.0
        self.connections = []
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.dict(os.environ, {"DCAR_LOADED_BUILD_ID": "sha256:" + "a" * 64}))
        self.stack.enter_context(patch.object(preflight, "PROJECT_ROOT", self.project))
        self.stack.enter_context(patch.object(preflight, "now_utc", return_value=NOW))
        self.stack.enter_context(patch.object(release, "_installed_evidence_uncached", side_effect=self.proof))
        self.stack.enter_context(patch("v8.runtime_database.require_current_process_writer_lock", return_value={}))
        self.stack.enter_context(patch("v8.profile_activations.activation_at", return_value={"id": 1}))
        self.stack.enter_context(patch.object(release.forward_recovery, "_route", return_value={"route": "current"}))
        self.stack.enter_context(patch.object(release, "_release_tools"))
        self.storage = self.stack.enter_context(patch("v8.provider_budget.require_storage_ready"))

    def git(self, *args):
        return subprocess.run(["git", "-C", str(self.project), *args], check=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout

    def connection(self):
        connection = sqlite3.connect(self.database, timeout=5)
        connection.row_factory = sqlite3.Row
        self.addCleanup(connection.close)
        return connection

    def proof(self, connection, *, at):
        self.calls += 1
        self.connections.append(connection.connection)
        self.assertEqual(connection.connection.execute("PRAGMA query_only").fetchone()[0], 1)
        self.receipt.read_bytes()
        for _ in range(4):
            connection.execute("SELECT * FROM proof WHERE kind='deployment' ORDER BY id DESC LIMIT 1").fetchone()
            connection.execute("SELECT * FROM proof WHERE kind='roster' ORDER BY id DESC LIMIT 1").fetchone()
            connection.execute("SELECT * FROM proof WHERE kind='control' AND effective_at<=? ORDER BY id DESC LIMIT 1", (at,)).fetchone()
        time.sleep(self.delay)
        return {"active": {"id": 1}, "storage_policy": {}, "deployment": {"status": "accepted"},
                "manifest": {"route": "current"}, "immutable": {"passed": True}}

    def prepared(self):
        return preflight.prepare_installed_evidence(self.database)

    def validate(self, *, at=NOW):
        connection = self.connection()
        with transaction(connection), preflight.evidence_boundary(connection):
            return release._installed_evidence(connection, at=at)

    def test_one_proof_per_boundary_closes_reader_and_deduplicates_selectors(self):
        with self.prepared():
            self.assertEqual(len(preflight._PREPARED.get().queries), 3)
            with self.assertRaises(sqlite3.ProgrammingError):
                self.connections[-1].execute("SELECT 1")
            with transaction(connection := self.connection()), preflight.evidence_boundary(connection):
                first = release._installed_evidence(connection, at=NOW)
                first["immutable"]["passed"] = False
                self.assertTrue(release._installed_evidence(connection, at=NOW)["immutable"]["passed"])
            self.assertEqual(self.calls, 1)
        self.assertIsNone(preflight._PREPARED.get())
        with self.prepared():
            self.validate()
        self.assertEqual(self.calls, 2)

    def test_latest_deployment_roster_and_hold_changes_fail_closed(self):
        for kind, value in (("deployment", "new"), ("roster", "roster-b"), ("control", "hold")):
            with self.subTest(kind=kind), self.prepared():
                with self.connection() as connection:
                    connection.execute("INSERT INTO proof(kind,value,effective_at) VALUES(?,?,?)", (kind, value, NOW))
                with self.assertRaises(preflight.EvidencePreflightChanged):
                    self.validate()

    def test_future_effective_record_is_reselected_at_fresh_time_without_local_writes(self):
        with self.connection() as connection:
            connection.execute("INSERT INTO proof(kind,value,effective_at) VALUES('control','hold',?)", (LATER,))
        with self.prepared(), transaction(connection := self.connection()), preflight.evidence_boundary(connection):
            release._installed_evidence(connection, at=NOW)
            with self.assertRaises(preflight.EvidencePreflightChanged):
                release._installed_evidence(connection, at=LATER)

    def test_normalized_timestamp_parameters_are_refreshed_and_clock_rollback_denied(self):
        self.assertEqual(preflight._fresh_argument("2026-09-07T04:00:00.000000+00:00", NOW, LATER), LATER)
        with self.prepared(), self.assertRaises(preflight.EvidencePreflightChanged):
            self.validate(at="2026-09-07T03:59:59Z")

    def test_same_size_rewrite_with_restored_mtime_is_rejected(self):
        with self.prepared():
            original = self.receipt.stat()
            self.receipt.write_bytes(b"x" * original.st_size)
            os.utime(self.receipt, ns=(original.st_atime_ns, original.st_mtime_ns))
            with self.assertRaises(preflight.EvidencePreflightChanged):
                self.validate()

    def test_delete_replace_symlink_permission_and_hardlink_changes_are_rejected(self):
        original = self.receipt.read_bytes()
        for mutation in ("replace", "symlink", "mode", "hardlink", "missing"):
            with self.subTest(mutation=mutation), self.prepared():
                if mutation == "mode":
                    self.receipt.chmod(0o644)
                elif mutation == "hardlink":
                    os.link(self.receipt, self.receipt.with_suffix(".link"))
                else:
                    self.receipt.unlink()
                    if mutation == "replace":
                        self.receipt.write_bytes(original)
                    elif mutation == "symlink":
                        target = self.receipt.with_suffix(".target")
                        target.write_bytes(original)
                        self.receipt.symlink_to(target)
                with self.assertRaises(preflight.EvidencePreflightChanged):
                    self.validate()
            self.receipt.unlink(missing_ok=True)
            self.receipt.with_suffix(".link").unlink(missing_ok=True)
            self.receipt.write_bytes(original)
            self.receipt.chmod(0o600)

    def test_untracked_addition_staged_change_deleted_source_and_head_change_rejected(self):
        for mutation in ("untracked", "staged", "deleted", "head"):
            with self.subTest(mutation=mutation), self.prepared():
                if mutation == "untracked":
                    (self.project / "new.py").write_text("new=1")
                elif mutation == "staged":
                    self.code.write_text("value = 2\n")
                    self.git("add", "code.py")
                elif mutation == "deleted":
                    self.code.unlink()
                else:
                    self.git("-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                             "commit", "--allow-empty", "-m", "new head")
                with self.assertRaises(preflight.EvidencePreflightChanged):
                    self.validate()
            self.git("reset", "--hard", "HEAD")
            (self.project / "new.py").unlink(missing_ok=True)

    def test_live_budget_and_storage_gate_do_not_use_prepared_verdict(self):
        with self.prepared():
            with self.connection() as connection:
                connection.execute("UPDATE budget SET remaining=0")
            def storage(connection):
                if connection.execute("SELECT remaining FROM budget").fetchone()[0] == 0:
                    raise RuntimeError("live budget exhausted")
            self.storage.side_effect = storage
            with self.assertRaisesRegex(RuntimeError, "live budget exhausted"):
                self.validate()

    def test_source_or_evidence_changes_during_preparation_fail_without_cold_fallback(self):
        original = self.proof
        def changing(connection, *, at):
            result = original(connection, at=at)
            self.receipt.write_text("changed")
            return result
        with patch.object(release, "_installed_evidence_uncached", side_effect=changing):
            with self.assertRaises(preflight.EvidencePreflightChanged), self.prepared():
                self.fail("stale preparation reached the transaction")
        self.assertEqual(self.calls, 1)
        self.assertIsNone(preflight._PREPARED.get())

    def test_explicit_observation_covers_warm_content_hash_cache(self):
        name = "_preflight_release_contract_test"
        path = Path(__file__).resolve().parents[1] / "scripts/v20_release_contract.py"
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        reference = {"path": str(self.receipt.resolve()), "sha256": hashlib.sha256(self.receipt.read_bytes()).hexdigest()}
        module.verified_reference(reference)
        files = {}
        token = preflight._READ_FILES.set(files)
        try:
            # A warm digest performs no file open, so the explicit reader hook
            # must still register this dependency.
            module.verified_reference(reference)
        finally:
            preflight._READ_FILES.reset(token)
        self.assertEqual(files[self.receipt.resolve()], preflight._version(self.receipt.resolve()))
        self.assertEqual(module._evidence_file_digest.cache_info().hits, 1)

    def test_cold_import_cache_creation_is_not_misclassified_as_evidence_tampering(self):
        path = Path(self.temp.name) / "cold_module.py"
        path.write_text("answer = 42\n")
        cache = Path(importlib.util.cache_from_source(str(path)))
        self.assertFalse(cache.exists())
        original = self.proof
        def importing(connection, *, at):
            result = original(connection, at=at)
            spec = importlib.util.spec_from_file_location("_preflight_cold_fixture", path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            self.assertEqual(module.answer, 42)
            return result
        with patch.object(sys, "dont_write_bytecode", False), patch.object(
            release, "_installed_evidence_uncached", side_effect=importing,
        ), self.prepared():
            self.assertTrue(cache.exists())
            self.assertIn(path, preflight._PREPARED.get().files)
            self.assertNotIn(cache, preflight._PREPARED.get().files)
            self.validate()

    def test_nested_live_proof_clock_is_anchored_only_during_preparation(self):
        original = self.proof
        def nested(connection, *, at):
            self.assertEqual(preflight.evidence_time(LATER), NOW)
            return original(connection, at=at)
        with patch.object(release, "_installed_evidence_uncached", side_effect=nested), self.prepared():
            self.assertEqual(preflight.evidence_time(LATER), LATER)
            self.validate()

    def test_stale_existing_import_cache_may_be_recompiled_from_unchanged_source(self):
        path = Path(self.temp.name) / "stale_module.py"
        path.write_text("answer = 1\n")
        def load():
            spec = importlib.util.spec_from_file_location("_preflight_stale_fixture", path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module.answer
        with patch.object(sys, "dont_write_bytecode", False):
            self.assertEqual(load(), 1)
            cache = Path(importlib.util.cache_from_source(str(path)))
            self.assertTrue(cache.exists())
            path.write_text("answer = 200\n")
            original = self.proof
            def importing(connection, *, at):
                result = original(connection, at=at)
                self.assertEqual(load(), 200)
                return result
            with patch.object(release, "_installed_evidence_uncached", side_effect=importing), self.prepared():
                self.assertNotIn(cache, preflight._PREPARED.get().files)
                self.assertIn(path, preflight._PREPARED.get().files)
                self.validate()

    def test_valid_import_bytecode_remains_a_dependency_despite_other_source_reads(self):
        path = Path(self.temp.name) / "valid_module.py"
        path.write_text("answer = 1\n")
        def load():
            spec = importlib.util.spec_from_file_location("_preflight_valid_fixture", path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        with patch.object(sys, "dont_write_bytecode", False):
            load()
            cache = Path(importlib.util.cache_from_source(str(path)))
            original = self.proof
            def importing(connection, *, at):
                result = original(connection, at=at)
                load()
                path.read_bytes()  # A validator read is not a loader fallback.
                return result
            with patch.object(release, "_installed_evidence_uncached", side_effect=importing), self.prepared():
                self.assertIn(cache, preflight._PREPARED.get().files)
                cache.write_bytes(cache.read_bytes() + b"tampered")
                with self.assertRaises(preflight.EvidencePreflightChanged):
                    self.validate()

    def test_actual_claim_and_send_boundaries_reprepare_and_enforce_changed_budget(self):
        # Real A/B and accounting against the existing isolated budget fixture.
        # Only installed OS evidence is substituted; no provider is invoked.
        from tests.test_v8_provider_budget import ProviderBudgetTest, AT
        from v8 import capture, storage
        from v8.provider_budget import BudgetBlocked
        fixture = ProviderBudgetTest(methodName="runTest")
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        fixture.roster()
        budget = fixture.budget()
        calls = []
        def installed(connection, *, at):
            calls.append(at)
            self.assertEqual(connection.connection.execute("PRAGMA query_only").fetchone()[0], 1)
            connection.execute("PRAGMA user_version").fetchone()
            self.receipt.read_bytes()
            return {"active": {"id": 1}, "storage_policy": {}, "deployment": {"status": "accepted"},
                    "manifest": {"route": "current"}}
        with patch.object(release, "_installed_evidence_uncached", side_effect=installed), patch.object(
            preflight, "now_utc", return_value=AT,
        ), patch.object(capture, "now_utc", return_value=AT), patch("v8.profile_activations.activation_at", real_activation_at):
            claim = fixture.claim_only(budget)
            with storage.connect(fixture.db) as connection, transaction(connection):
                connection.execute("UPDATE provider_budget_batches SET status='completed' WHERE id=?", (budget,))
            with self.assertRaises(BudgetBlocked):
                capture._mark_paid_sent(claim, operation="douyin_video_detail", budget_id=budget, db_path=fixture.db)
        self.assertEqual(len(calls), 2)
        self.assertEqual(fixture.calls, 0)
        with storage.connect(fixture.db) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM fetch_attempts").fetchone()[0], 0)
            self.assertEqual(connection.execute("SELECT sum(request_attempts) FROM provider_usage").fetchone()[0], 0)

    def test_operation_command_reuses_preflight_and_takes_fresh_boundary_clock(self):
        from v8 import capture_release_commands as commands
        def publish(connection, *, at, operation):
            self.assertEqual(at, LATER)
            self.assertEqual(operation, "douyin_video_detail")
            return release._installed_evidence(connection, at=at)
        with patch.object(commands, "_current_command"), patch.object(commands, "now_utc", return_value=LATER), patch.object(
            release, "publish_operation_gate", side_effect=publish,
        ):
            result = commands.run_command(db_path=self.database, mirror_root=None, command_claim={},
                action="operation_publish", operation="douyin_video_detail")
        self.assertEqual(self.calls, 1)
        self.assertTrue(result["immutable"]["passed"])

    def test_automatic_qualification_maintenance_uses_same_preflight_boundary(self):
        from v8 import pipeline
        def maintain(connection, *, at, mirror_root):
            self.assertEqual(at, LATER)
            return release._installed_evidence(connection, at=at)
        with patch("v8.account_roster_capture.activate_prepared_roster_capture_in_transaction"), patch(
            "v8.capture_quality.maintenance_tick", return_value={"status": "done"},
        ), patch.object(release, "maintain_operation_qualifications", side_effect=maintain), patch.object(
            pipeline, "now_utc", return_value=LATER,
        ):
            result = pipeline._capture_v25_job(kind="maintenance", db_path=self.database)
        self.assertEqual(self.calls, 1)
        self.assertTrue(result["qualification_maintenance"]["immutable"]["passed"])

    def test_readonly_preparation_rejects_write_and_cursor_escape(self):
        for sql in ("INSERT INTO proof(kind) VALUES('invalid')", "PRAGMA query_only=OFF", "PRAGMA user_version=0"):
            with self.subTest(sql=sql), patch.object(release, "_installed_evidence_uncached",
                    side_effect=lambda connection, *, at: connection.execute(sql)):
                with self.assertRaises(preflight.EvidencePreflightChanged), self.prepared():
                    self.fail("write preparation yielded")
        reader = preflight._ReadInputs(self.connection())
        with self.assertRaises(preflight.EvidencePreflightChanged):
            reader.cursor()

    def test_writer_owner_loss_database_inode_and_environment_changes_rejected(self):
        with self.prepared():
            with patch("v8.runtime_database.require_current_process_writer_lock", side_effect=RuntimeError("owner lost")):
                with self.assertRaisesRegex(RuntimeError, "owner lost"):
                    self.validate()
            with patch.dict(os.environ, {"DCAR_LOADED_BUILD_ID": "sha256:" + "b" * 64}):
                with self.assertRaises(preflight.EvidencePreflightChanged):
                    self.validate()
            prepared = preflight._PREPARED.get()
            prepared.database_inode = (0, 0)
            with self.assertRaises(preflight.EvidencePreflightChanged):
                self.validate()

    def test_seconds_scale_proof_allows_heartbeat_and_old_lock_placement_expires(self):
        # Real elapsed seconds and actual WAL writer contention. TTL is shortened
        # only in this fixture: the production 180-second lease is unchanged.
        outcomes = []
        for under_lock in (False, True):
            with self.connection() as connection:
                connection.execute("UPDATE owner SET expires=?,ticks=0", (time.monotonic() + 0.5,))
            stop, started = threading.Event(), threading.Event()
            errors = []
            def heartbeat():
                connection = sqlite3.connect(self.database, timeout=5)
                try:
                    started.set()
                    while not stop.wait(0.08):
                        with transaction(connection, priority="heartbeat"):
                            now = time.monotonic()
                            expires = connection.execute("SELECT expires FROM owner").fetchone()[0]
                            if expires <= now:
                                errors.append("expired")
                                return
                            connection.execute("UPDATE owner SET expires=?,ticks=ticks+1", (now + 0.5,))
                finally:
                    connection.close()
            thread = threading.Thread(target=heartbeat)
            thread.start()
            self.assertTrue(started.wait(2))
            try:
                if under_lock:
                    with transaction(self.connection()):
                        time.sleep(1.2)
                else:
                    self.delay = 1.2
                    with self.prepared():
                        self.validate()
                time.sleep(0.12)
            finally:
                stop.set()
                thread.join(3)
            self.assertFalse(thread.is_alive())
            ticks = self.connection().execute("SELECT ticks FROM owner").fetchone()[0]
            outcomes.append((errors, ticks))
        self.assertEqual(outcomes[0][0], [])
        self.assertGreaterEqual(outcomes[0][1], 8)
        self.assertEqual(outcomes[1][0], ["expired"])


if __name__ == "__main__":
    unittest.main()
