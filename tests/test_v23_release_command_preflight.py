"""Release controls preserve live authority and atomic generation fences."""
from contextlib import contextmanager
import os
import sqlite3
import threading
import unittest
from unittest.mock import patch

from v8 import capture_release_commands as commands, pipeline, storage
from v8 import runtime_evidence_context as evidence
from tests import test_v23_runtime_evidence_context as fixtures


AT = "2026-09-13T03:20:00Z"
LATER = "2026-09-13T03:20:10Z"


class ReleaseCommandPreflightTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.RuntimeEvidenceContextTest(methodName="runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.connection.execute("CREATE TABLE issued_gate(operation,at)")
        self.fixture.connection.commit()
        original_proof = self.fixture.verifier.side_effect

        def prove(**kwargs):
            self.assertNotEqual(storage._SQLITE_WRITE_TRANSACTION_LOCK._owner, threading.get_ident())
            return original_proof(**kwargs)

        self.fixture.verifier.side_effect = prove
        self.enterContext(patch.object(evidence, "_now", return_value=AT))
        # This fixture isolates authority fences from schema installation;
        # production connect/schema validation is covered by release checks.
        self.enterContext(patch.object(commands, "connect", self.connection))
        self.enterContext(patch.object(pipeline, "connect", self.connection))
        self.check_phases = []
        self.enterContext(patch.object(commands, "_current_command", side_effect=self.check_command))

    def check_command(self, connection, claim, parameters):
        self.check_phases.append(connection.in_transaction)

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

    def reuse(self, connection, at):
        from pathlib import Path

        f = self.fixture
        return evidence.reuse_inheritance(connection=connection, build=f.build,
            build_ref=f.build_ref, install_path=Path(f.install_ref["path"]),
            database=f.db, source=f.source, at=at)

    def publish(self, connection, *, at, operation, **_):
        self.assertTrue(connection.in_transaction)
        self.assertEqual(self.reuse(connection, at), {"immutable": "original"})
        self.assertEqual(self.reuse(connection, at), {"immutable": "original"})
        connection.execute("INSERT INTO issued_gate VALUES(?,?)", (operation, at))
        return {"operation": operation, "at": at, "provider_calls": 0}

    def run_command(self, *, action="operation_publish", publish=None):
        method = "publish_operation_gate" if action == "operation_publish" else "renew_operation_gate"
        with patch.object(commands.release, method, side_effect=publish or self.publish), \
                patch.object(commands, "now_utc", side_effect=[AT, LATER]):
            return commands.run_command(db_path=self.fixture.db, mirror_root=None,
                command_claim={}, action=action, operation="douyin_video_detail")

    def issued(self):
        return self.fixture.connection.execute("SELECT * FROM issued_gate").fetchall()

    def test_publish_proves_once_without_writer_lock_and_rechecks_live_command(self):
        result = self.run_command()
        self.assertEqual(result["at"], LATER)
        self.assertEqual(self.check_phases, [False, True])
        self.assertEqual(self.fixture.calls, 1)
        self.assertEqual(len(self.issued()), 1)
        self.assertIsNone(evidence._PREPARED.get())

    def test_renewal_gets_its_own_proof_instead_of_cross_command_cache(self):
        self.run_command()
        self.run_command(action="operation_renew")
        self.assertEqual(self.fixture.calls, 2)
        self.assertEqual(len(self.issued()), 2)

    def test_command_changed_during_preparation_cannot_publish(self):
        def check(connection, claim, parameters):
            if connection.in_transaction:
                raise ValueError("command no longer owns its durable claim")

        with patch.object(commands, "_current_command", side_effect=check), \
                self.assertRaisesRegex(ValueError, "durable claim"):
            self.run_command()
        self.assertEqual(self.fixture.calls, 1)
        self.assertEqual(self.issued(), [])

    def test_file_generation_changed_before_boundary_never_falls_back_cold(self):
        @contextmanager
        def changed(connection):
            with storage.transaction(connection):
                old = self.fixture.code.stat()
                self.fixture.code.write_text("evil = True\n")
                os.utime(self.fixture.code, ns=(old.st_atime_ns, old.st_mtime_ns))
                yield

        with patch.object(commands, "transaction", changed), \
                self.assertRaises(evidence.RuntimeEvidenceChanged):
            self.run_command()
        self.assertEqual(self.fixture.calls, 1)
        self.assertEqual(self.issued(), [])

    def test_file_generation_changed_after_gate_write_rolls_back(self):
        def changed(connection, **kwargs):
            value = self.publish(connection, **kwargs)
            self.fixture.plist.write_bytes(b"another installed writer")
            return value

        with self.assertRaises(evidence.RuntimeEvidenceChanged):
            self.run_command(publish=changed)
        self.assertEqual(self.fixture.calls, 1)
        self.assertEqual(self.issued(), [])

    def test_changed_database_proof_rolls_back_without_reverification(self):
        def changed(connection, **kwargs):
            value = self.publish(connection, **kwargs)
            connection.execute("UPDATE proof SET value='changed'")
            return value

        with self.assertRaises(evidence.RuntimeEvidenceChanged):
            self.run_command(publish=changed)
        self.assertEqual(self.fixture.calls, 1)
        self.assertEqual(self.issued(), [])
        self.assertEqual(self.fixture.connection.execute("SELECT value FROM proof").fetchone()[0], "original")

    def test_unrelated_control_preserves_existing_path_without_inheritance_preparation(self):
        with patch("v8.account_profile_recovery.enqueue_profile_compensation", return_value={"provider_calls": 0}), \
                patch.object(commands, "now_utc", return_value=AT):
            result = commands.run_command(db_path=self.fixture.db, mirror_root=None,
                command_claim={}, action="profile_compensate", work_id=17)
        self.assertEqual(result["provider_calls"], 0)
        self.assertEqual(self.fixture.calls, 0)
        self.assertEqual(self.check_phases, [False, True])

    def test_schema22_keeps_existing_boundary_clock_and_no_schema23_proof(self):
        self.fixture.connection.execute("PRAGMA user_version=22")
        self.fixture.connection.commit()

        def legacy(connection, *, at, operation):
            self.assertIsNone(evidence._PREPARED.get())
            self.assertEqual(at, AT)
            return {"provider_calls": 0}

        self.run_command(publish=legacy)
        self.assertEqual(self.fixture.calls, 0)

    def maintenance(self, callback):
        with patch("v8.account_roster_capture.activate_prepared_roster_capture_in_transaction"), \
                patch("v8.capture.recover_stale_fetch_slots", return_value={}), \
                patch("v8.capture_quality.maintenance_tick", return_value={"status": "measured"}), \
                patch.object(commands.release, "maintain_operation_qualifications", side_effect=callback), \
                patch.object(pipeline, "now_utc", return_value=LATER):
            return pipeline._capture_v25_job(kind="maintenance", db_path=self.fixture.db, at=AT)

    def test_automatic_maintenance_uses_same_atomic_fence_and_fresh_clock(self):
        result = self.maintenance(lambda connection, **kwargs: self.publish(
            connection, operation="existing_authorized_gate", **kwargs))
        self.assertEqual(result["qualification_maintenance"]["at"], LATER)
        self.assertEqual(self.fixture.calls, 1)
        self.assertEqual(len(self.issued()), 1)

    def test_maintenance_generation_change_rolls_back_gate_writes(self):
        def changed(connection, **kwargs):
            result = self.publish(connection, operation="existing_authorized_gate", **kwargs)
            self.fixture.plist.write_bytes(b"changed")
            return result

        with self.assertRaises(evidence.RuntimeEvidenceChanged):
            self.maintenance(changed)
        self.assertEqual(self.issued(), [])


class InstalledReleaseControlPreflightTest(unittest.TestCase):
    def test_real_schema23_publish_and_due_maintenance_commit_with_full_proof(self):
        from datetime import timedelta
        from tests import test_four_platform_flow_release as installed_fixtures
        from v8 import four_platform_flow_release as flow
        from v8.source_routing import parse_time

        fixture = installed_fixtures.FourPlatformFlowReleaseTest(methodName="runTest")
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.enterContext(patch("socket.socket.connect", side_effect=AssertionError("network forbidden")))
        self.enterContext(patch.object(evidence, "_loaded_source_root", return_value=fixture.source))
        with fixture.flow_runtime():
            connection = fixture.f.connection
            operation = "wechat_channels_video_comments"
            before = connection.execute("SELECT count(*) FROM provider_usage").fetchone()[0]
            at = fixture.installer.now()
            published = fixture.comments_release_command("operation_publish", at)
            self.assertEqual(published["status"], "succeeded", published)
            due = (parse_time(at) + timedelta(hours=19)).isoformat()
            real_verify = flow.verify_inheritance
            proofs = []

            def prove(**kwargs):
                if type(kwargs["connection"]).__name__ == "_ReadSet":
                    self.assertNotEqual(storage._SQLITE_WRITE_TRANSACTION_LOCK._owner, threading.get_ident())
                    proofs.append("_ReadSet")
                else:
                    self.assertIsNotNone(evidence._PREPARED.get())
                return real_verify(**kwargs)

            with patch.object(flow, "verify_inheritance", side_effect=prove), \
                    patch.object(evidence, "_now", return_value=due), \
                    patch.object(pipeline, "now_utc", return_value=due), \
                    patch("v8.account_roster_capture.activate_prepared_roster_capture_in_transaction"), \
                    patch("v8.capture.recover_stale_fetch_slots", return_value={}), \
                    patch("v8.capture_quality.maintenance_tick", return_value={"status": "measured"}):
                result = pipeline._capture_v25_job(kind="maintenance", db_path=fixture.f.db, at=due)
            renewed = result["qualification_maintenance"]["operations"][operation]
            self.assertEqual(renewed["status"], "renewed", renewed)
            self.assertEqual(proofs, ["_ReadSet"])
            self.assertEqual(connection.execute(
                "SELECT count(*) FROM capture_paid_send_gate_events WHERE operation=?", (operation,)
            ).fetchone()[0], 2)
            self.assertEqual(connection.execute("SELECT count(*) FROM provider_usage").fetchone()[0], before)


if __name__ == "__main__":
    unittest.main()
