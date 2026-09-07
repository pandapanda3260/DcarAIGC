"""CLI lease integration; temporary real plist, SQLite file and flock only."""
from __future__ import annotations

import io
import json
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing, redirect_stdout
from pathlib import Path
from unittest.mock import patch

import issue_v20_code_successor as issuer
import seal_r0_receipts as sealer
from tests.test_v8_runtime_database import InstalledRuntimeFixture
from v8 import capture_code_successor as successor, runtime_database as runtime


class CodeSuccessorCliLeaseTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.fixture = InstalledRuntimeFixture(Path(self.temp.name).resolve())
        self.fixture.database.write_bytes(b"")
        with closing(sqlite3.connect(self.fixture.database)) as connection:
            connection.execute("CREATE TABLE cli_probe (value TEXT)")
        self.addCleanup(patch.stopall)
        patch.object(runtime, "_current_home", return_value=self.fixture.home).start()
        self.original_path = list(sys.path)
        self.addCleanup(self._restore_path)

    def _restore_path(self) -> None:
        sys.path[:] = self.original_path

    def _argv(self, action: str) -> list[str]:
        args = ["issue_v20_code_successor.py", action, "--project-root", str(self.fixture.project)]
        if action == "prepare":
            args.extend(["--previous-build", str(self.fixture.project / "previous.json"),
                "--evidence-dir", str(self.fixture.database.parent / "evidence"),
                "--actor", "isolated-test", "--reason", "CLI lease integration"])
            for name in sorted(sealer.REQUIRED_TEST_RESULTS | {"code_successor"}):
                args.extend(["--test-result", name + "=" + str(self.fixture.project / (name + ".log"))])
        else:
            args.extend(["--build", str(self.fixture.project / "sealed.json")])
        return args

    def _assert_released(self) -> None:
        with closing(sqlite3.connect(self.fixture.database)) as connection:
            with self.assertRaisesRegex(runtime.RuntimeDatabaseError, "does not own"):
                runtime.require_current_process_writer_lock(connection)
        installed = runtime.load_installed_writer_contract(required=True)
        access = runtime.resolve_installed_database_access(runtime.DatabaseAccessMode.WRITER,
            database=self.fixture.database, project_root=self.fixture.project,
            environ=self.fixture.environment, installed=installed)
        self.assertFalse(runtime.observe_writer_lock(access)["held"])
        sealer._require_no_database_holders(self.fixture.database)

    def test_prepare_and_postseal_enter_real_writer_registry_and_close(self) -> None:
        held_connections: list[sqlite3.Connection] = []

        def producer(connection: sqlite3.Connection, **kwargs: object) -> dict[str, str]:
            proof = runtime.require_current_process_writer_lock(connection)
            self.assertEqual(proof["database_path"], str(self.fixture.database))
            self.assertTrue(connection.in_transaction)
            self.assertEqual(connection.execute("PRAGMA foreign_keys").fetchone()[0], 1)
            held_connections.append(connection)
            connection.execute("INSERT INTO cli_probe VALUES ('committed')")
            return {"boundary": "real-writer-lease-only"}

        for action, name in (("prepare", "prepare_plan"), ("postseal", "issue_decision")):
            with self.subTest(action=action), patch.object(successor, name, side_effect=producer), \
                    patch.object(sys, "argv", self._argv(action)), redirect_stdout(io.StringIO()) as output:
                issuer.main()
                self.assertEqual(json.loads(output.getvalue()), {"boundary": "real-writer-lease-only"})
            with self.assertRaisesRegex(sqlite3.ProgrammingError, "closed"):
                held_connections[-1].execute("SELECT 1")
            self._assert_released()
        with closing(sqlite3.connect(self.fixture.database)) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM cli_probe").fetchone()[0], 2)

    def test_actual_prepare_guard_passes_and_failure_rolls_back(self) -> None:
        # Do not replace the producer or its Writer guard. Stop only at its
        # first receipt read, which follows the real require_current_process.
        with patch.object(successor, "_ref", side_effect=ValueError("receipt-boundary")) as receipt, \
                patch.object(sys, "argv", self._argv("prepare")):
            with self.assertRaisesRegex(ValueError, "receipt-boundary"):
                issuer.main()
            receipt.assert_called_once()
        self._assert_released()

    def test_existing_database_holder_blocks_before_issuing(self) -> None:
        with closing(sqlite3.connect(self.fixture.database)) as holder:
            holder.execute("SELECT * FROM cli_probe").fetchall()
            with patch.object(successor, "prepare_plan") as produce, \
                    patch.object(sys, "argv", self._argv("prepare")):
                with self.assertRaisesRegex(sealer.R0ReceiptError, "open holder"):
                    issuer.main()
                produce.assert_not_called()
        self._assert_released()


if __name__ == "__main__":
    unittest.main()
