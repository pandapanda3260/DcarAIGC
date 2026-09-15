from contextlib import contextmanager
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from v8 import api, storage


class ProofWorkerLifecycleTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / "fixture.sqlite3"
        with sqlite3.connect(self.path) as connection:
            connection.execute("PRAGMA user_version=24")
        self.config = SimpleNamespace(db_path=self.path, read_only=False)

    @contextmanager
    def reader(self, path, *, read_only=False):
        self.assertEqual(path, self.path)
        self.assertTrue(read_only)
        connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
        try:
            yield connection
        finally:
            connection.close()

    def test_installed_schema24_children_close_even_if_runtime_startup_fails(self):
        with patch.dict("os.environ", {"DCAR_LOADED_BUILD_ID": "sha256:fixture"}), \
                patch.object(api, "connect", side_effect=self.reader), \
                patch("v8.runtime_proof_workers.start") as start, \
                patch("v8.runtime_proof_workers.stop") as stop:
            with self.assertRaisesRegex(RuntimeError, "startup failed"):
                with api._runtime_proof_worker_pool(self.config, enabled=True):
                    start.assert_called_once_with(self.path)
                    stop.assert_not_called()
                    raise RuntimeError("startup failed")
            stop.assert_called_once_with()

    def test_readonly_and_uninstalled_processes_never_start_children(self):
        for enabled, read_only, loaded in ((False, False, "sha256:fixture"),
                                          (True, True, "sha256:fixture"),
                                          (True, False, "")):
            with self.subTest(enabled=enabled, read_only=read_only, loaded=loaded), \
                    patch.dict("os.environ", {"DCAR_LOADED_BUILD_ID": loaded}), \
                    patch.object(api, "connect", side_effect=AssertionError("unexpected DB open")), \
                    patch("v8.runtime_proof_workers.start") as start:
                config = SimpleNamespace(db_path=self.path, read_only=read_only)
                with api._runtime_proof_worker_pool(config, enabled=enabled):
                    pass
                start.assert_not_called()

    def test_previous_schema_keeps_its_existing_runtime(self):
        with sqlite3.connect(self.path) as connection:
            connection.execute("PRAGMA user_version=23")
        with patch.dict("os.environ", {"DCAR_LOADED_BUILD_ID": "sha256:fixture"}), \
                patch.object(api, "connect", side_effect=self.reader), \
                patch("v8.runtime_proof_workers.start") as start:
            with api._runtime_proof_worker_pool(self.config, enabled=True):
                pass
            start.assert_not_called()

    def test_schema24_committed_only_in_live_wal_still_starts_workers(self):
        writer = sqlite3.connect(self.path)
        self.addCleanup(writer.close)
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("PRAGMA user_version=23")
        writer.commit()
        writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        writer.execute("PRAGMA user_version=24")
        writer.commit()
        with sqlite3.connect(self.path.as_uri() + "?mode=ro&immutable=1", uri=True) as sealed:
            self.assertEqual(sealed.execute("PRAGMA user_version").fetchone()[0], 23)
        with patch.dict("os.environ", {"DCAR_LOADED_BUILD_ID": "sha256:fixture"}), \
                patch.object(api, "connect", storage.connect), \
                patch("v8.runtime_proof_workers.start") as start, \
                patch("v8.runtime_proof_workers.stop") as stop:
            with api._runtime_proof_worker_pool(self.config, enabled=True):
                start.assert_called_once_with(self.path)
            stop.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
