import importlib.util
import json
from pathlib import Path
import plistlib
import sqlite3
from types import SimpleNamespace

import unittest
import tempfile
from unittest.mock import patch


spec = importlib.util.spec_from_file_location("install_account_cleanup", Path(__file__).parents[1] / "scripts/install_account_cleanup.py")
installer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(installer)


class InstallAccountCleanupTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        tmp_path = Path(self.directory.name).resolve()
        formal = tmp_path / "formal.sqlite3"
        candidate = tmp_path / "candidate.sqlite3"
        for path, ids in ((formal, (1, 2)), (candidate, (1,))):
            connection = sqlite3.connect(path)
            connection.executescript("PRAGMA user_version=20; CREATE TABLE accounts(id INTEGER PRIMARY KEY); CREATE TABLE content_items(id INTEGER PRIMARY KEY, account_id REFERENCES accounts(id)); CREATE TABLE account_directory_rows(id INTEGER PRIMARY KEY);")
            for identifier in ids:
                connection.execute("INSERT INTO accounts VALUES(?)", (identifier,))
                connection.execute("INSERT INTO content_items VALUES(?,?)", (identifier, identifier))
            connection.executemany("INSERT INTO account_directory_rows VALUES(?)", [(i,) for i in range(1, 293)])
            connection.commit()
            connection.close()
        lock = tmp_path / "writer.lock"
        lock.write_bytes(b"")
        plist = tmp_path / "Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
        plist.parent.mkdir(parents=True)
        plist.write_bytes(plistlib.dumps({"EnvironmentVariables": {"DCAR_V8_DB": str(formal), "DCAR_WRITER_LOCK": str(lock)}}))
        plan = {"contract": "account-cleanup-prepared-v1", "formal_database": str(formal),
                "frozen_formal_sha256": installer.digest_file(formal),
                "candidate": {"path": str(candidate), "sha256": installer.digest_file(candidate)},
                "rollback_database": str(tmp_path / "rollback.sqlite3"),
                "install_receipt_path": str(tmp_path / "installed.json"), "evidence": [],
                "source_database_sha256": installer.digest_file(formal), "build_receipt": {},
                "expected_scope": {"account_ids": [1], "content_ids": [1]}}
        path = tmp_path / "prepared.json"
        path.write_text(json.dumps(plan))
        self.mock_home = patch.object(installer.pwd, "getpwuid", lambda _: SimpleNamespace(pw_dir=str(tmp_path)))
        self.mock_home.start()
        self.addCleanup(self.mock_home.stop)
        self.mock_process = patch.object(installer.subprocess, "run", return_value=SimpleNamespace(returncode=1, stdout=""))
        self.process = self.mock_process.start()
        self.addCleanup(self.mock_process.stop)
        self.prepared = path, plan



    def test_installs_verified_scope_and_keeps_rollback(self):
        path, plan = self.prepared
        inode = Path(plan["candidate"]["path"]).stat().st_ino
        result = installer.install(path, installer.digest_file(path))
        assert result["installed"]["inode"] == inode
        assert installer.digest_file(Path(plan["rollback_database"])) == plan["frozen_formal_sha256"]
        assert installer.digest_file(Path(plan["formal_database"])) == plan["candidate"]["sha256"]


    def test_refuses_changed_frozen_source(self):
        path, plan = self.prepared
        with sqlite3.connect(plan["formal_database"]) as connection:
            connection.execute("INSERT INTO accounts VALUES(3)")
        with self.assertRaisesRegex(ValueError, "Frozen database changed"):
            installer.install(path, installer.digest_file(path))
        assert Path(plan["candidate"]["path"]).exists()
        assert not Path(plan["rollback_database"]).exists()


    def test_refuses_open_handles(self):
        path, plan = self.prepared
        self.process.return_value = SimpleNamespace(returncode=0, stdout="123\n")
        with self.assertRaisesRegex(ValueError, "open handles"):
            installer.install(path, installer.digest_file(path))
        assert not Path(plan["rollback_database"]).exists()


    def test_failed_receipt_restores_source_before_releasing_lock(self):
        path, plan = self.prepared
        def fail(*args):
            raise OSError("simulated receipt failure")
        mock_json = patch.object(installer, "atomic_json", fail)
        mock_json.start()
        self.addCleanup(mock_json.stop)
        with self.assertRaisesRegex(OSError, "receipt failure"):
            installer.install(path, installer.digest_file(path))
        assert installer.digest_file(Path(plan["formal_database"])) == plan["frozen_formal_sha256"]
        assert installer.digest_file(Path(plan["candidate"]["path"])) == plan["candidate"]["sha256"]
