"""Temporary WAL fixtures only; never open the installed/formal database."""
from __future__ import annotations

import sys
sys.dont_write_bytecode = True

import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

WEB = Path(__file__).resolve().parents[1]
BACKEND = Path(os.environ.get("DCAR_CONTENT_SEARCH_TEST_BACKEND", str(
    WEB.parents[1] / "runtime/web/content-date-filter-20260907/backend"
)))
SPEC = importlib.util.spec_from_file_location("content_search_helper", WEB / "server/content_search.py")
helper = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(helper)
os.environ["DCAR_TEST_DENY_FORMAL_DB"] = "1"


class ContentSearchTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not BACKEND.is_dir():
            raise unittest.SkipTest("Set DCAR_CONTENT_SEARCH_TEST_BACKEND to the deployed local date-search backend")
        cls.environment = os.environ.copy()
        cls.import_paths = sys.path[:]
        cls.modules = {key: value for key, value in sys.modules.items() if key == "v8" or key.startswith("v8.")}
        # Merely importing the API must not open a database, launch work, or
        # connect to a remote service. The import does not enter its lifespan.
        with tempfile.TemporaryDirectory() as tmp, patch("sqlite3.connect") as connect, \
                patch("threading.Thread.start") as thread, patch("socket.socket.connect") as network:
            cls.api, cls.storage = helper.load_backend(BACKEND, Path(tmp) / "not-created.sqlite3", BACKEND)
            connect.assert_not_called()
            thread.assert_not_called()
            network.assert_not_called()
            assert not (Path(tmp) / "not-created.sqlite3").exists()

    @classmethod
    def tearDownClass(cls):
        # The production helper exits after each request; mirror that isolation
        # before unrelated helper suites import the ordinary API source.
        os.environ.clear()
        os.environ.update(cls.environment)
        sys.path[:] = cls.import_paths
        for key in list(sys.modules):
            if key == "v8" or key.startswith("v8."):
                sys.modules.pop(key)
        sys.modules.update(cls.modules)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "fixture.sqlite3"
        self.writer = self.storage.connect(self.db, read_only=False)
        self.storage.initialize_database(self.writer)
        self.writer.commit()
        self.writer.execute("PRAGMA journal_mode=WAL")
        self.writer.execute("PRAGMA wal_autocheckpoint=0")
        self.writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self.insert(1, "2026-09-06T15:59:59.999Z")
        self.insert(2, "2026-09-06T16:00:00Z")
        self.insert(3, "2026-09-07T15:59:59.999Z")
        self.insert(4, "2026-09-07T16:00:00Z")
        self.insert(5, "2026-09-07T04:00:00Z", platform="xiaohongshu")
        self.insert(6, None)
        self.writer.execute("""INSERT INTO evidence_artifacts(
            content_id,artifact_type,local_path,status,created_at
        ) VALUES (2,'media','fixture.jpg','available','2026-09-07T00:00:00Z')""")
        self.writer.commit()

    def tearDown(self):
        self.writer.close()
        self.temp.cleanup()

    def insert(self, number, published, platform="douyin", account_id=None):
        self.writer.execute("""INSERT INTO content_items(
            id,link_id,platform,platform_content_id,canonical_url,published_at,title,
            content_type,account_id,imported_at,created_at,updated_at
        ) VALUES (?,?,?,?,?,?,?,'video',?,'now','now','now')""",
            (number, f"T{number:05}", platform, str(number), f"https://example.test/{number}",
             published, "汽车测试", account_id))

    def search(self, **values):
        return helper.search(helper.validate_request(values, self.api), self.db, self.api, self.storage)

    def test_committed_wal_rows_are_visible_without_checkpoint(self):
        self.assertGreater(self.db.with_name(self.db.name + "-wal").stat().st_size, 0)
        with self.storage.connect(self.db, read_only=True) as immutable:
            self.assertEqual(immutable.execute("SELECT COUNT(*) FROM content_items").fetchone()[0], 0)
        self.assertEqual(self.search()["total"], 6)
        self.insert(7, "2026-09-08T01:00:00Z")
        self.writer.commit()
        latest = self.search()
        self.assertEqual(latest["total"], 7)
        self.assertEqual(latest["items"][0]["id"], 7)

    def test_date_platform_pagination_and_total_keep_existing_scope(self):
        filters = dict(published_from="2026-09-07", published_to="2026-09-07", platform="douyin", query="汽车")
        first = self.search(**filters, page_size=1)
        second = self.search(**filters, page=2, page_size=1)
        self.assertEqual((first["total"], second["total"]), (2, 2))
        self.assertEqual([first["items"][0]["id"], second["items"][0]["id"]], [3, 2])
        self.assertEqual(self.search(**filters, page=2000)["items"], [])
        self.assertEqual(self.search(published_to="2026-09-07")["total"], 4)
        self.assertEqual(self.search()["total"], 6)
        self.writer.execute("INSERT INTO accounts(id,phone,enabled,created_at,updated_at) VALUES(1,'fixture',0,'now','now')")
        self.insert(7, "2026-09-07T05:00:00Z", account_id=1)
        self.writer.commit()
        self.assertEqual(self.search(**filters)["total"], 2)
        self.assertEqual(self.search()["total"], 6)

    def test_read_only_query_keeps_local_media_projection(self):
        page = self.search()
        flags = {item["id"]: item["local_media_available"] for item in page["items"]}
        self.assertTrue(flags[2])
        self.assertFalse(flags[3])
        with self.storage.live_wal_read_only_connections():
            replica = self.api._content_search(self.api.ContentSearchRequest(), db_path=self.db, read_only=True)
        self.assertFalse(any(item["local_media_available"] for item in replica["items"]))

    def test_connection_is_readonly_and_files_are_unchanged(self):
        paths = [self.db, self.db.with_name(self.db.name + "-wal")]
        before = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
        opened = []
        original = self.api.connect

        def checked_connect(path, *, read_only):
            self.assertTrue(read_only)
            connection = original(path, read_only=read_only)
            self.assertEqual(connection.execute("PRAGMA query_only").fetchone()[0], 1)
            with self.assertRaises(sqlite3.OperationalError):
                connection.execute("UPDATE content_items SET title='must not write'")
            connection.rollback()
            connection.execute("PRAGMA query_only=OFF")
            with self.assertRaises(sqlite3.OperationalError):
                connection.execute("UPDATE content_items SET title='mode ro still blocks writes'")
            connection.rollback()
            connection.execute("PRAGMA query_only=ON")
            opened.append(connection)
            return connection

        with patch.object(self.api, "connect", checked_connect):
            self.assertEqual(self.search()["total"], 6)
        self.assertEqual(len(opened), 1)
        self.assertEqual(before, {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths})

    def test_count_rows_and_media_share_one_snapshot_during_concurrent_commit(self):
        original = self.api.connect
        test = self
        committed = False

        class ObservedConnection:
            def __init__(self, connection):
                self.connection = connection
            def __enter__(self):
                self.connection.__enter__()
                return self
            def __exit__(self, *args):
                return self.connection.__exit__(*args)
            def __getattr__(self, key):
                return getattr(self.connection, key)
            def execute(self, sql, *args):
                nonlocal committed
                cursor = self.connection.execute(sql, *args)
                if "SELECT COUNT(*) FROM content_items" in sql and not committed:
                    test.assertTrue(self.connection.in_transaction)
                    committed = True
                    test.insert(7, "2026-09-08T01:00:00Z")
                    test.writer.execute("UPDATE evidence_artifacts SET status='missing' WHERE content_id=2")
                    test.writer.commit()
                return cursor

        with patch.object(self.api, "connect", lambda *args, **kwargs: ObservedConnection(original(*args, **kwargs))):
            result = self.search()
        self.assertTrue(committed)
        self.assertEqual(result["total"], 6)
        self.assertEqual(len(result["items"]), 6)
        self.assertTrue(next(item["local_media_available"] for item in result["items"] if item["id"] == 2))
        next_result = self.search()
        self.assertEqual(next_result["total"], 7)
        self.assertFalse(next(item["local_media_available"] for item in next_result["items"] if item["id"] == 2))

    def test_strict_validation_and_bounded_json(self):
        for body in [b"[]", b"{", b"{\"page\":1,\"page\":2}", b"{\"page\":NaN}", b" " * 8193]:
            with self.subTest(body=body[:80]), self.assertRaises(helper.RequestError):
                helper.read_request(io.BytesIO(body))
        for payload in [dict(page=True), dict(page="1"), dict(page_size=101), dict(page=0),
                        dict(unknown=1), dict(query=[]), dict(published_from="2026-02-30"),
                        dict(published_from="2026-09-08", published_to="2026-09-07")]:
            with self.subTest(payload=payload), self.assertRaises(helper.RequestError):
                helper.validate_request(payload, self.api)
        self.assertEqual(helper.validate_request(dict(page=2000), self.api).page, 2000)
        self.assertEqual(helper.read_request(io.BytesIO(b"{}")), {})

    def test_sql_deadline_interrupts_expensive_queries(self):
        for number in range(7, 2500):
            self.insert(number, "2026-09-07T04:00:00Z")
        self.writer.commit()
        with patch.object(helper, "QUERY_TIMEOUT_SECONDS", 0.000001):
            with self.assertRaisesRegex(sqlite3.OperationalError, "interrupted"):
                self.search()

    def test_cli_returns_safe_422_and_503_errors(self):
        argv = ["--db", str(self.db), "--backend-root", str(BACKEND), "--project-root", str(BACKEND)]
        for body, status in [(b'{"published_from":"2026-02-30"}', 422), (b"{}", 503)]:
            stdout = io.StringIO()
            stdin = type("Stdin", (), {"buffer": io.BytesIO(body)})()
            with patch.object(sys, "stdin", stdin), patch.object(sys, "stdout", stdout), \
                    patch.object(helper, "verify_database", side_effect=RuntimeError("secret-file-path")):
                self.assertEqual(helper.main(argv), 1)
            result = json.loads(stdout.getvalue())
            self.assertEqual(result["error"]["status"], status)
            self.assertNotIn("secret-file-path", stdout.getvalue())
            self.assertNotIn(str(self.db), stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
