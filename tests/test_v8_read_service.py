"""Reader isolation, committed WAL visibility and cache correctness contracts."""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import httpx
from fastapi.testclient import TestClient

from dcar_auth import gateway, store as auth_store
from tests.test_v8_api import _seed_read_model_database
from v8 import api
from v8.read_api import ReadApiConfig, create_app
from v8.read_cache import BoundedReadCache, DatabaseRevision, ReadCacheBusy
from v8.read_contract import READ_KEY_HEADER, READ_SCOPE_HEADER, read_domain, invalidation_domains

KEY = "read-service-fixture-key-32-characters-minimum"


class ReadCacheTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "read.sqlite3"
        self.writer = sqlite3.connect(self.path)
        self.addCleanup(self.writer.close)
        self.writer.executescript("PRAGMA journal_mode=WAL; CREATE TABLE accounts(id INTEGER PRIMARY KEY, name TEXT, updated_at TEXT);"
                                 "CREATE TABLE heartbeat(id INTEGER PRIMARY KEY, updated_at TEXT);"
                                 "CREATE TABLE content_items(id INTEGER PRIMARY KEY, title TEXT, updated_at TEXT);"
                                 "INSERT INTO accounts VALUES(1,'first','same-second');"
                                 "INSERT INTO content_items VALUES(1,'first','same-second');")
        self.revision = DatabaseRevision(self.path, check_interval=0)
        self.addCleanup(self.revision.close)
        self.clock = [10.0]
        self.cache = BoundedReadCache(self.revision, max_entries=2, max_bytes=100,
                                     ttl_seconds=30, clock=lambda: self.clock[0])

    def get(self, key="one", loader=lambda: {"value": "first"}, domain="accounts"):
        timings = {}
        result = self.cache.get(domain, (key,), loader, timings)
        return result, timings

    def test_unrelated_commits_do_not_invalidate_but_same_timestamp_account_edit_does(self):
        first, _ = self.get()
        self.writer.execute("INSERT INTO heartbeat VALUES(1,'later')")
        self.writer.commit()
        second, timing = self.get()
        self.assertEqual(first.revision, second.revision)
        self.assertEqual(timing["cache"], "hit")
        self.writer.execute("UPDATE accounts SET name='changed' WHERE id=1")
        self.writer.commit()
        third, timing = self.get(loader=lambda: {"value": "changed"})
        self.assertNotEqual(first.revision, third.revision)
        self.assertEqual(timing["cache"], "miss")

    def test_summary_collision_has_hard_ttl_and_write_invalidation(self):
        self.get(domain="contents")
        self.writer.execute("UPDATE content_items SET title='changed' WHERE id=1")
        self.writer.commit()
        self.assertEqual(self.get(domain="contents")[1]["cache"], "hit")
        self.clock[0] += 30
        self.assertEqual(self.get(domain="contents")[1]["cache"], "miss")
        self.cache.invalidate({"contents"})
        self.assertEqual(self.get(domain="contents")[1]["cache"], "miss")

    def test_entry_byte_limits_and_authorization_scope_separation(self):
        for key in ("one", "two", "three"):
            self.get(key)
        self.assertEqual(len(self.cache.values), 2)
        self.assertLessEqual(self.cache.bytes, 100)
        self.assertEqual(self.get("another-user")[1]["cache"], "miss")
        self.get("too-big", lambda: {"payload": "x" * 101})
        self.assertFalse(any("too-big" in key for key in self.cache.values))

    def test_singleflight(self):
        entered, release = threading.Event(), threading.Event()
        waiting = threading.Event()
        waiter_count = []
        calls = []

        class ObservedFuture(Future):
            def result(self, timeout=None):
                waiter_count.append(1)
                if len(waiter_count) == 3:
                    waiting.set()
                return super().result(timeout)

        def load():
            calls.append(1)
            entered.set()
            self.assertTrue(release.wait(5))
            return {"value": "done"}

        with patch("v8.read_cache.Future", ObservedFuture), ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(self.get, "shared", load) for _ in range(4)]
            self.assertTrue(entered.wait(5))
            self.assertTrue(waiting.wait(5))
            release.set()
            results = [future.result(5) for future in futures]
        self.assertTrue(all(json.loads(result[0].body)["value"] == "done" for result in results))
        self.assertEqual(len(calls), 1)
        self.assertEqual(sum(result[1]["cache"] == "coalesced" for result in results), 3)

    def test_invalidation_during_load_cannot_repopulate_old_generation(self):
        entered, release = threading.Event(), threading.Event()

        def old_loader():
            entered.set()
            self.assertTrue(release.wait(5))
            return {"value": "old"}

        with ThreadPoolExecutor(max_workers=1) as pool:
            first = pool.submit(self.get, "same", old_loader)
            self.assertTrue(entered.wait(5))
            self.cache.invalidate({"accounts"})
            self.get("same", lambda: {"value": "new"})
            release.set()
            first.result(5)
        current, timing = self.get("same")
        self.assertEqual(timing["cache"], "hit")
        self.assertEqual(json.loads(current.body)["value"], "new")

    def test_singleflight_wait_timeout_is_busy_and_does_not_cancel_owner(self):
        entered, release = threading.Event(), threading.Event()

        class TimeoutFuture(Future):
            def result(self, timeout=None):
                raise TimeoutError("simulated wait expiry")

        def load():
            entered.set()
            self.assertTrue(release.wait(5))
            return {"value": "complete"}

        with patch("v8.read_cache.Future", TimeoutFuture), ThreadPoolExecutor(max_workers=1) as pool:
            owner = pool.submit(self.get, "shared", load)
            self.assertTrue(entered.wait(5))
            try:
                with self.assertRaises(ReadCacheBusy):
                    self.get("shared", load)
                self.assertEqual(len(self.cache.inflight), 1)
            finally:
                release.set()
            owner.result(5)
        self.assertEqual(self.get("shared")[1]["cache"], "hit")

    def test_revision_sampling_is_bounded_and_explicit_invalidation_bypasses_window(self):
        revision = DatabaseRevision(self.path, check_interval=5, clock=lambda: self.clock[0])
        self.addCleanup(revision.close)
        first = revision.get("accounts")
        self.writer.execute("UPDATE accounts SET name='second'")
        self.writer.commit()
        self.assertEqual(revision.get("accounts"), first)
        self.clock[0] += 5
        second = revision.get("accounts")
        self.assertNotEqual(second, first)
        self.writer.execute("UPDATE accounts SET name='third'")
        self.writer.commit()
        revision.invalidate({"accounts"})
        self.assertNotEqual(revision.get("accounts"), second)

    def test_replaced_database_reopens_anchor_and_clears_cached_values(self):
        first, _ = self.get()
        replacement = self.path.with_name("replacement.sqlite3")
        with sqlite3.connect(replacement) as connection:
            connection.executescript("CREATE TABLE accounts(id INTEGER PRIMARY KEY,name TEXT,updated_at TEXT);"
                                     "INSERT INTO accounts VALUES(2,'replacement','same-second');")
        self.writer.close()
        replacement.replace(self.path)
        result, timing = self.get(loader=lambda: {"value": "replacement"})
        self.assertEqual(timing["cache"], "miss")
        self.assertNotEqual(first.revision, result.revision)
        self.assertEqual(len(self.cache.values), 1)

    def test_anchor_rejects_writes_and_failed_loader_is_not_cached(self):
        self.revision.get("accounts")
        with self.assertRaises(sqlite3.OperationalError):
            self.revision.connection.execute("UPDATE accounts SET name='bad'")
        with self.assertRaisesRegex(RuntimeError, "failure"):
            self.get(loader=lambda: (_ for _ in ()).throw(RuntimeError("failure")))
        self.assertEqual(self.get()[1]["cache"], "miss")


class ReadServiceTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.path = self.root / "fixture.sqlite3"
        _seed_read_model_database(self.path)
        self.key = self.root / "reader.key"
        self.key.write_text(KEY)
        self.config = ReadApiConfig(self.path, self.root, self.key, test_fixture=True)
        self.headers = {READ_KEY_HEADER: KEY, READ_SCOPE_HEADER: "admin-fixture"}

    def test_no_writer_lifespan_routes_or_untrusted_reads(self):
        with patch.object(api, "_lifespan_runtime", side_effect=AssertionError("writer must not start")):
            with TestClient(create_app(self.config)) as client:
                self.assertEqual(client.get("/internal/read/health").status_code, 403)
                self.assertEqual(client.get("/internal/read/health", headers=self.headers).json()["scheduler_enabled"], False)
                self.assertEqual(client.post("/api/v8/accounts/search", json={}, headers={READ_KEY_HEADER: KEY}).status_code, 403)
                self.assertEqual(client.post("/api/v8/accounts", json={}, headers=self.headers).status_code, 404)
                self.assertEqual(client.get("/api/v8/health", headers=self.headers).status_code, 404)

    def test_search_matches_original_and_cache_is_observable(self):
        from v8.storage import live_wal_read_only_connections
        with live_wal_read_only_connections():
            expected = api._account_search(api.AccountSearchRequest(page_size=100), db_path=self.path, read_only=True)
        with TestClient(create_app(self.config)) as client:
            first = client.post("/api/v8/accounts/search", json={"page_size": 100}, headers=self.headers)
            self.assertEqual(first.status_code, 200, first.text)
            self.assertEqual(first.json(), expected)
            second = client.post("/api/v8/accounts/search", json={"page_size": 100}, headers=self.headers)
            self.assertIn('cache;desc="hit"', second.headers["server-timing"])
            self.assertEqual(second.headers["cache-control"], "private, no-store")
            self.assertRegex(second.headers["x-dcar-read-request-id"], r"^[a-f0-9]{32}$")
            self.assertNotEqual(first.headers["x-dcar-read-request-id"], second.headers["x-dcar-read-request-id"])
            self.assertEqual(client.post("/api/v8/accounts/search", json={"compact": "true"}, headers=self.headers).status_code, 422)
            self.assertEqual(client.get("/api/v8/accounts/directory/99999", headers=self.headers).status_code, 404)

    def test_overload_and_wait_timeout_return_retryable_503(self):
        with TestClient(create_app(self.config)) as client:
            for error in (ReadCacheBusy("full"), TimeoutError("expired")):
                with patch.object(client.app.state.cache, "get", side_effect=error):
                    response = client.post("/api/v8/accounts/search", json={},
                        headers={**self.headers, "X-Dcar-Read-Request-Id": "forged"})
                self.assertEqual(response.status_code, 503)
                self.assertEqual(response.headers["retry-after"], "1")
                self.assertNotEqual(response.headers["x-dcar-read-request-id"], "forged")
                self.assertEqual(response.headers["cache-control"], "private, no-store")

    def test_environment_cannot_bypass_formal_lineage(self):
        with patch.dict("os.environ", {"DCAR_V8_DB": str(self.path), "DCAR_PROJECT_ROOT": str(self.root),
                                     "DCAR_READ_API_KEY_FILE": str(self.key), "DCAR_READ_API_TEST_FIXTURE": "1"}):
            config = ReadApiConfig.from_env()
            self.assertFalse(config.test_fixture)
            with patch("v8.read_api.resolve_installed_database_access", side_effect=RuntimeError("wrong lineage")):
                with self.assertRaisesRegex(RuntimeError, "wrong lineage"):
                    with TestClient(create_app(config)):
                        pass


class ReadGatewayTest(unittest.TestCase):
    def test_allowlist_identity_gates_write_routing_and_targeted_invalidation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            key = root / "read.key"
            key.write_text(KEY)
            pepper = root / "pepper"
            pepper.write_text("0123456789abcdef" * 4)
            login = root / "login.html"
            login.write_text("login")
            config = gateway.AuthGatewayConfig(base_path="", web_upstream="http://web.test",
                api_upstream="http://writer.test", read_api_upstream="http://127.0.0.1:8768",
                read_api_key_path=key, session_db_path=root / "auth.sqlite3", login_template_path=login,
                pepper_path=pepper, secure_cookie=False)
            store = auth_store.AuthStore(config.session_db_path, pepper=pepper.read_text())
            store.initialize()
            store.create_user("admin", "test-password-hash", role="superadmin")
            store.create_user("operator", "test-password-hash", role="operator")
            store.create_user("pending", "test-password-hash", role="new_user")
            tokens = {name: store.create_session(name, 3600) for name in ("admin", "operator", "pending")}
            requests = []

            def transport(request):
                requests.append(request)
                return httpx.Response(200, stream=httpx.ByteStream(json.dumps({"upstream": request.url.host}).encode()),
                                      headers={"Server-Timing": "compute;dur=1"})

            mock = httpx.MockTransport(transport)
            app = gateway.create_app(config, web_transport=mock, api_transport=mock, read_api_transport=mock)
            with TestClient(app, base_url="http://gateway.test") as client:
                for name in (None, "operator", "pending"):
                    client.cookies.clear()
                    if name:
                        client.cookies.set(gateway.SESSION_COOKIE, tokens[name])
                    response = client.post("/api/v8/accounts/search", json={})
                    self.assertIn(response.status_code, {401, 403})
                self.assertEqual(requests, [])
                client.cookies.set(gateway.SESSION_COOKIE, tokens["admin"])
                response = client.post("/api/v8/accounts/search", json={"compact": True},
                    headers={READ_KEY_HEADER: "forged", READ_SCOPE_HEADER: "forged"})
                self.assertEqual(response.json()["upstream"], "127.0.0.1")
                self.assertEqual(requests[-1].headers[READ_KEY_HEADER], KEY)
                self.assertNotEqual(requests[-1].headers[READ_SCOPE_HEADER], "forged")
                self.assertIn("gateway_proxy;dur=", response.headers["server-timing"])
                self.assertEqual(response.headers["cache-control"], "private, no-store")
                self.assertEqual(client.get("/api/v8/health").json()["upstream"], "writer.test")
                response = client.patch("/api/v8/accounts/1", json={"nickname": "new"})
                self.assertEqual(response.json()["upstream"], "writer.test")
                self.assertEqual(requests[-1].url.path, "/internal/read/invalidate")
                self.assertIn("accounts", json.loads(requests[-1].content)["domains"])
                response = client.post("/api/v8/account-directory/1/identity", json={"platform": "douyin"})
                self.assertEqual(response.json()["upstream"], "writer.test")
                self.assertEqual(requests[-1].url.path, "/internal/read/invalidate")
                self.assertIn("accounts", json.loads(requests[-1].content)["domains"])

    def test_allowlist_is_not_prefix_or_method_based(self):
        for method, path in (("DELETE", "/api/v8/accounts/directory/1"), ("POST", "/api/v8/accounts/1"),
                             ("GET", "/api/v8/accounts/search"), ("GET", "/api/v8/health"),
                             ("POST", "/api/v8/accounts/search/extra")):
            self.assertIsNone(read_domain(method, path))
        self.assertIn("accounts", invalidation_domains("POST", "/api/v8/account-directory/1/identity"))


class ReadStartupScriptTest(unittest.TestCase):
    """Exercise the shell boundary without a production DB or a listening server."""
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        (self.root / "scripts").mkdir()
        self.script = self.root / "scripts/start_read_api.sh"
        shutil.copyfile(Path(__file__).resolve().parents[1] / "scripts/start_read_api.sh", self.script)
        package = self.root / "src/dcar_eval/v8"
        package.mkdir(parents=True)
        (package / "__init__.py").write_text("")
        (package / "read_api.py").write_text('''
import os
from pathlib import Path
class ReadApiConfig:
    @classmethod
    def from_env(cls):
        assert "FIXTURE_PROVIDER_SECRET" not in os.environ
        assert "HTTP_PROXY" not in os.environ
        assert os.environ["DCAR_SCHEDULER_ENABLED"] == "0"
        assert os.environ["DCAR_READ_ONLY"] == "1"
        assert os.environ["DCAR_LLM_DISABLED"] == "1"
        value = cls()
        value.db_path = Path(os.environ["DCAR_V8_DB"])
        value.key_path = Path(os.environ["DCAR_READ_API_KEY_FILE"])
        return value
    def validate(self):
        if self.db_path.read_text() != "accepted-lineage":
            raise RuntimeError("wrong lineage")
''')
        (package / "storage.py").write_text('''
from contextlib import nullcontext
def live_wal_read_only_connections():
    return nullcontext()
def connect(path, *, read_only):
    assert read_only is True
    return nullcontext(object())
def require_schema_compatibility(connection, *, supported_versions):
    assert 23 in supported_versions
''')
        self.database = self.root / "lineage-fixture"
        self.database.write_text("accepted-lineage")
        self.key = self.root / "private/reader.key"
        self.environment = {**os.environ, "DCAR_PROJECT_ROOT": str(self.root), "DCAR_V8_DB": str(self.database),
            "DCAR_READ_API_KEY_FILE": str(self.key), "DCAR_READ_PYTHON": sys.executable,
            "FIXTURE_PROVIDER_SECRET": "do-not-inherit", "HTTP_PROXY": "http://fixture.invalid",
            "DCAR_SCHEDULER_ENABLED": "1", "PYTHONPATH": "/invalid", "DCAR_READ_API_PORT": "8768"}

    def run_check(self):
        return subprocess.run(["/bin/bash", str(self.script), "--check"], env=self.environment,
                              text=True, capture_output=True, timeout=10)

    def test_clean_environment_initializes_private_key_once(self):
        first = self.run_check()
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertTrue(json.loads(first.stdout)["key_initialized"])
        self.assertEqual(self.key.stat().st_mode & 0o777, 0o600)
        original = self.key.read_text()
        self.assertNotIn(original.strip(), first.stdout + first.stderr)
        second = self.run_check()
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertFalse(json.loads(second.stdout)["key_initialized"])
        self.assertEqual(self.key.read_text(), original)
        self.key.chmod(0o644)
        self.assertNotEqual(self.run_check().returncode, 0)

    def test_lineage_failure_creates_no_key(self):
        self.database.write_text("wrong-lineage")
        result = self.run_check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("wrong lineage", result.stderr)
        self.assertFalse(self.key.exists())
