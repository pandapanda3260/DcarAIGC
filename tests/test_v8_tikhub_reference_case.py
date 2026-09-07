from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from tests.roster_fixture import accept_roster
from v8 import capture, capture_integrated_natural_due, capture_runtime, providers
from v8.capture import ProviderResult
from v8.operations import upsert_account
from v8.storage import connect, initialize_database, now_utc


SEC = "MS4wLjAB" + "A" * 68
UID = "99887766"


class ProviderReferenceCaseTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.db = self.root / "reference-case.sqlite3"
        self.raw_patch = patch.object(capture, "RAW_ROOT", self.root / "raw")
        self.raw_patch.start()
        self.addCleanup(self.raw_patch.stop)
        with connect(self.db) as connection:
            initialize_database(connection)
        account = upsert_account({"phone": "", "platforms": [
            {"platform": "douyin", "uid": UID, "nickname": "已注册账号"}]}, db_path=self.db)
        self.account_id = int(account["id"])
        with connect(self.db) as connection:
            self.identity_id = int(connection.execute(
                "SELECT id FROM account_platform_identities WHERE account_id=?", (self.account_id,)
            ).fetchone()[0])
            accept_roster(connection)
            connection.commit()

    def _store_reference(self, provider: str) -> None:
        at = now_utc()
        with connect(self.db) as connection:
            connection.execute("INSERT INTO account_provider_references(account_identity_id,provider,"
                               "reference_kind,reference_value,created_at,updated_at) VALUES(?,?,'sec_user_id',?,?,?)",
                               (self.identity_id, provider, SEC, at, at))
            connection.commit()

    def _discover(self) -> tuple[dict, list[str]]:
        calls: list[str] = []

        def fake_transport(operation, identity):
            calls.append(operation)
            self.assertEqual(operation, "discover_content", "Cached registration must not buy UID resolution")
            self.assertEqual(identity["uid"], UID)
            return ProviderResult({"items": [], "has_more": False},
                                  {"data": {"aweme_list": [], "has_more": 0}}, 200, True)

        result = providers.discover_account_content(self.account_id, "douyin", UID,
            as_of=date(2026, 9, 6), db_path=self.db, call_override=fake_transport)
        return result, calls

    def test_lowercase_registration_reaches_real_fetch_without_reference_lookup(self) -> None:
        self._store_reference("tikhub")
        result, calls = self._discover()
        self.assertEqual(result["reference_status"], "cached")
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(calls, ["discover_content"])
        with connect(self.db) as connection:
            usages = connection.execute("SELECT operation,amount FROM provider_usage").fetchall()
            self.assertEqual([(row[0], row[1]) for row in usages], [("douyin_user_posts", 0.001)])
            refs = connection.execute("SELECT provider,reference_value FROM account_provider_references").fetchall()
            self.assertEqual([(row[0], row[1]) for row in refs], [("tikhub", SEC)])

    def test_original_mixed_case_reference_remains_usable(self) -> None:
        self._store_reference("TikHub")
        result, calls = self._discover()
        self.assertEqual(result["reference_status"], "cached")
        self.assertEqual(calls, ["discover_content"])

    def test_reference_case_fix_does_not_admit_disabled_account(self) -> None:
        self._store_reference("tikhub")
        with connect(self.db) as connection:
            connection.execute("UPDATE accounts SET enabled=0 WHERE id=?", (self.account_id,))
            connection.commit()
        with self.assertRaises(Exception) as caught:
            self._discover()
        self.assertIn("member", str(getattr(caught.exception, "error_code", "")))


class IntegratedReferenceCaseTest(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:")
        self.connection.row_factory = sqlite3.Row
        self.addCleanup(self.connection.close)
        self.connection.executescript("""
            CREATE TABLE account_provider_references(account_identity_id INTEGER,
                provider TEXT,reference_kind TEXT,reference_value TEXT);
            CREATE TABLE capture_paid_send_gate_events(id INTEGER PRIMARY KEY,provider TEXT,
                operation TEXT,state TEXT,recorded_at TEXT);
        """)
        self.connection.execute("INSERT INTO account_provider_references VALUES (1,'tikhub','sec_user_id',?)", (SEC,))
        self.envelope = {"platform": "douyin", "uid": UID, "stage": "discovery", "operation": "douyin_user_posts",
                         "identity_id": 1, "account_id": 1, "assignment_id": 1, "cursor": 0,
                         "logical_due": "2026-09-06T18:00:00+08:00", "window_start": "2026-09-05T00:00:00+08:00",
                         "window_end": "2026-09-06T00:00:00+08:00"}

    def test_lowercase_reference_passes_planning_reference_check_but_keeps_send_gate(self) -> None:
        route = {"id": 1, "mode": "active", "route": "integrated"}
        with patch.object(capture_runtime.planning, "resolve_route", return_value=route):
            result = capture_runtime._readiness(self.connection, self.envelope, at="2026-09-06T10:00:00Z")
        self.assertEqual(result, ("provider_blocked", "provider_transport_blocked"))

    def test_final_integrated_request_uses_registered_reference_as_exact_subject(self) -> None:
        request = capture_integrated_natural_due._single_identity(
            self.connection, {"envelope_json": json.dumps(self.envelope)})
        self.assertEqual(request.document["request_parameters"]["sec_user_id"], SEC)


if __name__ == "__main__":
    unittest.main()
