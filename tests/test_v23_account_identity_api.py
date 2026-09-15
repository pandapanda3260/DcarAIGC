"""Actual schema23/API path for original-row locator corrections."""
from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from tests.test_v8_api import _test_config
from v8 import api
from v8.account_directory_reconciliation import directory_locator_snapshot
from v8.account_intake import _fingerprint
from v8.storage import connect, initialize_database

AT = "2026-09-12T03:00:00Z"


class AccountIdentityApiV23Test(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.config = _test_config(Path(self.temp.name))
        with connect(self.config.db_path) as db:
            initialize_database(db, target_version=23)
            self.did = db.execute("""INSERT INTO account_directory_rows
                (source_sha256,source_name,source_sheet,source_row,platform,nickname,account_status,identity_status,raw_json,imported_at,updated_at)
                VALUES(?,'fixture.xlsx','accounts',2,'douyin','fixture','daily','identity_missing',?,?,?)""",
                ("a" * 64, json.dumps({"original": "keep"}), AT, AT)).lastrowid
            row = dict(db.execute("SELECT * FROM account_directory_rows WHERE id=?", (self.did,)).fetchone())
            self.hash = _fingerprint(directory_locator_snapshot(row))
            db.commit()
        self.client = TestClient(api.create_app(self.config)); self.addCleanup(self.client.close)
        self.enterContext(patch("socket.socket.connect", side_effect=AssertionError("network forbidden")))

    def body(self, **values):
        return {"request_id": "fixture-repair", "expected_locator_sha256": self.hash,
                "platform": "douyin", "uid": "123456789", **values}

    def test_api_keeps_original_row_and_conflicts_stale_second_intent(self):
        path = f"/api/v8/account-directory/{self.did}/identity"
        response = self.client.post(path, json=self.body())
        self.assertEqual(response.status_code, 202, response.text)
        self.assertEqual(response.json()["directory_row_id"], self.did)
        self.assertNotEqual(response.json()["locator_sha256"], self.hash)
        repeated = self.client.post(path, json=self.body())
        self.assertEqual(repeated.status_code, 202, repeated.text)
        self.assertTrue(repeated.json()["replayed"])
        stale = self.client.post(path, json=self.body(request_id="different", uid="987654321"))
        self.assertEqual(stale.status_code, 409, stale.text)
        with connect(self.config.db_path) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM account_directory_rows").fetchone()[0], 1)
            self.assertEqual(json.loads(db.execute("SELECT raw_json FROM account_directory_rows").fetchone()[0]), {"original": "keep"})
            self.assertEqual(db.execute("SELECT count(*) FROM account_intake_requests").fetchone()[0], 1)

    def test_shortlink_is_outside_write_transaction_and_replay_never_reexpands(self):
        path = f"/api/v8/account-directory/{self.did}/identity"
        def expand(url):
            with connect(self.config.db_path) as db:
                db.execute("BEGIN IMMEDIATE"); db.rollback()
            return "https://www.douyin.com/user/123456789"
        body = self.body(uid="", profile_url="https://v.douyin.com/fixture/")
        with patch("v8.account_profile_public.expand_public_profile_url", side_effect=expand) as lookup:
            first = self.client.post(path, json=body)
            self.assertEqual(first.status_code, 202, first.text)
            lookup.side_effect = AssertionError("replay must not resolve again")
            second = self.client.post(path, json=body)
            self.assertEqual(second.status_code, 202, second.text)
            self.assertTrue(second.json()["replayed"])
            lookup.assert_called_once()


if __name__ == "__main__":
    unittest.main()
