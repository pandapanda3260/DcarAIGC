"""Read projection compatibility, page boundaries, and bounded SQL regression."""
from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from tests.test_v8_api import _seed_read_model_database
from v8 import account_listing as listing, api
from v8.account_directory import DIRECTORY_SCHEMA
from v8.account_metrics import ACCOUNT_FIELDS, persist_account_metric_observation, select_account_metrics
from v8.operations import OperationError
from v8.schema_v22 import INTAKE_SQL
from v8.storage import connect, live_wal_read_only_connections
from v8.system_roster import seal_system_members

AT = "2026-09-13T02:00:00Z"


class AccountListingTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="dcar-account-listing-")
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / "fixture.sqlite3"
        _seed_read_model_database(self.path)
        self.enterContext(patch("socket.socket.connect", side_effect=AssertionError("network forbidden")))
        self.enterContext(patch("v8.account_metrics.now_utc", return_value=AT))
        self.enterContext(patch("v8.account_roster.now_utc", return_value=AT))
        with connect(self.path) as connection:
            connection.execute(DIRECTORY_SCHEMA)
            connection.execute(INTAKE_SQL)
            first = dict(connection.execute("SELECT * FROM accounts LIMIT 1").fetchone())
            identity = dict(connection.execute("SELECT * FROM account_platform_identities WHERE account_id=?", (first["id"],)).fetchone())
            self.identity_id = identity["id"]
            self.account_id = first["id"]
            for index in range(205):
                platform = ("douyin", "xiaohongshu", "kuaishou", "wechat_channels")[index % 4]
                uid = f"{1000000000000000000 + index}"
                aid = None
                if index == 0:
                    aid, platform, uid = first["id"], identity["platform"], identity["uid"]
                elif index < 200:
                    aid = connection.execute(
                        "INSERT INTO accounts(phone,operator_name,enabled,created_at,updated_at) VALUES(?,?,1,?,?)",
                        (f"1330000{index:04d}", f"Owner {index}", AT, AT),
                    ).lastrowid
                    connection.execute("INSERT INTO account_platform_identities(account_id,platform,uid,nickname,created_at,updated_at) "
                                       "VALUES(?,?,?,?,?,?)", (aid, platform, uid, "provider label", AT, AT))
                nickname = "Straße 车圈" if index == 0 else f"目录 {index}"
                raw = {"account_summary": {"fields": {"粉丝": "导入观察值", "手机": "untrusted source"},
                       "pending_fields": {"粉丝": "待核对"}, "comment": "original source note"}}
                connection.execute("""INSERT INTO account_directory_rows(
                    source_sha256,source_name,source_sheet,source_row,account_id,platform,uid,nickname,
                    display_account_id,phone,operator_name,account_group,business_direction,account_status,
                    identity_status,raw_json,imported_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (str(index % 2) * 64, "fixture.xlsx", "accounts", index // 2 + 2, aid, platform,
                     uid if aid else None, nickname, f"DISPLAY_{index}", f"1330000{index:04d}", f"Owner {index}",
                     "innovation" if index % 2 else "unknown", "new_car" if index % 3 else "unknown",
                     ("daily", "weekly", "paused", "unmarked")[index % 4],
                     "existing_verified" if aid else "identity_missing", json.dumps(raw), AT, AT))
            self.ids = [row[0] for row in connection.execute("SELECT id FROM account_directory_rows ORDER BY source_row,id")]
            self._metric(connection, 100, "2026-09-12T00:00:00Z")
            self._metric(connection, 70, "2026-09-13T00:00:00Z")
            self._metric(connection, None, "2026-09-13T00:30:00Z", status="missing")
            self._metric(connection, 9999, "2026-09-14T00:00:00Z")

    def _metric(self, connection, count, captured, *, status="provided"):
        raw_id = connection.execute("""INSERT INTO provider_raw_responses(
            account_id,provider,operation,local_path,sha256,byte_size,http_status,captured_at)
            VALUES(?,'newrank_matrix','matrix_account_list','fixture.json',?,10,200,?)""",
            (self.account_id, "a" * 64, captured)).lastrowid
        identity = connection.execute("SELECT platform,uid FROM account_platform_identities WHERE id=?", (self.identity_id,)).fetchone()
        normalized = {"identity": dict(identity), "statistics_date": (date.fromisoformat(captured[:10]) - timedelta(days=1)).isoformat(),
                      "metrics": {field: count if field == "follower_count" else None for field in ACCOUNT_FIELDS},
                      "field_status": {field: {"status": status if field == "follower_count" else "not_requested"}
                                       for field in ACCOUNT_FIELDS}}
        persist_account_metric_observation(connection, account_identity_id=self.identity_id, provider="newrank_matrix",
                                          raw_response_id=raw_id, normalized=normalized, captured_at=captured, recorded_at=captured)

    def new(self, **values):
        compact = values.pop("compact", False)
        return listing.search_accounts(api.AccountSearchRequest(**values), db_path=self.path, compact=compact)

    def old(self, **values):
        with live_wal_read_only_connections():
            return api._account_search(api.AccountSearchRequest(**values), db_path=self.path, read_only=True)

    def test_complete_shape_matches_old_query_for_pages_and_all_filter_families(self):
        for params in ({"page": page, "page_size": 100} for page in (1, 2, 3, 4)):
            with self.subTest(params=params):
                self.assertEqual(self.new(**params), self.old(**params))
        for params in ({"query": " STRASSE "}, {"query": "display_12"}, {"query": "车圈"},
                       {"query": "owner 20"}, {"platform": "wechat_channels"}, {"account_status": "paused"},
                       {"account_status": "unmarked"}, {"account_group": "innovation"},
                       {"business_direction": "new_car"}, {"platform": "douyin", "account_group": "innovation"},
                       {"query": "not-a-match"}):
            with self.subTest(params=params):
                self.assertEqual(self.new(**params), self.old(**params))

    def test_stable_directory_order_includes_every_unlinked_row_once(self):
        rows = [row for page in (1, 2, 3) for row in self.new(page=page, page_size=100)["items"]]
        self.assertEqual([row["directory_row_id"] for row in rows], self.ids)
        self.assertEqual(len({row["directory_row_id"] for row in rows}), 205)
        self.assertEqual(sum(row["id"] < 0 for row in rows), 5)
        self.assertTrue(all(row["id"] == -row["directory_row_id"] for row in rows[-5:]))
        self.assertEqual(self.new(page=4, page_size=100)["total"], 205)

    def test_active_roster_metadata_keeps_original_snapshot_identity_and_display_rules(self):
        with connect(self.path) as connection:
            identity = dict(connection.execute("SELECT * FROM account_platform_identities WHERE uid=?",
                                               ("1000000000000000004",)).fetchone())
            sealed = seal_system_members(connection, [{"platform": identity["platform"], "uid": identity["uid"],
                "nickname": "snapshot nickname", "profile_ref": "https://www.douyin.com/user/MS4w.fixture",
                "metadata": {"avatar_url": "https://example.invalid/avatar.jpg", "display_account_id": "snapshot-display"}}],
                raw_root=self.path.parent / "raw", actor="test", reason="metadata fixture")
        runtime = {"ready": True, "source_family": "system", "snapshot_id": sealed["snapshot_id"]}
        with patch.object(api, "runtime_account_summary", return_value=runtime), \
             patch.object(listing, "runtime_account_summary", return_value=runtime):
            actual = self.new(page_size=10)
            self.assertEqual(actual, self.old(page_size=10))
        item = next(row for row in actual["items"] if row["id"] == identity["account_id"])
        self.assertEqual(item["platforms"][0]["nickname"], "目录 4")
        self.assertEqual(item["platforms"][0]["unique_id"], "DISPLAY_4")
        self.assertEqual(item["platforms"][0]["profile_ref"], "https://www.douyin.com/user/MS4w.fixture")

    def test_compact_dto_keeps_values_and_fetches_complete_linked_and_unlinked_details(self):
        complete = self.new(page=1, page_size=100)
        compact = self.new(page=1, page_size=100, compact=True)
        self.assertEqual(compact["list_contract_version"], 1)
        for full, lean in zip(complete["items"], compact["items"]):
            self.assertNotIn("account_summary", lean)
            self.assertTrue(lean["has_account_summary"])
            expected = {key: value for key, value in full.items() if key != "account_summary"}
            expected["has_account_summary"] = True
            expected["platforms"] = [{key: value for key, value in identity.items()
                                     if key not in {"metric_fields", "statistic_identity_metadata"}}
                                    for identity in full["platforms"]]
            self.assertEqual(lean, expected)
        self.assertEqual(listing.account_detail(self.ids[0], db_path=self.path), complete["items"][0])
        missing = self.new(page=3, page_size=100)["items"][-1]
        self.assertEqual(listing.account_detail(missing["directory_row_id"], db_path=self.path), missing)
        self.assertIsNone(listing.account_detail(999999, db_path=self.path))
        self.assertIsNone(listing.account_detail(-1, db_path=self.path))

    def test_batched_metric_selection_retains_valid_decrease_missing_and_future_cutoff(self):
        with live_wal_read_only_connections(), connect(self.path, read_only=True) as connection:
            ids = [row[0] for row in connection.execute("SELECT id FROM account_platform_identities LIMIT 5")]
            batched = select_account_metrics(connection, ids, cutoff_at=AT)
            separate = {identity: select_account_metrics(connection, [identity], cutoff_at=AT)[identity] for identity in ids}
        self.assertEqual(batched, separate)
        self.assertEqual(batched[self.identity_id]["follower_count"], 70)
        self.assertEqual(batched[self.identity_id]["data_status"], "stale")
        self.assertEqual(batched[self.identity_id]["metric_fields"]["follower_count"]["captured_at"], "2026-09-13T00:00:00Z")

    def test_legacy_without_directory_keeps_full_shape_even_when_compact_requested(self):
        legacy_path = self.path.parent / "legacy.sqlite3"
        _seed_read_model_database(legacy_path)
        request = api.AccountSearchRequest()
        with live_wal_read_only_connections():
            expected = api._account_search(request, db_path=legacy_path, read_only=True)
        actual = listing.search_accounts(request, db_path=legacy_path, compact=True)
        self.assertEqual(actual, expected)
        self.assertNotIn("list_contract_version", actual)

    def test_preparation_annotation_reads_only_latest_request_for_the_visible_directory(self):
        from v8.account_intake import annotate_account_preparation
        with connect(self.path) as connection:
            connection.execute("CREATE TABLE capture_work_items(id INTEGER PRIMARY KEY,intake_request_id INTEGER,state TEXT,reason TEXT,due_at TEXT)")
            for key, directory_id, status in (("earlier", self.ids[0], "ready"),
                                              ("latest", self.ids[0], "conflict"),
                                              ("other-page", self.ids[-1], "ready")):
                request_id = connection.execute("""INSERT INTO account_intake_requests(
                    request_key,input_sha256,preparation_key,platform,input_json,source_json,directory_row_id,
                    result_json,created_at,updated_at) VALUES(?,?,?,'douyin','{}','{}',?,?,?,?)""",
                    (key, "a" * 64, key, directory_id, json.dumps({"status": status}), AT, AT)).lastrowid
                connection.execute("INSERT INTO capture_work_items(intake_request_id,state) VALUES(?,'queued')", (request_id,))
            items = [{"directory_row_id": self.ids[0]}]
            annotate_account_preparation(connection, items)
        self.assertEqual(items[0]["account_preparation"]["state"], "blocked")
        self.assertEqual(items[0]["account_preparation"]["reason"], "identity_conflict")

    def test_sql_count_does_not_grow_with_returned_account_count_or_empty_pages(self):
        counts = []
        original = listing.connect
        for size in (1, 100):
            statements = []
            def traced(*args, **kwargs):
                connection = original(*args, **kwargs)
                connection.set_trace_callback(statements.append)
                return connection
            with patch.object(listing, "connect", side_effect=traced):
                self.new(page_size=size, compact=True)
            counts.append(len(statements))
            self.assertLess(len(statements), 70)
        self.assertLessEqual(abs(counts[1] - counts[0]), 2)
        with patch.object(listing, "_account_models", side_effect=AssertionError("empty pages must not build models")):
            self.assertEqual(self.new(page=999)["items"], [])

    def test_list_and_detail_observe_committed_wal_without_changing_database(self):
        with connect(self.path) as anchor:
            before = anchor.total_changes
            anchor.execute("UPDATE account_directory_rows SET nickname='latest WAL name' WHERE id=?", (self.ids[0],))
            anchor.commit()
            modified = anchor.total_changes
            self.assertGreater(modified, before)
            self.assertEqual(listing.account_detail(self.ids[0], db_path=self.path)["platforms"][0]["nickname"], "latest WAL name")
            self.assertEqual(self.new(query="latest WAL name")["total"], 1)
            self.assertEqual(anchor.total_changes, modified)

    def test_identity_consistency_guard_still_fails_closed(self):
        admission = {self.identity_id: {"account_id": self.account_id,
                     "member": {"platform": "douyin", "uid": "wrong-subject"}}}
        with patch.object(listing, "load_admission_members", return_value=admission):
            with self.assertRaisesRegex(OperationError, "平台身份不一致"):
                self.new(page_size=1)
            with self.assertRaisesRegex(OperationError, "平台身份不一致"):
                listing.account_detail(self.ids[0], db_path=self.path)


if __name__ == "__main__":
    unittest.main()
