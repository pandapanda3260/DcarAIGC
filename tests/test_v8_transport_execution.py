from __future__ import annotations

import http.client
import json
import sqlite3
import unittest
import urllib.parse
from pathlib import Path
from unittest.mock import patch

from tests import test_v8_diagnostic_capture_boundary as capture_fixture
from tests import test_v8_provider_transport as transport_fixture
from tests import test_v8_tikhub_scan as scan_fixture
from v8 import durable_runs, metric_observations, paid_drain, providers, tikhub_scan, transport_execution
from v8.provider_transport import request_json
from v8.raw_evidence import read_raw_evidence
from v8.storage import connect, transaction
from v8.transport_authority import current_diagnostic_request_binding
from v8.transport_execution import execute_primary_member_page

AT = capture_fixture.AT


class TransportExecutionTest(unittest.TestCase):
    def setUp(self):
        self.fixture = capture_fixture.DiagnosticCaptureBoundaryTest(methodName="runTest")
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()
        self.db = self.fixture.db
        self.raw_root = self.fixture.fixture.root / "executor-raw"
        self.calls = []
        self.bodies = {}
        self.fail_rank = None
        self.more = False
        self.status = 200
        self.body_override = None
        self.omit_length = False
        for module in (providers, tikhub_scan, transport_execution, metric_observations):
            self.enterContext(patch.object(module, "now_utc", return_value=AT))
        self.enterContext(patch.object(providers, "_load_key", return_value="fixture-no-real-key"))
        self.enterContext(patch.object(providers, "request_json_transport", side_effect=self._http))

    def _http(self, request, **kwargs):
        params = urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)
        candidate = next(item for item in self.fixture.candidates
                         if item["request_identity"].document["subject"] == params["sec_user_id"][0])
        rank = self.fixture.candidates.index(candidate) + 1
        self.calls.append(rank)
        self.assertEqual(params["max_cursor"], ["0"])
        self.assertEqual(urllib.parse.urlsplit(request.full_url).hostname, "api.tikhub.dev")
        item = scan_fixture.dy_item(
            number=rank, author={"uid": candidate["scope"].uid, "nickname": "fixture"},
            create_time=scan_fixture.epoch("2026-09-05T10:00:00Z"),
            statistics={"play_count": 321, "digg_count": 10, "comment_count": 2,
                        "share_count": 1, "collect_count": 3},
        )
        body = json.dumps(self.body_override or scan_fixture.dy_page(
            [item], more=self.more, cursor=123 if self.more else 0,
        ), separators=(",", ":")).encode()
        self.bodies[rank] = body
        response = transport_fixture.FakeResponse(
            body, status=self.status, headers={} if self.omit_length else {"Content-Length": str(len(body))},
            response_url=request.full_url,
            read_error=http.client.IncompleteRead(b'{"data":', len(body)) if rank == self.fail_rank else None,
        )
        return request_json(
            request, **kwargs, opener=transport_fixture.FakeOpener(response),
            clock=lambda: AT, chunk_size=13,
        )

    def _execute(self, rank=1):
        return execute_primary_member_page(
            self.fixture.members[rank - 1]["receipt_id"],
            operator_claim=self.fixture.fixture.operator, scheduler=self.fixture.scheduler,
            db_path=self.db, raw_root=self.raw_root,
        )

    def test_twenty_members_materialize_through_real_parser_without_extra_pages(self):
        results = [self._execute(rank) for rank in range(1, 21)]
        self.assertEqual(self.calls, list(range(1, 21)))
        self.assertIsNone(current_diagnostic_request_binding())
        self.assertTrue(all(row["effective_starts"] == 1 and row["materialized"] for row in results))
        self.assertTrue(all(row["dispatch_terminal"] == "succeeded" for row in results))
        self.assertTrue(all(row["scan"]["complete"] and not row["qualified"] for row in results))
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0], 20)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM content_items").fetchone()[0], 20)
            self.assertEqual(connection.execute(
                "SELECT COUNT(DISTINCT content_id) FROM content_metric_observations "
                "WHERE like_count=10 AND comment_count=2 AND share_count=1 AND collect_count=3",
            ).fetchone()[0], 20)
            self.assertEqual(connection.execute("SELECT SUM(request_attempts) FROM provider_usage").fetchone()[0], 20)
            self.assertAlmostEqual(connection.execute("SELECT SUM(amount) FROM provider_usage").fetchone()[0], .020)
            for row in results:
                raw = connection.execute("SELECT * FROM provider_raw_responses WHERE id=?", (row["raw_response_id"],)).fetchone()
                self.assertEqual(raw["source"], "live_applied")
                self.assertEqual(read_raw_evidence(Path(raw["local_path"])).entity_bytes, self.bodies[row["rank"]])
            self.assertEqual(paid_drain.dispatch_state(connection, at=AT).state, "draining")
        with self.assertRaises(RuntimeError):
            self._execute(1)
        self.assertEqual(len(self.calls), 20)

    def test_corrupt_committed_manifest_is_detected_before_purchase(self):
        candidate = self.fixture.candidates[0]
        child = self.fixture.fixture.children[candidate["scope"].scheduler_run_id]
        with connect(self.db) as connection, transaction(connection):
            durable_runs.checkpoint(connection, child, {
                "last_manifest": {"path": str(self.raw_root / "missing-manifest.json"),
                                  "byte_size": 10, "sha256": "0" * 64},
            }, now=AT)
        result = self._execute()
        self.assertEqual(result["effective_starts"], 0)
        self.assertFalse(result["materialized"])
        self.assertEqual(result["scan"]["reason"], "manifest_integrity_error")
        self.assertEqual(self.calls, [])
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0], 0)

    def test_noncanonical_cursor_is_rejected_before_purchase(self):
        candidate = self.fixture.candidates[0]
        child = self.fixture.fixture.children[candidate["scope"].scheduler_run_id]
        with connect(self.db) as connection, transaction(connection):
            durable_runs.checkpoint(connection, child, {"cursor": "000"}, now=AT)
        result = self._execute()
        self.assertEqual(result["scan"]["reason"], "diagnostic_cursor_not_canonical")
        self.assertEqual(result["effective_starts"], 0)
        self.assertEqual(self.calls, [])
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0], 0)

    def test_more_pages_yields_after_one_page_without_advancing_campaign_scope(self):
        self.more = True
        result = self._execute()
        self.assertEqual(self.calls, [1])
        self.assertTrue(result["materialized"])
        self.assertFalse(result["scan"]["complete"])
        self.assertEqual(result["scan"]["status"], "partial")
        run = durable_runs.get_run(result["scan"]["scheduler_run_id"], db_path=self.db)
        self.assertEqual(run["details"]["checkpoint"]["cursor"], 123)
        self.assertIsNone(run["details"]["checkpoint"]["pending_materialization"])

    def test_eof_complete_without_content_length_preserves_transport_contract(self):
        self.omit_length = True
        result = self._execute()
        self.assertTrue(result["response_complete"])
        self.assertTrue(result["materialized"])
        self.assertIsNone(result["transport_receipt"]["length_match"])

    def test_truncated_member_remains_failed_and_next_rank_is_not_replacement(self):
        self.fail_rank = 1
        first = self._execute()
        self.assertEqual(first["dispatch_terminal"], "billing_unknown")
        self.assertEqual(first["effective_starts"], 1)
        self.assertFalse(first["materialized"])
        self.assertFalse(first["response_complete"])
        self.assertFalse(first["qualified"])
        self.assertIsNone(first["raw_response_id"])
        run = durable_runs.get_run(first["scan"]["scheduler_run_id"], db_path=self.db)
        self.assertEqual(run["details"]["checkpoint"]["cursor"], 0)
        second = self._execute(2)
        self.assertEqual(self.calls, [1, 2])
        self.assertEqual(second["dispatch_terminal"], "succeeded")
        self.assertTrue(second["materialized"])
        with connect(self.db) as connection:
            rows = connection.execute("SELECT details_json FROM provider_usage ORDER BY id").fetchall()
            self.assertEqual(len(rows), 2)
            failed = json.loads(rows[0][0])
            self.assertEqual(failed["diagnostic_member"]["rank"], 1)
            receipt = failed["transport"]
            self.assertEqual(receipt["partial_bytes"], 8)
            self.assertTrue(receipt["quarantine_receipt_sha256"])

    def test_local_materialization_failure_retains_raw_and_does_not_repurchase(self):
        with patch.object(providers, "materialize_account_discovery_page",
                          side_effect=sqlite3.OperationalError("fixture local write failure")):
            result = self._execute()
        self.assertEqual(result["dispatch_terminal"], "succeeded")
        self.assertFalse(result["materialized"])
        self.assertFalse(result["scan"]["complete"])
        run = durable_runs.get_run(result["scan"]["scheduler_run_id"], db_path=self.db)
        self.assertIsNotNone(run["details"]["checkpoint"]["pending_materialization"])
        with self.assertRaises(RuntimeError):
            self._execute()
        self.assertEqual(self.calls, [1])

    def test_full_balance_error_is_retained_and_blocks_following_member(self):
        self.status = 402
        self.body_override = {"code": 402, "message": "insufficient balance"}
        first = self._execute()
        self.assertEqual(first["effective_starts"], 1)
        self.assertEqual(first["dispatch_terminal"], "failed")
        self.assertFalse(first["materialized"])
        self.assertTrue(first["response_complete"])
        self.assertIsNotNone(first["raw_response_id"])
        self.status = 200
        self.body_override = None
        second = self._execute(2)
        self.assertEqual(second["effective_starts"], 0)
        self.assertEqual(second["dispatch_terminal"], "not_reserved")
        self.assertEqual(self.calls, [1])
        with connect(self.db) as connection:
            row = connection.execute("SELECT * FROM provider_raw_responses WHERE http_status=402").fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(first["raw_response_id"], row["id"])
            self.assertEqual(read_raw_evidence(Path(row["local_path"])).entity_bytes, self.bodies[1])
