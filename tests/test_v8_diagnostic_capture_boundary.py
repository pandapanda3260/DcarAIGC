from __future__ import annotations

import json
import unittest
import urllib.request
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore[import-untyped]

from tests import test_v8_provider_transport as transport_fixtures
from tests import test_v8_transport_members as member_fixtures
from v8 import capture, durable_runs, paid_drain
from v8.paid_dispatch import dispatch_events
from v8.provider_budget import PaidScopeBlocked, paid_scope
from v8.provider_transport import request_json
from v8.raw_evidence import read_raw_evidence
from v8.runtime_database import acquire_writer_lock
from v8.storage import connect, transaction
from v8.transport_authority import (
    current_diagnostic_request_binding,
    diagnostic_dispatch_binding,
    diagnostic_request_context,
)
from v8.transport_members import DiagnosticMemberError


AT = member_fixtures.AT
URL = "https://api.tikhub.dev/api/v1/douyin/app/v3/fetch_user_post_videos"


class DiagnosticCaptureBoundaryTest(unittest.TestCase):
    def setUp(self):
        self.fixture = member_fixtures.TransportMembersTest(methodName="runTest")
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()
        self.db = self.fixture.db
        self.candidates = self.fixture._prepare()
        self.enterContext(acquire_writer_lock(self.fixture.writer_access))
        with connect(self.db) as connection, transaction(connection):
            self.members = self.fixture._issue(connection)
            self.drain_event_count = connection.execute(
                "SELECT COUNT(*) FROM pipeline_paid_drain_events"
            ).fetchone()[0]
            self.schema_objects = list(
                connection.execute(
                    "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
                )
            )
            connection.execute(
                "INSERT INTO provider_budget_batches(id,purpose,provider,operation,currency,verified_unit_price,"
                "max_billable_requests,max_amount,pilot_size,daily_quota,price_verified_at,status,created_at,updated_at) "
                "VALUES ('diagnostic-test','diagnostic-test','TikHub','douyin_user_posts','USD',0.001,"
                "100,0.100,0,100,?,'approved',?,?)",
                (AT, AT, AT),
            )
        self.now = AT
        self.enterContext(
            patch.object(capture, "now_utc", side_effect=lambda: self.now)
        )
        self.scheduler = BackgroundScheduler(timezone="UTC")
        self.scheduler.start(paused=True)
        self.addCleanup(self.scheduler.shutdown, wait=False)
        self.calls = 0
        self.entity = b'{"data":{"aweme_list":[],"has_more":false,"max_cursor":0}}'

    def _response(self):
        self.calls += 1
        manifest = self.fixture.transport["manifest"]
        transport = request_json(
            urllib.request.Request(URL),
            route_id=manifest["transport_route_id"],
            route_generation=manifest["route_generation"],
            timeout=45,
            opener=transport_fixtures.FakeOpener(
                transport_fixtures.FakeResponse(
                    self.entity,
                    headers={"Content-Length": str(len(self.entity))},
                    response_url=URL,
                )
            ),
            clock=lambda: self.now,
            chunk_size=7,
        )
        return capture.ProviderResult(
            data=transport.payload,
            raw_response=transport.payload,
            http_status=transport.status,
            billed=True,
            entity_bytes=transport.entity_body,
            transport_receipt=transport.receipt,
        )

    def _fetch(self, rank=1):
        candidate = self.candidates[rank - 1]
        scope = candidate["scope"]
        with paid_scope(
            "reconcile",
            activation_id=scope.activation_id,
            roster_snapshot_id=scope.roster_snapshot_id,
            roster_snapshot_hash=scope.roster_snapshot_hash,
            scheduler_run_id=scope.scheduler_run_id,
            scheduler_attempt_id=scope.scheduler_attempt_id,
            business_day=scope.business_day,
        ):
            return capture.execute_account_fetch(
                account_id=scope.account_id,
                stage="discovery",
                window_key=candidate["request_identity"].document["due_bucket"],
                provider="TikHub",
                adapter_version="fixture",
                operation="douyin_user_posts",
                call=self._response,
                db_path=self.db,
                raw_root=self.fixture.root / "capture-raw",
                budget_id="diagnostic-test",
                paid_request_identity=candidate["request_identity"],
                request_transport=self.fixture.transport,
            )

    def _context(self, rank=1, *, receipt_id=None):
        return diagnostic_request_context(
            self.members[rank - 1]["receipt_id"] if receipt_id is None else receipt_id,
            self.fixture.operator,
            scheduler=self.scheduler,
        )

    @contextmanager
    def _after_network_wait(self, action):
        semaphore = capture.TIKHUB_NETWORK_SLOTS

        @contextmanager
        def queued():
            with semaphore:
                action()
                yield

        with patch.object(capture, "TIKHUB_NETWORK_SLOTS", queued()):
            yield

    def _assert_not_sent(self):
        self.assertEqual(self.calls, 0)
        self.assertIsNone(current_diagnostic_request_binding())
        with connect(self.db) as connection:
            usage = connection.execute("SELECT * FROM provider_usage").fetchall()
            self.assertEqual(len(usage), 1)
            self.assertEqual(usage[0]["request_attempts"], 0)
            self.assertEqual(usage[0]["amount"], 0)
            self.assertEqual(json.loads(usage[0]["details_json"])["state"], "not_sent")
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM fetch_attempts").fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT event_type FROM paid_provider_dispatch_events ORDER BY id"
                ).fetchall()[1][0],
                "not_sent",
            )
            self.assertEqual(
                connection.execute(
                    "SELECT consumed_requests FROM provider_budget_batches WHERE id='diagnostic-test'"
                ).fetchone()[0],
                0,
            )
        self.assertFalse((self.db.parent / "paid_send_claims").exists())

    def test_rank_one_binds_usage_dispatch_send_claim_and_exact_raw_once(self):
        with self._context():
            outcome = self._fetch()
        self.assertIsNone(current_diagnostic_request_binding())
        expected = diagnostic_dispatch_binding(self.members[0])
        with connect(self.db) as connection:
            usage = connection.execute("SELECT * FROM provider_usage").fetchone()
            metadata = json.loads(usage["details_json"])
            self.assertEqual(usage["request_attempts"], 1)
            self.assertEqual(usage["amount"], 0.001)
            self.assertEqual(metadata["diagnostic_member"], expected)
            self.assertEqual(
                metadata["paid_identity"],
                self.candidates[0]["request_identity"].document,
            )
            dispatch_id = connection.execute(
                "SELECT dispatch_id FROM paid_provider_dispatch_events LIMIT 1"
            ).fetchone()[0]
            events = dispatch_events(connection, dispatch_id)
            self.assertEqual(
                [event.event_type for event in events],
                ["reserved", "send_marked", "succeeded"],
            )
            self.assertTrue(
                all(event.scope["diagnostic_member"] == expected for event in events)
            )
            self.assertEqual(
                events[0].permit_event_id, expected["legacy_release_anchor"]["event_id"]
            )
            self.assertNotEqual(events[0].permit_event_id, expected["start_event_id"])
            self.assertEqual(
                expected["legacy_release_anchor"]["role"], "historical_lineage_only"
            )
            self.assertEqual(events[-1].raw_response_id, outcome.raw_response_id)
            self.assertEqual(
                paid_drain.dispatch_state(connection, at=AT).state, "draining"
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM pipeline_paid_drain_events"
                ).fetchone()[0],
                self.drain_event_count,
            )
            self.assertEqual(
                list(
                    connection.execute(
                        "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
                    )
                ),
                self.schema_objects,
            )
            raw = connection.execute(
                "SELECT * FROM provider_raw_responses WHERE id=?",
                (outcome.raw_response_id,),
            ).fetchone()
            self.assertEqual(
                read_raw_evidence(Path(raw["local_path"])).entity_bytes, self.entity
            )
            send_claim = json.loads(Path(metadata["paid_send_claim_path"]).read_bytes())
            self.assertEqual(send_claim["claim"]["diagnostic_member"], expected)
        with self._context(), self.assertRaises(capture.SlotUnavailable):
            self._fetch()
        self.assertEqual(self.calls, 1)
        self.assertIsNone(current_diagnostic_request_binding())
        with self.assertRaises(paid_drain.PaidDrainBlocked):
            self._fetch()

    def test_ordinary_hold_missing_wrong_and_early_rank_permits_send_nothing(self):
        with self.assertRaises(paid_drain.PaidDrainBlocked):
            self._fetch()
        with self.assertRaises(DiagnosticMemberError):
            with self._context(receipt_id=0):
                self._fetch()
        with (
            self._context(receipt_id=self.fixture.campaign["receipt_id"]),
            self.assertRaises(DiagnosticMemberError) as wrong,
        ):
            self._fetch()
        self.assertEqual(wrong.exception.error_code, "provider_transport_blocked")
        with self._context(rank=2), self.assertRaises(DiagnosticMemberError) as early:
            self._fetch(rank=2)
        self.assertEqual(early.exception.code, "diagnostic_rank_not_due")
        self.assertEqual(self.calls, 0)
        self.assertIsNone(current_diagnostic_request_binding())
        with connect(self.db) as connection:
            for table in (
                "provider_usage",
                "fetch_attempts",
                "paid_provider_dispatch_events",
            ):
                self.assertEqual(
                    connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], 0
                )

    def test_scheduler_resumed_while_waiting_releases_reservation_without_send(self):
        with (
            self._context(),
            self._after_network_wait(self.scheduler.resume),
            self.assertRaises(DiagnosticMemberError) as caught,
        ):
            self._fetch()
        self.assertEqual(caught.exception.error_code, "provider_transport_blocked")
        self._assert_not_sent()

    def test_expiry_while_waiting_releases_reservation_without_send(self):
        def expire():
            self.now = "2026-09-07T06:00:00Z"

        with (
            self._context(),
            self._after_network_wait(expire),
            self.assertRaises(DiagnosticMemberError) as caught,
        ):
            self._fetch()
        self.assertEqual(caught.exception.error_code, "provider_transport_blocked")
        self._assert_not_sent()

    def test_source_owner_lost_while_waiting_releases_reservation_without_send(self):
        def lose_owner():
            child = self.fixture.children[self.candidates[0]["scope"].scheduler_run_id]
            durable_runs.finish_run(
                child, status="partial", db_path=self.db, now=AT, next_resume_at=AT
            )

        with (
            self._context(),
            self._after_network_wait(lose_owner),
            self.assertRaises(DiagnosticMemberError) as caught,
        ):
            self._fetch()
        self.assertEqual(caught.exception.error_code, "provider_transport_blocked")
        self._assert_not_sent()

    def test_reserved_member_tamper_is_rejected_before_send_marker(self):
        def tamper():
            with connect(self.db) as connection, transaction(connection):
                usage = connection.execute(
                    "SELECT id,details_json FROM provider_usage"
                ).fetchone()
                metadata = json.loads(usage["details_json"])
                metadata["diagnostic_member"]["rank"] = 2
                connection.execute(
                    "UPDATE provider_usage SET details_json=? WHERE id=?",
                    (json.dumps(metadata), usage["id"]),
                )

        with (
            self._context(),
            self._after_network_wait(tamper),
            self.assertRaises(PaidScopeBlocked) as caught,
        ):
            self._fetch()
        self.assertEqual(caught.exception.error_code, "provider_transport_blocked")
        self._assert_not_sent()
