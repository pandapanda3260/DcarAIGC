"""Offline catalog profile execution through real slots, raw storage and worker.

The installed operation authority is a fixture (covered by operator-release
tests). Catalog eligibility, durable ownership, A/B accounting, provider parsing,
raw receipts, account observations and worker completion are not mocked. The
only HTTP endpoint is an in-memory response and socket connections are forbidden.
"""
from __future__ import annotations

import json
import unittest
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch

from tests import test_v8_catalog_capture_planner as catalog_fixture
from tests import test_v8_provider_transport as http_fixture
from tests import test_v8_transport_campaign as transport_fixture
from v8 import account_metrics, capture, capture_authorizations as auth
from v8 import capture_operator_release, capture_release as release, capture_runtime as runtime
from v8 import provider_budget, providers, raw_archive
from v8.provider_transport import request_json
from v8.storage import connect, transaction

AT = catalog_fixture.AT
OPERATION = "douyin_uid_profile"


class CatalogProfileExecutionTest(unittest.TestCase):
    def setUp(self):
        self.fixture = catalog_fixture.CatalogCapturePlannerTest()
        self.addCleanup(self.fixture.doCleanups)
        with patch.object(capture_operator_release, "authority", return_value=None):
            self.fixture.setUp()
        self.db = self.fixture.db
        self.root = self.fixture.base.root.resolve()
        self.aid, self.iid = self.fixture.aid, self.fixture.iid
        self.calls = []
        self.response = {"code": 200, "data": {"status_code": 0, "data": {
            "id_str": "123456789", "sec_uid": catalog_fixture.SEC,
            "follow_info": {"follower_count": 0},
        }}}
        self.transport = transport_fixture.TransportCampaignTest._transport(self)
        self.enterContext(patch.object(providers, "_freeze_tikhub_transport", return_value=self.transport))
        self.enterContext(patch.object(providers, "_load_key", return_value="offline-fixture-key"))
        self.enterContext(patch.object(providers, "request_json_transport", side_effect=self.http))
        self.enterContext(patch.object(capture, "RAW_ROOT", self.root / "profile-raw"))
        for name in ("capture", "providers", "provider_budget", "account_metrics", "capture_runtime", "durable_runs"):
            self.enterContext(patch(f"v8.{name}.now_utc", return_value=AT))
        # Qualification and installed release evidence are independent tests;
        # leave real member/owner/route/price/slot and physical-send guards intact.
        self.enterContext(patch.object(release, "validate_current_dispatch_control"))
        self.enterContext(patch.object(auth, "current_runtime_bindings", return_value={}))
        self.authorizations = self.enterContext(patch.object(auth, "validate_authorization", side_effect=self.authorize))
        with connect(self.db) as connection, transaction(connection):
            ready = {"provider": "tikhub", "operation": OPERATION, "status": "ready", "reason": "offline authority",
                "evidence_json": "{}", "created_at": AT, "expires_at": "2026-09-05T11:00:00Z"}
            self.ready_id = connection.execute(
                "INSERT INTO provider_readiness_receipts(provider,operation,status,reason,evidence_json,created_at,expires_at,receipt_sha256) VALUES(?,?,?,?,?,?,?,?)",
                (*ready.values(), auth.digest(ready))).lastrowid
            gate = {"provider": "tikhub", "operation": OPERATION, "state": "open", "reason": "offline authority",
                "evidence_json": "{}", "recorded_at": AT}
            self.gate_id = connection.execute(
                "INSERT INTO capture_paid_send_gate_events(provider,operation,state,reason,evidence_json,recorded_at,event_sha256) VALUES(?,?,?,?,?,?,?)",
                (*gate.values(), auth.digest(gate))).lastrowid
        self.plan = self.fixture.plan()
        with connect(self.db) as connection, transaction(connection):
            self.assertTrue(runtime._enqueue(connection, self.plan, self.plan["cohort"][0],
                stage="account_metrics", operation=OPERATION,
                logical_due="account-metrics:" + runtime._bucket(AT, 6 * 3600), at=AT))
            self.work_id = connection.execute("SELECT id FROM capture_work_items WHERE account_id=? AND operation=?",
                (self.aid, OPERATION)).fetchone()[0]

    def authorize(self, connection, **kwargs):
        self.assertEqual(kwargs["operation"], OPERATION)
        self.assertEqual(kwargs["amount_microusd"], provider_budget.PRICES_MICROUSD[OPERATION])
        self.assertEqual(kwargs.get("sequence", 0), 0)
        return {"authority_sha256": "a" * 64, "gate_event_id": self.gate_id,
            "readiness_receipt_id": self.ready_id, "scope_hash": "b" * 64,
            "charge_business_day": provider_budget.budget_day(AT), "budget_bucket": "discovery",
            "total_microusd_before": 0, "provider_calls": 0}

    def http(self, request, **kwargs):
        parsed = urlsplit(request.full_url)
        self.assertEqual(parsed.path, "/api/v1/douyin/web/fetch_user_profile_by_uid")
        self.assertEqual(parse_qs(parsed.query), {"uid": ["123456789"]})
        self.calls.append(request.full_url)
        body = json.dumps(self.response).encode()
        response = http_fixture.FakeResponse(body, response_url=request.full_url,
            headers={"Content-Length": str(len(body))})
        return request_json(request, **kwargs, opener=http_fixture.FakeOpener(response), clock=lambda: AT, chunk_size=13)

    def work(self):
        with connect(self.db) as connection:
            return dict(connection.execute("SELECT * FROM capture_work_items WHERE id=?", (self.work_id,)).fetchone())

    def run_profile(self):
        return runtime._run_single(self.db, AT)

    def test_new_catalog_account_zero_followers_persists_evidence_and_replays_without_http(self):
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM account_roster_members WHERE account_identity_id=?", (self.iid,)).fetchone()[0], 0)
        outcome = self.run_profile()
        self.assertEqual(outcome["status"], "terminal", outcome)
        self.assertTrue(outcome["complete"], outcome)
        self.assertEqual(len(self.calls), 1)
        self.assertIsNotNone(self.work()["completed_at"])
        with connect(self.db) as connection:
            observation = connection.execute("SELECT * FROM account_metric_observations WHERE account_identity_id=?", (self.iid,)).fetchone()
            self.assertIsNotNone(observation)
            fields = json.loads(observation["payload_json"])["fields"]
            self.assertEqual(fields["follower_count"]["value"], 0)
            self.assertEqual(fields["follower_count"]["status"], "provided")
            raw = connection.execute("SELECT * FROM provider_raw_responses WHERE id=?", (observation["raw_response_id"],)).fetchone()
            self.assertEqual((raw["operation"], raw["account_id"], raw["http_status"]), (OPERATION, self.aid, 200))
            self.assertEqual(json.loads(raw_archive.read_response_entity(connection, raw["id"])), self.response)
            reference = connection.execute("SELECT * FROM account_provider_references WHERE account_identity_id=?", (self.iid,)).fetchone()
            self.assertEqual(reference["reference_value"], catalog_fixture.SEC)
            self.assertEqual(reference["source_raw_response_id"], raw["id"])
            self.assertEqual(connection.execute("SELECT count(*) FROM provider_usage WHERE operation=?", (OPERATION,)).fetchone()[0], 1)
            selected = account_metrics.select_account_metrics(connection, [self.iid], cutoff_at=AT)[self.iid]
            self.assertEqual(selected["follower_count"], 0)
            envelope = json.loads(self.work()["envelope_json"])
        self.assertEqual(self.run_profile()["status"], "idle")
        # Local re-application after a completed fetch also reuses the exact raw.
        replay = runtime._account_request(envelope, db_path=self.db, at=AT)
        self.assertTrue(replay["complete"])
        self.assertEqual(replay["provider_cost"], 0)
        self.assertEqual(len(self.calls), 1)
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM account_metric_observations WHERE account_identity_id=?", (self.iid,)).fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT count(*) FROM provider_usage WHERE operation=?", (OPERATION,)).fetchone()[0], 1)

    def test_wrong_uid_does_not_complete_or_materialize_an_observation(self):
        self.response["data"]["data"]["id_str"] = "999999999"
        self.assert_failed_profile("paid_identity_hold:AccountMetricError")

    def test_missing_locator_does_not_complete_or_replace_existing_reference(self):
        del self.response["data"]["data"]["sec_uid"]
        self.assert_failed_profile("paid_identity_hold:invalid_response")

    def test_missing_followers_does_not_claim_refresh_completion(self):
        del self.response["data"]["data"]["follow_info"]["follower_count"]
        self.assert_failed_profile("paid_identity_hold:AccountMetricError")
        self.assert_preserved_paid_raw()

    def test_invalid_followers_does_not_claim_refresh_completion(self):
        self.response["data"]["data"]["follow_info"]["follower_count"] = -1
        self.assert_failed_profile("paid_identity_hold:AccountMetricError")
        self.assert_preserved_paid_raw()

    def assert_preserved_paid_raw(self):
        with connect(self.db) as connection:
            rows = connection.execute("SELECT * FROM provider_raw_responses WHERE account_id=? AND operation=?",
                (self.aid, OPERATION)).fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(json.loads(raw_archive.read_response_entity(connection, rows[0]["id"])), self.response)
            usage = connection.execute("SELECT * FROM provider_usage WHERE operation=?", (OPERATION,)).fetchall()
            self.assertEqual(len(usage), 1)
            self.assertEqual((usage[0]["request_attempts"], usage[0]["billed_requests"], usage[0]["amount"]), (1, 1, 0.001))

    def assert_failed_profile(self, reason):
        outcome = self.run_profile()
        self.assertEqual(len(self.calls), 1, outcome)
        self.assertFalse(outcome.get("complete", False), outcome)
        self.assertEqual((outcome["status"], outcome["reason"]), ("paid_identity_hold", reason))
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM account_metric_observations WHERE account_identity_id=?", (self.iid,)).fetchone()[0], 0)
            reference = connection.execute("SELECT * FROM account_provider_references WHERE account_identity_id=?", (self.iid,)).fetchone()
            self.assertEqual(reference["reference_value"], catalog_fixture.SEC)
            self.assertIsNone(reference["source_raw_response_id"])
        self.run_profile()
        self.assertEqual(len(self.calls), 1)


if __name__ == "__main__":
    unittest.main()
