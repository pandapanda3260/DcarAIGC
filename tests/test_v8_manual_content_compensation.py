"""Real isolated Writer/ledger grants with fixture-only provider responses."""
from __future__ import annotations

import gzip
import json
import unittest
import urllib.request
from unittest.mock import patch

from tests import test_v8_manual_content_scope as fixture
from v8 import capture, capture_commands, capture_compensation, capture_planning
from v8 import capture_runtime, provider_budget, providers, usage_settlements
from v8.provider_transport import request_json
from v8.storage import connect, transaction


class ManualContentCompensationTest(unittest.TestCase):
    def setUp(self):
        self.base = fixture.ManualContentScopeTest()
        self.base.setUp()
        self.addCleanup(self.base.doCleanups)
        self.db, self.cid = self.base.db, self.base.cid
        self.enterContext(patch.object(capture_runtime, "now_utc", return_value=fixture.AT))

    def create_failure(self, *, unknown=False, billed=False, gzip_no_length=False):
        def response(group, content):
            self.base.calls.append(group)
            if unknown:
                raise capture.CaptureError("fixture incomplete response", retryable=True,
                    error_code="transport_error", billed=None)
            raw = ({"code": 200, "data": {"statistics_list": [{"aweme_id": fixture.PID}]}}
                   if billed else {"detail": {"code": 400, "message": "Fixture request failed; no charge"}})
            entity = json.dumps(raw).encode()
            body = gzip.compress(entity) if gzip_no_length else entity
            headers = {"Content-Encoding": "gzip"} if gzip_no_length else {"Content-Length": str(len(entity))}
            url = "https://fixture.invalid/metrics"
            received = request_json(urllib.request.Request(url), route_id="fixture-route",
                route_generation="fixture-generation", timeout=45,
                opener=fixture.transport_fixture.FakeOpener(fixture.transport_fixture.FakeResponse(
                    body, status=200 if billed else 400, headers=headers, response_url=url)),
                clock=lambda: fixture.AT)
            raise capture.CaptureError("fixture failed statistics response", retryable=True,
                error_code="invalid_response" if billed else "provider_retry_requested",
                http_status=200 if billed else 400, billed=billed,
                raw_response=received.payload, entity_bytes=received.entity_body,
                transport_receipt=received.receipt)

        with self.assertRaises(capture.CaptureError):
            self.base.refresh(allowed_groups=["statistics"], call_override=response)
        capture_commands.process_commands(db_path=self.db, at=fixture.AT)
        with connect(self.db) as connection, transaction(connection):
            work = connection.execute("SELECT * FROM capture_work_items WHERE content_id=? AND operation=?",
                (self.cid, fixture.OP)).fetchone()
            self.assertEqual(work["state"], "paid_identity_hold" if unknown else "runnable")
            self.work_id = int(work["id"])
            usage = connection.execute("SELECT * FROM provider_usage ORDER BY id DESC LIMIT 1").fetchone()
            self.details = json.loads(usage["details_json"])
            self.settlement = usage_settlements.record_settlement(connection,
                usage_id=usage["id"], at=fixture.AT)
            self.before_work = dict(work)

    def authorize(self):
        with connect(self.db) as connection, transaction(connection):
            grants = {}
            identities = {"request": self.details["paid_scope_identity"],
                "member": usage_settlements.member_identity(self.details["paid_identity"])}
            for kind, identity in identities.items():
                grants[kind] = usage_settlements.authorize_compensation(connection,
                    authorization_key="fixture-manual-retry:" + kind,
                    settlement_id=self.settlement["id"], identity=identity, scope_kind=kind,
                    owner="isolated-test", reason="fixture explicit unbilled retry",
                    gap_evidence_ref="provider-usage:" + str(self.settlement["provider_usage_id"]),
                    raw_unrecoverable_reason="complete fixture error body has no requested statistics",
                    local_replay_exhausted=True, business_gap_due=True, max_amount_microunits=1000,
                    expires_at="2026-09-05T11:00:00Z", at=fixture.AT,
                    provider_ready=True, budget_available=True)
            self.grants = grants
            return capture_compensation.enqueue_authorized_compensation(connection,
                work_id=self.work_id, request_issuance_id=grants["request"]["issuance_id"],
                member_issuance_id=grants["member"]["issuance_id"], at=fixture.AT)

    def test_unbilled_manual_retry_uses_original_scope_once_without_auto_route(self):
        before = self.base.snapshot()
        self.create_failure()
        result = self.authorize()
        self.assertEqual(result["sequence"], 1)
        with connect(self.db) as connection:
            work = dict(connection.execute("SELECT * FROM capture_work_items WHERE id=?", (self.work_id,)).fetchone())
            self.assertEqual(work["work_identity"], self.before_work["work_identity"])
            envelope = json.loads(work["envelope_json"])
            self.assertEqual(envelope["logical_due"], self.details["paid_identity"]["due_bucket"])
            self.assertIsNone(capture_planning.resolve_route(connection,
                account_id=work["account_id"], content_id=self.cid, operation=fixture.OP, at=fixture.AT))
        with patch.object(providers, "_load_key", return_value="fixture-key"), \
             patch.object(providers, "_freeze_tikhub_transport", return_value=None), \
             patch.object(providers, "_douyin_call", side_effect=lambda *a, **k: self.base.response("statistics", {"platform_content_id": fixture.PID})):
            result = capture_compensation.run_authorized_work(self.db, self.work_id, fixture.AT)
        self.assertEqual(result["status"], "terminal", result)
        self.assertEqual(self.base.calls, ["statistics", "statistics"])
        self.assertEqual(self.base.snapshot(), before)
        with connect(self.db) as connection:
            usages = connection.execute("SELECT details_json FROM provider_usage ORDER BY id").fetchall()
            identities = [json.loads(row[0]) for row in usages]
            self.assertEqual([row["paid_sequence"] for row in identities], [0, 1])
            self.assertEqual(identities[0]["paid_identity"], identities[1]["paid_identity"])
            self.assertEqual(connection.execute("SELECT count(*) FROM authorization_issuance_consumptions").fetchone()[0], 2)
        again = capture_compensation.run_authorized_work(self.db, self.work_id, fixture.AT)
        self.assertEqual(again["status"], "idle")
        self.assertEqual(len(self.base.calls), 2)

    def test_compensation_rechecks_manual_identity_budget_and_assignment(self):
        self.create_failure()
        self.authorize()
        with connect(self.db) as connection, transaction(connection):
            original = connection.execute("SELECT envelope_json FROM capture_work_items WHERE id=?", (self.work_id,)).fetchone()[0]
            envelope = json.loads(original)
            for key, value in (("task_max_amount", 99), ("manual_command_run_id", 999999)):
                changed = {**envelope, key: value}
                connection.execute("UPDATE capture_work_items SET envelope_json=? WHERE id=?", (json.dumps(changed), self.work_id))
                with self.assertRaises((ValueError, RuntimeError)):
                    capture_compensation._validate_proof(connection, self.work_id, at=fixture.AT)
            changed = dict(envelope)
            changed.pop("manual_command_run_id")
            connection.execute("UPDATE capture_work_items SET envelope_json=? WHERE id=?", (json.dumps(changed), self.work_id))
            with self.assertRaisesRegex(provider_budget.PaidScopeBlocked, "assigned route"):
                capture_compensation._validate_proof(connection, self.work_id, at=fixture.AT)
            connection.execute("UPDATE capture_work_items SET envelope_json=? WHERE id=?", (original, self.work_id))
        self.assertEqual(len(self.base.calls), 1)

    def test_unknown_hold_is_not_released_by_manual_command_or_compensation_context(self):
        self.create_failure(unknown=True)
        with connect(self.db) as connection, transaction(connection):
            with self.assertRaises(provider_budget.PaidScopeBlocked):
                capture_compensation.enqueue_authorized_compensation(connection, work_id=self.work_id,
                    request_issuance_id=999998, member_issuance_id=999999, at=fixture.AT)
        with self.assertRaises(Exception):
            self.base.refresh(allowed_groups=["statistics"])
        self.assertEqual(len(self.base.calls), 1)

    def test_known_billed_empty_response_keeps_original_charge_and_requires_grants(self):
        self.create_failure(billed=True)
        original = dict(self.settlement)
        self.authorize()
        with connect(self.db) as connection, transaction(connection):
            self.assertEqual(usage_settlements.read_settlement(connection, original["id"]), original)
            self.assertEqual(original["amount_microunits"], 1000)
            proof = capture_compensation._validate_proof(connection, self.work_id, at=fixture.AT)
            self.assertEqual(proof["sequence"], 1)
        self.assertEqual(len(self.base.calls), 1)

    def test_complete_gzip_without_length_accepts_but_explicit_integrity_failures_reject(self):
        self.create_failure(billed=True, gzip_no_length=True)
        transport = self.details["transport"]
        self.assertIsNone(transport["content_length"])
        self.assertIsNone(transport["length_match"])
        self.assertTrue(transport["gzip_crc_ok"])
        self.assertTrue(transport["clean_eof"])
        usage_id = self.settlement["provider_usage_id"]
        with connect(self.db) as connection:
            original = connection.execute("SELECT details_json FROM provider_usage WHERE id=?", (usage_id,)).fetchone()[0]
        for field in ("length_match", "gzip_crc_ok"):
            with self.subTest(field=field):
                changed = json.loads(original)
                changed["transport"][field] = False
                with connect(self.db) as connection, transaction(connection):
                    connection.execute("UPDATE provider_usage SET details_json=? WHERE id=?", (json.dumps(changed), usage_id))
                try:
                    with self.assertRaisesRegex(provider_budget.PaidScopeBlocked, "complete known-billing failure"):
                        self.authorize()
                finally:
                    with connect(self.db) as connection, transaction(connection):
                        connection.execute("UPDATE provider_usage SET details_json=? WHERE id=?", (original, usage_id))
        self.authorize()
        with patch.object(providers, "_load_key", return_value="fixture-key"), \
             patch.object(providers, "_freeze_tikhub_transport", return_value=None), \
             patch.object(providers, "_douyin_call", side_effect=lambda *a, **k: self.base.response("statistics", {"platform_content_id": fixture.PID})):
            result = capture_compensation.run_authorized_work(self.db, self.work_id, fixture.AT)
        self.assertEqual(result["status"], "terminal", result)
        with connect(self.db) as connection:
            usages = [json.loads(row[0]) for row in connection.execute("SELECT details_json FROM provider_usage ORDER BY id")]
            self.assertEqual([row["paid_sequence"] for row in usages], [0, 1])
            self.assertEqual(usages[0]["paid_identity"], usages[1]["paid_identity"])
            self.assertEqual(connection.execute("SELECT count(*) FROM authorization_issuance_consumptions").fetchone()[0], 2)
        self.assertEqual(len(self.base.calls), 2)


if __name__ == "__main__":
    unittest.main()
