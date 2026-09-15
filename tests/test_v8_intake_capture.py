"""Temporary-DB target/raw tests; do not simulate successful live admission."""
from __future__ import annotations

from contextlib import nullcontext
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import patch

from v8 import capture, capture_singletons, account_preparation, platform_adapters
from v8.paid_identity import build_paid_request_identity
from v8.provider_budget import PaidScope, PaidScopeBlocked, task_budget_id
from v8.provider_transport import JsonTransportResult
from v8.storage import connect, initialize_database, transaction

AT = "2026-09-12T00:00:00Z"


class IntakeCaptureTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.db = self.root / "fixture.sqlite3"
        self.raw = self.root / "raw"
        self.connection = connect(self.db)
        self.addCleanup(self.connection.close)
        initialize_database(self.connection, target_version=22)
        self.enterContext(patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden")))
        c = self.connection
        for key in ("one", "two"):
            c.execute("INSERT INTO account_intake_requests(request_key,input_sha256,preparation_key,platform,input_json,source_json,created_at,updated_at) VALUES (?,?,'prep','douyin','{}','{}',?,?)", (key, "a" * 64, AT, AT))
        self.slot = capture.ensure_intake_slot(c, intake_request_id=1, stage="profile_prepare", window_key="prepare-fixture", provider="TikHub", adapter_version="fixture")
        self.attempt = c.execute("INSERT INTO fetch_attempts(slot_id,attempt_number,request_started_at,http_status,billed) VALUES (?,1,?,200,0)", (self.slot, AT)).lastrowid
        c.execute("UPDATE fetch_slots SET status='succeeded',attempt_count=1 WHERE id=?", (self.slot,))
        c.commit()
        self.request = build_paid_request_identity(provider="TikHub", operation="douyin_uid_profile", platform="douyin", subject="00012345", request_parameters={"uid": "00012345"}, cursor=None, due_bucket="prepare-fixture")
        self.claim = capture.SlotClaim(slot_id=self.slot, attempt_id=self.attempt, attempt_number=1,
            content_id=None, account_id=None, intake_request_id=1, stage="profile_prepare", window_key="prepare-fixture",
            provider="TikHub", adapter_version="fixture", paid_scope_identity=self.request.scope_identity)
        self.entity = b'{ "uid": "00012345", "nickname": "fixture", "data": [1, 2] }\n'

    def store(self, claim=None):
        return capture._store_raw_response(self.connection, claim=claim or self.claim,
            operation="douyin_uid_profile", value=json.loads(self.entity), http_status=200,
            raw_root=self.raw, entity_bytes=self.entity)

    def test_complete_entity_index_and_raw_keep_intake_target_and_replay_without_writes(self):
        with transaction(self.connection):
            raw_id = self.store()
        row = self.connection.execute("SELECT * FROM provider_raw_responses WHERE id=?", (raw_id,)).fetchone()
        self.assertEqual((row["intake_request_id"], row["account_id"], row["content_id"]), (1, None, None))
        self.assertEqual(capture.raw_archive.read_response_entity(self.connection, raw_id), self.entity)
        index_path = next(self.raw.rglob("*.response.json"))
        index = json.loads(index_path.read_text())
        self.assertEqual(index["intake_request_id"], 1)
        self.assertIn("intake-1", str(index_path))
        # Completing the directory binding must not rewrite original HTTP target.
        with transaction(self.connection):
            self.connection.execute("INSERT INTO accounts(phone,created_at,updated_at) VALUES ('',?,?)", (AT, AT))
            self.connection.execute("UPDATE account_intake_requests SET account_id=1 WHERE id=1")
        with transaction(self.connection):
            replay = capture.replay_schema20_response_index(self.connection, index_path=index_path, raw_root=self.raw)
        self.assertEqual(replay, raw_id)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM provider_raw_responses").fetchone()[0], 1)
        stored = capture.load_succeeded_raw_response(intake_request_id=1, stage="profile_prepare",
            window_key="prepare-fixture", operation="douyin_uid_profile", db_path=self.db)
        self.assertEqual(stored.raw_response_id, raw_id)
        self.assertEqual(stored.value, json.loads(self.entity))

    def test_crash_after_file_save_rebuilds_only_local_index_and_blob_registration(self):
        self.connection.execute("BEGIN")
        self.store()
        self.connection.rollback()
        self.assertEqual(self.connection.execute("SELECT count(*) FROM provider_raw_responses").fetchone()[0], 0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM provider_raw_blobs").fetchone()[0], 0)
        index = next(self.raw.rglob("*.response.json"))
        with transaction(self.connection):
            raw_id = capture.replay_schema20_response_index(self.connection, index_path=index, raw_root=self.raw)
        self.assertEqual(capture.raw_archive.read_response_entity(self.connection, raw_id), self.entity)
        self.assertEqual(self.connection.execute("SELECT billed FROM fetch_attempts").fetchone()[0], 0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM provider_usage").fetchone()[0], 0)

    def test_changed_intake_target_and_invented_account_cannot_reuse_raw_identity(self):
        with transaction(self.connection):
            self.store()
        for changed in (replace(self.claim, intake_request_id=2), replace(self.claim, account_id=1)):
            with self.subTest(claim=changed), self.assertRaises(capture.RawEvidenceError):
                with transaction(self.connection):
                    self.store(changed)
        index = next(self.raw.rglob("*.response.json"))
        with transaction(self.connection):
            self.connection.execute("UPDATE fetch_slots SET intake_request_id=2 WHERE id=?", (self.slot,))
        with self.assertRaisesRegex(capture.RawEvidenceError, "attempt is missing or changed"):
            with transaction(self.connection):
                capture.replay_schema20_response_index(self.connection, index_path=index, raw_root=self.raw)

    def test_intake_slots_are_idempotent_and_cannot_borrow_content_stage(self):
        with transaction(self.connection):
            repeated = capture.ensure_intake_slot(self.connection, intake_request_id=1, stage="profile_prepare",
                window_key="prepare-fixture", provider="TikHub", adapter_version="changed-does-not-repurchase")
        self.assertEqual(repeated, self.slot)
        for stage in ("discovery", "detail", "metrics"):
            with self.assertRaises(ValueError):
                capture.ensure_intake_slot(self.connection, intake_request_id=1, stage=stage,
                    window_key="other", provider="TikHub", adapter_version="fixture")
        with self.assertRaises(ValueError):
            capture.load_succeeded_raw_response(intake_request_id=1, account_id=1, stage="profile_prepare", window_key="prepare-fixture", db_path=self.db)

    def singleton(self):
        c = self.connection
        assignment = c.execute("INSERT INTO capture_route_assignments(scope_type,scope_key,provider,operation,intake_request_id,generation,route,mode,effective_at,recorded_at,assignment_sha256) VALUES ('intake','1','tikhub','douyin_uid_profile',1,1,'integrated','active',?,?,?)", (AT, AT, "b" * 64)).lastrowid
        scope = PaidScope(intake_request_id=1, preparation_subject="00012345")
        with patch("v8.capture_planning.require_send_route", return_value={"id": assignment}):
            batch, actual_assignment = capture_singletons.freeze(c, request=self.request, scope=scope, at=AT)
            self.assertEqual(actual_assignment, assignment)
        return batch, assignment, scope

    def test_singleton_freezes_exact_intake_without_account_or_content(self):
        with transaction(self.connection):
            batch, assignment, scope = self.singleton()
            member = self.connection.execute("SELECT * FROM fetch_request_batch_members WHERE batch_id=?", (batch,)).fetchone()
            self.assertEqual((member["intake_request_id"], member["account_id"], member["content_id"]), (1, None, None))
            capture_singletons.validate_raw_target(self.connection, batch_id=batch, intake_request_id=1, account_id=None, content_id=None)
            with self.assertRaises(PaidScopeBlocked):
                capture_singletons.validate_raw_target(self.connection, batch_id=batch, intake_request_id=2, account_id=None, content_id=None)
            with patch("v8.capture_planning.require_send_route", return_value={"id": assignment}):
                with self.assertRaises(PaidScopeBlocked):
                    capture_singletons.validate(self.connection, batch_id=batch, request=self.request,
                        assignment_id=assignment, scope=replace(scope, intake_request_id=2), at=AT)
            with patch("v8.capture_planning.require_send_route", side_effect=PaidScopeBlocked("route_changed", "fixture rejection")):
                with self.assertRaises(PaidScopeBlocked):
                    capture_singletons.validate(self.connection, batch_id=batch, request=self.request,
                        assignment_id=assignment, scope=scope, at=AT)

    def test_public_executor_passes_exact_target_through_paid_claim_and_stops_on_rejection(self):
        observed = []
        def claim(**kwargs):
            observed.append(kwargs)
            raise PaidScopeBlocked("preparation_scope_changed", "fixture gate rejected")
        with patch.object(capture, "paid_dispatch_owner", return_value=nullcontext()), \
             patch.object(capture, "_claim_paid_tikhub", side_effect=claim), \
             self.assertRaises(PaidScopeBlocked):
            capture.execute_intake_fetch(intake_request_id=1, window_key="prepare-fixture", provider="TikHub",
                adapter_version="fixture", operation="douyin_uid_profile", call=lambda: self.fail("provider must not run"),
                paid_request_identity=self.request, request_transport={}, db_path=self.db,
                budget_id=task_budget_id("fixture", "TikHub", "douyin_uid_profile"), task_id="fixture", task_max_amount=1)
        self.assertEqual(len(observed), 1)
        self.assertEqual((observed[0]["content_id"], observed[0]["account_id"], observed[0]["intake_request_id"]), (None, None, 1))
        self.assertEqual(observed[0]["stage"], "profile_prepare")
        self.assertIs(observed[0]["paid_request_identity"], self.request)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM provider_usage").fetchone()[0], 0)

    def response(self, payload):
        entity = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        checksum = hashlib.sha256(entity).hexdigest()
        receipt = {"contract_version": "provider-json-transport-v1", "status": "succeeded", "error_code": None,
            "transport_route_id": "fixture-route-v1", "route_generation": "route-config-sha256:fixture",
            "http_stack": "fixture-stream-v1", "request_host": "fixture.invalid", "http_status": 200,
            "content_encoding": "identity", "content_length": len(entity), "clean_eof": True,
            "length_match": True, "gzip_crc_ok": None, "json_parse_ok": True, "entity_bytes": len(entity),
            "entity_sha256": checksum, "zero_body": False, "request_started_at": AT,
            "response_finished_at": AT, "http_encoded_bytes": len(entity), "http_encoded_sha256": checksum}
        return JsonTransportResult(200, payload, entity, entity, receipt)

    def test_http200_business_failure_keeps_complete_raw_and_retryable_slot(self):
        response = self.response({"code": 500, "data": {"message": "temporary upstream failure"}})
        value = {"platform": "douyin", "uid": "00012345"}
        target = platform_adapters.next_profile_request(value)
        # Exercise only the raw/result path using a local fixture provider. No
        # live admission or scheduler authority is fabricated by this test.
        claim = replace(self.claim, provider="FixtureTransport")
        with self.assertRaises(capture.CaptureError) as result:
            capture._execute_claimed_fetch_in_slot(claim=claim, operation=target["operation"],
                call=lambda: account_preparation._validated_profile_result(response, value=value, responses=[], target=target),
                db_path=self.db, raw_root=self.raw, network_call=False)
        self.assertEqual(result.exception.error_code, "provider_business_failure")
        self.assertTrue(result.exception.retryable)
        self.assertTrue(result.exception.billed)
        row = self.connection.execute("SELECT * FROM provider_raw_responses").fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["intake_request_id"], 1)
        self.assertEqual(capture.raw_archive.read_response_entity(self.connection, row["id"]), response.entity_body)
        self.assertEqual(self.connection.execute("SELECT status,last_error_code FROM fetch_slots WHERE id=?", (self.slot,)).fetchone()[:],
                         ("retryable_failed", "provider_business_failure"))
        self.assertEqual(self.connection.execute("SELECT count(*) FROM fetch_transport_receipts").fetchone()[0], 1)
        with self.assertRaises(capture.SlotUnavailable):
            capture.load_succeeded_raw_response(intake_request_id=1, stage="profile_prepare",
                window_key="prepare-fixture", operation=target["operation"], db_path=self.db)

    def test_identity_mismatch_is_terminal_and_retains_the_response_on_exception(self):
        value = {"platform": "douyin", "uid": "00012345"}
        target = platform_adapters.next_profile_request(value)
        response = self.response({"code": 200, "router": target["path"], "params": target["params"],
            "data": {"status_code": 0, "data": {"id_str": "98765432", "sec_uid": "MS4wLjAB" + "A"*64, "nickname": "fixture"}}})
        with self.assertRaises(capture.CaptureError) as result:
            account_preparation._validated_profile_result(response, value=value, responses=[], target=target)
        self.assertIn(result.exception.error_code, {"identity_conflict", "request_identity_mismatch"})
        self.assertFalse(result.exception.retryable)
        self.assertEqual(result.exception.entity_bytes, response.entity_body)
        self.assertEqual(result.exception.transport_receipt, response.receipt)


    def test_unsupported_provider_is_rejected_before_any_network_or_slot_claim(self):
        with self.assertRaises(capture.BudgetBlocked):
            capture.execute_intake_fetch(intake_request_id=1, window_key="prepare-fixture", provider="other",
                adapter_version="fixture", operation="douyin_uid_profile", call=lambda: self.fail("must not call"),
                paid_request_identity=self.request, request_transport={}, db_path=self.db)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM fetch_slots").fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
