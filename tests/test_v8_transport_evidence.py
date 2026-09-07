from __future__ import annotations

import json
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch

from tests import test_v8_transport_execution as execution_fixture
from v8.raw_evidence import RawEvidenceError
from v8.storage import connect, transaction
from v8.transport_evidence import DiagnosticEvidenceError, read_primary_member_evidence


AT = execution_fixture.AT


class TransportEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.fixture = execution_fixture.TransportExecutionTest(methodName="runTest")
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()
        self.db = self.fixture.db
        self.member_id = self.fixture.fixture.members[0]["receipt_id"]

    def _read(self, *, at=AT):
        with connect(self.db) as connection:
            connection.execute("PRAGMA query_only=ON")
            result = read_primary_member_evidence(connection, self.member_id, at=at)
            self.assertEqual(connection.total_changes, 0)
            return result

    def _usage(self):
        with connect(self.db) as connection:
            return dict(connection.execute("SELECT * FROM provider_usage").fetchone())

    def test_complete_evidence_remains_valid_after_member_expires(self):
        result = self.fixture._execute()
        evidence = self._read()
        self.assertEqual(evidence, self._read(at="2026-09-10T00:00:00Z"))
        self.assertEqual(evidence["raw_response_id"], result["raw_response_id"])
        self.assertEqual(evidence["effective_starts"], 1)
        self.assertEqual(evidence["amount_microusd"], 1000)
        self.assertTrue(evidence["billing_settled"])
        self.assertTrue(evidence["response_complete"])
        self.assertFalse(evidence["qualified"])

    def test_unstarted_member_does_not_claim_send_or_qualification(self):
        evidence = self._read()
        self.assertEqual(evidence["state"], "not_started")
        self.assertEqual(evidence["effective_starts"], 0)
        self.assertFalse(evidence["qualified"])
        self.assertEqual(self.fixture.calls, [])

    def test_full_402_retains_complete_raw_and_zero_known_charge(self):
        self.fixture.status = 402
        self.fixture.body_override = {"code": 402, "message": "insufficient balance"}
        self.fixture._execute()
        evidence = self._read()
        self.assertEqual(evidence["state"], "failed")
        self.assertTrue(evidence["response_complete"])
        self.assertTrue(evidence["billing_settled"])
        self.assertEqual(evidence["amount_microusd"], 0)

    def test_quarantine_and_unknown_cannot_be_settled_by_mutable_state(self):
        self.fixture.fail_rank = 1
        self.fixture._execute()
        evidence = self._read()
        self.assertEqual(evidence["state"], "billing_unknown")
        self.assertEqual(evidence["amount_microusd"], 1000)
        self.assertFalse(evidence["response_complete"])
        self.assertFalse(evidence["billing_settled"])
        usage = self._usage()
        metadata = json.loads(usage["details_json"])
        metadata["state"] = "failed"
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE provider_usage SET details_json=? WHERE id=?",
                               (json.dumps(metadata), usage["id"]))
        self.assertFalse(self._read()["billing_settled"])

    def test_send_marker_tamper_is_rejected(self):
        self.fixture._execute()
        metadata = json.loads(self._usage()["details_json"])
        Path(metadata["paid_send_claim_path"]).write_bytes(b"{}")
        with self.assertRaises(DiagnosticEvidenceError):
            self._read()

    def test_raw_tamper_is_rejected(self):
        result = self.fixture._execute()
        with connect(self.db) as connection:
            path = connection.execute("SELECT local_path FROM provider_raw_responses WHERE id=?",
                                      (result["raw_response_id"],)).fetchone()[0]
        Path(path).write_bytes(b"{}")
        with self.assertRaises((DiagnosticEvidenceError, RawEvidenceError)):
            self._read()

    def test_quarantine_bytes_tamper_is_rejected(self):
        self.fixture.fail_rank = 1
        self.fixture._execute()
        transport = json.loads(self._usage()["details_json"])["transport"]
        Path(transport["quarantine_path"]).write_bytes(b"damaged")
        with self.assertRaises(DiagnosticEvidenceError):
            self._read()

    def test_zero_body_transport_failure_has_hash_verified_receipt(self):
        with patch.object(execution_fixture.transport_fixture.FakeResponse, "read",
                          side_effect=urllib.error.URLError("fixture connection closed")):
            self.fixture._execute()
        evidence = self._read()
        self.assertEqual(evidence["effective_starts"], 1)
        self.assertFalse(evidence["response_complete"])
        self.assertFalse(evidence["billing_settled"])
        receipt = json.loads(self._usage()["details_json"])["transport"]
        self.assertTrue(receipt["zero_body"])
        self.assertEqual(receipt["partial_bytes"], 0)
        self.assertIsNone(receipt["quarantine_path"])
        self.assertTrue(Path(receipt["quarantine_receipt_path"]).is_file())

    def test_changed_route_in_usage_is_rejected(self):
        self.fixture._execute()
        usage = self._usage()
        metadata = json.loads(usage["details_json"])
        metadata["request_transport"]["manifest"]["request_host"] = "api.tikhub.io"
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE provider_usage SET details_json=? WHERE id=?",
                               (json.dumps(metadata), usage["id"]))
        with self.assertRaises(DiagnosticEvidenceError):
            self._read()

    def test_known_amount_must_match_price_and_attempt(self):
        self.fixture._execute()
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE provider_usage SET amount=0")
        with self.assertRaises(DiagnosticEvidenceError):
            self._read()

    def test_revoked_at_semaphore_is_not_sent_without_charge_or_marker(self):
        capture_fixture = self.fixture.fixture
        with capture_fixture._after_network_wait(capture_fixture.scheduler.resume):
            result = self.fixture._execute()
        self.assertEqual(result["effective_starts"], 0)
        evidence = self._read()
        self.assertEqual(evidence["state"], "not_sent")
        self.assertEqual(evidence["amount_microusd"], 0)
        self.assertEqual(self.fixture.calls, [])
