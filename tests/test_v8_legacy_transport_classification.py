from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from tests import test_v8_provider_budget as fixture
from v8.capture import CaptureError
from v8.provider_budget import (
    _write_receipt,
    PaidScopeBlocked,
    circuit_state,
    classify_legacy_transport_fault,
    record_circuit,
)
from v8.storage import connect, transaction


class LegacyTransportClassificationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = fixture.ProviderBudgetTest(methodName="runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.db = self.fixture.db
        self.fixture.roster()
        self.budget = self.fixture.budget()
        with patch("v8.capture.now_utc", return_value=fixture.AT):
            with self.assertRaises(CaptureError):
                self.fixture.execute(self.budget, call=lambda: (_ for _ in ()).throw(
                    CaptureError("IncompleteRead", retryable=True,
                                 error_code="transport_error", billed=None)
                ))
        self.original = dict(self.fixture.usage()[0])
        with connect(self.db) as connection, transaction(connection):
            self.legacy = _write_receipt(connection, "provider_circuit:tikhub", {
                "contract_version": "provider-circuit-v1", "provider": "TikHub",
                "open": True, "reason": "transport_error",
                "usage_id": self.original["id"], "opened_at": fixture.AT,
            }, fixture.AT)

    def classify(self, connection):
        return classify_legacy_transport_fault(
            connection, expected_receipt_id=self.legacy["id"],
            actor="fixture-owner", at=fixture.AT,
        )

    def test_classification_releases_unrelated_requests_and_preserves_unknown(self) -> None:
        with connect(self.db) as connection, transaction(connection):
            self.assertTrue(circuit_state(connection)["open"])
            first = self.classify(connection)
            again = self.classify(connection)
            self.assertEqual(first["id"], again["receipt_id"])
            self.assertFalse(circuit_state(connection)["open"])
        with patch("v8.capture.now_utc", return_value=fixture.AT):
            with self.assertRaises(PaidScopeBlocked) as caught:
                self.fixture.execute(self.budget)
            self.assertEqual(caught.exception.error_code, "billing_unknown_retry_blocked")
            self.fixture.execute(self.budget, content_id=2)
        self.assertEqual(dict(self.fixture.usage()[0]), self.original)
        self.assertEqual(len(self.fixture.usage()), 2)

    def test_classification_never_clears_a_real_provider_fault(self) -> None:
        with connect(self.db) as connection, transaction(connection):
            record_circuit(connection, reason="provider_balance_blocked",
                           usage_id=None, at=fixture.AT)
            self.classify(connection)
            current = circuit_state(connection)
            self.assertTrue(current["open"])
            self.assertEqual(current["fault_class"], "balance")

    def test_unprotected_original_request_cannot_be_classified(self) -> None:
        original = json.loads(self.original["details_json"])
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE fetch_slots SET last_error_code=NULL WHERE id=?",
                               (original["slot_id"],))
            with self.assertRaisesRegex(ValueError, "retry guard"):
                self.classify(connection)
            self.assertTrue(circuit_state(connection)["open"])

    def test_wrong_receipt_cannot_be_classified(self) -> None:
        with connect(self.db) as connection, transaction(connection):
            with self.assertRaisesRegex(ValueError, "exact transport"):
                classify_legacy_transport_fault(connection,
                    expected_receipt_id=self.legacy["id"] + 1,
                    actor="fixture-owner", at=fixture.AT)
