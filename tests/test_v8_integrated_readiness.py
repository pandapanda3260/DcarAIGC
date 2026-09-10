"""Native RELEASE fixes control diagnostics, never promotes data or billing."""
from __future__ import annotations

import json
import sqlite3
import unittest
from unittest.mock import patch

from v8 import paid_drain, runtime_receipts


class IntegratedReadinessTest(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.addCleanup(self.db.close)
        self.db.execute("CREATE TABLE pipeline_paid_drain_events(id INTEGER PRIMARY KEY, "
                        "target_activation_id INTEGER,payload_json TEXT,event_type TEXT,contract_version TEXT)")
        self.db.execute("INSERT INTO pipeline_paid_drain_events VALUES(1,3,'{}','release',?)",
                        (paid_drain.PROFILE_CONTRACT_VERSION,))
        self.active = {"activation_id": 3, "profile_id": "integrated_route_v1",
                       "roster_snapshot_id": 2, "roster_members_sha256": "a" * 64,
                       "effective_at": "2026-09-07T00:00:00Z"}
        self.drain = paid_drain.DrainState("open", activation_id=3, permit_event_id=1)

    def valid(self, drain=None):
        return runtime_receipts._current_hold_control_valid(
            self.db, active=self.active, drain_state=drain or self.drain,
            at="2026-09-07T03:00:00Z")

    def test_native_release_is_control_ready(self):
        self.assertTrue(self.valid())

    def test_native_does_not_accept_other_profile_or_activation(self):
        for change in ({"profile_id": "tikhub_managed_v1"}, {"activation_id": 4}):
            with self.subTest(change=change), patch.dict(self.active, change):
                self.assertFalse(self.valid())

    def test_malformed_or_wrong_event_is_not_ready(self):
        for column, value in (("event_type", "sealed"), ("contract_version", "unknown"),
                              ("payload_json", "[]"), ("payload_json", '{"control":null}')):
            with self.subTest(column=column):
                self.db.execute("SAVEPOINT fixture")
                self.db.execute(f"UPDATE pipeline_paid_drain_events SET {column}=?", (value,))
                self.assertFalse(self.valid())
                self.db.execute("ROLLBACK TO fixture")
                self.db.execute("RELEASE fixture")

    def test_non_open_or_stale_dispatch_cannot_be_ready(self):
        for state in ("invalid", "closed", "sealed", "draining"):
            with self.subTest(state=state):
                self.assertFalse(self.valid(paid_drain.DrainState(
                    state, activation_id=3, last_event_id=1, permit_event_id=1)))
        self.assertFalse(self.valid(paid_drain.DrainState("open", activation_id=4, permit_event_id=1)))

    def test_legacy_control_still_requires_exact_binding(self):
        control = {"contract_version": "current_activation_hold_v1", "control_purpose": "full_day_release",
                   "activation_id": 3, "roster_snapshot_id": 2, "roster_snapshot_hash": "a" * 64}
        self.db.execute("UPDATE pipeline_paid_drain_events SET payload_json=?", (json.dumps({"control": control}),))
        self.assertTrue(self.valid())
        control["roster_snapshot_id"] = 99
        self.db.execute("UPDATE pipeline_paid_drain_events SET payload_json=?", (json.dumps({"control": control}),))
        self.assertFalse(self.valid())

    def test_missing_day_receipt_stays_data_not_ready(self):
        with patch("v8.profile_activations.activation_at", return_value=self.active), \
                patch.object(paid_drain, "dispatch_state", return_value=self.drain), \
                patch.object(runtime_receipts, "_latest_complete_current_day_row", return_value=None):
            result = runtime_receipts.current_activation_readiness(self.db, at="2026-09-07T03:00:00Z")
        self.assertTrue(result["control_readiness"])
        self.assertFalse(result["data_readiness"])
        self.assertEqual(result["reason"], "current_activation_coverage_incomplete")


if __name__ == "__main__":
    unittest.main()
