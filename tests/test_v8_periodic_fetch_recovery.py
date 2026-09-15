"""Periodic orphan recovery uses real local slots, dispatches and accounting."""
from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timezone
import json
from pathlib import Path
import socket
import unittest
from unittest.mock import patch

from tests import test_v8_provider_budget as fixtures
from v8 import account_intake, capture, durable_runs, pipeline
from v8.paid_dispatch import dispatch_events, reserve_dispatch_in_transaction
from v8.paid_drain import dispatch_state
from v8.provider_budget import fault_state
from v8.storage import connect, initialize_database, transaction

AT = fixtures.AT
LATER = "2026-08-29T04:20:00Z"


class PeriodicFetchRecoveryTest(unittest.TestCase):
    # Seed genuine historical reservations through the existing paid-claim
    # fixture, then perform the supported offline upgrade of this temp DB.
    setUp = fixtures.ProviderBudgetTest.setUp
    tearDown = fixtures.ProviderBudgetTest.tearDown
    roster = fixtures.ProviderBudgetTest.roster
    dispatch_scope = fixtures.ProviderBudgetTest.dispatch_scope
    budget = fixtures.ProviderBudgetTest.budget
    claim_only = fixtures.ProviderBudgetTest.claim_only
    usage = fixtures.ProviderBudgetTest.usage

    def upgrade(self, version=22):
        with connect(self.db) as connection:
            initialize_database(connection, target_version=version)

    def owner(self, suffix, at=AT):
        owner = durable_runs.claim_run("periodic-fixture:" + suffix, {"fixture": suffix},
                                       db_path=self.db, now=at)
        self.assertIsNotNone(owner)
        self.dispatch_owner = owner
        return owner

    def bind_owner(self, slot, owner, at):
        with connect(self.db) as connection, transaction(connection):
            state = dispatch_state(connection, at=at)
            event = reserve_dispatch_in_transaction(connection, provider="TikHub",
                operation="douyin_video_detail", activation_id=state.activation_id,
                business_day="2026-08-29", scheduler_run_id=owner.scheduler_run_id,
                scheduler_attempt_id=owner.attempt_id, scope={"fixture": True},
                created_at=at, fetch_slot_id=slot)
            self.assertIsNotNone(event)

    def periodic(self, at=LATER):
        def fixture_writer(connection):
            path = Path(connection.execute("PRAGMA database_list").fetchone()[2]).resolve()
            self.assertEqual(path, self.db.resolve())
            self.assertTrue(path.is_relative_to(Path(self.temp.name).resolve()))
            return {"fixture": True}

        # Only installation/authorization boundaries are fixtures. The writer
        # transaction, recovery SQL, dispatch evidence, accounting, quality
        # maintenance and receipts all execute against the real temporary DB.
        with patch("v8.runtime_database.require_current_process_writer_lock", side_effect=fixture_writer), \
             patch("v8.capture_authorizations.runtime_authority", return_value=nullcontext()), \
             patch("v8.account_roster_capture.activate_prepared_roster_capture_in_transaction"), \
             patch("v8.capture_release.maintain_operation_qualifications", return_value={"fixture": True}), \
             patch.object(socket.socket, "connect", side_effect=AssertionError("provider network forbidden")) as network:
            result = pipeline._capture_v25_job(kind="maintenance", db_path=self.db, at=at)
        network.assert_not_called()
        return result

    def states(self):
        with connect(self.db) as connection:
            return {row["id"]: dict(row) for row in connection.execute("SELECT * FROM fetch_slots")}

    def test_periodic_recovers_orphans_but_preserves_live_fresh_and_unknown_paid_evidence(self):
        self.roster()
        budget = self.budget()
        with connect(self.db) as connection, transaction(connection):
            for identifier in (3, 4):
                connection.execute("""INSERT INTO content_items(id,link_id,platform,platform_content_id,
                    canonical_url,account_id,raw_account_uid,imported_at,created_at,updated_at)
                    VALUES(?,?,'douyin',?,'https://example.com',1,'10000001',?,?,?)""",
                    (identifier, f"C{identifier:05d}", str(identifier), AT, AT, AT))
        with patch("v8.capture.now_utc", return_value=AT):
            unsent = self.claim_only(budget)
            sent = self.claim_only(budget, content_id=2)
            capture._mark_paid_sent(sent, operation="douyin_video_detail", budget_id=budget, db_path=self.db)
        self.upgrade()
        with connect(self.db) as connection, transaction(connection):
            live = capture.ensure_content_slot(connection, content_id=3, stage="detail",
                window_key="lifetime", provider="TikHub", adapter_version="fixture")
            fresh = capture.ensure_content_slot(connection, content_id=4, stage="detail",
                window_key="lifetime", provider="TikHub", adapter_version="fixture")
            connection.executemany("UPDATE fetch_slots SET status='running',started_at=?,updated_at=? WHERE id=?",
                [(AT, AT, live), ("2026-08-29T04:19:00Z", "2026-08-29T04:19:00Z", fresh)])
            intake = account_intake.submit_account_intake(connection, request_key="orphan-intake",
                value={"platform": "douyin", "uid": "123456789"}, source={"kind": "fixture"}, at=AT)
            orphan = capture.ensure_intake_slot(connection, intake_request_id=intake["intake_id"],
                stage="profile_prepare", window_key=AT, provider="TikHub", adapter_version="fixture")
            connection.execute("UPDATE fetch_slots SET status='running',started_at=?,updated_at=? WHERE id=?",
                               (AT, AT, orphan))
        live_owner = self.owner("live", at=AT)
        self.bind_owner(live, live_owner, AT)
        for minute in range(2, 20, 2):
            with connect(self.db) as connection, transaction(connection):
                durable_runs.heartbeat(connection, live_owner, now=f"2026-08-29T04:{minute:02d}:00Z")
        before = self.states()
        result = self.periodic()
        self.assertEqual(result["recovered_fetch_slots"], {"stale_candidates": 3, "recovered": 3})
        self.assertEqual(result["provider_calls"], 0)
        states = self.states()
        for identifier in (unsent.slot_id, sent.slot_id, orphan):
            self.assertEqual(states[identifier]["status"], "retryable_failed")
        self.assertEqual(states[live], before[live])
        self.assertEqual(states[fresh], before[fresh])
        usages = self.usage()
        self.assertEqual([row["amount"] for row in usages], [0, .001])
        self.assertEqual([json.loads(row["details_json"])["state"] for row in usages],
                         ["not_sent", "billing_unknown"])
        self.assertEqual(states[sent.slot_id]["last_error_code"], capture.BILLING_UNKNOWN_SLOT_ERROR)
        with connect(self.db) as connection:
            hold = fault_state(connection, scope_kind="paid_identity_hold",
                paid_identity=json.loads(usages[1]["details_json"])["paid_scope_identity"])
            self.assertTrue(hold["open"])
            self.assertIsNotNone(connection.execute("SELECT id FROM data_quality_receipts WHERE id=?",
                                                    (result["receipt_id"],)).fetchone())
            self.assertEqual(connection.execute("SELECT count(*) FROM provider_raw_responses").fetchone()[0], 0)
            self.assertEqual(connection.execute("SELECT count(*) FROM fetch_transport_receipts").fetchone()[0], 0)
            ids = [row[0] for row in connection.execute(
                "SELECT dispatch_id FROM paid_provider_dispatch_events WHERE sequence=1 ORDER BY id")]
            self.assertEqual([[event.event_type for event in dispatch_events(connection, identifier)] for identifier in ids],
                [["reserved", "not_sent"], ["reserved", "send_marked", "billing_unknown"], ["reserved"]])
        replay = self.periodic()
        self.assertEqual(replay["recovered_fetch_slots"], {"stale_candidates": 0, "recovered": 0})
        self.assertEqual(replay["status"], "already_recorded")
        self.assertEqual(self.usage(), usages)
        # The same old slot becomes recoverable once its actual owner expires;
        # a five-minute maintenance tick then clears the orphan automatically.
        later = self.periodic("2026-08-29T04:35:00Z")
        self.assertEqual(later["recovered_fetch_slots"]["recovered"], 2)

    def test_any_live_owner_protects_slot_with_multiple_historical_dispatches(self):
        self.roster()
        self.upgrade()
        with connect(self.db) as connection, transaction(connection):
            slot = capture.ensure_content_slot(connection, content_id=1, stage="detail",
                window_key="lifetime", provider="TikHub", adapter_version="fixture")
            connection.execute("UPDATE fetch_slots SET status='running',started_at=?,updated_at=? WHERE id=?",
                               (AT, AT, slot))
        old_owner = self.owner("old", at=AT)
        self.bind_owner(slot, old_owner, AT)
        live_owner = self.owner("live", at="2026-08-29T04:38:00Z")
        self.bind_owner(slot, live_owner, "2026-08-29T04:38:00Z")
        result = self.periodic("2026-08-29T04:40:00Z")
        self.assertEqual(result["recovered_fetch_slots"]["recovered"], 0)
        self.assertEqual(self.states()[slot]["status"], "running")
        # The historical public API still uses its original startup semantics.
        recovered = capture.recover_stale_fetch_slots(db_path=self.db,
            current_time=datetime(2026, 8, 29, 4, 40, tzinfo=timezone.utc))
        self.assertEqual(recovered["recovered"], 1)

    def test_legacy_periodic_maintenance_does_not_call_new_recovery(self):
        for version in (20, 21):
            with self.subTest(schema=version):
                self.upgrade(version)
                with patch.object(capture, "recover_stale_fetch_slots", side_effect=AssertionError("legacy recovery forbidden")) as recovery:
                    result = self.periodic()
                recovery.assert_not_called()
                self.assertNotIn("recovered_fetch_slots", result)


if __name__ == "__main__":
    unittest.main()
