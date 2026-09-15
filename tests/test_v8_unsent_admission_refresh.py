"""Reacquire released admissions through the real claim transaction on temporary DBs.

Installed authority and the verified budget quote are fixture boundaries. The
production singleton, scope availability, reservation SQL and rollback remain
real; no send boundary, provider request or live authorization is exercised.
"""
from __future__ import annotations

import json
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import patch

from v8 import capture, capture_singletons
from v8.paid_identity import build_paid_request_identity
from v8.provider_budget import BudgetBlocked, PaidScope, PaidScopeBlocked
from v8.storage import connect, initialize_database, transaction

OLD = "2026-09-12T15:50:00Z"
NOW = "2026-09-12T16:01:00Z"  # Next Beijing business day.
OPERATION = "douyin_uid_profile"
UID = "00012345"
WINDOW = "prepare:fixture:revision:1"


class UnsentAdmissionRefreshTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.db = Path(temporary.name) / "fixture.sqlite3"
        self.connection = connect(self.db)
        self.addCleanup(self.connection.close)
        initialize_database(self.connection, target_version=22)
        c = self.connection
        c.execute("""INSERT INTO account_intake_requests(request_key,input_sha256,preparation_key,
            platform,input_json,source_json,created_at,updated_at)
            VALUES ('fixture',?,'fixture','douyin','{}','{}',?,?)""", ("a" * 64, OLD, OLD))
        self.assignment = c.execute("""INSERT INTO capture_route_assignments(scope_type,scope_key,provider,
            operation,intake_request_id,generation,route,mode,effective_at,recorded_at,assignment_sha256)
            VALUES ('intake','1','tikhub',?,1,1,'integrated','active',?,?,?)""",
            (OPERATION, OLD, OLD, "b" * 64)).lastrowid
        c.commit()
        self.scope = PaidScope(platform="douyin", intake_request_id=1, preparation_subject=UID)
        self.request = build_paid_request_identity(provider="TikHub", operation=OPERATION,
            platform="douyin", subject=UID, request_parameters={"uid": UID}, cursor=None, due_bucket=WINDOW)
        self.enterContext(patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden")))
        self.enterContext(patch.object(capture, "now_utc", return_value=NOW))
        self.enterContext(patch.object(capture.paid_drain, "require_paid_dispatch_open"))
        self.enterContext(patch.object(capture, "freeze_scope", return_value=self.scope))
        self.enterContext(patch.object(capture, "_intake_request_subject", return_value=UID))
        self.enterContext(patch("v8.capture_planning.require_send_route", return_value={"id": self.assignment}))
        self.enterContext(patch("v8.capture_authorizations.current_runtime_bindings", return_value={}))
        self.enterContext(patch("v8.capture_authorizations.validate_authorization",
            return_value={"authority_sha256": "c" * 64}))
        self.enterContext(patch.object(capture, "supports_dispatch_ledger", return_value=False))
        self.enterContext(patch.object(capture, "_reserve_budget", side_effect=self.reserve_budget))
        with transaction(c):
            self.batch, _ = capture_singletons.freeze(c, request=self.request, scope=self.scope, at=OLD)
            self.slot = capture.ensure_intake_slot(c, intake_request_id=1, stage="profile_prepare",
                window_key=WINDOW, provider="TikHub", adapter_version="fixture")
            self.member_before = tuple(c.execute("SELECT * FROM fetch_request_batch_members").fetchone())
            self.batch_before = tuple(c.execute("SELECT * FROM fetch_request_batches").fetchone())

    def reserve_budget(self, connection, **kwargs):
        usage_id = connection.execute("""INSERT INTO provider_usage(provider,operation,currency,amount,
            recorded_at,details_json) VALUES ('TikHub',?,'USD',0.001,?,?)""",
            (kwargs["operation"], NOW, json.dumps({"state": "reserved"}))).lastrowid
        return usage_id, 0.001, "USD"

    def seed(self, *, day="2026-09-12", amount=500, state="released_unsent"):
        with transaction(self.connection):
            self.connection.execute("DELETE FROM admission_reservations")
            self.connection.execute("DELETE FROM provider_usage")
            self.connection.execute("UPDATE fetch_slots SET status='pending'")
            self.connection.execute("""INSERT INTO admission_reservations(batch_id,state,amount_microusd,
                charge_business_day,created_at,expires_at,updated_at) VALUES (?,?,?,?,?,?,?)""",
                (self.batch, state, amount, day, OLD, "2026-09-12T15:53:00Z", OLD))

    def claim(self, request=None):
        return capture._claim_paid_tikhub(content_id=None, account_id=None, intake_request_id=1,
            stage="profile_prepare", window_key=WINDOW, provider="TikHub", adapter_version="fixture",
            operation=OPERATION, db_path=self.db, budget_id="fixture-verified-budget", task_id="fixture",
            task_max_amount=1, allow_terminal_retry=False, paid_request_identity=request or self.request)

    def admission(self):
        return dict(self.connection.execute("SELECT * FROM admission_reservations WHERE batch_id=?",
            (self.batch,)).fetchone())

    def test_released_unsent_refreshes_cross_day_and_changed_quote_without_changing_identity(self):
        for old_day, old_amount in (("2026-09-12", 1000), ("2026-09-13", 500), ("2026-09-12", 500)):
            with self.subTest(day=old_day, amount=old_amount):
                self.seed(day=old_day, amount=old_amount)
                claim = self.claim()
                admission = self.admission()
                self.assertEqual(admission["state"], "reserved_unsent")
                self.assertEqual(admission["charge_business_day"], "2026-09-13")
                self.assertEqual(admission["amount_microusd"], 1000)
                self.assertEqual(admission["expires_at"], "2026-09-12T16:04:00Z")
                self.assertEqual(admission["updated_at"], NOW)
                self.assertEqual(admission["created_at"], NOW)
                self.assertEqual(claim.request_batch_id, self.batch)
                self.assertEqual(claim.slot_id, self.slot)
                self.assertEqual(claim.window_key, WINDOW)
                self.assertEqual(claim.paid_scope_identity, self.request.scope_identity)
                self.assertEqual(claim.paid_sequence, 0)
                self.assertEqual(tuple(self.connection.execute("SELECT * FROM fetch_request_batches").fetchone()), self.batch_before)
                self.assertEqual(tuple(self.connection.execute("SELECT * FROM fetch_request_batch_members").fetchone()), self.member_before)
                self.assertEqual(self.connection.execute("SELECT count(*) FROM fetch_attempts").fetchone()[0], 0)
                self.assertEqual(self.connection.execute("SELECT count(*) FROM provider_paid_scope_claims").fetchone()[0], 0)

    def test_reserved_or_sent_admissions_are_not_refreshed(self):
        for state in ("reserved_unsent", "sent_unsettled", "settled"):
            with self.subTest(state=state):
                self.seed(state=state)
                before = self.admission()
                if state == "reserved_unsent":
                    self.claim()
                else:
                    with self.assertRaisesRegex(PaidScopeBlocked, "already sent"):
                        self.claim()
                    self.assertEqual(self.connection.execute("SELECT count(*) FROM provider_usage").fetchone()[0], 0)
                self.assertEqual(self.admission(), before)

    def test_current_budget_rejection_leaves_released_admission_untouched(self):
        self.seed()
        before = self.admission()
        with patch.object(capture, "_reserve_budget", side_effect=BudgetBlocked("current budget unavailable")):
            with self.assertRaisesRegex(BudgetBlocked, "current budget unavailable"):
                self.claim()
        self.assertEqual(self.admission(), before)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM provider_usage").fetchone()[0], 0)

    def test_changed_request_scope_cannot_refresh_the_old_admission(self):
        self.seed()
        before = self.admission()
        different = build_paid_request_identity(provider="TikHub", operation=OPERATION,
            platform="douyin", subject="88776655", request_parameters={"uid": "88776655"},
            cursor=None, due_bucket=WINDOW)
        with self.assertRaises(PaidScopeBlocked):
            self.claim(different)
        self.assertEqual(self.admission(), before)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM provider_usage").fetchone()[0], 0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM fetch_request_batches").fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
