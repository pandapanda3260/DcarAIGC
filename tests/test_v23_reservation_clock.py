"""Real reservation/admission SQL shares the post-qualification clock; no HTTP."""
from datetime import datetime
import unittest
from unittest.mock import patch

from tests import test_v8_unsent_admission_refresh as fixtures
from v8 import capture
from v8.storage import transaction

CLAIMED_AT = "2026-09-13T15:59:40Z"
RESERVED_AT = "2026-09-13T16:00:25Z"  # Qualification took 45 s across Beijing midnight.
BUDGET = "reservation-clock-fixture"


class ReservationClockTest(unittest.TestCase):
    def setUp(self):
        real_reserve = capture._reserve_budget
        self.fixture = fixtures.UnsentAdmissionRefreshTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.c = self.fixture.connection
        self.at = CLAIMED_AT
        self.enterContext(patch.object(capture, "now_utc", side_effect=lambda: self.at))
        self.enterContext(patch.object(capture, "_reserve_budget", wraps=real_reserve))
        # Only external authority/owner policy is a fixture boundary. Budget
        # validation, accounting, singleton, admission and slot SQL stay real.
        self.enterContext(patch("v8.provider_budget.renew_paid_owner_lease"))
        self.reservation_check = self.enterContext(patch.object(
            capture, "check_reservation", return_value={"state": "reserved"}))
        self.authority = self.enterContext(patch(
            "v8.capture_authorizations.validate_authorization", side_effect=self.authorize))
        with transaction(self.c):
            self.c.execute("""INSERT INTO provider_budget_batches(id,purpose,provider,operation,
                currency,verified_unit_price,max_billable_requests,max_amount,pilot_size,
                daily_quota,price_verified_at,status,created_at,updated_at)
                VALUES (?,?,'TikHub',?,'USD',0.001,10,1,10,10,?,'approved',?,?)""",
                (BUDGET, BUDGET, fixtures.OPERATION, CLAIMED_AT, CLAIMED_AT, CLAIMED_AT))

    def authorize(self, connection, **kwargs):
        self.assertEqual(kwargs["at"], CLAIMED_AT)
        self.at = RESERVED_AT
        return {"authority_sha256": "c" * 64}

    def claim(self):
        return capture._claim_paid_tikhub(
            content_id=None, account_id=None, intake_request_id=1,
            stage="profile_prepare", window_key=fixtures.WINDOW, provider="TikHub",
            adapter_version="fixture", operation=fixtures.OPERATION, db_path=self.fixture.db,
            budget_id=BUDGET, task_id=None, task_max_amount=None, allow_terminal_retry=False,
            paid_request_identity=self.fixture.request,
        )

    def assert_current_reservation(self, claim):
        usage = self.c.execute("SELECT * FROM provider_usage WHERE id=?",
            (claim.reserved_usage_id,)).fetchone()
        admission = self.fixture.admission()
        self.assertEqual(usage["recorded_at"], RESERVED_AT)
        self.assertEqual(admission["created_at"], usage["recorded_at"])
        self.assertEqual(admission["updated_at"], usage["recorded_at"])
        self.assertEqual(admission["charge_business_day"], "2026-09-14")
        self.assertEqual((datetime.fromisoformat(admission["expires_at"].replace("Z", "+00:00"))
            - datetime.fromisoformat(usage["recorded_at"].replace("Z", "+00:00"))).total_seconds(), 180)
        self.assertEqual(self.authority.call_count, 1)
        self.assertEqual(self.reservation_check.call_args.kwargs["at"], RESERVED_AT)
        self.assertEqual(usage["request_attempts"], 0)
        self.assertEqual(self.c.execute("SELECT count(*) FROM fetch_attempts").fetchone()[0], 0)
        slot = self.c.execute("SELECT * FROM fetch_slots WHERE id=?", (claim.slot_id,)).fetchone()
        self.assertEqual(slot["started_at"], CLAIMED_AT)

    def test_initial_reservation_uses_actual_budget_clock_after_qualification(self):
        self.assert_current_reservation(self.claim())

    def test_released_reservation_uses_new_clock_and_preserves_old_usage(self):
        self.fixture.seed()
        with transaction(self.c):
            previous_id = self.c.execute("""INSERT INTO provider_usage(provider,operation,
                currency,amount,recorded_at,details_json) VALUES ('TikHub',?,'USD',0,?,'{}')""",
                (fixtures.OPERATION, fixtures.OLD)).lastrowid
        previous = tuple(self.c.execute("SELECT * FROM provider_usage WHERE id=?",
            (previous_id,)).fetchone())
        self.assert_current_reservation(self.claim())
        self.assertEqual(tuple(self.c.execute("SELECT * FROM provider_usage WHERE id=?",
            (previous_id,)).fetchone()), previous)


if __name__ == "__main__":
    unittest.main()
