from __future__ import annotations

import unittest

from tests import test_v8_transport_cohort as fixtures
from tikhub_config import resolve_tikhub_transport_manifest
from v8 import paid_drain
from v8.provider_transport import RequestTransportBindingError
from v8.storage import connect, transaction
from v8.transport_campaign import TransportCampaignError, freeze_primary_transport_campaign
from v8.transport_cohort import TransportCohortError


class TransportCampaignTest(unittest.TestCase):
    # Use the existing real HOLD fixture without collecting its tests again.
    setUp = fixtures.TransportCohortTest.setUp
    _before_hold = fixtures.TransportCohortTest._before_hold
    _roster = fixtures.TransportCohortTest._roster
    _evidence = fixtures.TransportCohortTest._evidence
    _register_prerequisite = fixtures.TransportCohortTest._register_prerequisite
    _raw = fixtures.TransportCohortTest._raw
    _freeze = fixtures.TransportCohortTest._freeze
    _seed_rank_fixture = fixtures.TransportCohortTest._seed_rank_fixture

    def _transport(self, host: str = "api.tikhub.dev") -> dict:
        path = self.root / "route.env"
        path.write_text(f"TIKHUB_API_BASE=https://{host}\n", encoding="utf-8")
        path.chmod(0o600)
        return {
            "manifest": resolve_tikhub_transport_manifest(path, honor_environment=False),
            "config_path": str(path),
            "honor_environment": False,
        }

    def _campaign(self, cohort: dict, transport: dict, *, at: str = fixtures.FREEZE_AT) -> dict:
        with connect(self.db) as connection, transaction(connection):
            return freeze_primary_transport_campaign(
                connection, drain_id=fixtures.HOLD_ID,
                cohort_receipt_id=cohort["receipt_id"], request_transport=transport,
                at=at, mirror_root=self.mirror_root,
            )

    def test_fixed_primary_header_is_idempotent_and_never_opens_dispatch(self):
        self._seed_rank_fixture()
        cohort = self._freeze()
        transport = self._transport()
        first = self._campaign(cohort, transport)
        repeated = self._campaign(cohort, transport, at="2026-09-06T06:00:00Z")
        self.assertEqual(first, repeated)
        payload = first["payload"]
        self.assertEqual(payload["sample_limit"], 20)
        self.assertEqual(payload["rank_start"], 1)
        self.assertEqual(payload["arm"], "primary")
        self.assertEqual(payload["max_cost_microusd"], 20_000)
        self.assertEqual(payload["cohort_receipt_sha256"], cohort["self_sha256"])
        self.assertEqual(payload["hold_binding"], cohort["payload"]["hold_binding"])
        self.assertEqual(payload["request_transport"], transport)
        self.assertEqual(payload["expires_at"], "2026-09-07T05:30:00+00:00")
        with connect(self.db) as connection, transaction(connection):
            with self.assertRaises(paid_drain.PaidDrainBlocked):
                paid_drain.require_paid_dispatch_open(
                    connection, provider="TikHub", operation="douyin_user_posts",
                    at=fixtures.FREEZE_AT,
                )
            for table in ("provider_usage", "fetch_attempts", "paid_provider_dispatch_events"):
                self.assertEqual(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], 0)
            self.assertEqual(connection.execute(
                "SELECT COUNT(*) FROM scheduler_runs WHERE job_id='transport_receipt:campaign'"
            ).fetchone()[0], 1)

    def test_route_change_and_wrong_primary_host_fail_closed(self):
        self._seed_rank_fixture()
        cohort = self._freeze()
        wrong = self._transport("api.tikhub.io")
        with self.assertRaises(TransportCampaignError) as caught:
            self._campaign(cohort, wrong)
        self.assertEqual(caught.exception.code, "transport_campaign_primary_route_invalid")
        transport = self._transport()
        self._campaign(cohort, transport)
        self._transport("api.tikhub.io")
        with self.assertRaises(RequestTransportBindingError):
            self._campaign(cohort, transport)

    def test_expiry_keeps_original_denominator_and_binding_drift_rejects(self):
        self._seed_rank_fixture()
        cohort = self._freeze()
        transport = self._transport()
        original = self._campaign(cohort, transport)
        self._raw(account_id=6, padding=10_000, captured_at="2026-09-06T07:00:00Z")
        # Deadline expiry does not permit a new set of 20 or move the HWM.
        self.assertEqual(
            self._campaign(cohort, transport, at="2026-09-07T06:00:00Z"), original,
        )
        self._register_prerequisite("config", fixtures.NEW_CONFIG, at="2026-09-07T06:01:00Z")
        with self.assertRaises(TransportCampaignError) as caught:
            self._campaign(cohort, transport, at="2026-09-07T06:02:00Z")
        self.assertEqual(caught.exception.code, "transport_campaign_cohort_invalid")
        with self.assertRaises(TransportCohortError):
            self._freeze(at="2026-09-07T06:02:00Z")

    def test_shortest_prerequisite_expiry_caps_campaign_deadline(self):
        self._seed_rank_fixture()
        # Register a valid, shorter-lived budget prerequisite before freezing.
        from v8 import profile_control
        profile_control.record_current_activation_hold_prerequisite(
            db_path=self.db, drain_id=fixtures.HOLD_ID, kind="budget",
            artifact_sha256="9" * 64,
            receipt_contract_version=profile_control.CURRENT_HOLD_PREREQUISITE_CONTRACTS["budget"],
            expires_at="2026-09-06T07:00:00Z",
            evidence=self._evidence("budget", "9" * 64), actor=fixtures.OWNER,
            now="2026-09-06T05:10:00Z",
        )
        receipt = self._campaign(self._freeze(), self._transport())
        self.assertEqual(receipt["payload"]["expires_at"], "2026-09-06T07:00:00+00:00")


if __name__ == "__main__":
    unittest.main()
