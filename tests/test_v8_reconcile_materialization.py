from __future__ import annotations

import json
import sqlite3
import unittest
from unittest.mock import Mock, patch

from tests import test_v8_tikhub_scan as scan_fixtures
from v8 import paid_drain, pipeline, provider_budget, providers, runtime_receipts
from v8.storage import connect, transaction


class ReconcileMaterializationIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        # Compose the established fixture instead of inheriting its TestCase;
        # inheritance would collect and rerun the entire TikHub scan suite.
        fixture = scan_fixtures.TikHubScanTest(
            methodName=(
                "test_v2_materialization_lock_failure_resumes_locally_without_provider_call"
            )
        )
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.fixture = fixture
        self.db = fixture.db
        self.raw_root = fixture.raw_root
        self.reports = fixture.root / "reports"

    def _rows(self, table: str) -> list[dict[str, object]]:
        with connect(self.db) as connection:
            return [
                dict(row)
                for row in connection.execute(f"SELECT * FROM {table} ORDER BY id")
            ]

    def test_reconcile_replays_local_raw_before_control_work_without_paid_send(self) -> None:
        page = scan_fixtures.dy_page(
            [
                scan_fixtures.dy_item(
                    statistics={
                        "play_count": 321,
                        "digg_count": 10,
                        "comment_count": 2,
                        "share_count": 1,
                        "collect_count": 3,
                    }
                )
            ]
        )
        with patch.object(
            providers,
            "materialize_account_discovery_page",
            side_effect=sqlite3.OperationalError("fixture initial local write lock"),
        ):
            pending = self.fixture.scan(
                call_override=lambda _operation, _request: scan_fixtures.result(page)
            )
        self.assertEqual(
            (pending["status"], pending["reason"], pending["complete"]),
            ("partial", "materialization_pending", False),
        )
        run_id = int(pending["scheduler_run_id"])
        self.assertIsNotNone(self.fixture.state(run_id)["pending_materialization"])
        before_observations = self.fixture.scalar(
            "SELECT COUNT(*) FROM content_metric_observations"
        )
        self.assertEqual(before_observations, 1)
        self.assertEqual(
            self.fixture.scalar(
                "SELECT COUNT(*) FROM fetch_slots WHERE stage IN ('detail','metrics')"
            ),
            0,
        )

        active = self.fixture.activation()
        paid_drain.start_paid_drain(
            "reconcile-local-fixture",
            binding={
                "source_activation_id": active["activation_id"],
                "target_activation_id": active["activation_id"],
                "business_day": "2026-08-29",
                "planned_effective_at": "2026-08-29T16:00:00Z",
                "build_receipt_sha256": "a" * 64,
                "runtime_root_receipt_sha256": "b" * 64,
            },
            db_path=self.db,
            now=scan_fixtures.later(scan_fixtures.NOW, 1),
        )
        with connect(self.db) as connection, transaction(connection):
            provider_budget.record_fault_state(
                connection,
                scope_kind="provider_hard",
                provider="TikHub",
                fault_class="balance",
                reason="provider_balance_blocked",
                usage_id=None,
                at=scan_fixtures.later(scan_fixtures.NOW, 2),
                state_evidence={"http_status": 402},
            )
        with connect(self.db) as connection:
            self.assertEqual(paid_drain.dispatch_state(connection).state, "draining")
            fault = provider_budget.fault_state(
                connection, scope_kind="provider_hard", provider="TikHub"
            )
        self.assertIsNotNone(fault)
        assert fault is not None
        self.assertTrue(fault["open"])

        usage_before = self._rows("provider_usage")
        dispatch_before = self._rows("paid_provider_dispatch_events")
        self.assertEqual(len(usage_before), 1)
        usage_amount_before = sum(float(row["amount"]) for row in usage_before)
        self.assertGreater(usage_amount_before, 0.0)
        self.assertEqual(len(dispatch_before), 3)
        network = Mock(side_effect=AssertionError("reconcile must not call a provider"))
        real_materialize = providers.materialize_account_discovery_page
        real_rounds = pipeline._reconcile_current_day_rounds
        materialized_before_rounds: list[bool] = []

        def observe_rounds(**kwargs):
            state = self.fixture.state(run_id)
            derived = self.fixture.scalar(
                "SELECT COUNT(*) FROM fetch_slots "
                "WHERE stage IN ('detail','metrics') AND status='succeeded'"
            )
            materialized_before_rounds.append(
                state["pending_materialization"] is None and derived == 2
            )
            return real_rounds(**kwargs)

        with patch.object(
            providers,
            "materialize_account_discovery_page",
            wraps=real_materialize,
        ) as local_materializer, patch.object(
            pipeline,
            "_reconcile_current_day_rounds",
            side_effect=observe_rounds,
        ), patch(
            "v8.capture.RAW_ROOT", self.raw_root / "pipeline-derived"
        ):
            reconciled = pipeline._dispatch(
                "pipeline_reconcile",
                db_path=self.db,
                reports_root=self.reports,
                at=scan_fixtures.later(scan_fixtures.NOW),
                call_override=network,
            )

        network.assert_not_called()
        local_materializer.assert_called_once()
        self.assertEqual(materialized_before_rounds, [True])
        self.assertEqual(len(reconciled["local_replay"]), 1)
        replay = reconciled["local_replay"][0]
        self.assertEqual(replay["scheduler_run_id"], run_id)
        self.assertTrue(replay["materialization_finalized"])
        self.assertEqual(
            (replay["processed_items"], replay["succeeded_items"], replay["failed_items"]),
            (1, 1, 0),
        )
        self.assertIsNone(self.fixture.state(run_id)["pending_materialization"])

        with connect(self.db) as connection:
            content = connection.execute(
                "SELECT id,title,platform_content_id FROM content_items"
            ).fetchone()
            stages = [
                tuple(row)
                for row in connection.execute(
                    "SELECT stage,status FROM fetch_slots "
                    "WHERE stage IN ('detail','metrics') ORDER BY stage"
                )
            ]
            derived_billing = connection.execute(
                "SELECT COUNT(*),COALESCE(SUM(billed),0) FROM fetch_attempts "
                "WHERE slot_id IN (SELECT id FROM fetch_slots "
                "WHERE stage IN ('detail','metrics'))"
            ).fetchone()
        self.assertIsNotNone(content)
        assert content is not None
        self.assertEqual(content["title"], "fixture 1")
        self.assertEqual(stages, [("detail", "succeeded"), ("metrics", "succeeded")])
        self.assertEqual(tuple(derived_billing), (2, 0))
        self.assertGreater(
            self.fixture.scalar("SELECT COUNT(*) FROM content_metric_observations"),
            before_observations,
        )

        self.assertEqual(self._rows("provider_usage"), usage_before)
        self.assertEqual(
            sum(float(row["amount"]) for row in self._rows("provider_usage")),
            usage_amount_before,
        )
        self.assertEqual(self._rows("paid_provider_dispatch_events"), dispatch_before)

        with connect(self.db) as connection:
            slice_row = connection.execute(
                "SELECT id,status,details_json FROM scheduler_runs "
                "WHERE job_id='pipeline_reconcile_slice'"
            ).fetchone()
            readiness = runtime_receipts.current_activation_readiness(
                connection, at=scan_fixtures.later(scan_fixtures.NOW)
            )
        self.assertIsNotNone(slice_row)
        assert slice_row is not None
        self.assertEqual(slice_row["status"], "succeeded")
        slice_details = json.loads(slice_row["details_json"])
        self.assertTrue(slice_details["complete"])
        self.assertFalse(slice_details["summary"]["data_complete"])
        self.assertFalse(readiness["control_readiness"])
        self.assertFalse(readiness["data_readiness"])
        self.assertEqual(readiness["reason"], "current_activation_permit_missing")
        self.assertIsNone(readiness["receipt"])


if __name__ == "__main__":
    unittest.main()
