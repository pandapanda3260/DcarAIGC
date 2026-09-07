from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from tests import test_v8_pipeline as pipeline_fixtures
from v8 import pipeline, provider_budget
from v8.media_state import MediaTerminalDetail
from v8.storage import connect, transaction


AT = pipeline_fixtures.AT
LATER = pipeline_fixtures.LATER


class QueueBacklogTest(unittest.TestCase):
    # Reuse the established real temporary-DB setup without inheriting the
    # original TestCase, which would collect its full suite again.
    setUp = pipeline_fixtures.PipelineTest.setUp
    account_row = pipeline_fixtures.PipelineTest.account_row
    roster = pipeline_fixtures.PipelineTest.roster
    activate = pipeline_fixtures.PipelineTest.activate
    content = pipeline_fixtures.PipelineTest.content
    durable = pipeline_fixtures.PipelineTest.durable

    def _ledger_counts(self) -> tuple[int, int, int]:
        with connect(self.db) as connection:
            return tuple(
                int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                for table in (
                    "scheduler_runs",
                    "scheduler_run_attempts",
                    "provider_usage",
                )
            )

    def _open_provider_fault(self) -> dict:
        with connect(self.db) as connection, transaction(connection):
            provider_budget.record_fault_state(
                connection,
                scope_kind="provider_hard",
                fault_class="balance",
                reason="provider_balance_blocked",
                usage_id=None,
                at=AT,
                state_evidence={"balance_snapshot": "blocked-fixture"},
            )
            return dict(
                provider_budget.fault_state(
                    connection,
                    scope_kind="provider_hard",
                    fault_class="balance",
                )
            )

    def _recover_provider_fault(self, fault: dict) -> None:
        proof = {
            "contract_version": "provider-circuit-probe-v1",
            "state": "succeeded",
            "circuit_generation": fault["generation"],
            "state_fingerprint": fault["state_fingerprint"],
            "fault_class": fault["fault_class"],
        }
        with connect(self.db) as connection, transaction(connection):
            cursor = connection.execute(
                """INSERT INTO scheduler_runs(
                       job_id,scheduled_for,status,started_at,completed_at,details_json)
                   VALUES (?,?,'succeeded',?,?,?)""",
                (
                    provider_budget.PROBE_JOB,
                    LATER,
                    LATER,
                    LATER,
                    json.dumps(proof, sort_keys=True),
                ),
            )
            self.assertTrue(
                provider_budget.resolve_fault_state(
                    connection,
                    scope_kind="provider_hard",
                    fault_class="balance",
                    expected_generation=fault["generation"],
                    expected_fingerprint=fault["state_fingerprint"],
                    evidence_id=int(cursor.lastrowid or 0),
                    at=LATER,
                )
            )

    def test_provider_fault_partitions_claimed_content_without_losing_denominator(
        self,
    ) -> None:
        snapshot = self.activate()
        content = self.content()
        self.durable(
            "content_pipeline",
            {
                "pipeline_version": pipeline.PIPELINE_VERSION,
                "kind": "content_pipeline",
                "roster_snapshot_id": snapshot["id"],
                "roster_snapshot_hash": snapshot["members_sha256"],
                "created_for": AT,
                "candidate_ids": [content["id"]],
            },
            checkpoint={
                "pending_ids": [content["id"]],
                "items": [
                    {
                        "id": content["id"],
                        "identity_id": self.identity_id,
                        "historical": False,
                    }
                ],
                "results": {},
                "complete": False,
            },
        )

        baseline = pipeline.queue_backlog_summary(db_path=self.db, at=AT)
        self.assertEqual(baseline["contract_version"], "queue-backlog-v1")
        self.assertEqual(
            {key: baseline[key] for key in ("total", "runnable", "blocked")},
            {"total": 3, "runnable": 3, "blocked": 0},
        )
        self.assertEqual(
            baseline["by_kind"]["content_pipeline"]["total"],
            1,
        )

        fault = self._open_provider_fault()
        before_read = self._ledger_counts()
        blocked = pipeline.queue_backlog_summary(db_path=self.db, at=AT)
        self.assertEqual(self._ledger_counts(), before_read)
        self.assertEqual(blocked["total"], baseline["total"])
        self.assertEqual(
            {key: blocked[key] for key in ("runnable", "blocked")},
            {"runnable": 0, "blocked": 3},
        )
        self.assertEqual(blocked["blocked_operation_count"], 4)
        self.assertEqual(
            blocked["by_kind"]["metrics_backfill"],
            {
                "total": 1,
                "runnable": 0,
                "blocked": 1,
                "blocked_operation_count": 2,
            },
        )
        self.assertEqual(blocked["by_reason"], {"provider_circuit_open": 3})

        self._recover_provider_fault(fault)
        after_recovery = self._ledger_counts()
        recovered = pipeline.queue_backlog_summary(db_path=self.db, at=LATER)
        self.assertEqual(self._ledger_counts(), after_recovery)
        self.assertEqual(recovered["total"], baseline["total"])
        self.assertEqual(
            {key: recovered[key] for key in ("runnable", "blocked")},
            {"runnable": 3, "blocked": 0},
        )
        self.assertEqual(recovered["blocked_operation_count"], 0)

    def test_partly_blocked_metrics_remain_runnable_and_count_blocked_operation(
        self,
    ) -> None:
        self.activate()
        self.content()
        with connect(self.db) as connection, transaction(connection):
            provider_budget.record_fault_state(
                connection,
                scope_kind="operation",
                operation="douyin_video_detail",
                fault_class="field_contract",
                reason="field_contract_invalid",
                usage_id=None,
                at=AT,
                state_evidence={"contract_hash": "detail-fixture"},
            )

        summary = pipeline.queue_backlog_summary(db_path=self.db, at=AT)
        metrics = summary["by_kind"]["metrics_backfill"]
        self.assertEqual(
            metrics,
            {
                "total": 1,
                "runnable": 1,
                "blocked": 0,
                "blocked_operation_count": 1,
            },
        )
        self.assertEqual(summary["by_reason"]["operation_blocked"], 2)
        self.assertEqual(summary["total"], summary["runnable"] + summary["blocked"])

    def test_media_prerequisite_is_blocked_work_not_a_dropped_candidate(self) -> None:
        self.activate()
        content = self.content()
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                """INSERT INTO taxonomy_versions(
                       id,version,status,definition,created_at,published_at)
                   VALUES ('queue-backlog-taxonomy','queue-backlog-taxonomy',
                           'published','{}',?,?)""",
                (AT, AT),
            )
            connection.execute(
                """INSERT INTO evaluation_releases(
                       id,rule_version,taxonomy_version,matcher_rule_sha256,status,
                       created_at,updated_at,activated_at)
                   VALUES ('queue-backlog-release','queue-backlog-rule',
                           'queue-backlog-taxonomy',?,'active',?,?,?)""",
                ("a" * 64, AT, AT, AT),
            )
        states = {
            content["id"]: MediaTerminalDetail("pending", "restore_required")
        }
        with patch.object(
            pipeline, "media_terminal_state_details", return_value=states
        ), patch.object(
            pipeline,
            "current_bundle",
            return_value={"bundle_id": "b" * 32, "state": {}},
        ):
            summary = pipeline.queue_backlog_summary(db_path=self.db, at=AT)

        self.assertEqual(
            summary["by_kind"]["content_pipeline"],
            {
                "total": 1,
                "runnable": 0,
                "blocked": 1,
                "blocked_operation_count": 0,
            },
        )
        self.assertEqual(summary["by_reason"]["restore_required"], 1)
        self.assertEqual(summary["total"], summary["runnable"] + summary["blocked"])

    def test_non_durable_receipts_do_not_hide_runs_or_change_discovery_coverage(
        self,
    ) -> None:
        self.activate()
        before = pipeline.pipeline_summary(db_path=self.db, at=AT)
        durable_id = self.durable(
            "backlog-visible-fixture",
            {"fixture": "must-remain-visible"},
            complete=True,
        )
        with connect(self.db) as connection, transaction(connection):
            connection.executemany(
                """INSERT INTO scheduler_runs(
                       job_id,scheduled_for,status,started_at,completed_at,details_json)
                   VALUES (?,?, 'succeeded',?,?,?)""",
                [
                    (
                        "non_durable_noise",
                        f"noise:{index:03d}",
                        AT,
                        AT,
                        json.dumps(
                            {
                                "contract_version": "unrelated-receipt-v1",
                                "index": index,
                            },
                            sort_keys=True,
                        ),
                    )
                    for index in range(500)
                ],
            )
        before_read = self._ledger_counts()
        after = pipeline.pipeline_summary(db_path=self.db, at=AT)
        self.assertEqual(self._ledger_counts(), before_read)

        self.assertIn(durable_id, {row["id"] for row in after["runs"]})
        self.assertEqual(after["discovery_coverage"], before["discovery_coverage"])
        self.assertEqual(after["discovery_complete"], before["discovery_complete"])
        self.assertFalse(after["discovery_complete"])


if __name__ == "__main__":
    unittest.main()
