from __future__ import annotations

import json
import unittest
from unittest.mock import Mock, patch

from tests import test_v8_pipeline as pipeline_fixtures
from v8 import capture, pipeline, provider_budget, providers
from v8.capture import ProviderResult
from v8.storage import connect, transaction


AT = pipeline_fixtures.AT
LATER = pipeline_fixtures.LATER
AFTER = pipeline_fixtures.AFTER


class PipelineWorkReadinessTest(unittest.TestCase):
    # Reuse the established real temporary-DB setup without inheriting the
    # original TestCase (which would make unittest collect its full suite here).
    setUp = pipeline_fixtures.PipelineTest.setUp
    account_row = pipeline_fixtures.PipelineTest.account_row
    roster = pipeline_fixtures.PipelineTest.roster
    activate = pipeline_fixtures.PipelineTest.activate
    content = pipeline_fixtures.PipelineTest.content

    def _patch_clock_and_raw_root(self) -> None:
        raw_root = patch.object(capture, "RAW_ROOT", self.root / "raw")
        raw_root.start()
        self.addCleanup(raw_root.stop)
        for module in (
            "capture",
            "providers",
            "provider_updates",
            "provider_budget",
        ):
            current = patch(f"v8.{module}.now_utc", return_value=AT)
            current.start()
            self.addCleanup(current.stop)

    def _ledger_counts(self, job_id: str) -> dict[str, int]:
        with connect(self.db) as connection:
            return {
                "queue_attempts": int(
                    connection.execute(
                        "SELECT COUNT(*) FROM scheduler_run_attempts a "
                        "JOIN scheduler_runs r ON r.id=a.scheduler_run_id "
                        "WHERE r.job_id=?",
                        (job_id,),
                    ).fetchone()[0]
                ),
                "usage": int(
                    connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0]
                ),
                "paid_markers": int(
                    connection.execute(
                        "SELECT COUNT(*) FROM paid_provider_dispatch_events"
                    ).fetchone()[0]
                ),
                "blocked_receipts": int(
                    connection.execute(
                        "SELECT COUNT(*) FROM scheduler_runs "
                        "WHERE job_id LIKE 'work_readiness_blocked:%'"
                    ).fetchone()[0]
                ),
            }

    def _paid_ledger_rows(self, job_id: str) -> dict[str, list[dict]]:
        with connect(self.db) as connection:
            return {
                "queue_attempts": [
                    dict(row)
                    for row in connection.execute(
                        "SELECT a.* FROM scheduler_run_attempts a "
                        "JOIN scheduler_runs r ON r.id=a.scheduler_run_id "
                        "WHERE r.job_id=? ORDER BY a.id",
                        (job_id,),
                    )
                ],
                "usage": [
                    dict(row)
                    for row in connection.execute(
                        "SELECT * FROM provider_usage ORDER BY id"
                    )
                ],
                "paid_markers": [
                    dict(row)
                    for row in connection.execute(
                        "SELECT * FROM paid_provider_dispatch_events ORDER BY id"
                    )
                ],
            }

    def _open_fault(
        self,
        *,
        scope_kind: str,
        operation: str | None = None,
        fault_class: str,
        reason: str,
    ) -> dict:
        with connect(self.db) as connection, transaction(connection):
            return provider_budget.record_fault_state(
                connection,
                scope_kind=scope_kind,
                operation=operation,
                fault_class=fault_class,
                reason=reason,
                usage_id=None,
                at=AT,
                state_evidence={"fixture": reason},
            )

    def _assert_repeated_detail_block_does_not_start_paid_work(
        self, *, scope_kind: str, fault_class: str, reason: str
    ) -> None:
        self.activate()
        content = self.content()
        self._open_fault(
            scope_kind=scope_kind,
            operation=("douyin_video_detail" if scope_kind == "operation" else None),
            fault_class=fault_class,
            reason=reason,
        )
        local = Mock(
            return_value={
                "terminal_ids": [],
                "pending_ids": [content["id"]],
                "errors": [],
                "blocked_media": {},
            }
        )

        first = pipeline.run_content_batch(
            "content_pipeline", db_path=self.db, at=AT, local_runner=local
        )
        after_first = self._ledger_counts("content_pipeline")
        second = pipeline.run_content_batch(
            "content_pipeline", db_path=self.db, at=LATER, local_runner=local
        )
        after_second = self._ledger_counts("content_pipeline")

        self.assertTrue(first.get("blocked_work"), first)
        self.assertTrue(second.get("blocked_work"), second)
        self.assertEqual(after_first["queue_attempts"], 0)
        self.assertEqual(after_first["usage"], 0)
        self.assertEqual(after_first["paid_markers"], 0)
        self.assertEqual(after_second, after_first)
        self.assertEqual(after_first["blocked_receipts"], 1)
        self.assertEqual(local.call_count, 2)
        self.assertEqual(first["local"][str(content["id"])], {"status": "partial"})
        self.assertEqual(second["local"][str(content["id"])], {"status": "partial"})

    def test_provider_block_repeated_tick_does_not_create_paid_queue_attempts(self):
        self._assert_repeated_detail_block_does_not_start_paid_work(
            scope_kind="provider_hard",
            fault_class="balance",
            reason="provider_balance_blocked",
        )

    def test_operation_block_repeated_tick_does_not_create_paid_queue_attempts(self):
        self._assert_repeated_detail_block_does_not_start_paid_work(
            scope_kind="operation",
            fault_class="field_contract",
            reason="field_contract_invalid",
        )

    def _recovery_receipt(self, contract: str, details: dict) -> int:
        payload = {
            "contract_version": contract,
            "issued_at": AT,
            "expires_at": "2026-08-30T00:00:00Z",
            **details,
        }
        with connect(self.db) as connection, transaction(connection):
            cursor = connection.execute(
                "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,"
                "completed_at,details_json) VALUES (?,?,'succeeded',?,?,?)",
                (
                    f"fixture:{contract}",
                    f"fixture:{contract}",
                    AT,
                    AT,
                    json.dumps(payload, sort_keys=True),
                ),
            )
            return int(cursor.lastrowid or 0)

    def _resolve_field_contract_fault(self, fault: dict) -> None:
        operation = str(fault["operation"])
        policy_sha256 = "a" * 64
        replay_id = self._recovery_receipt(
            "field-policy-replay-v1",
            {"operation": operation, "policy_sha256": policy_sha256, "passed": True},
        )
        canary_id = self._recovery_receipt(
            "field-policy-canary-v1",
            {"operation": operation, "policy_sha256": policy_sha256, "passed": True},
        )
        policy_id = self._recovery_receipt(
            "field-policy-release-v1",
            {
                "operation": operation,
                "policy_sha256": policy_sha256,
                "replay_receipt_id": replay_id,
                "canary_receipt_id": canary_id,
                "fault_generation": fault["generation"],
                "fault_fingerprint": fault["state_fingerprint"],
            },
        )
        with connect(self.db) as connection, transaction(connection):
            recovered = provider_budget.resolve_operation_fault(
                connection,
                operation=operation,
                fault_class=str(fault["fault_class"]),
                recovery_evidence={
                    "contract_version": "field-contract-recovery-v1",
                    "fault_generation": fault["generation"],
                    "fault_fingerprint": fault["state_fingerprint"],
                    "policy_receipt_id": policy_id,
                },
                at=AFTER,
            )
        self.assertTrue(recovered["fault_closed"])

    def test_ready_metric_group_runs_while_blocked_group_stays_pending_then_resumes(self):
        self._patch_clock_and_raw_root()
        self.activate()
        content = self.content()
        fault = self._open_fault(
            scope_kind="operation",
            operation="douyin_video_detail",
            fault_class="field_contract",
            reason="field_contract_invalid",
        )
        calls: list[str] = []

        def metric_call(group: str, current: dict) -> ProviderResult:
            calls.append(group)
            if group == "detail_counts":
                raw = {
                    "code": 200,
                    "data": {
                        "status_code": 0,
                        "aweme_detail": {
                            "aweme_id": current["platform_content_id"],
                            "author": {"uid": "100000000001"},
                            "statistics": {
                                "comment_count": 3,
                                "collect_count": 2,
                                "digg_count": 9,
                                "share_count": 5,
                            },
                        },
                    },
                }
                parsed = providers._parse_douyin_stage_payload(
                    "detail", current["platform_content_id"], raw
                )
                return ProviderResult(parsed.data["metrics"], raw, 200, True)
            raw = {
                "code": 200,
                "data": {
                    "status_code": 0,
                    "statistics_list": [
                        {
                            "aweme_id": current["platform_content_id"],
                            "play_count": 88,
                            "digg_count": 9,
                            "share_count": 5,
                        }
                    ],
                },
            }
            return providers._parse_douyin_stage_payload(
                "metrics", current["platform_content_id"], raw
            )

        first = pipeline.run_content_batch(
            "metrics_backfill",
            db_path=self.db,
            at=AT,
            call_override=metric_call,
        )

        self.assertEqual(calls, ["statistics"])
        self.assertFalse(first["complete"])
        checkpoint = first["details"]["checkpoint"]
        self.assertEqual(checkpoint["pending_ids"], [content["id"]])
        self.assertNotIn("platform", checkpoint["items"][0])
        self.assertNotIn("published_at", checkpoint["items"][0])
        self.assertEqual(
            checkpoint["items"][0]["cycle_key"],
            checkpoint["results"][str(content["id"])]["cycle_key"],
        )
        self.assertEqual(
            checkpoint["results"][str(content["id"])]["deferred_groups"],
            ["detail_counts"],
        )
        with connect(self.db) as connection:
            operations = [
                row["operation"]
                for row in connection.execute(
                    "SELECT operation FROM provider_usage ORDER BY id"
                )
            ]
            applied = connection.execute(
                "SELECT fs.status,pr.source FROM fetch_slots fs "
                "JOIN fetch_attempts fa ON fa.slot_id=fs.id "
                "JOIN provider_raw_responses pr ON pr.fetch_attempt_id=fa.id "
                "WHERE fs.content_id=? AND pr.operation='douyin_video_statistics'",
                (content["id"],),
            ).fetchone()
        self.assertEqual(operations, ["douyin_video_statistics"])
        self.assertEqual(tuple(applied), ("succeeded", "live_applied"))

        before_blocked_resume = self._paid_ledger_rows("metrics_backfill")
        still_blocked = pipeline.run_content_batch(
            "metrics_backfill",
            db_path=self.db,
            at=LATER,
            resume_run_id=first["scheduler_run_id"],
            call_override=metric_call,
        )
        after_blocked_resume = self._paid_ledger_rows("metrics_backfill")

        self.assertTrue(still_blocked.get("blocked_work"), still_blocked)
        self.assertEqual(still_blocked["scheduler_run_id"], first["scheduler_run_id"])
        self.assertEqual(calls, ["statistics"])
        self.assertEqual(after_blocked_resume, before_blocked_resume)

        self._resolve_field_contract_fault(fault)
        resumed = pipeline.run_content_batch(
            "metrics_backfill",
            db_path=self.db,
            at=AFTER,
            resume_run_id=first["scheduler_run_id"],
            call_override=metric_call,
        )

        self.assertEqual(calls, ["statistics", "detail_counts"])
        self.assertTrue(resumed["complete"], resumed)
        self.assertEqual(resumed["details"]["checkpoint"]["pending_ids"], [])
        with connect(self.db) as connection:
            operations = [
                row["operation"]
                for row in connection.execute(
                    "SELECT operation FROM provider_usage ORDER BY id"
                )
            ]
        self.assertEqual(
            operations, ["douyin_video_statistics", "douyin_video_detail"]
        )


if __name__ == "__main__":
    unittest.main()
