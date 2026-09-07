from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from v8 import capture, provider_budget
from v8.provider_budget import (
    fault_state,
    record_fault_state,
    resolve_fault_state,
)
from v8.storage import connect, initialize_database, transaction
from v8.work_readiness import (
    WorkReadinessPass,
    assess_work_readiness,
    record_work_blocked,
)


AT = "2026-08-28T15:59:59Z"
NEXT_DAY = "2026-08-28T16:00:00Z"


class WorkReadinessTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "readiness.sqlite3"
        with connect(self.db) as connection:
            initialize_database(connection)
            connection.execute(
                """INSERT INTO accounts(
                       id,phone,phone_normalized,operator_name,account_type,
                       content_direction,enabled,created_at,updated_at)
                   VALUES (1,'',NULL,'','unknown','unknown',1,?,?)""",
                (AT, AT),
            )
            connection.execute(
                """INSERT INTO account_platform_identities(
                       id,account_id,platform,uid,nickname,source,created_at,updated_at)
                   VALUES (1,1,'douyin','10000001','test','manual',?,?)""",
                (AT, AT),
            )
            connection.execute(
                """INSERT INTO content_items(
                       id,link_id,platform,platform_content_id,canonical_url,
                       account_id,raw_account_uid,imported_at,created_at,updated_at)
                   VALUES (1,'C00001','douyin','1','https://example.com/1',
                           1,'10000001',?,?,?)""",
                (AT, AT, AT),
            )
            connection.commit()

    def tearDown(self) -> None:
        self.temp.cleanup()

    @staticmethod
    def _assess(
        connection,
        *,
        operation: str = "douyin_video_detail",
        category: str = "detail",
        at: str = AT,
        stage: str | None = None,
        window_key: str | None = None,
    ) -> dict:
        return assess_work_readiness(
            connection,
            operation=operation,
            category=category,
            at=at,
            content_id=1,
            stage=stage,
            window_key=window_key,
        )

    def test_authorization_recovery_changes_generation_and_receipt_is_idempotent(
        self,
    ) -> None:
        with connect(self.db) as connection:
            with transaction(connection):
                record_fault_state(
                    connection,
                    scope_kind="authorization_hard",
                    authorization_id=1,
                    fault_class="account_authorization",
                    reason="authorization_scope_missing",
                    usage_id=None,
                    at=AT,
                    state_evidence={"scope": "video.list"},
                )
            blocked = self._assess(connection)
            repeated = self._assess(connection)
            self.assertFalse(blocked["runnable"])
            self.assertEqual(blocked["reason"], "authorization_hard")
            self.assertEqual(
                blocked["readiness_generation"], repeated["readiness_generation"]
            )

            with transaction(connection):
                first = record_work_blocked(
                    connection, assessment=blocked, at=AT
                )
                duplicate = record_work_blocked(
                    connection, assessment=blocked, at=AT
                )
            self.assertTrue(first["recorded"])
            self.assertFalse(duplicate["recorded"])
            self.assertEqual(first["run_id"], duplicate["run_id"])
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM scheduler_run_attempts "
                    "WHERE scheduler_run_id=?",
                    (first["run_id"],),
                ).fetchone()[0],
                0,
            )

            current = fault_state(
                connection,
                scope_kind="authorization_hard",
                authorization_id=1,
                fault_class="account_authorization",
            )
            with transaction(connection):
                health = connection.execute(
                    """INSERT INTO scheduler_runs(
                           job_id,scheduled_for,status,started_at,completed_at,details_json)
                       VALUES ('authorization_health:test',?,'succeeded',?,?, '{}')""",
                    (NEXT_DAY, NEXT_DAY, NEXT_DAY),
                )
                self.assertTrue(
                    resolve_fault_state(
                        connection,
                        scope_kind="authorization_hard",
                        authorization_id=1,
                        fault_class="account_authorization",
                        expected_generation=current["generation"],
                        expected_fingerprint=current["state_fingerprint"],
                        evidence_id=int(health.lastrowid or 0),
                        at=NEXT_DAY,
                    )
                )
            ready = self._assess(connection, at=NEXT_DAY)
            self.assertTrue(ready["runnable"])
            self.assertEqual(ready["reason"], "ready")
            self.assertNotEqual(
                blocked["readiness_generation"], ready["readiness_generation"]
            )

            with transaction(connection):
                record_fault_state(
                    connection,
                    scope_kind="authorization_hard",
                    authorization_id=1,
                    fault_class="account_authorization",
                    reason="authorization_refresh_failed",
                    usage_id=None,
                    at="2026-08-28T16:01:00Z",
                    state_evidence={"refresh": "failed"},
                )
            reopened = self._assess(connection, at="2026-08-28T16:01:00Z")
            self.assertFalse(reopened["runnable"])
            self.assertNotEqual(
                blocked["readiness_generation"], reopened["readiness_generation"]
            )

    def test_operation_fault_isolated_and_unrelated_fault_does_not_drift(self) -> None:
        with connect(self.db) as connection:
            with transaction(connection):
                record_fault_state(
                    connection,
                    scope_kind="operation",
                    operation="douyin_video_detail",
                    fault_class="rate_limit",
                    reason="http_429",
                    usage_id=None,
                    at=AT,
                    state_evidence={"quota_window": "detail-1"},
                )
            detail = self._assess(connection)
            statistics = self._assess(
                connection,
                operation="douyin_video_statistics",
                category="metrics",
            )
            self.assertEqual(detail["reason"], "operation_blocked")
            self.assertEqual(detail["gate_scope"]["operation"], "douyin_video_detail")
            self.assertTrue(statistics["runnable"])

            with transaction(connection):
                record_fault_state(
                    connection,
                    scope_kind="operation",
                    operation="douyin_video_statistics",
                    fault_class="field_contract",
                    reason="field_contract_invalid",
                    usage_id=None,
                    at=NEXT_DAY,
                    state_evidence={"contract_hash": "statistics-1"},
                )
            detail_after_unrelated_fault = self._assess(connection)
            self.assertEqual(
                detail["readiness_generation"],
                detail_after_unrelated_fault["readiness_generation"],
            )

    def test_storage_fault_is_global_and_legacy_provider_circuit_is_compatible(
        self,
    ) -> None:
        with connect(self.db) as connection:
            with transaction(connection):
                record_fault_state(
                    connection,
                    scope_kind="storage_hard",
                    fault_class="capacity",
                    reason="archive_capacity_critical",
                    usage_id=None,
                    at=AT,
                    state_evidence={"volume": "archive"},
                )
            for operation, category in (
                ("douyin_video_detail", "detail"),
                ("douyin_user_posts", "reconcile"),
            ):
                blocked = self._assess(
                    connection, operation=operation, category=category
                )
                self.assertEqual(blocked["reason"], "storage_hard")
                self.assertEqual(blocked["gate_scope"]["scope_kind"], "storage_hard")
                self.assertEqual(blocked["gate_scope"]["provider"], "all")

        legacy_db = Path(self.temp.name) / "legacy-circuit.sqlite3"
        with connect(legacy_db) as connection:
            initialize_database(connection)
            connection.execute(
                """INSERT INTO scheduler_runs(
                       job_id,scheduled_for,status,started_at,completed_at,details_json)
                   VALUES ('provider_circuit:tikhub',?,'succeeded',?,?,?)""",
                (
                    AT,
                    AT,
                    AT,
                    json.dumps(
                        {
                            "contract_version": "provider-circuit-v1",
                            "provider": "TikHub",
                            "open": True,
                            "generation": "legacy-1",
                            "reason": "provider_balance_blocked",
                            "opened_at": AT,
                            "last_failure_at": AT,
                        },
                        sort_keys=True,
                    ),
                ),
            )
            connection.commit()
            blocked = assess_work_readiness(
                connection,
                operation="douyin_video_detail",
                category="detail",
                at=AT,
            )
            self.assertEqual(blocked["reason"], "provider_circuit_open")
            self.assertEqual(blocked["gate_scope"]["scope_kind"], "provider_hard")

    def test_budget_boundary_changes_generation_at_beijing_midnight(self) -> None:
        with connect(self.db) as connection:
            with transaction(connection):
                connection.execute(
                    """INSERT INTO provider_usage(
                           provider,operation,request_attempts,billed_requests,
                           currency,amount,recorded_at,details_json)
                       VALUES ('TikHub','douyin_video_statistics',1,1,
                               'USD',15,?,?)""",
                    (
                        AT,
                        json.dumps(
                            {"state": "completed", "category": "metrics"},
                            sort_keys=True,
                        ),
                    ),
                )
            exhausted = self._assess(
                connection,
                operation="douyin_video_statistics",
                category="metrics",
                at=AT,
            )
            reset = self._assess(
                connection,
                operation="douyin_video_statistics",
                category="metrics",
                at=NEXT_DAY,
            )
            self.assertFalse(exhausted["runnable"])
            self.assertEqual(exhausted["reason"], "metrics_budget_exhausted")
            self.assertEqual(exhausted["budget_day"], "2026-08-28")
            self.assertTrue(reset["runnable"])
            self.assertEqual(reset["budget_day"], "2026-08-29")
            self.assertNotEqual(
                exhausted["readiness_generation"], reset["readiness_generation"]
            )

    def test_exact_unresolved_slot_isolation_cache_and_read_only_contract(self) -> None:
        with connect(self.db) as connection:
            with transaction(connection):
                slots = {
                    window: capture.ensure_content_slot(
                        connection,
                        content_id=1,
                        stage="detail",
                        window_key=window,
                        provider="TikHub",
                        adapter_version="test-v1",
                    )
                    for window in (
                        "unknown",
                        "charged",
                        "ready-a",
                        "ready-b",
                    )
                }
                connection.execute(
                    "UPDATE fetch_slots SET last_error_code=? WHERE id=?",
                    ("billing_unknown_retry_blocked", slots["unknown"]),
                )
                for window, state in (
                    ("unknown", "billing_unknown"),
                    ("charged", "charged_unverified"),
                ):
                    connection.execute(
                        """INSERT INTO provider_usage(
                               provider,operation,request_attempts,billed_requests,
                               currency,amount,recorded_at,details_json)
                           VALUES ('TikHub','douyin_video_detail',1,1,
                                   'USD',.001,?,?)""",
                        (
                            AT,
                            json.dumps(
                                {
                                    "state": state,
                                    "slot_id": slots[window],
                                    "category": "detail",
                                    "budget_day": "2026-08-28",
                                    "paid_scope_identity": f"identity-{window}",
                                },
                                sort_keys=True,
                            ),
                        ),
                    )
            tables = (
                "provider_usage",
                "fetch_attempts",
                "paid_provider_dispatch_events",
                "scheduler_runs",
            )
            before = {
                table: connection.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0]
                for table in tables
            }
            readiness = WorkReadinessPass(connection, at=AT)
            with patch(
                "v8.work_readiness.check_reservation",
                wraps=provider_budget.check_reservation,
            ) as checked:
                outcomes = {
                    window: readiness.assess(
                        operation="douyin_video_detail",
                        category="detail",
                        content_id=1,
                        stage="detail",
                        window_key=window,
                    )
                    for window in slots
                }
            self.assertEqual(checked.call_count, 1)
            for window in ("unknown", "charged"):
                self.assertFalse(outcomes[window]["runnable"])
                self.assertEqual(
                    outcomes[window]["reason"], "billing_unknown_retry_blocked"
                )
            for window in ("ready-a", "ready-b"):
                self.assertTrue(outcomes[window]["runnable"])
            after = {
                table: connection.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0]
                for table in tables
            }
            self.assertEqual(before, after)

            with transaction(connection):
                connection.execute(
                    """UPDATE provider_usage
                       SET details_json=json_set(details_json,'$.state','completed')
                       WHERE json_extract(details_json,'$.slot_id')=?""",
                    (slots["unknown"],),
                )
                connection.execute(
                    "UPDATE fetch_slots SET last_error_code=NULL WHERE id=?",
                    (slots["unknown"],),
                )
            recovered = WorkReadinessPass(connection, at=AT).assess(
                operation="douyin_video_detail",
                category="detail",
                content_id=1,
                stage="detail",
                window_key="unknown",
            )
            self.assertTrue(recovered["runnable"])
            self.assertNotEqual(
                outcomes["unknown"]["readiness_generation"],
                recovered["readiness_generation"],
            )

    def test_stage_and_window_must_be_an_exact_pair(self) -> None:
        with connect(self.db) as connection:
            with self.assertRaisesRegex(ValueError, "supplied together"):
                self._assess(connection, stage="detail")


if __name__ == "__main__":
    unittest.main()
