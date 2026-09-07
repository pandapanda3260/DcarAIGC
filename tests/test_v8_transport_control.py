from __future__ import annotations

import json
import unittest
from dataclasses import asdict
from unittest import mock

from tests import test_v8_transport_runner as runner_fixture
from tikhub_config import resolve_tikhub_transport_manifest
from v8 import durable_runs, paid_drain, profile_control, transport_natural_due
from v8.profile_control import (
    ProfileControlError,
    _claim_current_hold_command,
    current_activation_hold_command_status,
    enqueue_current_activation_hold_command,
    process_current_activation_hold_commands,
)
from v8.source_routing import parse_time
from v8.storage import connect, transaction
from v8.transport_accounting import settle_primary_member_unknown
from v8.transport_campaign import (
    TransportCampaignError,
    freeze_control_transport_campaign,
)
from v8.transport_members import OPERATOR_JOB, _current_campaign, primary_operator_identity
from v8.transport_owner_evidence import read_primary_campaign_owners
from v8.transport_receipts import read_transport_receipt
from v8.transport_verdict import record_primary_route_verdict


AT = runner_fixture.AT


class TransportControlTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = runner_fixture.TransportRunnerTest(methodName="runTest")
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()
        self.db = self.fixture.db
        self.mirror_root = self.fixture.fixture.fixture.mirror_root
        self.enterContext(mock.patch.object(profile_control, "now_utc", return_value=AT))

    def _binding(self, *, base: str, stack: str) -> dict:
        path = self.fixture.fixture.fixture.root / f"{base.rsplit('.', 1)[-1]}-{stack}.env"
        path.write_text(
            f"TIKHUB_API_BASE={base}\nTIKHUB_HTTP_STACK={stack}\n",
            encoding="utf-8",
        )
        path.chmod(0o600)
        return {
            "manifest": resolve_tikhub_transport_manifest(path, honor_environment=False),
            "config_path": str(path),
            "honor_environment": False,
        }

    def _failed_primary_verdict(self) -> int:
        self.fixture.fail_rank = 1
        terminal = self.fixture._run()["receipt"]
        with connect(self.db) as connection, transaction(connection):
            settle_primary_member_unknown(
                connection,
                terminal["payload"]["members"][0]["receipt_id"],
                scheduler=self.fixture.scheduler,
                at=AT,
                mirror_root=self.mirror_root,
            )
            verdict = record_primary_route_verdict(
                connection,
                self.fixture.fixture.fixture.campaign["receipt_id"],
                at=AT,
                mirror_root=self.mirror_root,
            )
        self.assertEqual(verdict["payload"]["status"], "failed")
        self.assertEqual(verdict["payload"]["next_action"], "run_disjoint_control_arms")
        return int(verdict["receipt_id"])

    def test_control_campaigns_require_failed_primary_verdict_and_fixed_routes(self) -> None:
        source_verdict_id = self._failed_primary_verdict()
        with connect(self.db) as connection, transaction(connection):
            io_campaign = freeze_control_transport_campaign(
                connection,
                drain_id="transport-cohort-hold",
                source_verdict_receipt_id=source_verdict_id,
                arm="control_io",
                request_transport=self._binding(
                    base="https://api.tikhub.io",
                    stack="urllib-stream-v1",
                ),
                at=AT,
                mirror_root=self.mirror_root,
            )
            legacy_campaign = freeze_control_transport_campaign(
                connection,
                drain_id="transport-cohort-hold",
                source_verdict_receipt_id=source_verdict_id,
                arm="control_legacy",
                request_transport=self._binding(
                    base="https://api.tikhub.dev",
                    stack="urllib-legacy-v1",
                ),
                at=AT,
                mirror_root=self.mirror_root,
            )

        self.assertEqual(io_campaign["payload"]["arm"], "control_io")
        self.assertEqual(
            io_campaign["payload"]["request_transport"]["manifest"]["request_host"],
            "api.tikhub.io",
        )
        self.assertEqual(legacy_campaign["payload"]["arm"], "control_legacy")
        self.assertEqual(
            legacy_campaign["payload"]["request_transport"]["manifest"]["http_stack"],
            "urllib-legacy-v1",
        )

    def test_control_campaign_rejects_wrong_arm_route(self) -> None:
        source_verdict_id = self._failed_primary_verdict()
        with connect(self.db) as connection, transaction(connection):
            with self.assertRaises(TransportCampaignError) as caught:
                freeze_control_transport_campaign(
                    connection,
                    drain_id="transport-cohort-hold",
                    source_verdict_receipt_id=source_verdict_id,
                    arm="control_legacy",
                    request_transport=self._binding(
                        base="https://api.tikhub.io",
                        stack="urllib-stream-v1",
                    ),
                    at=AT,
                    mirror_root=self.mirror_root,
                )
        self.assertEqual(caught.exception.code, "transport_campaign_control_route_invalid")

    def test_control_campaign_accepts_previous_generation_primary_verdict(self) -> None:
        source_verdict_id = self._failed_primary_verdict()
        with connect(self.db) as connection, transaction(connection):
            source_verdict = read_transport_receipt(connection, source_verdict_id)
            current_hold = dict(source_verdict["payload"]["hold_binding"])
            current_hold["generation"] += 1
            current_hold["build_receipt_sha256"] = "1" * 64
            current_hold["runtime_root_receipt_sha256"] = "2" * 64
            current_hold["config_receipt_sha256"] = "3" * 64
            with mock.patch(
                "v8.transport_campaign.read_current_diagnostic_hold",
                return_value=current_hold,
            ):
                campaign = freeze_control_transport_campaign(
                    connection,
                    drain_id="transport-cohort-hold",
                    source_verdict_receipt_id=source_verdict_id,
                    arm="control_io",
                    request_transport=self._binding(
                        base="https://api.tikhub.io",
                        stack="urllib-stream-v1",
                    ),
                    at=AT,
                    mirror_root=self.mirror_root,
                )
            with mock.patch(
                "v8.transport_members.read_current_diagnostic_hold",
                return_value=current_hold,
            ):
                reloaded = _current_campaign(connection, campaign["receipt_id"], AT)

        self.assertEqual(campaign["payload"]["hold_binding"], current_hold)
        self.assertEqual(
            campaign["payload"]["source_verdict_receipt_id"],
            source_verdict_id,
        )
        self.assertEqual(reloaded["receipt_id"], campaign["receipt_id"])

    def test_control_operator_due_time_accepts_control_command_owner(self) -> None:
        source_verdict_id = self._failed_primary_verdict()
        queued = enqueue_current_activation_hold_command(
            db_path=self.db,
            command_id="control-io-due-owner",
            command="transport_control",
            parameters={
                "drain_id": "transport-cohort-hold",
                "source_verdict_receipt_id": source_verdict_id,
                "arm": "control_io",
            },
        )
        self.assertEqual(queued["status"], "queued")
        claim = _claim_current_hold_command(self.db)
        assert claim is not None
        with connect(self.db) as connection, transaction(connection):
            campaign = freeze_control_transport_campaign(
                connection,
                drain_id="transport-cohort-hold",
                source_verdict_receipt_id=source_verdict_id,
                arm="control_io",
                request_transport=self._binding(
                    base="https://api.tikhub.io",
                    stack="urllib-stream-v1",
                ),
                at=AT,
                mirror_root=self.mirror_root,
            )
            operator = durable_runs.claim_run_in_transaction(
                connection,
                OPERATOR_JOB,
                primary_operator_identity(campaign),
                invocation_source="operator_retry",
                now=AT,
            )
            assert operator is not None
            details = {
                **claim["details"],
                "control_operator": {
                    "arm": "control_io",
                    "campaign_receipt_id": campaign["receipt_id"],
                    "claim": asdict(operator),
                },
            }
            connection.execute(
                "UPDATE scheduler_runs SET details_json=? WHERE id=?",
                (
                    json.dumps(details, sort_keys=True, separators=(",", ":")),
                    claim["run_id"],
                ),
            )
            binding = {
                "command_run_id": claim["run_id"],
                "command_attempt_id": claim["attempt_id"],
                "command_sha256": details["command_sha256"],
                "campaign_receipt_id": campaign["receipt_id"],
                "operator_claim": asdict(operator),
            }
            scheduled = transport_natural_due._operator_due_time(
                connection,
                binding,
                now=parse_time(AT),
                require_running=True,
            )
            attempt_started_at = connection.execute(
                "SELECT started_at FROM scheduler_run_attempts WHERE id=?",
                (claim["attempt_id"],),
            ).fetchone()["started_at"]

        self.assertEqual(scheduled, parse_time(attempt_started_at))

    def test_control_prepare_failure_writes_zero_start_terminal_for_tail(self) -> None:
        source_verdict_id = self._failed_primary_verdict()
        queued = enqueue_current_activation_hold_command(
            db_path=self.db,
            command_id="control-io-prepared-failure",
            command="transport_control",
            parameters={
                "drain_id": "transport-cohort-hold",
                "source_verdict_receipt_id": source_verdict_id,
                "arm": "control_io",
            },
        )
        with mock.patch(
            "v8.transport_preparation.prepare_primary_due_inventory",
            side_effect=transport_natural_due.NaturalDueError(
                "natural_due_operator_invalid",
                "fixture control due failure",
            ),
        ):
            processed = process_current_activation_hold_commands(
                db_path=self.db,
                mirror_root=self.mirror_root,
                scheduler=self.fixture.scheduler,
            )

        self.assertEqual(processed["processed"][0]["status"], "failed", processed)
        self.assertEqual(
            processed["processed"][0]["error"]["code"],
            "natural_due_operator_invalid",
        )
        self.assertEqual(self.fixture.calls, list(range(1, 21)))
        with connect(self.db) as connection, transaction(connection):
            status = current_activation_hold_command_status(
                connection,
                run_id=queued["run_id"],
            )
            details = json.loads(connection.execute(
                "SELECT details_json FROM scheduler_runs WHERE id=?",
                (queued["run_id"],),
            ).fetchone()[0])
            campaign_id = details["control_operator"]["campaign_receipt_id"]
            terminal_row = connection.execute(
                "SELECT id FROM scheduler_runs WHERE job_id='transport_receipt:campaign_terminal' "
                "AND scheduled_for=?",
                (f"control_io-execution:{campaign_id}",),
            ).fetchone()
            self.assertIsNotNone(terminal_row)
            terminal = read_transport_receipt(connection, terminal_row["id"])
            self.assertEqual(terminal["payload"]["effective_starts"], 0)
            self.assertEqual(terminal["payload"]["charged_microusd"], 0)
            self.assertFalse(terminal["payload"]["sample_complete"])
            self.assertEqual(terminal["payload"]["members"], [])
            self.assertEqual(terminal["payload"]["failure"]["code"], "natural_due_operator_invalid")
            owners = read_primary_campaign_owners(connection, campaign_id, at=AT)
            self.assertEqual(owners["terminal_receipt_id"], terminal["receipt_id"])
            self.assertEqual(owners["members"], [])
            self.assertEqual(
                owners["paid_run_ids"],
                [details["control_operator"]["claim"]["scheduler_run_id"]],
            )
            tail = paid_drain.verify_profile_drain_sealable(
                connection,
                self.fixture.fixture.fixture.campaign["payload"]["hold_binding"]["drain_id"],
                now=AT,
            )
        self.assertEqual(status["error"]["code"], "natural_due_operator_invalid")
        self.assertIn(terminal["receipt_id"], tail["diagnostic_tail"]["campaign_terminal_receipt_ids"])

    def test_zero_start_close_remains_consistent_when_receipt_write_crosses_second(self) -> None:
        from v8 import transport_runner

        clock = [AT]
        append = transport_runner.append_transport_receipt

        def slow_append(*args, **kwargs):
            result = append(*args, **kwargs)
            if (kwargs.get("kind") == "campaign_terminal"
                    and kwargs.get("payload", {}).get("effective_starts") == 0):
                from datetime import timedelta
                clock[0] = (parse_time(AT) + timedelta(seconds=1)).isoformat()
            return result

        with (
            mock.patch.object(transport_runner, "now_utc", side_effect=lambda: clock[0]),
            mock.patch.object(transport_runner, "append_transport_receipt", side_effect=slow_append),
        ):
            self.test_control_prepare_failure_writes_zero_start_terminal_for_tail()

    def test_control_legacy_unknown_uses_arm_terminal_for_accounting(self) -> None:
        source_verdict_id = self._failed_primary_verdict()
        self.fixture.fixture.candidates = []
        self.fixture.calls = []
        self.fixture.fail_rank = 1
        queued = enqueue_current_activation_hold_command(
            db_path=self.db,
            command_id="control-legacy-unknown-accounting",
            command="transport_control",
            parameters={
                "drain_id": "transport-cohort-hold",
                "source_verdict_receipt_id": source_verdict_id,
                "arm": "control_legacy",
            },
        )

        processed = process_current_activation_hold_commands(
            db_path=self.db,
            mirror_root=self.mirror_root,
            scheduler=self.fixture.scheduler,
        )

        self.assertEqual(processed["processed"][0]["status"], "succeeded", processed)
        self.assertEqual(self.fixture.calls, list(range(1, 21)))
        with connect(self.db) as connection:
            status = current_activation_hold_command_status(
                connection,
                run_id=queued["run_id"],
            )
            result = status["result"]
            campaign_id = result["campaign_receipt_id"]
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["effective_starts"], 20)
            self.assertEqual(result["original_billing_unknown_count"], 1)
            self.assertEqual(result["accounted_microusd"], 20_000)
            usage = connection.execute(
                "SELECT details_json FROM provider_usage "
                "WHERE json_extract(details_json,'$.diagnostic_member.campaign_receipt_id')=? "
                "AND json_extract(details_json,'$.diagnostic_member.rank')=1",
                (campaign_id,),
            ).fetchone()
            metadata = json.loads(usage["details_json"])
            self.assertEqual(metadata["state"], "charged_unverified")
            member_id = metadata["diagnostic_member"]["receipt_id"]
            accounting = read_transport_receipt(
                connection,
                metadata["diagnostic_accounting"]["receipt_id"],
            )
            self.assertEqual(
                accounting["identity_key"],
                f"control_legacy-unknown:{member_id}",
            )
            self.assertEqual(
                accounting["payload"]["campaign_terminal_id"],
                result["campaign_terminal_id"],
            )

    def test_control_command_can_finalize_existing_terminal_without_repurchase(self) -> None:
        source_verdict_id = self._failed_primary_verdict()
        self.fixture.fixture.candidates = []
        self.fixture.calls = []
        self.fixture.fail_rank = 1
        first = enqueue_current_activation_hold_command(
            db_path=self.db,
            command_id="control-legacy-fails-after-terminal",
            command="transport_control",
            parameters={
                "drain_id": "transport-cohort-hold",
                "source_verdict_receipt_id": source_verdict_id,
                "arm": "control_legacy",
            },
        )
        with mock.patch(
            "v8.transport_accounting.settle_primary_member_unknown",
            side_effect=RuntimeError("fixture accounting unavailable"),
        ):
            failed = process_current_activation_hold_commands(
                db_path=self.db,
                mirror_root=self.mirror_root,
                scheduler=self.fixture.scheduler,
            )

        self.assertEqual(failed["processed"][0]["status"], "failed", failed)
        self.assertEqual(self.fixture.calls, list(range(1, 21)))
        call_count = len(self.fixture.calls)
        with connect(self.db) as connection:
            failed_status = current_activation_hold_command_status(
                connection,
                run_id=first["run_id"],
            )
            self.assertEqual(
                failed_status["error"]["message"],
                "fixture accounting unavailable",
            )
            terminal_count = connection.execute(
                "SELECT COUNT(*) FROM scheduler_runs "
                "WHERE job_id='transport_receipt:campaign_terminal' "
                "AND scheduled_for LIKE 'control_legacy-execution:%'",
            ).fetchone()[0]
            self.assertEqual(terminal_count, 1)

        second = enqueue_current_activation_hold_command(
            db_path=self.db,
            command_id="control-legacy-finalize-existing-terminal",
            command="transport_control",
            parameters={
                "drain_id": "transport-cohort-hold",
                "source_verdict_receipt_id": source_verdict_id,
                "arm": "control_legacy",
            },
        )
        succeeded = process_current_activation_hold_commands(
            db_path=self.db,
            mirror_root=self.mirror_root,
            scheduler=self.fixture.scheduler,
        )

        self.assertEqual(succeeded["processed"][0]["status"], "succeeded", succeeded)
        self.assertEqual(len(self.fixture.calls), call_count)
        with connect(self.db) as connection:
            status = current_activation_hold_command_status(
                connection,
                run_id=second["run_id"],
            )
            result = status["result"]
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["original_billing_unknown_count"], 1)
            self.assertEqual(result["accounted_microusd"], 20_000)
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM scheduler_runs "
                    "WHERE job_id='transport_receipt:campaign_terminal' "
                    "AND scheduled_for LIKE 'control_legacy-execution:%'",
                ).fetchone()[0],
                1,
            )

    def test_current_hold_command_validation_accepts_only_fixed_control_arms(self) -> None:
        queued = enqueue_current_activation_hold_command(
            db_path=self.db,
            command_id="control-io-1",
            command="transport_control",
            parameters={
                "drain_id": "transport-cohort-hold",
                "source_verdict_receipt_id": 1,
                "arm": "control_io",
            },
        )
        self.assertEqual(queued["status"], "queued")
        with self.assertRaises(ProfileControlError):
            enqueue_current_activation_hold_command(
                db_path=self.db,
                command_id="control-bad-1",
                command="transport_control",
                parameters={
                    "drain_id": "transport-cohort-hold",
                    "source_verdict_receipt_id": 1,
                    "arm": "control_extra",
                },
            )

if __name__ == "__main__":
    unittest.main()
