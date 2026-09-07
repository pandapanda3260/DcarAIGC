from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from tests import test_v8_current_activation_hold as hold_fixture
from v8 import profile_control, scheduler
from v8.paid_drain import issue_activation_permit_in_transaction
from v8.profile_activations import TIKHUB_PROFILE, activation_at, append_activation
from v8.storage import connect, transaction


SUBMITTED_AT = "2026-09-07T15:59:59Z"
NEW_ACTIVATION_AT = "2026-09-07T16:00:01Z"
EXECUTED_AT = "2026-09-07T16:00:02Z"
RELEASE_EFFECT_AT = "2026-09-07T16:04:59Z"
RETRY_AT = "2026-09-07T16:05:01Z"

COMMAND_BUILD = "3" * 64
COMMAND_RUNTIME = "4" * 64
NEXT_BUILD = "5" * 64


class CurrentHoldCommandRecoveryTest(unittest.TestCase):
    """Submission fencing and effect-before-finish crash recovery."""

    def setUp(self) -> None:
        self.hold = hold_fixture.CurrentActivationHoldTest(
            "test_missing_current_hold_fails_closed_before_any_paid_dispatch"
        )
        self.addCleanup(self.hold.doCleanups)
        self.hold.setUp()
        self.root = self.hold.root
        self.db = self.hold.db

    def test_hold_begin_fails_if_submission_scope_drifts_at_midnight(self) -> None:
        drain_id = "activation-2-submission-scope-drift"
        queued = profile_control.enqueue_current_activation_hold_command(
            db_path=self.db,
            command_id="hold-begin-submission-scope-drift",
            command="hold_begin",
            parameters={
                "drain_id": drain_id,
                "build_receipt_sha256": COMMAND_BUILD,
                "runtime_root_receipt_sha256": COMMAND_RUNTIME,
                "actor": hold_fixture.ACTOR,
                "reason": "freeze the pre-midnight activation scope",
                "not_before_business_day": "2026-09-09",
            },
            submitted_at=SUBMITTED_AT,
        )

        with connect(self.db) as connection:
            active_at_submission = activation_at(connection, SUBMITTED_AT)
            self.assertIsNotNone(active_at_submission)
            assert active_at_submission is not None
            predecessor = connection.execute(
                "SELECT * FROM pipeline_paid_drain_events "
                "WHERE target_activation_id=? AND event_type='release' "
                "AND julianday(created_at)<=julianday(?) ORDER BY id DESC LIMIT 1",
                (active_at_submission["activation_id"], SUBMITTED_AT),
            ).fetchone()
            self.assertIsNotNone(predecessor)
            assert predecessor is not None
            command_row = connection.execute(
                "SELECT details_json FROM scheduler_runs WHERE id=?",
                (int(queued["run_id"]),),
            ).fetchone()
            self.assertIsNotNone(command_row)
            assert command_row is not None
            submission_scope = json.loads(command_row["details_json"])["binding"][
                "submission_scope"
            ]

        self.assertEqual(
            submission_scope,
            {
                "contract_version": (
                    profile_control.CURRENT_HOLD_COMMAND_SCOPE_CONTRACT
                ),
                "valid": True,
                "activation_id": int(active_at_submission["activation_id"]),
                "profile_id": str(active_at_submission["profile_id"]),
                "roster_snapshot_id": int(
                    active_at_submission["roster_snapshot_id"]
                ),
                "roster_snapshot_hash": str(
                    active_at_submission["roster_members_sha256"]
                ),
                "predecessor_release_event_id": int(predecessor["id"]),
                "predecessor_release_event_hash": str(predecessor["event_hash"]),
            },
        )

        with connect(self.db) as connection:
            next_roster = self.hold._snapshot(
                connection, "system", "system-roster-after-midnight"
            )
            connection.commit()
            with transaction(connection):
                next_activation = append_activation(
                    connection,
                    profile_id=TIKHUB_PROFILE,
                    roster_snapshot_id=int(next_roster["id"]),
                    roster_members_sha256=str(next_roster["members_sha256"]),
                    effective_at=NEW_ACTIVATION_AT,
                    build_receipt_sha256=NEXT_BUILD,
                    actor="fixture-midnight-activation",
                    reason="activation changes after command submission",
                    created_at=NEW_ACTIVATION_AT,
                )
                issue_activation_permit_in_transaction(
                    connection,
                    activation_id=int(next_activation["activation_id"]),
                    drain_id="activation-3-midnight-permit",
                    source_activation_id=int(active_at_submission["activation_id"]),
                    business_day="2026-09-08",
                    planned_effective_at=NEW_ACTIVATION_AT,
                    build_receipt_sha256=NEXT_BUILD,
                    runtime_root_receipt_sha256=COMMAND_RUNTIME,
                    now=NEW_ACTIVATION_AT,
                )

        with patch.object(profile_control, "now_utc", return_value=EXECUTED_AT):
            processed = profile_control.process_current_activation_hold_commands(
                db_path=self.db,
                limit=1,
            )

        self.assertEqual(processed["count"], 1)
        self.assertEqual(processed["processed"][0]["status"], "failed")
        self.assertEqual(
            processed["processed"][0]["error"]["code"],
            "current_hold_submission_scope_drift",
        )
        command = profile_control.read_current_activation_hold_command(
            db_path=self.db,
            run_id=int(queued["run_id"]),
            read_only=False,
        )
        self.assertEqual(command["status"], "failed")
        self.assertEqual(
            command["error"]["code"], "current_hold_submission_scope_drift"
        )
        self.assertEqual(len(command["attempts"]), 1)
        self.assertEqual(self.hold._hold_event_rows(drain_id), [])

    def test_release_effect_survives_crash_and_retry_after_window(self) -> None:
        self.hold._begin()
        self.hold._seal()
        queued = profile_control.enqueue_current_activation_hold_command(
            db_path=self.db,
            command_id="hold-release-crash-after-effect",
            command="hold_release",
            parameters={
                "drain_id": hold_fixture.HOLD_ID,
                "actor": hold_fixture.ACTOR,
            },
            submitted_at="2026-09-07T16:04:58Z",
        )

        with (
            patch.object(
                profile_control,
                "now_utc",
                return_value=RELEASE_EFFECT_AT,
            ),
            patch.object(
                profile_control,
                "_finish_current_hold_command",
                side_effect=SystemExit("simulated crash after HOLD_RELEASE commit"),
            ) as finish_command,
            self.assertRaises(SystemExit),
        ):
            profile_control.process_current_activation_hold_commands(
                db_path=self.db,
                limit=1,
            )

        self.assertEqual(finish_command.call_count, 1)
        self.assertEqual(finish_command.call_args.kwargs["status"], "succeeded")
        release_rows = [
            row
            for row in self.hold._hold_event_rows(hold_fixture.HOLD_ID)
            if row["event_type"] == "release"
        ]
        self.assertEqual(len(release_rows), 1)
        self.assertEqual(release_rows[0]["created_at"], RELEASE_EFFECT_AT)

        crashed = profile_control.read_current_activation_hold_command(
            db_path=self.db,
            run_id=int(queued["run_id"]),
            read_only=False,
        )
        self.assertEqual(crashed["status"], "running")
        self.assertEqual([attempt["status"] for attempt in crashed["attempts"]], ["running"])

        with patch.object(scheduler, "now_utc", return_value=RETRY_AT):
            self.assertEqual(
                scheduler.recover_interrupted_scheduler_runs(db_path=self.db),
                1,
            )
        with patch.object(profile_control, "now_utc", return_value=RETRY_AT):
            retried = profile_control.process_current_activation_hold_commands(
                db_path=self.db,
                limit=1,
            )

        self.assertEqual(retried["count"], 1)
        self.assertEqual(retried["processed"][0]["status"], "succeeded")
        command = profile_control.read_current_activation_hold_command(
            db_path=self.db,
            run_id=int(queued["run_id"]),
            read_only=False,
        )
        self.assertEqual(command["status"], "succeeded")
        self.assertEqual(
            [attempt["status"] for attempt in command["attempts"]],
            ["interrupted", "succeeded"],
        )
        self.assertEqual(len(command["attempts"]), 2)
        release_rows_after_retry = [
            row
            for row in self.hold._hold_event_rows(hold_fixture.HOLD_ID)
            if row["event_type"] == "release"
        ]
        self.assertEqual(len(release_rows_after_retry), 1)
        self.assertEqual(
            int(command["result"]["release"]["event_id"]),
            int(release_rows[0]["id"]),
        )


if __name__ == "__main__":
    unittest.main()
