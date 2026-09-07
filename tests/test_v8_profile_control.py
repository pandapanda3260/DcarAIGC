from __future__ import annotations

import fcntl
import hashlib
import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from v8.paid_drain import dispatch_state, issue_activation_permit_in_transaction
from v8.profile_activations import (
    MATRIX_PROFILE,
    TIKHUB_PROFILE,
    activation_at,
    append_activation,
    cancel_activation,
)
from v8.profile_control import (
    ProfileControlError,
    begin_cross_profile_switch,
    complete_cross_profile_switch,
    main,
    schedule_accepted_roster_activation_in_transaction,
    schedule_same_profile_roster_activation,
)
from v8.runtime_database import (
    DatabaseAccessMode,
    FileIdentity,
    InstalledWriterContract,
    ResolvedDatabaseAccess,
    RuntimeDatabaseError,
)
from v8.storage import connect, initialize_database, transaction

INITIAL = "2026-09-01T00:00:00.000000Z"
BEGIN = "2026-09-01T12:00:00Z"  # 20:00 Beijing
COMPLETE = "2026-09-01T13:30:00Z"  # 21:30 Beijing
EFFECTIVE = "2026-09-01T16:00:00.000000Z"  # next Beijing midnight
BUILD = "a" * 64
RUNTIME = "b" * 64


class ProfileControlTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / "profile.sqlite3"
        with connect(self.db) as connection:
            initialize_database(connection)
            self.matrix = self._snapshot(connection, "matrix", "matrix-one")
            self.matrix_next = self._snapshot(connection, "matrix", "matrix-two")
            self.system = self._snapshot(connection, "system", "system-one")
            self.system_next = self._snapshot(connection, "system", "system-two")
            connection.commit()
            with transaction(connection):
                initial = append_activation(
                    connection,
                    profile_id=MATRIX_PROFILE,
                    roster_snapshot_id=self.matrix["id"],
                    roster_members_sha256=self.matrix["members_sha256"],
                    effective_at=INITIAL,
                    build_receipt_sha256=BUILD,
                    actor="fixture",
                    reason="initial profile",
                    created_at=INITIAL,
                )
                release = issue_activation_permit_in_transaction(
                    connection,
                    activation_id=int(initial["activation_id"]),
                    drain_id="bootstrap:1",
                    source_activation_id=int(initial["activation_id"]),
                    business_day="2026-09-01",
                    planned_effective_at=INITIAL,
                    build_receipt_sha256=BUILD,
                    runtime_root_receipt_sha256=RUNTIME,
                    now=INITIAL,
                )
                self.initial_activation_id = int(initial["activation_id"])
                self.initial_permit_id = release.event_id

    def _snapshot(self, connection, family: str, key: str) -> dict[str, object]:
        source = json.dumps({"family": family, "key": key}).encode()
        source_hash = hashlib.sha256(source).hexdigest()
        members_hash = hashlib.sha256(key.encode()).hexdigest()
        path = self.root / f"{key}.json"
        path.write_bytes(source)
        cursor = connection.execute(
            """INSERT INTO account_roster_snapshots(
                   source_family,source_type,scope_key,scope_json,source_instance_id,
                   source_captured_at,accepted_at,declared_count,member_count,
                   members_sha256,source_sha256,source_path,contract_version,metadata_json)
               VALUES (?,?,?,'{}',?,'2026-09-01T00:00:00Z','2026-09-01T00:00:00Z',
                       0,0,?,?,?,'account-roster-v2','{}')""",
            (
                family,
                "system_managed" if family == "system" else "manual_export",
                key,
                key,
                members_hash,
                source_hash,
                str(path),
            ),
        )
        return {"id": int(cursor.lastrowid or 0), "members_sha256": members_hash}

    def state(self, at: str):
        with connect(self.db) as connection:
            return dispatch_state(connection, at=at)

    def begin(self, *, drain_id: str = "switch-to-system") -> dict:
        return begin_cross_profile_switch(
            db_path=self.db,
            drain_id=drain_id,
            target_profile_id=TIKHUB_PROFILE,
            roster_snapshot_id=int(self.system["id"]),
            build_receipt_sha256=BUILD,
            runtime_root_receipt_sha256=RUNTIME,
            actor="operator",
            reason="switch provider profile",
            now=BEGIN,
        )

    def test_cross_profile_begin_complete_stays_closed_until_midnight(self) -> None:
        before = self.state("2026-09-01T11:59:59Z")
        self.assertEqual(
            (before.state, before.activation_id, before.permit_event_id),
            ("open", self.initial_activation_id, self.initial_permit_id),
        )
        begun = self.begin()
        self.assertEqual(begun["activation"]["effective_at"], EFFECTIVE)
        self.assertEqual(self.state("2026-09-01T12:00:01Z").state, "draining")

        mirror_root = self.root / "switch-mirrors"
        completed = complete_cross_profile_switch(
            db_path=self.db,
            drain_id="switch-to-system",
            now=COMPLETE,
            mirror_root=mirror_root,
        )
        self.assertEqual(completed["fetch_recovery"]["recovered"], 0)
        before_midnight = self.state("2026-09-01T15:59:59Z")
        self.assertEqual(before_midnight.state, "sealed")
        self.assertEqual(before_midnight.activation_id, self.initial_activation_id)
        after_midnight = self.state(EFFECTIVE)
        self.assertEqual(after_midnight.state, "open")
        self.assertEqual(
            after_midnight.activation_id,
            begun["activation"]["activation_id"],
        )
        self.assertEqual(
            after_midnight.permit_event_id,
            completed["release"]["event_id"],
        )
        self.assertTrue((mirror_root / "switch-to-system.release.json").is_file())
        repeated = complete_cross_profile_switch(
            db_path=self.db,
            drain_id="switch-to-system",
            now=COMPLETE,
            mirror_root=mirror_root,
        )
        self.assertEqual(
            repeated["release"]["event_id"], completed["release"]["event_id"]
        )

    def test_cross_profile_begin_before_20_is_atomic_rejection(self) -> None:
        with self.assertRaises(ProfileControlError) as caught:
            begin_cross_profile_switch(
                db_path=self.db,
                drain_id="too-early",
                target_profile_id=TIKHUB_PROFILE,
                roster_snapshot_id=int(self.system["id"]),
                build_receipt_sha256=BUILD,
                runtime_root_receipt_sha256=RUNTIME,
                actor="operator",
                reason="too early",
                now="2026-09-01T11:59:59Z",
            )
        self.assertEqual(caught.exception.code, "profile_control_begin_too_early")
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM acquisition_profile_activations"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM pipeline_paid_drain_events"
                ).fetchone()[0],
                3,
            )

    def test_cross_profile_can_activate_immediately_before_20(self) -> None:
        switched_at = "2026-09-01T03:00:00Z"
        normalized_switched_at = "2026-09-01T03:00:00.000000Z"
        begun = begin_cross_profile_switch(
            db_path=self.db,
            drain_id="switch-immediately",
            target_profile_id=TIKHUB_PROFILE,
            roster_snapshot_id=int(self.system["id"]),
            build_receipt_sha256=BUILD,
            runtime_root_receipt_sha256=RUNTIME,
            actor="operator",
            reason="provider authorization unavailable",
            now=switched_at,
            effective_now=True,
        )
        self.assertEqual(
            begun["activation"]["effective_at"], normalized_switched_at
        )
        self.assertEqual(
            begun["activation"]["metadata"]["effective_mode"], "immediate"
        )
        self.assertEqual(self.state(switched_at).state, "draining")

        completed = complete_cross_profile_switch(
            db_path=self.db,
            drain_id="switch-immediately",
            now=switched_at,
            mirror_root=self.root / "immediate-switch-mirrors",
        )
        current = self.state(switched_at)
        # An immediate production activation is a historical emergency path,
        # not a full-business-day permit.  The current-hold contract must be
        # established before ordinary paid dispatch can reopen.
        self.assertEqual(current.state, "closed")
        self.assertEqual(current.reason, "current_activation_hold_missing")
        self.assertEqual(
            current.activation_id, begun["activation"]["activation_id"]
        )
        self.assertIsNone(current.permit_event_id)
        self.assertNotEqual(current.last_event_id, None)
        self.assertEqual(current.last_event_id, completed["release"]["event_id"])

    def test_immediate_switch_supersedes_a_future_activation_atomically(self) -> None:
        scheduled = schedule_same_profile_roster_activation(
            db_path=self.db,
            roster_snapshot_id=int(self.matrix_next["id"]),
            build_receipt_sha256=BUILD,
            runtime_root_receipt_sha256=RUNTIME,
            actor="writer",
            reason="refresh",
            now="2026-09-01T02:00:00Z",
        )
        begun = begin_cross_profile_switch(
            db_path=self.db,
            drain_id="immediate-conflict",
            target_profile_id=TIKHUB_PROFILE,
            roster_snapshot_id=int(self.system["id"]),
            build_receipt_sha256=BUILD,
            runtime_root_receipt_sha256=RUNTIME,
            actor="operator",
            reason="provider authorization unavailable",
            now="2026-09-01T03:00:00Z",
            effective_now=True,
        )
        self.assertEqual(
            begun["superseded_activation_ids"],
            [scheduled["activation"]["activation_id"]],
        )
        complete_cross_profile_switch(
            db_path=self.db,
            drain_id="immediate-conflict",
            now="2026-09-01T03:00:00Z",
        )
        with connect(self.db) as connection:
            cancellation = connection.execute(
                "SELECT activation_id FROM activation_cancellations WHERE activation_id=?",
                (scheduled["activation"]["activation_id"],),
            ).fetchone()
        self.assertEqual(
            int(cancellation["activation_id"]),
            scheduled["activation"]["activation_id"],
        )
        self.assertEqual(
            self.state("2026-09-01T16:00:01Z").activation_id,
            begun["activation"]["activation_id"],
        )

    def test_complete_after_midnight_is_recovery_not_next_day_block(self) -> None:
        begun = self.begin(drain_id="late-complete")
        completed = complete_cross_profile_switch(
            db_path=self.db,
            drain_id="late-complete",
            now="2026-09-01T16:05:00Z",
        )
        state = self.state("2026-09-01T16:05:01Z")
        self.assertEqual(state.state, "open")
        self.assertEqual(state.activation_id, begun["activation"]["activation_id"])
        self.assertEqual(state.permit_event_id, completed["release"]["event_id"])

    def test_complete_before_2130_does_not_recover_or_seal(self) -> None:
        self.begin(drain_id="early-complete")
        with self.assertRaises(ProfileControlError) as caught:
            complete_cross_profile_switch(
                db_path=self.db,
                drain_id="early-complete",
                now="2026-09-01T13:29:59Z",
            )
        self.assertEqual(caught.exception.code, "profile_control_complete_too_early")
        with connect(self.db) as connection:
            rows = connection.execute(
                "SELECT event_type FROM pipeline_paid_drain_events "
                "WHERE drain_id='early-complete' ORDER BY id"
            ).fetchall()
        self.assertEqual([row["event_type"] for row in rows], ["start"])

    def test_same_profile_schedule_does_not_close_current_activation(self) -> None:
        outcome = schedule_same_profile_roster_activation(
            db_path=self.db,
            roster_snapshot_id=int(self.matrix_next["id"]),
            build_receipt_sha256=BUILD,
            runtime_root_receipt_sha256=RUNTIME,
            actor="writer",
            reason="accepted roster refresh",
            now="2026-09-01T10:00:00Z",
        )
        current = self.state("2026-09-01T15:00:00Z")
        self.assertEqual(
            (current.state, current.activation_id, current.permit_event_id),
            ("open", self.initial_activation_id, self.initial_permit_id),
        )
        next_state = self.state(EFFECTIVE)
        self.assertEqual(next_state.state, "open")
        self.assertEqual(
            next_state.activation_id, outcome["activation"]["activation_id"]
        )
        self.assertNotEqual(next_state.permit_event_id, self.initial_permit_id)
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM scheduler_runs WHERE job_id='pipeline_paid_drain_bridge'"
                ).fetchone()[0],
                0,
            )

    def test_writer_accepted_mode_b_roster_schedules_next_midnight(self) -> None:
        self.begin(drain_id="activate-system")
        complete_cross_profile_switch(
            db_path=self.db,
            drain_id="activate-system",
            now=COMPLETE,
        )
        accepted_at = "2026-09-02T02:00:00Z"
        with connect(self.db) as connection, transaction(connection):
            outcome = schedule_accepted_roster_activation_in_transaction(
                connection,
                roster_snapshot_id=int(self.system_next["id"]),
                actor="writer",
                reason="accepted managed roster refresh",
                now=accepted_at,
            )
        self.assertTrue(outcome["scheduled"])
        self.assertEqual(outcome["activation"]["profile_id"], TIKHUB_PROFILE)
        self.assertEqual(
            outcome["activation"]["effective_at"],
            "2026-09-02T16:00:00.000000Z",
        )
        before = self.state("2026-09-02T15:59:59Z")
        after = self.state("2026-09-02T16:00:00Z")
        self.assertNotEqual(before.activation_id, outcome["activation"]["activation_id"])
        self.assertEqual(after.state, "open")
        self.assertEqual(after.activation_id, outcome["activation"]["activation_id"])

    def test_writer_accepting_inactive_family_does_not_create_cross_switch(self) -> None:
        with connect(self.db) as connection:
            before = int(
                connection.execute(
                    "SELECT COUNT(*) FROM acquisition_profile_activations"
                ).fetchone()[0]
            )
            with transaction(connection):
                outcome = schedule_accepted_roster_activation_in_transaction(
                    connection,
                    roster_snapshot_id=int(self.system["id"]),
                    actor="writer",
                    reason="prepare rollback roster",
                    now="2026-09-01T10:00:00Z",
                )
            after = int(
                connection.execute(
                    "SELECT COUNT(*) FROM acquisition_profile_activations"
                ).fetchone()[0]
            )
        self.assertFalse(outcome["scheduled"])
        self.assertEqual(outcome["reason"], "profile_family_inactive")
        self.assertEqual(after, before)

    def test_same_profile_permit_is_nonblocking_with_paid_work_running(self) -> None:
        with connect(self.db) as connection, transaction(connection):
            run_id = int(
                connection.execute(
                    """INSERT INTO scheduler_runs(
                           job_id,scheduled_for,status,started_at,details_json)
                       VALUES ('matrix_works_scan','active','running',?,'{}')""",
                    ("2026-09-01T09:59:00Z",),
                ).lastrowid
            )
            connection.execute(
                """INSERT INTO scheduler_run_attempts(
                       scheduler_run_id,attempt_number,invocation_source,status,
                       started_at,details_json)
                   VALUES (?,1,'scheduled','running',?,'{}')""",
                (run_id, "2026-09-01T09:59:00Z"),
            )
        scheduled = schedule_same_profile_roster_activation(
            db_path=self.db,
            roster_snapshot_id=int(self.matrix_next["id"]),
            build_receipt_sha256=BUILD,
            runtime_root_receipt_sha256=RUNTIME,
            actor="writer",
            reason="refresh while workers run",
            now="2026-09-01T10:00:00Z",
        )
        self.assertEqual(self.state("2026-09-01T10:00:01Z").state, "open")
        self.assertEqual(self.state(EFFECTIVE).activation_id, scheduled["activation"]["activation_id"])
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM scheduler_runs WHERE id=?", (run_id,)
                ).fetchone()["status"],
                "running",
            )

    def test_later_same_day_roster_replaces_scheduled_activation(self) -> None:
        first = schedule_same_profile_roster_activation(
            db_path=self.db,
            roster_snapshot_id=int(self.matrix_next["id"]),
            build_receipt_sha256=BUILD,
            runtime_root_receipt_sha256=RUNTIME,
            actor="writer",
            reason="first refresh",
            now="2026-09-01T10:00:00Z",
        )
        second = schedule_same_profile_roster_activation(
            db_path=self.db,
            roster_snapshot_id=int(self.matrix["id"]),
            build_receipt_sha256="d" * 64,
            runtime_root_receipt_sha256=RUNTIME,
            actor="writer",
            reason="corrected refresh",
            now="2026-09-01T11:00:00Z",
        )
        with connect(self.db) as connection:
            cancelled = connection.execute(
                "SELECT 1 FROM activation_cancellations WHERE activation_id=?",
                (first["activation"]["activation_id"],),
            ).fetchone()
        self.assertIsNotNone(cancelled)
        state = self.state(EFFECTIVE)
        self.assertEqual(state.state, "open")
        self.assertEqual(state.activation_id, second["activation"]["activation_id"])
        self.assertEqual(state.permit_event_id, second["release"]["event_id"])

    def test_cross_switch_replaces_same_profile_midnight_schedule(self) -> None:
        scheduled = schedule_same_profile_roster_activation(
            db_path=self.db,
            roster_snapshot_id=int(self.matrix_next["id"]),
            build_receipt_sha256=BUILD,
            runtime_root_receipt_sha256=RUNTIME,
            actor="writer",
            reason="routine refresh",
            now="2026-09-01T10:00:00Z",
        )
        cross = self.begin(drain_id="replace-routine-refresh")
        with connect(self.db) as connection:
            self.assertIsNotNone(
                connection.execute(
                    "SELECT 1 FROM activation_cancellations WHERE activation_id=?",
                    (scheduled["activation"]["activation_id"],),
                ).fetchone()
            )
        self.assertEqual(cross["activation"]["profile_id"], TIKHUB_PROFILE)
        self.assertEqual(self.state("2026-09-01T12:00:01Z").state, "draining")

    def test_recent_running_slot_blocks_complete_and_stays_draining(self) -> None:
        self.begin(drain_id="switch-running")
        with connect(self.db) as connection, transaction(connection):
            account_id = int(
                connection.execute(
                    "INSERT INTO accounts(phone,created_at,updated_at) VALUES ('','','')"
                ).lastrowid
            )
            connection.execute(
                """INSERT INTO fetch_slots(
                       account_id,stage,window_key,provider,adapter_version,status,
                       started_at,created_at,updated_at)
                   VALUES (?,'discovery','recent','TikHub','fixture','running',?,?,?)""",
                (
                    account_id,
                    "2026-09-01T13:29:00Z",
                    "2026-09-01T13:29:00Z",
                    "2026-09-01T13:29:00Z",
                ),
            )
        with self.assertRaises(ProfileControlError) as caught:
            complete_cross_profile_switch(
                db_path=self.db, drain_id="switch-running", now=COMPLETE
            )
        self.assertEqual(caught.exception.code, "profile_control_running_work")
        self.assertEqual(self.state(COMPLETE).state, "draining")

    def test_complete_recovers_work_that_was_stale_since_start(self) -> None:
        with connect(self.db) as connection, transaction(connection):
            account_id = int(
                connection.execute(
                    "INSERT INTO accounts(phone,created_at,updated_at) VALUES ('','','')"
                ).lastrowid
            )
            slot_id = int(
                connection.execute(
                    """INSERT INTO fetch_slots(
                           account_id,stage,window_key,provider,adapter_version,status,
                           started_at,created_at,updated_at)
                       VALUES (?,'discovery','stale','TikHub','fixture','running',?,?,?)""",
                    (
                        account_id,
                        "2026-09-01T11:00:00Z",
                        "2026-09-01T11:00:00Z",
                        "2026-09-01T11:00:00Z",
                    ),
                ).lastrowid
            )
            run_id = int(
                connection.execute(
                    """INSERT INTO scheduler_runs(
                           job_id,scheduled_for,status,started_at,details_json)
                       VALUES ('matrix_works_scan','stale','running',?,'{}')""",
                    ("2026-09-01T11:00:00Z",),
                ).lastrowid
            )
            connection.execute(
                """INSERT INTO scheduler_run_attempts(
                       scheduler_run_id,attempt_number,invocation_source,status,
                       started_at,details_json)
                   VALUES (?,1,'scheduled','running',?,'{}')""",
                (run_id, "2026-09-01T11:00:00Z"),
            )
        self.begin(drain_id="recover-stale")
        result = complete_cross_profile_switch(
            db_path=self.db,
            drain_id="recover-stale",
            now=COMPLETE,
        )
        self.assertEqual(result["fetch_recovery"]["recovered"], 1)
        self.assertEqual(result["scheduler_recovery"], 1)
        with connect(self.db) as connection:
            slot = connection.execute(
                "SELECT status FROM fetch_slots WHERE id=?", (slot_id,)
            ).fetchone()
            run = connection.execute(
                "SELECT status FROM scheduler_runs WHERE id=?", (run_id,)
            ).fetchone()
        self.assertEqual(slot["status"], "retryable_failed")
        self.assertEqual(run["status"], "interrupted")

    def test_cancelled_target_fails_closed_even_after_release(self) -> None:
        begun = self.begin(drain_id="switch-cancelled")
        complete_cross_profile_switch(
            db_path=self.db, drain_id="switch-cancelled", now=COMPLETE
        )
        with connect(self.db) as connection:
            cancel_activation(
                connection,
                int(begun["activation"]["activation_id"]),
                cancelled_at="2026-09-01T15:00:00Z",
                actor="operator",
                reason="rollback gate failed",
            )
        state = self.state(EFFECTIVE)
        self.assertEqual(state.state, "invalid")
        self.assertIn("cancelled", state.reason or "")

    def test_cancelled_target_is_not_released_by_complete(self) -> None:
        begun = self.begin(drain_id="cancel-before-complete")
        with connect(self.db) as connection:
            cancel_activation(
                connection,
                int(begun["activation"]["activation_id"]),
                cancelled_at="2026-09-01T13:00:00Z",
                actor="operator",
                reason="cancel before complete",
            )
        with self.assertRaises(ProfileControlError) as caught:
            complete_cross_profile_switch(
                db_path=self.db,
                drain_id="cancel-before-complete",
                now=COMPLETE,
            )
        self.assertEqual(caught.exception.code, "profile_control_target_cancelled")
        with connect(self.db) as connection:
            events = connection.execute(
                "SELECT event_type FROM pipeline_paid_drain_events "
                "WHERE drain_id='cancel-before-complete' ORDER BY id"
            ).fetchall()
        self.assertEqual([row["event_type"] for row in events], ["start"])

    def test_future_permit_does_not_steal_current_dispatch_authority(self) -> None:
        outcome = schedule_same_profile_roster_activation(
            db_path=self.db,
            roster_snapshot_id=int(self.matrix_next["id"]),
            build_receipt_sha256=BUILD,
            runtime_root_receipt_sha256=RUNTIME,
            actor="writer",
            reason="refresh",
            now="2026-09-01T10:00:00Z",
        )
        with connect(self.db) as connection, transaction(connection):
            run_id = int(
                connection.execute(
                    """INSERT INTO scheduler_runs(
                           job_id,scheduled_for,status,started_at,details_json)
                       VALUES ('fixture','fixture','running','2026-09-01T15:00:00Z','{}')"""
                ).lastrowid
            )
            attempt_id = int(
                connection.execute(
                    """INSERT INTO scheduler_run_attempts(
                           scheduler_run_id,attempt_number,invocation_source,status,
                           started_at,details_json)
                       VALUES (?,1,'scheduled','running','2026-09-01T15:00:00Z','{}')""",
                    (run_id,),
                ).lastrowid
            )
            reserved = connection.execute(
                """INSERT INTO paid_provider_dispatch_events(
                       dispatch_id,sequence,event_type,provider,operation,activation_id,
                       business_day,permit_event_id,scheduler_run_id,scheduler_attempt_id,
                       scope_json,contract_version,event_hash,created_at)
                   VALUES ('old-active',1,'reserved','TikHub','fixture',?,'2026-09-01',
                           ?,?,?,'{}','paid-provider-dispatch-v1',?,'2026-09-01T15:00:00Z')""",
                (
                    self.initial_activation_id,
                    self.initial_permit_id,
                    run_id,
                    attempt_id,
                    "c" * 64,
                ),
            )
            reserved_id = int(reserved.lastrowid or 0)
            sent = connection.execute(
                """INSERT INTO paid_provider_dispatch_events(
                       dispatch_id,sequence,event_type,provider,operation,activation_id,
                       business_day,permit_event_id,scheduler_run_id,scheduler_attempt_id,
                       scope_json,previous_event_id,previous_event_hash,contract_version,
                       event_hash,created_at)
                   VALUES ('old-active',2,'send_marked','TikHub','fixture',?,'2026-09-01',
                           ?,?,?,'{}',?,?,'paid-provider-dispatch-v1',?,
                           '2026-09-01T15:59:59Z')""",
                (
                    self.initial_activation_id,
                    self.initial_permit_id,
                    run_id,
                    attempt_id,
                    reserved_id,
                    "c" * 64,
                    "e" * 64,
                ),
            )
            sent_id = int(sent.lastrowid or 0)
            # A request authorized before midnight must retain a terminal
            # billing receipt after the activation changes. It cannot issue a
            # second reservation under the superseded activation.
            connection.execute(
                """INSERT INTO paid_provider_dispatch_events(
                       dispatch_id,sequence,event_type,provider,operation,activation_id,
                       business_day,permit_event_id,scheduler_run_id,scheduler_attempt_id,
                       scope_json,previous_event_id,previous_event_hash,contract_version,
                       event_hash,created_at)
                   VALUES ('old-active',3,'succeeded','TikHub','fixture',?,'2026-09-01',
                           ?,?,?,'{}',?,?,'paid-provider-dispatch-v1',?,
                           '2026-09-01T16:00:01Z')""",
                (
                    self.initial_activation_id,
                    self.initial_permit_id,
                    run_id,
                    attempt_id,
                    sent_id,
                    "e" * 64,
                    "f" * 64,
                ),
            )
            unsent_reserved = connection.execute(
                """INSERT INTO paid_provider_dispatch_events(
                       dispatch_id,sequence,event_type,provider,operation,activation_id,
                       business_day,permit_event_id,scheduler_run_id,scheduler_attempt_id,
                       scope_json,contract_version,event_hash,created_at)
                   VALUES ('old-unsent',1,'reserved','TikHub','fixture',?,'2026-09-01',
                           ?,?,?,'{}','paid-provider-dispatch-v1',?,
                           '2026-09-01T15:59:59Z')""",
                (
                    self.initial_activation_id,
                    self.initial_permit_id,
                    run_id,
                    attempt_id,
                    "1" * 64,
                ),
            )
            connection.execute(
                """INSERT INTO paid_provider_dispatch_events(
                       dispatch_id,sequence,event_type,provider,operation,activation_id,
                       business_day,permit_event_id,scheduler_run_id,scheduler_attempt_id,
                       scope_json,previous_event_id,previous_event_hash,contract_version,
                       event_hash,created_at)
                   VALUES ('old-unsent',2,'not_sent','TikHub','fixture',?,'2026-09-01',
                           ?,?,?,'{}',?,?,'paid-provider-dispatch-v1',?,
                           '2026-09-01T16:00:01Z')""",
                (
                    self.initial_activation_id,
                    self.initial_permit_id,
                    run_id,
                    attempt_id,
                    int(unsent_reserved.lastrowid or 0),
                    "1" * 64,
                    "2" * 64,
                ),
            )
            with self.assertRaises(Exception):
                connection.execute(
                    """INSERT INTO paid_provider_dispatch_events(
                           dispatch_id,sequence,event_type,provider,operation,activation_id,
                           business_day,permit_event_id,scheduler_run_id,scheduler_attempt_id,
                           scope_json,contract_version,event_hash,created_at)
                       VALUES ('old-after',1,'reserved','TikHub','fixture',?,'2026-09-02',
                               ?,?,?,'{}','paid-provider-dispatch-v1',?,'2026-09-01T16:00:01Z')""",
                    (
                        self.initial_activation_id,
                        self.initial_permit_id,
                        run_id,
                        attempt_id,
                        "d" * 64,
                    ),
                )
        self.assertEqual(self.state(EFFECTIVE).activation_id, outcome["activation"]["activation_id"])

    def test_native_permit_tamper_fails_closed(self) -> None:
        with connect(self.db) as connection:
            connection.execute("DROP TRIGGER trg_paid_drain_events_no_update")
            connection.execute(
                "UPDATE pipeline_paid_drain_events SET payload_json='{}' WHERE id=?",
                (self.initial_permit_id,),
            )
            connection.commit()
        state = self.state(BEGIN)
        self.assertEqual(state.state, "invalid")
        self.assertIn("differs", state.reason or "")

    def test_formal_cli_requires_and_acquires_installed_writer_lock(self) -> None:
        project = self.root / "project"
        project.mkdir()
        lock_path = self.root / "writer.lock"
        lock_path.write_bytes(b"")
        lock_path.chmod(0o600)
        database_stat = self.db.stat()
        contract = InstalledWriterContract(
            home=self.root,
            plist_path=self.root / "writer.plist",
            project_root=project,
            program=project / "deploy/macos/run_writer_worker.sh",
            database=self.db,
            writer_lock=lock_path,
            payload={},
        )
        access = ResolvedDatabaseAccess(
            access_mode=DatabaseAccessMode.FORMAL_MUTATION,
            database=self.db,
            database_identity=FileIdentity.from_stat(database_stat),
            project_root=project,
            writer_lock=lock_path,
            installed=contract,
        )
        descriptor = os.open(lock_path, os.O_RDWR)
        self.addCleanup(os.close, descriptor)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.addCleanup(fcntl.flock, descriptor, fcntl.LOCK_UN)
        with patch("v8.profile_control.resolve_installed_database_access", return_value=access):
            with self.assertRaises(ProfileControlError) as forbidden:
                main(
                    [
                        "--db",
                        str(self.db),
                        "--project-root",
                        str(project),
                        "begin",
                        "--drain-id",
                        "forbidden-immediate-switch",
                        "--target-profile",
                        TIKHUB_PROFILE,
                        "--roster-snapshot-id",
                        str(self.system["id"]),
                        "--build-receipt-sha256",
                        BUILD,
                        "--runtime-root-receipt-sha256",
                        RUNTIME,
                        "--actor",
                        "operator",
                        "--reason",
                        "test",
                        "--effective-now",
                    ]
                )
            self.assertEqual(
                forbidden.exception.code, "profile_control_effective_now_forbidden"
            )
            with self.assertRaisesRegex(RuntimeDatabaseError, "already held"):
                main(
                    [
                        "--db",
                        str(self.db),
                        "--project-root",
                        str(project),
                        "begin",
                        "--drain-id",
                        "locked-switch",
                        "--target-profile",
                        TIKHUB_PROFILE,
                        "--roster-snapshot-id",
                        str(self.system["id"]),
                        "--build-receipt-sha256",
                        BUILD,
                        "--runtime-root-receipt-sha256",
                        RUNTIME,
                        "--actor",
                        "operator",
                        "--reason",
                        "test",
                    ]
                )

    def test_isolated_cli_must_be_explicit_and_accepts_clock_override(self) -> None:
        switched_at = "2026-09-01T03:00:00Z"
        with redirect_stdout(io.StringIO()):
            result = main(
                [
                "--db",
                str(self.db),
                "--isolated",
                "--at",
                switched_at,
                "begin",
                "--drain-id",
                "isolated-switch",
                "--target-profile",
                TIKHUB_PROFILE,
                "--roster-snapshot-id",
                str(self.system["id"]),
                "--build-receipt-sha256",
                BUILD,
                "--runtime-root-receipt-sha256",
                RUNTIME,
                "--actor",
                "operator",
                "--reason",
                "test",
                "--effective-now",
                ]
            )
        self.assertEqual(result, 0)
        with connect(self.db) as connection:
            current = activation_at(connection, switched_at)
        self.assertEqual(current["profile_id"], TIKHUB_PROFILE)


if __name__ == "__main__":
    unittest.main()
