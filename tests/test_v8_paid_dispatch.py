from __future__ import annotations

import hashlib
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from tests.roster_fixture import accept_roster
from v8.operations import upsert_account
from v8.paid_dispatch import (
    PaidDispatchError,
    close_dispatch_not_sent_in_transaction,
    dispatch_events,
    finish_dispatch_in_transaction,
    mark_dispatch_sent_in_transaction,
    reserve_dispatch_in_transaction,
)
from v8.paid_drain import dispatch_state
from v8.provider_budget import budget_day
from v8.storage import connect, initialize_database, now_utc, transaction


class PaidDispatchTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = Path(self.temp.name) / "paid-dispatch.sqlite3"
        with connect(self.db) as connection:
            initialize_database(connection)
        upsert_account(
            {
                "platforms": [
                    {"platform": "douyin", "uid": "123456789", "nickname": "test"}
                ]
            },
            db_path=self.db,
        )
        with connect(self.db) as connection:
            accept_roster(connection)
            state = dispatch_state(connection)
            assert state.activation_id is not None
            assert state.permit_event_id is not None
            self.activation_id = state.activation_id
            self.permit_event_id = state.permit_event_id
            at = now_utc()
            run = connection.execute(
                """INSERT INTO scheduler_runs(
                       job_id,scheduled_for,status,started_at,details_json)
                   VALUES ('paid-test','2026-09-02T00:00:00Z','running',?,'{}')""",
                (at,),
            )
            attempt = connection.execute(
                """INSERT INTO scheduler_run_attempts(
                       scheduler_run_id,attempt_number,invocation_source,status,
                       started_at,details_json)
                   VALUES (?,1,'scheduled','running',?,'{}')""",
                (run.lastrowid, at),
            )
            self.run_id = int(run.lastrowid or 0)
            self.attempt_id = int(attempt.lastrowid or 0)
            connection.commit()

    def _reserve(self, connection: sqlite3.Connection, *, at: str):
        return reserve_dispatch_in_transaction(
            connection,
            provider="TikHub",
            operation="douyin_user_posts",
            activation_id=self.activation_id,
            business_day=budget_day(at),
            scheduler_run_id=self.run_id,
            scheduler_attempt_id=self.attempt_id,
            scope={"purpose": "reconcile", "identity_id": 1},
            cursor_identity={"cursor": 0},
            created_at=at,
        )

    @staticmethod
    def _direct_append(
        connection: sqlite3.Connection,
        previous,
        *,
        event_type: str,
        sequence: int,
        created_at: str,
        **overrides,
    ) -> None:
        values = {
            "provider": previous.provider,
            "operation": previous.operation,
            "activation_id": previous.activation_id,
            "business_day": previous.business_day,
            "permit_event_id": previous.permit_event_id,
            "scheduler_run_id": previous.scheduler_run_id,
            "scheduler_attempt_id": previous.scheduler_attempt_id,
            "scope_json": '{"identity_id":1,"purpose":"reconcile"}',
            "provider_usage_id": previous.provider_usage_id,
            "fetch_slot_id": previous.fetch_slot_id,
            "fetch_attempt_id": previous.fetch_attempt_id,
            "raw_response_id": previous.raw_response_id,
            "cursor_identity_json": '{"cursor":17}',
        }
        values.update(overrides)
        event_hash = hashlib.sha256(
            f"{previous.dispatch_id}:{sequence}:{event_type}:{overrides}".encode()
        ).hexdigest()
        connection.execute(
            """INSERT INTO paid_provider_dispatch_events(
                   dispatch_id,sequence,event_type,provider,operation,activation_id,
                   business_day,permit_event_id,scheduler_run_id,scheduler_attempt_id,
                   scope_json,provider_usage_id,fetch_slot_id,fetch_attempt_id,
                   raw_response_id,cursor_identity_json,previous_event_id,
                   previous_event_hash,contract_version,event_hash,created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                previous.dispatch_id,
                sequence,
                event_type,
                values["provider"],
                values["operation"],
                values["activation_id"],
                values["business_day"],
                values["permit_event_id"],
                values["scheduler_run_id"],
                values["scheduler_attempt_id"],
                values["scope_json"],
                values["provider_usage_id"],
                values["fetch_slot_id"],
                values["fetch_attempt_id"],
                values["raw_response_id"],
                values["cursor_identity_json"],
                previous.event_id,
                previous.event_hash,
                "paid-provider-dispatch-v1",
                event_hash,
                created_at,
            ),
        )

    def test_success_chain_is_bound_and_append_only(self) -> None:
        at = now_utc()
        with connect(self.db) as connection, transaction(connection):
            reserved = self._reserve(connection, at=at)
            assert reserved is not None
            sent = mark_dispatch_sent_in_transaction(
                connection, reserved.dispatch_id, fetch_attempt_id=None, created_at=at
            )
            assert sent is not None
            terminal = finish_dispatch_in_transaction(
                connection,
                reserved.dispatch_id,
                outcome="succeeded",
                created_at=at,
            )
            assert terminal is not None
        with connect(self.db) as connection:
            events = dispatch_events(connection, reserved.dispatch_id)
            self.assertEqual(
                [event.event_type for event in events],
                ["reserved", "send_marked", "succeeded"],
            )
            self.assertTrue(
                all(event.activation_id == self.activation_id for event in events)
            )
            self.assertTrue(
                all(event.permit_event_id == self.permit_event_id for event in events)
            )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE paid_provider_dispatch_events SET operation='tampered' WHERE id=?",
                    (events[0].event_id,),
                )

    def test_not_sent_closes_without_send_or_terminal_event(self) -> None:
        at = now_utc()
        with connect(self.db) as connection, transaction(connection):
            reserved = self._reserve(connection, at=at)
            assert reserved is not None
            closed = close_dispatch_not_sent_in_transaction(
                connection,
                reserved.dispatch_id,
                reason="profile_switch_drain",
                created_at=at,
            )
            assert closed is not None
            replay = close_dispatch_not_sent_in_transaction(
                connection,
                reserved.dispatch_id,
                reason="profile_switch_drain",
                created_at=at,
            )
            self.assertEqual(replay.event_id, closed.event_id)
        with connect(self.db) as connection:
            self.assertEqual(
                [event.event_type for event in dispatch_events(connection, reserved.dispatch_id)],
                ["reserved", "not_sent"],
            )

    def test_invalid_transition_and_wrong_activation_fail_closed(self) -> None:
        at = now_utc()
        with connect(self.db) as connection, transaction(connection):
            with self.assertRaises(PaidDispatchError):
                reserve_dispatch_in_transaction(
                    connection,
                    provider="TikHub",
                    operation="douyin_user_posts",
                    activation_id=self.activation_id + 999,
                    business_day=budget_day(at),
                    scheduler_run_id=self.run_id,
                    scheduler_attempt_id=self.attempt_id,
                    scope={},
                    created_at=at,
                )
            reserved = self._reserve(connection, at=at)
            assert reserved is not None
            with self.assertRaises(PaidDispatchError):
                finish_dispatch_in_transaction(
                    connection,
                    reserved.dispatch_id,
                    outcome="failed",
                    created_at=at,
                )

    def test_terminal_can_be_recorded_after_business_day_rollover(self) -> None:
        at = now_utc()
        next_day = (
            datetime.fromisoformat(at.replace("Z", "+00:00")) + timedelta(days=1)
        ).astimezone(timezone.utc).isoformat()
        with connect(self.db) as connection, transaction(connection):
            reserved = self._reserve(connection, at=at)
            assert reserved is not None
            mark_dispatch_sent_in_transaction(
                connection, reserved.dispatch_id, fetch_attempt_id=None, created_at=at
            )
            terminal = finish_dispatch_in_transaction(
                connection,
                reserved.dispatch_id,
                outcome="billing_unknown",
                created_at=next_day,
                reason="transport_error",
            )
            assert terminal is not None
            self.assertEqual(terminal.business_day, budget_day(at))

    def test_database_trigger_rejects_dispatch_binding_tampering(self) -> None:
        at = now_utc()
        with connect(self.db) as connection, transaction(connection):
            account_id = int(connection.execute("SELECT id FROM accounts LIMIT 1").fetchone()[0])
            usage_id = int(
                connection.execute(
                    """INSERT INTO provider_usage(
                           provider,operation,request_attempts,billed_requests,
                           recorded_at,details_json)
                       VALUES ('TikHub','douyin_user_posts',0,0,?,'{}')""",
                    (at,),
                ).lastrowid
            )
            slot_id = int(
                connection.execute(
                    """INSERT INTO fetch_slots(
                           account_id,stage,window_key,provider,adapter_version,status,
                           created_at,updated_at)
                       VALUES (?,'discovery','dispatch-binding','TikHub','fixture',
                               'running',?,?)""",
                    (account_id, at, at),
                ).lastrowid
            )
            attempt_ids = [
                int(
                    connection.execute(
                        """INSERT INTO fetch_attempts(
                               slot_id,attempt_number,request_started_at)
                           VALUES (?,?,?)""",
                        (slot_id, attempt_number, at),
                    ).lastrowid
                )
                for attempt_number in (1, 2)
            ]
            raw_id = int(
                connection.execute(
                    """INSERT INTO provider_raw_responses(
                           fetch_attempt_id,account_id,provider,operation,local_path,
                           sha256,byte_size,captured_at)
                       VALUES (?,?,'TikHub','douyin_user_posts','fixture.json',?,1,?)""",
                    (attempt_ids[0], account_id, "a" * 64, at),
                ).lastrowid
            )
            reserved = reserve_dispatch_in_transaction(
                connection,
                provider="TikHub",
                operation="douyin_user_posts",
                activation_id=self.activation_id,
                business_day=budget_day(at),
                scheduler_run_id=self.run_id,
                scheduler_attempt_id=self.attempt_id,
                scope={"purpose": "reconcile", "identity_id": 1},
                provider_usage_id=usage_id,
                fetch_slot_id=slot_id,
                cursor_identity={"cursor": 17},
                created_at=at,
            )
            assert reserved is not None

            send_mutations = {
                "provider_usage_id": {"provider_usage_id": None},
                "fetch_slot_id": {"fetch_slot_id": None},
                "cursor_identity": {"cursor_identity_json": '{"cursor":18}'},
                "raw_before_terminal": {"raw_response_id": raw_id},
            }
            for label, mutation in send_mutations.items():
                with self.subTest(event="send_marked", field=label):
                    with self.assertRaises(sqlite3.IntegrityError):
                        self._direct_append(
                            connection,
                            reserved,
                            event_type="send_marked",
                            sequence=2,
                            created_at=at,
                            fetch_attempt_id=attempt_ids[0],
                            **mutation,
                        )

            with self.assertRaises(sqlite3.IntegrityError):
                self._direct_append(
                    connection,
                    reserved,
                    event_type="not_sent",
                    sequence=2,
                    created_at=at,
                    fetch_attempt_id=attempt_ids[0],
                )

            sent = mark_dispatch_sent_in_transaction(
                connection,
                reserved.dispatch_id,
                fetch_attempt_id=attempt_ids[0],
                created_at=at,
            )
            assert sent is not None
            terminal_mutations = {
                "provider_usage_id": {"provider_usage_id": None},
                "fetch_slot_id": {"fetch_slot_id": None},
                "fetch_attempt_id": {"fetch_attempt_id": attempt_ids[1]},
                "cursor_identity": {"cursor_identity_json": '{"cursor":18}'},
            }
            for label, mutation in terminal_mutations.items():
                with self.subTest(event="succeeded", field=label):
                    with self.assertRaises(sqlite3.IntegrityError):
                        self._direct_append(
                            connection,
                            sent,
                            event_type="succeeded",
                            sequence=3,
                            created_at=at,
                            raw_response_id=raw_id,
                            **mutation,
                        )

            terminal = finish_dispatch_in_transaction(
                connection,
                reserved.dispatch_id,
                outcome="succeeded",
                raw_response_id=raw_id,
                created_at=at,
            )
            assert terminal is not None
            self.assertEqual(terminal.raw_response_id, raw_id)
            self.assertEqual(terminal.cursor_identity, {"cursor": 17})


if __name__ == "__main__":
    unittest.main()
