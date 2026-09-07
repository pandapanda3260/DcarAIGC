from __future__ import annotations

import json
import sqlite3
import stat
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.roster_fixture import accept_roster
from v8.operations import upsert_account
from v8.paid_drain import (
    BRIDGE_JOB,
    PaidDrainBlocked,
    PaidDrainError,
    dispatch_state,
    release_paid_drain,
    require_paid_dispatch_open,
    seal_paid_drain,
    start_paid_drain,
    write_audit_mirror,
)
from v8.provider_budget import paid_dispatch_owner, paid_scope
from v8.storage import configure_connection_safety, connect, initialize_database, transaction

STARTED_AT = "2026-09-01T12:00:00Z"
SEALED_AT = "2026-09-01T12:30:00Z"
RELEASED_AT = "2026-09-01T16:01:00Z"


def binding(**changes: object) -> dict[str, object]:
    value: dict[str, object] = {
        "source_activation_id": 94,
        "target_activation_id": "planned:matrix-hybrid-v1",
        "business_day": "2026-09-01",
        "planned_effective_at": "2026-09-01T16:00:00Z",
        "build_receipt_sha256": "a" * 64,
        "runtime_root_receipt_sha256": "b" * 64,
    }
    value.update(changes)
    return value


class PaidDrainTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / "drain.sqlite3"
        with connect(self.db) as connection:
            initialize_database(connection)
        upsert_account(
            {
                "phone": "",
                "platforms": [
                    {"platform": "douyin", "uid": "paid-drain-fixture"}
                ],
            },
            db_path=self.db,
        )
        with connect(self.db) as connection, transaction(connection):
            accept_roster(connection, accepted_at="2026-08-31T16:00:00Z")

    def state(self):
        with connect(self.db) as connection:
            return dispatch_state(connection)

    def test_start_is_atomic_terminal_authority_and_mirror_is_not_authority(self) -> None:
        self.assertEqual(self.state().state, "open")
        with connect(self.db) as connection, transaction(connection):
            gate = require_paid_dispatch_open(
                connection, provider="TikHub", operation="douyin_user_posts"
            )
            self.assertIsNotNone(gate.activation_id)
            self.assertIsNotNone(gate.permit_event_id)
        with connect(self.db) as connection:
            with self.assertRaisesRegex(PaidDrainError, "BEGIN IMMEDIATE"):
                require_paid_dispatch_open(
                    connection, provider="TikHub", operation="douyin_user_posts"
                )

        receipt = start_paid_drain(
            "switch-20260902", binding=binding(), db_path=self.db, now=STARTED_AT
        )
        state = self.state()
        self.assertEqual((state.state, state.drain_id), ("draining", "switch-20260902"))
        with connect(self.db) as connection:
            run = connection.execute(
                "SELECT * FROM scheduler_runs WHERE id=?", (receipt.run_id,)
            ).fetchone()
            attempts = connection.execute(
                "SELECT * FROM scheduler_run_attempts WHERE scheduler_run_id=?",
                (receipt.run_id,),
            ).fetchall()
        self.assertEqual(run["job_id"], BRIDGE_JOB)
        self.assertEqual(run["status"], "succeeded")
        self.assertEqual(len(attempts), 1)
        self.assertEqual(attempts[0]["status"], "succeeded")
        self.assertEqual(attempts[0]["details_json"], run["details_json"])
        self.assertTrue(json.loads(run["details_json"])["complete"])

        mirror = write_audit_mirror(receipt, self.root / "paid-drain")
        self.assertEqual(stat.S_IMODE(mirror.stat().st_mode), 0o600)
        mirror.unlink()
        self.assertEqual(self.state().state, "draining")
        with connect(self.db) as connection, transaction(connection):
            with self.assertRaises(PaidDrainBlocked) as blocked:
                require_paid_dispatch_open(
                    connection, provider="TikHub", operation="douyin_user_posts"
                )
        self.assertEqual(blocked.exception.error_code, "profile_switch_drain")

    def test_start_freezes_a_running_direct_paid_owner(self) -> None:
        with paid_scope("metrics"):
            with paid_dispatch_owner(
                job_id="paid_capture_direct",
                identity={
                    "provider": "tikhub",
                    "operation": "douyin_video_statistics",
                    "content_id": 1,
                    "purpose": "metrics",
                },
                db_path=self.db,
                at=STARTED_AT,
            ):
                receipt = start_paid_drain(
                    "switch-direct-owner",
                    binding=binding(),
                    db_path=self.db,
                    now=STARTED_AT,
                )
                running = receipt.payload["frozen_dispatch"]["paid_running_attempts"]
                self.assertEqual(
                    [entry["job_id"] for entry in running], ["paid_capture_direct"]
                )
        with connect(self.db) as connection:
            run = connection.execute(
                "SELECT status FROM scheduler_runs "
                "WHERE job_id='paid_capture_direct'"
            ).fetchone()
        self.assertEqual(run["status"], "succeeded")
        self.assertEqual(self.state().state, "draining")

    def test_idempotency_illegal_transitions_and_release_reopens(self) -> None:
        first = start_paid_drain(
            "switch-1", binding=binding(), db_path=self.db, now=STARTED_AT
        )
        repeated = start_paid_drain(
            "switch-1", binding=binding(), db_path=self.db, now=SEALED_AT
        )
        self.assertEqual((repeated.run_id, repeated.event_hash), (first.run_id, first.event_hash))
        with self.assertRaisesRegex(PaidDrainError, "changed its frozen binding"):
            start_paid_drain(
                "switch-1",
                binding=binding(build_receipt_sha256="c" * 64),
                db_path=self.db,
            )
        with self.assertRaisesRegex(PaidDrainError, "already active"):
            start_paid_drain("switch-2", binding=binding(), db_path=self.db)
        with self.assertRaisesRegex(PaidDrainError, "valid SEALED"):
            release_paid_drain("switch-1", db_path=self.db)

        sealed = seal_paid_drain("switch-1", db_path=self.db, now=SEALED_AT)
        self.assertEqual(
            seal_paid_drain("switch-1", db_path=self.db).run_id, sealed.run_id
        )
        self.assertEqual(self.state().state, "sealed")
        released = release_paid_drain(
            "switch-1", db_path=self.db, now=RELEASED_AT
        )
        self.assertEqual(
            release_paid_drain("switch-1", db_path=self.db).run_id, released.run_id
        )
        self.assertEqual(self.state().state, "open")
        with connect(self.db) as connection, transaction(connection):
            require_paid_dispatch_open(
                connection, provider="newrank_matrix", operation="works"
            )

        with connect(self.db) as connection:
            rows = connection.execute(
                "SELECT status,details_json FROM scheduler_runs "
                "WHERE job_id=? AND scheduled_for LIKE 'paid-drain:switch-1:%' ORDER BY id",
                (BRIDGE_JOB,),
            ).fetchall()
        self.assertEqual(len(rows), 3)
        self.assertEqual({row["status"] for row in rows}, {"succeeded"})
        events = [json.loads(row["details_json"])["event"] for row in rows]
        self.assertEqual([event["event_type"] for event in events], ["start", "sealed", "release"])
        self.assertEqual(events[1]["previous_event_hash"], events[0]["event_hash"])
        self.assertEqual(events[2]["previous_event_hash"], events[1]["event_hash"])

    def _insert_active_dispatches(self) -> tuple[int, int, int]:
        with connect(self.db) as connection, transaction(connection):
            timestamp = STARTED_AT
            account_id = int(
                connection.execute(
                    "INSERT INTO accounts(phone,operator_name,created_at,updated_at) "
                    "VALUES ('','','','')"
                ).lastrowid
            )

            def slot() -> int:
                return int(
                    connection.execute(
                        "INSERT INTO fetch_slots(account_id,stage,window_key,provider,"
                        "adapter_version,status,created_at,updated_at) "
                        "VALUES (?,'discovery',?,'TikHub','fixture','running',?,?)",
                        (account_id, f"window-{connection.total_changes}", timestamp, timestamp),
                    ).lastrowid
                )

            reserved_slot = slot()
            sent_slot = slot()
            sent_attempt = int(
                connection.execute(
                    "INSERT INTO fetch_attempts(slot_id,attempt_number,request_started_at) "
                    "VALUES (?,1,?)",
                    (sent_slot, timestamp),
                ).lastrowid
            )
            connection.execute(
                "UPDATE fetch_slots SET attempt_count=1 WHERE id=?", (sent_slot,)
            )

            def usage(slot_id: int, state: str, request_attempts: int) -> int:
                details = {
                    "policy_version": "tikhub-global-budget-v2",
                    "state": state,
                    "budget_day": "2026-09-01",
                    "category": "reconcile",
                    "scope": {"business_day": "2026-09-01"},
                    "slot_id": slot_id,
                    "attempt_number": 1,
                }
                return int(
                    connection.execute(
                        "INSERT INTO provider_usage(provider,operation,request_attempts,"
                        "billed_requests,currency,amount,recorded_at,details_json) "
                        "VALUES ('TikHub','douyin_user_posts',?,1,'USD',0.001,?,?)",
                        (request_attempts, timestamp, json.dumps(details)),
                    ).lastrowid
                )

            reserved_usage = usage(reserved_slot, "reserved", 0)
            sent_usage = usage(sent_slot, "sent", 1)
            # Even a current-contract billing_unknown row is already terminal
            # for network drain. It remains monetary debt, not an in-flight
            # request in this START receipt.
            unknown_details = {
                "policy_version": "tikhub-global-budget-v2",
                "state": "billing_unknown",
                "budget_day": "2026-08-29",
                "category": "reconcile",
                "scope": {"business_day": "2026-08-29"},
                "slot_id": 999999,
                "attempt_number": 1,
            }
            connection.execute(
                "INSERT INTO provider_usage(provider,operation,request_attempts,"
                "billed_requests,currency,amount,recorded_at,details_json) "
                "VALUES ('TikHub','douyin_user_posts',1,1,'USD',0.001,?,?)",
                ("2026-08-29T00:00:00Z", json.dumps(unknown_details)),
            )
            # An old-format reservation remains disclosed in the ledger, but
            # is not fabricated into this START's exact in-flight set.
            connection.execute(
                "INSERT INTO provider_usage(provider,operation,request_attempts,"
                "billed_requests,currency,amount,recorded_at,details_json) "
                "VALUES ('TikHub','legacy',0,1,'USD',1,?,?)",
                (timestamp, json.dumps({"state": "reserved"})),
            )
            run_id = int(
                connection.execute(
                    "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,details_json) "
                    "VALUES ('matrix_works_scan','scan:fixture','running',?,?)",
                    (
                        timestamp,
                        json.dumps({"checkpoint": {"network_requests": 3}}),
                    ),
                ).lastrowid
            )
            attempt_id = int(
                connection.execute(
                    "INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,"
                    "invocation_source,status,started_at,details_json) "
                    "VALUES (?,1,'scheduled','running',?,?)",
                    (
                        run_id,
                        timestamp,
                        json.dumps({"checkpoint": {"network_requests": 3}}),
                    ),
                ).lastrowid
            )
        self.assertGreater(sent_attempt, 0)
        return reserved_usage, sent_usage, attempt_id

    def test_seal_waits_only_for_frozen_current_dispatches(self) -> None:
        reserved_usage, sent_usage, matrix_attempt = self._insert_active_dispatches()
        started = start_paid_drain(
            "switch-tail", binding=binding(), db_path=self.db, now=STARTED_AT
        )
        frozen = started.payload["frozen_dispatch"]
        self.assertEqual(
            [item["usage_id"] for item in frozen["tikhub_reserved"]],
            [reserved_usage],
        )
        self.assertEqual(
            [item["usage_id"] for item in frozen["tikhub_send_marked"]],
            [sent_usage],
        )
        self.assertEqual(len(frozen["matrix_running_dispatches"]), 1)
        with self.assertRaisesRegex(PaidDrainError, "unresolved frozen dispatches"):
            seal_paid_drain("switch-tail", db_path=self.db)

        with connect(self.db) as connection, transaction(connection):
            for usage_id, next_state in (
                (reserved_usage, "not_sent"),
                (sent_usage, "billing_unknown"),
            ):
                row = connection.execute(
                    "SELECT details_json FROM provider_usage WHERE id=?", (usage_id,)
                ).fetchone()
                details = json.loads(row["details_json"])
                details["state"] = next_state
                connection.execute(
                    "UPDATE provider_usage SET details_json=? WHERE id=?",
                    (json.dumps(details), usage_id),
                )
            run_id = int(
                connection.execute(
                    "SELECT scheduler_run_id FROM scheduler_run_attempts WHERE id=?",
                    (matrix_attempt,),
                ).fetchone()[0]
            )
            completed = json.dumps({"checkpoint": {"network_requests": 4}})
            connection.execute(
                "UPDATE scheduler_run_attempts SET status='succeeded',completed_at=?,"
                "details_json=? WHERE id=?",
                (SEALED_AT, completed, matrix_attempt),
            )
            connection.execute(
                "UPDATE scheduler_runs SET status='succeeded',completed_at=?,details_json=? "
                "WHERE id=?",
                (SEALED_AT, completed, run_id),
            )
        sealed = seal_paid_drain(
            "switch-tail", db_path=self.db, now=SEALED_AT
        )
        self.assertEqual(
            sealed.payload["verification"]["matrix_network_request_deltas"],
            {str(run_id): 1},
        )

    def test_post_start_paid_tail_permanently_blocks_seal(self) -> None:
        start_paid_drain("switch-tail", binding=binding(), db_path=self.db)
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                "INSERT INTO provider_usage(provider,operation,request_attempts,billed_requests,"
                "currency,amount,recorded_at,details_json) "
                "VALUES ('TikHub','douyin_user_posts',1,1,'USD',0.001,?,?)",
                (STARTED_AT, json.dumps({"state": "completed"})),
            )
        with self.assertRaisesRegex(PaidDrainError, "post-START dispatch tail"):
            seal_paid_drain("switch-tail", db_path=self.db)
        self.assertEqual(self.state().state, "draining")

    def test_run_tampering_is_invalid_and_fail_closed(self) -> None:
        receipt = start_paid_drain(
            "switch-invalid", binding=binding(), db_path=self.db
        )
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                "UPDATE scheduler_runs SET details_json='{}' WHERE id=?", (receipt.run_id,)
            )
        self.assertEqual(self.state().state, "invalid")
        with connect(self.db) as connection, transaction(connection):
            with self.assertRaises(PaidDrainBlocked):
                require_paid_dispatch_open(
                    connection, provider="TikHub", operation="douyin_user_posts"
                )

    def test_mutating_bridge_job_id_cannot_hide_authoritative_attempt(self) -> None:
        receipt = start_paid_drain(
            "switch-hidden", binding=binding(), db_path=self.db, now=STARTED_AT
        )
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                "UPDATE scheduler_runs SET job_id='not-the-bridge' WHERE id=?",
                (receipt.run_id,),
            )
        state = self.state()
        self.assertEqual(state.state, "invalid")
        with connect(self.db) as connection, transaction(connection):
            with self.assertRaises(PaidDrainBlocked):
                require_paid_dispatch_open(
                    connection, provider="TikHub", operation="douyin_user_posts"
                )

    def test_release_mirror_is_written_before_db_reopens(self) -> None:
        start_paid_drain(
            "switch-release-mirror", binding=binding(), db_path=self.db, now=STARTED_AT
        )
        seal_paid_drain(
            "switch-release-mirror", db_path=self.db, now=SEALED_AT
        )
        mirror_root = self.root / "release-mirror"
        with patch(
            "v8.paid_drain.write_audit_mirror",
            side_effect=OSError("fixture mirror failure"),
        ), self.assertRaisesRegex(OSError, "fixture mirror failure"):
            release_paid_drain(
                "switch-release-mirror",
                db_path=self.db,
                now=RELEASED_AT,
                mirror_root=mirror_root,
            )
        self.assertEqual(self.state().state, "sealed")
        receipt = release_paid_drain(
            "switch-release-mirror",
            db_path=self.db,
            now=RELEASED_AT,
            mirror_root=mirror_root,
        )
        self.assertEqual(self.state().state, "open")
        self.assertEqual(
            json.loads(
                (mirror_root / "switch-release-mirror.release.json").read_text()
            )["event_hash"],
            receipt.event_hash,
        )

    def test_begin_immediate_linearizes_send_gate_before_start(self) -> None:
        # Use a raw connection here so the test exercises SQLite's cross-writer
        # lock, not storage.transaction's in-process serialization helper.
        sender = sqlite3.connect(self.db, timeout=3)
        sender.row_factory = sqlite3.Row
        configure_connection_safety(sender)
        sender.execute("PRAGMA busy_timeout=3000")
        sender.execute("BEGIN IMMEDIATE")
        require_paid_dispatch_open(
            sender, provider="TikHub", operation="douyin_user_posts"
        )

        done = threading.Event()
        outcome: list[object] = []

        def start() -> None:
            try:
                outcome.append(
                    start_paid_drain(
                        "switch-race", binding=binding(), db_path=self.db
                    )
                )
            except Exception as exc:  # pragma: no cover - asserted below
                outcome.append(exc)
            finally:
                done.set()

        thread = threading.Thread(target=start)
        thread.start()
        time.sleep(0.05)
        self.assertFalse(done.is_set())
        sender.commit()
        sender.close()
        thread.join(timeout=3)
        self.assertTrue(done.is_set())
        self.assertEqual(len(outcome), 1)
        self.assertNotIsInstance(outcome[0], Exception)
        self.assertEqual(self.state().state, "draining")

        # START won for all later send transactions, so the final gate is
        # blocked before an external call can be made.
        with connect(self.db) as connection, transaction(connection):
            with self.assertRaises(PaidDrainBlocked):
                require_paid_dispatch_open(
                    connection, provider="TikHub", operation="douyin_user_posts"
                )


if __name__ == "__main__":
    unittest.main()
