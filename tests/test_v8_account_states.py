from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from v8.account_states import AccountStateError, set_account_enabled, state_events
from v8.storage import connect, initialize_database


class AccountStateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.connection = connect(Path(self.temp.name) / "state.sqlite3")
        initialize_database(self.connection)
        account = self.connection.execute(
            """INSERT INTO accounts(phone,phone_normalized,enabled,created_at,updated_at)
               VALUES ('','',1,'2026-09-01T00:00:00Z','2026-09-01T00:00:00Z')"""
        )
        identity = self.connection.execute(
            """INSERT INTO account_platform_identities(
                   account_id,platform,uid,nickname,source,created_at,updated_at)
               VALUES (?,'douyin','12345678','','test','2026-09-01T00:00:00Z',
                       '2026-09-01T00:00:00Z')""",
            (account.lastrowid,),
        )
        self.identity_id = int(identity.lastrowid or 0)
        self.connection.commit()

    def tearDown(self) -> None:
        self.connection.close()
        self.temp.cleanup()

    def test_change_updates_account_and_appends_event_atomically(self) -> None:
        result = set_account_enabled(
            self.connection,
            self.identity_id,
            enabled=False,
            effective_at="2026-09-01T01:00:00Z",
            created_at="2026-09-01T01:00:00Z",
            actor="operator",
            reason="pause",
        )
        self.assertEqual(result["status"], "changed")
        self.assertFalse(result["new_enabled"])
        enabled = self.connection.execute(
            """SELECT a.enabled FROM accounts a JOIN account_platform_identities i
               ON i.account_id=a.id WHERE i.id=?""",
            (self.identity_id,),
        ).fetchone()[0]
        self.assertEqual(enabled, 0)
        self.assertEqual(len(state_events(self.connection, self.identity_id)), 1)

    def test_unchanged_state_does_not_append_event(self) -> None:
        result = set_account_enabled(
            self.connection,
            self.identity_id,
            enabled=True,
            effective_at="2026-09-01T01:00:00Z",
            created_at="2026-09-01T01:00:00Z",
            actor="operator",
            reason="confirm",
        )
        self.assertEqual(result["status"], "unchanged")
        self.assertEqual(state_events(self.connection, self.identity_id), [])

    def test_second_transition_chains_from_first(self) -> None:
        for enabled, at in (
            (False, "2026-09-01T01:00:00Z"),
            (True, "2026-09-01T02:00:00Z"),
        ):
            set_account_enabled(
                self.connection,
                self.identity_id,
                enabled=enabled,
                effective_at=at,
                created_at=at,
                actor="operator",
                reason="state change",
            )
        values = state_events(self.connection, self.identity_id)
        self.assertEqual([item["new_enabled"] for item in values], [False, True])

    def test_non_increasing_effective_time_fails_and_rolls_back(self) -> None:
        set_account_enabled(
            self.connection,
            self.identity_id,
            enabled=False,
            effective_at="2026-09-01T02:00:00Z",
            created_at="2026-09-01T02:00:00Z",
            actor="operator",
            reason="pause",
        )
        for invalid_at in (
            "2026-09-01T01:00:00Z",
            "2026-09-01T02:00:00Z",
        ):
            with self.subTest(invalid_at=invalid_at):
                with self.assertRaises(AccountStateError) as caught:
                    set_account_enabled(
                        self.connection,
                        self.identity_id,
                        enabled=True,
                        effective_at=invalid_at,
                        created_at="2026-09-01T03:00:00Z",
                        actor="operator",
                        reason="resume",
                    )
                self.assertEqual(
                    caught.exception.code, "account_state_effective_order_invalid"
                )
                enabled = self.connection.execute(
                    """SELECT a.enabled FROM accounts a
                       JOIN account_platform_identities i ON i.account_id=a.id
                       WHERE i.id=?""",
                    (self.identity_id,),
                ).fetchone()[0]
                self.assertEqual(enabled, 0)
                self.assertEqual(len(state_events(self.connection, self.identity_id)), 1)

    def test_schema_rejects_out_of_order_event_even_when_api_is_bypassed(self) -> None:
        set_account_enabled(
            self.connection,
            self.identity_id,
            enabled=False,
            effective_at="2026-09-01T02:00:00Z",
            created_at="2026-09-01T02:00:00Z",
            actor="operator",
            reason="pause",
        )
        self.connection.execute("BEGIN IMMEDIATE")
        self.connection.execute(
            """UPDATE accounts SET enabled=1
               WHERE id=(SELECT account_id FROM account_platform_identities WHERE id=?)""",
            (self.identity_id,),
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                """INSERT INTO account_state_events(
                       account_identity_id,old_enabled,new_enabled,effective_at,
                       actor,reason,contract_version,event_sha256,metadata_json,created_at)
                   VALUES (?,0,1,?,'raw-sql','bypass',?,?,'{}',?)""",
                (
                    self.identity_id,
                    "2026-09-01T01:00:00.000000Z",
                    "account-state-event-v1",
                    "f" * 64,
                    "2026-09-01T03:00:00.000000Z",
                ),
            )
        self.connection.rollback()
        enabled = self.connection.execute(
            """SELECT a.enabled FROM accounts a
               JOIN account_platform_identities i ON i.account_id=a.id WHERE i.id=?""",
            (self.identity_id,),
        ).fetchone()[0]
        self.assertEqual(enabled, 0)
        self.assertEqual(len(state_events(self.connection, self.identity_id)), 1)

    def test_future_and_invalid_values_fail_without_partial_update(self) -> None:
        with self.assertRaises(AccountStateError) as caught:
            set_account_enabled(
                self.connection,
                self.identity_id,
                enabled=False,
                effective_at="2026-09-02T00:00:00Z",
                created_at="2026-09-01T00:00:00Z",
                actor="operator",
                reason="future",
            )
        self.assertEqual(caught.exception.code, "account_state_future_invalid")
        self.assertEqual(state_events(self.connection, self.identity_id), [])

    def test_events_are_append_only(self) -> None:
        value = set_account_enabled(
            self.connection,
            self.identity_id,
            enabled=False,
            effective_at="2026-09-01T01:00:00Z",
            created_at="2026-09-01T01:00:00Z",
            actor="operator",
            reason="pause",
        )
        with self.assertRaises(Exception):
            self.connection.execute(
                "UPDATE account_state_events SET reason='changed' WHERE id=?",
                (value["event_id"],),
            )
        self.connection.rollback()
        with self.assertRaises(Exception):
            self.connection.execute(
                "DELETE FROM account_state_events WHERE id=?", (value["event_id"],)
            )
        self.connection.rollback()


if __name__ == "__main__":
    unittest.main()
