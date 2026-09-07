from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from passlib.hash import sha512_crypt


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT / "src" / "dcar_eval"))

from dcar_auth import store as auth_store  # noqa: E402


PEPPER = b"pepper-for-tests-0123456789abcdef0123456789"
USERNAME = "operator"
PHONE = "13800138000"
NEW_PHONE = "13900139000"
IP = "203.0.113.5"
HASH = sha512_crypt.using(rounds=5000).hash("correct-password")
TTL = 3600


def _run_concurrently(count: int, target):
    barrier = threading.Barrier(count)
    results: list[object] = []
    lock = threading.Lock()

    def worker() -> None:
        barrier.wait()
        try:
            outcome = target()
        except Exception as exc:  # noqa: BLE001 - collected for assertions
            outcome = exc
        with lock:
            results.append(outcome)

    threads = [threading.Thread(target=worker) for _ in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return results


class AuthStoreTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "sessions.sqlite3"
        self.store = auth_store.AuthStore(
            self.path, pepper=PEPPER, throttle_window_seconds=600,
            throttle_max_failures=3, sms_daily_cap=300,
        )
        self.store.initialize()
        self._insert_user(USERNAME, PHONE)
        self.store.allow_phone(NEW_PHONE)

    def _insert_user(self, username: str, phone: str | None, status: str = "active") -> None:
        connection = sqlite3.connect(self.path)
        with connection:
            connection.execute(
                "INSERT INTO auth_users(username, phone, password_hash, status, "
                "role, created_at, password_updated_at) VALUES(?,?,?,?,'operator',1,1)",
                (username, phone, HASH, status),
            )
        connection.close()

    def _sent_code(self, purpose: str, phone: str = PHONE, ip: str = IP) -> tuple[int, str]:
        code = auth_store.generate_code()
        challenge_id, _ = self.store.reserve_send(purpose, phone, ip, code)
        self.assertTrue(self.store.finish_send(challenge_id, "sent", "OK"))
        return challenge_id, code

    def _set(self, challenge_id: int, **columns: object) -> None:
        connection = sqlite3.connect(self.path)
        with connection:
            for name, value in columns.items():
                connection.execute(
                    f"UPDATE auth_challenges SET {name}=? WHERE id=?", (value, challenge_id)
                )
        connection.close()

    def _invalidations(self) -> list[tuple[int, str | None]]:
        connection = sqlite3.connect(self.path)
        try:
            return [
                (int(row[0]), row[1])
                for row in connection.execute(
                    "SELECT id, invalidated_reason FROM auth_challenges "
                    "WHERE invalidated_at IS NOT NULL ORDER BY id"
                )
            ]
        finally:
            connection.close()

    def _user_version(self) -> int:
        connection = sqlite3.connect(self.path)
        try:
            return int(connection.execute("PRAGMA user_version").fetchone()[0])
        finally:
            connection.close()

    def _restore_schema_two_fixture(self) -> None:
        """Use the actual v2 account constraint, not just a version marker."""
        with sqlite3.connect(self.path) as connection:
            connection.execute("""
                CREATE TABLE auth_users_v2(
                    username TEXT PRIMARY KEY COLLATE NOCASE,
                    phone TEXT UNIQUE,
                    password_hash TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'active'
                        CHECK(status IN ('active','disabled')),
                    role TEXT NOT NULL DEFAULT 'operator'
                        CHECK(role IN ('superadmin','admin','operator')),
                    created_at INTEGER NOT NULL,
                    password_updated_at INTEGER NOT NULL
                )
            """)
            connection.execute("INSERT INTO auth_users_v2 SELECT * FROM auth_users")
            connection.execute("DROP TABLE auth_users")
            connection.execute("ALTER TABLE auth_users_v2 RENAME TO auth_users")
            connection.execute("PRAGMA user_version=2")
        events = [json.loads(line) for line in self.store.change_log_path.read_text().splitlines()]
        for event in events:
            event["user_version"] = 2
        self.store.change_log_path.write_text(
            "".join(json.dumps(event) + "\n" for event in events), encoding="utf-8"
        )

    def _database_rows(self) -> dict[str, list[tuple[object, ...]]]:
        with sqlite3.connect(self.path) as connection:
            return {
                table: connection.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()
                for table in auth_store.REQUIRED_TABLES
            }

    # ---------------------------------------------------------------- schema

    def test_initialize_upgrades_legacy_session_only_database(self) -> None:
        legacy = Path(self.temporary.name) / "legacy.sqlite3"
        connection = sqlite3.connect(legacy)
        with connection:
            connection.execute(
                "CREATE TABLE auth_sessions(token_sha256 TEXT PRIMARY KEY, username TEXT NOT NULL, "
                "created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL)"
            )
            connection.execute(
                "INSERT INTO auth_sessions VALUES('abc', 'operator', 1, 9999999999)"
            )
        connection.close()
        store = auth_store.AuthStore(legacy, pepper=PEPPER)
        self.assertEqual(store.initialize(), (0, 3))
        self.assertEqual(store.initialize(), (3, 3))
        connection = sqlite3.connect(legacy)
        try:
            self.assertEqual(int(connection.execute("PRAGMA user_version").fetchone()[0]), 3)
            columns = {row[1] for row in connection.execute("PRAGMA table_info(auth_sessions)")}
            self.assertIn("credential_fingerprint", columns)
            columns = {row[1] for row in connection.execute("PRAGMA table_info(auth_users)")}
            self.assertIn("role", columns)
            tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertTrue(auth_store.REQUIRED_TABLES.issubset(tables))
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM auth_sessions").fetchone()[0], 1)
            self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone()[0], "delete")
        finally:
            connection.close()
        store.healthcheck()

    def test_initialize_upgrades_schema_1_in_place_and_refuses_unknown_versions(self) -> None:
        # A schema-1 store (login upgrade) plus a schema-2 shape written before the
        # tombstone kept the phone number; historical users keep operator access.
        self.assertEqual(self._user_version(), 3)
        connection = sqlite3.connect(self.path)
        with connection:
            connection.execute("DROP TABLE auth_deleted_users")
            connection.execute("ALTER TABLE auth_users DROP COLUMN role")
            connection.execute(
                "CREATE TABLE auth_deleted_users(username TEXT PRIMARY KEY COLLATE NOCASE, "
                "deleted_at INTEGER NOT NULL, deleted_by TEXT NOT NULL, role TEXT NOT NULL)"
            )
            connection.execute(
                "INSERT INTO auth_deleted_users VALUES('gone_user', 1, 'root', 'operator')"
            )
            connection.execute("PRAGMA user_version = 1")
        connection.close()
        with self.assertRaises(auth_store.SchemaVersionError):
            self.store.healthcheck()
        self.assertEqual(self.store.initialize(), (1, 3))
        self.store.healthcheck()
        self.assertEqual(self._user_version(), 3)
        self.assertEqual(self.store.get_user(USERNAME).role, "operator")
        connection = sqlite3.connect(self.path)
        try:
            columns = {row[1] for row in connection.execute("PRAGMA table_info(auth_deleted_users)")}
            self.assertIn("phone", columns)
        finally:
            connection.close()
        self.assertTrue(self.store.username_reserved("GONE_USER"))
        challenge_id, code = self._sent_code("register", NEW_PHONE)
        with self.assertRaises(auth_store.UsernameTaken):
            self.store.prepare_register("gone_user", NEW_PHONE, code, IP)
        challenge = self.store.prepare_register("fresh_user", NEW_PHONE, code, IP)
        self.store.complete_register("fresh_user", NEW_PHONE, HASH, challenge, TTL)
        user = self.store.get_user("fresh_user")
        assert user is not None
        self.assertEqual(user.status, "active")
        connection = sqlite3.connect(self.path)
        with connection:
            connection.execute("PRAGMA user_version = 4")
        connection.close()
        with self.assertRaises(auth_store.SchemaVersionError):
            self.store.initialize()
        with self.assertRaises(auth_store.SchemaVersionError):
            self.store.healthcheck()

    # ------------------------------------------------------------ send limits

    def test_schema_two_migration_preserves_accounts_sessions_ledgers_and_audit(self) -> None:
        self.store.create_user("boss", HASH, role="superadmin")
        self.store.create_user("lead", HASH, role="admin", status="disabled")
        self.store.create_user("gone", HASH, phone="13700137000")
        self.store.delete_user_cli("gone")
        token = self.store.create_session(USERNAME, TTL)
        self._sent_code("login")
        with sqlite3.connect(self.path) as connection:
            connection.execute("INSERT INTO auth_failures(key, at) VALUES('user:operator', 42)")
        self._restore_schema_two_fixture()
        with sqlite3.connect(self.path) as connection:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("UPDATE auth_users SET role='new_user' WHERE username='operator'")
            connection.execute("CREATE INDEX users_by_role ON auth_users(role)")
            connection.execute(
                "CREATE TABLE user_preferences(username TEXT REFERENCES auth_users(username) "
                "ON DELETE CASCADE, value TEXT)"
            )
            connection.execute("INSERT INTO user_preferences VALUES('operator', 'kept')")
            connection.execute("CREATE TABLE role_history(username TEXT, role TEXT)")
            connection.execute(
                "CREATE TRIGGER users_role_audit AFTER UPDATE OF role ON auth_users "
                "BEGIN INSERT INTO role_history VALUES(NEW.username, NEW.role); END"
            )
        before = self._database_rows()
        audit_before = self.store.change_log_path.read_bytes()
        with self.assertRaises(auth_store.SchemaVersionError):
            self.store.healthcheck()
        self.assertEqual(self.store.initialize(), (2, 3))
        self.store.healthcheck()
        self.assertEqual(self._database_rows(), before)
        self.assertEqual(self.store.change_log_path.read_bytes(), audit_before)
        self.assertTrue(all(entry["user_version"] == 2 for entry in self.store.read_changes()))
        self.assertEqual(self.store.resolve_principal(token).role, "operator")
        self.assertEqual(self.store.initialize(), (3, 3))
        with sqlite3.connect(self.path) as connection:
            self.assertEqual(connection.execute("SELECT * FROM user_preferences").fetchall(), [("operator", "kept")])
            self.assertIsNotNone(connection.execute("SELECT sql FROM sqlite_master WHERE name='users_by_role'").fetchone())
            self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])
            connection.execute(
                "INSERT INTO auth_users(username,password_hash,created_at,password_updated_at) "
                "VALUES('defaulted',?,1,1)", (HASH,)
            )
        self.assertEqual(self.store.get_user("defaulted").role, "new_user")
        self.store.set_role("defaulted", "operator")
        with sqlite3.connect(self.path) as connection:
            self.assertEqual(connection.execute("SELECT * FROM role_history").fetchall(), [("defaulted", "operator")])
        self.assertTrue(self.store.change_log_path.read_bytes().startswith(audit_before))
        self.assertEqual(self.store.read_changes()[-1]["user_version"], 3)

    def test_schema_two_rebuild_failure_rolls_back_all_data_and_version(self) -> None:
        self._restore_schema_two_fixture()
        before = self._database_rows()
        audit_before = self.store.change_log_path.read_bytes()
        original = auth_store.AuthStore._upgrade_user_roles

        def fail_after_rebuild(connection: sqlite3.Connection) -> None:
            original(connection)
            raise RuntimeError("simulated migration failure")

        with patch.object(auth_store.AuthStore, "_upgrade_user_roles", side_effect=fail_after_rebuild):
            with self.assertRaisesRegex(RuntimeError, "simulated migration failure"):
                self.store.initialize()
        self.assertEqual(self._user_version(), 2)
        self.assertEqual(self._database_rows(), before)
        self.assertEqual(self.store.change_log_path.read_bytes(), audit_before)
        with sqlite3.connect(self.path) as connection:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("UPDATE auth_users SET role='new_user'")
            self.assertIsNone(connection.execute("SELECT name FROM sqlite_master WHERE name='auth_users_v3'").fetchone())

    def test_healthcheck_rejects_schema_three_marker_with_old_role_constraint(self) -> None:
        self._restore_schema_two_fixture()
        with sqlite3.connect(self.path) as connection:
            connection.execute("PRAGMA user_version=3")
        before = self.path.read_bytes()
        with self.assertRaisesRegex(RuntimeError, "new_user role/default"):
            self.store.healthcheck()
        self.assertEqual(self.path.read_bytes(), before)

    def test_schema_three_default_is_new_user_but_explicit_admin_creation_is_operator(self) -> None:
        with sqlite3.connect(self.path) as connection:
            connection.execute(
                "INSERT INTO auth_users(username,password_hash,created_at,password_updated_at) "
                "VALUES('defaulted',?,1,1)", (HASH,)
            )
        self.store.create_user("provisioned", HASH)
        self.assertEqual(self.store.get_user("defaulted").role, "new_user")
        self.assertEqual(self.store.get_user("provisioned").role, "operator")
        self.assertEqual(auth_store.ROLE_RANK["new_user"], 0)
        self.assertNotIn("new_user", auth_store.USER_ADMIN_ROLES)

    def test_send_limits_are_rolling_and_count_all_statuses(self) -> None:
        first, _ = self.store.reserve_send("login", PHONE, IP, "111111")
        self.store.finish_send(first, "rejected", "isv.BUSINESS_LIMIT_CONTROL")
        with self.assertRaises(auth_store.RateLimited) as caught:
            self.store.reserve_send("login", PHONE, IP, "222222")
        self.assertGreaterEqual(caught.exception.retry_after, 1)
        self.assertLessEqual(caught.exception.retry_after, 61)
        with patch.object(auth_store, "PHONE_SEND_LIMITS", ((3600, 5), (86400, 10))):
            for _ in range(4):
                challenge_id, _ = self.store.reserve_send("login", PHONE, IP, "333333")
                self.store.finish_send(challenge_id, "sent", "OK")
            with self.assertRaises(auth_store.RateLimited):
                self.store.reserve_send("login", PHONE, IP, "444444")
        with patch.object(auth_store, "PHONE_SEND_LIMITS", ()), patch.object(
            auth_store, "IP_SEND_LIMITS", ((3600, 5),)
        ):
            with self.assertRaises(auth_store.RateLimited):
                self.store.reserve_send("login", PHONE, IP, "555555")
            other_ip, _ = self.store.reserve_send("login", PHONE, "198.51.100.2", "555555")
            self.assertIsNotNone(other_ip)
        with patch.object(auth_store, "PHONE_SEND_LIMITS", ()), patch.object(
            auth_store, "IP_SEND_LIMITS", ()
        ):
            self.store.sms_daily_cap = 6
            with self.assertRaises(auth_store.RateLimited):
                self.store.reserve_send("login", PHONE, "198.51.100.3", "666666")

    def test_reservation_rejections_commit_failures_without_challenges(self) -> None:
        self.store.throttle_max_failures = 4
        with self.assertRaises(auth_store.PhoneNotRegistered):
            self.store.reserve_send("login", "13600136000", IP, "111111")
        with self.assertRaises(auth_store.PhoneRegistered):
            self.store.reserve_send("register", PHONE, IP, "111111")
        with self.assertRaises(auth_store.PhoneNotAllowed):
            self.store.reserve_send("register", "13700137000", IP, "111111")
        self.store.set_status(USERNAME, "disabled")
        with self.assertRaises(auth_store.AccountDisabled):
            self.store.reserve_send("login", PHONE, IP, "111111")
        self.assertEqual(self.store.failure_count(auth_store.AuthStore.ip_key(IP)), 4)
        self.assertEqual(self.store.counts()["auth_challenges"], 0)
        with self.assertRaises(auth_store.RateLimited):
            self.store.reserve_send("register", NEW_PHONE, IP, "111111")

    def test_concurrent_reservations_admit_exactly_one(self) -> None:
        results = _run_concurrently(
            20, lambda: self.store.reserve_send("login", PHONE, IP, "123456")
        )
        admitted = [r for r in results if isinstance(r, tuple)]
        limited = [r for r in results if isinstance(r, auth_store.RateLimited)]
        self.assertEqual(len(admitted), 1)
        self.assertEqual(len(limited), 19)
        self.assertEqual(admitted[0][1], USERNAME)

    # ----------------------------------------------------------- code states

    def test_only_latest_sent_row_is_valid_and_never_falls_back(self) -> None:
        self.store.throttle_max_failures = 100
        first, first_code = self._sent_code("login")
        with patch.object(auth_store, "PHONE_SEND_LIMITS", ()):
            second, second_code = self._sent_code("login")
            self.assertNotEqual(first_code, second_code)
            with self.assertRaises(auth_store.InvalidCode):
                self.store.login_with_code(PHONE, first_code, IP, TTL)
            pending, _ = self.store.reserve_send("login", PHONE, IP, "999999")
            rejected, _ = self.store.reserve_send("login", PHONE, IP, "888888")
            self.store.finish_send(rejected, "rejected", "isv.AMOUNT_NOT_ENOUGH")
            unknown, unknown_code = self.store.reserve_send("login", PHONE, IP, "777777")
            self.store.finish_send(unknown, "unknown", "timeout")
            with self.assertRaises(auth_store.InvalidCode):
                self.store.login_with_code(PHONE, "777777", IP, TTL)
            token, username = self.store.login_with_code(PHONE, second_code, IP, TTL)
            self.assertEqual(username, USERNAME)
            self.assertEqual(self.store.resolve_session(token), USERNAME)
            with self.assertRaises(auth_store.InvalidCode):
                self.store.login_with_code(PHONE, second_code, IP, TTL)
            self._set(second, consumed_at=None)
            self._set(second, expires_at=int(time.time()) - 1)
            with self.assertRaises(auth_store.InvalidCode):
                self.store.login_with_code(PHONE, second_code, IP, TTL)
            self.assertTrue(self.store.finish_send(pending, "sent", "OK"))
            self.assertFalse(self.store.finish_send(pending, "sent", "OK"))

    def test_finish_send_retains_provider_outcome_without_reviving_retired_row(self) -> None:
        challenge, _ = self.store.reserve_send("login", PHONE, IP, "111111")
        self.store.revoke_challenges(USERNAME)
        self.assertFalse(self.store.finish_send(challenge, "sent", "Tencent.OK"))
        self.assertEqual(self.store.challenge_status(challenge), "sent")
        self.assertEqual(self._invalidations(), [(challenge, "revoked")])
        with self.assertRaises(auth_store.InvalidCode):
            self.store.login_with_code(PHONE, "111111", IP, TTL)
        self.assertFalse(self.store.finish_send(challenge, "unknown", "timeout"))
        self.assertEqual(self.store.challenge_status(challenge), "sent")
        with sqlite3.connect(self.path) as connection:
            row = connection.execute(
                "SELECT provider_code FROM auth_challenges WHERE id=?", (challenge,)
            ).fetchone()
        self.assertEqual(row[0], "Tencent.OK")

    def test_wrong_code_attempts_are_committed_and_exhaust(self) -> None:
        self.store.throttle_max_failures = 50
        challenge_id, code = self._sent_code("login")
        wrong = "000000" if code != "000000" else "111111"
        for _ in range(auth_store.CODE_MAX_ATTEMPTS):
            with self.assertRaises(auth_store.InvalidCode):
                self.store.login_with_code(PHONE, wrong, IP, TTL)
        with self.assertRaises(auth_store.InvalidCode):
            self.store.login_with_code(PHONE, code, IP, TTL)
        self.assertEqual(self.store.failure_count(auth_store.AuthStore.phone_key(PHONE)), 6)

    def test_concurrent_consumption_admits_exactly_one(self) -> None:
        _challenge, code = self._sent_code("login")
        self.store.throttle_max_failures = 100
        results = _run_concurrently(
            20, lambda: self.store.login_with_code(PHONE, code, IP, TTL)
        )
        successes = [r for r in results if isinstance(r, tuple)]
        failures = [r for r in results if isinstance(r, auth_store.InvalidCode)]
        self.assertEqual(len(successes), 1)
        self.assertEqual(len(failures), 19)

    def test_challenges_are_bound_to_the_account(self) -> None:
        self._insert_user("second", NEW_PHONE)
        self.store.remove_allowed_phone(NEW_PHONE)
        challenge_id, code = self._sent_code("login")
        self.store.allow_phone(PHONE)
        self.store.set_phone("second", "13500135000")
        self.store.set_phone(USERNAME, NEW_PHONE)
        with self.assertRaises(auth_store.InvalidCode):
            self.store.login_with_code(PHONE, code, IP, TTL)
        # R1: the row is retired, not deleted, so the send ledger keeps counting it.
        self.assertEqual(self.store.counts()["auth_challenges"], 1)
        self.assertEqual(self._invalidations(), [(challenge_id, "phone_changed")])
        # R2: the old number left the admission list together with the rebinding.
        self.assertFalse(self.store.phone_allowed(PHONE))
        with self.assertRaises(auth_store.RateLimited):
            self.store.reserve_send("login", PHONE, IP, "123456")
        _challenge, code = self._sent_code("login", NEW_PHONE)
        connection = sqlite3.connect(self.path)
        with connection:
            connection.execute("UPDATE auth_challenges SET username='second'")
        connection.close()
        with self.assertRaises(auth_store.InvalidCode):
            self.store.login_with_code(NEW_PHONE, code, IP, TTL)

    # ----------------------------------------------------------- registration

    def test_register_two_phase_consumes_only_on_completion(self) -> None:
        _challenge, code = self._sent_code("register", NEW_PHONE)
        challenge_id = self.store.prepare_register("new_user", NEW_PHONE, code, IP)
        with self.assertRaises(auth_store.UsernameTaken):
            self.store.prepare_register("OPERATOR", NEW_PHONE, code, IP)
        challenge_id = self.store.prepare_register("new_user", NEW_PHONE, code, IP)
        wrong = "000000" if code != "000000" else "111111"
        with self.assertRaises(auth_store.InvalidCode):
            self.store.prepare_register("new_user", NEW_PHONE, wrong, IP)
        token = self.store.complete_register("new_user", NEW_PHONE, HASH, challenge_id, TTL)
        self.assertEqual(self.store.resolve_session(token), "new_user")
        self.assertEqual(self.store.get_user("new_user").role, "new_user")
        self.assertEqual(self.store.resolve_principal(token).role, "new_user")
        self.assertEqual(self.store.read_changes()[-1]["after"]["role"], "new_user")
        self.store.allow_phone("13700137000")
        with self.assertRaises(auth_store.InvalidCode):
            self.store.complete_register("another", "13700137000", HASH, challenge_id, TTL)

    def test_concurrent_registration_admits_one_and_keeps_code_for_loser(self) -> None:
        _challenge, code = self._sent_code("register", NEW_PHONE)
        self.store.throttle_max_failures = 100
        challenge_id = self.store.prepare_register("new_user", NEW_PHONE, code, IP)

        def attempt():
            return self.store.complete_register("new_user", NEW_PHONE, HASH, challenge_id, TTL)

        results = _run_concurrently(10, attempt)
        tokens = [r for r in results if isinstance(r, str)]
        self.assertEqual(len(tokens), 1)
        self.assertTrue(all(isinstance(r, auth_store.AuthStoreError) for r in results if not isinstance(r, str)))
        connection = sqlite3.connect(self.path)
        consumed = connection.execute("SELECT consumed_at FROM auth_challenges WHERE id=?", (challenge_id,)).fetchone()[0]
        connection.close()
        self.assertIsNotNone(consumed)

    def test_username_conflict_does_not_consume_code(self) -> None:
        _challenge, code = self._sent_code("register", NEW_PHONE)
        challenge_id = self.store.prepare_register("new_user", NEW_PHONE, code, IP)
        self._insert_user("new_user", None)
        with self.assertRaises(auth_store.UsernameTaken):
            self.store.complete_register("new_user", NEW_PHONE, HASH, challenge_id, TTL)
        connection = sqlite3.connect(self.path)
        consumed = connection.execute("SELECT consumed_at FROM auth_challenges WHERE id=?", (challenge_id,)).fetchone()[0]
        connection.close()
        self.assertIsNone(consumed)
        token = self.store.complete_register("other_user", NEW_PHONE, HASH, challenge_id, TTL)
        self.assertEqual(self.store.resolve_session(token), "other_user")

    # ---------------------------------------------------------------- reset

    def test_reset_ticket_lifecycle(self) -> None:
        session_before = self.store.create_session(USERNAME, TTL)
        _challenge, code = self._sent_code("reset")
        ticket = self.store.verify_reset(PHONE, code, IP)
        self.assertEqual(self.store.peek_ticket(ticket, IP), USERNAME)
        with self.assertRaises(auth_store.ResetExpired):
            self.store.peek_ticket("not-a-ticket", IP)
        with patch.object(auth_store, "PHONE_SEND_LIMITS", ()):
            _second, second_code = self._sent_code("reset")
            second_ticket = self.store.verify_reset(PHONE, second_code, IP)
        new_hash = sha512_crypt.using(rounds=5000).hash("new-password")
        token, username = self.store.confirm_reset(ticket, new_hash, TTL)
        self.assertEqual(username, USERNAME)
        self.assertEqual(self.store.resolve_session(token), USERNAME)
        self.assertIsNone(self.store.resolve_session(session_before))
        with self.assertRaises(auth_store.ResetExpired):
            self.store.confirm_reset(ticket, new_hash, TTL)
        with self.assertRaises(auth_store.ResetExpired):
            self.store.confirm_reset(second_ticket, new_hash, TTL)
        user = self.store.get_user(USERNAME)
        assert user is not None
        self.assertEqual(user.password_hash, new_hash)
        self.assertEqual(self.store.counts()["auth_challenges"], 2)
        self.assertEqual(
            [reason for _, reason in self._invalidations()],
            ["password_changed", "password_changed"],
        )

    def test_concurrent_ticket_claims_admit_exactly_one(self) -> None:
        _challenge, code = self._sent_code("reset")
        ticket = self.store.verify_reset(PHONE, code, IP)
        new_hash = sha512_crypt.using(rounds=5000).hash("new-password")
        results = _run_concurrently(20, lambda: self.store.confirm_reset(ticket, new_hash, TTL))
        self.assertEqual(len([r for r in results if isinstance(r, tuple)]), 1)
        self.assertEqual(len([r for r in results if isinstance(r, auth_store.ResetExpired)]), 19)

    def test_expired_ticket_is_rejected(self) -> None:
        challenge_id, code = self._sent_code("reset")
        ticket = self.store.verify_reset(PHONE, code, IP)
        self._set(challenge_id, ticket_expires_at=int(time.time()) - 1)
        with self.assertRaises(auth_store.ResetExpired):
            self.store.confirm_reset(ticket, HASH, TTL)

    # -------------------------------------------------------- password login

    def test_password_login_pipeline_guards_against_concurrent_change(self) -> None:
        stored = self.store.begin_password_login(USERNAME, IP)
        self.assertEqual(stored, HASH)
        self.assertIsNone(self.store.begin_password_login("missing", IP))
        rotated = sha512_crypt.using(rounds=5000).hash("rotated")
        self.store.set_password(USERNAME, rotated)
        self.assertIsNone(self.store.finish_password_login(USERNAME, HASH, TTL))
        result = self.store.finish_password_login("OPERATOR", rotated, TTL)
        assert result is not None
        token, canonical = result
        self.assertEqual(canonical, USERNAME)
        self.assertEqual(self.store.resolve_session(token), USERNAME)

    def test_failure_throttle_clears_only_account_key(self) -> None:
        for _ in range(3):
            self.store.record_login_failure(USERNAME, IP)
        with self.assertRaises(auth_store.RateLimited):
            self.store.begin_password_login(USERNAME, "198.51.100.9")
        with self.assertRaises(auth_store.RateLimited):
            self.store.begin_password_login("someone", IP)
        result = self.store.finish_password_login(USERNAME, HASH, TTL)
        assert result is not None
        self.assertEqual(self.store.failure_count(auth_store.AuthStore.user_key(USERNAME)), 0)
        self.assertEqual(self.store.failure_count(auth_store.AuthStore.ip_key(IP)), 3)
        self.store.set_status(USERNAME, "disabled")
        with self.assertRaises(auth_store.AccountDisabled):
            self.store.begin_password_login(USERNAME, "198.51.100.10")

    def test_disable_revokes_sessions_and_challenges(self) -> None:
        token = self.store.create_session(USERNAME, TTL)
        challenge_id, code = self._sent_code("login")
        self.store.set_status(USERNAME, "disabled")
        self.assertIsNone(self.store.resolve_session(token))
        self.assertEqual(self.store.counts()["auth_sessions"], 0)
        self.assertEqual(self.store.counts()["auth_challenges"], 1)
        self.assertEqual(self._invalidations(), [(challenge_id, "user_disabled")])
        self.store.set_status(USERNAME, "active")
        self.assertIsNotNone(self.store.begin_password_login(USERNAME, IP))
        # Re-enabling never resurrects a retired code.
        self.store.throttle_max_failures = 100
        with self.assertRaises(auth_store.InvalidCode):
            self.store.login_with_code(PHONE, code, IP, TTL)
        summary = self.store.list_users()[0]
        self.assertEqual((summary.username, summary.phone, summary.status), (USERNAME, PHONE, "active"))

    def test_invalidation_retires_rows_but_keeps_the_send_ledger(self) -> None:
        self.store.throttle_max_failures = 100
        login_id, login_code = self._sent_code("login")
        with patch.object(auth_store, "PHONE_SEND_LIMITS", ()):
            reset_id, reset_code = self._sent_code("reset")
            ticket = self.store.verify_reset(PHONE, reset_code, IP)
            self._insert_user("second", "13500135000")
            other_id, other_code = self._sent_code("login", "13500135000")
        register_id, register_code = self._sent_code("register", NEW_PHONE)

        rotated = sha512_crypt.using(rounds=5000).hash("rotated")
        self.store.set_password(USERNAME, rotated)
        self.assertEqual(
            self._invalidations(),
            [(login_id, "password_changed"), (reset_id, "password_changed")],
        )
        with self.assertRaises(auth_store.InvalidCode):
            self.store.login_with_code(PHONE, login_code, IP, TTL)
        with self.assertRaises(auth_store.ResetExpired):
            self.store.peek_ticket(ticket, IP)
        with self.assertRaises(auth_store.ResetExpired):
            self.store.confirm_reset(ticket, rotated, TTL)
        # Other accounts and the unrelated registration code are untouched.
        token, username = self.store.login_with_code("13500135000", other_code, IP, TTL)
        self.assertEqual(username, "second")
        challenge = self.store.prepare_register("new_user", NEW_PHONE, register_code, IP)
        self.assertEqual(challenge, register_id)

        # A retired latest row shadows older live rows exactly like a consumed one.
        with patch.object(auth_store, "PHONE_SEND_LIMITS", ()):
            older_id, older_code = self._sent_code("login")
            newer_id, newer_code = self._sent_code("login")
        self._set(newer_id, invalidated_at=1, invalidated_reason="revoked")
        for code in (newer_code, older_code):
            with self.assertRaises(auth_store.InvalidCode):
                self.store.login_with_code(PHONE, code, IP, TTL)

        # Rebinding retires the account's rows even when they are already consumed.
        self.store.set_phone("second", NEW_PHONE)
        self.assertIn((other_id, "phone_changed"), self._invalidations())
        with patch.object(auth_store, "PHONE_SEND_LIMITS", ()):
            fresh_id, _ = self._sent_code("login")
        self.assertEqual(self.store.revoke_challenges("OPERATOR"), 2)
        invalidations = self._invalidations()
        self.assertIn((older_id, "revoked"), invalidations)
        self.assertIn((fresh_id, "revoked"), invalidations)
        self.assertEqual(self.store.revoke_challenges("nobody"), 0)
        with patch.object(auth_store, "PHONE_SEND_LIMITS", ()):
            self._sent_code("login")
        # Everything still live: the new login row and the registration row.
        self.assertEqual(self.store.revoke_challenges(), 2)
        # Every row is still there for the ledger; the sweep alone deletes.
        self.assertEqual(self.store.counts()["auth_challenges"], 8)
        self.assertEqual(len(self._invalidations()), 8)
        connection = sqlite3.connect(self.path)
        try:
            with self.assertRaises(ValueError):
                auth_store.AuthStore._invalidate_challenges(
                    connection, username=None, phone=None, reason="bogus", now=1
                )
        finally:
            connection.close()

    def test_sessions_expire_and_fingerprint_mismatch_revokes(self) -> None:
        token = self.store.create_session(USERNAME, TTL)
        connection = sqlite3.connect(self.path)
        with connection:
            connection.execute("UPDATE auth_sessions SET credential_fingerprint='stale'")
        connection.close()
        self.assertIsNone(self.store.resolve_session(token))
        self.assertEqual(self.store.counts()["auth_sessions"], 0)
        token = self.store.create_session(USERNAME, 1)
        connection = sqlite3.connect(self.path)
        with connection:
            connection.execute("UPDATE auth_sessions SET expires_at=1")
        connection.close()
        self.assertIsNone(self.store.resolve_session(token))
        self.store.revoke_session("")
        self.assertIsNone(self.store.resolve_session("x" * 300))

    def test_password_policy(self) -> None:
        self.assertEqual(auth_store.password_problem("short"), "invalid_password")
        self.assertEqual(auth_store.password_problem("x" * 65), "invalid_password")
        self.assertEqual(auth_store.password_problem("has\x00nul-byte"), "invalid_password")
        self.assertEqual(auth_store.password_problem("12345678"), "password_too_common")
        self.assertEqual(auth_store.password_problem("QWERTY123"), "password_too_common")
        self.assertEqual(auth_store.password_problem("xxOperatorxx", username="operator"), "password_too_common")
        self.assertEqual(auth_store.password_problem(f"a{PHONE}b", phone=PHONE), "password_too_common")
        self.assertIsNone(auth_store.password_problem("Long-enough-passphrase", username="operator", phone=PHONE))
        self.assertGreater(len(auth_store.common_passwords()), 9000)

    def test_htpasswd_parser_rules(self) -> None:
        good = f"alice:{HASH}\nBob.Ops@x:{HASH}\n"
        self.assertEqual([u for u, _ in auth_store.parse_htpasswd(good)], ["alice", "Bob.Ops@x"])
        for bad in (
            "",
            f"alice:{HASH}\nALICE:{HASH}\n",
            f"a:b:{HASH}\n",
            f"temporary-bypass:{HASH}\n",
            f"运营:{HASH}\n",
            "alice:$apr1$x$y\n",
            f"alice:{HASH}\n\n",
            f" alice:{HASH}\n",
            f"alice:{HASH} \n",
            "alice:$6$not-a-valid-sha512-crypt-value\n",
            f"alice:{HASH[:-1]}\n",
            f"alice:{HASH}extra\n",
        ):
            with self.subTest(bad=bad[:12]):
                with self.assertRaises(auth_store.HtpasswdImportError):
                    auth_store.parse_htpasswd(bad)

    def test_htpasswd_comments_stay_disabled_and_hash_rounds_are_bounded(self) -> None:
        text = f"#retired:{HASH}\n  #historical comment\nalice:{HASH}\n"
        self.assertEqual(auth_store.parse_htpasswd(text), [("alice", HASH)])
        with self.assertRaises(auth_store.HtpasswdImportError):
            auth_store.parse_htpasswd(f"#retired:{HASH}\n")
        salt_checksum = HASH.removeprefix("$6$rounds=5000$").removeprefix("$6$")
        for rounds in (1000, 1001, 4999, 1000001, 999999999):
            with self.subTest(rounds=rounds), self.assertRaises(auth_store.HtpasswdImportError):
                auth_store.parse_htpasswd(f"alice:$6$rounds={rounds}${salt_checksum}\n")

    def test_missing_or_empty_common_password_file_fails_closed_at_initialization(self) -> None:
        missing = Path(self.temporary.name) / "missing-passwords.txt"
        with patch.object(auth_store, "_COMMON_PASSWORDS", None), patch.object(
            auth_store, "COMMON_PASSWORDS_PATH", missing
        ), self.assertRaisesRegex(RuntimeError, "policy file is unavailable"):
            self.store.initialize()
        missing.touch()
        with patch.object(auth_store, "_COMMON_PASSWORDS", None), patch.object(
            auth_store, "COMMON_PASSWORDS_PATH", missing
        ), self.assertRaisesRegex(RuntimeError, "policy file is empty"):
            self.store.initialize()

    def test_wal_database_is_unhealthy_until_initialize_converts_it_to_delete(self) -> None:
        with sqlite3.connect(self.path) as connection:
            self.assertEqual(connection.execute("PRAGMA journal_mode=WAL").fetchone()[0], "wal")
        with self.assertRaisesRegex(RuntimeError, "requires DELETE"):
            self.store.healthcheck()
        self.assertEqual(self.store.initialize(), (3, 3))
        self.store.healthcheck()
        self.assertIsNotNone(self.store.get_user(USERNAME))
        with sqlite3.connect(self.path) as connection:
            self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone()[0], "delete")

    def test_export_refuses_empty_or_damaged_hash_without_overwriting_target(self) -> None:
        target = Path(self.temporary.name) / "rollback.htpasswd"
        original = f"operator:{HASH}\n"
        target.write_text(original, encoding="utf-8")
        self.store.set_status(USERNAME, "disabled")
        with self.assertRaises(auth_store.HtpasswdExportError):
            self.store.export_htpasswd(target)
        self.assertEqual(target.read_text(encoding="utf-8"), original)
        self.assertEqual(self.store.export_htpasswd(target, allow_empty=True), 0)
        self.assertEqual(target.read_text(encoding="utf-8"), "")
        target.write_text(original, encoding="utf-8")
        with sqlite3.connect(self.path) as connection:
            connection.execute("UPDATE auth_users SET status='active', password_hash='$6$broken'")
        with self.assertRaises(auth_store.HtpasswdExportError):
            self.store.export_htpasswd(target)
        self.assertEqual(target.read_text(encoding="utf-8"), original)

    def test_rollback_export_excludes_new_users_without_changing_passwords(self) -> None:
        self.store.create_user("boss", HASH, role="superadmin")
        self.store.create_user("pending", HASH, role="new_user")
        target = Path(self.temporary.name) / "rollback.htpasswd"
        self.assertEqual(self.store.export_htpasswd(target), 2)
        self.assertEqual(set(target.read_text().splitlines()), {f"operator:{HASH}", f"boss:{HASH}"})
        self.assertEqual(self.store.get_user("pending").password_hash, HASH)
        self.store.set_status(USERNAME, "disabled")
        self.store.set_status("boss", "disabled")
        before = target.read_bytes()
        with self.assertRaises(auth_store.HtpasswdExportError):
            self.store.export_htpasswd(target)
        self.assertEqual(target.read_bytes(), before)

    # ------------------------------------------------------- user management

    def _session_hash(self, username: str) -> str:
        return auth_store.session_token_hash(self.store.create_session(username, TTL))

    def _challenge_row(self, phone: str, purpose: str, username: str | None) -> int:
        connection = sqlite3.connect(self.path)
        try:
            with connection:
                cursor = connection.execute(
                    "INSERT INTO auth_challenges(purpose, phone, username, client_ip, status, "
                    "code_hmac, created_at, expires_at) VALUES(?,?,?,?,'sent','hmac',?,?)",
                    (purpose, phone, username, IP, int(time.time()), int(time.time()) + 300),
                )
            return int(cursor.lastrowid)
        finally:
            connection.close()

    def test_operator_provisioning_and_roles_resolve_with_the_session(self) -> None:
        self.assertEqual(self.store.get_user(USERNAME).role, "operator")
        self.store.create_user("boss", HASH, role="superadmin", actor="test")
        principal = self.store.resolve_principal(self.store.create_session("Boss", TTL))
        assert principal is not None
        self.assertEqual((principal.username, principal.role), ("boss", "superadmin"))
        self.assertEqual(self.store.count_superadmins(), 1)
        with self.assertRaises(auth_store.LastSuperadmin):
            self.store.set_role("boss", "admin", actor="test")
        with self.assertRaises(auth_store.LastSuperadmin):
            self.store.set_role("boss", "new_user", actor="test")
        self.store.set_role(USERNAME, "superadmin", actor="test")
        self.store.set_role("boss", "admin", actor="test")
        self.assertEqual(self.store.get_user("boss").role, "admin")
        self.assertEqual(self.store.count_superadmins(), 1)
        with self.assertRaises(auth_store.InvalidRole):
            self.store.set_role("boss", "root", actor="test")
        with self.assertRaises(auth_store.UserNotFound):
            self.store.set_role("ghost", "admin", actor="test")
        self.assertEqual([user.username for user in self.store.list_users()], ["boss", USERNAME])
        self.assertEqual(self.store.list_users()[0].role, "admin")

    def test_admin_can_authorize_and_demote_new_users_with_existing_sessions(self) -> None:
        self.store.create_user("lead", HASH, role="admin")
        self.store.create_user("pending", HASH, role="new_user")
        token = self.store.create_session("pending", TTL)
        new_user_session = auth_store.session_token_hash(token)
        lead_session = self._session_hash("lead")
        with self.assertRaises(auth_store.ActorForbidden):
            self.store.update_user(new_user_session, "pending", phone=None, role="admin", password_hash=None)
        with self.assertRaises(auth_store.ActorForbidden):
            self.store.delete_user(new_user_session, USERNAME)
        self.store.update_user(lead_session, "pending", phone=None, role="operator", password_hash=None)
        self.assertEqual(self.store.resolve_principal(token).role, "operator")
        self.store.update_user(lead_session, "pending", phone=None, role="new_user", password_hash=None)
        self.assertEqual(self.store.resolve_principal(token).role, "new_user")
        self.assertEqual(self.store.read_changes()[-1]["after"], {"role": "new_user"})
        self.store.delete_user(lead_session, "pending")
        self.assertIsNone(self.store.resolve_principal(token))
        self.assertTrue(self.store.username_reserved("pending"))

    def test_privilege_checks_are_evaluated_inside_the_locked_transaction(self) -> None:
        self.store.create_user("boss", HASH, role="superadmin", actor="test")
        self.store.create_user("lead", HASH, role="admin", actor="test")
        self.store.create_user("temp", HASH, role="operator", actor="test")
        lead_session = self._session_hash("lead")

        # A second connection holds the write lock and promotes the target; the
        # update only enters its transaction after the lock is released and
        # must re-evaluate the hierarchy there.
        blocker = sqlite3.connect(self.path, isolation_level=None, timeout=10)
        blocker.execute("BEGIN IMMEDIATE")
        blocker.execute("UPDATE auth_users SET role='superadmin' WHERE username='temp'")
        outcome: dict[str, object] = {}

        def attempt() -> None:
            try:
                self.store.update_user(lead_session, "temp", phone=None, role="operator", password_hash=None)
                outcome["result"] = "updated"
            except auth_store.AuthStoreError as error:
                outcome["result"] = type(error).__name__

        worker = threading.Thread(target=attempt)
        worker.start()
        time.sleep(0.2)
        self.assertNotIn("result", outcome)
        blocker.execute("COMMIT")
        worker.join(timeout=10)
        self.assertEqual(outcome["result"], "TargetForbidden")
        self.assertEqual(self.store.get_user("temp").role, "superadmin")

        # The actor is demoted while waiting → ActorForbidden; a revoked
        # session → SessionRevoked.
        blocker.execute("BEGIN IMMEDIATE")
        blocker.execute("UPDATE auth_users SET role='operator' WHERE username='lead'")
        outcome.clear()
        worker = threading.Thread(target=attempt)
        worker.start()
        time.sleep(0.2)
        blocker.execute("COMMIT")
        worker.join(timeout=10)
        self.assertEqual(outcome["result"], "ActorForbidden")
        blocker.close()
        self.store.set_role("lead", "admin", actor="test")
        self.store.revoke_sessions("lead", actor="test")
        with self.assertRaises(auth_store.SessionRevoked):
            self.store.update_user(lead_session, "temp", phone=None, role="operator", password_hash=None)

    def test_update_rules_use_canonical_names_and_revoke_on_change(self) -> None:
        self.store.create_user("Mark", HASH, role="superadmin", actor="test")
        self.store.create_user("lead", HASH, role="admin", phone=NEW_PHONE, actor="test")
        session = self._session_hash("mark")
        with self.assertRaises(auth_store.SelfRoleChange):
            self.store.update_user(session, "MARK", phone=None, role="admin", password_hash=None)
        with self.assertRaises(auth_store.SelfPasswordChange):
            self.store.update_user(session, "mark", phone=None, role="superadmin", password_hash=HASH)
        with self.assertRaises(auth_store.SelfDelete):
            self.store.delete_user(session, "mArK")
        with self.assertRaises(auth_store.LastSuperadmin):
            self.store.set_role("mark", "admin", actor="test")
        with self.assertRaises(auth_store.PhoneConflict):
            self.store.update_user(session, USERNAME, phone=NEW_PHONE, role="operator", password_hash=None)
        record = self.store.update_user(session, "mark", phone="13700000000", role="superadmin", password_hash=None)
        self.assertEqual((record.username, record.phone), ("Mark", "13700000000"))

        # Phone change: the old number leaves the admission list and its
        # registration codes retire; password change: sessions are gone.
        login_row = self._challenge_row(NEW_PHONE, "login", "lead")
        register_row = self._challenge_row(NEW_PHONE, "register", None)
        lead_session = self.store.create_session("lead", TTL)
        other_hash = sha512_crypt.using(rounds=5000).hash("rotated")
        updated = self.store.update_user(
            session, "lead", phone="13600000000", role="operator", password_hash=other_hash
        )
        self.assertEqual((updated.role, updated.phone), ("operator", "13600000000"))
        self.assertFalse(self.store.phone_allowed(NEW_PHONE))
        self.assertIsNone(self.store.resolve_session(lead_session))
        self.assertEqual(
            self._invalidations(),
            [(login_row, "phone_changed"), (register_row, "phone_changed")],
        )
        lead = self.store.get_user("lead")
        assert lead is not None
        self.assertEqual(lead.password_hash, other_hash)
        actions = [entry["action"] for entry in self.store.read_changes()]
        self.assertEqual(actions[-2:], ["user.update", "user.update"])
        self.assertEqual(self.store.read_changes()[-1]["fields"], ["phone", "role", "password"])

    def test_delete_revokes_access_and_keeps_the_ledger(self) -> None:
        self.store.create_user("boss", HASH, role="superadmin", actor="test")
        self.store.create_user("temp", HASH, role="operator", phone="13700000000", actor="test")
        self.store.allow_phone("13700000000", "temp", actor="test")
        boss_session = self._session_hash("boss")
        temp_token = self.store.create_session("temp", TTL)
        login_row = self._challenge_row("13700000000", "login", "temp")
        register_row = self._challenge_row("13700000000", "register", None)
        other_row = self._challenge_row(PHONE, "login", USERNAME)

        with self.assertRaises(auth_store.LastSuperadmin):
            self.store.delete_user_cli("boss", actor="cli:test")
        deleted = self.store.delete_user(boss_session, "TEMP")
        self.assertEqual((deleted.username, deleted.phone), ("temp", "13700000000"))
        self.assertIsNone(self.store.get_user("temp"))
        self.assertIsNone(self.store.resolve_session(temp_token))
        self.assertFalse(self.store.phone_allowed("13700000000"))
        self.assertTrue(self.store.username_reserved("Temp"))
        self.assertEqual(
            self._invalidations(),
            [(login_row, "user_deleted"), (register_row, "user_deleted")],
        )
        connection = sqlite3.connect(self.path)
        try:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM auth_challenges").fetchone()[0], 3)
            self.assertEqual(
                connection.execute(
                    "SELECT deleted_by, role, phone FROM auth_deleted_users WHERE username='temp'"
                ).fetchone(),
                ("boss", "operator", "13700000000"),
            )
        finally:
            connection.close()
        self.assertIsNone(self.store.challenge_status(other_row + 1))
        with self.assertRaises(auth_store.UsernameTaken):
            self.store.create_user("temp", HASH, actor="test")
        with self.assertRaises(auth_store.UserNotFound):
            self.store.delete_user(boss_session, "temp")
        # The retired rows still count for the send ledger of that number, and
        # the tombstone blocks a page registration with the deleted name.
        self.store.allow_phone("13700000000", "again", actor="test")
        with self.assertRaises(auth_store.RateLimited):
            self._sent_code("register", "13700000000")
        self.store.allow_phone("13500000000", "fresh", actor="test")
        challenge_id, code = self._sent_code("register", "13500000000")
        with self.assertRaises(auth_store.UsernameTaken):
            self.store.prepare_register("temp", "13500000000", code, IP)

    def test_change_log_records_security_changes_without_secrets(self) -> None:
        self.store.create_user("boss", HASH, role="superadmin", phone="13800000000", actor="cli:mark")
        self.store.set_role("boss", "superadmin", actor="cli:mark")
        self.store.set_password("boss", HASH, actor="cli:mark")
        self.store.set_status("boss", "disabled", actor="cli:mark")
        entries = self.store.read_changes()
        self.assertEqual(
            [entry["action"] for entry in entries],
            ["phone.allow", "user.create", "user.set_role", "user.set_password", "user.disable"],
        )
        raw = self.store.change_log_path.read_text(encoding="utf-8")
        self.assertNotIn(HASH, raw)
        self.assertNotIn("correct-password", raw)
        self.assertEqual(entries[1]["phone"], "13800000000")
        self.assertEqual(entries[1]["actor"], "cli:mark")
        self.assertEqual(oct(self.store.change_log_path.stat().st_mode & 0o777), "0o600")
        self.assertEqual(self.store.change_log_path, self.path.parent / "auth-changes.log")
        since = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        self.assertEqual(self.store.read_changes(since), [])

    def test_every_security_mutation_rolls_back_when_durable_intent_fails(self) -> None:
        self.store.create_user("boss", HASH, role="superadmin")
        self.store.create_user("temp", HASH, phone="13700000000")
        session = self._session_hash("boss")
        self.store.create_session("temp", TTL)
        registration, code = self._sent_code("register", NEW_PHONE)
        self.store.prepare_register("fresh", NEW_PHONE, code, IP)
        _reset, reset_code = self._sent_code("reset")
        ticket = self.store.verify_reset(PHONE, reset_code, IP)

        def database_dump():
            with sqlite3.connect(self.path) as connection:
                return tuple(connection.iterdump())

        mutations = {
            "create": lambda: self.store.create_user("new_user", HASH),
            "allow": lambda: self.store.allow_phone("13600000000"),
            "remove": lambda: self.store.remove_allowed_phone(NEW_PHONE),
            "phone": lambda: self.store.set_phone("temp", "13600000000"),
            "password": lambda: self.store.set_password("temp", HASH),
            "status": lambda: self.store.set_status("temp", "disabled"),
            "sessions": lambda: self.store.revoke_sessions(),
            "challenges": lambda: self.store.revoke_challenges(),
            "register": lambda: self.store.complete_register("fresh", NEW_PHONE, HASH, registration, TTL),
            "reset": lambda: self.store.confirm_reset(ticket, HASH, TTL),
            "update": lambda: self.store.update_user(session, "temp", phone=None, role="admin", password_hash=HASH),
            "delete_page": lambda: self.store.delete_user(session, "temp"),
            "delete_cli": lambda: self.store.delete_user_cli("temp"),
            "role": lambda: self.store.set_role("temp", "admin"),
        }
        for name, mutate in mutations.items():
            with self.subTest(action=name):
                before = database_dump()
                with patch.object(self.store, "_append_change_event", side_effect=auth_store.ChangeLogError("fsync failed")):
                    with self.assertRaises(auth_store.ChangeLogError):
                        mutate()
                self.assertEqual(database_dump(), before)

    def test_intent_is_durable_before_database_commit_and_marker_is_after(self) -> None:
        append = self.store._append_change_event
        observed = []

        def inspect_append(entry):
            observed.append((entry["event"], self.store.get_user(USERNAME).role))
            append(entry)

        with patch.object(self.store, "_append_change_event", side_effect=inspect_append):
            self.store.set_role(USERNAME, "admin")
        self.assertEqual(observed, [("intent", "operator"), ("commit", "admin")])
        latest = self.store.read_changes()[-1]
        self.assertEqual(latest["after"], {"role": "admin"})
        self.assertEqual(latest["state"], "committed")

    def test_actual_intent_fsync_failure_rolls_back_database_and_keeps_conservative_row(self) -> None:
        with patch.object(auth_store.os, "fsync", side_effect=OSError("disk full")):
            with self.assertRaises(auth_store.ChangeLogError):
                self.store.set_role(USERNAME, "admin")
        self.assertEqual(self.store.get_user(USERNAME).role, "operator")
        self.assertEqual(self.store.read_changes()[-1]["state"], "conservative")

    def test_concurrent_security_changes_keep_complete_distinct_audit_pairs(self) -> None:
        baseline = len(self.store.read_changes())
        results = _run_concurrently(20, lambda: self.store.set_role(USERNAME, "admin"))
        self.assertTrue(all(isinstance(result, auth_store.UserRecord) for result in results))
        changes = self.store.read_changes()[baseline:]
        self.assertEqual(len(changes), 20)
        self.assertEqual(len({entry["change_id"] for entry in changes}), 20)
        self.assertTrue(all(entry["state"] == "committed" for entry in changes))

    def test_audit_requires_result_values_and_rejects_unknown_or_repeated_markers(self) -> None:
        log = self.store.change_log_path
        self.store.set_role(USERNAME, "admin")
        original = [json.loads(line) for line in log.read_text().splitlines()]
        for field, value in (("after", {}), ("after", {"role": None}), ("after", {"role": []}), ("fields", [["role"]])):
            with self.subTest(field=field, value=value):
                rows = [dict(row) for row in original]
                rows[-2][field] = value
                log.write_text("".join(json.dumps(row) + "\n" for row in rows))
                with self.assertRaises(auth_store.ChangeLogError):
                    self.store.read_changes()
        for rows in (original + [original[-1]], [original[-1]], original + [original[-2]]):
            with self.subTest(events=[row["event"] for row in rows]):
                log.write_text("".join(json.dumps(row) + "\n" for row in rows))
                with self.assertRaises(auth_store.ChangeLogError):
                    self.store.read_changes()

    def test_missing_commit_marker_is_conservative_even_before_since_cutoff(self) -> None:
        append = self.store._append_change_event

        def fail_commit(entry):
            if entry["event"] == "commit":
                raise auth_store.ChangeLogError("disk full after DB commit")
            append(entry)

        with patch.object(self.store, "_append_change_event", side_effect=fail_commit), self.assertLogs(auth_store.LOGGER, level="CRITICAL"):
            self.store.set_role(USERNAME, "admin")
        self.assertEqual(self.store.get_user(USERNAME).role, "admin")
        future = datetime.now(timezone.utc) + timedelta(days=1)
        entries = self.store.read_changes(future)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["state"], "conservative")
        self.assertEqual(entries[0]["after"], {"role": "admin"})

    def test_since_uses_commit_time_so_backup_crossing_transaction_keeps_event(self) -> None:
        self.store.set_role(USERNAME, "admin")
        entries = [json.loads(line) for line in self.store.change_log_path.read_text().splitlines()]
        entries[-2]["ts"] = "2026-09-05T01:00:00.000000+00:00"
        entries[-1]["ts"] = "2026-09-05T01:00:02.000000+00:00"
        self.store.change_log_path.write_text("".join(json.dumps(entry) + "\n" for entry in entries))
        result = self.store.read_changes(datetime.fromisoformat("2026-09-05T01:00:01+00:00"))
        matching = [entry for entry in result if entry["action"] == "user.set_role"]
        self.assertEqual(len(matching), 1)
        self.assertEqual(matching[0]["ts"], entries[-1]["ts"])
        self.assertEqual(matching[0]["intent_ts"], entries[-2]["ts"])

    def test_change_log_tightens_mode_and_rejects_symlink_or_hardlink(self) -> None:
        log = self.store.change_log_path
        log.chmod(0o644)
        self.store.set_role(USERNAME, "admin")
        self.assertEqual(log.stat().st_mode & 0o777, 0o600)
        other = log.with_name("saved-audit.log")
        log.rename(other)
        for linked in ("symbolic", "hard"):
            with self.subTest(link=linked):
                if linked == "symbolic":
                    log.symlink_to(other)
                else:
                    os.link(other, log)
                before = other.read_bytes()
                with self.assertRaises(auth_store.ChangeLogError):
                    self.store.set_role(USERNAME, "operator")
                with self.assertRaises(auth_store.ChangeLogError):
                    self.store.read_changes()
                self.assertEqual(self.store.get_user(USERNAME).role, "admin")
                self.assertEqual(other.read_bytes(), before)
                log.unlink()

    def test_malformed_log_is_reported_and_stops_new_security_changes(self) -> None:
        log = self.store.change_log_path
        original = log.read_bytes()
        invalid_rows = (b"{broken}\n", b"{\"event\":\"intent\"}\n", b"\n", b"\xff\n", b"{}")
        for payload in invalid_rows:
            with self.subTest(payload=payload):
                log.write_bytes(original + payload)
                with self.assertRaises(auth_store.ChangeLogError):
                    self.store.read_changes()
                with self.assertRaises(auth_store.ChangeLogError):
                    self.store.set_role(USERNAME, "admin")
                self.assertEqual(self.store.get_user(USERNAME).role, "operator")

    def test_legacy_audit_entries_are_explicitly_conservative(self) -> None:
        legacy = {
            "ts": "2026-09-05T00:00:00+00:00", "action": "user.set_role",
            "actor": "cli:test", "target": USERNAME, "fields": ["role"],
            "phone": None, "user_version": 2,
        }
        self.store.change_log_path.write_text(json.dumps(legacy) + "\n")
        entries = self.store.read_changes(datetime(2999, 1, 1, tzinfo=timezone.utc))
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["state"], "legacy_conservative")
        self.assertEqual(entries[0]["after"], {})
        legacy["action"] = []
        self.store.change_log_path.write_text(json.dumps(legacy) + "\n")
        with self.assertRaises(auth_store.ChangeLogError):
            self.store.read_changes()

    def test_audit_rejects_old_version_with_new_role_or_mismatched_commit(self) -> None:
        self.store.set_role(USERNAME, "new_user")
        events = [json.loads(line) for line in self.store.change_log_path.read_text().splitlines()]
        for index, event in enumerate(events):
            if event.get("action") == "user.set_role":
                intent_index = index
                break
        else:
            self.fail("missing role change intent")
        for damage in ("new_role_in_schema_two", "mismatched_commit", "future_version"):
            changed = json.loads(json.dumps(events))
            if damage == "new_role_in_schema_two":
                changed[intent_index]["user_version"] = 2
            elif damage == "mismatched_commit":
                changed[intent_index + 1]["user_version"] = 2
            else:
                changed[intent_index]["user_version"] = 4
            self.store.change_log_path.write_text("".join(json.dumps(event) + "\n" for event in changed))
            with self.subTest(damage=damage), self.assertRaises(auth_store.ChangeLogError):
                self.store.read_changes()


if __name__ == "__main__":
    unittest.main()
