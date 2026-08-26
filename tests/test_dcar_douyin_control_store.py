from __future__ import annotations

import base64
import asyncio
import os
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

from cryptography.fernet import Fernet


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT / "src" / "dcar_eval"))

from dcar_douyin_control.crypto import TokenCipher  # noqa: E402
from dcar_douyin_control.store import (  # noqa: E402
    AuthorizationConflict,
    StateTransitionError,
    VaultStore,
)
from dcar_douyin_control.tokens import DouyinTokenManager  # noqa: E402


class DouyinControlStoreTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.vault_path = self.root / "vault.sqlite3"
        self.keyring_path = self.root / "fernet.keys"
        self.hmac_path = self.root / "open-id-hmac.key"
        self.keyring_path.write_text(
            f"1:{Fernet.generate_key().decode('ascii')}\n", encoding="utf-8"
        )
        self.hmac_path.write_text(
            base64.urlsafe_b64encode(os.urandom(32)).decode("ascii"),
            encoding="utf-8",
        )
        self.cipher = TokenCipher(self.keyring_path, self.hmac_path)
        self.store = VaultStore(self.vault_path)
        self.store.initialize()

    @staticmethod
    def _candidate(open_id: str = "open-id-a") -> dict[str, object]:
        return {
            "open_id": open_id,
            "access_token": "access-canary",
            "refresh_token": "refresh-canary",
            "access_expires_at": 2_000_000_000,
            "refresh_expires_at": 2_000_100_000,
            "scopes": ["user_info", "video.list"],
            "nickname": "测试账号",
            "avatar": "",
        }

    def _insert_legacy_pending(
        self,
        *,
        digest: str,
        open_id: str,
        username: str = "operator",
        account_id: int | None = None,
        platform_uid: str | None = None,
        reason: str = "matching",
        now: int = 1_900_000_000,
    ) -> tuple[dict[str, object], str, dict[str, object]]:
        candidate = self._candidate(open_id)
        fingerprint = self.cipher.open_id_fingerprint(open_id)
        authorization_id = digest[:32]
        access = self.cipher.encrypt(
            authorization_id,
            "access",
            {"open_id": open_id, "token": str(candidate["access_token"])},
        )
        refresh = self.cipher.encrypt(
            authorization_id,
            "refresh",
            {"open_id": open_id, "token": str(candidate["refresh_token"])},
        )
        with self.store.write_connection() as connection:
            connection.execute(
                """
                INSERT INTO douyin_authorizations(
                    id,open_id_fingerprint,bound_username,account_id,platform_uid,
                    access_token_ciphertext,refresh_token_ciphertext,
                    access_expires_at,refresh_expires_at,renew_count,scopes_json,
                    key_version,version,refresh_lease_owner,
                    refresh_lease_expires_at,needs_reauthorization,status,
                    match_reason,created_at,updated_at,last_authorized_at
                ) VALUES(?,?,?,?,?,?,?,?,?,0,?,1,1,NULL,NULL,0,'pending_match',?,?,?,?)
                """,
                (
                    authorization_id,
                    fingerprint,
                    username,
                    account_id,
                    platform_uid,
                    access,
                    refresh,
                    int(candidate["access_expires_at"]),
                    int(candidate["refresh_expires_at"]),
                    '["user_info","video.list"]',
                    reason,
                    now,
                    now,
                    now,
                ),
            )
        return candidate, fingerprint, {
            "id": authorization_id,
            "status": "pending_match",
            "version": 1,
        }

    def _complete_targeted(
        self,
        *,
        digest: str,
        open_id: str,
        account_id: int = 1,
        platform_uid: str = "123456789",
        username: str = "operator",
        binding: str = "a" * 64,
        candidate: dict[str, object] | None = None,
        target_authorization_id: str | None = None,
        target_authorization_version: int | None = None,
        now: int = 1_900_000_000,
    ) -> tuple[dict[str, object], str, dict[str, object]]:
        value = candidate or self._candidate(open_id)
        fingerprint = self.cipher.open_id_fingerprint(open_id)
        self.store.create_state(
            state_digest=digest,
            bound_username=username,
            session_binding=binding,
            account_id=account_id,
            platform_uid=platform_uid,
            target_authorization_id=target_authorization_id,
            target_authorization_version=target_authorization_version,
            scopes=["user_info", "video.list"],
            expires_at=now + 600,
            request_id="targeted-start",
            now=now,
        )
        self.store.begin_exchange(
            digest,
            username,
            binding,
            request_id="targeted-callback",
            now=now + 1,
        )
        completed = self.store.complete_targeted_authorization(
            state_digest=digest,
            bound_username=username,
            session_binding=binding,
            open_id_fingerprint=fingerprint,
            candidate=value,
            cipher=self.cipher,
            request_id="targeted-complete",
            now=now + 2,
        )
        return value, fingerprint, completed

    def test_initialization_enforces_delete_and_connection_pragmas(self) -> None:
        with sqlite3.connect(self.vault_path) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 3)
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            authorization_columns = {
                str(row[1]): int(row[3])
                for row in connection.execute(
                    "PRAGMA table_info(douyin_authorizations)"
                )
            }
            oauth_columns = {
                str(row[1]): int(row[3])
                for row in connection.execute("PRAGMA table_info(oauth_states)")
            }
        self.assertTrue(
            {"oauth_states", "douyin_authorizations", "audit_events"}.issubset(tables)
        )
        self.assertIn("match_reason", authorization_columns)
        self.assertEqual(authorization_columns["account_id"], 0)
        self.assertEqual(authorization_columns["platform_uid"], 0)
        self.assertEqual(oauth_columns["target_authorization_id"], 0)
        self.assertEqual(oauth_columns["target_authorization_version"], 0)
        for _ in range(2):
            with self.store.read_connection() as connection:
                self.assertEqual(
                    connection.execute("PRAGMA journal_mode").fetchone()[0], "delete"
                )
                self.assertEqual(
                    connection.execute("PRAGMA synchronous").fetchone()[0], 3
                )
                self.assertEqual(
                    connection.execute("PRAGMA foreign_keys").fetchone()[0], 1
                )
                self.assertEqual(
                    connection.execute("PRAGMA busy_timeout").fetchone()[0], 10000
                )
                self.assertEqual(
                    connection.execute("PRAGMA locking_mode").fetchone()[0], "normal"
                )
        self.assertFalse(Path(f"{self.vault_path}-wal").exists())
        self.assertFalse(Path(f"{self.vault_path}-shm").exists())

    def _downgrade_index_contract_to_v1(self) -> None:
        with sqlite3.connect(self.vault_path) as connection:
            connection.execute("DROP INDEX douyin_authorizations_active_target")
            connection.execute(
                """
                CREATE UNIQUE INDEX douyin_authorizations_active_target
                ON douyin_authorizations(account_id,platform_uid)
                WHERE status='active'
                """
            )
            connection.execute("PRAGMA user_version=1")

    def _insert_raw_authorization(
        self,
        *,
        authorization_id: str,
        fingerprint: str,
        account_id: int,
        platform_uid: str,
    ) -> None:
        with sqlite3.connect(self.vault_path) as connection:
            connection.execute(
                """
                INSERT INTO douyin_authorizations(
                    id,open_id_fingerprint,bound_username,account_id,platform_uid,
                    renew_count,scopes_json,key_version,version,
                    needs_reauthorization,status,created_at,updated_at,
                    last_authorized_at
                ) VALUES(?,?,'operator',?,?,0,'["user_info"]',1,1,0,
                         'active',1,1,1)
                """,
                (authorization_id, fingerprint, account_id, platform_uid),
            )

    def test_schema_v1_migrates_index_in_place_and_initialize_is_idempotent(
        self,
    ) -> None:
        self._downgrade_index_contract_to_v1()
        observed_versions: list[int] = []

        class ObservingVaultStore(VaultStore):
            @classmethod
            def _migrate_v2_to_v3(cls, connection: sqlite3.Connection) -> None:
                observed_versions.append(
                    int(connection.execute("PRAGMA user_version").fetchone()[0])
                )
                super()._migrate_v2_to_v3(connection)

        ObservingVaultStore(self.vault_path).initialize()
        self.store.initialize()
        self.assertEqual(observed_versions, [2])
        with sqlite3.connect(self.vault_path) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 3)
            index_columns = [
                row[2]
                for row in connection.execute(
                    "PRAGMA index_info(douyin_authorizations_active_target)"
                )
            ]
            index_sql = connection.execute(
                """
                SELECT sql FROM sqlite_master
                WHERE type='index'
                  AND name='douyin_authorizations_active_target'
                """
            ).fetchone()[0]
        self.assertEqual(index_columns, ["platform_uid"])
        self.assertIn("WHERE status='active'", index_sql)

    def test_schema_v1_duplicate_active_uid_fails_closed_before_migration(self) -> None:
        self._downgrade_index_contract_to_v1()
        self._insert_raw_authorization(
            authorization_id="1" * 32,
            fingerprint="a" * 64,
            account_id=1,
            platform_uid="123456789",
        )
        self._insert_raw_authorization(
            authorization_id="2" * 32,
            fingerprint="b" * 64,
            account_id=2,
            platform_uid="123456789",
        )
        with self.assertRaisesRegex(RuntimeError, "duplicate active platform_uid"):
            self.store.initialize()
        with sqlite3.connect(self.vault_path) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 1)
            index_columns = [
                row[2]
                for row in connection.execute(
                    "PRAGMA index_info(douyin_authorizations_active_target)"
                )
            ]
        self.assertEqual(index_columns, ["account_id", "platform_uid"])

    def test_schema_v2_to_v3_preserves_all_columns_and_token_aad(self) -> None:
        path = self.root / "vault-v2.sqlite3"
        authorization_id = "c" * 32
        open_id = "migration-open-id"
        fingerprint = self.cipher.open_id_fingerprint(open_id)
        access_ciphertext = self.cipher.encrypt(
            authorization_id,
            "access",
            {"open_id": open_id, "token": "migration-access"},
        )
        refresh_ciphertext = self.cipher.encrypt(
            authorization_id,
            "refresh",
            {"open_id": open_id, "token": "migration-refresh"},
        )
        with sqlite3.connect(path) as connection:
            connection.executescript(
                """
                CREATE TABLE oauth_states(
                    state_digest TEXT PRIMARY KEY,
                    bound_username TEXT NOT NULL,
                    session_binding TEXT NOT NULL,
                    account_id INTEGER NOT NULL,
                    platform_uid TEXT NOT NULL,
                    requested_scopes_json TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN (
                        'created','exchanging','pending_confirmation','confirmed',
                        'failed','rejected','expired'
                    )),
                    expires_at INTEGER NOT NULL,
                    candidate_ciphertext BLOB,
                    candidate_open_id_fingerprint TEXT,
                    failure_reason TEXT,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                CREATE INDEX oauth_states_current
                    ON oauth_states(bound_username,session_binding,status,updated_at DESC);
                CREATE UNIQUE INDEX oauth_states_live_session
                    ON oauth_states(bound_username,session_binding)
                    WHERE status IN ('created','exchanging','pending_confirmation');
                CREATE TABLE douyin_authorizations(
                    id TEXT PRIMARY KEY,
                    open_id_fingerprint TEXT NOT NULL UNIQUE,
                    bound_username TEXT NOT NULL,
                    account_id INTEGER NOT NULL,
                    platform_uid TEXT NOT NULL,
                    access_token_ciphertext BLOB,
                    refresh_token_ciphertext BLOB,
                    access_expires_at INTEGER,
                    refresh_expires_at INTEGER,
                    renew_count INTEGER NOT NULL DEFAULT 0 CHECK(renew_count>=0),
                    scopes_json TEXT NOT NULL,
                    key_version INTEGER NOT NULL CHECK(key_version>=1),
                    version INTEGER NOT NULL DEFAULT 1 CHECK(version>=1),
                    refresh_lease_owner TEXT,
                    refresh_lease_expires_at INTEGER,
                    needs_reauthorization INTEGER NOT NULL DEFAULT 0
                        CHECK(needs_reauthorization IN (0,1)),
                    status TEXT NOT NULL CHECK(status IN ('active','unbound')),
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    last_authorized_at INTEGER NOT NULL,
                    unbound_at INTEGER
                );
                CREATE UNIQUE INDEX douyin_authorizations_active_target
                    ON douyin_authorizations(platform_uid) WHERE status='active';
                CREATE TABLE audit_events(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    occurred_at INTEGER NOT NULL,
                    actor TEXT NOT NULL,
                    action TEXT NOT NULL,
                    result TEXT NOT NULL,
                    reason_code TEXT NOT NULL,
                    subject_fingerprint TEXT NOT NULL,
                    request_id TEXT NOT NULL
                );
                PRAGMA user_version=2;
                """
            )
            connection.execute(
                """
                INSERT INTO douyin_authorizations(
                    id,open_id_fingerprint,bound_username,account_id,platform_uid,
                    access_token_ciphertext,refresh_token_ciphertext,
                    access_expires_at,refresh_expires_at,renew_count,scopes_json,
                    key_version,version,refresh_lease_owner,
                    refresh_lease_expires_at,needs_reauthorization,status,
                    created_at,updated_at,last_authorized_at,unbound_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    authorization_id,
                    fingerprint,
                    "migration-operator",
                    7,
                    "123456789",
                    access_ciphertext,
                    refresh_ciphertext,
                    2_100_000_000,
                    2_200_000_000,
                    3,
                    '["user_info","video.list"]',
                    1,
                    4,
                    "lease-owner",
                    1_900_000_060,
                    0,
                    "active",
                    1_800_000_000,
                    1_900_000_000,
                    1_850_000_000,
                    None,
                ),
            )
            connection.executemany(
                """
                INSERT INTO oauth_states(
                    state_digest,bound_username,session_binding,account_id,
                    platform_uid,requested_scopes_json,status,expires_at,
                    created_at,updated_at
                ) VALUES(?,?,?,7,'123456789','["user_info"]',?,?,?,?)
                """,
                (
                    (
                        "e" * 64,
                        "migration-operator",
                        "a",
                        "created",
                        2_000_000_000,
                        1,
                        1,
                    ),
                    (
                        "f" * 64,
                        "migration-operator",
                        "b",
                        "exchanging",
                        2_000_000_000,
                        2,
                        2,
                    ),
                    (
                        "9" * 64,
                        "migration-operator",
                        "c",
                        "pending_confirmation",
                        2_000_000_000,
                        3,
                        3,
                    ),
                ),
            )
        old_columns = [
            "id",
            "open_id_fingerprint",
            "bound_username",
            "account_id",
            "platform_uid",
            "access_token_ciphertext",
            "refresh_token_ciphertext",
            "access_expires_at",
            "refresh_expires_at",
            "renew_count",
            "scopes_json",
            "key_version",
            "version",
            "refresh_lease_owner",
            "refresh_lease_expires_at",
            "needs_reauthorization",
            "status",
            "created_at",
            "updated_at",
            "last_authorized_at",
            "unbound_at",
        ]
        with sqlite3.connect(path) as connection:
            before = connection.execute(
                f"SELECT {','.join(old_columns)} FROM douyin_authorizations"
            ).fetchone()
        migrated = VaultStore(path)
        migrated.initialize()
        with sqlite3.connect(path) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 3)
            after = connection.execute(
                f"SELECT {','.join(old_columns)} FROM douyin_authorizations"
            ).fetchone()
            match_reason = connection.execute(
                "SELECT match_reason FROM douyin_authorizations"
            ).fetchone()[0]
            migrated_target = connection.execute(
                """
                SELECT target_authorization_id,target_authorization_version
                FROM oauth_states WHERE state_digest=?
                """,
                ("e" * 64,),
            ).fetchone()
            migrated_states = connection.execute(
                """
                SELECT state_digest,status,failure_reason
                FROM oauth_states
                WHERE state_digest IN (?,?,?)
                ORDER BY state_digest
                """,
                ("e" * 64, "f" * 64, "9" * 64),
            ).fetchall()
            migration_audits = connection.execute(
                """
                SELECT actor,action,result,reason_code,subject_fingerprint,request_id
                FROM audit_events
                WHERE action='oauth_state_migration'
                ORDER BY subject_fingerprint
                """
            ).fetchall()
        self.assertEqual(before, after)
        self.assertEqual(bytes(after[5]), access_ciphertext)
        self.assertEqual(bytes(after[6]), refresh_ciphertext)
        self.assertIsNone(match_reason)
        self.assertEqual(migrated_target, (None, None))
        self.assertEqual(
            [(row[1], row[2]) for row in migrated_states],
            [("failed", "schema_v3_migration")] * 3,
        )
        self.assertEqual(len(migration_audits), 3)
        self.assertTrue(
            all(
                tuple(row[:4])
                == (
                    "migration-operator",
                    "oauth_state_migration",
                    "failed",
                    "schema_v3_migration",
                )
                and row[5] == "schema-v3-migration"
                for row in migration_audits
            )
        )
        manager = DouyinTokenManager(
            migrated,
            self.cipher,
            object(),  # type: ignore[arg-type]
            clock=lambda: 2_000_000_000,
        )
        access = asyncio.run(
            manager.authorized_access(
                authorization_id, actor="migration-test", request_id="migration-test"
            )
        )
        self.assertEqual(access.open_id, open_id)
        self.assertEqual(access.access_token, "migration-access")

    def test_persistent_wal_is_converted_but_active_wal_fails_closed(self) -> None:
        other_path = self.root / "old-wal.sqlite3"
        connection = sqlite3.connect(other_path)
        try:
            self.assertEqual(
                connection.execute("PRAGMA journal_mode=WAL").fetchone()[0], "wal"
            )
            connection.execute("CREATE TABLE legacy(value TEXT)")
            connection.commit()
        finally:
            connection.close()
        VaultStore(other_path).initialize()
        with sqlite3.connect(other_path) as connection:
            self.assertEqual(
                connection.execute("PRAGMA journal_mode").fetchone()[0], "delete"
            )

        held_path = self.root / "held-wal.sqlite3"
        holder = sqlite3.connect(held_path, isolation_level=None)
        self.addCleanup(holder.close)
        self.assertEqual(holder.execute("PRAGMA journal_mode=WAL").fetchone()[0], "wal")
        holder.execute("CREATE TABLE legacy(value TEXT)")
        holder.execute("BEGIN IMMEDIATE")
        holder.execute("INSERT INTO legacy VALUES('held')")
        with self.assertRaises((RuntimeError, sqlite3.OperationalError)):
            VaultStore(held_path).initialize()
        tables = {
            row[0]
            for row in holder.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        self.assertNotIn("oauth_states", tables)

    def test_state_is_single_use_bound_and_terminal_states_erase_candidate(
        self,
    ) -> None:
        candidate = self._candidate()
        self.store.create_state(
            state_digest="1" * 64,
            bound_username="operator",
            session_binding="a" * 64,
            scopes=["user_info", "video.list"],
            expires_at=1_900_000_600,
            request_id="start",
            now=1_900_000_000,
        )
        self.store.begin_exchange(
            "1" * 64, "operator", "a" * 64, request_id="callback", now=1_900_000_001
        )
        with self.store.write_connection() as connection:
            connection.execute(
                """
                UPDATE oauth_states SET status='matching',candidate_ciphertext=?,
                    candidate_open_id_fingerprint=? WHERE state_digest=?
                """,
                (b"encrypted-candidate", "f" * 64, "1" * 64),
            )
        with self.assertRaises(StateTransitionError):
            self.store.begin_exchange(
                "1" * 64,
                "operator",
                "a" * 64,
                request_id="replay",
                now=1_900_000_003,
            )
        with self.assertRaises(StateTransitionError):
            self.store.begin_exchange(
                "1" * 64,
                "other",
                "a" * 64,
                request_id="wrong-user",
                now=1_900_000_003,
            )
        self.store.fail_state(
            "1" * 64,
            "operator_rejected",
            request_id="reject",
            actor="operator",
            now=1_900_000_004,
        )
        with self.store.read_connection() as connection:
            row = connection.execute(
                "SELECT status,candidate_ciphertext FROM oauth_states WHERE state_digest=?",
                ("1" * 64,),
            ).fetchone()
        self.assertEqual(row["status"], "failed")
        self.assertIsNone(row["candidate_ciphertext"])
        database_bytes = self.vault_path.read_bytes()
        self.assertNotIn(str(candidate["access_token"]).encode(), database_bytes)
        self.assertNotIn(str(candidate["refresh_token"]).encode(), database_bytes)

    def test_new_start_only_supersedes_unclaimed_state(self) -> None:
        now = 1_900_000_100
        binding = "b" * 64
        first_digest = "2" * 64
        second_digest = "3" * 64
        third_digest = "5" * 64
        self.store.create_state(
            state_digest=first_digest,
            bound_username="operator",
            session_binding=binding,
            scopes=["user_info", "video.list"],
            expires_at=now + 600,
            request_id="first-start",
            now=now,
        )
        self.store.create_state(
            state_digest=second_digest,
            bound_username="operator",
            session_binding=binding,
            scopes=["user_info", "video.list"],
            expires_at=now + 601,
            request_id="second-start",
            now=now + 1,
        )
        with self.store.read_connection() as connection:
            first = connection.execute(
                "SELECT status,failure_reason FROM oauth_states WHERE state_digest=?",
                (first_digest,),
            ).fetchone()
        self.assertEqual(tuple(first), ("expired", "superseded"))

        self.store.begin_exchange(
            second_digest,
            "operator",
            binding,
            request_id="second-callback",
            now=now + 2,
        )

        with self.assertRaisesRegex(StateTransitionError, "oauth_state_in_progress"):
            self.store.create_state(
                state_digest=third_digest,
                bound_username="operator",
                session_binding=binding,
                scopes=["user_info", "video.list"],
                expires_at=now + 602,
                request_id="third-start",
                now=now + 3,
            )

        with self.store.write_connection() as connection:
            connection.execute(
                """
                UPDATE oauth_states SET status='matching',candidate_ciphertext=?,
                    candidate_open_id_fingerprint=?,updated_at=?
                WHERE state_digest=? AND status='exchanging'
                """,
                (b"encrypted-candidate", "f" * 64, now + 4, second_digest),
            )
        with self.assertRaisesRegex(StateTransitionError, "oauth_state_in_progress"):
            self.store.create_state(
                state_digest="6" * 64,
                bound_username="operator",
                session_binding=binding,
                scopes=["user_info", "video.list"],
                expires_at=now + 604,
                request_id="fourth-start",
                now=now + 5,
            )

        with self.store.read_connection() as connection:
            second = connection.execute(
                "SELECT status,failure_reason FROM oauth_states WHERE state_digest=?",
                (second_digest,),
            ).fetchone()
            third = connection.execute(
                "SELECT status FROM oauth_states WHERE state_digest=?",
                (third_digest,),
            ).fetchone()
        self.assertEqual(tuple(second), ("matching", None))
        self.assertIsNone(third)

    def test_wrong_callback_identity_leaves_security_audit(self) -> None:
        now = 1_900_000_200
        digest = "4" * 64
        binding = "c" * 64
        self.store.create_state(
            state_digest=digest,
            bound_username="operator",
            session_binding=binding,
            scopes=["user_info", "video.list"],
            expires_at=now + 600,
            request_id="start",
            now=now,
        )

        with self.assertRaisesRegex(StateTransitionError, "oauth_state_unavailable"):
            self.store.begin_exchange(
                digest,
                "wrong-operator",
                "d" * 64,
                request_id="wrong-callback",
                now=now + 1,
            )

        with self.store.read_connection() as connection:
            state = connection.execute(
                "SELECT status FROM oauth_states WHERE state_digest=?",
                (digest,),
            ).fetchone()
            audit = connection.execute(
                """
                SELECT actor,action,result,reason_code,subject_fingerprint,request_id
                FROM audit_events WHERE request_id='wrong-callback'
                """
            ).fetchone()
        self.assertEqual(state["status"], "created")
        self.assertEqual(
            tuple(audit),
            (
                "wrong-operator",
                "oauth_callback",
                "security_rejected",
                "identity_mismatch",
                digest,
                "wrong-callback",
            ),
        )
        claimed = self.store.begin_exchange(
            digest,
            "operator",
            binding,
            request_id="correct-callback",
            now=now + 2,
        )
        self.assertEqual(claimed["status"], "exchanging")

    def test_concurrent_callback_claims_exactly_once(self) -> None:
        now = int(time.time())
        self.store.create_state(
            state_digest="2" * 64,
            bound_username="operator",
            session_binding="b" * 64,
            account_id=1,
            platform_uid="123456789",
            scopes=["user_info"],
            expires_at=now + 600,
            request_id="start",
            now=now,
        )
        barrier = threading.Barrier(3)
        outcomes: list[str] = []

        def claim() -> None:
            barrier.wait()
            try:
                self.store.begin_exchange(
                    "2" * 64,
                    "operator",
                    "b" * 64,
                    request_id="concurrent",
                    now=now + 1,
                )
            except StateTransitionError:
                outcomes.append("rejected")
            else:
                outcomes.append("claimed")

        threads = [threading.Thread(target=claim) for _ in range(2)]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join(timeout=5)
        self.assertCountEqual(outcomes, ["claimed", "rejected"])

    def test_targeted_completion_activates_and_refreshes_in_place(self) -> None:
        _candidate, fingerprint, created = self._complete_targeted(
            digest="a" * 64,
            open_id="targeted-open-id",
        )
        authorization_id = str(created["id"])
        self.assertEqual(created["status"], "active")
        self.assertEqual(created["version"], 1)

        replacement = self._candidate("targeted-open-id")
        replacement["access_token"] = "replacement-access"
        _candidate2, fingerprint2, updated = self._complete_targeted(
            digest="b" * 64,
            open_id="targeted-open-id",
            username="other-operator",
            binding="b" * 64,
            candidate=replacement,
            now=1_900_001_000,
        )
        self.assertEqual(fingerprint2, fingerprint)
        self.assertEqual(updated["id"], authorization_id)
        self.assertEqual(updated["version"], 2)
        active = self.store.get_active_authorization(authorization_id)
        assert active is not None
        access = self.cipher.decrypt(
            authorization_id, "access", bytes(active["access_token_ciphertext"])
        )
        self.assertEqual(access["token"], "replacement-access")
        self.assertEqual(active["bound_username"], "other-operator")

    def test_targeted_completion_conflicts_do_not_change_active_tokens(self) -> None:
        _candidate, _fingerprint, created = self._complete_targeted(
            digest="c" * 64,
            open_id="bound-open-id",
        )
        authorization_id = str(created["id"])

        attempts = (
            ("d" * 64, "bound-open-id", 2, "987654321", "open_id_rebind_conflict"),
            ("e" * 64, "bound-open-id", 2, "123456789", "target_changed"),
            ("f" * 64, "other-open-id", 1, "123456789", "account_binding_conflict"),
        )
        for index, (digest, open_id, account_id, platform_uid, reason) in enumerate(
            attempts
        ):
            with self.subTest(reason=reason):
                candidate = self._candidate(open_id)
                self.store.create_state(
                    state_digest=digest,
                    bound_username=f"operator-{index}",
                    session_binding=str(index + 2) * 64,
                    account_id=account_id,
                    platform_uid=platform_uid,
                    scopes=["user_info", "video.list"],
                    expires_at=1_900_002_600 + index,
                    request_id=f"conflict-start-{index}",
                    now=1_900_002_000 + index,
                )
                self.store.begin_exchange(
                    digest,
                    f"operator-{index}",
                    str(index + 2) * 64,
                    request_id=f"conflict-callback-{index}",
                    now=1_900_002_010 + index,
                )
                with self.assertRaisesRegex(AuthorizationConflict, reason):
                    self.store.complete_targeted_authorization(
                        state_digest=digest,
                        bound_username=f"operator-{index}",
                        session_binding=str(index + 2) * 64,
                        open_id_fingerprint=self.cipher.open_id_fingerprint(open_id),
                        candidate=candidate,
                        cipher=self.cipher,
                        request_id=f"conflict-complete-{index}",
                        now=1_900_002_020 + index,
                    )

        active = self.store.get_active_authorization(authorization_id)
        assert active is not None
        self.assertEqual(active["version"], 1)
        access = self.cipher.decrypt(
            authorization_id, "access", bytes(active["access_token_ciphertext"])
        )
        self.assertEqual(access["token"], "access-canary")
        with self.store.read_connection() as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM douyin_authorizations"
                ).fetchone()[0],
                1,
            )
            failures = connection.execute(
                "SELECT failure_reason FROM oauth_states WHERE status='failed'"
            ).fetchall()
        self.assertCountEqual(
            [str(row[0]) for row in failures],
            [reason for *_rest, reason in attempts],
        )

    def test_targeted_reauthorization_is_versioned_and_identity_bound(self) -> None:
        _candidate, fingerprint, created = self._complete_targeted(
            digest="1" * 64,
            open_id="reauthorization-open-id",
        )
        authorization_id = str(created["id"])
        wrong_candidate = self._candidate("wrong-open-id")
        self.store.create_state(
            state_digest="2" * 64,
            bound_username="operator",
            session_binding="2" * 64,
            account_id=1,
            platform_uid="123456789",
            target_authorization_id=authorization_id,
            target_authorization_version=1,
            scopes=["user_info", "video.list"],
            expires_at=1_900_003_600,
            request_id="reauthorization-start",
            now=1_900_003_000,
        )
        self.store.begin_exchange(
            "2" * 64,
            "operator",
            "2" * 64,
            request_id="reauthorization-callback",
            now=1_900_003_001,
        )
        with self.assertRaisesRegex(
            AuthorizationConflict, "reauthorization_target_mismatch"
        ):
            self.store.complete_targeted_authorization(
                state_digest="2" * 64,
                bound_username="operator",
                session_binding="2" * 64,
                open_id_fingerprint=self.cipher.open_id_fingerprint("wrong-open-id"),
                candidate=wrong_candidate,
                cipher=self.cipher,
                request_id="reauthorization-complete",
                now=1_900_003_002,
            )
        active = self.store.get_active_authorization(authorization_id)
        assert active is not None
        self.assertEqual(active["version"], 1)
        self.assertEqual(active["open_id_fingerprint"], fingerprint)

        refreshed = self._candidate("reauthorization-open-id")
        refreshed["access_token"] = "reauthorized-access"
        _candidate2, _fingerprint2, updated = self._complete_targeted(
            digest="5" * 64,
            open_id="reauthorization-open-id",
            binding="5" * 64,
            candidate=refreshed,
            target_authorization_id=authorization_id,
            target_authorization_version=1,
            now=1_900_003_100,
        )
        self.assertEqual(updated["version"], 2)

        stale = self._candidate("reauthorization-open-id")
        stale["access_token"] = "stale-access"
        self.store.create_state(
            state_digest="6" * 64,
            bound_username="operator",
            session_binding="6" * 64,
            account_id=1,
            platform_uid="123456789",
            target_authorization_id=authorization_id,
            target_authorization_version=1,
            scopes=["user_info", "video.list"],
            expires_at=1_900_004_000,
            request_id="stale-start",
            now=1_900_003_200,
        )
        self.store.begin_exchange(
            "6" * 64,
            "operator",
            "6" * 64,
            request_id="stale-callback",
            now=1_900_003_201,
        )
        with self.assertRaisesRegex(
            AuthorizationConflict, "reauthorization_version_conflict"
        ):
            self.store.complete_targeted_authorization(
                state_digest="6" * 64,
                bound_username="operator",
                session_binding="6" * 64,
                open_id_fingerprint=fingerprint,
                candidate=stale,
                cipher=self.cipher,
                request_id="stale-complete",
                now=1_900_003_202,
            )
        current = self.store.get_active_authorization(authorization_id)
        assert current is not None
        self.assertEqual(current["version"], 2)
        access = self.cipher.decrypt(
            authorization_id, "access", bytes(current["access_token_ciphertext"])
        )
        self.assertEqual(access["token"], "reauthorized-access")

    def test_targeted_scope_failure_and_pending_invalidation_fail_closed(self) -> None:
        candidate = self._candidate("scope-open-id")
        candidate["scopes"] = ["user_info"]
        self.store.create_state(
            state_digest="3" * 64,
            bound_username="operator",
            session_binding="3" * 64,
            account_id=3,
            platform_uid="323456789",
            scopes=["user_info", "video.list"],
            expires_at=1_900_004_600,
            request_id="scope-start",
            now=1_900_004_000,
        )
        self.store.begin_exchange(
            "3" * 64,
            "operator",
            "3" * 64,
            request_id="scope-callback",
            now=1_900_004_001,
        )
        with self.assertRaisesRegex(AuthorizationConflict, "oauth_scope_incomplete"):
            self.store.complete_targeted_authorization(
                state_digest="3" * 64,
                bound_username="operator",
                session_binding="3" * 64,
                open_id_fingerprint=self.cipher.open_id_fingerprint("scope-open-id"),
                candidate=candidate,
                cipher=self.cipher,
                request_id="scope-complete",
                now=1_900_004_002,
            )
        with self.store.read_connection() as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM douyin_authorizations"
                ).fetchone()[0],
                0,
            )

        _old_candidate, _old_fingerprint, pending = self._insert_legacy_pending(
            digest="4" * 64,
            open_id="legacy-pending-open-id",
            now=1_900_005_000,
        )
        pending_id = str(pending["id"])
        self.assertTrue(
            self.store.unbind(
                actor="operator-admin",
                authorization_id=pending_id,
                expected_version=1,
                request_id="invalidate-pending",
                now=1_900_005_003,
            )
        )
        invalidated = self.store.get_authorization(pending_id)
        assert invalidated is not None
        self.assertEqual(invalidated["status"], "unbound")
        self.assertEqual(invalidated["version"], 2)
        self.assertTrue(invalidated["needs_reauthorization"])
        with self.store.read_connection() as connection:
            row = connection.execute(
                """
                SELECT access_token_ciphertext,refresh_token_ciphertext,
                       refresh_lease_owner,refresh_lease_expires_at
                FROM douyin_authorizations WHERE id=?
                """,
                (pending_id,),
            ).fetchone()
            audit = connection.execute(
                """
                SELECT reason_code FROM audit_events
                WHERE request_id='invalidate-pending'
                """
            ).fetchone()
        self.assertEqual(tuple(row), (None, None, None, None))
        self.assertEqual(audit["reason_code"], "operator_invalidated_pending_match")

    def test_targeted_unbind_requires_current_version_and_erases_tokens(self) -> None:
        _candidate, _fingerprint, created = self._complete_targeted(
            digest="7" * 64, open_id="unbind-open-id"
        )
        authorization_id = str(created["id"])
        with self.assertRaisesRegex(
            AuthorizationConflict, "authorization_version_conflict"
        ):
            self.store.unbind(
                actor="admin",
                authorization_id=authorization_id,
                expected_version=2,
                request_id="stale-unbind",
            )
        self.assertTrue(
            self.store.unbind(
                actor="admin",
                authorization_id=authorization_id,
                expected_version=1,
                request_id="current-unbind",
            )
        )
        with self.store.read_connection() as connection:
            row = connection.execute(
                """
                SELECT status,version,access_token_ciphertext,
                       refresh_token_ciphertext,refresh_lease_owner
                FROM douyin_authorizations WHERE id=?
                """,
                (authorization_id,),
            ).fetchone()
        self.assertEqual(tuple(row), ("unbound", 2, None, None, None))

    def test_targeted_completion_requires_account_locked_in_state(self) -> None:
        digest = "8" * 64
        self.store.create_state(
            state_digest=digest,
            bound_username="operator",
            session_binding="8" * 64,
            scopes=["user_info", "video.list"],
            expires_at=1_900_000_600,
            request_id="missing-target-start",
            now=1_900_000_000,
        )
        self.store.begin_exchange(
            digest,
            "operator",
            "8" * 64,
            request_id="missing-target-callback",
            now=1_900_000_001,
        )
        candidate = self._candidate("missing-target-open-id")
        with self.assertRaisesRegex(StateTransitionError, "oauth_target_missing"):
            self.store.complete_targeted_authorization(
                state_digest=digest,
                bound_username="operator",
                session_binding="8" * 64,
                open_id_fingerprint=self.cipher.open_id_fingerprint(
                    "missing-target-open-id"
                ),
                candidate=candidate,
                cipher=self.cipher,
                request_id="missing-target-complete",
                now=1_900_000_002,
            )
        self.assertEqual(self.store.list_authorizations(), [])

    def test_legacy_pending_blocks_targeted_binding_until_invalidated(self) -> None:
        _candidate, _fingerprint, pending = self._insert_legacy_pending(
            digest="9" * 64,
            open_id="legacy-owner-open-id",
            account_id=9,
            platform_uid="999999999",
        )
        with self.assertRaisesRegex(AuthorizationConflict, "account_binding_conflict"):
            self._complete_targeted(
                digest="a" * 64,
                open_id="new-open-id",
                account_id=9,
                platform_uid="999999999",
                binding="a" * 64,
                now=1_900_001_000,
            )
        self.assertTrue(
            self.store.unbind(
                actor="admin",
                authorization_id=str(pending["id"]),
                expected_version=1,
                request_id="invalidate-legacy",
            )
        )
        _value, _fingerprint2, active = self._complete_targeted(
            digest="b" * 64,
            open_id="new-open-id",
            account_id=9,
            platform_uid="999999999",
            binding="b" * 64,
            now=1_900_002_000,
        )
        self.assertEqual(active["status"], "active")

    def test_legacy_pending_invalidation_is_audited_and_fail_closed(self) -> None:
        _candidate, _fingerprint, pending = self._insert_legacy_pending(
            digest="c" * 64, open_id="legacy-audit-open-id"
        )
        self.store.unbind(
            actor="replacement-operator",
            authorization_id=str(pending["id"]),
            expected_version=1,
            request_id="legacy-audit-unbind",
        )
        with self.store.read_connection() as connection:
            audit = connection.execute(
                """
                SELECT actor,result,reason_code FROM audit_events
                WHERE request_id='legacy-audit-unbind'
                """
            ).fetchone()
        self.assertEqual(
            tuple(audit),
            ("replacement-operator", "unbound", "operator_invalidated_pending_match"),
        )

    def test_targeted_status_projection_tracks_active_and_unbound(self) -> None:
        _candidate, _fingerprint, created = self._complete_targeted(
            digest="d" * 64, open_id="status-open-id"
        )
        before = self.store.authorization_statuses(now=1_900_000_100)
        self.assertEqual(len(before), 1)
        self.assertTrue(before[0]["authorized"])
        self.store.unbind(
            actor="admin",
            authorization_id=str(created["id"]),
            expected_version=1,
            request_id="status-unbind",
            now=1_900_000_200,
        )
        after = self.store.authorization_statuses(now=1_900_000_201)
        self.assertEqual(len(after), 1)
        self.assertFalse(after[0]["authorized"])
        self.assertEqual(after[0]["status"], "unbound")

    def test_targeted_reauthorization_stale_callback_preserves_new_token(self) -> None:
        _candidate, _fingerprint, created = self._complete_targeted(
            digest="e" * 64, open_id="race-open-id"
        )
        authorization_id = str(created["id"])
        fresh = self._candidate("race-open-id")
        fresh["access_token"] = "fresh-access"
        self._complete_targeted(
            digest="f" * 64,
            open_id="race-open-id",
            candidate=fresh,
            binding="f" * 64,
            target_authorization_id=authorization_id,
            target_authorization_version=1,
            now=1_900_001_000,
        )
        stale = self._candidate("race-open-id")
        stale["access_token"] = "stale-access"
        with self.assertRaisesRegex(
            AuthorizationConflict, "reauthorization_version_conflict"
        ):
            self._complete_targeted(
                digest="0" * 64,
                open_id="race-open-id",
                candidate=stale,
                binding="0" * 64,
                target_authorization_id=authorization_id,
                target_authorization_version=1,
                now=1_900_002_000,
            )
        active = self.store.get_active_authorization(authorization_id)
        assert active is not None
        payload = self.cipher.decrypt(
            authorization_id, "access", bytes(active["access_token_ciphertext"])
        )
        self.assertEqual(payload["token"], "fresh-access")

    def test_targeted_reauthorization_scope_failure_preserves_active_token(self) -> None:
        _candidate, _fingerprint, created = self._complete_targeted(
            digest="1" * 64, open_id="scope-preserve-open-id"
        )
        authorization_id = str(created["id"])
        incomplete = self._candidate("scope-preserve-open-id")
        incomplete["scopes"] = ["user_info"]
        with self.assertRaisesRegex(AuthorizationConflict, "oauth_scope_incomplete"):
            self._complete_targeted(
                digest="2" * 64,
                open_id="scope-preserve-open-id",
                candidate=incomplete,
                binding="2" * 64,
                target_authorization_id=authorization_id,
                target_authorization_version=1,
                now=1_900_001_000,
            )
        active = self.store.get_active_authorization(authorization_id)
        assert active is not None
        self.assertEqual(active["version"], 1)
        payload = self.cipher.decrypt(
            authorization_id, "access", bytes(active["access_token_ciphertext"])
        )
        self.assertEqual(payload["token"], "access-canary")

    def test_any_authenticated_operator_can_manage_targeted_authorization(self) -> None:
        _candidate, _fingerprint, created = self._complete_targeted(
            digest="3" * 64, open_id="operator-handoff-open-id", username="creator"
        )
        self.assertTrue(
            self.store.unbind(
                actor="different-operator",
                authorization_id=str(created["id"]),
                expected_version=1,
                request_id="operator-handoff",
            )
        )
        item = self.store.get_authorization(str(created["id"]))
        assert item is not None
        self.assertEqual(item["status"], "unbound")

    def test_targeted_public_projection_never_exposes_identity_or_ciphertext(self) -> None:
        self._complete_targeted(
            digest="4" * 64, open_id="projection-open-id"
        )
        item = self.store.list_authorizations()[0]
        self.assertNotIn("open_id_fingerprint", item)
        self.assertNotIn("access_token_ciphertext", item)
        self.assertNotIn("refresh_token_ciphertext", item)
        self.assertNotIn("key_version", item)

    def test_machine_authorization_projection_excludes_identity_and_tokens(
        self,
    ) -> None:
        _candidate, _fingerprint, created = self._complete_targeted(
            digest="8" * 64,
            open_id="machine-list-open-id",
        )
        items = self.store.list_active_authorizations()
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["id"], created["id"])
        self.assertEqual(
            set(items[0]),
            {
                "id",
                "account_id",
                "platform_uid",
                "access_expires_at",
                "refresh_expires_at",
                "renew_count",
                "scopes",
                "needs_reauthorization",
                "updated_at",
            },
        )

    def test_token_lifecycle_lease_updates_limit_and_reauthorization_audit(
        self,
    ) -> None:
        _candidate, _fingerprint, created = self._complete_targeted(
            digest="9" * 64,
            open_id="token-lifecycle-open-id",
        )
        authorization_id = str(created["id"])
        self.assertRegex(authorization_id, r"\A[0-9a-f]{32}\Z")
        active = self.store.get_active_authorization(authorization_id)
        self.assertIsNotNone(active)
        assert active is not None
        self.assertEqual(active["scopes"], ["user_info", "video.list"])
        self.assertFalse(active["needs_reauthorization"])
        with self.assertRaises(ValueError):
            self.store.get_active_authorization("A" * 32)

        now = 1_900_000_100
        self.assertTrue(
            self.store.acquire_refresh_lease(
                authorization_id,
                lease_owner="worker-a",
                lease_seconds=60,
                actor="douyin-sync",
                request_id="lease-a",
                now=now,
            )
        )
        self.assertFalse(
            self.store.acquire_refresh_lease(
                authorization_id,
                lease_owner="worker-b",
                lease_seconds=60,
                actor="douyin-sync",
                request_id="lease-busy",
                now=now,
            )
        )
        refreshed_access = self.cipher.encrypt(
            authorization_id,
            "access",
            {"open_id": "open-id-a", "token": "access-refreshed"},
        )
        self.assertFalse(
            self.store.update_access_token(
                authorization_id,
                lease_owner="worker-b",
                access_token_ciphertext=refreshed_access,
                access_expires_at=now + 600,
                key_version=1,
                actor="douyin-sync",
                request_id="wrong-lease-update",
                now=now + 1,
            )
        )
        self.assertTrue(
            self.store.update_access_token(
                authorization_id,
                lease_owner="worker-a",
                access_token_ciphertext=refreshed_access,
                access_expires_at=now + 600,
                key_version=1,
                actor="douyin-sync",
                request_id="access-update",
                now=now + 1,
            )
        )
        self.assertTrue(
            self.store.release_refresh_lease(
                authorization_id,
                lease_owner="worker-a",
                actor="douyin-sync",
                request_id="release-a",
                now=now + 2,
            )
        )
        self.assertTrue(
            self.store.acquire_refresh_lease(
                authorization_id,
                lease_owner="worker-a",
                lease_seconds=60,
                actor="douyin-sync",
                request_id="lease-a-again",
                now=now + 3,
            )
        )
        bundled_access = self.cipher.encrypt(
            authorization_id,
            "access",
            {"open_id": "open-id-a", "token": "access-bundled"},
        )
        bundled_refresh = self.cipher.encrypt(
            authorization_id,
            "refresh",
            {"open_id": "open-id-a", "token": "refresh-bundled"},
        )
        bundle_access_expiry = now + 700
        bundle_refresh_expiry = now + 20_000
        self.assertTrue(
            self.store.update_refreshed_token_bundle(
                authorization_id,
                lease_owner="worker-a",
                access_token_ciphertext=bundled_access,
                refresh_token_ciphertext=bundled_refresh,
                access_expires_at=bundle_access_expiry,
                refresh_expires_at=bundle_refresh_expiry,
                key_version=1,
                actor="douyin-sync",
                request_id="bundle-update",
                now=now + 4,
            )
        )
        active = self.store.get_active_authorization(authorization_id)
        assert active is not None
        self.assertEqual(active["renew_count"], 0)
        self.assertEqual(active["access_expires_at"], bundle_access_expiry)
        self.assertEqual(active["refresh_expires_at"], bundle_refresh_expiry)
        self.assertEqual(
            self.cipher.decrypt(
                authorization_id,
                "access",
                bytes(active["access_token_ciphertext"]),
            )["token"],
            "access-bundled",
        )
        self.assertEqual(
            self.cipher.decrypt(
                authorization_id,
                "refresh",
                bytes(active["refresh_token_ciphertext"]),
            )["token"],
            "refresh-bundled",
        )
        for renew_number in range(1, 6):
            refreshed_refresh = self.cipher.encrypt(
                authorization_id,
                "refresh",
                {
                    "open_id": "open-id-a",
                    "token": f"refresh-{renew_number}",
                },
            )
            self.assertTrue(
                self.store.renew_refresh_token(
                    authorization_id,
                    lease_owner="worker-a",
                    refresh_token_ciphertext=refreshed_refresh,
                    refresh_expires_at=now + 20_000 + renew_number,
                    key_version=1,
                    actor="douyin-sync",
                    request_id=f"renew-{renew_number}",
                    now=now + renew_number,
                )
            )
        self.assertFalse(
            self.store.renew_refresh_token(
                authorization_id,
                lease_owner="worker-a",
                refresh_token_ciphertext=b"encrypted-but-unused",
                refresh_expires_at=now + 20_000,
                key_version=1,
                actor="douyin-sync",
                request_id="renew-limit",
                now=now + 6,
            )
        )
        active = self.store.get_active_authorization(authorization_id)
        assert active is not None
        self.assertEqual(active["renew_count"], 5)
        access_payload = self.cipher.decrypt(
            authorization_id,
            "access",
            bytes(active["access_token_ciphertext"]),
        )
        self.assertEqual(access_payload["token"], "access-bundled")

        new_keyring = self.root / "fernet-v2.keys"
        new_keyring.write_text(
            "\n".join(
                [
                    f"2:{Fernet.generate_key().decode('ascii')}",
                    self.keyring_path.read_text(encoding="utf-8").strip(),
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        rotating_cipher = TokenCipher(new_keyring, self.hmac_path)
        active_before_rotation = self.store.get_active_authorization(authorization_id)
        assert active_before_rotation is not None
        rotated_access = rotating_cipher.encrypt(
            authorization_id,
            "access",
            rotating_cipher.decrypt(
                authorization_id,
                "access",
                bytes(active_before_rotation["access_token_ciphertext"]),
            ),
        )
        rotated_refresh = rotating_cipher.encrypt(
            authorization_id,
            "refresh",
            rotating_cipher.decrypt(
                authorization_id,
                "refresh",
                bytes(active_before_rotation["refresh_token_ciphertext"]),
            ),
        )
        self.assertTrue(
            self.store.rotate_authorization_tokens(
                authorization_id,
                lease_owner="worker-a",
                access_token_ciphertext=rotated_access,
                refresh_token_ciphertext=rotated_refresh,
                key_version=2,
                actor="douyin-sync",
                request_id="lazy-rotate",
                now=now + 7,
            )
        )
        active_after_rotation = self.store.get_active_authorization(authorization_id)
        assert active_after_rotation is not None
        self.assertEqual(
            active_after_rotation["access_expires_at"], bundle_access_expiry
        )
        self.assertGreaterEqual(
            int(active_after_rotation["refresh_expires_at"]), bundle_refresh_expiry
        )
        self.assertEqual(active_after_rotation["renew_count"], 5)
        self.assertEqual(active_after_rotation["key_version"], 2)
        self.assertEqual(
            rotating_cipher.decrypt(
                authorization_id,
                "access",
                bytes(active_after_rotation["access_token_ciphertext"]),
            )["token"],
            "access-bundled",
        )

        self.assertFalse(
            self.store.release_refresh_lease(
                authorization_id,
                lease_owner="worker-b",
                actor="douyin-sync",
                request_id="wrong-release",
                now=now + 7,
            )
        )
        self.assertTrue(
            self.store.mark_needs_reauthorization(
                authorization_id,
                actor="douyin-sync",
                reason_code="renew_limit_reached",
                request_id="reauthorization-required",
                now=now + 8,
            )
        )
        self.assertFalse(
            self.store.acquire_refresh_lease(
                authorization_id,
                lease_owner="worker-b",
                lease_seconds=60,
                actor="douyin-sync",
                request_id="lease-after-mark",
                now=now + 9,
            )
        )
        active = self.store.get_active_authorization(authorization_id)
        assert active is not None
        self.assertTrue(active["needs_reauthorization"])
        self.assertIsNone(active["refresh_lease_owner"])
        with self.store.read_connection() as connection:
            actions = {
                str(row[0])
                for row in connection.execute(
                    """
                    SELECT action FROM audit_events
                    WHERE request_id IN (
                        'lease-a','lease-busy','wrong-lease-update','access-update',
                        'release-a','lease-a-again','bundle-update','renew-1',
                        'renew-limit','lazy-rotate','wrong-release',
                        'reauthorization-required','lease-after-mark'
                    )
                    """
                )
            }
        self.assertEqual(
            actions,
            {
                "token_refresh_lease_acquire",
                "access_token_update",
                "token_bundle_refresh",
                "refresh_token_renew",
                "token_refresh_lease_release",
                "token_ciphertext_rotate",
                "authorization_reauthorization_required",
            },
        )

    def test_refresh_lease_concurrent_acquire_has_one_owner(self) -> None:
        _candidate, _fingerprint, created = self._complete_targeted(
            digest="a" * 64,
            open_id="concurrent-lease-open-id",
        )
        authorization_id = str(created["id"])
        barrier = threading.Barrier(3)
        outcomes: list[tuple[str, bool]] = []

        def acquire(owner: str) -> None:
            barrier.wait()
            outcomes.append(
                (
                    owner,
                    self.store.acquire_refresh_lease(
                        authorization_id,
                        lease_owner=owner,
                        lease_seconds=60,
                        actor="douyin-sync",
                        request_id=f"concurrent-{owner}",
                        now=1_900_000_100,
                    ),
                )
            )

        threads = [
            threading.Thread(target=acquire, args=("worker-a",)),
            threading.Thread(target=acquire, args=("worker-b",)),
        ]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join(timeout=5)
        self.assertEqual(sorted(result for _owner, result in outcomes), [False, True])
        active = self.store.get_active_authorization(authorization_id)
        assert active is not None
        winner = next(owner for owner, result in outcomes if result)
        self.assertEqual(active["refresh_lease_owner"], winner)

    def test_cipher_record_kind_and_rotation_contract(self) -> None:
        old_key = Fernet.generate_key().decode("ascii")
        new_key = Fernet.generate_key().decode("ascii")
        old_path = self.root / "old.keys"
        old_path.write_text(f"1:{old_key}\n", encoding="utf-8")
        old_cipher = TokenCipher(old_path, self.hmac_path)
        ciphertext = old_cipher.encrypt("record-a", "access", {"token": "secret"})

        rotating_path = self.root / "rotating.keys"
        rotating_path.write_text(f"2:{new_key}\n1:{old_key}\n", encoding="utf-8")
        rotating = TokenCipher(rotating_path, self.hmac_path)
        self.assertEqual(
            rotating.decrypt("record-a", "access", ciphertext)["token"], "secret"
        )
        rotated = rotating.rotate(ciphertext)
        self.assertEqual(
            rotating.decrypt("record-a", "access", rotated)["token"], "secret"
        )
        with self.assertRaises(RuntimeError):
            rotating.decrypt("record-b", "access", rotated)
        with self.assertRaises(RuntimeError):
            rotating.decrypt("record-a", "refresh", rotated)


if __name__ == "__main__":
    unittest.main()
