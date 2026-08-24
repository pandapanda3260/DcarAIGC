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

    def _pending(
        self,
        *,
        digest: str,
        username: str = "operator",
        binding: str = "a" * 64,
        account_id: int = 1,
        platform_uid: str = "123456789",
        candidate: dict[str, object] | None = None,
        now: int = 1_900_000_000,
    ) -> tuple[dict[str, object], str]:
        value = candidate or self._candidate()
        fingerprint = self.cipher.open_id_fingerprint(str(value["open_id"]))
        self.store.create_state(
            state_digest=digest,
            bound_username=username,
            session_binding=binding,
            account_id=account_id,
            platform_uid=platform_uid,
            scopes=["user_info", "video.list"],
            expires_at=now + 600,
            request_id="request-start",
            now=now,
        )
        self.store.begin_exchange(
            digest,
            username,
            binding,
            request_id="request-callback",
            now=now + 1,
        )
        self.store.store_candidate(
            state_digest=digest,
            ciphertext=self.cipher.encrypt(digest, "oauth_candidate", value),
            open_id_fingerprint=fingerprint,
            confirmation_expires_at=now + 900,
            request_id="request-callback",
            now=now + 2,
        )
        return value, fingerprint

    def _stage(
        self,
        *,
        digest: str,
        open_id: str,
        username: str = "operator",
        binding: str = "a" * 64,
        target_authorization_id: str | None = None,
        target_authorization_version: int | None = None,
        now: int = 1_900_000_000,
    ) -> tuple[dict[str, object], str, dict[str, object]]:
        candidate = self._candidate(open_id)
        fingerprint = self.cipher.open_id_fingerprint(open_id)
        self.store.create_state(
            state_digest=digest,
            bound_username=username,
            session_binding=binding,
            target_authorization_id=target_authorization_id,
            target_authorization_version=target_authorization_version,
            scopes=["user_info", "video.list"],
            expires_at=now + 600,
            request_id="automatic-start",
            now=now,
        )
        self.store.begin_exchange(
            digest,
            username,
            binding,
            request_id="automatic-callback",
            now=now + 1,
        )
        staged = self.store.stage_authorization_candidate(
            state_digest=digest,
            bound_username=username,
            session_binding=binding,
            open_id_fingerprint=fingerprint,
            candidate=candidate,
            cipher=self.cipher,
            request_id="automatic-stage",
            now=now + 2,
        )
        return candidate, fingerprint, staged

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
            {"oauth_states", "douyin_authorizations", "audit_events"}.issubset(
                tables
            )
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
                self.assertEqual(connection.execute("PRAGMA synchronous").fetchone()[0], 3)
                self.assertEqual(connection.execute("PRAGMA foreign_keys").fetchone()[0], 1)
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
            connection.execute(
                "DROP INDEX douyin_authorizations_active_target"
            )
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

    def test_schema_v1_migrates_index_in_place_and_initialize_is_idempotent(self) -> None:
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
            self.assertEqual(connection.execute("PRAGMA journal_mode=WAL").fetchone()[0], "wal")
            connection.execute("CREATE TABLE legacy(value TEXT)")
            connection.commit()
        finally:
            connection.close()
        VaultStore(other_path).initialize()
        with sqlite3.connect(other_path) as connection:
            self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone()[0], "delete")

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
            for row in holder.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        self.assertNotIn("oauth_states", tables)

    def test_state_is_single_use_bound_and_terminal_states_erase_candidate(self) -> None:
        candidate, _fingerprint = self._pending(digest="1" * 64)
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
        self.assertTrue(
            self.store.reject_current(
                "operator",
                "a" * 64,
                request_id="reject",
                now=1_900_000_004,
            )
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

    def test_confirmation_reauthorization_conflicts_and_versioned_unbind(self) -> None:
        candidate, fingerprint = self._pending(digest="3" * 64)
        created = self.store.confirm_authorization(
            state_digest="3" * 64,
            bound_username="operator",
            session_binding="a" * 64,
            open_id_fingerprint=fingerprint,
            candidate=candidate,
            cipher=self.cipher,
            request_id="confirm-1",
            now=1_900_000_010,
        )
        self.assertEqual(created["version"], 1)
        authorization_id = str(created["id"])
        with self.store.read_connection() as connection:
            row = connection.execute(
                "SELECT * FROM douyin_authorizations WHERE id=?", (authorization_id,)
            ).fetchone()
        access = self.cipher.decrypt(
            authorization_id, "access", bytes(row["access_token_ciphertext"])
        )
        refresh = self.cipher.decrypt(
            authorization_id, "refresh", bytes(row["refresh_token_ciphertext"])
        )
        self.assertEqual(access["token"], "access-canary")
        self.assertEqual(refresh["token"], "refresh-canary")
        with self.assertRaises(RuntimeError):
            self.cipher.decrypt(
                authorization_id, "refresh", bytes(row["access_token_ciphertext"])
            )

        candidate2, fingerprint2 = self._pending(
            digest="4" * 64,
            candidate=self._candidate("open-id-a"),
            now=1_900_001_000,
        )
        updated = self.store.confirm_authorization(
            state_digest="4" * 64,
            bound_username="operator",
            session_binding="a" * 64,
            open_id_fingerprint=fingerprint2,
            candidate=candidate2,
            cipher=self.cipher,
            request_id="confirm-2",
            now=1_900_001_010,
        )
        self.assertEqual(updated["id"], authorization_id)
        self.assertEqual(updated["version"], 2)

        conflict_candidate, conflict_fingerprint = self._pending(
            digest="5" * 64,
            account_id=2,
            platform_uid="987654321",
            candidate=self._candidate("open-id-a"),
            now=1_900_002_000,
        )
        with self.assertRaisesRegex(AuthorizationConflict, "open_id_rebind_conflict"):
            self.store.confirm_authorization(
                state_digest="5" * 64,
                bound_username="operator",
                session_binding="a" * 64,
                open_id_fingerprint=conflict_fingerprint,
                candidate=conflict_candidate,
                cipher=self.cipher,
                request_id="conflict",
                now=1_900_002_010,
            )
        with self.store.read_connection() as connection:
            failed = connection.execute(
                "SELECT status,candidate_ciphertext FROM oauth_states WHERE state_digest=?",
                ("5" * 64,),
            ).fetchone()
        self.assertEqual(failed["status"], "failed")
        self.assertIsNone(failed["candidate_ciphertext"])

        changed_candidate, changed_fingerprint = self._pending(
            digest="8" * 64,
            account_id=2,
            platform_uid="123456789",
            candidate=self._candidate("open-id-a"),
            now=1_900_002_100,
        )
        with self.assertRaisesRegex(AuthorizationConflict, "target_changed"):
            self.store.confirm_authorization(
                state_digest="8" * 64,
                bound_username="operator",
                session_binding="a" * 64,
                open_id_fingerprint=changed_fingerprint,
                candidate=changed_candidate,
                cipher=self.cipher,
                request_id="target-changed",
                now=1_900_002_110,
            )

        owner_candidate, owner_fingerprint = self._pending(
            digest="6" * 64,
            username="other-operator",
            binding="b" * 64,
            candidate=self._candidate("open-id-a"),
            now=1_900_003_000,
        )
        with self.assertRaisesRegex(AuthorizationConflict, "owner_conflict"):
            self.store.confirm_authorization(
                state_digest="6" * 64,
                bound_username="other-operator",
                session_binding="b" * 64,
                open_id_fingerprint=owner_fingerprint,
                candidate=owner_candidate,
                cipher=self.cipher,
                request_id="owner-conflict",
                now=1_900_003_010,
            )

        target_candidate, target_fingerprint = self._pending(
            digest="7" * 64,
            candidate=self._candidate("open-id-b"),
            now=1_900_004_000,
        )
        with self.assertRaisesRegex(AuthorizationConflict, "account_binding_conflict"):
            self.store.confirm_authorization(
                state_digest="7" * 64,
                bound_username="operator",
                session_binding="a" * 64,
                open_id_fingerprint=target_fingerprint,
                candidate=target_candidate,
                cipher=self.cipher,
                request_id="target-conflict",
                now=1_900_004_010,
            )

        with self.assertRaisesRegex(
            AuthorizationConflict, "authorization_version_conflict"
        ):
            self.store.unbind(
                actor="operator",
                authorization_id=authorization_id,
                expected_version=1,
                request_id="stale-unbind",
            )
        self.assertTrue(
            self.store.unbind(
                actor="operator",
                authorization_id=authorization_id,
                expected_version=2,
                request_id="unbind",
            )
        )
        listed = self.store.list_authorizations("operator")
        self.assertEqual(listed[0]["status"], "unbound")
        self.assertEqual(listed[0]["version"], 3)
        self.assertNotIn("scopes_json", listed[0])
        self.assertNotIn("key_version", listed[0])

    def test_machine_authorization_projection_excludes_identity_and_tokens(self) -> None:
        candidate, fingerprint = self._pending(digest="8" * 64)
        created = self.store.confirm_authorization(
            state_digest="8" * 64,
            bound_username="operator",
            session_binding="a" * 64,
            open_id_fingerprint=fingerprint,
            candidate=candidate,
            cipher=self.cipher,
            request_id="confirm-machine-list",
            now=1_900_000_010,
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

    def test_automatic_match_stages_then_activates_and_reauthorizes_in_place(self) -> None:
        _candidate, fingerprint, staged = self._stage(
            digest="b" * 64, open_id="automatic-open-id"
        )
        authorization_id = str(staged["id"])
        self.assertEqual(staged["status"], "pending_match")
        self.assertEqual(staged["version"], 1)
        pending = self.store.get_authorization(authorization_id)
        assert pending is not None
        self.assertEqual(pending["match_reason"], "matching")
        self.assertNotIn("open_id_fingerprint", pending)
        self.assertNotIn("access_token_ciphertext", pending)

        activated = self.store.finalize_auto_match(
            state_digest="b" * 64,
            authorization_id=authorization_id,
            expected_version=1,
            outcome="matched",
            account_id=7,
            platform_uid="123456789",
            actor="operator",
            request_id="automatic-finalize",
            now=1_900_000_003,
        )
        self.assertEqual(activated["status"], "active")
        self.assertEqual(activated["version"], 2)
        statuses = self.store.authorization_statuses(now=1_900_000_004)
        self.assertEqual(len(statuses), 1)
        self.assertTrue(statuses[0]["authorized"])

        candidate2 = self._candidate("automatic-open-id")
        candidate2["access_token"] = "access-by-second-operator"
        self.store.create_state(
            state_digest="c" * 64,
            bound_username="other-operator",
            session_binding="b" * 64,
            target_authorization_id=authorization_id,
            target_authorization_version=2,
            scopes=["user_info", "video.list"],
            expires_at=1_900_001_600,
            request_id="reauthorize-start",
            now=1_900_001_000,
        )
        self.store.begin_exchange(
            "c" * 64,
            "other-operator",
            "b" * 64,
            request_id="reauthorize-callback",
            now=1_900_001_001,
        )
        updated = self.store.stage_authorization_candidate(
            state_digest="c" * 64,
            bound_username="other-operator",
            session_binding="b" * 64,
            open_id_fingerprint=fingerprint,
            candidate=candidate2,
            cipher=self.cipher,
            request_id="reauthorize-stage",
            now=1_900_001_002,
        )
        self.assertEqual(updated["id"], authorization_id)
        self.assertEqual(updated["status"], "active")
        self.assertEqual(updated["account_id"], 7)
        self.assertEqual(updated["platform_uid"], "123456789")
        self.assertEqual(updated["version"], 3)
        active = self.store.get_active_authorization(authorization_id)
        assert active is not None
        self.assertEqual(active["bound_username"], "other-operator")
        access = self.cipher.decrypt(
            authorization_id, "access", bytes(active["access_token_ciphertext"])
        )
        self.assertEqual(access["token"], "access-by-second-operator")

    def test_new_authorization_missing_requested_scope_fails_without_insert(self) -> None:
        digest = "b" * 64
        binding = "c" * 64
        now = 1_900_010_000
        candidate = self._candidate("missing-scope-new-open-id")
        candidate["scopes"] = ["user_info"]
        fingerprint = self.cipher.open_id_fingerprint(str(candidate["open_id"]))
        self.store.create_state(
            state_digest=digest,
            bound_username="scope-operator",
            session_binding=binding,
            scopes=["user_info", "video.list"],
            expires_at=now + 600,
            request_id="scope-start",
            now=now,
        )
        self.store.begin_exchange(
            digest,
            "scope-operator",
            binding,
            request_id="scope-callback",
            now=now + 1,
        )
        with self.assertRaisesRegex(AuthorizationConflict, "oauth_scope_incomplete"):
            self.store.stage_authorization_candidate(
                state_digest=digest,
                bound_username="scope-operator",
                session_binding=binding,
                open_id_fingerprint=fingerprint,
                candidate=candidate,
                cipher=self.cipher,
                request_id="scope-stage",
                now=now + 2,
            )
        with self.store.read_connection() as connection:
            state = connection.execute(
                "SELECT status,failure_reason FROM oauth_states WHERE state_digest=?",
                (digest,),
            ).fetchone()
            authorization_count = connection.execute(
                "SELECT COUNT(*) FROM douyin_authorizations"
            ).fetchone()[0]
            audit = connection.execute(
                """
                SELECT actor,action,result,reason_code
                FROM audit_events
                WHERE action='authorization_stage'
                ORDER BY id DESC LIMIT 1
                """
            ).fetchone()
        self.assertEqual(tuple(state), ("failed", "oauth_scope_incomplete"))
        self.assertEqual(authorization_count, 0)
        self.assertEqual(
            tuple(audit),
            (
                "scope-operator",
                "authorization_stage",
                "failed",
                "oauth_scope_incomplete",
            ),
        )

    def test_active_reauthorization_missing_scope_preserves_existing_token(self) -> None:
        _candidate, fingerprint, staged = self._stage(
            digest="c" * 64,
            open_id="missing-scope-active-open-id",
            now=1_900_020_000,
        )
        authorization_id = str(staged["id"])
        active = self.store.finalize_auto_match(
            state_digest="c" * 64,
            authorization_id=authorization_id,
            expected_version=1,
            outcome="matched",
            account_id=70,
            platform_uid="823456789",
            actor="operator",
            request_id="scope-activate",
            now=1_900_020_003,
        )
        self.store.create_state(
            state_digest="d" * 64,
            bound_username="replacement-operator",
            session_binding="d" * 64,
            target_authorization_id=authorization_id,
            target_authorization_version=int(active["version"]),
            scopes=["user_info", "video.list"],
            expires_at=1_900_021_600,
            request_id="scope-reauthorize-start",
            now=1_900_021_000,
        )
        self.store.begin_exchange(
            "d" * 64,
            "replacement-operator",
            "d" * 64,
            request_id="scope-reauthorize-callback",
            now=1_900_021_001,
        )
        replacement = self._candidate("missing-scope-active-open-id")
        replacement["access_token"] = "must-not-replace-active-token"
        replacement["scopes"] = ["user_info"]
        with self.assertRaisesRegex(AuthorizationConflict, "oauth_scope_incomplete"):
            self.store.stage_authorization_candidate(
                state_digest="d" * 64,
                bound_username="replacement-operator",
                session_binding="d" * 64,
                open_id_fingerprint=fingerprint,
                candidate=replacement,
                cipher=self.cipher,
                request_id="scope-reauthorize-stage",
                now=1_900_021_002,
            )
        preserved = self.store.get_active_authorization(authorization_id)
        assert preserved is not None
        access = self.cipher.decrypt(
            authorization_id, "access", bytes(preserved["access_token_ciphertext"])
        )
        self.assertEqual(access["token"], "access-canary")
        self.assertEqual(preserved["version"], active["version"])
        self.assertEqual(preserved["bound_username"], "operator")
        with self.store.read_connection() as connection:
            state = connection.execute(
                "SELECT status,failure_reason FROM oauth_states WHERE state_digest=?",
                ("d" * 64,),
            ).fetchone()
        self.assertEqual(tuple(state), ("failed", "oauth_scope_incomplete"))

    def test_authorization_statuses_prefer_rebound_active_target(self) -> None:
        _candidate, _fingerprint, first = self._stage(
            digest="4" * 64,
            open_id="status-old-open-id",
            now=1_900_030_000,
        )
        first_id = str(first["id"])
        first_active = self.store.finalize_auto_match(
            state_digest="4" * 64,
            authorization_id=first_id,
            expected_version=1,
            outcome="matched",
            account_id=80,
            platform_uid="923456789",
            actor="operator",
            request_id="status-first-active",
            now=1_900_030_003,
        )
        self.assertTrue(
            self.store.unbind(
                actor="operator",
                authorization_id=first_id,
                expected_version=int(first_active["version"]),
                request_id="status-unbind",
                now=1_900_030_004,
            )
        )
        _candidate2, _fingerprint2, second = self._stage(
            digest="5" * 64,
            open_id="status-new-open-id",
            binding="5" * 64,
            now=1_900_030_100,
        )
        second_id = str(second["id"])
        self.store.finalize_auto_match(
            state_digest="5" * 64,
            authorization_id=second_id,
            expected_version=1,
            outcome="matched",
            account_id=80,
            platform_uid="923456789",
            actor="replacement-operator",
            request_id="status-second-active",
            now=1_900_030_103,
        )
        statuses = [
            item
            for item in self.store.authorization_statuses(now=1_900_030_104)
            if item["account_id"] == 80 and item["platform_uid"] == "923456789"
        ]
        self.assertEqual(len(statuses), 1)
        self.assertEqual(statuses[0]["id"], second_id)
        self.assertEqual(statuses[0]["status"], "active")
        self.assertTrue(statuses[0]["authorized"])

    def test_old_reauthorization_callback_cannot_overwrite_newer_token(self) -> None:
        _candidate, fingerprint, staged = self._stage(
            digest="6" * 64, open_id="reauthorization-race-open-id"
        )
        authorization_id = str(staged["id"])
        self.store.finalize_auto_match(
            state_digest="6" * 64,
            authorization_id=authorization_id,
            expected_version=1,
            outcome="matched",
            account_id=30,
            platform_uid="723456789",
            actor="operator",
            request_id="race-activate",
            now=1_900_000_003,
        )

        self.store.create_state(
            state_digest="7" * 64,
            bound_username="old-operator",
            session_binding="7" * 64,
            target_authorization_id=authorization_id,
            target_authorization_version=2,
            scopes=["user_info", "video.list"],
            expires_at=1_900_001_600,
            request_id="old-reauthorization-start",
            now=1_900_001_000,
        )
        self.store.begin_exchange(
            "7" * 64,
            "old-operator",
            "7" * 64,
            request_id="old-reauthorization-callback",
            now=1_900_001_001,
        )

        newer = self._candidate("reauthorization-race-open-id")
        newer["access_token"] = "newer-access-token"
        self.store.create_state(
            state_digest="8" * 64,
            bound_username="new-operator",
            session_binding="8" * 64,
            target_authorization_id=authorization_id,
            target_authorization_version=2,
            scopes=["user_info", "video.list"],
            expires_at=1_900_001_700,
            request_id="new-reauthorization-start",
            now=1_900_001_100,
        )
        self.store.begin_exchange(
            "8" * 64,
            "new-operator",
            "8" * 64,
            request_id="new-reauthorization-callback",
            now=1_900_001_101,
        )
        current = self.store.stage_authorization_candidate(
            state_digest="8" * 64,
            bound_username="new-operator",
            session_binding="8" * 64,
            open_id_fingerprint=fingerprint,
            candidate=newer,
            cipher=self.cipher,
            request_id="new-reauthorization-stage",
            now=1_900_001_102,
        )
        self.assertEqual(current["version"], 3)

        older = self._candidate("reauthorization-race-open-id")
        older["access_token"] = "stale-access-token"
        with self.assertRaisesRegex(
            AuthorizationConflict, "reauthorization_version_conflict"
        ):
            self.store.stage_authorization_candidate(
                state_digest="7" * 64,
                bound_username="old-operator",
                session_binding="7" * 64,
                open_id_fingerprint=fingerprint,
                candidate=older,
                cipher=self.cipher,
                request_id="old-reauthorization-stage",
                now=1_900_001_103,
            )
        active = self.store.get_active_authorization(authorization_id)
        assert active is not None
        self.assertEqual(active["version"], 3)
        access = self.cipher.decrypt(
            authorization_id, "access", bytes(active["access_token_ciphertext"])
        )
        self.assertEqual(access["token"], "newer-access-token")
        with self.store.read_connection() as connection:
            old_state = connection.execute(
                "SELECT status,failure_reason FROM oauth_states WHERE state_digest=?",
                ("7" * 64,),
            ).fetchone()
        self.assertEqual(tuple(old_state), ("failed", "reauthorization_version_conflict"))

    def test_pending_rescan_reuses_id_and_stale_finalize_cannot_overwrite(self) -> None:
        _candidate, _fingerprint, first = self._stage(
            digest="d" * 64,
            open_id="pending-rescan-open-id",
            username="operator-a",
            binding="c" * 64,
        )
        authorization_id = str(first["id"])
        _candidate2, _fingerprint2, second = self._stage(
            digest="e" * 64,
            open_id="pending-rescan-open-id",
            username="operator-b",
            binding="d" * 64,
            now=1_900_000_100,
        )
        self.assertEqual(second["id"], authorization_id)
        self.assertEqual(second["version"], 2)
        with self.store.read_connection() as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM douyin_authorizations WHERE open_id_fingerprint=?",
                (self.cipher.open_id_fingerprint("pending-rescan-open-id"),),
            ).fetchone()[0]
        self.assertEqual(count, 1)
        stale = self.store.finalize_auto_match(
            state_digest="d" * 64,
            authorization_id=authorization_id,
            expected_version=1,
            outcome="matched",
            account_id=8,
            platform_uid="223456789",
            actor="operator-a",
            request_id="stale-finalize",
            now=1_900_000_103,
        )
        self.assertEqual(stale["status"], "stale")
        row = self.store.get_authorization(authorization_id)
        assert row is not None
        self.assertEqual(row["version"], 2)
        self.assertEqual(row["status"], "pending_match")
        self.assertIsNone(row["account_id"])
        self.assertEqual(row["match_reason"], "matching")

    def test_occupied_uid_stays_pending_and_manual_match_rechecks_target(self) -> None:
        _candidate, _fingerprint, first = self._stage(
            digest="f" * 64, open_id="occupied-open-id-a"
        )
        first_id = str(first["id"])
        self.store.finalize_auto_match(
            state_digest="f" * 64,
            authorization_id=first_id,
            expected_version=1,
            outcome="matched",
            account_id=10,
            platform_uid="323456789",
            actor="operator",
            request_id="first-active",
            now=1_900_000_003,
        )
        _candidate2, _fingerprint2, second = self._stage(
            digest="0" * 64,
            open_id="occupied-open-id-b",
            now=1_900_000_100,
        )
        second_id = str(second["id"])
        conflict = self.store.finalize_auto_match(
            state_digest="0" * 64,
            authorization_id=second_id,
            expected_version=1,
            outcome="matched",
            account_id=10,
            platform_uid="323456789",
            actor="operator",
            request_id="occupied-finalize",
            now=1_900_000_103,
        )
        self.assertEqual(conflict["status"], "pending_match")
        self.assertEqual(conflict["match_reason"], "account_binding_conflict")
        occupied_statuses = [
            item
            for item in self.store.authorization_statuses(now=1_900_000_103)
            if item["account_id"] == 10 and item["platform_uid"] == "323456789"
        ]
        self.assertEqual(len(occupied_statuses), 1)
        self.assertEqual(occupied_statuses[0]["id"], first_id)
        self.assertTrue(occupied_statuses[0]["authorized"])
        with self.assertRaisesRegex(AuthorizationConflict, "account_binding_conflict"):
            self.store.manual_match(
                authorization_id=second_id,
                account_id=10,
                platform_uid="323456789",
                expected_version=2,
                actor="other-operator",
                request_id="manual-conflict",
                now=1_900_000_104,
            )
        activated = self.store.manual_match(
            authorization_id=second_id,
            account_id=11,
            platform_uid="423456789",
            expected_version=3,
            actor="other-operator",
            request_id="manual-success",
            now=1_900_000_105,
        )
        self.assertEqual(activated["status"], "active")
        self.assertEqual(activated["version"], 4)
        with self.assertRaisesRegex(AuthorizationConflict, "authorization_not_pending"):
            self.store.manual_match(
                authorization_id=second_id,
                account_id=12,
                platform_uid="523456789",
                expected_version=4,
                actor="third-operator",
                request_id="active-cannot-rebind",
                now=1_900_000_106,
            )
        self.assertTrue(
            self.store.unbind(
                actor="third-operator",
                authorization_id=second_id,
                expected_version=4,
                request_id="any-operator-unbind",
                now=1_900_000_107,
            )
        )
        _candidate3, _fingerprint3, restaged = self._stage(
            digest="a" * 64,
            open_id="occupied-open-id-b",
            username="fourth-operator",
            binding="e" * 64,
            target_authorization_id=second_id,
            target_authorization_version=5,
            now=1_900_000_200,
        )
        self.assertEqual(restaged["id"], second_id)
        self.assertEqual(restaged["status"], "pending_match")
        self.assertIsNone(restaged["account_id"])
        self.assertIsNone(restaged["platform_uid"])

    def test_auto_match_outcomes_status_projection_and_wrong_target_rejected(self) -> None:
        staged_items: list[tuple[str, str, int]] = []
        for index, outcome in enumerate(("unmatched", "ambiguous", "unavailable")):
            digest = str(index + 1) * 64
            _candidate, _fingerprint, staged = self._stage(
                digest=digest,
                open_id=f"outcome-open-id-{index}",
                username=f"operator-{index}",
                binding=str(index + 5) * 64,
                now=1_900_002_000 + index * 100,
            )
            result = self.store.finalize_auto_match(
                state_digest=digest,
                authorization_id=str(staged["id"]),
                expected_version=1,
                outcome=outcome,
                actor=f"operator-{index}",
                request_id=f"outcome-{outcome}",
                now=1_900_002_003 + index * 100,
            )
            staged_items.append(
                (str(staged["id"]), str(result["match_reason"]), int(result["version"]))
            )
        self.assertEqual(
            [reason for _id, reason, _version in staged_items],
            ["no_match", "ambiguous_match", "auto_match_unavailable"],
        )
        listed = self.store.list_authorizations("unrelated-operator")
        self.assertEqual(len(listed), 3)
        self.assertTrue(all(item["status"] == "pending_match" for item in listed))
        self.assertTrue(all(not item["authorized"] for item in self.store.authorization_statuses()))

        target_id = staged_items[0][0]
        activated = self.store.manual_match(
            authorization_id=target_id,
            account_id=20,
            platform_uid="623456789",
            expected_version=2,
            actor="operator-admin",
            request_id="activate-before-wrong-target",
            now=1_900_002_500,
        )
        self.assertEqual(activated["status"], "active")
        self.store.create_state(
            state_digest="9" * 64,
            bound_username="operator-x",
            session_binding="9" * 64,
            target_authorization_id=target_id,
            target_authorization_version=3,
            scopes=["user_info", "video.list"],
            expires_at=1_900_003_600,
            request_id="wrong-target-start",
            now=1_900_003_000,
        )
        self.store.begin_exchange(
            "9" * 64,
            "operator-x",
            "9" * 64,
            request_id="wrong-target-callback",
            now=1_900_003_001,
        )
        wrong_candidate = self._candidate("different-open-id")
        with self.assertRaisesRegex(
            AuthorizationConflict, "reauthorization_target_mismatch"
        ):
            self.store.stage_authorization_candidate(
                state_digest="9" * 64,
                bound_username="operator-x",
                session_binding="9" * 64,
                open_id_fingerprint=self.cipher.open_id_fingerprint(
                    "different-open-id"
                ),
                candidate=wrong_candidate,
                cipher=self.cipher,
                request_id="wrong-target-stage",
                now=1_900_003_002,
            )
        with self.store.read_connection() as connection:
            wrong_rows = connection.execute(
                "SELECT COUNT(*) FROM douyin_authorizations WHERE open_id_fingerprint=?",
                (self.cipher.open_id_fingerprint("different-open-id"),),
            ).fetchone()[0]
        self.assertEqual(wrong_rows, 0)

    def test_token_lifecycle_lease_updates_limit_and_reauthorization_audit(self) -> None:
        candidate, fingerprint = self._pending(digest="9" * 64)
        created = self.store.confirm_authorization(
            state_digest="9" * 64,
            bound_username="operator",
            session_binding="a" * 64,
            open_id_fingerprint=fingerprint,
            candidate=candidate,
            cipher=self.cipher,
            request_id="confirm-token-lifecycle",
            now=1_900_000_010,
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
        self.assertEqual(active_after_rotation["access_expires_at"], bundle_access_expiry)
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
        candidate, fingerprint = self._pending(digest="a" * 64)
        created = self.store.confirm_authorization(
            state_digest="a" * 64,
            bound_username="operator",
            session_binding="a" * 64,
            open_id_fingerprint=fingerprint,
            candidate=candidate,
            cipher=self.cipher,
            request_id="confirm-concurrent-lease",
            now=1_900_000_010,
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
