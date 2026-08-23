from __future__ import annotations

import base64
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

    def test_initialization_enforces_delete_and_connection_pragmas(self) -> None:
        with sqlite3.connect(self.vault_path) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 2)
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
        self.assertTrue(
            {"oauth_states", "douyin_authorizations", "audit_events"}.issubset(
                tables
            )
        )
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
        self.store.initialize()
        self.store.initialize()
        with sqlite3.connect(self.vault_path) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 2)
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
        self.assertEqual(row["status"], "rejected")
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
                bound_username="operator",
                authorization_id=authorization_id,
                expected_version=1,
                request_id="stale-unbind",
            )
        self.assertTrue(
            self.store.unbind(
                bound_username="operator",
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
