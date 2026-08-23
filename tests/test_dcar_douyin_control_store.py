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
            "scopes": ["user_info", "video.list", "renew_refresh_token"],
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
            scopes=["user_info", "video.list", "renew_refresh_token"],
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
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 1)
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
