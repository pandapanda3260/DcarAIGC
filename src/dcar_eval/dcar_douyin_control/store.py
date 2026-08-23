from __future__ import annotations

import json
import hmac
import sqlite3
import stat
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator, Mapping, Optional

if TYPE_CHECKING:
    from .crypto import TokenCipher



class StateTransitionError(RuntimeError):
    pass


class AuthorizationConflict(RuntimeError):
    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


class VaultStore:
    """Single-writer SQLite store with a fail-closed DELETE-journal contract."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def initialize(self) -> None:
        if not self.path.parent.is_dir():
            raise RuntimeError("Douyin Vault directory must be pre-created")
        directory_mode = stat.S_IMODE(self.path.parent.stat().st_mode)
        if directory_mode != 0o700:
            raise RuntimeError("Douyin Vault directory mode must be 0700")
        self._preflight_delete_journal()
        connection = self._connect()
        try:
            connection.executescript(
                """
                BEGIN IMMEDIATE;
                CREATE TABLE IF NOT EXISTS oauth_states(
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

                CREATE INDEX IF NOT EXISTS oauth_states_current
                    ON oauth_states(bound_username,session_binding,status,updated_at DESC);

                CREATE UNIQUE INDEX IF NOT EXISTS oauth_states_live_session
                    ON oauth_states(bound_username,session_binding)
                    WHERE status IN ('created','exchanging','pending_confirmation');

                CREATE TABLE IF NOT EXISTS douyin_authorizations(
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

                CREATE UNIQUE INDEX IF NOT EXISTS douyin_authorizations_active_target
                    ON douyin_authorizations(account_id,platform_uid)
                    WHERE status='active';

                CREATE TABLE IF NOT EXISTS audit_events(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    occurred_at INTEGER NOT NULL,
                    actor TEXT NOT NULL,
                    action TEXT NOT NULL,
                    result TEXT NOT NULL,
                    reason_code TEXT NOT NULL,
                    subject_fingerprint TEXT NOT NULL,
                    request_id TEXT NOT NULL
                );
                PRAGMA user_version=1;
                COMMIT;
                """
            )
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
        self.path.chmod(0o600)

    def _preflight_delete_journal(self) -> None:
        connection = sqlite3.connect(self.path, timeout=10.0, isolation_level=None)
        try:
            connection.execute("PRAGMA busy_timeout=10000")
            mode = str(
                connection.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
            ).lower()
            if mode != "delete":
                raise RuntimeError(
                    "Douyin Vault must use SQLite journal_mode=DELETE"
                )
        finally:
            connection.close()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10.0, isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA busy_timeout=10000")
            mode = str(
                connection.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
            ).lower()
            connection.execute("PRAGMA synchronous=EXTRA")
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA locking_mode=NORMAL")
            settings = {
                "journal_mode": mode,
                "synchronous": int(
                    connection.execute("PRAGMA synchronous").fetchone()[0]
                ),
                "foreign_keys": int(
                    connection.execute("PRAGMA foreign_keys").fetchone()[0]
                ),
                "busy_timeout": int(
                    connection.execute("PRAGMA busy_timeout").fetchone()[0]
                ),
                "locking_mode": str(
                    connection.execute("PRAGMA locking_mode").fetchone()[0]
                ).lower(),
            }
            expected: Mapping[str, Any] = {
                "journal_mode": "delete",
                "synchronous": 3,
                "foreign_keys": 1,
                "busy_timeout": 10000,
                "locking_mode": "normal",
            }
            if settings != expected:
                raise RuntimeError("Douyin Vault SQLite connection is unsafe")
            return connection
        except BaseException:
            connection.close()
            raise

    @contextmanager
    def read_connection(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def write_connection(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
            except BaseException:
                connection.rollback()
                raise
            else:
                connection.commit()
        finally:
            connection.close()

    @staticmethod
    def _row(row: Optional[sqlite3.Row]) -> Optional[dict[str, Any]]:
        return dict(row) if row is not None else None

    @staticmethod
    def _audit(
        connection: sqlite3.Connection,
        *,
        actor: str,
        action: str,
        result: str,
        reason_code: str,
        subject_fingerprint: str,
        request_id: str,
        now: int,
    ) -> None:
        connection.execute(
            """
            INSERT INTO audit_events(
                occurred_at,actor,action,result,reason_code,
                subject_fingerprint,request_id
            ) VALUES(?,?,?,?,?,?,?)
            """,
            (
                now,
                actor,
                action,
                result,
                reason_code,
                subject_fingerprint[:64],
                request_id[:128],
            ),
        )

    @staticmethod
    def _expire_states(connection: sqlite3.Connection, now: int) -> None:
        connection.execute(
            """
            UPDATE oauth_states
            SET status='expired',candidate_ciphertext=NULL,
                candidate_open_id_fingerprint=NULL,failure_reason='expired',updated_at=?
            WHERE status IN ('created','exchanging','pending_confirmation')
              AND expires_at<=?
            """,
            (now, now),
        )

    def create_state(
        self,
        *,
        state_digest: str,
        bound_username: str,
        session_binding: str,
        account_id: int,
        platform_uid: str,
        scopes: list[str],
        expires_at: int,
        request_id: str,
        now: Optional[int] = None,
    ) -> None:
        timestamp = int(time.time()) if now is None else now
        with self.write_connection() as connection:
            self._expire_states(connection, timestamp)
            connection.execute(
                """
                UPDATE oauth_states
                SET status='expired',candidate_ciphertext=NULL,
                    candidate_open_id_fingerprint=NULL,
                    failure_reason='superseded',updated_at=?
                WHERE bound_username=? AND session_binding=?
                  AND status IN ('created','exchanging','pending_confirmation')
                """,
                (timestamp, bound_username, session_binding),
            )
            connection.execute(
                """
                INSERT INTO oauth_states(
                    state_digest,bound_username,session_binding,account_id,
                    platform_uid,requested_scopes_json,status,expires_at,
                    created_at,updated_at
                ) VALUES(?,?,?,?,?,?,'created',?,?,?)
                """,
                (
                    state_digest,
                    bound_username,
                    session_binding,
                    account_id,
                    platform_uid,
                    json.dumps(scopes, separators=(",", ":")),
                    expires_at,
                    timestamp,
                    timestamp,
                ),
            )
            self._audit(
                connection,
                actor=bound_username,
                action="oauth_start",
                result="created",
                reason_code="ok",
                subject_fingerprint=state_digest,
                request_id=request_id,
                now=timestamp,
            )

    def begin_exchange(
        self,
        state_digest: str,
        bound_username: str,
        session_binding: str,
        *,
        request_id: str,
        now: Optional[int] = None,
    ) -> dict[str, Any]:
        timestamp = int(time.time()) if now is None else now
        with self.write_connection() as connection:
            self._expire_states(connection, timestamp)
            cursor = connection.execute(
                """
                UPDATE oauth_states SET status='exchanging',updated_at=?
                WHERE state_digest=? AND bound_username=? AND session_binding=?
                  AND status='created' AND expires_at>?
                """,
                (
                    timestamp,
                    state_digest,
                    bound_username,
                    session_binding,
                    timestamp,
                ),
            )
            if cursor.rowcount != 1:
                raise StateTransitionError("oauth_state_unavailable")
            row = connection.execute(
                "SELECT * FROM oauth_states WHERE state_digest=?",
                (state_digest,),
            ).fetchone()
            self._audit(
                connection,
                actor=bound_username,
                action="oauth_callback",
                result="exchanging",
                reason_code="ok",
                subject_fingerprint=state_digest,
                request_id=request_id,
                now=timestamp,
            )
            assert row is not None
            return dict(row)

    def store_candidate(
        self,
        *,
        state_digest: str,
        ciphertext: bytes,
        open_id_fingerprint: str,
        confirmation_expires_at: int,
        request_id: str,
        now: Optional[int] = None,
    ) -> None:
        timestamp = int(time.time()) if now is None else now
        with self.write_connection() as connection:
            cursor = connection.execute(
                """
                UPDATE oauth_states
                SET status='pending_confirmation',candidate_ciphertext=?,
                    candidate_open_id_fingerprint=?,expires_at=?,updated_at=?
                WHERE state_digest=? AND status='exchanging'
                """,
                (
                    ciphertext,
                    open_id_fingerprint,
                    confirmation_expires_at,
                    timestamp,
                    state_digest,
                ),
            )
            if cursor.rowcount != 1:
                raise StateTransitionError("oauth_state_not_exchanging")
            row = connection.execute(
                "SELECT bound_username FROM oauth_states WHERE state_digest=?",
                (state_digest,),
            ).fetchone()
            assert row is not None
            self._audit(
                connection,
                actor=str(row["bound_username"]),
                action="oauth_callback",
                result="pending_confirmation",
                reason_code="ok",
                subject_fingerprint=state_digest,
                request_id=request_id,
                now=timestamp,
            )

    def fail_state(
        self,
        state_digest: str,
        reason_code: str,
        *,
        request_id: str,
        actor: str,
        now: Optional[int] = None,
    ) -> None:
        timestamp = int(time.time()) if now is None else now
        with self.write_connection() as connection:
            connection.execute(
                """
                UPDATE oauth_states
                SET status='failed',candidate_ciphertext=NULL,
                    candidate_open_id_fingerprint=NULL,failure_reason=?,updated_at=?
                WHERE state_digest=? AND status NOT IN ('confirmed','rejected','expired')
                """,
                (reason_code[:64], timestamp, state_digest),
            )
            self._audit(
                connection,
                actor=actor,
                action="oauth_callback",
                result="failed",
                reason_code=reason_code,
                subject_fingerprint=state_digest,
                request_id=request_id,
                now=timestamp,
            )

    def current_pending(
        self,
        bound_username: str,
        session_binding: str,
        *,
        now: Optional[int] = None,
    ) -> Optional[dict[str, Any]]:
        timestamp = int(time.time()) if now is None else now
        with self.write_connection() as connection:
            self._expire_states(connection, timestamp)
            return self._row(
                connection.execute(
                    """
                    SELECT * FROM oauth_states
                    WHERE bound_username=? AND session_binding=?
                      AND status='pending_confirmation' AND expires_at>?
                    ORDER BY updated_at DESC LIMIT 1
                    """,
                    (bound_username, session_binding, timestamp),
                ).fetchone()
            )

    def expire_due_states(self, *, now: Optional[int] = None) -> int:
        timestamp = int(time.time()) if now is None else now
        with self.write_connection() as connection:
            cursor = connection.execute(
                """
                UPDATE oauth_states
                SET status='expired',candidate_ciphertext=NULL,
                    candidate_open_id_fingerprint=NULL,
                    failure_reason='expired',updated_at=?
                WHERE status IN ('created','exchanging','pending_confirmation')
                  AND expires_at<=?
                """,
                (timestamp, timestamp),
            )
            return cursor.rowcount

    def confirm_authorization(
        self,
        *,
        state_digest: str,
        bound_username: str,
        session_binding: str,
        open_id_fingerprint: str,
        candidate: Mapping[str, Any],
        cipher: "TokenCipher",
        request_id: str,
        now: Optional[int] = None,
    ) -> dict[str, Any]:
        timestamp = int(time.time()) if now is None else now
        conflict_reason: Optional[str] = None
        result: Optional[dict[str, Any]] = None
        with self.write_connection() as connection:
            self._expire_states(connection, timestamp)
            state = connection.execute(
                """
                SELECT * FROM oauth_states
                WHERE state_digest=? AND bound_username=? AND session_binding=?
                  AND status='pending_confirmation' AND expires_at>?
                """,
                (state_digest, bound_username, session_binding, timestamp),
            ).fetchone()
            if state is None:
                raise StateTransitionError("oauth_confirmation_unavailable")
            if not hmac.compare_digest(
                str(state["candidate_open_id_fingerprint"]), open_id_fingerprint
            ):
                raise StateTransitionError("oauth_candidate_identity_changed")

            account_id = int(state["account_id"])
            platform_uid = str(state["platform_uid"])
            open_id = str(candidate["open_id"])
            if not hmac.compare_digest(
                cipher.open_id_fingerprint(open_id), open_id_fingerprint
            ):
                raise StateTransitionError("oauth_candidate_identity_changed")
            existing = connection.execute(
                """
                SELECT * FROM douyin_authorizations
                WHERE open_id_fingerprint=?
                """,
                (open_id_fingerprint,),
            ).fetchone()
            occupied = connection.execute(
                """
                SELECT id,open_id_fingerprint FROM douyin_authorizations
                WHERE account_id=? AND platform_uid=? AND status='active'
                """,
                (account_id, platform_uid),
            ).fetchone()
            if existing is not None and not hmac.compare_digest(
                str(existing["bound_username"]), bound_username
            ):
                conflict_reason = "owner_conflict"
            elif existing is not None and str(existing["status"]) == "active" and (
                int(existing["account_id"]) != account_id
                or str(existing["platform_uid"]) != platform_uid
            ):
                conflict_reason = "open_id_rebind_conflict"
            elif occupied is not None and not hmac.compare_digest(
                str(occupied["open_id_fingerprint"]), open_id_fingerprint
            ):
                conflict_reason = "account_binding_conflict"

            if conflict_reason is not None:
                connection.execute(
                    """
                    UPDATE oauth_states
                    SET status='failed',candidate_ciphertext=NULL,
                        candidate_open_id_fingerprint=NULL,
                        failure_reason=?,updated_at=?
                    WHERE state_digest=?
                    """,
                    (conflict_reason, timestamp, state_digest),
                )
                self._audit(
                    connection,
                    actor=bound_username,
                    action="oauth_confirm",
                    result="failed",
                    reason_code=conflict_reason,
                    subject_fingerprint=open_id_fingerprint,
                    request_id=request_id,
                    now=timestamp,
                )
            else:
                authorization_id = (
                    uuid.uuid4().hex if existing is None else str(existing["id"])
                )
                access_token_ciphertext = cipher.encrypt(
                    authorization_id,
                    "access",
                    {"open_id": open_id, "token": str(candidate["access_token"])},
                )
                refresh_token_ciphertext = cipher.encrypt(
                    authorization_id,
                    "refresh",
                    {"open_id": open_id, "token": str(candidate["refresh_token"])},
                )
                scopes = [str(scope) for scope in candidate["scopes"]]
                scopes_json = json.dumps(scopes, separators=(",", ":"))
                access_expires_at = int(candidate["access_expires_at"])
                refresh_expires_at = int(candidate["refresh_expires_at"])
                key_version = cipher.key_version

                if existing is None:
                    version = 1
                    connection.execute(
                        """
                        INSERT INTO douyin_authorizations(
                            id,open_id_fingerprint,bound_username,account_id,
                            platform_uid,access_token_ciphertext,
                            refresh_token_ciphertext,access_expires_at,
                            refresh_expires_at,renew_count,scopes_json,key_version,
                            version,refresh_lease_owner,refresh_lease_expires_at,
                            needs_reauthorization,status,created_at,updated_at,
                            last_authorized_at,unbound_at
                        ) VALUES(?,?,?,?,?,?,?,?,?,0,?,?,1,NULL,NULL,0,'active',?,?,?,NULL)
                        """,
                        (
                            authorization_id,
                            open_id_fingerprint,
                            bound_username,
                            account_id,
                            platform_uid,
                            access_token_ciphertext,
                            refresh_token_ciphertext,
                            access_expires_at,
                            refresh_expires_at,
                            scopes_json,
                            key_version,
                            timestamp,
                            timestamp,
                            timestamp,
                        ),
                    )
                else:
                    version = int(existing["version"]) + 1
                    connection.execute(
                        """
                        UPDATE douyin_authorizations
                        SET bound_username=?,account_id=?,platform_uid=?,
                            access_token_ciphertext=?,refresh_token_ciphertext=?,
                            access_expires_at=?,refresh_expires_at=?,renew_count=0,
                            scopes_json=?,key_version=?,version=?,
                            refresh_lease_owner=NULL,refresh_lease_expires_at=NULL,
                            needs_reauthorization=0,status='active',updated_at=?,
                            last_authorized_at=?,unbound_at=NULL
                        WHERE id=?
                        """,
                        (
                            bound_username,
                            account_id,
                            platform_uid,
                            access_token_ciphertext,
                            refresh_token_ciphertext,
                            access_expires_at,
                            refresh_expires_at,
                            scopes_json,
                            key_version,
                            version,
                            timestamp,
                            timestamp,
                            authorization_id,
                        ),
                    )
                connection.execute(
                    """
                    UPDATE oauth_states
                    SET status='confirmed',candidate_ciphertext=NULL,
                        candidate_open_id_fingerprint=NULL,updated_at=?
                    WHERE state_digest=?
                    """,
                    (timestamp, state_digest),
                )
                self._audit(
                    connection,
                    actor=bound_username,
                    action="oauth_confirm",
                    result="confirmed",
                    reason_code="ok",
                    subject_fingerprint=open_id_fingerprint,
                    request_id=request_id,
                    now=timestamp,
                )
                result = {
                    "id": authorization_id,
                    "account_id": account_id,
                    "platform_uid": platform_uid,
                    "version": version,
                }
        if conflict_reason is not None:
            raise AuthorizationConflict(conflict_reason)
        if result is None:
            raise RuntimeError("authorization confirmation produced no result")
        return result

    def reject_current(
        self,
        bound_username: str,
        session_binding: str,
        *,
        request_id: str,
        now: Optional[int] = None,
    ) -> bool:
        timestamp = int(time.time()) if now is None else now
        with self.write_connection() as connection:
            self._expire_states(connection, timestamp)
            row = connection.execute(
                """
                SELECT state_digest FROM oauth_states
                WHERE bound_username=? AND session_binding=?
                  AND status='pending_confirmation' AND expires_at>?
                ORDER BY updated_at DESC LIMIT 1
                """,
                (bound_username, session_binding, timestamp),
            ).fetchone()
            if row is None:
                return False
            state_digest = str(row["state_digest"])
            connection.execute(
                """
                UPDATE oauth_states SET status='rejected',candidate_ciphertext=NULL,
                    candidate_open_id_fingerprint=NULL,updated_at=?
                WHERE state_digest=?
                """,
                (timestamp, state_digest),
            )
            self._audit(
                connection,
                actor=bound_username,
                action="oauth_reject",
                result="rejected",
                reason_code="operator_rejected",
                subject_fingerprint=state_digest,
                request_id=request_id,
                now=timestamp,
            )
            return True

    def list_authorizations(self, bound_username: str) -> list[dict[str, Any]]:
        with self.read_connection() as connection:
            rows = connection.execute(
                """
                SELECT id,account_id,platform_uid,access_expires_at,
                       refresh_expires_at,renew_count,scopes_json,
                       version,needs_reauthorization,status,updated_at
                FROM douyin_authorizations
                WHERE bound_username=? ORDER BY updated_at DESC
                """,
                (bound_username,),
            ).fetchall()
            items: list[dict[str, Any]] = []
            for row in rows:
                item = dict(row)
                item["scopes"] = json.loads(str(item.pop("scopes_json")))
                item["needs_reauthorization"] = bool(
                    item["needs_reauthorization"]
                )
                items.append(item)
            return items

    def unbind(
        self,
        *,
        bound_username: str,
        authorization_id: str,
        expected_version: int,
        request_id: str,
        now: Optional[int] = None,
    ) -> bool:
        timestamp = int(time.time()) if now is None else now
        with self.write_connection() as connection:
            row = connection.execute(
                """
                SELECT id,open_id_fingerprint,version FROM douyin_authorizations
                WHERE bound_username=? AND id=? AND status='active'
                """,
                (bound_username, authorization_id),
            ).fetchone()
            if row is None:
                return False
            if int(row["version"]) != expected_version:
                raise AuthorizationConflict("authorization_version_conflict")
            connection.execute(
                """
                UPDATE douyin_authorizations
                SET access_token_ciphertext=NULL,refresh_token_ciphertext=NULL,
                    access_expires_at=NULL,refresh_expires_at=NULL,
                    refresh_lease_owner=NULL,refresh_lease_expires_at=NULL,
                    needs_reauthorization=1,status='unbound',updated_at=?,
                    unbound_at=?,version=version+1 WHERE id=?
                """,
                (timestamp, timestamp, str(row["id"])),
            )
            self._audit(
                connection,
                actor=bound_username,
                action="authorization_unbind",
                result="unbound",
                reason_code="operator_unbound",
                subject_fingerprint=str(row["open_id_fingerprint"]),
                request_id=request_id,
                now=timestamp,
            )
            return True

    def healthcheck(self) -> dict[str, Any]:
        with self.read_connection() as connection:
            quick_check = str(
                connection.execute("PRAGMA quick_check").fetchone()[0]
            )
            mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0])
            return {"quick_check": quick_check, "journal_mode": mode.lower()}
