from __future__ import annotations

import json
import hmac
import re
import sqlite3
import stat
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator, Mapping, Optional

if TYPE_CHECKING:
    from .crypto import TokenCipher


VAULT_SCHEMA_VERSION = 3
_AUTHORIZATION_ID_RE = re.compile(r"[0-9a-f]{32}\Z")


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
            current_version = int(
                connection.execute("PRAGMA user_version").fetchone()[0]
            )
            existing_tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
                if not str(row[0]).startswith("sqlite_")
            }
            if current_version == 0:
                vault_tables = {
                    "oauth_states",
                    "douyin_authorizations",
                    "audit_events",
                }
                if existing_tables.intersection(vault_tables):
                    raise RuntimeError(
                        "Douyin Vault has application tables but no supported schema version"
                    )
                self._create_schema_v3(connection)
            elif current_version == 1:
                self._migrate_v1_to_v2(connection)
                self._migrate_v2_to_v3(connection)
            elif current_version == 2:
                self._migrate_v2_to_v3(connection)
            elif current_version == VAULT_SCHEMA_VERSION:
                self._validate_schema_v3(connection)
            else:
                raise RuntimeError(
                    f"Unsupported Douyin Vault schema version: {current_version}"
                )
        finally:
            connection.close()
        self.path.chmod(0o600)

    @staticmethod
    def _create_schema_v3(connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
                BEGIN IMMEDIATE;
                CREATE TABLE IF NOT EXISTS oauth_states(
                    state_digest TEXT PRIMARY KEY,
                    bound_username TEXT NOT NULL,
                    session_binding TEXT NOT NULL,
                    account_id INTEGER,
                    platform_uid TEXT,
                    target_authorization_id TEXT,
                    target_authorization_version INTEGER,
                    requested_scopes_json TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN (
                        'created','exchanging','matching','completed','failed','expired'
                    )),
                    expires_at INTEGER NOT NULL,
                    candidate_ciphertext BLOB,
                    candidate_open_id_fingerprint TEXT,
                    failure_reason TEXT,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    CHECK(
                        (target_authorization_id IS NULL AND
                         target_authorization_version IS NULL) OR
                        (target_authorization_id IS NOT NULL AND
                         target_authorization_version IS NOT NULL AND
                         target_authorization_version>=1)
                    )
                );

                CREATE INDEX IF NOT EXISTS oauth_states_current
                    ON oauth_states(bound_username,session_binding,status,updated_at DESC);

                CREATE UNIQUE INDEX IF NOT EXISTS oauth_states_live_session
                    ON oauth_states(bound_username,session_binding)
                    WHERE status IN ('created','exchanging','matching');

                CREATE TABLE IF NOT EXISTS douyin_authorizations(
                    id TEXT PRIMARY KEY,
                    open_id_fingerprint TEXT NOT NULL UNIQUE,
                    bound_username TEXT NOT NULL,
                    account_id INTEGER,
                    platform_uid TEXT,
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
                    status TEXT NOT NULL CHECK(status IN (
                        'active','unbound','pending_match'
                    )),
                    match_reason TEXT CHECK(match_reason IS NULL OR match_reason IN (
                        'matching','no_match','ambiguous_match',
                        'account_binding_conflict','auto_match_unavailable',
                        'reauthorization_required'
                    )),
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    last_authorized_at INTEGER NOT NULL,
                    unbound_at INTEGER,
                    CHECK(
                        (account_id IS NULL AND platform_uid IS NULL) OR
                        (account_id IS NOT NULL AND platform_uid IS NOT NULL)
                    ),
                    CHECK(status!='active' OR (
                        account_id IS NOT NULL AND platform_uid IS NOT NULL AND
                        match_reason IS NULL
                    )),
                    CHECK(status!='pending_match' OR match_reason IS NOT NULL)
                );

                CREATE UNIQUE INDEX IF NOT EXISTS douyin_authorizations_active_target
                    ON douyin_authorizations(platform_uid)
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
                PRAGMA user_version=3;
                COMMIT;
            """
        )

    @staticmethod
    def _active_platform_uid_duplicates(
        connection: sqlite3.Connection,
    ) -> list[str]:
        return [
            str(row[0])
            for row in connection.execute(
                """
                SELECT platform_uid
                FROM douyin_authorizations
                WHERE status='active'
                GROUP BY platform_uid
                HAVING COUNT(*)>1
                ORDER BY platform_uid
                """
            )
        ]

    @staticmethod
    def _invalid_authorization_ids(connection: sqlite3.Connection) -> int:
        return int(
            connection.execute(
                """
                SELECT COUNT(*) FROM douyin_authorizations
                WHERE length(id)!=32 OR id GLOB '*[^0-9a-f]*'
                """
            ).fetchone()[0]
        )

    @classmethod
    def _migrate_v1_to_v2(cls, connection: sqlite3.Connection) -> None:
        required_tables = {"oauth_states", "douyin_authorizations", "audit_events"}
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if not required_tables.issubset(tables):
            raise RuntimeError("Douyin Vault schema v1 is missing required tables")
        if cls._invalid_authorization_ids(connection):
            raise RuntimeError("Douyin Vault schema v1 has invalid authorization IDs")
        duplicates = cls._active_platform_uid_duplicates(connection)
        if duplicates:
            raise RuntimeError(
                "Douyin Vault schema v1 has duplicate active platform_uid values"
            )
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "DROP INDEX IF EXISTS douyin_authorizations_active_target"
            )
            connection.execute(
                """
                CREATE UNIQUE INDEX douyin_authorizations_active_target
                ON douyin_authorizations(platform_uid)
                WHERE status='active'
                """
            )
            connection.execute("PRAGMA user_version=2")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise

    @classmethod
    def _migrate_v2_to_v3(cls, connection: sqlite3.Connection) -> None:
        required_tables = {"oauth_states", "douyin_authorizations", "audit_events"}
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if not required_tables.issubset(tables):
            raise RuntimeError("Douyin Vault schema v2 is missing required tables")
        if cls._invalid_authorization_ids(connection):
            raise RuntimeError("Douyin Vault schema v2 has invalid authorization IDs")
        duplicates = cls._active_platform_uid_duplicates(connection)
        if duplicates:
            raise RuntimeError(
                "Douyin Vault schema v2 has duplicate active platform_uid values"
            )
        try:
            connection.executescript(
                """
                BEGIN IMMEDIATE;
                DROP INDEX IF EXISTS oauth_states_current;
                DROP INDEX IF EXISTS oauth_states_live_session;
                DROP INDEX IF EXISTS douyin_authorizations_active_target;

                ALTER TABLE oauth_states RENAME TO oauth_states_v2;
                ALTER TABLE douyin_authorizations RENAME TO douyin_authorizations_v2;

                CREATE TABLE oauth_states(
                    state_digest TEXT PRIMARY KEY,
                    bound_username TEXT NOT NULL,
                    session_binding TEXT NOT NULL,
                    account_id INTEGER,
                    platform_uid TEXT,
                    target_authorization_id TEXT,
                    target_authorization_version INTEGER,
                    requested_scopes_json TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN (
                        'created','exchanging','matching','completed','failed','expired'
                    )),
                    expires_at INTEGER NOT NULL,
                    candidate_ciphertext BLOB,
                    candidate_open_id_fingerprint TEXT,
                    failure_reason TEXT,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    CHECK(
                        (target_authorization_id IS NULL AND
                         target_authorization_version IS NULL) OR
                        (target_authorization_id IS NOT NULL AND
                         target_authorization_version IS NOT NULL AND
                         target_authorization_version>=1)
                    )
                );

                INSERT INTO oauth_states(
                    state_digest,bound_username,session_binding,account_id,
                    platform_uid,target_authorization_id,
                    target_authorization_version,requested_scopes_json,status,
                    expires_at,candidate_ciphertext,
                    candidate_open_id_fingerprint,failure_reason,created_at,updated_at
                )
                SELECT
                    state_digest,bound_username,session_binding,account_id,
                    platform_uid,NULL,NULL,requested_scopes_json,
                    CASE status
                        WHEN 'confirmed' THEN 'completed'
                        WHEN 'expired' THEN 'expired'
                        ELSE 'failed'
                    END,
                    expires_at,NULL,NULL,
                    CASE
                        WHEN status IN ('created','exchanging','pending_confirmation')
                            THEN 'schema_v3_migration'
                        WHEN status IN ('confirmed','expired')
                            THEN failure_reason
                        ELSE COALESCE(failure_reason,'schema_v3_migration')
                    END,
                    created_at,updated_at
                FROM oauth_states_v2;

                INSERT INTO audit_events(
                    occurred_at,actor,action,result,reason_code,
                    subject_fingerprint,request_id
                )
                SELECT
                    updated_at,bound_username,'oauth_state_migration','failed',
                    'schema_v3_migration',state_digest,'schema-v3-migration'
                FROM oauth_states_v2
                WHERE status IN ('created','exchanging','pending_confirmation');

                CREATE INDEX oauth_states_current
                    ON oauth_states(bound_username,session_binding,status,updated_at DESC);
                CREATE UNIQUE INDEX oauth_states_live_session
                    ON oauth_states(bound_username,session_binding)
                    WHERE status IN ('created','exchanging','matching');

                CREATE TABLE douyin_authorizations(
                    id TEXT PRIMARY KEY,
                    open_id_fingerprint TEXT NOT NULL UNIQUE,
                    bound_username TEXT NOT NULL,
                    account_id INTEGER,
                    platform_uid TEXT,
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
                    status TEXT NOT NULL CHECK(status IN (
                        'active','unbound','pending_match'
                    )),
                    match_reason TEXT CHECK(match_reason IS NULL OR match_reason IN (
                        'matching','no_match','ambiguous_match',
                        'account_binding_conflict','auto_match_unavailable',
                        'reauthorization_required'
                    )),
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    last_authorized_at INTEGER NOT NULL,
                    unbound_at INTEGER,
                    CHECK(
                        (account_id IS NULL AND platform_uid IS NULL) OR
                        (account_id IS NOT NULL AND platform_uid IS NOT NULL)
                    ),
                    CHECK(status!='active' OR (
                        account_id IS NOT NULL AND platform_uid IS NOT NULL AND
                        match_reason IS NULL
                    )),
                    CHECK(status!='pending_match' OR match_reason IS NOT NULL)
                );

                INSERT INTO douyin_authorizations(
                    id,open_id_fingerprint,bound_username,account_id,platform_uid,
                    access_token_ciphertext,refresh_token_ciphertext,
                    access_expires_at,refresh_expires_at,renew_count,scopes_json,
                    key_version,version,refresh_lease_owner,
                    refresh_lease_expires_at,needs_reauthorization,status,
                    match_reason,created_at,updated_at,last_authorized_at,unbound_at
                )
                SELECT
                    id,open_id_fingerprint,bound_username,account_id,platform_uid,
                    access_token_ciphertext,refresh_token_ciphertext,
                    access_expires_at,refresh_expires_at,renew_count,scopes_json,
                    key_version,version,refresh_lease_owner,
                    refresh_lease_expires_at,needs_reauthorization,status,
                    NULL,created_at,updated_at,last_authorized_at,unbound_at
                FROM douyin_authorizations_v2;

                CREATE UNIQUE INDEX douyin_authorizations_active_target
                    ON douyin_authorizations(platform_uid)
                    WHERE status='active';

                DROP TABLE oauth_states_v2;
                DROP TABLE douyin_authorizations_v2;
                PRAGMA user_version=3;
                """
            )
            cls._validate_schema_v3(connection)
            connection.commit()
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise

    @classmethod
    def _validate_schema_v3(cls, connection: sqlite3.Connection) -> None:
        if cls._invalid_authorization_ids(connection):
            raise RuntimeError("Douyin Vault schema v3 has invalid authorization IDs")
        duplicates = cls._active_platform_uid_duplicates(connection)
        if duplicates:
            raise RuntimeError("Douyin Vault schema v3 violates active UID uniqueness")
        expected_authorization_columns = [
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
            "match_reason",
            "created_at",
            "updated_at",
            "last_authorized_at",
            "unbound_at",
        ]
        authorization_columns = connection.execute(
            "PRAGMA table_info(douyin_authorizations)"
        ).fetchall()
        if [str(row[1]) for row in authorization_columns] != expected_authorization_columns:
            raise RuntimeError("Douyin Vault schema v3 authorization columns are invalid")
        nullability = {str(row[1]): int(row[3]) for row in authorization_columns}
        if nullability.get("account_id") != 0 or nullability.get("platform_uid") != 0:
            raise RuntimeError("Douyin Vault schema v3 targets must be nullable")
        authorization_sql_row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='douyin_authorizations'"
        ).fetchone()
        authorization_sql = (
            "" if authorization_sql_row is None else "".join(str(authorization_sql_row[0]).lower().split())
        )
        if "'active','unbound','pending_match'" not in authorization_sql:
            raise RuntimeError("Douyin Vault schema v3 authorization status is invalid")
        if (
            "'matching','no_match','ambiguous_match','account_binding_conflict',"
            "'auto_match_unavailable','reauthorization_required'"
            not in authorization_sql
        ):
            raise RuntimeError("Douyin Vault schema v3 match reasons are invalid")
        fingerprint_unique = False
        for row in connection.execute("PRAGMA index_list(douyin_authorizations)"):
            if int(row[2]) != 1:
                continue
            columns = [
                str(item[2])
                for item in connection.execute(f"PRAGMA index_info('{str(row[1])}')")
            ]
            if columns == ["open_id_fingerprint"]:
                fingerprint_unique = True
                break
        if not fingerprint_unique:
            raise RuntimeError("Douyin Vault schema v3 fingerprint uniqueness is missing")
        invalid_rows = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM douyin_authorizations
                WHERE (account_id IS NULL) != (platform_uid IS NULL)
                   OR (status='active' AND (
                        account_id IS NULL OR platform_uid IS NULL OR match_reason IS NOT NULL
                   ))
                   OR (status='pending_match' AND match_reason IS NULL)
                """
            ).fetchone()[0]
        )
        if invalid_rows:
            raise RuntimeError("Douyin Vault schema v3 authorization rows are invalid")
        index = connection.execute(
            """
            SELECT sql FROM sqlite_master
            WHERE type='index' AND name='douyin_authorizations_active_target'
            """
        ).fetchone()
        if index is None or index[0] is None:
            raise RuntimeError("Douyin Vault schema v3 active UID index is missing")
        normalized = "".join(str(index[0]).lower().split())
        if (
            "on douyin_authorizations(platform_uid)".replace(" ", "")
            not in normalized
            or "wherestatus='active'" not in normalized
        ):
            raise RuntimeError("Douyin Vault schema v3 active UID index is invalid")
        oauth_columns = connection.execute("PRAGMA table_info(oauth_states)").fetchall()
        expected_oauth_columns = [
            "state_digest",
            "bound_username",
            "session_binding",
            "account_id",
            "platform_uid",
            "target_authorization_id",
            "target_authorization_version",
            "requested_scopes_json",
            "status",
            "expires_at",
            "candidate_ciphertext",
            "candidate_open_id_fingerprint",
            "failure_reason",
            "created_at",
            "updated_at",
        ]
        if [str(row[1]) for row in oauth_columns] != expected_oauth_columns:
            raise RuntimeError("Douyin Vault schema v3 OAuth state columns are invalid")
        oauth_nullability = {str(row[1]): int(row[3]) for row in oauth_columns}
        if (
            oauth_nullability.get("account_id") != 0
            or oauth_nullability.get("platform_uid") != 0
            or oauth_nullability.get("target_authorization_id") != 0
            or oauth_nullability.get("target_authorization_version") != 0
        ):
            raise RuntimeError("Douyin Vault schema v3 OAuth targets must be nullable")
        oauth_sql_row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='oauth_states'"
        ).fetchone()
        oauth_sql = "" if oauth_sql_row is None else "".join(str(oauth_sql_row[0]).lower().split())
        if "'created','exchanging','matching','completed','failed','expired'" not in oauth_sql:
            raise RuntimeError("Douyin Vault schema v3 OAuth state status is invalid")
        invalid_oauth_targets = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM oauth_states
                WHERE (target_authorization_id IS NULL) !=
                      (target_authorization_version IS NULL)
                   OR target_authorization_version<1
                """
            ).fetchone()[0]
        )
        if invalid_oauth_targets:
            raise RuntimeError("Douyin Vault schema v3 OAuth targets are invalid")
        target_constraint = (
            "(target_authorization_idisnullandtarget_authorization_versionisnull)or"
            "(target_authorization_idisnotnullandtarget_authorization_versionisnotnulland"
            "target_authorization_version>=1)"
        )
        if target_constraint not in oauth_sql:
            raise RuntimeError("Douyin Vault schema v3 OAuth target constraint is invalid")
        live_index = connection.execute(
            """
            SELECT sql FROM sqlite_master
            WHERE type='index' AND name='oauth_states_live_session'
            """
        ).fetchone()
        live_index_sql = (
            "" if live_index is None or live_index[0] is None
            else "".join(str(live_index[0]).lower().split())
        )
        if (
            "onoauth_states(bound_username,session_binding)" not in live_index_sql
            or "wherestatusin('created','exchanging','matching')" not in live_index_sql
        ):
            raise RuntimeError("Douyin Vault schema v3 live OAuth index is invalid")

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
            WHERE status IN ('created','exchanging','matching')
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
        account_id: Optional[int] = None,
        platform_uid: Optional[str] = None,
        target_authorization_id: Optional[str] = None,
        target_authorization_version: Optional[int] = None,
        scopes: list[str],
        expires_at: int,
        request_id: str,
        now: Optional[int] = None,
    ) -> None:
        if (target_authorization_id is None) != (
            target_authorization_version is None
        ) or (
            target_authorization_version is not None
            and target_authorization_version < 1
        ):
            raise ValueError("invalid reauthorization target")
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
                  AND status IN ('created','exchanging','matching')
                """,
                (timestamp, bound_username, session_binding),
            )
            connection.execute(
                """
                INSERT INTO oauth_states(
                    state_digest,bound_username,session_binding,account_id,
                    platform_uid,target_authorization_id,
                    target_authorization_version,requested_scopes_json,status,expires_at,
                    created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,'created',?,?,?)
                """,
                (
                    state_digest,
                    bound_username,
                    session_binding,
                    account_id,
                    platform_uid,
                    target_authorization_id,
                    target_authorization_version,
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
                SET status='matching',candidate_ciphertext=?,
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
                result="matching",
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
                WHERE state_digest=? AND status NOT IN ('completed','expired')
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
                      AND status='matching' AND expires_at>?
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
                WHERE status IN ('created','exchanging','matching')
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
                  AND status='matching' AND expires_at>?
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
                WHERE platform_uid=? AND status='active'
                """,
                (platform_uid,),
            ).fetchone()
            if existing is not None and str(existing["platform_uid"]) != platform_uid:
                conflict_reason = "open_id_rebind_conflict"
            elif existing is not None and int(existing["account_id"]) != account_id:
                conflict_reason = "target_changed"
            elif existing is not None and not hmac.compare_digest(
                str(existing["bound_username"]), bound_username
            ):
                conflict_reason = "owner_conflict"
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
                            needs_reauthorization=0,status='active',match_reason=NULL,
                            updated_at=?,
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
                    SET status='completed',candidate_ciphertext=NULL,
                        candidate_open_id_fingerprint=NULL,updated_at=?
                    WHERE state_digest=?
                    """,
                    (timestamp, state_digest),
                )
                self._audit(
                    connection,
                    actor=bound_username,
                    action="oauth_confirm",
                    result="completed",
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

    def stage_authorization_candidate(
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
        """Persist exchanged tokens before any external account matching call."""
        timestamp = int(time.time()) if now is None else now
        conflict_reason: Optional[str] = None
        result: Optional[dict[str, Any]] = None
        with self.write_connection() as connection:
            self._expire_states(connection, timestamp)
            state = connection.execute(
                """
                SELECT * FROM oauth_states
                WHERE state_digest=? AND bound_username=? AND session_binding=?
                  AND status='exchanging' AND expires_at>?
                """,
                (state_digest, bound_username, session_binding, timestamp),
            ).fetchone()
            if state is None:
                raise StateTransitionError("oauth_exchange_unavailable")
            open_id = str(candidate["open_id"])
            if not hmac.compare_digest(
                cipher.open_id_fingerprint(open_id), open_id_fingerprint
            ):
                raise StateTransitionError("oauth_candidate_identity_changed")
            requested_scopes = json.loads(str(state["requested_scopes_json"]))
            candidate_scopes = candidate.get("scopes")
            if (
                not isinstance(requested_scopes, list)
                or any(not isinstance(scope, str) for scope in requested_scopes)
            ):
                raise RuntimeError("oauth state requested scopes are invalid")
            granted_scopes = (
                {str(scope) for scope in candidate_scopes}
                if isinstance(candidate_scopes, list)
                else set()
            )
            if not set(requested_scopes).issubset(granted_scopes):
                conflict_reason = "oauth_scope_incomplete"
            existing = connection.execute(
                "SELECT * FROM douyin_authorizations WHERE open_id_fingerprint=?",
                (open_id_fingerprint,),
            ).fetchone()
            target_authorization_id = state["target_authorization_id"]
            target_authorization_version = state["target_authorization_version"]
            if conflict_reason is None and target_authorization_id is not None:
                if existing is None or not hmac.compare_digest(
                    str(existing["id"]), str(target_authorization_id)
                ):
                    conflict_reason = "reauthorization_target_mismatch"
                elif int(existing["version"]) != int(target_authorization_version):
                    conflict_reason = "reauthorization_version_conflict"
            if conflict_reason is not None:
                connection.execute(
                    """
                    UPDATE oauth_states
                    SET status='failed',failure_reason=?,candidate_ciphertext=NULL,
                        candidate_open_id_fingerprint=NULL,updated_at=?
                    WHERE state_digest=?
                    """,
                    (conflict_reason, timestamp, state_digest),
                )
                self._audit(
                    connection,
                    actor=bound_username,
                    action="authorization_stage",
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
                scopes_json = json.dumps(
                    [str(scope) for scope in candidate["scopes"]],
                    separators=(",", ":"),
                )
                values = (
                    access_token_ciphertext,
                    refresh_token_ciphertext,
                    int(candidate["access_expires_at"]),
                    int(candidate["refresh_expires_at"]),
                    scopes_json,
                    cipher.key_version,
                )
                if existing is None:
                    connection.execute(
                        """
                        INSERT INTO douyin_authorizations(
                            id,open_id_fingerprint,bound_username,account_id,
                            platform_uid,access_token_ciphertext,
                            refresh_token_ciphertext,access_expires_at,
                            refresh_expires_at,renew_count,scopes_json,key_version,
                            version,refresh_lease_owner,refresh_lease_expires_at,
                            needs_reauthorization,status,match_reason,created_at,
                            updated_at,last_authorized_at,unbound_at
                        ) VALUES(?,?,?,NULL,NULL,?,?,?,?,0,?,?,1,NULL,NULL,0,
                                 'pending_match','matching',?,?,?,NULL)
                        """,
                        (
                            authorization_id,
                            open_id_fingerprint,
                            bound_username,
                            *values,
                            timestamp,
                            timestamp,
                            timestamp,
                        ),
                    )
                    version = 1
                    status = "pending_match"
                elif str(existing["status"]) == "active":
                    version = int(existing["version"]) + 1
                    connection.execute(
                        """
                        UPDATE douyin_authorizations
                        SET bound_username=?,access_token_ciphertext=?,
                            refresh_token_ciphertext=?,access_expires_at=?,
                            refresh_expires_at=?,renew_count=0,scopes_json=?,
                            key_version=?,version=?,refresh_lease_owner=NULL,
                            refresh_lease_expires_at=NULL,needs_reauthorization=0,
                            match_reason=NULL,updated_at=?,last_authorized_at=?,
                            unbound_at=NULL
                        WHERE id=? AND status='active'
                        """,
                        (
                            bound_username,
                            *values,
                            version,
                            timestamp,
                            timestamp,
                            authorization_id,
                        ),
                    )
                    status = "active"
                else:
                    version = int(existing["version"]) + 1
                    connection.execute(
                        """
                        UPDATE douyin_authorizations
                        SET bound_username=?,account_id=NULL,platform_uid=NULL,
                            access_token_ciphertext=?,refresh_token_ciphertext=?,
                            access_expires_at=?,refresh_expires_at=?,renew_count=0,
                            scopes_json=?,key_version=?,version=?,
                            refresh_lease_owner=NULL,refresh_lease_expires_at=NULL,
                            needs_reauthorization=0,status='pending_match',
                            match_reason='matching',updated_at=?,
                            last_authorized_at=?,unbound_at=NULL
                        WHERE id=? AND status IN ('pending_match','unbound')
                        """,
                        (
                            bound_username,
                            *values,
                            version,
                            timestamp,
                            timestamp,
                            authorization_id,
                        ),
                    )
                    status = "pending_match"
                state_status = "completed" if status == "active" else "matching"
                connection.execute(
                    """
                    UPDATE oauth_states
                    SET status=?,target_authorization_id=?,
                        target_authorization_version=?,candidate_ciphertext=NULL,
                        candidate_open_id_fingerprint=NULL,failure_reason=NULL,
                        updated_at=?
                    WHERE state_digest=?
                    """,
                    (
                        state_status,
                        authorization_id,
                        version,
                        timestamp,
                        state_digest,
                    ),
                )
                self._audit(
                    connection,
                    actor=bound_username,
                    action="authorization_stage",
                    result=status,
                    reason_code="reauthorized" if status == "active" else "matching",
                    subject_fingerprint=open_id_fingerprint,
                    request_id=request_id,
                    now=timestamp,
                )
                result = {
                    "id": authorization_id,
                    "status": status,
                    "version": version,
                    "account_id": existing["account_id"] if status == "active" else None,
                    "platform_uid": (
                        existing["platform_uid"] if status == "active" else None
                    ),
                    "already_active": status == "active",
                }
        if conflict_reason is not None:
            raise AuthorizationConflict(conflict_reason)
        if result is None:
            raise RuntimeError("authorization candidate staging produced no result")
        return result

    def finalize_auto_match(
        self,
        *,
        state_digest: str,
        authorization_id: str,
        expected_version: int,
        outcome: str,
        actor: str,
        request_id: str,
        account_id: Optional[int] = None,
        platform_uid: Optional[str] = None,
        now: Optional[int] = None,
    ) -> dict[str, Any]:
        self._validate_authorization_id(authorization_id)
        reasons = {
            "unmatched": "no_match",
            "ambiguous": "ambiguous_match",
            "unavailable": "auto_match_unavailable",
        }
        if outcome not in {"matched", *reasons}:
            raise ValueError("unsupported automatic match outcome")
        if outcome == "matched":
            if account_id is None or account_id <= 0 or not platform_uid:
                raise ValueError("matched outcome requires a target")
        elif account_id is not None or platform_uid is not None:
            raise ValueError("non-matched outcome cannot include a target")
        timestamp = int(time.time()) if now is None else now
        with self.write_connection() as connection:
            state = connection.execute(
                """
                SELECT status,target_authorization_id,target_authorization_version
                FROM oauth_states WHERE state_digest=?
                """,
                (state_digest,),
            ).fetchone()
            row = connection.execute(
                "SELECT * FROM douyin_authorizations WHERE id=?",
                (authorization_id,),
            ).fetchone()
            if (
                state is None
                or str(state["status"]) != "matching"
                or str(state["target_authorization_id"]) != authorization_id
                or int(state["target_authorization_version"]) != expected_version
                or row is None
                or str(row["status"]) != "pending_match"
                or int(row["version"]) != expected_version
            ):
                connection.execute(
                    """
                    UPDATE oauth_states
                    SET status='completed',failure_reason='superseded_by_newer_authorization',
                        updated_at=?
                    WHERE state_digest=? AND target_authorization_id=?
                      AND target_authorization_version=?
                    """,
                    (timestamp, state_digest, authorization_id, expected_version),
                )
                self._audit(
                    connection,
                    actor=actor,
                    action="authorization_auto_match",
                    result="stale",
                    reason_code="superseded_by_newer_authorization",
                    subject_fingerprint=(
                        str(row["open_id_fingerprint"])
                        if row is not None
                        else authorization_id
                    ),
                    request_id=request_id,
                    now=timestamp,
                )
                return {"id": authorization_id, "status": "stale"}

            status = "pending_match"
            reason: Optional[str]
            next_account_id: Optional[int] = None
            next_platform_uid: Optional[str] = None
            if outcome == "matched":
                occupied = connection.execute(
                    """
                    SELECT id FROM douyin_authorizations
                    WHERE platform_uid=? AND status='active' AND id<>?
                    """,
                    (platform_uid, authorization_id),
                ).fetchone()
                next_account_id = account_id
                next_platform_uid = platform_uid
                if occupied is None:
                    status = "active"
                    reason = None
                else:
                    reason = "account_binding_conflict"
            else:
                reason = reasons[outcome]
            next_version = expected_version + 1
            connection.execute(
                """
                UPDATE douyin_authorizations
                SET bound_username=?,account_id=?,platform_uid=?,status=?,
                    match_reason=?,version=?,updated_at=?
                WHERE id=? AND status='pending_match' AND version=?
                """,
                (
                    actor,
                    next_account_id,
                    next_platform_uid,
                    status,
                    reason,
                    next_version,
                    timestamp,
                    authorization_id,
                    expected_version,
                ),
            )
            connection.execute(
                """
                UPDATE oauth_states
                SET status='completed',failure_reason=NULL,updated_at=?
                WHERE state_digest=? AND target_authorization_id=?
                  AND target_authorization_version=?
                """,
                (timestamp, state_digest, authorization_id, expected_version),
            )
            self._audit(
                connection,
                actor=actor,
                action="authorization_auto_match",
                result=status,
                reason_code="ok" if reason is None else reason,
                subject_fingerprint=str(row["open_id_fingerprint"]),
                request_id=request_id,
                now=timestamp,
            )
            return {
                "id": authorization_id,
                "status": status,
                "match_reason": reason,
                "account_id": next_account_id,
                "platform_uid": next_platform_uid,
                "version": next_version,
            }

    def manual_match(
        self,
        *,
        authorization_id: str,
        account_id: int,
        platform_uid: str,
        expected_version: int,
        actor: str,
        request_id: str,
        now: Optional[int] = None,
    ) -> dict[str, Any]:
        self._validate_authorization_id(authorization_id)
        if account_id <= 0 or not platform_uid:
            raise ValueError("manual match requires a target")
        timestamp = int(time.time()) if now is None else now
        conflict = False
        result: Optional[dict[str, Any]] = None
        with self.write_connection() as connection:
            row = connection.execute(
                "SELECT * FROM douyin_authorizations WHERE id=?",
                (authorization_id,),
            ).fetchone()
            if row is None or str(row["status"]) != "pending_match":
                raise AuthorizationConflict("authorization_not_pending")
            if int(row["version"]) != expected_version:
                raise AuthorizationConflict("authorization_version_conflict")
            occupied = connection.execute(
                """
                SELECT id FROM douyin_authorizations
                WHERE platform_uid=? AND status='active' AND id<>?
                """,
                (platform_uid, authorization_id),
            ).fetchone()
            next_version = expected_version + 1
            if occupied is not None:
                conflict = True
                connection.execute(
                    """
                    UPDATE douyin_authorizations
                    SET bound_username=?,account_id=?,platform_uid=?,
                        match_reason='account_binding_conflict',version=?,updated_at=?
                    WHERE id=? AND status='pending_match' AND version=?
                    """,
                    (
                        actor,
                        account_id,
                        platform_uid,
                        next_version,
                        timestamp,
                        authorization_id,
                        expected_version,
                    ),
                )
            else:
                connection.execute(
                    """
                    UPDATE douyin_authorizations
                    SET bound_username=?,account_id=?,platform_uid=?,status='active',
                        match_reason=NULL,version=?,updated_at=?
                    WHERE id=? AND status='pending_match' AND version=?
                    """,
                    (
                        actor,
                        account_id,
                        platform_uid,
                        next_version,
                        timestamp,
                        authorization_id,
                        expected_version,
                    ),
                )
                result = {
                    "id": authorization_id,
                    "status": "active",
                    "account_id": account_id,
                    "platform_uid": platform_uid,
                    "version": next_version,
                }
            self._audit(
                connection,
                actor=actor,
                action="authorization_manual_match",
                result="pending_match" if conflict else "active",
                reason_code="account_binding_conflict" if conflict else "ok",
                subject_fingerprint=str(row["open_id_fingerprint"]),
                request_id=request_id,
                now=timestamp,
            )
        if conflict:
            raise AuthorizationConflict("account_binding_conflict")
        assert result is not None
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
                  AND status='matching' AND expires_at>?
                ORDER BY updated_at DESC LIMIT 1
                """,
                (bound_username, session_binding, timestamp),
            ).fetchone()
            if row is None:
                return False
            state_digest = str(row["state_digest"])
            connection.execute(
                """
                UPDATE oauth_states SET status='failed',candidate_ciphertext=NULL,
                    candidate_open_id_fingerprint=NULL,
                    failure_reason='operator_rejected',updated_at=?
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

    @staticmethod
    def _validate_authorization_id(authorization_id: str) -> None:
        if _AUTHORIZATION_ID_RE.fullmatch(authorization_id) is None:
            raise ValueError("authorization_id must be 32 lowercase hex characters")

    def get_authorization(self, authorization_id: str) -> Optional[dict[str, Any]]:
        """Return a browser-safe authorization projection without identity secrets."""
        self._validate_authorization_id(authorization_id)
        with self.read_connection() as connection:
            row = connection.execute(
                """
                SELECT id,bound_username,account_id,platform_uid,status,match_reason,
                       version,needs_reauthorization,access_expires_at,
                       refresh_expires_at,scopes_json,updated_at
                FROM douyin_authorizations WHERE id=?
                """,
                (authorization_id,),
            ).fetchone()
        item = self._row(row)
        if item is None:
            return None
        item["scopes"] = json.loads(str(item.pop("scopes_json")))
        item["needs_reauthorization"] = bool(item["needs_reauthorization"])
        return item

    def get_active_authorization(
        self, authorization_id: str
    ) -> Optional[dict[str, Any]]:
        self._validate_authorization_id(authorization_id)
        with self.read_connection() as connection:
            row = connection.execute(
                """
                SELECT * FROM douyin_authorizations
                WHERE id=? AND status='active'
                """,
                (authorization_id,),
            ).fetchone()
        item = self._row(row)
        if item is None:
            return None
        item["scopes"] = json.loads(str(item.pop("scopes_json")))
        item["needs_reauthorization"] = bool(item["needs_reauthorization"])
        return item

    def acquire_refresh_lease(
        self,
        authorization_id: str,
        *,
        lease_owner: str,
        lease_seconds: int,
        actor: str,
        request_id: str,
        now: Optional[int] = None,
    ) -> bool:
        self._validate_authorization_id(authorization_id)
        if not lease_owner or lease_seconds <= 0:
            raise ValueError("refresh lease requires an owner and positive duration")
        timestamp = int(time.time()) if now is None else now
        with self.write_connection() as connection:
            row = connection.execute(
                """
                SELECT open_id_fingerprint FROM douyin_authorizations WHERE id=?
                """,
                (authorization_id,),
            ).fetchone()
            cursor = connection.execute(
                """
                UPDATE douyin_authorizations
                SET refresh_lease_owner=?,refresh_lease_expires_at=?,updated_at=?
                WHERE id=? AND status='active' AND needs_reauthorization=0
                  AND (refresh_lease_owner IS NULL
                       OR refresh_lease_expires_at IS NULL
                       OR refresh_lease_expires_at<=?
                       OR refresh_lease_owner=?)
                """,
                (
                    lease_owner,
                    timestamp + lease_seconds,
                    timestamp,
                    authorization_id,
                    timestamp,
                    lease_owner,
                ),
            )
            acquired = cursor.rowcount == 1
            self._audit(
                connection,
                actor=actor,
                action="token_refresh_lease_acquire",
                result="acquired" if acquired else "rejected",
                reason_code="ok" if acquired else "lease_unavailable",
                subject_fingerprint=(
                    str(row["open_id_fingerprint"])
                    if row is not None
                    else authorization_id
                ),
                request_id=request_id,
                now=timestamp,
            )
            return acquired

    def release_refresh_lease(
        self,
        authorization_id: str,
        *,
        lease_owner: str,
        actor: str,
        request_id: str,
        now: Optional[int] = None,
    ) -> bool:
        self._validate_authorization_id(authorization_id)
        if not lease_owner:
            raise ValueError("refresh lease owner is required")
        timestamp = int(time.time()) if now is None else now
        with self.write_connection() as connection:
            row = connection.execute(
                """
                SELECT open_id_fingerprint FROM douyin_authorizations WHERE id=?
                """,
                (authorization_id,),
            ).fetchone()
            cursor = connection.execute(
                """
                UPDATE douyin_authorizations
                SET refresh_lease_owner=NULL,refresh_lease_expires_at=NULL,
                    updated_at=?
                WHERE id=? AND refresh_lease_owner=?
                """,
                (timestamp, authorization_id, lease_owner),
            )
            released = cursor.rowcount == 1
            self._audit(
                connection,
                actor=actor,
                action="token_refresh_lease_release",
                result="released" if released else "rejected",
                reason_code="ok" if released else "lease_not_owned",
                subject_fingerprint=(
                    str(row["open_id_fingerprint"])
                    if row is not None
                    else authorization_id
                ),
                request_id=request_id,
                now=timestamp,
            )
            return released

    def update_access_token(
        self,
        authorization_id: str,
        *,
        lease_owner: str,
        access_token_ciphertext: bytes,
        access_expires_at: int,
        key_version: int,
        actor: str,
        request_id: str,
        now: Optional[int] = None,
    ) -> bool:
        self._validate_authorization_id(authorization_id)
        if not lease_owner or not access_token_ciphertext or key_version < 1:
            raise ValueError("invalid access token update")
        timestamp = int(time.time()) if now is None else now
        if access_expires_at <= timestamp:
            raise ValueError("access token expiry must be in the future")
        with self.write_connection() as connection:
            row = connection.execute(
                """
                SELECT open_id_fingerprint FROM douyin_authorizations WHERE id=?
                """,
                (authorization_id,),
            ).fetchone()
            cursor = connection.execute(
                """
                UPDATE douyin_authorizations
                SET access_token_ciphertext=?,access_expires_at=?,key_version=?,
                    updated_at=?
                WHERE id=? AND status='active' AND needs_reauthorization=0
                  AND refresh_lease_owner=? AND refresh_lease_expires_at>?
                """,
                (
                    access_token_ciphertext,
                    access_expires_at,
                    key_version,
                    timestamp,
                    authorization_id,
                    lease_owner,
                    timestamp,
                ),
            )
            updated = cursor.rowcount == 1
            self._audit(
                connection,
                actor=actor,
                action="access_token_update",
                result="updated" if updated else "rejected",
                reason_code="ok" if updated else "refresh_lease_required",
                subject_fingerprint=(
                    str(row["open_id_fingerprint"])
                    if row is not None
                    else authorization_id
                ),
                request_id=request_id,
                now=timestamp,
            )
            return updated

    def update_refreshed_token_bundle(
        self,
        authorization_id: str,
        *,
        lease_owner: str,
        access_token_ciphertext: bytes,
        refresh_token_ciphertext: bytes,
        access_expires_at: int,
        refresh_expires_at: int,
        key_version: int,
        actor: str,
        request_id: str,
        now: Optional[int] = None,
    ) -> bool:
        self._validate_authorization_id(authorization_id)
        if (
            not lease_owner
            or not access_token_ciphertext
            or not refresh_token_ciphertext
            or key_version < 1
        ):
            raise ValueError("invalid refreshed token bundle")
        timestamp = int(time.time()) if now is None else now
        if access_expires_at <= timestamp or refresh_expires_at <= timestamp:
            raise ValueError("refreshed token expiry must be in the future")
        with self.write_connection() as connection:
            row = connection.execute(
                """
                SELECT open_id_fingerprint FROM douyin_authorizations WHERE id=?
                """,
                (authorization_id,),
            ).fetchone()
            cursor = connection.execute(
                """
                UPDATE douyin_authorizations
                SET access_token_ciphertext=?,refresh_token_ciphertext=?,
                    access_expires_at=?,refresh_expires_at=?,key_version=?,
                    updated_at=?
                WHERE id=? AND status='active' AND needs_reauthorization=0
                  AND refresh_lease_owner=? AND refresh_lease_expires_at>?
                """,
                (
                    access_token_ciphertext,
                    refresh_token_ciphertext,
                    access_expires_at,
                    refresh_expires_at,
                    key_version,
                    timestamp,
                    authorization_id,
                    lease_owner,
                    timestamp,
                ),
            )
            updated = cursor.rowcount == 1
            self._audit(
                connection,
                actor=actor,
                action="token_bundle_refresh",
                result="updated" if updated else "rejected",
                reason_code="ok" if updated else "refresh_lease_required",
                subject_fingerprint=(
                    str(row["open_id_fingerprint"])
                    if row is not None
                    else authorization_id
                ),
                request_id=request_id,
                now=timestamp,
            )
            return updated

    def rotate_authorization_tokens(
        self,
        authorization_id: str,
        *,
        lease_owner: str,
        access_token_ciphertext: bytes,
        refresh_token_ciphertext: bytes,
        key_version: int,
        actor: str,
        request_id: str,
        now: Optional[int] = None,
    ) -> bool:
        self._validate_authorization_id(authorization_id)
        if (
            not lease_owner
            or not access_token_ciphertext
            or not refresh_token_ciphertext
            or key_version < 1
        ):
            raise ValueError("invalid token ciphertext rotation")
        timestamp = int(time.time()) if now is None else now
        with self.write_connection() as connection:
            row = connection.execute(
                """
                SELECT open_id_fingerprint FROM douyin_authorizations WHERE id=?
                """,
                (authorization_id,),
            ).fetchone()
            cursor = connection.execute(
                """
                UPDATE douyin_authorizations
                SET access_token_ciphertext=?,refresh_token_ciphertext=?,
                    key_version=?,updated_at=?
                WHERE id=? AND status='active' AND needs_reauthorization=0
                  AND refresh_lease_owner=? AND refresh_lease_expires_at>?
                """,
                (
                    access_token_ciphertext,
                    refresh_token_ciphertext,
                    key_version,
                    timestamp,
                    authorization_id,
                    lease_owner,
                    timestamp,
                ),
            )
            updated = cursor.rowcount == 1
            self._audit(
                connection,
                actor=actor,
                action="token_ciphertext_rotate",
                result="updated" if updated else "rejected",
                reason_code="ok" if updated else "refresh_lease_required",
                subject_fingerprint=(
                    str(row["open_id_fingerprint"])
                    if row is not None
                    else authorization_id
                ),
                request_id=request_id,
                now=timestamp,
            )
            return updated

    def renew_refresh_token(
        self,
        authorization_id: str,
        *,
        lease_owner: str,
        refresh_token_ciphertext: bytes,
        refresh_expires_at: int,
        key_version: int,
        actor: str,
        request_id: str,
        now: Optional[int] = None,
    ) -> bool:
        self._validate_authorization_id(authorization_id)
        if not lease_owner or not refresh_token_ciphertext or key_version < 1:
            raise ValueError("invalid refresh token renewal")
        timestamp = int(time.time()) if now is None else now
        if refresh_expires_at <= timestamp:
            raise ValueError("refresh token expiry must be in the future")
        with self.write_connection() as connection:
            row = connection.execute(
                """
                SELECT open_id_fingerprint,renew_count
                FROM douyin_authorizations WHERE id=?
                """,
                (authorization_id,),
            ).fetchone()
            cursor = connection.execute(
                """
                UPDATE douyin_authorizations
                SET refresh_token_ciphertext=?,refresh_expires_at=?,
                    renew_count=renew_count+1,key_version=?,updated_at=?
                WHERE id=? AND status='active' AND needs_reauthorization=0
                  AND renew_count<5
                  AND refresh_lease_owner=? AND refresh_lease_expires_at>?
                """,
                (
                    refresh_token_ciphertext,
                    refresh_expires_at,
                    key_version,
                    timestamp,
                    authorization_id,
                    lease_owner,
                    timestamp,
                ),
            )
            updated = cursor.rowcount == 1
            reason_code = "ok"
            if not updated:
                reason_code = (
                    "renew_limit_reached"
                    if row is not None and int(row["renew_count"]) >= 5
                    else "refresh_lease_required"
                )
            self._audit(
                connection,
                actor=actor,
                action="refresh_token_renew",
                result="updated" if updated else "rejected",
                reason_code=reason_code,
                subject_fingerprint=(
                    str(row["open_id_fingerprint"])
                    if row is not None
                    else authorization_id
                ),
                request_id=request_id,
                now=timestamp,
            )
            return updated

    def mark_needs_reauthorization(
        self,
        authorization_id: str,
        *,
        actor: str,
        reason_code: str,
        request_id: str,
        now: Optional[int] = None,
    ) -> bool:
        self._validate_authorization_id(authorization_id)
        if not reason_code:
            raise ValueError("reauthorization reason is required")
        timestamp = int(time.time()) if now is None else now
        with self.write_connection() as connection:
            row = connection.execute(
                """
                SELECT open_id_fingerprint FROM douyin_authorizations WHERE id=?
                """,
                (authorization_id,),
            ).fetchone()
            cursor = connection.execute(
                """
                UPDATE douyin_authorizations
                SET needs_reauthorization=1,refresh_lease_owner=NULL,
                    refresh_lease_expires_at=NULL,updated_at=?
                WHERE id=? AND status='active' AND needs_reauthorization=0
                """,
                (timestamp, authorization_id),
            )
            updated = cursor.rowcount == 1
            self._audit(
                connection,
                actor=actor,
                action="authorization_reauthorization_required",
                result="marked" if updated else "unchanged",
                reason_code=reason_code,
                subject_fingerprint=(
                    str(row["open_id_fingerprint"])
                    if row is not None
                    else authorization_id
                ),
                request_id=request_id,
                now=timestamp,
            )
            return updated

    def list_active_authorizations(self) -> list[dict[str, Any]]:
        """Return the machine-facing authorization projection without secrets."""
        with self.read_connection() as connection:
            rows = connection.execute(
                """
                SELECT id,account_id,platform_uid,access_expires_at,
                       refresh_expires_at,renew_count,scopes_json,
                       needs_reauthorization,updated_at
                FROM douyin_authorizations
                WHERE status='active'
                ORDER BY account_id,id
                """
            ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["scopes"] = json.loads(str(item.pop("scopes_json")))
            item["needs_reauthorization"] = bool(item["needs_reauthorization"])
            items.append(item)
        return items

    def list_authorizations(
        self, _bound_username: Optional[str] = None
    ) -> list[dict[str, Any]]:
        with self.read_connection() as connection:
            rows = connection.execute(
                """
                SELECT id,bound_username,account_id,platform_uid,access_expires_at,
                       refresh_expires_at,renew_count,scopes_json,
                       version,needs_reauthorization,status,match_reason,updated_at
                FROM douyin_authorizations
                ORDER BY updated_at DESC,id
                """
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

    def authorization_statuses(self, *, now: Optional[int] = None) -> list[dict[str, Any]]:
        timestamp = int(time.time()) if now is None else now
        with self.read_connection() as connection:
            rows = connection.execute(
                """
                SELECT id,account_id,platform_uid,status,match_reason,scopes_json,
                       refresh_expires_at,needs_reauthorization,
                       access_token_ciphertext IS NOT NULL AS has_access_token,
                       refresh_token_ciphertext IS NOT NULL AS has_refresh_token,
                       updated_at
                FROM douyin_authorizations
                ORDER BY updated_at DESC,id
                """
            ).fetchall()
        items: list[dict[str, Any]] = []
        selected_targets: dict[tuple[int, str], tuple[int, dict[str, Any]]] = {}
        required_scopes = {"user_info", "video.list"}
        for row in rows:
            item = dict(row)
            scopes = {str(scope) for scope in json.loads(str(item.pop("scopes_json")))}
            has_access_token = bool(item.pop("has_access_token"))
            has_refresh_token = bool(item.pop("has_refresh_token"))
            item["authorized"] = bool(
                item["status"] == "active"
                and not bool(item["needs_reauthorization"])
                and has_access_token
                and has_refresh_token
                and item["refresh_expires_at"] is not None
                and int(item["refresh_expires_at"]) > timestamp
                and required_scopes.issubset(scopes)
            )
            item["scopes"] = sorted(scopes)
            item["needs_reauthorization"] = bool(item["needs_reauthorization"])
            account_id = item["account_id"]
            platform_uid = item["platform_uid"]
            if account_id is None or platform_uid is None:
                items.append(item)
                continue
            target = (int(account_id), str(platform_uid))
            priority = (
                3
                if bool(item["authorized"])
                else 2
                if item["status"] == "active"
                else 1
            )
            selected = selected_targets.get(target)
            if selected is None or priority > selected[0]:
                selected_targets[target] = (priority, item)
        items.extend(item for _priority, item in selected_targets.values())
        items.sort(
            key=lambda item: (int(item["updated_at"]), str(item["id"])),
            reverse=True,
        )
        return items

    def unbind(
        self,
        *,
        actor: str,
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
                WHERE id=? AND status='active'
                """,
                (authorization_id,),
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
                    match_reason=NULL,unbound_at=?,version=version+1 WHERE id=?
                """,
                (timestamp, timestamp, str(row["id"])),
            )
            self._audit(
                connection,
                actor=actor,
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
