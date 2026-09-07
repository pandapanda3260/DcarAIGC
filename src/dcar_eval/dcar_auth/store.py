"""Account, session, challenge and throttle storage for the Dcar auth gateway.

All state lives in one SQLite file (historically ``sessions.sqlite3``).  Every
write goes through a short ``BEGIN IMMEDIATE`` transaction so concurrent
``asyncio.to_thread`` callers queue on the SQLite write lock instead of racing.
Expensive password hashing never happens inside a transaction.

Schema 2 adds the account role (``superadmin`` / ``admin`` / ``operator``), the
``auth_deleted_users`` tombstone table and the privileged user-management
transactions that re-verify the caller's session inside the write lock.  Every
security-relevant change is appended to ``auth-changes.log`` next to the
database (outside of it, so a restored backup can be reconciled against it).
Schema 3 adds ``new_user``, the default for self-registration, with no business
permissions. Existing account roles are preserved during the table rebuild.
"""

from __future__ import annotations

import fcntl
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import sqlite3
import stat
import threading
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, NoReturn, Optional, Sequence

from passlib.hash import sha512_crypt  # type: ignore[import-untyped]


LOGGER = logging.getLogger("dcar-auth.store")
SCHEMA_VERSION = 3
ACCEPTED_SCHEMA_VERSIONS = frozenset({SCHEMA_VERSION})
REQUIRED_TABLES = frozenset(
    {
        "auth_sessions",
        "auth_users",
        "auth_allowed_phones",
        "auth_challenges",
        "auth_failures",
        "auth_deleted_users",
    }
)
ROLE_SUPERADMIN = "superadmin"
ROLE_ADMIN = "admin"
ROLE_OPERATOR = "operator"
ROLE_NEW_USER = "new_user"
ROLES = (ROLE_SUPERADMIN, ROLE_ADMIN, ROLE_OPERATOR, ROLE_NEW_USER)
ROLE_RANK = {ROLE_SUPERADMIN: 3, ROLE_ADMIN: 2, ROLE_OPERATOR: 1, ROLE_NEW_USER: 0}
# Roles allowed to open the user-management page and its endpoints.
USER_ADMIN_ROLES = frozenset({ROLE_SUPERADMIN, ROLE_ADMIN})
STATUS_ACTIVE = "active"
STATUS_DISABLED = "disabled"
CHANGE_LOG_FILENAME = "auth-changes.log"
CHANGE_LOG_SCHEMA = "dcar-auth-change-v2"
# The audit event format is unchanged; schema-2 evidence remains appendable.
CHANGE_LOG_USER_VERSIONS = frozenset({2, 3})
CHANGE_LOG_MAX_LINE_BYTES = 16 * 1024
PASSWORD_ROUNDS = 100_000
HTPASSWD_MIN_ROUNDS = 5_000
HTPASSWD_MAX_ROUNDS = 1_000_000
PASSWORD_MIN_LENGTH = 8
PASSWORD_MAX_LENGTH = 64
CODE_TTL_SECONDS = 300
CODE_MAX_ATTEMPTS = 5
TICKET_TTL_SECONDS = 600
CHALLENGE_RETENTION_SECONDS = 24 * 60 * 60
PHONE_SEND_LIMITS: tuple[tuple[int, int], ...] = ((60, 1), (3600, 5), (86400, 10))
IP_SEND_LIMITS: tuple[tuple[int, int], ...] = ((3600, 10), (86400, 30))
GLOBAL_SEND_WINDOW_SECONDS = 86400
PURPOSES = ("register", "login", "reset")
# Challenge rows are never deleted by account operations (the 24h send-rate
# ledger must survive them); they are marked with one of these reasons instead.
INVALIDATION_REASONS = frozenset(
    {"password_changed", "phone_changed", "user_disabled", "user_deleted", "revoked"}
)
RESERVED_USERNAMES = frozenset({"temporary-bypass"})
USERNAME_PATTERN = re.compile(r"[A-Za-z0-9_]{4,32}")
LEGACY_USERNAME_PATTERN = re.compile(r"[\x21-\x39\x3b-\x7e]{1,128}")
PHONE_PATTERN = re.compile(r"1[3-9][0-9]{9}", re.ASCII)
CODE_PATTERN = re.compile(r"[0-9]{6}", re.ASCII)
COMMON_PASSWORDS_PATH = Path(__file__).with_name("common_passwords.txt")
SHA512_CRYPT_PATTERN = re.compile(
    r"\$6\$(?:rounds=([1-9][0-9]{3,8})\$)?"
    r"([./0-9A-Za-z]{1,16})\$([./0-9A-Za-z]{86})\Z"
)
AUDIT_ACTIONS = frozenset(
    {
        "phone.allow",
        "phone.remove",
        "sessions.revoke",
        "challenges.revoke",
        "user.create",
        "user.delete",
        "user.disable",
        "user.enable",
        "user.import",
        "user.register",
        "user.reset_password",
        "user.set_password",
        "user.set_phone",
        "user.set_role",
        "user.update",
    }
)
AUDIT_FIELD_NAMES = frozenset(
    {"count", "note", "password", "phone", "role", "status"}
)
AUDIT_AFTER_NAMES = frozenset(
    {"allowed", "count", "deleted", "password_changed", "phone", "role", "status"}
)
_CHANGE_ID_PATTERN = re.compile(r"[0-9a-f]{32}\Z")

_SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS auth_sessions(
        token_sha256 TEXT PRIMARY KEY,
        username TEXT NOT NULL,
        credential_fingerprint TEXT NOT NULL DEFAULT '',
        created_at INTEGER NOT NULL,
        expires_at INTEGER NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS auth_sessions_expiry ON auth_sessions(expires_at)",
    "CREATE INDEX IF NOT EXISTS auth_sessions_username ON auth_sessions(username)",
    """
    CREATE TABLE IF NOT EXISTS auth_users(
        username TEXT PRIMARY KEY COLLATE NOCASE,
        phone TEXT UNIQUE,
        password_hash TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'active'
            CHECK(status IN ('active','disabled')),
        role TEXT NOT NULL DEFAULT 'new_user'
            CHECK(role IN ('superadmin','admin','operator','new_user')),
        created_at INTEGER NOT NULL,
        password_updated_at INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS auth_allowed_phones(
        phone TEXT PRIMARY KEY,
        note TEXT NOT NULL DEFAULT '',
        added_at INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS auth_challenges(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        purpose TEXT NOT NULL CHECK(purpose IN ('register','login','reset')),
        phone TEXT NOT NULL,
        username TEXT,
        client_ip TEXT NOT NULL,
        status TEXT NOT NULL
            CHECK(status IN ('pending','sent','rejected','unknown')),
        provider_code TEXT NOT NULL DEFAULT '',
        code_hmac TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        created_at INTEGER NOT NULL,
        expires_at INTEGER NOT NULL,
        consumed_at INTEGER,
        ticket_hmac TEXT,
        ticket_expires_at INTEGER,
        ticket_used_at INTEGER,
        invalidated_at INTEGER,
        invalidated_reason TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS auth_challenges_phone "
    "ON auth_challenges(phone, purpose, status, id DESC)",
    "CREATE INDEX IF NOT EXISTS auth_challenges_ip "
    "ON auth_challenges(client_ip, created_at)",
    "CREATE INDEX IF NOT EXISTS auth_challenges_username "
    "ON auth_challenges(username)",
    "CREATE UNIQUE INDEX IF NOT EXISTS auth_challenges_ticket "
    "ON auth_challenges(ticket_hmac) WHERE ticket_hmac IS NOT NULL",
    """
    CREATE TABLE IF NOT EXISTS auth_failures(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        key TEXT NOT NULL,
        at INTEGER NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS auth_failures_key ON auth_failures(key, at)",
    # Tombstones: a deleted username can never be registered or imported again,
    # and the deletion survives a rollback of the code.
    """
    CREATE TABLE IF NOT EXISTS auth_deleted_users(
        username TEXT PRIMARY KEY COLLATE NOCASE,
        deleted_at INTEGER NOT NULL,
        deleted_by TEXT NOT NULL,
        role TEXT NOT NULL,
        phone TEXT
    )
    """,
)

# Idempotent column additions for databases created before a column existed
# (the schema-0 session store, schema-1 files written before R1, and schema-2
# files written before the tombstone kept the phone number).
_COLUMN_UPGRADES: tuple[tuple[str, str, str], ...] = (
    ("auth_sessions", "credential_fingerprint", "TEXT NOT NULL DEFAULT ''"),
    (
        "auth_users",
        "role",
        "TEXT NOT NULL DEFAULT 'operator' "
        "CHECK(role IN ('superadmin','admin','operator'))",
    ),
    ("auth_challenges", "username", "TEXT"),
    ("auth_challenges", "invalidated_at", "INTEGER"),
    ("auth_challenges", "invalidated_reason", "TEXT"),
    ("auth_deleted_users", "phone", "TEXT"),
)


class AuthStoreError(Exception):
    """Base class for expected authentication outcomes.

    ``code`` is the stable snake_case identifier the gateway returns to the
    user-management page; login-flow errors keep their own mapping.
    """

    code = "auth_store_error"


class RateLimited(AuthStoreError):
    code = "rate_limited"

    def __init__(self, retry_after: int) -> None:
        super().__init__(f"retry after {retry_after}s")
        self.retry_after = max(1, int(retry_after))


class AccountDisabled(AuthStoreError):
    code = "account_disabled"


class PhoneNotAllowed(AuthStoreError):
    code = "phone_not_allowed"


class PhoneNotRegistered(AuthStoreError):
    code = "phone_not_registered"


class PhoneRegistered(AuthStoreError):
    code = "phone_registered"


class UsernameTaken(AuthStoreError):
    code = "username_taken"


class InvalidCode(AuthStoreError):
    code = "invalid_code"


class ResetExpired(AuthStoreError):
    code = "reset_expired"


class UserNotFound(AuthStoreError):
    code = "user_not_found"


class PhoneConflict(AuthStoreError):
    code = "phone_conflict"


class HtpasswdImportError(AuthStoreError):
    code = "import_rejected"


class HtpasswdExportError(AuthStoreError):
    code = "export_rejected"


class ChangeLogError(RuntimeError):
    """The durable security-change journal cannot be trusted or appended."""


class SessionRevoked(AuthStoreError):
    """The caller's session no longer verifies inside the write transaction."""

    code = "session_revoked"


class ActorForbidden(AuthStoreError):
    """The caller's role (re-read inside the transaction) may not manage users."""

    code = "forbidden"


class TargetForbidden(AuthStoreError):
    """The target outranks the caller."""

    code = "target_forbidden"


class RoleForbidden(AuthStoreError):
    """The requested role outranks the caller."""

    code = "role_forbidden"


class SelfRoleChange(AuthStoreError):
    code = "self_role_change"


class SelfPasswordChange(AuthStoreError):
    code = "self_password_change"


class SelfDelete(AuthStoreError):
    code = "self_delete"


class LastSuperadmin(AuthStoreError):
    """At least one active superadmin must remain."""

    code = "last_superadmin"


class InvalidRole(AuthStoreError):
    code = "role_invalid"


class SchemaVersionError(RuntimeError):
    pass


@dataclass(frozen=True)
class Principal:
    """A resolved session: ``role`` is None only for the bypass pseudo-user."""

    username: str
    role: Optional[str]
    token_sha256: str


@dataclass(frozen=True)
class UserRecord:
    username: str
    phone: Optional[str]
    password_hash: str
    status: str
    role: str
    created_at: int
    password_updated_at: int

    def to_public(self) -> dict[str, object]:
        """The user-management page representation: no hash, ISO-8601 times."""
        data = asdict(self)
        del data["password_hash"]
        data["created_at"] = _iso(self.created_at)
        data["password_updated_at"] = _iso(self.password_updated_at)
        return data


@dataclass(frozen=True)
class UserSummary:
    username: str
    phone: Optional[str]
    status: str
    role: str
    created_at: int
    password_updated_at: int
    active_sessions: int

    def to_public(self) -> dict[str, object]:
        """The user-management list row (session counts stay CLI-only)."""
        return {
            "username": self.username,
            "phone": self.phone,
            "role": self.role,
            "status": self.status,
            "created_at": _iso(self.created_at),
            "password_updated_at": _iso(self.password_updated_at),
        }


def _iso(timestamp: int) -> str:
    return datetime.fromtimestamp(int(timestamp), tz=timezone.utc).isoformat()


def hash_password(password: str) -> str:
    return sha512_crypt.using(rounds=PASSWORD_ROUNDS).hash(password)


def verify_password(password: str, stored_hash: str) -> bool:
    try:
        return bool(sha512_crypt.verify(password, stored_hash))
    except (TypeError, ValueError):
        return False


_DUMMY_HASH_LOCK = threading.Lock()
_DUMMY_HASH: Optional[str] = None


def dummy_hash() -> str:
    """A real 100000-round hash used to equalise timing for unknown accounts."""
    global _DUMMY_HASH
    if _DUMMY_HASH is None:
        with _DUMMY_HASH_LOCK:
            if _DUMMY_HASH is None:
                _DUMMY_HASH = hash_password(secrets.token_hex(16))
    return _DUMMY_HASH


def credential_fingerprint(password_hash: str) -> str:
    return hashlib.sha256(password_hash.encode("utf-8")).hexdigest()


def session_token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def code_hmac(pepper: bytes, purpose: str, phone: str, code: str) -> str:
    message = f"code|{purpose}|{phone}|{code}".encode("utf-8")
    return hmac.new(pepper, message, hashlib.sha256).hexdigest()


def ticket_hmac(pepper: bytes, token: str) -> str:
    message = f"reset-ticket|{token}".encode("utf-8")
    return hmac.new(pepper, message, hashlib.sha256).hexdigest()


def generate_code() -> str:
    return f"{secrets.randbelow(10**6):06d}"


def generate_token() -> str:
    return secrets.token_urlsafe(32)


def load_pepper(path: Path) -> bytes:
    value = path.read_text(encoding="utf-8").strip()
    if len(value) < 32 or any(ord(character) < 33 for character in value):
        raise RuntimeError("authentication pepper has an invalid format")
    return value.encode("utf-8")


def valid_username(value: str) -> bool:
    return USERNAME_PATTERN.fullmatch(value) is not None


def valid_phone(value: str) -> bool:
    return PHONE_PATTERN.fullmatch(value) is not None


def valid_code(value: str) -> bool:
    return CODE_PATTERN.fullmatch(value) is not None


_COMMON_PASSWORDS_LOCK = threading.Lock()
_COMMON_PASSWORDS: Optional[frozenset[str]] = None


def common_passwords() -> frozenset[str]:
    global _COMMON_PASSWORDS
    if _COMMON_PASSWORDS is None:
        with _COMMON_PASSWORDS_LOCK:
            if _COMMON_PASSWORDS is None:
                try:
                    entries = {
                        line.strip().lower()
                        for line in COMMON_PASSWORDS_PATH.read_text(
                            encoding="utf-8"
                        ).splitlines()
                        if line.strip()
                    }
                except (OSError, UnicodeError) as exc:
                    raise RuntimeError(
                        f"common-password policy file is unavailable: {COMMON_PASSWORDS_PATH}"
                    ) from exc
                if not entries:
                    raise RuntimeError(
                        f"common-password policy file is empty: {COMMON_PASSWORDS_PATH}"
                    )
                _COMMON_PASSWORDS = frozenset(entries)
    return _COMMON_PASSWORDS


def password_problem(
    password: str, *, username: str = "", phone: str = ""
) -> Optional[str]:
    """Return an error code for an unacceptable password, or None."""
    if (
        not PASSWORD_MIN_LENGTH <= len(password) <= PASSWORD_MAX_LENGTH
        or "\x00" in password
    ):
        return "invalid_password"
    lowered = password.lower()
    if lowered in common_passwords():
        return "password_too_common"
    if username and username.lower() in lowered:
        return "password_too_common"
    if phone and phone in password:
        return "password_too_common"
    return None


def _sha512_crypt_rounds(password_hash: str) -> int:
    """Validate one complete, canonical and bounded SHA-512 crypt hash."""
    match = SHA512_CRYPT_PATTERN.fullmatch(password_hash)
    if match is None:
        raise ValueError("malformed SHA-512 crypt hash")
    rounds = int(match.group(1) or 5_000)
    if not HTPASSWD_MIN_ROUNDS <= rounds <= HTPASSWD_MAX_ROUNDS:
        raise ValueError(
            f"SHA-512 crypt rounds must be {HTPASSWD_MIN_ROUNDS}..{HTPASSWD_MAX_ROUNDS}"
        )
    try:
        parsed = sha512_crypt.from_string(password_hash)
    except (TypeError, ValueError) as exc:
        raise ValueError("malformed SHA-512 crypt hash") from exc
    if parsed.checksum is None or parsed.rounds != rounds or parsed.to_string() != password_hash:
        raise ValueError("non-canonical SHA-512 crypt hash")
    return rounds


def parse_htpasswd(text: str) -> list[tuple[str, str]]:
    """Strictly parse ``username:hash`` lines; any defect rejects the file."""
    entries: list[tuple[str, str]] = []
    seen: set[str] = set()
    for number, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        # The pre-migration verifier ignored commented accounts.  Preserve that
        # revocation semantic: a line beginning with '#' can never become a user.
        if line.startswith("#"):
            continue
        if not line:
            raise HtpasswdImportError(f"line {number}: empty line")
        if line != raw_line:
            raise HtpasswdImportError(f"line {number}: surrounding whitespace")
        if line.count(":") != 1:
            raise HtpasswdImportError(f"line {number}: expected username:hash")
        username, password_hash = line.split(":", 1)
        if LEGACY_USERNAME_PATTERN.fullmatch(username) is None:
            raise HtpasswdImportError(f"line {number}: invalid username")
        if username.lower() in RESERVED_USERNAMES:
            raise HtpasswdImportError(f"line {number}: reserved username")
        try:
            _sha512_crypt_rounds(password_hash)
        except ValueError as exc:
            raise HtpasswdImportError(
                f"line {number}: invalid SHA-512 crypt ($6$) hash: {exc}"
            ) from exc
        if username.lower() in seen:
            raise HtpasswdImportError(f"line {number}: duplicate username")
        seen.add(username.lower())
        entries.append((username, password_hash))
    if not entries:
        raise HtpasswdImportError("account file is empty")
    return entries


class AuthStore:
    """SQLite-backed accounts, sessions, challenges and failure throttling."""

    def __init__(
        self,
        path: Path,
        *,
        pepper: bytes = b"",
        throttle_window_seconds: int = 600,
        throttle_max_failures: int = 8,
        sms_daily_cap: int = 300,
        change_log_path: Optional[Path] = None,
    ) -> None:
        self.path = Path(path)
        self.pepper = pepper
        self.throttle_window_seconds = int(throttle_window_seconds)
        self.throttle_max_failures = int(throttle_max_failures)
        self.sms_daily_cap = int(sms_daily_cap)
        self.change_log_path = (
            Path(change_log_path)
            if change_log_path is not None
            else self.path.parent / CHANGE_LOG_FILENAME
        )

    # ------------------------------------------------------------------ setup

    def initialize(self) -> tuple[int, int]:
        """Create or upgrade the schema in place; returns (before, after).

        Versions 0 through 2 are upgraded to 3 inside one write transaction.
        The role constraint is rebuilt while existing account rows, sessions,
        challenge ledgers and audit files are preserved. Newer versions refuse.
        """
        # A missing password-policy artifact must stop the service instead of
        # silently disabling the blocklist.
        common_passwords()
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            self.path.parent.chmod(0o700)
        except OSError:
            pass
        connection = self._connect()
        try:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version > SCHEMA_VERSION:
                raise SchemaVersionError(
                    f"unsupported authentication schema version {version}"
                )
            journal_mode = str(
                connection.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
            ).lower()
            if journal_mode != "delete":
                raise RuntimeError(
                    f"authentication database requires DELETE journal mode, got {journal_mode}"
                )
            connection.execute("BEGIN IMMEDIATE")
            try:
                # Another migrator may have completed while this one waited.
                version = int(connection.execute("PRAGMA user_version").fetchone()[0])
                if version > SCHEMA_VERSION:
                    raise SchemaVersionError(
                        f"unsupported authentication schema version {version}"
                    )
                self._apply_schema(connection)
                if version < 3 and not self._has_new_user_role_schema(connection):
                    self._upgrade_user_roles(connection)
                if version < SCHEMA_VERSION:
                    connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
            connection.execute("COMMIT")
        finally:
            connection.close()
        try:
            self.path.chmod(0o600)
        except OSError:
            pass
        return version, SCHEMA_VERSION

    @staticmethod
    def _apply_schema(connection: sqlite3.Connection) -> None:
        for statement in _SCHEMA_STATEMENTS:
            connection.execute(statement)
        for table, column, definition in _COLUMN_UPGRADES:
            columns = {
                str(row["name"])
                for row in connection.execute(f"PRAGMA table_info({table})")
            }
            if column not in columns:
                connection.execute(
                    f"ALTER TABLE {table} ADD COLUMN {column} {definition}"
                )

    @staticmethod
    def _has_new_user_role_schema(connection: sqlite3.Connection) -> bool:
        role = next(
            (row for row in connection.execute("PRAGMA table_info(auth_users)")
             if row["name"] == "role"),
            None,
        )
        table = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='auth_users'"
        ).fetchone()
        sql = re.sub(r"\s+", "", str(table["sql"] or "").lower()) if table else ""
        return (
            role is not None
            and bool(role["notnull"])
            and role["dflt_value"] == "'new_user'"
            and "check(rolein('superadmin','admin','operator','new_user'))" in sql
        )

    @staticmethod
    def _upgrade_user_roles(connection: sqlite3.Connection) -> None:
        # SQLite cannot change a CHECK constraint in place. Rebuild only the
        # account table, keeping its name so session/ledger references survive.
        columns = (
            "username", "phone", "password_hash", "status", "role",
            "created_at", "password_updated_at",
        )
        existing = {row["name"] for row in connection.execute("PRAGMA table_info(auth_users)")}
        if existing != set(columns):
            raise SchemaVersionError("cannot migrate an unexpected auth_users column layout")
        auxiliary_sql = [
            str(row["sql"])
            for row in connection.execute(
                "SELECT sql FROM sqlite_master WHERE tbl_name='auth_users' "
                "AND type IN ('index','trigger') AND sql IS NOT NULL ORDER BY type, name"
            )
        ]
        statement = next(
            sql for sql in _SCHEMA_STATEMENTS
            if "CREATE TABLE IF NOT EXISTS auth_users(" in sql
        )
        connection.execute(statement.replace(
            "CREATE TABLE IF NOT EXISTS auth_users(", "CREATE TABLE auth_users_v3("
        ))
        names = "rowid, " + ", ".join(columns)
        connection.execute(f"INSERT INTO auth_users_v3({names}) SELECT {names} FROM auth_users")
        connection.execute("DROP TABLE auth_users")
        connection.execute("ALTER TABLE auth_users_v3 RENAME TO auth_users")
        for sql in auxiliary_sql:
            connection.execute(sql)
        if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise SchemaVersionError("account migration failed foreign-key validation")

    def healthcheck(self) -> None:
        # A health probe must not recreate a missing database (including when
        # it disappears between the caller's file check and this connection).
        connection = self._connect(must_exist=True)
        try:
            connection.execute("BEGIN IMMEDIATE")
            try:
                version = int(
                    connection.execute("PRAGMA user_version").fetchone()[0]
                )
                tables = {
                    str(row["name"])
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                journal_mode = str(
                    connection.execute("PRAGMA journal_mode").fetchone()[0]
                ).lower()
                role_schema_ready = self._has_new_user_role_schema(connection)
            finally:
                connection.execute("ROLLBACK")
        finally:
            connection.close()
        if version not in ACCEPTED_SCHEMA_VERSIONS:
            raise SchemaVersionError(
                f"unsupported authentication schema version {version}"
            )
        if not REQUIRED_TABLES.issubset(tables):
            raise RuntimeError("authentication schema is missing required tables")
        if not role_schema_ready:
            raise RuntimeError("authentication schema is missing the new_user role/default")
        if journal_mode != "delete":
            raise RuntimeError(
                f"authentication database requires DELETE journal mode, got {journal_mode}"
            )

    def _connect(self, *, must_exist: bool = False) -> sqlite3.Connection:
        connection = sqlite3.connect(
            f"{self.path.resolve().as_uri()}?mode=rw" if must_exist else self.path,
            uri=must_exist,
            timeout=10,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=10000")
        return connection

    @contextmanager
    def _immediate(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
            except BaseException:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise
            if connection.in_transaction:
                connection.execute("COMMIT")
        finally:
            connection.close()

    @staticmethod
    def _now() -> int:
        return int(time.time())

    # --------------------------------------------------------------- failures

    @staticmethod
    def user_key(username: str) -> str:
        return f"user:{username.lower()}"

    @staticmethod
    def phone_key(phone: str) -> str:
        return f"phone:{phone}"

    @staticmethod
    def ip_key(client_ip: str) -> str:
        return f"ip:{client_ip}"

    def _retry_after(
        self, connection: sqlite3.Connection, keys: Sequence[str], now: int
    ) -> int:
        window = self.throttle_window_seconds
        for key in keys:
            stamps = [
                int(row["at"])
                for row in connection.execute(
                    "SELECT at FROM auth_failures WHERE key=? AND at>? ORDER BY at",
                    (key, now - window),
                )
            ]
            if len(stamps) >= self.throttle_max_failures:
                return max(1, window - (now - stamps[0]) + 1)
        return 0

    def _record_failure(
        self, connection: sqlite3.Connection, keys: Sequence[str], now: int
    ) -> None:
        connection.execute(
            "DELETE FROM auth_failures WHERE at<=?",
            (now - self.throttle_window_seconds,),
        )
        connection.executemany(
            "INSERT INTO auth_failures(key, at) VALUES(?, ?)",
            [(key, now) for key in keys],
        )

    @staticmethod
    def _clear_failures(connection: sqlite3.Connection, key: str) -> None:
        connection.execute("DELETE FROM auth_failures WHERE key=?", (key,))

    def _reject(
        self,
        connection: sqlite3.Connection,
        keys: Sequence[str],
        now: int,
        error: AuthStoreError,
    ) -> NoReturn:
        """Commit a failure record, then raise ``error``."""
        self._record_failure(connection, keys, now)
        connection.execute("COMMIT")
        raise error

    def failure_count(self, key: str) -> int:
        connection = self._connect()
        try:
            return int(
                connection.execute(
                    "SELECT COUNT(*) FROM auth_failures WHERE key=?", (key,)
                ).fetchone()[0]
            )
        finally:
            connection.close()

    # ------------------------------------------------------------------ users

    @staticmethod
    def _user_from_row(row: sqlite3.Row) -> UserRecord:
        return UserRecord(
            username=str(row["username"]),
            phone=(str(row["phone"]) if row["phone"] is not None else None),
            password_hash=str(row["password_hash"]),
            status=str(row["status"]),
            role=str(row["role"]),
            created_at=int(row["created_at"]),
            password_updated_at=int(row["password_updated_at"]),
        )

    @staticmethod
    def _select_user(
        connection: sqlite3.Connection, username: str
    ) -> Optional[sqlite3.Row]:
        return connection.execute(
            "SELECT username, phone, password_hash, status, role, created_at, "
            "password_updated_at FROM auth_users WHERE username=?",
            (username,),
        ).fetchone()

    @staticmethod
    def _select_user_by_phone(
        connection: sqlite3.Connection, phone: str
    ) -> Optional[sqlite3.Row]:
        return connection.execute(
            "SELECT username, phone, password_hash, status, role, created_at, "
            "password_updated_at FROM auth_users WHERE phone=?",
            (phone,),
        ).fetchone()

    @staticmethod
    def _active_superadmins(connection: sqlite3.Connection) -> int:
        return int(
            connection.execute(
                "SELECT COUNT(*) FROM auth_users WHERE role=? AND status=?",
                (ROLE_SUPERADMIN, STATUS_ACTIVE),
            ).fetchone()[0]
        )

    def count_superadmins(self) -> int:
        connection = self._connect()
        try:
            return self._active_superadmins(connection)
        finally:
            connection.close()

    def get_user(self, username: str) -> Optional[UserRecord]:
        connection = self._connect()
        try:
            row = self._select_user(connection, username)
        finally:
            connection.close()
        return self._user_from_row(row) if row is not None else None

    def get_user_by_phone(self, phone: str) -> Optional[UserRecord]:
        connection = self._connect()
        try:
            row = self._select_user_by_phone(connection, phone)
        finally:
            connection.close()
        return self._user_from_row(row) if row is not None else None

    def user_count(self) -> int:
        connection = self._connect()
        try:
            return int(
                connection.execute("SELECT COUNT(*) FROM auth_users").fetchone()[0]
            )
        finally:
            connection.close()

    @staticmethod
    def _table_exists(connection: sqlite3.Connection, name: str) -> bool:
        return (
            connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (name,),
            ).fetchone()
            is not None
        )

    def _username_reserved(
        self, connection: sqlite3.Connection, username: str
    ) -> bool:
        if username.lower() in RESERVED_USERNAMES:
            return True
        if self._select_user(connection, username) is not None:
            return True
        if self._table_exists(connection, "auth_deleted_users"):
            row = connection.execute(
                "SELECT 1 FROM auth_deleted_users WHERE username=? COLLATE NOCASE",
                (username,),
            ).fetchone()
            if row is not None:
                return True
        return False

    def username_reserved(self, username: str) -> bool:
        connection = self._connect()
        try:
            return self._username_reserved(connection, username)
        finally:
            connection.close()

    @staticmethod
    def _phone_allowed(connection: sqlite3.Connection, phone: str) -> bool:
        return (
            connection.execute(
                "SELECT 1 FROM auth_allowed_phones WHERE phone=?", (phone,)
            ).fetchone()
            is not None
        )

    def list_users(self) -> list[UserSummary]:
        """Every account, newest registration first (the page order)."""
        connection = self._connect()
        try:
            now = self._now()
            rows = connection.execute(
                """
                SELECT u.username, u.phone, u.status, u.role, u.created_at,
                       u.password_updated_at,
                       (SELECT COUNT(*) FROM auth_sessions s
                        WHERE s.username=u.username AND s.expires_at>?) AS sessions
                FROM auth_users u ORDER BY u.created_at DESC, u.rowid DESC
                """,
                (now,),
            ).fetchall()
        finally:
            connection.close()
        return [
            UserSummary(
                username=str(row["username"]),
                phone=(str(row["phone"]) if row["phone"] is not None else None),
                status=str(row["status"]),
                role=str(row["role"]),
                created_at=int(row["created_at"]),
                password_updated_at=int(row["password_updated_at"]),
                active_sessions=int(row["sessions"]),
            )
            for row in rows
        ]

    def list_allowed_phones(self) -> list[dict[str, object]]:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT phone, note, added_at FROM auth_allowed_phones "
                "ORDER BY added_at, phone"
            ).fetchall()
        finally:
            connection.close()
        return [
            {
                "phone": str(row["phone"]),
                "note": str(row["note"]),
                "added_at": _iso(int(row["added_at"])),
            }
            for row in rows
        ]

    def create_user(
        self,
        username: str,
        password_hash: str,
        *,
        phone: Optional[str] = None,
        role: str = ROLE_OPERATOR,
        status: str = STATUS_ACTIVE,
        actor: str = "cli",
    ) -> UserRecord:
        """Provision an account with an explicit offline operator default.

        Public registration uses ``complete_register`` and always starts with
        ``new_user``. Keep this trusted provisioning helper compatible with
        existing integrations and test/account bootstrap callers.
        """
        """Insert an account directly (tests and tooling; the page registers)."""
        if role not in ROLE_RANK:
            raise InvalidRole(role)
        if status not in {STATUS_ACTIVE, STATUS_DISABLED}:
            raise ValueError("status must be active or disabled")
        now = self._now()
        change_id: Optional[str] = None
        with self._immediate() as connection:
            if self._username_reserved(connection, username):
                raise UsernameTaken(username)
            if phone and self._select_user_by_phone(connection, phone) is not None:
                raise PhoneConflict(phone)
            connection.execute(
                "INSERT INTO auth_users(username, phone, password_hash, status, role, "
                "created_at, password_updated_at) VALUES(?,?,?,?,?,?,?)",
                (username, phone, password_hash, status, role, now, now),
            )
            row = self._select_user(connection, username)
            change_id = self._record_change_intent(
                "user.create",
                actor,
                username,
                ("role", "status", "phone"),
                phone=phone,
                after={"role": role, "status": status, "phone": phone},
            )
        assert change_id is not None
        self._record_change_commit(change_id)
        assert row is not None
        return self._user_from_row(row)

    def allow_phone(self, phone: str, note: str = "", *, actor: str = "cli") -> None:
        change_id: Optional[str] = None
        with self._immediate() as connection:
            connection.execute(
                "INSERT INTO auth_allowed_phones(phone, note, added_at) "
                "VALUES(?, ?, ?) ON CONFLICT(phone) DO UPDATE SET note=excluded.note",
                (phone, note, self._now()),
            )
            change_id = self._record_change_intent(
                "phone.allow",
                actor,
                phone,
                ("note",),
                phone=phone,
                after={"allowed": True, "phone": phone},
            )
        assert change_id is not None
        self._record_change_commit(change_id)

    def remove_allowed_phone(self, phone: str, *, actor: str = "cli") -> bool:
        change_id: Optional[str] = None
        with self._immediate() as connection:
            cursor = connection.execute(
                "DELETE FROM auth_allowed_phones WHERE phone=?", (phone,)
            )
            removed = cursor.rowcount == 1
            if removed:
                change_id = self._record_change_intent(
                    "phone.remove",
                    actor,
                    phone,
                    (),
                    phone=phone,
                    after={"allowed": False, "phone": phone},
                )
        if change_id is not None:
            self._record_change_commit(change_id)
        return removed

    def phone_allowed(self, phone: str) -> bool:
        connection = self._connect()
        try:
            return self._phone_allowed(connection, phone)
        finally:
            connection.close()

    @staticmethod
    def _invalidate_challenges(
        connection: sqlite3.Connection,
        *,
        username: Optional[str],
        phone: Optional[str],
        reason: str,
        now: int,
    ) -> int:
        """Retire live challenge rows without deleting them.

        Rows bound to ``username`` (any purpose) and registration rows for
        ``phone`` stop being valid for codes and tickets, but they stay in the
        table so the 24h send-rate ledger and the daily SMS cap are unaffected;
        only the retention sweep in ``reserve_send`` deletes rows.  With both
        arguments ``None`` every live row is retired.
        """
        if reason not in INVALIDATION_REASONS:
            raise ValueError("invalid invalidation reason")
        scopes: list[str] = []
        params: list[object] = [now, reason]
        if username is not None:
            scopes.append("username=? COLLATE NOCASE")
            params.append(username)
        if phone is not None:
            scopes.append("(phone=? AND purpose='register')")
            params.append(phone)
        clause = f" AND ({' OR '.join(scopes)})" if scopes else ""
        cursor = connection.execute(
            "UPDATE auth_challenges SET invalidated_at=?, invalidated_reason=? "
            f"WHERE invalidated_at IS NULL{clause}",
            params,
        )
        return int(cursor.rowcount)

    def set_phone(self, username: str, phone: str, *, actor: str = "cli") -> None:
        change_id: Optional[str] = None
        with self._immediate() as connection:
            now = self._now()
            row = self._select_user(connection, username)
            if row is None:
                raise UserNotFound(username)
            canonical = str(row["username"])
            previous = None if row["phone"] is None else str(row["phone"])
            conflict = self._select_user_by_phone(connection, phone)
            if conflict is not None and str(conflict["username"]) != canonical:
                raise PhoneConflict(phone)
            if previous == phone:
                return
            connection.execute(
                "UPDATE auth_users SET phone=? WHERE username=?", (phone, canonical)
            )
            if previous is not None:
                # The old number loses its admission: registering with it again
                # needs an explicit allow-phone.
                connection.execute(
                    "DELETE FROM auth_allowed_phones WHERE phone=?", (previous,)
                )
            self._invalidate_challenges(
                connection,
                username=canonical,
                phone=previous,
                reason="phone_changed",
                now=now,
            )
            change_id = self._record_change_intent(
                "user.set_phone",
                actor,
                canonical,
                ("phone",),
                phone=phone,
                after={"phone": phone},
            )
        assert change_id is not None
        self._record_change_commit(change_id)

    def set_password(
        self, username: str, password_hash: str, *, actor: str = "cli"
    ) -> None:
        change_id: Optional[str] = None
        with self._immediate() as connection:
            now = self._now()
            row = self._select_user(connection, username)
            if row is None:
                raise UserNotFound(username)
            canonical = str(row["username"])
            connection.execute(
                "UPDATE auth_users SET password_hash=?, password_updated_at=? "
                "WHERE username=?",
                (password_hash, now, canonical),
            )
            connection.execute(
                "DELETE FROM auth_sessions WHERE username=?", (canonical,)
            )
            self._invalidate_challenges(
                connection,
                username=canonical,
                phone=None if row["phone"] is None else str(row["phone"]),
                reason="password_changed",
                now=now,
            )
            change_id = self._record_change_intent(
                "user.set_password",
                actor,
                canonical,
                ("password",),
                after={"password_changed": True},
            )
        assert change_id is not None
        self._record_change_commit(change_id)

    def set_status(self, username: str, status: str, *, actor: str = "cli") -> None:
        if status not in {STATUS_ACTIVE, STATUS_DISABLED}:
            raise ValueError("status must be active or disabled")
        change_id: Optional[str] = None
        with self._immediate() as connection:
            now = self._now()
            row = self._select_user(connection, username)
            if row is None:
                raise UserNotFound(username)
            canonical = str(row["username"])
            connection.execute(
                "UPDATE auth_users SET status=? WHERE username=?",
                (status, canonical),
            )
            if status == STATUS_DISABLED:
                connection.execute(
                    "DELETE FROM auth_sessions WHERE username=?", (canonical,)
                )
                self._invalidate_challenges(
                    connection,
                    username=canonical,
                    phone=None if row["phone"] is None else str(row["phone"]),
                    reason="user_disabled",
                    now=now,
                )
            change_id = self._record_change_intent(
                "user.disable" if status == STATUS_DISABLED else "user.enable",
                actor,
                canonical,
                ("status",),
                after={"status": status},
            )
        assert change_id is not None
        self._record_change_commit(change_id)

    def revoke_sessions(
        self, username: Optional[str] = None, *, actor: str = "cli"
    ) -> int:
        change_id: Optional[str] = None
        with self._immediate() as connection:
            if username is None:
                cursor = connection.execute("DELETE FROM auth_sessions")
            else:
                cursor = connection.execute(
                    "DELETE FROM auth_sessions WHERE username=? COLLATE NOCASE",
                    (username,),
                )
            count = int(cursor.rowcount)
            change_id = self._record_change_intent(
                "sessions.revoke",
                actor,
                username or "*",
                ("count",),
                after={"count": count},
            )
        assert change_id is not None
        self._record_change_commit(change_id)
        return count

    def revoke_challenges(
        self, username: Optional[str] = None, *, actor: str = "cli"
    ) -> int:
        """Retire live codes and reset tickets; returns the number of rows retired."""
        change_id: Optional[str] = None
        with self._immediate() as connection:
            now = self._now()
            if username is None:
                count = self._invalidate_challenges(
                    connection, username=None, phone=None, reason="revoked", now=now
                )
            else:
                row = self._select_user(connection, username)
                phone = None
                if row is not None:
                    username = str(row["username"])
                    phone = None if row["phone"] is None else str(row["phone"])
                count = self._invalidate_challenges(
                    connection, username=username, phone=phone, reason="revoked", now=now
                )
            change_id = self._record_change_intent(
                "challenges.revoke",
                actor,
                username or "*",
                ("count",),
                after={"count": count},
            )
        assert change_id is not None
        self._record_change_commit(change_id)
        return count

    # --------------------------------------------------------- htpasswd bridge

    @staticmethod
    def parse_htpasswd(text: str) -> list[tuple[str, str]]:
        return parse_htpasswd(text)

    def import_htpasswd(
        self, source: Path, *, actor: str = "cli", bootstrap_superadmin: bool = False
    ) -> int:
        """Import active accounts; any defect rejects the whole transaction.

        Only an empty account table accepts an import, and a username that is
        reserved, present or tombstoned rejects the whole file.  Roles are
        assigned afterwards with ``set_role`` unless the offline caller explicitly
        bootstraps the first parsed account in this same import transaction.
        """
        entries = parse_htpasswd(Path(source).read_text(encoding="utf-8"))
        now = self._now()
        change_ids: list[str] = []
        with self._immediate() as connection:
            count = int(
                connection.execute("SELECT COUNT(*) FROM auth_users").fetchone()[0]
            )
            if count:
                raise HtpasswdImportError("account table is not empty")
            for username, password_hash in entries:
                if self._username_reserved(connection, username):
                    raise HtpasswdImportError(f"username reserved: {username}")
                connection.execute(
                    "INSERT INTO auth_users(username, phone, password_hash, status, "
                    "role, created_at, password_updated_at) "
                    "VALUES(?, NULL, ?, 'active', 'operator', ?, ?)",
                    (username, password_hash, now, now),
                )
            after: dict[str, object] = {
                "count": len(entries),
                "role": ROLE_OPERATOR,
                "status": STATUS_ACTIVE,
            }
            if bootstrap_superadmin:
                first_username = entries[0][0]
                connection.execute(
                    "UPDATE auth_users SET role=? WHERE username=?",
                    (ROLE_SUPERADMIN, first_username),
                )
            change_ids.append(self._record_change_intent(
                "user.import",
                actor,
                "*",
                ("count", "role", "status"),
                after=after,
            ))
            if bootstrap_superadmin:
                change_ids.append(self._record_change_intent(
                    "user.set_role", actor, entries[0][0], ("role",),
                    after={"role": ROLE_SUPERADMIN},
                ))
        for change_id in change_ids:
            self._record_change_commit(change_id)
        return len(entries)

    def export_htpasswd(self, output: Path, *, allow_empty: bool = False) -> int:
        # A legacy gateway grants business access to every exported identity.
        # Exclude unapproved users so a rollback cannot silently authorize them.
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT username, password_hash FROM auth_users "
                "WHERE status='active' AND role IN ('superadmin','admin','operator') "
                "ORDER BY created_at, username"
            ).fetchall()
        finally:
            connection.close()
        lines = []
        for row in rows:
            password_hash = str(row["password_hash"])
            try:
                _sha512_crypt_rounds(password_hash)
            except ValueError as exc:
                raise HtpasswdExportError(
                    f"account {row['username']} has an invalid SHA-512 crypt hash: {exc}"
                ) from exc
            lines.append(f"{row['username']}:{password_hash}\n")
        if not lines and not allow_empty:
            raise HtpasswdExportError(
                "no active authorized accounts; refusing an empty rollback export "
                "without --allow-empty"
            )
        output = Path(output)
        output.parent.mkdir(parents=True, exist_ok=True)
        partial = output.with_name(f".{output.name}.{os.getpid()}.partial")
        descriptor = os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                handle.write("".join(lines))
                handle.flush()
                os.fsync(handle.fileno())
        except BaseException:
            partial.unlink(missing_ok=True)
            raise
        os.replace(partial, output)
        directory = os.open(output.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return len(lines)

    # --------------------------------------------------------------- sessions

    def _create_session(
        self,
        connection: sqlite3.Connection,
        username: str,
        fingerprint: str,
        ttl_seconds: int,
        now: int,
    ) -> str:
        token = generate_token()
        connection.execute("DELETE FROM auth_sessions WHERE expires_at<=?", (now,))
        connection.execute(
            "INSERT INTO auth_sessions(token_sha256, username, "
            "credential_fingerprint, created_at, expires_at) VALUES(?,?,?,?,?)",
            (session_token_hash(token), username, fingerprint, now, now + ttl_seconds),
        )
        return token

    def create_session(self, username: str, ttl_seconds: int) -> str:
        with self._immediate() as connection:
            row = self._select_user(connection, username)
            if row is None:
                raise UserNotFound(username)
            return self._create_session(
                connection,
                str(row["username"]),
                credential_fingerprint(str(row["password_hash"])),
                ttl_seconds,
                self._now(),
            )

    def resolve_principal(self, token: str) -> Optional[Principal]:
        """Read-only session lookup; the role is read fresh on every request."""
        if not token or len(token) > 256:
            return None
        token_hash = session_token_hash(token)
        now = self._now()
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT u.username, s.credential_fingerprint, s.expires_at,
                       u.password_hash, u.status, u.role
                FROM auth_sessions s JOIN auth_users u ON u.username=s.username
                WHERE s.token_sha256=?
                """,
                (token_hash,),
            ).fetchone()
            if row is None:
                # Session rows without a matching account (or expired orphans).
                connection.execute(
                    "DELETE FROM auth_sessions WHERE token_sha256=?", (token_hash,)
                )
                return None
            valid = (
                int(row["expires_at"]) > now
                and str(row["status"]) == STATUS_ACTIVE
                and hmac.compare_digest(
                    str(row["credential_fingerprint"]),
                    credential_fingerprint(str(row["password_hash"])),
                )
            )
            if not valid:
                connection.execute(
                    "DELETE FROM auth_sessions WHERE token_sha256=?", (token_hash,)
                )
                return None
            return Principal(
                username=str(row["username"]),
                role=str(row["role"]),
                token_sha256=token_hash,
            )
        finally:
            connection.close()

    def resolve_session(self, token: str) -> Optional[str]:
        principal = self.resolve_principal(token)
        return principal.username if principal is not None else None

    def revoke_session(self, token: str) -> None:
        if not token or len(token) > 256:
            return
        with self._immediate() as connection:
            connection.execute(
                "DELETE FROM auth_sessions WHERE token_sha256=?",
                (session_token_hash(token),),
            )

    # --------------------------------------------------------- password login

    def begin_password_login(self, username: str, client_ip: str) -> Optional[str]:
        """Throttle check plus account lookup; returns the stored hash or None."""
        keys = (self.user_key(username), self.ip_key(client_ip))
        with self._immediate() as connection:
            now = self._now()
            retry_after = self._retry_after(connection, keys, now)
            if retry_after:
                raise RateLimited(retry_after)
            row = self._select_user(connection, username)
            if row is None:
                return None
            if str(row["status"]) != "active":
                self._reject(connection, keys, now, AccountDisabled(username))
            return str(row["password_hash"])

    def record_login_failure(self, username: str, client_ip: str) -> None:
        keys = (self.user_key(username), self.ip_key(client_ip))
        with self._immediate() as connection:
            self._record_failure(connection, keys, self._now())

    def finish_password_login(
        self, username: str, verified_hash: str, ttl_seconds: int
    ) -> Optional[tuple[str, str]]:
        """Create a session only if the verified hash is still current."""
        with self._immediate() as connection:
            row = self._select_user(connection, username)
            if (
                row is None
                or not hmac.compare_digest(str(row["password_hash"]), verified_hash)
                or str(row["status"]) != "active"
            ):
                return None
            now = self._now()
            canonical = str(row["username"])
            token = self._create_session(
                connection,
                canonical,
                credential_fingerprint(str(row["password_hash"])),
                ttl_seconds,
                now,
            )
            self._clear_failures(connection, self.user_key(username))
            return token, canonical

    # ------------------------------------------------------------- challenges

    def reserve_send(
        self, purpose: str, phone: str, client_ip: str, code: str
    ) -> tuple[int, Optional[str]]:
        if purpose not in PURPOSES:
            raise ValueError("invalid purpose")
        keys = (self.phone_key(phone), self.ip_key(client_ip))
        with self._immediate() as connection:
            now = self._now()
            retry_after = self._retry_after(connection, keys, now)
            if retry_after:
                raise RateLimited(retry_after)
            connection.execute(
                "DELETE FROM auth_challenges WHERE created_at<?",
                (now - CHALLENGE_RETENTION_SECONDS,),
            )
            for window, limit in PHONE_SEND_LIMITS:
                self._check_send_window(
                    connection, "phone", phone, window, limit, now
                )
            for window, limit in IP_SEND_LIMITS:
                self._check_send_window(
                    connection, "client_ip", client_ip, window, limit, now
                )
            self._check_send_window(
                connection, None, None, GLOBAL_SEND_WINDOW_SECONDS,
                self.sms_daily_cap, now,
            )
            username: Optional[str] = None
            if purpose == "register":
                if self._select_user_by_phone(connection, phone) is not None:
                    self._reject(connection, keys, now, PhoneRegistered(phone))
                if not self._phone_allowed(connection, phone):
                    self._reject(connection, keys, now, PhoneNotAllowed(phone))
            else:
                row = self._select_user_by_phone(connection, phone)
                if row is None:
                    self._reject(connection, keys, now, PhoneNotRegistered(phone))
                if str(row["status"]) != "active":
                    self._reject(connection, keys, now, AccountDisabled(phone))
                username = str(row["username"])
            cursor = connection.execute(
                "INSERT INTO auth_challenges(purpose, phone, username, client_ip, "
                "status, code_hmac, created_at, expires_at) "
                "VALUES(?, ?, ?, ?, 'pending', ?, ?, ?)",
                (
                    purpose,
                    phone,
                    username,
                    client_ip,
                    code_hmac(self.pepper, purpose, phone, code),
                    now,
                    now + CODE_TTL_SECONDS,
                ),
            )
            assert cursor.lastrowid is not None
            return int(cursor.lastrowid), username

    @staticmethod
    def _check_send_window(
        connection: sqlite3.Connection,
        column: Optional[str],
        value: Optional[str],
        window: int,
        limit: int,
        now: int,
    ) -> None:
        if column is None:
            rows = connection.execute(
                "SELECT created_at FROM auth_challenges WHERE created_at>? "
                "ORDER BY created_at",
                (now - window,),
            ).fetchall()
        else:
            rows = connection.execute(
                f"SELECT created_at FROM auth_challenges WHERE {column}=? "
                "AND created_at>? ORDER BY created_at",
                (value, now - window),
            ).fetchall()
        if len(rows) >= limit:
            oldest = int(rows[0]["created_at"])
            raise RateLimited(max(1, window - (now - oldest) + 1))

    def finish_send(self, challenge_id: int, status: str, provider_code: str) -> bool:
        """Retain the provider outcome but never revive a retired challenge."""
        if status not in {"sent", "rejected", "unknown"}:
            raise ValueError("invalid send status")
        with self._immediate() as connection:
            cursor = connection.execute(
                "UPDATE auth_challenges SET status=?, provider_code=? "
                "WHERE id=? AND status='pending'",
                (status, provider_code[:64], challenge_id),
            )
            if cursor.rowcount != 1:
                return False
            row = connection.execute(
                "SELECT invalidated_at FROM auth_challenges WHERE id=?",
                (challenge_id,),
            ).fetchone()
            return row is not None and row["invalidated_at"] is None

    @staticmethod
    def _latest_sent(
        connection: sqlite3.Connection, purpose: str, phone: str
    ) -> Optional[sqlite3.Row]:
        return connection.execute(
            "SELECT id, username, phone, code_hmac, attempts, expires_at, consumed_at, "
            "invalidated_at FROM auth_challenges "
            "WHERE phone=? AND purpose=? AND status='sent' ORDER BY id DESC LIMIT 1",
            (phone, purpose),
        ).fetchone()

    def _match_code(
        self,
        connection: sqlite3.Connection,
        purpose: str,
        phone: str,
        code: str,
        now: int,
    ) -> tuple[Optional[sqlite3.Row], Optional[sqlite3.Row]]:
        """Return (challenge_row, user_row) when ``code`` is currently valid.

        A wrong code bumps ``attempts`` on the latest sent row; the caller
        commits that increment before reporting failure.
        """
        row = self._latest_sent(connection, purpose, phone)
        if row is None:
            return None, None
        # The newest sent row is the only candidate; once it is consumed,
        # expired, exhausted or retired, older rows never become valid again.
        if (
            row["consumed_at"] is not None
            or row["invalidated_at"] is not None
            or int(row["expires_at"]) <= now
            or int(row["attempts"]) >= CODE_MAX_ATTEMPTS
        ):
            return None, None
        user_row: Optional[sqlite3.Row] = None
        if purpose != "register":
            if row["username"] is None:
                return None, None
            user_row = self._select_user(connection, str(row["username"]))
            if (
                user_row is None
                or user_row["phone"] is None
                or str(user_row["phone"]) != phone
                or str(user_row["status"]) != "active"
            ):
                return None, None
        expected = code_hmac(self.pepper, purpose, phone, code)
        if not hmac.compare_digest(str(row["code_hmac"]), expected):
            connection.execute(
                "UPDATE auth_challenges SET attempts=attempts+1 WHERE id=?",
                (int(row["id"]),),
            )
            return None, None
        return row, user_row

    def _consume(self, connection: sqlite3.Connection, challenge_id: int, now: int) -> bool:
        cursor = connection.execute(
            "UPDATE auth_challenges SET consumed_at=? WHERE id=? AND status='sent' "
            "AND consumed_at IS NULL AND invalidated_at IS NULL AND expires_at>? "
            "AND attempts<?",
            (now, challenge_id, now, CODE_MAX_ATTEMPTS),
        )
        return cursor.rowcount == 1

    def prepare_register(
        self, username: str, phone: str, code: str, client_ip: str
    ) -> int:
        """Cheap pre-checks for registration; returns the challenge id.

        The code is compared but not consumed, so the caller can hash the
        password afterwards and consume inside ``complete_register``.
        """
        keys = (self.phone_key(phone), self.ip_key(client_ip))
        with self._immediate() as connection:
            now = self._now()
            retry_after = self._retry_after(connection, keys, now)
            if retry_after:
                raise RateLimited(retry_after)
            if self._username_reserved(connection, username):
                self._reject(connection, keys, now, UsernameTaken(username))
            if self._select_user_by_phone(connection, phone) is not None:
                self._reject(connection, keys, now, PhoneRegistered(phone))
            if not self._phone_allowed(connection, phone):
                self._reject(connection, keys, now, PhoneNotAllowed(phone))
            row, _user = self._match_code(connection, "register", phone, code, now)
            if row is None:
                self._reject(connection, keys, now, InvalidCode(phone))
            return int(row["id"])

    def complete_register(
        self,
        username: str,
        phone: str,
        password_hash: str,
        challenge_id: int,
        ttl_seconds: int,
    ) -> str:
        change_id: Optional[str] = None
        with self._immediate() as connection:
            now = self._now()
            if self._username_reserved(connection, username):
                raise UsernameTaken(username)
            if self._select_user_by_phone(connection, phone) is not None:
                raise PhoneRegistered(phone)
            if not self._phone_allowed(connection, phone):
                raise PhoneNotAllowed(phone)
            if not self._consume(connection, challenge_id, now):
                raise InvalidCode(phone)
            try:
                connection.execute(
                    "INSERT INTO auth_users(username, phone, password_hash, status, "
                    "role, created_at, password_updated_at) "
                    "VALUES(?, ?, ?, 'active', 'new_user', ?, ?)",
                    (username, phone, password_hash, now, now),
                )
            except sqlite3.IntegrityError as exc:
                if "phone" in str(exc):
                    raise PhoneRegistered(phone) from exc
                raise UsernameTaken(username) from exc
            token = self._create_session(
                connection,
                username,
                credential_fingerprint(password_hash),
                ttl_seconds,
                now,
            )
            self._clear_failures(connection, self.phone_key(phone))
            change_id = self._record_change_intent(
                "user.register",
                username,
                username,
                ("role", "status", "phone"),
                phone=phone,
                after={
                    "role": ROLE_NEW_USER,
                    "status": STATUS_ACTIVE,
                    "phone": phone,
                },
            )
        assert change_id is not None
        self._record_change_commit(change_id)
        return token

    def login_with_code(
        self, phone: str, code: str, client_ip: str, ttl_seconds: int
    ) -> tuple[str, str]:
        keys = (self.phone_key(phone), self.ip_key(client_ip))
        with self._immediate() as connection:
            now = self._now()
            retry_after = self._retry_after(connection, keys, now)
            if retry_after:
                raise RateLimited(retry_after)
            row, user_row = self._match_code(connection, "login", phone, code, now)
            if row is None or user_row is None:
                self._reject(connection, keys, now, InvalidCode(phone))
            if not self._consume(connection, int(row["id"]), now):
                self._reject(connection, keys, now, InvalidCode(phone))
            canonical = str(user_row["username"])
            token = self._create_session(
                connection,
                canonical,
                credential_fingerprint(str(user_row["password_hash"])),
                ttl_seconds,
                now,
            )
            self._clear_failures(connection, self.phone_key(phone))
            return token, canonical

    def verify_reset(self, phone: str, code: str, client_ip: str) -> str:
        keys = (self.phone_key(phone), self.ip_key(client_ip))
        with self._immediate() as connection:
            now = self._now()
            retry_after = self._retry_after(connection, keys, now)
            if retry_after:
                raise RateLimited(retry_after)
            row, user_row = self._match_code(connection, "reset", phone, code, now)
            if row is None or user_row is None:
                self._reject(connection, keys, now, InvalidCode(phone))
            if not self._consume(connection, int(row["id"]), now):
                self._reject(connection, keys, now, InvalidCode(phone))
            token = generate_token()
            connection.execute(
                "UPDATE auth_challenges SET ticket_hmac=?, ticket_expires_at=? "
                "WHERE id=?",
                (ticket_hmac(self.pepper, token), now + TICKET_TTL_SECONDS, int(row["id"])),
            )
            self._clear_failures(connection, self.phone_key(phone))
            return token

    def _select_ticket(
        self, connection: sqlite3.Connection, token: str, now: int
    ) -> Optional[sqlite3.Row]:
        return connection.execute(
            """
            SELECT c.id, c.username, c.phone, u.phone AS user_phone, u.status,
                   u.password_hash
            FROM auth_challenges c JOIN auth_users u ON u.username=c.username
            WHERE c.ticket_hmac=? AND c.ticket_used_at IS NULL
              AND c.invalidated_at IS NULL
              AND c.ticket_expires_at>? AND c.purpose='reset'
            """,
            (ticket_hmac(self.pepper, token), now),
        ).fetchone()

    @staticmethod
    def _ticket_binding_ok(row: sqlite3.Row) -> bool:
        return (
            row["user_phone"] is not None
            and str(row["user_phone"]) == str(row["phone"])
            and str(row["status"]) == "active"
        )

    def peek_ticket(self, token: str, client_ip: str) -> str:
        """Throttle check plus ticket lookup; returns the bound username."""
        keys = (self.ip_key(client_ip),)
        with self._immediate() as connection:
            now = self._now()
            retry_after = self._retry_after(connection, keys, now)
            if retry_after:
                raise RateLimited(retry_after)
            row = self._select_ticket(connection, token, now)
            if row is None or not self._ticket_binding_ok(row):
                self._reject(connection, keys, now, ResetExpired(""))
            return str(row["username"])

    def confirm_reset(
        self, token: str, password_hash: str, ttl_seconds: int
    ) -> tuple[str, str]:
        change_id: Optional[str] = None
        with self._immediate() as connection:
            now = self._now()
            digest = ticket_hmac(self.pepper, token)
            cursor = connection.execute(
                "UPDATE auth_challenges SET ticket_used_at=? WHERE ticket_hmac=? "
                "AND ticket_used_at IS NULL AND invalidated_at IS NULL "
                "AND ticket_expires_at>?",
                (now, digest, now),
            )
            if cursor.rowcount != 1:
                raise ResetExpired("")
            row = connection.execute(
                """
                SELECT c.username, c.phone, u.phone AS user_phone, u.status,
                       u.password_hash
                FROM auth_challenges c JOIN auth_users u ON u.username=c.username
                WHERE c.ticket_hmac=?
                """,
                (digest,),
            ).fetchone()
            if row is None or not self._ticket_binding_ok(row):
                raise ResetExpired("")
            canonical = str(row["username"])
            updated = connection.execute(
                "UPDATE auth_users SET password_hash=?, password_updated_at=? "
                "WHERE username=?",
                (password_hash, now, canonical),
            )
            if updated.rowcount != 1:
                raise ResetExpired("")
            connection.execute(
                "DELETE FROM auth_sessions WHERE username=?", (canonical,)
            )
            self._invalidate_challenges(
                connection,
                username=canonical,
                phone=str(row["phone"]),
                reason="password_changed",
                now=now,
            )
            session_token = self._create_session(
                connection,
                canonical,
                credential_fingerprint(password_hash),
                ttl_seconds,
                now,
            )
            change_id = self._record_change_intent(
                "user.reset_password",
                canonical,
                canonical,
                ("password",),
                after={"password_changed": True},
            )
        assert change_id is not None
        self._record_change_commit(change_id)
        return session_token, canonical

    # ------------------------------------------------- user management (page)

    def _load_actor(self, connection: sqlite3.Connection, session_hash: str) -> sqlite3.Row:
        """Re-verify the caller's session inside the write transaction.

        The request already resolved the session, but a password reset, a
        deletion or a demotion may have landed between that lookup and the
        write lock; the transaction is the only place where the decision is
        final.  A session that no longer verifies raises ``SessionRevoked`` and
        an account whose role can no longer manage users raises
        ``ActorForbidden``.
        """
        row = connection.execute(
            """
            SELECT u.username, s.expires_at, s.credential_fingerprint,
                   u.password_hash, u.role, u.status
            FROM auth_sessions s JOIN auth_users u ON u.username=s.username
            WHERE s.token_sha256=?
            """,
            (session_hash,),
        ).fetchone()
        if (
            row is None
            or int(row["expires_at"]) <= self._now()
            or str(row["status"]) != STATUS_ACTIVE
            or not hmac.compare_digest(
                str(row["credential_fingerprint"]),
                credential_fingerprint(str(row["password_hash"])),
            )
        ):
            raise SessionRevoked(session_hash[:8])
        if str(row["role"]) not in USER_ADMIN_ROLES:
            raise ActorForbidden(str(row["username"]))
        return row

    def update_user(
        self,
        session_hash: str,
        target: str,
        *,
        phone: Optional[str],
        role: str,
        password_hash: Optional[str],
    ) -> UserRecord:
        """Change a user's phone, role and/or password on behalf of ``session_hash``.

        Rules (all evaluated inside the transaction): the caller must outrank
        or equal the target; the granted role must not outrank the caller; a
        caller cannot change their own role or password here; the last active
        superadmin cannot be demoted.  A phone change revokes the old number's
        admission; a password change revokes the target's sessions.  Challenge
        rows are retired, never deleted.
        """
        if role not in ROLE_RANK:
            raise InvalidRole(role)
        changed: list[str] = []
        change_id: Optional[str] = None
        with self._immediate() as connection:
            now = self._now()
            actor = self._load_actor(connection, session_hash)
            row = self._select_user(connection, target)
            if row is None:
                raise UserNotFound(target)
            actor_name, actor_role = str(actor["username"]), str(actor["role"])
            target_name, target_role = str(row["username"]), str(row["role"])
            is_self = actor_name == target_name
            if ROLE_RANK[actor_role] < ROLE_RANK[target_role]:
                raise TargetForbidden(target_name)
            if is_self and role != target_role:
                raise SelfRoleChange(target_name)
            if is_self and password_hash is not None:
                raise SelfPasswordChange(target_name)
            if ROLE_RANK[role] > ROLE_RANK[actor_role]:
                raise RoleForbidden(role)
            if (
                target_role == ROLE_SUPERADMIN
                and role != ROLE_SUPERADMIN
                and str(row["status"]) == STATUS_ACTIVE
                and self._active_superadmins(connection) <= 1
            ):
                raise LastSuperadmin(target_name)
            old_phone = None if row["phone"] is None else str(row["phone"])
            if phone:
                conflict = self._select_user_by_phone(connection, phone)
                if conflict is not None and str(conflict["username"]) != target_name:
                    raise PhoneConflict(phone)
            if phone != old_phone:
                changed.append("phone")
            if role != target_role:
                changed.append("role")
            if password_hash is not None:
                changed.append("password")
                connection.execute(
                    "UPDATE auth_users SET phone=?, role=?, password_hash=?, "
                    "password_updated_at=? WHERE username=?",
                    (phone, role, password_hash, now, target_name),
                )
            else:
                connection.execute(
                    "UPDATE auth_users SET phone=?, role=? WHERE username=?",
                    (phone, role, target_name),
                )
            if "phone" in changed:
                if old_phone is not None:
                    connection.execute(
                        "DELETE FROM auth_allowed_phones WHERE phone=?", (old_phone,)
                    )
                self._invalidate_challenges(
                    connection,
                    username=target_name,
                    phone=old_phone,
                    reason="phone_changed",
                    now=now,
                )
            if "password" in changed:
                connection.execute(
                    "DELETE FROM auth_sessions WHERE username=?", (target_name,)
                )
                self._invalidate_challenges(
                    connection,
                    username=target_name,
                    phone=phone,
                    reason="password_changed",
                    now=now,
                )
            updated = self._select_user(connection, target_name)
            if changed:
                after: dict[str, object] = {}
                if "phone" in changed:
                    after["phone"] = phone
                if "role" in changed:
                    after["role"] = role
                if "password" in changed:
                    after["password_changed"] = True
                change_id = self._record_change_intent(
                    "user.update",
                    actor_name,
                    target_name,
                    tuple(changed),
                    phone=phone if "phone" in changed else None,
                    after=after,
                )
        if change_id is not None:
            self._record_change_commit(change_id)
        assert updated is not None
        return self._user_from_row(updated)

    def delete_user(self, session_hash: str, target: str) -> UserRecord:
        """Delete ``target`` on behalf of ``session_hash`` (never oneself)."""
        change_id: Optional[str] = None
        with self._immediate() as connection:
            actor = self._load_actor(connection, session_hash)
            row = self._select_user(connection, target)
            if row is None:
                raise UserNotFound(target)
            actor_name, actor_role = str(actor["username"]), str(actor["role"])
            target_name = str(row["username"])
            if actor_name == target_name:
                raise SelfDelete(target_name)
            if ROLE_RANK[actor_role] < ROLE_RANK[str(row["role"])]:
                raise TargetForbidden(target_name)
            record = self._delete_row(connection, row, deleted_by=actor_name)
            change_id = self._record_change_intent(
                "user.delete",
                actor_name,
                target_name,
                ("role", "phone"),
                phone=record.phone,
                after={"deleted": True, "role": record.role, "phone": record.phone},
            )
        assert change_id is not None
        self._record_change_commit(change_id)
        return record

    def delete_user_cli(self, username: str, *, actor: str = "cli") -> UserRecord:
        change_id: Optional[str] = None
        with self._immediate() as connection:
            row = self._select_user(connection, username)
            if row is None:
                raise UserNotFound(username)
            record = self._delete_row(connection, row, deleted_by=actor)
            change_id = self._record_change_intent(
                "user.delete",
                actor,
                record.username,
                ("role", "phone"),
                phone=record.phone,
                after={"deleted": True, "role": record.role, "phone": record.phone},
            )
        assert change_id is not None
        self._record_change_commit(change_id)
        return record

    def _delete_row(
        self, connection: sqlite3.Connection, row: sqlite3.Row, *, deleted_by: str
    ) -> UserRecord:
        """Deletion revokes access: tombstone, phone admission, sessions, codes."""
        target_name = str(row["username"])
        target_role = str(row["role"])
        phone = None if row["phone"] is None else str(row["phone"])
        if (
            target_role == ROLE_SUPERADMIN
            and str(row["status"]) == STATUS_ACTIVE
            and self._active_superadmins(connection) <= 1
        ):
            raise LastSuperadmin(target_name)
        now = self._now()
        connection.execute(
            "INSERT INTO auth_deleted_users(username, deleted_at, deleted_by, role, phone) "
            "VALUES(?,?,?,?,?)",
            (target_name, now, deleted_by, target_role, phone),
        )
        if phone is not None:
            connection.execute("DELETE FROM auth_allowed_phones WHERE phone=?", (phone,))
        connection.execute("DELETE FROM auth_sessions WHERE username=?", (target_name,))
        self._invalidate_challenges(
            connection, username=target_name, phone=phone, reason="user_deleted", now=now
        )
        cursor = connection.execute(
            "DELETE FROM auth_users WHERE username=?", (target_name,)
        )
        if cursor.rowcount != 1:
            raise UserNotFound(target_name)
        return self._user_from_row(row)

    def set_role(self, username: str, role: str, *, actor: str = "cli") -> UserRecord:
        """CLI-only role change: the first superadmin and zero-superadmin recovery."""
        if role not in ROLE_RANK:
            raise InvalidRole(role)
        change_id: Optional[str] = None
        with self._immediate() as connection:
            row = self._select_user(connection, username)
            if row is None:
                raise UserNotFound(username)
            target_name = str(row["username"])
            if (
                str(row["role"]) == ROLE_SUPERADMIN
                and role != ROLE_SUPERADMIN
                and str(row["status"]) == STATUS_ACTIVE
                and self._active_superadmins(connection) <= 1
            ):
                raise LastSuperadmin(target_name)
            connection.execute(
                "UPDATE auth_users SET role=? WHERE username=?", (role, target_name)
            )
            updated = self._select_user(connection, target_name)
            change_id = self._record_change_intent(
                "user.set_role",
                actor,
                target_name,
                ("role",),
                after={"role": role},
            )
        assert change_id is not None
        self._record_change_commit(change_id)
        assert updated is not None
        return self._user_from_row(updated)

    # ------------------------------------------------------------ change log

    @staticmethod
    def _validate_audit_timestamp(value: object, *, line_number: int) -> None:
        if not isinstance(value, str):
            raise ChangeLogError(f"change log line {line_number}: ts must be a string")
        try:
            stamp = datetime.fromisoformat(value)
        except ValueError as exc:
            raise ChangeLogError(
                f"change log line {line_number}: invalid ISO-8601 timestamp"
            ) from exc
        if stamp.tzinfo is None:
            raise ChangeLogError(
                f"change log line {line_number}: timestamp must include a timezone"
            )

    @staticmethod
    def _validate_audit_intent(entry: object, *, line_number: int) -> dict[str, object]:
        if not isinstance(entry, dict):
            raise ChangeLogError(f"change log line {line_number}: JSON object required")
        expected = {
            "schema",
            "event",
            "change_id",
            "ts",
            "action",
            "actor",
            "target",
            "fields",
            "after",
            "phone",
            "user_version",
        }
        if set(entry) != expected:
            raise ChangeLogError(
                f"change log line {line_number}: invalid intent fields"
            )
        if entry.get("schema") != CHANGE_LOG_SCHEMA or entry.get("event") != "intent":
            raise ChangeLogError(f"change log line {line_number}: invalid intent marker")
        change_id = entry.get("change_id")
        if not isinstance(change_id, str) or _CHANGE_ID_PATTERN.fullmatch(change_id) is None:
            raise ChangeLogError(f"change log line {line_number}: invalid change_id")
        AuthStore._validate_audit_timestamp(entry.get("ts"), line_number=line_number)
        action = entry.get("action")
        if not isinstance(action, str) or action not in AUDIT_ACTIONS:
            raise ChangeLogError(f"change log line {line_number}: unknown action")
        for name in ("actor", "target"):
            value = entry.get(name)
            if not isinstance(value, str) or not value or len(value) > 256:
                raise ChangeLogError(
                    f"change log line {line_number}: invalid {name}"
                )
        fields = entry.get("fields")
        if (
            not isinstance(fields, list)
            or any(not isinstance(value, str) or value not in AUDIT_FIELD_NAMES for value in fields)
            or len(set(fields)) != len(fields)
        ):
            raise ChangeLogError(f"change log line {line_number}: invalid fields list")
        after = entry.get("after")
        if not isinstance(after, dict) or not set(after).issubset(AUDIT_AFTER_NAMES):
            raise ChangeLogError(f"change log line {line_number}: invalid after values")
        if any(not isinstance(name, str) for name in after):
            raise ChangeLogError(f"change log line {line_number}: invalid after key")
        role = after.get("role")
        if "role" in after and (not isinstance(role, str) or role not in ROLES):
            raise ChangeLogError(f"change log line {line_number}: invalid resulting role")
        status_value = after.get("status")
        if "status" in after and (
            not isinstance(status_value, str)
            or status_value not in {STATUS_ACTIVE, STATUS_DISABLED}
        ):
            raise ChangeLogError(f"change log line {line_number}: invalid resulting status")
        phone_value = after.get("phone")
        if phone_value is not None and (
            not isinstance(phone_value, str) or not valid_phone(phone_value)
        ):
            raise ChangeLogError(f"change log line {line_number}: invalid resulting phone")
        for boolean_name in ("allowed", "deleted", "password_changed"):
            if boolean_name in after and not isinstance(after[boolean_name], bool):
                raise ChangeLogError(
                    f"change log line {line_number}: {boolean_name} must be boolean"
                )
        if "count" in after and (
            not isinstance(after["count"], int)
            or isinstance(after["count"], bool)
            or int(after["count"]) < 0
        ):
            raise ChangeLogError(f"change log line {line_number}: count must be non-negative")
        phone = entry.get("phone")
        if phone is not None and (not isinstance(phone, str) or not valid_phone(phone)):
            raise ChangeLogError(f"change log line {line_number}: invalid phone")
        if (
            type(entry.get("user_version")) is not int
            or entry["user_version"] not in CHANGE_LOG_USER_VERSIONS
            or (entry["user_version"] == 2 and role == ROLE_NEW_USER)
        ):
            raise ChangeLogError(f"change log line {line_number}: invalid user_version")
        field_set = set(fields)
        after_keys = set(after)
        fixed_contracts: dict[str, tuple[set[str], set[str]]] = {
            "phone.allow": ({"note"}, {"allowed", "phone"}),
            "phone.remove": (set(), {"allowed", "phone"}),
            "sessions.revoke": ({"count"}, {"count"}),
            "challenges.revoke": ({"count"}, {"count"}),
            "user.create": ({"role", "status", "phone"}, {"role", "status", "phone"}),
            "user.delete": ({"role", "phone"}, {"deleted", "role", "phone"}),
            "user.disable": ({"status"}, {"status"}),
            "user.enable": ({"status"}, {"status"}),
            "user.import": ({"count", "role", "status"}, {"count", "role", "status"}),
            "user.register": ({"role", "status", "phone"}, {"role", "status", "phone"}),
            "user.reset_password": ({"password"}, {"password_changed"}),
            "user.set_password": ({"password"}, {"password_changed"}),
            "user.set_phone": ({"phone"}, {"phone"}),
            "user.set_role": ({"role"}, {"role"}),
        }
        if action == "user.update":
            if not field_set or not field_set.issubset({"phone", "role", "password"}):
                raise ChangeLogError(
                    f"change log line {line_number}: invalid user.update fields"
                )
            expected_after = {
                "password_changed" if name == "password" else name
                for name in field_set
            }
            if after_keys != expected_after:
                raise ChangeLogError(
                    f"change log line {line_number}: incomplete user.update after values"
                )
        else:
            expected_fields, expected_after = fixed_contracts[action]
            if field_set != expected_fields or after_keys != expected_after:
                raise ChangeLogError(
                    f"change log line {line_number}: action fields do not match {action}"
                )
        if action == "phone.allow" and after.get("allowed") is not True:
            raise ChangeLogError(f"change log line {line_number}: phone.allow must allow")
        if action == "phone.remove" and after.get("allowed") is not False:
            raise ChangeLogError(f"change log line {line_number}: phone.remove must remove")
        if action == "user.delete" and after.get("deleted") is not True:
            raise ChangeLogError(f"change log line {line_number}: user.delete must delete")
        if "password_changed" in after and after["password_changed"] is not True:
            raise ChangeLogError(f"change log line {line_number}: password change must be true")
        if action == "user.disable" and after.get("status") != STATUS_DISABLED:
            raise ChangeLogError(f"change log line {line_number}: user.disable status mismatch")
        if action == "user.enable" and after.get("status") != STATUS_ACTIVE:
            raise ChangeLogError(f"change log line {line_number}: user.enable status mismatch")
        if "phone" in after and entry.get("phone") != after.get("phone"):
            raise ChangeLogError(f"change log line {line_number}: phone values disagree")
        if "phone" not in after and entry.get("phone") is not None:
            raise ChangeLogError(f"change log line {line_number}: unexpected phone value")
        return entry

    @staticmethod
    def _validate_audit_commit(entry: object, *, line_number: int) -> dict[str, object]:
        if not isinstance(entry, dict):
            raise ChangeLogError(f"change log line {line_number}: JSON object required")
        expected = {"schema", "event", "change_id", "ts", "user_version"}
        if set(entry) != expected:
            raise ChangeLogError(
                f"change log line {line_number}: invalid commit fields"
            )
        if entry.get("schema") != CHANGE_LOG_SCHEMA or entry.get("event") != "commit":
            raise ChangeLogError(f"change log line {line_number}: invalid commit marker")
        change_id = entry.get("change_id")
        if not isinstance(change_id, str) or _CHANGE_ID_PATTERN.fullmatch(change_id) is None:
            raise ChangeLogError(f"change log line {line_number}: invalid change_id")
        AuthStore._validate_audit_timestamp(entry.get("ts"), line_number=line_number)
        if (
            type(entry.get("user_version")) is not int
            or entry["user_version"] not in CHANGE_LOG_USER_VERSIONS
        ):
            raise ChangeLogError(f"change log line {line_number}: invalid user_version")
        return entry

    def _change_log_parent_descriptor(self) -> int:
        parent = self.change_log_path.parent
        try:
            parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            if parent.is_symlink():
                raise ChangeLogError("security change-log directory may not be a symlink")
            flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            return os.open(parent, flags)
        except ChangeLogError:
            raise
        except OSError as exc:
            raise ChangeLogError(
                f"cannot open security change-log directory: {parent}"
            ) from exc

    @staticmethod
    def _validate_change_log_file(descriptor: int) -> os.stat_result:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise ChangeLogError(
                "security change log must be a regular file with one hard link"
            )
        return metadata

    def _append_change_event(self, entry: dict[str, object]) -> None:
        encoded = (json.dumps(entry, ensure_ascii=False, separators=(",", ":")) + "\n").encode(
            "utf-8"
        )
        if len(encoded) > CHANGE_LOG_MAX_LINE_BYTES:
            raise ChangeLogError("security change-log entry is too large")
        directory_descriptor = self._change_log_parent_descriptor()
        descriptor: Optional[int] = None
        try:
            flags = os.O_RDWR | os.O_CREAT | os.O_APPEND | getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(
                self.change_log_path.name,
                flags,
                0o600,
                dir_fd=directory_descriptor,
            )
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            metadata = self._validate_change_log_file(descriptor)
            os.fchmod(descriptor, 0o600)
            if stat.S_IMODE(os.fstat(descriptor).st_mode) != 0o600:
                raise ChangeLogError("security change-log mode is not 0600")
            if metadata.st_size and os.pread(descriptor, 1, metadata.st_size - 1) != b"\n":
                raise ChangeLogError("security change log has an incomplete final line")
            # A well-terminated but corrupt historical row must not silently
            # turn subsequent security changes into an unreadable journal.
            existing: list[bytes] = []
            read_offset = 0
            while read_offset < metadata.st_size:
                chunk = os.pread(descriptor, 64 * 1024, read_offset)
                if not chunk:
                    raise ChangeLogError("short read from security change log")
                existing.append(chunk)
                read_offset += len(chunk)
            try:
                self._parse_change_log_text((b"".join(existing) + encoded).decode("utf-8"))
            except UnicodeDecodeError as exc:
                raise ChangeLogError("security change log is not valid UTF-8") from exc
            offset = 0
            while offset < len(encoded):
                written = os.write(descriptor, encoded[offset:])
                if written <= 0:
                    raise ChangeLogError("short write to security change log")
                offset += written
            os.fsync(descriptor)
            os.fsync(directory_descriptor)
        except ChangeLogError:
            raise
        except OSError as exc:
            raise ChangeLogError(
                f"cannot append security change log: {self.change_log_path}"
            ) from exc
        finally:
            if descriptor is not None:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                finally:
                    os.close(descriptor)
            os.close(directory_descriptor)

    def _record_change_intent(
        self,
        action: str,
        actor: str,
        target: str,
        fields: Sequence[str] = (),
        *,
        phone: Optional[str] = None,
        after: Optional[dict[str, object]] = None,
    ) -> str:
        """Persist an intent before the database transaction may commit."""
        change_id = secrets.token_hex(16)
        entry: dict[str, object] = {
            "schema": CHANGE_LOG_SCHEMA,
            "event": "intent",
            "change_id": change_id,
            "ts": datetime.now(tz=timezone.utc).isoformat(timespec="microseconds"),
            "action": action,
            "actor": actor,
            "target": target,
            "fields": list(fields),
            "after": dict(after or {}),
            "phone": phone,
            "user_version": SCHEMA_VERSION,
        }
        self._validate_audit_intent(entry, line_number=0)
        self._append_change_event(entry)
        return change_id

    def _record_change_commit(self, change_id: str) -> None:
        """Best-effort commit marker; a missing marker remains conservative."""
        entry = {
            "schema": CHANGE_LOG_SCHEMA,
            "event": "commit",
            "change_id": change_id,
            "ts": datetime.now(tz=timezone.utc).isoformat(timespec="microseconds"),
            "user_version": SCHEMA_VERSION,
        }
        try:
            self._validate_audit_commit(entry, line_number=0)
            self._append_change_event(entry)
        except ChangeLogError:
            # The database already committed.  Never report a normal rollback or
            # invite a blind retry: the durable intent forces conservative
            # recovery until an operator reconciles it.
            LOGGER.critical(
                "database change %s committed without an audit commit marker; "
                "recovery must treat its intent conservatively",
                change_id,
                exc_info=True,
            )

    def record_change(
        self,
        action: str,
        actor: str,
        target: str,
        fields: Sequence[str] = (),
        *,
        phone: Optional[str] = None,
        after: Optional[dict[str, object]] = None,
    ) -> None:
        """Write a standalone intent/commit pair (transactional callers use internals)."""
        change_id = self._record_change_intent(
            action, actor, target, fields, phone=phone, after=after
        )
        self._record_change_commit(change_id)

    def _read_change_log_text(self) -> str:
        try:
            directory_descriptor = self._change_log_parent_descriptor()
        except ChangeLogError as exc:
            if not self.change_log_path.parent.exists():
                return ""
            raise exc
        descriptor: Optional[int] = None
        try:
            flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            try:
                descriptor = os.open(
                    self.change_log_path.name, flags, dir_fd=directory_descriptor
                )
            except FileNotFoundError:
                return ""
            fcntl.flock(descriptor, fcntl.LOCK_SH)
            metadata = self._validate_change_log_file(descriptor)
            if stat.S_IMODE(metadata.st_mode) != 0o600:
                raise ChangeLogError("security change-log mode is not 0600")
            chunks: list[bytes] = []
            while True:
                chunk = os.read(descriptor, 64 * 1024)
                if not chunk:
                    break
                chunks.append(chunk)
            payload = b"".join(chunks)
            if payload and not payload.endswith(b"\n"):
                raise ChangeLogError("security change log has an incomplete final line")
            try:
                return payload.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ChangeLogError("security change log is not valid UTF-8") from exc
        except ChangeLogError:
            raise
        except OSError as exc:
            raise ChangeLogError(
                f"cannot read security change log: {self.change_log_path}"
            ) from exc
        finally:
            if descriptor is not None:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                finally:
                    os.close(descriptor)
            os.close(directory_descriptor)

    def read_changes(self, since: Optional[datetime] = None) -> list[dict[str, object]]:
        """Return one normalized item per intent, including conservative intents."""
        text = self._read_change_log_text()
        return self._parse_change_log_text(text, since)

    def _parse_change_log_text(
        self, text: str, since: Optional[datetime] = None
    ) -> list[dict[str, object]]:
        if not text:
            return []
        intents: dict[str, dict[str, object]] = {}
        intent_order: list[str] = []
        committed: dict[str, str] = {}
        legacy: list[dict[str, object]] = []
        for line_number, raw_line in enumerate(text.splitlines(), start=1):
            if len(raw_line.encode("utf-8")) + 1 > CHANGE_LOG_MAX_LINE_BYTES:
                raise ChangeLogError(f"change log line {line_number}: line is too large")
            if not raw_line.strip():
                raise ChangeLogError(f"change log line {line_number}: blank line")
            try:
                entry = json.loads(raw_line)
            except ValueError as exc:
                raise ChangeLogError(
                    f"change log line {line_number}: malformed JSON"
                ) from exc
            if isinstance(entry, dict) and "schema" not in entry:
                expected_legacy = {
                    "ts", "action", "actor", "target", "fields", "phone", "user_version"
                }
                if set(entry) != expected_legacy:
                    raise ChangeLogError(
                        f"change log line {line_number}: invalid legacy fields"
                    )
                self._validate_audit_timestamp(entry.get("ts"), line_number=line_number)
                if not isinstance(entry.get("action"), str) or entry["action"] not in AUDIT_ACTIONS:
                    raise ChangeLogError(
                        f"change log line {line_number}: unknown legacy action"
                    )
                for name in ("actor", "target"):
                    value = entry.get(name)
                    if not isinstance(value, str) or not value or len(value) > 256:
                        raise ChangeLogError(f"change log line {line_number}: invalid legacy {name}")
                fields = entry.get("fields")
                if not isinstance(fields, list) or any(
                    not isinstance(value, str) or value not in AUDIT_FIELD_NAMES
                    for value in fields
                ) or len(set(fields)) != len(fields):
                    raise ChangeLogError(f"change log line {line_number}: invalid legacy fields")
                phone = entry.get("phone")
                if phone is not None and (not isinstance(phone, str) or not valid_phone(phone)):
                    raise ChangeLogError(f"change log line {line_number}: invalid legacy phone")
                if type(entry.get("user_version")) is not int or entry["user_version"] not in {1, 2}:
                    raise ChangeLogError(f"change log line {line_number}: invalid legacy user_version")
                legacy_entry = dict(entry)
                legacy_entry["state"] = "legacy_conservative"
                legacy_entry["after"] = {}
                legacy.append(legacy_entry)
                continue
            if isinstance(entry, dict) and entry.get("event") == "intent":
                intent = self._validate_audit_intent(entry, line_number=line_number)
                change_id = str(intent["change_id"])
                if change_id in intents:
                    raise ChangeLogError(
                        f"change log line {line_number}: duplicate intent {change_id}"
                    )
                intents[change_id] = intent
                intent_order.append(change_id)
                continue
            commit = self._validate_audit_commit(entry, line_number=line_number)
            change_id = str(commit["change_id"])
            if change_id not in intents:
                raise ChangeLogError(
                    f"change log line {line_number}: commit without prior intent {change_id}"
                )
            if change_id in committed:
                raise ChangeLogError(
                    f"change log line {line_number}: duplicate commit {change_id}"
                )
            if commit["user_version"] != intents[change_id]["user_version"]:
                raise ChangeLogError(
                    f"change log line {line_number}: intent/commit user_version mismatch"
                )
            committed[change_id] = str(commit["ts"])

        normalized = legacy
        for change_id in intent_order:
            item = dict(intents[change_id])
            item.pop("schema", None)
            item.pop("event", None)
            item["state"] = "committed" if change_id in committed else "conservative"
            item["intent_ts"] = item["ts"]
            if change_id in committed:
                item["ts"] = committed[change_id]
            normalized.append(item)
        if since is None:
            return normalized
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        return [
            entry
            for entry in normalized
            if entry["state"] != "committed"
            or datetime.fromisoformat(str(entry["ts"])) >= since
        ]

    # ------------------------------------------------------------ inspection

    def challenge_status(self, challenge_id: int) -> Optional[str]:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT status FROM auth_challenges WHERE id=?", (challenge_id,)
            ).fetchone()
        finally:
            connection.close()
        return str(row["status"]) if row is not None else None

    def counts(self) -> dict[str, int]:
        connection = self._connect()
        try:
            return {
                name: int(
                    connection.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
                )
                for name in sorted(REQUIRED_TABLES)
            }
        finally:
            connection.close()
